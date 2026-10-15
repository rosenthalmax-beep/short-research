"""Chronological gate scenarios. Uncertainty and chosen ordering stay explicit."""
import itertools,statistics
from datetime import timedelta
import execution as e
from history import END

SCENARIOS=('END_ASC','END_DESC','START_ASC','START_DESC')

def candidate_data(data,signal):
    sign=1 if signal['side']=='BUY' else -1
    r=dict(entry=signal['entry_utc'],exit=e.stamp(min(END,e.dt(signal['entry_utc'])+timedelta(seconds=e.STEP[signal['timeframe']]))),
           signal=signal['signal_start_utc'],pair=signal['pair'],side=signal['side'],tf=signal['timeframe'],rr=signal['rr'],sid=signal['strategy_id'],r=0)
    g=dict(index=0,pair=signal['pair'],side=signal['side'],sid=signal['strategy_id'],fill=signal['reference_entry'],reference_entry=signal['reference_entry'],
           stop=signal['stop'],target=signal['target'],stop_distance=sign*(signal['reference_entry']-signal['stop']),
           geometry_source='REGENERATED_OBSERVED_BAR_LOCKED_RULE',geometry_validation='PREDICATES_PINNED_FULL_HISTORY_SEED_RECONCILIATION_REPORTED')
    return dict(reference={'book':[r],'geometry':[g]},historical_end=e.stamp(END),funding=data['funding'])

def ordered(signals,scenario):
    for when,group in itertools.groupby(sorted(signals,key=lambda s:s['entry_utc']),key=lambda s:s['entry_utc']):
        groups={}
        for s in group:groups.setdefault(s['strategy_id'],[]).append(s)
        for sid in sorted(groups,reverse=scenario.endswith('DESC')):
            yield from sorted(groups[sid],key=lambda s:(s['same_strategy_trigger_priority'],s['signal_key']))

def release_time(path,scenario):
    if path.get('kind')=='OPEN_AT_FIXED_CUTOFF':return END
    field='interval_end_utc' if scenario.startswith('END') else 'interval_start_utc'
    return e.dt(path[field]) if path.get(field) else END

def replay(signals,scenario,path_for,progress=lambda **x:None,hedging=False):
    e.require(scenario in SCENARIOS,'Unknown admission scenario')
    active=[];rows=[];uncertain_pairs=set();sequence=0
    for signal in ordered(signals,scenario):
        when=e.dt(signal['entry_utc']);active=[x for x in active if release_time(x['path'],scenario)>when]
        blockers=[x for x in active if x['signal']['strategy_id']==signal['strategy_id']]
        gate='STRATEGY_PYRAMIDING_0'
        if not blockers:
            blockers=[x for x in active if x['signal']['pair']==signal['pair'] and x['signal']['side']!=signal['side'] and (not hedging or signal['pair']=='AUD_JPY')]
            gate='OPPOSITE_PAIR_NONHEDGING'
        base=dict(scenario=scenario,sequence=sequence,signal_key=signal['signal_key'],strategy_id=signal['strategy_id'],pair=signal['pair'],side=signal['side'],entry_utc=signal['entry_utc'],
                  same_strategy_trigger_priority=signal['same_strategy_trigger_priority'],prior_uncertain_pair_lineage=signal['pair'] in uncertain_pairs)
        sequence+=1
        if blockers:
            uncertain=any(x['path'].get('status') not in ('CLOSED_RESOLVED','OPEN_AT_CUTOFF') for x in blockers)
            if uncertain:uncertain_pairs.add(signal['pair'])
            rows.append(dict(base,decision='BLOCKED_UNDER_SCENARIO',gate=gate,blocking_signal_keys=[x['signal']['signal_key'] for x in blockers],
                             blocker_path_uncertain=uncertain,decision_has_uncertain_lineage=signal['pair'] in uncertain_pairs,active_positions=len(active)))
            continue
        path=path_for(signal)
        if path['status']=='ENTRY_PROXY_REJECTED':
            rows.append(dict(base,decision='ENTRY_PROXY_REJECTED',gate=path['entry_status'],decision_has_uncertain_lineage=signal['pair'] in uncertain_pairs,active_positions=len(active)));continue
        if path['status'] in ('ERROR','NEEDS_CANDLE_DATA','UNRESOLVED_ENTRY'):
            uncertain_pairs.add(signal['pair'])
            rows.append(dict(base,decision='UNRESOLVED_ENTRY_OR_DATA',gate=path.get('entry_status',path['status']),decision_has_uncertain_lineage=True,active_positions=len(active)));continue
        uncertain=path['status'] not in ('CLOSED_RESOLVED','OPEN_AT_CUTOFF')
        if uncertain:uncertain_pairs.add(signal['pair'])
        active.append(dict(signal=signal,path=path))
        rows.append(dict(base,decision='ADMITTED_UNDER_SCENARIO',path_status=path['status'],exit_kind=path.get('kind'),exit_certainty=path.get('certainty'),
                         modeled_release_utc=e.stamp(release_time(path,scenario)),path_uncertain=uncertain,
                         decision_has_uncertain_lineage=signal['pair'] in uncertain_pairs,active_positions=len(active)))
        if sequence%100==0:progress(phase='REPLAY_ADMISSIONS',scenario=scenario,candidates_processed=sequence,candidates_required=len(signals),admitted=sum(x['decision']=='ADMITTED_UNDER_SCENARIO' for x in rows))
    return rows

def outcome_budget_r(signal,path,ambiguity):
    if path.get('kind')=='OPEN_AT_FIXED_CUTOFF':return None
    value=path.get('observed_path_worst_bound_budget_r')
    if value is not None:return float(value)
    if path.get('kind')=='AMBIGUOUS_BOTH' and path.get('model_worst_stop_distance',0)>0:
        price=path['model_stop'] if ambiguity=='STOP_FIRST' else path['model_target']
        sign=1 if signal['side']=='BUY' else -1
        return sign*(price-path['model_fill'])/path['model_worst_stop_distance']
    return None

def model_accounting(ledger,signals,paths,ambiguity='STOP_FIRST'):
    """Illustrative closed-balance risk units. Never GBP/NAV or validated returns."""
    mapping={s['signal_key']:s for s in signals}
    admitted=[r for r in ledger if r['decision']=='ADMITTED_UNDER_SCENARIO']
    events=[];unpriced=[];open_count=0
    for row in admitted:
        key=row['signal_key'];s=mapping[key];p=paths[key];value=outcome_budget_r(s,p,ambiguity)
        events.append((e.dt(s['entry_utc']),1,row['sequence'],'ENTRY',key,None))
        if p.get('kind')=='OPEN_AT_FIXED_CUTOFF':open_count+=1
        elif value is None:unpriced.append(key)
        else:events.append((e.dt(row['modeled_release_utc']),0,row['sequence'],'EXIT',key,value))
    # Same instant: existing exits first. A newly admitted position that closes
    # immediately still needs its own ENTRY before its EXIT; handle explicitly.
    events.sort(key=lambda x:(x[0],x[1],x[2]))
    balance=100.0;peak=balance;worst=0.0;risks={};recorded=[];deferred={};net_r=0.0
    def close(key,value,when):
        nonlocal balance,peak,worst,net_r
        risk=risks.pop(key);before=balance;balance+=risk*value;net_r+=value;peak=max(peak,balance);worst=min(worst,(balance/peak-1)*100)
        recorded.append(dict(time=e.stamp(when),event='EXIT',signal_key=key,risk_units=risk,price_budget_r=value,balance_before=before,balance_after=balance,closed_drawdown_pct=(balance/peak-1)*100))
    for when,priority,sequence,kind,key,value in events:
        if kind=='ENTRY':
            fraction=.0075 if mapping[key]['strategy_id']=='EUR_JPY_M15_SHORT' else .01
            risks[key]=max(0,balance)*fraction
            recorded.append(dict(time=e.stamp(when),event='ENTRY',signal_key=key,risk_units=risks[key],balance_before=balance,balance_after=balance))
            if key in deferred:close(key,deferred.pop(key),when)
        elif key in risks:close(key,value,when)
        else:deferred[key]=value
    complete=not unpriced and balance>0
    years=(END-e.dt('2005-01-01T00:00:00Z')).total_seconds()/(365.25*86400)
    result=dict(ambiguity_assumption=ambiguity,initial_research_units=100,admitted=len(admitted),priced_exits=sum(r['event']=='EXIT' for r in recorded),open_at_cutoff=open_count,
                unresolved_entry_decisions=sum(r['decision']=='UNRESOLVED_ENTRY_OR_DATA' for r in ledger),
                entry_uncertainty_assumption='OMIT_UNRESOLVED_ENTRIES_AND_FLAG_LATER_PAIR_LINEAGE',
                unknown_exit_values=len(unpriced),conditional_observed_path_budget_r_sum=net_r,
                conditional_closed_balance_units=balance if complete else None,
                conditional_closed_equity_cagr_pct=((balance/100)**(1/years)-1)*100 if complete else None,
                conditional_closed_equity_drawdown_pct=worst if complete else None,
                scope='ILLUSTRATIVE_OBSERVED_BARRIER_SCENARIO_WITH_UNCERTAINTY_NOT_VALIDATED_PORTFOLIO',
                uses_unresolved_observed_barriers=any(paths[r['signal_key']]['status']=='UNRESOLVED_PATH' for r in admitted),
                actual_home_currency_conversion=False,whole_units=False,nav_based_sizing=False,margin_modeled=False,
                financing_included=False,open_equity_drawdown_measured=False,actual_broker_returns=False)
    return result,recorded
