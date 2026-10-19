"""Comparable account returns and admission/cash attribution."""
from collections import Counter, defaultdict
from statistics import median
import execution as e


def next_month(month):
    y,m = map(int,month.split('-'))
    return f'{y+1:04d}-01' if m==12 else f'{y:04d}-{m+1:02d}'


def period_tables(result, start, end):
    summary = result['summary']
    observed = {m['month']:m for m in result['monthly_observations']}
    admitted = Counter(r['entry_utc'][:7] for r in result['ledger'] if r['decision']=='ADMITTED')
    closed = Counter(r['exit_utc'][:7] for r in result['trades'])
    month = start[:7]
    previous = summary['initial_balance_gbp']
    monthly = []
    while month <= end[:7]:
        row = observed.get(month)
        closing = row['closing_observed_nav_gbp'] if row else previous
        full = e.dt(month+'-01T00:00:00Z') >= e.dt(start) and e.dt(next_month(month)+'-01T00:00:00Z') <= e.dt(end)
        monthly.append(dict(month=month,opening_nav_gbp=previous,closing_nav_gbp=closing,
                            nav_return_pct=100*(closing/previous-1) if previous>0 else None,
                            complete_calendar_month=full,admitted=admitted[month],closed=closed[month],
                            valuation_basis='LAST_CAUSAL_OBSERVATION_ASOF_MONTH_END; CARRY_FLAT_ACCOUNT_WHEN_NO_EVENTS',
                            last_observation_utc=row.get('last_observed_time') if row else None))
        previous = closing; month = next_month(month)
    by_year = defaultdict(list)
    for row in monthly: by_year[row['month'][:4]].append(row)
    annual = []
    for year,rows in sorted(by_year.items()):
        opening,closing=rows[0]['opening_nav_gbp'],rows[-1]['closing_nav_gbp']
        annual.append(dict(year=year,nav_return_pct=100*(closing/opening-1) if opening>0 else None,
                           complete_calendar_year=len(rows)==12 and all(r['complete_calendar_month'] for r in rows),
                           admitted=sum(r['admitted'] for r in rows),closed=sum(r['closed'] for r in rows),
                           opening_nav_gbp=opening,closing_nav_gbp=closing))
    rolling = []
    for i in range(11,len(monthly)):
        rows=monthly[i-11:i+1]
        if all(r['complete_calendar_month'] for r in rows):
            a,b=rows[0]['opening_nav_gbp'],rows[-1]['closing_nav_gbp']
            rolling.append(dict(start_month=rows[0]['month'],end_month=rows[-1]['month'],
                                nav_return_pct=100*(b/a-1) if a>0 else None,
                                admitted=sum(r['admitted'] for r in rows)))
    return dict(monthly=monthly,annual=annual,rolling12=rolling)


def trade_stats(rows):
    values=[r['net_r_actual_entry_risk'] for r in rows if r.get('net_r_actual_entry_risk') is not None]
    positive=sum(v for v in values if v>0);negative=-sum(v for v in values if v<0)
    return dict(closed=len(rows),net_wins=sum(r['net_pl_gbp']>0 for r in rows),
                win_rate_pct=100*sum(r['net_pl_gbp']>0 for r in rows)/len(rows) if rows else None,
                mean_net_r=sum(values)/len(values) if values else None,
                profit_factor_r=positive/negative if negative else None,
                net_cash_gbp=sum(r['net_pl_gbp'] for r in rows),
                financing_gbp=sum(r['financing_gbp'] for r in rows),
                uncertain_path_count=sum(r.get('path_status') not in ('CLOSED_RESOLVED','OPEN_AT_CUTOFF') for r in rows))


def band(value):
    if value is None:return 'UNKNOWN'
    if value<=10:return 'UP_TO_10'
    if value<=15:return 'OVER_10_TO_15'
    if value<=20:return 'OVER_15_TO_20'
    if value<=30:return 'OVER_20_TO_30'
    return 'OVER_30'


def grouped(result, paths):
    groups=defaultdict(list)
    for r in result['trades']:
        p=paths.get(r['signal_key'],{})
        for kind,key in [('pair',r['pair']),('strategy',r['strategy_id']),('entry_year',r['entry_utc'][:4]),
                         ('entry_band',band(p.get('model_adverse_ticks'))),
                         ('era','2021_ONWARD' if r['entry_utc']>='2021' else 'BEFORE_2021')]:
            groups[kind,key].append(r)
    return [dict(grouping=kind,group=key,**trade_stats(rows)) for (kind,key),rows in sorted(groups.items())]


def extra_summary(result, periods, start, end):
    complete=[r['nav_return_pct'] for r in periods['monthly'] if r['complete_calendar_month'] and r['nav_return_pct'] is not None]
    years=[r['nav_return_pct'] for r in periods['annual'] if r['complete_calendar_year'] and r['nav_return_pct'] is not None]
    rolling=[r['nav_return_pct'] for r in periods['rolling12'] if r['nav_return_pct'] is not None]
    streak=longest=0
    entry_rank={r['signal_key']:i for i,r in enumerate(result['ledger']) if r['decision']=='ADMITTED'}
    for r in sorted(result['trades'],key=lambda r:entry_rank[r['signal_key']]):
        streak=streak+1 if r['net_pl_gbp']<0 else 0
        longest=max(longest,streak)
    years_elapsed=(e.dt(end)-e.dt(start)).total_seconds()/(365.25*86400)
    admitted=result['summary']['decisions'].get('ADMITTED',0)
    return dict(average_full_month_return_pct=sum(complete)/len(complete) if complete else None,
                median_full_month_return_pct=median(complete) if complete else None,
                positive_full_month_pct=100*sum(v>0 for v in complete)/len(complete) if complete else None,
                worst_full_year_return_pct=min(years) if years else None,best_full_year_return_pct=max(years) if years else None,
                worst_rolling12_return_pct=min(rolling) if rolling else None,
                median_rolling12_return_pct=median(rolling) if rolling else None,
                negative_rolling12_count=sum(v<0 for v in rolling),rolling12_windows=len(rolling),
                max_losing_streak_entry_order=longest,admitted_per_year=admitted/years_elapsed,
                admitted_per_month=admitted/(years_elapsed*12),admitted_per_week=admitted/(years_elapsed*365.25/7))


def admission_changes(control,result):
    old={r['signal_key']:r for r in control['ledger'] if r['decision']=='ADMITTED'}
    new={r['signal_key']:r for r in result['ledger'] if r['decision']=='ADMITTED'}
    allold={r['signal_key']:r for r in control['ledger']};allnew={r['signal_key']:r for r in result['ledger']}
    rows=[]
    for key in sorted(set(old)|set(new)):
        category='COMMON' if key in old and key in new else 'ADDED' if key in new else 'DISPLACED'
        rows.append(dict(signal_key=key,category=category,
                         entry_utc=(new.get(key) or old[key])['entry_utc'],
                         control_decision=allold.get(key,{}).get('decision'),policy_decision=allnew.get(key,{}).get('decision'),
                         control_units=old.get(key,{}).get('units'),policy_units=new.get(key,{}).get('units')))
    by_category=Counter(r['category'] for r in rows)
    cashold=defaultdict(float);cashnew=defaultdict(float)
    for r in control['cash_events']:cashold[r['signal_key']]+=r['cash_gbp']
    for r in result['cash_events']:cashnew[r['signal_key']]+=r['cash_gbp']
    contribution=defaultdict(float)
    category={r['signal_key']:r['category'] for r in rows}
    for key in set(cashold)|set(cashnew):
        contribution[category.get(key,'UNCLASSIFIED')]+=cashnew[key]-cashold[key]
    delta=result['summary']['ending_balance_gbp']-control['summary']['ending_balance_gbp']
    e.require(abs(sum(contribution.values())-delta)<.001, 'Policy cash difference does not reconcile')
    old_upl=control['summary']['conditional_ending_nav_gbp']-control['summary']['ending_balance_gbp']
    new_upl=result['summary']['conditional_ending_nav_gbp']-result['summary']['ending_balance_gbp']
    return rows,dict(added_admissions=by_category['ADDED'],displaced_admissions=by_category['DISPLACED'],
                     common_admissions=by_category['COMMON'],ending_balance_change_gbp=delta,
                     added_booked_cash_gbp=contribution['ADDED'],displaced_booked_cash_gbp=contribution['DISPLACED'],
                     common_resize_and_compounding_cash_gbp=contribution['COMMON'],
                     cash_attribution_includes_financing_on_open_positions=True,
                     cash_attribution_residual_gbp=delta-sum(contribution.values()),
                     ending_unrealised_pl_change_gbp=new_upl-old_upl,
                     scope='Path-dependent account cash decomposition, not independent marginal alpha')
