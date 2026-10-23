"""Causal lifecycle instrumentation; post-expiry observations are not trade paths."""
from collections import Counter
from pullback_lab import core as e
from pullback_lab.signals import exact_engulf
from audusd_matrix.signals import evaluate

RULES=[dict(candidate_id='M_D'+str(d)+'_B1.25',kind='MATRIX',distance_max=d,body_min=1.25) for d in (1.,1.25,1.5)]


def utc(t):return e.stamp(e.datetime.fromtimestamp(t,e.UTC))


class Audit:
    def __init__(self,rule,times,start,end):
        self.rule=rule;self.times=times;self.lo=int(e.dt(start).timestamp());self.hi=int(e.dt(end).timestamp())
        self.active=None;self.monitors=[];self.setups=[];self.triggers=[];self.late=[]
    def stamp(self,index,close=False):return utc(self.times[index]+(900 if close else 0))
    def in_window(self,index):return self.lo<=self.times[index]+900<self.hi
    def new(self,index,setup):
        return dict(candidate_id=self.rule['candidate_id'],setup_start_utc=self.stamp(index),start_index=index,
            started_in_report_window=self.in_window(index),touch_area=setup['area'],touch_atr=setup['start_atr'],
            failed_trigger_count=0,body_only_failures=0,distance_only_failures=0,both_failures=0,
            best_failed_body_atr=None,smallest_failed_distance_atr=None)
    def close(self,reason,index):
        if self.active is None:return None
        row=dict(self.active,termination=reason,termination_close_utc=self.stamp(index,True),
                 termination_bar_age=index-self.active['start_index']+1)
        self.setups.append(row);self.active=None;return row
    def cancel_monitors(self,reason,index):
        for row in self.monitors:self.finish_monitor(row,reason,index)
        self.monitors=[]
    def reset_gap(self,index):
        self.close('GAP_RESET',index);self.cancel_monitors('GAP_RESET',index)
    def finish_monitor(self,row,reason,index,features=None):
        r=dict(candidate_id=self.rule['candidate_id'],setup_start_utc=row['setup_start_utc'],
            expiry_close_utc=row['termination_close_utc'],started_in_report_window=row['started_in_report_window'],
            observation_status=reason,observation_close_utc=self.stamp(index,True),bar_age=index-row['start_index']+1,
            observation_only=True,additional_strategy_signal=False,priced=False)
        if features is not None:
            r.update(body_atr=features['body_atr'],h1_distance_atr=features['h1_distance_atr'],
                inherited_low=row['extreme'],within_12_bar_lifetime=r['bar_age']<=12,
                within_16_bar_lifetime=r['bar_age']<=16,
                implied_stop=float(e.broker_price('AUD_USD',row['extreme']-10*float(e.tick('AUD_USD')))))
        self.late.append(r)
    def observe(self,current,previous,trend,current_ma,index,features,accept):
        remaining=[]
        for row in self.monitors:
            age=index-row['start_index']+1
            if not trend:self.finish_monitor(row,'TREND_RESET',index);continue
            if age>16:self.finish_monitor(row,'NO_PASS_THROUGH_16',index);continue
            row['extreme']=min(row['extreme'],current['l'])
            trigger=exact_engulf(previous,current,'BUY') and current['c']>current_ma
            if trigger and accept(features):self.finish_monitor(row,'QUALIFIED_AFTER_EXPIRY',index,features);continue
            # Include bar 16, then stop. Never look beyond the declared horizon.
            if age==16:self.finish_monitor(row,'NO_PASS_THROUGH_16',index);continue
            remaining.append(row)
        self.monitors=remaining
    def step(self,state,current,previous,trend,previous_ma,current_ma,atr_value,index,features,accept):
        old=state['setup']
        if old is not None and not trend:self.close('TREND_RESET',index)
        elif old is not None and index-old['index']>=8:
            expired=self.close('EXPIRED',index)
            e.require(expired is not None,'Audit lost active setup at expiry')
            self.monitors.append(dict(expired,extreme=old['extreme']))
        self.observe(current,previous,trend,current_ma,index,features,accept)
        chosen,events=evaluate(state,current,previous,trend,previous_ma,current_ma,atr_value,index,features,accept)
        setup=chosen['setup'] if chosen else state['setup']
        if self.active is None and setup is not None:self.active=self.new(setup['index'],setup)
        triggered=chosen is not None or events.get('filter_failed_trigger',0)>0
        if triggered:
            e.require(self.active is not None,'Trigger without tracked setup')
            body_ok=features['body_atr']>=self.rule['body_min'];dist_ok=features['h1_distance_atr']<=self.rule['distance_max']
            reason='ACCEPTED' if chosen else ('BODY_ONLY' if dist_ok else 'DISTANCE_ONLY' if body_ok else 'BOTH')
            self.triggers.append(dict(candidate_id=self.rule['candidate_id'],setup_start_utc=self.active['setup_start_utc'],
                signal_close_utc=self.stamp(index,True),in_report_window=self.in_window(index),
                bar_age=index-self.active['start_index']+1,outcome=reason,body_atr=features['body_atr'],
                h1_distance_atr=features['h1_distance_atr'],body_shortfall_atr=max(0.,self.rule['body_min']-features['body_atr']),
                distance_excess_atr=max(0.,features['h1_distance_atr']-self.rule['distance_max'])))
            if chosen:self.close('ACCEPTED',index)
            else:
                self.active['failed_trigger_count']+=1
                self.active[{'BODY_ONLY':'body_only_failures','DISTANCE_ONLY':'distance_only_failures','BOTH':'both_failures'}[reason]]+=1
                b=self.active['best_failed_body_atr'];d=self.active['smallest_failed_distance_atr']
                self.active['best_failed_body_atr']=features['body_atr'] if b is None else max(b,features['body_atr'])
                self.active['smallest_failed_distance_atr']=features['h1_distance_atr'] if d is None else min(d,features['h1_distance_atr'])
        return chosen,events
    def finish(self,index):
        self.close('RIGHT_CENSORED',index);self.cancel_monitors('RIGHT_CENSORED',index)
    def annotate(self,signals):
        accepted={s['signal_close_utc']:s for s in signals}
        for row in self.late:
            if row['observation_status']!='QUALIFIED_AFTER_EXPIRY':continue
            signal=accepted.get(row['observation_close_utc'])
            row['baseline_signal_same_close']=signal is not None
            row['baseline_same_inherited_stop']=bool(signal and signal['stop']==row['implied_stop'])
        # A first late pass is counted once per expired setup, independently of
        # other expired setups and fresh setups. This cannot be added to signals.


def summarize(audit,start,end,label):
    lo,hi=int(e.dt(start).timestamp()),int(e.dt(end).timestamp())
    inside=lambda t:lo<=int(e.dt(t).timestamp())<hi
    setups=[r for r in audit.setups if inside(r['setup_start_utc']) and r['started_in_report_window']]
    # Setup cohorts are assigned by their touch. Trigger cohorts are assigned
    # by trigger close; they have a different denominator, stated explicitly.
    triggers=[r for r in audit.triggers if inside(r['signal_close_utc']) and r['in_report_window']]
    expired=[r for r in setups if r['termination']=='EXPIRED'];keys={r['setup_start_utc'] for r in expired}
    late=[r for r in audit.late if r['setup_start_utc'] in keys]
    first=[r for r in late if r['observation_status']=='QUALIFIED_AFTER_EXPIRY']
    c=Counter(r['termination'] for r in setups);f=Counter(r['outcome'] for r in triggers)
    years=(e.dt(end)-e.dt(start)).total_seconds()/31557600.
    return dict(candidate_id=audit.rule['candidate_id'],window_id=label,start=start,end=end,
        setup_cohort='TOUCH_TIME; FOLLOWED_THROUGH_FROZEN_CUTOFF_NOT_TRUNCATED_AT_ERA_END',
        trigger_cohort='TRIGGER_CLOSE_TIME',setups_started=len(setups),accepted_setups=c['ACCEPTED'],
        expired_setups=c['EXPIRED'],trend_reset_setups=c['TREND_RESET'],gap_reset_setups=c['GAP_RESET'],
        right_censored_setups=c['RIGHT_CENSORED'],
        setup_fates_reconcile=sum(c.values())==len(setups),qualifying_signal_closes=f['ACCEPTED'],
        qualifying_signal_closes_per_year=f['ACCEPTED']/years if years else None,
        failed_trigger_attempts=len(triggers)-f['ACCEPTED'],body_only_failed_attempts=f['BODY_ONLY'],
        distance_only_failed_attempts=f['DISTANCE_ONLY'],both_failed_attempts=f['BOTH'],
        expired_with_no_failed_engulf=sum(r['failed_trigger_count']==0 for r in expired),
        expired_with_prior_failed_engulf=sum(r['failed_trigger_count']>0 for r in expired),
        expired_first_late_pass_by_12=sum(r['within_12_bar_lifetime'] for r in first),
        expired_first_late_pass_by_16=len(first),
        expired_late_pass_pct=100*len(first)/len(expired) if expired else None,
        late_passes_also_baseline_signal_same_close=sum(r['baseline_signal_same_close'] for r in first),
        late_passes_with_no_baseline_signal_same_close=sum(not r['baseline_signal_same_close'] for r in first),
        late_observation_termination_counts=dict(Counter(r['observation_status'] for r in late)),
        late_observations_reconcile=len(late)==len(expired),
        changed_lifetime_strategy_replayed=False,return_or_edge_estimated=False)
