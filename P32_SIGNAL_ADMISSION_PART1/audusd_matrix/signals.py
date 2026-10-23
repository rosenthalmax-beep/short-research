"""Independent causal state machines; filters act BEFORE consuming a pullback."""
import math
from collections import Counter
from pullback_lab import core as e
from pullback_lab.signals import ema,atr,exact_engulf,RULES

DISTANCE=(.75,1.,1.25)
BODY=(.5,.75,1.)


def candidates():
    result=[dict(candidate_id='BASELINE',kind='BASELINE',distance_max=None,body_min=None)]
    for value in DISTANCE:result.append(dict(candidate_id='DIST_'+str(value),kind='DISTANCE_ONLY',distance_max=value,body_min=None))
    for value in BODY:result.append(dict(candidate_id='BODY_'+str(value),kind='BODY_ONLY',distance_max=None,body_min=value))
    for distance in DISTANCE:
        for body in BODY:
            result.append(dict(candidate_id='M_D'+str(distance)+'_B'+str(body),kind='MATRIX',distance_max=distance,body_min=body))
    return result


def passes(rule,features):
    return (rule['distance_max'] is None or features['h1_distance_atr']<=rule['distance_max']) and (rule['body_min'] is None or features['body_atr']>=rule['body_min'])


def evaluate(state,current,previous,trend,previous_ma,current_ma,atr_value,index,features,accept):
    """One completed-bar transition. No price after this close is inspected."""
    events=Counter();setup=state['setup'];armed=state['armed']
    if not trend:setup=None;armed=False
    else:
        if setup is not None and index-setup['index']>=8:
            events['expired']+=1;setup=None;armed=False
        if setup is None and armed and current['l']<=previous_ma:
            setup=dict(index=index,extreme=current['l'],area=previous_ma,start_atr=atr_value)
            armed=False
        if setup is not None:setup['extreme']=min(setup['extreme'],current['l'])
    trigger=bool(trend and setup is not None and exact_engulf(previous,current,'BUY') and current['c']>current_ma)
    selected=None
    if trigger:
        details=dict(features,duration_bars=index-setup['index']+1,
                     depth_atr=max(0.,setup['area']-setup['extreme'])/setup['start_atr'],pullback_qualified=True)
        if accept(details):
            selected=dict(setup=setup.copy(),features=details);events['accepted']+=1
            setup=None;armed=False
        else:
            # Keep the original touch and all intervening lows. The next exact
            # engulf can qualify until expiry/gap/trend failure. Later execution
            # rejection is deliberately NOT a reason to reopen an accepted setup.
            events['filter_failed_trigger']+=1
    elif trend and setup is None:armed=current['l']>current_ma
    state.update(setup=setup,armed=armed)
    return selected,events


def generate_grid(m15,h1,start,end,ready,progress=lambda **x:None,rules=None):
    rules=candidates() if rules is None else rules
    result={r['candidate_id']:[] for r in rules};counts={r['candidate_id']:Counter() for r in rules}
    states={r['candidate_id']:dict(setup=None,armed=False) for r in rules}
    m,h=m15.values['mid'],h1.values['mid'];ma=ema(m['c'],20);av=atr(m15)
    hm={n:ema(h['c'],n) for n in (50,100,200)};ha=atr(h1);hi=-1
    lo,through,eligible=int(e.dt(start).timestamp()),int(e.dt(end).timestamp()),int(e.dt(ready).timestamp())
    for i in range(1,len(m15)):
        t=m15.times[i]+900
        while hi+1<len(h1) and h1.times[hi+1]+3600<=t:hi+=1
        if t<eligible:continue
        if m15.times[i]!=m15.times[i-1]+900:
            for state in states.values():state.update(setup=None,armed=False)
            for counter in counts.values():counter['gap_reset']+=1
            continue
        if not math.isfinite(av[i]) or av[i]<=0 or not math.isfinite(ma[i-1]):continue
        current={k:m[k][i] for k in 'ohlc'};previous={k:m[k][i-1] for k in 'ohlc'}
        fresh=hi>=102 and math.isfinite(hm[100][hi-3]) and ha[hi]>0 and 0<=t-(h1.times[hi]+3600)<3600
        trend=bool(fresh and h['c'][hi]>hm[100][hi] and hm[100][hi]>hm[100][hi-3])
        span=current['h']-current['l']
        features=dict(body_atr=abs(current['c']-current['o'])/av[i],range_atr=span/av[i],
            close_location=(current['c']-current['l'])/span if span else None,h1_trend=trend,
            h1_source_start_utc=e.stamp(e.datetime.fromtimestamp(h1.times[hi],e.UTC)) if fresh else None,
            h1_source_close_utc=e.stamp(e.datetime.fromtimestamp(h1.times[hi]+3600,e.UTC)) if fresh else None,
            h1_slope_atr=(hm[100][hi]-hm[100][hi-3])/ha[hi] if fresh else None,
            h1_distance_atr=(h['c'][hi]-hm[100][hi])/ha[hi] if fresh else None,
            h1_ema50_200_aligned=bool(fresh and math.isfinite(hm[200][hi]) and hm[50][hi]>hm[200][hi]))
        for rule in rules:
            cid=rule['candidate_id'];chosen,events=evaluate(states[cid],current,previous,trend,ma[i-1],ma[i],av[i],i,features,lambda f,r=rule:passes(r,f))
            counts[cid].update(events)
            if chosen is None or not lo<=t<through:continue
            setup=chosen['setup'];stop=float(e.broker_price('AUD_USD',setup['extreme']-10*float(e.tick('AUD_USD'))))
            geometry=e.levels('AUD_USD','BUY',current['c'],stop,3.,15)
            identity=dict(pair='AUD_USD',side='BUY',control='TREND_PULLBACK_ENGULF',close=t)
            if cid!='BASELINE':identity['candidate_id']=cid
            feature=dict(chosen['features'],stop_atr=(float(geometry['reference_entry'])-stop)/av[i])
            result[cid].append(dict(candidate_id=cid,signal_key=e.sha(e.canon(identity)),
                strategy_id='PB1_AUD_USD_BUY_TREND_PULLBACK_ENGULF' if cid=='BASELINE' else 'AUDUSD_PB2_'+cid,
                control='TREND_PULLBACK_ENGULF',pair='AUD_USD',side='BUY',timeframe='M15',same_strategy_trigger_priority=0,
                signal_start_utc=e.stamp(e.datetime.fromtimestamp(m15.times[i],e.UTC)),
                signal_close_utc=e.stamp(e.datetime.fromtimestamp(t,e.UTC)),entry_utc=e.stamp(e.datetime.fromtimestamp(t,e.UTC)),
                rr=3.,reference_entry=float(geometry['reference_entry']),stop=stop,target=float(geometry['target']),atr14=av[i],features=feature,
                pullback_start_utc=e.stamp(e.datetime.fromtimestamp(m15.times[setup['index']],e.UTC))))
        if i%10000==0:progress(phase='REGENERATE_ALL_CANDIDATES',bars_evaluated=i,bars_required=len(m15),generated_signals=sum(len(s) for s in result.values()))
    return result,{k:dict(v) for k,v in counts.items()}
