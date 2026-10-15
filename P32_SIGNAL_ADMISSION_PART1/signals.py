"""Recover observed-bar qualifications before any position/admission filter."""
import json, math
from datetime import timedelta
import execution as e
import locked_rules as rules
import m15_adapter as m15
from history import START,END,atr,sma,Htf,ReadyDaily,prepare_long_daily,prepare_short_daily

_old_long=rules.previous_daily_values
_old_short=rules.previous_short_daily_values
_old_extreme=rules.m15_previous_extreme

def previous_long(t,state,config):
    return state.at(t) if isinstance(state,ReadyDaily) else _old_long(t,state,config)

def previous_short(t,state):
    return state.at(t) if isinstance(state,ReadyDaily) else _old_short(t,state)

def previous_extreme(candles,index,lookback,field,want_max):
    if hasattr(candles,'previous_extreme'):return candles.previous_extreme(index,lookback,field,want_max)
    return _old_extreme(candles,index,lookback,field,want_max)

rules.previous_daily_values=previous_long
rules.previous_short_daily_values=previous_short
rules.m15_previous_extreme=previous_extreme
m15.m15_previous_extreme=previous_extreme

def clean(value):
    if hasattr(value,'tzinfo'):return e.stamp(value)
    if isinstance(value,dict):return {k:clean(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)):return [clean(v) for v in value]
    if isinstance(value,float):e.require(math.isfinite(value),'Nonfinite signal diagnostic')
    return value

def normalize(pair,side,tf,result,config=None,priority=0):
    if config is None:
        sid=result['strategy_id'];reference=result['entry'];stop=result['stop'];rr=result['rr'];trigger=result['trigger_id']
    else:
        sid=config.get('strategy_id',pair+'_H1_'+('LONG' if side=='BUY' else 'SHORT'))
        reference=result['close'];stop=result['low']-config['stop_buffer_ticks']*config['tick_size'] if side=='BUY' else result['high']+config['stop_buffer_ticks']*config['tick_size']
        rr=config['reward_risk'];trigger=result.get('trigger_id',config.get('trigger_id','H1_LONG' if side=='BUY' else 'H1_SHORT'))
    levels=e.levels(pair,side,reference,stop,rr)
    close=result['signal_close_utc'];start=result['signal_start_utc']
    return dict(signal_key=sid+'-'+str(int(close.timestamp()*1000))+'-'+str(trigger),strategy_id=sid,pair=pair,side=side,timeframe=tf,
                signal_start_utc=e.stamp(start),entry_utc=e.stamp(close),reference_entry=float(levels['reference_entry']),stop=float(levels['stop']),target=float(levels['target']),rr=rr,
                same_strategy_trigger_priority=priority,diagnostics=clean(result),
                qualification_basis='CURRENT_LOCKED_PREDICATES_FULL_OBSERVED_HISTORY_SEED',signal_was_position_filtered=False)

def validate_registry(registry):
    known={x['live_sid']:x for x in registry}
    expected={p+'_H1_LONG' for p in rules.STRATEGIES if 'strategy_id' not in rules.STRATEGIES[p]}
    expected.update(c['strategy_id'] for c in rules.STRATEGIES.values() if 'strategy_id' in c)
    expected.update(c.get('strategy_id',p+'_H1_SHORT') for p,c in rules.SHORT_STRATEGIES.items())
    expected.update(rules.M15_STRATEGY_IDS)
    e.require(expected==set(known) and len(expected)==32,'Locked rule registry does not match all 32 live IDs')
    return known

def recover_pair(pair,h1,daily,m15_bars=None,h4=None,progress=lambda **x:None):
    found=[];audit=[]
    h1_atr=atr(h1,chf=pair=='EUR_CHF')
    long_state=None;short_state=None
    if pair in rules.STRATEGIES:long_state=prepare_long_daily(daily,rules.STRATEGIES[pair])
    if pair in rules.SHORT_STRATEGIES and rules.SHORT_STRATEGIES[pair].get('requires_daily_state',True) and rules.SHORT_STRATEGIES[pair].get('signal_family') not in ('audusd_two_trigger','compression_breakdown'):
        short_state=prepare_short_daily(daily,rules.SHORT_STRATEGIES[pair])
    for i in range(1,len(h1)):
        current=h1[i]
        if current['time']<START or current['time']+timedelta(hours=1)>END:continue
        if pair in rules.STRATEGIES:
            result=rules.evaluate_signal_at_index(pair,h1,h1_atr,i,long_state)
            if result and result.get('qualified'):found.append(normalize(pair,'BUY','H1',result,rules.STRATEGIES[pair]))
        if pair in rules.SHORT_STRATEGIES:
            result=rules.evaluate_short_signal_at_index(pair,h1,h1_atr,i,short_state)
            if result and result.get('qualified'):found.append(normalize(pair,'SELL','H1',result,rules.SHORT_STRATEGIES[pair]))
        if i%2000==0:progress(phase='RECOVER_RAW_SIGNALS',pair=pair,timeframe='H1',bars_evaluated=i,bars_required=len(h1),signals_found=len(found))
    if m15_bars is not None:
        values=atr(m15_bars);mean20=sma(values,20)
        h1_state=Htf(h1,ratio=pair=='USD_JPY') if pair in {'USD_JPY','USD_CAD','EUR_GBP','EUR_JPY','AUD_USD'} else None
        h4_state=Htf(h4) if pair in {'USD_JPY','USD_CAD'} else None
        daily_state=Htf(daily) if pair in {'USD_JPY','USD_CAD'} else None
        for i in range(199,len(m15_bars)):
            t=m15_bars[i]['time']
            if t<START or t+timedelta(minutes=15)>END:continue
            result=m15.evaluate_m15_at_index(pair,m15_bars,i,values,mean20,h1_state.at(t) if h1_state else None,h4_state.at(t) if h4_state else None,daily_state.at(t) if daily_state else None)
            for priority,signal in enumerate(result['signals']):found.append(normalize(pair,signal['side'],'M15',signal,priority=priority))
            if i%5000==0:progress(phase='RECOVER_RAW_SIGNALS',pair=pair,timeframe='M15',bars_evaluated=i,bars_required=len(m15_bars),signals_found=len(found))
    found.sort(key=lambda r:(r['entry_utc'],r['strategy_id'],r['same_strategy_trigger_priority']))
    e.require(len({x['signal_key'] for x in found})==len(found),'Duplicate regenerated signal keys')
    return found

def reconcile(data,recovered):
    index={}
    for signal in recovered:index.setdefault((signal['strategy_id'],signal['signal_start_utc']),[]).append(signal)
    aliases={r['archived_sid']:r['live_sid'] for r in data['registry']}
    archived=[]
    for i,(r,g) in enumerate(zip(data['reference']['book'],data['reference']['geometry'])):
        candidates=index.get((aliases[r['sid']],r['signal']),[])
        same=[s for s in candidates if float(s['rr'])==float(r['rr'])]
        actual=next((s for s in same if s['reference_entry']==float(e.broker_price(r['pair'],g['reference_entry'])) and s['stop']==float(e.broker_price(r['pair'],g['stop']))),None)
        archived.append(dict(index=i,archived_sid=r['sid'],live_sid=aliases[r['sid']],signal_start_utc=r['signal'],
                             status='SIGNAL_AND_ROUNDED_ENTRY_STOP_RR_MATCH' if actual else 'SAME_SIGNAL_DIFFERENT_GEOMETRY_OR_RR' if candidates else 'ARCHIVED_SIGNAL_NOT_REPRODUCED',
                             generated_signal_keys=[s['signal_key'] for s in candidates],archived_reference=g['reference_entry'],archived_stop=g['stop'],archived_rr=r['rr']))
    old_raw=[]
    for r in data.get('old_raw_streams',{}).get('eurusd_raw',[]):
        candidates=index.get((aliases[r['sid']],r['signal']),[])
        match=any(s['rr']==float(r['rr']) and s['reference_entry']==float(e.broker_price(r['pair'],r['reference'])) and s['stop']==float(e.broker_price(r['pair'],r['stop'])) for s in candidates)
        old_raw.append(dict(strategy_id=r['sid'],signal_start_utc=r['signal'],status='SIGNAL_AND_GEOMETRY_MATCH' if match else 'SAME_TIME_DIFFERENCE' if candidates else 'NOT_REPRODUCED'))
    return archived,old_raw
