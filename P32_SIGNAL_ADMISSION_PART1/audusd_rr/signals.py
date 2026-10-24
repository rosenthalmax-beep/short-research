"""RR changes target geometry only; every account still reruns actual admission."""
import copy
from pullback_lab import core as e
from audusd_lifetime.signals import generate_grid as causal_generate
from .config import candidates,base_id


def generate_grid(m15,h1,start,end,ready,progress=lambda **x:None,rules=None):
    rules=candidates() if rules is None else rules
    variants=[r for r in rules if r['candidate_id']!='BASELINE']
    controls=[r for r in variants if r['rr']==3.]
    e.require(len(controls)==6 and len({r['candidate_id'] for r in controls})==6,'Six fixed RR3 qualification controls required')
    raw_rules=[{k:v for k,v in r.items() if k!='rr'} for r in controls]
    if any(r['candidate_id']=='BASELINE' for r in rules):
        raw_rules=[dict(candidate_id='BASELINE',kind='BASELINE',distance_max=None,body_min=None)]+raw_rules
    raw,counts=causal_generate(m15,h1,start,end,ready,progress,rules=raw_rules)
    result={};new_counts={}
    if 'BASELINE' in raw:result['BASELINE']=raw['BASELINE'];new_counts['BASELINE']=counts['BASELINE']
    for rule in variants:
        cid=rule['candidate_id'];source_id=base_id(rule['distance_max'],rule['lifetime_bars'])
        if rule['rr']==3.:
            e.require(cid==source_id,'RR3 ID changed');result[cid]=raw[source_id]
        else:
            out=[]
            for original in raw[source_id]:
                signal=copy.deepcopy(original)
                geometry=e.levels('AUD_USD','BUY',signal['reference_entry'],signal['stop'],rule['rr'],15)
                signal.update(candidate_id=cid,rr=rule['rr'],target=float(geometry['target']),strategy_id='AUDUSD_PB2_'+cid)
                identity=dict(pair='AUD_USD',side='BUY',control=signal['control'],close=int(e.dt(signal['entry_utc']).timestamp()),candidate_id=cid)
                signal['signal_key']=e.sha(e.canon(identity));out.append(signal)
            result[cid]=out
        new_counts[cid]=copy.deepcopy(counts[source_id])
    return result,new_counts


def neutral(signal):
    return {k:v for k,v in signal.items() if k not in ('candidate_id','signal_key','strategy_id','rr','target')}


def qualification_parity(signals):
    checks=[]
    for rule in candidates():
        cid=rule['candidate_id'];control_id=base_id(rule['distance_max'],rule['lifetime_bars'])
        actual=signals[cid];reference=signals[control_id]
        e.require(e.canon([neutral(s) for s in actual])==e.canon([neutral(s) for s in reference]),'RR changed qualification times/stops/features: '+cid)
        e.require(len({s['signal_key'] for s in actual})==len(actual),'Duplicate RR signal keys')
        for s in actual:
            expected=e.levels('AUD_USD','BUY',s['reference_entry'],s['stop'],rule['rr'],15)
            e.require(s['rr']==rule['rr'] and s['target']==float(expected['target']),'RR target geometry mismatch')
        checks.append(dict(candidate_id=cid,control_candidate_id=control_id,status='PASS',signals=len(actual)))
    return dict(status='PASS',identical_qualification_times_stops_features=True,targets_repriced_not_scaled=True,checks=checks)
