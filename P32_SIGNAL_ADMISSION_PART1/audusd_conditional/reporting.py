import gzip,json
from audusd_matrix import io as x
from audusd_rr.reporting import comparisons as unused
from .config import candidates,ANCHORS,FEATURES

def comparisons(accounts):
 by={(r['candidate_id'],r['window_id'],r['financing_mode']):r for r in accounts};out=[]
 for r in accounts:
  if r.get('kind')!='ONE_FEATURE':continue
  a=by.get((r['anchor_id'],r['window_id'],r['financing_mode']))
  if a is None:continue
  row={k:r[k] for k in ('candidate_id','anchor_id','feature','operator','threshold','window_id','financing_mode')}
  for k in ('closed_trades','conditional_nav_cagr_pct','sampled_mid_nav_drawdown_pct','mean_net_r_actual_entry_risk','mean_net_r_without_best_five','conditional_ending_nav_gbp'):
   row[k+'_delta']=r[k]-a[k] if r.get(k) is not None and a.get(k) is not None else None
  out.append(row)
 return out

def regions(rules,accounts):
 by={(r['candidate_id'],r['window_id'],r['financing_mode']):r for r in accounts};out=[]
 for anchor in ANCHORS:
  for feature,(_,levels,_) in FEATURES.items():
   members=[r['candidate_id'] for r in rules if r.get('anchor_id')==anchor and r.get('feature')==feature]
   rows=[by.get((c,w,m)) for c in members for w in ('FULL_HISTORY','RECENT_2021_TO_CUTOFF') for m in ('FROZEN_14OCT2026_RATES','ADVERSE_6PCT_ANNUAL')]
   complete=all(r is not None for r in rows)
   positive=complete and all(r['mean_net_r_actual_entry_risk'] is not None and r['mean_net_r_actual_entry_risk']>0 for r in rows)
   out.append(dict(anchor_id=anchor,feature=feature,thresholds=levels,members=members,all_full_recent_available=complete,all_neighbours_positive_full_recent_both_fundings=positive,conditional_plateau_screen_pass=False,interpretation='DISCOVERY_DIAGNOSTIC_ONLY; NO_COMBINED_PLATEAU_OR_APPROVAL'))
 return out

def write_attribution(job,dirs):
 by={p.name:p for p in dirs};out=[]
 for rule in candidates():
  if rule.get('kind')!='ONE_FEATURE' or rule['candidate_id'] not in by or rule['anchor_id'] not in by:continue
  def read(cid,w,m):
   p=by[cid]/'accounts'/(w+'__'+m+'.json.gz')
   return json.loads(gzip.decompress(p.read_bytes()))['result']
  for w in ('FULL_HISTORY','RECENT_2021_TO_CUTOFF'):
   for mode in ('FROZEN_14OCT2026_RATES','ADVERSE_6PCT_ANNUAL'):
    a,b=read(rule['anchor_id'],w,mode),read(rule['candidate_id'],w,mode)
    # Full raw payloads are retained; compare on entry/stop/target, not study-specific signal IDs.
    def signals(cid):
     with gzip.open(by[cid]/'signals.jsonl.gz','rt') as f:return {s['signal_key']:s for s in map(json.loads,f)}
    sa,sb=signals(rule['anchor_id']),signals(rule['candidate_id'])
    def keyed(result,ss):
     out={}
     for t in result['trades']:
      s=ss[t['signal_key']];key=(s['entry_utc'],s['reference_entry'],s['stop'],s['target'])
      if key in out:raise ValueError('Duplicate account trade geometry')
      out[key]=t
     return out
    aa,bb=keyed(a,sa),keyed(b,sb)
    for label,keys in [('REMOVED',aa.keys()-bb.keys()),('NEW',bb.keys()-aa.keys()),('SHARED',aa.keys()&bb.keys())]:
     out.append(dict(candidate_id=rule['candidate_id'],anchor_id=rule['anchor_id'],window_id=w,financing_mode=mode,population=label,count=len(keys),
      anchor_net_r=sum(aa[k]['net_r_actual_entry_risk'] or 0 for k in keys if k in aa),candidate_net_r=sum(bb[k]['net_r_actual_entry_risk'] or 0 for k in keys if k in bb),
      anchor_net_cash_gbp=sum(aa[k]['net_pl_gbp'] for k in keys if k in aa),candidate_net_cash_gbp=sum(bb[k]['net_pl_gbp'] for k in keys if k in bb),
      scope='CLOSED_ACCOUNT_TRADES_MATCHED_ON_ENTRY_REFERENCE_STOP_TARGET; SHARED_RETURNS_CAN_DIFFER_WITH_SIZING_AND_FUNDING'))
 x.atomic(job.work/'trade_attribution.csv',x.csv_bytes(out))

def review_text(done,accounts):
 return f"""# AUD/USD conditional-feature discovery
{done}/{len(candidates())} candidates; {accounts}/{len(candidates())*14} accounts.
Six exact RR3 controls (84 accounts), one broader previously tested eight-bar control,
and 72 one-feature variants across three predeclared anchors. RR3 fixed.
No feature combinations, lifetime search, timing optimization or automatic selection.
Read protocol.json for precise causal feature definitions. Missing features reject;
filter failures preserve the setup for later qualification. Accounts regenerate
signals independently and replay all entries, costs, financing, blocking and sizing.
Candidate screens retain the old sample thresholds; diagnostic neighbourhoods do
not approve strategies. All history has been examined and uncertainty remains.
Review full/recent, both funding assumptions, marginal trades and neighbouring
thresholds before designing any two-factor study. Alternative trigger comes later.
"""
