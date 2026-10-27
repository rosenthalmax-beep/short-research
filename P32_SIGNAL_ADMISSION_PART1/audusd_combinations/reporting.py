"""Compare combinations with both single filters; preserve weak eras and samples."""
import gzip,json,math
from pathlib import Path
from audusd_matrix import io as x
from audusd_matrix.reporting import account_screen
from pullback_lab import core as e
from .config import candidates,ANCHORS,CLOSE,MOMENTUM,MODES,combination_id

METRICS=('closed_trades','conditional_nav_cagr_pct','sampled_mid_nav_drawdown_pct','mean_net_r_actual_entry_risk','mean_net_r_without_best_five','conditional_ending_nav_gbp','net_r_sum_actual_entry_risk')
def references(rule):
 if rule['kind']=='COMBINATION':return [('UNFILTERED',rule['anchor_id']),('CLOSE_ONLY',rule['close_control']),('MOMENTUM_ONLY',rule['momentum_control'])]
 if rule.get('anchor_id'):return [('UNFILTERED',rule['anchor_id'])]
 return []

def comparisons(accounts):
 by={(r['candidate_id'],r['window_id'],r['financing_mode']):r for r in accounts};out=[]
 for r in accounts:
  for label,cid in references(r):
   a=by.get((cid,r['window_id'],r['financing_mode']))
   if a is None:continue
   row=dict(candidate_id=r['candidate_id'],anchor_id=r.get('anchor_id'),comparison=label,reference_candidate_id=cid,window_id=r['window_id'],financing_mode=r['financing_mode'])
   for k in METRICS:row[k+'_delta']=r[k]-a[k] if r.get(k) is not None and a.get(k) is not None else None
   row['closed_trade_retention_ratio']=r['closed_trades']/a['closed_trades'] if a['closed_trades'] else None
   row['scope']='CONDITIONAL_FRESH_ACCOUNT_COMPARISON; NOT_INCREMENTAL_LIVE32_RETURN'
   out.append(row)
 return out

def regions(rules,accounts):
 by={r['candidate_id']:[a for a in accounts if a['candidate_id']==r['candidate_id']] for r in rules};out=[]
 for anchor in ANCHORS:
  for i in range(2):
   for j in range(2):
    members=[combination_id(anchor,ii,jj) for ii in (i,i+1) for jj in (j,j+1)]
    screens=[dict(candidate_id=cid,**account_screen(by.get(cid,[]))) for cid in members]
    allrows=[a for cid in members for a in by.get(cid,[])]
    full=[a for a in allrows if a['window_id']=='FULL_HISTORY'];recent=[a for a in allrows if a['window_id']=='RECENT_2021_TO_CUTOFF']
    complete=all(len(by.get(cid,[]))==14 for cid in members)
    fullrecent=full+recent
    def minimum(rows,key):return min((a[key] for a in rows if a.get(key) is not None),default=None)
    def maximum(rows,key):return max((a[key] for a in rows if a.get(key) is not None),default=None)
    out.append(dict(anchor_id=anchor,close_low=CLOSE[i],close_high=CLOSE[i+1],momentum_low=MOMENTUM[j],momentum_high=MOMENTUM[j+1],
     member_candidates=members,member_count=4,all_member_accounts_complete=complete,member_screens=screens,
     conditional_plateau_screen_pass=complete and all(s['screen_pass'] for s in screens),
     all_neighbours_positive_full_recent_both_fundings=len(fullrecent)==16 and all(a['conditional_ending_nav_gbp']>100 for a in fullrecent),
     all_neighbours_positive_without_best_five=len(fullrecent)==16 and all(a['mean_net_r_without_best_five'] is not None and a['mean_net_r_without_best_five']>0 for a in fullrecent),
     min_full_closed_trades=minimum(full,'closed_trades'),min_recent_closed_trades=minimum(recent,'closed_trades'),
     full_cagr_min=minimum(full,'conditional_nav_cagr_pct'),full_cagr_max=maximum(full,'conditional_nav_cagr_pct'),
     recent_cagr_min=minimum(recent,'conditional_nav_cagr_pct'),recent_cagr_max=maximum(recent,'conditional_nav_cagr_pct'),
     full_worst_sampled_nav_drawdown_pct=minimum(full,'sampled_mid_nav_drawdown_pct'),recent_worst_sampled_nav_drawdown_pct=minimum(recent,'sampled_mid_nav_drawdown_pct'),
     min_positive_eras=min(s['positive_eras'] for s in screens),min_supported_eras=min(s['supported_eras'] for s in screens),
     account_lineage_uncertainty_present=any(s['uncertain_account_rows'] for s in screens),automatic_selection=False,full_portfolio_validated=False))
 return out

def write_attribution(job,dirs):
 by={p.name:p for p in dirs};signals_cache={};result_cache={};out=[]
 def signals(cid):
  if cid not in signals_cache:
   with gzip.open(by[cid]/'signals.jsonl.gz','rt') as f:signals_cache[cid]={s['signal_key']:s for s in map(json.loads,f)}
  return signals_cache[cid]
 def trades(cid,w,m):
  key=(cid,w,m)
  if key not in result_cache:
   result=json.loads(gzip.decompress((by[cid]/'accounts'/(w+'__'+m+'.json.gz')).read_bytes()))['result'];ss=signals(cid);items={}
   for t in result['trades']:
    s=ss[t['signal_key']];identity=(s['entry_utc'],s['reference_entry'],s['stop'],s['target'])
    e.require(identity not in items,'Duplicate closed account trade geometry')
    items[identity]=t
   result_cache[key]=items
  return result_cache[key]
 def add(rule,label,reference,w,m):
  aa,bb=trades(reference,w,m),trades(rule['candidate_id'],w,m)
  for population,keys in [('REMOVED',aa.keys()-bb.keys()),('NEW',bb.keys()-aa.keys()),('SHARED',aa.keys()&bb.keys())]:
   row=dict(candidate_id=rule['candidate_id'],anchor_id=rule.get('anchor_id'),comparison=label,reference_candidate_id=reference,window_id=w,financing_mode=m,population=population,count=len(keys))
   for owner,items in [('reference',aa),('candidate',bb)]:
    chosen=[items[k] for k in keys if k in items];values=[t['net_r_actual_entry_risk'] for t in chosen if t['net_r_actual_entry_risk'] is not None]
    row[owner+'_net_r']=sum(values);row[owner+'_unmeasured_r_count']=len(chosen)-len(values);row[owner+'_net_cash_gbp']=sum(t['net_pl_gbp'] for t in chosen)
   row['scope']='CLOSED_ACCOUNT_TRADES; MATCHED_BY_ENTRY_REFERENCE_STOP_TARGET; SHARED_SIZING_CAN_DIFFER; CONDITIONAL_UNCERTAINTY_RETAINED'
   out.append(row)
 for rule in candidates():
  if rule['candidate_id'] not in by:continue
  for label,reference in references(rule):
   if reference not in by:continue
   # Include all five reset eras as well as full history and recent: no recent-only attribution.
   windows=[p.name.split('__')[0] for p in (by[rule['candidate_id']]/'accounts').glob('*__'+MODES[0]+'.json.gz')]
   for w in sorted(windows):
    for m in MODES:add(rule,label,reference,w,m)
 x.atomic(job.work/'trade_attribution.csv',x.csv_bytes(out))

def review_text(done,accounts):
 return f'''# AUD/USD bounded two-factor study
{done}/32 configurations and {accounts}/448 fresh GBP100 accounts.
Fourteen unchanged control configurations must match all 196 prior account
payloads before new combinations run. Entries, opportunity payloads and all
seven reset windows under both financing assumptions must reproduce exactly.

Eighteen combinations across two anchors: close caps 0.4/0.5/0.6 and pre-touch
four-hour rise minima 0/0.5/1.0 ATR. No new thresholds, timeframe changes,
RR search, timing search, lifetime search or three-feature combinations.

Each candidate regenerates causal qualification state; filter failure retains
the original EMA touch and all intervening lows. Acceptance consumes the setup
before broker rejection or position blocking. RR3, 15-tick entry bound,
10-tick stop buffer and 1% NAV risk stay fixed. Pricing, funding, GBP conversion,
whole units, margin and all uncertainty conventions are unchanged.

control_comparisons.csv compares each combination with its unfiltered anchor
and BOTH applicable single-feature accounts. trade_attribution.csv partitions
closed trades into removed/new/shared populations for every account window.
Shared trade cash and R can differ with account sizing; these are conditional
fresh-account comparisons, not marginal live32 portfolio returns.

plateau_regions.csv describes eight adjacent 2x2 regions. All four member
candidate screens must pass for a region to pass. Original 200/60/20 samples,
both financing cases, best-five sensitivity and era rules are unchanged.
Descriptive profitability flags are separate from approval. All overlapping
configurations and historical windows are correlated, and all history was seen.

Current broker terms and frozen funding are modeled assumptions, not historical
schedules. Source gaps, unknown-entry omissions, stale marks and barrier-order
ambiguity stay explicit. Sampled NAV DD is not worst intrabar equity. Neither
this run nor independent reproduction establishes live32 admission or new OOS.

No automatic selection or orders. Review, then one bounded local confirmation
only if supported; the separate resumption-trigger hypothesis comes afterwards.
'''
