"""Matched trigger comparison and frozen benchmark attribution; no grid approval."""
import gzip,json
from audusd_matrix import io as x
from pullback_lab import core as e
from .config import candidates,MODES,ENGULF,RESUMPTION,LEGACY_BROAD,LEGACY_SINGLE,LEGACY_QUALITY
METRICS=('closed_trades','conditional_nav_cagr_pct','sampled_mid_nav_drawdown_pct','mean_net_r_actual_entry_risk','mean_net_r_without_best_five','conditional_ending_nav_gbp','net_r_sum_actual_entry_risk')
def references(rule):
 if rule['candidate_id']==ENGULF:return [('REMOVE_BODY_FLOOR',LEGACY_BROAD)]
 if rule['candidate_id']==RESUMPTION:return [('MATCHED_TRIGGER',ENGULF),('LEGACY_BROAD',LEGACY_BROAD),('LEGACY_SINGLE_FILTER',LEGACY_SINGLE),('LEGACY_QUALITY',LEGACY_QUALITY)]
 return []
def regions(rules,accounts):return []
def comparisons(accounts):
 by={(r['candidate_id'],r['window_id'],r['financing_mode']):r for r in accounts};out=[]
 for r in accounts:
  for label,cid in references(r):
   a=by.get((cid,r['window_id'],r['financing_mode']))
   if a is None:continue
   row=dict(candidate_id=r['candidate_id'],comparison=label,reference_candidate_id=cid,window_id=r['window_id'],financing_mode=r['financing_mode'],
    interpretation='TRIGGER_ONLY_SAME_CONTEXT_AND_NO_BODY_FLOOR' if label=='MATCHED_TRIGGER' else 'BENCHMARK_COMPARISON_WITH_OTHER_RULE_DIFFERENCES')
   for k in METRICS:row[k+'_delta']=r[k]-a[k] if r.get(k) is not None and a.get(k) is not None else None
   row['closed_trade_retention_ratio']=r['closed_trades']/a['closed_trades'] if a['closed_trades'] else None
   row['scope']='CONDITIONAL_FRESH_ACCOUNT_COMPARISON; NOT_INCREMENTAL_LIVE32_RETURN'
   out.append(row)
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
 return f'''# AUD/USD broad resumption mechanism pass 1
{done}/5 configurations and {accounts}/70 fresh GBP100 accounts.
Three old benchmarks must match all 42 saved account payloads plus exact signals
and opportunities before either new broad mechanism runs. 4086 original baseline
signals must also reproduce. Existing source/accounting modules are unchanged.

The primary comparison changes ONLY the trigger. Both new candidates share
completed H1 uptrend/EMA100 context, a 1.25 H1 ATR distance cap, M15 EMA20 touch,
16-bar setup lifetime, RR3, 15-tick entry bound, 10-tick stop buffer, 1% NAV risk,
whole-unit GBP100 accounting and both original funding assumptions. Neither has
a body-size, previous-close or pre-touch-momentum filter.

Broad resumption: completed M15 close > open, close > previous completed M15
HIGH, and close > current EMA20. It does not require exact body engulfing or a
previous close below EMA20. Broad engulfing: exact original bullish body engulf
and close > current EMA20. This matched engulfing account isolates the trigger
change from removing the old body floor. A trigger difference can consume the
setup earlier, change stops and displace later entries. Signals and accounts
are regenerated independently, not intersected or combined.

control_comparisons.csv and trade_attribution.csv include the matched-trigger
comparison, the removal-of-body-floor comparison and legacy benchmarks. Only
MATCHED_TRIGGER isolates the trigger. Attribution covers removed/new/shared
closed trades in all reset windows and funding cases. Shared sizing may change.
signal_overlap.csv compares raw qualification time and geometry overlap; this
is distinct from closed account-trade overlap and portfolio overlap.

feature_diagnostics.csv describes PREDECLARED coarse bins using RAW independent
qualification outcomes, with missing-feature and unresolved/nonclosed/rejected
counts visible. Feature membership is causal, while outcomes follow to the
fixed study cutoff even when entry cohorts end earlier. These are not replayed
filter strategies or compounded accounts and report no CAGR. Do not pick an
optimum bin, stack filters or treat overlapping features as independent evidence.
feature_coverage.csv preserves each feature's raw denominator. Full/recent and
five reset account windows remain separate from diagnostic entry cohorts.

The original 200/60/20 samples, era coverage and best-five sensitivity screens
are unchanged. There is NO parameter grid or plateau test in this pass: a screen
pass is a research lead, not a plateau, independent validation or live admission.

All history was seen. Actual historical funding, margin and conversion schedules
are unknown; frozen 14-Oct-2026 rates and adverse 6% annual funding are modeled.
Source gaps, stale valuations, barrier-order assumptions and unknown-entry
omissions remain explicit. Sampled NAV DD is not an intrabar bound. Broad outcomes
and fixture reconciliation do not validate a combined live32 account. No orders
or activation. Review the broad mechanism first; only if justified proceed to
bounded single-feature account replays, interactions and neighbouring plateaus.
'''
