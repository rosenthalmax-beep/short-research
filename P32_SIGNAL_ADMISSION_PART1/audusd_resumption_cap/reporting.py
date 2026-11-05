"""Bounded single-feature account comparisons and neighbouring-cap screens."""
import gzip,json
from audusd_matrix import io as x
from audusd_matrix.reporting import account_screen
from pullback_lab import core as e
from .config import candidates,MODES,CAPS,cap_id,RESUMPTION
METRICS=('closed_trades','conditional_nav_cagr_pct','sampled_mid_nav_drawdown_pct','mean_net_r_actual_entry_risk','mean_net_r_without_best_five','conditional_ending_nav_gbp','net_r_sum_actual_entry_risk')
def references(rule):
 if rule['kind']!='H1_DISTANCE_CAP':return []
 out=[('BROAD_RESUMPTION',RESUMPTION)];i=CAPS.index(rule['distance_max'])
 if i+1<len(CAPS):out.append(('LOOSER_NEIGHBOUR',cap_id(CAPS[i+1])))
 return out

def comparisons(accounts):
 by={(r['candidate_id'],r['window_id'],r['financing_mode']):r for r in accounts};out=[]
 for r in accounts:
  for label,cid in references(r):
   a=by.get((cid,r['window_id'],r['financing_mode']))
   if a is None:continue
   row=dict(candidate_id=r['candidate_id'],comparison=label,reference_candidate_id=cid,window_id=r['window_id'],financing_mode=r['financing_mode'],
    candidate_h1_cap=r['distance_max'],reference_h1_cap=a['distance_max'],interpretation='ONLY_H1_DISTANCE_CAP_DIFFERS; INDEPENDENT_CAUSAL_STATE_AND_ACCOUNT_REPLAY')
   for k in METRICS:row[k+'_delta']=r[k]-a[k] if r.get(k) is not None and a.get(k) is not None else None
   row['closed_trade_retention_ratio']=r['closed_trades']/a['closed_trades'] if a['closed_trades'] else None
   row['scope']='CONDITIONAL_FRESH_ACCOUNT_COMPARISON; NOT_INCREMENTAL_LIVE32_RETURN'
   out.append(row)
 return out

def regions(rules,accounts):
 by={r['candidate_id']:[a for a in accounts if a['candidate_id']==r['candidate_id']] for r in rules};out=[]
 for values in (CAPS[:2],CAPS[1:],CAPS):
  members=[cap_id(c) for c in values];screens=[dict(candidate_id=cid,**account_screen(by.get(cid,[]))) for cid in members]
  allrows=[a for cid in members for a in by.get(cid,[])];full=[a for a in allrows if a['window_id']=='FULL_HISTORY'];recent=[a for a in allrows if a['window_id']=='RECENT_2021_TO_CUTOFF'];fr=full+recent
  complete=all(len(by.get(cid,[]))==14 for cid in members)
  def low(rows,k):return min((a[k] for a in rows if a.get(k) is not None),default=None)
  def high(rows,k):return max((a[k] for a in rows if a.get(k) is not None),default=None)
  out.append(dict(region_kind='FULL_THREE_CAP_SET' if len(values)==3 else 'ADJACENT_CAP_PAIR',cap_values=list(values),cap_low=min(values),cap_high=max(values),
   member_candidates=members,member_count=len(members),all_member_accounts_complete=complete,member_screens=screens,
   conditional_plateau_screen_pass=complete and all(s['screen_pass'] for s in screens),
   all_neighbours_positive_full_recent_both_fundings=len(fr)==4*len(members) and all(a['conditional_ending_nav_gbp']>100 for a in fr),
   all_neighbours_positive_without_best_five=len(fr)==4*len(members) and all(a['mean_net_r_without_best_five'] is not None and a['mean_net_r_without_best_five']>0 for a in fr),
   min_full_closed_trades=low(full,'closed_trades'),min_recent_closed_trades=low(recent,'closed_trades'),
   full_cagr_min=low(full,'conditional_nav_cagr_pct'),full_cagr_max=high(full,'conditional_nav_cagr_pct'),recent_cagr_min=low(recent,'conditional_nav_cagr_pct'),recent_cagr_max=high(recent,'conditional_nav_cagr_pct'),
   full_worst_sampled_nav_drawdown_pct=low(full,'sampled_mid_nav_drawdown_pct'),recent_worst_sampled_nav_drawdown_pct=low(recent,'sampled_mid_nav_drawdown_pct'),
   min_positive_eras=min(s['positive_eras'] for s in screens),min_supported_eras=min(s['supported_eras'] for s in screens),
   account_lineage_uncertainty_present=any(s['uncertain_account_rows'] for s in screens),automatic_selection=False,full_portfolio_validated=False,
   scope='LOCAL_CONDITIONAL_SENSITIVITY_SCREEN; CORRELATED_NEIGHBOURS; NO_STATISTICAL_SIGNIFICANCE_OR_NEW_OOS'))
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
 return f'''# AUD/USD bounded H1-distance-cap confirmation
{done}/8 configurations and {accounts}/112 fresh GBP100 accounts.
All FIVE preceding configurations must reproduce all 70 saved account payloads,
exact signals and opportunities before new caps run. The original 4086 baseline
signals and all previous source/accounting code are also checked unchanged.

Only three new cases: H1 distance caps <=0.20, <=0.25 and <=0.30 H1 ATR14.
Distance is (last completed native H1 close - H1 EMA100) / H1 ATR14, evaluated
when a resumption candle qualifies. It is not distance from the M15 signal close.
The H1 uptrend definition stays fixed. Failing this cap retains the original
M15 touch and intervening lows until the unchanged expiry, gap or trend reset.
Each cap independently regenerates state; a tighter cap may admit a LATER
qualification absent from the broad or looser-cap stream. Account trading is
replayed from scratch, never selected from a coarse diagnostic bin or old CSV.

The trigger stays bullish M15 close > previous completed M15 HIGH and > current
EMA20; no exact engulf/body floor or close-position/momentum filter. Lifetime16,
RR3, 15-tick adverse entry bound, 10-tick stop buffer, GBP100 whole-unit sizing,
1% modeled NAV risk, funding, conversion, margin and path conventions are fixed.
All seven reset windows run under frozen 14-Oct-2026 rates and adverse 6% annual
funding. Actual historical schedules are not reconstructed.

control_comparisons.csv compares every cap with the broad resumption and each
tighter cap with its adjacent looser neighbour. trade_attribution.csv reports
removed/new/shared closed trades in every window and cost case. Shared sizing
can differ. signal_overlap.csv reports raw qualification time/geometry overlap,
which is distinct from account admissions and incremental live32 overlap.

cap_regions.csv contains both adjacent pairs and the full three-cap set.
EVERY member must pass the unchanged 200 full / 60 recent / 20 per-era samples,
era coverage, full/recent profitability and best-five tests under both funding
cases for a region to pass. Descriptive positivity stays separate from approval.
All three cases and weak eras remain visible; there is no automatic winner.

The original coarse feature bins are unchanged and remain descriptive raw
opportunity reports, NOT extra filtered account tests or compounded returns.
Entry cohorts follow outcomes to the fixed full-study cutoff; fresh account
windows stop at their own end. The original <0.25 diagnostic bin is not identical
to this inclusive <=0.25 strategy, especially after causal state changes.

All history was seen. Neighbouring caps and overlapping windows are correlated.
Original gaps, unresolved paths, stale valuations and barrier-order assumptions
remain explicit. Sampled NAV DD is not a worst intrabar equity bound. These
conditional standalone accounts do not establish independent confirmation or
combined live32 admission. No live orders or activation are supported.

Review this one bounded neighbourhood. If support is absent, park the branch;
no automatic threshold extension, extra feature stacking or RR search.
'''
