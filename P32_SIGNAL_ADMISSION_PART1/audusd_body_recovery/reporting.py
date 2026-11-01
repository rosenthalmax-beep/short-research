"""Compare combinations with both single filters; preserve weak eras and samples."""
import gzip,json,math
from pathlib import Path
from audusd_matrix import io as x
from audusd_matrix.reporting import account_screen
from pullback_lab import core as e
from .config import candidates,BODIES,CLOSE,MOMENTUM,MODES,combination_id

METRICS=('closed_trades','conditional_nav_cagr_pct','sampled_mid_nav_drawdown_pct','mean_net_r_actual_entry_risk','mean_net_r_without_best_five','conditional_ending_nav_gbp','net_r_sum_actual_entry_risk')
def references(rule):
 refs=[]
 if rule.get('original_body_control'):refs.append(('ORIGINAL_BODY',rule['original_body_control']))
 if rule['kind']=='COMBINATION':refs += [('UNFILTERED',rule['anchor_id']),('CLOSE_ONLY',rule['close_control']),('MOMENTUM_ONLY',rule['momentum_control'])]
 elif rule.get('anchor_id'):refs.append(('UNFILTERED',rule['anchor_id']))
 return refs

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
 def add(body_values,i,kind):
  members=[combination_id(body,ii,j) for body in body_values for ii in (i,i+1) for j in range(2)]
  screens=[dict(candidate_id=cid,**account_screen(by.get(cid,[]))) for cid in members]
  allrows=[a for cid in members for a in by.get(cid,[])]
  full=[a for a in allrows if a['window_id']=='FULL_HISTORY'];recent=[a for a in allrows if a['window_id']=='RECENT_2021_TO_CUTOFF'];fr=full+recent
  complete=all(len(by.get(cid,[]))==14 for cid in members)
  def low(rows,key):return min((a[key] for a in rows if a.get(key) is not None),default=None)
  def high(rows,key):return max((a[key] for a in rows if a.get(key) is not None),default=None)
  out.append(dict(region_kind=kind,body_low=min(body_values),body_high=max(body_values),close_low=CLOSE[i],close_high=CLOSE[i+1],momentum_low=0.,momentum_high=.5,
   member_candidates=members,member_count=len(members),all_member_accounts_complete=complete,member_screens=screens,
   conditional_plateau_screen_pass=complete and all(s['screen_pass'] for s in screens),
   all_neighbours_positive_full_recent_both_fundings=len(fr)==4*len(members) and all(a['conditional_ending_nav_gbp']>100 for a in fr),
   all_neighbours_positive_without_best_five=len(fr)==4*len(members) and all(a['mean_net_r_without_best_five'] is not None and a['mean_net_r_without_best_five']>0 for a in fr),
   min_full_closed_trades=low(full,'closed_trades'),min_recent_closed_trades=low(recent,'closed_trades'),
   full_cagr_min=low(full,'conditional_nav_cagr_pct'),full_cagr_max=high(full,'conditional_nav_cagr_pct'),
   recent_cagr_min=low(recent,'conditional_nav_cagr_pct'),recent_cagr_max=high(recent,'conditional_nav_cagr_pct'),
   full_worst_sampled_nav_drawdown_pct=low(full,'sampled_mid_nav_drawdown_pct'),recent_worst_sampled_nav_drawdown_pct=low(recent,'sampled_mid_nav_drawdown_pct'),
   min_positive_eras=min(s['positive_eras'] for s in screens),min_supported_eras=min(s['supported_eras'] for s in screens),
   account_lineage_uncertainty_present=any(s['uncertain_account_rows'] for s in screens),automatic_selection=False,full_portfolio_validated=False))
 for body in BODIES:
  for i in range(2):add((body,),i,'WITHIN_BODY_CLOSE_MOMENTUM_2X2')
 for lo,hi in zip(BODIES,BODIES[1:]):
  for i in range(2):add((lo,hi),i,'ADJACENT_BODY_CLOSE_MOMENTUM_2X2X2')
 return out

def frequency_rows(accounts):
 by={(r['candidate_id'],r['window_id'],r['financing_mode']):r for r in accounts};out=[]
 for r in accounts:
  cid=r.get('original_body_control')
  if not cid:continue
  base=by.get((cid,r['window_id'],r['financing_mode']))
  if base is None:continue
  row=dict(candidate_id=r['candidate_id'],reference_candidate_id=cid,body_min=r['body_min'],window_id=r['window_id'],financing_mode=r['financing_mode'],
   closed_trades=r['closed_trades'],original_body_closed_trades=base['closed_trades'],closed_trades_delta=r['closed_trades']-base['closed_trades'],
   qualification_delta=r['qualifications']-base['qualifications'],closed_trades_per_year=r['closed_trades_per_year'],
   trade_count_ratio=r['closed_trades']/base['closed_trades'] if base['closed_trades'] else None,
   original_body_control=True,scope='CAUSAL_REGENERATED_FRESH_ACCOUNT; NOT_EXTRA_INDEPENDENT_TRADES_OR_LIVE32_ADMISSION')
  for k in METRICS[1:]:
   row[k]=r.get(k);row['original_'+k]=base.get(k)
   row[k+'_delta']=r[k]-base[k] if r.get(k) is not None and base.get(k) is not None else None
  row['uncertain_account_lineage']=r.get('uncertain_account_lineage')
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
 return f'''# AUD/USD final bounded body-relaxation confirmation
{done}/36 configurations and {accounts}/504 fresh GBP100 accounts.
Twelve original 1.25-ATR-body controls must reproduce all 168 prior account
payloads, exact signals and opportunities before new cases are accepted.

The six close <=0.4/0.5/0.6 by original pre-touch momentum >=0/0.5 ATR
combinations are held fixed while body minimum is 1.0, 1.125 or 1.25 ATR.
Each body also has unfiltered and applicable single-filter controls, so we can
separate body relaxation from the two-factor effect. 24 cases are new; 12 are
exact prior cases. This is one final bounded confirmation, not an open search.

Lifetime 16 bars, H1 distance cap 1.25 ATR, RR3, 15-tick entry bound,
10-tick stop buffer, 1% modeled NAV risk, GBP100 resets, pricing and account
rules are fixed. Smaller bodies independently regenerate setup state. An early
acceptance can consume a setup and displace a later original-body trade, so
never treat the new signal or trade set as a superset. Rejection by a filter
retains original touch and lows; acceptance consumes before execution admission.

control_comparisons.csv and trade_attribution.csv compare smaller bodies with
their SAME-FILTER original body, plus each combination with its same-body
unfiltered and both single-filter controls. Removed/new/shared CLOSED ACCOUNT
trades are attributed for every reset window and both funding cases. Shared
trade account sizing and cash may change. frequency_recovery.csv shows counts,
qualification changes, annual frequency, return, DD and best-five sensitivity
relative to the unchanged-body account. It is not marginal live32 return.

plateau_regions.csv reports six within-body adjacent 2x2 regions and four
adjacent-body 2x2x2 regions. Every member must pass the original screen.
The 200 full / 60 recent / 20 per-era samples, era coverage, both funding cases
and best-five rules are unchanged. Descriptive profitability is not approval.
Review NEW and REMOVED trades, weak eras and neighbouring settings; recovering
trade count alone is not success. There is no automatic selection or activation.

All history was seen. Overlapping settings and windows are correlated.
Actual historical financing, margin and conversion schedules are not known;
current terms, frozen rates and adverse 6% annual funding are modeled. Original
candle gaps, stale marks, barrier uncertainty and unknown-entry omissions remain
explicit; sampled NAV drawdown is not an intrabar worst-equity bound.
No future/synthetic data is inserted into the market study. Neither this replay
nor broker fixture reconciliation validates live32 portfolio admission.

Stop this branch after this bounded confirmation and review; proceed to the
separate pullback-resumption trigger hypothesis. No third feature or RR search.
'''
