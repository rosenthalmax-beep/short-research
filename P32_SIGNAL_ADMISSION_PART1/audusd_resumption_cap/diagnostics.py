"""Predeclared descriptive bins, never automatic filters or account selections."""
import gzip,json,math
from pathlib import Path
from pullback_lab import core as e
from pullback_lab.reports import stats
from audusd_matrix import io as x
from .config import candidates,MODES

# None at the ends means an unbounded edge; no non-finite JSON is serialized.
BINS={
 'body_atr':(None,.5,1.,1.5,2.,None),
 'range_atr':(None,.5,1.,1.5,2.,3.,None),
 'close_location':(None,.2,.4,.6,.8,1.,None),
 'previous_close_location':(None,.2,.4,.6,.8,1.,None),
 'duration_bars':(None,5.,9.,13.,None),
 'depth_atr':(None,.25,.5,.75,1.,2.,None),
 'stop_atr':(None,.5,1.,1.5,2.,3.,None),
 'h1_distance_atr':(None,.25,.5,.75,1.,1.25,None),
 'm15_atr_ratio':(None,.75,1.,1.25,1.5,2.,None),
 'h1_atr_ratio':(None,.75,1.,1.25,1.5,2.,None),
 'pre_touch_rise_4h_atr':(None,0.,.5,1.,2.,None),
 'retracement_fraction':(None,.25,.5,.75,1.,2.,None),
 'prior_low20_distance_atr':(None,.25,.5,1.,2.,None),
 'prior_low60_distance_atr':(None,.25,.5,1.,2.,None)}

def bin_protocol():
 return dict(features={k:list(v) for k,v in BINS.items()},interval='LOWER_INCLUSIVE_UPPER_EXCLUSIVE; NULL_END_EDGE_UNBOUNDED; MISSING_SEPARATE',
  purpose='DESCRIPTIVE_RAW_OPPORTUNITIES_ONLY; NO_CUMULATIVE_THRESHOLD_SEARCH_OR_SELECTION',outcome_cutoff='UNCHANGED_FIXED_STUDY_CUTOFF',
  denominator='ALL_QUALIFICATIONS_IN_ENTRY_COHORT; EACH_FEATURE_PARTITION_RECONCILES_TO_THAT_DENOMINATOR')

def bin_index(value,edges):
 if value is None or isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value):return None
 for i,(lo,hi) in enumerate(zip(edges,edges[1:])):
  if (lo is None or value>=lo) and (hi is None or value<hi):return i
 raise ValueError('Finite feature outside predeclared bins')

def describe(cid,opps,windows):
 rows=[];coverage=[]
 for w in windows:
  group=[o for o in opps if w['effective_start']<=o['entry_utc']<w['end']]
  for feature,edges in BINS.items():
   buckets={i:[] for i in range(len(edges)-1)};buckets[None]=[]
   for o in group:buckets[bin_index(o['features'].get(feature),edges)].append(o)
   e.require(sum(map(len,buckets.values()))==len(group),'Feature partition lost a raw qualification')
   missing=len(buckets[None]);coverage.append(dict(candidate_id=cid,entry_cohort=w['window_id'],feature=feature,all_raw_qualifications=len(group),finite_feature_qualifications=len(group)-missing,missing_or_nonfinite_feature_qualifications=missing,
    scope='CAUSAL_FEATURE_COVERAGE_OF_RAW_QUALIFICATIONS; NOT_CLOSED_TRADES'))
   for i,subset in buckets.items():
    rows.append(dict(candidate_id=cid,entry_cohort=w['window_id'],entry_from=w['effective_start'],entry_to=w['end'],feature=feature,bin='MISSING' if i is None else str(i),
     lower_inclusive=None if i is None else edges[i],upper_exclusive=None if i is None else edges[i+1],
     all_raw_qualifications_in_cohort=len(group),feature_missing_qualifications=missing,
     bin_share_of_all_qualifications_pct=100*len(subset)/len(group) if group else None,
     outcomes_followed_to_fixed_study_cutoff=True,filtered_account_replay=False,automatic_selection=False,**stats(subset)))
 return rows,coverage

def write_diagnostics(job,dirs):
 by={p.name:p for p in dirs};features=[];coverage=[];overlap=[];signals={}
 def signal_rows(cid):
  if cid not in signals:
   with gzip.open(by[cid]/'signals.jsonl.gz','rt') as f:signals[cid]=list(map(json.loads,f))
  return signals[cid]
 from .reporting import references
 for rule in candidates():
  cid=rule['candidate_id']
  if cid not in by or not rule.get('diagnostic_features'):continue
  accounts=json.loads((by[cid]/'account_summaries.json').read_bytes())
  windows=[a for a in accounts if a['financing_mode']==MODES[0]]
  with gzip.open(by[cid]/'opportunities.jsonl.gz','rt') as f:opps=list(map(json.loads,f))
  rows,cov=describe(cid,opps,windows);features.extend(rows);coverage.extend(cov)
  for label,reference in references(rule):
   if reference not in by:continue
   for w in windows:
    def selected(ident):return [s for s in signal_rows(ident) if w['effective_start']<=s['entry_utc']<w['end']]
    a,b=selected(cid),selected(reference)
    def geometry(items):return {(s['entry_utc'],s['reference_entry'],s['stop'],s['target']) for s in items}
    aa,bb=geometry(a),geometry(b);e.require(len(aa)==len(a) and len(bb)==len(b),'Duplicate raw qualification geometry')
    overlap.append(dict(candidate_id=cid,reference_candidate_id=reference,comparison=label,entry_cohort=w['window_id'],
     candidate_raw_qualifications=len(a),reference_raw_qualifications=len(b),shared_geometry=len(aa&bb),candidate_only_geometry=len(aa-bb),reference_only_geometry=len(bb-aa),
     shared_entry_times=len({s['entry_utc'] for s in a}&{s['entry_utc'] for s in b}),
     scope='RAW_CAUSAL_QUALIFICATIONS; NOT_ACCOUNT_ADMISSIONS_OR_LIVE32_OVERLAP'))
 for name,rows in [('feature_diagnostics.csv',features),('feature_coverage.csv',coverage),('signal_overlap.csv',overlap)]:x.atomic(job.work/name,x.csv_bytes(rows))
 x.atomic(job.work/'research_stage.json',e.canon(dict(stage='BOUNDED_H1_DISTANCE_CAP',new_mechanisms=0,parameter_grid=True,plateau_regions_tested=3,
  plateau_conclusion='SEE_CAP_REGIONS; EVERY_MEMBER_MUST_PASS_UNCHANGED_SCREEN',all_history_seen=True,automatic_selection=False,filtered_feature_accounts_tested=True,full_portfolio_validated=False,orders_sent=False)))
