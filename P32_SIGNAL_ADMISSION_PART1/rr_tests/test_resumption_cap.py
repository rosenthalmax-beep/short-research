import copy,json,unittest,tempfile,collections
from pathlib import Path
from test_rr import history,START,END
from audusd_resumption_cap.config import candidates,OVERLAPS,ENGULF,RESUMPTION,configuration,CAPS,cap_id
from audusd_resumption_cap.signals import generate_grid,passes,evaluate
from audusd_resumption.signals import generate_grid as previous_generate
from audusd_resumption_cap.runner import Research,BASELINE
from audusd_resumption_cap.reporting import regions,references
from audusd_resumption_cap.diagnostics import BINS,bin_index,describe
from audusd_resumption_cap.provenance_guard import verify_report,REFERENCES
from audusd_pullback_resumption_cap import application
from pullback_lab import core as e

class ResumptionTests(unittest.TestCase):
 def state(self):return dict(setup=dict(index=10,extreme=.99,area=1.1,start_atr=.1),armed=False)
 def step(self,current,previous=None,kind='PRIOR_HIGH_CLOSE',ma=1.,accept=True,index=12,state=None,trend=True):
  return evaluate(self.state() if state is None else state,current,previous or dict(o=1.,c=1.04,h=1.05,l=.99),trend,1.1,ma,.1,index,{},lambda f:accept,16,kind)
 def test_eight_cases_five_controls_and_three_fixed_caps(self):
  rows=candidates();self.assertEqual(len(rows),8);self.assertEqual(tuple(r['candidate_id'] for r in rows[:5]),OVERLAPS)
  self.assertTrue(all(r['is_control'] for r in rows[:5]));self.assertEqual({r['distance_max'] for r in rows[5:]},set(CAPS))
  base=next(r for r in rows if r['candidate_id']==RESUMPTION)
  for r in rows[5:]:
   self.assertFalse(r['is_control']);self.assertEqual(r['trigger'],'PRIOR_HIGH_CLOSE');self.assertIsNone(r['body_min']);self.assertFalse(r.get('filters'));self.assertFalse(r.get('feature'))
   for k in set(base)|set(r):
    if k not in ('candidate_id','kind','is_control','distance_max','anchor_id'):self.assertEqual(r.get(k),base.get(k))
   self.assertEqual(references(r)[0],('BROAD_RESUMPTION',RESUMPTION))
  cfg=configuration();self.assertEqual(cfg['accounts'],112);self.assertEqual(cfg['exact_control_accounts'],70);self.assertEqual(cfg['entry_limit_ticks'],15)
  self.assertEqual([cfg['sample_screen'][k] for k in ('min_full_closed_trades','min_recent_closed_trades','min_era_closed_trades')],[200,60,20])
 def test_inclusive_caps_and_failed_cap_retains_setup_until_later_qualification(self):
  tight,loose=[next(r for r in candidates() if r['candidate_id']==cap_id(c)) for c in (.2,.3)]
  for cap in CAPS:
   rule=next(r for r in candidates() if r['candidate_id']==cap_id(cap))
   self.assertTrue(passes(rule,dict(h1_distance_atr=cap,body_atr=.01)))
   self.assertFalse(passes(rule,dict(h1_distance_atr=cap+.00001,body_atr=.01)))
  t=self.state();l=self.state();now=dict(o=1.03,c=1.06,h=1.07,l=.98);prior=dict(o=1.,c=1.04,h=1.05,l=.99)
  f=dict(h1_distance_atr=.26,body_atr=.01)
  a,_=evaluate(t,now,prior,True,1.1,1.,.1,12,f,lambda v:passes(tight,v),16,'PRIOR_HIGH_CLOSE')
  b,_=evaluate(l,now,prior,True,1.1,1.,.1,12,f,lambda v:passes(loose,v),16,'PRIOR_HIGH_CLOSE')
  self.assertIsNone(a);self.assertIsNotNone(b);self.assertEqual(t['setup']['index'],10);self.assertEqual(t['setup']['extreme'],.98)
  f['h1_distance_atr']=.15
  a,_=evaluate(t,now,prior,True,1.1,1.,.1,13,f,lambda v:passes(tight,v),16,'PRIOR_HIGH_CLOSE')
  b,_=evaluate(l,now,prior,True,1.1,1.,.1,13,f,lambda v:passes(loose,v),16,'PRIOR_HIGH_CLOSE')
  self.assertIsNotNone(a);self.assertIsNone(b)
 def test_two_adjacent_pairs_and_triplet_require_every_member_and_original_samples(self):
  rows=[]
  for r in candidates():
   for w in ['FULL_HISTORY','RECENT_2021_TO_CUTOFF']+['ERA_'+str(y)+'_'+str(y+4) for y in (2005,2010,2015,2020,2025)]:
    for mode in ('FROZEN_14OCT2026_RATES','ADVERSE_6PCT_ANNUAL'):
     rows.append(dict(r,window_id=w,financing_mode=mode,closed_trades=300,conditional_ending_nav_gbp=130.,mean_net_r_without_best_five=.1,uncertain_account_lineage=True,conditional_nav_cagr_pct=2.,sampled_mid_nav_drawdown_pct=-10.))
  blocks=regions(candidates(),rows);self.assertEqual(len(blocks),3);self.assertEqual([b['member_count'] for b in blocks],[2,2,3]);self.assertTrue(all(b['conditional_plateau_screen_pass'] for b in blocks))
  for r in rows:
   if r['candidate_id']==cap_id(.25) and r['window_id']=='RECENT_2021_TO_CUTOFF':r['closed_trades']=59
  self.assertTrue(all(not b['conditional_plateau_screen_pass'] for b in regions(candidates(),rows)))
  self.assertTrue(all(b['account_lineage_uncertainty_present'] for b in regions(candidates(),rows)))
  self.assertTrue(all(not b['conditional_plateau_screen_pass'] for b in regions(candidates(),[])))
 def test_resumption_works_without_engulf_and_requires_bullish_strict_prior_high_and_ema(self):
  now=dict(o=1.03,c=1.06,h=1.07,l=1.02)
  yes,_=self.step(now);no,_=self.step(now,kind='ENGULF');self.assertIsNotNone(yes);self.assertIsNone(no)
  for current,ma in [(dict(now,c=1.05),1.),(dict(now,o=1.06),1.),(dict(now,o=1.065),1.),(now,1.06),(now,1.07)]:
   selected,_=self.step(current,ma=ma);self.assertIsNone(selected)
  # An engulfing candle whose close does not clear the preceding upper wick
  # qualifies only for the original trigger.
  prior=dict(o=1.04,c=1.,h=1.08,l=.99);current=dict(o=.99,c=1.06,h=1.07,l=.98)
  self.assertIsNotNone(self.step(current,prior,kind='ENGULF')[0]);self.assertIsNone(self.step(current,prior)[0])
 def test_failure_retains_touch_all_lows_and_acceptance_consumes_until_reset(self):
  state=self.state();now=dict(o=1.03,c=1.06,h=1.07,l=.98)
  selected,_=self.step(now,state=state,accept=False);self.assertIsNone(selected);self.assertEqual(state['setup']['index'],10);self.assertEqual(state['setup']['extreme'],.98)
  selected,_=self.step(now,state=state,index=13);self.assertIsNotNone(selected);self.assertEqual(selected['setup']['extreme'],.98);self.assertIsNone(state['setup'])
  self.assertIsNone(self.step(now,state=state,index=14)[0])
  self.assertIsNotNone(self.step(now,index=25)[0]);self.assertIsNone(self.step(now,index=26)[0]);self.assertIsNone(self.step(now,trend=False)[0])
 def test_controls_baseline_exact_and_future_m15_h1_do_not_change_past(self):
  st,m,h=history();rules=[BASELINE]+candidates();actual,c=generate_grid(m,h,START,END,START,rules=rules)
  oldrules=[r for r in rules if r['candidate_id']=='BASELINE' or r.get('is_control')];old,oc=previous_generate(m,h,START,END,START,rules=oldrules)
  for cid in ('BASELINE',)+OVERLAPS:self.assertEqual(actual[cid],old[cid]);self.assertEqual(c[cid],oc[cid])
  for r in rules[1:]:
   self.assertGreater(len(actual[r['candidate_id']]),0)
   for signal in actual[r['candidate_id']]:
    self.assertTrue(passes(r,signal['features']));self.assertLessEqual(signal['features']['h1_source_close_utc'],signal['entry_utc'])
    if r.get('diagnostic_features'):self.assertTrue(set(BINS)<=set(signal['features']))
  self.assertTrue(all(s['control']=='TREND_PULLBACK_RESUMPTION' for s in actual[RESUMPTION]))
  changed=copy.deepcopy(m);changed_h=copy.deepcopy(h);cut='2020-03-06T00:00:00Z'
  for stream in (changed,changed_h):
   for i,t in enumerate(stream.times):
    if t>=e.dt(cut).timestamp():
     for side in stream.values:
      for f in 'ohlc':stream.values[side][f][i]+=.1
  after,_=generate_grid(changed,changed_h,START,END,START,rules=rules)
  for r in rules:
   cid=r['candidate_id'];self.assertEqual([s for s in actual[cid] if s['entry_utc']<cut],[s for s in after[cid] if s['entry_utc']<cut])
  st.close()
 def test_missing_diagnostic_features_are_not_new_filters(self):
  rule=next(r for r in candidates() if r['candidate_id']==RESUMPTION)
  f=dict(h1_distance_atr=1.,body_atr=.01,previous_close_location=None,pre_touch_rise_4h_atr=None)
  self.assertTrue(passes(rule,f))
 def test_feature_bins_boundaries_missing_and_raw_denominator_reconcile(self):
  self.assertEqual(bin_index(.5,BINS['body_atr']),1);self.assertEqual(bin_index(0.,BINS['pre_touch_rise_4h_atr']),1)
  for value in (None,float('nan'),float('inf'),True):self.assertIsNone(bin_index(value,BINS['body_atr']))
  vals=[None,float('nan'),-.2,0.,.5,1.,1.5,10.]
  opps=[dict(entry_utc=START,path_status='UNRESOLVED',features={k:v for k in BINS},shadow=dict(shadow_status='UNRESOLVED')) for v in vals]
  rows,cov=describe(RESUMPTION,opps,[dict(window_id='ALL',effective_start=START,end=END)])
  for x in cov:self.assertEqual(x['all_raw_qualifications'],8);self.assertEqual(x['finite_feature_qualifications'],6);self.assertEqual(x['missing_or_nonfinite_feature_qualifications'],2)
  by=collections.defaultdict(list)
  for x in rows:by[x['feature']].append(x);self.assertFalse(x['filtered_account_replay']);self.assertIsNone(x['cagr'])
  for k,group in by.items():self.assertEqual(sum(x['qualifications'] for x in group),8);self.assertEqual(sum(x['unresolved_or_nonclosed_or_sizing_excluded'] for x in group),8)
 def test_new_source_provenance_allows_only_three_work_counters(self):
  with tempfile.TemporaryDirectory() as tmp:
   path=Path(tmp)/'source_provenance.json';digest,record=next((k,v) for k,v in REFERENCES.items() if v['archive']=='AUDUSD_RESUMPTION_SUMMARY.zip');changed=copy.deepcopy(record['payload'])
   changed['new_geometry_paths_computed']+=1;path.write_bytes(e.canon(changed));self.assertEqual(verify_report(path,digest)[1]['classification'],'COUNTERS_ONLY')
   changed['history']['m15']['observed_bars']+=1;path.write_bytes(e.canon(changed))
   with self.assertRaisesRegex(ValueError,'locked fields'):verify_report(path,digest)
 def test_protected_roots_http_and_no_auto_start_or_orders(self):
  for parent in ('/data/p32_audusd_resumption','/data/p32_audusd_body_recovery','/data/p32_audusd_combinations','/data/p32_audusd_conditional','/data/p32_audusd_lifetime_extension','/data/p32_pullback_pass1','/data/p32_audusd_rr','/data'):
   with self.assertRaisesRegex(ValueError,'separate'):Research(dict(AUDUSD_RESUMPTION_CAP_OUTPUT_DIR=parent+'/child' if parent!='/data' else parent))
  with tempfile.TemporaryDirectory() as tmp:
   job=Research(dict(AUDUSD_RESUMPTION_CAP_OUTPUT_DIR=tmp+'/out'));app=application(job);codes=[]
   bodies=[b''.join(app(dict(PATH_INFO=path,REQUEST_METHOD=method),lambda s,h:codes.append(s))) for path,method in [('/health','GET'),('/status','HEAD'),('/orders','POST')]]
   self.assertEqual(codes,['200 OK','200 OK','405 Method Not Allowed']);self.assertIsNone(job.thread);self.assertEqual(json.loads(bodies[0])['overlap_account_payloads'],70)
