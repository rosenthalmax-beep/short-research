import copy,json,unittest,tempfile,collections
from pathlib import Path
from test_rr import history,START,END
from audusd_resumption.config import candidates,OVERLAPS,ENGULF,RESUMPTION,configuration
from audusd_resumption.signals import generate_grid,passes,evaluate
from audusd_body_recovery.signals import generate_grid as previous_generate
from audusd_resumption.runner import Research,BASELINE
from audusd_resumption.reporting import regions,references
from audusd_resumption.diagnostics import BINS,bin_index,describe
from audusd_resumption.provenance_guard import verify_report,REFERENCES
from audusd_pullback_resumption import application
from pullback_lab import core as e

class ResumptionTests(unittest.TestCase):
 def state(self):return dict(setup=dict(index=10,extreme=.99,area=1.1,start_atr=.1),armed=False)
 def step(self,current,previous=None,kind='PRIOR_HIGH_CLOSE',ma=1.,accept=True,index=12,state=None,trend=True):
  return evaluate(self.state() if state is None else state,current,previous or dict(o=1.,c=1.04,h=1.05,l=.99),trend,1.1,ma,.1,index,{},lambda f:accept,16,kind)
 def test_five_cases_three_controls_and_matched_context(self):
  rows=candidates();self.assertEqual(len(rows),5);self.assertEqual(tuple(r['candidate_id'] for r in rows[:3]),OVERLAPS)
  self.assertTrue(all(r['is_control'] for r in rows[:3]));a,b=rows[3:]
  self.assertEqual(a['candidate_id'],ENGULF);self.assertEqual(b['candidate_id'],RESUMPTION)
  for key in set(a)|set(b):
   if key not in ('candidate_id','trigger'):self.assertEqual(a[key],b[key])
  self.assertIsNone(a['body_min']);self.assertFalse(a.get('filters'));self.assertFalse(a.get('feature'))
  self.assertEqual(configuration()['accounts'],70);self.assertEqual(configuration()['entry_limit_ticks'],15);self.assertEqual(configuration()['sample_screen']['min_full_closed_trades'],200)
  self.assertEqual(regions(rows,[]),[]);self.assertEqual(references(b)[0],('MATCHED_TRIGGER',ENGULF))
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
   path=Path(tmp)/'source_provenance.json';digest,record=next((k,v) for k,v in REFERENCES.items() if v['archive']=='AUDUSD_BODY_RECOVERY_SUMMARY.zip');changed=copy.deepcopy(record['payload'])
   changed['new_geometry_paths_computed']+=1;path.write_bytes(e.canon(changed));self.assertEqual(verify_report(path,digest)[1]['classification'],'COUNTERS_ONLY')
   changed['history']['m15']['observed_bars']+=1;path.write_bytes(e.canon(changed))
   with self.assertRaisesRegex(ValueError,'locked fields'):verify_report(path,digest)
 def test_protected_roots_http_and_no_auto_start_or_orders(self):
  for parent in ('/data/p32_audusd_body_recovery','/data/p32_audusd_combinations','/data/p32_audusd_conditional','/data/p32_audusd_lifetime_extension','/data/p32_pullback_pass1','/data/p32_audusd_rr','/data'):
   with self.assertRaisesRegex(ValueError,'separate'):Research(dict(AUDUSD_RESUMPTION_OUTPUT_DIR=parent+'/child' if parent!='/data' else parent))
  with tempfile.TemporaryDirectory() as tmp:
   job=Research(dict(AUDUSD_RESUMPTION_OUTPUT_DIR=tmp+'/out'));app=application(job);codes=[]
   bodies=[b''.join(app(dict(PATH_INFO=path,REQUEST_METHOD=method),lambda s,h:codes.append(s))) for path,method in [('/health','GET'),('/status','HEAD'),('/orders','POST')]]
   self.assertEqual(codes,['200 OK','200 OK','405 Method Not Allowed']);self.assertIsNone(job.thread);self.assertEqual(json.loads(bodies[0])['overlap_account_payloads'],42)
