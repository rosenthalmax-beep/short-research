import copy,json,unittest,tempfile
from pathlib import Path
from test_rr import history,START,END
from audusd_body_recovery.config import candidates,OVERLAPS,BODIES,CLOSE,MOMENTUM,combination_id,configuration
from audusd_body_recovery.signals import generate_grid,passes,evaluate
from audusd_combinations.signals import generate_grid as previous_generate
from audusd_body_recovery.runner import Research,BASELINE
from audusd_body_recovery.reporting import regions,references,comparisons,frequency_rows
from audusd_body_recovery.provenance_guard import verify_report,REFERENCES
from audusd_pullback_body_recovery import application
from pullback_lab import core as e

class BodyRecoveryTests(unittest.TestCase):
 def test_bounded_grid_36_cases_12_controls_first_and_original_screens(self):
  rows=candidates();self.assertEqual(len(rows),36);self.assertEqual(len(set(r['candidate_id'] for r in rows)),36)
  self.assertTrue(all(r.get('is_control') for r in rows[:12]));self.assertTrue(all(not r.get('is_control') for r in rows[12:]));self.assertEqual(len(OVERLAPS),12)
  self.assertEqual({(r['body_min'],r['close_max'],r['momentum_min']) for r in rows if r['kind']=='COMBINATION'},{(b,c,m) for b in BODIES for c in CLOSE for m in MOMENTUM})
  self.assertTrue(all(r['rr']==3. and r['lifetime_bars']==16 and r['distance_max']==1.25 for r in rows))
  self.assertEqual([configuration()['plateau_screen'][k] for k in ('min_full_closed_trades','min_recent_closed_trades','min_era_closed_trades')],[200,60,20])
 def test_all_twelve_control_signals_exact_and_every_new_case_causal(self):
  st,m,h=history();rules=[BASELINE]+candidates();actual,c=generate_grid(m,h,START,END,START,rules=rules)
  oldrules=[r for r in rules if r['candidate_id']=='BASELINE' or r.get('is_control')];old,oc=previous_generate(m,h,START,END,START,rules=oldrules)
  for cid in ('BASELINE',)+OVERLAPS:self.assertEqual(actual[cid],old[cid]);self.assertEqual(c[cid],oc[cid])
  for rule in candidates():
   self.assertGreater(len(actual[rule['candidate_id']]),0)
   for signal in actual[rule['candidate_id']]:
    self.assertTrue(passes(rule,signal['features']));self.assertLessEqual(signal['features']['h1_source_close_utc'],signal['entry_utc']);self.assertNotIn('_setup_features',signal['features'])
  changed=copy.deepcopy(m);cut='2020-03-06T00:00:00Z'
  for i,t in enumerate(changed.times):
   if t>=e.dt(cut).timestamp():
    for side in changed.values:
     for f in 'ohlc':changed.values[side][f][i]+=.1
  changed_h=copy.deepcopy(h)
  for i,t in enumerate(changed_h.times):
   if t>=e.dt(cut).timestamp():
    for side in changed_h.values:
     for f in 'ohlc':changed_h.values[side][f][i]+=.1
  after,_=generate_grid(changed,changed_h,START,END,START,rules=rules)
  for rule in rules:
   cid=rule['candidate_id'];self.assertEqual([s for s in actual[cid] if s['entry_utc']<cut],[s for s in after[cid] if s['entry_utc']<cut])
  st.close()
 def test_body_and_both_filter_boundaries_missing_and_nan(self):
  rule=next(r for r in candidates() if r['kind']=='COMBINATION' and r['body_min']==1.125 and r['close_max']==.5 and r['momentum_min']==.5)
  f=dict(h1_distance_atr=1.25,body_atr=1.125,previous_close_location=.5,pre_touch_rise_4h_atr=.5);self.assertTrue(passes(rule,f))
  for field,bad in [('body_atr',1.124),('h1_distance_atr',1.251),('previous_close_location',.51),('pre_touch_rise_4h_atr',.49),('previous_close_location',None),('pre_touch_rise_4h_atr',float('nan'))]:
   changed=dict(f);changed[field]=bad;self.assertFalse(passes(rule,changed))
 def test_relaxed_acceptance_consumes_and_can_displace_later_strict_trade(self):
  strict=dict(setup=dict(index=10,extreme=.99,area=1.1,start_atr=.1),armed=False);relaxed=copy.deepcopy(strict)
  now=dict(o=1.07,c=1.14,h=1.15,l=.98);prior=dict(o=1.13,c=1.08,h=1.14,l=1.07)
  x,_=evaluate(relaxed,now,prior,True,1.1,1.1,.1,12,dict(body_atr=1.1),lambda f:f['body_atr']>=1.,16)
  y,_=evaluate(strict,now,prior,True,1.1,1.1,.1,12,dict(body_atr=1.1),lambda f:f['body_atr']>=1.25,16)
  self.assertIsNotNone(x);self.assertIsNone(y);self.assertIsNone(relaxed['setup']);self.assertEqual(strict['setup']['index'],10)
  x,_=evaluate(relaxed,now,prior,True,1.1,1.1,.1,13,dict(body_atr=1.3),lambda f:True,16)
  y,_=evaluate(strict,now,prior,True,1.1,1.1,.1,13,dict(body_atr=1.3),lambda f:True,16)
  self.assertIsNone(x);self.assertIsNotNone(y);self.assertEqual(y['setup']['extreme'],.98)
 def test_ten_regions_keep_samples_and_require_all_four_or_eight_members(self):
  rows=[]
  for r in candidates():
   for w in ['FULL_HISTORY','RECENT_2021_TO_CUTOFF']+['ERA_'+str(y)+'_'+str(y+4) for y in (2005,2010,2015,2020,2025)]:
    for m in ('FROZEN_14OCT2026_RATES','ADVERSE_6PCT_ANNUAL'):
     rows.append(dict(r,window_id=w,financing_mode=m,closed_trades=300,conditional_ending_nav_gbp=130.,mean_net_r_without_best_five=.1,uncertain_account_lineage=True,conditional_nav_cagr_pct=2.,sampled_mid_nav_drawdown_pct=-10.))
  blocks=regions(candidates(),rows);self.assertEqual(len(blocks),10);self.assertEqual([b['member_count'] for b in blocks],[4]*6+[8]*4);self.assertTrue(all(b['conditional_plateau_screen_pass'] for b in blocks))
  target=combination_id(1.125,1,1)
  for r in rows:
   if r['candidate_id']==target and r['window_id']=='RECENT_2021_TO_CUTOFF':r['closed_trades']=59
  failed=regions(candidates(),rows);self.assertTrue(all(not b['conditional_plateau_screen_pass'] for b in failed if target in b['member_candidates']));self.assertTrue(all(b['account_lineage_uncertainty_present'] for b in failed))
 def test_same_filter_original_body_and_same_body_factor_comparisons(self):
  for r in candidates():
   refs=references(r)
   if r.get('original_body_control'):self.assertEqual(refs[0],('ORIGINAL_BODY',r['candidate_id'].replace('_B'+str(r['body_min']),'_B1.25')))
   if r['kind']=='COMBINATION':self.assertEqual({x[0] for x in refs}-{'ORIGINAL_BODY'},{'UNFILTERED','CLOSE_ONLY','MOMENTUM_ONLY'})
 def test_guard_locks_new_reviewed_provenance_and_only_allows_named_counters(self):
  with tempfile.TemporaryDirectory() as tmp:
   path=Path(tmp)/'source_provenance.json';digest,record=next((k,v) for k,v in REFERENCES.items() if v['archive']=='AUDUSD_COMBINATIONS_SUMMARY.zip');original=copy.deepcopy(record['payload']);changed=copy.deepcopy(original)
   changed['new_geometry_paths_computed']+=1;path.write_bytes(e.canon(changed));_,check=verify_report(path,digest);self.assertEqual(check['classification'],'COUNTERS_ONLY')
   changed['history']['m15']['observed_bars']+=1;path.write_bytes(e.canon(changed))
   with self.assertRaisesRegex(ValueError,'locked fields'):verify_report(path,digest)
 def test_output_roots_http_and_no_automatic_start(self):
  for parent in ('/data/p32_audusd_combinations','/data/p32_audusd_conditional','/data/p32_audusd_lifetime_extension','/data/p32_pullback_pass1','/data/p32_audusd_rr','/data'):
   with self.assertRaisesRegex(ValueError,'separate'):Research(dict(AUDUSD_BODY_RECOVERY_OUTPUT_DIR=parent+'/child' if parent!='/data' else parent))
  with tempfile.TemporaryDirectory() as tmp:
   job=Research(dict(AUDUSD_BODY_RECOVERY_OUTPUT_DIR=tmp+'/out'));app=application(job);codes=[]
   bodies=[b''.join(app(dict(PATH_INFO=path,REQUEST_METHOD=method),lambda s,h:codes.append(s))) for path,method in [('/health','GET'),('/status','HEAD'),('/orders','POST')]]
   self.assertEqual(codes,['200 OK','200 OK','405 Method Not Allowed']);self.assertIsNone(job.thread);self.assertEqual(json.loads(bodies[0])['overlap_account_payloads'],168)
