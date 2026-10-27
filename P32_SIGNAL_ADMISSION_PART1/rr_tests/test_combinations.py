import copy,json,unittest,tempfile
from pathlib import Path
from test_rr import history,START,END
from audusd_combinations.config import candidates,OVERLAPS,ANCHORS,combination_id
from audusd_combinations.signals import generate_grid,passes,evaluate
from audusd_conditional.signals import generate_grid as previous_generate
from audusd_combinations.runner import Research,BASELINE
from audusd_combinations.reporting import regions,comparisons
from audusd_combinations.provenance_guard import verify_report,REFERENCES
from audusd_pullback_combinations import application
from pullback_lab import core as e

class CombinationTests(unittest.TestCase):
 def test_fixed_32_settings_and_14_controls_first(self):
  rows=candidates();self.assertEqual(len(rows),32);self.assertEqual(len(set(r['candidate_id'] for r in rows)),32)
  self.assertTrue(all(r.get('is_control') for r in rows[:14]));self.assertTrue(all(r['kind']=='COMBINATION' for r in rows[14:]));self.assertEqual(len(OVERLAPS),14)
  self.assertEqual({(r['anchor_id'],r['close_max'],r['momentum_min']) for r in rows[14:]},{(a,c,m) for a in ANCHORS for c in (.4,.5,.6) for m in (0.,.5,1.)})
  self.assertTrue(all(r['rr']==3. for r in rows))
 def test_all_14_control_signals_exact_and_combo_is_causal(self):
  st,m,h=history();rules=[BASELINE]+candidates();actual,c=generate_grid(m,h,START,END,START,rules=rules)
  oldrules=[r for r in rules if r['candidate_id']=='BASELINE' or r.get('is_control')];old,oc=previous_generate(m,h,START,END,START,rules=oldrules)
  for cid in ('BASELINE',)+OVERLAPS:self.assertEqual(actual[cid],old[cid]);self.assertEqual(c[cid],oc[cid])
  for rule in rules:
   if rule.get('kind')=='COMBINATION':
    for signal in actual[rule['candidate_id']]:
     self.assertTrue(passes(rule,signal['features']));self.assertLessEqual(signal['features']['h1_source_close_utc'],signal['entry_utc']);self.assertNotIn('_setup_features',signal['features'])
  changed=copy.deepcopy(m);cut='2020-03-06T00:00:00Z'
  for i,t in enumerate(changed.times):
   if t>=e.dt(cut).timestamp():
    for side in changed.values:
     for f in 'ohlc':changed.values[side][f][i]+=.1
  after,_=generate_grid(changed,h,START,END,START,rules=rules)
  for rule in rules:
   cid=rule['candidate_id'];self.assertEqual([s for s in actual[cid] if s['entry_utc']<cut],[s for s in after[cid] if s['entry_utc']<cut])
  st.close()
 def test_both_conditions_required_missing_rejected_and_boundaries_included(self):
  rule=next(r for r in candidates() if r['kind']=='COMBINATION' and r['close_max']==.5 and r['momentum_min']==.5)
  f=dict(h1_distance_atr=1.,body_atr=1.3,previous_close_location=.5,pre_touch_rise_4h_atr=.5);self.assertTrue(passes(rule,f))
  for field,bad in [('previous_close_location',.51),('pre_touch_rise_4h_atr',.49),('previous_close_location',None),('pre_touch_rise_4h_atr',float('nan'))]:
   changed=dict(f);changed[field]=bad;self.assertFalse(passes(rule,changed))
 def test_combination_filter_failure_preserves_touch_until_next_qualification(self):
  state=dict(setup=dict(index=10,extreme=.99,area=1.1,start_atr=.1),armed=False);now=dict(o=1.07,c=1.14,h=1.15,l=.98);prior=dict(o=1.13,c=1.08,h=1.14,l=1.07)
  x,_=evaluate(state,now,prior,True,1.1,1.1,.1,12,{},lambda f:False,16);self.assertIsNone(x);self.assertEqual(state['setup']['index'],10)
  x,_=evaluate(state,now,prior,True,1.1,1.1,.1,13,{},lambda f:True,16);self.assertEqual(x['setup']['extreme'],.98);self.assertIsNone(state['setup'])
 def test_eight_regions_require_every_member_to_pass(self):
  rows=[]
  for r in candidates():
   for w in ['FULL_HISTORY','RECENT_2021_TO_CUTOFF']+['ERA_'+str(y)+'_'+str(y+4) for y in (2005,2010,2015,2020,2025)]:
    for m in ('FROZEN_14OCT2026_RATES','ADVERSE_6PCT_ANNUAL'):
     rows.append(dict(r,window_id=w,financing_mode=m,closed_trades=300,conditional_ending_nav_gbp=130.,mean_net_r_without_best_five=.1,uncertain_account_lineage=True,conditional_nav_cagr_pct=2.,sampled_mid_nav_drawdown_pct=-10.))
  blocks=regions(candidates(),rows);self.assertEqual(len(blocks),8);self.assertTrue(all(b['conditional_plateau_screen_pass'] for b in blocks));self.assertTrue(all(b['account_lineage_uncertainty_present'] for b in blocks))
  target=combination_id(ANCHORS[0],1,1)
  for r in rows:
   if r['candidate_id']==target and r['window_id']=='RECENT_2021_TO_CUTOFF':r['closed_trades']=59
  failed=regions(candidates(),rows);self.assertTrue(all(not b['conditional_plateau_screen_pass'] for b in failed if b['anchor_id']==ANCHORS[0]))
 def test_guard_accepts_only_counter_drift(self):
  with tempfile.TemporaryDirectory() as tmp:
   path=Path(tmp)/'source_provenance.json';digest,record=next((k,v) for k,v in REFERENCES.items() if v['archive']=='AUDUSD_CONDITIONAL_SUMMARY.zip');original=copy.deepcopy(record['payload']);changed=copy.deepcopy(original)
   changed['new_geometry_paths_computed']=0;path.write_bytes(e.canon(changed));_,check=verify_report(path,digest);self.assertEqual(check['classification'],'COUNTERS_ONLY')
   changed['history']['m15']['observed_bars']+=1;path.write_bytes(e.canon(changed))
   with self.assertRaisesRegex(ValueError,'locked fields'):verify_report(path,digest)
 def test_output_roots_and_http_are_protected(self):
  for parent in ('/data/p32_audusd_conditional','/data/p32_audusd_lifetime_extension','/data/p32_pullback_pass1','/data/p32_audusd_rr'):
   with self.assertRaisesRegex(ValueError,'separate'):Research(dict(AUDUSD_COMBINATIONS_OUTPUT_DIR=parent+'/child'))
  with tempfile.TemporaryDirectory() as tmp:
   job=Research(dict(AUDUSD_COMBINATIONS_OUTPUT_DIR=tmp+'/out'));app=application(job);codes=[]
   for path,method in [('/health','GET'),('/status','HEAD'),('/orders','POST')]:list(app(dict(PATH_INFO=path,REQUEST_METHOD=method),lambda s,h:codes.append(s)))
   self.assertEqual(codes,['200 OK','200 OK','405 Method Not Allowed']);self.assertIsNone(job.thread)
