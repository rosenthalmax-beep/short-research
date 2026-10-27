import copy,math,unittest,tempfile
from test_rr import history,START,END
from audusd_conditional.features import ratios,preceding_lows
from audusd_conditional.config import candidates,OVERLAPS
from audusd_conditional.signals import generate_grid,passes,evaluate
from audusd_lifetime.signals import generate_grid as old_generate
from audusd_pullback_conditional import application
from audusd_conditional.runner import Research,BASELINE
class ConditionalTests(unittest.TestCase):
 def test_predeclared_plan(self):
  rules=candidates();self.assertEqual(len(rules),79);self.assertEqual(len({r['candidate_id'] for r in rules}),79)
  self.assertEqual(tuple(r['candidate_id'] for r in rules[:6]),OVERLAPS);self.assertTrue(all(r['rr']==3 for r in rules));self.assertEqual(sum(r['kind']=='ONE_FEATURE' for r in rules),72)
 def test_baseline_and_seven_controls_unchanged(self):
  st,m,h=history();rules=[BASELINE]+[r for r in candidates() if r['kind']!='ONE_FEATURE']
  actual,counts=generate_grid(m,h,START,END,START,rules=rules);expected,ec=old_generate(m,h,START,END,START,rules=rules)
  self.assertEqual(actual,expected);self.assertEqual(counts,ec);st.close()
 def test_future_prices_do_not_change_past_signals(self):
  st,m,h=history();rules=candidates();before,_=generate_grid(m,h,START,END,START,rules=rules)
  changed=copy.deepcopy(m);hh=copy.deepcopy(h);cut='2020-03-06T00:00:00Z'
  from pullback_lab import core as e
  for bars in (changed,hh):
   for i,t in enumerate(bars.times):
    if t>=e.dt(cut).timestamp():
     for side in bars.values:
      for field in 'ohlc':bars.values[side][field][i]+=.1
  after,_=generate_grid(changed,hh,START,END,START,rules=rules);self.assertGreater(sum(map(len,before.values())),0)
  for r in rules:
   cid=r['candidate_id'];self.assertEqual([s for s in before[cid] if s['entry_utc']<cut],[s for s in after[cid] if s['entry_utc']<cut])
   if r['kind']=='ONE_FEATURE':
    for s in before[cid]:
     self.assertTrue(passes(r,s['features']));self.assertNotIn('_setup_features',s['features']);self.assertLessEqual(s['features']['h1_source_close_utc'],s['entry_utc'])
  st.close()
 def test_volatility_normalizer_excludes_current(self):
  self.assertEqual(ratios([2.]*50+[4.])[-1],2.);self.assertIsNone(ratios([2.]*49+[4.])[-1])
 def test_structure_excludes_signal(self):self.assertEqual(preceding_lows([5.,4.,3.,1.],3)[-1],3.)
 def test_missing_features_and_equality(self):
  r=dict(distance_max=1.25,body_min=1.,feature='x',operator='<=',threshold=.5);f=dict(h1_distance_atr=1.,body_atr=1.,x=.5);self.assertTrue(passes(r,f))
  for bad in (None,float('nan'),float('inf')):f['x']=bad;self.assertFalse(passes(r,f))
 def test_filter_failure_retains_original_touch(self):
  state=dict(setup=dict(index=10,extreme=.99,area=1.1,start_atr=.1),armed=False);now=dict(o=1.07,c=1.14,h=1.15,l=.98);prior=dict(o=1.13,c=1.08,h=1.14,l=1.07)
  picked,_=evaluate(state,now,prior,True,1.1,1.1,.1,12,{},lambda f:False,16);self.assertIsNone(picked);self.assertEqual(state['setup']['index'],10)
  picked,_=evaluate(state,now,prior,True,1.1,1.1,.1,13,{},lambda f:True,16);self.assertEqual(picked['setup']['extreme'],.98);self.assertIsNone(state['setup'])
 def test_health_head_and_post_do_not_start(self):
  with tempfile.TemporaryDirectory() as tmp:
   job=Research(dict(AUDUSD_CONDITIONAL_OUTPUT_DIR=tmp+'/out'));app=application(job);codes=[]
   for path,method in [('/health','GET'),('/status','HEAD'),('/orders','POST')]:list(app(dict(PATH_INFO=path,REQUEST_METHOD=method),lambda s,h:codes.append(s)))
   self.assertEqual(codes,['200 OK','200 OK','405 Method Not Allowed']);self.assertIsNone(job.thread)
