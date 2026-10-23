import copy,gzip,json,tempfile,unittest,zipfile
from pathlib import Path
from unittest.mock import patch
from pullback_lab import core as e
from pullback_lab.storage import load_bars
from audusd_matrix import io as x
from audusd_matrix.signals import generate_grid as original_generate
from audusd_frequency.analysis import Audit,RULES,summarize
from audusd_frequency.generator import generate_grid
from audusd_frequency.runner import Research,NativeReader,exact_control,PINS
from audusd_pullback_frequency import application
from fixture import SyntheticStore

START='2020-04-01T00:00:00Z';END='2020-08-01T00:00:00Z';WARM='2020-01-01T00:00:00Z'
NOW=dict(o=1.07,c=1.14,h=1.15,l=1.00);PREV=dict(o=1.13,c=1.08,h=1.14,l=1.07)
FEATURE=dict(body_atr=1.3,h1_distance_atr=1.)

class LifecycleTests(unittest.TestCase):
 def make(self):
  times=[int(e.dt('2020-01-01T00:00:00Z').timestamp())+i*900 for i in range(30)]
  a=Audit(RULES[1],times,'2020-01-01T00:00:00Z','2020-01-02T00:00:00Z');s=dict(index=1,extreme=.99,area=1.1,start_atr=.1)
  a.active=a.new(1,s);return a,dict(setup=s,armed=False)
 def step(self,a,s,i,f=None,trend=True):
  return a.step(s,NOW,PREV,trend,1.1,1.1,.1,i,FEATURE if f is None else f,lambda r:r['body_atr']>=1.25 and r['h1_distance_atr']<=1.25)
 def test_expiry_is_before_ninth_bar_but_not_eighth(self):
  a,s=self.make();picked,_=self.step(a,s,8);self.assertIsNotNone(picked)
  a,s=self.make();picked,_=self.step(a,s,9);self.assertIsNone(picked)
  self.assertEqual(a.setups[0]['termination'],'EXPIRED');self.assertEqual(a.late[0]['bar_age'],9)
  self.assertEqual(a.late[0]['implied_stop'],.98990)
 def test_original_low_is_updated_and_only_first_late_pass_counts(self):
  a,s=self.make();s['setup']['extreme']=1.02;self.step(a,s,9)
  self.assertEqual(a.late[0]['inherited_low'],1.)
  for i in range(10,18):self.step(a,s,i)
  self.assertEqual(len(a.late),1)
 def test_gap_and_trend_cancel_monitors(self):
  for reason in ('GAP_RESET','TREND_RESET'):
   a,s=self.make();self.step(a,s,9,dict(body_atr=.8,h1_distance_atr=1.))
   if reason=='GAP_RESET':a.reset_gap(10)
   else:self.step(a,s,10,trend=False)
   self.assertEqual(a.late[0]['observation_status'],reason)
 def test_trend_reset_has_priority_over_expiry(self):
  a,s=self.make();self.step(a,s,9,trend=False)
  self.assertEqual(a.setups[0]['termination'],'TREND_RESET');self.assertEqual(a.late,[])
 def test_failed_filter_retains_original_touch_and_exclusive_reasons(self):
  a,s=self.make()
  for i,(b,d,label) in enumerate([(1.1,1.,'BODY_ONLY'),(1.3,1.4,'DISTANCE_ONLY'),(1.1,1.4,'BOTH')],3):
   self.step(a,s,i,dict(body_atr=b,h1_distance_atr=d));self.assertEqual(s['setup']['index'],1)
   self.assertEqual(a.triggers[-1]['outcome'],label)
  self.step(a,s,6);self.assertIsNone(s['setup']);self.assertEqual(a.setups[0]['failed_trigger_count'],3)
 def test_age16_is_included_and_observation_then_stops(self):
  for qualifies in (True,False):
   a,s=self.make()
   for i in range(9,16):self.step(a,s,i,dict(body_atr=.8,h1_distance_atr=1.))
   self.step(a,s,16,None if qualifies else dict(body_atr=.8,h1_distance_atr=1.))
   self.assertEqual(a.late[0]['observation_status'],'QUALIFIED_AFTER_EXPIRY' if qualifies else 'NO_PASS_THROUGH_16')
   self.assertFalse(a.monitors)
 def test_overlap_and_censoring_are_explicit_and_counts_reconcile(self):
  a,s=self.make();self.step(a,s,9);r=a.late[0]
  a.annotate([dict(signal_close_utc=r['observation_close_utc'],stop=r['implied_stop'])])
  z=summarize(a,'2020-01-01T00:00:00Z','2020-01-02T00:00:00Z','FULL')
  self.assertTrue(z['setup_fates_reconcile']);self.assertTrue(z['late_observations_reconcile'])
  self.assertEqual(z['late_passes_also_baseline_signal_same_close'],1)
  self.assertEqual(z['late_passes_with_no_baseline_signal_same_close'],0)
  a,s=self.make();a.finish(2);self.assertEqual(a.setups[0]['termination'],'RIGHT_CENSORED')

class GenerationTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  store=SyntheticStore(None)
  cls.m,mc=load_bars(store,'AUD_USD','M15',e.dt(WARM),e.dt(START),e.dt(END))
  cls.h,hc=load_bars(store,'AUD_USD','H1',e.dt(WARM),e.dt(START),e.dt(END))
  cls.ready=max(e.dt(mc['indicator_ready_from_utc']),e.dt(hc['indicator_ready_from_utc']),e.dt(START))
 def grid(self,end=END,m=None,h=None):
  m=self.m if m is None else m;h=self.h if h is None else h
  audits={r['candidate_id']:Audit(r,m.times,e.stamp(self.ready),end) for r in RULES}
  signals,counts=generate_grid(m,h,START,end,self.ready,rules=RULES,audits=audits)
  for cid,a in audits.items():a.annotate(signals[cid])
  return signals,counts,audits
 def test_all_payloads_and_counts_match_original_generator(self):
  expected,counts=original_generate(self.m,self.h,START,END,self.ready,rules=RULES)
  actual,ac,audits=self.grid();self.assertEqual(actual,expected);self.assertEqual(ac,counts)
  self.assertGreater(sum(len(a.triggers) for a in audits.values()),0)
  for cid,a in audits.items():self.assertEqual(sum(s['termination']=='ACCEPTED' for s in a.setups if s['started_in_report_window']),len(actual[cid]))
 def test_future_bars_cannot_change_earlier_signals_or_attempts(self):
  end='2020-06-01T00:00:00Z';cut=int(e.dt(end).timestamp())
  def truncate(b,step):
   out=copy.deepcopy(b);n=sum(t+step<=cut for t in out.times);out.times=out.times[:n]
   for side in out.values:
    for k in out.values[side]:out.values[side][k]=out.values[side][k][:n]
   return out
  full,_,fa=self.grid();small,_,sa=self.grid(end,truncate(self.m,900),truncate(self.h,3600))
  for cid in full:
   self.assertEqual([r for r in full[cid] if r['signal_close_utc']<end],small[cid])
   self.assertEqual([r for r in fa[cid].triggers if r['signal_close_utc']<end],[r for r in sa[cid].triggers if r['signal_close_utc']<end])
 def test_saved_control_field_mutation_is_rejected_even_with_valid_hash(self):
  signals,_,_=self.grid();cid=RULES[0]['candidate_id']
  with tempfile.TemporaryDirectory() as tmp:
   d=Path(tmp)/'candidates'/cid;d.mkdir(parents=True)
   def save(rows):
    x.write_jsonl(d/'signals.jsonl.gz',rows)
    (d/'COMPLETE.json').write_bytes(e.canon(dict(signature=PINS['signature'],candidate_id=cid,files={'signals.jsonl.gz':x.sha_file(d/'signals.jsonl.gz')})))
   save(signals[cid]);self.assertEqual(exact_control(Path(tmp),cid,signals[cid])['status'],'PASS')
   changed=copy.deepcopy(signals[cid])
   if changed:changed[0]['stop']+=.00001
   else:changed=[dict(changed=True)]
   save(changed)
   with self.assertRaisesRegex(ValueError,'exactly match'):exact_control(Path(tmp),cid,signals[cid])
 def test_synthetic_full_runner_publish_and_resume_without_replay(self):
  signals,_,_=self.grid()
  base_rule=dict(candidate_id='BASELINE',kind='BASELINE',distance_max=None,body_min=None)
  baseline,_=original_generate(self.m,self.h,START,END,self.ready,rules=[base_rule])
  original=[{k:v for k,v in s.items() if k!='candidate_id'} for s in baseline['BASELINE']]
  class Store:
   reader=type('Reader',(),dict(calls=0))();reused=0
   def index(self):return []
   def close(self):pass
  with tempfile.TemporaryDirectory() as tmp:
   env=dict(AUDUSD_FREQUENCY_OUTPUT_DIR=tmp+'/output',AUDUSD_EXTENSION_OUTPUT_DIR=tmp+'/extension')
   job=Research(env);job.extension.mkdir(parents=True);(job.extension.parent/'runner.lock').touch()
   for cid in signals:
    d=job.extension/'candidates'/cid;d.mkdir(parents=True);x.write_jsonl(d/'signals.jsonl.gz',signals[cid])
    x.atomic(d/'COMPLETE.json',e.canon(dict(signature=PINS['signature'],candidate_id=cid,files={'signals.jsonl.gz':x.sha_file(d/'signals.jsonl.gz')})))
   source=dict(start=START,end=END,warmup_start=WARM)
   coverage=dict(eligibility=dict(effective_evaluation_start_utc=e.stamp(self.ready)))
   with patch('audusd_frequency.runner.persistence',return_value=dict(separate_mount_detected=True)),patch.object(job,'verify_extension',return_value=dict(status='SYNTHETIC')),patch.object(job,'prepare_source',return_value=(source,coverage,original)),patch.object(job,'make_store',return_value=Store()),patch.object(job,'load_history',return_value=(self.m,self.h,self.ready,{})):
    job.run()
   self.assertEqual(job.state['status'],'COMPLETE',job.state)
   with zipfile.ZipFile(job.work/'AUDUSD_FREQUENCY_SUMMARY.zip') as z:
    self.assertEqual(json.loads(z.read('parity.json'))['status'],'PASS')
    self.assertEqual(json.loads(z.read('status.json'))['status'],'COMPLETE')
    self.assertFalse(any(n.startswith('candidates/') for n in z.namelist()))
   self.assertTrue(job.resume_complete());self.assertTrue(job.state['resumed_without_history_load'])

class ServiceTests(unittest.TestCase):
 def test_health_head_and_post_never_start_job(self):
  with tempfile.TemporaryDirectory() as tmp:
   job=Research(dict(AUDUSD_FREQUENCY_OUTPUT_DIR=tmp+'/out'));app=application(job);codes=[]
   for p,m in [('/health','GET'),('/status','HEAD'),('/orders','POST')]:list(app(dict(PATH_INFO=p,REQUEST_METHOD=m),lambda s,h:codes.append(s)))
   self.assertEqual(codes,['200 OK','200 OK','405 Method Not Allowed']);self.assertIsNone(job.thread)
 def test_native_reader_restricts_instrument_timeframes_and_budget(self):
  class R:
   receipts=[]
   def get(self,i):return {}
  r=NativeReader(R())
  for i in [dict(pair='GBP_USD',granularity='M15'),dict(pair='AUD_USD',granularity='M1')]:
   with self.assertRaises(ValueError):r.get(i)
  for i in range(128):r.get(dict(pair='AUD_USD',granularity='H1'))
  with self.assertRaisesRegex(ValueError,'budget'):r.get(dict(pair='AUD_USD',granularity='H1'))
 def test_changed_completed_report_is_refused_on_resume(self):
  with tempfile.TemporaryDirectory() as tmp:
   j=Research(dict(AUDUSD_FREQUENCY_OUTPUT_DIR=tmp+'/out'));j.work.mkdir(parents=True)
   p=j.work/'REVIEW.md';p.write_text('original')
   (j.work/'RUN_COMPLETE.json').write_bytes(e.canon(dict(signature=j.signature,files={'REVIEW.md':x.sha_file(p)})))
   p.write_text('changed')
   with self.assertRaisesRegex(ValueError,'missing or changed'):j.resume_complete()
 def test_existing_native_cache_is_opened_read_only(self):
  import sqlite3
  from audusd_matrix.runner import Research as Parent
  with tempfile.TemporaryDirectory() as tmp:
   j=Research(dict(AUDUSD_FREQUENCY_OUTPUT_DIR=tmp+'/out',AUDUSD_MATRIX_OUTPUT_DIR=tmp+'/old',AUDUSD_EXTENSION_OUTPUT_DIR=tmp+'/ext'))
   p=j.old_matrix/'canonical_candles/candles.sqlite3';p.parent.mkdir(parents=True)
   with sqlite3.connect(p) as c:c.execute('CREATE TABLE blocks(key TEXT PRIMARY KEY,identity TEXT,body BLOB,sha256 TEXT)')
   before=x.sha_file(p)
   class Raw:
    receipts=[]
   class Fake:
    reader=Raw();legacy=[]
   with patch.object(Parent,'make_store',return_value=Fake()):store=j.make_store()
   self.assertEqual(len(store.legacy),1)
   with self.assertRaises(sqlite3.OperationalError):store.legacy[0][1].execute("INSERT INTO blocks VALUES('a','b',X'00','c')")
   store.legacy[0][1].close();self.assertEqual(before,x.sha_file(p))
 def test_changed_extension_witness_is_refused(self):
  import audusd_frequency.runner as module
  with tempfile.TemporaryDirectory() as tmp:
   j=Research(dict(AUDUSD_FREQUENCY_OUTPUT_DIR=tmp+'/out',AUDUSD_EXTENSION_OUTPUT_DIR=tmp+'/ext'))
   j.extension.mkdir(parents=True)
   p=j.extension/'source_provenance.json';p.write_text('{"original_matrix_code":{}}')
   pins=dict(PINS,files={'source_provenance.json':x.sha_file(p)})
   with patch.object(module,'PINS',pins):
    self.assertEqual(j.verify_extension()['status'],'PASS')
    p.write_text('changed')
    with self.assertRaisesRegex(ValueError,'missing or changed'):j.verify_extension()
 def test_old_sources_cannot_be_output_directories(self):
  for p in ['/data/p32_pullback_pass1','/data/p32_audusd_long_matrix','/data/p32_audusd_long_extension']:
   with self.assertRaises(ValueError):Research(dict(AUDUSD_FREQUENCY_OUTPUT_DIR=p))

if __name__=='__main__':unittest.main()
