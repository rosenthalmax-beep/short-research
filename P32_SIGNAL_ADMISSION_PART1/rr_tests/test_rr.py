"""Offline lifecycle, source-freeze, exact parity and full account replay checks."""
import copy,gzip,io,json,sqlite3,tempfile,threading,unittest,zipfile
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from fixture import SyntheticStore
from pullback_lab import core as e
from pullback_lab.storage import Store,load_bars
from pullback_lab.signals import generate
from audusd_matrix import io as x
from audusd_matrix.pool import Pool
from audusd_lifetime.signals import generate_grid as old_generate
from audusd_matrix.runner import Research as OriginalResearch
from audusd_matrix.storage import frozen_store_class
from audusd_extension.runner import Research as ExtensionResearch
from audusd_extension.reuse import Previous as MatrixPrevious
from audusd_rr.config import candidates,DISTANCE,LIFETIME,OVERLAPS
from audusd_lifetime.signals import evaluate
from audusd_rr.signals import generate_grid,qualification_parity
from audusd_rr.reuse import Previous,ReusePool,add_previous_sources,immutable_db
from audusd_rr.reporting import regions,comparisons
from audusd_rr.runner import Research,BASELINE
from audusd_lifetime_extension.runner import Research as PriorExtensionResearch
from audusd_lifetime_extension.reuse import Previous as PriorExtensionPrevious
from audusd_lifetime.runner import Research as PriorLifetimeResearch
from audusd_lifetime.reuse import Previous as PriorLifetimePrevious
from audusd_rr.download import Stream
from audusd_pullback_rr import application

START='2020-03-02T00:00:00Z';END='2020-03-09T00:00:00Z';WARM='2020-01-01T00:00:00Z'


def history():
 st=SyntheticStore('unused');m,_=load_bars(st,'AUD_USD','M15',e.dt(WARM),e.dt(START),e.dt(END));h,_=load_bars(st,'AUD_USD','H1',e.dt(WARM),e.dt(START),e.dt(END));return st,m,h


def sample_rows(cid):
 return [dict(candidate_id=cid,window_id=w,financing_mode=mode,closed_trades=300,conditional_ending_nav_gbp=130.,
  conditional_nav_cagr_pct=5.,mean_net_r_without_best_five=.05,uncertain_account_lineage=True)
  for w in ['FULL_HISTORY','RECENT_2021_TO_CUTOFF']+[f'ERA_{y}_{y+4}' for y in (2005,2010,2015,2020,2025)]
  for mode in ('FROZEN_14OCT2026_RATES','ADVERSE_6PCT_ANNUAL')]


class LifetimeTests(unittest.TestCase):
 def transition(self,n,index,state=None,accept=True,low=1.,trend=True):
  if state is None:state=dict(setup=dict(index=10,extreme=.99,area=1.1,start_atr=.1),armed=False)
  picked,count=evaluate(state,dict(o=1.07,c=1.14,h=1.15,l=low),dict(o=1.13,c=1.08,h=1.14,l=1.07),trend,1.1,1.1,.1,index,dict(body_atr=1.3),lambda f:accept,n)
  return picked,count,state
 def test_each_lifetime_includes_last_bar_and_excludes_next(self):
  for n in LIFETIME:
   picked,_,_=self.transition(n,10+n-1);self.assertIsNotNone(picked);self.assertEqual(picked['features']['duration_bars'],n)
   picked,counts,state=self.transition(n,10+n);self.assertIsNone(picked);self.assertEqual(counts['expired'],1);self.assertIsNone(state['setup'])
 def test_failed_filter_retains_touch_and_all_lows_until_later_acceptance(self):
  _,_,state=self.transition(24,30,accept=False,low=.98)
  self.assertEqual(state['setup']['index'],10);self.assertEqual(state['setup']['extreme'],.98)
  picked,_,state=self.transition(24,33,state=state,low=1.)
  self.assertEqual(picked['setup']['extreme'],.98);self.assertEqual(picked['features']['duration_bars'],24);self.assertIsNone(state['setup'])
 def test_trend_failure_still_resets_long_lived_setup(self):
  picked,_,state=self.transition(16,18,trend=False);self.assertIsNone(picked);self.assertIsNone(state['setup']);self.assertFalse(state['armed'])
 def test_fixed_eighteen_cells_and_six_rr3_controls_first(self):
  rules=candidates();self.assertEqual(len(rules),18)
  self.assertEqual({(r['distance_max'],r['lifetime_bars'],r['rr']) for r in rules},{(d,n,rr) for d in DISTANCE for n in LIFETIME for rr in (2.5,3.,3.5)})
  self.assertEqual(tuple(r['candidate_id'] for r in rules[:6]),OVERLAPS)
  self.assertTrue(all(r['body_min']==1.25 for r in rules))
 def test_unfiltered_and_all_rr3_control_signals_are_exact(self):
  st,m,h=history();rules=[BASELINE]+candidates();actual,counts=generate_grid(m,h,START,END,START,rules=rules)
  old_rules=[{k:v for k,v in r.items() if k!='rr'} for r in rules if r['candidate_id']=='BASELINE' or r.get('rr')==3.]
  expected,ec=old_generate(m,h,START,END,START,rules=old_rules)
  for cid in ('BASELINE',)+OVERLAPS:self.assertEqual(actual[cid],expected[cid]);self.assertEqual(counts[cid],ec[cid])
  raw,_=generate('AUD_USD',m,h,e.dt(START),e.dt(END),indicator_ready_from=e.dt(START));raw=[s for s in raw if s['side']=='BUY' and s['control']=='TREND_PULLBACK_ENGULF']
  self.assertEqual(x.baseline_parity(actual['BASELINE'],raw)['status'],'PASS');st.close()
 def test_future_prices_do_not_change_past_signals_and_h1_is_causal(self):
  st,m,h=history();before,_=generate_grid(m,h,START,END,START);changed=copy.deepcopy(m);through='2020-03-06T00:00:00Z'
  for i,t in enumerate(changed.times):
   if t>=e.dt(through).timestamp():
    for component in changed.values:
     for field in 'ohlc':changed.values[component][field][i]+=.1
  after,_=generate_grid(changed,h,START,END,START)
  self.assertGreater(sum(map(len,before.values())),0)
  for rule in candidates():
   cid=rule['candidate_id'];self.assertEqual([s for s in before[cid] if s['entry_utc']<through],[s for s in after[cid] if s['entry_utc']<through])
   for s in before[cid]:
    self.assertEqual(s['rr'],rule['rr']);self.assertLessEqual(s['features']['duration_bars'],rule['lifetime_bars'])
    self.assertGreaterEqual(s['features']['body_atr'],1.25);self.assertLessEqual(s['features']['h1_distance_atr'],rule['distance_max'])
    age=(e.dt(s['signal_close_utc'])-e.dt(s['features']['h1_source_close_utc'])).total_seconds();self.assertGreaterEqual(age,0);self.assertLess(age,3600)
  st.close()
 def test_four_eight_member_regions_require_all_neighbours_and_original_samples(self):
  rules=candidates();rows=[r for rule in rules for r in sample_rows(rule['candidate_id'])]
  blocks=regions(rules,rows);self.assertEqual(len(blocks),4);self.assertTrue(all(b['conditional_plateau_screen_pass'] and len(b['member_candidates'])==8 for b in blocks))
  corner=blocks[0]['member_candidates'][0]
  for r in rows:
   if r['candidate_id']==corner and r['window_id']=='FULL_HISTORY':r['closed_trades']=199
  self.assertFalse(regions(rules,rows)[0]['conditional_plateau_screen_pass']);self.assertTrue(all(not b['conditional_plateau_screen_pass'] for b in regions(rules,[])))
 def test_comparisons_only_use_same_distance_window_and_funding(self):
  a=dict(candidate_id='OLD',rr=3.,distance_max=1.25,lifetime_bars=16,window_id='FULL_HISTORY',financing_mode='FROZEN',closed_trades=100,conditional_nav_cagr_pct=1.,sampled_mid_nav_drawdown_pct=-15.)
  b=dict(a,candidate_id='NEW',rr=2.5,closed_trades=150,conditional_nav_cagr_pct=2.,sampled_mid_nav_drawdown_pct=-12.)
  result=comparisons([a,b]);self.assertEqual(result[1]['closed_trades_delta'],50);self.assertEqual(result[1]['cagr_difference_percentage_points'],1.)
  self.assertEqual(result[1]['sampled_nav_drawdown_difference_percentage_points'],3.)
  self.assertEqual(result[1]['closed_trades_ratio'],1.5)
  self.assertEqual(len(comparisons([a,dict(b,financing_mode='OTHER')])),1)


class Reader:
 def __init__(self):self.changed=False;self.receipts=[]
 def get(self,ident):
  p={k:'1.26' if self.changed else '1.25' for k in 'ohlc'}
  return dict(instrument=ident['pair'],granularity=ident['granularity'],candles=[dict(time=ident['start'],complete=True,mid=p,bid=p,ask=p)])


class SourceTests(unittest.TestCase):
 def test_frozen_new_extension_source_revision_and_conflicts_are_refused(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);reader=Reader();ident=Store.identity('AUD_USD','M1',e.dt('2020-01-01T00:00:00Z'),e.dt('2020-01-02T00:00:00Z'))
   row=dict(ident,key=e.sha(e.canon(ident)),sha256=e.sha(e.canon(reader.get(ident))))
   cls=frozen_store_class(None,None,'test',raw_mb=64,reserve_mb=0);st=cls(root/'cache',reader=reader)
   add_previous_sources(st,SimpleNamespace(source_rows=[row]),root/'none')
   with self.assertRaisesRegex(ValueError,'disagree'):add_previous_sources(st,SimpleNamespace(source_rows=[dict(row,sha256='b'*64)]),root/'none')
   reader.changed=True
   with self.assertRaisesRegex(ValueError,'changed on re-fetch'):st.exact('AUD_USD','M1',e.dt(ident['start']),e.dt(ident['end']))
   st.close()
 def test_previous_geometry_reuses_identical_history_without_old_db_writes(self):
  st,m,h=history();signals,_=generate_grid(m,h,START,END,START);s=next(s for group in signals.values() for s in group)
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);prior=root/'prior';new=root/'new';prior.mkdir();new.mkdir()
   p=Pool(prior,st,m,'AUD_USD',e.dt(END),x.PINS['signature']);expected=p.get(s);p.close();before=x.sha_file(prior/'paths.sqlite3')
   pool=ReusePool(new,st,m,'AUD_USD',e.dt(END),x.PINS['signature'],previous=SimpleNamespace(path_db=prior/'paths.sqlite3'))
   with patch.object(Pool,'compute',side_effect=AssertionError('Existing geometry recomputed')):self.assertEqual(pool.get(s),expected)
   pool.close();self.assertEqual(before,x.sha_file(prior/'paths.sqlite3'))
  st.close()
 def test_live_wal_cache_is_not_read_as_immutable(self):
  with tempfile.TemporaryDirectory() as tmp:
   p=Path(tmp)/'test.db'
   with sqlite3.connect(p) as con:con.execute('CREATE TABLE paths(key TEXT)')
   Path(str(p)+'-wal').write_bytes(b'active');self.assertIsNone(immutable_db(p,('paths',)))
 def test_ancestor_chain_requires_all_three_indices_and_hashed_witnesses(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);matrix=root/'matrix'/'matrix_sig';extension=root/'extension'/'ext_sig';life=root/'life'/'life_sig';child=root/'latest'
   for p in [matrix,extension,life,child]:p.mkdir(parents=True)
   for p in [matrix,extension,life]:(p/'private_source_index.json.gz').write_bytes(gzip.compress(e.canon([])))
   def witness(signature,files):return dict(signature=signature,files=files,private_source_index_semantic_sha256=e.sha(e.canon([])))
   x.atomic(extension/'previous_matrix_witness.json',e.canon(witness('matrix_sig',{})))
   x.atomic(life/'previous_matrix_witness.json',e.canon(witness('ext_sig',{'previous_matrix_witness.json':x.sha_file(extension/'previous_matrix_witness.json')})))
   x.atomic(child/'previous_matrix_witness.json',e.canon(witness('life_sig',{'previous_matrix_witness.json':x.sha_file(life/'previous_matrix_witness.json')})))
   previous=Previous.__new__(Previous);previous.root=child
   result=previous.ancestors(root/'matrix',root/'extension',root/'life');self.assertEqual(len(result),3)
   self.assertTrue(all(not a.source_rows for a,c in result))
   (extension/'previous_matrix_witness.json').write_text('changed')
   with self.assertRaisesRegex(ValueError,'summary changed'):previous.ancestors(root/'matrix',root/'extension',root/'life')
 def test_regions_keep_account_uncertainty_visible(self):
  rules=candidates();rows=[r for rule in rules for r in sample_rows(rule['candidate_id'])]
  self.assertTrue(all(b['account_lineage_uncertainty_present'] for b in regions(rules,rows)))
 def test_health_head_and_post_do_not_start_research(self):
  with tempfile.TemporaryDirectory() as tmp:
   job=Research(dict(AUDUSD_RR_OUTPUT_DIR=tmp+'/out'));app=application(job);codes=[]
   for p,m in [('/health','GET'),('/status','HEAD'),('/orders','POST')]:list(app(dict(PATH_INFO=p,REQUEST_METHOD=m),lambda s,h:codes.append(s)))
   self.assertEqual(codes,['200 OK','200 OK','405 Method Not Allowed']);self.assertIsNone(job.thread)
 def test_source_output_locations_are_protected(self):
  for old in ['/data/p32_audusd_long_matrix','/data/p32_audusd_long_extension','/data/p32_pullback_pass1','/data/p32_audusd_frequency','/data/p32_audusd_lifetime','/data/p32_audusd_lifetime_extension']:
   with self.assertRaisesRegex(ValueError,'separate'):Research(dict(AUDUSD_RR_OUTPUT_DIR=old+'/child'))
 def test_summary_stream_manifest_and_no_large_archive_disk_copy(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);(root/'REVIEW.md').write_text('Synthetic');(root/'run_state.json').write_text(json.dumps(dict(status='PARTIAL',published_candidates=[])))
   job=SimpleNamespace(work=root,signature='TEST',lock=threading.RLock());data=b''.join(Stream(job,True))
   with zipfile.ZipFile(io.BytesIO(data)) as z:
    self.assertIsNone(z.testzip())
    for n,w in json.loads(z.read('report_manifest.json')).items():self.assertEqual(e.sha(z.read(n)),w['sha256'])
   self.assertFalse(list(root.glob('*.zip')))


class ReplayTests(unittest.TestCase):
 def test_full_reference_chain_and_252_rr_accounts_exact_84_controls_resume_tamper(self):
  store,m,h=history();source=dict(start=START,end=END,warmup_start=WARM)
  original,_=generate('AUD_USD',m,h,e.dt(START),e.dt(END),indicator_ready_from=e.dt(START));original=[s for s in original if s['side']=='BUY' and s['control']=='TREND_PULLBACK_ENGULF']
  coverage=dict(eligibility=dict(effective_evaluation_start_utc=START))
  ws=[dict(window_id=f'ERA_{y}_{y+4}',start=START,end=END) for y in (2005,2010,2015,2020,2025)]+[dict(window_id='RECENT_2021_TO_CUTOFF',start=START,end=END)]
  def prepare(job):return source,coverage,original
  def load(job,st,src):return m,h,e.dt(START),dict(fixture='SYNTHETIC_ONLY')
  def make(job):return SyntheticStore('unused')
  def fixture_pins(job):
   names=('REVIEW.md','run_state.json','account_matrix.csv','protocol.json','previous_matrix_witness.json','source_provenance.json')
   return dict(signature=job.signature,files={n:dict(bytes=(job.work/n).stat().st_size,sha256=x.sha_file(job.work/n)) for n in names if (job.work/n).is_file()})
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);oldroot=root/'matrix';extroot=root/'extension';priorroot=root/'lifetime';firstextroot=root/'final_extension';newroot=root/'rr'
   with ExitStack() as stack:
    for obj in [OriginalResearch,ExtensionResearch,PriorLifetimeResearch,PriorExtensionResearch,Research]:
     stack.enter_context(patch.object(obj,'prepare_source',prepare));stack.enter_context(patch.object(obj,'load_history',load));stack.enter_context(patch.object(obj,'make_store',make))
    stack.enter_context(patch.object(Pool,'seed_source',return_value=dict(synthetic=True)))
    stack.enter_context(patch('pullback_lab.runner.persistence',return_value=dict(separate_mount_detected=True)))
    stack.enter_context(patch('socket.create_connection',side_effect=AssertionError('No live HTTP in tests')))
    for module in ['audusd_matrix.runner','audusd_extension.runner','audusd_lifetime.runner','audusd_lifetime_extension.runner','audusd_rr.runner']:stack.enter_context(patch(module+'.windows',return_value=ws))
    old=OriginalResearch(dict(AUDUSD_MATRIX_OUTPUT_DIR=str(oldroot)));old.run();self.assertEqual(old.state['status'],'COMPLETE',old.state)
    (old.work/'private_source_index.json.gz').write_bytes(gzip.compress(e.canon([])))
    pins=fixture_pins(old)
    stack.enter_context(patch('audusd_extension.runner.Previous',side_effect=lambda path,progress:MatrixPrevious(path,progress,pins=pins)))
    extension=ExtensionResearch(dict(AUDUSD_MATRIX_OUTPUT_DIR=str(oldroot),AUDUSD_EXTENSION_OUTPUT_DIR=str(extroot)));extension.run()
    self.assertEqual(extension.state['status'],'COMPLETE',extension.state)
    (extension.work/'private_source_index.json.gz').write_bytes(gzip.compress(e.canon([])))
    ext_pins=fixture_pins(extension)
    stack.enter_context(patch('audusd_lifetime.runner.Previous',side_effect=lambda path,progress:PriorLifetimePrevious(path,progress,pins=ext_pins)))
    prior=PriorLifetimeResearch(dict(AUDUSD_MATRIX_OUTPUT_DIR=str(oldroot),AUDUSD_EXTENSION_OUTPUT_DIR=str(extroot),AUDUSD_LIFETIME_OUTPUT_DIR=str(priorroot)))
    prior.run();self.assertEqual(prior.state['status'],'COMPLETE',prior.state)
    (prior.work/'private_source_index.json.gz').write_bytes(gzip.compress(e.canon([])))
    prior_pins=fixture_pins(prior)
    stack.enter_context(patch('audusd_lifetime_extension.runner.Previous',side_effect=lambda path,progress:PriorExtensionPrevious(path,progress,pins=prior_pins)))
    extjob=PriorExtensionResearch(dict(AUDUSD_MATRIX_OUTPUT_DIR=str(oldroot),AUDUSD_EXTENSION_OUTPUT_DIR=str(extroot),AUDUSD_LIFETIME_OUTPUT_DIR=str(priorroot),AUDUSD_LIFETIME_EXTENSION_OUTPUT_DIR=str(firstextroot)))
    extjob.run();self.assertEqual(extjob.state['status'],'COMPLETE',extjob.state)
    (extjob.work/'private_source_index.json.gz').write_bytes(gzip.compress(e.canon([])))
    rr_pins=fixture_pins(extjob)
    # Reproduce the reported failure: completed prior results stay unchanged,
    # but a resume rewrites only its three operational path-work counters.
    import audusd_rr.provenance_guard as guard
    prov_path=extjob.work/'source_provenance.json';reviewed=json.loads(prov_path.read_bytes())
    reviewed_sha=e.sha(e.canon(reviewed))
    stack.enter_context(patch.dict(guard.REFERENCES,{reviewed_sha:dict(archive='SYNTHETIC_TEST_ONLY',payload=reviewed)}))
    resumed=copy.deepcopy(reviewed)
    for field in guard.COUNTERS:resumed[field]=0
    prov_path.write_bytes(e.canon(resumed))
    def previous(path,progress):return Previous(path,progress,pins=rr_pins)
    stack.enter_context(patch('audusd_rr.runner.Previous',side_effect=previous))
    before={str(p.relative_to(root)):x.sha_file(p) for parent in [oldroot,extroot,priorroot,firstextroot] for p in parent.rglob('*') if p.is_file()}
    env=dict(AUDUSD_MATRIX_OUTPUT_DIR=str(oldroot),AUDUSD_EXTENSION_OUTPUT_DIR=str(extroot),AUDUSD_LIFETIME_OUTPUT_DIR=str(priorroot),AUDUSD_LIFETIME_EXTENSION_OUTPUT_DIR=str(firstextroot),AUDUSD_RR_OUTPUT_DIR=str(newroot))
    job=Research(env);job.run();self.assertEqual(job.state['status'],'COMPLETE',job.state)
    self.assertEqual(job.state['accounts_completed'],252);self.assertEqual(job.state['overlap_account_payloads_matched'],84)
    audit=json.loads((job.work/'provenance_verification.json').read_bytes())
    self.assertTrue(any(c['classification']=='COUNTERS_ONLY' for c in audit['checks']))
    self.assertTrue(all(c['locked_fields_match'] for c in audit['checks']))
    self.assertEqual(len(list(job.work.glob('candidates/*/accounts/*.json.gz'))),252)
    with zipfile.ZipFile(io.BytesIO(b''.join(Stream(job,True)))) as z:
     self.assertIsNone(z.testzip());state=json.loads(z.read('RUN_COMPLETE.json'));self.assertEqual(state['accounts_completed'],252)
     self.assertEqual(state['overlap_account_payloads_matched'],84);self.assertIn('control_comparisons.csv',z.namelist())
    with patch('audusd_rr.runner.Replay.run',side_effect=AssertionError('Completed account was replayed')):
     again=Research(env);again.run();self.assertEqual(again.state['status'],'COMPLETE',again.state)
    after={str(p.relative_to(root)):x.sha_file(p) for parent in [oldroot,extroot,priorroot,firstextroot] for p in parent.rglob('*') if p.is_file()};self.assertEqual(before,after)
    checkpoint=Previous.checkpoint
    def changed(prior,cid,w,mode):
     r=copy.deepcopy(checkpoint(prior,cid,w,mode));r['summary']['ending_balance_gbp']+=.0001;return r
    with patch.object(Previous,'checkpoint',changed):
     failed=Research(env);failed.run();self.assertEqual(failed.state['status'],'ERROR');self.assertIn('payload differs',failed.state['error'])
    target=next(job.work.glob('candidates/*/accounts/*.json.gz'));target.write_bytes(b'corrupt')
    failed=Research(env);failed.run();self.assertEqual(failed.state['status'],'ERROR');self.assertIn('report changed',failed.state['error'])
  store.close()


class RRSpecificTests(unittest.TestCase):
 def test_qualification_sequences_identical_and_target_mutation_refused(self):
  st,m,h=history();signals,counts=generate_grid(m,h,START,END,START)
  self.assertEqual(qualification_parity(signals)['status'],'PASS')
  cid=next(k for k in signals if signals[k] and '_RR2.5' in k)
  changed=copy.deepcopy(signals);changed[cid][0]['target']+=.00001
  with self.assertRaisesRegex(ValueError,'geometry mismatch'):qualification_parity(changed)
  changed=copy.deepcopy(signals);changed[cid][0]['stop']-=.00001
  with self.assertRaisesRegex(ValueError,'qualification times'):qualification_parity(changed)
  st.close()
 def test_rr_geometry_has_distinct_paths_and_different_barrier_outcomes(self):
  from pullback_lab.storage import Bars
  epoch=e.dt('2020-01-01T12:00:00Z');seconds=int(epoch.timestamp())
  points=[(0,1.2),(900,1.2001),(1800,1.2026),(2700,1.1989),(3600,1.1995)]
  def price(t):
   q=t-seconds
   for (a,x),(b,y) in zip(points,points[1:]):
    if a<=q<=b:return round(x+(y-x)*(q-a)/(b-a),8)
   return points[-1][1]
  class Curve:
   def range(self,pair,gran,start,end):
    step=e.STEP[gran];rows=[]
    for t in range(int(start.timestamp()),int(end.timestamp()),step):
     values=[price(t),price(t+step)];p=dict(o=values[0],h=max(values),l=min(values),c=values[1])
     rows.append(dict(time=e.stamp(e.datetime.fromtimestamp(t,e.UTC)),complete=True,mid=p,bid=p,ask=p))
    return rows
  st=Curve();bars=Bars();end=epoch+e.timedelta(hours=1)
  for row in st.range('AUD_USD','M15',epoch,end):bars.append(row)
  with tempfile.TemporaryDirectory() as tmp:
   pool=Pool(tmp,st,bars,'AUD_USD',end,x.PINS['signature']);found=[]
   for rr in (2.5,3.,3.5):
    s=dict(pair='AUD_USD',side='BUY',entry_utc=e.stamp(epoch),reference_entry=1.2,stop=1.199,rr=rr)
    found.append(pool.get(s))
   self.assertEqual(len({r['path_cache_key'] for r in found}),3)
   self.assertEqual([r['kind'] for r in found],['TARGET','STOP','STOP'])
   self.assertTrue(all(r['status']=='CLOSED_RESOLVED' for r in found));self.assertEqual(found[0]['exit_price'],1.2025)
   pool.close()


if __name__=='__main__':unittest.main()
