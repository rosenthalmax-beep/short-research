import copy,gzip,io,json,tempfile,unittest,zipfile
from pathlib import Path
from contextlib import ExitStack
from unittest.mock import patch
from fixture import SyntheticStore
from test_rr import history,START,END,WARM
from pullback_lab import core as e
from pullback_lab.signals import generate
from audusd_matrix import io as x
from audusd_matrix.pool import Pool
from audusd_matrix.runner import Research as OriginalResearch
from audusd_extension.runner import Research as ExtensionResearch
from audusd_extension.reuse import Previous as MatrixPrevious
from audusd_lifetime.runner import Research as PriorLifetimeResearch
from audusd_lifetime.reuse import Previous as PriorLifetimePrevious
from audusd_lifetime_extension.runner import Research as PriorExtensionResearch
from audusd_lifetime_extension.reuse import Previous as PriorExtensionPrevious
from audusd_rr.reuse import Previous
from audusd_conditional.runner import Research as ConditionalResearch
from audusd_conditional.download import Stream
from audusd_combinations.runner import Research
from audusd_combinations.reuse import Previous as CombinationPrevious
from audusd_combinations.download import Stream as CombinationStream
# A two-day synthetic slice exercises all 18 combinations (three signals each),
# exits, NY17 funding, all 448 reset accounts and the complete source contract.
# Market-history validation remains on the service; extra synthetic days add no
# distinct software branch coverage.
END='2020-03-04T00:00:00Z'

class CombinationReplayTests(unittest.TestCase):
 def setUp(self):
  # Synthetic candle data is deterministic. Reuse completed M15/H1 windows
  # directly, avoiding repeated fake candle construction for the 7 reset
  # accounts. All production source and account code remains unmodified.
  from functools import lru_cache
  raw=SyntheticStore.range
  cached=lru_cache(maxsize=32)(raw)
  def repeat(store,pair,gran,start,end):
   return cached(store,pair,gran,start,end) if gran in ('M15','H1') else raw(store,pair,gran,start,end)
  patcher=patch.object(SyntheticStore,'range',repeat);patcher.start()
  self.addCleanup(patcher.stop);self.addCleanup(cached.cache_clear)

 def test_complete_448_accounts_196_exact_controls_resume_and_tamper(self):
  store,m,h=history();source=dict(start=START,end=END,warmup_start=WARM)
  original,_=generate('AUD_USD',m,h,e.dt(START),e.dt(END),indicator_ready_from=e.dt(START));original=[s for s in original if s['side']=='BUY' and s['control']=='TREND_PULLBACK_ENGULF']
  coverage=dict(eligibility=dict(effective_evaluation_start_utc=START))
  ws=[dict(window_id=f'ERA_{y}_{y+4}',start=START,end=END) for y in (2005,2010,2015,2020,2025)]+[dict(window_id='RECENT_2021_TO_CUTOFF',start=START,end=END)]
  def prepare(job):return source,coverage,original
  def load(job,st,src):return m,h,e.dt(START),dict(fixture='SYNTHETIC_ONLY')
  def make(job):return SyntheticStore('unused')
  def fixture_pins(job):
   names=('REVIEW.md','run_state.json','account_matrix.csv','protocol.json','previous_matrix_witness.json','source_provenance.json')
   return dict(signature=job.signature,completed_accounts=job.state['accounts_completed'],completed_candidates=job.state['candidates_completed'],files={n:dict(bytes=(job.work/n).stat().st_size,sha256=x.sha_file(job.work/n)) for n in names if (job.work/n).is_file()})
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);oldroot=root/'matrix';extroot=root/'extension';priorroot=root/'lifetime';firstextroot=root/'final_extension';newroot=root/'conditional';combination_root=root/'combinations'
   with ExitStack() as stack:
    for obj in [OriginalResearch,ExtensionResearch,PriorLifetimeResearch,PriorExtensionResearch,ConditionalResearch,Research]:
     stack.enter_context(patch.object(obj,'prepare_source',prepare));stack.enter_context(patch.object(obj,'load_history',load));stack.enter_context(patch.object(obj,'make_store',make))
    stack.enter_context(patch.object(Pool,'seed_source',return_value=dict(synthetic=True)))
    stack.enter_context(patch('pullback_lab.runner.persistence',return_value=dict(separate_mount_detected=True)))
    stack.enter_context(patch('socket.create_connection',side_effect=AssertionError('No live HTTP in tests')))
    for module in ['audusd_matrix.runner','audusd_extension.runner','audusd_lifetime.runner','audusd_lifetime_extension.runner','audusd_conditional.runner','audusd_combinations.runner']:stack.enter_context(patch(module+'.windows',return_value=ws))
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
    stack.enter_context(patch('audusd_conditional.runner.Previous',side_effect=previous))
    before={str(p.relative_to(root)):x.sha_file(p) for parent in [oldroot,extroot,priorroot,firstextroot] for p in parent.rglob('*') if p.is_file()}
    env=dict(AUDUSD_MATRIX_OUTPUT_DIR=str(oldroot),AUDUSD_EXTENSION_OUTPUT_DIR=str(extroot),AUDUSD_LIFETIME_OUTPUT_DIR=str(priorroot),AUDUSD_LIFETIME_EXTENSION_OUTPUT_DIR=str(firstextroot),AUDUSD_CONDITIONAL_OUTPUT_DIR=str(newroot))
    # Build a focused synthetic predecessor fixture: the six legacy controls
    # plus all fourteen reference cases needed here (two are shared). This is
    # an 18-case, 252-account TEST fixture, not a fake 79-case market result.
    from audusd_conditional.config import candidates as all_prior_candidates
    from audusd_combinations.config import OVERLAPS
    selected=[r for r in all_prior_candidates() if r['kind']=='CONTROL' or r['candidate_id'] in OVERLAPS]
    self.assertEqual(len(selected),18)
    stack.enter_context(patch('audusd_conditional.config.candidates',return_value=selected))
    stack.enter_context(patch('audusd_conditional.runner.candidates',return_value=selected))
    job=ConditionalResearch(env);job.run();self.assertEqual(job.state['status'],'COMPLETE',job.state)
    self.assertEqual(job.state['accounts_completed'],252);self.assertEqual(job.state['overlap_account_payloads_matched'],84)
    (job.work/'private_source_index.json.gz').write_bytes(gzip.compress(e.canon([])))
    new_pins=fixture_pins(job)
    stack.enter_context(patch.dict('audusd_combinations.runner.PINS',{'signature':job.signature}))
    import audusd_combinations.provenance_guard as combo_guard
    stack.enter_context(patch.dict(combo_guard.REFERENCES,guard.REFERENCES))
    new_prov=job.work/'source_provenance.json';reference=json.loads(new_prov.read_bytes());digest=e.sha(e.canon(reference))
    stack.enter_context(patch.dict(combo_guard.REFERENCES,{digest:dict(archive='SYNTHETIC_ONLY',payload=reference)}))
    changed_counters=copy.deepcopy(reference)
    for field in combo_guard.COUNTERS:changed_counters[field]=0
    new_prov.write_bytes(e.canon(changed_counters))
    stack.enter_context(patch('audusd_combinations.runner.Previous',side_effect=lambda path,progress,**kw:CombinationPrevious(path,progress,pins=new_pins,**kw)))
    env['AUDUSD_COMBINATIONS_OUTPUT_DIR']=str(combination_root)
    parents=[oldroot,extroot,priorroot,firstextroot,newroot]
    before={str(p.relative_to(root)):x.sha_file(p) for parent in parents for p in parent.rglob('*') if p.is_file()}
    combined=Research(env);combined.run();self.assertEqual(combined.state['status'],'COMPLETE',combined.state)
    self.assertEqual(combined.state['accounts_completed'],448);self.assertEqual(combined.state['overlap_account_payloads_matched'],196)
    self.assertEqual(len(list(combined.work.glob('candidates/*/accounts/*.json.gz'))),448)
    for directory in combined.work.glob('candidates/*__C*'):
     with gzip.open(directory/'signals.jsonl.gz','rt') as handle:self.assertGreater(len(list(handle)),0)
    # The additional ancestor level and source immutability are exercised too.
    ancestry=combined.previous.ancestors(oldroot,extroot,priorroot);self.assertEqual(len(ancestry),4);combined.previous.assert_unchanged()
    with zipfile.ZipFile(io.BytesIO(b''.join(CombinationStream(combined,True)))) as z:
     self.assertIsNone(z.testzip());state=json.loads(z.read('RUN_COMPLETE.json'));self.assertEqual(state['accounts_completed'],448)
     self.assertEqual(state['overlap_account_payloads_matched'],196)
     for name,w in json.loads(z.read('report_manifest.json')).items():self.assertEqual(e.sha(z.read(name)),w['sha256'])
     self.assertIn('trade_attribution.csv',z.namelist())
     import csv,math,collections
     accts={(r['candidate_id'],r['window_id'],r['financing_mode']):r for r in csv.DictReader(io.StringIO(z.read('account_matrix.csv').decode()))}
     groups=collections.defaultdict(dict)
     for row in csv.DictReader(io.StringIO(z.read('trade_attribution.csv').decode())):
      key=tuple(row[k] for k in ('candidate_id','reference_candidate_id','window_id','financing_mode'));groups[key][row['population']]=row
     self.assertTrue(groups)
     for (cid,reference,w,mode),parts in groups.items():
      self.assertEqual(set(parts),{'NEW','REMOVED','SHARED'})
      for owner,ident,pops in [('candidate',cid,('NEW','SHARED')),('reference',reference,('REMOVED','SHARED'))]:
       expected=accts[ident,w,mode];self.assertEqual(sum(int(parts[p]['count']) for p in pops),int(expected['closed_trades']))
       self.assertTrue(math.isclose(sum(float(parts[p][owner+'_net_r']) for p in pops),float(expected['net_r_sum_actual_entry_risk']),abs_tol=1e-8))
     import csv
     rows=list(csv.DictReader(io.StringIO(z.read('plateau_regions.csv').decode())));self.assertEqual(len(rows),8)
    # A partial restart reruns only the removed checkpoint; completed neighbours survive.
    damaged=next(combined.work.glob('candidates/*__C*/accounts/*.json.gz'));(damaged.parents[1]/'COMPLETE.json').unlink();damaged.unlink()
    from audusd_combinations.runner import Replay
    original_replay=Replay.run;calls=[]
    def rerun(self):calls.append(1);return original_replay(self)
    with patch.object(Replay,'run',rerun):
     partial=Research(env);partial.run();self.assertEqual(partial.state['status'],'COMPLETE',partial.state);self.assertEqual(len(calls),1)
    with patch('audusd_combinations.runner.Replay.run',side_effect=AssertionError('Completed account was replayed')):
     again=Research(env);again.run();self.assertEqual(again.state['status'],'COMPLETE',again.state)
    after={str(p.relative_to(root)):x.sha_file(p) for parent in parents for p in parent.rglob('*') if p.is_file()};self.assertEqual(before,after)
    checkpoint=CombinationPrevious.checkpoint
    def altered(prior,cid,w,mode):
     result=copy.deepcopy(checkpoint(prior,cid,w,mode));result['summary']['ending_balance_gbp']+=.0001;return result
    with patch.object(CombinationPrevious,'checkpoint',altered):
     failed=Research(env);failed.run();self.assertEqual(failed.state['status'],'ERROR');self.assertIn('payload differs',failed.state['error'])
    damaged.write_bytes(b'corrupt')
    failed=Research(env);failed.run();self.assertEqual(failed.state['status'],'ERROR');self.assertIn('report changed',failed.state['error'])
  store.close()
