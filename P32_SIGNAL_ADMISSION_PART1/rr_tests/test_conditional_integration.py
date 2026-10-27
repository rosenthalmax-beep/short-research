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
from audusd_conditional.runner import Research
from audusd_conditional.download import Stream
class ReplayTests(unittest.TestCase):
 def test_full_chain_1106_accounts_exact_84_controls_resume_tamper(self):
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
    for module in ['audusd_matrix.runner','audusd_extension.runner','audusd_lifetime.runner','audusd_lifetime_extension.runner','audusd_conditional.runner']:stack.enter_context(patch(module+'.windows',return_value=ws))
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
    job=Research(env);job.run();self.assertEqual(job.state['status'],'COMPLETE',job.state)
    self.assertEqual(job.state['accounts_completed'],1106);self.assertEqual(job.state['overlap_account_payloads_matched'],84)
    audit=json.loads((job.work/'provenance_verification.json').read_bytes())
    self.assertTrue(any(c['classification']=='COUNTERS_ONLY' for c in audit['checks']))
    self.assertTrue(all(c['locked_fields_match'] for c in audit['checks']))
    self.assertEqual(len(list(job.work.glob('candidates/*/accounts/*.json.gz'))),1106)
    with zipfile.ZipFile(io.BytesIO(b''.join(Stream(job,True)))) as z:
     self.assertIsNone(z.testzip());state=json.loads(z.read('RUN_COMPLETE.json'));self.assertEqual(state['accounts_completed'],1106)
     self.assertEqual(state['overlap_account_payloads_matched'],84);self.assertIn('control_comparisons.csv',z.namelist());self.assertIn('trade_attribution.csv',z.namelist())
    with patch('audusd_conditional.runner.Replay.run',side_effect=AssertionError('Completed account was replayed')):
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


