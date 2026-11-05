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
from audusd_body_recovery.runner import Research as BodyResearch
from audusd_body_recovery.reuse import Previous as BodyPrevious
from audusd_body_recovery.download import Stream as BodyStream
from audusd_resumption.runner import Research as ResumptionResearch
from audusd_resumption.reuse import Previous as ResumptionPrevious
from audusd_resumption.download import Stream as ResumptionStream
from audusd_resumption_cap.runner import Research as CapResearch
from audusd_resumption_cap.reuse import Previous as CapPrevious
from audusd_resumption_cap.download import Stream as CapStream
from audusd_combinations.reuse import Previous as CombinationPrevious
from audusd_combinations.download import Stream as CombinationStream
# A two-day synthetic slice exercises all body settings and six filter pairs,
# exits, NY17 funding, all 504 reset accounts and the complete source contract.
# Market-history validation remains on the service; extra synthetic days add no
# distinct software branch coverage.
END='2020-03-04T00:00:00Z'

class CapReplayTests(unittest.TestCase):
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

 def test_complete_112_accounts_70_controls_cap_regions_attribution_resume_and_tamper(self):
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
    for obj in [OriginalResearch,ExtensionResearch,PriorLifetimeResearch,PriorExtensionResearch,ConditionalResearch,Research,BodyResearch,ResumptionResearch,CapResearch]:
     stack.enter_context(patch.object(obj,'prepare_source',prepare));stack.enter_context(patch.object(obj,'load_history',load));stack.enter_context(patch.object(obj,'make_store',make))
    stack.enter_context(patch.object(Pool,'seed_source',return_value=dict(synthetic=True)))
    stack.enter_context(patch('pullback_lab.runner.persistence',return_value=dict(separate_mount_detected=True)))
    stack.enter_context(patch('socket.create_connection',side_effect=AssertionError('No live HTTP in tests')))
    for module in ['audusd_matrix.runner','audusd_extension.runner','audusd_lifetime.runner','audusd_lifetime_extension.runner','audusd_conditional.runner','audusd_combinations.runner','audusd_body_recovery.runner','audusd_resumption.runner','audusd_resumption_cap.runner']:stack.enter_context(patch(module+'.windows',return_value=ws))
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
    # Focused frozen-engine TEST predecessor: 20 cases / 280 accounts.
    # Real service pins remain the reviewed 32 cases / 448 accounts.
    from audusd_combinations.config import candidates as full_combinations
    from audusd_body_recovery.config import OVERLAPS as BODY_CONTROLS
    combo_cases=[r for r in full_combinations() if r.get('is_control') or r['candidate_id'] in BODY_CONTROLS]
    self.assertEqual(len(combo_cases),20)
    stack.enter_context(patch('audusd_combinations.config.candidates',return_value=combo_cases))
    stack.enter_context(patch('audusd_combinations.runner.candidates',return_value=combo_cases))
    combined=Research(env);combined.run();self.assertEqual(combined.state['status'],'COMPLETE',combined.state)
    self.assertEqual(combined.state['accounts_completed'],280);self.assertEqual(combined.state['overlap_account_payloads_matched'],196)
    (combined.work/'private_source_index.json.gz').write_bytes(gzip.compress(e.canon([])))
    body_pins=fixture_pins(combined)
    stack.enter_context(patch.dict('audusd_body_recovery.runner.PINS',{'signature':combined.signature}))
    stack.enter_context(patch('audusd_body_recovery.runner.Previous',side_effect=lambda path,progress,**kw:BodyPrevious(path,progress,pins=body_pins,**kw)))
    import audusd_body_recovery.provenance_guard as body_guard
    stack.enter_context(patch.dict(body_guard.REFERENCES,combo_guard.REFERENCES))
    new_prov=combined.work/'source_provenance.json';reference=json.loads(new_prov.read_bytes());digest=e.sha(e.canon(reference))
    stack.enter_context(patch.dict(body_guard.REFERENCES,{digest:dict(archive='SYNTHETIC_ONLY',payload=reference)}))
    changed_counters=copy.deepcopy(reference)
    for field in body_guard.COUNTERS:changed_counters[field]=0
    new_prov.write_bytes(e.canon(changed_counters))
    env['AUDUSD_BODY_RECOVERY_OUTPUT_DIR']=str(root/'body_recovery')
    parents.append(combination_root)
    before={str(p.relative_to(root)):x.sha_file(p) for parent in parents for p in parent.rglob('*') if p.is_file()}
    body=BodyResearch(env);body.run();self.assertEqual(body.state['status'],'COMPLETE',body.state)
    self.assertEqual(body.state['accounts_completed'],504);self.assertEqual(body.state['overlap_account_payloads_matched'],168)
    (body.work/'private_source_index.json.gz').write_bytes(gzip.compress(e.canon([])))
    res_pins=fixture_pins(body)
    stack.enter_context(patch.dict('audusd_resumption.runner.PINS',{'signature':body.signature}))
    stack.enter_context(patch('audusd_resumption.runner.Previous',side_effect=lambda path,progress,**kw:ResumptionPrevious(path,progress,pins=res_pins,**kw)))
    import audusd_resumption.provenance_guard as res_guard
    stack.enter_context(patch.dict(res_guard.REFERENCES,body_guard.REFERENCES))
    new_prov=body.work/'source_provenance.json';reference=json.loads(new_prov.read_bytes());digest=e.sha(e.canon(reference))
    stack.enter_context(patch.dict(res_guard.REFERENCES,{digest:dict(archive='SYNTHETIC_ONLY',payload=reference)}))
    changed_counters=copy.deepcopy(reference)
    for field in res_guard.COUNTERS:changed_counters[field]=0
    new_prov.write_bytes(e.canon(changed_counters))
    env['AUDUSD_RESUMPTION_OUTPUT_DIR']=str(root/'resumption')
    parents.append(root/'body_recovery')
    before={str(p.relative_to(root)):x.sha_file(p) for parent in parents for p in parent.rglob('*') if p.is_file()}
    res=ResumptionResearch(env);res.run();self.assertEqual(res.state['status'],'COMPLETE',res.state)
    self.assertEqual(res.state['accounts_completed'],70);self.assertEqual(res.state['overlap_account_payloads_matched'],42)
    (res.work/'private_source_index.json.gz').write_bytes(gzip.compress(e.canon([])))
    cap_pins=fixture_pins(res)
    stack.enter_context(patch.dict('audusd_resumption_cap.runner.PINS',{'signature':res.signature}))
    stack.enter_context(patch('audusd_resumption_cap.runner.Previous',side_effect=lambda path,progress,**kw:CapPrevious(path,progress,pins=cap_pins,**kw)))
    import audusd_resumption_cap.provenance_guard as cap_guard
    stack.enter_context(patch.dict(cap_guard.REFERENCES,res_guard.REFERENCES))
    new_prov=res.work/'source_provenance.json';reference=json.loads(new_prov.read_bytes());digest=e.sha(e.canon(reference))
    stack.enter_context(patch.dict(cap_guard.REFERENCES,{digest:dict(archive='SYNTHETIC_ONLY',payload=reference)}))
    changed_counters=copy.deepcopy(reference)
    for field in cap_guard.COUNTERS:changed_counters[field]=0
    new_prov.write_bytes(e.canon(changed_counters))
    env['AUDUSD_RESUMPTION_CAP_OUTPUT_DIR']=str(root/'resumption_cap');parents.append(root/'resumption')
    before={str(p.relative_to(root)):x.sha_file(p) for parent in parents for p in parent.rglob('*') if p.is_file()}
    cap=CapResearch(env);cap.run();self.assertEqual(cap.state['status'],'COMPLETE',cap.state)
    self.assertEqual(cap.state['accounts_completed'],112);self.assertEqual(cap.state['overlap_account_payloads_matched'],70)
    self.assertEqual(len(list(cap.work.glob('candidates/*/accounts/*.json.gz'))),112)
    ancestry=cap.previous.ancestors(oldroot,extroot,priorroot);self.assertEqual(len(ancestry),7);cap.previous.assert_unchanged()
    with zipfile.ZipFile(io.BytesIO(b''.join(CapStream(cap,True)))) as z:
     self.assertIsNone(z.testzip());state=json.loads(z.read('RUN_COMPLETE.json'));self.assertEqual(state['accounts_completed'],112)
     self.assertEqual(state['overlap_account_payloads_matched'],70);self.assertEqual(state['plateau_regions_tested'],3);self.assertEqual(state['plateau_regions_passing'],0)
     for name,w in json.loads(z.read('report_manifest.json')).items():self.assertEqual(e.sha(z.read(name)),w['sha256'])
     self.assertIn('cap_regions.csv',z.namelist());self.assertIn('signal_overlap.csv',z.namelist())
     import csv,math,collections
     accts={(r['candidate_id'],r['window_id'],r['financing_mode']):r for r in csv.DictReader(io.StringIO(z.read('account_matrix.csv').decode()))}
     groups=collections.defaultdict(dict)
     for row in csv.DictReader(io.StringIO(z.read('trade_attribution.csv').decode())):
      key=tuple(row[k] for k in ('candidate_id','reference_candidate_id','window_id','financing_mode'));groups[key][row['population']]=row
     self.assertEqual(len(groups),70)
     for (cid,reference,w,mode),parts in groups.items():
      self.assertEqual(set(parts),{'NEW','REMOVED','SHARED'})
      for owner,ident,pops in [('candidate',cid,('NEW','SHARED')),('reference',reference,('REMOVED','SHARED'))]:
       expected=accts[ident,w,mode];self.assertEqual(sum(int(parts[p]['count']) for p in pops),int(expected['closed_trades']))
       self.assertTrue(math.isclose(sum(float(parts[p][owner+'_net_r']) for p in pops),float(expected['net_r_sum_actual_entry_risk']),abs_tol=1e-8))
       payload=json.loads(gzip.decompress((cap.work/'candidates'/ident/'accounts'/(w+'__'+mode+'.json.gz')).read_bytes()))['result']
       closed_cash=sum(t['net_pl_gbp'] for t in payload['trades']);self.assertTrue(math.isclose(sum(float(parts[p][owner+'_net_cash_gbp']) for p in pops),closed_cash,abs_tol=.00011))
       self.assertTrue(math.isclose(closed_cash+sum(p['financing_gbp'] for p in payload['open_positions']),float(expected['net_booked_cash_gbp']),abs_tol=.0002))
     regions=list(csv.DictReader(io.StringIO(z.read('cap_regions.csv').decode())));self.assertEqual(len(regions),3);self.assertEqual([int(r['member_count']) for r in regions],[2,2,3]);self.assertTrue(all(r['all_member_accounts_complete']=='True' for r in regions))
     feats=list(csv.DictReader(io.StringIO(z.read('feature_diagnostics.csv').decode())));cov=list(csv.DictReader(io.StringIO(z.read('feature_coverage.csv').decode())))
     from audusd_resumption_cap.diagnostics import BINS
     self.assertEqual(len(cov),5*7*len(BINS));partitions=collections.defaultdict(list)
     for r in feats:partitions[r['candidate_id'],r['entry_cohort'],r['feature']].append(r);self.assertEqual(r['filtered_account_replay'],'False');self.assertEqual(r['cagr'],'')
     for r in cov:
      parts=partitions[r['candidate_id'],r['entry_cohort'],r['feature']];self.assertEqual(sum(int(p['qualifications']) for p in parts),int(r['all_raw_qualifications']))
      self.assertEqual(int(r['finite_feature_qualifications'])+int(r['missing_or_nonfinite_feature_qualifications']),int(r['all_raw_qualifications']))
     overlaps=list(csv.DictReader(io.StringIO(z.read('signal_overlap.csv').decode())));self.assertEqual(len(overlaps),35)
     for r in overlaps:
      self.assertEqual(int(r['shared_geometry'])+int(r['candidate_only_geometry']),int(r['candidate_raw_qualifications']))
      self.assertEqual(int(r['shared_geometry'])+int(r['reference_only_geometry']),int(r['reference_raw_qualifications']))
    damaged=next(cap.work.glob('candidates/RES2_H1DIST_0P25/accounts/*.json.gz'));(damaged.parents[1]/'COMPLETE.json').unlink();damaged.unlink()
    from audusd_resumption_cap.runner import Replay
    original_replay=Replay.run;calls=[]
    def rerun(self):calls.append(1);return original_replay(self)
    with patch.object(Replay,'run',rerun):
     partial=CapResearch(env);partial.run();self.assertEqual(partial.state['status'],'COMPLETE',partial.state);self.assertEqual(len(calls),1)
    with patch('audusd_resumption_cap.runner.Replay.run',side_effect=AssertionError('Completed account was replayed')):
     again=CapResearch(env);again.run();self.assertEqual(again.state['status'],'COMPLETE',again.state)
    after={str(p.relative_to(root)):x.sha_file(p) for parent in parents for p in parent.rglob('*') if p.is_file()};self.assertEqual(before,after)
    checkpoint=CapPrevious.checkpoint
    def altered(prior,cid,w,mode):
     result=copy.deepcopy(checkpoint(prior,cid,w,mode));result['summary']['ending_balance_gbp']+=.0001;return result
    with patch.object(CapPrevious,'checkpoint',altered):
     failed=CapResearch(env);failed.run();self.assertEqual(failed.state['status'],'ERROR');self.assertIn('payload differs',failed.state['error'])
    damaged.write_bytes(b'corrupt')
    failed=CapResearch(env);failed.run();self.assertEqual(failed.state['status'],'ERROR');self.assertIn('report changed',failed.state['error'])
  store.close()
