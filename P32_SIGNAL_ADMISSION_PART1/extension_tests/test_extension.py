"""Offline causality, source-freeze, parity and complete 126-account tests."""
import copy
import gzip
import io
import json
import sqlite3
import tempfile
import threading
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from fixture import SyntheticStore
from pullback_lab import core as e
from pullback_lab.storage import Store,load_bars
from pullback_lab.signals import generate
from audusd_matrix.signals import generate_grid
from audusd_matrix.pool import Pool
from audusd_matrix import io as x
from audusd_matrix.storage import frozen_store_class
from audusd_matrix.runner import Research as OldResearch
from audusd_extension.config import candidates,DISTANCE,BODY,OVERLAPS
from audusd_extension.reporting import regions
from audusd_extension.reuse import Previous,ReusePool,add_previous_sources,immutable_db
from audusd_extension.runner import Research,BASELINE
from audusd_extension.download import Stream
from audusd_pullback_extension import application

START='2020-03-02T00:00:00Z';END='2020-03-09T00:00:00Z';WARM='2020-01-01T00:00:00Z'


def history():
    store=SyntheticStore('unused')
    m15,_=load_bars(store,'AUD_USD','M15',e.dt(WARM),e.dt(START),e.dt(END))
    h1,_=load_bars(store,'AUD_USD','H1',e.dt(WARM),e.dt(START),e.dt(END))
    return store,m15,h1


def rows_for(cid):
    return [dict(candidate_id=cid,window_id=w,financing_mode=mode,closed_trades=300,
        conditional_ending_nav_gbp=130.,conditional_nav_cagr_pct=5.,mean_net_r_without_best_five=.05,
        uncertain_account_lineage=True) for w in ['FULL_HISTORY','RECENT_2021_TO_CUTOFF']+
        [f'ERA_{y}_{y+4}' for y in (2005,2010,2015,2020,2025)] for mode in ('FROZEN_14OCT2026_RATES','ADVERSE_6PCT_ANNUAL')]


class GridAndCausalityTests(unittest.TestCase):
    def test_fixed_nine_cells_and_two_original_controls_first(self):
        rules=candidates();self.assertEqual(len(rules),9)
        self.assertEqual({(r['distance_max'],r['body_min']) for r in rules},{(d,b) for d in DISTANCE for b in BODY})
        self.assertEqual(tuple(r['candidate_id'] for r in rules[:2]),OVERLAPS)
    def test_regenerated_baseline_and_overlap_signals_are_exact(self):
        store,m,h=history();old,_=generate_grid(m,h,START,END,START)
        new,_=generate_grid(m,h,START,END,START,rules=[BASELINE]+candidates())
        for cid in ('BASELINE',)+OVERLAPS:self.assertEqual(new[cid],old[cid])
        raw,_=generate('AUD_USD',m,h,e.dt(START),e.dt(END),indicator_ready_from=e.dt(START))
        raw=[s for s in raw if s['side']=='BUY' and s['control']=='TREND_PULLBACK_ENGULF']
        self.assertEqual(x.baseline_parity(new['BASELINE'],raw)['status'],'PASS')
        store.close()
    def test_all_new_rules_have_causal_h1_and_fixed_rr(self):
        store,m,h=history();new,_=generate_grid(m,h,START,END,START,rules=candidates())
        for rule in candidates():
            for s in new[rule['candidate_id']]:
                self.assertEqual(s['rr'],3.)
                self.assertGreaterEqual(s['features']['body_atr'],rule['body_min'])
                self.assertLessEqual(s['features']['h1_distance_atr'],rule['distance_max'])
                age=(e.dt(s['signal_close_utc'])-e.dt(s['features']['h1_source_close_utc'])).total_seconds()
                self.assertGreaterEqual(age,0);self.assertLess(age,3600)
        store.close()
    def test_future_prices_cannot_change_past_signals(self):
        store,m,h=history();before,_=generate_grid(m,h,START,END,START,rules=candidates())
        changed=copy.deepcopy(m);through='2020-03-06T00:00:00Z'
        for i,t in enumerate(changed.times):
            if t>=e.dt(through).timestamp():
                for component in changed.values:
                    for field in 'ohlc':changed.values[component][field][i]+=.1
        after,_=generate_grid(changed,h,START,END,START,rules=candidates())
        for cid in before:self.assertEqual([s for s in before[cid] if s['entry_utc']<through],[s for s in after[cid] if s['entry_utc']<through])
        store.close()
    def test_four_regions_and_no_isolated_peak_selection(self):
        rules=candidates();rows=[a for r in rules for a in rows_for(r['candidate_id'])]
        blocks=regions(rules,rows);self.assertEqual(len(blocks),4)
        self.assertTrue(all(b['conditional_plateau_screen_pass'] for b in blocks))
        corner=blocks[0]['member_candidates'][0]
        for r in rows:
            if r['candidate_id']==corner:r['mean_net_r_without_best_five']=-.01
        self.assertFalse(regions(rules,rows)[0]['conditional_plateau_screen_pass'])
    def test_original_sample_thresholds_are_not_relaxed(self):
        rules=candidates();rows=[a for r in rules for a in rows_for(r['candidate_id'])]
        for row in rows:
            if row['window_id']=='FULL_HISTORY':row['closed_trades']=199
        blocks=regions(rules,rows)
        self.assertTrue(all(not b['conditional_plateau_screen_pass'] for b in blocks))
        self.assertTrue(any('SMALL_SAMPLE' in reason for s in blocks[0]['member_screens'] for reason in s['screen_reasons']))
    def test_partial_accounts_cannot_pass(self):
        self.assertTrue(all(not b['conditional_plateau_screen_pass'] for b in regions(candidates(),[])))


class Reader:
    receipts=[]
    def __init__(self):self.changed=False;self.receipts=[]
    def get(self,ident):
        p={k:'1.26' if self.changed else '1.25' for k in 'ohlc'}
        return dict(instrument=ident['pair'],granularity=ident['granularity'],candles=[dict(time=ident['start'],complete=True,mid=p,bid=p,ask=p)])


class SourceAndServiceTests(unittest.TestCase):
    def test_previous_matrix_new_windows_are_also_frozen(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);reader=Reader();ident=Store.identity('AUD_USD','M1',e.dt('2020-01-01T00:00:00Z'),e.dt('2020-01-02T00:00:00Z'))
            expected=dict(ident,key=e.sha(e.canon(ident)),sha256=e.sha(e.canon(reader.get(ident))))
            cls=frozen_store_class(None,None,'test',raw_mb=64,reserve_mb=0);store=cls(root/'cache',reader=reader)
            add_previous_sources(store,SimpleNamespace(source_rows=[expected]),root/'nonexistent.sqlite3')
            reader.changed=True
            with self.assertRaisesRegex(ValueError,'changed on re-fetch'):store.exact('AUD_USD','M1',e.dt(ident['start']),e.dt(ident['end']))
            store.close()
    def test_conflicting_source_windows_fail_before_consumption(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);cls=frozen_store_class(None,None,'test',raw_mb=64,reserve_mb=0);store=cls(root/'cache',reader=Reader())
            ident=Store.identity('AUD_USD','M1',e.dt('2020-01-01T00:00:00Z'),e.dt('2020-01-02T00:00:00Z'))
            row=dict(ident,key=e.sha(e.canon(ident)),sha256='a'*64)
            add_previous_sources(store,SimpleNamespace(source_rows=[row]),root/'none')
            with self.assertRaisesRegex(ValueError,'disagree'):add_previous_sources(store,SimpleNamespace(source_rows=[dict(row,sha256='b'*64)]),root/'none')
            store.close()
    def test_previous_path_cache_reused_without_writing_to_it(self):
        store,m,h=history();signals,_=generate_grid(m,h,START,END,START,rules=candidates());s=signals[OVERLAPS[0]][0]
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);prior=root/'prior';current=root/'current';prior.mkdir();current.mkdir()
            old=Pool(prior,store,m,'AUD_USD',e.dt(END),x.PINS['signature']);p=old.get(s);old.close()
            checksum=x.sha_file(prior/'paths.sqlite3')
            new=ReusePool(current,store,m,'AUD_USD',e.dt(END),x.PINS['signature'],previous=SimpleNamespace(path_db=prior/'paths.sqlite3'))
            with patch.object(Pool,'compute',side_effect=AssertionError('Existing path recomputed')):self.assertEqual(new.get(s),p)
            new.close();self.assertEqual(checksum,x.sha_file(prior/'paths.sqlite3'))
        store.close()
    def test_wal_sources_are_not_read_as_immutable(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'test.db'
            with sqlite3.connect(p) as con:con.execute('CREATE TABLE paths(key TEXT)')
            Path(str(p)+'-wal').write_bytes(b'writer-active')
            self.assertIsNone(immutable_db(p,('paths',)))
    def test_health_head_post_do_not_start_research(self):
        with tempfile.TemporaryDirectory() as tmp:
            job=Research(dict(AUDUSD_EXTENSION_OUTPUT_DIR=tmp));app=application(job);codes=[]
            for path,method in [('/health','GET'),('/status','HEAD'),('/orders','POST')]:list(app(dict(PATH_INFO=path,REQUEST_METHOD=method),lambda s,h:codes.append(s)))
            self.assertEqual(codes,['200 OK','200 OK','405 Method Not Allowed']);self.assertIsNone(job.thread);self.assertFalse(job.work.exists())
    def test_old_output_directories_protected(self):
        with self.assertRaisesRegex(ValueError,'separate'):Research(dict(AUDUSD_EXTENSION_OUTPUT_DIR='/data/p32_audusd_long_matrix/child'))
    def test_stream_is_valid_without_full_zip_on_disk(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'REVIEW.md').write_text('Synthetic')
            (root/'run_state.json').write_text(json.dumps(dict(status='PARTIAL',published_candidates=[])))
            job=SimpleNamespace(work=root,signature='TEST',lock=threading.RLock())
            data=b''.join(Stream(job,True))
            with zipfile.ZipFile(io.BytesIO(data)) as z:
                self.assertIsNone(z.testzip())
                for name,w in json.loads(z.read('report_manifest.json')).items():self.assertEqual(e.sha(z.read(name)),w['sha256'])
            self.assertFalse(list(root.glob('*.zip')))


class FullReplayTests(unittest.TestCase):
    def test_old_224_and_new_126_accounts_exact_overlap_resume_and_corruption(self):
        store,m,h=history();source=dict(start=START,end=END,warmup_start=WARM)
        original,_=generate('AUD_USD',m,h,e.dt(START),e.dt(END),indicator_ready_from=e.dt(START))
        original=[s for s in original if s['side']=='BUY' and s['control']=='TREND_PULLBACK_ENGULF']
        coverage=dict(eligibility=dict(effective_evaluation_start_utc=START))
        eras=[dict(window_id=f'ERA_{y}_{y+4}',start=START,end=END) for y in (2005,2010,2015,2020,2025)]
        eras.append(dict(window_id='RECENT_2021_TO_CUTOFF',start=START,end=END))
        def prepare(job):return source,coverage,original
        def load(job,st,src):return m,h,e.dt(START),dict(fixture='SYNTHETIC_ONLY')
        def make(job):return SyntheticStore('unused')
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);oldroot=root/'original_matrix';newroot=root/'extension'
            common=[patch.object(OldResearch,'prepare_source',prepare),patch.object(OldResearch,'load_history',load),
                    patch.object(OldResearch,'make_store',make),patch.object(Pool,'seed_source',return_value=dict(synthetic=True)),
                    patch('pullback_lab.runner.persistence',return_value=dict(separate_mount_detected=True))]
            from contextlib import ExitStack
            with ExitStack() as stack:
                for item in common:stack.enter_context(item)
                stack.enter_context(patch('audusd_matrix.runner.windows',return_value=eras))
                old=OldResearch(dict(AUDUSD_MATRIX_OUTPUT_DIR=str(oldroot)));old.run()
                self.assertEqual(old.state['status'],'COMPLETE',old.state)
            # The synthetic store has no real price-source identities. This is
            # fixture metadata only; production imports all real frozen rows.
            (old.work/'private_source_index.json.gz').write_bytes(gzip.compress(e.canon([])))
            names=('REVIEW.md','run_state.json','account_matrix.csv','protocol.json')
            pins=dict(signature=old.signature,files={n:dict(bytes=(old.work/n).stat().st_size,sha256=x.sha_file(old.work/n)) for n in names})
            def previous(path,progress):return Previous(path,progress,pins=pins)
            before={p.relative_to(oldroot).as_posix():x.sha_file(p) for p in oldroot.rglob('*') if p.is_file()}
            env=dict(AUDUSD_MATRIX_OUTPUT_DIR=str(oldroot),AUDUSD_EXTENSION_OUTPUT_DIR=str(newroot))
            with ExitStack() as stack:
                stack.enter_context(patch.object(Research,'prepare_source',prepare))
                stack.enter_context(patch.object(Research,'load_history',load))
                stack.enter_context(patch.object(Research,'make_store',make))
                stack.enter_context(patch.object(Pool,'seed_source',return_value=dict(synthetic=True)))
                stack.enter_context(patch('pullback_lab.runner.persistence',return_value=dict(separate_mount_detected=True)))
                stack.enter_context(patch('audusd_extension.runner.Previous',side_effect=previous))
                stack.enter_context(patch('audusd_extension.runner.windows',return_value=eras))
                stack.enter_context(patch('socket.create_connection',side_effect=AssertionError('No live HTTP in tests')))
                job=Research(env);job.run()
                self.assertEqual(job.state['status'],'COMPLETE',job.state)
                self.assertEqual(job.state['accounts_completed'],126);self.assertEqual(job.state['candidates_completed'],9)
                self.assertEqual(job.state['overlap_account_payloads_matched'],28)
                self.assertEqual(len(list(job.work.glob('candidates/*/accounts/*.json.gz'))),126)
                parity=json.loads((job.work/'overlap_parity.json').read_bytes());self.assertEqual(parity['status'],'PASS')
                with zipfile.ZipFile(io.BytesIO(b''.join(Stream(job,True)))) as z:
                    self.assertIsNone(z.testzip());self.assertEqual(json.loads(z.read('RUN_COMPLETE.json'))['accounts_completed'],126)
                    self.assertIn('coverage_context.json',z.namelist())
                with patch('audusd_extension.runner.Replay.run',side_effect=AssertionError('Completed accounts replayed')):
                    again=Research(env);again.run();self.assertEqual(again.state['status'],'COMPLETE',again.state)
                after={p.relative_to(oldroot).as_posix():x.sha_file(p) for p in oldroot.rglob('*') if p.is_file()}
                self.assertEqual(before,after)
                # Full payload parity catches a change even when a headline
                # such as closed-trade count would remain unchanged.
                real_checkpoint=Previous.checkpoint
                def changed(prior,cid,window,mode):
                    result=copy.deepcopy(real_checkpoint(prior,cid,window,mode))
                    result['summary']['ending_balance_gbp']+=.0001
                    return result
                with patch.object(Previous,'checkpoint',changed):
                    failed=Research(env);failed.run();self.assertEqual(failed.state['status'],'ERROR');self.assertIn('payload differs',failed.state['error'])
                target=next(job.work.glob('candidates/*/accounts/*.json.gz'));target.write_bytes(b'corrupt')
                failed=Research(env);failed.run();self.assertEqual(failed.state['status'],'ERROR');self.assertIn('report changed',failed.state['error'])
        store.close()


if __name__=='__main__':unittest.main()
