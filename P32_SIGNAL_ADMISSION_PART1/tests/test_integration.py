"""Synthetic full replay/resume; no broker requests or real return claims."""
import copy
import gzip
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch
from fixture import SyntheticStore
from pullback_lab import core as e
from pullback_lab.storage import load_bars
from pullback_lab.signals import generate
from pullback_lab.market import Market
from pullback_lab.accounting import Replay
from pullback_lab.funding import Funding
from pullback_lab.paths import truncate
from pullback_lab.broker_reconcile import reconcile
from audusd_matrix.signals import generate_grid
from audusd_matrix.runner import Research
from audusd_matrix.pool import Pool
from audusd_matrix.io import baseline_parity
from audusd_matrix.download import Stream

START='2020-03-02T00:00:00Z';END='2020-03-09T00:00:00Z';WARM='2020-01-01T00:00:00Z'


def history():
    store=SyntheticStore('unused')
    m15,_=load_bars(store,'AUD_USD','M15',e.dt(WARM),e.dt(START),e.dt(END))
    h1,_=load_bars(store,'AUD_USD','H1',e.dt(WARM),e.dt(START),e.dt(END))
    return store,m15,h1


class IntegrationTests(unittest.TestCase):
    def test_unfiltered_records_match_original_generator_and_future_bars_cannot_change_prefix(self):
        store,m15,h1=history()
        source,_=generate('AUD_USD',m15,h1,e.dt(START),e.dt(END),indicator_ready_from=e.dt(START))
        expected=[s for s in source if s['side']=='BUY' and s['control']=='TREND_PULLBACK_ENGULF']
        signals,_=generate_grid(m15,h1,START,END,START)
        self.assertGreater(len(expected),0)
        self.assertTrue(all(len(v)>0 for v in signals.values()))
        for rows in signals.values():
            for row in rows:
                age=(e.dt(row['signal_close_utc'])-e.dt(row['features']['h1_source_close_utc'])).total_seconds()
                self.assertGreaterEqual(age,0);self.assertLess(age,3600)
        self.assertEqual(baseline_parity(signals['BASELINE'],expected)['status'],'PASS')
        future='2020-03-06T00:00:00Z'
        # Prefix invariance: make much later prices materially different.
        modified=copy.deepcopy(m15)
        for i,t in enumerate(modified.times):
            if t>=e.dt(future).timestamp():
                for component in modified.values:
                    for field in 'ohlc':modified.values[component][field][i]+=.1
        after,_=generate_grid(modified,h1,START,END,START)
        for cid in signals:
            before_prefix=[s for s in signals[cid] if s['entry_utc']<future]
            after_prefix=[s for s in after[cid] if s['entry_utc']<future]
            self.assertEqual(before_prefix,after_prefix)
        store.close()

    def test_gap_reset_preserves_original_baseline_semantics(self):
        store,m15,h1=history()
        index=next(i for i,t in enumerate(m15.times) if t>=e.dt('2020-03-05T12:00:00Z').timestamp())
        del m15.times[index]
        for component in m15.values:
            for field in 'ohlc':del m15.values[component][field][index]
        old,_=generate('AUD_USD',m15,h1,e.dt(START),e.dt(END),indicator_ready_from=e.dt(START))
        old=[s for s in old if s['side']=='BUY' and s['control']=='TREND_PULLBACK_ENGULF']
        new,_=generate_grid(m15,h1,START,END,START)
        self.assertEqual(baseline_parity(new['BASELINE'],old)['status'],'PASS')
        store.close()

    def test_cash_results_identical_for_original_and_regenerated_baseline(self):
        store,m15,h1=history();old,_=generate('AUD_USD',m15,h1,e.dt(START),e.dt(END),indicator_ready_from=e.dt(START))
        old=[s for s in old if s['side']=='BUY' and s['control']=='TREND_PULLBACK_ENGULF']
        new,_=generate_grid(m15,h1,START,END,START)
        evidence=json.loads((Path(__file__).resolve().parents[1]/'pullback_lab/saved_broker_evidence.json').read_bytes())
        broker,_=reconcile(evidence);funding=Funding(evidence['funding_schedules'])
        with tempfile.TemporaryDirectory() as tmp:
            pool=Pool(Path(tmp),store,m15,'AUD_USD',e.dt(END),'SYNTHETIC_TEST')
            results=[]
            for selected in [old,new['BASELINE']]:
                result=Replay(selected,pool.get,Market(store),funding,evidence['instruments'],'END_ASC','FROZEN_14OCT2026_RATES',
                    broker['observed_symmetric_conversion_factor_markup'],initial=100.,start=START,end=END).run()
                self.assertEqual(result['summary']['status'],'COMPLETE_CONDITIONAL');results.append(result)
            self.assertGreater(results[0]['summary']['closed_trades'],0)
            self.assertEqual(results[0],results[1]);pool.close()
        store.close()

    def test_runner_publishes_all_224_checkpoints_and_resumes_without_replaying_accounts(self):
        # Exercise the production orchestrator on small deterministic data. Only
        # source loading, cache seeding and dates are replaced by fixture inputs.
        # Actual generator, path model, funding, account, checkpoints and reports run.
        store,m15,h1=history();source=dict(start=START,end=END,warmup_start=WARM)
        original,_=generate('AUD_USD',m15,h1,e.dt(START),e.dt(END),indicator_ready_from=e.dt(START))
        original=[s for s in original if s['side']=='BUY' and s['control']=='TREND_PULLBACK_ENGULF']
        coverage=dict(eligibility=dict(effective_evaluation_start_utc=START))
        eras=[dict(window_id=f'ERA_{y}_{y+4}',start=START,end=END) for y in (2005,2010,2015,2020,2025)]
        eras.append(dict(window_id='RECENT_2021_TO_CUTOFF',start=START,end=END))
        def fixture_source(job):return source,coverage,original
        def fixture_history(job,st,src):return m15,h1,e.dt(START),dict(source='SYNTHETIC_OFFLINE_TEST')
        def fixture_store(job):return SyntheticStore('unused')
        with tempfile.TemporaryDirectory() as tmp:
            env=dict(AUDUSD_MATRIX_OUTPUT_DIR=tmp)
            with patch.object(Research,'prepare_source',fixture_source),patch.object(Research,'load_history',fixture_history),patch.object(Research,'make_store',fixture_store),patch.object(Pool,'seed_source',return_value=dict(test_fixture=True)),patch('audusd_matrix.runner.windows',return_value=eras),patch('pullback_lab.runner.persistence',return_value=dict(separate_mount_detected=True)):
                job=Research(env);job.run()
                self.assertEqual(job.state['status'],'COMPLETE',job.state)
                self.assertEqual(job.state['candidates_completed'],16);self.assertEqual(job.state['accounts_completed'],224)
                saved=list(job.work.glob('candidates/*/accounts/*.json.gz'));self.assertEqual(len(saved),224)
                for cid in generate_grid(m15,h1,START,END,START)[0]:
                    rows=json.loads((job.work/'candidates'/cid/'account_summaries.json').read_bytes())
                    self.assertGreater(sum(r['closed_trades'] for r in rows),0,cid)
                for path in saved:
                    r=json.loads(gzip.decompress(path.read_bytes()))['result'];s=r['summary']
                    self.assertAlmostEqual(s['ending_balance_gbp'],100+s['price_pl_gbp']+s['financing_gbp'],places=4)
                with zipfile.ZipFile(io.BytesIO(b''.join(Stream(job,True)))) as z:
                    self.assertIsNone(z.testzip());self.assertEqual(json.loads(z.read('RUN_COMPLETE.json'))['accounts_completed'],224)
                # Completed reports are immutable witnesses for resume.
                with patch('audusd_matrix.runner.Replay.run',side_effect=AssertionError('Completed account replayed')):
                    again=Research(env);again.run();self.assertEqual(again.state['status'],'COMPLETE',again.state)
                target=next(job.work.glob('candidates/*/accounts/*.json.gz'));target.write_bytes(b'corrupt')
                failed=Research(env);failed.run()
                self.assertEqual(failed.state['status'],'ERROR');self.assertIn('report changed',failed.state['error'])
        store.close()

if __name__=='__main__':unittest.main()
