import copy
import gzip
import io
import json
import sqlite3
import tempfile
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from pullback_lab import core as e
from pullback_lab.storage import load_bars,Store
from pullback_lab.signals import generate
from audusd_matrix.signals import evaluate,candidates,generate_grid
from audusd_matrix.io import baseline_parity,engine_check,PINS
from audusd_matrix.reporting import account_screen,regions,augment
from audusd_matrix.storage import frozen_store_class
from audusd_matrix.runner import Research
from audusd_matrix.download import Stream
from audusd_pullback_matrix import application


class TransitionTests(unittest.TestCase):
    def test_failed_filter_retains_touch_and_a_later_candle_can_enter(self):
        state=dict(setup=None,armed=True)
        prev=dict(o=1.12,c=1.08,h=1.13,l=1.07)
        now=dict(o=1.07,c=1.13,h=1.14,l=1.00)
        chosen,counts=evaluate(state,now,prev,True,1.1,1.1,.1,10,dict(body_atr=.6),lambda f:f['body_atr']>=1.)
        self.assertIsNone(chosen);self.assertEqual(state['setup']['index'],10);self.assertEqual(counts['filter_failed_trigger'],1)
        # A non-engulf down candle updates the same setup's low.
        evaluate(state,dict(o=1.13,c=1.08,h=1.14,l=.99),now,True,1.1,1.1,.1,11,{},lambda f:False)
        chosen,_=evaluate(state,dict(o=1.07,c=1.14,h=1.15,l=1.01),dict(o=1.13,c=1.08,h=1.14,l=.99),True,1.1,1.1,.1,12,dict(body_atr=1.1),lambda f:f['body_atr']>=1.)
        self.assertIsNotNone(chosen);self.assertEqual(chosen['setup']['index'],10)
        self.assertEqual(chosen['setup']['extreme'],.99);self.assertEqual(chosen['features']['duration_bars'],3)
        self.assertIsNone(state['setup']);self.assertFalse(state['armed'])

    def test_accepted_signal_consumes_even_before_execution_result_is_known(self):
        state=dict(setup=None,armed=True)
        chosen,_=evaluate(state,dict(o=1.07,c=1.13,h=1.14,l=1.),dict(o=1.12,c=1.08,h=1.13,l=1.07),True,1.1,1.1,.1,10,{},lambda f:True)
        self.assertIsNotNone(chosen);self.assertIsNone(state['setup']);self.assertFalse(state['armed'])
        # The generator has no quote or order outcome argument to reopen it.

    def test_expiry_and_trend_failure_clear_setup(self):
        for trend,index in [(True,18),(False,11)]:
            state=dict(setup=dict(index=10,extreme=1.,area=1.1,start_atr=.1),armed=False)
            selected,_=evaluate(state,dict(o=1.07,c=1.13,h=1.14,l=1.),dict(o=1.12,c=1.08,h=1.13,l=1.07),trend,1.1,1.1,.1,index,{},lambda f:True)
            self.assertIsNone(selected);self.assertIsNone(state['setup'])

    def test_independent_candidate_states_and_fixed_matrix_size(self):
        rules=candidates();self.assertEqual(len(rules),16);self.assertEqual(sum(r['kind']=='MATRIX' for r in rules),9)
        a=dict(setup=None,armed=True);b=copy.deepcopy(a)
        args=(dict(o=1.07,c=1.13,h=1.14,l=1.),dict(o=1.12,c=1.08,h=1.13,l=1.07),True,1.1,1.1,.1,10,{})
        evaluate(a,*args,lambda f:True);evaluate(b,*args,lambda f:False)
        self.assertIsNone(a['setup']);self.assertIsNotNone(b['setup'])


def account_rows(cid):
    result=[]
    for wid in ['FULL_HISTORY','RECENT_2021_TO_CUTOFF']+[f'ERA_{y}_{y+4}' for y in (2005,2010,2015,2020,2025)]:
        for mode in ['FROZEN_14OCT2026_RATES','ADVERSE_6PCT_ANNUAL']:
            result.append(dict(candidate_id=cid,window_id=wid,financing_mode=mode,closed_trades=300,
                conditional_ending_nav_gbp=130.,conditional_nav_cagr_pct=5.,mean_net_r_without_best_five=.05,uncertain_account_lineage=True))
    return result


class PlateauTests(unittest.TestCase):
    def test_four_neighbours_must_pass_and_isolated_peak_fails(self):
        rules=candidates();rows=[a for r in rules for a in account_rows(r['candidate_id'])]
        before=regions(rules,rows);self.assertEqual(len(before),4);self.assertTrue(all(b['conditional_plateau_screen_pass'] for b in before))
        # Fail one corner; its adjacent region must fail even though three pass.
        bad=before[0]['member_candidates'][0]
        for r in rows:
            if r['candidate_id']==bad and r['window_id']=='RECENT_2021_TO_CUTOFF':r['conditional_ending_nav_gbp']=90.
        after=regions(rules,rows);self.assertFalse(after[0]['conditional_plateau_screen_pass'])
        self.assertTrue(after[0]['account_lineage_uncertainty_present']);self.assertFalse(after[0]['automatic_selection'])

    def test_partial_region_cannot_pass_and_sample_failures_are_visible(self):
        rows=account_rows('one');rows[0]['closed_trades']=10
        self.assertIn('FULL_HISTORY_FROZEN_14OCT2026_RATES_SMALL_SAMPLE',account_screen(rows)['screen_reasons'])
        self.assertTrue(all(not r['conditional_plateau_screen_pass'] for r in regions(candidates(),[])))

    def test_cash_reconciliation_and_single_position_invariant(self):
        result=dict(summary=dict(ending_balance_gbp=101.,price_pl_gbp=1.2,financing_gbp=-.2,max_positions=1),
            trades=[dict(net_r_actual_entry_risk=1.)],cash_events=[dict(cash_gbp=1.2),dict(cash_gbp=-.2)])
        self.assertEqual(augment(result,'2020-01-01T00:00:00Z','2021-01-01T00:00:00Z')['ledger_reconciliation'],'PASS')
        result['summary']['ending_balance_gbp']=102.
        with self.assertRaisesRegex(ValueError,'cash ledger'):augment(result,'2020-01-01T00:00:00Z','2021-01-01T00:00:00Z')


class Reader:
    def __init__(self):self.receipts=[];self.changed=False
    def get(self,ident):
        v='1.26' if self.changed else '1.25';prices={k:v for k in 'ohlc'}
        return dict(instrument=ident['pair'],granularity=ident['granularity'],candles=[dict(time=ident['start'],complete=True,mid=prices,bid=prices,ask=prices)])


class CacheAndServiceTests(unittest.TestCase):
    def test_original_window_revision_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);reader=Reader();start=e.dt('2020-01-01T00:00:00Z');end=e.dt('2020-01-02T00:00:00Z')
            ident=Store.identity('AUD_USD','M1',start,end);raw=e.canon(reader.get(ident));row=dict(ident,key=e.sha(e.canon(ident)),sha256=e.sha(raw))
            index=root/'index.gz';index.write_bytes(gzip.compress(e.canon([row])))
            reader.changed=True;cls=frozen_store_class(None,index,'test',raw_mb=64,reserve_mb=0);store=cls(root/'cache',reader=reader)
            with self.assertRaisesRegex(ValueError,'changed on re-fetch'):store.exact('AUD_USD','M1',start,end)
            reader.changed=False
            restored=store.exact('AUD_USD','M1',start,end)
            self.assertEqual(e.sha(e.canon(dict(instrument='AUD_USD',granularity='M1',candles=restored))),row['sha256'])
            store.close()

    def test_private_eviction_refetch_remains_consistent(self):
        from audusd_matrix.cache import cache_class
        with tempfile.TemporaryDirectory() as tmp:
            store=cache_class(Store,200,0)(tmp,reader=Reader())
            for i in range(10):
                start=e.dt('2020-01-01T00:00:00Z')+e.timedelta(days=i)
                store.exact('AUD_USD','M1',start,start+e.timedelta(days=1))
                self.assertLessEqual(store.db.execute('SELECT coalesce(sum(length(body)),0) FROM blocks').fetchone()[0],200)
            self.assertEqual(len(store.index()),10);store.close()

    def test_health_head_and_order_requests_do_not_start(self):
        with tempfile.TemporaryDirectory() as tmp:
            job=Research(dict(AUDUSD_MATRIX_OUTPUT_DIR=tmp));app=application(job);codes=[]
            for path,method in [('/health','GET'),('/status','HEAD'),('/orders','POST')]:list(app({'PATH_INFO':path,'REQUEST_METHOD':method},lambda status,headers:codes.append(status)))
            self.assertEqual(codes,['200 OK','200 OK','405 Method Not Allowed']);self.assertIsNone(job.thread)
            self.assertFalse(job.work.exists())

    def test_engine_and_output_location_protection(self):
        engine_check(Path(__file__).resolve().parents[1])
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError,'separate'):Research(dict(AUDUSD_MATRIX_SOURCE_DIR=tmp,AUDUSD_MATRIX_OUTPUT_DIR=tmp+'/child'))

    def test_streamed_zip_is_valid_and_does_not_write_a_full_copy(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'REVIEW.md').write_text('small review')
            (root/'run_state.json').write_text(json.dumps(dict(status='PARTIAL',published_candidates=[])))
            import threading
            job=SimpleNamespace(work=root,signature='test',lock=threading.RLock())
            data=b''.join(Stream(job,True))
            with zipfile.ZipFile(io.BytesIO(data)) as z:
                self.assertIsNone(z.testzip());manifest=json.loads(z.read('report_manifest.json'))
                for name,m in manifest.items():self.assertEqual(e.sha(z.read(name)),m['sha256'])
            self.assertFalse(list(root.glob('*.zip')))

if __name__=='__main__':unittest.main()
