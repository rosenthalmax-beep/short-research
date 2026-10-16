"""Regressions for prehistory cache reuse and uncertainty-preserving evidence."""
import copy,json,pathlib,tempfile,unittest,zlib
from unittest.mock import patch
import execution as e,history as h,signals,gap_audit as g
from self_test import bars

T=e.dt('2012-06-04T08:00:00Z')
def candle(t,low=1.1000,high=1.1002):
    p=dict(o=1.1001,h=high,l=low,c=1.1001)
    return dict(time=e.stamp(t),complete=True,mid=p,bid=p,ask=p)
def price_path():
    return dict(signal_key='case',pair='EUR_USD',strategy_id='EUR_USD_H1_LONG',side='BUY',timeframe='H1',
                entry_utc=e.stamp(T),native_interval_start_utc=e.stamp(T+e.timedelta(hours=1)),
                native_interval_end_utc=e.stamp(T+e.timedelta(hours=2)),interval_start_utc=e.stamp(T+e.timedelta(hours=1,minutes=3)),
                resolution='M1',kind='TARGET',status='UNRESOLVED_PATH',model_stop=1.099,model_target=1.104,
                prior_native_unexplained_slots=0,earlier_gap=True)

class WarmupTests(unittest.TestCase):
    def request_failure_case(self,error,prefix_only):
        start=e.dt('2005-01-01T00:00:00Z');end=start+e.timedelta(days=1)
        class Reader:
            def get(self,ident):
                if not prefix_only or e.dt(ident['start'])<start:raise e.CandleFetchError(error)
                return dict(instrument='EUR_USD',granularity='H1',candles=[])
        with tempfile.TemporaryDirectory() as tmp,patch.object(h,'WARMUP_START',start-e.timedelta(days=1)),patch.object(h,'END',end):
            store=h.HistoryStore(tmp,Reader())
            try:
                result=store.series('EUR_USD','H1')
                return store.coverage,store.db.execute('SELECT count(*) FROM history').fetchone()[0]
            finally:store.close()
    def test_unavailable_pre2005_range_recorded_without_fabricated_cache(self):
        coverage,cached=self.request_failure_case('MID candles: HTTP 400',True)
        self.assertEqual(cached,1);self.assertEqual(len(coverage[0]['warmup_unavailable_blocks']),1);self.assertFalse(coverage[0]['warmup_sufficient'])
    def test_in_period_bad_request_is_not_hidden(self):
        with self.assertRaises(e.CandleFetchError):self.request_failure_case('MID candles: HTTP 400',False)
    def test_authentication_failure_is_not_hidden_as_missing_warmup(self):
        with self.assertRaises(e.CandleFetchError):self.request_failure_case('MID candles: HTTP 401',True)
    def test_other_source_errors_are_not_hidden_as_missing_warmup(self):
        with self.assertRaises(e.CandleFetchError):self.request_failure_case('MID candles: HTTP 500',True)
    def test_existing_2005_cache_key_is_reused_after_prefix(self):
        start=e.dt('2005-01-01T00:00:00Z');end=start+e.timedelta(days=2);prefix=start-e.timedelta(days=2)
        class Reader:
            def __init__(self):self.calls=[]
            def get(self,ident):self.calls.append(ident);return dict(instrument='EUR_USD',granularity='H1',candles=[])
        with tempfile.TemporaryDirectory() as tmp,patch.object(h,'WARMUP_START',prefix),patch.object(h,'END',end):
            reader=Reader();store=h.HistoryStore(tmp,reader)
            ident=dict(schema='MID_HISTORY_NY17_V1',broker='https://api-fxtrade.oanda.com',pair='EUR_USD',granularity='H1',start=e.stamp(start),end=e.stamp(end),price='M',smooth=False,dailyAlignment=17,alignmentTimezone='America/New_York')
            body=e.canon(dict(instrument='EUR_USD',granularity='H1',candles=[]))
            store.db.execute('INSERT INTO history VALUES(?,?,?,?)',(e.sha(e.canon(ident)),e.canon(ident).decode(),zlib.compress(body),e.sha(body)));store.db.commit()
            try:store.series('EUR_USD','H1');self.assertEqual(store.hits,1);self.assertEqual(len(reader.calls),1);self.assertEqual(reader.calls[0]['end'],e.stamp(start))
            finally:store.close()
    def test_warmup_bars_never_evaluated_as_trading_signals(self):
        data=bars(200,start=e.dt('2004-12-30T00:00:00Z'),seconds=3600);daily=bars(300,start=e.dt('2003-01-01T22:00:00Z'),seconds=86400)
        evaluated=[]
        def evaluate(pair,series,atr,index,state):evaluated.append(series[index]['time']);return None
        with patch.object(signals.rules,'evaluate_signal_at_index',side_effect=evaluate),patch.object(signals.rules,'evaluate_short_signal_at_index',side_effect=evaluate):
            self.assertEqual(signals.recover_pair('EUR_USD',data,daily),[])
        self.assertTrue(evaluated);self.assertTrue(all(t>=h.START for t in evaluated));self.assertTrue(any(c['time']<h.START for c in data))
    def test_baseline_and_preexisting_uncertainty_counts_pinned(self):
        b=g.load_baseline(pathlib.Path(g.__file__).parent/'prior_signal_admission_results.zip')
        unresolved=[p for p in b['paths'] if p['status']=='UNRESOLVED_PATH']
        self.assertEqual(len(unresolved),171);self.assertEqual(sum(p['prior_native_unexplained_slots']>0 for p in unresolved),17)
        self.assertEqual(len(b['raw']),3600)
    def test_corrupted_prior_archive_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=pathlib.Path(tmp)/'bad.zip';p.write_bytes(b'bad')
            with self.assertRaisesRegex(ValueError,'checksum'):g.load_baseline(p)
    def test_warmup_changes_are_reported_without_forcing_old_signals(self):
        before=dict(signal_key='s',strategy_id='s',entry_utc=e.stamp(T),reference_entry=1.1,stop=1.,target=1.4,rr=3.)
        after=dict(before,stop=.99)
        summary,rows,_=g.compare(dict(raw=[before]),[after],[])
        self.assertEqual(rows[0]['status'],'GEOMETRY_CHANGED');self.assertFalse(summary['full_portfolio_validated'])

class GapTests(unittest.TestCase):
    def window(self,gran='M1'):return dict(tier='M1_BEFORE_REFINED_TOUCH',granularity=gran,start=e.stamp(T),end=e.stamp(T+e.timedelta(minutes=1)),missing_slots=1)
    def test_weekends_are_assumed_not_verified_and_other_gaps_retained(self):
        friday=e.dt('2026-10-16T20:00:00Z');saturday=e.dt('2026-10-17T12:00:00Z')
        self.assertEqual(len(g.missing_windows([],friday,friday+e.timedelta(minutes=1),60)),1)
        self.assertEqual(g.missing_windows([],saturday,saturday+e.timedelta(minutes=1),60),[])
    def test_empty_two_pair_responses_never_prove_a_closure(self):
        class Empty:
            def range(self,*args):return []
        row=g.investigate_window(Empty(),price_path(),self.window())
        self.assertEqual(row['assessment'],'COINCIDENT_PAIR_ABSENCE_UNPROVEN_SCHEDULE');self.assertFalse(row['historical_closure_verified']);self.assertFalse(row['original_path_automatically_promoted'])
    def test_control_quotes_show_pair_specific_absence(self):
        class Peer:
            def range(self,pair,*args):return [candle(T)] if pair=='USD_JPY' else []
        self.assertEqual(g.investigate_window(Peer(),price_path(),self.window())['assessment'],'PAIR_SPECIFIC_ABSENCE')
    def test_recovered_earlier_barrier_is_flagged_for_replay_not_promoted(self):
        class Source:
            def range(self,pair,gran,*args):return [candle(T,low=1.098)] if gran=='S5' else []
        row=g.investigate_window(Source(),price_path(),self.window())
        self.assertEqual(row['assessment'],'RECOVERED_BARRIER_CANDIDATE_REQUIRES_REPLAY');self.assertEqual(row['first_recovered_touch_kind'],'STOP');self.assertFalse(row['original_path_automatically_promoted'])
    def test_recovered_coarse_barrier_not_hidden_by_missing_finer_quotes(self):
        class Source:
            def range(self,pair,gran,*args):return [candle(T,low=1.098)] if gran=='M1' else []
        row=g.investigate_window(Source(),price_path(),self.window())
        self.assertEqual(row['assessment'],'RECOVERED_BARRIER_CANDIDATE_REQUIRES_REPLAY');self.assertEqual(row['recovered_touch_granularity'],'M1');self.assertFalse(row['original_path_automatically_promoted'])
    def test_complete_finer_quotes_without_touch_are_separate_from_empty(self):
        class Source:
            def range(self,pair,gran,*args):return [candle(T+e.timedelta(seconds=i)) for i in range(0,60,5)] if gran=='S5' else []
        row=g.investigate_window(Source(),price_path(),self.window())
        self.assertEqual(row['assessment'],'FINER_QUOTES_COVER_GAP_WITHOUT_BARRIER_TOUCH');self.assertEqual(row['finer_missing_nonweekend_slots'],0)
    def test_missing_m1_before_exit_found_even_without_native_gap(self):
        p=price_path();parent=e.dt(p['native_interval_start_utc'])
        class Source:
            def range(self,pair,gran,start,end):return [candle(T)] if gran=='H1' else [candle(parent),candle(parent+e.timedelta(minutes=2))]
        found=g.locate_windows(Source(),p)
        self.assertEqual(len(found),1);self.assertEqual(found[0]['tier'],'M1_BEFORE_REFINED_TOUCH');self.assertEqual(found[0]['missing_slots'],1)
    def test_missing_s5_before_exit_found(self):
        p=price_path();parent=e.dt(p['native_interval_start_utc']);p.update(resolution='S5',interval_start_utc=e.stamp(parent+e.timedelta(seconds=15)))
        class Source:
            def range(self,pair,gran,start,end):return [candle(T)] if gran=='H1' else [candle(parent),candle(parent+e.timedelta(seconds=10))]
        found=g.locate_windows(Source(),p)
        self.assertEqual(len(found),1);self.assertEqual(found[0]['tier'],'S5_BEFORE_REFINED_TOUCH');self.assertEqual(found[0]['missing_slots'],1)
    def test_gap_checkpoints_resume_without_new_requests(self):
        class Reader:
            def __init__(self):self.calls=0
            def get(self,ident):self.calls+=1;return dict(instrument=ident['pair'],granularity=ident['granularity'],candles=[])
        class Original:broker='https://api-fxtrade.oanda.com'
        reader=Reader();p=price_path()
        with tempfile.TemporaryDirectory() as tmp,patch.object(g,'locate_windows',return_value=[self.window()]):
            first,rows,windows=g.run(tmp,dict(paths=[p]),Original(),reader);calls=reader.calls
            second,_,_=g.run(tmp,dict(paths=[p]),Original(),reader)
            self.assertGreater(calls,0);self.assertEqual(calls,reader.calls);self.assertEqual(second['checkpoint_hits'],1);self.assertEqual(first['automatic_status_promotions'],0)

if __name__=='__main__':unittest.main()
