"""Offline behavioral tests. No credentials, account access, orders or network."""
import copy
import importlib.util
import json
import sqlite3
import tempfile
import unittest
import zipfile
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import execution as a


T = a.dt('2020-06-01T08:00:00Z')


def candle(t, o=1.1000, h=1.1005, l=1.0995, c=1.1000, bid=None, ask=None):
    mid = dict(o=o, h=h, l=l, c=c)
    return dict(time=a.stamp(t), complete=True, volume=10,
                mid=mid, bid=bid or dict(mid), ask=ask or dict(mid))


class FakeStore:
    def __init__(self, series):
        self.series = series
        self.calls = []

    def range(self, pair, gran, start, end):
        self.calls.append((pair, gran, start, end))
        return [x for x in self.series.get(gran, []) if start <= a.dt(x['time']) < end]

    def segment_end(self, pair, gran, start, end):
        return end


def record(entry=T, exit=None):
    return dict(pair='EUR_USD', side='BUY', tf='H1', entry=a.stamp(entry),
                exit=a.stamp(exit or entry + timedelta(hours=1)), rr=4.0, sid='EUR_USD_H1_LONG')


def geo():
    return dict(fill=a.dec('1.1000'), stop=a.dec('1.0990'), target=a.dec('1.1040'),
                stop_distance=a.dec('.0010'), worst_stop_distance=a.dec('.0011'))


class PriceTests(unittest.TestCase):
    def test_jpy_binary_stop_artifact_is_exact_touch(self):
        stop = a.broker_price('USD_JPY', 81.53200000000001)
        bar = candle(T, 81.5, 81.532, 81.49, 81.51)
        self.assertEqual(a.touch(bar, 'ask', 'SELL', stop, a.dec(81.3))['kind'], 'STOP')

    def test_fractional_midpoint_below_barrier_is_not_rounded_into_hit(self):
        bar = candle(T, 1.1000, 1.103999, 1.0995, 1.1001)
        self.assertIsNone(a.touch(bar, 'mid', 'BUY', a.dec('1.099'), a.dec('1.104')))

    def test_buy_guard_boundary(self):
        good = a.entry_preview('EUR_USD', 'BUY', 1.1, 1.099, 4, 1.10008, 1.10010)
        bad = a.entry_preview('EUR_USD', 'BUY', 1.1, 1.099, 4, 1.10009, 1.10011)
        self.assertEqual(good['status'], 'ADMITTED_ENTRY_PROXY')
        self.assertEqual(bad['status'], 'REJECTED_10T_ENTRY_PROXY')

    def test_sell_guard_boundary(self):
        good = a.entry_preview('USD_JPY', 'SELL', 150, 150.1, 2.5, 149.990, 150.000)
        bad = a.entry_preview('USD_JPY', 'SELL', 150, 150.1, 2.5, 149.989, 149.999)
        self.assertEqual(good['status'], 'ADMITTED_ENTRY_PROXY')
        self.assertEqual(bad['status'], 'REJECTED_10T_ENTRY_PROXY')

    def test_favorable_fill_not_adverse(self):
        x = a.entry_preview('EUR_USD', 'BUY', 1.1, 1.099, 4, 1.0998, 1.0999)
        self.assertEqual(x['adverse_ticks'], 0)
        self.assertEqual(x['target'], a.dec('1.104'))

    def test_chf_special_target_gate(self):
        x = a.entry_preview('EUR_CHF', 'SELL', .95, .95002, 3.5, .94993, .94994)
        self.assertEqual(x['status'], 'REJECTED_TARGET_ALREADY_PASSED_PROXY')

    def test_budget_r_denominator_not_fill_r_denominator(self):
        x = a.entry_preview('EUR_USD', 'BUY', 1.1, 1.099, 4, 1.10000, 1.10002)
        self.assertEqual(x['stop_distance'], a.dec('.00102'))
        self.assertEqual(x['worst_stop_distance'], a.dec('.00110'))

    def test_negative_spread_rejected(self):
        with self.assertRaises(ValueError):
            a.entry_preview('EUR_USD', 'BUY', 1.1, 1.099, 4, 1.101, 1.100)

    def test_invalid_reference_geometry_rejected(self):
        with self.assertRaises(ValueError):
            a.levels('EUR_USD', 'BUY', 1.1, 1.101, 4)

    def test_bid_and_ask_change_stop(self):
        c = candle(T, 1.1, 1.1008, 1.0995, 1.1,
                   ask=dict(o=1.1001, h=1.1011, l=1.0996, c=1.1001))
        self.assertIsNone(a.touch(c, 'mid', 'SELL', a.dec('1.101'), a.dec('1.096')))
        self.assertEqual(a.touch(c, 'ask', 'SELL', a.dec('1.101'), a.dec('1.096'))['kind'], 'STOP')

    def test_stop_open_gap_can_exceed_one_r(self):
        hit = a.touch(candle(T, 1.098, 1.0985, 1.097, 1.098), 'bid', 'BUY', a.dec('1.099'), a.dec('1.104'))
        self.assertEqual(hit['exit_price'], 1.098)
        self.assertTrue(hit['open_gap'])
        self.assertEqual((a.dec(hit['exit_price']) - a.dec('1.100')) / a.dec('.001'), -2)

    def test_target_open_gap_gets_better_quote(self):
        hit = a.touch(candle(T, 1.105, 1.106, 1.1045, 1.105), 'bid', 'BUY', a.dec('1.099'), a.dec('1.104'))
        self.assertEqual(hit['exit_price'], 1.105)

    def test_rounding_and_preview_parity_against_supplied_executor(self):
        spec = importlib.util.spec_from_file_location('calculation_fixture', Path(__file__).parent / 'fixtures/executor_calculation.py')
        old = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(old)
        data = a.load_reference()
        mapping = {x['archived_sid']: x['live_sid'] for x in data['registry']}
        tested = 0
        for r, g in zip(data['reference']['book'], data['reference']['geometry']):
            unit = a.tick(r['pair'])
            ref, sl = float(a.broker_price(r['pair'], g['reference_entry'])), float(a.broker_price(r['pair'], g['stop']))
            sign = 1 if r['side'] == 'BUY' else -1
            for deviation in (-2, 0, 10, 11):
                p = a.dec(ref) + sign * deviation * unit
                bid, ask = (p - 2 * unit, p) if sign == 1 else (p, p + 2 * unit)
                new = a.entry_preview(r['pair'], r['side'], ref, sl, r['rr'], bid, ask)
                try:
                    prior = old.calculate_execution(r['pair'], r['side'], ref, sl, r['rr'], strategy_id=mapping[r['sid']],
                                                    price_snapshot=(float(bid), float(ask), 1.0), account_snapshot={'NAV': '10000'})
                except ValueError:
                    self.assertTrue(new['status'].startswith('REJECTED_'), (r, deviation, new))
                else:
                    self.assertEqual(new['status'], 'ADMITTED_ENTRY_PROXY')
                    for key in ('stop', 'target', 'price_bound'):
                        self.assertEqual(float(new[key]), prior[key], (r, key))
                    self.assertEqual(float(new['fill']), prior['live_price'])
                tested += 1
        self.assertEqual(tested, 13564)


class PathTests(unittest.TestCase):
    def test_saved_jpy_float_and_eurusd_intrabar_regressions(self):
        cases = json.loads((Path(__file__).parent / 'fixtures/saved_regression_paths.json').read_text())
        for case in cases:
            r, original = case['record'], case['geometry']
            g = a.levels(r['pair'], r['side'], original['reference_entry'], original['stop'], r['rr'])
            g['fill'] = a.broker_price(r['pair'], original['fill'])
            g['stop_distance'] = abs(g['fill'] - g['stop'])
            store = FakeStore({r['tf']: case['native'], 'M1': case['m1']})
            result = a.first_exit(store, r, g, 'mid', a.dt(r['exit']))
            self.assertEqual(result['kind'], 'STOP', case['index'])
            self.assertEqual(result['certainty'], 'RESOLVED_TO_CANDLE_INTERVAL', case['index'])
            self.assertEqual(result['interval_start_utc'], case['expected_first_stop'])

    def test_h1_both_uses_m1_sequence(self):
        parent = candle(T, 1.1, 1.105, 1.098, 1.1)
        minute = candle(T, 1.1, 1.1005, 1.098, 1.0995)
        x = a.refine(FakeStore({'M1': [minute]}), 'EUR_USD', 'BUY', 'bid', a.dec('1.099'), a.dec('1.104'), parent, 'H1')
        self.assertEqual((x['kind'], x['certainty']), ('STOP', 'RESOLVED_TO_CANDLE_INTERVAL'))

    def test_m1_both_uses_s5_sequence(self):
        parent = candle(T, 1.1, 1.105, 1.098, 1.1)
        first = candle(T, 1.1, 1.1045, 1.0995, 1.104)
        x = a.refine(FakeStore({'S5': [first]}), 'EUR_USD', 'BUY', 'bid', a.dec('1.099'), a.dec('1.104'), parent, 'M1')
        self.assertEqual((x['kind'], x['certainty']), ('TARGET', 'RESOLVED_TO_CANDLE_INTERVAL'))

    def test_s5_both_stays_ambiguous(self):
        parent = candle(T, 1.1, 1.105, 1.098, 1.1)
        x = a.refine(FakeStore({}), 'EUR_USD', 'BUY', 'bid', a.dec('1.099'), a.dec('1.104'), parent, 'S5')
        self.assertEqual(x['certainty'], 'AMBIGUOUS_INTRABAR_ORDER')
        self.assertIsNone(x['exit_price'])

    def test_missing_s5_before_hit_qualifies_outcome(self):
        parent = candle(T, 1.1, 1.105, 1.098, 1.1)
        later = candle(T + timedelta(seconds=5), 1.1, 1.1045, 1.0995, 1.104)
        x = a.refine(FakeStore({'S5': [later]}), 'EUR_USD', 'BUY', 'bid', a.dec('1.099'), a.dec('1.104'), parent, 'M1')
        self.assertEqual(x['certainty'], 'TOUCH_WITH_EARLIER_UNEXPLAINED_GAP')

    def test_missing_children_after_first_hit_do_not_erase_it(self):
        parent = candle(T, 1.1, 1.105, 1.098, 1.1)
        first = candle(T, 1.1, 1.1045, 1.0995, 1.104)
        x = a.refine(FakeStore({'S5': [first]}), 'EUR_USD', 'BUY', 'bid', a.dec('1.099'), a.dec('1.104'), parent, 'M1')
        self.assertEqual(x['certainty'], 'RESOLVED_TO_CANDLE_INTERVAL')

    def test_tier_disagreement_is_unresolved(self):
        parent = candle(T, 1.1, 1.1041, 1.0995, 1.1)
        child = candle(T, 1.1, 1.105, 1.098, 1.1)
        # Earliest lower interval crosses outside parent; never call it verified.
        x = a.refine(FakeStore({'M1': [child], 'S5': [candle(T, 1.1, 1.1005, 1.098, 1.0995)]}), 'EUR_USD', 'BUY', 'bid', a.dec('1.099'), a.dec('1.104'), parent, 'H1')
        self.assertEqual(x['certainty'], 'SOURCE_TIER_MISMATCH')

    def test_unconfirmed_finer_touch_is_unresolved(self):
        parent = candle(T, 1.1, 1.105, 1.0995, 1.1)
        x = a.refine(FakeStore({'M1': [candle(T)]}), 'EUR_USD', 'BUY', 'bid', a.dec('1.099'), a.dec('1.104'), parent, 'H1')
        self.assertEqual(x['certainty'], 'UNCONFIRMED_FINER_TOUCH')

    def test_continues_past_archived_exit(self):
        h1 = [candle(T), candle(T + timedelta(hours=1), 1.1, 1.105, 1.0995, 1.104)]
        m1 = [candle(T + timedelta(hours=1), 1.1, 1.105, 1.0995, 1.104)]
        x = a.first_exit(FakeStore({'H1': h1, 'M1': m1}), record(), geo(), 'bid', T + timedelta(hours=3))
        self.assertEqual(x['kind'], 'TARGET')
        self.assertEqual(x['interval_end_utc'], a.stamp(T + timedelta(hours=1, minutes=1)))

    def test_pre_entry_candle_ignored(self):
        old = candle(T - timedelta(hours=1), 1.1, 1.105, 1.098, 1.1)
        x = a.first_exit(FakeStore({'H1': [old, candle(T)]}), record(), geo(), 'bid', T + timedelta(hours=1))
        self.assertEqual(x['kind'], 'OPEN_AT_FIXED_CUTOFF')

    def test_missing_native_before_touch_stays_qualified(self):
        later = candle(T + timedelta(hours=1), 1.1, 1.105, 1.0995, 1.104)
        x = a.first_exit(FakeStore({'H1': [later], 'M1': [later]}), record(), geo(), 'bid', T + timedelta(hours=3))
        self.assertEqual(x['certainty'], 'TOUCH_WITH_EARLIER_UNEXPLAINED_GAP')

    def test_open_not_forced_to_zero_or_loss(self):
        x = a.first_exit(FakeStore({'H1': [candle(T)]}), record(), geo(), 'bid', T + timedelta(hours=1))
        self.assertEqual(x['kind'], 'OPEN_AT_FIXED_CUTOFF')
        self.assertIsNone(x['exit_price'])

    def test_partial_final_native_interval_checked_in_m1(self):
        last = candle(T + timedelta(hours=1), 1.1, 1.105, 1.0995, 1.104)
        x = a.first_exit(FakeStore({'H1': [candle(T)], 'M1': [last]}), record(), geo(), 'bid', T + timedelta(hours=1, minutes=29))
        self.assertEqual(x['kind'], 'TARGET')

    def test_no_future_candle_used(self):
        future = candle(T + timedelta(hours=1), 1.1, 1.105, 1.0995, 1.104)
        x = a.first_exit(FakeStore({'H1': [candle(T), future], 'M1': [future]}), record(), geo(), 'bid', T + timedelta(hours=1))
        self.assertEqual(x['kind'], 'OPEN_AT_FIXED_CUTOFF')

    def test_weekend_assumption_uses_new_york_dst(self):
        self.assertTrue(a.conventional_weekend(a.dt('2020-07-03T21:00:00Z')))
        self.assertFalse(a.conventional_weekend(a.dt('2020-01-03T21:00:00Z')))
        self.assertTrue(a.conventional_weekend(a.dt('2020-01-03T22:00:00Z')))


class FundingAndReportingTests(unittest.TestCase):
    def schedule(self):
        return dict(days=dict(zip(a.DAYS, (1, 1, 3, 1, 1, 0, 0))), long_rate=-.0365, short_rate=.02)

    def test_triple_day_charge(self):
        r = record(a.dt('2020-06-03T20:00:00Z'))
        out = dict(kind='TARGET', certainty='RESOLVED_TO_CANDLE_INTERVAL', interval_start_utc='2020-06-03T22:00:00Z', interval_end_utc='2020-06-03T22:01:00Z')
        x = a.funding_sensitivity(r, geo(), out, self.schedule())
        self.assertEqual(x['funding_days_definite'], 3)
        self.assertAlmostEqual(x['funding_snapshot_r_low'], -.33)
        self.assertFalse(x['historical_funding_known'])

    def test_boundary_exit_keeps_funding_range(self):
        r = record(a.dt('2020-06-03T20:00:00Z'))
        out = dict(kind='TARGET', certainty='RESOLVED_TO_CANDLE_INTERVAL', interval_start_utc='2020-06-03T21:00:00Z', interval_end_utc='2020-06-03T21:01:00Z')
        x = a.funding_sensitivity(r, geo(), out, self.schedule())
        self.assertEqual(x['funding_days_definite'], 0)
        self.assertEqual(x['funding_days_exit_or_entry_order_uncertain'], 3)
        self.assertAlmostEqual(x['funding_snapshot_r_low'], -.33)
        self.assertEqual(x['funding_snapshot_r_high'], 0)

    def test_unresolved_funding_not_reported_as_zero(self):
        x = a.funding_sensitivity(record(), geo(), dict(kind='STOP', certainty='SOURCE_TIER_MISMATCH'), self.schedule())
        self.assertNotIn('funding_snapshot_r_low', x)

    def test_summary_retains_denominators_and_withholds_cagr(self):
        rows = [dict(variant=a.VARIANTS[1], status='CLOSED_RESOLVED', observed_path_price_r=3),
                dict(variant=a.VARIANTS[1], status='UNRESOLVED_PATH', observed_path_price_r=-1),
                dict(variant=a.VARIANTS[1], status='ENTRY_PROXY_REJECTED')]
        x = a.summarize(rows)[1]
        self.assertEqual(x['attempted'], 3)
        self.assertEqual(x['resolved_closed'], 1)
        self.assertEqual(x['resolved_only_net_price_r'], 3)
        self.assertIsNone(x['cagr'])
        self.assertIsNone(x['portfolio_drawdown'])


class SourceAndSafetyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = a.CandleStore(Path(self.tmp.name), 'https://api-fxtrade.oanda.com')

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_offline_missing_request_does_not_call_network(self):
        with patch('urllib.request.OpenerDirector.open', side_effect=AssertionError('No network allowed')):
            with self.assertRaises(a.NeedData) as caught:
                self.store.range('EUR_USD', 'M1', T, T + timedelta(minutes=1))
        self.assertEqual(caught.exception.identity['start'], a.stamp(T))

    def test_empty_download_is_cached_as_empty_not_refetched(self):
        calls = []
        def transport(req):
            calls.append(req)
            return dict(instrument='EUR_USD', granularity='M1', candles=[])
        self.store.reader = a.CandleReader('FAKE', self.store.broker, transport=transport)
        for _ in range(2):
            self.assertEqual(self.store.range('EUR_USD', 'M1', T, T + timedelta(minutes=1)), [])
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].get_method(), 'GET')
        self.assertNotIn('count=', calls[0].full_url)

    def test_mock_online_fetch_extends_then_refines_without_order_endpoint(self):
        from urllib.parse import parse_qs, urlsplit
        seen = []
        def transport(req):
            seen.append(req)
            query = parse_qs(urlsplit(req.full_url).query)
            gran = query['granularity'][0]
            start, end = a.dt(query['from'][0]), a.dt(query['to'][0])
            bars = []
            while start < end:
                if start >= T + timedelta(hours=1):
                    bars.append(candle(start, 1.1, 1.105, 1.0995, 1.104))
                else:
                    bars.append(candle(start))
                start += timedelta(seconds=a.STEP[gran])
            return dict(instrument='EUR_USD', granularity=gran, candles=bars)
        self.store.reader = a.CandleReader('FAKE', self.store.broker, requests_per_second=5, transport=transport)
        outcome = a.first_exit(self.store, record(), geo(), 'bid', T + timedelta(hours=3))
        self.assertEqual((outcome['kind'], outcome['certainty']), ('TARGET', 'RESOLVED_TO_CANDLE_INTERVAL'))
        self.assertEqual(outcome['interval_start_utc'], a.stamp(T + timedelta(hours=1)))
        self.assertEqual(len(seen), 3)
        self.assertTrue(all(req.get_method() == 'GET' and '/v3/instruments/EUR_USD/candles?' in req.full_url for req in seen))

    def test_cache_corruption_is_not_silently_accepted(self):
        def transport(req):
            return dict(instrument='EUR_USD', granularity='M1', candles=[candle(T)])
        self.store.reader = a.CandleReader('FAKE', self.store.broker, transport=transport)
        self.store.range('EUR_USD', 'M1', T, T + timedelta(minutes=1))
        self.store.db.execute("UPDATE blocks SET sha256='wrong'")
        self.store.db.commit()
        self.store.memo.clear()
        with self.assertRaises(ValueError):
            self.store.range('EUR_USD', 'M1', T, T + timedelta(minutes=1))

    def test_resumed_export_keeps_source_evidence_without_read_again(self):
        def transport(req):
            return dict(instrument='EUR_USD', granularity='M1', candles=[candle(T)])
        self.store.reader = a.CandleReader('FAKE', self.store.broker, transport=transport)
        self.store.range('EUR_USD', 'M1', T, T + timedelta(minutes=1))
        self.store.used.clear()
        path = Path(self.tmp.name) / 'export.zip'
        with zipfile.ZipFile(path, 'w') as z:
            self.store.export(z)
        with zipfile.ZipFile(path) as z:
            index = json.loads(z.read('new_source_candle_index.json'))
            self.assertEqual(len(index), 1)
            self.assertEqual(a.sha(z.read(index[0]['file'])), index[0]['sha256'])

    def test_partial_bar_is_not_used(self):
        ident = self.store.identity('EUR_USD', 'M1', T, T + timedelta(minutes=1))
        c = candle(T)
        c['complete'] = False
        self.assertEqual(self.store.validate(dict(instrument='EUR_USD', granularity='M1', candles=[c]), ident), [])

    def test_unordered_source_rejected(self):
        ident = self.store.identity('EUR_USD', 'M1', T, T + timedelta(minutes=2))
        with self.assertRaises(ValueError):
            self.store.validate(dict(instrument='EUR_USD', granularity='M1', candles=[candle(T + timedelta(minutes=1)), candle(T)]), ident)

    def test_missing_ask_cannot_be_filled_with_mid(self):
        ident = self.store.identity('EUR_USD', 'M1', T, T + timedelta(minutes=1))
        c = candle(T)
        del c['ask']
        with self.assertRaises(ValueError):
            self.store.validate(dict(instrument='EUR_USD', granularity='M1', candles=[c]), ident)

    def test_reader_rejects_path_injection(self):
        reader = a.CandleReader('FAKE', self.store.broker, transport=lambda req: self.fail('Must not send'))
        ident = self.store.identity('EUR_USD', 'M1', T, T + timedelta(minutes=1))
        ident['pair'] = '../accounts/1/orders'
        with self.assertRaises(ValueError):
            reader.get(ident)

    def test_reader_rejects_other_host(self):
        with self.assertRaises(ValueError):
            a.CandleReader('FAKE', 'https://example.com')

    def test_http_post_is_rejected(self):
        responses = []
        a.application({'REQUEST_METHOD': 'POST', 'PATH_INFO': '/webhook'}, lambda status, headers: responses.append(status))
        self.assertTrue(responses[0].startswith('405'))

    def test_health_and_head_status_do_not_start_run(self):
        with patch.object(a, 'start_once', side_effect=AssertionError('Must not run')):
            for method, path in [('GET', '/health'), ('HEAD', '/status')]:
                a.application({'REQUEST_METHOD': method, 'PATH_INFO': path}, lambda status, headers: None)

    def test_order_url_does_not_exist(self):
        responses = []
        a.application({'REQUEST_METHOD': 'GET', 'PATH_INFO': '/orders'}, lambda status, headers: responses.append(status))
        self.assertTrue(responses[0].startswith('404'))


if __name__ == '__main__':
    unittest.main(verbosity=2)
