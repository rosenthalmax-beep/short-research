"""Offline arithmetic, event-order, input-integrity and service regressions."""
import copy
import csv
import io
import json
import tempfile
import unittest
import zipfile
from collections import OrderedDict
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit
import execution as e
import inputs
import app
from accounting import Replay, converted
from broker_reconcile import reconcile
from funding import Funding, MODES, MissingHistoricalRate, rollovers
from market import Market, QuoteUnavailable

ROOT = Path(__file__).parent
T = e.dt('2026-10-12T12:00:00Z')


def signal(key='a', pair='GBP_USD', side='BUY', offset=0, sid=None):
    return dict(signal_key=key, strategy_id=sid or key, pair=pair, side=side, timeframe='M15',
                entry_utc=e.stamp(T + timedelta(minutes=offset)),
                signal_start_utc=e.stamp(T + timedelta(minutes=offset - 15)),
                same_strategy_trigger_priority=0, reference_entry=1.25, stop=1.24, rr=3.5)


def path(s, exit_offset=60, price=1.26, status='CLOSED_RESOLVED', **extra):
    start = T + timedelta(minutes=exit_offset)
    return dict(signal_key=s['signal_key'], model_fill=1.25, model_stop=1.24,
                model_price_bound=1.2501, model_target=1.285,
                status=status, kind='TARGET' if price > 1.25 else 'STOP',
                exit_price=price, interval_start_utc=e.stamp(start),
                interval_end_utc=e.stamp(start + timedelta(minutes=1)),
                certainty='RESOLVED_TO_CANDLE_INTERVAL', **extra)


def schedules():
    days = dict(zip(e.DAYS, (1., 1., 3., 1., 1., 0., 0.)))
    return {p: dict(days=dict(days), long_rate=-.04, short_rate=.02) for p in e.PAIRS}


def instruments():
    return [dict(name=p, marginRate='0.05' if p.startswith('AUD_') else '0.03333333333333',
                 minimumTradeSize='1', maximumOrderUnits='100000000', tradeUnitsPrecision=0) for p in e.PAIRS]


class FixedMarket:
    def __init__(self, prices=None):
        self.prices = prices or {}
        self.current_stale = False; self.current_max_age = 0

    def begin_event(self):
        self.current_stale = False; self.current_max_age = 0

    def quote(self, pair, when):
        result = self.prices.get(pair, 1.25)
        result = result(when) if callable(result) else result
        if isinstance(result, Exception):
            raise result
        if isinstance(result, dict):
            return result
        return dict(bid=result, ask=result, mid=result)

    def fx_mid(self, currency, when):
        if currency == 'GBP':
            return 1.
        if currency == 'USD':
            return 1 / self.quote('GBP_USD', when)['mid']
        return .005 if currency == 'JPY' else .8


def replay(signals, paths, market=None, mode=MODES[0], scenario='END_ASC', funding=None, end=None, **kwargs):
    return Replay(signals, lambda s: paths[s['signal_key']], market or FixedMarket(),
                  funding or Funding(schedules()), instruments(), scenario, mode, .01,
                  start=T, end=end or T + timedelta(days=1), keep_nav=True, **kwargs).run()


class BrokerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.evidence = inputs.broker(ROOT)

    def test_saved_broker_records_reconcile(self):
        summary, rows = reconcile(self.evidence)
        self.assertEqual(summary['status'], 'PASS')
        self.assertEqual(len(rows), 82)
        self.assertEqual(summary['checks']['INITIAL_MARGIN_MID_CONVERSION_pass'], 10)
        self.assertEqual(summary['checks']['FINANCING_BASE_TO_GBP_pass'], 4)
        self.assertAlmostEqual(summary['transaction_slice_net_cash_gbp'], -2.234)
        self.assertAlmostEqual(summary['daily_financing_gbp'], -.2807)
        self.assertAlmostEqual(summary['observed_symmetric_conversion_factor_markup'], .01)

    def test_six_day_charge_not_assumed_three(self):
        summary, _ = reconcile(self.evidence)
        days = [r['inferred_charged_days'] for r in summary['observed_live_financing_day_multipliers']]
        self.assertAlmostEqual(days[0], 6)
        self.assertAlmostEqual(days[2], 3)

    def test_modified_cash_fails(self):
        evidence = copy.deepcopy(self.evidence)
        next(t for t in evidence['transactions'] if t['type'] == 'ORDER_FILL' and float(t.get('pl', 0)))['pl'] = '99'
        self.assertEqual(reconcile(evidence)[0]['status'], 'FAILED')

    def test_missing_conversion_fails(self):
        evidence = copy.deepcopy(self.evidence)
        t = next(t for t in evidence['transactions'] if t['type'] == 'ORDER_FILL' and float(t.get('pl', 0)) > 0)
        del t['homeConversionFactors']['gainQuoteHome']
        with self.assertRaises(ValueError):
            reconcile(evidence)


class FundingTests(unittest.TestCase):
    def test_credit_debit_and_home_identity(self):
        self.assertEqual(converted(10, .8, 'USD', .01), (7.92, -.08000000000000007))
        self.assertAlmostEqual(converted(-10, .8, 'USD', .01)[0], -8.08)
        self.assertEqual(converted(-10, 1, 'GBP', .01), (-10, 0))

    def test_ny_rollovers_preserve_dst(self):
        ts = list(rollovers('2026-10-31T00:00:00Z', '2026-11-02T23:00:00Z'))
        self.assertEqual([t.hour for t in ts], [21, 22, 22])

    def test_cad_snapshot_thursday_multiplier(self):
        f = Funding(inputs.broker(ROOT)['funding_schedules'])
        self.assertEqual(f.terms(MODES[1], 'USD_CAD', 'SELL', e.dt('2026-10-15T21:00:00Z'))['charged_days'], 3)

    def test_historical_missing_not_zero(self):
        with self.assertRaises(MissingHistoricalRate):
            Funding(schedules()).terms('HISTORICAL_FILE', 'GBP_USD', 'BUY', T)

    def test_historical_rows_have_separate_day_multiplier(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'rates.csv'
            p.write_text('ny_date,pair,long_rate,short_rate,charged_days,source,basis,rate_basis\n2026-10-12,GBP_USD,-0.04,0.02,6,broker archived rate,FINALIZED_DAILY_RATE,ANNUAL_ONE_DAY_BASE_NOTIONAL\n')
            f = Funding(schedules(), p)
            row = f.terms('HISTORICAL_FILE', 'GBP_USD', 'BUY', T)
            self.assertEqual(row['charged_days'], 6)
            self.assertEqual(row['annual_rate'], -.04)
            self.assertFalse(row['historical_rate_independently_verified'])

    def test_duplicate_historical_rows_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'rates.csv'
            line = '2026-10-12,GBP_USD,-0.04,0.02,6,broker,FINALIZED_DAILY_RATE,ANNUAL_ONE_DAY_BASE_NOTIONAL\n'
            p.write_text('ny_date,pair,long_rate,short_rate,charged_days,source,basis,rate_basis\n' + line * 2)
            with self.assertRaises(ValueError):
                Funding(schedules(), p)

    def test_no_implicit_percent_units(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'rates.csv'
            p.write_text('ny_date,pair,long_rate,short_rate,charged_days,source,basis,rate_basis\n2026-10-12,GBP_USD,-4,2,3,broker,FINALIZED_DAILY_RATE,ANNUAL_ONE_DAY_BASE_NOTIONAL\n')
            with self.assertRaises(ValueError):
                Funding(schedules(), p)


class MarketTests(unittest.TestCase):
    def make_market(self, rows):
        class Store:
            downloads = 0
            memo = OrderedDict()
            def range(self, pair, gran, start, end):
                return [r for r in rows if start <= e.dt(r['time']) < end]
        return Market(Store())

    def candle(self, when, op=1.25, close=9.):
        return dict(time=e.stamp(when), **{c: dict(o=str(op), c=str(close)) for c in ('bid', 'ask', 'mid')})

    def test_no_future_close_high_low_at_entry(self):
        m = self.make_market([self.candle(T, 1.25, 9)])
        self.assertEqual(m.quote('GBP_USD', T)['mid'], 1.25)
        self.assertEqual(m.quote('GBP_USD', T + timedelta(minutes=14))['mid'], 1.25)

    def test_completed_close_eligible_only_after_close(self):
        m = self.make_market([self.candle(T, 1.25, 1.3)])
        self.assertEqual(m.quote('GBP_USD', T + timedelta(minutes=15))['mid'], 1.3)

    def test_new_open_precedes_previous_close_tie(self):
        m = self.make_market([self.candle(T, 1.25, 1.3), self.candle(T + timedelta(minutes=15), 1.4, 9)])
        self.assertEqual(m.quote('GBP_USD', T + timedelta(minutes=15))['mid'], 1.4)

    def test_absent_quote_never_looks_forward(self):
        m = self.make_market([self.candle(T + timedelta(minutes=15), 1.4, 9)])
        with self.assertRaises(QuoteUnavailable):
            m.quote('GBP_USD', T)

    def test_stale_quote_flagged(self):
        m = self.make_market([self.candle(T, 1.25, 1.3)])
        m.quote('GBP_USD', T + timedelta(hours=2))
        self.assertTrue(m.current_stale)
        self.assertTrue(m.quote_gap_seen)

    def test_cross_currency_route(self):
        m = self.make_market([])
        with patch.object(m, 'quote', side_effect=lambda pair, when: {'mid': 200. if pair == 'USD_JPY' else 1.25}):
            self.assertAlmostEqual(m.fx_mid('JPY', T), .004)
            self.assertEqual(m.fx_mid('GBP', T), 1)

    def test_sparse_seed_holes_use_one_canonical_request(self):
        with tempfile.TemporaryDirectory() as d:
            calls = []
            def transport(request):
                q = parse_qs(urlsplit(request.full_url).query)
                start, end = e.dt(q['from'][0]), e.dt(q['to'][0]); when = start; rows = []
                while when < end:
                    v = {k: '1.25' for k in 'ohlc'}
                    rows.append(dict(time=e.stamp(when), complete=True, mid=dict(v), bid=dict(v), ask=dict(v)))
                    when += timedelta(minutes=15)
                calls.append((start, end))
                return dict(instrument='GBP_USD', granularity='M15', candles=rows)
            reader = e.CandleReader('synthetic', 'https://api-fxtrade.oanda.com', 5, transport)
            store = e.CandleStore(d, 'https://api-fxtrade.oanda.com', reader)
            try:
                # Seed a single middle interval, leaving two uncovered regions.
                store._fetch('GBP_USD', 'M15', T, T + timedelta(minutes=15))
                calls.clear()
                m = Market(store); m.quote('GBP_USD', T)
                self.assertEqual(len(calls), 1)
                self.assertEqual(calls[0][1] - calls[0][0], timedelta(days=14))
            finally:
                store.close()


class AccountTests(unittest.TestCase):
    def test_floor_size_cash_and_conversion_once(self):
        s = signal(); p = path(s)
        r = replay([s], {'a': p})
        trade = r['trades'][0]
        expected_units = int(1 / ((1.2501 - 1.24) * .8 * 1.01))
        self.assertEqual(trade['units'], expected_units)
        expected = round(expected_units * .01 * .8 * .99, 4)
        self.assertAlmostEqual(trade['price_pl_gbp'], expected)
        self.assertAlmostEqual(r['summary']['ending_balance_gbp'], 100 + expected)
        self.assertLessEqual(trade['worst_bound_sizing_risk_gbp'], 1)

    def test_nav_updates_before_next_overlapping_entry(self):
        a = signal('a'); b = signal('b', offset=30)
        market = FixedMarket({'GBP_USD': lambda t: 1.25 if t < T + timedelta(minutes=15) else 1.20})
        r = replay([a, b], {'a': path(a, 120), 'b': path(b, 120)}, market=market)
        accepted = [r for r in r['ledger'] if r['decision'] == 'ADMITTED']
        self.assertEqual(len(accepted), 2)
        self.assertLess(accepted[1]['risk_budget_gbp'], accepted[0]['risk_budget_gbp'])

    def test_eurjpy_override(self):
        s = signal(sid='EUR_JPY_M15_SHORT', pair='EUR_JPY')
        p = path(s)
        p.update(model_fill=160., model_stop=159., model_price_bound=160.01, exit_price=161.)
        r = replay([s], {'a': p}, market=FixedMarket({'EUR_JPY': 160.}))
        self.assertAlmostEqual(r['ledger'][0]['risk_budget_gbp'], .75)

    def test_opposite_and_same_strategy_gates(self):
        a = signal('a', sid='same'); b = signal('b', offset=15, sid='same'); c = signal('c', side='SELL', offset=30)
        r = replay([a, b, c], {s['signal_key']: path(s, 120) for s in (a, b, c)})
        self.assertEqual([v['decision'] for v in r['ledger']], ['ADMITTED', 'BLOCKED', 'BLOCKED'])
        self.assertEqual(r['ledger'][2]['reason'], 'OPPOSITE_PAIR_NONHEDGING')

    def test_exit_before_same_time_entry(self):
        a = signal('a', sid='same'); b = signal('b', offset=60, sid='same')
        r = replay([a, b], {'a': path(a, 60), 'b': path(b, 120)}, scenario='START_ASC')
        self.assertEqual([v['decision'] for v in r['ledger']], ['ADMITTED', 'ADMITTED'])

    def test_end_start_interval_scenarios_remain_different(self):
        a = signal('a', sid='same'); b = signal('b', offset=60, sid='same')
        paths = {'a': path(a, 60), 'b': path(b, 120)}
        self.assertEqual(replay([a, b], paths, scenario='END_ASC')['ledger'][1]['decision'], 'BLOCKED')
        self.assertEqual(replay([a, b], paths, scenario='START_ASC')['ledger'][1]['decision'], 'ADMITTED')

    def test_immediate_exit_follows_own_entry(self):
        s = signal(); r = replay([s], {'a': path(s, 0)}, scenario='START_ASC')
        self.assertEqual([v['event'] for v in r['cash_events']], ['ENTRY', 'EXIT'])
        self.assertEqual(len(r['trades']), 1)

    def test_minimum_unit_size_rejection(self):
        s = signal(); r = replay([s], {'a': path(s)}, initial=.001)
        self.assertEqual(r['ledger'][0]['decision'], 'REJECTED_BELOW_MINIMUM_UNITS')

    def test_margin_rejection(self):
        s = signal(); p = path(s); p.update(model_stop=1.249999, model_price_bound=1.250001)
        r = replay([s], {'a': p})
        self.assertEqual(r['ledger'][0]['decision'], 'REJECTED_INSUFFICIENT_MARGIN')

    def test_sampled_closeout_flattens_all(self):
        s = signal()
        market = FixedMarket({'GBP_USD': lambda t: 1.25 if t < T + timedelta(minutes=15) else .01})
        r = replay([s], {'a': path(s, 120)}, market=market)
        self.assertEqual(r['summary']['modeled_margin_liquidations'], 1)
        self.assertEqual(r['trades'][0]['exit_reason'], 'SAMPLED_MARGIN_CLOSEOUT')
        self.assertEqual(r['summary']['open_positions'], 0)

    def test_margin_rejected_signal_does_not_block_later_strategy_entry(self):
        a = signal('a', sid='same'); b = signal('b', offset=30, sid='same')
        pa = path(a); pa.update(model_stop=1.249999, model_price_bound=1.250001)
        r = replay([a, b], {'a': pa, 'b': path(b, 120)})
        self.assertEqual([v['decision'] for v in r['ledger']], ['REJECTED_INSUFFICIENT_MARGIN', 'ADMITTED'])

    def test_close_at_rollover_is_before_financing_in_declared_order(self):
        s = signal(); p = path(s, 9 * 60)
        r = replay([s], {'a': p}, scenario='START_ASC', mode=MODES[3])
        self.assertEqual(r['summary']['financing_gbp'], 0)

    def test_open_trade_financing_in_cash_but_not_closed_wins(self):
        s = signal(); p = path(s)
        p.update(kind='OPEN_AT_FIXED_CUTOFF', status='OPEN_AT_CUTOFF', exit_price=None)
        r = replay([s], {'a': p}, mode=MODES[3])
        self.assertEqual(r['summary']['closed_trades'], 0)
        self.assertEqual(r['summary']['open_positions'], 1)
        self.assertLess(r['summary']['financing_gbp'], 0)
        self.assertAlmostEqual(r['summary']['ending_balance_gbp'], 100 + r['summary']['financing_gbp'])

    def test_open_path_with_prior_gap_stays_uncertain(self):
        s = signal(); p = path(s)
        p.update(kind='OPEN_AT_FIXED_CUTOFF', status='UNRESOLVED_PATH', exit_price=None)
        r = replay([s], {'a': p})
        self.assertTrue(r['summary']['uncertain_account_lineage'])

    def test_mid_and_liquidation_equity_separate(self):
        s = signal(); m = FixedMarket({'GBP_USD': dict(mid=1.25, bid=1.24, ask=1.26)})
        r = replay([s], {'a': path(s)}, market=m)
        row = next(v for v in r['nav_samples'] if v['event'] == 'AFTER_ENTRY')
        self.assertLess(row['liquidation_equity_gbp'], row['nav_mid_gbp'])

    def test_native_vs_m15_quote_disagreement_reported(self):
        s = signal(); p = path(s); p.update(model_bid=1.24, model_ask=1.25)
        r = replay([s], {'a': p})
        self.assertIn('NATIVE_ENTRY_VS_M15_VALUATION_QUOTE_DISAGREEMENT', r['summary']['uncertainty_causes'])

    def test_financing_posts_before_later_sizing_and_counted_once(self):
        a = signal('a'); b = signal('b', offset=10 * 60)
        paths = {'a': path(a, 24 * 60, 1.25), 'b': path(b, 24 * 60, 1.25)}
        r = replay([a, b], paths, mode=MODES[3], end=T + timedelta(days=2))
        self.assertLess(r['ledger'][1]['risk_budget_gbp'], r['ledger'][0]['risk_budget_gbp'])
        self.assertAlmostEqual(r['summary']['ending_balance_gbp'] - 100, sum(v['financing_gbp'] for v in r['trades']), places=4)
        first = r['financing'][0]
        self.assertAlmostEqual(first['base_financing'], first['units'] * -.06 / 365)

    def test_historical_missing_stops_without_zero_imputation(self):
        s = signal(); r = replay([s], {'a': path(s, 24 * 60)}, mode='HISTORICAL_FILE', end=T + timedelta(days=2))
        self.assertEqual(r['summary']['status'], 'STOPPED_MISSING_HISTORICAL_RATE')
        self.assertEqual(r['summary']['financing_gbp'], 0)
        self.assertIsNone(r['summary']['conditional_nav_cagr_pct'])

    def test_unknown_entry_contaminates_account_wide_lineage(self):
        a = signal('a'); b = signal('b', pair='EUR_USD', offset=30)
        r = replay([a, b], {'a': dict(status='UNRESOLVED_ENTRY'), 'b': path(b, 120)})
        self.assertEqual(r['ledger'][0]['decision'], 'UNRESOLVED_ENTRY_OMITTED')
        self.assertTrue(r['ledger'][1]['uncertain_account_lineage'])

    def test_gap_qualified_path_is_not_promoted(self):
        s = signal(); r = replay([s], {'a': path(s, status='UNRESOLVED_PATH')})
        self.assertEqual(r['trades'][0]['path_status'], 'UNRESOLVED_PATH')
        self.assertTrue(r['summary']['uncertain_account_lineage'])

    def test_missing_quote_suppresses_whole_period_cagr(self):
        s = signal(); r = replay([s], {'a': path(s)}, market=FixedMarket({'GBP_USD': QuoteUnavailable('missing')}))
        self.assertEqual(r['summary']['status'], 'STOPPED_MISSING_QUOTE_OR_UNPRICED_EXIT')
        self.assertIsNone(r['summary']['conditional_nav_cagr_pct'])

    def test_streaming_nav_sink_and_sampled_drawdown(self):
        s = signal(); rows = []
        r = replay([s], {'a': path(s)}, nav_sink=rows.append)
        self.assertGreater(len(rows), 3)
        self.assertEqual(r['nav_samples'], [])
        self.assertIn('sampled_liquidation_equity_drawdown_pct', r['summary'])


class InputsAndServiceTests(unittest.TestCase):
    def test_warmed_input_manifest_and_counts(self):
        signals, paths, summary = inputs.baseline(ROOT)
        self.assertEqual(len(signals), 3629)
        self.assertEqual(len(paths), 3506)
        self.assertEqual(summary['archived_control_counts'], {'SIGNAL_AND_ROUNDED_ENTRY_STOP_RR_MATCH': 3391})

    def test_tampered_input_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'saved_broker_evidence.json'; p.write_text('{}')
            with self.assertRaises(ValueError):
                inputs.checked(p)

    def test_all_89_bundled_source_additions_import_and_resume(self):
        with tempfile.TemporaryDirectory() as d:
            store = e.CandleStore(d, 'https://api-fxtrade.oanda.com')
            try:
                inputs.import_additions(ROOT, store)
                self.assertEqual(store.db.execute('SELECT COUNT(*) FROM blocks').fetchone()[0], 89)
                inputs.import_additions(ROOT, store)
                self.assertEqual(store.db.execute('SELECT COUNT(*) FROM blocks').fetchone()[0], 89)
                self.assertEqual(store.downloads, 0)
            finally:
                store.close()

    def call(self, method, path):
        headers = []
        content = b''.join(app.application(dict(REQUEST_METHOD=method, PATH_INFO=path), lambda status, h: headers.append((status, h))))
        return headers[0][0], content

    def test_head_status_does_not_start(self):
        with patch.object(app, 'start') as start:
            code, body = self.call('HEAD', '/status')
            self.assertEqual(code, '200 OK'); self.assertEqual(body, b''); start.assert_not_called()

    def test_post_rejected(self):
        self.assertEqual(self.call('POST', '/status')[0], '405 Method Not Allowed')

    def test_health_never_starts_a_job(self):
        with patch.object(app, 'start') as start:
            code, body = self.call('GET', '/health')
            self.assertEqual(code, '200 OK'); self.assertFalse(json.loads(body)['orders_supported']); start.assert_not_called()

    def test_persistence_marker_stable(self):
        with tempfile.TemporaryDirectory() as d:
            first = app.persistence(d, True); second = app.persistence(d, True)
            self.assertEqual(first['persistence_marker'], second['persistence_marker'])

    def test_complete_service_report_and_resume_offline_transport(self):
        s = signal(); p = path(s)
        summary = dict(evaluation_start=e.stamp(T), historical_cutoff=e.stamp(T + timedelta(days=1)))
        calls = []
        def transport(request):
            parsed = urlsplit(request.full_url); q = parse_qs(parsed.query)
            gran = q['granularity'][0]; start, end = e.dt(q['from'][0]), e.dt(q['to'][0])
            pair = parsed.path.split('/')[-2]; rows = []; when = start
            while when < end:
                component = {k: '1.25' for k in 'ohlc'}
                rows.append(dict(time=e.stamp(when), complete=True, volume=1,
                                 mid=dict(component), bid=dict(component), ask=dict(component)))
                when += timedelta(seconds=e.STEP[gran])
            calls.append(request.full_url)
            return dict(instrument=pair, granularity=gran, candles=rows)
        reader = e.CandleReader('synthetic-not-a-secret', 'https://api-fxtrade.oanda.com', 5, transport)
        with tempfile.TemporaryDirectory() as d, patch.object(inputs, 'baseline', return_value=([s], {'a': p}, summary)), \
             patch.object(e.CandleStore, 'import_seeds'), patch.object(inputs, 'import_additions'), \
             patch.object(e, 'CandleReader', return_value=reader), patch.dict('os.environ', {'OANDA_TOKEN': 'synthetic', 'OANDA_ENV': 'live'}):
            first = app.run(d, scenario_filter='END_ASC', mode_filter=MODES[0])
            self.assertEqual(first['scenario_status_counts'], {'COMPLETE_CONDITIONAL': 1})
            count = len(calls)
            with zipfile.ZipFile(Path(d) / app.RESULT) as z:
                manifest = json.loads(z.read('report_manifest.json'))
                for name, meta in manifest.items():
                    self.assertEqual(e.sha(z.read(name)), meta['sha256'])
            with zipfile.ZipFile(Path(d) / app.EQUITY) as z:
                self.assertEqual(len(z.namelist()), 1)
                self.assertGreater(len(z.read(z.namelist()[0]).splitlines()), 3)
            app.run(d, scenario_filter='END_ASC', mode_filter=MODES[0])
            self.assertEqual(len(calls), count)


if __name__ == '__main__':
    unittest.main(verbosity=2)
