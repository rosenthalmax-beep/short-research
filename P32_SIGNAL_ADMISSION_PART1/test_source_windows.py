"""Offline reproduction of the reported overlap failure and isolated-source fix."""
import json
import tempfile
import unittest
from pathlib import Path
from datetime import timedelta
from unittest.mock import patch
import execution as e
from market import Market
from valuation_sources import ValuationWindows

T = e.dt('2005-01-04T07:45:00Z')
START = e.datetime.fromtimestamp(int(T.timestamp()) // (14 * 86400) * (14 * 86400), e.UTC)
END = START + timedelta(days=14)


def candle(price):
    return dict(time=e.stamp(T), complete=True, volume=1,
                **{c: {v: str(price) for v in 'ohlc'} for c in ('mid', 'bid', 'ask')})


def put(store, begin, end, price, origin='seed_01.zip'):
    identity = store.identity('GBP_USD', 'M15', begin, end)
    key = e.sha(e.canon(identity))
    body = e.canon(dict(instrument='GBP_USD', granularity='M15', candles=[candle(price)]))
    store.validate(json.loads(body), identity)
    store.db.execute('INSERT INTO blocks VALUES (?,?,?,?,?)',
                     (key, e.canon(identity).decode(), e.zlib.compress(body), e.sha(body), origin))
    store.db.commit(); store.index[('GBP_USD', 'M15')].append((begin, end, key))
    return key


class WindowTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.root = Path(self.temp.name)
        self.frozen = e.CandleStore(self.root / 'frozen', 'https://api-fxtrade.oanda.com')
        self.legacy = e.CandleStore(self.root / 'legacy', 'https://api-fxtrade.oanda.com')
        self.valuation = e.CandleStore(self.root / 'valuation', 'https://api-fxtrade.oanda.com')

    def tearDown(self):
        self.frozen.close(); self.legacy.close(); self.valuation.close(); self.temp.cleanup()

    def test_reproduce_original_merge_failure_and_fix_without_overwrite(self):
        frozen_key = put(self.frozen, T, T + timedelta(minutes=15), '1.25')
        put(self.legacy, T, T + timedelta(minutes=15), '1.25')
        put(self.legacy, START, END, '1.26', 'NEW_DOWNLOAD')
        before = self.frozen.db.execute('SELECT body,sha256 FROM blocks WHERE key=?', (frozen_key,)).fetchone()
        with self.assertRaisesRegex(ValueError, 'Overlapping source candles disagree'):
            self.legacy.range('GBP_USD', 'M15', T, T + timedelta(minutes=15))
        windows = ValuationWindows(self.valuation, self.frozen, self.legacy)
        market = Market(windows)
        self.assertEqual(market.quote('GBP_USD', T)['mid'], 1.26)
        self.assertEqual(windows.reused_legacy_windows, 1)
        self.assertEqual(windows.downloads, 0)
        self.assertEqual(windows.difference_count(), 1)
        row = windows.differences()[0]
        self.assertEqual(row['changed_fields']['mid.o'], dict(frozen='1.25', valuation='1.26'))
        self.assertEqual(before, self.frozen.db.execute('SELECT body,sha256 FROM blocks WHERE key=?', (frozen_key,)).fetchone())
        self.assertTrue(market.source_disagreement_seen)

    def test_dedicated_cache_never_merges_an_overlapping_request(self):
        put(self.valuation, T, T + timedelta(minutes=15), '9.99')
        put(self.valuation, START, END, '1.26', 'NEW_DOWNLOAD')
        windows = ValuationWindows(self.valuation, self.frozen)
        self.assertEqual(float(windows.window('GBP_USD', START, END)[0]['mid']['o']), 1.26)

    def test_representation_only_differences_are_not_numeric_revisions(self):
        put(self.frozen, T, T + timedelta(minutes=15), '1.25000')
        put(self.legacy, START, END, '1.25', 'NEW_DOWNLOAD')
        windows = ValuationWindows(self.valuation, self.frozen, self.legacy)
        windows.window('GBP_USD', START, END)
        self.assertEqual(windows.difference_count(), 0)

    def test_corrupt_legacy_response_rejected(self):
        key = put(self.legacy, START, END, '1.25', 'NEW_DOWNLOAD')
        self.legacy.db.execute('UPDATE blocks SET sha256=? WHERE key=?', ('0' * 64, key)); self.legacy.db.commit()
        with self.assertRaisesRegex(ValueError, 'checksum'):
            ValuationWindows(self.valuation, self.frozen, self.legacy).window('GBP_USD', START, END)

    def test_repeated_window_uses_saved_exact_response_and_diagnostics(self):
        put(self.frozen, T, T + timedelta(minutes=15), '1.25')
        put(self.legacy, START, END, '1.26', 'NEW_DOWNLOAD')
        first = ValuationWindows(self.valuation, self.frozen, self.legacy)
        first.window('GBP_USD', START, END)
        second = ValuationWindows(self.valuation, self.frozen)
        with patch.object(self.valuation, '_fetch', side_effect=AssertionError('unexpected download')):
            second.window('GBP_USD', START, END)
        self.assertEqual(second.difference_count(), 1)

    def test_missing_frozen_candle_is_reported_without_promoting_exit(self):
        put(self.frozen, T, T + timedelta(minutes=15), '1.25')
        identity = self.valuation.identity('GBP_USD', 'M15', START, END)
        key = e.sha(e.canon(identity)); body = e.canon(dict(instrument='GBP_USD', granularity='M15', candles=[]))
        self.valuation.db.execute('INSERT INTO blocks VALUES (?,?,?,?,?)',
                                  (key, e.canon(identity).decode(), e.zlib.compress(body), e.sha(body), 'NEW_DOWNLOAD'))
        self.valuation.db.commit()
        windows = ValuationWindows(self.valuation, self.frozen)
        self.assertEqual(windows.window('GBP_USD', START, END), [])
        self.assertEqual(windows.differences()[0]['kind'], 'FROZEN_CANDLE_ABSENT_FROM_VALUATION_RESPONSE')

    def test_fresh_request_does_not_copy_partial_seed_into_new_window(self):
        put(self.frozen, T, T + timedelta(minutes=15), '1.25')
        put(self.legacy, T, T + timedelta(minutes=15), '1.25')
        calls = []
        def transport(request):
            calls.append(request.full_url)
            return dict(instrument='GBP_USD', granularity='M15', candles=[candle('1.26')])
        self.valuation.reader = e.CandleReader('synthetic', 'https://api-fxtrade.oanda.com', 5, transport)
        windows = ValuationWindows(self.valuation, self.frozen, self.legacy)
        windows.window('GBP_USD', START, END)
        self.assertEqual(len(calls), 1)
        self.assertEqual(windows.reused_legacy_windows, 0)
        self.assertEqual(windows.downloads, 1)
        self.assertEqual(windows.difference_count(), 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
