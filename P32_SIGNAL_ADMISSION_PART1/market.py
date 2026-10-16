"""Causal M15 valuations and explicit GBP conversion assumptions.

Only candle OPENs already reached, or completed earlier candle CLOSEs, are used.
High/low/current-bar close never enters a valuation at the bar's opening time.
"""
import bisect
from collections import OrderedDict
from datetime import timedelta
import execution as e

ROUTES = {
    'GBP': (),
    'USD': (('GBP_USD', -1),),
    'EUR': (('EUR_GBP', 1),),
    'JPY': (('USD_JPY', -1), ('GBP_USD', -1)),
    'CAD': (('USD_CAD', -1), ('GBP_USD', -1)),
    'CHF': (('EUR_CHF', -1), ('EUR_GBP', 1)),
    'AUD': (('AUD_USD', 1), ('GBP_USD', -1)),
}


class QuoteUnavailable(Exception):
    pass


class Market:
    def __init__(self, store, progress=lambda **x: None, max_stale_seconds=7 * 86400):
        self.store, self.progress, self.max_stale = store, progress, max_stale_seconds
        self.windows = OrderedDict()
        self.calls = self.window_loads = self.stale_calls = 0
        self.max_age = 0
        self.current_max_age = 0
        self.current_stale = False
        self.quote_gap_seen = False
        self.curve_gap_seen = False
        self.source_disagreement_seen = False

    def begin_event(self):
        self.current_max_age = 0
        self.current_stale = False

    def _window(self, pair, block):
        key = (pair, block)
        if key in self.windows:
            self.windows.move_to_end(key)
            return self.windows[key]
        start = e.datetime.fromtimestamp(block * 14 * 86400, e.UTC)
        end = start + timedelta(days=14)
        self.progress(quote_pair=pair, quote_from=e.stamp(start), quote_to=e.stamp(end),
                      valuation_windows_loaded=self.window_loads, new_candle_blocks=self.store.downloads)
        if hasattr(self.store, 'window'):
            candles = self.store.window(pair, start, end)
            self.source_disagreement_seen = self.source_disagreement_seen or self.store.difference_count() > 0
        elif hasattr(self.store, 'intervals'):
            # Sparse seed windows must not create dozens of tiny HTTP requests
            # for every intervening hole. One canonical 14-day MBA request fills
            # the window; range() still checks overlap against frozen sources.
            cursor = start
            for begin, finish, _ in self.store.intervals(pair, 'M15'):
                if begin > cursor:
                    break
                if finish > cursor:
                    cursor = finish
                if cursor >= end:
                    break
            if cursor < end:
                self.store._fetch(pair, 'M15', start, end)
            candles = self.store.range(pair, 'M15', start, end)
        else:
            candles = self.store.range(pair, 'M15', start, end)
        points = []
        for c in candles:
            t = e.dt(c['time']).timestamp()
            # tie rank 1 means an observed new OPEN wins over the prior CLOSE.
            points.append((t, 1, float(c['bid']['o']), float(c['ask']['o']), float(c['mid']['o'])))
            points.append((t + 900, 0, float(c['bid']['c']), float(c['ask']['c']), float(c['mid']['c'])))
        points.sort(key=lambda p: (p[0], p[1]))
        times = [p[0] for p in points]
        self.windows[key] = (times, points)
        self.window_loads += 1
        while len(self.windows) > 32:
            self.windows.popitem(last=False)
        # Native source objects contain full MBA OHLC dictionaries; the compact
        # valuation tuples above suffice after this window has been consumed.
        while len(self.store.memo) > 8:
            self.store.memo.popitem(last=False)
        return times, points

    def quote(self, pair, when):
        e.require(pair in e.PAIRS, 'Unapproved valuation pair')
        timestamp = e.dt(when).timestamp()
        block = int(timestamp) // (14 * 86400)
        times, points = self._window(pair, block)
        index = bisect.bisect_right(times, timestamp) - 1
        if index < 0:
            times, points = self._window(pair, block - 1)
            index = bisect.bisect_right(times, timestamp) - 1
        if index < 0:
            raise QuoteUnavailable(pair + ' ' + e.stamp(when))
        point = points[index]
        age = timestamp - point[0]
        if age > self.max_stale:
            raise QuoteUnavailable(pair + ' ' + e.stamp(when) + ' beyond maximum causal quote age')
        self.calls += 1
        self.max_age = max(self.max_age, age)
        self.current_max_age = max(self.current_max_age, age)
        if age > 900:
            self.stale_calls += 1
            self.current_stale = True
            if not e.conventional_weekend(when):
                self.quote_gap_seen = True
                self.curve_gap_seen = True
        return dict(bid=point[2], ask=point[3], mid=point[4], source_at=e.stamp(e.datetime.fromtimestamp(point[0], e.UTC)),
                    source_type='M15_OPEN_ASOF' if point[1] else 'COMPLETED_M15_CLOSE_ASOF',
                    source_age_seconds=age, exact_tick_time_known=False)

    def fx_mid(self, currency, when):
        e.require(currency in ROUTES, 'No approved GBP conversion route')
        value = 1.
        for pair, direction in ROUTES[currency]:
            quote = self.quote(pair, when)['mid']
            value *= quote if direction > 0 else 1 / quote
        return value

    def summary(self):
        return dict(valuation_basis='CAUSAL_M15_OPEN_OR_PREVIOUS_COMPLETED_CLOSE',
                    calls=self.calls, windows_loaded=self.window_loads,
                    stale_over_15min_calls=self.stale_calls, max_source_age_seconds=self.max_age,
                    max_permitted_source_age_seconds=self.max_stale,
                    nonweekend_stale_quote_seen=self.quote_gap_seen,
                    valuation_vs_frozen_source_disagreement_seen=self.source_disagreement_seen,
                    no_future_bar_high_low_close_used=True,
                    conversion_routes={k: list(v) for k, v in ROUTES.items()},
                    cross_rate_basis='Chained historical mid-rates, then frozen gain/loss markup once; not recovered broker conversion quotes')
