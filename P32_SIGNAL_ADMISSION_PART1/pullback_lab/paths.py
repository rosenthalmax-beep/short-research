"""Cached executable paths with M1/S5 refinement, explicit unknown outcomes."""
import bisect
import json
import math
import sqlite3
from array import array
from collections import OrderedDict
from datetime import timedelta
from pathlib import Path
from . import core as e
from .accounting import converted, cash
from .funding import rollovers


class BarrierIndex:
    def __init__(self, lows, highs):
        self.n = 1
        while self.n < len(lows):
            self.n *= 2
        self.lo = array('d', [math.inf]) * (self.n * 2)
        self.hi = array('d', [-math.inf]) * (self.n * 2)
        self.lo[self.n:self.n + len(lows)] = array('d', lows)
        self.hi[self.n:self.n + len(highs)] = array('d', highs)
        for i in range(self.n - 1, 0, -1):
            self.lo[i] = min(self.lo[2 * i], self.lo[2 * i + 1])
            self.hi[i] = max(self.hi[2 * i], self.hi[2 * i + 1])

    def first(self, start, end, lower, upper):
        def find(node, left, right):
            if right <= start or left >= end or (self.lo[node] > lower and self.hi[node] < upper):
                return None
            if right - left == 1:
                return left
            mid = (left + right) // 2
            result = find(node * 2, left, mid)
            return result if result is not None else find(node * 2 + 1, mid, right)
        return find(1, 0, self.n)


class Paths:
    def __init__(self, directory, store, bars, pair, end, signature, progress=lambda **x: None):
        self.db = sqlite3.connect(str(Path(directory) / 'paths.sqlite3'))
        self.db.execute('CREATE TABLE IF NOT EXISTS paths(key TEXT PRIMARY KEY, body BLOB, sha256 TEXT)')
        self.db.commit()
        self.store, self.bars, self.pair, self.end = store, bars, pair, end
        self.signature, self.progress = signature, progress
        self.memo = OrderedDict(); self.trees = {}
        self.computed = self.cached = 0
        self.gaps = array('q', [0]) * len(bars)
        for i in range(1, len(bars)):
            missing = sum(not e.conventional_weekend(e.datetime.fromtimestamp(t, e.UTC))
                          for t in range(bars.times[i - 1] + 900, bars.times[i], 900))
            self.gaps[i] = self.gaps[i-1] + missing
        self.trailing_gaps = sum(not e.conventional_weekend(e.datetime.fromtimestamp(t, e.UTC))
                                 for t in range(bars.times[-1] + 900, int(end.timestamp()), 900)) if len(bars) else 0
        digest = e.hashlib.sha256(bars.times.tobytes())
        for component in ('mid', 'bid', 'ask'):
            for field in 'ohlc':
                digest.update(bars.values[component][field].tobytes())
        self.source_sha = digest.hexdigest()

    def get(self, signal):
        identity = dict(signature=self.signature, native_sha256=self.source_sha, pair=self.pair,
                        side=signal['side'], entry=signal['entry_utc'], reference=signal['reference_entry'],
                        stop=signal['stop'], rr=signal['rr'], end=e.stamp(self.end), limit=15)
        key = e.sha(e.canon(identity))
        if key in self.memo:
            self.cached += 1
            self.memo.move_to_end(key)
            return self.memo[key]
        old = self.db.execute('SELECT body,sha256 FROM paths WHERE key=?', (key,)).fetchone()
        if old:
            e.require(e.sha(old[0]) == old[1], 'Path checkpoint checksum failed')
            result = json.loads(old[0]); self.cached += 1
        else:
            result = self.compute(signal)
            result.update(native_source_sha256=self.source_sha, path_cache_key=key)
            raw = e.canon(result)
            self.db.execute('INSERT INTO paths VALUES(?,?,?)', (key, raw, e.sha(raw)))
            self.db.commit(); self.computed += 1
        self.memo[key] = result
        while len(self.memo) > 2000:
            self.memo.popitem(last=False)
        return result

    def compute(self, signal):
        ts = int(e.dt(signal['entry_utc']).timestamp())
        index = bisect.bisect_left(self.bars.times, ts)
        if index >= len(self.bars) or self.bars.times[index] != ts:
            return dict(status='UNRESOLVED_ENTRY', entry_status='MISSING_EXACT_M15_ENTRY_OPEN',
                        source_horizon_utc=e.stamp(self.end))
        first = self.bars.row(index)
        g = e.entry_preview(self.pair, signal['side'], signal['reference_entry'], signal['stop'],
                            signal['rr'], first['bid']['o'], first['ask']['o'], 15)
        result = {'model_' + k: v for k, v in e.serial_geometry(g).items()}
        result.update(entry_status=g['status'], source_horizon_utc=e.stamp(self.end),
                      entry_model='FIRST_NATIVE_M15_OPEN_PROXY', live_latency_and_cancellations_modeled=False)
        rejected = g['status'].startswith('REJECTED_')
        if g['stop_distance'] <= 0:
            return dict(result, status='ENTRY_PROXY_REJECTED', counterfactual_outcome_status='INVALID_STOP_GEOMETRY')
        side = signal['side']; component = 'bid' if side == 'BUY' else 'ask'
        if component not in self.trees:
            self.trees[component] = BarrierIndex(self.bars.values[component]['l'], self.bars.values[component]['h'])
        lo, high = (float(g['stop']), float(g['target'])) if side == 'BUY' else (float(g['target']), float(g['stop']))
        hit = self.trees[component].first(index, len(self.bars), lo, high)
        if hit is None:
            missing = self.gaps[-1] - self.gaps[index] + self.trailing_gaps
            outcome = dict(kind='OPEN_AT_FIXED_CUTOFF', exit_price=None,
                           interval_start_utc=None, interval_end_utc=None,
                           certainty='NO_TOUCH_WITH_UNEXPLAINED_GAPS' if missing else 'NO_TOUCH_OBSERVED_TO_CUTOFF',
                           prior_native_unexplained_slots=missing,
                           last_observed_close=self.bars.values[component]['c'][-1],
                           last_observed_close_time_utc=e.stamp(e.datetime.fromtimestamp(self.bars.times[-1] + 900, e.UTC)))
        else:
            outcome = e.refine(self.store, self.pair, side, component, g['stop'], g['target'], self.bars.row(hit), 'M15')
            missing = self.gaps[hit] - self.gaps[index]
            if missing and outcome['certainty'] == 'RESOLVED_TO_CANDLE_INTERVAL':
                outcome['certainty'] = 'TOUCH_WITH_EARLIER_UNEXPLAINED_GAP'
            outcome.update(prior_native_unexplained_slots=missing,
                           native_interval_start_utc=e.stamp(e.datetime.fromtimestamp(self.bars.times[hit], e.UTC)))
        status = ('CLOSED_RESOLVED' if outcome['kind'] in ('STOP', 'TARGET') and outcome['certainty'] == 'RESOLVED_TO_CANDLE_INTERVAL'
                  else 'OPEN_AT_CUTOFF' if outcome['kind'] == 'OPEN_AT_FIXED_CUTOFF' and outcome['certainty'] == 'NO_TOUCH_OBSERVED_TO_CUTOFF'
                  else 'UNRESOLVED_PATH')
        result.update(outcome, exit_component=component, status='ENTRY_PROXY_REJECTED' if rejected else status)
        if rejected:
            result['counterfactual_outcome_status'] = status
            result['counterfactual_basis'] = 'BYPASS_ENTRY_GUARD_AT_OBSERVED_QUOTE; ORIGINAL_STOP_TARGET; NOT_A_TRADE'
        if outcome.get('exit_price') is not None:
            sign = 1 if side == 'BUY' else -1
            move = sign * (e.dec(outcome['exit_price']) - g['fill'])
            result['observed_path_price_r'] = float(move / g['stop_distance'])
            result['observed_path_worst_bound_budget_r'] = float(move / g['worst_stop_distance'])
        return result

    def close(self):
        self.db.close()


def truncate(path, end):
    """Account windows never use a later exit or close before evidence allows."""
    if path['status'] in ('ENTRY_PROXY_REJECTED', 'UNRESOLVED_ENTRY'):
        return path
    if path.get('interval_end_utc') and e.dt(path['interval_end_utc']) <= end:
        return path
    return dict(path, kind='OPEN_AT_FIXED_CUTOFF', exit_price=None,
                interval_start_utc=None, interval_end_utc=None,
                status='OPEN_AT_CUTOFF' if path['status'] in ('CLOSED_RESOLVED', 'OPEN_AT_CUTOFF') else 'UNRESOLVED_PATH',
                window_truncated=True)


def shadow_gbp(signal, path, market, funding, terms, markup, end):
    """Independent GBP100 opportunity: no overlapping positions or compounding.

    These rows support feature discovery only. Actual control account admission
    is separately rerun from ALL signals with nonhedging/pyramiding/margin gates.
    """
    if path['status'] != 'CLOSED_RESOLVED':
        return dict(shadow_status=path['status'])
    begin = e.dt(signal['entry_utc']); finish = e.dt(path['interval_end_utc'])
    base, quote = signal['pair'].split('_')
    market.begin_event()
    quote_fx = market.fx_mid(quote, begin)
    distance = abs(float(path['model_price_bound']) - float(path['model_stop']))
    loss_factor = quote_fx * (1 if quote == 'GBP' else 1 + markup)
    units = math.floor(1. / (distance * loss_factor))
    if units < float(terms['minimumTradeSize']):
        return dict(shadow_status='BELOW_MINIMUM_UNITS', units=units)
    if units > float(terms['maximumOrderUnits']):
        return dict(shadow_status='ABOVE_MAXIMUM_UNITS', units=units)
    sign = 1 if signal['side'] == 'BUY' else -1
    price = market.quote(signal['pair'], begin)
    margin = units * market.fx_mid(base, begin) * float(terms['marginRate'])
    post_nav = cash(100. + units * sign * (price['mid'] - path['model_fill']) * quote_fx)
    if margin > post_nav:
        return dict(shadow_status='INSUFFICIENT_STANDALONE_MARGIN', units=units)
    stale = market.current_stale
    actual_risk = units * abs(path['model_fill'] - path['model_stop']) * loss_factor
    market.begin_event()
    pl, fee = converted(units * sign * (path['exit_price'] - path['model_fill']), market.fx_mid(quote, finish), quote, markup)
    pl = cash(pl); stale = stale or market.current_stale
    amounts = {'FROZEN_14OCT2026_RATES': 0., 'ADVERSE_6PCT_ANNUAL': 0.}
    rollover_count = 0
    for when in rollovers(begin, finish):
        if not begin < when < finish or when.astimezone(e.NY).weekday() >= 5:
            continue
        rollover_count += 1; market.begin_event()
        fx = market.fx_mid(base, when)
        stale = stale or market.current_stale
        for mode in amounts:
            term = funding.terms(mode, signal['pair'], signal['side'], when)
            amount, _ = converted(units * term['annual_rate'] * term['charged_days'] / 365, fx, base, markup)
            amounts[mode] = cash(amounts[mode] + cash(amount))
    return dict(shadow_status='CLOSED_RESOLVED', shadow_basis='INDEPENDENT_GBP100_ONE_OPPORTUNITY_NOT_ACCOUNT_RETURN',
                units=units, actual_stop_risk_gbp=actual_risk, sizing_risk_gbp=units * distance * loss_factor,
                price_pl_gbp=pl, price_r=pl / actual_risk,
                frozen_financing_gbp=amounts['FROZEN_14OCT2026_RATES'],
                frozen_net_r=cash(pl + amounts['FROZEN_14OCT2026_RATES']) / actual_risk,
                adverse6_net_r=cash(pl + amounts['ADVERSE_6PCT_ANNUAL']) / actual_risk,
                rollover_events=rollover_count, duration_hours=(finish - begin).total_seconds() / 3600,
                stale_conversion_quote_seen=stale, historical_financing_known=False)
