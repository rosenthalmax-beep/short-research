"""Frozen first-pass hypothesis. All features are known at M15 close."""
import bisect
import math
from array import array
from collections import Counter
from . import core as e

CONTROLS = ('RAW_ENGULF', 'TREND_ENGULF', 'TREND_PULLBACK_ENGULF')
RULES = dict(h1_ema=100, h1_slope_observed_bars=3, m15_ema=20, atr_length=14,
             pullback_max_bars=8, stop_buffer_ticks=10, rr=3.0, entry_limit_ticks=15,
             risk_fraction=.01, one_signal_per_pullback=True,
             h1_timing='Latest completed native H1 at M15 close; simultaneous closes eligible',
             rising='EMA100 strictly above its value three observed H1 bars earlier',
             pullback_touch='Long LOW <= previous-bar EMA20; short HIGH >= previous-bar EMA20',
             rearm='A fully clear M15 candle: long LOW > its EMA20; short HIGH < its EMA20',
             expiry='Eight consecutive M15 bars including touch; reset on any M15 gap or failed H1 trend',
             confirmation='Exact body engulf, close on trend side of current EMA20; touch bar can confirm',
             stop_control='Raw/trend controls use signal extreme; pullback uses extreme from touch through signal',
             raw_control_attribution='Full pullback control changes both selection and stop geometry',
             reset_on_qualification='Consume first qualifying trigger even if execution is later rejected',
             h1_max_age_seconds=3600, gap_policy='Do not invent candles; reset setups and log gaps')


def ema(values, n):
    out = array('d', [math.nan]) * len(values)
    if len(values) < n:
        return out
    out[n - 1] = sum(values[:n]) / n
    alpha = 2. / (n + 1)
    for i in range(n, len(values)):
        out[i] = alpha * values[i] + (1 - alpha) * out[i - 1]
    return out


def atr(bars, n=14):
    v = bars.values['mid']; size = len(bars)
    out = array('d', [math.nan]) * size
    if size < n:
        return out
    tr = array('d', [v['h'][0] - v['l'][0]])
    tr.extend(max(v['h'][i] - v['l'][i], abs(v['h'][i] - v['c'][i-1]),
                  abs(v['l'][i] - v['c'][i-1])) for i in range(1, size))
    out[n - 1] = sum(tr[:n]) / n
    for i in range(n, size):
        out[i] = (out[i - 1] * (n - 1) + tr[i]) / n
    return out


def exact_engulf(previous, current, side):
    # Exact inequalities from locked_rules.m15_exact_*_engulf, no body threshold.
    if side == 'BUY':
        return previous['c'] < previous['o'] and current['c'] > current['o'] and current['o'] <= previous['c'] and current['c'] >= previous['o']
    return previous['c'] > previous['o'] and current['c'] < current['o'] and current['o'] >= previous['c'] and current['c'] <= previous['o']


def gap_rows(bars, seconds, pair, gran, start, end):
    rows = []
    # A partially elapsed final H1 interval is not a missing complete H1 bar.
    end = e.floor_time(end, seconds)
    start = e.floor_time(start, seconds) if e.floor_time(start, seconds) == start else e.floor_time(start, seconds) + e.timedelta(seconds=seconds)
    # Only describe gaps inside the evaluated range. Leading/trailing gaps are
    # important too; weekend labels are assumptions, not proof of data quality.
    prior = int(start.timestamp())
    for t in bars.times:
        if t >= end.timestamp():
            break
        if t < prior:
            continue
        if t > prior:
            missing, weekends = 0, 0
            for value in range(prior, min(t, int(end.timestamp())), seconds):
                if e.conventional_weekend(e.datetime.fromtimestamp(value, e.UTC)):
                    weekends += 1
                else:
                    missing += 1
            if missing or weekends:
                rows.append(dict(pair=pair, granularity=gran, start_utc=e.stamp(e.datetime.fromtimestamp(prior, e.UTC)),
                                 end_utc=e.stamp(e.datetime.fromtimestamp(min(t, int(end.timestamp())), e.UTC)),
                                 unexplained_slots=missing, assumed_weekend_slots=weekends))
        prior = t + seconds
        if prior >= end.timestamp():
            break
    if prior < end.timestamp():
        missing = sum(not e.conventional_weekend(e.datetime.fromtimestamp(v, e.UTC))
                      for v in range(prior, int(end.timestamp()), seconds))
        slots = (int(end.timestamp()) - prior) // seconds
        rows.append(dict(pair=pair, granularity=gran, start_utc=e.stamp(e.datetime.fromtimestamp(prior, e.UTC)),
                         end_utc=e.stamp(end), unexplained_slots=missing, assumed_weekend_slots=slots-missing))
    return rows


def generate(pair, m15, h1, start, end, progress=lambda **x: None, indicator_ready_from=None):
    m, h = m15.values['mid'], h1.values['mid']
    ma = ema(m['c'], 20); av = atr(m15)
    hm = {n: ema(h['c'], n) for n in (50, 100, 200)}; ha = atr(h1)
    setups = {side: None for side in ('BUY', 'SELL')}
    armed = {side: False for side in setups}
    counts = Counter(); result = []
    start_ts, end_ts = int(start.timestamp()), int(end.timestamp())
    ready_ts = int(indicator_ready_from.timestamp()) if indicator_ready_from is not None else -math.inf
    hi = -1
    for i in range(1, len(m15)):
        t = m15.times[i] + 900
        while hi + 1 < len(h1) and h1.times[hi + 1] + 3600 <= t:
            hi += 1
        if t < ready_ts:
            if start_ts <= t < end_ts:
                counts['evaluation_bars_excluded_for_indicator_warmup'] += 1
            # No setup is accumulated using indicators before the fixed
            # history requirement. Future candles cannot repair earlier bars.
            continue
        if m15.times[i] != m15.times[i - 1] + 900:
            counts['m15_gap_setup_resets'] += 1
            setups = {side: None for side in setups}; armed = {side: False for side in setups}
            continue
        if not math.isfinite(av[i]) or av[i] <= 0 or not math.isfinite(ma[i - 1]):
            continue
        current = {k: m[k][i] for k in 'ohlc'}
        previous = {k: m[k][i - 1] for k in 'ohlc'}
        h_ready = (hi >= 102 and math.isfinite(hm[100][hi-3]) and ha[hi] > 0 and
                   0 <= t - (h1.times[hi] + 3600) < RULES['h1_max_age_seconds'])
        if start_ts <= t < end_ts and not h_ready:
            counts['m15_evaluations_without_fresh_h1'] += 1
        for side in ('BUY', 'SELL'):
            sign = 1 if side == 'BUY' else -1
            trend = bool(h_ready and sign * (h['c'][hi] - hm[100][hi]) > 0 and
                         sign * (hm[100][hi] - hm[100][hi-3]) > 0)
            engulf = exact_engulf(previous, current, side)
            setup = setups[side]
            if not trend:
                setup = None; armed[side] = False
            else:
                if setup is not None and i - setup['index'] >= RULES['pullback_max_bars']:
                    counts['expired_setups'] += 1
                    setup = None; armed[side] = False
                touch = current['l'] <= ma[i-1] if side == 'BUY' else current['h'] >= ma[i-1]
                if setup is None and armed[side] and touch:
                    setup = dict(index=i, extreme=current['l'] if side == 'BUY' else current['h'],
                                 area=ma[i-1], start_atr=av[i])
                    armed[side] = False
                if setup is not None:
                    setup['extreme'] = (min(setup['extreme'], current['l']) if side == 'BUY'
                                        else max(setup['extreme'], current['h']))
            qualifies_pullback = bool(trend and setup is not None and engulf and sign * (current['c'] - ma[i]) > 0)
            if engulf and start_ts <= t < end_ts:
                span = current['h'] - current['l']
                features = dict(body_atr=abs(current['c'] - current['o']) / av[i], range_atr=span / av[i],
                                close_location=((current['c'] - current['l']) if side == 'BUY' else (current['h'] - current['c'])) / span if span else None,
                                h1_trend=trend, h1_source_start_utc=e.stamp(e.datetime.fromtimestamp(h1.times[hi], e.UTC)) if h_ready else None,
                                h1_source_close_utc=e.stamp(e.datetime.fromtimestamp(h1.times[hi] + 3600, e.UTC)) if h_ready else None,
                                h1_slope_atr=sign * (hm[100][hi] - hm[100][hi-3]) / ha[hi] if h_ready else None,
                                h1_distance_atr=sign * (h['c'][hi] - hm[100][hi]) / ha[hi] if h_ready else None,
                                h1_ema50_200_aligned=bool(h_ready and math.isfinite(hm[200][hi]) and sign * (hm[50][hi] - hm[200][hi]) > 0),
                                pullback_qualified=qualifies_pullback,
                                duration_bars=i - setup['index'] + 1 if setup else None,
                                depth_atr=max(0., sign * (setup['area'] - setup['extreme'])) / setup['start_atr'] if setup else None)
                controls = ['RAW_ENGULF'] + (['TREND_ENGULF'] if trend else []) + (['TREND_PULLBACK_ENGULF'] if qualifies_pullback else [])
                for control in controls:
                    extreme = setup['extreme'] if control == 'TREND_PULLBACK_ENGULF' else current['l'] if side == 'BUY' else current['h']
                    stop = float(e.broker_price(pair, extreme - sign * RULES['stop_buffer_ticks'] * float(e.tick(pair))))
                    geometry = e.levels(pair, side, current['c'], stop, RULES['rr'], 15)
                    identity = dict(pair=pair, side=side, control=control, close=t)
                    sid = 'PB1_' + pair + '_' + side + '_' + control
                    feature = dict(features, stop_atr=sign * (float(geometry['reference_entry']) - stop) / av[i])
                    result.append(dict(signal_key=e.sha(e.canon(identity)), strategy_id=sid, control=control,
                                       pair=pair, side=side, timeframe='M15', same_strategy_trigger_priority=0,
                                       signal_start_utc=e.stamp(e.datetime.fromtimestamp(m15.times[i], e.UTC)),
                                       signal_close_utc=e.stamp(e.datetime.fromtimestamp(t, e.UTC)),
                                       entry_utc=e.stamp(e.datetime.fromtimestamp(t, e.UTC)), rr=RULES['rr'],
                                       reference_entry=float(geometry['reference_entry']), stop=stop,
                                       target=float(geometry['target']), atr14=av[i], features=feature,
                                       pullback_start_utc=e.stamp(e.datetime.fromtimestamp(m15.times[setup['index']], e.UTC)) if control == 'TREND_PULLBACK_ENGULF' else None))
                    counts[control + '_' + side] += 1
            if qualifies_pullback:
                setup = None; armed[side] = False
            elif trend and setup is None:
                armed[side] = current['l'] > ma[i] if side == 'BUY' else current['h'] < ma[i]
            setups[side] = setup
        if i % 10000 == 0:
            progress(phase='GENERATE_SIGNALS', pair=pair, bars_evaluated=i, bars_required=len(m15),
                     signals_found=len(result), through_utc=e.stamp(e.datetime.fromtimestamp(t, e.UTC)))
    return result, dict(counts)


# Diagnostic partitions of the frozen first qualifying pullback signal.
# These are NOT replayed alternative filter strategies and never produce CAGR.
FEATURE_FILTERS = [dict(filter_id='UNFILTERED', feature=None, operator=None, threshold=None)]
for feature, op, thresholds in (
    ('body_atr', '>=', (.25, .5, .75, 1.0)),
    ('range_atr', '>=', (.5, .75, 1., 1.25)),
    ('close_location', '>=', (.55, .65, .75, .85)),
    ('duration_bars', '<=', (2, 4, 6)),
    ('depth_atr', '<=', (.5, 1., 1.5, 2.)),
    ('h1_slope_atr', '>=', (.05, .1, .2)),
    ('h1_distance_atr', '<=', (.5, 1., 2., 3.)),
    ('stop_atr', '<=', (1., 2., 3., 4.)),
    ('h1_ema50_200_aligned', '==', (True,)),
):
    for value in thresholds:
        FEATURE_FILTERS.append(dict(filter_id=feature + op + str(value), feature=feature, operator=op, threshold=value))


def accepts(features, rule):
    if rule['feature'] is None:
        return True
    value = features.get(rule['feature'])
    if value is None:
        return False
    return value >= rule['threshold'] if rule['operator'] == '>=' else value <= rule['threshold'] if rule['operator'] == '<=' else value == rule['threshold']
