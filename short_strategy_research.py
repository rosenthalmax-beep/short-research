import os
import csv
import math
import time
import zipfile
import threading
from bisect import bisect_left
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from statistics import median

import numpy as np
import requests
from flask import Flask, jsonify, send_file


# ============================================================
# EUR/JPY H1 LONG — ENGULF/STRUCTURE LOCAL REFINEMENT
# ============================================================
#
# PURPOSE
# -------
# Refine ONLY the raw EUR/JPY H1 LONG family discovered in the
# prior broad new-pair scan:
#
#   exact bullish engulfing
#   + minimum engulfing body ratio
#   + minimum bullish body / ATR14
#   + proximity to prior rolling low
#   + fixed stop buffer
#   + fixed-R target
#
# No session / weekday / HTF regime filters are tested here.
# Those are intentionally reserved for the NEXT stage, after
# we identify a genuine local geometry/RR plateau.
#
# IMPORTANT
# ---------
# This is a research-only service. It cannot place orders.
#
# ============================================================
# HISTORICAL CONVENTIONS
# ============================================================
#
# Instrument: OANDA EUR_JPY
# Timeframe:  H1
# Price:      midpoint candles
# ATR14:      Wilder/RMA, SMA seeded
#
# Signal:
#   previous candle bearish
#   current candle bullish
#   current body exactly engulfs previous body
#
# Structure:
#   current signal LOW compared with PRIOR rolling low
#   current candle excluded from lookback
#   absolute low-to-prior-low distance <= X * ATR14
#
# Entry reference:
#   signal candle close
#
# Historical adverse entry fill:
#   +0.5 pip for LONG
#   EUR/JPY pip = 0.01
#
# Stop:
#   signal low - 10 ticks
#   EUR/JPY tick = 0.001
#
# Target:
#   reference close + RR * reference-close risk
#
# Realised R:
#   measured from adverse historical fill to stop/target
#
# Exit:
#   first subsequent H1 candle to touch stop/target
#   signal candle itself cannot exit
#
# Same-bar stop+target tie:
#   if candle HIGH is closer to candle open => TARGET first
#   otherwise STOP first
#
# Pyramiding:
#   0 for the candidate strategy
#
# Exact exit-candle signal:
#   eligible for a new trade
#
# ============================================================
# SEARCH DESIGN
# ============================================================
#
# STAGE 1 — geometry at 5R
#
#   body ratio:
#       1.10, 1.20, 1.30, 1.40, 1.50
#
#   bullish body / ATR14:
#       1.00, 1.10, 1.20, 1.25, 1.30, 1.45, 1.60
#
#   prior-low lookback:
#       8, 10, 12, 15, 18, 20, 25, 30, 45, 60
#
#   structure distance / ATR14:
#       0.05, 0.10, 0.15, 0.20, 0.25
#
# = 1,750 broad local geometries.
#
# STAGE 2 — RR sweep only on robust Stage-1 geometries
#
#   3.5R, 4R, 4.5R, 5R, 5.5R, 6R, 6.5R, 7R
#
# The runner ranks by robustness rather than maximum PF alone.
# It reports old/new temporal splits, eras, recent periods,
# 2x-cost stress, calendar years, rolling windows and local
# parameter-neighbourhood stability.
#
# ============================================================


app = Flask(__name__)

TOKEN = os.getenv("OANDA_TOKEN")
BASE = os.getenv("OANDA_API_URL", "https://api-fxtrade.oanda.com")

PAIR = "EUR_JPY"
PAIR_LABEL = "EUR/JPY"

REQUESTED_START = datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc)
VALIDATION_START = datetime(2018, 1, 1, tzinfo=timezone.utc)
NOW = datetime.now(timezone.utc).replace(second=0, microsecond=0)

TICK = 0.001
PIP = 0.01
STOP_BUFFER_TICKS = 10
BASE_COST_PIPS = 0.50

STAGE1_RR = 5.0

BR_GRID = [1.10, 1.20, 1.30, 1.40, 1.50]
BODY_ATR_GRID = [1.00, 1.10, 1.20, 1.25, 1.30, 1.45, 1.60]
LOOKBACK_GRID = [8, 10, 12, 15, 18, 20, 25, 30, 45, 60]
DISTANCE_GRID = [0.05, 0.10, 0.15, 0.20, 0.25]
RR_GRID = [3.50, 4.00, 4.50, 5.00, 5.50, 6.00, 6.50, 7.00]

STAGE2_GEOMETRY_LIMIT = 60
FINALIST_LIMIT = 20

OUTS = {
    "coverage": "eurjpy_h1_long_refinement_coverage.csv",
    "stage1": "eurjpy_h1_long_refinement_stage1_geometry.csv",
    "stage1_shortlist": "eurjpy_h1_long_refinement_stage1_shortlist.csv",
    "stage2": "eurjpy_h1_long_refinement_stage2_rr.csv",
    "finalists": "eurjpy_h1_long_refinement_finalists.csv",
    "periods": "eurjpy_h1_long_refinement_periods.csv",
    "cost_stress": "eurjpy_h1_long_refinement_cost_stress.csv",
    "calendar": "eurjpy_h1_long_refinement_calendar.csv",
    "calendar_summary": "eurjpy_h1_long_refinement_calendar_summary.csv",
    "rolling": "eurjpy_h1_long_refinement_rolling.csv",
    "rolling_summary": "eurjpy_h1_long_refinement_rolling_summary.csv",
    "plateau": "eurjpy_h1_long_refinement_plateau.csv",
    "axis_summary": "eurjpy_h1_long_refinement_axis_summary.csv",
    "trades": "eurjpy_h1_long_refinement_finalist_trades.csv",
    "notes": "eurjpy_h1_long_refinement_notes.csv",
}

BUNDLE = "EURJPY_H1_LONG_ENGULF_STRUCTURE_REFINEMENT_RESULTS.zip"

STATUS = {
    "state": "not_started",
    "message": "Not started",
    "pair": PAIR,
    "timeframe": "H1",
    "side": "LONG",
    "family": "ENGULF_STRUCTURE",
    "orders_supported": False,
    "trading_enabled": False,
}


# ============================================================
# BASIC HELPERS
# ============================================================

def iso(dt):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_time(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def med(values):
    values = list(values)
    return median(values) if values else 0.0


def safe_pf(gross_profit, gross_loss):
    if gross_loss > 0:
        return gross_profit / gross_loss
    if gross_profit > 0:
        return 99.0
    return 0.0


def write_csv(path, rows):
    rows = list(rows)

    if not rows:
        Path(path).write_text("", encoding="utf-8")
        return

    fields = []
    seen = set()

    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fields.append(key)

    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def add_months(dt, months):
    value = dt.year * 12 + dt.month - 1 + months
    return datetime(value // 12, value % 12 + 1, 1, tzinfo=timezone.utc)


def month_floor(dt):
    return datetime(dt.year, dt.month, 1, tzinfo=timezone.utc)


def download(path):
    if not os.path.exists(path):
        return jsonify({
            "status": "not_ready",
            "state": STATUS["state"],
            "message": STATUS["message"],
        }), 404

    return send_file(
        os.path.abspath(path),
        as_attachment=True,
        download_name=os.path.basename(path),
    )


def pack_results():
    with zipfile.ZipFile(BUNDLE, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in OUTS.values():
            if os.path.exists(path):
                archive.write(path, arcname=os.path.basename(path))


# ============================================================
# OANDA HISTORY
# ============================================================

def headers():
    if not TOKEN:
        raise RuntimeError("OANDA_TOKEN is not configured")
    return {"Authorization": "Bearer " + TOKEN.strip()}


def fetch_chunk(start, end):
    response = requests.get(
        f"{BASE}/v3/instruments/{PAIR}/candles",
        headers=headers(),
        params={
            "price": "M",
            "granularity": "H1",
            "smooth": "false",
            "from": iso(start),
            "to": iso(end),
            "includeFirst": "true",
        },
        timeout=60,
    )
    response.raise_for_status()

    rows = []

    for raw in response.json().get("candles", []):
        if not raw.get("complete", False):
            continue

        mid = raw.get("mid")
        if not mid:
            continue

        rows.append({
            "time": parse_time(raw["time"]),
            "open": float(mid["o"]),
            "high": float(mid["h"]),
            "low": float(mid["l"]),
            "close": float(mid["c"]),
        })

    return rows


def fetch_history(start, end):
    cursor = start
    by_time = {}
    chunk_no = 0

    while cursor < end:
        chunk_no += 1
        chunk_end = min(cursor + timedelta(days=180), end)

        STATUS.update({
            "state": "fetching",
            "message": f"H1 chunk {chunk_no}: {iso(cursor)} -> {iso(chunk_end)}",
        })

        rows = None
        last_error = None

        for attempt in range(1, 4):
            try:
                rows = fetch_chunk(cursor, chunk_end)
                last_error = None
                break
            except requests.HTTPError as error:
                status_code = (
                    error.response.status_code
                    if error.response is not None
                    else None
                )

                # Some instruments have no history in an early requested period.
                if status_code in (400, 404):
                    rows = []
                    last_error = None
                    break

                last_error = error
            except Exception as error:
                last_error = error

            if attempt < 3:
                time.sleep(0.5 * attempt)

        if last_error is not None:
            raise last_error

        for candle in rows or []:
            by_time[candle["time"]] = candle

        cursor = chunk_end
        time.sleep(0.02)

    candles = sorted(by_time.values(), key=lambda row: row["time"])
    return candles


# ============================================================
# INDICATORS / FEATURE CACHE
# ============================================================

def atr14_array(candles):
    n = len(candles)
    result = np.full(n, np.nan, dtype=float)

    if n == 0:
        return result

    high = np.array([c["high"] for c in candles], dtype=float)
    low = np.array([c["low"] for c in candles], dtype=float)
    close = np.array([c["close"] for c in candles], dtype=float)

    tr = np.full(n, np.nan, dtype=float)
    tr[0] = high[0] - low[0]

    for i in range(1, n):
        tr[i] = max(
            high[i] - low[i],
            abs(high[i] - close[i - 1]),
            abs(low[i] - close[i - 1]),
        )

    period = 14

    if n < period:
        return result

    result[period - 1] = float(np.mean(tr[:period]))

    for i in range(period, n):
        result[i] = (
            result[i - 1] * (period - 1)
            + tr[i]
        ) / period

    return result


def rolling_previous_low(values, lookback):
    values = np.asarray(values, dtype=float)
    n = len(values)
    result = np.full(n, np.nan, dtype=float)
    dq = deque()

    for i in range(n):
        add_index = i - 1

        if add_index >= 0:
            value = values[add_index]
            while dq and values[dq[-1]] >= value:
                dq.pop()
            dq.append(add_index)

        minimum_index = i - lookback
        while dq and dq[0] < minimum_index:
            dq.popleft()

        if i >= lookback and dq:
            result[i] = values[dq[0]]

    return result


def build_features(candles):
    opens = np.array([c["open"] for c in candles], dtype=float)
    highs = np.array([c["high"] for c in candles], dtype=float)
    lows = np.array([c["low"] for c in candles], dtype=float)
    closes = np.array([c["close"] for c in candles], dtype=float)
    times = [c["time"] for c in candles]

    atr = atr14_array(candles)

    previous_open = np.full(len(candles), np.nan, dtype=float)
    previous_close = np.full(len(candles), np.nan, dtype=float)

    if len(candles) > 1:
        previous_open[1:] = opens[:-1]
        previous_close[1:] = closes[:-1]

    current_body = closes - opens
    previous_body = np.abs(previous_close - previous_open)

    body_atr = np.divide(
        current_body,
        atr,
        out=np.zeros_like(closes),
        where=(current_body > 0) & np.isfinite(atr) & (atr > 0),
    )

    body_ratio = np.divide(
        current_body,
        previous_body,
        out=np.zeros_like(closes),
        where=(current_body > 0) & (previous_body > 0),
    )

    exact_bull = (
        (previous_close < previous_open)
        & (closes > opens)
        & (opens <= previous_close)
        & (closes >= previous_open)
    )

    prior_lows = {
        lookback: rolling_previous_low(lows, lookback)
        for lookback in LOOKBACK_GRID
    }

    return {
        "times": times,
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
        "atr": atr,
        "body_atr": body_atr,
        "body_ratio": body_ratio,
        "exact_bull": exact_bull,
        "prior_lows": prior_lows,
    }


# ============================================================
# CONFIG / SIGNALS
# ============================================================

def config_id(config):
    return (
        "EURJPY_H1_LONG"
        f"|BR={config['br_min']:.2f}"
        f"|BODYATR={config['body_atr_min']:.2f}"
        f"|LB={int(config['lookback'])}"
        f"|DIST={config['distance_atr_max']:.3f}"
        f"|RR={config['rr']:.2f}"
    )


def make_config(br_min, body_atr_min, lookback, distance_atr_max, rr):
    config = {
        "pair": PAIR,
        "timeframe": "H1",
        "side": "LONG",
        "family": "ENGULF_STRUCTURE",
        "br_min": float(br_min),
        "body_atr_min": float(body_atr_min),
        "lookback": int(lookback),
        "distance_atr_max": float(distance_atr_max),
        "rr": float(rr),
    }
    config["config_id"] = config_id(config)
    return config


def raw_signal_indices(config, features):
    atr = features["atr"]
    lows = features["low"]
    prior_low = features["prior_lows"][config["lookback"]]

    distance_atr = np.divide(
        np.abs(lows - prior_low),
        atr,
        out=np.full(len(lows), np.inf, dtype=float),
        where=np.isfinite(prior_low) & np.isfinite(atr) & (atr > 0),
    )

    mask = (
        features["exact_bull"]
        & np.isfinite(atr)
        & (atr > 0)
        & (features["body_ratio"] >= config["br_min"])
        & (features["body_atr"] >= config["body_atr_min"])
        & (distance_atr <= config["distance_atr_max"])
    )

    return np.flatnonzero(mask).astype(int)


# ============================================================
# TRADE SIMULATION
# ============================================================

def determine_exit(features, index, stop, target):
    o = features["open"][index]
    h = features["high"][index]
    l = features["low"][index]

    stop_touched = l <= stop
    target_touched = h >= target

    if stop_touched and not target_touched:
        return "STOP"

    if target_touched and not stop_touched:
        return "TARGET"

    if stop_touched and target_touched:
        distance_to_high = abs(h - o)
        distance_to_low = abs(o - l)
        return "TARGET" if distance_to_high < distance_to_low else "STOP"

    return None


def find_exit_index(features, signal_index, stop, target):
    high = features["high"]
    low = features["low"]
    n = len(high)

    block = 2048

    for left in range(signal_index + 1, n, block):
        right = min(left + block, n)

        touched = (
            (low[left:right] <= stop)
            | (high[left:right] >= target)
        )

        hits = np.flatnonzero(touched)

        if len(hits):
            return left + int(hits[0])

    return None


def backtest(config, features, signal_indices, cost_multiplier=1.0):
    times = features["times"]
    close = features["close"]
    low = features["low"]
    high = features["high"]

    rr = config["rr"]
    adverse_cost = BASE_COST_PIPS * cost_multiplier * PIP

    signal_indices = np.asarray(signal_indices, dtype=int)
    trades = []
    pointer = 0

    while pointer < len(signal_indices):
        signal_index = int(signal_indices[pointer])

        reference_entry = float(close[signal_index])
        stop = float(low[signal_index]) - STOP_BUFFER_TICKS * TICK
        reference_risk = reference_entry - stop

        if reference_risk <= 0:
            pointer += 1
            continue

        target = reference_entry + rr * reference_risk
        fill = reference_entry + adverse_cost
        actual_risk = fill - stop

        if actual_risk <= 0:
            pointer += 1
            continue

        exit_index = find_exit_index(
            features,
            signal_index,
            stop,
            target,
        )

        if exit_index is None:
            # Open at end of history; omit from closed-trade stats.
            break

        reason = determine_exit(
            features,
            exit_index,
            stop,
            target,
        )

        if reason is None:
            raise RuntimeError("Unable to resolve exit candle")

        exit_price = target if reason == "TARGET" else stop
        result_r = (exit_price - fill) / actual_risk

        trades.append({
            "config_id": config["config_id"],
            "signal_time": times[signal_index],
            "exit_time": times[exit_index],
            "signal_index": signal_index,
            "exit_index": exit_index,
            "reference_entry": reference_entry,
            "historical_fill": fill,
            "stop": stop,
            "target": target,
            "rr": rr,
            "cost_pips": BASE_COST_PIPS * cost_multiplier,
            "exit_reason": reason,
            "result_r": result_r,
            "duration_bars": exit_index - signal_index,
        })

        # Pyramiding 0; signal on exact exit candle remains eligible.
        pointer = bisect_left(
            signal_indices,
            exit_index,
            lo=pointer + 1,
        )

    return trades


# ============================================================
# METRICS
# ============================================================

def metrics(trades):
    trades = list(trades)

    if not trades:
        return {
            "trades": 0,
            "winners": 0,
            "losers": 0,
            "win_rate_pct": 0.0,
            "profit_factor": 0.0,
            "total_r": 0.0,
            "expectancy_r": 0.0,
            "max_drawdown_r": 0.0,
            "longest_losing_streak": 0,
            "median_duration_bars": 0.0,
        }

    values = [float(t["result_r"]) for t in trades]

    winners = sum(value > 0 for value in values)
    losers = len(values) - winners

    gross_profit = sum(value for value in values if value > 0)
    gross_loss = -sum(value for value in values if value < 0)

    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0
    streak = 0
    max_streak = 0

    for value in values:
        cumulative += value
        peak = max(peak, cumulative)
        max_dd = min(max_dd, cumulative - peak)

        if value <= 0:
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 0

    return {
        "trades": len(trades),
        "winners": winners,
        "losers": losers,
        "win_rate_pct": 100.0 * winners / len(trades),
        "profit_factor": safe_pf(gross_profit, gross_loss),
        "total_r": sum(values),
        "expectancy_r": sum(values) / len(values),
        "max_drawdown_r": max_dd,
        "longest_losing_streak": max_streak,
        "median_duration_bars": med(t["duration_bars"] for t in trades),
    }


def period_trades(trades, start=None, end=None):
    result = []

    for trade in trades:
        t = trade["signal_time"]

        if start is not None and t < start:
            continue
        if end is not None and t >= end:
            continue

        result.append(trade)

    return result


def period_metrics(trades, start=None, end=None):
    return metrics(period_trades(trades, start, end))


def evaluate(config, features, signals, cost_multiplier=1.0):
    trades = backtest(
        config,
        features,
        signals,
        cost_multiplier=cost_multiplier,
    )

    full = metrics(trades)

    dev = period_metrics(
        trades,
        None,
        VALIDATION_START,
    )

    validation = period_metrics(
        trades,
        VALIDATION_START,
        None,
    )

    eras = [
        (
            datetime(2002, 1, 1, tzinfo=timezone.utc),
            datetime(2008, 1, 1, tzinfo=timezone.utc),
        ),
        (
            datetime(2008, 1, 1, tzinfo=timezone.utc),
            datetime(2014, 1, 1, tzinfo=timezone.utc),
        ),
        (
            datetime(2014, 1, 1, tzinfo=timezone.utc),
            datetime(2020, 1, 1, tzinfo=timezone.utc),
        ),
        (
            datetime(2020, 1, 1, tzinfo=timezone.utc),
            None,
        ),
    ]

    era_results = [
        period_metrics(trades, start, end)
        for start, end in eras
    ]

    positive_eras = sum(
        result["trades"] > 0 and result["total_r"] > 0
        for result in era_results
    )

    last5 = period_metrics(
        trades,
        NOW - timedelta(days=365.25 * 5),
        None,
    )

    last2 = period_metrics(
        trades,
        NOW - timedelta(days=365.25 * 2),
        None,
    )

    split_positive = (
        dev["trades"] > 0
        and validation["trades"] > 0
        and dev["total_r"] > 0
        and validation["total_r"] > 0
    )

    min_split_pf = min(
        dev["profit_factor"] if dev["trades"] else 0.0,
        validation["profit_factor"] if validation["trades"] else 0.0,
    )

    row = {
        "config_id": config["config_id"],
        "br_min": config["br_min"],
        "body_atr_min": config["body_atr_min"],
        "lookback": config["lookback"],
        "distance_atr_max": config["distance_atr_max"],
        "rr": config["rr"],
        "full_trades": full["trades"],
        "full_winners": full["winners"],
        "full_win_rate_pct": full["win_rate_pct"],
        "full_pf": full["profit_factor"],
        "full_total_r": full["total_r"],
        "full_expectancy_r": full["expectancy_r"],
        "full_max_dd_r": full["max_drawdown_r"],
        "full_longest_losing_streak": full["longest_losing_streak"],
        "dev_2002_2017_trades": dev["trades"],
        "dev_2002_2017_pf": dev["profit_factor"],
        "dev_2002_2017_r": dev["total_r"],
        "validation_2018_plus_trades": validation["trades"],
        "validation_2018_plus_pf": validation["profit_factor"],
        "validation_2018_plus_r": validation["total_r"],
        "both_temporal_splits_positive": split_positive,
        "min_temporal_split_pf": min_split_pf,
        "positive_eras": positive_eras,
        "era_2002_2007_r": era_results[0]["total_r"],
        "era_2008_2013_r": era_results[1]["total_r"],
        "era_2014_2019_r": era_results[2]["total_r"],
        "era_2020_now_r": era_results[3]["total_r"],
        "last5y_trades": last5["trades"],
        "last5y_pf": last5["profit_factor"],
        "last5y_r": last5["total_r"],
        "last2y_trades": last2["trades"],
        "last2y_pf": last2["profit_factor"],
        "last2y_r": last2["total_r"],
    }

    return row, trades


# ============================================================
# ROBUSTNESS-FIRST RANKING
# ============================================================

def geometry_rank_key(row):
    minimum_trades = 50

    enough_trades = row["full_trades"] >= minimum_trades
    split_ok = row["both_temporal_splits_positive"]
    eras_ok = row["positive_eras"] >= 3
    recent_ok = row["last5y_r"] > 0
    pf_ok = row["full_pf"] >= 1.25

    return (
        1 if enough_trades else 0,
        1 if split_ok else 0,
        1 if eras_ok else 0,
        1 if recent_ok else 0,
        1 if pf_ok else 0,
        row["min_temporal_split_pf"],
        row["positive_eras"],
        row["full_pf"],
        row["full_total_r"],
        row["full_trades"],
    )


def final_rank_key(row):
    return (
        1 if row.get("robust_pass") else 0,
        row["min_temporal_split_pf"],
        row.get("cost_2x_pf", 0.0),
        row.get("plateau_split_positive_pct", 0.0),
        row["full_pf"],
        row["full_total_r"],
        row["full_trades"],
    )


# ============================================================
# PERIOD / CALENDAR / ROLLING
# ============================================================

def detailed_period_rows(config, trades):
    periods = [
        ("FULL", None, None),
        ("DEV_2002_2017", None, VALIDATION_START),
        ("VALIDATION_2018_PLUS", VALIDATION_START, None),
        (
            "2002_2007",
            datetime(2002, 1, 1, tzinfo=timezone.utc),
            datetime(2008, 1, 1, tzinfo=timezone.utc),
        ),
        (
            "2008_2013",
            datetime(2008, 1, 1, tzinfo=timezone.utc),
            datetime(2014, 1, 1, tzinfo=timezone.utc),
        ),
        (
            "2014_2019",
            datetime(2014, 1, 1, tzinfo=timezone.utc),
            datetime(2020, 1, 1, tzinfo=timezone.utc),
        ),
        (
            "2020_NOW",
            datetime(2020, 1, 1, tzinfo=timezone.utc),
            None,
        ),
        (
            "LAST_5Y",
            NOW - timedelta(days=365.25 * 5),
            None,
        ),
        (
            "LAST_2Y",
            NOW - timedelta(days=365.25 * 2),
            None,
        ),
    ]

    rows = []

    for name, start, end in periods:
        rows.append({
            "config_id": config["config_id"],
            "period": name,
            **period_metrics(trades, start, end),
        })

    return rows


def calendar_rows(config, trades):
    if not trades:
        return []

    first_year = trades[0]["signal_time"].year
    last_year = trades[-1]["signal_time"].year
    rows = []

    for year in range(first_year, last_year + 1):
        start = datetime(year, 1, 1, tzinfo=timezone.utc)
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)

        rows.append({
            "config_id": config["config_id"],
            "year": year,
            **period_metrics(trades, start, end),
        })

    return rows


def calendar_summary(rows):
    grouped = defaultdict(list)

    for row in rows:
        grouped[row["config_id"]].append(row)

    output = []

    for config_id, group in grouped.items():
        completed = [
            row for row in group
            if row["year"] < NOW.year and row["trades"] > 0
        ]

        values = [row["total_r"] for row in completed]
        positives = [value for value in values if value > 0]

        output.append({
            "config_id": config_id,
            "active_completed_years": len(values),
            "positive_active_years": len(positives),
            "positive_active_years_pct": (
                100.0 * len(positives) / len(values)
                if values else 0.0
            ),
            "median_active_year_r": med(values),
            "worst_active_year_r": min(values) if values else 0.0,
            "best_active_year_r": max(values) if values else 0.0,
        })

    return output


def rolling_rows(config, trades):
    if not trades:
        return []

    first_month = month_floor(trades[0]["signal_time"])
    last_month = month_floor(trades[-1]["signal_time"])

    rows = []

    for months in [12, 24, 36]:
        start = first_month

        while True:
            end = add_months(start, months)

            if end > add_months(last_month, 1):
                break

            rows.append({
                "config_id": config["config_id"],
                "window_months": months,
                "window_start": iso(start),
                "window_end": iso(end),
                **period_metrics(trades, start, end),
            })

            start = add_months(start, 1)

    return rows


def rolling_summary(rows):
    grouped = defaultdict(list)

    for row in rows:
        grouped[
            (
                row["config_id"],
                row["window_months"],
            )
        ].append(row)

    output = []

    for (config_id, months), group in grouped.items():
        active = [row for row in group if row["trades"] > 0]
        values = [row["total_r"] for row in active]
        positives = [value for value in values if value > 0]

        output.append({
            "config_id": config_id,
            "window_months": months,
            "windows": len(group),
            "active_windows": len(active),
            "positive_active_windows": len(positives),
            "positive_active_windows_pct": (
                100.0 * len(positives) / len(active)
                if active else 0.0
            ),
            "median_r_active": med(values),
            "worst_r_active": min(values) if values else 0.0,
            "best_r_active": max(values) if values else 0.0,
        })

    return output


# ============================================================
# LOCAL PARAMETER PLATEAU
# ============================================================

GRID_MAP = {
    "br_min": BR_GRID,
    "body_atr_min": BODY_ATR_GRID,
    "lookback": LOOKBACK_GRID,
    "distance_atr_max": DISTANCE_GRID,
    "rr": RR_GRID,
}


def adjacent_value(a, b, field):
    grid = GRID_MAP[field]

    try:
        ia = grid.index(a)
        ib = grid.index(b)
    except ValueError:
        return False

    return abs(ia - ib) <= 1


def neighbour_of(candidate, other):
    different = 0

    for field in [
        "br_min",
        "body_atr_min",
        "lookback",
        "distance_atr_max",
        "rr",
    ]:
        a = candidate[field]
        b = other[field]

        if a == b:
            continue

        if not adjacent_value(a, b, field):
            return False

        different += 1

    # Local orthogonal neighbourhood: at most one dimension changes by
    # one grid step. Candidate itself is included.
    return different <= 1


def plateau_rows(finalists, stage2_rows):
    output = []

    for finalist in finalists:
        neighbours = [
            row for row in stage2_rows
            if neighbour_of(finalist, row)
        ]

        if not neighbours:
            neighbours = [finalist]

        positive = [
            row for row in neighbours
            if row["full_total_r"] > 0
        ]

        split_positive = [
            row for row in neighbours
            if row["both_temporal_splits_positive"]
        ]

        pfs = [row["full_pf"] for row in neighbours]
        rs = [row["full_total_r"] for row in neighbours]

        output.append({
            "config_id": finalist["config_id"],
            "neighbour_count": len(neighbours),
            "positive_neighbours": len(positive),
            "positive_neighbours_pct": (
                100.0 * len(positive) / len(neighbours)
            ),
            "both_split_positive_neighbours": len(split_positive),
            "both_split_positive_neighbours_pct": (
                100.0 * len(split_positive) / len(neighbours)
            ),
            "median_neighbour_pf": med(pfs),
            "min_neighbour_pf": min(pfs),
            "max_neighbour_pf": max(pfs),
            "median_neighbour_total_r": med(rs),
            "min_neighbour_total_r": min(rs),
            "max_neighbour_total_r": max(rs),
        })

    return output


# ============================================================
# AXIS SUMMARY — SHOW PLATEAUS / BOUNDARY BEHAVIOUR
# ============================================================

def axis_summary(stage1_rows, stage2_rows):
    rows = []

    # Stage 1 geometry axes at fixed 5R.
    for field, values in [
        ("br_min", BR_GRID),
        ("body_atr_min", BODY_ATR_GRID),
        ("lookback", LOOKBACK_GRID),
        ("distance_atr_max", DISTANCE_GRID),
    ]:
        for value in values:
            group = [
                row for row in stage1_rows
                if row[field] == value
            ]

            eligible = [
                row for row in group
                if row["full_trades"] >= 40
            ]

            pool = eligible if eligible else group

            rows.append({
                "stage": "STAGE1_GEOMETRY_AT_5R",
                "axis": field,
                "value": value,
                "configs": len(group),
                "eligible_configs": len(eligible),
                "median_pf": med(row["full_pf"] for row in pool),
                "median_total_r": med(row["full_total_r"] for row in pool),
                "positive_pct": (
                    100.0
                    * sum(row["full_total_r"] > 0 for row in pool)
                    / len(pool)
                    if pool else 0.0
                ),
                "both_split_positive_pct": (
                    100.0
                    * sum(row["both_temporal_splits_positive"] for row in pool)
                    / len(pool)
                    if pool else 0.0
                ),
            })

    # Stage 2 RR axis across shortlisted geometries.
    for rr in RR_GRID:
        group = [
            row for row in stage2_rows
            if row["rr"] == rr
        ]

        rows.append({
            "stage": "STAGE2_RR",
            "axis": "rr",
            "value": rr,
            "configs": len(group),
            "eligible_configs": sum(row["full_trades"] >= 40 for row in group),
            "median_pf": med(row["full_pf"] for row in group),
            "median_total_r": med(row["full_total_r"] for row in group),
            "positive_pct": (
                100.0
                * sum(row["full_total_r"] > 0 for row in group)
                / len(group)
                if group else 0.0
            ),
            "both_split_positive_pct": (
                100.0
                * sum(row["both_temporal_splits_positive"] for row in group)
                / len(group)
                if group else 0.0
            ),
        })

    return rows


# ============================================================
# FINAL ROBUSTNESS
# ============================================================

def add_final_robustness(
    finalist_rows,
    cost_rows,
    calendar_summary_rows,
    rolling_summary_rows,
    plateau_summary_rows,
):
    cost2 = {
        row["config_id"]: row
        for row in cost_rows
        if abs(row["cost_multiplier"] - 2.0) < 1e-9
    }

    calendar_map = {
        row["config_id"]: row
        for row in calendar_summary_rows
    }

    rolling_map = defaultdict(dict)
    for row in rolling_summary_rows:
        rolling_map[row["config_id"]][row["window_months"]] = row

    plateau_map = {
        row["config_id"]: row
        for row in plateau_summary_rows
    }

    output = []

    for original in finalist_rows:
        row = dict(original)

        c2 = cost2.get(row["config_id"], {})
        cal = calendar_map.get(row["config_id"], {})
        r12 = rolling_map[row["config_id"]].get(12, {})
        r24 = rolling_map[row["config_id"]].get(24, {})
        r36 = rolling_map[row["config_id"]].get(36, {})
        plat = plateau_map.get(row["config_id"], {})

        row.update({
            "cost_2x_pf": c2.get("profit_factor", 0.0),
            "cost_2x_total_r": c2.get("total_r", 0.0),
            "positive_calendar_year_pct": cal.get(
                "positive_active_years_pct",
                0.0,
            ),
            "worst_calendar_year_r": cal.get(
                "worst_active_year_r",
                0.0,
            ),
            "rolling12_positive_pct": r12.get(
                "positive_active_windows_pct",
                0.0,
            ),
            "rolling12_worst_r": r12.get(
                "worst_r_active",
                0.0,
            ),
            "rolling24_positive_pct": r24.get(
                "positive_active_windows_pct",
                0.0,
            ),
            "rolling24_worst_r": r24.get(
                "worst_r_active",
                0.0,
            ),
            "rolling36_positive_pct": r36.get(
                "positive_active_windows_pct",
                0.0,
            ),
            "rolling36_worst_r": r36.get(
                "worst_r_active",
                0.0,
            ),
            "plateau_neighbour_count": plat.get(
                "neighbour_count",
                0,
            ),
            "plateau_positive_pct": plat.get(
                "positive_neighbours_pct",
                0.0,
            ),
            "plateau_split_positive_pct": plat.get(
                "both_split_positive_neighbours_pct",
                0.0,
            ),
            "plateau_median_pf": plat.get(
                "median_neighbour_pf",
                0.0,
            ),
        })

        row["robust_pass"] = (
            row["full_trades"] >= 50
            and row["full_pf"] >= 1.40
            and row["both_temporal_splits_positive"]
            and row["min_temporal_split_pf"] >= 1.20
            and row["positive_eras"] >= 3
            and row["last5y_r"] > 0
            and row["last2y_r"] > 0
            and row["cost_2x_pf"] >= 1.25
            and row["cost_2x_total_r"] > 0
            and row["plateau_positive_pct"] >= 80.0
            and row["plateau_split_positive_pct"] >= 60.0
        )

        output.append(row)

    output.sort(key=final_rank_key, reverse=True)
    return output


# ============================================================
# MAIN
# ============================================================

def run_research():
    try:
        STATUS.update({
            "state": "fetching",
            "message": "Fetching full EUR/JPY H1 history",
        })

        candles = fetch_history(REQUESTED_START, NOW)

        if len(candles) < 5000:
            raise RuntimeError(
                f"Unexpectedly small EUR/JPY H1 history: {len(candles)} candles"
            )

        write_csv(
            OUTS["coverage"],
            [{
                "pair": PAIR,
                "timeframe": "H1",
                "candles": len(candles),
                "first_candle_utc": iso(candles[0]["time"]),
                "last_candle_utc": iso(candles[-1]["time"]),
                "base_cost_pips": BASE_COST_PIPS,
                "stop_buffer_ticks": STOP_BUFFER_TICKS,
                "stage1_geometries_expected": (
                    len(BR_GRID)
                    * len(BODY_ATR_GRID)
                    * len(LOOKBACK_GRID)
                    * len(DISTANCE_GRID)
                ),
                "stage2_rr_values": len(RR_GRID),
            }],
        )

        STATUS.update({
            "state": "features",
            "message": "Building EUR/JPY H1 feature cache",
        })

        features = build_features(candles)

        # ----------------------------------------------------
        # STAGE 1 — 1,750 geometries at 5R
        # ----------------------------------------------------
        stage1_rows = []
        signal_cache = {}
        config_cache = {}

        total_stage1 = (
            len(BR_GRID)
            * len(BODY_ATR_GRID)
            * len(LOOKBACK_GRID)
            * len(DISTANCE_GRID)
        )

        completed = 0

        for br in BR_GRID:
            for body_atr in BODY_ATR_GRID:
                for lookback in LOOKBACK_GRID:
                    for distance in DISTANCE_GRID:
                        completed += 1

                        config = make_config(
                            br,
                            body_atr,
                            lookback,
                            distance,
                            STAGE1_RR,
                        )

                        STATUS.update({
                            "state": "stage1_geometry",
                            "message": (
                                f"{completed}/{total_stage1} "
                                f"{config['config_id']}"
                            ),
                        })

                        signals = raw_signal_indices(config, features)
                        row, _ = evaluate(
                            config,
                            features,
                            signals,
                        )

                        stage1_rows.append(row)
                        signal_cache[config["config_id"]] = signals
                        config_cache[config["config_id"]] = config

        stage1_rows.sort(key=geometry_rank_key, reverse=True)
        write_csv(OUTS["stage1"], stage1_rows)

        # Avoid selecting sixty tiny variants from one exact point by
        # keeping a maximum of two per (lookback, body threshold) cell
        # before filling remaining places by global robustness rank.
        selected = []
        selected_ids = set()
        cell_counts = defaultdict(int)

        for row in stage1_rows:
            if len(selected) >= STAGE2_GEOMETRY_LIMIT:
                break

            cell = (
                row["lookback"],
                row["body_atr_min"],
            )

            if cell_counts[cell] >= 2:
                continue

            if row["full_trades"] < 35:
                continue

            selected.append(row)
            selected_ids.add(row["config_id"])
            cell_counts[cell] += 1

        if len(selected) < STAGE2_GEOMETRY_LIMIT:
            for row in stage1_rows:
                if len(selected) >= STAGE2_GEOMETRY_LIMIT:
                    break

                if row["config_id"] in selected_ids:
                    continue

                if row["full_trades"] < 35:
                    continue

                selected.append(row)
                selected_ids.add(row["config_id"])

        write_csv(OUTS["stage1_shortlist"], selected)

        # ----------------------------------------------------
        # STAGE 2 — RR sweep on shortlisted geometry
        # ----------------------------------------------------
        stage2_rows = []
        stage2_configs = {}
        stage2_signals = {}

        total_stage2 = len(selected) * len(RR_GRID)
        completed = 0

        for seed in selected:
            seed_config = config_cache[seed["config_id"]]
            seed_signals = signal_cache[seed["config_id"]]

            for rr in RR_GRID:
                completed += 1

                config = make_config(
                    seed_config["br_min"],
                    seed_config["body_atr_min"],
                    seed_config["lookback"],
                    seed_config["distance_atr_max"],
                    rr,
                )

                STATUS.update({
                    "state": "stage2_rr",
                    "message": (
                        f"{completed}/{total_stage2} "
                        f"{config['config_id']}"
                    ),
                })

                row, _ = evaluate(
                    config,
                    features,
                    seed_signals,
                )

                stage2_rows.append(row)
                stage2_configs[config["config_id"]] = config
                stage2_signals[config["config_id"]] = seed_signals

        # Deduplicate if the same geometry entered selected through any route.
        dedup = {}
        for row in stage2_rows:
            dedup[row["config_id"]] = row
        stage2_rows = list(dedup.values())

        stage2_rows.sort(key=geometry_rank_key, reverse=True)
        write_csv(OUTS["stage2"], stage2_rows)

        # ----------------------------------------------------
        # FINALIST SEED — diversity across lookback branches
        # ----------------------------------------------------
        finalist_seed = []
        finalist_ids = set()

        # Best from short, medium and long structure branches first.
        branches = [
            ("SHORT_8_18", {8, 10, 12, 15, 18}),
            ("MEDIUM_20_30", {20, 25, 30}),
            ("LONG_45_60", {45, 60}),
        ]

        for _, allowed in branches:
            branch_rows = [
                row for row in stage2_rows
                if row["lookback"] in allowed
            ]

            branch_rows.sort(key=geometry_rank_key, reverse=True)

            for row in branch_rows[:5]:
                if row["config_id"] not in finalist_ids:
                    finalist_seed.append(row)
                    finalist_ids.add(row["config_id"])

        # Fill to 20 with global best.
        for row in stage2_rows:
            if len(finalist_seed) >= FINALIST_LIMIT:
                break

            if row["config_id"] in finalist_ids:
                continue

            finalist_seed.append(row)
            finalist_ids.add(row["config_id"])

        # ----------------------------------------------------
        # DETAILED FINALIST ANALYSIS
        # ----------------------------------------------------
        period_rows = []
        cost_rows = []
        calendar = []
        rolling = []
        trade_rows = []

        for number, row in enumerate(finalist_seed, 1):
            config = stage2_configs[row["config_id"]]
            signals = stage2_signals[row["config_id"]]

            STATUS.update({
                "state": "final_analysis",
                "message": (
                    f"{number}/{len(finalist_seed)} "
                    f"{config['config_id']}"
                ),
            })

            trades = backtest(
                config,
                features,
                signals,
                cost_multiplier=1.0,
            )

            period_rows.extend(
                detailed_period_rows(config, trades)
            )

            for multiplier in [0.5, 1.0, 1.5, 2.0]:
                stressed = backtest(
                    config,
                    features,
                    signals,
                    cost_multiplier=multiplier,
                )

                cost_rows.append({
                    "config_id": config["config_id"],
                    "cost_multiplier": multiplier,
                    "cost_pips": BASE_COST_PIPS * multiplier,
                    **metrics(stressed),
                })

            calendar.extend(
                calendar_rows(config, trades)
            )

            rolling.extend(
                rolling_rows(config, trades)
            )

            for trade in trades:
                out = dict(trade)
                out["signal_time"] = iso(out["signal_time"])
                out["exit_time"] = iso(out["exit_time"])
                out.update({
                    "pair": PAIR,
                    "timeframe": "H1",
                    "side": "LONG",
                    "family": "ENGULF_STRUCTURE",
                    "br_min": config["br_min"],
                    "body_atr_min": config["body_atr_min"],
                    "lookback": config["lookback"],
                    "distance_atr_max": config["distance_atr_max"],
                })
                trade_rows.append(out)

        cal_summary = calendar_summary(calendar)
        roll_summary = rolling_summary(rolling)
        plateau = plateau_rows(finalist_seed, stage2_rows)

        final_rows = add_final_robustness(
            finalist_seed,
            cost_rows,
            cal_summary,
            roll_summary,
            plateau,
        )

        write_csv(OUTS["finalists"], final_rows)
        write_csv(OUTS["periods"], period_rows)
        write_csv(OUTS["cost_stress"], cost_rows)
        write_csv(OUTS["calendar"], calendar)
        write_csv(OUTS["calendar_summary"], cal_summary)
        write_csv(OUTS["rolling"], rolling)
        write_csv(OUTS["rolling_summary"], roll_summary)
        write_csv(OUTS["plateau"], plateau)
        write_csv(OUTS["axis_summary"], axis_summary(stage1_rows, stage2_rows))
        write_csv(OUTS["trades"], trade_rows)

        robust = [row for row in final_rows if row["robust_pass"]]

        notes = [
            {
                "topic": "scope",
                "note": (
                    "Dedicated EUR/JPY H1 LONG exact bullish engulfing + "
                    "structure refinement. No session, weekday or HTF filters "
                    "are tested in this runner."
                ),
            },
            {
                "topic": "prior_discovery_reference",
                "note": (
                    "Prior broad discovery highlighted a high-trade branch near "
                    "BR 1.30, body 1.25 ATR, 15-bar structure, 0.15 ATR distance "
                    "and 5R, while a 60-bar branch also showed high PF with fewer "
                    "trades. Both short and long lookback branches are therefore "
                    "included here."
                ),
            },
            {
                "topic": "anti_boundary_design",
                "note": (
                    "Lookback is extended down to 8 and up to 60; body threshold "
                    "extends to 1.60 ATR; RR extends to 7R. A finalist sitting on "
                    "a search boundary should not be frozen automatically."
                ),
            },
            {
                "topic": "temporal_split",
                "note": (
                    "2002-2017 vs 2018+ is a robustness split, not claimed to be "
                    "pristine out-of-sample because all history participates in "
                    "the research process."
                ),
            },
            {
                "topic": "next_step",
                "note": (
                    "Only after a stable raw geometry/RR plateau is identified "
                    "should we test daily/H4 regime, session and weekday filters "
                    "one factor at a time, then freeze and test marginal value "
                    "against the existing 20-strategy portfolio."
                ),
            },
            {
                "topic": "robust_finalists",
                "note": (
                    f"{len(robust)} of {len(final_rows)} detailed finalists "
                    "passed the predeclared robustness screen."
                ),
            },
        ]

        write_csv(OUTS["notes"], notes)

        STATUS.update({
            "state": "packaging",
            "message": "Building EUR/JPY H1 refinement results ZIP",
        })

        pack_results()

        STATUS.update({
            "state": "complete",
            "message": "EUR/JPY H1 LONG refinement complete",
            "pair": PAIR,
            "timeframe": "H1",
            "side": "LONG",
            "candles": len(candles),
            "stage1_geometries": len(stage1_rows),
            "stage1_shortlist": len(selected),
            "stage2_rr_configs": len(stage2_rows),
            "detailed_finalists": len(final_rows),
            "robust_finalists": len(robust),
            "bundle": BUNDLE,
        })

    except Exception as error:
        STATUS.update({
            "state": "error",
            "message": str(error),
        })

        print(
            "EURJPY H1 LONG REFINEMENT ERROR:",
            repr(error),
            flush=True,
        )


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def root():
    return jsonify({
        "service": "EUR/JPY H1 LONG Engulf-Structure Refinement",
        "status": STATUS["state"],
        "instrument": PAIR,
        "timeframe": "H1",
        "side": "LONG",
        "family": "ENGULF_STRUCTURE",
        "orders_supported": False,
        "trading_enabled": False,
        "stage1_geometry_count": (
            len(BR_GRID)
            * len(BODY_ATR_GRID)
            * len(LOOKBACK_GRID)
            * len(DISTANCE_GRID)
        ),
        "rr_grid": RR_GRID,
        "routes": [
            "/eurjpy-h1-long-refinement/status",
            "/eurjpy-h1-long-refinement/results",
        ],
    })


@app.route("/eurjpy-h1-long-refinement/status")
def status():
    return jsonify(STATUS)


@app.route("/eurjpy-h1-long-refinement/results")
def results():
    return download(BUNDLE)


if __name__ == "__main__":
    threading.Thread(
        target=run_research,
        daemon=True,
    ).start()

    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "5000")),
        debug=False,
    )
