import os
import csv
import math
import time
import zipfile
import threading
from bisect import bisect_left, bisect_right
from collections import deque, defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import requests
from flask import Flask, jsonify, send_file

# ============================================================
# AUD/USD H1 LONG — FINAL CONTROLLED CONFIRMATION
# ============================================================
#
# PURPOSE
# -------
# Final signal-level confirmation after:
#   1) broad AUD/USD H1 LONG discovery
#   2) dedicated OUTSIDE_REVERSAL refinement
#
# This runner does NOT reopen broad optimisation.
#
# PREDECLARED BRANCHES
# --------------------
# A) FREQUENCY / CONTROL BRANCH — geometry frozen
#      body >= 0.75 ATR14
#      outside candle
#      lookback = 25
#      distance <= 0.20 ATR14
#      close location >= 0.75
#      RR = 3.25
#
#    Only the already-discovered Asia-Pacific session mechanism is checked:
#      Tokyo 08:00-16:59
#      Sydney 08:00-16:59
#      Tokyo OR Sydney
#      Tokyo AND Sydney
#
# B) QUALITY BRANCH — small fixed neighbourhood only
#      body >= 1.00 ATR14
#      outside candle
#      lookback = 30 / 40
#      distance <= 0.35 / 0.40 ATR14
#      close location >= 0.85
#      RR = 3.00 / 3.25
#
#    Same four predeclared Asia-Pacific session definitions.
#
# Total fixed confirmation candidates = 36.
#
# NO:
#   - new signal families
#   - weekday mining
#   - HTF-filter mining
#   - arbitrary hour-by-hour optimisation
#   - additional geometry dimensions
#   - extra RR search beyond 3.00 / 3.25 on the quality branch
#
# PARITY GUARDS
# -------------
# 1) The original three discovery controls must reproduce through
#    2026-09-16 20:00 UTC.
#
# 2) Five key context candidates from the refinement run must reproduce
#    exactly through 2026-09-17 18:00 UTC.
#
# If parity fails, the study aborts.
#
# FINAL DIAGNOSTICS FOR ALL 36 FIXED CANDIDATES
# ---------------------------------------------
#   - full-history PF / R / DD / streak
#   - 2002-2017 vs 2018+
#   - pre-2010 / 2010+
#   - four broad eras
#   - last 5Y / last 2Y
#   - 0.5x / 1x / 1.5x / 2x adverse-cost stress
#   - rolling 12 / 24 / 36M
#   - calendar-year consistency
#   - Asia-session mechanism comparison
#   - branch-level stability summary
#   - full trade export
#
# HISTORICAL EXECUTION
# --------------------
# OANDA midpoint H1
# ATR14 = Wilder/RMA, SMA seeded
# signal timestamp = H1 candle OPEN
# reference entry = signal CLOSE
# baseline adverse fill = +0.5 pip for LONG
# stop = signal low - 10 ticks
# target based on REFERENCE signal-close risk
# realised R based on adverse fill
# exits begin on NEXT H1 candle
# exact exit-candle re-entry eligible
# pyramiding = 0 per candidate
#
# IMPORTANT
# ---------
# A confirmation pass is NOT an automatic live deployment.
# A surviving signal must still pass the exact frozen 24-strategy
# portfolio-addition test before it can become strategy #25.
#
# READ ONLY. NEVER SENDS ORDERS.
# ============================================================

app = Flask(__name__)

TOKEN = os.getenv("OANDA_TOKEN")
BASE = os.getenv("OANDA_API_URL", "https://api-fxtrade.oanda.com")

PAIR = "AUD_USD"
PAIR_LABEL = "AUD/USD"

REQUESTED_START = datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc)
NOW = datetime.now(timezone.utc).replace(second=0, microsecond=0)
WARMUP_START = REQUESTED_START - timedelta(days=900)

VALIDATION_START = datetime(2018, 1, 1, tzinfo=timezone.utc)
PRE2010_END = datetime(2010, 1, 1, tzinfo=timezone.utc)

DISCOVERY_CONTROL_CUTOFF = datetime(
    2026, 9, 16, 20, 0,
    tzinfo=timezone.utc,
)

REFINEMENT_CONTROL_CUTOFF = datetime(
    2026, 9, 17, 18, 0,
    tzinfo=timezone.utc,
)

TICK = 0.00001
PIP = 0.0001
STOP_BUFFER_TICKS = 10

H1_PRIMARY_COST_PIPS = 0.50
M15_PRIMARY_COST_PIPS = 1.00
COST_MULTIPLIERS = [0.5, 1.0, 1.5, 2.0]

# Time-zone objects used by the Asia-Pacific session confirmation masks.
NY = ZoneInfo("America/New_York")
LONDON = ZoneInfo("Europe/London")
TOKYO = ZoneInfo("Asia/Tokyo")
SYDNEY = ZoneInfo("Australia/Sydney")

# Legacy discovery helper constants retained because the shared research
# framework still contains generic helper functions that reference them.
# They are NOT used to expand the fixed 36-candidate confirmation study.
STAGE1_RR = 3.50
STAGE1_KEEP_PER_FAMILY = 5
FINAL_KEEP_PER_STREAM = 8

FREQUENCY_BODY_ATR = 0.75
FREQUENCY_LOOKBACK = 25
FREQUENCY_DISTANCE_ATR = 0.20
FREQUENCY_CLOSE_LOCATION = 0.75
FREQUENCY_RR = 3.25

QUALITY_BODY_ATR = 1.00
QUALITY_LOOKBACKS = [30, 40]
QUALITY_DISTANCE_ATR = [0.35, 0.40]
QUALITY_CLOSE_LOCATION = 0.85
QUALITY_RRS = [3.00, 3.25]

FINAL_SESSION_IDS = [
    "SESSION_TOKYO_08_17",
    "SESSION_SYDNEY_08_17",
    "SESSION_ASIA_UNION_08_17",
    "SESSION_ASIA_INTERSECTION_08_17",
]

OUTS = {
    "coverage": "audusd_h1_long_final_confirmation_coverage.csv",
    "discovery_parity": "audusd_h1_long_final_confirmation_discovery_parity.csv",
    "refinement_parity": "audusd_h1_long_final_confirmation_refinement_parity.csv",
    "candidates": "audusd_h1_long_final_confirmation_candidates.csv",
    "decision": "audusd_h1_long_final_confirmation_decision_matrix.csv",
    "periods": "audusd_h1_long_final_confirmation_periods.csv",
    "cost_stress": "audusd_h1_long_final_confirmation_cost_stress.csv",
    "rolling": "audusd_h1_long_final_confirmation_rolling.csv",
    "rolling_summary": "audusd_h1_long_final_confirmation_rolling_summary.csv",
    "calendar": "audusd_h1_long_final_confirmation_calendar_years.csv",
    "calendar_summary": "audusd_h1_long_final_confirmation_calendar_summary.csv",
    "session_mechanism": "audusd_h1_long_final_confirmation_session_mechanism.csv",
    "branch_summary": "audusd_h1_long_final_confirmation_branch_summary.csv",
    "trades": "audusd_h1_long_final_confirmation_trades.csv",
    "notes": "audusd_h1_long_final_confirmation_notes.csv",
}

BUNDLE = "AUDUSD_H1_LONG_FINAL_CONFIRMATION_RESULTS.zip"

STATUS = {
    "state": "not_started",
    "message": "Not started",
    "pair": PAIR,
    "timeframe": "H1",
    "side": "LONG",
    "candidate_count_expected": 36,
    "orders_supported": False,
    "trading_enabled": False,
}


# ============================================================
# GENERAL HELPERS
# ============================================================

def iso(dt):
    return (
        dt.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def parse_time(value):
    value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value).astimezone(timezone.utc)


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


def pack():
    with zipfile.ZipFile(BUNDLE, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in OUTS.values():
            if os.path.exists(path):
                archive.write(path, arcname=os.path.basename(path))


def safe_pf(gross_profit, gross_loss):
    if gross_loss > 0:
        return gross_profit / gross_loss
    if gross_profit > 0:
        return 99.0
    return 0.0


def med(values):
    values = list(values)
    return median(values) if values else 0.0


def add_months(dt, months):
    value = dt.year * 12 + dt.month - 1 + months
    return datetime(
        value // 12,
        value % 12 + 1,
        1,
        tzinfo=timezone.utc,
    )


def month_floor(dt):
    return datetime(dt.year, dt.month, 1, tzinfo=timezone.utc)


# ============================================================
# OANDA DATA
# ============================================================

def headers():
    if not TOKEN:
        raise RuntimeError("OANDA_TOKEN is not configured")

    return {
        "Authorization": "Bearer " + TOKEN.strip(),
    }


def fetch_chunk(granularity, start, end):
    params = {
        "price": "M",
        "granularity": granularity,
        "smooth": "false",
        "from": iso(start),
        "to": iso(end),
        "includeFirst": "true",
    }

    response = requests.get(
        f"{BASE}/v3/instruments/{PAIR}/candles",
        headers=headers(),
        params=params,
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


def fetch_history(granularity, start, end, chunk_days):
    cursor = start
    by_time = {}
    chunk_number = 0

    while cursor < end:
        chunk_number += 1
        chunk_end = min(
            cursor + timedelta(days=chunk_days),
            end,
        )

        STATUS.update({
            "state": "fetching",
            "message": (
                f"{granularity} chunk {chunk_number}: "
                f"{iso(cursor)} -> {iso(chunk_end)}"
            ),
        })

        last_error = None

        for attempt in range(1, 4):
            try:
                rows = fetch_chunk(
                    granularity,
                    cursor,
                    chunk_end,
                )
                last_error = None
                break

            except requests.HTTPError as error:
                status_code = (
                    error.response.status_code
                    if error.response is not None
                    else None
                )

                # OANDA can return 400/404 for a period before an
                # instrument has history. Treat that chunk as empty.
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

        for candle in rows:
            by_time[candle["time"]] = candle

        cursor = chunk_end

        # Gentle pacing for a long full-history fetch.
        time.sleep(0.02)

    result = list(by_time.values())
    result.sort(key=lambda item: item["time"])
    return result


# ============================================================
# INDICATORS / FEATURE CACHE
# ============================================================

def atr14_array(candles):
    n = 14
    length = len(candles)

    high = np.array([c["high"] for c in candles], dtype=float)
    low = np.array([c["low"] for c in candles], dtype=float)
    close = np.array([c["close"] for c in candles], dtype=float)

    tr = np.empty(length, dtype=float)
    tr[:] = np.nan

    if length == 0:
        return tr

    tr[0] = high[0] - low[0]

    for i in range(1, length):
        tr[i] = max(
            high[i] - low[i],
            abs(high[i] - close[i - 1]),
            abs(low[i] - close[i - 1]),
        )

    atr = np.empty(length, dtype=float)
    atr[:] = np.nan

    if length < n:
        return atr

    seed = float(np.mean(tr[:n]))
    atr[n - 1] = seed

    for i in range(n, length):
        atr[i] = (
            atr[i - 1] * (n - 1)
            + tr[i]
        ) / n

    return atr


def rolling_previous_extreme(values, lookback, want_max):
    """
    For each index i:
        result[i] = extreme(values[i-lookback:i])
    Current candle is excluded.
    """
    values = np.asarray(values, dtype=float)
    n = len(values)

    result = np.empty(n, dtype=float)
    result[:] = np.nan

    dq = deque()

    for i in range(n):
        # Add prior candle i-1.
        add_index = i - 1

        if add_index >= 0:
            add_value = values[add_index]

            if want_max:
                while dq and values[dq[-1]] <= add_value:
                    dq.pop()
            else:
                while dq and values[dq[-1]] >= add_value:
                    dq.pop()

            dq.append(add_index)

        minimum_index = i - lookback
        while dq and dq[0] < minimum_index:
            dq.popleft()

        if i >= lookback and dq:
            result[i] = values[dq[0]]

    return result


def rolling_previous_mean(values, lookback):
    """
    For each i:
        mean(values[i-lookback:i])
    Current value excluded.
    NaNs make the window unavailable.
    """
    values = np.asarray(values, dtype=float)
    n = len(values)

    result = np.empty(n, dtype=float)
    result[:] = np.nan

    q = deque()
    total = 0.0
    bad = 0

    for i in range(n):
        add_index = i - 1

        if add_index >= 0:
            value = values[add_index]
            q.append(value)

            if math.isfinite(value):
                total += value
            else:
                bad += 1

        if len(q) > lookback:
            old = q.popleft()
            if math.isfinite(old):
                total -= old
            else:
                bad -= 1

        if len(q) == lookback and bad == 0:
            result[i] = total / lookback

    return result


def build_features(candles, timeframe):
    opens = np.array([c["open"] for c in candles], dtype=float)
    highs = np.array([c["high"] for c in candles], dtype=float)
    lows = np.array([c["low"] for c in candles], dtype=float)
    closes = np.array([c["close"] for c in candles], dtype=float)

    times = [c["time"] for c in candles]

    atr = atr14_array(candles)
    atr_mean20_prev = rolling_previous_mean(atr, 20)

    previous_open = np.empty(len(candles), dtype=float)
    previous_high = np.empty(len(candles), dtype=float)
    previous_low = np.empty(len(candles), dtype=float)
    previous_close = np.empty(len(candles), dtype=float)

    previous_open[:] = np.nan
    previous_high[:] = np.nan
    previous_low[:] = np.nan
    previous_close[:] = np.nan

    if len(candles) > 1:
        previous_open[1:] = opens[:-1]
        previous_high[1:] = highs[:-1]
        previous_low[1:] = lows[:-1]
        previous_close[1:] = closes[:-1]

    bullish_body = closes - opens
    bearish_body = opens - closes
    previous_body = np.abs(previous_close - previous_open)

    candle_range = highs - lows

    close_location = np.divide(
        closes - lows,
        candle_range,
        out=np.zeros_like(closes),
        where=candle_range > 0,
    )

    lower_wick = np.minimum(opens, closes) - lows
    upper_wick = highs - np.maximum(opens, closes)

    lower_wick_body = np.divide(
        lower_wick,
        bullish_body,
        out=np.zeros_like(closes),
        where=bullish_body > 0,
    )

    upper_wick_body = np.divide(
        upper_wick,
        bearish_body,
        out=np.zeros_like(closes),
        where=bearish_body > 0,
    )

    bullish_body_atr = np.divide(
        bullish_body,
        atr,
        out=np.zeros_like(closes),
        where=(bullish_body > 0) & np.isfinite(atr) & (atr > 0),
    )

    bearish_body_atr = np.divide(
        bearish_body,
        atr,
        out=np.zeros_like(closes),
        where=(bearish_body > 0) & np.isfinite(atr) & (atr > 0),
    )

    range_atr = np.divide(
        candle_range,
        atr,
        out=np.zeros_like(closes),
        where=np.isfinite(atr) & (atr > 0),
    )

    bull_ratio = np.divide(
        bullish_body,
        previous_body,
        out=np.zeros_like(closes),
        where=(bullish_body > 0) & (previous_body > 0),
    )

    bear_ratio = np.divide(
        bearish_body,
        previous_body,
        out=np.zeros_like(closes),
        where=(bearish_body > 0) & (previous_body > 0),
    )

    exact_bull = (
        (previous_close < previous_open)
        & (closes > opens)
        & (opens <= previous_close)
        & (closes >= previous_open)
    )

    exact_bear = (
        (previous_close > previous_open)
        & (closes < opens)
        & (opens >= previous_close)
        & (closes <= previous_open)
    )

    outside = (
        (highs > previous_high)
        & (lows < previous_low)
    )

    compression = np.divide(
        np.roll(atr, 1),
        atr_mean20_prev,
        out=np.full(len(candles), np.nan, dtype=float),
        where=np.isfinite(atr_mean20_prev) & (atr_mean20_prev > 0),
    )
    if len(compression):
        compression[0] = np.nan

    if timeframe == "M15":
        structure_lbs = [30, 60, 120, 165]
        sweep_lbs = [20, 60, 120]
        outside_lbs = [30, 60, 120]
        breakout_lbs = [5, 10, 20]
    else:
        structure_lbs = [15, 30, 60, 100]
        sweep_lbs = [15, 30, 60]
        outside_lbs = [15, 20, 25, 30, 35, 40, 45, 60]
        breakout_lbs = [5, 10, 20]

    needed_lbs = sorted(
        set(
            structure_lbs
            + sweep_lbs
            + outside_lbs
            + breakout_lbs
        )
    )

    prev_lows = {}
    prev_highs = {}

    for lookback in needed_lbs:
        prev_lows[lookback] = rolling_previous_extreme(
            lows,
            lookback,
            want_max=False,
        )
        prev_highs[lookback] = rolling_previous_extreme(
            highs,
            lookback,
            want_max=True,
        )

    return {
        "timeframe": timeframe,
        "times": times,
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
        "atr": atr,
        "bull_body_atr": bullish_body_atr,
        "bear_body_atr": bearish_body_atr,
        "range_atr": range_atr,
        "close_location": close_location,
        "lower_wick_body": lower_wick_body,
        "upper_wick_body": upper_wick_body,
        "bull_ratio": bull_ratio,
        "bear_ratio": bear_ratio,
        "exact_bull": exact_bull,
        "exact_bear": exact_bear,
        "outside": outside,
        "compression": compression,
        "previous_high": previous_high,
        "previous_low": previous_low,
        "prev_lows": prev_lows,
        "prev_highs": prev_highs,
        "structure_lbs": structure_lbs,
        "sweep_lbs": sweep_lbs,
        "outside_lbs": outside_lbs,
        "breakout_lbs": breakout_lbs,
    }


# ============================================================
# BROAD DISCOVERY CONFIGS
# ============================================================

def config_id(config):
    parts = [
        config["timeframe"],
        config["side"],
        config["family"],
    ]

    ordered = [
        "br_min",
        "body_atr_min",
        "range_atr_min",
        "lookback",
        "distance_atr_max",
        "close_location",
        "wick_body_min",
        "compression_max",
        "breakout_lookback",
        "context_id",
        "rr",
    ]

    for field in ordered:
        value = config.get(field)
        if value is None:
            continue

        if isinstance(value, float):
            rendered = (
                f"{value:.3f}"
                .rstrip("0")
                .rstrip(".")
            )
        else:
            rendered = str(value)

        parts.append(f"{field}={rendered}")

    return "|".join(parts)


def stage1_configs(timeframe, side, features):
    configs = []

    # --------------------------------------------------------
    # 1) EXACT ENGULF + MAJOR STRUCTURE
    # --------------------------------------------------------
    for br_min in [1.00, 1.30, 1.60]:
        for body_atr_min in [0.75, 1.00, 1.25]:
            for lookback in features["structure_lbs"]:
                for distance in [0.05, 0.15, 0.30]:
                    config = {
                        "timeframe": timeframe,
                        "side": side,
                        "family": "ENGULF_STRUCTURE",
                        "br_min": br_min,
                        "body_atr_min": body_atr_min,
                        "lookback": lookback,
                        "distance_atr_max": distance,
                        "rr": STAGE1_RR,
                    }
                    config["config_id"] = config_id(config)
                    configs.append(config)

    # --------------------------------------------------------
    # 2) STRUCTURE SWEEP + DISPLACEMENT THROUGH PREVIOUS BAR
    # --------------------------------------------------------
    for body_atr_min in [0.75, 1.00, 1.25]:
        for lookback in features["sweep_lbs"]:
            for wick_body_min in [0.00, 0.25]:
                config = {
                    "timeframe": timeframe,
                    "side": side,
                    "family": "SWEEP_DISPLACEMENT",
                    "body_atr_min": body_atr_min,
                    "lookback": lookback,
                    "wick_body_min": wick_body_min,
                    "rr": STAGE1_RR,
                }
                config["config_id"] = config_id(config)
                configs.append(config)

    # --------------------------------------------------------
    # 3) FAILED BREAK / RECLAIM
    # LONG:
    #   low below prior structure, close back above structure
    # SHORT:
    #   high above prior structure, close back below structure
    # --------------------------------------------------------
    close_values = (
        [0.60, 0.75, 0.90]
        if side == "LONG"
        else [0.40, 0.25, 0.10]
    )

    for body_atr_min in [0.75, 1.00, 1.25]:
        for lookback in features["sweep_lbs"]:
            for close_location in close_values:
                config = {
                    "timeframe": timeframe,
                    "side": side,
                    "family": "FAILED_BREAK_RECLAIM",
                    "body_atr_min": body_atr_min,
                    "lookback": lookback,
                    "close_location": close_location,
                    "rr": STAGE1_RR,
                }
                config["config_id"] = config_id(config)
                configs.append(config)

    # --------------------------------------------------------
    # 4) OUTSIDE REVERSAL NEAR MAJOR STRUCTURE
    # --------------------------------------------------------
    outside_close_values = (
        [0.65, 0.75, 0.85]
        if side == "LONG"
        else [0.35, 0.25, 0.15]
    )

    for body_atr_min in [0.50, 0.75, 1.00]:
        for lookback in features["outside_lbs"]:
            for distance in [0.10, 0.30]:
                for close_location in outside_close_values:
                    config = {
                        "timeframe": timeframe,
                        "side": side,
                        "family": "OUTSIDE_REVERSAL",
                        "body_atr_min": body_atr_min,
                        "lookback": lookback,
                        "distance_atr_max": distance,
                        "close_location": close_location,
                        "rr": STAGE1_RR,
                    }
                    config["config_id"] = config_id(config)
                    configs.append(config)

    # --------------------------------------------------------
    # 5) VOLATILITY COMPRESSION -> STRUCTURE BREAKOUT
    # --------------------------------------------------------
    for compression_max in [0.65, 0.80, 0.95]:
        for body_atr_min in [0.75, 1.00, 1.25]:
            for range_atr_min in [1.00, 1.50]:
                for breakout_lookback in features["breakout_lbs"]:
                    config = {
                        "timeframe": timeframe,
                        "side": side,
                        "family": "COMPRESSION_BREAKOUT",
                        "compression_max": compression_max,
                        "body_atr_min": body_atr_min,
                        "range_atr_min": range_atr_min,
                        "breakout_lookback": breakout_lookback,
                        "rr": STAGE1_RR,
                    }
                    config["config_id"] = config_id(config)
                    configs.append(config)

    return configs


# ============================================================
# SIGNAL MASKS
# ============================================================

def signal_indices(config, features):
    side = config["side"]
    family = config["family"]

    atr = features["atr"]
    high = features["high"]
    low = features["low"]
    close = features["close"]

    valid = np.isfinite(atr) & (atr > 0)

    if side == "LONG":
        body_atr = features["bull_body_atr"]
        candle_direction = body_atr > 0
        close_location = features["close_location"]
    else:
        body_atr = features["bear_body_atr"]
        candle_direction = body_atr > 0
        close_location = features["close_location"]

    mask = valid & candle_direction

    # --------------------------------------------------------
    # ENGULF_STRUCTURE
    # --------------------------------------------------------
    if family == "ENGULF_STRUCTURE":
        lookback = config["lookback"]

        if side == "LONG":
            structure = features["prev_lows"][lookback]
            distance = np.abs(low - structure) / atr

            mask &= features["exact_bull"]
            mask &= features["bull_ratio"] >= config["br_min"]
            mask &= body_atr >= config["body_atr_min"]
            mask &= np.isfinite(structure)
            mask &= distance <= config["distance_atr_max"]

        else:
            structure = features["prev_highs"][lookback]
            distance = np.abs(high - structure) / atr

            mask &= features["exact_bear"]
            mask &= features["bear_ratio"] >= config["br_min"]
            mask &= body_atr >= config["body_atr_min"]
            mask &= np.isfinite(structure)
            mask &= distance <= config["distance_atr_max"]

    # --------------------------------------------------------
    # SWEEP_DISPLACEMENT
    # --------------------------------------------------------
    elif family == "SWEEP_DISPLACEMENT":
        lookback = config["lookback"]

        if side == "LONG":
            structure = features["prev_lows"][lookback]

            mask &= body_atr >= config["body_atr_min"]
            mask &= np.isfinite(structure)
            mask &= low < structure
            mask &= close > features["previous_high"]
            mask &= (
                features["lower_wick_body"]
                >= config["wick_body_min"]
            )

        else:
            structure = features["prev_highs"][lookback]

            mask &= body_atr >= config["body_atr_min"]
            mask &= np.isfinite(structure)
            mask &= high > structure
            mask &= close < features["previous_low"]
            mask &= (
                features["upper_wick_body"]
                >= config["wick_body_min"]
            )

    # --------------------------------------------------------
    # FAILED_BREAK_RECLAIM
    # --------------------------------------------------------
    elif family == "FAILED_BREAK_RECLAIM":
        lookback = config["lookback"]

        if side == "LONG":
            structure = features["prev_lows"][lookback]

            mask &= body_atr >= config["body_atr_min"]
            mask &= np.isfinite(structure)
            mask &= low < structure
            mask &= close > structure
            mask &= (
                close_location
                >= config["close_location"]
            )

        else:
            structure = features["prev_highs"][lookback]

            mask &= body_atr >= config["body_atr_min"]
            mask &= np.isfinite(structure)
            mask &= high > structure
            mask &= close < structure
            mask &= (
                close_location
                <= config["close_location"]
            )

    # --------------------------------------------------------
    # OUTSIDE_REVERSAL
    # --------------------------------------------------------
    elif family == "OUTSIDE_REVERSAL":
        lookback = config["lookback"]

        mask &= features["outside"]
        mask &= body_atr >= config["body_atr_min"]

        if side == "LONG":
            structure = features["prev_lows"][lookback]
            distance = np.abs(low - structure) / atr

            mask &= np.isfinite(structure)
            mask &= distance <= config["distance_atr_max"]
            mask &= (
                close_location
                >= config["close_location"]
            )

        else:
            structure = features["prev_highs"][lookback]
            distance = np.abs(high - structure) / atr

            mask &= np.isfinite(structure)
            mask &= distance <= config["distance_atr_max"]
            mask &= (
                close_location
                <= config["close_location"]
            )

    # --------------------------------------------------------
    # COMPRESSION_BREAKOUT
    # --------------------------------------------------------
    elif family == "COMPRESSION_BREAKOUT":
        lookback = config["breakout_lookback"]
        compression = features["compression"]

        mask &= np.isfinite(compression)
        mask &= compression <= config["compression_max"]
        mask &= body_atr >= config["body_atr_min"]
        mask &= features["range_atr"] >= config["range_atr_min"]

        if side == "LONG":
            structure = features["prev_highs"][lookback]
            mask &= np.isfinite(structure)
            mask &= close > structure
        else:
            structure = features["prev_lows"][lookback]
            mask &= np.isfinite(structure)
            mask &= close < structure

    else:
        raise ValueError(f"Unknown family: {family}")

    return np.flatnonzero(mask).astype(int)


# ============================================================
# HISTORICAL TRADE SIMULATION
# ============================================================

def cost_pips_for(timeframe, multiplier=1.0):
    base = (
        H1_PRIMARY_COST_PIPS
        if timeframe == "H1"
        else M15_PRIMARY_COST_PIPS
    )
    return base * multiplier


def find_exit_index(
    features,
    entry_index,
    stop,
    target,
    side,
):
    """
    Vectorised block search for the first future candle touching
    either stop or target.
    """
    high = features["high"]
    low = features["low"]
    n = len(high)

    start = entry_index + 1
    block = 2048

    for left in range(start, n, block):
        right = min(left + block, n)

        if side == "LONG":
            touched = (
                (low[left:right] <= stop)
                | (high[left:right] >= target)
            )
        else:
            touched = (
                (high[left:right] >= stop)
                | (low[left:right] <= target)
            )

        hits = np.flatnonzero(touched)

        if len(hits):
            return left + int(hits[0])

    return None


def determine_exit_reason(features, index, stop, target, side):
    o = features["open"][index]
    h = features["high"][index]
    l = features["low"][index]

    if side == "LONG":
        stop_touched = l <= stop
        target_touched = h >= target

        if stop_touched and not target_touched:
            return "STOP"

        if target_touched and not stop_touched:
            return "TARGET"

        if stop_touched and target_touched:
            distance_to_high = abs(h - o)
            distance_to_low = abs(o - l)

            return (
                "TARGET"
                if distance_to_high < distance_to_low
                else "STOP"
            )

    else:
        stop_touched = h >= stop
        target_touched = l <= target

        if stop_touched and not target_touched:
            return "STOP"

        if target_touched and not stop_touched:
            return "TARGET"

        if stop_touched and target_touched:
            distance_to_high = abs(h - o)
            distance_to_low = abs(o - l)

            return (
                "STOP"
                if distance_to_high < distance_to_low
                else "TARGET"
            )

    return None


def backtest(
    config,
    features,
    raw_signal_indices,
    rr=None,
    cost_multiplier=1.0,
):
    timeframe = config["timeframe"]
    side = config["side"]

    if rr is None:
        rr = config["rr"]

    cost_pips = cost_pips_for(
        timeframe,
        multiplier=cost_multiplier,
    )
    adverse_cost = cost_pips * PIP

    high = features["high"]
    low = features["low"]
    close = features["close"]
    times = features["times"]

    trades = []

    pointer = 0
    raw_signal_indices = np.asarray(
        raw_signal_indices,
        dtype=int,
    )

    while pointer < len(raw_signal_indices):
        signal_index = int(raw_signal_indices[pointer])

        reference_entry = float(close[signal_index])

        if side == "LONG":
            stop = (
                float(low[signal_index])
                - STOP_BUFFER_TICKS * TICK
            )
            reference_risk = reference_entry - stop
            target = reference_entry + rr * reference_risk
            fill = reference_entry + adverse_cost
            actual_risk = fill - stop

        else:
            stop = (
                float(high[signal_index])
                + STOP_BUFFER_TICKS * TICK
            )
            reference_risk = stop - reference_entry
            target = reference_entry - rr * reference_risk
            fill = reference_entry - adverse_cost
            actual_risk = stop - fill

        if reference_risk <= 0 or actual_risk <= 0:
            pointer += 1
            continue

        exit_index = find_exit_index(
            features,
            signal_index,
            stop,
            target,
            side,
        )

        if exit_index is None:
            # Still open at end of data. Do not count as closed trade.
            break

        reason = determine_exit_reason(
            features,
            exit_index,
            stop,
            target,
            side,
        )

        if reason is None:
            raise RuntimeError(
                "Exit search found a candle but exit reason was None"
            )

        exit_price = (
            target
            if reason == "TARGET"
            else stop
        )

        if side == "LONG":
            realised_r = (
                exit_price - fill
            ) / actual_risk
        else:
            realised_r = (
                fill - exit_price
            ) / actual_risk

        duration_bars = exit_index - signal_index

        trades.append({
            "signal_index": signal_index,
            "exit_index": exit_index,
            "signal_time": times[signal_index],
            "exit_time": times[exit_index],
            "side": side,
            "timeframe": timeframe,
            "family": config["family"],
            "config_id": config["config_id"],
            "rr": rr,
            "cost_pips": cost_pips,
            "reference_entry": reference_entry,
            "historical_fill": fill,
            "stop": stop,
            "target": target,
            "exit_reason": reason,
            "result_r": realised_r,
            "duration_bars": duration_bars,
        })

        # Pyramiding=0 with exact exit-candle signal eligibility:
        # next signal may be on exit_index itself.
        pointer = bisect_left(
            raw_signal_indices,
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

    winners = sum(1 for value in values if value > 0)
    losers = sum(1 for value in values if value <= 0)

    gross_profit = sum(
        value for value in values
        if value > 0
    )
    gross_loss = -sum(
        value for value in values
        if value < 0
    )

    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0

    current_loss_streak = 0
    longest_loss_streak = 0

    for value in values:
        cumulative += value
        peak = max(peak, cumulative)
        max_dd = min(
            max_dd,
            cumulative - peak,
        )

        if value <= 0:
            current_loss_streak += 1
            longest_loss_streak = max(
                longest_loss_streak,
                current_loss_streak,
            )
        else:
            current_loss_streak = 0

    return {
        "trades": len(trades),
        "winners": winners,
        "losers": losers,
        "win_rate_pct": 100.0 * winners / len(trades),
        "profit_factor": safe_pf(
            gross_profit,
            gross_loss,
        ),
        "total_r": sum(values),
        "expectancy_r": sum(values) / len(values),
        "max_drawdown_r": max_dd,
        "longest_losing_streak": longest_loss_streak,
        "median_duration_bars": med(
            t["duration_bars"]
            for t in trades
        ),
    }


def trades_in_period(trades, start=None, end=None):
    result = []

    for trade in trades:
        signal_time = trade["signal_time"]

        if start is not None and signal_time < start:
            continue

        if end is not None and signal_time >= end:
            continue

        result.append(trade)

    return result


def period_metrics(trades, start=None, end=None):
    return metrics(
        trades_in_period(
            trades,
            start,
            end,
        )
    )


def evaluate_candidate(config, features, raw_indices):
    trades = backtest(
        config,
        features,
        raw_indices,
        rr=config["rr"],
        cost_multiplier=1.0,
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
    pre2010 = period_metrics(
        trades,
        None,
        PRE2010_END,
    )
    post2010 = period_metrics(
        trades,
        PRE2010_END,
        None,
    )

    era_ranges = [
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
            "2020_now",
            datetime(2020, 1, 1, tzinfo=timezone.utc),
            None,
        ),
    ]

    era_metrics = []
    positive_eras = 0

    for _, start, end in era_ranges:
        result = period_metrics(
            trades,
            start,
            end,
        )
        era_metrics.append(result)

        if (
            result["trades"] > 0
            and result["total_r"] > 0
        ):
            positive_eras += 1

    last5_start = NOW - timedelta(days=365.25 * 5)
    last2_start = NOW - timedelta(days=365.25 * 2)

    last5 = period_metrics(
        trades,
        last5_start,
        None,
    )
    last2 = period_metrics(
        trades,
        last2_start,
        None,
    )

    min_split_pf = min(
        dev["profit_factor"]
        if dev["trades"] else 0.0,
        validation["profit_factor"]
        if validation["trades"] else 0.0,
    )

    both_split_positive = (
        dev["trades"] > 0
        and validation["trades"] > 0
        and dev["total_r"] > 0
        and validation["total_r"] > 0
    )

    min_trade_requirement = (
        40
        if config["timeframe"] == "H1"
        else 50
    )

    promising_pass = (
        full["trades"] >= min_trade_requirement
        and full["profit_factor"] >= 1.20
        and both_split_positive
        and positive_eras >= 3
    )

    row = {
        "config_id": config["config_id"],
        "pair": PAIR,
        "timeframe": config["timeframe"],
        "side": config["side"],
        "family": config["family"],
        "context_id": config.get("context_id", "NONE"),
        "rr": config["rr"],
        "br_min": config.get("br_min"),
        "body_atr_min": config.get("body_atr_min"),
        "range_atr_min": config.get("range_atr_min"),
        "lookback": config.get("lookback"),
        "distance_atr_max": config.get("distance_atr_max"),
        "close_location": config.get("close_location"),
        "wick_body_min": config.get("wick_body_min"),
        "compression_max": config.get("compression_max"),
        "breakout_lookback": config.get("breakout_lookback"),
        "full_trades": full["trades"],
        "full_winners": full["winners"],
        "full_win_rate_pct": full["win_rate_pct"],
        "full_pf": full["profit_factor"],
        "full_total_r": full["total_r"],
        "full_expectancy_r": full["expectancy_r"],
        "full_max_dd_r": full["max_drawdown_r"],
        "full_longest_loss_streak": full["longest_losing_streak"],
        "dev_2002_2017_trades": dev["trades"],
        "dev_2002_2017_pf": dev["profit_factor"],
        "dev_2002_2017_r": dev["total_r"],
        "validation_2018_plus_trades": validation["trades"],
        "validation_2018_plus_pf": validation["profit_factor"],
        "validation_2018_plus_r": validation["total_r"],
        "pre2010_trades": pre2010["trades"],
        "pre2010_pf": pre2010["profit_factor"],
        "pre2010_r": pre2010["total_r"],
        "post2010_trades": post2010["trades"],
        "post2010_pf": post2010["profit_factor"],
        "post2010_r": post2010["total_r"],
        "positive_eras": positive_eras,
        "era_2002_2007_r": era_metrics[0]["total_r"],
        "era_2008_2013_r": era_metrics[1]["total_r"],
        "era_2014_2019_r": era_metrics[2]["total_r"],
        "era_2020_now_r": era_metrics[3]["total_r"],
        "last5y_trades": last5["trades"],
        "last5y_pf": last5["profit_factor"],
        "last5y_r": last5["total_r"],
        "last2y_trades": last2["trades"],
        "last2y_pf": last2["profit_factor"],
        "last2y_r": last2["total_r"],
        "both_temporal_splits_positive": both_split_positive,
        "min_temporal_split_pf": min_split_pf,
        "promising_pass": promising_pass,
    }

    return row, trades


def candidate_sort_key(row):
    # Robustness-first, not "highest full-history PF wins".
    return (
        1 if row["promising_pass"] else 0,
        1 if row["both_temporal_splits_positive"] else 0,
        row["positive_eras"],
        row["min_temporal_split_pf"],
        row["full_pf"],
        row["full_total_r"],
        row["full_trades"],
    )


# ============================================================
# STAGE 1 SHORTLIST
# ============================================================

def shortlist_stage1(rows):
    grouped = defaultdict(list)

    for row in rows:
        grouped[
            (
                row["timeframe"],
                row["side"],
                row["family"],
            )
        ].append(row)

    selected = []

    for _, family_rows in grouped.items():
        family_rows.sort(
            key=candidate_sort_key,
            reverse=True,
        )

        timeframe = family_rows[0]["timeframe"]
        minimum_trades = (
            30
            if timeframe == "H1"
            else 40
        )

        eligible = [
            row for row in family_rows
            if row["full_trades"] >= minimum_trades
        ]

        pool = eligible if eligible else family_rows

        selected.extend(
            pool[:STAGE1_KEEP_PER_FAMILY]
        )

    return selected


# ============================================================
# STAGE 2 RR SWEEP / FINALISTS
# ============================================================

def copy_geometry_from_row(row, rr):
    config = {
        "timeframe": row["timeframe"],
        "side": row["side"],
        "family": row["family"],
        "rr": rr,
    }

    for field in [
        "br_min",
        "body_atr_min",
        "range_atr_min",
        "lookback",
        "distance_atr_max",
        "close_location",
        "wick_body_min",
        "compression_max",
        "breakout_lookback",
    ]:
        value = row.get(field)
        if value not in (None, ""):
            config[field] = value

    config["config_id"] = config_id(config)
    return config


def select_finalists(rows):
    grouped = defaultdict(list)

    for row in rows:
        grouped[
            (
                row["timeframe"],
                row["side"],
            )
        ].append(row)

    finalists = []

    for _, stream_rows in grouped.items():
        stream_rows.sort(
            key=candidate_sort_key,
            reverse=True,
        )

        # First take the strongest candidate from each family so the final
        # output is not eight tiny variants of one archetype.
        by_family = defaultdict(list)
        for row in stream_rows:
            by_family[row["family"]].append(row)

        first_pass = []
        for family_rows in by_family.values():
            family_rows.sort(
                key=candidate_sort_key,
                reverse=True,
            )
            first_pass.append(family_rows[0])

        first_pass.sort(
            key=candidate_sort_key,
            reverse=True,
        )

        selected_ids = set()
        selected = []

        for row in first_pass:
            if len(selected) >= FINAL_KEEP_PER_STREAM:
                break
            selected.append(row)
            selected_ids.add(row["config_id"])

        for row in stream_rows:
            if len(selected) >= FINAL_KEEP_PER_STREAM:
                break
            if row["config_id"] in selected_ids:
                continue
            selected.append(row)
            selected_ids.add(row["config_id"])

        finalists.extend(selected)

    return finalists


# ============================================================
# DETAILED FINALIST ANALYSIS
# ============================================================

def detailed_period_rows(config, trades):
    periods = [
        ("FULL", None, None),
        (
            "DEV_2002_2017",
            None,
            VALIDATION_START,
        ),
        (
            "VALIDATION_2018_PLUS",
            VALIDATION_START,
            None,
        ),
        (
            "PRE_2010",
            None,
            PRE2010_END,
        ),
        (
            "2010_PLUS",
            PRE2010_END,
            None,
        ),
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
        result = period_metrics(
            trades,
            start,
            end,
        )

        rows.append({
            "config_id": config["config_id"],
            "timeframe": config["timeframe"],
            "side": config["side"],
            "family": config["family"],
            "period": name,
            **result,
        })

    return rows


def cost_stress_rows(config, features, raw_indices):
    rows = []

    for multiplier in COST_MULTIPLIERS:
        trades = backtest(
            config,
            features,
            raw_indices,
            rr=config["rr"],
            cost_multiplier=multiplier,
        )

        result = metrics(trades)

        rows.append({
            "config_id": config["config_id"],
            "timeframe": config["timeframe"],
            "side": config["side"],
            "family": config["family"],
            "cost_multiplier": multiplier,
            "cost_pips": cost_pips_for(
                config["timeframe"],
                multiplier,
            ),
            **result,
        })

    return rows


def calendar_rows(config, trades):
    if not trades:
        return []

    first_year = trades[0]["signal_time"].year
    last_year = trades[-1]["signal_time"].year

    rows = []

    for year in range(first_year, last_year + 1):
        start = datetime(
            year,
            1,
            1,
            tzinfo=timezone.utc,
        )
        end = datetime(
            year + 1,
            1,
            1,
            tzinfo=timezone.utc,
        )

        result = period_metrics(
            trades,
            start,
            end,
        )

        rows.append({
            "config_id": config["config_id"],
            "timeframe": config["timeframe"],
            "side": config["side"],
            "family": config["family"],
            "year": year,
            **result,
        })

    return rows


def rolling_rows(config, trades):
    if not trades:
        return []

    first_month = month_floor(
        trades[0]["signal_time"]
    )
    last_month = month_floor(
        trades[-1]["signal_time"]
    )

    rows = []

    for months in [12, 24, 36]:
        window_start = first_month

        while True:
            window_end = add_months(
                window_start,
                months,
            )

            if window_end > add_months(
                last_month,
                1,
            ):
                break

            result = period_metrics(
                trades,
                window_start,
                window_end,
            )

            rows.append({
                "config_id": config["config_id"],
                "timeframe": config["timeframe"],
                "side": config["side"],
                "family": config["family"],
                "window_months": months,
                "window_start": iso(window_start),
                "window_end": iso(window_end),
                **result,
            })

            window_start = add_months(
                window_start,
                1,
            )

    return rows


def rolling_summary(rows):
    grouped = defaultdict(list)

    for row in rows:
        grouped[
            (
                row["config_id"],
                row["timeframe"],
                row["side"],
                row["family"],
                row["window_months"],
            )
        ].append(row)

    output = []

    for key, group in grouped.items():
        active = [
            row for row in group
            if row["trades"] > 0
        ]

        positive = [
            row for row in active
            if row["total_r"] > 0
        ]

        total_rs = [
            row["total_r"]
            for row in active
        ]

        output.append({
            "config_id": key[0],
            "timeframe": key[1],
            "side": key[2],
            "family": key[3],
            "window_months": key[4],
            "windows": len(group),
            "active_windows": len(active),
            "positive_active_windows": len(positive),
            "positive_active_windows_pct": (
                100.0 * len(positive) / len(active)
                if active else 0.0
            ),
            "median_r_active": med(total_rs),
            "worst_r_active": (
                min(total_rs)
                if total_rs else 0.0
            ),
            "best_r_active": (
                max(total_rs)
                if total_rs else 0.0
            ),
        })

    return output


def calendar_summary(rows):
    grouped = defaultdict(list)

    for row in rows:
        grouped[
            (
                row["config_id"],
                row["timeframe"],
                row["side"],
                row["family"],
            )
        ].append(row)

    output = []

    for key, group in grouped.items():
        completed = [
            row for row in group
            if row["year"] < NOW.year
        ]

        active = [
            row for row in completed
            if row["trades"] > 0
        ]

        positive = [
            row for row in active
            if row["total_r"] > 0
        ]

        values = [
            row["total_r"]
            for row in active
        ]

        output.append({
            "config_id": key[0],
            "timeframe": key[1],
            "side": key[2],
            "family": key[3],
            "completed_years": len(completed),
            "active_completed_years": len(active),
            "positive_active_years": len(positive),
            "positive_active_years_pct": (
                100.0 * len(positive) / len(active)
                if active else 0.0
            ),
            "median_active_year_r": med(values),
            "worst_active_year_r": (
                min(values)
                if values else 0.0
            ),
            "best_active_year_r": (
                max(values)
                if values else 0.0
            ),
        })

    return output


# ============================================================
# PARAMETER PLATEAU
# ============================================================

PARAM_FIELDS = [
    "br_min",
    "body_atr_min",
    "range_atr_min",
    "lookback",
    "distance_atr_max",
    "close_location",
    "wick_body_min",
    "compression_max",
    "breakout_lookback",
]


def geometry_tuple(row):
    return tuple(
        row.get(field)
        for field in PARAM_FIELDS
    )


def difference_count(a, b):
    count = 0

    for field in PARAM_FIELDS:
        av = a.get(field)
        bv = b.get(field)

        if av != bv:
            count += 1

    return count


def plateau_rows(finalists, stage2_rows):
    rows = []

    by_stream_family = defaultdict(list)

    for row in stage2_rows:
        by_stream_family[
            (
                row["timeframe"],
                row["side"],
                row["family"],
            )
        ].append(row)

    for finalist in finalists:
        pool = by_stream_family[
            (
                finalist["timeframe"],
                finalist["side"],
                finalist["family"],
            )
        ]

        neighbours = [
            row for row in pool
            if (
                abs(row["rr"] - finalist["rr"]) <= 0.500001
                and difference_count(
                    row,
                    finalist,
                ) <= 1
            )
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

        pfs = [
            row["full_pf"]
            for row in neighbours
        ]

        total_rs = [
            row["full_total_r"]
            for row in neighbours
        ]

        rows.append({
            "config_id": finalist["config_id"],
            "timeframe": finalist["timeframe"],
            "side": finalist["side"],
            "family": finalist["family"],
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
            "median_neighbour_total_r": med(total_rs),
            "min_neighbour_total_r": min(total_rs),
            "max_neighbour_total_r": max(total_rs),
        })

    return rows


# ============================================================
# FAMILY SUMMARY
# ============================================================

def family_summary(rows):
    grouped = defaultdict(list)

    for row in rows:
        grouped[
            (
                row["timeframe"],
                row["side"],
                row["family"],
            )
        ].append(row)

    output = []

    for key, group in grouped.items():
        positive = [
            row for row in group
            if row["full_total_r"] > 0
        ]

        promising = [
            row for row in group
            if row["promising_pass"]
        ]

        split_positive = [
            row for row in group
            if row["both_temporal_splits_positive"]
        ]

        sorted_group = sorted(
            group,
            key=candidate_sort_key,
            reverse=True,
        )
        best = sorted_group[0]

        output.append({
            "timeframe": key[0],
            "side": key[1],
            "family": key[2],
            "configs_tested": len(group),
            "positive_full_configs": len(positive),
            "positive_full_pct": (
                100.0 * len(positive) / len(group)
            ),
            "both_split_positive_configs": len(split_positive),
            "both_split_positive_pct": (
                100.0 * len(split_positive) / len(group)
            ),
            "promising_configs": len(promising),
            "promising_pct": (
                100.0 * len(promising) / len(group)
            ),
            "best_config_id": best["config_id"],
            "best_full_trades": best["full_trades"],
            "best_full_pf": best["full_pf"],
            "best_full_total_r": best["full_total_r"],
            "best_min_temporal_split_pf": best["min_temporal_split_pf"],
            "best_validation_2018_plus_r": best[
                "validation_2018_plus_r"
            ],
            "best_last5y_r": best["last5y_r"],
        })

    output.sort(
        key=lambda row: (
            row["timeframe"],
            row["side"],
            -row["promising_pct"],
            -row["best_full_pf"],
        )
    )

    return output


# ============================================================
# ROBUST FINAL PASS
# ============================================================

def add_final_robustness(
    finalist_rows,
    cost_rows,
    rolling_summary_rows,
    calendar_summary_rows,
    plateau_summary_rows,
):
    cost_map = {}
    for row in cost_rows:
        if abs(row["cost_multiplier"] - 2.0) < 1e-9:
            cost_map[row["config_id"]] = row

    rolling_map = defaultdict(dict)
    for row in rolling_summary_rows:
        rolling_map[row["config_id"]][
            row["window_months"]
        ] = row

    calendar_map = {
        row["config_id"]: row
        for row in calendar_summary_rows
    }

    plateau_map = {
        row["config_id"]: row
        for row in plateau_summary_rows
    }

    output = []

    for row in finalist_rows:
        row = dict(row)

        cost2 = cost_map.get(
            row["config_id"],
            {},
        )
        r12 = rolling_map[
            row["config_id"]
        ].get(12, {})
        r24 = rolling_map[
            row["config_id"]
        ].get(24, {})
        r36 = rolling_map[
            row["config_id"]
        ].get(36, {})
        cal = calendar_map.get(
            row["config_id"],
            {},
        )
        plateau = plateau_map.get(
            row["config_id"],
            {},
        )

        row.update({
            "cost_2x_pf": cost2.get("profit_factor", 0.0),
            "cost_2x_total_r": cost2.get("total_r", 0.0),
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
            "positive_calendar_year_pct": cal.get(
                "positive_active_years_pct",
                0.0,
            ),
            "worst_calendar_year_r": cal.get(
                "worst_active_year_r",
                0.0,
            ),
            "plateau_positive_neighbours_pct": plateau.get(
                "positive_neighbours_pct",
                0.0,
            ),
            "plateau_split_positive_neighbours_pct": plateau.get(
                "both_split_positive_neighbours_pct",
                0.0,
            ),
            "plateau_median_pf": plateau.get(
                "median_neighbour_pf",
                0.0,
            ),
        })

        min_trade_requirement = (
            40
            if row["timeframe"] == "H1"
            else 50
        )

        row["robust_pass"] = (
            row["full_trades"] >= min_trade_requirement
            and row["full_pf"] >= 1.35
            and row["both_temporal_splits_positive"]
            and row["positive_eras"] >= 3
            and row["last5y_r"] > 0
            and row["cost_2x_pf"] >= 1.10
            and row["cost_2x_total_r"] > 0
            and row["rolling24_positive_pct"] >= 70.0
            and row["plateau_positive_neighbours_pct"] >= 70.0
        )

        output.append(row)

    output.sort(
        key=lambda row: (
            1 if row["robust_pass"] else 0,
            row["min_temporal_split_pf"],
            row["cost_2x_pf"],
            row["full_pf"],
            row["full_total_r"],
        ),
        reverse=True,
    )

    return output


# ============================================================
# FINAL CONFIRMATION HELPERS
# ============================================================

def outside_config(
    body_atr_min,
    lookback,
    distance_atr_max,
    close_location,
    rr,
    context_id=None,
):
    config = {
        "timeframe": "H1",
        "side": "LONG",
        "family": "OUTSIDE_REVERSAL",
        "body_atr_min": float(body_atr_min),
        "lookback": int(lookback),
        "distance_atr_max": float(distance_atr_max),
        "close_location": float(close_location),
        "rr": float(rr),
    }

    if context_id is not None:
        config["context_id"] = context_id

    config["config_id"] = config_id(config)
    return config


def engulf_control_config():
    config = {
        "timeframe": "H1",
        "side": "LONG",
        "family": "ENGULF_STRUCTURE",
        "br_min": 1.00,
        "body_atr_min": 1.25,
        "lookback": 100,
        "distance_atr_max": 0.15,
        "rr": 4.00,
    }
    config["config_id"] = config_id(config)
    return config


def slice_features(features, end_exclusive):
    output = {}

    for key, value in features.items():
        if isinstance(value, np.ndarray):
            output[key] = value[:end_exclusive].copy()

        elif key == "times":
            output[key] = value[:end_exclusive]

        elif key in ("prev_lows", "prev_highs"):
            output[key] = {
                lookback: array[:end_exclusive].copy()
                for lookback, array in value.items()
            }

        else:
            output[key] = value

    return output


def build_session_cache(signal_times):
    n = len(signal_times)

    ny_weekday = np.empty(
        n,
        dtype=int,
    )

    session_ny = np.zeros(
        n,
        dtype=bool,
    )
    session_london = np.zeros(
        n,
        dtype=bool,
    )
    session_tokyo = np.zeros(
        n,
        dtype=bool,
    )
    session_sydney = np.zeros(
        n,
        dtype=bool,
    )

    for i, timestamp in enumerate(signal_times):
        ny_time = timestamp.astimezone(NY)
        london_time = timestamp.astimezone(LONDON)
        tokyo_time = timestamp.astimezone(TOKYO)
        sydney_time = timestamp.astimezone(SYDNEY)

        ny_weekday[i] = ny_time.weekday()

        session_ny[i] = (
            8 <= ny_time.hour < 17
        )
        session_london[i] = (
            7 <= london_time.hour < 16
        )
        session_tokyo[i] = (
            8 <= tokyo_time.hour < 17
        )
        session_sydney[i] = (
            8 <= sydney_time.hour < 17
        )

    return {
        "NY_WEEKDAY": ny_weekday,
        "SESSION_NY": session_ny,
        "SESSION_LONDON": session_london,
        "SESSION_TOKYO": session_tokyo,
        "SESSION_SYDNEY": session_sydney,
    }


def context_mask(context_id, cache):
    n = len(cache["NY_WEEKDAY"])

    if context_id == "NONE":
        return np.ones(n, dtype=bool)

    if context_id == "SESSION_TOKYO_08_17":
        return cache["SESSION_TOKYO"].copy()

    if context_id == "SESSION_SYDNEY_08_17":
        return cache["SESSION_SYDNEY"].copy()

    if context_id == "SESSION_ASIA_UNION_08_17":
        return (
            cache["SESSION_TOKYO"]
            | cache["SESSION_SYDNEY"]
        )

    if context_id == "SESSION_ASIA_INTERSECTION_08_17":
        return (
            cache["SESSION_TOKYO"]
            & cache["SESSION_SYDNEY"]
        )

    raise ValueError(
        f"Unsupported final-confirmation context: {context_id}"
    )


def apply_context(
    raw_indices,
    context_id,
    context_cache,
):
    raw_indices = np.asarray(
        raw_indices,
        dtype=int,
    )

    if not len(raw_indices):
        return raw_indices

    mask = context_mask(
        context_id,
        context_cache,
    )

    return raw_indices[
        mask[raw_indices]
    ]


def discovery_control_specs():
    return [
        {
            "name": "OUTSIDE_RR3",
            "config": outside_config(
                0.75, 30, 0.30, 0.65, 3.00
            ),
            "expected_trades": 323,
            "expected_pf": 1.3070928181727706,
            "expected_total_r": 68.48169845252784,
        },
        {
            "name": "OUTSIDE_RR3_5",
            "config": outside_config(
                0.75, 30, 0.30, 0.65, 3.50
            ),
            "expected_trades": 316,
            "expected_pf": 1.2727724664850295,
            "expected_total_r": 62.73766729155682,
        },
        {
            "name": "ENGULF_RR4",
            "config": engulf_control_config(),
            "expected_trades": 47,
            "expected_pf": 1.6640192322077896,
            "expected_total_r": 21.91263466285706,
        },
    ]


def refinement_control_specs():
    return [
        {
            "name": "FREQUENCY_TOKYO",
            "config": outside_config(
                0.75,
                25,
                0.20,
                0.75,
                3.25,
                context_id="SESSION_TOKYO_08_17",
            ),
            "expected_trades": 94,
            "expected_pf": 1.8715648326425856,
            "expected_total_r": 51.422325125912565,
        },
        {
            "name": "QUALITY_LB40_D035_TOKYO",
            "config": outside_config(
                1.00,
                40,
                0.35,
                0.85,
                3.25,
                context_id="SESSION_TOKYO_08_17",
            ),
            "expected_trades": 60,
            "expected_pf": 2.256610520811067,
            "expected_total_r": 43.98136822838734,
        },
        {
            "name": "QUALITY_LB40_D035_SYDNEY",
            "config": outside_config(
                1.00,
                40,
                0.35,
                0.85,
                3.25,
                context_id="SESSION_SYDNEY_08_17",
            ),
            "expected_trades": 50,
            "expected_pf": 2.4843918557892115,
            "expected_total_r": 41.56297196209791,
        },
        {
            "name": "QUALITY_LB30_D035_TOKYO",
            "config": outside_config(
                1.00,
                30,
                0.35,
                0.85,
                3.25,
                context_id="SESSION_TOKYO_08_17",
            ),
            "expected_trades": 69,
            "expected_pf": 2.0304739748458496,
            "expected_total_r": 43.27990694352567,
        },
        {
            "name": "QUALITY_LB30_D035_SYDNEY",
            "config": outside_config(
                1.00,
                30,
                0.35,
                0.85,
                3.25,
                context_id="SESSION_SYDNEY_08_17",
            ),
            "expected_trades": 58,
            "expected_pf": 2.2312209022716543,
            "expected_total_r": 41.86151067723624,
        },
    ]


def run_parity_specs(
    specs,
    control_features,
    control_session_cache,
    cutoff,
):
    rows = []

    for spec in specs:
        config = spec["config"]

        raw_indices = signal_indices(
            config,
            control_features,
        )

        context_id = config.get(
            "context_id"
        )

        if context_id:
            raw_indices = apply_context(
                raw_indices,
                context_id,
                control_session_cache,
            )

        trades = backtest(
            config,
            control_features,
            raw_indices,
        )

        result = metrics(trades)

        trades_ok = (
            result["trades"]
            == spec["expected_trades"]
        )

        pf_ok = (
            abs(
                result["profit_factor"]
                - spec["expected_pf"]
            )
            <= 1e-8
        )

        r_ok = (
            abs(
                result["total_r"]
                - spec["expected_total_r"]
            )
            <= 1e-8
        )

        parity_pass = (
            trades_ok
            and pf_ok
            and r_ok
        )

        rows.append({
            "control_name": spec["name"],
            "control_cutoff_utc": iso(cutoff),
            "config_id": config["config_id"],
            "expected_trades": spec["expected_trades"],
            "actual_trades": result["trades"],
            "expected_pf": spec["expected_pf"],
            "actual_pf": result["profit_factor"],
            "expected_total_r": spec["expected_total_r"],
            "actual_total_r": result["total_r"],
            "parity_pass": parity_pass,
        })

        if not parity_pass:
            raise RuntimeError(
                "Parity failure for "
                f"{spec['name']}: "
                f"trades={result['trades']} "
                f"(expected {spec['expected_trades']}), "
                f"PF={result['profit_factor']} "
                f"(expected {spec['expected_pf']}), "
                f"R={result['total_r']} "
                f"(expected {spec['expected_total_r']})"
            )

    return rows


def final_candidate_configs():
    configs = []

    # --------------------------------------------------------
    # A) FREQUENCY / CONTROL BRANCH
    # Geometry and RR are frozen. Only the four already-declared
    # Asia-Pacific session definitions are compared.
    # --------------------------------------------------------
    for session_id in FINAL_SESSION_IDS:
        config = outside_config(
            FREQUENCY_BODY_ATR,
            FREQUENCY_LOOKBACK,
            FREQUENCY_DISTANCE_ATR,
            FREQUENCY_CLOSE_LOCATION,
            FREQUENCY_RR,
            context_id=session_id,
        )
        config["branch_id"] = "FREQUENCY_CONTROL"
        configs.append(config)

    # --------------------------------------------------------
    # B) QUALITY BRANCH
    # Small fixed neighbourhood only.
    # --------------------------------------------------------
    for lookback in QUALITY_LOOKBACKS:
        for distance in QUALITY_DISTANCE_ATR:
            for rr in QUALITY_RRS:
                for session_id in FINAL_SESSION_IDS:
                    config = outside_config(
                        QUALITY_BODY_ATR,
                        lookback,
                        distance,
                        QUALITY_CLOSE_LOCATION,
                        rr,
                        context_id=session_id,
                    )
                    config["branch_id"] = "QUALITY"
                    configs.append(config)

    if len(configs) != 36:
        raise RuntimeError(
            f"Final candidate contract broken: "
            f"{len(configs)} candidates instead of 36"
        )

    return configs


def add_branch_fields(row, config):
    row = dict(row)

    row["branch_id"] = config["branch_id"]
    row["session_id"] = config["context_id"]

    return row


def add_confirmation_diagnostics(
    candidate_rows,
    cost_rows,
    rolling_summary_rows,
    calendar_summary_rows,
):
    cost_2x = {
        row["config_id"]: row
        for row in cost_rows
        if abs(
            float(row["cost_multiplier"])
            - 2.0
        ) <= 1e-9
    }

    rolling_map = defaultdict(dict)

    for row in rolling_summary_rows:
        rolling_map[
            row["config_id"]
        ][int(row["window_months"])] = row

    calendar_map = {
        row["config_id"]: row
        for row in calendar_summary_rows
    }

    output = []

    for source in candidate_rows:
        row = dict(source)

        cost = cost_2x.get(
            row["config_id"],
            {},
        )

        roll24 = rolling_map[
            row["config_id"]
        ].get(24, {})

        roll36 = rolling_map[
            row["config_id"]
        ].get(36, {})

        cal = calendar_map.get(
            row["config_id"],
            {},
        )

        row.update({
            "cost_2x_pf": cost.get(
                "profit_factor",
                0.0,
            ),
            "cost_2x_total_r": cost.get(
                "total_r",
                0.0,
            ),
            "rolling24_positive_pct": roll24.get(
                "positive_active_windows_pct",
                0.0,
            ),
            "rolling24_worst_r": roll24.get(
                "worst_r_active",
                0.0,
            ),
            "rolling36_positive_pct": roll36.get(
                "positive_active_windows_pct",
                0.0,
            ),
            "rolling36_worst_r": roll36.get(
                "worst_r_active",
                0.0,
            ),
            "positive_calendar_year_pct": cal.get(
                "positive_active_years_pct",
                0.0,
            ),
            "worst_calendar_year_r": cal.get(
                "worst_active_year_r",
                0.0,
            ),
        })

        # Predeclared confirmation screen. This is intentionally
        # demanding but NOT an automatic live-strategy lock.
        row["confirmation_pass"] = (
            row["full_trades"] >= 40
            and row["full_pf"] >= 1.50
            and row["both_temporal_splits_positive"]
            and row["min_temporal_split_pf"] >= 1.35
            and row["positive_eras"] == 4
            and row["last5y_r"] > 0
            and row["last2y_r"] > 0
            and row["cost_2x_pf"] >= 1.35
            and row["cost_2x_total_r"] > 0
            and row["rolling24_positive_pct"] >= 75.0
            and row["rolling36_positive_pct"] >= 85.0
            and row["positive_calendar_year_pct"] >= 60.0
        )

        output.append(row)

    output.sort(
        key=lambda row: (
            1 if row["confirmation_pass"] else 0,
            row["min_temporal_split_pf"],
            row["cost_2x_pf"],
            row["full_pf"],
            row["full_total_r"],
        ),
        reverse=True,
    )

    return output


def session_mechanism_rows(decision_rows):
    grouped = defaultdict(list)

    for row in decision_rows:
        key = (
            row["branch_id"],
            row["body_atr_min"],
            int(row["lookback"]),
            row["distance_atr_max"],
            row["close_location"],
            row["rr"],
        )
        grouped[key].append(row)

    output = []

    for key, rows in grouped.items():
        by_session = {
            row["session_id"]: row
            for row in rows
        }

        tokyo = by_session.get(
            "SESSION_TOKYO_08_17"
        )
        sydney = by_session.get(
            "SESSION_SYDNEY_08_17"
        )
        union = by_session.get(
            "SESSION_ASIA_UNION_08_17"
        )
        intersection = by_session.get(
            "SESSION_ASIA_INTERSECTION_08_17"
        )

        if not all(
            item is not None
            for item in (
                tokyo,
                sydney,
                union,
                intersection,
            )
        ):
            continue

        output.append({
            "branch_id": key[0],
            "body_atr_min": key[1],
            "lookback": key[2],
            "distance_atr_max": key[3],
            "close_location": key[4],
            "rr": key[5],

            "tokyo_trades": tokyo["full_trades"],
            "tokyo_pf": tokyo["full_pf"],
            "tokyo_total_r": tokyo["full_total_r"],
            "tokyo_validation_pf": tokyo[
                "validation_2018_plus_pf"
            ],
            "tokyo_last2y_r": tokyo["last2y_r"],

            "sydney_trades": sydney["full_trades"],
            "sydney_pf": sydney["full_pf"],
            "sydney_total_r": sydney["full_total_r"],
            "sydney_validation_pf": sydney[
                "validation_2018_plus_pf"
            ],
            "sydney_last2y_r": sydney["last2y_r"],

            "union_trades": union["full_trades"],
            "union_pf": union["full_pf"],
            "union_total_r": union["full_total_r"],
            "union_validation_pf": union[
                "validation_2018_plus_pf"
            ],
            "union_last2y_r": union["last2y_r"],

            "intersection_trades": intersection[
                "full_trades"
            ],
            "intersection_pf": intersection[
                "full_pf"
            ],
            "intersection_total_r": intersection[
                "full_total_r"
            ],
            "intersection_validation_pf": intersection[
                "validation_2018_plus_pf"
            ],
            "intersection_last2y_r": intersection[
                "last2y_r"
            ],

            "all_four_full_positive": all(
                row["full_total_r"] > 0
                for row in rows
            ),
            "all_four_temporal_splits_positive": all(
                row[
                    "both_temporal_splits_positive"
                ]
                for row in rows
            ),
            "all_four_last5y_positive": all(
                row["last5y_r"] > 0
                for row in rows
            ),
            "all_four_last2y_positive": all(
                row["last2y_r"] > 0
                for row in rows
            ),
            "confirmation_pass_count": sum(
                1
                for row in rows
                if row["confirmation_pass"]
            ),
        })

    return output


def branch_summary_rows(decision_rows):
    output = []

    for branch_id in [
        "FREQUENCY_CONTROL",
        "QUALITY",
    ]:
        rows = [
            row
            for row in decision_rows
            if row["branch_id"] == branch_id
        ]

        if not rows:
            continue

        pfs = [
            row["full_pf"]
            for row in rows
        ]

        split_pfs = [
            row["min_temporal_split_pf"]
            for row in rows
        ]

        cost_pfs = [
            row["cost_2x_pf"]
            for row in rows
        ]

        rolling24 = [
            row["rolling24_positive_pct"]
            for row in rows
        ]

        rolling36 = [
            row["rolling36_positive_pct"]
            for row in rows
        ]

        output.append({
            "branch_id": branch_id,
            "candidates": len(rows),
            "confirmation_passes": sum(
                1
                for row in rows
                if row["confirmation_pass"]
            ),
            "positive_full_history": sum(
                1
                for row in rows
                if row["full_total_r"] > 0
            ),
            "positive_both_temporal_splits": sum(
                1
                for row in rows
                if row[
                    "both_temporal_splits_positive"
                ]
            ),
            "positive_all_four_eras": sum(
                1
                for row in rows
                if row["positive_eras"] == 4
            ),
            "positive_last5y": sum(
                1
                for row in rows
                if row["last5y_r"] > 0
            ),
            "positive_last2y": sum(
                1
                for row in rows
                if row["last2y_r"] > 0
            ),
            "median_pf": float(
                np.median(pfs)
            ),
            "min_pf": min(pfs),
            "max_pf": max(pfs),
            "median_min_temporal_split_pf": float(
                np.median(split_pfs)
            ),
            "min_temporal_split_pf": min(
                split_pfs
            ),
            "median_cost_2x_pf": float(
                np.median(cost_pfs)
            ),
            "min_cost_2x_pf": min(
                cost_pfs
            ),
            "median_rolling24_positive_pct": float(
                np.median(rolling24)
            ),
            "median_rolling36_positive_pct": float(
                np.median(rolling36)
            ),
        })

    return output


# ============================================================
# MAIN RESEARCH
# ============================================================

def run_research():
    try:
        STATUS.update({
            "state": "fetching",
            "message": "Fetching full AUD/USD H1 history",
        })

        h1 = fetch_history(
            "H1",
            REQUESTED_START,
            NOW,
            chunk_days=180,
        )

        if len(h1) < 5000:
            raise RuntimeError(
                f"Unexpectedly small H1 history: {len(h1)}"
            )

        write_csv(
            OUTS["coverage"],
            [{
                "pair": PAIR,
                "timeframe": "H1",
                "candles": len(h1),
                "first_utc": iso(h1[0]["time"]),
                "last_utc": iso(h1[-1]["time"]),
                "baseline_adverse_cost_pips":
                    H1_PRIMARY_COST_PIPS,
                "stop_buffer_ticks":
                    STOP_BUFFER_TICKS,
                "fixed_candidate_count": 36,
            }],
        )

        STATUS.update({
            "state": "features",
            "message": "Building H1 features and Asia-Pacific session masks",
        })

        features = build_features(
            h1,
            "H1",
        )

        session_cache = build_session_cache(
            features["times"]
        )

        # ----------------------------------------------------
        # PARITY 1 — ORIGINAL DISCOVERY CONTROLS
        # ----------------------------------------------------
        discovery_end = bisect_right(
            features["times"],
            DISCOVERY_CONTROL_CUTOFF,
        )

        discovery_features = slice_features(
            features,
            discovery_end,
        )

        discovery_session_cache = (
            build_session_cache(
                discovery_features["times"]
            )
        )

        discovery_parity = run_parity_specs(
            discovery_control_specs(),
            discovery_features,
            discovery_session_cache,
            DISCOVERY_CONTROL_CUTOFF,
        )

        write_csv(
            OUTS["discovery_parity"],
            discovery_parity,
        )

        # ----------------------------------------------------
        # PARITY 2 — KEY CONTEXT RESULTS FROM REFINEMENT
        # ----------------------------------------------------
        refinement_end = bisect_right(
            features["times"],
            REFINEMENT_CONTROL_CUTOFF,
        )

        refinement_features = slice_features(
            features,
            refinement_end,
        )

        refinement_session_cache = (
            build_session_cache(
                refinement_features["times"]
            )
        )

        refinement_parity = run_parity_specs(
            refinement_control_specs(),
            refinement_features,
            refinement_session_cache,
            REFINEMENT_CONTROL_CUTOFF,
        )

        write_csv(
            OUTS["refinement_parity"],
            refinement_parity,
        )

        # ----------------------------------------------------
        # FIXED 36-CANDIDATE CONFIRMATION
        # ----------------------------------------------------
        configs = final_candidate_configs()

        candidate_rows = []
        config_map = {}
        signal_map = {}
        trade_map = {}

        periods = []
        cost_rows = []
        rolling = []
        calendar = []
        exported_trades = []

        for number, config in enumerate(
            configs,
            1,
        ):
            STATUS.update({
                "state": "confirmation",
                "message": (
                    f"{number}/{len(configs)} "
                    f"{config['config_id']}"
                ),
            })

            raw_indices = signal_indices(
                config,
                features,
            )

            filtered_indices = apply_context(
                raw_indices,
                config["context_id"],
                session_cache,
            )

            row, trades = evaluate_candidate(
                config,
                features,
                filtered_indices,
            )

            row = add_branch_fields(
                row,
                config,
            )

            candidate_rows.append(row)
            config_map[
                config["config_id"]
            ] = config
            signal_map[
                config["config_id"]
            ] = filtered_indices
            trade_map[
                config["config_id"]
            ] = trades

            periods.extend(
                detailed_period_rows(
                    config,
                    trades,
                )
            )

            cost_rows.extend(
                cost_stress_rows(
                    config,
                    features,
                    filtered_indices,
                )
            )

            rolling.extend(
                rolling_rows(
                    config,
                    trades,
                )
            )

            calendar.extend(
                calendar_rows(
                    config,
                    trades,
                )
            )

            for trade in trades:
                output = dict(trade)
                output["signal_time"] = iso(
                    output["signal_time"]
                )
                output["exit_time"] = iso(
                    output["exit_time"]
                )
                output["config_id"] = config[
                    "config_id"
                ]
                output["branch_id"] = config[
                    "branch_id"
                ]
                output["session_id"] = config[
                    "context_id"
                ]
                output["body_atr_min"] = config[
                    "body_atr_min"
                ]
                output["lookback"] = config[
                    "lookback"
                ]
                output["distance_atr_max"] = config[
                    "distance_atr_max"
                ]
                output["close_location"] = config[
                    "close_location"
                ]
                output["rr"] = config[
                    "rr"
                ]

                exported_trades.append(
                    output
                )

        rolling_summaries = rolling_summary(
            rolling
        )

        calendar_summaries = calendar_summary(
            calendar
        )

        decision_rows = add_confirmation_diagnostics(
            candidate_rows,
            cost_rows,
            rolling_summaries,
            calendar_summaries,
        )

        # Keep the raw candidate table in deterministic contract order.
        write_csv(
            OUTS["candidates"],
            candidate_rows,
        )

        write_csv(
            OUTS["decision"],
            decision_rows,
        )

        write_csv(
            OUTS["periods"],
            periods,
        )

        write_csv(
            OUTS["cost_stress"],
            cost_rows,
        )

        write_csv(
            OUTS["rolling"],
            rolling,
        )

        write_csv(
            OUTS["rolling_summary"],
            rolling_summaries,
        )

        write_csv(
            OUTS["calendar"],
            calendar,
        )

        write_csv(
            OUTS["calendar_summary"],
            calendar_summaries,
        )

        mechanism_rows = session_mechanism_rows(
            decision_rows
        )

        write_csv(
            OUTS["session_mechanism"],
            mechanism_rows,
        )

        branch_rows = branch_summary_rows(
            decision_rows
        )

        write_csv(
            OUTS["branch_summary"],
            branch_rows,
        )

        write_csv(
            OUTS["trades"],
            exported_trades,
        )

        passes = [
            row
            for row in decision_rows
            if row["confirmation_pass"]
        ]

        notes = [
            {
                "topic": "scope",
                "note": (
                    "Final controlled confirmation only. "
                    "Exactly 36 predeclared OUTSIDE_REVERSAL candidates."
                ),
            },
            {
                "topic": "frequency_branch",
                "note": (
                    "Frequency/control geometry and RR are frozen at "
                    "body0.75/LB25/dist0.20/close0.75/RR3.25. "
                    "Only Tokyo, Sydney, their union and their intersection "
                    "are compared."
                ),
            },
            {
                "topic": "quality_branch",
                "note": (
                    "Quality branch is restricted to body1.00/close0.85, "
                    "LB30 or 40, distance0.35 or 0.40, RR3.00 or 3.25, "
                    "and the same four Asia-Pacific session definitions."
                ),
            },
            {
                "topic": "session_union",
                "note": (
                    "SESSION_ASIA_UNION_08_17 means the H1 signal-open is "
                    "inside Tokyo 08:00-16:59 OR Sydney 08:00-16:59, with "
                    "ZoneInfo DST handling."
                ),
            },
            {
                "topic": "session_intersection",
                "note": (
                    "SESSION_ASIA_INTERSECTION_08_17 means the H1 signal-open "
                    "is simultaneously inside both local session blocks."
                ),
            },
            {
                "topic": "parity",
                "note": (
                    "The run aborts unless all 3 original discovery controls "
                    "and all 5 key refinement controls reproduce exactly at "
                    "their frozen cutoffs."
                ),
            },
            {
                "topic": "screen",
                "note": (
                    "confirmation_pass is a predeclared diagnostic screen, "
                    "not an automatic winner-selection rule or live lock."
                ),
            },
            {
                "topic": "next_step",
                "note": (
                    "If one or more candidates survive broadly, stop signal "
                    "research and run exact portfolio-addition testing against "
                    "the frozen live 24-strategy portfolio before any #25 "
                    "deployment decision."
                ),
            },
            {
                "topic": "passes",
                "note": (
                    f"{len(passes)} of {len(decision_rows)} fixed candidates "
                    "passed the confirmation screen."
                ),
            },
        ]

        write_csv(
            OUTS["notes"],
            notes,
        )

        STATUS.update({
            "state": "packaging",
            "message": "Building final confirmation ZIP",
        })

        pack()

        STATUS.update({
            "state": "complete",
            "message": (
                "AUD/USD H1 LONG final controlled confirmation complete"
            ),
            "h1_candles": len(h1),
            "discovery_parity_passes": len(
                discovery_parity
            ),
            "refinement_parity_passes": len(
                refinement_parity
            ),
            "fixed_candidates": len(
                candidate_rows
            ),
            "confirmation_passes": len(
                passes
            ),
            "session_mechanism_groups": len(
                mechanism_rows
            ),
            "bundle": BUNDLE,
        })

    except Exception as error:
        STATUS.update({
            "state": "error",
            "message": str(error),
        })

        print(
            "AUDUSD H1 LONG FINAL CONFIRMATION ERROR:",
            repr(error),
            flush=True,
        )


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def root():
    return jsonify({
        "service": "AUD/USD H1 LONG Final Controlled Confirmation",
        "status": STATUS["state"],
        "instrument": PAIR,
        "timeframe": "H1",
        "side": "LONG",
        "family": "OUTSIDE_REVERSAL",
        "fixed_candidates": 36,
        "frequency_branch": {
            "body_atr_min": FREQUENCY_BODY_ATR,
            "lookback": FREQUENCY_LOOKBACK,
            "distance_atr_max": FREQUENCY_DISTANCE_ATR,
            "close_location": FREQUENCY_CLOSE_LOCATION,
            "rr": FREQUENCY_RR,
        },
        "quality_branch": {
            "body_atr_min": QUALITY_BODY_ATR,
            "lookbacks": QUALITY_LOOKBACKS,
            "distance_atr_max": QUALITY_DISTANCE_ATR,
            "close_location": QUALITY_CLOSE_LOCATION,
            "rrs": QUALITY_RRS,
        },
        "sessions": FINAL_SESSION_IDS,
        "discovery_control_cutoff_utc": iso(
            DISCOVERY_CONTROL_CUTOFF
        ),
        "refinement_control_cutoff_utc": iso(
            REFINEMENT_CONTROL_CUTOFF
        ),
        "orders_supported": False,
        "trading_enabled": False,
        "routes": [
            "/audusd-h1-long-final/status",
            "/audusd-h1-long-final/results",
        ],
    })


@app.route("/audusd-h1-long-final/status")
def status():
    return jsonify(STATUS)


@app.route("/audusd-h1-long-final/results")
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
