import os
import csv
import math
import time
import zipfile
import threading
from bisect import bisect_left
from collections import deque, defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median
from pathlib import Path

import numpy as np
import requests
from flask import Flask, jsonify, send_file

# ============================================================
# EUR/JPY NEW-PAIR DISCOVERY — H1 + M15, LONG + SHORT
# ============================================================
#
# PURPOSE
# -------
# First-pass discovery on a NEW currency pair.
#
# This is deliberately NOT a fine optimiser and does NOT reuse the
# existing EUR/USD / GBP/USD / USD/JPY / USD/CAD / EUR/GBP parameters.
#
# We scan broad, economically interpretable archetypes:
#
#   1) ENGULF_STRUCTURE
#   2) SWEEP_DISPLACEMENT
#   3) FAILED_BREAK_RECLAIM
#   4) OUTSIDE_REVERSAL
#   5) COMPRESSION_BREAKOUT
#
# Each family is tested:
#   - H1 LONG
#   - H1 SHORT
#   - M15 LONG
#   - M15 SHORT
#
# STAGED PROCESS
# --------------
# Stage 1:
#   broad raw geometry at fixed 3.5R
#   NO session filter
#   NO weekday filter
#   NO H1/H4/D regime filter
#
# Stage 2:
#   only the strongest broad geometries receive an RR sweep
#   RR = 2.5 / 3.0 / 3.5 / 4.0 / 4.5 / 5.0
#
# Final:
#   full-history metrics
#   2002-2017 / 2018+ temporal split
#   pre-2010 / 2010+
#   four broad eras
#   last 5Y / last 2Y
#   0.5x / 1x / 1.5x / 2x adverse-cost stress
#   calendar years
#   rolling 12 / 24 / 36M
#   parameter-neighbourhood / plateau summary
#
# HISTORICAL CONVENTIONS
# ----------------------
# OANDA midpoint
# ATR14 = Wilder/RMA, SMA seeded
# signal timestamp = candle OPEN
# reference entry = signal CLOSE
#
# H1 adverse fill:
#   5 ticks = 0.5 pip on EUR/JPY
#
# M15 adverse fill:
#   1.0 pip
#
# Stop:
#   LONG  = signal low  - 10 ticks
#   SHORT = signal high + 10 ticks
#
# Target:
#   based on REFERENCE signal-close risk
#
# Actual realised R:
#   based on adverse fill
#
# Exit testing:
#   begins on the NEXT candle
#
# Exact exit-candle signal:
#   eligible
#
# Pyramiding:
#   0 PER CANDIDATE STRATEGY
#
# Same-bar tie:
#   LONG:
#       if high is closer to candle open => TARGET first
#       otherwise STOP first
#   SHORT:
#       if high is closer to candle open => STOP first
#       otherwise TARGET first
#
# IMPORTANT
# ---------
# This is a DISCOVERY runner.
# A good candidate from this file is NOT automatically live-worthy.
# The next stage should refine/confirm it, then test its marginal effect
# on the existing locked 20-strategy portfolio.
#
# READ ONLY. NEVER SENDS ORDERS.
# ============================================================

app = Flask(__name__)

TOKEN = os.getenv("OANDA_TOKEN")
BASE = os.getenv("OANDA_API_URL", "https://api-fxtrade.oanda.com")

PAIR = "EUR_JPY"
PAIR_LABEL = "EUR/JPY"

REQUESTED_START = datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc)
NOW = datetime.now(timezone.utc).replace(second=0, microsecond=0)

VALIDATION_START = datetime(2018, 1, 1, tzinfo=timezone.utc)
PRE2010_END = datetime(2010, 1, 1, tzinfo=timezone.utc)

TICK = 0.001
PIP = 0.01
STOP_BUFFER_TICKS = 10

H1_PRIMARY_COST_PIPS = 0.50
M15_PRIMARY_COST_PIPS = 1.00
COST_MULTIPLIERS = [0.5, 1.0, 1.5, 2.0]

STAGE1_RR = 3.50
RR_GRID = [2.50, 3.00, 3.50, 4.00, 4.50, 5.00]

STAGE1_KEEP_PER_FAMILY = 5
FINAL_KEEP_PER_STREAM = 8

OUTS = {
    "coverage": "eurjpy_new_pair_discovery_coverage.csv",
    "stage1": "eurjpy_new_pair_discovery_stage1.csv",
    "stage1_shortlist": "eurjpy_new_pair_discovery_stage1_shortlist.csv",
    "stage2_rr": "eurjpy_new_pair_discovery_stage2_rr.csv",
    "finalists": "eurjpy_new_pair_discovery_finalists.csv",
    "family_summary": "eurjpy_new_pair_discovery_family_summary.csv",
    "periods": "eurjpy_new_pair_discovery_periods.csv",
    "cost_stress": "eurjpy_new_pair_discovery_cost_stress.csv",
    "calendar": "eurjpy_new_pair_discovery_calendar_years.csv",
    "calendar_summary": "eurjpy_new_pair_discovery_calendar_summary.csv",
    "rolling": "eurjpy_new_pair_discovery_rolling.csv",
    "rolling_summary": "eurjpy_new_pair_discovery_rolling_summary.csv",
    "plateau": "eurjpy_new_pair_discovery_plateau.csv",
    "trades": "eurjpy_new_pair_discovery_finalist_trades.csv",
    "notes": "eurjpy_new_pair_discovery_notes.csv",
}

BUNDLE = "EURJPY_NEW_PAIR_H1_M15_DISCOVERY_RESULTS.zip"

STATUS = {
    "state": "not_started",
    "message": "Not started",
    "pair": PAIR,
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
        outside_lbs = [15, 30, 60]
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
# MAIN RESEARCH
# ============================================================

def run_research():
    try:
        STATUS.update({
            "state": "fetching",
            "message": "Fetching full EUR/JPY H1 history",
        })

        h1 = fetch_history(
            "H1",
            REQUESTED_START,
            NOW,
            chunk_days=180,
        )

        STATUS.update({
            "state": "fetching",
            "message": "Fetching full EUR/JPY M15 history",
        })

        m15 = fetch_history(
            "M15",
            REQUESTED_START,
            NOW,
            chunk_days=30,
        )

        if len(h1) < 5000:
            raise RuntimeError(
                f"Unexpectedly small H1 history: {len(h1)} candles"
            )

        if len(m15) < 20000:
            raise RuntimeError(
                f"Unexpectedly small M15 history: {len(m15)} candles"
            )

        coverage_rows = [
            {
                "pair": PAIR,
                "timeframe": "H1",
                "candles": len(h1),
                "first_candle_utc": iso(h1[0]["time"]),
                "last_candle_utc": iso(h1[-1]["time"]),
                "primary_adverse_cost_pips": H1_PRIMARY_COST_PIPS,
                "stop_buffer_ticks": STOP_BUFFER_TICKS,
            },
            {
                "pair": PAIR,
                "timeframe": "M15",
                "candles": len(m15),
                "first_candle_utc": iso(m15[0]["time"]),
                "last_candle_utc": iso(m15[-1]["time"]),
                "primary_adverse_cost_pips": M15_PRIMARY_COST_PIPS,
                "stop_buffer_ticks": STOP_BUFFER_TICKS,
            },
        ]
        write_csv(OUTS["coverage"], coverage_rows)

        STATUS.update({
            "state": "features",
            "message": "Building H1 feature cache",
        })
        h1_features = build_features(h1, "H1")

        STATUS.update({
            "state": "features",
            "message": "Building M15 feature cache",
        })
        m15_features = build_features(m15, "M15")

        feature_map = {
            "H1": h1_features,
            "M15": m15_features,
        }

        # ----------------------------------------------------
        # STAGE 1
        # ----------------------------------------------------
        stage1_rows = []
        config_map = {}
        signal_cache = {}

        streams = [
            ("H1", "LONG"),
            ("H1", "SHORT"),
            ("M15", "LONG"),
            ("M15", "SHORT"),
        ]

        total_configs = 0
        stream_configs = {}

        for timeframe, side in streams:
            configs = stage1_configs(
                timeframe,
                side,
                feature_map[timeframe],
            )
            stream_configs[(timeframe, side)] = configs
            total_configs += len(configs)

        completed = 0

        for timeframe, side in streams:
            features = feature_map[timeframe]

            for config in stream_configs[(timeframe, side)]:
                completed += 1

                STATUS.update({
                    "state": "stage1",
                    "message": (
                        f"{completed}/{total_configs} "
                        f"{config['config_id']}"
                    ),
                })

                raw_indices = signal_indices(
                    config,
                    features,
                )

                signal_cache[
                    config["config_id"]
                ] = raw_indices

                row, _ = evaluate_candidate(
                    config,
                    features,
                    raw_indices,
                )

                stage1_rows.append(row)
                config_map[config["config_id"]] = config

        stage1_rows.sort(
            key=lambda row: (
                row["timeframe"],
                row["side"],
                row["family"],
                -row["full_pf"],
            )
        )

        write_csv(
            OUTS["stage1"],
            stage1_rows,
        )

        stage1_shortlist = shortlist_stage1(
            stage1_rows
        )

        write_csv(
            OUTS["stage1_shortlist"],
            stage1_shortlist,
        )

        # ----------------------------------------------------
        # STAGE 2 — RR SWEEP ONLY
        # ----------------------------------------------------
        stage2_rows = []
        stage2_config_map = {}
        stage2_signal_cache = {}

        total_stage2 = len(stage1_shortlist) * len(RR_GRID)
        stage2_completed = 0

        for base_row in stage1_shortlist:
            base_config = config_map[
                base_row["config_id"]
            ]
            raw_indices = signal_cache[
                base_row["config_id"]
            ]
            features = feature_map[
                base_row["timeframe"]
            ]

            for rr in RR_GRID:
                stage2_completed += 1

                config = dict(base_config)
                config["rr"] = rr
                config["config_id"] = config_id(config)

                STATUS.update({
                    "state": "stage2_rr",
                    "message": (
                        f"{stage2_completed}/{total_stage2} "
                        f"{config['config_id']}"
                    ),
                })

                row, _ = evaluate_candidate(
                    config,
                    features,
                    raw_indices,
                )

                stage2_rows.append(row)
                stage2_config_map[
                    config["config_id"]
                ] = config
                stage2_signal_cache[
                    config["config_id"]
                ] = raw_indices

        stage2_rows.sort(
            key=lambda row: (
                row["timeframe"],
                row["side"],
                row["family"],
                -row["full_pf"],
            )
        )

        write_csv(
            OUTS["stage2_rr"],
            stage2_rows,
        )

        # ----------------------------------------------------
        # FINALIST SELECTION
        # ----------------------------------------------------
        finalist_seed_rows = select_finalists(
            stage2_rows
        )

        periods = []
        costs = []
        calendar = []
        rolling = []
        all_trades = []

        finalist_trade_map = {}

        for number, row in enumerate(
            finalist_seed_rows,
            1,
        ):
            config = stage2_config_map[
                row["config_id"]
            ]
            features = feature_map[
                row["timeframe"]
            ]
            raw_indices = stage2_signal_cache[
                row["config_id"]
            ]

            STATUS.update({
                "state": "final_analysis",
                "message": (
                    f"{number}/{len(finalist_seed_rows)} "
                    f"{row['config_id']}"
                ),
            })

            trades = backtest(
                config,
                features,
                raw_indices,
                rr=config["rr"],
                cost_multiplier=1.0,
            )

            finalist_trade_map[
                row["config_id"]
            ] = trades

            periods.extend(
                detailed_period_rows(
                    config,
                    trades,
                )
            )

            costs.extend(
                cost_stress_rows(
                    config,
                    features,
                    raw_indices,
                )
            )

            cal_rows = calendar_rows(
                config,
                trades,
            )
            calendar.extend(cal_rows)

            roll_rows = rolling_rows(
                config,
                trades,
            )
            rolling.extend(roll_rows)

            for trade in trades:
                output = dict(trade)

                for key in [
                    "signal_time",
                    "exit_time",
                ]:
                    output[key] = iso(output[key])

                output.update({
                    "pair": PAIR,
                    "candidate_timeframe": config["timeframe"],
                    "candidate_side": config["side"],
                    "candidate_family": config["family"],
                })

                all_trades.append(output)

        rolling_summaries = rolling_summary(
            rolling
        )
        calendar_summaries = calendar_summary(
            calendar
        )
        plateau_summaries = plateau_rows(
            finalist_seed_rows,
            stage2_rows,
        )

        final_rows = add_final_robustness(
            finalist_seed_rows,
            costs,
            rolling_summaries,
            calendar_summaries,
            plateau_summaries,
        )

        write_csv(
            OUTS["finalists"],
            final_rows,
        )
        write_csv(
            OUTS["family_summary"],
            family_summary(stage2_rows),
        )
        write_csv(
            OUTS["periods"],
            periods,
        )
        write_csv(
            OUTS["cost_stress"],
            costs,
        )
        write_csv(
            OUTS["calendar"],
            calendar,
        )
        write_csv(
            OUTS["calendar_summary"],
            calendar_summaries,
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
            OUTS["plateau"],
            plateau_summaries,
        )
        write_csv(
            OUTS["trades"],
            all_trades,
        )

        robust = [
            row for row in final_rows
            if row["robust_pass"]
        ]

        notes = [
            {
                "topic": "purpose",
                "note": (
                    "Broad first-pass EUR/JPY discovery on H1 and M15, "
                    "long and short. No candidate is automatically live-worthy."
                ),
            },
            {
                "topic": "stage1",
                "note": (
                    "Stage 1 uses raw price/volatility families only with "
                    "RR fixed at 3.5. No session, weekday or HTF trend filters."
                ),
            },
            {
                "topic": "stage2",
                "note": (
                    "Only shortlisted Stage-1 geometries receive the RR sweep "
                    "2.5/3.0/3.5/4.0/4.5/5.0."
                ),
            },
            {
                "topic": "temporal_split",
                "note": (
                    "2002-2017 and 2018+ are used as temporal robustness "
                    "splits. Because the whole history is visible during "
                    "discovery, 2018+ is not claimed to be pristine OOS."
                ),
            },
            {
                "topic": "costs",
                "note": (
                    "H1 native adverse cost = 0.5 pip (5 ticks); "
                    "M15 native adverse cost = 1.0 pip. Cost stress reaches 2x."
                ),
            },
            {
                "topic": "next_step",
                "note": (
                    "If robust candidates exist, refine only those families "
                    "with broad HTF/session/weekday contexts and local geometry. "
                    "Then test the frozen finalist against the existing "
                    "20-strategy portfolio for marginal diversification."
                ),
            },
            {
                "topic": "robust_finalists",
                "note": (
                    f"{len(robust)} of {len(final_rows)} detailed finalists "
                    "met the predeclared robust_pass screen."
                ),
            },
        ]

        write_csv(
            OUTS["notes"],
            notes,
        )

        STATUS.update({
            "state": "packaging",
            "message": "Building results ZIP",
        })

        pack()

        STATUS.update({
            "state": "complete",
            "message": "EUR/JPY H1+M15 discovery complete",
            "pair": PAIR,
            "h1_candles": len(h1),
            "m15_candles": len(m15),
            "stage1_configs": len(stage1_rows),
            "stage1_shortlist": len(stage1_shortlist),
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
            "EURJPY DISCOVERY ERROR:",
            repr(error),
            flush=True,
        )


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def root():
    return jsonify({
        "service": "EUR/JPY New-Pair H1+M15 Discovery",
        "status": STATUS["state"],
        "instrument": PAIR,
        "timeframes": ["H1", "M15"],
        "sides": ["LONG", "SHORT"],
        "families": [
            "ENGULF_STRUCTURE",
            "SWEEP_DISPLACEMENT",
            "FAILED_BREAK_RECLAIM",
            "OUTSIDE_REVERSAL",
            "COMPRESSION_BREAKOUT",
        ],
        "requested_start_utc": iso(REQUESTED_START),
        "validation_start_utc": iso(VALIDATION_START),
        "h1_primary_cost_pips": H1_PRIMARY_COST_PIPS,
        "m15_primary_cost_pips": M15_PRIMARY_COST_PIPS,
        "orders_supported": False,
        "trading_enabled": False,
        "routes": [
            "/eurjpy-new-pair-discovery/status",
            "/eurjpy-new-pair-discovery/results",
        ],
    })


@app.route("/eurjpy-new-pair-discovery/status")
def status():
    return jsonify(STATUS)


@app.route("/eurjpy-new-pair-discovery/results")
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
