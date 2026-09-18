import os
import csv
import json
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
# AUD/USD H1 SHORT — BROAD DISCOVERY
# ============================================================
#
# PURPOSE
# -------
# First-pass SHORT discovery on AUD/USD H1.
#
# This deliberately starts from scratch. It does NOT mirror the locked
# AUD/USD H1 LONG rules backwards, does NOT import parameters from the
# existing live portfolio, and does NOT assume the winning SHORT family
# in advance.
#
# BROAD FAMILIES
# --------------
#   1) ENGULF_STRUCTURE
#   2) SWEEP_DISPLACEMENT
#   3) FAILED_BREAK_RECLAIM
#   4) OUTSIDE_REVERSAL
#   5) COMPRESSION_BREAKOUT
#
# STAGED PROCESS
# --------------
# Stage 1:
#   broad raw H1-long geometry at fixed 3.5R
#   NO session filter
#   NO weekday filter
#   NO H1/H4/D regime filter
#
# Stage 2:
#   only the strongest Stage-1 geometries receive an RR sweep
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
# OANDA midpoint candles
# ATR14 = Wilder/RMA, SMA seeded
# signal timestamp = H1 candle OPEN
# reference entry = signal CLOSE
#
# H1 baseline adverse fill:
#   0.5 pip = 5 AUD/USD ticks
#
# Stop:
#   signal low - 10 ticks
#
# Target:
#   based on REFERENCE signal-close risk
#
# Actual realised R:
#   based on adverse fill
#
# Exit testing:
#   begins on the NEXT H1 candle
#
# Exact exit-candle signal:
#   eligible
#
# Pyramiding:
#   0 PER CANDIDATE STRATEGY
#
# Same-bar tie:
#   if high is closer to candle open => TARGET first
#   otherwise STOP first
#
# IMPORTANT
# ---------
# This is a DISCOVERY runner, not a live strategy.
# A good candidate must still survive controlled refinement, local/boundary
# confirmation, exact portfolio integration against the frozen 24-strategy
# live portfolio, and a separate implementation review before deployment.
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

VALIDATION_START = datetime(2018, 1, 1, tzinfo=timezone.utc)
PRE2010_END = datetime(2010, 1, 1, tzinfo=timezone.utc)

TICK = 0.00001
PIP = 0.0001
STOP_BUFFER_TICKS = 10

H1_PRIMARY_COST_PIPS = 0.50
M15_PRIMARY_COST_PIPS = 1.00
COST_MULTIPLIERS = [0.5, 1.0, 1.5, 2.0]

STAGE1_RR = 3.50
RR_GRID = [2.50, 3.00, 3.50, 4.00, 4.50, 5.00]

STAGE1_KEEP_PER_FAMILY = 5
FINAL_KEEP_PER_STREAM = 8

OUTS = {
    "coverage": "audusd_h1_short_discovery_coverage.csv",
    "stage1": "audusd_h1_short_discovery_stage1.csv",
    "stage1_shortlist": "audusd_h1_short_discovery_stage1_shortlist.csv",
    "stage2_rr": "audusd_h1_short_discovery_stage2_rr.csv",
    "finalists": "audusd_h1_short_discovery_finalists.csv",
    "family_summary": "audusd_h1_short_discovery_family_summary.csv",
    "periods": "audusd_h1_short_discovery_periods.csv",
    "cost_stress": "audusd_h1_short_discovery_cost_stress.csv",
    "calendar": "audusd_h1_short_discovery_calendar_years.csv",
    "calendar_summary": "audusd_h1_short_discovery_calendar_summary.csv",
    "rolling": "audusd_h1_short_discovery_rolling.csv",
    "rolling_summary": "audusd_h1_short_discovery_rolling_summary.csv",
    "plateau": "audusd_h1_short_discovery_plateau.csv",
    "trades": "audusd_h1_short_discovery_finalist_trades.csv",
    "notes": "audusd_h1_short_discovery_notes.csv",
}

BUNDLE = "AUDUSD_H1_SHORT_DISCOVERY_RESULTS.zip"

STATUS = {
    "state": "not_started",
    "message": "AUD/USD H1 SHORT discovery not started",
    "pair": PAIR,
    "timeframe": "H1",
    "side": "SHORT",
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


def pct(numerator, denominator):
    return (
        100.0 * numerator / denominator
        if denominator
        else 0.0
    )


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
                f"Unexpectedly small H1 history: {len(h1)} candles"
            )

        coverage_rows = [
            {
                "pair": PAIR,
                "timeframe": "H1",
                "side": "SHORT",
                "candles": len(h1),
                "first_candle_utc": iso(h1[0]["time"]),
                "last_candle_utc": iso(h1[-1]["time"]),
                "primary_adverse_cost_pips": H1_PRIMARY_COST_PIPS,
                "stop_buffer_ticks": STOP_BUFFER_TICKS,
            },
        ]
        write_csv(OUTS["coverage"], coverage_rows)

        STATUS.update({
            "state": "features",
            "message": "Building AUD/USD H1 feature cache",
        })

        h1_features = build_features(h1, "H1")

        feature_map = {
            "H1": h1_features,
        }

        # ----------------------------------------------------
        # STAGE 1 — BROAD RAW GEOMETRY, H1 SHORT ONLY
        # ----------------------------------------------------
        stage1_rows = []
        config_map = {}
        signal_cache = {}

        streams = [
            ("H1", "SHORT"),
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
            features = h1_features

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
                row["family"],
                -row["full_pf"],
            )
        )

        write_csv(
            OUTS["stage2_rr"],
            stage2_rows,
        )

        # ----------------------------------------------------
        # FINALIST SELECTION + DEEP DIAGNOSTICS
        # ----------------------------------------------------
        finalist_seed_rows = select_finalists(
            stage2_rows
        )

        periods = []
        costs = []
        calendar = []
        rolling = []
        all_trades = []

        for number, row in enumerate(
            finalist_seed_rows,
            1,
        ):
            config = stage2_config_map[
                row["config_id"]
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
                h1_features,
                raw_indices,
                rr=config["rr"],
                cost_multiplier=1.0,
            )

            periods.extend(
                detailed_period_rows(
                    config,
                    trades,
                )
            )

            costs.extend(
                cost_stress_rows(
                    config,
                    h1_features,
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
                    "candidate_timeframe": "H1",
                    "candidate_side": "SHORT",
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
                    "Broad first-pass AUD/USD H1 SHORT discovery. "
                    "No candidate is automatically live-worthy."
                ),
            },
            {
                "topic": "stage1",
                "note": (
                    "Stage 1 uses five raw price/volatility families only, "
                    "with RR fixed at 3.5. No session, weekday or HTF trend "
                    "filters are allowed in this first pass."
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
                    "H1 native adverse cost = 0.5 pip (5 AUD/USD ticks). "
                    "Cost stress reaches 2x baseline."
                ),
            },
            {
                "topic": "portfolio",
                "note": (
                    "The current live 25-strategy portfolio remains frozen. "
                    "Any AUD/USD H1 SHORT finalist must later be tested as a "
                    "prospective #26 for marginal portfolio benefit under the exact "
                    "live overlap/non-hedging rules. Because AUD/USD H1 LONG is "
                    "already live, the eventual portfolio test must model exact "
                    "same-pair opposite-direction conflicts."
                ),
            },
            {
                "topic": "next_step",
                "note": (
                    "If a broad family survives, refine only that family: "
                    "expand boundary dimensions, test simple causal regime/"
                    "session/weekday contexts, run local plateau/ablation, "
                    "then perform exact portfolio integration."
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
            "message": "AUD/USD H1 SHORT discovery complete",
            "pair": PAIR,
            "timeframe": "H1",
            "side": "SHORT",
            "h1_candles": len(h1),
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
            "AUDUSD H1 SHORT DISCOVERY ERROR:",
            repr(error),
            flush=True,
        )


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def root():
    return jsonify({
        "service": "AUD/USD H1 SHORT Discovery",
        "status": STATUS["state"],
        "instrument": PAIR,
        "timeframes": ["H1"],
        "sides": ["SHORT"],
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
        "tick_size": TICK,
        "pip_size": PIP,
        "stop_buffer_ticks": STOP_BUFFER_TICKS,
        "stage1_rr": STAGE1_RR,
        "rr_grid": RR_GRID,
        "orders_supported": False,
        "trading_enabled": False,
        "routes": [
            "/audusd-h1-short-discovery/status",
            "/audusd-h1-short-discovery/results",
        ],
    })


@app.route("/audusd-h1-short-discovery/status")
def status():
    return jsonify(STATUS)


@app.route("/audusd-h1-short-discovery/results")
def results():
    return download(BUNDLE)



# ============================================================
# AUD/USD H1 SHORT — CONTROLLED REFINEMENT
# ============================================================
#
# PRIMARY BRANCH
# --------------
# COMPRESSION_BREAKOUT only.
#
# The broad discovery showed the strongest FAMILY-WIDE evidence around
# compression_max ~= 0.80. This runner refines only the local neighbourhood:
#
#   compression_max: 0.75 / 0.80 / 0.85
#   body >= ATR:     0.75 / 1.00 / 1.25
#   range >= ATR:    1.00 / 1.25 / 1.50
#   breakout LB:     5 / 10 / 15 / 20
#
# Stage 1 geometry RR is fixed at 4.00.
# Total local geometries = 108.
#
# Stage 2 RR confirmation:
#   3.00 / 3.50 / 4.00 / 4.50 / 5.00
#
# Stage 3:
# single-factor causal contexts ONLY on a small predeclared compression
# shortlist:
#   - weekday exclusions
#   - Sydney / Tokyo / London / New York daytime
#   - strictly completed H4 bearish trend states
#   - strictly completed Daily bearish trend states
#   - strictly completed H4 / Daily ATR regime states
#
# NO context interactions are mined.
#
# SECONDARY FROZEN CONTROLS
# -------------------------
# SWEEP_DISPLACEMENT:
#   body >= 1.25 ATR
#   prior-high LB15
#   upper wick/body >= 0.25
#   RR 3.00 / 3.50 / 4.00
#
# OUTSIDE_REVERSAL benchmark:
#   body >= 1.00 ATR
#   prior-high LB60
#   distance <= 0.30 ATR
#   close location <= 0.15
#   RR 4.50
#
# The secondary controls are NOT further geometry-optimised here.
#
# PARITY
# ------
# Before refinement, exact broad-discovery controls are reproduced through
# 2026-09-18 14:00 UTC.
#
# READ ONLY. NEVER SENDS ORDERS.
# ============================================================

from bisect import bisect_right
from zoneinfo import ZoneInfo

RF_STATUS = {
    "state": "not_started",
    "message": "AUD/USD H1 SHORT controlled refinement not started",
    "pair": PAIR,
    "timeframe": "H1",
    "side": "SHORT",
    "orders_supported": False,
    "trading_enabled": False,
}

RF_BUNDLE = "AUDUSD_H1_SHORT_COMPRESSION_REFINEMENT_RESULTS.zip"

RF_OUT = {
    "coverage": "audusd_h1_short_refinement_coverage.csv",
    "control_parity": "audusd_h1_short_refinement_control_parity.csv",
    "geometry": "audusd_h1_short_refinement_compression_geometry.csv",
    "geometry_summary": "audusd_h1_short_refinement_geometry_summary.csv",
    "geometry_shortlist": "audusd_h1_short_refinement_geometry_shortlist.csv",
    "rr": "audusd_h1_short_refinement_rr_confirmation.csv",
    "context_bases": "audusd_h1_short_refinement_context_bases.csv",
    "contexts": "audusd_h1_short_refinement_context_scan.csv",
    "context_ablation": "audusd_h1_short_refinement_context_ablation.csv",
    "secondary_controls": "audusd_h1_short_refinement_secondary_controls.csv",
    "finalists": "audusd_h1_short_refinement_finalists.csv",
    "periods": "audusd_h1_short_refinement_periods.csv",
    "cost_stress": "audusd_h1_short_refinement_cost_stress.csv",
    "rolling": "audusd_h1_short_refinement_rolling.csv",
    "rolling_summary": "audusd_h1_short_refinement_rolling_summary.csv",
    "calendar": "audusd_h1_short_refinement_calendar.csv",
    "calendar_summary": "audusd_h1_short_refinement_calendar_summary.csv",
    "trades": "audusd_h1_short_refinement_finalist_trades.csv",
    "notes": "audusd_h1_short_refinement_notes.csv",
}

RF_DISCOVERY_CUTOFF = datetime(
    2026, 9, 18, 14, 0,
    tzinfo=timezone.utc,
)

RF_WARMUP_START = datetime(
    1999, 1, 1,
    tzinfo=timezone.utc,
)

RF_GEOMETRY_RR = 4.00
RF_RR_GRID = [3.00, 3.50, 4.00, 4.50, 5.00]

RF_COMPRESSION_GRID = [0.75, 0.80, 0.85]
RF_BODY_GRID = [0.75, 1.00, 1.25]
RF_RANGE_GRID = [1.00, 1.25, 1.50]
RF_BREAKOUT_GRID = [5, 10, 15, 20]

RF_GEOMETRY_SHORTLIST = 24
RF_CONTEXT_BASES = 12
RF_CONTEXT_FINAL_KEEP = 8

RF_NY = ZoneInfo("America/New_York")
RF_LONDON = ZoneInfo("Europe/London")
RF_TOKYO = ZoneInfo("Asia/Tokyo")
RF_SYDNEY = ZoneInfo("Australia/Sydney")


# Exact broad-discovery controls through RF_DISCOVERY_CUTOFF.
RF_PARITY_EXPECTED = {
    "COMPRESSION_CENTRAL_RR4": {
        "trades": 73,
        "pf": 1.4601802837294595,
        "total_r": 24.38955503766136,
    },
    "COMPRESSION_FREQUENCY_RR4": {
        "trades": 110,
        "pf": 1.3780651808779074,
        "total_r": 30.623279651110504,
    },
    "SWEEP_LB15_RR3P5": {
        "trades": 84,
        "pf": 1.3720690499383532,
        "total_r": 22.32414299630119,
    },
    "OUTSIDE_LB60_D030_RR4P5": {
        "trades": 107,
        "pf": 1.4098867450738546,
        "total_r": 33.20082635098223,
    },
}


def rf_pack():
    with zipfile.ZipFile(
        RF_BUNDLE,
        "w",
        zipfile.ZIP_DEFLATED,
    ) as archive:
        for path in RF_OUT.values():
            if os.path.exists(path):
                archive.write(
                    path,
                    arcname=os.path.basename(path),
                )


def rf_config_with_context(config, context_id="NONE"):
    result = dict(config)
    result["context_id"] = context_id
    result["config_id"] = (
        config_id(result)
        + f"|context={context_id}"
    )
    return result


def rf_compression_config(
    body_atr_min,
    range_atr_min,
    compression_max,
    breakout_lookback,
    rr,
    context_id=None,
):
    config = {
        "timeframe": "H1",
        "side": "SHORT",
        "family": "COMPRESSION_BREAKOUT",
        "body_atr_min": float(body_atr_min),
        "range_atr_min": float(range_atr_min),
        "compression_max": float(compression_max),
        "breakout_lookback": int(breakout_lookback),
        "rr": float(rr),
    }
    config["config_id"] = config_id(config)

    if context_id is not None:
        config = rf_config_with_context(
            config,
            context_id,
        )

    return config


def rf_sweep_config(rr):
    config = {
        "timeframe": "H1",
        "side": "SHORT",
        "family": "SWEEP_DISPLACEMENT",
        "body_atr_min": 1.25,
        "lookback": 15,
        "wick_body_min": 0.25,
        "rr": float(rr),
    }
    config["config_id"] = config_id(config)
    return config


def rf_outside_benchmark():
    config = {
        "timeframe": "H1",
        "side": "SHORT",
        "family": "OUTSIDE_REVERSAL",
        "body_atr_min": 1.00,
        "lookback": 60,
        "distance_atr_max": 0.30,
        "close_location": 0.15,
        "rr": 4.50,
    }
    config["config_id"] = config_id(config)
    return config


def rf_geometry_configs():
    configs = []

    for compression_max in RF_COMPRESSION_GRID:
        for body_atr_min in RF_BODY_GRID:
            for range_atr_min in RF_RANGE_GRID:
                for breakout_lookback in RF_BREAKOUT_GRID:
                    configs.append(
                        rf_compression_config(
                            body_atr_min,
                            range_atr_min,
                            compression_max,
                            breakout_lookback,
                            RF_GEOMETRY_RR,
                        )
                    )

    return configs


def rf_refinement_sort_key(row):
    # Robustness first, not highest headline PF.
    return (
        1 if row["both_temporal_splits_positive"] else 0,
        row["positive_eras"],
        1 if row["last5y_r"] > 0 else 0,
        row["min_temporal_split_pf"],
        row["full_pf"],
        row["full_total_r"],
        row["full_trades"],
    )


def rf_select_geometry_shortlist(rows):
    eligible = [
        row for row in rows
        if (
            row["full_trades"] >= 40
            and row["full_pf"] >= 1.15
            and row["both_temporal_splits_positive"]
            and row["positive_eras"] >= 3
        )
    ]

    pool = eligible if eligible else list(rows)
    pool = sorted(
        pool,
        key=rf_refinement_sort_key,
        reverse=True,
    )

    selected = []
    seen = set()

    # Force the two exact discovery compression controls.
    forced = [
        (1.00, 1.50, 0.80, 10),
        (1.00, 1.00, 0.80, 5),
    ]

    for body, rng, comp, lb in forced:
        for row in rows:
            if (
                row["body_atr_min"] == body
                and row["range_atr_min"] == rng
                and row["compression_max"] == comp
                and int(row["breakout_lookback"]) == lb
            ):
                selected.append(row)
                seen.add(row["config_id"])
                break

    for row in pool:
        if len(selected) >= RF_GEOMETRY_SHORTLIST:
            break
        if row["config_id"] in seen:
            continue
        selected.append(row)
        seen.add(row["config_id"])

    return selected


def rf_geometry_summary(rows):
    output = []

    dimensions = [
        ("compression_max", RF_COMPRESSION_GRID),
        ("body_atr_min", RF_BODY_GRID),
        ("range_atr_min", RF_RANGE_GRID),
        ("breakout_lookback", RF_BREAKOUT_GRID),
    ]

    for field, values in dimensions:
        for value in values:
            subset_rows = [
                r for r in rows
                if r[field] == value
            ]

            if not subset_rows:
                continue

            pfs = [
                r["full_pf"]
                for r in subset_rows
            ]

            output.append({
                "dimension": field,
                "value": value,
                "configs": len(subset_rows),
                "positive_full_configs": sum(
                    r["full_total_r"] > 0
                    for r in subset_rows
                ),
                "positive_full_pct": 100.0 * sum(
                    r["full_total_r"] > 0
                    for r in subset_rows
                ) / len(subset_rows),
                "both_split_positive_configs": sum(
                    r["both_temporal_splits_positive"]
                    for r in subset_rows
                ),
                "both_split_positive_pct": 100.0 * sum(
                    r["both_temporal_splits_positive"]
                    for r in subset_rows
                ) / len(subset_rows),
                "last5_positive_configs": sum(
                    r["last5y_r"] > 0
                    for r in subset_rows
                ),
                "last2_positive_configs": sum(
                    r["last2y_r"] > 0
                    for r in subset_rows
                ),
                "median_pf": float(
                    median(pfs)
                ),
                "best_pf": max(pfs),
            })

    return output


def rf_ema(values, length):
    values = np.asarray(
        values,
        dtype=float,
    )

    result = np.full(
        len(values),
        np.nan,
        dtype=float,
    )

    if len(values) < length:
        return result

    seed = values[:length]

    if not np.all(
        np.isfinite(seed)
    ):
        return result

    result[length - 1] = float(
        np.mean(seed)
    )

    alpha = 2.0 / (
        length + 1.0
    )

    for i in range(
        length,
        len(values),
    ):
        result[i] = (
            alpha * values[i]
            + (
                1.0 - alpha
            ) * result[i - 1]
        )

    return result


def rf_prev_mean(values, length):
    values = np.asarray(
        values,
        dtype=float,
    )
    result = np.full(
        len(values),
        np.nan,
        dtype=float,
    )

    for i in range(
        length,
        len(values),
    ):
        window = values[
            i - length:i
        ]
        if np.all(
            np.isfinite(window)
        ):
            result[i] = float(
                np.mean(window)
            )

    return result


def rf_completed_values(
    signal_times,
    higher_times,
    values,
):
    """
    Strictly completed HTF values.

    Higher-timeframe candle i becomes usable only once candle i+1 has begun.
    """
    output = np.full(
        len(signal_times),
        np.nan,
        dtype=float,
    )

    if len(higher_times) < 2:
        return output

    completion_times = higher_times[1:]
    completed_values = np.asarray(
        values[:-1],
        dtype=float,
    )

    for i, signal_time in enumerate(
        signal_times
    ):
        index = (
            bisect_right(
                completion_times,
                signal_time,
            )
            - 1
        )

        if index >= 0:
            output[i] = completed_values[
                index
            ]

    return output


def rf_context_cache(
    h1_features,
    h4_candles,
    daily_candles,
):
    signal_times = h1_features[
        "times"
    ]

    h4_times = [
        c["time"]
        for c in h4_candles
    ]
    d_times = [
        c["time"]
        for c in daily_candles
    ]

    h4_close = np.array(
        [c["close"] for c in h4_candles],
        dtype=float,
    )
    d_close = np.array(
        [c["close"] for c in daily_candles],
        dtype=float,
    )

    h4_ema50 = rf_ema(
        h4_close, 50
    )
    h4_ema100 = rf_ema(
        h4_close, 100
    )
    h4_ema200 = rf_ema(
        h4_close, 200
    )

    d_ema50 = rf_ema(
        d_close, 50
    )
    d_ema100 = rf_ema(
        d_close, 100
    )
    d_ema200 = rf_ema(
        d_close, 200
    )

    h4_atr = atr14_array(
        h4_candles
    )
    d_atr = atr14_array(
        daily_candles
    )

    h4_atr_mean50_prev = rf_prev_mean(
        h4_atr, 50
    )
    d_atr_mean50_prev = rf_prev_mean(
        d_atr, 50
    )

    h4_atr_ratio50 = np.divide(
        h4_atr,
        h4_atr_mean50_prev,
        out=np.full(
            len(h4_atr),
            np.nan,
        ),
        where=(
            np.isfinite(
                h4_atr_mean50_prev
            )
            & (
                h4_atr_mean50_prev
                > 0
            )
        ),
    )

    d_atr_ratio50 = np.divide(
        d_atr,
        d_atr_mean50_prev,
        out=np.full(
            len(d_atr),
            np.nan,
        ),
        where=(
            np.isfinite(
                d_atr_mean50_prev
            )
            & (
                d_atr_mean50_prev
                > 0
            )
        ),
    )

    cache = {
        "H4_CLOSE":
            rf_completed_values(
                signal_times,
                h4_times,
                h4_close,
            ),
        "H4_EMA50":
            rf_completed_values(
                signal_times,
                h4_times,
                h4_ema50,
            ),
        "H4_EMA100":
            rf_completed_values(
                signal_times,
                h4_times,
                h4_ema100,
            ),
        "H4_EMA200":
            rf_completed_values(
                signal_times,
                h4_times,
                h4_ema200,
            ),
        "H4_ATR_RATIO50":
            rf_completed_values(
                signal_times,
                h4_times,
                h4_atr_ratio50,
            ),
        "D_CLOSE":
            rf_completed_values(
                signal_times,
                d_times,
                d_close,
            ),
        "D_EMA50":
            rf_completed_values(
                signal_times,
                d_times,
                d_ema50,
            ),
        "D_EMA100":
            rf_completed_values(
                signal_times,
                d_times,
                d_ema100,
            ),
        "D_EMA200":
            rf_completed_values(
                signal_times,
                d_times,
                d_ema200,
            ),
        "D_ATR_RATIO50":
            rf_completed_values(
                signal_times,
                d_times,
                d_atr_ratio50,
            ),
    }

    n = len(
        signal_times
    )

    cache["NY_WEEKDAY"] = np.empty(
        n,
        dtype=int,
    )

    for key in (
        "SESSION_SYDNEY",
        "SESSION_TOKYO",
        "SESSION_LONDON",
        "SESSION_NY",
    ):
        cache[key] = np.zeros(
            n,
            dtype=bool,
        )

    for i, timestamp in enumerate(
        signal_times
    ):
        ny = timestamp.astimezone(
            RF_NY
        )
        london = timestamp.astimezone(
            RF_LONDON
        )
        tokyo = timestamp.astimezone(
            RF_TOKYO
        )
        sydney = timestamp.astimezone(
            RF_SYDNEY
        )

        cache["NY_WEEKDAY"][i] = (
            ny.weekday()
        )

        cache["SESSION_SYDNEY"][i] = (
            8 <= sydney.hour < 17
        )
        cache["SESSION_TOKYO"][i] = (
            8 <= tokyo.hour < 17
        )
        cache["SESSION_LONDON"][i] = (
            7 <= london.hour < 16
        )
        cache["SESSION_NY"][i] = (
            8 <= ny.hour < 17
        )

    return cache


def rf_context_definitions():
    return [
        (
            "NONE",
            "BASE",
            "No context filter",
        ),

        (
            "EXCLUDE_MON_NY",
            "WEEKDAY",
            "Exclude Monday America/New_York",
        ),
        (
            "EXCLUDE_TUE_NY",
            "WEEKDAY",
            "Exclude Tuesday America/New_York",
        ),
        (
            "EXCLUDE_WED_NY",
            "WEEKDAY",
            "Exclude Wednesday America/New_York",
        ),
        (
            "EXCLUDE_THU_NY",
            "WEEKDAY",
            "Exclude Thursday America/New_York",
        ),
        (
            "EXCLUDE_FRI_NY",
            "WEEKDAY",
            "Exclude Friday America/New_York",
        ),

        (
            "SESSION_SYDNEY_08_17",
            "SESSION",
            "08:00-16:59 Australia/Sydney",
        ),
        (
            "SESSION_TOKYO_08_17",
            "SESSION",
            "08:00-16:59 Asia/Tokyo",
        ),
        (
            "SESSION_LONDON_07_16",
            "SESSION",
            "07:00-15:59 Europe/London",
        ),
        (
            "SESSION_NY_08_17",
            "SESSION",
            "08:00-16:59 America/New_York",
        ),

        (
            "H4_CLOSE_LT_EMA100",
            "H4_TREND",
            "Prior strictly completed H4 close < EMA100",
        ),
        (
            "H4_CLOSE_LT_EMA200",
            "H4_TREND",
            "Prior strictly completed H4 close < EMA200",
        ),
        (
            "H4_EMA50_LT_EMA200",
            "H4_TREND",
            "Prior strictly completed H4 EMA50 < EMA200",
        ),

        (
            "D_CLOSE_LT_EMA100",
            "D_TREND",
            "Prior strictly completed Daily close < EMA100",
        ),
        (
            "D_CLOSE_LT_EMA200",
            "D_TREND",
            "Prior strictly completed Daily close < EMA200",
        ),
        (
            "D_EMA50_LT_EMA200",
            "D_TREND",
            "Prior strictly completed Daily EMA50 < EMA200",
        ),

        (
            "H4_ATR_RATIO50_GE_080",
            "VOLATILITY",
            "Prior completed H4 ATR14 / prior50 mean >= 0.80",
        ),
        (
            "D_ATR_RATIO50_GE_080",
            "VOLATILITY",
            "Prior completed Daily ATR14 / prior50 mean >= 0.80",
        ),
    ]


def rf_context_mask(
    context_id,
    cache,
):
    n = len(
        cache["NY_WEEKDAY"]
    )

    if context_id == "NONE":
        return np.ones(
            n,
            dtype=bool,
        )

    weekday = {
        "EXCLUDE_MON_NY": 0,
        "EXCLUDE_TUE_NY": 1,
        "EXCLUDE_WED_NY": 2,
        "EXCLUDE_THU_NY": 3,
        "EXCLUDE_FRI_NY": 4,
    }

    if context_id in weekday:
        return (
            cache["NY_WEEKDAY"]
            != weekday[context_id]
        )

    session_map = {
        "SESSION_SYDNEY_08_17":
            "SESSION_SYDNEY",
        "SESSION_TOKYO_08_17":
            "SESSION_TOKYO",
        "SESSION_LONDON_07_16":
            "SESSION_LONDON",
        "SESSION_NY_08_17":
            "SESSION_NY",
    }

    if context_id in session_map:
        return cache[
            session_map[context_id]
        ].copy()

    if context_id == "H4_CLOSE_LT_EMA100":
        return (
            np.isfinite(cache["H4_CLOSE"])
            & np.isfinite(cache["H4_EMA100"])
            & (
                cache["H4_CLOSE"]
                < cache["H4_EMA100"]
            )
        )

    if context_id == "H4_CLOSE_LT_EMA200":
        return (
            np.isfinite(cache["H4_CLOSE"])
            & np.isfinite(cache["H4_EMA200"])
            & (
                cache["H4_CLOSE"]
                < cache["H4_EMA200"]
            )
        )

    if context_id == "H4_EMA50_LT_EMA200":
        return (
            np.isfinite(cache["H4_EMA50"])
            & np.isfinite(cache["H4_EMA200"])
            & (
                cache["H4_EMA50"]
                < cache["H4_EMA200"]
            )
        )

    if context_id == "D_CLOSE_LT_EMA100":
        return (
            np.isfinite(cache["D_CLOSE"])
            & np.isfinite(cache["D_EMA100"])
            & (
                cache["D_CLOSE"]
                < cache["D_EMA100"]
            )
        )

    if context_id == "D_CLOSE_LT_EMA200":
        return (
            np.isfinite(cache["D_CLOSE"])
            & np.isfinite(cache["D_EMA200"])
            & (
                cache["D_CLOSE"]
                < cache["D_EMA200"]
            )
        )

    if context_id == "D_EMA50_LT_EMA200":
        return (
            np.isfinite(cache["D_EMA50"])
            & np.isfinite(cache["D_EMA200"])
            & (
                cache["D_EMA50"]
                < cache["D_EMA200"]
            )
        )

    if context_id == "H4_ATR_RATIO50_GE_080":
        return (
            np.isfinite(
                cache["H4_ATR_RATIO50"]
            )
            & (
                cache["H4_ATR_RATIO50"]
                >= 0.80
            )
        )

    if context_id == "D_ATR_RATIO50_GE_080":
        return (
            np.isfinite(
                cache["D_ATR_RATIO50"]
            )
            & (
                cache["D_ATR_RATIO50"]
                >= 0.80
            )
        )

    raise ValueError(
        f"Unknown context: {context_id}"
    )


def rf_apply_context(
    raw_indices,
    context_id,
    cache,
):
    raw_indices = np.asarray(
        raw_indices,
        dtype=int,
    )

    if not len(
        raw_indices
    ):
        return raw_indices

    mask = rf_context_mask(
        context_id,
        cache,
    )

    return raw_indices[
        mask[raw_indices]
    ]


def rf_row_to_compression(
    row,
    rr=None,
    context_id=None,
):
    return rf_compression_config(
        row["body_atr_min"],
        row["range_atr_min"],
        row["compression_max"],
        int(
            row["breakout_lookback"]
        ),
        (
            row["rr"]
            if rr is None
            else rr
        ),
        context_id=context_id,
    )


def rf_select_context_bases(
    rr_rows,
):
    eligible = [
        row for row in rr_rows
        if (
            row["full_trades"] >= 40
            and row["full_pf"] >= 1.20
            and row[
                "both_temporal_splits_positive"
            ]
            and row["positive_eras"] >= 3
            and row["last5y_r"] > 0
        )
    ]

    pool = eligible if eligible else list(
        rr_rows
    )

    pool = sorted(
        pool,
        key=rf_refinement_sort_key,
        reverse=True,
    )

    selected = []
    seen = set()

    forced_configs = [
        rf_compression_config(
            1.00, 1.50, 0.80, 10, 4.00
        ),
        rf_compression_config(
            1.00, 1.00, 0.80, 5, 4.00
        ),
    ]

    row_map = {
        r["config_id"]: r
        for r in rr_rows
    }

    for config in forced_configs:
        row = row_map.get(
            config["config_id"]
        )
        if row is not None:
            selected.append(row)
            seen.add(
                row["config_id"]
            )

    for row in pool:
        if len(selected) >= RF_CONTEXT_BASES:
            break

        if row["config_id"] in seen:
            continue

        selected.append(row)
        seen.add(
            row["config_id"]
        )

    return selected


def rf_context_ablation(
    context_rows,
):
    by_base = defaultdict(
        dict
    )

    for row in context_rows:
        key = (
            row["body_atr_min"],
            row["range_atr_min"],
            row["compression_max"],
            int(
                row["breakout_lookback"]
            ),
            row["rr"],
        )
        by_base[key][
            row["context_id"]
        ] = row

    output = []

    for key, rows in by_base.items():
        base = rows.get(
            "NONE"
        )

        if base is None:
            continue

        for context_id, row in rows.items():
            if context_id == "NONE":
                continue

            output.append({
                "body_atr_min": key[0],
                "range_atr_min": key[1],
                "compression_max": key[2],
                "breakout_lookback": key[3],
                "rr": key[4],
                "context_id": context_id,
                "base_trades": base[
                    "full_trades"
                ],
                "context_trades": row[
                    "full_trades"
                ],
                "retention_pct": (
                    100.0
                    * row["full_trades"]
                    / base["full_trades"]
                    if base["full_trades"]
                    else 0.0
                ),
                "base_pf": base["full_pf"],
                "context_pf": row["full_pf"],
                "delta_pf": (
                    row["full_pf"]
                    - base["full_pf"]
                ),
                "base_total_r":
                    base["full_total_r"],
                "context_total_r":
                    row["full_total_r"],
                "delta_total_r": (
                    row["full_total_r"]
                    - base["full_total_r"]
                ),
                "base_min_split_pf":
                    base[
                        "min_temporal_split_pf"
                    ],
                "context_min_split_pf":
                    row[
                        "min_temporal_split_pf"
                    ],
                "delta_min_split_pf": (
                    row[
                        "min_temporal_split_pf"
                    ]
                    - base[
                        "min_temporal_split_pf"
                    ]
                ),
                "base_last5_r":
                    base["last5y_r"],
                "context_last5_r":
                    row["last5y_r"],
                "delta_last5_r": (
                    row["last5y_r"]
                    - base["last5y_r"]
                ),
                "base_last2_r":
                    base["last2y_r"],
                "context_last2_r":
                    row["last2y_r"],
                "delta_last2_r": (
                    row["last2y_r"]
                    - base["last2y_r"]
                ),
            })

    return output


def rf_parity_check(
    features,
):
    cutoff_index = bisect_right(
        features["times"],
        RF_DISCOVERY_CUTOFF,
    )

    # Slice only arrays/dictionaries needed by signal + backtest.
    sliced = {}

    for key, value in features.items():
        if isinstance(
            value,
            np.ndarray,
        ):
            sliced[key] = value[
                :cutoff_index
            ].copy()

        elif key == "times":
            sliced[key] = value[
                :cutoff_index
            ]

        elif key in (
            "prev_lows",
            "prev_highs",
        ):
            sliced[key] = {
                lb: arr[
                    :cutoff_index
                ].copy()
                for lb, arr
                in value.items()
            }

        else:
            sliced[key] = value

    controls = {
        "COMPRESSION_CENTRAL_RR4":
            rf_compression_config(
                1.00, 1.50, 0.80, 10, 4.00
            ),

        "COMPRESSION_FREQUENCY_RR4":
            rf_compression_config(
                1.00, 1.00, 0.80, 5, 4.00
            ),

        "SWEEP_LB15_RR3P5":
            rf_sweep_config(
                3.50
            ),

        "OUTSIDE_LB60_D030_RR4P5":
            rf_outside_benchmark(),
    }

    rows = []

    for name, config in controls.items():
        indices = signal_indices(
            config,
            sliced,
        )

        row, _ = evaluate_candidate(
            config,
            sliced,
            indices,
        )

        expected = RF_PARITY_EXPECTED[
            name
        ]

        passes = (
            row["full_trades"]
            == expected["trades"]
            and abs(
                row["full_pf"]
                - expected["pf"]
            ) <= 1e-9
            and abs(
                row["full_total_r"]
                - expected["total_r"]
            ) <= 1e-9
        )

        rows.append({
            "control": name,
            "config_id":
                config["config_id"],
            "expected_trades":
                expected["trades"],
            "actual_trades":
                row["full_trades"],
            "expected_pf":
                expected["pf"],
            "actual_pf":
                row["full_pf"],
            "expected_total_r":
                expected["total_r"],
            "actual_total_r":
                row["full_total_r"],
            "pass":
                passes,
        })

        if not passes:
            raise RuntimeError(
                f"Discovery parity failed for {name}: "
                f"{rows[-1]}"
            )

    return rows


def rf_secondary_control_rows(
    features,
):
    rows = []

    for rr in (
        3.00,
        3.50,
        4.00,
    ):
        config = rf_sweep_config(
            rr
        )
        indices = signal_indices(
            config,
            features,
        )
        row, _ = evaluate_candidate(
            config,
            features,
            indices,
        )
        row["control_role"] = (
            "SECONDARY_RECENTLY_POSITIVE_SWEEP"
        )
        rows.append(row)

    config = rf_outside_benchmark()
    indices = signal_indices(
        config,
        features,
    )
    row, _ = evaluate_candidate(
        config,
        features,
        indices,
    )
    row["control_role"] = (
        "FROZEN_ROBUST_OUTSIDE_BENCHMARK"
    )
    rows.append(row)

    return rows


def rf_select_finalists(
    context_rows,
    rr_rows,
    secondary_rows,
):
    # We deliberately favour candidates that repair recent weakness without
    # sacrificing the old sample.
    context_eligible = [
        r for r in context_rows
        if (
            r["full_trades"] >= 40
            and r[
                "both_temporal_splits_positive"
            ]
            and r["positive_eras"] >= 3
            and r["last5y_r"] > 0
            and r["last2y_r"] > 0
        )
    ]

    context_pool = (
        context_eligible
        if context_eligible
        else context_rows
    )

    context_pool = sorted(
        context_pool,
        key=rf_refinement_sort_key,
        reverse=True,
    )

    selected = []
    seen = set()

    for row in context_pool:
        if len(selected) >= RF_CONTEXT_FINAL_KEEP:
            break

        if row["config_id"] in seen:
            continue

        selected.append(
            dict(row)
        )
        seen.add(
            row["config_id"]
        )

    # Exact no-context compression references.
    rr_map = {
        r["config_id"]: r
        for r in rr_rows
    }

    for config in [
        rf_compression_config(
            1.00, 1.50, 0.80, 10, 4.00
        ),
        rf_compression_config(
            1.00, 1.00, 0.80, 5, 4.00
        ),
    ]:
        row = rr_map.get(
            config["config_id"]
        )
        if (
            row is not None
            and row["config_id"] not in seen
        ):
            output = dict(row)
            output["context_id"] = "NONE"
            output["context_group"] = "BASE"
            output["context_description"] = (
                "Forced no-context compression reference"
            )
            selected.append(output)
            seen.add(
                row["config_id"]
            )

    # Secondary controls are deep-diagnosed but never treated as compression
    # refinement winners.
    for row in secondary_rows:
        output = dict(row)
        output["context_id"] = "NONE"
        output["context_group"] = "CONTROL"
        output["context_description"] = (
            row["control_role"]
        )

        if output["config_id"] in seen:
            continue

        selected.append(output)
        seen.add(
            output["config_id"]
        )

    return selected


def rf_deep_config_from_row(
    row,
):
    if (
        row["family"]
        == "COMPRESSION_BREAKOUT"
    ):
        return rf_compression_config(
            row["body_atr_min"],
            row["range_atr_min"],
            row["compression_max"],
            int(
                row["breakout_lookback"]
            ),
            row["rr"],
            context_id=row.get(
                "context_id",
                "NONE",
            ),
        )

    if (
        row["family"]
        == "SWEEP_DISPLACEMENT"
    ):
        return rf_sweep_config(
            row["rr"]
        )

    if (
        row["family"]
        == "OUTSIDE_REVERSAL"
    ):
        return rf_outside_benchmark()

    raise ValueError(
        f"Unsupported deep family: "
        f"{row['family']}"
    )


def rf_screen_rows(
    finalist_rows,
    costs,
    rolling_summaries,
    calendar_summaries,
    ablations,
):
    cost_lookup = {
        (
            r["config_id"],
            r["cost_multiplier"],
        ): r
        for r in costs
    }

    roll_lookup = defaultdict(dict)

    for row in rolling_summaries:
        roll_lookup[
            row["config_id"]
        ][int(
            row["window_months"]
        )] = row

    cal_lookup = {
        row["config_id"]: row
        for row in calendar_summaries
    }

    ablation_lookup = {
        (
            r["body_atr_min"],
            r["range_atr_min"],
            r["compression_max"],
            int(
                r["breakout_lookback"]
            ),
            r["rr"],
            r["context_id"],
        ): r
        for r in ablations
    }

    output = []

    for seed in finalist_rows:
        row = dict(seed)
        cid = row["config_id"]

        cost2 = cost_lookup.get(
            (cid, 2.0),
            {},
        )
        r24 = roll_lookup.get(
            cid,
            {},
        ).get(
            24,
            {},
        )
        r36 = roll_lookup.get(
            cid,
            {},
        ).get(
            36,
            {},
        )
        cal = cal_lookup.get(
            cid,
            {},
        )

        context_id = row.get(
            "context_id",
            "NONE",
        )

        ablation = {}

        if (
            row["family"]
            == "COMPRESSION_BREAKOUT"
            and context_id != "NONE"
        ):
            ablation = ablation_lookup.get(
                (
                    row["body_atr_min"],
                    row["range_atr_min"],
                    row["compression_max"],
                    int(
                        row[
                            "breakout_lookback"
                        ]
                    ),
                    row["rr"],
                    context_id,
                ),
                {},
            )

        row.update({
            "cost_2x_pf":
                cost2.get(
                    "profit_factor",
                    0.0,
                ),
            "cost_2x_total_r":
                cost2.get(
                    "total_r",
                    0.0,
                ),
            "rolling24_positive_pct":
                r24.get(
                    "positive_active_windows_pct",
                    0.0,
                ),
            "rolling24_worst_r":
                r24.get(
                    "worst_r_active",
                    0.0,
                ),
            "rolling36_positive_pct":
                r36.get(
                    "positive_active_windows_pct",
                    0.0,
                ),
            "rolling36_worst_r":
                r36.get(
                    "worst_r_active",
                    0.0,
                ),
            "positive_calendar_year_pct":
                cal.get(
                    "positive_active_years_pct",
                    0.0,
                ),
            "worst_calendar_year_r":
                cal.get(
                    "worst_active_year_r",
                    0.0,
                ),
            "context_delta_pf":
                ablation.get(
                    "delta_pf",
                    0.0,
                ),
            "context_delta_last2_r":
                ablation.get(
                    "delta_last2_r",
                    0.0,
                ),
        })

        compression_candidate = (
            row["family"]
            == "COMPRESSION_BREAKOUT"
        )

        row["refinement_screen_pass"] = bool(
            compression_candidate
            and row["full_trades"] >= 40
            and row["full_pf"] >= 1.35
            and row[
                "both_temporal_splits_positive"
            ]
            and row[
                "min_temporal_split_pf"
            ] >= 1.25
            and row["positive_eras"] >= 3
            and row["last5y_r"] > 0
            and row["last2y_r"] > 0
            and row["cost_2x_pf"] >= 1.20
            and row["cost_2x_total_r"] > 0
            and row[
                "rolling24_positive_pct"
            ] >= 70.0
            and row[
                "rolling36_positive_pct"
            ] >= 75.0
            and row[
                "positive_calendar_year_pct"
            ] >= 55.0
        )

        output.append(row)

    return output


def run_rf_research():
    try:
        RF_STATUS.update({
            "state": "fetching",
            "message": "Fetching AUD/USD H1/H4/D history",
        })

        h1 = fetch_history(
            "H1",
            REQUESTED_START,
            NOW,
            chunk_days=180,
        )

        h4 = fetch_history(
            "H4",
            RF_WARMUP_START,
            NOW,
            chunk_days=720,
        )

        daily = fetch_history(
            "D",
            RF_WARMUP_START,
            NOW,
            chunk_days=3000,
        )

        if len(h1) < 100000:
            raise RuntimeError(
                f"Unexpectedly small H1 history: {len(h1)}"
            )

        if len(h4) < 10000:
            raise RuntimeError(
                f"Unexpectedly small H4 history: {len(h4)}"
            )

        if len(daily) < 5000:
            raise RuntimeError(
                f"Unexpectedly small Daily history: {len(daily)}"
            )

        coverage = [
            {
                "pair": PAIR,
                "timeframe": "H1",
                "candles": len(h1),
                "first_candle_utc": iso(
                    h1[0]["time"]
                ),
                "last_candle_utc": iso(
                    h1[-1]["time"]
                ),
            },
            {
                "pair": PAIR,
                "timeframe": "H4",
                "candles": len(h4),
                "first_candle_utc": iso(
                    h4[0]["time"]
                ),
                "last_candle_utc": iso(
                    h4[-1]["time"]
                ),
            },
            {
                "pair": PAIR,
                "timeframe": "D",
                "candles": len(daily),
                "first_candle_utc": iso(
                    daily[0]["time"]
                ),
                "last_candle_utc": iso(
                    daily[-1]["time"]
                ),
            },
        ]

        write_csv(
            RF_OUT["coverage"],
            coverage,
        )

        RF_STATUS.update({
            "state": "features",
            "message": "Building H1 + strictly completed HTF features",
        })

        features = build_features(
            h1,
            "H1",
        )

        # The broad runner used 5/10/20 breakout windows. This focused
        # refinement adds only the local midpoint LB15.
        if 15 not in features[
            "prev_lows"
        ]:
            features[
                "prev_lows"
            ][15] = (
                rolling_previous_extreme(
                    features["low"],
                    15,
                    want_max=False,
                )
            )
            features[
                "prev_highs"
            ][15] = (
                rolling_previous_extreme(
                    features["high"],
                    15,
                    want_max=True,
                )
            )

        context_cache = rf_context_cache(
            features,
            h4,
            daily,
        )

        parity_rows = rf_parity_check(
            features
        )

        write_csv(
            RF_OUT["control_parity"],
            parity_rows,
        )

        # ----------------------------------------------------
        # STAGE 1 — LOCAL COMPRESSION GEOMETRY
        # ----------------------------------------------------
        geometry_rows = []
        geometry_indices = {}

        geometry_configs = (
            rf_geometry_configs()
        )

        for number, config in enumerate(
            geometry_configs,
            1,
        ):
            RF_STATUS.update({
                "state": "geometry",
                "message": (
                    f"{number}/{len(geometry_configs)} "
                    f"{config['config_id']}"
                ),
            })

            indices = signal_indices(
                config,
                features,
            )

            geometry_indices[
                config["config_id"]
            ] = indices

            row, _ = evaluate_candidate(
                config,
                features,
                indices,
            )

            geometry_rows.append(row)

        geometry_rows.sort(
            key=rf_refinement_sort_key,
            reverse=True,
        )

        write_csv(
            RF_OUT["geometry"],
            geometry_rows,
        )

        write_csv(
            RF_OUT["geometry_summary"],
            rf_geometry_summary(
                geometry_rows
            ),
        )

        geometry_shortlist = (
            rf_select_geometry_shortlist(
                geometry_rows
            )
        )

        write_csv(
            RF_OUT["geometry_shortlist"],
            geometry_shortlist,
        )

        # ----------------------------------------------------
        # STAGE 2 — RR CONFIRMATION
        # ----------------------------------------------------
        rr_rows = []
        rr_indices = {}

        total_rr = (
            len(geometry_shortlist)
            * len(RF_RR_GRID)
        )
        done = 0

        for base_row in geometry_shortlist:
            base_config = (
                rf_row_to_compression(
                    base_row,
                    rr=RF_GEOMETRY_RR,
                )
            )

            indices = geometry_indices.get(
                base_config["config_id"]
            )

            if indices is None:
                indices = signal_indices(
                    base_config,
                    features,
                )

            for rr in RF_RR_GRID:
                done += 1

                config = (
                    rf_row_to_compression(
                        base_row,
                        rr=rr,
                    )
                )

                RF_STATUS.update({
                    "state": "rr_confirmation",
                    "message": (
                        f"{done}/{total_rr} "
                        f"{config['config_id']}"
                    ),
                })

                row, _ = evaluate_candidate(
                    config,
                    features,
                    indices,
                )

                rr_rows.append(row)
                rr_indices[
                    config["config_id"]
                ] = indices

        # Guarantee the exact two compression controls are present.
        for config in [
            rf_compression_config(
                1.00, 1.50, 0.80, 10, 4.00
            ),
            rf_compression_config(
                1.00, 1.00, 0.80, 5, 4.00
            ),
        ]:
            if config["config_id"] in rr_indices:
                continue

            indices = signal_indices(
                config,
                features,
            )
            row, _ = evaluate_candidate(
                config,
                features,
                indices,
            )
            rr_rows.append(row)
            rr_indices[
                config["config_id"]
            ] = indices

        rr_rows.sort(
            key=rf_refinement_sort_key,
            reverse=True,
        )

        write_csv(
            RF_OUT["rr"],
            rr_rows,
        )

        # ----------------------------------------------------
        # SECONDARY FROZEN CONTROLS
        # ----------------------------------------------------
        secondary_rows = (
            rf_secondary_control_rows(
                features
            )
        )

        write_csv(
            RF_OUT["secondary_controls"],
            secondary_rows,
        )

        # ----------------------------------------------------
        # STAGE 3 — SINGLE-FACTOR CONTEXTS
        # ----------------------------------------------------
        context_bases = (
            rf_select_context_bases(
                rr_rows
            )
        )

        write_csv(
            RF_OUT["context_bases"],
            context_bases,
        )

        context_rows = []
        context_indices = {}

        contexts = (
            rf_context_definitions()
        )

        total_context = (
            len(context_bases)
            * len(contexts)
        )
        done = 0

        for base_row in context_bases:
            base_config = (
                rf_row_to_compression(
                    base_row
                )
            )

            base_indices = rr_indices.get(
                base_config["config_id"]
            )

            if base_indices is None:
                base_indices = signal_indices(
                    base_config,
                    features,
                )

            for (
                context_id,
                context_group,
                description,
            ) in contexts:
                done += 1

                RF_STATUS.update({
                    "state": "context_scan",
                    "message": (
                        f"{done}/{total_context} "
                        f"{context_id}"
                    ),
                })

                config = (
                    rf_row_to_compression(
                        base_row,
                        context_id=context_id,
                    )
                )

                indices = rf_apply_context(
                    base_indices,
                    context_id,
                    context_cache,
                )

                row, _ = evaluate_candidate(
                    config,
                    features,
                    indices,
                )

                row["context_id"] = (
                    context_id
                )
                row["context_group"] = (
                    context_group
                )
                row[
                    "context_description"
                ] = description
                row[
                    "unfiltered_signal_count"
                ] = len(
                    base_indices
                )
                row[
                    "filtered_signal_count"
                ] = len(
                    indices
                )
                row[
                    "signal_retention_pct"
                ] = (
                    100.0
                    * len(indices)
                    / len(base_indices)
                    if len(base_indices)
                    else 0.0
                )

                context_rows.append(
                    row
                )
                context_indices[
                    config["config_id"]
                ] = indices

        context_rows.sort(
            key=rf_refinement_sort_key,
            reverse=True,
        )

        write_csv(
            RF_OUT["contexts"],
            context_rows,
        )

        ablations = (
            rf_context_ablation(
                context_rows
            )
        )

        write_csv(
            RF_OUT[
                "context_ablation"
            ],
            ablations,
        )

        # ----------------------------------------------------
        # DEEP FINALISTS
        # ----------------------------------------------------
        finalist_seeds = (
            rf_select_finalists(
                context_rows,
                rr_rows,
                secondary_rows,
            )
        )

        periods = []
        costs = []
        rolling = []
        calendar = []
        trades_output = []

        for number, seed in enumerate(
            finalist_seeds,
            1,
        ):
            config = (
                rf_deep_config_from_row(
                    seed
                )
            )

            RF_STATUS.update({
                "state": "deep_validation",
                "message": (
                    f"{number}/{len(finalist_seeds)} "
                    f"{config['config_id']}"
                ),
            })

            if (
                config["family"]
                == "COMPRESSION_BREAKOUT"
            ):
                context_id = config.get(
                    "context_id",
                    "NONE",
                )

                indices = (
                    context_indices.get(
                        config["config_id"]
                    )
                )

                if indices is None:
                    base = dict(config)
                    base.pop(
                        "context_id",
                        None,
                    )
                    base["config_id"] = (
                        config_id(base)
                    )

                    raw = signal_indices(
                        base,
                        features,
                    )

                    indices = rf_apply_context(
                        raw,
                        context_id,
                        context_cache,
                    )

            else:
                indices = signal_indices(
                    config,
                    features,
                )

            trades = backtest(
                config,
                features,
                indices,
            )

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
                    indices,
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
                output[
                    "signal_time"
                ] = iso(
                    output[
                        "signal_time"
                    ]
                )
                output[
                    "exit_time"
                ] = iso(
                    output[
                        "exit_time"
                    ]
                )
                output[
                    "context_id"
                ] = config.get(
                    "context_id",
                    "NONE",
                )
                trades_output.append(
                    output
                )

        rolling_summaries = (
            rolling_summary(
                rolling
            )
        )

        calendar_summaries = (
            calendar_summary(
                calendar
            )
        )

        final_rows = rf_screen_rows(
            finalist_seeds,
            costs,
            rolling_summaries,
            calendar_summaries,
            ablations,
        )

        write_csv(
            RF_OUT["finalists"],
            final_rows,
        )
        write_csv(
            RF_OUT["periods"],
            periods,
        )
        write_csv(
            RF_OUT["cost_stress"],
            costs,
        )
        write_csv(
            RF_OUT["rolling"],
            rolling,
        )
        write_csv(
            RF_OUT["rolling_summary"],
            rolling_summaries,
        )
        write_csv(
            RF_OUT["calendar"],
            calendar,
        )
        write_csv(
            RF_OUT["calendar_summary"],
            calendar_summaries,
        )
        write_csv(
            RF_OUT["trades"],
            trades_output,
        )

        screen_passes = [
            r for r in final_rows
            if r[
                "refinement_screen_pass"
            ]
        ]

        write_csv(
            RF_OUT["notes"],
            [
                {
                    "topic": "scope",
                    "note": (
                        "Controlled AUD/USD H1 SHORT refinement. "
                        "Only COMPRESSION_BREAKOUT geometry is locally "
                        "refined. Sweep and outside-reversal branches are "
                        "frozen controls."
                    ),
                },
                {
                    "topic": "parity",
                    "note": (
                        "Four exact broad-discovery controls must reproduce "
                        "through 2026-09-18 14:00 UTC before refinement."
                    ),
                },
                {
                    "topic": "geometry",
                    "note": (
                        "108 compression geometries only: compression "
                        "0.75/0.80/0.85, body 0.75/1.00/1.25 ATR, range "
                        "1.00/1.25/1.50 ATR, breakout LB5/10/15/20; fixed RR4."
                    ),
                },
                {
                    "topic": "rr",
                    "note": (
                        "Only shortlisted compression geometries receive "
                        "RR3/3.5/4/4.5/5 confirmation."
                    ),
                },
                {
                    "topic": "contexts",
                    "note": (
                        "Single-factor weekday/session/strictly-completed "
                        "H4/D bearish-trend and ATR-regime contexts only. "
                        "No context interactions."
                    ),
                },
                {
                    "topic": "recent_weakness",
                    "note": (
                        "The refinement screen explicitly requires positive "
                        "last-2Y R because broad compression was strong over "
                        "long history but weak in the most recent two years."
                    ),
                },
                {
                    "topic": "controls",
                    "note": (
                        "SWEEP_DISPLACEMENT body1.25/LB15/wick0.25 is retained "
                        "at RR3/3.5/4 as the recently-positive secondary control. "
                        "OUTSIDE_REVERSAL body1/LB60/dist0.30/close0.15/RR4.5 "
                        "is retained as the frozen robustness benchmark."
                    ),
                },
                {
                    "topic": "costs",
                    "note": (
                        "H1 baseline adverse fill remains 0.5 pip; deep "
                        "finalists are stressed through 2x cost."
                    ),
                },
                {
                    "topic": "portfolio",
                    "note": (
                        "No live25 portfolio integration occurs here. Any "
                        "surviving short must next be tested as prospective "
                        "#26 with exact AUD/USD LONG/SHORT non-hedging conflicts."
                    ),
                },
                {
                    "topic": "screen",
                    "note": (
                        f"{len(screen_passes)} of {len(final_rows)} deep "
                        "rows passed the predeclared compression refinement "
                        "screen. A pass is not a live lock."
                    ),
                },
            ],
        )

        RF_STATUS.update({
            "state": "packaging",
            "message": "Packaging AUD/USD H1 SHORT refinement results",
        })

        rf_pack()

        RF_STATUS.update({
            "state": "complete",
            "message": "AUD/USD H1 SHORT controlled refinement complete",
            "h1_candles": len(h1),
            "h4_candles": len(h4),
            "daily_candles": len(daily),
            "control_parity_passes": len(
                parity_rows
            ),
            "geometry_configs": len(
                geometry_rows
            ),
            "geometry_shortlist": len(
                geometry_shortlist
            ),
            "rr_rows": len(
                rr_rows
            ),
            "context_bases": len(
                context_bases
            ),
            "context_rows": len(
                context_rows
            ),
            "deep_finalists": len(
                final_rows
            ),
            "refinement_screen_passes": len(
                screen_passes
            ),
            "bundle": RF_BUNDLE,
        })

    except Exception as error:
        import traceback

        RF_STATUS.update({
            "state": "error",
            "message": str(error),
            "error_type":
                type(error).__name__,
            "traceback":
                traceback.format_exc(),
        })

        print(
            "AUDUSD H1 SHORT REFINEMENT ERROR:",
            repr(error),
            flush=True,
        )


@app.route(
    "/audusd-h1-short-refinement/status"
)
def rf_status():
    return jsonify(
        RF_STATUS
    )


@app.route(
    "/audusd-h1-short-refinement/results"
)
def rf_results():
    if not os.path.exists(
        RF_BUNDLE
    ):
        return jsonify({
            "status": "not_ready",
            "state": RF_STATUS[
                "state"
            ],
            "message": RF_STATUS[
                "message"
            ],
        }), 404

    return send_file(
        os.path.abspath(
            RF_BUNDLE
        ),
        as_attachment=True,
        download_name=RF_BUNDLE,
    )


@app.route(
    "/audusd-h1-short-refinement/info"
)
def rf_info():
    return jsonify({
        "service": (
            "AUD/USD H1 SHORT Controlled Compression Refinement"
        ),
        "read_only": True,
        "orders_supported": False,
        "primary_family":
            "COMPRESSION_BREAKOUT",
        "geometry_rr":
            RF_GEOMETRY_RR,
        "geometry_configs_expected":
            len(RF_COMPRESSION_GRID)
            * len(RF_BODY_GRID)
            * len(RF_RANGE_GRID)
            * len(RF_BREAKOUT_GRID),
        "rr_grid":
            RF_RR_GRID,
        "context_count":
            len(
                rf_context_definitions()
            ),
        "secondary_controls": [
            "SWEEP_DISPLACEMENT body1.25 LB15 wick0.25 RR3/3.5/4",
            "OUTSIDE_REVERSAL body1 LB60 dist0.30 close0.15 RR4.5",
        ],
        "prospective_portfolio_strategy":
            "#26",
        "routes": [
            "/audusd-h1-short-refinement/status",
            "/audusd-h1-short-refinement/results",
            "/audusd-h1-short-refinement/info",
        ],
    })



# ============================================================
# AUD/USD H1 SHORT — FINAL STANDALONE CONFIRMATION
# ============================================================
#
# PURPOSE
# -------
# Freeze and deeply validate ONE exact AUD/USD H1 SHORT candidate before
# any prospective #26 portfolio-add test.
#
# FROZEN PRIMARY CANDIDATE
# ------------------------
# Family: COMPRESSION_BREAKOUT
# Side: SHORT
# Timeframe: H1
#
# Signal candle:
#   bearish
#   body >= 1.25 ATR14
#   range >= 1.50 ATR14
#   prior-H1 ATR14 / prior20-H1 ATR14 mean <= 0.85
#   close < previous 15-bar low, current excluded
#
# Execution:
#   reference entry = signal close
#   historical adverse fill = reference entry - 0.5 pip
#   stop = signal high + 10 ticks
#   target = reference entry - 3.50 * reference risk
#   pyramiding = 0 exact strategy stream
#   exit-candle re-entry eligible
#
# No weekday filter
# No session filter
# No H4/D regime filter
#
# EXACT REFINEMENT ANCHOR THROUGH 2026-09-18 14:00 UTC
# ------------------------------------------------------
# 158 trades
# 52 winners
# PF 1.667562...
# +70.761534R
# max DD -11R
# dev 2002-2017 PF 1.798946...
# validation 2018+ PF 1.432453...
# last 5Y +13.623363R
# last 2Y +0.137825R
#
# FROZEN SECONDARY CONTROLS
# -------------------------
# A) SWEEP_DISPLACEMENT
#    body >= 1.25 ATR
#    sweep prior15 high
#    upper wick/body >= 0.25
#    close < previous H1 low
#    RR3.50
#
# B) OUTSIDE_REVERSAL
#    body >= 1.00 ATR
#    prior60 high distance <= 0.30 ATR
#    close location <= 0.15
#    RR4.50
#
# PARAMETER ROBUSTNESS
# --------------------
# One-at-a-time neighbours only. We do NOT optimise a new grid:
#   body:        1.00 / [1.25] / 1.50 ATR
#   range:       1.25 / [1.50] / 1.75 ATR
#   compression: 0.80 / [0.85] / 0.90
#   breakout LB: 10 / [15] / 20
#   RR:          3.00 / [3.50] / 4.00
#
# Deep diagnostics:
#   exact parity
#   full/dev/validation/eras/recent periods
#   0.5x / 1x / 1.5x / 2x cost stress
#   rolling 12/24/36 month windows
#   calendar years
#   exact trade ledger
#   frozen-control comparison
#
# This runner does NOT test portfolio contribution and does NOT send orders.
# If the primary candidate survives, the next step is the exact current25 ->
# prospective26 portfolio-add test, including AUD/USD LONG-vs-SHORT conflicts.
# ============================================================

FC_STATUS = {
    "state": "not_started",
    "message": "AUD/USD H1 SHORT final standalone confirmation not started",
    "pair": PAIR,
    "timeframe": "H1",
    "side": "SHORT",
    "orders_supported": False,
    "trading_enabled": False,
}

FC_BUNDLE = "AUDUSD_H1_SHORT_FINAL_CONFIRMATION_RESULTS.zip"

FC_OUT = {
    "coverage": "audusd_h1_short_final_confirmation_coverage.csv",
    "parity": "audusd_h1_short_final_confirmation_parity.csv",
    "headline": "audusd_h1_short_final_confirmation_headline.csv",
    "periods": "audusd_h1_short_final_confirmation_periods.csv",
    "cost_stress": "audusd_h1_short_final_confirmation_cost_stress.csv",
    "rolling": "audusd_h1_short_final_confirmation_rolling.csv",
    "rolling_summary": "audusd_h1_short_final_confirmation_rolling_summary.csv",
    "calendar": "audusd_h1_short_final_confirmation_calendar.csv",
    "calendar_summary": "audusd_h1_short_final_confirmation_calendar_summary.csv",
    "parameter_neighbours": "audusd_h1_short_final_confirmation_parameter_neighbours.csv",
    "neighbour_summary": "audusd_h1_short_final_confirmation_neighbour_summary.csv",
    "candidate_trades": "audusd_h1_short_final_confirmation_candidate_trades.csv",
    "control_trades": "audusd_h1_short_final_confirmation_control_trades.csv",
    "decision": "audusd_h1_short_final_confirmation_decision.csv",
    "notes": "audusd_h1_short_final_confirmation_notes.csv",
}

FC_PARITY_CUTOFF = datetime(
    2026, 9, 18, 14, 0,
    tzinfo=timezone.utc,
)

FC_PRIMARY_ID = "AUD_USD_H1_SHORT_COMPRESSION_FINAL_CANDIDATE"
FC_SWEEP_ID = "AUD_USD_H1_SHORT_SWEEP_CONTROL"
FC_OUTSIDE_ID = "AUD_USD_H1_SHORT_OUTSIDE_CONTROL"

FC_PARITY_FLOAT_TOL = 1e-8


def fc_primary_config(
    body=1.25,
    range_atr=1.50,
    compression=0.85,
    breakout_lb=15,
    rr=3.50,
):
    config = {
        "timeframe": "H1",
        "side": "SHORT",
        "family": "COMPRESSION_BREAKOUT",
        "body_atr_min": float(body),
        "range_atr_min": float(range_atr),
        "compression_max": float(compression),
        "breakout_lookback": int(breakout_lb),
        "rr": float(rr),
    }
    config["config_id"] = config_id(config)
    return config


def fc_sweep_control():
    config = {
        "timeframe": "H1",
        "side": "SHORT",
        "family": "SWEEP_DISPLACEMENT",
        "body_atr_min": 1.25,
        "lookback": 15,
        "wick_body_min": 0.25,
        "rr": 3.50,
    }
    config["config_id"] = config_id(config)
    return config


def fc_outside_control():
    config = {
        "timeframe": "H1",
        "side": "SHORT",
        "family": "OUTSIDE_REVERSAL",
        "body_atr_min": 1.00,
        "lookback": 60,
        "distance_atr_max": 0.30,
        "close_location": 0.15,
        "rr": 4.50,
    }
    config["config_id"] = config_id(config)
    return config


FC_PARITY_EXPECTED = {
    "PRIMARY": {
        # Exact row from uploaded:
        # audusd_h1_short_refinement_rr_confirmation.csv
        "trades": 158,
        "winners": 52,
        "win_rate_pct": 32.91139240506329,
        "pf": 1.6675616410006013,
        "total_r": 70.76153394606374,
        "expectancy_r": 0.4478578097852135,
        "max_dd_r": -11.0,
        "dev_pf": 1.7989459960867704,
        "dev_r": 54.328327733900394,
        "validation_pf": 1.4324527950569301,
        "validation_r": 16.43320621216334,
        "positive_eras": 4,
        "last5_r": 13.623363324324794,
        "last2_r": 0.13782540988423353,
    },
    "SWEEP_CONTROL": {
        # Exact RR3.5 row from uploaded:
        # audusd_h1_short_refinement_secondary_controls.csv
        "trades": 84,
        "pf": 1.3720690499383532,
        "total_r": 22.32414299630119,
        "validation_pf": 1.5382551919167795,
        "validation_r": 10.765103838335591,
        "last5_r": 9.927581921327935,
        "last2_r": 2.7993201787608513,
    },
    "OUTSIDE_CONTROL": {
        # Exact row from uploaded:
        # audusd_h1_short_refinement_secondary_controls.csv
        "trades": 107,
        "pf": 1.4098867450738546,
        "total_r": 33.20082635098223,
        "validation_pf": 1.508489626891367,
        "validation_r": 16.271668060523744,
        "last5_r": 8.7856660253948,
        "last2_r": -5.61752136752136,
    },
}


def fc_pack():
    with zipfile.ZipFile(
        FC_BUNDLE,
        "w",
        compression=zipfile.ZIP_DEFLATED,
    ) as archive:
        for path in FC_OUT.values():
            if os.path.exists(path):
                archive.write(
                    path,
                    arcname=os.path.basename(path),
                )


def fc_ensure_lb15(features):
    if 15 in features["prev_lows"]:
        return

    features["prev_lows"][15] = rolling_previous_extreme(
        features["low"],
        15,
        want_max=False,
    )
    features["prev_highs"][15] = rolling_previous_extreme(
        features["high"],
        15,
        want_max=True,
    )


def fc_slice_features(features, cutoff):
    stop = bisect_right(
        features["times"],
        cutoff,
    )

    sliced = {}

    for key, value in features.items():
        if isinstance(value, np.ndarray):
            sliced[key] = value[:stop].copy()

        elif key == "times":
            sliced[key] = value[:stop]

        elif key in ("prev_lows", "prev_highs"):
            sliced[key] = {
                lb: arr[:stop].copy()
                for lb, arr in value.items()
            }

        else:
            sliced[key] = value

    return sliced


def fc_parity_row(label, config, features, expected):
    indices = signal_indices(
        config,
        features,
    )
    row, _ = evaluate_candidate(
        config,
        features,
        indices,
    )

    checks = {
        "trades": row["full_trades"] == expected["trades"],
        "pf": abs(
            row["full_pf"] - expected["pf"]
        ) <= FC_PARITY_FLOAT_TOL,
        "total_r": abs(
            row["full_total_r"] - expected["total_r"]
        ) <= FC_PARITY_FLOAT_TOL,
    }

    if "winners" in expected:
        checks["winners"] = (
            row["full_winners"] == expected["winners"]
        )

    if "max_dd_r" in expected:
        checks["max_dd_r"] = abs(
            row["full_max_dd_r"] - expected["max_dd_r"]
        ) <= FC_PARITY_FLOAT_TOL

    if "validation_pf" in expected:
        checks["validation_pf"] = abs(
            row["validation_2018_plus_pf"]
            - expected["validation_pf"]
        ) <= FC_PARITY_FLOAT_TOL

    if "last5_r" in expected:
        checks["last5_r"] = abs(
            row["last5y_r"]
            - expected["last5_r"]
        ) <= FC_PARITY_FLOAT_TOL

    if "last2_r" in expected:
        checks["last2_r"] = abs(
            row["last2y_r"]
            - expected["last2_r"]
        ) <= FC_PARITY_FLOAT_TOL

    passed = all(checks.values())

    output = {
        "label": label,
        "config_id": config["config_id"],
        "expected_trades": expected["trades"],
        "actual_trades": row["full_trades"],
        "expected_pf": expected["pf"],
        "actual_pf": row["full_pf"],
        "expected_total_r": expected["total_r"],
        "actual_total_r": row["full_total_r"],
        "expected_validation_pf": expected.get("validation_pf"),
        "actual_validation_pf": row.get("validation_2018_plus_pf"),
        "expected_last5_r": expected.get("last5_r"),
        "actual_last5_r": row.get("last5y_r"),
        "expected_last2_r": expected.get("last2_r"),
        "actual_last2_r": row.get("last2y_r"),
        "float_tolerance": FC_PARITY_FLOAT_TOL,
        "checks_json": json.dumps(
            checks,
            sort_keys=True,
        ),
        "pass": passed,
    }

    if not passed:
        raise RuntimeError(
            f"Final-confirmation parity failed for {label}: "
            f"{json.dumps(output, default=str)}"
        )

    return output


def fc_extended_period_rows(config, trades):
    rows = detailed_period_rows(
        config,
        trades,
    )

    extra = [
        (
            "LAST_1Y",
            NOW - timedelta(days=365.25),
            None,
        ),
        (
            "LAST_3Y",
            NOW - timedelta(days=365.25 * 3),
            None,
        ),
    ]

    for name, start, end in extra:
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


def fc_serialise_trades(
    label,
    config,
    trades,
):
    output = []

    for trade in trades:
        row = dict(trade)
        row["strategy_label"] = label
        row["config_id"] = config["config_id"]
        row["signal_time"] = iso(
            row["signal_time"]
        )
        row["exit_time"] = iso(
            row["exit_time"]
        )
        output.append(row)

    return output


def fc_one_at_a_time_neighbours():
    """
    Predeclared one-factor-at-a-time robustness set around the frozen
    candidate. The primary appears once only.
    """
    specs = [
        (
            "PRIMARY",
            1.25, 1.50, 0.85, 15, 3.50,
            "Frozen primary candidate",
        ),

        (
            "BODY_DOWN",
            1.00, 1.50, 0.85, 15, 3.50,
            "Only body threshold relaxed",
        ),
        (
            "BODY_UP",
            1.50, 1.50, 0.85, 15, 3.50,
            "Only body threshold tightened",
        ),

        (
            "RANGE_DOWN",
            1.25, 1.25, 0.85, 15, 3.50,
            "Only range threshold relaxed",
        ),
        (
            "RANGE_UP",
            1.25, 1.75, 0.85, 15, 3.50,
            "Only range threshold tightened",
        ),

        (
            "COMPRESSION_TIGHTER",
            1.25, 1.50, 0.80, 15, 3.50,
            "Only compression threshold tightened",
        ),
        (
            "COMPRESSION_LOOSER",
            1.25, 1.50, 0.90, 15, 3.50,
            "Only compression threshold loosened",
        ),

        (
            "LB_DOWN",
            1.25, 1.50, 0.85, 10, 3.50,
            "Only breakout lookback shortened",
        ),
        (
            "LB_UP",
            1.25, 1.50, 0.85, 20, 3.50,
            "Only breakout lookback lengthened",
        ),

        (
            "RR_DOWN",
            1.25, 1.50, 0.85, 15, 3.00,
            "Only reward:risk reduced",
        ),
        (
            "RR_UP",
            1.25, 1.50, 0.85, 15, 4.00,
            "Only reward:risk increased",
        ),
    ]

    rows = []

    for (
        label,
        body,
        rng,
        compression,
        lb,
        rr,
        note,
    ) in specs:
        config = fc_primary_config(
            body=body,
            range_atr=rng,
            compression=compression,
            breakout_lb=lb,
            rr=rr,
        )
        rows.append({
            "neighbour_label": label,
            "note": note,
            "config": config,
        })

    return rows


def fc_neighbour_summary(rows):
    neighbours = [
        row for row in rows
        if row["neighbour_label"] != "PRIMARY"
    ]

    positive = [
        row for row in neighbours
        if row["full_total_r"] > 0
    ]
    split_positive = [
        row for row in neighbours
        if row["both_temporal_splits_positive"]
    ]
    validation_positive = [
        row for row in neighbours
        if row["validation_2018_plus_r"] > 0
    ]
    last5_positive = [
        row for row in neighbours
        if row["last5y_r"] > 0
    ]
    last2_positive = [
        row for row in neighbours
        if row["last2y_r"] > 0
    ]

    pfs = [
        row["full_pf"]
        for row in neighbours
    ]
    split_pfs = [
        row["min_temporal_split_pf"]
        for row in neighbours
    ]

    return [{
        "neighbours_excluding_primary":
            len(neighbours),
        "positive_full":
            len(positive),
        "positive_full_pct":
            pct(
                len(positive),
                len(neighbours),
            ),
        "both_temporal_splits_positive":
            len(split_positive),
        "both_temporal_splits_positive_pct":
            pct(
                len(split_positive),
                len(neighbours),
            ),
        "validation_positive":
            len(validation_positive),
        "validation_positive_pct":
            pct(
                len(validation_positive),
                len(neighbours),
            ),
        "last5_positive":
            len(last5_positive),
        "last5_positive_pct":
            pct(
                len(last5_positive),
                len(neighbours),
            ),
        "last2_positive":
            len(last2_positive),
        "last2_positive_pct":
            pct(
                len(last2_positive),
                len(neighbours),
            ),
        "median_full_pf":
            med(pfs),
        "minimum_full_pf":
            min(pfs),
        "median_min_temporal_split_pf":
            med(split_pfs),
        "minimum_min_temporal_split_pf":
            min(split_pfs),
    }]


def fc_summary_lookup(rows, config_id):
    for row in rows:
        if row["config_id"] == config_id:
            return row
    raise RuntimeError(
        f"Missing summary row for {config_id}"
    )


def fc_decision_row(
    primary_row,
    cost_rows,
    rolling_summary_rows,
    calendar_summary_rows,
    neighbour_summary_rows,
):
    cost2 = next(
        row for row in cost_rows
        if (
            row["config_id"]
            == primary_row["config_id"]
            and abs(
                row["cost_multiplier"] - 2.0
            ) < 1e-12
        )
    )

    rolling = {
        int(row["window_months"]): row
        for row in rolling_summary_rows
        if row["config_id"]
        == primary_row["config_id"]
    }

    calendar = fc_summary_lookup(
        calendar_summary_rows,
        primary_row["config_id"],
    )

    neighbours = neighbour_summary_rows[0]

    checks = {
        "sample_ge_100":
            primary_row["full_trades"] >= 100,

        "pf_ge_1_35":
            primary_row["full_pf"] >= 1.35,

        "both_temporal_splits_positive":
            bool(
                primary_row[
                    "both_temporal_splits_positive"
                ]
            ),

        "validation_pf_ge_1_25":
            primary_row[
                "validation_2018_plus_pf"
            ] >= 1.25,

        "four_of_four_eras_positive":
            primary_row["positive_eras"] == 4,

        "last5_positive":
            primary_row["last5y_r"] > 0,

        # Recent sample is marginal by design; require non-negative rather
        # than manufacturing a filter around 2024-2026.
        "last2_non_negative":
            primary_row["last2y_r"] >= 0,

        "cost_2x_pf_ge_1_20":
            cost2["profit_factor"] >= 1.20,

        "cost_2x_total_r_positive":
            cost2["total_r"] > 0,

        "rolling24_positive_pct_ge_70":
            rolling.get(
                24, {}
            ).get(
                "positive_active_windows_pct",
                0.0,
            ) >= 70.0,

        "rolling36_positive_pct_ge_75":
            rolling.get(
                36, {}
            ).get(
                "positive_active_windows_pct",
                0.0,
            ) >= 75.0,

        "calendar_positive_pct_ge_55":
            calendar[
                "positive_active_years_pct"
            ] >= 55.0,

        "all_one_factor_neighbours_profitable":
            neighbours[
                "positive_full_pct"
            ] >= 100.0,

        "neighbour_split_positive_pct_ge_80":
            neighbours[
                "both_temporal_splits_positive_pct"
            ] >= 80.0,
    }

    return {
        "candidate_id":
            FC_PRIMARY_ID,
        "config_id":
            primary_row["config_id"],
        "standalone_confirmation_pass":
            all(checks.values()),
        "checks_passed":
            sum(checks.values()),
        "checks_total":
            len(checks),
        "checks_json":
            json.dumps(
                checks,
                sort_keys=True,
            ),
        "full_trades":
            primary_row["full_trades"],
        "full_pf":
            primary_row["full_pf"],
        "full_total_r":
            primary_row["full_total_r"],
        "full_expectancy_r":
            primary_row[
                "full_expectancy_r"
            ],
        "full_max_dd_r":
            primary_row[
                "full_max_dd_r"
            ],
        "validation_2018_plus_pf":
            primary_row[
                "validation_2018_plus_pf"
            ],
        "validation_2018_plus_r":
            primary_row[
                "validation_2018_plus_r"
            ],
        "last5y_r":
            primary_row["last5y_r"],
        "last2y_r":
            primary_row["last2y_r"],
        "cost_2x_pf":
            cost2["profit_factor"],
        "cost_2x_total_r":
            cost2["total_r"],
        "rolling12_positive_pct":
            rolling.get(
                12, {}
            ).get(
                "positive_active_windows_pct",
                0.0,
            ),
        "rolling12_worst_r":
            rolling.get(
                12, {}
            ).get(
                "worst_r_active",
                0.0,
            ),
        "rolling24_positive_pct":
            rolling.get(
                24, {}
            ).get(
                "positive_active_windows_pct",
                0.0,
            ),
        "rolling24_worst_r":
            rolling.get(
                24, {}
            ).get(
                "worst_r_active",
                0.0,
            ),
        "rolling36_positive_pct":
            rolling.get(
                36, {}
            ).get(
                "positive_active_windows_pct",
                0.0,
            ),
        "rolling36_worst_r":
            rolling.get(
                36, {}
            ).get(
                "worst_r_active",
                0.0,
            ),
        "positive_calendar_year_pct":
            calendar[
                "positive_active_years_pct"
            ],
        "worst_calendar_year_r":
            calendar[
                "worst_active_year_r"
            ],
        "neighbour_positive_pct":
            neighbours[
                "positive_full_pct"
            ],
        "neighbour_split_positive_pct":
            neighbours[
                "both_temporal_splits_positive_pct"
            ],
        "next_step_if_pass":
            (
                "Exact current25 -> prospective26 portfolio-add test "
                "with AUD/USD LONG-vs-SHORT non-hedging conflicts"
            ),
    }


def run_fc_research():
    try:
        global STATUS
        STATUS = FC_STATUS

        FC_STATUS.update({
            "state": "fetching",
            "message": "Fetching AUD/USD H1 history",
        })

        h1 = fetch_history(
            "H1",
            REQUESTED_START,
            NOW,
            chunk_days=180,
        )

        if len(h1) < 100000:
            raise RuntimeError(
                f"Unexpectedly small H1 history: {len(h1)}"
            )

        write_csv(
            FC_OUT["coverage"],
            [{
                "pair": PAIR,
                "timeframe": "H1",
                "candles": len(h1),
                "first_candle_utc":
                    iso(h1[0]["time"]),
                "last_candle_utc":
                    iso(h1[-1]["time"]),
                "parity_cutoff_utc":
                    iso(FC_PARITY_CUTOFF),
            }],
        )

        FC_STATUS.update({
            "state": "features",
            "message": "Building frozen H1 feature set",
        })

        features = build_features(
            h1,
            "H1",
        )
        fc_ensure_lb15(
            features
        )

        primary = fc_primary_config()
        sweep = fc_sweep_control()
        outside = fc_outside_control()

        # ----------------------------------------------------
        # EXACT PARITY TO THE UPLOADED REFINEMENT
        # ----------------------------------------------------
        FC_STATUS.update({
            "state": "parity",
            "message": "Reproducing uploaded refinement anchors exactly",
        })

        parity_features = fc_slice_features(
            features,
            FC_PARITY_CUTOFF,
        )

        parity_rows = [
            fc_parity_row(
                "PRIMARY",
                primary,
                parity_features,
                FC_PARITY_EXPECTED["PRIMARY"],
            ),
            fc_parity_row(
                "SWEEP_CONTROL",
                sweep,
                parity_features,
                FC_PARITY_EXPECTED[
                    "SWEEP_CONTROL"
                ],
            ),
            fc_parity_row(
                "OUTSIDE_CONTROL",
                outside,
                parity_features,
                FC_PARITY_EXPECTED[
                    "OUTSIDE_CONTROL"
                ],
            ),
        ]

        write_csv(
            FC_OUT["parity"],
            parity_rows,
        )

        # ----------------------------------------------------
        # FULL CURRENT-HISTORY HEADLINE + DEEP DIAGNOSTICS
        # ----------------------------------------------------
        FC_STATUS.update({
            "state": "deep_confirmation",
            "message": "Running exact candidate and frozen controls",
        })

        named_configs = [
            (
                FC_PRIMARY_ID,
                "PRIMARY_CANDIDATE",
                primary,
            ),
            (
                FC_SWEEP_ID,
                "SECONDARY_CONTROL",
                sweep,
            ),
            (
                FC_OUTSIDE_ID,
                "ROBUSTNESS_CONTROL",
                outside,
            ),
        ]

        headline_rows = []
        all_period_rows = []
        all_cost_rows = []
        all_rolling_rows = []
        all_calendar_rows = []
        candidate_trade_rows = []
        control_trade_rows = []

        for (
            strategy_id,
            role,
            config,
        ) in named_configs:
            indices = signal_indices(
                config,
                features,
            )

            row, trades = evaluate_candidate(
                config,
                features,
                indices,
            )

            row["strategy_id"] = (
                strategy_id
            )
            row["confirmation_role"] = role
            headline_rows.append(row)

            all_period_rows.extend(
                fc_extended_period_rows(
                    config,
                    trades,
                )
            )

            all_cost_rows.extend(
                cost_stress_rows(
                    config,
                    features,
                    indices,
                )
            )

            all_rolling_rows.extend(
                rolling_rows(
                    config,
                    trades,
                )
            )

            all_calendar_rows.extend(
                calendar_rows(
                    config,
                    trades,
                )
            )

            serialised = (
                fc_serialise_trades(
                    strategy_id,
                    config,
                    trades,
                )
            )

            if role == "PRIMARY_CANDIDATE":
                candidate_trade_rows.extend(
                    serialised
                )
            else:
                control_trade_rows.extend(
                    serialised
                )

        write_csv(
            FC_OUT["headline"],
            headline_rows,
        )
        write_csv(
            FC_OUT["periods"],
            all_period_rows,
        )
        write_csv(
            FC_OUT["cost_stress"],
            all_cost_rows,
        )
        write_csv(
            FC_OUT["rolling"],
            all_rolling_rows,
        )
        write_csv(
            FC_OUT["calendar"],
            all_calendar_rows,
        )
        write_csv(
            FC_OUT["candidate_trades"],
            candidate_trade_rows,
        )
        write_csv(
            FC_OUT["control_trades"],
            control_trade_rows,
        )

        rolling_summary_rows = (
            rolling_summary(
                all_rolling_rows
            )
        )
        calendar_summary_rows = (
            calendar_summary(
                all_calendar_rows
            )
        )

        write_csv(
            FC_OUT["rolling_summary"],
            rolling_summary_rows,
        )
        write_csv(
            FC_OUT["calendar_summary"],
            calendar_summary_rows,
        )

        # ----------------------------------------------------
        # ONE-FACTOR-AT-A-TIME PARAMETER ROBUSTNESS
        # ----------------------------------------------------
        FC_STATUS.update({
            "state": "parameter_neighbours",
            "message": "Running predeclared one-factor parameter neighbours",
        })

        neighbour_rows = []

        for item in fc_one_at_a_time_neighbours():
            config = item["config"]
            indices = signal_indices(
                config,
                features,
            )

            row, _ = evaluate_candidate(
                config,
                features,
                indices,
            )

            row["neighbour_label"] = (
                item["neighbour_label"]
            )
            row["neighbour_note"] = (
                item["note"]
            )
            neighbour_rows.append(
                row
            )

        write_csv(
            FC_OUT["parameter_neighbours"],
            neighbour_rows,
        )

        neighbour_summary_rows = (
            fc_neighbour_summary(
                neighbour_rows
            )
        )

        write_csv(
            FC_OUT["neighbour_summary"],
            neighbour_summary_rows,
        )

        # ----------------------------------------------------
        # PREDECLARED FINAL STANDALONE SCREEN
        # ----------------------------------------------------
        primary_row = next(
            row for row in headline_rows
            if row["strategy_id"]
            == FC_PRIMARY_ID
        )

        decision_row = fc_decision_row(
            primary_row,
            all_cost_rows,
            rolling_summary_rows,
            calendar_summary_rows,
            neighbour_summary_rows,
        )

        write_csv(
            FC_OUT["decision"],
            [decision_row],
        )

        write_csv(
            FC_OUT["notes"],
            [
                {
                    "topic": "frozen_candidate",
                    "note": (
                        "AUD/USD H1 SHORT COMPRESSION_BREAKOUT: bearish; "
                        "body>=1.25 ATR14; range>=1.50 ATR14; prior-H1 ATR14/"
                        "prior20 mean<=0.85; close below previous15 low; RR3.50; "
                        "no weekday/session/HTF regime."
                    ),
                },
                {
                    "topic": "execution",
                    "note": (
                        "Historical fill = signal close -0.5 pip adverse; "
                        "stop = signal high +10 ticks; target uses reference "
                        "entry risk; pyramiding0; exit-candle re-entry eligible."
                    ),
                },
                {
                    "topic": "parity",
                    "note": (
                        "Primary plus frozen sweep/outside controls must reproduce "
                        "the uploaded refinement exactly through "
                        "2026-09-18 14:00 UTC before current-history confirmation."
                    ),
                },
                {
                    "topic": "neighbours",
                    "note": (
                        "Parameter robustness is one-factor-at-a-time only; "
                        "this is confirmation, not another optimisation grid."
                    ),
                },
                {
                    "topic": "recent_sample",
                    "note": (
                        "The candidate's uploaded last-2Y edge was approximately "
                        "flat (+0.138R). The confirmation deliberately requires "
                        "only non-negative last-2Y R; no context filter is added "
                        "to manufacture recent performance."
                    ),
                },
                {
                    "topic": "portfolio",
                    "note": (
                        "No current25 portfolio contribution is tested here. "
                        "A standalone pass only authorises the next research step: "
                        "exact current25 -> prospective26 portfolio-add testing "
                        "with same-pair opposite-direction non-hedging conflicts."
                    ),
                },
                {
                    "topic": "historical_not_forecast",
                    "note": (
                        "All outputs are historical backtest results, not forecasts."
                    ),
                },
            ],
        )

        FC_STATUS.update({
            "state": "packaging",
            "message": "Packaging final standalone confirmation",
        })

        fc_pack()

        FC_STATUS.update({
            "state": "complete",
            "message": "AUD/USD H1 SHORT final standalone confirmation complete",
            "h1_candles": len(h1),
            "parity_passes": sum(
                row["pass"]
                for row in parity_rows
            ),
            "parity_total": len(
                parity_rows
            ),
            "primary_trades":
                primary_row["full_trades"],
            "primary_pf":
                primary_row["full_pf"],
            "primary_total_r":
                primary_row["full_total_r"],
            "standalone_confirmation_pass":
                decision_row[
                    "standalone_confirmation_pass"
                ],
            "checks_passed":
                decision_row["checks_passed"],
            "checks_total":
                decision_row["checks_total"],
            "bundle": FC_BUNDLE,
            "orders_supported": False,
            "trading_enabled": False,
        })

    except Exception as error:
        import traceback

        FC_STATUS.update({
            "state": "error",
            "message": str(error),
            "error_type":
                type(error).__name__,
            "traceback":
                traceback.format_exc(),
            "orders_supported": False,
            "trading_enabled": False,
        })

        print(
            "AUDUSD H1 SHORT FINAL CONFIRMATION ERROR:",
            repr(error),
            flush=True,
        )


@app.route(
    "/audusd-h1-short-final-confirmation/status"
)
def fc_status():
    return jsonify(
        FC_STATUS
    )


@app.route(
    "/audusd-h1-short-final-confirmation/results"
)
def fc_results():
    if not os.path.exists(
        FC_BUNDLE
    ):
        return jsonify({
            "status": "not_ready",
            "state":
                FC_STATUS["state"],
            "message":
                FC_STATUS["message"],
        }), 404

    return send_file(
        os.path.abspath(
            FC_BUNDLE
        ),
        as_attachment=True,
        download_name=FC_BUNDLE,
    )


@app.route(
    "/audusd-h1-short-final-confirmation/info"
)
def fc_info():
    return jsonify({
        "service": (
            "AUD/USD H1 SHORT Final Standalone Confirmation"
        ),
        "read_only": True,
        "orders_supported": False,
        "candidate": {
            "family":
                "COMPRESSION_BREAKOUT",
            "body_atr_min": 1.25,
            "range_atr_min": 1.50,
            "compression_max": 0.85,
            "breakout_lookback": 15,
            "rr": 3.50,
            "weekday_filter": None,
            "session_filter": None,
            "htf_filter": None,
        },
        "parameter_confirmation":
            "one-factor-at-a-time only",
        "controls": [
            (
                "SWEEP_DISPLACEMENT body1.25 "
                "LB15 wick0.25 RR3.50"
            ),
            (
                "OUTSIDE_REVERSAL body1.00 "
                "LB60 distance0.30 close0.15 RR4.50"
            ),
        ],
        "next_step_if_pass": (
            "current25 -> prospective26 exact portfolio-add test"
        ),
        "routes": [
            "/audusd-h1-short-final-confirmation/status",
            "/audusd-h1-short-final-confirmation/results",
            "/audusd-h1-short-final-confirmation/info",
        ],
    })



# ============================================================
# AUD/USD H1 SHORT — COMPLEMENTARY SWEEP RESEARCH
# ============================================================
#
# PURPOSE
# -------
# Improve the RETURN SHAPE of the frozen AUD/USD H1 SHORT compression
# strategy without changing that core.
#
# The primary objective is NOT headline PF optimisation. It is to test
# whether a structurally different high-sweep displacement trigger can:
#   - add useful frequency,
#   - repair weak calendar/rolling periods,
#   - materially improve 12/24/36M consistency,
#   - reduce worst rolling troughs,
# while preserving long-history / validation robustness.
#
# ============================================================
# FROZEN CORE — NEVER OPTIMISED IN THIS RUN
# ============================================================
#
# COMPRESSION_BREAKOUT
# bearish H1 candle
# body >= 1.25 ATR14
# range >= 1.50 ATR14
# previous H1 ATR14 / previous20 ATR mean <= 0.85
# close below previous 15-H1 low, current excluded
# RR 3.50
# stop = signal high + 10 ticks
# H1 historical adverse fill = 0.5 pip
# no weekday/session/HTF filter
#
# Exact uploaded parity anchor through 2026-09-18 14:00 UTC:
#   158 trades
#   52 winners
#   PF 1.6675616410006013
#   +70.76153394606374R
#
# ============================================================
# COMPLEMENT FAMILY
# ============================================================
#
# SWEEP_DISPLACEMENT SHORT:
#   bearish H1 candle
#   high > previous N-bar high, current excluded
#   close < previous H1 candle low
#   upper wick/body >= threshold
#   body >= threshold * ATR14
#   stop = signal high + 10 ticks
#
# Stage 1 geometry, fixed RR3.50:
#   body >= 1.00 / 1.25 / 1.50 ATR
#   sweep LB = 10 / 15 / 20 / 30
#   upper wick/body >= 0.10 / 0.25 / 0.40
#   36 geometries
#
# Stage 2 RR confirmation on the strongest 12 geometries:
#   RR 3.00 / 3.50 / 4.00 / 4.50
#
# Stage 3 SINGLE-FACTOR context confirmation on the strongest 8:
#   NONE
#   prior ~4H rally momentum >= 0.50 / 1.00 / 1.50 ATR
#   prior strictly completed H4 close < EMA100 / EMA200
#   prior strictly completed D close < EMA100 / EMA200
#   London 07:00-15:59
#   New York 08:00-16:59
#
# NO context interactions are mined.
#
# ============================================================
# UNION EXECUTION
# ============================================================
#
# Core + complement share exact strategy-level pyramiding=0.
# Core has same-candle priority.
# Half-open overlap logic:
#   accepted trade blocks signals while
#       signal_index < exit_index
#   but a signal ON exit_index is eligible.
#
# This is an exact raw-signal union rerun; it does NOT merge the already-
# accepted standalone ledgers.
#
# ============================================================
# SELECTION EMPHASIS
# ============================================================
#
# Baseline core rolling targets from the uploaded final confirmation:
#   12M positive active windows ~66.8%
#   24M ~75.7%
#   36M ~86.5%
#   worst 24M ~-7.22R
#   worst 36M ~-7.80R
#
# A serious combined finalist should approximately:
#   combined trades >= 180
#   marginal complement accepted trades >= 25
#   combined total R > frozen core
#   complement marginal accepted R > 0
#   12M positive >= 70%
#   24M positive >= 85%
#   36M positive >= 92%
#   worst 24M >= -6.5R
#   worst 36M >= -6.5R
#   both temporal splits positive
#   2018+ PF >= 1.30
#   last 5Y positive
#   2x-cost combined PF >= 1.30
#
# Those are confirmation gates, not optimisation objectives.
#
# READ ONLY. NEVER SENDS ORDERS.
# ============================================================

CR_STATUS = {
    "state": "not_started",
    "message": "AUD/USD H1 SHORT complementary sweep research not started",
    "pair": PAIR,
    "timeframe": "H1",
    "side": "SHORT",
    "orders_supported": False,
    "trading_enabled": False,
}

CR_BUNDLE = "AUDUSD_H1_SHORT_COMPLEMENT_SWEEP_RESULTS.zip"

CR_OUT = {
    "coverage": "audusd_h1_short_complement_coverage.csv",
    "parity": "audusd_h1_short_complement_parity.csv",
    "core_baseline": "audusd_h1_short_complement_core_baseline.csv",
    "stage1": "audusd_h1_short_complement_stage1_geometry.csv",
    "stage1_shortlist": "audusd_h1_short_complement_stage1_shortlist.csv",
    "stage2": "audusd_h1_short_complement_stage2_rr.csv",
    "stage2_shortlist": "audusd_h1_short_complement_stage2_shortlist.csv",
    "stage3": "audusd_h1_short_complement_stage3_context.csv",
    "finalists": "audusd_h1_short_complement_finalists.csv",
    "decision": "audusd_h1_short_complement_decision.csv",
    "periods": "audusd_h1_short_complement_periods.csv",
    "cost_stress": "audusd_h1_short_complement_cost_stress.csv",
    "rolling": "audusd_h1_short_complement_rolling.csv",
    "rolling_summary": "audusd_h1_short_complement_rolling_summary.csv",
    "calendar": "audusd_h1_short_complement_calendar.csv",
    "calendar_summary": "audusd_h1_short_complement_calendar_summary.csv",
    "trades": "audusd_h1_short_complement_finalist_trades.csv",
    "overlaps": "audusd_h1_short_complement_overlap_summary.csv",
    "notes": "audusd_h1_short_complement_notes.csv",
}

CR_PARITY_CUTOFF = datetime(
    2026, 9, 18, 14, 0,
    tzinfo=timezone.utc,
)

CR_STAGE1_RR = 3.50
CR_BODY_GRID = [1.00, 1.25, 1.50]
CR_LB_GRID = [10, 15, 20, 30]
CR_WICK_GRID = [0.10, 0.25, 0.40]
CR_RR_GRID = [3.00, 3.50, 4.00, 4.50]

CR_STAGE1_KEEP = 12
CR_STAGE2_KEEP = 8
CR_FINAL_KEEP = 12

CR_HTF_WARMUP = datetime(
    1999, 1, 1,
    tzinfo=timezone.utc,
)

CR_CONTEXTS = [
    "NONE",
    "PRIOR4H_MOM_GE_050",
    "PRIOR4H_MOM_GE_100",
    "PRIOR4H_MOM_GE_150",
    "H4_CLOSE_LT_EMA100",
    "H4_CLOSE_LT_EMA200",
    "D_CLOSE_LT_EMA100",
    "D_CLOSE_LT_EMA200",
    "SESSION_LONDON_07_16",
    "SESSION_NY_08_17",
]


def cr_pack():
    with zipfile.ZipFile(
        CR_BUNDLE,
        "w",
        zipfile.ZIP_DEFLATED,
    ) as archive:
        for path in CR_OUT.values():
            if os.path.exists(path):
                archive.write(
                    path,
                    arcname=os.path.basename(path),
                )


def cr_core_config():
    return fc_primary_config(
        body=1.25,
        range_atr=1.50,
        compression=0.85,
        breakout_lb=15,
        rr=3.50,
    )


def cr_sweep_config(
    body,
    lookback,
    wick,
    rr,
    context_id="NONE",
):
    config = {
        "timeframe": "H1",
        "side": "SHORT",
        "family": "SWEEP_DISPLACEMENT",
        "body_atr_min": float(body),
        "lookback": int(lookback),
        "wick_body_min": float(wick),
        "rr": float(rr),
        "context_id": context_id,
    }

    base_for_id = dict(config)
    base_for_id.pop(
        "context_id",
        None,
    )
    config["base_config_id"] = config_id(
        base_for_id
    )
    config["config_id"] = (
        config["base_config_id"]
        + f"|context={context_id}"
    )
    return config


def cr_union_config(candidate):
    return {
        "config_id": (
            "AUD_USD_H1_SHORT_CORE_PLUS_SWEEP|"
            + candidate["config_id"]
        ),
        "timeframe": "H1",
        "side": "SHORT",
        "family": "COMPRESSION_PLUS_SWEEP",
    }


def cr_ensure_lookbacks(features):
    needed = set(
        CR_LB_GRID
        + [15]
    )

    for lookback in sorted(needed):
        if lookback not in features["prev_lows"]:
            features["prev_lows"][lookback] = (
                rolling_previous_extreme(
                    features["low"],
                    lookback,
                    want_max=False,
                )
            )

        if lookback not in features["prev_highs"]:
            features["prev_highs"][lookback] = (
                rolling_previous_extreme(
                    features["high"],
                    lookback,
                    want_max=True,
                )
            )


def cr_base_signal_config(candidate):
    result = dict(candidate)
    result.pop(
        "context_id",
        None,
    )
    result.pop(
        "base_config_id",
        None,
    )
    result["config_id"] = config_id(
        result
    )
    return result


def cr_prior4h_momentum(features):
    close = features["close"]
    atr = features["atr"]

    output = np.full(
        len(close),
        np.nan,
        dtype=float,
    )

    # Signal is evaluated on current H1 candle. Use only completed candles:
    # approximately prior 4H = previous close minus close four H1 bars earlier.
    for i in range(
        5,
        len(close),
    ):
        if (
            np.isfinite(atr[i])
            and atr[i] > 0
        ):
            output[i] = (
                close[i - 1]
                - close[i - 5]
            ) / atr[i]

    return output


def cr_context_mask(
    context_id,
    features,
    htf_cache,
    prior4h_momentum,
):
    n = len(
        features["times"]
    )

    if context_id == "NONE":
        return np.ones(
            n,
            dtype=bool,
        )

    if context_id == "PRIOR4H_MOM_GE_050":
        return (
            np.isfinite(
                prior4h_momentum
            )
            & (
                prior4h_momentum
                >= 0.50
            )
        )

    if context_id == "PRIOR4H_MOM_GE_100":
        return (
            np.isfinite(
                prior4h_momentum
            )
            & (
                prior4h_momentum
                >= 1.00
            )
        )

    if context_id == "PRIOR4H_MOM_GE_150":
        return (
            np.isfinite(
                prior4h_momentum
            )
            & (
                prior4h_momentum
                >= 1.50
            )
        )

    aliases = {
        "H4_CLOSE_LT_EMA100":
            "H4_CLOSE_LT_EMA100",
        "H4_CLOSE_LT_EMA200":
            "H4_CLOSE_LT_EMA200",
        "D_CLOSE_LT_EMA100":
            "D_CLOSE_LT_EMA100",
        "D_CLOSE_LT_EMA200":
            "D_CLOSE_LT_EMA200",
        "SESSION_LONDON_07_16":
            "SESSION_LONDON_07_16",
        "SESSION_NY_08_17":
            "SESSION_NY_08_17",
    }

    if context_id in aliases:
        return rf_context_mask(
            aliases[context_id],
            htf_cache,
        )

    raise ValueError(
        f"Unknown complement context: {context_id}"
    )


def cr_filter_indices(
    raw_indices,
    context_id,
    features,
    htf_cache,
    prior4h_momentum,
):
    raw_indices = np.asarray(
        raw_indices,
        dtype=int,
    )

    if not len(raw_indices):
        return raw_indices

    mask = cr_context_mask(
        context_id,
        features,
        htf_cache,
        prior4h_momentum,
    )

    return raw_indices[
        mask[raw_indices]
    ]


def cr_trade_from_signal(
    trigger_name,
    config,
    features,
    signal_index,
    cost_multiplier=1.0,
):
    side = "SHORT"
    rr = float(
        config["rr"]
    )

    cost_pips = cost_pips_for(
        "H1",
        multiplier=cost_multiplier,
    )
    adverse_cost = (
        cost_pips
        * PIP
    )

    high = features["high"]
    close = features["close"]
    times = features["times"]

    reference_entry = float(
        close[signal_index]
    )
    stop = (
        float(
            high[signal_index]
        )
        + STOP_BUFFER_TICKS
        * TICK
    )
    reference_risk = (
        stop
        - reference_entry
    )
    fill = (
        reference_entry
        - adverse_cost
    )
    actual_risk = (
        stop
        - fill
    )

    if (
        reference_risk <= 0
        or actual_risk <= 0
    ):
        return None

    target = (
        reference_entry
        - rr
        * reference_risk
    )

    exit_index = find_exit_index(
        features,
        signal_index,
        stop,
        target,
        side,
    )

    if exit_index is None:
        return None

    reason = determine_exit_reason(
        features,
        exit_index,
        stop,
        target,
        side,
    )

    if reason is None:
        raise RuntimeError(
            "Combined union exit reason was None"
        )

    exit_price = (
        target
        if reason == "TARGET"
        else stop
    )

    realised_r = (
        fill
        - exit_price
    ) / actual_risk

    return {
        "signal_index":
            int(signal_index),
        "exit_index":
            int(exit_index),
        "signal_time":
            times[signal_index],
        "exit_time":
            times[exit_index],
        "side": "SHORT",
        "timeframe": "H1",
        "family":
            config["family"],
        "trigger_id":
            trigger_name,
        "trigger_config_id":
            config["config_id"],
        "rr": rr,
        "cost_pips":
            cost_pips,
        "reference_entry":
            reference_entry,
        "historical_fill":
            fill,
        "stop": stop,
        "target": target,
        "exit_reason":
            reason,
        "result_r":
            realised_r,
        "duration_bars":
            exit_index
            - signal_index,
    }


def cr_union_backtest(
    core_config,
    candidate_config,
    features,
    core_raw_indices,
    candidate_raw_indices,
    cost_multiplier=1.0,
):
    """
    Exact raw-signal union:
      - CORE priority on same candle.
      - strategy-level pyramiding=0.
      - half-open [signal_index, exit_index) blocking.
      - signal on exit_index is eligible.
    """
    core_raw_indices = np.asarray(
        core_raw_indices,
        dtype=int,
    )
    candidate_raw_indices = np.asarray(
        candidate_raw_indices,
        dtype=int,
    )

    events = []

    for index in core_raw_indices:
        events.append(
            (
                int(index),
                0,
                "CORE_COMPRESSION",
                core_config,
            )
        )

    for index in candidate_raw_indices:
        events.append(
            (
                int(index),
                1,
                "SWEEP_COMPLEMENT",
                candidate_config,
            )
        )

    events.sort(
        key=lambda item: (
            item[0],
            item[1],
        )
    )

    event_indices = [
        item[0]
        for item in events
    ]

    trades = []
    rejected = []

    accepted_core = 0
    accepted_candidate = 0
    rejected_core_overlap = 0
    rejected_candidate_overlap = 0
    same_candle_candidate_rejected = 0

    pointer = 0

    while pointer < len(events):
        (
            signal_index,
            _priority,
            trigger_name,
            config,
        ) = events[pointer]

        trade = cr_trade_from_signal(
            trigger_name,
            config,
            features,
            signal_index,
            cost_multiplier=cost_multiplier,
        )

        if trade is None:
            # If the signal is so late that it remains open at end of data,
            # no later event can complete either.
            break

        if trigger_name == "CORE_COMPRESSION":
            accepted_core += 1
        else:
            accepted_candidate += 1

        trades.append(
            trade
        )

        next_pointer = bisect_left(
            event_indices,
            trade["exit_index"],
            lo=pointer + 1,
        )

        skipped = events[
            pointer + 1:
            next_pointer
        ]

        for (
            skipped_index,
            _skipped_priority,
            skipped_trigger,
            skipped_config,
        ) in skipped:
            same_candle = (
                skipped_index
                == signal_index
            )

            if skipped_trigger == "CORE_COMPRESSION":
                rejected_core_overlap += 1
            else:
                rejected_candidate_overlap += 1

            if (
                same_candle
                and trigger_name
                == "CORE_COMPRESSION"
                and skipped_trigger
                == "SWEEP_COMPLEMENT"
            ):
                same_candle_candidate_rejected += 1

            rejected.append({
                "accepted_trigger":
                    trigger_name,
                "accepted_signal_index":
                    signal_index,
                "accepted_signal_time":
                    features["times"][
                        signal_index
                    ],
                "accepted_exit_index":
                    trade["exit_index"],
                "accepted_exit_time":
                    trade["exit_time"],
                "rejected_trigger":
                    skipped_trigger,
                "rejected_config_id":
                    skipped_config["config_id"],
                "rejected_signal_index":
                    skipped_index,
                "rejected_signal_time":
                    features["times"][
                        skipped_index
                    ],
                "same_candle":
                    same_candle,
            })

        pointer = (
            next_pointer
        )

    audit = {
        "core_raw_signals":
            len(core_raw_indices),
        "candidate_raw_signals":
            len(candidate_raw_indices),
        "accepted_core":
            accepted_core,
        "accepted_candidate":
            accepted_candidate,
        "accepted_total":
            len(trades),
        "rejected_core_overlap":
            rejected_core_overlap,
        "rejected_candidate_overlap":
            rejected_candidate_overlap,
        "same_candle_candidate_rejected":
            same_candle_candidate_rejected,
    }

    return (
        trades,
        rejected,
        audit,
    )


def cr_era_metrics(trades):
    eras = [
        (
            "2002_2007",
            datetime(
                2002, 1, 1,
                tzinfo=timezone.utc,
            ),
            datetime(
                2008, 1, 1,
                tzinfo=timezone.utc,
            ),
        ),
        (
            "2008_2013",
            datetime(
                2008, 1, 1,
                tzinfo=timezone.utc,
            ),
            datetime(
                2014, 1, 1,
                tzinfo=timezone.utc,
            ),
        ),
        (
            "2014_2019",
            datetime(
                2014, 1, 1,
                tzinfo=timezone.utc,
            ),
            datetime(
                2020, 1, 1,
                tzinfo=timezone.utc,
            ),
        ),
        (
            "2020_NOW",
            datetime(
                2020, 1, 1,
                tzinfo=timezone.utc,
            ),
            None,
        ),
    ]

    output = {}
    positive = 0

    for name, start, end in eras:
        result = period_metrics(
            trades,
            start,
            end,
        )
        output[
            f"era_{name}_trades"
        ] = result["trades"]
        output[
            f"era_{name}_pf"
        ] = result[
            "profit_factor"
        ]
        output[
            f"era_{name}_r"
        ] = result["total_r"]

        if (
            result["trades"] > 0
            and result["total_r"] > 0
        ):
            positive += 1

    output["positive_eras"] = (
        positive
    )
    return output


def cr_rolling_summary_for(
    combined_config,
    trades,
):
    rows = rolling_rows(
        combined_config,
        trades,
    )
    summary = rolling_summary(
        rows
    )

    by_months = {
        int(row["window_months"]):
            row
        for row in summary
    }

    result = {}

    for months in (
        12, 24, 36
    ):
        row = by_months.get(
            months,
            {},
        )
        result[
            f"rolling{months}_positive_pct"
        ] = row.get(
            "positive_active_windows_pct",
            0.0,
        )
        result[
            f"rolling{months}_worst_r"
        ] = row.get(
            "worst_r_active",
            0.0,
        )
        result[
            f"rolling{months}_median_r"
        ] = row.get(
            "median_r_active",
            0.0,
        )

    return result


def cr_calendar_summary_for(
    combined_config,
    trades,
):
    rows = calendar_rows(
        combined_config,
        trades,
    )
    summary = calendar_summary(
        rows
    )

    if not summary:
        return {
            "positive_calendar_year_pct":
                0.0,
            "positive_calendar_years":
                0,
            "active_calendar_years":
                0,
            "worst_calendar_year_r":
                0.0,
        }

    row = summary[0]

    return {
        "positive_calendar_year_pct":
            row[
                "positive_active_years_pct"
            ],
        "positive_calendar_years":
            row[
                "positive_active_years"
            ],
        "active_calendar_years":
            row[
                "active_completed_years"
            ],
        "worst_calendar_year_r":
            row[
                "worst_active_year_r"
            ],
    }


def cr_summary(
    core_config,
    candidate_config,
    features,
    core_raw,
    candidate_raw,
    core_standalone_trades,
    core_baseline,
    cost_multiplier=1.0,
):
    combined_config = (
        cr_union_config(
            candidate_config
        )
    )

    (
        combined_trades,
        rejected,
        audit,
    ) = cr_union_backtest(
        core_config,
        candidate_config,
        features,
        core_raw,
        candidate_raw,
        cost_multiplier=cost_multiplier,
    )

    combined = metrics(
        combined_trades
    )

    complement_trades = [
        trade
        for trade in combined_trades
        if trade["trigger_id"]
        == "SWEEP_COMPLEMENT"
    ]

    union_core_trades = [
        trade
        for trade in combined_trades
        if trade["trigger_id"]
        == "CORE_COMPRESSION"
    ]

    complement_metrics = metrics(
        complement_trades
    )

    dev = period_metrics(
        combined_trades,
        None,
        VALIDATION_START,
    )
    validation = period_metrics(
        combined_trades,
        VALIDATION_START,
        None,
    )

    last5 = period_metrics(
        combined_trades,
        NOW
        - timedelta(
            days=365.25 * 5
        ),
        None,
    )
    last2 = period_metrics(
        combined_trades,
        NOW
        - timedelta(
            days=365.25 * 2
        ),
        None,
    )

    rolling_values = (
        cr_rolling_summary_for(
            combined_config,
            combined_trades,
        )
    )
    calendar_values = (
        cr_calendar_summary_for(
            combined_config,
            combined_trades,
        )
    )

    core_standalone_signals = {
        trade["signal_index"]:
            trade
        for trade
        in core_standalone_trades
    }

    union_core_signals = {
        trade["signal_index"]:
            trade
        for trade
        in union_core_trades
    }

    displaced = (
        set(
            core_standalone_signals
        )
        - set(
            union_core_signals
        )
    )

    newly_eligible = (
        set(
            union_core_signals
        )
        - set(
            core_standalone_signals
        )
    )

    displaced_r = sum(
        core_standalone_signals[
            index
        ]["result_r"]
        for index in displaced
    )

    newly_eligible_r = sum(
        union_core_signals[
            index
        ]["result_r"]
        for index in newly_eligible
    )

    row = {
        "candidate_config_id":
            candidate_config[
                "config_id"
            ],
        "base_config_id":
            candidate_config[
                "base_config_id"
            ],
        "context_id":
            candidate_config[
                "context_id"
            ],
        "body_atr_min":
            candidate_config[
                "body_atr_min"
            ],
        "lookback":
            candidate_config[
                "lookback"
            ],
        "wick_body_min":
            candidate_config[
                "wick_body_min"
            ],
        "rr":
            candidate_config["rr"],
        "cost_multiplier":
            cost_multiplier,

        **audit,

        "combined_trades":
            combined["trades"],
        "combined_winners":
            combined["winners"],
        "combined_win_rate_pct":
            combined[
                "win_rate_pct"
            ],
        "combined_pf":
            combined[
                "profit_factor"
            ],
        "combined_total_r":
            combined["total_r"],
        "combined_expectancy_r":
            combined[
                "expectancy_r"
            ],
        "combined_max_dd_r":
            combined[
                "max_drawdown_r"
            ],
        "combined_longest_loss_streak":
            combined[
                "longest_losing_streak"
            ],

        "marginal_accepted_trades":
            complement_metrics[
                "trades"
            ],
        "marginal_winners":
            complement_metrics[
                "winners"
            ],
        "marginal_pf":
            complement_metrics[
                "profit_factor"
            ],
        "marginal_total_r":
            complement_metrics[
                "total_r"
            ],
        "marginal_expectancy_r":
            complement_metrics[
                "expectancy_r"
            ],
        "marginal_max_dd_r":
            complement_metrics[
                "max_drawdown_r"
            ],

        "core_standalone_accepted":
            len(
                core_standalone_trades
            ),
        "core_union_accepted":
            len(
                union_core_trades
            ),
        "core_displaced_count":
            len(displaced),
        "core_displaced_r":
            displaced_r,
        "core_newly_eligible_count":
            len(newly_eligible),
        "core_newly_eligible_r":
            newly_eligible_r,

        "delta_total_r_vs_core":
            combined["total_r"]
            - core_baseline[
                "full_total_r"
            ],
        "delta_pf_vs_core":
            combined[
                "profit_factor"
            ]
            - core_baseline[
                "full_pf"
            ],
        "delta_max_dd_r_vs_core":
            combined[
                "max_drawdown_r"
            ]
            - core_baseline[
                "full_max_dd_r"
            ],

        "dev_trades":
            dev["trades"],
        "dev_pf":
            dev["profit_factor"],
        "dev_r":
            dev["total_r"],
        "validation_trades":
            validation["trades"],
        "validation_pf":
            validation[
                "profit_factor"
            ],
        "validation_r":
            validation["total_r"],
        "both_temporal_splits_positive":
            bool(
                dev["trades"] > 0
                and validation[
                    "trades"
                ] > 0
                and dev["total_r"] > 0
                and validation[
                    "total_r"
                ] > 0
            ),

        "last5y_trades":
            last5["trades"],
        "last5y_pf":
            last5[
                "profit_factor"
            ],
        "last5y_r":
            last5["total_r"],
        "last2y_trades":
            last2["trades"],
        "last2y_pf":
            last2[
                "profit_factor"
            ],
        "last2y_r":
            last2["total_r"],

        **cr_era_metrics(
            combined_trades
        ),
        **rolling_values,
        **calendar_values,
    }

    for months in (
        12, 24, 36
    ):
        row[
            f"delta_rolling{months}_positive_pct"
        ] = (
            row[
                f"rolling{months}_positive_pct"
            ]
            - core_baseline[
                f"rolling{months}_positive_pct"
            ]
        )
        row[
            f"delta_rolling{months}_worst_r"
        ] = (
            row[
                f"rolling{months}_worst_r"
            ]
            - core_baseline[
                f"rolling{months}_worst_r"
            ]
        )

    return (
        row,
        combined_trades,
        rejected,
    )


def cr_rank_key(row):
    """
    Rolling consistency first, then robustness / marginal contribution.
    """
    return (
        1 if row[
            "both_temporal_splits_positive"
        ] else 0,
        1 if row[
            "marginal_total_r"
        ] > 0 else 0,
        row[
            "rolling36_positive_pct"
        ],
        row[
            "rolling24_positive_pct"
        ],
        row[
            "rolling12_positive_pct"
        ],
        row[
            "rolling36_worst_r"
        ],
        row[
            "rolling24_worst_r"
        ],
        row[
            "validation_pf"
        ],
        row[
            "delta_total_r_vs_core"
        ],
        row[
            "marginal_accepted_trades"
        ],
    )


def cr_stage1_configs():
    configs = []

    for body in CR_BODY_GRID:
        for lookback in CR_LB_GRID:
            for wick in CR_WICK_GRID:
                configs.append(
                    cr_sweep_config(
                        body,
                        lookback,
                        wick,
                        CR_STAGE1_RR,
                        "NONE",
                    )
                )

    return configs


def cr_select_stage1(rows):
    eligible = [
        row for row in rows
        if (
            row[
                "marginal_accepted_trades"
            ] >= 20
            and row[
                "marginal_total_r"
            ] > 0
            and row[
                "both_temporal_splits_positive"
            ]
            and row[
                "positive_eras"
            ] >= 3
            and row[
                "last5y_r"
            ] > 0
        )
    ]

    pool = (
        eligible
        if eligible
        else rows
    )

    selected = sorted(
        pool,
        key=cr_rank_key,
        reverse=True,
    )[
        :CR_STAGE1_KEEP
    ]

    # Force the known sweep control geometry.
    anchor_key = (
        1.25,
        15,
        0.25,
        3.50,
        "NONE",
    )

    if not any(
        (
            row["body_atr_min"],
            int(row["lookback"]),
            row["wick_body_min"],
            row["rr"],
            row["context_id"],
        ) == anchor_key
        for row in selected
    ):
        anchor = next(
            (
                row
                for row in rows
                if (
                    row["body_atr_min"],
                    int(row["lookback"]),
                    row["wick_body_min"],
                    row["rr"],
                    row["context_id"],
                ) == anchor_key
            ),
            None,
        )

        if anchor is not None:
            if len(
                selected
            ) >= CR_STAGE1_KEEP:
                selected[-1] = anchor
            else:
                selected.append(
                    anchor
                )

    return selected


def cr_select_stage2(rows):
    eligible = [
        row for row in rows
        if (
            row[
                "marginal_accepted_trades"
            ] >= 20
            and row[
                "marginal_total_r"
            ] > 0
            and row[
                "both_temporal_splits_positive"
            ]
            and row[
                "positive_eras"
            ] >= 3
            and row[
                "last5y_r"
            ] > 0
        )
    ]

    pool = (
        eligible
        if eligible
        else rows
    )

    return sorted(
        pool,
        key=cr_rank_key,
        reverse=True,
    )[
        :CR_STAGE2_KEEP
    ]


def cr_deep_gate(
    row,
    cost2_row,
):
    checks = {
        "combined_trades_ge_180":
            row[
                "combined_trades"
            ] >= 180,

        "marginal_accepted_ge_25":
            row[
                "marginal_accepted_trades"
            ] >= 25,

        "combined_total_r_beats_core":
            row[
                "delta_total_r_vs_core"
            ] > 0,

        "marginal_total_r_positive":
            row[
                "marginal_total_r"
            ] > 0,

        "marginal_pf_ge_1_10":
            row[
                "marginal_pf"
            ] >= 1.10,

        "both_temporal_splits_positive":
            bool(
                row[
                    "both_temporal_splits_positive"
                ]
            ),

        "validation_pf_ge_1_30":
            row[
                "validation_pf"
            ] >= 1.30,

        "positive_eras_ge_3":
            row[
                "positive_eras"
            ] >= 3,

        "last5_positive":
            row[
                "last5y_r"
            ] > 0,

        "rolling12_ge_70":
            row[
                "rolling12_positive_pct"
            ] >= 70.0,

        "rolling24_ge_85":
            row[
                "rolling24_positive_pct"
            ] >= 85.0,

        "rolling36_ge_92":
            row[
                "rolling36_positive_pct"
            ] >= 92.0,

        "worst24_ge_minus_6_5":
            row[
                "rolling24_worst_r"
            ] >= -6.5,

        "worst36_ge_minus_6_5":
            row[
                "rolling36_worst_r"
            ] >= -6.5,

        "calendar_positive_pct_ge_60":
            row[
                "positive_calendar_year_pct"
            ] >= 60.0,

        "core_displaced_le_20":
            row[
                "core_displaced_count"
            ] <= 20,

        "cost2_pf_ge_1_30":
            cost2_row[
                "combined_pf"
            ] >= 1.30,

        "cost2_total_r_positive":
            cost2_row[
                "combined_total_r"
            ] > 0,
    }

    return {
        "deep_pass":
            all(
                checks.values()
            ),
        "checks_passed":
            sum(
                checks.values()
            ),
        "checks_total":
            len(checks),
        "checks_json":
            json.dumps(
                checks,
                sort_keys=True,
            ),
    }


def cr_serialise_trade(
    trade,
    finalist_rank,
    candidate_config_id,
):
    row = dict(
        trade
    )
    row[
        "finalist_rank"
    ] = finalist_rank
    row[
        "candidate_config_id"
    ] = candidate_config_id
    row[
        "signal_time"
    ] = iso(
        row["signal_time"]
    )
    row[
        "exit_time"
    ] = iso(
        row["exit_time"]
    )
    return row


def cr_serialise_rejection(
    row,
    finalist_rank,
    candidate_config_id,
):
    output = dict(
        row
    )
    output[
        "finalist_rank"
    ] = finalist_rank
    output[
        "candidate_config_id"
    ] = candidate_config_id
    output[
        "accepted_signal_time"
    ] = iso(
        output[
            "accepted_signal_time"
        ]
    )
    output[
        "accepted_exit_time"
    ] = iso(
        output[
            "accepted_exit_time"
        ]
    )
    output[
        "rejected_signal_time"
    ] = iso(
        output[
            "rejected_signal_time"
        ]
    )
    return output


def cr_period_rows(
    combined_config,
    trades,
):
    rows = detailed_period_rows(
        combined_config,
        trades,
    )

    for (
        name,
        start,
        end,
    ) in [
        (
            "LAST_1Y",
            NOW
            - timedelta(
                days=365.25
            ),
            None,
        ),
        (
            "LAST_3Y",
            NOW
            - timedelta(
                days=365.25
                * 3
            ),
            None,
        ),
    ]:
        result = period_metrics(
            trades,
            start,
            end,
        )
        rows.append({
            "config_id":
                combined_config[
                    "config_id"
                ],
            "timeframe": "H1",
            "side": "SHORT",
            "family":
                "COMPRESSION_PLUS_SWEEP",
            "period": name,
            **result,
        })

    return rows


def run_cr_research():
    try:
        global STATUS
        STATUS = CR_STATUS

        CR_STATUS.update({
            "state": "fetching",
            "message": "Fetching AUD/USD H1/H4/D history",
        })

        h1 = fetch_history(
            "H1",
            REQUESTED_START,
            NOW,
            chunk_days=180,
        )

        h4 = fetch_history(
            "H4",
            CR_HTF_WARMUP,
            NOW,
            chunk_days=700,
        )

        daily = fetch_history(
            "D",
            CR_HTF_WARMUP,
            NOW,
            chunk_days=3000,
        )

        if len(h1) < 100000:
            raise RuntimeError(
                f"Incomplete H1 history: {len(h1)}"
            )

        if len(h4) < 10000:
            raise RuntimeError(
                f"Incomplete H4 history: {len(h4)}"
            )

        if len(daily) < 5000:
            raise RuntimeError(
                f"Incomplete Daily history: {len(daily)}"
            )

        write_csv(
            CR_OUT["coverage"],
            [
                {
                    "timeframe": "H1",
                    "candles": len(h1),
                    "first":
                        iso(
                            h1[0]["time"]
                        ),
                    "last":
                        iso(
                            h1[-1]["time"]
                        ),
                },
                {
                    "timeframe": "H4",
                    "candles": len(h4),
                    "first":
                        iso(
                            h4[0]["time"]
                        ),
                    "last":
                        iso(
                            h4[-1]["time"]
                        ),
                },
                {
                    "timeframe": "D",
                    "candles":
                        len(daily),
                    "first":
                        iso(
                            daily[0]["time"]
                        ),
                    "last":
                        iso(
                            daily[-1]["time"]
                        ),
                },
            ],
        )

        CR_STATUS.update({
            "state": "features",
            "message": "Building frozen core and complement features",
        })

        features = build_features(
            h1,
            "H1",
        )
        cr_ensure_lookbacks(
            features
        )

        htf_cache = rf_context_cache(
            features,
            h4,
            daily,
        )
        prior4h_momentum = (
            cr_prior4h_momentum(
                features
            )
        )

        core_config = (
            cr_core_config()
        )

        # ----------------------------------------------------
        # PARITY — EXACT FROZEN CORE + KNOWN SWEEP CONTROL
        # ----------------------------------------------------
        CR_STATUS.update({
            "state": "parity",
            "message": "Reproducing frozen core and sweep-control anchors",
        })

        parity_features = (
            fc_slice_features(
                features,
                CR_PARITY_CUTOFF,
            )
        )

        parity_rows = [
            fc_parity_row(
                "FROZEN_CORE",
                core_config,
                parity_features,
                FC_PARITY_EXPECTED[
                    "PRIMARY"
                ],
            ),
            fc_parity_row(
                "KNOWN_SWEEP_CONTROL",
                fc_sweep_control(),
                parity_features,
                FC_PARITY_EXPECTED[
                    "SWEEP_CONTROL"
                ],
            ),
        ]

        write_csv(
            CR_OUT["parity"],
            parity_rows,
        )

        # ----------------------------------------------------
        # CURRENT-HISTORY FROZEN CORE BASELINE
        # ----------------------------------------------------
        core_raw = signal_indices(
            core_config,
            features,
        )

        core_standalone_trades = (
            backtest(
                core_config,
                features,
                core_raw,
                rr=core_config[
                    "rr"
                ],
                cost_multiplier=1.0,
            )
        )

        core_eval, _ = (
            evaluate_candidate(
                core_config,
                features,
                core_raw,
            )
        )

        core_combined_config = {
            "config_id":
                "FROZEN_CORE_ONLY",
            "timeframe": "H1",
            "side": "SHORT",
            "family":
                "COMPRESSION_BREAKOUT",
        }

        core_roll = (
            cr_rolling_summary_for(
                core_combined_config,
                core_standalone_trades,
            )
        )
        core_cal = (
            cr_calendar_summary_for(
                core_combined_config,
                core_standalone_trades,
            )
        )

        core_baseline = {
            **core_eval,
            **core_roll,
            **core_cal,
        }

        write_csv(
            CR_OUT["core_baseline"],
            [
                core_baseline
            ],
        )

        # ----------------------------------------------------
        # STAGE 1 — LOCAL SWEEP GEOMETRY, RR3.50
        # ----------------------------------------------------
        stage1_rows = []
        stage1_raw = {}

        stage1_configs = (
            cr_stage1_configs()
        )

        for number, candidate in enumerate(
            stage1_configs,
            1,
        ):
            CR_STATUS.update({
                "state":
                    "stage1_geometry",
                "message": (
                    f"{number}/"
                    f"{len(stage1_configs)} "
                    f"{candidate['config_id']}"
                ),
            })

            base_signal_config = (
                cr_base_signal_config(
                    candidate
                )
            )

            raw = signal_indices(
                base_signal_config,
                features,
            )

            stage1_raw[
                candidate[
                    "base_config_id"
                ]
            ] = raw

            row, _, _ = cr_summary(
                core_config,
                candidate,
                features,
                core_raw,
                raw,
                core_standalone_trades,
                core_baseline,
                cost_multiplier=1.0,
            )

            stage1_rows.append(
                row
            )

        stage1_rows.sort(
            key=cr_rank_key,
            reverse=True,
        )

        write_csv(
            CR_OUT["stage1"],
            stage1_rows,
        )

        stage1_shortlist = (
            cr_select_stage1(
                stage1_rows
            )
        )

        write_csv(
            CR_OUT[
                "stage1_shortlist"
            ],
            stage1_shortlist,
        )

        # ----------------------------------------------------
        # STAGE 2 — RR CONFIRMATION
        # ----------------------------------------------------
        stage2_rows = []
        stage2_raw = {}

        total_stage2 = (
            len(
                stage1_shortlist
            )
            * len(
                CR_RR_GRID
            )
        )
        done = 0

        for base_row in stage1_shortlist:
            raw = stage1_raw[
                base_row[
                    "base_config_id"
                ]
            ]

            for rr in CR_RR_GRID:
                done += 1

                candidate = (
                    cr_sweep_config(
                        base_row[
                            "body_atr_min"
                        ],
                        int(
                            base_row[
                                "lookback"
                            ]
                        ),
                        base_row[
                            "wick_body_min"
                        ],
                        rr,
                        "NONE",
                    )
                )

                CR_STATUS.update({
                    "state":
                        "stage2_rr",
                    "message": (
                        f"{done}/"
                        f"{total_stage2} "
                        f"{candidate['config_id']}"
                    ),
                })

                row, _, _ = (
                    cr_summary(
                        core_config,
                        candidate,
                        features,
                        core_raw,
                        raw,
                        core_standalone_trades,
                        core_baseline,
                        cost_multiplier=1.0,
                    )
                )

                stage2_rows.append(
                    row
                )
                stage2_raw[
                    candidate[
                        "config_id"
                    ]
                ] = raw

        stage2_rows.sort(
            key=cr_rank_key,
            reverse=True,
        )

        write_csv(
            CR_OUT["stage2"],
            stage2_rows,
        )

        stage2_shortlist = (
            cr_select_stage2(
                stage2_rows
            )
        )

        write_csv(
            CR_OUT[
                "stage2_shortlist"
            ],
            stage2_shortlist,
        )

        # ----------------------------------------------------
        # STAGE 3 — SINGLE-FACTOR CONTEXT CONFIRMATION
        # ----------------------------------------------------
        stage3_rows = []
        stage3_indices = {}

        total_stage3 = (
            len(
                stage2_shortlist
            )
            * len(
                CR_CONTEXTS
            )
        )
        done = 0

        for base_row in stage2_shortlist:
            raw = stage2_raw[
                base_row[
                    "candidate_config_id"
                ]
            ]

            for context_id in CR_CONTEXTS:
                done += 1

                candidate = (
                    cr_sweep_config(
                        base_row[
                            "body_atr_min"
                        ],
                        int(
                            base_row[
                                "lookback"
                            ]
                        ),
                        base_row[
                            "wick_body_min"
                        ],
                        base_row["rr"],
                        context_id,
                    )
                )

                filtered = (
                    cr_filter_indices(
                        raw,
                        context_id,
                        features,
                        htf_cache,
                        prior4h_momentum,
                    )
                )

                CR_STATUS.update({
                    "state":
                        "stage3_context",
                    "message": (
                        f"{done}/"
                        f"{total_stage3} "
                        f"{candidate['config_id']}"
                    ),
                })

                row, _, _ = (
                    cr_summary(
                        core_config,
                        candidate,
                        features,
                        core_raw,
                        filtered,
                        core_standalone_trades,
                        core_baseline,
                        cost_multiplier=1.0,
                    )
                )

                row[
                    "candidate_signals_before_context"
                ] = len(raw)
                row[
                    "candidate_signals_after_context"
                ] = len(
                    filtered
                )
                row[
                    "context_signal_retention_pct"
                ] = pct(
                    len(filtered),
                    len(raw),
                )

                stage3_rows.append(
                    row
                )
                stage3_indices[
                    candidate[
                        "config_id"
                    ]
                ] = filtered

        stage3_rows.sort(
            key=cr_rank_key,
            reverse=True,
        )

        write_csv(
            CR_OUT["stage3"],
            stage3_rows,
        )

        # ----------------------------------------------------
        # FINALIST POOL
        # ----------------------------------------------------
        pool = (
            list(
                stage3_rows
            )
            + list(
                stage2_rows
            )
        )

        # Dedupe by exact candidate config ID.
        deduped = {}
        for row in pool:
            cid = row[
                "candidate_config_id"
            ]

            existing = (
                deduped.get(
                    cid
                )
            )

            if (
                existing is None
                or cr_rank_key(
                    row
                )
                > cr_rank_key(
                    existing
                )
            ):
                deduped[
                    cid
                ] = row

        candidate_pool = list(
            deduped.values()
        )

        candidate_pool.sort(
            key=cr_rank_key,
            reverse=True,
        )

        finalist_rows = (
            candidate_pool[
                :CR_FINAL_KEEP
            ]
        )

        # Force the plain known control:
        # body1.25 / LB15 / wick0.25 / RR3.5 / NONE.
        control_candidate = (
            cr_sweep_config(
                1.25,
                15,
                0.25,
                3.50,
                "NONE",
            )
        )

        if not any(
            row[
                "candidate_config_id"
            ]
            == control_candidate[
                "config_id"
            ]
            for row in finalist_rows
        ):
            control_row = next(
                (
                    row
                    for row
                    in candidate_pool
                    if row[
                        "candidate_config_id"
                    ]
                    == control_candidate[
                        "config_id"
                    ]
                ),
                None,
            )

            if control_row is not None:
                if len(
                    finalist_rows
                ) >= CR_FINAL_KEEP:
                    finalist_rows[
                        -1
                    ] = control_row
                else:
                    finalist_rows.append(
                        control_row
                    )

        finalist_rows.sort(
            key=cr_rank_key,
            reverse=True,
        )

        # ----------------------------------------------------
        # DEEP FINALISTS
        # ----------------------------------------------------
        deep_rows = []
        decision_rows = []
        cost_rows = []
        period_rows = []
        rolling_rows_out = []
        calendar_rows_out = []
        trade_rows = []
        overlap_rows = []
        overlap_summaries = []

        for rank, seed in enumerate(
            finalist_rows,
            1,
        ):
            candidate = (
                cr_sweep_config(
                    seed[
                        "body_atr_min"
                    ],
                    int(
                        seed["lookback"]
                    ),
                    seed[
                        "wick_body_min"
                    ],
                    seed["rr"],
                    seed[
                        "context_id"
                    ],
                )
            )

            if (
                candidate[
                    "context_id"
                ] == "NONE"
            ):
                raw = stage2_raw.get(
                    candidate[
                        "config_id"
                    ]
                )

                if raw is None:
                    base_signal_config = (
                        cr_base_signal_config(
                            candidate
                        )
                    )
                    raw = signal_indices(
                        base_signal_config,
                        features,
                    )
            else:
                raw = stage3_indices[
                    candidate[
                        "config_id"
                    ]
                ]

            CR_STATUS.update({
                "state":
                    "deep_finalists",
                "message": (
                    f"{rank}/"
                    f"{len(finalist_rows)} "
                    f"{candidate['config_id']}"
                ),
            })

            (
                base_row,
                combined_trades,
                rejected,
            ) = cr_summary(
                core_config,
                candidate,
                features,
                core_raw,
                raw,
                core_standalone_trades,
                core_baseline,
                cost_multiplier=1.0,
            )

            combined_config = (
                cr_union_config(
                    candidate
                )
            )

            finalist_cost_rows = []

            for multiplier in (
                0.5,
                1.0,
                1.5,
                2.0,
            ):
                (
                    stress_row,
                    _stress_trades,
                    _stress_rejected,
                ) = cr_summary(
                    core_config,
                    candidate,
                    features,
                    core_raw,
                    raw,
                    core_standalone_trades,
                    core_baseline,
                    cost_multiplier=multiplier,
                )

                stress_row[
                    "finalist_rank"
                ] = rank

                cost_rows.append(
                    stress_row
                )
                finalist_cost_rows.append(
                    stress_row
                )

            cost2 = next(
                row
                for row
                in finalist_cost_rows
                if abs(
                    row[
                        "cost_multiplier"
                    ]
                    - 2.0
                ) < 1e-12
            )

            gate = cr_deep_gate(
                base_row,
                cost2,
            )

            deep_row = {
                "finalist_rank":
                    rank,
                **base_row,
                **gate,
                "cost2_combined_pf":
                    cost2[
                        "combined_pf"
                    ],
                "cost2_combined_total_r":
                    cost2[
                        "combined_total_r"
                    ],
                "cost2_marginal_total_r":
                    cost2[
                        "marginal_total_r"
                    ],
            }

            deep_rows.append(
                deep_row
            )

            decision_rows.append({
                "finalist_rank":
                    rank,
                "candidate_config_id":
                    candidate[
                        "config_id"
                    ],
                **gate,
                "combined_trades":
                    base_row[
                        "combined_trades"
                    ],
                "marginal_accepted_trades":
                    base_row[
                        "marginal_accepted_trades"
                    ],
                "combined_pf":
                    base_row[
                        "combined_pf"
                    ],
                "combined_total_r":
                    base_row[
                        "combined_total_r"
                    ],
                "delta_total_r_vs_core":
                    base_row[
                        "delta_total_r_vs_core"
                    ],
                "marginal_pf":
                    base_row[
                        "marginal_pf"
                    ],
                "marginal_total_r":
                    base_row[
                        "marginal_total_r"
                    ],
                "rolling12_positive_pct":
                    base_row[
                        "rolling12_positive_pct"
                    ],
                "rolling24_positive_pct":
                    base_row[
                        "rolling24_positive_pct"
                    ],
                "rolling36_positive_pct":
                    base_row[
                        "rolling36_positive_pct"
                    ],
                "rolling24_worst_r":
                    base_row[
                        "rolling24_worst_r"
                    ],
                "rolling36_worst_r":
                    base_row[
                        "rolling36_worst_r"
                    ],
                "validation_pf":
                    base_row[
                        "validation_pf"
                    ],
                "last5y_r":
                    base_row[
                        "last5y_r"
                    ],
                "last2y_r":
                    base_row[
                        "last2y_r"
                    ],
                "cost2_combined_pf":
                    cost2[
                        "combined_pf"
                    ],
                "cost2_combined_total_r":
                    cost2[
                        "combined_total_r"
                    ],
                "core_displaced_count":
                    base_row[
                        "core_displaced_count"
                    ],
            })

            period_rows.extend(
                cr_period_rows(
                    combined_config,
                    combined_trades,
                )
            )

            roll_rows = rolling_rows(
                combined_config,
                combined_trades,
            )
            cal_rows = calendar_rows(
                combined_config,
                combined_trades,
            )

            rolling_rows_out.extend(
                roll_rows
            )
            calendar_rows_out.extend(
                cal_rows
            )

            for trade in combined_trades:
                trade_rows.append(
                    cr_serialise_trade(
                        trade,
                        rank,
                        candidate[
                            "config_id"
                        ],
                    )
                )

            for rejection in rejected:
                overlap_rows.append(
                    cr_serialise_rejection(
                        rejection,
                        rank,
                        candidate[
                            "config_id"
                        ],
                    )
                )

            overlap_summaries.append({
                "finalist_rank":
                    rank,
                "candidate_config_id":
                    candidate[
                        "config_id"
                    ],
                "core_raw_signals":
                    base_row[
                        "core_raw_signals"
                    ],
                "candidate_raw_signals":
                    base_row[
                        "candidate_raw_signals"
                    ],
                "accepted_core":
                    base_row[
                        "accepted_core"
                    ],
                "accepted_candidate":
                    base_row[
                        "accepted_candidate"
                    ],
                "rejected_core_overlap":
                    base_row[
                        "rejected_core_overlap"
                    ],
                "rejected_candidate_overlap":
                    base_row[
                        "rejected_candidate_overlap"
                    ],
                "same_candle_candidate_rejected":
                    base_row[
                        "same_candle_candidate_rejected"
                    ],
                "core_displaced_count":
                    base_row[
                        "core_displaced_count"
                    ],
                "core_newly_eligible_count":
                    base_row[
                        "core_newly_eligible_count"
                    ],
            })

        deep_rows.sort(
            key=lambda row: (
                1
                if row[
                    "deep_pass"
                ]
                else 0,
                cr_rank_key(
                    row
                ),
            ),
            reverse=True,
        )

        write_csv(
            CR_OUT["finalists"],
            deep_rows,
        )
        write_csv(
            CR_OUT["decision"],
            decision_rows,
        )
        write_csv(
            CR_OUT["cost_stress"],
            cost_rows,
        )
        write_csv(
            CR_OUT["periods"],
            period_rows,
        )
        write_csv(
            CR_OUT["rolling"],
            rolling_rows_out,
        )
        write_csv(
            CR_OUT[
                "rolling_summary"
            ],
            rolling_summary(
                rolling_rows_out
            ),
        )
        write_csv(
            CR_OUT["calendar"],
            calendar_rows_out,
        )
        write_csv(
            CR_OUT[
                "calendar_summary"
            ],
            calendar_summary(
                calendar_rows_out
            ),
        )
        write_csv(
            CR_OUT["trades"],
            trade_rows,
        )
        write_csv(
            CR_OUT["overlaps"],
            overlap_summaries,
        )

        write_csv(
            CR_OUT["notes"],
            [
                {
                    "topic":
                        "frozen_core",
                    "note": (
                        "Compression core is completely frozen: body1.25, "
                        "range1.50, compression<=0.85, close below prior15 low, "
                        "RR3.50, no context filter."
                    ),
                },
                {
                    "topic":
                        "objective",
                    "note": (
                        "Complement search ranks return-shape improvement first: "
                        "12/24/36M positive-window consistency, worst rolling "
                        "troughs, validation strength and marginal accepted R."
                    ),
                },
                {
                    "topic":
                        "union_execution",
                    "note": (
                        "Exact raw-signal union with core same-candle priority, "
                        "strategy-level pyramiding0, half-open overlap blocking "
                        "[signal_index, exit_index), exit-candle re-entry eligible."
                    ),
                },
                {
                    "topic":
                        "contexts",
                    "note": (
                        "Only single-factor contexts are tested; no context "
                        "interactions or combinations are mined."
                    ),
                },
                {
                    "topic":
                        "portfolio",
                    "note": (
                        "This remains standalone AUD/USD SHORT research. "
                        "No live25 -> 26 portfolio-add decision is made here."
                    ),
                },
                {
                    "topic":
                        "historical_not_forecast",
                    "note": (
                        "All results are historical backtests, not forecasts."
                    ),
                },
            ],
        )

        CR_STATUS.update({
            "state": "packaging",
            "message": "Packaging complementary sweep research",
        })

        cr_pack()

        passes = [
            row
            for row
            in deep_rows
            if row[
                "deep_pass"
            ]
        ]

        best = (
            deep_rows[0]
            if deep_rows
            else None
        )

        CR_STATUS.update({
            "state": "complete",
            "message": "AUD/USD H1 SHORT complementary sweep research complete",
            "core_current_trades":
                core_baseline[
                    "full_trades"
                ],
            "core_current_pf":
                core_baseline[
                    "full_pf"
                ],
            "core_current_total_r":
                core_baseline[
                    "full_total_r"
                ],
            "stage1_configs":
                len(stage1_rows),
            "stage2_rows":
                len(stage2_rows),
            "stage3_rows":
                len(stage3_rows),
            "deep_finalists":
                len(deep_rows),
            "deep_passes":
                len(passes),
            "best_candidate":
                (
                    best[
                        "candidate_config_id"
                    ]
                    if best
                    else None
                ),
            "best_combined_trades":
                (
                    best[
                        "combined_trades"
                    ]
                    if best
                    else None
                ),
            "best_combined_total_r":
                (
                    best[
                        "combined_total_r"
                    ]
                    if best
                    else None
                ),
            "best_rolling24_positive_pct":
                (
                    best[
                        "rolling24_positive_pct"
                    ]
                    if best
                    else None
                ),
            "best_rolling36_positive_pct":
                (
                    best[
                        "rolling36_positive_pct"
                    ]
                    if best
                    else None
                ),
            "bundle":
                CR_BUNDLE,
            "orders_supported":
                False,
            "trading_enabled":
                False,
        })

    except Exception as error:
        import traceback

        CR_STATUS.update({
            "state": "error",
            "message":
                str(error),
            "error_type":
                type(error).__name__,
            "traceback":
                traceback.format_exc(),
            "orders_supported":
                False,
            "trading_enabled":
                False,
        })

        print(
            "AUDUSD H1 SHORT COMPLEMENT RESEARCH ERROR:",
            repr(error),
            flush=True,
        )


@app.route(
    "/audusd-h1-short-complement/status"
)
def cr_status():
    return jsonify(
        CR_STATUS
    )


@app.route(
    "/audusd-h1-short-complement/results"
)
def cr_results():
    if not os.path.exists(
        CR_BUNDLE
    ):
        return jsonify({
            "status":
                "not_ready",
            "state":
                CR_STATUS[
                    "state"
                ],
            "message":
                CR_STATUS[
                    "message"
                ],
        }), 404

    return send_file(
        os.path.abspath(
            CR_BUNDLE
        ),
        as_attachment=True,
        download_name=
            CR_BUNDLE,
    )


@app.route(
    "/audusd-h1-short-complement/info"
)
def cr_info():
    return jsonify({
        "service": (
            "AUD/USD H1 SHORT Complementary Sweep Research"
        ),
        "read_only":
            True,
        "orders_supported":
            False,
        "frozen_core": {
            "family":
                "COMPRESSION_BREAKOUT",
            "body_atr_min":
                1.25,
            "range_atr_min":
                1.50,
            "compression_max":
                0.85,
            "breakout_lookback":
                15,
            "rr":
                3.50,
        },
        "complement_family":
            "SWEEP_DISPLACEMENT",
        "stage1_geometry_configs":
            len(
                CR_BODY_GRID
            )
            * len(
                CR_LB_GRID
            )
            * len(
                CR_WICK_GRID
            ),
        "rr_grid":
            CR_RR_GRID,
        "contexts":
            CR_CONTEXTS,
        "execution": (
            "core priority, exact union p0, "
            "exit-candle re-entry eligible"
        ),
        "next_step_if_robust": (
            "Freeze two-trigger AUD/USD H1 SHORT then run exact "
            "current25 -> prospective26 portfolio-add test"
        ),
        "routes": [
            "/audusd-h1-short-complement/status",
            "/audusd-h1-short-complement/results",
            "/audusd-h1-short-complement/info",
        ],
    })



# ============================================================
# AUD/USD H1 SHORT — SWEEP BOUNDARY / PLATEAU CONFIRMATION
# ============================================================
#
# PURPOSE
# -------
# Resolve the ONLY remaining optimisation-boundary question from the
# successful complementary-sweep study.
#
# Everything except sweep lookback and prior-4H rally momentum is frozen.
#
# FROZEN COMPRESSION CORE
# -----------------------
# COMPRESSION_BREAKOUT
# bearish H1 candle
# body >= 1.25 ATR14
# range >= 1.50 ATR14
# previous H1 ATR14 / previous20 ATR mean <= 0.85
# close below previous 15-H1 low
# RR3.50
# stop = signal high +10 ticks
# historical H1 adverse fill = 0.5 pip
# no weekday/session/HTF filter
#
# FROZEN SWEEP COMPLEMENT GEOMETRY
# --------------------------------
# bearish H1 candle
# body >= 1.25 ATR14
# high > previous N-bar high
# close < previous H1 low
# upper wick/body >= 0.25
# RR3.50
# stop = signal high +10 ticks
#
# ONLY VARIABLES TESTED
# ---------------------
# sweep lookback:
#   15 / 20 / 25 / 30 / 40 / 50
#
# prior ~4H rally momentum:
#   >= 0.25 / 0.50 / 0.75 / 1.00 ATR14
#
# Total = 24 predeclared combinations.
#
# UNION EXECUTION
# ---------------
# Exact raw-signal core + complement union.
# Core same-candle priority.
# Exact strategy pyramiding0.
# Half-open blocking [signal_index, exit_index).
# Exit-candle signal remains eligible.
#
# PRIMARY QUESTION
# ----------------
# Does the strong LB30 / momentum0.50 result sit inside a robust local
# plateau, especially across LB25/30/40 and momentum0.25/0.50/0.75?
#
# This runner therefore reports:
#   - all 24 exact union results
#   - 0.5x/1x/1.5x/2x cost stress
#   - 12/24/36M rolling consistency
#   - yearly / era / validation / recent periods
#   - marginal sweep contribution
#   - exact overlap/displacement audit
#   - lookback and momentum summaries
#   - central 3x3 plateau summary
#
# No new sessions, weekdays, EMAs, trigger families, body, wick or RR.
# No live orders.
# ============================================================

BC_STATUS = {
    "state": "not_started",
    "message": "AUD/USD H1 SHORT sweep boundary confirmation not started",
    "pair": PAIR,
    "timeframe": "H1",
    "side": "SHORT",
    "orders_supported": False,
    "trading_enabled": False,
}

BC_BUNDLE = "AUDUSD_H1_SHORT_SWEEP_BOUNDARY_CONFIRMATION_RESULTS.zip"

BC_OUT = {
    "coverage": "audusd_h1_short_sweep_boundary_coverage.csv",
    "parity": "audusd_h1_short_sweep_boundary_parity.csv",
    "core_baseline": "audusd_h1_short_sweep_boundary_core_baseline.csv",
    "matrix": "audusd_h1_short_sweep_boundary_matrix.csv",
    "cost_stress": "audusd_h1_short_sweep_boundary_cost_stress.csv",
    "lookback_summary": "audusd_h1_short_sweep_boundary_lookback_summary.csv",
    "momentum_summary": "audusd_h1_short_sweep_boundary_momentum_summary.csv",
    "plateau": "audusd_h1_short_sweep_boundary_plateau.csv",
    "periods": "audusd_h1_short_sweep_boundary_periods.csv",
    "rolling": "audusd_h1_short_sweep_boundary_rolling.csv",
    "rolling_summary": "audusd_h1_short_sweep_boundary_rolling_summary.csv",
    "calendar": "audusd_h1_short_sweep_boundary_calendar.csv",
    "calendar_summary": "audusd_h1_short_sweep_boundary_calendar_summary.csv",
    "top_trades": "audusd_h1_short_sweep_boundary_top_trades.csv",
    "overlaps": "audusd_h1_short_sweep_boundary_overlaps.csv",
    "decision": "audusd_h1_short_sweep_boundary_decision.csv",
    "notes": "audusd_h1_short_sweep_boundary_notes.csv",
}

BC_PARITY_CUTOFF = datetime(
    2026, 9, 18, 17, 0,
    tzinfo=timezone.utc,
)

BC_LOOKBACKS = [15, 20, 25, 30, 40, 50]
BC_MOMENTUMS = [0.25, 0.50, 0.75, 1.00]

BC_BODY = 1.25
BC_WICK = 0.25
BC_RR = 3.50

BC_CENTRAL_LOOKBACKS = {25, 30, 40}
BC_CENTRAL_MOMENTUMS = {0.25, 0.50, 0.75}

BC_PARITY_EXPECTED = {
    # Exact row from uploaded complementary-sweep finalist output.
    "combined_trades": 196,
    "combined_winners": 65,
    "combined_pf": 1.6891864676018515,
    "combined_total_r": 90.28342725584255,
    "combined_max_dd_r": -9.556818181818162,
    "marginal_accepted_trades": 40,
    "marginal_winners": 13,
    "marginal_pf": 1.6489071205687165,
    "marginal_total_r": 17.520492255355343,
    "core_displaced_count": 3,
    "validation_pf": 1.703665833436535,
    "validation_r": 32.368628338080626,
    "last5y_r": 25.721263533234417,
    "last2y_r": 0.5594281973406892,
    "rolling12_positive_pct": 77.7327935222672,
    "rolling24_positive_pct": 91.91489361702128,
    "rolling36_positive_pct": 99.10313901345292,
    "rolling24_worst_r": -5.556818181818162,
    "rolling36_worst_r": -2.4821135712452,
}

BC_FLOAT_TOL = 1e-8


def bc_pack():
    with zipfile.ZipFile(
        BC_BUNDLE,
        "w",
        zipfile.ZIP_DEFLATED,
    ) as archive:
        for path in BC_OUT.values():
            if os.path.exists(path):
                archive.write(
                    path,
                    arcname=os.path.basename(path),
                )


def bc_ensure_lookbacks(features):
    for lookback in BC_LOOKBACKS:
        if lookback not in features["prev_lows"]:
            features["prev_lows"][lookback] = (
                rolling_previous_extreme(
                    features["low"],
                    lookback,
                    want_max=False,
                )
            )

        if lookback not in features["prev_highs"]:
            features["prev_highs"][lookback] = (
                rolling_previous_extreme(
                    features["high"],
                    lookback,
                    want_max=True,
                )
            )


def bc_candidate_config(
    lookback,
    momentum_threshold,
):
    context_id = (
        "PRIOR4H_MOM_GE_"
        + f"{int(round(momentum_threshold * 100)):03d}"
    )

    return cr_sweep_config(
        BC_BODY,
        int(lookback),
        BC_WICK,
        BC_RR,
        context_id,
    )


def bc_momentum_mask(
    prior4h_momentum,
    threshold,
):
    return (
        np.isfinite(
            prior4h_momentum
        )
        & (
            prior4h_momentum
            >= float(threshold)
        )
    )


def bc_filtered_sweep_indices(
    lookback,
    momentum_threshold,
    features,
    prior4h_momentum,
):
    candidate = (
        bc_candidate_config(
            lookback,
            momentum_threshold,
        )
    )

    base_config = (
        cr_base_signal_config(
            candidate
        )
    )

    raw = signal_indices(
        base_config,
        features,
    )

    mask = bc_momentum_mask(
        prior4h_momentum,
        momentum_threshold,
    )

    filtered = raw[
        mask[raw]
    ]

    return (
        candidate,
        raw,
        filtered,
    )


def bc_core_baseline(
    core_config,
    features,
):
    core_raw = signal_indices(
        core_config,
        features,
    )

    core_trades = backtest(
        core_config,
        features,
        core_raw,
        rr=core_config["rr"],
        cost_multiplier=1.0,
    )

    core_eval, _ = (
        evaluate_candidate(
            core_config,
            features,
            core_raw,
        )
    )

    synthetic = {
        "config_id":
            "FROZEN_CORE_ONLY",
        "timeframe": "H1",
        "side": "SHORT",
        "family":
            "COMPRESSION_BREAKOUT",
    }

    roll = cr_rolling_summary_for(
        synthetic,
        core_trades,
    )
    cal = cr_calendar_summary_for(
        synthetic,
        core_trades,
    )

    baseline = {
        **core_eval,
        **roll,
        **cal,
    }

    return (
        core_raw,
        core_trades,
        baseline,
    )


def bc_parity_check(
    features,
):
    parity_features = (
        fc_slice_features(
            features,
            BC_PARITY_CUTOFF,
        )
    )

    bc_ensure_lookbacks(
        parity_features
    )

    core = cr_core_config()
    prior4h = cr_prior4h_momentum(
        parity_features
    )

    (
        core_raw,
        core_trades,
        core_baseline,
    ) = bc_core_baseline(
        core,
        parity_features,
    )

    (
        candidate,
        _raw,
        filtered,
    ) = bc_filtered_sweep_indices(
        30,
        0.50,
        parity_features,
        prior4h,
    )

    (
        row,
        _trades,
        _rejected,
    ) = cr_summary(
        core,
        candidate,
        parity_features,
        core_raw,
        filtered,
        core_trades,
        core_baseline,
        cost_multiplier=1.0,
    )

    checks = {}

    integer_fields = [
        "combined_trades",
        "combined_winners",
        "marginal_accepted_trades",
        "marginal_winners",
        "core_displaced_count",
    ]

    float_fields = [
        "combined_pf",
        "combined_total_r",
        "combined_max_dd_r",
        "marginal_pf",
        "marginal_total_r",
        "validation_pf",
        "validation_r",
        "last5y_r",
        "last2y_r",
        "rolling12_positive_pct",
        "rolling24_positive_pct",
        "rolling36_positive_pct",
        "rolling24_worst_r",
        "rolling36_worst_r",
    ]

    for field in integer_fields:
        checks[field] = (
            int(row[field])
            == int(
                BC_PARITY_EXPECTED[
                    field
                ]
            )
        )

    for field in float_fields:
        checks[field] = (
            abs(
                float(row[field])
                - float(
                    BC_PARITY_EXPECTED[
                        field
                    ]
                )
            )
            <= BC_FLOAT_TOL
        )

    output = {
        "candidate_config_id":
            candidate[
                "config_id"
            ],
        "cutoff_utc":
            iso(
                BC_PARITY_CUTOFF
            ),
        "pass":
            all(
                checks.values()
            ),
        "checks_json":
            json.dumps(
                checks,
                sort_keys=True,
            ),
    }

    for field in (
        integer_fields
        + float_fields
    ):
        output[
            f"expected_{field}"
        ] = (
            BC_PARITY_EXPECTED[
                field
            ]
        )
        output[
            f"actual_{field}"
        ] = row[field]

    if not output["pass"]:
        raise RuntimeError(
            "Boundary-confirmation parity failed: "
            + json.dumps(
                output,
                default=str,
            )
        )

    return output


def bc_pass_checks(
    row,
    cost2_row,
):
    checks = {
        "combined_trades_ge_180":
            row[
                "combined_trades"
            ] >= 180,

        "marginal_accepted_ge_25":
            row[
                "marginal_accepted_trades"
            ] >= 25,

        "marginal_r_positive":
            row[
                "marginal_total_r"
            ] > 0,

        "combined_r_beats_core":
            row[
                "delta_total_r_vs_core"
            ] > 0,

        "both_splits_positive":
            bool(
                row[
                    "both_temporal_splits_positive"
                ]
            ),

        "validation_pf_ge_1_30":
            row[
                "validation_pf"
            ] >= 1.30,

        "positive_eras_ge_3":
            row[
                "positive_eras"
            ] >= 3,

        "last5_positive":
            row[
                "last5y_r"
            ] > 0,

        "rolling12_ge_70":
            row[
                "rolling12_positive_pct"
            ] >= 70.0,

        "rolling24_ge_85":
            row[
                "rolling24_positive_pct"
            ] >= 85.0,

        "rolling36_ge_92":
            row[
                "rolling36_positive_pct"
            ] >= 92.0,

        "worst24_ge_minus_6_5":
            row[
                "rolling24_worst_r"
            ] >= -6.5,

        "worst36_ge_minus_6_5":
            row[
                "rolling36_worst_r"
            ] >= -6.5,

        "calendar_positive_ge_60":
            row[
                "positive_calendar_year_pct"
            ] >= 60.0,

        "cost2_pf_ge_1_30":
            cost2_row[
                "combined_pf"
            ] >= 1.30,

        "cost2_r_positive":
            cost2_row[
                "combined_total_r"
            ] > 0,
    }

    return {
        "boundary_pass":
            all(
                checks.values()
            ),
        "checks_passed":
            sum(
                checks.values()
            ),
        "checks_total":
            len(checks),
        "checks_json":
            json.dumps(
                checks,
                sort_keys=True,
            ),
    }


def bc_rank_key(row):
    return (
        1
        if row[
            "boundary_pass"
        ]
        else 0,
        row[
            "rolling36_positive_pct"
        ],
        row[
            "rolling24_positive_pct"
        ],
        row[
            "rolling12_positive_pct"
        ],
        row[
            "rolling36_worst_r"
        ],
        row[
            "rolling24_worst_r"
        ],
        row[
            "validation_pf"
        ],
        row[
            "marginal_total_r"
        ],
        row[
            "combined_total_r"
        ],
    )


def bc_dimension_summary(
    rows,
    field,
    values,
):
    output = []

    for value in values:
        subset = [
            row
            for row in rows
            if abs(
                float(
                    row[field]
                )
                - float(value)
            )
            < 1e-12
        ]

        if not subset:
            continue

        output.append({
            field:
                value,
            "configs":
                len(subset),
            "passes":
                sum(
                    bool(
                        row[
                            "boundary_pass"
                        ]
                    )
                    for row
                    in subset
                ),
            "pass_pct":
                pct(
                    sum(
                        bool(
                            row[
                                "boundary_pass"
                            ]
                        )
                        for row
                        in subset
                    ),
                    len(subset),
                ),
            "positive_marginal_r_configs":
                sum(
                    row[
                        "marginal_total_r"
                    ] > 0
                    for row
                    in subset
                ),
            "median_combined_pf":
                med(
                    row[
                        "combined_pf"
                    ]
                    for row
                    in subset
                ),
            "median_combined_total_r":
                med(
                    row[
                        "combined_total_r"
                    ]
                    for row
                    in subset
                ),
            "median_marginal_total_r":
                med(
                    row[
                        "marginal_total_r"
                    ]
                    for row
                    in subset
                ),
            "median_rolling12_positive_pct":
                med(
                    row[
                        "rolling12_positive_pct"
                    ]
                    for row
                    in subset
                ),
            "median_rolling24_positive_pct":
                med(
                    row[
                        "rolling24_positive_pct"
                    ]
                    for row
                    in subset
                ),
            "median_rolling36_positive_pct":
                med(
                    row[
                        "rolling36_positive_pct"
                    ]
                    for row
                    in subset
                ),
            "median_validation_pf":
                med(
                    row[
                        "validation_pf"
                    ]
                    for row
                    in subset
                ),
            "median_cost2_pf":
                med(
                    row[
                        "cost2_combined_pf"
                    ]
                    for row
                    in subset
                ),
        })

    return output


def bc_plateau_summary(rows):
    central = [
        row
        for row in rows
        if (
            int(
                row[
                    "lookback"
                ]
            )
            in BC_CENTRAL_LOOKBACKS
            and round(
                float(
                    row[
                        "momentum_threshold"
                    ]
                ),
                2,
            )
            in BC_CENTRAL_MOMENTUMS
        )
    ]

    if len(central) != 9:
        raise RuntimeError(
            f"Expected 9 central plateau cells, got {len(central)}"
        )

    passed = [
        row
        for row in central
        if row[
            "boundary_pass"
        ]
    ]

    return [{
        "central_lookbacks":
            "25,30,40",
        "central_momentums":
            "0.25,0.50,0.75",
        "cells":
            len(central),
        "passes":
            len(passed),
        "pass_pct":
            pct(
                len(passed),
                len(central),
            ),
        "all_marginal_r_positive":
            all(
                row[
                    "marginal_total_r"
                ] > 0
                for row
                in central
            ),
        "all_combined_r_above_core":
            all(
                row[
                    "delta_total_r_vs_core"
                ] > 0
                for row
                in central
            ),
        "minimum_validation_pf":
            min(
                row[
                    "validation_pf"
                ]
                for row
                in central
            ),
        "minimum_rolling24_positive_pct":
            min(
                row[
                    "rolling24_positive_pct"
                ]
                for row
                in central
            ),
        "minimum_rolling36_positive_pct":
            min(
                row[
                    "rolling36_positive_pct"
                ]
                for row
                in central
            ),
        "worst_rolling24_r":
            min(
                row[
                    "rolling24_worst_r"
                ]
                for row
                in central
            ),
        "worst_rolling36_r":
            min(
                row[
                    "rolling36_worst_r"
                ]
                for row
                in central
            ),
        "minimum_cost2_pf":
            min(
                row[
                    "cost2_combined_pf"
                ]
                for row
                in central
            ),
        "plateau_confirmed":
            (
                len(passed) >= 7
                and all(
                    row[
                        "marginal_total_r"
                    ] > 0
                    for row
                    in central
                )
                and all(
                    row[
                        "delta_total_r_vs_core"
                    ] > 0
                    for row
                    in central
                )
            ),
    }]


def bc_serialise_trade(
    trade,
    rank,
    candidate_config_id,
):
    row = dict(
        trade
    )
    row["rank"] = rank
    row[
        "candidate_config_id"
    ] = candidate_config_id
    row[
        "signal_time"
    ] = iso(
        row[
            "signal_time"
        ]
    )
    row[
        "exit_time"
    ] = iso(
        row[
            "exit_time"
        ]
    )
    return row


def run_bc_research():
    try:
        global STATUS
        STATUS = BC_STATUS

        BC_STATUS.update({
            "state": "fetching",
            "message": "Fetching AUD/USD H1 history",
        })

        h1 = fetch_history(
            "H1",
            REQUESTED_START,
            NOW,
            chunk_days=180,
        )

        if len(h1) < 100000:
            raise RuntimeError(
                f"Incomplete H1 history: {len(h1)}"
            )

        write_csv(
            BC_OUT["coverage"],
            [{
                "pair":
                    PAIR,
                "timeframe":
                    "H1",
                "candles":
                    len(h1),
                "first_candle_utc":
                    iso(
                        h1[0][
                            "time"
                        ]
                    ),
                "last_candle_utc":
                    iso(
                        h1[-1][
                            "time"
                        ]
                    ),
                "parity_cutoff_utc":
                    iso(
                        BC_PARITY_CUTOFF
                    ),
            }],
        )

        BC_STATUS.update({
            "state": "features",
            "message": "Building frozen core and sweep features",
        })

        features = build_features(
            h1,
            "H1",
        )

        bc_ensure_lookbacks(
            features
        )

        prior4h = (
            cr_prior4h_momentum(
                features
            )
        )

        # ----------------------------------------------
        # HARD PARITY TO UPLOADED LB30/MOM0.50 WINNER
        # ----------------------------------------------
        BC_STATUS.update({
            "state": "parity",
            "message": "Reproducing uploaded LB30 / momentum0.50 winner",
        })

        parity_row = (
            bc_parity_check(
                features
            )
        )

        write_csv(
            BC_OUT["parity"],
            [parity_row],
        )

        # ----------------------------------------------
        # CURRENT CORE BASELINE
        # ----------------------------------------------
        core_config = (
            cr_core_config()
        )

        (
            core_raw,
            core_trades,
            core_baseline,
        ) = bc_core_baseline(
            core_config,
            features,
        )

        write_csv(
            BC_OUT[
                "core_baseline"
            ],
            [
                core_baseline
            ],
        )

        # ----------------------------------------------
        # EXACT 24-CELL MATRIX + COST STRESS
        # ----------------------------------------------
        matrix_rows = []
        cost_rows = []
        combined_cache = {}
        rejected_cache = {}
        candidate_indices = {}

        total = (
            len(
                BC_LOOKBACKS
            )
            * len(
                BC_MOMENTUMS
            )
        )

        done = 0

        for lookback in BC_LOOKBACKS:
            for momentum in BC_MOMENTUMS:
                done += 1

                (
                    candidate,
                    raw,
                    filtered,
                ) = (
                    bc_filtered_sweep_indices(
                        lookback,
                        momentum,
                        features,
                        prior4h,
                    )
                )

                BC_STATUS.update({
                    "state":
                        "matrix",
                    "message": (
                        f"{done}/{total} "
                        f"LB{lookback} "
                        f"MOM>={momentum:.2f}"
                    ),
                })

                (
                    base_row,
                    combined_trades,
                    rejected,
                ) = cr_summary(
                    core_config,
                    candidate,
                    features,
                    core_raw,
                    filtered,
                    core_trades,
                    core_baseline,
                    cost_multiplier=1.0,
                )

                base_row[
                    "momentum_threshold"
                ] = momentum
                base_row[
                    "sweep_signals_before_momentum"
                ] = len(
                    raw
                )
                base_row[
                    "sweep_signals_after_momentum"
                ] = len(
                    filtered
                )
                base_row[
                    "momentum_retention_pct"
                ] = pct(
                    len(
                        filtered
                    ),
                    len(
                        raw
                    ),
                )

                stress_by_mult = {}

                for multiplier in (
                    0.5,
                    1.0,
                    1.5,
                    2.0,
                ):
                    (
                        stress_row,
                        _stress_trades,
                        _stress_rejected,
                    ) = cr_summary(
                        core_config,
                        candidate,
                        features,
                        core_raw,
                        filtered,
                        core_trades,
                        core_baseline,
                        cost_multiplier=
                            multiplier,
                    )

                    stress_row[
                        "momentum_threshold"
                    ] = momentum

                    cost_rows.append(
                        stress_row
                    )
                    stress_by_mult[
                        multiplier
                    ] = stress_row

                cost2 = (
                    stress_by_mult[
                        2.0
                    ]
                )

                gate = bc_pass_checks(
                    base_row,
                    cost2,
                )

                row = {
                    **base_row,
                    "momentum_threshold":
                        momentum,
                    "sweep_signals_before_momentum":
                        len(raw),
                    "sweep_signals_after_momentum":
                        len(filtered),
                    "momentum_retention_pct":
                        pct(
                            len(filtered),
                            len(raw),
                        ),
                    "cost2_combined_pf":
                        cost2[
                            "combined_pf"
                        ],
                    "cost2_combined_total_r":
                        cost2[
                            "combined_total_r"
                        ],
                    "cost2_marginal_total_r":
                        cost2[
                            "marginal_total_r"
                        ],
                    **gate,
                }

                matrix_rows.append(
                    row
                )

                combined_cache[
                    candidate[
                        "config_id"
                    ]
                ] = combined_trades
                rejected_cache[
                    candidate[
                        "config_id"
                    ]
                ] = rejected
                candidate_indices[
                    candidate[
                        "config_id"
                    ]
                ] = filtered

        matrix_rows.sort(
            key=bc_rank_key,
            reverse=True,
        )

        write_csv(
            BC_OUT["matrix"],
            matrix_rows,
        )
        write_csv(
            BC_OUT[
                "cost_stress"
            ],
            cost_rows,
        )

        lookback_summary = (
            bc_dimension_summary(
                matrix_rows,
                "lookback",
                BC_LOOKBACKS,
            )
        )

        momentum_summary = (
            bc_dimension_summary(
                matrix_rows,
                "momentum_threshold",
                BC_MOMENTUMS,
            )
        )

        plateau = (
            bc_plateau_summary(
                matrix_rows
            )
        )

        write_csv(
            BC_OUT[
                "lookback_summary"
            ],
            lookback_summary,
        )
        write_csv(
            BC_OUT[
                "momentum_summary"
            ],
            momentum_summary,
        )
        write_csv(
            BC_OUT["plateau"],
            plateau,
        )

        # ----------------------------------------------
        # DEEP OUTPUTS FOR TOP 6 + EXACT OLD WINNER
        # ----------------------------------------------
        deep_rows = list(
            matrix_rows[:6]
        )

        old_winner_id = (
            bc_candidate_config(
                30,
                0.50,
            )[
                "config_id"
            ]
        )

        if not any(
            row[
                "candidate_config_id"
            ] == old_winner_id
            for row in deep_rows
        ):
            old_row = next(
                row
                for row
                in matrix_rows
                if row[
                    "candidate_config_id"
                ] == old_winner_id
            )
            deep_rows.append(
                old_row
            )

        period_rows = []
        rolling_rows_out = []
        calendar_rows_out = []
        top_trade_rows = []
        overlap_rows = []

        for rank, row in enumerate(
            deep_rows,
            1,
        ):
            cid = row[
                "candidate_config_id"
            ]
            trades = combined_cache[
                cid
            ]
            rejected = rejected_cache[
                cid
            ]

            combined_config = {
                "config_id": (
                    "AUD_USD_H1_SHORT_BOUNDARY|"
                    + cid
                ),
                "timeframe":
                    "H1",
                "side":
                    "SHORT",
                "family":
                    "COMPRESSION_PLUS_SWEEP",
            }

            period_rows.extend(
                cr_period_rows(
                    combined_config,
                    trades,
                )
            )

            rolling_rows_out.extend(
                rolling_rows(
                    combined_config,
                    trades,
                )
            )

            calendar_rows_out.extend(
                calendar_rows(
                    combined_config,
                    trades,
                )
            )

            for trade in trades:
                top_trade_rows.append(
                    bc_serialise_trade(
                        trade,
                        rank,
                        cid,
                    )
                )

            overlap_rows.append({
                "rank":
                    rank,
                "candidate_config_id":
                    cid,
                "lookback":
                    row[
                        "lookback"
                    ],
                "momentum_threshold":
                    row[
                        "momentum_threshold"
                    ],
                "core_raw_signals":
                    row[
                        "core_raw_signals"
                    ],
                "candidate_raw_signals":
                    row[
                        "candidate_raw_signals"
                    ],
                "accepted_core":
                    row[
                        "accepted_core"
                    ],
                "accepted_candidate":
                    row[
                        "accepted_candidate"
                    ],
                "rejected_core_overlap":
                    row[
                        "rejected_core_overlap"
                    ],
                "rejected_candidate_overlap":
                    row[
                        "rejected_candidate_overlap"
                    ],
                "same_candle_candidate_rejected":
                    row[
                        "same_candle_candidate_rejected"
                    ],
                "core_displaced_count":
                    row[
                        "core_displaced_count"
                    ],
                "core_newly_eligible_count":
                    row[
                        "core_newly_eligible_count"
                    ],
                "rejection_records":
                    len(
                        rejected
                    ),
            })

        write_csv(
            BC_OUT["periods"],
            period_rows,
        )
        write_csv(
            BC_OUT["rolling"],
            rolling_rows_out,
        )
        write_csv(
            BC_OUT[
                "rolling_summary"
            ],
            rolling_summary(
                rolling_rows_out
            ),
        )
        write_csv(
            BC_OUT["calendar"],
            calendar_rows_out,
        )
        write_csv(
            BC_OUT[
                "calendar_summary"
            ],
            calendar_summary(
                calendar_rows_out
            ),
        )
        write_csv(
            BC_OUT["top_trades"],
            top_trade_rows,
        )
        write_csv(
            BC_OUT["overlaps"],
            overlap_rows,
        )

        # ----------------------------------------------
        # DECISION SUMMARY
        # ----------------------------------------------
        best = matrix_rows[0]
        central = plateau[0]

        decision = [{
            "best_candidate_config_id":
                best[
                    "candidate_config_id"
                ],
            "best_lookback":
                best[
                    "lookback"
                ],
            "best_momentum_threshold":
                best[
                    "momentum_threshold"
                ],
            "best_boundary_pass":
                best[
                    "boundary_pass"
                ],
            "best_checks_passed":
                best[
                    "checks_passed"
                ],
            "best_checks_total":
                best[
                    "checks_total"
                ],
            "best_combined_trades":
                best[
                    "combined_trades"
                ],
            "best_combined_pf":
                best[
                    "combined_pf"
                ],
            "best_combined_total_r":
                best[
                    "combined_total_r"
                ],
            "best_marginal_accepted_trades":
                best[
                    "marginal_accepted_trades"
                ],
            "best_marginal_pf":
                best[
                    "marginal_pf"
                ],
            "best_marginal_total_r":
                best[
                    "marginal_total_r"
                ],
            "best_validation_pf":
                best[
                    "validation_pf"
                ],
            "best_last5y_r":
                best[
                    "last5y_r"
                ],
            "best_last2y_r":
                best[
                    "last2y_r"
                ],
            "best_rolling12_positive_pct":
                best[
                    "rolling12_positive_pct"
                ],
            "best_rolling24_positive_pct":
                best[
                    "rolling24_positive_pct"
                ],
            "best_rolling36_positive_pct":
                best[
                    "rolling36_positive_pct"
                ],
            "best_rolling24_worst_r":
                best[
                    "rolling24_worst_r"
                ],
            "best_rolling36_worst_r":
                best[
                    "rolling36_worst_r"
                ],
            "best_cost2_pf":
                best[
                    "cost2_combined_pf"
                ],
            "central_plateau_passes":
                central[
                    "passes"
                ],
            "central_plateau_cells":
                central[
                    "cells"
                ],
            "central_plateau_pass_pct":
                central[
                    "pass_pct"
                ],
            "central_plateau_confirmed":
                central[
                    "plateau_confirmed"
                ],
            "next_step_if_plateau_confirmed": (
                "Freeze the two-trigger AUD/USD H1 SHORT and run exact "
                "current25 -> prospective26 portfolio-add test including "
                "AUD/USD LONG-vs-SHORT non-hedging conflicts."
            ),
        }]

        write_csv(
            BC_OUT["decision"],
            decision,
        )

        write_csv(
            BC_OUT["notes"],
            [
                {
                    "topic":
                        "scope",
                    "note": (
                        "Only sweep lookback and prior-4H rally momentum are "
                        "tested. Core, sweep body, wick, RR, costs and union "
                        "execution remain frozen."
                    ),
                },
                {
                    "topic":
                        "matrix",
                    "note": (
                        "Exactly 24 predeclared cells: LB15/20/25/30/40/50 x "
                        "momentum0.25/0.50/0.75/1.00."
                    ),
                },
                {
                    "topic":
                        "plateau",
                    "note": (
                        "Primary robustness question is the central 3x3 region: "
                        "LB25/30/40 x momentum0.25/0.50/0.75."
                    ),
                },
                {
                    "topic":
                        "ranking",
                    "note": (
                        "Ranking prioritises boundary-pass status, then 36M, "
                        "24M and 12M positive-window consistency, rolling troughs, "
                        "validation PF and marginal contribution."
                    ),
                },
                {
                    "topic":
                        "no_further_filters",
                    "note": (
                        "No sessions, weekdays, EMA regimes, additional trigger "
                        "families, body thresholds, wick thresholds or RR values "
                        "are introduced."
                    ),
                },
                {
                    "topic":
                        "historical_not_forecast",
                    "note": (
                        "All outputs are historical backtests, not forecasts."
                    ),
                },
            ],
        )

        BC_STATUS.update({
            "state":
                "packaging",
            "message":
                "Packaging sweep boundary confirmation",
        })

        bc_pack()

        BC_STATUS.update({
            "state":
                "complete",
            "message": (
                "AUD/USD H1 SHORT sweep boundary confirmation complete"
            ),
            "matrix_cells":
                len(
                    matrix_rows
                ),
            "boundary_passes":
                sum(
                    bool(
                        row[
                            "boundary_pass"
                        ]
                    )
                    for row
                    in matrix_rows
                ),
            "central_plateau_confirmed":
                plateau[0][
                    "plateau_confirmed"
                ],
            "central_plateau_passes":
                plateau[0][
                    "passes"
                ],
            "best_candidate":
                best[
                    "candidate_config_id"
                ],
            "best_combined_trades":
                best[
                    "combined_trades"
                ],
            "best_combined_total_r":
                best[
                    "combined_total_r"
                ],
            "best_rolling24_positive_pct":
                best[
                    "rolling24_positive_pct"
                ],
            "best_rolling36_positive_pct":
                best[
                    "rolling36_positive_pct"
                ],
            "bundle":
                BC_BUNDLE,
            "orders_supported":
                False,
            "trading_enabled":
                False,
        })

    except Exception as error:
        import traceback

        BC_STATUS.update({
            "state":
                "error",
            "message":
                str(error),
            "error_type":
                type(error).__name__,
            "traceback":
                traceback.format_exc(),
            "orders_supported":
                False,
            "trading_enabled":
                False,
        })

        print(
            "AUDUSD H1 SHORT BOUNDARY CONFIRMATION ERROR:",
            repr(error),
            flush=True,
        )


@app.route(
    "/audusd-h1-short-boundary/status"
)
def bc_status():
    return jsonify(
        BC_STATUS
    )


@app.route(
    "/audusd-h1-short-boundary/results"
)
def bc_results():
    if not os.path.exists(
        BC_BUNDLE
    ):
        return jsonify({
            "status":
                "not_ready",
            "state":
                BC_STATUS[
                    "state"
                ],
            "message":
                BC_STATUS[
                    "message"
                ],
        }), 404

    return send_file(
        os.path.abspath(
            BC_BUNDLE
        ),
        as_attachment=True,
        download_name=
            BC_BUNDLE,
    )


@app.route(
    "/audusd-h1-short-boundary/info"
)
def bc_info():
    return jsonify({
        "service": (
            "AUD/USD H1 SHORT Sweep Boundary Confirmation"
        ),
        "read_only":
            True,
        "orders_supported":
            False,
        "frozen_core": {
            "body_atr_min":
                1.25,
            "range_atr_min":
                1.50,
            "compression_max":
                0.85,
            "breakout_lookback":
                15,
            "rr":
                3.50,
        },
        "frozen_sweep": {
            "body_atr_min":
                BC_BODY,
            "wick_body_min":
                BC_WICK,
            "rr":
                BC_RR,
        },
        "lookbacks":
            BC_LOOKBACKS,
        "momentum_thresholds":
            BC_MOMENTUMS,
        "matrix_cells":
            len(
                BC_LOOKBACKS
            )
            * len(
                BC_MOMENTUMS
            ),
        "central_plateau": {
            "lookbacks":
                [25, 30, 40],
            "momentum_thresholds":
                [0.25, 0.50, 0.75],
        },
        "routes": [
            "/audusd-h1-short-boundary/status",
            "/audusd-h1-short-boundary/results",
            "/audusd-h1-short-boundary/info",
        ],
    })


if __name__ == "__main__":
    threading.Thread(
        target=run_bc_research,
        daemon=True,
    ).start()

    app.run(
        host="0.0.0.0",
        port=int(
            os.getenv(
                "PORT",
                "5000",
            )
        ),
        debug=False,
    )
