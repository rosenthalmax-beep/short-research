# EURAUD_H1_SHORT_FINAL_OUTSIDE_REVERSAL_REGIME.py
# Research-only, three predeclared cases. Adapted directly from the archived
# EURAUD_H1_BOTH_BROAD_DISCOVERY.py for exact control parity.
# OANDA_TOKEN required on separate Railway research service.
# Python packages: flask numpy requests.
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
# EUR/AUD H1 LONG + SHORT — NEW-PAIR BROAD DISCOVERY
# ============================================================
#
# PURPOSE
# -------
# First-pass independent LONG and SHORT discovery on EUR/AUD H1.
#
# This deliberately starts from scratch. It does NOT import parameters
# from the existing live portfolio and does NOT assume the winning family
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
#   broad raw H1 long and short geometry at fixed 3.5R
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
#   pre-2018 / 2018+ temporal split (both discovery-inspected)
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
# H1 assumed adverse entry cost:
#   2.0 pips = 20 EUR/AUD ticks; assumption, not verified market spread
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
# confirmation, exact portfolio integration against the frozen 27-strategy
# live portfolio, and a separate implementation review before deployment.
#
# READ ONLY. NEVER SENDS ORDERS.
# ============================================================

app = Flask(__name__)

TOKEN = os.getenv("OANDA_TOKEN")
BASE = os.getenv("OANDA_API_URL", "https://api-fxtrade.oanda.com")

PAIR = "EUR_AUD"
PAIR_LABEL = "EUR/AUD"

REQUESTED_START = datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc)
NOW = datetime.now(timezone.utc).replace(second=0, microsecond=0)

VALIDATION_START = datetime(2018, 1, 1, tzinfo=timezone.utc)
PRE2010_END = datetime(2010, 1, 1, tzinfo=timezone.utc)

TICK = 0.00001
PIP = 0.0001
STOP_BUFFER_TICKS = 10

H1_PRIMARY_COST_PIPS = 2.00
M15_PRIMARY_COST_PIPS = 2.00
COST_MULTIPLIERS = [0.5, 1.0, 1.5, 2.0]

STAGE1_RR = 3.50
RR_GRID = [2.50, 3.00, 3.50, 4.00, 4.50, 5.00]

STAGE1_KEEP_PER_FAMILY = 5
FINAL_KEEP_PER_STREAM = 8

OUTS = {
    "coverage": "euraud_h1_both_discovery_coverage.csv",
    "stage1": "euraud_h1_both_discovery_stage1.csv",
    "stage1_shortlist": "euraud_h1_both_discovery_stage1_shortlist.csv",
    "stage2_rr": "euraud_h1_both_discovery_stage2_rr.csv",
    "finalists": "euraud_h1_both_discovery_finalists.csv",
    "family_summary": "euraud_h1_both_discovery_family_summary.csv",
    "periods": "euraud_h1_both_discovery_periods.csv",
    "cost_stress": "euraud_h1_both_discovery_cost_stress.csv",
    "calendar": "euraud_h1_both_discovery_calendar_years.csv",
    "calendar_summary": "euraud_h1_both_discovery_calendar_summary.csv",
    "rolling": "euraud_h1_both_discovery_rolling.csv",
    "rolling_summary": "euraud_h1_both_discovery_rolling_summary.csv",
    "plateau": "euraud_h1_both_discovery_plateau.csv",
    "trades": "euraud_h1_both_discovery_finalist_trades.csv",
    "notes": "euraud_h1_both_discovery_notes.csv",
}

BUNDLE = "EURAUD_H1_BOTH_DISCOVERY_RESULTS.zip"

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
                if status_code in (400, 404) and not by_time:
                    # Some instruments have no data before their inception.
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


 
# ============================================================
# FINAL, PREDECLARED EUR/AUD H1 SHORT REGIME STUDY
# This is a standalone READ-ONLY research service.
# It reuses the ORIGINAL May-2026/September-2026 discovery geometry and
# execution simulator above to enforce frozen-control trade-by-trade parity.
# No order endpoint or executor connection exists in this script.
# ============================================================
import json
import base64
import zlib
import hashlib
import bisect
from zoneinfo import ZoneInfo

REF_ARCHIVE_HASH = "37b125764b06271e9d4aa84c2f940a35bf217b8ff2281f08d184b56eb409f92c"
REF_ARCHIVE_B85 = "c-pm{OK)8{a)AGf*P{mOP0lWhAj<@aRtbVZXT~!z&~XQD4=_QH|DNJGSJL65$ZP9b5VYW_Zs~k3)|<Tl_P-x~`}CL3KmYQFPoMwv@oyh~{{gb)55N2H@o%5LzOG@Cx^|QN`su$u9*aqy5<ew-PVoDb{QF;D{=-fL@^jKRkuM+r{PD}j&wu>*hmW7Xe)*sJOMg-;DE{m}KmGRg*Ds&`=)dRBpMLq}<yorF{K<cx3)zl^uRnkJ%g3+hl1S$A`t|w>Uq1f)+pnKp<X@iu@t=<0{o7xE@nv6mf}fJF{OPY>e*XID*Ux|WkDtH%b^u5p{_l4K7h;v33$c}ei#;WIMqF`m5q$>Sa7kQ3E{RRj>lZYLQjrdk6o893y#o=?2x$`P2`O-(IRH<XpCK;@0X`*s23r%7LIEEsm|G@@9TF9KQ<`D}lIOqs=YRP7=N1b7RQ#<9DOf_ltTsX9|7a6LhB_cJv?(I>mw!t13=|?dMfCZ2zEmQ>P=`9T$vvW+;DSisp*2$^nG*I$G6@7o+}?0wOhzPpAlYb*o%>~q#9#Bu#BEJ)Y%=BVP@60UjJKc06GU8Qh)Tu*QOTSFMEZnfWs+Y+pllFfs5SnmA%t!SFJCP!gT!xUC+VHEEEhJ?%m{8+vP@0&3>Q#NrRN1y1ZTjk?jcxivqJ>oIhCCc%Mkl8E1eI+L9Ntn{TnE}SFO-@troXs2qrqU*E9ts%7d@>MS}p89BRWWQ1V2nVL1iGY!-oH7HdFpPTYYaF8T(F@6{+REXWSo|23%0%r*v~isA^WWDP*+6G-FQM^IX$BG?#i$r(!I-LkcI${k_Oo$)pR{C<m#Q$G(?-3A)ISV9_z8?`w_cAh?C=8y&|H#D+=_1?bZ<^H-G%v?)6*x`{2u*6(+6(hci(2h$%47rr!MK4-{PzD!I@R-1G#svWmU)81*aKak6<imk`!VufzSCNgrxFa`>o%~@cFJ7LHprdi~BIzMH#dIGbo$B0jAuJ`}l54KDuqvlWi*C+^IE)7iVWTe<V(+Jn8Q-|@qeDV;Z?7Ymf*OD+{=M2rhX~*^Zp^L8nO9Cj9?{%5aB~dWMLH<?Uawt{#o1AHM>b6%Y?_Cnc$6`~5i5BP$CD^GA&Wfg6e*kCgg+(8Fn8MZnU#yv_60R*tc)+Y%D_8R#ile2-oo#<MSras4c8OuixC}-SCLJuS85IrsER*>Ch;t{Xyc+PMcgwy*a7)_lTKim#xQ)}z#0b99!6G)42nOa#-O-80q9{gg$%xX3yCLFXoAAoXl8fkTgdl0j+{JV2soYICa*XhB0EQ&l|!ZRXRXrs8+CR!lQp^c!|iT9W0jVR=QI#-sa2d+VD}=jwYwQ9%EtW}w8>_HK7cV1weP2)(|R9r8KM?KNi!vZ^pxPpA|--x6X8aWrqSGO7f+)(PrRF`#a&0~oRIjY(C^41>GH){?>50iYGY;86L$?jQO%{VDlvkhnhxy3JGLy5*7!4Ow1#t^4Y1~Sg)R`lC@{jf)l23ua1qnpM5uA#Qy88oMkctvJGL+cdm08=-%JbLn^JoW<gDFRd3<&h!v{|lRnu9=a=K~>ILWbfY{kh%>^W`f>EuC3WP+*Wl7o6WC+djRAQj)F<sE4@%@GwFip3;n4SR-2k81gHU)Ad9Gr&fblsPJ?`!l45i=DZ|Rh7tUX#4F&bu66Sp|g<b{nb&*mWOhmFP2#d2vDtXt*VD!>rF&9s+256<p@fen{otsz8jZFv5@MI%fKn;G1H`ccH9C)wy21nBJS7|><}e`5K$6n5e>};G~afQ$-w7e3c<L?3Q-J(VCCfD=*Ymf0!k(jIeFhcrE&6(;82OH@lldxyEAa|iP){&`4eY8-mS}oljr++h?whQ#Hk$3JzQ}*MRt44twD}CQEoY9?J=QTB4&AT!o?nsjaWQE=Q$Qnv;o)ZQ$^%C?XW<?P<`O+v0V0**kEo7J*20nb9GTSX9E|dX}7#~d7--Qg}pgN^47Z}N~kXWtW_7p((Nh)W_V0tYpyS9H0q1mv_gDzzmMsPC6_}4Hq#aL#X>H%8`*LZPNA!U3|dyX`a2m(;xaI*iySbjN#-Ise-+vFT3j6vLft8CIOzFmpHCnXJ*Fx;-BaRe@ZhR^QPP|X43hx^7r@@lPr2}!VotfZ@;0^x<!l;Xxe##v_V#b~Oq&%wjr80eZPxxJn}cG45!4ye&Oy}zsjWewZH==3ktK=;YssmJ2hp8G9|UT~vI2Gg&I&Xr=RAVR)5<YWHMzK~Xq+OvjRH}k9wtg|O^*t6EllLJ&L-}sGR;JV2u6EDG*7eWm=>_6(l=7Udn(9SghDPwpy};m$$e~tu0Z)A`DX-1$VM<C8i1+lurkD_$Q|lph7|52OHKZXIT`o+NG(0aLroJ-VS^!?@<oCGjMVm2JdpD$va9iUxXqz~$!_9WjXC}36^!UL1(ioPV_Y@HzMU^B2VktG*^8pii^wiZ;F}$pRxJFj8ymxPHbkx!i*&;KP8!oGq&jRcZXZ`n3t*IEAY{3HO6}KND;IMZKpesVXtac}^f0Js$`9E-IJYat<J6J3f227WNHrr`8eA8tU8)@zu&}W1&)UK|;~5pVU@ng6T_eUg?Y~G#JgQ8Y4_^Zx-Zw4!cQ+B(w;aN*l)Hdt^GJml<_3al4{}K`pD<P)N`1a)901}Yg5eb)t`W@Ez~o%Q;(Bhz)wob!tYOpZ9LYPXdvm=}Kuwd$YlQ%dtS3aPi2A15rcA8aLN1!6w3Xs>+VBdd7Es!P`(3F)$?BVe5-6KZt5|8SKZN5Xk}oO+pvuvmp%tidsMw}1QRf|_!4iKa(G))mq_MhQ_%Y_0=8Qb}9T`c)sJ0cBe(Ml%W!ep?Gee9B+NJ~dmWM$ptu*QWLSLFwu`DAMD>0Sok#tR^Gfn}O6!!J_ESO)oaDOhwbK!yyNuQ=sPMy{rwS_~|a~|nPs~vSa^|5bKDaTDJZlj#jxi^-FOnxL5Pt67_5(Iy8G+dTNtW!kySaMt#u2q;e#>q-UQ+%T?fkie#kD$Oq?c!n5)Fe++29-&*PeqUdQ0I{tU4g=yWbQ_BID^EXfI3j;{Pz`<$E*3<T~|p2?g;BlrRCFl3<hZKy3}Ndntf3tP<(X5%L>#XvNJ%G0mCk5hSYXB-RBQ27)$n$p{053lWiO%@Wqfz9d~R>c_WuY1UD{SP{K6JpS5Y0(77$Iu`<-5$#3uARm1hFbw_Z1Uvgdtdq+!MXnH=cQYc3wYPr)~WPj31vWDYKxtrN9*c<9vvbcMMN#X754C?L+pe+#&6&*{8fw?~VbgSC>AyN;*Z1M_8mO?n`HbV7E2Wex4?w#5xf}W{SI@*X+8<eK8p$V3elgua9LF-N;SQU6)M7HsB#)VtM-mP(867+=(4@uqU`r{sJaQ9ZrkaI3@3{Nb)*eQ~?eLW^l;iwK#?xJSYq44Fd?lBd?d9lZx91iB$7w5H7dzt);-PVi9Hf~Oq!?-z_wsA8+hQ8P%`7wv3=NjXNcRUGPNGRaKM+XX5Tn>@#P7(`8ZMhy7i!xD7xlb+XxZV$JKpdx%hNHphY}d>f>0Wovb7s3B+Rirpe6ZA@o25+BC!-##rH71HV8n$cTyk1*0o-u0dj@Vb#{;*VyMbHnt*{m=-yZUKfR{-;W2>*2rpbj)Lmszn<2<`BiUpwZ9#lT^`dul*6RWV6;Zch8$weLthNM5R6I}Bfoq(|vgQ3mNIz)D}1Y96I!RgQ1Xpjr{13K>O#U6YNuO@;MEs^<bn;p|6ymH?5GH|jp#U-3o;_h%FmqLB$R~vrco`BY|16GeDMVxoyyW2KSzKm7}H3gWCQONaXf@92fx0wVWJ`dr6ej6jh*~_?sa(3x4v$gZhM3W&vU!<v<%Eu&IS~Vr|%W&-u3}y%uP-~BdyVLBChC_2q<Zk^@a_^(5)Uh?2j^za2++;krSQjb4blRM}!gSi4?HPKO_zXQ$=}(PAe}>-DV=Aa~^)b^jaxsgz9Ofr2)#nt+yGvnIIJ^}`sj<a+^P0`6abbB#_%}Dp`SSm~EY45T<XiuIk(0lO>~botlM@jaEY}gO_UarW?hq+7tsS|0O50-uWKK}Pf6UGX!}QErE(}8hxi028vvUA&(Lg>$%Dxz81;S#Oo#(7BdyM;lN7^}Ud+_;(k_ruV3tyZ^%;&*aST#sb5!~O*P{Ja!Kk8~<!!;+8Grs4@!&FBK(pamgizQ$oM-6Am;t<){VTBZ5`cjLboxSM~*K5M-Sl2u3&HR!jG8?)0A_iQJWMNTAeihl>QgMqPrd&KEZ*OCubF%o##VwNjIR8|t85+27VhFgLu`jQ<l)G-e3#YLaQ+u!5IIB}D!QT_zSgPl=7(KQ!nOBhKl!V67H(%5|r&Mph!&PPUMPzqLms0GiT1W)F)pc$~R@J{bq&mQ*?xO^f^4SErFH$ZF<K2>Vab|k(^+<KtEPfN=U8qw+%agimxd8hFLK8HDUk`Rp;3_wEX6h;hT<YY2Ew<QA<c>=>B|M#{qD^3|nykw})$_>j$w1v)Sxr8%7IgIP;06-r$#rGEI=!C&&!c#&Q)q7@yEcu+A)!CRHif7ssfX}}?yYud_7uc}1EOS%D0kmJm2T509UeMkh#4NP8G3vW|0Z%rhtKnXsN5yY?X@|=87-b5B)L|;-g972=9IW38!2&t6j0LR@(*26Iz`GZMg|Qj+3l9FHzgt-Vq^t<^p#~vZE}}eU9|v5Ip!s4h2s?2*<^@YS04Sftt<BtV>n30kExN)$z0EHy{;bZkr|mfm$GhlzKU#%q`8FWi2RxLMN%H8P4(M*!fqxA6+FR~dZCWodrykTS<K_*h~U~ft6(4<b0@UOC{7XN?LfCu+^6wpfKjH-+z-9$^=9Pvof)yYK4IeVJb<stbN~bC$h>87{OTr>_n7KDTzK22w#P<PK!)2k0hXtQqe&Ha^jf#6(Y(;C5h<}UAvG^GpBQR89oD1(@3@yVg?$@;*7j|bsV@~O`Cs{%RBXQ2>d}q~8kpagjOYBnuFUnTh-{T8&Qe*sEY(%L4;Cq3N4@dES*kNIoop{pI4B^4$Ki=3nd8>qJ(&_-L*&oeYlwV%^%G8YgjOFy1|p>?W4G^EwEzvRFWG(**_AdlzSqm2wfB0dj#<1Yl^>XS`Co@=Btz$&f-^GbQ9fIdIc_T27B>hjc3mi4(MNPB`p`9Xe@B(jY>vjs!-&SMV40(-14~PH#kZNJ?Je4LUN1aHbN2tQXm}|7G^ZTj^+^x`8iq7OLlC3Mi<_BlBIS-!A%!6`b%utNhH4L%Z+GL@iy6~dlpQQ`u)92!r3t58B2IYBldeqiDY6^jQI0S9a^DnQ@^##x-dkr+X&#bh1j>{d$D)R>&Xq}S*OM-SomY`fSm-Hd2+2I)YpRC^=b-BK(#0ggkIqM3i^q$Gd=;m+H{kIZA+WlW{uHUn#3uGLOBUj=+ZoJP95x)TuAS_=Lu2s3IQt)GuBpZB{@9opCcc~)VHz9j?Jj}^AH79F+c3w@G2i)z8+7vqYW<|Lx`FzRTPdx6K#Sy3@0x<MPjcU_a2^sXOg;s|BZ?G|I<FYS6{*v=@3M&aLm;4?k<)QkY04QNa*z!I4bt;+<c^Et^mc7LJ}d-m74zur!wj~L-ECUv*xfg^j-5q@6hf~jN#yQ=>cxd|xj)$=gFMk7!|7xX9<%H~t9tV(V)yv@l4D12U1LYamSDcO8n4d%^?T;rgO3iW7ue+q9GT8%)Hg53$v;`aIYsieFNpDc2Bg*y)l2q=QV%`mlCOx2DNM+m^XPLjHSEc2GO)jZNFm4f`a|yS^{)k~o(z6ZL#L+yP|vj0j~6ox0>pp+e?;Xj)c"
REFERENCE = json.loads(zlib.decompress(base64.b85decode(REF_ARCHIVE_B85)).decode())
REFERENCE_COUNT = 99
REFERENCE_R = 30.413898320646485
REFERENCE_H1_CANDLES = 137837
REFERENCE_H1_FIRST = "2004-05-31T20:00:00Z"
REFERENCE_H1_LAST = "2026-09-21T11:00:00Z"
REFERENCE_LAST = parse_time(REFERENCE_H1_LAST)
REFERENCE_ID = "H1|SHORT|OUTSIDE_REVERSAL|body_atr_min=1|lookback=15|distance_atr_max=0.1|close_location=0.15|rr=3"
CONTROL_CONFIG = {
    "timeframe": "H1", "side": "SHORT", "family": "OUTSIDE_REVERSAL",
    "body_atr_min": 1.0, "lookback": 15,
    "distance_atr_max": 0.10, "close_location": 0.15,
    "rr": 3.0, "config_id": REFERENCE_ID,
}
# Exactly three cases. No EMA searches, combinations, RR sweeps or extra filters.
CASES = ["CONTROL", "H4_EMA50_BELOW_EMA200", "D1_CLOSE_BELOW_EMA200"]
RESEARCH_END = REFERENCE_LAST
OUTPUT_DIR = Path(os.getenv("RESEARCH_OUTPUT_DIR", "."))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
BUNDLE = str(OUTPUT_DIR / "EURAUD_H1_SHORT_FINAL_REGIME_RESULTS.zip")
STATUS = {
    "state":"not_started", "message":"Not started", "pair": PAIR,
    "orders_supported":False, "trading_enabled":False,
    "frozen_config_id":REFERENCE_ID, "cases":CASES,
    "reference_trades":99, "reference_total_r":REFERENCE_R,
    "four_pip_cost_is_assumed":True,
}

OUTFILES = [
    "coverage.csv", "control_parity.csv", "control_parity_mismatches.csv",
    "regime_matrix.csv", "regime_removed_and_added_trades.csv",
    "all_candidate_trades.csv", "periods.csv", "rolling_summary.csv",
    "cost_stress.csv", "decision_gates.csv", "study_notes.csv",
]

def out(name): return str(OUTPUT_DIR / name)

def emit(name,rows):
    write_csv(out(name), rows)

def package():
    with zipfile.ZipFile(BUNDLE,"w",zipfile.ZIP_DEFLATED) as z:
        for fn in OUTFILES:
            if Path(out(fn)).exists(): z.write(out(fn),fn)

def archive_integrity():
    raw=json.dumps(REFERENCE,separators=(',',':')).encode()
    if hashlib.sha256(raw).hexdigest()!=REF_ARCHIVE_HASH:
        raise RuntimeError("EMBEDDED REFERENCE LEDGER HASH FAILED")
    if len(REFERENCE)!=REFERENCE_COUNT:
        raise RuntimeError("Embedded control ledger is not 99 trades")
    if abs(sum(float(x['result_r']) for x in REFERENCE)-REFERENCE_R)>1e-8:
        raise RuntimeError("Embedded control total R changed")
    if config_id(CONTROL_CONFIG)!=REFERENCE_ID:
        raise RuntimeError("Frozen entry geometry changed")
    return True

def h1_control_parity(h1):
    ts=[c['time'] for c in h1]
    cutoff=bisect.bisect_right(ts, REFERENCE_LAST)
    checks=[
        ("archive_sha256",archive_integrity(),True),
        ("candle_count_at_frozen_cutoff",cutoff,REFERENCE_H1_CANDLES),
        ("first_h1",iso(h1[0]['time']) if h1 else "",REFERENCE_H1_FIRST),
        ("last_frozen_h1",iso(h1[cutoff-1]['time']) if cutoff else "",REFERENCE_H1_LAST),
    ]
    emit("control_parity.csv",[{"check":k,"actual":a,"expected":b,"pass":a==b} for k,a,b in checks])
    if not all(a==b for _,a,b in checks):
        package(); raise RuntimeError("CANDLE COVERAGE/PARITY FAILURE: baseline must not drift")
    frozen=h1[:cutoff]
    features=build_features(frozen,"H1")
    raw=signal_indices(CONTROL_CONFIG,features)
    trades=backtest(CONTROL_CONFIG,features,raw,rr=3.0,cost_multiplier=1.0)
    checks.extend([("accepted_control_trades",len(trades),REFERENCE_COUNT),
                   ("control_total_r",sum(float(t['result_r']) for t in trades),REFERENCE_R)])
    diffs=[]
    def compare_numeric(field,a,b,i):
        if abs(float(a)-float(b))>1e-8:
            diffs.append({"trade":i,"field":field,"actual":a,"expected":b})
    for i in range(max(len(trades),len(REFERENCE))):
        if i>=len(trades) or i>=len(REFERENCE):
            diffs.append({"trade":i,"field":"missing_or_added","actual":i<len(trades),"expected":i<len(REFERENCE)})
            continue
        t=trades[i];r=REFERENCE[i]
        for key in ["signal_index","exit_index","duration_bars"]:
            if int(t[key])!=int(r[key]): diffs.append({"trade":i,"field":key,"actual":t[key],"expected":r[key]})
        for key in ["signal_time","exit_time"]:
            if iso(t[key])!=r[key]: diffs.append({"trade":i,"field":key,"actual":iso(t[key]),"expected":r[key]})
        if t['exit_reason']!=r['exit_reason']:
            diffs.append({"trade":i,"field":"exit_reason","actual":t['exit_reason'],"expected":r['exit_reason']})
        for key in ["reference_entry","historical_fill","stop","target","result_r"]:
            compare_numeric(key,t[key],r[key],i)
    emit("control_parity_mismatches.csv",diffs)
    checks[-1]=(checks[-1][0],round(checks[-1][1],10),round(checks[-1][2],10))
    checks.append(("trade_field_mismatches",len(diffs),0))
    checks.append(("raw_signals_in_frozen_data",len(raw),len(raw)))
    emit("control_parity.csv",[{"check":k,"actual":a,"expected":b,"pass":a==b} for k,a,b in checks])
    if diffs or len(trades)!=99 or abs(sum(t['result_r'] for t in trades)-REFERENCE_R)>1e-8:
        package(); raise RuntimeError("FROZEN 99-TRADE CONTROL PARITY FAILED. No regime results generated.")
    return frozen,features,raw,trades,checks


def ema_sma_seed(closes,length):
    values=np.asarray(closes,dtype=float)
    out=np.full(len(values),np.nan)
    if len(values)<length:return out
    out[length-1]=float(np.mean(values[:length]))
    alpha=2.0/(length+1.0)
    for i in range(length,len(values)):
        out[i]=out[i-1]+alpha*(values[i]-out[i-1])
    return out

NY_ZONE=ZoneInfo("America/New_York")
def higher_close_time(c,granularity):
    t=c['time']
    if granularity=="H4": return t+timedelta(hours=4)
    if granularity=="D":
        local=t.astimezone(NY_ZONE)
        return (local+timedelta(days=1)).astimezone(timezone.utc)
    raise ValueError(granularity)

def asof_completed(close_times,values,signal_time):
    # Signal occurs at H1 candle close; a higher bar ending at exactly the
    # same instant is strictly complete, not a future candle.
    ix=bisect.bisect_right(close_times,signal_time)-1
    if ix<0: return None,None
    v=values[ix]
    return (float(v) if np.isfinite(v) else None), ix

def build_regime_maps(h1,raw,h4,d1):
    if not h4 or not d1: raise RuntimeError("Missing H4 or daily midpoint candles")
    h4_close_times=[higher_close_time(c,"H4") for c in h4]
    d_close_times=[higher_close_time(c,"D") for c in d1]
    if any(b<=a for a,b in zip(h4_close_times,h4_close_times[1:])): raise RuntimeError("Invalid H4 chronology")
    if any(b<=a for a,b in zip(d_close_times,d_close_times[1:])): raise RuntimeError("Invalid daily chronology")
    h4cl=[x['close'] for x in h4]; dcl=[x['close'] for x in d1]
    h4f=ema_sma_seed(h4cl,50);h4s=ema_sma_seed(h4cl,200)
    d200=ema_sma_seed(dcl,200)
    h4map={};dmap={};state=[]
    for ix in raw:
        ix=int(ix); evaluation_time=h1[ix]['time']+timedelta(hours=1)
        f,hi=asof_completed(h4_close_times,h4f,evaluation_time)
        s,_=asof_completed(h4_close_times,h4s,evaluation_time)
        d,di=asof_completed(d_close_times,d200,evaluation_time)
        dc=dcl[di] if di is not None and d is not None else None
        h4map[ix]=f is not None and s is not None and f<s
        dmap[ix]=d is not None and dc<d
        state.append({"signal_index":ix,"signal_time":iso(h1[ix]['time']),
                     "h1_signal_evaluated_at":iso(evaluation_time),
                     "h4_last_completed_at":iso(h4_close_times[hi]) if hi is not None else "",
                     "d1_last_completed_at":iso(d_close_times[di]) if di is not None else "",
                     "h4_ema50":f,"h4_ema200":s,"h4_pass":h4map[ix],
                     "d1_close":dc,"d1_ema200":d,"d1_pass":dmap[ix]})
    return h4map,dmap,state

def period_summaries(case,trades):
    windows=[("FULL",None,None),
             ("PRE_2018",None,VALIDATION_START),
             ("2018_PLUS",VALIDATION_START,None),
             ("PRE_2010",None,PRE2010_END),
             ("2010_PLUS",PRE2010_END,None),
             ("LAST_5Y",REFERENCE_LAST-timedelta(days=365.25*5),None),
             ("LAST_3Y",REFERENCE_LAST-timedelta(days=365.25*3),None),
             ("LAST_2Y",REFERENCE_LAST-timedelta(days=365.25*2),None),
             ("LAST_1Y",REFERENCE_LAST-timedelta(days=365.25),None)]
    return [{"case":case,"period":name,**period_metrics(trades,a,b)} for name,a,b in windows]

def rolling_stats(trades,months):
    # Entry-dated closed-trade R. Counts empty windows as zero, not positive.
    first=month_floor(parse_time(REFERENCE_H1_FIRST))
    last=month_floor(REFERENCE_LAST)
    windows=[]; m=add_months(first,months)
    while m<=last:
        start=add_months(m,-months)
        items=trades_in_period(trades,start,m)
        windows.append((start,m,len(items),sum(float(x['result_r']) for x in items)))
        m=add_months(m,1)
    active=[x for x in windows if x[2]>0]
    return {"months":months,"windows":len(windows),"active_windows":len(active),
            "positive_all_pct":100*sum(x[3]>0 for x in windows)/len(windows) if windows else 0,
            "positive_active_pct":100*sum(x[3]>0 for x in active)/len(active) if active else 0,
            "worst_r":min((x[3] for x in windows),default=0),
            "best_r":max((x[3] for x in windows),default=0),
            "worst_start":iso(min(windows,key=lambda x:x[3])[0]) if windows else "",
            "worst_end":iso(min(windows,key=lambda x:x[3])[1]) if windows else ""}

def emit_trades(case,trades):
    return [{"case":case,**{k:(iso(v) if isinstance(v,datetime) else v) for k,v in t.items()}} for t in trades]

def run_final_study():
    try:
        archive_integrity()
        STATUS.update(state="fetching",message="Fetching fresh full-history H1 candles")
        h1=fetch_history("H1",REQUESTED_START,NOW,180)
        STATUS.update(state="parity",message="Rebuilding 99-trade frozen control")
        h1,features,raw,core,checks=h1_control_parity(h1)
        STATUS.update(state="fetching",message="Fetching completed H4 candles")
        h4=fetch_history("H4",REQUESTED_START,NOW,180)
        STATUS.update(state="fetching",message="Fetching completed D candles")
        d1=fetch_history("D",REQUESTED_START,NOW,360)
        if len(h4)<200 or len(d1)<200: raise RuntimeError("Not enough H4/D1 candles to seed regimes")
        emit("coverage.csv",[{"instrument":PAIR,"timeframe":"H1","frozen_candles":len(h1),
             "h4_candles":len(h4),"d1_candles":len(d1),
             "first_h1":iso(h1[0]['time']),"last_h1":iso(h1[-1]['time']),
             "last_h4":iso(h4[-1]['time']),"last_d1":iso(d1[-1]['time']),
             "control_trades":len(core),"control_r":sum(x['result_r'] for x in core),
             "reference_cost_pips":2,"stress_cost_pips":4,"live_orders_sent":0}])
        STATUS.update(state="calculating",message="Applying exactly two predeclared independent completed-HTF regimes")
        h4map,dmap,state=build_regime_maps(h1,raw,h4,d1)
        conds={"CONTROL":set(map(int,raw)),
               "H4_EMA50_BELOW_EMA200":set(int(i) for i in raw if h4map[int(i)]),
               "D1_CLOSE_BELOW_EMA200":set(int(i) for i in raw if dmap[int(i)])}
        control_index={int(x['signal_index']):x for x in core}
        matrices=[];differences=[];alltrades=[];periods=[];rolling=[];cost=[];gates=[]
        control_rolling={months:rolling_stats(core,months) for months in [12,24,36]}
        core_total=sum(float(t['result_r']) for t in core)
        for case in CASES:
            accepted=sorted(conds[case]); filtered=backtest(CONTROL_CONFIG,features,accepted,rr=3.0,cost_multiplier=1.0)
            stressed=backtest(CONTROL_CONFIG,features,accepted,rr=3.0,cost_multiplier=2.0)
            current_index={int(x['signal_index']):x for x in filtered}
            removed=[x for ix,x in control_index.items() if ix not in current_index]
            added=[x for ix,x in current_index.items() if ix not in control_index]
            common=[x for ix,x in current_index.items() if ix in control_index]
            for x in removed: differences.append({"case":case,"classification":"REMOVED_OR_DISPLACED_CORE","signal_time":iso(x['signal_time']),"result_r":x['result_r'],"signal_index":x['signal_index']})
            for x in added: differences.append({"case":case,"classification":"NEWLY_ACCEPTED_AFTER_FILTER","signal_time":iso(x['signal_time']),"result_r":x['result_r'],"signal_index":x['signal_index']})
            for x in common:
                y=control_index[x['signal_index']]
                if abs(x['result_r']-y['result_r'])>1e-8 or x['exit_index']!=y['exit_index']:
                    raise RuntimeError("Unchanged signal unexpectedly changed outcome")
            sm=metrics(filtered);four=metrics(stressed)
            last5=period_metrics(filtered, REFERENCE_LAST-timedelta(days=365.25*5))
            last2=period_metrics(filtered, REFERENCE_LAST-timedelta(days=365.25*2))
            early=period_metrics(filtered,end=VALIDATION_START)
            late=period_metrics(filtered,start=VALIDATION_START)
            rr24=rolling_stats(filtered,24);rr36=rolling_stats(filtered,36)
            changes={"case":case,"raw_signals":len(raw),"raw_pass":len(accepted),
                     "core_accepted":len(core),"accepted":len(filtered),
                     "unchanged_core":len(common),"removed_core":len(removed),
                     "removed_core_r":sum(x['result_r'] for x in removed),
                     "newly_accepted":len(added),"newly_accepted_r":sum(x['result_r'] for x in added),
                     "net_change_r":sm['total_r']-core_total,
                     "four_pip_total_r":four['total_r'],
                     "last5_trades":last5['trades'],"last5_r":last5['total_r'],
                     "last2_trades":last2['trades'],"last2_r":last2['total_r'],
                     "pre2018_trades":early['trades'],"pre2018_r":early['total_r'],
                     "post2018_trades":late['trades'],"post2018_r":late['total_r'],
                     "rolling24_positive_all_pct":rr24['positive_all_pct'],
                     "rolling36_positive_all_pct":rr36['positive_all_pct'],
                     "rolling24_worst_r":rr24['worst_r'],
                     "rolling36_worst_r":rr36['worst_r'],
                     **sm}
            matrices.append(changes)
            alltrades+=emit_trades(case,filtered)
            periods+=period_summaries(case,filtered)
            for months in [12,24,36]: rolling.append({"case":case,**rolling_stats(filtered,months)})
            cost.append({"case":case,"assumed_entry_cost_pips":2,**sm})
            cost.append({"case":case,"assumed_entry_cost_pips":4,**four})
            # Gate is intentionally demanding and cannot itself authorise deployment.
            cond={"at_least_60_accepted_trades":sm['trades']>=60,
                  "pre_2018_at_least_15":early['trades']>=15,
                  "2018_plus_at_least_20":late['trades']>=20,
                  "last5_at_least_10":last5['trades']>=10,
                  "last2_at_least_3":last2['trades']>=3,
                  "positive_recent_2y_r":last2['total_r']>0,
                  "two_pip_pf_at_least_1_35":sm['profit_factor']>=1.35,
                  "four_pip_r_positive":four['total_r']>0,
                  "rolling24_all_positive_pct_at_least_70":rr24['positive_all_pct']>=70,
                  "rolling36_all_positive_pct_at_least_75":rr36['positive_all_pct']>=75,
                  "no_worse_standalone_max_dd":sm['max_drawdown_r']>=metrics(core)['max_drawdown_r']-1e-8,
                  "no_worse_worst_24m_r":rr24['worst_r']>=control_rolling[24]['worst_r']-1e-8,
                  "not_discarding_majority_of_core_r":sm['total_r']>=0.70*core_total,
                  "positive_removed_trade_attribution":sum(x['result_r'] for x in removed)<=0,
                  "meaningful_24m_or_36m_smoothing":(rr24['positive_all_pct']>=control_rolling[24]['positive_all_pct']+3.0 or rr36['positive_all_pct']>=control_rolling[36]['positive_all_pct']+3.0),
                  }
            if case=="CONTROL": cond={k:False for k in cond}
            gates.append({"case":case,**cond,"gate_pass":case!="CONTROL" and all(cond.values()),
                          "next_step":"EXACT_PORTFOLIO27_ADD_REQUIRED" if case!="CONTROL" and all(cond.values()) else "DO_NOT_PROMOTE"})
        emit("regime_matrix.csv",matrices); emit("regime_removed_and_added_trades.csv",differences)
        emit("all_candidate_trades.csv",alltrades);emit("periods.csv",periods)
        emit("rolling_summary.csv",rolling); emit("cost_stress.csv",cost)
        emit("decision_gates.csv",gates)
        emit("study_notes.csv",[
            {"topic":"scope","note":"Exactly three cases: frozen 99-trade outside-reversal control and separate H4/D1 regime gates. No parameter or RR search."},
            {"topic":"frozen_rules","note":REFERENCE_ID+"; signal candle midpoint close, short stop high+10 ticks, first possible exit next candle; no sessions; no weekday filters"},
            {"topic":"causality","note":"Only OANDA complete H4/D1 midpoint candles with completion time <= H1 signal CLOSE; daily aligned to 17:00 NY, DST aware."},
            {"topic":"EMA","note":"EMA50/EMA200 H4 and EMA200 D1, SMA seeded. First insufficient-history values are unavailable -> reject signal."},
            {"topic":"rerun","note":"Rebacktest all raw gated signals with pyramiding zero. Removing a core trade may allow a formerly blocked raw signal; those are logged as newly accepted."},
            {"topic":"cost","note":"2- and 4-pip adverse entry assumptions, not historical bid/ask/barrier reconstruction or real spread observations."},
            {"topic":"periods","note":"Period and rolling returns group completed simulated trades by H1 signal OPEN timestamp; entry-time attribution is not exit-time cashflow."},
            {"topic":"validation","note":"All periods are already repeatedly inspected historical data. These gates are descriptive; no out-of-sample guarantee."},
            {"topic":"portfolio","note":"This is a standalone filtering test only. Passing still requires exact 27->28 portfolio contribution, execution and forward checks. Never auto-deploy."},
            {"topic":"last_study","note":"If both filters fail, end EUR/AUD SHORT research instead of optimising extra thresholds."},
        ])
        package()
        STATUS.update(state="complete",message="Frozen EUR/AUD H1 SHORT final three-case study complete",
                      reference_parity_checks=len(checks),reference_mismatches=0,
                      cases=len(matrices),cases_passing=sum(x['gate_pass'] for x in gates),
                      bundle=BUNDLE,orders_sent=0)
    except Exception as err:
        STATUS.update(state="error",message=repr(err),orders_sent=0)
        try: package()
        except Exception: pass
        print("EURAUD SHORT FINAL STUDY ERROR:",repr(err),flush=True)

RUN_LOCK=threading.Lock()
STARTED=False

def launch():
    global STARTED
    with RUN_LOCK:
        if STARTED:return False
        STARTED=True
        threading.Thread(target=run_final_study,daemon=True,name="euraud-short-final").start()
        return True

@app.route("/")
def landing():
    return jsonify({"service":"EUR/AUD H1 SHORT FINAL outside-reversal regime study",
                    "status":STATUS['state'],"orders_supported":False,
                    "cases":CASES,"results":"/euraud-h1-short-final/results",
                    "status_route":"/euraud-h1-short-final/status",
                    "start":"/euraud-h1-short-final/start"})

@app.route("/euraud-h1-short-final/start")
def start_final():
    return jsonify({"started_now":launch(),"state":STATUS['state'],"orders_supported":False})

@app.route("/euraud-h1-short-final/status")
def status_final(): return jsonify(STATUS)

@app.route("/euraud-h1-short-final/results")
def results_final(): return download(BUNDLE)

if __name__=="__main__":
    launch()
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")),debug=False)
