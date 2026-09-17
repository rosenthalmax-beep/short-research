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
# AUD/USD H1 LONG — OUTSIDE REVERSAL REFINEMENT
# ============================================================
#
# PURPOSE
# -------
# Dedicated second-stage refinement after the broad AUD/USD H1 LONG
# discovery found a family-wide OUTSIDE_REVERSAL edge.
#
# This is deliberately NOT another all-family search.
#
# FROZEN DISCOVERY FINDINGS USED AS CONTROLS
# ------------------------------------------
# Control A:
#   OUTSIDE_REVERSAL
#   body >= 0.75 ATR14
#   lookback = 30
#   distance <= 0.30 ATR14
#   close location >= 0.65
#   RR = 3.00
#   through 2026-09-16 20:00 UTC:
#     323 trades
#     PF 1.3070928182
#     +68.4816984525R
#
# Control B:
#   same geometry, RR = 3.50
#   through 2026-09-16 20:00 UTC:
#     316 trades
#     PF 1.2727724665
#     +62.7376672916R
#
# Frozen alternative/control:
#   ENGULF_STRUCTURE
#   BR >= 1.00
#   body >= 1.25 ATR14
#   lookback = 100
#   distance <= 0.15 ATR14
#   RR = 4.00
#   through 2026-09-16 20:00 UTC:
#     47 trades
#     PF 1.6640192322
#     +21.9126346629R
#
# PARITY GUARD
# ------------
# The exact three controls above are re-run ONLY through the old discovery
# cutoff. The research aborts if their trade count/PF/R does not reproduce.
#
# REFINEMENT DESIGN
# -----------------
# Stage 1 — local geometry map, fixed RR3.25:
#   body ATR:      0.65 / 0.75 / 0.85 / 1.00
#   lookback:      20 / 25 / 30 / 35 / 40 / 45
#   distance ATR:  0.20 / 0.25 / 0.30 / 0.35 / 0.40
#   close loc:     0.60 / 0.65 / 0.70 / 0.75 / 0.80 / 0.85
#   total = 720 raw geometries
#
# Stage 2 — RR confirmation on a robustness-first geometry shortlist:
#   RR 2.75 / 3.00 / 3.25 / 3.50 / 3.75 / 4.00
#
# Stage 3 — simple SINGLE-FACTOR causal contexts only:
#   - no context
#   - one weekday exclusion at a time (America/New_York)
#   - Sydney / Tokyo / London / New York daytime blocks
#   - strictly completed H4 trend states
#   - strictly completed daily trend states
#
# No context interactions are mined in this runner.
#
# FINAL DIAGNOSTICS
# -----------------
#   - 2002-2017 vs 2018+
#   - pre-2010 / 2010+
#   - four broad eras
#   - last 5Y / last 2Y
#   - 0.5x / 1x / 1.5x / 2x adverse-cost stress
#   - rolling 12 / 24 / 36M
#   - calendar years
#   - local geometry/RR plateau
#   - context ablation vs the same no-context geometry/RR
#   - overlap with the frozen 47-trade engulf control
#
# HISTORICAL EXECUTION
# --------------------
# OANDA midpoint H1
# signal timestamp = H1 candle OPEN
# reference entry = signal CLOSE
# baseline adverse fill = +0.5 pip for LONG
# stop = signal low - 10 ticks
# target based on REFERENCE signal-close risk
# realised R based on adverse fill
# exits begin on NEXT H1 candle
# exact exit-candle re-entry eligible
# pyramiding = 0
#
# IMPORTANT
# ---------
# This is research only. It does NOT alter the frozen live 24-strategy
# portfolio and can never place an order.
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

CONTROL_CUTOFF = datetime(2026, 9, 16, 20, 0, tzinfo=timezone.utc)

TICK = 0.00001
PIP = 0.0001
STOP_BUFFER_TICKS = 10

H1_PRIMARY_COST_PIPS = 0.50
M15_PRIMARY_COST_PIPS = 1.00
COST_MULTIPLIERS = [0.5, 1.0, 1.5, 2.0]

GEOMETRY_RR = 3.25
RR_GRID = [2.75, 3.00, 3.25, 3.50, 3.75, 4.00]

GEOMETRY_BODY_ATR = [0.65, 0.75, 0.85, 1.00]
GEOMETRY_LOOKBACK = [20, 25, 30, 35, 40, 45]
GEOMETRY_DISTANCE = [0.20, 0.25, 0.30, 0.35, 0.40]
GEOMETRY_CLOSE = [0.60, 0.65, 0.70, 0.75, 0.80, 0.85]

GEOMETRY_SHORTLIST_SIZE = 36
CONTEXT_BASES = 12
FINAL_KEEP = 12

NY = ZoneInfo("America/New_York")
LONDON = ZoneInfo("Europe/London")
TOKYO = ZoneInfo("Asia/Tokyo")
SYDNEY = ZoneInfo("Australia/Sydney")

OUTS = {
    "coverage": "audusd_h1_long_outside_refinement_coverage.csv",
    "control_parity": "audusd_h1_long_outside_refinement_control_parity.csv",
    "geometry": "audusd_h1_long_outside_refinement_geometry.csv",
    "geometry_shortlist": "audusd_h1_long_outside_refinement_geometry_shortlist.csv",
    "rr": "audusd_h1_long_outside_refinement_rr_sweep.csv",
    "contexts": "audusd_h1_long_outside_refinement_context_scan.csv",
    "context_ablation": "audusd_h1_long_outside_refinement_context_ablation.csv",
    "finalists": "audusd_h1_long_outside_refinement_finalists.csv",
    "periods": "audusd_h1_long_outside_refinement_periods.csv",
    "cost_stress": "audusd_h1_long_outside_refinement_cost_stress.csv",
    "rolling": "audusd_h1_long_outside_refinement_rolling.csv",
    "rolling_summary": "audusd_h1_long_outside_refinement_rolling_summary.csv",
    "calendar": "audusd_h1_long_outside_refinement_calendar_years.csv",
    "calendar_summary": "audusd_h1_long_outside_refinement_calendar_summary.csv",
    "plateau": "audusd_h1_long_outside_refinement_plateau.csv",
    "overlap": "audusd_h1_long_outside_refinement_overlap_vs_engulf.csv",
    "trades": "audusd_h1_long_outside_refinement_finalist_trades.csv",
    "notes": "audusd_h1_long_outside_refinement_notes.csv",
}

BUNDLE = "AUDUSD_H1_LONG_OUTSIDE_REVERSAL_REFINEMENT_RESULTS.zip"

STATUS = {
    "state": "not_started",
    "message": "Not started",
    "pair": PAIR,
    "timeframe": "H1",
    "side": "LONG",
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
# OUTSIDE-REVERSAL REFINEMENT HELPERS
# ============================================================

def ema_array(values, length):
    values = np.asarray(values, dtype=float)

    result = np.full(
        len(values),
        np.nan,
        dtype=float,
    )

    if len(values) < length:
        return result

    seed_window = values[:length]
    if not np.all(np.isfinite(seed_window)):
        return result

    result[length - 1] = float(
        np.mean(seed_window)
    )

    alpha = 2.0 / (length + 1.0)

    for i in range(length, len(values)):
        value = values[i]

        if not math.isfinite(value):
            result[i] = result[i - 1]
            continue

        result[i] = (
            alpha * value
            + (1.0 - alpha) * result[i - 1]
        )

    return result


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


def aligned_completed_values(
    signal_times,
    htf_times,
    values,
):
    """
    Strictly completed higher-timeframe state.

    For HTF candle i, its value becomes available only when the NEXT HTF
    candle begins. This handles daily DST alignment safely because completion
    is inferred from the actual next OANDA candle timestamp.
    """
    output = np.full(
        len(signal_times),
        np.nan,
        dtype=float,
    )

    if len(htf_times) < 2:
        return output

    completion_times = htf_times[1:]
    completed_values = np.asarray(
        values[:-1],
        dtype=float,
    )

    for i, signal_time in enumerate(signal_times):
        index = (
            bisect_right(
                completion_times,
                signal_time,
            )
            - 1
        )

        if index >= 0:
            output[i] = completed_values[index]

    return output


def build_context_cache(
    h1_features,
    h4_candles,
    daily_candles,
):
    signal_times = h1_features["times"]

    h4_close = np.array(
        [c["close"] for c in h4_candles],
        dtype=float,
    )
    daily_close = np.array(
        [c["close"] for c in daily_candles],
        dtype=float,
    )

    h4_times = [
        c["time"]
        for c in h4_candles
    ]
    daily_times = [
        c["time"]
        for c in daily_candles
    ]

    h4_ema50 = ema_array(h4_close, 50)
    h4_ema100 = ema_array(h4_close, 100)
    h4_ema200 = ema_array(h4_close, 200)

    d_ema50 = ema_array(daily_close, 50)
    d_ema100 = ema_array(daily_close, 100)
    d_ema200 = ema_array(daily_close, 200)

    cache = {
        "H4_CLOSE": aligned_completed_values(
            signal_times,
            h4_times,
            h4_close,
        ),
        "H4_EMA50": aligned_completed_values(
            signal_times,
            h4_times,
            h4_ema50,
        ),
        "H4_EMA100": aligned_completed_values(
            signal_times,
            h4_times,
            h4_ema100,
        ),
        "H4_EMA200": aligned_completed_values(
            signal_times,
            h4_times,
            h4_ema200,
        ),
        "D_CLOSE": aligned_completed_values(
            signal_times,
            daily_times,
            daily_close,
        ),
        "D_EMA50": aligned_completed_values(
            signal_times,
            daily_times,
            d_ema50,
        ),
        "D_EMA100": aligned_completed_values(
            signal_times,
            daily_times,
            d_ema100,
        ),
        "D_EMA200": aligned_completed_values(
            signal_times,
            daily_times,
            d_ema200,
        ),
    }

    ny_weekday = np.empty(
        len(signal_times),
        dtype=int,
    )

    session_ny = np.zeros(
        len(signal_times),
        dtype=bool,
    )
    session_london = np.zeros(
        len(signal_times),
        dtype=bool,
    )
    session_tokyo = np.zeros(
        len(signal_times),
        dtype=bool,
    )
    session_sydney = np.zeros(
        len(signal_times),
        dtype=bool,
    )

    for i, timestamp in enumerate(signal_times):
        ny_time = timestamp.astimezone(NY)
        london_time = timestamp.astimezone(LONDON)
        tokyo_time = timestamp.astimezone(TOKYO)
        sydney_time = timestamp.astimezone(SYDNEY)

        ny_weekday[i] = ny_time.weekday()

        # signal timestamp is candle OPEN
        session_ny[i] = 8 <= ny_time.hour < 17
        session_london[i] = 7 <= london_time.hour < 16
        session_tokyo[i] = 8 <= tokyo_time.hour < 17
        session_sydney[i] = 8 <= sydney_time.hour < 17

    cache["NY_WEEKDAY"] = ny_weekday
    cache["SESSION_NY"] = session_ny
    cache["SESSION_LONDON"] = session_london
    cache["SESSION_TOKYO"] = session_tokyo
    cache["SESSION_SYDNEY"] = session_sydney

    return cache


def context_definitions():
    return [
        ("NONE", "BASE", "No context filter"),

        ("EXCLUDE_MON_NY", "WEEKDAY", "Exclude Monday NY"),
        ("EXCLUDE_TUE_NY", "WEEKDAY", "Exclude Tuesday NY"),
        ("EXCLUDE_WED_NY", "WEEKDAY", "Exclude Wednesday NY"),
        ("EXCLUDE_THU_NY", "WEEKDAY", "Exclude Thursday NY"),
        ("EXCLUDE_FRI_NY", "WEEKDAY", "Exclude Friday NY"),

        ("SESSION_SYDNEY_08_17", "SESSION", "08:00-16:59 Australia/Sydney"),
        ("SESSION_TOKYO_08_17", "SESSION", "08:00-16:59 Asia/Tokyo"),
        ("SESSION_LONDON_07_16", "SESSION", "07:00-15:59 Europe/London"),
        ("SESSION_NY_08_17", "SESSION", "08:00-16:59 America/New_York"),

        ("H4_CLOSE_GT_EMA100", "H4_TREND", "Prior completed H4 close > EMA100"),
        ("H4_CLOSE_GT_EMA200", "H4_TREND", "Prior completed H4 close > EMA200"),
        ("H4_EMA50_GT_EMA200", "H4_TREND", "Prior completed H4 EMA50 > EMA200"),

        ("D_CLOSE_GT_EMA100", "D_TREND", "Prior completed D close > EMA100"),
        ("D_CLOSE_GT_EMA200", "D_TREND", "Prior completed D close > EMA200"),
        ("D_EMA50_GT_EMA200", "D_TREND", "Prior completed D EMA50 > EMA200"),
    ]


def context_mask(context_id, cache):
    n = len(cache["NY_WEEKDAY"])

    if context_id == "NONE":
        return np.ones(n, dtype=bool)

    weekday_map = {
        "EXCLUDE_MON_NY": 0,
        "EXCLUDE_TUE_NY": 1,
        "EXCLUDE_WED_NY": 2,
        "EXCLUDE_THU_NY": 3,
        "EXCLUDE_FRI_NY": 4,
    }

    if context_id in weekday_map:
        return (
            cache["NY_WEEKDAY"]
            != weekday_map[context_id]
        )

    if context_id == "SESSION_SYDNEY_08_17":
        return cache["SESSION_SYDNEY"].copy()

    if context_id == "SESSION_TOKYO_08_17":
        return cache["SESSION_TOKYO"].copy()

    if context_id == "SESSION_LONDON_07_16":
        return cache["SESSION_LONDON"].copy()

    if context_id == "SESSION_NY_08_17":
        return cache["SESSION_NY"].copy()

    if context_id == "H4_CLOSE_GT_EMA100":
        return (
            np.isfinite(cache["H4_CLOSE"])
            & np.isfinite(cache["H4_EMA100"])
            & (
                cache["H4_CLOSE"]
                > cache["H4_EMA100"]
            )
        )

    if context_id == "H4_CLOSE_GT_EMA200":
        return (
            np.isfinite(cache["H4_CLOSE"])
            & np.isfinite(cache["H4_EMA200"])
            & (
                cache["H4_CLOSE"]
                > cache["H4_EMA200"]
            )
        )

    if context_id == "H4_EMA50_GT_EMA200":
        return (
            np.isfinite(cache["H4_EMA50"])
            & np.isfinite(cache["H4_EMA200"])
            & (
                cache["H4_EMA50"]
                > cache["H4_EMA200"]
            )
        )

    if context_id == "D_CLOSE_GT_EMA100":
        return (
            np.isfinite(cache["D_CLOSE"])
            & np.isfinite(cache["D_EMA100"])
            & (
                cache["D_CLOSE"]
                > cache["D_EMA100"]
            )
        )

    if context_id == "D_CLOSE_GT_EMA200":
        return (
            np.isfinite(cache["D_CLOSE"])
            & np.isfinite(cache["D_EMA200"])
            & (
                cache["D_CLOSE"]
                > cache["D_EMA200"]
            )
        )

    if context_id == "D_EMA50_GT_EMA200":
        return (
            np.isfinite(cache["D_EMA50"])
            & np.isfinite(cache["D_EMA200"])
            & (
                cache["D_EMA50"]
                > cache["D_EMA200"]
            )
        )

    raise ValueError(
        f"Unknown context_id: {context_id}"
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

    mask = context_mask(
        context_id,
        context_cache,
    )

    if not len(raw_indices):
        return raw_indices

    return raw_indices[
        mask[raw_indices]
    ]


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


def control_specs():
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


def run_control_parity(control_features):
    rows = []

    for spec in control_specs():
        config = spec["config"]
        indices = signal_indices(
            config,
            control_features,
        )
        trades = backtest(
            config,
            control_features,
            indices,
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
            "control_cutoff_utc": iso(CONTROL_CUTOFF),
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
                "Control parity failed for "
                f"{spec['name']}: "
                f"trades {result['trades']} vs {spec['expected_trades']}, "
                f"PF {result['profit_factor']} vs {spec['expected_pf']}, "
                f"R {result['total_r']} vs {spec['expected_total_r']}"
            )

    return rows


def geometry_configs():
    configs = []

    for body in GEOMETRY_BODY_ATR:
        for lookback in GEOMETRY_LOOKBACK:
            for distance in GEOMETRY_DISTANCE:
                for close_location in GEOMETRY_CLOSE:
                    configs.append(
                        outside_config(
                            body,
                            lookback,
                            distance,
                            close_location,
                            GEOMETRY_RR,
                        )
                    )

    return configs


def refinement_sort_key(row):
    return (
        1 if row["both_temporal_splits_positive"] else 0,
        row["positive_eras"],
        row["min_temporal_split_pf"],
        1 if row["last5y_r"] > 0 else 0,
        1 if row["last2y_r"] > 0 else 0,
        row["full_pf"],
        row["full_total_r"],
        row["full_trades"],
    )


def select_geometry_shortlist(rows):
    eligible = [
        row
        for row in rows
        if (
            row["full_trades"] >= 100
            and row["both_temporal_splits_positive"]
            and row["positive_eras"] >= 3
        )
    ]

    pool = eligible if eligible else list(rows)

    pool = sorted(
        pool,
        key=refinement_sort_key,
        reverse=True,
    )

    selected = []
    selected_ids = set()

    # First make sure each tested lookback gets representation if it has a
    # credible candidate. This prevents the shortlist collapsing into one
    # narrow lookback by ranking alone.
    by_lookback = defaultdict(list)

    for row in pool:
        by_lookback[int(row["lookback"])].append(row)

    for lookback in GEOMETRY_LOOKBACK:
        group = sorted(
            by_lookback.get(lookback, []),
            key=refinement_sort_key,
            reverse=True,
        )

        for row in group[:3]:
            if row["config_id"] in selected_ids:
                continue

            selected.append(row)
            selected_ids.add(row["config_id"])

    for row in pool:
        if len(selected) >= GEOMETRY_SHORTLIST_SIZE:
            break

        if row["config_id"] in selected_ids:
            continue

        selected.append(row)
        selected_ids.add(row["config_id"])

    selected = selected[:GEOMETRY_SHORTLIST_SIZE]
    selected.sort(
        key=refinement_sort_key,
        reverse=True,
    )

    return selected


def row_to_outside_config(row, rr=None, context_id=None):
    return outside_config(
        row["body_atr_min"],
        int(row["lookback"]),
        row["distance_atr_max"],
        row["close_location"],
        row["rr"] if rr is None else rr,
        context_id=context_id,
    )


def select_context_bases(rr_rows):
    # One best RR per exact geometry first.
    grouped = defaultdict(list)

    for row in rr_rows:
        geometry_key = (
            row["body_atr_min"],
            int(row["lookback"]),
            row["distance_atr_max"],
            row["close_location"],
        )
        grouped[geometry_key].append(row)

    best_per_geometry = []

    for group in grouped.values():
        group.sort(
            key=refinement_sort_key,
            reverse=True,
        )
        best_per_geometry.append(group[0])

    best_per_geometry.sort(
        key=refinement_sort_key,
        reverse=True,
    )

    selected = best_per_geometry[:CONTEXT_BASES]

    # Always preserve the two exact discovery controls as context bases so
    # single-factor filters are also tested on the known central geometry.
    required = [
        outside_config(
            0.75, 30, 0.30, 0.65, 3.00
        ),
        outside_config(
            0.75, 30, 0.30, 0.65, 3.50
        ),
    ]

    rr_map = {
        row["config_id"]: row
        for row in rr_rows
    }

    selected_ids = {
        row["config_id"]
        for row in selected
    }

    for config in required:
        if (
            config["config_id"] in rr_map
            and config["config_id"] not in selected_ids
        ):
            selected.append(
                rr_map[config["config_id"]]
            )
            selected_ids.add(
                config["config_id"]
            )

    selected.sort(
        key=refinement_sort_key,
        reverse=True,
    )

    return selected[: max(CONTEXT_BASES, len(required))]


def context_ablation_rows(context_rows, rr_rows):
    base_map = {
        row["config_id"]: row
        for row in rr_rows
    }

    output = []

    for row in context_rows:
        context_id = row.get("context_id", "NONE")

        if context_id == "NONE":
            continue

        base_config = outside_config(
            row["body_atr_min"],
            int(row["lookback"]),
            row["distance_atr_max"],
            row["close_location"],
            row["rr"],
        )

        base = base_map.get(
            base_config["config_id"]
        )

        if not base:
            continue

        output.append({
            "config_id": row["config_id"],
            "context_id": context_id,
            "base_config_id": base_config["config_id"],
            "context_trades": row["full_trades"],
            "base_trades": base["full_trades"],
            "trade_delta": (
                row["full_trades"]
                - base["full_trades"]
            ),
            "context_pf": row["full_pf"],
            "base_pf": base["full_pf"],
            "pf_delta": (
                row["full_pf"]
                - base["full_pf"]
            ),
            "context_total_r": row["full_total_r"],
            "base_total_r": base["full_total_r"],
            "total_r_delta": (
                row["full_total_r"]
                - base["full_total_r"]
            ),
            "context_min_split_pf": row[
                "min_temporal_split_pf"
            ],
            "base_min_split_pf": base[
                "min_temporal_split_pf"
            ],
            "min_split_pf_delta": (
                row["min_temporal_split_pf"]
                - base["min_temporal_split_pf"]
            ),
            "context_last5y_r": row["last5y_r"],
            "base_last5y_r": base["last5y_r"],
            "last5y_r_delta": (
                row["last5y_r"]
                - base["last5y_r"]
            ),
        })

    output.sort(
        key=lambda row: (
            row["min_split_pf_delta"],
            row["pf_delta"],
            row["total_r_delta"],
        ),
        reverse=True,
    )

    return output


def select_finalist_rows(context_rows, rr_rows):
    eligible = [
        row
        for row in context_rows
        if (
            row["full_trades"] >= 80
            and row["both_temporal_splits_positive"]
            and row["positive_eras"] >= 3
            and row["last5y_r"] > 0
        )
    ]

    pool = eligible if eligible else list(context_rows)

    pool.sort(
        key=refinement_sort_key,
        reverse=True,
    )

    selected = []
    selected_ids = set()
    per_base_count = defaultdict(int)

    for row in pool:
        base_key = (
            row["body_atr_min"],
            int(row["lookback"]),
            row["distance_atr_max"],
            row["close_location"],
            row["rr"],
        )

        # Avoid a final table dominated by many filters on one exact geometry.
        if per_base_count[base_key] >= 2:
            continue

        selected.append(row)
        selected_ids.add(row["config_id"])
        per_base_count[base_key] += 1

        if len(selected) >= 8:
            break

    # Force useful no-context references into deep diagnostics.
    rr_sorted = sorted(
        rr_rows,
        key=refinement_sort_key,
        reverse=True,
    )

    required_configs = [
        outside_config(
            0.75, 30, 0.30, 0.65, 3.00,
            context_id="NONE",
        ),
        outside_config(
            0.75, 30, 0.30, 0.65, 3.50,
            context_id="NONE",
        ),
    ]

    # Add the strongest raw/no-context candidate as well.
    if rr_sorted:
        strongest = row_to_outside_config(
            rr_sorted[0],
            context_id="NONE",
        )
        required_configs.append(strongest)

    context_map = {
        row["config_id"]: row
        for row in context_rows
    }

    for config in required_configs:
        row = context_map.get(
            config["config_id"]
        )

        if (
            row is not None
            and row["config_id"] not in selected_ids
        ):
            selected.append(row)
            selected_ids.add(row["config_id"])

    selected.sort(
        key=refinement_sort_key,
        reverse=True,
    )

    return selected[:FINAL_KEEP]


def add_refinement_screen(
    rows,
    cost_rows,
    rolling_summary_rows,
    calendar_summary_rows,
    plateau_summary_rows,
    ablation_rows,
):
    cost_map = {
        row["config_id"]: row
        for row in cost_rows
        if abs(row["cost_multiplier"] - 2.0) < 1e-9
    }

    rolling_map = defaultdict(dict)
    for row in rolling_summary_rows:
        rolling_map[
            row["config_id"]
        ][row["window_months"]] = row

    calendar_map = {
        row["config_id"]: row
        for row in calendar_summary_rows
    }

    plateau_map = {
        row["config_id"]: row
        for row in plateau_summary_rows
    }

    ablation_map = {
        row["config_id"]: row
        for row in ablation_rows
    }

    output = []

    for source in rows:
        row = dict(source)

        cost2 = cost_map.get(
            row["config_id"],
            {},
        )
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
        ablation = ablation_map.get(
            row["config_id"],
            {},
        )

        row.update({
            "cost_2x_pf": cost2.get(
                "profit_factor",
                0.0,
            ),
            "cost_2x_total_r": cost2.get(
                "total_r",
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
            "context_pf_delta_vs_none": ablation.get(
                "pf_delta",
                0.0,
            ),
            "context_min_split_pf_delta_vs_none": ablation.get(
                "min_split_pf_delta",
                0.0,
            ),
        })

        row["refinement_screen_pass"] = (
            row["full_trades"] >= 80
            and row["full_pf"] >= 1.25
            and row["both_temporal_splits_positive"]
            and row["min_temporal_split_pf"] >= 1.15
            and row["positive_eras"] >= 3
            and row["last5y_r"] > 0
            and row["last2y_r"] > 0
            and row["cost_2x_pf"] >= 1.15
            and row["cost_2x_total_r"] > 0
            and row["rolling24_positive_pct"] >= 70.0
            and row["rolling36_positive_pct"] >= 75.0
            and row["positive_calendar_year_pct"] >= 65.0
            and row["plateau_positive_neighbours_pct"] >= 80.0
        )

        output.append(row)

    output.sort(
        key=lambda row: (
            1 if row["refinement_screen_pass"] else 0,
            row["min_temporal_split_pf"],
            row["cost_2x_pf"],
            row["full_pf"],
            row["full_total_r"],
        ),
        reverse=True,
    )

    return output


def overlap_with_engulf_rows(
    finalist_trade_map,
    engulf_trades,
):
    output = []

    engulf_signals = {
        trade["signal_index"]
        for trade in engulf_trades
    }

    engulf_intervals = [
        (
            trade["signal_index"],
            trade["exit_index"],
        )
        for trade in engulf_trades
    ]

    for config_id, trades in finalist_trade_map.items():
        exact = 0
        holding_overlap = 0

        for trade in trades:
            if trade["signal_index"] in engulf_signals:
                exact += 1

            left = trade["signal_index"]
            right = trade["exit_index"]

            overlapped = any(
                (
                    left < other_right
                    and other_left < right
                )
                for other_left, other_right
                in engulf_intervals
            )

            if overlapped:
                holding_overlap += 1

        n = len(trades)

        output.append({
            "config_id": config_id,
            "candidate_trades": n,
            "engulf_control_trades": len(
                engulf_trades
            ),
            "exact_signal_overlap_count": exact,
            "exact_signal_overlap_pct_candidate": (
                100.0 * exact / n
                if n else 0.0
            ),
            "holding_period_overlap_count": holding_overlap,
            "holding_period_overlap_pct_candidate": (
                100.0 * holding_overlap / n
                if n else 0.0
            ),
            "candidate_exact_signal_nonoverlap_count": (
                n - exact
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
            WARMUP_START,
            NOW,
            chunk_days=720,
        )

        daily = fetch_history(
            "D",
            WARMUP_START,
            NOW,
            chunk_days=3000,
        )

        if len(h1) < 5000:
            raise RuntimeError(
                f"Unexpectedly small H1 history: {len(h1)}"
            )

        if len(h4) < 1000:
            raise RuntimeError(
                f"Unexpectedly small H4 history: {len(h4)}"
            )

        if len(daily) < 1000:
            raise RuntimeError(
                f"Unexpectedly small daily history: {len(daily)}"
            )

        coverage = [
            {
                "pair": PAIR,
                "timeframe": "H1",
                "candles": len(h1),
                "first_utc": iso(h1[0]["time"]),
                "last_utc": iso(h1[-1]["time"]),
            },
            {
                "pair": PAIR,
                "timeframe": "H4",
                "candles": len(h4),
                "first_utc": iso(h4[0]["time"]),
                "last_utc": iso(h4[-1]["time"]),
            },
            {
                "pair": PAIR,
                "timeframe": "D",
                "candles": len(daily),
                "first_utc": iso(daily[0]["time"]),
                "last_utc": iso(daily[-1]["time"]),
            },
        ]

        write_csv(
            OUTS["coverage"],
            coverage,
        )

        STATUS.update({
            "state": "features",
            "message": "Building features and strict completed HTF states",
        })

        features = build_features(
            h1,
            "H1",
        )

        context_cache = build_context_cache(
            features,
            h4,
            daily,
        )

        # ----------------------------------------------------
        # CONTROL PARITY
        # ----------------------------------------------------
        cutoff_index = bisect_right(
            features["times"],
            CONTROL_CUTOFF,
        )

        control_features = slice_features(
            features,
            cutoff_index,
        )

        parity_rows = run_control_parity(
            control_features
        )

        write_csv(
            OUTS["control_parity"],
            parity_rows,
        )

        # Frozen engulf control on CURRENT full history for overlap only.
        engulf_config = engulf_control_config()
        engulf_indices = signal_indices(
            engulf_config,
            features,
        )
        engulf_trades = backtest(
            engulf_config,
            features,
            engulf_indices,
        )

        # ----------------------------------------------------
        # STAGE 1 — OUTSIDE GEOMETRY
        # ----------------------------------------------------
        configs = geometry_configs()

        geometry_rows = []
        raw_index_cache = {}

        for number, config in enumerate(
            configs,
            1,
        ):
            STATUS.update({
                "state": "geometry",
                "message": (
                    f"{number}/{len(configs)} "
                    f"{config['config_id']}"
                ),
            })

            raw_indices = signal_indices(
                config,
                features,
            )

            raw_index_cache[
                config["config_id"]
            ] = raw_indices

            row, _ = evaluate_candidate(
                config,
                features,
                raw_indices,
            )

            geometry_rows.append(row)

        geometry_rows.sort(
            key=refinement_sort_key,
            reverse=True,
        )

        write_csv(
            OUTS["geometry"],
            geometry_rows,
        )

        geometry_shortlist = (
            select_geometry_shortlist(
                geometry_rows
            )
        )

        write_csv(
            OUTS["geometry_shortlist"],
            geometry_shortlist,
        )

        # ----------------------------------------------------
        # STAGE 2 — RR SWEEP
        # ----------------------------------------------------
        rr_rows = []
        rr_config_map = {}
        rr_signal_cache = {}

        total_rr = (
            len(geometry_shortlist)
            * len(RR_GRID)
        )
        rr_done = 0

        for base_row in geometry_shortlist:
            geometry_config = row_to_outside_config(
                base_row,
                rr=GEOMETRY_RR,
            )

            geometry_indices = raw_index_cache.get(
                geometry_config["config_id"]
            )

            if geometry_indices is None:
                geometry_indices = signal_indices(
                    geometry_config,
                    features,
                )

            for rr in RR_GRID:
                rr_done += 1

                config = row_to_outside_config(
                    base_row,
                    rr=rr,
                )

                STATUS.update({
                    "state": "rr_sweep",
                    "message": (
                        f"{rr_done}/{total_rr} "
                        f"{config['config_id']}"
                    ),
                })

                row, _ = evaluate_candidate(
                    config,
                    features,
                    geometry_indices,
                )

                rr_rows.append(row)
                rr_config_map[
                    config["config_id"]
                ] = config
                rr_signal_cache[
                    config["config_id"]
                ] = geometry_indices

        # Guarantee exact central discovery controls are available in RR table
        # even if that geometry did not make the top refinement shortlist.
        for config in [
            outside_config(
                0.75, 30, 0.30, 0.65, 3.00
            ),
            outside_config(
                0.75, 30, 0.30, 0.65, 3.50
            ),
        ]:
            if config["config_id"] in rr_config_map:
                continue

            raw_indices = signal_indices(
                config,
                features,
            )

            row, _ = evaluate_candidate(
                config,
                features,
                raw_indices,
            )

            rr_rows.append(row)
            rr_config_map[
                config["config_id"]
            ] = config
            rr_signal_cache[
                config["config_id"]
            ] = raw_indices

        rr_rows.sort(
            key=refinement_sort_key,
            reverse=True,
        )

        write_csv(
            OUTS["rr"],
            rr_rows,
        )

        # ----------------------------------------------------
        # STAGE 3 — SINGLE-FACTOR CONTEXTS
        # ----------------------------------------------------
        context_bases = select_context_bases(
            rr_rows
        )

        contexts = context_definitions()
        context_rows = []
        context_config_map = {}
        context_signal_cache = {}

        total_contexts = (
            len(context_bases)
            * len(contexts)
        )
        context_done = 0

        for base_row in context_bases:
            base_config = row_to_outside_config(
                base_row
            )

            base_indices = rr_signal_cache.get(
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
                context_description,
            ) in contexts:
                context_done += 1

                config = row_to_outside_config(
                    base_row,
                    context_id=context_id,
                )

                STATUS.update({
                    "state": "context_scan",
                    "message": (
                        f"{context_done}/{total_contexts} "
                        f"{config['config_id']}"
                    ),
                })

                filtered_indices = apply_context(
                    base_indices,
                    context_id,
                    context_cache,
                )

                row, _ = evaluate_candidate(
                    config,
                    features,
                    filtered_indices,
                )

                row["context_group"] = context_group
                row["context_description"] = (
                    context_description
                )
                row["raw_geometry_signals"] = len(
                    base_indices
                )
                row["context_signals"] = len(
                    filtered_indices
                )
                row["context_signal_retention_pct"] = (
                    100.0
                    * len(filtered_indices)
                    / len(base_indices)
                    if len(base_indices)
                    else 0.0
                )

                context_rows.append(row)
                context_config_map[
                    config["config_id"]
                ] = config
                context_signal_cache[
                    config["config_id"]
                ] = filtered_indices

        context_rows.sort(
            key=refinement_sort_key,
            reverse=True,
        )

        write_csv(
            OUTS["contexts"],
            context_rows,
        )

        ablation_rows = context_ablation_rows(
            context_rows,
            rr_rows,
        )

        write_csv(
            OUTS["context_ablation"],
            ablation_rows,
        )

        # ----------------------------------------------------
        # DEEP FINALISTS
        # ----------------------------------------------------
        finalist_seed_rows = select_finalist_rows(
            context_rows,
            rr_rows,
        )

        periods = []
        costs = []
        rolling = []
        calendar = []
        all_trades = []
        finalist_trade_map = {}
        finalist_configs = []

        for number, seed in enumerate(
            finalist_seed_rows,
            1,
        ):
            config = context_config_map[
                seed["config_id"]
            ]
            raw_indices = context_signal_cache[
                seed["config_id"]
            ]

            finalist_configs.append(config)

            STATUS.update({
                "state": "deep_validation",
                "message": (
                    f"{number}/{len(finalist_seed_rows)} "
                    f"{config['config_id']}"
                ),
            })

            trades = backtest(
                config,
                features,
                raw_indices,
            )

            finalist_trade_map[
                config["config_id"]
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
                output["context_id"] = config.get(
                    "context_id",
                    "NONE",
                )
                all_trades.append(output)

        rolling_summaries = rolling_summary(
            rolling
        )
        calendar_summaries = calendar_summary(
            calendar
        )

        plateau_summaries = plateau_rows(
            finalist_seed_rows,
            rr_rows,
        )

        final_rows = add_refinement_screen(
            finalist_seed_rows,
            costs,
            rolling_summaries,
            calendar_summaries,
            plateau_summaries,
            ablation_rows,
        )

        overlap_rows = overlap_with_engulf_rows(
            finalist_trade_map,
            engulf_trades,
        )

        write_csv(
            OUTS["finalists"],
            final_rows,
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
        write_csv(
            OUTS["plateau"],
            plateau_summaries,
        )
        write_csv(
            OUTS["overlap"],
            overlap_rows,
        )
        write_csv(
            OUTS["trades"],
            all_trades,
        )

        screen_passes = [
            row
            for row in final_rows
            if row["refinement_screen_pass"]
        ]

        notes = [
            {
                "topic": "research_scope",
                "note": (
                    "Second-stage AUD/USD H1 LONG refinement. "
                    "Only OUTSIDE_REVERSAL is optimised. "
                    "The 47-trade ENGULF_STRUCTURE setup is frozen as a "
                    "comparison/overlap control."
                ),
            },
            {
                "topic": "parity",
                "note": (
                    "Three exact discovery controls are reproduced through "
                    "2026-09-16 20:00 UTC before any new optimisation runs."
                ),
            },
            {
                "topic": "geometry",
                "note": (
                    "720 local OUTSIDE_REVERSAL geometries are tested at "
                    "fixed RR3.25. The grid deliberately surrounds the broad "
                    "0.30 ATR structure-distance area rather than expanding "
                    "into unrelated parameter space."
                ),
            },
            {
                "topic": "contexts",
                "note": (
                    "Only single-factor weekday/session/H4/daily contexts are "
                    "tested. No context interactions are mined in this run."
                ),
            },
            {
                "topic": "htf_no_lookahead",
                "note": (
                    "H4 and daily states use only a candle whose NEXT HTF "
                    "candle has already begun by the H1 signal-open time."
                ),
            },
            {
                "topic": "costs",
                "note": (
                    "Baseline H1 cost remains 0.5 pip adverse; final candidates "
                    "are stressed at 0.25/0.5/0.75/1.0 pip via 0.5x/1x/1.5x/2x."
                ),
            },
            {
                "topic": "portfolio",
                "note": (
                    "This runner does not alter or simulate the current live "
                    "24-strategy portfolio. Any surviving AUD/USD candidate "
                    "must next pass a separate exact portfolio-addition test "
                    "before it can become strategy #25."
                ),
            },
            {
                "topic": "screen_result",
                "note": (
                    f"{len(screen_passes)} of {len(final_rows)} deep finalists "
                    "passed the predeclared refinement screen. A pass is not "
                    "an automatic live lock."
                ),
            },
        ]

        write_csv(
            OUTS["notes"],
            notes,
        )

        STATUS.update({
            "state": "packaging",
            "message": "Building refinement ZIP",
        })

        pack()

        STATUS.update({
            "state": "complete",
            "message": (
                "AUD/USD H1 LONG outside-reversal refinement complete"
            ),
            "h1_candles": len(h1),
            "h4_candles": len(h4),
            "daily_candles": len(daily),
            "control_parity_passes": len(parity_rows),
            "geometry_configs": len(geometry_rows),
            "geometry_shortlist": len(geometry_shortlist),
            "rr_rows": len(rr_rows),
            "context_bases": len(context_bases),
            "context_rows": len(context_rows),
            "deep_finalists": len(final_rows),
            "refinement_screen_passes": len(screen_passes),
            "engulf_control_current_trades": len(engulf_trades),
            "bundle": BUNDLE,
        })

    except Exception as error:
        STATUS.update({
            "state": "error",
            "message": str(error),
        })

        print(
            "AUDUSD H1 LONG OUTSIDE REFINEMENT ERROR:",
            repr(error),
            flush=True,
        )


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def root():
    return jsonify({
        "service": "AUD/USD H1 LONG Outside-Reversal Refinement",
        "status": STATUS["state"],
        "instrument": PAIR,
        "timeframe": "H1",
        "side": "LONG",
        "family": "OUTSIDE_REVERSAL",
        "frozen_alternative_control": "ENGULF_STRUCTURE",
        "geometry_rr": GEOMETRY_RR,
        "rr_grid": RR_GRID,
        "geometry_configs_expected": (
            len(GEOMETRY_BODY_ATR)
            * len(GEOMETRY_LOOKBACK)
            * len(GEOMETRY_DISTANCE)
            * len(GEOMETRY_CLOSE)
        ),
        "context_count": len(
            context_definitions()
        ),
        "control_cutoff_utc": iso(
            CONTROL_CUTOFF
        ),
        "orders_supported": False,
        "trading_enabled": False,
        "routes": [
            "/audusd-h1-long-outside-refinement/status",
            "/audusd-h1-long-outside-refinement/results",
        ],
    })


@app.route("/audusd-h1-long-outside-refinement/status")
def status():
    return jsonify(STATUS)


@app.route("/audusd-h1-long-outside-refinement/results")
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
