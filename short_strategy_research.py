#!/usr/bin/env python3
"""EUR/AUD H1 LONG, predeclared regime + independent complement study.
READ ONLY. Separate Railway research service; never import/modify live executor.
No parameter combinations: frozen LB25 geometry, fixed RR3.5/RR4.0.
Any 'validation' period has already been seen during discovery, NOT OOS.
Historic 2-pip adverse fill assumed, 4-pip stress assumed, not actual spread.
Midpoint candle simulation not proof of live execution. No live orders.
"""
import base64, zlib, json
from zoneinfo import ZoneInfo
"""EUR/AUD H1 LONG sweep/displacement — plateau and finalist confirmation.

READ ONLY research. This script does not import any live executor and never sends orders.
Use OANDA_TOKEN on a SEPARATE Railway research service. Fetches EUR/AUD H1 midpoint history.

Predeclared control: LONG; low < previous 30-bar low, bullish close > previous
H1 high, bullish body >= 1.00 ATR14 (Wilder/RMA), lower wick / body >= 0.25.
Reference entry = completed signal close; stop = signal low - 10 ticks;
2-pip adverse historical fill (4-pip doubled-cost check); target anchored to
REFERENCE-entry risk; per-config pyramiding 0 and exit-candle signal eligible.

Predeclared single-factor plateau grids with bounded, rule-triggered expansion.
Separate RR sweeps for unchanged geometries. No combined-factor optimisation,
no live trades or automatic strategy promotion.
Full dataset has already been inspected in discovery; all temporal splits
are robustness diagnostics, NOT untouched out-of-sample validation.
"""
import os
import csv
import math
import time
import zipfile
import threading
import hashlib
from bisect import bisect_left, bisect_right
from collections import deque, defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median
from pathlib import Path

import numpy as np
import requests
from flask import Flask, jsonify, send_file

app = Flask(__name__)
TOKEN = os.getenv("OANDA_TOKEN")
BASE = os.getenv("OANDA_API_URL", "https://api-fxtrade.oanda.com")
PAIR = "EUR_AUD"
REQUESTED_START = datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc)
NOW = datetime.now(timezone.utc).replace(second=0, microsecond=0)
REF_FIRST = datetime(2004, 5, 31, 20, 0, tzinfo=timezone.utc)
REF_LAST = datetime(2026, 9, 21, 11, 0, tzinfo=timezone.utc)
REF_CANDLES = 137837
REF_METRICS = {
  2.50: {"trades":96, "winners":39, "total_r":35.01047956198102,
         "profit_factor":1.6142189396838775,
         "fingerprint":"4b9cba40eb68ec5fb38ffdf4cccbd1a3637762c4c60d281ff7eb7ffcf2138d26"},
  4.00: {"trades":96, "winners":30, "total_r":47.80685132075335,
         "profit_factor":1.7243462321326266,
         "fingerprint":"eecbca171c81fc2533d057736a24b7826639207e722bb7a677ffaf6040454507"},
}
VALIDATION_START = datetime(2018, 1, 1, tzinfo=timezone.utc)
PRE2010_END = datetime(2010, 1, 1, tzinfo=timezone.utc)
TICK=0.00001
PIP=0.0001
STOP_BUFFER_TICKS=10
H1_PRIMARY_COST_PIPS=2.0
M15_PRIMARY_COST_PIPS=2.0  # original historical engine compatibility; H1 ONLY
RR_GRID=(2.50,3.00,3.50,4.00)
STAGE1_RR=3.5  # not used in this refinement
COST_MULTIPLIERS=(1.0,2.0)
BUNDLE="EURAUD_H1_LONG_PLATEAU_CONFIRMATION_RESULTS.zip"
ROOT="euraud_h1_long_plateau_confirmation"
OUTS={
 "coverage":f"{ROOT}_coverage.csv",
 "parity":f"{ROOT}_parity.csv",
 "matrix":f"{ROOT}_matrix.csv",
 "periods":f"{ROOT}_periods.csv",
 "costs":f"{ROOT}_cost_stress.csv",
 "rolling":f"{ROOT}_rolling.csv",
 "rolling_summary":f"{ROOT}_rolling_summary.csv",
 "years":f"{ROOT}_calendar_years.csv",
 "attribution":f"{ROOT}_incremental_attribution.csv",
 "changed_trades":f"{ROOT}_trade_level_differences.csv",
 "trades":f"{ROOT}_all_accepted_trades.csv",
 "notes":f"{ROOT}_notes.csv",
 "boundary":f"{ROOT}_boundary_decisions.csv",
 "plateaus":f"{ROOT}_contiguous_plateaus.csv",
}
STATUS={"state":"not_started","message":"Waiting","pair":PAIR,
        "orders_supported":False,"trading_enabled":False,
        "tested_configurations":0, "parity_passed":False}

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


# ==============================================================
# PREDECLARED PLATEAU PROTOCOL — no live/portfolio writes
# ==============================================================
# Boundaries are deliberately widened only by the FIXED rule below.
# If the outermost expanded boundary remains viable, report UNRESOLVED_EDGE;
# do not assert a plateau and do not search beyond the declared safety cap.
# The control remains immutable at every RR and at the 2026-09-21 cutoff.
CONTROL = dict(timeframe="H1", side="LONG", family="SWEEP_DISPLACEMENT",
               lookback=30, body_atr_min=1.00, wick_body_min=0.25,
               close_location_min=None)
GEOMETRIES = {
    "FROZEN30": dict(CONTROL),
    "LB25": dict(CONTROL, lookback=25),
    "CLOSE080": dict(CONTROL, close_location_min=0.80),
}
# These are the ONLY one-factor axes. LB25 is an individually investigated
# alternative anchor; CLOSE080 changes only the original 30-bar geometry.
AXES = {
    "LOOKBACK_LB25": {"anchor":"LB25", "field":"lookback", "rrs":(2.5,4.0),
        "base":(15,20,25,30,35,40,45,50),
        "low":(5,10), "high":(55,60,70,80,100,120)},
    "BODY_LB25": {"anchor":"LB25", "field":"body_atr_min", "rrs":(2.5,4.0),
        "base":(0.8,0.9,1.0,1.1,1.2),
        "low":(0.5,0.6,0.7), "high":(1.3,1.4,1.5)},
    "WICK_LB25": {"anchor":"LB25", "field":"wick_body_min", "rrs":(2.5,4.0),
        "base":(0.10,0.15,0.20,0.25,0.30,0.35,0.40),
        "low":(0.0,0.05), "high":(0.45,0.50,0.60,0.70)},
    "CLOSE_FROZEN30": {"anchor":"FROZEN30", "field":"close_location_min",
        "rrs":(2.5,4.0), "base":(0.65,0.70,0.75,0.80,0.85,0.90),
        "low":(0.50,0.55,0.60), "high":(0.95,)},
}
RR_BASE=(2.0,2.5,3.0,3.5,4.0,4.5,5.0)
RR_LOW=(1.5,1.75)
RR_HIGH=(5.5,6.0,6.5,7.0)
# Each RR sweep uses an UNCHANGED geometry, not a newly combined signal.
# Mandatory parity on previously inspected control/finalists at cutoff.
REF_FINALISTS = {
    ("LB25",2.5): {"trades":106,"winners":44,"total_r":41.73519048992438,
        "fingerprint":"156eb6b291dab8aa5a4fe788de7bcb59e4ffaecfe5284e1f0a7a93c22fa88349"},
    ("LB25",4.0): {"trades":105,"winners":34,"total_r":57.90421353631645,
        "fingerprint":"60918dffa0a02143416e42d7cbd9f1eee90029d063bebb822a4f7189aec90bee"},
    ("CLOSE080",2.5): {"trades":82,"winners":36,"total_r":38.97032121126139,
        "fingerprint":"6f9d79b3fbe25a5f8a7730b16c076e2d8338c6cba201c7d75b41ad8c27608b5b"},
    ("CLOSE080",4.0): {"trades":82,"winners":27,"total_r":47.463767962582494,
        "fingerprint":"46655efddf9e2cf37300c0e5692aad0cf2e53b06f5daf7bfa5d5d3ce2b18deed"},
}


def label_value(x):
    return f"{float(x):.4f}".rstrip("0").rstrip(".") if isinstance(x,float) else str(x)


def make_cfg(anchor, rr, axis="RR", value=None, phase="BASE"):
    if anchor not in GEOMETRIES:
        raise ValueError(anchor)
    c=dict(GEOMETRIES[anchor])
    if axis in AXES:
        field=AXES[axis]["field"]
        if value is None: raise ValueError("Missing one-factor value")
        c[field]=value
    elif axis != "RR":
        raise ValueError(axis)
    c["rr"]=float(rr)
    c["axis"]=axis
    c["anchor"]=anchor
    c["axis_value"]=float(rr) if axis=="RR" else value
    c["phase"]=phase
    c["variant"]=anchor if axis=="RR" else axis
    c["config_id"]=(f"EURAUD28_{anchor}_{axis}_{label_value(c['axis_value'])}"
                    f"_RR{rr:.2f}")
    return c


def features_for_plateau(candles):
    f=build_features(candles,"H1")
    lbs=sorted({int(v) for v in (
        list(AXES["LOOKBACK_LB25"]["base"])+
        list(AXES["LOOKBACK_LB25"]["low"])+
        list(AXES["LOOKBACK_LB25"]["high"])+[25,30])})
    for lb in lbs:
        if lb not in f["prev_lows"]:
            f["prev_lows"][lb]=rolling_previous_extreme(
                f["low"],lb,want_max=False)
    return f


def refined_signals(cfg,f):
    raw=signal_indices(cfg,f)
    minimum=cfg.get("close_location_min")
    if minimum is not None:
        raw=raw[f["close_location"][raw]>=minimum]
    return raw


def fingerprint(trades):
    lines=[f"{int(t['signal_index'])}|{int(t['exit_index'])}|"
           f"{t['exit_reason']}|{float(t['result_r']):.8f}" for t in trades]
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()


def reference_parity(all_candles):
    times=[c["time"] for c in all_candles]
    cutoff=bisect_right(times,REF_LAST)
    rows=[
      {"test":"first_candle","actual":iso(times[0]),"expected":iso(REF_FIRST),"pass":times[0]==REF_FIRST},
      {"test":"cutoff_last","actual":iso(times[cutoff-1]) if cutoff else "NONE",
       "expected":iso(REF_LAST),"pass":bool(cutoff) and times[cutoff-1]==REF_LAST},
      {"test":"cutoff_candles","actual":cutoff,"expected":REF_CANDLES,"pass":cutoff==REF_CANDLES},
    ]
    if not all(r["pass"] for r in rows):
        write_csv(OUTS["parity"],rows)
        raise RuntimeError("Candle coverage changed: STOP and inspect parity CSV")
    f=features_for_plateau(all_candles[:cutoff])
    tests=[("FROZEN30",rr,v) for rr,v in REF_METRICS.items()]
    tests += [(a,rr,v) for (a,rr),v in REF_FINALISTS.items()]
    for anchor,rr,expected in tests:
        c=make_cfg(anchor,rr)
        tr=backtest(c,f,refined_signals(c,f),rr=rr)
        m=metrics(tr)
        for k in ("trades","winners","total_r"):
            actual=m[k]; want=expected[k]
            passed=(abs(actual-want)<=1e-7 if isinstance(want,float)
                    else actual==want)
            rows.append({"test":f"{anchor}_RR{rr}_{k}","actual":actual,
                         "expected":want,"pass":passed})
        fp=fingerprint(tr)
        rows.append({"test":f"{anchor}_RR{rr}_fingerprint","actual":fp,
                     "expected":expected["fingerprint"],"pass":fp==expected["fingerprint"]})
    write_csv(OUTS["parity"],rows)
    if not all(r["pass"] for r in rows):
        raise RuntimeError("Original/refinement finalist parity FAILED: STOP and inspect parity CSV")
    return cutoff,rows


def periods(trades):
    spans=[("FULL",None,None),
       ("PRE2010",None,PRE2010_END),
       ("2010_PLUS",PRE2010_END,None),
       ("2018_PLUS",VALIDATION_START,None),
       ("LAST5Y",NOW-timedelta(days=365.25*5),None),
       ("LAST3Y",NOW-timedelta(days=365.25*3),None),
       ("LAST2Y",NOW-timedelta(days=365.25*2),None),
       ("LAST1Y",NOW-timedelta(days=365.25),None)]
    return {name:period_metrics(trades,a,b) for name,a,b in spans}


def monthly_rolling(trades):
    anchor=datetime(2005,1,1,tzinfo=timezone.utc)
    last_whole=month_floor(NOW)
    out=[]
    for length in (12,24,36):
        t=anchor
        while add_months(t,length)<=last_whole:
            end=add_months(t,length)
            m=period_metrics(trades,t,end)
            out.append({"months":length,"start":iso(t),"end":iso(end),
                        "active":m["trades"]>0,**m})
            t=add_months(t,1)
    return out


def rolling_group(rolls):
    out={}
    for length in (12,24,36):
        items=[r for r in rolls if r["months"]==length]
        active=[r for r in items if r["active"]]
        out[length]={"windows":len(items),"active_windows":len(active),
            "zero_windows":len(items)-len(active),
            "positive_active_pct":100*sum(r["total_r"]>0 for r in active)/len(active) if active else 0.0,
            "worst_r":min((r["total_r"] for r in items),default=0.0),
            "median_r":med(r["total_r"] for r in items)}
    return out


def compact(t):
    return {k:(iso(v) if isinstance(v,datetime) else v) for k,v in t.items()}


def trade_attribution(control,candidate):
    base={int(x["signal_index"]):x for x in control}
    cand={int(x["signal_index"]):x for x in candidate}
    new=set(cand)-set(base); removed=set(base)-set(cand); shared=set(base)&set(cand)
    new_r=sum(cand[i]["result_r"] for i in new)
    removed_r=sum(base[i]["result_r"] for i in removed)
    shared_delta=sum(cand[i]["result_r"]-base[i]["result_r"] for i in shared)
    delta=metrics(candidate)["total_r"]-metrics(control)["total_r"]
    assert abs(delta-(new_r-removed_r+shared_delta))<1e-7
    recent=NOW-timedelta(days=365.25*5)
    return {"new_accepted":len(new),"removed_accepted":len(removed),
       "new_only_r":new_r,"removed_original_r":removed_r,
       "shared_outcome_delta_r":shared_delta,"net_delta_r":delta,
       "new_2018_plus":sum(cand[i]["signal_time"]>=VALIDATION_START for i in new),
       "new_last5y":sum(cand[i]["signal_time"]>=recent for i in new),
       "removed_last5y":sum(base[i]["signal_time"]>=recent for i in removed)}


def viability(row):
    # Exploratory extension trigger, NOT promotion or proof of edge.
    # Boundary needs multiple observations/eras and doubled-cost survival.
    return (row["full_trades"]>=55 and row["full_profit_factor"]>=1.40
        and row["full_total_r"]>=15 and row["cost4p_r"]>=10
        and row["r_2018_plus"]>0
        and row["rolling24_positive_active_pct"]>=65
        and row["rolling36_positive_active_pct"]>=65)


def close_to_neighbour(edge,near):
    # "Good at the extreme" means not a fragile isolated spike:
    # both boundary and its immediate neighbour pass viability,
    # edge total R >= 85% of neighbour and PF >= 90% of neighbour.
    return (viability(edge) and viability(near)
        and edge["full_total_r"]>=0.85*near["full_total_r"]
        and edge["full_profit_factor"]>=0.90*near["full_profit_factor"])


def add_plateau_rows(rows,axis,anchor,rr):
    data=sorted((r for r in rows if r["axis"]==axis and r["anchor"]==anchor
                 and abs(r["rr"]-rr)<1e-8),key=lambda r:r["axis_value"])
    out=[]; start=None; prev=None; group=[]
    for r in data:
        if viability(r):
            if start is None: start=r["axis_value"]
            group.append(r)
        else:
            if group and len(group)>=3:
                out.append({"axis":axis,"anchor":anchor,"rr":rr,
                            "first":start,"last":prev,"members":len(group),
                            "min_r":min(g["full_total_r"] for g in group),
                            "min_pf":min(g["full_profit_factor"] for g in group),
                            "min_cost4p_r":min(g["cost4p_r"] for g in group),
                            "config_ids":";".join(g["config_id"] for g in group)})
            start=None;group=[]
        prev=r["axis_value"]
    if len(group)>=3:
        out.append({"axis":axis,"anchor":anchor,"rr":rr,
                    "first":start,"last":prev,"members":len(group),
                    "min_r":min(g["full_total_r"] for g in group),
                    "min_pf":min(g["full_profit_factor"] for g in group),
                    "min_cost4p_r":min(g["cost4p_r"] for g in group),
                    "config_ids":";".join(g["config_id"] for g in group)})
    return out



# SHA256-locked, archived accepted Portfolio27 baseline; not reconstructed from signals.
BASELINE_SHA256='d6b83b591ce625ca3520871a91dd5d66af3d7e5b09e9cbf63e65094d5ce24a7c'
BASELINE_B85=(
    'c-q9hTd(yzk{<S7`r5t0I`A}iK8!sG#)r&wUrch*Fp#k^fMEpATqFqm-y@2Ymc+%PN?R1O_xr8)9aC*R#Z@d8i&ank@Bi-~{_(&6^Z)pN{_t0SKx6D54'
    'gAsYpN#)2wtr>J|MrJ}@rQr>zyIl<tI_Zo{XhTnKmCto^soQ?cYpa`{?k7!{}1W^`Xl_9^w0Ef{z1F^`~Ui<|Ks!6U;aJVzx?fg{15*w5B%dY@~{5<59'
    '|N&U;q1m{%>Dc$dtGJ^M6^!mjCe|{^!4F;I@APSRNE`FQe`=T5$jQ@BhsKH~s{7_%Y-*DQFca3sL?zs}O(xH~;bP{`8UJ@BjL5|F$W{@^bT;w7wWFi7b'
    'lY)BjJk82|Fm|K=|Xtj6@y&u8|_Q-ApzP~%>(942kW{Wl=NUgj6944bcK-*eo5D2|$Zclzf0lfU4M;^c$*6NF1Cn1DQ5js~K0yA*i%CN?Q<d=vik>fSd'
    '2_$QDR2$mHUn=<_Cf4M0T{V)ggHaIjcgyo+tbt?axB!p@$CtONp0)GNoix66CKsG_XWrFlmpTKCRKK#j`Xgbqq+jL56*Pn5x8CRRmj74WUmuJz)^Bqry'
    '^?>`|{j2}f#BENnm#6ef5>5;7VzT?77Zn2ATdU`N%G)PaCIi=*c!WP08F?Co3oCO({>-qT`4f_E!qNT-<qZ~>z2e%$Q}8A_?-?RFi-TmaCr#+dKp;B__'
    '(wzYG(91?0UBk{&;p&Hw9U%NBP);HLJb<!>tu>XT$<`HbvgVne*Cc~`jU|aoe*iuXHkguW_q^h%Vuhc!NKAXxPTm*o9SlWb>BnAMM@DCkXG=_@~E3;Cp'
    'R=2Z)|<Gvsl4i`;_8#dQckOe6w=3W?Z(5+D}7$o}w=5)k|hcOZ>^Z7re&6<gF<`2;rkM=F4lmqcSTr(y5G-9eDgB;_0jTt$n|NTWe<gY0v+j{e~~oJ9G'
    'Q?nOh6oC;vp_%Vf<kpD(AOH~fYCyf-c9K6Cg%Y|Z|*Pu-JZK?W)*ka}i5joJ${FCDt?8us)qrD4Z4;2}*tuBQ!{5~$g*C?`9lEuf8?k~?GQhJk##A!Rg'
    'b<i{iI_l%>Nr6%E|lCqkt6DoqHHNu7$(@j6lfd^yAgJ&Ns4cMJBFs%$I2gUMeoZ|uKm$^HrMMbC6=;Z>fH=|p6zJbx`ITZ)F2Ru#$PA5aw2Odt)YTzLm'
    'DYjZYd9(p-VAQ~m8|61}G#9pfak7rPsD}}TeFLWP*OzCb?D<kUvNR0)*I~&JFK0dPD6%t0OY>V}nQ^gl$woP-)DvO6|CnJ%9qe=*j52JPXdkN}c^5Uyz'
    'GEK6uhn`!b4c6i?Ni1EMz>nivm{S9Utcwi3gB(iq@M2}!)~6Z&sxGiz~0ypkSs?0Tns!@89tqfFGT3APkZ`aqzfAS{MwHQ&%XxkVzoV6pH7YZ@P_Y>+F'
    'QMSPiGTJe>lahh2ZID@@={uvpz)j<N|*J!1kwkMWa%m1WTzt*@2fV4o1}u6$}8m@_Rh4{{FgVL#8o5oID}~Ay&i5qer-i)hvf57m>{V`S~8OZABGAY``'
    '>;8cuiOhDBvcqw&@%z~)7Fg8i3&`*(l+zyA5J|HHrjhid`sNdsh6;_f|t9*qyrg6dVw<)WTd+LP{KNdyjbiqF29V34t(ZoZR&wKD8*3h7HrmjhVm0xtf'
    'sUEIv)&Sl2MLqv$DgQc+lqT$6m4(Lp9!NGw2_nrtr4pefvbe*O=;dG7q0CqSn<x5ZVnUTz}m1{N1cWCSAEl2Y~v?fCd%Gr*mbf*UDtC;YxQ}_l(w<j&n'
    'BFYYqxxR2(G<v(<wj8V{J8JR~bd#S@_GO?sN5TBE8>p#^dfui~&0rU{?ZURR?hW$0Zh)4zA#^Qc_?CuP_0EG{z6{Qzc*BHK%Y@%h5wE-s=GC#6{aiffJ'
    'H4wSL3y=g*I==9%w9*2CiT_*GG(2l3?<EenURr<n_BzjONoOS3@YcF<vSX?rVslYxPO0`&Pc2hbd)uF9YvZH1<P2Jx}Ac!Ai_?gej4obHlv0u-uBv(r&'
    'K#tFGY<uFrZqK0P=QY(+SY{DF!ejVdZRhPWQIXPnJ%bj)Jv}upHC^vmX&C%E+A394?-Wczm$M=jx|^Em-$(FN3k46g(EP@>N14U_mbgKv#%x8SQf3e*Q'
    '%JF_h-_`%-$dQOZCd+Z?y1JcJAPmVVNKVo#rS){k&@GcuJ0aw*6po{n6qP%!4gZ^WNN^q0J8=5ro=NR<p37${}%r*wyg4DN>S`w;dy>8lGnoZ^x->~MW'
    '^GZ1z-P2mM>(XDayb+KSYBf7<YB6wqh<EHsJ>;rlkbY9M-453vLRBJt3G?sl#e>2Bn--B9)D4S4M`*H7{o$~9H76&@}m#cH2g``wiwk@=7{bcZNkQ88)'
    ';V`#ev2~<y_aZXSlqO-(!gjKI@7C-}xZ$@wTd&n5J%b(R&9)vk!pS45U?a3j-fjkEl%CCoMH4c0>GJiMomJzeR#@hplD7?u(c%Uaj#JAhw>+}1tjI&bY'
    'H+{<T;mOx=4j>Ws8=mSbASD^kB5rq!`}4JJXX=VuI7j2bcx?YIHfJVn+3FiQO<rR-Obn8tuG#IGZNO#Hks4$v6^i%t<=u?Bw`L_+HrAeh6EKhXd1KQ*H'
    'B&*%#JU42r#A6cEe(B;3i3D_+AwINnrJB4*R&dr|SUMfmQp%U~S<1X%U`BD>E#W;`<atJ%*9%nRlm4*|%qY#iG-6z|+=W&pgg^!`8Q%sGDvGvYqke#{E'
    'ty{DbtE4UBHSd-pZIt^j$}fAsnro3L&-66~<v=ir=7feb~9n&i0HIy#N)Bx5mp*wlsXF0#b&Cs_FWas0fbJi|hz1ZM}gmLRlZd0+1DA%}1=>SJ_=PjUC'
    'I2%Yk`Jc_gY#tn>SFAd!UygYg@8uMF9mY`3+3+qu9Wzc?N2q}XBCGIs^*>)83Xl&kOUzal7t~K1iXgtMGC#8J@2Q%rKGL-#OJKtikE3C;_{`yFok~4#i'
    'jw2Y0qm(8nLti1Kr5Gy;kw)E@^YYSNYknQ8?wRtlp$A-0n(?M3AyFFgly)zh7Bbf!F8xFZhR_EWf;FB-xVsq`=w<MtnaPoY{&{rKZuhwzix`IZz8f-~%'
    '^}mXIqh%a<(&Lb!UC#qflP0cA~SW9BBS1<de_5w)K9894EhV<xK>L@O23WHVT#<|rcv`)(;C*e=OsYqms#a(d)aM_0>k1P;l}(fRD%}l_2PQYm_>CKYj'
    'VdP?8Pzezy^{C2*=NLa?!P*N!~~E=rpwkQtq6wF7vHXuK_+XQ;G#PFr=~~ndeEeey}cVzF<u9c=)l-4q;4+H3d~)j3}r2=21H(^)#-8j-*o>Tyuh@xyN'
    '(H@XLHj+xv9Wjh%{~byT2`8NbzMEI~QpQ9jnia&0u@V3Cn%K!d3{29Kc2DBhqkdElRvx2duP)3b7sdm6P8(dKwJHiJ=ZKpB~oDpf|qMBG-F9<@9Qrl%('
    '$<}my|oU4=pj4o9wqm=B*E(4hv(a-`WD3u*n>b2(nUq;(pL&uY|wP?b|6KY{ZruB|Y;SA(wJ4?1AvV2?hfz(Sj;l>_r1J^md_LB_Y%_j}!!QEFu209@+'
    '#e;eDl9r8rh+U-My|pHW_}FtVs8BC}l?4Dz9uK2KjMjw2I&bjtR$E}PQUVq^P12}sRi}LMS0Q+cK!(X0Fm}wwi>xF#NzXdb7cQfI&Ru(+GnF@7fH~2Gq'
    'L+vu$KSQ@V02cKjOfOv(KRYIrOa^0qR|tmFQ!kp^XN4fMKs<xg2iH@4H!o#4b+_5veIOfB+J0p(SnrM{`sX5#T*3}Dn*blvGeH7+l~jDE~sy=3o<9;4%'
    'bOwiBvpzur@j(^U0<9y|o8*<>&Hf+M8wFsR+kkUAIs?7>%#Fw(=0Y9VnN<SL?QLuSDw>M-y<&x=p}9H}@zv11O_BX^(=M@~aD*WSz&7t#O$^xRz4-#Zw'
    'ExSob*QRAAX55`cyr#6|QTOkg6<;0NT)+>~JrxVJrpJW6vK)a+PDwc%D|)(zZY5>w><I*q4*>zxD0O5<A<xUZCTl(*?ZP`GYFQN#^+kKOEvmBG5kPS$T'
    'siOAwHJi~@L`GgsZ?i7yYz}{BxtqlfUI0X1<y<iO5A-ayuP=cg0nQ2LsBHp8OeR2h|gV6y;nGqt7&hsNP7^71gL^0<Vz9`})k^%_cqg_rS-g$@5De(#}'
    'Qi|YCF}7L+cU{~+%9Gr%fXY5D&!RwamPY+`H31KVjVgk(0t{u1@vZE<FCKyq1L1OZJ@&|aVl7cw44)pAbFK0yv}=FbS7=x}F==1bPrf$0DnE;+hr7X-^'
    'G;Ub4(1ZL+d61A?c+X5Q|#mZx@-HGAC)=;=FxaB`Q*1g`UXYe46Kg=oD{f1oksnI%ea^pd-#d?*nd0#)2R%gutxXMFogp007eO1vXI*-;EaT&9SbOxM)'
    'KF)ry1t7C92D`-jOw%P4A=OhAroxwkg-*j;e0c+q3QP=|1YN^W1H;p4kNxI6|<arax*TsIGMXG)`QmXj&PZFSuU^EV@;i>hopGj(lw+2bvco6+xjZT~a'
    '|dFSu@{69O>%+u#On?&6+6;&zxLg?3|U)zmgFc{E(d+1Ew|nCeg*dH?)$dO)VJG+4YZQ3@vod<SPS9k3{I2ia>xfwStREh)dw=(HwtGJ-JQY1Ce1jqwj'
    '3)FxmUkZ5>-W>nLka)+Ea{b1)%hwo6Va`I8=F`Y-L<+8tFXRuBI?}E17sdm@>nem4YG%VJ;=a;Fj(Tlbx1d>qmwvAra{w&+5KN5q`xI&;EAuEOPLT#|y'
    'T0Z()A%X{P<pu^6`z+PXyidSMxMX@E?!LcGwF0Qr@waW}ItunikLll6*&e`XtR&UZgc|tF&<52_W4aR_WIIU{8U5U+J)sJ>n5ODV6D?z=={Km_!0*7Y3'
    'p>t(&B~~N0`}!0bwJksZP1<FRBDg*isM51?K9&tH&MSp8?(JlYe(RGa{GooryOO+;*_wxkPonr#u=@z4_I{DP445u0`4{;_iMPLs#)8P%!+DKrPxYD;$'
    'Tc*L*yVOTiYQkRF|Th9|a*>aFap7!Ogg}_MzZU`8&V7``VFtr4SMa_hA&K1nlzLbnzpqQ;TZc;X2L>KLO1CC&H)kBG$`5dxLjCrdtMNPVu=)ip(#-bE4'
    'H77!B~Rc8Vw9yb_V~b2G1)?u<m`baBmuRDs5GeaKstsjks7u?HGKzH{70X-c#%cVvyK1-f;zYVWAIb<j0$4;w~n2_kS1XhCnsO22w+^`&s~t<2W7secZ'
    '|pS_T5`9aG(<W$Nf@~`<a)<#7}^2%}>BfeylfoPZSVEy9^MS5P|u|soOF}N!R-=&lJ{T`dKsGLh%l95S*z1GXqML8&>{P3-w$$uiik3vbdX0_|jzE8KX'
    '?aj&-YW+U**|I^!u-|?lV>6C@do66xG+S4mZ!0TNyw|$(QKTckj&)iD<|O0y^g}ykS=tDGM=5@InNDk#XT`vhE!ihiPTv`dQBJm(T?Jd;)r%E1j=(q{9'
    'f0-GKM#9kT%;61%AVav`G&qc+N`WLd$u*Z6>zkzh6`_5d3EZ8t*a(#*mRHQ_qS|0`^%J81-n}+ax3NcE!6rQ6}vlX<>0;Q6mM8G#?aLq{wLhIf0Lgx+p'
    'fO)k!RbjeJ5G4Bx|!QxMK8434{!W*?R{F7Di0{225jBoXpYI5#miM!X1=VehER2VmrZYdE+8m+b3#$v?^JtK)16tzdT>`Zp&)}Lqi<jj4Y^*9j<O?UrB'
    'MlCC%@rJ|Q99)QcPzc@(d62!@#&9dX6}sH`w4u1Qlam|W=tsRIosC%!S&ibBrAj|b-mIJ{r;)s-QfNP%k^!n_@I!$K;vj^WgqrN$j4wx8qPM9~@t%b|M'
    't2lAlgstuZEck*)I0$1952!t&!(&z8ER7WOXf-~)JVilo5U-$-T$N`LU?pbyw*U`om$XSJ!DRvafPeceK*1-l$<5~KeuC@}QYAz@;{=~fl*&>L2ZbmaM'
    'r&J&-r!xDxC#%Go2J!QUjD3E?;7cn8od{mw9(7wEkE=?u#Tt<#vlt=@=@W)r?rg|NwIBdw*TL6KuiuT3<=3HZ_91rHK)hZ9BXLKy;v=0*Oobcyy4Ge<j'
    '>C_0){eN5-H_?bC<ZxUC6D5q_~iOBqr0d2YKc@YS-x(jx)}Lw+g(0`8{ji<^QBOqP*OZek^pA)tvP_v>{)A9AnZ*2xp?YL(%l`6?mjKlw9XZB;L|nfi_'
    '8QEFrWr5PdV6DSFbmYvKwlZf<q_t5mq`znv_$Pu~;2!<eoE75gfj05H~2A-9JAomGDWuYHD{l`bYto31fr<3B(hBctd7%Qgr-bsd3Famk8UO>2#)(R}*'
    'TkZ(%nAzwAPGIny!_7#Oe}ef0tAiV)?*k8PBvq$F-{Q)*0<WAp1M;yjIs%HaG4OxlVPkvd(pmP%aH9vJt}=Zp{sWSV^`)-4`SQshxLY4T(7m=u#A>m-m'
    'GHn(WR>Ow7h{^21-#QEh`<<8``DP9&Mw2rMWxicAzQvwH+Q*dMHMmQ<v6bmba5_phnaHBwN^~K47?0f(K8-g)3f|FwdrtuWwT3~MlOCM`0fLC*eH!Mbn'
    '8|9j~ZIsNEV!r%3qtqJgB(TOc8&MM__T(3JF?LZMGbBtLl>}X`w11FnO&N_-?xo7hoE~-Pp}u*0{OkScdj>CHOWSJcXh9PvTeYixciN~?yGi%01qmxby'
    'WUh+j6rW}h||xt#5<QqW9!28r5MbPMzQ<kUM5m4Vj6{O0^lLp`~?Z<rW-&EWt7obv`Xc5R*9RsxX-yYuj3{?Zr8KhpeV;)PDhTmU&{ggC@GxUj3f2-%s'
    'k3dBE02wD*t?k27+}#Zk11b<^MP~4ht78!(*Do){J(ANNppWC<MVhE`s03@pdR0*dRA6%cHL4Xnj$0a#DC392yt=h|TG%*M+S2&?8ewqrTM^;OmqGurF'
    'T+0}e_1<Y|@w)J;Nb8G_XLb9nH&U<t~p551{JZbidtdVO+!?}FsKd%o4KuvC@ksRe?OP5;{E0TdY#U`=GA=y0(>_IXA`!R3`A$XP-A=oHJ7UuKjlFZM2'
    'TrJKhk=S;FZpV~j`@wqb($}VPEzoU3Fxr~yT)Ma?bqH^XjnN|@tuIEN`OJ5!oWfe543)O-RddMM6Iz4Ukq?>RrpHx?lI1{FvRIzT?-31-QC{iZqM{}8^'
    '1c#bd3SmT&Qb9&qjgc>fA@h_O1^Ys#VFc?#BtJ-flsCk%^4N9OE5JN6ld&kx49NM;r6~qqUs5_S;37vznQj9vDGM%AiXf+4<pvps=k$9%pwYPXS+lvT_'
    'jAh3%FC4Vo%vo^m~VvErSKOS;GrK02+r8uYswuOsU{!KS<JN<cG``{jzx*vi>%nCO@q0Z{xO{b4qz^U`<3P%g~-QYbV5VN{cVcCM}C%0rl`hE_d0Gr#7'
    '!wss>j_d_1j0k=exepb_dhGocsLq3+>jV^~K=R0lgN(o+jkIE?EaGq{R&#b+;0&&QO1HM{<2Pw!v!eNWO6Q3u)J9E-!05S&R(eqj_BZv;!lZ4Czqbk-@'
    'Z%tZ!C_?x3{z-RXo><M-EILGtN8fo`_`sf)1V@;VW)YcktG?l&m-_t$A{CbAGJ+eh8|7g5{06wxsyXrACk-bQVcj!4^6_J}4_DIT!s_M<9&@*8)974xm'
    'xwwmx`el!Y6O8Y2ZI0NH$(Tqj6i;9_?>u9<|iPL6VmB^WK86ov|M;$K9-{{!05hy&qEZdO<l`+JwGk`)N>l+>n>UV!Exf>*b4$>*(=)nYInLT?Gz#Fpe'
    'Y1I7y40JOegb->TZBiD!?80PFM)xJ;9^lfKkeR%vR0AE*=w=>RxM|%-C$M7g@6t{;3)G|g=)IQf=C=pb4T{FLuc4x2)T4rpzQrgq{z~isMW-SNLZbKv`'
    '&Q4L-}{IgYL$xQtWFSAB=MfmEEQ=bn2K}l<&6~UX4`LW|C>H4CVOp;VNh6)dGx9S*|N!p_~{e$Az6BK7TS<$mS=A9A<aG2(Y!b3K10#Yy*)`-qqb6-(%'
    '>d)2P_(^lKXoYdA5VpXZn;69()YZS_fnUyAhF>RR|>I+y^;wLAQIV{`wAV$KrLctB&;*?26x6AO>c_&mZ@h`e#B^H8EWwSm<ZxKV3+s<O>|o=<Eyxq3X'
    '?-P6U7#ci4%u<<KE3B0s;xK0wng;AMY65XHdNqbD=Lo7tc!VFw}gdK(3rk*#r<B$c|OlrrEy^!L~1GJ0IjcnAS~a6_ymsxk)r%63n}Mcy_F;U1AUJTHD'
    'b0Mjg9UMZC%zj7n=2RrAax1ljQ<WiBxIttCauy+)nl|>%Dv>OoDE7zRD!tFbRQ??8b4HRUEpp?d)_K8{=C-IOUbNb^UQU(;Cr^X+UX>2I3g_bBw5wB}*'
    '>4(4(I!neuqI?@AMNsjHY&hF_H0vlDsr}kJOBtdnk7mc_#WjKEI!KH^;ltBK%wsOPxfIT_)k+b>{Q%D$U0x*lK7i5f0qN35wxn`p$ca<<klG&v-ldz@I'
    'vJSm=FhGtW9bmEjyAf6zfqU7g9%#T)CEp=yHx%*VwsVsPAj$)Oto!|EnRWROq%Uh^_Dc-2y%SbmN?p+*+x*thwaKW<7LoMB7{9&NBLk7V)=~jAsOQi%j'
    'qP@Y5Hy3=nC5;&j=N7P)MaMWCR+_44Y30T2j%Iagn#5IN}C2;{v69AtT=ct}=j8q@!5!Yc>!?b7+nCASh_-L!PWYuD(C4WpFL3w0C@cS5u>QVLN%<t}O'
    ';GW0{MuZz-_bKmCw1;zq%*(I8jna#vyiTvsoSn|*-Ocio%)T;~sh&8Zk--rb{IO*+t>@8l{valX+=eOpK0+q=3os65-gZd`T(?Mo^`-@?<6M~j~Gr!&z'
    'Gu=g7<&CGA~!hIp;WZAT-j(fPJeM+J%f90x2%h0?(N;2v=&#rjARnG+8R#cLdpK30>F^Hig*t$jEgCCI`v?0@}OlALSDO@Xk?-h@1gF-skBxmn=A$<1b'
    '%>=vuA>)UrJ&-b$&vz3LeQ70mO%MO!2}JMt`GLgivl0E8BmuGr{^Xs$dP(c+ikXh~gmQJ?#vSU8P{P!MGeq8|Jqj0xK0K!&*`cWpQ=vYm*HX~k1o_|F%'
    '}{qU?ZWnQDrhLohL@*UkDp)XA5$^xz*s3mkZ)<T40z#FE@}ed6Yw4s@7|DUjA*ED@@px?wwuQbbBrNaV;6UAF+i3^+?R*ae^vKQMA^H~qhVGA>TAlnlM'
    'hDrdcwMoOv;GPP*6ABz;cu_2zixnB+X?cK@A#YJ#W|Nep#IO`3+`}#Zl*mhEmuo={YnzaK5wEhGvnmsOFvI^K}%>EQQe~pKCzl6l%4*xDn(U%TTJht|)'
    'AQ5QB@+T3DRQFyauq0n_bD6`5H>i0`}PHwvQ<V3feUe5Id>;;&TQ|9}s{M*@>*zT6(3_pSRgy$U5{pi?0NIfM{ZUC%IeS+?G~fD{mxe~)nr2$T_pSbBJ'
    'pB!rXtB}i<-zdwv`STtAnarHo*IQbPz)qBKbI~J>(ZjtsgkAjK=*@ejIO3EVQh#an{<N)=BVq3hITDiX)S(oy}Kgw3zz-VkvtUG8`z^g8wFWzIuy3<#8'
    'wmZduvCaZ{S<OLP4VWMGUD)B0Lye8!{~VeU{ysRLVL6=yNOG26>~u9dW_aM?<ZTh`m{j8pn^9?fZcE;i!#~veJHS~*0Ggk*;_XPGw#LibkqBvPjn1c>('
    'clM!UN>Ay8A8|OtQ8Jq-f!@*gYlZEUz%RIJ|K@G&Nzw`B$~5pkDLFXF_)prtyi}R7R-^k@%IN4HCMkcq~+3q>YJ=vK~Ltbdmi~HxJV~L)fv4O!L}01YK'
    'lH%j&xZt!e7{tDHd;f){BcbQ7|`KkiT1zTtIo{w-8UhQT;8%R!QDFmVXBWD&vzkB^J~N`CdPthrlx!QfW-ZDNzBZl{}v=3=r8!rHS$K<7t!_@wA(E#UL'
    'X_70PJ1;d5er9@ifUcPzT;uSh1E)2|N`KS_fwV061}mnN>zUMup=A{vV^7$9`O1M<ya<BU^S8z^-zq6|(_nzqOAB#2YMcs=gen|pJeC>r?i>&QRO6#bs'
    'h3lXH4y*TwR9Bc~`qXg|`H)kEiJB72q4}KIVq(mK0#1a*1xdF=P6p$T?#+8AZ)V79=Qs&C|AbokAa=OI^dD|$=A)YEwyc#u<jG8p{YG?vSXG)>h&~mkc'
    '5S7A^c=26?QBd=RV3BP!sTqn7W4;g4-Fp{8e<5O7MsXH3&yI$4>(<E!DJcDaz|M#t>|k^TM{M<%N9npzcPbBL!_fxkz2j(}2LKB$rR_oLIR@w^Zcs>SU'
    'jSuJjWS9Xg=xv1As~--nREmLk~ecx?{Ov7&^g5DMIM$k8d^@-OLiZ<j)x`V0xz!CG#YPd%<roicQCruN^<smX2hS<9&*Q`xl79(I*h;pRz~9-I?Wx6#;'
    'Rv(zbH<(ahj~YaHL8!ali#n0Tz^V?~_MKk_^7pf#4N}@cnI?*anuaTSmrx$vHHLuV=@too*=u?z?-+`><6haQRV08FswhL-7a6co*|0f6_c2%YdMhA(c'
    '-V**HS0i`!e?<JNP9Rk{u`Niu94iI+WQC~=zy@-c)!Oe2gu)~S)*>F3ioRSV&y{IWMRKVdOS*g<xHalMB&*SSFk{q~d2PJ@I9N*5UGM(&jmxDo_h63Z?'
    'JKeu@Dv}lnFaX{!}!=>A7b6$rigS)<hT~ysKkK6Ja{eaBS9oP^d%z$FGcW`%ZWY^J4h6(z?;|~vEi7Chw#k^tboYIU!ZKHq_PJ*G)P)1n1;nQ%0B-GT}'
    'w_fnjLZdHBceYbOnippq!gb7Vv7C-0#$cm|NFSqfG@OTX;I+zxMjMLW+*fsBl?+ui+vR$F20OQP(!y?^1NTvXIrZet{s)9AmtP5?I2Gnc!-;CU79yO|T'
    '?J|$i_t~m;A}MLaDel6fTmjpIXS2iOb8n>zqw3iA%suWV}R$T^MLF!vJf`zeHNu4bKOB3egYhsE?s<lv3x+Lkpkta>mYXp_-0Fu&RX!uka#Khu`{>78#'
    'U_!ODeZ0gu6~?9K@Eozt5`n-%gv-P&oQ835e(i*avc%5`^55?CiI9Svxe=&A-U4v5hwJ8Q+NIAHe9ezq+uhfzzbe!~Qm<zJt_FIBX3W{7l%Wg)JT0_fe'
    'kYI%tpSEnp4U6PiebPdf$`=ya3`BIta+y{>JK0f2BhARfAoJv1u`m{-bh?krTx5L3irMp5qpjK;yCl0@9LhqRs;+(mvjFV1E(glL0tOcuv%PE_YN_r)L'
    '~K9xtsI)Txt6+0NC(HmNW&r#4DDqHT1B+COBrQUei(omhQ@ZM$Bj)j!-9R<qw3>#V%kF~n<r#%L8bi>p=swm$#pyIAdH4U=h3X>-YflZ2~p&PNoW?ZQI'
    'CdBldO@V~+@WCZjI~FZ$*M;rx>iyG1q8*eLzdzZg8h^O%DZFg91Rr9^<uEnKY{EI+pese_eGBa(!aBO(QYaja3(JYnVH^`(*g7|`Y8h7{<AxZ1-mZm4d'
    '5EI5^3AjY;dzu>jo9A_YHnCa<t0>YR2#Uz1L1EJ>hG_!T1Ao4qw}bnQZ&0G2|6tyb7}@sfhKdR?0h-Yv7ym;lPvR8ZKHsb4y~-@soH^&2ph~P@+1Yw{M'
    'y0^Ls;8143j|@FW{Oopdmw?bIbEJVO`W98**Ob4%f_zDK>1K!LT@&0gy(xHW?W!h42E0X!tbvm~<A|!047M1o^ItS0<OBU;qIM<tG9SQhW%?%PL;kRzT'
    'QOxiFT`xhUMQkZSTV_H1P?c<VElEK}^G<?Z)V?fYvQlO32>?XSFEfk^wS6*>;SW$?*p(*F*y+bcG=j`$8{*QrG*W(JGV2lfoeL!6th3GGb2t#58>DZG@'
    '3Ig~YeN1`&8w$Qn<F!eb=e#MIrjPcPew%+q%a~|^|9B`3p;(N!BwatT>lu=>zhYwqrx!S1x8iBYXt+GFd2#zECv<3JjMZ%6Ns6`OZAvRS<n0W0j1f-pb'
    'Ik*wtLI!4)GT`YJy&(hN>N@zuyWi00>`irCs*$^EF5O|L?)*AX&bSv^mhGdVnNl&D^63CZ6Roszr^m2$f#KOPGplU*sKFnudD9i4N}Y|-f_B|AtP{#IU'
    'NP=Ye}r8h74KbLT_r9Nvn|e~!A$e(ki#$;qFx3PETgN7vxWSv#jZPJ1Fe5M`<xBofJ|d0GfpRe6N=2IVOG-BE@F4~gB;-AMrZ9y>-!+84j~yE%6b!}YR'
    'ywOJ3jFBM0i|PbpWPRn<(E4x;3N{6nUcn(Gx{vXDE!wqHs#$q>M7kakSi82CixI4sE&7di(r%;!483T7=Hm5bu$&JezsY&b(iJXeTUzJnC=IpgwYOHZU'
    '5sUwb(q$-sl;4UdoBY*;k7L*X}RA7ew=1N!{SLrNMBSV)B%P)?%^0wu*8JU2TS)$SDPj6W6bmtEnn$?V}riIi#|4O4h54`6f((~EV4Vp-<os|0<pBdq7'
    'zAj7uT=>};la&FmAA&^;o;x>cBFNDz6z^spo2X0A-cTzn<O&HjP-PuF)sGW0y3U#Mc8FgHvzEUP!T+5{3`UGVUW$nT702wkcqm+S!V&84_^6bGs!TabJ'
    'y*HdbX~fpLPM@akcZbQc5S*g?bg^_Pd`Jjmv@|3-AImvVDTbAA1!JknxT@TAhA}exV9`4h9#?K2fN9*Exb_$#VcS|qFY}_8gWxg8IDYiWJW!{gqN~WyM'
    'mgmv*1dV&;sV~0pl%@p$l;tkic=zIv2OO-1<e9oLLRlP6uEcKPO+}L@4~inkyjqQll%Wjud6xg>NVJ|ir9P10*`hmuzDTw0y*wB3Vv1@(FR6mMl`@@u('
    'SRKc*m~+ujzou<Oyv|46R7k@biSY{AQz88nMtNH_`C3tf5y{wtT_*1`ROJ8V2P-HTMUIB5ZWJoS?<$VkBY{9l?n7jtnd)Wr%WcCXasiczGu{vtdyz)x='
    'Tn#1MRDus88Ke(IJfw=Q*PH1s%lnn9_u%dhc=>paRaThdL`K@5qBv;IniqhPT**twNL(`_lEJflruXtZ_cDtX1ZTH~hEDwhc|Quj1Mij3aa0amiSTRZt'
    'qpp{)m5w7bfh8!vi5~Bm1+bu~NgGS0zJ&|kD=U5%lKWBS9Ak*njik}IUXl3+@x^HpOmwgFxHN7P_BF3=;Q)G6O_8VT-ACeBG!y|hIi&JV4iYL4b+rTUX'
    'pP+yeHTAZoM5RQN7Aul^SNaX;x2F0AYWXTYckxiL*SH82+~Pa33n2_CFWZqRO-$^~eWG45Q~Hu_V04)fdvg<#fh)pwAuii$U}A9O0iVY;c3c|M2)+EPJ'
    'SvceA9Fkx<c0$*8<YnC;sJ^A1(_-_K0V8)eW;d#rF+&>^q0_sGaM<Pr)Wp`Y8x`mfh6N(`Mcm&(BqwLD!)y0m7{{~=2RuzgzaSiY#nXf7}SxPfM^^W>*'
    'As?C}qaqxi8*<8J)^_p;t#JT|e;p8qF_HV*u-kV`Q)ccCoKsIK3|djCk)_+N8MaklFD9ioJgvGAp=1CjuIgKCz7YIhDpc7M=A^j*Su5KpM5Tg7J@Vvob'
    'e6^X#LnGF6t-yV;ZFeW2W_QW~~|9P7(xDN}-RaT_>wI<!bUA{LT?X|+AbF@-F{71J;NB)10wEZePd(U2Q*w9U}0@~faTJ9a}1p0;Q+7LA**B9qb%3N1f'
    'h9I*Tkga|fbL_?{VaYTmME`>se))ojo9pYIuxdn<<qPDWPypjtlDxxCu;v*X$5D-tv62JG?P}#(8*ATd=KW|OF)!j-#msT^6qL~yo))Q$a_Ho<O^VF2d'
    '-O9NA^|3HR(b>io*pYFw0Xtrp_{6uyDVfhWOV$S%v)gXabf&U%m6^%yZ#5@<wnDX+nT2&<D51-v(`fp3FrZQ!u?^NX3Sm;da{&`{6P~XPK~VR6g*hirV'
    'h3YX3QlVsaRYeV2;lEH!jGJ<4a_C*49yBbgKcz9r;Z)VwcD~jD)z?Aqohi7wmvdNTzq$O6d!L%J21TrZKJr$;NB0=??}87Gzm^i9=#c4`mNKlLorI!5|'
    'dXRwL|tF9cb1#xf`r@4MHAutpdLnJgprIt6{gV5%y8KQBnDOQ@G<+TqA5V+MjRD6}|Ul5SQaL1_$lNSGTU{_jhY&KMGg$+bCZ5@Ql`RN@gNMfX;I(o_f'
    'fq(FzeYSN8j(p(g~;t)krD5v1%;yi$RXda#q%<)<xJR29+3(x#(La+^opDKz<J5buBnbQA7`4omweT@V_Lxv@NeA)^IuW#Hj1Bj)c;qX#HMVEenb^N&p'
    'XE#O`_S8St4Hg+io$QhuRKYetgr0}XkM*MW5>1fSS#y6W9#zxDYE99N6m5fx<ciNS_u^Wq#v2M(!gh9QtRy6tb?8lQfzSp?+fJLV@WO}<2nO0h`?EC(@'
    'f6x9l-EN9Cjou0OHaek3et(<O;%50oo#Vq3_~~{es<UV(s=JT6NnrqOR5w2wg)5kS^v+yAF(vD*1Ck!L7d);!8m3UE9>9P~x4{SroUw43L!9K{W-LxO-'
    '#yq^^TCSSk9RR}AdW7O31gJzju0qEUe;zsnmaFR08@e?`sH-zp3?yC$c$15k{vND<$L`AZ>n-8k7mJ-gw|R6JluPLrrA@mPF1eNK)&Y7SwjWJF`|LgqB'
    ')F+6l7pZ+ldmF%Qgz`y6nanW?Uo*K9MjD`X*4v?6TS%Be65a(j?`wa4Rvuv7?YbJs!z1e;u97z(Dg&P#xa+wyMTY+Ql*!(!oZSWAG#-Exl0cx4-QMY~O'
    '=@$+tHawo}6vZwuBw3ynmL?*gLeyzyYHhu|X*6GC@;z-u<5v|971oyuplzZUfPonSM$ZY(A2!Q{>2MwE}(Y;$~%ee8ftV>4H>e9>#pqv5*Qu5%bEV)TF'
    'xVN6hgOTQ$|L5tT~5m}k!0+;b-Gh<Qe5VGt$WKQvL?Z+)HsAVQYK6*RCi_5rBDMFApT`t%3SDC_LaTq>7(LGPv-l2g?foy#~uD=OZc|a5mmJjdg`LW&B'
    'Nq6!WVdeI~apS_j)3Ur-@~uYgG&Q{IqsyVET`rYJL8^5*l}q6==VVRr%UJ;382ht9(;Na;prQkvIsN^`DV~3Vb!;P%0VZp4KtzATMOqm;OQ4-c@x25r*'
    'sz=|nT@x0#1g2ee!9wJWxvSQ{p;RLNXSsU616{_bQ`GYR-)4{OQQ~0box^9&sVt7xB9=H4G#an@;FDk{$~Lm6He^FjMDB`wCJ@EXO^v(C%?MWxxy`;6K'
    'zfUCy|smM3X`%-UcAz5g}V!ZY&j=zp_J%*Aba^c~(|S#^QCb!N{J!L{$AoX5s;iX2rU~)+z$WQ$u0F-82AZ>m8DaRz_R~ZOD{TczK_4YltMyada{iR4P'
    'rlKn-!qRZxDH7IYM9(e_c8(OK+(#i&#y5jwCq*+?21p@TCYJhL4!^$C~KHX_L$r7I^|xY}JgpiQ<t+Ql#XRFOW}UN>aAwVDo+Alz=b`uO{9R?Y@SbGzk'
    'A!cbd-e5t2<y>04_M7dnkX}hELu@yXi$ug#wsWjb}W^=u%=If5+b~*c%j}VO?;mY`$5V<Qv5J=<lXp|DT&zD&)5<5@ioQ?S0;AB`qDM1u!@AuJZUd;wZ'
    'W9<FfBUN6idGt;)&XcG?ejlowc%Kr$MSm=hhFR8e14Cv2aNFuQ`kJoX1dbr<%9`0eqc&#;W0b0boy@Iu6cy(^1dHq(xnw74jE!s&mkAao@4uo9e0^;-X'
    'nXSY)m|5MCjmVO_dMUX%BeN>;!37}L>>@f^7VG!6M;ZZxeSHW=Za8>2p1<Z1Pjiy9dlKck+EtC<f-<e6q^1}gVZNOQQg&sx`TI(np-55uf!x*C<+#{9h'
    'd8(o;KIO%FBfOs28@SbyVoAd%TN6UxFKJim@M(Shpe5>`|e6C7Va>MY1dZ1QUaeU^!y6VZ;GE+_;Yo^~G^RhIq?C_<dM)!vb2|AnQFE&21(c0FI0ngGU'
    'B_-pFu=rd$VA`>|RIxZY^_2b{H$gSBKVVHmQM6Ru$*Of#EzP<JY*Lw#W)(Pr5cVhxJ{;zY#rJcN*8ky3(A4qYDQDZM*e6DH%1r3RpVdd<Cy?5q<>tjiT'
    'T7AKY){<fvuMtRPOWP6>)RK{+`L>{%1E^nefO(NFKP;1z6D(tAtHe0*&h?$D9bLTXeHD!OB)58v3*l`ML%3%&!rQJDfB-s;SsSx%Z;G}BH85?bXYT#B='
    '8W5cI9s-~V##$JpHT=y)B56&Ky~o@*glBZGIAGD4#KZz5&wUEgz`R8o)YFY{Dx$ApV=IxPj$K=iYuY~>jKRYQ&wT@?n|t)~IXl*kZC{@IR74zgbDycSS'
    '+Fx{^g6D$oLHABFQ0&JkTX!Re9N<+x^{r*e&x|i$9I&u+`$+vaLJ1)qi)8ve0i6~he?m3=);uJFgbO;ycX2>rFdi-^{so#Hx73N3M)}NG5%Ok#~D|`1&'
    'Pyv2D!o|k3L?QTF{`ZZ%L0@@eumm@8!+1oPtHE*5JBxt!NxJsBpTxO*`8`baitlA0aF3h5OSTjm~%{Y_gu@QJ#^WRNkg4HYjakqKrEXX)2&Yt>R;h-F7'
    'T_)V31XYEK2hdxl7s125+b9;551yw^yNIx*chkKUwZ8nSbu)3ITtN4>z0y=jh7aKIjZz~K<GrX8BjUF@*elu?+saD(2>?PQCm{tD1KWPDLgE{AIa2+>0'
    '7R1pS*l6EN+I+S5blb`uX&5elTGqu^!Xr><up*Bd!okURn!YKoq?A{`9xvD$21Tk|sc;lyyicVFc&_iP%wR8A_b}WiuKz<hFby~$QrMgPA<Ozrg13BlP'
    'Z?_qja+Ovf8zT65DwAGkmAIjc+sz_{El{+m&znGvT5KZlh7Y`<(W!cBT4}}kZue3AzU%eTh1{+u06xF#Ex1@W`Bh-_TLd#HY+A6WR%q_|AtXV<N)v{&7'
    '!Y?bnr*bpN|rhC8!1U&Ud)<l?Q#8t5ro;GCn+n96F`>1O64@8k5KttN{vZ}auzivhY#*>**@{SHZ(ew0WUkgdDJQDn>UsMc40_TQhBv1hc=P;qj(vaN8'
    'u`KnSKakYR<F%S|B$fW1v90zM5R4#A2QHK8og={Uwoggy5|;5F9-Xm3@F+MzG7H)Q;TjqYy3=Wx#qg7TJL3bF24XCY^ksfl>+~PFY4{D^5Aw*ZDwEqE0'
    '(=5K*T%7QKGu@j>F1palldee^Q=G;FH#g{TfSw`pn0s6ExGzvxze(16m&NY|-lG-whb!CUaoSV!c;k(%bw`cz%6mPcb(X=3Xv@?x~_14^krDLcN!khqT'
    '?sZ(C`7mjGI0v;c1tr*j16ebY%9<h)DcXp$rpfUK1d+Z0Ap1}l-resoL7IA~SOQ!ZaFaCptM!+^CS($THlL^4QoM0)KfKRU!?#n>!(7|UEri2cTXuztJ'
    'o!U<QarS94avp>{OrU!^;?}APm>z8K3f@Mfl$xJ94|s2QgzNsb)OtdI(CYLv3KMR*%iA=~P0OQrB7>`8V=M3XE6ELQWTNO0-ExAWr&(@CWV-z+S@I}L('
    'QJ_33&{Vnc<%^C4ID|<8b61f{euOST5}0IOvpsJ4?DDj@SoAE=otL?c%WsB?4I7@eBN!9Rvxjpv!YnjalGv2=!5ZQIf^iOw4*kw3=Py`$UbF73G=dVR)'
    'xTNI)p2AMBCI*$yS_@0e=LeF*TH;$VQ59kkEAFt6<wwU<d0bq8|`9$hcI|4q9nDj4VP>amushfWbM#iBx2S4Whc8_LU4ySiMwAUs!#2%ulwe4UA^J=4Z'
    'Wd>Za0W<o)J8M=VCA_tkZ0g&W{*Wz&FYM+U_@g#l<^OO;Wa=OR!YWADPY?FKHN(Q8sY4$+1!CyPHsU082ZH}={MnNH~?zN^nD&kB5yj`Au$O6Pme3O9k'
    'K<R2pV6gpV{W;bNZ+K}mHpP;v85!`hVhAdqI<$lF$7QN)DaoYIsiYIXS7K}wlV10-jINBj!!v;<BHE_*aU5NqEs^6+F#?i!R?6T`73UG@&^h?PQS^1^7'
    'Q+ihYr8tT8W&A$IO-69>TR7h)*SLKv2K&WQ%8>$kSe&19VjpsN*^nuv5ZW9TU7}XU=2tGPG8CoGBwQ@cJ<zqz&`*e;p8y{&zGoVgk?H2X>X%+i!Pnig7'
    'jxu{2gYF2rysnE6F{r{Myz~?#dY$DeA^GpK{$Hr&5(lt7mhcb1g=euSVp5%;`oAV!FB8I9&Uu59l|r?|7*#wM=Uz+M<+WITsSr8eOAE_7+qoucdu)6D-'
    '3^~waerjq~sP_He!036!-zAQueP-Aq^SgEM>Pf2}c*VlMi2c)J{nXvAXp1&!bKVQLUp5xAUcF_y$H3qRQP^PzD}bDO|E^Rx9_dSmp*s2|TvjwdB#1CXU'
    'mt3^z1Jg&aUWrhoNGQRvF=59S*d&6&g&pG#5|pi@eZbSBAO%=$@(JMu@ieS@Y`95^?Guf-6Z`oZN4A26shF}dTSIQ>CVo58$NM=;6>?{20LuQ@+Qc`x~'
    'GPUQsHmGeW!jf(8nQ8#WGi*ny5(=&_&b2=p)vFOZV734YLc6A1llz~H*ar^Lg#MQnn;WNeDyV^6XWmre&1ngb$|1_%Z>SmKQZr?hljwi|dyv=BFuOw-0'
    'o9G1Z<!eid;P#)uLw2MMnZ`hFjdMVd0?c*p*|_9N*W^^|AQ#il^C(X;k@;<)Tc%Y%y&5*mVfFZ6(Zc4Qg@k60;L!{!A0$eX2wskW<sM1bC0>vk16z}S('
    '2R{_yFz5FlBVipUP)8YKfJ6t>208f-BEbtdwehtLcmVfllFtlc2QsVP6`|;dK(Qn0Q|H<eR3)5>L%kCLWtLyt`uI(2xmB5E8X&Fkc*;RhZg2E^KMzRVX'
    'cP>6h5s69?pDF)<pT5GVLUUU&F7H?maRTt3=Hw@{$U5${8|~-1iNOCOY8Pdo>hi+qi2cxODx^7F=rH?IU>{YUVQrrRqrS2|uHLLNCrE7L6ylJ`2>t-H4'
    'sOlqt7?(E(5OcO+_0RVOHM2YfQ+Y$W@TJK*zX)aeX@e9f9i;}k>hYfkzuXfGdF^JtiqLZ9EJaw<bzS8;+J>Kg>f*aV9}4D6N<@?og8*?QTiq^VALl{D#'
    'GV`kmde08ts;)XVN);<b+*&ZYYjK-Ev=SJ*X8!(OK3CT){`|aAO=4R!z`Tf~@1Ebk+!y3wzwFXTqjy=3F&IgK8XYkzEtU{fLe&K?6B6*gE$U;mVL7a6h'
    '$MQJ$DiNxF8QqyjC!fQ^+HTxl-VQT+8;nc-4v;+-PyPNnCnM9v{PJA0C`A>G75jF(#Zv<pLNJMkWyD4hYOZNnonn1OdlDj+P~wbDH`bhQUC>^xTg#(7s'
    'SIO<jiiSS7qEFs7GJy(BwjlWpsXWp4&PeQ_x!8n(^@}5&oeNk%0O3T+SOn0_-V1OMC-vuBj-fNx?$tHca|F%Q0Yw0scLUixe>v2&uinICoKORx2fMXfj'
    '9!w>{BuA1}G~H`e{+W6^e=~W6g3B{hB-qILA5`EI|!h@*~%4#Jkq<K|ackMxpBko0TWrwhmKYeOe-ekNGnidd6mUoHE_qhn~s+wgSa(xWWaBQe)C33dp'
    'GWx-|AVaop!!MhV)UiWq9p_LRVNIn_tdM^mWO@)qHpq~%~mPce}e8!0q4+8BN}73fVjLKig;As1B6Hi8;<CIo6m1~SodBZyt6iaQ4eT;vEAt@D16&SYq'
    'c*gyhGV&QRNkcAmN!;WB-z=IgpEBwV%jztBDCa72DGm4<AaEBBFj_lB&UCyM;i~~$Ms1x<CMY!FV<@b@ip}506s^ka9i<_{cNXt-0`688bQ4F>Y*pM~B'
    'TYxbR*al7GfUQJ_&9*0H6kFEvE!S~FV^nq~r64#_Z?H)t^qRh>*IAw7REZijZg(B~q92zCJ)*&A$wsyVJcEo&srzI^{+&E}ya;@)GvN`7W^2m)PC~L9Y'
    '=l{pm;`q;Mu*&;7GjOuT_=w7k3|mzKH#D;j>nL|?9h~AcrosqM=!7Z7Tw2y#=`oE4hQId#-#}>l}ClJQW^EL4k1_<3HRqdi1(I>L7J|)4d^G;PdWx`BC'
    'lS~5bK$tdENA<^NW+Ea8gRsDPRzoA{j?EP{1(agpz?-r3}8^z~wU<XLRX4g3&GCs@k&xj!iP8dh!!hbt0*>mJuQixO>`x={A8(L?sG=_wt6HY+f4{HEt'
    '>WQ^fsB5Fx(|bh@4%^)l*d1OPj=y&84wqIQ=#hy*4glZ&1`vwMD^&9H(}0w>a&r_p)$d<BbF!<O#L3T!t`U6e$F1($5l(Vq;?tA(sP7^Oj%={eG9oW&<'
    '!ps;Sbb4zAvy2(hKeDApN8I5kf3B*LzJbD@HNU4TkxRedC$6;cqI(M!S)Vf2>M~r>+x?GMNVE|k!D<C`$-|TO`Ub6X7i5>#B)*CdX5_bpV)wtUX1^ejx'
    'ow`5oA{}Uu(o^?QKcjMGe;X@ddm+~<j|S<i@rsUs*OWaSa5wDZJ{oRZ-2A%exiyDfvjS1z%G^i&jee+gN}m48;{XO!_7J5GIC=E!NQm9%qDS`LIL3oP{'
    'O#?*jtg`;gHAYC?;@B<b_Jd0AiX|HQV$;}9WUlI6#anlcW_G!w4zLWug<15hh06tz-^{=V)+&F*wqv_<2WSZZYm1OwFcX0;}W<&hoYg#o?KuA<J(2%_u'
    '@u7G_Ud(b9}lZhP!SUJ!<wd7O$LrCo2thm<e7mFJE)m!wuBKU1fm0jT`6=ZbTV3%~TUml?vxaxwl9`RVs|0f{TfvoR*d92CdOtw69yE!1BQYRSb)Z55o'
    'o)30(Ti5)JTSGA`165omg522CrP@s|FeQJWbwz2%CiYDA0Zf?+`NCVCsj387rOxn7>FAOuS;%^R-4hK7tr@vB0^8o9sL8u|eOU~k!^Vlsz=X5Z^o$ahs'
    'J%yVzP=JNvfk(=;)>JjH8pB&H_ozet>$ZZ?dSW7ZME(W*!4UhYce{iWHU?eyB+eVLP@%N|zcPv4*QEj-}O0$j5xPWg>Vjp<NEnEAdfSHl#wqprOJt2Y9'
    'tpoe5MY^F;PB?cpoLgy1=VLbhMDIK^1ZUVe@XzyU;nSQ)sqZWYPf6ed9$#Rvzl@by<4&p0HR?6rhA$}8<K7H}F#2H{<WBnBpL^*#61MgzS7}%N$2c1};'
    '99@8`1nJPRhty5ucA7E)k4rbw>ZDet6{e*$o5f~a5veSqaNy<!7AxJ;<R4Z2Q)fePX`U#Lw>7b@0ka`p;02YJxys7iM&u8ykUW;wCo8t$hgBj`iEx@K{'
    'bb6756=RQhA{7hL=cxNTTi`iE5AjWjm-oB+*K{-_eqo_MraBdVw09_Y9FzGdzIDv=uurqf&LQ-SU$VT-qp3$QVCh0WIz}Wyn4nE(7ofE;{Ee@ndSP12T'
    ';x%*AOIXMYW>&f1i?K2Y?Lv%F@x=6!1-w?Awa2Ohj*Z|uj7v_DO=qnmqHu3qlunXEmD49z;NZr!ewE7r?*+If_3Soue@?UlFKU$83BfKpb^DU|9CWNya'
    'r^E>T~i*zC+PF^F60Fx3oH!M28m+RIM-!tFfR(i|8!<j&%tLD&&g0avp^t+yb44Y+hbsjDH$MpmUA+&Q3vU$%<Jb6No;sY4v<nw9gsexN5&HB{Z+8T={'
    'M+)G`vHNV3f-|K?UkuyTp)e=MeTSme8i{^!8&&Yv!=cGty1N`?c2s1HEtm}<VAfeA<5rqWv~s#tZWjh~f_fQ>RihRUtQobLrReJODi?0gww1A|Gisxgb'
    ')BsomqSyG-bduf#_@B`#RD{*mBA>-f$f@(Gf!orG7fChF-44^=E;Xi?o7slO4ua3GwbNhFy8M&O17>`?kI_^Nuz4qJgc;Df1U2QVOPc_kb&FlW~F0$Kk'
    'u(KN`<jPW%D))=Kml3q{JWO5P|Kmw!9G{*wE-y9*kU!V(a77Tb)UEBue{|$#zB1bfQvyH3K+fLFF=qPN?0ke3sK<zbhD>zx9Re8#loG<q4{$08o1MwP{'
    '*l>xlPs8@;hQGZ;<iafK!x@z$1jFw?GNG^9dau~=8g(OmPqkMms6e3Q)ynj)4i=TW-Wl+>2Y^7e~FevlBTjGAhM@E2O+ej>%d(c$8*8;W+2&AB6l?fGI'
    'THWMBlLCr|K5_Ig%JeiprQ^FFI8`JVELJv36yp0K?@^EVFs-YS;v@RvT5($m7)|2;!Lx^_35ijF%N)bT54|a8i+?xl}3`M2rZasQN88u3&J%iaTcPv2-'
    'yWKN8kJ8<KjlaJC-JobT(-oNLq~}pPX*11lL*;CD+}%8i^I&VeT@CDM#_%3)FR($cqv$oh3%fuj1{=Y0a72S9-)jh=^P}N(*HurxcpLD-ZOjHnbCAw0Y'
    'vnZxH>rA_q$9azPkUj$L1F{hM|sxm*9OL@Y+~+qB$;xsQlq!e4z%o*(FD&H*#ieU)DX0j@oz{Gl$ITge3Ow!!<^kezsxF?274B=-FfLX3kj&UkYI&g9Q'
    '$avxXT#RF$zj2-s9Q6uO^*yg{xcey*}Yld5GI^i`FHcvOQlt4}HyHcfLj5H7Jd6FOB(P2`zbu#sqYWeX#@X?VOT?4YpRVM%+kqyROSVC=mnjA_iK|5k}'
    ';fY``>|s;{XnPeQDtmwc7!eCIzsfH)2@@)?;^t==!3l@tK2)%)d`!E#Pr^og9e-WoT;G4pl%4OySE5K;`)mtqc>Loc&|(X3_PO$p0ATJf?9Wv?_`{*Ep'
    'TF{ECfAm-~%D76q)3h52)II<%NO3-VS2}z)7W`XL4MrU`b39XUGc?WJ^CvtTsypyBvg+2@~xd|DJQc+T&4T!d--7Dplu*?QUXIYSY)4hx$&YNyz{d8aA'
    'HaFbhXSGTjaNSOtv5ClStD5tzby>~g#e|FD^$dGb=GWH@c2Re3U@~f&l*QxA9gI>*tbFz8*E~C%HrrNt^XLoBFH1J?OiCz_n;H&2ln)#Huob#OBb`m`o'
    'cR2D!w{#{LHhbWu8X|F_Kh6PG}}^L3ib3R!|qS`oFn$v6n3WjtIPTccj)BWjVI6urI(1tx`a~i)x!<boZ3*T!Ia4PYR1FtES~&Ym3OPbKk26Q9%`Qs3n'
    'ZcTCr(I-wEFX70*5iyaf1dr<>?$P!kTeNn#(0`oFx~mbHUgVwldUxZ?3+Jju2-)rB`j`23xsuqs*7Ijy8^?Z)tv1QY3{n@FGQw!djt)7unQ{Vo><}L_4'
    'JVcSEMzrvT(5M_4Z(zSr+dxq~YhB|kz~^CKmPGLMESEieyY$S8q>Z0Fmp7~gTkG8Uzwhagv&gf;l$o5v*y_9F#f+d=5`1XrDsHYgMeeBLmMcy?y5aCa6'
    'aPOOdDaI4e=K_&#tuyLaQh@d3|3v$cIb(COMMrKg<i_DKkxu!jX<|*|J842k?Q>WuYT=)CyW^-&KI183BwYzK=m);eDwA~+K;UpwU1ZW-a-l=HcuxPAG'
    'OrE`HRicpm^-U*h{q`Sv7-3>!&E?UZ`@Rfr;x)i8aJNn4I`@}qznDV<-ZBLn7qt&KVq5x}(7pnNuHuK*J?YoIJ6M-{`=lQlv<kwFRS-{i^bG?<llUk$E'
    'V{{uP7E&=9*wKjOWwL@oLOupTWe^{@&#=~Zxe!KA&&R(uR3HK7M;@cPD*htH-mX*2S9Chpwo{lkGe^@?D=h|gl)xyjI}43q$V#Uvu$9Mi!^i>X}sn}@;'
    '`>`ZX@xN4f9lKf_{;Pp0Xoo_&?bB$iYMUbc1qwzNWV)1VT^qv3XhZMfk+V5g}U&$U?^WJ;JA0dg1Csusvl$F<F0f=cSA!s6qFX$Is(Z{F=ZnY$pfc<4O'
    'S;FH)}Y2MorIPtL)I?&(RNw2Vx5fwMj<o(N;z$?Mg2diiano_w*rOV$~j30v3RB^HjBj?P?C{o@Z)N<q!P25FECUKvBkDCMaqBW+kmGZ~qzbIX~W5sk5'
    '}T&07-(aI2X+<uEcU{EYGOrvxiyjeC~<H?rg>w!mXx&@h63K6?P#Az*#3YJ^2B~Q&niHm!<L;pR6vZXA~#_n{_a_9x%!hD14sR_7gl<nnhUJqNIZn^R3'
    'E7J{5Kiyueswty(Mu;_oMpA=@4(M>546%k-Jx6QNd-O<dkj8jjPQFTrYx>P-60fq4>)$TYfOG6<A%Y^<Z7j$9NuL^zN4j)Ff*{g!=h23L@Jh67$D#<>q'
    '-Ue(*DRxGT4cFjGf(J<J(=bFw=4!6GWAjI)+CU^C3hNK({$*912skZ%pwI?9|GF}39CCWQth)Svsk22(D;}9;heGL;PHXBz;ep3fqFY``WkrJj*Fz;an'
    'YZm@ilCJVrWn}X6E;P88&FTC4K2MZcd{xk4CJ3QBHS41~+lPn}AqeYt;Cq4(n@g;#SFdeU^;|kk~q({=s1cHT{lEWA&kKJzPscr<)(Cw^TG{`!9H*SLs'
    'cgxgpcoOjs!;Bwd1<aN@<1q4F|SCy(_K{d5Ue!w#(s#M)zH;$_a`;(7WQJPk;J-hmmT6ylgSS_`puTwyzB?!$K!M}iGOjCL=7P{Fjc%)PS#4J}JI#)x4'
    '+D5?O{gu4q$S?$|}>EFi|;}<a;6Jcs?Gh*A#$c$15nP#_)&Z5~Zuk*^)i*i|L8I7|yu>4x5oOmlARG==*mvR$aQXIQ}IvBwTumjWWUwH2N@-0N8?ZGQf'
    '+Xh7g8`m(WK;@Z6!>Ln}#8+dNIVZ&6VzdrMP#NsVG}|r~LWZ6cbYc3F^FWMXY&vq0Lp!9LVaKMHL%IY4totlVA$=Dh5(P%2V1Wk2^!GVbEN5JITA{sG8'
    'TFGW8Fny6HTxjvz@||=Rm_N;OURjk?3d}cF1jHZu{$=U94K#OShwJmc4J1i78#>+&WzcOGcvE!qP`GmQ}7CY3=FVktbJmLkB*l2gs=h|*rJ$Fcixq(tG'
    '5;m8a4r(cY~~J#VXa;Co6KA<<U#J=j9N<22)%*eIAT!9nQVGDsD)Da*Odi3T9H%wjGRaIip;`v5Y3lB$`q<UZ7lOT4&|9LNsU_6<8>SX29%7yc%>nn<0'
    '-n$%z}xmbha9C2S{9Z0@5|ZWi~qu^M)}59eWYLTS?BZI=r&KO0ZTD%@aB-uH`^<N$_Lz^h&s-@q>;`jTgM%c0!grd03j3P^eacpVR?A27@5#5#+x>_P0'
    'G*AE_$>9hudP@$hk#bzw8On%TXv2MWHN0NPXN?zLjHqyfmJ=h3SVJkNvG(ytCJ~|aPk{)*G!M=>(;LoR-H3g3_$UIgzvE3C~HRPzJI5Uh3J>|&Z;;MUO'
    'HqjogZJ+r9Ns6faD$Y(yEre?wV!uKm7WM*nJ_dAt9SPZ?S?zb>dwXoolAOTubikt7V!z@fC){~-O4Y{xHkFmf(shXh*wFOW->AwzfB~zS@2X`_1st2LH'
    'tL93Vw4X)`Tz_*gb`aT*6hc<$<M{?q71i7#-GOTx2Z!t{&xF{eUxS-8Xd6c)@nstT;nDwk@Q?t>mmnZA+itAKR>T#SXMdnUf`75M`;2B%l<N>K}~gMlo'
    'M)N50Hx6LNeY(Tt5NAh_h=3=G8JlM+R)gSYMfWQf8{}a~p+~x{iXGv{E-Lnw6T7MR~S^6aqV*08R9Oi^3pSzc@Heb9qP!)T)dsAvr(l<*kZ3^a)4L%=u'
    '-f3b@!C4Cv53jq2^dNGU^*O+5)0ZL)jw`xy2Cj822V3cVupXpjgXPa5QHfasRP2`wfk+CeG=R?{&uKk9l`r#jWY8bcsu3Bmd(g1phvy5CJRmI%ttm1@V'
    'Of!a=pyYDYUCF<Bk9q%&BJW_3pYTRUmBo<7~$qmhEL%&0D8q^t=6;Y#aHOhG>@}9A<61F`Rrq+{fPhNuHfx(9g#F&WuoJ;NiO=Hy8XT^9n3(PF9*p>V;'
    'P*1*S4Y}@jB-J+<69O3PF_BjW`>?)Y$7PiwIN8W>-DLO0RPP-ODse|4PjMd&bBqkv-Pe1lbHV{h3WFyaY#}b`z*36krB8N&pA&2xplL!iT(>E%#Hf0cO'
    '0~W*L<9K~-5>=*QhyalUBxGr8~Hhi1oUCZR?BsGx+??=6fJqwYVOkzQe9Q~>D!+h>V&M~YuRze-qvgUlmr7-Qf%z7xa%+QECW>Wd0e5E4Nxvf;w#ErlU'
    'V;8Mer7n1$n=}wdcD3Kv!RqSBIk3XP>X1=?XuI8ieg)ehfv7@i9xt0hmVV?HcQm^vBi>dEfMSY{}16=#P~f1?N$b%4+mdwaPZZO3?GB`W|X2rzWEgmz6'
    'xo#((~_nN4noYc^;~J!d&YEKUAP4yLUc{=3s3<WsDzDcvL=$EN!zg8^Iwi=%`H%0sPPT~jAde!OlBIQ%{;gR(mOU{1S2{Uw;zJ^mZq=m#)5tp||LvsUQ'
    'H_>#%VofCD_&AmM-kHVBRzHNnRQVeWO4p`%MDuxHNF)YWaR5$`)2Asn)E~T}vc(N-IQq=JJ1~WhiY^;xdIhX{pBcj8$*|1I$g5~r+yE@l)(xsHGYXH0Y'
    'gW;6UQsWLSl80A%N*&OU8o9i?VpNqGAplv&&INCY2Wd@E@3a`@q=`JrlOnGxY$QGGj<Uz=ye9nd^!Y*KpVv|xkZDxkt#`N54!e!=oDAsgbxPn~jzQ(oS'
    'r=%o6zD6oCO=9&*kGo@Mm=nQ6ZSjh6d8+C!xkMHSG)L9MZN`G%;K`SMmFHwh?HEXzp2kKjl4wHqUwGFKK==Y?AE26LVQg1cL1hbuA#eJ!*$P0{2@c+jS'
    'tR(H#A6@f_mvzO`R$c0Ip*nX|HAWfH9gxBgS^jLQcq(3t4PDy^X@8%UP%FJH0-Nwx>PluIm=lg@XZb=%aBl#4$Gx8JSXls%^OzB1%c=Lum|mBr503uZe'
    'V01R;;cS>c=fI;+NwAmiri3uR1|(=j^F=#~T304|mtm{H2Wvex_h=2j}4HXaj|MmpT0_oKcDR%*Rp?IMdpw5?|SMUamKhL{6-LeFEqi7E=y6{1s3>gyh'
    'EuX=463n*bnxeUgu8X*j5sI|RISN4*dK}%X6SLTc>i(?au(&DN?x};)bc!UJiCexL`3-nd{XqeJC;{ZnEr1_G$B>~UudVNnbrB+6!h!%;Qb$~<elYO03'
    '))6BlqP(L^d=ev%#>+NO<Q5+j0ll-)eWHQCW%D`UBDD-sbRl=UG#5Iw_-4!A#Somy%YmN~q8)%KzlcsbNhqA~WTxa8Z(wK;aGF2?^()xY2_>u!gPjyf+'
    'i)xFr2_V}dg^<ygX|iI`j$&dy`*#&p+s-IELQ7>UbSV2RwoUx&{pm+Iw=siy-t%2vUUEIz6Nm8S{)yn%2=FozP)@Jl$sE~rS#yH=fSmqthg`5((t`M+q'
    '`&Z(E`Hy5g5nZn{3c@I}?Fya$-rFz9O~{{_q?8h)UlKOl5y!U0&DtDy}<;qKN@xBIfs;+FA~^KN%wEqydnZajZ{~k~MV!w^BkA*bkN_L3DSdVJk~RQy5'
    '4OU^FhodcHDF*aq_Hi!Kuc0noT*TeB8CEq{#?gHDNkS7Lxr7LmBnQEC}-be|Uq49D`x>;|~c8!+{E0m70fZ6B_M@QDr_4PmgHxC{qGZZ{!x3xQJ5T-q;'
    'yTSv-Qx{)8y7~Sl5Zt8LqL<RREBogO<)*(9Mc!X=6ktz3x%8#gph^@4eSMrcz?eKI_2f3&zmX0l;u1$TC)4O5OEm-VieZ@5+1uyfE$lARe9s&IGcz?#F'
    '+=eTV4H0G~W8}9zhpp^BH|(0?F7Cd@+zK>2Dm=-r)8I}s3S#v`p6|<x^)Ys(Kp`b+?4n*r>HR)=``c9cjb%Qey@{4lp3&d$fJL!|mS;m(OXB48<8S5c>'
    'WiGum`8=A%<=Kd9gK3~gXp`IPKSyDJ!cIfBUt3aS?Y;xEXYtd`ygNXm(f`^y}Zuoam#xVxE8g;*Gbj}znship#vLf@DkASRFu|QLcv%XwJjdqcMh^U7*'
    'N?vl*58~bdu*=pg84p%LN-{G)zOSEYrx3f~@fs34?G@wbn&2`6E~uz=u<SDHWY$zRvH?V$!NDISMv72n0NM*`kuhv-AoBVcF(UoaE_jSTw5^dg(@`8B@'
    '@tyrgenjX{MW<{#9VN2Obrl|}Haz4wg_yYDV#J=7pGxusERssBrxEhzgk<VT~J9Ggec+`#=Pz64*~&s|l|jF!?8MY!kGKS{6H!RXc~t{1W*aCa)s7MiO'
    'd`u+FN)b;87<2f(lQVbL|zAEK@TjNoAQIa18*+jP6Q@!FMY#hWo)S-u(jMRHvV(5$%=y4f&!KD%b*8`XoJy=H-Y(K_WpAx_wQ)=!sB5+fXkx~dNvN4xY'
    'dy(Qb%<61l6tUIvtdnQoOO3%!R40pP5V3rE8YqQ|3_Jai8UpX5bV=AUprO_Qi&4!!#GdZw?9M)J{0Synr1I&JM~M-&fi&y|=*#FVO1<(r(5>3cbttJ#C'
    'S{z`UuDOlQyXOYP?bmRq`+lnfmZ8D=Q?2>g-LrXsxK2#1F3U<X@#2LWO`mr;o8A~YPtyoGE`do;*doBev4{fHQNcv^~oc{Iu~-NK@X+E*^od~f+k`A*+'
    ';RNkvrIs9I#v(5^6Nqt8x2NU_@vv3m6R{+F%?b;?vc+<09qMOQrg>HGfj1iyF&};1gA(1v4<gX@{uPevlIA_E9^*Lhdg^9cn}&9by~3?CG(%F%b@bj;D'
    '}c=Kbx+f>H=6Q+FG^Y(<Gb)<q8+k>rUO>X$v<56CWU{`xGL-d90zooGNm<e04w1ccUreVlc$15<tzB;_pbqhUsmr~PeC0~<^Dw+%aw+DWjn1c%kIn>m&'
    'HXuQZ!$DcJP5gAfNp^1i`J39wxq}a;7dRSLtr0&4xA|ozsWnhdazkWV$jdXye69G^t(aWPW;BHNg8}L-@)yHkSxb1alo_?GQi`7g`hR}kIPvi9an~iP}'
    'KoCw;`zTLMsmQPOnv1FYXzbyyn&!P2b={`x4sIuGx_R_FCGia=x&s(c>AcuhYajJfjw#wcTKUl+gQikPd0D-mxEJGMDVM*3{WNrXwqIdT3PCdSWgfMv8'
    ')X}<Pc+{q@ZxUTp$nIke3(04)0L)O%Xzx>@hNb>d>xF7Sa!gz&nAT?vO`;^8CkQ^#UswhZ>3L@9o6jTJ+<vL<Wau3a~sk`@qk6+DNxOmTH|KvKL4CVAN'
    'JqGQuo{z<u{{p>VuF3xQ#Zt&A)MDasXpgbDx9VlFneWHOTlP<^vR+%HY}sL7MNX{_w>p<^haR1DEYRVvBa*onuuSx1itX<YN_;E8ZEUY6mb%MG5jTmg`'
    '*amrXea<uR5@il3<8u~lgOnmwuV4ZHJA5=CvEZ@9kymMWHi0i0G=TFsvS1+4~dTTKsMn(+*VwZKWJgW5;q#nN2-#1H~;w7}NEF_ZZBq5BP(W|?yGa*$*'
    'pVvEV-l?<%|8XfXpjGI-|Rig|#>x106lMl<Vo0M%|j9bCc_REIsAR$nbr$VKFOvkvV(KyMtv+@cyKN?RBuPfLowETTadMQk>fiWswC)3k)GILra?k`iR'
    'y6r}?*U?+t9}iGleIVP`3{*90x<*YuDSM2Ub2J;5NZ<V^86YTSKp~Tc8qICqy0YKjrJZ;$@SAO;jktB4#T3KGG+G88;uhGNV7aqaZCyiEixFD~jhAD<w'
    'kG)Pehr-y^*Y)(afnH9Pv>2Ipw5j;qTtEod4~(!!~Dn%3~7P4b0<3r^F|EV)@cqsEkTJfdLNy2yvuJhI@r~%?+LaODx=?mUENP#!<KZLpmo%EO}8@`jc'
    '&V|VyK>PC$TCIcL7|@R3}gOSEqvpYIl7%ka(Mbqql*rjYI8uM(C8ncsbDMO6`!I@AS9yItM5^%>fkHaraS}6CZWuP^JkAxH<$}!wz#=u<ua3PDdchKCc'
    't%wBF#v-N6LybVoVO)2$cQJQuiDPteGlzFQ;meu{^lkyxFYRQJ)}vwf*1iMHJlL<7sQh@-XsF|}iBI=ezZSBNHu&Pyv4L67>}Ex1PQS{V*?*eLN8B&zx'
    'Jmost5CCr9QlkVV48CLsj;n!F%UF2z`km=qlg~<dQ5{~h8g;-yVC?D~>kSsi$xqU~XI@kHNL1Gj^r*4hmvM@9;00fJX0lYs_R{#FSIs?<$d%spiu`I+*'
    '#<VB#QeW!9RCABmQy9d1Ucoci2@9xUcV;`~GMnp!Y}^*>@)^-S-?B0;?abS>DUl??>S~ag;6`pxbcQkqhb7o#P9Bm#lF!D*{Xe#2WH?9AyE0U)948qvm'
    'A8VTk-f1YAgxBHT@-rv<eiZjo&FHWk<?JPmV4FI6DT98`OzqY28k}>mUF3}YhBbj&!!Z4%%kx}Zpo9x<qeC*9awqX(a5;NiDUKUJJb!04mnu)?mg5gp;'
    '7{vEsZ#=XJY$24w+%GPG=y>p_;Hd*ilh>22?BzSQy~hhmr%Vk^nn*d!bGceltXtUxrH5u8Z2wp_tsEXi*0lb!H8|))qvEyl8q#U#3%@>=PM+Qiiy$ZxC'
    'FAJc@HTbq;7W4nZt+hZ1sF&*f7>dK(laY$pfbzv!N&hD8Auox#Lc9`Vzx?wJfu8;6f|6SQx|ztt%4_iju^Vs)CH7B-9eg8usQ0DpWyXM&5?Sv3Bbp8O!'
    'xw?WhCPnDi=6yuvX^i5Nlp=dVNokO_k)RfUSsl~^KgdcCh-awo2a|!Zm&Tne48_RQFp7pX6c;d>Ea`bGfjL4rvI3kyJN2Zj5#n8<@$}=3}U0)T^n)IxE'
    '^^`}^Ou2g6p^$32!3Z>mR48JN!l$&slFJ{<U(Z)>!DV%;7mRppEu&M~F70mvHEbjqcJtu+B5Xt*Y?QMK@@QkDdc(pOC|-#gd;1bLtCniNCUwqWuqHB5j'
    'Pbc&Q*g1;!r-}KOGAb`>H1Hy-ghvd18&9OSRS3!5@82pbV@@;=rBerrKvj+RlZvPvxCv>p&+xx@1t=-)5rsqJFr=XeJu8SHD!4RLn`3ed`j~TAXP`aa7'
    '^9N=%&1rq5sW(`=&JqbxMAEH1rfDpYUjoerG63HHjxe+8TB|odBC^LO;RS)K8ABcNocxxR%_J=~M<$EWd`0*Jz%yFFt^-;yOKRWk)fO@+@UXtMn6bJ-A'
    '7`XCC!)`j+kQQ<0n$GFT@g=j&z4KV%NSA9ko;6EN<twpt;qcrTnd5u1UTuWN&2o6+E(-y0NMq|~6U2*%Fgki3oBb@kp|-xbi0BhPHS4U4UeNc{Zfpa3&'
    '!8N_1;u7|~^%<4Bl>!M*;{1p*2NSP4B86FCjpj@dSGfJjWcg-ldtkmd<4WqRLA^HI|s_;ca)fd8v8GCv3l3sGyTCL*fJ(WQ@*Da0OQ>E*xIr}>p-E>zJ'
    'dlm3JN=Dz`@z<P%<!xT6>vZnhTjM@gcE7wvs}9x@Zr%uDZOe1#!k&7`oz6cl8js*ORUIB0MgP`~Kj0#@3{AI&H!fN8eht$B_0<a9r`E`z{Tv@)dzpz!)'
    'IsQ}vX8>WiD{7UdqAU8bdatw@~CZbYkXhEEb7hWR?oxp%a|RRVoTj|f;j(RubTu6VNE|^QO<rx+i_i@ewMa_UfXk%RT6ZjO{Y6PtZ{$>DkVv0AS<eR%J'
    'zWkUc(`RL+ikL2cGev%e&)Z<;(|xuYMnGZ%fD9_q|o}@@YS}V86e7KOob55uI~h9(6-1;Z?Y`2fY@s6Kq>k(@B1NFo=N@4J~-%o^P%hmR15U8~d#z%!~'
    '_;9Zb;S1|uXCav=aVcPb2if5EYXajJa@>qMj)c#yhzoMHpkI9#kv`YYh$(7=uhlye`2@Wwtmr8=7%RJa$g+nmw7vF5r7X$}V{8lOD_lV=;vU9!!mh0Wa'
    'H8179wf<X9?NBO!WZb(+l0gH0pSzsc}qh8agjN)EX<D!etU4snED`$RnJH?rIEm!iZI^~#|=e!-@qHVx5Ujx-lJ|`il(=hI2FO7wO5#91}=+p4PUQ%iE'
    'qWB)D^fqfBMQGh)!;}!y0gO@2JxKgS`{)d2sQqQ2n{Hs)HK1~aoN|K7FLQd-p@$mGw7FIhTJy%-3&QCmkGiI658qQaD4OF2$(dl=D8MYQmz23+yfLIk('
    'UOpe`*_~4Xe<F66P-ga_VGo*0hz|EaBVtIQm{bX_Co)x{cWIf-MQvAt8qt(z5R1YrJopV3_vC@0OE&Nfq&ZJca;utiIM8XU|EdN;u3vj&2k+z@4~L~Q!'
    'ChD=ClOgU#9YYJM^b{)U_JieYr1)1d9XXqxHt)7!g3}zHlA!Z3H>4TV>Ir^VUW}G(@45`TGl{&%j7!b1sB%*Ng=G0}djfx8S@F9!4BVZVC~4O8`+LgaP'
    'Z2<ND)-B&>UG4%d81J!KL*mhp!W^Ri-oz#)Xo4XMWV*lu`I(w<jrkJU#fzv7RLh3l`oJ=2F6B9eDQP9gc_sGNL|dW7zyQ;H<Ew?Wj+K9G<?wv7T#ijrk'
    '4RtY=Gr>AXnMkj;)bxx@!h(Zg7JWA5F<=}jX0~U=@w{=g825y*gCvWTC8$B(-?wV|aerrvleoa=u8nENVW!V@Wy8U%VtK1~t8myzYq5}`-o0K;N8CV~-'
    'yDSX`YxbYyy9^Tov_kmP_f!kvTS_AvmPZG@4UE#VMA_GJYbG&YL^<paMGlM8Zd@v@<%gB!J1VV}iLwLZO3#r?Vk@_ZV66|!8H$V@aYdYgsZLK$E(+$tS'
    '&E8-hbK&(N7xXDNCSL!*Q{=mL~ydz%%gZKx#Rb#G#fpz#%Q|YfwiM$+vpsRz}4Ph7dNs{4Zn|uSuWTHMhV>ZbiLjG;!D@rG9>Ox7s+58qU&hYx{h9s46'
    'h=XNhdv5v-9*&lj&(XnU<!%bi=g0PNOk?t;vxB+P;JXXi6w}!=lFB<~-R)?ZfH_^@ML=G&di<ZWUDnk1cx9Iv&Y}y@R-ZdL%m_6ubdb&VCfCo4F7;Hg5'
    'P{a^p8JI^f{s!}S+mX44~cKQWRIAwUev^bfg$%D7bblq;R8Xln9kJ3eqEoQW>@-K$3kBevKLnevMW@_A|<ZF~`5tpi|r9+$7dERLmtp&2$AAt=9+UkLN'
    'ch4(GgnYlt-3p}TOdxt_w)K(6`XWmtqhT1Bl?tZ<Ub>eoFS?yqoC%j?N?N%7&94ueAW_va4k>93r$AZkCo=0t~TE5<7Eyw-Oa)KSO#|ABV`i4@p^Lg~D'
    '=`{YhA!m>cixPJyB|DG$ISzc#r(uUW(Yi+Mr90NqLEumrxOAYyDR@zVPFx-^hfaY%gu0r@K#=Q%^Jp`C{m$J?D0RZB35U?r?RFcLm(rF(?N@Yh$DVfM`'
    'MfB2Tg2$BFP|1^(Bwz8wx=F8Qu=r(3DNVkJ)<F&Nz6ne;`HNO<hXXg)~(ZAek~$*liJtqSd^-QojcS}hvna1rx=xIySS2FN@g(@zLu+|$Gn`1oFN0&c!'
    'uXak9S}}DFe!fp(rF5zN0d+=D+E|29!PBFJ9nF2Y{>FjeD>Y7O#ZuPv!bGY=7#sCG|ThooXEWMRPn4DazWQX)I)1ZJ-lpKLwQhbq@?g7tEsm<Wp*=AEQ'
    'DEnbmz4SqPGZK#8O4D1>o-uLSjA{169zgQhw2#ZoLRsZ{zd=+^DW??b{H7KIM&T+2-zDjdd&kW!#vj9X@ZSrgWe3@H_a8ePh8C66tQ(siQwkn>1Ef=bX'
    'N#<mOS>)b-W%)hq~d~`7a4!E@3ff=0|Iq}?GM#JTuH`>KX4v{^%V2$wu0yjG{ofN5FMP3YGB@bwLJ$J#PRQt5g8X2~enp2NXjo<mJ6>f4OhGl`kdmp76'
    'eTRRaRl7scSxof3*=|=@zR+%Mdz*^uBGK7GZ-wS|(G!T54p=m65_)wXh3eH#I+Xy_0WW#JS3(czRFdf^8@A-zIa~Z0UOdbYJQ0s@&I+<{M~Jr4K)rxpy'
    ')gcs(XQ6yMJfXKr%1QN{EE&2Mnj8>3_||W9qz6JFtrd;93c@5LaRO8mkNP1%-%bI$R-ZbAg>E5WRy~#a3(huS`i;*NWG9uCT6n1x&g<<0xT$Hkn?XLQ&'
    '8~r_u!m$emN-mMXL;q_#Uw7p~*5xsUMl2wL!R3FNo-PIcViC!^IKnVZ)_3V`VN=aSPiBaQc~Xi^`a=vl}=Xk>0uk(=2MNIn8Pz#JaR&r(DY}K=g~d9~N'
    'D5Lt>n;64{`tuJIdaEru8@JliJnk-!jhFg83V5h}Pyw<MD9fbz0-_h<#5S)+>j=K*X5xiJ3lVvC{yr1TGCaD+E{ONd;F!NO9`3HVbo=yoimTD?`VdWAd'
    'pbLAOlmi#is&7m#4m8NO^x6uZB=@$!Vfc^L2i~c&sZ~z86A)Jupx{t;QcjWuau4B#i!viW#y>Br(zCa{&z@nUOf9f|=;||vbe#i>74Yt%v++Nm_+qi$V'
    '6^W<?B}sSpEP@8QZVHA!6eFX><)mmAXP+ON^!?SOmZ2RNv4sS+;hlYzd50J&8szkGQ8J>|9w1dr%5Ds+oiD;EH`m2aUe1m#{(r=M4Yu?s%k7MwC7`17p'
    'IFCetdu2&oEZCbf7SOc>s{GW1d*T3=4F2Pk{|vOst#(+CR}fb>r0064(d2EIC=F>fa<Rv{poQUi<56*lEqQaO>i8oWAfWI7oT%|Ra`3jgEF^GU0p$hTO'
    'S6hw>4i|rLz^w{HhiG3@0$40uB;ATwQe+p>O&}<{&1C0U6z-Z2+k_4)S;X*9g3J8!R;@e^E7TDj&DYh+`Mgd78Bi|HV%?-bN5YwWrku9e4F<K`Z_!pSV'
    ';82VLF4qjP}_h2SIF5FX!V4`7P4ggYoXW!a;VDoAFB_bZs7#@$a9x`VDzVf3SSCYt=OjbVz_N2AFb=C4ZgTk7gnCM)R=u(59c#~YsEv0=B(U3v*nYV)n'
    'DaYf}+eVS3q-V1dX$LeMGOQNT89!&&i2;CGnGvjjVEx=In0!_l%`dc#Y__Oeog}y#m^&?Q*@z{BZue!og=Wp}@wtJWNn*nw1c}TfJv#N7|+R#U{4bA*0'
    '{s-H8e31Tfuu}{wwIQ3~Z{dYm45w9l(c<Z?+b)iVO+*O5{IsOu^k~rj5`D66SGYbqGs)i=sM?e07<d#?jTyMnWUKdqZ`Q_?<w||<Gi+)`hB_6>?D{m=#'
    'J$;4_-8-vz@pQdQRE~u#I8ERsEsv^#67bAc{N<SpM;TJU?3nzIK%y<J0}22a}D-Y>dp)NN$py84$G$_p7TSXYO;ab`AFf5!}V(uwp5{Cl{0U*FGC)z-K'
    ';-Rd|u6bLZ*2OGN-L{81I~`hOcRJEK`~-VUg{206eb5tH7-Cmk0s}N?i>rI#y3uG*0x+0*pqKJ(%mZ$vHvu9SKC8u*G32dDZs%vwqwjFyN!zd}!R>UYO'
    '$c*fLo~y@?=d44CV8cUF4n=XV}zj94n`3Oy`@70L68#VN(@hKeqdLQM~iXBlZV-OCPwZR`02N;FHbS$E@^qxbxj6RnC%zk`w7`7Za#6S;f6+*uCOX69m'
    'CQR-W<pbj<~IZ<xg0FiW+0%Y*)$(YSOvphxgXq0L5A6n>k5A>X>NDUg5-ei6gz37Ocy&kqYEgx}!(fM9D^jk7-;8h@>U~{>Sf7$V@ZQO8UFvz+8eH%+?'
    '&?*$A3(!z>G^ojFMmAaWh|!q<(YyZS(%X6;)jJT0+%9%-HaHy7Q6eWI0-?zFX6VLhIo;}arC-7A)xrv+8LUChe*wc3MIx;)=}IGX|7XZsFe_%3ov>&`z'
    'mvm{MDJG3wW(mV@*X%fuANc&rn$*v{!xjV<y^ljBc89l1K!?JOOD<40X%09tRXkvznWs&vc>@Fs*jybK_?1it)u6c*9=RkJI!$Ab_cO%Zhiw#AC75*Gr'
    '~?8mr-sZj9%N#{Xo3Ff7=iuZ!Ts)^5iGI!w}T(Fhto`&c6d(L~J&<K2^kSXO=UFrzF;s@Xax^K$8G+Esab2OPH^LitkY!pRni*|0s8xS9zV`S0``tSEa'
    'g_eYN8*zje@NETofnkfYsR;D3E5(U7qyt$p&P-4M3Dw)W1O)HcIuh|riJLTl|YDC5H89k^xui}z4_d5CFUb=PvY!9i-<3xIgy;{Utgy>f`DJNV*R-hy4'
    'lX;dHQb+}U~R#tfkQrqM!xZ#_k?E*I#!7}DmTr%MN1V&>TeVHsL0(ZT;(@UY71B?<l%EceP9YFi4P0v_@Qc+kS!`@djwe322vx*EmL-TTRy-(H^ag8bW'
    'ZI#$a++^eoLSE%HS5@E6(vwkh8+G|Y&20zwi)qRHEUN^~@(a1IqFH6SGnk;pEoSt%<gS+}c821VsN)weV}lyZ*U0uaSt3tpG=`gp>1!5xzS^7T=E(-KV'
    '1NSJc<!M7d%*+i)7QKojPe@_{@!{2NNJj`ry=Tr4d+6^r+-ENo<hw?=v~nBjo}^8Jkk#|z0%aT^n=y<frE<=w-at&EpFg;7~K%|@Z>@6y^OCM^4tz2l2'
    'H#x#Ovy+poEr2TnT%Xj@*I`_!SInVCU-sT_hWc>u}xeULU47;RiC!*_pqKqVQK<dJfz=;nUX^TiEoKzI*r5bAZv*2ZXjjrv=W}WwI0H&B`K@o$OtVqJ$'
    'C`U*w8sFjj|q6c-OdXU$05PkC4?D5JwKat*o<E%w+cPo=z6r!4`B6EJT%=t0Xom~jT#;mWIiNeNO^k{ac&#-Zq==2l@b+Q7i+6DjhHgw}yfDf-J@_$3p'
    'M{{kF>s54-CExGOJU`rG|qPO5oWPUscr~+f13N=yM-#sYCA6sPFe1nJp9y!d}LwYnQ_rSy7p&Nwx{T;#v-dPJip4yon?t#_61A<%vnOEJKyHkFcQKCk<'
    'u{E#8)n}KMkG_7DW&E}``S>ND;YXN5+=GJrsG2TG{}O9WWJe}=i*5!BNyg>WTR;%4els}k<yhX&PdzmIqx^0Z3LxJIC`EmGH?||q&!7`;x{9F~A^@cUM'
    '~1imA-UP_ZB+XDyO52PdCaS{V%+x$3#nA6$-T6{0`xjk-U#buBpPEg8nkez&aHIPgA-LfY9W7Bn){MhqcjEf%)M$%1WwH~5j}^;B)BJB3Vq2_2nM1@)D'
    'e#a^3i#R+g9uhLtlU?e}~kCB^dfLLtxPez7rawlfD<dgCu=jrMst8<i4G=&XJ|(mF@+X*HjB;lty69Dyw+IQd)uRODX)Oba4z^&VZ!VZlC-Mrm`R*ovx'
    '=y_~3ozW&AYdX{uKgcJeqOmylNDy27afCMa?5C0!)p{w9ojleV;C(dhOrQOSKzV=lUdQ*Y>YM&fkB29RG!P?h1&0-cbM5;P4#$Da9n=Y@op2XF{m4#B6'
    '@aPM;T@P*XI;EO=z7ZSS-?myJihQsD^5o4MxP0x8ID=yv3nRBpxe+868X~6|FAv%Xhk=%sh?+Q%gx(o)=9ILBxX6g|%u1v}q7)CPpSo)vX7<6jGh%)JS'
    '25rn5((m6<I0GHQ(j(XxHVO>_zx2n!V+d*R3_Ii8g!cC`?M72In7=A<S&5ry%hox5{=A73()0xwD)(UJLn;rsFp3`{0w5%diQXAI>4&h&BMV2BkiU~U#'
    '1UAI>;DnW=yEH_!mLe%Rp;(K+%lz48r9LweZRt8k+FdC<!y#6wy@pxL}B_DO)#mU&|%z;TAK6nf>VorP^p(!@tT*Hf{&EOEs@{;%=7ltf=l`8CU+o+Tf'
    'm^=!8hK*K2<nI^pWT2QbNYcO~fz0#by(vXz8CZ7xM%~<EY{8)5xztGlAwsk0&F!)_K1VyRbq_<guzabn6V~^oeo880QnT%jjwa+Vvg$G4^Fe31x+XO5u'
    'B9e2DK1t67k9t&b%&{;|}7Zqerbm3g;5PC<ZQzdH~-uT&S6aX)Y&%BaJ2$|(i0_wtlftCF0v=Z9=X2Fgu@(Ix<TREs0y-VyyBr3neRPHBL?>`{xHzTze'
    'Cg(y#0K!<y18^PUcKzODopf`?)0y#ombvG2{@3mwnFh+G2<w_Jg0Dg~N<}q+q9->VW@)!6mzw6K0;rcYCSwtsH=B%r<4z~S@VGLpWA#8tLD{23sK?)|i'
    '^vUNN(=)5M$xJg)G=G<q*DF~?<Q{eU8@2H#FrWpF1Nq0^bl}(2hBX)M9<VRiQgCK^$x=n4(sgWo;uf^+9YHSyHGF5h)DZR_IqqBXyy}-^+)rRMmK@t$U'
    ';`Y?GXFM1u{v=B2q~vybqzhw35&*(pq%16RxjH=D2*e*5KY`{xi{w-3={5jsLcEM&-AasW^T2?|J}a)h;;-yH-1VlDt^x@fA<hN!0297j9*EfFW|JNb?'
    'B17mE(4g3qBAayAXJiy}|ffJKo>M4pcmp@<cPZegnMwu;M}BsjonWs5-5FCa`sOgNr-^YLj9Yu=^FMc~wRGbPiuT8y^NED=e?v0STn~gB`rVLUaP7)PF'
    '0qeALx71k&|!M&-TJFm6hMW7g_SZ~)O*_S+{g*%^@n(uYy0q|8LMiC;yl?_Rj6&EE~B4^}>FX=mD_B{|9pCg=p-Q1LgofqQkpuf3<uFw?QXSyp%3@)^J'
    'B7n5=>fIlWl$%Y-FH!$TVsds8lWKT1$QW==iwCAvw^p56{3~PLQYC~{CjUgBvNd#X7W|t`r`y|D2fYAWom07%UTjB2dh97TRB5+wt^6(!SbPi<GMf)cc'
    'I7=Q=AyYhY6F;)+8JaFrU|Un#Zo}Q1XYj;<%ve|{eQBD+v3kj+xLL#yBe>}O=1Dq_ZpvlEww%>N>7yS>A44y&_m1W_OvtkA-R^BlKFq!f@m9^llK}96M'
    'kDS_yf2B{?cpC><;47qBp68?Q1%7#s(-&+qdp7upjie@QVMvsZvO~q2s#KwNp&?YGKa49rADxk<S@X#3RheN_8+l@K*o4Sj`3VR{_F+aTo`4B#+6<rlJ'
    '>#9L-ifk>%an9+z7J#zbD_mtTs)uXrwj-a_l{?`c)odew<b5iEjfnU%+?rZsXQmj(#ZZWpVp*!1NE!yn2U__>r69+tY3+ZeXEizpmn<oS1Ke{v&yBb`O'
    'nUP4X(Ns%|*IfJ(hHv=MN+Ah)vrJa5L_B1LP!I-bzUe1fLj2FvDU=dm_^MLST%VwGycvT<Rdw&V?O>qme?&{5!XJ60DBxT;``4mTP3?w>ia@qGrgiUjn'
    'aSqAOy4VC%3YwMMS8pl^r){wNiDpi!9rdJT8k%C+XC~$LaP5sxMjN$W5sP00XZXlxUGZ8mkbA<t0hnrD{5EyaB9x^ba+=G>`{VdGFZh0vJU?TDf+p**$'
    'xTk#&CtMmwX8*2OHn^{%=6BdsC`4k20f6kJAHph&2)T_3e{CY{ef;*C)tyabOv!kI9?mm!x~%T19|BRCl^Jh=+p(apI+XWd4;&ia9U8ObuwuPC!)@)`u'
    'dHts4?egn<%g*(W+?R}l9C&g=8>PXy%@kbq8N<h2<)D_WTEv)F18?6ZAx2s$NkN)qY}3JD#LDJyB?duJ@;3A-8jH#t{b6klx=}`UEm*2!8#yJZNMHk13'
    'AOCXJD)oe#7+L+IGC|XP&1sfJ!eu$edOCDw*pHp4-k<6Ss5Kr2qO4<@?{g<4`E8v^(ez=h{~>To-0+-c1a|ls`Dc3Es_tOH-kj>9b|WAX8f{s&xqM0Lv'
    '@jXJ{SZepO9mXeq$Pz@r>@54WemKeIAPM@gsb;vn++x?a<AlDFGUD$r&w9oa~8Y>sXec@Jj030Nt6br*A#2ae@cF}l=`x&Pf?kII2#6&A~&-F1E`mG$_'
    '~KhgOal8^e5&A87}mnyF_0lvGz`2H*{cA(K1_KXdO$m!v|;|}nkJ&`i0H;H<KaBXO2xik4`sMC~DKWARWWsSe_Mnh-Zla>SIyl71K35x=EqNlZDl&=s)'
    'n>EI~V9_W_0cMA=c^z!0>l!Fu4I|iKmcnKw?A>H`mAkHzS)knYX-eg0u~e`yGyBEd83h#R<PLJPby;0fepH^N)bVEYQUe;pb@YWcmu0)mMaKvc-S&SI4'
    'x-9OtlI>L=De)>Wg4Hc=rlfeWsA7G?AtFLpcRc0xfk1Ge(9m#WS}TdQz}ZX)#<~A-P7mGaH$1$0rMyJXYSp7Uz-9Wc?Yn^L#@06(%eab0E3O-xBnR!JJ'
    'n)lgrwe#6pAdXaY@X*JdBm7NsJ#AVr#!h_qBZ78rXd8JEpNTOl%Ap7XTp6k^GG<@K9gEb?ADQLk8_!(IM<83`D7`aBCon`RnO)KvCREe}Q#i-@D%ZHN5'
    '*P!6EFXS*?Mc+795N<AnZ&P0`Vo&nY{Ajvp!Q0EeekzfAd;a@KP+fRz3p#^8h`a6zUzsBmXN6Jow)H7Hu#T!@@MaY~C@ezdAGvY<qH4>y@vo>Yq!FrdW'
    'E@|%l)xJmupjmP$2xquB$cyqC2CNC!<Ti$e~{2Pg9MDr>%&FlN47tJ>MRU7Y=ix)5|4#gtjfx5?g`K1Tv)bV5MV5iXyUW5(ZIe{I9ZW|5T^sny+-S1l('
    'Ahop|^cUt;;6>F01q^G2Z@>7tT4l#xLbw%v^wtv@A54Jl{pA^{)e24b&Z3-df`k9%G7Mt2i<RRL?dBvS&M}{;%sv&%f`70(^Y10tR#FSF`CzB*9#;EmV'
    'zydQcvYbo9ctKpG?GxGG)(23>WkLd?VQ?-P6V`QpSMt)pj{RN<?l+JwRx4Vt5v57A5^Wi`Kv*Q6d$XX9V`1))<xJ1j|Kbnwm#WH&;*P3ucB1YpI7N?45'
    '3h+N_gg1>d!^K=Q31iinI3s+7Cz7RbF9p>eEOmeG<%lU0pz>aQdZnpsi3vbF8kh?+0w8Nw7Ob{Vx<T--gX2%Sfh|BK`>#b8rk6_fI}o*8Y(xx*}uc`$<'
    'DH)2}#u;U-w0rm~K>#{|1EoVv<4#*((xKex`)1I(o#d3?7|TbNh<D%!9U7}5ccvPDFpTVi_6KI`d4>sR9lv{^8pqaq19&|n4Lq+=Ce!7cj)MW@->Qxsw'
    'QFNQkBWC-s2C_3C;#hSE#7eOcr%d6idOv1##=m|Ch?yQNyBHG8y2`6N_*E5s8OPZdVZOy$YKnOJBNX^K+PV><YH}O3jrmI5mZ*$qB4c3u20na1t@PR&|'
    'Rn2TZzB@3K$k!?4SzcJiV%4%A!M43{{3{o|d#I(6%>!Y-g<3VSIe&F<J(Wgwy;;aqL8-j2A9mj_{F$`XJg>c0srV0b?+A99dlE$0d9}6l_k3No@0A~?b'
    'JD3Dd*1^0QWM|o+h|6jmG&TVM5Us6i5o*Da-@{CA1X<A2>d&@(Ts(a(&h_k<5eb-{~3!6<Pzgaw9k{06_<Vs03*p?vg6=Ca^Rzl;LuV0)So>38CX+4b_'
    'iOu?9?U^-aL;o6i}lU&wgsF4PMrHT|T3Jb)fUui!HthTdC(r!&f!-EX+G07it5rL)cbm!_KSz9yi-F)wY^0?EF<0rY-ENIr>Fr$ti@anyzpF6a89h`wp'
    'cZ>?7CcgC&06CsrSgI?*TLl8^zT(&|SGXv0omG&wITGvxEZZEUZzM~;H^uzw=*1huzHe*6QZ7*qctioWz1ACR3Lz%|sB{9$D8hhsAG(2eY#!!afXJfiA'
    'E<ery3oS<pqF;?zXIab#Y5L7In(;N6s?KQ9gzXrcaNe#5#v7_`~4^wg)2QZ!Nv#Tg>f_pLcH~RNaV06GmzQVlfzT(2KzqLw=LOy7(C3Sagd!JMubN=%*'
    'NB7aIbsq;8)_oP0kk7K7M!W(Gvglz^?_|ZIg6&FqxC9$;ofqudLiQ1Ca9^);3)?-n>*_c30Hb+Bx9>*TsmZHuN&YTB3w6?N@)ix~e)Y&r9&0V?-UN{WY'
    'ClAci$skoQstg7e?bGR0!_+<ksDs6UEBBfVqnziFVb0-VJPXi$YAHEX<<vem8Vr&Fd6mqIL%$!mE@f$Z~NucD-zRxPbB82In_<*z&#K+;N@(VeEUZ*4m'
    '~kXl)l$XA9y7FmD+AKBscq4$;c@#Cj?;La!<%8Q(Ug_K3ONj4mUD$Pl_-OETqzs2H7ueSX5^G^V`bO_AMLa7iureKz&EIGnSx)jk2)wAckx9-czE*lJ}'
    'GLfO^sUuk>lV2j81n{7PK+fTGd!{M~`ZaPR$mMbCZPVL23bkc&k9KHQ8i<|yF5Xg&c^Fg&I7(wCNQ&3WqOw!6BzfHAHo&ynbJkCLS4AW{sy;B_5fbeax'
    'sq%+t(iC($7WhkH&w;R2m;1}^{RV>OE6#PiF(}kPY!S-t9&Ph{}bfh~W^k@1Yq-ei)A-d=oCTV&`1}eLf?ob0w=j&=H{1;Ar0C*+={kxHAuI$6#VW>Xn'
    '*ZJuAX<GH+uRgYp;KQ2S?$4~7vAzt?Aq=Y`ars$VS;vvji1BOY*vqnx+G;Le(!PDzEDSq;N7hZFjVx=HNgVkvV{sl8aIuobu_BWi7kJNkniT_|PgpcBB'
    ')7?i1~+t-<QupB2Z70%K#ma2hvDvl-@>IySS5NGoIyWBQQo{)WGJ9S9Y${_#8q!+Mxxa64VD;|%d322*yEEB%Yj9y<%0$;Ug?nk{IVlyqmv7vxJ^5+DV'
    '5KlP>)|aH}mk4csbF5B3t%kjgJ=0r=sHT`HGB-bz2}9y`==#bNuKZfi*U|M407mG~szmU4F9aR|su~X8SAH-nH(XXm02Z+LmD33)q)do&I@BlZerKB48'
    'UPnD`%oU2Ng(8Ma&a4&Ml|MvkXHuzpHO-~mk&&B#ElVD$5>Fmqw_Hy6P=@PN^5E`m4yIe*g$nbGYcehu_(e~fPB@he_Jewfv%5P+P&%d5*!r-wNu>Ont'
    'U*kh(<&2bxA4|aqa2i2GR>aC`oNA2kc7~Kn+rnVR%l$2Mm3SRBMH0RfoOR*#fF3hQ;JD_P|N&C1Jh@zd@Ry%B2FO?S^U^J#B?#5I)s$!{`z6#|b8Al<t'
    '9y{mJg(+&3p&3=YAj*vXbsLNBQBg_gpTEjq?`4acdjqTH6S1D4=yV29E&+@}31EBgHV}^zht02~3HCC!!{bJh2mHG3{cfSd)zJFJn``{}Bb-vKejro+5'
    'CivkUj@4;{+qEFrSQM0Fg~!uZg%Zg2>(SM%@Y`<yd~g#%niXsfwG&p2l1M`>eu)*Q6I30J6)NK8jZwkFgwZ=TTZtt&_YZyPhz;F2uQ_WF45O{4j2P_Kj'
    'Z%TBe3BnQ1*di35{4c-h@Fw_9?e#EgSMve47l-DxCt9BWLM##buGRSQ0JQ!cVy9i-D{~-K|0%aUcD;d#7Sic=F}DEKA$=TGv`n5-gL&7`F>|2;RGH{O|'
    'PkYLuox^GfN<J%q)?^W`qA3DImDKo?DXY=3@*H^lFUIv=l_@$EnG6v=LPD#Y|(_xuegz?5$Q&}a`0o#>0se1W1wjYbT2q*X=FLkcx<a65?;2c9GM1j{L'
    'RK*F4>x*Auou9atz5;T9EMh)m(pL$ILmgZa0b{~l$&E}lop;cr$<B2TBDa)$6=--@Y^fMNPXIW+f#eVpv%-;EJE_lTwu2>ZDnlhV4xH+Yn9<Emb{Nttp'
    '7|#&N^JeJHjK;@;OmmOQ!&7j)i8Vow0!4`$r7`tkXIggGjCY%wpyObHaQi@>Vkt8)R_-B+onin&x&G*C^1!=+*3Ff1V>`p@3pWZ332#{Vj6@5X<#!eLS'
    'k0m&pnB-Z@zryqPb+KgU0>g!x~*U|HXYk>TP^S)^>n<T-1%`zfrHG3l=Z~mil)MX#416fSO8R3-NjWK|3%^xUwoW#(=N|emFSm&C`R4|-J7TMNc5y@_f'
    '21QBV!0oBK&;i1x=50T4_hI?k%r&8Vui<DO14&C2$<Rp@@rUoz>G<@>iwfXZKh7QKX+rAcx51pI~u@qND00IxzOdo>$%VED-wVPa`P+&Ld6!<~3GeR=E'
    'cfu6f6*kSd#<;68!5yxHaNhOZa91%9&t`zi47b&<RT?q#hBOQHcM)G9S;`0U%u#iD-H`Y=+W_Cl|9D8qU#I}6fnl(wr-{)DGd$BN1;H^9QY>rkfWbd<b'
    'Jr#d*981(jWFyn;Gs5D-zw5_Wv(q#2vO3Ro>pGXD<tu$QmTtQ<EGX)bJ5=V~{29KYh>C6o}fu=IA`h{;UIt@+bfy<0hhwG``o(UXmK71N&c`o&QTJ&56'
    'tF%J|BUdM{u9(hJAEzk)3Hy`|snofyIz?wZGnS9&-Q%i=`e@V$ALW_pdDUIdKEr?38smeFn?4z~leVB5pq{_VUxk0|78FtJP3HrBI5;`*^J~fp8tAlXk'
    'InQg>K>60##u}b#-FfgOz8ey1X+W^s;FcIW0YzWhHvk(fKz=O>T!n=ZiKbCwXbQE53RgPOM7#wxJZqEq%W}-GkYQFRG)^*QwpqnyyjJV5lO&*fviCrHY'
    'd$HHZbL^&CfRaU5NfBmekpuqSYrZ=7C0MA0f&&S`<3zUVC35DlfHzuvuN@b+~nrv*W9S^9WUZMQM&Mguu=jH2mS8dFIn3WU9l7b`MUvn^RlWA~zY^qYo'
    '+sm~cbPxV&-;DD-z9tDXMt`DNxOD4OdywK=O6wu1L(Gnd`~#p5$R9$i#{fl~BQjz>}H{=N*1kdM(ha6UTYVM<_x$|0&T`6L(oAG^54e)Z(lC1sI+{c4n'
    '9W3Tz|rO+$2lf%dI_OgRRgM;e3e-uI*K<+oYX(;*$kr7gI1chUajhFWT*A1kbp)rKkvrC^gP6|t9XioW4<T;WgZ6jPq*XDUU)K0Y=BM(AL)ByRnw@LpZ'
    '!~j6s$?H;VwqIv+_BT~pl#sDj{y~@tP*?p8Z}ab+$0snPI=wK;mqRObq`eYM&QMtQ)@Bk-$yJ~U(}Hh^yT|C+7q@NMFI#cbR}t)cMM4#e%1Z7NR?I+4t'
    'mNa&rVOfJSc&_4GQ_WT8@I?K$WJ3RZ1F~7lQVxw6NvwaytTn?hLKDRaoQeq7ZNMaXUwbJDBsJ`8`~D;Kftd_c7nCYn^*6n-_E7TQ|`9+b|DQttgDN1g;'
    'qJ{aQ|xR0h5XmY^ndh@(DP&?Jk96%yum&PcwXi0~x7oy-3`6+Qlm4qho8VUv)#+eo4LANlF?YEDsbiGH`o$F2<vGv}r>b3fzx;0`%7eNOLM!tdblv9D{'
    '$PORH0rn@#fwGMI1&H%YjmSCsfpQ2xN8xhA=m_@KodB%-g`O5(aT+LExN&}&&&<05Z2Kh5i4gZ-K;(7*<9qQ~sQ)e{(<l7N@Pct6u@-l}h|NJuB`#!|M'
    'R#aWW`HM^OdGX!nr9F5y4WN}RyP=?}^sG}SpbuESL%kLvbWq{Pl41wX8OtEBsbj6~weqQd;&a3V^NRi4Vodt44fvh!_o)=7dWt(<5hByqi;O{kJ*~S;V'
    'Bl-L^Eo_$c-0$g(sA!uE%;-D;`-tdgXuVg?N<F?R8uL6W0oTc%mF9df%p=3b>(~YT^Ohla*gtVo{Rwu`xBX(HJ;3)EtzXd_f5S=q`-ImM7|oS)AH`tg_'
    'Z6a4y77k2K-93<)~~uD?08858whdJ81KLs0zYpLR<{Rqa<!7b?Z!NVjaFHt#Jg3M9GQK!Qylir*~%>`$Ov}M@b%wNE`IQxeAl0A1r8{h_jBg~@6S)Gt9'
    '+4XpH;GXVA0`jh%Or3XnWH;-*Ftd4Xcf`WaOp)&}_e~+-f)d2iPxcH0z}8ln%?m`3+8%c3jf$86*O7QM-BcRyO=7FTa%;i|(x)-HS5$yS+X(nN0N7dNu'
    '~#nU(hM2N3ITNue|;N-T?-j>*kmbnM8Y-Txcx^C3+qWEvI9gv%)Y6n&19m$O}=<%b#l3SyIa=DLJy76|vxB74~GO5U06$H3=dq)^(;bvs~TesEod8&y='
    'U7yVbh#0M6g=AikiYiuQNzMPA9OBoI@qyr8kl}EasPMWH}EZFkl$j@lk&cKvvkAqV8y!t=9wrG_t^FIh1{K*QN{|CR)_@@%9f&9bQ&A02IZBAMI{F8`0'
    'r(C=9!*#s|`MWsWMp$(B`w{6-`cW7JmbJ&+n<4PTJ<eEEN)yX9v2W>PNl#tTzNQHo6fkmNq*Hak-^plB&@_fWmVsq`>BoH(MCjfH&voMzL$DNS+l&j{U'
    'eJ3~(+4mURhl2$8Di}wP>OoY&N%f6$pr-hE7$zy)v!qKV~OtRU=J8xcE;+CyZ_U~ZJf0T>;s$4TbpY6W7qEyHk0eFrI$FZ^24m(4UKP#z=s--R*rAS_L'
    'eW)R}2w&+8kvvj@eErIf-o;W+;94b*F7NA%@Rpk9|IYQDnFiJuiq}ep$r@bJ<T&6iSo7D}^g7S@xZwet);`6%6Tsqi8A%a9UL6Q^06!Wu~I;4;ngFFNY'
    '>inWA{YqLjB2lYVJc?CF2G_qHHYHDZ<TI(Zk|o0jy-0sAY_k<x(7D^gb16m^t`869dQ(U1J4sD94{E9Bu(2bw^P6_?epV#?_Wi^h;c+cWWn8+P~qns=X'
    'qk7;bicJJ8!%=4O#f=hGu7m#MS1zPlS?oZQc42;@r%j$KGn8W6E+5AcFToeB18+UoOQufUyrP+i`K>F1y9D`*B$K*{aQkl>!T#$L?CahRz4H7f<8wO`b'
    '0!sBs-KZdfPOEW^ZO;!gdelN~Bw&fwE(t6ZESl=b#D?r;uMt}V^ro;g6sJSoj8AHX4Lu9f1|uzUOjFvUiPo7<)z}j=nUR5V6N7{GShY*I(dy$gCoAr0Z'
    'IprguY2FS9n2Mt4mp94JBvF-qL@4D`!a$VS1EflEPJFUW6+yBfpo3x%xu^gPn$#2r=q*B>$*Kul;-9hrdN(1&r%#j`KwkaHmIvhJl_+R%bRKbE(@uqx@'
    'sR5rRZMI`ZxuhRsC*|1-@6d((56k6BMPqgTUwzKdi@NLvH^OG8z{GlEYwTOdLKz1E<{v$Z8XM9oVg%AvmRK0zS4gr%mWy*&uY0*HynvgF<=>m5TqJQft'
    'p8bDiecL?3L(<8R$2{7I7ko(D9NJXoQq_E?1_fu#I2(iGLkV>(b2O%8U4vwz!o`_F>MR0AK=fBE?o*%L6G0fHd(W#vuaHFv_MKD=2U@6ZNB_V~D$xdIE'
    'iA0nd^e+cXq^1qSiIDyd#Uu=qtg)LiA5%0H)h_z%AS9ZYXm3<YMR`_mYf0~+!_b#s$i_+$x)WA|#?K;@1o|XNp4GT>Jds{`b3^o}}Fe&cDq+TeUiF?zo'
    '?<vVotCqJI97Fz$j|-nGFi`ISB_AiR;*xtvewNXJ1`>L1>gp1*y!te&gdIn)k+0b)+`sLv3f3V81Q(yysa90V4T$5y<H0s+7BI3@D5yj&al7VK*J}WI$'
    'GT{#UF%n)3>&_@p?A-JELGGR-yP)^kh;pZs+s?M<-pPbqJ}+E?(T4a*VM%6wL5$T0b1BWGAie^3^0bcojx8t_&(p(ZHN6prnzd9JYC|nI#%}WcCk6@z!'
    'ZUuJ&n|Upg`0Xzl`_bgnGhbbqO6|g~A&B*{Nkk2Y}iphF8@-V=;Qz-El~Hb+3&lbt3OsREfIxYe`KYj8q-sqGsEQ1$6?)K`<<@##P#l+VI!G&`L7u4ny'
    'k;lu0KnI(yj2^9#3aS(oLWCQRkkHZYsdIKjXBia&(6vg8k@%l-?RX*c^kc;|D;eobBdH26Kq9P~Lj`@e&2>)(ENA*9}4aKe5>ItK0pPW@OYEIY+Mue`1'
    'qiq|O(3@#Y9<t2aBHfAiShdp>(6|=Fni*o?yO&ZM<0?=bF@rq0Nx?&tjzCUig4cd`ObTj$WfaB*;fs9M}E0DR^!Y$6qM}jA<?zo}n>x8*#@2I>}j#A5C'
    'B{>zmuU-}@!@mIj^M~<NaDsm}?*}qXs5;|Bc^CVgZ(O*6{|Zm5jn1+C3?BYF5Bdq2au?$`BwrJA)4w#lBYI*o&c_H7CnQ#gpwz4=JDU4y=j3{UL84+YI'
    '@mx08GSsM5GPd9C}AfoJO#F5CTJR5ZHYquWVAERu6ui6IqTsBy6YD>FzVBE5ho}2k=0ecbdkYX_7KhXiH6Qad;GwLDx1nq-?j2C&OsolsSE)*H9&6`G~'
    'n6#xW7Bh2Q-}#6&aXghp6&R@4+egmk(U$Kt~7lZ{^`jl}!g2(CI$Qxe&Ks%-wZKJVY=6X_)MGO@ui<5VA5I;#$6&b`P;Pi}hRh_%E{GbRL2?)_dStzXY'
    'p@cVnSW?GGD_g)_9nW6(|!!c;?Pc>naqPpD))LDRgyet1*QyrFl<;Qmg)Z~~)|znABe#Di*;))*eIoKAA!5>xj6cCh<NbyLRT)vy6>!hYwrm7ivmu)XZ'
    'z?5lWv#%{;zCz?ia$`PB9>HHl(tTpg0r^Ksid#|rH@TC^9Zy^2N?dWfeY=4$&IMC>9A{aUIz4O@uFDdTGNUToWnc`S7<o7c>e;k=|!lJX|VC7r5-zhGx'
    'iJOTn8Sv~PvyaZtxW)Q*DDqJFR(5H886R***B=SrYMq!}yaT)5q}<->>#NUF8kSUfBcJ?D*WbIKqx#@SBVSs5>Gm4;fWW`tf+b?Ik&^%GW(58YWJYBf^'
    'LMShi%zTAYu;i8Lt5YiPoPhB_?F+LYwbJ19>}{BBzWnb>RVz$g}g=JzDErEN-@(-*iPtT&8ytwKKTjjIYhs`rfC!(8$0EOflkxXdVCcI*73Vd_tk)E#-'
    'g>4(&&S@(}aV>Iy1GSvLaD6eeAH#X8RZPd63zwWfBJ#jTvWoV8gx&Mq=Xn;v}fm*W0H*4{Z3W=vg_h;Q%S-|F~^r5mHdL^vOjF5Fq#&S0vUaz<Ez`%bU'
    'RNXxJf}vz;&np(4An3Hi+hyzZf&b+rBnbO<{LOqlWD61R$hF~pI;&}QpX#n6m<M@EK|Hle7X@SX!2lZp>4(wR*4&j;46Jv0<Tzc0aMty!7`^7*r2jzP_'
    '{(=;I(ZVQh)zXL59pjjWMi!Ft6M)iS*!A!^R+xO<?L<okEoelPR;3Kr<9kTuedA@dCU6g?e%JJIyt4@A%L)&@3fX2Ck(&G1Wxj|?tSVYScH@$J_HoL>o'
    'xH-RpSJobcmb$myLzLotXE!KX=ZrOhArXA}=Uz}~XRHl(5Qe%R?loULdSQcF^^;5BMbO}`yDc|^A1u0<|23QT%<y_LGN;_c_mtvx6ZZOMo*1cf1;YLrz'
    ')rcRAJ8-x-cW*tgKe02QA;t4nk2MA)GOTLfty#|lIvoA7ASRQkVQLI7e&uPA#Kxn2@QQ&13azbqS~^MVy>w43=l_3(hNO+F;JY5D3t^U(e2O{>%I$G8{'
    'H1WGH40(y@@6_Kx@yTG=IPhK`YoULPTqv@q`iBObH?6O{glFVo7oL4R|a^fmtw1<^)D#!S`tgq8Bh4BQV>7$F|X6ByAX2lwH~eE(SP!2mBs1ie@mndtO'
    '!qkb(Q_?zzq9vd(XAIv`n>a!@mTu8hnmrBCu@HSW>*tHdAwgVW!q?DOv|<rNqy_YlA08SM|T-#Nz7j!oR)Jbr3YW`sKqV4%__WVv}due$5n)p+`c`@@$'
    '1V1hC<#Wo7Lyzn;X)$?F1p?kVS|Bzkqc<vVbYsRYJQoY#ayK%S&zs5BySTuc8l^|b)ZF~8_cM_ZBab7ELD;`^gt)JJV*9sP=ly?vln|T$kM&;Rmi6DTs'
    'iz<2(!kI+niK>S$wyvS*0lr?yW)A?pK=B(n<qAb-WzdR|g0vbmts>6G`Y%QMbcpc@m%>>q-xaD?07~u7xO!!?l*S?(>r-;vGR|;<Dl$ddGqJYI>9(}$R'
    '82dZub+lqrvGdfe?rW%0MlGr0EYG_udd+}C=YW=)J~++6lW;B8|^NjNQpm)k>borZ{WH}(Yg?g1rxXbp677={m9M0tZolLA}MYeM8LOZHUGKYt70M5TM'
    'JI~`it$k>QLB@E(n{KnFzo<81JX3KRBJ!pFtWo+QOP)k@aa>jfo;zOr2@4grEI{MrY{}#OrQZ#bp9V&|Pq0rwbhz_wA@dy67pAoXbDR8~0?0#x>iyHV5'
    'x0y1T8Uf6PObXDQ!Gl)FR9s$F%IqSwWT({u|t1eI3pHK9=Q<ZIM2e0a+q>;RS1hz^Wk<Q>E{qe6P(8leUwQro6gxNd=>f0k06+5V4@%rml<1({OxAdgQ'
    'dtJj-2@vc&!e&FWbTZ?U#*wq$D-h$Umv52QVSYn!yQY+9@taq@}hl(C$UiC{XW`35{$v#yNTM;z+Yl`Yw$7+5WDPgl%tCCmoj=_0w>CRA$QbqDeJPCFh'
    'n!?wXp2?tb$iYRk-CC#E`rh=sAJL;Zrlj;dn$PO{t#qGfIVEV{Pj{Eq(7WwTKcLFPoFdy@zA6OeKdW{H=h_*}Re-DtwCg;6Ru^!teIBM2ckBn=^J=Q0c'
    'MAgVA1px)8+tRh3T*0;r_66zIAgiG(G_I;HH)^z&DS%yCu%o*ziLC^%?!N;ctdRc-Z<qC7^UUTxFtnF==Bp%KQy}{9E;nIj7bwMe%F|wRT~q@L#e9^=4'
    '*bC7&YiFS7uoa_kt`>&h};oz!m5)0&SKZHjVs-sKZb{!pg%&MI&6k8vCiQyxOom3fD~&Bc*Ihu#t>&Pvg&LU^)ZdFkp5z;a7QsO(C5udTd7_exb&lCed'
    'FLMa2)(BwDmjFK!=XsN95;eJpHAX}X(8cl{H;Y6!z{h1vzc!dh<?4sM=_8pLfh@!@w}fc}71LHua@MFVve;KJv!|EvuR9Mfkor}7wdf62aoa5=5)g^5&'
    'Uby3LTg9VhL5Aqchg=6Y9*sQ#R(p~{1!6w1#ykaZ&5fpb&M;W!EhKl}yqhNjhMD&j<lRn6FR%VxOH(OU$?W%0u8I8tg&oe$yh+I%h$^Kqn#2E~zL=J<y'
    '9kn!4Z#t9IHtmnp9gG~4BO#OVjx+fqqakcz%z0Tw=@k8{!65(nCy<-s3>?sO%HGh2wcFr(W3~Q%YofLfZzy{js*LN6yL(M#NUt)&ke(6jACd^~0vdw7;'
    'F{oRgU1J$QSL$D>Nu~i3D{97w$U9&(k$7s8t(<sABtNuKDGh_8o^$Wm-{>m^`Pa~GY9?X{Xy*?Fixo?iJM9G7yBK0Z6kqY+}y71HQoNt#%DcbAAv=k<W'
    'Xqp{H*%RY_v_+BMVF}P25KRlnUkZY@^-;$~xb_BfVwa(J7+?JFtOu?~`@Cb4SWpRMNJ4vc?PE?!J1fjQ!8;GG{OvZ`TgJU0YH|YvgV9?yeOJMn<sT(Qa'
    'ERM0BvftCL&UPQ%RN88(yR2^@#SH9kr9--Akro7f9R`T^j5_1uCkIAA`YqnJOP=kU0^@eEB90=7F9kP&|iq#mc<TPlR8jBD(`#K^U68T;Ri#$+g3)Is*'
    'ZbR)IqnHda9A8env)g^fStllfHe{dl^z!;Ts(>{f*|BmL_R_1b;6UWovsU`K{JDQ<s-qAd-NqR@!enU6c%{}Fsm;M0RO;CMTXyz3<w4=nq9#F?fV1o}9'
    'cguewReExR+L1w2Zi3`GBv)=Vbn!!Qhz6r07t$EcIH8K*z@>3@TuUI+*#ju%hyoc-gFPJ4XR6=dlk*vtRsIT5zJ9xOh?wii&`j0=A=?x-Gf#V)Ke%)eZ'
    'MWYJy}XA-T^t`QPAB@5VH=3vUo%^ztfe!X!$#Xn5CVXE+A#b<7L=RtL)rUYpW$n=_j=j0?DFihh0L`DwO~HQ;oi?Npek13^3@>Uvs}v$B<PPbgdtj{bQ'
    'BOiCFuG9W7WIs_so?Pt@nE-&|Yui!>nNZhi~M`CJta4*T{?1wx3mZi(Ci0F)5V>c^$WH`Ks@;E7a9nl$cKxKV$Ja*!Z2JY+&bEa#x;%2egL37~YOvnBX'
    '`Zuyh)OR(^MVYsjQV;mziM*lZbR+#548R{stIwMVh+W+pY$2Z>VW8;bl6XtPY`8}{fS>_O<>mkZFfvtZ)p^n>@t1mX5|*IDpo5T9xjWaLO}ChuP-`-DX'
    'miDi*FxUBM;VQe2PsDw>@9+a}`d#?RYC!*O7R-yoyW<cVMs>%bH=7yDFvMhUlJ;ZeiliKHTUgg;2=?Kyxb&MsrGuh<tg>VNJ&B^pL)uD^~eWuqBmOJte'
    'GVbnx=ohI&y22d2tvUT5>WfGjIC~7<8I4k7NUq-IGStjoa7NWg6oK0;&V&#GEBTGfcby!_VPRI-uS>}U!IEQV31LQ5kXu%hkp|)L&23WX(zF)*3~O;bC'
    '7i(B$zk#y2EPNrHLI=L&MX_*us28)gMA)Q%doV+0*>qucKJ%*$pK|7O7nwO4n}e7U%#0H;`VMdMEzZ8)S?d3Av?YK?0}-PHe}=+5SQ+at5k}M5XduGAB'
    'mCdlvK(AO=I}wxd>&IC3pUX3WZ}Dg2q<}t;_xkd<CP+=V2;;K9te466%z>vX@0rfDE2J8RIqs8F5O?DMJI@F2KlTEunaE){So^aIT`??y^&|p$9ZjsSw'
    '~uh4NtAHCNftvP}=AZ{-HTV~)dR=J3NQY`}h4CLeCXD%%CaPTnF}{g6tdjaf1=`izED788bsN6S&?*+I(LlEZ*gtPtp}5DfSXLhFNyGXVp*%|F`?un++'
    '0c?dAH(|$)=7`58(tDvC9a*UP&_%xNs+bO?-y6Zmd5n@@98ggrCV!eYD*a5e>5I%fn<2*cw@q5z1@O?<-kP^#8DBbR>dXWw^I^PGqTrC(%C%%1slJ^<6'
    'PT~apIS`-K>o_vnfScu~5NwE_Qfhn>Tsx3y>}cGpjU>-AuihCO{7B(rJHK@doMh7zOJH<~=b53XtW9hgd|TAKW>)YAi&esoL)fv$WcyOPLw=lAOGeSCe'
    'G11aVyUQ)KjrBleuVm#OZ*JAwh|=n?xOrgHZ?j@VvP&|xe$Ug5GJ@Y8J6<x1i20*G3Ps}j_3v2fDA^7dlV^)dpt!!7Egd7kR|5=hAC;DSi)wyR~1Qa)6'
    'A=JRq025oL36p4uv0k*YKZd13pCW9BiKO=0+Nt5J!up{y-`GI1)aYb-jbJP0)+7Pp_ohp`2GcojvcAfLAaIr?)^8B2bASwCe}77bhfuGzi{OjGn!F?#o'
    'n}>4d+tV_PR$%XPtniiUL3_hQv|TIJp>oS#=V8t@d!ZkJZ$`S}~0ah)3jhI;@Mt8dDx@2dXvYg2iey@5N7aIYa=D@a(U>eryx<aBPLCNV^rR^4@%DgCp'
    'EU>#u^x6EYRgmhX#X7rnoO+3)GS2?MQqGHim_`%I1t$Gc6IU$DBW9|Ir9}qt7aWAl}PVxw1)l*r;HG!Fm1ymLWhiZ=`O;~Q!-U4ylhs^~?6Uw#9vyIYD'
    '=F)Fb`a#O>eDogno`(A8DSX}neDD@zWT4(f5PgKS8gDshkE9<oBXG)hqwJqSOJ5~cu_S&0whZVu7a>eB{}|Q&8Ah?FIPU=zSx)wmU9{V6JWUdFGp<(!n'
    'x@LQ$a?%sxW(0I<*}f~FA;8}Rq9pK{p1NfTP*)r!!z=Pzgj%p1c?PvbrX7LvyJB5^7QlLRJDvM;PDox@`JR<UF33pxs1+R#2Hqw7@ej7i0QPty2fe9Pe'
    'Y}+n_PE`o4<_E>xu%bEHf_O4Wh(Tz1gauWdXxFO~FC<V4o^w;MN89N%-Jr)YnkrB$tP#2}f|v^02**?7hX*rtG~7z!RJU=$4#yrH>`ZA?~4Dxb=>C$yk'
    'C8HW+c3LY@iLIf!22W~*2<FDaI4e=*(%*YVZl1$;`oy`8~}Cu~BJJ@jW0)zw8*vjqyNQNsW=NdvgPvQjo~E|>|yr($ShC*)s$CzuU4;f1#q_Td;#3WCf'
    'Inq?$G4O(JqD|5rH`9YljTwsA|j`DaNPg@Se4_;d|D&-*;BHR3F&rzH|<}?c~)NjGdTYw&C+YjL%ktK+m<?-l(b&o0k3NEMILKvO5{b8<elzNnhDNooY'
    'WYr3uu6ugZ1n)|uk7Rj9Ru-^3A5}WI&<@qde$jmXsXj<6dbDbVAu|F^uw>rl2Z|Ooi2QS9H7*i$dKRid4~|y?iM@4l=X!Y-=|Ow(I8C=*TBaYM(GMt->'
    'H3+Fx_Vt-L%~Jdt{uii2J0WEMNYsj?Fp1gfBixOIEL7*m=;x`c%|qiKcKEIq4%gyLnZ7W<e;+KXeEz*szrg$#K940fT6Do_j0YzWDP3%rV329DJgTP$o'
    'B;w$$BPA1Axx75`RkgRa|b$7Em|-)EnoCg27+_r@W|lwm?D{udZGdV@(R<ozUnEBiNzvmkf0OAW@22<cm92@i*H0MqO|Pqo6_0-yNQ%d8GXUZiW*Uot9'
    '))9Wo=_3&QIsETEG&e}!{f++uircALQoj2?K96{TA$_L`!wYAWaP-F}NJbfz1=e2OM4pfo5!$o7IfcJYlO;GUE&aQCykb#<N7lB+5du=mM;rkjI-W8B}'
    'GzK#<ZQm8k|-9L48P1{m^9IFl<+%K0wFogSRhpV@Z3K`4<jj3kb#&hBCU&BHtXrSAMlg-PX&_P;L`&-d~@&^DTzo{t95BaI=2_af<z@>@|IOhZY76~|j'
    'X<u8h9Q9Zge8rCv(Z2{bMDoBUu_oJDN(88X7r+T64t4d;pzfzi1Wr|)m%r-;imANH*ZG(f+cg{t`#HHdZ5v+2H+29N!y0+jt_j9yGaC7;VPt#ybFN-go'
    '1nKn2tuoKUKQWlscF25L@DlEPAlm;(ly1-cD!OB10i^hxM}?3Ziy2xou;D`>N)GGrWBqSLjZCvl8yAZ_@O=<l)r=*CaE8*c;^yj1x7e6?mdvJ(fd5mRW'
    'kG>M6O@4Gt`!2%HsYR;M$^AL{GoTk%9a3fNl$)x&?Wn@U1`<TZIox0!tZ*R`x+iE7etA1v+TbO3;}I+kkEgQKa~(W#d9Xav0H0QF%~Rg+Uq6DJBf^s$V'
    '2_U7t$+s+YZC5;|>OC}pcySP5GipL1GWb%mnhHtNiIVpsB_Va%gH*90LCEILbwG<rXg79K;*1hbKzG&J%pYP+WTr9NyL>bNS9cMCTSG8(oJ)Lh!|&Bxt'
    '5MR1PDJ2yr2x;l~rBuWmu-I=$;vis42M)QK&Ldk9=<oG;aeFr$ISd{X2Sw1)SRWxhfQQMYo4;5t)w9c!zpbX}Cm@fEbThE|Doep4J;-h3FqysGuhJ-To'
    'P{WGNV4^4M95_RkrU+g)Ci1iglJ>AOrdYS=?aW{_!RjCJ{t6%5A_p;JQQd*|F@_-7aaUM+^{K~4aoq!q#*B}F`@KKrmy7VXjwCXKz|NV(koUM_<NhXCt'
    '`Kl#4uoBj_&u=baHr_MbQ^L5zJ%H4(X|CbBUOhen@3mX+b=mk^TSAw+KDP6PfMJC5BibOP(={FVzwRJ_M&Qs{(%jaxX=s4FeL?iAXDzb$t5Lu^_KwWrQ'
    'G!aMspmwwo2z0ct`v%oujTTbs7WzZhq+6^)=_HYfC{!uW7?q(aJk~brimA$ua0HIe~ns*u{NoSk@z>*i`w;()N33nnl!LmW3-a6h8yCSsAXlg*%6wts6'
    '<(uJI8MFdAFp_iQ`LYT|kXc<<N}-_{`v<;}9A;@e)kxAg>A>8kj)+fIf14X^M=f>)q}3_4tsBu+CW2?{Gw2Z5MxUrja9cs-Cb1P@gHkIy4%9_e||49SF'
    'JcBvPyaCuzmA|J(f!?%)M$$p;*+<g(Syz3Pnk5dBQ)rKUs`SxMDmx)xN5Pc|4xwCG@&Ef#2ac%eS4$NbJkoIfl5rf)1CS?95?l(!M>SI+dZXWpR1vh{T'
    'r|Z=yAUyq9u#Y*{PQWxS0k_n%olUrlu+bE5kNPxSG2lK(R?I*19@+L6VF3PTw%asusw#@i-alvKsmNHVK|wBd$*WgyuKgF_!(weid^~aW1k9*3sA2PHH'
    'jyVbEtnKrv7l0enmGh5?s&~MI4Kd6p%|UYAj)x!eHEY1XH*}x2N<29$%m;3yj#B@8=7;^`q|ECfd}oSiDg#Gx=J^^c-u7F-*F~cNkj?zOuN1LwQpPUs_'
    'l~Sy->G!fYI2KFzhnfSC`N}S17C$_n>Yuui_#r7;Y3QhL(4sgWKQAxZt1Tbcd8Q);kY7L=iW1=eWHS%E(VszLnyT88~MD;VNG}D96meG23q-?r`^|G=J'
    '~#J_Zfy^aKaqNf1h+et4+~LivZkhUNX;lxJY56Mh~^C+>mob*Vpu?bRm`4kAfmudTYO>Pqx`pHZpkx*WWxsFHRM+PG3^34q!?fW#;DhUiJlixJ!N_6v3'
    't&d_&YXDq0M9pr?-F@kK3QIG8vas(R&eJ-(O<dbHK+B6fjeELt%!k^g=D>Be;LROh%D6jcdz4zOF8Yx=x3=>?iAK8v}keuTv?x3mV6sgypkCBYoKEXRX'
    '<)qEfl&>xsaTj@5jZjEBJ86)SXh7FdEhK;j1xwauV@|AlhG%5QDlkPH^?DPw7YuGIs0=v<i(&taI3X@}cN13hi|ZzqSpM=fP%4lW!;qo^$<cua?m@v~l'
    '(54WSwjmue(}Z#mfrw{`iGyPR`^i!!>^OmSDJ&8zc2lL^>S#%tXs+NbOu@c`&T9Bz7Xj9OXvPHQq0j5=e__#<sRJEID~c&Zo)`irJ*^nXbeq82D0o!Z&'
    '1-UZu%GFcAYv}@8Wp^<HKF((g@|dDDNWn%w_8{7`7BPC33jEz%XN6OHaT<9g3pgoW=u~Yo^4_cQes?w$6D3Yr}Krt>E&yEhG~k`r%<X$hOyz;#4rCvYo'
    ')fSi`*9Z8CW0<l3=7RLWZp$LCccdHFR+on0cu-&7`gh!!VQ>s4e*;YYEZv#f^817K5pVjK5Aj1K3V0EwZ3ls`oroxUI@7`{@OCNRT>KcMxWEZ~z==C1%'
    'pn?N~gpI5;y8HiHBuoAZyuf1i}t#f^vK;IB|6oYHOGfAZ4gYAxM@^!^C_Qy=#x-=GCO3{-MX3@Yg(FIqvOja}+vx99}#4mCPx8@f8m5-%DF*?{mcG-?q'
    'TH<PAX@k@_X;|_rB;fA5H?OpOR4kyw?F82VKfJh?xd-RNc0q*bn7y6witey<UIMl}4z1*GTgN|gS(j&_4m3uQuC=V%%bQ7}Q4X6)OrPMapQH2{nNE4gh'
    'zaH22>>r!#{-Sd%nZ8ue<ubAcPcWLEC=+PUt{nb4*z>mYaI-6X?3trZ<}skd|h=n3MiaVKyjrvCn%R^(_vQnM!v2tNfy?pT}RTe@L%`mT-8-xL2pu@Mp'
    '`YAdRUItOU|V2h8>AQuo&5acm1*Ue-OT^p(TGc2)&8;q4*v*YuSHbz}lFugdWM-=kfa!GNaRcsSf&BMMWCU*=-*#?*H`@hK`B5kba6NP+uRb9j@`#c%G'
    '(cTV^xCc%Go?ev4*qP^(e4NB#G6J>mL~55_)ped>d)?uW<}fs*-0FOeP3u6Q|t(H#Kk5*g)}d0(~5fcsxxQv@D_U=EJi(<hhE;lZA+Jp#lI-MYHAj{!T'
    'Z?Xsq0X=?3jIu`aNT0j7F0;AD%Y^SNWz{Qx;d{-x9A*H;>u)D1bzm*owZZ;9qe0W0lG+KXq3sx8?g@uV+!O{tCyVF{`nf72Kgb8Vr3eD?w(U5;PyU3j7'
    '3wqQmmY^~?xO<f-<F;RS97T!9AA61J(?E;+JB!oe_PrYEH*!2DFvd&(?yi|qHs#p-%7~7bAmy6<^G=4_+dvRHUXE2*FwnLlF}jx&Vf2!wx-@s`Nk)vs!'
    'EGKf6UJ!gn0!>#V^)}57O%R>m)SOBHuWk05*qus=Kh2XRPS#?d-=Ny%@l|2*z&>q=#W#1-ISB7;k~^0CHgd~xbqG}k5BC_+lf|1qguZ!owynI0&Zpzby'
    'wI_8ahXy{iWU9OeJ%`{XdM4^W?{q6~u>lkbZ-#FIJjrJG+hQ1V*DU)V6T8z{6dxj|sODDl=+Sf^V8?8vJ1H)};L@^-z%*J?J26%eIPFBz*WUcytz{bCi'
    'ms&Ww`}90h7gfQGt*Ffuks_tHKOLf;vRSNWGa<M6=T^v|b}KmQ<dd9W?(fRh;kZ|boBI?q{&n~d5v5{yhtUR^?_egId7Y-~G#;R|_Q4NIz3>f==QW&i4'
    '#f3(HzuSq^rh}#3W%@vM7{(S9zU|tSn8uOCB>tyM3Z))$)To_A)c5~|uG8^-ptVi6%;HMaDB3x6hZN81On=_n<8+`QJ&oRThxwVO=LciHW(@XGb1T8<!'
    'ya)S*n#;ey_Cwgb6t+{s4su4>3ZuV%YNWl{9QfQ8LoV7W!M+U5sCR)<I^ULZ%~glhfreD(a+B}?Ihtjo*G_13$k7Nx5$Y-~lzB#@JEHPPN)fsLDp7tlO'
    'kmsbn3uR&d{=Qz_n?Sccr_C&8i8KI_)(#t5;Y5?cgJd{0p?q|lP`Aa7G#n<_`8bEDh52<S<VN^sJyyHVVpkww0~7BtaPgOcKSyaoJ|G)oZ5HdtQ|z%4u'
    'EBq&#Qis<K<e`ks<6TJ6^8zJFdtexVH1-5$GjEUw2WX`C`OsQ0I0FxF`hT&8XCWt)CKwJdlyfyrftWin8z1ul(GS$k%_InQ}?Wz(Dy+3~HHNd*TYon+O'
    '&>nV8Co*`u2gt8w~rCRb*$it5Da6}cj-bhReeN9SRNcXI$!YLhep`q;!}Fh0#tFrPodDP=1MGTo7jd&i)-i@4G&KG5ipr%0r%L}`_75EMU%3m;H)YLg?'
    '8B5aTtDqpCGJHV)ci`LT*3Y<Q`-DW-G_R_jwnT&x?pzO)v+wdOAcE|5ANhxCK%RD7}G02+zX87Iq;zD2({nNgw6E2O~`(Y|AuU?mbkd3t{*RY#$h<SZF'
    '&8K(|%6sTj|G$%B*s<vR<Q*7<5p0M{t|$3ftOK3ryh}QdD4n$*owxrsC1DCCI;222$JqPs1bRv9^9@!K{W8|M`ZSfn;oYQ2-Wm&~iEn5O7vrV_9fS_Yx'
    '=I(7ahr=7kX`U-e2BdLf2P#Q9ndrdLY$^;i-t0<!ZOc>_jJ21A-EGq1Ifr~Nuy(Z7V5MHtQ@=HQkDo;K7B-n&By?U0A&3Pr#>UoioTf`@U@GbTU8OvvC'
    'eu*z5%lZ{IG{Td3`Y`e~7Thqa=f$SE(B<zIHHgtNj5+W8?n@{3Xx$CUBBtI-yWbUPVVJ8nopDkZ4MHBtP7!bU*A}@FdGWEW1!057SA1v*ajUsKVZ@x~}'
    'l!Ktnp@PENrdtF)$^FF(#HH3bH4O(u@MGpzGbu}21DwZNm$$G)#N;|#<Z7pZ{T`1A>$X0&*PZY);X1foF7iG|A9tZ^zclAt@4aG)|fu_`LcY<zI+EQA='
    'X{o*$Jdzno}W@l`oVLy=q$RRO!$LK9Mra71Idz%QOV>S<%pe3D+2NsRm$Gwt>WZ`!;=yuRRfjYK4eX_7)J8!xL-K{SBEzEM5=6ZQ{5YSH1n2gNmHj&0S'
    '8~NR(-k6u4t{3Y<V7q@3A<ejo@5prZp?MQl-UQwoF#SySpv^AFc3gK0+e#Ob`C1R8>@bRj_Jd@F=~N@M$E-a?5IngFZsmbX6F!K;OL*KfcINucjm8=4w'
    '@cvm4!bZ(c}Q)2F7|-SlFj!}M`2oXMs2Pc>}Pet9$0h-R4PW3TGw~n3ojxE8d4!g*)|r&Z@lcKOBtPrNz>6=#3@RHdOO*?ke%1n-g1in9&p;9rtG%R;5'
    'IiVk^Pww>^|<nO)9VQrHPLSX44kh9N}od&2T9XV2U?3X*P(PD631R1)i|zaEo(<@~XQYW;4l`DOfayJp+?&9%+@o0uVwf%Hie%JBoh$^x&sh%L+{?{UB'
    '15lvTGVY*WD~%zR?N2C)J<t)g_We#sw*Ha9*v7k!+Q3arSCZWnR|U0JnDoSO15%^fPUVH=@>u5e}JWz)SQi0n7%Lr6T)c7lsjk?Ax@93sE3BHU}y`E%&'
    '0AhAl(2eIM?4x+Z}3^;k%Zm%2&7~(9w&(L(TM-<z6>nc5x{SDxV$_8G5w%4@<?xaGwW|&j`W1=Z<Z&OM+QlaEjptyx*N|F%01V->sKF<Y<&zso`LeOm{'
    ')2B-BLt4F)Abk1?WHg#rU`SS#R$b5hyfBijV$s}9(pSAmRgzZ$o=#QK>3oL5YSiLun^)sP)Veoyr~5j0L$#>IbM`yWz~~_w%iE{J8D$6+nIia92+2{Fe'
    'Z1}|wf_JKLa=7LTy4%W&iNsNuJkE!WaF1h*K7f>7q0)d`uLAhOo2^c;t)*iPTV|MedU`A_*-NwZNh!sh}%uLUcT_Bx5raJfc(L3jxxdbpr)ZRLHsH}Xg'
    '3k1=8d@n0!~nj3ihDBKCSY?PSc7;i9CpPqIng~jT_1{Wyhea6fK1CvcadDt-XIP=`Ki=_P&|i`wJQtl@gby(L>mY52_u3_wM1UL!EJ1;SzhS0$)^U(Z3'
    'L_@w}Z$9?W*ix~F#K1nklrR+!!7R9<yo@f493$#`Vo#P&hn;PX<J6SPYgA%E8kQK-Bcd&NUP)26m-t=aw;Y1&#e&;6VeFx_vFs-?O%p30;Fp&sWeA{96'
    '1fbyr0)HV3`3II>gjBXn^dYkX&mjC)TpJb+bo52f#w7MGB<g4oQ6qSnkyDVI&s6)LkxyKPULxlURwGiSQ;j`v;b%5;G{?#qurF(2s^QJQ)4dk(gA3F>c'
    'm{aZ{j(!Ft)h&fSe*k{Y&k)r=1G?cB{59Fxb{NFZKhea;zC;D4+C%0RH^Lqy=J8jo54MiW6>7<5?%a&#I;3W|dwR6MmK|fT&v~~hGSY7%$f9@Gm)t3ye'
    '#N5I7%x<iQejLt3mVxflAr{==Lh?~LSFT^!j^v=%5%b^aSo19hZS%GExX5_(2x!}eKiNR$gOJp%V}^*uyMlpmlHJQHl%J-IylP}N9}f5alU+pz;j+%UG'
    'M1ubF>LB@t%E!(@G0x`m_Nj?ZN5yz(P4%UG<6|qD~VEX;kQ>O|YgqjXrGg(o2z6d70OIpwYbtO|^cD9D8A@cdULwqB^QFw=#=v0${K9cz(9OLh(w}^c9'
    'b2QByDF{KjyJ6BtcYfl6~}U%P34*RO!nzs-+la5n7!@rLGY<6KxX-i8$hir3Y6QEdVLqCYh;Z-0+R9Gs9Tcj4q0mi9te%{3Q|!x#+ES6AVmoqSS~59F^'
    '(ZTrU#Zuvfcr#7x&K#4mF1$GrTFS9yPQ(s31J-Lxl!?KGYwS!WIs7XjW(1*GFibu&Jf^~#}A#NteJ?wp_4zv7l3wEgH;d->h_M(<p)E<u_p7SeMwZ`We'
    'b0lx!Oq?S`VksJru8>*zNs-tCn(q8+2J>~c5!M-;W`^KM{5c5&!hw}nX+cNr>2aFdtxYKlQEzN1$+?qUexfP|8jY%CY%GZUSk<??Kf2Ezk5h`=%VyrT&'
    '3aPIGLW$*lJkrX`C)(90ZV7{ZAlT0c0eoYxGEOZ!A_1ei@FYU-HW%~!A%HAn@45=0wO-H;XNVK={<O%3O}#%m(6<l0(+p*DSmR`qO3ODo2mRxv^8T1I<'
    '-L~YrEU~K6uH!J0nr*dgCoNvc@jg*()A!hC)izPL5dGl2f@FNwQf)*sicPF%1}qQ_j=_8L9LfzsZt1viB>-&!4d9-cR(h8*E#Of|qs1D?Q&Ia`fi)%oB'
    '&<f)w%?{ldWnj}uB-4p>Ts2?DQWUA-a&hJS2AO~j)8q%og_I}T(T^>Kx(2&5lEL8COK8}Ecgi95=sGTZmC=GvX1s7$3Ru{=rInXgUXc>2gX9$<9h20s#'
    'a!$K`|o^r6!dL-{$1P71V1u8I7Rj_2TaT;#IU1q<@h8PJ$aM8GcJi{N($WW(3;uugWT-pX!DZXwFC>@;EvYAP%u%`Xxghr?NWCTXbx(bV~O&Pb+(h!X7'
    'IzexIP5Z(+<7(;>Ke6~{MxH=x<ac&su~5hl?tWkF*XA=|WqWlWR!Ub`s#v_=OG1P2GwiEyui5?2mhyn26MPD`qbPW!>sUdW&*#%*J8!MC7i6^BH^8^qi'
    ';?KjjFhL(X!44o^lk29i28fDR*O2`BNBab4e{+Df`P{H9!rxF3VC(GWcm6mRSJQJAu%r1O-t@htdEEttkQj(lKHK#W&)&XB&WY{t`IVRKUn*O*OJD-pc'
    '{jC{u{W?-*60ofh?l6Q}Q?5@(quE2_QM~G2)DVL4SahX%B9xz*^K+8p!ZM4E_K^I*s|BkJ$o`s_IoV=@ycxAiZ}}P9<)~dJcupLSNvqdU=sbT-RnhQp;'
    'yF{G8R~1Woz!vRv!IZL7eGya*>SN*xMcE>{aYd<}h+8j-+Q^v)U#mb{<Sh{Wxb)$ums=(ufa@!xcA^23zpZG%ho0>42veiEToc{ihu1>U2ER~c^p2bQ7'
    '>!G{nn158PGv%Y2Fauec^%`e9E?W~39Lrh)++UF%7_jcgFezM;kth72AjLz!dpo~7RUYEuNMiXiKpP+MrotqIWK9CvhF2o#oUZwX(WSk_X)0r^x(V9^C'
    'yFQw_G-aQJAl7GvWv$8;ic_K{`BciQ;a05dDM?R#tQNnO?}K>|NmwRztGo~Ha~s59P1wUeyeL8$%B&z2Y%uaQB(y0|DFuk0Jf_j6F%f>yge2erOk>!??'
    ')e8<W4cVyFKJA3I~t1K3!H>>jFK?R+9?h+nj<K-bW>l*^J=(G;RJA;oG|jiVwj=$`OzxH-$~%bG*nEf%gz<>hu6$mHo>6*j&$nCnPM|DL{#pE1+MgC^%'
    'imTlm6Y<<_ku!n+$R1N5C7t%!9yuEX7Egi^z|5w6JlN3zm_PQr09Nt0Ox7*|k_&Li8T27PR{+9ey`TmcgJ_MV!o0689hjT@J=dBPZ*1gZ->sI4wy@Y*Q'
    'x9E^=;%)?D@S<GfP*2t(jq!}78ZoI!b=-Xq8XvREn<-zP3RLD4w|h{DVl>Tue>d<-FKro7~@`q9gptM^vWTA#ou-_Jo=a1^?4en|R0<M%>vG{eB7wn8|'
    '{Qzz+Qw5Jt(?iGvjT*|Ye2Cy~bu#Bt!!ij71;U=Q4{1eRXEeq8tWfg9DUw<x0saO<NCVv+NE0b3Jb<Pt1eX3EM_i=*g3ofG+y`1wdtA52$=lV3%!3HCS'
    '@zQFzE{&&uKKRX>G0bd(F#;>-t)N~3rWCz6p{cIgTi5PC*LGAairS9!UAcKBt+HmaRMq+I(*ZueI8|J_H+MjTQC8hDwbv3~q_4&=s)-hMTr@1@ghi(^D'
    'AQw=Raz!)RPtk`uUcUWdtJpv9ibJBQr<hEzX<$$2F!btm`+%fxCf`vKA{AbkYW`i%9oNrbivB1zjnd!^Jh*Re9!y<0;LO<C(lSO*fE!e76ia56pbDKVd'
    '-sQ)1m-aK@ya*Cb42K%_A{izl2EPySXkDJSGlb;t8pa3QQ?^nYpyA;;ja`2T6nSJQZ_k`ED2@538&?lwlC_fUl??zYlw4)cLydI?dm;X^=ty&)Wrc$~6'
    'h}_YEj?MbaKpJk~67V4zJh4q>~|OPXo-*1e?Q^e-uVci6Vb{a4qM)L{nTC|I99ov~9CKjR{mi3BeadHv4(dNFodp(tUqm{dwbj!Z8S_C|?JfkHY}Z)Ur'
    '8P}|GFdKR2Nrn`Oh5I@s<G}@3-9>pZv_`JHN8oNABXXSAi!$X0}<wyP)m`${i5sdM3<c~K(de}0kwA=7swIHwFoQ|1QO7VYl@3y$b3hZ-9;i(KF6%%x4'
    'WuGewwc_8QfodB7Vjcl@#@S1L%#=rnn@bE76W^W<&u@bnmg>mG(HsJC2d|W06)dEL9YmhPJTnORy8R~gP>{1k#s?psmoXJwtlPq7W>Kgh11t<CS5(Pw^'
    'iou4q_RK*)|ayC*Q741&~(z5TR_U{stkPvqm*|T0uP$0u$zAw5%Z|D?Jbx<C{=-}HzDP_>*^x6AwN#x=i6h<QkSLoSOs@E-3`12-fX_Lyg6_2xelF>fp'
    '!;tW^jAVGcvcrP}q$_>47WQ!0l&dQ^#quorz8_hC)t{yL8iz@AhK~b=9t*A*)YArM#tj)w;S6wyH$ceg<-l)O#sxt+d4?@o9BQU2}b&)+&l)p==p(q@b'
    'qRF0<QJiYR(SPw?2Iw4Vu4I_a}O(qC8o8jiyg8d4b-2t(<|FMhlCkq5orkW|KetSa;-xiFhAV-OOdk(pRaULmqhgYs@@up6_a=Jo_e;|}1SsFd7+yt;y'
    'G<an4-IRk^q-nLAGx@<(4a&xU%NC}%|A0|(mrX^jVComcx21|34m4Jty>-&=iw8Vw&+W^I<vEmahQtct12P9~EKE_rZu?HI6I|^RtH><03F=Yc2<)nK)'
    'S>pspCv=p8WVQ@;x$Fi_lgW(_(K`o>064|gpejn1zv_ju^1A9TF5TaH^Vb0735@Qg6`P(<s|$+uj;Ea)#mBgVP~VYPV{dHm%_B*VJE#gg>UTkwcU;qhE'
    '5er4I|^BZ)9Ri2t4C)NMpYUipYlOyK&Y$!Mj6l}m+Jtdv5C}n%{9PNARI5_NM^ex8PCxJcpJwZ2sgF?po-)5fVSHpi5cRW66N-BdVrY%+)AA*v$<UE1D'
    '6I0oFu-;eI)G+fkUQZ%#qs4kGE<i>#wW2V^EugX+Y=HhjokWO9rD;ljK!Nk-L57$r|3*iUpMN_CtAxYksjAC1WQjN@)lAT0w1M<h?BX35({Pv^#^V6SV'
    '}4rkCFuVCxxeh$dK|89HNIf^=DDtUT^9uYxp_{%dew$m3PmTL$#_yp*6q^D0-yw)`xeZSWHN_#@CO*$+Wqw)0JLckiJ_f#~P73hLY3pBb#`)0|R8aFEx'
    'qh0$w`QO{UVC+=(5aYd`@UD#ygBXl3~ae0KM+MWQ>80&WP$dimC8RdcMN)Jl^9_>m9^@=c(<L94XcuM-zwcRlT#djsIF8vhH%|pJU_^#V@APGOn!I!*h'
    'SCqyaPtyg~T}#)|z|JF~7hS3kFr-qGTs17J2|vFZ=?<fxfg>YnC-0cWHx%jGccg!(!g1>n-IVLy0ZsRc=4l$D(r~Y&<fYP#1B^~-a^F7*N`tW%Sbrzdc'
    'wo`^?ykf@%>Z{?<9g+Xd7ZZB5^YJ^1U-76w(Q!(&p*g8BRG8k(}})GOn+^{w};is#}R?j()eK!QeIub_g)`{O4LqhSFNjyt{C}YphFEtu58GwaTRJX$_'
    'fU**KT;>EIL-{kz%Wj`0+T+>EmT;?qe0qk{QkwjMoV~4}`ITz;Rjo%n1ys1di|ptpz?~ZoJZTA6S&Q{a0ymi`)0kpVx5vFVACuoA>Lc<X-Ko=joqf-Eq'
    'tjF5^NRH=3xm=0K+01<9}Jz6!V~@WO@dSrd5S_H1l%+g^{u^tS!_EY)$(Df-qQvu?|T8ZgTW8V@v}5_c|Hl*HXHXnZL-$AvdWrzbcFEb`8Xq8PN`*%&l'
    '{hS5fvawcVDPNz5Nf-k?MNXx1LGZe2xJ#Z7bc=uNfTWkUV2DZ%6dFLj$x)~TK_YlAG<*npnuPf%AG}3`ZW7b1Ce)_TxrI+?HjYMSVAlbqhccV}SF)KvB'
    'f3;5%c3QpD``%F)waq!6XLQ2%L*aKz(_TrLhB7}*V=6<cJRJ!<^th^C6gb}yc#!Lk!=QBJi=h}Pk^cunKA33j6c0YM<xs<I<Yrqd`~cf3(#0W*{{nmPC'
    'Pu@6z;Vindmz)99w7O~8cI3L*e~98Vc18c{nMhK;ujytbSgFKpE*|T>#W7~VMeJ!XhyL6%o{J=x<i+>LNNAudMe4d0T&PxouGhD+=Dia&{i`Gb<`$cFz'
    '7Z9hYdxT;HB+P)}@xhUcnRPEyPtBnv6tM{2j{(p1!4w<sPY5m%q2>pTKC0<Ihx&7P!0ii6;eoW0%uJ61S5>r?Ktrd>=I{Q4jhv;%-&>V)8Nmi{1u^+x0'
    'Jcwjt7#y4cvVzzkI;RnbII_yG5)tsixU$F{m?Bz{0SvJ|x>zsz95b`fE60Cs|XkENJMbDYNFbAhBnB=O~aQ4&S(qu*{)n<sOM9z64p4`@cGLS9ZY?LP$'
    '?#Z4T2n$SoFdD~8y@-~B4CY|z}5i9RuKRcFq2*=11r3OV|Hk0iPB@ujj>r?DwbLThrfavoYJZXc^$ds~2D+kf`)n$F18H-cG_M^iV{dJx)WwwcJ%zqyI'
    '1n(^4GWspJ(H1)4%XfGa^3$vuHVj}-nC5&#)}RH=;(?V$3*hxcfQhnZWx_J1K<Yr&WR_K(<cDd5ox~|uJ`_H8#preCLO{xO;$a3qMFEC)_R#eX%Bow_W'
    'AH0a`Ao`FGNd$ATIFlUc=Kw=xZ^L_FeN8dp(!rk8K{(BNUO1DWZqEx0tIwRlWISlh#JhYO84?KRdhIi7g$u96l3LO%AT?4v?OsI%Z3JQZ+zNIS^`$*ah'
    '0#KXj*ba+uqO;exfB-S#w&EiPH8xclSMMs|Y{}E+Na8S1(H-XZ6fhFr*VV50zUnK%M@fcOHw2>nXU$H4z1uR?}HbdL&2S_P~xuc7>H`j)V+FI)t)HYs#'
    '%E7Tp_)W5-LJl6_VgqUw5u80-|4LA~FFEYP&()$h*Fcgtuh8jaFGNyqkup9dMZOLAh1Xn{Bq7{U-9Uy^?3E6B3H+<{+J`+E~Lf8Dr$<-TU>)d3q-(W9a'
    'fLP|by+k>PpU(B-h^tZjSGa6|tG^0}c?1PEhuoAQWd>ap~KRCu6M1lm^Y5Z>WzW?1iLOYLer0eMAFb1^qv0hBp6ewPeT0E9^GB;x{H9bTiW65$rZ=%Nu'
    'ZF&V6DmM|t3YxSUpU;xI@q%VBIz`7Q+s90$=dP<HAt0DEPYPnhDOHkX7fK_Kadb50)iss-^~cs&dEBe4y)1nzaX){JB8`Y(0U)ONZ#?1k#kOrqmOYNjv'
    'Ge#0jbmeY`~K$GP;xE#xnqOs+UztYz=%|#dG)5*{CmAs1p_MW0hzJ2%$-=2oGDmXDQ}R^)_v~8Wr|?Cx~4fDW%6-w(alg90Od)$PnH--KJ?P6zhlYBgw'
    '2)Tb`qVz36u&2nD%Fo3;VZKx~)CW8GZ{Wqr>0y2Q~Q7-wTvIp@FPmG)FG(xe{6b$w*8DeS72nbq`sU#*@(%ixqw7f_8<U{F25!6%yF6S$PvU>As;UM-{'
    '=XJ%G3egV)nKCTdI;=%t&Gp-D;AeHG1h<YvNcq$0UKR`XZmNZP4_W(na&#bR`@!3gp9x*D#BikMLU-JCQ3`2KoAc4=b{@gm8>x65egD;iYlI!LqN>Z-d'
    ';QIh}sMHi-RLpKX9zmy=HuE*-F;P#(~epW0>?Z1SLxPl&PI_z7sQT}=!-A0W<8auC2Z*E#^Y67L(AA1MefqJe|dZaL@Nos5MfY-C%Hc!~N2oyQkm?-IK'
    'lKR}(L>#FPb&L65xJXqjMh`oPrZPKs@P4ZJghgi?v&&|kEN*v;;rhv;nz4`qH_8*3>S|bI98o!eY5%Iouvk}@RJYZqIVEg~pWs-%oXxRmhKFcu{v;D;S'
    'V!zOlSzB9Ldj2E4U4=0Y?Cclu;UPRyw&^s*WSI8lr+vGwlp;@@NiAxLjO{+{_zjQ{*ihwEL<P%wA<K66Z8$_J`nVLbCjl$QrP|nXnE89=;N$i#4p0Z;V'
    'x1RNHUjH2u2hkgU>sBZq5C(hlXu&_~=Ez@Kwp#{t(@f2BQ=9uMvY27*J>swZxbm+579}17~1+2l8j^yJdGzfc$LINT2o?KFgc9Spxq+XQ-u>8KQPVT=G'
    'EOsSW-qvkBIq@erKz42GVkrcTf_PFfZ(426$t7;EbDbkVXrGsB3;^XeTogTFO!=Z6_p#fK|{Cq!?a1d5<H(oiQbI!zflYR9U5f2>h`n%4@Pg)Y&3wPAy'
    'MSx;Ei6<UD>QQT;IFXm6qIKXCEAiC(?cC^wI8;L4zLszC}h<;k0t$|BejVl<Z)ENXhI(z^h>pK41Y`Z5;=W2{Z6B>(8$aHUPLlV&0g<nw0SD^%@(y&k{'
    'R9AV?JvKiLm9WFeuIE*~u0QhMkH;ysB#h)u<}1VSvX%t3xJe>stgF0Y$leKy#;jYJw=1twZ=S@@V8-BqfP&xt3t@uft&+B@VhqC_#5L}^ZMiKkP=l9IT'
    'ZuPkewxxM%dV8L{X$`@QG20GGOzMIvY$t`@Bm|UZ|C6bvBG@gS8So}mS0Qaj>9)HUPoZ&DM{47kx`Ca<kff`H%i>PWE?O9;1mZpKbrJ=kR;E$Fs^RT?w'
    '<{0nTb88Y3lA_!B2&y{1xCkY1;e>P3hr_H7EI5qzCQ)0Qv=m2T*1@Y+I?>qV@}IIe=Qln+Dnz^*#rP77Q7wU_f<;v!%^)AZ|3v#O;;0AKP{)wy61D)!-'
    'Aq_P_!vY3HlHbmZro==<zFCn&lDA5ji!?W?FtUU+tn(Ln?g?G%-1Xst&ogHi*0UG+;VeCuPC?hCr%Z=k5Mwu!+z`jCly!t+`66_=l~1$SLGJ1b^!4|bI'
    'E)>dd|YR7nn<bin(iIf(%8|h=>ii*st483bBdcStzN`hlWThIxNCKllKJdJchxi&NN7Ku_tqB@}R2t5!Cy&sG?m|b0c0%LU29stj)@rEPw=YIa%xSJeJ'
    'gB!Xz-gm0jTub>l(i0d|4Cd8UVch&Q(8G>5rTx?A1_%eOT!3I_yrF>VH#C=FONyTAZ0<O|4`?h;Xq2jMh0#oPm2b=ie00}1!00SFL^(|7_KewYlTu4|W'
    ')2p@{jbltac`toh31qeDH_=ya6*0cTc#!pO{MT@U?<41sTU6Tk$x&X^3kAoHUz|}?r;WS)VmoNPFywVus%&`I5Eg&U{3HQUxOW#rC&R++pya*9)Mr~Eh'
    '2ei&O^Tb%EVle={Ip^sA)DqMq*2U$Kq!+ny`T{<$+f4CXKyr=}(>Ufuq}g14fK!r=$%IWE%6o<3uTF<69o~m*sz;QOe&B<?p|OQSaWJyp}XQUo-|b-?x'
    '0&ubP{TMP=N5ikA*FCBwd)0CQbf@&~QS$aGo*-(A=3F8F$?E&U4)YSF$o)=wy~I)EuPh~>srX%nyH`trlH3KWKVAHEzoL4k;!IsU)^&$wjeXKObxNSp0'
    '5QJ00wG8WdsZh*D0tu#sHRt<qlVbCb^H~FhPlyzJt>-03QgiUhu*uL7C`1NxS4{_7)m_N^>vi{ylb^z0vn7?e|ho9acxnQGl-Wz9U#Gh_&LcEXURYk>;'
    'g~*8r#amz#rWj52-Q;gu_?^+PA@>}JHh*(FekZ*Vw>BY$b$pr8Fs^sE|C~*u<1iWy+fCQb@;Ff)<MA!qTDy&%S9Z+Kftxy=A|-*`l+K%<LDj=;(4ag!4'
    '`_xp*=64jS{><-I7%R+#x-2o6^bgplSgfnjGC^QyV4~QHZy9_ZvPL9af0A8E>vy-Wx5M)Z^2TY`PAhaKa+AFILrqw&6~TUL?izUin#LTdTs}CB5w>nI*'
    'wC%&9eBS?ssT<ahp9fjN)r@<=o|8m+<rGRY1`33rB-!0vZVYp&Iv+6I}XQbQ{>GJV`GC;`8XH+Te3La%!mk5JskR25`N?_izjHZUWCOZ0gPYeXGB37n8'
    'D4j*ELNBDWe<C-2b5&2=-(&Rai2*-_isx`RZJx8Qndp_l5QGZ>?ly^{+pxvh42(@)3`b4qa!QV@B%`t?k7V&*)D%~*=g&v{|h2?=fovi@#DDCV9$4YGm'
    '>I^3B_k_6nOU^1W9Lw|q)m9EpEBWE8AFVt|)uEOD#j8m5O4oeJPhSp*3Y6<@PN;fml$y?=z8|5yXoNvynakV#=AFKTpc%(W2QZM%l_C1vD6pek~ht`YN'
    'k$o8aW$!%w3pUn&Bh!TI$bn1akOi3*x$Xg$OhT_<Sf@03r~}Ekh2G-Ug?G06OO9uRHV<T=(jY+jA`;pg&5N<{|BAcXY)MfVx}!cMArKZb``^Z>Agz?aR'
    'EhMw*Y6KC$4d$@<l|mx3(dNw_op)SPtdn4__Umwky8LB@qv6CQE4wN6I0*vUZ?0y+l4;f_Z&Ik{pY~M{(H~S^%Rt^A=kERU&DHp@;AhAi=XC#i#~=x%*'
    '#cV#_WKrs(bBwz)q#0tZu78b9sTEe8R{Sko<_KRK_L7FCywCu~M8?=+@iODxe;Af7uI<$WQFREZ;*avDWKJq~gE9oT9<h27dd%JH01)ROVc=^21DtxwN'
    '@QK{q!)SN{FR+-%*i+h?I;?s&y>@UdT`INE=XMS?%?*Nn?*`_#34(l{oDQ`ziLY?g1dH5%CKsLZ<3=EtGU+yGtNY2pS2u%nLZd#MQ7#`~5watSG{*TMv'
    'Nyc#}O=qoTW&OvnFKDFV`*H?@fefx|IRopq+Y7n`qzl)du+(pi?z&L}Xhb+_PAHfubs|v;zvfQBcfYY~)?2ner3dY$Cj$RLQ@1t{Al2efmHQGC!I=@@o'
    '=|r~mYNPUw1<bgk`VMWlwqIq_PHjk9$Gslha8hk=kM&@YQzB#*pu~%(E}lj>VHKId`V}X2UQTI-$oK(^5x1Ork6PT9>|U>|nu>*-5x3TuROI31{)NN=f'
    'Wa-U00nveLMpH<NuBkh>BQ#TT0Y&^&<$$atXPbe*XeDFz<sz)MtOwb7WXT94xWc3qY5&z86TXQj4G3Gb6B%J7i$+btL|eSMVeBaRiH#0YJ}d3xu$D;A*'
    '5QKWn3Xy7zC8<iH2zQxC0&0q*%<Gx-sc1J8Lk}r0hWfV_Mp<m05WexBCZVALX%R#2?hmQyHbNcpD$oqf|7;6%*CYi=vi&F9t~wG2qJfg@{M4jGr|^KDE'
    '!g*6+>%z(gt>Q}W;(CZ7_)fK&3TDS+ODQKldUB_dcH8A^*kED*?#`5wU7l(BwPFXJm(;B*tSUR+2IiP#HCbV<U90CquU(s0W-J9{|?9VXS5RW!D}3_4?'
    'C(S1+187L%Pwg9XQgb_FmxlCnb(Q5ysrZdYd+|&4^@!h*)u~{FxTNvS9@@GiJO+jMZvdBG$0r|W7rr*7@j6}?!Q9ti7Xl!z?fJWm9kGc$MR?bGauW=@<'
    'znC}<Su93T)cjGcS7#lK_q8eh4-iCL^{l{&JMe)#Jtxu1hg*?eU%qh*{J1IF2^9+2V2ge*qEa2#=Cy?dog`+&#Zllfr(_0Zu|4)=aRA%Q=bM*UG|S=?T'
    'uuq_?Wp@<;l;udFs=Dj7AH7H*R?{k#QU%t@545Hr7+|D_0B?+KschdwL&9joQBq@Uyf2Yg_W~HvB3sig}jW$FJkomnZ4mGV|fca=)n2DHfw<9dqu*wE!'
    '!#OE2C3n$IG+O2%1&>hccSxyqR_=#+A*w>x+fHVv?=d==x?@omHLgWtZ2eLF5E$$>j1Uc^vXyf?!_cS<_l`2@o~zhBxl2*eYyymkj#e_Pp+cdS}U?H*e'
    '3}9<6Opx|FtjB|BWP*!CwmRn>1Dg)2U!j|ySn;Mgs$qld^s!)9R<^(#0pH*8q7klmRL^Ti7`w#_xllF6`GiA~l~xiP)}L!Mo9e(@ZMnc{%$S|4txL&={'
    'F-D$|`yY#2AGCcVd0KFq{=%RnF9af>4ry&Zdcpgob^d(V3;Rt^Hi}7*xvI0Znib_f#VH;hYjR^_0ip1MM11aTu`zX$!XsA%ch&m~4#@i@@g@O~<A^Er*'
    'nzF2XKQ2d=5y~m++nWr+DRSLT2CszcKf(tQ$>FN#63HWl$MJ&$GBE96HJw~%0fy3#8sgsiEgom`D>Ca>p|MEkQ5sT!U^BgmLppynbS)n(lJ<0H&WxV$;'
    'mf$<vf4c=shv9d4Q;@~_ZCJ|X6FE{WBbW1rS-93H#~L?PgA7Ru8etScyK+#<D7QPtM6GiIClLtEIQt|1i*eh<Wph@A|deeD&h*w8D}BN^|p>qDyul4v8'
    '<a;$SjY>dnavqO$j;g5^;#gM@RG5<K7xSv&;ZiCkpO3i~~ab6{H~>c%OyE<HwD~R(fB=lx~*_B^r$%b;mQ0#@k!t%BB?=5s(r%9<>Wri7~PoUv!rtkH#'
    '77LiK56w0=6>C|lU+s)4kZUqJpT!EnGrrbA~<p_6i4Qp-cFL^P92e)&Hn2{Zid-TgI`e8A<*bI_IywIzjb<r-Af%7VeDF#l%Imjr|#r~OKPkjh^#G~!Z'
    '%t+2R03DU{@>S&nJvU9*<SuxqX+%<l9KP*_E<&DPP+GBlpt4=fahJ{SV2B&0@^C+AMcnuvRKdPx6uo!WZe&f!gG^LNOJ`QZ_q~RcJamV|T$}uzm^>JW&'
    '^^;DB8;UTyoAv6a!hmbCH^$RTvavVD6UmKMLU|yfcg%jwF2%FHc-Q!ua_f4RX5)u1oZg44rlC;l0MF2*yK3Y{acT^Y=3$XqmisMmnN!wuwYT)uo<Ev;<'
    '_4H+Zn2viNC%ErHQFC}9B_bv1u=?0bpSq{hNaZJepY(ZaCBew?-BQZ2l9@ei~cJx>l%uuC`>ztcv6OWIY#4K!jr!BJGWE6CeqME!0kd<*|R{xQQhvz2D'
    'aW*eQ6uf4tm(g18zGmB>6}|Ji_v}f+45=j?8xIBlf<CYZBr&Jox-9qv;zJ?G`kBN7VOo6-C7Y#xo~VCzFo*OHY`8kL2xOzy@6O97vXR_V>Aaznu87{KW'
    'yS#}yeESB>c5jXWCWgd_9AjBCBoVrWn|@(Y?+3KEET$E&3IP35_D3RwftFa>VCqSF+%(OZ(lVTl}WJ@77WuP>OaaP21I_QoZ%eH64~hD+(a{o?whzF@~'
    '<Ic2!bPPMqhsa@k`cV_#3jluR!ecQ2$9Kf<k-7!DUX}~7UxV&xAH1xLZ6fTLT-U%9Tp$oRcR^9k*6z?TOFc3reJrGX*EFlsu^vJt&@OrwBw_<Rd6(Hl5'
    'v$l(yd*?c^mY^TZ<x#O1k9AdAJM1daGSS@0Z0@zS6neN7&9P%v*s<{b3rD5TN0UHjA!yGZ4ZV}{x_|VoV`jx-JRP&|czxO1Mio+$69uAom;eI;E&tMp>'
    '&Fhva@X4gh}9!Lk4|!c^TSLlCX~G6wdpsF&|A4l`XdR^1I0w(yhk38THS$}O|K@(@@SlBdRwL}Z0&rcIFh0C)J@=4J7BRmlA-kTskr@B#dDiUU_t=r5='
    '2if#zB)fHyYkfLewP#@yfG#BQmq2wyhX{iiWhH`MyMWOrPZejBSI$#uG%kwL!ircl-TJ-?121N#}i<N7JPDX?~jZWJ!t~RM^F2VRlh%2E{h~D#vQr^)t'
    'aBnc)9c!UI09n+a`aUvCDzxKe1l)|7tq1$#oKNU(<WAdILj2qm&)xp?nFp4ZzpZs(2M=8QQ|ME1!+3gBc&&Ui-#=2>W~X_NYbIY<46Fsi<Q;uc!~6||F'
    'oP!}IB<$gN&le%<rTivZxh4aBN1_F*L$%v5I4$QW4(CNAFXuJ~=Il)zlIs{oWvJV@3E3Rn!9-70lnF6^hm*JK4eTHIMJ<=+~o=|U!Uy$xQat{BwKHryZ'
    '1{szZXApG%Qd(ZWTf09AmSrr_2)omclO@aOUU?mz=kgN-2hG9SlCukqUFYpTgs`+o&X6?Vhv-@7wAqpMaQg}5FIPtD!zR6vNKAbQ{?Bq16kZO;<tL;V2'
    'HeCqT(+K~?W0Kg6#RXS)8doGzdt_uF{{35AMVZ}>VX{D$QHwG!9<7$l1dy?FtH;e+hP*EZcAECXYHG)EwC($&pGIu5DSUl_EGGVaLIc6LcI4H7}8$4f&'
    'kB*h78NNmQ)IpIyx<eFGd!VP(<81s|UkX)!B#QeOfHfi_;dC_ZW}KSBa4WG88Q}r+W?<?<t3t{R&~(!@T_+m59j5jD7@)e*cx0R_|r^r>Q-;fvqNi@+i'
    '#-BujfQy+ci1)PAP1ZM!vf@8x-PQo<zJFXkW8do61G2N(1FShnMr9{%Sf#Ad6b@jS_Le_HoKDjYM=rH}J}e3Tf8#i!LVpvy{@W17me;&*74fWih;?H&2'
    'h-Fw;iVr(5rZP7xDI^5sJ#{|$a5^n^p$w$^vnRF=Trv)SIpz}CoTjQ)1vO>X8sM9=Se+3)lOW5xj8;oZmbbPB1DaN;rt@u{|{SQ2$oOb'
)

# ===============================================================
# NEW STUDY: parameter list fixed before examining new outcomes.
# Regime filters are alternatives, never stacked in this study.
# Complements are separate geometries, not relaxed core thresholds.
# ===============================================================
ROOT="euraud_h1_long_regime_complement"
BUNDLE="EURAUD_H1_LONG_REGIME_COMPLEMENT_RESULTS.zip"
OUTS={k:f"{ROOT}_{k}.csv" for k in (
 "coverage","parity","regime_matrix","regime_removed_trades",
 "complement_matrix","complement_trade_attribution","candidate_trades",
 "periods","cost_stress","rolling_summary","portfolio_summary",
 "portfolio_periods","portfolio_rolling_summary","boundary_decisions","notes")}
STATUS={"state":"not_started","message":"Waiting","orders_supported":False,
        "trading_enabled":False,"pair":PAIR,"completed_variants":0}
SNAPSHOT_END=datetime(2026,9,20,18,29,tzinfo=timezone.utc)
SAMPLE_START=datetime(2005,1,4,7,45,tzinfo=timezone.utc)
NY=ZoneInfo("America/New_York")
FIXED_RRS=(3.5,4.0)
# H4/D1 higher-timeframe state is STRICTLY PREVIOUSLY COMPLETED as of
# the signal candle OPEN, not same-hour or still-forming daily candle.
REGIMES=(
 ("NO_FILTER","none",None),
 ("H4_CLOSE_GT_EMA50","h4_close_ema",50),
 ("H4_CLOSE_GT_EMA100","h4_close_ema",100),
 ("D1_CLOSE_GT_EMA100","d1_close_ema",100),
 ("D1_CLOSE_GT_EMA200","d1_close_ema",200),
 ("H4_EMA50_GT_EMA100","h4_ema_align",(50,100)),
 ("D1_EMA50_GT_EMA100","d1_ema_align",(50,100)),
 ("H4_ATR_RATIO_GE_080","h4_atr_ratio",0.80),
 ("H4_ATR_RATIO_GE_100","h4_atr_ratio",1.00),
 ("H4_ATR_RATIO_GE_120","h4_atr_ratio",1.20),
)
# Predeclared adjacent lookbacks are sensitivity checks, NOT a permission
# to select whichever happens to produce the highest historical return.
COMPLEMENTS=(
 ("ENGULF_STRUCTURE_25", "ENGULF_STRUCTURE",25),
 ("ENGULF_STRUCTURE_30", "ENGULF_STRUCTURE",30),
 ("ENGULF_STRUCTURE_40", "ENGULF_STRUCTURE",40),
 ("COMPRESSION_BREAKOUT_15", "COMPRESSION_BREAKOUT",15),
 ("COMPRESSION_BREAKOUT_20", "COMPRESSION_BREAKOUT",20),
 ("COMPRESSION_BREAKOUT_25", "COMPRESSION_BREAKOUT",25),
)
# Automatic plateau extension is restricted to H4 ATR-ratio, one new
# step each side, and only when the edge AND its neighbour are viable.
VOL_EXTENSION_LOW=("H4_ATR_RATIO_GE_060","h4_atr_ratio",0.60)
VOL_EXTENSION_HIGH=("H4_ATR_RATIO_GE_140","h4_atr_ratio",1.40)
# These are the only allowed contingent expansions for trend-EMA boundaries.
# The initial fixed 50/100 (H4) and 100/200 (D1) pairs are compared first.
TREND_EDGE_STEPS=(
 ('H4_CLOSE_GT_EMA50','H4_CLOSE_GT_EMA100',('H4_CLOSE_GT_EMA25','h4_close_ema',25)),
 ('H4_CLOSE_GT_EMA100','H4_CLOSE_GT_EMA50',('H4_CLOSE_GT_EMA150','h4_close_ema',150)),
 ('D1_CLOSE_GT_EMA100','D1_CLOSE_GT_EMA200',('D1_CLOSE_GT_EMA50','d1_close_ema',50)),
 ('D1_CLOSE_GT_EMA200','D1_CLOSE_GT_EMA100',('D1_CLOSE_GT_EMA300','d1_close_ema',300)),
)
COMP_EDGE_STEPS=(
 ('ENGULF_STRUCTURE_25','ENGULF_STRUCTURE_30',('ENGULF_STRUCTURE_15','ENGULF_STRUCTURE',15)),
 ('ENGULF_STRUCTURE_40','ENGULF_STRUCTURE_30',('ENGULF_STRUCTURE_50','ENGULF_STRUCTURE',50)),
 ('COMPRESSION_BREAKOUT_15','COMPRESSION_BREAKOUT_20',('COMPRESSION_BREAKOUT_10','COMPRESSION_BREAKOUT',10)),
 ('COMPRESSION_BREAKOUT_25','COMPRESSION_BREAKOUT_20',('COMPRESSION_BREAKOUT_30','COMPRESSION_BREAKOUT',30)),
)


def embedded_baseline():
    raw=zlib.decompress(base64.b85decode(BASELINE_B85))
    digest=hashlib.sha256(raw).hexdigest()
    if digest!=BASELINE_SHA256:
        raise RuntimeError("Embedded Portfolio27 SHA256 mismatch")
    rows=json.loads(raw)
    if len(rows)!=3029 or len({t["sid"] for t in rows})!=27:
        raise RuntimeError("Portfolio27 trade-count/strategy-count parity FAILED")
    if any(parse_time(t['entry'])>SNAPSHOT_END or parse_time(t['exit'])>SNAPSHOT_END for t in rows):
        raise RuntimeError("Portfolio baseline extends beyond frozen cutoff")
    weighted=sum(float(t['r'])*(.75 if t['sid']=='EUR_JPY_M15_SHORT' else 1.)
                 for t in rows)
    if abs(weighted-1763.8084030796124)>1e-6:
        raise RuntimeError("Portfolio27 weighted-R parity FAILED")
    return rows,digest


def ema_np(values,n):
    a=np.asarray(values,dtype=float)
    out=np.full(len(a),np.nan)
    if len(a)<n: return out
    out[n-1]=float(np.mean(a[:n]))
    alpha=2.0/(n+1)
    for i in range(n,len(a)):
        out[i]=alpha*a[i]+(1-alpha)*out[i-1]
    return out


def group_ohlc(candles,frame):
    """Aggregate H1 in UTC H4 or 17:00 NY D1 groups; DST aware for D1.
    A group becomes usable ONLY after the expected group closing instant.
    """
    groups=defaultdict(list)
    for c in candles:
        t=c['time']
        if frame=='H4':
            group=t.replace(hour=(t.hour//4)*4,minute=0,second=0,microsecond=0)
        else:
            local=t.astimezone(NY)
            # 17:00 NY belongs to the NEW daily candle.
            group=(local-timedelta(hours=17)).date()
        groups[group].append(c)
    out=[]
    for key,items in sorted(groups.items(),key=lambda p:p[0]):
        items.sort(key=lambda c:c['time'])
        if frame=='H4':
            expected=key+timedelta(hours=4)
            if len(items)!=4 or items[0]['time']!=key or items[-1]['time']+timedelta(hours=1)!=expected:
                continue
        else:
            # local day boundary at 17:00; 23h/25h on NY DST changes.
            expected=datetime.combine(key+timedelta(days=1),datetime.min.time(),NY).replace(hour=17).astimezone(timezone.utc)
            # At weekends the FX Sunday/Monday 'daily' bucket may contain fewer
            # H1 candles; require at least twelve, and a genuinely closed day.
            if len(items)<12 or items[-1]['time']+timedelta(hours=1)!=expected:
                continue
        out.append({'time':items[0]['time'],'end':expected,
                    'open':items[0]['open'],'high':max(x['high'] for x in items),
                    'low':min(x['low'] for x in items),'close':items[-1]['close']})
    return out


def htf_state(candles,frame):
    bars=group_ohlc(candles,frame)
    if len(bars)<320:
        raise RuntimeError(f"Insufficient completed {frame} bars")
    closes=np.asarray([b['close'] for b in bars])
    atr=atr14_array(bars)
    ma=rolling_previous_mean(atr,20)
    ratio=np.divide(atr,ma,out=np.full(len(bars),np.nan),
                    where=np.isfinite(ma)&(ma>0))
    # H1 signal *start* must be >= completed H4/D1 bar end.
    return {'bars':bars,'end':[b['end'] for b in bars], 'close':closes,
            'ema':{n:ema_np(closes,n) for n in (25,50,100,150,200,300)},
            'atr_ratio':ratio}


def regime_mask(f,h4,d1,kind,level):
    n=len(f['times']); selected=np.zeros(n,dtype=bool)
    if kind=='none':
        selected[:]=True
        return selected,0
    state=h4 if kind.startswith('h4') else d1
    idx=np.asarray([bisect_right(state['end'],t)-1 for t in f['times']],dtype=int)
    valid=idx>=0
    ix=np.maximum(idx,0)
    if kind.endswith('close_ema'):
        ema=state['ema'][level][ix]
        finite=np.isfinite(ema)
        selected=valid&finite&(state['close'][ix]>ema)
    elif kind.endswith('ema_align'):
        fast,slow=level
        a=state['ema'][fast][ix]; b=state['ema'][slow][ix]
        finite=np.isfinite(a)&np.isfinite(b)
        selected=valid&finite&(a>b)
    else:
        v=state['atr_ratio'][ix]
        finite=np.isfinite(v)
        selected=valid&finite&(v>=level)
    # warmup excluded separately, NOT silently counted as filter losses.
    return selected,int(np.sum(~(valid&finite)))


def complement_config(name,family,lookback,rr):
    c={'config_id':f'EURAUD28_COMP_{name}_RR{rr:.2f}',
       'timeframe':'H1','side':'LONG','family':family,'rr':rr,
       'lookback':lookback}
    if family=='ENGULF_STRUCTURE':
        c.update(br_min=1.25,body_atr_min=1.0,distance_atr_max=.10)
    else:
        c.update(compression_max=.85,body_atr_min=1.0,
                 range_atr_min=1.5,breakout_lookback=lookback)
    return c


def signal_union_backtest(f,core_raw,complement_raw,rr,cost_multiplier=1.):
    # Core priority on a shared signal candle; one pyramiding-zero timeline.
    c=make_cfg('LB25',rr)
    core=set(map(int,core_raw)); comp=set(map(int,complement_raw))
    signals=np.asarray(sorted(core|comp),dtype=int)
    trades=backtest(c,f,signals,rr=rr,cost_multiplier=cost_multiplier)
    for t in trades:
        t['trigger_id']='CORE' if t['signal_index'] in core else 'COMPLEMENT'
    return trades,{'core_raw':len(core),'complement_raw':len(comp),
                   'same_candle_both':len(core&comp),
                   'accepted_core':sum(t['trigger_id']=='CORE' for t in trades),
                   'accepted_complement':sum(t['trigger_id']=='COMPLEMENT' for t in trades)}


def attribution(core,candidate):
    base={int(t['signal_index']):t for t in core}
    other={int(t['signal_index']):t for t in candidate}
    new=set(other)-set(base);removed=set(base)-set(other);shared=set(base)&set(other)
    new_r=sum(other[i]['result_r'] for i in new)
    removed_r=sum(base[i]['result_r'] for i in removed)
    shared_delta=sum(other[i]['result_r']-base[i]['result_r'] for i in shared)
    delta=sum(t['result_r'] for t in candidate)-sum(t['result_r'] for t in core)
    assert abs(delta-new_r+removed_r-shared_delta)<1e-6
    return {'new':len(new),'removed':len(removed),'new_only_r':new_r,
            'removed_original_r':removed_r,'shared_outcome_change_r':shared_delta,
            'net_delta_r':delta,
            'new_from_2018':sum(other[i]['signal_time'].year>=2018 for i in new),
            'new_last5y':sum(other[i]['signal_time']>=SNAPSHOT_END-timedelta(days=365.25*5) for i in new),
            'new_last2y':sum(other[i]['signal_time']>=SNAPSHOT_END-timedelta(days=365.25*2) for i in new)}


def summarize_standalone(candidate,core,rr,cost4,kind,config):
    m=metrics(candidate); at=attribution(core,candidate)
    p=periods(candidate); rolls=rolling_group(monthly_rolling(candidate))
    return {'kind':kind,'config':config,'rr':rr,**m,**at,
            'cost4p_r':metrics(cost4)['total_r'],
            'cost4p_pf':metrics(cost4)['profit_factor'],
            'r_pre2010':p['PRE2010']['total_r'],
            'r_2018_plus':p['2018_PLUS']['total_r'],
            'r_last5y':p['LAST5Y']['total_r'],
            'r_last3y':p['LAST3Y']['total_r'],
            'r_last2y':p['LAST2Y']['total_r'],
            'r_last1y':p['LAST1Y']['total_r'],
            'trades_last5y':p['LAST5Y']['trades'],
            'trades_last2y':p['LAST2Y']['trades'],
            'positive_rolling24_pct':rolls[24]['positive_active_pct'],
            'positive_rolling36_pct':rolls[36]['positive_active_pct'],
            'worst_rolling24_r':rolls[24]['worst_r'],
            'worst_rolling36_r':rolls[36]['worst_r']}


def portfolio_row(t):
    return {'entry':iso(t['signal_time']+timedelta(hours=1)),
            'exit':iso(t['exit_time']+timedelta(hours=1)),
            'pair':'EUR_AUD','r':float(t['result_r']), 'rr':float(t['rr']),
            'sid':'EUR_AUD_H1_LONG','side':'BUY', 'tf':'H1',
            'signal':iso(t['signal_time'])}


def portfolio_equity(trades):
    # SAME event semantics as exact archived 27->28 runner: closed equity,
    # entry balance risk, exit BEFORE entry on equal timestamp, conservative
    # outstanding-stop-risk floor. This does not mark open trades to market.
    events=[]
    for i,t in enumerate(sorted(trades,key=lambda x:(x['entry'],x['sid']))):
        events.append((t['entry'],1,t['sid'],i,t))
        events.append((t['exit'],0,t['sid'],i,t))
    events.sort(key=lambda e:(e[0],e[1],e[2],e[3]))
    balance=100.;peak=100.;openpos={};riskopen=0.
    dd=0.;floor=0.;maxpos=0;maxrisk=0.;closes=[]
    for timestamp,event,sid,i,t in events:
        if event==0:
            risk,entry=openpos.pop(i)
            riskopen-=risk
            balance+=risk*float(t['r'])
            if balance<=0:raise RuntimeError('Nonpositive simulated balance')
            peak=max(peak,balance)
            dd=min(dd,100*(balance/peak-1))
            closes.append((parse_time(timestamp),balance))
        else:
            rp=.0075 if sid=='EUR_JPY_M15_SHORT' else .01
            risk=balance*rp
            riskopen+=risk
            openpos[i]=(risk,t)
        maxpos=max(maxpos,len(openpos))
        maxrisk=max(maxrisk,100*riskopen/balance)
        floor=min(floor,100*((balance-riskopen)/peak-1))
    return {'balance':balance,'closed_dd_pct':dd,'floor_dd_pct':floor,
            'max_open_positions':maxpos,'max_open_risk_pct':maxrisk,
            'closed_balance':closes}


def portfolio_cagr(sim,trades):
    first=min(parse_time(t['entry']) for t in trades)
    last=max(parse_time(t['exit']) for t in trades)
    years=(last-first).total_seconds()/(365.2425*86400)
    return ((sim['balance']/100.0)**(1/years)-1)*100 if years>0 else None


def portfolio_trailing_returns(sim,end):
    import bisect as _bisect
    values=sim['closed_balance']; ts=[x[0] for x in values]; balances=[x[1] for x in values]
    def at(t):
        i=_bisect.bisect_right(ts,t)-1
        return balances[i] if i>=0 else 100.0
    for years in (1,2,3,5):
        earlier=end-timedelta(days=365.2425*years)
        beginning=at(earlier);end_bal=at(end)
        total=100*(end_bal/beginning-1)
        yield {'metric_type':'COMPOUNDED','period':f'LAST{years}Y',
               'start':iso(earlier),'end':iso(end),
               'start_balance':beginning,'end_balance':end_bal,
               'total_return_pct':total,
               'annualised_return_pct':100*((end_bal/beginning)**(1/years)-1)}


def portfolio_rolling(sim,start,end):
    import bisect as _bisect
    closes=sim['closed_balance'];ts=[x[0] for x in closes];b=[x[1] for x in closes]
    def at(t):
        j=_bisect.bisect_right(ts,t)-1
        return b[j] if j>=0 else 100.
    out=[];boundary=month_floor(end)
    for months in (12,24,36):
        values=[];point=month_floor(start)
        while add_months(point,months)<=boundary:
            last=add_months(point,months)
            values.append(100*(at(last)/at(point)-1))
            point=add_months(point,1)
        out.append({'months':months,'windows':len(values),
                    'worst_pct':min(values) if values else None,
                    'median_pct':med(values) if values else None,
                    'positive_pct':100*sum(x>0 for x in values)/len(values) if values else 0.})
    return out


def study_periods(rows):
    spans=[('LAST1Y',SNAPSHOT_END-timedelta(days=365.25)),
           ('LAST2Y',SNAPSHOT_END-timedelta(days=365.25*2)),
           ('LAST3Y',SNAPSHOT_END-timedelta(days=365.25*3)),
           ('LAST5Y',SNAPSHOT_END-timedelta(days=365.25*5)),
           ('2018_PLUS',VALIDATION_START)]
    return [{'period':name,**metrics([{'result_r':float(t['r']),'duration_bars':0}
            for t in rows if parse_time(t['exit'])>=start and parse_time(t['exit'])<=SNAPSHOT_END])}
            for name,start in spans]


def candidate_at_cutoff(trades):
    # No premature credit for trades not closed by common archive cutoff.
    return [t for t in trades if SAMPLE_START <= t['signal_time']
            and t['signal_time']+timedelta(hours=1)<=SNAPSHOT_END
            and t['exit_time']+timedelta(hours=1)<=SNAPSHOT_END]


def plateau_edge_viable(row,near):
    """Only trigger predeclared extra volatility points if BOTH neighbours
    show independently defensible positive conditions, not a lone spike."""
    return (row['trades']>=40 and near['trades']>=40
            and row['profit_factor']>=1.35 and near['profit_factor']>=1.35
            and row['cost4p_r']>0 and near['cost4p_r']>0
            and row['r_2018_plus']>0 and near['r_2018_plus']>0
            and row['positive_rolling36_pct']>=65 and near['positive_rolling36_pct']>=65
            and row['total_r']>=.85*near['total_r'])


def run_research():
    try:
        STATUS.update(state='loading_baseline',message='Checking immutable 3029-trade portfolio')
        baseline,digest=embedded_baseline()
        sim0=portfolio_equity(baseline)
        expectations={'balance':1731448888.7485507,
                      'closed_dd_pct':-17.089509091188603,
                      'floor_dd_pct':-17.84461250071128,
                      'max_open_positions':6}
        for key,want in expectations.items():
            got=sim0[key]
            if abs(got-want)>1e-6:
                raise RuntimeError(f'Baseline {key} parity failed: {got} vs {want}')
        write_csv(OUTS['parity'],[{'check':'embedded_baseline_sha256','actual':digest,'expected':BASELINE_SHA256,'pass':True}]+[
            {'check':k,'actual':sim0[k],'expected':v,'pass':True} for k,v in expectations.items()])
        STATUS.update(state='fetching',message='Fetching complete EUR/AUD H1 midpoint history')
        candles=fetch_history('H1',REQUESTED_START,NOW,180)
        if len(candles)<REF_CANDLES:raise RuntimeError('Insufficient EUR/AUD history')
        _,prior_parity=reference_parity(candles)
        # Store reference parity separately from snapshot portfolio parity.
        write_csv(OUTS['parity'],[{'check':'embedded_baseline_sha256','actual':digest,'expected':BASELINE_SHA256,'pass':True}]+
             [{'check':k,'actual':sim0[k],'expected':v,'pass':True} for k,v in expectations.items()]+
             [{'check':'signal_'+row['test'],**{key:row[key] for key in ('actual','expected','pass')}} for row in prior_parity])
        STATUS.update(state='features',message='Building causal H1 and completed-H4/D1 features')
        f=features_for_plateau(candles)
        for lb in (10,15,25,30,40,50):
            for key,values,want_max in [('prev_lows',f['low'],False),('prev_highs',f['high'],True)]:
                if lb not in f[key]: f[key][lb]=rolling_previous_extreme(values,lb,want_max)
        h4=htf_state(candles,'H4');d1=htf_state(candles,'D1')
        write_csv(OUTS['coverage'],[{'h1_candles':len(candles),'h4_complete':len(h4['bars']),
              'd1_complete':len(d1['bars']), 'first_h1':iso(candles[0]['time']),
              'last_h1':iso(candles[-1]['time']),'portfolio_cutoff':iso(SNAPSHOT_END),
              'reference_cutoff':iso(REF_LAST),'archived_baseline_count':len(baseline)}])
        rows_reg=[]; rows_comp=[];removed=[];attributions=[];trades_out=[]
        period_rows_out=[];cost_rows=[];roll_rows=[];portfolio=[];pr_periods=[];pr_rolls=[];bound=[]
        portfolio.append({'config':'CONTROL27',**{k:v for k,v in sim0.items() if k!='closed_balance'},
                          'historical_cagr_pct':portfolio_cagr(sim0,baseline),
                          'accepted_trades':len(baseline),'candidate_trades':0,'incremental_r':0.})
        if abs(portfolio_cagr(sim0,baseline)-115.53348121349032)>1e-7:
            raise RuntimeError('Baseline CAGR parity failed')
        for r in portfolio_trailing_returns(sim0,SNAPSHOT_END):
            pr_periods.append({'config':'CONTROL27',**r})
        for r in portfolio_rolling(sim0,SAMPLE_START,SNAPSHOT_END):pr_rolls.append({'config':'CONTROL27',**r})
        for r in study_periods(baseline):pr_periods.append({'config':'CONTROL27','metric_type':'TRADE_R_BY_EXIT',**r})
        cache={};rows_by_rr={}
        raw=refined_signals(make_cfg('LB25',4.0),f)
        for rr in FIXED_RRS:
            cfg=make_cfg('LB25',rr)
            core=backtest(cfg,f,raw,rr=rr)
            core4=backtest(cfg,f,raw,rr=rr,cost_multiplier=2.)
            # Full available history must still reproduce the original cutoff.
            cutoff=bisect_right(f['times'],REF_LAST)
            historical_f=features_for_plateau(candles[:cutoff])
            historical_cfg=make_cfg('LB25',rr)
            historic=backtest(historical_cfg,historical_f,refined_signals(historical_cfg,historical_f),rr=rr)
            want={3.5:(106,53.647915196669246),4.0:(105,57.90421353631645)}[rr]
            observed=metrics(historic)
            if observed['trades']!=want[0] or abs(observed['total_r']-want[1])>1e-7:
                raise RuntimeError(f'25-bar RR{rr} hard parity failed')
            rows_by_rr[rr]=(cfg,core,core4)
            cache[f'CORE25_RR{rr:.2f}']=(core,core4)
            STATUS.update(state='evaluating',message=f'RR {rr}: frozen core and regime filters')
            def process(label,kind,level,extension=False):
                filt,warm=regime_mask(f,h4,d1,kind,level)
                kept=raw[filt[raw]]
                candidate=backtest(cfg,f,kept,rr=rr)
                cand4=backtest(cfg,f,kept,rr=rr,cost_multiplier=2.)
                row=summarize_standalone(candidate,core,rr,cand4,'REGIME',label)
                deleted={int(t['signal_index']):t for t in core}
                saved={int(t['signal_index']) for t in candidate}
                discarded=[t for i,t in deleted.items() if i not in saved]
                row.update(regime_kind=kind,threshold=level,raw_retained=len(kept),
                           original_raw=len(raw),warmup_unavailable_h1=warm,
                           excluded_original_accepted=len(discarded),
                           excluded_original_r=sum(t['result_r'] for t in discarded),
                           extension=extension)
                rows_reg.append(row)
                for t in discarded:removed.append({'config':label,'rr':rr,**compact(t)})
                return row,candidate,cand4
            current={}
            for label,kind,level in REGIMES:
                row,cand,c4=process(label,kind,level)
                current[label]=(row,cand,c4)
                cache[f'{label}_RR{rr:.2f}']=(cand,c4)
                STATUS['completed_variants']+=1
            # Preserve both edge/neighbour results for a transparent decision.
            for edge,neighbor,extra,edge_name in [
                ('H4_ATR_RATIO_GE_080','H4_ATR_RATIO_GE_100',VOL_EXTENSION_LOW,'LOW'),
                ('H4_ATR_RATIO_GE_120','H4_ATR_RATIO_GE_100',VOL_EXTENSION_HIGH,'HIGH')]:
                edge_row=current[edge][0];near_row=current[neighbor][0]
                gate=plateau_edge_viable(edge_row,near_row)
                bound.append({'rr':rr,'axis':'H4_ATR_RATIO','edge':edge,'neighbor':neighbor,
                              'edge_total_r':edge_row['total_r'],'neighbor_total_r':near_row['total_r'],
                              'extension_trigger':gate,'extension_point':extra[0],
                              'decision':'PREDECLARED_EXTENSION' if gate else 'STOP_AT_EDGE'})
                if gate:
                    row,cand,c4=process(*extra,extension=True)
                    cache[f'{extra[0]}_RR{rr:.2f}']=(cand,c4)
                    STATUS['completed_variants']+=1
                    if plateau_edge_viable(row,edge_row):
                        bound.append({'rr':rr,'axis':'H4_ATR_RATIO','edge':extra[0],
                            'decision':'UNRESOLVED_AT_DECLARED_CAP_NO_FURTHER_SEARCH'})
            for edge,neighbor,extra in TREND_EDGE_STEPS:
                edge_row=current[edge][0];near_row=current[neighbor][0]
                gate=plateau_edge_viable(edge_row,near_row)
                bound.append({'rr':rr,'axis':'TREND_EMA','edge':edge,'neighbor':neighbor,
                              'extension_trigger':gate,'extension_point':extra[0],
                              'decision':'PREDECLARED_EXTENSION' if gate else 'STOP_AT_EDGE'})
                if gate:
                    row,cand,c4=process(*extra,extension=True)
                    cache[f'{extra[0]}_RR{rr:.2f}']=(cand,c4)
                    STATUS['completed_variants']+=1
                    if plateau_edge_viable(row,edge_row):
                        bound.append({'rr':rr,'axis':'TREND_EMA','edge':extra[0],
                            'decision':'UNRESOLVED_AT_DECLARED_CAP_NO_FURTHER_SEARCH'})
            STATUS.update(state='evaluating',message=f'RR {rr}: independent complement signal unions')
            complement_by_name={}
            def process_comp(name,family,lb,extension=False):
                cc=complement_config(name,family,lb,rr)
                sig=signal_indices(cc,f)
                union,counts=signal_union_backtest(f,raw,sig,rr)
                union4,_=signal_union_backtest(f,raw,sig,rr,cost_multiplier=2.)
                row=summarize_standalone(union,core,rr,union4,'UNION',name)
                accepted=[t for t in union if t['trigger_id']=='COMPLEMENT']
                core_survived=[t for t in union if t['trigger_id']=='CORE']
                core_kept={t['signal_index'] for t in core_survived}
                displaced=[t for t in core if t['signal_index'] not in core_kept]
                row.update(**counts,complement_accepted_r=sum(t['result_r'] for t in accepted),
                           complement_accepted_pf=metrics(accepted)['profit_factor'],
                           complement_last5y=sum(t['signal_time']>=SNAPSHOT_END-timedelta(days=365.25*5) for t in accepted),
                           displaced_core_r=sum(t['result_r'] for t in displaced),
                           displaced_core_count=len(displaced),extension=extension)
                rows_comp.append(row);attributions.append({'config':name,'rr':rr,**counts,**attribution(core,union)})
                cache[f'{name}_RR{rr:.2f}']=(union,union4)
                STATUS['completed_variants']+=1
                return row
            for name,family,lb in COMPLEMENTS:
                complement_by_name[name]=process_comp(name,family,lb)
            for edge,neighbor,extra in COMP_EDGE_STEPS:
                a=complement_by_name[edge];b=complement_by_name[neighbor]
                # Edge continuation needs genuinely profitable NEW trades;
                # merely increasing combined historical R is insufficient.
                def comp_viable(z):
                    return (z['accepted_complement']>=15 and z['complement_accepted_r']>0
                            and z['complement_accepted_pf']>=1.25
                            and z['cost4p_r']>0 and z['new_last5y']>=3)
                gate=comp_viable(a) and comp_viable(b) and a['complement_accepted_r']>=.80*b['complement_accepted_r']
                bound.append({'rr':rr,'axis':'COMPLEMENT_GEOMETRY','edge':edge,'neighbor':neighbor,
                              'extension_trigger':gate,'extension_point':extra[0],
                              'decision':'PREDECLARED_EXTENSION' if gate else 'STOP_AT_EDGE'})
                if gate:
                    row=process_comp(*extra,extension=True)
                    if comp_viable(row):
                        bound.append({'rr':rr,'axis':'COMPLEMENT_GEOMETRY','edge':extra[0],
                            'decision':'UNRESOLVED_AT_DECLARED_CAP_NO_FURTHER_SEARCH'})
            STATUS.update(state='portfolio',message=f'RR {rr}: exact frozen baseline equity replay')
        # Portfolio calculations are deliberately done on ALL candidates,
        # without choosing a winner or tuning to its portfolio score.
        for name,(candidate,cand4) in cache.items():
            rr=next(float(x) for x in name.split('RR')[-1:])
            cleaned=candidate_at_cutoff(candidate)
            rows=sorted(baseline+[portfolio_row(t) for t in cleaned],key=lambda t:(t['entry'],t['sid']))
            incumbent=[t for t in rows if t['sid']!='EUR_AUD_H1_LONG']
            if incumbent!=baseline:
                raise RuntimeError(f'{name}: incumbent accepted-trade parity failure')
            sim=portfolio_equity(rows)
            portfolio.append({'config':name,'rr':rr,'accepted_trades':len(rows),
                              'candidate_trades':len(cleaned),
                              'incremental_r':sum(t['result_r'] for t in cleaned),
                              'historical_cagr_pct':portfolio_cagr(sim,rows),
                              **{k:v for k,v in sim.items() if k!='closed_balance'}})
            for r in portfolio_rolling(sim,SAMPLE_START,SNAPSHOT_END):pr_rolls.append({'config':name,**r})
            for r in portfolio_trailing_returns(sim,SNAPSHOT_END):pr_periods.append({'config':name,**r})
            for r in study_periods(rows):pr_periods.append({'config':name,'metric_type':'TRADE_R_BY_EXIT',**r})
            for t in candidate:trades_out.append({'config':name,**compact(t)})
            for label,trades in [('2P',candidate),('4P',cand4)]:
                cost_rows.append({'config':name,'assumed_adverse_pips':2 if label=='2P' else 4,
                                  **metrics(trades)})
            for label,stats in periods(candidate).items():
                period_rows_out.append({'config':name,'period':label,**stats})
            for months,rg in rolling_group(monthly_rolling(candidate)).items():
                roll_rows.append({'config':name,'months':months,**rg})
        write_csv(OUTS['regime_matrix'],rows_reg)
        write_csv(OUTS['regime_removed_trades'],removed)
        write_csv(OUTS['complement_matrix'],rows_comp)
        write_csv(OUTS['complement_trade_attribution'],attributions)
        write_csv(OUTS['candidate_trades'],trades_out)
        write_csv(OUTS['periods'],period_rows_out)
        write_csv(OUTS['cost_stress'],cost_rows)
        write_csv(OUTS['rolling_summary'],roll_rows)
        write_csv(OUTS['portfolio_summary'],portfolio)
        write_csv(OUTS['portfolio_periods'],pr_periods)
        write_csv(OUTS['portfolio_rolling_summary'],pr_rolls)
        write_csv(OUTS['boundary_decisions'],bound)
        write_csv(OUTS['notes'],[
          {'topic':'STATUS','detail':'EXPLORATORY: no promotion, no live submission, no change to Portfolio27'},
          {'topic':'CONTROL','detail':'25-bar LONG sweep, body>=1 ATR14, lower-wick/body>=0.25, RR3.5 and 4.0 fixed'},
          {'topic':'HTF_CAUSALITY','detail':'H4/D1 groups must be complete by H1 signal OPEN; D1 17:00 NY DST aware'},
          {'topic':'COST','detail':'EUR/AUD H1 midpoint 2pip adverse entry; 4pip variant changes EUR/AUD only, not all 27 incumbent costs'},
          {'topic':'PERIOD','detail':'All available history was inspected in prior discovery; 2018+ and last1/2/3/5y are NOT untouched OOS'},
          {'topic':'PORTFOLIO','detail':'Archived 3029 accepted incumbent trades frozen at 2026-09-20 18:29Z; only closed EUR/AUD candidates by this cutoff credited'},
          {'topic':'PORTFOLIO_RISK','detail':'Historical risk 1% at entry on realised balance (EURJPY M15 SHORT 0.75%); live sizes NAV, so differs'},
          {'topic':'LIMITATIONS','detail':'No interpair currency exposure cap; intrabar both-touched heuristic unchanged; no historical executable bid-ask verification'},
          {'topic':'COMPLEMENT','detail':'Core priority on same candle, exit-candle signal eligible; one pyramiding-zero EUR/AUD chronology'},
          {'topic':'AVOID_OVERFIT','detail':'No combined filters, no new RR/body/wick search; only predeclared one-step capped extensions for H4 volatility, trend EMA, complement lookback'}])
        pack()
        STATUS.update(state='complete',message='Download results ZIP; live 27 unchanged',
                      result_file=BUNDLE,parity_passed=True,
                      completed_variants=len(cache),portfolio_baseline_trades=len(baseline))
    except Exception as e:
        import traceback
        STATUS.update(state='failed',message=f'{type(e).__name__}: {e}',
                      traceback=traceback.format_exc()[-3500:])
        # Do not zip partial result files as if they were complete.

RESEARCH_LOCK=threading.Lock()
RESEARCH_STARTED=False

def launch_research():
    global RESEARCH_STARTED
    with RESEARCH_LOCK:
        if RESEARCH_STARTED:return False
        RESEARCH_STARTED=True
        threading.Thread(target=run_research,daemon=True).start()
        return True

@app.route('/')
def root():
    return jsonify({'study':'EUR/AUD H1 LONG regime and complement',
                    'read_only':True,'orders_supported':False,
                    'status_route':'/euraud-h1-long-improvement/status',
                    'results_route':'/euraud-h1-long-improvement/results'})

@app.route('/euraud-h1-long-improvement/start')
def start():
    return jsonify({'started_now':launch_research(),'state':STATUS['state']})

@app.route('/euraud-h1-long-improvement/status')
def status():
    return jsonify(STATUS)

@app.route('/euraud-h1-long-improvement/results')
def results():
    return download(BUNDLE)

if __name__=='__main__':
    launch_research()
    app.run(host='0.0.0.0',port=int(os.getenv('PORT','5000')),debug=False)
