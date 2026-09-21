"""EUR/AUD H1 LONG sweep/displacement — controlled one-factor refinement.

READ ONLY research. This script does not import any live executor and never sends orders.
Use OANDA_TOKEN on a SEPARATE Railway research service. Fetches EUR/AUD H1 midpoint history.

Predeclared control: LONG; low < previous 30-bar low, bullish close > previous
H1 high, bullish body >= 1.00 ATR14 (Wilder/RMA), lower wick / body >= 0.25.
Reference entry = completed signal close; stop = signal low - 10 ticks;
2-pip adverse historical fill (4-pip doubled-cost check); target anchored to
REFERENCE-entry risk; per-config pyramiding 0 and exit-candle signal eligible.

40 configurations = control + nine ONE-FACTOR geometry/context changes,
each at RR 2.50, 3.00, 3.50, 4.00. No interaction grid, no optimisation
of an existing live strategy, no session/weekday/HTF hindsight filters.
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
BUNDLE="EURAUD_H1_LONG_CONTROLLED_REFINEMENT_RESULTS.zip"
ROOT="euraud_h1_long_controlled_refinement"
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
}
STATUS={"state":"not_started","message":"Waiting","pair":PAIR,
        "orders_supported":False,"trading_enabled":False,
        "tested_configurations":40, "parity_passed":False}

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


# ============================================================
# FROZEN EXPERIMENT PLAN. ONLY ONE GEOMETRY FIELD CHANGES.
# ============================================================
CONTROL = dict(timeframe="H1", side="LONG", family="SWEEP_DISPLACEMENT",
               lookback=30, body_atr_min=1.00, wick_body_min=0.25,
               close_location_min=None)
VARIANTS = [
    ("CONTROL", None, None),
    ("LB25", "lookback", 25),
    ("LB35", "lookback", 35),
    ("LB40", "lookback", 40),
    ("BODY090", "body_atr_min", 0.90),
    ("BODY110", "body_atr_min", 1.10),
    ("WICK015", "wick_body_min", 0.15),
    ("WICK035", "wick_body_min", 0.35),
    ("CLOSE070", "close_location_min", 0.70),
    ("CLOSE080", "close_location_min", 0.80),
]
assert len(VARIANTS) * len(RR_GRID) == 40


def plan():
    for label, key, value in VARIANTS:
        for rr in RR_GRID:
            conf = dict(CONTROL)
            if key is not None:
                conf[key] = value
            conf["rr"] = rr
            conf["variant"] = label
            conf["change_field"] = key or "NONE"
            conf["change_value"] = value
            conf["config_id"] = f"EURAUD_H1_LONG_{label}_RR{rr:.2f}"
            yield conf


def features_for_refinement(candles):
    f = build_features(candles, "H1")
    for lookback in (25, 35, 40):
        f["prev_lows"][lookback] = rolling_previous_extreme(
            f["low"], lookback, want_max=False)
    return f


def refined_signals(config, features):
    raw = signal_indices(config, features)
    minimum = config.get("close_location_min")
    if minimum is not None:
        raw = raw[features["close_location"][raw] >= minimum]
    return raw


def fingerprint(trades):
    lines = [f"{int(t['signal_index'])}|{int(t['exit_index'])}|"
             f"{t['exit_reason']}|{float(t['result_r']):.8f}" for t in trades]
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()


def reference_parity(all_candles):
    # Make sure corrections in OANDA history or changes in the historical
    # simulator do NOT quietly turn the discovery benchmark into a new model.
    times = [c["time"] for c in all_candles]
    cutoff = bisect_right(times, REF_LAST)
    rows = [
        {"test":"frozen_first_candle", "observed":iso(times[0]),
         "expected":iso(REF_FIRST), "pass":times[0]==REF_FIRST},
        {"test":"frozen_last_candle", "observed":iso(times[cutoff-1]) if cutoff else None,
         "expected":iso(REF_LAST), "pass":bool(cutoff) and times[cutoff-1]==REF_LAST},
        {"test":"frozen_candle_count", "observed":cutoff,
         "expected":REF_CANDLES, "pass":cutoff==REF_CANDLES},
    ]
    if not all(r["pass"] for r in rows):
        write_csv(OUTS["parity"],rows)
        raise RuntimeError("FROZEN COVERAGE PARITY FAILED: inspect parity CSV, do not compare refinements")
    f = features_for_refinement(all_candles[:cutoff])
    for rr, expected in REF_METRICS.items():
        cfg = dict(CONTROL,rr=rr, config_id=f"FROZEN_CONTROL_RR{rr}")
        raw = refined_signals(cfg,f)
        trades = backtest(cfg,f,raw,rr=rr,cost_multiplier=1.0)
        m = metrics(trades)
        for name in ("trades","winners","total_r","profit_factor"):
            observed=m[name]
            want=expected[name]
            passed=(abs(observed-want)<1e-7 if isinstance(want,float)
                    else observed==want)
            rows.append({"test":f"RR{rr}_{name}","observed":observed,
                         "expected":want,"pass":passed})
        sig=fingerprint(trades)
        rows.append({"test":f"RR{rr}_trade_fingerprint","observed":sig,
                     "expected":expected["fingerprint"],
                     "pass":sig==expected["fingerprint"]})
    write_csv(OUTS["parity"],rows)
    if not all(x["pass"] for x in rows):
        raise RuntimeError("FROZEN DISCOVERY TRADE PARITY FAILED: inspect parity CSV")
    return cutoff, rows


def details(trades):
    return metrics(trades)


def periods(trades):
    spans = [
        ("FULL",None,None),
        ("PRE_2010",None,PRE2010_END),
        ("2010_PLUS",PRE2010_END,None),
        ("PRE_2018",None,VALIDATION_START),
        ("2018_PLUS",VALIDATION_START,None),
        ("2020_PLUS",datetime(2020,1,1,tzinfo=timezone.utc),None),
        ("LAST_5Y",NOW-timedelta(days=365.25*5),None),
        ("LAST_3Y",NOW-timedelta(days=365.25*3),None),
        ("LAST_2Y",NOW-timedelta(days=365.25*2),None),
        ("LAST_1Y",NOW-timedelta(days=365.25),None),
        ("2004_2009",None,PRE2010_END),
        ("2010_2015",PRE2010_END,datetime(2016,1,1,tzinfo=timezone.utc)),
        ("2016_2021",datetime(2016,1,1,tzinfo=timezone.utc),
         datetime(2022,1,1,tzinfo=timezone.utc)),
        ("2022_NOW",datetime(2022,1,1,tzinfo=timezone.utc),None),
    ]
    return {name:period_metrics(trades,a,b) for name,a,b in spans}


def completed_years(trades, anchor=2005):
    out=[]
    for y in range(anchor,NOW.year+1):
        a=datetime(y,1,1,tzinfo=timezone.utc)
        b=datetime(y+1,1,1,tzinfo=timezone.utc)
        out.append({"year":y,"year_complete":b<=NOW,
                    **period_metrics(trades,a,b)})
    return out


def monthly_rolling(trades):
    # FIXED windows anchored to the same first completed whole calendar month
    # for ALL candidates, including windows with zero trades.
    anchor=datetime(2005,1,1,tzinfo=timezone.utc)
    last_whole=month_floor(NOW)
    out=[]
    for length in (12,24,36):
        t=anchor
        while add_months(t,length)<=last_whole:
            end=add_months(t,length)
            m=period_metrics(trades,t,end)
            out.append({"window_months":length,"start":iso(t),"end":iso(end),
                        "active":m["trades"]>0,**m})
            t=add_months(t,1)
    return out


def rolling_group(rolls):
    out={}
    for length in (12,24,36):
        rows=[x for x in rolls if x["window_months"]==length]
        active=[x for x in rows if x["active"]]
        positives=[x for x in active if x["total_r"]>0]
        out[length]={"windows":len(rows),"active_windows":len(active),
                     "positive_active_pct":100*len(positives)/len(active) if active else 0.0,
                     "zero_trade_windows":len(rows)-len(active),
                     "worst_r":min((x["total_r"] for x in rows),default=0.0),
                     "median_r":median(x["total_r"] for x in rows) if rows else 0.0}
    return out


def compact(t):
    return {k:(iso(v) if isinstance(v,datetime) else v) for k,v in t.items()}


def trade_attribution(control,candidate,cfg):
    # SAME-RR comparator: new-only / dropped-only / shared changed outcomes.
    ca={int(t["signal_index"]):t for t in candidate}
    co={int(t["signal_index"]):t for t in control}
    new=sorted(set(ca)-set(co))
    dropped=sorted(set(co)-set(ca))
    common=sorted(set(ca)&set(co))
    def subtotal(keys,book):return sum(book[x]["result_r"] for x in keys)
    new_r=subtotal(new,ca)
    dropped_r=subtotal(dropped,co)
    shared_delta=sum(ca[i]["result_r"]-co[i]["result_r"] for i in common)
    total_delta=sum(x["result_r"] for x in candidate)-sum(x["result_r"] for x in control)
    if abs(total_delta-(new_r-dropped_r+shared_delta))>1e-7:
        raise RuntimeError(f"Incremental R reconciliation failed for {cfg['config_id']}")
    changes=[]
    for i in new:
        changes.append({"change":"NEW_ACCEPTED","signal_index":i,
                        "control_r":None,"candidate_r":ca[i]["result_r"],
                        "delta_r":ca[i]["result_r"],**compact(ca[i])})
    for i in dropped:
        changes.append({"change":"DROPPED_ACCEPTED","signal_index":i,
                        "control_r":co[i]["result_r"],"candidate_r":None,
                        "delta_r":-co[i]["result_r"],**compact(co[i])})
    for i in common:
        delta=ca[i]["result_r"]-co[i]["result_r"]
        if abs(delta)>1e-8 or ca[i]["exit_index"]!=co[i]["exit_index"]:
            changes.append({"change":"SHARED_CHANGED_OUTCOME","signal_index":i,
                            "control_r":co[i]["result_r"],
                            "candidate_r":ca[i]["result_r"],
                            "delta_r":delta,**compact(ca[i])})
    for x in changes:
        x["config_id"]=cfg["config_id"]
        x["variant"]=cfg["variant"]
        x["rr"]=cfg["rr"]
    def count_recent(indices,book,start):
        return sum(book[i]["signal_time"]>=start for i in indices)
    recent=NOW-timedelta(days=365.25*5)
    return ({"new_accepted":len(new),"dropped_accepted":len(dropped),
             "shared_accepted":len(common),"new_r":new_r,
             "dropped_original_r":dropped_r,"shared_outcome_delta_r":shared_delta,
             "net_delta_r":total_delta,
             "new_last5y":count_recent(new,ca,recent),
             "dropped_last5y":count_recent(dropped,co,recent),
             "new_2018_plus":count_recent(new,ca,VALIDATION_START),
             "dropped_2018_plus":count_recent(dropped,co,VALIDATION_START)},changes)


def run_research():
    try:
        STATUS.update(state="fetching",message="Fetching full EUR/AUD H1 midpoint history")
        candles=fetch_history("H1",REQUESTED_START,NOW,chunk_days=180)
        if not candles or len(candles)<REF_CANDLES:
            raise RuntimeError(f"Too few EUR/AUD candles: {len(candles)}")
        write_csv(OUTS["coverage"],[{
            "instrument":PAIR,"candles":len(candles),
            "first_utc":iso(candles[0]["time"]),
            "last_utc":iso(candles[-1]["time"]),
            "reference_cutoff":iso(REF_LAST),
            "assumed_entry_cost_pips":H1_PRIMARY_COST_PIPS,
            "stress_entry_cost_pips":2*H1_PRIMARY_COST_PIPS,
            "read_only":True,"trading_enabled":False}])
        STATUS.update(state="parity",message="Reproducing original discovery's 96 trades at RR2.5 and RR4.0")
        cutoff,parity=reference_parity(candles)
        STATUS["parity_passed"]=True
        STATUS["parity_checks"]=len(parity)
        STATUS.update(state="features",message="Building full H1 features after successful parity")
        f=features_for_refinement(candles)
        rows=[];period_rows=[];cost_rows=[];year_rows=[];rolling_rows=[]
        rolling_summaries=[];attrib_rows=[];diff_rows=[];trade_rows=[]
        controls={}
        for rr in RR_GRID:
            cfg=next(c for c in plan() if c["variant"]=="CONTROL" and c["rr"]==rr)
            controls[rr]=backtest(cfg,f,refined_signals(cfg,f),rr=rr,cost_multiplier=1.0)
        for n,cfg in enumerate(plan(),1):
            STATUS.update(state="matrix",message=f"{n}/40 — {cfg['config_id']}",completed=n-1)
            raw=refined_signals(cfg,f)
            accepted=backtest(cfg,f,raw,rr=cfg["rr"],cost_multiplier=1.0)
            base=controls[cfg["rr"]]
            attribution,changed=trade_attribution(base,accepted,cfg)
            diff_rows.extend(changed)
            p=periods(accepted)
            control_p=periods(base)
            for period,stats in p.items():
                period_rows.append({"config_id":cfg["config_id"],"variant":cfg["variant"],
                                    "rr":cfg["rr"],"period":period,**stats})
            cost_metrics={}
            for multiplier in COST_MULTIPLIERS:
                # Recompute actual R under 2 vs 4 adverse pips: same raw signals
                # and targets, not artificial multiplication of P/L after the fact.
                trades=(accepted if multiplier==1.0 else backtest(
                    cfg,f,raw,rr=cfg["rr"],cost_multiplier=multiplier))
                m=metrics(trades)
                cost_metrics[multiplier]=m
                cost_rows.append({"config_id":cfg["config_id"],
                                  "variant":cfg["variant"],"rr":cfg["rr"],
                                  "adverse_pips":H1_PRIMARY_COST_PIPS*multiplier,
                                  **m})
            years=completed_years(accepted)
            for y in years:
                year_rows.append({"config_id":cfg["config_id"],"rr":cfg["rr"],
                                  "variant":cfg["variant"],**y})
            rolls=monthly_rolling(accepted)
            for r in rolls:
                rolling_rows.append({"config_id":cfg["config_id"],
                                     "variant":cfg["variant"],"rr":cfg["rr"],**r})
            rolling=rolling_group(rolls)
            for length,stat in rolling.items():
                rolling_summaries.append({"config_id":cfg["config_id"],
                                          "variant":cfg["variant"],"rr":cfg["rr"],
                                          "months":length,**stat})
            complete_years=[y for y in years if y["year_complete"]]
            positive_years=sum(y["total_r"]>0 for y in complete_years)
            zero_years=sum(y["trades"]==0 for y in complete_years)
            base_m=metrics(base)
            rows.append({"config_id":cfg["config_id"],"variant":cfg["variant"],
                         "change_field":cfg["change_field"],
                         "change_value":cfg["change_value"],"rr":cfg["rr"],
                         "raw_signals":len(raw),"accepted":len(accepted),
                         **{f"full_{k}":v for k,v in metrics(accepted).items()},
                         "control_accepted_same_rr":len(base),
                         "control_r_same_rr":base_m["total_r"],
                         **attribution,
                         "r_pre2010":p["PRE_2010"]["total_r"],
                         "r_post2010":p["2010_PLUS"]["total_r"],
                         "r_2018_plus":p["2018_PLUS"]["total_r"],
                         "r_last5y":p["LAST_5Y"]["total_r"],
                         "trades_last5y":p["LAST_5Y"]["trades"],
                         "r_last3y":p["LAST_3Y"]["total_r"],
                         "r_last2y":p["LAST_2Y"]["total_r"],
                         "trades_last2y":p["LAST_2Y"]["trades"],
                         "r_last1y":p["LAST_1Y"]["total_r"],
                         "cost4p_r":cost_metrics[2.0]["total_r"],
                         "cost4p_pf":cost_metrics[2.0]["profit_factor"],
                         "positive_complete_years":positive_years,
                         "complete_years":len(complete_years),
                         "zero_trade_complete_years":zero_years,
                         "rolling12_positive_active_pct":rolling[12]["positive_active_pct"],
                         "rolling24_positive_active_pct":rolling[24]["positive_active_pct"],
                         "rolling36_positive_active_pct":rolling[36]["positive_active_pct"],
                         "rolling12_worst_r":rolling[12]["worst_r"],
                         "rolling24_worst_r":rolling[24]["worst_r"],
                         "rolling36_worst_r":rolling[36]["worst_r"]})
            attrib_rows.append({"config_id":cfg["config_id"],"variant":cfg["variant"],
                                "rr":cfg["rr"],**attribution,
                                "control_last5y_r":control_p["LAST_5Y"]["total_r"],
                                "candidate_last5y_r":p["LAST_5Y"]["total_r"]})
            for t in accepted:
                trade_rows.append({"variant":cfg["variant"],"rr":cfg["rr"],
                                   **compact(t)})
        STATUS.update(state="writing",message="Writing metrics, trade differences, rolling and calendar diagnostics")
        for key,data in (("matrix",rows),("periods",period_rows),
                         ("costs",cost_rows),("years",year_rows),
                         ("rolling",rolling_rows),("rolling_summary",rolling_summaries),
                         ("attribution",attrib_rows),("changed_trades",diff_rows),
                         ("trades",trade_rows)):
            write_csv(OUTS[key],data)
        write_csv(OUTS["notes"],[
            {"topic":"scope","note":"Controlled one-factor sweep/displacement LONG study only; 40 predeclared configurations. NO LIVE ORDERS."},
            {"topic":"baseline","note":"Frozen discovery coverage and both 96-trade RR fingerprints must match before new full-history analysis."},
            {"topic":"time_convention","note":"H1 candle OPEN = signal_time; reference entry is completed signal CLOSE; backtest exit starts NEXT H1 candle."},
            {"topic":"cost","note":"2p adverse historical fill; 4p stress. No actual historical bid/ask or gap reconstruction."},
            {"topic":"trade_comparison","note":"New/drop/shared are accepted-trade differences vs SAME RR control; shared exit/outcome changes are separate. Not a 27-to-28 portfolio test."},
            {"topic":"data_snooping","note":"All historical periods examined in discovery; 2018+ and last1/2/3/5Y are NOT untouched OOS. No automatic promotion gate."},
            {"topic":"market_exposure","note":"EUR/AUD not in live 27. No cross-pair capital, concurrency, opposite-side or NAV simulation performed."},
            {"topic":"discovery_anchor","note":"2004-05-31T20:00Z through 2026-09-21T11:00Z; 137837 completed EUR/AUD H1 candles; frozen RR2.5 and RR4.0 fingerprints."},
            {"topic":"rolling","note":"Fixed monthly windows 2005-Jan to last completed month; signal-entry attribution, post global p0, zero-trade windows retained."},
            {"topic":"other","note":"New 2026+ bars may add trades; benchmark parity is checked separately at frozen cutoff."},
        ])
        pack()
        STATUS.update(state="complete",message="40 variants complete; ZIP ready",
                      completed=40,parity_passed=True,
                      results_zip=BUNDLE,history_candles=len(candles),
                      reference_candles=cutoff)
    except Exception as exc:
        STATUS.update(state="failed",message=f"{type(exc).__name__}: {exc}")
        try:
            pack()  # Make parity and coverage diagnostics downloadable on failure.
        except Exception:
            pass


RESEARCH_LOCK=threading.Lock()
RESEARCH_STARTED=False

def launch_research():
    global RESEARCH_STARTED
    with RESEARCH_LOCK:
        if RESEARCH_STARTED:
            return False
        RESEARCH_STARTED=True
        threading.Thread(target=run_research,daemon=True,name="euraud-h1-controlled-refinement").start()
        return True


@app.route("/")
def root():
    return jsonify({"service":"EUR/AUD H1 LONG controlled refinement",
                    "pair":PAIR,"timeframe":"H1","side":"LONG",
                    "state":STATUS["state"],"orders_supported":False,
                    "trading_enabled":False,"variants":len(VARIANTS),
                    "rr_grid":RR_GRID,"configurations":40,
                    "benchmark_candles":REF_CANDLES,
                    "benchmark_cutoff":iso(REF_LAST),
                    "routes":["/euraud-h1-long-refinement/start",
                              "/euraud-h1-long-refinement/status",
                              "/euraud-h1-long-refinement/results"]})


@app.route("/euraud-h1-long-refinement/start")
def start():
    return jsonify({"started_now":launch_research(),"state":STATUS["state"],
                    "orders_supported":False})


@app.route("/euraud-h1-long-refinement/status")
def status():
    return jsonify(STATUS)


@app.route("/euraud-h1-long-refinement/results")
def results():
    return download(BUNDLE)


if __name__ == "__main__":
    launch_research()
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")),debug=False)
