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


def run_research():
    try:
        STATUS.update(state="fetching",message="Fetching EUR/AUD H1 midpoint candles")
        candles=fetch_history("H1",REQUESTED_START,NOW,chunk_days=180)
        if len(candles)<REF_CANDLES:
            raise RuntimeError("History insufficient for original locked reference")
        write_csv(OUTS["coverage"],[{"pair":PAIR,"candles":len(candles),
           "first":iso(candles[0]["time"]),"last":iso(candles[-1]["time"]),
           "cutoff":iso(REF_LAST),"base_cost_pips":2,"stress_cost_pips":4,
           "orders_supported":False,"trading_enabled":False}])
        STATUS.update(state="parity",message="Verifying frozen control AND prior finalists")
        cutoff,pr=reference_parity(candles)
        STATUS.update(parity_passed=True,parity_checks=len(pr),
                      state="features",message="Building plateau indicator arrays")
        f=features_for_plateau(candles)
        rows=[];period_rows=[];cost_rows=[];year_rows=[];roll_rows=[]
        roll_sum=[];att_rows=[];diff_rows=[];tr_rows=[];bound_rows=[];plateau_rows=[]
        memo={}; base_controls={}
        # Every comparison uses the frozen 30-bar candidate at the SAME RR.
        def evaluate(cfg):
            key=cfg["config_id"]
            if key in memo:return memo[key]
            STATUS.update(state="evaluating",message=key,completed=len(memo))
            sig=refined_signals(cfg,f)
            trades=backtest(cfg,f,sig,rr=cfg["rr"])
            stress=backtest(cfg,f,sig,rr=cfg["rr"],cost_multiplier=2.0)
            m=metrics(trades);costm=metrics(stress)
            p=periods(trades)
            rolls=monthly_rolling(trades); rg=rolling_group(rolls)
            years=[]
            for y in range(2005,NOW.year+1):
                a=datetime(y,1,1,tzinfo=timezone.utc); b=datetime(y+1,1,1,tzinfo=timezone.utc)
                years.append({"year":y,"year_complete":b<=NOW,
                              **period_metrics(trades,a,b)})
            comp_key=cfg["rr"]
            if cfg["anchor"]=="FROZEN30" and cfg["axis"]=="RR":
                base_controls[comp_key]=trades
            if comp_key not in base_controls:
                control=make_cfg("FROZEN30",comp_key)
                if control["config_id"]!=key:evaluate(control)
            assert comp_key in base_controls
            attrib=trade_attribution(base_controls[comp_key],trades)
            row={"config_id":key,"phase":cfg["phase"],"axis":cfg["axis"],
                 "anchor":cfg["anchor"],"axis_value":cfg["axis_value"],
                 "rr":cfg["rr"],"lookback":cfg["lookback"],
                 "body_atr_min":cfg["body_atr_min"],
                 "wick_body_min":cfg["wick_body_min"],
                 "close_location_min":cfg["close_location_min"],
                 "raw_signals":len(sig),
                 **{f"full_{k}":v for k,v in m.items()},
                 "cost4p_r":costm["total_r"],"cost4p_pf":costm["profit_factor"],
                 **attrib,
                 **{f"r_{name.lower()}":stats["total_r"] for name,stats in p.items() if name!="FULL"},
                 "trades_last5y":p["LAST5Y"]["trades"],
                 "trades_last2y":p["LAST2Y"]["trades"],
                 "rolling12_positive_active_pct":rg[12]["positive_active_pct"],
                 "rolling24_positive_active_pct":rg[24]["positive_active_pct"],
                 "rolling36_positive_active_pct":rg[36]["positive_active_pct"],
                 "rolling12_worst_r":rg[12]["worst_r"],
                 "rolling24_worst_r":rg[24]["worst_r"],
                 "rolling36_worst_r":rg[36]["worst_r"],
                 "positive_completed_years":sum(y["year_complete"] and y["total_r"]>0 for y in years),
                 "zero_trade_completed_years":sum(y["year_complete"] and y["trades"]==0 for y in years)}
            row["viability_screen_pass"] = viability(row)
            rows.append(row)
            for name,stats in p.items():period_rows.append({"config_id":key,"period":name,**stats})
            for multiplier,statistics in ((1,m),(2,costm)):
                cost_rows.append({"config_id":key,"adverse_pips":multiplier*H1_PRIMARY_COST_PIPS,
                                  **statistics})
            for y in years:year_rows.append({"config_id":key,**y})
            for rrrow in rolls:roll_rows.append({"config_id":key,**rrrow})
            for months,stat in rg.items():roll_sum.append({"config_id":key,"months":months,**stat})
            att_rows.append({"config_id":key,**attrib})
            bmap={int(t["signal_index"]):t for t in base_controls[comp_key]}
            cmap={int(t["signal_index"]):t for t in trades}
            for i in sorted(set(bmap)|set(cmap)):
                a=bmap.get(i); b=cmap.get(i)
                if a and b and abs(a["result_r"]-b["result_r"])<1e-8 and a["exit_index"]==b["exit_index"]:
                    continue
                diff_rows.append({"config_id":key,
                    "change":"NEW" if b and not a else "REMOVED" if a and not b else "SHARED_CHANGED",
                    "signal_index":i,"signal_time":iso((b or a)["signal_time"]),
                    "baseline_r":a["result_r"] if a else None,
                    "candidate_r":b["result_r"] if b else None,
                    "delta_r":(b["result_r"] if b else 0)-(a["result_r"] if a else 0)})
            for t in trades:tr_rows.append({"config_id":key,**compact(t)})
            memo[key]=(row,trades)
            STATUS["tested_configurations"]=len(memo)
            return memo[key]

        # Stage A: evaluate fixed core RR grid, then each single-factor axis.
        # Do not pick the best configuration to decide the next research axis.
        for anchor in GEOMETRIES:
            for rr in RR_BASE:evaluate(make_cfg(anchor,rr))
        for axis,settings in AXES.items():
            for rr in settings["rrs"]:
                for val in settings["base"]:
                    evaluate(make_cfg(settings["anchor"],rr,axis,val))
        # Stage B: symmetric independent upper/lower extensions only if
        # the adjacent TWO boundary points meet the predeclared robustness rule.
        # This guards against a single spike at the end of a grid.
        grid_list=[(f"RR_{anchor}",anchor,"RR",rr,RR_BASE,RR_LOW,RR_HIGH)
                   for anchor in GEOMETRIES for rr in (None,)]
        for axis,s in AXES.items():
            for rr in s["rrs"]:
                grid_list.append((axis,s["anchor"],axis,rr,s["base"],s["low"],s["high"]))
        for group,anchor,axis,rr,base_grid,lower_grid,upper_grid in grid_list:
            ordered=sorted(base_grid)
            for side,values in (("LOW",lower_grid),("HIGH",upper_grid)):
                edge=ordered[0] if side=="LOW" else ordered[-1]
                near=ordered[1] if side=="LOW" else ordered[-2]
                def lookup(v):
                    c=make_cfg(anchor,v) if axis=="RR" else make_cfg(anchor,rr,axis,v)
                    return evaluate(c)[0]
                er=lookup(edge); nr=lookup(near)
                trigger=close_to_neighbour(er,nr)
                extension=sorted(values) if trigger else []
                if trigger:
                    STATUS.update(state="extending",message=f"Boundary {group} {rr} {side}: predeclared expansion")
                    for v in extension:
                        c=(make_cfg(anchor,v,phase="EXTENDED") if axis=="RR" else
                           make_cfg(anchor,rr,axis,v,phase="EXTENDED"))
                        evaluate(c)
                terminal=extension[0] if side=="LOW" and extension else extension[-1] if extension else None
                terminal_near=(extension[1] if side=="LOW" and len(extension)>1 else
                               extension[-2] if side=="HIGH" and len(extension)>1 else edge)
                term_strong=(close_to_neighbour(lookup(terminal),lookup(terminal_near))
                             if terminal is not None else False)
                bound_rows.append({"group":group,"anchor":anchor,"rr":rr,
                   "axis":axis,"side":side,"base_edge":edge,"base_neighbour":near,
                   "edge_viable":viability(er),"neighbour_viable":viability(nr),
                   "extension_triggered":trigger,"extended_values":";".join(map(str,extension)),
                   "terminal_edge":terminal,"terminal_still_strong":term_strong,
                   "boundary_status":("UNRESOLVED_EDGE_AT_CAP" if term_strong else
                        "EXTENDED_INSIDE_CAP" if trigger else "NO_EXTENSION"),
                   "edge_r":er["full_total_r"],"neighbour_r":nr["full_total_r"]})
        # Plateau intervals are candidate *descriptions*, not awards/ranks.
        for anchor in GEOMETRIES:
            rr_rows=sorted((r for r in rows if r["axis"]=="RR" and r["anchor"]==anchor),
                           key=lambda r:r["axis_value"])
            # For RR the sweep axis changes rr, so axis/rr matching above is unsuitable.
            group=[]
            for r in rr_rows+[None]:
                if r is not None and viability(r):group.append(r);continue
                if len(group)>=3:
                    plateau_rows.append({"axis":"RR","anchor":anchor,"rr":"VARIES",
                       "first":group[0]["axis_value"],"last":group[-1]["axis_value"],
                       "members":len(group),"min_r":min(x["full_total_r"] for x in group),
                       "min_pf":min(x["full_profit_factor"] for x in group),
                       "min_cost4p_r":min(x["cost4p_r"] for x in group),
                       "config_ids":";".join(x["config_id"] for x in group)})
                group=[]
        for axis,s in AXES.items():
            for rr in s["rrs"]:
                plateau_rows.extend(add_plateau_rows(rows,axis,s["anchor"],rr))
        STATUS.update(state="writing",message="Writing plateau and trade-level audit files")
        for k,v in (("matrix",rows),("periods",period_rows),("costs",cost_rows),
             ("years",year_rows),("rolling",roll_rows),("rolling_summary",roll_sum),
             ("attribution",att_rows),("changed_trades",diff_rows),("trades",tr_rows),
             ("boundary",bound_rows),("plateaus",plateau_rows)):
            write_csv(OUTS[k],v)
        write_csv(OUTS["notes"],[
          {"topic":"scope","note":"READ ONLY; no live #27 changes; this is NOT a 27-to-28 portfolio-addition test."},
          {"topic":"parity","note":"Frozen 30-bar 96-trade RR2.5/4.0 AND prior LB25/CLOSE080 finalist fingerprints mandatory."},
          {"topic":"axis isolation","note":"Single-factor sweeps only, with LB25 as an already studied alternative; CLOSE080 only on original 30-bar."},
          {"topic":"boundary rule","note":"Extend low and high separately when edge + nearest interior point both viable, edge R >=85% and PF >=90% of neighbour. Caps predeclared; unresolved outer edge flagged."},
          {"topic":"plateau","note":"Contiguous three-or-more threshold values passing an exploratory screen. NOT independent OOS and NOT approval to deploy."},
          {"topic":"cost","note":"OANDA midpoint H1; 2 pip adverse fill and 4 pip stress are assumptions, no historical bid/ask reconstruction."},
          {"topic":"history","note":"All prior data inspected; recent and post-2018 splits NOT untouched OOS."},
          {"topic":"time","note":"Signal time = H1 candle open, executed at completed close with adverse fill; next-bar exit; exit-candle eligibility."},
          {"topic":"portfolio","note":"Exact EUR/AUD versus 27 incumbents/portfolio concurrency, hedging and NAV simulation required only AFTER locking finalist."},
        ])
        pack()
        STATUS.update(state="complete",message="Plateau ZIP ready",completed=len(memo),
            parity_passed=True,results_zip=BUNDLE,
            boundary_extensions=sum(x["extension_triggered"] for x in bound_rows),
            unresolved_boundaries=sum(x["terminal_still_strong"] for x in bound_rows),
            plateau_intervals=len(plateau_rows),reference_candles=cutoff,
            history_candles=len(candles))
    except Exception as exc:
        STATUS.update(state="failed",message=f"{type(exc).__name__}: {exc}")
        try:pack()
        except Exception:pass


RESEARCH_LOCK=threading.Lock()
RESEARCH_STARTED=False

def launch_research():
    global RESEARCH_STARTED
    with RESEARCH_LOCK:
        if RESEARCH_STARTED:return False
        RESEARCH_STARTED=True
        threading.Thread(target=run_research,daemon=True,name="euraud-h1-plateau").start()
        return True

@app.route("/")
def root():
    return jsonify({"service":"EUR/AUD H1 LONG plateau confirmation",
       "read_only":True,"orders_supported":False,"trading_enabled":False,
       "status_route":"/euraud-h1-long-plateau/status",
       "results_route":"/euraud-h1-long-plateau/results"})

@app.route("/euraud-h1-long-plateau/start")
def start():
    return jsonify({"started_now":launch_research(),"state":STATUS["state"]})

@app.route("/euraud-h1-long-plateau/status")
def status():
    return jsonify(STATUS)

@app.route("/euraud-h1-long-plateau/results")
def results():
    return download(BUNDLE)

if __name__=="__main__":
    launch_research()
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")),debug=False)
