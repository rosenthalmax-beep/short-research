#!/usr/bin/env python3
"""
AUD/JPY H1 SHORT — Pass 1B boundary clarification
=================================================

RESEARCH ONLY. READ ONLY. NEVER PLACES ORDERS OR MODIFIES THE LIVE EXECUTOR.

Purpose
-------
Pass 1 found a coherent stressed bearish-engulf region but several useful
dimensions landed on tested boundaries. This pass does NOT add filters. It
only clarifies the unresolved lookback/distance/body/range boundaries before
any Pass-2 conditional-feature work.

Frozen source/execution protocol is IDENTICAL to Pass 1:
- AUD_JPY H1 OANDA midpoint completed candles
- exact bearish body engulfing
- data end exclusive 2026-09-30T08:00:00Z
- reference RR3.50
- stop = signal high + 10 ticks
- historical adverse SHORT fills: 10T primary, 20T stressed, 40T extreme
- exits start next H1 candle
- pyramiding=0; exact exit-candle signal eligible
- no weekday/session/RR/portfolio optimisation

Predeclared controlled slices
-----------------------------
A. STRUCTURE_BOUNDARY: body>=0.60 ATR, range>=1.25 ATR;
   LB 60/80/100/125/150/175/200/250 x distance .025-.20 ATR.
B. BODY_RANGE_LB100_D010: fixed LB100/D<=.10; body .60-2.00 x range 1.00-2.50.
C. BODY_RANGE_LB150_D005: fixed LB150/D<=.05; same body/range extension.
D. JOINT_BOUNDARY_CUBE: LB100/125/150/200 x D .025/.05/.075/.10 x
   body .60/1.00/1.40/1.75 x range 1.25/1.50/1.75/2.00.

Total unique planned configurations by slice = 424. Overlap is intentional:
it provides cross-slice parity and neighbourhood context, not independent wins.
No row is auto-approved or frozen.

Research template:
/Trading Strategies/FOREX_STRATEGY_RESEARCH_TEMPLATE_AUDJPY_2026-09-24.md
"""

from __future__ import annotations

import bisect
import csv
import hashlib
import json
import math
import os
import threading
import time
import traceback
import zipfile
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median

import numpy as np
import requests
from flask import Flask, jsonify, send_file

# ============================================================
# FROZEN PROTOCOL + PASS-1 PARITY REFERENCES
# ============================================================

PAIR = "AUD_JPY"
TIMEFRAME = "H1"
SIDE = "SHORT"
REQUESTED_START = datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc)
DATA_END = datetime(2026, 9, 30, 8, 0, tzinfo=timezone.utc)  # exclusive
D1_WARMUP_START = REQUESTED_START - timedelta(days=900)

TICK = 0.001
PIP = 0.01
STOP_BUFFER_TICKS = 10
REFERENCE_RR = 3.50
COST_CASES = (
    ("LIVE_LIMIT_10T", 10, 1.0, "PRIMARY_LIVE_PARITY"),
    ("STRESS_20T", 20, 2.0, "STRESSED_SELECTION"),
    ("EXTREME_40T", 40, 4.0, "EXTREME_DIAGNOSTIC_ONLY"),
)
PRIMARY_COST_LABEL = "LIVE_LIMIT_10T"
STRESS_COST_LABEL = "STRESS_20T"
EXTREME_COST_LABEL = "EXTREME_40T"

# Exact Pass-1 frozen source/raw fingerprints. Fail closed if OANDA history at
# the frozen cutoff no longer reproduces them.
EXPECTED_H1_ROWS = 138030
EXPECTED_H1_SHA256 = "5b844d2f79af6ae49e7f36586f68c366d4884859f897792bdb09ebe4e7bfcdfe"
EXPECTED_D1_ROWS = 6476
EXPECTED_D1_SHA256 = "bb43365840ff5d38d8d3ed3260c4d74204a927780e4619074ea3f9e63bcaba59"
EXPECTED_RAW_SIGNAL_COUNT = 10465
EXPECTED_RAW_SIGNAL_SHA256 = "fc53892ad7e01e06b49594108b8b1fe8047ade46ec33e0d3a8d554c84322f3ab"

# Exact Pass-1 control: GRID_LB100_D0.10_B0.60_R1.25.
# Sequence hash serialises accepted signal_index|exit_index|exit_reason.
PASS1_CONTROL_SEQUENCE_SHA256 = "94fa6be8ebe02a59a08f4e8a9633aba4b8d367fa230d3eae9946cdad372b4ef4"
PASS1_CONTROL_ACCEPTED = 124
PASS1_CONTROL_TOTAL_R = {
    "LIVE_LIMIT_10T": 44.619138706852425,
    "STRESS_20T": 38.33994906482319,
    "EXTREME_40T": 27.263649998417392,
}

# Boundary clarification dimensions.
STRUCTURE_BOUNDARY_LOOKBACKS = (60, 80, 100, 125, 150, 175, 200, 250)
STRUCTURE_BOUNDARY_DISTANCES = (0.025, 0.05, 0.075, 0.10, 0.125, 0.15, 0.20)
STRUCTURE_FIXED_BODY = 0.60
STRUCTURE_FIXED_RANGE = 1.25

BODY_RANGE_BODY_LEVELS = (0.60, 0.80, 1.00, 1.20, 1.40, 1.60, 1.75, 2.00)
BODY_RANGE_RANGE_LEVELS = (1.00, 1.25, 1.50, 1.75, 2.00, 2.25, 2.50)

JOINT_LOOKBACKS = (100, 125, 150, 200)
JOINT_DISTANCES = (0.025, 0.05, 0.075, 0.10)
JOINT_BODY_LEVELS = (0.60, 1.00, 1.40, 1.75)
JOINT_RANGE_LEVELS = (1.25, 1.50, 1.75, 2.00)

# Names retained because shared Pass-1 feature builders use them. No momentum
# or new conditional feature is tested in Pass 1B.
STRUCTURE_LOOKBACKS = tuple(sorted(set(STRUCTURE_BOUNDARY_LOOKBACKS) | set(JOINT_LOOKBACKS)))
GRID_LOOKBACKS = STRUCTURE_LOOKBACKS
MOMENTUM_LOOKBACKS = ()

EXPECTED_STRUCTURE_ROWS = len(STRUCTURE_BOUNDARY_LOOKBACKS) * len(STRUCTURE_BOUNDARY_DISTANCES)
EXPECTED_BODY_RANGE_ROWS_PER_ANCHOR = len(BODY_RANGE_BODY_LEVELS) * len(BODY_RANGE_RANGE_LEVELS)
EXPECTED_JOINT_ROWS = len(JOINT_LOOKBACKS) * len(JOINT_DISTANCES) * len(JOINT_BODY_LEVELS) * len(JOINT_RANGE_LEVELS)
EXPECTED_CONFIGS = EXPECTED_STRUCTURE_ROWS + 2 * EXPECTED_BODY_RANGE_ROWS_PER_ANCHOR + EXPECTED_JOINT_ROWS
assert EXPECTED_STRUCTURE_ROWS == 56
assert EXPECTED_BODY_RANGE_ROWS_PER_ANCHOR == 56
assert EXPECTED_JOINT_ROWS == 256
assert EXPECTED_CONFIGS == 424

RESEARCH_VERSION = "AUDJPY_H1_SHORT_PASS1B_BOUNDARY_CLARIFICATION_V1_2026_09_30"

API = os.getenv("OANDA_API_URL", "https://api-fxtrade.oanda.com").rstrip("/")
TOKEN = os.getenv("OANDA_TOKEN", "")

OUT_DIR = Path(os.getenv("AUDJPY_H1_SHORT_PASS1B_OUTPUT_DIR", "/tmp/audjpy_h1_short_pass1b"))
OUT_DIR.mkdir(parents=True, exist_ok=True)
BUNDLE = OUT_DIR / "AUDJPY_H1_SHORT_PASS1B_BOUNDARY_CLARIFICATION_RESULTS.zip"

OUTPUTS = {
    "coverage": OUT_DIR / "coverage.csv",
    "source_fingerprint": OUT_DIR / "source_fingerprint.csv",
    "hard_parity": OUT_DIR / "hard_parity.csv",
    "plan": OUT_DIR / "boundary_plan.csv",
    "summary": OUT_DIR / "boundary_summary.csv",
    "slice_summary": OUT_DIR / "slice_summary.csv",
    "level_summary": OUT_DIR / "level_summary.csv",
    "screen": OUT_DIR / "diagnostic_screen.csv",
    "screen_ledgers": OUT_DIR / "diagnostic_screen_accepted_ledgers.csv",
    "periods": OUT_DIR / "diagnostic_periods.csv",
    "years": OUT_DIR / "diagnostic_calendar_years.csv",
    "rolling": OUT_DIR / "diagnostic_rolling_12_24_36m.csv",
    "rolling_summary": OUT_DIR / "diagnostic_rolling_summary.csv",
    "methodology": OUT_DIR / "methodology.csv",
    "errors": OUT_DIR / "error_report.csv",
}

STATUS = {
    "state": "not_started",
    "progress": 0,
    "message": "AUD/JPY H1 SHORT Pass 1B waiting",
    "orders_supported": False,
    "trading_enabled": False,
    "pair": PAIR,
    "timeframe": TIMEFRAME,
    "side": SIDE,
    "rr": REFERENCE_RR,
    "cost_cases": [x[0] for x in COST_CASES],
    "planned_configurations": EXPECTED_CONFIGS,
    "research_version": RESEARCH_VERSION,
}
STATUS_LOCK = threading.Lock()
RESEARCH_LOCK = threading.Lock()
RESEARCH_STARTED = False

app = Flask(__name__)

# ============================================================
# GENERIC HELPERS
# ============================================================

def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def set_status(**kwargs):
    with STATUS_LOCK:
        STATUS.update(kwargs)


def write_csv(path: Path, rows):
    rows = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                fields.append(key)
                seen.add(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def pack_results():
    with zipfile.ZipFile(BUNDLE, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in OUTPUTS.values():
            if path.exists():
                archive.write(path, arcname=path.name)


def safe_pf(gross_profit: float, gross_loss: float) -> float:
    if gross_loss > 0:
        return gross_profit / gross_loss
    if gross_profit > 0:
        return 99.0
    return 0.0


def sha_rows(rows) -> str:
    h = hashlib.sha256()
    for row in rows:
        h.update((row + "\n").encode("utf-8"))
    return h.hexdigest()


def month_floor(value: datetime) -> datetime:
    return datetime(value.year, value.month, 1, tzinfo=timezone.utc)


def add_months(value: datetime, months: int) -> datetime:
    m = value.year * 12 + value.month - 1 + months
    return datetime(m // 12, m % 12 + 1, 1, tzinfo=timezone.utc)

# ============================================================
# OANDA FETCH
# ============================================================

def headers():
    if not TOKEN:
        raise RuntimeError("OANDA_TOKEN is not configured")
    return {"Authorization": f"Bearer {TOKEN.strip()}"}


def fetch_chunk(granularity: str, start: datetime, end: datetime):
    params = {
        "price": "M",
        "granularity": granularity,
        "smooth": "false",
        "from": iso(start),
        "to": iso(end),
        "includeFirst": "true",
    }
    if granularity == "D":
        params["dailyAlignment"] = 17
        params["alignmentTimezone"] = "America/New_York"

    response = requests.get(
        f"{API}/v3/instruments/{PAIR}/candles",
        headers=headers(),
        params=params,
        timeout=60,
    )
    response.raise_for_status()

    out = []
    for raw in response.json().get("candles", []):
        if not raw.get("complete", False):
            continue
        mid = raw.get("mid")
        if not mid:
            continue
        out.append({
            "time": parse_time(raw["time"]),
            "open": float(mid["o"]),
            "high": float(mid["h"]),
            "low": float(mid["l"]),
            "close": float(mid["c"]),
        })
    return out


def fetch_history(granularity: str, start: datetime, end: datetime, chunk_days: int):
    cursor = start
    by_time = {}
    n = 0
    while cursor < end:
        n += 1
        chunk_end = min(cursor + timedelta(days=chunk_days), end)
        set_status(
            state="fetching",
            message=f"Fetching {granularity} chunk {n}: {iso(cursor)} -> {iso(chunk_end)}",
        )
        rows = None
        last_error = None
        for attempt in range(1, 4):
            try:
                rows = fetch_chunk(granularity, cursor, chunk_end)
                last_error = None
                break
            except requests.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else None
                if status_code in (400, 404) and not by_time:
                    rows = []
                    last_error = None
                    break
                last_error = exc
            except Exception as exc:
                last_error = exc
            if attempt < 3:
                time.sleep(0.75 * attempt)
        if last_error is not None:
            raise last_error
        for row in rows or []:
            if row["time"] < end:
                by_time[row["time"]] = row
        cursor = chunk_end
        time.sleep(0.02)

    result = sorted(by_time.values(), key=lambda x: x["time"])
    if len(result) < 100:
        raise RuntimeError(f"Insufficient {granularity} history: {len(result)} completed candles")
    return result

# ============================================================
# INDICATORS / FEATURES
# ============================================================

def atr14(candles):
    n = len(candles)
    out = np.full(n, np.nan, dtype=float)
    if n < 14:
        return out
    high = np.array([x["high"] for x in candles], dtype=float)
    low = np.array([x["low"] for x in candles], dtype=float)
    close = np.array([x["close"] for x in candles], dtype=float)
    tr = np.full(n, np.nan, dtype=float)
    tr[0] = high[0] - low[0]
    for i in range(1, n):
        tr[i] = max(high[i] - low[i], abs(high[i] - close[i - 1]), abs(low[i] - close[i - 1]))
    out[13] = float(np.mean(tr[:14]))
    for i in range(14, n):
        out[i] = (out[i - 1] * 13.0 + tr[i]) / 14.0
    return out


def ema(values, length):
    values = np.asarray(values, dtype=float)
    out = np.full(len(values), np.nan, dtype=float)
    if len(values) < length:
        return out
    seed = float(np.mean(values[:length]))
    out[length - 1] = seed
    alpha = 2.0 / (length + 1.0)
    for i in range(length, len(values)):
        out[i] = alpha * values[i] + (1.0 - alpha) * out[i - 1]
    return out


def rolling_previous_extreme(values, lookback, want_max=False):
    values = np.asarray(values, dtype=float)
    out = np.full(len(values), np.nan, dtype=float)
    dq = deque()
    for i in range(len(values)):
        j = i - 1
        if j >= 0:
            v = values[j]
            if want_max:
                while dq and values[dq[-1]] <= v:
                    dq.pop()
            else:
                while dq and values[dq[-1]] >= v:
                    dq.pop()
            dq.append(j)
        minimum = i - lookback
        while dq and dq[0] < minimum:
            dq.popleft()
        if i >= lookback and dq:
            out[i] = values[dq[0]]
    return out


def rolling_previous_mean(values, lookback):
    values = np.asarray(values, dtype=float)
    out = np.full(len(values), np.nan, dtype=float)
    q = deque()
    total = 0.0
    bad = 0
    for i in range(len(values)):
        j = i - 1
        if j >= 0:
            v = values[j]
            q.append(v)
            if math.isfinite(v):
                total += v
            else:
                bad += 1
        if len(q) > lookback:
            old = q.popleft()
            if math.isfinite(old):
                total -= old
            else:
                bad -= 1
        if len(q) == lookback and bad == 0:
            out[i] = total / lookback
    return out


def build_h1_features(candles):
    opens = np.array([x["open"] for x in candles], dtype=float)
    highs = np.array([x["high"] for x in candles], dtype=float)
    lows = np.array([x["low"] for x in candles], dtype=float)
    closes = np.array([x["close"] for x in candles], dtype=float)
    times = [x["time"] for x in candles]
    atr = atr14(candles)

    po = np.full(len(candles), np.nan)
    pc = np.full(len(candles), np.nan)
    if len(candles) > 1:
        po[1:] = opens[:-1]
        pc[1:] = closes[:-1]
    previous_body = np.abs(pc - po)
    bearish_body = opens - closes
    candle_range = highs - lows
    upper_wick = highs - np.maximum(opens, closes)

    body_atr = np.divide(
        bearish_body, atr,
        out=np.zeros_like(closes),
        where=(bearish_body > 0) & np.isfinite(atr) & (atr > 0),
    )
    range_atr = np.divide(
        candle_range, atr,
        out=np.zeros_like(closes),
        where=np.isfinite(atr) & (atr > 0),
    )
    bear_ratio = np.divide(
        bearish_body, previous_body,
        out=np.zeros_like(closes),
        where=(bearish_body > 0) & (previous_body > 0),
    )
    close_location = np.divide(
        closes - lows, candle_range,
        out=np.zeros_like(closes),
        where=candle_range > 0,
    )
    upper_wick_body = np.divide(
        upper_wick, bearish_body,
        out=np.zeros_like(closes),
        where=bearish_body > 0,
    )
    exact_bear = (
        (pc > po)
        & (closes < opens)
        & (opens >= pc)
        & (closes <= po)
    )

    needed_lbs = sorted(set(STRUCTURE_LOOKBACKS) | set(GRID_LOOKBACKS))
    prev_highs = {lb: rolling_previous_extreme(highs, lb, want_max=True) for lb in needed_lbs}
    atr50_prev = rolling_previous_mean(atr, 50)
    h1_atr_ratio = np.divide(
        atr, atr50_prev,
        out=np.full(len(candles), np.nan),
        where=np.isfinite(atr50_prev) & (atr50_prev > 0),
    )

    momentum = {}
    for lb in MOMENTUM_LOOKBACKS:
        arr = np.full(len(candles), np.nan)
        # Prior movement only: previous completed H1 close versus the close lb bars earlier.
        for i in range(lb + 1, len(candles)):
            if math.isfinite(atr[i]) and atr[i] > 0:
                arr[i] = (closes[i - 1] - closes[i - 1 - lb]) / atr[i]
        momentum[lb] = arr

    return {
        "times": times,
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
        "atr": atr,
        "exact_bear": exact_bear,
        "bear_ratio": bear_ratio,
        "body_atr": body_atr,
        "range_atr": range_atr,
        "close_location": close_location,
        "upper_wick_body": upper_wick_body,
        "prev_highs": prev_highs,
        "h1_atr_ratio": h1_atr_ratio,
        "momentum": momentum,
    }


def build_daily_states(daily, h1_times):
    d_close = np.array([x["close"] for x in daily], dtype=float)
    d_atr = atr14(daily)
    d_ema50 = ema(d_close, 50)
    d_ema100 = ema(d_close, 100)
    d_ema200 = ema(d_close, 200)
    d_atr50_prev = rolling_previous_mean(d_atr, 50)
    d_atr_ratio = np.divide(
        d_atr, d_atr50_prev,
        out=np.full(len(daily), np.nan),
        where=np.isfinite(d_atr50_prev) & (d_atr50_prev > 0),
    )

    # OANDA daily candle j is considered completed at the OPEN of daily candle j+1.
    completion_times = [daily[i + 1]["time"] for i in range(len(daily) - 1)]
    mapped = {
        "close_lt_ema50": np.zeros(len(h1_times), dtype=bool),
        "close_lt_ema100": np.zeros(len(h1_times), dtype=bool),
        "close_lt_ema200": np.zeros(len(h1_times), dtype=bool),
        "ema50_lt_ema200": np.zeros(len(h1_times), dtype=bool),
        "atr_ratio": np.full(len(h1_times), np.nan),
        "daily_index": np.full(len(h1_times), -1, dtype=int),
    }
    for i, t in enumerate(h1_times):
        pos = bisect.bisect_right(completion_times, t) - 1
        if pos < 0:
            continue
        j = pos
        mapped["daily_index"][i] = j
        if math.isfinite(d_ema50[j]):
            mapped["close_lt_ema50"][i] = d_close[j] < d_ema50[j]
        if math.isfinite(d_ema100[j]):
            mapped["close_lt_ema100"][i] = d_close[j] < d_ema100[j]
        if math.isfinite(d_ema200[j]):
            mapped["close_lt_ema200"][i] = d_close[j] < d_ema200[j]
        if math.isfinite(d_ema50[j]) and math.isfinite(d_ema200[j]):
            mapped["ema50_lt_ema200"][i] = d_ema50[j] < d_ema200[j]
        mapped["atr_ratio"][i] = d_atr_ratio[j]
    return mapped

# ============================================================
# RAW SIGNAL / EXECUTION PARITY
# ============================================================

def raw_exact_vector_indices(features):
    mask = features["exact_bear"] & np.isfinite(features["atr"]) & (features["atr"] > 0)
    return np.flatnonzero(mask).astype(int)


def raw_exact_scalar_indices(candles, atr_values):
    result = []
    for i in range(1, len(candles)):
        p = candles[i - 1]
        c = candles[i]
        exact = (
            p["close"] > p["open"]
            and c["close"] < c["open"]
            and c["open"] >= p["close"]
            and c["close"] <= p["open"]
        )
        if exact and math.isfinite(float(atr_values[i])) and atr_values[i] > 0:
            result.append(i)
    return np.asarray(result, dtype=int)


def find_exit(features, signal_index, stop, target):
    high = features["high"]
    low = features["low"]
    opn = features["open"]
    for i in range(signal_index + 1, len(high)):
        s = high[i] >= stop
        t = low[i] <= target
        if not s and not t:
            continue
        if s and t:
            # SHORT tie convention: whichever barrier side is closer to candle open.
            reason = "TARGET" if abs(opn[i] - low[i]) < abs(high[i] - opn[i]) else "STOP"
        else:
            reason = "STOP" if s else "TARGET"
        return i, reason
    return None, None


def result_r_for_cost(reference_entry, stop, target, reason, cost_pips):
    # Adverse SHORT fill is below the signal close.
    fill = reference_entry - cost_pips * PIP
    actual_risk = stop - fill
    if actual_risk <= 0:
        return None, fill
    exit_price = target if reason == "TARGET" else stop
    return (fill - exit_price) / actual_risk, fill


def build_raw_outcomes(features, raw_indices, daily_states):
    records = []
    censored = 0
    for k, i in enumerate(raw_indices):
        if k % 1000 == 0:
            set_status(message=f"Building raw exact-engulf outcomes {k}/{len(raw_indices)}")
        entry = float(features["close"][i])
        stop = float(features["high"][i]) + STOP_BUFFER_TICKS * TICK
        ref_risk = stop - entry
        if ref_risk <= 0:
            continue
        target = entry - REFERENCE_RR * ref_risk
        exit_index, reason = find_exit(features, int(i), stop, target)
        if exit_index is None:
            censored += 1
            continue

        row = {
            "raw_position": len(records),
            "signal_index": int(i),
            "exit_index": int(exit_index),
            "signal_time": features["times"][i],
            "exit_time": features["times"][exit_index],
            "reference_entry": entry,
            "stop": stop,
            "target": target,
            "exit_reason": reason,
            "bear_ratio": float(features["bear_ratio"][i]),
            "body_atr": float(features["body_atr"][i]),
            "range_atr": float(features["range_atr"][i]),
            "close_location": float(features["close_location"][i]),
            "upper_wick_body": float(features["upper_wick_body"][i]),
            "h1_atr_ratio": float(features["h1_atr_ratio"][i]) if math.isfinite(features["h1_atr_ratio"][i]) else math.nan,
            "daily_close_lt_ema50": bool(daily_states["close_lt_ema50"][i]),
            "daily_close_lt_ema100": bool(daily_states["close_lt_ema100"][i]),
            "daily_close_lt_ema200": bool(daily_states["close_lt_ema200"][i]),
            "daily_ema50_lt_ema200": bool(daily_states["ema50_lt_ema200"][i]),
            "daily_atr_ratio": float(daily_states["atr_ratio"][i]) if math.isfinite(daily_states["atr_ratio"][i]) else math.nan,
            "hour_utc": features["times"][i].hour,
            "weekday_utc": features["times"][i].weekday(),
        }
        for lb in STRUCTURE_LOOKBACKS:
            structure = features["prev_highs"][lb][i]
            row[f"structure_dist_{lb}"] = (
                abs(float(features["high"][i]) - float(structure)) / float(features["atr"][i])
                if math.isfinite(structure) else math.nan
            )
        for lb in MOMENTUM_LOOKBACKS:
            value = features["momentum"][lb][i]
            row[f"momentum_{lb}"] = float(value) if math.isfinite(value) else math.nan
        for label, ticks, pips, purpose in COST_CASES:
            result_r, fill = result_r_for_cost(entry, stop, target, reason, pips)
            if result_r is None:
                raise RuntimeError(f"Invalid actual risk for {label} at signal {iso(features['times'][i])}")
            row[f"result_r__{label}"] = float(result_r)
            row[f"fill__{label}"] = float(fill)
        records.append(row)
    return records, censored


def scalar_exit_from_candles(candles, signal_index, stop, target):
    """Independent plain-Python exit scan used only for parity checking."""
    for j in range(signal_index + 1, len(candles)):
        candle = candles[j]
        stop_touched = candle["high"] >= stop
        target_touched = candle["low"] <= target
        if not stop_touched and not target_touched:
            continue
        if stop_touched and target_touched:
            reason = "TARGET" if abs(candle["open"] - candle["low"]) < abs(candle["high"] - candle["open"]) else "STOP"
        else:
            reason = "STOP" if stop_touched else "TARGET"
        return j, reason
    return None, None


def execution_parity_sample(features, candles, records, sample_n=100):
    # Independent scalar recomputation from original candle dictionaries.
    failures = []
    for row in records[:sample_n]:
        i = row["signal_index"]
        entry = float(candles[i]["close"])
        stop = float(candles[i]["high"]) + 10 * 0.001
        target = entry - 3.5 * (stop - entry)
        exit_i, reason = scalar_exit_from_candles(candles, i, stop, target)
        if exit_i != row["exit_index"] or reason != row["exit_reason"]:
            failures.append(f"exit mismatch {i}")
            continue
        for label, ticks, pips, purpose in COST_CASES:
            rr, fill = result_r_for_cost(entry, stop, target, reason, pips)
            if abs(rr - row[f"result_r__{label}"]) > 1e-12:
                failures.append(f"R mismatch {i} {label}")
    return failures

# ============================================================
# REPLAY / METRICS
# ============================================================

def replay_positions(records, qualified_positions):
    accepted = []
    active_exit = -1
    for pos in qualified_positions:
        row = records[int(pos)]
        if row["signal_index"] < active_exit:
            continue
        accepted.append(int(pos))
        active_exit = row["exit_index"]
    return accepted


def metrics(records, positions, cost_label):
    values = [float(records[p][f"result_r__{cost_label}"]) for p in positions]
    if not values:
        return {
            "accepted_trades": 0,
            "winners": 0,
            "losers": 0,
            "win_rate_pct": 0.0,
            "profit_factor": 0.0,
            "total_r": 0.0,
            "expectancy_r": 0.0,
            "max_drawdown_r": 0.0,
            "longest_losing_streak": 0,
        }
    winners = sum(v > 0 for v in values)
    losers = len(values) - winners
    gp = sum(v for v in values if v > 0)
    gl = -sum(v for v in values if v < 0)
    cum = 0.0
    peak = 0.0
    dd = 0.0
    streak = 0
    longest = 0
    for v in values:
        cum += v
        peak = max(peak, cum)
        dd = min(dd, cum - peak)
        if v <= 0:
            streak += 1
            longest = max(longest, streak)
        else:
            streak = 0
    return {
        "accepted_trades": len(values),
        "winners": winners,
        "losers": losers,
        "win_rate_pct": 100.0 * winners / len(values),
        "profit_factor": safe_pf(gp, gl),
        "total_r": sum(values),
        "expectancy_r": sum(values) / len(values),
        "max_drawdown_r": dd,
        "longest_losing_streak": longest,
    }


def summarize_config(records, mask, config_meta):
    qualified = np.flatnonzero(mask).astype(int).tolist()
    accepted = replay_positions(records, qualified)
    rows = []
    for label, ticks, pips, purpose in COST_CASES:
        m = metrics(records, accepted, label)
        all_qualified_total_r = sum(float(records[p][f"result_r__{label}"]) for p in qualified)
        rows.append({
            **config_meta,
            "cost_label": label,
            "adverse_ticks": ticks,
            "adverse_pips": pips,
            "cost_purpose": purpose,
            "qualified_raw_signals": len(qualified),
            "accepted_after_p0": len(accepted),
            "blocked_by_p0": len(qualified) - len(accepted),
            "candidate_only_total_r_no_p0": all_qualified_total_r,
            **m,
        })
    return rows, accepted


def all_record_arrays(records):
    keys = [
        "bear_ratio", "body_atr", "range_atr", "close_location",
        "upper_wick_body", "h1_atr_ratio", "daily_atr_ratio",
    ]
    out = {k: np.array([r[k] for r in records], dtype=float) for k in keys}
    out["daily_close_lt_ema50"] = np.array([r["daily_close_lt_ema50"] for r in records], dtype=bool)
    out["daily_close_lt_ema100"] = np.array([r["daily_close_lt_ema100"] for r in records], dtype=bool)
    out["daily_close_lt_ema200"] = np.array([r["daily_close_lt_ema200"] for r in records], dtype=bool)
    out["daily_ema50_lt_ema200"] = np.array([r["daily_ema50_lt_ema200"] for r in records], dtype=bool)
    for lb in STRUCTURE_LOOKBACKS:
        out[f"structure_dist_{lb}"] = np.array([r[f"structure_dist_{lb}"] for r in records], dtype=float)
    for lb in MOMENTUM_LOOKBACKS:
        out[f"momentum_{lb}"] = np.array([r[f"momentum_{lb}"] for r in records], dtype=float)
    return out

# ============================================================
# PASS-1B CONTROLLED BOUNDARY PLAN
# ============================================================

def _structure_mask(arr, lb, distance):
    d = arr[f"structure_dist_{lb}"]
    return np.isfinite(d) & (d <= distance)


def boundary_plan(arr):
    # A) Clarify lookback and tight distance boundary with Pass-1 top row's
    # body/range geometry held fixed.
    for lb in STRUCTURE_BOUNDARY_LOOKBACKS:
        for distance in STRUCTURE_BOUNDARY_DISTANCES:
            config_id = f"STRUCT_BOUND_LB{lb}_D{distance:.3f}_B0.60_R1.25"
            mask = (
                _structure_mask(arr, lb, distance)
                & (arr["body_atr"] >= STRUCTURE_FIXED_BODY)
                & (arr["range_atr"] >= STRUCTURE_FIXED_RANGE)
            )
            yield {
                "config_id": config_id,
                "slice": "STRUCTURE_BOUNDARY",
                "lookback": lb,
                "distance_atr_max": distance,
                "body_atr_min": STRUCTURE_FIXED_BODY,
                "range_atr_min": STRUCTURE_FIXED_RANGE,
                "lookback_is_min": lb == min(STRUCTURE_BOUNDARY_LOOKBACKS),
                "lookback_is_max": lb == max(STRUCTURE_BOUNDARY_LOOKBACKS),
                "distance_is_min": distance == min(STRUCTURE_BOUNDARY_DISTANCES),
                "distance_is_max": distance == max(STRUCTURE_BOUNDARY_DISTANCES),
                "body_is_min": True,
                "body_is_max": True,
                "range_is_min": True,
                "range_is_max": True,
                "mask": mask,
            }

    # B/C) Clarify body/range edges at two predeclared structural anchors.
    for slice_name, lb, distance in (
        ("BODY_RANGE_LB100_D010", 100, 0.10),
        ("BODY_RANGE_LB150_D005", 150, 0.05),
    ):
        struct = _structure_mask(arr, lb, distance)
        for body in BODY_RANGE_BODY_LEVELS:
            for rng in BODY_RANGE_RANGE_LEVELS:
                config_id = f"{slice_name}_B{body:.2f}_R{rng:.2f}"
                mask = struct & (arr["body_atr"] >= body) & (arr["range_atr"] >= rng)
                yield {
                    "config_id": config_id,
                    "slice": slice_name,
                    "lookback": lb,
                    "distance_atr_max": distance,
                    "body_atr_min": body,
                    "range_atr_min": rng,
                    "lookback_is_min": True,
                    "lookback_is_max": True,
                    "distance_is_min": True,
                    "distance_is_max": True,
                    "body_is_min": body == min(BODY_RANGE_BODY_LEVELS),
                    "body_is_max": body == max(BODY_RANGE_BODY_LEVELS),
                    "range_is_min": rng == min(BODY_RANGE_RANGE_LEVELS),
                    "range_is_max": rng == max(BODY_RANGE_RANGE_LEVELS),
                    "mask": mask,
                }

    # D) Small joint cube to test whether the extended dimensions interact.
    for lb in JOINT_LOOKBACKS:
        for distance in JOINT_DISTANCES:
            struct = _structure_mask(arr, lb, distance)
            for body in JOINT_BODY_LEVELS:
                for rng in JOINT_RANGE_LEVELS:
                    config_id = f"JOINT_LB{lb}_D{distance:.3f}_B{body:.2f}_R{rng:.2f}"
                    mask = struct & (arr["body_atr"] >= body) & (arr["range_atr"] >= rng)
                    yield {
                        "config_id": config_id,
                        "slice": "JOINT_BOUNDARY_CUBE",
                        "lookback": lb,
                        "distance_atr_max": distance,
                        "body_atr_min": body,
                        "range_atr_min": rng,
                        "lookback_is_min": lb == min(JOINT_LOOKBACKS),
                        "lookback_is_max": lb == max(JOINT_LOOKBACKS),
                        "distance_is_min": distance == min(JOINT_DISTANCES),
                        "distance_is_max": distance == max(JOINT_DISTANCES),
                        "body_is_min": body == min(JOINT_BODY_LEVELS),
                        "body_is_max": body == max(JOINT_BODY_LEVELS),
                        "range_is_min": rng == min(JOINT_RANGE_LEVELS),
                        "range_is_max": rng == max(JOINT_RANGE_LEVELS),
                        "mask": mask,
                    }


def accepted_sequence_hash(records, positions):
    return sha_rows(
        f"{records[p]['signal_index']}|{records[p]['exit_index']}|{records[p]['exit_reason']}"
        for p in positions
    )


def build_plan_rows():
    return [
        {
            "slice": "STRUCTURE_BOUNDARY",
            "purpose": "Extend Pass-1 upper lookback and lower/tighter distance boundaries with B0.60/R1.25 fixed",
            "lookbacks": json.dumps(STRUCTURE_BOUNDARY_LOOKBACKS),
            "distances": json.dumps(STRUCTURE_BOUNDARY_DISTANCES),
            "body_levels": json.dumps((STRUCTURE_FIXED_BODY,)),
            "range_levels": json.dumps((STRUCTURE_FIXED_RANGE,)),
            "configurations": EXPECTED_STRUCTURE_ROWS,
        },
        {
            "slice": "BODY_RANGE_LB100_D010",
            "purpose": "Extend body/range boundaries at Pass-1 leading structural anchor LB100/D0.10",
            "lookbacks": json.dumps((100,)),
            "distances": json.dumps((0.10,)),
            "body_levels": json.dumps(BODY_RANGE_BODY_LEVELS),
            "range_levels": json.dumps(BODY_RANGE_RANGE_LEVELS),
            "configurations": EXPECTED_BODY_RANGE_ROWS_PER_ANCHOR,
        },
        {
            "slice": "BODY_RANGE_LB150_D005",
            "purpose": "Test same body/range extension at longer/tighter structural branch suggested by Pass-1 single-factor scan",
            "lookbacks": json.dumps((150,)),
            "distances": json.dumps((0.05,)),
            "body_levels": json.dumps(BODY_RANGE_BODY_LEVELS),
            "range_levels": json.dumps(BODY_RANGE_RANGE_LEVELS),
            "configurations": EXPECTED_BODY_RANGE_ROWS_PER_ANCHOR,
        },
        {
            "slice": "JOINT_BOUNDARY_CUBE",
            "purpose": "Compact interaction check across extended structure/body/range region; diagnostic only, not an optimisation target",
            "lookbacks": json.dumps(JOINT_LOOKBACKS),
            "distances": json.dumps(JOINT_DISTANCES),
            "body_levels": json.dumps(JOINT_BODY_LEVELS),
            "range_levels": json.dumps(JOINT_RANGE_LEVELS),
            "configurations": EXPECTED_JOINT_ROWS,
        },
    ]


def summarise_slices(summary_rows):
    out = []
    slices = sorted(set(r["slice"] for r in summary_rows))
    for slice_name in slices:
        for cost_label, _, _, _ in COST_CASES:
            rows = [r for r in summary_rows if r["slice"] == slice_name and r["cost_label"] == cost_label]
            vals = [float(r["total_r"]) for r in rows]
            out.append({
                "slice": slice_name,
                "cost_label": cost_label,
                "configurations": len(rows),
                "positive_total_r": sum(v > 0 for v in vals),
                "positive_pf": sum(float(r["profit_factor"]) > 1.0 for r in rows),
                "positive_with_30plus_trades": sum(float(r["total_r"]) > 0 and int(r["accepted_trades"]) >= 30 for r in rows),
                "median_total_r": float(median(vals)) if vals else 0.0,
                "best_total_r": max(vals) if vals else 0.0,
                "worst_total_r": min(vals) if vals else 0.0,
                "median_accepted_trades": float(median([int(r["accepted_trades"]) for r in rows])) if rows else 0.0,
            })
    return out


def summarise_levels(summary_rows):
    out = []
    param_map = [
        ("lookback", "lookback"),
        ("distance_atr_max", "distance_atr_max"),
        ("body_atr_min", "body_atr_min"),
        ("range_atr_min", "range_atr_min"),
    ]
    for slice_name in sorted(set(r["slice"] for r in summary_rows)):
        stress_rows = [r for r in summary_rows if r["slice"] == slice_name and r["cost_label"] == STRESS_COST_LABEL]
        for param_name, key in param_map:
            values = sorted(set(r[key] for r in stress_rows))
            if len(values) <= 1:
                continue
            for value in values:
                rows = [r for r in stress_rows if r[key] == value]
                rs = [float(r["total_r"]) for r in rows]
                out.append({
                    "slice": slice_name,
                    "cost_label": STRESS_COST_LABEL,
                    "parameter": param_name,
                    "level": value,
                    "configurations": len(rows),
                    "positive_total_r": sum(x > 0 for x in rs),
                    "median_total_r": float(median(rs)) if rs else 0.0,
                    "best_total_r": max(rs) if rs else 0.0,
                    "worst_total_r": min(rs) if rs else 0.0,
                    "median_accepted_trades": float(median([int(r["accepted_trades"]) for r in rows])) if rows else 0.0,
                })
    return out

# ============================================================
# PERIOD / ROLLING DIAGNOSTICS FOR PREDECLARED MECHANICAL SCREEN
# ============================================================

def positions_in_period(records, positions, start=None, end=None):
    out = []
    for p in positions:
        t = records[p]["signal_time"]
        if start is not None and t < start:
            continue
        if end is not None and t >= end:
            continue
        out.append(p)
    return out


def diagnostics_for_config(records, config_id, accepted, cost_label):
    periods = [
        ("PRE2010", None, datetime(2010, 1, 1, tzinfo=timezone.utc)),
        ("2010_2015", datetime(2010, 1, 1, tzinfo=timezone.utc), datetime(2016, 1, 1, tzinfo=timezone.utc)),
        ("2016_2021", datetime(2016, 1, 1, tzinfo=timezone.utc), datetime(2022, 1, 1, tzinfo=timezone.utc)),
        ("2022_PLUS", datetime(2022, 1, 1, tzinfo=timezone.utc), None),
        ("LAST5Y", DATA_END - timedelta(days=365.2425 * 5), None),
        ("LAST2Y", DATA_END - timedelta(days=365.2425 * 2), None),
    ]
    period_rows = []
    for name, start, end in periods:
        pos = positions_in_period(records, accepted, start, end)
        period_rows.append({"config_id": config_id, "cost_label": cost_label, "period": name, **metrics(records, pos, cost_label)})

    year_rows = []
    first_year = min(records[p]["signal_time"].year for p in accepted) if accepted else 2005
    last_year = DATA_END.year
    for year in range(first_year, last_year + 1):
        start = datetime(year, 1, 1, tzinfo=timezone.utc)
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        pos = positions_in_period(records, accepted, start, end)
        m = metrics(records, pos, cost_label)
        year_rows.append({"config_id": config_id, "cost_label": cost_label, "year": year, "zero_trade_year": len(pos) == 0, "complete_year": year < DATA_END.year, **m})

    rolling_rows = []
    rolling_summary = []
    first_month = month_floor(records[0]["signal_time"])
    last_complete_month = month_floor(DATA_END)
    for window in (12, 24, 36):
        rows_this = []
        cursor = first_month
        while add_months(cursor, window) <= last_complete_month:
            end = add_months(cursor, window)
            pos = positions_in_period(records, accepted, cursor, end)
            m = metrics(records, pos, cost_label)
            row = {
                "config_id": config_id,
                "cost_label": cost_label,
                "window_months": window,
                "start": iso(cursor),
                "end_exclusive": iso(end),
                "zero_trade_window": len(pos) == 0,
                **m,
            }
            rolling_rows.append(row)
            rows_this.append(row)
            cursor = add_months(cursor, 1)
        if rows_this:
            weakest = min(rows_this, key=lambda x: x["total_r"])
            rolling_summary.append({
                "config_id": config_id,
                "cost_label": cost_label,
                "window_months": window,
                "windows": len(rows_this),
                "positive_windows": sum(x["total_r"] > 0 for x in rows_this),
                "zero_trade_windows": sum(x["zero_trade_window"] for x in rows_this),
                "worst_total_r": weakest["total_r"],
                "worst_start": weakest["start"],
                "worst_end_exclusive": weakest["end_exclusive"],
            })
    return period_rows, year_rows, rolling_rows, rolling_summary

# ============================================================
# MAIN RESEARCH
# ============================================================

def run_research():
    try:
        hard_checks = []
        set_status(state="fetching", progress=2, message="Fetching exact frozen Pass-1 H1/D1 midpoint history")
        h1 = fetch_history("H1", REQUESTED_START, DATA_END, 180)
        d1 = fetch_history("D", D1_WARMUP_START, DATA_END, 1200)

        h1_times = [x["time"] for x in h1]
        d1_times = [x["time"] for x in d1]
        if h1_times != sorted(set(h1_times)):
            raise RuntimeError("H1 timestamps are duplicated or non-monotonic")
        if d1_times != sorted(set(d1_times)):
            raise RuntimeError("D1 timestamps are duplicated or non-monotonic")
        if not h1 or h1[-1]["time"] >= DATA_END:
            raise RuntimeError("H1 source violates frozen cutoff")

        h1_sha = sha_rows(
            f"{iso(x['time'])}|{x['open']:.6f}|{x['high']:.6f}|{x['low']:.6f}|{x['close']:.6f}"
            for x in h1
        )
        d1_sha = sha_rows(
            f"{iso(x['time'])}|{x['open']:.6f}|{x['high']:.6f}|{x['low']:.6f}|{x['close']:.6f}"
            for x in d1
        )
        for check, actual, expected in (
            ("h1_rows", len(h1), EXPECTED_H1_ROWS),
            ("h1_sha256", h1_sha, EXPECTED_H1_SHA256),
            ("d1_rows", len(d1), EXPECTED_D1_ROWS),
            ("d1_sha256", d1_sha, EXPECTED_D1_SHA256),
        ):
            ok = actual == expected
            hard_checks.append({"check": check, "status": "PASS" if ok else "FAIL", "actual": actual, "expected": expected})
            if not ok:
                write_csv(OUTPUTS["hard_parity"], hard_checks)
                raise RuntimeError(f"Frozen source parity failed: {check}")

        write_csv(OUTPUTS["coverage"], [
            {"pair": PAIR, "timeframe": "H1", "requested_start": iso(REQUESTED_START), "first_completed_candle": iso(h1[0]["time"]), "last_completed_candle_open": iso(h1[-1]["time"]), "frozen_end_exclusive": iso(DATA_END), "completed_candles": len(h1)},
            {"pair": PAIR, "timeframe": "D", "requested_start": iso(D1_WARMUP_START), "first_completed_candle": iso(d1[0]["time"]), "last_completed_candle_open": iso(d1[-1]["time"]), "frozen_end_exclusive": iso(DATA_END), "completed_candles": len(d1), "daily_alignment": "17:00 America/New_York"},
        ])
        write_csv(OUTPUTS["source_fingerprint"], [
            {"series": "AUD_JPY_H1_MID_OHLC", "sha256": h1_sha, "rows": len(h1), "expected_sha256": EXPECTED_H1_SHA256, "parity": "PASS"},
            {"series": "AUD_JPY_D1_MID_OHLC", "sha256": d1_sha, "rows": len(d1), "expected_sha256": EXPECTED_D1_SHA256, "parity": "PASS"},
        ])

        set_status(state="features", progress=10, message="Building exact Pass-1 feature/execution state")
        features = build_h1_features(h1)
        daily_states = build_daily_states(d1, features["times"])
        vec_raw = raw_exact_vector_indices(features)
        scalar_raw = raw_exact_scalar_indices(h1, features["atr"])
        vec_hash = sha_rows(iso(features["times"][i]) for i in vec_raw)
        scalar_hash = sha_rows(iso(features["times"][i]) for i in scalar_raw)
        raw_ok = (
            len(vec_raw) == EXPECTED_RAW_SIGNAL_COUNT
            and len(scalar_raw) == EXPECTED_RAW_SIGNAL_COUNT
            and np.array_equal(vec_raw, scalar_raw)
            and vec_hash == EXPECTED_RAW_SIGNAL_SHA256
            and scalar_hash == EXPECTED_RAW_SIGNAL_SHA256
        )
        hard_checks.append({
            "check": "raw_exact_bearish_engulf_vector_scalar_and_pass1_hash",
            "status": "PASS" if raw_ok else "FAIL",
            "actual": f"vector={len(vec_raw)};scalar={len(scalar_raw)};vhash={vec_hash};shash={scalar_hash}",
            "expected": f"count={EXPECTED_RAW_SIGNAL_COUNT};hash={EXPECTED_RAW_SIGNAL_SHA256}",
        })
        if not raw_ok:
            write_csv(OUTPUTS["hard_parity"], hard_checks)
            raise RuntimeError("Raw exact-signal Pass-1 parity failed")

        set_status(state="raw_replay", progress=16, message=f"Replaying {len(vec_raw)} exact bearish-engulf raw signals")
        records, censored = build_raw_outcomes(features, vec_raw, daily_states)
        exec_failures = execution_parity_sample(features, h1, records, sample_n=min(100, len(records)))
        exec_ok = not exec_failures
        hard_checks.append({
            "check": "execution_scalar_recompute_first_100_closed_raw_signals",
            "status": "PASS" if exec_ok else "FAIL",
            "actual": "; ".join(exec_failures[:10]) if exec_failures else "100 matched",
            "expected": "all matched",
        })
        if not exec_ok:
            write_csv(OUTPUTS["hard_parity"], hard_checks)
            raise RuntimeError("Execution parity failed")

        arr = all_record_arrays(records)

        # Exact Pass-1 control replay before any new boundary result is interpreted.
        control_mask = (
            _structure_mask(arr, 100, 0.10)
            & (arr["body_atr"] >= 0.60)
            & (arr["range_atr"] >= 1.25)
        )
        control_rows, control_accepted = summarize_config(records, control_mask, {
            "config_id": "PASS1_CONTROL_GRID_LB100_D0.10_B0.60_R1.25",
            "stage": "PASS1_CONTROL",
            "slice": "PASS1_CONTROL",
            "lookback": 100,
            "distance_atr_max": 0.10,
            "body_atr_min": 0.60,
            "range_atr_min": 1.25,
            "rr": REFERENCE_RR,
        })
        control_hash = accepted_sequence_hash(records, control_accepted)
        control_hash_ok = len(control_accepted) == PASS1_CONTROL_ACCEPTED and control_hash == PASS1_CONTROL_SEQUENCE_SHA256
        hard_checks.append({
            "check": "pass1_control_accepted_sequence",
            "status": "PASS" if control_hash_ok else "FAIL",
            "actual": f"accepted={len(control_accepted)};hash={control_hash}",
            "expected": f"accepted={PASS1_CONTROL_ACCEPTED};hash={PASS1_CONTROL_SEQUENCE_SHA256}",
        })
        if not control_hash_ok:
            write_csv(OUTPUTS["hard_parity"], hard_checks)
            raise RuntimeError("Pass-1 control accepted-sequence parity failed")

        for row in control_rows:
            expected_r = PASS1_CONTROL_TOTAL_R[row["cost_label"]]
            ok = abs(float(row["total_r"]) - expected_r) <= 1e-9
            hard_checks.append({
                "check": f"pass1_control_total_r_{row['cost_label']}",
                "status": "PASS" if ok else "FAIL",
                "actual": row["total_r"],
                "expected": expected_r,
            })
            if not ok:
                write_csv(OUTPUTS["hard_parity"], hard_checks)
                raise RuntimeError(f"Pass-1 control R parity failed at {row['cost_label']}")

        write_csv(OUTPUTS["hard_parity"], hard_checks)
        write_csv(OUTPUTS["plan"], build_plan_rows())

        set_status(state="boundary_grid", progress=25, message=f"Running {EXPECTED_CONFIGS} predeclared boundary configurations")
        summary_rows = []
        accepted_by_id = {}
        meta_by_id = {}
        count = 0
        for spec in boundary_plan(arr):
            count += 1
            if count % 25 == 0:
                set_status(message=f"Boundary clarification {count}/{EXPECTED_CONFIGS}", progress=25 + int(45 * count / EXPECTED_CONFIGS))
            mask = spec.pop("mask")
            config_id = spec["config_id"]
            rows, accepted = summarize_config(records, mask, {
                **spec,
                "stage": "PASS1B_BOUNDARY_CLARIFICATION",
                "rr": REFERENCE_RR,
            })
            summary_rows.extend(rows)
            accepted_by_id[config_id] = accepted
            meta_by_id[config_id] = dict(spec)
        if count != EXPECTED_CONFIGS:
            raise RuntimeError(f"Boundary plan enumeration mismatch: expected {EXPECTED_CONFIGS}, got {count}")
        write_csv(OUTPUTS["summary"], summary_rows)
        write_csv(OUTPUTS["slice_summary"], summarise_slices(summary_rows))
        write_csv(OUTPUTS["level_summary"], summarise_levels(summary_rows))

        # Mechanical diagnostic screen ONLY. It must not be treated as a frozen
        # selection rule. It simply gives rolling/year ledgers for the strongest
        # stress-resistant rows plus the exact Pass-1 control.
        by_cfg = defaultdict(dict)
        for row in summary_rows:
            by_cfg[row["config_id"]][row["cost_label"]] = row
        eligible = []
        for config_id, costs in by_cfg.items():
            if not all(x in costs for x in (PRIMARY_COST_LABEL, STRESS_COST_LABEL, EXTREME_COST_LABEL)):
                continue
            live = costs[PRIMARY_COST_LABEL]
            stress = costs[STRESS_COST_LABEL]
            extreme = costs[EXTREME_COST_LABEL]
            if int(stress["accepted_trades"]) >= 30 and float(live["total_r"]) > 0 and float(stress["total_r"]) > 0 and float(extreme["total_r"]) > 0:
                eligible.append((float(stress["total_r"]), config_id))
        eligible.sort(reverse=True)
        selected_ids = [cid for _, cid in eligible[:12]]

        screen_rows = []
        for rank, cid in enumerate(selected_ids, start=1):
            costs = by_cfg[cid]
            stress = costs[STRESS_COST_LABEL]
            screen_rows.append({
                "diagnostic_rank": rank,
                "config_id": cid,
                "selection_status": "DIAGNOSTIC_ONLY_NOT_FROZEN",
                "screen_rule": "20T trades>=30; totalR positive at 10T/20T/40T; ranked by 20T totalR",
                "slice": stress["slice"],
                "live10T_trades": costs[PRIMARY_COST_LABEL]["accepted_trades"],
                "live10T_total_r": costs[PRIMARY_COST_LABEL]["total_r"],
                "stress20T_total_r": stress["total_r"],
                "stress20T_pf": stress["profit_factor"],
                "extreme40T_total_r": costs[EXTREME_COST_LABEL]["total_r"],
                "extreme40T_pf": costs[EXTREME_COST_LABEL]["profit_factor"],
                "lookback": stress["lookback"],
                "distance_atr_max": stress["distance_atr_max"],
                "body_atr_min": stress["body_atr_min"],
                "range_atr_min": stress["range_atr_min"],
                "any_tested_boundary": bool(
                    stress.get("lookback_is_min") or stress.get("lookback_is_max")
                    or stress.get("distance_is_min") or stress.get("distance_is_max")
                    or stress.get("body_is_min") or stress.get("body_is_max")
                    or stress.get("range_is_min") or stress.get("range_is_max")
                ),
            })
        # Explicit control row at rank 0 for side-by-side diagnostics.
        c10 = next(x for x in control_rows if x["cost_label"] == PRIMARY_COST_LABEL)
        c20 = next(x for x in control_rows if x["cost_label"] == STRESS_COST_LABEL)
        c40 = next(x for x in control_rows if x["cost_label"] == EXTREME_COST_LABEL)
        screen_rows.insert(0, {
            "diagnostic_rank": 0,
            "config_id": "PASS1_CONTROL_GRID_LB100_D0.10_B0.60_R1.25",
            "selection_status": "FROZEN_PARITY_CONTROL_NOT_CANDIDATE_SELECTION",
            "screen_rule": "Exact Pass-1 leading-row parity control",
            "slice": "PASS1_CONTROL",
            "live10T_trades": c10["accepted_trades"],
            "live10T_total_r": c10["total_r"],
            "stress20T_total_r": c20["total_r"],
            "stress20T_pf": c20["profit_factor"],
            "extreme40T_total_r": c40["total_r"],
            "extreme40T_pf": c40["profit_factor"],
            "lookback": 100,
            "distance_atr_max": 0.10,
            "body_atr_min": 0.60,
            "range_atr_min": 1.25,
            "any_tested_boundary": False,
        })
        write_csv(OUTPUTS["screen"], screen_rows)

        set_status(state="diagnostics", progress=82, message="Building diagnostic ledgers, eras, years and rolling windows")
        ledger_rows = []
        period_rows = []
        year_rows = []
        rolling_rows = []
        rolling_summary_rows = []
        diag_configs = [("PASS1_CONTROL_GRID_LB100_D0.10_B0.60_R1.25", control_accepted)] + [(cid, accepted_by_id[cid]) for cid in selected_ids]
        for config_id, accepted in diag_configs:
            for cost_label, ticks, pips, purpose in COST_CASES:
                for seq, p in enumerate(accepted, start=1):
                    r = records[p]
                    ledger_rows.append({
                        "config_id": config_id,
                        "cost_label": cost_label,
                        "sequence": seq,
                        "signal_time": iso(r["signal_time"]),
                        "exit_time": iso(r["exit_time"]),
                        "signal_index": r["signal_index"],
                        "exit_index": r["exit_index"],
                        "reference_entry": r["reference_entry"],
                        "historical_fill": r[f"fill__{cost_label}"],
                        "stop": r["stop"],
                        "target": r["target"],
                        "exit_reason": r["exit_reason"],
                        "result_r": r[f"result_r__{cost_label}"],
                    })
                p_rows, y_rows, r_rows, rs_rows = diagnostics_for_config(records, config_id, accepted, cost_label)
                period_rows.extend(p_rows)
                year_rows.extend(y_rows)
                rolling_rows.extend(r_rows)
                rolling_summary_rows.extend(rs_rows)
        write_csv(OUTPUTS["screen_ledgers"], ledger_rows)
        write_csv(OUTPUTS["periods"], period_rows)
        write_csv(OUTPUTS["years"], year_rows)
        write_csv(OUTPUTS["rolling"], rolling_rows)
        write_csv(OUTPUTS["rolling_summary"], rolling_summary_rows)

        methodology = [
            {"topic": "research_version", "value": RESEARCH_VERSION},
            {"topic": "purpose", "value": "Pass 1B boundary clarification only; no new filter hunt and no candidate frozen automatically"},
            {"topic": "source_parity", "value": "H1/D1 rows and SHA256, raw exact-signal count/hash, scalar/vector parity, execution sample and exact Pass-1 leading-row accepted sequence must all pass before results are produced"},
            {"topic": "pattern", "value": "exact bearish body engulfing; unchanged from Pass 1"},
            {"topic": "frozen_cutoff", "value": iso(DATA_END)},
            {"topic": "execution", "value": "reference=signal close; stop=signal high+10 ticks; fixed RR3.50; exits next H1 candle onward; p0; exit-candle signal eligible"},
            {"topic": "costs", "value": "10T/1pip primary; 20T/2pip stressed selection; 40T/4pip extreme diagnostic. Assumed adverse historical fills, not measured historic quotes."},
            {"topic": "slices", "value": "56 structure-boundary + 56 LB100/D0.10 body-range + 56 LB150/D0.05 body-range + 256 compact joint-boundary cube = 424 planned configurations"},
            {"topic": "timing", "value": "No session or weekday optimisation"},
            {"topic": "rr", "value": "RR3.50 fixed; RR sweep remains a later final entry-stage only"},
            {"topic": "portfolio", "value": "Portfolio 30 is not used. Exact portfolio admission occurs only after standalone geometry, RR and independent implementation are frozen."},
            {"topic": "interpretation", "value": "Inspect stressed neighbourhoods, boundary occupancy, trade count and weak periods. A top row on a new boundary is not permission to freeze it."},
            {"topic": "data_snooping", "value": "All history remains repeatedly examined/in-sample; rolling/recent periods are robustness diagnostics, not unseen OOS."},
            {"topic": "next_gate", "value": "If an interior/stable region is now intelligible, freeze a small number of distinguishable anchors for Pass 2 one-feature-at-a-time testing; otherwise stop or widen only the still-constraining boundary."},
        ]
        write_csv(OUTPUTS["methodology"], methodology)

        if OUTPUTS["errors"].exists():
            OUTPUTS["errors"].unlink()
        pack_results()
        set_status(
            state="complete",
            progress=100,
            message="AUD/JPY H1 SHORT Pass 1B boundary clarification complete; ZIP ready",
            h1_candles=len(h1),
            d1_candles=len(d1),
            raw_exact_signals=len(vec_raw),
            raw_closed_outcomes=len(records),
            right_censored_raw_signals=censored,
            planned_configurations=EXPECTED_CONFIGS,
            completed_configurations=count,
            diagnostic_screen_count=len(selected_ids),
            hard_parity_passed=True,
            h1_source_sha256=h1_sha,
            raw_signal_sha256=vec_hash,
            pass1_control_sequence_sha256=control_hash,
            results_zip=str(BUNDLE),
        )

    except Exception as exc:
        tb = traceback.format_exc()
        write_csv(OUTPUTS["errors"], [{"error_type": type(exc).__name__, "message": str(exc), "traceback": tb}])
        try:
            pack_results()
        except Exception:
            pass
        set_status(state="failed", progress=100, message=f"{type(exc).__name__}: {exc}", hard_parity_passed=False)


# ============================================================
# FLASK ROUTES
# ============================================================

def launch_once():
    global RESEARCH_STARTED
    with RESEARCH_LOCK:
        if RESEARCH_STARTED:
            return False
        RESEARCH_STARTED = True
        threading.Thread(target=run_research, daemon=True, name="audjpy-h1-short-pass1b").start()
        return True


@app.route("/")
def root():
    return jsonify({
        "service": "AUD/JPY H1 SHORT Pass 1B boundary clarification",
        "research_only": True,
        "orders_supported": False,
        "trading_enabled": False,
        "pair": PAIR,
        "timeframe": TIMEFRAME,
        "side": SIDE,
        "rr_fixed": REFERENCE_RR,
        "planned_configurations": EXPECTED_CONFIGS,
        "frozen_end_exclusive": iso(DATA_END),
        "pass1_source_hash_required": EXPECTED_H1_SHA256,
        "pass1_raw_signal_hash_required": EXPECTED_RAW_SIGNAL_SHA256,
        "routes": [
            "/audjpy-h1-short-pass1b/start",
            "/audjpy-h1-short-pass1b/status",
            "/audjpy-h1-short-pass1b/results",
        ],
    })


@app.route("/audjpy-h1-short-pass1b/start")
def start_route():
    return jsonify({"started_now": launch_once(), "state": STATUS["state"], "orders_supported": False})


@app.route("/audjpy-h1-short-pass1b/status")
def status_route():
    with STATUS_LOCK:
        return jsonify(dict(STATUS))


@app.route("/audjpy-h1-short-pass1b/results")
def results_route():
    if not BUNDLE.exists():
        return jsonify({"status": "not_ready", "state": STATUS["state"], "message": STATUS["message"]}), 404
    return send_file(str(BUNDLE.resolve()), as_attachment=True, download_name=BUNDLE.name)


if __name__ == "__main__":
    launch_once()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)
