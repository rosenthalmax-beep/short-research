#!/usr/bin/env python3
"""
AUD/JPY H1 SHORT — Pass 4 RR-last sweep
========================================

RESEARCH ONLY. READ ONLY. NEVER PLACES ORDERS OR MODIFIES THE LIVE EXECUTOR.

Passes 1/1B/1C froze the exact-bearish-engulf entry geometry. Passes 2/3
resolved conditional features. This runner changes ONLY reward:risk and rebuilds
the complete signal->trade chronology independently for every RR because target
duration changes pyramiding-zero eligibility.

Frozen PRIMARY
--------------
- exact bearish engulf
- previous 175 H1-bar high
- ABS(signal high - previous 175-bar high) <= 0.10 ATR14
- bearish body >= 0.60 ATR14
- signal range >= 1.25 ATR14
- previous H1 candle close location >= 0.50

Frozen QUALITY
--------------
- all PRIMARY rules
- previous completed D1 ATR14 / 50-day mean <= 1.15

RR sweep
--------
2.50R through 4.50R in 0.25R increments. Historical adverse SHORT fill
assumptions remain 10T / 20T / 40T. Entry geometry, conditional filters,
timing and costs are NOT tuned here.

Fail-closed parity first reproduces the exact frozen Pass 3 PRIMARY and QUALITY
RR3.50 accepted ledgers at all three costs plus the frozen H1/D1/raw-signal
fingerprints. The D1 source is explicitly clipped to the Pass 3 last completed
D1 open so a later rerun cannot silently add a newly completed daily candle.

All examined history is in-sample. This is RR robustness evidence, not fresh
out-of-sample validation and not live-trading authorisation.

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
# FROZEN PROTOCOL
# ============================================================

PAIR = "AUD_JPY"
TIMEFRAME = "H1"
SIDE = "SHORT"
REQUESTED_START = datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc)
DATA_END = datetime(2026, 9, 30, 8, 0, tzinfo=timezone.utc)  # exclusive; exact Pass 3 cutoff
D1_WARMUP_START = REQUESTED_START - timedelta(days=900)
EXPECTED_D1_LAST_OPEN = datetime(2026, 9, 28, 21, 0, tzinfo=timezone.utc)

TICK = 0.001
PIP = 0.01
STOP_BUFFER_TICKS = 10
REFERENCE_RR = 3.50
RR_GRID = (2.50, 2.75, 3.00, 3.25, 3.50, 3.75, 4.00, 4.25, 4.50)
EXPECTED_RR_CONFIGS = 18
assert len(RR_GRID) * 2 == EXPECTED_RR_CONFIGS

COST_CASES = (
    ("LIVE_LIMIT_10T", 10, 1.0, "PRIMARY_LIVE_PARITY"),
    ("STRESS_20T", 20, 2.0, "STRESSED_SELECTION"),
    ("EXTREME_40T", 40, 4.0, "EXTREME_DIAGNOSTIC_ONLY"),
)
PRIMARY_COST_LABEL = "LIVE_LIMIT_10T"
STRESS_COST_LABEL = "STRESS_20T"
EXTREME_COST_LABEL = "EXTREME_40T"

STRUCTURE_LOOKBACKS = (175,)
GRID_LOOKBACKS = (175,)
MOMENTUM_LOOKBACKS = (4, 8, 12, 24, 48)

ANCHORS = {
    "CORE": dict(lookback=175, distance_atr_max=0.10, body_atr_min=0.60, range_atr_min=1.25),
}

# Retained only so inherited Pass-3 helper functions remain self-contained;
# no Pass-3 search is executed in this runner.
PREVIOUS_CLOSE_LOCATION_LEVELS = (0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60)
D1_ATR_MIN_LEVELS = (0.65, 0.70, 0.75, 0.80, 0.85, 0.90)
D1_ATR_MAX_LEVELS = (1.05, 1.10, 1.15, 1.20, 1.25, 1.30)
PRIOR_RISE_LB24_LEVELS = (0.25, 0.50, 0.75, 1.00)

EXPECTED_H1_ROWS = 138030
EXPECTED_D1_ROWS = 6476
EXPECTED_H1_SHA256 = "5b844d2f79af6ae49e7f36586f68c366d4884859f897792bdb09ebe4e7bfcdfe"
EXPECTED_D1_SHA256 = "bb43365840ff5d38d8d3ed3260c4d74204a927780e4619074ea3f9e63bcaba59"
EXPECTED_RAW_SIGNAL_COUNT = 10465
EXPECTED_RAW_SIGNAL_SHA256 = "fc53892ad7e01e06b49594108b8b1fe8047ade46ec33e0d3a8d554c84322f3ab"

# Frozen CORE anchor is retained as an upstream mechanics checkpoint.
ANCHOR_EXPECTED = {
    "CORE": {
        "qualified_raw_signals": 100,
        "accepted_trades": 96,
        "LIVE_LIMIT_10T": dict(total_r=51.05130586762477, profit_factor=1.823408159155238, max_drawdown_r=-9.709302325581533, ledger_sha256="ee0eb7148549d252b1155e7404e9770fd677b790684a56cd122c3a334a34bdf9"),
        "STRESS_20T": dict(total_r=45.61181431915988, profit_factor=1.7356744245025788, max_drawdown_r=-9.899999999999965, ledger_sha256="4a9b7f9873cbc860f9e9ee11f83e97c827045be3b3f6b0f67ad7ec86015b7865"),
        "EXTREME_40T": dict(total_r=35.99669387216922, profit_factor=1.5805918366478906, max_drawdown_r=-10.234693877551152, ledger_sha256="ec44209e9db2d06c0a66ae9d81a34b96bc10a5e193f13e9e5798ce87ab03aba3"),
    },
}

FROZEN_VARIANTS = {
    "PRIMARY": {
        "description": "CORE + previous H1 close location >= 0.50",
        "previous_close_location_min": 0.50,
        "daily_atr_ratio_max": None,
    },
    "QUALITY": {
        "description": "CORE + previous H1 close location >= 0.50 + completed-D1 ATR14/50mean <= 1.15",
        "previous_close_location_min": 0.50,
        "daily_atr_ratio_max": 1.15,
    },
}

RR35_EXPECTED = {
    "PRIMARY": {
        "qualified": 94, "accepted": 91,
        "LIVE_LIMIT_10T": dict(total_r=56.05130586762477, profit_factor=1.9833562432916625, max_drawdown_r=-8.0, ledger_sha256="5ba356f3be4e25ba7b1573260a87ca82b51af5a6c418b296f83450dcc8bbe207"),
        "STRESS_20T": dict(total_r=50.61181431915988, profit_factor=1.8879265670028051, max_drawdown_r=-8.133802816901252, ledger_sha256="9c557d21f6ff2a0afe2ac1960f63ea4d178181f794323b57dcc1bed43a8a6409"),
        "EXTREME_40T": dict(total_r=40.99669387216922, profit_factor=1.7192402433713898, max_drawdown_r=-8.6111111111112, ledger_sha256="b7bd7de7394753482330a310a54c183e7e3979db0fcdf40e2cc3b4384f61d380"),
    },
    "QUALITY": {
        "qualified": 81, "accepted": 78,
        "LIVE_LIMIT_10T": dict(total_r=55.77881402981588, profit_factor=2.186783277230125, max_drawdown_r=-5.0, ledger_sha256="871ca41f621f4b1fc289de8aafeca1ce7a2d8642c0be15dc313bf5b960b829be"),
        "STRESS_20T": dict(total_r=50.558709503399804, profit_factor=2.0757172234765915, max_drawdown_r=-5.13380281690125, ledger_sha256="03b22d690116b6ebbeb16598ba6f1eb69f6dfb07c993bbd59fae84a828c7d4b2"),
        "EXTREME_40T": dict(total_r=41.359794867805064, profit_factor=1.879995635485214, max_drawdown_r=-5.6111111111111995, ledger_sha256="d0d1813fad83f87ead8dae902fb1eda40a07aab2939dca0a595421c102437acf"),
    },
}

PASS_VERSION = "AUDJPY_H1_SHORT_PASS4_RR_SWEEP_V1_2026-09-30"

API = os.getenv("OANDA_API_URL", "https://api-fxtrade.oanda.com").rstrip("/")
TOKEN = os.getenv("OANDA_TOKEN", "")

OUT_DIR = Path(os.getenv("AUDJPY_H1_SHORT_PASS4_OUTPUT_DIR", "/tmp/audjpy_h1_short_pass4"))
OUT_DIR.mkdir(parents=True, exist_ok=True)
BUNDLE = OUT_DIR / "AUDJPY_H1_SHORT_PASS4_RR_SWEEP_RESULTS.zip"

OUTPUTS = {
    "coverage": OUT_DIR / "coverage.csv",
    "source_fingerprint": OUT_DIR / "source_fingerprint.csv",
    "parity": OUT_DIR / "parity.csv",
    "rr35_parity": OUT_DIR / "rr35_frozen_variant_parity.csv",
    "rr_plan": OUT_DIR / "rr_sweep_plan.csv",
    "summary": OUT_DIR / "rr_summary.csv",
    "comparison": OUT_DIR / "rr_comparison_vs_3p50.csv",
    "ledgers": OUT_DIR / "rr_full_accepted_ledgers.csv",
    "periods": OUT_DIR / "rr_periods.csv",
    "years": OUT_DIR / "rr_calendar_years.csv",
    "rolling": OUT_DIR / "rr_rolling_12_24_36m.csv",
    "rolling_summary": OUT_DIR / "rr_rolling_summary.csv",
    "overlap": OUT_DIR / "rr_accepted_signal_overlap_vs_3p50.csv",
    "methodology": OUT_DIR / "methodology.csv",
    "errors": OUT_DIR / "error_report.csv",
}

STATUS = {
    "state": "not_started",
    "progress": 0,
    "message": "AUD/JPY H1 SHORT Pass 4 RR sweep waiting",
    "orders_supported": False,
    "trading_enabled": False,
    "pair": PAIR,
    "timeframe": TIMEFRAME,
    "side": SIDE,
    "rr_grid": list(RR_GRID),
    "pass_version": PASS_VERSION,
    "frozen_variants": FROZEN_VARIANTS,
    "rr_configurations": EXPECTED_RR_CONFIGS,
    "frozen_pass3_cutoff": DATA_END.isoformat().replace("+00:00", "Z"),
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
    ph = np.full(len(candles), np.nan)
    pl = np.full(len(candles), np.nan)
    if len(candles) > 1:
        po[1:] = opens[:-1]
        pc[1:] = closes[:-1]
        ph[1:] = highs[:-1]
        pl[1:] = lows[:-1]

    previous_body = np.abs(pc - po)
    previous_range = ph - pl
    previous_body_atr = np.divide(
        previous_body, atr, out=np.full(len(candles), np.nan),
        where=np.isfinite(previous_body) & np.isfinite(atr) & (atr > 0),
    )
    previous_range_atr = np.divide(
        previous_range, atr, out=np.full(len(candles), np.nan),
        where=np.isfinite(previous_range) & np.isfinite(atr) & (atr > 0),
    )
    previous_close_location = np.divide(
        pc - pl, previous_range, out=np.full(len(candles), np.nan),
        where=np.isfinite(previous_range) & (previous_range > 0),
    )

    bearish_body = opens - closes
    candle_range = highs - lows
    upper_wick = highs - np.maximum(opens, closes)
    body_atr = np.divide(
        bearish_body, atr, out=np.zeros_like(closes),
        where=(bearish_body > 0) & np.isfinite(atr) & (atr > 0),
    )
    range_atr = np.divide(
        candle_range, atr, out=np.zeros_like(closes),
        where=np.isfinite(atr) & (atr > 0),
    )
    bear_ratio = np.divide(
        bearish_body, previous_body, out=np.zeros_like(closes),
        where=(bearish_body > 0) & (previous_body > 0),
    )
    close_location = np.divide(
        closes - lows, candle_range, out=np.zeros_like(closes), where=candle_range > 0,
    )
    upper_wick_body = np.divide(
        upper_wick, bearish_body, out=np.zeros_like(closes), where=bearish_body > 0,
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
        atr, atr50_prev, out=np.full(len(candles), np.nan),
        where=np.isfinite(atr50_prev) & (atr50_prev > 0),
    )

    momentum = {}
    for lb in MOMENTUM_LOOKBACKS:
        arr = np.full(len(candles), np.nan)
        for i in range(lb + 1, len(candles)):
            if math.isfinite(atr[i]) and atr[i] > 0:
                arr[i] = (closes[i - 1] - closes[i - 1 - lb]) / atr[i]
        momentum[lb] = arr

    return {
        "times": times, "open": opens, "high": highs, "low": lows, "close": closes, "atr": atr,
        "exact_bear": exact_bear, "bear_ratio": bear_ratio, "body_atr": body_atr,
        "range_atr": range_atr, "close_location": close_location, "upper_wick_body": upper_wick_body,
        "previous_body_atr": previous_body_atr, "previous_range_atr": previous_range_atr,
        "previous_close_location": previous_close_location, "prev_highs": prev_highs,
        "h1_atr_ratio": h1_atr_ratio, "momentum": momentum,
    }


def build_daily_states(daily, h1_times):
    d_close = np.array([x["close"] for x in daily], dtype=float)
    d_atr = atr14(daily)
    d_ema50 = ema(d_close, 50)
    d_ema100 = ema(d_close, 100)
    d_ema200 = ema(d_close, 200)
    d_atr50_prev = rolling_previous_mean(d_atr, 50)
    d_atr_ratio = np.divide(
        d_atr, d_atr50_prev, out=np.full(len(daily), np.nan),
        where=np.isfinite(d_atr50_prev) & (d_atr50_prev > 0),
    )

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
            reason = "TARGET" if abs(opn[i] - low[i]) < abs(high[i] - opn[i]) else "STOP"
        else:
            reason = "STOP" if s else "TARGET"
        return i, reason
    return None, None


def result_r_for_cost(reference_entry, stop, target, reason, cost_pips):
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
            set_status(message=f"Building raw exact-bearish-engulf outcomes {k}/{len(raw_indices)}")
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
            "raw_position": len(records), "signal_index": int(i), "exit_index": int(exit_index),
            "signal_time": features["times"][i], "exit_time": features["times"][exit_index],
            "reference_entry": entry, "stop": stop, "target": target, "exit_reason": reason,
            "bear_ratio": float(features["bear_ratio"][i]),
            "body_atr": float(features["body_atr"][i]),
            "range_atr": float(features["range_atr"][i]),
            "close_location": float(features["close_location"][i]),
            "upper_wick_body": float(features["upper_wick_body"][i]),
            "previous_body_atr": float(features["previous_body_atr"][i]) if math.isfinite(features["previous_body_atr"][i]) else math.nan,
            "previous_range_atr": float(features["previous_range_atr"][i]) if math.isfinite(features["previous_range_atr"][i]) else math.nan,
            "previous_close_location": float(features["previous_close_location"][i]) if math.isfinite(features["previous_close_location"][i]) else math.nan,
            "h1_atr_ratio": float(features["h1_atr_ratio"][i]) if math.isfinite(features["h1_atr_ratio"][i]) else math.nan,
            "daily_close_lt_ema50": bool(daily_states["close_lt_ema50"][i]),
            "daily_close_lt_ema100": bool(daily_states["close_lt_ema100"][i]),
            "daily_close_lt_ema200": bool(daily_states["close_lt_ema200"][i]),
            "daily_ema50_lt_ema200": bool(daily_states["ema50_lt_ema200"][i]),
            "daily_atr_ratio": float(daily_states["atr_ratio"][i]) if math.isfinite(daily_states["atr_ratio"][i]) else math.nan,
            "hour_utc": features["times"][i].hour, "weekday_utc": features["times"][i].weekday(),
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
    failures = []
    for row in records[:sample_n]:
        i = row["signal_index"]
        entry = float(candles[i]["close"])
        stop = float(candles[i]["high"]) + STOP_BUFFER_TICKS * TICK
        target = entry - REFERENCE_RR * (stop - entry)
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
        "upper_wick_body", "previous_body_atr", "previous_range_atr",
        "previous_close_location", "h1_atr_ratio", "daily_atr_ratio",
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
# PASS 3 EXPERIMENT PLAN — BOUNDARIES + JUSTIFIED INTERACTIONS
# ============================================================

def fmt_level(value):
    if isinstance(value, int):
        return str(value)
    return f"{float(value):.3f}".rstrip("0").rstrip(".")


def anchor_mask(arr, anchor_name):
    p = ANCHORS[anchor_name]
    d = arr[f"structure_dist_{p['lookback']}"]
    return (
        np.isfinite(d)
        & (d <= p["distance_atr_max"])
        & (arr["body_atr"] >= p["body_atr_min"])
        & (arr["range_atr"] >= p["range_atr_min"])
    )


def pass3_experiment_plan(arr):
    """Yield only predeclared Pass 3 boundary rows and justified interactions."""
    n = len(arr["previous_close_location"])
    base = np.ones(n, dtype=bool)
    pcl = arr["previous_close_location"]
    d1v = arr["daily_atr_ratio"]
    rise24 = arr["momentum_24"]
    ema_ge = ~arr["daily_ema50_lt_ema200"]

    # A) Pure boundary clarification rows.
    for v in PREVIOUS_CLOSE_LOCATION_LEVELS:
        yield dict(
            factor_id=f"PREV_CLOSE_LOC_MIN_{fmt_level(v)}",
            family="boundary_previous_close", operator=">=",
            parameter="previous_close_location", threshold=v,
            mask=base & np.isfinite(pcl) & (pcl >= v),
        )
    for v in D1_ATR_MIN_LEVELS:
        yield dict(
            factor_id=f"D1_ATR_RATIO_MIN_{fmt_level(v)}",
            family="boundary_d1_atr_min", operator=">=",
            parameter="daily_atr_ratio", threshold=v,
            mask=base & np.isfinite(d1v) & (d1v >= v),
        )
    for v in D1_ATR_MAX_LEVELS:
        yield dict(
            factor_id=f"D1_ATR_RATIO_MAX_{fmt_level(v)}",
            family="boundary_d1_atr_max", operator="<=",
            parameter="daily_atr_ratio", threshold=v,
            mask=base & np.isfinite(d1v) & (d1v <= v),
        )

    # B) Previous close x completed-D1 ATR minimum.
    for pc in PREVIOUS_CLOSE_LOCATION_LEVELS:
        for dv in D1_ATR_MIN_LEVELS:
            yield dict(
                factor_id=f"PREV_CLOSE_MIN_{fmt_level(pc)}__D1_ATR_MIN_{fmt_level(dv)}",
                family="interaction_prevclose_x_d1atr_min", operator="AND",
                parameter="previous_close_location & daily_atr_ratio",
                threshold=f">={fmt_level(pc)} & >={fmt_level(dv)}",
                mask=(base & np.isfinite(pcl) & np.isfinite(d1v) & (pcl >= pc) & (d1v >= dv)),
            )

    # C) Previous close x completed-D1 ATR maximum.
    for pc in PREVIOUS_CLOSE_LOCATION_LEVELS:
        for dv in D1_ATR_MAX_LEVELS:
            yield dict(
                factor_id=f"PREV_CLOSE_MIN_{fmt_level(pc)}__D1_ATR_MAX_{fmt_level(dv)}",
                family="interaction_prevclose_x_d1atr_max", operator="AND",
                parameter="previous_close_location & daily_atr_ratio",
                threshold=f">={fmt_level(pc)} & <={fmt_level(dv)}",
                mask=(base & np.isfinite(pcl) & np.isfinite(d1v) & (pcl >= pc) & (d1v <= dv)),
            )

    # D) Previous close x prior 24-H1-bar rise.
    for pc in PREVIOUS_CLOSE_LOCATION_LEVELS:
        for mv in PRIOR_RISE_LB24_LEVELS:
            yield dict(
                factor_id=f"PREV_CLOSE_MIN_{fmt_level(pc)}__PRIOR_RISE_LB24_MIN_{fmt_level(mv)}",
                family="interaction_prevclose_x_priorrise24", operator="AND",
                parameter="previous_close_location & momentum_24",
                threshold=f">={fmt_level(pc)} & >={fmt_level(mv)}",
                mask=(base & np.isfinite(pcl) & np.isfinite(rise24) & (pcl >= pc) & (rise24 >= mv)),
            )

    # E) Previous close x D1 EMA50>=EMA200 quality regime.
    for pc in PREVIOUS_CLOSE_LOCATION_LEVELS:
        yield dict(
            factor_id=f"PREV_CLOSE_MIN_{fmt_level(pc)}__D1_EMA50_GE_EMA200",
            family="interaction_prevclose_x_d1ema50ge200", operator="AND",
            parameter="previous_close_location & d1_ema50_ge_ema200",
            threshold=f">={fmt_level(pc)} & true",
            mask=(base & np.isfinite(pcl) & (pcl >= pc) & ema_ge),
        )

    # F/G) D1 ATR x D1 EMA50>=EMA200 secondary quality interactions.
    for dv in D1_ATR_MIN_LEVELS:
        yield dict(
            factor_id=f"D1_ATR_MIN_{fmt_level(dv)}__D1_EMA50_GE_EMA200",
            family="interaction_d1atr_min_x_d1ema50ge200", operator="AND",
            parameter="daily_atr_ratio & d1_ema50_ge_ema200",
            threshold=f">={fmt_level(dv)} & true",
            mask=(base & np.isfinite(d1v) & (d1v >= dv) & ema_ge),
        )
    for dv in D1_ATR_MAX_LEVELS:
        yield dict(
            factor_id=f"D1_ATR_MAX_{fmt_level(dv)}__D1_EMA50_GE_EMA200",
            family="interaction_d1atr_max_x_d1ema50ge200", operator="AND",
            parameter="daily_atr_ratio & d1_ema50_ge_ema200",
            threshold=f"<={fmt_level(dv)} & true",
            mask=(base & np.isfinite(d1v) & (d1v <= dv) & ema_ge),
        )


def accepted_ledger_hash(records, accepted, cost_label):
    return sha_rows(
        "|".join((
            iso(records[p]["signal_time"]),
            iso(records[p]["exit_time"]),
            str(records[p]["signal_index"]),
            str(records[p]["exit_index"]),
            records[p]["exit_reason"],
            f'{records[p][f"result_r__{cost_label}"]:.15g}',
        ))
        for p in accepted
    )


def compare_to_anchor(records, anchor_accepted, candidate_accepted, cost_label):
    a = set(anchor_accepted)
    c = set(candidate_accepted)
    retained = a & c
    removed = a - c
    newly = c - a
    am = metrics(records, anchor_accepted, cost_label)
    cm = metrics(records, candidate_accepted, cost_label)
    return {
        "anchor_accepted_trades": len(anchor_accepted),
        "candidate_accepted_trades": len(candidate_accepted),
        "accepted_trade_retention_pct": 100.0 * len(candidate_accepted) / len(anchor_accepted) if anchor_accepted else 0.0,
        "retained_anchor_accepted": len(retained),
        "removed_anchor_accepted": len(removed),
        "newly_accepted_due_to_p0_replay": len(newly),
        "removed_anchor_r": sum(float(records[p][f"result_r__{cost_label}"]) for p in removed),
        "newly_accepted_r": sum(float(records[p][f"result_r__{cost_label}"]) for p in newly),
        "anchor_total_r": am["total_r"],
        "candidate_total_r": cm["total_r"],
        "delta_total_r": cm["total_r"] - am["total_r"],
        "anchor_expectancy_r": am["expectancy_r"],
        "candidate_expectancy_r": cm["expectancy_r"],
        "delta_expectancy_r": cm["expectancy_r"] - am["expectancy_r"],
        "anchor_profit_factor": am["profit_factor"],
        "candidate_profit_factor": cm["profit_factor"],
        "delta_profit_factor": cm["profit_factor"] - am["profit_factor"],
        "anchor_max_drawdown_r": am["max_drawdown_r"],
        "candidate_max_drawdown_r": cm["max_drawdown_r"],
        "delta_max_drawdown_r": cm["max_drawdown_r"] - am["max_drawdown_r"],
    }


def family_summary_rows(summary_rows, delta_rows):
    # Stress-case description only; this does not select/freeze a candidate.
    stress_summ = {r["config_id"]: r for r in summary_rows if r["cost_label"] == STRESS_COST_LABEL}
    stress_delta = {r["config_id"]: r for r in delta_rows if r["cost_label"] == STRESS_COST_LABEL}
    out = []
    keys = sorted({(r["anchor"], r["factor_family"]) for r in summary_rows})
    for anchor, family in keys:
        ids = sorted({r["config_id"] for r in summary_rows if r["anchor"] == anchor and r["factor_family"] == family})
        rows = [stress_summ[x] for x in ids]
        dels = [stress_delta[x] for x in ids]
        best_delta = max(dels, key=lambda x: x["delta_total_r"])
        best_exp = max(dels, key=lambda x: x["candidate_expectancy_r"])
        out.append({
            "anchor": anchor,
            "factor_family": family,
            "configurations": len(ids),
            "positive_total_r_configs": sum(float(x["total_r"]) > 0 for x in rows),
            "improved_total_r_vs_anchor": sum(float(x["delta_total_r"]) > 0 for x in dels),
            "improved_expectancy_vs_anchor": sum(float(x["delta_expectancy_r"]) > 0 for x in dels),
            "median_trade_retention_pct": float(median(float(x["accepted_trade_retention_pct"]) for x in dels)),
            "median_delta_total_r": float(median(float(x["delta_total_r"]) for x in dels)),
            "best_delta_total_r_config": best_delta["config_id"],
            "best_delta_total_r": best_delta["delta_total_r"],
            "best_expectancy_config": best_exp["config_id"],
            "best_expectancy_r": best_exp["candidate_expectancy_r"],
        })
    return out


def diagnostic_screen_rows(summary_rows, delta_rows):
    """Mechanical diagnostics only: up to two non-duplicate rows per family/anchor."""
    by_cost = defaultdict(dict)
    for r in summary_rows:
        by_cost[r["config_id"]][r["cost_label"]] = r
    stress_delta = {r["config_id"]: r for r in delta_rows if r["cost_label"] == STRESS_COST_LABEL}
    out = []
    for anchor in ANCHORS:
        families = sorted({r["factor_family"] for r in summary_rows if r["anchor"] == anchor})
        for family in families:
            ids = sorted({r["config_id"] for r in summary_rows if r["anchor"] == anchor and r["factor_family"] == family})
            eligible = []
            for cid in ids:
                live = by_cost[cid].get(PRIMARY_COST_LABEL)
                stress = by_cost[cid].get(STRESS_COST_LABEL)
                d = stress_delta[cid]
                if not live or not stress:
                    continue
                if float(live["total_r"]) <= 0 or float(stress["total_r"]) <= 0:
                    continue
                if float(d["accepted_trade_retention_pct"]) < 50.0:
                    continue
                eligible.append(cid)
            if not eligible:
                continue
            picks = []
            picks.append(max(eligible, key=lambda cid: float(stress_delta[cid]["delta_total_r"])))
            picks.append(max(eligible, key=lambda cid: float(stress_delta[cid]["candidate_expectancy_r"])))
            seen = set()
            for reason, cid in zip(("BEST_STRESS_DELTA_R_WITH_GE50PCT_RETENTION", "BEST_STRESS_EXPECTANCY_WITH_GE50PCT_RETENTION"), picks):
                if cid in seen:
                    continue
                seen.add(cid)
                s = by_cost[cid][STRESS_COST_LABEL]
                d = stress_delta[cid]
                out.append({
                    "anchor": anchor,
                    "factor_family": family,
                    "config_id": cid,
                    "diagnostic_reason": reason,
                    "selection_status": "DIAGNOSTIC_ONLY_NOT_FROZEN",
                    "stress20T_trades": s["accepted_trades"],
                    "stress20T_total_r": s["total_r"],
                    "stress20T_pf": s["profit_factor"],
                    "stress20T_expectancy_r": s["expectancy_r"],
                    "trade_retention_pct": d["accepted_trade_retention_pct"],
                    "delta_total_r_vs_anchor": d["delta_total_r"],
                    "delta_expectancy_vs_anchor": d["delta_expectancy_r"],
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

# ============================================================
# PASS 4 — FROZEN VARIANTS + RR-LAST REPLAY
# ============================================================

def build_raw_outcomes_rr(features, raw_indices, daily_states, rr):
    records = []
    censored = 0
    for k, i in enumerate(raw_indices):
        if k % 1000 == 0:
            set_status(message=f"Building RR{rr:.2f} raw outcomes {k}/{len(raw_indices)}")
        entry = float(features["close"][i])
        stop = float(features["high"][i]) + STOP_BUFFER_TICKS * TICK
        ref_risk = stop - entry
        if ref_risk <= 0:
            continue
        target = entry - float(rr) * ref_risk
        exit_index, reason = find_exit(features, int(i), stop, target)
        if exit_index is None:
            censored += 1
            continue
        row = {
            "raw_position": len(records), "signal_index": int(i), "exit_index": int(exit_index),
            "signal_time": features["times"][i], "exit_time": features["times"][exit_index],
            "reference_entry": entry, "stop": stop, "target": target, "exit_reason": reason,
            "bear_ratio": float(features["bear_ratio"][i]),
            "body_atr": float(features["body_atr"][i]),
            "range_atr": float(features["range_atr"][i]),
            "close_location": float(features["close_location"][i]),
            "upper_wick_body": float(features["upper_wick_body"][i]),
            "previous_body_atr": float(features["previous_body_atr"][i]) if math.isfinite(features["previous_body_atr"][i]) else math.nan,
            "previous_range_atr": float(features["previous_range_atr"][i]) if math.isfinite(features["previous_range_atr"][i]) else math.nan,
            "previous_close_location": float(features["previous_close_location"][i]) if math.isfinite(features["previous_close_location"][i]) else math.nan,
            "h1_atr_ratio": float(features["h1_atr_ratio"][i]) if math.isfinite(features["h1_atr_ratio"][i]) else math.nan,
            "daily_close_lt_ema50": bool(daily_states["close_lt_ema50"][i]),
            "daily_close_lt_ema100": bool(daily_states["close_lt_ema100"][i]),
            "daily_close_lt_ema200": bool(daily_states["close_lt_ema200"][i]),
            "daily_ema50_lt_ema200": bool(daily_states["ema50_lt_ema200"][i]),
            "daily_atr_ratio": float(daily_states["atr_ratio"][i]) if math.isfinite(daily_states["atr_ratio"][i]) else math.nan,
            "hour_utc": features["times"][i].hour, "weekday_utc": features["times"][i].weekday(),
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


def execution_parity_sample_rr(features, candles, records, rr, sample_n=100):
    failures = []
    for row in records[:sample_n]:
        i = row["signal_index"]
        entry = float(candles[i]["close"])
        stop = float(candles[i]["high"]) + STOP_BUFFER_TICKS * TICK
        target = entry - float(rr) * (stop - entry)
        exit_i, reason = scalar_exit_from_candles(candles, i, stop, target)
        if exit_i != row["exit_index"] or reason != row["exit_reason"]:
            failures.append(f"exit mismatch {i}")
            continue
        for label, ticks, pips, purpose in COST_CASES:
            value, fill = result_r_for_cost(entry, stop, target, reason, pips)
            if abs(value - row[f"result_r__{label}"]) > 1e-12:
                failures.append(f"R mismatch {i} {label}")
    return failures


def frozen_variant_mask(arr, variant_id):
    base = anchor_mask(arr, "CORE")
    spec = FROZEN_VARIANTS[variant_id]
    pcl = arr["previous_close_location"]
    mask = base & np.isfinite(pcl) & (pcl >= spec["previous_close_location_min"])
    vmax = spec["daily_atr_ratio_max"]
    if vmax is not None:
        d1v = arr["daily_atr_ratio"]
        mask = mask & np.isfinite(d1v) & (d1v <= vmax)
    return mask


def rr_config_id(variant_id, rr):
    return f"{variant_id}__RR_{rr:.2f}".replace(".", "p")


def signal_key(row):
    return (iso(row["signal_time"]), int(row["signal_index"]))


def run_research():
    try:
        set_status(state="fetching", progress=2, message="Fetching exact frozen Pass 3 H1/D1 OANDA midpoint history")
        h1 = fetch_history("H1", REQUESTED_START, DATA_END, 180)
        d1_all = fetch_history("D", D1_WARMUP_START, DATA_END, 1200)
        # Freeze D1 as-of Pass 3 so a later fetch cannot silently include a newly completed D1 bar.
        d1 = [x for x in d1_all if x["time"] <= EXPECTED_D1_LAST_OPEN]

        h1_times = [x["time"] for x in h1]
        d1_times = [x["time"] for x in d1]
        if h1_times != sorted(set(h1_times)):
            raise RuntimeError("H1 timestamps are duplicated or non-monotonic")
        if d1_times != sorted(set(d1_times)):
            raise RuntimeError("D1 timestamps are duplicated or non-monotonic")
        if not h1 or not d1 or h1[-1]["time"] >= DATA_END:
            raise RuntimeError("Frozen source coverage invalid")
        if d1[-1]["time"] != EXPECTED_D1_LAST_OPEN:
            raise RuntimeError(f"D1 frozen last-open mismatch: {iso(d1[-1]['time'])} vs {iso(EXPECTED_D1_LAST_OPEN)}")

        h1_sha = sha_rows(f"{iso(x['time'])}|{x['open']:.6f}|{x['high']:.6f}|{x['low']:.6f}|{x['close']:.6f}" for x in h1)
        d1_sha = sha_rows(f"{iso(x['time'])}|{x['open']:.6f}|{x['high']:.6f}|{x['low']:.6f}|{x['close']:.6f}" for x in d1)
        write_csv(OUTPUTS["coverage"], [
            {"pair":PAIR,"timeframe":"H1","requested_start":iso(REQUESTED_START),"first_completed_candle":iso(h1[0]["time"]),"last_completed_candle_open":iso(h1[-1]["time"]),"frozen_end_exclusive":iso(DATA_END),"completed_candles":len(h1)},
            {"pair":PAIR,"timeframe":"D","requested_start":iso(D1_WARMUP_START),"first_completed_candle":iso(d1[0]["time"]),"last_completed_candle_open":iso(d1[-1]["time"]),"frozen_end_exclusive":iso(DATA_END),"completed_candles":len(d1),"daily_alignment":"17:00 America/New_York","fetched_complete_rows_before_freeze":len(d1_all)},
        ])
        write_csv(OUTPUTS["source_fingerprint"], [
            {"series":"AUD_JPY_H1_MID_OHLC","sha256":h1_sha,"rows":len(h1)},
            {"series":"AUD_JPY_D1_MID_OHLC_FROZEN_ASOF_PASS3","sha256":d1_sha,"rows":len(d1)},
        ])

        checks = [
            {"check":"H1_ROW_COUNT","status":"PASS" if len(h1)==EXPECTED_H1_ROWS else "FAIL","actual":len(h1),"expected":EXPECTED_H1_ROWS},
            {"check":"D1_ROW_COUNT_AFTER_FREEZE","status":"PASS" if len(d1)==EXPECTED_D1_ROWS else "FAIL","actual":len(d1),"expected":EXPECTED_D1_ROWS},
            {"check":"D1_LAST_OPEN_FREEZE","status":"PASS" if d1[-1]["time"]==EXPECTED_D1_LAST_OPEN else "FAIL","actual":iso(d1[-1]["time"]),"expected":iso(EXPECTED_D1_LAST_OPEN)},
            {"check":"H1_SOURCE_SHA256","status":"PASS" if h1_sha==EXPECTED_H1_SHA256 else "FAIL","actual":h1_sha,"expected":EXPECTED_H1_SHA256},
            {"check":"D1_SOURCE_SHA256","status":"PASS" if d1_sha==EXPECTED_D1_SHA256 else "FAIL","actual":d1_sha,"expected":EXPECTED_D1_SHA256},
        ]
        if any(x["status"] != "PASS" for x in checks):
            write_csv(OUTPUTS["parity"], checks)
            raise RuntimeError("Frozen source parity FAILED; do not interpret RR sweep")

        set_status(state="features", progress=9, message="Building H1/D1 features and raw-signal parity")
        features = build_h1_features(h1)
        daily_states = build_daily_states(d1, features["times"])
        vec_raw = raw_exact_vector_indices(features)
        scalar_raw = raw_exact_scalar_indices(h1, features["atr"])
        vec_hash = sha_rows(iso(features["times"][i]) for i in vec_raw)
        scalar_hash = sha_rows(iso(features["times"][i]) for i in scalar_raw)
        raw_ok = (
            len(vec_raw)==EXPECTED_RAW_SIGNAL_COUNT
            and len(scalar_raw)==EXPECTED_RAW_SIGNAL_COUNT
            and np.array_equal(vec_raw,scalar_raw)
            and vec_hash==scalar_hash==EXPECTED_RAW_SIGNAL_SHA256
        )
        checks.append({
            "check":"RAW_SIGNAL_VECTOR_SCALAR_AND_PASS3_FINGERPRINT",
            "status":"PASS" if raw_ok else "FAIL",
            "vector_count":len(vec_raw),"scalar_count":len(scalar_raw),
            "vector_sha256":vec_hash,"scalar_sha256":scalar_hash,
            "expected_count":EXPECTED_RAW_SIGNAL_COUNT,"expected_sha256":EXPECTED_RAW_SIGNAL_SHA256,
        })
        if not raw_ok:
            write_csv(OUTPUTS["parity"], checks)
            raise RuntimeError("Raw signal parity/fingerprint FAILED")

        # Reference-RR replay is a hard upstream control before the sweep.
        set_status(state="rr35_parity", progress=15, message="Replaying RR3.50 controls before RR sweep")
        control_records, control_censored = build_raw_outcomes_rr(features, vec_raw, daily_states, REFERENCE_RR)
        exec_failures = execution_parity_sample_rr(features, h1, control_records, REFERENCE_RR, sample_n=min(100,len(control_records)))
        checks.append({"check":"RR3P50_EXECUTION_SCALAR_RECOMPUTE_FIRST_100","status":"PASS" if not exec_failures else "FAIL","failures":";".join(exec_failures[:10])})
        write_csv(OUTPUTS["parity"], checks)
        if exec_failures:
            raise RuntimeError("RR3.50 scalar execution parity FAILED")

        control_arr = all_record_arrays(control_records)

        # Upstream CORE control.
        core_mask = anchor_mask(control_arr, "CORE")
        core_qualified = int(np.sum(core_mask))
        core_accepted = replay_positions(control_records, np.flatnonzero(core_mask).astype(int).tolist())
        core_rows=[]
        for label,ticks,pips,purpose in COST_CASES:
            m=metrics(control_records,core_accepted,label)
            sha=accepted_ledger_hash(control_records,core_accepted,label)
            exp=ANCHOR_EXPECTED["CORE"]
            tgt=exp[label]
            ok=(core_qualified==exp["qualified_raw_signals"] and len(core_accepted)==exp["accepted_trades"] and abs(m["total_r"]-tgt["total_r"])<1e-10 and abs(m["profit_factor"]-tgt["profit_factor"])<1e-10 and abs(m["max_drawdown_r"]-tgt["max_drawdown_r"])<1e-10 and sha==tgt["ledger_sha256"])
            core_rows.append({"variant_id":"UPSTREAM_CORE","rr":REFERENCE_RR,"cost_label":label,"status":"PASS" if ok else "FAIL","qualified_raw_signals":core_qualified,"expected_qualified_raw_signals":exp["qualified_raw_signals"],"accepted_trades":len(core_accepted),"expected_accepted_trades":exp["accepted_trades"],"total_r":m["total_r"],"expected_total_r":tgt["total_r"],"profit_factor":m["profit_factor"],"expected_profit_factor":tgt["profit_factor"],"max_drawdown_r":m["max_drawdown_r"],"expected_max_drawdown_r":tgt["max_drawdown_r"],"ledger_sha256":sha,"expected_ledger_sha256":tgt["ledger_sha256"]})

        rr35_rows=list(core_rows)
        for vid in FROZEN_VARIANTS:
            mask=frozen_variant_mask(control_arr,vid)
            qualified=int(np.sum(mask))
            accepted=replay_positions(control_records,np.flatnonzero(mask).astype(int).tolist())
            exp=RR35_EXPECTED[vid]
            for label,ticks,pips,purpose in COST_CASES:
                m=metrics(control_records,accepted,label)
                sha=accepted_ledger_hash(control_records,accepted,label)
                tgt=exp[label]
                ok=(qualified==exp["qualified"] and len(accepted)==exp["accepted"] and abs(m["total_r"]-tgt["total_r"])<1e-10 and abs(m["profit_factor"]-tgt["profit_factor"])<1e-10 and abs(m["max_drawdown_r"]-tgt["max_drawdown_r"])<1e-10 and sha==tgt["ledger_sha256"])
                rr35_rows.append({"variant_id":vid,"rr":REFERENCE_RR,"cost_label":label,"status":"PASS" if ok else "FAIL","qualified_raw_signals":qualified,"expected_qualified_raw_signals":exp["qualified"],"accepted_trades":len(accepted),"expected_accepted_trades":exp["accepted"],"total_r":m["total_r"],"expected_total_r":tgt["total_r"],"profit_factor":m["profit_factor"],"expected_profit_factor":tgt["profit_factor"],"max_drawdown_r":m["max_drawdown_r"],"expected_max_drawdown_r":tgt["max_drawdown_r"],"ledger_sha256":sha,"expected_ledger_sha256":tgt["ledger_sha256"]})
        write_csv(OUTPUTS["rr35_parity"],rr35_rows)
        if any(x["status"]!="PASS" for x in rr35_rows):
            raise RuntimeError("Frozen RR3.50 full-ledger parity FAILED; do not interpret RR sweep")

        plan=[]
        n=0
        for vid,spec in FROZEN_VARIANTS.items():
            for rr in RR_GRID:
                n+=1
                plan.append({"configuration_number":n,"config_id":rr_config_id(vid,rr),"variant_id":vid,"rr":rr,"description":spec["description"],"application":"FROZEN_ENTRY_AND_CONDITIONAL_RULES_RR_ONLY"})
        if n != EXPECTED_RR_CONFIGS:
            raise RuntimeError(f"RR plan mismatch: {n} vs {EXPECTED_RR_CONFIGS}")
        write_csv(OUTPUTS["rr_plan"],plan)

        set_status(state="rr_sweep", progress=22, message=f"Running {EXPECTED_RR_CONFIGS} frozen-variant RR configurations")
        summary_rows=[]; ledger_rows=[]; period_rows=[]; year_rows=[]; rolling_rows=[]; rolling_summary_rows=[]
        accepted_signals={}; censored_by_rr={}
        cache_records={REFERENCE_RR:control_records}
        for rr_i,rr in enumerate(RR_GRID,start=1):
            set_status(progress=22+int(60*rr_i/len(RR_GRID)),message=f"RR sweep {rr_i}/{len(RR_GRID)}: RR{rr:.2f}")
            if rr in cache_records:
                records=cache_records[rr]
                censored=control_censored
            else:
                records,censored=build_raw_outcomes_rr(features,vec_raw,daily_states,rr)
            censored_by_rr[rr]=censored
            if rr in (2.50,4.50):
                failures=execution_parity_sample_rr(features,h1,records,rr,sample_n=min(50,len(records)))
                if failures:
                    raise RuntimeError(f"RR{rr:.2f} scalar execution parity FAILED: {failures[:5]}")
            arr=all_record_arrays(records)
            for vid,spec in FROZEN_VARIANTS.items():
                mask=frozen_variant_mask(arr,vid)
                qualified=int(np.sum(mask))
                accepted=replay_positions(records,np.flatnonzero(mask).astype(int).tolist())
                cid=rr_config_id(vid,rr)
                accepted_signals[(vid,rr)]={signal_key(records[p]) for p in accepted}
                for label,ticks,pips,purpose in COST_CASES:
                    m=metrics(records,accepted,label)
                    summary_rows.append({"config_id":cid,"variant_id":vid,"rr":rr,"cost_label":label,"adverse_ticks":ticks,"adverse_pips":pips,"cost_purpose":purpose,"qualified_raw_signals":qualified,"accepted_trades":len(accepted),"raw_outcomes_censored_at_cutoff":censored,**m})
                    for seq,p in enumerate(accepted,start=1):
                        r=records[p]
                        ledger_rows.append({"config_id":cid,"variant_id":vid,"rr":rr,"cost_label":label,"sequence":seq,"signal_time":iso(r["signal_time"]),"exit_time":iso(r["exit_time"]),"signal_index":r["signal_index"],"exit_index":r["exit_index"],"reference_entry":r["reference_entry"],"historical_fill":r[f"fill__{label}"],"stop":r["stop"],"target":r["target"],"exit_reason":r["exit_reason"],"result_r":r[f"result_r__{label}"]})
                    p_rows,y_rows,r_rows,rs_rows=diagnostics_for_config(records,cid,accepted,label)
                    for x in p_rows: x.update({"variant_id":vid,"rr":rr})
                    for x in y_rows: x.update({"variant_id":vid,"rr":rr})
                    for x in r_rows: x.update({"variant_id":vid,"rr":rr})
                    for x in rs_rows: x.update({"variant_id":vid,"rr":rr})
                    period_rows.extend(p_rows); year_rows.extend(y_rows); rolling_rows.extend(r_rows); rolling_summary_rows.extend(rs_rows)

        write_csv(OUTPUTS["summary"],summary_rows)
        write_csv(OUTPUTS["ledgers"],ledger_rows)
        write_csv(OUTPUTS["periods"],period_rows)
        write_csv(OUTPUTS["years"],year_rows)
        write_csv(OUTPUTS["rolling"],rolling_rows)
        write_csv(OUTPUTS["rolling_summary"],rolling_summary_rows)

        control_map={(r["variant_id"],r["cost_label"]):r for r in summary_rows if abs(float(r["rr"])-REFERENCE_RR)<1e-12}
        comparison=[]; overlap=[]
        for r in summary_rows:
            c=control_map[(r["variant_id"],r["cost_label"])]
            comparison.append({"config_id":r["config_id"],"variant_id":r["variant_id"],"rr":r["rr"],"cost_label":r["cost_label"],"accepted_trades":r["accepted_trades"],"control_rr":REFERENCE_RR,"control_accepted_trades":c["accepted_trades"],"delta_trades":r["accepted_trades"]-c["accepted_trades"],"total_r":r["total_r"],"control_total_r":c["total_r"],"delta_total_r":r["total_r"]-c["total_r"],"expectancy_r":r["expectancy_r"],"control_expectancy_r":c["expectancy_r"],"delta_expectancy_r":r["expectancy_r"]-c["expectancy_r"],"profit_factor":r["profit_factor"],"control_profit_factor":c["profit_factor"],"delta_profit_factor":r["profit_factor"]-c["profit_factor"],"max_drawdown_r":r["max_drawdown_r"],"control_max_drawdown_r":c["max_drawdown_r"],"delta_max_drawdown_r":r["max_drawdown_r"]-c["max_drawdown_r"]})
        for vid in FROZEN_VARIANTS:
            base=accepted_signals[(vid,REFERENCE_RR)]
            for rr in RR_GRID:
                cur=accepted_signals[(vid,rr)]
                union=cur|base
                overlap.append({"variant_id":vid,"rr":rr,"accepted_signals":len(cur),"rr3p50_signals":len(base),"intersection":len(cur&base),"only_this_rr":len(cur-base),"only_rr3p50":len(base-cur),"jaccard_pct":100.0*len(cur&base)/len(union) if union else 100.0,"raw_outcomes_censored_at_cutoff":censored_by_rr[rr]})
        write_csv(OUTPUTS["comparison"],comparison)
        write_csv(OUTPUTS["overlap"],overlap)

        methodology=[
            {"topic":"purpose","value":"RR-last sweep after Pass 3 froze conditional entry rules. No entry-rule changes."},
            {"topic":"primary","value":json.dumps(FROZEN_VARIANTS["PRIMARY"],sort_keys=True)},
            {"topic":"quality","value":json.dumps(FROZEN_VARIANTS["QUALITY"],sort_keys=True)},
            {"topic":"rr_grid","value":json.dumps(list(RR_GRID))},
            {"topic":"parity","value":"Fails closed on exact frozen H1/D1/raw fingerprints, upstream CORE, and exact RR3.50 PRIMARY/QUALITY full accepted ledgers at 10T/20T/40T."},
            {"topic":"execution","value":"For every RR independently: reference entry=signal close; stop=signal high+10 ticks; target=reference entry - RR*reference risk; exits begin next H1 candle; short-side tie convention; p0 rebuilt independently; exact exit-candle re-entry eligible."},
            {"topic":"costs","value":"10T primary live-parity, 20T stressed selection, 40T extreme diagnostic. Assumed historical MID entry penalties, not measured historical executable spreads/slippage."},
            {"topic":"d1_freeze","value":f"D1 source clipped to exact Pass 3 last completed D1 open {iso(EXPECTED_D1_LAST_OPEN)} before hashing/replay."},
            {"topic":"right_censoring","value":"Raw exact signals whose RR-specific target/stop does not resolve before frozen cutoff are right-censored and reported per RR."},
            {"topic":"selection_rule","value":"Interpret the RR neighbourhood, costs, eras, calendar years, rolling 12/24/36m windows, drawdown and chronology changes. Do not select solely by maximum lifetime R."},
            {"topic":"not_tested","value":"No geometry changes, no conditional-filter changes, no timing search, no portfolio feedback and no live orders."},
            {"topic":"data_snooping","value":"All history has been repeatedly examined and is in-sample. This is historical robustness evidence, not untouched OOS validation."},
            {"topic":"next_gate","value":"Freeze at most one final variant/RR after reviewing the whole RR neighbourhood. Then independently reproduce its complete ledger before Portfolio 30->31 admission."},
        ]
        write_csv(OUTPUTS["methodology"],methodology)
        if OUTPUTS["errors"].exists(): OUTPUTS["errors"].unlink()
        pack_results()
        set_status(state="complete",progress=100,message="AUD/JPY H1 SHORT Pass 4 RR sweep complete; ZIP ready",parity_passed=True,h1_candles=len(h1),d1_candles=len(d1),raw_exact_signals=len(vec_raw),rr_configurations=EXPECTED_RR_CONFIGS,summary_rows=len(summary_rows),h1_source_sha256=h1_sha,raw_signal_sha256=vec_hash,results_zip=str(BUNDLE))
    except Exception as exc:
        tb=traceback.format_exc()
        write_csv(OUTPUTS["errors"],[{"error_type":type(exc).__name__,"message":str(exc),"traceback":tb}])
        try: pack_results()
        except Exception: pass
        set_status(state="failed",progress=100,message=f"{type(exc).__name__}: {exc}",parity_passed=False)

# ============================================================
# FLASK ROUTES
# ============================================================

def launch_once():
    global RESEARCH_STARTED
    with RESEARCH_LOCK:
        if RESEARCH_STARTED:
            return False
        RESEARCH_STARTED=True
        threading.Thread(target=run_research,daemon=True,name="audjpy-h1-short-pass4-rr-sweep").start()
        return True

@app.route("/")
def root():
    return jsonify({
        "service":"AUD/JPY H1 SHORT Pass 4 RR-last sweep",
        "pass_version":PASS_VERSION,
        "research_only":True,"orders_supported":False,"trading_enabled":False,
        "pair":PAIR,"timeframe":TIMEFRAME,"side":SIDE,"rr_grid":list(RR_GRID),
        "frozen_variants":FROZEN_VARIANTS,"rr_configurations":EXPECTED_RR_CONFIGS,
        "frozen_end_exclusive":iso(DATA_END),
        "cost_cases":[{"label":a,"ticks":b,"pips":c,"purpose":d} for a,b,c,d in COST_CASES],
        "routes":["/audjpy-h1-short-pass4/start","/audjpy-h1-short-pass4/status","/audjpy-h1-short-pass4/results"],
    })

@app.route("/audjpy-h1-short-pass4/start")
def start_route():
    return jsonify({"started_now":launch_once(),"state":STATUS["state"],"orders_supported":False})

@app.route("/audjpy-h1-short-pass4/status")
def status_route():
    with STATUS_LOCK:
        return jsonify(dict(STATUS))

@app.route("/audjpy-h1-short-pass4/results")
def results_route():
    if not BUNDLE.exists():
        return jsonify({"status":"not_ready","state":STATUS["state"],"message":STATUS["message"]}),404
    return send_file(str(BUNDLE.resolve()),as_attachment=True,download_name=BUNDLE.name)

if __name__ == "__main__":
    launch_once()
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")),debug=False)
