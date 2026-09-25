#!/usr/bin/env python3
"""
AUD/JPY H1 LONG — Pass 1B boundary clarification
=================================================

RESEARCH ONLY. READ ONLY. NEVER PLACES ORDERS OR MODIFIES THE LIVE EXECUTOR.

Purpose
-------
Pass 1 found two distinct exact-bullish-engulf regions worth clarifying before
any conditional-filter work:

1) BROAD / higher-frequency region around LB30, distance<=0.75 ATR,
   body>=0.60-0.80 ATR, range>=1.00 ATR.
2) TIGHT / high-range region around LB15, distance<=0.10-0.20 ATR,
   body around 1.00 ATR, range>=1.75 ATR.

Pass 1B DOES NOT search new mechanisms, optimise RR, add timing filters, or use
Portfolio 29 as a selection target. It widens only the boundaries justified by
Pass 1 and maps the local neighbourhoods. The frozen execution remains:

- OANDA midpoint completed H1 candles
- exact bullish body engulfing
- ATR14 Wilder/RMA, SMA seeded
- reference entry = signal close
- stop = signal low - 10 ticks
- RR3.50 fixed
- 10T/1 pip adverse fill = primary live-parity case
- 20T/2 pips = stressed selection case
- 40T/4 pips = extreme diagnostic only
- exits start next H1 candle
- p0; exact exit-candle signal remains eligible

The historical cutoff is intentionally frozen to the exact Pass 1 cutoff so
source and control parity can be checked before any widened-boundary result is
interpreted. All history remains exploratory/in-sample.

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
SIDE = "LONG"
REQUESTED_START = datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc)
DATA_END = datetime(2026, 9, 25, 19, 0, tzinfo=timezone.utc)  # exclusive
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

# Pass 1B boundary maps. These are deliberately limited to the two regions
# justified by Pass 1. No new signal family or context filter is introduced.
BROAD_LOOKBACKS = (15, 20, 25, 30, 40)
BROAD_DISTANCES = (0.50, 0.625, 0.75, 0.875, 1.00)
BROAD_BODY_ATR = (0.50, 0.60, 0.70, 0.80, 0.90)
BROAD_RANGE_ATR = (0.90, 1.00, 1.10, 1.25, 1.50, 1.75)

TIGHT_LOOKBACKS = (10, 12, 15, 20, 30)
TIGHT_DISTANCES = (0.05, 0.075, 0.10, 0.125, 0.15, 0.20, 0.25)
TIGHT_BODY_ATR = (0.60, 0.80, 1.00, 1.20)
TIGHT_RANGE_ATR = (1.50, 1.625, 1.75, 1.875, 2.00, 2.125, 2.25)

BROAD_GRID_ROWS = len(BROAD_LOOKBACKS) * len(BROAD_DISTANCES) * len(BROAD_BODY_ATR) * len(BROAD_RANGE_ATR)
TIGHT_GRID_ROWS = len(TIGHT_LOOKBACKS) * len(TIGHT_DISTANCES) * len(TIGHT_BODY_ATR) * len(TIGHT_RANGE_ATR)
EXPECTED_GRID_ROWS = BROAD_GRID_ROWS + TIGHT_GRID_ROWS
assert BROAD_GRID_ROWS == 750
assert TIGHT_GRID_ROWS == 980
assert EXPECTED_GRID_ROWS == 1730

# Used by the inherited feature builder so every Pass 1B lookback is prepared.
GRID_LOOKBACKS = tuple(sorted(set(BROAD_LOOKBACKS) | set(TIGHT_LOOKBACKS)))
GRID_DISTANCES = tuple(sorted(set(BROAD_DISTANCES) | set(TIGHT_DISTANCES)))
GRID_BODY_ATR = tuple(sorted(set(BROAD_BODY_ATR) | set(TIGHT_BODY_ATR)))
GRID_RANGE_ATR = tuple(sorted(set(BROAD_RANGE_ATR) | set(TIGHT_RANGE_ATR)))

# Single-factor levels. Each is applied to the unchanged raw exact-engulf stream.
BR_LEVELS = (1.00, 1.10, 1.20, 1.30, 1.40, 1.50, 1.75, 2.00)
BODY_ATR_LEVELS = (0.40, 0.50, 0.60, 0.75, 0.90, 1.00, 1.25, 1.50, 1.75)
RANGE_ATR_LEVELS = (0.80, 1.00, 1.20, 1.40, 1.60, 1.80, 2.00, 2.25)
CLOSE_LOCATION_LEVELS = (0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90)
LOWER_WICK_BODY_LEVELS = (0.10, 0.20, 0.30, 0.40, 0.50, 0.75, 1.00)
STRUCTURE_LOOKBACKS = (10, 12, 15, 20, 25, 30, 40, 60, 80, 100, 150)
STRUCTURE_DISTANCES = (0.05, 0.10, 0.15, 0.20, 0.30, 0.50, 0.75, 1.00)
H1_ATR_RATIO_MIN_LEVELS = (0.70, 0.80, 0.90, 1.00, 1.10, 1.20, 1.30)
MOMENTUM_LOOKBACKS = (4, 8, 12, 24, 48)
MOMENTUM_THRESHOLDS = (0.50, 1.00, 1.50, 2.00)
DAILY_ATR_RATIO_MIN_LEVELS = (0.80, 0.90, 1.00, 1.10, 1.20)

# Frozen Pass 1 source/signal fingerprints and control rows. Pass 1B fails
# closed if these no longer reproduce at the identical historical cutoff.
EXPECTED_H1_ROWS = 137969
EXPECTED_D1_ROWS = 6473
EXPECTED_H1_SHA256 = "2ebb773ab6d8bd53b3725265e16b879952436f0e198682142e490ff0e0f5957a"
EXPECTED_D1_SHA256 = "611c4edf4801888e0be378f36368ce2232fecd86f6c758f14d17fbaa0759688b"
EXPECTED_RAW_SIGNAL_COUNT = 10624
EXPECTED_RAW_SIGNAL_SHA256 = "47dd2498e9ad03ac69c3f8f8d758b878808d08bb5c72fecad8b87542d65a329f"

PASS1_CONTROL_EXPECTED = {
    ("BROAD", 30, 0.75, 0.60, 1.00): {
        "LIVE_LIMIT_10T": dict(qualified_raw_signals=1102, accepted_trades=980, total_r=124.30894100208393, profit_factor=1.1716974323233202, max_drawdown_r=-33.98286139312198),
        "STRESS_20T": dict(qualified_raw_signals=1102, accepted_trades=980, total_r=81.04814567773595, profit_factor=1.111944952593558, max_drawdown_r=-38.53919641239111),
        "EXTREME_40T": dict(qualified_raw_signals=1102, accepted_trades=980, total_r=5.317062087210012, profit_factor=1.0073440084077487, max_drawdown_r=-54.02887382803912),
    },
    ("BROAD", 30, 0.75, 0.80, 1.00): {
        "LIVE_LIMIT_10T": dict(qualified_raw_signals=832, accepted_trades=759, total_r=106.02450750543557, profit_factor=1.1896681708505108, max_drawdown_r=-35.18568598052075),
        "STRESS_20T": dict(qualified_raw_signals=832, accepted_trades=759, total_r=74.14845801149505, profit_factor=1.1326448264964133, max_drawdown_r=-36.265503990951714),
        "EXTREME_40T": dict(qualified_raw_signals=832, accepted_trades=759, total_r=17.99812257777572, profit_factor=1.0321969992446791, max_drawdown_r=-44.77699627950781),
    },
    ("TIGHT", 15, 0.10, 1.00, 1.75): {
        "LIVE_LIMIT_10T": dict(qualified_raw_signals=57, accepted_trades=55, total_r=28.09651134876348, profit_factor=1.7804586485767635, max_drawdown_r=-7.613065326633221),
        "STRESS_20T": dict(qualified_raw_signals=57, accepted_trades=55, total_r=25.84338254085452, profit_factor=1.7178717372459589, max_drawdown_r=-7.720588235294073),
        "EXTREME_40T": dict(qualified_raw_signals=57, accepted_trades=55, total_r=21.731221667678422, profit_factor=1.6036450463244007, max_drawdown_r=-7.920560747663606),
    },
    ("TIGHT", 15, 0.20, 1.00, 1.75): {
        "LIVE_LIMIT_10T": dict(qualified_raw_signals=107, accepted_trades=103, total_r=28.22369662522981, profit_factor=1.3866259811675317, max_drawdown_r=-13.52496906270298),
        "STRESS_20T": dict(qualified_raw_signals=107, accepted_trades=103, total_r=24.682259040401604, profit_factor=1.338113137539748, max_drawdown_r=-14.026499897351837),
        "EXTREME_40T": dict(qualified_raw_signals=107, accepted_trades=103, total_r=18.21527469426499, profit_factor=1.2495243108803424, max_drawdown_r=-14.965758962294316),
    },
}
EXPECTED_BROAD_LEDGER_SHA20 = {
    (30, 0.75, 0.60, 1.00): "ac299b37bdc0af1c42e6b576fff6e6e7ba45f2fbe33fe3272e0b4ca6067849b4",
    (30, 0.75, 0.80, 1.00): "5dedb34c63cf36dda292b4ab7e56b972c7fe1c150b4e9ffa10fa404a94dfa1ae",
}

# HASHFIX V2: these expected SHA256 values were re-derived directly from
# Pass 1 diagnostic_screen_accepted_ledgers.csv using Python stdlib csv parsing
# and the exact accepted_ledger_hash serialization below. No research geometry,
# costs, replay rules, data cutoff, or selection logic changed.
PATCH_VERSION = "PASS1B_HASH_REFERENCE_FIX_V2_2026-09-25"

API = os.getenv("OANDA_API_URL", "https://api-fxtrade.oanda.com").rstrip("/")
TOKEN = os.getenv("OANDA_TOKEN", "")

OUT_DIR = Path(os.getenv("AUDJPY_H1_LONG_PASS1B_OUTPUT_DIR", "/tmp/audjpy_h1_long_pass1b"))
OUT_DIR.mkdir(parents=True, exist_ok=True)
BUNDLE = OUT_DIR / "AUDJPY_H1_LONG_PASS1B_BOUNDARY_CLARIFICATION_RESULTS.zip"

OUTPUTS = {
    "coverage": OUT_DIR / "coverage.csv",
    "source_fingerprint": OUT_DIR / "source_fingerprint.csv",
    "parity": OUT_DIR / "parity.csv",
    "pass1_control_parity": OUT_DIR / "pass1_control_parity.csv",
    "raw_baseline": OUT_DIR / "raw_baseline_summary.csv",
    "raw_signals": OUT_DIR / "raw_signal_outcomes.csv",
    "grid": OUT_DIR / "boundary_grid_summary.csv",
    "levels": OUT_DIR / "levels_and_boundaries.csv",
    "branch_summary": OUT_DIR / "branch_summary.csv",
    "axis_summary": OUT_DIR / "branch_axis_summary.csv",
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
    "message": "AUD/JPY H1 LONG Pass 1B waiting",
    "orders_supported": False,
    "trading_enabled": False,
    "pair": PAIR,
    "timeframe": TIMEFRAME,
    "side": SIDE,
    "rr": REFERENCE_RR,
    "cost_cases": [x[0] for x in COST_CASES],
    "broad_grid_rows": BROAD_GRID_ROWS,
    "tight_grid_rows": TIGHT_GRID_ROWS,
    "grid_rows": EXPECTED_GRID_ROWS,
    "frozen_pass1_cutoff": DATA_END.isoformat().replace("+00:00", "Z"),
    "patch_version": PATCH_VERSION,
    "expected_broad_ledger_sha20": {
        "LB30_D075_B060_R100": EXPECTED_BROAD_LEDGER_SHA20[(30, 0.75, 0.60, 1.00)],
        "LB30_D075_B080_R100": EXPECTED_BROAD_LEDGER_SHA20[(30, 0.75, 0.80, 1.00)],
    },
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
    bullish_body = closes - opens
    candle_range = highs - lows
    lower_wick = np.minimum(opens, closes) - lows

    body_atr = np.divide(
        bullish_body, atr,
        out=np.zeros_like(closes),
        where=(bullish_body > 0) & np.isfinite(atr) & (atr > 0),
    )
    range_atr = np.divide(
        candle_range, atr,
        out=np.zeros_like(closes),
        where=np.isfinite(atr) & (atr > 0),
    )
    bull_ratio = np.divide(
        bullish_body, previous_body,
        out=np.zeros_like(closes),
        where=(bullish_body > 0) & (previous_body > 0),
    )
    close_location = np.divide(
        closes - lows, candle_range,
        out=np.zeros_like(closes),
        where=candle_range > 0,
    )
    lower_wick_body = np.divide(
        lower_wick, bullish_body,
        out=np.zeros_like(closes),
        where=bullish_body > 0,
    )
    exact_bull = (
        (pc < po)
        & (closes > opens)
        & (opens <= pc)
        & (closes >= po)
    )

    needed_lbs = sorted(set(STRUCTURE_LOOKBACKS) | set(GRID_LOOKBACKS))
    prev_lows = {lb: rolling_previous_extreme(lows, lb, want_max=False) for lb in needed_lbs}
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
        "exact_bull": exact_bull,
        "bull_ratio": bull_ratio,
        "body_atr": body_atr,
        "range_atr": range_atr,
        "close_location": close_location,
        "lower_wick_body": lower_wick_body,
        "prev_lows": prev_lows,
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
        "close_gt_ema50": np.zeros(len(h1_times), dtype=bool),
        "close_gt_ema100": np.zeros(len(h1_times), dtype=bool),
        "close_gt_ema200": np.zeros(len(h1_times), dtype=bool),
        "ema50_gt_ema200": np.zeros(len(h1_times), dtype=bool),
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
            mapped["close_gt_ema50"][i] = d_close[j] > d_ema50[j]
        if math.isfinite(d_ema100[j]):
            mapped["close_gt_ema100"][i] = d_close[j] > d_ema100[j]
        if math.isfinite(d_ema200[j]):
            mapped["close_gt_ema200"][i] = d_close[j] > d_ema200[j]
        if math.isfinite(d_ema50[j]) and math.isfinite(d_ema200[j]):
            mapped["ema50_gt_ema200"][i] = d_ema50[j] > d_ema200[j]
        mapped["atr_ratio"][i] = d_atr_ratio[j]
    return mapped

# ============================================================
# RAW SIGNAL / EXECUTION PARITY
# ============================================================

def raw_exact_vector_indices(features):
    mask = features["exact_bull"] & np.isfinite(features["atr"]) & (features["atr"] > 0)
    return np.flatnonzero(mask).astype(int)


def raw_exact_scalar_indices(candles, atr_values):
    result = []
    for i in range(1, len(candles)):
        p = candles[i - 1]
        c = candles[i]
        exact = (
            p["close"] < p["open"]
            and c["close"] > c["open"]
            and c["open"] <= p["close"]
            and c["close"] >= p["open"]
        )
        if exact and math.isfinite(float(atr_values[i])) and atr_values[i] > 0:
            result.append(i)
    return np.asarray(result, dtype=int)


def find_exit(features, signal_index, stop, target):
    high = features["high"]
    low = features["low"]
    opn = features["open"]
    for i in range(signal_index + 1, len(high)):
        s = low[i] <= stop
        t = high[i] >= target
        if not s and not t:
            continue
        if s and t:
            # LONG tie convention used in the prior research engine.
            reason = "TARGET" if abs(high[i] - opn[i]) < abs(opn[i] - low[i]) else "STOP"
        else:
            reason = "STOP" if s else "TARGET"
        return i, reason
    return None, None


def result_r_for_cost(reference_entry, stop, target, reason, cost_pips):
    fill = reference_entry + cost_pips * PIP
    actual_risk = fill - stop
    if actual_risk <= 0:
        return None, fill
    exit_price = target if reason == "TARGET" else stop
    return (exit_price - fill) / actual_risk, fill


def build_raw_outcomes(features, raw_indices, daily_states):
    records = []
    censored = 0
    for k, i in enumerate(raw_indices):
        if k % 1000 == 0:
            set_status(message=f"Building raw exact-engulf outcomes {k}/{len(raw_indices)}")
        entry = float(features["close"][i])
        stop = float(features["low"][i]) - STOP_BUFFER_TICKS * TICK
        ref_risk = entry - stop
        if ref_risk <= 0:
            continue
        target = entry + REFERENCE_RR * ref_risk
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
            "bull_ratio": float(features["bull_ratio"][i]),
            "body_atr": float(features["body_atr"][i]),
            "range_atr": float(features["range_atr"][i]),
            "close_location": float(features["close_location"][i]),
            "lower_wick_body": float(features["lower_wick_body"][i]),
            "h1_atr_ratio": float(features["h1_atr_ratio"][i]) if math.isfinite(features["h1_atr_ratio"][i]) else math.nan,
            "daily_close_gt_ema50": bool(daily_states["close_gt_ema50"][i]),
            "daily_close_gt_ema100": bool(daily_states["close_gt_ema100"][i]),
            "daily_close_gt_ema200": bool(daily_states["close_gt_ema200"][i]),
            "daily_ema50_gt_ema200": bool(daily_states["ema50_gt_ema200"][i]),
            "daily_atr_ratio": float(daily_states["atr_ratio"][i]) if math.isfinite(daily_states["atr_ratio"][i]) else math.nan,
            "hour_utc": features["times"][i].hour,
            "weekday_utc": features["times"][i].weekday(),
        }
        for lb in STRUCTURE_LOOKBACKS:
            structure = features["prev_lows"][lb][i]
            row[f"structure_dist_{lb}"] = (
                abs(float(features["low"][i]) - float(structure)) / float(features["atr"][i])
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
        stop_touched = candle["low"] <= stop
        target_touched = candle["high"] >= target
        if not stop_touched and not target_touched:
            continue
        if stop_touched and target_touched:
            reason = "TARGET" if abs(candle["high"] - candle["open"]) < abs(candle["open"] - candle["low"]) else "STOP"
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
        stop = float(candles[i]["low"]) - 10 * 0.001
        target = entry + 3.5 * (entry - stop)
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
        "bull_ratio", "body_atr", "range_atr", "close_location",
        "lower_wick_body", "h1_atr_ratio", "daily_atr_ratio",
    ]
    out = {k: np.array([r[k] for r in records], dtype=float) for k in keys}
    out["daily_close_gt_ema50"] = np.array([r["daily_close_gt_ema50"] for r in records], dtype=bool)
    out["daily_close_gt_ema100"] = np.array([r["daily_close_gt_ema100"] for r in records], dtype=bool)
    out["daily_close_gt_ema200"] = np.array([r["daily_close_gt_ema200"] for r in records], dtype=bool)
    out["daily_ema50_gt_ema200"] = np.array([r["daily_ema50_gt_ema200"] for r in records], dtype=bool)
    for lb in STRUCTURE_LOOKBACKS:
        out[f"structure_dist_{lb}"] = np.array([r[f"structure_dist_{lb}"] for r in records], dtype=float)
    for lb in MOMENTUM_LOOKBACKS:
        out[f"momentum_{lb}"] = np.array([r[f"momentum_{lb}"] for r in records], dtype=float)
    return out

# ============================================================
# EXPERIMENT PLAN
# ============================================================

def single_factor_plan(arr):
    n = len(arr["bull_ratio"])
    base = np.ones(n, dtype=bool)

    for v in BR_LEVELS:
        yield f"BR_MIN_{v:.2f}", "body_ratio", ">=", v, base & (arr["bull_ratio"] >= v)
    for v in BODY_ATR_LEVELS:
        yield f"BODY_ATR_MIN_{v:.2f}", "body_atr", ">=", v, base & (arr["body_atr"] >= v)
    for v in RANGE_ATR_LEVELS:
        yield f"RANGE_ATR_MIN_{v:.2f}", "range_atr", ">=", v, base & (arr["range_atr"] >= v)
    for v in CLOSE_LOCATION_LEVELS:
        yield f"CLOSE_LOC_MIN_{v:.2f}", "close_location", ">=", v, base & (arr["close_location"] >= v)
    for v in LOWER_WICK_BODY_LEVELS:
        yield f"LOWER_WICK_BODY_MIN_{v:.2f}", "lower_wick_body", ">=", v, base & (arr["lower_wick_body"] >= v)

    for lb in STRUCTURE_LOOKBACKS:
        d = arr[f"structure_dist_{lb}"]
        for v in STRUCTURE_DISTANCES:
            yield (
                f"STRUCT_LB{lb}_DIST_MAX_{v:.2f}",
                "structure_distance",
                "<=",
                f"lb={lb};dist={v}",
                base & np.isfinite(d) & (d <= v),
            )

    for v in H1_ATR_RATIO_MIN_LEVELS:
        x = arr["h1_atr_ratio"]
        yield f"H1_ATR_RATIO_MIN_{v:.2f}", "h1_atr_ratio", ">=", v, base & np.isfinite(x) & (x >= v)

    for lb in MOMENTUM_LOOKBACKS:
        x = arr[f"momentum_{lb}"]
        for v in MOMENTUM_THRESHOLDS:
            yield f"PRIOR_RISE_{lb}_MIN_{v:.2f}", "prior_momentum", ">=", f"lb={lb};rise={v}", base & np.isfinite(x) & (x >= v)
            yield f"PRIOR_FALL_{lb}_MAX_NEG{v:.2f}", "prior_momentum", "<=", f"lb={lb};fall=-{v}", base & np.isfinite(x) & (x <= -v)

    yield "D1_CLOSE_GT_EMA50", "daily_regime", "bool", "close>ema50", base & arr["daily_close_gt_ema50"]
    yield "D1_CLOSE_GT_EMA100", "daily_regime", "bool", "close>ema100", base & arr["daily_close_gt_ema100"]
    yield "D1_CLOSE_GT_EMA200", "daily_regime", "bool", "close>ema200", base & arr["daily_close_gt_ema200"]
    yield "D1_EMA50_GT_EMA200", "daily_regime", "bool", "ema50>ema200", base & arr["daily_ema50_gt_ema200"]
    for v in DAILY_ATR_RATIO_MIN_LEVELS:
        x = arr["daily_atr_ratio"]
        yield f"D1_ATR_RATIO_MIN_{v:.2f}", "daily_atr_ratio", ">=", v, base & np.isfinite(x) & (x >= v)


def fmt_level(value):
    if isinstance(value, int):
        return str(value)
    return f"{float(value):.3f}".rstrip("0").rstrip(".")


def boundary_config_id(branch, lb, distance, body, rng):
    return (
        f"P1B_{branch}_LB{lb}_D{fmt_level(distance)}_"
        f"B{fmt_level(body)}_R{fmt_level(rng)}"
    )


def branch_grid_plan(arr):
    plans = (
        ("BROAD", BROAD_LOOKBACKS, BROAD_DISTANCES, BROAD_BODY_ATR, BROAD_RANGE_ATR),
        ("TIGHT", TIGHT_LOOKBACKS, TIGHT_DISTANCES, TIGHT_BODY_ATR, TIGHT_RANGE_ATR),
    )
    for branch, lbs, distances, bodies, ranges in plans:
        for lb in lbs:
            d = arr[f"structure_dist_{lb}"]
            for distance in distances:
                structure_mask = np.isfinite(d) & (d <= distance)
                for body in bodies:
                    body_mask = arr["body_atr"] >= body
                    for rng in ranges:
                        config_id = boundary_config_id(branch, lb, distance, body, rng)
                        mask = structure_mask & body_mask & (arr["range_atr"] >= rng)
                        yield branch, config_id, lb, distance, body, rng, mask


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


def branch_axis_rows(grid_rows):
    # Descriptive plateau aid only. This never selects a strategy.
    stress = [r for r in grid_rows if r["cost_label"] == STRESS_COST_LABEL]
    out = []
    for branch in ("BROAD", "TIGHT"):
        subset = [r for r in stress if r["branch"] == branch]
        for field in ("lookback", "distance_atr_max", "body_atr_min", "range_atr_min"):
            values = sorted({r[field] for r in subset})
            for value in values:
                rows = [r for r in subset if r[field] == value]
                trs = [float(r["total_r"]) for r in rows]
                pfs = [float(r["profit_factor"]) for r in rows]
                counts = [int(r["accepted_trades"]) for r in rows]
                out.append({
                    "branch": branch,
                    "cost_label": STRESS_COST_LABEL,
                    "axis": field,
                    "axis_value": value,
                    "configurations": len(rows),
                    "positive_total_r_configs": sum(x > 0 for x in trs),
                    "positive_rate_pct": 100.0 * sum(x > 0 for x in trs) / len(rows),
                    "median_total_r": float(median(trs)),
                    "best_total_r": max(trs),
                    "worst_total_r": min(trs),
                    "median_profit_factor": float(median(pfs)),
                    "median_accepted_trades": float(median(counts)),
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
        set_status(state="fetching", progress=2, message="Fetching frozen Pass 1 H1/D1 OANDA midpoint history")
        h1 = fetch_history("H1", REQUESTED_START, DATA_END, 180)
        d1 = fetch_history("D", D1_WARMUP_START, DATA_END, 1200)

        h1_times = [x["time"] for x in h1]
        d1_times = [x["time"] for x in d1]
        if h1_times != sorted(set(h1_times)):
            raise RuntimeError("H1 timestamps are duplicated or non-monotonic")
        if d1_times != sorted(set(d1_times)):
            raise RuntimeError("D1 timestamps are duplicated or non-monotonic")
        if h1[-1]["time"] >= DATA_END:
            raise RuntimeError("H1 source contains candle at/after frozen end")

        h1_sha = sha_rows(
            f"{iso(x['time'])}|{x['open']:.6f}|{x['high']:.6f}|{x['low']:.6f}|{x['close']:.6f}"
            for x in h1
        )
        d1_sha = sha_rows(
            f"{iso(x['time'])}|{x['open']:.6f}|{x['high']:.6f}|{x['low']:.6f}|{x['close']:.6f}"
            for x in d1
        )
        write_csv(OUTPUTS["coverage"], [
            {"pair":PAIR,"timeframe":"H1","requested_start":iso(REQUESTED_START),"first_completed_candle":iso(h1[0]["time"]),"last_completed_candle_open":iso(h1[-1]["time"]),"frozen_end_exclusive":iso(DATA_END),"completed_candles":len(h1)},
            {"pair":PAIR,"timeframe":"D","requested_start":iso(D1_WARMUP_START),"first_completed_candle":iso(d1[0]["time"]),"last_completed_candle_open":iso(d1[-1]["time"]),"frozen_end_exclusive":iso(DATA_END),"completed_candles":len(d1),"daily_alignment":"17:00 America/New_York"},
        ])
        write_csv(OUTPUTS["source_fingerprint"], [
            {"series":"AUD_JPY_H1_MID_OHLC","sha256":h1_sha,"rows":len(h1)},
            {"series":"AUD_JPY_D1_MID_OHLC","sha256":d1_sha,"rows":len(d1)},
        ])

        source_checks = [
            {"check":"H1_ROW_COUNT","status":"PASS" if len(h1)==EXPECTED_H1_ROWS else "FAIL","actual":len(h1),"expected":EXPECTED_H1_ROWS},
            {"check":"D1_ROW_COUNT","status":"PASS" if len(d1)==EXPECTED_D1_ROWS else "FAIL","actual":len(d1),"expected":EXPECTED_D1_ROWS},
            {"check":"H1_SOURCE_SHA256","status":"PASS" if h1_sha==EXPECTED_H1_SHA256 else "FAIL","actual":h1_sha,"expected":EXPECTED_H1_SHA256},
            {"check":"D1_SOURCE_SHA256","status":"PASS" if d1_sha==EXPECTED_D1_SHA256 else "FAIL","actual":d1_sha,"expected":EXPECTED_D1_SHA256},
        ]
        if any(x["status"] != "PASS" for x in source_checks):
            write_csv(OUTPUTS["parity"], source_checks)
            raise RuntimeError("Frozen Pass 1 source parity FAILED; do not interpret Pass 1B")

        set_status(state="features", progress=10, message="Building H1 features and raw exact-engulf parity")
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
            and vec_hash == scalar_hash == EXPECTED_RAW_SIGNAL_SHA256
        )
        source_checks.append({
            "check":"RAW_SIGNAL_VECTOR_SCALAR_AND_PASS1_FINGERPRINT",
            "status":"PASS" if raw_ok else "FAIL",
            "vector_count":len(vec_raw),"scalar_count":len(scalar_raw),
            "vector_sha256":vec_hash,"scalar_sha256":scalar_hash,
            "expected_count":EXPECTED_RAW_SIGNAL_COUNT,"expected_sha256":EXPECTED_RAW_SIGNAL_SHA256,
        })
        if not raw_ok:
            write_csv(OUTPUTS["parity"], source_checks)
            raise RuntimeError("Raw signal parity/fingerprint FAILED")

        set_status(state="raw_replay", progress=16, message="Replaying frozen raw exact-engulf outcomes")
        records, censored = build_raw_outcomes(features, vec_raw, daily_states)
        exec_failures = execution_parity_sample(features, h1, records, sample_n=min(100, len(records)))
        source_checks.append({
            "check":"EXECUTION_SCALAR_RECOMPUTE_FIRST_100",
            "status":"PASS" if not exec_failures else "FAIL",
            "sample":min(100,len(records)),
            "failures":"; ".join(exec_failures[:10]),
        })
        write_csv(OUTPUTS["parity"], source_checks)
        if exec_failures:
            raise RuntimeError("Execution parity FAILED")

        raw_export=[]
        for r in records:
            row=dict(r); row["signal_time"]=iso(row["signal_time"]); row["exit_time"]=iso(row["exit_time"]); raw_export.append(row)
        write_csv(OUTPUTS["raw_signals"], raw_export)
        arr=all_record_arrays(records)
        raw_summary, raw_accepted = summarize_config(records, np.ones(len(records),dtype=bool), {
            "config_id":"RAW_EXACT_BULLISH_ENGULF","stage":"RAW_BASELINE","rr":REFERENCE_RR,"rule":"exact bullish engulf only",
        })
        for row in raw_summary:
            row["raw_exact_total_signals"]=len(vec_raw); row["right_censored_raw_signals"]=censored; row["raw_signal_sha256"]=vec_hash
        write_csv(OUTPUTS["raw_baseline"], raw_summary)

        set_status(state="boundary_grid", progress=25, message=f"Running {EXPECTED_GRID_ROWS} predeclared Pass 1B boundary configurations")
        grid_rows=[]
        accepted_by_id={}
        config_meta={}
        count=0
        for branch, config_id, lb, distance, body, rng, mask in branch_grid_plan(arr):
            count += 1
            if count % 100 == 0:
                set_status(message=f"Boundary grid {count}/{EXPECTED_GRID_ROWS}")
            rows, accepted = summarize_config(records, mask, {
                "config_id":config_id,"stage":"PASS1B_BOUNDARY","branch":branch,
                "lookback":lb,"distance_atr_max":distance,"body_atr_min":body,"range_atr_min":rng,"rr":REFERENCE_RR,
            })
            grid_rows.extend(rows)
            accepted_by_id[config_id]=accepted
            config_meta[config_id]=(branch,lb,distance,body,rng)
        if count != EXPECTED_GRID_ROWS:
            raise RuntimeError(f"Boundary grid enumeration mismatch: expected {EXPECTED_GRID_ROWS}, got {count}")
        write_csv(OUTPUTS["grid"], grid_rows)

        # Frozen Pass 1 aggregate controls plus two full accepted-ledger fingerprints.
        set_status(state="control_parity", progress=68, message="Checking exact Pass 1 control parity")
        rows_by_key={}
        for row in grid_rows:
            key=(row["branch"],int(row["lookback"]),float(row["distance_atr_max"]),float(row["body_atr_min"]),float(row["range_atr_min"]))
            rows_by_key.setdefault(key,{})[row["cost_label"]]=row
        control_rows=[]
        failures=[]
        tol=1e-9
        for key, expected_by_cost in PASS1_CONTROL_EXPECTED.items():
            actual_by_cost=rows_by_key.get(key,{})
            for cost_label, expected in expected_by_cost.items():
                actual=actual_by_cost.get(cost_label)
                ok=actual is not None
                diffs={}
                if ok:
                    for field in ("qualified_raw_signals","accepted_trades"):
                        diffs[field]=int(actual[field])-int(expected[field])
                        ok &= diffs[field] == 0
                    for field in ("total_r","profit_factor","max_drawdown_r"):
                        diffs[field]=float(actual[field])-float(expected[field])
                        ok &= abs(diffs[field]) <= tol
                control_rows.append({
                    "branch":key[0],"lookback":key[1],"distance_atr_max":key[2],"body_atr_min":key[3],"range_atr_min":key[4],
                    "cost_label":cost_label,"status":"PASS" if ok else "FAIL",**{f"diff_{k}":v for k,v in diffs.items()},
                })
                if not ok: failures.append(f"aggregate {key} {cost_label}")

        for params, expected_hash in EXPECTED_BROAD_LEDGER_SHA20.items():
            cid=boundary_config_id("BROAD",*params)
            actual_hash=accepted_ledger_hash(records, accepted_by_id[cid], STRESS_COST_LABEL)
            ok=actual_hash == expected_hash
            control_rows.append({
                "branch":"BROAD","lookback":params[0],"distance_atr_max":params[1],"body_atr_min":params[2],"range_atr_min":params[3],
                "cost_label":STRESS_COST_LABEL,"check":"FULL_ACCEPTED_LEDGER_SHA256","status":"PASS" if ok else "FAIL",
                "actual_sha256":actual_hash,"expected_sha256":expected_hash,
            })
            if not ok: failures.append(f"ledger {params}")
        write_csv(OUTPUTS["pass1_control_parity"], control_rows)
        if failures:
            raise RuntimeError("Frozen Pass 1 control parity FAILED: " + "; ".join(failures[:8]))

        level_rows=[
            {"branch":"BROAD","parameter":"lookback","levels":json.dumps(BROAD_LOOKBACKS),"lower_boundary":min(BROAD_LOOKBACKS),"upper_boundary":max(BROAD_LOOKBACKS)},
            {"branch":"BROAD","parameter":"distance_atr_max","levels":json.dumps(BROAD_DISTANCES),"lower_boundary":min(BROAD_DISTANCES),"upper_boundary":max(BROAD_DISTANCES)},
            {"branch":"BROAD","parameter":"body_atr_min","levels":json.dumps(BROAD_BODY_ATR),"lower_boundary":min(BROAD_BODY_ATR),"upper_boundary":max(BROAD_BODY_ATR)},
            {"branch":"BROAD","parameter":"range_atr_min","levels":json.dumps(BROAD_RANGE_ATR),"lower_boundary":min(BROAD_RANGE_ATR),"upper_boundary":max(BROAD_RANGE_ATR)},
            {"branch":"TIGHT","parameter":"lookback","levels":json.dumps(TIGHT_LOOKBACKS),"lower_boundary":min(TIGHT_LOOKBACKS),"upper_boundary":max(TIGHT_LOOKBACKS)},
            {"branch":"TIGHT","parameter":"distance_atr_max","levels":json.dumps(TIGHT_DISTANCES),"lower_boundary":min(TIGHT_DISTANCES),"upper_boundary":max(TIGHT_DISTANCES)},
            {"branch":"TIGHT","parameter":"body_atr_min","levels":json.dumps(TIGHT_BODY_ATR),"lower_boundary":min(TIGHT_BODY_ATR),"upper_boundary":max(TIGHT_BODY_ATR)},
            {"branch":"TIGHT","parameter":"range_atr_min","levels":json.dumps(TIGHT_RANGE_ATR),"lower_boundary":min(TIGHT_RANGE_ATR),"upper_boundary":max(TIGHT_RANGE_ATR)},
        ]
        write_csv(OUTPUTS["levels"], level_rows)

        # Branch-level breadth counts at each cost.
        branch_rows=[]
        for branch in ("BROAD","TIGHT"):
            for cost_label, *_ in COST_CASES:
                rows=[r for r in grid_rows if r["branch"]==branch and r["cost_label"]==cost_label]
                branch_rows.append({
                    "branch":branch,"cost_label":cost_label,"configurations":len(rows),
                    "positive_total_r":sum(float(r["total_r"])>0 for r in rows),
                    "pf_gt_1":sum(float(r["profit_factor"])>1 for r in rows),
                    "trades_ge_40":sum(int(r["accepted_trades"])>=40 for r in rows),
                    "positive_and_trades_ge_40":sum(float(r["total_r"])>0 and int(r["accepted_trades"])>=40 for r in rows),
                    "median_total_r":float(median([float(r["total_r"]) for r in rows])),
                    "median_expectancy_r":float(median([float(r["expectancy_r"]) for r in rows])),
                    "median_trades":float(median([int(r["accepted_trades"]) for r in rows])),
                    "best_total_r":max(float(r["total_r"]) for r in rows),
                    "worst_total_r":min(float(r["total_r"]) for r in rows),
                })
        write_csv(OUTPUTS["branch_summary"], branch_rows)
        write_csv(OUTPUTS["axis_summary"], branch_axis_rows(grid_rows))

        # Diagnostic screen: top 10 stressed rows per branch, reported only.
        by_id=defaultdict(dict)
        for row in grid_rows: by_id[row["config_id"]][row["cost_label"]]=row
        screen_rows=[]; top_ids=[]
        for branch in ("BROAD","TIGHT"):
            eligible=[]
            for cid,costs in by_id.items():
                if config_meta[cid][0] != branch: continue
                live=costs.get(PRIMARY_COST_LABEL); stress=costs.get(STRESS_COST_LABEL); extreme=costs.get(EXTREME_COST_LABEL)
                if live and stress and extreme and int(stress["accepted_trades"])>=40 and float(live["total_r"])>0 and float(stress["total_r"])>0:
                    eligible.append((float(stress["total_r"]),cid,live,stress,extreme))
            eligible.sort(reverse=True,key=lambda x:x[0])
            for rank,(_,cid,live,stress,extreme) in enumerate(eligible[:10],start=1):
                top_ids.append(cid)
                meta=config_meta[cid]
                screen_rows.append({
                    "branch":branch,"diagnostic_rank_within_branch":rank,"config_id":cid,
                    "selection_status":"DIAGNOSTIC_ONLY_NOT_FROZEN",
                    "screen_rule":"stress20T trades>=40; live10T and stress20T totalR>0; ranked by stress20T totalR",
                    "live10T_trades":live["accepted_trades"],"live10T_total_r":live["total_r"],"live10T_pf":live["profit_factor"],
                    "stress20T_total_r":stress["total_r"],"stress20T_pf":stress["profit_factor"],
                    "extreme40T_total_r":extreme["total_r"],"extreme40T_pf":extreme["profit_factor"],
                    "lookback":meta[1],"distance_atr_max":meta[2],"body_atr_min":meta[3],"range_atr_min":meta[4],
                })
        write_csv(OUTPUTS["screen"], screen_rows)

        # Predeclared line slices around the two Pass 1 centres, plus the top 5/branch.
        diag_ids=set()
        def add_if(branch,lb,d,b,r):
            cid=boundary_config_id(branch,lb,d,b,r)
            if cid in accepted_by_id: diag_ids.add(cid)
        # Broad centre and one-axis slices.
        for b in (0.60,0.80):
            for r in BROAD_RANGE_ATR: add_if("BROAD",30,0.75,b,r)
        for d in BROAD_DISTANCES: add_if("BROAD",30,d,0.60,1.00)
        for lb in BROAD_LOOKBACKS: add_if("BROAD",lb,0.75,0.60,1.00)
        # Tight centre and one-axis slices.
        for r in TIGHT_RANGE_ATR: add_if("TIGHT",15,0.10,1.00,r)
        for d in TIGHT_DISTANCES: add_if("TIGHT",15,d,1.00,1.75)
        for lb in TIGHT_LOOKBACKS: add_if("TIGHT",lb,0.10,1.00,1.75)
        for b in TIGHT_BODY_ATR: add_if("TIGHT",15,0.10,b,1.75)
        # Preserve the four exact Pass 1 controls explicitly.
        add_if("BROAD",30,0.75,0.60,1.00); add_if("BROAD",30,0.75,0.80,1.00)
        add_if("TIGHT",15,0.10,1.00,1.75); add_if("TIGHT",15,0.20,1.00,1.75)
        # Add top 5 stressed rows per branch as diagnostics, not selections.
        for branch in ("BROAD","TIGHT"):
            ids=[r["config_id"] for r in screen_rows if r["branch"]==branch][:5]
            diag_ids.update(ids)

        set_status(state="diagnostics", progress=82, message=f"Building detailed diagnostics for {len(diag_ids)} predeclared/top boundary rows")
        ledger_rows=[]; period_rows=[]; year_rows=[]; rolling_rows=[]; rolling_summary_rows=[]
        diagnostic_configs=[("RAW_EXACT_BULLISH_ENGULF",raw_accepted)] + [(cid,accepted_by_id[cid]) for cid in sorted(diag_ids)]
        for config_id,accepted in diagnostic_configs:
            for cost_label, ticks, pips, purpose in COST_CASES:
                for seq,p in enumerate(accepted,start=1):
                    r=records[p]
                    ledger_rows.append({
                        "config_id":config_id,"cost_label":cost_label,"sequence":seq,
                        "signal_time":iso(r["signal_time"]),"exit_time":iso(r["exit_time"]),"signal_index":r["signal_index"],"exit_index":r["exit_index"],
                        "reference_entry":r["reference_entry"],"historical_fill":r[f"fill__{cost_label}"],"stop":r["stop"],"target":r["target"],
                        "exit_reason":r["exit_reason"],"result_r":r[f"result_r__{cost_label}"],
                    })
                p_rows,y_rows,r_rows,rs_rows=diagnostics_for_config(records,config_id,accepted,cost_label)
                period_rows.extend(p_rows); year_rows.extend(y_rows); rolling_rows.extend(r_rows); rolling_summary_rows.extend(rs_rows)
        write_csv(OUTPUTS["screen_ledgers"],ledger_rows)
        write_csv(OUTPUTS["periods"],period_rows)
        write_csv(OUTPUTS["years"],year_rows)
        write_csv(OUTPUTS["rolling"],rolling_rows)
        write_csv(OUTPUTS["rolling_summary"],rolling_summary_rows)

        methodology=[
            {"topic":"purpose","value":"Pass 1B boundary clarification only: map the two Pass 1 AUD/JPY H1 LONG engulfing regions before conditional-filter work."},
            {"topic":"frozen_source","value":"Uses exact Pass 1 cutoff/source fingerprints and fails closed if H1/D1 or raw signal history changed."},
            {"topic":"controls","value":"Four exact Pass 1 geometry rows are aggregate-parity checked at all three costs; two broad controls also require full accepted-ledger SHA256 parity at 20T."},
            {"topic":"broad_branch","value":f"lookbacks={BROAD_LOOKBACKS}; distances={BROAD_DISTANCES}; body={BROAD_BODY_ATR}; range={BROAD_RANGE_ATR}"},
            {"topic":"tight_branch","value":f"lookbacks={TIGHT_LOOKBACKS}; distances={TIGHT_DISTANCES}; body={TIGHT_BODY_ATR}; range={TIGHT_RANGE_ATR}"},
            {"topic":"execution","value":"exact bullish engulf; reference=signal close; stop=signal low-10 ticks; RR3.50 fixed; exits next H1 candle onward; p0; exit-candle re-entry eligible"},
            {"topic":"costs","value":"10T primary live-parity; 20T stressed selection; 40T extreme diagnostic only; assumed adverse MID shifts, not measured historical executable spread/slippage."},
            {"topic":"not_tested","value":"No new conditional filters, no weekday/session search, no RR optimisation, no Portfolio 29 feedback."},
            {"topic":"interpretation","value":"Inspect branch breadth, neighbouring levels, weak rows, eras and rolling windows. Do not freeze the top row merely because it has the largest historical R."},
            {"topic":"data_snooping","value":"All history repeatedly examined/in-sample. Pass 1B clarifies geometry; it is not prospective validation."},
            {"topic":"next_gate","value":"If one/both regions show stable interior plateaus, freeze distinguishable anchor(s) and move to Pass 2 one conditional feature at a time."},
        ]
        write_csv(OUTPUTS["methodology"],methodology)

        if OUTPUTS["errors"].exists(): OUTPUTS["errors"].unlink()
        pack_results()
        set_status(
            state="complete",progress=100,message="AUD/JPY H1 LONG Pass 1B complete; ZIP ready",
            h1_candles=len(h1),d1_candles=len(d1),raw_exact_signals=len(vec_raw),raw_closed_outcomes=len(records),
            broad_grid_configurations=BROAD_GRID_ROWS,tight_grid_configurations=TIGHT_GRID_ROWS,total_grid_configurations=count,
            diagnostic_configurations=len(diag_ids),parity_passed=True,h1_source_sha256=h1_sha,raw_signal_sha256=vec_hash,
            results_zip=str(BUNDLE),
        )
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
        RESEARCH_STARTED = True
        threading.Thread(target=run_research, daemon=True, name="audjpy-h1-long-pass1b").start()
        return True


@app.route("/")
def root():
    return jsonify({
        "service": "AUD/JPY H1 LONG Pass 1B boundary clarification",
        "patch_version": PATCH_VERSION,
        "hash_reference_fix": True,
        "expected_broad_ledger_sha20": {
            "LB30_D075_B060_R100": EXPECTED_BROAD_LEDGER_SHA20[(30, 0.75, 0.60, 1.00)],
            "LB30_D075_B080_R100": EXPECTED_BROAD_LEDGER_SHA20[(30, 0.75, 0.80, 1.00)],
        },
        "research_only": True,
        "orders_supported": False,
        "trading_enabled": False,
        "pair": PAIR,
        "timeframe": TIMEFRAME,
        "side": SIDE,
        "rr_fixed": REFERENCE_RR,
        "cost_cases": [
            {"label": label, "ticks": ticks, "pips": pips, "purpose": purpose}
            for label, ticks, pips, purpose in COST_CASES
        ],
        "controlled_grid_rows": EXPECTED_GRID_ROWS,
        "frozen_end_exclusive": iso(DATA_END),
        "routes": [
            "/audjpy-h1-long-pass1b/start",
            "/audjpy-h1-long-pass1b/status",
            "/audjpy-h1-long-pass1b/results",
        ],
    })


@app.route("/audjpy-h1-long-pass1b/start")
def start_route():
    return jsonify({"started_now": launch_once(), "state": STATUS["state"], "orders_supported": False})


@app.route("/audjpy-h1-long-pass1b/status")
def status_route():
    with STATUS_LOCK:
        return jsonify(dict(STATUS))


@app.route("/audjpy-h1-long-pass1b/results")
def results_route():
    if not BUNDLE.exists():
        return jsonify({"status": "not_ready", "state": STATUS["state"], "message": STATUS["message"]}), 404
    return send_file(str(BUNDLE.resolve()), as_attachment=True, download_name=BUNDLE.name)


if __name__ == "__main__":
    launch_once()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)
