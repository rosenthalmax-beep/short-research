#!/usr/bin/env python3
"""
AUD/JPY H1 LONG — Pass 3 boundary + justified interactions
===========================================================

RESEARCH ONLY. READ ONLY. NEVER PLACES ORDERS OR MODIFIES THE LIVE EXECUTOR.

Pass 1/1B established two frozen exact-bullish-engulf anchors. Pass 2 tested
one conditional feature at a time and found one cross-anchor effect strong
enough to carry forward: a cap on current H1 ATR relative to its previous-50
H1 ATR mean. The TIGHT anchor also showed a coherent prior-fall neighbourhood
and secondary bearish completed-D1 regimes.

Pass 3 is deliberately bounded. It does NOT reopen the full Pass 2 feature
search. It only:
1) maps the H1-volatility cap boundary on BOTH frozen anchors;
2) maps the TIGHT prior-fall neighbourhood across nearby lookbacks/thresholds;
3) tests three predeclared TIGHT two-factor interaction families:
   H1-vol cap x prior fall, H1-vol cap x bearish D1 regime,
   prior fall x bearish D1 regime.

Frozen anchors
--------------
BROAD: LB30, structure distance<=0.75 ATR14, body>=0.80 ATR14,
       range>=1.00 ATR14.
TIGHT: LB12, structure distance<=0.15 ATR14, body>=1.00 ATR14,
       range>=1.75 ATR14.

Frozen execution
----------------
- exact bullish body engulfing
- OANDA completed MID H1 candles
- ATR14 Wilder/RMA, SMA seeded
- reference entry = signal close
- stop = signal low - 10 ticks
- RR3.50 fixed
- 10T / 1 pip adverse entry = primary live-parity case
- 20T / 2 pips = stressed selection case
- 40T / 4 pips = extreme diagnostic only
- exits begin next H1 candle
- p0 within each candidate; exact exit-candle signal remains eligible

The source cutoff and exact Pass 1B fingerprints remain frozen. BOTH anchors
must reproduce complete accepted-ledger SHA256 hashes at all three costs before
any Pass 3 candidate is evaluated.

No RR sweep, weekday/session search, new entry geometry, three-way interaction,
or Portfolio 29 feedback occurs here. All history is repeatedly examined and
in-sample; this is robustness mapping, not OOS proof.

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
DATA_END = datetime(2026, 9, 25, 19, 0, tzinfo=timezone.utc)  # exclusive; exact Pass 1B cutoff
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

# Feature preparation. Structure itself is frozen in Pass 2; these lookbacks
# preserve the Pass 1B raw-outcome schema and support exact anchor parity.
STRUCTURE_LOOKBACKS = (10, 12, 15, 20, 25, 30, 40, 60, 80, 100, 150)
GRID_LOOKBACKS = (12, 30)
MOMENTUM_LOOKBACKS = (4, 8, 10, 12, 14, 16, 24, 48)

ANCHORS = {
    "BROAD": dict(lookback=30, distance_atr_max=0.75, body_atr_min=0.80, range_atr_min=1.00),
    "TIGHT": dict(lookback=12, distance_atr_max=0.15, body_atr_min=1.00, range_atr_min=1.75),
}

# Pass 3 predeclared boundary and interaction levels. These are intentionally
# narrow and justified only by the clean Pass 2 findings.
H1_VOL_CAP_LEVELS = (1.00, 1.05, 1.10, 1.15, 1.20, 1.25, 1.30, 1.35, 1.40, 1.45, 1.50, 1.55, 1.60)
TIGHT_FALL_LOOKBACKS = (8, 10, 12, 14, 16)
TIGHT_FALL_LEVELS = (0.30, 0.40, 0.50, 0.60, 0.75, 0.90, 1.00)
INTERACTION_VOL_LEVELS = (1.10, 1.20, 1.30, 1.40, 1.50)
INTERACTION_FALL_LEVELS = (0.40, 0.50, 0.60, 0.75, 0.90)
BEARISH_D1_REGIMES = (
    ("D1_CLOSE_LE_EMA50", "close<=ema50"),
    ("D1_CLOSE_LE_EMA100", "close<=ema100"),
    ("D1_EMA50_LE_EMA200", "ema50<=ema200"),
)

EXPECTED_CANDIDATES = (
    2 * len(H1_VOL_CAP_LEVELS)
    + len(TIGHT_FALL_LOOKBACKS) * len(TIGHT_FALL_LEVELS)
    + len(INTERACTION_VOL_LEVELS) * len(INTERACTION_FALL_LEVELS)
    + len(INTERACTION_VOL_LEVELS) * len(BEARISH_D1_REGIMES)
    + len(INTERACTION_FALL_LEVELS) * len(BEARISH_D1_REGIMES)
)
assert EXPECTED_CANDIDATES == 116

# Exact clean Pass 1B source/raw-signal fingerprints.
EXPECTED_H1_ROWS = 137969
EXPECTED_D1_ROWS = 6473
EXPECTED_H1_SHA256 = "2ebb773ab6d8bd53b3725265e16b879952436f0e198682142e490ff0e0f5957a"
EXPECTED_D1_SHA256 = "611c4edf4801888e0be378f36368ce2232fecd86f6c758f14d17fbaa0759688b"
EXPECTED_RAW_SIGNAL_COUNT = 10624
EXPECTED_RAW_SIGNAL_SHA256 = "47dd2498e9ad03ac69c3f8f8d758b878808d08bb5c72fecad8b87542d65a329f"

# Frozen anchor controls from the clean Pass 1B rerun. Metrics and complete
# accepted-ledger hashes are checked at ALL costs.
ANCHOR_EXPECTED = {
    "BROAD": {
        "qualified_raw_signals": 832,
        "accepted_trades": 759,
        "LIVE_LIMIT_10T": dict(total_r=106.02450750543557, profit_factor=1.1896681708505108, max_drawdown_r=-35.18568598052075, ledger_sha256="5092a6c489473c501572fcbf3e24c1fef17c6d1a33b79aae6eb0801195287b73"),
        "STRESS_20T": dict(total_r=74.14845801149505, profit_factor=1.1326448264964133, max_drawdown_r=-36.265503990951714, ledger_sha256="5dedb34c63cf36dda292b4ab7e56b972c7fe1c150b4e9ffa10fa404a94dfa1ae"),
        "EXTREME_40T": dict(total_r=17.99812257777572, profit_factor=1.0321969992446791, max_drawdown_r=-44.77699627950781, ledger_sha256="f8ffb89f772f21406d469ddc2fa2ba81b6818147f32be6c5d4e1288824f30b39"),
    },
    "TIGHT": {
        "qualified_raw_signals": 83,
        "accepted_trades": 78,
        "LIVE_LIMIT_10T": dict(total_r=31.32147629841921, profit_factor=1.59097125091357, max_drawdown_r=-8.69437826044886, ledger_sha256="611a62d2edc3fe9bedf82f93c614d5e77679bcb1b9b82ad252390d10a84b696e"),
        "STRESS_20T": dict(total_r=28.34279459079046, profit_factor=1.5347697092601973, max_drawdown_r=-8.88175517934733, ledger_sha256="0e8809b2250a77761e7a48739842c3fb1d71373c8ecc0ba99669d1b078b81605"),
        "EXTREME_40T": dict(total_r=22.90899408682286, profit_factor=1.432245171449488, max_drawdown_r=-9.23721351385768, ledger_sha256="edf36433a5d0aec38c45b3ac782b05642e0f0cfe1bc675cf43358f2262b37437"),
    },
}

PASS_VERSION = "AUDJPY_H1_LONG_PASS3_BOUNDARY_INTERACTIONS_V1_2026-09-25"

API = os.getenv("OANDA_API_URL", "https://api-fxtrade.oanda.com").rstrip("/")
TOKEN = os.getenv("OANDA_TOKEN", "")

OUT_DIR = Path(os.getenv("AUDJPY_H1_LONG_PASS3_OUTPUT_DIR", "/tmp/audjpy_h1_long_pass3"))
OUT_DIR.mkdir(parents=True, exist_ok=True)
BUNDLE = OUT_DIR / "AUDJPY_H1_LONG_PASS3_BOUNDARY_INTERACTIONS_RESULTS.zip"

OUTPUTS = {
    "coverage": OUT_DIR / "coverage.csv",
    "source_fingerprint": OUT_DIR / "source_fingerprint.csv",
    "parity": OUT_DIR / "parity.csv",
    "anchor_parity": OUT_DIR / "anchor_parity.csv",
    "anchor_ledgers": OUT_DIR / "anchor_accepted_ledgers.csv",
    "raw_signals": OUT_DIR / "raw_signal_outcomes.csv",
    "factor_plan": OUT_DIR / "pass3_plan.csv",
    "conditional_summary": OUT_DIR / "candidate_summary.csv",
    "delta_vs_anchor": OUT_DIR / "delta_vs_anchor.csv",
    "family_summary": OUT_DIR / "family_summary_20T.csv",
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
    "message": "AUD/JPY H1 LONG Pass 3 waiting",
    "orders_supported": False,
    "trading_enabled": False,
    "pair": PAIR,
    "timeframe": TIMEFRAME,
    "side": SIDE,
    "rr_fixed": REFERENCE_RR,
    "pass_version": PASS_VERSION,
    "anchors": ANCHORS,
    "candidate_configurations": EXPECTED_CANDIDATES,
    "frozen_pass1b_cutoff": DATA_END.isoformat().replace("+00:00", "Z"),
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
        previous_body, atr,
        out=np.full(len(candles), np.nan),
        where=np.isfinite(previous_body) & np.isfinite(atr) & (atr > 0),
    )
    previous_range_atr = np.divide(
        previous_range, atr,
        out=np.full(len(candles), np.nan),
        where=np.isfinite(previous_range) & np.isfinite(atr) & (atr > 0),
    )
    previous_close_location = np.divide(
        pc - pl, previous_range,
        out=np.full(len(candles), np.nan),
        where=np.isfinite(previous_range) & (previous_range > 0),
    )
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
        "previous_body_atr": previous_body_atr,
        "previous_range_atr": previous_range_atr,
        "previous_close_location": previous_close_location,
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
            "previous_body_atr": float(features["previous_body_atr"][i]) if math.isfinite(features["previous_body_atr"][i]) else math.nan,
            "previous_range_atr": float(features["previous_range_atr"][i]) if math.isfinite(features["previous_range_atr"][i]) else math.nan,
            "previous_close_location": float(features["previous_close_location"][i]) if math.isfinite(features["previous_close_location"][i]) else math.nan,
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
        "lower_wick_body", "previous_body_atr", "previous_range_atr",
        "previous_close_location", "h1_atr_ratio", "daily_atr_ratio",
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
# PASS 3 EXPERIMENT PLAN — BOUNDED BOUNDARIES + JUSTIFIED INTERACTIONS
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


def bearish_daily_regime_masks(arr):
    return {
        "D1_CLOSE_LE_EMA50": ~arr["daily_close_gt_ema50"],
        "D1_CLOSE_LE_EMA100": ~arr["daily_close_gt_ema100"],
        "D1_EMA50_LE_EMA200": ~arr["daily_ema50_gt_ema200"],
    }


def pass3_candidate_plan(arr):
    """Yield the 116 predeclared Pass 3 candidates. No hidden search."""
    h1v = arr["h1_atr_ratio"]
    regimes = bearish_daily_regime_masks(arr)

    # 1) Cross-anchor H1 volatility-cap boundary map.
    for anchor in ("BROAD", "TIGHT"):
        for v in H1_VOL_CAP_LEVELS:
            mask = np.isfinite(h1v) & (h1v <= v)
            yield dict(
                anchor=anchor,
                candidate_id=f"P3_{anchor}__H1_ATR_RATIO_MAX_{fmt_level(v)}",
                family="h1_vol_boundary",
                description=f"H1 ATR ratio <= {v}",
                parameter_1="h1_atr_ratio", operator_1="<=", threshold_1=v,
                parameter_2="", operator_2="", threshold_2="",
                mask=mask,
            )

    # 2) TIGHT prior-fall boundary map.
    for lb in TIGHT_FALL_LOOKBACKS:
        x = arr[f"momentum_{lb}"]
        for v in TIGHT_FALL_LEVELS:
            mask = np.isfinite(x) & (x <= -v)
            yield dict(
                anchor="TIGHT",
                candidate_id=f"P3_TIGHT__PRIOR_FALL_LB{lb}_MAX_NEG{fmt_level(v)}",
                family="tight_prior_fall",
                description=f"prior {lb} H1 movement <= -{v} ATR",
                parameter_1=f"momentum_{lb}", operator_1="<=", threshold_1=-v,
                parameter_2="", operator_2="", threshold_2="",
                mask=mask,
            )

    # 3a) TIGHT H1-volatility cap x prior-12H fall.
    x12 = arr["momentum_12"]
    for vol in INTERACTION_VOL_LEVELS:
        for fall in INTERACTION_FALL_LEVELS:
            mask = np.isfinite(h1v) & (h1v <= vol) & np.isfinite(x12) & (x12 <= -fall)
            yield dict(
                anchor="TIGHT",
                candidate_id=f"P3_TIGHT__H1ATR_MAX_{fmt_level(vol)}__FALL_LB12_NEG{fmt_level(fall)}",
                family="tight_vol_x_fall",
                description=f"H1 ATR ratio <= {vol} AND prior12 movement <= -{fall} ATR",
                parameter_1="h1_atr_ratio", operator_1="<=", threshold_1=vol,
                parameter_2="momentum_12", operator_2="<=", threshold_2=-fall,
                mask=mask,
            )

    # 3b) TIGHT H1-volatility cap x one bearish completed-D1 regime.
    for vol in INTERACTION_VOL_LEVELS:
        for regime_id, regime_label in BEARISH_D1_REGIMES:
            mask = np.isfinite(h1v) & (h1v <= vol) & regimes[regime_id]
            yield dict(
                anchor="TIGHT",
                candidate_id=f"P3_TIGHT__H1ATR_MAX_{fmt_level(vol)}__{regime_id}",
                family="tight_vol_x_daily",
                description=f"H1 ATR ratio <= {vol} AND {regime_label}",
                parameter_1="h1_atr_ratio", operator_1="<=", threshold_1=vol,
                parameter_2="daily_regime", operator_2="bool", threshold_2=regime_label,
                mask=mask,
            )

    # 3c) TIGHT prior-12H fall x one bearish completed-D1 regime.
    for fall in INTERACTION_FALL_LEVELS:
        for regime_id, regime_label in BEARISH_D1_REGIMES:
            mask = np.isfinite(x12) & (x12 <= -fall) & regimes[regime_id]
            yield dict(
                anchor="TIGHT",
                candidate_id=f"P3_TIGHT__FALL_LB12_NEG{fmt_level(fall)}__{regime_id}",
                family="tight_fall_x_daily",
                description=f"prior12 movement <= -{fall} ATR AND {regime_label}",
                parameter_1="momentum_12", operator_1="<=", threshold_1=-fall,
                parameter_2="daily_regime", operator_2="bool", threshold_2=regime_label,
                mask=mask,
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

def run_research():
    try:
        set_status(state="fetching", progress=2, message="Fetching exact frozen Pass 1B H1/D1 OANDA midpoint history")
        h1 = fetch_history("H1", REQUESTED_START, DATA_END, 180)
        d1 = fetch_history("D", D1_WARMUP_START, DATA_END, 1200)

        h1_times = [x["time"] for x in h1]
        d1_times = [x["time"] for x in d1]
        if h1_times != sorted(set(h1_times)):
            raise RuntimeError("H1 timestamps are duplicated or non-monotonic")
        if d1_times != sorted(set(d1_times)):
            raise RuntimeError("D1 timestamps are duplicated or non-monotonic")
        if not h1 or not d1 or h1[-1]["time"] >= DATA_END:
            raise RuntimeError("Frozen source coverage invalid")

        h1_sha = sha_rows(f"{iso(x['time'])}|{x['open']:.6f}|{x['high']:.6f}|{x['low']:.6f}|{x['close']:.6f}" for x in h1)
        d1_sha = sha_rows(f"{iso(x['time'])}|{x['open']:.6f}|{x['high']:.6f}|{x['low']:.6f}|{x['close']:.6f}" for x in d1)
        write_csv(OUTPUTS["coverage"], [
            {"pair":PAIR,"timeframe":"H1","requested_start":iso(REQUESTED_START),"first_completed_candle":iso(h1[0]["time"]),"last_completed_candle_open":iso(h1[-1]["time"]),"frozen_end_exclusive":iso(DATA_END),"completed_candles":len(h1)},
            {"pair":PAIR,"timeframe":"D","requested_start":iso(D1_WARMUP_START),"first_completed_candle":iso(d1[0]["time"]),"last_completed_candle_open":iso(d1[-1]["time"]),"frozen_end_exclusive":iso(DATA_END),"completed_candles":len(d1),"daily_alignment":"17:00 America/New_York"},
        ])
        write_csv(OUTPUTS["source_fingerprint"], [
            {"series":"AUD_JPY_H1_MID_OHLC","sha256":h1_sha,"rows":len(h1)},
            {"series":"AUD_JPY_D1_MID_OHLC","sha256":d1_sha,"rows":len(d1)},
        ])

        checks = [
            {"check":"H1_ROW_COUNT","status":"PASS" if len(h1)==EXPECTED_H1_ROWS else "FAIL","actual":len(h1),"expected":EXPECTED_H1_ROWS},
            {"check":"D1_ROW_COUNT","status":"PASS" if len(d1)==EXPECTED_D1_ROWS else "FAIL","actual":len(d1),"expected":EXPECTED_D1_ROWS},
            {"check":"H1_SOURCE_SHA256","status":"PASS" if h1_sha==EXPECTED_H1_SHA256 else "FAIL","actual":h1_sha,"expected":EXPECTED_H1_SHA256},
            {"check":"D1_SOURCE_SHA256","status":"PASS" if d1_sha==EXPECTED_D1_SHA256 else "FAIL","actual":d1_sha,"expected":EXPECTED_D1_SHA256},
        ]
        if any(x["status"] != "PASS" for x in checks):
            write_csv(OUTPUTS["parity"], checks)
            raise RuntimeError("Frozen Pass 1B source parity FAILED; do not interpret Pass 3")

        set_status(state="features", progress=10, message="Building H1/D1 features and raw-signal parity")
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
            "check":"RAW_SIGNAL_VECTOR_SCALAR_AND_PASS1B_FINGERPRINT",
            "status":"PASS" if raw_ok else "FAIL",
            "vector_count":len(vec_raw),"scalar_count":len(scalar_raw),
            "vector_sha256":vec_hash,"scalar_sha256":scalar_hash,
            "expected_count":EXPECTED_RAW_SIGNAL_COUNT,"expected_sha256":EXPECTED_RAW_SIGNAL_SHA256,
        })
        if not raw_ok:
            write_csv(OUTPUTS["parity"], checks)
            raise RuntimeError("Raw signal parity/fingerprint FAILED")

        set_status(state="raw_replay", progress=16, message="Replaying frozen raw exact-engulf outcomes")
        records, censored = build_raw_outcomes(features, vec_raw, daily_states)
        exec_failures = execution_parity_sample(features, h1, records, sample_n=min(100,len(records)))
        checks.append({
            "check":"EXECUTION_SCALAR_RECOMPUTE_FIRST_100",
            "status":"PASS" if not exec_failures else "FAIL",
            "sample":min(100,len(records)),"failures":"; ".join(exec_failures[:10]),
        })
        write_csv(OUTPUTS["parity"], checks)
        if exec_failures:
            raise RuntimeError("Execution parity FAILED")

        raw_export=[]
        for r in records:
            row=dict(r); row["signal_time"]=iso(row["signal_time"]); row["exit_time"]=iso(row["exit_time"]); raw_export.append(row)
        write_csv(OUTPUTS["raw_signals"], raw_export)
        arr = all_record_arrays(records)

        set_status(state="anchor_parity", progress=22, message="Reproducing both frozen Pass 1B anchors at all costs")
        anchor_masks = {name: anchor_mask(arr,name) for name in ANCHORS}
        anchor_accepted = {}
        anchor_rows = []
        anchor_ledger_rows = []
        parity_rows = []
        for name in ANCHORS:
            summary, accepted = summarize_config(records, anchor_masks[name], {
                "config_id":f"ANCHOR_{name}","anchor":name,"stage":"PASS2_FROZEN_ANCHOR","rr":REFERENCE_RR,**ANCHORS[name],
            })
            anchor_accepted[name] = accepted
            anchor_rows.extend(summary)
            expected = ANCHOR_EXPECTED[name]
            qualified_count = int(np.sum(anchor_masks[name]))
            for row in summary:
                label=row["cost_label"]
                exp=expected[label]
                ledger_sha=accepted_ledger_hash(records, accepted, label)
                ok=(
                    qualified_count==expected["qualified_raw_signals"]
                    and len(accepted)==expected["accepted_trades"]
                    and abs(float(row["total_r"])-float(exp["total_r"]))<1e-10
                    and abs(float(row["profit_factor"])-float(exp["profit_factor"]))<1e-10
                    and abs(float(row["max_drawdown_r"])-float(exp["max_drawdown_r"]))<1e-10
                    and ledger_sha==exp["ledger_sha256"]
                )
                parity_rows.append({
                    "anchor":name,"cost_label":label,"status":"PASS" if ok else "FAIL",
                    "qualified_raw_signals":qualified_count,"expected_qualified_raw_signals":expected["qualified_raw_signals"],
                    "accepted_trades":len(accepted),"expected_accepted_trades":expected["accepted_trades"],
                    "total_r":row["total_r"],"expected_total_r":exp["total_r"],
                    "profit_factor":row["profit_factor"],"expected_profit_factor":exp["profit_factor"],
                    "max_drawdown_r":row["max_drawdown_r"],"expected_max_drawdown_r":exp["max_drawdown_r"],
                    "ledger_sha256":ledger_sha,"expected_ledger_sha256":exp["ledger_sha256"],
                })
                for seq,p in enumerate(accepted,start=1):
                    r=records[p]
                    anchor_ledger_rows.append({
                        "anchor":name,"cost_label":label,"sequence":seq,
                        "signal_time":iso(r["signal_time"]),"exit_time":iso(r["exit_time"]),
                        "signal_index":r["signal_index"],"exit_index":r["exit_index"],
                        "reference_entry":r["reference_entry"],"historical_fill":r[f"fill__{label}"],
                        "stop":r["stop"],"target":r["target"],"exit_reason":r["exit_reason"],
                        "result_r":r[f"result_r__{label}"],
                    })
        write_csv(OUTPUTS["anchor_parity"], parity_rows)
        write_csv(OUTPUTS["anchor_ledgers"], anchor_ledger_rows)
        if any(x["status"] != "PASS" for x in parity_rows):
            raise RuntimeError("Frozen Pass 1B anchor full-ledger parity FAILED; do not interpret Pass 3")

        candidates = list(pass3_candidate_plan(arr))
        if len(candidates) != EXPECTED_CANDIDATES:
            raise RuntimeError(f"Pass 3 plan enumeration mismatch: expected {EXPECTED_CANDIDATES}, got {len(candidates)}")
        plan_rows=[]
        for i,c in enumerate(candidates,start=1):
            plan_rows.append({
                "candidate_number":i,"candidate_id":c["candidate_id"],"anchor":c["anchor"],
                "family":c["family"],"description":c["description"],
                "parameter_1":c["parameter_1"],"operator_1":c["operator_1"],"threshold_1":c["threshold_1"],
                "parameter_2":c["parameter_2"],"operator_2":c["operator_2"],"threshold_2":c["threshold_2"],
                "application":"PREDECLARED_PASS3_BOUNDARY_OR_TWO_FACTOR_INTERACTION",
            })
        write_csv(OUTPUTS["factor_plan"], plan_rows)

        set_status(state="candidate_scan", progress=30, message=f"Running {EXPECTED_CANDIDATES} bounded Pass 3 configurations")
        summary_rows=[]
        delta_rows=[]
        accepted_by_id={}
        candidate_meta={}
        candidate_count=0
        for c in candidates:
            candidate_count += 1
            if candidate_count % 20 == 0:
                set_status(message=f"Pass 3 candidate {candidate_count}/{EXPECTED_CANDIDATES}")
            anchor=c["anchor"]
            amask=anchor_masks[anchor]
            anchor_qualified=int(np.sum(amask))
            cid=c["candidate_id"]
            cmask=amask & c["mask"]
            meta={
                "config_id":cid,"stage":"PASS3_BOUNDARY_INTERACTION","anchor":anchor,
                "factor_id":cid,"factor_family":c["family"],
                "description":c["description"],
                "parameter_1":c["parameter_1"],"operator_1":c["operator_1"],"threshold_1":c["threshold_1"],
                "parameter_2":c["parameter_2"],"operator_2":c["operator_2"],"threshold_2":c["threshold_2"],
                "rr":REFERENCE_RR,**ANCHORS[anchor],
            }
            rows,accepted=summarize_config(records,cmask,meta)
            accepted_by_id[cid]=accepted
            candidate_meta[cid]=meta
            candidate_qualified=int(np.sum(cmask))
            for row in rows:
                row["anchor_qualified_raw_signals"]=anchor_qualified
                row["conditional_qualified_raw_signals"]=candidate_qualified
                row["qualified_signal_retention_pct"]=100.0*candidate_qualified/anchor_qualified if anchor_qualified else 0.0
                summary_rows.append(row)
                comp=compare_to_anchor(records,anchor_accepted[anchor],accepted,row["cost_label"])
                delta_rows.append({
                    "config_id":cid,"anchor":anchor,"factor_id":cid,"factor_family":c["family"],
                    "description":c["description"],
                    "parameter_1":c["parameter_1"],"operator_1":c["operator_1"],"threshold_1":c["threshold_1"],
                    "parameter_2":c["parameter_2"],"operator_2":c["operator_2"],"threshold_2":c["threshold_2"],
                    "cost_label":row["cost_label"],
                    "anchor_qualified_raw_signals":anchor_qualified,"candidate_qualified_raw_signals":candidate_qualified,
                    "qualified_signal_retention_pct":100.0*candidate_qualified/anchor_qualified if anchor_qualified else 0.0,
                    **comp,
                })
        if candidate_count != EXPECTED_CANDIDATES:
            raise RuntimeError(f"Candidate enumeration mismatch: expected {EXPECTED_CANDIDATES}, got {candidate_count}")
        write_csv(OUTPUTS["conditional_summary"], summary_rows)
        write_csv(OUTPUTS["delta_vs_anchor"], delta_rows)
        write_csv(OUTPUTS["family_summary"], family_summary_rows(summary_rows,delta_rows))

        screen_rows=diagnostic_screen_rows(summary_rows,delta_rows)
        write_csv(OUTPUTS["screen"],screen_rows)
        diag_ids=sorted({x["config_id"] for x in screen_rows})

        set_status(state="diagnostics", progress=82, message=f"Building periods/rolling diagnostics for {len(diag_ids)} mechanical screen rows plus both anchors")
        ledger_rows=[]; period_rows=[]; year_rows=[]; rolling_rows=[]; rolling_summary_rows=[]
        diagnostics=[]
        for anchor in ANCHORS:
            diagnostics.append((f"ANCHOR_{anchor}",anchor,anchor_accepted[anchor]))
        for cid in diag_ids:
            diagnostics.append((cid,candidate_meta[cid]["anchor"],accepted_by_id[cid]))
        for config_id,anchor,accepted in diagnostics:
            for label,ticks,pips,purpose in COST_CASES:
                for seq,p in enumerate(accepted,start=1):
                    r=records[p]
                    ledger_rows.append({
                        "config_id":config_id,"anchor":anchor,"cost_label":label,"sequence":seq,
                        "signal_time":iso(r["signal_time"]),"exit_time":iso(r["exit_time"]),
                        "signal_index":r["signal_index"],"exit_index":r["exit_index"],
                        "reference_entry":r["reference_entry"],"historical_fill":r[f"fill__{label}"],
                        "stop":r["stop"],"target":r["target"],"exit_reason":r["exit_reason"],
                        "result_r":r[f"result_r__{label}"],
                    })
                p_rows,y_rows,r_rows,rs_rows=diagnostics_for_config(records,config_id,accepted,label)
                for x in p_rows: x["anchor"]=anchor
                for x in y_rows: x["anchor"]=anchor
                for x in r_rows: x["anchor"]=anchor
                for x in rs_rows: x["anchor"]=anchor
                period_rows.extend(p_rows); year_rows.extend(y_rows); rolling_rows.extend(r_rows); rolling_summary_rows.extend(rs_rows)
        write_csv(OUTPUTS["screen_ledgers"],ledger_rows)
        write_csv(OUTPUTS["periods"],period_rows)
        write_csv(OUTPUTS["years"],year_rows)
        write_csv(OUTPUTS["rolling"],rolling_rows)
        write_csv(OUTPUTS["rolling_summary"],rolling_summary_rows)

        methodology=[
            {"topic":"purpose","value":"Pass 3 bounded boundary/interaction study using only effects justified by clean Pass 2."},
            {"topic":"broad_anchor","value":json.dumps(ANCHORS["BROAD"],sort_keys=True)},
            {"topic":"tight_anchor","value":json.dumps(ANCHORS["TIGHT"],sort_keys=True)},
            {"topic":"parity","value":"Fails closed unless exact Pass 1B H1/D1/raw-signal fingerprints and both full anchor ledgers match at 10T/20T/40T."},
            {"topic":"execution","value":"exact bullish engulf; signal-close reference; stop=signal low-10 ticks; RR3.50 fixed; next-H1 exits; p0; exact exit-candle re-entry eligible."},
            {"topic":"costs","value":"10T primary live-parity; 20T stressed selection; 40T extreme diagnostic only. Historical MID shifts are assumed costs, not measured executable spreads/slippage."},
            {"topic":"h1_vol_boundary","value":"Both anchors: H1 ATR/current ATR50-prev ratio cap mapped from 1.00 to 1.60 in 0.05 steps."},
            {"topic":"tight_prior_fall_boundary","value":"TIGHT only: prior H1 movement lookbacks 8/10/12/14/16 with fall thresholds -0.30 to -1.00 signal ATR."},
            {"topic":"tight_interactions","value":"Only three predeclared two-factor families: H1-vol cap x prior12 fall; H1-vol cap x bearish completed-D1 regime; prior12 fall x bearish completed-D1 regime."},
            {"topic":"daily_regimes","value":"Only close<=EMA50, close<=EMA100, and EMA50<=EMA200 from Pass 2 bearish-regime evidence."},
            {"topic":"p0_attribution","value":"delta_vs_anchor reports removed anchor trades and newly accepted later signals after full chronological replay."},
            {"topic":"not_tested","value":"No RR sweep, no weekday/session search, no body/range/structure retuning, no new filters, no three-way interactions, no Portfolio 29 selection feedback."},
            {"topic":"diagnostic_screen","value":"Mechanical screen is reporting only; it does not freeze a rule. Full neighbourhood/era/rolling evidence must be reviewed."},
            {"topic":"data_snooping","value":"All history has been repeatedly examined and is in-sample. Recent/era/rolling diagnostics are robustness descriptions, not untouched OOS tests."},
            {"topic":"next_gate","value":"If a stable interior region remains, freeze at most one BROAD and one TIGHT conditional geometry before local confirmation/RR-last stages."},
        ]
        write_csv(OUTPUTS["methodology"],methodology)

        if OUTPUTS["errors"].exists():
            OUTPUTS["errors"].unlink()
        pack_results()
        set_status(
            state="complete",progress=100,message="AUD/JPY H1 LONG Pass 3 complete; ZIP ready",
            parity_passed=True,h1_candles=len(h1),d1_candles=len(d1),raw_exact_signals=len(vec_raw),raw_closed_outcomes=len(records),
            candidate_configurations=candidate_count,
            diagnostic_configurations=len(diag_ids),h1_source_sha256=h1_sha,raw_signal_sha256=vec_hash,results_zip=str(BUNDLE),
        )
    except Exception as exc:
        tb=traceback.format_exc()
        write_csv(OUTPUTS["errors"],[{"error_type":type(exc).__name__,"message":str(exc),"traceback":tb}])
        try:
            pack_results()
        except Exception:
            pass
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
        threading.Thread(target=run_research,daemon=True,name="audjpy-h1-long-pass3").start()
        return True


@app.route("/")
def root():
    return jsonify({
        "service":"AUD/JPY H1 LONG Pass 3 boundary + justified interactions",
        "pass_version":PASS_VERSION,
        "research_only":True,"orders_supported":False,"trading_enabled":False,
        "pair":PAIR,"timeframe":TIMEFRAME,"side":SIDE,"rr_fixed":REFERENCE_RR,
        "anchors":ANCHORS,"candidate_configurations":EXPECTED_CANDIDATES,"frozen_end_exclusive":iso(DATA_END),
        "cost_cases":[{"label":a,"ticks":b,"pips":c,"purpose":d} for a,b,c,d in COST_CASES],
        "routes":["/audjpy-h1-long-pass3/start","/audjpy-h1-long-pass3/status","/audjpy-h1-long-pass3/results"],
    })


@app.route("/audjpy-h1-long-pass3/start")
def start_route():
    return jsonify({"started_now":launch_once(),"state":STATUS["state"],"orders_supported":False})


@app.route("/audjpy-h1-long-pass3/status")
def status_route():
    with STATUS_LOCK:
        return jsonify(dict(STATUS))


@app.route("/audjpy-h1-long-pass3/results")
def results_route():
    if not BUNDLE.exists():
        return jsonify({"status":"not_ready","state":STATUS["state"],"message":STATUS["message"]}),404
    return send_file(str(BUNDLE.resolve()),as_attachment=True,download_name=BUNDLE.name)


if __name__ == "__main__":
    launch_once()
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")),debug=False)
