"""
M15 FINAL LOCKED 10-STRATEGY PORTFOLIO ANALYSIS
================================================

READ-ONLY research service. NEVER sends orders.

Purpose
-------
Rebuild and merge the FINAL full-history-validated M15 strategies for:
    EUR/USD  LONG + SHORT
    GBP/USD  LONG + SHORT
    USD/JPY  LONG + SHORT
    USD/CAD  LONG + SHORT
    EUR/GBP  LONG + SHORT

The strategy rules below are frozen. This file performs NO optimization.
It downloads OANDA midpoint history, recreates each final locked strategy,
then exports one ZIP containing:

    - exact strategy manifest
    - data coverage
    - per-strategy summary and period stats
    - per-pair summary
    - full portfolio summary
    - last 1/2/3/5/10Y stats
    - calendar-year returns / trade counts
    - rolling 12/24/36M returns and trade counts
    - rolling-window summary / best and worst windows
    - trade-frequency stats
    - 0.5/1.0/1.5/2.0-pip cost stress
    - monthly strategy returns + correlation matrix
    - trade overlap / concurrency diagnostics
    - all 1-pip baseline trades
    - parity / validation-reference diagnostics

Historical conventions
----------------------
- OANDA midpoint candles, M15 signal timestamp = candle OPEN.
- ATR14 = Wilder/RMA, SMA seeded.
- Reference entry = signal close.
- Historical adverse fill: LONG close + cost; SHORT close - cost.
- Stop buffer = 10 ticks.
- Target is based on REFERENCE signal-close risk.
- Actual R is measured from the adverse fill.
- Exit testing starts on the NEXT M15 candle.
- Signal on the exact exit candle is eligible.
- Same-bar LONG tie: if high is closer to candle open => TARGET first;
  otherwise STOP first.
- Same-bar SHORT tie: if high is closer to candle open => STOP first;
  otherwise TARGET first.
- HTF state becomes available only at next ACTUAL HTF candle open;
  lookup = bisect_right(completion_times, signal_time) - 1.
- OANDA dailyAlignment=17, alignmentTimezone=America/New_York.
- Each strategy enforces pyramiding=0 internally.
- DIFFERENT strategy IDs are allowed to overlap/concurrently hold positions.
  This includes opposite directions on the same pair; the analysis reports
  those overlaps rather than suppressing them.

Final full-history locks used
-----------------------------
EUR/USD LONG:
    exact bullish engulf; BR>=1.20; body>=1.00 ATR; S165/D0.10;
    exclude Tuesday (America/New_York); NO hour exclusion; RR3.75.
EUR/USD SHORT:
    exact bearish engulf; BR>=1.10; body>=1.30 ATR; range>=1.70 ATR;
    S60/D0.225; NY 02:00-03:59; no weekday exclusion; RR3.50.
GBP/USD LONG:
    failed breakdown/reclaim of prior165 low; body>=1.00 ATR;
    close location>=0.75; NY04:00-07:59; RR4.25.
GBP/USD SHORT:
    exact bearish engulf; BR>=1.30; body>=1.10 ATR; range>=1.60 ATR;
    S100/D0.075; no time/day filters; RR3.00.
USD/JPY LONG:
    SWEEP30_RR4.00; bullish; NO BR filter; body>=1.25 ATR;
    low<prior30 low; close>previous M15 high; lower wick/body>=0.25;
    prior4h momentum<=-1.75 ATR; previous completed H1 ATR14/mean50>=0.80;
    RR4.00.
USD/JPY SHORT:
    core compression-breakdown + daily EMA200, RR4.75;
    complement prior165 high sweep/rejection + H4 EMA100, RR3.00;
    same-candle core priority; pyramiding0 on union stream.
USD/CAD LONG:
    core bullish engulf S165/D0.175 + daily EMA50>EMA200, RR5.00;
    complement compression-breakout prior5 high + H4 close>EMA100, RR5.25;
    same-candle core priority; pyramiding0 on union stream.
USD/CAD SHORT:
    core bearish outside S120/D0.05 + H4 close<EMA100, RR4.00;
    complement prior20 high sweep rejection + H1 close<EMA100, RR3.50;
    historical half-open complement/core overlap rejection.
EUR/GBP LONG:
    core sweep60 displacement / body1.20 / wick0.25 / prior4h<=-1.25 /
    NY01-03 / RR2.75;
    complement exact bullish engulf BR1.20/body1.10/S165/D0.20 /
    Europe/London03-07 / RR2.25;
    historical half-open complement/core overlap rejection.
EUR/GBP SHORT:
    core high-sweep120 / body1.25 / closeLoc<=.20 / wick>=.40 /
    exclude Wednesday Europe/London / RR4.50;
    complement exact bearish engulf BR1.60/body1.00/S100/D0.15 /
    previous completed H1 close<EMA100 / RR2.50;
    historical half-open complement/core overlap rejection.
"""

import os
import csv
import json
import math
import time
import bisect
import zipfile
import threading
import gc
from collections import deque, defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median
from zoneinfo import ZoneInfo

import numpy as np
import requests
from flask import Flask, jsonify, send_file


# ============================================================
# SERVICE / GLOBAL SETTINGS
# ============================================================

app = Flask(__name__)

TOKEN = os.getenv("OANDA_TOKEN")
BASE = os.getenv("OANDA_API_URL", "https://api-fxtrade.oanda.com")

START = datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc)
HTF_WARMUP = START - timedelta(days=1000)
NOW = datetime.now(timezone.utc).replace(second=0, microsecond=0)

NY = ZoneInfo("America/New_York")
LONDON = ZoneInfo("Europe/London")

PAIRS = ["EUR_USD", "GBP_USD", "USD_JPY", "USD_CAD", "EUR_GBP"]
COSTS = [0.5, 1.0, 1.5, 2.0]
BASELINE_COST = 1.0

PAIR_META = {
    "EUR_USD": {"tick": 0.00001, "pip": 0.0001},
    "GBP_USD": {"tick": 0.00001, "pip": 0.0001},
    "USD_JPY": {"tick": 0.001, "pip": 0.01},
    "USD_CAD": {"tick": 0.00001, "pip": 0.0001},
    "EUR_GBP": {"tick": 0.00001, "pip": 0.0001},
}

# Only required HTFs are fetched per pair.
PAIR_HTFS = {
    "EUR_USD": [],
    "GBP_USD": [],
    "USD_JPY": ["H1", "H4", "D"],
    "USD_CAD": ["H1", "H4", "D"],
    "EUR_GBP": ["H1"],
}

# Safe OANDA chunk sizes; deliberately kept below 5000-candle limits.
CHUNK_DAYS = {
    "M15": 35,
    "H1": 180,
    "H4": 700,
    "D": 3000,
}

# Diagnostic reference counts from the final validation runs.
# A current run can legitimately be ABOVE these if new signals have appeared.
REFERENCE_COUNTS = {
    "EUR_USD_M15_LONG": 87,
    "EUR_USD_M15_SHORT": 73,
    "GBP_USD_M15_LONG": 84,
    "GBP_USD_M15_SHORT": 74,
    "USD_JPY_M15_LONG": 78,
    "USD_JPY_M15_SHORT": 131,
    "USD_CAD_M15_LONG": 149,
    "USD_CAD_M15_SHORT": 182,
    "EUR_GBP_M15_LONG": 92,
    "EUR_GBP_M15_SHORT": 71,
}

OUT = {
    "manifest": "m15_final_portfolio_manifest.csv",
    "coverage": "m15_final_portfolio_coverage.csv",
    "parity": "m15_final_portfolio_parity.csv",
    "strategy_summary": "m15_final_portfolio_strategy_summary.csv",
    "strategy_periods": "m15_final_portfolio_strategy_periods.csv",
    "strategy_cost": "m15_final_portfolio_strategy_cost_stress.csv",
    "strategy_calendar": "m15_final_portfolio_strategy_calendar_years.csv",
    "strategy_rolling": "m15_final_portfolio_strategy_rolling.csv",
    "strategy_rolling_summary": "m15_final_portfolio_strategy_rolling_summary.csv",
    "pair_summary": "m15_final_portfolio_pair_summary.csv",
    "portfolio_summary": "m15_final_portfolio_summary.csv",
    "portfolio_periods": "m15_final_portfolio_periods.csv",
    "portfolio_cost": "m15_final_portfolio_cost_stress.csv",
    "portfolio_calendar": "m15_final_portfolio_calendar_years.csv",
    "portfolio_rolling": "m15_final_portfolio_rolling.csv",
    "portfolio_rolling_summary": "m15_final_portfolio_rolling_summary.csv",
    "frequency": "m15_final_portfolio_trade_frequency.csv",
    "monthly": "m15_final_portfolio_monthly_by_strategy.csv",
    "correlation": "m15_final_portfolio_monthly_correlation.csv",
    "overlap": "m15_final_portfolio_overlap.csv",
    "concurrency": "m15_final_portfolio_concurrency.csv",
    "trades": "m15_final_portfolio_trades.csv",
    "notes": "m15_final_portfolio_notes.csv",
}
BUNDLE = "M15_FINAL_LOCKED_PORTFOLIO_ANALYSIS_RESULTS.zip"

STATUS = {
    "state": "not_started",
    "message": "M15 final locked portfolio analysis not started",
    "orders_supported": False,
    "trading_enabled": False,
    "progress": 0,
}


# ============================================================
# FROZEN MANIFEST — SOURCE OF TRUTH INSIDE THIS RUNNER
# ============================================================

MANIFEST = [
    {
        "strategy_id": "EUR_USD_M15_LONG", "pair": "EUR_USD", "side": "BUY",
        "architecture": "single",
        "rules": "Exact bull engulf; BR>=1.20; body>=1.00ATR; prior165-low distance<=0.10ATR; exclude Tue NY; all hours; RR3.75",
        "source": "full-history re-examination final lock (supersedes older BR1.35/body0.75/NY07-exclusion file)",
    },
    {
        "strategy_id": "EUR_USD_M15_SHORT", "pair": "EUR_USD", "side": "SELL",
        "architecture": "single",
        "rules": "Exact bear engulf; BR>=1.10; body>=1.30ATR; range>=1.70ATR; prior60-high distance<=0.225ATR; NY02-03; RR3.50",
        "source": "final full-history confirmation / locked file",
    },
    {
        "strategy_id": "GBP_USD_M15_LONG", "pair": "GBP_USD", "side": "BUY",
        "architecture": "single",
        "rules": "Failed breakdown+reclaim prior165 low; body>=1.00ATR; closeLoc>=0.75; NY04-07; RR4.25",
        "source": "final full-history locked file",
    },
    {
        "strategy_id": "GBP_USD_M15_SHORT", "pair": "GBP_USD", "side": "SELL",
        "architecture": "single",
        "rules": "Exact bear engulf; BR>=1.30; body>=1.10ATR; range>=1.60ATR; prior100-high distance<=0.075ATR; RR3.00",
        "source": "GRID_0240 final full-history locked file",
    },
    {
        "strategy_id": "USD_JPY_M15_LONG", "pair": "USD_JPY", "side": "BUY",
        "architecture": "single",
        "rules": "SWEEP30; bull; no BR; body>=1.25ATR; close>prev high; lowerWick/body>=0.25; prior4h<=-1.75ATR; completed H1 ATR/mean50>=0.80; RR4.00",
        "source": "SWEEP30_RR4.00 final full-history locked file (supersedes older any-20/40/60/100 version)",
    },
    {
        "strategy_id": "USD_JPY_M15_SHORT", "pair": "USD_JPY", "side": "SELL",
        "architecture": "priority_union",
        "rules": "Core compression<=0.80/body1.25/range1.60/break40/daily<EMA200/RR4.75 + complement sweep165/body1.00/closeLoc<=.20/H4<EMA100/RR3.00",
        "source": "final full-history two-trigger locked file",
    },
    {
        "strategy_id": "USD_CAD_M15_LONG", "pair": "USD_CAD", "side": "BUY",
        "architecture": "priority_union",
        "rules": "Core bull engulf BR1.70/body1.25/range1.50/S165D.175/daily EMA50>200/RR5 + complement compression.70/body1.0/range1.5/break5/H4>EMA100/RR5.25",
        "source": "final full-history two-trigger locked file",
    },
    {
        "strategy_id": "USD_CAD_M15_SHORT", "pair": "USD_CAD", "side": "SELL",
        "architecture": "historical_overlay",
        "rules": "Core bear outside/body.75/closeLoc.35/S120D.05/H4<EMA100/RR4 + complement sweep20/body1.25/closeLoc.40/wick.25/H1<EMA100/RR3.5",
        "source": "final full-history two-trigger locked file",
    },
    {
        "strategy_id": "EUR_GBP_M15_LONG", "pair": "EUR_GBP", "side": "BUY",
        "architecture": "historical_overlay",
        "rules": "Core sweep60/body1.20/wick.25/prior4h<=-1.25/NY01-03/RR2.75 + complement bull engulf BR1.20/body1.10/S165D.20/London03-07/RR2.25",
        "source": "final full-history two-trigger locked file",
    },
    {
        "strategy_id": "EUR_GBP_M15_SHORT", "pair": "EUR_GBP", "side": "SELL",
        "architecture": "historical_overlay",
        "rules": "Core sweep120/body1.25/closeLoc.20/wick.40/excl Wed London/RR4.5 + complement bear engulf BR1.60/body1.00/S100D.15/H1<EMA100/RR2.5",
        "source": "final full-history BR-confirmed two-trigger locked file",
    },
]


# ============================================================
# BASIC HELPERS
# ============================================================

def iso(dt):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_time(value):
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    if "." in value:
        left, right = value.split(".", 1)
        sign = None
        offset = None
        if "+" in right:
            frac, offset = right.split("+", 1); sign = "+"
        elif "-" in right:
            frac, offset = right.split("-", 1); sign = "-"
        else:
            frac = right
        frac = frac[:6].ljust(6, "0")
        value = left + "." + frac
        if sign:
            value += sign + offset
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def write_csv(path, rows):
    rows = list(rows)
    if not rows:
        with open(path, "w", encoding="utf-8") as f:
            f.write("")
        return
    fields, seen = [], set()
    for row in rows:
        for k in row:
            if k not in seen:
                seen.add(k); fields.append(k)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader(); w.writerows(rows)


def safe_median(vals):
    vals = [x for x in vals if x is not None and math.isfinite(x)]
    return median(vals) if vals else 0.0


def month_floor(dt):
    return datetime(dt.year, dt.month, 1, tzinfo=timezone.utc)


def add_months(dt, months):
    z = dt.year * 12 + dt.month - 1 + months
    return datetime(z // 12, z % 12 + 1, 1, tzinfo=timezone.utc)


def pct(num, den):
    return 100.0 * num / den if den else 0.0


# ============================================================
# OANDA HISTORY
# ============================================================

def headers():
    if not TOKEN:
        raise RuntimeError("OANDA_TOKEN is not configured")
    return {"Authorization": "Bearer " + TOKEN.strip()}


def fetch_chunk(pair, gran, start, end):
    params = {
        "price": "M", "granularity": gran,
        "from": iso(start), "to": iso(end),
        "smooth": "false", "includeFirst": "true",
    }
    if gran == "D":
        params["dailyAlignment"] = 17
        params["alignmentTimezone"] = "America/New_York"
    r = requests.get(
        f"{BASE}/v3/instruments/{pair}/candles",
        headers=headers(), params=params, timeout=45,
    )
    if r.status_code in (400, 404):
        # Older unavailable-history chunks can legitimately be rejected.
        # Coverage guards below prevent this from silently truncating M15.
        return []
    r.raise_for_status()
    out = []
    for c in r.json().get("candles", []):
        if not c.get("complete", False):
            continue
        m = c.get("mid") or {}
        if not all(k in m for k in ("o", "h", "l", "c")):
            continue
        out.append({
            "time": parse_time(c["time"]),
            "open": float(m["o"]), "high": float(m["h"]),
            "low": float(m["l"]), "close": float(m["c"]),
        })
    return out


def fetch_history(pair, gran, start, end):
    step = timedelta(days=CHUNK_DAYS[gran])
    by_t = {}
    cursor = start
    empty = 0
    while cursor < end:
        nxt = min(cursor + step, end)
        chunk = fetch_chunk(pair, gran, cursor, nxt)
        if not chunk:
            empty += 1
        for c in chunk:
            by_t[c["time"]] = c
        cursor = nxt
        time.sleep(0.015)
    out = [by_t[k] for k in sorted(by_t)]
    return out, empty


# ============================================================
# INDICATORS / FEATURE CACHE
# ============================================================

def rma(values, n):
    x = np.asarray(values, dtype=float)
    out = np.full(len(x), np.nan)
    if len(x) < n:
        return out
    out[n - 1] = np.mean(x[:n])
    for i in range(n, len(x)):
        out[i] = (out[i - 1] * (n - 1) + x[i]) / n
    return out


def sma(values, n):
    x = np.asarray(values, dtype=float)
    out = np.full(len(x), np.nan)
    q = deque(); s = 0.0; invalid = 0
    for i, v in enumerate(x):
        q.append(v)
        if math.isfinite(v): s += v
        else: invalid += 1
        if len(q) > n:
            old = q.popleft()
            if math.isfinite(old): s -= old
            else: invalid -= 1
        if len(q) == n and invalid == 0:
            out[i] = s / n
    return out


def ema(values, n):
    x = np.asarray(values, dtype=float)
    out = np.full(len(x), np.nan)
    if len(x) < n:
        return out
    seed = x[:n]
    if not np.all(np.isfinite(seed)):
        return out
    out[n - 1] = np.mean(seed)
    a = 2.0 / (n + 1.0)
    for i in range(n, len(x)):
        if math.isfinite(x[i]):
            out[i] = a * x[i] + (1.0 - a) * out[i - 1]
    return out


def atr14(candles):
    h = np.array([c["high"] for c in candles], dtype=float)
    l = np.array([c["low"] for c in candles], dtype=float)
    c = np.array([c["close"] for c in candles], dtype=float)
    tr = np.full(len(c), np.nan)
    if len(c): tr[0] = h[0] - l[0]
    if len(c) > 1:
        tr[1:] = np.maximum.reduce([
            h[1:] - l[1:], np.abs(h[1:] - c[:-1]), np.abs(l[1:] - c[:-1])
        ])
    return rma(tr, 14)


def prev_extreme(values, lookback, want_max):
    x = np.asarray(values, dtype=float)
    out = np.full(len(x), np.nan)
    dq = deque()
    for i in range(len(x)):
        # At i, deque represents indices in [i-lookback, i-1].
        min_idx = i - lookback
        while dq and dq[0] < min_idx:
            dq.popleft()
        if i >= lookback and dq:
            out[i] = x[dq[0]]
        # add current only AFTER emitting so current candle is excluded
        if want_max:
            while dq and x[dq[-1]] <= x[i]: dq.pop()
        else:
            while dq and x[dq[-1]] >= x[i]: dq.pop()
        dq.append(i)
    return out


def htf_state(candles):
    close = np.array([c["close"] for c in candles], dtype=float)
    a14 = atr14(candles)
    am50 = sma(a14, 50)
    e50 = ema(close, 50)
    e100 = ema(close, 100)
    e200 = ema(close, 200)
    states = []
    for i, c in enumerate(candles):
        complete_at = candles[i + 1]["time"] if i + 1 < len(candles) else None
        if complete_at is None:
            continue
        states.append({
            "complete_at": complete_at,
            "close": close[i],
            "ema50": e50[i],
            "ema100": e100[i],
            "ema200": e200[i],
            "atr14": a14[i],
            "atr_ratio50": (a14[i] / am50[i]) if math.isfinite(a14[i]) and math.isfinite(am50[i]) and am50[i] != 0 else np.nan,
        })
    return states


def align_htf(m15_times, states):
    ct = [s["complete_at"] for s in states]
    fields = ["close", "ema50", "ema100", "ema200", "atr14", "atr_ratio50"]
    out = {k: np.full(len(m15_times), np.nan) for k in fields}
    for i, t in enumerate(m15_times):
        j = bisect.bisect_right(ct, t) - 1
        if j >= 0:
            s = states[j]
            for k in fields:
                out[k][i] = s[k]
    return out


def build_features(pair, m15, aligned):
    n = len(m15)
    o = np.array([x["open"] for x in m15], dtype=float)
    h = np.array([x["high"] for x in m15], dtype=float)
    l = np.array([x["low"] for x in m15], dtype=float)
    c = np.array([x["close"] for x in m15], dtype=float)
    a = atr14(m15)
    body = c - o
    absbody = np.abs(body)
    rng = h - l
    bull_body = np.maximum(body, 0.0)
    bear_body = np.maximum(-body, 0.0)
    bull_atr = np.divide(bull_body, a, out=np.full(n, np.nan), where=np.isfinite(a) & (a > 0))
    bear_atr = np.divide(bear_body, a, out=np.full(n, np.nan), where=np.isfinite(a) & (a > 0))
    range_atr = np.divide(rng, a, out=np.full(n, np.nan), where=np.isfinite(a) & (a > 0))
    close_loc = np.divide(c - l, rng, out=np.full(n, np.nan), where=rng > 0)
    lower_wick = np.minimum(o, c) - l
    upper_wick = h - np.maximum(o, c)
    lwb = np.divide(lower_wick, bull_body, out=np.full(n, np.nan), where=bull_body > 0)
    uwb = np.divide(upper_wick, bear_body, out=np.full(n, np.nan), where=bear_body > 0)

    exact_bull = np.zeros(n, dtype=bool)
    exact_bear = np.zeros(n, dtype=bool)
    bull_br = np.full(n, np.nan); bear_br = np.full(n, np.nan)
    if n > 1:
        prev_abs = absbody[:-1]
        exact_bull[1:] = (c[:-1] < o[:-1]) & (c[1:] > o[1:]) & (o[1:] <= c[:-1]) & (c[1:] >= o[:-1])
        exact_bear[1:] = (c[:-1] > o[:-1]) & (c[1:] < o[1:]) & (o[1:] >= c[:-1]) & (c[1:] <= o[:-1])
        bull_br[1:] = np.divide(bull_body[1:], prev_abs, out=np.full(n-1, np.nan), where=prev_abs > 0)
        bear_br[1:] = np.divide(bear_body[1:], prev_abs, out=np.full(n-1, np.nan), where=prev_abs > 0)

    lookbacks = {
        "EUR_USD": {"low": [165], "high": [60]},
        "GBP_USD": {"low": [165], "high": [100]},
        "USD_JPY": {"low": [30, 40], "high": [165]},
        "USD_CAD": {"low": [165], "high": [5, 20, 120]},
        "EUR_GBP": {"low": [60, 165], "high": [100, 120]},
    }[pair]
    lows = {lb: prev_extreme(l, lb, False) for lb in lookbacks["low"]}
    highs = {lb: prev_extreme(h, lb, True) for lb in lookbacks["high"]}

    prev_a = np.r_[np.nan, a[:-1]]
    a_mean20 = sma(a, 20)
    prev_a_mean20 = np.r_[np.nan, a_mean20[:-1]]
    compression = np.divide(prev_a, prev_a_mean20, out=np.full(n, np.nan), where=np.isfinite(prev_a) & np.isfinite(prev_a_mean20) & (prev_a_mean20 != 0))

    mom4 = np.full(n, np.nan)
    if n > 17:
        mom4[17:] = np.divide(c[16:-1] - c[:-17], a[17:], out=np.full(n-17, np.nan), where=np.isfinite(a[17:]) & (a[17:] > 0))

    prev_high = np.r_[np.nan, h[:-1]]
    prev_low = np.r_[np.nan, l[:-1]]

    # Time arrays only where rules need them; inexpensive relative to backtest.
    times = [x["time"] for x in m15]
    ny_hour = np.array([t.astimezone(NY).hour for t in times], dtype=np.int16)
    ny_weekday = np.array([t.astimezone(NY).weekday() for t in times], dtype=np.int8)
    london_hour = np.array([t.astimezone(LONDON).hour for t in times], dtype=np.int16)
    london_weekday = np.array([t.astimezone(LONDON).weekday() for t in times], dtype=np.int8)

    return {
        "o": o, "h": h, "l": l, "c": c, "atr": a,
        "bull": body > 0, "bear": body < 0,
        "bull_atr": bull_atr, "bear_atr": bear_atr, "range_atr": range_atr,
        "close_loc": close_loc, "lwb": lwb, "uwb": uwb,
        "exact_bull": exact_bull, "exact_bear": exact_bear,
        "bull_br": bull_br, "bear_br": bear_br,
        "low": lows, "high": highs,
        "compression": compression, "mom4": mom4,
        "prev_high": prev_high, "prev_low": prev_low,
        "ny_hour": ny_hour, "ny_weekday": ny_weekday,
        "london_hour": london_hour, "london_weekday": london_weekday,
        "htf": aligned,
    }


# ============================================================
# FROZEN SIGNAL RULES
# ============================================================

def idx_from_mask(mask):
    return np.flatnonzero(np.asarray(mask, dtype=bool)).tolist()


def signal_sets(pair, f):
    A = f["atr"]; C=f["c"]; H=f["h"]; L=f["l"]
    valid = np.isfinite(A) & (A > 0)
    htf = f["htf"]

    if pair == "EUR_USD":
        d165 = np.abs(L - f["low"][165]) / A
        long_m = valid & f["exact_bull"] & (f["bull_br"] >= 1.20) & (f["bull_atr"] >= 1.00) & (d165 <= 0.10) & (f["ny_weekday"] != 1)
        d60 = np.abs(H - f["high"][60]) / A
        short_m = valid & f["exact_bear"] & (f["bear_br"] >= 1.10) & (f["bear_atr"] >= 1.30) & (f["range_atr"] >= 1.70) & (d60 <= 0.225) & np.isin(f["ny_hour"], [2,3])
        return {
            "EUR_USD_M15_LONG": {"side":"BUY","mode":"single","signals":[(i,"CORE",3.75) for i in idx_from_mask(long_m)]},
            "EUR_USD_M15_SHORT": {"side":"SELL","mode":"single","signals":[(i,"CORE",3.50) for i in idx_from_mask(short_m)]},
        }

    if pair == "GBP_USD":
        p165=f["low"][165]
        long_m = valid & f["bull"] & (f["bull_atr"] >= 1.00) & (f["close_loc"] >= 0.75) & (L < p165) & (C > p165) & np.isin(f["ny_hour"], [4,5,6,7])
        d100=np.abs(H-f["high"][100])/A
        short_m = valid & f["exact_bear"] & (f["bear_br"] >= 1.30) & (f["bear_atr"] >= 1.10) & (f["range_atr"] >= 1.60) & (d100 <= 0.075)
        return {
            "GBP_USD_M15_LONG": {"side":"BUY","mode":"single","signals":[(i,"CORE",4.25) for i in idx_from_mask(long_m)]},
            "GBP_USD_M15_SHORT": {"side":"SELL","mode":"single","signals":[(i,"CORE",3.00) for i in idx_from_mask(short_m)]},
        }

    if pair == "USD_JPY":
        h1=htf["H1"]; h4=htf["H4"]; d=htf["D"]
        p30=f["low"][30]
        long_m = valid & f["bull"] & (f["bull_atr"] >= 1.25) & (L < p30) & (C > f["prev_high"]) & (f["lwb"] >= 0.25) & (f["mom4"] <= -1.75) & (h1["atr_ratio50"] >= 0.80)
        core = valid & f["bear"] & (f["compression"] <= 0.80) & (f["bear_atr"] >= 1.25) & (f["range_atr"] >= 1.60) & (C < f["low"][40]) & (d["close"] < d["ema200"])
        p165=f["high"][165]
        comp = valid & f["bear"] & (H > p165) & (C < p165) & (f["bear_atr"] >= 1.00) & (f["close_loc"] <= 0.20) & (h4["close"] < h4["ema100"])
        union=[]
        for i in sorted(set(idx_from_mask(core)) | set(idx_from_mask(comp))):
            if core[i]: union.append((i,"CORE_GRID_0731",4.75))
            elif comp[i]: union.append((i,"COMPLEMENT_CAND_0322",3.00))
        return {
            "USD_JPY_M15_LONG": {"side":"BUY","mode":"single","signals":[(i,"SWEEP30_RR4.00",4.00) for i in idx_from_mask(long_m)]},
            "USD_JPY_M15_SHORT": {"side":"SELL","mode":"priority_union","signals":union},
        }

    if pair == "USD_CAD":
        h1=htf["H1"]; h4=htf["H4"]; d=htf["D"]
        d165=np.abs(L-f["low"][165])/A
        core_l = valid & f["exact_bull"] & (f["bull_br"] >= 1.70) & (f["bull_atr"] >= 1.25) & (f["range_atr"] >= 1.50) & (d165 <= 0.175) & (d["ema50"] > d["ema200"])
        comp_l = valid & f["bull"] & (f["compression"] <= 0.70) & (f["bull_atr"] >= 1.00) & (f["range_atr"] >= 1.50) & (C > f["high"][5]) & (h4["close"] > h4["ema100"])
        union=[]
        for i in sorted(set(idx_from_mask(core_l)) | set(idx_from_mask(comp_l))):
            if core_l[i]: union.append((i,"CORE_EMA50_REGIME_BR170",5.00))
            elif comp_l[i]: union.append((i,"COMPLEMENT_H4_COMPRESSION_BREAKOUT",5.25))

        d120=np.abs(H-f["high"][120])/A
        core_s = valid & f["bear"] & (H > f["prev_high"]) & (L < f["prev_low"]) & (f["bear_atr"] >= 0.75) & (f["close_loc"] <= 0.35) & (d120 <= 0.05) & (h4["close"] < h4["ema100"])
        p20=f["high"][20]
        comp_s = valid & f["bear"] & (H > p20) & (C < p20) & (f["bear_atr"] >= 1.25) & (f["close_loc"] <= 0.40) & (f["uwb"] >= 0.25) & (h1["close"] < h1["ema100"])
        return {
            "USD_CAD_M15_LONG": {"side":"BUY","mode":"priority_union","signals":union},
            "USD_CAD_M15_SHORT": {"side":"SELL","mode":"overlay","core_signals":[(i,"CORE_OUTSIDE_H4EMA100",4.00) for i in idx_from_mask(core_s)],"comp_signals":[(i,"COMPLEMENT_HIGH_SWEEP_H1EMA100",3.50) for i in idx_from_mask(comp_s)]},
        }

    if pair == "EUR_GBP":
        h1=htf["H1"]
        p60=f["low"][60]
        core_l = valid & f["bull"] & (f["bull_atr"] >= 1.20) & (L < p60) & (C > f["prev_high"]) & (f["lwb"] >= 0.25) & (f["mom4"] <= -1.25) & np.isin(f["ny_hour"],[1,2,3])
        d165=np.abs(L-f["low"][165])/A
        comp_l = valid & f["exact_bull"] & (f["bull_br"] >= 1.20) & (f["bull_atr"] >= 1.10) & (d165 <= 0.20) & np.isin(f["london_hour"],[3,4,5,6,7])

        p120=f["high"][120]
        core_s = valid & f["bear"] & (H > p120) & (C < p120) & (f["bear_atr"] >= 1.25) & (f["close_loc"] <= 0.20) & (f["uwb"] >= 0.40) & (f["london_weekday"] != 2)
        d100=np.abs(H-f["high"][100])/A
        comp_s = valid & f["exact_bear"] & (f["bear_br"] >= 1.60) & (f["bear_atr"] >= 1.00) & (d100 <= 0.15) & (h1["close"] < h1["ema100"])
        return {
            "EUR_GBP_M15_LONG": {"side":"BUY","mode":"overlay","core_signals":[(i,"CORE_SWEEP_DISPLACEMENT",2.75) for i in idx_from_mask(core_l)],"comp_signals":[(i,"COMPLEMENT_LONDON_ENGULF_STRUCTURE",2.25) for i in idx_from_mask(comp_l)]},
            "EUR_GBP_M15_SHORT": {"side":"SELL","mode":"overlay","core_signals":[(i,"CORE_HIGH_SWEEP_REJECTION",4.50) for i in idx_from_mask(core_s)],"comp_signals":[(i,"COMPLEMENT_BEAR_ENGULF_H1EMA100",2.50) for i in idx_from_mask(comp_s)]},
        }

    raise KeyError(pair)


# ============================================================
# BACKTEST ENGINE
# ============================================================

def one_outcome(pair, strategy_id, trigger, side, m15, i, rr, cost_pips):
    meta=PAIR_META[pair]; tick=meta["tick"]; pip=meta["pip"]
    sig=m15[i]; ref=sig["close"]
    if side == "BUY":
        stop=sig["low"] - 10*tick
        ref_risk=ref-stop
        if ref_risk <= 0: return None
        target=ref + rr*ref_risk
        fill=ref + cost_pips*pip
        actual_risk=fill-stop
    else:
        stop=sig["high"] + 10*tick
        ref_risk=stop-ref
        if ref_risk <= 0: return None
        target=ref - rr*ref_risk
        fill=ref - cost_pips*pip
        actual_risk=stop-fill
    if actual_risk <= 0: return None

    for j in range(i+1, len(m15)):
        x=m15[j]
        hit_stop = x["low"] <= stop if side=="BUY" else x["high"] >= stop
        hit_target = x["high"] >= target if side=="BUY" else x["low"] <= target
        if not hit_stop and not hit_target:
            continue
        if hit_stop and hit_target:
            high_closer = abs(x["high"] - x["open"]) < abs(x["open"] - x["low"])
            if side=="BUY":
                result="TARGET" if high_closer else "STOP"
            else:
                result="STOP" if high_closer else "TARGET"
        else:
            result="STOP" if hit_stop else "TARGET"
        r = -1.0 if result=="STOP" else ((target-fill)/actual_risk if side=="BUY" else (fill-target)/actual_risk)
        return {
            "pair":pair, "strategy_id":strategy_id, "trigger":trigger, "side":side,
            "signal_index":i, "exit_index":j,
            "signal_time":sig["time"], "exit_time":x["time"],
            "rr":rr, "reference_entry":ref, "historical_fill":fill,
            "stop":stop, "target":target, "result":result, "r":float(r),
            "cost_pips":cost_pips,
        }
    return None


def run_stream(pair, strategy_id, side, m15, signals, cost_pips):
    signals=sorted(signals, key=lambda z:z[0])
    idxs=[z[0] for z in signals]
    out=[]; p=0
    while p < len(signals):
        i, trig, rr = signals[p]
        tr=one_outcome(pair,strategy_id,trig,side,m15,i,rr,cost_pips)
        if tr is None:
            p += 1; continue
        out.append(tr)
        # exact exit-candle signal eligible
        p=bisect.bisect_left(idxs, tr["exit_index"], lo=p+1)
    return out


def overlay(core, comp):
    core=sorted(core,key=lambda x:x["signal_index"])
    comp=sorted(comp,key=lambda x:x["signal_index"])
    accepted=[]; rejected=[]; p=0
    for tr in comp:
        s,e=tr["signal_index"],tr["exit_index"]
        while p<len(core) and core[p]["exit_index"] <= s:
            p += 1
        overlaps = p<len(core) and core[p]["signal_index"] < e and core[p]["exit_index"] > s
        (rejected if overlaps else accepted).append(tr)
    return sorted(core+accepted,key=lambda x:x["signal_index"]), accepted, rejected


def evaluate_strategy(pair, strategy_id, spec, m15, cost_pips):
    if spec["mode"] in ("single","priority_union"):
        trades=run_stream(pair,strategy_id,spec["side"],m15,spec["signals"],cost_pips)
        return trades,{"accepted_complement":0,"rejected_overlap":0}
    core=run_stream(pair,strategy_id,spec["side"],m15,spec["core_signals"],cost_pips)
    comp=run_stream(pair,strategy_id,spec["side"],m15,spec["comp_signals"],cost_pips)
    combined,accepted,rejected=overlay(core,comp)
    return combined,{"accepted_complement":len(accepted),"rejected_overlap":len(rejected),"core_trades":len(core),"candidate_complement":len(comp)}


# ============================================================
# METRICS
# ============================================================

def calc_stats(trades, order="signal"):
    if not trades:
        return {"trades":0,"winners":0,"losers":0,"win_rate_pct":0.0,"profit_factor":0.0,"total_r":0.0,"expectancy_r":0.0,"max_drawdown_r":0.0,"longest_loss_streak":0}
    key="signal_time" if order=="signal" else "exit_time"
    ts=sorted(trades,key=lambda x:(x[key],x["strategy_id"]))
    rs=[x["r"] for x in ts]
    pos=sum(x for x in rs if x>0); neg=-sum(x for x in rs if x<0)
    pf=pos/neg if neg>0 else (999.0 if pos>0 else 0.0)
    eq=0.0; peak=0.0; dd=0.0; streak=0; maxst=0
    for r in rs:
        eq += r; peak=max(peak,eq); dd=min(dd,eq-peak)
        if r<0: streak+=1; maxst=max(maxst,streak)
        else: streak=0
    w=sum(r>0 for r in rs); l=sum(r<0 for r in rs)
    return {
        "trades":len(rs),"winners":w,"losers":l,"win_rate_pct":pct(w,len(rs)),
        "profit_factor":pf,"total_r":sum(rs),"expectancy_r":sum(rs)/len(rs),
        "max_drawdown_r":dd,"longest_loss_streak":maxst,
    }


def subset(trades,start=None,end=None):
    return [t for t in trades if (start is None or t["signal_time"]>=start) and (end is None or t["signal_time"]<end)]


def stats_row(scope,label,trades,start=None,end=None):
    s=calc_stats(subset(trades,start,end))
    return {"scope":scope,"period":label,"start_utc":iso(start) if start else "","end_utc":iso(end) if end else "",**s,
            "simple_return_pct_at_0_25pct_risk":s["total_r"]*0.25,
            "simple_return_pct_at_0_50pct_risk":s["total_r"]*0.50,
            "simple_return_pct_at_0_75pct_risk":s["total_r"]*0.75,
            "simple_return_pct_at_1pct_risk":s["total_r"]*1.00}


def rolling_rows(scope,trades,months,data_start,end_complete_month):
    rows=[]
    start_month=month_floor(data_start)
    cur=start_month
    while add_months(cur,months) <= end_complete_month:
        end=add_months(cur,months)
        s=calc_stats(subset(trades,cur,end))
        rows.append({"scope":scope,"months":months,"start_utc":iso(cur),"end_utc":iso(end),**s,"simple_return_pct_at_1pct_risk":s["total_r"]})
        cur=add_months(cur,1)
    return rows


def rolling_summary(rows):
    grouped=defaultdict(list)
    for r in rows: grouped[(r["scope"],r["months"])].append(r)
    out=[]
    for (scope,m),g in grouped.items():
        active=[x for x in g if x["trades"]>0]
        positive=[x for x in active if x["total_r"]>0]
        pfs=[x["profit_factor"] for x in active if x["profit_factor"]<900]
        best=max(g,key=lambda x:x["total_r"]) if g else None
        worst=min(g,key=lambda x:x["total_r"]) if g else None
        out.append({
            "scope":scope,"months":m,"windows":len(g),"active_windows":len(active),"zero_trade_windows":len(g)-len(active),
            "positive_windows":sum(x["total_r"]>0 for x in g),"positive_active_windows":len(positive),
            "positive_all_windows_pct":pct(sum(x["total_r"]>0 for x in g),len(g)),
            "positive_active_windows_pct":pct(len(positive),len(active)),
            "median_r_active":safe_median(x["total_r"] for x in active),"median_pf_active":safe_median(pfs),
            "median_trades_active":safe_median(x["trades"] for x in active),
            "worst_r":worst["total_r"] if worst else 0,"worst_start_utc":worst["start_utc"] if worst else "",
            "best_r":best["total_r"] if best else 0,"best_start_utc":best["start_utc"] if best else "",
        })
    return out


def calendar_rows(scope,trades,start_year,end_year):
    out=[]
    for y in range(start_year,end_year+1):
        a=datetime(y,1,1,tzinfo=timezone.utc); b=datetime(y+1,1,1,tzinfo=timezone.utc)
        s=calc_stats(subset(trades,a,b))
        out.append({"scope":scope,"year":y,"complete_year":y<NOW.year,**s,"simple_return_pct_at_1pct_risk":s["total_r"]})
    return out


def frequency_row(scope,trades,start,end,label):
    t=subset(trades,start,end)
    days=max((end-start).total_seconds()/86400.0,1e-9)
    times=sorted(x["signal_time"] for x in t)
    gaps=[(b-a).total_seconds()/86400.0 for a,b in zip(times,times[1:])]
    return {
        "scope":scope,"period":label,"trades":len(t),"days":days,
        "trades_per_week":len(t)/(days/7.0),"trades_per_30d":len(t)/(days/30.0),
        "median_days_between_signals":safe_median(gaps),
        "max_days_between_signals":max(gaps) if gaps else 0.0,
    }


# ============================================================
# PORTFOLIO OVERLAP / MONTHLY
# ============================================================

def intervals_overlap(a,b):
    return a["signal_time"] < b["exit_time"] and b["signal_time"] < a["exit_time"]


def overlap_rows(strategy_trades):
    ids=sorted(strategy_trades)
    out=[]
    for i,a in enumerate(ids):
        for b in ids[i+1:]:
            ta=strategy_trades[a]; tb=strategy_trades[b]
            count_a=sum(any(intervals_overlap(x,y) for y in tb) for x in ta)
            count_b=sum(any(intervals_overlap(y,x) for x in ta) for y in tb)
            same_pair = (ta[0]["pair"] if ta else a.split("_M15")[0]) == (tb[0]["pair"] if tb else b.split("_M15")[0])
            out.append({"strategy_a":a,"strategy_b":b,"a_trades_overlapping_b":count_a,"b_trades_overlapping_a":count_b,"same_pair":same_pair})
    return out


def concurrency_rows(all_trades):
    events=[]
    for t in all_trades:
        events.append((t["signal_time"],1,t["strategy_id"]))
        events.append((t["exit_time"],-1,t["strategy_id"]))
    # End events before starts at same timestamp: exact exit-candle new signal eligible.
    events.sort(key=lambda z:(z[0],z[1]))
    active=0; max_active=0; hist=defaultdict(int); last=None
    for ts,delta,sid in events:
        if last is not None and ts>last:
            hist[active] += (ts-last).total_seconds()
        active += delta; max_active=max(max_active,active); last=ts
    total=sum(hist.values())
    rows=[{"metric":"max_concurrent_positions","value":max_active}]
    for k in sorted(hist):
        rows.append({"metric":f"pct_time_{k}_positions","value":pct(hist[k],total)})
    # same-timestamp signals
    clusters=defaultdict(int)
    for t in all_trades: clusters[t["signal_time"]]+=1
    rows.append({"metric":"signal_timestamps_with_2plus_trades","value":sum(v>=2 for v in clusters.values())})
    rows.append({"metric":"max_same_timestamp_signals","value":max(clusters.values()) if clusters else 0})
    return rows


def monthly_matrix(strategy_trades, start_month, end_month):
    ids=sorted(strategy_trades)
    rows=[]; cur=start_month
    while cur < end_month:
        nxt=add_months(cur,1); row={"month":cur.strftime("%Y-%m")}
        for sid in ids:
            row[sid]=sum(t["r"] for t in strategy_trades[sid] if cur<=t["signal_time"]<nxt)
        row["PORTFOLIO"]=sum(row[sid] for sid in ids)
        rows.append(row); cur=nxt
    return rows


def correlation_rows(monthly, ids):
    cols=ids+["PORTFOLIO"]
    arr={k:np.array([r[k] for r in monthly],dtype=float) for k in cols}
    out=[]
    for a in cols:
        row={"strategy":a}
        for b in cols:
            x,y=arr[a],arr[b]
            if len(x)<2 or np.std(x)==0 or np.std(y)==0: val=0.0
            else: val=float(np.corrcoef(x,y)[0,1])
            row[b]=val
        out.append(row)
    return out


# ============================================================
# MAIN RESEARCH
# ============================================================

def run_research():
    try:
        STATUS.update(state="starting",message="Starting exact final-locked M15 portfolio rebuild",progress=1)
        write_csv(OUT["manifest"],[{**r,"reference_validation_trades":REFERENCE_COUNTS.get(r["strategy_id"],"")} for r in MANIFEST])

        strategy_trades_by_cost={cost:{} for cost in COSTS}
        strategy_overlay_meta={}
        coverage=[]

        for pi,pair in enumerate(PAIRS):
            STATUS.update(state="downloading",message=f"Downloading {pair} M15 + required HTF history",progress=5+pi*11)
            m15,empty=fetch_history(pair,"M15",START,NOW)
            if len(m15)<350000:
                raise RuntimeError(f"Incomplete {pair} M15 history: only {len(m15)} candles; first={iso(m15[0]['time']) if m15 else 'NONE'}; empty_chunks={empty}")
            if m15[0]["time"] > datetime(2003,1,1,tzinfo=timezone.utc):
                raise RuntimeError(f"Incomplete {pair} early M15 coverage: first candle {iso(m15[0]['time'])}")

            aligned={}
            htf_counts={}
            for gran in PAIR_HTFS[pair]:
                hs,_=fetch_history(pair,gran,HTF_WARMUP,NOW)
                htf_counts[gran]=len(hs)
                if not hs: raise RuntimeError(f"Missing required {pair} {gran} history")
                aligned[gran]=align_htf([x["time"] for x in m15],htf_state(hs))
                del hs

            coverage.append({"pair":pair,"requested_start_utc":iso(START),"actual_first_m15_utc":iso(m15[0]["time"]),"actual_last_m15_utc":iso(m15[-1]["time"]),"m15_candles":len(m15),"h1_candles":htf_counts.get("H1",0),"h4_candles":htf_counts.get("H4",0),"daily_candles":htf_counts.get("D",0),"empty_m15_chunks":empty})

            STATUS.update(state="precomputing",message=f"Building {pair} frozen feature cache",progress=9+pi*11)
            f=build_features(pair,m15,aligned)
            specs=signal_sets(pair,f)

            for cost in COSTS:
                for sid,spec in specs.items():
                    trades,meta=evaluate_strategy(pair,sid,spec,m15,cost)
                    strategy_trades_by_cost[cost][sid]=trades
                    if cost==BASELINE_COST:
                        strategy_overlay_meta[sid]=meta

            del f, aligned, m15, specs
            gc.collect()

        write_csv(OUT["coverage"],coverage)
        baseline=strategy_trades_by_cost[BASELINE_COST]
        ids=[r["strategy_id"] for r in MANIFEST]

        # Parity/reference diagnostics: lower-than-reference is a red flag;
        # equal or greater can reflect newer signals after validation cutoff.
        parity=[]
        for sid in ids:
            actual=len(baseline[sid]); expected=REFERENCE_COUNTS[sid]
            parity.append({"strategy_id":sid,"reference_validation_trades":expected,"current_full_history_trades":actual,"status":"PASS_EQUAL" if actual==expected else ("PASS_NEWER_TRADES" if actual>expected else "CHECK_BELOW_REFERENCE"),**strategy_overlay_meta.get(sid,{})})
        write_csv(OUT["parity"],parity)
        if any(r["status"]=="CHECK_BELOW_REFERENCE" for r in parity):
            bad=[r for r in parity if r["status"]=="CHECK_BELOW_REFERENCE"]
            raise RuntimeError("Strategy reproduction below final reference count: "+json.dumps(bad))

        STATUS.update(state="analyzing",message="Merging 10 final locked strategies",progress=68)
        portfolio=sorted([t for sid in ids for t in baseline[sid]],key=lambda x:(x["signal_time"],x["strategy_id"]))

        # Strategy summaries / periods / calendars / rolling.
        strategy_summary=[]; strategy_periods=[]; strategy_calendar=[]; strategy_roll=[]; strategy_cost=[]
        period_defs=[
            ("FULL",None,None),
            ("PRE_2010",None,datetime(2010,1,1,tzinfo=timezone.utc)),
            ("2010_PLUS",datetime(2010,1,1,tzinfo=timezone.utc),None),
            ("2018_PLUS",datetime(2018,1,1,tzinfo=timezone.utc),None),
            ("LAST_10Y",NOW-timedelta(days=365.2425*10),NOW),
            ("LAST_5Y",NOW-timedelta(days=365.2425*5),NOW),
            ("LAST_3Y",NOW-timedelta(days=365.2425*3),NOW),
            ("LAST_2Y",NOW-timedelta(days=365.2425*2),NOW),
            ("LAST_1Y",NOW-timedelta(days=365.2425),NOW),
        ]
        common_start=max(datetime.fromisoformat(r["actual_first_m15_utc"].replace("Z","+00:00")) for r in coverage)
        end_complete=month_floor(NOW)

        for sid in ids:
            tr=baseline[sid]; s=calc_stats(tr); se=calc_stats(tr,"exit")
            strategy_summary.append({"strategy_id":sid,**s,"exit_order_max_drawdown_r":se["max_drawdown_r"],"contribution_pct_of_portfolio_r":0.0})
            for label,a,b in period_defs: strategy_periods.append(stats_row(sid,label,tr,a,b))
            strategy_calendar.extend(calendar_rows(sid,tr,START.year,NOW.year))
            for m in (12,24,36): strategy_roll.extend(rolling_rows(sid,tr,m,common_start,end_complete))
            for cost in COSTS:
                ss=calc_stats(strategy_trades_by_cost[cost][sid])
                strategy_cost.append({"strategy_id":sid,"cost_pips":cost,**ss})

        port_stats=calc_stats(portfolio); port_exit=calc_stats(portfolio,"exit")
        for r in strategy_summary:
            r["contribution_pct_of_portfolio_r"]=pct(r["total_r"],port_stats["total_r"])
        write_csv(OUT["strategy_summary"],strategy_summary)
        write_csv(OUT["strategy_periods"],strategy_periods)
        write_csv(OUT["strategy_calendar"],strategy_calendar)
        write_csv(OUT["strategy_rolling"],strategy_roll)
        write_csv(OUT["strategy_rolling_summary"],rolling_summary(strategy_roll))
        write_csv(OUT["strategy_cost"],strategy_cost)

        # Pair summaries.
        pair_rows=[]
        for pair in PAIRS:
            ts=[t for t in portfolio if t["pair"]==pair]
            s=calc_stats(ts)
            pair_rows.append({"pair":pair,**s,"contribution_pct_of_portfolio_r":pct(s["total_r"],port_stats["total_r"])})
        write_csv(OUT["pair_summary"],pair_rows)

        # Portfolio headline + periods.
        write_csv(OUT["portfolio_summary"],[{"scope":"PORTFOLIO","strategies":len(ids),"pairs":len(PAIRS),**port_stats,"signal_order_max_drawdown_r":port_stats["max_drawdown_r"],"exit_order_max_drawdown_r":port_exit["max_drawdown_r"],"simple_full_return_pct_at_1pct_risk":port_stats["total_r"]}])
        write_csv(OUT["portfolio_periods"],[stats_row("PORTFOLIO",label,portfolio,a,b) for label,a,b in period_defs])

        # Portfolio cost stress.
        pc=[]
        for cost in COSTS:
            pts=sorted([t for sid in ids for t in strategy_trades_by_cost[cost][sid]],key=lambda x:(x["signal_time"],x["strategy_id"]))
            ss=calc_stats(pts); ee=calc_stats(pts,"exit")
            pc.append({"cost_pips":cost,**ss,"exit_order_max_drawdown_r":ee["max_drawdown_r"]})
        write_csv(OUT["portfolio_cost"],pc)

        # Portfolio calendar / rolling.
        write_csv(OUT["portfolio_calendar"],calendar_rows("PORTFOLIO",portfolio,START.year,NOW.year))
        pr=[]
        for m in (12,24,36): pr.extend(rolling_rows("PORTFOLIO",portfolio,m,common_start,end_complete))
        write_csv(OUT["portfolio_rolling"],pr)
        write_csv(OUT["portfolio_rolling_summary"],rolling_summary(pr))

        STATUS.update(state="analyzing",message="Calculating trade frequency, monthly correlation and concurrency",progress=82)
        # Frequency by portfolio, pair and strategy across useful trailing windows.
        freq=[]
        freq_periods=[
            ("FULL",common_start,NOW),
            ("LAST_10Y",NOW-timedelta(days=365.2425*10),NOW),
            ("LAST_5Y",NOW-timedelta(days=365.2425*5),NOW),
            ("LAST_3Y",NOW-timedelta(days=365.2425*3),NOW),
            ("LAST_2Y",NOW-timedelta(days=365.2425*2),NOW),
            ("LAST_1Y",NOW-timedelta(days=365.2425),NOW),
        ]
        scopes={"PORTFOLIO":portfolio}
        scopes.update({sid:baseline[sid] for sid in ids})
        scopes.update({pair:[t for t in portfolio if t["pair"]==pair] for pair in PAIRS})
        for scope,tr in scopes.items():
            for label,a,b in freq_periods: freq.append(frequency_row(scope,tr,a,b,label))
        write_csv(OUT["frequency"],freq)

        monthly=monthly_matrix(baseline,month_floor(common_start),month_floor(NOW))
        write_csv(OUT["monthly"],monthly)
        write_csv(OUT["correlation"],correlation_rows(monthly,ids))
        write_csv(OUT["overlap"],overlap_rows(baseline))
        write_csv(OUT["concurrency"],concurrency_rows(portfolio))

        # All baseline trades, serializable timestamps.
        trade_rows=[]
        for t in portfolio:
            r=dict(t); r["signal_time"]=iso(r["signal_time"]); r["exit_time"]=iso(r["exit_time"])
            trade_rows.append(r)
        write_csv(OUT["trades"],trade_rows)

        write_csv(OUT["notes"],[
            {"item":"Frozen rules","value":"NO optimization is performed. All 10 strategy definitions are the final full-history locks listed in manifest."},
            {"item":"Portfolio concurrency","value":"Pyramiding0 is enforced per strategy; different strategy IDs may overlap, including opposite directions on one pair. Concurrency is reported, not suppressed."},
            {"item":"R aggregation","value":"Combined R is arithmetic across trades, treating each trade as one independent risk unit. simple_return_pct_at_1pct_risk = total_R × 1%; it is not an equity-compounded return when trades overlap."},
            {"item":"Drawdown","value":"Signal-order DD matches the historical strategy-sequence convention; exit-order DD is also exported and is often more intuitive for concurrently open portfolio trades."},
            {"item":"Rolling windows","value":"12/24/36M windows are month-aligned and complete only; current partial month is excluded from rolling-window endpoints."},
            {"item":"Cost stress","value":"Every strategy is rebuilt at 0.5/1.0/1.5/2.0 pips adverse fill; signal rules are unchanged."},
            {"item":"EURUSD legacy warning","value":"Old EURUSD files in Library are superseded. This runner uses the later full-history locks: LONG BR1.20/body1.00/no hour exclusion; SHORT BR1.10/body1.30/range1.70/S60D.225/NY02-03/no weekday."},
            {"item":"USDJPY legacy warning","value":"This runner uses final SWEEP30_RR4.00 + completed H1 ATR ratio>=0.80 for USDJPY LONG, not the older any-20/40/60/100 sweep lock."},
        ])

        STATUS.update(state="packaging",message="Building one ZIP results bundle",progress=95)
        with zipfile.ZipFile(BUNDLE,"w",compression=zipfile.ZIP_DEFLATED) as z:
            for p in OUT.values():
                if os.path.exists(p): z.write(p,arcname=os.path.basename(p))
        STATUS.update(state="complete",message="M15 final locked 10-strategy portfolio analysis complete",progress=100,results=BUNDLE,portfolio_trades=len(portfolio),portfolio_total_r=port_stats["total_r"],portfolio_pf=port_stats["profit_factor"])
    except Exception as e:
        import traceback
        STATUS.update(state="error",message=str(e),error_type=type(e).__name__,traceback=traceback.format_exc(),progress=STATUS.get("progress",0))


# ============================================================
# FLASK ROUTES
# ============================================================
@app.get("/")
def root():
    return jsonify({"service":"M15 Final Locked 10-Strategy Portfolio Analysis","state":STATUS.get("state"),"message":STATUS.get("message"),"orders_supported":False,"trading_enabled":False,"routes":["/m15-final-portfolio/status","/m15-final-portfolio/results"]})

@app.get("/m15-final-portfolio/status")
def status():
    return jsonify(STATUS)

@app.get("/m15-final-portfolio/results")
def results():
    if not os.path.exists(BUNDLE):
        return jsonify({"error":"Results not ready","status":STATUS}),404
    return send_file(os.path.abspath(BUNDLE),as_attachment=True,download_name=BUNDLE)


def start_background():
    threading.Thread(target=run_research,daemon=True).start()

if __name__ == "__main__":
    start_background()
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","8080")),threaded=True)
