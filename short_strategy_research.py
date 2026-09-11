"""
H1 + M15 FINAL LOCKED 20-STRATEGY PORTFOLIO ANALYSIS
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
    - event-driven compounded equity at 0.50% / 0.75% / 1.00% risk per trade
    - compounded calendar-year and rolling 12/24/36M returns
    - concurrent open-risk exposure and conservative open-risk floor DD
    - compounded cost-stress matrix across 0.5/1.0/1.5/2.0-pip fills

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

# Equity simulation. Scale-invariant: change the starting balance if desired.
# Every new trade risks this fraction of THEN-REALISED account equity.
# Existing open trades keep the cash risk amount fixed at their own entry.
STARTING_BALANCE = float(os.getenv("PORTFOLIO_START_BALANCE", "100.0"))
RISK_LEVELS = [0.0050, 0.0075, 0.0100]  # 0.50%, 0.75%, 1.00%

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
    "equity_summary": "m15_final_portfolio_equity_summary.csv",
    "equity_periods": "m15_final_portfolio_equity_periods.csv",
    "equity_calendar": "m15_final_portfolio_equity_calendar_years.csv",
    "equity_calendar_summary": "m15_final_portfolio_equity_calendar_summary.csv",
    "equity_rolling": "m15_final_portfolio_equity_rolling.csv",
    "equity_rolling_summary": "m15_final_portfolio_equity_rolling_summary.csv",
    "equity_curve": "m15_final_portfolio_equity_curve.csv",
    "equity_trades": "m15_final_portfolio_equity_trades.csv",
    "equity_cost_stress": "m15_final_portfolio_equity_cost_stress.csv",
}
BUNDLE = "M15_FINAL_LOCKED_PORTFOLIO_EQUITY_COMPOUNDING_RESULTS.zip"

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
# EVENT-DRIVEN EQUITY / COMPOUNDING
# ============================================================

def _trade_key(t):
    return (
        t["strategy_id"],
        t["pair"],
        int(t.get("signal_index", -1)),
        t["signal_time"],
        t["exit_time"],
    )


def _equity_balance_before(curve_exit_times, curve_balances, ts, starting_balance):
    """
    Balance immediately BEFORE events stamped exactly at ts.
    This makes a [start, end) period include exits at start and exclude exits at end.
    """
    j = bisect.bisect_left(curve_exit_times, ts) - 1
    return curve_balances[j] if j >= 0 else starting_balance


def simulate_equity(trades, risk_fraction, starting_balance=100.0):
    """
    Event-driven compounding with real concurrency.

    Sizing:
        risk_cash = realised_equity_at_signal * risk_fraction

    Concurrent positions:
        each position keeps its own original cash-risk amount until exit.
        No portfolio risk cap is imposed; this intentionally represents the
        requested fixed per-trade risk system.

    Event ordering:
        EXIT before ENTRY at the exact same timestamp.
        This matches the historical rule that a signal on an exact exit candle
        is eligible and lets newly freed equity be used for that new signal.

    Equity:
        balance is realised/closed equity. We do NOT invent intra-trade MTM,
        because the backtest trade record contains only entry/stop/target/exit.
        We therefore also report a conservative open-risk floor:
            realised_equity - sum(open trade cash risks)
        i.e. the balance if every currently open position instantly lost 1R.
    """
    if not trades:
        return {
            "summary": {
                "risk_fraction": risk_fraction,
                "risk_pct_per_trade": risk_fraction * 100.0,
                "starting_balance": starting_balance,
                "ending_balance": starting_balance,
                "total_return_pct": 0.0,
                "cagr_pct": 0.0,
                "max_closed_equity_dd_pct": 0.0,
                "max_open_risk_floor_dd_pct": 0.0,
                "max_open_positions": 0,
                "max_open_risk_cash": 0.0,
                "max_open_risk_pct_of_realised_equity": 0.0,
                "trades": 0,
            },
            "curve": [],
            "trade_rows": [],
            "exit_times": [],
            "exit_balances": [],
        }

    trades_sorted = sorted(trades, key=lambda x: (x["signal_time"], x["strategy_id"]))
    events = []
    for n, t in enumerate(trades_sorted):
        key = _trade_key(t) + (n,)
        events.append((t["signal_time"], 1, t["strategy_id"], key, t))  # entry
        events.append((t["exit_time"], 0, t["strategy_id"], key, t))    # exit
    # 0=exit before 1=entry at same timestamp.
    events.sort(key=lambda e: (e[0], e[1], e[2], e[3]))

    balance = float(starting_balance)
    peak = balance
    max_closed_dd_pct = 0.0
    max_open_floor_dd_pct = 0.0

    open_trades = {}
    open_risk_cash = 0.0
    max_open_positions = 0
    max_open_risk_cash = 0.0
    max_open_risk_pct = 0.0

    curve = []
    trade_rows = []
    exit_times = []
    exit_balances = []

    first_signal = trades_sorted[0]["signal_time"]
    last_exit = max(t["exit_time"] for t in trades_sorted)

    for ts, event_kind, sid, key, t in events:
        if event_kind == 0:  # EXIT
            rec = open_trades.pop(key, None)
            if rec is None:
                raise RuntimeError(
                    f"Equity simulator exit without open trade: {sid} {iso(ts)}"
                )

            balance_before_exit = balance
            pnl_cash = rec["risk_cash"] * float(t["r"])
            balance += pnl_cash
            open_risk_cash -= rec["risk_cash"]
            if abs(open_risk_cash) < 1e-12:
                open_risk_cash = 0.0

            peak = max(peak, balance)
            closed_dd_pct = ((balance / peak) - 1.0) * 100.0 if peak > 0 else -100.0
            max_closed_dd_pct = min(max_closed_dd_pct, closed_dd_pct)

            open_risk_pct = (
                (open_risk_cash / balance) * 100.0 if balance > 0 else 999.0
            )
            floor_equity = balance - open_risk_cash
            floor_dd_pct = (
                ((floor_equity / peak) - 1.0) * 100.0 if peak > 0 else -100.0
            )
            max_open_floor_dd_pct = min(max_open_floor_dd_pct, floor_dd_pct)

            exit_times.append(ts)
            exit_balances.append(balance)

            trade_rows.append({
                "risk_pct_per_trade": risk_fraction * 100.0,
                "pair": t["pair"],
                "strategy_id": t["strategy_id"],
                "trigger": t["trigger"],
                "side": t["side"],
                "signal_time": iso(t["signal_time"]),
                "exit_time": iso(t["exit_time"]),
                "r": t["r"],
                "result": t["result"],
                "entry_realised_equity": rec["entry_equity"],
                "risk_cash": rec["risk_cash"],
                "pnl_cash": pnl_cash,
                "balance_before_exit": balance_before_exit,
                "balance_after_exit": balance,
                "open_positions_after_exit": len(open_trades),
                "open_risk_cash_after_exit": open_risk_cash,
                "open_risk_pct_of_realised_equity_after_exit": open_risk_pct,
                "closed_equity_drawdown_pct": closed_dd_pct,
                "open_risk_floor_equity": floor_equity,
                "open_risk_floor_drawdown_pct": floor_dd_pct,
            })

            curve.append({
                "risk_pct_per_trade": risk_fraction * 100.0,
                "time_utc": iso(ts),
                "event": "EXIT",
                "strategy_id": sid,
                "balance": balance,
                "peak_balance": peak,
                "closed_equity_drawdown_pct": closed_dd_pct,
                "open_positions": len(open_trades),
                "open_risk_cash": open_risk_cash,
                "open_risk_pct_of_realised_equity": open_risk_pct,
                "open_risk_floor_equity": floor_equity,
                "open_risk_floor_drawdown_pct": floor_dd_pct,
            })

        else:  # ENTRY
            if balance <= 0:
                raise RuntimeError(
                    f"Equity depleted before entry: balance={balance} at {iso(ts)}"
                )
            risk_cash = balance * risk_fraction
            open_trades[key] = {
                "risk_cash": risk_cash,
                "entry_equity": balance,
            }
            open_risk_cash += risk_cash
            max_open_positions = max(max_open_positions, len(open_trades))
            max_open_risk_cash = max(max_open_risk_cash, open_risk_cash)

            open_risk_pct = (
                (open_risk_cash / balance) * 100.0 if balance > 0 else 999.0
            )
            max_open_risk_pct = max(max_open_risk_pct, open_risk_pct)

            floor_equity = balance - open_risk_cash
            floor_dd_pct = (
                ((floor_equity / peak) - 1.0) * 100.0 if peak > 0 else -100.0
            )
            max_open_floor_dd_pct = min(max_open_floor_dd_pct, floor_dd_pct)

            curve.append({
                "risk_pct_per_trade": risk_fraction * 100.0,
                "time_utc": iso(ts),
                "event": "ENTRY",
                "strategy_id": sid,
                "balance": balance,
                "peak_balance": peak,
                "closed_equity_drawdown_pct": ((balance / peak) - 1.0) * 100.0,
                "open_positions": len(open_trades),
                "open_risk_cash": open_risk_cash,
                "open_risk_pct_of_realised_equity": open_risk_pct,
                "open_risk_floor_equity": floor_equity,
                "open_risk_floor_drawdown_pct": floor_dd_pct,
            })

    if open_trades:
        raise RuntimeError(
            f"Equity simulator finished with {len(open_trades)} open trades"
        )

    years = max((last_exit - first_signal).total_seconds() / (365.2425 * 86400.0), 1e-9)
    total_return_pct = ((balance / starting_balance) - 1.0) * 100.0
    cagr_pct = (
        ((balance / starting_balance) ** (1.0 / years) - 1.0) * 100.0
        if balance > 0 and starting_balance > 0 else -100.0
    )

    summary = {
        "risk_fraction": risk_fraction,
        "risk_pct_per_trade": risk_fraction * 100.0,
        "starting_balance": starting_balance,
        "ending_balance": balance,
        "ending_multiple": balance / starting_balance if starting_balance else 0.0,
        "total_return_pct": total_return_pct,
        "cagr_pct": cagr_pct,
        "simulation_start_utc": iso(first_signal),
        "simulation_end_utc": iso(last_exit),
        "simulation_years": years,
        "trades": len(trades_sorted),
        "max_closed_equity_dd_pct": max_closed_dd_pct,
        "max_open_risk_floor_dd_pct": max_open_floor_dd_pct,
        "max_open_positions": max_open_positions,
        "max_open_risk_cash": max_open_risk_cash,
        "max_open_risk_pct_of_realised_equity": max_open_risk_pct,
    }

    return {
        "summary": summary,
        "curve": curve,
        "trade_rows": trade_rows,
        "exit_times": exit_times,
        "exit_balances": exit_balances,
    }


def equity_period_row(sim, risk_fraction, label, start, end, trades):
    sb = _equity_balance_before(
        sim["exit_times"], sim["exit_balances"], start, STARTING_BALANCE
    )
    eb = _equity_balance_before(
        sim["exit_times"], sim["exit_balances"], end, STARTING_BALANCE
    )
    tr = [t for t in trades if start <= t["exit_time"] < end]
    ret = ((eb / sb) - 1.0) * 100.0 if sb > 0 else 0.0
    days = max((end - start).total_seconds() / 86400.0, 1e-9)
    years = days / 365.2425
    ann = (
        ((eb / sb) ** (1.0 / years) - 1.0) * 100.0
        if sb > 0 and eb > 0 and years > 0 else 0.0
    )
    return {
        "risk_pct_per_trade": risk_fraction * 100.0,
        "period": label,
        "start_utc": iso(start),
        "end_utc": iso(end),
        "start_balance": sb,
        "end_balance": eb,
        "compounded_return_pct": ret,
        "annualized_return_pct": ann,
        "realized_exits": len(tr),
    }


def equity_calendar_rows(sim, risk_fraction, trades, first_active_year, last_year):
    rows = []
    for y in range(first_active_year, last_year + 1):
        a = datetime(y, 1, 1, tzinfo=timezone.utc)
        nominal_b = datetime(y + 1, 1, 1, tzinfo=timezone.utc)
        b = min(nominal_b, NOW)
        if b <= a:
            continue
        row = equity_period_row(sim, risk_fraction, str(y), a, b, trades)
        row["year"] = y
        row["complete_year"] = nominal_b <= NOW
        rows.append(row)
    return rows


def equity_calendar_summary(rows):
    grouped = defaultdict(list)
    for r in rows:
        grouped[r["risk_pct_per_trade"]].append(r)

    out = []
    for risk_pct, g in grouped.items():
        complete = [x for x in g if x["complete_year"]]
        active = [x for x in complete if x["realized_exits"] > 0]
        positive = [x for x in active if x["compounded_return_pct"] > 0]
        worst = min(active, key=lambda x: x["compounded_return_pct"]) if active else None
        best = max(active, key=lambda x: x["compounded_return_pct"]) if active else None
        out.append({
            "risk_pct_per_trade": risk_pct,
            "completed_years": len(complete),
            "active_completed_years": len(active),
            "positive_active_completed_years": len(positive),
            "positive_active_completed_years_pct": pct(len(positive), len(active)),
            "average_compounded_return_pct_active_year": (
                sum(x["compounded_return_pct"] for x in active) / len(active)
                if active else 0.0
            ),
            "median_compounded_return_pct_active_year": safe_median(
                x["compounded_return_pct"] for x in active
            ),
            "worst_year": worst["year"] if worst else "",
            "worst_year_return_pct": worst["compounded_return_pct"] if worst else 0.0,
            "best_year": best["year"] if best else "",
            "best_year_return_pct": best["compounded_return_pct"] if best else 0.0,
        })
    return out


def equity_rolling_rows(sim, risk_fraction, trades, months, start_month, end_complete_month):
    rows = []
    cur = start_month
    while add_months(cur, months) <= end_complete_month:
        end = add_months(cur, months)
        row = equity_period_row(
            sim, risk_fraction, f"ROLLING_{months}M", cur, end, trades
        )
        row["months"] = months
        rows.append(row)
        cur = add_months(cur, 1)
    return rows


def equity_rolling_summary(rows):
    grouped = defaultdict(list)
    for r in rows:
        grouped[(r["risk_pct_per_trade"], r["months"])].append(r)

    out = []
    for (risk_pct, months), g in grouped.items():
        active = [x for x in g if x["realized_exits"] > 0]
        positive = [x for x in active if x["compounded_return_pct"] > 0]
        worst = min(active, key=lambda x: x["compounded_return_pct"]) if active else None
        best = max(active, key=lambda x: x["compounded_return_pct"]) if active else None
        out.append({
            "risk_pct_per_trade": risk_pct,
            "months": months,
            "windows": len(g),
            "active_windows": len(active),
            "zero_exit_windows": len(g) - len(active),
            "positive_active_windows": len(positive),
            "positive_active_windows_pct": pct(len(positive), len(active)),
            "median_compounded_return_pct_active": safe_median(
                x["compounded_return_pct"] for x in active
            ),
            "median_realized_exits_active": safe_median(
                x["realized_exits"] for x in active
            ),
            "worst_compounded_return_pct": (
                worst["compounded_return_pct"] if worst else 0.0
            ),
            "worst_start_utc": worst["start_utc"] if worst else "",
            "best_compounded_return_pct": (
                best["compounded_return_pct"] if best else 0.0
            ),
            "best_start_utc": best["start_utc"] if best else "",
        })
    return out


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

        # ------------------------------------------------------------
        # Event-driven compounded equity analysis at baseline 1-pip cost.
        # ------------------------------------------------------------
        STATUS.update(
            state="analyzing",
            message="Simulating compounded equity at 0.50%, 0.75% and 1.00% risk",
            progress=72,
        )
        equity_sims = {
            rf: simulate_equity(portfolio, rf, STARTING_BALANCE)
            for rf in RISK_LEVELS
        }

        equity_summary = [equity_sims[rf]["summary"] for rf in RISK_LEVELS]
        equity_curve = []
        equity_trades = []
        equity_periods = []
        equity_calendar = []
        equity_rolling = []

        active_start = month_floor(portfolio[0]["signal_time"])
        first_active_year = portfolio[0]["signal_time"].year
        equity_period_defs = [
            ("LAST_10Y", NOW - timedelta(days=365.2425 * 10), NOW),
            ("LAST_5Y", NOW - timedelta(days=365.2425 * 5), NOW),
            ("LAST_3Y", NOW - timedelta(days=365.2425 * 3), NOW),
            ("LAST_2Y", NOW - timedelta(days=365.2425 * 2), NOW),
            ("LAST_1Y", NOW - timedelta(days=365.2425), NOW),
        ]
        equity_end_complete = month_floor(NOW)

        for rf in RISK_LEVELS:
            sim = equity_sims[rf]
            equity_curve.extend(sim["curve"])
            equity_trades.extend(sim["trade_rows"])

            for label, a, b in equity_period_defs:
                equity_periods.append(
                    equity_period_row(sim, rf, label, a, b, portfolio)
                )

            equity_calendar.extend(
                equity_calendar_rows(
                    sim, rf, portfolio, first_active_year, NOW.year
                )
            )

            for months in (12, 24, 36):
                equity_rolling.extend(
                    equity_rolling_rows(
                        sim, rf, portfolio, months, active_start, equity_end_complete
                    )
                )

        write_csv(OUT["equity_summary"], equity_summary)
        write_csv(OUT["equity_periods"], equity_periods)
        write_csv(OUT["equity_calendar"], equity_calendar)
        write_csv(
            OUT["equity_calendar_summary"],
            equity_calendar_summary(equity_calendar),
        )
        write_csv(OUT["equity_rolling"], equity_rolling)
        write_csv(
            OUT["equity_rolling_summary"],
            equity_rolling_summary(equity_rolling),
        )
        write_csv(OUT["equity_curve"], equity_curve)
        write_csv(OUT["equity_trades"], equity_trades)

        # Compounded cost stress: same frozen signals/rebuild logic, all
        # requested cost assumptions x all three risk levels.
        equity_cost_rows = []
        for cost in COSTS:
            pts = sorted(
                [
                    t
                    for sid in ids
                    for t in strategy_trades_by_cost[cost][sid]
                ],
                key=lambda x: (x["signal_time"], x["strategy_id"]),
            )
            for rf in RISK_LEVELS:
                ss = simulate_equity(pts, rf, STARTING_BALANCE)["summary"]
                equity_cost_rows.append({
                    "cost_pips": cost,
                    **ss,
                })
        write_csv(OUT["equity_cost_stress"], equity_cost_rows)

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
            {"item":"Equity sizing","value":"For compounded simulations, each new trade risks 0.50%, 0.75% or 1.00% of THEN-REALISED equity at signal time. Existing open positions keep their original cash-risk amount."},
            {"item":"Equity event order","value":"At the same timestamp, exits are processed before new entries, matching exact exit-candle signal eligibility."},
            {"item":"Equity drawdown","value":"max_closed_equity_dd_pct is based on realised exits only. Because intra-trade mark-to-market is not reconstructed, max_open_risk_floor_dd_pct is also exported as a conservative floor assuming every currently open trade instantly loses its full 1R cash risk."},
            {"item":"No portfolio risk cap","value":"Different locked strategies may overlap exactly as in the portfolio test. The compounding simulation does not suppress trades or cap aggregate open risk; it reports the resulting concurrent cash risk."},
            {"item":"Equity rolling windows","value":"Compounded rolling 12/24/36M returns start from the month of the first actual portfolio trade, avoiding the pre-signal 2002-2004 zero-history distortion."},
        ])

        STATUS.update(state="packaging",message="Building one ZIP results bundle",progress=95)
        with zipfile.ZipFile(BUNDLE,"w",compression=zipfile.ZIP_DEFLATED) as z:
            for p in OUT.values():
                if os.path.exists(p): z.write(p,arcname=os.path.basename(p))
        STATUS.update(state="complete",message="M15 final locked 10-strategy portfolio + equity analysis complete",progress=100,results=BUNDLE,portfolio_trades=len(portfolio),portfolio_total_r=port_stats["total_r"],portfolio_pf=port_stats["profit_factor"])
    except Exception as e:
        import traceback
        STATUS.update(state="error",message=str(e),error_type=type(e).__name__,traceback=traceback.format_exc(),progress=STATUS.get("progress",0))



# ============================================================
# H1 + M15 20-STRATEGY COMBINED PORTFOLIO LAYER
# ============================================================
#
# READ ONLY. NEVER SENDS ORDERS.
#
# H1 authority:
#   strategy_probe(6).py SHA256:
#   4ec3b8870fb085eb842fdf85e60681b81968b300efa1a9e1ab677a1d242d5a65
#
# Executor reference:
#   app(7).py SHA256:
#   14d9aa2c555f7809efd8b8f03a53923c773d47494aaa0c60883d5b54cbfa89d4
#
# M15 authority:
#   exact previously verified M15 portfolio runner SHA256:
#   703baac7e933c6b5b33c2f5870bbacf6a0d7954b5a5067a313c524c7b8f15df8
#
# The H1 strategy dictionaries below were extracted literally from the
# attached live strategy engine. The H1 historical signal/trade functions
# below are copied from that same live engine. The short historical trade
# wrapper comes from the prior final-live H1 portfolio audit whose strategy
# dictionaries were programmatically verified identical to the attached
# live engine before this combined file was generated.
#
# Baseline costs:
#   H1  = 5 adverse ticks (the live H1 historical convention)
#   M15 = 1 adverse pip (the final full-history M15 convention)
#
# Portfolio modes:
#   INDEPENDENT
#       Every strategy keeps only its own internal pyramiding=0 rule.
#       Different H1/M15 strategies can overlap, including same instrument.
#
#   PAIR_GATE_H1_FIRST
#       Mimics the current live strategy-engine rule that an already-open
#       OANDA trade on an instrument blocks any new trade on that instrument.
#       If H1 and M15 signals occur at the exact same entry timestamp, H1
#       gets priority. BUY precedes SELL within the same timeframe.
#
#   PAIR_GATE_M15_FIRST
#       Same one-open-trade-per-instrument gate, but M15 gets priority on an
#       exact H1/M15 tie. This exists because the future combined watcher
#       scheduling order has not yet been defined.
#
# Equity scenarios:
#   full 3x3 H1-risk x M15-risk matrix:
#       0.50%, 0.75%, 1.00% per new trade.
#
# Sizing:
#   risk is based on then-REALISED equity because candle-only backtests do
#   not reconstruct mark-to-market NAV between exits. The actual executor
#   uses current OANDA NAV. Open positions retain their entry cash-risk.
# ============================================================

# H1 globals expected by the exact copied live functions.
NY_TZ = NY
DAILY_ALIGNMENT_HOUR = 17
DAILY_ALIGNMENT_TIMEZONE = "America/New_York"
BACKTEST_SLIPPAGE_TICKS = 5

STRATEGIES = {'EUR_USD': {'tick_size': 1e-05,
             'price_precision': 5,
             'signal_id_prefix': 'EURUSD',
             'minimum_body_ratio': 1.0,
             'strong_close_enabled': True,
             'minimum_close_location': 0.6,
             'lower_wick_filter_enabled': False,
             'minimum_lower_wick_body_ratio': None,
             'atr_length': 14,
             'structure_lookback': 15,
             'maximum_distance_atr': 0.1,
             'minimum_range_enabled': False,
             'minimum_range_atr': None,
             'fast_daily_ema': 1,
             'slow_daily_ema': 1,
             'daily_close_ema': None,
             'require_daily_close_above_slow': False,
             'require_daily_fast_above_slow': False,
             'minimum_daily_atr_ratio_50': None,
             'session_timezone': 'America/New_York',
             'session_mode': 'include',
             'session_start_hour': 8,
             'session_end_hour': 16,
             'excluded_weekdays': {1, 4},
             'reward_risk': 3.5,
             'stop_buffer_ticks': 10},
 'GBP_USD': {'tick_size': 1e-05,
             'price_precision': 5,
             'signal_id_prefix': 'GBPUSD',
             'minimum_body_ratio': 1.4,
             'strong_close_enabled': True,
             'minimum_close_location': 0.65,
             'lower_wick_filter_enabled': False,
             'minimum_lower_wick_body_ratio': None,
             'atr_length': 14,
             'structure_lookback': 20,
             'maximum_distance_atr': 0.25,
             'minimum_range_enabled': True,
             'minimum_range_atr': 0.9,
             'fast_daily_ema': 50,
             'slow_daily_ema': 70,
             'require_daily_close_above_slow': True,
             'require_daily_fast_above_slow': True,
             'session_timezone': 'America/New_York',
             'session_mode': 'exclude',
             'session_start_hour': 14,
             'session_end_hour': 19,
             'excluded_weekdays': set(),
             'reward_risk': 4.25,
             'stop_buffer_ticks': 10},
 'USD_JPY': {'tick_size': 0.001,
             'price_precision': 3,
             'signal_id_prefix': 'USDJPY',
             'minimum_body_ratio': 1.0,
             'strong_close_enabled': True,
             'minimum_close_location': 0.6,
             'lower_wick_filter_enabled': False,
             'minimum_lower_wick_body_ratio': None,
             'atr_length': 14,
             'minimum_body_atr': 0.8,
             'structure_lookback': 100,
             'maximum_distance_atr': 0.55,
             'minimum_range_enabled': False,
             'minimum_range_atr': None,
             'fast_daily_ema': 1,
             'slow_daily_ema': 1,
             'daily_close_ema': None,
             'require_daily_close_above_slow': False,
             'require_daily_fast_above_slow': False,
             'minimum_daily_atr_ratio_50': None,
             'session_timezone': 'America/New_York',
             'session_mode': 'all',
             'session_start_hour': None,
             'session_end_hour': None,
             'excluded_weekdays': set(),
             'reward_risk': 3.75,
             'stop_buffer_ticks': 10},
 'USD_CAD': {'tick_size': 1e-05,
             'price_precision': 5,
             'signal_id_prefix': 'USDCAD',
             'minimum_body_ratio': 1.1,
             'strong_close_enabled': False,
             'minimum_close_location': 0.75,
             'lower_wick_filter_enabled': True,
             'minimum_lower_wick_body_ratio': 0.2,
             'atr_length': 14,
             'minimum_body_atr': 0.6,
             'structure_lookback': 40,
             'maximum_distance_atr': 0.05,
             'minimum_range_enabled': True,
             'minimum_range_atr': 1.2,
             'fast_daily_ema': 1,
             'slow_daily_ema': 1,
             'require_daily_close_above_slow': False,
             'require_daily_fast_above_slow': False,
             'minimum_daily_atr_ratio_50': 0.95,
             'session_timezone': 'America/New_York',
             'session_mode': 'all',
             'session_start_hour': None,
             'session_end_hour': None,
             'excluded_weekdays': set(),
             'reward_risk': 3.5,
             'stop_buffer_ticks': 10},
 'EUR_GBP': {'tick_size': 1e-05,
             'price_precision': 5,
             'signal_id_prefix': 'EURGBP',
             'minimum_body_ratio': 1.0,
             'strong_close_enabled': False,
             'minimum_close_location': 0.75,
             'lower_wick_filter_enabled': False,
             'minimum_lower_wick_body_ratio': None,
             'atr_length': 14,
             'minimum_body_atr': 1.1,
             'structure_lookback': 30,
             'maximum_distance_atr': 0.075,
             'minimum_range_enabled': True,
             'minimum_range_atr': 1.4,
             'daily_close_ema': 200,
             'fast_daily_ema': 20,
             'slow_daily_ema': 150,
             'require_daily_close_above_slow': False,
             'require_daily_fast_above_slow': True,
             'minimum_daily_atr_ratio_50': None,
             'session_timezone': 'Europe/London',
             'session_mode': 'all',
             'session_start_hour': None,
             'session_end_hour': None,
             'excluded_weekdays': set(),
             'reward_risk': 3.0,
             'stop_buffer_ticks': 10}}
SHORT_STRATEGIES = {'EUR_USD': {'strategy_name': 'BALANCED_815',
             'tick_size': 1e-05,
             'price_precision': 5,
             'signal_id_prefix': 'EURUSDSHORT',
             'minimum_body_ratio': 1.1,
             'maximum_close_location': 0.275,
             'atr_length': 14,
             'structure_lookback': 55,
             'maximum_distance_atr': 0.35,
             'fast_daily_ema': 85,
             'slow_daily_ema': 100,
             'require_daily_fast_below_slow': True,
             'minimum_daily_ema_separation_atr': 0.05,
             'maximum_slow_ema_slope_5d_atr': None,
             'minimum_daily_atr_ratio_50': None,
             'session_timezone': 'America/New_York',
             'excluded_hours': {2, 10, 12, 14},
             'excluded_weekdays': set(),
             'reward_risk': 4.0,
             'stop_buffer_ticks': 10},
 'GBP_USD': {'strategy_name': 'GBPUSD_FINAL_SHORT',
             'tick_size': 1e-05,
             'price_precision': 5,
             'signal_id_prefix': 'GBPUSDSHORT',
             'minimum_body_ratio': 1.0,
             'maximum_close_location': None,
             'atr_length': 14,
             'structure_lookback': 70,
             'maximum_distance_atr': 0.175,
             'fast_daily_ema': 40,
             'slow_daily_ema': 100,
             'require_daily_fast_below_slow': True,
             'minimum_daily_ema_separation_atr': None,
             'maximum_slow_ema_slope_5d_atr': -0.05,
             'minimum_daily_atr_ratio_50': 0.8,
             'session_timezone': 'America/New_York',
             'excluded_hours': {3, 15},
             'excluded_weekdays': set(),
             'reward_risk': 2.5,
             'stop_buffer_ticks': 10},
 'USD_JPY': {'strategy_name': 'USDJPY_FINAL_SHORT',
             'tick_size': 0.001,
             'price_precision': 3,
             'signal_id_prefix': 'USDJPYSHORT',
             'minimum_body_ratio': 1.45,
             'maximum_close_location': None,
             'atr_length': 14,
             'structure_lookback': 90,
             'maximum_distance_atr': 0.5,
             'fast_daily_ema': 90,
             'slow_daily_ema': 90,
             'require_daily_fast_below_slow': False,
             'minimum_daily_ema_separation_atr': None,
             'maximum_slow_ema_slope_5d_atr': None,
             'minimum_daily_atr_ratio_50': None,
             'session_timezone': 'America/New_York',
             'excluded_hours': {1, 5, 6, 10, 11},
             'excluded_weekdays': set(),
             'reward_risk': 2.5,
             'stop_buffer_ticks': 10},
 'USD_CAD': {'strategy_name': 'USDCAD_FINAL_SHORT',
             'tick_size': 1e-05,
             'price_precision': 5,
             'signal_id_prefix': 'USDCADSHORT',
             'minimum_body_ratio': 1.4,
             'maximum_close_location': None,
             'atr_length': 14,
             'structure_lookback': 60,
             'maximum_distance_atr': 0.25,
             'fast_daily_ema': 300,
             'slow_daily_ema': 300,
             'require_daily_fast_below_slow': False,
             'minimum_daily_ema_separation_atr': None,
             'maximum_slow_ema_slope_5d_atr': None,
             'minimum_daily_atr_ratio_50': None,
             'momentum_lookback_bars': 24,
             'minimum_upward_momentum_atr': 0.5,
             'minimum_signal_range_atr': 0.9,
             'maximum_stop_size_atr': 1.6,
             'session_timezone': 'America/New_York',
             'excluded_hours': {18},
             'excluded_weekdays': set(),
             'reward_risk': 3.25,
             'stop_buffer_ticks': 10},
 'EUR_GBP': {'strategy_name': 'EURGBP_CONFIRMED_SHORT',
             'tick_size': 1e-05,
             'price_precision': 5,
             'signal_id_prefix': 'EURGBPSHORT',
             'minimum_body_ratio': 1.0,
             'maximum_close_location': 0.2,
             'atr_length': 14,
             'structure_lookback': 90,
             'maximum_distance_atr': 0.075,
             'fast_daily_ema': 1,
             'slow_daily_ema': 1,
             'require_daily_close_below_slow': False,
             'require_daily_fast_below_slow': False,
             'minimum_daily_ema_separation_atr': None,
             'maximum_slow_ema_slope_5d_atr': None,
             'minimum_daily_atr_ratio_50': None,
             'momentum_requirements': {12: 0.25, 48: 1.0},
             'minimum_signal_range_atr': 1.1,
             'maximum_stop_size_atr': 2.5,
             'minimum_upper_wick_body_ratio': 0.1,
             'minimum_h1_atr_ratio_50': 0.8,
             'session_timezone': 'America/New_York',
             'excluded_hours': {9},
             'excluded_weekdays': set(),
             'reward_risk': 3.0,
             'stop_buffer_ticks': 10}}

H1_REFERENCE_FULL_TRADES = 1314
M15_REFERENCE_PORTFOLIO_TRADES = 1021

H1_DATA_WARMUP = START - timedelta(days=120)
H1_DAILY_WARMUP = START - timedelta(days=1500)

COMBINED_RISK_LEVELS = [0.0050, 0.0075, 0.0100]
COST_MULTIPLIERS = [0.50, 1.00, 1.50, 2.00]

COMBINED_OUT = {
    "source_audit": "h1_m15_20_source_audit.csv",
    "manifest": "h1_m15_20_manifest.csv",
    "coverage": "h1_m15_20_coverage.csv",
    "parity": "h1_m15_20_parity.csv",
    "strategy_summary": "h1_m15_20_strategy_summary.csv",
    "timeframe_summary": "h1_m15_20_timeframe_summary.csv",
    "pair_summary": "h1_m15_20_pair_summary.csv",
    "mode_summary": "h1_m15_20_portfolio_mode_summary.csv",
    "periods": "h1_m15_20_periods.csv",
    "calendar": "h1_m15_20_calendar_years.csv",
    "rolling": "h1_m15_20_rolling.csv",
    "rolling_summary": "h1_m15_20_rolling_summary.csv",
    "frequency": "h1_m15_20_trade_frequency.csv",
    "monthly": "h1_m15_20_monthly_by_strategy.csv",
    "correlation": "h1_m15_20_monthly_correlation.csv",
    "overlap": "h1_m15_20_overlap.csv",
    "concurrency": "h1_m15_20_concurrency.csv",
    "pair_gate_rejections": "h1_m15_20_pair_gate_rejections.csv",
    "cost_stress": "h1_m15_20_cost_stress.csv",
    "equity_matrix": "h1_m15_20_equity_risk_matrix.csv",
    "equity_periods": "h1_m15_20_equity_periods.csv",
    "equity_calendar": "h1_m15_20_equity_calendar_years.csv",
    "equity_calendar_summary": "h1_m15_20_equity_calendar_summary.csv",
    "equity_rolling": "h1_m15_20_equity_rolling.csv",
    "equity_rolling_summary": "h1_m15_20_equity_rolling_summary.csv",
    "equity_key_curve": "h1_m15_20_equity_key_curves.csv",
    "equity_key_trades": "h1_m15_20_equity_key_trades.csv",
    "trades_independent": "h1_m15_20_trades_independent.csv",
    "trades_pair_gate_h1_first": "h1_m15_20_trades_pair_gate_h1_first.csv",
    "trades_pair_gate_m15_first": "h1_m15_20_trades_pair_gate_m15_first.csv",
    "notes": "h1_m15_20_notes.csv",
}
COMBINED_BUNDLE = "H1_M15_FINAL_20_STRATEGY_PORTFOLIO_ANALYSIS_RESULTS.zip"

COMBINED_STATUS = {
    "state": "not_started",
    "message": "H1 + M15 20-strategy analysis not started",
    "orders_supported": False,
    "trading_enabled": False,
    "progress": 0,
}

# ----------------------------------------------------------------
# Exact functions copied from attached live H1 strategy engine.
# ----------------------------------------------------------------

def strategy_timezone(
    config
):

    return ZoneInfo(
        config[
            "session_timezone"
        ]
    )


def round_price(
    value,
    config
):

    return round(
        value,
        config[
            "price_precision"
        ]
    )


def ema_series(
    values,
    length
):

    if len(values) < length:

        raise ValueError(
            f"Not enough values "
            f"for EMA{length}"
        )

    result = [
        None
    ] * len(
        values
    )

    multiplier = (
        2.0
        / (
            length + 1.0
        )
    )

    initial = (
        sum(
            values[
                :length
            ]
        )
        / length
    )

    result[
        length - 1
    ] = initial

    previous = initial

    for index in range(
        length,
        len(values)
    ):

        current = (
            (
                values[index]
                - previous
            )
            * multiplier
            + previous
        )

        result[
            index
        ] = current

        previous = current

    return result


def true_ranges(
    candles
):

    values = []

    for index, candle in enumerate(
        candles
    ):

        if index == 0:

            tr = (
                candle["high"]
                - candle["low"]
            )

        else:

            previous_close = (
                candles[
                    index - 1
                ]["close"]
            )

            tr = max(

                candle["high"]
                - candle["low"],

                abs(
                    candle["high"]
                    - previous_close
                ),

                abs(
                    candle["low"]
                    - previous_close
                )
            )

        values.append(
            tr
        )

    return values


def rma_series(
    values,
    length
):

    if len(values) < length:

        raise ValueError(
            f"Not enough values "
            f"for RMA{length}"
        )

    result = [
        None
    ] * len(
        values
    )

    initial = (
        sum(
            values[
                :length
            ]
        )
        / length
    )

    result[
        length - 1
    ] = initial

    previous = initial

    for index in range(
        length,
        len(values)
    ):

        current = (
            (
                previous
                * (
                    length - 1
                )
            )
            + values[index]
        ) / length

        result[
            index
        ] = current

        previous = current

    return result


def atr_series(
    candles,
    length
):

    return rma_series(
        true_ranges(
            candles
        ),
        length
    )


def current_daily_start(
    timestamp_utc
):

    ny_time = (
        timestamp_utc
        .astimezone(
            NY_TZ
        )
    )

    candidate = (
        ny_time.replace(

            hour=
                DAILY_ALIGNMENT_HOUR,

            minute=0,
            second=0,
            microsecond=0
        )
    )

    if ny_time < candidate:

        candidate = (
            candidate
            - timedelta(
                days=1
            )
        )

    return candidate.astimezone(
        timezone.utc
    )


def build_daily_state(
    daily,
    config
):

    closes = [
        candle["close"]
        for candle in daily
    ]

    fast_length = (
        config[
            "fast_daily_ema"
        ]
    )

    slow_length = (
        config[
            "slow_daily_ema"
        ]
    )

    if fast_length is not None:

        fast_ema = ema_series(
            closes,
            fast_length
        )

    else:

        fast_ema = [
            None
        ] * len(
            closes
        )

    slow_ema = ema_series(
        closes,
        slow_length
    )

    # Optional independent daily-close EMA regime for long
    # strategies. Existing longs leave this unset.
    daily_close_ema_length = config.get(
        "daily_close_ema"
    )

    if daily_close_ema_length is not None:

        daily_close_ema = ema_series(
            closes,
            daily_close_ema_length
        )

    else:

        daily_close_ema = [
            None
        ] * len(
            closes
        )

    # Daily volatility regime support for long strategies.
    # Existing longs ignore this unless
    # minimum_daily_atr_ratio_50 is configured.
    daily_atr = atr_series(
        daily,
        14
    )

    daily_atr_sma50 = [
        None
    ] * len(
        daily_atr
    )

    rolling_sum = 0.0
    rolling_values = []

    for index, value in enumerate(
        daily_atr
    ):

        if value is None:
            rolling_values.append(
                None
            )
            continue

        rolling_values.append(
            value
        )
        rolling_sum += value

        if len(
            rolling_values
        ) > 50:

            removed = (
                rolling_values[
                    -51
                ]
            )

            if removed is not None:
                rolling_sum -= removed

        window = (
            rolling_values[
                -50:
            ]
        )

        if (
            len(
                window
            ) == 50
            and
            all(
                item is not None
                for item in window
            )
        ):

            daily_atr_sma50[
                index
            ] = (
                rolling_sum
                / 50.0
            )

    result = []

    for index, candle in enumerate(
        daily
    ):

        daily_atr_ratio_50 = None

        # Match the short/research warm-up convention:
        # ATR14 seed + 50 valid ATR observations.
        if (
            index >= 63
            and
            daily_atr[
                index
            ] is not None
            and
            daily_atr_sma50[
                index
            ] is not None
            and
            daily_atr_sma50[
                index
            ] > 0
        ):

            daily_atr_ratio_50 = (
                daily_atr[
                    index
                ]
                / daily_atr_sma50[
                    index
                ]
            )

        result.append({

            "time":
                candle["time"],

            "close":
                candle["close"],

            "fast_ema":
                fast_ema[
                    index
                ],

            "slow_ema":
                slow_ema[
                    index
                ],

            "daily_close_ema":
                daily_close_ema[
                    index
                ],

            "daily_atr":
                daily_atr[
                    index
                ],

            "daily_atr_ratio_50":
                daily_atr_ratio_50
        })

    return result


def previous_daily_values(
    signal_time,
    daily_state,
    config
):

    session_start = (
        current_daily_start(
            signal_time
        )
    )

    selected = None

    require_fast = (
        config[
            "require_daily_fast_above_slow"
        ]
    )

    require_daily_close_ema = (
        config.get(
            "daily_close_ema"
        )
        is not None
    )

    for row in daily_state:

        slow_ready = (
            row[
                "slow_ema"
            ]
            is not None
        )

        fast_ready = (
            not require_fast
            or
            row[
                "fast_ema"
            ]
            is not None
        )

        daily_close_ema_ready = (
            not require_daily_close_ema
            or
            row[
                "daily_close_ema"
            ]
            is not None
        )

        if (
            row["time"]
            < session_start
            and
            slow_ready
            and
            fast_ready
            and
            daily_close_ema_ready
        ):

            selected = row

        elif (
            row["time"]
            >= session_start
        ):

            break

    return selected


def build_short_daily_state(
    daily,
    config
):

    closes = [
        candle["close"]
        for candle in daily
    ]

    fast_ema = ema_series(
        closes,
        config[
            "fast_daily_ema"
        ]
    )

    slow_ema = ema_series(
        closes,
        config[
            "slow_daily_ema"
        ]
    )

    daily_atr = atr_series(
        daily,
        14
    )

    # 50-day simple average of Daily ATR14.
    daily_atr_sma50 = [None] * len(daily_atr)
    rolling_sum = 0.0
    rolling_values = []

    for index, value in enumerate(daily_atr):

        if value is None:
            rolling_values.append(None)
            continue

        rolling_values.append(value)
        rolling_sum += value

        if len(rolling_values) > 50:
            removed = rolling_values[-51]
            if removed is not None:
                rolling_sum -= removed

        window = rolling_values[-50:]

        if (
            len(window) == 50
            and all(
                item is not None
                for item in window
            )
        ):
            daily_atr_sma50[index] = (
                rolling_sum / 50.0
            )

    result = []

    for index, candle in enumerate(
        daily
    ):

        slow_slope_5d_atr = None

        if (
            index >= 5
            and slow_ema[index] is not None
            and slow_ema[index - 5] is not None
            and daily_atr[index] is not None
            and daily_atr[index] > 0
        ):
            slow_slope_5d_atr = (
                slow_ema[index]
                - slow_ema[index - 5]
            ) / daily_atr[index]

        daily_atr_ratio_50 = None

        # Match the research warmup convention exactly:
        # ATR14 first becomes valid after its 14-bar seed, then
        # the 50-day ATR mean is not exposed until index 63.
        if (
            index >= 63
            and daily_atr[index] is not None
            and daily_atr_sma50[index] is not None
            and daily_atr_sma50[index] > 0
        ):
            daily_atr_ratio_50 = (
                daily_atr[index]
                / daily_atr_sma50[index]
            )

        result.append({

            "time":
                candle["time"],

            "close":
                candle["close"],

            "fast_ema":
                fast_ema[index],

            "slow_ema":
                slow_ema[index],

            "daily_atr":
                daily_atr[index],

            "slow_ema_slope_5d_atr":
                slow_slope_5d_atr,

            "daily_atr_ratio_50":
                daily_atr_ratio_50
        })

    return result


def previous_short_daily_values(
    signal_time,
    daily_state
):

    session_start = (
        current_daily_start(
            signal_time
        )
    )

    selected = None

    for row in daily_state:

        ready = (
            row["fast_ema"] is not None
            and row["slow_ema"] is not None
            and row["daily_atr"] is not None
        )

        if (
            row["time"] < session_start
            and ready
        ):
            selected = row

        elif row["time"] >= session_start:
            break

    return selected


def local_signal_time(
    signal_time,
    config
):

    return signal_time.astimezone(
        strategy_timezone(
            config
        )
    )


def session_allowed_for(
    signal_time,
    config
):

    mode = (
        config[
            "session_mode"
        ]
    )

    if mode == "all":

        return True

    local_time = (
        local_signal_time(
            signal_time,
            config
        )
    )

    hour = (
        local_time.hour
    )

    inside_window = (
        hour
        >= config[
            "session_start_hour"
        ]
        and
        hour
        < config[
            "session_end_hour"
        ]
    )

    if mode == "include":

        return inside_window

    if mode == "exclude":

        return not inside_window

    raise ValueError(
        f"Unknown session mode: "
        f"{mode}"
    )


def weekday_allowed_for(
    signal_time,
    config
):

    local_time = (
        local_signal_time(
            signal_time,
            config
        )
    )

    return (
        local_time.weekday()
        not in config[
            "excluded_weekdays"
        ]
    )


def evaluate_signal_at_index(
    instrument,
    h1,
    atr,
    index,
    daily_state
):

    config = (
        STRATEGIES[
            instrument
        ]
    )

    minimum_index = max(

        config[
            "atr_length"
        ],

        config[
            "structure_lookback"
        ]
    )

    if index < minimum_index:

        return None

    signal = (
        h1[index]
    )

    previous = (
        h1[
            index - 1
        ]
    )

    current_atr = (
        atr[index]
    )

    if current_atr is None:

        return None

    # ==============================================
    # CANDLE VALUES
    # ==============================================

    previous_body = abs(
        previous["close"]
        - previous["open"]
    )

    current_body = abs(
        signal["close"]
        - signal["open"]
    )

    signal_range = (
        signal["high"]
        - signal["low"]
    )

    lower_wick = (
        min(
            signal["open"],
            signal["close"]
        )
        - signal["low"]
    )

    close_location = (

        (
            signal["close"]
            - signal["low"]
        )
        / signal_range

        if signal_range > 0

        else 0.0
    )

    body_ratio = (

        current_body
        / previous_body

        if previous_body > 0

        else None
    )

    body_atr = (

        current_body
        / current_atr

        if current_atr > 0

        else None
    )

    # ==============================================
    # BODY RATIO
    # ==============================================

    body_ratio_allowed = (
        previous_body > 0
        and
        current_body
        >= (
            previous_body
            * config[
                "minimum_body_ratio"
            ]
        )
    )

    # ==============================================
    # BULLISH ENGULFING
    # ==============================================

    bullish_engulfing = (

        previous[
            "close"
        ]
        < previous[
            "open"
        ]

        and

        signal[
            "close"
        ]
        > signal[
            "open"
        ]

        and

        signal[
            "open"
        ]
        <= previous[
            "close"
        ]

        and

        signal[
            "close"
        ]
        >= previous[
            "open"
        ]

        and

        body_ratio_allowed
    )

    # ==============================================
    # STRONG CLOSE
    # ==============================================

    if config[
        "strong_close_enabled"
    ]:

        strong_close_allowed = (
            close_location
            >= config[
                "minimum_close_location"
            ]
        )

    else:

        strong_close_allowed = True

    # ==============================================
    # LOWER WICK
    # ==============================================

    if config[
        "lower_wick_filter_enabled"
    ]:

        lower_wick_allowed = (
            lower_wick
            >= (
                current_body
                * config[
                    "minimum_lower_wick_body_ratio"
                ]
            )
        )

    else:

        lower_wick_allowed = True

    # ==============================================
    # MINIMUM RANGE
    # ==============================================

    if config[
        "minimum_range_enabled"
    ]:

        minimum_range_allowed = (
            signal_range
            >= (
                current_atr
                * config[
                    "minimum_range_atr"
                ]
            )
        )

    else:

        minimum_range_allowed = True

    # ==============================================
    # MINIMUM BODY / ATR
    # ==============================================

    minimum_body_atr = config.get(
        "minimum_body_atr"
    )

    minimum_body_atr_allowed = (

        True

        if minimum_body_atr is None

        else (
            body_atr is not None
            and
            body_atr >= minimum_body_atr
        )
    )

    # ==============================================
    # STRUCTURE
    # ==============================================

    lookback = (
        config[
            "structure_lookback"
        ]
    )

    previous_bars = (
        h1[
            index - lookback:
            index
        ]
    )

    previous_lowest_low = min(
        candle["low"]
        for candle
        in previous_bars
    )

    distance_from_recent_low = (
        signal["low"]
        - previous_lowest_low
    )

    maximum_distance = (
        current_atr
        * config[
            "maximum_distance_atr"
        ]
    )

    structure_allowed = (
        distance_from_recent_low
        <= maximum_distance
    )

    # ==============================================
    # DAILY
    # ==============================================

    daily = (
        previous_daily_values(
            signal["time"],
            daily_state,
            config
        )
    )

    if daily is None:

        return None

    if config[
        "require_daily_close_above_slow"
    ]:

        daily_regime_allowed = (
            daily["close"]
            > daily["slow_ema"]
        )

    else:

        daily_regime_allowed = True

    if config[
        "require_daily_fast_above_slow"
    ]:

        daily_alignment_allowed = (
            daily["fast_ema"]
            > daily["slow_ema"]
        )

    else:

        daily_alignment_allowed = True

    daily_close_ema_length = config.get(
        "daily_close_ema"
    )

    daily_close_ema_value = daily.get(
        "daily_close_ema"
    )

    daily_close_ema_allowed = (

        True

        if daily_close_ema_length is None

        else (
            daily_close_ema_value is not None
            and
            daily["close"] > daily_close_ema_value
        )
    )

    minimum_daily_atr_ratio_50 = (
        config.get(
            "minimum_daily_atr_ratio_50"
        )
    )

    daily_atr_ratio_50 = (
        daily.get(
            "daily_atr_ratio_50"
        )
    )

    daily_atr_ratio_allowed = (

        True

        if minimum_daily_atr_ratio_50 is None

        else (
            daily_atr_ratio_50 is not None
            and
            daily_atr_ratio_50
            >= minimum_daily_atr_ratio_50
        )
    )

    # ==============================================
    # SESSION / WEEKDAY
    # ==============================================

    session_allowed = (
        session_allowed_for(
            signal["time"],
            config
        )
    )

    weekday_allowed = (
        weekday_allowed_for(
            signal["time"],
            config
        )
    )

    local_time = (
        local_signal_time(
            signal["time"],
            config
        )
    )

    # ==============================================
    # FINAL
    # ==============================================

    qualified = all([

        bullish_engulfing,
        strong_close_allowed,
        lower_wick_allowed,
        minimum_range_allowed,
        minimum_body_atr_allowed,
        structure_allowed,
        daily_regime_allowed,
        daily_alignment_allowed,
        daily_close_ema_allowed,
        daily_atr_ratio_allowed,
        session_allowed,
        weekday_allowed
    ])

    return {

        "qualified":
            qualified,

        "signal_start_utc":
            signal["time"],

        "signal_close_utc":
            signal["time"]
            + timedelta(
                hours=1
            ),

        "open":
            signal["open"],

        "high":
            signal["high"],

        "low":
            signal["low"],

        "close":
            signal["close"],

        "atr":
            current_atr,

        "body_ratio":
            body_ratio,

        "body_atr":
            body_atr,

        "daily_atr_ratio_50":
            daily_atr_ratio_50,

        "close_location":
            close_location,

        "lower_wick":
            lower_wick,

        "lower_wick_body_ratio":
            (
                lower_wick
                / current_body

                if current_body > 0

                else None
            ),

        "local_timezone":
            config[
                "session_timezone"
            ],

        "local_hour":
            local_time.hour,

        "weekday":
            local_time.strftime(
                "%A"
            ),

        "previous_daily_close":
            daily[
                "close"
            ],

        "previous_daily_fast_ema":
            daily[
                "fast_ema"
            ],

        "previous_daily_slow_ema":
            daily[
                "slow_ema"
            ],

        "previous_daily_close_ema":
            daily.get(
                "daily_close_ema"
            )
    }


def evaluate_short_signal_at_index(
    instrument,
    h1,
    atr,
    index,
    daily_state
):

    if instrument not in SHORT_STRATEGIES:
        raise ValueError(
            f"{instrument} has no short strategy"
        )

    config = SHORT_STRATEGIES[instrument]

    momentum_requirements = config.get(
        "momentum_requirements",
        {}
    )

    maximum_momentum_lookback = max(
        momentum_requirements.keys(),
        default=0
    )

    h1_atr_ratio_warmup = (
        config["atr_length"] + 49
        if config.get("minimum_h1_atr_ratio_50") is not None
        else 0
    )

    minimum_index = max(
        config["atr_length"],
        config["structure_lookback"],
        config.get("momentum_lookback_bars", 0),
        maximum_momentum_lookback,
        h1_atr_ratio_warmup
    )

    if index < minimum_index:
        return None

    signal = h1[index]
    previous = h1[index - 1]
    current_atr = atr[index]

    if current_atr is None:
        return None

    previous_body = abs(
        previous["close"] - previous["open"]
    )
    current_body = abs(
        signal["close"] - signal["open"]
    )
    signal_range = (
        signal["high"] - signal["low"]
    )

    close_location = (
        (signal["close"] - signal["low"])
        / signal_range
        if signal_range > 0
        else 1.0
    )

    body_ratio = (
        current_body / previous_body
        if previous_body > 0
        else None
    )

    body_ratio_allowed = (
        previous_body > 0
        and current_body >= (
            previous_body
            * config["minimum_body_ratio"]
        )
    )

    bearish_engulfing = (
        previous["close"] > previous["open"]
        and signal["close"] < signal["open"]
        and signal["open"] >= previous["close"]
        and signal["close"] <= previous["open"]
        and body_ratio_allowed
    )

    maximum_close_location = config.get(
        "maximum_close_location"
    )
    strong_close_allowed = (
        True
        if maximum_close_location is None
        else close_location <= maximum_close_location
    )

    lookback = config["structure_lookback"]
    previous_bars = h1[index - lookback:index]
    previous_highest_high = max(
        candle["high"]
        for candle in previous_bars
    )
    distance_from_recent_high = (
        previous_highest_high
        - signal["high"]
    )
    maximum_distance = (
        current_atr
        * config["maximum_distance_atr"]
    )
    structure_allowed = (
        distance_from_recent_high
        <= maximum_distance
    )

    daily = previous_short_daily_values(
        signal["time"],
        daily_state
    )
    if daily is None:
        return None

    require_daily_close_below_slow = config.get(
        "require_daily_close_below_slow",
        True
    )
    daily_regime_allowed = (
        True
        if not require_daily_close_below_slow
        else daily["close"] < daily["slow_ema"]
    )
    require_fast_below_slow = config.get(
        "require_daily_fast_below_slow",
        True
    )
    daily_alignment_allowed = (
        True
        if not require_fast_below_slow
        else daily["fast_ema"] < daily["slow_ema"]
    )

    daily_separation = (
        (
            daily["slow_ema"]
            - daily["fast_ema"]
        ) / daily["daily_atr"]
        if daily["daily_atr"] > 0
        else None
    )

    minimum_separation = config.get(
        "minimum_daily_ema_separation_atr"
    )
    daily_separation_allowed = (
        True
        if minimum_separation is None
        else (
            daily_separation is not None
            and daily_separation >= minimum_separation
        )
    )

    maximum_slope = config.get(
        "maximum_slow_ema_slope_5d_atr"
    )
    daily_slope = daily.get(
        "slow_ema_slope_5d_atr"
    )
    daily_slope_allowed = (
        True
        if maximum_slope is None
        else (
            daily_slope is not None
            and daily_slope <= maximum_slope
        )
    )

    minimum_atr_ratio = config.get(
        "minimum_daily_atr_ratio_50"
    )
    daily_atr_ratio = daily.get(
        "daily_atr_ratio_50"
    )
    daily_atr_ratio_allowed = (
        True
        if minimum_atr_ratio is None
        else (
            daily_atr_ratio is not None
            and daily_atr_ratio >= minimum_atr_ratio
        )
    )

    # Optional H1 filters. Existing strategies leave these absent unless used.
    momentum_lookback = config.get("momentum_lookback_bars")
    minimum_upward_momentum_atr = config.get(
        "minimum_upward_momentum_atr"
    )
    upward_momentum = None
    upward_momentum_atr = None

    if momentum_lookback is not None:
        momentum_reference_close = h1[index - momentum_lookback]["close"]
        upward_momentum = signal["close"] - momentum_reference_close
        upward_momentum_atr = (
            upward_momentum / current_atr
            if current_atr > 0
            else None
        )

    upward_momentum_allowed = (
        True
        if minimum_upward_momentum_atr is None
        else (
            upward_momentum_atr is not None
            and upward_momentum_atr >= minimum_upward_momentum_atr
        )
    )

    momentum_requirement_values = {}
    momentum_requirements_allowed = True

    for lookback_bars, minimum_atr in momentum_requirements.items():
        momentum_atr_value = (
            signal["close"] - h1[index - lookback_bars]["close"]
        ) / current_atr
        momentum_requirement_values[lookback_bars] = momentum_atr_value
        if momentum_atr_value < minimum_atr:
            momentum_requirements_allowed = False

    minimum_signal_range_atr = config.get("minimum_signal_range_atr")
    signal_range_atr = (
        signal_range / current_atr
        if current_atr > 0
        else None
    )
    signal_range_allowed = (
        True
        if minimum_signal_range_atr is None
        else (
            signal_range_atr is not None
            and signal_range_atr >= minimum_signal_range_atr
        )
    )

    maximum_stop_size_atr = config.get("maximum_stop_size_atr")
    stop_price_for_filter = (
        signal["high"]
        + config["stop_buffer_ticks"] * config["tick_size"]
    )
    stop_size = stop_price_for_filter - signal["close"]
    stop_size_atr = (
        stop_size / current_atr
        if current_atr > 0
        else None
    )
    stop_size_allowed = (
        True
        if maximum_stop_size_atr is None
        else (
            stop_size_atr is not None
            and stop_size_atr <= maximum_stop_size_atr
        )
    )

    upper_wick = max(
        0.0,
        signal["high"] - max(signal["open"], signal["close"])
    )
    upper_wick_body_ratio = (
        upper_wick / current_body
        if current_body > 0
        else None
    )
    minimum_upper_wick_body_ratio = config.get(
        "minimum_upper_wick_body_ratio"
    )
    upper_wick_allowed = (
        True
        if minimum_upper_wick_body_ratio is None
        else (
            upper_wick_body_ratio is not None
            and upper_wick_body_ratio >= minimum_upper_wick_body_ratio
        )
    )

    minimum_h1_atr_ratio_50 = config.get(
        "minimum_h1_atr_ratio_50"
    )
    h1_atr_ratio_50 = None

    if minimum_h1_atr_ratio_50 is not None:
        atr_window = atr[index - 49:index + 1]
        if (
            len(atr_window) == 50
            and all(value is not None for value in atr_window)
        ):
            atr_mean_50 = sum(atr_window) / 50.0
            if atr_mean_50 > 0:
                h1_atr_ratio_50 = current_atr / atr_mean_50

    h1_atr_ratio_allowed = (
        True
        if minimum_h1_atr_ratio_50 is None
        else (
            h1_atr_ratio_50 is not None
            and h1_atr_ratio_50 >= minimum_h1_atr_ratio_50
        )
    )

    local_time = signal["time"].astimezone(
        ZoneInfo(config["session_timezone"])
    )
    session_allowed = (
        local_time.hour
        not in config["excluded_hours"]
    )
    weekday_allowed = (
        local_time.weekday()
        not in config["excluded_weekdays"]
    )

    qualified = all([
        bearish_engulfing,
        strong_close_allowed,
        structure_allowed,
        daily_regime_allowed,
        daily_alignment_allowed,
        daily_separation_allowed,
        daily_slope_allowed,
        daily_atr_ratio_allowed,
        upward_momentum_allowed,
        momentum_requirements_allowed,
        signal_range_allowed,
        stop_size_allowed,
        upper_wick_allowed,
        h1_atr_ratio_allowed,
        session_allowed,
        weekday_allowed
    ])

    return {
        "qualified": qualified,
        "side": "SELL",
        "strategy_name": config["strategy_name"],
        "signal_start_utc": signal["time"],
        "signal_close_utc": signal["time"] + timedelta(hours=1),
        "open": signal["open"],
        "high": signal["high"],
        "low": signal["low"],
        "close": signal["close"],
        "atr": current_atr,
        "body_ratio": body_ratio,
        "close_location": close_location,
        "previous_highest_high": previous_highest_high,
        "distance_from_recent_high": distance_from_recent_high,
        "distance_from_recent_high_atr": (
            distance_from_recent_high / current_atr
            if current_atr > 0
            else None
        ),
        "local_timezone": config["session_timezone"],
        "local_hour": local_time.hour,
        "weekday": local_time.strftime("%A"),
        "previous_daily_close": daily["close"],
        "previous_daily_fast_ema": daily["fast_ema"],
        "previous_daily_slow_ema": daily["slow_ema"],
        "previous_daily_atr14": daily["daily_atr"],
        "daily_ema_separation_atr": daily_separation,
        "slow_ema_slope_5d_atr": daily_slope,
        "daily_atr_ratio_50": daily_atr_ratio,
        "upward_momentum": upward_momentum,
        "upward_momentum_atr": upward_momentum_atr,
        "momentum_requirements_atr": momentum_requirement_values,
        "signal_range_atr": signal_range_atr,
        "stop_size": stop_size,
        "stop_size_atr": stop_size_atr,
        "upper_wick_body_ratio": upper_wick_body_ratio,
        "h1_atr_ratio_50": h1_atr_ratio_50
    }


def create_trade(
    instrument,
    signal_result
):

    config = (
        STRATEGIES[
            instrument
        ]
    )

    tick = (
        config[
            "tick_size"
        ]
    )

    reference_entry = (
        signal_result[
            "close"
        ]
    )

    backtest_entry = (
        reference_entry
        + (
            BACKTEST_SLIPPAGE_TICKS
            * tick
        )
    )

    stop = (
        signal_result[
            "low"
        ]
        - (
            config[
                "stop_buffer_ticks"
            ]
            * tick
        )
    )

    trade_risk = (
        reference_entry
        - stop
    )

    target = (
        reference_entry
        + (
            trade_risk
            * config[
                "reward_risk"
            ]
        )
    )

    return {

        "instrument":
            instrument,

        "signal_start_utc":
            signal_result[
                "signal_start_utc"
            ],

        "entry_time_utc":
            signal_result[
                "signal_close_utc"
            ],

        "reference_entry":
            round_price(
                reference_entry,
                config
            ),

        "backtest_entry":
            round_price(
                backtest_entry,
                config
            ),

        "stop":
            round_price(
                stop,
                config
            ),

        "target":
            round_price(
                target,
                config
            ),

        "exit_bar_start_utc":
            None,

        "exit_time_utc":
            None,

        "exit_reason":
            None
    }


def determine_exit_on_bar(
    trade,
    candle
):

    stop = (
        trade[
            "stop"
        ]
    )

    target = (
        trade[
            "target"
        ]
    )

    stop_touched = (
        candle["low"]
        <= stop
    )

    target_touched = (
        candle["high"]
        >= target
    )

    if (
        not stop_touched
        and
        not target_touched
    ):

        return None

    if (
        stop_touched
        and
        not target_touched
    ):

        return "STOP"

    if (
        target_touched
        and
        not stop_touched
    ):

        return "TARGET"

    # TradingView historical broker-emulator
    # same-bar path approximation.
    distance_to_high = abs(
        candle["high"]
        - candle["open"]
    )

    distance_to_low = abs(
        candle["open"]
        - candle["low"]
    )

    if (
        distance_to_high
        < distance_to_low
    ):

        return "TARGET"

    return "STOP"


def simulate_trades(
    instrument,
    h1,
    atr,
    daily_state,
    start,
    end
):

    config = (
        STRATEGIES[
            instrument
        ]
    )

    raw_signals = []
    trades = []
    ignored_signals = []

    open_trade = None

    start_index = max(

        config[
            "atr_length"
        ],

        config[
            "structure_lookback"
        ]
    )

    evaluated_bars = 0

    for index in range(
        start_index,
        len(h1)
    ):

        candle = (
            h1[index]
        )

        candle_time = (
            candle["time"]
        )

        if candle_time < start:

            continue

        if candle_time >= end:

            break

        evaluated_bars += 1

        # ==========================================
        # EXISTING POSITION
        # ==========================================

        if open_trade is not None:

            exit_reason = (
                determine_exit_on_bar(
                    open_trade,
                    candle
                )
            )

            if exit_reason is not None:

                open_trade[
                    "exit_reason"
                ] = exit_reason

                open_trade[
                    "exit_bar_start_utc"
                ] = candle_time

                open_trade[
                    "exit_time_utc"
                ] = (
                    candle_time
                    + timedelta(
                        hours=1
                    )
                )

                open_trade = None

        # ==========================================
        # SIGNAL
        # ==========================================

        result = (
            evaluate_signal_at_index(
                instrument,
                h1,
                atr,
                index,
                daily_state
            )
        )

        if (
            result is None
            or
            not result[
                "qualified"
            ]
        ):

            continue

        raw_signals.append(
            result
        )

        # ==========================================
        # PYRAMIDING=0
        # ==========================================

        if open_trade is not None:

            ignored_signals.append({

                "signal_start_utc":
                    result[
                        "signal_start_utc"
                    ],

                "signal_close_utc":
                    result[
                        "signal_close_utc"
                    ],

                "reason":
                    "POSITION_ALREADY_OPEN",

                "existing_trade_entry_time":
                    open_trade[
                        "entry_time_utc"
                    ]
            })

            continue

        new_trade = (
            create_trade(
                instrument,
                result
            )
        )

        trades.append(
            new_trade
        )

        open_trade = (
            new_trade
        )

    return {

        "evaluated_bars":
            evaluated_bars,

        "raw_signals":
            raw_signals,

        "trades":
            trades,

        "ignored_signals":
            ignored_signals,

        "position_still_open_at_end":
            open_trade is not None
    }

# ----------------------------------------------------------------
# Faster exact-equivalent daily lookup.
#
# The live historical helper linearly scans the daily list. That is fine for
# short live windows but prohibitively slow over ~24 years. These overrides
# preserve the same strict `row["time"] < current_daily_start(signal_time)`
# semantics and readiness rules using bisect.
# ----------------------------------------------------------------

_H1_DAILY_TIMES = {}

def register_h1_daily_state(daily_state):
    _H1_DAILY_TIMES[id(daily_state)] = [row["time"] for row in daily_state]


def previous_daily_values(signal_time, daily_state, config):
    times = _H1_DAILY_TIMES.get(id(daily_state))
    if times is None:
        times = [row["time"] for row in daily_state]
        _H1_DAILY_TIMES[id(daily_state)] = times

    session_start = current_daily_start(signal_time)
    j = bisect.bisect_left(times, session_start) - 1

    require_fast = config["require_daily_fast_above_slow"]
    require_daily_close_ema = config.get("daily_close_ema") is not None

    while j >= 0:
        row = daily_state[j]
        slow_ready = row["slow_ema"] is not None
        fast_ready = (not require_fast) or row["fast_ema"] is not None
        close_ema_ready = (
            (not require_daily_close_ema)
            or row["daily_close_ema"] is not None
        )
        if slow_ready and fast_ready and close_ema_ready:
            return row
        j -= 1

    return None


def previous_short_daily_values(signal_time, daily_state):
    times = _H1_DAILY_TIMES.get(id(daily_state))
    if times is None:
        times = [row["time"] for row in daily_state]
        _H1_DAILY_TIMES[id(daily_state)] = times

    session_start = current_daily_start(signal_time)
    j = bisect.bisect_left(times, session_start) - 1

    while j >= 0:
        row = daily_state[j]
        ready = (
            row["fast_ema"] is not None
            and row["slow_ema"] is not None
            and row["daily_atr"] is not None
        )
        if ready:
            return row
        j -= 1

    return None


# ----------------------------------------------------------------
# Proven short historical mechanics + H1 R conversion.
# ----------------------------------------------------------------

def portfolio_short_trade_from_signal(
    instrument,
    signal_result
):
    config = SHORT_STRATEGIES[instrument]
    tick = config["tick_size"]

    reference_entry = signal_result["close"]

    # Locked research convention:
    # adverse short fill = signal close - 5 ticks.
    backtest_entry = (
        reference_entry
        - BACKTEST_SLIPPAGE_TICKS * tick
    )

    stop = (
        signal_result["high"]
        + config["stop_buffer_ticks"] * tick
    )

    reference_risk = stop - reference_entry

    if reference_risk <= 0:
        raise RuntimeError(
            f"Invalid short reference risk for {instrument}"
        )

    target = (
        reference_entry
        - reference_risk * config["reward_risk"]
    )

    return {
        "instrument": instrument,
        "side": "SELL",
        "strategy": "SHORT",
        "signal_start_utc": signal_result["signal_start_utc"],
        "entry_time_utc": signal_result["signal_close_utc"],
        "reference_entry": round_price(reference_entry, config),
        "backtest_entry": round_price(backtest_entry, config),
        "stop": round_price(stop, config),
        "target": round_price(target, config),
        "exit_bar_start_utc": None,
        "exit_time_utc": None,
        "exit_reason": None,
    }


def portfolio_short_exit_on_bar(
    trade,
    candle
):
    stop_touched = (
        candle["high"] >= trade["stop"]
    )

    target_touched = (
        candle["low"] <= trade["target"]
    )

    if not stop_touched and not target_touched:
        return None

    if stop_touched and not target_touched:
        return "STOP"

    if target_touched and not stop_touched:
        return "TARGET"

    # Locked short same-bar rule:
    # if high is closer to candle open, stop is assumed first;
    # otherwise target is assumed first.
    distance_to_high = abs(
        candle["high"] - candle["open"]
    )

    distance_to_low = abs(
        candle["open"] - candle["low"]
    )

    if distance_to_high < distance_to_low:
        return "STOP"

    return "TARGET"


def portfolio_simulate_shorts(
    instrument,
    h1,
    atr,
    daily_state,
    start,
    end
):
    config = SHORT_STRATEGIES[instrument]

    required_lookbacks = [
        config["atr_length"],
        config["structure_lookback"],
    ]

    momentum_lookback = config.get(
        "momentum_lookback_bars"
    )

    if momentum_lookback is not None:
        required_lookbacks.append(
            momentum_lookback
        )

    momentum_requirements = config.get(
        "momentum_requirements",
        {}
    )

    for lookback_bars in momentum_requirements.keys():
        required_lookbacks.append(
            int(lookback_bars)
        )

    if config.get(
        "minimum_h1_atr_ratio_50"
    ) is not None:
        required_lookbacks.append(50)

    start_index = max(
        required_lookbacks
    )

    trades = []
    open_trade = None
    raw_signal_count = 0
    ignored_signal_count = 0

    for index in range(
        start_index,
        len(h1)
    ):
        candle = h1[index]
        candle_time = candle["time"]

        if candle_time < start:
            continue

        if candle_time >= end:
            break

        # Existing short position is evaluated first.
        # This preserves the locked convention that a new
        # signal on the exact H1 candle where the old trade
        # exits is allowed.
        if open_trade is not None:
            exit_reason = (
                portfolio_short_exit_on_bar(
                    open_trade,
                    candle
                )
            )

            if exit_reason is not None:
                open_trade["exit_reason"] = (
                    exit_reason
                )
                open_trade[
                    "exit_bar_start_utc"
                ] = candle_time
                open_trade[
                    "exit_time_utc"
                ] = (
                    candle_time
                    + timedelta(hours=1)
                )
                open_trade = None

        result = evaluate_short_signal_at_index(
            instrument,
            h1,
            atr,
            index,
            daily_state
        )

        if (
            result is None
            or not result["qualified"]
        ):
            continue

        raw_signal_count += 1

        if open_trade is not None:
            ignored_signal_count += 1
            continue

        new_trade = (
            portfolio_short_trade_from_signal(
                instrument,
                result
            )
        )

        trades.append(
            new_trade
        )

        open_trade = (
            new_trade
        )

    return {
        "trades": trades,
        "raw_signal_count": raw_signal_count,
        "ignored_signal_count": ignored_signal_count,
        "position_still_open_at_end": (
            open_trade is not None
        ),
    }


def portfolio_long_trade_r(
    trade
):
    if trade["exit_reason"] not in {
        "TARGET",
        "STOP",
    }:
        return None

    entry = float(
        trade["backtest_entry"]
    )
    stop = float(
        trade["stop"]
    )
    target = float(
        trade["target"]
    )

    actual_risk = entry - stop

    if actual_risk <= 0:
        return None

    if trade["exit_reason"] == "STOP":
        exit_price = stop
    else:
        exit_price = target

    return (
        exit_price - entry
    ) / actual_risk


def portfolio_short_trade_r(
    trade
):
    if trade["exit_reason"] not in {
        "TARGET",
        "STOP",
    }:
        return None

    entry = float(
        trade["backtest_entry"]
    )
    stop = float(
        trade["stop"]
    )
    target = float(
        trade["target"]
    )

    actual_risk = stop - entry

    if actual_risk <= 0:
        return None

    if trade["exit_reason"] == "STOP":
        exit_price = stop
    else:
        exit_price = target

    return (
        entry - exit_price
    ) / actual_risk


def h1_standardise_trade(pair, side, trade, result_r, reward_risk):
    signal_time = trade["signal_start_utc"]
    entry_time = trade["entry_time_utc"]
    exit_bar_start = trade["exit_bar_start_utc"]
    exit_event = trade["exit_time_utc"]

    sid = f"{pair}_H1_{'LONG' if side == 'BUY' else 'SHORT'}"

    return {
        "pair": pair,
        "strategy_id": sid,
        "trigger": "LIVE_H1_LONG" if side == "BUY" else "LIVE_H1_SHORT",
        "timeframe": "H1",
        "side": side,
        "signal_time": signal_time,
        "entry_time": entry_time,
        "exit_time": exit_bar_start,
        "exit_event_time": exit_event,
        "rr": float(reward_risk),
        "reference_entry": float(trade["reference_entry"]),
        "historical_fill": float(trade["backtest_entry"]),
        "stop": float(trade["stop"]),
        "target": float(trade["target"]),
        "result": trade["exit_reason"],
        "r": float(result_r),
        "cost_model": "H1_5_ADVERSE_TICKS",
        "baseline_cost_value": float(BACKTEST_SLIPPAGE_TICKS),
    }


def collect_h1_pair(pair):
    h1, h1_empty = fetch_history(pair, "H1", H1_DATA_WARMUP, NOW)
    daily, d_empty = fetch_history(pair, "D", H1_DAILY_WARMUP, NOW)

    if len(h1) < 90000:
        raise RuntimeError(
            f"Incomplete H1 history for {pair}: {len(h1)} candles; "
            f"first={iso(h1[0]['time']) if h1 else 'NONE'}"
        )
    if len(daily) < 5000:
        raise RuntimeError(
            f"Incomplete daily history for {pair}: {len(daily)} candles; "
            f"first={iso(daily[0]['time']) if daily else 'NONE'}"
        )

    out = {}
    meta = []

    long_cfg = STRATEGIES[pair]
    long_atr = atr_series(h1, long_cfg["atr_length"])
    long_daily_state = build_daily_state(daily, long_cfg)
    register_h1_daily_state(long_daily_state)

    long_sim = simulate_trades(
        pair, h1, long_atr, long_daily_state, START, NOW
    )

    long_trades = []
    for tr in long_sim["trades"]:
        result_r = portfolio_long_trade_r(tr)
        if result_r is None:
            continue
        long_trades.append(
            h1_standardise_trade(
                pair, "BUY", tr, result_r, long_cfg["reward_risk"]
            )
        )

    long_sid = f"{pair}_H1_LONG"
    out[long_sid] = long_trades
    meta.append({
        "strategy_id": long_sid,
        "raw_signals": len(long_sim["raw_signals"]),
        "ignored_while_own_position_open": len(long_sim["ignored_signals"]),
        "position_still_open_at_end": long_sim["position_still_open_at_end"],
    })

    short_cfg = SHORT_STRATEGIES[pair]
    short_atr = atr_series(h1, short_cfg["atr_length"])
    short_daily_state = build_short_daily_state(daily, short_cfg)
    register_h1_daily_state(short_daily_state)

    short_sim = portfolio_simulate_shorts(
        pair, h1, short_atr, short_daily_state, START, NOW
    )

    short_trades = []
    for tr in short_sim["trades"]:
        result_r = portfolio_short_trade_r(tr)
        if result_r is None:
            continue
        short_trades.append(
            h1_standardise_trade(
                pair, "SELL", tr, result_r, short_cfg["reward_risk"]
            )
        )

    short_sid = f"{pair}_H1_SHORT"
    out[short_sid] = short_trades
    meta.append({
        "strategy_id": short_sid,
        "raw_signals": short_sim["raw_signal_count"],
        "ignored_while_own_position_open": short_sim["ignored_signal_count"],
        "position_still_open_at_end": short_sim["position_still_open_at_end"],
    })

    coverage = {
        "pair": pair,
        "h1_requested_start_utc": iso(H1_DATA_WARMUP),
        "h1_actual_first_utc": iso(h1[0]["time"]),
        "h1_actual_last_utc": iso(h1[-1]["time"]),
        "h1_candles": len(h1),
        "daily_requested_start_utc": iso(H1_DAILY_WARMUP),
        "daily_actual_first_utc": iso(daily[0]["time"]),
        "daily_actual_last_utc": iso(daily[-1]["time"]),
        "daily_candles": len(daily),
        "empty_h1_chunks": h1_empty,
        "empty_daily_chunks": d_empty,
    }

    # Prevent id-cache growth after the pair is finished.
    _H1_DAILY_TIMES.pop(id(long_daily_state), None)
    _H1_DAILY_TIMES.pop(id(short_daily_state), None)

    return out, meta, coverage


def load_m15_baseline_trades():
    path = OUT["trades"]
    if not os.path.exists(path):
        raise RuntimeError(
            "M15 baseline trade CSV is missing after the embedded M15 run"
        )

    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for raw in csv.DictReader(f):
            signal_time = parse_time(raw["signal_time"])
            exit_bar_start = parse_time(raw["exit_time"])
            rows.append({
                "pair": raw["pair"],
                "strategy_id": raw["strategy_id"],
                "trigger": raw["trigger"],
                "timeframe": "M15",
                "side": raw["side"],
                "signal_time": signal_time,
                # Signal is only knowable at the end of its M15 candle.
                "entry_time": signal_time + timedelta(minutes=15),
                "exit_time": exit_bar_start,
                # Exit bar is only resolved at bar granularity; use its close
                # as the event timestamp so exact-exit-candle replacement is
                # handled consistently with the H1 engine.
                "exit_event_time": exit_bar_start + timedelta(minutes=15),
                "rr": float(raw["rr"]),
                "reference_entry": float(raw["reference_entry"]),
                "historical_fill": float(raw["historical_fill"]),
                "stop": float(raw["stop"]),
                "target": float(raw["target"]),
                "result": raw["result"],
                "r": float(raw["r"]),
                "cost_model": "M15_1_ADVERSE_PIP",
                "baseline_cost_value": float(raw.get("cost_pips") or 1.0),
            })
    return rows


def actual_intervals_overlap(a, b):
    return (
        a["entry_time"] < b["exit_event_time"]
        and b["entry_time"] < a["exit_event_time"]
    )


def actual_overlap_rows(strategy_trades):
    ids = sorted(strategy_trades)
    out = []
    for ia, a in enumerate(ids):
        for b in ids[ia + 1:]:
            ta = strategy_trades[a]
            tb = strategy_trades[b]
            ca = sum(any(actual_intervals_overlap(x, y) for y in tb) for x in ta)
            cb = sum(any(actual_intervals_overlap(y, x) for x in ta) for y in tb)
            pa = ta[0]["pair"] if ta else a.split("_H1")[0].split("_M15")[0]
            pb = tb[0]["pair"] if tb else b.split("_H1")[0].split("_M15")[0]
            out.append({
                "strategy_a": a,
                "strategy_b": b,
                "a_trades_overlapping_b": ca,
                "b_trades_overlapping_a": cb,
                "same_pair": pa == pb,
                "timeframe_a": ta[0]["timeframe"] if ta else "",
                "timeframe_b": tb[0]["timeframe"] if tb else "",
            })
    return out


def actual_concurrency_rows(scope, all_trades):
    events = []
    for t in all_trades:
        events.append((t["entry_time"], 1, t["strategy_id"], t["pair"], t["timeframe"]))
        events.append((t["exit_event_time"], -1, t["strategy_id"], t["pair"], t["timeframe"]))

    # Exit before entry at the same timestamp.
    events.sort(key=lambda z: (z[0], z[1], z[2]))

    active = 0
    max_active = 0
    hist = defaultdict(float)
    last = None

    for ts, delta, sid, pair, tf in events:
        if last is not None and ts > last:
            hist[active] += (ts - last).total_seconds()
        active += delta
        max_active = max(max_active, active)
        last = ts

    total = sum(hist.values())
    rows = [
        {"scope": scope, "metric": "max_concurrent_positions", "value": max_active}
    ]
    for k in sorted(hist):
        rows.append({
            "scope": scope,
            "metric": f"pct_time_{k}_positions",
            "value": pct(hist[k], total),
        })

    clusters = defaultdict(int)
    for t in all_trades:
        clusters[t["entry_time"]] += 1

    rows.append({
        "scope": scope,
        "metric": "entry_timestamps_with_2plus_trades",
        "value": sum(v >= 2 for v in clusters.values()),
    })
    rows.append({
        "scope": scope,
        "metric": "max_same_timestamp_entries",
        "value": max(clusters.values()) if clusters else 0,
    })
    return rows


def apply_pair_gate(trades, priority):
    """
    Apply the current live-engine style instrument gate:
    no new trade on a pair while any accepted trade on that pair is open.

    priority:
        H1_FIRST or M15_FIRST for exact same-timestamp conflicts.
        BUY precedes SELL within a timeframe, matching the current H1
        watcher ordering where longs are processed before shorts.
    """
    if priority not in {"H1_FIRST", "M15_FIRST"}:
        raise ValueError(priority)

    tf_order = (
        {"H1": 0, "M15": 1}
        if priority == "H1_FIRST"
        else {"M15": 0, "H1": 1}
    )
    side_order = {"BUY": 0, "SELL": 1}

    candidates = sorted(
        trades,
        key=lambda t: (
            t["entry_time"],
            tf_order[t["timeframe"]],
            side_order.get(t["side"], 9),
            t["strategy_id"],
        ),
    )

    active = {}
    accepted = []
    rejected = []

    for t in candidates:
        pair = t["pair"]
        cur = active.get(pair)

        if cur is not None and cur["exit_event_time"] <= t["entry_time"]:
            active.pop(pair, None)
            cur = None

        if cur is not None:
            rejected.append({
                "priority_mode": priority,
                "pair": pair,
                "rejected_strategy_id": t["strategy_id"],
                "rejected_timeframe": t["timeframe"],
                "rejected_side": t["side"],
                "rejected_entry_time": iso(t["entry_time"]),
                "blocking_strategy_id": cur["strategy_id"],
                "blocking_timeframe": cur["timeframe"],
                "blocking_side": cur["side"],
                "blocking_entry_time": iso(cur["entry_time"]),
                "blocking_exit_time": iso(cur["exit_event_time"]),
            })
            continue

        accepted.append(t)
        active[pair] = t

    accepted.sort(key=lambda t: (t["signal_time"], t["strategy_id"]))
    return accepted, rejected


def serialise_trade(t):
    row = dict(t)
    for k in ("signal_time", "entry_time", "exit_time", "exit_event_time"):
        if isinstance(row.get(k), datetime):
            row[k] = iso(row[k])
    return row


def recalc_trade_cost_multiplier(t, multiplier):
    """
    Stress each timeframe relative to its native historical baseline:
      H1 1.0x = 5 adverse ticks
      M15 1.0x = 1 adverse pip

    Signal, stop, target, result and exit timing stay unchanged; only actual
    R from the adverse fill changes.
    """
    x = dict(t)
    pair = x["pair"]
    tick = PAIR_META[pair]["tick"]
    pip = PAIR_META[pair]["pip"]

    if x["timeframe"] == "H1":
        precision = 3 if pair == "USD_JPY" else 5
        slip = BACKTEST_SLIPPAGE_TICKS * multiplier * tick
        fill = (
            x["reference_entry"] + slip
            if x["side"] == "BUY"
            else x["reference_entry"] - slip
        )
        fill = round(fill, precision)
        x["cost_stress_value"] = BACKTEST_SLIPPAGE_TICKS * multiplier
        x["cost_stress_unit"] = "ticks"
    else:
        slip = multiplier * pip
        fill = (
            x["reference_entry"] + slip
            if x["side"] == "BUY"
            else x["reference_entry"] - slip
        )
        x["cost_stress_value"] = multiplier
        x["cost_stress_unit"] = "pips"

    stop = x["stop"]
    target = x["target"]

    if x["side"] == "BUY":
        actual_risk = fill - stop
        r = -1.0 if x["result"] == "STOP" else (target - fill) / actual_risk
    else:
        actual_risk = stop - fill
        r = -1.0 if x["result"] == "STOP" else (fill - target) / actual_risk

    if actual_risk <= 0:
        raise RuntimeError(
            f"Invalid stressed risk {x['strategy_id']} multiplier={multiplier}"
        )

    x["historical_fill"] = float(fill)
    x["r"] = float(r)
    x["cost_multiplier"] = float(multiplier)
    return x


def simulate_mixed_equity(
    trades,
    h1_risk_fraction,
    m15_risk_fraction,
    starting_balance=100.0,
):
    """
    Event-driven mixed-timeframe compounding.

    Each new trade risks its timeframe-specific percentage of then-REALISED
    equity. Existing positions retain their entry cash risk. The real OANDA
    executor sizes from current NAV, including unrealised P/L; that cannot be
    reconstructed exactly from OHLC outcome-only trade records.

    Exits are processed before entries at the exact same timestamp.
    """
    if not trades:
        return {
            "summary": {
                "h1_risk_pct": h1_risk_fraction * 100,
                "m15_risk_pct": m15_risk_fraction * 100,
                "starting_balance": starting_balance,
                "ending_balance": starting_balance,
                "ending_multiple": 1.0,
                "total_return_pct": 0.0,
                "cagr_pct": 0.0,
                "max_closed_equity_dd_pct": 0.0,
                "max_open_risk_floor_dd_pct": 0.0,
                "max_open_positions": 0,
                "max_open_risk_pct_of_realised_equity": 0.0,
                "trades": 0,
            },
            "curve": [],
            "trade_rows": [],
            "exit_times": [],
            "exit_balances": [],
        }

    ordered = sorted(trades, key=lambda t: (t["entry_time"], t["strategy_id"]))

    events = []
    for n, t in enumerate(ordered):
        key = (t["strategy_id"], t["entry_time"], t["exit_event_time"], n)
        events.append((t["entry_time"], 1, t["strategy_id"], key, t))
        events.append((t["exit_event_time"], 0, t["strategy_id"], key, t))

    # 0 exit, 1 entry
    events.sort(key=lambda e: (e[0], e[1], e[2], e[3]))

    balance = float(starting_balance)
    peak = balance
    max_closed_dd = 0.0
    max_floor_dd = 0.0
    open_trades = {}
    open_risk_cash = 0.0
    max_open_positions = 0
    max_open_risk_pct = 0.0
    curve = []
    trade_rows = []
    exit_times = []
    exit_balances = []

    for ts, kind, sid, key, t in events:
        if kind == 0:
            rec = open_trades.pop(key, None)
            if rec is None:
                raise RuntimeError(
                    f"Mixed equity exit without entry: {sid} {iso(ts)}"
                )

            pnl_cash = rec["risk_cash"] * float(t["r"])
            before = balance
            balance += pnl_cash
            open_risk_cash -= rec["risk_cash"]
            if abs(open_risk_cash) < 1e-12:
                open_risk_cash = 0.0

            peak = max(peak, balance)
            closed_dd = ((balance / peak) - 1.0) * 100.0 if peak > 0 else -100.0
            max_closed_dd = min(max_closed_dd, closed_dd)

            floor_equity = balance - open_risk_cash
            floor_dd = ((floor_equity / peak) - 1.0) * 100.0 if peak > 0 else -100.0
            max_floor_dd = min(max_floor_dd, floor_dd)

            exit_times.append(ts)
            exit_balances.append(balance)

            trade_rows.append({
                "h1_risk_pct": h1_risk_fraction * 100.0,
                "m15_risk_pct": m15_risk_fraction * 100.0,
                "pair": t["pair"],
                "strategy_id": t["strategy_id"],
                "timeframe": t["timeframe"],
                "side": t["side"],
                "entry_time": iso(t["entry_time"]),
                "exit_time": iso(t["exit_event_time"]),
                "r": t["r"],
                "result": t["result"],
                "entry_realised_equity": rec["entry_equity"],
                "risk_fraction": rec["risk_fraction"],
                "risk_cash": rec["risk_cash"],
                "pnl_cash": pnl_cash,
                "balance_before_exit": before,
                "balance_after_exit": balance,
                "closed_equity_drawdown_pct": closed_dd,
                "open_positions_after_exit": len(open_trades),
                "open_risk_cash_after_exit": open_risk_cash,
                "open_risk_floor_equity": floor_equity,
                "open_risk_floor_drawdown_pct": floor_dd,
            })

            curve.append({
                "time_utc": iso(ts),
                "event": "EXIT",
                "strategy_id": sid,
                "balance": balance,
                "peak_balance": peak,
                "closed_equity_drawdown_pct": closed_dd,
                "open_positions": len(open_trades),
                "open_risk_cash": open_risk_cash,
                "open_risk_floor_equity": floor_equity,
                "open_risk_floor_drawdown_pct": floor_dd,
            })

        else:
            risk_fraction = (
                h1_risk_fraction
                if t["timeframe"] == "H1"
                else m15_risk_fraction
            )

            if balance <= 0:
                raise RuntimeError(
                    f"Equity depleted before {sid} at {iso(ts)}"
                )

            risk_cash = balance * risk_fraction
            open_trades[key] = {
                "risk_cash": risk_cash,
                "risk_fraction": risk_fraction,
                "entry_equity": balance,
            }
            open_risk_cash += risk_cash
            max_open_positions = max(max_open_positions, len(open_trades))

            open_risk_pct = (
                (open_risk_cash / balance) * 100.0
                if balance > 0
                else 999.0
            )
            max_open_risk_pct = max(max_open_risk_pct, open_risk_pct)

            floor_equity = balance - open_risk_cash
            floor_dd = ((floor_equity / peak) - 1.0) * 100.0 if peak > 0 else -100.0
            max_floor_dd = min(max_floor_dd, floor_dd)

            curve.append({
                "time_utc": iso(ts),
                "event": "ENTRY",
                "strategy_id": sid,
                "balance": balance,
                "peak_balance": peak,
                "closed_equity_drawdown_pct": ((balance / peak) - 1.0) * 100.0,
                "open_positions": len(open_trades),
                "open_risk_cash": open_risk_cash,
                "open_risk_pct_of_realised_equity": open_risk_pct,
                "open_risk_floor_equity": floor_equity,
                "open_risk_floor_drawdown_pct": floor_dd,
            })

    if open_trades:
        raise RuntimeError(
            f"Mixed equity finished with {len(open_trades)} open trades"
        )

    first_entry = min(t["entry_time"] for t in ordered)
    last_exit = max(t["exit_event_time"] for t in ordered)
    years = max(
        (last_exit - first_entry).total_seconds() / (365.2425 * 86400.0),
        1e-9,
    )
    total_return_pct = ((balance / starting_balance) - 1.0) * 100.0
    cagr_pct = (
        ((balance / starting_balance) ** (1.0 / years) - 1.0) * 100.0
        if balance > 0 and starting_balance > 0
        else -100.0
    )

    return {
        "summary": {
            "h1_risk_pct": h1_risk_fraction * 100.0,
            "m15_risk_pct": m15_risk_fraction * 100.0,
            "starting_balance": starting_balance,
            "ending_balance": balance,
            "ending_multiple": balance / starting_balance,
            "total_return_pct": total_return_pct,
            "cagr_pct": cagr_pct,
            "simulation_start_utc": iso(first_entry),
            "simulation_end_utc": iso(last_exit),
            "simulation_years": years,
            "trades": len(ordered),
            "max_closed_equity_dd_pct": max_closed_dd,
            "max_open_risk_floor_dd_pct": max_floor_dd,
            "max_open_positions": max_open_positions,
            "max_open_risk_pct_of_realised_equity": max_open_risk_pct,
        },
        "curve": curve,
        "trade_rows": trade_rows,
        "exit_times": exit_times,
        "exit_balances": exit_balances,
    }


def mixed_balance_before(sim, ts):
    j = bisect.bisect_left(sim["exit_times"], ts) - 1
    return sim["exit_balances"][j] if j >= 0 else STARTING_BALANCE


def mixed_equity_period_row(
    mode,
    sim,
    h1_risk,
    m15_risk,
    label,
    start,
    end,
    trades,
):
    sb = mixed_balance_before(sim, start)
    eb = mixed_balance_before(sim, end)
    exits = [t for t in trades if start <= t["exit_event_time"] < end]
    ret = ((eb / sb) - 1.0) * 100.0 if sb > 0 else 0.0
    years = max((end - start).total_seconds() / (365.2425 * 86400.0), 1e-9)
    ann = (
        ((eb / sb) ** (1.0 / years) - 1.0) * 100.0
        if sb > 0 and eb > 0
        else 0.0
    )
    return {
        "portfolio_mode": mode,
        "h1_risk_pct": h1_risk * 100.0,
        "m15_risk_pct": m15_risk * 100.0,
        "period": label,
        "start_utc": iso(start),
        "end_utc": iso(end),
        "start_balance": sb,
        "end_balance": eb,
        "compounded_return_pct": ret,
        "annualized_return_pct": ann,
        "realized_exits": len(exits),
    }


def mixed_equity_calendar_rows(
    mode, sim, h1_risk, m15_risk, trades, first_year, last_year
):
    rows = []
    for y in range(first_year, last_year + 1):
        a = datetime(y, 1, 1, tzinfo=timezone.utc)
        nominal_b = datetime(y + 1, 1, 1, tzinfo=timezone.utc)
        b = min(nominal_b, NOW)
        if b <= a:
            continue
        row = mixed_equity_period_row(
            mode, sim, h1_risk, m15_risk, str(y), a, b, trades
        )
        row["year"] = y
        row["complete_year"] = nominal_b <= NOW
        rows.append(row)
    return rows


def mixed_equity_calendar_summary(rows):
    grouped = defaultdict(list)
    for r in rows:
        key = (
            r["portfolio_mode"],
            r["h1_risk_pct"],
            r["m15_risk_pct"],
        )
        grouped[key].append(r)

    out = []
    for key, g in grouped.items():
        mode, h1p, m15p = key
        complete = [x for x in g if x["complete_year"]]
        active = [x for x in complete if x["realized_exits"] > 0]
        positive = [x for x in active if x["compounded_return_pct"] > 0]
        worst = min(active, key=lambda x: x["compounded_return_pct"]) if active else None
        best = max(active, key=lambda x: x["compounded_return_pct"]) if active else None
        out.append({
            "portfolio_mode": mode,
            "h1_risk_pct": h1p,
            "m15_risk_pct": m15p,
            "completed_years": len(complete),
            "active_completed_years": len(active),
            "positive_active_completed_years": len(positive),
            "positive_active_completed_years_pct": pct(len(positive), len(active)),
            "average_return_pct_active": (
                sum(x["compounded_return_pct"] for x in active) / len(active)
                if active else 0.0
            ),
            "median_return_pct_active": safe_median(
                x["compounded_return_pct"] for x in active
            ),
            "worst_year": worst["year"] if worst else "",
            "worst_year_return_pct": worst["compounded_return_pct"] if worst else 0.0,
            "best_year": best["year"] if best else "",
            "best_year_return_pct": best["compounded_return_pct"] if best else 0.0,
        })
    return out


def mixed_equity_rolling_rows(
    mode,
    sim,
    h1_risk,
    m15_risk,
    trades,
    months,
    start_month,
    end_complete_month,
):
    rows = []
    cur = start_month
    while add_months(cur, months) <= end_complete_month:
        end = add_months(cur, months)
        row = mixed_equity_period_row(
            mode, sim, h1_risk, m15_risk,
            f"ROLLING_{months}M", cur, end, trades
        )
        row["months"] = months
        rows.append(row)
        cur = add_months(cur, 1)
    return rows


def mixed_equity_rolling_summary(rows):
    grouped = defaultdict(list)
    for r in rows:
        key = (
            r["portfolio_mode"],
            r["h1_risk_pct"],
            r["m15_risk_pct"],
            r["months"],
        )
        grouped[key].append(r)

    out = []
    for key, g in grouped.items():
        mode, h1p, m15p, months = key
        active = [x for x in g if x["realized_exits"] > 0]
        positive = [x for x in active if x["compounded_return_pct"] > 0]
        worst = min(active, key=lambda x: x["compounded_return_pct"]) if active else None
        best = max(active, key=lambda x: x["compounded_return_pct"]) if active else None
        out.append({
            "portfolio_mode": mode,
            "h1_risk_pct": h1p,
            "m15_risk_pct": m15p,
            "months": months,
            "windows": len(g),
            "active_windows": len(active),
            "zero_exit_windows": len(g) - len(active),
            "positive_active_windows": len(positive),
            "positive_active_windows_pct": pct(len(positive), len(active)),
            "median_compounded_return_pct_active": safe_median(
                x["compounded_return_pct"] for x in active
            ),
            "worst_compounded_return_pct": (
                worst["compounded_return_pct"] if worst else 0.0
            ),
            "worst_start_utc": worst["start_utc"] if worst else "",
            "best_compounded_return_pct": (
                best["compounded_return_pct"] if best else 0.0
            ),
            "best_start_utc": best["start_utc"] if best else "",
        })
    return out


def combined_monthly_matrix(strategy_trades, start_month, end_month):
    ids = sorted(strategy_trades)
    rows = []
    cur = start_month
    while cur < end_month:
        nxt = add_months(cur, 1)
        row = {"month": cur.strftime("%Y-%m")}
        for sid in ids:
            row[sid] = sum(
                t["r"]
                for t in strategy_trades[sid]
                if cur <= t["signal_time"] < nxt
            )
        row["PORTFOLIO"] = sum(row[sid] for sid in ids)
        rows.append(row)
        cur = nxt
    return rows


def combined_json_safe(value):
    if isinstance(value, set):
        return sorted(value)
    if isinstance(value, dict):
        return {k: combined_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [combined_json_safe(v) for v in value]
    return value


def run_combined_research():
    try:
        COMBINED_STATUS.update(
            state="starting",
            message="Running exact 10-strategy M15 rebuild first",
            progress=1,
        )

        # ------------------------------------------------------------
        # 1) Re-run the exact already-verified M15 engine.
        # ------------------------------------------------------------
        run_research()
        if STATUS.get("state") != "complete":
            raise RuntimeError(
                "Embedded M15 rebuild failed: " + json.dumps(STATUS, default=str)
            )

        m15_trades = load_m15_baseline_trades()
        if len(m15_trades) < M15_REFERENCE_PORTFOLIO_TRADES:
            raise RuntimeError(
                f"M15 portfolio below known reference: "
                f"{len(m15_trades)} < {M15_REFERENCE_PORTFOLIO_TRADES}"
            )

        COMBINED_STATUS.update(
            state="h1_download",
            message="Rebuilding the exact current-live 10-strategy H1 portfolio",
            progress=55,
        )

        # ------------------------------------------------------------
        # 2) Rebuild exact current-live H1 portfolio from attached rules.
        # ------------------------------------------------------------
        h1_by_strategy = {}
        h1_meta = []
        h1_coverage = []

        for pi, pair in enumerate(PAIRS):
            COMBINED_STATUS.update(
                state="h1_rebuild",
                message=f"Rebuilding current-live H1 {pair} long + short",
                progress=57 + pi * 5,
            )
            pair_trades, pair_meta, cov = collect_h1_pair(pair)
            h1_by_strategy.update(pair_trades)
            h1_meta.extend(pair_meta)
            h1_coverage.append(cov)
            gc.collect()

        h1_trades = sorted(
            [t for sid in sorted(h1_by_strategy) for t in h1_by_strategy[sid]],
            key=lambda t: (t["signal_time"], t["strategy_id"]),
        )

        if len(h1_trades) < H1_REFERENCE_FULL_TRADES:
            raise RuntimeError(
                f"H1 portfolio reproduction below prior final-live reference: "
                f"{len(h1_trades)} < {H1_REFERENCE_FULL_TRADES}. "
                f"Do not trust combined results until this is reconciled."
            )

        COMBINED_STATUS.update(
            state="merging",
            message="Merging H1 + M15 and applying live instrument-gate variants",
            progress=83,
        )

        # ------------------------------------------------------------
        # 3) Unified strategy dictionaries + portfolio modes.
        # ------------------------------------------------------------
        m15_by_strategy = defaultdict(list)
        for t in m15_trades:
            m15_by_strategy[t["strategy_id"]].append(t)

        all_by_strategy = {}
        all_by_strategy.update({k: list(v) for k, v in h1_by_strategy.items()})
        all_by_strategy.update({k: list(v) for k, v in m15_by_strategy.items()})

        independent = sorted(
            h1_trades + m15_trades,
            key=lambda t: (t["signal_time"], t["strategy_id"]),
        )

        gated_h1, rejected_h1 = apply_pair_gate(independent, "H1_FIRST")
        gated_m15, rejected_m15 = apply_pair_gate(independent, "M15_FIRST")

        modes = {
            "INDEPENDENT": independent,
            "PAIR_GATE_H1_FIRST": gated_h1,
            "PAIR_GATE_M15_FIRST": gated_m15,
        }

        # ------------------------------------------------------------
        # 4) Audit / manifest / parity.
        # ------------------------------------------------------------
        source_audit = [
            {
                "source": "strategy_probe(6).py",
                "role": "CURRENT LIVE H1 STRATEGY AUTHORITY",
                "sha256": "4ec3b8870fb085eb842fdf85e60681b81968b300efa1a9e1ab677a1d242d5a65",
            },
            {
                "source": "app(7).py",
                "role": "CURRENT LIVE EXECUTOR / 1%-NAV REFERENCE",
                "sha256": "14d9aa2c555f7809efd8b8f03a53923c773d47494aaa0c60883d5b54cbfa89d4",
            },
            {
                "source": "m15_final_locked_portfolio_EQUITY_COMPOUNDING_analysis.py",
                "role": "FINAL LOCKED 10-STRATEGY M15 REBUILD AUTHORITY",
                "sha256": "703baac7e933c6b5b33c2f5870bbacf6a0d7954b5a5067a313c524c7b8f15df8",
            },
        ]
        write_csv(COMBINED_OUT["source_audit"], source_audit)

        manifest = []
        for pair in PAIRS:
            manifest.append({
                "strategy_id": f"{pair}_H1_LONG",
                "pair": pair,
                "timeframe": "H1",
                "side": "BUY",
                "source": "attached current live strategy_probe(6).py",
                "config_json": json.dumps(
                    combined_json_safe(STRATEGIES[pair]), sort_keys=True
                ),
            })
            manifest.append({
                "strategy_id": f"{pair}_H1_SHORT",
                "pair": pair,
                "timeframe": "H1",
                "side": "SELL",
                "source": "attached current live strategy_probe(6).py",
                "config_json": json.dumps(
                    combined_json_safe(SHORT_STRATEGIES[pair]), sort_keys=True
                ),
            })
        for row in MANIFEST:
            manifest.append({
                "strategy_id": row["strategy_id"],
                "pair": row["pair"],
                "timeframe": "M15",
                "side": row["side"],
                "source": row["source"],
                "config_json": row["rules"],
            })
        write_csv(COMBINED_OUT["manifest"], manifest)

        coverage_rows = []
        for r in h1_coverage:
            coverage_rows.append({"timeframe": "H1", **r})
        # Carry forward the exact M15 coverage produced by embedded runner.
        if os.path.exists(OUT["coverage"]):
            with open(OUT["coverage"], newline="", encoding="utf-8") as f:
                for r in csv.DictReader(f):
                    coverage_rows.append({"timeframe": "M15", **r})
        write_csv(COMBINED_OUT["coverage"], coverage_rows)

        parity_rows = []
        for sid in sorted(h1_by_strategy):
            parity_rows.append({
                "scope": "H1_STRATEGY",
                "strategy_id": sid,
                "trades": len(h1_by_strategy[sid]),
                "status": "INFO",
            })
        parity_rows.extend({
            "scope": "H1_PORTFOLIO",
            "strategy_id": "ALL_H1",
            "trades": len(h1_trades),
            "reference_min": H1_REFERENCE_FULL_TRADES,
            "status": (
                "PASS_EQUAL"
                if len(h1_trades) == H1_REFERENCE_FULL_TRADES
                else "PASS_NEWER_TRADES"
            ),
        } for _ in [0])
        parity_rows.extend({
            "scope": "M15_PORTFOLIO",
            "strategy_id": "ALL_M15",
            "trades": len(m15_trades),
            "reference_min": M15_REFERENCE_PORTFOLIO_TRADES,
            "status": (
                "PASS_EQUAL"
                if len(m15_trades) == M15_REFERENCE_PORTFOLIO_TRADES
                else "PASS_NEWER_TRADES"
            ),
        } for _ in [0])
        write_csv(COMBINED_OUT["parity"], parity_rows)

        # ------------------------------------------------------------
        # 5) Non-compounded strategy / timeframe / pair / mode stats.
        # ------------------------------------------------------------
        all_strategy_ids = sorted(all_by_strategy)
        independent_stats = calc_stats(independent)

        strategy_summary = []
        for sid in all_strategy_ids:
            tr = all_by_strategy[sid]
            s = calc_stats(tr)
            strategy_summary.append({
                "strategy_id": sid,
                "pair": tr[0]["pair"] if tr else "",
                "timeframe": tr[0]["timeframe"] if tr else "",
                "side": tr[0]["side"] if tr else "",
                **s,
                "contribution_pct_of_independent_total_r": pct(
                    s["total_r"], independent_stats["total_r"]
                ),
            })
        write_csv(COMBINED_OUT["strategy_summary"], strategy_summary)

        tf_rows = []
        for tf in ("H1", "M15"):
            tr = [t for t in independent if t["timeframe"] == tf]
            s = calc_stats(tr)
            tf_rows.append({
                "timeframe": tf,
                **s,
                "contribution_pct_of_independent_total_r": pct(
                    s["total_r"], independent_stats["total_r"]
                ),
            })
        write_csv(COMBINED_OUT["timeframe_summary"], tf_rows)

        pair_rows = []
        for pair in PAIRS:
            tr = [t for t in independent if t["pair"] == pair]
            s = calc_stats(tr)
            pair_rows.append({
                "pair": pair,
                **s,
                "contribution_pct_of_independent_total_r": pct(
                    s["total_r"], independent_stats["total_r"]
                ),
            })
        write_csv(COMBINED_OUT["pair_summary"], pair_rows)

        mode_rows = []
        for mode, tr in modes.items():
            s = calc_stats(tr)
            se = calc_stats(tr, "exit")
            mode_rows.append({
                "portfolio_mode": mode,
                "strategies_available": len(all_strategy_ids),
                "trades": s["trades"],
                "winners": s["winners"],
                "losers": s["losers"],
                "win_rate_pct": s["win_rate_pct"],
                "profit_factor": s["profit_factor"],
                "total_r": s["total_r"],
                "expectancy_r": s["expectancy_r"],
                "signal_order_max_drawdown_r": s["max_drawdown_r"],
                "exit_order_max_drawdown_r": se["max_drawdown_r"],
                "longest_loss_streak": s["longest_loss_streak"],
                "trades_removed_vs_independent": len(independent) - len(tr),
            })
        write_csv(COMBINED_OUT["mode_summary"], mode_rows)

        # ------------------------------------------------------------
        # 6) Periods / calendar / rolling / frequency for each mode.
        # ------------------------------------------------------------
        period_defs = [
            ("FULL", None, None),
            ("PRE_2010", None, datetime(2010, 1, 1, tzinfo=timezone.utc)),
            ("2010_PLUS", datetime(2010, 1, 1, tzinfo=timezone.utc), None),
            ("2018_PLUS", datetime(2018, 1, 1, tzinfo=timezone.utc), None),
            ("LAST_10Y", NOW - timedelta(days=365.2425 * 10), NOW),
            ("LAST_5Y", NOW - timedelta(days=365.2425 * 5), NOW),
            ("LAST_3Y", NOW - timedelta(days=365.2425 * 3), NOW),
            ("LAST_2Y", NOW - timedelta(days=365.2425 * 2), NOW),
            ("LAST_1Y", NOW - timedelta(days=365.2425), NOW),
        ]

        active_start = month_floor(min(t["signal_time"] for t in independent))
        end_complete = month_floor(NOW)

        period_rows = []
        calendar_out = []
        rolling_out = []
        frequency_out = []

        for mode, tr in modes.items():
            for label, a, b in period_defs:
                row = stats_row(mode, label, tr, a, b)
                row["portfolio_mode"] = mode
                period_rows.append(row)

            calendar_out.extend(
                dict(r, portfolio_mode=mode)
                for r in calendar_rows(mode, tr, START.year, NOW.year)
            )

            for months in (12, 24, 36):
                rolling_out.extend(
                    dict(r, portfolio_mode=mode)
                    for r in rolling_rows(
                        mode, tr, months, active_start, end_complete
                    )
                )

            freq_periods = [
                ("FULL", active_start, NOW),
                ("LAST_10Y", NOW - timedelta(days=365.2425 * 10), NOW),
                ("LAST_5Y", NOW - timedelta(days=365.2425 * 5), NOW),
                ("LAST_3Y", NOW - timedelta(days=365.2425 * 3), NOW),
                ("LAST_2Y", NOW - timedelta(days=365.2425 * 2), NOW),
                ("LAST_1Y", NOW - timedelta(days=365.2425), NOW),
            ]
            for label, a, b in freq_periods:
                row = frequency_row(mode, tr, a, b, label)
                row["portfolio_mode"] = mode
                frequency_out.append(row)

        # Also include H1-only and M15-only frequency.
        for tf in ("H1", "M15"):
            tf_tr = [t for t in independent if t["timeframe"] == tf]
            for label, a, b in [
                ("FULL", active_start, NOW),
                ("LAST_5Y", NOW - timedelta(days=365.2425 * 5), NOW),
                ("LAST_2Y", NOW - timedelta(days=365.2425 * 2), NOW),
                ("LAST_1Y", NOW - timedelta(days=365.2425), NOW),
            ]:
                row = frequency_row(f"{tf}_ONLY", tf_tr, a, b, label)
                row["portfolio_mode"] = f"{tf}_ONLY"
                frequency_out.append(row)

        write_csv(COMBINED_OUT["periods"], period_rows)
        write_csv(COMBINED_OUT["calendar"], calendar_out)
        write_csv(COMBINED_OUT["rolling"], rolling_out)
        write_csv(COMBINED_OUT["rolling_summary"], rolling_summary(rolling_out))
        write_csv(COMBINED_OUT["frequency"], frequency_out)

        # ------------------------------------------------------------
        # 7) Correlation / overlap / concurrency / gate rejections.
        # ------------------------------------------------------------
        monthly = combined_monthly_matrix(
            all_by_strategy, active_start, month_floor(NOW)
        )
        write_csv(COMBINED_OUT["monthly"], monthly)
        write_csv(
            COMBINED_OUT["correlation"],
            correlation_rows(monthly, all_strategy_ids),
        )
        write_csv(COMBINED_OUT["overlap"], actual_overlap_rows(all_by_strategy))

        concurrency_out = []
        for mode, tr in modes.items():
            concurrency_out.extend(actual_concurrency_rows(mode, tr))
        write_csv(COMBINED_OUT["concurrency"], concurrency_out)

        gate_rejections = rejected_h1 + rejected_m15
        write_csv(COMBINED_OUT["pair_gate_rejections"], gate_rejections)

        # ------------------------------------------------------------
        # 8) Native-cost multiplier stress.
        # ------------------------------------------------------------
        cost_rows = []
        for mult in COST_MULTIPLIERS:
            stressed_independent = [
                recalc_trade_cost_multiplier(t, mult) for t in independent
            ]
            stressed_by_key = {
                (
                    t["strategy_id"],
                    t["entry_time"],
                    t["exit_event_time"],
                ): t
                for t in stressed_independent
            }

            for mode, base_trades in modes.items():
                stressed = [
                    stressed_by_key[(
                        t["strategy_id"],
                        t["entry_time"],
                        t["exit_event_time"],
                    )]
                    for t in base_trades
                ]
                s = calc_stats(stressed)
                cost_rows.append({
                    "portfolio_mode": mode,
                    "cost_multiplier": mult,
                    "h1_adverse_ticks": BACKTEST_SLIPPAGE_TICKS * mult,
                    "m15_adverse_pips": 1.0 * mult,
                    **s,
                })

                # Two most decision-relevant risk allocations under cost stress.
                for h1r, m15r, label in [
                    (0.0100, 0.0100, "H1_1_M15_1"),
                    (0.0075, 0.0100, "H1_0.75_M15_1"),
                ]:
                    es = simulate_mixed_equity(
                        stressed, h1r, m15r, STARTING_BALANCE
                    )["summary"]
                    cost_rows.append({
                        "portfolio_mode": mode,
                        "cost_multiplier": mult,
                        "h1_adverse_ticks": BACKTEST_SLIPPAGE_TICKS * mult,
                        "m15_adverse_pips": 1.0 * mult,
                        "equity_scenario": label,
                        **es,
                    })
        write_csv(COMBINED_OUT["cost_stress"], cost_rows)

        # ------------------------------------------------------------
        # 9) Full H1-risk x M15-risk equity matrix for all 3 modes.
        # ------------------------------------------------------------
        COMBINED_STATUS.update(
            state="equity",
            message="Running 3x3 H1/M15 risk matrix across all portfolio modes",
            progress=91,
        )

        equity_matrix = []
        equity_periods = []
        equity_calendar = []
        equity_rolling = []
        key_curves = []
        key_trade_rows = []

        key_scenarios = {
            ("INDEPENDENT", 1.00, 1.00),
            ("INDEPENDENT", 0.75, 1.00),
            ("PAIR_GATE_H1_FIRST", 1.00, 1.00),
            ("PAIR_GATE_H1_FIRST", 0.75, 1.00),
        }

        for mode, tr in modes.items():
            first_year = min(t["entry_time"].year for t in tr)
            roll_start = month_floor(min(t["entry_time"] for t in tr))

            for h1r in COMBINED_RISK_LEVELS:
                for m15r in COMBINED_RISK_LEVELS:
                    sim = simulate_mixed_equity(
                        tr, h1r, m15r, STARTING_BALANCE
                    )
                    equity_matrix.append({
                        "portfolio_mode": mode,
                        **sim["summary"],
                    })

                    for label, a, b in [
                        ("LAST_10Y", NOW - timedelta(days=365.2425 * 10), NOW),
                        ("LAST_5Y", NOW - timedelta(days=365.2425 * 5), NOW),
                        ("LAST_3Y", NOW - timedelta(days=365.2425 * 3), NOW),
                        ("LAST_2Y", NOW - timedelta(days=365.2425 * 2), NOW),
                        ("LAST_1Y", NOW - timedelta(days=365.2425), NOW),
                    ]:
                        equity_periods.append(
                            mixed_equity_period_row(
                                mode, sim, h1r, m15r, label, a, b, tr
                            )
                        )

                    equity_calendar.extend(
                        mixed_equity_calendar_rows(
                            mode, sim, h1r, m15r, tr, first_year, NOW.year
                        )
                    )

                    for months in (12, 24, 36):
                        equity_rolling.extend(
                            mixed_equity_rolling_rows(
                                mode, sim, h1r, m15r, tr,
                                months, roll_start, end_complete
                            )
                        )

                    scenario_key = (
                        mode,
                        round(h1r * 100.0, 2),
                        round(m15r * 100.0, 2),
                    )
                    if scenario_key in key_scenarios:
                        for r in sim["curve"]:
                            key_curves.append({
                                "portfolio_mode": mode,
                                "h1_risk_pct": h1r * 100.0,
                                "m15_risk_pct": m15r * 100.0,
                                **r,
                            })
                        for r in sim["trade_rows"]:
                            key_trade_rows.append({
                                "portfolio_mode": mode,
                                **r,
                            })

        write_csv(COMBINED_OUT["equity_matrix"], equity_matrix)
        write_csv(COMBINED_OUT["equity_periods"], equity_periods)
        write_csv(COMBINED_OUT["equity_calendar"], equity_calendar)
        write_csv(
            COMBINED_OUT["equity_calendar_summary"],
            mixed_equity_calendar_summary(equity_calendar),
        )
        write_csv(COMBINED_OUT["equity_rolling"], equity_rolling)
        write_csv(
            COMBINED_OUT["equity_rolling_summary"],
            mixed_equity_rolling_summary(equity_rolling),
        )
        write_csv(COMBINED_OUT["equity_key_curve"], key_curves)
        write_csv(COMBINED_OUT["equity_key_trades"], key_trade_rows)

        # ------------------------------------------------------------
        # 10) Trade logs + notes + one ZIP.
        # ------------------------------------------------------------
        write_csv(
            COMBINED_OUT["trades_independent"],
            [serialise_trade(t) for t in independent],
        )
        write_csv(
            COMBINED_OUT["trades_pair_gate_h1_first"],
            [serialise_trade(t) for t in gated_h1],
        )
        write_csv(
            COMBINED_OUT["trades_pair_gate_m15_first"],
            [serialise_trade(t) for t in gated_m15],
        )

        notes = [
            {
                "item": "Read only",
                "value": "This research service never sends orders.",
            },
            {
                "item": "H1 authority",
                "value": "All 10 H1 configs are literal extracts from the attached current live strategy_probe(6).py.",
            },
            {
                "item": "H1 backtest cost",
                "value": "Baseline adverse entry is exactly 5 ticks, matching current H1 historical code.",
            },
            {
                "item": "M15 authority",
                "value": "The embedded exact final 10-strategy M15 runner is executed first and must pass its own final-lock parity guards.",
            },
            {
                "item": "M15 backtest cost",
                "value": "Baseline adverse entry is 1 pip, matching the final full-history M15 research.",
            },
            {
                "item": "Current live instrument gate",
                "value": "The attached H1 live engine refuses a new signal when OANDA already has any open trade on that instrument. PAIR_GATE modes model that restriction across H1+M15; INDEPENDENT shows pure research diversification.",
            },
            {
                "item": "Exact-tie gate priority",
                "value": "Both H1-first and M15-first variants are exported because the future combined H1/M15 watcher tie order is not yet defined. BUY precedes SELL within a timeframe.",
            },
            {
                "item": "Risk matrix",
                "value": "Every portfolio mode is compounded at all nine H1-risk x M15-risk combinations from 0.50%, 0.75% and 1.00% per trade.",
            },
            {
                "item": "NAV caveat",
                "value": "Actual app(7).py sizes from current OANDA NAV. The backtest sizes from realised equity because candle-level outcome records do not reconstruct intra-trade mark-to-market NAV. Conservative open-risk-floor DD is exported.",
            },
            {
                "item": "Actual event timing",
                "value": "H1 entries use signal candle close (+1h); M15 entries use signal candle close (+15m). Exit event timestamps use the close of the historical exit bar. This is used for cross-timeframe pair gating and concurrency.",
            },
            {
                "item": "Cost multiplier stress",
                "value": "0.5x/1x/1.5x/2x means H1 2.5/5/7.5/10 adverse ticks while M15 uses 0.5/1/1.5/2 adverse pips.",
            },
        ]
        write_csv(COMBINED_OUT["notes"], notes)

        COMBINED_STATUS.update(
            state="packaging",
            message="Packaging one H1+M15 20-strategy ZIP",
            progress=97,
        )

        with zipfile.ZipFile(
            COMBINED_BUNDLE, "w", compression=zipfile.ZIP_DEFLATED
        ) as z:
            for p in COMBINED_OUT.values():
                if os.path.exists(p):
                    z.write(p, arcname=os.path.basename(p))
            # Include the embedded M15 parity/result bundle as provenance.
            if os.path.exists(BUNDLE):
                z.write(BUNDLE, arcname=os.path.basename(BUNDLE))

        ind = calc_stats(independent)
        COMBINED_STATUS.update(
            state="complete",
            message="H1 + M15 final 20-strategy portfolio analysis complete",
            progress=100,
            results=COMBINED_BUNDLE,
            h1_trades=len(h1_trades),
            m15_trades=len(m15_trades),
            independent_trades=len(independent),
            independent_total_r=ind["total_r"],
            independent_pf=ind["profit_factor"],
            pair_gate_h1_first_trades=len(gated_h1),
            pair_gate_m15_first_trades=len(gated_m15),
        )

    except Exception as e:
        import traceback
        COMBINED_STATUS.update(
            state="error",
            message=str(e),
            error_type=type(e).__name__,
            traceback=traceback.format_exc(),
            progress=COMBINED_STATUS.get("progress", 0),
        )




# ============================================================
# H1 + M15 EXIT RESEARCH LAYER
# ============================================================
#
# READ ONLY. NEVER SENDS ORDERS.
#
# Purpose:
#   Test whether universal early-exit rules improve the exact locked
#   20-strategy H1+M15 portfolio without changing any ENTRY rule.
#
# Control:
#   Existing STOP / TARGET only.
#
# Candidate families:
#   1) MAX HOLD ONLY
#      H1: 12 / 24 / 48 / 72 completed post-entry H1 bars
#      M15: 24 / 48 / 72 / 96 completed post-entry M15 bars
#
#   2) TIME + MFE PROGRESS
#      At the same bar limits, exit at that bar close ONLY IF the trade
#      has never achieved +0.25R, +0.50R or +0.75R MFE since entry.
#      If the threshold has already been reached, the trade is left alone
#      to continue to its original stop or target.
#
#   3) UNIVERSAL REVERSAL
#      Exit at bar close after a strong exact opposite engulfing candle.
#      Opposite body thresholds: 0.75 / 1.00 / 1.25 ATR14.
#      This is deliberately generic: no pair-specific reversal optimization.
#
# Important causality conventions:
#   - Original stop/target is always checked FIRST inside each bar.
#   - If neither is hit, MFE/reversal/time logic is evaluated at bar close.
#   - MFE starts only AFTER entry; the signal candle's pre-entry excursion
#     is never counted.
#   - Exact exit-candle re-entry remains eligible, preserving locked p0 logic.
#   - Original entry adverse-cost convention is unchanged.
#   - Primary early-exit result uses the bar close with no invented extra
#     market-exit slippage. A separate sensitivity table adds 0.5x and 1.0x
#     the native adverse entry-cost amount ONLY to early-market exits.
#
# Live portfolio modes:
#   INDEPENDENT
#       Research ceiling: different strategies can overlap in either direction.
#
#   LIVE_SAFE_H1_FIRST
#       Matches the current non-hedging OANDA constraint: same-direction
#       same-pair overlaps are allowed, but a new trade is rejected while an
#       opposite-direction trade on that pair is open. H1 wins exact entry ties.
#
#   LIVE_SAFE_M15_FIRST
#       Same rule, but M15 wins exact H1/M15 entry ties. Exported as a tie-order
#       sensitivity because the two live watcher loops are independent.
# ============================================================

EXIT_STATUS = {
    "state": "not_started",
    "message": "Exit research not started",
    "progress": 0,
}

EXIT_BUNDLE = "H1_M15_20_EXIT_RESEARCH_RESULTS.zip"
EXIT_OUT = {
    "scenario_definitions": "h1_m15_exit_scenario_definitions.csv",
    "control_parity": "h1_m15_exit_control_parity.csv",
    "scenario_summary": "h1_m15_exit_scenario_summary.csv",
    "delta_vs_control": "h1_m15_exit_delta_vs_control.csv",
    "by_strategy": "h1_m15_exit_by_strategy.csv",
    "by_timeframe": "h1_m15_exit_by_timeframe.csv",
    "periods": "h1_m15_exit_periods.csv",
    "rolling": "h1_m15_exit_rolling.csv",
    "rolling_summary": "h1_m15_exit_rolling_summary.csv",
    "calendar": "h1_m15_exit_calendar.csv",
    "calendar_summary": "h1_m15_exit_calendar_summary.csv",
    "exit_reasons": "h1_m15_exit_reason_summary.csv",
    "hold_summary": "h1_m15_exit_hold_summary.csv",
    "gate_rejections": "h1_m15_exit_live_safe_gate_rejections.csv",
    "market_exit_cost_stress": "h1_m15_exit_market_exit_cost_stress.csv",
    "trades": "h1_m15_exit_all_trades.csv",
    "notes": "h1_m15_exit_notes.csv",
}

# Current known control counts from the exact combined run on 2026-09-10.
# A later run may have MORE trades because the history endpoint advances, but
# it must never fall below these counts under CONTROL.
H1_CONTROL_MIN_BY_STRATEGY = {
    "EUR_GBP_H1_LONG": 52,
    "EUR_GBP_H1_SHORT": 73,
    "EUR_USD_H1_LONG": 163,
    "EUR_USD_H1_SHORT": 122,
    "GBP_USD_H1_LONG": 205,
    "GBP_USD_H1_SHORT": 123,
    "USD_CAD_H1_LONG": 82,
    "USD_CAD_H1_SHORT": 98,
    "USD_JPY_H1_LONG": 228,
    "USD_JPY_H1_SHORT": 169,
}
M15_CONTROL_MIN_BY_STRATEGY = dict(REFERENCE_COUNTS)


def build_exit_scenarios():
    out = [{
        "scenario_id": "CONTROL",
        "family": "CONTROL",
        "h1_max_bars": None,
        "m15_max_bars": None,
        "mfe_threshold_r": None,
        "reversal_body_atr_min": None,
        "description": "Original locked stop/target exits only",
    }]

    bar_pairs = [
        (12, 24, "A"),
        (24, 48, "B"),
        (48, 72, "C"),
        (72, 96, "D"),
    ]

    for h1_bars, m15_bars, label in bar_pairs:
        out.append({
            "scenario_id": f"TIME_ONLY_{label}_H1_{h1_bars}_M15_{m15_bars}",
            "family": "TIME_ONLY",
            "h1_max_bars": h1_bars,
            "m15_max_bars": m15_bars,
            "mfe_threshold_r": None,
            "reversal_body_atr_min": None,
            "description": (
                f"Exit at bar close after H1={h1_bars} or M15={m15_bars} "
                "post-entry bars if stop/target has not already hit"
            ),
        })

        for mfe in (0.25, 0.50, 0.75):
            mfe_label = str(mfe).replace(".", "P")
            out.append({
                "scenario_id": (
                    f"TIME_MFE_{label}_H1_{h1_bars}_M15_{m15_bars}_MFE_{mfe_label}R"
                ),
                "family": "TIME_MFE",
                "h1_max_bars": h1_bars,
                "m15_max_bars": m15_bars,
                "mfe_threshold_r": mfe,
                "reversal_body_atr_min": None,
                "description": (
                    f"At H1={h1_bars}/M15={m15_bars} bars, exit only if "
                    f"post-entry MFE has never reached +{mfe:.2f}R"
                ),
            })

    for body_atr in (0.75, 1.00, 1.25):
        label = str(body_atr).replace(".", "P")
        out.append({
            "scenario_id": f"REVERSAL_OPP_ENGULF_BODY_{label}ATR",
            "family": "REVERSAL",
            "h1_max_bars": None,
            "m15_max_bars": None,
            "mfe_threshold_r": None,
            "reversal_body_atr_min": body_atr,
            "description": (
                "Exit at bar close on an exact opposite engulfing candle "
                f"whose body is >= {body_atr:.2f} ATR14"
            ),
        })

    return out


EXIT_SCENARIOS = build_exit_scenarios()
EXIT_SCENARIO_MAP = {x["scenario_id"]: x for x in EXIT_SCENARIOS}


def exit_rule_for_timeframe(scenario, timeframe):
    if timeframe == "H1":
        max_bars = scenario["h1_max_bars"]
    elif timeframe == "M15":
        max_bars = scenario["m15_max_bars"]
    else:
        raise ValueError(f"Unknown timeframe: {timeframe}")

    return {
        "family": scenario["family"],
        "max_bars": max_bars,
        "mfe_threshold_r": scenario["mfe_threshold_r"],
        "reversal_body_atr_min": scenario["reversal_body_atr_min"],
    }


def exact_opposite_reversal(candles, atr_values, index, open_side, body_atr_min):
    if body_atr_min is None or index <= 0:
        return False
    if index >= len(candles) or index >= len(atr_values):
        return False

    atr = atr_values[index]
    if atr is None or not math.isfinite(float(atr)) or atr <= 0:
        return False

    previous = candles[index - 1]
    current = candles[index]

    if open_side == "BUY":
        if not (
            previous["close"] > previous["open"]
            and current["close"] < current["open"]
            and current["open"] >= previous["close"]
            and current["close"] <= previous["open"]
        ):
            return False
        body = current["open"] - current["close"]
    else:
        if not (
            previous["close"] < previous["open"]
            and current["close"] > current["open"]
            and current["open"] <= previous["close"]
            and current["close"] >= previous["open"]
        ):
            return False
        body = current["close"] - current["open"]

    return body > 0 and (body / atr) >= body_atr_min


def exit_result_r(side, fill, stop, exit_price):
    if side == "BUY":
        actual_risk = fill - stop
        if actual_risk <= 0:
            return None
        return (exit_price - fill) / actual_risk

    actual_risk = stop - fill
    if actual_risk <= 0:
        return None
    return (fill - exit_price) / actual_risk


def evaluate_trade_exit_on_bar(
    *,
    side,
    trade,
    candles,
    atr_values,
    index,
    bars_open,
    mfe_r_before,
    rule,
):
    """
    Returns (exit_reason, exit_price, mfe_r_after).

    Intrabar stop/target is always resolved before any close-based early exit.
    """
    candle = candles[index]
    stop = trade["stop"]
    target = trade["target"]
    fill = trade["historical_fill"]
    actual_risk = (
        fill - stop if side == "BUY" else stop - fill
    )
    if actual_risk <= 0:
        raise RuntimeError("Invalid actual risk")

    hit_stop = (
        candle["low"] <= stop if side == "BUY" else candle["high"] >= stop
    )
    hit_target = (
        candle["high"] >= target if side == "BUY" else candle["low"] <= target
    )

    if hit_stop or hit_target:
        if hit_stop and hit_target:
            high_closer = (
                abs(candle["high"] - candle["open"])
                < abs(candle["open"] - candle["low"])
            )
            if side == "BUY":
                reason = "TARGET" if high_closer else "STOP"
            else:
                reason = "STOP" if high_closer else "TARGET"
        else:
            reason = "STOP" if hit_stop else "TARGET"

        price = stop if reason == "STOP" else target
        # We do not try to infer MFE beyond the event that happened first.
        return reason, price, mfe_r_before

    if side == "BUY":
        bar_favourable = (candle["high"] - fill) / actual_risk
    else:
        bar_favourable = (fill - candle["low"]) / actual_risk

    mfe_after = max(mfe_r_before, float(bar_favourable))

    reversal_min = rule.get("reversal_body_atr_min")
    if reversal_min is not None and exact_opposite_reversal(
        candles, atr_values, index, side, reversal_min
    ):
        return "REVERSAL_EXIT", candle["close"], mfe_after

    max_bars = rule.get("max_bars")
    if max_bars is not None and bars_open >= max_bars:
        threshold = rule.get("mfe_threshold_r")
        if threshold is None:
            return "TIME_EXIT", candle["close"], mfe_after
        if mfe_after < threshold:
            return "TIME_PROGRESS_EXIT", candle["close"], mfe_after

    return None, None, mfe_after


# ============================================================
# M15 EXIT-RESEARCH BACKTEST
# ============================================================

def m15_one_outcome_exit(
    pair,
    strategy_id,
    trigger,
    side,
    m15,
    atr_values,
    i,
    rr,
    cost_pips,
    scenario,
):
    meta = PAIR_META[pair]
    tick = meta["tick"]
    pip = meta["pip"]
    sig = m15[i]
    ref = sig["close"]

    if side == "BUY":
        stop = sig["low"] - 10 * tick
        ref_risk = ref - stop
        if ref_risk <= 0:
            return None
        target = ref + rr * ref_risk
        fill = ref + cost_pips * pip
    else:
        stop = sig["high"] + 10 * tick
        ref_risk = stop - ref
        if ref_risk <= 0:
            return None
        target = ref - rr * ref_risk
        fill = ref - cost_pips * pip

    actual_risk = fill - stop if side == "BUY" else stop - fill
    if actual_risk <= 0:
        return None

    trade = {
        "stop": stop,
        "target": target,
        "historical_fill": fill,
    }
    rule = exit_rule_for_timeframe(scenario, "M15")
    mfe_r = 0.0

    for j in range(i + 1, len(m15)):
        reason, exit_price, mfe_r = evaluate_trade_exit_on_bar(
            side=side,
            trade=trade,
            candles=m15,
            atr_values=atr_values,
            index=j,
            bars_open=j - i,
            mfe_r_before=mfe_r,
            rule=rule,
        )

        if reason is None:
            continue

        r = exit_result_r(side, fill, stop, exit_price)
        if r is None:
            return None

        return {
            "pair": pair,
            "strategy_id": strategy_id,
            "trigger": trigger,
            "timeframe": "M15",
            "side": side,
            "signal_index": i,
            "exit_index": j,
            "signal_time": sig["time"],
            "entry_time": sig["time"] + timedelta(minutes=15),
            "exit_time": m15[j]["time"],
            "exit_event_time": m15[j]["time"] + timedelta(minutes=15),
            "rr": rr,
            "reference_entry": ref,
            "historical_fill": fill,
            "stop": stop,
            "target": target,
            "result": reason,
            "exit_price": float(exit_price),
            "r": float(r),
            "cost_model": "M15_1_ADVERSE_PIP",
            "baseline_cost_value": float(cost_pips),
            "bars_held": j - i,
            "hold_hours": (j - i) * 0.25,
            "mfe_r_at_exit": float(mfe_r),
            "early_exit": reason in {
                "TIME_EXIT", "TIME_PROGRESS_EXIT", "REVERSAL_EXIT"
            },
        }

    return None


def m15_run_stream_exit(
    pair,
    strategy_id,
    side,
    m15,
    atr_values,
    signals,
    cost_pips,
    scenario,
):
    signals = sorted(signals, key=lambda z: z[0])
    idxs = [z[0] for z in signals]
    out = []
    p = 0

    while p < len(signals):
        i, trig, rr = signals[p]
        tr = m15_one_outcome_exit(
            pair,
            strategy_id,
            trig,
            side,
            m15,
            atr_values,
            i,
            rr,
            cost_pips,
            scenario,
        )
        if tr is None:
            p += 1
            continue

        out.append(tr)
        # Exact exit-candle signal is eligible.
        p = bisect.bisect_left(idxs, tr["exit_index"], lo=p + 1)

    return out


def m15_evaluate_strategy_exit(
    pair,
    strategy_id,
    spec,
    m15,
    atr_values,
    cost_pips,
    scenario,
):
    if spec["mode"] in ("single", "priority_union"):
        trades = m15_run_stream_exit(
            pair,
            strategy_id,
            spec["side"],
            m15,
            atr_values,
            spec["signals"],
            cost_pips,
            scenario,
        )
        return trades

    core = m15_run_stream_exit(
        pair,
        strategy_id,
        spec["side"],
        m15,
        atr_values,
        spec["core_signals"],
        cost_pips,
        scenario,
    )
    comp = m15_run_stream_exit(
        pair,
        strategy_id,
        spec["side"],
        m15,
        atr_values,
        spec["comp_signals"],
        cost_pips,
        scenario,
    )

    combined, _, _ = overlay(core, comp)
    return combined


# ============================================================
# H1 EXIT-RESEARCH BACKTEST
# ============================================================

def h1_open_trade_from_result(pair, side, result):
    config = STRATEGIES[pair] if side == "BUY" else SHORT_STRATEGIES[pair]
    tick = config["tick_size"]
    ref = float(result["close"])

    if side == "BUY":
        fill = ref + BACKTEST_SLIPPAGE_TICKS * tick
        stop = float(result["low"]) - config["stop_buffer_ticks"] * tick
        ref_risk = ref - stop
        target = ref + ref_risk * config["reward_risk"]
    else:
        fill = ref - BACKTEST_SLIPPAGE_TICKS * tick
        stop = float(result["high"]) + config["stop_buffer_ticks"] * tick
        ref_risk = stop - ref
        target = ref - ref_risk * config["reward_risk"]

    actual_risk = fill - stop if side == "BUY" else stop - fill
    if ref_risk <= 0 or actual_risk <= 0:
        return None

    return {
        "pair": pair,
        "strategy_id": f"{pair}_H1_{'LONG' if side == 'BUY' else 'SHORT'}",
        "trigger": "LIVE_H1_LONG" if side == "BUY" else "LIVE_H1_SHORT",
        "timeframe": "H1",
        "side": side,
        "signal_time": result["signal_start_utc"],
        "entry_time": result["signal_close_utc"],
        "rr": float(config["reward_risk"]),
        "reference_entry": ref,
        "historical_fill": float(fill),
        "stop": float(stop),
        "target": float(target),
        "entry_index": int(result.get("index", -1)),
        "cost_model": "H1_5_ADVERSE_TICKS",
        "baseline_cost_value": float(BACKTEST_SLIPPAGE_TICKS),
        "mfe_r": 0.0,
    }


def h1_simulate_side_exit(
    pair,
    side,
    h1,
    atr_values,
    daily_state,
    start,
    end,
    scenario,
):
    config = STRATEGIES[pair] if side == "BUY" else SHORT_STRATEGIES[pair]

    required = [config["atr_length"], config["structure_lookback"]]
    if side == "SELL":
        ml = config.get("momentum_lookback_bars")
        if ml is not None:
            required.append(int(ml))
        for lookback in config.get("momentum_requirements", {}).keys():
            required.append(int(lookback))
        if config.get("minimum_h1_atr_ratio_50") is not None:
            required.append(50)

    start_index = max(required)
    open_trade = None
    trades = []
    raw_signals = 0
    ignored = 0
    rule = exit_rule_for_timeframe(scenario, "H1")

    for index in range(start_index, len(h1)):
        candle = h1[index]
        candle_time = candle["time"]
        if candle_time < start:
            continue
        if candle_time >= end:
            break

        if open_trade is not None:
            bars_open = index - open_trade["signal_index"]
            reason, exit_price, mfe_after = evaluate_trade_exit_on_bar(
                side=side,
                trade=open_trade,
                candles=h1,
                atr_values=atr_values,
                index=index,
                bars_open=bars_open,
                mfe_r_before=open_trade["mfe_r"],
                rule=rule,
            )
            open_trade["mfe_r"] = mfe_after

            if reason is not None:
                r = exit_result_r(
                    side,
                    open_trade["historical_fill"],
                    open_trade["stop"],
                    exit_price,
                )
                if r is not None:
                    finished = dict(open_trade)
                    finished.update({
                        "exit_index": index,
                        "exit_time": candle_time,
                        "exit_event_time": candle_time + timedelta(hours=1),
                        "result": reason,
                        "exit_price": float(exit_price),
                        "r": float(r),
                        "bars_held": bars_open,
                        "hold_hours": float(bars_open),
                        "mfe_r_at_exit": float(mfe_after),
                        "early_exit": reason in {
                            "TIME_EXIT", "TIME_PROGRESS_EXIT", "REVERSAL_EXIT"
                        },
                    })
                    finished.pop("mfe_r", None)
                    finished.pop("entry_index", None)
                    trades.append(finished)
                open_trade = None

        if side == "BUY":
            result = evaluate_signal_at_index(
                pair, h1, atr_values, index, daily_state
            )
        else:
            result = evaluate_short_signal_at_index(
                pair, h1, atr_values, index, daily_state
            )

        if result is None or not result.get("qualified"):
            continue

        raw_signals += 1
        if open_trade is not None:
            ignored += 1
            continue

        new_trade = h1_open_trade_from_result(pair, side, result)
        if new_trade is None:
            continue
        # Signal candle index is the current index; MFE starts next bar.
        new_trade["signal_index"] = index
        open_trade = new_trade

    return {
        "trades": trades,
        "raw_signal_count": raw_signals,
        "ignored_signal_count": ignored,
        "position_still_open_at_end": open_trade is not None,
    }


# ============================================================
# LIVE-SAFE NON-HEDGING GATE
# ============================================================

def live_safe_sort_key(trade, priority):
    if priority == "H1_FIRST":
        tf_rank = 0 if trade["timeframe"] == "H1" else 1
    elif priority == "M15_FIRST":
        tf_rank = 0 if trade["timeframe"] == "M15" else 1
    else:
        raise ValueError(priority)

    side_rank = 0 if trade["side"] == "BUY" else 1
    return (
        trade["entry_time"],
        tf_rank,
        side_rank,
        trade["strategy_id"],
    )


def apply_live_safe_nonhedging_gate(trades, priority="H1_FIRST"):
    """
    Allows same-pair SAME-DIRECTION overlaps across different strategies.
    Blocks only an entry that opposes any still-open trade on that pair.
    """
    ordered = sorted(trades, key=lambda t: live_safe_sort_key(t, priority))
    active = []
    accepted = []
    rejected = []

    for t in ordered:
        ts = t["entry_time"]
        active = [x for x in active if x["exit_event_time"] > ts]

        blockers = [
            x for x in active
            if x["pair"] == t["pair"] and x["side"] != t["side"]
        ]

        if blockers:
            blocker = sorted(
                blockers,
                key=lambda x: (x["exit_event_time"], x["strategy_id"]),
            )[0]
            rejected.append({
                "priority": priority,
                "candidate_strategy_id": t["strategy_id"],
                "candidate_pair": t["pair"],
                "candidate_side": t["side"],
                "candidate_timeframe": t["timeframe"],
                "candidate_entry_time": iso(t["entry_time"]),
                "candidate_r": t["r"],
                "blocker_strategy_id": blocker["strategy_id"],
                "blocker_side": blocker["side"],
                "blocker_timeframe": blocker["timeframe"],
                "blocker_exit_event_time": iso(blocker["exit_event_time"]),
            })
            continue

        accepted.append(t)
        active.append(t)

    return accepted, rejected


# ============================================================
# EXIT-RESEARCH METRICS
# ============================================================

def serialise_exit_trade(scenario_id, mode, t):
    row = {"scenario_id": scenario_id, "portfolio_mode": mode}
    for k, v in t.items():
        if isinstance(v, datetime):
            row[k] = iso(v)
        else:
            row[k] = v
    return row


def exit_reason_rows(scenario_id, mode, trades):
    grouped = defaultdict(list)
    for t in trades:
        grouped[t["result"]].append(t)

    rows = []
    for reason in sorted(grouped):
        g = grouped[reason]
        s = calc_stats(g)
        rows.append({
            "scenario_id": scenario_id,
            "portfolio_mode": mode,
            "exit_reason": reason,
            **s,
            "pct_of_trades": pct(len(g), len(trades)),
            "median_hold_hours": safe_median(t.get("hold_hours", 0.0) for t in g),
            "median_mfe_r_at_exit": safe_median(t.get("mfe_r_at_exit", 0.0) for t in g),
        })
    return rows


def hold_summary_row(scenario_id, mode, trades):
    holds = [float(t.get("hold_hours", 0.0)) for t in trades]
    early = [t for t in trades if t.get("early_exit")]
    return {
        "scenario_id": scenario_id,
        "portfolio_mode": mode,
        "trades": len(trades),
        "early_exits": len(early),
        "early_exit_pct": pct(len(early), len(trades)),
        "median_hold_hours": safe_median(holds),
        "mean_hold_hours": (sum(holds) / len(holds)) if holds else 0.0,
        "max_hold_hours": max(holds) if holds else 0.0,
        "median_early_exit_mfe_r": safe_median(
            t.get("mfe_r_at_exit", 0.0) for t in early
        ),
        "mean_early_exit_r": (
            sum(t["r"] for t in early) / len(early)
            if early else 0.0
        ),
    }


def calendar_summary_exit(rows):
    grouped = defaultdict(list)
    for r in rows:
        grouped[(r["scenario_id"], r["portfolio_mode"])].append(r)

    out = []
    for (scenario_id, mode), g in grouped.items():
        complete = [x for x in g if str(x.get("complete_year")).lower() in {"true", "1"}]
        active = [x for x in complete if x["realized_exits"] > 0]
        positive = [x for x in active if x["compounded_return_pct"] > 0]
        worst = min(active, key=lambda x: x["compounded_return_pct"]) if active else None
        best = max(active, key=lambda x: x["compounded_return_pct"]) if active else None
        out.append({
            "scenario_id": scenario_id,
            "portfolio_mode": mode,
            "completed_active_years": len(active),
            "positive_completed_active_years": len(positive),
            "positive_completed_active_years_pct": pct(len(positive), len(active)),
            "median_calendar_return_pct": safe_median(
                x["compounded_return_pct"] for x in active
            ),
            "worst_year": worst["year"] if worst else "",
            "worst_year_return_pct": worst["compounded_return_pct"] if worst else 0.0,
            "best_year": best["year"] if best else "",
            "best_year_return_pct": best["compounded_return_pct"] if best else 0.0,
        })
    return out


def reprice_early_exit_cost(t, extra_cost_mult):
    if not t.get("early_exit") or extra_cost_mult == 0:
        return dict(t)

    out = dict(t)
    pair = t["pair"]

    if t["timeframe"] == "H1":
        native_price_cost = (
            BACKTEST_SLIPPAGE_TICKS * PAIR_META[pair]["tick"]
        )
    else:
        native_price_cost = PAIR_META[pair]["pip"]

    extra = extra_cost_mult * native_price_cost
    raw_exit = float(t["exit_price"])
    stressed_exit = (
        raw_exit - extra if t["side"] == "BUY" else raw_exit + extra
    )

    out["exit_price"] = stressed_exit
    out["r"] = float(exit_result_r(
        t["side"],
        float(t["historical_fill"]),
        float(t["stop"]),
        stressed_exit,
    ))
    out["early_exit_extra_cost_mult"] = extra_cost_mult
    return out


# ============================================================
# MAIN EXIT RESEARCH
# ============================================================

def run_exit_research():
    try:
        EXIT_STATUS.update(
            state="starting",
            message="Starting exact 20-strategy exit research",
            progress=1,
        )
        write_csv(EXIT_OUT["scenario_definitions"], EXIT_SCENARIOS)

        scenario_by_strategy = {
            s["scenario_id"]: defaultdict(list)
            for s in EXIT_SCENARIOS
        }

        # ------------------------------------------------------------
        # 1) Rebuild M15 signals once per pair, then evaluate every exit rule.
        # ------------------------------------------------------------
        for pi, pair in enumerate(PAIRS):
            EXIT_STATUS.update(
                state="m15_rebuild",
                message=f"M15 {pair}: downloading history + testing exit rules",
                progress=4 + pi * 8,
            )

            m15, empty = fetch_history(pair, "M15", START, NOW)
            if len(m15) < 350000:
                raise RuntimeError(
                    f"Incomplete {pair} M15 history: {len(m15)} candles"
                )

            aligned = {}
            for gran in PAIR_HTFS[pair]:
                hs, _ = fetch_history(pair, gran, HTF_WARMUP, NOW)
                if not hs:
                    raise RuntimeError(f"Missing {pair} {gran} history")
                aligned[gran] = align_htf(
                    [x["time"] for x in m15],
                    htf_state(hs),
                )
                del hs

            features = build_features(pair, m15, aligned)
            specs = signal_sets(pair, features)
            atr_values = [
                None if not np.isfinite(x) else float(x)
                for x in features["atr"]
            ]

            for scenario in EXIT_SCENARIOS:
                sid = scenario["scenario_id"]
                for strategy_id, spec in specs.items():
                    trades = m15_evaluate_strategy_exit(
                        pair,
                        strategy_id,
                        spec,
                        m15,
                        atr_values,
                        BASELINE_COST,
                        scenario,
                    )
                    scenario_by_strategy[sid][strategy_id].extend(trades)

            del features, specs, aligned, atr_values, m15
            gc.collect()

        # ------------------------------------------------------------
        # 2) Rebuild H1 once per pair, test every exit rule.
        # ------------------------------------------------------------
        for pi, pair in enumerate(PAIRS):
            EXIT_STATUS.update(
                state="h1_rebuild",
                message=f"H1 {pair}: downloading history + testing exit rules",
                progress=45 + pi * 7,
            )

            h1, _ = fetch_history(pair, "H1", H1_DATA_WARMUP, NOW)
            daily, _ = fetch_history(pair, "D", H1_DAILY_WARMUP, NOW)

            if len(h1) < 90000:
                raise RuntimeError(
                    f"Incomplete {pair} H1 history: {len(h1)} candles"
                )
            if len(daily) < 5000:
                raise RuntimeError(
                    f"Incomplete {pair} daily history: {len(daily)} candles"
                )

            long_cfg = STRATEGIES[pair]
            long_atr = atr_series(h1, long_cfg["atr_length"])
            long_daily = build_daily_state(daily, long_cfg)
            register_h1_daily_state(long_daily)

            short_cfg = SHORT_STRATEGIES[pair]
            short_atr = atr_series(h1, short_cfg["atr_length"])
            short_daily = build_short_daily_state(daily, short_cfg)
            register_h1_daily_state(short_daily)

            for scenario in EXIT_SCENARIOS:
                sid = scenario["scenario_id"]

                long_sim = h1_simulate_side_exit(
                    pair,
                    "BUY",
                    h1,
                    long_atr,
                    long_daily,
                    START,
                    NOW,
                    scenario,
                )
                short_sim = h1_simulate_side_exit(
                    pair,
                    "SELL",
                    h1,
                    short_atr,
                    short_daily,
                    START,
                    NOW,
                    scenario,
                )

                scenario_by_strategy[sid][f"{pair}_H1_LONG"].extend(
                    long_sim["trades"]
                )
                scenario_by_strategy[sid][f"{pair}_H1_SHORT"].extend(
                    short_sim["trades"]
                )

            _H1_DAILY_TIMES.pop(id(long_daily), None)
            _H1_DAILY_TIMES.pop(id(short_daily), None)
            del h1, daily, long_atr, short_atr, long_daily, short_daily
            gc.collect()

        # ------------------------------------------------------------
        # 3) Hard control-parity guard.
        # ------------------------------------------------------------
        EXIT_STATUS.update(
            state="parity",
            message="Checking CONTROL against locked portfolio references",
            progress=82,
        )

        parity = []
        control_by_strategy = scenario_by_strategy["CONTROL"]
        for strategy_id, minimum in {
            **H1_CONTROL_MIN_BY_STRATEGY,
            **M15_CONTROL_MIN_BY_STRATEGY,
        }.items():
            actual = len(control_by_strategy.get(strategy_id, []))
            status = (
                "PASS_EQUAL"
                if actual == minimum
                else ("PASS_NEWER_TRADES" if actual > minimum else "FAIL_BELOW_REFERENCE")
            )
            parity.append({
                "strategy_id": strategy_id,
                "reference_min_trades": minimum,
                "control_current_trades": actual,
                "status": status,
            })

        write_csv(EXIT_OUT["control_parity"], parity)
        bad = [x for x in parity if x["status"] == "FAIL_BELOW_REFERENCE"]
        if bad:
            raise RuntimeError(
                "CONTROL reproduction fell below reference: "
                + json.dumps(bad, default=str)
            )

        # ------------------------------------------------------------
        # 4) Apply portfolio modes and calculate 1%+1% equity statistics.
        # ------------------------------------------------------------
        summary_rows = []
        strategy_rows = []
        timeframe_rows = []
        period_rows = []
        rolling_rows_out = []
        calendar_rows_out = []
        exit_reason_out = []
        hold_rows = []
        gate_rejections = []
        trade_rows = []
        mode_trade_cache = {}

        end_complete = month_floor(NOW)

        for si, scenario in enumerate(EXIT_SCENARIOS):
            scenario_id = scenario["scenario_id"]
            EXIT_STATUS.update(
                state="analyzing",
                message=f"Analysing {scenario_id}",
                progress=84 + int(9 * (si + 1) / len(EXIT_SCENARIOS)),
            )

            by_strategy = scenario_by_strategy[scenario_id]
            independent = sorted(
                [t for sid in sorted(by_strategy) for t in by_strategy[sid]],
                key=lambda t: (t["entry_time"], t["strategy_id"]),
            )

            live_h1, rej_h1 = apply_live_safe_nonhedging_gate(
                independent, "H1_FIRST"
            )
            live_m15, rej_m15 = apply_live_safe_nonhedging_gate(
                independent, "M15_FIRST"
            )

            modes = {
                "INDEPENDENT": independent,
                "LIVE_SAFE_H1_FIRST": live_h1,
                "LIVE_SAFE_M15_FIRST": live_m15,
            }

            for r in rej_h1:
                gate_rejections.append({"scenario_id": scenario_id, **r})
            for r in rej_m15:
                gate_rejections.append({"scenario_id": scenario_id, **r})

            for mode, trades in modes.items():
                mode_trade_cache[(scenario_id, mode)] = trades
                stats = calc_stats(trades)
                exit_stats = calc_stats(trades, "exit")
                sim = simulate_mixed_equity(
                    trades,
                    0.01,
                    0.01,
                    STARTING_BALANCE,
                )
                es = sim["summary"]
                early = [t for t in trades if t.get("early_exit")]

                summary_rows.append({
                    "scenario_id": scenario_id,
                    "family": scenario["family"],
                    "portfolio_mode": mode,
                    "trades": stats["trades"],
                    "winners": stats["winners"],
                    "losers": stats["losers"],
                    "win_rate_pct": stats["win_rate_pct"],
                    "profit_factor": stats["profit_factor"],
                    "total_r": stats["total_r"],
                    "expectancy_r": stats["expectancy_r"],
                    "signal_order_max_drawdown_r": stats["max_drawdown_r"],
                    "exit_order_max_drawdown_r": exit_stats["max_drawdown_r"],
                    "longest_loss_streak": stats["longest_loss_streak"],
                    "early_exits": len(early),
                    "early_exit_pct": pct(len(early), len(trades)),
                    "cagr_pct_1pct_h1_1pct_m15": es["cagr_pct"],
                    "total_return_pct_1pct_h1_1pct_m15": es["total_return_pct"],
                    "max_closed_equity_dd_pct": es["max_closed_equity_dd_pct"],
                    "max_open_risk_floor_dd_pct": es["max_open_risk_floor_dd_pct"],
                    "max_open_positions": es["max_open_positions"],
                    "max_open_risk_pct_of_realised_equity": es[
                        "max_open_risk_pct_of_realised_equity"
                    ],
                })

                hold_rows.append(hold_summary_row(scenario_id, mode, trades))
                exit_reason_out.extend(exit_reason_rows(scenario_id, mode, trades))

                for tf in ("H1", "M15"):
                    g = [t for t in trades if t["timeframe"] == tf]
                    st = calc_stats(g)
                    timeframe_rows.append({
                        "scenario_id": scenario_id,
                        "portfolio_mode": mode,
                        "timeframe": tf,
                        **st,
                        "early_exits": sum(bool(t.get("early_exit")) for t in g),
                        "median_hold_hours": safe_median(
                            t.get("hold_hours", 0.0) for t in g
                        ),
                    })

                for strategy_id in sorted(by_strategy):
                    g = [t for t in trades if t["strategy_id"] == strategy_id]
                    st = calc_stats(g)
                    strategy_rows.append({
                        "scenario_id": scenario_id,
                        "portfolio_mode": mode,
                        "strategy_id": strategy_id,
                        "timeframe": (
                            g[0]["timeframe"]
                            if g else ("H1" if "_H1_" in strategy_id else "M15")
                        ),
                        **st,
                        "early_exits": sum(bool(t.get("early_exit")) for t in g),
                        "median_hold_hours": safe_median(
                            t.get("hold_hours", 0.0) for t in g
                        ),
                    })

                for label, a, b in [
                    ("LAST_10Y", NOW - timedelta(days=365.2425 * 10), NOW),
                    ("LAST_5Y", NOW - timedelta(days=365.2425 * 5), NOW),
                    ("LAST_3Y", NOW - timedelta(days=365.2425 * 3), NOW),
                    ("LAST_2Y", NOW - timedelta(days=365.2425 * 2), NOW),
                    ("LAST_1Y", NOW - timedelta(days=365.2425), NOW),
                ]:
                    row = mixed_equity_period_row(
                        mode,
                        sim,
                        0.01,
                        0.01,
                        label,
                        a,
                        b,
                        trades,
                    )
                    row["scenario_id"] = scenario_id
                    period_rows.append(row)

                first_year = min(t["entry_time"].year for t in trades)
                cal = mixed_equity_calendar_rows(
                    mode,
                    sim,
                    0.01,
                    0.01,
                    trades,
                    first_year,
                    NOW.year,
                )
                for row in cal:
                    row["scenario_id"] = scenario_id
                    calendar_rows_out.append(row)

                roll_start = month_floor(min(t["entry_time"] for t in trades))
                for months in (12, 24, 36):
                    rr = mixed_equity_rolling_rows(
                        mode,
                        sim,
                        0.01,
                        0.01,
                        trades,
                        months,
                        roll_start,
                        end_complete,
                    )
                    for row in rr:
                        row["scenario_id"] = scenario_id
                        rolling_rows_out.append(row)

                # Keep one complete trade log. ~20 scenarios x ~2300 trades is
                # compact enough for a CSV and invaluable for auditing winners.
                for t in trades:
                    trade_rows.append(serialise_exit_trade(scenario_id, mode, t))

        # ------------------------------------------------------------
        # 5) Deltas vs CONTROL within each portfolio mode.
        # ------------------------------------------------------------
        summary_lookup = {
            (r["scenario_id"], r["portfolio_mode"]): r
            for r in summary_rows
        }
        delta_rows = []
        for row in summary_rows:
            control = summary_lookup[("CONTROL", row["portfolio_mode"])]
            delta_rows.append({
                "scenario_id": row["scenario_id"],
                "family": row["family"],
                "portfolio_mode": row["portfolio_mode"],
                "delta_trades": row["trades"] - control["trades"],
                "delta_total_r": row["total_r"] - control["total_r"],
                "delta_pf": row["profit_factor"] - control["profit_factor"],
                "delta_expectancy_r": row["expectancy_r"] - control["expectancy_r"],
                "delta_cagr_pct_points": (
                    row["cagr_pct_1pct_h1_1pct_m15"]
                    - control["cagr_pct_1pct_h1_1pct_m15"]
                ),
                "delta_closed_dd_pct_points": (
                    row["max_closed_equity_dd_pct"]
                    - control["max_closed_equity_dd_pct"]
                ),
                "delta_floor_dd_pct_points": (
                    row["max_open_risk_floor_dd_pct"]
                    - control["max_open_risk_floor_dd_pct"]
                ),
                "early_exits": row["early_exits"],
            })

        # ------------------------------------------------------------
        # 6) Rolling summaries, adapted to scenario dimension.
        # ------------------------------------------------------------
        roll_grouped = defaultdict(list)
        for row in rolling_rows_out:
            roll_grouped[(
                row["scenario_id"],
                row["portfolio_mode"],
                row["months"],
            )].append(row)

        roll_summary_out = []
        for (scenario_id, mode, months), g in roll_grouped.items():
            active = [x for x in g if x["realized_exits"] > 0]
            positive = [x for x in active if x["compounded_return_pct"] > 0]
            worst = min(active, key=lambda x: x["compounded_return_pct"]) if active else None
            best = max(active, key=lambda x: x["compounded_return_pct"]) if active else None
            roll_summary_out.append({
                "scenario_id": scenario_id,
                "portfolio_mode": mode,
                "months": months,
                "windows": len(g),
                "active_windows": len(active),
                "positive_active_windows": len(positive),
                "positive_active_windows_pct": pct(len(positive), len(active)),
                "median_return_pct_active": safe_median(
                    x["compounded_return_pct"] for x in active
                ),
                "worst_return_pct": worst["compounded_return_pct"] if worst else 0.0,
                "worst_start_utc": worst["start_utc"] if worst else "",
                "best_return_pct": best["compounded_return_pct"] if best else 0.0,
                "best_start_utc": best["start_utc"] if best else "",
            })

        # ------------------------------------------------------------
        # 7) Extra market-exit-cost sensitivity for live-safe H1-first.
        # ------------------------------------------------------------
        market_cost_rows = []
        for scenario in EXIT_SCENARIOS:
            scenario_id = scenario["scenario_id"]
            base_trades = mode_trade_cache[(scenario_id, "LIVE_SAFE_H1_FIRST")]
            for extra_mult in (0.0, 0.5, 1.0):
                stressed = [
                    reprice_early_exit_cost(t, extra_mult)
                    for t in base_trades
                ]
                st = calc_stats(stressed)
                es = simulate_mixed_equity(
                    stressed, 0.01, 0.01, STARTING_BALANCE
                )["summary"]
                market_cost_rows.append({
                    "scenario_id": scenario_id,
                    "portfolio_mode": "LIVE_SAFE_H1_FIRST",
                    "extra_early_exit_cost_mult": extra_mult,
                    "interpretation": (
                        "0=no extra close slippage; 0.5/1.0 = adverse extra "
                        "close cost equal to 0.5x/1.0x each timeframe's native "
                        "entry adverse-cost amount, applied only to early exits"
                    ),
                    **st,
                    "cagr_pct": es["cagr_pct"],
                    "max_closed_equity_dd_pct": es["max_closed_equity_dd_pct"],
                    "max_open_risk_floor_dd_pct": es["max_open_risk_floor_dd_pct"],
                })

        # ------------------------------------------------------------
        # 8) Write everything.
        # ------------------------------------------------------------
        write_csv(EXIT_OUT["scenario_summary"], summary_rows)
        write_csv(EXIT_OUT["delta_vs_control"], delta_rows)
        write_csv(EXIT_OUT["by_strategy"], strategy_rows)
        write_csv(EXIT_OUT["by_timeframe"], timeframe_rows)
        write_csv(EXIT_OUT["periods"], period_rows)
        write_csv(EXIT_OUT["rolling"], rolling_rows_out)
        write_csv(EXIT_OUT["rolling_summary"], roll_summary_out)
        write_csv(EXIT_OUT["calendar"], calendar_rows_out)
        write_csv(EXIT_OUT["calendar_summary"], calendar_summary_exit(calendar_rows_out))
        write_csv(EXIT_OUT["exit_reasons"], exit_reason_out)
        write_csv(EXIT_OUT["hold_summary"], hold_rows)
        write_csv(EXIT_OUT["gate_rejections"], gate_rejections)
        write_csv(EXIT_OUT["market_exit_cost_stress"], market_cost_rows)
        write_csv(EXIT_OUT["trades"], trade_rows)

        write_csv(EXIT_OUT["notes"], [
            {
                "item": "Entry rules frozen",
                "value": "No entry parameter is optimized or changed. This file inherits the exact final locked 10 H1 + 10 M15 definitions from the validated combined runner.",
            },
            {
                "item": "Primary live mode",
                "value": "LIVE_SAFE_H1_FIRST allows same-direction same-pair overlaps and blocks only opposite-direction same-pair entries, matching the current non-hedging account constraint. M15-first is exported as exact-tie sensitivity.",
            },
            {
                "item": "1% + 1%",
                "value": "All equity comparisons use 1% per accepted H1 trade and 1% per accepted M15 trade, matching the current live deployment decision.",
            },
            {
                "item": "Time exit timing",
                "value": "Stop/target is checked intrabar first. If neither is hit and the time rule fires, the market exit is assumed at that bar close.",
            },
            {
                "item": "MFE definition",
                "value": "MFE is calculated from the adverse historical fill in units of actual stop risk, using only post-entry bars. Signal-candle excursion before entry is excluded.",
            },
            {
                "item": "Progress exit",
                "value": "The TIME_MFE rule is a one-time deadline test: at the specified bar count, exit only if MFE has never reached the threshold. If threshold was reached, the trade remains on original stop/target indefinitely.",
            },
            {
                "item": "Reversal exit",
                "value": "Universal exact opposite engulfing only; body must exceed the specified ATR14 threshold. No pair-specific reversal tuning is used.",
            },
            {
                "item": "Early exit costs",
                "value": "Primary scenario results preserve the original adverse-entry cost convention. A separate sensitivity file adds 0.5x and 1.0x native adverse price cost to early-market exits only.",
            },
            {
                "item": "Selection discipline",
                "value": "Do not pick the single highest CAGR blindly. Prefer broad plateaus that improve PF/expectancy/DD/rolling windows across H1, M15 and many strategies, including under extra early-exit cost stress.",
            },
        ])

        EXIT_STATUS.update(
            state="packaging",
            message="Packaging exit-research ZIP",
            progress=98,
        )
        with zipfile.ZipFile(
            EXIT_BUNDLE, "w", compression=zipfile.ZIP_DEFLATED
        ) as z:
            for path in EXIT_OUT.values():
                if os.path.exists(path):
                    z.write(path, arcname=os.path.basename(path))

        control_live = summary_lookup[("CONTROL", "LIVE_SAFE_H1_FIRST")]
        EXIT_STATUS.update(
            state="complete",
            message="20-strategy exit research complete",
            progress=100,
            results=EXIT_BUNDLE,
            scenarios=len(EXIT_SCENARIOS),
            control_live_safe_trades=control_live["trades"],
            control_live_safe_pf=control_live["profit_factor"],
            control_live_safe_cagr_pct=control_live[
                "cagr_pct_1pct_h1_1pct_m15"
            ],
            control_live_safe_floor_dd_pct=control_live[
                "max_open_risk_floor_dd_pct"
            ],
        )

    except Exception as e:
        import traceback
        EXIT_STATUS.update(
            state="error",
            message=str(e),
            error_type=type(e).__name__,
            traceback=traceback.format_exc(),
            progress=EXIT_STATUS.get("progress", 0),
        )


# ============================================================
# EXIT RESEARCH FLASK ROUTES
# ============================================================
@app.get("/")
def exit_root():
    return jsonify({
        "service": "H1 + M15 20-Strategy Exit Research",
        "state": EXIT_STATUS.get("state"),
        "message": EXIT_STATUS.get("message"),
        "orders_supported": False,
        "trading_enabled": False,
        "risk_model": "1% H1 + 1% M15",
        "primary_live_mode": "LIVE_SAFE_H1_FIRST",
        "scenarios": len(EXIT_SCENARIOS),
        "routes": [
            "/h1-m15-exit-research/status",
            "/h1-m15-exit-research/results",
        ],
    })


@app.get("/h1-m15-exit-research/status")
def exit_research_status():
    return jsonify(EXIT_STATUS)


@app.get("/h1-m15-exit-research/results")
def exit_research_results():
    if not os.path.exists(EXIT_BUNDLE):
        return jsonify({
            "error": "Exit research results not ready",
            "status": EXIT_STATUS,
        }), 404
    return send_file(
        os.path.abspath(EXIT_BUNDLE),
        as_attachment=True,
        download_name=EXIT_BUNDLE,
    )


def start_background():
    threading.Thread(target=run_exit_research, daemon=True).start()



# ============================================================
# EUR/JPY STRATEGY #21 — FINAL TARGETED VALIDATION + PORTFOLIO ADD
# ============================================================
#
# This section is intentionally narrow.
#
# Frozen raw family:
#   EUR_JPY H1 LONG
#   exact bullish engulfing
#   BR >= 1.30
#   body >= 1.25 ATR14
#   signal low within 0.15 ATR14 of prior low
#   RR = 5.50
#   stop = signal low - 10 ticks
#   adverse historical entry = +5 ticks = +0.5 pip
#
# Geometry sensitivity retained:
#   lookback = 12 / 15 / 20
#
# Context variants ONLY:
#   CONTROL
#   EXCLUDE_FRIDAY
#   EXCLUDE_NY08
#   EXCLUDE_FRIDAY_NY08
#
# Every one of the 12 candidates is:
#   - rebuilt from OANDA midpoint H1 candles
#   - checked at 1x and cost-stressed 0.5x / 1.5x / 2x
#   - split 2002-2017 vs 2018+
#   - checked over four broad eras
#   - checked last 5Y / 2Y / 1Y
#   - calendar-year tested
#   - rolling 12 / 24 / 36M tested
#   - added ONE AT A TIME to the exact current locked 20-strategy portfolio
#   - passed through the current non-hedging-safe live gate
#   - compounded at 1% per accepted H1/M15 trade
#
# The existing 20-strategy portfolio is rebuilt with the exact frozen
# H1/M15 machinery already present above. No baseline summary is hard-coded.
#
# READ ONLY. NEVER SENDS ORDERS.
# ============================================================

EV_STATUS = {
    "state": "not_started",
    "message": "EUR/JPY final validation not started",
    "progress": 0,
}

EV_BUNDLE = "EURJPY_H1_LONG_FINAL_VALIDATION_AND_PORTFOLIO_ADD_RESULTS.zip"

EV_OUT = {
    "baseline_parity": "eurjpy_final_current20_control_parity.csv",
    "baseline_summary": "eurjpy_final_current20_baseline_summary.csv",
    "candidate_summary": "eurjpy_final_candidate_summary.csv",
    "candidate_periods": "eurjpy_final_candidate_periods.csv",
    "candidate_cost_stress": "eurjpy_final_candidate_cost_stress.csv",
    "candidate_calendar": "eurjpy_final_candidate_calendar.csv",
    "candidate_calendar_summary": "eurjpy_final_candidate_calendar_summary.csv",
    "candidate_rolling": "eurjpy_final_candidate_rolling.csv",
    "candidate_rolling_summary": "eurjpy_final_candidate_rolling_summary.csv",
    "candidate_trades": "eurjpy_final_candidate_trades.csv",
    "portfolio_summary": "eurjpy_final_portfolio_addition_summary.csv",
    "portfolio_delta": "eurjpy_final_portfolio_delta_vs_current20.csv",
    "portfolio_frequency": "eurjpy_final_portfolio_frequency.csv",
    "portfolio_rolling": "eurjpy_final_portfolio_rolling.csv",
    "portfolio_rolling_summary": "eurjpy_final_portfolio_rolling_summary.csv",
    "portfolio_calendar": "eurjpy_final_portfolio_calendar.csv",
    "portfolio_calendar_summary": "eurjpy_final_portfolio_calendar_summary.csv",
    "portfolio_gate_rejections": "eurjpy_final_portfolio_gate_rejections.csv",
    "notes": "eurjpy_final_notes.csv",
}

EV_PAIR = "EUR_JPY"
EV_TICK = 0.001
EV_PIP = 0.01
EV_STOP_BUFFER_TICKS = 10
EV_BASE_COST_TICKS = 5.0
EV_RR = 5.50
EV_BR_MIN = 1.30
EV_BODY_ATR_MIN = 1.25
EV_DISTANCE_ATR_MAX = 0.15
EV_LOOKBACKS = [12, 15, 20]
EV_NY = ZoneInfo("America/New_York")

EV_VARIANTS = [
    {
        "variant_id": "CONTROL",
        "exclude_friday": False,
        "exclude_ny08": False,
    },
    {
        "variant_id": "EXCLUDE_FRIDAY",
        "exclude_friday": True,
        "exclude_ny08": False,
    },
    {
        "variant_id": "EXCLUDE_NY08",
        "exclude_friday": False,
        "exclude_ny08": True,
    },
    {
        "variant_id": "EXCLUDE_FRIDAY_NY08",
        "exclude_friday": True,
        "exclude_ny08": True,
    },
]

# Reference counts from the immediately preceding context-filter run.
# Later history may add trades, but a re-run must never fall BELOW these.
EV_REFERENCE_COUNTS = {
    (12, "CONTROL"): 102,
    (15, "CONTROL"): 91,
    (20, "CONTROL"): 72,
    (12, "EXCLUDE_FRIDAY"): 90,
    (15, "EXCLUDE_FRIDAY"): 78,
    (20, "EXCLUDE_FRIDAY"): 63,
    (12, "EXCLUDE_NY08"): 95,
    (15, "EXCLUDE_NY08"): 86,
    (20, "EXCLUDE_NY08"): 68,
    (12, "EXCLUDE_FRIDAY_NY08"): 84,
    (15, "EXCLUDE_FRIDAY_NY08"): 74,
    (20, "EXCLUDE_FRIDAY_NY08"): 59,
}


# ============================================================
# EUR/JPY CANDIDATE HELPERS
# ============================================================

def ev_atr14(candles):
    return atr_series(candles, 14)


def ev_prior_low(candles, index, lookback):
    if index < lookback:
        return None
    return min(float(candles[j]["low"]) for j in range(index - lookback, index))


def ev_raw_signal(candles, atr_values, index, lookback):
    if index <= 0 or index < lookback:
        return False

    prev = candles[index - 1]
    cur = candles[index]
    atr = atr_values[index]

    if atr is None or not math.isfinite(float(atr)) or float(atr) <= 0:
        return False

    # Exact bullish engulfing.
    if not (float(prev["close"]) < float(prev["open"])):
        return False
    if not (float(cur["close"]) > float(cur["open"])):
        return False
    if not (float(cur["open"]) <= float(prev["close"])):
        return False
    if not (float(cur["close"]) >= float(prev["open"])):
        return False

    current_body = float(cur["close"]) - float(cur["open"])
    previous_body = abs(float(prev["close"]) - float(prev["open"]))

    if previous_body <= 0:
        return False

    if current_body / previous_body < EV_BR_MIN:
        return False

    if current_body / float(atr) < EV_BODY_ATR_MIN:
        return False

    prior_low = ev_prior_low(candles, index, lookback)
    if prior_low is None:
        return False

    distance_atr = abs(float(cur["low"]) - prior_low) / float(atr)
    if distance_atr > EV_DISTANCE_ATR_MAX:
        return False

    return True


def ev_context_allowed(signal_open_utc, variant):
    ny = signal_open_utc.astimezone(EV_NY)

    if variant["exclude_friday"] and ny.weekday() == 4:
        return False

    if variant["exclude_ny08"] and ny.hour == 8:
        return False

    return True


def ev_find_exit(candles, signal_index, stop, target):
    for index in range(signal_index + 1, len(candles)):
        candle = candles[index]
        o = float(candle["open"])
        h = float(candle["high"])
        l = float(candle["low"])

        stop_hit = l <= stop
        target_hit = h >= target

        if not stop_hit and not target_hit:
            continue

        if stop_hit and target_hit:
            # Locked H1 long same-bar tie convention.
            reason = "TARGET" if abs(h - o) < abs(o - l) else "STOP"
        elif target_hit:
            reason = "TARGET"
        else:
            reason = "STOP"

        return index, reason

    return None, None


def ev_build_candidate_trades(
    candles,
    atr_values,
    lookback,
    variant,
    cost_multiplier=1.0,
):
    """
    Exact p0 candidate stream.

    Signal on exact exit candle is eligible.
    """
    strategy_id = (
        f"EUR_JPY_H1_LONG_LB{lookback}_{variant['variant_id']}"
    )

    raw_signal_indices = []
    for index in range(max(14, lookback), len(candles)):
        t = candles[index]["time"]

        if t < START:
            continue
        if t >= NOW:
            break

        if not ev_raw_signal(candles, atr_values, index, lookback):
            continue

        if not ev_context_allowed(t, variant):
            continue

        raw_signal_indices.append(index)

    trades = []
    pointer = 0
    cost_ticks = EV_BASE_COST_TICKS * float(cost_multiplier)

    while pointer < len(raw_signal_indices):
        signal_index = raw_signal_indices[pointer]
        signal = candles[signal_index]

        reference_entry = float(signal["close"])
        historical_fill = reference_entry + cost_ticks * EV_TICK
        stop = float(signal["low"]) - EV_STOP_BUFFER_TICKS * EV_TICK
        reference_risk = reference_entry - stop
        actual_risk = historical_fill - stop

        if reference_risk <= 0 or actual_risk <= 0:
            pointer += 1
            continue

        target = reference_entry + EV_RR * reference_risk

        exit_index, reason = ev_find_exit(
            candles,
            signal_index,
            stop,
            target,
        )

        if exit_index is None:
            break

        exit_price = target if reason == "TARGET" else stop
        result_r = (exit_price - historical_fill) / actual_risk

        exit_candle = candles[exit_index]

        trades.append({
            "pair": EV_PAIR,
            "strategy_id": strategy_id,
            "trigger": variant["variant_id"],
            "timeframe": "H1",
            "side": "BUY",
            "signal_time": signal["time"],
            "entry_time": signal["time"] + timedelta(hours=1),
            "exit_time": exit_candle["time"],
            "exit_event_time": exit_candle["time"] + timedelta(hours=1),
            "rr": EV_RR,
            "reference_entry": reference_entry,
            "historical_fill": historical_fill,
            "stop": stop,
            "target": target,
            "exit_price": exit_price,
            "result": reason,
            "r": float(result_r),
            "bars_held": exit_index - signal_index,
            "hold_hours": float(exit_index - signal_index),
            "early_exit": False,
            "lookback": lookback,
            "variant_id": variant["variant_id"],
            "cost_multiplier": float(cost_multiplier),
            "cost_model": f"EURJPY_H1_{cost_ticks:g}_ADVERSE_TICKS",
        })

        # Exact exit-candle signal remains eligible.
        pointer = bisect.bisect_left(
            raw_signal_indices,
            exit_index,
            lo=pointer + 1,
        )

    return trades


def ev_candidate_period_rows(lookback, variant_id, trades):
    periods = [
        ("FULL", None, None),
        ("DEV_2002_2017", None, datetime(2018, 1, 1, tzinfo=timezone.utc)),
        ("VALIDATION_2018_PLUS", datetime(2018, 1, 1, tzinfo=timezone.utc), None),
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
        ("LAST_5Y", NOW - timedelta(days=365.2425 * 5), None),
        ("LAST_2Y", NOW - timedelta(days=365.2425 * 2), None),
        ("LAST_1Y", NOW - timedelta(days=365.2425), None),
    ]

    rows = []

    for label, start, end in periods:
        s = calc_stats(subset(trades, start, end))
        rows.append({
            "lookback": lookback,
            "variant_id": variant_id,
            "period": label,
            **s,
        })

    return rows


def ev_candidate_summary_row(lookback, variant_id, trades):
    full = calc_stats(trades)
    dev = calc_stats(subset(
        trades,
        None,
        datetime(2018, 1, 1, tzinfo=timezone.utc),
    ))
    val = calc_stats(subset(
        trades,
        datetime(2018, 1, 1, tzinfo=timezone.utc),
        None,
    ))

    era_ranges = [
        (
            datetime(2002, 1, 1, tzinfo=timezone.utc),
            datetime(2008, 1, 1, tzinfo=timezone.utc),
        ),
        (
            datetime(2008, 1, 1, tzinfo=timezone.utc),
            datetime(2014, 1, 1, tzinfo=timezone.utc),
        ),
        (
            datetime(2014, 1, 1, tzinfo=timezone.utc),
            datetime(2020, 1, 1, tzinfo=timezone.utc),
        ),
        (
            datetime(2020, 1, 1, tzinfo=timezone.utc),
            None,
        ),
    ]

    era_stats = [
        calc_stats(subset(trades, a, b))
        for a, b in era_ranges
    ]

    last5 = calc_stats(subset(
        trades,
        NOW - timedelta(days=365.2425 * 5),
        None,
    ))
    last2 = calc_stats(subset(
        trades,
        NOW - timedelta(days=365.2425 * 2),
        None,
    ))
    last1 = calc_stats(subset(
        trades,
        NOW - timedelta(days=365.2425),
        None,
    ))

    positive_eras = sum(
        s["trades"] > 0 and s["total_r"] > 0
        for s in era_stats
    )

    return {
        "lookback": lookback,
        "variant_id": variant_id,
        **{f"full_{k}": v for k, v in full.items()},
        "dev_2002_2017_trades": dev["trades"],
        "dev_2002_2017_pf": dev["profit_factor"],
        "dev_2002_2017_r": dev["total_r"],
        "validation_2018_plus_trades": val["trades"],
        "validation_2018_plus_pf": val["profit_factor"],
        "validation_2018_plus_r": val["total_r"],
        "min_temporal_split_pf": min(
            dev["profit_factor"] if dev["trades"] else 0.0,
            val["profit_factor"] if val["trades"] else 0.0,
        ),
        "both_temporal_splits_positive": (
            dev["trades"] > 0
            and val["trades"] > 0
            and dev["total_r"] > 0
            and val["total_r"] > 0
        ),
        "positive_eras": positive_eras,
        "era_2002_2007_r": era_stats[0]["total_r"],
        "era_2008_2013_r": era_stats[1]["total_r"],
        "era_2014_2019_r": era_stats[2]["total_r"],
        "era_2020_now_r": era_stats[3]["total_r"],
        "last5y_trades": last5["trades"],
        "last5y_pf": last5["profit_factor"],
        "last5y_r": last5["total_r"],
        "last2y_trades": last2["trades"],
        "last2y_pf": last2["profit_factor"],
        "last2y_r": last2["total_r"],
        "last1y_trades": last1["trades"],
        "last1y_pf": last1["profit_factor"],
        "last1y_r": last1["total_r"],
    }


def ev_serialise_trade(t):
    out = dict(t)
    for key, value in list(out.items()):
        if isinstance(value, datetime):
            out[key] = iso(value)
    return out


# ============================================================
# EXACT CURRENT 20-STRATEGY CONTROL REBUILD
# ============================================================

def ev_rebuild_current20():
    control_scenario = next(
        x for x in EXIT_SCENARIOS
        if x["scenario_id"] == "CONTROL"
    )

    by_strategy = defaultdict(list)

    # M15 exact final locks.
    for pi, pair in enumerate(PAIRS):
        EV_STATUS.update(
            state="baseline_m15",
            message=f"Rebuilding current 20: M15 {pair}",
            progress=3 + pi * 5,
        )

        m15, _ = fetch_history(pair, "M15", START, NOW)

        if len(m15) < 350000:
            raise RuntimeError(
                f"Incomplete {pair} M15 history: {len(m15)} candles"
            )

        aligned = {}
        for gran in PAIR_HTFS[pair]:
            hs, _ = fetch_history(pair, gran, HTF_WARMUP, NOW)
            if not hs:
                raise RuntimeError(f"Missing {pair} {gran} history")

            aligned[gran] = align_htf(
                [x["time"] for x in m15],
                htf_state(hs),
            )

        features = build_features(pair, m15, aligned)
        specs = signal_sets(pair, features)

        atr_values = [
            None if not np.isfinite(x) else float(x)
            for x in features["atr"]
        ]

        for strategy_id, spec in specs.items():
            by_strategy[strategy_id].extend(
                m15_evaluate_strategy_exit(
                    pair,
                    strategy_id,
                    spec,
                    m15,
                    atr_values,
                    BASELINE_COST,
                    control_scenario,
                )
            )

        del m15, aligned, features, specs, atr_values
        gc.collect()

    # H1 exact current locks.
    for pi, pair in enumerate(PAIRS):
        EV_STATUS.update(
            state="baseline_h1",
            message=f"Rebuilding current 20: H1 {pair}",
            progress=30 + pi * 6,
        )

        h1, _ = fetch_history(pair, "H1", H1_DATA_WARMUP, NOW)
        daily, _ = fetch_history(pair, "D", H1_DAILY_WARMUP, NOW)

        if len(h1) < 90000:
            raise RuntimeError(
                f"Incomplete {pair} H1 history: {len(h1)} candles"
            )

        if len(daily) < 5000:
            raise RuntimeError(
                f"Incomplete {pair} daily history: {len(daily)} candles"
            )

        long_cfg = STRATEGIES[pair]
        short_cfg = SHORT_STRATEGIES[pair]

        long_atr = atr_series(h1, long_cfg["atr_length"])
        short_atr = atr_series(h1, short_cfg["atr_length"])

        long_daily = build_daily_state(daily, long_cfg)
        short_daily = build_short_daily_state(daily, short_cfg)

        register_h1_daily_state(long_daily)
        register_h1_daily_state(short_daily)

        long_sim = h1_simulate_side_exit(
            pair,
            "BUY",
            h1,
            long_atr,
            long_daily,
            START,
            NOW,
            control_scenario,
        )

        short_sim = h1_simulate_side_exit(
            pair,
            "SELL",
            h1,
            short_atr,
            short_daily,
            START,
            NOW,
            control_scenario,
        )

        by_strategy[f"{pair}_H1_LONG"].extend(long_sim["trades"])
        by_strategy[f"{pair}_H1_SHORT"].extend(short_sim["trades"])

        _H1_DAILY_TIMES.pop(id(long_daily), None)
        _H1_DAILY_TIMES.pop(id(short_daily), None)

        del h1, daily, long_atr, short_atr, long_daily, short_daily
        gc.collect()

    parity = []

    for strategy_id, minimum in {
        **H1_CONTROL_MIN_BY_STRATEGY,
        **M15_CONTROL_MIN_BY_STRATEGY,
    }.items():
        actual = len(by_strategy.get(strategy_id, []))
        status = (
            "PASS_EQUAL"
            if actual == minimum
            else (
                "PASS_NEWER_TRADES"
                if actual > minimum
                else "FAIL_BELOW_REFERENCE"
            )
        )

        parity.append({
            "strategy_id": strategy_id,
            "reference_min_trades": minimum,
            "current_trades": actual,
            "status": status,
        })

    bad = [
        row for row in parity
        if row["status"] == "FAIL_BELOW_REFERENCE"
    ]

    if bad:
        raise RuntimeError(
            "Current 20-strategy control parity failure: "
            + json.dumps(bad, default=str)
        )

    independent = sorted(
        [
            t
            for strategy_id in sorted(by_strategy)
            for t in by_strategy[strategy_id]
        ],
        key=lambda t: (t["entry_time"], t["strategy_id"]),
    )

    live_h1, rejected_h1 = apply_live_safe_nonhedging_gate(
        independent,
        "H1_FIRST",
    )

    live_m15, rejected_m15 = apply_live_safe_nonhedging_gate(
        independent,
        "M15_FIRST",
    )

    return {
        "by_strategy": by_strategy,
        "independent": independent,
        "live_h1_first": live_h1,
        "live_m15_first": live_m15,
        "rejected_h1_first": rejected_h1,
        "rejected_m15_first": rejected_m15,
        "parity": parity,
    }


# ============================================================
# PORTFOLIO ANALYSIS HELPERS
# ============================================================

def ev_portfolio_summary_row(
    candidate_id,
    portfolio_mode,
    trades,
):
    stats = calc_stats(trades)
    exit_stats = calc_stats(trades, "exit")

    sim = simulate_mixed_equity(
        trades,
        0.01,
        0.01,
        STARTING_BALANCE,
    )

    es = sim["summary"]

    return {
        "candidate_id": candidate_id,
        "portfolio_mode": portfolio_mode,
        "trades": stats["trades"],
        "winners": stats["winners"],
        "losers": stats["losers"],
        "win_rate_pct": stats["win_rate_pct"],
        "profit_factor": stats["profit_factor"],
        "total_r": stats["total_r"],
        "expectancy_r": stats["expectancy_r"],
        "signal_order_max_drawdown_r": stats["max_drawdown_r"],
        "exit_order_max_drawdown_r": exit_stats["max_drawdown_r"],
        "longest_loss_streak": stats["longest_loss_streak"],
        "cagr_pct_1pct_h1_1pct_m15": es["cagr_pct"],
        "total_return_pct_1pct_h1_1pct_m15": es["total_return_pct"],
        "ending_balance_from_100": es["ending_balance"],
        "max_closed_equity_dd_pct": es["max_closed_equity_dd_pct"],
        "max_open_risk_floor_dd_pct": es["max_open_risk_floor_dd_pct"],
        "max_open_positions": es["max_open_positions"],
        "max_open_risk_pct_of_realised_equity": es[
            "max_open_risk_pct_of_realised_equity"
        ],
        "_sim": sim,
    }


def ev_frequency_detail(candidate_id, mode, trades):
    rows = []

    windows = [
        ("FULL", min(t["signal_time"] for t in trades), NOW),
        ("LAST_5Y", NOW - timedelta(days=365.2425 * 5), NOW),
        ("LAST_2Y", NOW - timedelta(days=365.2425 * 2), NOW),
        ("LAST_1Y", NOW - timedelta(days=365.2425), NOW),
    ]

    for label, start, end in windows:
        row = frequency_row(
            candidate_id,
            trades,
            start,
            end,
            label,
        )
        row["candidate_id"] = candidate_id
        row["portfolio_mode"] = mode
        rows.append(row)

    return rows


def ev_candidate_calendar_rows(lookback, variant_id, trades):
    if not trades:
        return []

    first_year = trades[0]["signal_time"].year
    last_year = trades[-1]["signal_time"].year

    rows = []

    for year in range(first_year, last_year + 1):
        start = datetime(year, 1, 1, tzinfo=timezone.utc)
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        s = calc_stats(subset(trades, start, end))
        rows.append({
            "lookback": lookback,
            "variant_id": variant_id,
            "year": year,
            "complete_year": year < NOW.year,
            **s,
        })

    return rows


def ev_candidate_rolling_rows(lookback, variant_id, trades):
    if not trades:
        return []

    start_month = month_floor(trades[0]["signal_time"])
    end_complete = month_floor(NOW)

    rows = []

    for months in (12, 24, 36):
        for row in rolling_rows(
            f"LB{lookback}_{variant_id}",
            trades,
            months,
            start_month,
            end_complete,
        ):
            row["lookback"] = lookback
            row["variant_id"] = variant_id
            rows.append(row)

    return rows


def ev_candidate_rolling_summary(rows):
    grouped = defaultdict(list)

    for row in rows:
        grouped[
            (
                row["lookback"],
                row["variant_id"],
                row["months"],
            )
        ].append(row)

    out = []

    for (lookback, variant_id, months), group in grouped.items():
        active = [x for x in group if x["trades"] > 0]
        positive = [x for x in active if x["total_r"] > 0]

        values = [x["total_r"] for x in active]

        out.append({
            "lookback": lookback,
            "variant_id": variant_id,
            "months": months,
            "active_windows": len(active),
            "positive_active_windows": len(positive),
            "positive_active_windows_pct": pct(
                len(positive),
                len(active),
            ),
            "median_r_active": safe_median(values),
            "worst_r_active": min(values) if values else 0.0,
            "best_r_active": max(values) if values else 0.0,
        })

    return out


def ev_portfolio_rolling_rows(candidate_id, mode, trades, sim):
    first_entry = min(t["entry_time"] for t in trades)
    start_month = month_floor(first_entry)
    end_complete = month_floor(NOW)

    rows = []

    for months in (12, 24, 36):
        rr = mixed_equity_rolling_rows(
            mode,
            sim,
            0.01,
            0.01,
            trades,
            months,
            start_month,
            end_complete,
        )

        for row in rr:
            row["candidate_id"] = candidate_id
            rows.append(row)

    return rows


def ev_portfolio_calendar_rows(candidate_id, mode, trades, sim):
    first_year = min(t["entry_time"] for t in trades).year

    rows = mixed_equity_calendar_rows(
        mode,
        sim,
        0.01,
        0.01,
        trades,
        first_year,
        NOW.year,
    )

    for row in rows:
        row["candidate_id"] = candidate_id

    return rows



def ev_portfolio_rolling_summary(rows):
    grouped = defaultdict(list)

    for row in rows:
        grouped[
            (
                row["candidate_id"],
                row["portfolio_mode"],
                row["h1_risk_pct"],
                row["m15_risk_pct"],
                row["months"],
            )
        ].append(row)

    out = []

    for key, group in grouped.items():
        candidate_id, mode, h1p, m15p, months = key
        active = [x for x in group if x["realized_exits"] > 0]
        positive = [
            x for x in active
            if x["compounded_return_pct"] > 0
        ]

        worst = (
            min(active, key=lambda x: x["compounded_return_pct"])
            if active else None
        )
        best = (
            max(active, key=lambda x: x["compounded_return_pct"])
            if active else None
        )

        out.append({
            "candidate_id": candidate_id,
            "portfolio_mode": mode,
            "h1_risk_pct": h1p,
            "m15_risk_pct": m15p,
            "months": months,
            "active_windows": len(active),
            "positive_active_windows": len(positive),
            "positive_active_windows_pct": pct(
                len(positive),
                len(active),
            ),
            "median_return_pct_active": safe_median(
                x["compounded_return_pct"] for x in active
            ),
            "worst_start_utc": (
                worst["start_utc"] if worst else ""
            ),
            "worst_return_pct": (
                worst["compounded_return_pct"] if worst else 0.0
            ),
            "best_start_utc": (
                best["start_utc"] if best else ""
            ),
            "best_return_pct": (
                best["compounded_return_pct"] if best else 0.0
            ),
        })

    return out


def ev_portfolio_calendar_summary(rows):
    grouped = defaultdict(list)

    for row in rows:
        grouped[
            (
                row["candidate_id"],
                row["portfolio_mode"],
                row["h1_risk_pct"],
                row["m15_risk_pct"],
            )
        ].append(row)

    out = []

    for key, group in grouped.items():
        candidate_id, mode, h1p, m15p = key

        complete = [x for x in group if x["complete_year"]]
        active = [x for x in complete if x["realized_exits"] > 0]
        positive = [
            x for x in active
            if x["compounded_return_pct"] > 0
        ]

        worst = (
            min(active, key=lambda x: x["compounded_return_pct"])
            if active else None
        )
        best = (
            max(active, key=lambda x: x["compounded_return_pct"])
            if active else None
        )

        out.append({
            "candidate_id": candidate_id,
            "portfolio_mode": mode,
            "h1_risk_pct": h1p,
            "m15_risk_pct": m15p,
            "completed_active_years": len(active),
            "positive_completed_active_years": len(positive),
            "positive_completed_active_years_pct": pct(
                len(positive),
                len(active),
            ),
            "median_calendar_return_pct": safe_median(
                x["compounded_return_pct"] for x in active
            ),
            "worst_year": worst["year"] if worst else "",
            "worst_year_return_pct": (
                worst["compounded_return_pct"] if worst else 0.0
            ),
            "best_year": best["year"] if best else "",
            "best_year_return_pct": (
                best["compounded_return_pct"] if best else 0.0
            ),
        })

    return out


def ev_candidate_calendar_summary(rows):
    grouped = defaultdict(list)

    for row in rows:
        grouped[(row["lookback"], row["variant_id"])].append(row)

    out = []

    for (lookback, variant_id), group in grouped.items():
        active = [
            x for x in group
            if x["complete_year"] and x["trades"] > 0
        ]
        positive = [x for x in active if x["total_r"] > 0]

        worst = (
            min(active, key=lambda x: x["total_r"])
            if active else None
        )
        best = (
            max(active, key=lambda x: x["total_r"])
            if active else None
        )

        out.append({
            "lookback": lookback,
            "variant_id": variant_id,
            "completed_active_years": len(active),
            "positive_completed_active_years": len(positive),
            "positive_completed_active_years_pct": pct(
                len(positive),
                len(active),
            ),
            "median_calendar_r": safe_median(
                x["total_r"] for x in active
            ),
            "worst_year": worst["year"] if worst else "",
            "worst_year_r": worst["total_r"] if worst else 0.0,
            "best_year": best["year"] if best else "",
            "best_year_r": best["total_r"] if best else 0.0,
        })

    return out



# ============================================================
# EUR/JPY H1 SHORT — FINAL VALIDATION + CURRENT21 -> 22 TEST
# ============================================================
#
# The large block above is retained deliberately because it is the exact
# frozen machinery used to reconstruct the existing 20-strategy H1+M15
# portfolio and the now-live EUR/JPY H1 LONG.
#
# CURRENT 21 BASELINE
# -------------------
# Existing 20 locked strategies:
#   5 H1 LONG + 5 H1 SHORT
#   5 M15 LONG + 5 M15 SHORT
#
# Plus live EUR/JPY H1 LONG:
#   exact bullish engulf
#   BR >= 1.30
#   body >= 1.25 ATR14
#   abs(signal low - prior 15-bar low) <= 0.15 ATR14
#   exclude Friday, America/New_York
#   exclude signal-open NY 08:00-08:59
#   RR 5.50
#   stop = signal low - 10 ticks
#   historical adverse fill = +5 ticks = +0.5 pip
#
# SHORT FAMILY
# ------------
# Common raw EUR/JPY H1 SHORT geometry:
#   previous completed H1 ATR14 / previous-20 ATR14 mean <= 0.80
#   bearish body >= 1.10 ATR14
#   signal range >= 1.65 ATR14
#   close < previous 2-bar low
#   RR 7.25
#   stop = signal high + 10 ticks
#   historical adverse fill = -5 ticks = -0.5 pip
#
# FINAL CANDIDATES
# ----------------
# A_RAW
#   no extra context
#
# B_EX_NY19
#   exclude signal candles opening 19:00-19:59 America/New_York
#
# C_EX_TUE_NY19
#   exclude Tuesday + NY 19:00-19:59
#
# D_H4_EMA50_LT_EMA200
#   previous strictly completed H4 EMA50 < EMA200
#
# Candidate C is the current practical favourite from context research.
# Candidate D is retained as the high-selectivity control.
#
# Every candidate gets:
#   - exact standalone rebuild
#   - 2002-2017 / 2018+ split
#   - four broad eras
#   - last 5Y / 2Y / 1Y
#   - calendar-year analysis
#   - rolling 12 / 24 / 36M
#   - 0.5x / 1.0x / 1.5x / 2.0x cost stress
#   - exact addition ONE AT A TIME to CURRENT 21
#   - current non-hedging-safe gate
#   - event-driven 1% H1 + 1% M15 compounding
#   - portfolio calendar + rolling comparison
#   - trade-frequency comparison
#
# IMPORTANT NON-HEDGING BEHAVIOUR
# -------------------------------
# EUR/JPY LONG and candidate SHORT may conflict.
#
# Same-direction overlaps are allowed.
# Opposite-direction same-pair entries are blocked while the opposite
# position remains open. This uses the same current portfolio gate as the
# rest of the live-safe analysis.
#
# READ ONLY. NEVER SENDS ORDERS.
# ============================================================

SV_STATUS = {
    "state": "not_started",
    "message": "EUR/JPY H1 SHORT final validation not started",
    "progress": 0,
}

# The exact current20 rebuild helper above writes status to EV_STATUS.
# Point it at this runner's status dictionary so Railway reports one coherent
# progress stream.
EV_STATUS = SV_STATUS

SV_BUNDLE = "EURJPY_H1_SHORT_FINAL_VALIDATION_AND_21_TO_22_PORTFOLIO_ADD_RESULTS.zip"

SV_OUT = {
    "baseline_parity": "eurjpy_short_final_current21_control_parity.csv",
    "baseline_summary": "eurjpy_short_final_current21_baseline_summary.csv",
    "candidate_summary": "eurjpy_short_final_candidate_summary.csv",
    "candidate_periods": "eurjpy_short_final_candidate_periods.csv",
    "candidate_cost_stress": "eurjpy_short_final_candidate_cost_stress.csv",
    "candidate_calendar": "eurjpy_short_final_candidate_calendar.csv",
    "candidate_calendar_summary": "eurjpy_short_final_candidate_calendar_summary.csv",
    "candidate_rolling": "eurjpy_short_final_candidate_rolling.csv",
    "candidate_rolling_summary": "eurjpy_short_final_candidate_rolling_summary.csv",
    "candidate_trades": "eurjpy_short_final_candidate_trades.csv",
    "portfolio_summary": "eurjpy_short_final_21_to_22_portfolio_summary.csv",
    "portfolio_delta": "eurjpy_short_final_21_to_22_portfolio_delta.csv",
    "portfolio_frequency": "eurjpy_short_final_21_to_22_portfolio_frequency.csv",
    "portfolio_rolling": "eurjpy_short_final_21_to_22_portfolio_rolling.csv",
    "portfolio_rolling_summary": "eurjpy_short_final_21_to_22_portfolio_rolling_summary.csv",
    "portfolio_calendar": "eurjpy_short_final_21_to_22_portfolio_calendar.csv",
    "portfolio_calendar_summary": "eurjpy_short_final_21_to_22_portfolio_calendar_summary.csv",
    "portfolio_gate_rejections": "eurjpy_short_final_21_to_22_gate_rejections.csv",
    "decision_matrix": "eurjpy_short_final_decision_matrix.csv",
    "notes": "eurjpy_short_final_notes.csv",
}

SV_PAIR = "EUR_JPY"
SV_TICK = 0.001
SV_PIP = 0.01
SV_STOP_BUFFER_TICKS = 10
SV_BASE_COST_TICKS = 5.0

SV_COMPRESSION_MAX = 0.80
SV_BODY_ATR_MIN = 1.10
SV_RANGE_ATR_MIN = 1.65
SV_LOOKBACK = 2
SV_RR = 7.25
SV_NY = ZoneInfo("America/New_York")

SV_CANDIDATES = [
    {
        "candidate_id": "A_RAW_LB2_RR7_25",
        "exclude_tuesday": False,
        "exclude_ny19": False,
        "require_h4_ema50_lt_ema200": False,
        "reference_min_trades": 116,
    },
    {
        "candidate_id": "B_EX_NY19_LB2_RR7_25",
        "exclude_tuesday": False,
        "exclude_ny19": True,
        "require_h4_ema50_lt_ema200": False,
        "reference_min_trades": 108,
    },
    {
        "candidate_id": "C_EX_TUE_NY19_LB2_RR7_25",
        "exclude_tuesday": True,
        "exclude_ny19": True,
        "require_h4_ema50_lt_ema200": False,
        "reference_min_trades": 88,
    },
    {
        "candidate_id": "D_H4_EMA50_LT_EMA200_LB2_RR7_25",
        "exclude_tuesday": False,
        "exclude_ny19": False,
        "require_h4_ema50_lt_ema200": True,
        "reference_min_trades": 62,
    },
]

SV_CURRENT21_EURJPY_LONG_REFERENCE_MIN = 74


# ============================================================
# SHORT CANDIDATE FEATURES / STRICT H4 STATE
# ============================================================

def sv_previous_mean(values, lookback):
    """
    Mean of the previous `lookback` values, excluding current index.

    At H1 index i this reproduces the context-research convention:
        mean(values[i-lookback:i])
    """
    out = [None] * len(values)
    q = []
    total = 0.0
    bad = 0

    for i in range(len(values)):
        add = i - 1
        if add >= 0:
            v = values[add]
            q.append(v)
            if v is None or not math.isfinite(float(v)):
                bad += 1
            else:
                total += float(v)

        if len(q) > lookback:
            old = q.pop(0)
            if old is None or not math.isfinite(float(old)):
                bad -= 1
            else:
                total -= float(old)

        if len(q) == lookback and bad == 0:
            out[i] = total / lookback

    return out


def sv_prior_low(candles, index, lookback):
    if index < lookback:
        return None
    return min(
        float(candles[j]["low"])
        for j in range(index - lookback, index)
    )


def sv_build_h1_raw_features(candles):
    atr = atr_series(candles, 14)
    atr_mean20_prev = sv_previous_mean(atr, 20)

    compression = [None] * len(candles)
    body_atr = [None] * len(candles)
    range_atr = [None] * len(candles)

    for i, candle in enumerate(candles):
        a = atr[i]

        if a is not None and math.isfinite(float(a)) and float(a) > 0:
            o = float(candle["open"])
            h = float(candle["high"])
            l = float(candle["low"])
            c = float(candle["close"])

            body_atr[i] = (o - c) / float(a)
            range_atr[i] = (h - l) / float(a)

        if i > 0:
            prev_atr = atr[i - 1]
            mean20 = atr_mean20_prev[i]

            if (
                prev_atr is not None
                and mean20 is not None
                and math.isfinite(float(prev_atr))
                and math.isfinite(float(mean20))
                and float(mean20) > 0
            ):
                compression[i] = float(prev_atr) / float(mean20)

    return {
        "atr": atr,
        "compression": compression,
        "body_atr": body_atr,
        "range_atr": range_atr,
    }


def sv_ema(values, length):
    """
    Standard EMA seeded by SMA(length), matching the context runner.
    """
    out = [None] * len(values)

    if len(values) < length:
        return out

    seed = sum(float(x) for x in values[:length]) / length
    out[length - 1] = seed

    alpha = 2.0 / (length + 1.0)
    prev = seed

    for i in range(length, len(values)):
        prev = alpha * float(values[i]) + (1.0 - alpha) * prev
        out[i] = prev

    return out


def sv_h4_completion_times(candles):
    """
    H4 state becomes available at the ACTUAL next H4 candle open.
    Weekend/gap aware; final-candle fallback is +4h.
    """
    times = [x["time"] for x in candles]
    out = []

    for i, t in enumerate(times):
        if i + 1 < len(times):
            out.append(times[i + 1])
        else:
            out.append(t + timedelta(hours=4))

    return out


def sv_map_strict_h4_ema_alignment(h1_times, h4_candles):
    closes = [float(x["close"]) for x in h4_candles]
    ema50 = sv_ema(closes, 50)
    ema200 = sv_ema(closes, 200)
    complete = sv_h4_completion_times(h4_candles)

    allowed = [False] * len(h1_times)
    state_index = [-1] * len(h1_times)

    j = -1

    for i, signal_open in enumerate(h1_times):
        while j + 1 < len(complete) and complete[j + 1] <= signal_open:
            j += 1

        state_index[i] = j

        if j < 0:
            continue

        fast = ema50[j]
        slow = ema200[j]

        if (
            fast is not None
            and slow is not None
            and math.isfinite(float(fast))
            and math.isfinite(float(slow))
            and float(fast) < float(slow)
        ):
            allowed[i] = True

    return {
        "ema50_lt_ema200": allowed,
        "state_index": state_index,
    }


def sv_raw_signal(candles, features, index):
    if index < max(21, SV_LOOKBACK):
        return False

    candle = candles[index]
    o = float(candle["open"])
    c = float(candle["close"])

    if c >= o:
        return False

    body_atr = features["body_atr"][index]
    range_atr = features["range_atr"][index]
    compression = features["compression"][index]

    if (
        body_atr is None
        or range_atr is None
        or compression is None
        or not math.isfinite(float(body_atr))
        or not math.isfinite(float(range_atr))
        or not math.isfinite(float(compression))
    ):
        return False

    if float(body_atr) < SV_BODY_ATR_MIN:
        return False

    if float(range_atr) < SV_RANGE_ATR_MIN:
        return False

    if float(compression) > SV_COMPRESSION_MAX:
        return False

    prior_low = sv_prior_low(candles, index, SV_LOOKBACK)

    if prior_low is None:
        return False

    if c >= prior_low:
        return False

    return True


def sv_context_allowed(candles, index, candidate, h4_state):
    t = candles[index]["time"]
    ny = t.astimezone(SV_NY)

    if candidate["exclude_tuesday"] and ny.weekday() == 1:
        return False

    if candidate["exclude_ny19"] and ny.hour == 19:
        return False

    if candidate["require_h4_ema50_lt_ema200"]:
        if not h4_state["ema50_lt_ema200"][index]:
            return False

    return True


# ============================================================
# SHORT EXECUTION
# ============================================================

def sv_find_exit(candles, signal_index, stop, target):
    for index in range(signal_index + 1, len(candles)):
        candle = candles[index]
        o = float(candle["open"])
        h = float(candle["high"])
        l = float(candle["low"])

        stop_hit = h >= stop
        target_hit = l <= target

        if not stop_hit and not target_hit:
            continue

        if stop_hit and target_hit:
            # Locked H1 SHORT same-bar convention:
            # high closer to candle open => STOP first; otherwise TARGET.
            reason = "STOP" if abs(h - o) < abs(o - l) else "TARGET"
        elif stop_hit:
            reason = "STOP"
        else:
            reason = "TARGET"

        return index, reason

    return None, None


def sv_build_candidate_trades(
    candles,
    features,
    h4_state,
    candidate,
    cost_multiplier=1.0,
):
    """
    Exact p0 EUR/JPY H1 SHORT candidate stream.

    Signal on exact exit candle remains eligible.
    """
    candidate_id = candidate["candidate_id"]
    strategy_id = f"EUR_JPY_H1_SHORT_{candidate_id}"

    raw = []

    for index in range(max(21, SV_LOOKBACK), len(candles)):
        t = candles[index]["time"]

        if t < START:
            continue

        if t >= NOW:
            break

        if not sv_raw_signal(candles, features, index):
            continue

        if not sv_context_allowed(
            candles,
            index,
            candidate,
            h4_state,
        ):
            continue

        raw.append(index)

    trades = []
    pointer = 0
    cost_ticks = SV_BASE_COST_TICKS * float(cost_multiplier)

    while pointer < len(raw):
        signal_index = raw[pointer]
        signal = candles[signal_index]

        reference_entry = float(signal["close"])
        historical_fill = reference_entry - cost_ticks * SV_TICK
        stop = float(signal["high"]) + SV_STOP_BUFFER_TICKS * SV_TICK

        reference_risk = stop - reference_entry
        actual_risk = stop - historical_fill

        if reference_risk <= 0 or actual_risk <= 0:
            pointer += 1
            continue

        target = reference_entry - SV_RR * reference_risk

        exit_index, reason = sv_find_exit(
            candles,
            signal_index,
            stop,
            target,
        )

        if exit_index is None:
            break

        exit_price = target if reason == "TARGET" else stop
        result_r = (historical_fill - exit_price) / actual_risk
        exit_candle = candles[exit_index]

        trades.append({
            "pair": SV_PAIR,
            "strategy_id": strategy_id,
            "trigger": candidate_id,
            "timeframe": "H1",
            "side": "SELL",
            "signal_time": signal["time"],
            "entry_time": signal["time"] + timedelta(hours=1),
            "exit_time": exit_candle["time"],
            "exit_event_time": exit_candle["time"] + timedelta(hours=1),
            "rr": SV_RR,
            "reference_entry": reference_entry,
            "historical_fill": historical_fill,
            "stop": stop,
            "target": target,
            "exit_price": exit_price,
            "result": reason,
            "r": float(result_r),
            "bars_held": exit_index - signal_index,
            "hold_hours": float(exit_index - signal_index),
            "early_exit": False,
            "lookback": SV_LOOKBACK,
            "candidate_id": candidate_id,
            "exclude_tuesday": candidate["exclude_tuesday"],
            "exclude_ny19": candidate["exclude_ny19"],
            "require_h4_ema50_lt_ema200": (
                candidate["require_h4_ema50_lt_ema200"]
            ),
            "cost_multiplier": float(cost_multiplier),
            "cost_model": f"EURJPY_H1_SHORT_{cost_ticks:g}_ADVERSE_TICKS",
        })

        # p0, with exact exit-candle signal eligible.
        pointer = bisect.bisect_left(
            raw,
            exit_index,
            lo=pointer + 1,
        )

    return trades


# ============================================================
# CURRENT 21 BASELINE
# ============================================================

def sv_rebuild_current21(eurjpy_h1, eurjpy_atr):
    """
    Rebuild exact current20, then append the now-live EUR/JPY H1 LONG and
    re-run the live-safe non-hedging gate.

    The long is generated by the exact final-validation helper retained
    above, so the 21st strategy is not approximated or hard-coded.
    """
    current20 = ev_rebuild_current20()

    long_variant = next(
        x for x in EV_VARIANTS
        if x["variant_id"] == "EXCLUDE_FRIDAY_NY08"
    )

    long_trades_raw = ev_build_candidate_trades(
        eurjpy_h1,
        eurjpy_atr,
        15,
        long_variant,
        cost_multiplier=1.0,
    )

    if len(long_trades_raw) < SV_CURRENT21_EURJPY_LONG_REFERENCE_MIN:
        raise RuntimeError(
            "EUR/JPY live LONG reproduction fell below reference: "
            f"{len(long_trades_raw)} < "
            f"{SV_CURRENT21_EURJPY_LONG_REFERENCE_MIN}"
        )

    # Rename to the live strategy identity used by the 21-strategy system.
    long_trades = []

    for trade in long_trades_raw:
        t = dict(trade)
        t["strategy_id"] = "EUR_JPY_H1_LONG"
        t["trigger"] = "LIVE_LOCKED_LB15_EX_FRIDAY_NY08"
        long_trades.append(t)

    by_strategy = defaultdict(list)

    for sid, trades in current20["by_strategy"].items():
        by_strategy[sid].extend(trades)

    by_strategy["EUR_JPY_H1_LONG"].extend(long_trades)

    independent = sorted(
        current20["independent"] + long_trades,
        key=lambda t: (t["entry_time"], t["strategy_id"]),
    )

    live_h1, rejected_h1 = apply_live_safe_nonhedging_gate(
        independent,
        "H1_FIRST",
    )

    live_m15, rejected_m15 = apply_live_safe_nonhedging_gate(
        independent,
        "M15_FIRST",
    )

    parity = list(current20["parity"])
    parity.append({
        "strategy_id": "EUR_JPY_H1_LONG",
        "reference_min_trades": SV_CURRENT21_EURJPY_LONG_REFERENCE_MIN,
        "current_trades": len(long_trades),
        "status": (
            "PASS_EQUAL"
            if len(long_trades) == SV_CURRENT21_EURJPY_LONG_REFERENCE_MIN
            else "PASS_NEWER_TRADES"
        ),
    })

    return {
        "by_strategy": by_strategy,
        "independent": independent,
        "live_h1_first": live_h1,
        "live_m15_first": live_m15,
        "rejected_h1_first": rejected_h1,
        "rejected_m15_first": rejected_m15,
        "parity": parity,
        "eurjpy_long_trades": long_trades,
    }


# ============================================================
# STANDALONE VALIDATION HELPERS
# ============================================================

def sv_period_rows(candidate_id, trades):
    periods = [
        ("FULL", None, None),
        (
            "DEV_2002_2017",
            None,
            datetime(2018, 1, 1, tzinfo=timezone.utc),
        ),
        (
            "VALIDATION_2018_PLUS",
            datetime(2018, 1, 1, tzinfo=timezone.utc),
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
        ("LAST_5Y", NOW - timedelta(days=365.2425 * 5), None),
        ("LAST_2Y", NOW - timedelta(days=365.2425 * 2), None),
        ("LAST_1Y", NOW - timedelta(days=365.2425), None),
    ]

    rows = []

    for label, start, end in periods:
        stats = calc_stats(subset(trades, start, end))
        rows.append({
            "candidate_id": candidate_id,
            "period": label,
            **stats,
        })

    return rows


def sv_summary_row(candidate, trades):
    candidate_id = candidate["candidate_id"]

    full = calc_stats(trades)

    dev = calc_stats(
        subset(
            trades,
            None,
            datetime(2018, 1, 1, tzinfo=timezone.utc),
        )
    )

    validation = calc_stats(
        subset(
            trades,
            datetime(2018, 1, 1, tzinfo=timezone.utc),
            None,
        )
    )

    era_ranges = [
        (
            datetime(2002, 1, 1, tzinfo=timezone.utc),
            datetime(2008, 1, 1, tzinfo=timezone.utc),
        ),
        (
            datetime(2008, 1, 1, tzinfo=timezone.utc),
            datetime(2014, 1, 1, tzinfo=timezone.utc),
        ),
        (
            datetime(2014, 1, 1, tzinfo=timezone.utc),
            datetime(2020, 1, 1, tzinfo=timezone.utc),
        ),
        (
            datetime(2020, 1, 1, tzinfo=timezone.utc),
            None,
        ),
    ]

    eras = [
        calc_stats(subset(trades, start, end))
        for start, end in era_ranges
    ]

    last5 = calc_stats(
        subset(
            trades,
            NOW - timedelta(days=365.2425 * 5),
            None,
        )
    )

    last2 = calc_stats(
        subset(
            trades,
            NOW - timedelta(days=365.2425 * 2),
            None,
        )
    )

    last1 = calc_stats(
        subset(
            trades,
            NOW - timedelta(days=365.2425),
            None,
        )
    )

    return {
        "candidate_id": candidate_id,
        "lookback": SV_LOOKBACK,
        "rr": SV_RR,
        "compression_max": SV_COMPRESSION_MAX,
        "body_atr_min": SV_BODY_ATR_MIN,
        "range_atr_min": SV_RANGE_ATR_MIN,
        "exclude_tuesday": candidate["exclude_tuesday"],
        "exclude_ny19": candidate["exclude_ny19"],
        "require_h4_ema50_lt_ema200": (
            candidate["require_h4_ema50_lt_ema200"]
        ),
        **{f"full_{k}": v for k, v in full.items()},
        "dev_2002_2017_trades": dev["trades"],
        "dev_2002_2017_pf": dev["profit_factor"],
        "dev_2002_2017_r": dev["total_r"],
        "validation_2018_plus_trades": validation["trades"],
        "validation_2018_plus_pf": validation["profit_factor"],
        "validation_2018_plus_r": validation["total_r"],
        "both_temporal_splits_positive": (
            dev["trades"] > 0
            and validation["trades"] > 0
            and dev["total_r"] > 0
            and validation["total_r"] > 0
        ),
        "min_temporal_split_pf": min(
            dev["profit_factor"] if dev["trades"] else 0.0,
            validation["profit_factor"] if validation["trades"] else 0.0,
        ),
        "positive_eras": sum(
            x["trades"] > 0 and x["total_r"] > 0
            for x in eras
        ),
        "era_2002_2007_r": eras[0]["total_r"],
        "era_2008_2013_r": eras[1]["total_r"],
        "era_2014_2019_r": eras[2]["total_r"],
        "era_2020_now_r": eras[3]["total_r"],
        "last5y_trades": last5["trades"],
        "last5y_pf": last5["profit_factor"],
        "last5y_r": last5["total_r"],
        "last2y_trades": last2["trades"],
        "last2y_pf": last2["profit_factor"],
        "last2y_r": last2["total_r"],
        "last1y_trades": last1["trades"],
        "last1y_pf": last1["profit_factor"],
        "last1y_r": last1["total_r"],
    }


def sv_calendar_rows(candidate_id, trades):
    if not trades:
        return []

    first_year = trades[0]["signal_time"].year
    last_year = trades[-1]["signal_time"].year

    rows = []

    for year in range(first_year, last_year + 1):
        start = datetime(year, 1, 1, tzinfo=timezone.utc)
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)

        stats = calc_stats(subset(trades, start, end))

        rows.append({
            "candidate_id": candidate_id,
            "year": year,
            "complete_year": year < NOW.year,
            **stats,
        })

    return rows


def sv_calendar_summary(rows):
    grouped = defaultdict(list)

    for row in rows:
        grouped[row["candidate_id"]].append(row)

    out = []

    for candidate_id, group in grouped.items():
        active = [
            x for x in group
            if x["complete_year"] and x["trades"] > 0
        ]
        positive = [
            x for x in active
            if x["total_r"] > 0
        ]

        worst = (
            min(active, key=lambda x: x["total_r"])
            if active else None
        )
        best = (
            max(active, key=lambda x: x["total_r"])
            if active else None
        )

        out.append({
            "candidate_id": candidate_id,
            "completed_active_years": len(active),
            "positive_completed_active_years": len(positive),
            "positive_completed_active_years_pct": pct(
                len(positive),
                len(active),
            ),
            "median_calendar_r": safe_median(
                x["total_r"] for x in active
            ),
            "worst_year": worst["year"] if worst else "",
            "worst_year_r": worst["total_r"] if worst else 0.0,
            "best_year": best["year"] if best else "",
            "best_year_r": best["total_r"] if best else 0.0,
        })

    return out


def sv_rolling_rows(candidate_id, trades):
    if not trades:
        return []

    start_month = month_floor(trades[0]["signal_time"])
    end_complete = month_floor(NOW)
    rows = []

    for months in (12, 24, 36):
        for row in rolling_rows(
            candidate_id,
            trades,
            months,
            start_month,
            end_complete,
        ):
            row["candidate_id"] = candidate_id
            rows.append(row)

    return rows


def sv_rolling_summary(rows):
    grouped = defaultdict(list)

    for row in rows:
        grouped[
            (
                row["candidate_id"],
                row["months"],
            )
        ].append(row)

    out = []

    for (candidate_id, months), group in grouped.items():
        active = [x for x in group if x["trades"] > 0]
        positive = [x for x in active if x["total_r"] > 0]
        values = [x["total_r"] for x in active]

        out.append({
            "candidate_id": candidate_id,
            "months": months,
            "active_windows": len(active),
            "positive_active_windows": len(positive),
            "positive_active_windows_pct": pct(
                len(positive),
                len(active),
            ),
            "median_r_active": safe_median(values),
            "worst_r_active": min(values) if values else 0.0,
            "best_r_active": max(values) if values else 0.0,
        })

    return out


def sv_serialise_trade(trade):
    row = dict(trade)

    for key, value in list(row.items()):
        if isinstance(value, datetime):
            row[key] = iso(value)

    return row


# ============================================================
# DECISION MATRIX
# ============================================================

def sv_decision_matrix(
    candidate_summary,
    cost_rows,
    rolling_summary_rows,
    portfolio_delta_rows,
    portfolio_roll_summary_rows,
):
    cost_lookup = {}
    for row in cost_rows:
        if abs(float(row["cost_multiplier"]) - 2.0) < 1e-9:
            cost_lookup[row["candidate_id"]] = row

    rolling_lookup = {
        (row["candidate_id"], int(row["months"])): row
        for row in rolling_summary_rows
    }

    portfolio_delta_lookup = {
        (row["candidate_id"], row["portfolio_mode"]): row
        for row in portfolio_delta_rows
    }

    portfolio_roll_lookup = {
        (
            row["candidate_id"],
            row["portfolio_mode"],
            int(row["months"]),
        ): row
        for row in portfolio_roll_summary_rows
    }

    out = []

    for row in candidate_summary:
        cid = row["candidate_id"]
        cost2 = cost_lookup.get(cid, {})
        r24 = rolling_lookup.get((cid, 24), {})
        r36 = rolling_lookup.get((cid, 36), {})

        for mode in ("LIVE_SAFE_H1_FIRST", "LIVE_SAFE_M15_FIRST"):
            delta = portfolio_delta_lookup.get((cid, mode), {})
            p12 = portfolio_roll_lookup.get((cid, mode, 12), {})
            p24 = portfolio_roll_lookup.get((cid, mode, 24), {})
            p36 = portfolio_roll_lookup.get((cid, mode, 36), {})

            out.append({
                "candidate_id": cid,
                "portfolio_mode": mode,
                "standalone_trades": row["full_trades"],
                "standalone_pf": row["full_profit_factor"],
                "standalone_total_r": row["full_total_r"],
                "standalone_expectancy_r": row["full_expectancy_r"],
                "standalone_max_dd_r": row["full_max_drawdown_r"],
                "standalone_min_split_pf": row["min_temporal_split_pf"],
                "standalone_last5y_r": row["last5y_r"],
                "standalone_last2y_r": row["last2y_r"],
                "standalone_2x_cost_pf": cost2.get("profit_factor", 0.0),
                "standalone_2x_cost_r": cost2.get("total_r", 0.0),
                "standalone_rolling24_positive_pct": (
                    r24.get("positive_active_windows_pct", 0.0)
                ),
                "standalone_rolling36_positive_pct": (
                    r36.get("positive_active_windows_pct", 0.0)
                ),
                "portfolio_delta_trades": delta.get("delta_trades", 0),
                "portfolio_delta_total_r": delta.get("delta_total_r", 0.0),
                "portfolio_delta_pf": delta.get("delta_profit_factor", 0.0),
                "portfolio_delta_cagr_pct_points": (
                    delta.get("delta_cagr_pct_1pct_h1_1pct_m15", 0.0)
                ),
                "portfolio_delta_closed_dd_pct_points": (
                    delta.get("delta_max_closed_equity_dd_pct", 0.0)
                ),
                "portfolio_delta_floor_dd_pct_points": (
                    delta.get("delta_max_open_risk_floor_dd_pct", 0.0)
                ),
                "portfolio_delta_max_positions": (
                    delta.get("delta_max_open_positions", 0)
                ),
                "portfolio_rolling12_positive_pct": (
                    p12.get("positive_active_windows_pct", 0.0)
                ),
                "portfolio_rolling24_positive_pct": (
                    p24.get("positive_active_windows_pct", 0.0)
                ),
                "portfolio_rolling36_positive_pct": (
                    p36.get("positive_active_windows_pct", 0.0)
                ),
                "portfolio_worst12_return_pct": (
                    p12.get("worst_return_pct", 0.0)
                ),
                "portfolio_worst24_return_pct": (
                    p24.get("worst_return_pct", 0.0)
                ),
                "portfolio_worst36_return_pct": (
                    p36.get("worst_return_pct", 0.0)
                ),
            })

    return out


# ============================================================
# MAIN FINAL VALIDATION
# ============================================================

def run_eurjpy_short_final_validation():
    try:
        SV_STATUS.update(
            state="starting",
            message="Starting EUR/JPY H1 SHORT final validation + current21 comparison",
            progress=1,
        )

        # ----------------------------------------------------
        # 1) Fetch EUR/JPY once for live long + short candidates
        # ----------------------------------------------------
        SV_STATUS.update(
            state="eurjpy_fetch",
            message="Fetching EUR/JPY H1 + H4 history",
            progress=2,
        )

        eurjpy_h1, _ = fetch_history(
            SV_PAIR,
            "H1",
            H1_DATA_WARMUP,
            NOW,
        )

        eurjpy_h4, _ = fetch_history(
            SV_PAIR,
            "H4",
            H1_DAILY_WARMUP,
            NOW,
        )

        if len(eurjpy_h1) < 90000:
            raise RuntimeError(
                f"Incomplete EUR/JPY H1 history: {len(eurjpy_h1)} candles"
            )

        if len(eurjpy_h4) < 20000:
            raise RuntimeError(
                f"Incomplete EUR/JPY H4 history: {len(eurjpy_h4)} candles"
            )

        eurjpy_atr = ev_atr14(eurjpy_h1)
        short_features = sv_build_h1_raw_features(eurjpy_h1)
        h4_state = sv_map_strict_h4_ema_alignment(
            [x["time"] for x in eurjpy_h1],
            eurjpy_h4,
        )

        # ----------------------------------------------------
        # 2) Exact CURRENT 21 baseline
        # ----------------------------------------------------
        SV_STATUS.update(
            state="baseline",
            message="Rebuilding exact current 21-strategy portfolio",
            progress=5,
        )

        baseline = sv_rebuild_current21(
            eurjpy_h1,
            eurjpy_atr,
        )

        write_csv(
            SV_OUT["baseline_parity"],
            baseline["parity"],
        )

        baseline_summary_rows = []
        baseline_lookup = {}

        for mode, trades in [
            ("LIVE_SAFE_H1_FIRST", baseline["live_h1_first"]),
            ("LIVE_SAFE_M15_FIRST", baseline["live_m15_first"]),
        ]:
            row = ev_portfolio_summary_row(
                "CURRENT_21",
                mode,
                trades,
            )

            sim = row.pop("_sim")
            baseline_summary_rows.append(row)

            baseline_lookup[mode] = {
                "row": row,
                "sim": sim,
                "trades": trades,
            }

        write_csv(
            SV_OUT["baseline_summary"],
            baseline_summary_rows,
        )

        # ----------------------------------------------------
        # 3) Standalone final short candidates
        # ----------------------------------------------------
        candidate_summary = []
        candidate_periods = []
        candidate_cost_stress = []
        candidate_calendar = []
        candidate_rolling = []
        candidate_trade_rows = []
        candidate_cache = {}

        for i, candidate in enumerate(SV_CANDIDATES, 1):
            cid = candidate["candidate_id"]

            SV_STATUS.update(
                state="candidate_validation",
                message=f"{i}/{len(SV_CANDIDATES)} validating {cid}",
                progress=55 + i * 4,
            )

            trades = sv_build_candidate_trades(
                eurjpy_h1,
                short_features,
                h4_state,
                candidate,
                cost_multiplier=1.0,
            )

            candidate_cache[cid] = trades

            reference = candidate["reference_min_trades"]

            if len(trades) < reference:
                raise RuntimeError(
                    f"{cid} parity failure: {len(trades)} < {reference}"
                )

            summary = sv_summary_row(candidate, trades)
            summary["reference_min_trades"] = reference
            summary["parity_status"] = (
                "PASS_EQUAL"
                if len(trades) == reference
                else "PASS_NEWER_TRADES"
            )
            candidate_summary.append(summary)

            candidate_periods.extend(
                sv_period_rows(cid, trades)
            )

            candidate_calendar.extend(
                sv_calendar_rows(cid, trades)
            )

            candidate_rolling.extend(
                sv_rolling_rows(cid, trades)
            )

            for multiplier in (0.5, 1.0, 1.5, 2.0):
                stressed = sv_build_candidate_trades(
                    eurjpy_h1,
                    short_features,
                    h4_state,
                    candidate,
                    cost_multiplier=multiplier,
                )

                candidate_cost_stress.append({
                    "candidate_id": cid,
                    "cost_multiplier": multiplier,
                    "adverse_cost_ticks": (
                        SV_BASE_COST_TICKS * multiplier
                    ),
                    "adverse_cost_pips": (
                        SV_BASE_COST_TICKS
                        * SV_TICK
                        / SV_PIP
                        * multiplier
                    ),
                    **calc_stats(stressed),
                })

            for trade in trades:
                candidate_trade_rows.append(
                    sv_serialise_trade(trade)
                )

        candidate_calendar_summary = sv_calendar_summary(
            candidate_calendar
        )
        candidate_rolling_summary = sv_rolling_summary(
            candidate_rolling
        )

        write_csv(SV_OUT["candidate_summary"], candidate_summary)
        write_csv(SV_OUT["candidate_periods"], candidate_periods)
        write_csv(SV_OUT["candidate_cost_stress"], candidate_cost_stress)
        write_csv(SV_OUT["candidate_calendar"], candidate_calendar)
        write_csv(
            SV_OUT["candidate_calendar_summary"],
            candidate_calendar_summary,
        )
        write_csv(SV_OUT["candidate_rolling"], candidate_rolling)
        write_csv(
            SV_OUT["candidate_rolling_summary"],
            candidate_rolling_summary,
        )
        write_csv(SV_OUT["candidate_trades"], candidate_trade_rows)

        # ----------------------------------------------------
        # 4) Add each short ONE AT A TIME to CURRENT 21
        # ----------------------------------------------------
        portfolio_summary_rows = []
        portfolio_delta_rows = []
        portfolio_frequency_rows = []
        portfolio_rolling_rows = []
        portfolio_calendar_rows = []
        gate_rejections = []

        # Baseline rows first.
        for mode in ("LIVE_SAFE_H1_FIRST", "LIVE_SAFE_M15_FIRST"):
            base = baseline_lookup[mode]
            portfolio_summary_rows.append(dict(base["row"]))

            portfolio_frequency_rows.extend(
                ev_frequency_detail(
                    "CURRENT_21",
                    mode,
                    base["trades"],
                )
            )

            portfolio_rolling_rows.extend(
                ev_portfolio_rolling_rows(
                    "CURRENT_21",
                    mode,
                    base["trades"],
                    base["sim"],
                )
            )

            portfolio_calendar_rows.extend(
                ev_portfolio_calendar_rows(
                    "CURRENT_21",
                    mode,
                    base["trades"],
                    base["sim"],
                )
            )

        for i, candidate in enumerate(SV_CANDIDATES, 1):
            cid = candidate["candidate_id"]

            SV_STATUS.update(
                state="portfolio_addition",
                message=f"{i}/{len(SV_CANDIDATES)} testing {cid} as strategy 22",
                progress=72 + i * 5,
            )

            short_trades = candidate_cache[cid]

            combined_independent = sorted(
                baseline["independent"] + short_trades,
                key=lambda t: (
                    t["entry_time"],
                    t["strategy_id"],
                ),
            )

            for priority, mode in [
                ("H1_FIRST", "LIVE_SAFE_H1_FIRST"),
                ("M15_FIRST", "LIVE_SAFE_M15_FIRST"),
            ]:
                accepted, rejected = apply_live_safe_nonhedging_gate(
                    combined_independent,
                    priority,
                )

                short_strategy_id = (
                    f"EUR_JPY_H1_SHORT_{cid}"
                )

                short_accepted = [
                    t for t in accepted
                    if t["strategy_id"] == short_strategy_id
                ]

                short_rejected = [
                    r for r in rejected
                    if r["candidate_strategy_id"] == short_strategy_id
                ]

                long_rejected = [
                    r for r in rejected
                    if r["candidate_strategy_id"] == "EUR_JPY_H1_LONG"
                ]

                for rejection in rejected:
                    gate_rejections.append({
                        "candidate_id": cid,
                        **rejection,
                    })

                row = ev_portfolio_summary_row(
                    cid,
                    mode,
                    accepted,
                )

                sim = row.pop("_sim")

                row.update({
                    "short_trades_before_gate": len(short_trades),
                    "short_trades_accepted": len(short_accepted),
                    "short_trades_rejected_by_long": len(short_rejected),
                    "eurjpy_long_entries_rejected_by_short": len(long_rejected),
                    "eurjpy_long_baseline_trades": len(
                        baseline["eurjpy_long_trades"]
                    ),
                })

                portfolio_summary_rows.append(row)

                baseline_row = baseline_lookup[mode]["row"]

                delta = {
                    "candidate_id": cid,
                    "portfolio_mode": mode,
                    "short_trades_before_gate": len(short_trades),
                    "short_trades_accepted": len(short_accepted),
                    "short_trades_rejected_by_long": len(short_rejected),
                    "eurjpy_long_entries_rejected_by_short": len(long_rejected),
                }

                for field in [
                    "trades",
                    "profit_factor",
                    "total_r",
                    "expectancy_r",
                    "signal_order_max_drawdown_r",
                    "exit_order_max_drawdown_r",
                    "cagr_pct_1pct_h1_1pct_m15",
                    "total_return_pct_1pct_h1_1pct_m15",
                    "max_closed_equity_dd_pct",
                    "max_open_risk_floor_dd_pct",
                    "max_open_positions",
                    "max_open_risk_pct_of_realised_equity",
                ]:
                    delta[f"baseline_{field}"] = baseline_row[field]
                    delta[f"candidate_{field}"] = row[field]
                    delta[f"delta_{field}"] = (
                        row[field] - baseline_row[field]
                    )

                portfolio_delta_rows.append(delta)

                portfolio_frequency_rows.extend(
                    ev_frequency_detail(
                        cid,
                        mode,
                        accepted,
                    )
                )

                portfolio_rolling_rows.extend(
                    ev_portfolio_rolling_rows(
                        cid,
                        mode,
                        accepted,
                        sim,
                    )
                )

                portfolio_calendar_rows.extend(
                    ev_portfolio_calendar_rows(
                        cid,
                        mode,
                        accepted,
                        sim,
                    )
                )

        portfolio_rolling_summary = ev_portfolio_rolling_summary(
            portfolio_rolling_rows
        )
        portfolio_calendar_summary = ev_portfolio_calendar_summary(
            portfolio_calendar_rows
        )

        write_csv(
            SV_OUT["portfolio_summary"],
            portfolio_summary_rows,
        )
        write_csv(
            SV_OUT["portfolio_delta"],
            portfolio_delta_rows,
        )
        write_csv(
            SV_OUT["portfolio_frequency"],
            portfolio_frequency_rows,
        )
        write_csv(
            SV_OUT["portfolio_rolling"],
            portfolio_rolling_rows,
        )
        write_csv(
            SV_OUT["portfolio_rolling_summary"],
            portfolio_rolling_summary,
        )
        write_csv(
            SV_OUT["portfolio_calendar"],
            portfolio_calendar_rows,
        )
        write_csv(
            SV_OUT["portfolio_calendar_summary"],
            portfolio_calendar_summary,
        )
        write_csv(
            SV_OUT["portfolio_gate_rejections"],
            gate_rejections,
        )

        # ----------------------------------------------------
        # 5) One concise decision table
        # ----------------------------------------------------
        decision = sv_decision_matrix(
            candidate_summary,
            candidate_cost_stress,
            candidate_rolling_summary,
            portfolio_delta_rows,
            portfolio_rolling_summary,
        )

        write_csv(
            SV_OUT["decision_matrix"],
            decision,
        )

        notes = [
            {
                "item": "scope",
                "value": (
                    "Final EUR/JPY H1 SHORT validation of exactly four "
                    "predeclared candidates, then current21->22 portfolio addition."
                ),
            },
            {
                "item": "current21",
                "value": (
                    "Current21 is rebuilt from the exact frozen 20-strategy "
                    "H1+M15 machinery above plus the live EUR/JPY H1 LONG "
                    "LB15 / exclude Friday / exclude NY08 / RR5.50."
                ),
            },
            {
                "item": "short_raw_geometry",
                "value": (
                    "Compression<=0.80; bearish body>=1.10 ATR14; range>=1.65 "
                    "ATR14; close below previous 2-bar low; RR7.25; stop signal "
                    "high+10 ticks."
                ),
            },
            {
                "item": "candidates",
                "value": (
                    "A raw; B exclude NY19; C exclude Tuesday+NY19; "
                    "D strict previous-completed H4 EMA50<EMA200."
                ),
            },
            {
                "item": "h4_causality",
                "value": (
                    "D uses the latest H4 candle whose ACTUAL next-H4-open "
                    "completion time is <= H1 signal OPEN; therefore no "
                    "simultaneous or future H4 state leaks into the signal."
                ),
            },
            {
                "item": "cost",
                "value": (
                    "EUR/JPY short baseline adverse fill = signal close -5 ticks "
                    "= -0.005 = -0.5 pip. Standalone candidates are tested at "
                    "0.5x/1x/1.5x/2x this cost."
                ),
            },
            {
                "item": "non_hedging_gate",
                "value": (
                    "Same-pair same-direction overlaps are allowed. Opposite "
                    "EUR/JPY direction is blocked while an opposite trade is open. "
                    "Thus adding the short can both reject short entries while "
                    "the long is open and reject later long entries while the "
                    "short is open."
                ),
            },
            {
                "item": "selection_rule",
                "value": (
                    "Choose on marginal CURRENT21 portfolio value, not standalone "
                    "PF. Prefer higher CAGR/total R and rolling returns without "
                    "material deterioration in conservative open-risk-floor DD, "
                    "while retaining recent standalone activity and 2x-cost edge."
                ),
            },
            {
                "item": "known_context_references",
                "value": (
                    "Prior context research references: A 116 trades; "
                    "B 108; C 88; D 62. A/B/C retained positive recent activity; "
                    "D is intentionally included as a high-selectivity control."
                ),
            },
        ]

        write_csv(
            SV_OUT["notes"],
            notes,
        )

        # ----------------------------------------------------
        # 6) Package
        # ----------------------------------------------------
        SV_STATUS.update(
            state="packaging",
            message="Packaging EUR/JPY short final-validation results",
            progress=97,
        )

        with zipfile.ZipFile(
            SV_BUNDLE,
            "w",
            compression=zipfile.ZIP_DEFLATED,
        ) as z:
            for path in SV_OUT.values():
                if os.path.exists(path):
                    z.write(
                        path,
                        arcname=os.path.basename(path),
                    )

        SV_STATUS.update(
            state="complete",
            message=(
                "EUR/JPY H1 SHORT final validation + current21->22 "
                "portfolio test complete"
            ),
            progress=100,
            results=SV_BUNDLE,
            current21_live_safe_h1_first_trades=len(
                baseline["live_h1_first"]
            ),
            eurjpy_long_trades=len(
                baseline["eurjpy_long_trades"]
            ),
            short_candidates=len(SV_CANDIDATES),
        )

    except Exception as error:
        SV_STATUS.update(
            state="error",
            message=str(error),
            progress=SV_STATUS.get("progress", 0),
        )

        print(
            "EURJPY SHORT FINAL VALIDATION ERROR:",
            repr(error),
            flush=True,
        )


# ============================================================
# ROUTES
# ============================================================

@app.route("/eurjpy-short-final/status")
def eurjpy_short_final_status():
    return jsonify(SV_STATUS)


@app.route("/eurjpy-short-final/results")
def eurjpy_short_final_results():
    if not os.path.exists(SV_BUNDLE):
        return jsonify({
            "status": "not_ready",
            "state": SV_STATUS.get("state"),
            "message": SV_STATUS.get("message"),
        }), 404

    return send_file(
        os.path.abspath(SV_BUNDLE),
        as_attachment=True,
        download_name=SV_BUNDLE,
    )


@app.route("/eurjpy-short-final/info")
def eurjpy_short_final_info():
    return jsonify({
        "service": (
            "EUR/JPY H1 SHORT final validation + "
            "current21-to-22 portfolio addition"
        ),
        "status": SV_STATUS.get("state"),
        "read_only": True,
        "orders_supported": False,
        "current21_eurjpy_long": {
            "lookback": 15,
            "rr": 5.50,
            "exclude_friday": True,
            "exclude_ny08": True,
        },
        "short_raw_family": {
            "pair": SV_PAIR,
            "timeframe": "H1",
            "side": "SELL",
            "compression_max": SV_COMPRESSION_MAX,
            "body_atr_min": SV_BODY_ATR_MIN,
            "range_atr_min": SV_RANGE_ATR_MIN,
            "breakout_lookback": SV_LOOKBACK,
            "rr": SV_RR,
            "stop_buffer_ticks": SV_STOP_BUFFER_TICKS,
            "base_adverse_ticks": SV_BASE_COST_TICKS,
        },
        "candidates": [
            x["candidate_id"]
            for x in SV_CANDIDATES
        ],
        "portfolio_baseline": "exact current locked 21 strategies",
        "portfolio_risk": "1% H1 + 1% M15 per accepted trade",
        "portfolio_constraint": (
            "non-hedging-safe: same-direction overlap allowed, "
            "opposite-direction same-pair entry blocked"
        ),
        "routes": [
            "/eurjpy-short-final/status",
            "/eurjpy-short-final/results",
            "/eurjpy-short-final/info",
        ],
    })


if __name__ == "__main__":
    threading.Thread(
        target=run_eurjpy_short_final_validation,
        daemon=True,
    ).start()

    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "5000")),
        debug=False,
    )
