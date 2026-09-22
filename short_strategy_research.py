# AUD/USD M15 LONG Stage 3: penetration boundary and conditional H1/H4 checks.
# Read-only, no live portfolio changes. Standalone file: Flask / requests / numpy.
# Audited Stage 2 candle/feature/backtest helper logic retained below.
import os
import csv
import time
import bisect
import zipfile
import threading
from copy import deepcopy
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import requests
from flask import Flask, jsonify, send_file

# ============================================================
# AUD/USD M15 LONG — CONDITIONAL STAGE 2 REAUDIT (PROSPECTIVE #28)
# ============================================================
# This is standalone research, NOT a live strategy or a deployed #27.
# The 27 live strategies are not modified or accessed.
# No AUD/USD H1 LONG/SHORT parameters are inherited.
#
# OANDA midpoint M15 candles. ATR14 Wilder/RMA SMA-seeded.
# signal timestamp = signal candle OPEN; reference entry = signal CLOSE.
# Historical BUY fill = reference close + 1.0 pip adverse (10 ticks).
# Stop = signal low - 10 ticks; target from reference-close risk.
# Actual realised R uses fill-to-stop risk. Exit search starts on NEXT M15.
# Exact exit-candle signal is eligible, p0 PER CANDIDATE, no portfolio gate yet.
# If target and stop hit same bar: target first only when high side is
# strictly closer to bar open; otherwise stop first (conservative tie).
# HTF H1/H4/D state only usable at next ACTUAL HTF candle open.
# Stage 1: broad independent geometry families (no contexts) at RR3.5.
# Stage 2: one predeclared context at a time on diverse base geometries.
# Stage 3: RR/local geometry neighbours, one parameter at a time.
# Final: dev/validation, eras, last5/2/1Y, 0.5/1/1.5/2 pip costs,
# rolling12/24/36M, calendar years, full finalist trade ledgers.
# These are searched historical diagnostics, NOT pristine OOS or forecast.
# Read-only. NO ORDER SUBMISSION. Deep validation and exact 26->27
# portfolio-add/nonhedging conflict test required before live deployment.
# ============================================================


app = Flask(__name__)

TOKEN = os.getenv("OANDA_TOKEN")
BASE = os.getenv("OANDA_API_URL", "https://api-fxtrade.oanda.com")
PAIR = "AUD_USD"

START = datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc)
NOW = datetime.now(timezone.utc).replace(second=0, microsecond=0)
WARMUP = START - timedelta(days=900)

NY = ZoneInfo("America/New_York")
LONDON = ZoneInfo("Europe/London")
TOKYO = ZoneInfo("Asia/Tokyo")

TICK = 0.00001
PIP = 0.0001
STOP_TICKS = 10

PRIMARY_COST = 1.0
COSTS = [0.5, 1.0, 1.5, 2.0]

# Diversity is intentional: do not let one family monopolise the next stage.
STAGE1_PER_FAMILY = 2
STAGE1_BASE_KEEP = 14
STAGE2_PER_FAMILY = 2
STAGE2_BASE_KEEP = 12
FINAL_PER_FAMILY = 2
FINAL_KEEP = 14

OUTS = {
    "coverage": "audusd_m15_long_27_coverage.csv",
    "stage1": "audusd_m15_long_27_stage1_summary.csv",
    "stage1_family": "audusd_m15_long_27_stage1_family_summary.csv",
    "stage2": "audusd_m15_long_27_stage2_summary.csv",
    "stage2_family": "audusd_m15_long_27_stage2_family_summary.csv",
    "stage3": "audusd_m15_long_27_stage3_summary.csv",
    "stage3_family": "audusd_m15_long_27_stage3_family_summary.csv",
    "shortlist": "audusd_m15_long_27_shortlist.csv",
    "periods": "audusd_m15_long_27_shortlist_periods.csv",
    "cost": "audusd_m15_long_27_shortlist_cost_stress.csv",
    "rolling": "audusd_m15_long_27_shortlist_rolling.csv",
    "rolling_summary": "audusd_m15_long_27_shortlist_rolling_summary.csv",
    "calendar": "audusd_m15_long_27_shortlist_calendar_years.csv",
    "calendar_summary": "audusd_m15_long_27_shortlist_calendar_summary.csv",
    "trades": "audusd_m15_long_27_shortlist_trades.csv",
    "decision": "audusd_m15_long_27_decision_matrix.csv",
    "notes": "audusd_m15_long_27_notes.csv",
}
BUNDLE = "AUDUSD_M15_LONG_27_BROAD_DISCOVERY_RESULTS.zip"

STATUS = {
    "state": "not_started",
    "message": "Not started",
    "orders_supported": False,
    "trading_enabled": False,
}

# ============================================================
# GENERAL HELPERS
# ============================================================

def iso(dt):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_time(s):
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    if "." in s:
        left, right = s.split(".", 1)
        sign = None
        off = None
        if "+" in right:
            frac, off = right.split("+", 1)
            sign = "+"
        elif "-" in right:
            frac, off = right.split("-", 1)
            sign = "-"
        else:
            frac = right
        s = left + "." + frac[:6].ljust(6, "0")
        if sign:
            s += sign + off
    return datetime.fromisoformat(s).astimezone(timezone.utc)


def write_csv(path, rows):
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
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def download(path):
    if not os.path.exists(path):
        return jsonify({"error": "not ready"}), 404
    return send_file(
        os.path.abspath(path),
        as_attachment=True,
        download_name=os.path.basename(path),
    )


def package_results():
    with zipfile.ZipFile(BUNDLE, "w", zipfile.ZIP_DEFLATED) as z:
        for path in OUTS.values():
            if os.path.exists(path):
                z.write(path, arcname=os.path.basename(path))


def add_months(dt, n):
    m = dt.year * 12 + dt.month - 1 + n
    return datetime(m // 12, m % 12 + 1, 1, tzinfo=timezone.utc)


def month_floor(dt):
    return datetime(dt.year, dt.month, 1, tzinfo=timezone.utc)


def med(values):
    return median(values) if values else 0.0


# ============================================================
# OANDA HISTORY
# ============================================================

def headers():
    if not TOKEN:
        raise RuntimeError("OANDA_TOKEN is not configured")
    return {"Authorization": "Bearer " + TOKEN.strip()}


def fetch_chunk(granularity, start, end):
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

    url = f"{BASE}/v3/instruments/{PAIR}/candles"
    response = requests.get(url, headers=headers(), params=params, timeout=60)
    response.raise_for_status()

    rows = []
    for candle in response.json().get("candles", []):
        if not candle.get("complete", False):
            continue
        mid = candle["mid"]
        rows.append({
            "time": parse_time(candle["time"]),
            "open": float(mid["o"]),
            "high": float(mid["h"]),
            "low": float(mid["l"]),
            "close": float(mid["c"]),
        })
    return rows


def fetch(granularity, start, end, chunk_days):
    current = start
    by_time = {}
    chunk = 0

    while current < end:
        chunk += 1
        nxt = min(current + timedelta(days=chunk_days), end)
        STATUS.update({
            "state": "fetch",
            "message": f"Fetching {granularity} chunk {chunk}: {iso(current)} -> {iso(nxt)}",
        })

        rows = fetch_chunk(granularity, current, nxt)
        for row in rows:
            by_time[row["time"]] = row

        current = nxt
        time.sleep(0.04)

    return [by_time[t] for t in sorted(by_time)]


# ============================================================
# INDICATORS / HTF CAUSAL ALIGNMENT
# ============================================================

def sma(values, length):
    values = np.asarray(values, dtype=float)
    out = np.full(len(values), np.nan)
    if len(values) < length:
        return out
    valid = np.isfinite(values).astype(int)
    sums = np.cumsum(np.where(np.isfinite(values), values, 0.0))
    cnts = np.cumsum(valid)
    sums = np.r_[0.0, sums]
    cnts = np.r_[0, cnts]
    for i in range(length - 1, len(values)):
        total = sums[i + 1] - sums[i + 1 - length]
        count = cnts[i + 1] - cnts[i + 1 - length]
        if count == length:
            out[i] = total / length
    return out


def ema(values, length):
    values = np.asarray(values, dtype=float)
    out = np.full(len(values), np.nan)
    if len(values) < length:
        return out

    seed_i = None
    for i in range(length - 1, len(values)):
        window = values[i - length + 1:i + 1]
        if np.all(np.isfinite(window)):
            seed_i = i
            out[i] = float(np.mean(window))
            break

    if seed_i is None:
        return out

    alpha = 2.0 / (length + 1.0)
    for i in range(seed_i + 1, len(values)):
        if np.isfinite(values[i]) and np.isfinite(out[i - 1]):
            out[i] = alpha * values[i] + (1.0 - alpha) * out[i - 1]
    return out


def atr(candles, length=14):
    n = len(candles)
    out = np.full(n, np.nan)
    if n == 0:
        return out

    tr = np.full(n, np.nan)
    for i, candle in enumerate(candles):
        h = candle["high"]
        l = candle["low"]
        if i == 0:
            tr[i] = h - l
        else:
            pc = candles[i - 1]["close"]
            tr[i] = max(h - l, abs(h - pc), abs(l - pc))

    if n < length:
        return out

    out[length - 1] = float(np.mean(tr[:length]))
    for i in range(length, n):
        out[i] = ((out[i - 1] * (length - 1)) + tr[i]) / length
    return out


def prev_extreme(values, lookback, mode):
    values = np.asarray(values, dtype=float)
    out = np.full(len(values), np.nan)
    from collections import deque
    dq = deque()

    for i in range(len(values)):
        while dq and dq[0] < i - lookback:
            dq.popleft()
        if i > 0:
            j = i - 1
            if mode == "max":
                while dq and values[dq[-1]] <= values[j]:
                    dq.pop()
            else:
                while dq and values[dq[-1]] >= values[j]:
                    dq.pop()
            dq.append(j)
        if i >= lookback and dq:
            out[i] = values[dq[0]]
    return out


def infer_completion_times(candles):
    """Next ACTUAL candle open = first time the prior HTF bar is safely usable."""
    complete_at = [None] * len(candles)
    for i in range(len(candles) - 1):
        complete_at[i] = candles[i + 1]["time"]
    return complete_at


def htf_state(candles):
    closes = np.array([x["close"] for x in candles], dtype=float)
    atr14 = atr(candles, 14)
    atr_mean50 = sma(atr14, 50)

    e50 = ema(closes, 50)
    e100 = ema(closes, 100)
    e200 = ema(closes, 200)
    complete_at = infer_completion_times(candles)

    rows = []
    for i, candle in enumerate(candles):
        ratio = None
        if (
            np.isfinite(atr14[i])
            and np.isfinite(atr_mean50[i])
            and atr_mean50[i] > 0
        ):
            ratio = float(atr14[i] / atr_mean50[i])

        rows.append({
            "time": candle["time"],
            "complete_at": complete_at[i],
            "close": float(candle["close"]),
            "ema50": float(e50[i]) if np.isfinite(e50[i]) else None,
            "ema100": float(e100[i]) if np.isfinite(e100[i]) else None,
            "ema200": float(e200[i]) if np.isfinite(e200[i]) else None,
            "atr_ratio50": ratio,
        })
    return rows


def align_htf(m15_times, state):
    rows = [row for row in state if row["complete_at"] is not None]
    completion_times = [row["complete_at"] for row in rows]
    keys = ["close", "ema50", "ema100", "ema200", "atr_ratio50"]
    out = {key: np.full(len(m15_times), np.nan) for key in keys}

    for i, signal_time in enumerate(m15_times):
        p = bisect.bisect_right(completion_times, signal_time) - 1
        if p < 0:
            continue
        row = rows[p]
        for key in keys:
            if row[key] is not None:
                out[key][i] = row[key]
    return out


# ============================================================
# M15 FEATURE CACHE — FRESH LONG ORIENTED
# ============================================================

def features(candles, h1, h4, daily):
    n = len(candles)
    times = [x["time"] for x in candles]
    o = np.fromiter((x["open"] for x in candles), float, count=n)
    h = np.fromiter((x["high"] for x in candles), float, count=n)
    l = np.fromiter((x["low"] for x in candles), float, count=n)
    cl = np.fromiter((x["close"] for x in candles), float, count=n)
    a = atr(candles, 14)
    am20 = sma(a, 20)
    bullish = cl > o
    exact_bull = np.zeros(n, dtype=bool)
    exact_bull[1:] = ((cl[:-1] < o[:-1]) & (cl[1:] > o[1:])
                      & (o[1:] <= cl[:-1]) & (cl[1:] >= o[:-1]))
    bull_body = cl - o
    prior_body = np.r_[np.nan, np.abs(cl[:-1] - o[:-1])]
    br = np.full(n, np.nan)
    ok_body = prior_body > 0
    br[ok_body] = bull_body[ok_body] / prior_body[ok_body]
    valid_atr = np.isfinite(a) & (a > 0)
    body_atr = np.full(n, np.nan)
    body_atr[valid_atr] = bull_body[valid_atr] / a[valid_atr]
    candle_range = h - l
    range_atr = np.full(n, np.nan)
    range_atr[valid_atr] = candle_range[valid_atr] / a[valid_atr]
    close_loc = np.full(n, np.nan)
    valid_range = candle_range > 0
    close_loc[valid_range] = (cl[valid_range] - l[valid_range]) / candle_range[valid_range]
    lower_wick = np.minimum(o, cl) - l
    lower_wick_body = np.full(n, np.nan)
    lower_wick_body[bull_body > 0] = lower_wick[bull_body > 0] / bull_body[bull_body > 0]
    compression = np.full(n, np.nan)
    previous_atr = np.r_[np.nan, a[:-1]]
    previous_atr_mean20 = np.r_[np.nan, am20[:-1]]
    ok_comp = (np.isfinite(previous_atr) & np.isfinite(previous_atr_mean20)
               & (previous_atr_mean20 > 0))
    compression[ok_comp] = previous_atr[ok_comp] / previous_atr_mean20[ok_comp]
    lookbacks = [10, 20, 40, 60, 80, 100, 120, 165, 200]
    prev_low = {lb: prev_extreme(l, lb, "min") for lb in lookbacks}
    prev_high = {lb: prev_extreme(h, lb, "max") for lb in lookbacks}
    dist_low = {}
    for lb in [40, 60, 80, 100, 120, 165, 200]:
        x = np.full(n, np.nan)
        ok = valid_atr & np.isfinite(prev_low[lb])
        x[ok] = np.abs(l[ok] - prev_low[lb][ok]) / a[ok]
        dist_low[lb] = x
    # 4h M15 momentum: strictly prior completed candles i-1 and i-17.
    mom4 = np.full(n, np.nan)
    mom4[17:] = np.divide(cl[16:-1] - cl[:-17], a[17:],
                          out=np.full(n-17, np.nan), where=valid_atr[17:])
    ny_hour = np.zeros(n, dtype=np.int16)
    ny_weekday = np.zeros(n, dtype=np.int16)
    london_hour = np.zeros(n, dtype=np.int16)
    tokyo_hour = np.zeros(n, dtype=np.int16)
    sydney_hour = np.zeros(n, dtype=np.int16)
    sydney = ZoneInfo("Australia/Sydney")
    for i, t in enumerate(times):
        z = t.astimezone(NY)
        ny_hour[i], ny_weekday[i] = z.hour, z.weekday()
        london_hour[i] = t.astimezone(LONDON).hour
        tokyo_hour[i] = t.astimezone(TOKYO).hour
        sydney_hour[i] = t.astimezone(sydney).hour
    return {
        "n": n, "times": times, "open": o, "high": h, "low": l,
        "close": cl, "atr": a, "valid_atr": valid_atr,
        "bullish": bullish, "exact_bull": exact_bull, "bull_br": br,
        "body_atr": body_atr, "range_atr": range_atr,
        "close_loc": close_loc, "lower_wick_body": lower_wick_body,
        "compression": compression, "prev_low": prev_low,
        "prev_high": prev_high, "structure_dist_low": dist_low,
        "mom4": mom4, "ny_hour": ny_hour, "ny_weekday": ny_weekday,
        "london_hour": london_hour, "tokyo_hour": tokyo_hour,
        "sydney_hour": sydney_hour,
        "h1_close": h1["close"], "h1_ema50": h1["ema50"],
        "h1_ema100": h1["ema100"], "h1_ema200": h1["ema200"],
        "h1_atr": h1["atr_ratio50"],
        "h4_close": h4["close"], "h4_ema100": h4["ema100"],
        "h4_ema200": h4["ema200"], "h4_atr": h4["atr_ratio50"],
        "d_close": daily["close"], "d_ema50": daily["ema50"],
        "d_ema200": daily["ema200"], "d_atr": daily["atr_ratio50"],
    }


# ============================================================
# CANDIDATE CONFIGURATION
# ============================================================

def cfg(config_id, family, rr=3.5, **kwargs):
    row = {
        "config_id": config_id, "family": family, "rr": rr,
        "context": "NONE", "br_min": None,
        "body_atr_min": None, "range_atr_min": None,
        "close_loc_min": None, "lower_wick_body_min": None,
        "structure_lb": None, "structure_dist_atr_max": None,
        "sweep_lb": None, "breakout_lb": None,
        "compression_max": None, "mom4_min": None,
        "excluded_weekdays": set(), "excluded_ny_hours": set(),
    }
    row.update(kwargs)
    return row


def stage1_configs():
    # Predeclared broad geometries; none are transplanted from AUD/USD H1.
    out = []
    engulf = [
        (1.00, 0.50, 60, 0.10), (1.00, 0.75, 100, 0.10),
        (1.20, 0.50, 100, 0.15), (1.20, 0.75, 120, 0.10),
        (1.20, 1.00, 165, 0.10), (1.35, 0.50, 120, 0.20),
        (1.35, 0.75, 165, 0.10), (1.35, 1.00, 165, 0.15),
        (1.50, 0.75, 100, 0.10), (1.50, 1.00, 165, 0.10),
    ]
    for k,(br,body,lb,dist) in enumerate(engulf):
        out.append(cfg(f"S1_ENG_{k}", "BULL_ENGULF_STRUCTURE",
            br_min=br, body_atr_min=body, structure_lb=lb,
            structure_dist_atr_max=dist))
    sweep = [
        (20, 0.75, 0.15, -0.50), (20, 1.00, 0.25, -1.00),
        (40, 0.75, 0.25, -0.50), (40, 1.00, 0.25, -1.00),
        (40, 1.25, 0.25, -1.25), (60, 0.75, 0.25, -0.75),
        (60, 1.00, 0.35, -1.00), (60, 1.25, 0.25, -1.50),
        (100, 1.00, 0.35, -1.25), (100, 1.25, 0.35, -1.50),
    ]
    for k,(lb,body,wick,mom) in enumerate(sweep):
        out.append(cfg(f"S1_SWEEP_{k}", "LOW_SWEEP_DISPLACEMENT",
            sweep_lb=lb, body_atr_min=body,
            lower_wick_body_min=wick, mom4_min=mom))
    failed = [
        (20, 0.50, 0.60), (20, 0.75, 0.70),
        (40, 0.50, 0.60), (40, 0.75, 0.70),
        (40, 1.00, 0.75), (60, 0.50, 0.65),
        (60, 0.75, 0.70), (60, 1.00, 0.75),
        (100, 0.75, 0.70), (100, 1.00, 0.80),
    ]
    for k,(lb,body,close_min) in enumerate(failed):
        out.append(cfg(f"S1_FAIL_{k}", "FAILED_BREAKDOWN_RECLAIM",
            sweep_lb=lb, body_atr_min=body, close_loc_min=close_min))
    outside = [
        (0.50, 0.60, 40, 0.20), (0.75, 0.65, 40, 0.15),
        (0.75, 0.70, 60, 0.20), (1.00, 0.65, 60, 0.15),
        (1.00, 0.75, 80, 0.20), (1.25, 0.70, 80, 0.15),
        (1.25, 0.75, 100, 0.20), (1.50, 0.75, 100, 0.15),
    ]
    for k,(body,close_min,lb,dist) in enumerate(outside):
        out.append(cfg(f"S1_OUT_{k}", "OUTSIDE_REVERSAL",
            body_atr_min=body, close_loc_min=close_min,
            structure_lb=lb, structure_dist_atr_max=dist))
    compression = [
        (0.60, 0.75, 1.20, 10), (0.65, 0.75, 1.30, 10),
        (0.65, 1.00, 1.40, 10), (0.70, 0.75, 1.30, 10),
        (0.70, 1.00, 1.40, 10), (0.70, 1.25, 1.50, 10),
        (0.75, 0.75, 1.30, 10), (0.75, 1.00, 1.40, 10),
        (0.75, 1.25, 1.50, 20), (0.80, 1.00, 1.50, 20),
    ]
    for k,(comp,body,ran,lb) in enumerate(compression):
        out.append(cfg(f"S1_COMP_{k}", "COMPRESSION_BREAKOUT",
            compression_max=comp, body_atr_min=body,
            range_atr_min=ran, breakout_lb=lb))
    pullback = [
        (20, 0.50, -0.75, 0.65), (20, 0.75, -1.00, 0.70),
        (40, 0.50, -1.00, 0.65), (40, 0.75, -1.25, 0.70),
        (40, 1.00, -1.50, 0.75), (60, 0.75, -1.25, 0.70),
        (60, 1.00, -1.50, 0.75), (100, 1.00, -1.50, 0.75),
    ]
    for k,(lb,body,mom,close_min) in enumerate(pullback):
        out.append(cfg(f"S1_PULLBACK_{k}", "PULLBACK_REJECTION",
            sweep_lb=lb, body_atr_min=body,
            mom4_min=mom, close_loc_min=close_min))
    return out


CONTEXTS = [
    "NONE", "H1_CLOSE_GT_EMA100", "H1_CLOSE_GT_EMA200",
    "H1_EMA50_GT_EMA200", "H4_CLOSE_GT_EMA100", "H4_CLOSE_GT_EMA200",
    "D_CLOSE_GT_EMA200", "D_EMA50_GT_EMA200",
    "H1_ATR_GE_080", "H4_ATR_GE_080", "D_ATR_GE_080",
    *[f"NY_BLOCK_{n:02d}-{n+3:02d}" for n in range(0,24,4)],
    *[f"LDN_BLOCK_{n:02d}-{n+3:02d}" for n in range(0,24,4)],
    *[f"TOKYO_BLOCK_{n:02d}-{n+3:02d}" for n in range(0,24,4)],
    *[f"SYDNEY_BLOCK_{n:02d}-{n+3:02d}" for n in range(0,24,4)],
    *[f"EXCLUDE_WEEKDAY_{day}" for day in range(5)],
]


# ============================================================
# SIGNAL EVALUATION
# ============================================================

def context_mask(mask, config, f):
    ctx = config.get("context", "NONE")
    trend = {
        "H1_CLOSE_GT_EMA100": ("h1_close", "h1_ema100"),
        "H1_CLOSE_GT_EMA200": ("h1_close", "h1_ema200"),
        "H1_EMA50_GT_EMA200": ("h1_ema50", "h1_ema200"),
        "H4_CLOSE_GT_EMA100": ("h4_close", "h4_ema100"),
        "H4_CLOSE_GT_EMA200": ("h4_close", "h4_ema200"),
        "D_CLOSE_GT_EMA200": ("d_close", "d_ema200"),
        "D_EMA50_GT_EMA200": ("d_ema50", "d_ema200"),
    }
    if ctx in trend:
        a,b = trend[ctx]
        mask &= f[a] > f[b]
    elif ctx in ("H1_ATR_GE_080", "H4_ATR_GE_080", "D_ATR_GE_080"):
        mask &= f[ctx.split("_")[0].lower() + "_atr"] >= 0.80
    elif "_BLOCK_" in ctx:
        tz, hours = ctx.split("_BLOCK_")
        a,b = map(int,hours.split("-"))
        key = {"NY":"ny_hour", "LDN":"london_hour",
               "TOKYO":"tokyo_hour", "SYDNEY":"sydney_hour"}[tz]
        mask &= (f[key] >= a) & (f[key] <= b)
    elif ctx.startswith("EXCLUDE_WEEKDAY_"):
        mask &= f["ny_weekday"] != int(ctx.split("_")[-1])
    elif ctx != "NONE":
        raise ValueError(f"Unknown context: {ctx}")
    for day in config.get("excluded_weekdays", set()):
        mask &= f["ny_weekday"] != day
    for hour in config.get("excluded_ny_hours", set()):
        mask &= f["ny_hour"] != hour
    return mask


def indices(config, f):
    mask = f["valid_atr"].copy() & f["bullish"]
    family = config["family"]
    if family == "BULL_ENGULF_STRUCTURE":
        mask &= f["exact_bull"]
        mask &= f["bull_br"] >= config["br_min"]
        mask &= f["body_atr"] >= config["body_atr_min"]
        mask &= (f["structure_dist_low"][config["structure_lb"]]
                 <= config["structure_dist_atr_max"])
    elif family == "LOW_SWEEP_DISPLACEMENT":
        lb = config["sweep_lb"]
        mask &= f["low"] < f["prev_low"][lb]
        prev_high = np.r_[np.nan,f["high"][:-1]]
        mask &= f["close"] > prev_high
        mask &= f["body_atr"] >= config["body_atr_min"]
        mask &= f["lower_wick_body"] >= config["lower_wick_body_min"]
        mask &= f["mom4"] <= config["mom4_min"]
    elif family == "FAILED_BREAKDOWN_RECLAIM":
        prev_low = f["prev_low"][config["sweep_lb"]]
        mask &= f["low"] < prev_low
        mask &= f["close"] > prev_low
        mask &= f["body_atr"] >= config["body_atr_min"]
        mask &= f["close_loc"] >= config["close_loc_min"]
    elif family == "OUTSIDE_REVERSAL":
        prev_high = np.r_[np.nan,f["high"][:-1]]
        prev_low = np.r_[np.nan,f["low"][:-1]]
        mask &= f["high"] > prev_high
        mask &= f["low"] < prev_low
        mask &= f["body_atr"] >= config["body_atr_min"]
        mask &= f["close_loc"] >= config["close_loc_min"]
        mask &= (f["structure_dist_low"][config["structure_lb"]]
                 <= config["structure_dist_atr_max"])
    elif family == "COMPRESSION_BREAKOUT":
        mask &= f["compression"] <= config["compression_max"]
        mask &= f["body_atr"] >= config["body_atr_min"]
        mask &= f["range_atr"] >= config["range_atr_min"]
        mask &= f["close"] > f["prev_high"][config["breakout_lb"]]
    elif family == "PULLBACK_REJECTION":
        prev_low = f["prev_low"][config["sweep_lb"]]
        mask &= f["low"] < prev_low
        mask &= f["close"] > f["prev_low"][10]
        mask &= f["body_atr"] >= config["body_atr_min"]
        mask &= f["mom4"] <= config["mom4_min"]
        mask &= f["close_loc"] >= config["close_loc_min"]
    else:
        raise ValueError(f"Unknown family: {family}")
    mask = context_mask(mask, config, f)
    mask[:200] = False
    return np.flatnonzero(mask).tolist()


# ============================================================
# LONG BACKTEST — full-ledger p0, time-window metrics never reset p0
# ============================================================
OUTCOME_CACHE = {}
BACKTEST_CACHE = {}


def outcome(candles, signal_index, rr, cost_pips):
    key = (signal_index, round(rr,4), round(cost_pips,4))
    if len(OUTCOME_CACHE) >= 250_000:
        OUTCOME_CACHE.clear()  # bounded, affects only speed, never outcomes
    if key in OUTCOME_CACHE:
        x = OUTCOME_CACHE[key]
        return None if x is None else x
    candle = candles[signal_index]
    ref = candle["close"]
    stop = candle["low"] - STOP_TICKS*TICK
    ref_risk = ref - stop
    if ref_risk <= 0:
        OUTCOME_CACHE[key] = None
        return None
    target = ref + rr*ref_risk
    fill = ref + cost_pips*PIP
    actual_risk = fill - stop
    if actual_risk <= 0:
        OUTCOME_CACHE[key] = None
        return None
    for j in range(signal_index+1,len(candles)):
        bar = candles[j]
        stop_hit = bar["low"] <= stop
        target_hit = bar["high"] >= target
        if not (stop_hit or target_hit):
            continue
        if stop_hit and target_hit:
            # If high side closer to bar open, assume TARGET first.
            # Equal distances resolve to STOP (conservative).
            if abs(bar["high"]-bar["open"]) < abs(bar["open"]-bar["low"]):
                price, reason = target, "TARGET"
            else:
                price, reason = stop, "STOP"
        elif target_hit:
            price, reason = target, "TARGET"
        else:
            price, reason = stop, "STOP"
        x = {
            "signal_index": signal_index, "exit_index":j,
            "entry_time":candle["time"], "exit_time":bar["time"],
            "entry_time_utc":iso(candle["time"]), "exit_time_utc":iso(bar["time"]),
            "reference_entry":ref, "historical_fill":fill,
            "stop":stop, "target":target, "result_r":(price-fill)/actual_risk,
            "exit_reason":reason, "rr":rr,"cost_pips":cost_pips,
        }
        OUTCOME_CACHE[key] = x
        return x
    OUTCOME_CACHE[key] = None
    return None


def backtest(candles, candidate_indices, rr, cost_pips, start=None, end=None):
    # Full exact chronological strategy p0 FIRST, then slice by signal open.
    # This avoids fabricated extra trades at each historical window boundary.
    key = (tuple(candidate_indices), round(rr,4), round(cost_pips,4))
    trades = BACKTEST_CACHE.get(key)
    if trades is None:
        trades = []
        p = 0
        while p < len(candidate_indices):
            trade = outcome(candles,candidate_indices[p],rr,cost_pips)
            if trade is None:
                p += 1
                continue
            trades.append(trade)
            # Half-open holding [signal_index, exit_index): same exit candle eligible.
            p = bisect.bisect_left(candidate_indices,trade["exit_index"],lo=p+1)
        BACKTEST_CACHE[key] = trades
    if start is None and end is None:
        return trades
    return [t for t in trades
            if (start is None or start <= t["entry_time"])
            and (end is None or t["entry_time"] < end)]


def stats(trades):
    results = [float(x["result_r"]) for x in trades]
    wins = [r for r in results if r>0]
    losses = [r for r in results if r<0]
    gross = sum(wins)
    loss = -sum(losses)
    equity=peak=drawdown=0.0
    streak=longest=0
    for r in results:
        equity += r
        peak=max(peak,equity)
        drawdown=min(drawdown,equity-peak)
        if r<0:
            streak+=1; longest=max(longest,streak)
        else:
            streak=0
    return {
        "trades":len(results), "winners":len(wins), "losers":len(losses),
        "win_rate":100*len(wins)/len(results) if results else 0.0,
        "profit_factor":gross/loss if loss>0 else (999.0 if gross>0 else 0.0),
        "total_r":sum(results), "expectancy_r":sum(results)/len(results) if results else 0.0,
        "max_drawdown_r":drawdown, "longest_loss_streak":longest,
    }





# =====================================================================
# STAGE 3 — FROZEN LB40 SWEEP / PENETRATION BOUNDARY RESOLUTION
# =====================================================================
# The helpers above preserve the Stage 2 candle/ATR/p0/execution semantics.
# There is NO RR / entry / lookback / body / momentum / session tuning here.
# This is another examination of ALREADY SEARCHED historical data, not OOS.

import gc
import traceback

RR_FIXED = 3.5
PRIMARY_COST = 1.0
STRESS_COST = 2.0

# Frozen Stage 2 controls at the timestamp recorded in its coverage.csv.
# Parity is computed on an explicitly TRUNCATED candle history, not by
# filtering completed trades from the growing present (which would leave
# earlier unresolved positions and change p0 sequencing).
PARITY_LAST = datetime(2026, 9, 18, 20, 45, tzinfo=timezone.utc)
PARITY_NOW = datetime(2026, 9, 19, 11, 55, tzinfo=timezone.utc)
REFERENCE = {
    "candle_count":546849,"trades":55,"winners":21,
    "full_pf":1.955626,"full_r":32.491274,
    "since2018_trades":13,"since2018_pf":1.398884,
    "since2018_r":3.589957,"last5_r":2.339957,
    "last2_r":1.146079,"cost2_pf":1.781976,
    "cost2_r":26.587184,
}

STAGE2_LAST = datetime(2026, 9, 22, 21, 30, tzinfo=timezone.utc)
STAGE2_CANDLES = 547044
STAGE2_BASE = {"trades":283,"winners":83,"total_r":65.27471807923523,
               "profit_factor":1.3263735903961762,"max_drawdown_r":-19.194501889752548,
               "cost2_r":43.84178398836982}
STAGE2_NO_DEPTH = {'trades': 442, 'winners': 122, 'total_r': 69.27517553744045, 'profit_factor': 1.2164849235545012, 'max_drawdown_r': -32.92887688975253, 'cost2_r': 37.22065100718777}

# No adaptive thresholds. 0.10 was the UPPER edge of Stage 2. Test both
# sides at finer spacing and extend beyond it to 0.30 ATR; never rank by PF.
PENETRATION_GRID = (0.0, .025, .05, .075, .10, .125, .15, .175, .20, .25, .30)
FROZEN_P = .10
FROZEN_LB = 40
FROZEN_BODY = 1.00
FROZEN_DECLINE = 1.00
CONTEXTS = (
    ("H1_CLOSE_ABOVE_EMA100", "h1_close", "h1_ema100", ">"),
    ("H1_EMA50_ABOVE_EMA200", "h1_ema50", "h1_ema200", ">"),
    ("H4_CLOSE_ABOVE_EMA100", "h4_close", "h4_ema100", ">"),
    ("H4_ATR_RATIO_GE_080", "h4_atr", .80, ">="),
)

OUTS = {
 "coverage":"audusd_m15_long_s3_coverage.csv",
 "parity":"audusd_m15_long_s3_frozen_parity.csv",
 "penetrations":"audusd_m15_long_s3_all_penetrations.csv",
 "costs":"audusd_m15_long_s3_cost_periods.csv",
 "rolling":"audusd_m15_long_s3_rolling_windows.csv",
 "rolling_summary":"audusd_m15_long_s3_rolling_summary.csv",
 "calendar":"audusd_m15_long_s3_calendar_years.csv",
 "penetration_buckets":"audusd_m15_long_s3_penetration_buckets.csv",
 "incremental":"audusd_m15_long_s3_incremental_accepted_trades.csv",
 "control_trades":"audusd_m15_long_s3_frozen_core_trade_ledger.csv",
 "context":"audusd_m15_long_s3_separate_context_tests.csv",
 "context_attribution":"audusd_m15_long_s3_context_attribution.csv",
 "context_trades":"audusd_m15_long_s3_context_trade_ledgers.csv",
 "decision":"audusd_m15_long_s3_predeclared_gate.csv",
 "methods":"audusd_m15_long_s3_methodology.csv",
 "error":"audusd_m15_long_s3_error.csv",
}
BUNDLE="AUDUSD_M15_LONG_PORTFOLIO27_PENETRATION_BOUNDARY_RESULTS.zip"
STATUS.clear()
STATUS.update(state="not_started", progress=0,message="Waiting to fetch",orders_supported=False,trading_enabled=False)


def sweep_mask(f,penetration):
    prior=f['prev_low'][FROZEN_LB]
    mask=(f['valid_atr'] & f['bullish'] &
          (f['low'] < prior) &
          (f['close'] > np.r_[np.nan,f['high'][:-1]]) &
          (f['body_atr'] >= FROZEN_BODY) &
          (f['mom4'] <= -FROZEN_DECLINE))
    if penetration > 0:
        mask &= ((prior-f['low'])/f['atr'] >= penetration)
    mask[:200]=False
    return mask


def ixs(f,penetration):
    return np.flatnonzero(sweep_mask(f,penetration)).tolist()


def compute(f,candles,depth,cost=1.0):
    ix=ixs(f,depth)
    return ix, backtest(candles,ix,RR_FIXED,cost)


def row_for(label,cost,ix,tx):
    row=reported_row(label,'SWEEP','PENETRATION_ATR',label,ix,tx)
    row['assumed_adverse_fill_pips']=cost
    return row


def cost_eras(label,cost,ix,tx):
    r=row_for(label,cost,ix,tx)
    return {"config_id":label, "cost_pips":cost,
      "trades":r['full_trades'],"pf":r['full_profit_factor'],
      "total_r":r['full_total_r'],"dd_r":r['full_max_drawdown_r'],
      **{k:r[k] for k in r if k.startswith(("pre2010_","post2010_", "since2018_","last5_","last2_","last1_","era2002_", "era2008_", "era2014_", "era2020_"))}}


import base64
import zlib
import json
ARCHIVED_STAGE2_P010_LEDGER_B85 = 'c-qB%U9V*~j@^Ip^Hf13MTzoT9)dh3Na`s;U^F_OiGhy0VS9iX4CcQNrM=I&TOSl}qt4l}8#U9~?CyJ)DT<$q|Lfm=`2ClE`t?tL`RSKm|Ig3=^23im0HpB4Km73Xzx?vY^$hCSuYdgZuRs0q%m4fNr+@zA&w7p_@RtBzLi!f?$H+g1@IN2_Swk$T%qjNU&;R)IZ$JO~=bwLS|HUd8zUCBBKmFfde*fdIzy0!O{ki_}m%sew@rm@4e)!w@@2UP!fBfmUfBN~4^F%@{AAhy~jo<(IFMs^>n|}WSU!T9&Z$JO(_rLzChyUZ-fBkP&tP0Rq*8lg<fBpTBpZ@mC-+r&ZxBe&o{U2tCQ1S#3mbir|e*^lFf(Ma`@sGfJh>8gjcL;Hj0U^Q6;rSQ53!z9QjR?gEY$JqU5WfZR5IV$?gOFsg5YkVjSCJ36B&7ka0#{+_;i9j=Br%<TMwqbs_MiUyfBDbf{(kfl@`PTBVT)e;2En_6hgkdo*+4IE&>IjN0i$%ef%#qfl|rTwAwApZ7kz<TjTGz=dWglXeigC+D$OQ+0924hgg9PqAifPD3X_dW#JEEU;TzzGeYOx%F(mH>CR>M6wr^mw{sUamyn(NSOLeu3aGgJ$0T+J(wM5x7SWQe(-Cwy3V8N9E`&X}|&|B~;T>2>9@&bmKXD>k?-?#;=e8Np=VgSw!<kB9KC1PA~cm{=d^c0}n@eTsgY|8bJ#KtLof%px<!%4-sKA4U>MalFPD4F*#VHp8S;SR$NkzlgydPtj_n7;shQy83^c!*^OFlN*LWbXY>j-zd_igt%qrP7Q-50V~kZl8Ps`3A|e?hmoOxzZ=h1SztqRZOFG=MwI)?ut!@1z<aeL7KTr-ZebLvS+TqblJH#?;OUP#~i~QN~JH$eDn0M>@cFx`I8n#Rm_Y0qV0CZ!R?G`wC4xF?*T>tC*MF1n`JeCDJ|A@ZTKNZ&TUKCbPiG;oP#tu1R=c*m=bxuReC5pV34ZDIA-V&^W1P2`&JE>C&%$U(`fVtH(cS6G-Gr;9X3Y%4TG;dn5v`Xyl3X6;@pQSoM!9%Hj9iw=1ZlAV&|D4)CU}r6T(%o*hPBZZ*3-Eu-M8&UlHUZ%R=Q<X#oVxlQXG@5_Xtj=<+2NW>qXZhUjqb*O--PKKrp{%X(`qwlj%e3w`{a-w#?job4X8lH(3EeXr2V)>Se17Q_QhOb`43<^oinf!d_&=xexs4E#FK5|#OOrij@9Ew$Vwdbjf-rr7Q+JE52!Ch6ttU?YUl9U!JVd?u8_vvSGlY_4&_mgC(!;!m;SSur%Dm56<`62q9QQ*|phINuu3;q(ORA#FgSX6kYK1U|*0^8^w`TbBk^f-zxE*KFPU`~*VcIDv%GOcb==9`iT+1V|`VllC_*B-6>%Po`}TadfNzuOZ#roKK2^Q@Emsgf?I`1O6@S!<DC4@|@4K34#+387Sr{Vky@^`F%K*G@B?rOdG|@`nGvt-BZj<sQg{;U)-cN8OOZwn@U$w=IV<c78@{)<D`ar&f9m0p<~loY?occ3v1M(eLzT4!77Wo{Sn)rxxCC_h!2J8Kr3kj8o(DUo|o0csJW(2y@AY!P^f<h1xL^%O^(&8fMGSAlQuCP+PwlMHy0^Z`?CY)DJIVKQ)M>Il$4=fEO~S{`%;YDkpgC7oYD$<n9~lj8Zwg)Sq+)J+Euo5=6RDd&p4X+oZA!eCX`ab8Kso-7Nznnl&zV1h$%{Y#mZ!Z1OqT*{G9o1-X=$$-#d<Jcj466E?u^1jSIGlU+_wK$+SdjoqJ$-ykZh=X<%V>J?1OM$E7wvqUIH4Pk9b8ahxZiWLu>p`o9|N$$63-@9+g!fTlQD590=>s2EfyGwe;#A?D=71?)<Te(Rk~{jtJ;lVG@|9IWmxOwo%T#?7V6wbKHFXR>oGNCn4aOGeXa(r=}6GbO{AXOWs?H*r$839nTud70ak5{1c#r%jti1Eji5=-tEIYA#`a5A{6{rhdhv#QuR>Oha%ioXQoMx2ad+xr&}riekeg`i^z?YYmgMtQC4N<p?HBhZVoT6giGKsUyI48c)O4%ilS_OuaIODf5yt3G8qKH(`8;QRi^2aYHls({C;4{lJZmH>tyoK=}P>yp&6re1;?;@eU`1>PwNQU)XvRoPM%mdYq*<UbP=;07=8SvawxMSoKRERSc!3;$Eg)8WGPucm-+mHfjLOUiGhuRUHJ!<|CSS5)u2CQKpxX(zO%zZK{<6&ZbKbDeX|gR=^?;C2aBE9;Ip~Etpbj`GXIf^vN#c%NPjW7Q|%L%E=GaLoOyd0M!debabeSkz3Icz4)eq%KH8pv7Tf`G6QANNAc1HJ0%xLIF<xLYMnotoxb-z)>^=fWkjMamd%$S4@(SPZ}t*P!DN-=^;_-%lrWmH5^}iXLe{`}f{B@JUjne1EibzOI5zpOFXT~=Twll%Ww?-K=rI$u_ZMGc%J5Mpq!RU)gz~y7s>VS)p{9pswn196xf@kK#L#JDD8yu0d;JzW53EoaJ&D4qT{qOa)T^2Epw$hEHcnA5nAd3b5c3>*ec>~kIDznh)7WlZ+xIUSh_~hVBx_VZ;gm%-`jp19%o?ZE0_)=F6RU}~aMEC-^GsEFV0acw3P-Tr-#rL`iPinc)oqQhx_w2Rh)JvT&}<@-FD(S@niJM&H8F5X5`~l4Xr(fs#gs<05a5O)Y6)>3%&Ld7!)fp}Eu6rA0u_spZJZ$YIK>!EA42_uoHss%+IW#jdWDl(W`3DhUkG7_kZgZd9xbrosoJBV*6~*}ny+J~{&A!@=5Zt(xx)9TSA;nE^7XLT*p!-vxK<q2*c@Wf!G_V!)-Vp)8V+dJa)gC$Y3J8SM`RupQT1gLG<BqEnMSrQ{!`3pY)ArT3N_Un>J>DN`Nu99wR}?mL|@gnD(al&T$_XGb#+t-k%bq7HOZWvEUjQ8UMjUb%0EuGOwB%80i%9PUBIaB>EOnvEP?LmRu3HK{*QWU<{wI{-~=zSeu@<jsA4NtDuZI_UpyjJ@Rnhbps(-Tf~=>^JESmp)t-l#*8-9#n>h&m*5@E<@_R@#r^qF2x**l(4f*j0fBdfBe;IQM3mrRW)D7gQRS9M5rar{LX&z0H%v!X5i=E?@B231MA;P8|gkbCzc@c23wEA*Rg<HT4`78^#W7K(H!w+OPkOmqWNcA-lyX^rY#v4K+fa)aA%nNu<pR7USItAzq?LNiambMsaGuJgtzrjQ<kks;<sfkx26)DUUIS|jx&KjxICcA@&R1@<&v0OmRp3(v(4qY{4onJyKmxlTG&AnleIUfPF5)f>pN-Md#7qL{^kLddI#q?Rzk(R%vyJ$>z)zRo%C_U0qG#HT5(`X`9Gk)YXN<PKdNlBnYOQ-Z3cRE${2a_pv6Yl6#R!k^{1yV7TEmE<%sKG@lyCW6lXv$au1wBmqSdTDa6uf2fJJZ}_(@ygardn0ZK1}FOo`|qnRuoC0uf7UO*;F)A4)OiX$_kIn#2)ojcnezJ;BJB@UAz1mv}}%4=r`)?ijpu*Wem=@*sG#0%YuaHFsTcqAe{Y=dlHlnF=q%|)E6=f8MRBML#m`QrkX?PsCeFn6hWBDi1ZW!*+}8(#cwX+hgk7+szkdoR(Pn4<uuk{i>lNeCX0v{k)nnmJDsA&7_cq#oMOct)v0t#V!nYPNN?QpROUgwy?h5!aZyFArv-LM5jd?iPce@%rT}jqL5`(Iir5Zt3PQf4i%a1t6c?Zs&NgWA9Ic@JMLfiuDFIcFV7pj(@EtVj%d?9`cew48c)?duPkDz_?f5I6$!cO(Pi&3U;a{$iN+bhPV^dPA7~EpqbLNFkovqdeQrt|Ii&PUsa048>oIf-@B#dJRr5w4WH-RGoXCFaNv+Te@aGD)XvFOqd36k9ah%__+f{cUq5)p6VJ|Tg;P$NCvBh@o7nMiS+`f`xk)OHXdTAfHBWpqIaCEby*is4Lz_FPSR8g0;0D?+fRUWb_19-r8xx6p4XS)ir4Q$7cgJD7+fp2RUdXowhW&~i(&qX$|Zds5duZ_zC9=(oPWBN;}LRSm@?Mw=6pQk0K>y^IsHX7^@_zIrMf??GB`P4=AAL(FmGONeIYmVWCyw@QGqG*&b^d!Mb3XWPX95A*IamX>aDFXJJW-K>m|>5)>Q-Xnz^b{;9p9Uf^^BQZ1i#*;PA1g$=h;<)k6h`FR!g)wdVkh0Z)TA7!|#8#HtLT{LT`Wn?R6RE34f?Qr$C5z>pX6->NVH00h!kN^uJX#=DF|R#IGMkzw!_Zxksb*bj!yvy;$<!8v6+XxwKGE?lPBDjlPJd;S0Ykr0ANB}d2}WUp3gM1tlEX2Z+=Hg4*$%L4pE5Zef*)eyDHSBs+Mj;m2WUCVq*M&|AeDq?8^$@Hg%q^B6**1xrx-hh3;nS|FdT~Zt#>FCiN5o@$`riC^<u$!+OD4R1}SRoq<h!vA*T0ra}o8&0ybz0+P8Q~F4u#;Ik%pac8}jgkqL~m9_gvYJ6?b7*h+=jL#K)DdzbacVn(viQSSzqZ(;q0JrS!OB@TE;ds{+GbKIzLzSV_vpn<vLShYYyj|f?REHy>N0<8vz9a6ckhUrP+%sXekX8D;pfCec1!ald!=Cc5fj@QTl?&xAD9&k-;Uzo01lgPAMe1xzU6^j#c3EoIo%PV^AdP+7rsdcT9XVV{Id$mfop-NXil?FuL9bfe<;3ZG`Mw^x5Ozu}tBkXXh+@hbr=@8rFbcVP~prTwEHtWIBPIg6#bBcE)xC9rI=JiyJqJ1sJ!#P8~i*yb#aENHW%ewP)^&(%Y(Q`i_rH{V@zHFQtB!+nks-EsZ#O7^+2jU^N&E3@B9F4Sm`$pLWvGl7=>K%paVuiUikSdXiDhOkVP+kaPmU&LGokknP*qXQYRiLMsXZgu?SLxceTmL=viNbn`DP2m8?+Y8{T!t~>6pM$`J_34*iKA=;M)PU=c5Obb2a{V*G2^E9G^pc|A!#fHgjrsfj}Vc0gr%K|4IDSRi0IwYhu9vt7>cQ35kl`;P>I=yw~r^|k_mgGwn<sk(WC@SThP#He>=s<*ETFRXcPv}P$q3-y38HC52ylh@dnhAd$xf}ErIHquhfdzPHS^ZELe$&a!{vTvN<`9NFkKV0P#DS&vV8FJ$&B1mbrCzi-%Mb+xK$S-xO_?LK;;HW~2nlO&sxyUaoQy@YqQ4Cv+;Gy<Gbz5yAA$5PF;-bDAtYU2uwX5UD^u-b{F13|U7i7qD?Mn{coZ{v@Zl`4r2(p|3m+woe<_bjN7&X}Rl$OY^`Pmw5h5q`VHp9A<t>Jq5@+3^9Aj>8F_I(vs=W9y~>tmomesdFM;}rfa$MRBK{*b1fg6Kb&h>+BCmwz(Byhm!*eCf2<jvD}U0I-HD*OR7cfg%9rr)yV>LG^NjOy2i&=?m5U@ft<N7~;s8fb%=`r%S>TIaL6FI!32$m?p{`;`Tv%UMmKXBwtB&&mIXau1Eab5GZrCa2CG)Kjp!TVM2_nt+QLd@{{J!lWO!H)3J%x>iwXXRq0Bf8MvEt!$q$Sos9V8$Zq(Z*}qanqKtvB<fNa_E~0;#kyV*0P6N02<EYT_q&;dZhgNy=deBNL7x5ET~FU&XKbf_>yEB&FBgMb5BTQNx+?7CGp$h708A+=8+1o@?kZ*~ldtkfSM8p-;kYdu^0R50Z(GJnHuYm*QNJtfyx7x*;;q+gQK>_;of;+4jp0E|ZHh#%Xl?Q%G;ze3Tdc0D1~$!M6S0wY6*CYj}vY4F?W9!A2+K-g1?(2+4e1h%u#gx_(U%dYx9CTL(wj4u&inHt`zg=)sH-JWsVm+o>o#I2AHqM}04&?Nq>ZoAiY>@cWTd4<#*jZSn3tR)@h^E^>-tS9DtYv38LJQ;!=)nZip&?uJbnibz?xt&bf)CQi8$p*j~6Pa0zfrxI+S3iSSdor)Wg!g7mK0nGE3)_RH?r1%rvIS(;#Sk^0jqRF(BpbSh)WgOGf05>f7yGXCN9DhW>__`fr8r{#D1u2v*UKG4Y@(=@`?=0D~pd}5npsMc^UPaaKhE$jayn+j)^ptI+jzu)$-NA?0zFDsB?laq1Wnj<)8Ywr_kxJ=?-qpIoZXVXFr?f@tnA#u@DU2>vSD^sr9i*e~+RX?}l#BsxV2ePcGS7C@)3Wim%E>a~sMH}AJ)nfmSv^&kzO$F|)Y%&~xVa%?j~LbSosrU0+#<zZ3)oXkT$)coo944%k5tTKd#e7U4mS+wA)fQh15zcEty6#}1IZq#L(C(%QK!UQDXrhIpLZ!ZavpSp*frbrHmC>|JP7U7@;+k=^I}JvzJTf}=1l16?`N}DhG7V;7CgsJ-XQQk>s0Bk<~vuj8Oer+&g}8dIz7d_5roqPU&Dj_f@$?n)A-=k(!~w!yNb!8u3t~7Y>_HmM~I1(zr|f0aNCXp40gQogpC70F_s&q(=h@pu3Q8%>xr=ypV?Vieu^b`zfZxO?eXC(!}#zqgwxq^hQyl%%O#P7P}J7voL=Xrw_TqzaD29Xt${l>t%!FSv)?6Ge{st814uGFfMN2}qh5zQ7T?B1JSRuvNf2ouB`4jygVZ5LuJ?(yQx!<8$tl&Hd03s(4eT}z9$tc$Jka*@*cUN}9y}1)phX&xf(facEEv}c+k}*!reGpfM|<3kh=-U<({D3HNDk(%b4fW)g#96xP1>)vyz38EII8o?FPoQDu!v{@ArWsOOy0cgQ_NdEk`t1NVKO>?+Ys^uA(lJlL6Je`F1UJ{H<-aWM7y_YhnVZuvrX%w-+EdXQQ47CB86k6^^lW?qMFHVOY3D??E7`BvcAoDVKM{8crG2;Z%%rOd8B!=S#HxWg^;1J*zR$9U)aMD7Kto9-KkO|9^SL$onp}K@mxYfU`~DNw|?qVP~r%WdXe#s_^7>`-$Ff2u)~MI<C@}+>gpaJII^7U-HJj3H_4Hh=yof<u|~*w)!$H0E$!h)73&k|pwGi!dH9OCp)>b}C<{v(mjz>vw|ELUE~@mL*;)jDKrk#vTqA|8znQ&7xF*r?uo*C)|4_R{rXXdr84y*=f}~PcJU}V0Yi?kDJQkK)BdU#peI|j`#A^1qcw3gUy=6J~JMZ)#@wBqLT=)6BuhFPZXU*zW`D2QW6Mv%J`yu90;^kt^DYkF1ry4FeZ3hfMcNA_?9V4BA;xkWb0aeTM`$kf3IR}3`MouO>i+<|uRhqI`IRoCov8b-mL8Lw4bTS_sIHfb+M;=ZGX{EEE`nXN&n-{THsJhT`wCq55N8Lt!)kTUy-B^GvQrSz!G_hk5ykpsc?OooGutzG6Oi6k@H^bGhL`uRuy052@Hb~`~?{ktd{MaMqaR1~e(L^c=_ejMwTB*$Gmc8eg3Y5j|(^K9dh4=|`euz0dZ+gM9dHc>iaTlZlO-KRI9ohCen72YoEc)sL0t%n7Q~7LJ*y|J?9aKxC6sUfM1C9)onQFv)gF(-AE`I__Pi5mFNV7l5b3oe!g|~Gq2Qe3K>bIVQQe)ojt_u!}^g*hrsY-0k1z)$Y6f-X9Dck8r#g0u``+^RCEY#Xx#*l((wnBp5Y=v>HU7eu5gSZB>#sCYT5(FElN)1)KTtXA8T}BQodJfs7DbsH`4JtM+iq8N+Han_=(8pitbqmn3Iy-ZdjGnUXmDbFYOApJBJeI3s1#A^7h~wqN5xF`ez1i-4-QWZHb(izWYS1Ak=fKqpxzr-R<Z;J7#iFBg2<C2P{RX|x>9al}hZ#8B;HOF~i%?oNwAQH=oP(EWe~3Z!ysuPj?<<GF`{I#=xUTYfU(rh+AexKOdTR1YrN(`P9!JYljJ-@lv=!;J50_lSK?L088W8SS>3~uwF0NdmRBWVp+;hH0s)>1|^6FNJedX$=f_244uNsd!RW11)Z=;m50Ii%H048V$w~q^2O=OV=T8K7ifk!(P?M|$W@siYfV^*u^-?Pc8ryJwm0!GKVe~OVCFxr|HrSHj78x}{`iz)LR>Q%S&F4T*oS+Y;979YBm=4+%5)?x6h7uun<hCM?PqU1$}`Hr0hSn?{PTpm+9YozKLF)so56wBWFx(w#_y1tDq^SIg}C%z^i-UbteAaky!o^HH_>dWVvtJz<@Dkd&zx|(WxH6qKf8j+_RD>)&=8~iE?m40SMs!l`ONKGA~Yows{fOx(;ZyX1XU_OV8xQVef=9C-8ndm`rGU7*}Z1EFkd+AdQoFyWe#Ljh5$5!3rrw-5PPIQs>`+P78mF;o{E!(qlxz!1C=O-*cdx|+7fVu7!HtQI5cc}qdBAjX(Mc@ui1A=*7O>HqS^Ut~Y?$QN;YtZ=Iow$D_W8OqSGDbjg+^|&>TX)nn2+kg!0V!tF`sY^u!mggWMyiQ{=T6YkeY?Jsxk!HzGubTtSyeHZ>V;p09#i>{A8WZ^k(yAr(%c+aPp!4gCpzx3mZz9kLW^W>N78TocBC9}s$NHpm^KTofpRFR#-@cX`tkzc4vw_}xD?DC5eFj0EBZgg+=?38wQQ!r^Oy>c>cqXRABk$DHhUWwlC>$f{?c-~5snR$&$LUbqi&99X2E_$GfUX)&&ZOl+qY)ohgA1(q}Z4<B)sls>V2PAE!I_FVFO|rz1B5|hnTZ!zUFG84I-Mar-UiQRAg>>Ju5u004<m3ftH;&p{=O&tGDO!t2Jma)Fkzdn31A8b~IDn?npCIY8Gtg3?9rG`#XAyc}+`f_n7d|W5NV;l^eh``}nnlEG4S<Ti2ewER#=!IPa>gr{-=mX}&6ZTuDzc&!sI#-`l!{iJ;#rUtqe4ZGp)R`bE6h6kC^~hGZl4m@spYqO)4$^yUO}lZ}4sH`%O`JvHrmLl+ZOpKkVJ_0-zaTp>s`Hg|UZJjKYniEPf|>9;i8#c4rh_4wb{{Y{Z1&RHHk&DqB1n7YrNCgpSHe6LBAW48T#?i(zLWeT|Iy6SYQJ7OJF)5oE1+9>cc>OiU#`E&fOr)K;utxY0+;tDn~@VY6`?)f_2nM>J&#w;R*E2;YAcwwC+d2$rT*P&L*O!C%FZ4+O!h2UkWpJJY44`@e3fI~zCFg1W_Xo7bbF0SiOC#3YW1RE)6vCV#BrHLJsNqeLa&An3je3LI429h@ABEBZA^ZP)HXE%KhT1;i5Uf9@Q*#iwfLtm2v+BxfhL(V$DxJNdJ47)WqkZKbbR1R?=Os@-wSWWBf+t*VoRZ*H#)zg44kh_RE&0$5r?msF0fI<+Sdz`KmvhT}`CCD>N3sst@U?UaJiBAWq*y3+5Gp13o>3cyAz3GQEx?WI#ZWtn_L;|x)>8Z)oUt0M;dTITKnAatcN=z0|sT?GaPoNajG9aaHYshz)trP>!zJ#9A9;wvL8kk76#fxE&lvzL0Pn`>vq>LK`6*_#Q8YNo9(t2vLX_V%gyXD%am=hHyv9(D0rGZIC_g&$-Oqb0W)NuV6UPsVOIpulGMNcKzI(2jkIwSl|41;T?lAzgcqTl*<lXPZpI^<%)I||*RuJG(M=xH|6S{NFk4}S~WrqvHISHX(OrBSaN4)<?d37$&jti1)P)zhb^vPG({1P33fD&}<Kq)J(5-^Dy!#xzbbCXr(b@=f^E<vlY#`rsBDA3W<lw8!TVbJBh(+w~s()<-v~diVoAY9F@Ns?@Z#y143X5z*_cRlDmmaxs;SW>I_EwiMQ<SazFD>TW<2Il#~(7xIvWNSIQ%BeSf~=Y@)Kw(?mOf!JmY$h(5^97WhRFQs6=hWg3k8%f9dj4Nbt-Ho@{=!tctFSNnFP~df;PKgVSQ>w$wVnZmcdsVz!+Qh&sV?eUY7?6fC1{{UeE}RB{8`yId6Pj5w>Zw)6I3eis$0kxfky%Evn*fl8CIB4PJk+C3BlA?qV5m!`+8-gku6|wjZNLIJvC4}FA_wuLW6qiwlB;6&G)G+q+^g8J9jF5?K;AO)ACEx&^Qic~2sp`kPDj^M`vtAFLoW?+729hZke&4c8nRw6#nuNy@tdw=fkh-#>X`Em93ZFi{~_iTt<^Z$nTI1Q(HX8ut5o&g=6=Nh#~#QIpL%Ldg4SRB*wetC@<5wdPhLv?pu}C9tBixW;xdhb0^<!F2(j)_$}>_iiS-0RBdcL2rv4C1&NVxS6pi{}?0ZP6+dkG0ne#mV`PkAK+Bo~m+|wBHVho|4S}o%h2yA24dwc#6bGch2*=9)t9~oF~Vw|`T0`3~QQiLiKW6mkO5IKM_x4_j?(`L1W@|ACdws-O==5;Hj%~(R3#u5N{?1HXIp7(q95gtUt15#}I3=M)|TFOGIrc1rvRQ1Ts+C{o<1R1-?CrYk5??IcjWTZGh|H9WTzOL1NT7j5ZwjdTS0)L8m9&3gTT@c7H!xwlg0OfS!<-{=i_w_W<4lTrvmCHh_nl-P+n4@ea9*60w+72+HRoBM8Kl2qK5ij&AMXODsLXgho1kQt*szdKLq#msh#?Vme+ax$mqq7M}c7;U*iIqw%)+J`;bt;x*Zj-2|X>%cSTV9>K3z<KYulPF|Uk)a;e&)ENe0iUWWu6JV>M7t3DL|(a=PKqPmC<H=$rML!oY8b~5V)Z=B7*vOFe4=<W90$CkYBcts@;|BAVm<(TDAJ{eXSbH)U!k>C%yxzl3`)CO2%R%C7;u&CI;Y5R&9t&Xe2HnrkNZkhC6f$k#H()#JW;sqf@2nl<R$!rc>gkx=hAv{T7FYE*Oi|8eJlNa^0PJ5-&^HM2tLVq+{KVW+PX7wa{6caf)TXb0lsC(P9pR-8xo(b#@CEp=6At7Nz~I;beRrTpZIJdh2PneXiQq75yuDiup4^Y)`Zw4DZ*OpfU1}iSzreVaf}SR8J-C(5WN|o<~~68cBjj=3)=%Ar1q280LgC$9JMrR_K^J$3A}T=v}oY20n#EE;faPlz~EGtm$Wr*UH5k&AG(5FiH}wtq&D~W1kUtdUS|+!8CBT^T;ei9(i)tRSWN^C&tF5F#}a%Dm$PkI5RznEfYweU?pL&+n9&z7N<uq%N;x<5zm;Zhlv6NOP|{BUpbMsG}osX)HIe`(AlKkaY((#u|S>Ma#ed#4M<7MP%kJ+c->Cd_KMDqRh?o)8#$~GnUiWh#L6dF2eVDjs4x4_lFMPbihT6#4s>P#)YeOFgys-l=O2(dC=0w|vUpr75JxA(zB+w~eS&raqix~`F3YhKNgr=4o3i26mk*)5F7FrhpT_($*3-@XOg{VdhnUAm1v#<Zl$&{I%AK504K?1k#6XciTH%8cxA=&&FXTha8?D1l!3W2JkG_6+_8jhDNmWcF(gLY-@4SVS+lzaO`8?{W95!j)jGLMJuH`t9X#`2P_*k)SKAVwxu*_OW9Sh7|ox;+QAgqxBwA+@UUyPB;@#=Mblbqg&PM!xUVx3Xo9Xb@8F@Y*}<VVTtzs+cOm1Y{cN_o6-s#@Wl771ZyBd({CZG3Xu>6bk1X<{A?zs6Ev_evHVdL?V;v^C$9G}^#By!D7jd1wSpcFt5j$5f-IRKWg7MGoeKw0`R+q|@1K-Zz*Q{Qs~<B*qkAu0!Z41{*nPTYTK>c!+t(*{hZ7C$n!-U!!R(1*qIn^onWGZ`4jV2EL3dFxHqo#2n1*8!FJ_q=Dd!lf-yKa2Runi(9Fuyu%5p_H#!Mr<&Vxn$|Q9G3jdbTfcfG@wiqpCB9|#N~}5Ih0U-0_a;!aZLf4=y-T2K16!zRuD#7CHdhGix1M`3k@_;W&5t4D4Xi75k7Zum!dj-?friZ_?X~L^bE;hlBblJ#F+1}C8iui}Em4APqjzN!e`Ftyl+SByzCIxfo3E!xHga%!o_qUsig_-WUM$mqB_3RiL=$rQ5PrIAtGp1ncuX-5z{MEB2CgLqoop99l7ikuR2Ez1vJ5JBbVhXkSdQfe=FiBy*nZbet-IL}I98YL(hg5C52zycz)z83;HPfkvWN6T%y7fJox)QVjSifGHKc!_fzhG)ImLX>KL@laSpxQyEUA={0<WJsN0__YR6`*!rb=Y@!dxn=V#5MCkzga&$UOJ-1|MQhi-@_trAF7b3oB!JL+u*Dj(+6&S9~FWRl{chD?h~!z&iQ^K7c?S&c>iMM;pU&npmb6p`<IP@w{*T!DCrD^9sEXxsn%sTzVRdjT|-iB6!FhVqRKIJ$^GSc2+<yZ|PC??;=kO#5Xv(DJ(`<+UW+V*rq$PXWkxSPK-`rGXx4lVgtsB&knRqckHu{DK03W>uKCvOs>Y>X(D-ud5uc79n17MIP{H5Db5#@Nv;hxldA?&A72Huq)L|avWuzzt&6sC4L!paxteDTp1K`k<RNFz=p`Oz^wjAcGeR1>raOGK>TfTA;^RUXKpmX2?&F!x?Ep@~@u06Ya%bGz{&Zv~yUQ4_R~5Hpsjs63s-r?_HvM{<!3Hk3p=<Os>=1LO{*S`H#Uw!klf-2DB~KOHS1MK^S{VFd5|q?myt2G`tOyseJw6Oz+b-ybJ=K_vjk`(Xy?jTLWh#r|^>(^N%B4=aitUj)H?}O1ilwJ6)|<o=P~m-P#9Z0wCv<B2!N!u8RuOUUS!p#|j{|{{+1;hz`tGipN{drUOkhLBc=cQZy)Io*M^9J4rCmT-T9-5Uz#U?H%N|k83mCD06&b~u^``Eey`z+*PRPu-QBMiBpovE$aS?OwBH5u(q#+bi3xyL>5$?!ZS5nsb{i~aKl8IDp#ZA8BaESToeSnR5AFMk~4pzOocW4yL%vh<X2Z51^6MQ0TSjF0Sx|7|HWs}_xLw29?h!jbYor85DJ%B>5>-nmcqB*ZqM?`3c9QGR*E#we<4~vg>*|myA&XxgVnrL0rpU2G}P`H{mk=JD?vM?;vPuL`<qQ?CheCOg2gW_D%%(g+xJQ%bb^OT_winkcF6^}$c-I)FgLGg1rr<hv{4|d~@grRXKmpD1}1b0xw)rCBZ;JU70uuNKPiTmuOst>W^6MESi#Sw<cdP+%@OJIFtu28Vx%)_ClCT&nWSM54C@hRq2MVM`xIOc&Sj-yNgQMDyE+(eNQX3temaf1}M*{OZ~=pp8Yjw#tqVwHKb&q?qUI;!V=2OV}@swT5el@P3bR272Myesl_>S*xwC{nAFZz2_GtmRI(vG$v7C=nj@qPAIsU$<35F~WI|V>>N7<c^J>j*cB-=pkpfjHNK##WHdRHTu^(Y`xw@C<~mj>~Rv0?Cc?iE>2+1U+Xs-ks9}1>h+Ye9r37}UQ+urV|ktXT6cfV$@@YFS(JfXZjQo!EUk$V_-P%cXeYpkhY$$~?1~51)d+W}Tg<djHx1U<25m}mtwEF4H0Bo6g4pVIbn#r9#%Su~%sW3n1`5YGQ7)B`>2=zc4p#30IE{oiFF|SDVZj5goLlT2;Qp?zo=DS6AaF~gg}#^zKfg$^HpM9fM<1Ot(|(AtACTwcky%GXNJ9r_n23!fz#UtH^uFiC&_^i(Wrq}g!uB6x=my|oQ$Vu}6wsVydL<#>(Sfed3(UQ)^;GQJSF>EuIqhqtn%H%F$r`C-4ieSY);@M*=E=7&6mIbCImDAtwnr+4V#ki3YNCthE|P0|=elnbl8F=^G*#EW&G99~3~xk-ffstjN2PCt4xcS?4u=ar0&p(}fx%pnr{7|4#o<^T)xi^UxkZm^g?~m$Uy*Dhb+Co+%R>BU`tocOvTen2*e0ki&eVsm_iaZiuGnS?g6;plj+lf)U8XYwm+d}7espoUD%Qj>IL{v@+o)I8h90<SbwpiE{ec_L{ygcg!le>Klf16|S0N%;Xk3mCaN?{MKg2L}T)jq{egHd>4=^P1r;<mkD+%5|`N2WboWE(O#YT$8OSeYq5c7Od3g*~xs@vdu6_Y8GN<wl&SKN_Kn~<ufd508=*WmRObM5`HV{UyJOOgSpg41o3a)U2gF_{U1>Zy=}6l7ng)V)A(kqR56U_=TxgOLi_9UGJ?JOvh@*`pp$G(K?=YYM_^x>OYGM{*oiQ{x{upkcg&ahBDSpUG0|sae?OmR6Qecot2}^-!bjg2lnFh&b7<D4cG&kt~sCoLEdUA#M}w*roi|IHfiQ=;Op`(<(8vEy08oha$Hil@l$lVNQAT9_G<qUn7;9gAu%@68kxfBTmPz$R4N7%Bk7_Adwy{6c}HYL1hiT=FRH$ls4u|P9k1M!<v}uN5oD45{&9Kb>h@ypS><zU5sa~F%!}2V8bw%XWQw<fR&Sa(1ERt42A1tMA;M?%UEbUNN>Be%{6KBzEq%Ma%|TekfLBt^l_WKC11g+7<cP=4W#UCa9oCMa4Z6Ig6od)4Mt9Bc2xCr^C+I%A)@1`o?_zZ6x!|=<KTYjbG>oDIBvGM=*4_?GqpcocP}NUIAbV68w?+vBMycK_nfm{BGr|8W{SRqWc4#?P(F>o92->oy+Qh-RW6qlVpYu70apYU%2$q`Y6V<s>8|Jj_Y@;{%F4c`Nrr2h^3(#H^~t&;D^_z#v+1V*CQaG_fp}B`>0CwV&vr}L%%j0DkCx_cFHE#yLIDZ238k*fEm>YRFQh(CSS0Y<$`l(oY>C(80oRx@{YB6A^0OgfkwcmmWWzx+^CqoQ81f1nbHMibTf^tT0f&AzWU#kjXB@U*r)l9Cgh>2Gd=|YV_0-JTr!zC?G^#$uJQ~c9%*V09Ap@D?IO1NeTk7Nca8j3Mw&Qha@y3qF77Fgym_5WG_;jIZLl-*G?GaCOgbcT2yA+d!yG1!fc96Qxs+&mp+nw9qcoV5O?Cb&<N#rU?ZM@~hlQOR<uBT;#6juUu^$i@II+8jbkt#%6rDDH;1aLucncoLa3~`Z&ucyTZ=b&R0&oCWg#ZjZcX0cVjWww?l1)2$G<8@#oy$>`fi%fG}53%u~@+UfJo?>2Jt3<obQ_}@K5eMN!BaztNqfys;W+5P~ej(xO7AJA!w8)&sbM20ST+uhqqf>o~iMyt9vqF}}6|yw*mehL4tL^*${tvPa<Rb'


# Restored verbatim from frozen Stage 2 runner; not a trading-rule change.
def old_anchor():
    return cfg("FROZEN_OLD_SYDNEY_FAILED_BREAKDOWN_55", "FAILED_BREAKDOWN_RECLAIM",
               rr=3.5, sweep_lb=60, body_atr_min=1.0,
               close_loc_min=0.65, context="SYDNEY_BLOCK_04-07")


def recompute_anchor_parity(m15):
    """Hold old timestamp, not today's growing history, for comparability."""
    old=[x for x in m15 if x["time"] <= PARITY_LAST]
    if not old or old[-1]["time"]!=PARITY_LAST:
        raise RuntimeError("Old anchor cutoff candle missing: cannot prove old engine parity")
    nan={k: np.full(len(old),np.nan) for k in
         ("close","ema50","ema100","ema200","atr_ratio50")}
    ff=features(old,nan,nan,nan)
    ix=indices(old_anchor(),ff)
    tx=backtest(old,ix,3.5,1.0)
    s=stats(tx)
    s18=stats([t for t in tx if t["entry_time"] >= datetime(2018,1,1,tzinfo=timezone.utc)])
    s5=stats([t for t in tx if t["entry_time"] >= PARITY_NOW-timedelta(days=365.2425*5)])
    s2=stats([t for t in tx if t["entry_time"] >= PARITY_NOW-timedelta(days=365.2425*2)])
    sc2=stats(backtest(old,ix,3.5,2.0))
    obs={"candle_count":len(old),"trades":s["trades"],"winners":s["winners"],
         "full_pf":s["profit_factor"],"full_r":s["total_r"],
         "since2018_trades":s18["trades"],"since2018_pf":s18["profit_factor"],
         "since2018_r":s18["total_r"],"last5_r":s5["total_r"],
         "last2_r":s2["total_r"],"cost2_pf":sc2["profit_factor"],
         "cost2_r":sc2["total_r"]}
    rows=[{"field":k,"frozen_reference":v,"recomputed":obs[k],
           "pass":obs[k]==v if isinstance(v,int) else abs(obs[k]-v)<=.00002}
          for k,v in REFERENCE.items()]
    write_csv(OUTS["parity"],rows)
    OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
    del ff,old
    import gc;gc.collect()
    if not all(x["pass"] for x in rows):
        raise RuntimeError("STOP: old 55-trade control parity failed; inspect parity CSV")
    return rows


def moments(trades,a=None,b=None):
    if a is None and b is None:return stats(trades)
    return stats([t for t in trades if (a is None or t["entry_time"]>=a)
                  and (b is None or t["entry_time"]<b)])


def reported_row(name,family,factor,value,raw_ix,trades,stress=None):
    pre=datetime(2010,1,1,tzinfo=timezone.utc)
    late=datetime(2018,1,1,tzinfo=timezone.utc)
    years=[(2008,2014),(2014,2020),(2020,9999)]
    full=stats(trades)
    row={"config_id":name,"family":family,"factor_family":factor,
         "factor_value":value,"raw_signal_count":len(raw_ix),
         **{f"full_{k}":v for k,v in full.items()}}
    windows=[("pre2010",None,pre),( "post2010",pre,None),
      ("since2018",late,None),
      ("last5",NOW-timedelta(days=365.2425*5),None),
      ("last2",NOW-timedelta(days=365.2425*2),None),
      ("last1",NOW-timedelta(days=365.2425),None),
      ("era2002_07",None,datetime(2008,1,1,tzinfo=timezone.utc)),
      ("era2008_13",datetime(2008,1,1,tzinfo=timezone.utc),datetime(2014,1,1,tzinfo=timezone.utc)),
      ("era2014_19",datetime(2014,1,1,tzinfo=timezone.utc),datetime(2020,1,1,tzinfo=timezone.utc)),
      ("era2020_now",datetime(2020,1,1,tzinfo=timezone.utc),None)]
    for tag,a,b in windows:
        s=moments(trades,a,b)
        for key in ("trades","total_r","profit_factor","max_drawdown_r"):
            row[f"{tag}_{key}"]=s[key]
    if stress is not None:
        ss=stats(stress)
        for k in ("trades","profit_factor","total_r","max_drawdown_r"):
            row[f"2pip_{k}"]=ss[k]
    row["note"]="Exploratory old history; temporal splits have been inspected, no clean OOS"
    return row


def rolling_diag(config_id,trades):
    out=[];anchor=datetime(max(START.year,2002),6,1,tzinfo=timezone.utc)
    for months in (12,24,36):
        s=anchor
        while add_months(s,months)<=month_floor(NOW):
            e=add_months(s,months)
            result=moments(trades,s,e)
            out.append(dict(config_id=config_id,months=months,start_utc=iso(s),
                            end_utc=iso(e),trades=result["trades"],
                            total_r=result["total_r"],profit_factor=result["profit_factor"],
                            positive=result["total_r"]>0))
            s=add_months(s,1)
    return out


def calendar_diag(config_id,trades):
    rows=[]
    for year in range(START.year,NOW.year):
        s=datetime(year,1,1,tzinfo=timezone.utc);e=datetime(year+1,1,1,tzinfo=timezone.utc)
        result=moments(trades,s,e)
        rows.append(dict(config_id=config_id,year=year,
                         trades=result["trades"],total_r=result["total_r"],
                         profit_factor=result["profit_factor"]))
    return rows



def same_cutoff_parity(candles):
    # First reproduce the OLD Sydney research anchor using original helper.
    old_parity=recompute_anchor_parity(candles)
    old_end=next((i for i,c in enumerate(candles) if c['time']==STAGE2_LAST), None)
    if old_end is None: raise RuntimeError('Stage2 cutoff candle absent; parity cannot be established')
    frozen=candles[:old_end+1]
    checks=[('s2_candle_count',len(frozen),STAGE2_CANDLES,0)]
    if len(frozen)!=STAGE2_CANDLES:
        raise RuntimeError('Stage2 frozen candle count mismatch; history may have changed')
    blank={k:np.full(len(frozen),np.nan) for k in ('close','ema50','ema100','ema200','atr_ratio50')}
    f=features(frozen,blank,blank,blank)
    for depth,control in ((0.0,STAGE2_NO_DEPTH),(.10,STAGE2_BASE)):
        ix,tx=compute(f,frozen,depth,1.0)
        s=stats(tx)
        if depth==.10:
            archived=json.loads(zlib.decompress(base64.b85decode(ARCHIVED_STAGE2_P010_LEDGER_B85)).decode())
            if len(archived)!=len(tx):
                raise RuntimeError(f'Frozen P0.10 archived ledger length mismatch: {len(archived)} versus {len(tx)}')
            numeric=('reference_entry','historical_fill','stop','target','result_r','rr','cost_pips')
            for num,(old,cur) in enumerate(zip(archived,tx)):
                for key in ('signal_index','exit_index'):
                    if int(old[key])!=cur[key]:
                        raise RuntimeError(f'Stage2 archived trade {num} {key} does not match')
                for key in ('entry_time_utc','exit_time_utc','exit_reason'):
                    if str(old[key])!=str(cur[key]):
                        raise RuntimeError(f'Stage2 archived trade {num} {key} does not match')
                for key in numeric:
                    if abs(float(old[key])-float(cur[key]))>1e-9:
                        raise RuntimeError(f'Stage2 archived trade {num} {key} does not match')
            checks.append(('s2_P0.100_all_archived_trade_fields_matched',len(tx),len(archived),0))
        s2=stats(backtest(frozen,ix,RR_FIXED,2.0))
        for key,val in control.items():
            observed=s2['total_r'] if key=='cost2_r' else s[key]
            # Stage2 no-depth reference must be exact, no loose rounding.
            checks.append((f's2_P{depth:.3f}_{key}',observed,val,2e-5))
        OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
    write_csv(OUTS['parity'],[
       {"check":name,"recomputed":actual,"frozen_stage2":expected,
        "pass":bool(abs(actual-expected)<=eps),"tolerance":eps}
       for name,actual,expected,eps in checks]+[
       {"check":"old_sydney_"+str(r['field']),"recomputed":r['recomputed'],
        "frozen_stage2":r['frozen_reference'],"pass":r['pass'],"tolerance":2e-5}
       for r in old_parity])
    if not all(abs(a-b)<=e for _,a,b,e in checks):
        raise RuntimeError('STOP: Stage2 sweep parity failed; see frozen parity CSV')
    del frozen,f
    gc.collect()
    return checks


def accepted_differences(lower,upper,lo_id,hi_id,cost):
    a={t['signal_index']:t for t in lower}
    b={t['signal_index']:t for t in upper}
    shared=a.keys() & b.keys()
    removed=a.keys()-b.keys()
    added=b.keys()-a.keys()
    return dict(lower=lo_id,higher=hi_id,cost_pips=cost,
                lower_accepted=len(a),higher_accepted=len(b),
                shared_count=len(shared),removed_count=len(removed),
                removed_r_in_lower=sum(a[i]['result_r'] for i in removed),
                newly_eligible_count=len(added),newly_eligible_r=sum(b[i]['result_r'] for i in added),
                result_r_lower=sum(t['result_r'] for t in lower),
                result_r_higher=sum(t['result_r'] for t in upper),
                delta_total_r=sum(t['result_r'] for t in upper)-sum(t['result_r'] for t in lower),
                note='Accepted sets are separately replayed p0; new eligibility can arise after removed trades. The removed subset alone is not the net effect.')


def rolling_summary(rows,cost):
    out=[]
    for months in (12,24,36):
        active=[r for r in rows if r['months']==months and r['trades']>0]
        all_rows=[r for r in rows if r['months']==months]
        out.append(dict(months=months,cost_pips=cost,complete_windows=len(all_rows),
            active_windows=len(active),
            positive_active_pct=100*sum(r['total_r']>0 for r in active)/len(active) if active else 0,
            worst_active_r=min((r['total_r'] for r in active),default=None),
            worst_all_r=min((r['total_r'] for r in all_rows),default=None),
            zero_trade_windows=len(all_rows)-len(active)))
    return out


def context_eligible(f):
    # Gate set BEFORE research: .075, .10, .125 each have >=100 trades,
    # positive 1/2-pip lifetime R, pre/post-2010 and last-5Y positive R.
    # Recent 2-pip performance is NOT used here because that is the
    # weakness context is being tested to explain. It remains a mandatory
    # diagnostic, not an excuse to promote any context automatically.
    return [(p,f'{p:.3f}') for p in (.075,.10,.125)]


def context_filter(f,context):
    name,a,b,operator=context
    va=f[a]
    vb=f[b] if isinstance(b,str) else b
    if operator=='>': return np.isfinite(va)&np.isfinite(vb)&(va>vb)
    if operator=='>=':return np.isfinite(va)&(va>=vb)
    raise ValueError(operator)


def trow(config_id,tag,trades):
    return [dict(config_id=config_id,cost_pips=tag,**{k:(iso(v) if isinstance(v,datetime) else v)
               for k,v in tr.items()}) for tr in trades]


def bundle():
    with zipfile.ZipFile(BUNDLE,'w',zipfile.ZIP_DEFLATED) as z:
        for p in OUTS.values():
            if os.path.isfile(p):z.write(p,arcname=os.path.basename(p))


def run_research():
    try:
        STATUS.update(state='fetch',progress=1,message='Fetching full AUD/USD M15 history for boundary resolution')
        m15=fetch('M15',START,NOW,35)
        if len(m15)<STAGE2_CANDLES:raise RuntimeError('Not enough candles to reproduce Stage2 cutoff')
        STATUS.update(state='parity',progress=23,message='Checking Sydney anchor AND Stage2 0/0.10 sweep references')
        checks=same_cutoff_parity(m15)
        STATUS.update(state='features',progress=31,message='Computing fixed-geometry feature cache')
        blank={k:np.full(len(m15),np.nan) for k in ('close','ema50','ema100','ema200','atr_ratio50')}
        f=features(m15,blank,blank,blank)
        coverage=dict(pair=PAIR,granularity='M15',side='BUY',first_candle=iso(m15[0]['time']),
                 last_candle=iso(m15[-1]['time']),candles=len(m15),
                 stage2_reference_cutoff=iso(STAGE2_LAST),parity='PASS',
                 frozen_geometry='LB40 body1.00 mom4<=-1.00 no wick/session/EMA',
                 rr=RR_FIXED,cost1=1.0,cost2=2.0,penetration_thresholds=len(PENETRATION_GRID),
                 orders_supported=False)
        write_csv(OUTS['coverage'],[coverage])
        grid=[];cost_rows=[];rolls=[];roll_summary=[];cal=[];increments=[];buckets=[]
        prior_by_cost={};anchor_tx={};anchor_ix={};core_trades=[]
        STATUS.update(state='penetration_grid',progress=34,message='Fixed lookback/body/momentum; only penetration varies')
        n=len(PENETRATION_GRID)
        for j,p in enumerate(PENETRATION_GRID):
            name=f'P{p:.3f}'
            ix=ixs(f,p)
            by_cost={}
            one_row=None
            for cost in (1.,2.):
                tx=backtest(m15,ix,RR_FIXED,cost)
                by_cost[cost]=tx
                cost_rows.append(cost_eras(name,cost,ix,tx))
                if cost==1.:
                    one_row=row_for(name,cost,ix,tx)
                rr=rolling_diag(name,tx)
                for r in rr:r['cost_pips']=cost
                rolls.extend(rr)
                for s in rolling_summary(rr,cost):
                    roll_summary.append(dict(config_id=name,**s))
                for r in calendar_diag(name,tx):
                    r['cost_pips']=cost
                    cal.append(r)
                if p==FROZEN_P and cost==1.:
                    core_trades=trow(name,cost,tx)
                    anchor_tx[cost]=tx
                    anchor_ix[cost]=ix
                if p==FROZEN_P and cost==2.:
                    anchor_tx[cost]=tx
                    anchor_ix[cost]=ix
                if j>0:
                    lo_name=f'P{PENETRATION_GRID[j-1]:.3f}'
                    increments.append(accepted_differences(prior_by_cost[cost],tx,lo_name,name,cost))
                # complete rows written before clearing caches
            assert one_row is not None
            s2=stats(by_cost[2.])
            one_row.update(penetration_min_atr=p,full2_trades=s2['trades'],
                           full2_r=s2['total_r'],full2_pf=s2['profit_factor'],
                           full2_max_dd=s2['max_drawdown_r'],
                           last2_2pip_r=moments(by_cost[2.],NOW-timedelta(days=365.2425*2))['total_r'],
                           note='Exploratory penetration boundary only; 0.10 frozen reference; no winning threshold automatically chosen')
            grid.append(one_row)
            prior_by_cost=by_cost
            write_csv(OUTS['penetrations'],grid)
            write_csv(OUTS['costs'],cost_rows)
            write_csv(OUTS['incremental'],increments)
            STATUS.update(progress=34+int(36*(j+1)/n),message=f'Penetration {j+1}/{n}: {name}')
            # avoid retaining old 1000s of outcome objects unnecessarily
            OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
        write_csv(OUTS['rolling'],rolls)
        write_csv(OUTS['rolling_summary'],roll_summary)
        write_csv(OUTS['calendar'],cal)
        write_csv(OUTS['control_trades'],core_trades)
        # Partition frozen 0-penetration accepted trades by ACTUAL measured depth.
        # These are attribution bins, not independently p0-executed strategies.
        bucket_edges=(0.,.025,.05,.075,.10,.125,.15,.175,.20,.25,.30,float('inf'))
        root_ix=ixs(f,0.0)
        root_tx_by_cost={}
        for cost in (1.,2.):
            root_tx_by_cost[cost]=backtest(m15,root_ix,RR_FIXED,cost)
        all_p=(f['prev_low'][FROZEN_LB]-f['low'])/f['atr']
        for j,(lo,hi) in enumerate(zip(bucket_edges,bucket_edges[1:])):
            mask=sweep_mask(f,0.0)&(all_p>=lo)&(all_p<hi)
            for cost in (1.,2.):
                tr=[x for x in root_tx_by_cost[cost]
                   if lo<=all_p[x['signal_index']]<hi]
                r=stats(tr)
                buckets.append(dict(bucket_from=lo,bucket_to=hi,cost_pips=cost,
                                    raw_signals=int(mask.sum()),accepted_at_root=len(tr),
                                    accepted_root_total_r=r['total_r'],accepted_root_pf=r['profit_factor'],
                                    note='These are p0 accepted at P0.000, not standalone replayed depth-bin strategies'))
        write_csv(OUTS['penetration_buckets'],buckets)
        OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
        STATUS.update(state='gate',progress=75,message='Assessing predeclared boundary-coherence gate')
        by_depth={round(r['penetration_min_atr'],3):r for r in grid}
        neighbours=[by_depth[p] for p in (.075,.10,.125)]
        gate_parts={
          'at_least_100_each':all(r['full_trades']>=100 for r in neighbours),
          'full_1pip_positive_each':all(r['full_total_r']>0 for r in neighbours),
          'full_2pip_positive_each':all(r['full2_r']>0 for r in neighbours),
          'pre2010_positive_each':all(r['pre2010_total_r']>0 for r in neighbours),
          'post2010_positive_each':all(r['post2010_total_r']>0 for r in neighbours),
          'last5_positive_each':all(r['last5_total_r']>0 for r in neighbours),
        }
        can_context=all(gate_parts.values())
        # Does 0.30 sit at the boundary with comparable frequency / cost positivity?
        edge=by_depth[.30]
        unresolved=(edge['full_trades']>=100 and edge['full_total_r']>0 and edge['full2_r']>0)
        decision=[dict(item=k,passed=v,note='All three .075/.10/.125 neighbours must pass')
                    for k,v in gate_parts.items()]
        decision.extend([
          dict(item='context_stage_eligible',passed=can_context,
               note='Only permits four PREDECLARED separate context diagnostics on frozen P0.10, never automatic selection'),
          dict(item='upper_penetration_boundary_unresolved',passed=unresolved,
               note='If true, STOP_AT_EDGE: do not call 0.30 optimal; separately extend only if justified'),
          dict(item='historical_27_to_28_replay_performed',passed=False,
               note='No live/portfolio strategy evaluated, no activation permitted'),
        ])
        write_csv(OUTS['decision'],decision)
        ctxrows=[]; ctxattrib=[];ctxledger=[]
        if can_context:
            STATUS.update(state='context_fetch',progress=78,message='Boundary coherent; fetching completed H1/H4 for four SEPARATE contexts')
            h1=fetch('H1',START,NOW,180)
            h4=fetch('H4',START,NOW,180)
            m_times=f['times']
            h1a=align_htf(m_times,htf_state(h1))
            h4a=align_htf(m_times,htf_state(h4))
            f.update(h1_close=h1a['close'],h1_ema100=h1a['ema100'],
                    h1_ema50=h1a['ema50'],h1_ema200=h1a['ema200'],
                    h4_close=h4a['close'],h4_ema100=h4a['ema100'],
                    h4_atr=h4a['atr_ratio50'])
            original_ix=anchor_ix[1.]
            original_trades=anchor_tx[1.]
            for k,context in enumerate(CONTEXTS):
                name=context[0]
                mask=sweep_mask(f,FROZEN_P)&context_filter(f,context)
                ix=np.flatnonzero(mask).tolist()
                bycost={cost:backtest(m15,ix,RR_FIXED,cost) for cost in (1.,2.)}
                s1=stats(bycost[1.]);s2=stats(bycost[2.])
                row=row_for(name,1.,ix,bycost[1.])
                row.update(context=name,baseline='P0.100',
                           removed_raw_signals=len(original_ix)-len(ix),
                           stress_total_r=s2['total_r'],stress_pf=s2['profit_factor'],
                           stress_last2_r=moments(bycost[2.],NOW-timedelta(days=365.2425*2))['total_r'],
                           explanatory_only=True)
                ctxrows.append(row)
                ctxattrib.append(accepted_differences(original_trades,bycost[1.],
                                                     'P0.100',name,1.))
                ctxattrib.append(accepted_differences(anchor_tx[2.],bycost[2.],
                                                     'P0.100',name,2.))
                for cost in (1.,2.):
                    ctxledger.extend(trow(name,cost,bycost[cost]))
                OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
                STATUS.update(progress=80+int(18*(k+1)/len(CONTEXTS)),message=f'Context {k+1}/{len(CONTEXTS)}')
        else:
            ctxrows=[dict(context='SKIPPED',reason='One or more .075/.10/.125 coherence gates failed',
                          never_automatically_select=True)]
        for key,rows in [('context',ctxrows),('context_attribution',ctxattrib),('context_trades',ctxledger)]:
            write_csv(OUTS[key],rows)
        write_csv(OUTS['methods'],[
          dict(item='scope',value='AUD/USD M15 LONG; frozen LB40, B>=1.00 ATR14, previous 4H momentum<=-1.00 ATR, close above previous high; 11 penetration thresholds 0..0.30'),
          dict(item='frozen_references',value='Old Sydney 55-trade summary parity + Stage2 LB40 penetration 0 and 0.10 at Stage2 candle cutoff; fail hard'),
          dict(item='costs',value='Reference close+1pip adverse BUY fill, stress 2pip; target uses reference risk; actual-fill R'),
          dict(item='signal',value='Signal candle open timestamp, ATR14 Wilder including signal candle; previous 40-bar low excludes signal; 4H prior momentum excludes signal'),
          dict(item='p0',value='One position per candidate; target/stop evaluated from next M15; exit-bar entry eligible; chronology never reset for era/rolling windows'),
          dict(item='HTF',value='Only completed H1/H4 via next actual candle open; four separate predeclared filters, no EMA length optimisation; skipped if plateau gate fails'),
          dict(item='not_tested',value='No RR, weekday, session, new trigger, combination, live pricing, live orders or Portfolio27→28 portfolio replay'),
          dict(item='boundary',value='If 0.30 remains profitable at 2pip with >=100 trades, upper edge remains unresolved: STOP_AT_EDGE, not optimal'),
          dict(item='validation',value='All historical eras have been searched in prior studies; positive 2018+ or recent data are diagnostics, not OOS confirmation'),
        ])
        bundle()
        STATUS.update(state='complete',progress=100,message='Boundary study complete; research only',
                      frozen_parity='PASS',penetrations=len(grid),
                      contexts_run=len(ctxrows) if can_context else 0,
                      context_gate=can_context,stop_at_upper_edge=unresolved,
                      orders_supported=False,trading_enabled=False,
                      results='/audusd-m15-long-boundary/results')
    except Exception as e:
        STATUS.update(state='error',message=str(e),traceback=traceback.format_exc(),
                      orders_supported=False,trading_enabled=False)
        try:
            write_csv(OUTS['error'],[{'state':'error','progress':STATUS.get('progress'),
                                     'message':str(e),'traceback':STATUS['traceback'],
                                     'orders_supported':False,'trading_enabled':False}])
            bundle()
        except Exception:
            print('Could not write error report ZIP',flush=True)
        print(STATUS['traceback'],flush=True)


@app.route('/')
def index_route():
    return jsonify(service='AUD/USD M15 LONG penetration boundary research — READ ONLY',
                   status='/audusd-m15-long-boundary/status',
                   results='/audusd-m15-long-boundary/results',
                   orders_supported=False,trading_enabled=False)


@app.route('/audusd-m15-long-boundary/status')
def status_route():return jsonify(STATUS)


@app.route('/audusd-m15-long-boundary/results')
def results_route():return download(BUNDLE)


if __name__=='__main__':
    threading.Thread(target=run_research,daemon=True).start()
    app.run(host='0.0.0.0',port=int(os.getenv('PORT','8080')),debug=False)
