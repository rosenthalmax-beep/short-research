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
# AUD/USD M15 LONG — FRESH SIX-FAMILY BROAD DISCOVERY (PROSPECTIVE #27)
# ============================================================
# This is standalone research, NOT a live strategy or a deployed #27.
# The current 26 live strategies are not modified or accessed.
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




# ======================================================================
# PORTFOLIO 27 AUD/USD M15 LONG — STAGE 1 RAW SIGNAL & SINGLE-FACTOR AUDIT
# ======================================================================
# This deliberately REOPENS exploratory history. It is not independent OOS.
# Previous setups are frozen controls; NEVER silently vary them in the scan.
# No multi-filter combinations, RR tuning, top-row auto-selection or orders.
# Fixed RR3.50, 1 pip adverse historical BUY fill (2 pip cost stress).
# Baseline p0 replay is chronological before subperiod slicing.
# A later study may test conditional interactions ONLY after these reports.

OUTS = {
    "coverage": "audusd_m15_long_reaudit_coverage.csv",
    "parity": "audusd_m15_long_reaudit_old_core_parity.csv",
    "raw": "audusd_m15_long_reaudit_raw_families.csv",
    "single": "audusd_m15_long_reaudit_single_factor.csv",
    "conditional_raw": "audusd_m15_long_reaudit_raw_trade_ledgers.csv",
    "controls": "audusd_m15_long_reaudit_frozen_controls.csv",
    "control_trades": "audusd_m15_long_reaudit_frozen_control_trades.csv",
    "cost": "audusd_m15_long_reaudit_cost_stress.csv",
    "rolling": "audusd_m15_long_reaudit_rolling.csv",
    "calendar": "audusd_m15_long_reaudit_completed_years.csv",
    "notes": "audusd_m15_long_reaudit_methods.csv",
}
BUNDLE = "AUDUSD_M15_LONG_PORTFOLIO27_RAW_EDGE_AUDIT_RESULTS.zip"
STATUS.clear()
STATUS.update(state="not_started", message="Waiting to fetch", progress=0,
              orders_supported=False, trading_enabled=False)

PARITY_LAST = datetime(2026, 9, 18, 20, 45, tzinfo=timezone.utc)
PARITY_NOW = datetime(2026, 9, 19, 11, 55, tzinfo=timezone.utc)
REFERENCE = {
    "candle_count": 546849,
    "trades": 55, "winners": 21, "full_pf": 1.955626,
    "full_r": 32.491274, "since2018_trades": 13,
    "since2018_pf": 1.398884, "since2018_r": 3.589957,
    "last5_r": 2.339957, "last2_r": 1.146079,
    "cost2_pf": 1.781976, "cost2_r": 26.587184,
}
RR_FIXED=3.5
SCAN_MIN_TRADES_FOR_COST=50
FAMILY_IDS=(
    "BULL_ENGULF", "FAILED_BREAKDOWN", "LOW_SWEEP_DISPLACEMENT",
    "OUTSIDE_REVERSAL", "COMPRESSION_BREAKOUT", "PULLBACK_REJECTION",
)


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


def raw_family_masks(f):
    """These are minimally defined *hypotheses*, not copies of the old winners."""
    base=f["valid_atr"] & f["bullish"]
    old_high=np.r_[np.nan,f["high"][:-1]]
    old_low=np.r_[np.nan,f["low"][:-1]]
    low10=f["prev_low"][10]
    out={
      "BULL_ENGULF": base & f["exact_bull"] & (f["bull_br"]>=1.0),
      "FAILED_BREAKDOWN": base & (f["low"]<low10) & (f["close"]>low10),
      "LOW_SWEEP_DISPLACEMENT": base & (f["low"]<low10) & (f["close"]>old_high),
      "OUTSIDE_REVERSAL": base & (f["low"]<old_low) & (f["high"]>old_high),
      "COMPRESSION_BREAKOUT": base & (f["compression"]<=1.0)
                               & (f["close"]>f["prev_high"][10]),
      "PULLBACK_REJECTION": base & (f["low"]<f["prev_low"][20])
                              & (f["close"]>low10) & (f["mom4"]<=0),
    }
    for k in out:
        out[k][:200]=False
    return out


def predeclared_factors(f):
    """Each entry is a single overlay on a raw family; never combine here."""
    out=[]
    for field,label,thresholds in [
       ("body_atr","BODY_ATR_MIN",(.50,.75,1.00,1.25)),
       ("range_atr","RANGE_ATR_MIN",(.75,1.00,1.25,1.50)),
       ("close_loc","CLOSE_LOCATION_MIN",(.55,.65,.75,.85)),
       ("lower_wick_body","LOWER_WICK_BODY_MIN",(.10,.20,.35,.50)),
    ]:
        for v in thresholds:out.append((f"{label}_{v:.2f}",f[field]>=v,label,v))
    for v in (-.25,-.50,-1.00):
        out.append((f"PRIOR_4H_DECLINE_ATR_{v:.2f}",f["mom4"]<=v,"PRIOR_4H_MOMENTUM",v))
    for lb in (40,60,100,165):
        for d in (.10,.25,.50):
            out.append((f"NEAR_PREV_LOW_LB{lb}_D{d:.2f}",
                        f["structure_dist_low"][lb]<=d,
                        f"PRIOR_LOW_DISTANCE_LB{lb}",d))
    for lb in (20,40,60,100):
        out.append((f"BREAK_PREV_LOW_LB{lb}", f["low"]<f["prev_low"][lb],
                    "PREV_LOW_BREAK_LOOKBACK",lb))
    for name,a,b in [
        ("H1_CLOSE_GT_EMA100","h1_close","h1_ema100"),
        ("H1_EMA50_GT_EMA200","h1_ema50","h1_ema200"),
        ("H4_CLOSE_GT_EMA100","h4_close","h4_ema100"),
        ("H4_EMA100_GT_EMA200","h4_ema100","h4_ema200"),
        ("D_CLOSE_GT_EMA200","d_close","d_ema200"),
        ("D_EMA50_GT_EMA200","d_ema50","d_ema200"),
    ]:out.append((name,f[a]>f[b],"COMPLETED_HTF_REGIME",name))
    for tf in ("h1","h4","d"):
        for level in (.80,1.00):
            name=f"{tf.upper()}_ATR_RATIO_GE_{level:.2f}"
            out.append((name,f[f"{tf}_atr"]>=level,"COMPLETED_HTF_VOLATILITY",level))
    for tz,key in (("NY","ny_hour"),("SYDNEY","sydney_hour"),
                   ("TOKYO","tokyo_hour"),("LONDON","london_hour")):
        for start in (0,4,8,12,16,20):
            name=f"{tz}_HOURS_{start:02d}-{start+3:02d}"
            out.append((name,(f[key]>=start)&(f[key]<=start+3),
                        f"TIME_4H_{tz}",start))
    for wd in range(5):
        out.append((f"EXCLUDE_NY_WEEKDAY_{wd}",f["ny_weekday"]!=wd,
                    "EXCLUDE_WEEKDAY",wd))
    return out


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


def control_ledger(m15,f):
    """Two OLD decisions shown as controls: no optimization/selection from them."""
    old=old_anchor();ix=indices(old,f)
    old_trades=backtest(m15,ix,3.5,1.0)
    # Published complement SWEEP_STRONG_60: base previous-low sweep + strict
    # above-previous-high displacement + prior downward momentum & wick.
    sweep=cfg("OLD_SWEEP_STRONG_60", "LOW_SWEEP_DISPLACEMENT",rr=3.5,
              sweep_lb=60,body_atr_min=1.0,lower_wick_body_min=.20,mom4_min=-1.0)
    six=indices(sweep,f)
    sweep_trades=backtest(m15,six,3.5,1.0)
    return [(old["config_id"],ix,old_trades),
            (sweep["config_id"],six,sweep_trades)]


def write_bundle():
    with zipfile.ZipFile(BUNDLE,"w",zipfile.ZIP_DEFLATED) as z:
        for path in OUTS.values():
            if os.path.isfile(path):z.write(path,arcname=os.path.basename(path))


def run_research():
    try:
        STATUS.update(state="fetch",progress=1,message="Fetching AUD/USD history: research only")
        m15=fetch("M15",START,NOW,35)
        if len(m15)<100_000:raise RuntimeError("Insufficient M15 history")
        STATUS.update(state="old_control",progress=14,message="Recomputing old 55-trade anchor at FROZEN cutoff")
        recompute_anchor_parity(m15)
        STATUS.update(state="fetch_context",progress=20,message="Fetching strictly completed H1/H4/D context")
        h1=fetch("H1",WARMUP,NOW,180)
        h4=fetch("H4",WARMUP,NOW,700)
        daily=fetch("D",WARMUP,NOW,3500)
        if not all((h1,h4,daily)):raise RuntimeError("HTF data incomplete")
        write_csv(OUTS["coverage"],[dict(instrument=PAIR,granularity="M15",side="BUY",
                     first_m15=iso(m15[0]["time"]),last_m15=iso(m15[-1]["time"]),
                     m15_bars=len(m15),h1_bars=len(h1),h4_bars=len(h4),
                     daily_bars=len(daily),rr=RR_FIXED,assumed_cost_pips=PRIMARY_COST,
                     risk_model="signal close + adverse cost; stop low-10 ticks; reference-risk target")])
        STATUS.update(state="features",progress=27,message="Computing unbiased raw signals and HTF alignments")
        times=[v["time"] for v in m15]
        f=features(m15,align_htf(times,htf_state(h1)),
                   align_htf(times,htf_state(h4)),align_htf(times,htf_state(daily)))
        raw_masks=raw_family_masks(f)
        factors=predeclared_factors(f)
        raw_rows=[];factor_rows=[];raw_ledgers=[];control_rows=[];control_trades=[]
        cost_rows=[];roll=[];cal=[]
        for j,(name,ix,tr) in enumerate(control_ledger(m15,f)):
            c2=backtest(m15,ix,RR_FIXED,2.0)
            control_rows.append(reported_row(name,"OLD_REFERENCE","OLD_REFERENCE","unchanged",ix,tr,c2))
            for t in tr:control_trades.append(dict(config_id=name,**{k:v for k,v in t.items() if k not in ("entry_time","exit_time")}))
            for cost,ts in ((1.0,tr),(2.0,c2)):
                cost_rows.append(dict(config_id=name,cost_pips=cost,**stats(ts)))
            roll.extend(rolling_diag(name,tr));cal.extend(calendar_diag(name,tr))
            OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
        STATUS.update(state="raw_families",progress=35,message="Six raw long mechanisms, fixed RR3.5")
        for k,family in enumerate(FAMILY_IDS):
            base=raw_masks[family]
            ix=np.flatnonzero(base).tolist()
            tr=backtest(m15,ix,RR_FIXED,1.0)
            ts=backtest(m15,ix,RR_FIXED,2.0)
            raw_rows.append(reported_row("RAW_"+family,family,"NONE","raw",ix,tr,ts))
            for t in tr:raw_ledgers.append(dict(config_id="RAW_"+family,**{p:v for p,v in t.items() if p not in ("entry_time","exit_time")}))
            for cost,ledger in ((1.0,tr),(2.0,ts)):
                cost_rows.append(dict(config_id="RAW_"+family,cost_pips=cost,**stats(ledger)))
            roll.extend(rolling_diag("RAW_"+family,tr));cal.extend(calendar_diag("RAW_"+family,tr))
            OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
            STATUS.update(progress=35+int(20*(k+1)/len(FAMILY_IDS)),message=f"Raw family {k+1}/{len(FAMILY_IDS)}: {family}")
        write_csv(OUTS["raw"],raw_rows)
        write_csv(OUTS["controls"],control_rows)
        write_csv(OUTS["conditional_raw"],raw_ledgers)
        write_csv(OUTS["control_trades"],control_trades)
        STATUS.update(state="single_factor",progress=56,
                      message=f"Single-factor scans: {len(factors)} per raw family; no combos or RR tuning")
        for k,family in enumerate(FAMILY_IDS):
            base=raw_masks[family]
            for j,(label,overlay,group,value) in enumerate(factors):
                indices_arr=np.flatnonzero(base&overlay).tolist()
                tr=backtest(m15,indices_arr,RR_FIXED,1.0)
                # No selection: all rows exported, including negative/zero trade.
                stress=None
                if len(tr)>=SCAN_MIN_TRADES_FOR_COST:
                    stress=backtest(m15,indices_arr,RR_FIXED,2.0)
                factor_rows.append(reported_row(family+"__"+label,family,group,value,
                                                indices_arr,tr,stress))
                OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
            STATUS.update(progress=56+int(39*(k+1)/len(FAMILY_IDS)),
                          message=f"Single-factor family {k+1}/{len(FAMILY_IDS)} complete")
            write_csv(OUTS["single"],factor_rows)
        write_csv(OUTS["cost"],cost_rows)
        write_csv(OUTS["rolling"],roll)
        write_csv(OUTS["calendar"],cal)
        write_csv(OUTS["notes"],[
            dict(item="objective",value="Reassess AUD/USD M15 LONG from raw mechanisms before conditional matrices"),
            dict(item="prior_work",value="Old Sydney 55-trade core and strong low-sweep are searched-history REFERENCES only; archived old core+sweep 243 trades/+98.32R/-22R DD is contextual history NOT recalculated by this Stage 1 scan"),
            dict(item="data",value="May 2002 onwards: growing OANDA midpoint candles, as complete; original parity fixed Sep 18 2026"),
            dict(item="stage1",value="Six minimal raw bullish mechanisms, each reported separately"),
            dict(item="factor",value="Each of the predeclared overlays applied ONE AT A TIME to every raw family"),
            dict(item="cost",value="1pip assumed adverse BUY fill; 2pip stress for raw/control and scan rows >=50 accepted trades"),
            dict(item="time",value="Signal is candle OPEN; completed HTF mapped at next observed HTF open; conservative at M15 boundary"),
            dict(item="no_orders",value="Research-only GET OANDA candle API; does not import executor or place orders"),
            dict(item="limits",value="Repeated use of old 2002-2026 data; no untouched historical out-of-sample claims"),
            dict(item="next_stage",value="Inspect factor monotonicity, sample, era, cost, retained/removed trades, then tiny conditional matrices"),
            dict(item="portfolio",value="Do not promote before 27->28 exact chronological AUD/USD nonhedging portfolio add"),
            dict(item="p0",value="Full-history trade replay; same exit candle may provide new signal; historical exits are bar ambiguous"),
        ])
        write_bundle()
        STATUS.update(state="complete",progress=100,message="Read-only Stage 1 audit complete",
                      raw_families=len(raw_rows),single_factor_rows=len(factor_rows),
                      factors_per_family=len(factors),old_control_parity="PASS",
                      results_route="/audusd-m15-long-reaudit/results")
    except Exception as ex:
        import traceback
        STATUS.update(state="error",message=str(ex),traceback=traceback.format_exc(),
                      orders_supported=False,trading_enabled=False)
        try:write_bundle()
        except Exception:pass
        print(STATUS["traceback"],flush=True)

@app.route("/")
def main_route():
    return jsonify(dict(service="AUD/USD M15 LONG Portfolio27 raw edge reaudit",
                        state=STATUS["state"],orders_supported=False,trading_enabled=False,
                        status="/audusd-m15-long-reaudit/status",
                        results="/audusd-m15-long-reaudit/results"))

@app.route("/audusd-m15-long-reaudit/status")
def status_route():return jsonify(STATUS)

@app.route("/audusd-m15-long-reaudit/results")
def results_route():return download(BUNDLE)

if __name__=="__main__":
    threading.Thread(target=run_research,daemon=True).start()
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","8080")),debug=False)
