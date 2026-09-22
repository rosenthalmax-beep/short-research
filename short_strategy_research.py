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





# AUD/USD M15 LONG: Stage 2 conditional geometry (research only).
# Full M15 chronology, RR3.50, assumed historical 1pip adverse BUY fill,
# doubled 2pip cost stress. Two distinct hypotheses are never combined:
# A low-sweep displacement: LB x bullish body ATR x prior 4h decline
#   x a predeclared additional depth diagnostic (0 or 0.10 ATR).
# B failed-breakdown reclaim: LB x body ATR x close location.
# No RR search, no adaptive EMA/weekday/day exclusions, no order endpoints.
# Previously searched data is NOT untouched out-of-sample.
# There is no selection/promotion to live here; exact 27->28 portfolio
# replay with same-pair nonhedging is required for any eventual candidate.


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



# ----------------------------------------------------------------------
# FROZEN PLAN: four long-low lookbacks, fixed neighbourhoods, no adaptive
# trial insertion based on results. Full grid = 96 SWEEP + 64 FAILED.
# ----------------------------------------------------------------------
from itertools import product
import gc
import traceback

OUTS = {
    "coverage": "audusd_m15_long_s2_coverage.csv",
    "parity": "audusd_m15_long_s2_old_control_parity.csv",
    "geometry": "audusd_m15_long_s2_all_160_geometries.csv",
    "plateau": "audusd_m15_long_s2_local_neighbours.csv",
    "attribution": "audusd_m15_long_s2_conditional_attribution.csv",
    "diagnostic": "audusd_m15_long_s2_diagnostic_shortlist.csv",
    "sessions": "audusd_m15_long_s2_session_diagnostics.csv",
    "rolling": "audusd_m15_long_s2_rolling_windows.csv",
    "calendar": "audusd_m15_long_s2_calendar_years.csv",
    "costs": "audusd_m15_long_s2_cost_stress.csv",
    "trades": "audusd_m15_long_s2_diagnostic_trade_ledgers.csv",
    "signals": "audusd_m15_long_s2_signal_overlap.csv",
    "controls": "audusd_m15_long_s2_old_controls.csv",
    "notes": "audusd_m15_long_s2_methodology.csv",
}
BUNDLE = "AUDUSD_M15_LONG_PORTFOLIO27_CONDITIONAL_STAGE2_RESULTS.zip"
STATUS.clear()
STATUS.update(state="not_started",progress=0,message="Waiting to fetch",orders_supported=False,trading_enabled=False)
RR_FIXED = 3.5

# Same anchored historical reference as Stage 1. Do not reselect it.
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

LOOKBACKS=(20,40,60,100)
SWEEP_BODIES=(.75,1.00,1.25)
DECLINES=(.25,.50,1.00,1.50)  # positive magnitude; mom4 <= -decline
PENETRATIONS=(0.00,.10)      # (prior low - signal low)/signal ATR
FAILED_BODIES=(.50,.75,1.00,1.25)
FAILED_CLOSES=(.55,.65,.75,.85)


def make_configs():
    rows=[]
    for lb,body,decline,depth in product(LOOKBACKS,SWEEP_BODIES,DECLINES,PENETRATIONS):
        rows.append(dict(branch="SWEEP",lb=lb,body=body,decline=decline,
                         penetration=depth,close_loc=None,
                         config_id=f"SWEEP_L{lb}_B{body:.2f}_M{decline:.2f}_P{depth:.2f}"))
    for lb,body,cl in product(LOOKBACKS,FAILED_BODIES,FAILED_CLOSES):
        rows.append(dict(branch="FAILED",lb=lb,body=body,decline=None,
                         penetration=None,close_loc=cl,
                         config_id=f"FAILED_L{lb}_B{body:.2f}_C{cl:.2f}"))
    assert len(rows)==160 and len({r['config_id'] for r in rows})==160
    return rows


def geometry_mask(c,f,raw):
    # Crucial: anchor each geometry to ITS OWN previous-low lookback.
    # Do not inherit the raw 10-bar reclaim requirement when testing a
    # 20/40/60/100-bar failed breakdown: close > previous 10-bar low
    # is a DIFFERENT entry from close > previous 60-bar low.
    mask=f["valid_atr"].copy() & f["bullish"]
    lb=c["lb"]
    prior=f["prev_low"][lb]
    mask &= f["low"]<prior
    mask &= f["body_atr"]>=c["body"]
    if c["branch"]=="SWEEP":
        prior_candle_high=np.r_[np.nan,f["high"][:-1]]
        mask &= f["close"]>prior_candle_high
        mask &= f["mom4"]<=-c["decline"]
        # A strict low break is already required. Penetration 0.00
        # means no additional constraint (not an equal-low signal).
        if c["penetration"]>0:
            mask &= (prior-f["low"])/f["atr"]>=c["penetration"]
    else:
        mask &= f["close"]>prior  # reclaim SAME previous low as the break
        mask &= f["close_loc"]>=c["close_loc"]
    mask[:200]=False
    return mask


def summarise(name,branch,ix,trades,stress,extra=None):
    r=reported_row(name,branch,"CONDITIONAL_GEOMETRY",extra or "",ix,trades,stress)
    # report_row contains full plus historical splits; not an OOS test
    r.update(extra or {})
    # Strictly predeclared illustrative gate, NOT portfolio approval.
    # No redefinition based on how many pass.
    r["diagnostic_gate"]=(
        r["full_trades"]>=80 and r["pre2010_total_r"]>0 and
        r["post2010_total_r"]>0 and r["since2018_total_r"]>0 and
        r["last5_total_r"]>0 and r["last2_total_r"]>0 and
        (r.get("2pip_total_r") or 0)>0 and r["full_max_drawdown_r"]>=-15
    )
    return r


def conditional_attribution(c,f,raw,m15,full_ix,full_trades):
    """Same-branch ablation under p0; do not subtract backtest returns
    and call that the profitability of removed trades. Signal and
    accepted-trade comparisons are reported separately."""
    base_key="LOW_SWEEP_DISPLACEMENT" if c["branch"]=="SWEEP" else "FAILED_BREAKDOWN"
    full_ids={t["signal_index"]:t for t in full_trades}
    overlays=[("RAW",None),
              ("BREAK_LB", f["low"]<f["prev_low"][c["lb"]]),
              ("BODY",f["body_atr"]>=c["body"])]
    if c["branch"]=="SWEEP":
        overlays += [("MOMENTUM",f["mom4"]<=-c["decline"])]
        if c["penetration"]>0:
            overlays += [("PENETRATION",(f["prev_low"][c["lb"]]-f["low"])/f["atr"]>=c["penetration"])]
    else:
        overlays += [("CLOSE_LOCATION",f["close_loc"]>=c["close_loc"])]
    lines=[]
    for label,mask in overlays:
        ix=np.flatnonzero(raw[base_key] if mask is None else raw[base_key]&mask).tolist()
        tx=backtest(m15,ix,RR_FIXED,1.0)
        tm={t["signal_index"]:t for t in tx}
        shared=full_ids.keys() & tm.keys()
        only=full_ids.keys()-tm.keys()
        other=tm.keys()-full_ids.keys()
        lines.append(dict(config_id=c["config_id"],branch=c["branch"],factor=label,
                          raw_signals=len(ix),accepted=len(tx),total_r=stats(tx)["total_r"],
                          shared_accepted=len(shared),selected_only=len(only),
                          selected_only_r=sum(full_ids[i]["result_r"] for i in only),
                          ablation_only=len(other),
                          ablation_only_r=sum(tm[i]["result_r"] for i in other),
                          note="p0 replay changes which signals enter; differences in total R are not isolated removal expectancy"))
        OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
    return lines


def local_neighbourhood(configs,rows):
    byid={c['config_id']:c for c in configs}
    summaries={r['config_id']:r for r in rows}
    grouped={}
    for c in configs:
        grouped[(c['branch'],c['lb'],c['body'],c.get('decline'),c.get('penetration'),c.get('close_loc'))]=c
    lines=[]
    for c in configs:
        dims=(['lb','body','decline','penetration'] if c['branch']=='SWEEP'
              else ['lb','body','close_loc'])
        ns=[];at_edge=[]
        for d in dims:
            values={'lb':LOOKBACKS,'body':SWEEP_BODIES if c['branch']=='SWEEP' else FAILED_BODIES,
                    'decline':DECLINES,'penetration':PENETRATIONS,'close_loc':FAILED_CLOSES}[d]
            i=values.index(c[d])
            if i==0 or i==len(values)-1:at_edge.append(d)
            for j in (i-1,i+1):
                if 0<=j<len(values):
                    k=dict(c);k[d]=values[j]
                    target=(k['branch'],k['lb'],k['body'],k.get('decline'),k.get('penetration'),k.get('close_loc'))
                    near=grouped.get(target)
                    if near is not None:ns.append(near['config_id'])
        nrows=[summaries[x] for x in ns]
        good=lambda s:s['full_trades']>=50 and s['full_total_r']>0 and (s.get('2pip_total_r') or 0)>0
        lines.append(dict(config_id=c['config_id'],branch=c['branch'],
                          neighbour_count=len(nrows),neighbour_ids=';'.join(ns),
                          profitable_1pip_neighbours=sum(r['full_total_r']>0 for r in nrows),
                          profitable_2pip_neighbours=sum((r.get('2pip_total_r') or 0)>0 for r in nrows),
                          neighbours_50trades_cost_positive=sum(good(r) for r in nrows),
                          minimum_neighbour_r=min((r['full_total_r'] for r in nrows),default=None),
                          minimum_neighbour_2pip_r=min((r.get('2pip_total_r') or 0 for r in nrows),default=None),
                          at_tested_edges=';'.join(at_edge),
                          tested_range_resolved=(len(at_edge)==0),
                          note='Positive neighbours are descriptive; this grid cannot resolve tested boundaries. No auto-extension.' ))
    return lines


def diagnostic_selection(rows,neighbour_rows):
    nr={r['config_id']:r for r in neighbour_rows}
    selected=[]
    for branch in ('SWEEP','FAILED'):
        subset=[r for r in rows if r['family']==branch]
        strict=[r for r in subset if r['diagnostic_gate']]
        # The shortlist is only for trade and time-of-day attribution.
        # Falls back to 3 illustrative configurations when none pass;
        # this NEVER implies a candidate passed a gate.
        if strict:
            pool=strict
        else:
            pool=[r for r in subset if r['full_trades']>=50]
        def rank(r):
            n=nr[r['config_id']]
            return (n['neighbours_50trades_cost_positive'],
                    r['2pip_total_r']>0 if r.get('2pip_total_r') is not None else False,
                    r['pre2010_total_r']>0,
                    r['post2010_total_r']>0,
                    r['last5_total_r']>0,
                    r['last2_total_r']>0,
                    min(r['full_trades'],200),
                    r['full_total_r'])
        selection=sorted(pool,key=rank,reverse=True)[:3]
        for r in selection:
            selected.append(dict(config_id=r['config_id'],branch=branch,
                                 designation='GATE_PASS_DIAGNOSTIC' if strict else 'NO_GATE_PASS_ILLUSTRATIVE',
                                 why='Review accepted trades and timed subpopulations; NOT a strategy recommendation',
                                 **{k:v for k,v in r.items() if k not in ('config_id','branch')}))
    return selected


def add_ledger(c,ts):
    return [dict(config_id=c['config_id'],branch=c['branch'],
                 **{k:v for k,v in t.items() if k not in ('entry_time','exit_time')}) for t in ts]


def session_diagnostics(c,m15,f,raw,unfiltered):
    mask=geometry_mask(c,f,raw)
    full={t['signal_index']:t for t in unfiltered}
    out=[]
    for key,tz in [('sydney_hour','Sydney'),('ny_hour','New_York')]:
        for start in (0,4,8,12,16,20):
            hours=(f[key]>=start)&(f[key]<start+4)
            selected_ix=np.flatnonzero(mask&hours).tolist()
            active=backtest(m15,selected_ix,RR_FIXED,1.0)
            rawslice=[t for t in unfiltered if start<=int(f[key][t['signal_index']])<start+4]
            out.append(dict(config_id=c['config_id'],branch=c['branch'],timezone=tz,
                  hours=f'{start:02d}-{start+3:02d}',raw_signals=len(selected_ix),
                  replay_trades=len(active),replay_r=stats(active)['total_r'],
                  replay_pf=stats(active)['profit_factor'],
                  attribution_only_accepted=len(rawslice),
                  attribution_only_r=stats(rawslice)['total_r'],
                  note='Diagnostic only. Filtered p0 replay may differ from attribution of accepted all-hours trades; do not select a block here.'))
            OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
    return out


def overlap_controls(c,trades,old_trades,sweep_trades):
    ids={t['signal_index']:t for t in trades}
    out=[]
    for name,other in [('OLD_SYDNEY_FAILED',old_trades),('OLD_SWEEP_STRONG_60',sweep_trades)]:
        oid={t['signal_index']:t for t in other}
        shared=ids.keys()&oid.keys()
        unique=ids.keys()-oid.keys()
        oldonly=oid.keys()-ids.keys()
        out.append(dict(config_id=c['config_id'],branch=c['branch'],reference=name,
                        shared_accepted=len(shared),candidate_only=len(unique),
                        candidate_only_r=sum(ids[i]['result_r'] for i in unique),
                        reference_only=len(oldonly),
                        reference_only_r=sum(oid[i]['result_r'] for i in oldonly),
                        note='Standalone accepted-signal overlap only; NOT chronological pair/portfolio union'))
    return out


def write_bundle():
    with zipfile.ZipFile(BUNDLE,'w',zipfile.ZIP_DEFLATED) as z:
        for p in OUTS.values():
            if os.path.isfile(p):z.write(p,arcname=os.path.basename(p))


def run_research():
    try:
        STATUS.update(state='fetch',progress=1,message='Fetching full AUD/USD M15 midpoint history (read-only)')
        m15=fetch('M15',START,NOW,35)
        if len(m15)<100_000:raise RuntimeError('Insufficient full M15 history')
        STATUS.update(state='parity',progress=18,message='Reproducing frozen 55-trade Sydney reference')
        parity=recompute_anchor_parity(m15)
        STATUS.update(state='features',progress=24,message='Computing completed-candle and prev-only structure features')
        # No HTF filters in Stage 2. Keep identical feature implementation
        # with NaN higher-timeframe arrays; do not fetch irrelevant H1/H4/D.
        blank={k:np.full(len(m15),np.nan) for k in ('close','ema50','ema100','ema200','atr_ratio50')}
        f=features(m15,blank,blank,blank)
        raw=raw_family_masks(f)
        controls=[]
        for config in (old_anchor(),cfg('OLD_SWEEP_STRONG_60','LOW_SWEEP_DISPLACEMENT',rr=3.5,
                    sweep_lb=60,body_atr_min=1.0,lower_wick_body_min=.20,mom4_min=-1.0)):
            ix=indices(config,f)
            tr=backtest(m15,ix,RR_FIXED,1.0)
            stress=backtest(m15,ix,RR_FIXED,2.0)
            controls.append(dict(config_id=config['config_id'],**stats(tr),
                                 two_pip_r=stats(stress)['total_r'],
                                 date='growing history; archived reference at frozen cutoff is 55 trades'))
            if config['family']=='FAILED_BREAKDOWN_RECLAIM':old_trades=tr
            else:sweep_trades=tr
            OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
        write_csv(OUTS['controls'],controls)
        write_csv(OUTS['coverage'],[dict(pair=PAIR,granularity='M15',side='BUY',
             first_candle=iso(m15[0]['time']),last_candle=iso(m15[-1]['time']),
             candles=len(m15),frozen_control_parity='PASS',
             research_rr=RR_FIXED,base_adverse_pips=1,stress_adverse_pips=2,
             expected_configs=160,orders_supported=False)])
        configs=make_configs()
        rowlist=[];cache={};costs=[]
        STATUS.update(state='conditional_grid',progress=29,message=f'Running {len(configs)} all-hours conditional geometries')
        for j,c in enumerate(configs):
            ix=np.flatnonzero(geometry_mask(c,f,raw)).tolist()
            trades=backtest(m15,ix,RR_FIXED,1.0)
            stress=backtest(m15,ix,RR_FIXED,2.0)
            row=summarise(c['config_id'],c['branch'],ix,trades,stress,c)
            rowlist.append(row)
            costs.append(dict(config_id=c['config_id'],cost_pips=1.0,**stats(trades)))
            costs.append(dict(config_id=c['config_id'],cost_pips=2.0,**stats(stress)))
            OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
            if j%8==7:
                STATUS.update(progress=29+int(47*(j+1)/len(configs)),message=f'Grid {j+1}/{len(configs)}')
                write_csv(OUTS['geometry'],rowlist)
                write_csv(OUTS['costs'],costs)
        assert len(rowlist)==160
        write_csv(OUTS['geometry'],rowlist)
        write_csv(OUTS['costs'],costs)
        STATUS.update(state='plateau',progress=77,message='Computing actual one-factor neighbour scores')
        near=local_neighbourhood(configs,rowlist)
        write_csv(OUTS['plateau'],near)
        selected=diagnostic_selection(rowlist,near)
        write_csv(OUTS['diagnostic'],selected)
        byid={x['config_id']:x for x in configs}
        rolling=[];calendar=[];attrib=[];sessions=[];ledgers=[];overlap=[]
        for k,d in enumerate(selected):
            c=byid[d['config_id']]
            ix=np.flatnonzero(geometry_mask(c,f,raw)).tolist()
            tx=backtest(m15,ix,RR_FIXED,1.0)
            # Write a complete independent trade ledger for analysis of
            # current-only vs old-only and overlapping entry times.
            ledgers.extend(add_ledger(c,tx))
            rolling.extend(rolling_diag(c['config_id'],tx))
            calendar.extend(calendar_diag(c['config_id'],tx))
            overlap.extend(overlap_controls(c,tx,old_trades,sweep_trades))
            sessions.extend(session_diagnostics(c,m15,f,raw,tx))
            attrib.extend(conditional_attribution(c,f,raw,m15,ix,tx))
            STATUS.update(state='attribution',progress=78+int(20*(k+1)/max(1,len(selected))),
                          message=f'Diagnostic candidate {k+1}/{len(selected)}: {c["config_id"]}')
            OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
        for key,rows in [('rolling',rolling),('calendar',calendar),('attribution',attrib),
                          ('sessions',sessions),('trades',ledgers),('signals',overlap)]:
            write_csv(OUTS[key],rows)
        write_csv(OUTS['notes'],[
          dict(item='scope',value='Two separate all-hours conditional entry hypotheses: SWEEP and FAILED; 160 fixed full-grid geometries'),
          dict(item='control',value='Old Sydney 55-trade summary parity required at frozen 2026-09-18 cutoff, NOT archived field-by-field parity'),
          dict(item='sweep',value='Previous-low LB x body ATR x prior 4H decline x absolute low penetration ATR; close above previous M15 high'),
          dict(item='failed',value='Previous-low LB x body ATR x close location; close reclaims the SAME selected lookback low, not the raw 10-bar low'),
          dict(item='no_filter',value='No session filter, EMA, weekday filter, RR tuning, wick filter or trigger union in grid'),
          dict(item='sessions',value='Session diagnostics AFTER all-hours grid on up to 3 examples/branch; not permission to choose the best hour'),
          dict(item='selection',value='Diagnostic gate predeclared and visible per grid row; top-three fallback per branch does not count as pass'),
          dict(item='cost',value='OANDA midpoint bars, 1-pip adverse fill baseline, 2-pip adverse fill stress; reference-risk targets, actual-fill R'),
          dict(item='execution',value='pyramiding zero replay starts exit search next M15; exit candle reentry eligible; both-hit approximation'),
          dict(item='lookahead',value='All structure calculations use previous-only extrema; ATR14 includes completed signal; prior 4H momentum uses completed M15s'),
          dict(item='rolling',value='Monthly-stepped 12/24/36m by entry timestamp; incomplete current year excluded from completed-years table'),
          dict(item='limits',value='Old years inspected repeatedly; no unsearched OOS, gate is only further-research permission'),
          dict(item='live',value='Portfolio27 remains unchanged; separate exact 27->28 chronological same-pair nonhedging study required'),
        ])
        write_bundle()
        STATUS.update(state='complete',progress=100,message='Stage 2 research complete',
                      configs=len(configs),diagnostic_rows=len(selected),
                      gate_pass=sum(bool(r['diagnostic_gate']) for r in rowlist),
                      old_control_parity='PASS',
                      results='/audusd-m15-long-stage2/results')
    except Exception as e:
        STATUS.update(state='error',message=str(e),traceback=traceback.format_exc(),
                      trading_enabled=False,orders_supported=False)
        try:write_bundle()
        except Exception:pass
        print(STATUS['traceback'],flush=True)


@app.route('/')
def index_route():
    return jsonify(service='AUD/USD M15 LONG Stage 2 conditional geometry research',
                   status='/audusd-m15-long-stage2/status',
                   results='/audusd-m15-long-stage2/results',
                   orders_supported=False,trading_enabled=False)

@app.route('/audusd-m15-long-stage2/status')
def status_route():return jsonify(STATUS)

@app.route('/audusd-m15-long-stage2/results')
def results_route():return download(BUNDLE)

if __name__=='__main__':
    threading.Thread(target=run_research,daemon=True).start()
    app.run(host='0.0.0.0',port=int(os.getenv('PORT','8080')),debug=False)
