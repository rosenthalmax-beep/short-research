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
# AUD/USD M15 LONG — CONTROLLED FOCUSED REFINEMENT (PROSPECTIVE #27)
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
    "coverage": "audusd27_refinement_coverage.csv",
    "parity": "audusd27_refinement_frozen_anchor_parity.csv",
    "anchor": "audusd27_refinement_anchor.csv",
    "geometry": "audusd27_refinement_geometry_matrix.csv",
    "session": "audusd27_refinement_session_neighbours.csv",
    "rr": "audusd27_refinement_rr_neighbours.csv",
    "one_factor": "audusd27_refinement_one_factor.csv",
    "sweep_controls": "audusd27_refinement_sweep_family_controls.csv",
    "geometry_plateau": "audusd27_refinement_geometry_plateau.csv",
    "deep": "audusd27_refinement_deep_comparison.csv",
    "periods": "audusd27_refinement_periods.csv",
    "cost": "audusd27_refinement_cost_stress.csv",
    "rolling": "audusd27_refinement_rolling.csv",
    "rolling_summary": "audusd27_refinement_rolling_summary.csv",
    "calendar": "audusd27_refinement_calendar.csv",
    "calendar_summary": "audusd27_refinement_calendar_summary.csv",
    "trades": "audusd27_refinement_top_trades.csv",
    "decision": "audusd27_refinement_decision.csv",
    "notes": "audusd27_refinement_notes.csv",
}
BUNDLE = "AUDUSD_M15_LONG_27_FOCUSED_REFINEMENT_RESULTS.zip"

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


# ============================================================
# ROBUSTNESS SCORING / STAGED SEARCH
# ============================================================

ERAS = [
    ("ERA_2002_07", START, datetime(2008, 1, 1, tzinfo=timezone.utc)),
    ("ERA_2008_13", datetime(2008, 1, 1, tzinfo=timezone.utc), datetime(2014, 1, 1, tzinfo=timezone.utc)),
    ("ERA_2014_19", datetime(2014, 1, 1, tzinfo=timezone.utc), datetime(2020, 1, 1, tzinfo=timezone.utc)),
    ("ERA_2020_NOW", datetime(2020, 1, 1, tzinfo=timezone.utc), NOW),
]


def config_fields(config):
    keys = [
        "br_min",
        "body_atr_min",
        "range_atr_min",
        "close_loc_min",
        "lower_wick_body_min",
        "structure_lb",
        "structure_dist_atr_max",
        "sweep_lb",
        "breakout_lb",
        "compression_max",
        "mom4_min",
    ]
    return {key: config.get(key) for key in keys}


def evaluate(config, candles, candidate_indices):
    full = stats(backtest(candles, candidate_indices, config["rr"], PRIMARY_COST, candles[0]["time"], NOW))
    early = stats(backtest(candles, candidate_indices, config["rr"], PRIMARY_COST, candles[0]["time"], datetime(2010, 1, 1, tzinfo=timezone.utc)))
    late = stats(backtest(candles, candidate_indices, config["rr"], PRIMARY_COST, datetime(2010, 1, 1, tzinfo=timezone.utc), NOW))

    era_pf = []
    era_r = []
    era_trades = []
    for _, a, b in ERAS:
        s = stats(backtest(candles, candidate_indices, config["rr"], PRIMARY_COST, a, b))
        era_pf.append(s["profit_factor"])
        era_r.append(s["total_r"])
        era_trades.append(s["trades"])

    positive_eras = sum(x > 0 for x in era_r)
    active_eras = sum(x > 0 for x in era_trades)
    min_active_era_pf = min(
        [era_pf[i] for i in range(4) if era_trades[i] > 0],
        default=0.0,
    )

    # Deliberately rewards breadth and temporal persistence more than max PF.
    score = (
        1.50 * min(full["profit_factor"], 3.0)
        + 0.75 * min(early["profit_factor"], 2.5)
        + 0.90 * min(late["profit_factor"], 2.5)
        + 0.40 * positive_eras
        + 0.15 * active_eras
        + 0.20 * min(max(min_active_era_pf, 0.0), 2.0)
        + 0.15 * min(full["trades"] / 100.0, 2.0)
        + 0.10 * min(max(full["expectancy_r"], 0.0), 1.0)
    )

    row = {
        "config_id": config["config_id"],
        "family": config["family"],
        "context": config.get("context", "NONE"),
        "rr": config["rr"],
        "full_trades": full["trades"],
        "full_pf": round(full["profit_factor"], 6),
        "full_r": round(full["total_r"], 4),
        "full_exp": round(full["expectancy_r"], 6),
        "full_dd": round(full["max_drawdown_r"], 4),
        "full_win_rate": round(full["win_rate"], 4),
        "pre2010_trades": early["trades"],
        "pre2010_pf": round(early["profit_factor"], 6),
        "pre2010_r": round(early["total_r"], 4),
        "post2010_trades": late["trades"],
        "post2010_pf": round(late["profit_factor"], 6),
        "post2010_r": round(late["total_r"], 4),
        "positive_eras": positive_eras,
        "active_eras": active_eras,
        "min_active_era_pf": round(min_active_era_pf, 6),
        "robust_score": round(score, 6),
    }

    for i in range(4):
        row[f"era{i + 1}_trades"] = era_trades[i]
        row[f"era{i + 1}_pf"] = round(era_pf[i], 6)
        row[f"era{i + 1}_r"] = round(era_r[i], 4)

    row.update(config_fields(config))
    return row


def sort_rows(rows):
    return sorted(
        rows,
        key=lambda r: (
            r["positive_eras"],
            r["pre2010_r"] > 0,
            r["post2010_r"] > 0,
            r["robust_score"],
            r["full_r"],
            r["full_trades"],
        ),
        reverse=True,
    )


def family_summary(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["family"]].append(row)

    out = []
    for family, sub in grouped.items():
        ranked = sort_rows(sub)
        best = ranked[0]
        out.append({
            "family": family,
            "configs": len(sub),
            "positive_full_configs": sum(r["full_r"] > 0 for r in sub),
            "positive_pre_and_post_configs": sum(
                r["pre2010_r"] > 0 and r["post2010_r"] > 0 for r in sub
            ),
            "four_positive_era_configs": sum(r["positive_eras"] == 4 for r in sub),
            "best_config_id": best["config_id"],
            "best_full_trades": best["full_trades"],
            "best_full_pf": best["full_pf"],
            "best_full_r": best["full_r"],
            "best_pre2010_r": best["pre2010_r"],
            "best_post2010_r": best["post2010_r"],
            "best_positive_eras": best["positive_eras"],
            "best_robust_score": best["robust_score"],
        })

    return sorted(
        out,
        key=lambda r: (
            r["four_positive_era_configs"],
            r["positive_pre_and_post_configs"],
            r["best_positive_eras"],
            r["best_robust_score"],
        ),
        reverse=True,
    )


def select_diverse(rows, per_family, total):
    """Take a minimum amount of family diversity, then fill globally."""
    ranked = sort_rows(rows)
    selected = []
    seen_ids = set()

    by_family = defaultdict(list)
    for row in ranked:
        by_family[row["family"]].append(row)

    for family in sorted(by_family):
        for row in by_family[family][:per_family]:
            if row["config_id"] not in seen_ids:
                selected.append(row)
                seen_ids.add(row["config_id"])

    for row in ranked:
        if len(selected) >= total:
            break
        if row["config_id"] not in seen_ids:
            selected.append(row)
            seen_ids.add(row["config_id"])

    return sort_rows(selected)[:total]


def stage2_configs(base_rows, config_by_id):
    out = []
    for rank, row in enumerate(base_rows):
        base = config_by_id[row["config_id"]]
        for context in CONTEXTS:
            x = deepcopy(base)
            x["config_id"] = f"S2_{rank}_{context}"
            x["context"] = context
            out.append(x)
    return out


def local_variants(base, rank):
    out = []

    for rr in [
        2.50, 2.75, 3.00, 3.25, 3.50, 3.75, 4.00,
        4.25, 4.50, 4.75, 5.00, 5.25, 5.50, 5.75, 6.00,
    ]:
        x = deepcopy(base)
        x["rr"] = rr
        x["config_id"] = f"S3_{rank}_RR_{rr:.2f}"
        out.append(x)

    bumps = {
        "br_min": [-0.15, -0.05, 0.05, 0.15],
        "body_atr_min": [-0.20, -0.10, 0.10, 0.20],
        "range_atr_min": [-0.20, -0.10, 0.10, 0.20],
        "close_loc_min": [-0.10, -0.05, 0.05, 0.10],
        "lower_wick_body_min": [-0.10, -0.05, 0.05, 0.10],
        "structure_dist_atr_max": [-0.05, -0.025, 0.025, 0.05],
        "compression_max": [-0.05, -0.025, 0.025, 0.05],
        "mom4_min": [-0.50, -0.25, 0.25, 0.50],
    }

    for field, deltas in bumps.items():
        value = base.get(field)
        if value is None:
            continue
        for delta in deltas:
            new_value = round(value + delta, 4)
            if field == "close_loc_min":
                if not (0.05 <= new_value <= 0.95):
                    continue
            elif field == "mom4_min":
                pass  # downward momentum thresholds are naturally negative
            elif new_value <= 0:
                continue
            x = deepcopy(base)
            x[field] = new_value
            x["config_id"] = f"S3_{rank}_{field}_{new_value}"
            out.append(x)

    lookbacks = [10, 20, 40, 60, 80, 100, 120, 165, 200]
    for field in ["structure_lb", "sweep_lb", "breakout_lb"]:
        value = base.get(field)
        if value not in lookbacks:
            continue
        p = lookbacks.index(value)
        for q in [p - 1, p + 1]:
            if 0 <= q < len(lookbacks):
                x = deepcopy(base)
                x[field] = lookbacks[q]
                x["config_id"] = f"S3_{rank}_{field}_{lookbacks[q]}"
                out.append(x)

    return out


def stage3_configs(base_rows, config_by_id):
    out = []
    seen = set()

    for rank, row in enumerate(base_rows):
        base = config_by_id[row["config_id"]]
        for x in local_variants(base, rank):
            signature = tuple(str(x.get(key)) for key in [
                "family",
                "br_min",
                "body_atr_min",
                "range_atr_min",
                "close_loc_min",
                "lower_wick_body_min",
                "structure_lb",
                "structure_dist_atr_max",
                "sweep_lb",
                "breakout_lb",
                "compression_max",
                "mom4_min",
                "context",
                "rr",
            ])
            if signature in seen:
                continue
            seen.add(signature)
            out.append(x)

    return out


# ============================================================
# FINAL SHORTLIST DIAGNOSTICS
# ============================================================

def stat_row(config, label, trades):
    s = stats(trades)
    return {
        "config_id": config["config_id"],
        "family": config["family"],
        "context": config.get("context", "NONE"),
        "rr": config["rr"],
        "period": label,
        **{
            key: round(value, 6) if isinstance(value, float) else value
            for key, value in s.items()
        },
    }


def period_rows(config, candles, candidate_indices):
    periods = [
        ("FULL", candles[0]["time"], NOW),
        ("PRE_2010", candles[0]["time"], datetime(2010, 1, 1, tzinfo=timezone.utc)),
        ("2010_PLUS", datetime(2010, 1, 1, tzinfo=timezone.utc), NOW),
        ("DEV_2002_17", candles[0]["time"], datetime(2018, 1, 1, tzinfo=timezone.utc)),
        ("VALIDATION_2018_PLUS", datetime(2018, 1, 1, tzinfo=timezone.utc), NOW),
        *ERAS,
        ("LAST_5Y", NOW - timedelta(days=365.2425 * 5), NOW),
        ("LAST_2Y", NOW - timedelta(days=365.2425 * 2), NOW),
        ("LAST_1Y", NOW - timedelta(days=365.2425), NOW),
    ]

    out = []
    for label, a, b in periods:
        row = stat_row(
            config,
            label,
            backtest(candles, candidate_indices, config["rr"], PRIMARY_COST, a, b),
        )
        row["start_utc"] = iso(a)
        row["end_utc"] = iso(b)
        out.append(row)
    return out


def cost_rows(config, candles, candidate_indices):
    out = []
    windows = [
        ("FULL", candles[0]["time"], NOW),
        ("VALIDATION_2018_PLUS", datetime(2018, 1, 1, tzinfo=timezone.utc), NOW),
        ("LAST_5Y", NOW - timedelta(days=365.2425 * 5), NOW),
        ("LAST_2Y", NOW - timedelta(days=365.2425 * 2), NOW),
    ]

    for cost in COSTS:
        for label, a, b in windows:
            row = stat_row(
                config,
                label,
                backtest(candles, candidate_indices, config["rr"], cost, a, b),
            )
            row["cost_pips"] = cost
            out.append(row)
    return out


def rolling_rows(config, candles, candidate_indices):
    out = []
    first = month_floor(max(candles[0]["time"], START))
    last = month_floor(NOW)

    for months in [12, 24, 36]:
        start = first
        while add_months(start, months) <= last:
            end = add_months(start, months)
            s = stats(backtest(
                candles,
                candidate_indices,
                config["rr"],
                PRIMARY_COST,
                start,
                end,
            ))
            out.append({
                "config_id": config["config_id"],
                "months": months,
                "start_utc": iso(start),
                "end_utc": iso(end),
                "trades": s["trades"],
                "profit_factor": round(s["profit_factor"], 6),
                "total_r": round(s["total_r"], 4),
                "positive": s["total_r"] > 0,
                "zero_trade": s["trades"] == 0,
            })
            start = add_months(start, 1)

    return out


def rolling_summary(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row["config_id"], row["months"])].append(row)

    out = []
    for (config_id, months), sub in grouped.items():
        active = [row for row in sub if row["trades"] > 0]
        out.append({
            "config_id": config_id,
            "months": months,
            "windows": len(sub),
            "active_windows": len(active),
            "zero_trade_windows": len(sub) - len(active),
            "positive_windows_pct": round(
                100.0 * sum(row["positive"] for row in sub) / len(sub),
                4,
            ) if sub else 0.0,
            "positive_active_windows_pct": round(
                100.0 * sum(row["positive"] for row in active) / len(active),
                4,
            ) if active else 0.0,
            "median_r_all": round(med([row["total_r"] for row in sub]), 4),
            "median_r_active": round(med([row["total_r"] for row in active]), 4),
            "median_pf_active": round(med([row["profit_factor"] for row in active]), 6),
            "worst_r": round(min((row["total_r"] for row in sub), default=0.0), 4),
            "best_r": round(max((row["total_r"] for row in sub), default=0.0), 4),
        })
    return out


def calendar_rows(config, candles, candidate_indices):
    out = []
    first_year = max(START.year, candles[0]["time"].year)

    for year in range(first_year, NOW.year):
        a = datetime(year, 1, 1, tzinfo=timezone.utc)
        b = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        s = stats(backtest(
            candles,
            candidate_indices,
            config["rr"],
            PRIMARY_COST,
            a,
            b,
        ))
        out.append({
            "config_id": config["config_id"],
            "year": year,
            "trades": s["trades"],
            "profit_factor": round(s["profit_factor"], 6),
            "total_r": round(s["total_r"], 4),
            "positive": s["total_r"] > 0,
            "negative": s["total_r"] < 0,
            "zero_trade": s["trades"] == 0,
        })
    return out


def calendar_summary(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["config_id"]].append(row)

    out = []
    for config_id, sub in grouped.items():
        active = [row for row in sub if row["trades"] > 0]
        out.append({
            "config_id": config_id,
            "completed_years": len(sub),
            "active_years": len(active),
            "positive_years": sum(row["positive"] for row in sub),
            "negative_years": sum(row["negative"] for row in sub),
            "zero_trade_years": sum(row["zero_trade"] for row in sub),
            "positive_years_pct": round(
                100.0 * sum(row["positive"] for row in sub) / len(sub),
                4,
            ) if sub else 0.0,
            "positive_active_years_pct": round(
                100.0 * sum(row["positive"] for row in active) / len(active),
                4,
            ) if active else 0.0,
            "median_trades_year": round(med([row["trades"] for row in sub]), 4),
            "median_year_r": round(med([row["total_r"] for row in sub]), 4),
            "worst_year_r": round(min((row["total_r"] for row in sub), default=0.0), 4),
            "best_year_r": round(max((row["total_r"] for row in sub), default=0.0), 4),
        })
    return out


def shortlist_summary(config, candles, candidate_indices):
    def window(a, b):
        return stats(backtest(candles, candidate_indices, config["rr"], PRIMARY_COST, a, b))

    full = window(candles[0]["time"], NOW)
    pre = window(candles[0]["time"], datetime(2010, 1, 1, tzinfo=timezone.utc))
    post = window(datetime(2010, 1, 1, tzinfo=timezone.utc), NOW)
    val = window(datetime(2018, 1, 1, tzinfo=timezone.utc), NOW)
    y20 = window(datetime(2020, 1, 1, tzinfo=timezone.utc), NOW)
    l5 = window(NOW - timedelta(days=365.2425 * 5), NOW)
    l2 = window(NOW - timedelta(days=365.2425 * 2), NOW)
    l1 = window(NOW - timedelta(days=365.2425), NOW)

    eras = [window(a, b) for _, a, b in ERAS]

    return {
        "config_id": config["config_id"],
        "family": config["family"],
        "context": config.get("context", "NONE"),
        "rr": config["rr"],
        **config_fields(config),
        "full_trades": full["trades"],
        "full_pf": round(full["profit_factor"], 6),
        "full_r": round(full["total_r"], 4),
        "full_exp": round(full["expectancy_r"], 6),
        "full_dd": round(full["max_drawdown_r"], 4),
        "full_win_rate": round(full["win_rate"], 4),
        "pre2010_pf": round(pre["profit_factor"], 6),
        "pre2010_r": round(pre["total_r"], 4),
        "post2010_pf": round(post["profit_factor"], 6),
        "post2010_r": round(post["total_r"], 4),
        "validation2018_plus_trades": val["trades"],
        "validation2018_plus_pf": round(val["profit_factor"], 6),
        "validation2018_plus_r": round(val["total_r"], 4),
        "era2020_plus_trades": y20["trades"],
        "era2020_plus_pf": round(y20["profit_factor"], 6),
        "era2020_plus_r": round(y20["total_r"], 4),
        "last5y_trades": l5["trades"],
        "last5y_pf": round(l5["profit_factor"], 6),
        "last5y_r": round(l5["total_r"], 4),
        "last2y_trades": l2["trades"],
        "last2y_pf": round(l2["profit_factor"], 6),
        "last2y_r": round(l2["total_r"], 4),
        "last1y_trades": l1["trades"],
        "last1y_pf": round(l1["profit_factor"], 6),
        "last1y_r": round(l1["total_r"], 4),
        "positive_eras": sum(x["trades"] > 0 and x["total_r"] > 0 for x in eras),
        "active_eras": sum(x["trades"] > 0 for x in eras),
        "min_active_era_pf": round(
            min([x["profit_factor"] for x in eras if x["trades"] > 0], default=0.0),
            6,
        ),
    }


def serialise_trade(config, trade):
    row = dict(trade)
    row.update({
        "config_id": config["config_id"],
        "family": config["family"],
        "context": config.get("context", "NONE"),
    })
    row.pop("entry_time", None)
    row.pop("exit_time", None)
    return row


def decision_rows(shortlist, costs, rollsum, calsum):
    cost_map = {
        (row["config_id"], row["period"], row["cost_pips"]): row
        for row in costs
    }
    roll_map = {
        (row["config_id"], row["months"]): row
        for row in rollsum
    }
    cal_map = {row["config_id"]: row for row in calsum}

    out = []
    for row in shortlist:
        cid = row["config_id"]
        c2_full = cost_map.get((cid, "FULL", 2.0), {})
        c2_val = cost_map.get((cid, "VALIDATION_2018_PLUS", 2.0), {})
        r12 = roll_map.get((cid, 12), {})
        r24 = roll_map.get((cid, 24), {})
        r36 = roll_map.get((cid, 36), {})
        cal = cal_map.get(cid, {})

        # Research gate only. PASS means "worth deeper validation", not live approval.
        checks = {
            "enough_trades": row["full_trades"] >= 50,
            "full_pf": row["full_pf"] >= 1.30,
            "full_positive": row["full_r"] > 0,
            "eras": row["positive_eras"] >= 3,
            "2018_positive": row["validation2018_plus_r"] > 0,
            "2020_positive": row["era2020_plus_r"] > 0,
            "last5_positive": row["last5y_r"] > 0,
            "2pip_full": c2_full.get("profit_factor", 0.0) >= 1.15,
            "2pip_2018": c2_val.get("total_r", 0.0) > 0,
            "rolling36": r36.get("positive_active_windows_pct", 0.0) >= 60.0,
        }
        passed = sum(bool(x) for x in checks.values())

        if all(checks.values()):
            verdict = "DEEP_VALIDATE"
        elif passed >= 8 and row["full_r"] > 0:
            verdict = "WATCH"
        else:
            verdict = "REJECT_OR_LOW_PRIORITY"

        out.append({
            "config_id": cid,
            "family": row["family"],
            "context": row["context"],
            "rr": row["rr"],
            "research_verdict": verdict,
            "checks_passed": passed,
            "checks_total": len(checks),
            **{f"check_{k}": v for k, v in checks.items()},
            "full_trades": row["full_trades"],
            "full_pf": row["full_pf"],
            "full_r": row["full_r"],
            "full_dd": row["full_dd"],
            "validation2018_plus_pf": row["validation2018_plus_pf"],
            "validation2018_plus_r": row["validation2018_plus_r"],
            "era2020_plus_pf": row["era2020_plus_pf"],
            "era2020_plus_r": row["era2020_plus_r"],
            "last5y_pf": row["last5y_pf"],
            "last5y_r": row["last5y_r"],
            "last2y_pf": row["last2y_pf"],
            "last2y_r": row["last2y_r"],
            "cost_2pip_full_pf": c2_full.get("profit_factor", 0.0),
            "cost_2pip_full_r": c2_full.get("total_r", 0.0),
            "cost_2pip_2018_pf": c2_val.get("profit_factor", 0.0),
            "cost_2pip_2018_r": c2_val.get("total_r", 0.0),
            "rolling12_positive_active_pct": r12.get("positive_active_windows_pct", 0.0),
            "rolling24_positive_active_pct": r24.get("positive_active_windows_pct", 0.0),
            "rolling36_positive_active_pct": r36.get("positive_active_windows_pct", 0.0),
            "rolling36_median_r": r36.get("median_r_active", 0.0),
            "rolling36_worst_r": r36.get("worst_r", 0.0),
            "active_calendar_years": cal.get("active_years", 0),
            "positive_active_years_pct": cal.get("positive_active_years_pct", 0.0),
            "zero_trade_years": cal.get("zero_trade_years", 0),
            "worst_calendar_year_r": cal.get("worst_year_r", 0.0),
        })

    order = {"DEEP_VALIDATE": 2, "WATCH": 1, "REJECT_OR_LOW_PRIORITY": 0}
    return sorted(
        out,
        key=lambda x: (
            order[x["research_verdict"]],
            x["checks_passed"],
            x["full_r"],
        ),
        reverse=True,
    )


# ============================================================
# MAIN RESEARCH RUNNER
# ============================================================


# ============================================================
# FOCUSED PREDECLARED REFINEMENT — NOT AN OPEN-ENDED OPTIMISER
# ============================================================
# Freeze broad-discovery anchor and mechanics. No HTF filter interactions.
# Anchor: FAILED_BREAKDOWN_RECLAIM, LB60, body1.00ATR, closeLoc0.65,
# Sydney signal OPEN hour04:00-07:59, RR3.50, 1pip adverse M15 fill.
# Only the geometry 3x3x4, one-factor hour/session neighbours, and
# separate RR neighbours are studied. Alternative LOW_SWEEP_DISPLACEMENT
# evidence is a CONTROL, not a mandatory complement or a deployed setup.
#
# Do NOT freeze from the best looking row: require neighbourhood/period
# robustness and a separate 26->27 portfolio conflict test.
# ============================================================

PARITY_LAST_M15_UTC = datetime(2026, 9, 18, 20, 45, tzinfo=timezone.utc)
PARITY_NOW = datetime(2026, 9, 19, 11, 55, tzinfo=timezone.utc)
PARITY_REFERENCE = {
    "trades": 55,
    "winners": 21,
    "full_pf": 1.955626,
    "full_r": 32.491274,
    "validation_trades": 13,
    "validation_pf": 1.398884,
    "validation_r": 3.589957,
    "last5_r": 2.339957,
    "last2_r": 1.146079,
    "cost2_pf": 1.781976,
    "cost2_r": 26.587184,
}
BASE_SESSION = "SYDNEY_BLOCK_04-07"
GEOMETRY_LB = (40, 60, 80)
GEOMETRY_BODY = (0.75, 1.00, 1.25)
GEOMETRY_CLOSE = (0.60, 0.65, 0.70, 0.75)
SESSION_WINDOWS = ((0,3),(2,5),(3,6),(4,7),(5,8),(6,9),(8,11))
RR_VALUES = (3.0,3.25,3.5,3.75,4.0)


def focus_anchor():
    return cfg(
        "ANCHOR_LB60_BODY100_CLOSE065_SYD04_07_RR350",
        "FAILED_BREAKDOWN_RECLAIM", rr=3.50,
        sweep_lb=60, body_atr_min=1.00,
        close_loc_min=0.65, context=BASE_SESSION,
    )


def focus_new(base, new_id, **updates):
    x = deepcopy(base)
    x.update(updates)
    x["config_id"] = new_id
    return x


def focus_indices(config, f):
    # Reuse EXACT discovery signal family and original session predicate.
    # Non-overlapping Sydney hour neighbour windows are a one-factor test.
    session = config.get("custom_sydney_window")
    if session is None:
        return indices(config, f)
    x = deepcopy(config)
    x["context"] = "NONE"
    raw = indices(x, f)
    a,b = session
    return [i for i in raw if a <= int(f["sydney_hour"][i]) <= b]


def focus_row(config, candles, f, experiment):
    ix = focus_indices(config, f)
    s = evaluate(config, candles, ix)
    s["experiment"] = experiment
    s["raw_signal_count"] = len(ix)
    s["session_hours"] = repr(config.get("custom_sydney_window", (4,7)))
    return s


def focus_full_stats(config, candles, f, start=None, end=None, cost=PRIMARY_COST):
    ix = focus_indices(config, f)
    tr = backtest(candles, ix, config["rr"], cost, start, end)
    return stats(tr)


def focus_parity(candles):
    """Recompute anchor on the exact LAST M15 CANDLE of the uploaded study.

    A later deployment may fetch newer candles; never silently compare a newer
    sample to an older reference. The frozen anchor is always independently
    rebuilt on the original truncated data, with no later candles allowed to
    affect ATR, exits, p0 or rolling endpoints.
    """
    anchor_bars = [b for b in candles if b["time"] <= PARITY_LAST_M15_UTC]
    if not anchor_bars or anchor_bars[-1]["time"] != PARITY_LAST_M15_UTC:
        raise RuntimeError("Frozen parity final M15 candle missing; cannot establish discovery parity")
    n = len(anchor_bars)
    # Feature function needs HTF context arrays, but the anchor's only context
    # is local Sydney time, so supply NaN placeholders. HTF must remain unused.
    absent = {k:np.full(n,np.nan) for k in
              ["close","ema50","ema100","ema200","atr_ratio50"]}
    f = features(anchor_bars, absent, absent, absent)
    c = focus_anchor()
    ix = focus_indices(c, f)
    tr = backtest(anchor_bars, ix, c["rr"], PRIMARY_COST)
    whole = stats(tr)
    validation = stats([t for t in tr if t["entry_time"] >= datetime(2018,1,1,tzinfo=timezone.utc)])
    recent5 = stats([t for t in tr if t["entry_time"] >= PARITY_NOW - timedelta(days=365.2425*5)])
    recent2 = stats([t for t in tr if t["entry_time"] >= PARITY_NOW - timedelta(days=365.2425*2)])
    cost2 = stats(backtest(anchor_bars, ix, c["rr"], 2.0))
    observed = {
        "trades":whole["trades"],"winners":whole["winners"],
        "full_pf":whole["profit_factor"],"full_r":whole["total_r"],
        "validation_trades":validation["trades"],
        "validation_pf":validation["profit_factor"],
        "validation_r":validation["total_r"],
        "last5_r":recent5["total_r"], "last2_r":recent2["total_r"],
        "cost2_pf":cost2["profit_factor"],"cost2_r":cost2["total_r"],
    }
    rows=[]
    for key,reference in PARITY_REFERENCE.items():
        current=observed[key]
        passed=(current==reference if isinstance(reference,int)
                else abs(current-reference) <= 0.00002)
        rows.append({"metric":key,"discovery_reference":reference,
                     "refinement_recomputed":current,"passed":passed})
    rows.append({"metric":"frozen_last_candle_utc", "discovery_reference":iso(PARITY_LAST_M15_UTC),
                 "refinement_recomputed":iso(anchor_bars[-1]["time"]), "passed":True})
    rows.append({"metric":"frozen_m15_bar_count", "discovery_reference":546849,
                 "refinement_recomputed":n,"passed":n==546849})
    OUTCOME_CACHE.clear(); BACKTEST_CACHE.clear()
    if not all(r["passed"] for r in rows):
        write_csv(OUTS["parity"],rows)
        raise RuntimeError("Frozen broad-discovery parity FAILED; see parity CSV; stop refinement")
    write_csv(OUTS["parity"],rows)
    return rows


def focus_experiments(anchor):
    geo = [focus_new(anchor, f"G_LB{lb}_B{b:.2f}_CL{cl:.2f}",
                     sweep_lb=lb,body_atr_min=b,close_loc_min=cl)
           for lb in GEOMETRY_LB for b in GEOMETRY_BODY for cl in GEOMETRY_CLOSE]
    session = [focus_new(anchor, f"S_SYD_{a:02d}_{b:02d}",
                         context="NONE", custom_sydney_window=(a,b))
               for a,b in SESSION_WINDOWS]
    session.append(focus_new(anchor,"S_ALL_HOURS",context="NONE"))
    rr = [focus_new(anchor, f"RR_{value:.2f}",rr=value) for value in RR_VALUES]
    # One parameter at a time away from the anchor. No compounding selected
    # gains across factors; retain weaker/failing neighbours visibly.
    one = []
    for lb in (20,40,60,80,100):
        one.append(focus_new(anchor,f"OF_LB{lb}",sweep_lb=lb))
    for b in (0.50,0.75,1.00,1.25):
        one.append(focus_new(anchor,f"OF_BODY{b:.2f}",body_atr_min=b))
    for cl in (0.55,0.60,0.65,0.70,0.75):
        one.append(focus_new(anchor,f"OF_CLOSE{cl:.2f}",close_loc_min=cl))
    # Only retain known, independently surfaced low-sweep setups to avoid
    # silently resampling hundreds of fresh control parameter combinations.
    sweep = [
        cfg("CONTROL_SWEEP_SYD_16_19", "LOW_SWEEP_DISPLACEMENT",rr=3.50,
            sweep_lb=60,body_atr_min=1.25,lower_wick_body_min=0.25,
            mom4_min=-1.50,context="SYDNEY_BLOCK_16-19"),
        cfg("CONTROL_SWEEP_NY_20_23", "LOW_SWEEP_DISPLACEMENT",rr=3.50,
            sweep_lb=60,body_atr_min=1.00,lower_wick_body_min=0.35,
            mom4_min=-1.00,context="NY_BLOCK_20-23"),
    ]
    return geo, session, rr, one, sweep


def focus_deep_summary(c, candles, f):
    ix=focus_indices(c,f)
    a=shortlist_summary(c,candles,ix)
    roll=rolling_rows(c,candles,ix)
    cal=calendar_rows(c,candles,ix)
    return a, roll, cal


def focus_checks(s,cost2,rolling,calendar):
    """Predeclared research gates; a passing row is NOT live approval.
    Counts and temporal returns matter more than rare high-PF samples.
    All metrics are historical and multiply searched, not untouched OOS.
    """
    rs = {int(r["months"]):r for r in rolling}
    cals = calendar
    checks={
        "trades_ge_70":s["full_trades"]>=70,
        "full_pf_ge_1_30":s["full_pf"]>=1.30,
        "both_pre2010_post2010_positive":s["pre2010_r"]>0 and s["post2010_r"]>0,
        "validation_trades_ge_20":s["validation2018_plus_trades"]>=20,
        "validation_pf_ge_1_20":s["validation2018_plus_pf"]>=1.20,
        "validation_r_positive":s["validation2018_plus_r"]>0,
        "eras_at_least_3_positive":s["positive_eras"]>=3,
        "last5_trades_ge_12":s["last5y_trades"]>=12,
        "last5_r_positive":s["last5y_r"]>0,
        "last2_trades_ge_5":s["last2y_trades"]>=5,
        "last2_r_positive":s["last2y_r"]>0,
        "2pip_pf_ge_1_25":cost2["profit_factor"]>=1.25,
        "2pip_total_r_positive":cost2["total_r"]>0,
        "rolling24_positive_ge_80":rs[24]["positive_active_windows_pct"]>=80,
        "rolling36_positive_ge_90":rs[36]["positive_active_windows_pct"]>=90,
        "calendar_positive_active_ge_60":cals["positive_active_years_pct"]>=60,
        "zero_trade_completed_years_le_3":cals["zero_trade_years"]<=3,
    }
    return checks


def run_focused_refinement():
    try:
        STATUS.update(state="fetch",message="Fetching AUD/USD M15 only; no HTF filters in this controlled pass",progress=1)
        # M15 only: even the sweep comparator uses M15 local momentum; no
        # unneeded H1/H4/D fetch/EMA warm-up can change the frozen predicates.
        candles=fetch("M15",START,NOW,35)
        if len(candles)<500_000:
            raise RuntimeError(f"Insufficient AUD/USD M15 history: {len(candles)}")
        write_csv(OUTS["coverage"],[{"pair":PAIR,"timeframe":"M15","first_utc":iso(candles[0]["time"]),
           "last_utc":iso(candles[-1]["time"]),"bars":len(candles),"historical_adverse_fill_pips":PRIMARY_COST,
           "stop_buffer_ticks":STOP_TICKS,"read_only":True}])
        STATUS.update(state="parity",message="Rebuilding the EXACT 55-trade frozen discovery anchor",progress=21)
        focus_parity(candles)
        n=len(candles)
        absent={k:np.full(n,np.nan) for k in ("close","ema50","ema100","ema200","atr_ratio50")}
        STATUS.update(state="features",message="Building M15 ATR14, prior lows and Sydney DST-aware hour cache",progress=30)
        f=features(candles,absent,absent,absent)
        anchor=focus_anchor()
        geo,session,rr,one,sweep=focus_experiments(anchor)
        groups=[("geometry",geo),("session",session),("rr",rr),("one_factor",one),("sweep_controls",sweep)]
        all_rows={}
        cfg_by_id={anchor["config_id"]:anchor}
        anchor_row=focus_row(anchor,candles,f,"FROZEN_ANCHOR")
        write_csv(OUTS["anchor"],[anchor_row])
        cfg_by_id.update({c["config_id"]:c for _,configs in groups for c in configs})
        for k,(label,configs) in enumerate(groups):
            STATUS.update(state="refine",message=f"Controlled {label} {len(configs)} configurations",progress=35+8*k)
            rows=[focus_row(c,candles,f,label) for c in configs]
            all_rows[label]=rows
            write_csv(OUTS[label],rows)
            OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()

        # Inspect the ENTIRE central 3x3 x close-location neighbourhood,
        # not only the single highest-scoring parameter point.
        plateau=[]
        for close_loc in GEOMETRY_CLOSE:
            sub=[r for r in all_rows["geometry"] if r["close_loc_min"]==close_loc]
            plateau.append({"close_loc_min":close_loc,"configs":len(sub),
                 "full_profitable":sum(r["full_r"]>0 for r in sub),
                 "both_pre2010_post2010_profitable":sum(r["pre2010_r"]>0 and r["post2010_r"]>0 for r in sub),
                 "positive_validation2018":sum(r["era4_r"]>0 for r in sub),
                 "median_trades":med([r["full_trades"] for r in sub]),
                 "median_full_pf":med([r["full_pf"] for r in sub]),
                 "median_full_r":med([r["full_r"] for r in sub])})
        write_csv(OUTS["geometry_plateau"],plateau)

        # Rank by quality AND actual frequency/validation, not rare PF alone.
        # Deep-diagnose anchored original + up to 8 diverse eligible geometry
        # points + 2 session + 2 RR + known sweep controls. Always retain
        # anchor and weak neighbours; nothing is automatically frozen.
        STATUS.update(state="deep",message="Focused shortlist: full ledger, costs, rolling and years",progress=78)
        candidate_rows=[r for r in all_rows["geometry"] if
                        r["full_trades"]>=45 and r["pre2010_r"]>0 and r["post2010_r"]>0]
        candidate_rows.sort(key=lambda r:(r["positive_eras"],r["full_trades"]>=70,
                            r["full_pf"]>=1.3,r["full_r"],r["full_trades"]),reverse=True)
        shortlist_ids=[anchor["config_id"]]
        picked_lb=set();picked_close=set()
        for r in candidate_rows:
            if len(shortlist_ids)>=9:break
            if r["config_id"] in shortlist_ids:continue
            if len(shortlist_ids)<=4 or r["sweep_lb"] not in picked_lb or r["close_loc_min"] not in picked_close:
                shortlist_ids.append(r["config_id"])
                picked_lb.add(r["sweep_lb"]);picked_close.add(r["close_loc_min"])
        for label,keep in (("session",2),("rr",2)):
            a=sorted(all_rows[label],key=lambda r:(r["full_trades"]>=45,r["positive_eras"],
                            r["pre2010_r"]>0 and r["post2010_r"]>0,r["full_r"]),reverse=True)
            shortlist_ids.extend(r["config_id"] for r in a[:keep] if r["config_id"] not in shortlist_ids)
        shortlist_ids.extend(c["config_id"] for c in sweep)
        shortlist_ids=list(dict.fromkeys(shortlist_ids))

        deep=[];period=[];cost=[];rolling=[];calendar=[];trades=[];decisions=[]
        for pos,identifier in enumerate(shortlist_ids):
            STATUS.update(state="deep",message=f"Deep diagnostic {pos+1}/{len(shortlist_ids)} {identifier}",
                          progress=79+int(17*pos/max(1,len(shortlist_ids))))
            c=cfg_by_id[identifier];ix=focus_indices(c,f)
            s=shortlist_summary(c,candles,ix)
            deep.append(s)
            period.extend(period_rows(c,candles,ix))
            cost.extend(cost_rows(c,candles,ix))
            rrows=rolling_rows(c,candles,ix)
            crows=calendar_rows(c,candles,ix)
            rolling.extend(rrows);calendar.extend(crows)
            rs={int(r["months"]):r for r in rolling_summary(rrows)}
            cs=calendar_summary(crows)[0]
            cost2=stats(backtest(candles,ix,c["rr"],2.0))
            checks=focus_checks(s,cost2,rs.values(),cs)
            decisions.append({"config_id":identifier,"family":c["family"],"experiment":
                ("anchor" if identifier==anchor["config_id"] else next(label for label,rows in all_rows.items()
                 if any(r["config_id"]==identifier for r in rows))),
                "gate_pass":all(checks.values()),"checks_passed":sum(checks.values()),"checks_total":len(checks),
                "checks_json":str(checks),"full_trades":s["full_trades"],"full_pf":s["full_pf"],"full_r":s["full_r"],
                "validation_trades":s["validation2018_plus_trades"],"validation_pf":s["validation2018_plus_pf"],
                "validation_r":s["validation2018_plus_r"],"last5_trades":s["last5y_trades"],
                "last5_r":s["last5y_r"],"last2_trades":s["last2y_trades"],"last2_r":s["last2y_r"],
                "cost2_pf":cost2["profit_factor"],"cost2_r":cost2["total_r"],
                "rolling12_active_pct":rs[12]["positive_active_windows_pct"],
                "rolling24_active_pct":rs[24]["positive_active_windows_pct"],
                "rolling36_active_pct":rs[36]["positive_active_windows_pct"],
                "worst24_r":rs[24]["worst_r"],"worst36_r":rs[36]["worst_r"],
                "calendar_positive_pct":cs["positive_active_years_pct"],
                "zero_trade_completed_years":cs["zero_trade_years"],
                "research_next_step":("FINAL_FROZEN_CONFIRMATION_REQUIRED" if all(checks.values())
                   else "NOT_READY_FOR_PORTFOLIO_OR_LIVE")})
            for t in backtest(candles,ix,c["rr"],PRIMARY_COST):trades.append(serialise_trade(c,t))
            OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
        write_csv(OUTS["deep"],deep)
        write_csv(OUTS["periods"],period)
        write_csv(OUTS["cost"],cost)
        write_csv(OUTS["rolling"],rolling)
        write_csv(OUTS["rolling_summary"],rolling_summary(rolling))
        write_csv(OUTS["calendar"],calendar)
        write_csv(OUTS["calendar_summary"],calendar_summary(calendar))
        write_csv(OUTS["trades"],trades)
        write_csv(OUTS["decision"],decisions)
        write_csv(OUTS["notes"],[
            {"item":"source","value":"Original AUDUSD_M15_LONG_27_BROAD_DISCOVERY.py historical features, exact signals, outcome, half-open strategy p0, and 1pip model are reused verbatim."},
            {"item":"frozen_reference","value":"Original 546849 M15 bars through 2026-09-18T20:45Z: 55 trades, 21 wins, PF1.955626, +32.491274R, 2018+13 trades +3.589957R."},
            {"item":"geometry","value":"LB40/60/80 x body0.75/1.00/1.25 x closeLoc0.60/0.65/0.70/0.75, Sydney04-07 RR3.50; 36 cells."},
            {"item":"sessions","value":"Single-factor Sydney 4h windows; no combined/session-interaction filters; DST-aware signal candle OPEN. Unfiltered same geometry shown as control."},
            {"item":"rr","value":"Frozen geometry/session; separate 3.00/3.25/3.50/3.75/4.00 RR neighbours."},
            {"item":"control","value":"Low-sweep family is separate comparator, NOT automatically joined with failed breakdown."},
            {"item":"honesty","value":"Previously searched historical periods are not pristine OOS. Passing screening checks means final confirmation only, NOT live approval."},
            {"item":"safety","value":"READ ONLY; no executor/OANDA order post; existing 26 live unchanged; 26->27 exact nonhedging portfolio-add test required after frozen standalone confirmation."},
        ])
        STATUS.update(state="packaging",message="Packaging controlled refinement ZIP",progress=98)
        package_results()
        STATUS.update(state="complete",message="Focused AUD/USD M15 LONG refinement complete",progress=100,
                      anchor_parity="PASS",geometry_configs=len(geo),session_configs=len(session),
                      rr_configs=len(rr),one_factor_configs=len(one),deep_configs=len(deep),
                      passing_deep=sum(r["gate_pass"] for r in decisions),bundle=BUNDLE)
    except Exception as ex:
        import traceback
        STATUS.update(state="error",message=str(ex),traceback=traceback.format_exc())
        print("AUDUSD M15 LONG #27 FOCUSED REFINEMENT ERROR",repr(ex),flush=True)


# ============================================================
# AUD/USD M15 LONG #27 — FINAL FROZEN-CORE COMPLEMENT SEARCH
# ============================================================
# The embedded original research functions are retained verbatim. The old
# focused-refinement runner is not launched; this study has its own routes.
# Nothing in this file submits or enables real orders. 26 live strategies
# remain unchanged. No portfolio-add test is claimed or performed here.
#
# CORE (UNCHANGED): failed breakdown/reclaim, previous60 low, bullish,
# body>=1 ATR14, close-location>=.65, Sydney open 04:00–07:59, RR3.5.
# M15 long fill reference close+1pip, stop low-10 ticks, reference RR target,
# same-bar exit tie convention and exit-bar re-entry all inherited unchanged.
#
# Complement search is finite/predeclared (12 broad no-session geometries,
# plus 2 previously surfaced tiny-sample sweep comparators). No searching
# specific bad calendar years, micro-hour windows or multi-filter cocktails.
# Every combo is shown, including failures. All have RR3.5. Same-candle
# CORE priority, strategy-level p0 on raw-signal union, cost-stress reruns
# whole union, never combine two separately backtested trade ledgers.
#
# Historical data have been repeatedly inspected: 2018+ is a validation
# segment, NOT pristine out-of-sample; results cannot establish future edge.
# ============================================================

import json

# Override only the output destination names used by shared helpers.
OUTS = {
    "coverage": "audusd27_complement_coverage.csv",
    "parity": "audusd27_complement_frozen_core_parity.csv",
    "core": "audusd27_complement_frozen_core.csv",
    "core_trades": "audusd27_complement_core_trades.csv",
    "raw": "audusd27_complement_predeclared_raw_matrix.csv",
    "matrix": "audusd27_complement_union_matrix.csv",
    "family": "audusd27_complement_family_summary.csv",
    "periods": "audusd27_complement_periods.csv",
    "rolling": "audusd27_complement_rolling.csv",
    "rolling_summary": "audusd27_complement_rolling_summary.csv",
    "calendar": "audusd27_complement_calendar.csv",
    "calendar_summary": "audusd27_complement_calendar_summary.csv",
    "stress": "audusd27_complement_cost_stress.csv",
    "overlap": "audusd27_complement_overlap_diagnostics.csv",
    "trades": "audusd27_complement_finalist_trades.csv",
    "decision": "audusd27_complement_decision.csv",
    "notes": "audusd27_complement_notes.csv",
}
BUNDLE = "AUDUSD_M15_LONG_27_FINAL_COMPLEMENT_RESULTS.zip"
STATUS = {"state":"not_started", "message":"Read-only study not started",
          "orders_supported":False, "trading_enabled":False}

CORE_ID = "FROZEN_FAILED_BREAKDOWN_LB60_BODY100_CL065_SYD04_07_RR350"
COMPLEMENT_RR = 3.50
# Exactly 12 fixed, structurally distinct geometries across 3 families.
# These are interpretations of price action, NOT optimised derivatives
# of the failed-breakdown core. No session or weekday additions.
COMPLEMENTS = (
    # A. Lower-low displacement beyond previous HIGH (different completion
    #    condition from core reclaim of the old LOW), after ~4h decline.
    ("SWEEP_BROAD_40", "LOW_SWEEP", dict(lb=40, body=.75, wick=.20, mom=-.50)),
    ("SWEEP_BROAD_60", "LOW_SWEEP", dict(lb=60, body=.75, wick=.20, mom=-.50)),
    ("SWEEP_STRONG_60", "LOW_SWEEP", dict(lb=60, body=1.00, wick=.20, mom=-1.00)),
    ("SWEEP_STRONG_80", "LOW_SWEEP", dict(lb=80, body=1.00, wick=.35, mom=-1.00)),
    # B. Prior 4h decline followed by bullish outside reversal. No 60-bar
    #    breakout/reclaim or session filter.
    ("OUTSIDE_050_BODY075", "DECLINE_OUTSIDE", dict(mom=-.50, body=.75, cl=.70)),
    ("OUTSIDE_100_BODY075", "DECLINE_OUTSIDE", dict(mom=-1.00, body=.75, cl=.70)),
    ("OUTSIDE_050_BODY100", "DECLINE_OUTSIDE", dict(mom=-.50, body=1.00, cl=.75)),
    ("OUTSIDE_100_BODY100", "DECLINE_OUTSIDE", dict(mom=-1.00, body=1.00, cl=.75)),
    # C. Expansion out of prior compression to new 10/20-bar HIGH.
    #    This is not a false low-break reclaim like the frozen core.
    ("COMPRESSION_10_LOOSE", "COMPRESSION_UP", dict(lb=10, body=.75, ran=1.25, comp=.85)),
    ("COMPRESSION_20_LOOSE", "COMPRESSION_UP", dict(lb=20, body=.75, ran=1.25, comp=.85)),
    ("COMPRESSION_10_STRONG", "COMPRESSION_UP", dict(lb=10, body=1.00, ran=1.50, comp=.80)),
    ("COMPRESSION_20_STRONG", "COMPRESSION_UP", dict(lb=20, body=1.00, ran=1.50, comp=.80)),
    # Previously surfaced selected controls. These are NOT independent
    # confirmation and may NOT pass if the broad family has no support.
    ("KNOWN_SWEEP_SYD_16_19", "KNOWN_SWEEP_CONTROL", dict(lb=60,body=1.25,wick=.25,mom=-1.50,zone="SYDNEY",hours=(16,19))),
    ("KNOWN_SWEEP_NY_20_23", "KNOWN_SWEEP_CONTROL", dict(lb=60,body=1.00,wick=.35,mom=-1.00,zone="NY",hours=(20,23))),
)

DEV_END = datetime(2018,1,1,tzinfo=timezone.utc)
ONE_YEAR = timedelta(days=365.2425)
FINALIST_LIMIT = 5


def comp_raw_indices(f, family, params):
    """All vector predicates are fixed here; M15 signal open defines time."""
    mask = f["valid_atr"].copy() & f["bullish"]
    if family in ("LOW_SWEEP", "KNOWN_SWEEP_CONTROL"):
        previous_high = np.r_[np.nan, f["high"][:-1]]
        mask &= f["low"] < f["prev_low"][params["lb"]]
        mask &= f["close"] > previous_high
        mask &= f["body_atr"] >= params["body"]
        mask &= f["lower_wick_body"] >= params["wick"]
        mask &= f["mom4"] <= params["mom"]
        if family == "KNOWN_SWEEP_CONTROL":
            hours = f["sydney_hour"] if params["zone"] == "SYDNEY" else f["ny_hour"]
            a,b = params["hours"]
            mask &= (hours >= a) & (hours <= b)
    elif family == "DECLINE_OUTSIDE":
        previous_high = np.r_[np.nan, f["high"][:-1]]
        previous_low = np.r_[np.nan, f["low"][:-1]]
        mask &= f["low"] < previous_low
        mask &= f["high"] > previous_high
        mask &= f["body_atr"] >= params["body"]
        mask &= f["close_loc"] >= params["cl"]
        mask &= f["mom4"] <= params["mom"]
    elif family == "COMPRESSION_UP":
        mask &= f["compression"] <= params["comp"]
        mask &= f["body_atr"] >= params["body"]
        mask &= f["range_atr"] >= params["ran"]
        mask &= f["close"] > f["prev_high"][params["lb"]]
    else:
        raise ValueError("Unrecognised predeclared family " + family)
    mask[:200] = False
    return np.flatnonzero(mask).tolist()


def comp_union(candles, core_indices, comp_indices, cost_pips=PRIMARY_COST):
    """Exact signal-union rebacktest: core priority, one p0 across both.

    Block signals while [signal_index, exit_index) holds; if next signal
    is ON the exit candle it is eligible (same as frozen original runner).
    No merging of independently pyramided ledgers.
    """
    core_set = set(core_indices)
    comp_set = set(comp_indices)
    if core_indices != sorted(set(core_indices)) or comp_indices != sorted(set(comp_indices)):
        raise ValueError("Raw indices must be strictly sorted and unique")
    raw_union = sorted(core_set | comp_set)
    trades = []
    rejected_core = rejected_comp = 0
    overlap_until = -1
    for i in raw_union:
        src = "CORE" if i in core_set else "COMPLEMENT"
        if i < overlap_until:
            if i in core_set: rejected_core += 1
            else: rejected_comp += 1
            continue
        trade = outcome(candles, i, COMPLEMENT_RR, cost_pips)
        if trade is None: continue
        t = dict(trade)
        t["trigger_id"] = src
        t["raw_both_same_candle"] = i in core_set and i in comp_set
        trades.append(t)
        overlap_until = int(t["exit_index"])
    return trades, {
        "core_raw":len(core_indices), "complement_raw":len(comp_indices),
        "same_candle_both":len(core_set & comp_set),
        "union_unique_raw":len(raw_union),
        "accepted_core":sum(t["trigger_id"]=="CORE" for t in trades),
        "accepted_complement":sum(t["trigger_id"]=="COMPLEMENT" for t in trades),
        "rejected_core_overlap":rejected_core,
        "rejected_complement_overlap":rejected_comp,
        "accepted":len(trades),
    }


def comp_window_stats(trades, start=None, end=None):
    return stats([t for t in trades if
                 (start is None or start<=t["entry_time"]) and
                 (end is None or t["entry_time"]<end)])


def comp_periods(identifier, trades):
    out=[]
    windows=(("FULL",START,NOW),
             ("DEVELOPMENT_BEFORE_2018",START,DEV_END),
             ("VALIDATION_2018_PLUS",DEV_END,NOW),
             ("LAST5Y",NOW-5*ONE_YEAR,NOW),
             ("LAST3Y",NOW-3*ONE_YEAR,NOW),
             ("LAST2Y",NOW-2*ONE_YEAR,NOW),
             ("LAST1Y",NOW-ONE_YEAR,NOW),
             *ERAS)
    for label,a,b in windows:
        out.append({"config_id":identifier,"period":label,**comp_window_stats(trades,a,b)})
    return out


def comp_rolling(identifier, trades):
    # Evaluates full chronological accepted ledger. The windows are slices
    # AFTER p0; never reset p0 at a new month/period boundary.
    first=month_floor(START)
    last=month_floor(NOW)
    out=[]
    ts=[t["entry_time"] for t in trades]
    # The trades are ordered by entry (signal open), not by close date.
    # prefix sums make rolling windows O(log n) and do not round away R.
    pref=[0.0]
    for t in trades: pref.append(pref[-1]+float(t["result_r"]))
    for months in (12,24,36):
        start=first
        while add_months(start,months)<=last:
            end=add_months(start,months)
            left=bisect.bisect_left(ts,start)
            right=bisect.bisect_left(ts,end)
            value=pref[right]-pref[left]
            out.append({"config_id":identifier,"months":months,
                "start_utc":iso(start),"end_utc":iso(end),"trades":right-left,
                "total_r":value,"positive":value>0,"zero_trade":right==left})
            start=add_months(start,1)
    return out


def comp_roll_summary(rows):
    grouped=defaultdict(list)
    for r in rows:grouped[(r["config_id"],int(r["months"]))].append(r)
    out=[]
    for (cid,months),sub in grouped.items():
        active=[r for r in sub if r["trades"]>0]
        out.append({"config_id":cid,"months":months,"windows":len(sub),
            "active_windows":len(active),"zero_trade_windows":len(sub)-len(active),
            "positive_windows_pct":100*sum(r["positive"] for r in sub)/len(sub),
            "positive_active_windows_pct":100*sum(r["positive"] for r in active)/len(active) if active else 0.0,
            "median_r_active":med([r["total_r"] for r in active]),
            "worst_r":min((r["total_r"] for r in sub),default=0.0)})
    return out


def comp_calendar(identifier, trades):
    out=[]
    for year in range(max(START.year,2002),NOW.year):
        a=datetime(year,1,1,tzinfo=timezone.utc)
        b=datetime(year+1,1,1,tzinfo=timezone.utc)
        s=comp_window_stats(trades,a,b)
        out.append({"config_id":identifier,"year":year,
                    "positive":s["total_r"]>0,"zero_trade":s["trades"]==0,**s})
    return out


def comp_cal_summary(rows):
    grouped=defaultdict(list)
    for row in rows:grouped[row["config_id"]].append(row)
    out=[]
    for cid,sub in grouped.items():
        active=[r for r in sub if r["trades"]>0]
        out.append({"config_id":cid,"completed_years":len(sub),
            "active_completed_years":len(active),
            "positive_years":sum(r["positive"] for r in active),
            "zero_trade_years":len(sub)-len(active),
            "positive_active_years_pct":100*sum(r["positive"] for r in active)/len(active) if active else 0.0,
            "worst_year_r":min((r["total_r"] for r in sub),default=0.0)})
    return out


def comp_assess(identifier, trades, core_trades, rsummary, csummary,
                psummary, cost2, family_support):
    full=stats(trades)
    prev=stats(core_trades)
    ps={r["period"]:r for r in psummary}
    rs={int(r["months"]):r for r in rsummary}
    # Criteria predeclared here, NOT tuned to achieve a selected PASS.
    # The core is small: require genuine frequency growth and rolling gain,
    # without giving up long-term / modern profitability or 2pip tolerance.
    checks={
       "trades_ge_85":full["trades"]>=85,
       "increment_accepted_ge_30":full["trades"]-prev["trades"]>=30,
       "full_pf_ge_1_30":full["profit_factor"]>=1.30,
       "total_r_exceeds_core":full["total_r"]>prev["total_r"],
       "max_dd_no_worse_than_core_plus_3r":full["max_drawdown_r"]>=prev["max_drawdown_r"]-3.0,
       "dev_positive":ps["DEVELOPMENT_BEFORE_2018"]["total_r"]>0,
       "validation_trades_ge_20":ps["VALIDATION_2018_PLUS"]["trades"]>=20,
       "validation_pf_ge_1_20":ps["VALIDATION_2018_PLUS"]["profit_factor"]>=1.20,
       "validation_positive":ps["VALIDATION_2018_PLUS"]["total_r"]>0,
       "last5_positive":ps["LAST5Y"]["total_r"]>0,
       "last2_positive":ps["LAST2Y"]["total_r"]>0,
       "double_cost_marginal_positive":family_support["cost2_marginal_r"]>0,
       "rolling24_worst_not_worse":rs[24]["worst_r"]>=family_support["core_worst24"],
       "rolling36_worst_not_worse":rs[36]["worst_r"]>=family_support["core_worst36"],
       "rolling24_positive_ge_80":rs[24]["positive_active_windows_pct"]>=80,
       "rolling36_positive_ge_90":rs[36]["positive_active_windows_pct"]>=90,
       "rolling24_vs_core_improves":rs[24]["positive_active_windows_pct"]>family_support["core_roll24"],
       "rolling36_vs_core_improves":rs[36]["positive_active_windows_pct"]>family_support["core_roll36"],
       "positive_years_ge_60":csummary["positive_active_years_pct"]>=60,
       "zero_trade_years_le_3":csummary["zero_trade_years"]<=3,
       "double_cost_pf_ge_1_25":cost2["profit_factor"]>=1.25,
       "double_cost_r_positive":cost2["total_r"]>0,
       "broad_family_supported":family_support["broad_family_supported"],
    }
    passed=all(checks.values())
    return {"config_id":identifier,"research_gate_pass":passed,
        "checks_passed":sum(checks.values()),"checks_total":len(checks),
        "checks_json":json.dumps(checks,sort_keys=True),
        "next_step":("FROZEN_CONFIRMATION_THEN_26_TO_27_PORTFOLIO_TEST" if passed
                     else "SHELVE_M15_LONG_OR_REQUIRE_GENUINELY_NEW_PREDECLARED_HYPOTHESIS"),
        "full_trades":full["trades"],"full_pf":full["profit_factor"],
        "full_r":full["total_r"],"full_dd":full["max_drawdown_r"],
        "validation_r":ps["VALIDATION_2018_PLUS"]["total_r"],
        "last5_r":ps["LAST5Y"]["total_r"],"last2_r":ps["LAST2Y"]["total_r"],
        "rolling24_pct":rs[24]["positive_active_windows_pct"],
        "rolling36_pct":rs[36]["positive_active_windows_pct"],
        "zero_trade_years":csummary["zero_trade_years"],
        "broad_family_supported":family_support["broad_family_supported"]}


def comp_trade_row(identifier,t):
    r={"config_id":identifier}
    for k,v in t.items():
        if isinstance(v,datetime):r[k]=iso(v)
        else:r[k]=v
    return r


def run_final_complement():
    try:
        STATUS.update(state="fetch",message="Fetch full AUD/USD M15 (read-only)",progress=1)
        candles=fetch("M15",START,NOW,35)
        if len(candles)<500_000:raise RuntimeError("AUD/USD M15 history too short")
        write_csv(OUTS["coverage"],[{"pair":PAIR,"bars":len(candles),
           "first_utc":iso(candles[0]["time"]),"last_utc":iso(candles[-1]["time"]),
           "baseline_cost_pips":PRIMARY_COST,"read_only":True,
           "predeclared_broad_cells":12,"prior_selected_controls":2}])
        STATUS.update(state="parity",message="Exact 55-trade discovery anchor at original cutoff",progress=16)
        focus_parity(candles)  # Stops immediately on any historic anchor drift.
        n=len(candles)
        absent={k:np.full(n,np.nan) for k in ("close","ema50","ema100","ema200","atr_ratio50")}
        STATUS.update(state="features",message="Build ATR14/causal M15 features and frozen raw signals",progress=25)
        f=features(candles,absent,absent,absent)
        core_config=focus_anchor()
        core_ix=focus_indices(core_config,f)
        core_trades=backtest(candles,core_ix,COMPLEMENT_RR,PRIMARY_COST)
        core_stats=stats(core_trades)
        core_union, core_audit=comp_union(candles,core_ix,[],PRIMARY_COST)
        if len(core_union)!=len(core_trades) or any(
                a["signal_index"]!=b["signal_index"] or
                abs(a["result_r"]-b["result_r"])>1e-10
                for a,b in zip(core_union,core_trades)):
            raise RuntimeError("Core p0 union engine differs from frozen core ledger")
        if len(core_trades)<PARITY_REFERENCE["trades"]:
            raise RuntimeError("New data lost frozen core trades; abort")
        core_periods=comp_periods(CORE_ID,core_trades)
        core_roll=comp_roll_summary(comp_rolling(CORE_ID,core_trades))
        core_roll_lookup={r["months"]:r for r in core_roll}
        core_calendar=comp_calendar(CORE_ID,core_trades)
        core_cals=comp_cal_summary(core_calendar)[0]
        write_csv(OUTS["core"],[{"config_id":CORE_ID,**core_stats,**core_audit,
            "rolling24_positive_active":core_roll_lookup[24]["positive_active_windows_pct"],
            "rolling36_positive_active":core_roll_lookup[36]["positive_active_windows_pct"],
            "zero_trade_completed_years":core_cals["zero_trade_years"]}])
        write_csv(OUTS["core_trades"],[comp_trade_row(CORE_ID,t) for t in core_trades])
        # First evaluate the complete fixed family list; show every failure.
        raw_rows=[];matrix=[];overlaps=[];precomputed={};family_rows=[]
        STATUS.update(state="families",message="Test 12 predeclared broad variants + two prior selected controls",progress=33)
        for pos,(identifier,family,params) in enumerate(COMPLEMENTS):
            idx=comp_raw_indices(f,family,params)
            ledger,audit=comp_union(candles,core_ix,idx)
            supplement=[t for t in ledger if t["trigger_id"]=="COMPLEMENT"]
            old_ids={t["signal_index"]:t for t in core_trades}
            new_ids={t["signal_index"]:t for t in ledger if t["trigger_id"]=="CORE"}
            displaced=set(old_ids)-set(new_ids)
            new_core=set(new_ids)-set(old_ids)
            p=comp_periods(identifier,ledger)
            rolls=comp_rolling(identifier,ledger)
            rsum=comp_roll_summary(rolls)
            cal=comp_calendar(identifier,ledger)
            csum=comp_cal_summary(cal)[0]
            pmap={x["period"]:x for x in p}
            rmap={x["months"]:x for x in rsum}
            row={"config_id":identifier,"family":family,
                 "params_json":json.dumps(params,sort_keys=True),
                 "prior_selected_control":family=="KNOWN_SWEEP_CONTROL",
                 **audit,**{f"combined_{k}":v for k,v in stats(ledger).items()},
                 **{f"marginal_{k}":v for k,v in stats(supplement).items()},
                 "delta_r_vs_core":stats(ledger)["total_r"]-core_stats["total_r"],
                 "validation2018_r":pmap["VALIDATION_2018_PLUS"]["total_r"],
                 "marginal_dev_r":sum(t["result_r"] for t in supplement if t["entry_time"]<DEV_END),
                 "marginal_validation_r":sum(t["result_r"] for t in supplement if t["entry_time"]>=DEV_END),
                 "last5_r":pmap["LAST5Y"]["total_r"],
                 "last2_r":pmap["LAST2Y"]["total_r"],
                 "rolling12_positive_pct":rmap[12]["positive_active_windows_pct"],
                 "rolling24_positive_pct":rmap[24]["positive_active_windows_pct"],
                 "rolling36_positive_pct":rmap[36]["positive_active_windows_pct"],
                 "rolling24_worst_r":rmap[24]["worst_r"],
                 "rolling36_worst_r":rmap[36]["worst_r"],
                 "calendar_positive_pct":csum["positive_active_years_pct"],
                 "zero_trade_years":csum["zero_trade_years"]}
            raw_rows.append({"config_id":identifier,"family":family,"params_json":row["params_json"],
              "prior_selected_control":row["prior_selected_control"],"raw_signals":len(idx),
              "raw_overlap_with_core":audit["same_candle_both"],
              "accepted_complement":audit["accepted_complement"],
              "marginal_r":row["marginal_total_r"],"marginal_pf":row["marginal_profit_factor"]})
            matrix.append(row)
            overlaps.append({"config_id":identifier,"family":family,**audit,
              "displaced_frozen_core_count":len(displaced),
              "displaced_frozen_core_r":sum(old_ids[i]["result_r"] for i in displaced),
              "newly_eligible_core_count":len(new_core),
              "newly_eligible_core_r":sum(new_ids[i]["result_r"] for i in new_core),
              "accepted_marginal_r":sum(t["result_r"] for t in supplement),
              "net_delta_r":row["delta_r_vs_core"]})
            precomputed[identifier]=(idx,ledger,p,rolls,rsum,cal,csum)
            STATUS.update(progress=34+int(34*(pos+1)/len(COMPLEMENTS)),
                message=f"Completed {pos+1}/{len(COMPLEMENTS)} fixed complement cells")
        write_csv(OUTS["raw"],raw_rows)
        write_csv(OUTS["matrix"],matrix)
        write_csv(OUTS["overlap"],overlaps)
        # Require evidence in at least 2 of 4 broad geometry neighbours,
        # including a positive MARGINAL contribution in BOTH broad temporal halves.
        for family in ("LOW_SWEEP","DECLINE_OUTSIDE","COMPRESSION_UP"):
            rows=[r for r in matrix if r["family"]==family]
            good=0
            for r in rows:
                p={x["period"]:x for x in precomputed[r["config_id"]][2]}
                if (r["marginal_total_r"]>0 and r["combined_total_r"]>core_stats["total_r"] and
                    r["marginal_dev_r"]>0 and r["marginal_validation_r"]>0 and
                    p["DEVELOPMENT_BEFORE_2018"]["total_r"]>0 and
                    p["VALIDATION_2018_PLUS"]["total_r"]>0):good+=1
            family_rows.append({"family":family,"configs":len(rows),
              "broad_geometry_cells_supporting":good,
              "family_supported":good>=2,
              "median_delta_r":med([r["delta_r_vs_core"] for r in rows]),
              "median_combined_pf":med([r["combined_profit_factor"] for r in rows]),
              "prior_selected_controls_are_independent_evidence":False})
        write_csv(OUTS["family"],family_rows)
        support={r["family"]:r["family_supported"] for r in family_rows}
        # Detailed rerun only top 4 BROAD geometries + strongest previous
        # control as DIAGNOSTIC (never passes without broad family support).
        eligible=[r for r in matrix if not r["prior_selected_control"]]
        eligible.sort(key=lambda r:(r["combined_trades"]>=85,
             r["marginal_total_r"]>0,r["validation2018_r"]>0,
             r["rolling36_positive_pct"],r["rolling24_positive_pct"],
             r["delta_r_vs_core"]),reverse=True)
        final_ids=[CORE_ID]+[r["config_id"] for r in eligible[:4]]
        controls=[r for r in matrix if r["prior_selected_control"]]
        controls.sort(key=lambda r:r["delta_r_vs_core"],reverse=True)
        if controls:final_ids.append(controls[0]["config_id"])
        period_rows=list(core_periods);roll_rows=comp_rolling(CORE_ID,core_trades)
        cal_rows=list(core_calendar);stress=[];trade_rows=[];decisions=[]
        # Core cost rerun as an explicit comparator; no outcome ledger merge.
        for cost in COSTS:
            core_cost,_=comp_union(candles,core_ix,[],cost)
            stress.append({"config_id":CORE_ID,"cost_pips":cost,
                           **stats(core_cost),"marginal_total_r":0.0})
        for pos,identifier in enumerate(final_ids[1:]):
            idx,ledger,p,rolls,rsum,cal,csum=precomputed[identifier]
            base=next(r for r in matrix if r["config_id"]==identifier)
            cost2=None
            cost2_marginal_r=None
            for cost in COSTS:
                cost_ledger,_=comp_union(candles,core_ix,idx,cost)
                marg=[t for t in cost_ledger if t["trigger_id"]=="COMPLEMENT"]
                stress.append({"config_id":identifier,"cost_pips":cost,
                     **stats(cost_ledger),"marginal_total_r":stats(marg)["total_r"]})
                if cost==2.0:
                    cost2=stats(cost_ledger)
                    cost2_marginal_r=stats(marg)["total_r"]
            period_rows.extend(p);roll_rows.extend(rolls);cal_rows.extend(cal)
            fam=base["family"]
            checks=comp_assess(identifier,ledger,core_trades,rsum,csum,p,cost2,
                {"core_roll24":core_roll_lookup[24]["positive_active_windows_pct"],
                 "core_roll36":core_roll_lookup[36]["positive_active_windows_pct"],
                 "core_worst24":core_roll_lookup[24]["worst_r"],
                 "core_worst36":core_roll_lookup[36]["worst_r"],
                 "cost2_marginal_r":cost2_marginal_r,
                 "broad_family_supported":support.get(fam,False)})
            # No previous selected narrow control can pass independently.
            if fam=="KNOWN_SWEEP_CONTROL":
                checks["research_gate_pass"]=False
                checks["next_step"]="REFERENCE_ONLY_ALREADY_SELECTED_AND_SMALL_SAMPLE"
            decisions.append(checks)
            for t in ledger:trade_rows.append(comp_trade_row(identifier,t))
            STATUS.update(state="deep",progress=70+int(25*(pos+1)/(len(final_ids)-1)),
                message=f"Deep cost/rolling audit {pos+1}/{len(final_ids)-1}")
        write_csv(OUTS["periods"],period_rows)
        write_csv(OUTS["rolling"],roll_rows)
        write_csv(OUTS["rolling_summary"],comp_roll_summary(roll_rows))
        write_csv(OUTS["calendar"],cal_rows)
        write_csv(OUTS["calendar_summary"],comp_cal_summary(cal_rows))
        write_csv(OUTS["stress"],stress)
        write_csv(OUTS["trades"],trade_rows)
        write_csv(OUTS["decision"],decisions)
        write_csv(OUTS["notes"],[
          {"item":"purpose","value":"One last complement attempt, NOT repeat core/session/RR optimization."},
          {"item":"research_scope","value":"12 predeclared no-session complement variants in 3 structural families; two previously selected tiny-sample sweep controls visible separately."},
          {"item":"frozen_core","value":"Failure/reclaim prior60 low, body>=1ATR, closeLoc>=.65, Sydney04-07 signal-open, RR3.50."},
          {"item":"exits","value":"1pip adverse M15 long entry; stop low-10 ticks; target reference close RR3.5; exit search next candle; conservative same-bar tie."},
          {"item":"union","value":"Exact chronological raw-signal union; core same-candle priority; strategy p0 across both; exit-candle signal eligible; no merging separately pyramided ledgers."},
          {"item":"family_quality","value":"2 of 4 broad local family geometries required; previously selected narrow controls do NOT count as independent evidence."},
          {"item":"year_diagnostics","value":"2016-18/2020-22 and no-trade years are reported, NOT used for rule selection or date exclusion."},
          {"item":"limitations","value":"All historical segments have been studied previously. Positive 2018+ is not untouched out-of-sample. No historical result predicts live profit."},
          {"item":"next","value":"Only if gated family and standalone union both pass: independent frozen confirmation, then exact 26->27 live-safe AUD/USD portfolio-add conflict test."},
          {"item":"safety","value":"Read only. No trade submission. The existing 26 strategies are unchanged."},
        ])
        STATUS.update(state="packaging",message="Package complete read-only results ZIP",progress=98)
        package_results()
        STATUS.update(state="complete",message="Final frozen-core complement research complete",
            progress=100,anchor_parity="PASS",tested_broad=12,prior_selected_controls=2,
            full_reported=len(matrix),deep=len(final_ids)-1,
            research_gate_passes=sum(r["research_gate_pass"] for r in decisions),bundle=BUNDLE)
    except Exception as ex:
        import traceback
        STATUS.update(state="error",message=str(ex),traceback=traceback.format_exc())
        print("AUD/USD M15 LONG FINAL COMPLEMENT ERROR",repr(ex),flush=True)

@app.route("/audusd-m15-long-27-complement/status")
def comp_status():
    return jsonify(STATUS)

@app.route("/audusd-m15-long-27-complement/results")
def comp_results():
    return download(BUNDLE)

@app.route("/audusd-m15-long-27-complement/info")
def comp_info():
    return jsonify({"service":"AUDUSD M15 LONG frozen-core complement search",
        "orders_supported":False,"trading_enabled":False,
        "live_strategies_unchanged":26,"frozen_core":"FAILED_BREAKDOWN_RECLAIM",
        "broad_complement_variants":12,"previous_selected_controls":2,
        "cost_pips":[.5,1.0,1.5,2.0],"rr":3.5,
        "routes":["/audusd-m15-long-27-complement/status",
                  "/audusd-m15-long-27-complement/results"]})

if __name__ == "__main__":
    threading.Thread(target=run_final_complement,daemon=True).start()
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")),debug=False)
