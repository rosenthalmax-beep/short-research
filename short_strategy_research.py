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
# EUR/JPY M15 SHORT #24 — FROZEN DEEP VALIDATION
#
# Purpose:
#   Search EUR/JPY M15 SHORT from first principles. This runner does NOT
#   invert or inherit the locked EUR/JPY M15 LONG #23 entry logic.
#
# Research universe:
#   2002-05-06 20:00 UTC -> present / earliest OANDA available
#
# Independent trigger families:
#   1) BEAR_ENGULF_STRUCTURE
#   2) HIGH_SWEEP_DISPLACEMENT
#   3) FAILED_BREAKOUT_REJECTION
#   4) OUTSIDE_REVERSAL
#   5) COMPRESSION_BREAKDOWN
#   6) RALLY_REJECTION
#
# Staged process:
#   Stage 1: raw archetype geometry, no HTF/session context
#   Stage 2: one broad HTF / volatility / session / weekday context at a time
#   Stage 3: local geometry + RR robustness around diverse Stage-2 winners
#   Final shortlist diagnostics:
#       temporal periods, four eras, 2018+, 2020+, last 5Y/2Y/1Y,
#       0.5/1/1.5/2 pip cost stress, rolling 12/24/36M,
#       calendar/tradeless years, full trade ledger.
#
# Historical conventions (matched to prior EUR/JPY M15 research):
#   OANDA midpoint
#   ATR14 Wilder/RMA, SMA seeded
#   signal timestamp = M15 candle OPEN
#   reference entry = signal close
#   primary adverse cost = 1 pip
#   SHORT historical fill = signal close - adverse cost
#   stop = signal high + 10 ticks
#   target = reference close - RR * reference-close risk
#   exits begin on next candle
#   pyramiding 0
#   exact exit-candle signal eligible (half-open holding interval)
#   short same-bar stop+target: lower side closer to open => target first,
#                               otherwise stop first
#
# HTF no-lookahead:
#   H1/H4/D state becomes usable only at the next ACTUAL HTF candle open.
#   lookup = bisect_right(completion_times, signal_time) - 1
#
# IMPORTANT:
#   This is exploratory full-history research, not pristine OOS validation.
#   No strategy is automatically approved for live trading by this runner.
#   Any survivor still needs deep validation and a 23 -> 24 portfolio-add test.
#
# READ ONLY. NEVER SENDS ORDERS.
# ============================================================

app = Flask(__name__)

TOKEN = os.getenv("OANDA_TOKEN")
BASE = os.getenv("OANDA_API_URL", "https://api-fxtrade.oanda.com")
PAIR = "EUR_JPY"

START = datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc)
NOW = datetime.now(timezone.utc).replace(second=0, microsecond=0)
WARMUP = START - timedelta(days=900)

NY = ZoneInfo("America/New_York")
LONDON = ZoneInfo("Europe/London")
TOKYO = ZoneInfo("Asia/Tokyo")

TICK = 0.001
PIP = 0.01
STOP_TICKS = 10

PRIMARY_COST = 1.0
COSTS = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0]

# Diversity is intentional: do not let one family monopolise the next stage.
STAGE1_PER_FAMILY = 2
STAGE1_BASE_KEEP = 14
STAGE2_PER_FAMILY = 2
STAGE2_BASE_KEEP = 12
FINAL_PER_FAMILY = 2
FINAL_KEEP = 14

OUTS = {
    "coverage": "eurjpy_m15_short_24_coverage.csv",
    "stage1": "eurjpy_m15_short_24_stage1_summary.csv",
    "stage1_family": "eurjpy_m15_short_24_stage1_family_summary.csv",
    "stage2": "eurjpy_m15_short_24_stage2_summary.csv",
    "stage2_family": "eurjpy_m15_short_24_stage2_family_summary.csv",
    "stage3": "eurjpy_m15_short_24_stage3_summary.csv",
    "stage3_family": "eurjpy_m15_short_24_stage3_family_summary.csv",
    "shortlist": "eurjpy_m15_short_24_shortlist.csv",
    "periods": "eurjpy_m15_short_24_shortlist_periods.csv",
    "cost": "eurjpy_m15_short_24_shortlist_cost_stress.csv",
    "rolling": "eurjpy_m15_short_24_shortlist_rolling.csv",
    "rolling_summary": "eurjpy_m15_short_24_shortlist_rolling_summary.csv",
    "calendar": "eurjpy_m15_short_24_shortlist_calendar_years.csv",
    "calendar_summary": "eurjpy_m15_short_24_shortlist_calendar_summary.csv",
    "trades": "eurjpy_m15_short_24_shortlist_trades.csv",
    "decision": "eurjpy_m15_short_24_decision_matrix.csv",
    "notes": "eurjpy_m15_short_24_notes.csv",
}
BUNDLE = "EURJPY_M15_SHORT_24_BROAD_RESEARCH_RESULTS.zip"

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
# M15 FEATURE CACHE — SHORT ORIENTED
# ============================================================

def features(candles, h1, h4, daily):
    n = len(candles)
    times = [x["time"] for x in candles]
    o = np.array([x["open"] for x in candles], dtype=float)
    h = np.array([x["high"] for x in candles], dtype=float)
    l = np.array([x["low"] for x in candles], dtype=float)
    cl = np.array([x["close"] for x in candles], dtype=float)

    a = atr(candles, 14)
    am20 = sma(a, 20)
    bearish = cl < o

    exact_bear = np.zeros(n, dtype=bool)
    exact_bear[1:] = (
        (cl[:-1] > o[:-1])
        & (cl[1:] < o[1:])
        & (o[1:] >= cl[:-1])
        & (cl[1:] <= o[:-1])
    )

    bear_body = o - cl
    previous_body = np.full(n, np.nan)
    previous_body[1:] = np.abs(cl[:-1] - o[:-1])

    bear_br = np.full(n, np.nan)
    valid_previous_body = previous_body > 0
    bear_br[valid_previous_body] = (
        bear_body[valid_previous_body] / previous_body[valid_previous_body]
    )

    valid_atr = np.isfinite(a) & (a > 0)

    body_atr = np.full(n, np.nan)
    body_atr[valid_atr] = bear_body[valid_atr] / a[valid_atr]

    candle_range = h - l
    range_atr = np.full(n, np.nan)
    range_atr[valid_atr] = candle_range[valid_atr] / a[valid_atr]

    # Short-friendly: 0 = close at low, 1 = close at high.
    close_loc = np.full(n, np.nan)
    valid_range = candle_range > 0
    close_loc[valid_range] = (
        (cl[valid_range] - l[valid_range]) / candle_range[valid_range]
    )

    upper_wick = h - np.maximum(o, cl)
    upper_wick_body = np.full(n, np.nan)
    positive_bear_body = bear_body > 0
    upper_wick_body[positive_bear_body] = (
        upper_wick[positive_bear_body] / bear_body[positive_bear_body]
    )

    compression = np.full(n, np.nan)
    previous_atr = np.r_[np.nan, a[:-1]]
    previous_atr_mean20 = np.r_[np.nan, am20[:-1]]
    comp_ok = (
        np.isfinite(previous_atr)
        & np.isfinite(previous_atr_mean20)
        & (previous_atr_mean20 > 0)
    )
    compression[comp_ok] = (
        previous_atr[comp_ok] / previous_atr_mean20[comp_ok]
    )

    lookbacks = [10, 20, 40, 60, 80, 100, 120, 165, 200]
    prev_low = {lb: prev_extreme(l, lb, "min") for lb in lookbacks}
    prev_high = {lb: prev_extreme(h, lb, "max") for lb in lookbacks}

    structure_dist_high = {}
    for lb in [40, 60, 80, 100, 120, 165, 200]:
        arr = np.full(n, np.nan)
        ok = valid_atr & np.isfinite(prev_high[lb])
        arr[ok] = np.abs(h[ok] - prev_high[lb][ok]) / a[ok]
        structure_dist_high[lb] = arr

    # Prior ~4 hours, excluding the signal candle.
    mom4 = np.full(n, np.nan)
    for i in range(17, n):
        if valid_atr[i]:
            mom4[i] = (cl[i - 1] - cl[i - 17]) / a[i]

    ny_hour = np.zeros(n, dtype=np.int16)
    ny_weekday = np.zeros(n, dtype=np.int16)
    london_hour = np.zeros(n, dtype=np.int16)
    london_weekday = np.zeros(n, dtype=np.int16)
    tokyo_hour = np.zeros(n, dtype=np.int16)
    tokyo_weekday = np.zeros(n, dtype=np.int16)

    for i, t in enumerate(times):
        z = t.astimezone(NY)
        ny_hour[i], ny_weekday[i] = z.hour, z.weekday()
        z = t.astimezone(LONDON)
        london_hour[i], london_weekday[i] = z.hour, z.weekday()
        z = t.astimezone(TOKYO)
        tokyo_hour[i], tokyo_weekday[i] = z.hour, z.weekday()

    return {
        "n": n,
        "times": times,
        "open": o,
        "high": h,
        "low": l,
        "close": cl,
        "atr": a,
        "valid_atr": valid_atr,
        "bearish": bearish,
        "exact_bear": exact_bear,
        "bear_br": bear_br,
        "body_atr": body_atr,
        "range_atr": range_atr,
        "close_loc": close_loc,
        "upper_wick_body": upper_wick_body,
        "compression": compression,
        "prev_low": prev_low,
        "prev_high": prev_high,
        "structure_dist_high": structure_dist_high,
        "mom4": mom4,
        "ny_hour": ny_hour,
        "ny_weekday": ny_weekday,
        "london_hour": london_hour,
        "london_weekday": london_weekday,
        "tokyo_hour": tokyo_hour,
        "tokyo_weekday": tokyo_weekday,
        "h1_close": h1["close"],
        "h1_ema50": h1["ema50"],
        "h1_ema100": h1["ema100"],
        "h1_ema200": h1["ema200"],
        "h1_atr": h1["atr_ratio50"],
        "h4_close": h4["close"],
        "h4_ema100": h4["ema100"],
        "h4_ema200": h4["ema200"],
        "h4_atr": h4["atr_ratio50"],
        "d_close": daily["close"],
        "d_ema50": daily["ema50"],
        "d_ema200": daily["ema200"],
        "d_atr": daily["atr_ratio50"],
    }


# ============================================================
# CANDIDATE CONFIGURATION
# ============================================================

def cfg(config_id, family, rr=3.5, **kwargs):
    row = {
        "config_id": config_id,
        "family": family,
        "rr": rr,
        "context": "NONE",
        "br_min": None,
        "body_atr_min": None,
        "range_atr_min": None,
        "close_loc_max": None,
        "upper_wick_body_min": None,
        "structure_lb": None,
        "structure_dist_atr_max": None,
        "sweep_lb": None,
        "breakdown_lb": None,
        "compression_max": None,
        "mom4_min": None,
        "excluded_weekdays": set(),
        "excluded_ny_hours": set(),
    }
    row.update(kwargs)
    return row


def stage1_configs():
    out = []

    # 1) Exact bearish engulf near prior resistance/high structure.
    engulf = [
        (1.00, 0.50, 60, 0.10),
        (1.00, 0.75, 100, 0.10),
        (1.20, 0.50, 100, 0.15),
        (1.20, 0.75, 120, 0.10),
        (1.20, 1.00, 165, 0.10),
        (1.35, 0.50, 120, 0.20),
        (1.35, 0.75, 165, 0.10),
        (1.35, 1.00, 165, 0.15),
        (1.50, 0.75, 100, 0.10),
        (1.50, 1.00, 165, 0.10),
    ]
    for i, (br, body, lb, dist) in enumerate(engulf):
        out.append(cfg(
            f"S1_ENG_{i}",
            "BEAR_ENGULF_STRUCTURE",
            br_min=br,
            body_atr_min=body,
            structure_lb=lb,
            structure_dist_atr_max=dist,
        ))

    # 2) Sweep prior high, then bearish displacement through previous candle low.
    sweep = [
        (20, 0.75, 0.15, 0.50),
        (20, 1.00, 0.25, 1.00),
        (40, 0.75, 0.25, 0.50),
        (40, 1.00, 0.25, 1.00),
        (40, 1.25, 0.25, 1.25),
        (60, 0.75, 0.25, 0.75),
        (60, 1.00, 0.35, 1.00),
        (60, 1.25, 0.25, 1.50),
        (100, 1.00, 0.35, 1.25),
        (100, 1.25, 0.35, 1.50),
    ]
    for i, (lb, body, wick, mom) in enumerate(sweep):
        out.append(cfg(
            f"S1_SWEEP_{i}",
            "HIGH_SWEEP_DISPLACEMENT",
            sweep_lb=lb,
            body_atr_min=body,
            upper_wick_body_min=wick,
            mom4_min=mom,
        ))

    # 3) False breakout above structure that closes back below it.
    failed = [
        (20, 0.50, 0.40),
        (20, 0.75, 0.30),
        (40, 0.50, 0.40),
        (40, 0.75, 0.30),
        (40, 1.00, 0.25),
        (60, 0.50, 0.35),
        (60, 0.75, 0.30),
        (60, 1.00, 0.25),
        (100, 0.75, 0.30),
        (100, 1.00, 0.20),
    ]
    for i, (lb, body, close_max) in enumerate(failed):
        out.append(cfg(
            f"S1_FAIL_{i}",
            "FAILED_BREAKOUT_REJECTION",
            sweep_lb=lb,
            body_atr_min=body,
            close_loc_max=close_max,
        ))

    # 4) Bearish outside bar at/near prior high structure.
    outside = [
        (0.50, 0.40, 40, 0.20),
        (0.75, 0.35, 40, 0.15),
        (0.75, 0.30, 60, 0.20),
        (1.00, 0.35, 60, 0.15),
        (1.00, 0.25, 80, 0.20),
        (1.25, 0.30, 80, 0.15),
        (1.25, 0.25, 100, 0.20),
        (1.50, 0.25, 100, 0.15),
    ]
    for i, (body, close_max, lb, dist) in enumerate(outside):
        out.append(cfg(
            f"S1_OUT_{i}",
            "OUTSIDE_REVERSAL",
            body_atr_min=body,
            close_loc_max=close_max,
            structure_lb=lb,
            structure_dist_atr_max=dist,
        ))

    # 5) Volatility compression followed by downside range/body expansion.
    compression = [
        (0.60, 0.75, 1.20, 10),
        (0.65, 0.75, 1.30, 10),
        (0.65, 1.00, 1.40, 10),
        (0.70, 0.75, 1.30, 10),
        (0.70, 1.00, 1.40, 10),
        (0.70, 1.25, 1.50, 10),
        (0.75, 0.75, 1.30, 10),
        (0.75, 1.00, 1.40, 10),
        (0.75, 1.25, 1.50, 20),
        (0.80, 1.00, 1.50, 20),
    ]
    for i, (comp, body, range_min, lb) in enumerate(compression):
        out.append(cfg(
            f"S1_COMP_{i}",
            "COMPRESSION_BREAKDOWN",
            compression_max=comp,
            body_atr_min=body,
            range_atr_min=range_min,
            breakdown_lb=lb,
        ))

    # 6) Prior rally into a high sweep/rejection, but not necessarily engulfing.
    rally_rejection = [
        (20, 0.50, 0.75, 0.35),
        (20, 0.75, 1.00, 0.30),
        (40, 0.50, 1.00, 0.35),
        (40, 0.75, 1.25, 0.30),
        (40, 1.00, 1.50, 0.25),
        (60, 0.75, 1.25, 0.30),
        (60, 1.00, 1.50, 0.25),
        (100, 1.00, 1.50, 0.25),
    ]
    for i, (lb, body, mom, close_max) in enumerate(rally_rejection):
        out.append(cfg(
            f"S1_RALLY_{i}",
            "RALLY_REJECTION",
            sweep_lb=lb,
            body_atr_min=body,
            mom4_min=mom,
            close_loc_max=close_max,
        ))

    return out


CONTEXTS = [
    "NONE",
    "H1_CLOSE_LT_EMA100",
    "H1_CLOSE_LT_EMA200",
    "H1_EMA50_LT_EMA200",
    "H4_CLOSE_LT_EMA100",
    "H4_CLOSE_LT_EMA200",
    "D_CLOSE_LT_EMA200",
    "D_EMA50_LT_EMA200",
    "H1_ATR_GE_080",
    "H4_ATR_GE_080",
    "D_ATR_GE_080",
    "NY_BLOCK_00-03",
    "NY_BLOCK_04-07",
    "NY_BLOCK_08-11",
    "NY_BLOCK_12-15",
    "NY_BLOCK_16-19",
    "NY_BLOCK_20-23",
    "LDN_BLOCK_00-03",
    "LDN_BLOCK_04-07",
    "LDN_BLOCK_08-11",
    "LDN_BLOCK_12-15",
    "LDN_BLOCK_16-19",
    "LDN_BLOCK_20-23",
    "TOKYO_BLOCK_00-03",
    "TOKYO_BLOCK_04-07",
    "TOKYO_BLOCK_08-11",
    "TOKYO_BLOCK_12-15",
    "TOKYO_BLOCK_16-19",
    "TOKYO_BLOCK_20-23",
    "EXCLUDE_WEEKDAY_0",
    "EXCLUDE_WEEKDAY_1",
    "EXCLUDE_WEEKDAY_2",
    "EXCLUDE_WEEKDAY_3",
    "EXCLUDE_WEEKDAY_4",
]


# ============================================================
# SIGNAL EVALUATION
# ============================================================

def context_mask(mask, config, f):
    ctx = config.get("context", "NONE")

    if ctx == "H1_CLOSE_LT_EMA100":
        mask &= f["h1_close"] < f["h1_ema100"]
    elif ctx == "H1_CLOSE_LT_EMA200":
        mask &= f["h1_close"] < f["h1_ema200"]
    elif ctx == "H1_EMA50_LT_EMA200":
        mask &= f["h1_ema50"] < f["h1_ema200"]
    elif ctx == "H4_CLOSE_LT_EMA100":
        mask &= f["h4_close"] < f["h4_ema100"]
    elif ctx == "H4_CLOSE_LT_EMA200":
        mask &= f["h4_close"] < f["h4_ema200"]
    elif ctx == "D_CLOSE_LT_EMA200":
        mask &= f["d_close"] < f["d_ema200"]
    elif ctx == "D_EMA50_LT_EMA200":
        mask &= f["d_ema50"] < f["d_ema200"]
    elif ctx == "H1_ATR_GE_080":
        mask &= f["h1_atr"] >= 0.80
    elif ctx == "H4_ATR_GE_080":
        mask &= f["h4_atr"] >= 0.80
    elif ctx == "D_ATR_GE_080":
        mask &= f["d_atr"] >= 0.80
    elif ctx.startswith("NY_BLOCK_"):
        a, b = map(int, ctx.split("_")[-1].split("-"))
        mask &= (f["ny_hour"] >= a) & (f["ny_hour"] <= b)
    elif ctx.startswith("LDN_BLOCK_"):
        a, b = map(int, ctx.split("_")[-1].split("-"))
        mask &= (f["london_hour"] >= a) & (f["london_hour"] <= b)
    elif ctx.startswith("TOKYO_BLOCK_"):
        a, b = map(int, ctx.split("_")[-1].split("-"))
        mask &= (f["tokyo_hour"] >= a) & (f["tokyo_hour"] <= b)
    elif ctx.startswith("EXCLUDE_WEEKDAY_"):
        weekday = int(ctx.split("_")[-1])
        mask &= f["ny_weekday"] != weekday

    for weekday in config.get("excluded_weekdays", set()):
        mask &= f["ny_weekday"] != weekday
    for hour in config.get("excluded_ny_hours", set()):
        mask &= f["ny_hour"] != hour

    return mask


def indices(config, f):
    mask = f["valid_atr"].copy() & f["bearish"]
    family = config["family"]

    if family == "BEAR_ENGULF_STRUCTURE":
        mask &= f["exact_bear"]
        mask &= f["bear_br"] >= config["br_min"]
        mask &= f["body_atr"] >= config["body_atr_min"]
        mask &= (
            f["structure_dist_high"][config["structure_lb"]]
            <= config["structure_dist_atr_max"]
        )

    elif family == "HIGH_SWEEP_DISPLACEMENT":
        lb = config["sweep_lb"]
        mask &= f["high"] > f["prev_high"][lb]
        previous_low = np.roll(f["low"], 1)
        mask[0] = False
        mask &= f["close"] < previous_low
        mask &= f["body_atr"] >= config["body_atr_min"]
        mask &= f["upper_wick_body"] >= config["upper_wick_body_min"]
        mask &= f["mom4"] >= config["mom4_min"]

    elif family == "FAILED_BREAKOUT_REJECTION":
        prior_high = f["prev_high"][config["sweep_lb"]]
        mask &= f["high"] > prior_high
        mask &= f["close"] < prior_high
        mask &= f["body_atr"] >= config["body_atr_min"]
        mask &= f["close_loc"] <= config["close_loc_max"]

    elif family == "OUTSIDE_REVERSAL":
        previous_high = np.roll(f["high"], 1)
        previous_low = np.roll(f["low"], 1)
        mask[0] = False
        mask &= f["high"] > previous_high
        mask &= f["low"] < previous_low
        mask &= f["body_atr"] >= config["body_atr_min"]
        mask &= f["close_loc"] <= config["close_loc_max"]
        mask &= (
            f["structure_dist_high"][config["structure_lb"]]
            <= config["structure_dist_atr_max"]
        )

    elif family == "COMPRESSION_BREAKDOWN":
        mask &= f["compression"] <= config["compression_max"]
        mask &= f["body_atr"] >= config["body_atr_min"]
        mask &= f["range_atr"] >= config["range_atr_min"]
        mask &= f["close"] < f["prev_low"][config["breakdown_lb"]]

    elif family == "RALLY_REJECTION":
        prior_high = f["prev_high"][config["sweep_lb"]]
        mask &= f["high"] > prior_high
        mask &= f["close"] < f["prev_high"][10]
        mask &= f["body_atr"] >= config["body_atr_min"]
        mask &= f["mom4"] >= config["mom4_min"]
        mask &= f["close_loc"] <= config["close_loc_max"]

    else:
        raise ValueError(f"Unknown family: {family}")

    mask = context_mask(mask, config, f)
    mask[:200] = False
    return np.flatnonzero(mask).tolist()


# ============================================================
# SHORT BACKTEST
# ============================================================

OUTCOME_CACHE = {}


def outcome(candles, signal_index, rr, cost_pips):
    key = (signal_index, round(rr, 4), round(cost_pips, 4))
    if key in OUTCOME_CACHE:
        cached = OUTCOME_CACHE[key]
        return None if cached is None else dict(cached)

    signal = candles[signal_index]
    reference = signal["close"]
    stop = signal["high"] + STOP_TICKS * TICK
    reference_risk = stop - reference

    if reference_risk <= 0:
        OUTCOME_CACHE[key] = None
        return None

    target = reference - rr * reference_risk
    fill = reference - cost_pips * PIP
    actual_risk = stop - fill

    if actual_risk <= 0:
        OUTCOME_CACHE[key] = None
        return None

    for j in range(signal_index + 1, len(candles)):
        bar = candles[j]
        hit_stop = bar["high"] >= stop
        hit_target = bar["low"] <= target

        if hit_stop and hit_target:
            # Short target is below. If the low side is closer to the open,
            # assume target first; otherwise stop first.
            if abs(bar["open"] - bar["low"]) < abs(bar["high"] - bar["open"]):
                exit_price = target
                reason = "TARGET"
            else:
                exit_price = stop
                reason = "STOP"
        elif hit_target:
            exit_price = target
            reason = "TARGET"
        elif hit_stop:
            exit_price = stop
            reason = "STOP"
        else:
            continue

        result_r = (fill - exit_price) / actual_risk
        row = {
            "signal_index": signal_index,
            "exit_index": j,
            "entry_time": signal["time"],
            "exit_time": bar["time"],
            "entry_time_utc": iso(signal["time"]),
            "exit_time_utc": iso(bar["time"]),
            "reference_entry": reference,
            "historical_fill": fill,
            "stop": stop,
            "target": target,
            "result_r": result_r,
            "exit_reason": reason,
            "rr": rr,
            "cost_pips": cost_pips,
        }
        OUTCOME_CACHE[key] = dict(row)
        return row

    OUTCOME_CACHE[key] = None
    return None


def backtest(candles, candidate_indices, rr, cost_pips, start=None, end=None):
    use = candidate_indices

    if start is not None or end is not None:
        times = [candles[i]["time"] for i in candidate_indices]
        a = 0 if start is None else bisect.bisect_left(times, start)
        b = len(candidate_indices) if end is None else bisect.bisect_left(times, end)
        use = candidate_indices[a:b]

    trades = []
    p = 0
    while p < len(use):
        trade = outcome(candles, use[p], rr, cost_pips)
        if trade is None:
            p += 1
            continue

        trades.append(dict(trade))

        # p0 half-open [signal_index, exit_index); a signal exactly on the
        # exit candle is eligible.
        p = bisect.bisect_left(use, trade["exit_index"], lo=p + 1)

    return trades


def stats(trades):
    results = [float(x["result_r"]) for x in trades]
    winners = [x for x in results if x > 0]
    losers = [x for x in results if x < 0]

    gross_profit = sum(winners)
    gross_loss = abs(sum(losers))
    pf = (
        gross_profit / gross_loss
        if gross_loss > 0
        else (999.0 if gross_profit > 0 else 0.0)
    )

    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    streak = 0
    longest_streak = 0

    for r in results:
        equity += r
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
        if r < 0:
            streak += 1
            longest_streak = max(longest_streak, streak)
        else:
            streak = 0

    return {
        "trades": len(results),
        "winners": len(winners),
        "losers": len(losers),
        "win_rate": 100.0 * len(winners) / len(results) if results else 0.0,
        "profit_factor": pf,
        "total_r": sum(results),
        "expectancy_r": sum(results) / len(results) if results else 0.0,
        "max_drawdown_r": max_dd,
        "longest_loss_streak": longest_streak,
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
        "close_loc_max",
        "upper_wick_body_min",
        "structure_lb",
        "structure_dist_atr_max",
        "sweep_lb",
        "breakdown_lb",
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
        "close_loc_max": [-0.10, -0.05, 0.05, 0.10],
        "upper_wick_body_min": [-0.10, -0.05, 0.05, 0.10],
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
            if field == "close_loc_max":
                if not (0.05 <= new_value <= 0.95):
                    continue
            elif new_value <= 0:
                continue
            x = deepcopy(base)
            x[field] = new_value
            x["config_id"] = f"S3_{rank}_{field}_{new_value}"
            out.append(x)

    lookbacks = [10, 20, 40, 60, 80, 100, 120, 165, 200]
    for field in ["structure_lb", "sweep_lb", "breakdown_lb"]:
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
                "close_loc_max",
                "upper_wick_body_min",
                "structure_lb",
                "structure_dist_atr_max",
                "sweep_lb",
                "breakdown_lb",
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

def run_research():
    try:
        STATUS.update({
            "state": "fetch",
            "message": "Fetching EUR/JPY M15 + H1/H4/D history",
            "progress": 1,
        })

        m15 = fetch("M15", START, NOW, 35)
        h1 = fetch("H1", WARMUP, NOW, 180)
        h4 = fetch("H4", WARMUP, NOW, 700)
        daily = fetch("D", WARMUP, NOW, 3500)

        if not all([m15, h1, h4, daily]):
            raise RuntimeError("Missing required history")

        write_csv(OUTS["coverage"], [{
            "instrument": PAIR,
            "requested_start_utc": iso(START),
            "actual_first_m15_utc": iso(m15[0]["time"]),
            "actual_last_m15_utc": iso(m15[-1]["time"]),
            "m15_candles": len(m15),
            "h1_candles": len(h1),
            "h4_candles": len(h4),
            "daily_candles": len(daily),
            "baseline_cost_pips": PRIMARY_COST,
            "side": "SHORT",
        }])

        STATUS.update({
            "state": "precompute",
            "message": "Building strict completed-HTF alignment and M15 feature cache",
            "progress": 20,
        })

        m15_times = [x["time"] for x in m15]
        aligned_h1 = align_htf(m15_times, htf_state(h1))
        aligned_h4 = align_htf(m15_times, htf_state(h4))
        aligned_daily = align_htf(m15_times, htf_state(daily))
        f = features(m15, aligned_h1, aligned_h4, aligned_daily)

        # ---------------- Stage 1 ----------------
        STATUS.update({
            "state": "stage1",
            "message": "Stage 1: raw independent short archetypes",
            "progress": 28,
        })

        stage1 = stage1_configs()
        config_by_id = {x["config_id"]: x for x in stage1}
        stage1_rows = []

        for i, config in enumerate(stage1, 1):
            if i % 8 == 0:
                STATUS.update({
                    "state": "stage1",
                    "message": f"Stage 1 {i}/{len(stage1)}",
                    "progress": 28 + int(12 * i / len(stage1)),
                })
            ix = indices(config, f)
            stage1_rows.append(evaluate(config, m15, ix))

        stage1_rows = sort_rows(stage1_rows)
        stage1_family = family_summary(stage1_rows)
        write_csv(OUTS["stage1"], stage1_rows)
        write_csv(OUTS["stage1_family"], stage1_family)

        stage1_bases = select_diverse(
            stage1_rows,
            STAGE1_PER_FAMILY,
            STAGE1_BASE_KEEP,
        )

        # ---------------- Stage 2 ----------------
        STATUS.update({
            "state": "stage2",
            "message": "Stage 2: broad HTF / volatility / session / weekday contexts",
            "progress": 42,
        })

        stage2 = stage2_configs(stage1_bases, config_by_id)
        config_by_id.update({x["config_id"]: x for x in stage2})
        stage2_rows = []

        for i, config in enumerate(stage2, 1):
            if i % 25 == 0:
                STATUS.update({
                    "state": "stage2",
                    "message": f"Stage 2 {i}/{len(stage2)}",
                    "progress": 42 + int(20 * i / len(stage2)),
                })
            ix = indices(config, f)
            stage2_rows.append(evaluate(config, m15, ix))

        stage2_rows = sort_rows(stage2_rows)
        stage2_family = family_summary(stage2_rows)
        write_csv(OUTS["stage2"], stage2_rows)
        write_csv(OUTS["stage2_family"], stage2_family)

        stage2_bases = select_diverse(
            stage2_rows,
            STAGE2_PER_FAMILY,
            STAGE2_BASE_KEEP,
        )

        # ---------------- Stage 3 ----------------
        STATUS.update({
            "state": "stage3",
            "message": "Stage 3: local geometry and RR robustness",
            "progress": 64,
        })

        stage3 = stage3_configs(stage2_bases, config_by_id)
        config_by_id.update({x["config_id"]: x for x in stage3})
        stage3_rows = []

        for i, config in enumerate(stage3, 1):
            if i % 25 == 0:
                STATUS.update({
                    "state": "stage3",
                    "message": f"Stage 3 {i}/{len(stage3)}",
                    "progress": 64 + int(15 * i / len(stage3)),
                })
            ix = indices(config, f)
            stage3_rows.append(evaluate(config, m15, ix))

        stage3_rows = sort_rows(stage3_rows)
        stage3_family = family_summary(stage3_rows)
        write_csv(OUTS["stage3"], stage3_rows)
        write_csv(OUTS["stage3_family"], stage3_family)

        final_rows = select_diverse(
            stage3_rows,
            FINAL_PER_FAMILY,
            FINAL_KEEP,
        )
        final_configs = [config_by_id[row["config_id"]] for row in final_rows]

        # ---------------- Deep diagnostics on shortlist ----------------
        STATUS.update({
            "state": "shortlist_diagnostics",
            "message": "Shortlist temporal / cost / rolling / calendar diagnostics",
            "progress": 80,
        })

        shortlist = []
        periods = []
        costs = []
        rolling = []
        calendar = []
        trades = []

        for i, config in enumerate(final_configs, 1):
            STATUS.update({
                "state": "shortlist_diagnostics",
                "message": f"Shortlist {i}/{len(final_configs)}: {config['config_id']}",
                "progress": 80 + int(15 * i / max(1, len(final_configs))),
            })

            ix = indices(config, f)
            shortlist.append(shortlist_summary(config, m15, ix))
            periods.extend(period_rows(config, m15, ix))
            costs.extend(cost_rows(config, m15, ix))
            rrows = rolling_rows(config, m15, ix)
            crows = calendar_rows(config, m15, ix)
            rolling.extend(rrows)
            calendar.extend(crows)

            for trade in backtest(m15, ix, config["rr"], PRIMARY_COST, m15[0]["time"], NOW):
                trades.append(serialise_trade(config, trade))

        rollsum = rolling_summary(rolling)
        calsum = calendar_summary(calendar)
        decisions = decision_rows(shortlist, costs, rollsum, calsum)

        write_csv(OUTS["shortlist"], shortlist)
        write_csv(OUTS["periods"], periods)
        write_csv(OUTS["cost"], costs)
        write_csv(OUTS["rolling"], rolling)
        write_csv(OUTS["rolling_summary"], rollsum)
        write_csv(OUTS["calendar"], calendar)
        write_csv(OUTS["calendar_summary"], calsum)
        write_csv(OUTS["trades"], trades)
        write_csv(OUTS["decision"], decisions)

        write_csv(OUTS["notes"], [
            {
                "item": "Scope",
                "value": "Fresh EUR/JPY M15 SHORT broad research. No existing short rule or EUR/JPY M15 LONG rule is treated as a benchmark.",
            },
            {
                "item": "Families",
                "value": "Bearish engulf at resistance, high-sweep displacement, failed breakout rejection, outside reversal, compression breakdown, and rally rejection are searched independently.",
            },
            {
                "item": "Historical execution",
                "value": "OANDA midpoint; reference entry signal close; short fill=close-1 pip baseline; stop=signal high+10 ticks; target from reference-close risk; p0; exact exit-candle signal eligible.",
            },
            {
                "item": "HTF causality",
                "value": "H1/H4/D values become usable only at the next actual HTF candle open; no same-candle lookahead.",
            },
            {
                "item": "Context search",
                "value": "Stage 2 tests one context at a time: H1/H4/D bearish trend states, ATR regime, NY/London/Tokyo 4-hour blocks, and weekday exclusions.",
            },
            {
                "item": "Cost stress",
                "value": "Final shortlist is retested at 0.5, 1.0, 1.5 and 2.0 pip adverse historical entry cost.",
            },
            {
                "item": "No pristine OOS claim",
                "value": "History has been repeatedly explored. Temporal splits and rolling windows are robustness diagnostics, not untouched out-of-sample evidence.",
            },
            {
                "item": "Decision matrix",
                "value": "DEEP_VALIDATE only means a candidate merits a separate frozen deep-validation runner. It is not live approval.",
            },
            {
                "item": "Next gate",
                "value": "After one candidate is frozen and deeply validated, run exact 23->24 portfolio-add analysis with non-hedging same-pair overlap handling before any live integration.",
            },
        ])

        STATUS.update({
            "state": "packaging",
            "message": "Packaging EUR/JPY M15 SHORT #24 broad research results",
            "progress": 97,
        })

        package_results()

        verdict_counts = defaultdict(int)
        for row in decisions:
            verdict_counts[row["research_verdict"]] += 1

        STATUS.update({
            "state": "complete",
            "message": "EUR/JPY M15 SHORT #24 broad research complete",
            "progress": 100,
            "stage1_configs": len(stage1),
            "stage2_configs": len(stage2),
            "stage3_configs": len(stage3),
            "shortlist_configs": len(final_configs),
            "decision_counts": dict(verdict_counts),
            "bundle": BUNDLE,
        })

    except Exception as error:
        STATUS.update({
            "state": "error",
            "message": str(error),
        })
        print("ERROR:", repr(error), flush=True)




# ============================================================
# FROZEN DEEP-VALIDATION STUDY
# ============================================================
#
# Basis: the completed broad-search results.
#
# Candidate A (main):
#   RALLY_REJECTION
#   sweep previous 40-bar high
#   close back below previous 10-bar high
#   bearish body >= 0.75 ATR14
#   prior 4-hour M15 momentum >= +1.25 ATR14
#   close location <= 0.30
#   include 16:00-19:59 America/New_York
#   RR 4.75
#
# Candidate B (sparse complement):
#   HIGH_SWEEP_DISPLACEMENT
#   sweep previous 60-bar high
#   close below previous candle low
#   bearish body >= 1.00 ATR14
#   upper wick/body >= 0.35
#   prior 4-hour M15 momentum >= +1.00 ATR14
#   previous strictly completed H1 close < H1 EMA100
#   RR 3.00
#
# This runner is NOT another broad optimiser. It performs controlled
# neighbourhood/stability tests around A and B, then tests A+B with exact
# p0 overlap handling. It does not auto-deploy or send orders.
# ============================================================

OUTS = {
    "coverage": "eurjpy_m15_short_24_deep_coverage.csv",
    "parity": "eurjpy_m15_short_24_deep_parity.csv",
    "local_summary": "eurjpy_m15_short_24_deep_local_summary.csv",
    "rr_plateau": "eurjpy_m15_short_24_deep_rr_plateau.csv",
    "session_stability": "eurjpy_m15_short_24_deep_session_stability.csv",
    "hour_diagnostic": "eurjpy_m15_short_24_deep_hour_diagnostic.csv",
    "weekday_diagnostic": "eurjpy_m15_short_24_deep_weekday_diagnostic.csv",
    "geometry_rr": "eurjpy_m15_short_24_deep_geometry_rr_matrix.csv",
    "candidate_b": "eurjpy_m15_short_24_deep_candidate_b_robustness.csv",
    "primary_summary": "eurjpy_m15_short_24_deep_primary_summary.csv",
    "periods": "eurjpy_m15_short_24_deep_periods.csv",
    "cost": "eurjpy_m15_short_24_deep_cost_stress.csv",
    "rolling": "eurjpy_m15_short_24_deep_rolling.csv",
    "rolling_summary": "eurjpy_m15_short_24_deep_rolling_summary.csv",
    "calendar": "eurjpy_m15_short_24_deep_calendar_years.csv",
    "calendar_summary": "eurjpy_m15_short_24_deep_calendar_summary.csv",
    "trades": "eurjpy_m15_short_24_deep_trades.csv",
    "overlap": "eurjpy_m15_short_24_deep_overlap.csv",
    "combined_rejections": "eurjpy_m15_short_24_deep_combined_rejections.csv",
    "decision": "eurjpy_m15_short_24_deep_decision_matrix.csv",
    "notes": "eurjpy_m15_short_24_deep_notes.csv",
}
BUNDLE = "EURJPY_M15_SHORT_24_DEEP_VALIDATION_RESULTS.zip"

STATUS = {
    "state": "not_started",
    "message": "Not started",
    "progress": 0,
    "orders_supported": False,
    "trading_enabled": False,
}


def a_cfg(
    config_id,
    rr=4.75,
    sweep_lb=40,
    body_atr_min=0.75,
    mom4_min=1.25,
    close_loc_max=0.30,
    context="NY_BLOCK_16-19",
    excluded_weekdays=None,
    test_group="A_CONTROL",
):
    return cfg(
        config_id,
        "RALLY_REJECTION",
        rr=rr,
        sweep_lb=sweep_lb,
        body_atr_min=body_atr_min,
        mom4_min=mom4_min,
        close_loc_max=close_loc_max,
        context=context,
        excluded_weekdays=set(excluded_weekdays or set()),
        test_group=test_group,
    )


def b_cfg(
    config_id,
    rr=3.00,
    sweep_lb=60,
    body_atr_min=1.00,
    upper_wick_body_min=0.35,
    mom4_min=1.00,
    context="H1_CLOSE_LT_EMA100",
    test_group="B_CONTROL",
):
    return cfg(
        config_id,
        "HIGH_SWEEP_DISPLACEMENT",
        rr=rr,
        sweep_lb=sweep_lb,
        body_atr_min=body_atr_min,
        upper_wick_body_min=upper_wick_body_min,
        mom4_min=mom4_min,
        context=context,
        test_group=test_group,
    )


A_CONTROL_ID = "A_CONTROL_RALLY_RR475"
B_CONTROL_ID = "B_CONTROL_HIGHSWEEP_RR300"


def fixed_deep_configs():
    """
    Controlled, predeclared neighbourhoods only.
    No automatic selection is performed inside the runner.
    """
    configs = []

    # ---------------- Candidate A control + RR plateau ----------------
    for rr in [4.50, 4.75, 5.00, 5.25]:
        cid = A_CONTROL_ID if abs(rr - 4.75) < 1e-12 else f"A_RR_{rr:.2f}"
        configs.append(a_cfg(
            cid,
            rr=rr,
            test_group="A_RR_PLATEAU",
        ))

    # ---------------- Session-boundary stability ----------------
    # These deliberately include the known 16-19 control plus neighbouring
    # blocks on both sides. The runner reports them; it does NOT cherry-pick.
    for context in [
        "NY_BLOCK_15-19",
        "NY_BLOCK_16-19",
        "NY_BLOCK_17-19",
        "NY_BLOCK_16-20",
        "NY_BLOCK_17-20",
        "NY_BLOCK_15-20",
        "NY_BLOCK_16-18",
    ]:
        cid = "A_SESSION_" + context.replace("NY_BLOCK_", "").replace("-", "_")
        if context == "NY_BLOCK_16-19":
            cid = "A_SESSION_CONTROL_16_19"
        configs.append(a_cfg(
            cid,
            context=context,
            test_group="A_SESSION_STABILITY",
        ))

    # ---------------- Per-hour diagnostic ----------------
    for hour in [15, 16, 17, 18, 19, 20]:
        configs.append(a_cfg(
            f"A_HOUR_{hour:02d}",
            context=f"NY_BLOCK_{hour:02d}-{hour:02d}",
            test_group="A_HOUR_DIAGNOSTIC",
        ))

    # ---------------- Weekday exclusion diagnostics ----------------
    for weekday, name in [
        (0, "MON"),
        (1, "TUE"),
        (2, "WED"),
        (3, "THU"),
        (4, "FRI"),
    ]:
        configs.append(a_cfg(
            f"A_EXCL_{name}",
            excluded_weekdays={weekday},
            test_group="A_WEEKDAY_DIAGNOSTIC",
        ))

    # ---------------- Geometry x RR one-factor interactions ----------------
    rr_grid = [4.50, 4.75, 5.00, 5.25]

    for body in [0.65, 0.75, 0.85]:
        for rr in rr_grid:
            configs.append(a_cfg(
                f"A_GEO_BODY_{body:.2f}_RR_{rr:.2f}",
                rr=rr,
                body_atr_min=body,
                test_group="A_GEOMETRY_RR",
            ))

    for mom in [1.00, 1.25, 1.50]:
        for rr in rr_grid:
            configs.append(a_cfg(
                f"A_GEO_MOM_{mom:.2f}_RR_{rr:.2f}",
                rr=rr,
                mom4_min=mom,
                test_group="A_GEOMETRY_RR",
            ))

    for close_max in [0.25, 0.30, 0.35]:
        for rr in rr_grid:
            configs.append(a_cfg(
                f"A_GEO_CLOSE_{close_max:.2f}_RR_{rr:.2f}",
                rr=rr,
                close_loc_max=close_max,
                test_group="A_GEOMETRY_RR",
            ))

    for lb in [20, 40, 60]:
        for rr in rr_grid:
            configs.append(a_cfg(
                f"A_GEO_SWEEP_{lb}_RR_{rr:.2f}",
                rr=rr,
                sweep_lb=lb,
                test_group="A_GEOMETRY_RR",
            ))

    # ---------------- Candidate B sparse robustness ----------------
    for rr in [2.50, 2.75, 3.00, 3.25, 3.50]:
        cid = B_CONTROL_ID if abs(rr - 3.00) < 1e-12 else f"B_RR_{rr:.2f}"
        configs.append(b_cfg(
            cid,
            rr=rr,
            test_group="B_RR_PLATEAU",
        ))

    for lb in [40, 60, 80]:
        configs.append(b_cfg(
            f"B_SWEEP_{lb}",
            sweep_lb=lb,
            test_group="B_GEOMETRY",
        ))

    for body in [0.90, 1.00, 1.10]:
        configs.append(b_cfg(
            f"B_BODY_{body:.2f}",
            body_atr_min=body,
            test_group="B_GEOMETRY",
        ))

    for wick in [0.25, 0.35, 0.45]:
        configs.append(b_cfg(
            f"B_WICK_{wick:.2f}",
            upper_wick_body_min=wick,
            test_group="B_GEOMETRY",
        ))

    for mom in [0.75, 1.00, 1.25]:
        configs.append(b_cfg(
            f"B_MOM_{mom:.2f}",
            mom4_min=mom,
            test_group="B_GEOMETRY",
        ))

    for context in [
        "NONE",
        "H1_CLOSE_LT_EMA100",
        "H1_CLOSE_LT_EMA200",
        "H1_EMA50_LT_EMA200",
    ]:
        configs.append(b_cfg(
            "B_CONTEXT_" + context,
            context=context,
            test_group="B_CONTEXT",
        ))

    # Remove exact duplicate signatures while preserving the first descriptive id.
    out = []
    seen = set()
    for c in configs:
        sig = (
            c["family"],
            c["rr"],
            c.get("context"),
            c.get("sweep_lb"),
            c.get("body_atr_min"),
            c.get("upper_wick_body_min"),
            c.get("mom4_min"),
            c.get("close_loc_max"),
            tuple(sorted(c.get("excluded_weekdays", set()))),
        )
        if sig in seen:
            continue
        seen.add(sig)
        out.append(c)

    # Ensure the canonical IDs exist even if a duplicate appeared earlier.
    ids = {x["config_id"] for x in out}
    if A_CONTROL_ID not in ids:
        out.append(a_cfg(A_CONTROL_ID))
    if B_CONTROL_ID not in ids:
        out.append(b_cfg(B_CONTROL_ID))

    return out


def deep_summary_row(config, candles, candidate_indices):
    row = shortlist_summary(config, candles, candidate_indices)
    row["test_group"] = config.get("test_group", "")
    return row


def filter_indices_by_time(candles, candidate_indices, start=None, end=None):
    if start is None and end is None:
        return candidate_indices
    times = [candles[i]["time"] for i in candidate_indices]
    a = 0 if start is None else bisect.bisect_left(times, start)
    b = len(candidate_indices) if end is None else bisect.bisect_left(times, end)
    return candidate_indices[a:b]


def combined_backtest(
    candles,
    ix_a,
    rr_a,
    ix_b,
    rr_b,
    cost_pips,
    start=None,
    end=None,
    priority="A",
    collect_rejections=False,
):
    """
    One-position p0 two-trigger portfolio for the eventual single strategy #24.

    Half-open overlap convention:
        an accepted trade occupies [signal_index, exit_index)
        a signal exactly on the exit candle remains eligible.

    If both triggers fire on the same signal candle, the selected priority wins.
    Candidate A priority is the main diagnostic because B is the complement.
    """
    use_a = filter_indices_by_time(candles, ix_a, start, end)
    use_b = filter_indices_by_time(candles, ix_b, start, end)

    pri = {"A": 0, "B": 1} if priority == "A" else {"B": 0, "A": 1}

    events = (
        [(i, pri["A"], "A", rr_a) for i in use_a]
        + [(i, pri["B"], "B", rr_b) for i in use_b]
    )
    events.sort(key=lambda x: (x[0], x[1]))

    trades = []
    rejected = []
    p = 0

    while p < len(events):
        signal_index = events[p][0]

        # Gather all triggers on this exact candle in declared priority order.
        q = p
        same = []
        while q < len(events) and events[q][0] == signal_index:
            same.append(events[q])
            q += 1

        chosen = None
        chosen_trade = None
        for event in same:
            _, _, trigger, rr = event
            t = outcome(candles, signal_index, rr, cost_pips)
            if t is not None:
                chosen = event
                chosen_trade = dict(t)
                break

        if chosen_trade is None:
            p = q
            continue

        chosen_trigger = chosen[2]
        chosen_trade["trigger"] = chosen_trigger
        chosen_trade["combined_priority"] = priority
        trades.append(chosen_trade)

        # Same-candle second trigger is rejected by the single-strategy p0 state.
        for event in same:
            if event is chosen:
                continue
            if collect_rejections:
                rejected.append({
                    "signal_index": event[0],
                    "signal_time_utc": iso(candles[event[0]]["time"]),
                    "trigger": event[2],
                    "reason": "SAME_CANDLE_LOWER_PRIORITY",
                    "blocking_trigger": chosen_trigger,
                    "blocking_entry_time_utc": chosen_trade["entry_time_utc"],
                    "blocking_exit_time_utc": chosen_trade["exit_time_utc"],
                })

        # Reject every signal strictly inside [entry, exit). A signal on the
        # exact exit candle is intentionally eligible.
        exit_index = chosen_trade["exit_index"]
        p = q
        while p < len(events) and events[p][0] < exit_index:
            if collect_rejections:
                rejected.append({
                    "signal_index": events[p][0],
                    "signal_time_utc": iso(candles[events[p][0]]["time"]),
                    "trigger": events[p][2],
                    "reason": "OPEN_POSITION_P0",
                    "blocking_trigger": chosen_trigger,
                    "blocking_entry_time_utc": chosen_trade["entry_time_utc"],
                    "blocking_exit_time_utc": chosen_trade["exit_time_utc"],
                })
            p += 1

    if collect_rejections:
        return trades, rejected
    return trades


def generic_period_rows(config_id, family, rr_label, trade_fn):
    periods = [
        ("FULL", START, NOW),
        ("PRE_2010", START, datetime(2010, 1, 1, tzinfo=timezone.utc)),
        ("2010_PLUS", datetime(2010, 1, 1, tzinfo=timezone.utc), NOW),
        ("DEV_2002_17", START, datetime(2018, 1, 1, tzinfo=timezone.utc)),
        ("VALIDATION_2018_PLUS", datetime(2018, 1, 1, tzinfo=timezone.utc), NOW),
        *ERAS,
        ("LAST_5Y", NOW - timedelta(days=365.2425 * 5), NOW),
        ("LAST_2Y", NOW - timedelta(days=365.2425 * 2), NOW),
        ("LAST_1Y", NOW - timedelta(days=365.2425), NOW),
    ]
    rows = []
    for label, a, b in periods:
        s = stats(trade_fn(PRIMARY_COST, a, b))
        rows.append({
            "config_id": config_id,
            "family": family,
            "rr": rr_label,
            "period": label,
            **{
                key: round(value, 6) if isinstance(value, float) else value
                for key, value in s.items()
            },
            "start_utc": iso(a),
            "end_utc": iso(b),
        })
    return rows


def generic_cost_rows(config_id, family, rr_label, trade_fn):
    rows = []
    for cost in COSTS:
        for label, a, b in [
            ("FULL", START, NOW),
            ("VALIDATION_2018_PLUS", datetime(2018, 1, 1, tzinfo=timezone.utc), NOW),
            ("LAST_5Y", NOW - timedelta(days=365.2425 * 5), NOW),
            ("LAST_2Y", NOW - timedelta(days=365.2425 * 2), NOW),
        ]:
            s = stats(trade_fn(cost, a, b))
            rows.append({
                "config_id": config_id,
                "family": family,
                "rr": rr_label,
                "period": label,
                "cost_pips": cost,
                **{
                    key: round(value, 6) if isinstance(value, float) else value
                    for key, value in s.items()
                },
            })
    return rows


def generic_rolling_rows(config_id, trade_fn):
    rows = []
    first = month_floor(START)
    last = month_floor(NOW)

    for months in [12, 24, 36]:
        s = first
        while add_months(s, months) <= last:
            e = add_months(s, months)
            st = stats(trade_fn(PRIMARY_COST, s, e))
            rows.append({
                "config_id": config_id,
                "months": months,
                "start_utc": iso(s),
                "end_utc": iso(e),
                "trades": st["trades"],
                "profit_factor": round(st["profit_factor"], 6),
                "total_r": round(st["total_r"], 4),
                "positive": st["total_r"] > 0,
                "zero_trade": st["trades"] == 0,
            })
            s = add_months(s, 1)

    return rows


def generic_calendar_rows(config_id, trade_fn):
    rows = []
    for year in range(START.year, NOW.year):
        a = datetime(year, 1, 1, tzinfo=timezone.utc)
        b = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        st = stats(trade_fn(PRIMARY_COST, a, b))
        rows.append({
            "config_id": config_id,
            "year": year,
            "trades": st["trades"],
            "profit_factor": round(st["profit_factor"], 6),
            "total_r": round(st["total_r"], 4),
            "positive": st["total_r"] > 0,
            "negative": st["total_r"] < 0,
            "zero_trade": st["trades"] == 0,
        })
    return rows


def primary_summary_from_periods(config_id, family, rr_label, period_rows_):
    by = {r["period"]: r for r in period_rows_}
    eras = [
        by.get("ERA_2002_07", {}),
        by.get("ERA_2008_13", {}),
        by.get("ERA_2014_19", {}),
        by.get("ERA_2020_NOW", {}),
    ]
    full = by["FULL"]
    return {
        "config_id": config_id,
        "family": family,
        "rr": rr_label,
        "full_trades": full["trades"],
        "full_pf": full["profit_factor"],
        "full_r": full["total_r"],
        "full_exp": full["expectancy_r"],
        "full_dd": full["max_drawdown_r"],
        "full_win_rate": full["win_rate"],
        "validation2018_plus_trades": by["VALIDATION_2018_PLUS"]["trades"],
        "validation2018_plus_pf": by["VALIDATION_2018_PLUS"]["profit_factor"],
        "validation2018_plus_r": by["VALIDATION_2018_PLUS"]["total_r"],
        "era2020_plus_trades": by["ERA_2020_NOW"]["trades"],
        "era2020_plus_pf": by["ERA_2020_NOW"]["profit_factor"],
        "era2020_plus_r": by["ERA_2020_NOW"]["total_r"],
        "last5y_trades": by["LAST_5Y"]["trades"],
        "last5y_pf": by["LAST_5Y"]["profit_factor"],
        "last5y_r": by["LAST_5Y"]["total_r"],
        "last2y_trades": by["LAST_2Y"]["trades"],
        "last2y_pf": by["LAST_2Y"]["profit_factor"],
        "last2y_r": by["LAST_2Y"]["total_r"],
        "last1y_trades": by["LAST_1Y"]["trades"],
        "last1y_pf": by["LAST_1Y"]["profit_factor"],
        "last1y_r": by["LAST_1Y"]["total_r"],
        "positive_eras": sum(
            int(x.get("trades", 0) > 0 and x.get("total_r", 0.0) > 0)
            for x in eras
        ),
        "min_active_era_pf": round(min(
            [
                float(x["profit_factor"])
                for x in eras
                if x.get("trades", 0) > 0
            ],
            default=0.0,
        ), 6),
    }


def full_overlap_rows(a_trades, b_trades, combined_a, combined_b):
    a_times = {t["signal_index"] for t in a_trades}
    b_times = {t["signal_index"] for t in b_trades}
    shared = a_times & b_times

    def interval_count(left, right):
        count = 0
        for x in left:
            if any(
                y["signal_index"] < x["exit_index"]
                and x["signal_index"] < y["exit_index"]
                for y in right
            ):
                count += 1
        return count

    return [{
        "a_standalone_trades": len(a_trades),
        "b_standalone_trades": len(b_trades),
        "exact_same_signal_candles": len(shared),
        "a_trades_with_any_b_interval_overlap": interval_count(a_trades, b_trades),
        "b_trades_with_any_a_interval_overlap": interval_count(b_trades, a_trades),
        "combined_a_priority_trades": len(combined_a),
        "combined_b_priority_trades": len(combined_b),
        "combined_a_priority_a_trades": sum(t.get("trigger") == "A" for t in combined_a),
        "combined_a_priority_b_trades": sum(t.get("trigger") == "B" for t in combined_a),
        "combined_b_priority_a_trades": sum(t.get("trigger") == "A" for t in combined_b),
        "combined_b_priority_b_trades": sum(t.get("trigger") == "B" for t in combined_b),
    }]


def deep_decision_rows(primary_summary, costs, rollsum, calsum, local_summary, parity_rows):
    cost_map = {
        (r["config_id"], r["period"], float(r["cost_pips"])): r
        for r in costs
    }
    roll_map = {
        (r["config_id"], int(r["months"])): r
        for r in rollsum
    }
    cal_map = {r["config_id"]: r for r in calsum}
    parity_map = {r["config_id"]: r for r in parity_rows}
    local_map = {r["config_id"]: r for r in local_summary}

    # Session breadth for A: count neighbouring blocks (not the per-hour rows)
    # that remain positive in FULL, 2018+, and last5Y.
    session_rows = [
        r for r in local_summary
        if r.get("test_group") == "A_SESSION_STABILITY"
    ]
    stable_session_count = sum(
        r["full_r"] > 0
        and r["validation2018_plus_r"] > 0
        and r["last5y_r"] > 0
        for r in session_rows
    )

    # Geometry robustness: evaluate the declared one-factor x RR matrix.
    geo_rows = [
        r for r in local_summary
        if r.get("test_group") == "A_GEOMETRY_RR"
    ]
    geo_recent_positive_pct = (
        100.0 * sum(
            r["full_r"] > 0
            and r["validation2018_plus_r"] > 0
            and r["last5y_r"] > 0
            for r in geo_rows
        ) / len(geo_rows)
        if geo_rows else 0.0
    )

    out = []
    a_full_r = next(
        (r["full_r"] for r in primary_summary if r["config_id"] == A_CONTROL_ID),
        0.0,
    )
    a_full_dd = next(
        (r["full_dd"] for r in primary_summary if r["config_id"] == A_CONTROL_ID),
        0.0,
    )

    for row in primary_summary:
        cid = row["config_id"]
        c2 = cost_map.get((cid, "FULL", 2.0), {})
        c3 = cost_map.get((cid, "FULL", 3.0), {})
        c3_val = cost_map.get((cid, "VALIDATION_2018_PLUS", 3.0), {})
        r36 = roll_map.get((cid, 36), {})
        cal = cal_map.get(cid, {})

        if cid == A_CONTROL_ID:
            checks = {
                "parity": parity_map.get(cid, {}).get("status") != "FAIL_BELOW_REFERENCE",
                "full_pf": row["full_pf"] >= 1.25,
                "all_eras_positive": row["positive_eras"] == 4,
                "2018_positive": row["validation2018_plus_r"] > 0,
                "2020_positive": row["era2020_plus_r"] > 0,
                "last5_positive": row["last5y_r"] > 0,
                "last2_positive": row["last2y_r"] > 0,
                "2pip_pf": c2.get("profit_factor", 0.0) >= 1.20,
                "3pip_positive": (
                    c3.get("profit_factor", 0.0) >= 1.10
                    and c3.get("total_r", 0.0) > 0
                    and c3_val.get("total_r", 0.0) > 0
                ),
                "rolling36": r36.get("positive_active_windows_pct", 0.0) >= 70.0,
                "session_breadth": stable_session_count >= 4,
                "geometry_breadth": geo_recent_positive_pct >= 50.0,
            }
            verdict = (
                "FREEZE_CANDIDATE_A"
                if all(checks.values())
                else "A_NEEDS_REVIEW"
            )

        elif cid == B_CONTROL_ID:
            checks = {
                "parity": parity_map.get(cid, {}).get("status") != "FAIL_BELOW_REFERENCE",
                "full_pf": row["full_pf"] >= 1.50,
                "all_eras_positive": row["positive_eras"] == 4,
                "2018_positive": row["validation2018_plus_r"] > 0,
                "last5_positive": row["last5y_r"] > 0,
                "2pip_pf": c2.get("profit_factor", 0.0) >= 1.40,
                "3pip_positive": c3.get("total_r", 0.0) > 0,
            }
            # Sparse by design: never auto-promote B to standalone.
            verdict = (
                "KEEP_AS_SPARSE_COMPLEMENT"
                if all(checks.values())
                else "B_REJECT_OR_REVIEW"
            )

        elif cid == "AB_A_PRIORITY":
            marginal_r = row["full_r"] - a_full_r
            dd_change = row["full_dd"] - a_full_dd
            checks = {
                "adds_r": marginal_r >= 3.0,
                "full_pf": row["full_pf"] >= 1.30,
                "2018_positive": row["validation2018_plus_r"] > 0,
                "2020_positive": row["era2020_plus_r"] > 0,
                "last5_positive": row["last5y_r"] > 0,
                "last2_positive": row["last2y_r"] > 0,
                "3pip_positive": c3.get("total_r", 0.0) > 0,
                "dd_not_materially_worse": dd_change >= -2.0,
                "rolling36": r36.get("positive_active_windows_pct", 0.0) >= 70.0,
            }
            verdict = (
                "PREFERRED_TWO_TRIGGER_CANDIDATE"
                if all(checks.values())
                else "COMBINATION_NEEDS_REVIEW"
            )

        else:  # B-priority diagnostic
            checks = {
                "full_positive": row["full_r"] > 0,
                "2018_positive": row["validation2018_plus_r"] > 0,
            }
            verdict = "PRIORITY_DIAGNOSTIC_ONLY"

        out.append({
            "config_id": cid,
            "family": row["family"],
            "rr": row["rr"],
            "deep_verdict": verdict,
            "checks_passed": sum(bool(x) for x in checks.values()),
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
            "cost_2pip_pf": c2.get("profit_factor", 0.0),
            "cost_2pip_r": c2.get("total_r", 0.0),
            "cost_3pip_pf": c3.get("profit_factor", 0.0),
            "cost_3pip_r": c3.get("total_r", 0.0),
            "cost_3pip_2018_r": c3_val.get("total_r", 0.0),
            "rolling36_positive_active_pct": r36.get("positive_active_windows_pct", 0.0),
            "rolling36_median_r": r36.get("median_r_active", 0.0),
            "rolling36_worst_r": r36.get("worst_r", 0.0),
            "active_calendar_years": cal.get("active_years", 0),
            "zero_trade_years": cal.get("zero_trade_years", 0),
            "positive_active_years_pct": cal.get("positive_active_years_pct", 0.0),
            "a_session_stable_neighbour_count": stable_session_count if cid == A_CONTROL_ID else "",
            "a_geometry_recent_positive_pct": round(geo_recent_positive_pct, 4) if cid == A_CONTROL_ID else "",
            "marginal_r_vs_a": round(row["full_r"] - a_full_r, 4) if cid.startswith("AB_") else "",
            "dd_change_vs_a": round(row["full_dd"] - a_full_dd, 4) if cid.startswith("AB_") else "",
        })

    return out


def run_deep_validation():
    try:
        STATUS.update({
            "state": "loading",
            "message": "Downloading EUR/JPY M15/H1/H4/D history",
            "progress": 2,
        })

        OUTCOME_CACHE.clear()

        # Same market-data conventions as the broad runner.
        m15 = fetch_range(PAIR, "M15", WARMUP, NOW, "M")
        h1 = fetch_range(PAIR, "H1", WARMUP, NOW, "M")
        h4 = fetch_range(PAIR, "H4", WARMUP, NOW, "M")
        daily = fetch_range(PAIR, "D", WARMUP, NOW, "M")

        # Keep study sample at/after the declared start.
        m15 = [x for x in m15 if x["time"] >= START]

        if len(m15) < 1000:
            raise RuntimeError("Insufficient EUR/JPY M15 history returned")

        coverage_rows = [
            {
                "granularity": "M15",
                "candles": len(m15),
                "first_utc": iso(m15[0]["time"]),
                "last_utc": iso(m15[-1]["time"]),
            },
            {
                "granularity": "H1",
                "candles": len(h1),
                "first_utc": iso(h1[0]["time"]) if h1 else "",
                "last_utc": iso(h1[-1]["time"]) if h1 else "",
            },
            {
                "granularity": "H4",
                "candles": len(h4),
                "first_utc": iso(h4[0]["time"]) if h4 else "",
                "last_utc": iso(h4[-1]["time"]) if h4 else "",
            },
            {
                "granularity": "D",
                "candles": len(daily),
                "first_utc": iso(daily[0]["time"]) if daily else "",
                "last_utc": iso(daily[-1]["time"]) if daily else "",
            },
        ]
        write_csv(OUTS["coverage"], coverage_rows)

        STATUS.update({
            "state": "precompute",
            "message": "Building strict completed-HTF alignment and feature cache",
            "progress": 18,
        })

        m15_times = [x["time"] for x in m15]
        aligned_h1 = align_htf(m15_times, htf_state(h1))
        aligned_h4 = align_htf(m15_times, htf_state(h4))
        aligned_daily = align_htf(m15_times, htf_state(daily))
        f = features(m15, aligned_h1, aligned_h4, aligned_daily)

        configs = fixed_deep_configs()
        config_by_id = {c["config_id"]: c for c in configs}

        # Canonical controls must exist under stable ids.
        if A_CONTROL_ID not in config_by_id:
            config_by_id[A_CONTROL_ID] = a_cfg(A_CONTROL_ID)
            configs.append(config_by_id[A_CONTROL_ID])
        if B_CONTROL_ID not in config_by_id:
            config_by_id[B_CONTROL_ID] = b_cfg(B_CONTROL_ID)
            configs.append(config_by_id[B_CONTROL_ID])

        STATUS.update({
            "state": "local_robustness",
            "message": f"Evaluating {len(configs)} frozen local robustness variants",
            "progress": 28,
        })

        local_summary = []
        indices_map = {}

        for i, c in enumerate(configs, 1):
            ix = indices(c, f)
            indices_map[c["config_id"]] = ix
            local_summary.append(deep_summary_row(c, m15, ix))
            if i % 10 == 0 or i == len(configs):
                STATUS.update({
                    "state": "local_robustness",
                    "message": f"Local robustness {i}/{len(configs)}",
                    "progress": 28 + int(28 * i / len(configs)),
                })

        local_summary.sort(
            key=lambda r: (
                r.get("test_group", ""),
                r["config_id"],
            )
        )
        write_csv(OUTS["local_summary"], local_summary)
        write_csv(
            OUTS["rr_plateau"],
            [r for r in local_summary if r.get("test_group") == "A_RR_PLATEAU"],
        )
        write_csv(
            OUTS["session_stability"],
            [r for r in local_summary if r.get("test_group") == "A_SESSION_STABILITY"],
        )
        write_csv(
            OUTS["hour_diagnostic"],
            [r for r in local_summary if r.get("test_group") == "A_HOUR_DIAGNOSTIC"],
        )
        write_csv(
            OUTS["weekday_diagnostic"],
            [r for r in local_summary if r.get("test_group") == "A_WEEKDAY_DIAGNOSTIC"],
        )
        write_csv(
            OUTS["geometry_rr"],
            [r for r in local_summary if r.get("test_group") == "A_GEOMETRY_RR"],
        )
        write_csv(
            OUTS["candidate_b"],
            [r for r in local_summary if str(r.get("test_group", "")).startswith("B_")],
        )

        # ---------------- Broad-search parity guards ----------------
        local_map = {r["config_id"]: r for r in local_summary}
        parity_rows = []
        refs = {
            A_CONTROL_ID: {
                "reference_min_trades": 140,
                "reference_pf_if_equal": 1.412597,
                "reference_r_if_equal": 43.7353,
            },
            B_CONTROL_ID: {
                "reference_min_trades": 29,
                "reference_pf_if_equal": 2.008885,
                "reference_r_if_equal": 17.1510,
            },
        }
        for cid, ref in refs.items():
            row = local_map[cid]
            if row["full_trades"] < ref["reference_min_trades"]:
                status = "FAIL_BELOW_REFERENCE"
            elif row["full_trades"] > ref["reference_min_trades"]:
                status = "PASS_NEWER_TRADES"
            else:
                pf_ok = abs(row["full_pf"] - ref["reference_pf_if_equal"]) <= 0.02
                r_ok = abs(row["full_r"] - ref["reference_r_if_equal"]) <= 0.50
                status = "PASS_EQUAL" if pf_ok and r_ok else "FAIL_METRIC_DRIFT"
            parity_rows.append({
                "config_id": cid,
                **ref,
                "current_trades": row["full_trades"],
                "current_pf": row["full_pf"],
                "current_r": row["full_r"],
                "status": status,
            })

        write_csv(OUTS["parity"], parity_rows)
        bad = [x for x in parity_rows if x["status"].startswith("FAIL")]
        if bad:
            raise RuntimeError(
                "Broad-result control parity failure: "
                + str(bad)
            )

        STATUS.update({
            "state": "primary_diagnostics",
            "message": "Running A, B and A+B deep temporal/cost/rolling diagnostics",
            "progress": 60,
        })

        a = config_by_id[A_CONTROL_ID]
        b = config_by_id[B_CONTROL_ID]
        ix_a = indices_map[A_CONTROL_ID]
        ix_b = indices_map[B_CONTROL_ID]

        def a_trade_fn(cost, start, end):
            return backtest(m15, ix_a, a["rr"], cost, start, end)

        def b_trade_fn(cost, start, end):
            return backtest(m15, ix_b, b["rr"], cost, start, end)

        def ab_a_trade_fn(cost, start, end):
            return combined_backtest(
                m15, ix_a, a["rr"], ix_b, b["rr"], cost,
                start=start, end=end, priority="A",
            )

        def ab_b_trade_fn(cost, start, end):
            return combined_backtest(
                m15, ix_a, a["rr"], ix_b, b["rr"], cost,
                start=start, end=end, priority="B",
            )

        primary_defs = [
            (A_CONTROL_ID, "RALLY_REJECTION", "4.75", a_trade_fn),
            (B_CONTROL_ID, "HIGH_SWEEP_DISPLACEMENT", "3.00", b_trade_fn),
            ("AB_A_PRIORITY", "TWO_TRIGGER_A_PLUS_B", "A4.75+B3.00", ab_a_trade_fn),
            ("AB_B_PRIORITY", "TWO_TRIGGER_A_PLUS_B", "A4.75+B3.00", ab_b_trade_fn),
        ]

        primary_summary = []
        periods_all = []
        costs_all = []
        rolling_all = []
        calendar_all = []
        trades_all = []

        for n, (cid, family, rr_label, trade_fn) in enumerate(primary_defs, 1):
            STATUS.update({
                "state": "primary_diagnostics",
                "message": f"Primary diagnostic {n}/{len(primary_defs)}: {cid}",
                "progress": 60 + int(28 * n / len(primary_defs)),
            })

            prows = generic_period_rows(cid, family, rr_label, trade_fn)
            periods_all.extend(prows)
            primary_summary.append(
                primary_summary_from_periods(cid, family, rr_label, prows)
            )
            costs_all.extend(
                generic_cost_rows(cid, family, rr_label, trade_fn)
            )
            rolling_all.extend(
                generic_rolling_rows(cid, trade_fn)
            )
            calendar_all.extend(
                generic_calendar_rows(cid, trade_fn)
            )

            full_trades = trade_fn(PRIMARY_COST, START, NOW)
            for t in full_trades:
                row = dict(t)
                row.pop("entry_time", None)
                row.pop("exit_time", None)
                row["config_id"] = cid
                row["family"] = family
                trades_all.append(row)

        rollsum = rolling_summary(rolling_all)
        calsum = calendar_summary(calendar_all)

        write_csv(OUTS["primary_summary"], primary_summary)
        write_csv(OUTS["periods"], periods_all)
        write_csv(OUTS["cost"], costs_all)
        write_csv(OUTS["rolling"], rolling_all)
        write_csv(OUTS["rolling_summary"], rollsum)
        write_csv(OUTS["calendar"], calendar_all)
        write_csv(OUTS["calendar_summary"], calsum)
        write_csv(OUTS["trades"], trades_all)

        a_full = a_trade_fn(PRIMARY_COST, START, NOW)
        b_full = b_trade_fn(PRIMARY_COST, START, NOW)
        ab_a_full, rej_a = combined_backtest(
            m15, ix_a, a["rr"], ix_b, b["rr"], PRIMARY_COST,
            start=START, end=NOW, priority="A", collect_rejections=True,
        )
        ab_b_full, rej_b = combined_backtest(
            m15, ix_a, a["rr"], ix_b, b["rr"], PRIMARY_COST,
            start=START, end=NOW, priority="B", collect_rejections=True,
        )

        write_csv(
            OUTS["overlap"],
            full_overlap_rows(a_full, b_full, ab_a_full, ab_b_full),
        )
        write_csv(
            OUTS["combined_rejections"],
            [
                {"combined_mode": "A_PRIORITY", **r}
                for r in rej_a
            ] + [
                {"combined_mode": "B_PRIORITY", **r}
                for r in rej_b
            ],
        )

        decisions = deep_decision_rows(
            primary_summary,
            costs_all,
            rollsum,
            calsum,
            local_summary,
            parity_rows,
        )
        write_csv(OUTS["decision"], decisions)

        write_csv(OUTS["notes"], [
            {
                "item": "Study basis",
                "value": "Frozen deep validation derived from the completed EUR/JPY M15 SHORT #24 broad-search results. No new trigger family search is performed.",
            },
            {
                "item": "Candidate A",
                "value": "Rally rejection: sweep prior40 high; close below prior10 high; bearish body>=0.75 ATR14; prior4h M15 momentum>=+1.25 ATR14; close location<=0.30; include NY16:00-19:59; RR4.75.",
            },
            {
                "item": "Candidate B",
                "value": "Sparse complement: high-sweep displacement; sweep prior60 high; close below previous candle low; body>=1.00 ATR14; upper wick/body>=0.35; prior4h M15 momentum>=+1.00 ATR14; previous strictly completed H1 close<H1 EMA100; RR3.00.",
            },
            {
                "item": "Historical execution",
                "value": "OANDA midpoint; SHORT fill=signal close minus adverse cost; base cost=1 pip; stop=signal high+10 ticks; targets use reference-close risk; p0; exact exit-candle signal eligible.",
            },
            {
                "item": "Session interpretation",
                "value": "NY_BLOCK_a-b means INCLUDE signal candles whose America/New_York opening hour is between a and b inclusive.",
            },
            {
                "item": "Session stability",
                "value": "Neighbouring NY blocks are reported as diagnostics. The runner does not automatically drop hour16 or choose the best block after seeing the results.",
            },
            {
                "item": "Weekday diagnostics",
                "value": "Each weekday exclusion is tested separately around Candidate A, but no weekday exclusion is automatically adopted.",
            },
            {
                "item": "Geometry x RR",
                "value": "One-factor neighbourhoods for body, momentum, close location and sweep lookback are crossed with RR4.50/4.75/5.00/5.25 to test whether the edge sits on a broad plateau.",
            },
            {
                "item": "Cost stress",
                "value": "Primary A/B/A+B variants are stressed at 0.5/1/1.5/2/2.5/3 pip adverse historical entry cost.",
            },
            {
                "item": "Two-trigger overlap",
                "value": "A+B uses one-position p0 with half-open [signal_index,exit_index) overlap rejection. Exact exit-candle signals remain eligible. A-priority is the intended main combination; B-priority is diagnostic.",
            },
            {
                "item": "No pristine OOS claim",
                "value": "The same long history has been repeatedly explored. Temporal/rolling/parameter tests are robustness evidence, not untouched out-of-sample evidence.",
            },
            {
                "item": "Next gate",
                "value": "Only after a frozen A or A+B rule survives this study should we run the exact 23->24 portfolio-add analysis with the live non-hedging gate.",
            },
        ])

        STATUS.update({
            "state": "packaging",
            "message": "Packaging EUR/JPY M15 SHORT #24 deep-validation results",
            "progress": 96,
        })
        package_results()

        STATUS.update({
            "state": "complete",
            "message": "EUR/JPY M15 SHORT #24 deep validation complete",
            "progress": 100,
            "local_variants": len(configs),
            "primary_variants": len(primary_defs),
            "bundle": BUNDLE,
            "decision_verdicts": {
                r["config_id"]: r["deep_verdict"]
                for r in decisions
            },
        })

    except Exception as error:
        STATUS.update({
            "state": "error",
            "message": str(error),
        })
        print("ERROR:", repr(error), flush=True)


# ============================================================
# FLASK ROUTES
# ============================================================

@app.route("/")
def root():
    return jsonify({
        "service": "EURJPY M15 SHORT #24 Deep Validation",
        "status": STATUS["state"],
        "instrument": PAIR,
        "timeframe": "M15",
        "side": "SELL",
        "primary_cost_pips": PRIMARY_COST,
        "stress_costs_pips": COSTS,
        "orders_supported": False,
        "trading_enabled": False,
        "candidate_a": {
            "family": "RALLY_REJECTION",
            "rr": 4.75,
            "ny_hours_included": "16-19",
        },
        "candidate_b": {
            "family": "HIGH_SWEEP_DISPLACEMENT",
            "rr": 3.00,
            "role": "sparse complement",
        },
        "routes": [
            "/eurjpy-m15-short-24-deep/status",
            "/eurjpy-m15-short-24-deep/results",
        ],
    })


@app.route("/eurjpy-m15-short-24-deep/status")
def deep_status():
    return jsonify(STATUS)


@app.route("/eurjpy-m15-short-24-deep/results")
def deep_results():
    return download(BUNDLE)


if __name__ == "__main__":
    threading.Thread(
        target=run_deep_validation,
        daemon=True,
    ).start()

    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "5000")),
        debug=False,
    )
