#!/usr/bin/env python3
"""EUR/AUD M15 LONG Stage 3: three NEW, bounded, standalone raw mechanisms.

1. Consecutive multi-bar washout and bullish reclaim.
2. Sweep/reclaim of the COMPLETED Tokyo 09:00-14:59 session low during a
   predeclared London 07:00-13:59 window (one mechanical session definition).
3. M15 pullback and resumption within strictly completed H1/H4 uptrends.

Each family has one broad RAW baseline plus eight within-mechanism geometry
controls; RR3.5, 2-pip assumed fill and 4-pip stress remain fixed. No mixing
with Stage-2 filters; no portfolio test, RR/time optimisation, or orders.
The six Stage-1 raw ledgers must reproduce at the archived candle cutoff.
Every historical period has been inspected previously: no untouched OOS claim.
"""
import os
import csv
import time
import bisect
import zipfile
import threading
import traceback
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median
from pathlib import Path
from zoneinfo import ZoneInfo
import numpy as np
import requests
from flask import Flask, jsonify, send_file

app = Flask(__name__)
TOKEN = os.getenv("OANDA_TOKEN")
BASE = os.getenv("OANDA_API_URL", "https://api-fxtrade.oanda.com").rstrip("/")
PAIR = "EUR_AUD"
START = datetime(2002, 5, 6, 20, tzinfo=timezone.utc)  # request; actual EUR/AUD starts later
NOW = datetime.now(timezone.utc).replace(second=0, microsecond=0)
WARMUP = START - timedelta(days=900)
NY = ZoneInfo("America/New_York")
LONDON = ZoneInfo("Europe/London")
TOKYO = ZoneInfo("Asia/Tokyo")
TICK = 0.00001
PIP = 0.0001
STOP_TICKS = 10
RR_FIXED = 3.50
PRIMARY_COST = 2.0
STRESS_COST = 4.0
MIN_COST_STRESS_TRADES = 50
STATUS = dict(state="not_started", progress=0, message="Waiting to start",
              orders_supported=False, trading_enabled=False)
OUTS = {name: f"euraud_m15_long_stage3_{name}.csv" for name in (
    "coverage", "stage1_raw_parity", "all_mechanisms", "branch_diagnostics",
    "neighbourhood", "anchor_trade_ledgers", "anchor_rolling",
    "anchor_calendar", "session_checks", "methods", "errors")}
BUNDLE = "EURAUD_M15_LONG_STAGE3_ALTERNATIVE_MECHANISMS_RESULTS.zip"

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
# FIRST-HISTORY TOLERANCE & SAFE RESULTS PACKAGING
# ============================================================

def fetch_chunk(granularity, start, end):
    if not TOKEN:
        raise RuntimeError("OANDA_TOKEN is not configured")
    params = dict(price="M", granularity=granularity, smooth="false",
                  **{"from":iso(start),"to":iso(end),"includeFirst":"true"})
    if granularity == "D":
        params.update(dailyAlignment=17, alignmentTimezone="America/New_York")
    response=requests.get(f"{BASE}/v3/instruments/{PAIR}/candles",
                          headers=headers(),params=params,timeout=70)
    if response.status_code >= 400:
        msg=response.text[:400]
        if response.status_code in (400,404) and (
            "no candles" in msg.lower() or "no data" in msg.lower()
            or "no candle" in msg.lower()
        ):
            return []
        response.raise_for_status()
    rows=[]
    for candle in response.json().get("candles",[]):
        if not candle.get("complete",False):continue
        mid=candle["mid"]
        rows.append(dict(time=parse_time(candle["time"]),open=float(mid["o"]),
                         high=float(mid["h"]),low=float(mid["l"]),close=float(mid["c"])))
    return rows


def fetch(granularity,start,end,chunk_days):
    seen={};current=start;chunk=0;had_history=False
    while current<end:
        chunk+=1;nxt=min(current+timedelta(days=chunk_days),end)
        STATUS.update(state="fetch",message=f"{granularity} chunk {chunk}: {iso(current)} → {iso(nxt)}")
        rows=fetch_chunk(granularity,current,nxt)
        if rows:had_history=True
        elif had_history and granularity in ("M15","H1","H4","D"):
            # A short empty span can occur in market closures. Nonempty overlap
            # and history continuity are also exposed in coverage for inspection.
            pass
        for row in rows:seen[row["time"]]=row
        current=nxt
        time.sleep(.04)
    return [seen[t] for t in sorted(seen)]


def zip_outputs():
    with zipfile.ZipFile(BUNDLE,"w",zipfile.ZIP_DEFLATED) as z:
        for name,path in OUTS.items():
            if os.path.isfile(path):z.write(path,arcname=os.path.basename(path))





# ============================================================
# FROZEN STAGE-1 PARITY REFERENCE
# ============================================================
REFERENCE_CUTOFF = parse_time("2026-09-23T09:15:00Z")
REFERENCE_FIRST = parse_time("2004-05-31T20:45:00Z")
REFERENCE_CANDLE_COUNT = 546817
FROZEN_RAW = {'BULL_ENGULF': (20525, '3e1e6eef952794e4cf74b8de125ee168631fc3eba1353094a4325c47d1e1eeb7'), 'FAILED_BREAKDOWN': (17620, '25f9c5876655bc722b49b972b5cdbfb27dca34258c399733dc06ad6e1646b36c'), 'LOW_SWEEP_DISPLACEMENT': (3066, 'cc004354b947994e62cfd24e9852d76321c3c744a81090247a6ddd1bbb7fde10'), 'OUTSIDE_REVERSAL': (16343, '6cc2d7af4b32f7b7947159cb71481228cf3823b8c20999f3ecf8654f25b1097b'), 'COMPRESSION_BREAKOUT': (9905, 'a2b08adbc6df24680839aa95a97d5dd35bf1bb59be311749ff3ef0a0ace18499'), 'PULLBACK_REJECTION': (13202, '8e5128c518960e37725d84e35807fef49fdb44725b541d9f51f41fbe4456152f')}
FROZEN_FIELDS = ['signal_index', 'exit_index', 'entry_time_utc', 'exit_time_utc', 'reference_entry', 'historical_fill', 'stop', 'target', 'result_r', 'exit_reason', 'rr', 'cost_pips']

# Stage-1 replay contract is frozen; Stage-3 mechanism controls were declared after Stage 2, before Stage-3 results.
# Dimension values are not changed during the run.
FAMILIES = ("BULL_ENGULF", "FAILED_BREAKDOWN", "LOW_SWEEP_DISPLACEMENT",
            "OUTSIDE_REVERSAL", "COMPRESSION_BREAKOUT", "PULLBACK_REJECTION")
ANCHORS = {
    "WASHOUT_RECLAIM": (2, .75, .50),
    "TOKYO_LOW_RECLAIM": (0.00, .50, .65),
    "TREND_PULLBACK": ("CLOSE_ABOVE_H1H4_EMA100", "PRIOR_4BAR_TOUCH", .50),
}

def frozen_row_text(t):
    numeric = {"reference_entry", "historical_fill", "stop", "target", "result_r", "rr", "cost_pips"}
    return "|".join(repr(float(t[k])) if k in numeric else str(t[k]) for k in FROZEN_FIELDS) + "\n"


def stage1_raw_parity(candles, f):
    times = [c["time"] for c in candles]
    cutoff_count = bisect.bisect_right(times, REFERENCE_CUTOFF)
    if not times or times[0] != REFERENCE_FIRST or cutoff_count != REFERENCE_CANDLE_COUNT or times[cutoff_count-1] != REFERENCE_CUTOFF:
        raise RuntimeError("Frozen Stage-1 candle coverage differs: first=%s cutoff_count=%s last_at_cutoff=%s" % (
            iso(times[0]) if times else "none", cutoff_count,
            iso(times[cutoff_count-1]) if cutoff_count else "none"))
    old_candles = candles[:cutoff_count]
    rows = []
    raw = raw_masks(f)
    for family in FAMILIES:
        OUTCOME_CACHE.clear(); BACKTEST_CACHE.clear()
        old_indices = np.flatnonzero(raw[family][:cutoff_count]).tolist()
        old_trades = backtest(old_candles, old_indices, RR_FIXED, PRIMARY_COST)
        digest = __import__("hashlib").sha256()
        for t in old_trades: digest.update(frozen_row_text(t).encode("utf-8"))
        expected_n, expected_hash = FROZEN_RAW[family]
        good = len(old_trades) == expected_n and digest.hexdigest() == expected_hash
        rows.append(dict(family=family, first=iso(times[0]), cutoff=iso(REFERENCE_CUTOFF),
                         candle_count=cutoff_count, archived_trades=expected_n,
                         reproduced_trades=len(old_trades), archived_sha256=expected_hash,
                         reproduced_sha256=digest.hexdigest(), status="PASS" if good else "FAIL"))
        if not good:
            write_csv(OUTS["stage1_raw_parity"], rows)
            raise RuntimeError("HARD STOP: Stage-1 raw accepted-trade ledger drift for " + family)
    write_csv(OUTS["stage1_raw_parity"], rows)
    OUTCOME_CACHE.clear(); BACKTEST_CACHE.clear()
    return rows


def raw_masks(f):
    # Must remain byte-for-byte equivalent in behavior to the Stage-1 masks.
    base=f["valid_atr"] & f["bullish"]
    prev_h=np.r_[np.nan,f["high"][:-1]]
    prev_l=np.r_[np.nan,f["low"][:-1]]
    low10=f["prev_low"][10]
    m={
      "BULL_ENGULF":base & f["exact_bull"] & (f["bull_br"]>=1.0),
      "FAILED_BREAKDOWN":base & (f["low"]<low10) & (f["close"]>low10),
      "LOW_SWEEP_DISPLACEMENT":base & (f["low"]<low10) & (f["close"]>prev_h),
      "OUTSIDE_REVERSAL":base & (f["low"]<prev_l) & (f["high"]>prev_h),
      "COMPRESSION_BREAKOUT":base & (f["compression"]<=1.0) & (f["close"]>f["prev_high"][10]),
      "PULLBACK_REJECTION":base & (f["low"]<f["prev_low"][20]) & (f["close"]>low10) & (f["mom4"]<=0),
    }
    for x in m.values():x[:200]=False
    return m



def part(trades,start=None,end=None):
    return [t for t in trades if (start is None or t["entry_time"]>=start)
            and (end is None or t["entry_time"]<end)]


def snapshot(trades):
    ss=stats(trades)
    return dict(trades=ss["trades"], pf=ss["profit_factor"],
                r=ss["total_r"], dd=ss["max_drawdown_r"],
                win_rate=ss["win_rate"],loss_streak=ss["longest_loss_streak"])


def report_row(family,config_id,coords,signals,main,stress):
    row=dict(family=family,config_id=config_id,**coords,signal_count=len(signals),
             **{f"full_{k}":v for k,v in snapshot(main).items()},
             **{f"4pip_full_{k}":v for k,v in snapshot(stress).items()})
    slices=(
      ("before2010",None,datetime(2010,1,1,tzinfo=timezone.utc)),
      ("since2010",datetime(2010,1,1,tzinfo=timezone.utc),None),
      ("since2018",datetime(2018,1,1,tzinfo=timezone.utc),None),
      ("last5y",NOW-timedelta(days=365.2425*5),None),
      ("last2y",NOW-timedelta(days=365.2425*2),None),
      ("last1y",NOW-timedelta(days=365.2425),None),
      ("era2004_2009",None,datetime(2010,1,1,tzinfo=timezone.utc)),
      ("era2010_2015",datetime(2010,1,1,tzinfo=timezone.utc),datetime(2016,1,1,tzinfo=timezone.utc)),
      ("era2016_2021",datetime(2016,1,1,tzinfo=timezone.utc),datetime(2022,1,1,tzinfo=timezone.utc)),
      ("era2022_now",datetime(2022,1,1,tzinfo=timezone.utc),None))
    for label,start,end in slices:
        for prefix, ledger in (("",main),("4pip_",stress)):
            s=stats(part(ledger,start,end))
            for k in ("trades","profit_factor","total_r"):
                row[f"{prefix}{label}_{k}"]=s[k]
    row["reason_not_frozen"]="Exploratory new-entry-mechanism study; no RR/timing/portfolio selection; all history previously examined"
    return row


def rolling_diag(cid,trades):
    rows=[]
    start=datetime(2004,6,1,tzinfo=timezone.utc)
    for months in (12,24,36):
        t=start
        while add_months(t,months)<=datetime(NOW.year,NOW.month,1,tzinfo=timezone.utc):
            e=add_months(t,months)
            ss=stats(part(trades,t,e))
            rows.append(dict(config_id=cid,months=months,from_utc=iso(t),to_utc=iso(e),
                             trades=ss["trades"],total_r=ss["total_r"],
                             positive_active=ss["total_r"]>0 if ss["trades"] else "NO_TRADES"))
            t=add_months(t,1)
    return rows


def calendar_diag(cid,trades):
    rows=[]
    for y in range(2005,NOW.year):
        ss=stats(part(trades,datetime(y,1,1,tzinfo=timezone.utc),
                      datetime(y+1,1,1,tzinfo=timezone.utc)))
        rows.append(dict(config_id=cid,year=y,trades=ss["trades"],
                         total_r=ss["total_r"],profit_factor=ss["profit_factor"]))
    return rows



# ============================================================
# THREE SEPARATE MECHANISMS, FIXED IN ADVANCE
# ============================================================

def shift_prior(a, steps, fill=False):
    """Current index i sees exclusively i-steps, never i or future."""
    if steps <= 0: raise ValueError(steps)
    return np.r_[np.full(steps, fill, dtype=a.dtype), a[:-steps]]


def completed_tokyo_low(candles):
    """Low of Tokyo 09:00:00–14:59:59 on *same Tokyo date*, usable >=15:00.

    Output is NaN when fewer than 16 M15 session bars exist, outside the
    fixed London 07:00–13:59 window, or at/before session completion. London
    window is part of the distinct session mechanism, NOT a searched filter.
    """
    sessions={}
    for candle in candles:
        dt=candle["time"].astimezone(TOKYO)
        if 9 <= dt.hour < 15:
            day=dt.date()
            old=sessions.get(day)
            if old is None: sessions[day]=(candle["low"], 1)
            else: sessions[day]=(min(old[0], candle["low"]), old[1]+1)
    low=np.full(len(candles),np.nan)
    checks=[]
    for i,candle in enumerate(candles):
        when=candle["time"]
        london=when.astimezone(LONDON)
        tokyo=when.astimezone(TOKYO)
        if not (7 <= london.hour < 14 and tokyo.hour >= 15): continue
        session=sessions.get(tokyo.date())
        if session is None or session[1] < 16: continue
        low[i]=session[0]
    for day,(price,n) in sorted(sessions.items()):
        if len(checks)<6: checks.append(dict(tokyo_date=day,session_m15_count=n,
                                              completed_low=price))
    return low, checks


def mechanism_masks(f, candles):
    """All 3 raw variants + 24 predeclared nested controls; NO cross-family combos."""
    n=f["n"]
    cl=f["close"]; op=f["open"]; hi=f["high"]; lo=f["low"]
    a=f["atr"]; positive=f["valid_atr"] & f["bullish"]
    prior_high=shift_prior(hi,1,fill=np.nan)
    masks=[]
    def add(name, axes, coords, mask, raw=False):
        mask=np.asarray(mask & positive,dtype=bool)
        mask[:200]=False  # identical warmup exclusion to Stages 1/2
        tag=("RAW" if raw else "CONTROL")
        cid=name+"__"+tag+"__"+"__".join(
            f"{key}{str(val).replace('.','p')}" for key,val in zip(axes,coords))
        masks.append((name,cid,dict(zip(axes,coords)),mask,raw))

    # A. Consecutive bearish candles ending immediately before signal, true
    # close-to-close washout, then current bullish CLOSE > last candle HIGH.
    bearish=cl<op
    wash={}
    for k in (2,3):
        consecutive=np.ones(n,dtype=bool)
        for j in range(1,k+1): consecutive &= shift_prior(bearish,j)
        decline=np.full(n,np.nan)
        decline[k+1:] = (cl[:-(k+1)]-cl[k:-1])/a[k+1:]
        wash[k]=(consecutive & (cl>prior_high),decline)
    wash_axes=("bearish_bars","prior_decline_atr","signal_body_atr")
    add("WASHOUT_RECLAIM",wash_axes,(2,0.,0.),wash[2][0],raw=True)
    for k in (2,3):
        for fall in (.75,1.25):
            for body in (.50,1.00):
                base,decline=wash[k]
                add("WASHOUT_RECLAIM",wash_axes,(k,fall,body),
                    base & (decline>=fall) & (f["body_atr"]>=body))

    # B. Closed Tokyo SESSION low (not rolling M15 low). London 07-13 local
    # is a single fixed observation window, not a session-hour sweep.
    asian_low, session_checks=completed_tokyo_low(candles)
    depth=(asian_low-lo)/a
    session_base=(np.isfinite(asian_low) & (lo<asian_low) & (cl>asian_low))
    session_axes=("penetration_atr","signal_body_atr","close_location")
    add("TOKYO_LOW_RECLAIM",session_axes,(0.,0.,0.),session_base,raw=True)
    for penetration in (0.,.10):
        for body in (.50,1.00):
            for close_min in (.65,.80):
                add("TOKYO_LOW_RECLAIM",session_axes,(penetration,body,close_min),
                    session_base & (depth>=penetration)
                    & (f["body_atr"]>=body) & (f["close_loc"]>=close_min))

    # C. Completed H1/H4 trend and M15 pullback to previous completed EMA20.
    # Both HTF arrays were aligned with completed-bar timestamps by features().
    ema20=ema(cl,20)
    prev_ema20=shift_prior(ema20,1,fill=np.nan)
    prev_close=shift_prior(cl,1,fill=np.nan)
    prior4_low=prev_extreme(lo,4,"min")
    htf_raw=(f["h1_close"]>f["h1_ema100"]) & (f["h4_close"]>f["h4_ema100"])
    htf_strict=htf_raw & (f["h1_ema50"]>f["h1_ema200"])
    # Strict context still requires completed H1+H4; it is a nested condition.
    broad=(np.isfinite(prev_ema20) & (cl>prior_high) & (cl>prev_ema20))
    touches={
       "PRIOR_4BAR_TOUCH": prior4_low<=prev_ema20,
       "PRIOR_CLOSE_BELOW": prev_close<prev_ema20,
    }
    trends={
       "CLOSE_ABOVE_H1H4_EMA100": htf_raw,
       "H1_EMA50_GT_200": htf_strict,
    }
    trend_axes=("htf_trend","pullback","signal_body_atr")
    add("TREND_PULLBACK",trend_axes,
        ("CLOSE_ABOVE_H1H4_EMA100","PRIOR_4BAR_TOUCH",0.),
        broad & htf_raw & touches["PRIOR_4BAR_TOUCH"],raw=True)
    for trend,htf_ok in trends.items():
        for touch,touch_mask in touches.items():
            for body in (.50,1.00):
                add("TREND_PULLBACK",trend_axes,(trend,touch,body),
                    broad & htf_ok & touch_mask & (f["body_atr"]>=body))

    assert len(masks)==27,len(masks)
    assert sum(x[4] for x in masks)==3
    assert len({x[1] for x in masks})==27
    return masks,session_checks


def neighbourhood(rows):
    """Every one-axis adjacent pair; no automated ranking or selection."""
    groups=defaultdict(list)
    for r in rows: groups[r["family"]].append(r)
    out=[]
    axes={
      "WASHOUT_RECLAIM": ("bearish_bars","prior_decline_atr","signal_body_atr"),
      "TOKYO_LOW_RECLAIM": ("penetration_atr","signal_body_atr","close_location"),
      "TREND_PULLBACK": ("htf_trend","pullback","signal_body_atr"),
    }
    for family,items in groups.items():
        keys=axes[family]
        levels={a:list(dict.fromkeys(r[a] for r in items)) for a in keys}
        vals={tuple(r[k] for k in keys):r for r in items if not r["is_raw"]}
        for coord,r in vals.items():
            for axis_idx,axis in enumerate(keys):
                values=levels[axis]
                i=values.index(r[axis])
                if i+1>=len(values): continue
                next_coord=list(coord);next_coord[axis_idx]=values[i+1]
                next_r=vals.get(tuple(next_coord))
                if not next_r:continue
                out.append(dict(family=family,axis=axis,from_value=r[axis],
                                to_value=next_r[axis],from_id=r["config_id"],
                                to_id=next_r["config_id"],
                                from_trades=r["full_trades"],to_trades=next_r["full_trades"],
                                delta_2pip_r=next_r["full_r"]-r["full_r"],
                                delta_4pip_r=next_r["4pip_full_r"]-r["4pip_full_r"],
                                delta_last2y_4pip_r=next_r["4pip_last2y_total_r"]-
                                    r["4pip_last2y_total_r"]))
    return out


def branch_diagnostics(rows):
    out=[]
    for family in ("WASHOUT_RECLAIM","TOKYO_LOW_RECLAIM","TREND_PULLBACK"):
        items=[r for r in rows if r["family"]==family]
        raw=next(r for r in items if r["is_raw"])
        kept=[r for r in items if r["full_trades"]>=50 and not r["is_raw"]]
        out.append(dict(family=family,total_settings=len(items),raw_trades=raw["full_trades"],
                        raw_2pip_r=raw["full_r"],raw_4pip_r=raw["4pip_full_r"],
                        controls_50trades=len(kept),positive_2pip=sum(x["full_r"]>0 for x in kept),
                        positive_4pip=sum(x["4pip_full_r"]>0 for x in kept),
                        early_late_positive_4pip=sum(x["4pip_before2010_total_r"]>0
                           and x["4pip_since2010_total_r"]>0 for x in kept),
                        note="Diagnostics only. NO auto-winner and no untouched OOS."))
    return out


def hist_coverage(label,candles):
    if not candles: raise RuntimeError("No candles for "+label)
    tt=[r["time"] for r in candles]
    max_gap=max(((b-a).total_seconds()/86400 for a,b in zip(tt,tt[1:])),default=0.)
    return dict(timeframe=label,count=len(tt),first_utc=iso(tt[0]),
                last_utc=iso(tt[-1]),max_gap_days=max_gap)


def run_research():
    try:
        STATUS.update(state="fetch",progress=1,message="Fetching full EUR/AUD M15 history")
        m15=fetch("M15",START,NOW,35)
        if len(m15)<100000:raise RuntimeError(f"Insufficient EUR/AUD M15 history: {len(m15)}")
        STATUS.update(state="fetch",progress=17,message="Fetching completed H1/H4/D history")
        h1=fetch("H1",WARMUP,NOW,180)
        h4=fetch("H4",WARMUP,NOW,700)
        daily=fetch("D",WARMUP,NOW,3500)
        if not (h1 and h4 and daily):raise RuntimeError("Missing HTF history")
        write_csv(OUTS["coverage"],[hist_coverage(name,cd) for name,cd in
                  (("M15",m15),("H1",h1),("H4",h4),("D",daily))])
        STATUS.update(state="features",progress=30,message="Aligning strictly completed higher timeframes")
        tt=[c["time"] for c in m15]
        f=features(m15,align_htf(tt,htf_state(h1)),align_htf(tt,htf_state(h4)),
                   align_htf(tt,htf_state(daily)))
        STATUS.update(state="parity",progress=37,message="Stage-1 six-raw-ledger field fingerprints")
        stage1_raw_parity(m15,f)
        STATUS.update(state="study",progress=43,message="3 raw mechanisms + 24 predeclared within-family controls")
        hypotheses,checks=mechanism_masks(f,m15)
        write_csv(OUTS["session_checks"],checks)
        rows=[];ledgers=[];roll=[];cal=[]
        for j,(family,cid,coords,mask,is_raw) in enumerate(hypotheses):
            ix=np.flatnonzero(mask).tolist()
            t2=backtest(m15,ix,RR_FIXED,PRIMARY_COST)
            t4=backtest(m15,ix,RR_FIXED,STRESS_COST)
            row=report_row(family,cid,coords,ix,t2,t4)
            row["is_raw"]=is_raw
            rows.append(row)
            if is_raw or tuple(coords.values())==ANCHORS[family]:
                for cost,ledger in (("2pip",t2),("4pip",t4)):
                    label=cid+"__"+cost
                    for tr in ledger:
                        ledgers.append(dict(config_id=cid,fill_assumption=cost,
                           **{k:v for k,v in tr.items() if k not in ("entry_time","exit_time")}))
                    roll.extend(rolling_diag(label,ledger))
                    cal.extend(calendar_diag(label,ledger))
            OUTCOME_CACHE.clear(); BACKTEST_CACHE.clear()
            write_csv(OUTS["all_mechanisms"],rows)  # save every 1/27 for resilience
            STATUS.update(progress=43+int(50*(j+1)/len(hypotheses)),
                           message=f"Mechanism control {j+1}/{len(hypotheses)}")
        write_csv(OUTS["branch_diagnostics"],branch_diagnostics(rows))
        write_csv(OUTS["neighbourhood"],neighbourhood(rows))
        write_csv(OUTS["anchor_trade_ledgers"],ledgers)
        write_csv(OUTS["anchor_rolling"],roll)
        write_csv(OUTS["anchor_calendar"],cal)
        write_csv(OUTS["methods"],[
          dict(topic="scope",detail="Standalone EUR/AUD M15 LONG new entry mechanisms; Portfolio27 NOT accessed"),
          dict(topic="families",detail="Raw + 8 controls for EACH of washout, completed Tokyo low, HTF pullback: 27 total"),
          dict(topic="fills",detail="Fixed RR3.5, 2pip adverse reference-close BUY, 4pip stress; NOT historical measured spreads"),
          dict(topic="stop",detail="Signal low minus 10 ticks; target 3.5 x reference-close risk; actual fill risk P/L; half-open p0"),
          dict(topic="sessions",detail="Tokyo local 09:00-14:59 ONLY; completed >=15 Tokyo; London 07:00-13:59 ONLY; >=16 bars; predeclared mechanism, no hour search"),
          dict(topic="htf",detail="H1/H4 complete only by next ACTUAL HTF candle open; EMA20 M15 uses prior candle"),
          dict(topic="parity",detail="Six archived Stage-1 raw ledgers and 546817 M15 bars at fixed cutoff must match EXACTLY"),
          dict(topic="independence",detail="Entire historical dataset has been repeatedly examined; no untouched OOS"),
          dict(topic="selection",detail="No RR, hour/day, Stage-2-filter or Portfolio27 tuning; no auto winner"),
          dict(topic="runtime",detail="All 27 controls predeclared; low concurrency; CSV checkpoints every setting")])
        zip_outputs()
        STATUS.update(state="complete",progress=100,stage1_parity="PASS",
                      mechanism_rows=len(rows),anchor_ledger_rows=len(ledgers),
                      result_path="/euraud-m15-long-stage3/results",
                      message="EUR/AUD M15 LONG alternative mechanism study complete")
    except Exception as ex:
        STATUS.update(state="error",message=str(ex),traceback=traceback.format_exc(),
                      orders_supported=False,trading_enabled=False)
        try:
            write_csv(OUTS["errors"],[dict(message=str(ex),traceback=STATUS["traceback"],
                                         progress=STATUS.get("progress"),state=STATUS["state"])])
            zip_outputs()
        finally: print(STATUS["traceback"],flush=True)


@app.route("/")
def root():
    return jsonify(service="EUR/AUD M15 LONG Stage 3; standalone alternative-entry research",
                   state=STATUS["state"],status="/euraud-m15-long-stage3/status",
                   results="/euraud-m15-long-stage3/results",
                   orders_supported=False,trading_enabled=False)


@app.route("/euraud-m15-long-stage3/status")
def status_route(): return jsonify(STATUS)


@app.route("/euraud-m15-long-stage3/results")
def results_route(): return download(BUNDLE)


if __name__=="__main__":
    threading.Thread(target=run_research,daemon=True).start()
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","8080")),debug=False)
   
