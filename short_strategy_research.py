#!/usr/bin/env python3
"""AUD/JPY M15 LONG — Stage 1 of a standalone, exploratory six-family edge study.

Never imports or edits Portfolio 27. No orders and no portfolio performance
information is loaded. Raw mechanisms are tested individually, followed by
one factor at a time on each raw family. No RR optimisation, no auto-winner,
no multi-filter combinations, and no session-conditioned candidate selection.

This is newly tested AUD/JPY data, but all inspected historical periods are
research diagnostics, NOT independent out-of-sample validation. Existing
portfolio instruments/periods have been inspected during prior research.

Market data: OANDA midpoint complete M15/H1/H4/D candles. ATR14 Wilder,
seeded with SMA; strict past-only extrema; previous completed 16 M15 bars for
prior 4h momentum. HTF state only used from the next ACTUAL HTF candle open.

Historical BUY: signal CLOSE + assumed 2-pip adverse fill (4-pip stress).
Stop = signal LOW - 10 ticks. TP = signal close + 3.5 x reference-close risk.
R uses actual fill-to-stop risk. Exit starts the next M15 bar. If TP and stop
are both touched, high-first only if the bar's high is strictly closer to
its open; tie -> stop. Pyramiding zero; exit-candle signal is eligible.

Neither adverse fill assumption is a measurement of historical bid/ask spread;
the model cannot reproduce tick-level intrabar order or overnight gaps.
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
PAIR = "AUD_JPY"
START = datetime(2002, 5, 6, 20, tzinfo=timezone.utc)  # request; actual first AUD/JPY candle reported
NOW = datetime.now(timezone.utc).replace(second=0, microsecond=0)
WARMUP = START - timedelta(days=900)
NY = ZoneInfo("America/New_York")
LONDON = ZoneInfo("Europe/London")
TOKYO = ZoneInfo("Asia/Tokyo")
# JPY quote precision: 1 pip = 0.01 JPY; 1 pricing tick = 0.001 JPY.
TICK = 0.001
PIP = 0.01
STOP_TICKS = 10
RR_FIXED = 3.50
PRIMARY_COST = 2.0
STRESS_COST = 4.0
MIN_COST_STRESS_TRADES = 50
STATUS = dict(state="not_started", progress=0, message="Waiting to start",
              orders_supported=False, trading_enabled=False)
OUTS = {name:f"audjpy_m15_long_stage1_{name}.csv" for name in (
    "coverage","raw_families","single_factors","raw_trade_ledgers",
    "raw_cost_stress","raw_rolling","raw_calendar","methods","errors")}
BUNDLE="AUDJPY_M15_LONG_STAGE1_RAW_EDGE_DISCOVERY_RESULTS.zip"

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
# SIX MINIMAL ENTRY MECHANISMS, NOT PRE-OPTIMISED STRATEGIES
# ============================================================
FAMILIES=("BULL_ENGULF","FAILED_BREAKDOWN","LOW_SWEEP_DISPLACEMENT",
          "OUTSIDE_REVERSAL","COMPRESSION_BREAKOUT","PULLBACK_REJECTION")


def raw_masks(f):
    base=f["valid_atr"] & f["bullish"]
    previous_high=np.r_[np.nan,f["high"][:-1]]
    previous_low=np.r_[np.nan,f["low"][:-1]]
    low10=f["prev_low"][10]
    masks={
      "BULL_ENGULF":base & f["exact_bull"] & (f["bull_br"]>=1.0),
      "FAILED_BREAKDOWN":base & (f["low"]<low10) & (f["close"]>low10),
      "LOW_SWEEP_DISPLACEMENT":base & (f["low"]<low10) & (f["close"]>previous_high),
      "OUTSIDE_REVERSAL":base & (f["low"]<previous_low) & (f["high"]>previous_high),
      "COMPRESSION_BREAKOUT":base & (f["compression"]<=1.0)
                              & (f["close"]>f["prev_high"][10]),
      "PULLBACK_REJECTION":base & (f["low"]<f["prev_low"][20])
                             & (f["close"]>low10) & (f["mom4"]<=0),
    }
    for v in masks.values():v[:200]=False
    return masks


def one_at_a_time_factors(f):
    rows=[]
    def add(name,mask,kind,value):
        rows.append((name,mask,kind,value))
    for field,label,thresholds in (
        ("body_atr","BODY_ATR_MIN",(.50,.75,1.00,1.25)),
        ("range_atr","RANGE_ATR_MIN",(.75,1.00,1.25,1.50)),
        ("close_loc","CLOSE_LOCATION_MIN",(.55,.65,.75,.85)),
        ("lower_wick_body","LOWER_WICK_BODY_MIN",(.10,.20,.35,.50))):
        for v in thresholds:add(f"{label}_{v:.2f}",f[field]>=v,label,v)
    for v in (-.25,-.50,-1.00):
        add(f"PRIOR_4H_DECLINE_{v:.2f}",f["mom4"]<=v,"PRIOR_4H_MOMENTUM",v)
    for lb in (40,60,100,165):
        for d in (.10,.25,.50):
            add(f"NEAR_PREV_LOW_LB{lb}_D{d:.2f}",
                f["structure_dist_low"][lb]<=d,f"PRIOR_LOW_DISTANCE_LB{lb}",d)
    for lb in (20,40,60,100):
        add(f"BREAK_PREV_LOW_LB{lb}",f["low"]<f["prev_low"][lb],
            "PREV_LOW_BREAK_LOOKBACK",lb)
    for depth in (.025,.05,.10,.20):
        # Normalised penetration below previous ten-bar low; values are
        # overlays on all raw families, NOT a selected finalist.
        penetration=(f["prev_low"][10]-f["low"])/f["atr"]
        add(f"PENETRATE_LB10_{depth:.3f}",penetration>=depth,
            "MINIMUM_LOW_PENETRATION_ATR_LB10",depth)
    for name,a,b in (
        ("H1_CLOSE_GT_EMA100","h1_close","h1_ema100"),
        ("H1_EMA50_GT_EMA200","h1_ema50","h1_ema200"),
        ("H4_CLOSE_GT_EMA100","h4_close","h4_ema100"),
        ("H4_EMA100_GT_EMA200","h4_ema100","h4_ema200"),
        ("D_CLOSE_GT_EMA200","d_close","d_ema200"),
        ("D_EMA50_GT_EMA200","d_ema50","d_ema200")):
        add(name,f[a]>f[b],"COMPLETED_HTF_REGIME",name)
    for tf in ("h1","h4","d"):
        for threshold in (.80,1.00):
            add(f"{tf.upper()}_ATR_RATIO_GE_{threshold:.2f}",
                f[f"{tf}_atr"]>=threshold,"COMPLETED_HTF_VOLATILITY",threshold)
    for tz,key in (("NY","ny_hour"),("SYDNEY","sydney_hour"),
                   ("TOKYO","tokyo_hour"),("LONDON","london_hour")):
        for start in (0,4,8,12,16,20):
            add(f"{tz}_HOURS_{start:02d}-{start+3:02d}",
                (f[key]>=start)&(f[key]<=start+3),f"TIME_4H_{tz}",start)
    for weekday in range(5):
        add(f"EXCLUDE_NY_WEEKDAY_{weekday}",f["ny_weekday"]!=weekday,
            "EXCLUDE_WEEKDAY",weekday)
    return rows


def selected(trades, start=None, end=None):
    return [t for t in trades if (start is None or t["entry_time"]>=start)
            and (end is None or t["entry_time"]<end)]


def summary_row(config_id,family,factor_family,factor_value,raw_indices,trades,stressed):
    s=stats(trades)
    row=dict(config_id=config_id,family=family,factor_family=factor_family,
             factor_value=factor_value,raw_signal_count=len(raw_indices),
             **{f"full_{k}":v for k,v in s.items()})
    windows=(
      ("before2010",None,datetime(2010,1,1,tzinfo=timezone.utc)),
      ("since2010",datetime(2010,1,1,tzinfo=timezone.utc),None),
      ("since2018",datetime(2018,1,1,tzinfo=timezone.utc),None),
      ("last5y",NOW-timedelta(days=365.2425*5),None),
      ("last2y",NOW-timedelta(days=365.2425*2),None),
      ("last1y",NOW-timedelta(days=365.2425),None),
      ("era2002_2009",None,datetime(2010,1,1,tzinfo=timezone.utc)),
      ("era2010_2015",datetime(2010,1,1,tzinfo=timezone.utc),datetime(2016,1,1,tzinfo=timezone.utc)),
      ("era2016_2021",datetime(2016,1,1,tzinfo=timezone.utc),datetime(2022,1,1,tzinfo=timezone.utc)),
      ("era2022_present",datetime(2022,1,1,tzinfo=timezone.utc),None),
    )
    for label,start,end in windows:
        p=stats(selected(trades,start,end))
        for k in ("trades","total_r","profit_factor","max_drawdown_r"):
            row[f"{label}_{k}"]=p[k]
    if stressed is not None:
        p=stats(stressed)
        for k in ("trades","total_r","profit_factor","max_drawdown_r"):
            row[f"4pip_{k}"]=p[k]
        p=stats(selected(stressed,NOW-timedelta(days=365.2425*2)))
        row["4pip_last2y_total_r"]=p["total_r"]
        row["4pip_last2y_trades"]=p["trades"]
    row["research_only"]="All historical periods examined; not independent OOS"
    return row


def add_months_utc(dt,n):
    month=dt.year*12+dt.month-1+n
    return datetime(month//12,month%12+1,1,tzinfo=timezone.utc)


HISTORY_FIRST = START


def rolling(config_id,trades):
    rows=[]
    start=month_floor(HISTORY_FIRST)
    # Begin at the first full calendar month of available price history.
    if HISTORY_FIRST > start:
        start=add_months_utc(start,1)
    for months in (12,24,36):
        t=start
        while add_months_utc(t,months)<=datetime(NOW.year,NOW.month,1,tzinfo=timezone.utc):
            end=add_months_utc(t,months)
            st=stats(selected(trades,t,end))
            rows.append(dict(config_id=config_id,months=months,from_utc=iso(t),
                             to_utc=iso(end),trades=st["trades"],total_r=st["total_r"],
                             positive_active=st["total_r"]>0 if st["trades"] else "NO_TRADES"))
            t=add_months_utc(t,1)
    return rows


def calendar(config_id,trades):
    rows=[]
    for year in range(HISTORY_FIRST.year,NOW.year+1):
        s=stats(selected(trades,datetime(year,1,1,tzinfo=timezone.utc),
                         datetime(year+1,1,1,tzinfo=timezone.utc)))
        rows.append(dict(config_id=config_id,year=year,trades=s["trades"],
                         total_r=s["total_r"],profit_factor=s["profit_factor"]))
    return rows


def hist_coverage(label,candles):
    times=[x["time"] for x in candles]
    gap_days=max(((b-a).total_seconds()/86400 for a,b in zip(times,times[1:])),default=0)
    return dict(timeframe=label,count=len(candles),first_utc=iso(times[0]),
                last_utc=iso(times[-1]),max_gap_days=gap_days)


def run_research():
    try:
        STATUS.update(state="fetch",progress=1,message="Fetching AUD/JPY M15 midpoint candles")
        m15=fetch("M15",START,NOW,35)
        if len(m15)<100_000:raise RuntimeError(f"Insufficient AUD/JPY M15 history: {len(m15)}")
        global HISTORY_FIRST
        HISTORY_FIRST=m15[0]["time"]
        STATUS.update(progress=15,message="Fetching H1/H4/D complete midpoint candles")
        h1=fetch("H1",WARMUP,NOW,180)
        h4=fetch("H4",WARMUP,NOW,700)
        daily=fetch("D",WARMUP,NOW,3500)
        if not all((h1,h4,daily)):
            raise RuntimeError("Missing context history, cannot screen completed HTF factors")
        coverage=[hist_coverage(label,candles) for label,candles in (
            ("M15",m15),("H1",h1),("H4",h4),("D",daily))]
        write_csv(OUTS["coverage"],coverage)
        STATUS.update(state="features",progress=28,message="Computing causal features and six raw signal masks")
        times=[c["time"] for c in m15]
        f=features(m15,align_htf(times,htf_state(h1)),
                   align_htf(times,htf_state(h4)),align_htf(times,htf_state(daily)))
        masks=raw_masks(f)
        factors=one_at_a_time_factors(f)
        raw_rows=[]; factor_rows=[];raw_ledgers=[];cost_rows=[];roll=[];cal=[]
        STATUS.update(state="raw",progress=35,message="Six raw mechanisms; 2/4-pip fills; RR3.5")
        for j,family in enumerate(FAMILIES):
            signal_indices=np.flatnonzero(masks[family]).tolist()
            tr=backtest(m15,signal_indices,RR_FIXED,PRIMARY_COST)
            s4=backtest(m15,signal_indices,RR_FIXED,STRESS_COST)
            cid="RAW_"+family
            raw_rows.append(summary_row(cid,family,"RAW","NONE",signal_indices,tr,s4))
            for t in tr:
                raw_ledgers.append(dict(config_id=cid,**{
                    k:v for k,v in t.items() if k not in ("entry_time","exit_time")}))
            for cost,ledger in ((PRIMARY_COST,tr),(STRESS_COST,s4)):
                cost_rows.append(dict(config_id=cid,assumed_fill_pips=cost,**stats(ledger)))
            roll.extend(rolling(cid,tr));cal.extend(calendar(cid,tr))
            OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
            STATUS.update(progress=35+int(20*(j+1)/len(FAMILIES)),
                          message=f"Raw mechanism {j+1}/{len(FAMILIES)}: {family}")
        write_csv(OUTS["raw_families"],raw_rows)
        write_csv(OUTS["raw_trade_ledgers"],raw_ledgers)
        write_csv(OUTS["raw_cost_stress"],cost_rows)
        write_csv(OUTS["raw_rolling"],roll)
        write_csv(OUTS["raw_calendar"],cal)
        STATUS.update(state="single_factor",progress=55,
                      message=f"Six families x {len(factors)} separate factor settings")
        for j,family in enumerate(FAMILIES):
            baseline=masks[family]
            for label,one_factor,group,value in factors:
                ix=np.flatnonzero(baseline&one_factor).tolist()
                tr=backtest(m15,ix,RR_FIXED,PRIMARY_COST)
                s4=backtest(m15,ix,RR_FIXED,STRESS_COST) if len(tr)>=MIN_COST_STRESS_TRADES else None
                factor_rows.append(summary_row(family+"__"+label,family,group,value,ix,tr,s4))
            # Shared outcomes survive neighbouring single-factor tests;
            # bounded caches clear only AFTER each full raw family.
            OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
            write_csv(OUTS["single_factors"],factor_rows)  # partial progress remains inspectable
            STATUS.update(progress=55+int(40*(j+1)/len(FAMILIES)),
                          message=f"Completed single-factor family {j+1}/{len(FAMILIES)}")
        write_csv(OUTS["methods"],[
            dict(item="scope",detail="AUD/JPY M15 LONG standalone research only; NO Portfolio27 data or ranking"),
            dict(item="history",detail="Start requested May 2002; coverage CSV determines actual first AUD/JPY bar"),
            dict(item="raw",detail="Six minimal structural families; no imported AUD/USD, EUR/JPY, or other portfolio strategy parameters"),
            dict(item="factors",detail="Exactly one overlay per raw family; no interaction/RR/session-combination search"),
            dict(item="cost",detail="Assumed 2pip adverse BUY fill, 4pip stress for all raw and >=50-trade factor rows; midpoint not executable bid/ask"),
            dict(item="entry_exit",detail="Reference close, 10 tick stop buffer, RR3.5 target anchored to reference risk, full history p0 before period slicing"),
            dict(item="htf",detail="Only prior strictly completed H1/H4/D via next actual HTF open"),
            dict(item="no_old_m15_reference",detail="No archived AUD/JPY M15 LONG ledger in this project; no cross-pair parity claim. Internal mechanical checks only"),
            dict(item="interpretation",detail="No standalone winner or untouched OOS inferred; historical data previously explored for other pairs/timeframes"),
            dict(item="next",detail="Inspect broad single-factor evidence, then predeclare SMALL conditional matrix independently; portfolio admission only after frozen standalone validation"),
        ])
        zip_outputs()
        STATUS.update(state="complete",progress=100,raw_families=len(raw_rows),
                      single_factor_rows=len(factor_rows),factors_per_family=len(factors),
                      message="Standalone AUD/JPY M15 LONG stage 1 complete",
                      result_path="/audjpy-m15-long-stage1/results")
    except Exception as ex:
        STATUS.update(state="error",message=str(ex),traceback=traceback.format_exc(),
                      orders_supported=False,trading_enabled=False)
        try:
            write_csv(OUTS["errors"],[dict(error=str(ex),traceback=STATUS["traceback"],
                                         progress=STATUS["progress"],state=STATUS["state"])])
            zip_outputs()
        finally:
            print(STATUS["traceback"],flush=True)


@app.route("/")
def root():
    return jsonify(service="AUD/JPY M15 LONG Stage 1 standalone research",state=STATUS["state"],
                   status="/audjpy-m15-long-stage1/status",
                   results="/audjpy-m15-long-stage1/results",
                   orders_supported=False,trading_enabled=False)

@app.route("/audjpy-m15-long-stage1/status")
def status_route():
    return jsonify(STATUS)

@app.route("/audjpy-m15-long-stage1/results")
def results_route():
    return download(BUNDLE)


if __name__=="__main__":
    threading.Thread(target=run_research,daemon=True).start()
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","8080")),debug=False)
