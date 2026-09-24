#!/usr/bin/env python3
"""AUD/JPY M15 SHORT — engulfing-first Pass 1; READ-ONLY research service.

Adapted from the saved AUD/JPY M15 LONG Pass 1 research script, NOT from
an active trading component. Exact bearish engulf: prior candle bullish,
signal candle bearish, its real body engulfs prior real body, body ratio >= 1.

The research engine uses the mathematically mirrored MIDPOINT OHLC coordinate:
   open'=-open; high'=-low; low'=-high; close'=-close.
Thus a bullish pattern and a prior-low distance IN THE INTERNAL ENGINE mean
bearish engulf and prior-HIGH distance in actual AUD/JPY. All output ledger
prices are converted back to actual positive AUD/JPY prices; RR is unchanged.
The short reference is signal close, stop=signal HIGH+10 JPY ticks, target
reference minus 3.5 times reference-to-stop distance, and assumed adverse
SELL fill=reference-2/4 pips. Stop/target testing begins NEXT M15 candle;
closer-to-open tie convention, pyramiding zero, exit-candle reentry.

This is a new bearish control: DO NOT compare its raw ledger to the archived
bullish hash. Cross-check every bearish raw index and full accepted baseline
ledgers against separate native-SHORT calculations at BOTH costs.

Full-history repeated research is exploratory/in-sample; 2 and 4 pip costs
are assumptions, NOT actual AUD/JPY bid/ask history. No orders, no webhook,
no portfolio tuning, no changes to the 28 live strategies or executor.
"""
import os
import csv
import time
import bisect
import zipfile
import threading
import traceback
import hashlib
import itertools
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
OUTPUT_DIR=Path(os.getenv("AUDJPY_SHORT_PASS1_OUTPUT_DIR", "/tmp/audjpy_short_pass1"))
OUTPUT_DIR.mkdir(parents=True,exist_ok=True)
OUTS={name:str(OUTPUT_DIR/f"audjpy_short_pass1_{name}.csv") for name in (
    "coverage","raw_engulf_parity","raw_engulf_control","raw_engulf_trades",
    "expanded_single_factors","engulf_geometry_matrix","matrix_neighbours",
    "matrix_levels","fixed_anchor_ledgers","rolling_fixed_anchors",
    "methodology","errors")}
BUNDLE=str(OUTPUT_DIR/"AUDJPY_M15_SHORT_ENGULFING_PASS1_RESULTS.zip")

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
# M15 FEATURE CACHE — MIRRORED SHORT SPACE
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
    for lb in [20, 40, 60, 80, 100, 120, 165, 200]:
        x = np.full(n, np.nan)
        ok = valid_atr & np.isfinite(prev_low[lb])
        x[ok] = np.abs(l[ok] - prev_low[lb][ok]) / a[ok]
        dist_low[lb] = x
    # Mirrored 4h momentum: prior actual rise => negative mirrored momentum.
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
# MIRRORED SHORT BACKTEST — full-ledger p0, time-window metrics never reset p0
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
            "reference_entry":-ref, "historical_fill":-fill,
            "stop":-stop, "target":-target, "result_r":(price-fill)/actual_risk,
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
# HISTORY FETCH & SAFE RESULTS PACKAGING
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


def mirror_ohlc(c):
    """Invert the price axis; enforce proper ordered OHLC in mirrored space."""
    return dict(time=c['time'], open=-c['open'], high=-c['low'],
                low=-c['high'], close=-c['close'])


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
    return [mirror_ohlc(seen[t]) for t in sorted(seen)]


def zip_outputs():
    with zipfile.ZipFile(BUNDLE,"w",zipfile.ZIP_DEFLATED) as z:
        for name,path in OUTS.items():
            if os.path.isfile(path):z.write(path,arcname=os.path.basename(path))


# ============================================================
# SIX MINIMAL ENTRY MECHANISMS, NOT PRE-OPTIMISED STRATEGIES
# ============================================================
FAMILIES=("BEAR_ENGULF","FAILED_BREAKOUT","HIGH_SWEEP_DISPLACEMENT",
          "OUTSIDE_REVERSAL","COMPRESSION_BREAKDOWN","RALLY_REJECTION")


def raw_masks(f):
    base=f["valid_atr"] & f["bullish"]
    previous_high=np.r_[np.nan,f["high"][:-1]]
    previous_low=np.r_[np.nan,f["low"][:-1]]
    low10=f["prev_low"][10]
    masks={
      "BEAR_ENGULF":base & f["exact_bull"] & (f["bull_br"]>=1.0),
      "FAILED_BREAKOUT":base & (f["low"]<low10) & (f["close"]>low10),
      "HIGH_SWEEP_DISPLACEMENT":base & (f["low"]<low10) & (f["close"]>previous_high),
      "OUTSIDE_REVERSAL":base & (f["low"]<previous_low) & (f["high"]>previous_high),
      "COMPRESSION_BREAKDOWN":base & (f["compression"]<=1.0)
                              & (f["close"]>f["prev_high"][10]),
      "RALLY_REJECTION":base & (f["low"]<f["prev_low"][20])
                             & (f["close"]>low10) & (f["mom4"]<=0),
    }
    for v in masks.values():v[:200]=False
    return masks




# ============================================================
# INDEPENDENT NATIVE-SHORT CONTROL (not the archived long ledger)
# ============================================================
REFERENCE_N = 0  # Short has no previously frozen historical ledger.


def actual_bearish_indices(candles):
    """Native actual-price bearish engulf, separately from mirror features."""
    o=np.array([-x['open'] for x in candles], dtype=float)
    cl=np.array([-x['close'] for x in candles], dtype=float)
    prev_body=np.r_[np.nan,np.abs(cl[:-1]-o[:-1])]
    body=o-cl
    native=np.zeros(len(candles),dtype=bool)
    native[1:]=(cl[:-1]>o[:-1])&(cl[1:]<o[1:])& \
               (o[1:]>=cl[:-1])&(cl[1:]<=o[:-1])& \
               (prev_body[1:]>0)&(body[1:]/np.maximum(prev_body[1:],1e-50)>=1.0)
    native[:200]=False
    return np.flatnonzero(native).tolist()


def native_short_outcome(candles, ix, rr, cost_pips):
    """Independent REAL-price SELL path, for exact baseline-ledger validation."""
    signal=candles[ix]
    ref=-signal['close']
    stop=-signal['low']+STOP_TICKS*TICK
    risk=stop-ref
    if risk<=0:return None
    target=ref-rr*risk
    fill=ref-cost_pips*PIP
    actual_risk=stop-fill
    if actual_risk<=0:return None
    for j in range(ix+1,len(candles)):
        bar=candles[j]
        high=-bar['low'];low=-bar['high'];open_price=-bar['open']
        hit_stop=high>=stop;hit_target=low<=target
        if not (hit_stop or hit_target):continue
        if hit_stop and hit_target:
            exit_price,reason=(target,'TARGET') if abs(open_price-low)<abs(high-open_price) \
                 else (stop,'STOP')
        elif hit_target:exit_price,reason=target,'TARGET'
        else:exit_price,reason=stop,'STOP'
        return dict(signal_index=ix,exit_index=j,reference_entry=ref,
            historical_fill=fill,stop=stop,target=target,
            result_r=(fill-exit_price)/actual_risk,exit_reason=reason)
    return None


def check_native_short_parity(candles, feat):
    mirrored_indices=np.flatnonzero(engulf_base(feat)).tolist()
    native_indices=actual_bearish_indices(candles)
    if mirrored_indices!=native_indices:
        raise RuntimeError('Native bearish engulf raw-index parity FAILED')
    checks=[]
    for cost in (PRIMARY_COST,STRESS_COST):
        OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
        baseline=backtest(candles,mirrored_indices,RR_FIXED,cost)
        native=[];p=0
        while p<len(native_indices):
            trade=native_short_outcome(candles,native_indices[p],RR_FIXED,cost)
            if trade is None:
                p+=1;continue
            native.append(trade)
            p=bisect.bisect_left(native_indices,trade['exit_index'],lo=p+1)
        if len(baseline)!=len(native):
            raise RuntimeError('Native bearish accepted-count parity FAILED')
        fields=('signal_index','exit_index','reference_entry','historical_fill',
                'stop','target','result_r','exit_reason')
        for a,b in zip(baseline,native):
            for field in fields:
                if (abs(a[field]-b[field])>1e-9 if field not in
                    ('signal_index','exit_index','exit_reason') else a[field]!=b[field]):
                    raise RuntimeError('Native bearish accepted-ledger parity FAILED on '+field)
        digest=hashlib.sha256()
        for index in mirrored_indices:
            digest.update((iso(candles[index]['time'])+'\n').encode())
        ledgerhash=hashlib.sha256()
        for t in baseline:
            ledgerhash.update((str(t['signal_index'])+'|'+str(t['exit_index'])+'|'+
                repr(t['result_r'])+'\n').encode())
        checks.append(dict(direction='SELL',pattern='EXACT_BEARISH_ENGULF_BR1',
            assumed_adverse_fill_pips=cost,raw_signal_count=len(mirrored_indices),
            accepted_trades=len(baseline),raw_signal_sha256=digest.hexdigest(),
            accepted_ledger_sha256=ledgerhash.hexdigest(),
            raw_native_parity='PASS',full_accepted_native_parity='PASS',
            cutoff_utc=iso(candles[-1]['time'])))
    write_csv(OUTS['raw_engulf_parity'],checks)
    OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
    return checks


def midpoint_fingerprint(candles, timeframe):
    digest=hashlib.sha256()
    for c in candles:
        digest.update((iso(c['time'])+'|'+repr(-c['open'])+'|'+
           repr(-c['low'])+'|'+repr(-c['high'])+'|'+repr(-c['close'])+'\n').encode())
    return dict(timeframe=timeframe,sha256_midpoint_ohlc=digest.hexdigest())


# ============================================================

def hist_coverage(label,candles):
    if not candles:raise RuntimeError('No candles for '+label)
    ts=[x['time'] for x in candles]
    return dict(timeframe=label,count=len(ts),first_utc=iso(ts[0]),last_utc=iso(ts[-1]),
        max_gap_days=max(((b-a).total_seconds()/86400 for a,b in zip(ts,ts[1:])),default=0))

def add_months_utc(value,n):
    return add_months(value,n)

# ============================================================
# SHORT ENGULFING-ONLY EXPLORATORY FEATURES (mirrored/real mapping)
# ============================================================
GEOMETRY_LB=(20,40,60,100,165)
GEOMETRY_D=(.05,.10,.25,.50)
GEOMETRY_BODY=(.50,.75,1.00,1.25)
GEOMETRY_RANGE=(.75,1.00,1.25,1.50)
# 5 x 4 x 4 x 4 = 320, plus 16 no-structure controls. Fixed RR3.5.
N_GEOMETRY=5*4*4*4+4*4
FIXED_ANCHORS={
  (40,.25,.75,1.00),
  (60,.10,1.00,1.25),
  (100,.25,1.00,1.25),
  (165,.10,1.25,1.50),
}


def prior_shift(values,bars,fill=np.nan):
    values=np.asarray(values)
    return np.r_[np.full(bars,fill,dtype=values.dtype),values[:-bars]]


def make_extra_features(f):
    """Only causal fields. Signal-candle geometry may use current OHLC at close."""
    n=f['n'];close=f['close'];low=f['low'];atr14=f['atr']
    good=np.isfinite(atr14)&(atr14>0)
    ret={}
    reference_risk=close-(low-STOP_TICKS*TICK)
    ret['stop_atr']=np.divide(reference_risk,atr14,
        out=np.full(n,np.nan),where=good)
    ret['body_ratio']=f['bull_br']
    ret['m15_atr_ratio20']=np.divide(atr14, sma(atr14,20),
        out=np.full(n,np.nan),where=np.isfinite(sma(atr14,20))&(sma(atr14,20)>0))
    ret['upper_wick_body']=np.divide(f['high']-np.maximum(f['open'],close),
        close-f['open'],out=np.full(n,np.nan),where=close>f['open'])
    # prior N-bar momentum i-1 versus i-(N+1), denominator known at signal close
    for name,bars in [('mom4',16),('mom12',48),('mom24',96),('mom48',192)]:
        m=np.full(n,np.nan)
        m[bars+1:]=np.divide(close[bars:-1]-close[:-(bars+1)],
            atr14[bars+1:],out=np.full(n-(bars+1),np.nan),where=good[bars+1:])
        ret[name]=m
    # Prior candle's ATR-scaled body; no forward data.
    pbody=np.abs(prior_shift(close-f['open'],1))
    ret['prev_body_atr']=np.divide(pbody,atr14,out=np.full(n,np.nan),where=good)
    for lb in GEOMETRY_LB:
        prior_low=f['prev_low'][lb]
        penetration=np.full(n,np.nan)
        mask=good&np.isfinite(prior_low)
        penetration[mask]=(prior_low[mask]-low[mask])/atr14[mask]
        ret[f'penetration_{lb}']=penetration
    return ret


def engulf_base(f):
    m=np.asarray(f['valid_atr']&f['exact_bull']&(f['bull_br']>=1.0),dtype=bool).copy()
    m[:200]=False
    return m


def independent_filters(f,e):
    """Old-style factor scan: independent predicates on raw mirrored bearish engulfing ONLY.

    Each returned predicate is run as full chronological p0; cost stress for
    EVERY setting. Timing is explicitly diagnostic, not a selectable gate here.
    """
    rows=[]
    def add(group,axis,val,mask):
        rows.append((group,axis,val,np.asarray(mask,dtype=bool)))
    for v in (1.00,1.10,1.25,1.50,1.75,2.00):
        add('body_ratio','min',v,f['bull_br']>=v)
    for name,vals in [
        ('body_atr',(.40,.60,.80,1.00,1.20,1.40,1.60)),
        ('range_atr',(.70,.90,1.10,1.30,1.50,1.80)),
        ('close_near_actual_low',(.55,.65,.75,.85,.90)),
        ('actual_upper_wick_body',(.10,.20,.30,.40,.60)),
        ('m15_atr_ratio20',(.70,.85,1.00,1.15,1.30)),
        ('prev_body_atr',(.25,.50,.75,1.00,1.25)),
    ]:
        x=(f['close_loc'] if name=='close_near_actual_low' else
           f['lower_wick_body'] if name=='actual_upper_wick_body' else
           e.get(name,f.get(name)))
        for v in vals:add(name,'min',v,x>=v)
    for v in (.10,.20,.30,.50,.75):
        add('actual_lower_wick_body','max',v,e['upper_wick_body']<=v)
    for v in (1.00,1.25,1.50,2.00,2.50,3.00):
        add('stop_atr','max',v,e['stop_atr']<=v)
    for lb in GEOMETRY_LB:
        for d in (.05,.10,.25,.50,1.00):
            add('distance_to_actual_prior_high_'+str(lb),'max',d,
                f['structure_dist_low'][lb]<=d)
        for p in (.00,.05,.10,.20):
            add('actual_high_penetration_'+str(lb),'min',p,
                e['penetration_'+str(lb)]>=p)
    for name in ('mom4','mom12','mom24','mom48'):
        for v in (.00,-.50,-1.00,-1.50):
            add('actual_prior_'+name+'_rise','min',-v,e[name]<=v)
        for v in (.00,.50,1.00):
            add('actual_prior_'+name+'_decline','min',v,e[name]>=v)
    for name,lhs,rhs in (
        ('h1_close_lt_ema50','h1_close','h1_ema50'),
        ('h1_close_lt_ema100','h1_close','h1_ema100'),
        ('h1_ema50_lt_ema200','h1_ema50','h1_ema200'),
        ('h4_close_lt_ema100','h4_close','h4_ema100'),
        ('h4_ema100_lt_ema200','h4_ema100','h4_ema200'),
        ('daily_close_lt_ema50','d_close','d_ema50'),
        ('daily_close_lt_ema200','d_close','d_ema200'),
        ('daily_ema50_lt_ema200','d_ema50','d_ema200'),
    ):
        add('completed_htf_regime',name,'yes',f[lhs]>f[rhs])
    for name in ('h1_atr','h4_atr','d_atr'):
        for v in (.80,1.00,1.20):add('completed_'+name,'min',v,f[name]>=v)
    # TIMING DIAGNOSTICS only: do not combine with matrix or automatically rank.
    for v in range(24):
        add('ny_hour_DIAGNOSTIC','equals',v,f['ny_hour']==v)
    for v in range(5):
        add('ny_weekday_DIAGNOSTIC','exclude',v,f['ny_weekday']!=v)
    assert len(rows)==len({(a,b,str(c)) for a,b,c,_ in rows})
    return rows


def geometries(f,base):
    """Core old-style matrix, not several unrelated raw trigger families."""
    strong=f['body_atr'];large=f['range_atr']
    count=0
    for body,rng in itertools.product(GEOMETRY_BODY,GEOMETRY_RANGE):
        mask=base&(strong>=body)&(large>=rng)
        yield ('NO_STRUCTURE',0,None,body,rng,mask)
        count+=1
    for lb in GEOMETRY_LB:
        dist=f['structure_dist_low'][lb]
        for d,body,rng in itertools.product(GEOMETRY_D,GEOMETRY_BODY,GEOMETRY_RANGE):
            mask=base&(dist<=d)&(strong>=body)&(large>=rng)
            yield ('NEAR_PREV_LOW',lb,d,body,rng,mask)
            count+=1
    if count!=N_GEOMETRY:raise RuntimeError('Geometry count '+str(count))


def config_id(lb,d,body,rng):
    return 'ENGULF__LB'+str(lb)+'__D'+('NONE' if d is None else str(d).replace('.','p'))+\
       '__BODY'+str(body).replace('.','p')+'__RANGE'+str(rng).replace('.','p')


def subset(trades,start=None,end=None):
    return [t for t in trades if (start is None or t['entry_time']>=start)
                and (end is None or t['entry_time']<end)]


def metrics_row(category,cid,axes,ix,main,stress,raw_stats):
    s2=stats(main);s4=stats(stress)
    row=dict(category=category,config_id=cid,**axes,raw_signals=len(ix),
        **{'2pip_'+k:v for k,v in s2.items()},
        **{'4pip_'+k:v for k,v in s4.items()},
        delta_expectancy_vs_raw_2pip=s2['expectancy_r']-raw_stats['expectancy_r'])
    windows=[
        ('pre2010',None,datetime(2010,1,1,tzinfo=timezone.utc)),
        ('2010to2015',datetime(2010,1,1,tzinfo=timezone.utc),datetime(2016,1,1,tzinfo=timezone.utc)),
        ('2016to2021',datetime(2016,1,1,tzinfo=timezone.utc),datetime(2022,1,1,tzinfo=timezone.utc)),
        ('2022on',datetime(2022,1,1,tzinfo=timezone.utc),None),
        ('since2010',datetime(2010,1,1,tzinfo=timezone.utc),None),
        ('last5y',NOW-timedelta(days=365.2425*5),None),
        ('last2y',NOW-timedelta(days=365.2425*2),None),
        ('last1y',NOW-timedelta(days=365.2425),None),
    ]
    for name,start,end in windows:
        for label,ledger in (('2pip',main),('4pip',stress)):
            s=stats(subset(ledger,start,end))
            for k in ('trades','total_r','profit_factor','max_drawdown_r'):
                row[label+'_'+name+'_'+k]=s[k]
    row['research_only']='Repeatedly examined history; not independent OOS'
    return row


def evaluate(candles,indices):
    tr=backtest(candles,indices,RR_FIXED,PRIMARY_COST)
    stress=backtest(candles,indices,RR_FIXED,STRESS_COST)
    return tr,stress


def neighbours(rows):
    """Every *adjacent* setting, including losing neighbours and empty rows."""
    ix={(r['lookback'],r['distance_atr'],r['body_atr_min'],r['range_atr_min']):r
        for r in rows}
    result=[]
    axes={'lookback':(0,GEOMETRY_LB),'distance_atr':(1,GEOMETRY_D),
          'body_atr_min':(2,GEOMETRY_BODY),'range_atr_min':(3,GEOMETRY_RANGE)}
    for r in rows:
        if r['lookback']==0:continue
        orig=(r['lookback'],r['distance_atr'],r['body_atr_min'],r['range_atr_min'])
        for axis,(pos,levels) in axes.items():
            idx=levels.index(orig[pos])
            if idx+1>=len(levels):continue
            nextcoord=list(orig);nextcoord[pos]=levels[idx+1]
            s=ix.get(tuple(nextcoord))
            if s is None:continue
            result.append(dict(axis=axis,from_id=r['config_id'],to_id=s['config_id'],
                from_value=orig[pos],to_value=nextcoord[pos],
                from_n=r['2pip_trades'],to_n=s['2pip_trades'],
                from_r=r['2pip_total_r'],to_r=s['2pip_total_r'],
                from_4pip_r=r['4pip_total_r'],to_4pip_r=s['4pip_total_r'],
                from_expectancy=r['2pip_expectancy_r'],to_expectancy=s['2pip_expectancy_r']))
    return result


def matrix_level_summary(rows):
    result=[]
    for name,levels in [('lookback',GEOMETRY_LB),('distance_atr',GEOMETRY_D),
                        ('body_atr_min',GEOMETRY_BODY),('range_atr_min',GEOMETRY_RANGE)]:
        for val in levels:
            v=[r for r in rows if r['lookback']>0 and r[name]==val]
            valid=[r for r in v if r['2pip_trades']>=50]
            result.append(dict(axis=name,value=val,rows=len(v),rows_50trades=len(valid),
                median_2pip_expectancy=med([x['2pip_expectancy_r'] for x in valid]),
                median_4pip_expectancy=med([x['4pip_expectancy_r'] for x in valid]),
                positive_2pip=sum(x['2pip_total_r']>0 for x in valid),
                positive_4pip=sum(x['4pip_total_r']>0 for x in valid),
                positive_pre2010_and_since2010_4pip=sum(x['4pip_pre2010_total_r']>0 and
                    x['4pip_since2010_total_r']>0 for x in valid),
                interpretation='Aggregate descriptive only; configurations overlap heavily'))
    return result


def anchor_rolling(cid,trades,assumption):
    rows=[]
    begin=datetime(HISTORY_FIRST.year,HISTORY_FIRST.month,1,tzinfo=timezone.utc)
    if HISTORY_FIRST>begin:begin=add_months_utc(begin,1)
    end_full=datetime(NOW.year,NOW.month,1,tzinfo=timezone.utc)
    for duration in (12,24,36):
        win=begin
        while add_months_utc(win,duration)<=end_full:
            finish=add_months_utc(win,duration)
            x=stats(subset(trades,win,finish))
            rows.append(dict(config_id=cid,cost_pips=assumption,months=duration,
                from_utc=iso(win),to_utc=iso(finish),trades=x['trades'],total_r=x['total_r']))
            win=add_months_utc(win,1)
    return rows


def run_research():
    try:
        STATUS.update(state='fetch',progress=1,message='Fetching AUD/JPY complete midpoint candles')
        candles=fetch('M15',START,NOW,35)
        if len(candles)<5000:raise RuntimeError('M15 history insufficient for a full-history study')
        global HISTORY_FIRST
        HISTORY_FIRST=candles[0]['time']
        STATUS.update(progress=13,message='Fetching completed H1/H4/D contexts')
        h1=fetch('H1',WARMUP,NOW,180)
        h4=fetch('H4',WARMUP,NOW,700)
        daily=fetch('D',WARMUP,NOW,3500)
        if not (h1 and h4 and daily):raise RuntimeError('No complete H1/H4/D data')
        write_csv(OUTS['coverage'],[dict(**hist_coverage(k,v),**midpoint_fingerprint(v,k)) for k,v in
            (('M15',candles),('H1',h1),('H4',h4),('D',daily))])
        STATUS.update(state='features',progress=26,message='Computing past-only indicators')
        times=[c['time'] for c in candles]
        f=features(candles,align_htf(times,htf_state(h1)),
                   align_htf(times,htf_state(h4)),align_htf(times,htf_state(daily)))
        STATUS.update(state='parity',progress=34,message='Independent native-SHORT control, raw indices and full ledgers at 2/4 pips')
        check=check_native_short_parity(candles,f)
        base=engulf_base(f)
        raw_ix=np.flatnonzero(base).tolist()
        raw2,raw4=evaluate(candles,raw_ix)
        st=stats(raw2)
        write_csv(OUTS['raw_engulf_control'],[
            metrics_row('RAW','BEAR_ENGULF_BR1',{},raw_ix,raw2,raw4,st)])
        write_csv(OUTS['raw_engulf_trades'],[
            {k:v for k,v in tr.items() if k not in ('entry_time','exit_time')}
            for tr in raw2])
        STATUS.update(state='single_factor',progress=41,
             message='Expanded one-factor diagnostics: each factor independently on engulfing')
        extra=make_extra_features(f)
        independent=independent_filters(f,extra)
        factors=[]
        for j,(group,axis,value,overlay) in enumerate(independent):
            ix=np.flatnonzero(base&overlay).tolist()
            tr2,tr4=evaluate(candles,ix)
            cid='FACTOR__'+group+'__'+axis+'__'+str(value).replace('.','p')
            row=metrics_row('ONE_FACTOR',cid,
                dict(factor_group=group,threshold_kind=axis,threshold=value),
                ix,tr2,tr4,st)
            factors.append(row)
            if j%16==15 or j==len(independent)-1:
                STATUS.update(progress=41+int(18*(j+1)/len(independent)),
                              message='Engulf-only filter %d/%d'%(j+1,len(independent)))
                write_csv(OUTS['expanded_single_factors'],factors)
            if j%36==35:OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
        STATUS.update(state='matrix',progress=60,
                      message='Starting 336 predeclared SHORT engulfing prior-HIGH x body x range settings')
        grid_rows=[];anchors=[];roll=[]
        seen=set()
        for j,(kind,lb,d,body,rng,mask) in enumerate(geometries(f,base)):
            cid=config_id(lb,d,body,rng)
            if cid in seen:raise RuntimeError('Duplicate config ID '+cid)
            seen.add(cid)
            ix=np.flatnonzero(mask).tolist()
            tr2,tr4=evaluate(candles,ix)
            r=metrics_row('MATRIX_'+kind,cid,dict(lookback=lb,distance_atr=d,
                body_atr_min=body,range_atr_min=rng),ix,tr2,tr4,st)
            grid_rows.append(r)
            if (lb,d,body,rng) in FIXED_ANCHORS:
                for assumption,ledger in ((2,tr2),(4,tr4)):
                    roll.extend(anchor_rolling(cid,ledger,assumption))
                    anchors.extend(dict(config_id=cid,assumed_fill_pips=assumption,
                         **{k:v for k,v in t.items() if k not in ('entry_time','exit_time')})
                         for t in ledger)
            if j%12==11 or j==N_GEOMETRY-1:
                STATUS.update(progress=60+int(34*(j+1)/N_GEOMETRY),
                              message='Engulfing matrix %d/%d'%(j+1,N_GEOMETRY))
                write_csv(OUTS['engulf_geometry_matrix'],grid_rows)
            # clear grouped cache occasionally, not once per configuration
            if j%48==47:OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
        if len(grid_rows)!=N_GEOMETRY:raise RuntimeError('Incomplete matrix')
        if sum(x['lookback']==0 for x in grid_rows)!=16:raise RuntimeError('Missing no-structure geometry controls')
        if sum((r['lookback'],r['distance_atr'],r['body_atr_min'],r['range_atr_min'])
            in FIXED_ANCHORS for r in grid_rows)!=len(FIXED_ANCHORS):
            raise RuntimeError('Fixed anchor missing from matrix')
        write_csv(OUTS['engulf_geometry_matrix'],grid_rows)
        write_csv(OUTS['matrix_neighbours'],neighbours(grid_rows))
        write_csv(OUTS['matrix_levels'],matrix_level_summary(grid_rows))
        write_csv(OUTS['fixed_anchor_ledgers'],anchors)
        write_csv(OUTS['rolling_fixed_anchors'],roll)
        write_csv(OUTS['methodology'],[
          dict(topic='SCOPE',detail='READ ONLY, AUD/JPY M15 SHORT exact bearish engulfing; no Portfolio 28 changes or orders'),
          dict(topic='NATIVE_CONTROL',detail='Independent native bearish raw signals and full accepted ledger parity at 2 and 4 assumed pips; dynamic current-history SHA256, NOT the old bullish frozen ledger'),
          dict(topic='PARAMETERS',detail='336 predeclared geometry settings: 16 no-structure + 320 prior-high structure x body x range; additional one-factor diagnostics independently'),
          dict(topic='RR',detail='Fixed RR3.50; do not optimise exits or timing in this pass'),
          dict(topic='COST',detail='Assumed 2-pip adverse SELL fill and 4-pip stress for all settings; not actual spread history'),
          dict(topic='CAUSAL',detail='Signal OHLC at completed close; prior-only extrema and momentum; completed HTF through next actual open'),
          dict(topic='EXECUTION',detail='Target from reference-close risk; actual R from fill; next-bar exit, closer-to-open tie stop fallback, exit-candle signal eligible'),
          dict(topic='TIMING',detail='NY hours and weekdays only single-factor diagnostics, not combined or candidate selection'),
          dict(topic='P0',detail='Recompute full chronological pyramiding-zero strategy then slice into eras; no fake boundary re-entry'),
          dict(topic='OVERFITTING',detail='All eras repeatedly inspected. Broad neighbourhoods, cost cushion, frequency & early/late/recent matter; no automatic promotion'),
          dict(topic='NEXT',detail='Inspect geometry landscape; test new filters CONDITIONALLY around several defensible representative regions only if warranted')])
        zip_outputs()
        STATUS.update(state='complete',progress=100,native_short_parity='PASS',
            full_candles=len(candles),single_factor_rows=len(factors),
            geometry_rows=len(grid_rows),orders_supported=False,trading_enabled=False,
            result_path='/audjpy-short-pass1/results',
            message='Short engulfing-first matrix complete; no live changes')
    except Exception as exc:
        STATUS.update(state='error',message=str(exc),traceback=traceback.format_exc(),
            orders_supported=False,trading_enabled=False)
        try:
            write_csv(OUTS['errors'],[dict(error=str(exc),traceback=STATUS['traceback'])])
            zip_outputs()
        finally:print(STATUS['traceback'],flush=True)


@app.route('/')
def root():
    return jsonify(service='AUD/JPY M15 SHORT engulfing-first research',
        state=STATUS['state'],status='/audjpy-short-pass1/status',
        results='/audjpy-short-pass1/results',
        orders_supported=False,trading_enabled=False)


@app.route('/audjpy-short-pass1/status')
def status_route():return jsonify(STATUS)


@app.route('/audjpy-short-pass1/results')
def results_route():return download(BUNDLE)


if __name__=='__main__':
    threading.Thread(target=run_research,daemon=True).start()
    app.run(host='0.0.0.0',port=int(os.getenv('PORT','8080')),debug=False,use_reloader=False)
