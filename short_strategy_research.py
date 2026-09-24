#!/usr/bin/env python3
"""AUD/JPY M15 SHORT — RESEARCH-ONLY Pass4 fine plateau and frequency.

FROZEN SOURCE: 546,907 OANDA MID M15 candles through 2026-09-24 19:00 UTC;
checks the exact Pass3 and archived bearish controls before results.
Fixed RR3.50, stop signal high +10 ticks, adverse SELL entry 2 and 4
ASSUMED pips (NOT observed spreads). Full chronological pyramiding-zero
replay independently by cost and configuration; no orders and NO changes
whatsoever to the live 28-strategy executor or strategy probe.

Predeclared local fine geometry within Pass3 interior, a separate prior-16
rally-strength sweep on FIVE unmodified Pass3 anchors, and isolated
one-axis frequency tests. Do NOT use a best in-sample row as final rules.
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
    lookbacks = [10, 20, 30, 40, 50, 60, 70, 80, 100, 120, 165, 200]
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



# ============================================================
# PREDECLARED NEW PASS: SIX INDEPENDENT MECHANISMS
# ============================================================
OUTPUT_DIR=Path(os.getenv('AUDJPY_SHORT_ALT_PASS1_OUTPUT_DIR','/tmp/audjpy_short_alt_pass1'))
OUTPUT_DIR.mkdir(parents=True,exist_ok=True)
OUTPUT_NAMES=('coverage','raw_engulf_parity','historical_control','mechanism_definitions','native_mechanism_parity',
    'raw_family_summary','mechanism_matrix','matrix_marginal_attribution','matrix_adjacent_neighbours',
    'matrix_axis_summary','representative_ledgers','representative_rolling',
    'representative_calendar_years','methodology','errors')
OUTS={name:str(OUTPUT_DIR/f'audjpy_short_alt_pass1_{name}.csv') for name in OUTPUT_NAMES}
BUNDLE=str(OUTPUT_DIR/'AUDJPY_M15_SHORT_ALTERNATIVE_MECHANISMS_PASS1_RESULTS.zip')
FIXED_END=datetime(2026,9,24,19,15,tzinfo=timezone.utc)  # 19:00 M15 open was fully closed
EXPECTED_LAST='2026-09-24T19:00:00Z'
EXPECTED_FIRST='2004-05-31T20:45:00Z'
EXPECTED_CANDLES=546907
EXPECTED_SHA='124e81dc0a8302d45433ed1db66e8df75cab7dc7a1237948bcef357a7b65153e'
EXPECTED_BEARISH_RAW_SHA='81f0d6f3c26e7c4a48d053b33da0ce229dc857faaa08e0613a19438d3ff7102d'
EXPECTED_BEARISH_LEDGER_SHA={2.0:'37e5d9cd21eebdbea97187477dc1f8688b2cadde3ba5acd7acc13570d46c2a3f',
    4.0:'ea8d77cf87ef56a2273af87d66fa516304ccc098b42f3ce4c6f9b8b7c3154bb5'}
# Preserve the date-origin of all last-N-year calculations in output rows.
NOW=FIXED_END
MATRIX_LB=(10,20,40,60)
MATRIX_BODY=(.75,1.00,1.25)
MATRIX_RANGE=(1.00,1.50,2.00)
MATRIX_CONFIRM=(0.00,.25)
FAMILIES=('FAILED_HIGH_BREAKOUT','HIGH_SWEEP_DISPLACEMENT','OUTSIDE_REVERSAL',
          'COMPRESSION_BREAKDOWN','RALLY_REJECTION','DOWNSIDE_BREAKDOWN')
EXPECTED_GRID=len(FAMILIES)*len(MATRIX_LB)*len(MATRIX_BODY)*len(MATRIX_RANGE)*len(MATRIX_CONFIRM)

FAMILY_RULES={
 'FAILED_HIGH_BREAKOUT':'high > prior N high; close < prior N high minus confirm*ATR; bearish',
 'HIGH_SWEEP_DISPLACEMENT':'high > prior N high; close < previous M15 low minus confirm*ATR; bearish',
 'OUTSIDE_REVERSAL':'high > prior N high and low < prior N low; close location in bottom 40% (confirm=0) or bottom 20% (confirm=.25); bearish',
 'COMPRESSION_BREAKDOWN':'previous ATR14/previous 20-bar ATR average <=1.00 (confirm=0) or <=.80 (confirm=.25); close < prior N low; bearish',
 'RALLY_REJECTION':'high > prior N high, close < prior N high; preceding 16 M15-bar actual close rise >= .50 ATR (confirm=0) or 1.50 ATR (confirm=.25); bearish',
 'DOWNSIDE_BREAKDOWN':'close < prior N low minus confirm*ATR; bearish',
}


def family_mask_mirror(f,family,lb,confirm):
    """Arrays in mirrored short-space: prior low = actual prior high."""
    atr14=f['atr']; lo=f['low']; hi=f['high'];cl=f['close']
    prev_low=f['prev_low'][lb];prev_high=f['prev_high'][lb]
    prev_bar_high=np.r_[np.nan,hi[:-1]]  # actual previous candle low negated
    bearish=f['bullish'] & f['valid_atr']
    if family=='FAILED_HIGH_BREAKOUT':
        m=(lo<prev_low)&(cl>prev_low+confirm*atr14)
    elif family=='HIGH_SWEEP_DISPLACEMENT':
        m=(lo<prev_low)&(cl>prev_bar_high+confirm*atr14)
    elif family=='OUTSIDE_REVERSAL':
        m=(lo<prev_low)&(hi>prev_high)&(f['close_loc']>=.60+.80*confirm)
    elif family=='COMPRESSION_BREAKDOWN':
        m=(f['compression']<=1.0-.8*confirm)&(cl>prev_high)
    elif family=='RALLY_REJECTION':
        m=(lo<prev_low)&(cl>prev_low)&(f['mom4']<=-(.50+4*confirm))
    elif family=='DOWNSIDE_BREAKDOWN':
        m=cl>prev_high+confirm*atr14
    else:raise ValueError('Unknown family: '+family)
    ret=np.asarray(bearish&m,dtype=bool)
    ret[:200]=False
    return ret


def family_mask_native(f,family,lb,confirm):
    """SEPARATELY assembled actual-price SELL predicates, not mirror function."""
    high=-f['low'];low=-f['high'];close=-f['close'];op=-f['open'];a=f['atr']
    prevhi=prev_extreme(high,lb,'max');prevlo=prev_extreme(low,lb,'min')
    lastlow=np.r_[np.nan,low[:-1]]
    bearish=(close<op)&np.isfinite(a)&(a>0)
    if family=='FAILED_HIGH_BREAKOUT':
        pred=(high>prevhi)&(close<prevhi-confirm*a)
    elif family=='HIGH_SWEEP_DISPLACEMENT':
        pred=(high>prevhi)&(close<lastlow-confirm*a)
    elif family=='OUTSIDE_REVERSAL':
        cloc=np.divide(close-low,high-low,out=np.full(len(a),np.nan),where=high>low)
        pred=(high>prevhi)&(low<prevlo)&(cloc<=.40-.80*confirm+1e-12)
    elif family=='COMPRESSION_BREAKDOWN':
        pred=(f['compression']<=1-.8*confirm)&(close<prevlo)
    elif family=='RALLY_REJECTION':
        priorrise=-f['mom4']
        pred=(high>prevhi)&(close<prevhi)&(priorrise>=.50+4*confirm)
    elif family=='DOWNSIDE_BREAKDOWN':
        pred=close<prevlo-confirm*a
    else:raise ValueError(family)
    ans=np.asarray(bearish&pred,dtype=bool)
    ans[:200]=False
    return ans


def digest_indices(candles,ix):
    h=hashlib.sha256()
    for i in ix:h.update((iso(candles[i]['time'])+'\n').encode())
    return h.hexdigest()


def digest_ledger(trades):
    h=hashlib.sha256()
    for t in trades:
        h.update((str(t['signal_index'])+'|'+str(t['exit_index'])+'|'+
                  repr(t['result_r'])+'\n').encode())
    return h.hexdigest()


def paired(candles,ix):
    return (backtest(candles,ix,RR_FIXED,PRIMARY_COST),
            backtest(candles,ix,RR_FIXED,STRESS_COST))


def family_parameters(family,lb,confirm):
    return dict(family=family,lookback=lb,confirmation=confirm,
                rule=FAMILY_RULES[family])


def matrix_id(family,lb,confirm,body,rng):
    return f'{family}__LB{lb}__C{confirm:.2f}__B{body:.2f}__R{rng:.2f}'


def trade_public(t):
    return {k:v for k,v in t.items() if k not in ('entry_time','exit_time')}


def rolling(family,ledger,cost):
    rows=[]
    begin=month_floor(HISTORY_FIRST)
    if begin<HISTORY_FIRST:begin=add_months(begin,1)
    last_full=month_floor(FIXED_END)
    for duration in (12,24,36):
        t=begin
        while add_months(t,duration)<=last_full:
            end=add_months(t,duration)
            sr=stats(subset(ledger,t,end))
            rows.append(dict(family=family,assumed_adverse_fill_pips=cost,
                window_months=duration,from_utc=iso(t),to_utc=iso(end),
                trades=sr['trades'],total_r=sr['total_r'],
                profit_factor=sr['profit_factor']))
            t=add_months(t,1)
    return rows


def annual(family,ledger,cost):
    rows=[]
    for year in range(HISTORY_FIRST.year,FIXED_END.year+1):
        left=datetime(year,1,1,tzinfo=timezone.utc)
        right=datetime(year+1,1,1,tzinfo=timezone.utc)
        st=stats(subset(ledger,left,right))
        rows.append(dict(family=family,assumed_adverse_fill_pips=cost,
             calendar_year=year,partial_year=year in (HISTORY_FIRST.year,FIXED_END.year),
             trades=st['trades'],total_r=st['total_r'],
             profit_factor=st['profit_factor']))
    return rows


def matrix_neighbours(rows):
    keys=('family','lookback','confirmation','body_atr_min','range_atr_min')
    table={tuple(row[k] for k in keys):row for row in rows}
    levels=(FAMILIES,MATRIX_LB,MATRIX_CONFIRM,MATRIX_BODY,MATRIX_RANGE)
    output=[]
    for row in rows:
        k=tuple(row[x] for x in keys)
        for j in (1,2,3,4):
            lev=levels[j]; n=lev.index(k[j]);
            if n+1==len(lev):continue
            neighbor=list(k);neighbor[j]=lev[n+1]
            b=table.get(tuple(neighbor))
            if b is None:continue
            output.append(dict(from_id=row['config_id'],to_id=b['config_id'],axis=keys[j],
                 from_value=k[j],to_value=neighbor[j],
                 from_2pip_r=row['2pip_total_r'],to_2pip_r=b['2pip_total_r'],
                 from_4pip_r=row['4pip_total_r'],to_4pip_r=b['4pip_total_r'],
                 from_4pip_trades=row['4pip_trades'],to_4pip_trades=b['4pip_trades']))
    return output


def axis_summary(rows):
    result=[]
    for axis,levels in (('family',FAMILIES),('lookback',MATRIX_LB),
                        ('confirmation',MATRIX_CONFIRM),('body_atr_min',MATRIX_BODY),
                        ('range_atr_min',MATRIX_RANGE)):
        for level in levels:
            bucket=[x for x in rows if x[axis]==level]
            enough=[x for x in bucket if x['4pip_trades']>=50]
            result.append(dict(axis=axis,value=level,configs=len(bucket),
                configs_with_50_trades=len(enough),
                positive_2pip=sum(x['2pip_total_r']>0 for x in enough),
                positive_4pip=sum(x['4pip_total_r']>0 for x in enough),
                median_2pip_expectancy=med([x['2pip_expectancy_r'] for x in enough]),
                median_4pip_expectancy=med([x['4pip_expectancy_r'] for x in enough]),
                description='Descriptive only: highly overlapping configurations; not independent discoveries'))
    return result


def validate_all_mechanisms(candles,f):
    rows=[]
    for family in FAMILIES:
        for lb,confirm in ((20,0.0),(40,.25)):
            mirrored=np.flatnonzero(family_mask_mirror(f,family,lb,confirm)).tolist()
            native=np.flatnonzero(family_mask_native(f,family,lb,confirm)).tolist()
            if mirrored!=native:raise RuntimeError(f'NATIVE SIGNAL PARITY FAILED: {family} LB{lb} confirm{confirm}')
            # Independently compute all accepted trades for each primary raw family.
            for cost in (PRIMARY_COST,STRESS_COST):
                trades=backtest(candles,mirrored,RR_FIXED,cost)
                native_ledger=[];p=0
                while p<len(native):
                    trade=native_short_outcome(candles,native[p],RR_FIXED,cost)
                    if trade is None:p+=1;continue
                    native_ledger.append(trade)
                    p=bisect.bisect_left(native,trade['exit_index'],lo=p+1)
                if len(trades)!=len(native_ledger):
                    raise RuntimeError(f'NATIVE TRADE COUNT PARITY FAILED: {family} {cost}')
                checks=('signal_index','exit_index','reference_entry',
                    'historical_fill','stop','target','result_r','exit_reason')
                for a,b in zip(trades,native_ledger):
                    for field in checks:
                        if field in ('exit_reason',):ok=a[field]==b[field]
                        elif field in ('signal_index','exit_index'):ok=a[field]==b[field]
                        else:ok=abs(a[field]-b[field])<=1e-9
                        if not ok:raise RuntimeError(f'NATIVE FULL LEDGER PARITY FAILED: {family} {cost} {field}')
                rows.append(dict(family=family,lookback=lb,confirmation=confirm,
                   assumed_adverse_fill_pips=cost,raw_signals=len(mirrored),
                   accepted_trades=len(trades),raw_sha256=digest_indices(candles,mirrored),
                   accepted_ledger_sha256=digest_ledger(trades),
                   independent_native_signal_parity='PASS',
                   independent_full_accepted_ledger_parity='PASS'))
    return rows


# ============================================================
# SHORT PASS 2 — FROZEN ANCHORS / ONE CONDITIONAL FACTOR
# PLUS SEPARATE PREDECLARED RANGE BOUNDARY EXPANSION
# ============================================================
# IMPORTANT: do NOT select anchors based on this pass's top row.
# All history has been repeatedly inspected; never call it unseen OOS.
OUTPUT_DIR=Path(os.getenv('AUDJPY_SHORT_PASS4_OUTPUT_DIR','/tmp/audjpy_short_pass4'))
OUTPUT_DIR.mkdir(parents=True,exist_ok=True)
OUTPUT_NAMES=('coverage','archived_bearish_parity','pass3_control_parity',
  'controls','fine_geometry_matrix','momentum_neighbourhood',
  'frequency_single_factor','candidate_marginal_attribution',
  'full_accepted_ledgers','adjacent_geometry','frequency_attribution',
  'rolling_worst_all','calendar_years_controls','full_rolling_controls',
  'edge_counts','methodology','errors')
OUTS={name:str(OUTPUT_DIR/f'audjpy_short_pass4_{name}.csv') for name in OUTPUT_NAMES}
BUNDLE=str(OUTPUT_DIR/'AUDJPY_M15_SHORT_PASS4_FINE_PLATEAU_FREQUENCY_RESULTS.zip')
ANCHORS=(
    ('LB40_B1.25_R2.00',40,1.25,2.00,168,5.3284244540584185),
    ('LB60_B1.00_R2.00',60,1.00,2.00,183,12.512788156428638),
    ('LB60_B1.25_R2.00',60,1.25,2.00,139,12.110963317166263),
)
# Primary 4pip baselines from the actual archived Pass1B matrix above.
# Other fill/costs are recomputed, not tuned to this pass.
# Range explicitly extends on BOTH SIDES of the previous tested 2.00 edge;
# LB80 and B1.50 expand the previously tested lookback/body ceilings too.
BOUNDARY_LB=(40,60,80)
BOUNDARY_BODY=(.75,1.00,1.25,1.50)
BOUNDARY_RANGE=(1.50,1.75,2.00,2.25,2.50,2.75,3.00,3.25,3.50)
EXPECTED_BOUNDARY=len(BOUNDARY_LB)*len(BOUNDARY_BODY)*len(BOUNDARY_RANGE)


def rally_geometry(f,lb,body,range_min):
    """Frozen rally rejection: >=1.50 ATR preceding 16 M15 candles,
    high strictly over previous LB high, close strictly below it; bearish.
    All price arrays in the original PASS1B are MIRRORED to BUY-space.
    """
    return (family_mask_mirror(f,'RALLY_REJECTION',lb,.25)
         & (f['body_atr']>=body)&(f['range_atr']>=range_min))


def conditional_predicates(f):
    """Each mask is exactly ONE condition, compared to its OWN anchor.
    Values are predeclared; no sequential / conjunction tuning here.
    All momentum is prior-only; candle-geometry fields known at close.
    """
    n=f['n']; a=f['atr']; close=f['close']; o=f['open']; high=f['high'];low=f['low']
    ok=np.isfinite(a)&(a>0)
    vals=[]
    def add(group,metric,operator,threshold,array,note=''):
        finite=np.isfinite(array)
        if operator=='>=': mask=finite&(array>=threshold)
        elif operator=='<=':mask=finite&(array<=threshold)
        else:raise ValueError(operator)
        vals.append((f'{group}__{metric}__{operator}__{threshold:.3f}',
             dict(factor_group=group,metric=metric,operator=operator,
                  threshold=threshold,factor_note=note),mask))
    # Original rally signal had prior-16 rise >=1.50 ATR. Lower thresholds
    # are vacuous; only tighten within conditional test.
    prior16rise=-f['mom4']
    for x in (1.75,2.,2.5,3.,4.):
        add('prior_movement','rise_prior16_atr','>=',x,prior16rise,
            'Preceding 16 completed M15 bars; signal ATR denominator')
    for bars in (48,96,192):
        rise=np.full(n,np.nan)
        rise[bars+1:]=np.divide(close[:-(bars+1)]-close[bars:-1],a[bars+1:],
            out=np.full(n-bars-1,np.nan),where=ok[bars+1:])
        # mirrored price: earlier-high minus recent-low => actual rise
        for x in (0.,.75,1.5,2.5):
            add('prior_movement',f'rise_prior{bars}_atr','>=',x,rise,
                'Prior completed M15 closes only; 48/96/192 MARKET bars')
    # A strong mirrored close near high is a strong actual bearish close.
    for x in (.60,.70,.80,.90):
        add('signal_quality','mirrored_close_location','>=',x,f['close_loc'],
            'Equivalent actual bearish candle close in bottom 40/30/20/10%')
    # Mirrored lower wick = actual short upper wick. Mirror body positive.
    for x in (.20,.40,.60,.80):
        add('signal_quality','actual_upper_wick_body','>=',x,f['lower_wick_body'])
    # Prior candle's absolute real body divided by signal ATR.
    prev_body=np.full(n,np.nan)
    prev_body[1:]=np.divide(np.abs(close[:-1]-o[:-1]),a[1:],
        out=np.full(n-1,np.nan),where=ok[1:])
    for x in (.50,.75,1.00,1.25):
        add('previous_bar','prior_body_atr','>=',x,prev_body)
    m20=sma(a,20)
    ratio=np.divide(a,m20,out=np.full(n,np.nan),
        where=np.isfinite(m20)&(m20>0))
    # Always prior ATR state; signal candle ATR only used in baseline geom.
    prior_ratio=np.r_[np.nan,ratio[:-1]]
    for x in (.85,1.,1.10,1.25,1.50):
        add('volatility','previous_m15_atr_ratio20','>=',x,prior_ratio)
    for x in (.70,.85,1.,1.15):
        add('volatility','previous_m15_atr_ratio20','<=',x,prior_ratio)
    # Actual prior high sweep penetration: previous mirrored low - mirrored low.
    for lb in (40,60):
        penetration=np.divide(f['prev_low'][lb]-low,a,out=np.full(n,np.nan),where=ok)
        for x in (.05,.10,.20,.35):
            add('structure',f'prior_high_sweep_LB{lb}_atr','>=',x,penetration)
    # Original target/stop geometry kept fixed; test distance as one factor.
    stop_distance=np.divide(close-(low-STOP_TICKS*TICK),a,
        out=np.full(n,np.nan),where=ok)
    for x in (.60,.80,1.,1.25):
        add('signal_quality','reference_stop_distance_atr','>=',x,stop_distance)
    for x in (1.50,2.,2.5,3.):
        add('signal_quality','reference_stop_distance_atr','<=',x,stop_distance)
    return vals


def accepted_delta(anchor,trades):
    a={t['signal_index']:t for t in anchor}
    b={t['signal_index']:t for t in trades}
    new=b.keys()-a.keys();removed=a.keys()-b.keys();same=b.keys()&a.keys()
    return dict(anchor_trades=len(a),candidate_trades=len(b),
        retained_accepted=len(same),newly_eligible_accepted=len(new),
        removed_accepted=len(removed),new_entries_r=sum(b[k]['result_r'] for k in new),
        removed_entries_r=sum(a[k]['result_r'] for k in removed),
        total_delta_r=stats(trades)['total_r']-stats(anchor)['total_r'],
        interpretation='New entries following full chronology are not an independently tradable strategy')


def rolling_worst(cid,ledger,cost):
    # Full chronological ledger first, slice into completed monthly windows.
    allrows=rolling(cid,ledger,cost)
    out=[]
    for duration in (12,24,36):
        bucket=[r for r in allrows if r['window_months']==duration]
        if not bucket:raise RuntimeError('No completed rolling windows')
        worst=min(bucket,key=lambda r:r['total_r'])
        with_trade=[r for r in bucket if r['trades']>0]
        worst_with_trade=min(with_trade,key=lambda r:r['total_r']) if with_trade else None
        out.append(dict(config_id=cid,assumed_adverse_fill_pips=cost,
          window_months=duration,completed_windows=len(bucket),
          zero_trade_windows=sum(r['trades']==0 for r in bucket),
          worst_window_start_utc=worst['from_utc'],
          worst_window_end_utc=worst['to_utc'],
          worst_total_r=worst['total_r'],worst_trades=worst['trades'],
          worst_with_trade_total_r=(worst_with_trade['total_r'] if worst_with_trade else None),
          worst_with_trade_start_utc=(worst_with_trade['from_utc'] if worst_with_trade else None)))
    return out


def row_for_candidate(kind,cid,axes,candles,ix,base_2,base_4):
    tr2,tr4=paired(candles,ix)
    row=metrics_row(kind,cid,axes,ix,tr2,tr4,stats(base_2))
    attr=[]
    for cost,base,tr in ((2,base_2,tr2),(4,base_4,tr4)):
        delta=accepted_delta(base,tr)
        attr.append(dict(config_id=cid,assumed_adverse_fill_pips=cost,**axes,**delta))
        for k,v in delta.items():
            if k!='interpretation':row[f'{cost}pip_{k}']=v
    return row,attr,tr2,tr4






# ==============================================================
# PASS 4. All candidate families predeclared before Railway run.
# ==============================================================
# Pass3 4-pip controls are an immutable interface; not selected by this run.
# Centre has 78 trades, +36.02554654R and -8R closed drawdown at 4p;
# positive neighbours are correlated, not independent observations.
# Upper range >3.0 and body >=2.0 were sparse; do not extrapolate.
# Fine grid resolves plateaus BETWEEN Pass3 tested values, not outer chase.
FINE_LOOKBACK=(40,50,60,70,80)
FINE_BODY=(1.25,1.40,1.50,1.60,1.75)
FINE_RANGE=(2.00,2.15,2.25,2.35,2.50)
EXPECTED_FINE=len(FINE_LOOKBACK)*len(FINE_BODY)*len(FINE_RANGE)
FROZEN_MOMENTUM=1.50

# Diverse, predeclared anchors from Pass3. The 1.75-body anchor tests
# different body/range economics; 2.50-range anchor is a low-DD comparator.
ANCHORS=(
 ('CENTRE_LB60_B1.50_R2.25',60,1.50,2.25,78,36.02554654285602),
 ('FREQUENCY_LB40_B1.50_R2.25',40,1.50,2.25,97,31.825233593369575),
 ('BODY_LB60_B1.75_R2.00',60,1.75,2.00,71,35.688751865),
 ('RANGE_LB60_B1.50_R2.50',60,1.50,2.50,58,28.721034),
 ('STRUCTURE_LB80_B1.50_R2.25',80,1.50,2.25,72,29.991129),
)
# Approximate controls verified against full published Pass3 rows within
# 0.0005R. The centre & frequency anchors use tighter archived tolerances.
MOMENTUM_THRESHOLDS=(1.25,1.50,1.75,2.00,2.25)
EXPECTED_MOMENTUM=len(ANCHORS)*len(MOMENTUM_THRESHOLDS)
FREQ_LB=(30,40,50,60,70,80,100)
FREQ_BODY=(1.00,1.25,1.40,1.50,1.60,1.75)
FREQ_RANGE=(1.75,2.00,2.15,2.25,2.35,2.50,2.75)
FREQ_MOMENTUM=(1.00,1.25,1.50,1.75,2.00,2.25)
EXPECTED_FREQUENCY=sum(map(len,(FREQ_LB,FREQ_BODY,FREQ_RANGE,FREQ_MOMENTUM)))


def rally_mask(f,lb,body,rng,rise,base_cache):
    # Initial mirror predicate has prior16 rise >=0.50 ATR; additional rise
    # threshold is applied to the same strictly PRIOR completed 16 bars.
    return (base_cache[lb] & (f['body_atr']>=body) &
            (f['range_atr']>=rng) & (-f['mom4']>=rise))


def name_geo(lb,body,rng,rise):
    return f'RALLY_REJECTION__LB{lb}__B{body:.2f}__R{rng:.2f}__M{rise:.2f}'


def ledger_rows(cat,cid,cost,ts):
    return [dict(category=cat,config_id=cid,assumed_fill_pips=cost,
                 **trade_public(t)) for t in ts]


def pass4_run():
    try:
        for path in list(OUTS.values())+[BUNDLE]:
            if os.path.isfile(path):os.remove(path)
        STATUS.update(state='fetch',progress=1,
             message='Read-only fetch of frozen source; no executor or live orders')
        candles=fetch('M15',START,FIXED_END,35)
        if not candles:raise RuntimeError('No candles')
        global HISTORY_FIRST
        HISTORY_FIRST=candles[0]['time']
        cov=hist_coverage('M15',candles)
        fp=midpoint_fingerprint(candles,'M15')
        verified=(cov['first_utc']==EXPECTED_FIRST and
          cov['last_utc']==EXPECTED_LAST and
          cov['count']==EXPECTED_CANDLES and
          fp['sha256_midpoint_ohlc']==EXPECTED_SHA)
        # Never duplicate `timeframe` via dict(**cov,**fp).
        write_csv(OUTS['coverage'],[dict(**cov,
          sha256_midpoint_ohlc=fp['sha256_midpoint_ohlc'],
          expected_first_utc=EXPECTED_FIRST,expected_last_utc=EXPECTED_LAST,
          expected_count=EXPECTED_CANDLES,expected_sha256=EXPECTED_SHA,
          source_parity='PASS' if verified else 'FAIL')])
        if not verified:raise RuntimeError('FROZEN MIDPOINT SOURCE PARITY FAILED')
        STATUS.update(state='controls',progress=20,message='Archived and Pass3 full-ledger controls')
        n=len(candles)
        no_htf={k:np.full(n,np.nan) for k in
          ('close','ema50','ema100','ema200','atr_ratio50')}
        f=features(candles,no_htf,no_htf,no_htf)
        originals=check_native_short_parity(candles,f)
        if any(x['raw_signal_sha256']!=EXPECTED_BEARISH_RAW_SHA or
           x['accepted_ledger_sha256']!=EXPECTED_BEARISH_LEDGER_SHA[x['assumed_adverse_fill_pips']] or
           x['accepted_trades']!=21136 for x in originals):
           raise RuntimeError('ARCHIVED BEARISH CONTROL FAILED')
        write_csv(OUTS['archived_bearish_parity'],originals)
        lookbacks=sorted(set(FINE_LOOKBACK+FREQ_LB+tuple(x[1] for x in ANCHORS)))
        base_cache={lb:family_mask_mirror(f,'RALLY_REJECTION',lb,0.0)
                    for lb in lookbacks}
        controls={};control_parity=[];control_rows=[];all_ledgers=[]
        years=[];roll=[];worst=[]
        for label,lb,body,rng,exp_n,exp_r in ANCHORS:
            mask=rally_mask(f,lb,body,rng,FROZEN_MOMENTUM,base_cache)
            # Native independently recomputes raw short-side high-sweep
            # conditions and exact historical short outcomes at BOTH costs.
            native=(family_mask_native(f,'RALLY_REJECTION',lb,.25)&
                (f['body_atr']>=body)&(f['range_atr']>=rng))
            ix=np.flatnonzero(mask).tolist();nix=np.flatnonzero(native).tolist()
            if ix!=nix:raise RuntimeError('NATIVE RAW PARITY FAILED: '+label)
            t2,t4=paired(candles,ix)
            if len(t4)!=exp_n or abs(stats(t4)['total_r']-exp_r)>(1e-8 if
                   label.startswith(('CENTRE_','FREQUENCY_')) else 5e-4):
                raise RuntimeError('PASS3 COUNT / 4PIP R CONTROL MISMATCH: '+label)
            cid=name_geo(lb,body,rng,FROZEN_MOMENTUM)
            controls[label]=dict(lb=lb,body=body,rng=rng,ix=ix,
                   mask=mask,t2=t2,t4=t4,cid=cid)
            control_rows.append(metrics_row('FROZEN_PASS3_ANCHOR',cid,
                dict(anchor_id=label,lookback=lb,body_atr_min=body,
                   range_atr_min=rng,prior16_rise_min_atr=FROZEN_MOMENTUM),
                ix,t2,t4,stats(t2)))
            for cost,tr in ((2.,t2),(4.,t4)):
                nt=[];p=0
                while p<len(nix):
                    item=native_short_outcome(candles,nix[p],RR_FIXED,cost)
                    if item is None:p+=1;continue
                    nt.append(item)
                    p=bisect.bisect_left(nix,item['exit_index'],lo=p+1)
                if len(nt)!=len(tr):raise RuntimeError('NATIVE ACCEPTED COUNT FAILED '+label)
                for a,b in zip(tr,nt):
                    for field in ('signal_index','exit_index','reference_entry',
                                  'historical_fill','stop','target','result_r','exit_reason'):
                        ok=(a[field]==b[field] if field in
                            ('signal_index','exit_index','exit_reason') else
                            abs(a[field]-b[field])<=1e-9)
                        if not ok:raise RuntimeError(f'NATIVE FULL LEDGER FAILED {label} {cost} {field}')
                control_parity.append(dict(anchor_id=label,cost=cost,
                    source_parity='PASS',raw_native_parity='PASS',
                    full_accepted_native_parity='PASS',accepted_trades=len(tr),
                    total_r=stats(tr)['total_r'],
                    raw_signal_sha256=digest_indices(candles,ix),
                    accepted_ledger_sha256=digest_ledger(tr)))
                all_ledgers.extend(ledger_rows('PASS3_CONTROL',cid,cost,tr))
                years.extend(annual(cid,tr,cost))
                roll.extend(rolling(cid,tr,cost))
                worst.extend(rolling_worst(cid,tr,cost))
        write_csv(OUTS['pass3_control_parity'],control_parity)
        write_csv(OUTS['controls'],control_rows)
        write_csv(OUTS['calendar_years_controls'],years)
        write_csv(OUTS['full_rolling_controls'],roll)
        STATUS.update(state='fine_plateau',progress=35,
            message=f'Frozen geometry centre; {EXPECTED_FINE} predeclared fine neighbours')
        centre=controls['CENTRE_LB60_B1.50_R2.25']
        rows=[];attr=[];grid={};freq_rows=[];freq_attr=[];mom_rows=[];edges=[]
        total=EXPECTED_FINE+EXPECTED_MOMENTUM+EXPECTED_FREQUENCY
        done=0
        def evaluate(category,lb,body,rng,rise,control,run_suffix=''):
            nonlocal done
            cid=name_geo(lb,body,rng,rise)+run_suffix
            ix=np.flatnonzero(rally_mask(f,lb,body,rng,rise,base_cache)).tolist()
            axes=dict(lookback=lb,body_atr_min=body,
                range_atr_min=rng,prior16_rise_min_atr=rise,
                comparison_anchor_id=control['cid'])
            row,delta,t2,t4=row_for_candidate(category,cid,axes,candles,
                  ix,control['t2'],control['t4'])
            attr.extend(delta)
            for cost,ts in ((2,t2),(4,t4)):
                all_ledgers.extend(ledger_rows(category,cid,cost,ts))
                worst.extend(rolling_worst(cid,ts,cost))
            done+=1
            if done%10==0:
                STATUS.update(progress=35+int(58*done/total),
                   message=f'{category}: completed {done}/{total} settings')
            if done%40==0:OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
            return row
        for lb,body,rng in itertools.product(FINE_LOOKBACK,FINE_BODY,FINE_RANGE):
            row=evaluate('FINE_GEOMETRY',lb,body,rng,1.50,centre)
            rows.append(row);grid[(lb,body,rng)]=row
        if len(rows)!=EXPECTED_FINE:raise RuntimeError('INCOMPLETE FINE GRID')
        write_csv(OUTS['fine_geometry_matrix'],rows)
        # Adjacent edges include actual count / total R changes at stressed fill.
        for key,row in grid.items():
            for axis,j,levels in [('lookback',0,FINE_LOOKBACK),
                                  ('body_atr_min',1,FINE_BODY),
                                  ('range_atr_min',2,FINE_RANGE)]:
                at=levels.index(key[j]);
                if at==len(levels)-1:continue
                target=list(key);target[j]=levels[at+1]
                to=grid[tuple(target)]
                edges.append(dict(from_id=row['config_id'],to_id=to['config_id'],
                   axis=axis,from_value=key[j],to_value=target[j],
                   from_4pip_trades=row['4pip_trades'],to_4pip_trades=to['4pip_trades'],
                   from_4pip_total_r=row['4pip_total_r'],
                   to_4pip_total_r=to['4pip_total_r'],
                   from_4pip_max_drawdown_r=row['4pip_max_drawdown_r'],
                   to_4pip_max_drawdown_r=to['4pip_max_drawdown_r']))
        write_csv(OUTS['adjacent_geometry'],edges)
        STATUS.update(state='momentum_neighbourhood',progress=75,
               message=f'{EXPECTED_MOMENTUM} strict prior-only momentum contrasts')
        for label,lb,body,rng,_,_ in ANCHORS:
            c=controls[label]
            for rise in MOMENTUM_THRESHOLDS:
                row=evaluate('PRIOR16_MOMENTUM_NEIGHBOURHOOD',lb,body,rng,rise,c)
                row['anchor_id']=label
                mom_rows.append(row)
        if len(mom_rows)!=EXPECTED_MOMENTUM:raise RuntimeError('INCOMPLETE MOMENTUM GRID')
        write_csv(OUTS['momentum_neighbourhood'],mom_rows)
        STATUS.update(state='frequency_recovery',progress=87,
                message=f'{EXPECTED_FREQUENCY} controlled one-axis relaxations/tightenings')
        base_args=(60,1.50,2.25,1.50)
        axes=(('lookback',FREQ_LB,0),('body_atr_min',FREQ_BODY,1),
              ('range_atr_min',FREQ_RANGE,2),
              ('prior16_rise_min_atr',FREQ_MOMENTUM,3))
        for axis,levels,idx in axes:
            for value in levels:
                vals=list(base_args);vals[idx]=value
                row=evaluate('FREQUENCY_ONE_FACTOR',*vals,centre,run_suffix='__FREQ_'+axis)
                row['changed_axis']=axis;row['changed_value']=value
                freq_rows.append(row)
                freq_attr.extend(dict(config_id=d['config_id'],
                  changed_axis=axis,changed_value=value,
                  **{k:v for k,v in d.items() if k!='config_id'})
                  for d in attr[-2:])
        if len(freq_rows)!=EXPECTED_FREQUENCY:raise RuntimeError('INCOMPLETE FREQUENCY GRID')
        write_csv(OUTS['frequency_single_factor'],freq_rows)
        write_csv(OUTS['frequency_attribution'],freq_attr)
        write_csv(OUTS['candidate_marginal_attribution'],attr)
        write_csv(OUTS['full_accepted_ledgers'],all_ledgers)
        write_csv(OUTS['rolling_worst_all'],worst)
        edge_counts=[]
        for axis,levels in (('lookback',FINE_LOOKBACK),('body_atr_min',FINE_BODY),
                            ('range_atr_min',FINE_RANGE)):
            for level in levels:
                members=[r for r in rows if r[axis]==level]
                with_count=[r for r in members if r['4pip_trades']>=50]
                edge_counts.append(dict(axis=axis,value=level,
                  configurations=len(members),configs_ge50=len(with_count),
                  positive_stress_ge50=sum(r['4pip_total_r']>0 for r in with_count),
                  median_stressed_trades=med([r['4pip_trades'] for r in members]),
                  median_stressed_total_r=med([r['4pip_total_r'] for r in members]),
                  boundary_value=level in (levels[0],levels[-1]),
                  note='Overlapping in-sample geometries, not independent wins'))
        write_csv(OUTS['edge_counts'],edge_counts)
        write_csv(OUTS['methodology'],[
         dict(topic='SCOPE',detail='READ ONLY; Portfolio28 unchanged; no webhook/orders'),
         dict(topic='SOURCE',detail=f'Frozen {EXPECTED_CANDLES} MID M15 bars through {EXPECTED_LAST}; SHA {EXPECTED_SHA}; fail closed'),
         dict(topic='CONTROLS',detail='Archived bearish 2/4p raw+ledger digests and 5 Pass3 full native short-side ledgers 2/4p; fixed controls, no reranking'),
         dict(topic='FINE_PLATEAU',detail=f'{EXPECTED_FINE} fine geometry grid {FINE_LOOKBACK} x {FINE_BODY} x {FINE_RANGE}; prior16 rise>=1.50 ATR'),
         dict(topic='MOMENTUM',detail=f'{EXPECTED_MOMENTUM} one-factor momentum threshold contrasts {MOMENTUM_THRESHOLDS} within 5 individually unchanged anchors'),
         dict(topic='FREQUENCY',detail=f'{EXPECTED_FREQUENCY} single-axis changes against same LB60/B1.50/R2.25/rise1.50 control; 2/4 full chronology + marginal attribution'),
         dict(topic='RISK',detail='Fixed RR3.50, stop actual high+10 ticks, target REFERENCE risk, 2/4 pip adverse SELL fill assumptions; NOT measured live spread/slippage'),
         dict(topic='REPORTING',detail='Every row at both fills, all full accepted ledgers, consecutive-neighbour deltas, worst completed rolling 12/24/36 windows incl zero trades, era/year controls; no automatic selected winner'),
         dict(topic='BOUNDARIES',detail='Pass3 body2.0 and range3.0 were sparse; no outer sweep. A winning fine-grid outer threshold means NOT a validated plateau; mark unresolved rather than extrapolate.'),
         dict(topic='LIMIT',detail='Same repeatedly mined historical sample, NOT unseen OOS. Marginal trades include chronology displacement; no portfolio admission yet.'),
         dict(topic='NEXT',detail='Inspect plateau/weak eras and marginal trade economics; if coherent, one small FINAL confirmation around predeclared centre; freeze entries BEFORE RR study; only then independent & Portfolio28->29')])
        zip_outputs()
        STATUS.update(state='complete',progress=100,source_parity='PASS',
          archived_control_parity='PASS',native_control_ledger_parity='PASS',
          fine_configurations=len(rows),momentum_configurations=len(mom_rows),
          frequency_configurations=len(freq_rows),orders_supported=False,
          trading_enabled=False,result_path='/audjpy-short-pass4/results',
          message='Completed read-only discovery; no candidate automatically frozen')
    except Exception as exc:
        STATUS.update(state='error',message=str(exc),traceback=traceback.format_exc(),
          orders_supported=False,trading_enabled=False)
        write_csv(OUTS['errors'],[dict(error_type=type(exc).__name__,
             error=str(exc),traceback=STATUS['traceback'])])
        zip_outputs()
        print(STATUS['traceback'],flush=True)


@app.route('/')
def home4():
    return jsonify(service='AUDJPY M15 SHORT Pass4 fine plateau and frequency',
       status='/audjpy-short-pass4/status',
       results='/audjpy-short-pass4/results',orders_supported=False,
       trading_enabled=False)

@app.route('/audjpy-short-pass4/status')
def status4():return jsonify(STATUS)

@app.route('/audjpy-short-pass4/results')
def results4():return download(BUNDLE)

if __name__=='__main__':
    threading.Thread(target=pass4_run,daemon=True).start()
    app.run(host='0.0.0.0',port=int(os.getenv('PORT','8080')),
            debug=False,use_reloader=False)
