#!/usr/bin/env python3
"""AUD/JPY M15 LONG — Stage 2: bounded conditional interaction discovery.

READ ONLY: OANDA midpoint history download only. Never sends orders, never
imports or touches Portfolio 27, live executor or any account state.

Frozen Stage 1 mechanics: ATR14 Wilder SMA seed; prior-only extrema; exactly
completed HTF alignment; reference buy=signal close; stop=low-10 JPY pricing
 ticks (0.010 JPY / one pip); 3.5R target on REFERENCE risk; 2pip adverse
fill baseline and 4pip fill stress, both assumptions not historical spread.
Full chronological per-configuration pyramiding zero, exit candle re-entry.

Every raw Stage 1 family is field-hash checked through its exact M15 cutoff.
124 predeclared combinations in five distinct families, including 16 small
outside/pullback comparisons. No RR, weekday, session or portfolio selection.
Historical periods are repeatedly inspected and not untouched OOS.
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
OUTPUT_DIR=Path(os.getenv("AUDJPY_STAGE2_OUTPUT_DIR", "/tmp/audjpy_m15_long_stage2"))
OUTPUT_DIR.mkdir(parents=True,exist_ok=True)
OUTS={name:str(OUTPUT_DIR/f"audjpy_m15_long_stage2_{name}.csv") for name in (
    "coverage","stage1_parity","geometries","branches","neighbours",
    "diagnostic_ledgers","rolling_summary","calendar","methods","errors")}
BUNDLE=str(OUTPUT_DIR/"AUDJPY_M15_LONG_STAGE2_CONDITIONAL_INTERACTIONS_RESULTS.zip")

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




# ============================================================
# IMMUTABLE STAGE 1 RAW-LEDGER PARITY (derived from user-uploaded CSV)
# ============================================================
REFERENCE_FIRST=datetime(2004,5,31,20,45,tzinfo=timezone.utc)
REFERENCE_LAST=datetime(2026,9,23,19,15,tzinfo=timezone.utc)
REFERENCE_N=546812
FROZEN_FIELDS=("signal_index","exit_index","entry_time_utc","exit_time_utc",
    "reference_entry","historical_fill","stop","target","result_r",
    "exit_reason","rr","cost_pips")
FROZEN_FLOAT_FIELDS={"reference_entry","historical_fill","stop","target",
                     "result_r","rr","cost_pips"}
FROZEN_RAW={
 "BULL_ENGULF":(19471,"5b09f312efb0a4e10b8a90b4247b172f58e18232695d76521ebd2272b54f8a0c"),
 "FAILED_BREAKDOWN":(13913,"8a34dd17d66a4a383af073dc604c5009bbe0a0ba01aae685bda69ced9ece000f"),
 "LOW_SWEEP_DISPLACEMENT":(2384,"2f913c2509c58d9d2966f982b6ba1e5f9e41f608104d814eb64ea90d68ae244c"),
 "OUTSIDE_REVERSAL":(13623,"06f8e59313e64acd254776c50d455050ee88bc463e291c1dfe390eb03a54162b"),
 "COMPRESSION_BREAKOUT":(10735,"170dc39b49530d359849a2b8b08855d231b55821ded50b0a1f7ca1f79baa86a2"),
 "PULLBACK_REJECTION":(10358,"52d429f7b0e798018769b62fcf2c598c070a60b384dee90e604e0f9c68286223"),
}
FAMILIES=tuple(FROZEN_RAW)


def ledger_text(trade):
    return "|".join(repr(float(trade[k])) if k in FROZEN_FLOAT_FIELDS
                    else str(trade[k]) for k in FROZEN_FIELDS)+"\n"


def stage1_parity(candles,feat):
    times=[c["time"] for c in candles]
    n=bisect.bisect_right(times,REFERENCE_LAST)
    if not times or times[0]!=REFERENCE_FIRST or n!=REFERENCE_N or times[n-1]!=REFERENCE_LAST:
        raise RuntimeError("Stage-1 M15 coverage drift: first=%s count=%d last=%s expected=%s/%d/%s" % (
            iso(times[0]) if times else "NONE", n,
            iso(times[n-1]) if n else "NONE",iso(REFERENCE_FIRST),REFERENCE_N,iso(REFERENCE_LAST)))
    original_candles=candles[:n]
    original_masks=raw_masks(feat)
    checks=[]
    for family in FAMILIES:
        OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
        indices=np.flatnonzero(original_masks[family][:n]).tolist()
        ledger=backtest(original_candles,indices,RR_FIXED,PRIMARY_COST)
        digest=hashlib.sha256()
        for trade in ledger: digest.update(ledger_text(trade).encode("utf-8"))
        expected_n,expected_hash=FROZEN_RAW[family]
        match=len(ledger)==expected_n and digest.hexdigest()==expected_hash
        checks.append(dict(family=family,expected_trades=expected_n,
            actual_trades=len(ledger),expected_sha256=expected_hash,
            actual_sha256=digest.hexdigest(),result="PASS" if match else "FAIL",
            cutoff_utc=iso(REFERENCE_LAST),candles=n))
        write_csv(OUTS["stage1_parity"],checks)
        if not match:
            raise RuntimeError("Stage-1 RAW LEDGER PARITY FAILED: "+family+
                ". Do NOT interpret Stage 2 results; inspect OANDA candle revisions and mechanics.")
    OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
    return checks


# ============================================================
# PREDECLARED FIVE SEPARATE FAMILIES, 124 CONFIGURATIONS
# ============================================================
BRANCH_NAMES=("FAILED_BREAKDOWN","LOW_SWEEP_DECLINE","COMPRESSION_NEAR_LOW",
              "OUTSIDE_STRUCTURE","PULLBACK_COMPARISON")
DIAGNOSTIC_ANCHORS={
    "FAILED_BREAKDOWN":(40,1.10,.70,1.00),
    "LOW_SWEEP_DECLINE":(40,1.10,.50,.08),
    "COMPRESSION_NEAR_LOW":(60,.50,1.10,1.00),
    "OUTSIDE_STRUCTURE":(40,1.00,.65),
    "PULLBACK_COMPARISON":(20,1.00,.25),
}


def declared_geometries(f):
    rows=[]
    base=f["valid_atr"] & f["bullish"]
    prev_high=np.r_[np.nan,f["high"][:-1]]
    prev_low=np.r_[np.nan,f["low"][:-1]]
    def add(branch,keys,vals,mask):
        mask=np.asarray(base & mask,dtype=bool)
        mask[:200]=False
        coords=dict(zip(keys,vals))
        cid=branch+"__"+"__".join(k+str(v).replace(".","p") for k,v in coords.items())
        rows.append((branch,cid,coords,mask))

    # A: failed move through a strictly prior low followed by full reclaim,
    # large bullish body, strong close and directional range.
    keys=("lookback","body_atr","close_location","range_atr")
    for lb,body,loc,rng in itertools.product((20,40,60),(.90,1.10,1.30),(.70,.85),(1.00,1.50)):
        low=f["prev_low"][lb]
        add("FAILED_BREAKDOWN",keys,(lb,body,loc,rng),
            (f["low"]<low)&(f["close"]>low)
            &(f["body_atr"]>=body)&(f["close_loc"]>=loc)
            &(f["range_atr"]>=rng))

    # B: STRICT past low sweep + previous-16-bar decline; sweep depth in ATR,
    # reversal closes beyond previous M15 HIGH (genuine displacement).
    keys=("lookback","body_atr","prior4h_decline_atr","penetration_atr")
    for lb,body,decline,pen in itertools.product((20,40,60),(.90,1.10,1.30),(.50,1.00),(.00,.08)):
        low=f["prev_low"][lb]
        depth=(low-f["low"])/f["atr"]
        add("LOW_SWEEP_DECLINE",keys,(lb,body,decline,pen),
            (f["low"]<low)&(depth>=pen)&(f["close"]>prev_high)
            &(f["mom4"]<=-decline)&(f["body_atr"]>=body))

    # C: expansion after PREVIOUS-candle ATR compression, near prior low,
    # closing above STRICT previous 10-bar high.
    keys=("lookback","distance_atr","body_atr","compression_max")
    for lb,dist,body,comp in itertools.product((40,60,100),(.25,.50),(.90,1.10,1.30),(.85,1.00)):
        add("COMPRESSION_NEAR_LOW",keys,(lb,dist,body,comp),
            (f["compression"]<=comp)&(f["close"]>f["prev_high"][10])
            &(f["structure_dist_low"][lb]<=dist)
            &(f["body_atr"]>=body))

    # D: small structural outside-candle comparison. Not merged with A/B/C.
    keys=("lookback","body_atr","close_location")
    outside=(f["low"]<prev_low)&(f["high"]>prev_high)
    for lb,body,loc in itertools.product((40,60),(1.00,1.25),(.65,.80)):
        add("OUTSIDE_STRUCTURE",keys,(lb,body,loc),
            outside&(f["structure_dist_low"][lb]<=.50)
            &(f["body_atr"]>=body)&(f["close_loc"]>=loc))

    # E: small pullback/rejection comparison, preserves Stage 1 prior-low
    # reclaim trigger and tests strong candle after a completed 4h decline.
    keys=("lookback","body_atr","prior4h_decline_atr")
    for lb,body,decline in itertools.product((20,40),(1.00,1.25),(.25,.75)):
        add("PULLBACK_COMPARISON",keys,(lb,body,decline),
            (f["low"]<f["prev_low"][lb])
            &(f["close"]>f["prev_low"][10])
            &(f["body_atr"]>=body)&(f["mom4"]<=-decline))
    assert len(rows)==124 and len({r[1] for r in rows})==124,len(rows)
    assert {b:sum(r[0]==b for r in rows) for b in BRANCH_NAMES}=={
        "FAILED_BREAKDOWN":36,"LOW_SWEEP_DECLINE":36,
        "COMPRESSION_NEAR_LOW":36,"OUTSIDE_STRUCTURE":8,"PULLBACK_COMPARISON":8}
    return rows


def subset(trades,start=None,end=None):
    return [t for t in trades if (start is None or t["entry_time"]>=start)
            and (end is None or t["entry_time"]<end)]


def report_row(branch,cid,coords,indices,main,stressed):
    a=stats(main);b=stats(stressed)
    row=dict(branch=branch,config_id=cid,**coords,raw_signals=len(indices),
             **{("full_"+k):v for k,v in a.items()},
             **{("4pip_full_"+k):v for k,v in b.items()})
    windows=(
      ("before2010",None,datetime(2010,1,1,tzinfo=timezone.utc)),
      ("since2010",datetime(2010,1,1,tzinfo=timezone.utc),None),
      ("era2010_2015",datetime(2010,1,1,tzinfo=timezone.utc),datetime(2016,1,1,tzinfo=timezone.utc)),
      ("era2016_2021",datetime(2016,1,1,tzinfo=timezone.utc),datetime(2022,1,1,tzinfo=timezone.utc)),
      ("era2022_now",datetime(2022,1,1,tzinfo=timezone.utc),None),
      ("last5y",NOW-timedelta(days=365.2425*5),None),
      ("last2y",NOW-timedelta(days=365.2425*2),None),
      ("last1y",NOW-timedelta(days=365.2425),None),
    )
    for name,lo,hi in windows:
        for prefix,ledger in (("",main),("4pip_",stressed)):
            ss=stats(subset(ledger,lo,hi))
            for k in ("trades","profit_factor","total_r","max_drawdown_r"):
                row[prefix+name+"_"+k]=ss[k]
    row["interpretation"]="Exploratory historical conditional study; NOT independent OOS"
    return row


def branches(rows):
    out=[]
    for branch in BRANCH_NAMES:
        rr=[r for r in rows if r["branch"]==branch]
        enough=[r for r in rr if r["full_trades"]>=50]
        out.append(dict(branch=branch,geometries=len(rr),at_least_50_trades=len(enough),
            positive_2pip=sum(r["full_total_r"]>0 for r in enough),
            positive_4pip=sum(r["4pip_full_total_r"]>0 for r in enough),
            positive_both_eras_4pip=sum(r["4pip_before2010_total_r"]>0 and r["4pip_since2010_total_r"]>0 for r in enough),
            positive_last2y_4pip=sum(r["4pip_last2y_total_r"]>0 for r in enough),
            robust_early_late_recent_4pip=sum(r["4pip_before2010_total_r"]>0 and r["4pip_since2010_total_r"]>0
                           and r["4pip_last2y_total_r"]>0 for r in enough),
            note="Diagnostic counts only; no auto-winner"))
    return out


def neighbours(rows):
    """ALL adjacent coordinates, report changes including failed neighbours."""
    out=[]
    for branch in BRANCH_NAMES:
        items=[r for r in rows if r["branch"]==branch]
        axes=list(DIAGNOSTIC_AXES[branch]); levels={a:sorted({r[a] for r in items}) for a in axes}
        by_coord={tuple(r[a] for a in axes):r for r in items}
        for r in items:
            orig=tuple(r[a] for a in axes)
            for i,a in enumerate(axes):
                pos=levels[a].index(r[a]);
                if pos+1>=len(levels[a]):continue
                other=list(orig);other[i]=levels[a][pos+1]
                s=by_coord.get(tuple(other))
                if s is None:continue
                out.append(dict(branch=branch,axis=a,from_value=r[a],to_value=s[a],
                    from_id=r["config_id"],to_id=s["config_id"],
                    from_n=r["full_trades"],to_n=s["full_trades"],
                    from_r=r["full_total_r"],to_r=s["full_total_r"],
                    from_4pip_r=r["4pip_full_total_r"],to_4pip_r=s["4pip_full_total_r"],
                    delta_r=s["full_total_r"]-r["full_total_r"],
                    delta_4pip_r=s["4pip_full_total_r"]-r["4pip_full_total_r"]))
    return out

DIAGNOSTIC_AXES={
 "FAILED_BREAKDOWN":("lookback","body_atr","close_location","range_atr"),
 "LOW_SWEEP_DECLINE":("lookback","body_atr","prior4h_decline_atr","penetration_atr"),
 "COMPRESSION_NEAR_LOW":("lookback","distance_atr","body_atr","compression_max"),
 "OUTSIDE_STRUCTURE":("lookback","body_atr","close_location"),
 "PULLBACK_COMPARISON":("lookback","body_atr","prior4h_decline_atr"),
}


def add_months_utc(x,months):
    m=x.year*12+x.month-1+months
    return datetime(m//12,m%12+1,1,tzinfo=timezone.utc)


def calendar_rows(cid,trades,assumption):
    rows=[]
    for year in range(2005,NOW.year+1):
        a=datetime(year,1,1,tzinfo=timezone.utc)
        b=datetime(year+1,1,1,tzinfo=timezone.utc)
        s=stats(subset(trades,a,b))
        rows.append(dict(config_id=cid,cost_pips=assumption,year=year,
                         trades=s["trades"],total_r=s["total_r"],profit_factor=s["profit_factor"]))
    return rows


def rolling_summary(cid,trades,assumption):
    # Monthly entries -> fast prefix of chronological sums, no p0 restart.
    first=datetime(2004,6,1,tzinfo=timezone.utc)
    end=datetime(NOW.year,NOW.month,1,tzinfo=timezone.utc)
    months=[];month=first
    while month<=end:
        months.append(month);month=add_months_utc(month,1)
    by_month=defaultdict(lambda:[0,0.0])
    for t in trades:
        tm=t["entry_time"]
        key=(tm.year,tm.month)
        by_month[key][0]+=1;by_month[key][1]+=t["result_r"]
    counts=[0];total=[0.0]
    for m in months:
        v=by_month[(m.year,m.month)]
        counts.append(counts[-1]+v[0]);total.append(total[-1]+v[1])
    rows=[]
    for period in (12,24,36):
        results=[];active=[];no_trade=0
        for start in range(0,len(months)-period):
            n=counts[start+period]-counts[start]
            r=total[start+period]-total[start]
            results.append((r,n,months[start],months[start+period]))
            if n:active.append(r)
            else:no_trade+=1
        if not results:continue
        worst=min(results,key=lambda v:v[0])
        rows.append(dict(config_id=cid,cost_pips=assumption,months=period,
            windows=len(results),active_windows=len(active),zero_trade_windows=no_trade,
            positive_active_pct=100*sum(v>0 for v in active)/len(active) if active else 0,
            worst_r=worst[0],worst_trades=worst[1],worst_from_utc=iso(worst[2]),
            worst_to_utc=iso(worst[3]),median_active_r=med(active)))
    return rows


def hist_coverage(label,candles):
    if not candles:raise RuntimeError("No candles for "+label)
    ts=[v["time"] for v in candles]
    return dict(timeframe=label,count=len(ts),first_utc=iso(ts[0]),last_utc=iso(ts[-1]),
         max_gap_days=max(((b-a).total_seconds()/86400 for a,b in zip(ts,ts[1:])),default=0))


def run_research():
    try:
        STATUS.update(state="fetch",progress=1,message="AUD/JPY M15 candles; no orders")
        m15=fetch("M15",START,NOW,35)
        if len(m15)<REFERENCE_N:
            raise RuntimeError("Insufficient M15 data for Stage-1 parity: "+str(len(m15)))
        STATUS.update(progress=14,message="Fetching completed H1, H4, D context for parity")
        h1=fetch("H1",WARMUP,NOW,180)
        h4=fetch("H4",WARMUP,NOW,700)
        daily=fetch("D",WARMUP,NOW,3500)
        if not all((h1,h4,daily)):raise RuntimeError("Missing HTF data")
        write_csv(OUTS["coverage"],[hist_coverage(k,v) for k,v in
                         (("M15",m15),("H1",h1),("H4",h4),("D",daily))])
        STATUS.update(state="features",progress=27,message="Computing causal M15 and completed HTF features")
        times=[x["time"] for x in m15]
        f=features(m15,align_htf(times,htf_state(h1)),
              align_htf(times,htf_state(h4)),align_htf(times,htf_state(daily)))
        STATUS.update(state="parity",progress=34,message="Six SHA256 Stage-1 raw ledger checks")
        checks=stage1_parity(m15,f)
        STATUS.update(state="grid",progress=42,message="Parity PASS; starting 124 predeclared geometries")
        grid=declared_geometries(f)
        assert [sum(r[0]==b for r in grid) for b in BRANCH_NAMES]==[36,36,36,8,8]
        results=[];diag_ledgers=[];roll=[];calendar=[]
        for j,(branch,cid,coords,mask) in enumerate(grid):
            ix=np.flatnonzero(mask).tolist()
            base=backtest(m15,ix,RR_FIXED,PRIMARY_COST)
            stress=backtest(m15,ix,RR_FIXED,STRESS_COST)
            results.append(report_row(branch,cid,coords,ix,base,stress))
            for cost,ledger in ((2.0,base),(4.0,stress)):
                roll.extend(rolling_summary(cid,ledger,cost))
                calendar.extend(calendar_rows(cid,ledger,cost))
                if tuple(coords.values())==DIAGNOSTIC_ANCHORS[branch]:
                    for t in ledger:
                        diag_ledgers.append(dict(config_id=cid,branch=branch,assumed_fill_pips=cost,
                            **{k:v for k,v in t.items() if k not in ("entry_time","exit_time")}))
            # Keep expensive outcomes cached for entire branch; bounded by outcome().
            if j==len(grid)-1 or grid[j+1][0]!=branch:
                OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
            if (j+1)%8==0 or j+1==len(grid):
                write_csv(OUTS["geometries"],results)
                STATUS.update(progress=42+int(50*(j+1)/len(grid)),
                    message="Geometry %d/%d; %s"%(j+1,len(grid),branch))
        write_csv(OUTS["geometries"],results)
        write_csv(OUTS["branches"],branches(results))
        write_csv(OUTS["neighbours"],neighbours(results))
        write_csv(OUTS["diagnostic_ledgers"],diag_ledgers)
        write_csv(OUTS["rolling_summary"],roll)
        write_csv(OUTS["calendar"],calendar)
        write_csv(OUTS["methods"],[
          dict(item="scope",detail="AUD/JPY M15 LONG exploratory independent Stage 2, READ ONLY, no Portfolio 27"),
          dict(item="geometry",detail="124 predeclared: failed-breakdown36, low-sweep36, compression36, outside8, pullback8; separate ledgers"),
          dict(item="parity",detail="Six exact SHA256 Stage1 raw trade-ledgers through 2026-09-23 19:15 UTC, 546812 M15 candles"),
          dict(item="execution",detail="RR3.5; JPY PIP .01 TICK .001; 10-tick stop; BUY close reference, target reference risk"),
          dict(item="cost",detail="2 and 4 assumed adverse fill pips ALL 124 settings; OANDA midpoint not measured spread"),
          dict(item="control",detail="Full chronological p0 then period slicing; stop-target same-bar nearer-side-to-open tie STOP"),
          dict(item="timing",detail="No session/weekday refinement, no RR optimisation, no combined triggers"),
          dict(item="validation",detail="Historical eras are exploratory; no genuinely untouched holdout because prior pair research inspected eras"),
          dict(item="next",detail="Only coherent neighbourhood with sample, early/late/recent and 4pip cushion merits further standalone testing")])
        zip_outputs()
        STATUS.update(state="complete",progress=100,stage1_parity="PASS",
            geometry_rows=len(results),branch_rows=len(BRANCH_NAMES),
            message="AUD/JPY M15 LONG conditional research complete",
            result_path="/audjpy-m15-long-stage2/results",orders_supported=False,trading_enabled=False)
    except Exception as exc:
        STATUS.update(state="error",message=str(exc),traceback=traceback.format_exc(),
                      orders_supported=False,trading_enabled=False)
        try:
            write_csv(OUTS["errors"],[dict(error=str(exc),traceback=STATUS["traceback"],
                        progress=STATUS.get("progress"),state=STATUS["state"])])
            zip_outputs()
        finally:
            print(STATUS["traceback"],flush=True)


@app.route("/")
def root():
    return jsonify(service="AUD/JPY M15 LONG standalone Stage 2",
        state=STATUS["state"],status="/audjpy-m15-long-stage2/status",
        results="/audjpy-m15-long-stage2/results",
        orders_supported=False,trading_enabled=False)

@app.route("/audjpy-m15-long-stage2/status")
def status_route():return jsonify(STATUS)

@app.route("/audjpy-m15-long-stage2/results")
def results_route():return download(BUNDLE)

if __name__=="__main__":
    threading.Thread(target=run_research,daemon=True).start()
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","8080")),debug=False,use_reloader=False)
