#!/usr/bin/env python3
"""AUD/JPY M15 LONG — Stage 3: alternative entry mechanisms.

READ ONLY: OANDA midpoint history download only. Never sends orders, never
imports or touches Portfolio 27, live executor or any account state.

Frozen Stage 1 mechanics: ATR14 Wilder SMA seed; prior-only extrema; exactly
completed HTF alignment; reference buy=signal close; stop=low-10 JPY pricing
 ticks (0.010 JPY / one pip); 3.5R target on REFERENCE risk; 2pip adverse
fill baseline and 4pip fill stress, both assumptions not historical spread.
Full chronological per-configuration pyramiding zero, exit candle re-entry.

Every raw Stage 1 family is field-hash checked through its exact M15 cutoff.
27 predeclared settings: 3 independent new raw mechanisms + 8 within-family
controls each. No RR, weekday, session-hour or portfolio optimisation.
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
OUTPUT_DIR=Path(os.getenv("AUDJPY_STAGE3_OUTPUT_DIR", "/tmp/audjpy_m15_long_stage3"))
OUTPUT_DIR.mkdir(parents=True,exist_ok=True)
OUTS={name:str(OUTPUT_DIR/f"audjpy_m15_long_stage3_{name}.csv") for name in (
    "coverage","stage1_parity","mechanisms","branches","neighbours",
    "diagnostic_ledgers","rolling_summary","calendar","session_checks","methods","errors")}
BUNDLE=str(OUTPUT_DIR/"AUDJPY_M15_LONG_STAGE3_ALTERNATIVE_MECHANISMS_RESULTS.zip")

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
                ". Do NOT interpret Stage 3 results; inspect OANDA candle revisions and mechanics.")
    OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
    return checks


# ============================================================
# ============================================================
# PREDECLARED INDEPENDENT STAGE 3 ENTRY MECHANISMS
# Three RAW baselines + 8 controls each; no combinations between families.
# ============================================================
BRANCH_NAMES = ("WASHOUT_RECLAIM", "TOKYO_LOW_RECLAIM", "TREND_PULLBACK")
DIAGNOSTIC_AXES = {
    "WASHOUT_RECLAIM": ("bearish_bars", "prior_decline_atr", "signal_body_atr"),
    "TOKYO_LOW_RECLAIM": ("penetration_atr", "signal_body_atr", "close_location"),
    "TREND_PULLBACK": ("htf_trend", "pullback", "signal_body_atr"),
}
ANCHORS = {
    "WASHOUT_RECLAIM": (2,.75,.50),
    "TOKYO_LOW_RECLAIM": (0.,.50,.65),
    "TREND_PULLBACK": ("CLOSE_ABOVE_H1H4_EMA100", "PRIOR_4BAR_TOUCH", .50),
}


def shift_prior(values, steps, fill=False):
    if steps <= 0:raise ValueError("steps must be positive")
    values=np.asarray(values)
    return np.r_[np.full(steps,fill,dtype=values.dtype),values[:-steps]]


def completed_tokyo_low(candles):
    """Tokyo-local 09:00–14:59 M15 bars; signal only after 15:00 Tokyo.

    Predeclared observation window: London-local 07:00–13:59. At least 16
    completed session candles required (out of normally 24). No session
    high/low from an incomplete or future session is made available.
    """
    sessions={}
    for bar in candles:
        local=bar['time'].astimezone(TOKYO)
        if 9<=local.hour<15:
            day=local.date()
            if day not in sessions:sessions[day]=(bar['low'],1)
            else:
                prev,num=sessions[day]
                sessions[day]=(min(prev,bar['low']),num+1)
    lows=np.full(len(candles),np.nan)
    checks=[]
    for i,bar in enumerate(candles):
        tok=bar['time'].astimezone(TOKYO)
        lon=bar['time'].astimezone(LONDON)
        if tok.hour < 15 or not 7<=lon.hour<14:continue
        row=sessions.get(tok.date())
        if row is None or row[1]<16:continue
        lows[i]=row[0]
    for day,(price,count) in sorted(sessions.items()):
        if len(checks)>=10:break
        checks.append(dict(tokyo_date=str(day),session_m15_count=count,
                           completed_session_low=price,
                           available_only_after_tokyo_1500=True))
    return lows,checks


def declared_mechanisms(f,candles):
    n=f['n']
    op=f['open'];cl=f['close'];hi=f['high'];lo=f['low'];a=f['atr']
    bullish=f['valid_atr'] & f['bullish']
    prior_high=shift_prior(hi,1,fill=np.nan)
    rows=[]

    def add(name,coords,mask,is_raw=False):
        axes=DIAGNOSTIC_AXES[name]
        m=np.asarray(mask & bullish,dtype=bool).copy()
        m[:200]=False
        key='__'.join(f'{axis}_{str(val).replace(".","p")}' for axis,val in zip(axes,coords))
        cid=name+'__'+('RAW' if is_raw else 'CONTROL')+'__'+key
        rows.append((name,cid,dict(zip(axes,coords)),m,is_raw))

    # 1. True multi-bar washout, then current close exceeds previous high.
    # 4-hour/structure/ATR filters from Stage 2 are NOT imported.
    is_bearish=cl<op
    wash={}
    for k in (2,3):
        consecutive=np.ones(n,dtype=bool)
        for j in range(1,k+1):consecutive &= shift_prior(is_bearish,j)
        decline=np.full(n,np.nan)
        decline[k+1:]=(cl[:-(k+1)]-cl[k:-1])/a[k+1:]
        wash[k]=(consecutive & (cl>prior_high),decline)
    add('WASHOUT_RECLAIM',(2,0.,0.),wash[2][0],is_raw=True)
    for k in (2,3):
        for minimum_decline in (.75,1.25):
            for body in (.50,1.00):
                base,decline=wash[k]
                add('WASHOUT_RECLAIM',(k,minimum_decline,body),
                    base & (decline>=minimum_decline) & (f['body_atr']>=body))

    # 2. Completed Tokyo-session low sweep during a SINGLE FIXED London
    # observation window; no search of hours / weekdays.
    session_low,checks=completed_tokyo_low(candles)
    penetration=(session_low-lo)/a
    sweep=(np.isfinite(session_low) & (lo<session_low) & (cl>session_low))
    add('TOKYO_LOW_RECLAIM',(0.,0.,0.),sweep,is_raw=True)
    for depth in (0.,.10):
        for body in (.50,1.00):
            for close_loc in (.65,.80):
                add('TOKYO_LOW_RECLAIM',(depth,body,close_loc),
                    sweep & (penetration>=depth) & (f['body_atr']>=body)
                          & (f['close_loc']>=close_loc))

    # 3. M15 pullback then bullish resumption. HTF indicators and close are
    # aligned to strictly completed H1/H4 bars by features/align_htf.
    ema20=ema(cl,20)
    prev_ema20=shift_prior(ema20,1,fill=np.nan)
    prev_close=shift_prior(cl,1,fill=np.nan)
    prior4_low=prev_extreme(lo,4,'min')
    trend_broad=(f['h1_close']>f['h1_ema100']) & (f['h4_close']>f['h4_ema100'])
    trend_strict=trend_broad & (f['h1_ema50']>f['h1_ema200'])
    rally=(np.isfinite(prev_ema20) & (cl>prior_high) & (cl>prev_ema20))
    touches={
      'PRIOR_4BAR_TOUCH':prior4_low<=prev_ema20,
      'PRIOR_CLOSE_BELOW':prev_close<prev_ema20,
    }
    trends={
      'CLOSE_ABOVE_H1H4_EMA100':trend_broad,
      'H1_EMA50_GT_200':trend_strict,
    }
    add('TREND_PULLBACK',('CLOSE_ABOVE_H1H4_EMA100','PRIOR_4BAR_TOUCH',0.),
        rally & trend_broad & touches['PRIOR_4BAR_TOUCH'],is_raw=True)
    for trend,regime in trends.items():
        for touch,pull in touches.items():
            for body in (.50,1.00):
                add('TREND_PULLBACK',(trend,touch,body),
                    rally & regime & pull & (f['body_atr']>=body))

    if len(rows)!=27 or sum(r[4] for r in rows)!=3 or len(set(r[1] for r in rows))!=27:
        raise RuntimeError('Predeclared geometry count or IDs mismatch')
    if [sum(r[0]==branch for r in rows) for branch in BRANCH_NAMES]!=[9,9,9]:
        raise RuntimeError('Expected nine settings per mechanism')
    return rows,checks


# ============================================================
# FULL-STREAM ANALYSIS — NEVER RESTART P0 INSIDE HISTORICAL SLICES
# ============================================================

def subset(trades,start=None,end=None):
    return [t for t in trades if (start is None or t['entry_time']>=start)
            and (end is None or t['entry_time']<end)]


def report_row(branch,cid,coords,indices,main,stress,is_raw):
    row=dict(branch=branch,config_id=cid,is_raw=is_raw,**coords,
        raw_signals=len(indices),
        **{'full_'+k:v for k,v in stats(main).items()},
        **{'4pip_full_'+k:v for k,v in stats(stress).items()})
    windows=(
      ('before2010',None,datetime(2010,1,1,tzinfo=timezone.utc)),
      ('since2010',datetime(2010,1,1,tzinfo=timezone.utc),None),
      ('era2010_2015',datetime(2010,1,1,tzinfo=timezone.utc),datetime(2016,1,1,tzinfo=timezone.utc)),
      ('era2016_2021',datetime(2016,1,1,tzinfo=timezone.utc),datetime(2022,1,1,tzinfo=timezone.utc)),
      ('era2022_now',datetime(2022,1,1,tzinfo=timezone.utc),None),
      ('last5y',NOW-timedelta(days=365.2425*5),None),
      ('last2y',NOW-timedelta(days=365.2425*2),None),
      ('last1y',NOW-timedelta(days=365.2425),None),
    )
    for name,start,end in windows:
        for prefix,ledger in (('',main),('4pip_',stress)):
            ss=stats(subset(ledger,start,end))
            for k in ('trades','profit_factor','total_r','max_drawdown_r'):
                row[prefix+name+'_'+k]=ss[k]
    row['interpretation']='Exploratory repeatedly examined history; NOT untouched OOS'
    return row


def branch_diagnostics(rows):
    summary=[]
    for branch in BRANCH_NAMES:
        family=[r for r in rows if r['branch']==branch]
        raw=next(r for r in family if r['is_raw'])
        enough=[r for r in family if not r['is_raw'] and r['full_trades']>=50]
        summary.append(dict(branch=branch,settings=len(family),
            raw_trades=raw['full_trades'],raw_2pip_r=raw['full_total_r'],
            raw_4pip_r=raw['4pip_full_total_r'],
            controls_50plus=len(enough),
            positive_2pip=sum(x['full_total_r']>0 for x in enough),
            positive_4pip=sum(x['4pip_full_total_r']>0 for x in enough),
            positive_both_eras_4pip=sum(x['4pip_before2010_total_r']>0
                                  and x['4pip_since2010_total_r']>0 for x in enough),
            positive_last2y_4pip=sum(x['4pip_last2y_total_r']>0 for x in enough),
            note='Diagnostics only; no automatic winner selection'))
    return summary


def neighbours(rows):
    result=[]
    for branch in BRANCH_NAMES:
        family=[r for r in rows if r['branch']==branch and not r['is_raw']]
        axes=DIAGNOSTIC_AXES[branch]
        levels={axis:list(dict.fromkeys(r[axis] for r in family)) for axis in axes}
        coords={tuple(r[a] for a in axes):r for r in family}
        for current in family:
            current_coord=tuple(current[a] for a in axes)
            for j,axis in enumerate(axes):
                value_levels=levels[axis]
                k=value_levels.index(current[axis])
                if k+1>=len(value_levels):continue
                target=list(current_coord);target[j]=value_levels[k+1]
                another=coords.get(tuple(target))
                if another is None:continue
                result.append(dict(branch=branch,axis=axis,
                    from_value=current[axis],to_value=another[axis],
                    from_id=current['config_id'],to_id=another['config_id'],
                    from_trades=current['full_trades'],to_trades=another['full_trades'],
                    from_r_2pip=current['full_total_r'],to_r_2pip=another['full_total_r'],
                    from_r_4pip=current['4pip_full_total_r'],to_r_4pip=another['4pip_full_total_r'],
                    delta_r_2pip=another['full_total_r']-current['full_total_r'],
                    delta_r_4pip=another['4pip_full_total_r']-current['4pip_full_total_r']))
    return result


def add_months_utc(x,months):
    month=x.year*12+x.month-1+months
    return datetime(month//12,month%12+1,1,tzinfo=timezone.utc)


def calendar_rows(cid,trades,assumption):
    rows=[]
    for year in range(2005,NOW.year+1):
        start=datetime(year,1,1,tzinfo=timezone.utc)
        end=datetime(year+1,1,1,tzinfo=timezone.utc)
        ss=stats(subset(trades,start,end))
        rows.append(dict(config_id=cid,cost_pips=assumption,year=year,
            trades=ss['trades'],total_r=ss['total_r'],profit_factor=ss['profit_factor']))
    return rows


def rolling_summary(cid,trades,assumption):
    first=datetime(2004,6,1,tzinfo=timezone.utc)
    end=datetime(NOW.year,NOW.month,1,tzinfo=timezone.utc)
    months=[];month=first
    while month<=end:
        months.append(month);month=add_months_utc(month,1)
    monthly=defaultdict(lambda:[0,0.])
    for t in trades:
        key=(t['entry_time'].year,t['entry_time'].month)
        monthly[key][0]+=1;monthly[key][1]+=t['result_r']
    counts=[0];sums=[0.]
    for m in months:
        n,total=monthly[(m.year,m.month)]
        counts.append(counts[-1]+n);sums.append(sums[-1]+total)
    out=[]
    for length in (12,24,36):
        periods=[]
        for i in range(len(months)-length+1):
            periods.append((sums[i+length]-sums[i],counts[i+length]-counts[i],
                            months[i],add_months_utc(months[i],length)))
        if not periods:continue
        nonzero=[r for r,n,_,_ in periods if n]
        worst=min(periods,key=lambda row:row[0])
        out.append(dict(config_id=cid,cost_pips=assumption,months=length,
            windows=len(periods),active_windows=len(nonzero),
            zero_trade_windows=len(periods)-len(nonzero),
            positive_active_pct=100*sum(x>0 for x in nonzero)/len(nonzero) if nonzero else 0,
            worst_r=worst[0],worst_trades=worst[1],
            worst_from_utc=iso(worst[2]),worst_to_utc=iso(worst[3]),
            median_active_r=med(nonzero)))
    return out


def hist_coverage(label,candles):
    if not candles:raise RuntimeError('No candles for '+label)
    times=[x['time'] for x in candles]
    return dict(timeframe=label,count=len(times),first_utc=iso(times[0]),
        last_utc=iso(times[-1]),
        max_gap_days=max(((b-a).total_seconds()/86400 for a,b in zip(times,times[1:])),default=0.))


def run_research():
    try:
        STATUS.update(state='fetch',progress=1,message='Fetching AUD/JPY M15; research read-only')
        m15=fetch('M15',START,NOW,35)
        if len(m15)<REFERENCE_N:raise RuntimeError('Insufficient M15 candles for parity')
        STATUS.update(state='fetch',progress=15,message='Fetching completed H1/H4/D histories')
        h1=fetch('H1',WARMUP,NOW,180)
        h4=fetch('H4',WARMUP,NOW,700)
        daily=fetch('D',WARMUP,NOW,3500)
        if not all((h1,h4,daily)):raise RuntimeError('Missing HTF history')
        write_csv(OUTS['coverage'],[hist_coverage(name,rows) for name,rows in
                        (('M15',m15),('H1',h1),('H4',h4),('D',daily))])
        STATUS.update(state='features',progress=27,message='Computing strictly completed HTF features')
        times=[x['time'] for x in m15]
        f=features(m15,align_htf(times,htf_state(h1)),
            align_htf(times,htf_state(h4)),align_htf(times,htf_state(daily)))
        STATUS.update(state='parity',progress=35,message='Six Stage 1 raw ledger SHA256 checks')
        parity=stage1_parity(m15,f)
        STATUS.update(state='study',progress=42,
            message='Parity PASS; 3 raw mechanisms and 24 predeclared controls')
        hypotheses,session_checks=declared_mechanisms(f,m15)
        write_csv(OUTS['session_checks'],session_checks)
        results=[];ledgers=[];rolling=[];calendar=[]
        for i,(branch,cid,coords,mask,is_raw) in enumerate(hypotheses):
            indices=np.flatnonzero(mask).tolist()
            two=backtest(m15,indices,RR_FIXED,PRIMARY_COST)
            four=backtest(m15,indices,RR_FIXED,STRESS_COST)
            row=report_row(branch,cid,coords,indices,two,four,is_raw)
            results.append(row)
            for cost,ledger in ((2.,two),(4.,four)):
                rolling.extend(rolling_summary(cid,ledger,cost))
                calendar.extend(calendar_rows(cid,ledger,cost))
                if is_raw or tuple(coords.values())==ANCHORS[branch]:
                    for tr in ledger:
                        ledgers.append(dict(branch=branch,config_id=cid,
                            assumed_fill_pips=cost,is_raw=is_raw,
                            **{key:value for key,value in tr.items()
                               if key not in ('entry_time','exit_time')}))
            # Retain outcome cache across configurations in a family;
            # clear only at a family boundary to limit Railway memory.
            if i==len(hypotheses)-1 or hypotheses[i+1][0]!=branch:
                OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
            write_csv(OUTS['mechanisms'],results)  # checkpoint each row
            STATUS.update(progress=42+int(50*(i+1)/len(hypotheses)),
                message=f'Completed {i+1}/{len(hypotheses)}: {branch}')
        write_csv(OUTS['branches'],branch_diagnostics(results))
        write_csv(OUTS['neighbours'],neighbours(results))
        write_csv(OUTS['diagnostic_ledgers'],ledgers)
        write_csv(OUTS['rolling_summary'],rolling)
        write_csv(OUTS['calendar'],calendar)
        write_csv(OUTS['methods'],[
           dict(topic='scope',detail='AUD/JPY M15 LONG Stage3, research only, 27 settings, portfolio27 untouched'),
           dict(topic='families',detail='3 raw families + 8 within-family controls each, no stage2 entry-filter reuse'),
           dict(topic='parity',detail='Six archived Stage1 raw ledgers checked at fixed 2026-09-23 19:15 UTC (546812 M15 candles)'),
           dict(topic='execution',detail='RR3.5, JPY PIP0.01 TICK0.001, BUY reference close; stop low-10ticks; target reference-risk; actual fill R; half-open pyramiding0'),
           dict(topic='cost',detail='2 and 4 pips adverse assumed historical fill for ALL 27 settings; midpoint not measured bid/ask spread'),
           dict(topic='session',detail='Tokyo local 09:00-14:59 session completed >=15:00; >=16 bars; fixed London local 07:00-13:59 observation; not tuned'),
           dict(topic='htf',detail='H1 and H4 completed strictly by next ACTUAL higher-timeframe candle open; M15 EMA20 previous only'),
           dict(topic='independence',detail='2004-2026 historical eras repeatedly examined; no untouched OOS'),
           dict(topic='selection',detail='No RR/hour/weekday/Stage2-entry-filter/Portfolio27 optimisation or automatic winner'),
           dict(topic='operation',detail='Read-only OANDA candle GET; no orders, trading enabled false')])
        zip_outputs()
        STATUS.update(state='complete',progress=100,stage1_parity='PASS',
            mechanism_rows=len(results),branch_rows=3,
            result_path='/audjpy-m15-long-stage3/results',
            message='AUD/JPY M15 LONG alternative mechanism study complete',
            orders_supported=False,trading_enabled=False)
    except Exception as ex:
        STATUS.update(state='error',message=str(ex),traceback=traceback.format_exc(),
            orders_supported=False,trading_enabled=False)
        try:
            write_csv(OUTS['errors'],[dict(message=str(ex),traceback=STATUS['traceback'],
                                          progress=STATUS.get('progress'))])
            zip_outputs()
        finally:print(STATUS['traceback'],flush=True)


@app.route('/')
def root():
    return jsonify(service='AUD/JPY M15 LONG Stage 3; standalone alternative-entry research',
        state=STATUS['state'],status='/audjpy-m15-long-stage3/status',
        results='/audjpy-m15-long-stage3/results',
        orders_supported=False,trading_enabled=False)


@app.route('/audjpy-m15-long-stage3/status')
def status_route():return jsonify(STATUS)


@app.route('/audjpy-m15-long-stage3/results')
def results_route():return download(BUNDLE)


if __name__=='__main__':
    threading.Thread(target=run_research,daemon=True).start()
    app.run(host='0.0.0.0',port=int(os.getenv('PORT','8080')),
            debug=False,use_reloader=False)
