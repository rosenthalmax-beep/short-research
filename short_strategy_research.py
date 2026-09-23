#!/usr/bin/env python3
"""EUR/AUD M15 SHORT — Stage 1: independent, standalone bearish-edge discovery.

Read-only OANDA MIDPOINT research, NO orders, NO Portfolio 27 inputs.
Six minimal bearish mechanisms and 80 independent overlays per mechanism,
480 one-factor configurations plus six raw baselines. Fixed RR3.50 throughout.
NO multi-factor matrices, RR/session tuning, automatic winner, or live changes.

M15 candles from requested May 2002; actual EUR/AUD historical coverage may
begin in May 2004. H1/H4/D candles aligned only after actual next candle open.
ATR14 Wilder, past-only high/low lookbacks, prior 16-bar M15 momentum.

SHORT entry reference: signal close. Assumed adverse fill = reference minus
2 pips (4-pip stress); neither assumption is measured historical bid/ask.
Stop = signal high + 10 ticks; TP = reference - 3.5 * (stop-reference).
Result R uses actual fill-to-stop risk; exits from NEXT M15 candle onward.
Both stop and TP in one bar: open-to-extreme approximation; TARGET
only when the low is strictly closer to the open than the high. Ties => STOP. Pyramiding 0; exit-candle signals are eligible.
Data quality and OHLC fill ambiguity limit interpretation; all historical
years are exploratory, not unseen out-of-sample observations.
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
MIN_M15_BARS = 100_000
STATUS = dict(state="not_started", progress=0, message="Waiting to start",
              orders_supported=False, trading_enabled=False)
OUTS = {name:f"euraud_m15_short_stage1_{name}.csv" for name in (
    "coverage","raw_families","single_factors","raw_trade_ledgers",
    "raw_cost_stress","raw_rolling","raw_calendar","methods","errors")}
BUNDLE="EURAUD_M15_SHORT_STAGE1_RAW_EDGE_DISCOVERY_RESULTS.zip"

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
# M15 FEATURE CACHE — INDEPENDENT SHORT ORIENTED
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
    bearish = cl < o
    exact_bear = np.zeros(n, dtype=bool)
    exact_bear[1:] = ((cl[:-1] > o[:-1]) & (cl[1:] < o[1:])
                      & (o[1:] >= cl[:-1]) & (cl[1:] <= o[:-1]))
    bear_body = o-cl
    prior_body = np.r_[np.nan, np.abs(cl[:-1] - o[:-1])]
    br = np.full(n, np.nan)
    ok_body = prior_body > 0
    br[ok_body] = bear_body[ok_body] / prior_body[ok_body]
    valid_atr = np.isfinite(a) & (a > 0)
    body_atr = np.full(n, np.nan)
    body_atr[valid_atr] = bear_body[valid_atr] / a[valid_atr]
    candle_range = h-l
    range_atr = np.full(n, np.nan)
    range_atr[valid_atr] = candle_range[valid_atr] / a[valid_atr]
    close_loc = np.full(n, np.nan)
    valid_range = candle_range > 0
    close_loc[valid_range] = (cl[valid_range]-l[valid_range]) / candle_range[valid_range]
    upper_wick = h-np.maximum(o,cl)
    upper_wick_body = np.full(n, np.nan)
    upper_wick_body[bear_body > 0] = upper_wick[bear_body > 0] / bear_body[bear_body > 0]
    compression = np.full(n, np.nan)
    previous_atr = np.r_[np.nan,a[:-1]]
    previous_atr_mean20 = np.r_[np.nan,am20[:-1]]
    ok_comp = (np.isfinite(previous_atr) & np.isfinite(previous_atr_mean20)
               & (previous_atr_mean20 > 0))
    compression[ok_comp] = previous_atr[ok_comp] / previous_atr_mean20[ok_comp]
    lookbacks = [10,20,40,60,80,100,120,165,200]
    prev_low = {lb: prev_extreme(l,lb,"min") for lb in lookbacks}
    prev_high = {lb: prev_extreme(h,lb,"max") for lb in lookbacks}
    dist_high = {}
    for lb in [40,60,80,100,120,165,200]:
        x=np.full(n,np.nan)
        ok=valid_atr & np.isfinite(prev_high[lb])
        x[ok]=np.abs(h[ok]-prev_high[lb][ok])/a[ok]
        dist_high[lb]=x
    # Strict prior 4h momentum: previous completed M15 close minus i-17 close.
    mom4=np.full(n,np.nan)
    mom4[17:]=np.divide(cl[16:-1]-cl[:-17],a[17:],
                        out=np.full(n-17,np.nan),where=valid_atr[17:])
    ny_hour=np.zeros(n,dtype=np.int16)
    ny_weekday=np.zeros(n,dtype=np.int16)
    london_hour=np.zeros(n,dtype=np.int16)
    tokyo_hour=np.zeros(n,dtype=np.int16)
    sydney_hour=np.zeros(n,dtype=np.int16)
    sydney=ZoneInfo("Australia/Sydney")
    for i,t in enumerate(times):
        z=t.astimezone(NY)
        ny_hour[i],ny_weekday[i]=z.hour,z.weekday()
        london_hour[i]=t.astimezone(LONDON).hour
        tokyo_hour[i]=t.astimezone(TOKYO).hour
        sydney_hour[i]=t.astimezone(sydney).hour
    return {
        "n":n,"times":times,"open":o,"high":h,"low":l,"close":cl,
        "atr":a,"valid_atr":valid_atr,"bearish":bearish,
        "exact_bear":exact_bear,"bear_br":br,"body_atr":body_atr,
        "range_atr":range_atr,"close_loc":close_loc,
        "upper_wick_body":upper_wick_body,"compression":compression,
        "prev_low":prev_low,"prev_high":prev_high,
        "structure_dist_high":dist_high,"mom4":mom4,
        "ny_hour":ny_hour,"ny_weekday":ny_weekday,
        "london_hour":london_hour,"tokyo_hour":tokyo_hour,
        "sydney_hour":sydney_hour,
        "h1_close":h1["close"],"h1_ema50":h1["ema50"],
        "h1_ema100":h1["ema100"],"h1_ema200":h1["ema200"],
        "h1_atr":h1["atr_ratio50"],"h4_close":h4["close"],
        "h4_ema100":h4["ema100"],"h4_ema200":h4["ema200"],
        "h4_atr":h4["atr_ratio50"],"d_close":daily["close"],
        "d_ema50":daily["ema50"],"d_ema200":daily["ema200"],
        "d_atr":daily["atr_ratio50"],
    }


# ============================================================
# SHORT BACKTEST — full-ledger p0, time-window metrics never reset p0
# ============================================================
OUTCOME_CACHE = {}
BACKTEST_CACHE = {}


def outcome(candles, signal_index, rr, cost_pips):
    key=(signal_index,round(rr,4),round(cost_pips,4))
    if len(OUTCOME_CACHE)>=250_000:
        OUTCOME_CACHE.clear()
    if key in OUTCOME_CACHE:
        return OUTCOME_CACHE[key]
    candle=candles[signal_index]
    ref=candle["close"]
    stop=candle["high"]+STOP_TICKS*TICK
    reference_risk=stop-ref
    if reference_risk<=0:
        OUTCOME_CACHE[key]=None
        return None
    target=ref-rr*reference_risk
    fill=ref-cost_pips*PIP
    actual_risk=stop-fill
    if actual_risk<=0:
        OUTCOME_CACHE[key]=None
        return None
    for j in range(signal_index+1,len(candles)):
        bar=candles[j]
        stop_hit=bar["high"]>=stop
        target_hit=bar["low"]<=target
        if not (stop_hit or target_hit):
            continue
        if stop_hit and target_hit:
            # Short: high is the STOP side, low is the TARGET side.
            # A tie is resolved to STOP (conservative).
            if abs(bar["low"]-bar["open"]) < abs(bar["open"]-bar["high"]):
                price,reason=target,"TARGET"
            else:
                price,reason=stop,"STOP"
        elif target_hit:
            price,reason=target,"TARGET"
        else:
            price,reason=stop,"STOP"
        trade={
            "signal_index":signal_index,"exit_index":j,
            "entry_time":candle["time"],"exit_time":bar["time"],
            "entry_time_utc":iso(candle["time"]),"exit_time_utc":iso(bar["time"]),
            "reference_entry":ref,"historical_fill":fill,
            "stop":stop,"target":target,"result_r":(fill-price)/actual_risk,
            "exit_reason":reason,"rr":rr,"cost_pips":cost_pips,
        }
        OUTCOME_CACHE[key]=trade
        return trade
    OUTCOME_CACHE[key]=None
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
FAMILIES=("BEAR_ENGULF","FAILED_UPBREAK","HIGH_SWEEP_DISPLACEMENT",
          "OUTSIDE_REVERSAL","COMPRESSION_BREAKDOWN","RALLY_REJECTION")


def raw_masks(f):
    base=f["valid_atr"] & f["bearish"]
    previous_low=np.r_[np.nan,f["low"][:-1]]
    previous_high=np.r_[np.nan,f["high"][:-1]]
    high10=f["prev_high"][10]
    masks={
        "BEAR_ENGULF":base & f["exact_bear"] & (f["bear_br"]>=1.00),
        "FAILED_UPBREAK":base & (f["high"]>high10) & (f["close"]<high10),
        "HIGH_SWEEP_DISPLACEMENT":base & (f["high"]>high10)
                                  & (f["close"]<previous_low),
        "OUTSIDE_REVERSAL":base & (f["high"]>previous_high)
                              & (f["low"]<previous_low),
        "COMPRESSION_BREAKDOWN":base & (f["compression"]<=1.00)
                                  & (f["close"]<f["prev_low"][10]),
        "RALLY_REJECTION":base & (f["high"]>f["prev_high"][20])
                             & (f["close"]<high10) & (f["mom4"]>=0),
    }
    for mask in masks.values():
        mask[:200]=False
    return masks


def one_at_a_time_factors(f):
    rows=[]
    def add(name,mask,kind,value):
        rows.append((name,mask,kind,value))
    for field,label,thresholds in (
        ("body_atr","BODY_ATR_MIN",(.50,.75,1.00,1.25)),
        ("range_atr","RANGE_ATR_MIN",(.75,1.00,1.25,1.50)),
        ("close_loc","CLOSE_LOCATION_MAX",(.45,.35,.25,.15)),
        ("upper_wick_body","UPPER_WICK_BODY_MIN",(.10,.20,.35,.50))):
        for v in thresholds:
            mask=f[field]<=v if field=="close_loc" else f[field]>=v
            add(f"{label}_{v:.2f}",mask,label,v)
    for v in (.25,.50,1.00):
        add(f"PRIOR_4H_RALLY_{v:.2f}",f["mom4"]>=v,"PRIOR_4H_MOMENTUM",v)
    for lb in (40,60,100,165):
        for d in (.10,.25,.50):
            add(f"NEAR_PREV_HIGH_LB{lb}_D{d:.2f}",
                f["structure_dist_high"][lb]<=d,
                f"PRIOR_HIGH_DISTANCE_LB{lb}",d)
    for lb in (20,40,60,100):
        add(f"BREAK_PREV_HIGH_LB{lb}",f["high"]>f["prev_high"][lb],
            "PREV_HIGH_BREAK_LOOKBACK",lb)
    penetration=(f["high"]-f["prev_high"][10])/f["atr"]
    for depth in (.025,.05,.10,.20):
        add(f"PENETRATE_LB10_{depth:.3f}",penetration>=depth,
            "MINIMUM_HIGH_PENETRATION_ATR_LB10",depth)
    for name,a,b in (
        ("H1_CLOSE_LT_EMA100","h1_close","h1_ema100"),
        ("H1_EMA50_LT_EMA200","h1_ema50","h1_ema200"),
        ("H4_CLOSE_LT_EMA100","h4_close","h4_ema100"),
        ("H4_EMA100_LT_EMA200","h4_ema100","h4_ema200"),
        ("D_CLOSE_LT_EMA200","d_close","d_ema200"),
        ("D_EMA50_LT_EMA200","d_ema50","d_ema200")):
        add(name,f[a]<f[b],"COMPLETED_HTF_REGIME",name)
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
    assert len(rows)==80, f"Expected 80 factors, got {len(rows)}"
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
      ("era2004_2009",None,datetime(2010,1,1,tzinfo=timezone.utc)),
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


def rolling(config_id,trades):
    rows=[]
    start=datetime(2004,6,1,tzinfo=timezone.utc)
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
    for year in range(2004,NOW.year):
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


def execution_self_tests():
    """Mechanically verify short TP/SL, fill direction, ties, and p0 exit reuse."""
    base=datetime(2020,1,2,tzinfo=timezone.utc)
    def bar(i,o,h,l,c):
        return dict(time=base+timedelta(minutes=15*i),
                    open=o,high=h,low=l,close=c)
    s=bar(0,1.5000,1.5005,1.4995,1.4998)
    stop=s["high"]+STOP_TICKS*TICK
    target=s["close"]-RR_FIXED*(stop-s["close"])
    assert stop>s["close"]>target
    # profitable short exits from low, with adverse SELL fill below reference
    win=bar(1,1.4990,1.4993,target-0.0001,1.4970)
    tr=outcome([s,win],0,RR_FIXED,PRIMARY_COST)
    assert tr and tr["exit_reason"]=="TARGET"
    assert tr["historical_fill"]<tr["reference_entry"]<tr["stop"]
    assert tr["result_r"]>0 and tr["result_r"]<RR_FIXED
    OUTCOME_CACHE.clear()
    lose=bar(1,1.5010,stop+0.0001,1.5000,1.5010)
    tr=outcome([s,lose],0,RR_FIXED,PRIMARY_COST)
    assert tr and tr["exit_reason"]=="STOP" and abs(tr["result_r"]+1)<1e-12
    OUTCOME_CACHE.clear()
    # Same-bar target & stop with strictly closer low implies target first.
    both=bar(1,target+0.00001,stop+0.0001,target-0.0001,target)
    tr=outcome([s,both],0,RR_FIXED,PRIMARY_COST)
    assert tr["exit_reason"]=="TARGET"
    OUTCOME_CACHE.clear()
    both=bar(1,stop-0.00001,stop+0.0001,target-0.0001,stop)
    tr=outcome([s,both],0,RR_FIXED,PRIMARY_COST)
    assert tr["exit_reason"]=="STOP"
    OUTCOME_CACHE.clear()
    # Half-open holding interval permits a new entry on exit candle.
    m=[s,bar(1,1.5000,1.5015,1.4995,1.4999),
       bar(2,1.4999,1.5018,1.4995,1.4998)]
    r=backtest(m,[0,1],RR_FIXED,PRIMARY_COST)
    assert len(r)==2 and r[0]["exit_index"]==1 and r[1]["signal_index"]==1
    OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()


def run_research():
    try:
        execution_self_tests()
        STATUS.update(state="fetch",progress=1,message="Fetching EUR/AUD M15 midpoint candles")
        m15=fetch("M15",START,NOW,35)
        if len(m15)<MIN_M15_BARS:raise RuntimeError(f"Insufficient EUR/AUD M15 history: {len(m15)}")
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
                OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
            write_csv(OUTS["single_factors"],factor_rows)  # partial progress remains inspectable
            STATUS.update(progress=55+int(40*(j+1)/len(FAMILIES)),
                          message=f"Completed single-factor family {j+1}/{len(FAMILIES)}")
        write_csv(OUTS["methods"],[
            dict(item="scope",detail="EUR/AUD M15 SHORT standalone research only; NO Portfolio27 data or ranking"),
            dict(item="history",detail="Start requested May 2002; actual coverage CSV determines first EUR/AUD bar"),
            dict(item="raw",detail="Six independent bearish mechanism definitions; no parameters inherited from prior LONG research"),
            dict(item="factors",detail="Exactly one overlay per raw family; no interaction/RR/session-combination search"),
            dict(item="cost",detail="Assumed 2pip adverse SELL fill, 4pip stress for all raw and >=50-trade factor rows; midpoint not executable bid/ask"),
            dict(item="entry_exit",detail="SHORT reference close, 10 tick stop above signal high, RR3.5 target below close anchored to reference risk, full history p0 before period slicing"),
            dict(item="htf",detail="Only prior strictly completed H1/H4/D via next actual HTF open"),
            dict(item="no_old_m15_reference",detail="No archived EUR/AUD M15 SHORT ledger exists; no claimed parity to any prior SHORT strategy"),
            dict(item="interpretation",detail="No standalone winner or untouched OOS inferred; historical data previously explored across pair/timeframes"),
            dict(item="next",detail="Inspect directional single-factor trends, sample size, cost and eras before authorising any small conditional study; portfolio only after standalone freeze"),
        ])
        zip_outputs()
        STATUS.update(state="complete",progress=100,raw_families=len(raw_rows),
                      single_factor_rows=len(factor_rows),factors_per_family=len(factors),
                      message="Standalone EUR/AUD M15 SHORT stage 1 complete",
                      result_path="/euraud-m15-short-stage1/results")
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
    return jsonify(service="EUR/AUD M15 SHORT Stage 1 independent standalone research",state=STATUS["state"],
                   status="/euraud-m15-short-stage1/status",
                   results="/euraud-m15-short-stage1/results",
                   orders_supported=False,trading_enabled=False)

@app.route("/euraud-m15-short-stage1/status")
def status_route():
    return jsonify(STATUS)

@app.route("/euraud-m15-short-stage1/results")
def results_route():
    return download(BUNDLE)


if __name__=="__main__":
    threading.Thread(target=run_research,daemon=True).start()
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","8080")),debug=False)
