#!/usr/bin/env python3
"""EUR/AUD M15 SHORT — Stage 3: bounded alternative-entry discovery.

Read-only OANDA MIDPOINT research, NO orders, NO Portfolio 27 inputs.
Four NEW predeclared entry mechanisms; 4 raw baselines + 8 nested controls each = 36 settings.
NO RR/session tuning, automatic winner, Portfolio 27 inputs, or live changes.

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
import hashlib
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
HTTP = requests.Session()  # Keep-alive across hundreds of read-only candle requests.
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
OUTS = {name:f"euraud_m15_short_stage3_{name}.csv" for name in (
    "coverage","stage1_raw_parity","all_mechanisms","branch_diagnostics",
    "neighbourhood","anchor_trade_ledgers","anchor_rolling",
    "anchor_calendar","session_checks","methods","errors")}
BUNDLE="EURAUD_M15_SHORT_STAGE3_ALTERNATIVE_MECHANISMS_RESULTS.zip"

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
    if len(OUTCOME_CACHE)>=50_000:  # Bound per-entry ledger memory on small Railway instances.
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
    response=HTTP.get(f"{BASE}/v3/instruments/{PAIR}/candles",
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
    total=max(1, ((end-start).days + chunk_days-1)//chunk_days + 1)
    lo,hi={"M15":(1,14),"H1":(15,20),"H4":(21,25),"D":(26,27)}[granularity]
    t0=time.monotonic()
    while current<end:
        chunk+=1;nxt=min(current+timedelta(days=chunk_days),end)
        STATUS.update(state="fetch",progress=lo+int((hi-lo)*min(chunk-1,total)/total),
                      message=f"{granularity} candle requests: {chunk}/{total} starting {iso(current)}; "
                              f"{len(seen):,} candles downloaded; {int(time.monotonic()-t0)}s elapsed")
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

# ============================================================
# FROZEN STAGE-1 SHORT CONTROLS — SHA-256 over 12 exact ledger fields
# Derived from the user-supplied Stage-1 result ZIP, NOT calibrated here.
# Truncate to Stage-1's last candle before rerunning each raw p0 ledger.
# ============================================================
FAMILIES=("BEAR_ENGULF","FAILED_UPBREAK","HIGH_SWEEP_DISPLACEMENT",
          "OUTSIDE_REVERSAL","COMPRESSION_BREAKDOWN","RALLY_REJECTION")
REFERENCE_FIRST=parse_time("2004-05-31T20:45:00Z")
REFERENCE_CUTOFF=parse_time("2026-09-23T12:15:00Z")
REFERENCE_CANDLE_COUNT=546829
FROZEN_RAW={
 "BEAR_ENGULF": (19034,"c60cdd013cdf2aa90f4666b386020bc015c729040bd075520d7f5dd94afec5a2"),
 "FAILED_UPBREAK": (16045,"f06c6be201d4559796ca9e1a1ec6207cecec1a7f81c6a562340807dedeaad687"),
 "HIGH_SWEEP_DISPLACEMENT": (2810,"0d9da43bf86b4b6a3ae448572e78b59cc5cd6f27cd7f8b5acd2898200b0d3a51"),
 "OUTSIDE_REVERSAL": (15150,"994ba12e8053649c287505482c381472eb95529ebdfc738c1da9cd2d3b6cab03"),
 "COMPRESSION_BREAKDOWN": (9816,"8461cecb7ecdb208dcd7e2a21766771168e8ae2fee082017a40bc64fbde4af0b"),
 "RALLY_REJECTION": (11831,"54d9b00c95517b9df750a3f30c96e7e331e952dd497ac06933e9e11a66cb455c"),
}
FROZEN_FIELDS=("signal_index","exit_index","entry_time_utc","exit_time_utc",
 "reference_entry","historical_fill","stop","target","result_r",
 "exit_reason","rr","cost_pips")
FROZEN_NUMERIC={"reference_entry","historical_fill","stop","target","result_r","rr","cost_pips"}

# Stage-3 anchors and all signal rules are declared before reviewing results.
FAMILY_AXES={
  "RALLY_BREAKDOWN": ("rising_bars", "prior_rally_atr", "signal_body_atr"),
  "TOKYO_HIGH_RECLAIM": ("penetration_atr", "signal_body_atr", "close_location_max"),
  "TREND_PULLBACK": ("htf_trend", "pullback", "signal_body_atr"),
  "FAILED_DOUBLE_HIGH": ("prior_high_lookback", "retest_tolerance_atr", "signal_body_atr"),
}
ANCHORS={
  "RALLY_BREAKDOWN": (2,.50,.50),
  "TOKYO_HIGH_RECLAIM": (0.,.50,.35),
  "TREND_PULLBACK": ("H1H4_BELOW_EMA100","PRIOR_4BAR_TOUCH",.50),
  "FAILED_DOUBLE_HIGH": (30,.15,.50),
}


def raw_masks(f):
    """Must remain equivalent to Stage-1 SHORT raw masks, including warm-up."""
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


def frozen_row_text(t):
    return "|".join(repr(float(t[k])) if k in FROZEN_NUMERIC else str(t[k])
                    for k in FROZEN_FIELDS)+"\n"


def stage1_raw_parity(candles,f):
    times=f["times"]
    count=bisect.bisect_right(times,REFERENCE_CUTOFF)
    if (not times or times[0]!=REFERENCE_FIRST or count!=REFERENCE_CANDLE_COUNT
        or times[count-1]!=REFERENCE_CUTOFF):
        raise RuntimeError("HARD STOP: Stage-1 SHORT cutoff candle coverage drift: "
                           f"first={iso(times[0]) if times else None}, count={count}, "
                           f"cutoff={iso(times[count-1]) if count else None}")
    old=candles[:count]
    raw=raw_masks(f)
    rows=[]
    for family in FAMILIES:
        OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
        indices=np.flatnonzero(raw[family][:count]).tolist()
        tr=backtest(old,indices,RR_FIXED,PRIMARY_COST)
        digest=hashlib.sha256()
        for t in tr:digest.update(frozen_row_text(t).encode("utf-8"))
        expected_n,expected_hash=FROZEN_RAW[family]
        good=len(tr)==expected_n and digest.hexdigest()==expected_hash
        rows.append(dict(family=family,archived_trades=expected_n,
                         reproduced_trades=len(tr),archived_sha256=expected_hash,
                         reproduced_sha256=digest.hexdigest(),
                         first_utc=iso(times[0]),cutoff_utc=iso(REFERENCE_CUTOFF),
                         cutoff_candles=count,status="PASS" if good else "FAIL"))
        STATUS.update(message=f"Stage 1 raw-ledger parity: {len(rows)}/6 checked; "
                              f"last={family} ({'PASS' if good else 'FAIL'})")
        if not good:
            write_csv(OUTS["stage1_raw_parity"],rows)
            raise RuntimeError("HARD STOP: archived SHORT Stage-1 raw ledger drift: "+family)
    OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
    write_csv(OUTS["stage1_raw_parity"],rows)
    return rows




# ============================================================
# STAGE-3 NEW SHORT ENTRY MECHANISMS (NO Stage-2 overlay reuse)
# ============================================================

def shift_prior(a, steps, fill=False):
    if steps <= 0: raise ValueError(steps)
    return np.r_[np.full(steps,fill,dtype=a.dtype),a[:-steps]]


def completed_tokyo_high(candles):
    """Tokyo 09:00–14:59 high by local day; available only at/after 15:00.

    One predeclared London 07:00–13:59 observation window, NOT a searched
    session filter. No current/unfinished Tokyo candle enters the reference.
    """
    sessions={}
    for c in candles:
        dt=c["time"].astimezone(TOKYO)
        if 9 <= dt.hour < 15:
            day=dt.date()
            old=sessions.get(day)
            sessions[day]=(max(old[0],c["high"]),old[1]+1) if old else (c["high"],1)
    series=np.full(len(candles),np.nan)
    checks=[]
    for i,c in enumerate(candles):
        when=c["time"]
        ld=when.astimezone(LONDON)
        tk=when.astimezone(TOKYO)
        if not (7 <= ld.hour < 14 and tk.hour >= 15):continue
        row=sessions.get(tk.date())
        if row is None or row[1]<16:continue
        series[i]=row[0]
        if len(checks)<15:
            checks.append(dict(signal_time_utc=iso(when),tokyo_day=str(tk.date()),
                               completed_tokyo_high=row[0],session_bar_count=row[1],
                               tokyo_local_hour=tk.hour,london_local_hour=ld.hour))
    return series,checks


def mechanism_masks(f,candles):
    """Four separate raw SHORT families and eight bounded controls per family."""
    n=f["n"];cl=f["close"];op=f["open"];hi=f["high"];lo=f["low"]
    a=f["atr"]
    bearish=f["bearish"] & f["valid_atr"]
    prior_low=shift_prior(lo,1,fill=np.nan)
    prior_close=shift_prior(cl,1,fill=np.nan)
    masks=[]
    def add(family,coords,mask,raw=False):
        axes=FAMILY_AXES[family]
        cid=family+"__"+("RAW" if raw else "CONTROL")+"__"+"__".join(
            f"{axis}{str(v).replace('.','p')}" for axis,v in zip(axes,coords))
        accepted=np.asarray(mask & bearish,dtype=bool)
        accepted[:200]=False
        masks.append((family,cid,dict(zip(axes,coords)),accepted,raw))

    # A: earlier 2/3 rising candles THEN a DIFFERENT bearish breakdown candle.
    rising=cl>op
    rally={}
    for k in (2,3):
        seq=np.ones(n,dtype=bool)
        for j in range(1,k+1):seq &= shift_prior(rising,j)
        size=np.full(n,np.nan)
        size[k+1:]=(cl[k:-1]-cl[:-(k+1)])/a[k+1:]
        rally[k]=(seq & (cl<prior_low),size)
    add("RALLY_BREAKDOWN",(2,0.,0.),rally[2][0],raw=True)
    for k in (2,3):
        for rise in (.50,1.00):
            for body in (.50,1.00):
                base,size=rally[k]
                add("RALLY_BREAKDOWN",(k,rise,body),base & (size>=rise)
                    & (f["body_atr"]>=body))

    # B: high of full COMPLETED Tokyo session, NOT a rolling 10/60-bar high.
    tokyo_high,checks=completed_tokyo_high(candles)
    base=np.isfinite(tokyo_high) & (hi>tokyo_high) & (cl<tokyo_high)
    depth=(hi-tokyo_high)/a
    add("TOKYO_HIGH_RECLAIM",(0.,0.,1.),base,raw=True)
    for depth_min in (0.,.10):
        for body in (.50,1.00):
            for close_max in (.35,.20):
                add("TOKYO_HIGH_RECLAIM",(depth_min,body,close_max),
                    base & (depth>=depth_min) & (f["body_atr"]>=body)
                    & (f["close_loc"]<=close_max))

    # C: previously completed H1/H4 downtrend; M15 pullback then breakdown.
    ema20=ema(cl,20)
    prior_ema=shift_prior(ema20,1,fill=np.nan)
    prior4_high=prev_extreme(hi,4,"max")
    broad=np.isfinite(prior_ema) & (cl<prior_low) & (cl<prior_ema)
    htf=(f["h1_close"]<f["h1_ema100"]) & (f["h4_close"]<f["h4_ema100"])
    regimes={"H1H4_BELOW_EMA100":htf,
             "H1_EMA50_LT_EMA200":htf & (f["h1_ema50"]<f["h1_ema200"])}
    pullbacks={"PRIOR_4BAR_TOUCH":prior4_high>=prior_ema,
               "PRIOR_CLOSE_ABOVE":prior_close>prior_ema}
    raw=broad & htf & pullbacks["PRIOR_4BAR_TOUCH"]
    add("TREND_PULLBACK",("H1H4_BELOW_EMA100","PRIOR_4BAR_TOUCH",0.),raw,raw=True)
    for regime,trend in regimes.items():
        for pullback,touch in pullbacks.items():
            for body in (.50,1.00):
                add("TREND_PULLBACK",(regime,pullback,body),
                    broad & trend & touch & (f["body_atr"]>=body))

    # D: TWO temporally separate local highs, then bearish breakdown.
    # Old high: prior LB bars as seen at i-8 (only i-9 and earlier).
    # First test: i-8..i-4. Second: i-3..i-1. Signal i closes under i-1 low.
    first=np.maximum.reduce([shift_prior(hi,j,fill=np.nan) for j in range(4,9)])
    second=np.maximum.reduce([shift_prior(hi,j,fill=np.nan) for j in range(1,4)])
    prev_high={lb:shift_prior(prev_extreme(hi,lb,"max"),8,fill=np.nan)
               for lb in (30,40,60)}
    def double(lb,tol):
        gap=tol*a
        return (np.isfinite(prev_high[lb]) & (first>=prev_high[lb])
                & (second>=first-gap) & (second<=first+gap)
                & (hi<=first+gap) & (cl<prior_low))
    add("FAILED_DOUBLE_HIGH",(40,.50,0.),double(40,.50),raw=True)
    for lb in (30,60):
        for tol in (.15,.35):
            for body in (.50,1.00):
                add("FAILED_DOUBLE_HIGH",(lb,tol,body),
                    double(lb,tol) & (f["body_atr"]>=body))

    assert len(masks)==36 and sum(m[4] for m in masks)==4,len(masks)
    assert len({m[1] for m in masks})==36
    assert {family:sum(x[0]==family for x in masks) for family in FAMILY_AXES} == {
        family:9 for family in FAMILY_AXES}
    return masks,checks


def neighbourhood(rows):
    """Every one-axis adjacent CONTROL pair; raw baselines never mislabelled neighbours."""
    groups=defaultdict(list)
    for r in rows:
        if not r["is_raw"]:groups[r["family"]].append(r)
    out=[]
    for family,items in groups.items():
        axes=FAMILY_AXES[family]
        levels={k:list(dict.fromkeys(r[k] for r in items)) for k in axes}
        keyed={tuple(r[k] for k in axes):r for r in items}
        for coord,r in keyed.items():
            for j,axis in enumerate(axes):
                lv=levels[axis]
                p=lv.index(r[axis])
                if p+1>=len(lv):continue
                right=list(coord);right[j]=lv[p+1]
                other=keyed.get(tuple(right))
                if other is None:continue
                out.append(dict(family=family,axis=axis,from_id=r["config_id"],
                    to_id=other["config_id"],from_value=r[axis],to_value=other[axis],
                    from_trades=r["full_trades"],to_trades=other["full_trades"],
                    delta_2pip_r=other["full_r"]-r["full_r"],
                    delta_4pip_r=other["4pip_full_r"]-r["4pip_full_r"],
                    delta_last2y_4pip_r=other["4pip_last2y_total_r"]-r["4pip_last2y_total_r"]))
    return out


def branch_diagnostics(rows):
    out=[]
    for family in FAMILY_AXES:
        items=[r for r in rows if r["family"]==family]
        control=[r for r in items if not r["is_raw"] and r["full_trades"]>=50]
        raw=next(r for r in items if r["is_raw"])
        out.append(dict(family=family,total_configs=9,raw_trades=raw["full_trades"],
            raw_2pip_r=raw["full_r"],raw_4pip_r=raw["4pip_full_r"],
            controls_50_trades=len(control),positive_2pip=sum(r["full_r"]>0 for r in control),
            positive_4pip=sum(r["4pip_full_r"]>0 for r in control),
            early_late_positive_4pip=sum(r["4pip_before2010_total_r"]>0
                and r["4pip_since2010_total_r"]>0 for r in control),
            note="Diagnostics only. No automatic winner; no uninspected OOS."))
    return out


def selected(trades,start=None,end=None):
    return [t for t in trades if (start is None or t["entry_time"]>=start)
            and (end is None or t["entry_time"]<end)]


def snapshot(trades):
    ss=stats(trades)
    return dict(trades=ss["trades"],pf=ss["profit_factor"],
                r=ss["total_r"],dd=ss["max_drawdown_r"],
                win_rate=ss["win_rate"],loss_streak=ss["longest_loss_streak"])


def report_row(family,cid,coords,indices,main,stress):
    row=dict(family=family,config_id=cid,**coords,signal_count=len(indices),
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
     ("era2022_present",datetime(2022,1,1,tzinfo=timezone.utc),None))
    for label,start,end in slices:
      for prefix, ledger in (("",main),("4pip_",stress)):
        ss=stats(selected(ledger,start,end))
        for k in ("trades","profit_factor","total_r","max_drawdown_r"):
            row[f"{prefix}{label}_{k}"]=ss[k]
    row["interpretation"]="Exploratory Stage-3 mechanisms, no freeze or untouched OOS"
    return row


def rolling_diag(cid,trades):
    rows=[]
    t=datetime(2004,6,1,tzinfo=timezone.utc)
    limit=datetime(NOW.year,NOW.month,1,tzinfo=timezone.utc)
    for months in (12,24,36):
        start=t
        while add_months(start,months)<=limit:
            end=add_months(start,months)
            ss=stats(selected(trades,start,end))
            rows.append(dict(config_id=cid,months=months,from_utc=iso(start),
                             to_utc=iso(end),trades=ss["trades"],
                             total_r=ss["total_r"],
                             positive_active=ss["total_r"]>0 if ss["trades"] else "NO_TRADES"))
            start=add_months(start,1)
    return rows


def calendar_diag(cid,trades):
    rows=[]
    for year in range(2004,NOW.year+1):
        ss=stats(selected(trades,datetime(year,1,1,tzinfo=timezone.utc),
                          datetime(year+1,1,1,tzinfo=timezone.utc)))
        rows.append(dict(config_id=cid,year=year,trades=ss["trades"],
                         total_r=ss["total_r"],profit_factor=ss["profit_factor"]))
    return rows


def hist_coverage(label,candles):
    times=[x["time"] for x in candles]
    gap=max(((b-a).total_seconds()/86400 for a,b in zip(times,times[1:])),default=0)
    return dict(timeframe=label,count=len(candles),first_utc=iso(times[0]),
                last_utc=iso(times[-1]),max_gap_days=gap)


def execution_self_tests():
    """Test adverse short fill, stop/target, both-touch & exit-candle entry."""
    origin=datetime(2020,1,2,tzinfo=timezone.utc)
    def bar(i,o,h,l,c):
        return dict(time=origin+timedelta(minutes=15*i),
                    open=o,high=h,low=l,close=c)
    sig=bar(0,1.5000,1.5005,1.4995,1.4998)
    stop=sig["high"]+STOP_TICKS*TICK
    target=sig["close"]-RR_FIXED*(stop-sig["close"])
    assert stop>sig["close"]>target
    win=bar(1,1.4990,1.4993,target-.0001,1.4970)
    t=outcome([sig,win],0,RR_FIXED,PRIMARY_COST)
    assert t and t["exit_reason"]=="TARGET" and 0<t["result_r"]<RR_FIXED
    assert t["historical_fill"]<t["reference_entry"]<t["stop"]
    OUTCOME_CACHE.clear()
    lost=bar(1,1.5010,stop+.0001,1.5000,1.5010)
    t=outcome([sig,lost],0,RR_FIXED,PRIMARY_COST)
    assert t and t["exit_reason"]=="STOP" and abs(t["result_r"]+1)<1e-12
    OUTCOME_CACHE.clear()
    both=bar(1,target+.00001,stop+.0001,target-.0001,target)
    assert outcome([sig,both],0,RR_FIXED,PRIMARY_COST)["exit_reason"]=="TARGET"
    OUTCOME_CACHE.clear()
    both=bar(1,stop-.00001,stop+.0001,target-.0001,stop)
    assert outcome([sig,both],0,RR_FIXED,PRIMARY_COST)["exit_reason"]=="STOP"
    OUTCOME_CACHE.clear()
    m=[sig,bar(1,1.5000,1.5015,1.4995,1.4999),
       bar(2,1.4999,1.5018,1.4995,1.4998)]
    ledger=backtest(m,[0,1],RR_FIXED,PRIMARY_COST)
    assert len(ledger)==2 and ledger[0]["exit_index"]==1 and ledger[1]["signal_index"]==1
    OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()


# ============================================================
# READ-ONLY RESEARCH SERVICE, CHECKPOINTS AND FULL ZIP
# ============================================================

def run_research():
    try:
        execution_self_tests()
        STATUS.update(state="fetch",progress=1,message="Fetching full EUR/AUD M15 midpoint candles")
        m15=fetch("M15",START,NOW,35)
        if len(m15)<MIN_M15_BARS:raise RuntimeError(f"Insufficient M15 history: {len(m15)}")
        STATUS.update(state="fetch",progress=15,message="Fetching H1/H4/D (strict completion)")
        h1=fetch("H1",WARMUP,NOW,180)
        h4=fetch("H4",WARMUP,NOW,700)
        daily=fetch("D",WARMUP,NOW,3500)
        if not (h1 and h4 and daily):raise RuntimeError("Missing HTF candles")
        write_csv(OUTS["coverage"],[hist_coverage(k,c) for k,c in
            (("M15",m15),("H1",h1),("H4",h4),("D",daily))])
        STATUS.update(state="features",progress=28,message="Calculating Stage-1-equivalent SHORT features")
        tt=[c["time"] for c in m15]
        f=features(m15,align_htf(tt,htf_state(h1)),
                   align_htf(tt,htf_state(h4)),align_htf(tt,htf_state(daily)))
        STATUS.update(state="parity",progress=37,message="Checking six archived SHORT Stage-1 raw ledgers")
        stage1_raw_parity(m15,f)
        STATUS.update(state="study",progress=43,message="SHORT Stage3: 4 raw mechanisms + 32 fixed controls")
        hypotheses,checks=mechanism_masks(f,m15)
        write_csv(OUTS["session_checks"],checks)
        rows=[];trades_out=[];rolling_out=[];years_out=[]
        start=time.monotonic()
        for j,(family,cid,coords,mask,is_raw) in enumerate(hypotheses):
            ix=np.flatnonzero(mask).tolist()
            main=backtest(m15,ix,RR_FIXED,PRIMARY_COST)
            stress=backtest(m15,ix,RR_FIXED,STRESS_COST)
            r=report_row(family,cid,coords,ix,main,stress)
            r["is_raw"]=is_raw
            rows.append(r)
            if is_raw or tuple(coords.values())==ANCHORS[family]:
                for fill,ledger in (("2pip",main),("4pip",stress)):
                    label=cid+"__"+fill
                    for t in ledger:
                        trades_out.append(dict(config_id=cid,fill_assumption=fill,
                          **{k:v for k,v in t.items() if k not in ("entry_time","exit_time")}))
                    rolling_out.extend(rolling_diag(label,ledger))
                    years_out.extend(calendar_diag(label,ledger))
            # Reuse bounded per-signal outcomes between configurations; drop only p0 ledger.
            BACKTEST_CACHE.clear()
            if j%4==3 or j==len(hypotheses)-1:
                write_csv(OUTS["all_mechanisms"],rows)
                elapsed=time.monotonic()-start
                remain=elapsed*(len(hypotheses)-j-1)/(j+1)
                STATUS.update(progress=43+int(51*(j+1)/len(hypotheses)),
                    message=f"Stage3 {j+1}/36 ({family}); {int(elapsed)}s elapsed, ~{int(remain)}s at current speed")
        write_csv(OUTS["branch_diagnostics"],branch_diagnostics(rows))
        write_csv(OUTS["neighbourhood"],neighbourhood(rows))
        write_csv(OUTS["anchor_trade_ledgers"],trades_out)
        write_csv(OUTS["anchor_rolling"],rolling_out)
        write_csv(OUTS["anchor_calendar"],years_out)
        write_csv(OUTS["methods"],[
          dict(item="scope",detail="Standalone EUR/AUD M15 SHORT; independent mechanisms; Portfolio 27 never loaded"),
          dict(item="grid",detail="Four separate raw baselines + eight predeclared controls per family = 36"),
          dict(item="families",detail="Rally breakdown; completed Tokyo high reclaim; completed H1/H4 downtrend pullback; failed two-high breakdown"),
          dict(item="pricing",detail="2pip adverse assumed SELL reference close fill, 4pip stress; midpoint OHLC NOT measured historical bid/ask"),
          dict(item="risk",detail="RR3.5, stop signal high+10 ticks, target anchored to signal reference close, R measured against actual fill-to-stop"),
          dict(item="pyramiding",detail="Full chronological p0 before period slicing, same exit candle eligible; ties conservative open/extremes approximation"),
          dict(item="tokyo",detail="Tokyo 09:00-14:59 local session completed >=15 Tokyo, >=16 session M15 bars, London 07:00-13:59 local ONLY"),
          dict(item="double_high",detail="Old reference high i-9 & earlier, first peak i-8..i-4, retest peak i-3..i-1, bearish signal i breaks previous candle low"),
          dict(item="htf",detail="H1/H4 align by next ACTUAL HTF candle open; M15 EMA20 pullback uses previous EMA"),
          dict(item="parity",detail="Archived six Stage-1 SHORT exact ledger hashes, reference 546829 candles through 2026-09-23T12:15Z"),
          dict(item="selection",detail="No Stage2 filters, RR/hour/day optimisation, auto winner, multi-trigger union, or portfolio selection"),
          dict(item="scope_of_claim",detail="Full history already extensively investigated; all results exploratory, no untouched OOS"),
          dict(item="runtime",detail="HTTP connection reuse, bounded outcome cache, 4-config checkpoints, serial run, progress ETA")])
        zip_outputs()
        STATUS.update(state="complete",progress=100,stage1_parity="PASS",
          mechanism_rows=len(rows),raw_rows=4,controls=32,
          result_path="/euraud-m15-short-stage3/results",
          message="EUR/AUD M15 SHORT alternative mechanisms complete; no trading enabled")
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
    return jsonify(service="EUR/AUD M15 SHORT Stage 3 read-only alternative-entry research",
         state=STATUS["state"],status="/euraud-m15-short-stage3/status",
         results="/euraud-m15-short-stage3/results",orders_supported=False,trading_enabled=False)


@app.route("/euraud-m15-short-stage3/status")
def status_route():return jsonify(STATUS)


@app.route("/euraud-m15-short-stage3/results")
def results_route():return download(BUNDLE)


if __name__=="__main__":
    threading.Thread(target=run_research,daemon=True).start()
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","8080")),debug=False)
