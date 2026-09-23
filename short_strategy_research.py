#!/usr/bin/env python3
"""AUD/JPY M15 LONG — ENGULFING PASS 2: CONDITIONAL FEATURES.

Three PREDECLARED anchors are frozen at the 2026-09-23 20:45 UTC Pass 1
cutoff, then a one-factor-at-a-time diagnostic is run INSIDE each anchor.
No RR, session or portfolio optimisation, no live orders, no automatic winner.
Future plateau stage MUST extend boundaries if historical improvement continues
at the edge; a winning boundary is NOT an optimum. Repeatedly inspected history.

Uses exactly the Pass 1 OANDA midpoint, 2/4-pip assumed adverse fills,
previous-only structure and completed HTF context, fixed RR3.5 and p0 model.
2/4 pip are modelling assumptions, not measured AUD/JPY executable costs.
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
OUTPUT_DIR=Path(os.getenv("AUDJPY_ENGULF_MATRIX1_OUTPUT_DIR", "/tmp/audjpy_engulf_matrix1"))
OUTPUT_DIR.mkdir(parents=True,exist_ok=True)
OUTS={name:str(OUTPUT_DIR/f"audjpy_engulf_matrix1_{name}.csv") for name in (
    "coverage","raw_engulf_parity","raw_engulf_control","raw_engulf_trades",
    "expanded_single_factors","engulf_geometry_matrix","matrix_neighbours",
    "matrix_levels","fixed_anchor_ledgers","rolling_fixed_anchors",
    "methodology","errors")}
BUNDLE=str(OUTPUT_DIR/"AUDJPY_M15_LONG_ENGULFING_MATRIX_PASS1_RESULTS.zip")

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
    for lb in [20, 40, 60, 80, 100, 120, 165, 200]:
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


def raw_engulf_parity(candles,feat):
    """Exact independent 2026-09-23 19:15 UTC raw-ledger hash check.

    Later candles are deliberately excluded from this parity test: a trade
    opened before cutoff but exiting after it is not counted in the original
    frozen ledger. Full-current data is used AFTER this check.
    """
    times=[c["time"] for c in candles]
    n=bisect.bisect_right(times,REFERENCE_LAST)
    if not times or times[0]!=REFERENCE_FIRST or n!=REFERENCE_N or times[n-1]!=REFERENCE_LAST:
        raise RuntimeError("Frozen M15 coverage changed: first=%s prefix=%s last=%s" % (
            iso(times[0]) if times else "NONE", n, iso(times[n-1]) if n else "NONE"))
    indices=np.flatnonzero(raw_masks(feat)["BULL_ENGULF"][:n]).tolist()
    OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
    ledger=backtest(candles[:n],indices,RR_FIXED,PRIMARY_COST)
    digest=hashlib.sha256()
    for trade in ledger:digest.update(ledger_text(trade).encode("utf-8"))
    expected_n, expected_hash=FROZEN_RAW["BULL_ENGULF"]
    actual_hash=digest.hexdigest()
    check=dict(family="BULL_ENGULF",expected_n=expected_n,actual_n=len(ledger),
               expected_sha256=expected_hash,actual_sha256=actual_hash,
               frozen_cutoff_utc=iso(REFERENCE_LAST),prefix_candles=n,
               result="PASS" if len(ledger)==expected_n and actual_hash==expected_hash else "FAIL")
    write_csv(OUTS["raw_engulf_parity"],[check])
    if check["result"]!="PASS":
        raise RuntimeError("AUD/JPY bullish-engulfing frozen trade-ledger parity FAILED; "+
          "stop research and inspect OANDA history/mechanics before interpreting results")
    OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
    return check



# ============================================================

def hist_coverage(label,candles):
    if not candles:raise RuntimeError('No candles for '+label)
    ts=[x['time'] for x in candles]
    return dict(timeframe=label,count=len(ts),first_utc=iso(ts[0]),last_utc=iso(ts[-1]),
        max_gap_days=max(((b-a).total_seconds()/86400 for a,b in zip(ts,ts[1:])),default=0))

def add_months_utc(value,n):
    return add_months(value,n)

# ============================================================
# ENGULFING-ONLY EXPLORATORY FEATURES (no replacement of archived features)
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
    """Old-style factor scan: independent predicates on raw engulfing ONLY.

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
        ('close_loc',(.55,.65,.75,.85,.90)),
        ('lower_wick_body',(.10,.20,.30,.40,.60)),
        ('m15_atr_ratio20',(.70,.85,1.00,1.15,1.30)),
        ('prev_body_atr',(.25,.50,.75,1.00,1.25)),
    ]:
        x=e.get(name,f.get(name))
        for v in vals:add(name,'min',v,x>=v)
    for v in (.10,.20,.30,.50,.75):
        add('upper_wick_body','max',v,e['upper_wick_body']<=v)
    for v in (1.00,1.25,1.50,2.00,2.50,3.00):
        add('stop_atr','max',v,e['stop_atr']<=v)
    for lb in GEOMETRY_LB:
        for d in (.05,.10,.25,.50,1.00):
            add('distance_to_prior_low_'+str(lb),'max',d,
                f['structure_dist_low'][lb]<=d)
        for p in (.00,.05,.10,.20):
            add('low_penetration_'+str(lb),'min',p,
                e['penetration_'+str(lb)]>=p)
    for name in ('mom4','mom12','mom24','mom48'):
        for v in (.00,-.50,-1.00,-1.50):
            add('prior_'+name+'_decline','max',v,e[name]<=v)
        for v in (.00,.50,1.00):
            add('prior_'+name+'_rise','min',v,e[name]>=v)
    for name,lhs,rhs in (
        ('h1_close_gt_ema50','h1_close','h1_ema50'),
        ('h1_close_gt_ema100','h1_close','h1_ema100'),
        ('h1_ema50_gt_ema200','h1_ema50','h1_ema200'),
        ('h4_close_gt_ema100','h4_close','h4_ema100'),
        ('h4_ema100_gt_ema200','h4_ema100','h4_ema200'),
        ('daily_close_gt_ema50','d_close','d_ema50'),
        ('daily_close_gt_ema200','d_close','d_ema200'),
        ('daily_ema50_gt_ema200','d_ema50','d_ema200'),
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
# PASS 2: FROZEN ARCHIVED ANCHORS + CONDITIONAL FEATURE STUDY
# ============================================================
PASS2_CUTOFF=datetime(2026,9,23,20,45,tzinfo=timezone.utc)
PASS2_CANDLES=546818
ANCHORS=(
    ('TIGHT_LB100',100,.05,.50,1.25,136,21.269462799230844,6.959336405264406),
    ('TIGHT_LB40',40,.05,1.00,1.25,169,18.610626380214256,.23463667419199496),
    ('BROAD_LB60',60,.50,1.00,1.50,632,31.419163602295654,-28.007510150158648),
)
OUTPUT_DIR=Path(os.getenv('AUDJPY_ENGULF_PASS2_OUTPUT_DIR','/tmp/audjpy_engulf_pass2'))
OUTPUT_DIR.mkdir(parents=True,exist_ok=True)
OUTS={name:str(OUTPUT_DIR/f'audjpy_engulf_pass2_{name}.csv') for name in (
 'coverage','raw_engulf_parity','anchor_parity','anchor_controls',
 'conditional_features','trade_attribution','conditional_rolling',
 'control_ledgers','boundary_register','methodology','errors')}
BUNDLE=str(OUTPUT_DIR/'AUDJPY_M15_LONG_ENGULFING_PASS2_RESULTS.zip')


def anchor_mask(f,base,lb,d,body,rng):
    return (base & (f['structure_dist_low'][lb]<=d)
            & (f['body_atr']>=body) & (f['range_atr']>=rng))


def anchor_parity(candles,f,base):
    """Fail closed against exactly Pass 1's completed 20:45 UTC candles.

    Pass 1 published aggregate anchor counts and R but NOT exact anchor
    trade fingerprints. Accordingly this is aggregate parity, not a claim
    that every anchor trade's field-level identity was verified.
    """
    ts=f['times'];n=bisect.bisect_right(ts,PASS2_CUTOFF)
    if n!=PASS2_CANDLES or ts[n-1]!=PASS2_CUTOFF:
        raise RuntimeError('Pass 1 anchor historical coverage mismatch: '
                           'expected 546818 candles through 2026-09-23 20:45 UTC; '
                           'got %s through %s'%(n,iso(ts[n-1]) if n else 'NONE'))
    checks=[]
    for aid,lb,d,body,rng,expected_n,expected_2,expected_4 in ANCHORS:
        ix=np.flatnonzero(anchor_mask(f,base,lb,d,body,rng)[:n]).tolist()
        # Full historical p0 only through the previously archived cutoff.
        main=backtest(candles[:n],ix,RR_FIXED,PRIMARY_COST)
        stress=backtest(candles[:n],ix,RR_FIXED,STRESS_COST)
        actual2=stats(main); actual4=stats(stress)
        valid=(actual2['trades']==expected_n and actual4['trades']==expected_n
               and abs(actual2['total_r']-expected_2)<1e-7
               and abs(actual4['total_r']-expected_4)<1e-7)
        checks.append(dict(anchor=aid,cutoff_utc=iso(PASS2_CUTOFF),
             expected_trades=expected_n,actual_2pip_trades=actual2['trades'],
             actual_4pip_trades=actual4['trades'],expected_2pip_r=expected_2,
             actual_2pip_r=actual2['total_r'],expected_4pip_r=expected_4,
             actual_4pip_r=actual4['total_r'],
             parity_type='archived aggregate count plus both R assumptions',
             result='PASS' if valid else 'FAIL'))
        if not valid:
            write_csv(OUTS['anchor_parity'],checks)
            raise RuntimeError('Frozen Pass 1 anchor parity FAILED for '+aid)
    write_csv(OUTS['anchor_parity'],checks)
    OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()  # avoid cached cutoff-limited outcomes
    return checks


def conditional_filters(f,extra):
    """One filter at a time INSIDE each unchanged anchor.

    No geometry-distance/lookback, breakout-penetration, hour, or weekday
    feature here; those are later plateau or timing questions. Every filter
    including historically losing ones is reported and double-cost tested.
    """
    prior=independent_filters(f,extra)
    skip=('distance_to_prior_low_','low_penetration_','ny_hour_DIAGNOSTIC',
          'ny_weekday_DIAGNOSTIC')
    result=[(g,ax,v,np.asarray(mask,dtype=bool)) for g,ax,v,mask in prior
            if not g.startswith(skip)]
    def add(g,ax,v,mask):
        result.append((g,ax,v,np.asarray(mask,dtype=bool)))
    for v in (.10,.20,.30,.40,.60):
        add('lower_wick_body','max',v,f['lower_wick_body']<=v)
    for v in (.25,.50,.75,1.00,1.25):
        add('prior_candle_body_atr','max',v,extra['prev_body_atr']<=v)
    for v in (.60,.75,.90,1.00):
        add('signal_stop_atr','min',v,extra['stop_atr']>=v)
    for v in (.70,.85,1.00,1.15):
        add('m15_atr_ratio20','max',v,extra['m15_atr_ratio20']<=v)
    for name in ('mom4','mom12','mom24','mom48'):
        for v in (.25,.50,1.00):
            add('prior_'+name+'_absolute_momentum','max',v,
                np.abs(extra[name])<=v)
    for name,lhs,rhs in (
        ('h1_close_below_ema100','h1_close','h1_ema100'),
        ('h1_ema50_below_ema200','h1_ema50','h1_ema200'),
        ('h4_close_below_ema100','h4_close','h4_ema100'),
        ('h4_ema100_below_ema200','h4_ema100','h4_ema200'),
        ('daily_close_below_ema200','d_close','d_ema200'),
        ('daily_ema50_below_ema200','d_ema50','d_ema200'),
    ):
        add('completed_htf_counterregime',name,'yes',f[lhs]<f[rhs])
    for name in ('h1_atr','h4_atr','d_atr'):
        for v in (.80,1.00,1.20):
            add('completed_'+name,'max',v,f[name]<=v)
    keys=[(g,ax,str(v)) for g,ax,v,_ in result]
    if len(keys)!=len(set(keys)):
        raise RuntimeError('Duplicate conditional factor IDs')
    return result


def sigmap(ledger):
    return {t['signal_index']:t for t in ledger}


def attribution(anchor,conditional):
    """Accepted-trade comparison, NOT just mask subtraction.

    Removing a signal can free a later previously blocked signal under p0;
    added/removed rows are therefore individually accounted for.
    """
    old=sigmap(anchor);new=sigmap(conditional)
    removed=[old[i] for i in sorted(old.keys()-new.keys())]
    added=[new[i] for i in sorted(new.keys()-old.keys())]
    common=len(old.keys()&new.keys())
    oldr=sum(t['result_r'] for t in anchor)
    newr=sum(t['result_r'] for t in conditional)
    return dict(control_trades=len(anchor),variant_trades=len(conditional),
      retained_control_trades=common,removed_accepted_trades=len(removed),
      removed_winners=sum(t['result_r']>0 for t in removed),
      removed_losers=sum(t['result_r']<0 for t in removed),
      removed_accepted_r=sum(t['result_r'] for t in removed),
      newly_eligible_trades=len(added),
      newly_eligible_winners=sum(t['result_r']>0 for t in added),
      newly_eligible_losers=sum(t['result_r']<0 for t in added),
      newly_eligible_r=sum(t['result_r'] for t in added),
      net_change_r=newr-oldr,
      ledger_change_reconciles=abs((sum(t['result_r'] for t in added)
                      -sum(t['result_r'] for t in removed))-(newr-oldr))<1e-7)


def rolling_summary(cid,ledger,cost_pips):
    """Full completed calendar-month windows, no p0 reset at window edges."""
    first=datetime(HISTORY_FIRST.year,HISTORY_FIRST.month,1,tzinfo=timezone.utc)
    if first<HISTORY_FIRST:first=add_months_utc(first,1)
    last=datetime(NOW.year,NOW.month,1,tzinfo=timezone.utc)
    ordered=sorted(ledger,key=lambda t:t['entry_time'])
    entry=[t['entry_time'] for t in ordered]
    pref=np.r_[0.0,np.cumsum([t['result_r'] for t in ordered])]
    rows=[]
    for months in (12,24,36):
        window=first;vals=[];active=[];zero=0
        while add_months_utc(window,months)<=last:
            end=add_months_utc(window,months)
            a=bisect.bisect_left(entry,window)
            b=bisect.bisect_left(entry,end)
            r=float(pref[b]-pref[a]);vals.append(r)
            if a==b:zero+=1
            else:active.append(r)
            window=add_months_utc(window,1)
        rows.append(dict(config_id=cid,cost_pips=cost_pips,
            rolling_months=months,window_count=len(vals),
            active_windows=len(active),zero_trade_windows=zero,
            positive_all_windows_pct=(100*sum(x>0 for x in vals)/len(vals)
                                       if vals else None),
            positive_active_windows_pct=(100*sum(x>0 for x in active)/len(active)
                                          if active else None),
            worst_window_r=min(vals) if vals else None,
            median_window_r=med(vals)))
    return rows


def boundary_register():
    """Forward protocol, NOT a fabricated extension result."""
    return [
      dict(axis='previous_low_lookback',pass1_levels='20/40/60/100/165',
           expand_if='strong region still improving at 20 or 165',
           proposed_exploratory_extension='10/15 below 20; 200/240 above 165'),
      dict(axis='ATR_normalised_distance',pass1_levels='.05/.10/.25/.50',
           expand_if='promising region touches .05 or .50',
           proposed_exploratory_extension='0/.01/.025/.075 below/near .05; .75/1.00 above .50'),
      dict(axis='body_ATR_min',pass1_levels='.50/.75/1.00/1.25',
           expand_if='promising region touches .50 or 1.25',
           proposed_exploratory_extension='.25/.40 below .50; 1.40/1.60 above 1.25'),
      dict(axis='range_ATR_min',pass1_levels='.75/1.00/1.25/1.50',
           expand_if='promising region touches .75 or 1.50',
           proposed_exploratory_extension='.50/.60 below .75; 1.65/1.80/2.00 above 1.50'),
      dict(axis='conditional_feature_threshold',pass1_levels='this Pass 2 grid',
           expand_if='broad cost-resistant response increases at either tested boundary',
           proposed_exploratory_extension='predeclare adjacent exterior levels, test all neighbours and trade attribution'),
    ]


def run_research():
    global HISTORY_FIRST
    try:
        # Railway may reuse the same output directory across deployments.
        # Start each run with a clean report set; an old errors.csv must not
        # be packaged as if it belonged to this successful run.
        for previous in list(OUTS.values()) + [BUNDLE]:
            try:
                Path(previous).unlink(missing_ok=True)
            except IsADirectoryError:
                raise RuntimeError('Report path is a directory: '+str(previous))
        STATUS.update(state='fetch',progress=1,message='Fetching AUD/JPY M15 and completed HTF candles')
        candles=fetch('M15',START,NOW,35)
        if len(candles)<PASS2_CANDLES:raise RuntimeError('M15 shorter than frozen Pass 1 cutoff')
        HISTORY_FIRST=candles[0]['time']
        STATUS.update(progress=12,message='Fetching H1/H4/D contexts; no orders')
        h1=fetch('H1',WARMUP,NOW,180)
        h4=fetch('H4',WARMUP,NOW,700)
        day=fetch('D',WARMUP,NOW,3500)
        if not(h1 and h4 and day):raise RuntimeError('Missing completed HTF data')
        write_csv(OUTS['coverage'],[hist_coverage(k,v) for k,v in (
            ('M15',candles),('H1',h1),('H4',h4),('D',day))])
        STATUS.update(state='features',progress=26,message='Computing causal indicators')
        f=features(candles,align_htf([c['time'] for c in candles],htf_state(h1)),
            align_htf([c['time'] for c in candles],htf_state(h4)),
            align_htf([c['time'] for c in candles],htf_state(day)))
        base=engulf_base(f)
        STATUS.update(state='parity',progress=36,message='Checking archived raw ledger and three anchors')
        raw_engulf_parity(candles,f)
        anchor_parity(candles,f,base)
        STATUS.update(state='anchors',progress=43,message='Replaying unchanged anchors on current full history')
        controls={};control_rows=[];control_trades=[]
        extra=make_extra_features(f)
        raw_reference=stats(backtest(candles,np.flatnonzero(base).tolist(),RR_FIXED,PRIMARY_COST))
        OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
        for aid,lb,d,body,rng,_,__,___ in ANCHORS:
            mask=anchor_mask(f,base,lb,d,body,rng)
            ix=np.flatnonzero(mask).tolist()
            t2,t4=evaluate(candles,ix)
            cid='ANCHOR__'+aid
            controls[aid]=(mask,ix,t2,t4)
            control_rows.append(dict(anchor=aid,lookback=lb,distance_atr=d,
                body_atr_min=body,range_atr_min=rng,
                **metrics_row('UNCHANGED_ANCHOR',cid,{},ix,t2,t4,raw_reference)))
            for cost,ledger in ((2,t2),(4,t4)):
                # cost_pips already exists on each trade; do not pass it twice.
                # Explicit rows also avoid silently dropping important outcome fields.
                control_trades.extend(
                    dict(anchor=aid, **{k:v for k,v in tr.items()
                        if k not in ('entry_time','exit_time')})
                    for tr in ledger
                )
        write_csv(OUTS['anchor_controls'],control_rows)
        write_csv(OUTS['control_ledgers'],control_trades)
        STATUS.update(state='conditional',progress=48,
                      message='Conditional features inside three fixed geometry anchors')
        factors=conditional_filters(f,extra)
        rows=[];attrs=[];roll=[]
        for aid,lb,d,body,rng,_,__,___ in ANCHORS:
            _,anchor_ix,anchor2,anchor4=controls[aid]
            ctrlid='ANCHOR__'+aid
            roll.extend(rolling_summary(ctrlid,anchor2,2))
            roll.extend(rolling_summary(ctrlid,anchor4,4))
            for j,(group,axis,val,overlay) in enumerate(factors):
                cid=aid+'__'+group+'__'+axis+'__'+str(val).replace('.','p')
                mask=controls[aid][0]&overlay
                ix=np.flatnonzero(mask).tolist()
                t2,t4=evaluate(candles,ix)
                row=metrics_row('CONDITIONAL',cid,dict(anchor=aid,
                    lookback=lb,distance_atr=d,body_atr_min=body,
                    range_atr_min=rng,filter_group=group,filter_axis=axis,
                    filter_value=val,control_config_id=ctrlid,
                    control_raw_signals=len(anchor_ix),
                    control_2pip_trades=len(anchor2),control_4pip_trades=len(anchor4),
                    delta_r_vs_anchor_2pip=stats(t2)['total_r']-stats(anchor2)['total_r'],
                    delta_r_vs_anchor_4pip=stats(t4)['total_r']-stats(anchor4)['total_r']),
                    ix,t2,t4,raw_reference)
                rows.append(row)
                for cost,t,ctl in ((2,t2,anchor2),(4,t4,anchor4)):
                    a=attribution(ctl,t)
                    if not a['ledger_change_reconciles']:
                        raise RuntimeError('P0 attribution reconciliation failed '+cid)
                    attrs.append(dict(config_id=cid,anchor=aid,cost_pips=cost,
                        filter_group=group,filter_axis=axis,filter_value=val,**a))
                    roll.extend(rolling_summary(cid,t,cost))
                if j%16==15 or j==len(factors)-1:
                    done=(ANCHORS.index(next(a for a in ANCHORS if a[0]==aid))*len(factors)+j+1)
                    total=len(ANCHORS)*len(factors)
                    STATUS.update(progress=48+int(47*done/total),
                          message='Conditional %d/%d; anchor %s'%(done,total,aid))
                    write_csv(OUTS['conditional_features'],rows)
                    write_csv(OUTS['trade_attribution'],attrs)
                    write_csv(OUTS['conditional_rolling'],roll)
                if j%24==23:OUTCOME_CACHE.clear();BACKTEST_CACHE.clear()
        assert len(rows)==len(ANCHORS)*len(factors)
        assert len(attrs)==2*len(rows)
        write_csv(OUTS['conditional_features'],rows)
        write_csv(OUTS['trade_attribution'],attrs)
        write_csv(OUTS['conditional_rolling'],roll)
        write_csv(OUTS['boundary_register'],boundary_register())
        write_csv(OUTS['methodology'],[
          dict(topic='SCOPE',detail='READ-ONLY engulfing-only, three frozen anchors, one additional factor at a time; no live or portfolio access'),
          dict(topic='PARITY',detail='19,471 raw trades exact sha256 to 19:15 UTC plus three archived aggregate anchor parity counts and R through 20:45 UTC 2026-09-23. Anchor parity is NOT field-level hash.'),
          dict(topic='COSTS',detail='All rows both 2 and 4 assumed adverse BUY pips, fixed RR3.5. Not measured historical bid/ask or slippage.'),
          dict(topic='EXECUTION',detail='Signal close reference; target from reference risk, true R from adverse fill risk; next candle exit, same exit candle re-entry, p0.'),
          dict(topic='CAUSAL',detail='Previous-only structure/momentum, completed H1/H4/D; timeframe completion based on actual next HTF open.'),
          dict(topic='ATTRIBUTION',detail='Removed and newly eligible ACCEPTED trades distinguished per cost/anchor. Net R change reconciles; not assumed simple deletion.'),
          dict(topic='ROLLING',detail='All complete 12/24/36 calendar-month windows computed from full p0 ledgers; no reset at splits. Overlap, not independent tests.'),
          dict(topic='TIMING',detail='No session/weekday combined filters. No RR optimisation.'),
          dict(topic='BOUNDARY_RULE',detail='If a promising neighbourhood improves at tested boundary, extend that parameter both directions before choosing a plateau; no boundary winner declared optimal.'),
          dict(topic='EVIDENCE',detail='All full-history and recent periods repeatedly inspected. No fresh out-of-sample. No automatic row ranking or strategy acceptance.')])
        zip_outputs()
        STATUS.update(state='complete',progress=100,raw_engulf_parity='PASS',
             anchor_parity='PASS',anchor_count=len(ANCHORS),
             conditional_factors_per_anchor=len(factors),
             conditional_rows=len(rows),orders_supported=False,
             trading_enabled=False,result_path='/audjpy-engulfing-pass2/results',
             message='Read-only conditional discovery complete; no live changes')
    except Exception as exc:
        STATUS.update(state='error',message=str(exc),traceback=traceback.format_exc(),
                      orders_supported=False,trading_enabled=False)
        try:
            write_csv(OUTS['errors'],[dict(error=str(exc),traceback=STATUS['traceback'])])
            zip_outputs()
        finally:print(STATUS['traceback'],flush=True)


@app.route('/')
def root():
    return jsonify(service='AUD/JPY M15 LONG exact engulfing Pass 2 conditional features',
      state=STATUS['state'],status='/audjpy-engulfing-pass2/status',
      results='/audjpy-engulfing-pass2/results',
      orders_supported=False,trading_enabled=False)

@app.route('/audjpy-engulfing-pass2/status')
def status_route():return jsonify(STATUS)

@app.route('/audjpy-engulfing-pass2/results')
def results_route():return download(BUNDLE)

if __name__=='__main__':
    threading.Thread(target=run_research,daemon=True).start()
    app.run(host='0.0.0.0',port=int(os.getenv('PORT','8080')),
            debug=False,use_reloader=False)
