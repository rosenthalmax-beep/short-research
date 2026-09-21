"""EUR/AUD H1 SHORT — read-only, portfolio-first, plateau-aware discovery.

Run as a SEPARATE Railway research service, never the live probe/executor.
Python dependencies: flask, numpy, requests. Environment: OANDA_TOKEN, PORT.
Start: python EURAUD_H1_SHORT_PORTFOLIO_FIRST_DISCOVERY.py
Routes: /euraud-h1-short-discovery/status and /euraud-h1-short-discovery/results

RESEARCH PRECOMMITMENT (do not edit after seeing results)
* Three separately hypothesised SHORT families only: sweep/rejection, compression
  breakdown, and bearish pullback continuation. The previously studied outside
  reversal is NOT recycled or silently relabelled.
* Stage 1: fixed 3.5R, at most 38 base configurations (12 + 18 + 8).
* Up to two gated geometry seeds per family advance. Further work is confined
  to one-factor sweeps and RR neighbours; never splice independently attractive
  filters. Boundary extensions require BOTH the outer setting and its neighbour
  to meet the same minimum-evidence gate. Stop after two predefined steps.
* A positive result is a RESEARCH LEAD, not promotion. Only an exact, separately
  run, accepted-trade-level 27->28 portfolio test can evaluate improvement in
  weak rolling periods, exposure, drawdown, and realised incremental R.
* All post-2018 / recent periods have been inspected previously; they are not
  fresh out-of-sample evidence. No future returns are predicted.

HISTORICAL CONVENTIONS
OANDA midpoint completed H1 bars, ATR14 Wilder SMA-seeded. Signal open time
is the H1 candle OPEN; entry reference is its CLOSE. Stop is signal HIGH + 10
price ticks (0.0001), target = reference - RR*(stop-reference). Historical
fill is reference - 2 pips (4 pips stress); risk is stop - adverse fill.
No intrabar entry fill simulation. Check stop/target from NEXT candle; when
both touch, use prior-run heuristic: if high closer to candle open -> stop,
otherwise target. One position per candidate; exit-candle signal eligible.
No weekday/session/pair filter. These assumptions are NOT verified bid/ask.
"""

import csv
import io
import math
import os
import threading
import time
import traceback
import zipfile
from bisect import bisect_left
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import requests
from flask import Flask, jsonify, send_file

app = Flask(__name__)
PAIR = "EUR_AUD"
BASE = os.getenv("OANDA_API_URL", "https://api-fxtrade.oanda.com").rstrip("/")
TOKEN = os.getenv("OANDA_TOKEN")
START = datetime(2002, 5, 6, 20, tzinfo=timezone.utc)
TICK, PIP, STOP_TICKS = .00001, .0001, 10
PRIMARY_COST, STRESS_COST = 2.0, 4.0
BASE_RR = 3.5
RR_INITIAL = [2.5, 3.0, 3.5, 4.0, 4.5]
RR_EXTRA_LOW, RR_EXTRA_HIGH = [2.0, 1.5], [5.0, 5.5]
OUTDIR = Path(os.getenv("RESEARCH_OUTPUT_DIR", ".")).resolve()
BUNDLE = OUTDIR / "EURAUD_H1_SHORT_PORTFOLIO_FIRST_DISCOVERY_RESULTS.zip"
DATE_2018 = datetime(2018, 1, 1, tzinfo=timezone.utc)
MIN_FULL_TRADES = 60
MIN_SPLIT_TRADES = 15
MIN_LAST5_TRADES = 10
MIN_LAST2_TRADES = 4
MIN_FULL_PF, MIN_SPLIT_PF, MIN_COST_PF = 1.30, 1.10, 1.15
MIN_ROLLING36_POSITIVE_PCT = 65.0
MAX_SEEDS_PER_FAMILY = 2
LOCK = threading.Lock()
STARTED = False
STATUS = {"state": "not_started", "message": "Awaiting launch", "instrument": PAIR,
          "side": "SHORT", "orders_supported": False, "trading_enabled": False}

FAMILY_PARAMS = {
    "SWEEP_REJECTION": {
        "lookback": ([20, 40, 60], [10, 5], [80, 100]),
        "wick_min": ([.25, .50], [.10, .00], [.75, 1.00]),
        "body_min": ([.75, 1.00], [.60, .45], [1.25, 1.50]),
    },
    "COMPRESSION_BREAKDOWN": {
        "compression_max": ([.75, .85, .95], [.65, .55], [1.05, 1.15]),
        "lookback": ([5, 10, 20], [3, 2], [30, 40]),
        "body_min": ([.90, 1.20], [.70, .50], [1.40, 1.60]),
    },
    "PULLBACK_CONTINUATION": {
        "pullback_bars": ([4, 8], [2, 1], [12, 16]),
        "momentum_min": ([.25, .50], [0.0], [.75, 1.00]),
        "body_min": ([.75, 1.00], [.60, .45], [1.25, 1.50]),
    },
}


def iso(d):
    return d.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def datetime_of(x):
    return datetime.fromisoformat(x.replace("Z", "+00:00")).astimezone(timezone.utc)


def month_start(d):
    return datetime(d.year, d.month, 1, tzinfo=timezone.utc)


def plus_months(d, k):
    v = d.year * 12 + d.month - 1 + k
    return datetime(v // 12, v % 12 + 1, 1, tzinfo=timezone.utc)


def csv_bytes(rows):
    out = io.StringIO()
    if not rows:
        return b""
    fields = list(dict.fromkeys(k for r in rows for k in r))
    writer = csv.DictWriter(out, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue().encode("utf-8")


def save_bundle(data):
    OUTDIR.mkdir(parents=True, exist_ok=True)
    temp = BUNDLE.with_suffix(".tmp")
    with zipfile.ZipFile(temp, "w", zipfile.ZIP_DEFLATED, compresslevel=7) as z:
        for name, rows in data.items():
            z.writestr(name, csv_bytes(rows))
    temp.replace(BUNDLE)


def fetch_candles(start, end):
    if not TOKEN:
        raise RuntimeError("OANDA_TOKEN missing. Research is read-only; no account ID needed")
    seen = {}
    cursor = start
    chunk = 0
    while cursor < end:
        right = min(cursor + timedelta(days=180), end)
        chunk += 1
        STATUS.update(state="fetching", message=f"H1 chunk {chunk} {iso(cursor)} -> {iso(right)}")
        params = {"price": "M", "granularity": "H1", "smooth": "false",
                  "from": iso(cursor), "to": iso(right), "includeFirst": "true"}
        error = None
        for attempt in range(3):
            try:
                response = requests.get(f"{BASE}/v3/instruments/{PAIR}/candles",
                     headers={"Authorization": "Bearer " + TOKEN.strip()},
                     params=params, timeout=60)
                # Empty instrument history prior to inception is permissible.
                if response.status_code in (400, 404) and not seen:
                    items = []
                else:
                    response.raise_for_status()
                    items = response.json().get("candles", [])
                error = None
                break
            except Exception as exc:
                error = exc
                time.sleep(.4 * (attempt + 1))
        if error:
            raise error
        for raw in items:
            if not raw.get("complete") or not raw.get("mid"):
                continue
            m = raw["mid"]
            when = datetime_of(raw["time"])
            seen[when] = (when, float(m["o"]), float(m["h"]),
                          float(m["l"]), float(m["c"]))
        cursor = right
        time.sleep(.025)
    rows = [seen[k] for k in sorted(seen)]
    if len(rows) < 20_000:
        raise RuntimeError(f"Historical coverage too short: {len(rows)} completed H1 candles")
    return rows


def rolling_prev(values, length, highest):
    vals = np.asarray(values, dtype=float)
    out = np.full(len(vals), np.nan)
    q = deque()
    for i in range(len(vals)):
        j = i - 1
        if j >= 0:
            while q and (vals[q[-1]] <= vals[j] if highest else vals[q[-1]] >= vals[j]):
                q.pop()
            q.append(j)
        while q and q[0] < i - length:
            q.popleft()
        if i >= length and q:
            out[i] = vals[q[0]]
    return out


def previous_mean(values, length):
    vals = np.asarray(values, dtype=float)
    result = np.full(len(vals), np.nan)
    q, total, invalid = deque(), 0., 0
    for i in range(len(vals)):
        if i:
            x = float(vals[i-1]); q.append(x)
            if math.isfinite(x): total += x
            else: invalid += 1
        if len(q) > length:
            x = q.popleft()
            if math.isfinite(x): total -= x
            else: invalid -= 1
        if len(q) == length and invalid == 0:
            result[i] = total / length
    return result


def ema(values, length):
    # SMA seed avoids constructing an artificial regime at the first candle.
    result = np.full(len(values), np.nan)
    if len(values) < length:
        return result
    result[length-1] = float(np.mean(values[:length]))
    a = 2 / (length + 1)
    for i in range(length, len(values)):
        result[i] = a * float(values[i]) + (1-a) * result[i-1]
    return result


def features(rows):
    ts = [r[0] for r in rows]
    o, h, l, c = (np.asarray([r[j] for r in rows], float) for j in range(1, 5))
    n = len(rows)
    tr = np.full(n, np.nan)
    tr[0] = h[0]-l[0]
    tr[1:] = np.maximum.reduce([h[1:]-l[1:], abs(h[1:]-c[:-1]), abs(l[1:]-c[:-1])])
    atr = np.full(n, np.nan)
    atr[13] = np.mean(tr[:14])
    for i in range(14, n):
        atr[i] = (13*atr[i-1]+tr[i])/14
    body = o-c
    upper = h-np.maximum(o,c)
    range_ = h-l
    prev_low, prev_high, prev_close = (np.roll(v, 1) for v in [l,h,c])
    prev_close[0] = np.nan; prev_low[0] = np.nan; prev_high[0] = np.nan
    loc = np.divide(c-l, range_, out=np.full(n, np.nan), where=range_>0)
    body_atr = np.divide(body,atr,out=np.full(n,np.nan),where=(body>0)&(atr>0))
    wick_body = np.divide(upper,body,out=np.full(n,np.nan),where=body>0)
    range_atr = np.divide(range_,atr,out=np.full(n,np.nan),where=atr>0)
    atr_prev = np.roll(atr,1);atr_prev[0]=np.nan
    prior_mean = previous_mean(atr,20)
    compression = np.divide(atr_prev,prior_mean,out=np.full(n,np.nan),where=prior_mean>0)
    ema50 = ema(c,50); ema200 = ema(c,200)
    ema50prev, ema200prev = np.roll(ema50,1), np.roll(ema200,1)
    ema50prev[0]=np.nan;ema200prev[0]=np.nan
    all_lbs = set()
    for cfg in FAMILY_PARAMS.values():
        all_lbs.update(cfg.get("lookback", ([],[],[]))[0] + cfg.get("lookback", ([],[],[]))[1] + cfg.get("lookback", ([],[],[]))[2])
    all_lbs.add(8)  # rolling high of the preceding completed pullback bars
    return {"time":ts,"o":o,"h":h,"l":l,"c":c,"atr":atr,"atr_prev":atr_prev,
        "body":body,"body_atr":body_atr,"wick_body":wick_body,"range_atr":range_atr,
        "loc":loc,"prev_low":prev_low,"prev_high":prev_high,"prev_close":prev_close,
        "compression":compression,"ema50prev":ema50prev,"ema200prev":ema200prev,
        "highs":{k:rolling_prev(h,k,True) for k in all_lbs},
        "lows":{k:rolling_prev(l,k,False) for k in all_lbs}}


def cfg_id(family, params, rr):
    return f"{family}|" + "|".join(f"{k}={v}" for k,v in sorted(params.items())) + f"|rr={rr:g}"


def config_set():
    from itertools import product
    result=[]
    for family, definitions in FAMILY_PARAMS.items():
        keys=list(definitions)
        for values in product(*(definitions[k][0] for k in keys)):
            result.append((family,dict(zip(keys,values)),BASE_RR))
    assert len(result)==38, f"Expected 38 predeclared starting geometries; got {len(result)}"
    return result


def raw_signals(f, family, p):
    n=len(f["time"])
    base = (np.isfinite(f["atr"]) & (f["atr"]>0)
       & (f["body_atr"] >= p["body_min"]))
    if family=="SWEEP_REJECTION":
        prior=f["highs"][p["lookback"]]
        mask=(base & (f["h"]>prior) & (f["c"]<prior)
             & (f["loc"]<=.35) & (f["wick_body"]>=p["wick_min"]))
    elif family=="COMPRESSION_BREAKDOWN":
        prior=f["lows"][p["lookback"]]
        mask=(base & (f["compression"]<=p["compression_max"])
              & (f["range_atr"]>=1.25) & (f["c"]<prior))
    elif family=="PULLBACK_CONTINUATION":
        # All regime inputs are strictly completed H1 values, never signal-close EMA.
        k=p["pullback_bars"]
        prevclose = f["prev_close"]
        older=np.full(n,np.nan)
        older[k+1:]=f["c"][:-k-1]
        mom=np.divide(prevclose-older,f["atr_prev"],
                      out=np.full(n,np.nan),where=f["atr_prev"]>0)
        oldema=np.full(n,np.nan); oldema[9:]=f["ema50prev"][:-9]
        prior_high=rolling_prev(f["h"],k,True)
        mask=(base & (f["ema50prev"]<f["ema200prev"])
              & (f["ema50prev"]<oldema)
              & (mom>=p["momentum_min"])
              & (prior_high>=f["ema50prev"]-.25*f["atr_prev"])
              & (f["c"]<f["prev_low"]))
    else:
        raise ValueError(f"Unexpected family {family}")
    return np.flatnonzero(mask).astype(int).tolist()


def find_exit(f, i, stop, target):
    n=len(f["time"])
    for lo in range(i+1,n,2048):
        hi=min(n,lo+2048)
        hits=np.flatnonzero((f["h"][lo:hi]>=stop)|(f["l"][lo:hi]<=target))
        if len(hits):
            j=lo+int(hits[0]); o,h,l=f["o"][j],f["h"][j],f["l"][j]
            stop_hit=h>=stop;target_hit=l<=target
            if stop_hit and target_hit:
                outcome="STOP" if abs(h-o)<abs(o-l) else "TARGET"
            else:
                outcome="STOP" if stop_hit else "TARGET"
            return j,outcome
    return None,None


def backtest(f, raw, family, p, rr, cost_pips=PRIMARY_COST):
    assert raw==sorted(set(raw))
    trades=[]; pointer=0
    while pointer < len(raw):
        i=raw[pointer]
        ref=float(f["c"][i]); stop=float(f["h"][i])+STOP_TICKS*TICK
        ref_risk=stop-ref
        fill=ref-cost_pips*PIP
        risk=stop-fill
        if risk<=0 or ref_risk<=0:
            pointer+=1; continue
        target=ref-rr*ref_risk
        j,outcome=find_exit(f,i,stop,target)
        if j is None:
            break # unresolved trade is not a closed result
        r=(fill-(stop if outcome=="STOP" else target))/risk
        trades.append({"config_id":cfg_id(family,p,rr),"family":family,
            "signal_index":i,"exit_index":j,"entry_time":f["time"][i],
            "exit_time":f["time"][j],"reference_entry":ref,"adverse_fill":fill,
            "stop":stop,"target":target,"exit_reason":outcome,"r":r,
            "rr":rr,"cost_pips":cost_pips,"duration_bars":j-i})
        pointer=bisect_left(raw,j,lo=pointer+1)  # exit-candle re-entry eligible
    return trades


def pf(trades):
    gp=sum(t["r"] for t in trades if t["r"]>0)
    gl=-sum(t["r"] for t in trades if t["r"]<0)
    return gp/gl if gl>0 else (float("inf") if gp else 0.)


def stats(trades):
    vals=[t["r"] for t in trades]
    peak=cum=dd=0.;loss=run=0
    for r in vals:
        cum+=r; peak=max(peak,cum); dd=min(dd,cum-peak)
        loss=loss+1 if r<=0 else 0
        run=max(run,loss)
    return {"trades":len(vals),"winners":sum(r>0 for r in vals),
       "profit_factor":pf(trades),"total_r":sum(vals),
       "expectancy_r":sum(vals)/len(vals) if vals else 0.,
       "max_dd_r":dd,"longest_losing_streak":run}


def rolling_summary(trades, start, end, length_months):
    """Closed trades attributed by ENTRY date; all monthly start windows.
    Empty windows retained; before first available full history excluded.
    """
    cursor=month_start(start)
    end_month=month_start(end)
    vals=[]
    while plus_months(cursor,length_months)<=end_month:
        right=plus_months(cursor,length_months)
        vals.append(sum(t["r"] for t in trades if cursor<=t["entry_time"]<right))
        cursor=plus_months(cursor,1)
    return {"rolling_windows":len(vals),"rolling_positive_pct":
        100*sum(v>0 for v in vals)/len(vals) if vals else 0.,
        "rolling_worst_r":min(vals) if vals else 0.}


def report(f, raw, family, p, rr, start, end, get_rolling=True):
    tr=backtest(f,raw,family,p,rr)
    stress=backtest(f,raw,family,p,rr,STRESS_COST)
    m=stats(tr);m.update({"cost4_pf":pf(stress),"cost4_r":sum(t["r"] for t in stress),
       "cost4_trades":len(stress),"raw_signals":len(raw)})
    for title,left,right in [
        ("pre2018",start,DATE_2018),("since2018",DATE_2018,end),
        ("last5y",end-timedelta(days=365.25*5),end),
        ("last3y",end-timedelta(days=365.25*3),end),
        ("last2y",end-timedelta(days=365.25*2),end),
        ("last1y",end-timedelta(days=365.25),end),
    ]:
        s=stats([t for t in tr if left<=t["entry_time"]<right])
        m.update({f"{title}_{k}":s[k] for k in ("trades","profit_factor","total_r")})
    if get_rolling:
        for months in (12,24,36):
            m.update({f"roll{months}_{k}":v for k,v in
                     rolling_summary(tr,start,end,months).items()})
    return m,tr,stress


def passes(m):
    criteria={
      "enough_history":m["trades"]>=MIN_FULL_TRADES,
      "full_pf":m["profit_factor"]>=MIN_FULL_PF,
      "split_trades":min(m["pre2018_trades"],m["since2018_trades"])>=MIN_SPLIT_TRADES,
      "split_pf":min(m["pre2018_profit_factor"],m["since2018_profit_factor"])>=MIN_SPLIT_PF,
      "recent_5yr":m["last5y_trades"]>=MIN_LAST5_TRADES and m["last5y_total_r"]>0,
      "recent_2yr":m["last2y_trades"]>=MIN_LAST2_TRADES,
      "doubled_cost":m["cost4_pf"]>=MIN_COST_PF and m["cost4_r"]>0,
      "rolling36":m.get("roll36_rolling_positive_pct",0)>=MIN_ROLLING36_POSITIVE_PCT,
    }
    return all(criteria.values()), ";".join(k for k,v in criteria.items() if not v)


def rank(m):
    # Deterministic heuristic ONLY for research shortlist, never promotion.
    return (min(m["pre2018_profit_factor"],m["since2018_profit_factor"]),
            m["cost4_pf"],m["last5y_total_r"],m["total_r"])


def export_trade(t):
    return {k:iso(v) if isinstance(v,datetime) else v for k,v in t.items()}


def main_research():
    files={}
    try:
        end=datetime.now(timezone.utc).replace(second=0,microsecond=0)
        candles=fetch_candles(START,end)
        f=features(candles)
        start=f["time"][0]
        STATUS.update(state="running",message="Stage 1: 38 frozen base geometries",
                      h1_candles=len(candles),first_h1=iso(start),last_h1=iso(f["time"][-1]))
        files["coverage.csv"]=[{"pair":PAIR,"first_h1":iso(start),
           "last_h1":iso(f["time"][-1]),"candles":len(candles),
           "study_cutoff_utc":iso(end),"historical_cost_pips":PRIMARY_COST,
           "stress_cost_pips":STRESS_COST,"stage1_configurations":38,
           "base_rr":BASE_RR,"read_only":True}]
        stage1=[];cache={}; seeds=[]
        for count,(family,p,rr) in enumerate(config_set(),1):
            raw=raw_signals(f,family,p)
            m,tr,_=report(f,raw,family,p,rr,start,end)
            gated,fail=passes(m)
            row={"config_id":cfg_id(family,p,rr),"family":family,**p,
                 "rr":rr,"research_gate_pass":gated,"gate_failures":fail,**m}
            stage1.append(row);cache[row["config_id"]]=(family,p,rr,raw,m,tr)
            STATUS.update(message=f"Stage 1: {count}/38 geometries")
        files["stage1_all_38.csv"]=stage1
        for family in FAMILY_PARAMS:
            good=[r for r in stage1 if r["family"]==family and r["research_gate_pass"]]
            good.sort(key=rank,reverse=True)
            seeds.extend(good[:MAX_SEEDS_PER_FAMILY])
        files["shortlisted_geometry_seeds.csv"]=seeds
        STATUS.update(state="running",message=f"Stage 2: RR and one-factor plateau tests for {len(seeds)} seeds")
        rrrows=[]; sweep=[]; edge=[]; detailed=[]; calendar=[]; trades_export=[]
        evaluated={}
        def examine(family,p,rr,origin,parent=""):
            ident=cfg_id(family,p,rr)
            if ident not in evaluated:
                raw=raw_signals(f,family,p)
                m,tr,stress=report(f,raw,family,p,rr,start,end)
                ok,fail=passes(m)
                evaluated[ident]=(raw,m,tr,stress,ok,fail)
            raw,m,tr,stress,ok,fail=evaluated[ident]
            return {"config_id":ident,"family":family,**p,"rr":rr,
                "study":origin,"seed_config_id":parent,"research_gate_pass":ok,
                "gate_failures":fail,**m}
        # Stage 2: RR exploration first, without changing seed geometry.
        for seed in seeds:
            family=seed["family"]
            p={k:seed[k] for k in FAMILY_PARAMS[family]}
            parent=cfg_id(family,p,BASE_RR)
            rr_series=[]
            for rr in RR_INITIAL:
                row=examine(family,p,rr,"RR_INITIAL",parent)
                rrrows.append(row);rr_series.append((rr,row))
            # Extend RR independently at either boundary; two consecutive
            # gated neighbours required. Do not select peak from the extension.
            for side,ext in (("low",RR_EXTRA_LOW),("high",RR_EXTRA_HIGH)):
                ordered=rr_series if side=="low" else list(reversed(rr_series))
                if all(r["research_gate_pass"] for _,r in ordered[:2]):
                    prev=ordered[0][0]
                    for value in ext:
                        row=examine(family,p,value,"RR_EXTENSION",parent)
                        rrrows.append(row)
                        edge.append({"seed":parent,"family":family,"factor":"rr",
                            "direction":side,"value":value,"gate_pass":row["research_gate_pass"],
                            "decision":"CONTINUE" if row["research_gate_pass"] else "STOP"})
                        prev=value
                        if not row["research_gate_pass"]:break
                    else:
                        edge.append({"seed":parent,"family":family,"factor":"rr",
                            "direction":side,"value":prev,"gate_pass":True,
                            "decision":"UNRESOLVED_EDGE_AT_CAP"})
                else:
                    edge.append({"seed":parent,"family":family,"factor":"rr",
                        "direction":side,"value":ordered[0][0],"gate_pass":False,
                        "decision":"NO_EXTENSION_GATE"})
            # One-factor tests centered on each seed (not a combinatorial search).
            for factor,(initial,lowext,highext) in FAMILY_PARAMS[family].items():
                values=sorted(set(initial+[p[factor]]))
                rows=[]
                for value in values:
                    trial=dict(p);trial[factor]=value
                    row=examine(family,trial,BASE_RR,"FACTOR_INITIAL",parent)
                    sweep.append({**row,"factor":factor,"factor_value":value})
                    rows.append((value,row))
                for side,extras in (("low",lowext),("high",highext)):
                    order=rows if side=="low" else list(reversed(rows))
                    if all(r["research_gate_pass"] for _,r in order[:2]):
                        last=order[0][0]
                        for value in extras:
                            trial=dict(p);trial[factor]=value
                            row=examine(family,trial,BASE_RR,"FACTOR_EXTENSION",parent)
                            sweep.append({**row,"factor":factor,"factor_value":value})
                            edge.append({"seed":parent,"family":family,"factor":factor,
                                "direction":side,"value":value,"gate_pass":row["research_gate_pass"],
                                "decision":"CONTINUE" if row["research_gate_pass"] else "STOP"})
                            last=value
                            if not row["research_gate_pass"]:break
                        else:
                            edge.append({"seed":parent,"family":family,"factor":factor,
                                "direction":side,"value":last,"gate_pass":True,
                                "decision":"UNRESOLVED_EDGE_AT_CAP"})
                    else:
                        edge.append({"seed":parent,"family":family,"factor":factor,
                           "direction":side,"value":order[0][0],"gate_pass":False,
                           "decision":"NO_EXTENSION_GATE"})
        files["rr_plateau_all.csv"]=rrrows
        files["one_factor_plateau_all.csv"]=sweep
        files["boundary_extension_decisions.csv"]=edge
        # Export ALL Stage-1 trades only for gated seeds, plus each distinct
        # gated Stage-2 configuration, so later exact portfolio test can use
        # frozen trade timestamps instead of rebuilding research results.
        to_export={r["config_id"] for r in seeds}
        to_export.update(r["config_id"] for r in rrrows+sweep if r["research_gate_pass"])
        for ident in sorted(to_export):
            raw,m,tr,stress,ok,fail=evaluated[ident] if ident in evaluated else (
                *cache[ident][3:5],cache[ident][5],[],True,"")
            trades_export.extend(export_trade(t) for t in tr)
            for k in (1,2,3,5):
                left=end-timedelta(days=365.25*k)
                s=stats([t for t in tr if left<=t["entry_time"]<end])
                detailed.append({"config_id":ident,"period":f"last_{k}y",**s})
            for yr in range(start.year,end.year+1):
                s=stats([t for t in tr if t["entry_time"].year==yr])
                calendar.append({"config_id":ident,"year":yr,**s})
        files["gated_candidate_trades.csv"]=trades_export
        files["gated_recent_periods.csv"]=detailed
        files["gated_calendar_years.csv"]=calendar
        files["study_notes.csv"]=[{"note":s} for s in [
          "Research discovery only. Absolutely no live order submission or executor changes.",
          "Previously inspected EUR/AUD history makes every historical split exploratory; forward evidence remains necessary.",
          "Outside-reversal SHORT was already studied (approx. 99 trades, +30.41R, PF 1.47) and failed rolling robustness; not recycled in this study.",
          "Stage 1 has 38 total configs at fixed 3.5R; at most 2 passing geometry seeds per family are advanced.",
          "Extensions apply only after both adjacent extreme settings pass; stop at first failed extension; cap is declared in source.",
          "The gate is a research shortlisting aid, not a test of statistical significance or a live promotion rule.",
          "2/4-pip midpoint adverse entry costs are assumptions, not verified live or historical EUR/AUD spreads.",
          "No result is a Portfolio 27 improvement claim: an independent exact 27-to-28 addition test is required.",
          "H1 EMA inputs in pullback continuation are strictly completed; signal candle is never used to set its trend regime.",
          "Signal entry and exits are modelled from candle midpoint, not from bid/ask or tick chronology.",
        ]] 
        save_bundle(files)
        STATUS.update(state="complete",message="Complete: download ZIP",results_name=BUNDLE.name,
                      stage1_count=len(stage1),seeds=len(seeds),gated_stage2=len(to_export))
    except Exception as exc:
        STATUS.update(state="failed",message=str(exc),error=traceback.format_exc()[-12000:])


@app.route("/")
def root():
    return jsonify({"service":"EUR/AUD H1 SHORT Portfolio-First Discovery",
       "read_only":True,"orders_supported":False,"stage1_geometries":38,
       "families":list(FAMILY_PARAMS),"status_url":"/euraud-h1-short-discovery/status",
       "results_url":"/euraud-h1-short-discovery/results"})


def launch():
    global STARTED
    with LOCK:
        if STARTED:
            return False
        STARTED=True
    threading.Thread(target=main_research,name="euraud-h1-short-research",daemon=True).start()
    return True


@app.route("/euraud-h1-short-discovery/start")
def start_route():
    return jsonify({"started_now":launch(),"state":STATUS["state"],"orders_supported":False})


@app.route("/euraud-h1-short-discovery/status")
def status_route():
    return jsonify(STATUS)


@app.route("/euraud-h1-short-discovery/results")
def result_route():
    if STATUS["state"]!="complete" or not BUNDLE.is_file():
        return jsonify({"status":"not_ready", "state":STATUS["state"]}),404
    return send_file(str(BUNDLE),as_attachment=True,download_name=BUNDLE.name)


if __name__=="__main__":
    launch()
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")),debug=False)
