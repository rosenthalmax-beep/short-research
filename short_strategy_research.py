import os
import itertools
import threading
import requests
import pandas as pd
from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

app = Flask(__name__)

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_URL = "https://api-fxtrade.oanda.com"
INSTRUMENT = "GBP_USD"
TICK_SIZE = 0.00001
NY_TZ = ZoneInfo("America/New_York")
DAILY_ALIGNMENT_HOUR = 17
DAILY_ALIGNMENT_TIMEZONE = "America/New_York"
STOP_BUFFER_TICKS = 10
BACKTEST_SLIPPAGE_TICKS = 5
H1_CHUNK_DAYS = 180
RESEARCH_FROM = datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc)
RESEARCH_TO = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
H1_WARMUP_DAYS = 150
DAILY_WARMUP_DAYS = 2200
OUTPUT_ALL = "gbpusd_short_pareto_refinement_all.csv"
OUTPUT_PARETO = "gbpusd_short_pareto_frontier.csv"
SLOW_EMA = 100
FAST_EMA = 40

ERAS = [
    ("2002_2009", datetime(2002,5,6,20,0,tzinfo=timezone.utc), datetime(2010,1,1,0,0,tzinfo=timezone.utc)),
    ("2010_2017", datetime(2010,1,1,0,0,tzinfo=timezone.utc), datetime(2018,1,1,0,0,tzinfo=timezone.utc)),
    ("2018_2023", datetime(2018,1,1,0,0,tzinfo=timezone.utc), datetime(2024,1,1,0,0,tzinfo=timezone.utc)),
    ("2024_present", datetime(2024,1,1,0,0,tzinfo=timezone.utc), None),
]

QUALITY_GRID = {
    "body_ratio":[1.00,1.05,1.10],
    "structure_lookback":[50,55,60,65,70],
    "max_distance_atr":[0.10,0.125,0.15,0.175,0.20],
    "strong_close_max":[None,0.40,0.35],
    "fast_ema_required":[True,False],
    "ema100_slope_max":[None,0.00,-0.02,-0.05,-0.08],
    "daily_atr_ratio_min":[None,0.80,0.90,1.00,1.10],
    "rr":[2.50,2.75,3.00],
}

BALANCED_GRID = {
    "body_ratio":[1.00,1.05,1.10],
    "structure_lookback":[35,40,45,50,55],
    "max_distance_atr":[0.15,0.175,0.20,0.225,0.25],
    "strong_close_max":[0.45,0.40,0.35,None],
    "daily_atr_ratio_min":[None,0.80,0.90,1.00],
    "signal_range_atr_min":[None,0.70,0.80,0.90,1.00],
    "sweep_lookback":[None,20,40],
    "rr":[2.50,2.75,3.00],
}

STATUS = {
    "state":"not_started",
    "message":"Research has not started",
    "service":"GBPUSD Short Quality vs Frequency Pareto Refinement",
    "instrument":INSTRUMENT,
    "quality_tests":0,
    "balanced_tests":0,
    "total_tests":0,
    "completed_tests":0,
    "rows_saved":0,
    "pareto_rows":0,
}

def headers():
    if not OANDA_TOKEN:
        raise RuntimeError("OANDA_TOKEN is not configured")
    return {"Authorization":f"Bearer {OANDA_TOKEN}"}

def iso_utc(dt):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00","Z")

def oanda_get(path, params):
    r=requests.get(OANDA_URL+path,headers=headers(),params=params,timeout=30)
    if not r.ok:
        raise RuntimeError(f"OANDA {r.status_code}: {r.text[:500]}")
    return r.json()

def parse_candle(raw):
    if not raw.get("complete",False): return None
    mid=raw.get("mid")
    if not mid: return None
    return {"time":datetime.fromisoformat(raw["time"].replace("Z","+00:00")),
            "open":float(mid["o"]),"high":float(mid["h"]),
            "low":float(mid["l"]),"close":float(mid["c"])}

def fetch_range(instrument,granularity,start,end):
    params={"price":"M","granularity":granularity,"from":iso_utc(start),"to":iso_utc(end),
            "smooth":"false","includeFirst":"true","dailyAlignment":17,
            "alignmentTimezone":"America/New_York"}
    data=oanda_get(f"/v3/instruments/{instrument}/candles",params)
    out=[]
    for raw in data.get("candles",[]):
        c=parse_candle(raw)
        if c is not None: out.append(c)
    return out

def fetch_chunked_history(instrument,granularity,start,end):
    by_time={}
    cursor=start
    while cursor<end:
        chunk_end=min(cursor+timedelta(days=H1_CHUNK_DAYS),end)
        print(f"Fetching {granularity}: {cursor.date()} -> {chunk_end.date()}",flush=True)
        for c in fetch_range(instrument,granularity,cursor,chunk_end):
            by_time[c["time"]]=c
        cursor=chunk_end
    out=list(by_time.values())
    out.sort(key=lambda x:x["time"])
    return out

def sma_series(values,length):
    out=[None]*len(values)
    if len(values)<length:return out
    s=sum(values[:length]);out[length-1]=s/length
    for i in range(length,len(values)):
        s+=values[i]-values[i-length]
        out[i]=s/length
    return out

def ema_series(values,length):
    out=[None]*len(values)
    if len(values)<length:return out
    initial=sum(values[:length])/length
    out[length-1]=initial
    m=2.0/(length+1.0);p=initial
    for i in range(length,len(values)):
        p=((values[i]-p)*m)+p
        out[i]=p
    return out

def true_ranges(c):
    out=[]
    for i,x in enumerate(c):
        if i==0: tr=x["high"]-x["low"]
        else:
            pc=c[i-1]["close"]
            tr=max(x["high"]-x["low"],abs(x["high"]-pc),abs(x["low"]-pc))
        out.append(tr)
    return out

def rma_series(values,length):
    out=[None]*len(values)
    if len(values)<length:return out
    p=sum(values[:length])/length
    out[length-1]=p
    for i in range(length,len(values)):
        p=((p*(length-1))+values[i])/length
        out[i]=p
    return out

def atr_series(c,length=14): return rma_series(true_ranges(c),length)

def current_daily_start(ts):
    ny=ts.astimezone(NY_TZ)
    candidate=ny.replace(hour=17,minute=0,second=0,microsecond=0)
    if ny<candidate:candidate-=timedelta(days=1)
    return candidate.astimezone(timezone.utc)

def build_daily_state(daily):
    closes=[c["close"] for c in daily]
    ema40=ema_series(closes,40);ema100=ema_series(closes,100);atr14=atr_series(daily,14)
    atr_sma50=sma_series([v if v is not None else 0.0 for v in atr14],50)
    for i in range(min(63,len(atr_sma50))):atr_sma50[i]=None
    return {"ema40":ema40,"ema100":ema100,"atr14":atr14,"atr14_sma50":atr_sma50}

def build_h1_daily_lookup(h1,daily,state):
    lookup=[None]*len(h1);di=-1
    for hi,c in enumerate(h1):
        ss=current_daily_start(c["time"])
        while di+1<len(daily) and daily[di+1]["time"]<ss:di+=1
        if di<0:continue
        d=daily[di];e40=state["ema40"][di];e100=state["ema100"][di];a=state["atr14"][di];asm=state["atr14_sma50"][di]
        slope=None
        if di>=5 and e100 is not None and state["ema100"][di-5] is not None and a is not None and a>0:
            slope=(e100-state["ema100"][di-5])/a
        ratio=None if a is None or asm is None or asm<=0 else a/asm
        lookup[hi]={"close":d["close"],"ema40":e40,"ema100":e100,
                    "ema100_slope_5_atr":slope,"daily_atr_ratio_50":ratio}
    return lookup

def build_candidates(h1,h1_atr,dlookup):
    out=[]
    for i in range(70,len(h1)):
        s=h1[i]
        if s["time"]<RESEARCH_FROM:continue
        if s["time"]>=RESEARCH_TO:break
        p=h1[i-1];a=h1_atr[i];d=dlookup[i]
        if a is None or a<=0 or d is None:continue
        pb=abs(p["close"]-p["open"]);cb=abs(s["close"]-s["open"])
        if pb<=0 or cb<=0:continue
        engulf=(p["close"]>p["open"] and s["close"]<s["open"] and s["open"]>=p["close"] and s["close"]<=p["open"])
        if not engulf:continue
        rng=s["high"]-s["low"]
        if rng<=0:continue
        distances={};highs={}
        for lb in [20,35,40,45,50,55,60,65,70]:
            ph=max(c["high"] for c in h1[i-lb:i]);highs[lb]=ph;distances[lb]=(ph-s["high"])/a
        out.append({"index":i,"time":s["time"],"body_ratio":cb/pb,
                    "strong_close":(s["close"]-s["low"])/rng,
                    "signal_range_atr":rng/a,"structure_distances":distances,
                    "sweep_prev20":s["high"]>=highs[20],"sweep_prev40":s["high"]>=highs[40],
                    "daily":d})
    return out

def quality_allowed(c,p):
    if c["body_ratio"]<p["body_ratio"]:return False
    if c["structure_distances"][p["structure_lookback"]]>p["max_distance_atr"]:return False
    sc=p["strong_close_max"]
    if sc is not None and c["strong_close"]>sc:return False
    d=c["daily"];e100=d.get("ema100");e40=d.get("ema40")
    if e100 is None or not d["close"]<e100:return False
    if p["fast_ema_required"] and (e40 is None or not e40<e100):return False
    sm=p["ema100_slope_max"]
    if sm is not None:
        slope=d.get("ema100_slope_5_atr")
        if slope is None or slope>sm:return False
    vm=p["daily_atr_ratio_min"]
    if vm is not None:
        ratio=d.get("daily_atr_ratio_50")
        if ratio is None or ratio<vm:return False
    return True

def balanced_allowed(c,p):
    if c["body_ratio"]<p["body_ratio"]:return False
    if c["structure_distances"][p["structure_lookback"]]>p["max_distance_atr"]:return False
    sc=p["strong_close_max"]
    if sc is not None and c["strong_close"]>sc:return False
    rm=p["signal_range_atr_min"]
    if rm is not None and c["signal_range_atr"]<rm:return False
    d=c["daily"];e100=d.get("ema100");e40=d.get("ema40")
    if e100 is None or e40 is None or not d["close"]<e100 or not e40<e100:return False
    vm=p["daily_atr_ratio_min"]
    if vm is not None:
        ratio=d.get("daily_atr_ratio_50")
        if ratio is None or ratio<vm:return False
    sw=p["sweep_lookback"]
    if sw==20 and not c["sweep_prev20"]:return False
    if sw==40 and not c["sweep_prev40"]:return False
    return True

EXIT_CACHE={}
def calculate_trade_exit(h1,signal_index,rr):
    key=(signal_index,rr)
    if key in EXIT_CACHE:return EXIT_CACHE[key]
    s=h1[signal_index];ref=s["close"];entry=ref-BACKTEST_SLIPPAGE_TICKS*TICK_SIZE
    stop=s["high"]+STOP_BUFFER_TICKS*TICK_SIZE;risk_ref=stop-ref
    if risk_ref<=0:raise RuntimeError("Invalid short reference risk")
    target=ref-risk_ref*rr;actual=stop-entry
    if actual<=0:raise RuntimeError("Invalid short actual risk")
    for i in range(signal_index+1,len(h1)):
        c=h1[i]
        if c["time"]>=RESEARCH_TO:break
        sh=c["high"]>=stop;th=c["low"]<=target
        if not(sh or th):continue
        if sh and th:
            if abs(c["high"]-c["open"])<abs(c["open"]-c["low"]):xp=stop;reason="STOP"
            else:xp=target;reason="TARGET"
        elif sh:xp=stop;reason="STOP"
        else:xp=target;reason="TARGET"
        result={"status":"CLOSED","signal_index":signal_index,"signal_time":s["time"],
                "exit_index":i,"exit_time":c["time"],"exit_reason":reason,
                "result_r":(entry-xp)/actual}
        EXIT_CACHE[key]=result;return result
    result={"status":"OPEN","signal_index":signal_index,"signal_time":s["time"],
            "exit_index":None,"exit_time":None,"exit_reason":None,"result_r":None}
    EXIT_CACHE[key]=result;return result

def simulate(h1,candidates,rr):
    trades=[];exit_i=-1;ignored=0;still_open=False
    for c in candidates:
        si=c["index"]
        if si<exit_i:ignored+=1;continue
        t=calculate_trade_exit(h1,si,rr)
        if t["status"]=="OPEN":still_open=True;break
        trades.append(t);exit_i=t["exit_index"]
    return trades,ignored,still_open

def stats_for_trades(trades,start=None,end=None):
    ts=[t for t in trades if (start is None or t["signal_time"]>=start) and (end is None or t["signal_time"]<end)]
    if not ts:return {"trades":0,"winners":0,"losers":0,"win_rate":0.0,"profit_factor":0.0,"total_r":0.0,"expectancy_r":0.0,"max_drawdown_r":0.0,"longest_loss_streak":0}
    r=[t["result_r"] for t in ts];w=[x for x in r if x>0];l=[x for x in r if x<0]
    gp=sum(w);gl=abs(sum(l));pf=gp/gl if gl>0 else (999.0 if gp>0 else 0.0);total=sum(r)
    eq=0.0;peak=0.0;dd=0.0;cur=0;longest=0
    for x in r:
        eq+=x;peak=max(peak,eq);dd=min(dd,eq-peak)
        if x<0:cur+=1;longest=max(longest,cur)
        else:cur=0
    return {"trades":len(r),"winners":len(w),"losers":len(l),
            "win_rate":round(len(w)/len(r)*100,2),"profit_factor":round(pf,3),
            "total_r":round(total,2),"expectancy_r":round(total/len(r),3),
            "max_drawdown_r":round(dd,2),"longest_loss_streak":longest}

def product_dict(grid):
    keys=list(grid)
    for combo in itertools.product(*[grid[k] for k in keys]):
        yield dict(zip(keys,combo))

def make_row(family,p,eligible,trades,ignored,still_open,years):
    full=stats_for_trades(trades)
    row={"family":family,"body_ratio":p["body_ratio"],"structure_lookback":p["structure_lookback"],
         "max_distance_atr":p["max_distance_atr"],"strong_close_max":p.get("strong_close_max"),
         "fast_ema_required":p.get("fast_ema_required"),"ema100_slope_max":p.get("ema100_slope_max"),
         "daily_atr_ratio_min":p.get("daily_atr_ratio_min"),"signal_range_atr_min":p.get("signal_range_atr_min"),
         "sweep_lookback":p.get("sweep_lookback"),"reward_risk":p["rr"],"raw_signals":len(eligible),
         "ignored_due_to_open_trade":ignored,"still_open_at_end":still_open,
         "trades":full["trades"],"trades_per_year":round(full["trades"]/years,2),
         "winners":full["winners"],"losers":full["losers"],"win_rate":full["win_rate"],
         "profit_factor":full["profit_factor"],"total_r":full["total_r"],
         "expectancy_r":full["expectancy_r"],"max_drawdown_r":full["max_drawdown_r"],
         "longest_loss_streak":full["longest_loss_streak"]}
    prof=0;eras5=0;prof5=0;minpf=None;minexp=None
    for name,start,end in ERAS:
        e=stats_for_trades(trades,start,end)
        row[f"{name}_trades"]=e["trades"];row[f"{name}_pf"]=e["profit_factor"];row[f"{name}_r"]=e["total_r"];row[f"{name}_expectancy"]=e["expectancy_r"];row[f"{name}_win_rate"]=e["win_rate"]
        if e["total_r"]>0:prof+=1
        if e["trades"]>=5:
            eras5+=1
            if e["total_r"]>0:prof5+=1
            minpf=e["profit_factor"] if minpf is None else min(minpf,e["profit_factor"])
            minexp=e["expectancy_r"] if minexp is None else min(minexp,e["expectancy_r"])
    row["profitable_eras"]=prof;row["eras_with_5_plus_trades"]=eras5;row["profitable_eras_with_5_plus_trades"]=prof5;row["minimum_era_pf_5_plus"]=minpf;row["minimum_era_expectancy_5_plus"]=minexp
    return row

def pareto_flags(df):
    pf=df["profit_factor"].to_numpy();freq=df["trades_per_year"].to_numpy();flags=[True]*len(df)
    for i in range(len(df)):
        for j in range(len(df)):
            if i!=j and pf[j]>=pf[i] and freq[j]>=freq[i] and (pf[j]>pf[i] or freq[j]>freq[i]):
                flags[i]=False;break
    return flags

def run_research():
    global STATUS
    try:
        qp=list(product_dict(QUALITY_GRID));bp=list(product_dict(BALANCED_GRID));total=len(qp)+len(bp)
        STATUS.update({"quality_tests":len(qp),"balanced_tests":len(bp),"total_tests":total,
                       "state":"fetching_data","message":"Fetching GBP/USD OANDA history"})
        h1=fetch_chunked_history(INSTRUMENT,"H1",RESEARCH_FROM-timedelta(days=H1_WARMUP_DAYS),RESEARCH_TO)
        daily=fetch_chunked_history(INSTRUMENT,"D",RESEARCH_FROM-timedelta(days=DAILY_WARMUP_DAYS),RESEARCH_TO)
        if not h1 or not daily:raise RuntimeError("Missing OANDA history")
        STATUS.update({"state":"precomputing","message":"Building indicators and features"})
        h1_atr=atr_series(h1,14);ds=build_daily_state(daily);dl=build_h1_daily_lookup(h1,daily,ds);candidates=build_candidates(h1,h1_atr,dl)
        STATUS["base_bearish_engulfings"]=len(candidates)
        years=(RESEARCH_TO-RESEARCH_FROM).total_seconds()/(365.2425*24*60*60)
        rows=[];done=0
        STATUS.update({"state":"running_quality","message":"Running quality family"})
        for p in qp:
            elig=[c for c in candidates if quality_allowed(c,p)]
            tr,ig,op=simulate(h1,elig,p["rr"]);rows.append(make_row("QUALITY",p,elig,tr,ig,op,years));done+=1;STATUS["completed_tests"]=done
        STATUS.update({"state":"running_balanced","message":"Running balanced family"})
        for p in bp:
            elig=[c for c in candidates if balanced_allowed(c,p)]
            tr,ig,op=simulate(h1,elig,p["rr"]);rows.append(make_row("BALANCED",p,elig,tr,ig,op,years));done+=1;STATUS["completed_tests"]=done
        df=pd.DataFrame(rows)
        df["all_four_eras_profitable"]=df["profitable_eras_with_5_plus_trades"]>=4
        df["pareto_efficient"]=False
        e=df[(df["trades"]>=80)&df["all_four_eras_profitable"]].copy()
        if not e.empty:
            e["pareto_efficient"]=pareto_flags(e)
            df.loc[e.index,"pareto_efficient"]=e["pareto_efficient"]
        df["pf_x_frequency"]=df["profit_factor"]*df["trades_per_year"]
        df["expectancy_x_frequency"]=df["expectancy_r"]*df["trades_per_year"]
        df.to_csv(OUTPUT_ALL,index=False)
        pareto=df[df["pareto_efficient"]].sort_values(["trades_per_year","profit_factor"],ascending=[True,False])
        pareto.to_csv(OUTPUT_PARETO,index=False)
        STATUS.update({"state":"complete","message":"GBP/USD Pareto refinement complete",
                       "completed_tests":total,"rows_saved":len(df),"pareto_rows":len(pareto),
                       "output_all":OUTPUT_ALL,"output_pareto":OUTPUT_PARETO,
                       "earliest_h1":h1[0]["time"].isoformat(),"latest_h1":h1[-1]["time"].isoformat()})
    except Exception as e:
        STATUS.update({"state":"error","message":str(e)})
        print("ERROR:",e,flush=True)

@app.route("/")
def home():
    return jsonify({"service":"GBPUSD Short Quality vs Frequency Pareto Refinement","status":STATUS,
                    "downloads":{"all_results":"/download/all","pareto_frontier":"/download/pareto"},
                    "trading_enabled":False,"orders_supported":False,"executor_connected":False})

@app.route("/status")
def status():return jsonify(STATUS)

@app.route("/download/all")
def download_all():
    if not os.path.exists(OUTPUT_ALL):return jsonify({"status":"not_ready"}),404
    return send_file(OUTPUT_ALL,as_attachment=True,download_name=OUTPUT_ALL)

@app.route("/download/pareto")
def download_pareto():
    if not os.path.exists(OUTPUT_PARETO):return jsonify({"status":"not_ready"}),404
    return send_file(OUTPUT_PARETO,as_attachment=True,download_name=OUTPUT_PARETO)

if __name__=="__main__":
    threading.Thread(target=run_research,daemon=True).start()
    app.run(host="0.0.0.0",port=int(os.getenv("PORT",5000)),debug=False)
