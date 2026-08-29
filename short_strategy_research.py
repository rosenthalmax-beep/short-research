import os, itertools, threading, requests
import pandas as pd
from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

app = Flask(__name__)

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_URL = "https://api-fxtrade.oanda.com"
INSTRUMENT = "USD_JPY"
TICK_SIZE = 0.001
NY_TZ = ZoneInfo("America/New_York")

DAILY_ALIGNMENT_HOUR = 17
STOP_BUFFER_TICKS = 10
SLIPPAGE_TICKS = 5
CHUNK_DAYS = 180

RESEARCH_FROM = datetime(2002,5,6,20,0,tzinfo=timezone.utc)
RESEARCH_TO = datetime.now(timezone.utc).replace(minute=0,second=0,microsecond=0)

OUTPUT = "usdjpy_short_broad_structural_discovery.csv"

BODY = [0.90,1.00,1.10,1.20,1.30,1.40,1.50]
STRUCTURE = [20,30,40,50,60,70,80]
DISTANCE = [0.05,0.10,0.15,0.20,0.25,0.30,0.40]
RR = [1.50,1.75,2.00,2.25,2.50,2.75,3.00,3.50,4.00]
EMA = [50,75,100,125,150,175,200,250,300,350,400]

TOTAL = len(BODY)*len(STRUCTURE)*len(DISTANCE)*len(RR)*len(EMA)

ERAS = [
    ("2002_2009", datetime(2002,5,6,20,0,tzinfo=timezone.utc), datetime(2010,1,1,tzinfo=timezone.utc)),
    ("2010_2017", datetime(2010,1,1,tzinfo=timezone.utc), datetime(2018,1,1,tzinfo=timezone.utc)),
    ("2018_2023", datetime(2018,1,1,tzinfo=timezone.utc), datetime(2024,1,1,tzinfo=timezone.utc)),
    ("2024_present", datetime(2024,1,1,tzinfo=timezone.utc), None),
]

STATUS = {
    "state":"not_started",
    "service":"USDJPY Short Broad Structural Discovery",
    "total_combinations":TOTAL,
    "completed_combinations":0
}

def headers():
    if not OANDA_TOKEN:
        raise RuntimeError("OANDA_TOKEN is not configured")
    return {"Authorization":f"Bearer {OANDA_TOKEN}"}

def iso(dt):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00","Z")

def get_candles(granularity,start,end):
    out={}
    cursor=start
    while cursor<end:
        chunk_end=min(cursor+timedelta(days=CHUNK_DAYS),end)
        print(f"Fetching {granularity}: {cursor.date()} -> {chunk_end.date()}",flush=True)
        params={
            "price":"M",
            "granularity":granularity,
            "from":iso(cursor),
            "to":iso(chunk_end),
            "smooth":"false",
            "includeFirst":"true",
            "dailyAlignment":17,
            "alignmentTimezone":"America/New_York"
        }
        r=requests.get(
            f"{OANDA_URL}/v3/instruments/{INSTRUMENT}/candles",
            headers=headers(),params=params,timeout=30
        )
        if not r.ok:
            raise RuntimeError(f"OANDA {r.status_code}: {r.text[:500]}")
        for raw in r.json().get("candles",[]):
            if not raw.get("complete",False) or not raw.get("mid"):
                continue
            m=raw["mid"]
            t=datetime.fromisoformat(raw["time"].replace("Z","+00:00"))
            out[t]={
                "time":t,
                "open":float(m["o"]),
                "high":float(m["h"]),
                "low":float(m["l"]),
                "close":float(m["c"])
            }
        cursor=chunk_end
    data=list(out.values())
    data.sort(key=lambda x:x["time"])
    return data

def ema(values,length):
    out=[None]*len(values)
    if len(values)<length:
        return out
    seed=sum(values[:length])/length
    out[length-1]=seed
    a=2/(length+1)
    prev=seed
    for i in range(length,len(values)):
        prev=(values[i]-prev)*a+prev
        out[i]=prev
    return out

def rma(values,length):
    out=[None]*len(values)
    if len(values)<length:
        return out
    prev=sum(values[:length])/length
    out[length-1]=prev
    for i in range(length,len(values)):
        prev=(prev*(length-1)+values[i])/length
        out[i]=prev
    return out

def atr(candles,length=14):
    tr=[]
    for i,c in enumerate(candles):
        if i==0:
            tr.append(c["high"]-c["low"])
        else:
            pc=candles[i-1]["close"]
            tr.append(max(
                c["high"]-c["low"],
                abs(c["high"]-pc),
                abs(c["low"]-pc)
            ))
    return rma(tr,length)

def session_start(ts):
    ny=ts.astimezone(NY_TZ)
    x=ny.replace(hour=17,minute=0,second=0,microsecond=0)
    if ny<x:
        x-=timedelta(days=1)
    return x.astimezone(timezone.utc)

def daily_lookup(h1,daily):
    closes=[c["close"] for c in daily]
    em={n:ema(closes,n) for n in EMA}
    lookup=[None]*len(h1)
    di=-1
    for i,c in enumerate(h1):
        ss=session_start(c["time"])
        while di+1<len(daily) and daily[di+1]["time"]<ss:
            di+=1
        if di<0:
            continue
        lookup[i]={
            "close":daily[di]["close"],
            "emas":{n:em[n][di] for n in EMA}
        }
    return lookup

def build_candidates(h1,h1atr,dlookup):
    out=[]
    maxlb=max(STRUCTURE)
    for i in range(maxlb,len(h1)):
        s=h1[i]
        if s["time"]<RESEARCH_FROM:
            continue
        if s["time"]>=RESEARCH_TO:
            break
        p=h1[i-1]
        a=h1atr[i]
        d=dlookup[i]
        if a is None or a<=0 or d is None:
            continue
        pb=abs(p["close"]-p["open"])
        cb=abs(s["close"]-s["open"])
        if pb<=0 or cb<=0:
            continue
        engulf=(
            p["close"]>p["open"]
            and s["close"]<s["open"]
            and s["open"]>=p["close"]
            and s["close"]<=p["open"]
        )
        if not engulf:
            continue
        distances={}
        for lb in STRUCTURE:
            ph=max(x["high"] for x in h1[i-lb:i])
            distances[lb]=(ph-s["high"])/a
        out.append({
            "index":i,
            "time":s["time"],
            "body_ratio":cb/pb,
            "distance":distances,
            "daily":d
        })
    return out

EXIT_CACHE={}

def trade_exit(h1,signal_index,rr):
    key=(signal_index,rr)
    if key in EXIT_CACHE:
        return EXIT_CACHE[key]
    s=h1[signal_index]
    ref=s["close"]
    entry=ref-SLIPPAGE_TICKS*TICK_SIZE
    stop=s["high"]+STOP_BUFFER_TICKS*TICK_SIZE
    ref_risk=stop-ref
    actual_risk=stop-entry
    if ref_risk<=0 or actual_risk<=0:
        raise RuntimeError("Invalid short risk")
    target=ref-ref_risk*rr
    for j in range(signal_index+1,len(h1)):
        c=h1[j]
        if c["time"]>=RESEARCH_TO:
            break
        sh=c["high"]>=stop
        th=c["low"]<=target
        if not (sh or th):
            continue
        if sh and th:
            if abs(c["high"]-c["open"])<abs(c["open"]-c["low"]):
                xp=stop; reason="STOP"
            else:
                xp=target; reason="TARGET"
        elif sh:
            xp=stop; reason="STOP"
        else:
            xp=target; reason="TARGET"
        result={
            "status":"CLOSED",
            "signal_index":signal_index,
            "signal_time":s["time"],
            "exit_index":j,
            "exit_time":c["time"],
            "exit_reason":reason,
            "result_r":(entry-xp)/actual_risk
        }
        EXIT_CACHE[key]=result
        return result
    result={
        "status":"OPEN",
        "signal_index":signal_index,
        "signal_time":s["time"],
        "exit_index":None,
        "result_r":None
    }
    EXIT_CACHE[key]=result
    return result

def simulate(h1,cands,rr):
    trades=[]
    exit_i=-1
    ignored=0
    still_open=False
    for c in cands:
        si=c["index"]
        if si<exit_i:
            ignored+=1
            continue
        t=trade_exit(h1,si,rr)
        if t["status"]=="OPEN":
            still_open=True
            break
        trades.append(t)
        exit_i=t["exit_index"]
    return trades,ignored,still_open

def stats(trades,start=None,end=None):
    arr=[
        t for t in trades
        if (start is None or t["signal_time"]>=start)
        and (end is None or t["signal_time"]<end)
    ]
    if not arr:
        return {
            "trades":0,"winners":0,"losers":0,"win_rate":0.0,
            "profit_factor":0.0,"total_r":0.0,"expectancy_r":0.0,
            "max_drawdown_r":0.0,"longest_loss_streak":0
        }
    rs=[t["result_r"] for t in arr]
    wins=[x for x in rs if x>0]
    losses=[x for x in rs if x<0]
    gp=sum(wins)
    gl=abs(sum(losses))
    pf=gp/gl if gl else (999.0 if gp>0 else 0.0)
    total=sum(rs)
    eq=0; peak=0; dd=0; cur=0; longest=0
    for x in rs:
        eq+=x
        peak=max(peak,eq)
        dd=min(dd,eq-peak)
        if x<0:
            cur+=1
            longest=max(longest,cur)
        else:
            cur=0
    return {
        "trades":len(rs),
        "winners":len(wins),
        "losers":len(losses),
        "win_rate":round(len(wins)/len(rs)*100,2),
        "profit_factor":round(pf,3),
        "total_r":round(total,2),
        "expectancy_r":round(total/len(rs),3),
        "max_drawdown_r":round(dd,2),
        "longest_loss_streak":longest
    }

def result_row(body,lb,dist,rr,slow,cands,trades,ignored,open_trade,years):
    full=stats(trades)
    row={
        "body_ratio":body,
        "structure_lookback":lb,
        "max_distance_atr":dist,
        "reward_risk":rr,
        "slow_daily_ema":slow,
        "raw_signals":len(cands),
        "ignored_due_to_open_trade":ignored,
        "still_open_at_end":open_trade,
        "trades":full["trades"],
        "trades_per_year":round(full["trades"]/years,2),
        "winners":full["winners"],
        "losers":full["losers"],
        "win_rate":full["win_rate"],
        "profit_factor":full["profit_factor"],
        "total_r":full["total_r"],
        "expectancy_r":full["expectancy_r"],
        "max_drawdown_r":full["max_drawdown_r"],
        "longest_loss_streak":full["longest_loss_streak"]
    }
    prof=0; prof5=0; eras5=0; minpf=None; minexp=None
    for name,start,end in ERAS:
        e=stats(trades,start,end)
        row[f"{name}_trades"]=e["trades"]
        row[f"{name}_pf"]=e["profit_factor"]
        row[f"{name}_r"]=e["total_r"]
        row[f"{name}_expectancy"]=e["expectancy_r"]
        if e["total_r"]>0:
            prof+=1
        if e["trades"]>=5:
            eras5+=1
            if e["total_r"]>0:
                prof5+=1
            minpf=e["profit_factor"] if minpf is None else min(minpf,e["profit_factor"])
            minexp=e["expectancy_r"] if minexp is None else min(minexp,e["expectancy_r"])
    row["profitable_eras"]=prof
    row["eras_with_5_plus_trades"]=eras5
    row["profitable_eras_with_5_plus_trades"]=prof5
    row["minimum_era_pf_5_plus"]=minpf
    row["minimum_era_expectancy_5_plus"]=minexp
    return row

def run():
    global STATUS
    try:
        STATUS.update({"state":"fetching","message":"Fetching USD/JPY history"})
        h1=get_candles("H1",RESEARCH_FROM-timedelta(days=180),RESEARCH_TO)
        daily=get_candles("D",RESEARCH_FROM-timedelta(days=2600),RESEARCH_TO)
        if not h1 or not daily:
            raise RuntimeError("Missing OANDA candles")
        STATUS.update({"state":"precomputing","message":"Building candidates"})
        h1atr=atr(h1,14)
        dl=daily_lookup(h1,daily)
        candidates=build_candidates(h1,h1atr,dl)
        STATUS["base_bearish_engulfings"]=len(candidates)
        years=(RESEARCH_TO-RESEARCH_FROM).total_seconds()/(365.2425*24*3600)
        rows=[]
        STATUS.update({"state":"running","message":"Running broad structural sweep"})
        for n,(body,lb,dist,rr,slow) in enumerate(
            itertools.product(BODY,STRUCTURE,DISTANCE,RR,EMA),start=1
        ):
            elig=[
                c for c in candidates
                if c["body_ratio"]>=body
                and c["distance"][lb]<=dist
                and c["daily"]["emas"][slow] is not None
                and c["daily"]["close"]<c["daily"]["emas"][slow]
            ]
            trades,ignored,open_trade=simulate(h1,elig,rr)
            rows.append(result_row(
                body,lb,dist,rr,slow,elig,trades,ignored,open_trade,years
            ))
            STATUS["completed_combinations"]=n
            if n%500==0:
                print(f"Progress {n}/{TOTAL}",flush=True)
        df=pd.DataFrame(rows)
        df["adequate_80"]=df["trades"]>=80
        df["adequate_100"]=df["trades"]>=100
        df["all_four_eras_profitable"]=df["profitable_eras_with_5_plus_trades"]>=4
        df["annual_r_linear"]=df["expectancy_r"]*df["trades_per_year"]
        df=df.sort_values(
            ["all_four_eras_profitable","adequate_100","minimum_era_pf_5_plus",
             "profit_factor","annual_r_linear","trades_per_year"],
            ascending=[False,False,False,False,False,False]
        )
        df.to_csv(OUTPUT,index=False)
        STATUS.update({
            "state":"complete",
            "message":"USD/JPY broad short discovery complete",
            "rows_saved":len(df),
            "output_file":OUTPUT
        })
    except Exception as e:
        STATUS.update({"state":"error","message":str(e)})
        print("ERROR:",e,flush=True)

@app.route("/")
def home():
    return jsonify({
        "service":"USDJPY Short Broad Structural Discovery",
        "status":STATUS,
        "instrument":INSTRUMENT,
        "direction":"SHORT",
        "timing_filters":"NONE",
        "total_combinations":TOTAL,
        "download":"/download",
        "trading_enabled":False
    })

@app.route("/status")
def status():
    return jsonify(STATUS)

@app.route("/download")
def download():
    if not os.path.exists(OUTPUT):
        return jsonify({"status":"not_ready"}),404
    return send_file(OUTPUT,as_attachment=True,download_name=OUTPUT)

if __name__=="__main__":
    threading.Thread(target=run,daemon=True).start()
    app.run(host="0.0.0.0",port=int(os.getenv("PORT",5000)),debug=False)
