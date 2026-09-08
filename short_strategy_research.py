
import os, csv, time, bisect, zipfile, threading
from copy import deepcopy
from collections import deque, defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median
from zoneinfo import ZoneInfo

import numpy as np
import requests
from flask import Flask, jsonify, send_file

# ============================================================
# EUR/USD M15 LONG — FULL-HISTORY RE-EXAMINATION
#
# Fresh research universe:
#   2002-05-06 20:00 UTC -> present / earliest OANDA available
#
# Old locked EUR/USD M15 LONG is retained as a benchmark only:
#   exact bullish engulf
#   BR >= 1.35
#   body >= 0.75 ATR14
#   structure 165 / 0.10 ATR
#   exclude Tuesday
#   exclude NY hour 07
#   RR 3.75
#
# Full-history families:
#   ENGULF_STRUCTURE
#   SWEEP_DISPLACEMENT
#   FAILED_BREAKDOWN_RECLAIM
#   OUTSIDE_REVERSAL
#   COMPRESSION_BREAKOUT
#   WASHOUT_RECLAIM
#
# Staged Railway-safe process:
#   Stage 1 raw archetypes
#   Stage 2 broad HTF/time/weekday contexts
#   Stage 3 local geometry + RR
#   Finalists: eras, 2002-17 / 2018+, last 2/5Y,
#              0.5/1/1.5/2 pip cost stress,
#              rolling 12/24/36M, calendar years,
#              ablation and local plateau
#
# Historical conventions:
#   OANDA midpoint
#   ATR14 Wilder/RMA SMA-seeded
#   signal timestamp = M15 candle OPEN
#   primary adverse cost = 1 pip
#   long fill = signal close + adverse cost
#   stop = signal low - 10 ticks
#   target based on REFERENCE signal-close risk
#   exits next candle
#   pyramiding 0
#   exact exit-candle signal eligible
#   long same-bar: high closer to open => target else stop
#
# HTF no-lookahead:
#   complete_at = next ACTUAL HTF candle OPEN
#   lookup = bisect_right(completion_times, signal_time) - 1
#
# Daily:
#   OANDA D, dailyAlignment=17, America/New_York
#
# ONE ZIP:
#   /eurusd-m15-long-full-history/results
#
# READ ONLY. NEVER SENDS ORDERS.
# ============================================================

app = Flask(__name__)

TOKEN = os.getenv("OANDA_TOKEN")
BASE = os.getenv("OANDA_API_URL", "https://api-fxtrade.oanda.com")
PAIR = "EUR_USD"

START = datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc)
NOW = datetime.now(timezone.utc).replace(second=0, microsecond=0)
WARMUP = START - timedelta(days=900)

NY = ZoneInfo("America/New_York")

TICK = 0.00001
PIP = 0.0001
STOP_TICKS = 10

PRIMARY_COST = 1.0
COSTS = [0.5, 1.0, 1.5, 2.0]

STAGE1_KEEP = 12
STAGE2_BASE_KEEP = 7
STAGE2_KEEP = 9
STAGE3_BASE_KEEP = 4
FINAL_KEEP = 8

OUTS = {
    "coverage": "eurusd_m15_long_full_history_coverage.csv",
    "benchmark": "eurusd_m15_long_full_history_benchmark.csv",
    "stage1": "eurusd_m15_long_full_history_stage1.csv",
    "stage2": "eurusd_m15_long_full_history_stage2.csv",
    "stage3": "eurusd_m15_long_full_history_stage3.csv",
    "final": "eurusd_m15_long_full_history_finalists.csv",
    "periods": "eurusd_m15_long_full_history_periods.csv",
    "cost": "eurusd_m15_long_full_history_cost_stress.csv",
    "rolling": "eurusd_m15_long_full_history_rolling.csv",
    "rolling_summary": "eurusd_m15_long_full_history_rolling_summary.csv",
    "calendar": "eurusd_m15_long_full_history_calendar_years.csv",
    "calendar_summary": "eurusd_m15_long_full_history_calendar_summary.csv",
    "ablation": "eurusd_m15_long_full_history_ablation.csv",
    "plateau": "eurusd_m15_long_full_history_plateau.csv",
    "trades": "eurusd_m15_long_full_history_finalist_trades.csv",
}
BUNDLE = "EURUSD_M15_LONG_FULL_HISTORY_REEXAMINATION_RESULTS.zip"

STATUS = {
    "state": "not_started",
    "message": "Not started",
    "orders_supported": False,
    "trading_enabled": False,
}

# ---------------- helpers ----------------

def iso(dt):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def parse_time(s):
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    if "." in s:
        left, right = s.split(".", 1)
        sign, off = None, None
        if "+" in right:
            frac, off = right.split("+", 1); sign = "+"
        elif "-" in right:
            frac, off = right.split("-", 1); sign = "-"
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
    fields, seen = [], set()
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k); fields.append(k)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader(); w.writerows(rows)

def dl(path):
    if not os.path.exists(path):
        return jsonify({"error": "not ready"}), 404
    return send_file(os.path.abspath(path), as_attachment=True,
                     download_name=os.path.basename(path))

def pack():
    with zipfile.ZipFile(BUNDLE, "w", zipfile.ZIP_DEFLATED) as z:
        for p in OUTS.values():
            if os.path.exists(p):
                z.write(p, arcname=os.path.basename(p))

def add_months(dt, n):
    m = dt.year * 12 + dt.month - 1 + n
    return datetime(m // 12, m % 12 + 1, 1, tzinfo=timezone.utc)

def month_floor(dt):
    return datetime(dt.year, dt.month, 1, tzinfo=timezone.utc)

def med(x):
    return median(x) if x else 0.0

# ---------------- OANDA ----------------

def headers():
    if not TOKEN:
        raise RuntimeError("OANDA_TOKEN is not configured")
    return {"Authorization": "Bearer " + TOKEN.strip()}

def fetch_chunk(gran, start, end):
    params = {
        "price": "M",
        "granularity": gran,
        "smooth": "false",
        "from": iso(start),
        "to": iso(end),
        "includeFirst": "true",
    }
    if gran == "D":
        params["dailyAlignment"] = 17
        params["alignmentTimezone"] = "America/New_York"
    url = f"{BASE}/v3/instruments/{PAIR}/candles"
    r = requests.get(url, headers=headers(), params=params, timeout=60)
    r.raise_for_status()
    out = []
    for c in r.json().get("candles", []):
        if not c.get("complete", False):
            continue
        m = c["mid"]
        out.append({
            "time": parse_time(c["time"]),
            "open": float(m["o"]),
            "high": float(m["h"]),
            "low": float(m["l"]),
            "close": float(m["c"]),
        })
    return out

def fetch(gran, start, end, chunk_days):
    cur, by_time, n = start, {}, 0
    while cur < end:
        n += 1
        nxt = min(cur + timedelta(days=chunk_days), end)
        STATUS.update({
            "state": "fetching",
            "message": f"{gran} chunk {n}: {iso(cur)} -> {iso(nxt)}"
        })
        try:
            rows = fetch_chunk(gran, cur, nxt)
        except requests.HTTPError as e:
            sc = e.response.status_code if e.response is not None else None
            if sc in (400, 404):
                rows = []
            else:
                raise
        for row in rows:
            by_time[row["time"]] = row
        cur = nxt
        time.sleep(0.02)
    out = list(by_time.values())
    out.sort(key=lambda x: x["time"])
    return out

# ---------------- indicators ----------------

def tr(c):
    x = np.full(len(c), np.nan)
    for i, b in enumerate(c):
        if i == 0:
            x[i] = b["high"] - b["low"]
        else:
            pc = c[i-1]["close"]
            x[i] = max(
                b["high"] - b["low"],
                abs(b["high"] - pc),
                abs(b["low"] - pc),
            )
    return x

def rma(v, n):
    o = np.full(len(v), np.nan)
    if len(v) < n:
        return o
    seed = v[:n]
    if np.isnan(seed).any():
        return o
    o[n-1] = seed.mean()
    for i in range(n, len(v)):
        if np.isfinite(v[i]) and np.isfinite(o[i-1]):
            o[i] = (o[i-1] * (n-1) + v[i]) / n
    return o

def atr(c):
    return rma(tr(c), 14)

def sma(v, n):
    out = np.full(len(v), np.nan)
    vals = np.nan_to_num(v, nan=0.0)
    valid = np.isfinite(v).astype(int)
    cs, cc = np.cumsum(vals), np.cumsum(valid)
    for i in range(n-1, len(v)):
        total, count = cs[i], cc[i]
        if i >= n:
            total -= cs[i-n]; count -= cc[i-n]
        if count == n:
            out[i] = total / n
    return out

def ema(vals, n):
    out = [None] * len(vals)
    if len(vals) < n:
        return out
    out[n-1] = sum(vals[:n]) / n
    a = 2.0 / (n + 1.0)
    for i in range(n, len(vals)):
        out[i] = a * vals[i] + (1-a) * out[i-1]
    return out

def prev_extreme(v, lb, mode):
    out = np.full(len(v), np.nan)
    q = deque()
    for i in range(len(v)):
        oldest = i - lb
        while q and q[0] < oldest:
            q.popleft()
        if i >= lb and q:
            out[i] = v[q[0]]
        if mode == "min":
            while q and v[q[-1]] >= v[i]:
                q.pop()
        else:
            while q and v[q[-1]] <= v[i]:
                q.pop()
        q.append(i)
    return out

# ---------------- HTF ----------------

def htf_state(c):
    closes = [x["close"] for x in c]
    e50, e100, e200 = ema(closes,50), ema(closes,100), ema(closes,200)
    a = atr(c); am = sma(a,50)
    rows = []
    for i,b in enumerate(c):
        complete_at = c[i+1]["time"] if i+1 < len(c) else None
        rows.append({
            "complete_at": complete_at,
            "close": b["close"],
            "ema50": e50[i], "ema100": e100[i], "ema200": e200[i],
            "atr_ratio50": (
                float(a[i]/am[i])
                if np.isfinite(a[i]) and np.isfinite(am[i]) and am[i] > 0
                else None
            ),
        })
    return rows

def align_htf(m15_times, state):
    rows = [r for r in state if r["complete_at"] is not None]
    ct = [r["complete_at"] for r in rows]
    keys = ["close","ema50","ema100","ema200","atr_ratio50"]
    out = {k: np.full(len(m15_times), np.nan) for k in keys}
    for i,t in enumerate(m15_times):
        p = bisect.bisect_right(ct, t) - 1
        if p < 0:
            continue
        r = rows[p]
        for k in keys:
            if r[k] is not None:
                out[k][i] = r[k]
    return out

# ---------------- M15 features ----------------

def features(c, h1, h4, d):
    n = len(c)
    times = [x["time"] for x in c]
    o = np.array([x["open"] for x in c])
    h = np.array([x["high"] for x in c])
    l = np.array([x["low"] for x in c])
    cl = np.array([x["close"] for x in c])

    a = atr(c)
    am20 = sma(a,20)
    bullish = cl > o

    exact = np.zeros(n, dtype=bool)
    exact[1:] = (
        (cl[:-1] < o[:-1]) &
        (cl[1:] > o[1:]) &
        (o[1:] <= cl[:-1]) &
        (cl[1:] >= o[:-1])
    )

    body = cl-o
    prev_body = np.full(n,np.nan)
    prev_body[1:] = np.abs(cl[:-1]-o[:-1])

    br = np.full(n,np.nan)
    vpb = prev_body > 0
    br[vpb] = body[vpb]/prev_body[vpb]

    valid_atr = np.isfinite(a) & (a>0)
    body_atr = np.full(n,np.nan)
    body_atr[valid_atr] = body[valid_atr]/a[valid_atr]

    crange = h-l
    range_atr = np.full(n,np.nan)
    range_atr[valid_atr] = crange[valid_atr]/a[valid_atr]

    close_loc = np.full(n,np.nan)
    vr = crange > 0
    close_loc[vr] = (cl[vr]-l[vr])/crange[vr]

    lw = np.minimum(o,cl)-l
    lwb = np.full(n,np.nan)
    pb = body > 0
    lwb[pb] = lw[pb]/body[pb]

    comp = np.full(n,np.nan)
    va = (
        np.r_[False, np.isfinite(a[:-1])] &
        np.r_[False, np.isfinite(am20[:-1])] &
        (np.r_[0.0, am20[:-1]] > 0)
    )
    pa = np.r_[np.nan, a[:-1]]
    pam = np.r_[np.nan, am20[:-1]]
    comp[va] = pa[va]/pam[va]

    lbs = [10,20,40,60,80,100,120,165,200]
    pl = {lb: prev_extreme(l,lb,"min") for lb in lbs}
    ph = {lb: prev_extreme(h,lb,"max") for lb in lbs}

    sd = {}
    for lb in [40,60,80,100,120,165,200]:
        x = np.full(n,np.nan)
        ok = valid_atr & np.isfinite(pl[lb])
        x[ok] = np.abs(l[ok]-pl[lb][ok])/a[ok]
        sd[lb] = x

    mom4 = np.full(n,np.nan)
    for i in range(17,n):
        if valid_atr[i]:
            mom4[i] = (cl[i-1]-cl[i-17])/a[i]

    nyh = np.zeros(n,dtype=np.int16)
    nyw = np.zeros(n,dtype=np.int16)
    for i,t in enumerate(times):
        z=t.astimezone(NY)
        nyh[i],nyw[i]=z.hour,z.weekday()

    return {
        "n":n,"times":times,"open":o,"high":h,"low":l,"close":cl,
        "atr":a,"valid_atr":valid_atr,"bullish":bullish,
        "exact":exact,"br":br,"body_atr":body_atr,
        "range_atr":range_atr,"close_loc":close_loc,
        "lwb":lwb,"compression":comp,"prev_low":pl,"prev_high":ph,
        "structure_dist":sd,"mom4":mom4,"ny_hour":nyh,"ny_weekday":nyw,
        "h1_close":h1["close"],"h1_ema50":h1["ema50"],
        "h1_ema100":h1["ema100"],"h1_ema200":h1["ema200"],
        "h1_atr":h1["atr_ratio50"],
        "h4_close":h4["close"],"h4_ema100":h4["ema100"],
        "h4_ema200":h4["ema200"],"h4_atr":h4["atr_ratio50"],
        "d_close":d["close"],"d_ema50":d["ema50"],
        "d_ema200":d["ema200"],"d_atr":d["atr_ratio50"],
    }

# ---------------- configs ----------------

def cfg(cid,fam,rr=3.5,**kw):
    x = {
        "config_id":cid,"family":fam,"rr":rr,"context":"NONE",
        "br_min":None,"body_atr_min":None,"range_atr_min":None,
        "close_loc_min":None,"lower_wick_body_min":None,
        "structure_lb":None,"structure_dist_atr_max":None,
        "sweep_lb":None,"breakout_lb":None,"compression_max":None,
        "mom4_max":None,
        "excluded_weekdays":set(),"excluded_ny_hours":set(),
    }
    x.update(kw)
    return x

BENCH = cfg(
    "BENCHMARK_LOCKED_2010_PLUS","ENGULF_STRUCTURE",3.75,
    br_min=1.35,body_atr_min=0.75,
    structure_lb=165,structure_dist_atr_max=0.10,
    excluded_weekdays={1},excluded_ny_hours={7},
)

def stage1_configs():
    out=[]
    eng=[
        (1.0,.5,60,.10),(1.0,.75,100,.10),(1.2,.5,100,.15),
        (1.2,.75,120,.10),(1.2,1.0,165,.10),(1.35,.5,120,.20),
        (1.35,.75,165,.10),(1.35,1.0,165,.15),
        (1.5,.75,100,.10),(1.5,1.0,165,.10),
    ]
    for i,(br,b,lb,d) in enumerate(eng):
        out.append(cfg(f"S1_ENG_{i}","ENGULF_STRUCTURE",
                       br_min=br,body_atr_min=b,
                       structure_lb=lb,structure_dist_atr_max=d))
    sweep=[
        (20,.75,.15,-.5),(20,1,.25,-1),(40,.75,.25,-.5),
        (40,1,.25,-1),(40,1.25,.25,-1.25),(60,.75,.25,-.75),
        (60,1,.35,-1),(60,1.25,.25,-1.5),(100,1,.35,-1.25),
        (100,1.25,.35,-1.5),
    ]
    for i,(lb,b,w,m) in enumerate(sweep):
        out.append(cfg(f"S1_SWEEP_{i}","SWEEP_DISPLACEMENT",
                       sweep_lb=lb,body_atr_min=b,
                       lower_wick_body_min=w,mom4_max=m))
    fail=[
        (20,.5,.6),(20,.75,.7),(40,.5,.6),(40,.75,.7),
        (40,1,.75),(60,.5,.65),(60,.75,.7),(60,1,.75),
        (100,.75,.7),(100,1,.8),
    ]
    for i,(lb,b,c) in enumerate(fail):
        out.append(cfg(f"S1_FAIL_{i}","FAILED_BREAKDOWN_RECLAIM",
                       sweep_lb=lb,body_atr_min=b,close_loc_min=c))
    outside=[
        (.5,.6,40,.2),(.75,.65,40,.15),(.75,.7,60,.2),
        (1,.65,60,.15),(1,.75,80,.2),(1.25,.7,80,.15),
        (1.25,.75,100,.2),(1.5,.75,100,.15),
    ]
    for i,(b,c,lb,d) in enumerate(outside):
        out.append(cfg(f"S1_OUT_{i}","OUTSIDE_REVERSAL",
                       body_atr_min=b,close_loc_min=c,
                       structure_lb=lb,structure_dist_atr_max=d))
    comp=[
        (.60,.75,1.2,10),(.65,.75,1.3,10),(.65,1,1.4,10),
        (.70,.75,1.3,10),(.70,1,1.4,10),(.70,1.25,1.5,10),
        (.75,.75,1.3,10),(.75,1,1.4,10),(.75,1.25,1.5,20),
        (.80,1,1.5,20),
    ]
    for i,(co,b,r,lb) in enumerate(comp):
        out.append(cfg(f"S1_COMP_{i}","COMPRESSION_BREAKOUT",
                       compression_max=co,body_atr_min=b,
                       range_atr_min=r,breakout_lb=lb))
    wash=[
        (20,.5,-.75,.65),(20,.75,-1,.7),(40,.5,-1,.65),
        (40,.75,-1.25,.7),(40,1,-1.5,.75),(60,.75,-1.25,.7),
        (60,1,-1.5,.75),(100,1,-1.5,.75),
    ]
    for i,(lb,b,m,c) in enumerate(wash):
        out.append(cfg(f"S1_WASH_{i}","WASHOUT_RECLAIM",
                       sweep_lb=lb,body_atr_min=b,
                       mom4_max=m,close_loc_min=c))
    return out

CONTEXTS=[
    "NONE","H1_CLOSE_GT_EMA100","H1_CLOSE_GT_EMA200",
    "H1_EMA50_GT_EMA200","H4_CLOSE_GT_EMA100","H4_CLOSE_GT_EMA200",
    "D_CLOSE_GT_EMA200","D_EMA50_GT_EMA200",
    "H1_ATR_GE_080","H4_ATR_GE_080","D_ATR_GE_080",
    "NY_BLOCK_00-03","NY_BLOCK_04-07","NY_BLOCK_08-11",
    "NY_BLOCK_12-15","NY_BLOCK_16-19","NY_BLOCK_20-23",
    "EXCLUDE_WEEKDAY_0","EXCLUDE_WEEKDAY_1","EXCLUDE_WEEKDAY_2",
    "EXCLUDE_WEEKDAY_3","EXCLUDE_WEEKDAY_4",
]

# ---------------- signal evaluation ----------------

def context_mask(mask,c,f):
    ctx=c.get("context","NONE")
    if ctx=="H1_CLOSE_GT_EMA100": mask &= f["h1_close"]>f["h1_ema100"]
    elif ctx=="H1_CLOSE_GT_EMA200": mask &= f["h1_close"]>f["h1_ema200"]
    elif ctx=="H1_EMA50_GT_EMA200": mask &= f["h1_ema50"]>f["h1_ema200"]
    elif ctx=="H4_CLOSE_GT_EMA100": mask &= f["h4_close"]>f["h4_ema100"]
    elif ctx=="H4_CLOSE_GT_EMA200": mask &= f["h4_close"]>f["h4_ema200"]
    elif ctx=="D_CLOSE_GT_EMA200": mask &= f["d_close"]>f["d_ema200"]
    elif ctx=="D_EMA50_GT_EMA200": mask &= f["d_ema50"]>f["d_ema200"]
    elif ctx=="H1_ATR_GE_080": mask &= f["h1_atr"]>=.8
    elif ctx=="H4_ATR_GE_080": mask &= f["h4_atr"]>=.8
    elif ctx=="D_ATR_GE_080": mask &= f["d_atr"]>=.8
    elif ctx.startswith("NY_BLOCK_"):
        a,b=map(int,ctx.split("_")[-1].split("-"))
        mask &= (f["ny_hour"]>=a)&(f["ny_hour"]<=b)
    elif ctx.startswith("EXCLUDE_WEEKDAY_"):
        w=int(ctx.split("_")[-1]); mask &= f["ny_weekday"]!=w
    for w in c.get("excluded_weekdays",set()):
        mask &= f["ny_weekday"]!=w
    for h in c.get("excluded_ny_hours",set()):
        mask &= f["ny_hour"]!=h
    return mask

def indices(c,f):
    m=f["valid_atr"].copy() & f["bullish"]
    fam=c["family"]

    if fam=="ENGULF_STRUCTURE":
        m &= f["exact"]
        if c["br_min"] is not None: m &= f["br"]>=c["br_min"]
        if c["body_atr_min"] is not None: m &= f["body_atr"]>=c["body_atr_min"]
        m &= f["structure_dist"][c["structure_lb"]] <= c["structure_dist_atr_max"]

    elif fam=="SWEEP_DISPLACEMENT":
        lb=c["sweep_lb"]
        m &= f["low"]<f["prev_low"][lb]
        prevh=np.roll(f["high"],1); m[0]=False
        m &= f["close"]>prevh
        m &= f["body_atr"]>=c["body_atr_min"]
        m &= f["lwb"]>=c["lower_wick_body_min"]
        m &= f["mom4"]<=c["mom4_max"]

    elif fam=="FAILED_BREAKDOWN_RECLAIM":
        pl=f["prev_low"][c["sweep_lb"]]
        m &= f["low"]<pl
        m &= f["close"]>pl
        m &= f["body_atr"]>=c["body_atr_min"]
        m &= f["close_loc"]>=c["close_loc_min"]

    elif fam=="OUTSIDE_REVERSAL":
        ph=np.roll(f["high"],1); pl=np.roll(f["low"],1); m[0]=False
        m &= f["high"]>ph
        m &= f["low"]<pl
        m &= f["body_atr"]>=c["body_atr_min"]
        m &= f["close_loc"]>=c["close_loc_min"]
        m &= f["structure_dist"][c["structure_lb"]] <= c["structure_dist_atr_max"]

    elif fam=="COMPRESSION_BREAKOUT":
        m &= f["compression"]<=c["compression_max"]
        m &= f["body_atr"]>=c["body_atr_min"]
        m &= f["range_atr"]>=c["range_atr_min"]
        m &= f["close"]>f["prev_high"][c["breakout_lb"]]

    elif fam=="WASHOUT_RECLAIM":
        pl=f["prev_low"][c["sweep_lb"]]
        m &= f["low"]<pl
        m &= f["close"]>f["prev_low"][10]
        m &= f["body_atr"]>=c["body_atr_min"]
        m &= f["mom4"]<=c["mom4_max"]
        m &= f["close_loc"]>=c["close_loc_min"]

    m=context_mask(m,c,f)
    m[:200]=False
    return np.flatnonzero(m).tolist()

# ---------------- backtest ----------------

CACHE={}

def outcome(candles,i,rr,cost):
    key=(i,round(rr,3),round(cost,3))
    if key in CACHE:
        return CACHE[key]
    s=candles[i]
    ref=s["close"]
    stop=s["low"]-STOP_TICKS*TICK
    ref_risk=ref-stop
    if ref_risk<=0:
        CACHE[key]=None; return None
    target=ref+rr*ref_risk
    fill=ref+cost*PIP
    risk=fill-stop
    if risk<=0:
        CACHE[key]=None; return None
    for j in range(i+1,len(candles)):
        b=candles[j]
        hs=b["low"]<=stop
        ht=b["high"]>=target
        if hs and ht:
            if abs(b["high"]-b["open"]) < abs(b["open"]-b["low"]):
                px,reason=target,"TARGET"
            else:
                px,reason=stop,"STOP"
        elif ht:
            px,reason=target,"TARGET"
        elif hs:
            px,reason=stop,"STOP"
        else:
            continue
        r=(px-fill)/risk
        out={
            "signal_index":i,"exit_index":j,
            "entry_time":s["time"],"exit_time":b["time"],
            "entry_time_utc":iso(s["time"]),"exit_time_utc":iso(b["time"]),
            "result_r":r,"exit_reason":reason,"rr":rr,"cost_pips":cost,
        }
        CACHE[key]=out
        return out
    CACHE[key]=None
    return None

def backtest(candles,ix,rr,cost,start=None,end=None):
    use=ix
    if start is not None or end is not None:
        ts=[candles[i]["time"] for i in ix]
        a=0 if start is None else bisect.bisect_left(ts,start)
        b=len(ix) if end is None else bisect.bisect_left(ts,end)
        use=ix[a:b]
    trades=[]; p=0
    while p<len(use):
        t=outcome(candles,use[p],rr,cost)
        if t is None:
            p+=1; continue
        trades.append(dict(t))
        p=bisect.bisect_left(use,t["exit_index"],lo=p+1)
    return trades

def stats(trades):
    rs=[float(x["result_r"]) for x in trades]
    w=[x for x in rs if x>0]; l=[x for x in rs if x<0]
    gp=sum(w); gl=abs(sum(l))
    pf=gp/gl if gl>0 else (999.0 if gp>0 else 0.0)
    eq=peak=0.0; dd=0.0; st=ls=0
    for r in rs:
        eq+=r; peak=max(peak,eq); dd=min(dd,eq-peak)
        if r<0: st+=1; ls=max(ls,st)
        else: st=0
    return {
        "trades":len(rs),"winners":len(w),"losers":len(l),
        "win_rate":100*len(w)/len(rs) if rs else 0.0,
        "profit_factor":pf,"total_r":sum(rs),
        "expectancy_r":sum(rs)/len(rs) if rs else 0.0,
        "max_drawdown_r":dd,"longest_loss_streak":ls,
    }

def cfields(c):
    return {k:c.get(k) for k in [
        "br_min","body_atr_min","range_atr_min","close_loc_min",
        "lower_wick_body_min","structure_lb","structure_dist_atr_max",
        "sweep_lb","breakout_lb","compression_max","mom4_max"
    ]}

ERAS=[
    ("E1_2002_07",START,datetime(2008,1,1,tzinfo=timezone.utc)),
    ("E2_2008_13",datetime(2008,1,1,tzinfo=timezone.utc),datetime(2014,1,1,tzinfo=timezone.utc)),
    ("E3_2014_19",datetime(2014,1,1,tzinfo=timezone.utc),datetime(2020,1,1,tzinfo=timezone.utc)),
    ("E4_2020_NOW",datetime(2020,1,1,tzinfo=timezone.utc),NOW),
]

def evaluate(c,candles,ix):
    full=stats(backtest(candles,ix,c["rr"],PRIMARY_COST,candles[0]["time"],NOW))
    pre=stats(backtest(candles,ix,c["rr"],PRIMARY_COST,candles[0]["time"],datetime(2010,1,1,tzinfo=timezone.utc)))
    post=stats(backtest(candles,ix,c["rr"],PRIMARY_COST,datetime(2010,1,1,tzinfo=timezone.utc),NOW))
    epf=[]; er=[]; et=[]
    for _,a,b in ERAS:
        s=stats(backtest(candles,ix,c["rr"],PRIMARY_COST,a,b))
        epf.append(s["profit_factor"]); er.append(s["total_r"]); et.append(s["trades"])
    pos=sum(x>0 for x in er)
    score=(
        1.5*min(full["profit_factor"],3)
        +.8*min(pre["profit_factor"],2.5)
        +.8*min(post["profit_factor"],2.5)
        +.35*pos
        +.2*min(max(min(epf),0),2)
        +.15*min(full["trades"]/100,2)
    )
    row={
        "config_id":c["config_id"],"family":c["family"],
        "context":c.get("context","NONE"),"rr":c["rr"],
        "full_trades":full["trades"],"full_pf":round(full["profit_factor"],6),
        "full_r":round(full["total_r"],4),"full_exp":round(full["expectancy_r"],6),
        "full_dd":round(full["max_drawdown_r"],4),
        "pre2010_trades":pre["trades"],"pre2010_pf":round(pre["profit_factor"],6),
        "pre2010_r":round(pre["total_r"],4),
        "post2010_trades":post["trades"],"post2010_pf":round(post["profit_factor"],6),
        "post2010_r":round(post["total_r"],4),
        "positive_eras":pos,"min_era_pf":round(min(epf),6),
        "robust_score":round(score,6),
    }
    for j in range(4):
        row[f"era{j+1}_pf"]=round(epf[j],6)
        row[f"era{j+1}_r"]=round(er[j],4)
        row[f"era{j+1}_trades"]=et[j]
    row.update(cfields(c))
    return row

def sortrows(rows):
    return sorted(rows,key=lambda r:(
        r["positive_eras"],
        r["pre2010_r"]>0,
        r["post2010_r"]>0,
        r["robust_score"],
        r["full_r"],
    ),reverse=True)

# ---------------- stage 2 / 3 ----------------

def stage2(top,byid):
    out=[]
    for rank,row in enumerate(top[:STAGE2_BASE_KEEP]):
        base=byid[row["config_id"]]
        for ctx in CONTEXTS:
            x=deepcopy(base)
            x["config_id"]=f"S2_{rank}_{ctx}"
            x["context"]=ctx
            out.append(x)
    return out

def local_variants(base,rank):
    out=[]
    for rr in [3.0,3.5,3.75,4.0,4.25]:
        x=deepcopy(base); x["rr"]=rr
        x["config_id"]=f"S3_{rank}_RR_{rr}"
        out.append(x)
    bumps={
        "br_min":[-.15,-.05,.05,.15],
        "body_atr_min":[-.2,-.1,.1,.2],
        "range_atr_min":[-.2,-.1,.1,.2],
        "close_loc_min":[-.1,-.05,.05,.1],
        "lower_wick_body_min":[-.1,-.05,.05,.1],
        "structure_dist_atr_max":[-.05,-.025,.025,.05],
        "compression_max":[-.05,-.025,.025,.05],
        "mom4_max":[-.5,-.25,.25,.5],
    }
    for fld,ds in bumps.items():
        v=base.get(fld)
        if v is None: continue
        for d in ds:
            nv=round(v+d,4)
            if fld!="mom4_max" and nv<=0: continue
            x=deepcopy(base); x[fld]=nv
            x["config_id"]=f"S3_{rank}_{fld}_{nv}"
            out.append(x)
    lbs=[10,20,40,60,80,100,120,165,200]
    for fld in ["structure_lb","sweep_lb","breakout_lb"]:
        v=base.get(fld)
        if v not in lbs: continue
        p=lbs.index(v)
        for q in [p-1,p+1]:
            if 0<=q<len(lbs):
                x=deepcopy(base); x[fld]=lbs[q]
                x["config_id"]=f"S3_{rank}_{fld}_{lbs[q]}"
                out.append(x)
    return out

def stage3(top,byid):
    out=[]; seen=set()
    for rank,row in enumerate(top[:STAGE3_BASE_KEEP]):
        for x in local_variants(byid[row["config_id"]],rank):
            sig=tuple(str(x.get(k)) for k in [
                "family","br_min","body_atr_min","range_atr_min","close_loc_min",
                "lower_wick_body_min","structure_lb","structure_dist_atr_max",
                "sweep_lb","breakout_lb","compression_max","mom4_max","context","rr"
            ])
            if sig in seen: continue
            seen.add(sig); out.append(x)
    return out

# ---------------- final diagnostics ----------------

def stat_row(c,label,trades):
    s=stats(trades)
    return {
        "config_id":c["config_id"],"family":c["family"],
        "context":c.get("context","NONE"),"rr":c["rr"],"period":label,
        **{k:round(v,6) if isinstance(v,float) else v for k,v in s.items()}
    }

def final_periods(c,candles,ix):
    periods=[
        ("FULL",candles[0]["time"],NOW),
        ("PRE_2010",candles[0]["time"],datetime(2010,1,1,tzinfo=timezone.utc)),
        ("2010_PLUS",datetime(2010,1,1,tzinfo=timezone.utc),NOW),
        *ERAS,
        ("DEV_2002_17",candles[0]["time"],datetime(2018,1,1,tzinfo=timezone.utc)),
        ("VALIDATION_2018_PLUS",datetime(2018,1,1,tzinfo=timezone.utc),NOW),
        ("LAST_5Y",NOW-timedelta(days=365.2425*5),NOW),
        ("LAST_2Y",NOW-timedelta(days=365.2425*2),NOW),
    ]
    out=[]
    for label,a,b in periods:
        r=stat_row(c,label,backtest(candles,ix,c["rr"],PRIMARY_COST,a,b))
        r["start_utc"]=iso(a); r["end_utc"]=iso(b); out.append(r)
    return out

def cost_rows(c,candles,ix):
    out=[]
    for label,a,b in [
        ("FULL",candles[0]["time"],NOW),
        ("PRE_2010",candles[0]["time"],datetime(2010,1,1,tzinfo=timezone.utc)),
        ("2010_PLUS",datetime(2010,1,1,tzinfo=timezone.utc),NOW),
    ]:
        for cost in COSTS:
            r=stat_row(c,label,backtest(candles,ix,c["rr"],cost,a,b))
            r["cost_pips"]=cost; out.append(r)
    return out

def rolling_rows(c,candles,ix):
    out=[]
    first=month_floor(max(candles[0]["time"],START))
    last=month_floor(NOW)
    for months in [12,24,36]:
        s=first
        while add_months(s,months)<=last:
            e=add_months(s,months)
            st=stats(backtest(candles,ix,c["rr"],PRIMARY_COST,s,e))
            out.append({
                "config_id":c["config_id"],"months":months,
                "start_utc":iso(s),"end_utc":iso(e),"trades":st["trades"],
                "profit_factor":round(st["profit_factor"],6),
                "total_r":round(st["total_r"],4),
                "positive":st["total_r"]>0,"zero_trade":st["trades"]==0
            })
            s=add_months(s,1)
    return out

def rolling_summary(rows):
    g=defaultdict(list)
    for r in rows: g[(r["config_id"],r["months"])].append(r)
    out=[]
    for (cid,m),sub in g.items():
        active=[r for r in sub if r["trades"]>0]
        out.append({
            "config_id":cid,"months":m,"windows":len(sub),
            "active_windows":len(active),
            "zero_trade_windows":len(sub)-len(active),
            "positive_windows_pct":round(100*sum(r["positive"] for r in sub)/len(sub),4),
            "positive_active_windows_pct":round(
                100*sum(r["positive"] for r in active)/len(active),4
            ) if active else 0.0,
            "median_r_all":round(med([r["total_r"] for r in sub]),4),
            "median_r_active":round(med([r["total_r"] for r in active]),4),
            "median_pf_active":round(med([r["profit_factor"] for r in active]),6),
            "worst_r":round(min(r["total_r"] for r in sub),4),
            "best_r":round(max(r["total_r"] for r in sub),4),
        })
    return out

def calendar_rows(c,candles,ix):
    out=[]
    for y in range(max(START.year,candles[0]["time"].year),NOW.year):
        a=datetime(y,1,1,tzinfo=timezone.utc)
        b=datetime(y+1,1,1,tzinfo=timezone.utc)
        s=stats(backtest(candles,ix,c["rr"],PRIMARY_COST,a,b))
        out.append({
            "config_id":c["config_id"],"year":y,"trades":s["trades"],
            "profit_factor":round(s["profit_factor"],6),
            "total_r":round(s["total_r"],4),
            "positive":s["total_r"]>0,"negative":s["total_r"]<0,
            "zero_trade":s["trades"]==0,
        })
    return out

def calendar_summary(rows):
    g=defaultdict(list)
    for r in rows: g[r["config_id"]].append(r)
    out=[]
    for cid,sub in g.items():
        active=[r for r in sub if r["trades"]>0]
        out.append({
            "config_id":cid,"completed_years":len(sub),
            "active_years":len(active),
            "positive_years":sum(r["positive"] for r in sub),
            "negative_years":sum(r["negative"] for r in sub),
            "zero_trade_years":sum(r["zero_trade"] for r in sub),
            "positive_years_pct":round(100*sum(r["positive"] for r in sub)/len(sub),4),
            "positive_active_years_pct":round(
                100*sum(r["positive"] for r in active)/len(active),4
            ) if active else 0.0,
            "median_trades_year":round(med([r["trades"] for r in sub]),4),
            "median_year_r":round(med([r["total_r"] for r in sub]),4),
            "worst_year_r":round(min(r["total_r"] for r in sub),4),
            "best_year_r":round(max(r["total_r"] for r in sub),4),
        })
    return out

# ---------------- runner ----------------

def run():
    try:
        m15=fetch("M15",START,NOW,35)
        h1=fetch("H1",WARMUP,NOW,180)
        h4=fetch("H4",WARMUP,NOW,700)
        d=fetch("D",WARMUP,NOW,3500)
        if not all([m15,h1,h4,d]):
            raise RuntimeError("Missing required history")

        write_csv(OUTS["coverage"], [{
            "instrument":PAIR,"requested_start_utc":iso(START),
            "actual_first_m15_utc":iso(m15[0]["time"]),
            "actual_last_m15_utc":iso(m15[-1]["time"]),
            "m15_candles":len(m15),"h1_candles":len(h1),
            "h4_candles":len(h4),"daily_candles":len(d),
        }])

        STATUS.update({"state":"precompute","message":"HTF completion alignment"})
        t=[x["time"] for x in m15]
        ah1=align_htf(t,htf_state(h1))
        ah4=align_htf(t,htf_state(h4))
        ad=align_htf(t,htf_state(d))

        STATUS.update({"state":"precompute","message":"M15 feature cache"})
        f=features(m15,ah1,ah4,ad)

        # benchmark
        bix=indices(BENCH,f)
        brows=final_periods(BENCH,m15,bix)
        parity=len(backtest(
            m15,bix,BENCH["rr"],PRIMARY_COST,
            datetime(2010,1,1,tzinfo=timezone.utc),NOW
        ))
        for r in brows:
            r["locked_2010_reference_trades"]=85
            r["current_2010_plus_trades"]=parity
            r["parity_status"]=(
                "MATCH" if parity==85 else
                "CURRENT_RUN_HAS_NEWER_TRADES" if parity>85 else
                "CHECK_PARITY"
            )
        write_csv(OUTS["benchmark"],brows)

        # stage 1
        s1=stage1_configs()
        s1map={x["config_id"]:x for x in s1}
        s1rows=[]
        for n,c in enumerate(s1,1):
            STATUS.update({"state":"stage1","message":f"{n}/{len(s1)} {c['config_id']}"})
            s1rows.append(evaluate(c,m15,indices(c,f)))
        s1rows=sortrows(s1rows)
        write_csv(OUTS["stage1"],s1rows)
        top1=[r for r in s1rows if r["full_trades"]>=45][:STAGE1_KEEP]
        if len(top1)<STAGE1_KEEP: top1=s1rows[:STAGE1_KEEP]

        # stage 2
        s2=stage2(top1,s1map)
        s2map={x["config_id"]:x for x in s2}
        s2rows=[]
        for n,c in enumerate(s2,1):
            STATUS.update({"state":"stage2","message":f"{n}/{len(s2)} {c['config_id']}"})
            s2rows.append(evaluate(c,m15,indices(c,f)))
        s2rows=sortrows(s2rows)
        write_csv(OUTS["stage2"],s2rows)
        top2=[r for r in s2rows if r["full_trades"]>=45][:STAGE2_KEEP]
        if len(top2)<STAGE2_KEEP: top2=s2rows[:STAGE2_KEEP]

        # stage 3
        s3=stage3(top2,s2map)
        s3map={x["config_id"]:x for x in s3}
        s3rows=[]
        for n,c in enumerate(s3,1):
            STATUS.update({"state":"stage3","message":f"{n}/{len(s3)} {c['config_id']}"})
            s3rows.append(evaluate(c,m15,indices(c,f)))
        s3rows=sortrows(s3rows)
        write_csv(OUTS["stage3"],s3rows)

        eligible=[
            r for r in s3rows
            if r["full_trades"]>=55 and r["pre2010_r"]>0
            and r["post2010_r"]>0 and r["positive_eras"]>=3
        ]
        finalrows=(eligible if eligible else s3rows)[:FINAL_KEEP]
        finals=[s3map[r["config_id"]] for r in finalrows]
        write_csv(OUTS["final"],finalrows)

        periods=[]; costs=[]; rolling=[]; cal=[]; trades=[]
        for n,c in enumerate(finals,1):
            STATUS.update({"state":"final","message":f"{n}/{len(finals)} {c['config_id']}"})
            ix=indices(c,f)
            periods.extend(final_periods(c,m15,ix))
            costs.extend(cost_rows(c,m15,ix))
            rolling.extend(rolling_rows(c,m15,ix))
            cal.extend(calendar_rows(c,m15,ix))
            for t0 in backtest(m15,ix,c["rr"],PRIMARY_COST,m15[0]["time"],NOW):
                z=dict(t0)
                z.update({
                    "config_id":c["config_id"],
                    "family":c["family"],
                    "context":c.get("context","NONE"),
                })
                trades.append(z)

        write_csv(OUTS["periods"],periods)
        write_csv(OUTS["cost"],costs)
        write_csv(OUTS["rolling"],rolling)
        write_csv(OUTS["rolling_summary"],rolling_summary(rolling))
        write_csv(OUTS["calendar"],cal)
        write_csv(OUTS["calendar_summary"],calendar_summary(cal))
        write_csv(OUTS["trades"],trades)

        # top finalist ablation + plateau
        if finals:
            top=finals[0]
            ab=[]
            if top.get("context","NONE")!="NONE":
                x=deepcopy(top); x["config_id"]=top["config_id"]+"_NO_CONTEXT"; x["context"]="NONE"
                r=evaluate(x,m15,indices(x,f)); r["ablation"]="REMOVE_CONTEXT"; ab.append(r)
            for fld in ["br_min","body_atr_min","range_atr_min","close_loc_min","lower_wick_body_min","mom4_max"]:
                if top.get(fld) is None: continue
                x=deepcopy(top); x["config_id"]=top["config_id"]+"_NO_"+fld; x[fld]=None
                try:
                    r=evaluate(x,m15,indices(x,f)); r["ablation"]="REMOVE_"+fld; ab.append(r)
                except Exception:
                    pass
            write_csv(OUTS["ablation"],sortrows(ab))

            plateau=[]
            for x in local_variants(top,99):
                x["rr"]=top["rr"]
                try:
                    plateau.append(evaluate(x,m15,indices(x,f)))
                except Exception:
                    pass
            write_csv(OUTS["plateau"],sortrows(plateau))
        else:
            write_csv(OUTS["ablation"],[])
            write_csv(OUTS["plateau"],[])

        STATUS.update({"state":"packaging","message":"Building ZIP"})
        pack()
        STATUS.update({
            "state":"complete",
            "message":"EUR/USD M15 LONG full-history re-examination complete",
            "stage1_configs":len(s1),"stage2_configs":len(s2),
            "stage3_configs":len(s3),"finalists":len(finals),
            "benchmark_2010_plus_trades":parity,
            "bundle":BUNDLE,
        })

    except Exception as e:
        STATUS.update({"state":"error","message":str(e)})
        print("ERROR:",e,flush=True)

@app.route("/")
def root():
    return jsonify({
        "service":"EURUSD M15 LONG Full-History Re-examination",
        "status":STATUS["state"],
        "instrument":PAIR,"timeframe":"M15","side":"BUY",
        "requested_start_utc":iso(START),
        "primary_cost_pips":PRIMARY_COST,
        "orders_supported":False,"trading_enabled":False,
        "routes":[
            "/eurusd-m15-long-full-history/status",
            "/eurusd-m15-long-full-history/results",
        ],
    })

@app.route("/eurusd-m15-long-full-history/status")
def status():
    return jsonify(STATUS)

@app.route("/eurusd-m15-long-full-history/results")
def results():
    return dl(BUNDLE)

if __name__=="__main__":
    threading.Thread(target=run,daemon=True).start()
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")),debug=False)
