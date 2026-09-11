
import os, csv, time, bisect, zipfile, threading
from copy import deepcopy
from collections import deque, defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import requests
from flask import Flask, jsonify, send_file

# ============================================================
# EUR/JPY M15 LONG — FULL-HISTORY RESEARCH
#
# Fresh research universe:
#   2002-05-06 20:00 UTC -> present / earliest OANDA available
#
# No prior EUR/JPY M15 LONG specification is treated as a benchmark.
# This is a clean full-history search from first principles.
#
# Pair-specific session handling:
#   America/New_York, Europe/London and Asia/Tokyo 4-hour blocks are tested
#   as broad Stage-2 contexts; none is assumed a priori.
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
#   Stage 2 broad HTF/time/weekday contexts (NY + London + Tokyo)
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
#   /eurjpy-m15-long-research/results
#
# READ ONLY. NEVER SENDS ORDERS.
# ============================================================

app = Flask(__name__)

TOKEN = os.getenv("OANDA_TOKEN")
BASE = os.getenv("OANDA_API_URL", "https://api-fxtrade.oanda.com")
PAIR = "EUR_JPY"

START = datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc)
NOW = datetime.now(timezone.utc).replace(second=0, microsecond=0)
WARMUP = START - timedelta(days=900)

NY = ZoneInfo("America/New_York")
LONDON = ZoneInfo("Europe/London")
TOKYO = ZoneInfo("Asia/Tokyo")

TICK = 0.001
PIP = 0.01
STOP_TICKS = 10

PRIMARY_COST = 1.0
COSTS = [0.5, 1.0, 1.5, 2.0]

STAGE1_KEEP = 12
STAGE2_BASE_KEEP = 7
STAGE2_KEEP = 9
STAGE3_BASE_KEEP = 4
FINAL_KEEP = 8

OUTS = {
    "coverage": "eurjpy_m15_long_final_deep_coverage.csv",
    "parity": "eurjpy_m15_long_final_deep_parity.csv",
    "summary": "eurjpy_m15_long_final_deep_summary.csv",
    "periods": "eurjpy_m15_long_final_deep_periods.csv",
    "cost": "eurjpy_m15_long_final_deep_cost_stress.csv",
    "rolling": "eurjpy_m15_long_final_deep_rolling.csv",
    "rolling_summary": "eurjpy_m15_long_final_deep_rolling_summary.csv",
    "calendar": "eurjpy_m15_long_final_deep_calendar_years.csv",
    "calendar_summary": "eurjpy_m15_long_final_deep_calendar_summary.csv",
    "trades": "eurjpy_m15_long_final_deep_trades.csv",
    "overlap": "eurjpy_m15_long_final_deep_overlap.csv",
    "ablation": "eurjpy_m15_long_final_deep_ablation.csv",
    "decision": "eurjpy_m15_long_final_deep_decision_matrix.csv",
    "notes": "eurjpy_m15_long_final_deep_notes.csv",
}
BUNDLE = "EURJPY_M15_LONG_FINAL_DEEP_VALIDATION_RESULTS.zip"

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
    ldh = np.zeros(n,dtype=np.int16)
    ldw = np.zeros(n,dtype=np.int16)
    tkh = np.zeros(n,dtype=np.int16)
    tkw = np.zeros(n,dtype=np.int16)
    for i,t in enumerate(times):
        z=t.astimezone(NY)
        nyh[i],nyw[i]=z.hour,z.weekday()
        q=t.astimezone(LONDON)
        ldh[i],ldw[i]=q.hour,q.weekday()
        r=t.astimezone(TOKYO)
        tkh[i],tkw[i]=r.hour,r.weekday()

    return {
        "n":n,"times":times,"open":o,"high":h,"low":l,"close":cl,
        "atr":a,"valid_atr":valid_atr,"bullish":bullish,
        "exact":exact,"br":br,"body_atr":body_atr,
        "range_atr":range_atr,"close_loc":close_loc,
        "lwb":lwb,"compression":comp,"prev_low":pl,"prev_high":ph,
        "structure_dist":sd,"mom4":mom4,"ny_hour":nyh,"ny_weekday":nyw,
        "ldn_hour":ldh,"ldn_weekday":ldw,
        "tokyo_hour":tkh,"tokyo_weekday":tkw,
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
    "LDN_BLOCK_00-03","LDN_BLOCK_04-07","LDN_BLOCK_08-11",
    "LDN_BLOCK_12-15","LDN_BLOCK_16-19","LDN_BLOCK_20-23",
    "TOKYO_BLOCK_00-03","TOKYO_BLOCK_04-07","TOKYO_BLOCK_08-11",
    "TOKYO_BLOCK_12-15","TOKYO_BLOCK_16-19","TOKYO_BLOCK_20-23",
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
    elif ctx.startswith("LDN_BLOCK_"):
        a,b=map(int,ctx.split("_")[-1].split("-"))
        mask &= (f["ldn_hour"]>=a)&(f["ldn_hour"]<=b)
    elif ctx.startswith("TOKYO_BLOCK_"):
        a,b=map(int,ctx.split("_")[-1].split("-"))
        mask &= (f["tokyo_hour"]>=a)&(f["tokyo_hour"]<=b)
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

def family_summary(rows):
    grouped = defaultdict(list)
    for r in rows:
        grouped[r["family"]].append(r)
    out = []
    for fam, sub in grouped.items():
        ranked = sortrows(sub)
        best = ranked[0]
        positive_full = [r for r in sub if r["full_r"] > 0]
        robust = [r for r in sub if r["pre2010_r"] > 0 and r["post2010_r"] > 0]
        out.append({
            "family": fam,
            "configs": len(sub),
            "positive_full_configs": len(positive_full),
            "positive_pre_and_post_configs": len(robust),
            "best_config_id": best["config_id"],
            "best_full_trades": best["full_trades"],
            "best_full_pf": best["full_pf"],
            "best_full_r": best["full_r"],
            "best_pre2010_r": best["pre2010_r"],
            "best_post2010_r": best["post2010_r"],
            "best_positive_eras": best["positive_eras"],
            "best_robust_score": best["robust_score"],
        })
    return sorted(out, key=lambda r: (
        r["positive_pre_and_post_configs"],
        r["best_positive_eras"],
        r["best_robust_score"],
    ), reverse=True)

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
    for rr in [2.75,3.0,3.25,3.5,3.75,4.0,4.25,4.5,4.75,5.0]:
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


# ============================================================
# FIXED FINAL DEEP-VALIDATION CANDIDATES
# ============================================================
#
# This is NOT another optimisation/search.
#
# It freezes exactly nine contenders selected from the prior full-history
# research:
#
# ENGULF family:
#   E1  BR1.50 / body0.75 / structure100 / dist0.10 / H1 EMA50>EMA200 / RR3.00
#   E2  same / RR4.25
#   E3  same / RR4.50
#   E4  BR1.45 / body0.75 / structure100 / dist0.10 / H1 EMA50>EMA200 / RR4.25
#   E5  BR1.35 / body0.75 / structure100 / dist0.10 / H1 EMA50>EMA200 / RR4.25
#   E6  BR1.50 / body0.75 / structure120 / dist0.10 / H1 EMA50>EMA200 / RR4.25
#
# SWEEP family:
#   S1  sweep60 / body1.00 / lowerwick0.35 / prior4h<=-1.00ATR
#       exclude Tuesday NY / RR4.50
#   S2  same / RR4.75
#   S3  sweep80 / same geometry/context / RR4.75
#
# Purpose:
#   - reproduce known central controls
#   - get proper 0.5/1/1.5/2-pip cost stress
#   - full temporal periods including 2018+, 2020+, last5Y/2Y
#   - true monthly-start rolling 12/24/36M
#   - calendar/no-trade years
#   - H1-trend and Tuesday-filter ablations
#   - exact-entry and holding-interval overlap between contenders
#
# Selection rule:
#   choose robustness/consistency, not maximum PF.
# ============================================================

def fixed_finalists():
    out = []

    def eng(cid, br, lb, rr):
        return cfg(
            cid,
            "ENGULF_STRUCTURE",
            rr=rr,
            br_min=br,
            body_atr_min=0.75,
            structure_lb=lb,
            structure_dist_atr_max=0.10,
            context="H1_EMA50_GT_EMA200",
        )

    def sweep(cid, lb, rr):
        return cfg(
            cid,
            "SWEEP_DISPLACEMENT",
            rr=rr,
            sweep_lb=lb,
            body_atr_min=1.00,
            lower_wick_body_min=0.35,
            mom4_max=-1.00,
            context="EXCLUDE_WEEKDAY_1",
        )

    out.extend([
        eng("E1_ENG_BR150_S100_RR300", 1.50, 100, 3.00),
        eng("E2_ENG_BR150_S100_RR425", 1.50, 100, 4.25),
        eng("E3_ENG_BR150_S100_RR450", 1.50, 100, 4.50),
        eng("E4_ENG_BR145_S100_RR425", 1.45, 100, 4.25),
        eng("E5_ENG_BR135_S100_RR425", 1.35, 100, 4.25),
        eng("E6_ENG_BR150_S120_RR425", 1.50, 120, 4.25),
        sweep("S1_SWEEP60_RR450", 60, 4.50),
        sweep("S2_SWEEP60_RR475", 60, 4.75),
        sweep("S3_SWEEP80_RR475", 80, 4.75),
    ])

    return out


KNOWN_PARITY = {
    # Exact configurations already observed in the broad search.
    "E1_ENG_BR150_S100_RR300": {"trades": 74, "pf": 1.775494, "r": 34.8972},
    "E2_ENG_BR150_S100_RR425": {"trades": 74, "pf": 1.889369, "r": 44.4685},
    "E3_ENG_BR150_S100_RR450": {"trades": 74, "pf": 1.878250, "r": 44.7907},
    "S1_SWEEP60_RR450": {"trades": 94, "pf": 1.451461, "r": 31.6023},
    "S2_SWEEP60_RR475": {"trades": 94, "pf": 1.450955, "r": 32.0178},
}


def deep_periods(c, candles, ix):
    periods = [
        ("FULL", candles[0]["time"], NOW),
        ("PRE_2010", candles[0]["time"], datetime(2010,1,1,tzinfo=timezone.utc)),
        ("2010_PLUS", datetime(2010,1,1,tzinfo=timezone.utc), NOW),
        ("DEV_2002_17", candles[0]["time"], datetime(2018,1,1,tzinfo=timezone.utc)),
        ("VALIDATION_2018_PLUS", datetime(2018,1,1,tzinfo=timezone.utc), NOW),
        ("ERA_2002_07", START, datetime(2008,1,1,tzinfo=timezone.utc)),
        ("ERA_2008_13", datetime(2008,1,1,tzinfo=timezone.utc), datetime(2014,1,1,tzinfo=timezone.utc)),
        ("ERA_2014_19", datetime(2014,1,1,tzinfo=timezone.utc), datetime(2020,1,1,tzinfo=timezone.utc)),
        ("ERA_2020_NOW", datetime(2020,1,1,tzinfo=timezone.utc), NOW),
        ("LAST_5Y", NOW-timedelta(days=365.2425*5), NOW),
        ("LAST_2Y", NOW-timedelta(days=365.2425*2), NOW),
        ("LAST_1Y", NOW-timedelta(days=365.2425), NOW),
    ]

    out = []
    for label, a, b in periods:
        r = stat_row(c, label, backtest(candles, ix, c["rr"], PRIMARY_COST, a, b))
        r["start_utc"] = iso(a)
        r["end_utc"] = iso(b)
        out.append(r)
    return out


def deep_summary(c, candles, ix):
    full = stats(backtest(candles, ix, c["rr"], PRIMARY_COST, candles[0]["time"], NOW))
    pre = stats(backtest(candles, ix, c["rr"], PRIMARY_COST, candles[0]["time"],
                         datetime(2010,1,1,tzinfo=timezone.utc)))
    post = stats(backtest(candles, ix, c["rr"], PRIMARY_COST,
                          datetime(2010,1,1,tzinfo=timezone.utc), NOW))
    dev = stats(backtest(candles, ix, c["rr"], PRIMARY_COST, candles[0]["time"],
                         datetime(2018,1,1,tzinfo=timezone.utc)))
    val = stats(backtest(candles, ix, c["rr"], PRIMARY_COST,
                         datetime(2018,1,1,tzinfo=timezone.utc), NOW))
    y20 = stats(backtest(candles, ix, c["rr"], PRIMARY_COST,
                         datetime(2020,1,1,tzinfo=timezone.utc), NOW))
    l5 = stats(backtest(candles, ix, c["rr"], PRIMARY_COST,
                        NOW-timedelta(days=365.2425*5), NOW))
    l2 = stats(backtest(candles, ix, c["rr"], PRIMARY_COST,
                        NOW-timedelta(days=365.2425*2), NOW))

    era_stats = []
    for _, a, b in ERAS:
        era_stats.append(stats(backtest(candles, ix, c["rr"], PRIMARY_COST, a, b)))

    return {
        "config_id": c["config_id"],
        "family": c["family"],
        "context": c.get("context","NONE"),
        "rr": c["rr"],
        **cfields(c),
        "full_trades": full["trades"],
        "full_pf": round(full["profit_factor"],6),
        "full_r": round(full["total_r"],4),
        "full_exp": round(full["expectancy_r"],6),
        "full_dd": round(full["max_drawdown_r"],4),
        "full_win_rate": round(full["win_rate"],4),
        "pre2010_trades": pre["trades"],
        "pre2010_pf": round(pre["profit_factor"],6),
        "pre2010_r": round(pre["total_r"],4),
        "post2010_trades": post["trades"],
        "post2010_pf": round(post["profit_factor"],6),
        "post2010_r": round(post["total_r"],4),
        "dev2002_17_pf": round(dev["profit_factor"],6),
        "dev2002_17_r": round(dev["total_r"],4),
        "validation2018_plus_trades": val["trades"],
        "validation2018_plus_pf": round(val["profit_factor"],6),
        "validation2018_plus_r": round(val["total_r"],4),
        "era2020_plus_trades": y20["trades"],
        "era2020_plus_pf": round(y20["profit_factor"],6),
        "era2020_plus_r": round(y20["total_r"],4),
        "last5y_trades": l5["trades"],
        "last5y_pf": round(l5["profit_factor"],6),
        "last5y_r": round(l5["total_r"],4),
        "last2y_trades": l2["trades"],
        "last2y_pf": round(l2["profit_factor"],6),
        "last2y_r": round(l2["total_r"],4),
        "positive_eras": sum(x["trades"] > 0 and x["total_r"] > 0 for x in era_stats),
        "min_era_pf": round(min(x["profit_factor"] for x in era_stats),6),
    }


def deep_cost_rows(c, candles, ix):
    rows = []
    for cost in COSTS:
        for label, a, b in [
            ("FULL", candles[0]["time"], NOW),
            ("VALIDATION_2018_PLUS", datetime(2018,1,1,tzinfo=timezone.utc), NOW),
            ("LAST_5Y", NOW-timedelta(days=365.2425*5), NOW),
            ("LAST_2Y", NOW-timedelta(days=365.2425*2), NOW),
        ]:
            r = stat_row(c, label, backtest(candles, ix, c["rr"], cost, a, b))
            r["cost_pips"] = cost
            rows.append(r)
    return rows


def serialise_trade(c, t):
    row = dict(t)
    row.update({
        "config_id": c["config_id"],
        "family": c["family"],
        "context": c.get("context","NONE"),
    })
    return row


def interval_overlap_count(a, b):
    """
    Count trades in A whose [entry,exit) interval overlaps >=1 trade in B.
    Diagnostic only; does not imply the trades could coexist under p0.
    """
    if not a or not b:
        return 0

    b_sorted = sorted(b, key=lambda x: x["entry_time"])
    starts = [x["entry_time"] for x in b_sorted]
    count = 0

    for x in a:
        i = bisect.bisect_left(starts, x["exit_time"])
        overlap = False
        for y in b_sorted[max(0, i-4):i+1]:
            if y["entry_time"] < x["exit_time"] and x["entry_time"] < y["exit_time"]:
                overlap = True
                break
        if overlap:
            count += 1

    return count


def overlap_rows(trade_map):
    rows = []
    ids = list(trade_map)

    for i, a_id in enumerate(ids):
        a = trade_map[a_id]
        a_times = {x["entry_time"] for x in a}

        for b_id in ids[i+1:]:
            b = trade_map[b_id]
            b_times = {x["entry_time"] for x in b}

            shared = a_times & b_times
            a_interval = interval_overlap_count(a, b)
            b_interval = interval_overlap_count(b, a)

            rows.append({
                "config_a": a_id,
                "config_b": b_id,
                "a_trades": len(a),
                "b_trades": len(b),
                "exact_same_entry": len(shared),
                "exact_same_entry_pct_a": round(100*len(shared)/len(a),4) if a else 0.0,
                "exact_same_entry_pct_b": round(100*len(shared)/len(b),4) if b else 0.0,
                "a_trades_with_any_interval_overlap_b": a_interval,
                "a_interval_overlap_pct": round(100*a_interval/len(a),4) if a else 0.0,
                "b_trades_with_any_interval_overlap_a": b_interval,
                "b_interval_overlap_pct": round(100*b_interval/len(b),4) if b else 0.0,
            })

    return rows


def ablation_rows(candidates, candles, f):
    rows = []

    # Central engulf: remove the H1 trend regime only.
    core = next(x for x in candidates if x["config_id"] == "E2_ENG_BR150_S100_RR425")
    x = deepcopy(core)
    x["config_id"] = "ABLATE_ENG_REMOVE_H1_EMA50_GT_EMA200"
    x["context"] = "NONE"
    r = deep_summary(x, candles, indices(x,f))
    r["ablation"] = "REMOVE_H1_EMA50_GT_EMA200"
    r["parent_config"] = core["config_id"]
    rows.append(r)

    # Sweep: remove Tuesday exclusion only.
    core = next(x for x in candidates if x["config_id"] == "S2_SWEEP60_RR475")
    x = deepcopy(core)
    x["config_id"] = "ABLATE_SWEEP_REMOVE_EXCLUDE_TUESDAY"
    x["context"] = "NONE"
    r = deep_summary(x, candles, indices(x,f))
    r["ablation"] = "REMOVE_EXCLUDE_TUESDAY"
    r["parent_config"] = core["config_id"]
    rows.append(r)

    return rows


def decision_rows(summary, costs, rollsum, calsum, parity_rows):
    cost2 = {
        r["config_id"]: r
        for r in costs
        if r["period"] == "FULL" and abs(float(r["cost_pips"]) - 2.0) < 1e-9
    }
    rolls = {
        (r["config_id"], int(r["months"])): r
        for r in rollsum
    }
    cals = {r["config_id"]: r for r in calsum}
    parity = {r["config_id"]: r for r in parity_rows}

    out = []

    for r in summary:
        cid = r["config_id"]
        r12 = rolls.get((cid,12),{})
        r24 = rolls.get((cid,24),{})
        r36 = rolls.get((cid,36),{})
        cal = cals.get(cid,{})
        c2 = cost2.get(cid,{})

        out.append({
            "config_id": cid,
            "family": r["family"],
            "rr": r["rr"],
            "full_trades": r["full_trades"],
            "full_pf": r["full_pf"],
            "full_r": r["full_r"],
            "full_exp": r["full_exp"],
            "full_dd": r["full_dd"],
            "validation2018_plus_pf": r["validation2018_plus_pf"],
            "validation2018_plus_r": r["validation2018_plus_r"],
            "era2020_plus_pf": r["era2020_plus_pf"],
            "era2020_plus_r": r["era2020_plus_r"],
            "last5y_pf": r["last5y_pf"],
            "last5y_r": r["last5y_r"],
            "last2y_pf": r["last2y_pf"],
            "last2y_r": r["last2y_r"],
            "cost_2pip_pf": c2.get("profit_factor",0.0),
            "cost_2pip_r": c2.get("total_r",0.0),
            "rolling12_positive_active_pct": r12.get("positive_active_windows_pct",0.0),
            "rolling12_median_r": r12.get("median_r_active",0.0),
            "rolling12_worst_r": r12.get("worst_r",0.0),
            "rolling24_positive_active_pct": r24.get("positive_active_windows_pct",0.0),
            "rolling24_median_r": r24.get("median_r_active",0.0),
            "rolling24_worst_r": r24.get("worst_r",0.0),
            "rolling36_positive_active_pct": r36.get("positive_active_windows_pct",0.0),
            "rolling36_median_r": r36.get("median_r_active",0.0),
            "rolling36_worst_r": r36.get("worst_r",0.0),
            "active_calendar_years": cal.get("active_years",0),
            "positive_active_years_pct": cal.get("positive_active_years_pct",0.0),
            "zero_trade_years": cal.get("zero_trade_years",0),
            "worst_calendar_year_r": cal.get("worst_year_r",0.0),
            "parity_status": parity.get(cid,{}).get("status","NEW_COMBINATION"),
        })

    return out


# ============================================================
# FINAL DEEP-VALIDATION RUNNER
# ============================================================

def run_final_deep_validation():
    try:
        STATUS.update({
            "state":"fetch",
            "message":"Fetching EUR/JPY M15 + strict completed HTF history",
        })

        m15 = fetch("M15", START, NOW, 35)
        h1 = fetch("H1", WARMUP, NOW, 180)
        h4 = fetch("H4", WARMUP, NOW, 700)
        d = fetch("D", WARMUP, NOW, 3500)

        if not all([m15,h1,h4,d]):
            raise RuntimeError("Missing required history")

        write_csv(OUTS["coverage"], [{
            "instrument": PAIR,
            "requested_start_utc": iso(START),
            "actual_first_m15_utc": iso(m15[0]["time"]),
            "actual_last_m15_utc": iso(m15[-1]["time"]),
            "m15_candles": len(m15),
            "h1_candles": len(h1),
            "h4_candles": len(h4),
            "daily_candles": len(d),
            "baseline_cost_pips": PRIMARY_COST,
        }])

        STATUS.update({
            "state":"precompute",
            "message":"Strict HTF completion alignment + M15 feature cache",
        })

        times = [x["time"] for x in m15]
        ah1 = align_htf(times, htf_state(h1))
        ah4 = align_htf(times, htf_state(h4))
        ad = align_htf(times, htf_state(d))
        f = features(m15, ah1, ah4, ad)

        candidates = fixed_finalists()

        summary = []
        periods = []
        costs = []
        rolling = []
        calendar = []
        trade_rows = []
        trade_map = {}
        parity_rows = []

        for n, c in enumerate(candidates, 1):
            STATUS.update({
                "state":"deep_validation",
                "message":f"{n}/{len(candidates)} {c['config_id']}",
            })

            ix = indices(c, f)
            trades = backtest(
                m15, ix, c["rr"], PRIMARY_COST,
                m15[0]["time"], NOW
            )
            trade_map[c["config_id"]] = trades

            s = deep_summary(c, m15, ix)
            summary.append(s)
            periods.extend(deep_periods(c, m15, ix))
            costs.extend(deep_cost_rows(c, m15, ix))
            rolling.extend(rolling_rows(c, m15, ix))
            calendar.extend(calendar_rows(c, m15, ix))

            for t0 in trades:
                trade_rows.append(serialise_trade(c, t0))

            ref = KNOWN_PARITY.get(c["config_id"])
            if ref:
                pf_diff = abs(s["full_pf"] - ref["pf"])
                r_diff = abs(s["full_r"] - ref["r"])
                status = (
                    "PASS"
                    if (
                        s["full_trades"] == ref["trades"]
                        and pf_diff <= 0.00002
                        and r_diff <= 0.01
                    )
                    else "FAIL"
                )
                parity_rows.append({
                    "config_id": c["config_id"],
                    "reference_trades": ref["trades"],
                    "current_trades": s["full_trades"],
                    "reference_pf": ref["pf"],
                    "current_pf": s["full_pf"],
                    "pf_abs_diff": round(pf_diff,8),
                    "reference_r": ref["r"],
                    "current_r": s["full_r"],
                    "r_abs_diff": round(r_diff,6),
                    "status": status,
                })

                if status != "PASS":
                    raise RuntimeError(
                        f"Parity failure for {c['config_id']}: "
                        f"{parity_rows[-1]}"
                    )
            else:
                parity_rows.append({
                    "config_id": c["config_id"],
                    "reference_trades": "",
                    "current_trades": s["full_trades"],
                    "reference_pf": "",
                    "current_pf": s["full_pf"],
                    "pf_abs_diff": "",
                    "reference_r": "",
                    "current_r": s["full_r"],
                    "r_abs_diff": "",
                    "status": "NEW_COMBINATION",
                })

        rollsum = rolling_summary(rolling)
        calsum = calendar_summary(calendar)
        overlap = overlap_rows(trade_map)
        ablation = ablation_rows(candidates, m15, f)
        decision = decision_rows(
            summary, costs, rollsum, calsum, parity_rows
        )

        write_csv(OUTS["parity"], parity_rows)
        write_csv(OUTS["summary"], summary)
        write_csv(OUTS["periods"], periods)
        write_csv(OUTS["cost"], costs)
        write_csv(OUTS["rolling"], rolling)
        write_csv(OUTS["rolling_summary"], rollsum)
        write_csv(OUTS["calendar"], calendar)
        write_csv(OUTS["calendar_summary"], calsum)
        write_csv(OUTS["trades"], trade_rows)
        write_csv(OUTS["overlap"], overlap)
        write_csv(OUTS["ablation"], ablation)
        write_csv(OUTS["decision"], decision)

        write_csv(OUTS["notes"], [
            {
                "item":"Scope",
                "value":"Fixed final deep validation only. No parameter search or automatic finalist selection."
            },
            {
                "item":"Engulf family",
                "value":"Exact bullish engulf; body>=0.75 ATR14; absolute distance to previous structure low; previous strictly completed H1 EMA50>EMA200."
            },
            {
                "item":"Sweep family",
                "value":"Bullish displacement after sweeping prior low; close>previous M15 high; body>=1.00 ATR14; lower wick/body>=0.35; prior 4h momentum<=-1.00 ATR; exclude Tuesday America/New_York."
            },
            {
                "item":"Historical execution",
                "value":"OANDA midpoint; signal timestamp M15 open; reference entry signal close; fill=close+1 pip baseline; stop=signal low-10 ticks; target from reference-close risk; pyramiding 0; exact exit-candle signal eligible."
            },
            {
                "item":"Cost stress",
                "value":"0.5 / 1.0 / 1.5 / 2.0 pip adverse entry, including recent-period diagnostics."
            },
            {
                "item":"HTF causality",
                "value":"H1/H4/D state becomes usable only at next actual HTF candle open; lookup uses bisect_right(completion_times, signal_time)-1."
            },
            {
                "item":"Overlap",
                "value":"Exact-entry overlap and holding-interval overlap are diagnostics to assess whether sweep displacement is genuinely complementary to engulf."
            },
            {
                "item":"Selection",
                "value":"Do not choose maximum PF mechanically. Prefer broad RR/geometry stability, recent-period survival, 2-pip survival, stronger rolling 24/36M consistency, fewer no-trade years and manageable drawdown."
            },
            {
                "item":"Next gate",
                "value":"Only after one M15 LONG candidate is frozen should it be added to the current 22-strategy portfolio under exact non-hedging behaviour."
            },
        ])

        STATUS.update({
            "state":"packaging",
            "message":"Packaging final EUR/JPY M15 LONG deep-validation results",
        })

        pack()

        STATUS.update({
            "state":"complete",
            "message":"EUR/JPY M15 LONG final deep validation complete",
            "candidates": len(candidates),
            "known_parity_controls": len(KNOWN_PARITY),
            "bundle": BUNDLE,
        })

    except Exception as e:
        STATUS.update({
            "state":"error",
            "message":str(e),
        })
        print("ERROR:", repr(e), flush=True)


@app.route("/")
def root():
    return jsonify({
        "service":"EURJPY M15 LONG Final Deep Validation",
        "status":STATUS["state"],
        "instrument":PAIR,
        "timeframe":"M15",
        "side":"BUY",
        "candidates":[x["config_id"] for x in fixed_finalists()],
        "primary_cost_pips":PRIMARY_COST,
        "orders_supported":False,
        "trading_enabled":False,
        "routes":[
            "/eurjpy-m15-long-final/status",
            "/eurjpy-m15-long-final/results",
        ],
    })


@app.route("/eurjpy-m15-long-final/status")
def final_status():
    return jsonify(STATUS)


@app.route("/eurjpy-m15-long-final/results")
def final_results():
    return dl(BUNDLE)


if __name__=="__main__":
    threading.Thread(
        target=run_final_deep_validation,
        daemon=True,
    ).start()

    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT","5000")),
        debug=False,
    )
