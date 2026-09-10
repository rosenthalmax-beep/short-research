
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
# EUR/GBP M15 LONG — FINAL SWEEP-DISPLACEMENT CONFIRMATION
#
# Purpose:
#   Final focused confirmation after the full-history broad search.
#   Five of six broad archetype families failed; only
#   SWEEP_DISPLACEMENT is re-opened here.
#
# Frozen research mechanics inherited from the broad run:
#   OANDA midpoint
#   2002-05-06 20:00 UTC -> present / earliest OANDA available
#   ATR14 Wilder/RMA SMA-seeded
#   signal timestamp = M15 candle OPEN
#   long fill = signal close + adverse cost
#   primary adverse cost = 1 pip
#   stop = signal low - 10 ticks
#   target based on REFERENCE signal-close risk
#   exits tested from next candle
#   pyramiding 0
#   exact exit-candle signal eligible
#   same-bar LONG tie: high closer to open => TARGET else STOP
#
# Sweep-displacement trigger:
#   current M15 candle bullish
#   current low < previous N-bar low (current excluded)
#   current close > previous M15 high
#   body >= body_atr_min * ATR14
#   lower wick >= wick_ratio * bullish body
#   prior 4h momentum <= mom4_max * ATR14
#
# Focused geometry:
#   sweep lookback: 20 / 40 / 60 / 80 / 100
#   body ATR:      1.10 / 1.15 / 1.20 / 1.25 / 1.30 / 1.35
#   lower wick:    0.20 / 0.25 / 0.30 body
#   prior 4h mom: -1.00 / -1.25 / -1.50 ATR
#
# Context comparison:
#   NY 00-03 anchor
#   adjacent NY 23-03 / 00-04 / 01-03 / 23-04
#   exclude Friday
#   previous completed H1 EMA50 > EMA200
#   no-context ablation
#
# Clean RR confirmation:
#   2.75 / 3.00 / 3.25 / 3.50 / 3.75 / 4.00 / 4.25
#
# Four hard parity guards through 2026-09-10 09:49 UTC:
#   NY00-03 LB40 body1.15 wick0.25 mom<=-1.25 RR3.50 => 72 trades
#   NY00-03 LB60 body1.15 wick0.25 mom<=-1.25 RR3.50 => 64 trades
#   ex-Friday LB40 body1.35 wick0.25 mom<=-1.25 RR3.50 => 96 trades
#   H1 EMA50>EMA200 LB40 body1.35 wick0.25 mom<=-1.25 RR3.50 => 57 trades
#
# Final diagnostics:
#   pre-2010 / 2010+
#   2002-07 / 2008-13 / 2014-19 / 2020+
#   2002-17 / 2018+
#   last 5Y / last 2Y
#   0.5 / 1.0 / 1.5 / 2.0 pip cost stress
#   rolling 12 / 24 / 36 months
#   calendar-year consistency
#   finalist trades
#
# ONE ZIP:
#   /eurgbp-m15-long-final-confirmation/results
#
# READ ONLY. NEVER SENDS ORDERS.
# ============================================================

app = Flask(__name__)

TOKEN = os.getenv("OANDA_TOKEN")
BASE = os.getenv("OANDA_API_URL", "https://api-fxtrade.oanda.com")
PAIR = "EUR_GBP"

START = datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc)
NOW = datetime.now(timezone.utc).replace(second=0, microsecond=0)
WARMUP = START - timedelta(days=900)
PARITY_CUTOFF = datetime(2026, 9, 10, 9, 49, tzinfo=timezone.utc)

NY = ZoneInfo("America/New_York")
LONDON = ZoneInfo("Europe/London")

TICK = 0.00001
PIP = 0.0001
STOP_TICKS = 10

PRIMARY_COST = 1.0
COSTS = [0.5, 1.0, 1.5, 2.0]
RR_VALUES = [2.75, 3.00, 3.25, 3.50, 3.75, 4.00, 4.25]

STAGE1_KEEP = 16
STAGE2_KEEP = 16
FINAL_KEEP = 12

OUTS = {
    "coverage": "eurgbp_m15_long_final_confirmation_coverage.csv",
    "parity": "eurgbp_m15_long_final_confirmation_parity.csv",
    "stage1": "eurgbp_m15_long_final_confirmation_stage1_geometry.csv",
    "stage2": "eurgbp_m15_long_final_confirmation_stage2_context.csv",
    "stage3": "eurgbp_m15_long_final_confirmation_stage3_rr.csv",
    "final": "eurgbp_m15_long_final_confirmation_finalists.csv",
    "periods": "eurgbp_m15_long_final_confirmation_periods.csv",
    "cost": "eurgbp_m15_long_final_confirmation_cost_stress.csv",
    "rolling": "eurgbp_m15_long_final_confirmation_rolling.csv",
    "rolling_summary": "eurgbp_m15_long_final_confirmation_rolling_summary.csv",
    "calendar": "eurgbp_m15_long_final_confirmation_calendar_years.csv",
    "calendar_summary": "eurgbp_m15_long_final_confirmation_calendar_summary.csv",
    "trades": "eurgbp_m15_long_final_confirmation_finalist_trades.csv",
    "notes": "eurgbp_m15_long_final_confirmation_notes.csv",
}
BUNDLE = "EURGBP_M15_LONG_FINAL_CONFIRMATION_RESULTS.zip"

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
    for i,t in enumerate(times):
        z=t.astimezone(NY)
        nyh[i],nyw[i]=z.hour,z.weekday()
        q=t.astimezone(LONDON)
        ldh[i],ldw[i]=q.hour,q.weekday()

    return {
        "n":n,"times":times,"open":o,"high":h,"low":l,"close":cl,
        "atr":a,"valid_atr":valid_atr,"bullish":bullish,
        "exact":exact,"br":br,"body_atr":body_atr,
        "range_atr":range_atr,"close_loc":close_loc,
        "lwb":lwb,"compression":comp,"prev_low":pl,"prev_high":ph,
        "structure_dist":sd,"mom4":mom4,"ny_hour":nyh,"ny_weekday":nyw,
        "ldn_hour":ldh,"ldn_weekday":ldw,
        "h1_close":h1["close"],"h1_ema50":h1["ema50"],
        "h1_ema100":h1["ema100"],"h1_ema200":h1["ema200"],
        "h1_atr":h1["atr_ratio50"],
        "h4_close":h4["close"],"h4_ema100":h4["ema100"],
        "h4_ema200":h4["ema200"],"h4_atr":h4["atr_ratio50"],
        "d_close":d["close"],"d_ema50":d["ema50"],
        "d_ema200":d["ema200"],"d_atr":d["atr_ratio50"],
    }

# ---------------- focused configs ----------------

def cfg(cid, fam="SWEEP_DISPLACEMENT", rr=3.5, **kw):
    x = {
        "config_id": cid,
        "family": fam,
        "rr": rr,
        "context": "NY_BLOCK_00-03",
        "br_min": None,
        "body_atr_min": None,
        "range_atr_min": None,
        "close_loc_min": None,
        "lower_wick_body_min": None,
        "structure_lb": None,
        "structure_dist_atr_max": None,
        "sweep_lb": None,
        "breakout_lb": None,
        "compression_max": None,
        "mom4_max": None,
        "excluded_weekdays": set(),
        "excluded_ny_hours": set(),
    }
    x.update(kw)
    return x


def focused_geometry_configs():
    out = []
    counter = 0
    for sweep_lb in [20, 40, 60, 80, 100]:
        for body in [1.10, 1.15, 1.20, 1.25, 1.30, 1.35]:
            for wick in [0.20, 0.25, 0.30]:
                for mom in [-1.00, -1.25, -1.50]:
                    counter += 1
                    out.append(cfg(
                        f"GEO_{counter:04d}",
                        sweep_lb=sweep_lb,
                        body_atr_min=body,
                        lower_wick_body_min=wick,
                        mom4_max=mom,
                        context="NY_BLOCK_00-03",
                        rr=3.50,
                    ))
    return out


CONTEXTS = [
    "NY_BLOCK_00-03",
    "NY_WINDOW_23-03",
    "NY_WINDOW_00-04",
    "NY_WINDOW_01-03",
    "NY_WINDOW_23-04",
    "EXCLUDE_WEEKDAY_4",
    "H1_EMA50_GT_EMA200",
    "NONE",
]


def _hour_in_window(hours, start_hour, end_hour):
    if start_hour <= end_hour:
        return (hours >= start_hour) & (hours <= end_hour)
    return (hours >= start_hour) | (hours <= end_hour)


def context_mask(mask, c, f):
    ctx = c.get("context", "NONE")

    if ctx == "H1_EMA50_GT_EMA200":
        mask &= f["h1_ema50"] > f["h1_ema200"]

    elif ctx == "NY_BLOCK_00-03":
        mask &= (f["ny_hour"] >= 0) & (f["ny_hour"] <= 3)

    elif ctx.startswith("NY_WINDOW_"):
        a, b = map(int, ctx.split("_")[-1].split("-"))
        mask &= _hour_in_window(f["ny_hour"], a, b)

    elif ctx.startswith("EXCLUDE_WEEKDAY_"):
        w = int(ctx.split("_")[-1])
        mask &= f["ny_weekday"] != w

    elif ctx != "NONE":
        raise ValueError(f"Unsupported focused context: {ctx}")

    for w in c.get("excluded_weekdays", set()):
        mask &= f["ny_weekday"] != w
    for h in c.get("excluded_ny_hours", set()):
        mask &= f["ny_hour"] != h
    return mask


def indices(c, f):
    m = f["valid_atr"].copy() & f["bullish"]

    lb = c["sweep_lb"]
    m &= f["low"] < f["prev_low"][lb]

    previous_high = np.roll(f["high"], 1)
    m[0] = False
    m &= f["close"] > previous_high

    m &= f["body_atr"] >= c["body_atr_min"]
    m &= f["lwb"] >= c["lower_wick_body_min"]
    m &= f["mom4"] <= c["mom4_max"]

    m = context_mask(m, c, f)
    m[:200] = False
    return np.flatnonzero(m).tolist()


# ---------------- focused stage 2 / 3 ----------------

def geometry_key(c):
    return (
        int(c["sweep_lb"]),
        round(float(c["body_atr_min"]), 4),
        round(float(c["lower_wick_body_min"]), 4),
        round(float(c["mom4_max"]), 4),
    )


def comparator_seeds():
    """Known strong geometries from the broad-search ZIP.

    These are injected into Stage 2 even if the NY-anchor geometry ranking
    would otherwise omit them. This protects the ex-Friday and H1-trend
    branches from being lost simply because Stage 1 is anchored to NY00-03.
    """
    return [
        cfg(
            "SEED_NY40_BODY115",
            sweep_lb=40,
            body_atr_min=1.15,
            lower_wick_body_min=0.25,
            mom4_max=-1.25,
            context="NY_BLOCK_00-03",
            rr=3.50,
        ),
        cfg(
            "SEED_NY60_BODY115",
            sweep_lb=60,
            body_atr_min=1.15,
            lower_wick_body_min=0.25,
            mom4_max=-1.25,
            context="NY_BLOCK_00-03",
            rr=3.50,
        ),
        cfg(
            "SEED_BODY135",
            sweep_lb=40,
            body_atr_min=1.35,
            lower_wick_body_min=0.25,
            mom4_max=-1.25,
            context="NY_BLOCK_00-03",
            rr=3.50,
        ),
    ]


def stage2_context_configs(top_rows, geometry_by_id):
    bases = []
    seen = set()

    for row in top_rows[:STAGE1_KEEP]:
        base = deepcopy(geometry_by_id[row["config_id"]])
        key = geometry_key(base)
        if key not in seen:
            seen.add(key)
            bases.append(base)

    for seed in comparator_seeds():
        key = geometry_key(seed)
        if key not in seen:
            seen.add(key)
            bases.append(seed)

    out = []
    for rank, base in enumerate(bases):
        for ctx in CONTEXTS:
            x = deepcopy(base)
            x["config_id"] = f"CTX_{rank:02d}_{ctx}"
            x["context"] = ctx
            x["rr"] = 3.50
            out.append(x)
    return out


def stage3_rr_configs(top_rows, context_by_id):
    out = []
    seen = set()
    for rank, row in enumerate(top_rows[:STAGE2_KEEP]):
        base = context_by_id[row["config_id"]]
        for rr in RR_VALUES:
            x = deepcopy(base)
            x["rr"] = rr
            x["config_id"] = f"RR_{rank:02d}_{rr:.2f}"
            sig = (
                geometry_key(x),
                x["context"],
                round(rr, 4),
            )
            if sig in seen:
                continue
            seen.add(sig)
            out.append(x)
    return out


# ---------------- hard parity comparators ----------------

def parity_configs():
    return [
        (
            "ANCHOR_NY40_BODY115",
            cfg(
                "ANCHOR_NY40_BODY115",
                sweep_lb=40,
                body_atr_min=1.15,
                lower_wick_body_min=0.25,
                mom4_max=-1.25,
                context="NY_BLOCK_00-03",
                rr=3.50,
            ),
            72,
        ),
        (
            "ANCHOR_NY60_BODY115",
            cfg(
                "ANCHOR_NY60_BODY115",
                sweep_lb=60,
                body_atr_min=1.15,
                lower_wick_body_min=0.25,
                mom4_max=-1.25,
                context="NY_BLOCK_00-03",
                rr=3.50,
            ),
            64,
        ),
        (
            "ANCHOR_EXFRI_BODY135",
            cfg(
                "ANCHOR_EXFRI_BODY135",
                sweep_lb=40,
                body_atr_min=1.35,
                lower_wick_body_min=0.25,
                mom4_max=-1.25,
                context="EXCLUDE_WEEKDAY_4",
                rr=3.50,
            ),
            96,
        ),
        (
            "ANCHOR_H1_BODY135",
            cfg(
                "ANCHOR_H1_BODY135",
                sweep_lb=40,
                body_atr_min=1.35,
                lower_wick_body_min=0.25,
                mom4_max=-1.25,
                context="H1_EMA50_GT_EMA200",
                rr=3.50,
            ),
            57,
        ),
    ]

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

# ---------------- runner ----------------

def run():
    try:
        STATUS.update({"state":"fetching","message":"Fetching EUR/GBP history"})
        m15=fetch("M15",START,NOW,35)
        h1=fetch("H1",WARMUP,NOW,180)
        h4=fetch("H4",WARMUP,NOW,700)
        d=fetch("D",WARMUP,NOW,3000)
        if not all([m15,h1,h4,d]):
            raise RuntimeError("Missing required EUR/GBP history")

        write_csv(OUTS["coverage"], [{
            "instrument":PAIR,
            "requested_start_utc":iso(START),
            "parity_cutoff_utc":iso(PARITY_CUTOFF),
            "actual_first_m15_utc":iso(m15[0]["time"]),
            "actual_last_m15_utc":iso(m15[-1]["time"]),
            "m15_candles":len(m15),
            "h1_candles":len(h1),
            "h4_candles":len(h4),
            "daily_candles":len(d),
        }])

        STATUS.update({"state":"precompute","message":"HTF completion alignment"})
        t=[x["time"] for x in m15]
        ah1=align_htf(t,htf_state(h1))
        ah4=align_htf(t,htf_state(h4))
        ad=align_htf(t,htf_state(d))

        STATUS.update({"state":"precompute","message":"M15 feature cache"})
        f=features(m15,ah1,ah4,ad)

        # ----------------------------------------------------
        # HARD PARITY
        # ----------------------------------------------------
        parity_rows=[]
        for label,c,expected in parity_configs():
            tr=backtest(
                m15,
                indices(c,f),
                c["rr"],
                PRIMARY_COST,
                m15[0]["time"],
                PARITY_CUTOFF,
            )
            actual=len(tr)
            parity_rows.append({
                "parity_id":label,
                "expected_trades":expected,
                "actual_trades":actual,
                "status":"MATCH" if actual==expected else "FAIL",
                "sweep_lb":c["sweep_lb"],
                "body_atr_min":c["body_atr_min"],
                "lower_wick_body_min":c["lower_wick_body_min"],
                "mom4_max":c["mom4_max"],
                "context":c["context"],
                "rr":c["rr"],
            })
            if actual != expected:
                write_csv(OUTS["parity"],parity_rows)
                raise RuntimeError(
                    f"Parity failed for {label}: expected {expected}, got {actual}"
                )
        write_csv(OUTS["parity"],parity_rows)

        # ----------------------------------------------------
        # STAGE 1 — focused geometry under frozen NY00-03 anchor
        # ----------------------------------------------------
        s1=focused_geometry_configs()
        s1map={x["config_id"]:x for x in s1}
        s1rows=[]
        for n,c in enumerate(s1,1):
            STATUS.update({
                "state":"stage1_geometry",
                "message":f"{n}/{len(s1)} {c['config_id']}"
            })
            s1rows.append(evaluate(c,m15,indices(c,f)))
        s1rows=sortrows(s1rows)
        write_csv(OUTS["stage1"],s1rows)

        eligible1=[
            r for r in s1rows
            if r["full_trades"]>=40
            and r["pre2010_r"]>0
            and r["post2010_r"]>0
            and r["positive_eras"]>=3
        ]
        top1=(eligible1 if eligible1 else s1rows)[:STAGE1_KEEP]

        # ----------------------------------------------------
        # STAGE 2 — context comparison on the strongest geometries
        # ----------------------------------------------------
        s2=stage2_context_configs(top1,s1map)
        s2map={x["config_id"]:x for x in s2}
        s2rows=[]
        for n,c in enumerate(s2,1):
            STATUS.update({
                "state":"stage2_context",
                "message":f"{n}/{len(s2)} {c['config_id']}"
            })
            s2rows.append(evaluate(c,m15,indices(c,f)))
        s2rows=sortrows(s2rows)
        write_csv(OUTS["stage2"],s2rows)

        eligible2=[
            r for r in s2rows
            if r["full_trades"]>=40
            and r["pre2010_r"]>0
            and r["post2010_r"]>0
            and r["positive_eras"]>=3
        ]
        top2=(eligible2 if eligible2 else s2rows)[:STAGE2_KEEP]

        # ----------------------------------------------------
        # STAGE 3 — clean RR sweep on actual improved geometries
        # ----------------------------------------------------
        s3=stage3_rr_configs(top2,s2map)
        s3map={x["config_id"]:x for x in s3}
        s3rows=[]
        for n,c in enumerate(s3,1):
            STATUS.update({
                "state":"stage3_rr",
                "message":f"{n}/{len(s3)} {c['config_id']}"
            })
            s3rows.append(evaluate(c,m15,indices(c,f)))
        s3rows=sortrows(s3rows)
        write_csv(OUTS["stage3"],s3rows)

        eligible3=[
            r for r in s3rows
            if r["full_trades"]>=45
            and r["pre2010_r"]>0
            and r["post2010_r"]>0
            and r["positive_eras"]==4
        ]
        finalrows=(eligible3 if eligible3 else s3rows)[:FINAL_KEEP]
        finals=[s3map[r["config_id"]] for r in finalrows]
        write_csv(OUTS["final"],finalrows)

        # ----------------------------------------------------
        # DEEP FINALIST DIAGNOSTICS
        # ----------------------------------------------------
        periods=[]; costs=[]; rolling=[]; cal=[]; trades=[]
        for n,c in enumerate(finals,1):
            STATUS.update({
                "state":"deep_finalists",
                "message":f"{n}/{len(finals)} {c['config_id']}"
            })
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
                    "sweep_lb":c["sweep_lb"],
                    "body_atr_min":c["body_atr_min"],
                    "lower_wick_body_min":c["lower_wick_body_min"],
                    "mom4_max":c["mom4_max"],
                })
                trades.append(z)

        write_csv(OUTS["periods"],periods)
        write_csv(OUTS["cost"],costs)
        write_csv(OUTS["rolling"],rolling)
        write_csv(OUTS["rolling_summary"],rolling_summary(rolling))
        write_csv(OUTS["calendar"],cal)
        write_csv(OUTS["calendar_summary"],calendar_summary(cal))
        write_csv(OUTS["trades"],trades)

        write_csv(OUTS["notes"], [
            {
                "item":"Research status",
                "value":"Final focused EUR/GBP M15 LONG confirmation. Only SWEEP_DISPLACEMENT remains open; no new archetypes are searched.",
            },
            {
                "item":"Parity",
                "value":"Hard guards reproduce 72-trade NY40/body1.15, 64-trade NY60/body1.15, 96-trade ex-Friday/body1.35 and 57-trade H1/body1.35 comparators through 2026-09-10 09:49 UTC.",
            },
            {
                "item":"Geometry grid",
                "value":"Sweep 20/40/60/80/100; body 1.10-1.35; lower wick/body 0.20/0.25/0.30; prior 4h momentum <= -1.00/-1.25/-1.50 ATR.",
            },
            {
                "item":"Contexts",
                "value":"NY00-03 anchor; NY23-03, NY00-04, NY01-03, NY23-04; exclude Friday; prior completed H1 EMA50>EMA200; no-context ablation.",
            },
            {
                "item":"RR",
                "value":"Clean RR confirmation 2.75/3.00/3.25/3.50/3.75/4.00/4.25 on Stage-2 survivors.",
            },
            {
                "item":"Historical cost",
                "value":"1.0 pip adverse long fill baseline; stress 0.5/1.0/1.5/2.0 pips.",
            },
            {
                "item":"Selection philosophy",
                "value":"Prefer four positive eras, positive pre/post-2010, stable rolling/calendar behaviour, recent survivability and parameter plateaus over maximum lifetime R.",
            },
            {
                "item":"Holdout caveat",
                "value":"Full history has been used in development; period splits are temporal robustness checks, not pristine untouched OOS.",
            },
        ])

        STATUS.update({"state":"packaging","message":"Building ZIP"})
        pack()
        STATUS.update({
            "state":"complete",
            "message":"EUR/GBP M15 LONG final confirmation complete",
            "parity":"MATCH",
            "stage1_geometry_configs":len(s1),
            "stage2_context_configs":len(s2),
            "stage3_rr_configs":len(s3),
            "finalists":len(finals),
            "bundle":BUNDLE,
        })

    except Exception as e:
        STATUS.update({"state":"error","message":str(e)})
        print("ERROR:",e,flush=True)

@app.route("/")
def root():
    return jsonify({
        "service":"EURGBP M15 LONG Final Sweep-Displacement Confirmation",
        "status":STATUS["state"],
        "instrument":PAIR,
        "timeframe":"M15",
        "side":"BUY",
        "requested_start_utc":iso(START),
        "parity_cutoff_utc":iso(PARITY_CUTOFF),
        "primary_cost_pips":PRIMARY_COST,
        "orders_supported":False,
        "trading_enabled":False,
        "routes":[
            "/eurgbp-m15-long-final-confirmation/status",
            "/eurgbp-m15-long-final-confirmation/results",
        ],
    })

@app.route("/eurgbp-m15-long-final-confirmation/status")
def status():
    return jsonify(STATUS)

@app.route("/eurgbp-m15-long-final-confirmation/results")
def results():
    return dl(BUNDLE)

if __name__=="__main__":
    threading.Thread(target=run,daemon=True).start()
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")),debug=False)
