import os
import csv
import time
import bisect
import zipfile
import threading
from copy import deepcopy
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import requests
from flask import Flask, jsonify, send_file

# ============================================================
# AUD/USD M15 SHORT — FRESH SIX-FAMILY BROAD DISCOVERY (#27 CANDIDATE)
# ============================================================
# Read-only research; live 26 strategies unchanged. This does not mirror the
# rejected AUD/USD M15 LONG or import either frozen H1 setup. Bearish archetypes
# are independently specified and evaluated. A later confirmation / 26->27
# live-safe portfolio conflict test is required before any deployment.
#
# OANDA midpoint bars; ATR14 Wilder/RMA SMA-seeded. M15 candle timestamp OPEN.
# Historical SELL reference = signal close; adverse fill = close - 1 pip.
# Stop = signal high + 10 ticks; target from REFERENCE close to stop at RR.
# Actual realised R = (fill-exit_price)/(stop-fill). Exit search NEXT M15.
# If stop & target on same candle, OPEN proximity chooses side: high side
# closer -> STOP, low side closer -> TARGET; equality -> STOP.
# Full chronological p0 per candidate; signal on exit candle eligible.
# H1/H4/D completed bars available only at next actual HTF candle OPEN.
# Broad geometry -> single-factor contexts -> RR/local neighbour diagnostics.
# Searched-history periods are robustness checks, NOT untouched out-of-sample.
# Spread, slippage & funding may differ live; historical cost tests use
# synthetic adverse entry-fill assumption, not observed bid/ask spreads.
# ============================================================
# ============================================================


app = Flask(__name__)

TOKEN = os.getenv("OANDA_TOKEN")
BASE = os.getenv("OANDA_API_URL", "https://api-fxtrade.oanda.com")
PAIR = "AUD_USD"

START = datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc)
NOW = datetime.now(timezone.utc).replace(second=0, microsecond=0)
WARMUP = START - timedelta(days=900)

NY = ZoneInfo("America/New_York")
LONDON = ZoneInfo("Europe/London")
TOKYO = ZoneInfo("Asia/Tokyo")

TICK = 0.00001
PIP = 0.0001
STOP_TICKS = 10

PRIMARY_COST = 1.0
COSTS = [0.5, 1.0, 1.5, 2.0]

# Diversity is intentional: do not let one family monopolise the next stage.
STAGE1_PER_FAMILY = 2
STAGE1_BASE_KEEP = 14
STAGE2_PER_FAMILY = 2
STAGE2_BASE_KEEP = 12
FINAL_PER_FAMILY = 2
FINAL_KEEP = 14

OUTS = {
    "coverage": "audusd_m15_short_27_coverage.csv",
    "stage1": "audusd_m15_short_27_stage1_summary.csv",
    "stage1_family": "audusd_m15_short_27_stage1_family_summary.csv",
    "stage2": "audusd_m15_short_27_stage2_summary.csv",
    "stage2_family": "audusd_m15_short_27_stage2_family_summary.csv",
    "stage3": "audusd_m15_short_27_stage3_summary.csv",
    "stage3_family": "audusd_m15_short_27_stage3_family_summary.csv",
    "shortlist": "audusd_m15_short_27_shortlist.csv",
    "periods": "audusd_m15_short_27_shortlist_periods.csv",
    "cost": "audusd_m15_short_27_shortlist_cost_stress.csv",
    "rolling": "audusd_m15_short_27_shortlist_rolling.csv",
    "rolling_summary": "audusd_m15_short_27_shortlist_rolling_summary.csv",
    "calendar": "audusd_m15_short_27_shortlist_calendar_years.csv",
    "calendar_summary": "audusd_m15_short_27_shortlist_calendar_summary.csv",
    "trades": "audusd_m15_short_27_shortlist_trades.csv",
    "decision": "audusd_m15_short_27_decision_matrix.csv",
    "notes": "audusd_m15_short_27_notes.csv",
}
BUNDLE = "AUDUSD_M15_SHORT_27_BROAD_DISCOVERY_RESULTS.zip"

STATUS = {
    "state": "not_started",
    "message": "Not started",
    "orders_supported": False,
    "trading_enabled": False,
}

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


def fetch_chunk(granularity, start, end):
    params = {
        "price": "M",
        "granularity": granularity,
        "smooth": "false",
        "from": iso(start),
        "to": iso(end),
        "includeFirst": "true",
    }
    if granularity == "D":
        params["dailyAlignment"] = 17
        params["alignmentTimezone"] = "America/New_York"

    url = f"{BASE}/v3/instruments/{PAIR}/candles"
    response = requests.get(url, headers=headers(), params=params, timeout=60)
    response.raise_for_status()

    rows = []
    for candle in response.json().get("candles", []):
        if not candle.get("complete", False):
            continue
        mid = candle["mid"]
        rows.append({
            "time": parse_time(candle["time"]),
            "open": float(mid["o"]),
            "high": float(mid["h"]),
            "low": float(mid["l"]),
            "close": float(mid["c"]),
        })
    return rows


def fetch(granularity, start, end, chunk_days):
    current = start
    by_time = {}
    chunk = 0

    while current < end:
        chunk += 1
        nxt = min(current + timedelta(days=chunk_days), end)
        STATUS.update({
            "state": "fetch",
            "message": f"Fetching {granularity} chunk {chunk}: {iso(current)} -> {iso(nxt)}",
        })

        rows = fetch_chunk(granularity, current, nxt)
        for row in rows:
            by_time[row["time"]] = row

        current = nxt
        time.sleep(0.04)

    return [by_time[t] for t in sorted(by_time)]


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
# M15 FEATURE CACHE — INDEPENDENT SHORT
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
    bear_body = o - cl
    prior_body = np.r_[np.nan, np.abs(cl[:-1] - o[:-1])]
    br = np.full(n, np.nan)
    ok_body = prior_body > 0
    br[ok_body] = bear_body[ok_body] / prior_body[ok_body]
    valid_atr = np.isfinite(a) & (a > 0)
    body_atr = np.full(n, np.nan)
    body_atr[valid_atr] = bear_body[valid_atr] / a[valid_atr]
    candle_range = h - l
    range_atr = np.full(n, np.nan)
    range_atr[valid_atr] = candle_range[valid_atr] / a[valid_atr]
    close_loc = np.full(n, np.nan)
    valid_range = candle_range > 0
    close_loc[valid_range] = (cl[valid_range] - l[valid_range]) / candle_range[valid_range]
    upper_wick = h - np.maximum(o,cl)
    upper_wick_body = np.full(n, np.nan)
    upper_wick_body[bear_body > 0] = upper_wick[bear_body > 0] / bear_body[bear_body > 0]
    compression = np.full(n,np.nan)
    previous_atr = np.r_[np.nan,a[:-1]]
    previous_atr_mean20 = np.r_[np.nan,am20[:-1]]
    ok_comp = (np.isfinite(previous_atr) & np.isfinite(previous_atr_mean20)
               & (previous_atr_mean20 > 0))
    compression[ok_comp] = previous_atr[ok_comp] / previous_atr_mean20[ok_comp]
    lookbacks = [10,20,40,60,80,100,120,165,200]
    prev_low = {lb:prev_extreme(l,lb,"min") for lb in lookbacks}
    prev_high = {lb:prev_extreme(h,lb,"max") for lb in lookbacks}
    dist_high = {}
    for lb in [40,60,80,100,120,165,200]:
        x = np.full(n,np.nan)
        ok = valid_atr & np.isfinite(prev_high[lb])
        x[ok] = np.abs(h[ok]-prev_high[lb][ok])/a[ok]
        dist_high[lb] = x
    # Prior 4h M15 rally momentum, excludes signal: (close[i-1]-close[i-17])/ATR[i].
    mom4 = np.full(n,np.nan)
    mom4[17:] = np.divide(cl[16:-1]-cl[:-17],a[17:],
                          out=np.full(n-17,np.nan),where=valid_atr[17:])
    ny_hour = np.zeros(n,dtype=np.int16)
    ny_weekday = np.zeros(n,dtype=np.int16)
    london_hour = np.zeros(n,dtype=np.int16)
    tokyo_hour = np.zeros(n,dtype=np.int16)
    sydney_hour = np.zeros(n,dtype=np.int16)
    sydney = ZoneInfo("Australia/Sydney")
    for i,t in enumerate(times):
        z=t.astimezone(NY)
        ny_hour[i],ny_weekday[i]=z.hour,z.weekday()
        london_hour[i]=t.astimezone(LONDON).hour
        tokyo_hour[i]=t.astimezone(TOKYO).hour
        sydney_hour[i]=t.astimezone(sydney).hour
    return {
        "n":n,"times":times,"open":o,"high":h,"low":l,"close":cl,
        "atr":a,"valid_atr":valid_atr,"bearish":bearish,"exact_bear":exact_bear,
        "bear_br":br,"body_atr":body_atr,"range_atr":range_atr,
        "close_loc":close_loc,"upper_wick_body":upper_wick_body,
        "compression":compression,"prev_low":prev_low,"prev_high":prev_high,
        "structure_dist_high":dist_high,"mom4":mom4,"ny_hour":ny_hour,
        "ny_weekday":ny_weekday,"london_hour":london_hour,
        "tokyo_hour":tokyo_hour,"sydney_hour":sydney_hour,
        "h1_close":h1["close"],"h1_ema50":h1["ema50"],
        "h1_ema100":h1["ema100"],"h1_ema200":h1["ema200"],
        "h1_atr":h1["atr_ratio50"],"h4_close":h4["close"],
        "h4_ema100":h4["ema100"],"h4_ema200":h4["ema200"],
        "h4_atr":h4["atr_ratio50"],"d_close":daily["close"],
        "d_ema50":daily["ema50"],"d_ema200":daily["ema200"],
        "d_atr":daily["atr_ratio50"],
    }


# ============================================================
# CANDIDATE CONFIGURATION
# ============================================================

def cfg(config_id, family, rr=3.5, **kwargs):
    row = {
        "config_id":config_id,"family":family,"rr":rr,
        "context":"NONE","br_min":None,"body_atr_min":None,
        "range_atr_min":None,"close_loc_max":None,
        "upper_wick_body_min":None,"structure_lb":None,
        "structure_dist_atr_max":None,"sweep_lb":None,"breakout_lb":None,
        "compression_max":None,"mom4_min":None,
        "excluded_weekdays":set(),"excluded_ny_hours":set(),
    }
    row.update(kwargs)
    return row


def stage1_configs():
    # Independent, compact bearish family geometries; no H1 short config lock.
    out=[]
    engulf=[
        (1.00,0.50,40,0.10),(1.00,0.75,80,0.15),
        (1.15,0.50,100,0.15),(1.15,0.75,120,0.10),
        (1.25,0.75,165,0.20),(1.25,1.00,80,0.15),
        (1.35,0.75,100,0.20),(1.35,1.00,120,0.10),
        (1.50,0.75,165,0.15),(1.50,1.00,200,0.20),
    ]
    for k,(br,body,lb,dist) in enumerate(engulf):
        out.append(cfg(f"S1_ENG_{k}","BEAR_ENGULF_STRUCTURE",
            br_min=br,body_atr_min=body,structure_lb=lb,
            structure_dist_atr_max=dist))
    sweep=[
        (20,0.75,0.15,0.50),(20,1.00,0.25,0.75),
        (40,0.75,0.25,0.75),(40,1.00,0.25,1.00),
        (40,1.25,0.35,1.25),(60,0.75,0.25,0.50),
        (60,1.00,0.35,1.00),(60,1.25,0.25,1.50),
        (100,1.00,0.35,1.25),(100,1.25,0.35,1.50),
    ]
    for k,(lb,body,wick,mom) in enumerate(sweep):
        out.append(cfg(f"S1_SWEEP_{k}","HIGH_SWEEP_DISPLACEMENT",
            sweep_lb=lb,body_atr_min=body,
            upper_wick_body_min=wick,mom4_min=mom))
    failed=[
        (20,0.50,0.40),(20,0.75,0.30),
        (40,0.50,0.40),(40,0.75,0.30),
        (40,1.00,0.25),(60,0.50,0.35),
        (60,0.75,0.30),(60,1.00,0.25),
        (100,0.75,0.30),(100,1.00,0.20),
    ]
    for k,(lb,body,loc) in enumerate(failed):
        out.append(cfg(f"S1_FAIL_{k}","FAILED_UPSIDE_BREAKOUT",
            sweep_lb=lb,body_atr_min=body,close_loc_max=loc))
    outside=[
        (0.50,0.40,40,0.20),(0.75,0.35,40,0.15),
        (0.75,0.30,60,0.20),(1.00,0.35,60,0.15),
        (1.00,0.25,80,0.20),(1.25,0.30,80,0.15),
        (1.25,0.25,100,0.20),(1.50,0.25,100,0.15),
    ]
    for k,(body,loc,lb,dist) in enumerate(outside):
        out.append(cfg(f"S1_OUT_{k}","BEAR_OUTSIDE_REVERSAL",
            body_atr_min=body,close_loc_max=loc,
            structure_lb=lb,structure_dist_atr_max=dist))
    compression=[
        (0.60,0.75,1.20,10),(0.65,0.75,1.30,10),
        (0.65,1.00,1.40,10),(0.70,0.75,1.30,10),
        (0.70,1.00,1.40,10),(0.70,1.25,1.50,10),
        (0.75,0.75,1.30,10),(0.75,1.00,1.40,10),
        (0.75,1.25,1.50,20),(0.80,1.00,1.50,20),
    ]
    for k,(comp,body,ran,lb) in enumerate(compression):
        out.append(cfg(f"S1_COMP_{k}","BEAR_COMPRESSION_BREAKDOWN",
            compression_max=comp,body_atr_min=body,
            range_atr_min=ran,breakout_lb=lb))
    rejection=[
        (20,0.50,0.75,0.35),(20,0.75,1.00,0.30),
        (40,0.50,1.00,0.35),(40,0.75,1.25,0.30),
        (40,1.00,1.50,0.25),(60,0.75,1.25,0.30),
        (60,1.00,1.50,0.25),(100,1.00,1.50,0.25),
    ]
    for k,(lb,body,mom,loc) in enumerate(rejection):
        out.append(cfg(f"S1_RALLY_{k}","RALLY_REJECTION",
            sweep_lb=lb,body_atr_min=body,mom4_min=mom,close_loc_max=loc))
    return out


CONTEXTS = [
    "NONE","H1_CLOSE_LT_EMA100","H1_CLOSE_LT_EMA200",
    "H1_EMA50_LT_EMA200","H4_CLOSE_LT_EMA100","H4_CLOSE_LT_EMA200",
    "D_CLOSE_LT_EMA200","D_EMA50_LT_EMA200",
    "H1_ATR_GE_080","H4_ATR_GE_080","D_ATR_GE_080",
    *[f"NY_BLOCK_{n:02d}-{n+3:02d}" for n in range(0,24,4)],
    *[f"LDN_BLOCK_{n:02d}-{n+3:02d}" for n in range(0,24,4)],
    *[f"TOKYO_BLOCK_{n:02d}-{n+3:02d}" for n in range(0,24,4)],
    *[f"SYDNEY_BLOCK_{n:02d}-{n+3:02d}" for n in range(0,24,4)],
    *[f"EXCLUDE_WEEKDAY_{day}" for day in range(5)],
]

# ============================================================
# SIGNAL EVALUATION
# ============================================================

def context_mask(mask, config, f):
    ctx=config.get("context","NONE")
    trend={
        "H1_CLOSE_LT_EMA100":("h1_close","h1_ema100"),
        "H1_CLOSE_LT_EMA200":("h1_close","h1_ema200"),
        "H1_EMA50_LT_EMA200":("h1_ema50","h1_ema200"),
        "H4_CLOSE_LT_EMA100":("h4_close","h4_ema100"),
        "H4_CLOSE_LT_EMA200":("h4_close","h4_ema200"),
        "D_CLOSE_LT_EMA200":("d_close","d_ema200"),
        "D_EMA50_LT_EMA200":("d_ema50","d_ema200"),
    }
    if ctx in trend:
        x,y=trend[ctx]
        mask &= f[x]<f[y]
    elif ctx in ("H1_ATR_GE_080","H4_ATR_GE_080","D_ATR_GE_080"):
        mask &= f[ctx.split("_")[0].lower()+"_atr"] >= 0.80
    elif "_BLOCK_" in ctx:
        tz,hours=ctx.split("_BLOCK_")
        a,b=map(int,hours.split("-"))
        key={"NY":"ny_hour","LDN":"london_hour",
             "TOKYO":"tokyo_hour","SYDNEY":"sydney_hour"}[tz]
        mask &= (f[key]>=a)&(f[key]<=b)
    elif ctx.startswith("EXCLUDE_WEEKDAY_"):
        mask &= f["ny_weekday"]!=int(ctx.split("_")[-1])
    elif ctx!="NONE":
        raise ValueError(f"Unknown context: {ctx}")
    for day in config.get("excluded_weekdays",set()):
        mask &= f["ny_weekday"]!=day
    for hour in config.get("excluded_ny_hours",set()):
        mask &= f["ny_hour"]!=hour
    return mask


def indices(config, f):
    mask=f["valid_atr"].copy() & f["bearish"]
    family=config["family"]
    if family=="BEAR_ENGULF_STRUCTURE":
        mask &= f["exact_bear"]
        mask &= f["bear_br"] >= config["br_min"]
        mask &= f["body_atr"] >= config["body_atr_min"]
        mask &= (f["structure_dist_high"][config["structure_lb"]]
                 <= config["structure_dist_atr_max"])
    elif family=="HIGH_SWEEP_DISPLACEMENT":
        lb=config["sweep_lb"]
        mask &= f["high"] > f["prev_high"][lb]
        previous_low=np.r_[np.nan,f["low"][:-1]]
        mask &= f["close"] < previous_low
        mask &= f["body_atr"] >= config["body_atr_min"]
        mask &= f["upper_wick_body"] >= config["upper_wick_body_min"]
        mask &= f["mom4"] >= config["mom4_min"]
    elif family=="FAILED_UPSIDE_BREAKOUT":
        prior_high=f["prev_high"][config["sweep_lb"]]
        mask &= f["high"] > prior_high
        mask &= f["close"] < prior_high
        mask &= f["body_atr"] >= config["body_atr_min"]
        mask &= f["close_loc"] <= config["close_loc_max"]
    elif family=="BEAR_OUTSIDE_REVERSAL":
        prev_high=np.r_[np.nan,f["high"][:-1]]
        prev_low=np.r_[np.nan,f["low"][:-1]]
        mask &= f["high"] > prev_high
        mask &= f["low"] < prev_low
        mask &= f["body_atr"] >= config["body_atr_min"]
        mask &= f["close_loc"] <= config["close_loc_max"]
        mask &= (f["structure_dist_high"][config["structure_lb"]]
                 <= config["structure_dist_atr_max"])
    elif family=="BEAR_COMPRESSION_BREAKDOWN":
        mask &= f["compression"] <= config["compression_max"]
        mask &= f["body_atr"] >= config["body_atr_min"]
        mask &= f["range_atr"] >= config["range_atr_min"]
        mask &= f["close"] < f["prev_low"][config["breakout_lb"]]
    elif family=="RALLY_REJECTION":
        prior_high=f["prev_high"][config["sweep_lb"]]
        mask &= f["high"] > prior_high
        mask &= f["close"] < f["prev_high"][10]
        mask &= f["body_atr"] >= config["body_atr_min"]
        mask &= f["mom4"] >= config["mom4_min"]
        mask &= f["close_loc"] <= config["close_loc_max"]
    else:
        raise ValueError(f"Unknown family: {family}")
    mask=context_mask(mask,config,f)
    mask[:200]=False
    return np.flatnonzero(mask).tolist()


# ============================================================
# SHORT BACKTEST — full-ledger p0, time-window metrics never reset p0
# ============================================================
OUTCOME_CACHE = {}
BACKTEST_CACHE = {}


def outcome(candles, signal_index, rr, cost_pips):
    key=(signal_index,round(rr,4),round(cost_pips,4))
    if len(OUTCOME_CACHE)>=250_000:
        OUTCOME_CACHE.clear()  # bounded; affects speed only
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
            # High side nearer the candle open => stop first. Equality => stop.
            if abs(bar["high"]-bar["open"])<=abs(bar["open"]-bar["low"]):
                price,reason=stop,"STOP"
            else:
                price,reason=target,"TARGET"
        elif target_hit:
            price,reason=target,"TARGET"
        else:
            price,reason=stop,"STOP"
        x={
            "signal_index":signal_index,"exit_index":j,
            "entry_time":candle["time"],"exit_time":bar["time"],
            "entry_time_utc":iso(candle["time"]),"exit_time_utc":iso(bar["time"]),
            "reference_entry":ref,"historical_fill":fill,
            "stop":stop,"target":target,"result_r":(fill-price)/actual_risk,
            "exit_reason":reason,"rr":rr,"cost_pips":cost_pips,
        }
        OUTCOME_CACHE[key]=x
        return x
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
# ROBUSTNESS SCORING / STAGED SEARCH
# ============================================================

ERAS = [
    ("ERA_2002_07", START, datetime(2008, 1, 1, tzinfo=timezone.utc)),
    ("ERA_2008_13", datetime(2008, 1, 1, tzinfo=timezone.utc), datetime(2014, 1, 1, tzinfo=timezone.utc)),
    ("ERA_2014_19", datetime(2014, 1, 1, tzinfo=timezone.utc), datetime(2020, 1, 1, tzinfo=timezone.utc)),
    ("ERA_2020_NOW", datetime(2020, 1, 1, tzinfo=timezone.utc), NOW),
]


def config_fields(config):
    keys = [
        "br_min",
        "body_atr_min",
        "range_atr_min",
        "close_loc_max",
        "upper_wick_body_min",
        "structure_lb",
        "structure_dist_atr_max",
        "sweep_lb",
        "breakout_lb",
        "compression_max",
        "mom4_min",
    ]
    return {key: config.get(key) for key in keys}


def evaluate(config, candles, candidate_indices):
    full = stats(backtest(candles, candidate_indices, config["rr"], PRIMARY_COST, candles[0]["time"], NOW))
    early = stats(backtest(candles, candidate_indices, config["rr"], PRIMARY_COST, candles[0]["time"], datetime(2010, 1, 1, tzinfo=timezone.utc)))
    late = stats(backtest(candles, candidate_indices, config["rr"], PRIMARY_COST, datetime(2010, 1, 1, tzinfo=timezone.utc), NOW))

    era_pf = []
    era_r = []
    era_trades = []
    for _, a, b in ERAS:
        s = stats(backtest(candles, candidate_indices, config["rr"], PRIMARY_COST, a, b))
        era_pf.append(s["profit_factor"])
        era_r.append(s["total_r"])
        era_trades.append(s["trades"])

    positive_eras = sum(x > 0 for x in era_r)
    active_eras = sum(x > 0 for x in era_trades)
    min_active_era_pf = min(
        [era_pf[i] for i in range(4) if era_trades[i] > 0],
        default=0.0,
    )

    # Deliberately rewards breadth and temporal persistence more than max PF.
    score = (
        1.50 * min(full["profit_factor"], 3.0)
        + 0.75 * min(early["profit_factor"], 2.5)
        + 0.90 * min(late["profit_factor"], 2.5)
        + 0.40 * positive_eras
        + 0.15 * active_eras
        + 0.20 * min(max(min_active_era_pf, 0.0), 2.0)
        + 0.15 * min(full["trades"] / 100.0, 2.0)
        + 0.10 * min(max(full["expectancy_r"], 0.0), 1.0)
    )

    row = {
        "config_id": config["config_id"],
        "family": config["family"],
        "context": config.get("context", "NONE"),
        "rr": config["rr"],
        "full_trades": full["trades"],
        "full_pf": round(full["profit_factor"], 6),
        "full_r": round(full["total_r"], 4),
        "full_exp": round(full["expectancy_r"], 6),
        "full_dd": round(full["max_drawdown_r"], 4),
        "full_win_rate": round(full["win_rate"], 4),
        "pre2010_trades": early["trades"],
        "pre2010_pf": round(early["profit_factor"], 6),
        "pre2010_r": round(early["total_r"], 4),
        "post2010_trades": late["trades"],
        "post2010_pf": round(late["profit_factor"], 6),
        "post2010_r": round(late["total_r"], 4),
        "positive_eras": positive_eras,
        "active_eras": active_eras,
        "min_active_era_pf": round(min_active_era_pf, 6),
        "robust_score": round(score, 6),
    }

    for i in range(4):
        row[f"era{i + 1}_trades"] = era_trades[i]
        row[f"era{i + 1}_pf"] = round(era_pf[i], 6)
        row[f"era{i + 1}_r"] = round(era_r[i], 4)

    row.update(config_fields(config))
    return row


def sort_rows(rows):
    return sorted(
        rows,
        key=lambda r: (
            r["positive_eras"],
            r["pre2010_r"] > 0,
            r["post2010_r"] > 0,
            r["robust_score"],
            r["full_r"],
            r["full_trades"],
        ),
        reverse=True,
    )


def family_summary(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["family"]].append(row)

    out = []
    for family, sub in grouped.items():
        ranked = sort_rows(sub)
        best = ranked[0]
        out.append({
            "family": family,
            "configs": len(sub),
            "positive_full_configs": sum(r["full_r"] > 0 for r in sub),
            "positive_pre_and_post_configs": sum(
                r["pre2010_r"] > 0 and r["post2010_r"] > 0 for r in sub
            ),
            "four_positive_era_configs": sum(r["positive_eras"] == 4 for r in sub),
            "best_config_id": best["config_id"],
            "best_full_trades": best["full_trades"],
            "best_full_pf": best["full_pf"],
            "best_full_r": best["full_r"],
            "best_pre2010_r": best["pre2010_r"],
            "best_post2010_r": best["post2010_r"],
            "best_positive_eras": best["positive_eras"],
            "best_robust_score": best["robust_score"],
        })

    return sorted(
        out,
        key=lambda r: (
            r["four_positive_era_configs"],
            r["positive_pre_and_post_configs"],
            r["best_positive_eras"],
            r["best_robust_score"],
        ),
        reverse=True,
    )


def select_diverse(rows, per_family, total):
    """Take a minimum amount of family diversity, then fill globally."""
    ranked = sort_rows(rows)
    selected = []
    seen_ids = set()

    by_family = defaultdict(list)
    for row in ranked:
        by_family[row["family"]].append(row)

    for family in sorted(by_family):
        for row in by_family[family][:per_family]:
            if row["config_id"] not in seen_ids:
                selected.append(row)
                seen_ids.add(row["config_id"])

    for row in ranked:
        if len(selected) >= total:
            break
        if row["config_id"] not in seen_ids:
            selected.append(row)
            seen_ids.add(row["config_id"])

    return sort_rows(selected)[:total]


def stage2_configs(base_rows, config_by_id):
    out = []
    for rank, row in enumerate(base_rows):
        base = config_by_id[row["config_id"]]
        for context in CONTEXTS:
            x = deepcopy(base)
            x["config_id"] = f"S2_{rank}_{context}"
            x["context"] = context
            out.append(x)
    return out


def local_variants(base, rank):
    out = []

    for rr in [
        2.50, 2.75, 3.00, 3.25, 3.50, 3.75, 4.00,
        4.25, 4.50, 4.75, 5.00, 5.25, 5.50, 5.75, 6.00,
    ]:
        x = deepcopy(base)
        x["rr"] = rr
        x["config_id"] = f"S3_{rank}_RR_{rr:.2f}"
        out.append(x)

    bumps = {
        "br_min": [-0.15, -0.05, 0.05, 0.15],
        "body_atr_min": [-0.20, -0.10, 0.10, 0.20],
        "range_atr_min": [-0.20, -0.10, 0.10, 0.20],
        "close_loc_max": [-0.10, -0.05, 0.05, 0.10],
        "upper_wick_body_min": [-0.10, -0.05, 0.05, 0.10],
        "structure_dist_atr_max": [-0.05, -0.025, 0.025, 0.05],
        "compression_max": [-0.05, -0.025, 0.025, 0.05],
        "mom4_min": [-0.50, -0.25, 0.25, 0.50],
    }

    for field, deltas in bumps.items():
        value = base.get(field)
        if value is None:
            continue
        for delta in deltas:
            new_value = round(value + delta, 4)
            if field == "close_loc_max":
                if not (0.05 <= new_value <= 0.95):
                    continue
            elif field == "mom4_min":
                pass  # prior upward momentum threshold, local +/- only
            elif new_value <= 0:
                continue
            x = deepcopy(base)
            x[field] = new_value
            x["config_id"] = f"S3_{rank}_{field}_{new_value}"
            out.append(x)

    lookbacks = [10, 20, 40, 60, 80, 100, 120, 165, 200]
    for field in ["structure_lb", "sweep_lb", "breakout_lb"]:
        value = base.get(field)
        if value not in lookbacks:
            continue
        p = lookbacks.index(value)
        for q in [p - 1, p + 1]:
            if 0 <= q < len(lookbacks):
                x = deepcopy(base)
                x[field] = lookbacks[q]
                x["config_id"] = f"S3_{rank}_{field}_{lookbacks[q]}"
                out.append(x)

    return out


def stage3_configs(base_rows, config_by_id):
    out = []
    seen = set()

    for rank, row in enumerate(base_rows):
        base = config_by_id[row["config_id"]]
        for x in local_variants(base, rank):
            signature = tuple(str(x.get(key)) for key in [
                "family",
                "br_min",
                "body_atr_min",
                "range_atr_min",
                "close_loc_max",
                "upper_wick_body_min",
                "structure_lb",
                "structure_dist_atr_max",
                "sweep_lb",
                "breakout_lb",
                "compression_max",
                "mom4_min",
                "context",
                "rr",
            ])
            if signature in seen:
                continue
            seen.add(signature)
            out.append(x)

    return out


# ============================================================
# FINAL SHORTLIST DIAGNOSTICS
# ============================================================

def stat_row(config, label, trades):
    s = stats(trades)
    return {
        "config_id": config["config_id"],
        "family": config["family"],
        "context": config.get("context", "NONE"),
        "rr": config["rr"],
        "period": label,
        **{
            key: round(value, 6) if isinstance(value, float) else value
            for key, value in s.items()
        },
    }


def period_rows(config, candles, candidate_indices):
    periods = [
        ("FULL", candles[0]["time"], NOW),
        ("PRE_2010", candles[0]["time"], datetime(2010, 1, 1, tzinfo=timezone.utc)),
        ("2010_PLUS", datetime(2010, 1, 1, tzinfo=timezone.utc), NOW),
        ("DEV_2002_17", candles[0]["time"], datetime(2018, 1, 1, tzinfo=timezone.utc)),
        ("VALIDATION_2018_PLUS", datetime(2018, 1, 1, tzinfo=timezone.utc), NOW),
        *ERAS,
        ("LAST_5Y", NOW - timedelta(days=365.2425 * 5), NOW),
        ("LAST_2Y", NOW - timedelta(days=365.2425 * 2), NOW),
        ("LAST_1Y", NOW - timedelta(days=365.2425), NOW),
    ]

    out = []
    for label, a, b in periods:
        row = stat_row(
            config,
            label,
            backtest(candles, candidate_indices, config["rr"], PRIMARY_COST, a, b),
        )
        row["start_utc"] = iso(a)
        row["end_utc"] = iso(b)
        out.append(row)
    return out


def cost_rows(config, candles, candidate_indices):
    out = []
    windows = [
        ("FULL", candles[0]["time"], NOW),
        ("VALIDATION_2018_PLUS", datetime(2018, 1, 1, tzinfo=timezone.utc), NOW),
        ("LAST_5Y", NOW - timedelta(days=365.2425 * 5), NOW),
        ("LAST_2Y", NOW - timedelta(days=365.2425 * 2), NOW),
    ]

    for cost in COSTS:
        for label, a, b in windows:
            row = stat_row(
                config,
                label,
                backtest(candles, candidate_indices, config["rr"], cost, a, b),
            )
            row["cost_pips"] = cost
            out.append(row)
    return out


def rolling_rows(config, candles, candidate_indices):
    out = []
    first = month_floor(max(candles[0]["time"], START))
    last = month_floor(NOW)

    for months in [12, 24, 36]:
        start = first
        while add_months(start, months) <= last:
            end = add_months(start, months)
            s = stats(backtest(
                candles,
                candidate_indices,
                config["rr"],
                PRIMARY_COST,
                start,
                end,
            ))
            out.append({
                "config_id": config["config_id"],
                "months": months,
                "start_utc": iso(start),
                "end_utc": iso(end),
                "trades": s["trades"],
                "profit_factor": round(s["profit_factor"], 6),
                "total_r": round(s["total_r"], 4),
                "positive": s["total_r"] > 0,
                "zero_trade": s["trades"] == 0,
            })
            start = add_months(start, 1)

    return out


def rolling_summary(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row["config_id"], row["months"])].append(row)

    out = []
    for (config_id, months), sub in grouped.items():
        active = [row for row in sub if row["trades"] > 0]
        out.append({
            "config_id": config_id,
            "months": months,
            "windows": len(sub),
            "active_windows": len(active),
            "zero_trade_windows": len(sub) - len(active),
            "positive_windows_pct": round(
                100.0 * sum(row["positive"] for row in sub) / len(sub),
                4,
            ) if sub else 0.0,
            "positive_active_windows_pct": round(
                100.0 * sum(row["positive"] for row in active) / len(active),
                4,
            ) if active else 0.0,
            "median_r_all": round(med([row["total_r"] for row in sub]), 4),
            "median_r_active": round(med([row["total_r"] for row in active]), 4),
            "median_pf_active": round(med([row["profit_factor"] for row in active]), 6),
            "worst_r": round(min((row["total_r"] for row in sub), default=0.0), 4),
            "best_r": round(max((row["total_r"] for row in sub), default=0.0), 4),
        })
    return out


def calendar_rows(config, candles, candidate_indices):
    out = []
    first_year = max(START.year, candles[0]["time"].year)

    for year in range(first_year, NOW.year):
        a = datetime(year, 1, 1, tzinfo=timezone.utc)
        b = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        s = stats(backtest(
            candles,
            candidate_indices,
            config["rr"],
            PRIMARY_COST,
            a,
            b,
        ))
        out.append({
            "config_id": config["config_id"],
            "year": year,
            "trades": s["trades"],
            "profit_factor": round(s["profit_factor"], 6),
            "total_r": round(s["total_r"], 4),
            "positive": s["total_r"] > 0,
            "negative": s["total_r"] < 0,
            "zero_trade": s["trades"] == 0,
        })
    return out


def calendar_summary(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["config_id"]].append(row)

    out = []
    for config_id, sub in grouped.items():
        active = [row for row in sub if row["trades"] > 0]
        out.append({
            "config_id": config_id,
            "completed_years": len(sub),
            "active_years": len(active),
            "positive_years": sum(row["positive"] for row in sub),
            "negative_years": sum(row["negative"] for row in sub),
            "zero_trade_years": sum(row["zero_trade"] for row in sub),
            "positive_years_pct": round(
                100.0 * sum(row["positive"] for row in sub) / len(sub),
                4,
            ) if sub else 0.0,
            "positive_active_years_pct": round(
                100.0 * sum(row["positive"] for row in active) / len(active),
                4,
            ) if active else 0.0,
            "median_trades_year": round(med([row["trades"] for row in sub]), 4),
            "median_year_r": round(med([row["total_r"] for row in sub]), 4),
            "worst_year_r": round(min((row["total_r"] for row in sub), default=0.0), 4),
            "best_year_r": round(max((row["total_r"] for row in sub), default=0.0), 4),
        })
    return out


def shortlist_summary(config, candles, candidate_indices):
    def window(a, b):
        return stats(backtest(candles, candidate_indices, config["rr"], PRIMARY_COST, a, b))

    full = window(candles[0]["time"], NOW)
    pre = window(candles[0]["time"], datetime(2010, 1, 1, tzinfo=timezone.utc))
    post = window(datetime(2010, 1, 1, tzinfo=timezone.utc), NOW)
    val = window(datetime(2018, 1, 1, tzinfo=timezone.utc), NOW)
    y20 = window(datetime(2020, 1, 1, tzinfo=timezone.utc), NOW)
    l5 = window(NOW - timedelta(days=365.2425 * 5), NOW)
    l2 = window(NOW - timedelta(days=365.2425 * 2), NOW)
    l1 = window(NOW - timedelta(days=365.2425), NOW)

    eras = [window(a, b) for _, a, b in ERAS]

    return {
        "config_id": config["config_id"],
        "family": config["family"],
        "context": config.get("context", "NONE"),
        "rr": config["rr"],
        **config_fields(config),
        "full_trades": full["trades"],
        "full_pf": round(full["profit_factor"], 6),
        "full_r": round(full["total_r"], 4),
        "full_exp": round(full["expectancy_r"], 6),
        "full_dd": round(full["max_drawdown_r"], 4),
        "full_win_rate": round(full["win_rate"], 4),
        "pre2010_pf": round(pre["profit_factor"], 6),
        "pre2010_r": round(pre["total_r"], 4),
        "post2010_pf": round(post["profit_factor"], 6),
        "post2010_r": round(post["total_r"], 4),
        "validation2018_plus_trades": val["trades"],
        "validation2018_plus_pf": round(val["profit_factor"], 6),
        "validation2018_plus_r": round(val["total_r"], 4),
        "era2020_plus_trades": y20["trades"],
        "era2020_plus_pf": round(y20["profit_factor"], 6),
        "era2020_plus_r": round(y20["total_r"], 4),
        "last5y_trades": l5["trades"],
        "last5y_pf": round(l5["profit_factor"], 6),
        "last5y_r": round(l5["total_r"], 4),
        "last2y_trades": l2["trades"],
        "last2y_pf": round(l2["profit_factor"], 6),
        "last2y_r": round(l2["total_r"], 4),
        "last1y_trades": l1["trades"],
        "last1y_pf": round(l1["profit_factor"], 6),
        "last1y_r": round(l1["total_r"], 4),
        "positive_eras": sum(x["trades"] > 0 and x["total_r"] > 0 for x in eras),
        "active_eras": sum(x["trades"] > 0 for x in eras),
        "min_active_era_pf": round(
            min([x["profit_factor"] for x in eras if x["trades"] > 0], default=0.0),
            6,
        ),
    }


def serialise_trade(config, trade):
    row = dict(trade)
    row.update({
        "config_id": config["config_id"],
        "family": config["family"],
        "context": config.get("context", "NONE"),
    })
    row.pop("entry_time", None)
    row.pop("exit_time", None)
    return row


def decision_rows(shortlist, costs, rollsum, calsum):
    cost_map = {
        (row["config_id"], row["period"], row["cost_pips"]): row
        for row in costs
    }
    roll_map = {
        (row["config_id"], row["months"]): row
        for row in rollsum
    }
    cal_map = {row["config_id"]: row for row in calsum}

    out = []
    for row in shortlist:
        cid = row["config_id"]
        c2_full = cost_map.get((cid, "FULL", 2.0), {})
        c2_val = cost_map.get((cid, "VALIDATION_2018_PLUS", 2.0), {})
        r12 = roll_map.get((cid, 12), {})
        r24 = roll_map.get((cid, 24), {})
        r36 = roll_map.get((cid, 36), {})
        cal = cal_map.get(cid, {})

        # Research gate only. PASS means "worth deeper validation", not live approval.
        checks = {
            "enough_trades": row["full_trades"] >= 50,
            "full_pf": row["full_pf"] >= 1.30,
            "full_positive": row["full_r"] > 0,
            "eras": row["positive_eras"] >= 3,
            "2018_positive": row["validation2018_plus_r"] > 0,
            "2020_positive": row["era2020_plus_r"] > 0,
            "last5_positive": row["last5y_r"] > 0,
            "last2_positive": row["last2y_r"] > 0,
            "rolling24": r24.get("positive_active_windows_pct", 0.0) >= 65.0,
            "2pip_full": c2_full.get("profit_factor", 0.0) >= 1.15,
            "2pip_2018": c2_val.get("total_r", 0.0) > 0,
            "rolling36": r36.get("positive_active_windows_pct", 0.0) >= 70.0,
        }
        passed = sum(bool(x) for x in checks.values())

        if all(checks.values()):
            verdict = "DEEP_VALIDATE"
        elif passed >= 10 and row["full_r"] > 0:
            verdict = "WATCH"
        else:
            verdict = "REJECT_OR_LOW_PRIORITY"

        out.append({
            "config_id": cid,
            "family": row["family"],
            "context": row["context"],
            "rr": row["rr"],
            "research_verdict": verdict,
            "checks_passed": passed,
            "checks_total": len(checks),
            **{f"check_{k}": v for k, v in checks.items()},
            "full_trades": row["full_trades"],
            "full_pf": row["full_pf"],
            "full_r": row["full_r"],
            "full_dd": row["full_dd"],
            "validation2018_plus_pf": row["validation2018_plus_pf"],
            "validation2018_plus_r": row["validation2018_plus_r"],
            "era2020_plus_pf": row["era2020_plus_pf"],
            "era2020_plus_r": row["era2020_plus_r"],
            "last5y_pf": row["last5y_pf"],
            "last5y_r": row["last5y_r"],
            "last2y_pf": row["last2y_pf"],
            "last2y_r": row["last2y_r"],
            "cost_2pip_full_pf": c2_full.get("profit_factor", 0.0),
            "cost_2pip_full_r": c2_full.get("total_r", 0.0),
            "cost_2pip_2018_pf": c2_val.get("profit_factor", 0.0),
            "cost_2pip_2018_r": c2_val.get("total_r", 0.0),
            "rolling12_positive_active_pct": r12.get("positive_active_windows_pct", 0.0),
            "rolling24_positive_active_pct": r24.get("positive_active_windows_pct", 0.0),
            "rolling36_positive_active_pct": r36.get("positive_active_windows_pct", 0.0),
            "rolling36_median_r": r36.get("median_r_active", 0.0),
            "rolling36_worst_r": r36.get("worst_r", 0.0),
            "active_calendar_years": cal.get("active_years", 0),
            "positive_active_years_pct": cal.get("positive_active_years_pct", 0.0),
            "zero_trade_years": cal.get("zero_trade_years", 0),
            "worst_calendar_year_r": cal.get("worst_year_r", 0.0),
        })

    order = {"DEEP_VALIDATE": 2, "WATCH": 1, "REJECT_OR_LOW_PRIORITY": 0}
    return sorted(
        out,
        key=lambda x: (
            order[x["research_verdict"]],
            x["checks_passed"],
            x["full_r"],
        ),
        reverse=True,
    )


# ============================================================
# MAIN RESEARCH RUNNER
# ============================================================

def run_research():
    try:
        STATUS.update({
            "state": "fetch",
            "message": "Fetching AUD/USD M15 + H1/H4/D history",
            "progress": 1,
        })

        m15 = fetch("M15", START, NOW, 35)
        h1 = fetch("H1", WARMUP, NOW, 180)
        h4 = fetch("H4", WARMUP, NOW, 700)
        daily = fetch("D", WARMUP, NOW, 3500)

        if not all([m15, h1, h4, daily]):
            raise RuntimeError("Missing required history")
        if len(m15) < 100_000:
            raise RuntimeError(f"Insufficient AUD/USD M15 bars: {len(m15)}")

        write_csv(OUTS["coverage"], [{
            "instrument": PAIR,
            "requested_start_utc": iso(START),
            "actual_first_m15_utc": iso(m15[0]["time"]),
            "actual_last_m15_utc": iso(m15[-1]["time"]),
            "m15_candles": len(m15),
            "h1_candles": len(h1),
            "h4_candles": len(h4),
            "daily_candles": len(daily),
            "baseline_cost_pips": PRIMARY_COST,
            "side": "SHORT",
        }])

        STATUS.update({
            "state": "precompute",
            "message": "Building strict completed-HTF alignment and M15 feature cache",
            "progress": 20,
        })

        m15_times = [x["time"] for x in m15]
        aligned_h1 = align_htf(m15_times, htf_state(h1))
        aligned_h4 = align_htf(m15_times, htf_state(h4))
        aligned_daily = align_htf(m15_times, htf_state(daily))
        f = features(m15, aligned_h1, aligned_h4, aligned_daily)

        # ---------------- Stage 1 ----------------
        STATUS.update({
            "state": "stage1",
            "message": "Stage 1: raw independent short archetypes",
            "progress": 28,
        })

        stage1 = stage1_configs()
        config_by_id = {x["config_id"]: x for x in stage1}
        stage1_rows = []

        for i, config in enumerate(stage1, 1):
            if i % 8 == 0:
                STATUS.update({
                    "state": "stage1",
                    "message": f"Stage 1 {i}/{len(stage1)}",
                    "progress": 28 + int(12 * i / len(stage1)),
                })
            ix = indices(config, f)
            stage1_rows.append(evaluate(config, m15, ix))

        stage1_rows = sort_rows(stage1_rows)
        stage1_family = family_summary(stage1_rows)
        write_csv(OUTS["stage1"], stage1_rows)
        write_csv(OUTS["stage1_family"], stage1_family)

        BACKTEST_CACHE.clear()
        OUTCOME_CACHE.clear()

        stage1_bases = select_diverse(
            stage1_rows,
            STAGE1_PER_FAMILY,
            STAGE1_BASE_KEEP,
        )

        # ---------------- Stage 2 ----------------
        STATUS.update({
            "state": "stage2",
            "message": "Stage 2: broad HTF / volatility / session / weekday contexts",
            "progress": 42,
        })

        stage2 = stage2_configs(stage1_bases, config_by_id)
        config_by_id.update({x["config_id"]: x for x in stage2})
        stage2_rows = []

        for i, config in enumerate(stage2, 1):
            if i % 25 == 0:
                STATUS.update({
                    "state": "stage2",
                    "message": f"Stage 2 {i}/{len(stage2)}",
                    "progress": 42 + int(20 * i / len(stage2)),
                })
            ix = indices(config, f)
            stage2_rows.append(evaluate(config, m15, ix))

        stage2_rows = sort_rows(stage2_rows)
        stage2_family = family_summary(stage2_rows)
        write_csv(OUTS["stage2"], stage2_rows)
        write_csv(OUTS["stage2_family"], stage2_family)

        BACKTEST_CACHE.clear()
        OUTCOME_CACHE.clear()

        stage2_bases = select_diverse(
            stage2_rows,
            STAGE2_PER_FAMILY,
            STAGE2_BASE_KEEP,
        )

        # ---------------- Stage 3 ----------------
        STATUS.update({
            "state": "stage3",
            "message": "Stage 3: local geometry and RR robustness",
            "progress": 64,
        })

        stage3 = stage3_configs(stage2_bases, config_by_id)
        config_by_id.update({x["config_id"]: x for x in stage3})
        stage3_rows = []

        for i, config in enumerate(stage3, 1):
            if i % 25 == 0:
                STATUS.update({
                    "state": "stage3",
                    "message": f"Stage 3 {i}/{len(stage3)}",
                    "progress": 64 + int(15 * i / len(stage3)),
                })
            ix = indices(config, f)
            stage3_rows.append(evaluate(config, m15, ix))

        stage3_rows = sort_rows(stage3_rows)
        stage3_family = family_summary(stage3_rows)
        write_csv(OUTS["stage3"], stage3_rows)
        write_csv(OUTS["stage3_family"], stage3_family)

        BACKTEST_CACHE.clear()
        OUTCOME_CACHE.clear()

        final_rows = select_diverse(
            stage3_rows,
            FINAL_PER_FAMILY,
            FINAL_KEEP,
        )
        final_configs = [config_by_id[row["config_id"]] for row in final_rows]

        # ---------------- Deep diagnostics on shortlist ----------------
        STATUS.update({
            "state": "shortlist_diagnostics",
            "message": "Shortlist temporal / cost / rolling / calendar diagnostics",
            "progress": 80,
        })

        shortlist = []
        periods = []
        costs = []
        rolling = []
        calendar = []
        trades = []

        for i, config in enumerate(final_configs, 1):
            STATUS.update({
                "state": "shortlist_diagnostics",
                "message": f"Shortlist {i}/{len(final_configs)}: {config['config_id']}",
                "progress": 80 + int(15 * i / max(1, len(final_configs))),
            })

            ix = indices(config, f)
            shortlist.append(shortlist_summary(config, m15, ix))
            periods.extend(period_rows(config, m15, ix))
            costs.extend(cost_rows(config, m15, ix))
            rrows = rolling_rows(config, m15, ix)
            crows = calendar_rows(config, m15, ix)
            rolling.extend(rrows)
            calendar.extend(crows)

            for trade in backtest(m15, ix, config["rr"], PRIMARY_COST, m15[0]["time"], NOW):
                trades.append(serialise_trade(config, trade))

        rollsum = rolling_summary(rolling)
        calsum = calendar_summary(calendar)
        decisions = decision_rows(shortlist, costs, rollsum, calsum)

        write_csv(OUTS["shortlist"], shortlist)
        write_csv(OUTS["periods"], periods)
        write_csv(OUTS["cost"], costs)
        write_csv(OUTS["rolling"], rolling)
        write_csv(OUTS["rolling_summary"], rollsum)
        write_csv(OUTS["calendar"], calendar)
        write_csv(OUTS["calendar_summary"], calsum)
        write_csv(OUTS["trades"], trades)
        write_csv(OUTS["decision"], decisions)

        write_csv(OUTS["notes"], [
            {
                "item": "Scope",
                "value": "Fresh AUD/USD M15 SHORT broad research; no existing AUD/USD H1 or M15 LONG rule is a benchmark.",
            },
            {
                "item": "Families",
                "value": "Bearish engulf near highs, high-sweep displacement, failed upside breakout, outside reversal, compression breakdown, and rally rejection are searched independently.",
            },
            {
                "item": "Historical execution",
                "value": "OANDA midpoint; reference entry signal close; short fill=close-1 pip baseline; stop=signal high+10 ticks; target from reference-close risk; p0; exact exit-candle signal eligible.",
            },
            {
                "item": "HTF causality",
                "value": "H1/H4/D values become usable only at the next actual HTF candle open; no same-candle lookahead.",
            },
            {
                "item": "Context search",
                "value": "Stage 2 tests one context at a time: H1/H4/D bearish trend states, ATR regime, NY/London/Tokyo/Sydney 4-hour blocks, and weekday exclusions.",
            },
            {
                "item": "Cost stress",
                "value": "Final shortlist is retested at 0.5, 1.0, 1.5 and 2.0 pip adverse historical entry cost.",
            },
            {
                "item": "No pristine OOS claim",
                "value": "History has been repeatedly explored. Temporal splits and rolling windows are robustness diagnostics, not untouched out-of-sample evidence.",
            },
            {
                "item": "Decision matrix",
                "value": "DEEP_VALIDATE only means a candidate merits a separate frozen deep-validation runner. It is not live approval.",
            },
            {
                "item": "Next gate",
                "value": "After one candidate is frozen and deeply validated, run exact 26->27 portfolio-add analysis with non-hedging same-pair overlap handling before any live integration.",
            },
        ])

        STATUS.update({
            "state": "packaging",
            "message": "Packaging AUD/USD M15 SHORT prospective #27 broad research results",
            "progress": 97,
        })

        package_results()

        verdict_counts = defaultdict(int)
        for row in decisions:
            verdict_counts[row["research_verdict"]] += 1

        STATUS.update({
            "state": "complete",
            "message": "AUD/USD M15 SHORT prospective #27 broad research complete",
            "progress": 100,
            "stage1_configs": len(stage1),
            "stage2_configs": len(stage2),
            "stage3_configs": len(stage3),
            "shortlist_configs": len(final_configs),
            "decision_counts": dict(verdict_counts),
            "bundle": BUNDLE,
        })

    except Exception as error:
        STATUS.update({
            "state": "error",
            "message": str(error),
        })
        import traceback
        STATUS["traceback"] = traceback.format_exc()
        print("ERROR:", repr(error), flush=True)


# ============================================================
# FLASK ROUTES
# ============================================================

@app.route("/")
def root():
    return jsonify({
        "service": "AUDUSD M15 SHORT #27 Broad Research",
        "status": STATUS["state"],
        "instrument": PAIR,
        "timeframe": "M15",
        "side": "SELL",
        "primary_cost_pips": PRIMARY_COST,
        "families": [
            "BEAR_ENGULF_STRUCTURE",
            "HIGH_SWEEP_DISPLACEMENT",
            "FAILED_UPSIDE_BREAKOUT",
            "BEAR_OUTSIDE_REVERSAL",
            "BEAR_COMPRESSION_BREAKDOWN",
            "RALLY_REJECTION",
        ],
        "orders_supported": False,
        "trading_enabled": False,
        "routes": [
            "/audusd-m15-short-27/status",
            "/audusd-m15-short-27/results",
        ],
    })


@app.route("/audusd-m15-short-27/status")
def research_status():
    return jsonify(STATUS)


@app.route("/audusd-m15-short-27/results")
def research_results():
    return download(BUNDLE)


if __name__ == "__main__":
    threading.Thread(
        target=run_research,
        daemon=True,
    ).start()

    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "5000")),
        debug=False,
    )
