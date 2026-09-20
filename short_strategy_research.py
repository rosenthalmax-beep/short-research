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





REF_ID = "S2_1_H1_EMA50_LT_EMA200"
CID = "AUDUSD_M15_SHORT_27_FROZEN_ENGULF_H1_50_LT_200"
REF_LAST_M15 = datetime(2026, 9, 18, 20, 45, tzinfo=timezone.utc)
REF_FIRST_M15 = datetime(2002, 5, 6, 20, 45, tzinfo=timezone.utc)
REF_M15_COUNT = 546849
REF_S2 = {
    "trades": 62,
    "pf": 1.609900,
    "r": 25.0059,
    "dd": -5.8516,
    "winners": 21,  # 33.871% of 62 = 21 wins
    "pre2010_trades": 16,
    "pre2010_r": 4.6654,
    "post2010_trades": 46,
    "post2010_r": 20.3405,
    "era_r": (2.9571, 6.6963, 7.4581, 7.8944),
    "era_trades": (9, 19, 13, 21),
}

# Deliberately narrow one-factor neighbours.  They are DIAGNOSTICS:
# neither a stronger neighbour nor a different RR replaces the frozen case.
NEIGHBOURS = [
    ("ENGULF_RATIO_1_25", "br_min", 1.25),
    ("ENGULF_RATIO_1_45", "br_min", 1.45),
    ("BODY_ATR_0_90", "body_atr_min", 0.90),
    ("BODY_ATR_1_10", "body_atr_min", 1.10),
    ("STRUCTURE_LB_100", "structure_lb", 100),
    ("STRUCTURE_LB_165", "structure_lb", 165),
    ("DISTANCE_ATR_0_075", "structure_dist_atr_max", 0.075),
    ("DISTANCE_ATR_0_125", "structure_dist_atr_max", 0.125),
    ("H1_CLOSE_LT_EMA200", "context", "H1_CLOSE_LT_EMA200"),
    ("H1_CLOSE_LT_EMA100", "context", "H1_CLOSE_LT_EMA100"),
    ("H4_CLOSE_LT_EMA200", "context", "H4_CLOSE_LT_EMA200"),
    ("NO_HTF_TREND", "context", "NONE"),
]

OUTS = {
    "coverage": "audusd27_engulf_confirmation_coverage.csv",
    "parity": "audusd27_engulf_confirmation_frozen_parity.csv",
    "frozen_config": "audusd27_engulf_confirmation_frozen_config.csv",
    "frozen_summary": "audusd27_engulf_confirmation_frozen_summary.csv",
    "periods": "audusd27_engulf_confirmation_periods.csv",
    "cost": "audusd27_engulf_confirmation_cost_stress.csv",
    "rolling": "audusd27_engulf_confirmation_rolling.csv",
    "rolling_summary": "audusd27_engulf_confirmation_rolling_summary.csv",
    "calendar": "audusd27_engulf_confirmation_calendar.csv",
    "calendar_summary": "audusd27_engulf_confirmation_calendar_summary.csv",
    "trades": "audusd27_engulf_confirmation_trades.csv",
    "signal_audit": "audusd27_engulf_confirmation_signal_audit.csv",
    "neighbours": "audusd27_engulf_confirmation_one_factor_neighbours.csv",
    "neighbour_periods": "audusd27_engulf_confirmation_neighbour_periods.csv",
    "neighbour_summary": "audusd27_engulf_confirmation_neighbour_summary.csv",
    "decision": "audusd27_engulf_confirmation_decision.csv",
    "notes": "audusd27_engulf_confirmation_notes.csv",
}
BUNDLE = "AUDUSD_M15_SHORT_27_ENGULFING_FROZEN_CONFIRMATION_RESULTS.zip"
STATUS = {
    "state": "not_started", "message": "Frozen Stage 2 confirmation has not started",
    "orders_supported": False, "trading_enabled": False,
}


def ec_frozen_config():
    # Build from the original discovery's EXACT S1_ENG_7 dictionary,
    # then attach the original S2 context.  No newly invented signal.
    original = next(x for x in stage1_configs() if x["config_id"] == "S1_ENG_7")
    expected = {
        "family": "BEAR_ENGULF_STRUCTURE", "rr": 3.5,
        "br_min": 1.35, "body_atr_min": 1.0,
        "structure_lb": 120, "structure_dist_atr_max": 0.10,
    }
    for key, value in expected.items():
        if original[key] != value:
            raise RuntimeError(f"Underlying discovery rule drift: {key}, {original[key]} != {value}")
    selected = deepcopy(original)
    selected["config_id"] = CID
    selected["context"] = "H1_EMA50_LT_EMA200"
    if selected.get("excluded_weekdays") or selected.get("excluded_ny_hours"):
        raise RuntimeError("Unexpected old weekday/session exclusions")
    return selected


def ec_parity(candles, f, config):
    """Recreate the exact prior dataset through the original last M15 candle.

    The forward fetch may contain extra bars, but it MUST include exactly the
    prior candle chronology before evaluating any prospective incremental data.
    """
    cutoff = bisect.bisect_right([x["time"] for x in candles], REF_LAST_M15)
    subset_candles = candles[:cutoff]
    rows = [{
        "check": "prior_candle_count", "expected": REF_M15_COUNT,
        "actual": len(subset_candles), "pass": len(subset_candles) == REF_M15_COUNT,
    }, {
        "check": "prior_first_open", "expected": iso(REF_FIRST_M15),
        "actual": iso(subset_candles[0]["time"]) if subset_candles else "",
        "pass": bool(subset_candles) and subset_candles[0]["time"] == REF_FIRST_M15,
    }, {
        "check": "prior_last_open", "expected": iso(REF_LAST_M15),
        "actual": iso(subset_candles[-1]["time"]) if subset_candles else "",
        "pass": bool(subset_candles) and subset_candles[-1]["time"] == REF_LAST_M15,
    }]
    if not all(x["pass"] for x in rows):
        write_csv(OUTS["parity"], rows)
        raise RuntimeError("Previous M15 candle coverage differs from original discovery; STOP before new analysis")

    ix = [i for i in indices(config, f) if i < cutoff]
    # Important: evaluate outcomes against the historical cutoff candles,
    # NOT later future bars that were unavailable to the prior run.
    BACKTEST_CACHE.clear()
    OUTCOME_CACHE.clear()
    trades = backtest(subset_candles, ix, config["rr"], PRIMARY_COST)
    s = stats(trades)
    pre = stats([x for x in trades if x["entry_time"] < datetime(2010, 1, 1, tzinfo=timezone.utc)])
    post = stats([x for x in trades if x["entry_time"] >= datetime(2010, 1, 1, tzinfo=timezone.utc)])
    era_stats = [stats([t for t in trades if start <= t["entry_time"] < stop])
                 for _, start, stop in ERAS]
    checks = [
        ("trades", REF_S2["trades"], s["trades"], 0),
        ("winners", REF_S2["winners"], s["winners"], 0),
        ("pf", REF_S2["pf"], s["profit_factor"], .000051),
        ("r", REF_S2["r"], s["total_r"], .000051),
        ("dd", REF_S2["dd"], s["max_drawdown_r"], .000051),
        ("pre2010_trades", REF_S2["pre2010_trades"], pre["trades"], 0),
        ("pre2010_r", REF_S2["pre2010_r"], pre["total_r"], .000051),
        ("post2010_trades", REF_S2["post2010_trades"], post["trades"], 0),
        ("post2010_r", REF_S2["post2010_r"], post["total_r"], .000051),
    ]
    for j, era in enumerate(era_stats):
        checks.extend([
            (f"era{j+1}_trades", REF_S2["era_trades"][j], era["trades"], 0),
            (f"era{j+1}_r", REF_S2["era_r"][j], era["total_r"], .000051),
        ])
    rows += [{"check": key, "expected": expected, "actual": actual,
              "tolerance": tolerance, "pass": abs(actual-expected) <= tolerance}
             for key, expected, actual, tolerance in checks]
    write_csv(OUTS["parity"], rows)
    if not all(x["pass"] for x in rows):
        fail = [x["check"] for x in rows if not x["pass"]]
        raise RuntimeError(f"Frozen discovery parity failed ({', '.join(fail)}); DO NOT select a replacement configuration")
    BACKTEST_CACHE.clear()
    OUTCOME_CACHE.clear()
    return rows


# ============================================================
# AUD/USD M15 SHORT #27 — FROZEN CORE FREQUENCY EXPANSION
# ============================================================
# NO changes to frozen S2 engulfing rule / stop / target / 1-pip cost / H1.
# Three small, predetermined one-factor relaxations, followed by six
# structurally distinct complements. NO session / weekday / RR optimisation.
# Additional accepted trades are evaluated independently; combined results
# are fully rebacktested in chronological order, one p0 over both signals.
# Core has SAME-CANDLE priority, not immunity from an earlier open complement.
# Previously searched dates (including 2018+) are NOT untouched OOS.
# The confirmation baseline must match BOTH original summary and 62 trade IDs.
# Research-only: NO OANDA orders, NO portfolio/live deployment.
# ============================================================
import hashlib
import traceback

FREQ_NAME = 'AUDUSD_M15_SHORT_27_FROZEN_CORE_FREQUENCY_EXPANSION'
FREQ_BUNDLE = FREQ_NAME + '_RESULTS.zip'
FREQ_CORE_FINGERPRINT = 'b51089aff76fbcbc9ec67af33f341aa9c3cee4471f24875476b854a72b056a8f'
FREQ_OUTS = {k: f'audusd27_freq_{v}.csv' for k,v in {
    'coverage':'coverage', 'parity':'frozen_parity', 'rule':'rules',
    'rejections':'core_rejection_audit', 'matrix':'predeclared_matrix',
    'periods':'combined_and_incremental_periods',
    'cost':'cost_stress', 'rolling':'combined_rolling',
    'rolling_summary':'combined_rolling_summary', 'calendar':'combined_calendar',
    'calendar_summary':'combined_calendar_summary',
    'trades':'combined_accepted_trades',
    'new_standalone':'additional_only_standalone_trades',
    'displaced':'core_trade_displacements',
    'neighbour':'predeclared_neighbour_diagnostics',
    'decisions':'decision_matrix', 'notes':'interpretation_notes',
}.items()}
# Original parity routine must write to THIS study, not overwrite the previous
# frozen-confirmation results when deployed in a shared working directory.
OUTS['parity'] = FREQ_OUTS['parity']

# 1-factor relaxations are NOT independent newly discovered strategies.
# Two-factor combination is included only as a stress diagnostic and cannot
# become the automatic selected winner.
FREQ_RELAX = [
    ('R_BR125', {'br_min':1.25}, True),
    ('R_BODY090', {'body_atr_min':0.90}, True),
    ('R_DIST0125', {'structure_dist_atr_max':0.125}, True),
    ('R_BR125_BODY090_DIAGNOSTIC', {'br_min':1.25,'body_atr_min':0.90}, False),
]

# Two predeclared representatives per distinctive family. The original
# discovery previously examined these families: none is genuine independent
# OOS. All share the FROZEN, completed-H1 bearish regime and original RR.
FREQ_COMPLEMENTS = [
    ('C_SWEEP60', 'HIGH_SWEEP_DISPLACEMENT',
     {'sweep_lb':60,'body_atr_min':1.00,'upper_wick_body_min':0.25,'mom4_min':0.75}),
    ('C_SWEEP40', 'HIGH_SWEEP_DISPLACEMENT',
     {'sweep_lb':40,'body_atr_min':1.00,'upper_wick_body_min':0.25,'mom4_min':0.50}),
    ('C_FAILED60', 'FAILED_UPSIDE_BREAKOUT',
     {'sweep_lb':60,'body_atr_min':1.00,'close_loc_max':0.30}),
    ('C_FAILED40', 'FAILED_UPSIDE_BREAKOUT',
     {'sweep_lb':40,'body_atr_min':0.75,'close_loc_max':0.30}),
    ('C_RALLY40', 'RALLY_REJECTION',
     {'sweep_lb':40,'body_atr_min':0.75,'mom4_min':1.00,'close_loc_max':0.30}),
    ('C_RALLY60', 'RALLY_REJECTION',
     {'sweep_lb':60,'body_atr_min':1.00,'mom4_min':1.25,'close_loc_max':0.30}),
]

FREQ_STATUS = {
    'state':'not_started', 'message':'Frequency research not started',
    'trading_enabled':False, 'orders_supported':False, 'live_strategy_count_unchanged':26,
    'frozen_core':'S2_1_H1_EMA50_LT_EMA200',
}
# Reuse the source fetcher's per-chunk progress in this study's /status.
STATUS = FREQ_STATUS


def freq_set_status(state, progress, message):
    FREQ_STATUS.update(state=state,progress=progress,message=message)
    print(f'FREQUENCY [{progress}%] {state}: {message}',flush=True)


def freq_package():
    with zipfile.ZipFile(FREQ_BUNDLE,'w',zipfile.ZIP_DEFLATED) as z:
        for path in FREQ_OUTS.values():
            if os.path.exists(path):
                z.write(path,arcname=os.path.basename(path))


def freq_core_hash(trades):
    serial='\n'.join(
        f'{t["entry_time_utc"]}|{t["exit_time_utc"]}|{t["exit_reason"]}|{float(t["result_r"]):.10f}'
        for t in trades
    )
    return hashlib.sha256(serial.encode('utf-8')).hexdigest()


def freq_parity(candles, f, frozen):
    # The original source's twenty numeric/coverage checks, evaluated strictly
    # on the old end-candle horizon, then a new 62-trade identity fingerprint.
    rows=ec_parity(candles,f,frozen)
    cutoff=bisect.bisect_right([x['time'] for x in candles],REF_LAST_M15)
    original_candles=candles[:cutoff]
    ix=[i for i in indices(frozen,f) if i<cutoff]
    original_trades=backtest(original_candles,ix,frozen['rr'],PRIMARY_COST)
    actual=freq_core_hash(original_trades)
    identity={
        'check':'original_62_trade_fingerprint',
        'expected':FREQ_CORE_FINGERPRINT,'actual':actual,
        'pass':actual==FREQ_CORE_FINGERPRINT,
        'trade_count':len(original_trades),
        'fingerprint_fields':'entry_UTC|exit_UTC|exit_reason|R_to_10_decimals',
    }
    rows.append(identity)
    write_csv(FREQ_OUTS['parity'],rows)
    if not identity['pass']:
        raise RuntimeError('Frozen 62-trade identity changed; STOP: no expansion analysis')
    OUTCOME_CACHE.clear(); BACKTEST_CACHE.clear()
    return rows


def freq_rules(frozen):
    rules=[{'id':'FROZEN_CORE', 'kind':'original_exact','eligible_for_selection':False,
            'design':'Never change original Core A',**config_fields(frozen),
            'context':frozen['context'],'rr':frozen['rr']}]
    candidates=[]
    for name,delta,eligible in FREQ_RELAX:
        c=deepcopy(frozen)
        c['config_id']=name
        c.update(delta)
        candidates.append((name,'RELAXATION',c,eligible,delta))
        rules.append({'id':name,'kind':'RELAXATION',
                      'eligible_for_selection':eligible,'changed_fields':repr(delta),
                      'context':c['context'],'rr':c['rr'],**config_fields(c)})
    for name,family,geometry in FREQ_COMPLEMENTS:
        c=cfg(name,family,rr=frozen['rr'],context=frozen['context'],**geometry)
        candidates.append((name,'COMPLEMENT',c,True,geometry))
        rules.append({'id':name,'kind':'COMPLEMENT',
                      'eligible_for_selection':True,'changed_fields':repr(geometry),
                      'context':c['context'],'rr':c['rr'],**config_fields(c)})
    assert len(candidates)==10 and len({c[0] for c in candidates})==10
    return rules,candidates


def freq_rejection_audit(f):
    '''Interpretations: cumulative gates and independent failures are different.
    Independent failures may overlap, and do NOT add to the total rejected.
    Rejection counts are M15 raw candle signals, not p0 accepted trades.
    '''
    n=f['n']; warm=np.arange(n)>=200
    h1=np.isfinite(f['h1_ema50']) & np.isfinite(f['h1_ema200'])
    scope=warm & f['valid_atr'] & h1 & f['exact_bear']
    base={
        'engulf_ratio_ge_1_35':f['bear_br']>=1.35,
        'body_ge_1_ATR':f['body_atr']>=1.00,
        'near_prior120_high_0_10_ATR':f['structure_dist_high'][120]<=0.10,
        'completed_H1_EMA50_below_EMA200':f['h1_ema50']<f['h1_ema200'],
    }
    result=[{'step':'scope_exact_bearish_engulf_with_valid_features',
             'sequential_survivors':int(scope.sum()),
             'failed_independently_within_scope':0}]
    mask=scope.copy()
    for title,gate in base.items():
        prior=int(mask.sum())
        mask &= gate
        result.append({'step':title,'sequential_before':prior,
                       'sequential_survivors':int(mask.sum()),
                       'failed_at_this_sequential_step':prior-int(mask.sum()),
                       'failed_independently_within_scope':int((scope & ~gate).sum()),
                       'failed_exclusively_this_gate':int((scope & ~gate &
                           np.logical_and.reduce([x for k,x in base.items() if k!=title])).sum()),
                      })
    result.append({'step':'all_frozen_raw_signals',
                   'sequential_survivors':int(mask.sum())})
    return result


def freq_union(candles, frozen_ix, novel_ix, cost):
    '''Recalculate from raw indices with CORE same-candle priority and one p0.
    Half-open [signal_index,exit_index) permits signal ON the exit candle.
    Not the sum/merge of two independently pyramided trade ledgers.
    '''
    if frozen_ix != sorted(set(frozen_ix)) or novel_ix != sorted(set(novel_ix)):
        raise ValueError('Raw indices must be unique sorted integers')
    core=set(frozen_ix); novel=set(novel_ix)
    accepted=[]; blocked={'core':0,'new':0}
    until=-1
    for i in sorted(core|novel):
        source='FROZEN_CORE' if i in core else 'INCREMENTAL'
        if i<until:
            blocked['core' if source=='FROZEN_CORE' else 'new']+=1
            continue
        trade=outcome(candles,i,3.5,cost)
        if trade is None:
            continue
        t=dict(trade)
        t['source']=source
        t['same_candle_both']=bool(i in core and i in novel)
        accepted.append(t)
        until=int(t['exit_index'])
    assert all(x['exit_index']<=y['signal_index'] for x,y in zip(accepted,accepted[1:]))
    assert len({x['signal_index'] for x in accepted})==len(accepted)
    info={
        'raw_frozen_signals':len(core),'raw_variant_signals':len(novel),
        'raw_exclusive_incremental_signals':len(novel-core),
        'raw_both_same_candle':len(core&novel),
        'raw_union_signals':len(core|novel),
        'accepted_frozen_core':sum(x['source']=='FROZEN_CORE' for x in accepted),
        'accepted_incremental':sum(x['source']=='INCREMENTAL' for x in accepted),
        'blocked_core_by_p0':blocked['core'],
        'blocked_incremental_by_p0':blocked['new'],
    }
    assert len(accepted)==info['accepted_frozen_core']+info['accepted_incremental']
    return accepted,info


def freq_slice(trades,start=None,stop=None):
    return [t for t in trades
            if (start is None or start<=t['entry_time'])
            and (stop is None or t['entry_time']<stop)]


def freq_windows(candles):
    return [
        ('FULL',candles[0]['time'],NOW),
        ('PRE_2010',candles[0]['time'],datetime(2010,1,1,tzinfo=timezone.utc)),
        ('POST_2010',datetime(2010,1,1,tzinfo=timezone.utc),NOW),
        ('2018_PLUS',datetime(2018,1,1,tzinfo=timezone.utc),NOW),
        ('2020_PLUS',datetime(2020,1,1,tzinfo=timezone.utc),NOW),
        ('LAST_5Y',NOW-timedelta(days=365.2425*5),NOW),
        ('LAST_3Y',NOW-timedelta(days=365.2425*3),NOW),
        ('LAST_2Y',NOW-timedelta(days=365.2425*2),NOW),
        ('LAST_1Y',NOW-timedelta(days=365.2425),NOW),
    ]


def freq_period_metrics(name,kind,base,combined,exclusive_standalone,windows,cost):
    rows=[]
    for window,a,b in windows:
        before=freq_slice(base,a,b)
        after=freq_slice(combined,a,b)
        inc=[t for t in after if t['source']=='INCREMENTAL']
        core=[t for t in after if t['source']=='FROZEN_CORE']
        isolated=freq_slice(exclusive_standalone,a,b)
        old_s=stats(before); new_s=stats(after)
        increment_s=stats(inc)
        alone_s=stats(isolated)
        rows.append({
            'config_id':name,'kind':kind,'cost_pips':cost,'period':window,
            'start_utc':iso(a),'end_utc':iso(b),
            'baseline_trades':old_s['trades'],'baseline_r':old_s['total_r'],
            'combined_trades':new_s['trades'],'combined_pf':new_s['profit_factor'],
            'combined_r':new_s['total_r'],'combined_dd_r':new_s['max_drawdown_r'],
            'net_delta_r':new_s['total_r']-old_s['total_r'],
            'net_added_trades':new_s['trades']-old_s['trades'],
            'retained_core_trades':len(core),
            'incremental_accepted_trades':increment_s['trades'],
            'incremental_accepted_pf':increment_s['profit_factor'],
            'incremental_accepted_r':increment_s['total_r'],
            'incremental_accepted_dd_r':increment_s['max_drawdown_r'],
            'incremental_only_p0_trades':alone_s['trades'],
            'incremental_only_p0_pf':alone_s['profit_factor'],
            'incremental_only_p0_r':alone_s['total_r'],
            'incremental_only_p0_dd_r':alone_s['max_drawdown_r'],
        })
    return rows


def freq_rolling(name,trades,first):
    out=[]; start0=month_floor(max(first,START)); last=month_floor(NOW)
    for months in (12,24,36):
        start=start0
        while add_months(start,months)<=last:
            end=add_months(start,months)
            s=stats(freq_slice(trades,start,end))
            out.append({'config_id':name,'months':months,
                        'start_utc':iso(start),'end_utc':iso(end),
                        'trades':s['trades'],'total_r':round(s['total_r'],4),
                        'profit_factor':round(s['profit_factor'],6),
                        'positive':s['total_r']>0,'zero_trade':s['trades']==0})
            start=add_months(start,1)
    return out


def freq_calendar(name,trades,first):
    out=[]
    for year in range(max(START.year,first.year),NOW.year):
        a=datetime(year,1,1,tzinfo=timezone.utc)
        b=datetime(year+1,1,1,tzinfo=timezone.utc)
        s=stats(freq_slice(trades,a,b))
        out.append({'config_id':name,'year':year,'trades':s['trades'],
                    'profit_factor':round(s['profit_factor'],6),
                    'total_r':round(s['total_r'],4),
                    'positive':s['total_r']>0,'negative':s['total_r']<0,
                    'zero_trade':s['trades']==0})
    return out


def freq_decision(name,kind,eligible,matrix,one,two,rolls,cal,neighbour):
    base={x['period']:x for x in one}
    c2={x['period']:x for x in two}
    rs={x['months']:x for x in rolls}
    cs=cal[0]
    full=base['FULL']; val=base['2018_PLUS']; last5=base['LAST_5Y']; last2=base['LAST_2Y']
    full2=c2['FULL']; val2=c2['2018_PLUS']
    # No PF-only selection. Net portfolio effect must be checked separately.
    # 100–150 historical trades is a frequency OBJECTIVE, not a target to fit.
    checks={
        'at_least_20_new_exclusive_raw_signals':matrix['raw_exclusive_incremental_signals']>=20,
        'at_least_15_accepted_additions':full['incremental_accepted_trades']>=15,
        'at_least_4_new_accepted_last5y':last5['incremental_accepted_trades']>=4,
        'new_accepted_full_positive':full['incremental_accepted_r']>0,
        'new_accepted_2018_plus_positive':val['incremental_accepted_r']>0,
        'new_accepted_last5_positive':last5['incremental_accepted_r']>0,
        'new_only_full_pf_at_least_1_15':full['incremental_only_p0_pf']>=1.15,
        'new_only_full_r_positive':full['incremental_only_p0_r']>0,
        'combined_2018_net_delta_positive':val['net_delta_r']>0,
        'combined_last5_net_delta_positive':last5['net_delta_r']>0,
        'combined_last2_net_delta_nonnegative':last2['net_delta_r']>=0,
        'combined_full_net_delta_positive':full['net_delta_r']>0,
        'combined_drawdown_not_below_minus_12r':full['combined_dd_r']>=-12,
        'at_least_60_retained_core_trades':full['retained_core_trades']>=60,
        'double_cost_new_accepted_full_positive':full2['incremental_accepted_r']>0,
        'double_cost_new_only_p0_full_positive':full2['incremental_only_p0_r']>0,
        'double_cost_2018_net_delta_positive':val2['net_delta_r']>0,
        'rolling24_positive_active_at_least_70pct':rs[24]['positive_active_windows_pct']>=70,
        'rolling36_positive_active_at_least_80pct':rs[36]['positive_active_windows_pct']>=80,
        'rolling24_worst_not_below_minus_10r':rs[24]['worst_r']>=-10,
        'rolling36_worst_not_below_minus_12r':rs[36]['worst_r']>=-12,
        'at_least_55pct_positive_active_calendar_years':cs['positive_active_years_pct']>=55,
    }
    # Predeclared related geometry cannot authorise a swap to a new winner.
    # A single variant with a sharp numeric edge gets a warning.
    if neighbour:
        checks['paired_geometry_positive_new_accepted_full']=neighbour['incremental_accepted_r']>0
        checks['paired_geometry_positive_2018_net_delta']=neighbour['validation_delta_r']>0
    passed=sum(bool(x) for x in checks.values())
    return {
        'config_id':name,'kind':kind,'eligible_for_promotion':eligible,
        'research_gate':('WORTH_SEPARATE_PORTFOLIO_TEST_NOT_LIVE_APPROVAL'
                         if eligible and all(checks.values()) else
                         'DIAGNOSTIC_ONLY_NOT_PROMOTABLE' if not eligible else
                         'NO_EXPANSION_APPROVAL'),
        'checks_passed':passed,'checks_total':len(checks),
        'full_trades':full['combined_trades'],
        'full_accepted_additions':full['incremental_accepted_trades'],
        'full_delta_r':full['net_delta_r'],
        'new_accepted_full_r':full['incremental_accepted_r'],
        'new_only_full_r':full['incremental_only_p0_r'],
        'recent2_delta_r':last2['net_delta_r'],
        'double_cost_delta_r':full2['net_delta_r'],
        'rolling24_pct':rs[24]['positive_active_windows_pct'],
        'rolling36_pct':rs[36]['positive_active_windows_pct'],
        'rolling24_worst_r':rs[24]['worst_r'],
        'rolling36_worst_r':rs[36]['worst_r'],
        **{f'gate_{k}':bool(v) for k,v in checks.items()},
    }


def freq_run():
    try:
        freq_set_status('fetch',1,'Fetching complete M15 and completed H1/H4/D midpoint bars')
        # Global STATUS serves the source fetch() progress; no live routing uses it.
        m15=fetch('M15',START,NOW,35)
        h1=fetch('H1',WARMUP,NOW,180)
        h4=fetch('H4',WARMUP,NOW,700)
        d=fetch('D',WARMUP,NOW,3500)
        if not all([m15,h1,h4,d]) or len(m15)<REF_M15_COUNT:
            raise RuntimeError('OANDA coverage insufficient for frozen parity')
        write_csv(FREQ_OUTS['coverage'],[{
            'pair':PAIR,'side':'SELL','granularity':'M15',
            'm15':len(m15),'h1':len(h1),'h4':len(h4),'d':len(d),
            'first_m15_utc':iso(m15[0]['time']),
            'last_m15_utc':iso(m15[-1]['time']),
            'original_cutoff_utc':iso(REF_LAST_M15),
            'request_cutoff_utc':iso(NOW),
            'p0':'one_position_total_core_plus_complement',
            'historical_adverse_entry_cost_pips':PRIMARY_COST,
        }])
        freq_set_status('features',21,'Original features; strictly completed H1 EMA trend')
        ts=[b['time'] for b in m15]
        f=features(m15,align_htf(ts,htf_state(h1)),
                   align_htf(ts,htf_state(h4)),align_htf(ts,htf_state(d)))
        core=ec_frozen_config()
        freq_set_status('parity',30,'Checking 20 original gates and 62 exact trade identities')
        parity=freq_parity(m15,f,core)
        core_ix=indices(core,f)
        core_trades=backtest(m15,core_ix,3.5,PRIMARY_COST)
        core2=backtest(m15,core_ix,3.5,2.0)
        frozen_trade_map={t['signal_index']:t for t in core_trades}
        rule_rows,candidates=freq_rules(core)
        write_csv(FREQ_OUTS['rule'],rule_rows)
        write_csv(FREQ_OUTS['rejections'],freq_rejection_audit(f))
        windows=freq_windows(m15)
        rolling=freq_rolling('FROZEN_CORE',core_trades,m15[0]['time'])
        calendar=freq_calendar('FROZEN_CORE',core_trades,m15[0]['time'])
        matrix=[]; periods=[]; costs=[]; trades=[]; alone_trades=[]
        displaced=[]; decisions=[]; pairs=[]
        rollsummary=rolling_summary(rolling)
        calsummary=calendar_summary(calendar)
        baseline_periods=freq_period_metrics('FROZEN_CORE','BASELINE',core_trades,
                                             [dict(t,source='FROZEN_CORE') for t in core_trades],
                                             [],windows,1.0)
        periods.extend(baseline_periods)
        freqs={} ; source_diagnostics={}
        freq_set_status('candidates',40,'Testing 10 frozen-core expansions, no parameter search')
        for j,(name,kind,c,eligible,delta) in enumerate(candidates,1):
            raw=indices(c,f)
            extra=sorted(set(raw)-set(core_ix))
            new_only=backtest(m15,extra,3.5,PRIMARY_COST)
            new_only2=backtest(m15,extra,3.5,2.0)
            union,info=freq_union(m15,core_ix,raw,PRIMARY_COST)
            union2,info2=freq_union(m15,core_ix,raw,2.0)
            result=stats(union)
            old=stats(core_trades)
            prior_core=set(frozen_trade_map)
            current_core={t['signal_index'] for t in union if t['source']=='FROZEN_CORE'}
            lost=prior_core-current_core
            new_core=current_core-prior_core
            for idx in sorted(lost):
                t=frozen_trade_map[idx]
                displaced.append({'config_id':name,'type':'LOST_FROZEN_CORE',
                                  'signal_index':idx,'entry_time_utc':t['entry_time_utc'],
                                  'original_result_r':t['result_r'],
                                  'note':'Earlier combined trade can block previously accepted original core'})
            for idx in sorted(new_core):
                t=next(t for t in union if t['signal_index']==idx)
                displaced.append({'config_id':name,'type':'NEWLY_ELIGIBLE_CORE',
                                  'signal_index':idx,'entry_time_utc':t['entry_time_utc'],
                                  'original_result_r':t['result_r']})
            full=freq_period_metrics(name,kind,core_trades,union,new_only,windows,1.0)
            doubled=freq_period_metrics(name,kind,core2,union2,new_only2,windows,2.0)
            periods.extend(full)
            costs.extend([x for x in doubled if x['period'] in ('FULL','2018_PLUS','LAST_5Y','LAST_2Y')])
            one={x['period']:x for x in full}
            matrix_row={
                'config_id':name,'kind':kind,'eligible_for_promotion':eligible,
                'changed_fields':repr(delta),'context':c['context'],
                **info,
                'frozen_p0_baseline_accepted':len(core_trades),
                'displaced_baseline_core_count':len(lost),
                'displaced_baseline_core_r':sum(frozen_trade_map[i]['result_r'] for i in lost),
                'newly_accepted_core_count':len(new_core),
                'standalone_new_only_accepted':len(new_only),
                'standalone_new_only_r':stats(new_only)['total_r'],
                'full_combined_trades':len(union),
                'full_combined_r':result['total_r'],
                'full_combined_pf':result['profit_factor'],
                'full_combined_dd_r':result['max_drawdown_r'],
                'full_frozen_r':old['total_r'],
                'full_delta_r':result['total_r']-old['total_r'],
                'new_accepted_r':one['FULL']['incremental_accepted_r'],
                'last5_delta_r':one['LAST_5Y']['net_delta_r'],
                'last2_delta_r':one['LAST_2Y']['net_delta_r'],
                'double_cost_combined_r':stats(union2)['total_r'],
                'double_cost_delta_r':stats(union2)['total_r']-stats(core2)['total_r'],
                'double_cost_additions_r':sum(x['result_r'] for x in union2 if x['source']=='INCREMENTAL'),
                'double_cost_incremental_newonly_r':stats(new_only2)['total_r'],
            }
            matrix.append(matrix_row)
            rolls=freq_rolling(name,union,m15[0]['time'])
            rolling.extend(rolls)
            rs=rolling_summary(rolls)
            rollsummary.extend(rs)
            cal=freq_calendar(name,union,m15[0]['time'])
            calendar.extend(cal)
            cs=calendar_summary(cal)
            calsummary.extend(cs)
            for t in union:
                trades.append({**serialise_trade(c,t),'config_id':name,
                               'source':t['source'],
                               'same_candle_both':t['same_candle_both']})
            for t in new_only:
                alone_trades.append({**serialise_trade(c,t),'config_id':name,
                                     'source':'STANDALONE_NEW_ONLY'})
            source_diagnostics[name]={'matrix':matrix_row,'full':full,'cost2':doubled,
                                      'rolls':rs,'calendar':cs,'eligible':eligible,
                                      'kind':kind}
            freq_set_status('candidates',40+int(49*j/len(candidates)),
                            f'{j}/{len(candidates)} {name} — +{info["accepted_incremental"]} accepted, '
                            f'net {matrix_row["full_delta_r"]:+.2f}R')
        # Predeclared related-geometry pairing is a diagnostic, not permission
        # to retune the original or promote the better-looking paired variant.
        pair_map={
            'C_SWEEP60':'C_SWEEP40','C_SWEEP40':'C_SWEEP60',
            'C_FAILED60':'C_FAILED40','C_FAILED40':'C_FAILED60',
            'C_RALLY40':'C_RALLY60','C_RALLY60':'C_RALLY40',
        }
        for name,kind,c,eligible,delta in candidates:
            diag=source_diagnostics[name]
            paired=source_diagnostics[pair_map[name]] if name in pair_map else None
            paired_dict=None
            if paired:
                p={x['period']:x for x in paired['full']}
                paired_dict={'config_id':name,'paired_config_id':pair_map[name],
                             'incremental_accepted_r':p['FULL']['incremental_accepted_r'],
                             'validation_delta_r':p['2018_PLUS']['net_delta_r'],
                             'paired_full_r':p['FULL']['combined_r'],
                             'paired_full_delta_r':p['FULL']['net_delta_r'],
                             'paired_last2_delta_r':p['LAST_2Y']['net_delta_r'],
                             'not_a_selected_replacement':True}
                pairs.append(paired_dict)
            decisions.append(freq_decision(name,kind,eligible,diag['matrix'],
                         diag['full'],diag['cost2'],diag['rolls'],diag['calendar'],paired_dict))
        writes=[('matrix',matrix),('periods',periods),('cost',costs),
                ('rolling',rolling),('rolling_summary',rollsummary),
                ('calendar',calendar),('calendar_summary',calsummary),
                ('trades',trades),('new_standalone',alone_trades),
                ('displaced',displaced),('neighbour',pairs),('decisions',decisions)]
        freq_set_status('packaging',92,'Writing full incremental, conflicts, cost, rolling and calendar CSVs')
        for k,rows in writes:
            write_csv(FREQ_OUTS[k],rows)
        write_csv(FREQ_OUTS['notes'],[
            {'item':'scope','value':'Frozen 62-trade AUDUSD M15 SHORT core plus 3 one-factor relaxations, 1 nonpromotable two-factor diagnostic, 6 independent-geometries-with-same-H1-regime complements.'},
            {'item':'source','value':'Original AUDUSD_M15_SHORT_27_BROAD_DISCOVERY.py signal/HTF/outcome; original AUDUSD_M15_SHORT_27_ENGULFING_FROZEN_CONFIRMATION.py parity.'},
            {'item':'core','value':'Exact bearish engulf >=1.35, body>=ATR14, dist prior120 high<=.10ATR, previous completed H1 EMA50<EMA200, RR3.5.'},
            {'item':'price','value':'Historical SHORT midpoint signal reference close, synthetic adverse entry 1.0/2.0pip, stop=high+10ticks, target from reference risk; no execution-level spread measurement.'},
            {'item':'priority','value':'Same-candle frozen-core priority, combined p0 across both, half-open holding intervals; earlier new trades may displace old accepted core.'},
            {'item':'incremental','value':'New raw signals exclude frozen raw signal indices; new-only backtest has its own p0; incremental accepted trades are those labelled INCREMENTAL in the rebacktested union. Portfolio net delta includes lost core.'},
            {'item':'cost','value':'At 2 pips all stop/target outcomes and p0 acceptance are recomputed, not merely scaled from 1pip.'},
            {'item':'no_curvfit','value':'No re-ranking/retuning thresholds, RR, session, weekday or HTF; all 10 variants declared in source before results. All historical periods used in prior discovery: NO untouched OOS.'},
            {'item':'decision','value':'WORTH_SEPARATE_PORTFOLIO_TEST means 26->27 exact integration study required; no live deployment from this runner. No forced frequency minimum of 100 trades.'},
            {'item':'live','value':'Existing 26 strategy executor and orders NOT modified.'},
        ])
        freq_package()
        passing=[x['config_id'] for x in decisions if x['research_gate']=='WORTH_SEPARATE_PORTFOLIO_TEST_NOT_LIVE_APPROVAL']
        FREQ_STATUS.update(state='complete',progress=100,bundle=FREQ_BUNDLE,
                           original_62_trade_parity_pass=all(x['pass'] for x in parity),
                           candidate_count=len(candidates),
                           passed_research_gate=passing,
                           message='Frequency expansion research complete; 26 live unchanged')
        print('COMPLETE',FREQ_STATUS,flush=True)
    except Exception as ex:
        FREQ_STATUS.update(state='error',message=str(ex),traceback=traceback.format_exc())
        print('AUD/USD SHORT #27 FREQ ERROR',traceback.format_exc(),flush=True)


@app.route('/audusd-m15-short-27-frequency/status')
def freq_status():
    return jsonify(FREQ_STATUS)


@app.route('/audusd-m15-short-27-frequency/results')
def freq_results():
    return download(FREQ_BUNDLE)


@app.route('/audusd-m15-short-27-frequency/info')
def freq_info():
    return jsonify({
        'instrument':PAIR,'timeframe':'M15','side':'SELL','research_only':True,
        'live_strategy_count_unchanged':26,
        'frozen_candidate':CID,
        'original_62_trade_fingerprint':FREQ_CORE_FINGERPRINT,
        'baseline_cost_pips':PRIMARY_COST,'double_cost_pips':2.0,
        'one_factor_relaxations':len([r for r in FREQ_RELAX if r[2]]),
        'nonpromotable_two_factor_diagnostics':len([r for r in FREQ_RELAX if not r[2]]),
        'distinct_family_complements':len(FREQ_COMPLEMENTS),
        'read_only':True,'orders_supported':False,
    })


@app.route('/')
def freq_root():
    return jsonify({'service':FREQ_NAME,'research_only':True,
                    'routes':['/audusd-m15-short-27-frequency/status',
                              '/audusd-m15-short-27-frequency/results',
                              '/audusd-m15-short-27-frequency/info']})


if __name__=='__main__':
    threading.Thread(target=freq_run,daemon=True).start()
    app.run(host='0.0.0.0',port=int(os.getenv('PORT','5000')),debug=False)
