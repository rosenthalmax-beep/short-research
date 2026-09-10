import os, csv, time, bisect, zipfile, threading
from copy import deepcopy
from collections import deque, defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median
from pathlib import Path
from zoneinfo import ZoneInfo
from itertools import product

import numpy as np
import requests
from flask import Flask, jsonify, send_file

# ============================================================
# EUR/GBP M15 SHORT — COMPLEMENTARY FREQUENCY SEARCH
#
# PURPOSE
#   Freeze the FINAL 44-trade EUR/GBP M15 SHORT high-sweep core
#   exactly, then search only for a structurally different Trigger B
#   that can raise trade frequency without watering down the edge.
#
# FROZEN CORE — NEVER OPTIMISE / LOOSEN
#   family: HIGH_SWEEP_REJECTION
#   bearish M15 candle
#   current high > previous 120-bar high (current excluded)
#   close back below previous 120-bar high
#   body >= 1.25 ATR14
#   close location <= 0.20
#   upper wick/body >= 0.40
#   exclude Wednesday in Europe/London
#   no session filter
#   no H1/H4/D trend filter
#   RR 4.50
#   stop = signal high + 10 ticks
#   short adverse historical fill = signal close - 1 pip
#   pyramiding 0
#
# FINAL CORE VALIDATION SNAPSHOT
#   44 trades / 20 winners / 24 losers
#   PF 3.455 / +58.93R / max DD -6R
#   2-pip stress PF ~3.20 / +52.88R
#
# COMPLEMENT SEARCH — STRUCTURALLY DIFFERENT FAMILIES ONLY
#   1) COMPRESSION_BREAKDOWN
#   2) BEAR_ENGULF_STRUCTURE
#   3) BEAR_OUTSIDE_REVERSAL
#   4) RALLY_FAILURE_BREAKDOWN
#
# Deliberately NOT searched:
#   - HIGH_SWEEP_REJECTION (the frozen core family)
#   - failed-upside-breakout/rejection variants that are effectively
#     the same structural hypothesis as the frozen core
#
# ANTI-OVERFIT DESIGN
#   - Stage 1: broad clean geometry at fixed RR3.50, NO context
#   - Stage 2: only strong geometries receive simple causal contexts
#   - Stage 3: only survivors receive RR confirmation + 2-pip guard
#   - no scoring reward for filling 2005 / 2009 / 2013 / 2023
#   - those inactive years are REPORT-ONLY diagnostics
#   - 2002-2004 are explicitly early-history non-target years
#   - no individual weekday optimisation in the complement search
#   - prefer >= roughly 12-15 genuinely new trades with standalone
#     pre/post and era evidence, not a tiny lucky add-on
#
# STAGE-2 CONTEXTS
#   NONE
#   previous completed H1 close < EMA100
#   previous completed H1 EMA50 < EMA200
#   previous completed H4 close < EMA100
#   previous completed daily close < EMA200
#   broad London 04-07 / 08-11 blocks
#   broad New York 00-03 / 04-07 blocks
#
# OVERLAP RULE
#   Candidate trades are first backtested independently (pyramiding0).
#   A candidate interval [signal_index, exit_index) is rejected if it
#   overlaps a frozen core trade. Exact core exit-candle signal remains
#   eligible. Core therefore always has priority.
#
# BASELINE HISTORICAL CONVENTION
#   OANDA midpoint, full available M15 history from 2002 onward
#   completed H1/H4/D states only; no lookahead
#   ATR14 Wilder/RMA SMA-seeded
#   signal timestamp = M15 candle OPEN
#   stop = signal high + 10 ticks
#   target based on REFERENCE signal-close risk
#   baseline cost = 1.0 pip adverse SHORT fill
#   stress = 0.5 / 1.0 / 1.5 / 2.0 pips
#   exits begin next M15 candle
#   exact exit-candle signal eligible
#   same-bar SHORT tie:
#       high closer to candle open => STOP first
#       otherwise TARGET first
#
# FINALIST DIAGNOSTICS
#   core / accepted complement / combined
#   pre-2010 / 2010+
#   2002-07 / 2008-13 / 2014-19 / 2020+
#   2002-17 / 2018+
#   last 5Y / last 2Y
#   0.5-2.0 pip stress
#   rolling 12 / 24 / 36 months
#   completed calendar years
#   inactive-year fill diagnostics (NOT scoring inputs)
#   overlap + finalist trade ledgers
#
# DESIRED OUTCOME — NOT A HARD FIT TARGET
#   complement roughly 20-40 clean adds if available
#   accepted PF preferably >= 1.4-1.5
#   accepted pre-2010 and 2010+ positive
#   >= 3/4 positive eras
#   accepted complement remains positive at 2 pips
#   combined PF preferably remains > 2.0
#   combined DD ideally stays around <= 8R, hard finalist ceiling 10R
#   materially fewer zero rolling windows if possible
#
# ONE ZIP
#   /eurgbp-m15-short-complementary-frequency/results
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
PARITY_CUTOFF = datetime(2026, 9, 10, 12, 30, tzinfo=timezone.utc)

NY = ZoneInfo("America/New_York")
LONDON = ZoneInfo("Europe/London")

TICK = 0.00001
PIP = 0.0001
STOP_TICKS = 10
PRIMARY_COST = 1.0
COSTS = [0.5, 1.0, 1.5, 2.0]

CORE_RR = 4.50
STAGE1_RR = 3.50
STAGE3_RRS = [2.50, 3.00, 3.50, 4.00, 4.50, 5.00]

STAGE1_KEEP = 30
STAGE2_GEOMETRY_KEEP = 18
STAGE2_KEEP = 24
STAGE3_BASE_KEEP = 18
FINAL_KEEP = 16

MEANINGFUL_CORE_INACTIVE_YEARS = [2005, 2009, 2013, 2023]
EARLY_NON_TARGET_YEARS = [2002, 2003, 2004]

OUTS = {
    "coverage": "eurgbp_m15_short_complementary_frequency_coverage.csv",
    "parity": "eurgbp_m15_short_complementary_frequency_parity.csv",
    "core": "eurgbp_m15_short_complementary_frequency_core_baseline.csv",
    "stage1": "eurgbp_m15_short_complementary_frequency_stage1.csv",
    "family": "eurgbp_m15_short_complementary_frequency_family_summary.csv",
    "stage2": "eurgbp_m15_short_complementary_frequency_stage2_context.csv",
    "stage3": "eurgbp_m15_short_complementary_frequency_stage3_rr.csv",
    "finalists": "eurgbp_m15_short_complementary_frequency_finalists.csv",
    "periods": "eurgbp_m15_short_complementary_frequency_periods.csv",
    "cost": "eurgbp_m15_short_complementary_frequency_cost_stress.csv",
    "rolling": "eurgbp_m15_short_complementary_frequency_rolling.csv",
    "rolling_summary": "eurgbp_m15_short_complementary_frequency_rolling_summary.csv",
    "calendar": "eurgbp_m15_short_complementary_frequency_calendar_years.csv",
    "calendar_summary": "eurgbp_m15_short_complementary_frequency_calendar_summary.csv",
    "overlap": "eurgbp_m15_short_complementary_frequency_overlap.csv",
    "trades": "eurgbp_m15_short_complementary_frequency_finalist_trades.csv",
    "notes": "eurgbp_m15_short_complementary_frequency_notes.csv",
}
BUNDLE = "EURGBP_M15_SHORT_COMPLEMENTARY_FREQUENCY_RESULTS.zip"

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
    o = np.array([x["open"] for x in c], dtype=float)
    h = np.array([x["high"] for x in c], dtype=float)
    l = np.array([x["low"] for x in c], dtype=float)
    cl = np.array([x["close"] for x in c], dtype=float)

    a = atr(c)
    am20 = sma(a, 20)
    bearish = cl < o

    exact = np.zeros(n, dtype=bool)
    exact[1:] = (
        (cl[:-1] > o[:-1]) &
        (cl[1:] < o[1:]) &
        (o[1:] >= cl[:-1]) &
        (cl[1:] <= o[:-1])
    )

    body = o - cl
    prev_body = np.full(n, np.nan)
    prev_body[1:] = np.abs(cl[:-1] - o[:-1])

    br = np.full(n, np.nan)
    vpb = prev_body > 0
    br[vpb] = body[vpb] / prev_body[vpb]
    br[prev_body == 0] = 999.0

    valid_atr = np.isfinite(a) & (a > 0)
    body_atr = np.full(n, np.nan)
    body_atr[valid_atr] = body[valid_atr] / a[valid_atr]

    crange = h - l
    range_atr = np.full(n, np.nan)
    range_atr[valid_atr] = crange[valid_atr] / a[valid_atr]

    close_loc = np.full(n, np.nan)
    vr = crange > 0
    close_loc[vr] = (cl[vr] - l[vr]) / crange[vr]

    uw = h - np.maximum(o, cl)
    uwb = np.full(n, np.nan)
    pb = body > 0
    uwb[pb] = uw[pb] / body[pb]

    comp = np.full(n, np.nan)
    pa = np.r_[np.nan, a[:-1]]
    pam = np.r_[np.nan, am20[:-1]]
    va = np.isfinite(pa) & np.isfinite(pam) & (pam > 0)
    comp[va] = pa[va] / pam[va]

    lbs = [5, 10, 20, 40, 60, 80, 100, 120, 165, 200]
    pl = {lb: prev_extreme(l, lb, "min") for lb in lbs}
    ph = {lb: prev_extreme(h, lb, "max") for lb in lbs}

    # ATR-normalised distance from current signal high to prior high.
    sd_high = {}
    for lb in [40, 60, 80, 100, 120, 165, 200]:
        x = np.full(n, np.nan)
        ok = valid_atr & np.isfinite(ph[lb])
        x[ok] = np.abs(h[ok] - ph[lb][ok]) / a[ok]
        sd_high[lb] = x

    # Strict prior 12H rally, ending at close[i-1].
    rally12 = np.full(n, np.nan)
    for i in range(49, n):
        if valid_atr[i]:
            rally12[i] = (cl[i-1] - cl[i-49]) / a[i]

    nyh = np.zeros(n, dtype=np.int16)
    nyw = np.zeros(n, dtype=np.int16)
    ldh = np.zeros(n, dtype=np.int16)
    ldw = np.zeros(n, dtype=np.int16)
    for i, t in enumerate(times):
        z = t.astimezone(NY)
        nyh[i], nyw[i] = z.hour, z.weekday()
        q = t.astimezone(LONDON)
        ldh[i], ldw[i] = q.hour, q.weekday()

    return {
        "n": n, "times": times,
        "open": o, "high": h, "low": l, "close": cl,
        "atr": a, "valid_atr": valid_atr, "bearish": bearish,
        "exact": exact, "br": br, "body_atr": body_atr,
        "range_atr": range_atr, "close_loc": close_loc,
        "uwb": uwb, "compression": comp,
        "prev_low": pl, "prev_high": ph,
        "structure_dist_high": sd_high,
        "rally12": rally12,
        "ny_hour": nyh, "ny_weekday": nyw,
        "ldn_hour": ldh, "ldn_weekday": ldw,
        "h1_close": h1["close"], "h1_ema50": h1["ema50"],
        "h1_ema100": h1["ema100"], "h1_ema200": h1["ema200"],
        "h1_atr": h1["atr_ratio50"],
        "h4_close": h4["close"], "h4_ema100": h4["ema100"],
        "h4_ema200": h4["ema200"], "h4_atr": h4["atr_ratio50"],
        "d_close": d["close"], "d_ema50": d["ema50"],
        "d_ema200": d["ema200"], "d_atr": d["atr_ratio50"],
    }



# ============================================================
# CONFIGS
# ============================================================

def cfg(cid, fam, rr=STAGE1_RR, **kw):
    x = {
        "config_id": cid,
        "family": fam,
        "rr": rr,
        "context": "NONE",
        "br_min": None,
        "body_atr_min": None,
        "range_atr_min": None,
        "close_loc_max": None,
        "upper_wick_body_min": None,
        "structure_lb": None,
        "structure_dist_atr_max": None,
        "breakout_lb": None,
        "compression_max": None,
        "rally12_min": None,
    }
    x.update(kw)
    return x


def frozen_core_cfg():
    return cfg(
        "FROZEN_CORE",
        "HIGH_SWEEP_REJECTION",
        rr=CORE_RR,
        body_atr_min=1.25,
        close_loc_max=0.20,
        upper_wick_body_min=0.40,
        structure_lb=120,
        context="EXCLUDE_WEDNESDAY",
    )


def build_stage1_configs():
    """Broad, clean, structurally distinct Trigger-B geometries."""
    out = []
    n = 0

    # 1) Compression -> bearish expansion through local low: 54.
    for co, body, rang, lb in product(
        [0.65, 0.75, 0.85],
        [0.80, 1.00, 1.20],
        [1.20, 1.40, 1.60],
        [5, 10],
    ):
        out.append(cfg(
            f"S1_{n:04d}", "COMPRESSION_BREAKDOWN",
            compression_max=co,
            body_atr_min=body,
            range_atr_min=rang,
            breakout_lb=lb,
        )); n += 1

    # 2) Exact bearish engulf near prior major high: 54.
    for br, body, lb, dist in product(
        [1.00, 1.20, 1.40],
        [0.60, 0.80, 1.00],
        [60, 100, 165],
        [0.10, 0.20],
    ):
        out.append(cfg(
            f"S1_{n:04d}", "BEAR_ENGULF_STRUCTURE",
            br_min=br,
            body_atr_min=body,
            structure_lb=lb,
            structure_dist_atr_max=dist,
        )); n += 1

    # 3) Bearish outside reversal WITHOUT major-high sweep condition: 27.
    #    This deliberately avoids duplicating the frozen core hypothesis.
    for body, rang, close_loc in product(
        [0.60, 0.80, 1.00],
        [1.20, 1.40, 1.60],
        [0.20, 0.30, 0.40],
    ):
        out.append(cfg(
            f"S1_{n:04d}", "BEAR_OUTSIDE_REVERSAL",
            body_atr_min=body,
            range_atr_min=rang,
            close_loc_max=close_loc,
        )); n += 1

    # 4) Prior 12H rally -> bearish local breakdown: 36.
    for rally, body, lb, close_loc in product(
        [0.75, 1.25],
        [0.60, 0.80, 1.00],
        [5, 10, 20],
        [0.20, 0.30],
    ):
        out.append(cfg(
            f"S1_{n:04d}", "RALLY_FAILURE_BREAKDOWN",
            rally12_min=rally,
            body_atr_min=body,
            breakout_lb=lb,
            close_loc_max=close_loc,
        )); n += 1

    return out


STAGE2_CONTEXTS = [
    "NONE",
    "H1_CLOSE_LT_EMA100",
    "H1_EMA50_LT_EMA200",
    "H4_CLOSE_LT_EMA100",
    "D_CLOSE_LT_EMA200",
    "LDN_BLOCK_04-07",
    "LDN_BLOCK_08-11",
    "NY_BLOCK_00-03",
    "NY_BLOCK_04-07",
]


# ============================================================
# SIGNAL EVALUATION
# ============================================================

def apply_context(mask, c, f):
    ctx = c.get("context", "NONE")
    if ctx == "NONE":
        return mask
    if ctx == "EXCLUDE_WEDNESDAY":
        return mask & (f["ldn_weekday"] != 2)
    if ctx == "H1_CLOSE_LT_EMA100":
        return mask & (f["h1_close"] < f["h1_ema100"])
    if ctx == "H1_EMA50_LT_EMA200":
        return mask & (f["h1_ema50"] < f["h1_ema200"])
    if ctx == "H4_CLOSE_LT_EMA100":
        return mask & (f["h4_close"] < f["h4_ema100"])
    if ctx == "D_CLOSE_LT_EMA200":
        return mask & (f["d_close"] < f["d_ema200"])
    if ctx.startswith("NY_BLOCK_"):
        lo, hi = map(int, ctx.split("_")[-1].split("-"))
        return mask & (f["ny_hour"] >= lo) & (f["ny_hour"] <= hi)
    if ctx.startswith("LDN_BLOCK_"):
        lo, hi = map(int, ctx.split("_")[-1].split("-"))
        return mask & (f["ldn_hour"] >= lo) & (f["ldn_hour"] <= hi)
    raise ValueError(f"Unknown context: {ctx}")


def signal_indices(c, f):
    m = f["valid_atr"].copy() & f["bearish"]
    fam = c["family"]

    if fam == "HIGH_SWEEP_REJECTION":
        # Frozen core only: high sweeps previous 120-bar high and closes
        # back below it. No ATR-distance requirement.
        prior_high = f["prev_high"][c["structure_lb"]]
        m &= f["high"] > prior_high
        m &= f["close"] < prior_high
        m &= f["body_atr"] >= c["body_atr_min"]
        m &= f["close_loc"] <= c["close_loc_max"]
        m &= f["uwb"] >= c["upper_wick_body_min"]

    elif fam == "COMPRESSION_BREAKDOWN":
        m &= f["compression"] <= c["compression_max"]
        m &= f["body_atr"] >= c["body_atr_min"]
        m &= f["range_atr"] >= c["range_atr_min"]
        m &= f["close"] < f["prev_low"][c["breakout_lb"]]

    elif fam == "BEAR_ENGULF_STRUCTURE":
        m &= f["exact"]
        m &= f["br"] >= c["br_min"]
        m &= f["body_atr"] >= c["body_atr_min"]
        m &= (
            f["structure_dist_high"][c["structure_lb"]]
            <= c["structure_dist_atr_max"]
        )

    elif fam == "BEAR_OUTSIDE_REVERSAL":
        prev_high = np.roll(f["high"], 1)
        prev_low = np.roll(f["low"], 1)
        m[0] = False
        m &= f["high"] > prev_high
        m &= f["low"] < prev_low
        m &= f["body_atr"] >= c["body_atr_min"]
        m &= f["range_atr"] >= c["range_atr_min"]
        m &= f["close_loc"] <= c["close_loc_max"]

    elif fam == "RALLY_FAILURE_BREAKDOWN":
        m &= f["rally12"] >= c["rally12_min"]
        m &= f["body_atr"] >= c["body_atr_min"]
        m &= f["close_loc"] <= c["close_loc_max"]
        m &= f["close"] < f["prev_low"][c["breakout_lb"]]

    else:
        raise ValueError(f"Unknown family: {fam}")

    m = apply_context(m, c, f)
    m[:220] = False
    return np.flatnonzero(m).tolist()


# ============================================================
# BACKTEST
# ============================================================

OUTCOME_CACHE = {}


def compute_outcome(candles, i, rr, cost_pips):
    key = (i, round(rr, 4), round(cost_pips, 4))
    if key in OUTCOME_CACHE:
        return OUTCOME_CACHE[key]

    signal = candles[i]
    reference_entry = signal["close"]
    stop = signal["high"] + STOP_TICKS * TICK
    reference_risk = stop - reference_entry
    if reference_risk <= 0:
        OUTCOME_CACHE[key] = None
        return None

    target = reference_entry - rr * reference_risk
    fill = reference_entry - cost_pips * PIP
    actual_risk = stop - fill
    if actual_risk <= 0:
        OUTCOME_CACHE[key] = None
        return None

    for j in range(i + 1, len(candles)):
        bar = candles[j]
        hit_stop = bar["high"] >= stop
        hit_target = bar["low"] <= target

        if hit_stop and hit_target:
            # Frozen SHORT tie convention: high closer to open => STOP.
            if abs(bar["high"] - bar["open"]) < abs(bar["open"] - bar["low"]):
                exit_price, reason = stop, "STOP"
            else:
                exit_price, reason = target, "TARGET"
        elif hit_target:
            exit_price, reason = target, "TARGET"
        elif hit_stop:
            exit_price, reason = stop, "STOP"
        else:
            continue

        result_r = (fill - exit_price) / actual_risk
        out = {
            "signal_index": i,
            "exit_index": j,
            "entry_time": signal["time"],
            "exit_time": bar["time"],
            "entry_time_utc": iso(signal["time"]),
            "exit_time_utc": iso(bar["time"]),
            "reference_entry": reference_entry,
            "historical_fill": fill,
            "stop": stop,
            "target": target,
            "result_r": result_r,
            "exit_reason": reason,
            "rr": rr,
            "cost_pips": cost_pips,
        }
        OUTCOME_CACHE[key] = out
        return out

    OUTCOME_CACHE[key] = None
    return None


def run_backtest(candles, indices, rr, cost_pips, start=None, end=None):
    use = indices
    if start is not None or end is not None:
        times = [candles[i]["time"] for i in indices]
        left = 0 if start is None else bisect.bisect_left(times, start)
        right = len(indices) if end is None else bisect.bisect_left(times, end)
        use = indices[left:right]

    trades = []
    p = 0
    while p < len(use):
        signal_index = use[p]
        trade = compute_outcome(candles, signal_index, rr, cost_pips)
        if trade is None:
            p += 1
            continue
        trades.append(dict(trade))
        # Exact exit candle remains eligible.
        p = bisect.bisect_left(use, trade["exit_index"], lo=p + 1)
    return trades


# ============================================================
# OVERLAY / STATS
# ============================================================

def nonoverlap_overlay(core_trades, candidate_trades):
    core = sorted(core_trades, key=lambda x: x["signal_index"])
    cand = sorted(candidate_trades, key=lambda x: x["signal_index"])
    accepted, rejected = [], []
    cp = 0

    for trade in cand:
        start = trade["signal_index"]
        end = trade["exit_index"]
        while cp < len(core) and core[cp]["exit_index"] <= start:
            cp += 1

        overlaps = False
        if cp < len(core):
            ct = core[cp]
            overlaps = ct["signal_index"] < end and start < ct["exit_index"]

        (rejected if overlaps else accepted).append(trade)

    combined = []
    for trade in core:
        x = dict(trade); x["source"] = "CORE"; combined.append(x)
    for trade in accepted:
        x = dict(trade); x["source"] = "COMPLEMENT"; combined.append(x)
    combined.sort(key=lambda x: x["signal_index"])
    return combined, accepted, rejected


def stats(trades):
    values = [float(x["result_r"]) for x in trades]
    winners = [x for x in values if x > 0]
    losers = [x for x in values if x < 0]
    gp = sum(winners); gl = abs(sum(losers))
    pf = gp / gl if gl > 0 else (999.0 if gp > 0 else 0.0)
    total = sum(values)
    eq = peak = 0.0
    dd = 0.0
    streak = longest = 0
    for r in values:
        eq += r; peak = max(peak, eq); dd = min(dd, eq - peak)
        if r < 0:
            streak += 1; longest = max(longest, streak)
        else:
            streak = 0
    return {
        "trades": len(values),
        "winners": len(winners),
        "losers": len(losers),
        "win_rate": 100.0 * len(winners) / len(values) if values else 0.0,
        "profit_factor": pf,
        "total_r": total,
        "expectancy_r": total / len(values) if values else 0.0,
        "max_drawdown_r": dd,
        "longest_loss_streak": longest,
    }


def subset(trades, start, end):
    return [x for x in trades if start <= x["entry_time"] < end]


ERAS = [
    ("ERA_2002_2007", START, datetime(2008,1,1,tzinfo=timezone.utc)),
    ("ERA_2008_2013", datetime(2008,1,1,tzinfo=timezone.utc), datetime(2014,1,1,tzinfo=timezone.utc)),
    ("ERA_2014_2019", datetime(2014,1,1,tzinfo=timezone.utc), datetime(2020,1,1,tzinfo=timezone.utc)),
    ("ERA_2020_NOW", datetime(2020,1,1,tzinfo=timezone.utc), NOW),
]


def config_fields(c):
    return {k: c.get(k) for k in [
        "compression_max", "body_atr_min", "range_atr_min", "breakout_lb",
        "br_min", "structure_lb", "structure_dist_atr_max",
        "close_loc_max", "upper_wick_body_min", "rally12_min",
    ]}


def evaluation_row(c, core_trades, candidate_trades):
    combined, accepted, rejected = nonoverlap_overlay(core_trades, candidate_trades)
    cs = stats(candidate_trades)
    ac = stats(accepted)
    co = stats(combined)

    pre = stats(subset(accepted, START, datetime(2010,1,1,tzinfo=timezone.utc)))
    post = stats(subset(accepted, datetime(2010,1,1,tzinfo=timezone.utc), NOW))
    era_stats = [stats(subset(accepted, a, b)) for _,a,b in ERAS]
    positive_eras = sum(s["total_r"] > 0 for s in era_stats)

    overlap_rate = 100.0 * len(rejected) / len(candidate_trades) if candidate_trades else 0.0

    # General robustness score only. There is intentionally NO reward for
    # 2009 / 2012 / 2016 specifically; those are diagnostics later.
    score = (
        0.10 * min(len(accepted), 45)
        + 2.0 * min(max(ac["profit_factor"], 0.0), 3.0)
        + 1.4 * min(max(co["profit_factor"], 0.0), 3.0)
        + 0.7 * positive_eras
        + 1.2 * (pre["total_r"] > 0)
        + 1.2 * (post["total_r"] > 0)
        + 0.4 * min(max(ac["total_r"], 0.0) / 10.0, 3.0)
        - 0.40 * max(0.0, abs(co["max_drawdown_r"]) - 8.0)
        - 0.015 * overlap_rate
    )

    row = {
        "config_id": c["config_id"],
        "family": c["family"],
        "context": c.get("context", "NONE"),
        "rr": c["rr"],
        "candidate_trades": cs["trades"],
        "candidate_pf": round(cs["profit_factor"], 6),
        "candidate_r": round(cs["total_r"], 4),
        "candidate_dd": round(cs["max_drawdown_r"], 4),
        "accepted_adds": ac["trades"],
        "rejected_overlap": len(rejected),
        "overlap_rate_pct": round(overlap_rate, 4),
        "accepted_pf": round(ac["profit_factor"], 6),
        "accepted_r": round(ac["total_r"], 4),
        "accepted_dd": round(ac["max_drawdown_r"], 4),
        "accepted_pre2010_r": round(pre["total_r"], 4),
        "accepted_post2010_r": round(post["total_r"], 4),
        "accepted_positive_eras": positive_eras,
        "combined_trades": co["trades"],
        "combined_pf": round(co["profit_factor"], 6),
        "combined_r": round(co["total_r"], 4),
        "combined_dd": round(co["max_drawdown_r"], 4),
        "robust_score": round(score, 6),
        **config_fields(c),
    }
    for (name, _, _), es in zip(ERAS, era_stats):
        row[name + "_trades"] = es["trades"]
        row[name + "_r"] = round(es["total_r"], 4)
        row[name + "_pf"] = round(es["profit_factor"], 6)
    return row


def sort_rows(rows):
    return sorted(rows, key=lambda r: (
        r["accepted_r"] > 0,
        r["accepted_pre2010_r"] > 0,
        r["accepted_post2010_r"] > 0,
        r["accepted_positive_eras"],
        r["robust_score"],
        r["combined_pf"],
        r["accepted_adds"],
    ), reverse=True)


def family_summary(rows):
    grouped = defaultdict(list)
    for r in rows:
        grouped[r["family"]].append(r)
    out = []
    for fam, sub in grouped.items():
        ranked = sort_rows(sub)
        best = ranked[0]
        out.append({
            "family": fam,
            "configs": len(sub),
            "positive_accepted_r_configs": sum(r["accepted_r"] > 0 for r in sub),
            "positive_pre_and_post_configs": sum(
                r["accepted_pre2010_r"] > 0 and r["accepted_post2010_r"] > 0
                for r in sub
            ),
            "three_plus_positive_era_configs": sum(r["accepted_positive_eras"] >= 3 for r in sub),
            "best_config_id": best["config_id"],
            "best_accepted_adds": best["accepted_adds"],
            "best_accepted_pf": best["accepted_pf"],
            "best_accepted_r": best["accepted_r"],
            "best_combined_pf": best["combined_pf"],
            "best_combined_r": best["combined_r"],
            "best_combined_dd": best["combined_dd"],
            "best_robust_score": best["robust_score"],
        })
    return sorted(out, key=lambda r: (
        r["positive_pre_and_post_configs"],
        r["three_plus_positive_era_configs"],
        r["best_robust_score"],
    ), reverse=True)


# ============================================================
# STAGED SEARCH
# ============================================================

def stage2_configs(stage1_rows, by_id):
    # Preserve family diversity: take up to 5 strong geometries from each
    # family, then fill remaining slots by overall rank.
    selected = []
    seen = set()
    by_family = defaultdict(list)
    for r in sort_rows(stage1_rows):
        by_family[r["family"]].append(r)
    for fam, rows in by_family.items():
        for r in rows[:5]:
            if r["config_id"] not in seen:
                selected.append(r); seen.add(r["config_id"])
    for r in sort_rows(stage1_rows):
        if len(selected) >= STAGE2_GEOMETRY_KEEP:
            break
        if r["config_id"] not in seen:
            selected.append(r); seen.add(r["config_id"])

    out = []
    for rank, row in enumerate(selected):
        base = by_id[row["config_id"]]
        for context in STAGE2_CONTEXTS:
            x = deepcopy(base)
            x["config_id"] = f"S2_{rank:02d}_{context}"
            x["context"] = context
            out.append(x)
    return out


def stage3_configs(stage2_rows, by_id):
    # Deepen only robust survivors, while retaining family diversity.
    eligible = [r for r in sort_rows(stage2_rows) if (
        r["accepted_adds"] >= 8
        and r["accepted_r"] > 0
        and r["accepted_positive_eras"] >= 2
    )]
    base_rows = eligible[:STAGE3_BASE_KEEP] if eligible else sort_rows(stage2_rows)[:STAGE3_BASE_KEEP]
    out = []
    seen = set()
    for rank, row in enumerate(base_rows):
        base = by_id[row["config_id"]]
        for rr in STAGE3_RRS:
            x = deepcopy(base)
            x["config_id"] = f"S3_{rank:02d}_RR_{rr:.2f}"
            x["rr"] = rr
            sig = tuple(str(x.get(k)) for k in [
                "family","context","compression_max","body_atr_min",
                "range_atr_min","breakout_lb","br_min","structure_lb",
                "structure_dist_atr_max","close_loc_max","upper_wick_body_min","rally12_min","rr"
            ])
            if sig not in seen:
                seen.add(sig); out.append(x)
    return out


# ============================================================
# DEEP DIAGNOSTICS
# ============================================================

def period_defs(actual_start):
    return [
        ("FULL_HISTORY", actual_start, NOW),
        ("PRE_2010", actual_start, datetime(2010,1,1,tzinfo=timezone.utc)),
        ("2010_PLUS", datetime(2010,1,1,tzinfo=timezone.utc), NOW),
        *ERAS,
        ("DEV_2002_2017", actual_start, datetime(2018,1,1,tzinfo=timezone.utc)),
        ("VALIDATION_2018_PLUS", datetime(2018,1,1,tzinfo=timezone.utc), NOW),
        ("LAST_5Y", NOW - timedelta(days=365.2425*5), NOW),
        ("LAST_2Y", NOW - timedelta(days=365.2425*2), NOW),
    ]


def source_trades(core, cand, start, end):
    combined, accepted, rejected = nonoverlap_overlay(core, cand)
    return {
        "CORE": subset(core, start, end),
        "ACCEPTED_COMPLEMENT": subset(accepted, start, end),
        "COMBINED": subset(combined, start, end),
    }, rejected


def deep_period_rows(c, core, cand, actual_start):
    rows = []
    combined, accepted, _ = nonoverlap_overlay(core, cand)
    for label, a, b in period_defs(actual_start):
        for source, trades in [
            ("CORE", subset(core,a,b)),
            ("ACCEPTED_COMPLEMENT", subset(accepted,a,b)),
            ("COMBINED", subset(combined,a,b)),
        ]:
            s = stats(trades)
            rows.append({
                "config_id": c["config_id"], "family": c["family"],
                "context": c["context"], "rr": c["rr"],
                "source": source, "period": label,
                "start_utc": iso(a), "end_utc": iso(b),
                **{k: round(v,6) if isinstance(v,float) else v for k,v in s.items()},
            })
    return rows


def deep_cost_rows(c, candles, core_ix, cand_ix, actual_start):
    rows = []
    for cost in COSTS:
        core = run_backtest(candles, core_ix, CORE_RR, cost, actual_start, NOW)
        cand = run_backtest(candles, cand_ix, c["rr"], cost, actual_start, NOW)
        combined, accepted, _ = nonoverlap_overlay(core, cand)
        for source, trades in [
            ("CORE", core),
            ("ACCEPTED_COMPLEMENT", accepted),
            ("COMBINED", combined),
        ]:
            s = stats(trades)
            rows.append({
                "config_id": c["config_id"], "family": c["family"],
                "context": c["context"], "rr": c["rr"],
                "source": source, "cost_pips": cost,
                **{k: round(v,6) if isinstance(v,float) else v for k,v in s.items()},
            })
    return rows


def rolling_rows(c, candles, core_ix, cand_ix, actual_start):
    rows = []
    first = month_floor(max(actual_start, START))
    last = month_floor(NOW)
    for months in [12,24,36]:
        s = first
        while add_months(s, months) <= last:
            e = add_months(s, months)
            core = run_backtest(candles, core_ix, CORE_RR, PRIMARY_COST, s, e)
            cand = run_backtest(candles, cand_ix, c["rr"], PRIMARY_COST, s, e)
            combined, accepted, _ = nonoverlap_overlay(core, cand)
            for source, trades in [
                ("CORE", core),
                ("ACCEPTED_COMPLEMENT", accepted),
                ("COMBINED", combined),
            ]:
                st = stats(trades)
                rows.append({
                    "config_id": c["config_id"], "months": months,
                    "source": source, "start_utc": iso(s), "end_utc": iso(e),
                    "trades": st["trades"],
                    "profit_factor": round(st["profit_factor"],6),
                    "total_r": round(st["total_r"],4),
                    "positive": st["total_r"] > 0,
                    "zero_trade": st["trades"] == 0,
                })
            s = add_months(s, 1)
    return rows


def rolling_summary(rows):
    grouped = defaultdict(list)
    for r in rows:
        grouped[(r["config_id"],r["source"],r["months"])].append(r)
    out = []
    for (cid,source,months), sub in grouped.items():
        active = [r for r in sub if r["trades"] > 0]
        out.append({
            "config_id": cid, "source": source, "months": months,
            "windows": len(sub), "active_windows": len(active),
            "zero_trade_windows": len(sub)-len(active),
            "positive_active_windows_pct": round(
                100*sum(r["positive"] for r in active)/len(active),4
            ) if active else 0.0,
            "median_r_all": round(med([r["total_r"] for r in sub]),4),
            "median_r_active": round(med([r["total_r"] for r in active]),4) if active else 0.0,
            "worst_r": round(min(r["total_r"] for r in sub),4),
            "best_r": round(max(r["total_r"] for r in sub),4),
        })
    return out


def calendar_rows(c, candles, core_ix, cand_ix):
    rows = []
    for year in range(max(START.year, candles[0]["time"].year), NOW.year):
        a = datetime(year,1,1,tzinfo=timezone.utc)
        b = datetime(year+1,1,1,tzinfo=timezone.utc)
        core = run_backtest(candles, core_ix, CORE_RR, PRIMARY_COST, a, b)
        cand = run_backtest(candles, cand_ix, c["rr"], PRIMARY_COST, a, b)
        combined, accepted, _ = nonoverlap_overlay(core, cand)
        for source, trades in [
            ("CORE",core),("ACCEPTED_COMPLEMENT",accepted),("COMBINED",combined)
        ]:
            st=stats(trades)
            rows.append({
                "config_id":c["config_id"],"source":source,"year":year,
                "trades":st["trades"],"profit_factor":round(st["profit_factor"],6),
                "total_r":round(st["total_r"],4),"positive":st["total_r"]>0,
                "negative":st["total_r"]<0,"zero_trade":st["trades"]==0,
                "meaningful_core_gap_year": year in MEANINGFUL_CORE_INACTIVE_YEARS,
                "early_non_target_year": year in EARLY_NON_TARGET_YEARS,
            })
    return rows


def calendar_summary(rows):
    grouped=defaultdict(list)
    for r in rows:
        grouped[(r["config_id"],r["source"])].append(r)
    out=[]
    for (cid,source),sub in grouped.items():
        active=[r for r in sub if r["trades"]>0]
        meaningful=[r for r in sub if r["meaningful_core_gap_year"]]
        out.append({
            "config_id":cid,"source":source,"completed_years":len(sub),
            "active_years":len(active),"zero_trade_years":len(sub)-len(active),
            "positive_active_years_pct":round(
                100*sum(r["positive"] for r in active)/len(active),4
            ) if active else 0.0,
            "median_trades_year":round(med([r["trades"] for r in sub]),4),
            "median_year_r":round(med([r["total_r"] for r in sub]),4),
            "worst_year_r":round(min(r["total_r"] for r in sub),4),
            "meaningful_gap_years_active":",".join(str(r["year"]) for r in meaningful if r["trades"]>0),
            "meaningful_gap_years_profitable":",".join(str(r["year"]) for r in meaningful if r["total_r"]>0),
        })
    return out


# ============================================================
# RUNNER
# ============================================================

def run_research():
    try:
        STATUS.update({"state":"fetching","message":"Fetching EUR/GBP M15/H1/H4/D history for SHORT complement search"})
        m15 = fetch("M15", START, NOW, 35)
        h1 = fetch("H1", WARMUP, NOW, 180)
        h4 = fetch("H4", WARMUP, NOW, 700)
        daily = fetch("D", WARMUP, NOW, 3000)
        if not all([m15,h1,h4,daily]):
            raise RuntimeError("Missing required EUR/GBP history")

        write_csv(OUTS["coverage"],[{
            "instrument":PAIR,"requested_start_utc":iso(START),
            "parity_cutoff_utc":iso(PARITY_CUTOFF),
            "actual_first_m15_utc":iso(m15[0]["time"]),
            "actual_last_m15_utc":iso(m15[-1]["time"]),
            "m15_candles":len(m15),"h1_candles":len(h1),
            "h4_candles":len(h4),"daily_candles":len(daily),
        }])

        STATUS.update({"state":"precompute","message":"Building causal HTF and M15 feature cache"})
        times=[x["time"] for x in m15]
        ah1=align_htf(times,htf_state(h1))
        ah4=align_htf(times,htf_state(h4))
        ad=align_htf(times,htf_state(daily))
        f=features(m15,ah1,ah4,ad)

        core_cfg=frozen_core_cfg()
        core_ix=signal_indices(core_cfg,f)

        # HARD PARITY through fixed historical cutoff.
        parity_core=run_backtest(
            m15,core_ix,CORE_RR,PRIMARY_COST,m15[0]["time"],PARITY_CUTOFF
        )
        parity_rows=[{
            "check":"FROZEN_CORE_44","expected_trades":44,
            "actual_trades":len(parity_core),
            "status":"MATCH" if len(parity_core)==44 else "MISMATCH",
            "cutoff_utc":iso(PARITY_CUTOFF),
        }]
        write_csv(OUTS["parity"],parity_rows)
        if len(parity_core)!=44:
            raise RuntimeError(
                f"Frozen EUR/GBP SHORT core parity failed: expected 44, got {len(parity_core)}"
            )

        core_full=run_backtest(m15,core_ix,CORE_RR,PRIMARY_COST,m15[0]["time"],NOW)
        core_stats=stats(core_full)
        write_csv(OUTS["core"],[{
            "strategy":"FROZEN_CORE","rr":CORE_RR,
            **{k:round(v,6) if isinstance(v,float) else v for k,v in core_stats.items()},
            "known_meaningful_inactive_years":"2005,2009,2013,2023",
            "note":"Core is frozen; no parameter in Trigger A is searched or loosened.",
        }])

        # STAGE 1
        stage1=build_stage1_configs()
        by1={c["config_id"]:c for c in stage1}
        rows1=[]
        for n,c in enumerate(stage1,1):
            STATUS.update({"state":"stage1","message":f"Stage 1 {n}/{len(stage1)} {c['config_id']}"})
            cand_ix=signal_indices(c,f)
            cand=run_backtest(m15,cand_ix,c["rr"],PRIMARY_COST,m15[0]["time"],NOW)
            rows1.append(evaluation_row(c,core_full,cand))
        rows1=sort_rows(rows1)
        write_csv(OUTS["stage1"],rows1)
        write_csv(OUTS["family"],family_summary(rows1))

        # STAGE 2 contexts
        stage2=stage2_configs(rows1,by1)
        by2={c["config_id"]:c for c in stage2}
        rows2=[]
        for n,c in enumerate(stage2,1):
            STATUS.update({"state":"stage2","message":f"Stage 2 {n}/{len(stage2)} {c['config_id']}"})
            cand_ix=signal_indices(c,f)
            cand=run_backtest(m15,cand_ix,c["rr"],PRIMARY_COST,m15[0]["time"],NOW)
            rows2.append(evaluation_row(c,core_full,cand))
        rows2=sort_rows(rows2)
        write_csv(OUTS["stage2"],rows2)

        # STAGE 3 RR confirmation + explicit 2-pip survival guard.
        stage3=stage3_configs(rows2,by2)
        by3={c["config_id"]:c for c in stage3}
        rows3=[]
        core_full_2p=run_backtest(m15,core_ix,CORE_RR,2.0,m15[0]["time"],NOW)
        for n,c in enumerate(stage3,1):
            STATUS.update({"state":"stage3","message":f"Stage 3 {n}/{len(stage3)} {c['config_id']}"})
            cand_ix=signal_indices(c,f)
            cand=run_backtest(m15,cand_ix,c["rr"],PRIMARY_COST,m15[0]["time"],NOW)
            row=evaluation_row(c,core_full,cand)

            cand_2p=run_backtest(m15,cand_ix,c["rr"],2.0,m15[0]["time"],NOW)
            combined_2p,accepted_2p,rejected_2p=nonoverlap_overlay(core_full_2p,cand_2p)
            a2=stats(accepted_2p); c2=stats(combined_2p)
            row.update({
                "accepted_2p_trades":a2["trades"],
                "accepted_2p_pf":round(a2["profit_factor"],6),
                "accepted_2p_r":round(a2["total_r"],4),
                "combined_2p_pf":round(c2["profit_factor"],6),
                "combined_2p_r":round(c2["total_r"],4),
                "combined_2p_dd":round(c2["max_drawdown_r"],4),
                "rejected_overlap_2p":len(rejected_2p),
            })
            rows3.append(row)

        rows3=sorted(rows3,key=lambda r:(
            r["accepted_2p_r"]>0,
            r["accepted_pre2010_r"]>0,
            r["accepted_post2010_r"]>0,
            r["accepted_positive_eras"],
            r["robust_score"],
            r["combined_2p_pf"],
            r["accepted_adds"],
        ),reverse=True)
        write_csv(OUTS["stage3"],rows3)

        # Finalists: robustness-first, never gap-year-specific.
        eligible=[r for r in rows3 if (
            r["accepted_adds"]>=12
            and r["accepted_r"]>0
            and r["accepted_pf"]>=1.25
            and r["accepted_pre2010_r"]>0
            and r["accepted_post2010_r"]>0
            and r["accepted_positive_eras"]>=3
            and r["accepted_2p_r"]>0
            and r["accepted_2p_pf"]>=1.20
            and r["combined_pf"]>=2.0
            and r["combined_2p_pf"]>=1.75
            and r["combined_r"]>core_stats["total_r"]
            and r["combined_dd"]>=-10.0
        )]
        finalist_rows=(eligible if eligible else rows3)[:FINAL_KEEP]
        finalists=[by3[r["config_id"]] for r in finalist_rows]
        write_csv(OUTS["finalists"],finalist_rows)

        periods=[]; costs=[]; rolling=[]; calendar=[]; overlap=[]; trade_rows=[]
        for n,c in enumerate(finalists,1):
            STATUS.update({"state":"deep_validation","message":f"Deep finalist {n}/{len(finalists)} {c['config_id']}"})
            cand_ix=signal_indices(c,f)
            cand=run_backtest(m15,cand_ix,c["rr"],PRIMARY_COST,m15[0]["time"],NOW)
            combined,accepted,rejected=nonoverlap_overlay(core_full,cand)

            periods.extend(deep_period_rows(c,core_full,cand,m15[0]["time"]))
            costs.extend(deep_cost_rows(c,m15,core_ix,cand_ix,m15[0]["time"]))
            rolling.extend(rolling_rows(c,m15,core_ix,cand_ix,m15[0]["time"]))
            calendar.extend(calendar_rows(c,m15,core_ix,cand_ix))

            overlap.append({
                "config_id":c["config_id"],"family":c["family"],"context":c["context"],"rr":c["rr"],
                "candidate_trades":len(cand),"accepted_nonoverlap":len(accepted),
                "rejected_overlap":len(rejected),
                "overlap_rate_pct":round(100*len(rejected)/len(cand),4) if cand else 0.0,
                "combined_trades":len(combined),
            })
            for source,trades in [("CORE",core_full),("ACCEPTED_COMPLEMENT",accepted),("COMBINED",combined)]:
                for t in trades:
                    x=dict(t)
                    x.update({"config_id":c["config_id"],"family":c["family"],"context":c["context"],"source":source})
                    trade_rows.append(x)

        write_csv(OUTS["periods"],periods)
        write_csv(OUTS["cost"],costs)
        write_csv(OUTS["rolling"],rolling)
        write_csv(OUTS["rolling_summary"],rolling_summary(rolling))
        write_csv(OUTS["calendar"],calendar)
        write_csv(OUTS["calendar_summary"],calendar_summary(calendar))
        write_csv(OUTS["overlap"],overlap)
        write_csv(OUTS["trades"],trade_rows)
        write_csv(OUTS["notes"],[
            {"note":"Frozen core parity must equal 44 through 2026-09-10 12:30 UTC."},
            {"note":"Stage scoring deliberately does not reward filling 2005, 2009, 2013 or 2023; those years are diagnostics only."},
            {"note":"2002-2004 are early-history non-target years and are not optimisation targets."},
            {"note":"Candidate interval [signal, exit) overlapping any core trade is rejected; exact core exit candle remains eligible."},
            {"note":"Final complement should be selected on general temporal/rolling/cost robustness and frequency improvement, not highest lifetime R/PF."},
            {"note":"Target-year activity is report-only. Do not select a complement because it happens to fill 2005/2009/2013/2023."},
            {"note":"Stage-3 shortlist requires accepted complement to remain profitable at 2-pip adverse cost and combined PF >=2.0 at baseline."},
            {"note":"Full history has been used in development, so period splits are robustness diagnostics rather than pristine OOS."},
        ])

        pack()
        STATUS.update({
            "state":"complete","message":"EUR/GBP M15 SHORT complementary-frequency search complete",
            "core_trades":len(core_full),"stage1_configs":len(stage1),
            "stage2_configs":len(stage2),"stage3_configs":len(stage3),
            "finalists":len(finalists),"results_bundle":BUNDLE,
            "orders_supported":False,"trading_enabled":False,
        })

    except Exception as e:
        STATUS.update({"state":"error","message":str(e),"orders_supported":False,"trading_enabled":False})
        print("ERROR:",e,flush=True)


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def root():
    return jsonify({
        "service":"EUR/GBP M15 SHORT Complementary Frequency Search",
        "state":STATUS["state"],"instrument":PAIR,"timeframe":"M15","side":"SELL",
        "orders_supported":False,"trading_enabled":False,
        "routes":[
            "/eurgbp-m15-short-complementary-frequency/status",
            "/eurgbp-m15-short-complementary-frequency/results",
        ],
    })

@app.route("/eurgbp-m15-short-complementary-frequency/status")
def route_status():
    return jsonify(STATUS)

@app.route("/eurgbp-m15-short-complementary-frequency/results")
def route_results():
    return dl(BUNDLE)

if __name__ == "__main__":
    thread=threading.Thread(target=run_research,name="eurgbp-m15-short-complement",daemon=True)
    thread.start()
    port=int(os.getenv("PORT",5000))
    app.run(host="0.0.0.0",port=port,debug=False)
