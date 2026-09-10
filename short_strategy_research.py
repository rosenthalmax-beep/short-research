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
# EUR/GBP M15 SHORT — FINAL COMPLEMENT BR-ONLY CONFIRMATION
#
# PURPOSE
#   Freeze the FINAL 44-trade EUR/GBP M15 SHORT core exactly,
#   then locally confirm the only promising Trigger-B family found
#   by the complementary-frequency search:
#       exact bearish engulfing near a prior major high,
#       gated by a completed H1 bearish regime.
#
# SOURCE-DERIVED ANCHOR FROM THE COMPLEMENT SEARCH
#   family: BEAR_ENGULF_STRUCTURE
#   BR >= 1.40
#   body >= 1.00 ATR14
#   previous 100-bar high
#   abs(signal high - prior high) <= 0.20 ATR14
#   previous completed H1 close < H1 EMA100
#   RR 2.50
#   candidate 41 / accepted 40 / overlap 1 / combined 84
#   accepted PF ~1.613 / +14.10R
#   combined PF ~2.554 / +73.03R / DD ~-5R
#   accepted 2-pip PF ~1.430 / +9.89R
#
# FROZEN CORE — NEVER OPTIMISE / LOOSEN
#   HIGH_SWEEP_REJECTION
#   bearish M15 candle
#   current high > previous 120-bar high, then close back below it
#   body >= 1.25 ATR14
#   close location <= 0.20
#   upper wick/body >= 0.40
#   exclude Wednesday in Europe/London
#   RR 4.50
#   stop = signal high + 10 ticks
#   1-pip adverse historical SHORT fill
#   pyramiding 0
#
# CONFIRMATION DESIGN — STAGED, NOT A GIANT CARTESIAN SEARCH
#   Stage 1A: BR x body, with structure/H1/RR frozen to anchor
#   Stage 1B: structure lookback x distance, with candle/H1/RR frozen
#   Stage 2: controlled interaction of the strongest local candle and
#            structure neighbourhoods (anchor is always retained)
#   Stage 3: H1 bearish-regime confirmation:
#            NONE and completed H1 close < EMA50/75/100/125/150/200
#   Stage 4: RR confirmation 2.00/2.25/2.50/2.75/3.00
#   Deep finalists: full/pre/post, four eras, 2018+, last5Y/2Y,
#                    0.5-2p stress, rolling 12/24/36M, calendar years.
#
# SEARCH RANGES
#   BR:       1.20 / 1.30 / 1.40 / 1.50 / 1.60
#   body ATR: 0.80 / 0.90 / 1.00 / 1.10 / 1.20
#   LB:       60 / 80 / 100 / 120 / 140 / 165
#   distance: 0.10 / 0.15 / 0.20 / 0.25 / 0.30 ATR
#   H1 EMA:   50 / 75 / 100 / 125 / 150 / 200, plus NONE ablation
#   RR:       2.00 / 2.25 / 2.50 / 2.75 / 3.00
#
# ANTI-OVERFIT RULES
#   - raw NONE H1 context is included as an ablation, not hidden
#   - no session or weekday search for Trigger B
#   - no target-year scoring
#   - 2005/2009/2013/2023 are diagnostics only
#   - candidate interval [signal, exit) overlapping a core trade is
#     rejected; exact core exit-candle signal remains eligible
#   - core always has priority
#   - selection prefers broad temporal/cost robustness over max R/PF
#
# HARD PARITY CUTOFF
#   2026-09-10 12:30 UTC
#   frozen core = 44 trades
#   anchor candidate = 41 trades
#   anchor accepted = 40 trades
#   anchor overlap = 1 trade
#   anchor combined = 84 trades
#
# BASELINE EXECUTION
#   OANDA midpoint, full available EUR_GBP M15 history from 2002+
#   completed H1/H4/D states only; no lookahead
#   ATR14 Wilder/RMA SMA-seeded
#   signal timestamp = M15 candle OPEN
#   stop = signal high + 10 ticks
#   target based on REFERENCE signal-close risk
#   short adverse fill = signal close - cost pips
#   baseline 1.0 pip; stress 0.5/1.0/1.5/2.0
#   exits start next M15 candle
#   same-bar SHORT tie: high closer to open => STOP first,
#                       otherwise TARGET first
#
# ONE ZIP
#   /eurgbp-m15-short-complement-final-br-only/results
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
ANCHOR_HISTORY_CUTOFF = datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc)

NY = ZoneInfo("America/New_York")
LONDON = ZoneInfo("Europe/London")

TICK = 0.00001
PIP = 0.0001
STOP_TICKS = 10
PRIMARY_COST = 1.0
COSTS = [0.5, 1.0, 1.5, 2.0]

CORE_RR = 4.50
ANCHOR_RR = 2.50
RRS = [2.50]

BR_VALUES = [1.40, 1.50, 1.60, 1.70, 1.80, 2.00]
BODY_VALUES = [1.00]
LB_VALUES = [100]
DIST_VALUES = [0.15]
H1_EMA_PERIODS = [100]

STAGE2_CANDLE_KEEP = 6
STAGE2_STRUCTURE_KEEP = 1
STAGE3_GEOMETRY_KEEP = 6
STAGE4_H1_KEEP = 6
FINAL_KEEP = 6

MEANINGFUL_CORE_INACTIVE_YEARS = [2005, 2009, 2013, 2023]
EARLY_NON_TARGET_YEARS = [2002, 2003, 2004]

OUTS = {
    "coverage": "eurgbp_m15_short_complement_final_br_only_coverage.csv",
    "parity": "eurgbp_m15_short_complement_final_br_only_parity.csv",
    "core": "eurgbp_m15_short_complement_final_br_only_core_baseline.csv",
    "stage1_br_body": "eurgbp_m15_short_complement_final_br_only_stage1_br_body.csv",
    "stage1_structure": "eurgbp_m15_short_complement_final_br_only_stage1_structure_distance.csv",
    "stage2_geometry": "eurgbp_m15_short_complement_final_br_only_stage2_geometry_interactions.csv",
    "stage3_h1": "eurgbp_m15_short_complement_final_br_only_stage3_h1_regime.csv",
    "stage4_rr": "eurgbp_m15_short_complement_final_br_only_stage4_rr.csv",
    "finalists": "eurgbp_m15_short_complement_final_br_only_finalists.csv",
    "periods": "eurgbp_m15_short_complement_final_br_only_periods.csv",
    "cost": "eurgbp_m15_short_complement_final_br_only_cost_stress.csv",
    "rolling": "eurgbp_m15_short_complement_final_br_only_rolling.csv",
    "rolling_summary": "eurgbp_m15_short_complement_final_br_only_rolling_summary.csv",
    "calendar": "eurgbp_m15_short_complement_final_br_only_calendar_years.csv",
    "calendar_summary": "eurgbp_m15_short_complement_final_br_only_calendar_summary.csv",
    "overlap": "eurgbp_m15_short_complement_final_br_only_overlap.csv",
    "trades": "eurgbp_m15_short_complement_final_br_only_finalist_trades.csv",
    "parameter_summary": "eurgbp_m15_short_complement_final_br_only_parameter_summary.csv",
    "notes": "eurgbp_m15_short_complement_final_br_only_notes.csv",
}
BUNDLE = "EURGBP_M15_SHORT_COMPLEMENT_FINAL_BR_ONLY_RESULTS.zip"

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
    periods = [50, 75, 100, 125, 150, 200]
    emas = {p: ema(closes, p) for p in periods}
    a = atr(c)
    am = sma(a, 50)
    rows = []
    for i, bar in enumerate(c):
        complete_at = c[i+1]["time"] if i+1 < len(c) else None
        row = {
            "complete_at": complete_at,
            "close": bar["close"],
            "atr_ratio50": (
                float(a[i] / am[i])
                if np.isfinite(a[i]) and np.isfinite(am[i]) and am[i] > 0
                else None
            ),
        }
        for p in periods:
            row[f"ema{p}"] = emas[p][i]
        rows.append(row)
    return rows


def align_htf(m15_times, state):
    rows = [r for r in state if r["complete_at"] is not None]
    ct = [r["complete_at"] for r in rows]
    keys = ["close", "ema50", "ema75", "ema100", "ema125", "ema150", "ema200", "atr_ratio50"]
    out = {k: np.full(len(m15_times), np.nan) for k in keys}
    for i, t in enumerate(m15_times):
        p = bisect.bisect_right(ct, t) - 1
        if p < 0:
            continue
        r = rows[p]
        for k in keys:
            if r.get(k) is not None:
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

    # Build extreme caches from the actual configured research lookbacks
    # plus the small fixed lookbacks used by other trigger families. This
    # prevents a grid value (e.g. LB140) from existing downstream without
    # its upstream prior-high/prior-low cache being created first.
    lbs = sorted(set([5, 10, 20, 40, 60, 80, 100, 120, 165, 200] + list(LB_VALUES)))
    pl = {lb: prev_extreme(l, lb, "min") for lb in lbs}
    ph = {lb: prev_extreme(h, lb, "max") for lb in lbs}

    # ATR-normalised distance from current signal high to prior high.
    sd_high = {}
    for lb in sorted(set([40, 60, 80, 100, 120, 165, 200] + list(LB_VALUES))):
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
        "h1_close": h1["close"],
        "h1_ema50": h1["ema50"], "h1_ema75": h1["ema75"],
        "h1_ema100": h1["ema100"], "h1_ema125": h1["ema125"],
        "h1_ema150": h1["ema150"], "h1_ema200": h1["ema200"],
        "h1_atr": h1["atr_ratio50"],
        "h4_close": h4["close"], "h4_ema100": h4["ema100"],
        "h4_ema200": h4["ema200"], "h4_atr": h4["atr_ratio50"],
        "d_close": d["close"], "d_ema50": d["ema50"],
        "d_ema200": d["ema200"], "d_atr": d["atr_ratio50"],
    }



# ============================================================
# CONFIGS
# ============================================================

def cfg(cid, fam="BEAR_ENGULF_STRUCTURE", rr=ANCHOR_RR, **kw):
    x = {
        "config_id": cid,
        "family": fam,
        "rr": rr,
        "context": "H1_CLOSE_LT_EMA100",
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
        "h1_ema_period": None,
    }
    x.update(kw)
    return x


def frozen_core_cfg():
    return cfg(
        "FROZEN_CORE",
        fam="HIGH_SWEEP_REJECTION",
        rr=CORE_RR,
        body_atr_min=1.25,
        close_loc_max=0.20,
        upper_wick_body_min=0.40,
        structure_lb=120,
        context="EXCLUDE_WEDNESDAY",
    )


def anchor_cfg(cid="ANCHOR"):
    return cfg(
        cid,
        br_min=1.40,
        body_atr_min=1.00,
        structure_lb=100,
        structure_dist_atr_max=0.20,
        h1_ema_period=100,
        context="H1_CLOSE_LT_EMA100",
        rr=ANCHOR_RR,
    )


def build_stage1_br_body():
    # FINAL MICRO TEST: only BR varies.
    # body=1.00, LB100, distance=0.15, H1 EMA100 and RR2.50 are frozen.
    return [cfg(
        f"BR_ONLY_{br:.2f}",
        br_min=br,
        body_atr_min=1.00,
        structure_lb=100,
        structure_dist_atr_max=0.15,
        h1_ema_period=100,
        context="H1_CLOSE_LT_EMA100",
        rr=2.50,
    ) for br in BR_VALUES]

def build_stage1_structure_distance():
    # Single frozen structure reference; no structure optimization in this run.
    return [cfg(
        "FROZEN_STRUCTURE_REFERENCE",
        br_min=1.60,
        body_atr_min=1.00,
        structure_lb=100,
        structure_dist_atr_max=0.15,
        h1_ema_period=100,
        context="H1_CLOSE_LT_EMA100",
        rr=2.50,
    )]


def choose_local_rows(rows, keep):
    ranked = sort_rows(rows)
    eligible = [r for r in ranked if (
        r["accepted_adds"] >= 10
        and r["accepted_r"] > 0
        and r["accepted_positive_eras"] >= 2
    )]
    selected = (eligible if eligible else ranked)[:keep]
    return selected


def build_stage2_geometry(rows_a, rows_b, by_a, by_b):
    # BR-only confirmation: preserve all six BR values exactly.
    return [cfg(
        f"BR_ONLY_{br:.2f}",
        br_min=br, body_atr_min=1.00, structure_lb=100,
        structure_dist_atr_max=0.15, h1_ema_period=100,
        context="H1_CLOSE_LT_EMA100", rr=2.50,
    ) for br in BR_VALUES]

def build_stage3_h1(rows2, by2):
    # H1 EMA100 is frozen; no NONE ablation or alternate EMA periods here.
    return [deepcopy(by2[r["config_id"]]) for r in rows2]

def build_stage4_rr(rows3, by3):
    # RR2.50 is frozen; only BR is under test.
    out = []
    for r in rows3:
        x = deepcopy(by3[r["config_id"]])
        x["config_id"] = x["config_id"] + "_RR2.50"
        x["rr"] = 2.50
        out.append(x)
    return out

def apply_context(mask, c, f):
    ctx = c.get("context", "NONE")
    if ctx == "NONE":
        return mask
    if ctx == "EXCLUDE_WEDNESDAY":
        return mask & (f["ldn_weekday"] != 2)
    if ctx.startswith("H1_CLOSE_LT_EMA"):
        period = int(ctx.rsplit("EMA", 1)[1])
        key = f"h1_ema{period}"
        if key not in f:
            raise KeyError(f"Missing H1 EMA feature for period {period}")
        return mask & np.isfinite(f[key]) & (f["h1_close"] < f[key])
    raise ValueError(f"Unknown context in focused confirmation: {ctx}")


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
        "h1_ema_period",
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
# PARAMETER / STAGE SUMMARIES
# ============================================================

def parameter_summary(rows, stage):
    out = []
    if not rows:
        return out
    for key in ["br_min", "body_atr_min", "structure_lb", "structure_dist_atr_max", "h1_ema_period", "rr"]:
        grouped = defaultdict(list)
        for r in rows:
            val = r.get(key)
            if val is not None and not (isinstance(val, float) and np.isnan(val)):
                grouped[val].append(r)
        for val, sub in grouped.items():
            out.append({
                "stage": stage,
                "parameter": key,
                "value": val,
                "configs": len(sub),
                "median_accepted_pf": round(med([r["accepted_pf"] for r in sub]), 6),
                "median_accepted_r": round(med([r["accepted_r"] for r in sub]), 4),
                "median_combined_pf": round(med([r["combined_pf"] for r in sub]), 6),
                "median_combined_r": round(med([r["combined_r"] for r in sub]), 4),
                "median_combined_dd": round(med([r["combined_dd"] for r in sub]), 4),
                "positive_pre_post_pct": round(100.0 * sum(
                    r["accepted_pre2010_r"] > 0 and r["accepted_post2010_r"] > 0 for r in sub
                ) / len(sub), 4),
                "four_positive_eras_pct": round(100.0 * sum(
                    r["accepted_positive_eras"] == 4 for r in sub
                ) / len(sub), 4),
            })
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
        STATUS.update({"state": "download", "message": "Downloading EUR/GBP full history"})
        m15 = fetch("M15", START, NOW, 35)
        h1 = fetch("H1", WARMUP, NOW, 180)
        h4 = fetch("H4", WARMUP, NOW, 700)
        daily = fetch("D", WARMUP, NOW, 3000)
        if not all([m15, h1, h4, daily]):
            raise RuntimeError("Missing required EUR_GBP history")

        # Hard coverage guard.  OANDA caps candles returned per request; if a
        # future edit makes a chunk too large, the fetch loop can otherwise
        # skip 400 responses and leave only a recent tail.  This guard makes
        # that failure explicit before any parity/strategy logic runs.
        if (
            len(m15) < 500000
            or m15[0]["time"] > datetime(2002, 6, 1, tzinfo=timezone.utc)
            or len(h1) < 100000
            or len(h4) < 25000
        ):
            raise RuntimeError(
                "Incomplete OANDA history: "
                f"M15={len(m15)} first={iso(m15[0]['time']) if m15 else 'NONE'}; "
                f"H1={len(h1)}; H4={len(h4)}. "
                "Check candle chunk sizes / OANDA download coverage."
            )

        write_csv(OUTS["coverage"], [{
            "instrument": PAIR,
            "requested_start_utc": iso(START),
            "parity_cutoff_utc": iso(PARITY_CUTOFF),
            "actual_first_m15_utc": iso(m15[0]["time"]),
            "actual_last_m15_utc": iso(m15[-1]["time"]),
            "m15_candles": len(m15),
            "h1_candles": len(h1),
            "h4_candles": len(h4),
            "daily_candles": len(daily),
        }])

        STATUS.update({"state": "precompute", "message": "Building causal HTF and M15 feature cache"})
        times = [x["time"] for x in m15]
        ah1 = align_htf(times, htf_state(h1))
        ah4 = align_htf(times, htf_state(h4))
        ad = align_htf(times, htf_state(daily))
        f = features(m15, ah1, ah4, ad)

        core_cfg = frozen_core_cfg()
        anchor = anchor_cfg()
        core_ix = signal_indices(core_cfg, f)
        anchor_ix = signal_indices(anchor, f)

        # ----------------------------------------------------
        # HARD PARITY
        # ----------------------------------------------------
        # IMPORTANT: the uploaded complementary-frequency run only hard-
        # parity-locked the core at 2026-09-10 12:30 UTC.  Its headline
        # 41 candidate / 40 accepted / 84 combined figures were FULL-RUN
        # outputs at 13:01 UTC, not fixed-cutoff parity observations.
        #
        # The uploaded finalist trade file *does* give us a clean historical
        # anchor: by 2026-03-01 there were exactly 43 core trades and all 40
        # accepted engulf-complement trades had already occurred (the last
        # accepted complement signal was 2026-02-17 03:45 UTC).  We therefore
        # hard-check only quantities actually supported by the prior files.
        core_cut = run_backtest(
            m15, core_ix, CORE_RR, PRIMARY_COST,
            m15[0]["time"], PARITY_CUTOFF,
        )
        core_hist = run_backtest(
            m15, core_ix, CORE_RR, PRIMARY_COST,
            m15[0]["time"], ANCHOR_HISTORY_CUTOFF,
        )
        anchor_hist = run_backtest(
            m15, anchor_ix, ANCHOR_RR, PRIMARY_COST,
            m15[0]["time"], ANCHOR_HISTORY_CUTOFF,
        )
        combined_hist, accepted_hist, rejected_hist = nonoverlap_overlay(
            core_hist, anchor_hist
        )

        # Current/full anchor counts are useful diagnostics, but are not a
        # hard parity requirement because the prior 41/40/84 snapshot was
        # taken at a later full-run timestamp rather than PARITY_CUTOFF.
        anchor_current = run_backtest(
            m15, anchor_ix, ANCHOR_RR, PRIMARY_COST,
            m15[0]["time"], NOW,
        )
        core_current = run_backtest(
            m15, core_ix, CORE_RR, PRIMARY_COST,
            m15[0]["time"], NOW,
        )
        combined_current, accepted_current, rejected_current = nonoverlap_overlay(
            core_current, anchor_current
        )

        parity_rows = [
            {
                "check": "FROZEN_CORE_44",
                "expected": 44,
                "actual": len(core_cut),
                "status": "MATCH" if len(core_cut) == 44 else "MISMATCH",
                "hard_guard": True,
                "cutoff_utc": iso(PARITY_CUTOFF),
            },
            {
                "check": "HISTORICAL_CORE_BY_2026_03_01",
                "expected": 43,
                "actual": len(core_hist),
                "status": "MATCH" if len(core_hist) == 43 else "MISMATCH",
                "hard_guard": True,
                "cutoff_utc": iso(ANCHOR_HISTORY_CUTOFF),
            },
            {
                "check": "HISTORICAL_ENGULF_ACCEPTED_BY_2026_03_01",
                "expected": 40,
                "actual": len(accepted_hist),
                "status": "MATCH" if len(accepted_hist) == 40 else "MISMATCH",
                "hard_guard": True,
                "cutoff_utc": iso(ANCHOR_HISTORY_CUTOFF),
            },
            {
                "check": "HISTORICAL_COMBINED_BY_2026_03_01",
                "expected": 83,
                "actual": len(combined_hist),
                "status": "MATCH" if len(combined_hist) == 83 else "MISMATCH",
                "hard_guard": True,
                "cutoff_utc": iso(ANCHOR_HISTORY_CUTOFF),
            },
            {
                "check": "CURRENT_ENGULF_CANDIDATE_DIAGNOSTIC",
                "expected": 41,
                "actual": len(anchor_current),
                "status": "MATCH" if len(anchor_current) == 41 else "CURRENT_DIFF",
                "hard_guard": False,
                "cutoff_utc": iso(NOW),
            },
            {
                "check": "CURRENT_ENGULF_ACCEPTED_DIAGNOSTIC",
                "expected": 40,
                "actual": len(accepted_current),
                "status": "MATCH" if len(accepted_current) == 40 else "CURRENT_DIFF",
                "hard_guard": False,
                "cutoff_utc": iso(NOW),
            },
            {
                "check": "CURRENT_ENGULF_OVERLAP_DIAGNOSTIC",
                "expected": 1,
                "actual": len(rejected_current),
                "status": "MATCH" if len(rejected_current) == 1 else "CURRENT_DIFF",
                "hard_guard": False,
                "cutoff_utc": iso(NOW),
            },
            {
                "check": "CURRENT_COMBINED_DIAGNOSTIC",
                "expected": 84,
                "actual": len(combined_current),
                "status": "MATCH" if len(combined_current) == 84 else "CURRENT_DIFF",
                "hard_guard": False,
                "cutoff_utc": iso(NOW),
            },
        ]
        write_csv(OUTS["parity"], parity_rows)
        hard_failures = [
            r for r in parity_rows
            if r.get("hard_guard") and r["status"] != "MATCH"
        ]
        if hard_failures:
            details = "; ".join(
                f'{r["check"]}: expected {r["expected"]}, got {r["actual"]}'
                for r in hard_failures
            )
            raise RuntimeError("Parity failure: " + details)

        core_full = run_backtest(m15, core_ix, CORE_RR, PRIMARY_COST, m15[0]["time"], NOW)
        core_stats = stats(core_full)
        write_csv(OUTS["core"], [{
            "strategy": "FROZEN_CORE",
            "rr": CORE_RR,
            **{k: round(v, 6) if isinstance(v, float) else v for k, v in core_stats.items()},
            "note": "Core is frozen. No Trigger-A parameter is searched or loosened.",
        }])

        param_rows = []

        # ----------------------------------------------------
        # STAGE 1A: BR x BODY around exact anchor
        # ----------------------------------------------------
        stage1a = build_stage1_br_body()
        by1a = {c["config_id"]: c for c in stage1a}
        rows1a = []
        for n, c in enumerate(stage1a, 1):
            STATUS.update({"state": "stage1_br_body", "message": f"BR/body {n}/{len(stage1a)}"})
            cand_ix = signal_indices(c, f)
            cand = run_backtest(m15, cand_ix, c["rr"], PRIMARY_COST, m15[0]["time"], NOW)
            rows1a.append(evaluation_row(c, core_full, cand))
        rows1a = sort_rows(rows1a)
        write_csv(OUTS["stage1_br_body"], rows1a)
        param_rows.extend(parameter_summary(rows1a, "STAGE1_BR_BODY"))

        # ----------------------------------------------------
        # STAGE 1B: STRUCTURE x DISTANCE around exact anchor
        # ----------------------------------------------------
        stage1b = build_stage1_structure_distance()
        by1b = {c["config_id"]: c for c in stage1b}
        rows1b = []
        for n, c in enumerate(stage1b, 1):
            STATUS.update({"state": "stage1_structure", "message": f"Structure/distance {n}/{len(stage1b)}"})
            cand_ix = signal_indices(c, f)
            cand = run_backtest(m15, cand_ix, c["rr"], PRIMARY_COST, m15[0]["time"], NOW)
            rows1b.append(evaluation_row(c, core_full, cand))
        rows1b = sort_rows(rows1b)
        write_csv(OUTS["stage1_structure"], rows1b)
        param_rows.extend(parameter_summary(rows1b, "STAGE1_STRUCTURE_DISTANCE"))

        # ----------------------------------------------------
        # STAGE 2: controlled geometry interactions
        # ----------------------------------------------------
        stage2 = build_stage2_geometry(rows1a, rows1b, by1a, by1b)
        by2 = {c["config_id"]: c for c in stage2}
        rows2 = []
        for n, c in enumerate(stage2, 1):
            STATUS.update({"state": "stage2_geometry", "message": f"Geometry interaction {n}/{len(stage2)}"})
            cand_ix = signal_indices(c, f)
            cand = run_backtest(m15, cand_ix, c["rr"], PRIMARY_COST, m15[0]["time"], NOW)
            rows2.append(evaluation_row(c, core_full, cand))
        rows2 = sort_rows(rows2)
        write_csv(OUTS["stage2_geometry"], rows2)
        param_rows.extend(parameter_summary(rows2, "STAGE2_GEOMETRY"))

        # ----------------------------------------------------
        # STAGE 3: H1 bearish-regime confirmation + NONE ablation
        # ----------------------------------------------------
        stage3 = build_stage3_h1(rows2, by2)
        by3 = {c["config_id"]: c for c in stage3}
        rows3 = []
        for n, c in enumerate(stage3, 1):
            STATUS.update({"state": "stage3_h1", "message": f"H1 regime {n}/{len(stage3)}"})
            cand_ix = signal_indices(c, f)
            cand = run_backtest(m15, cand_ix, c["rr"], PRIMARY_COST, m15[0]["time"], NOW)
            rows3.append(evaluation_row(c, core_full, cand))
        rows3 = sort_rows(rows3)
        write_csv(OUTS["stage3_h1"], rows3)
        param_rows.extend(parameter_summary(rows3, "STAGE3_H1_REGIME"))

        # ----------------------------------------------------
        # STAGE 4: RR + explicit 2-pip survival
        # ----------------------------------------------------
        stage4 = build_stage4_rr(rows3, by3)
        by4 = {c["config_id"]: c for c in stage4}
        rows4 = []
        core_full_2p = run_backtest(m15, core_ix, CORE_RR, 2.0, m15[0]["time"], NOW)
        for n, c in enumerate(stage4, 1):
            STATUS.update({"state": "stage4_rr", "message": f"RR confirmation {n}/{len(stage4)}"})
            cand_ix = signal_indices(c, f)
            cand = run_backtest(m15, cand_ix, c["rr"], PRIMARY_COST, m15[0]["time"], NOW)
            row = evaluation_row(c, core_full, cand)

            cand_2p = run_backtest(m15, cand_ix, c["rr"], 2.0, m15[0]["time"], NOW)
            combined_2p, accepted_2p, rejected_2p = nonoverlap_overlay(core_full_2p, cand_2p)
            a2, c2 = stats(accepted_2p), stats(combined_2p)
            row.update({
                "accepted_2p_trades": a2["trades"],
                "accepted_2p_pf": round(a2["profit_factor"], 6),
                "accepted_2p_r": round(a2["total_r"], 4),
                "combined_2p_pf": round(c2["profit_factor"], 6),
                "combined_2p_r": round(c2["total_r"], 4),
                "combined_2p_dd": round(c2["max_drawdown_r"], 4),
                "rejected_overlap_2p": len(rejected_2p),
            })
            rows4.append(row)

        rows4 = sorted(rows4, key=lambda r: (
            r["accepted_2p_r"] > 0,
            r["accepted_pre2010_r"] > 0,
            r["accepted_post2010_r"] > 0,
            r["accepted_positive_eras"],
            r["accepted_pf"],
            r["combined_pf"],
            r["accepted_adds"],
        ), reverse=True)
        write_csv(OUTS["stage4_rr"], rows4)
        param_rows.extend(parameter_summary(rows4, "STAGE4_RR"))
        write_csv(OUTS["parameter_summary"], param_rows)

        # ----------------------------------------------------
        # ROBUST FINALISTS
        # ----------------------------------------------------
        eligible = [r for r in rows4 if (
            r["accepted_adds"] >= 12
            and r["accepted_r"] > 0
            and r["accepted_pf"] >= 1.25
            and r["accepted_pre2010_r"] > 0
            and r["accepted_post2010_r"] > 0
            and r["accepted_positive_eras"] >= 3
            and r["accepted_2p_r"] > 0
            and r["accepted_2p_pf"] >= 1.20
            and r["combined_pf"] >= 2.0
            and r["combined_2p_pf"] >= 1.75
            and r["combined_r"] > core_stats["total_r"]
            and r["combined_dd"] >= -10.0
        )]
        finalist_rows = (eligible if eligible else rows4)[:FINAL_KEEP]
        finalists = [by4[r["config_id"]] for r in finalist_rows]
        write_csv(OUTS["finalists"], finalist_rows)

        periods, costs, rolling, calendar, overlap, trade_rows = [], [], [], [], [], []
        for n, c in enumerate(finalists, 1):
            STATUS.update({"state": "deep_validation", "message": f"Deep finalist {n}/{len(finalists)}"})
            cand_ix = signal_indices(c, f)
            cand = run_backtest(m15, cand_ix, c["rr"], PRIMARY_COST, m15[0]["time"], NOW)
            combined, accepted, rejected = nonoverlap_overlay(core_full, cand)

            periods.extend(deep_period_rows(c, core_full, cand, m15[0]["time"]))
            costs.extend(deep_cost_rows(c, m15, core_ix, cand_ix, m15[0]["time"]))
            rolling.extend(rolling_rows(c, m15, core_ix, cand_ix, m15[0]["time"]))
            calendar.extend(calendar_rows(c, m15, core_ix, cand_ix))
            overlap.append({
                "config_id": c["config_id"],
                "family": c["family"],
                "context": c["context"],
                "h1_ema_period": c.get("h1_ema_period"),
                "rr": c["rr"],
                "candidate_trades": len(cand),
                "accepted_nonoverlap": len(accepted),
                "rejected_overlap": len(rejected),
                "overlap_rate_pct": round(100 * len(rejected) / len(cand), 4) if cand else 0.0,
                "combined_trades": len(combined),
            })
            for source, trades in [("CORE", core_full), ("ACCEPTED_COMPLEMENT", accepted), ("COMBINED", combined)]:
                for t in trades:
                    x = dict(t)
                    x.update({
                        "config_id": c["config_id"],
                        "family": c["family"],
                        "context": c["context"],
                        "h1_ema_period": c.get("h1_ema_period"),
                        "source": source,
                    })
                    trade_rows.append(x)

        write_csv(OUTS["periods"], periods)
        write_csv(OUTS["cost"], costs)
        write_csv(OUTS["rolling"], rolling)
        write_csv(OUTS["rolling_summary"], rolling_summary(rolling))
        write_csv(OUTS["calendar"], calendar)
        write_csv(OUTS["calendar_summary"], calendar_summary(calendar))
        write_csv(OUTS["overlap"], overlap)
        write_csv(OUTS["trades"], trade_rows)
        write_csv(OUTS["notes"], [
            {"note": "BR-only test freezes body1.00/LB100/dist0.15/H1 EMA100/RR2.50 and varies only BR 1.40/1.50/1.60/1.70/1.80/2.00."},
            {"note": "Source anchor from uploaded complement run: BR1.40/body1.00/LB100/dist0.20/H1 close<EMA100/RR2.50; prior full-run snapshot was 41 candidate, 40 accepted, 1 overlap, 84 combined at 2026-09-10 13:01 UTC."},
            {"note": "Hard parity uses only source-supported fixed historical observations: core=44 through 2026-09-10 12:30 UTC; by 2026-03-01 core=43, accepted complement=40, combined=83. Current 41/40/1/84 figures are diagnostic only."},
            {"note": "No session or weekday optimisation is performed for the complement."},
            {"note": "H1 NONE is explicitly tested as an ablation to quantify how much the bearish regime gate contributes."},
            {"note": "H1 state uses only the previous completed H1 candle via completion-time alignment; no lookahead."},
            {"note": "2005/2009/2013/2023 activity is report-only and is not part of the selection score."},
            {"note": "Candidate trade [signal,exit) overlapping a frozen core trade is rejected; exact core exit-candle signals remain eligible."},
            {"note": "Choose a final complement on broad parameter/regime/RR plateau plus temporal, rolling and cost robustness, not the highest lifetime PF/R."},
            {"note": "Historical period splits are robustness diagnostics, not pristine OOS, because the full history is now part of development."},
        ])

        pack()
        STATUS.update({
            "state": "complete",
            "message": "EUR/GBP M15 SHORT final BR-only complement confirmation complete",
            "core_trades": len(core_full),
            "stage1_br_body_configs": len(stage1a),
            "stage1_structure_configs": len(stage1b),
            "stage2_geometry_configs": len(stage2),
            "stage3_h1_configs": len(stage3),
            "stage4_rr_configs": len(stage4),
            "finalists": len(finalists),
            "results_bundle": BUNDLE,
            "orders_supported": False,
            "trading_enabled": False,
        })

    except Exception as e:
        import traceback
        STATUS.update({
            "state": "error",
            "message": str(e),
            "error_type": type(e).__name__,
            "traceback": traceback.format_exc(),
            "orders_supported": False,
            "trading_enabled": False,
        })
        print("ERROR:", traceback.format_exc(), flush=True)


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def root():
    payload = {
        "service": "EUR/GBP M15 SHORT Final BR-Only Complement Confirmation",
        "state": STATUS["state"],
        "instrument": PAIR,
        "timeframe": "M15",
        "side": "SELL",
        "orders_supported": False,
        "trading_enabled": False,
        "routes": [
            "/eurgbp-m15-short-complement-final-br-only/status",
            "/eurgbp-m15-short-complement-final-br-only/results",
        ],
    }
    if STATUS.get("state") == "error":
        payload.update({
            "error_type": STATUS.get("error_type"),
            "message": STATUS.get("message"),
            "traceback": STATUS.get("traceback"),
        })
    return jsonify(payload)


@app.route("/eurgbp-m15-short-complement-final-br-only/status")
def route_status():
    return jsonify(STATUS)


@app.route("/eurgbp-m15-short-complement-final-br-only/results")
def route_results():
    return dl(BUNDLE)


if __name__ == "__main__":
    thread = threading.Thread(
        target=run_research,
        name="eurgbp-m15-short-final-engulf-complement-confirmation",
        daemon=True,
    )
    thread.start()
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

