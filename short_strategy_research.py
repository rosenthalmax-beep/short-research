#!/usr/bin/env python3
"""
AUD/JPY H1 SHORT — Pass 5 independent confirmation
===================================================

RESEARCH ONLY. READ ONLY. NEVER PLACES ORDERS OR MODIFIES LIVE STRATEGIES.

Purpose
-------
Independently reimplement and confirm the two frozen RR3.25 candidates produced
before portfolio admission. This runner does NOT import or call the exploratory
Pass 1-4 engines. Signal qualification, daily as-of state, exits, p0 replay,
metrics and ledger hashing are rebuilt here in a compact scalar implementation.

Frozen PRIMARY
--------------
- exact bearish engulf
- previous 175 H1-bar high (excluding signal candle)
- ABS(signal high - previous 175-bar high) <= 0.10 ATR14
- bearish body >= 0.60 ATR14
- signal range >= 1.25 ATR14
- previous H1 candle close location >= 0.50
- RR 3.25
- stop = signal high + 10 ticks

Frozen QUALITY
--------------
- all PRIMARY rules
- previous completed D1 ATR14 / previous-50-D1 ATR14 mean <= 1.15

Execution model
---------------
- OANDA AUD_JPY MID completed candles
- historical adverse SHORT fills: 10T / 20T / 40T = 1 / 2 / 4 pips
- target derived from reference close-to-stop risk; historical fill changes
  realised R denominator but does not move the frozen target
- p0 per strategy; signal on the exit candle is eligible
- same-candle stop+target tie uses the frozen distance-from-open heuristic
- frozen cutoff 2026-09-30 08:00 UTC exclusive
- D1 source frozen through the exact completed D1 open 2026-09-28 21:00 UTC

This is implementation parity on repeatedly examined historical data, not fresh
out-of-sample evidence and not live-trading authorisation.
"""

from __future__ import annotations

import bisect
import csv
import hashlib
import math
import os
import threading
import time
import traceback
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import requests
from flask import Flask, jsonify, send_file

# ============================================================
# FROZEN PROTOCOL / EXPECTED FINGERPRINTS
# ============================================================

PAIR = "AUD_JPY"
TIMEFRAME = "H1"
SIDE = "SHORT"
REQUESTED_START = datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc)
DATA_END = datetime(2026, 9, 30, 8, 0, tzinfo=timezone.utc)  # exclusive
D1_WARMUP_START = REQUESTED_START - timedelta(days=900)
EXPECTED_D1_LAST_OPEN = datetime(2026, 9, 28, 21, 0, tzinfo=timezone.utc)

TICK = 0.001
PIP = 0.01
STOP_BUFFER_TICKS = 10
RR = 3.25
ATR_LENGTH = 14
STRUCTURE_LOOKBACK = 175
MAX_STRUCTURE_DISTANCE_ATR = 0.10
MIN_BODY_ATR = 0.60
MIN_RANGE_ATR = 1.25
MIN_PREVIOUS_CLOSE_LOCATION = 0.50
QUALITY_MAX_D1_ATR_RATIO = 1.15

COST_CASES = (
    ("LIVE_LIMIT_10T", 10, 1.0, "PRIMARY_LIVE_PARITY"),
    ("STRESS_20T", 20, 2.0, "STRESSED_SELECTION"),
    ("EXTREME_40T", 40, 4.0, "EXTREME_DIAGNOSTIC_ONLY"),
)

EXPECTED_H1_ROWS = 138030
EXPECTED_D1_ROWS = 6476
EXPECTED_H1_SHA256 = "5b844d2f79af6ae49e7f36586f68c366d4884859f897792bdb09ebe4e7bfcdfe"
EXPECTED_D1_SHA256 = "bb43365840ff5d38d8d3ed3260c4d74204a927780e4619074ea3f9e63bcaba59"
EXPECTED_RAW_SIGNAL_COUNT = 10465
EXPECTED_RAW_SIGNAL_SHA256 = "fc53892ad7e01e06b49594108b8b1fe8047ade46ec33e0d3a8d554c84322f3ab"

EXPECTED = {
    "PRIMARY": {
        "qualified": 94,
        "qualified_sha256": "faecaaee693022784dcd1dc7ada2aef768198f69b34d9e5487556bd68896b929",
        "accepted": 91,
        "accepted_sequence_sha256": "f63cfcd3ab8d4603595f802728b656b2c5b11bf7c2649037e168b591d4ba50f8",
        "LIVE_LIMIT_10T": {
            "winners": 36, "losers": 55,
            "total_r": 56.2481156001,
            "profit_factor": 2.0226930109,
            "max_drawdown_r": -8.0719696970,
            "longest_losing_streak": 8,
            "ledger_sha256": "2723d577e8fd6b224cce3d9ce0b48a99551468b800875ea83e4cb6c7734a761e",
        },
        "STRESS_20T": {
            "winners": 36, "losers": 55,
            "total_r": 50.9822037922,
            "profit_factor": 1.9269491599,
            "max_drawdown_r": -8.3485915493,
            "longest_losing_streak": 8,
            "ledger_sha256": "8f0095bb318c0914b38e30a4071d3b7b99404524f70bafa5220cbd194310df13",
        },
        "EXTREME_40T": {
            "winners": 36, "losers": 55,
            "total_r": 41.6579789952,
            "profit_factor": 1.7574177999,
            "max_drawdown_r": -8.7993827160,
            "longest_losing_streak": 8,
            "ledger_sha256": "21a29d35ea90da9362b75544ad76a4a853445749d5e2dd028cb5ae559ad891cb",
        },
    },
    "QUALITY": {
        "qualified": 81,
        "qualified_sha256": "2ae1c75047528353d4631fdd368a0067fff2d25664bcf2b39b7d735db32148f2",
        "accepted": 78,
        "accepted_sequence_sha256": "b1a93e71a4804bf3acb2f8ef86100f7b0bf6349c06c08201484f370d429ad2fe",
        "LIVE_LIMIT_10T": {
            "winners": 33, "losers": 45,
            "total_r": 56.7129844200,
            "profit_factor": 2.2602885427,
            "max_drawdown_r": -5.0719696970,
            "longest_losing_streak": 5,
            "ledger_sha256": "19598e7b0c18582a454c4f3485937b1a4d8cdc278021476dfd3d2a3d546eeeee",
        },
        "STRESS_20T": {
            "winners": 33, "losers": 45,
            "total_r": 51.6542714662,
            "profit_factor": 2.1478726992,
            "max_drawdown_r": -5.3485915493,
            "longest_losing_streak": 5,
            "ledger_sha256": "708c7342960ddc565bcea0453be84b17d6f07172b3be97ac93da1fbbbd54ec79",
        },
        "EXTREME_40T": {
            "winners": 33, "losers": 45,
            "total_r": 42.7231299355,
            "profit_factor": 1.9494028875,
            "max_drawdown_r": -5.7993827160,
            "longest_losing_streak": 5,
            "ledger_sha256": "aa9a827d01315caf3d4b9a29f8c42134756792ba79f7b08554faeeb017c2caa2",
        },
    },
}

PASS_VERSION = "AUDJPY_H1_SHORT_PASS5_INDEPENDENT_CONFIRMATION_V1_2026-09-30"

API = os.getenv("OANDA_API_URL", "https://api-fxtrade.oanda.com").rstrip("/")
TOKEN = os.getenv("OANDA_TOKEN", "")
OUT_DIR = Path(os.getenv("AUDJPY_H1_SHORT_PASS5_OUTPUT_DIR", "/tmp/audjpy_h1_short_pass5"))
OUT_DIR.mkdir(parents=True, exist_ok=True)
BUNDLE = OUT_DIR / "AUDJPY_H1_SHORT_PASS5_INDEPENDENT_CONFIRMATION_RESULTS.zip"

OUTPUTS = {
    "coverage": OUT_DIR / "coverage.csv",
    "source": OUT_DIR / "source_fingerprint.csv",
    "hard_parity": OUT_DIR / "hard_parity.csv",
    "raw_parity": OUT_DIR / "raw_signal_parity.csv",
    "final_parity": OUT_DIR / "final_strategy_parity.csv",
    "summary": OUT_DIR / "final_summary.csv",
    "ledgers": OUT_DIR / "final_full_accepted_ledgers.csv",
    "years": OUT_DIR / "final_calendar_years.csv",
    "periods": OUT_DIR / "final_periods.csv",
    "rolling": OUT_DIR / "final_rolling_summary.csv",
    "methodology": OUT_DIR / "methodology.csv",
    "errors": OUT_DIR / "error_report.csv",
}

STATUS = {
    "state": "not_started",
    "progress": 0,
    "message": "AUD/JPY H1 SHORT independent confirmation waiting",
    "orders_supported": False,
    "trading_enabled": False,
    "pair": PAIR,
    "timeframe": TIMEFRAME,
    "side": SIDE,
    "rr": RR,
    "variants": ["PRIMARY", "QUALITY"],
    "pass_version": PASS_VERSION,
    "frozen_cutoff": DATA_END.isoformat().replace("+00:00", "Z"),
}
STATUS_LOCK = threading.Lock()
RUN_LOCK = threading.Lock()
RUN_STARTED = False

app = Flask(__name__)

# ============================================================
# GENERIC HELPERS
# ============================================================

def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_time(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def set_status(**kwargs):
    with STATUS_LOCK:
        STATUS.update(kwargs)


def sha_lines(lines) -> str:
    h = hashlib.sha256()
    for line in lines:
        h.update((str(line) + "\n").encode("utf-8"))
    return h.hexdigest()


def write_csv(path: Path, rows):
    rows = list(rows)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields, seen = [], set()
    for row in rows:
        for key in row:
            if key not in seen:
                fields.append(key)
                seen.add(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def pack_results():
    with zipfile.ZipFile(BUNDLE, "w", zipfile.ZIP_DEFLATED) as z:
        for p in OUTPUTS.values():
            if p.exists():
                z.write(p, arcname=p.name)


def month_floor(dt: datetime) -> datetime:
    return datetime(dt.year, dt.month, 1, tzinfo=timezone.utc)


def add_months(dt: datetime, n: int) -> datetime:
    m = dt.year * 12 + (dt.month - 1) + n
    return datetime(m // 12, m % 12 + 1, 1, tzinfo=timezone.utc)

# ============================================================
# OANDA MID HISTORY
# ============================================================

def auth_headers():
    if not TOKEN:
        raise RuntimeError("OANDA_TOKEN is not configured")
    return {"Authorization": f"Bearer {TOKEN.strip()}"}


def fetch_chunk(granularity: str, start: datetime, end: datetime):
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
    r = requests.get(
        f"{API}/v3/instruments/{PAIR}/candles",
        headers=auth_headers(), params=params, timeout=60,
    )
    r.raise_for_status()
    rows = []
    for c in r.json().get("candles", []):
        if not c.get("complete", False) or not c.get("mid"):
            continue
        mid = c["mid"]
        rows.append({
            "time": parse_time(c["time"]),
            "open": float(mid["o"]),
            "high": float(mid["h"]),
            "low": float(mid["l"]),
            "close": float(mid["c"]),
        })
    return rows


def fetch_history(granularity: str, start: datetime, end: datetime, chunk_days: int):
    cursor, by_time, chunk_no = start, {}, 0
    while cursor < end:
        chunk_no += 1
        nxt = min(cursor + timedelta(days=chunk_days), end)
        set_status(state="fetching", message=f"Fetching {granularity} chunk {chunk_no}: {iso(cursor)} -> {iso(nxt)}")
        last_exc = None
        rows = None
        for attempt in range(1, 4):
            try:
                rows = fetch_chunk(granularity, cursor, nxt)
                last_exc = None
                break
            except requests.HTTPError as exc:
                code = exc.response.status_code if exc.response is not None else None
                if code in (400, 404) and not by_time:
                    rows = []
                    last_exc = None
                    break
                last_exc = exc
            except Exception as exc:
                last_exc = exc
            if attempt < 3:
                time.sleep(0.75 * attempt)
        if last_exc is not None:
            raise last_exc
        for row in rows or []:
            if row["time"] < end:
                by_time[row["time"]] = row
        cursor = nxt
        time.sleep(0.02)
    out = sorted(by_time.values(), key=lambda x: x["time"])
    if len(out) < 100:
        raise RuntimeError(f"Insufficient {granularity} completed history: {len(out)}")
    return out

# ============================================================
# INDEPENDENT INDICATORS / DAILY AS-OF STATE
# ============================================================

def wilder_atr(candles, length=14):
    n = len(candles)
    out = [math.nan] * n
    if n < length:
        return out
    tr = [0.0] * n
    tr[0] = candles[0]["high"] - candles[0]["low"]
    for i in range(1, n):
        prev_close = candles[i - 1]["close"]
        tr[i] = max(
            candles[i]["high"] - candles[i]["low"],
            abs(candles[i]["high"] - prev_close),
            abs(candles[i]["low"] - prev_close),
        )
    # NumPy mean only for exact historical seed parity; recurrence below is scalar.
    out[length - 1] = float(np.mean(np.asarray(tr[:length], dtype=float)))
    for i in range(length, n):
        out[i] = (out[i - 1] * (length - 1) + tr[i]) / length
    return out


def previous_rolling_mean(values, lookback):
    out = [math.nan] * len(values)
    q = []
    total = 0.0
    bad = 0
    head = 0
    for i in range(len(values)):
        j = i - 1
        if j >= 0:
            v = values[j]
            q.append(v)
            if math.isfinite(v):
                total += v
            else:
                bad += 1
        while len(q) - head > lookback:
            old = q[head]
            head += 1
            if math.isfinite(old):
                total -= old
            else:
                bad -= 1
        if len(q) - head == lookback and bad == 0:
            out[i] = total / lookback
    return out


def build_daily_atr_ratio_asof(daily, h1_times):
    d_atr = wilder_atr(daily, ATR_LENGTH)
    d_prev50 = previous_rolling_mean(d_atr, 50)
    ratio = [math.nan] * len(daily)
    for i in range(len(daily)):
        if math.isfinite(d_atr[i]) and math.isfinite(d_prev50[i]) and d_prev50[i] > 0:
            ratio[i] = d_atr[i] / d_prev50[i]

    # Daily bar j is only available once the next aligned daily candle opens.
    completion_times = [daily[i + 1]["time"] for i in range(len(daily) - 1)]
    mapped = [math.nan] * len(h1_times)
    for i, t in enumerate(h1_times):
        j = bisect.bisect_right(completion_times, t) - 1
        if j >= 0:
            mapped[i] = ratio[j]
    return mapped

# ============================================================
# INDEPENDENT SIGNAL QUALIFICATION
# ============================================================

def is_exact_bearish_engulf(previous, current):
    return (
        previous["close"] > previous["open"]
        and current["close"] < current["open"]
        and current["open"] >= previous["close"]
        and current["close"] <= previous["open"]
    )


def raw_signal_indices(h1, atr):
    out = []
    for i in range(1, len(h1)):
        if math.isfinite(atr[i]) and atr[i] > 0 and is_exact_bearish_engulf(h1[i - 1], h1[i]):
            out.append(i)
    return out


def previous_close_location(h1, i):
    p = h1[i - 1]
    rng = p["high"] - p["low"]
    if rng <= 0:
        return math.nan
    return (p["close"] - p["low"]) / rng


def qualifies_primary(h1, atr, i):
    if i < STRUCTURE_LOOKBACK:
        return False
    a = atr[i]
    if not math.isfinite(a) or a <= 0:
        return False
    c = h1[i]
    body = c["open"] - c["close"]
    rng = c["high"] - c["low"]
    if body < MIN_BODY_ATR * a or rng < MIN_RANGE_ATR * a:
        return False
    prior_high = max(x["high"] for x in h1[i - STRUCTURE_LOOKBACK:i])
    if abs(c["high"] - prior_high) / a > MAX_STRUCTURE_DISTANCE_ATR:
        return False
    pcl = previous_close_location(h1, i)
    if not math.isfinite(pcl) or pcl < MIN_PREVIOUS_CLOSE_LOCATION:
        return False
    return True


def qualifying_indices(h1, atr, d1_ratio_asof, raw_indices, variant):
    q = []
    for i in raw_indices:
        if not qualifies_primary(h1, atr, i):
            continue
        if variant == "QUALITY":
            r = d1_ratio_asof[i]
            if not math.isfinite(r) or r > QUALITY_MAX_D1_ATR_RATIO:
                continue
        q.append(i)
    return q

# ============================================================
# INDEPENDENT EXIT / P0 REPLAY
# ============================================================

def find_short_exit(h1, signal_index, stop, target):
    for j in range(signal_index + 1, len(h1)):
        c = h1[j]
        stop_hit = c["high"] >= stop
        target_hit = c["low"] <= target
        if not stop_hit and not target_hit:
            continue
        if stop_hit and target_hit:
            # Frozen short-side same-candle tie convention.
            reason = "TARGET" if abs(c["open"] - c["low"]) < abs(c["high"] - c["open"]) else "STOP"
        else:
            reason = "STOP" if stop_hit else "TARGET"
        return j, reason
    return None, None


def replay_variant(h1, qualified_indices):
    accepted = []
    active_exit_index = -1
    censored = 0
    for i in qualified_indices:
        # p0 half-open interval [signal, exit): signal on exit candle is eligible.
        if i < active_exit_index:
            continue
        entry = h1[i]["close"]
        stop = h1[i]["high"] + STOP_BUFFER_TICKS * TICK
        ref_risk = stop - entry
        if ref_risk <= 0:
            raise RuntimeError(f"Invalid reference risk at {iso(h1[i]['time'])}")
        target = entry - RR * ref_risk
        exit_index, reason = find_short_exit(h1, i, stop, target)
        if exit_index is None:
            censored += 1
            continue
        accepted.append({
            "signal_index": i,
            "exit_index": exit_index,
            "signal_time": h1[i]["time"],
            "exit_time": h1[exit_index]["time"],
            "reference_entry": entry,
            "stop": stop,
            "target": target,
            "exit_reason": reason,
        })
        active_exit_index = exit_index
    return accepted, censored


def attach_cost_outcomes(records):
    for rec in records:
        for label, ticks, pips, purpose in COST_CASES:
            fill = rec["reference_entry"] - pips * PIP
            risk = rec["stop"] - fill
            if risk <= 0:
                raise RuntimeError(f"Invalid historical SHORT risk for {label} at {iso(rec['signal_time'])}")
            exit_price = rec["target"] if rec["exit_reason"] == "TARGET" else rec["stop"]
            rec[f"fill__{label}"] = fill
            rec[f"result_r__{label}"] = (fill - exit_price) / risk

# ============================================================
# HASHES / METRICS / DIAGNOSTICS
# ============================================================

def qualified_hash(h1, indices):
    return sha_lines(f"{iso(h1[i]['time'])}|{i}" for i in indices)


def accepted_sequence_hash(records):
    return sha_lines(
        f"{iso(r['signal_time'])}|{r['signal_index']}|{iso(r['exit_time'])}|{r['exit_index']}|{r['exit_reason']}"
        for r in records
    )


def accepted_ledger_hash(records, cost_label):
    return sha_lines(
        "|".join((
            iso(r["signal_time"]), iso(r["exit_time"]),
            str(r["signal_index"]), str(r["exit_index"]), r["exit_reason"],
            f'{r[f"result_r__{cost_label}"]:.15g}',
        ))
        for r in records
    )


def metrics(records, cost_label):
    vals = [float(r[f"result_r__{cost_label}"]) for r in records]
    if not vals:
        return dict(accepted_trades=0, winners=0, losers=0, win_rate_pct=0.0,
                    profit_factor=0.0, total_r=0.0, expectancy_r=0.0,
                    max_drawdown_r=0.0, longest_losing_streak=0)
    winners = sum(v > 0 for v in vals)
    losers = len(vals) - winners
    gp = sum(v for v in vals if v > 0)
    gl = -sum(v for v in vals if v < 0)
    pf = gp / gl if gl > 0 else (99.0 if gp > 0 else 0.0)
    cum = peak = 0.0
    max_dd = 0.0
    streak = longest = 0
    for v in vals:
        cum += v
        peak = max(peak, cum)
        max_dd = min(max_dd, cum - peak)
        if v <= 0:
            streak += 1
            longest = max(longest, streak)
        else:
            streak = 0
    return {
        "accepted_trades": len(vals), "winners": winners, "losers": losers,
        "win_rate_pct": 100.0 * winners / len(vals),
        "profit_factor": pf, "total_r": sum(vals),
        "expectancy_r": sum(vals) / len(vals), "max_drawdown_r": max_dd,
        "longest_losing_streak": longest,
    }


def records_in_period(records, start=None, end=None):
    out = []
    for r in records:
        t = r["signal_time"]
        if start is not None and t < start:
            continue
        if end is not None and t >= end:
            continue
        out.append(r)
    return out


def period_rows(variant, records, cost_label):
    windows = [
        ("PRE2010", None, datetime(2010, 1, 1, tzinfo=timezone.utc)),
        ("2010_2015", datetime(2010, 1, 1, tzinfo=timezone.utc), datetime(2016, 1, 1, tzinfo=timezone.utc)),
        ("2016_2021", datetime(2016, 1, 1, tzinfo=timezone.utc), datetime(2022, 1, 1, tzinfo=timezone.utc)),
        ("2022_PLUS", datetime(2022, 1, 1, tzinfo=timezone.utc), None),
        ("LAST5Y", DATA_END - timedelta(days=365.2425 * 5), None),
        ("LAST2Y", DATA_END - timedelta(days=365.2425 * 2), None),
    ]
    return [
        {"variant_id": variant, "cost_label": cost_label, "period": name,
         **metrics(records_in_period(records, start, end), cost_label)}
        for name, start, end in windows
    ]


def calendar_rows(variant, records, cost_label):
    first_year = min(r["signal_time"].year for r in records)
    rows = []
    for year in range(first_year, DATA_END.year + 1):
        start = datetime(year, 1, 1, tzinfo=timezone.utc)
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        sub = records_in_period(records, start, end)
        rows.append({
            "variant_id": variant, "cost_label": cost_label, "year": year,
            "complete_year": year < DATA_END.year, "zero_trade_year": len(sub) == 0,
            **metrics(sub, cost_label),
        })
    return rows


def rolling_summary_rows(variant, records, cost_label):
    # Match the frozen research window origin: first raw exact signal was Jan 2005.
    first_month = datetime(2005, 1, 1, tzinfo=timezone.utc)
    last_complete_month = month_floor(DATA_END)
    out = []
    for months in (12, 24, 36):
        cursor = first_month
        vals = []
        while add_months(cursor, months) <= last_complete_month:
            end = add_months(cursor, months)
            sub = records_in_period(records, cursor, end)
            m = metrics(sub, cost_label)
            vals.append((cursor, end, m, len(sub) == 0))
            cursor = add_months(cursor, 1)
        weakest = min(vals, key=lambda x: x[2]["total_r"])
        out.append({
            "variant_id": variant, "cost_label": cost_label, "window_months": months,
            "windows": len(vals), "positive_windows": sum(x[2]["total_r"] > 0 for x in vals),
            "negative_windows": sum(x[2]["total_r"] < 0 for x in vals),
            "zero_trade_windows": sum(x[3] for x in vals),
            "worst_total_r": weakest[2]["total_r"],
            "worst_start": iso(weakest[0]), "worst_end_exclusive": iso(weakest[1]),
        })
    return out

# ============================================================
# MAIN CONFIRMATION
# ============================================================

def run_confirmation():
    try:
        set_status(state="fetching", progress=2, message="Fetching frozen H1/D1 OANDA midpoint history")
        h1 = fetch_history("H1", REQUESTED_START, DATA_END, 180)
        d1_all = fetch_history("D", D1_WARMUP_START, DATA_END, 1200)
        d1 = [x for x in d1_all if x["time"] <= EXPECTED_D1_LAST_OPEN]

        if [x["time"] for x in h1] != sorted({x["time"] for x in h1}):
            raise RuntimeError("H1 timestamps duplicated or non-monotonic")
        if [x["time"] for x in d1] != sorted({x["time"] for x in d1}):
            raise RuntimeError("D1 timestamps duplicated or non-monotonic")
        if not h1 or not d1 or h1[-1]["time"] >= DATA_END:
            raise RuntimeError("Frozen source coverage invalid")

        h1_sha = sha_lines(
            f"{iso(x['time'])}|{x['open']:.6f}|{x['high']:.6f}|{x['low']:.6f}|{x['close']:.6f}" for x in h1
        )
        d1_sha = sha_lines(
            f"{iso(x['time'])}|{x['open']:.6f}|{x['high']:.6f}|{x['low']:.6f}|{x['close']:.6f}" for x in d1
        )

        write_csv(OUTPUTS["coverage"], [
            {"pair": PAIR, "timeframe": "H1", "requested_start": iso(REQUESTED_START),
             "first_completed_candle": iso(h1[0]["time"]), "last_completed_candle_open": iso(h1[-1]["time"]),
             "frozen_end_exclusive": iso(DATA_END), "completed_candles": len(h1)},
            {"pair": PAIR, "timeframe": "D", "requested_start": iso(D1_WARMUP_START),
             "first_completed_candle": iso(d1[0]["time"]), "last_completed_candle_open": iso(d1[-1]["time"]),
             "frozen_end_exclusive": iso(DATA_END), "completed_candles": len(d1),
             "daily_alignment": "17:00 America/New_York", "fetched_complete_rows_before_freeze": len(d1_all)},
        ])
        write_csv(OUTPUTS["source"], [
            {"series": "AUD_JPY_H1_MID_OHLC", "sha256": h1_sha, "rows": len(h1)},
            {"series": "AUD_JPY_D1_MID_OHLC_FROZEN_ASOF_PASS3", "sha256": d1_sha, "rows": len(d1)},
        ])

        hard = [
            {"check": "H1_ROW_COUNT", "status": "PASS" if len(h1) == EXPECTED_H1_ROWS else "FAIL", "actual": len(h1), "expected": EXPECTED_H1_ROWS},
            {"check": "D1_ROW_COUNT_AFTER_FREEZE", "status": "PASS" if len(d1) == EXPECTED_D1_ROWS else "FAIL", "actual": len(d1), "expected": EXPECTED_D1_ROWS},
            {"check": "D1_LAST_OPEN_FREEZE", "status": "PASS" if d1[-1]["time"] == EXPECTED_D1_LAST_OPEN else "FAIL", "actual": iso(d1[-1]["time"]), "expected": iso(EXPECTED_D1_LAST_OPEN)},
            {"check": "H1_SOURCE_SHA256", "status": "PASS" if h1_sha == EXPECTED_H1_SHA256 else "FAIL", "actual": h1_sha, "expected": EXPECTED_H1_SHA256},
            {"check": "D1_SOURCE_SHA256", "status": "PASS" if d1_sha == EXPECTED_D1_SHA256 else "FAIL", "actual": d1_sha, "expected": EXPECTED_D1_SHA256},
        ]
        write_csv(OUTPUTS["hard_parity"], hard)
        if any(x["status"] != "PASS" for x in hard):
            raise RuntimeError("Frozen source parity FAILED")

        set_status(state="features", progress=12, message="Rebuilding ATR, daily as-of state and exact bearish-engulf stream independently")
        h1_atr = wilder_atr(h1, ATR_LENGTH)
        d1_ratio_asof = build_daily_atr_ratio_asof(d1, [x["time"] for x in h1])
        raw = raw_signal_indices(h1, h1_atr)
        raw_sha = sha_lines(iso(h1[i]["time"]) for i in raw)
        raw_ok = len(raw) == EXPECTED_RAW_SIGNAL_COUNT and raw_sha == EXPECTED_RAW_SIGNAL_SHA256
        write_csv(OUTPUTS["raw_parity"], [{
            "check": "INDEPENDENT_RAW_EXACT_BEARISH_ENGULF",
            "status": "PASS" if raw_ok else "FAIL",
            "actual_count": len(raw), "expected_count": EXPECTED_RAW_SIGNAL_COUNT,
            "actual_sha256": raw_sha, "expected_sha256": EXPECTED_RAW_SIGNAL_SHA256,
        }])
        if not raw_ok:
            raise RuntimeError("Independent raw-signal parity FAILED")

        set_status(state="confirmation", progress=25, message="Qualifying and replaying frozen PRIMARY / QUALITY RR3.25")
        summary_rows, parity_rows, ledger_rows = [], [], []
        year_rows, periods, rolling_rows = [], [], []

        for variant in ("PRIMARY", "QUALITY"):
            q = qualifying_indices(h1, h1_atr, d1_ratio_asof, raw, variant)
            q_sha = qualified_hash(h1, q)
            exp = EXPECTED[variant]
            q_ok = len(q) == exp["qualified"] and q_sha == exp["qualified_sha256"]
            parity_rows.append({
                "variant_id": variant, "stage": "QUALIFIED_SIGNAL_STREAM",
                "status": "PASS" if q_ok else "FAIL",
                "actual_count": len(q), "expected_count": exp["qualified"],
                "actual_sha256": q_sha, "expected_sha256": exp["qualified_sha256"],
            })
            if not q_ok:
                continue

            accepted, censored = replay_variant(h1, q)
            attach_cost_outcomes(accepted)
            seq_sha = accepted_sequence_hash(accepted)
            seq_ok = len(accepted) == exp["accepted"] and seq_sha == exp["accepted_sequence_sha256"] and censored == 0
            parity_rows.append({
                "variant_id": variant, "stage": "ACCEPTED_SEQUENCE_RR3P25",
                "status": "PASS" if seq_ok else "FAIL",
                "actual_count": len(accepted), "expected_count": exp["accepted"],
                "censored": censored,
                "actual_sha256": seq_sha, "expected_sha256": exp["accepted_sequence_sha256"],
            })

            for label, ticks, pips, purpose in COST_CASES:
                m = metrics(accepted, label)
                ledger_sha = accepted_ledger_hash(accepted, label)
                tgt = exp[label]
                ok = (
                    seq_ok
                    and m["winners"] == tgt["winners"]
                    and m["losers"] == tgt["losers"]
                    and abs(m["total_r"] - tgt["total_r"]) < 1e-9
                    and abs(m["profit_factor"] - tgt["profit_factor"]) < 1e-9
                    and abs(m["max_drawdown_r"] - tgt["max_drawdown_r"]) < 1e-9
                    and m["longest_losing_streak"] == tgt["longest_losing_streak"]
                    and ledger_sha == tgt["ledger_sha256"]
                )
                parity_rows.append({
                    "variant_id": variant, "stage": "FULL_LEDGER", "cost_label": label,
                    "status": "PASS" if ok else "FAIL",
                    "accepted_trades": len(accepted), "expected_accepted_trades": exp["accepted"],
                    "total_r": m["total_r"], "expected_total_r": tgt["total_r"],
                    "profit_factor": m["profit_factor"], "expected_profit_factor": tgt["profit_factor"],
                    "max_drawdown_r": m["max_drawdown_r"], "expected_max_drawdown_r": tgt["max_drawdown_r"],
                    "ledger_sha256": ledger_sha, "expected_ledger_sha256": tgt["ledger_sha256"],
                })
                summary_rows.append({
                    "variant_id": variant, "rr": RR, "cost_label": label,
                    "adverse_ticks": ticks, "adverse_pips": pips, "cost_purpose": purpose,
                    "qualified_raw_signals": len(q), "accepted_trades": len(accepted),
                    "right_censored": censored, **m,
                })
                for seq, r in enumerate(accepted, start=1):
                    ledger_rows.append({
                        "variant_id": variant, "rr": RR, "cost_label": label, "sequence": seq,
                        "signal_time": iso(r["signal_time"]), "exit_time": iso(r["exit_time"]),
                        "signal_index": r["signal_index"], "exit_index": r["exit_index"],
                        "reference_entry": r["reference_entry"], f"historical_fill": r[f"fill__{label}"],
                        "stop": r["stop"], "target": r["target"], "exit_reason": r["exit_reason"],
                        "result_r": r[f"result_r__{label}"],
                    })
                year_rows.extend(calendar_rows(variant, accepted, label))
                periods.extend(period_rows(variant, accepted, label))
                rolling_rows.extend(rolling_summary_rows(variant, accepted, label))

        write_csv(OUTPUTS["final_parity"], parity_rows)
        if any(x["status"] != "PASS" for x in parity_rows):
            raise RuntimeError("Independent final-strategy parity FAILED; do not proceed to portfolio admission")

        write_csv(OUTPUTS["summary"], summary_rows)
        write_csv(OUTPUTS["ledgers"], ledger_rows)
        write_csv(OUTPUTS["years"], year_rows)
        write_csv(OUTPUTS["periods"], periods)
        write_csv(OUTPUTS["rolling"], rolling_rows)
        write_csv(OUTPUTS["methodology"], [
            {"topic": "purpose", "value": "Independent implementation confirmation of frozen AUD/JPY H1 SHORT PRIMARY and QUALITY at RR3.25 before exact Portfolio 30 -> 31 admission."},
            {"topic": "independence", "value": "Standalone scalar reimplementation; does not import/call Pass 1-4 feature, qualification, replay or metric functions. NumPy is used only for the ATR seed mean to preserve floating-history parity."},
            {"topic": "price_source", "value": "OANDA completed MID candles; historical adverse fills are assumptions, not measured historical bid/ask."},
            {"topic": "execution", "value": "SHORT stop=signal high+10 ticks; target=reference close - 3.25*(stop-reference close); adverse fills 10T/20T/40T; p0 half-open [signal,exit) so exit-candle re-entry is eligible."},
            {"topic": "primary_rules", "value": "Exact bearish engulf; previous175 high; abs high distance<=0.10 ATR14; body>=0.60 ATR14; range>=1.25 ATR14; previous H1 close location>=0.50; RR3.25."},
            {"topic": "quality_rules", "value": "PRIMARY plus previous completed D1 ATR14 / previous50 D1 ATR14 mean <=1.15."},
            {"topic": "d1_asof", "value": f"D1 clipped through {iso(EXPECTED_D1_LAST_OPEN)}; daily bar j is available only from the next aligned D1 open."},
            {"topic": "interpretation", "value": "Trade-for-trade confirmation on repeatedly examined history. Passing parity is an implementation gate, not fresh OOS evidence or live authorisation."},
            {"topic": "next_gate", "value": "Only if every source/raw/qualified/accepted/full-ledger parity check passes: run exact Portfolio 30 -> 31 admission independently for PRIMARY and QUALITY; do not use portfolio results to retune either candidate."},
        ])
        pack_results()
        set_status(
            state="complete", progress=100,
            message="AUD/JPY H1 SHORT independent confirmation complete; all parity gates passed",
            parity_passed=True, h1_candles=len(h1), d1_candles=len(d1),
            raw_exact_signals=len(raw), results_zip=str(BUNDLE),
        )
    except Exception as exc:
        tb = traceback.format_exc()
        write_csv(OUTPUTS["errors"], [{"error": repr(exc), "traceback": tb}])
        try:
            pack_results()
        except Exception:
            pass
        set_status(state="error", progress=100, message=str(exc), parity_passed=False, error=repr(exc))

# ============================================================
# FLASK / RAILWAY
# ============================================================

def ensure_started():
    global RUN_STARTED
    with RUN_LOCK:
        if RUN_STARTED:
            return
        RUN_STARTED = True
        t = threading.Thread(target=run_confirmation, name="audjpy-h1-short-pass5", daemon=True)
        t.start()


@app.get("/")
def root():
    ensure_started()
    return jsonify({
        "status": "online", "pass_version": PASS_VERSION,
        "research_only": True, "orders_supported": False,
        "status_route": "/audjpy-h1-short-pass5/status",
        "results_route": "/audjpy-h1-short-pass5/results",
        "frozen_variants": ["PRIMARY", "QUALITY"], "rr": RR,
    })


@app.get("/audjpy-h1-short-pass5/status")
def status_route():
    ensure_started()
    with STATUS_LOCK:
        return jsonify(dict(STATUS))


@app.get("/audjpy-h1-short-pass5/results")
def results_route():
    ensure_started()
    with STATUS_LOCK:
        state = STATUS.get("state")
    if state == "complete" and BUNDLE.exists():
        return send_file(BUNDLE, as_attachment=True, download_name=BUNDLE.name)
    if state == "error" and BUNDLE.exists():
        return send_file(BUNDLE, as_attachment=True, download_name=BUNDLE.name)
    return jsonify(dict(STATUS)), 202


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
