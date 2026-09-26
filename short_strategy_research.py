#!/usr/bin/env python3
"""
AUD/JPY H1 LONG — Pass 6 independent confirmation
==================================================

RESEARCH ONLY. READ ONLY. NEVER PLACES ORDERS OR MODIFIES THE LIVE EXECUTOR.

Purpose
-------
Independently reimplement the TWO frozen AUD/JPY H1 LONG candidates selected
after Pass 5, using a deliberately separate calculation/replay path from the
exploratory Pass 1-5 engine, then require complete accepted-ledger parity.

FROZEN STRATEGY A — BROAD CORE
-------------------------------
- exact bullish engulf
- previous 30-bar low
- signal low within 0.75 ATR14 of previous 30-bar low
- bullish body >= 0.80 ATR14
- signal range >= 1.00 ATR14
- H1 ATR14 / previous-50-H1 ATR14 mean <= 1.20
- stop = signal low - 10 ticks
- RR = 3.50
- pyramiding 0; exit-candle re-entry eligible

FROZEN STRATEGY B — TIGHT QUALITY
----------------------------------
- exact bullish engulf
- previous 12-bar low
- signal low within 0.15 ATR14 of previous 12-bar low
- bullish body >= 1.00 ATR14
- signal range >= 1.75 ATR14
- previous 10 completed H1 bars' price movement
  (close[i-1] - close[i-11]) / ATR14[i] <= -0.90
- stop = signal low - 10 ticks
- RR = 4.25
- pyramiding 0; exit-candle re-entry eligible

Historical fill assumptions
---------------------------
OANDA MID candles. The 10T / 20T / 40T entry penalties are ASSUMED adverse
historical fills, not measured historical bid/ask or slippage.

This runner MUST reproduce the exact Pass-5 accepted ledgers at all three cost
assumptions before either strategy is marked independently confirmed.
All repeatedly examined history remains in-sample. Passing this gate proves
implementation reproducibility on the same history, not untouched OOS validity.
"""

from __future__ import annotations

import csv
import hashlib
import math
import os
import threading
import time
import traceback
import zipfile
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import requests
from flask import Flask, jsonify, send_file

# ============================================================
# FROZEN SOURCE / EXECUTION PROTOCOL
# ============================================================

PAIR = "AUD_JPY"
TIMEFRAME = "H1"
SIDE = "LONG"
REQUESTED_START = datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc)
DATA_END = datetime(2026, 9, 25, 19, 0, tzinfo=timezone.utc)  # exclusive
D1_WARMUP_START = REQUESTED_START - timedelta(days=900)
EXPECTED_D1_LAST_OPEN = datetime(2026, 9, 23, 21, 0, tzinfo=timezone.utc)

TICK = 0.001
PIP = 0.01
STOP_BUFFER_TICKS = 10

COST_CASES = (
    ("LIVE_LIMIT_10T", 10, 1.0, "PRIMARY_LIVE_PARITY"),
    ("STRESS_20T", 20, 2.0, "STRESSED_SELECTION"),
    ("EXTREME_40T", 40, 4.0, "EXTREME_DIAGNOSTIC_ONLY"),
)

# Exact frozen source / raw exact-engulf fingerprints from Pass 5.
EXPECTED_H1_ROWS = 137969
EXPECTED_D1_ROWS = 6473
EXPECTED_H1_SHA256 = "2ebb773ab6d8bd53b3725265e16b879952436f0e198682142e490ff0e0f5957a"
EXPECTED_D1_SHA256 = "611c4edf4801888e0be378f36368ce2232fecd86f6c758f14d17fbaa0759688b"
EXPECTED_RAW_SIGNAL_COUNT = 10624
EXPECTED_RAW_SIGNAL_SHA256 = "47dd2498e9ad03ac69c3f8f8d758b878808d08bb5c72fecad8b87542d65a329f"

STRATEGIES = {
    "AUDJPY_H1_LONG_BROAD_CORE": {
        "short_name": "BROAD_CORE",
        "rr": 3.50,
        "lookback": 30,
        "distance_atr_max": 0.75,
        "body_atr_min": 0.80,
        "range_atr_min": 1.00,
        "h1_atr_ratio_max": 1.20,
        "prior_fall_lookback": None,
        "prior_fall_max_atr": None,
        "expected_qualified": 637,
        "expected_accepted": 593,
        "expected": {
            "LIVE_LIMIT_10T": {
                "winners": 169, "losers": 424,
                "total_r": 136.37606147318655,
                "profit_factor": 1.321641654417893,
                "max_drawdown_r": -28.208826967563546,
                "ledger_sha256": "0ce964ea127cdc1c6ed226759c0bcd700cba6236bf949bc2d2c57fd02f163a2d",
            },
            "STRESS_20T": {
                "winners": 169, "losers": 424,
                "total_r": 108.09399479523447,
                "profit_factor": 1.2549386669698928,
                "max_drawdown_r": -29.575128601667565,
                "ledger_sha256": "1ab7e9720d8a66db5f1cb00b09972faffbb677f6a4dc02bb64e7e3a5f9365926",
            },
            "EXTREME_40T": {
                "winners": 169, "losers": 424,
                "total_r": 58.473696740053725,
                "profit_factor": 1.1379096621227682,
                "max_drawdown_r": -33.872721831025444,
                "ledger_sha256": "0b14bca77a1f30de3a4950b9cb4fa130d227d9525b80574a2fc2fed326e9fc81",
            },
        },
    },
    "AUDJPY_H1_LONG_TIGHT_QUALITY": {
        "short_name": "TIGHT_QUALITY",
        "rr": 4.25,
        "lookback": 12,
        "distance_atr_max": 0.15,
        "body_atr_min": 1.00,
        "range_atr_min": 1.75,
        "h1_atr_ratio_max": None,
        "prior_fall_lookback": 10,
        "prior_fall_max_atr": -0.90,
        "expected_qualified": 58,
        "expected_accepted": 55,
        "expected": {
            "LIVE_LIMIT_10T": {
                "winners": 20, "losers": 35,
                "total_r": 46.98753104099917,
                "profit_factor": 2.3425008868856905,
                "max_drawdown_r": -8.0,
                "ledger_sha256": "b69532f26e5b25bb1d107c55f813bb6f0472eeed5bb7c5221fbe63515edc1071",
            },
            "STRESS_20T": {
                "winners": 20, "losers": 35,
                "total_r": 44.16308458850353,
                "profit_factor": 2.261802416814387,
                "max_drawdown_r": -8.0,
                "ledger_sha256": "46da89c328955b955587c779685793240bc3cb29136559749c8dc17e9b0becb1",
            },
            "EXTREME_40T": {
                "winners": 20, "losers": 35,
                "total_r": 39.00780309748533,
                "profit_factor": 2.1145086599281524,
                "max_drawdown_r": -8.0,
                "ledger_sha256": "16a58465ffa74e6d60d4fe31ecc4a47be1ca834daf0afcbfa5d364b996a75402",
            },
        },
    },
}

PASS_VERSION = "AUDJPY_H1_LONG_PASS6_INDEPENDENT_CONFIRMATION_V1_2026-09-26"
API = os.getenv("OANDA_API_URL", "https://api-fxtrade.oanda.com").rstrip("/")
TOKEN = os.getenv("OANDA_TOKEN", "")

OUT_DIR = Path(os.getenv("AUDJPY_H1_LONG_PASS6_OUTPUT_DIR", "/tmp/audjpy_h1_long_pass6"))
OUT_DIR.mkdir(parents=True, exist_ok=True)
BUNDLE = OUT_DIR / "AUDJPY_H1_LONG_PASS6_INDEPENDENT_CONFIRMATION_RESULTS.zip"
OUTPUTS = {
    "coverage": OUT_DIR / "coverage.csv",
    "source": OUT_DIR / "source_fingerprint.csv",
    "raw": OUT_DIR / "raw_signal_parity.csv",
    "parity": OUT_DIR / "final_strategy_parity.csv",
    "summary": OUT_DIR / "final_summary.csv",
    "ledgers": OUT_DIR / "final_full_accepted_ledgers.csv",
    "years": OUT_DIR / "final_calendar_years.csv",
    "periods": OUT_DIR / "final_periods.csv",
    "rolling": OUT_DIR / "final_rolling_summary.csv",
    "methodology": OUT_DIR / "methodology.csv",
    "errors": OUT_DIR / "error_report.csv",
}

STATUS_LOCK = threading.Lock()
STATUS = {
    "state": "idle",
    "message": "Independent confirmation ready",
    "progress": 0,
    "pair": PAIR,
    "timeframe": TIMEFRAME,
    "side": SIDE,
    "version": PASS_VERSION,
    "orders_supported": False,
    "trading_enabled": False,
    "strategies": list(STRATEGIES),
    "results_zip": str(BUNDLE),
}

app = Flask(__name__)

# ============================================================
# HELPERS
# ============================================================

def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def set_status(**kwargs):
    with STATUS_LOCK:
        STATUS.update(kwargs)


def write_csv(path: Path, rows):
    rows = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                fields.append(key)
                seen.add(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def pack_results():
    with zipfile.ZipFile(BUNDLE, "w", zipfile.ZIP_DEFLATED) as z:
        for path in OUTPUTS.values():
            if path.exists():
                z.write(path, arcname=path.name)


def sha_rows(rows) -> str:
    h = hashlib.sha256()
    for row in rows:
        h.update((row + "\n").encode("utf-8"))
    return h.hexdigest()


def safe_pf(gross_profit: float, gross_loss: float) -> float:
    if gross_loss > 0:
        return gross_profit / gross_loss
    if gross_profit > 0:
        return 99.0
    return 0.0


def month_floor(value: datetime) -> datetime:
    return datetime(value.year, value.month, 1, tzinfo=timezone.utc)


def add_months(value: datetime, months: int) -> datetime:
    n = value.year * 12 + value.month - 1 + months
    return datetime(n // 12, n % 12 + 1, 1, tzinfo=timezone.utc)

# ============================================================
# OANDA SOURCE — SAME FROZEN SOURCE, SEPARATE RESEARCH ENGINE
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
    response = requests.get(
        f"{API}/v3/instruments/{PAIR}/candles",
        headers=auth_headers(),
        params=params,
        timeout=60,
    )
    response.raise_for_status()
    result = []
    for raw in response.json().get("candles", []):
        if not raw.get("complete", False):
            continue
        mid = raw.get("mid")
        if not mid:
            continue
        result.append({
            "time": parse_time(raw["time"]),
            "open": float(mid["o"]),
            "high": float(mid["h"]),
            "low": float(mid["l"]),
            "close": float(mid["c"]),
        })
    return result


def fetch_history(granularity: str, start: datetime, end: datetime, chunk_days: int):
    by_time = {}
    cursor = start
    chunk_no = 0
    while cursor < end:
        chunk_no += 1
        chunk_end = min(cursor + timedelta(days=chunk_days), end)
        set_status(message=f"Fetching {granularity} chunk {chunk_no}: {iso(cursor)} -> {iso(chunk_end)}")
        last_error = None
        rows = None
        for attempt in range(1, 4):
            try:
                rows = fetch_chunk(granularity, cursor, chunk_end)
                last_error = None
                break
            except requests.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else None
                if status_code in (400, 404) and not by_time:
                    rows = []
                    last_error = None
                    break
                last_error = exc
            except Exception as exc:
                last_error = exc
            if attempt < 3:
                time.sleep(0.75 * attempt)
        if last_error is not None:
            raise last_error
        for row in rows or []:
            if row["time"] < end:
                by_time[row["time"]] = row
        cursor = chunk_end
        time.sleep(0.02)
    result = sorted(by_time.values(), key=lambda x: x["time"])
    if len(result) < 100:
        raise RuntimeError(f"Insufficient {granularity} history: {len(result)}")
    return result

# ============================================================
# INDEPENDENT PLAIN-PYTHON INDICATORS / SIGNALS
# ============================================================

def atr14_plain(candles):
    """Wilder ATR14. Separate implementation from Pass 1-5 feature builder."""
    n = len(candles)
    atr = [math.nan] * n
    if n < 14:
        return atr
    tr = [0.0] * n
    tr[0] = candles[0]["high"] - candles[0]["low"]
    for i in range(1, n):
        h = candles[i]["high"]
        l = candles[i]["low"]
        pc = candles[i - 1]["close"]
        tr[i] = max(h - l, abs(h - pc), abs(l - pc))
    # Use the identical mathematical seed but through an independent routine.
    # np.mean is retained for the 14-value seed to avoid a false parity failure
    # caused solely by a different floating-point reduction order.
    atr[13] = float(np.mean(np.asarray(tr[:14], dtype=float)))
    for i in range(14, n):
        atr[i] = (atr[i - 1] * 13.0 + tr[i]) / 14.0
    return atr


def previous_rolling_min(values, lookback):
    """For bar i, minimum of values[i-lookback:i]."""
    n = len(values)
    out = [math.nan] * n
    q = deque()
    for i in range(n):
        add = i - 1
        if add >= 0:
            v = values[add]
            while q and values[q[-1]] >= v:
                q.pop()
            q.append(add)
        cutoff = i - lookback
        while q and q[0] < cutoff:
            q.popleft()
        if i >= lookback and q:
            out[i] = values[q[0]]
    return out


def previous_mean_finite(values, lookback):
    """Previous lookback values only; returns NaN if any member is non-finite."""
    n = len(values)
    out = [math.nan] * n
    q = deque()
    total = 0.0
    bad = 0
    for i in range(n):
        add = i - 1
        if add >= 0:
            v = values[add]
            q.append(v)
            if math.isfinite(v):
                total += v
            else:
                bad += 1
        if len(q) > lookback:
            old = q.popleft()
            if math.isfinite(old):
                total -= old
            else:
                bad -= 1
        if len(q) == lookback and bad == 0:
            out[i] = total / lookback
    return out


def exact_bullish_engulf(candles, i: int) -> bool:
    if i <= 0:
        return False
    p = candles[i - 1]
    c = candles[i]
    return (
        p["close"] < p["open"]
        and c["close"] > c["open"]
        and c["open"] <= p["close"]
        and c["close"] >= p["open"]
    )


def build_independent_features(candles):
    atr = atr14_plain(candles)
    lows = [c["low"] for c in candles]
    prev_low_12 = previous_rolling_min(lows, 12)
    prev_low_30 = previous_rolling_min(lows, 30)
    atr50_prev = previous_mean_finite(atr, 50)
    h1_atr_ratio = [math.nan] * len(candles)
    for i in range(len(candles)):
        if math.isfinite(atr[i]) and math.isfinite(atr50_prev[i]) and atr50_prev[i] > 0:
            h1_atr_ratio[i] = atr[i] / atr50_prev[i]
    return {
        "atr": atr,
        "prev_low_12": prev_low_12,
        "prev_low_30": prev_low_30,
        "h1_atr_ratio": h1_atr_ratio,
    }


def raw_exact_indices(candles, atr):
    result = []
    for i in range(1, len(candles)):
        if math.isfinite(atr[i]) and atr[i] > 0 and exact_bullish_engulf(candles, i):
            result.append(i)
    return result


def qualifies(candles, features, i: int, spec: dict) -> bool:
    a = features["atr"][i]
    if not (math.isfinite(a) and a > 0):
        return False
    if not exact_bullish_engulf(candles, i):
        return False

    c = candles[i]
    bullish_body = c["close"] - c["open"]
    if bullish_body <= 0:
        return False
    body_atr = bullish_body / a
    range_atr = (c["high"] - c["low"]) / a
    if body_atr < spec["body_atr_min"] or range_atr < spec["range_atr_min"]:
        return False

    lb = spec["lookback"]
    structure = features[f"prev_low_{lb}"][i]
    if not math.isfinite(structure):
        return False
    distance = abs(c["low"] - structure) / a
    if distance > spec["distance_atr_max"]:
        return False

    vol_cap = spec.get("h1_atr_ratio_max")
    if vol_cap is not None:
        ratio = features["h1_atr_ratio"][i]
        if not math.isfinite(ratio) or ratio > vol_cap:
            return False

    fall_lb = spec.get("prior_fall_lookback")
    if fall_lb is not None:
        if i < fall_lb + 1:
            return False
        movement = (candles[i - 1]["close"] - candles[i - 1 - fall_lb]["close"]) / a
        if movement > spec["prior_fall_max_atr"]:
            return False

    return True

# ============================================================
# INDEPENDENT EXECUTION / P0 REPLAY
# ============================================================

def find_exit_plain(candles, signal_index: int, stop: float, target: float):
    for j in range(signal_index + 1, len(candles)):
        c = candles[j]
        stop_touched = c["low"] <= stop
        target_touched = c["high"] >= target
        if not stop_touched and not target_touched:
            continue
        if stop_touched and target_touched:
            # Frozen LONG tie convention.
            reason = "TARGET" if abs(c["high"] - c["open"]) < abs(c["open"] - c["low"]) else "STOP"
        else:
            reason = "STOP" if stop_touched else "TARGET"
        return j, reason
    return None, None


def r_for_cost(reference_entry: float, stop: float, target: float, reason: str, adverse_pips: float):
    fill = reference_entry + adverse_pips * PIP
    actual_risk = fill - stop
    if actual_risk <= 0:
        raise RuntimeError("Non-positive actual risk")
    exit_price = target if reason == "TARGET" else stop
    return (exit_price - fill) / actual_risk, fill


def build_strategy_raw_trades(candles, features, raw_indices, strategy_id, spec):
    qualified = []
    censored = 0
    rr = spec["rr"]
    for i in raw_indices:
        if not qualifies(candles, features, i, spec):
            continue
        c = candles[i]
        entry = c["close"]
        stop = c["low"] - STOP_BUFFER_TICKS * TICK
        risk = entry - stop
        if risk <= 0:
            continue
        target = entry + rr * risk
        exit_index, reason = find_exit_plain(candles, i, stop, target)
        if exit_index is None:
            censored += 1
            continue
        row = {
            "strategy_id": strategy_id,
            "signal_index": i,
            "exit_index": exit_index,
            "signal_time": c["time"],
            "exit_time": candles[exit_index]["time"],
            "reference_entry": entry,
            "stop": stop,
            "target": target,
            "exit_reason": reason,
        }
        for label, ticks, pips, purpose in COST_CASES:
            r, fill = r_for_cost(entry, stop, target, reason, pips)
            row[f"result_r__{label}"] = r
            row[f"fill__{label}"] = fill
        qualified.append(row)
    return qualified, censored


def replay_p0(raw_trades):
    """Chronological p0; a signal on the prior trade's exit candle is eligible."""
    accepted = []
    active_exit = -1
    for row in raw_trades:
        if row["signal_index"] < active_exit:
            continue
        accepted.append(row)
        active_exit = row["exit_index"]
    return accepted


def ledger_hash(accepted, cost_label):
    return sha_rows(
        "|".join((
            iso(row["signal_time"]),
            iso(row["exit_time"]),
            str(row["signal_index"]),
            str(row["exit_index"]),
            row["exit_reason"],
            f'{row[f"result_r__{cost_label}"]:.15g}',
        ))
        for row in accepted
    )


def metrics(accepted, cost_label):
    values = [float(x[f"result_r__{cost_label}"]) for x in accepted]
    winners = sum(v > 0 for v in values)
    losers = len(values) - winners
    gp = sum(v for v in values if v > 0)
    gl = -sum(v for v in values if v < 0)
    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0
    streak = 0
    longest = 0
    for v in values:
        cumulative += v
        peak = max(peak, cumulative)
        max_dd = min(max_dd, cumulative - peak)
        if v <= 0:
            streak += 1
            longest = max(longest, streak)
        else:
            streak = 0
    return {
        "accepted_trades": len(values),
        "winners": winners,
        "losers": losers,
        "win_rate_pct": 100.0 * winners / len(values) if values else 0.0,
        "profit_factor": safe_pf(gp, gl),
        "total_r": sum(values),
        "expectancy_r": sum(values) / len(values) if values else 0.0,
        "max_drawdown_r": max_dd,
        "longest_losing_streak": longest,
    }

# ============================================================
# DIAGNOSTICS
# ============================================================

def filter_by_signal_time(accepted, start=None, end=None):
    out = []
    for row in accepted:
        t = row["signal_time"]
        if start is not None and t < start:
            continue
        if end is not None and t >= end:
            continue
        out.append(row)
    return out


def period_rows(strategy_id, accepted):
    periods = (
        ("PRE2010", None, datetime(2010, 1, 1, tzinfo=timezone.utc)),
        ("2010_2015", datetime(2010, 1, 1, tzinfo=timezone.utc), datetime(2016, 1, 1, tzinfo=timezone.utc)),
        ("2016_2021", datetime(2016, 1, 1, tzinfo=timezone.utc), datetime(2022, 1, 1, tzinfo=timezone.utc)),
        ("2022_PLUS", datetime(2022, 1, 1, tzinfo=timezone.utc), DATA_END),
        ("LAST5Y", datetime(2021, 9, 25, 19, 0, tzinfo=timezone.utc), DATA_END),
        ("LAST2Y", datetime(2024, 9, 25, 19, 0, tzinfo=timezone.utc), DATA_END),
    )
    rows = []
    for cost_label, _, _, _ in COST_CASES:
        for label, start, end in periods:
            subset = filter_by_signal_time(accepted, start, end)
            row = {"strategy_id": strategy_id, "cost_label": cost_label, "period": label}
            row.update(metrics(subset, cost_label))
            rows.append(row)
    return rows


def calendar_year_rows(strategy_id, accepted):
    rows = []
    for cost_label, _, _, _ in COST_CASES:
        for year in range(2005, 2027):
            start = datetime(year, 1, 1, tzinfo=timezone.utc)
            end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
            subset = filter_by_signal_time(accepted, start, end)
            row = {"strategy_id": strategy_id, "cost_label": cost_label, "year": year}
            row.update(metrics(subset, cost_label))
            rows.append(row)
    return rows


def rolling_summary_rows(strategy_id, accepted):
    rows = []
    first_month = datetime(2005, 1, 1, tzinfo=timezone.utc)
    final_month = month_floor(DATA_END)
    for cost_label, _, _, _ in COST_CASES:
        for months in (12, 24, 36):
            windows = []
            start = first_month
            while add_months(start, months) <= final_month:
                end = add_months(start, months)
                subset = filter_by_signal_time(accepted, start, end)
                total_r = sum(float(x[f"result_r__{cost_label}"]) for x in subset)
                windows.append((start, end, len(subset), total_r))
                start = add_months(start, 1)
            if windows:
                worst = min(windows, key=lambda x: x[3])
                rows.append({
                    "strategy_id": strategy_id,
                    "cost_label": cost_label,
                    "window_months": months,
                    "windows": len(windows),
                    "positive_windows": sum(x[3] > 0 for x in windows),
                    "zero_trade_windows": sum(x[2] == 0 for x in windows),
                    "worst_total_r": worst[3],
                    "worst_start": iso(worst[0]),
                    "worst_end_exclusive": iso(worst[1]),
                })
    return rows

# ============================================================
# MAIN RESEARCH RUN
# ============================================================

def run_research():
    try:
        set_status(state="fetching", progress=2, message="Fetching frozen OANDA H1/D1 midpoint history")
        h1 = fetch_history("H1", REQUESTED_START, DATA_END, 180)
        d1_fetched = fetch_history("D", D1_WARMUP_START, DATA_END, 1200)
        d1 = [row for row in d1_fetched if row["time"] <= EXPECTED_D1_LAST_OPEN]

        if [x["time"] for x in h1] != sorted(set(x["time"] for x in h1)):
            raise RuntimeError("H1 timestamps duplicated/non-monotonic")
        if [x["time"] for x in d1] != sorted(set(x["time"] for x in d1)):
            raise RuntimeError("D1 timestamps duplicated/non-monotonic")
        if not h1 or h1[-1]["time"] >= DATA_END:
            raise RuntimeError("H1 cutoff invalid")
        if not d1 or d1[-1]["time"] != EXPECTED_D1_LAST_OPEN:
            raise RuntimeError("Frozen D1 cutoff invalid")

        h1_sha = sha_rows(
            f"{iso(x['time'])}|{x['open']:.6f}|{x['high']:.6f}|{x['low']:.6f}|{x['close']:.6f}"
            for x in h1
        )
        d1_sha = sha_rows(
            f"{iso(x['time'])}|{x['open']:.6f}|{x['high']:.6f}|{x['low']:.6f}|{x['close']:.6f}"
            for x in d1
        )
        write_csv(OUTPUTS["coverage"], [
            {
                "pair": PAIR, "timeframe": "H1", "requested_start": iso(REQUESTED_START),
                "first_completed_candle": iso(h1[0]["time"]),
                "last_completed_candle_open": iso(h1[-1]["time"]),
                "frozen_end_exclusive": iso(DATA_END), "completed_candles": len(h1),
            },
            {
                "pair": PAIR, "timeframe": "D", "requested_start": iso(D1_WARMUP_START),
                "first_completed_candle": iso(d1[0]["time"]),
                "last_completed_candle_open": iso(d1[-1]["time"]),
                "frozen_end_exclusive": iso(DATA_END), "completed_candles": len(d1),
                "daily_alignment": "17:00 America/New_York",
                "fetched_complete_rows": len(d1_fetched), "frozen_rows": len(d1),
                "source_freeze": "clip_to_exact_Pass5_last_D1_open_before_hashing",
            },
        ])
        write_csv(OUTPUTS["source"], [
            {"series": "AUD_JPY_H1_MID_OHLC", "rows": len(h1), "sha256": h1_sha},
            {"series": "AUD_JPY_D1_MID_OHLC", "rows": len(d1), "sha256": d1_sha},
        ])

        source_checks = [
            ("H1_ROW_COUNT", len(h1), EXPECTED_H1_ROWS),
            ("D1_ROW_COUNT", len(d1), EXPECTED_D1_ROWS),
            ("H1_SOURCE_SHA256", h1_sha, EXPECTED_H1_SHA256),
            ("D1_SOURCE_SHA256", d1_sha, EXPECTED_D1_SHA256),
        ]
        source_rows = [
            {"check": name, "actual": actual, "expected": expected, "status": "PASS" if actual == expected else "FAIL"}
            for name, actual, expected in source_checks
        ]
        if any(x["status"] != "PASS" for x in source_rows):
            write_csv(OUTPUTS["raw"], source_rows)
            raise RuntimeError("Frozen source parity FAILED")

        set_status(state="features", progress=15, message="Building independent plain-Python H1 indicators")
        features = build_independent_features(h1)
        raw = raw_exact_indices(h1, features["atr"])
        raw_hash = sha_rows(iso(h1[i]["time"]) for i in raw)
        raw_rows = source_rows + [
            {
                "check": "RAW_EXACT_BULL_COUNT",
                "actual": len(raw), "expected": EXPECTED_RAW_SIGNAL_COUNT,
                "status": "PASS" if len(raw) == EXPECTED_RAW_SIGNAL_COUNT else "FAIL",
            },
            {
                "check": "RAW_EXACT_BULL_SHA256",
                "actual": raw_hash, "expected": EXPECTED_RAW_SIGNAL_SHA256,
                "status": "PASS" if raw_hash == EXPECTED_RAW_SIGNAL_SHA256 else "FAIL",
            },
        ]
        write_csv(OUTPUTS["raw"], raw_rows)
        if any(x["status"] != "PASS" for x in raw_rows):
            raise RuntimeError("Raw-signal/source parity FAILED")

        parity_rows = []
        summary_rows = []
        ledger_rows = []
        year_rows = []
        periods = []
        rolling_rows = []
        all_pass = True

        for n, (strategy_id, spec) in enumerate(STRATEGIES.items(), start=1):
            set_status(
                state="confirming", progress=25 + n * 25,
                message=f"Independent confirmation {n}/2: {strategy_id}",
            )
            raw_trades, censored = build_strategy_raw_trades(h1, features, raw, strategy_id, spec)
            accepted = replay_p0(raw_trades)
            qualified_count = len(raw_trades) + censored

            signal_hash = sha_rows(iso(x["signal_time"]) for x in raw_trades)
            common_ok = (
                qualified_count == spec["expected_qualified"]
                and len(accepted) == spec["expected_accepted"]
                and censored == 0
            )

            for cost_label, ticks, pips, purpose in COST_CASES:
                got = metrics(accepted, cost_label)
                exp = spec["expected"][cost_label]
                digest = ledger_hash(accepted, cost_label)
                checks = {
                    "qualified_raw_signals": (qualified_count, spec["expected_qualified"]),
                    "accepted_trades": (got["accepted_trades"], spec["expected_accepted"]),
                    "winners": (got["winners"], exp["winners"]),
                    "losers": (got["losers"], exp["losers"]),
                    "total_r": (got["total_r"], exp["total_r"]),
                    "profit_factor": (got["profit_factor"], exp["profit_factor"]),
                    "max_drawdown_r": (got["max_drawdown_r"], exp["max_drawdown_r"]),
                    "ledger_sha256": (digest, exp["ledger_sha256"]),
                }
                exact_fields_ok = (
                    checks["qualified_raw_signals"][0] == checks["qualified_raw_signals"][1]
                    and checks["accepted_trades"][0] == checks["accepted_trades"][1]
                    and checks["winners"][0] == checks["winners"][1]
                    and checks["losers"][0] == checks["losers"][1]
                    and checks["ledger_sha256"][0] == checks["ledger_sha256"][1]
                )
                float_fields_ok = all(
                    abs(float(checks[k][0]) - float(checks[k][1])) <= 1e-10
                    for k in ("total_r", "profit_factor", "max_drawdown_r")
                )
                status = "PASS" if common_ok and exact_fields_ok and float_fields_ok else "FAIL"
                all_pass = all_pass and (status == "PASS")
                parity_rows.append({
                    "strategy_id": strategy_id,
                    "short_name": spec["short_name"],
                    "rr": spec["rr"],
                    "cost_label": cost_label,
                    "status": status,
                    "qualified_raw_signals": qualified_count,
                    "expected_qualified_raw_signals": spec["expected_qualified"],
                    "accepted_trades": got["accepted_trades"],
                    "expected_accepted_trades": spec["expected_accepted"],
                    "winners": got["winners"], "expected_winners": exp["winners"],
                    "losers": got["losers"], "expected_losers": exp["losers"],
                    "total_r": got["total_r"], "expected_total_r": exp["total_r"],
                    "profit_factor": got["profit_factor"], "expected_profit_factor": exp["profit_factor"],
                    "max_drawdown_r": got["max_drawdown_r"], "expected_max_drawdown_r": exp["max_drawdown_r"],
                    "ledger_sha256": digest, "expected_ledger_sha256": exp["ledger_sha256"],
                    "qualified_signal_sha256_independent": signal_hash,
                    "right_censored_qualified_signals": censored,
                })
                srow = {
                    "strategy_id": strategy_id,
                    "short_name": spec["short_name"],
                    "rr": spec["rr"],
                    "cost_label": cost_label,
                    "adverse_ticks": ticks,
                    "adverse_pips": pips,
                    "cost_purpose": purpose,
                    "qualified_raw_signals": qualified_count,
                    "right_censored_qualified_signals": censored,
                    "independent_confirmation_status": status,
                }
                srow.update(got)
                summary_rows.append(srow)

                for seq, trade in enumerate(accepted, start=1):
                    ledger_rows.append({
                        "strategy_id": strategy_id,
                        "short_name": spec["short_name"],
                        "rr": spec["rr"],
                        "cost_label": cost_label,
                        "sequence": seq,
                        "signal_time": iso(trade["signal_time"]),
                        "exit_time": iso(trade["exit_time"]),
                        "signal_index": trade["signal_index"],
                        "exit_index": trade["exit_index"],
                        "reference_entry": trade["reference_entry"],
                        "historical_fill": trade[f"fill__{cost_label}"],
                        "stop": trade["stop"],
                        "target": trade["target"],
                        "exit_reason": trade["exit_reason"],
                        "result_r": trade[f"result_r__{cost_label}"],
                    })

            year_rows.extend(calendar_year_rows(strategy_id, accepted))
            periods.extend(period_rows(strategy_id, accepted))
            rolling_rows.extend(rolling_summary_rows(strategy_id, accepted))

        write_csv(OUTPUTS["parity"], parity_rows)
        write_csv(OUTPUTS["summary"], summary_rows)
        write_csv(OUTPUTS["ledgers"], ledger_rows)
        write_csv(OUTPUTS["years"], year_rows)
        write_csv(OUTPUTS["periods"], periods)
        write_csv(OUTPUTS["rolling"], rolling_rows)
        write_csv(OUTPUTS["methodology"], [
            {"topic": "purpose", "value": "Independent reimplementation of the two FINAL frozen AUD/JPY H1 LONG candidates after Pass 5; no parameter search."},
            {"topic": "implementation_independence", "value": "Direct plain-Python qualification from frozen H1 candles; only ATR14 seed uses NumPy mean to preserve floating reduction parity. No Pass1-5 feature-matrix or candidate-grid functions are imported/reused."},
            {"topic": "broad_core", "value": "exact bullish engulf; LB30; distance<=0.75 ATR14; body>=0.80 ATR14; range>=1.00 ATR14; H1 ATR14/previous50 ATR mean<=1.20; RR3.50; stop low-10 ticks; p0."},
            {"topic": "tight_quality", "value": "exact bullish engulf; LB12; distance<=0.15 ATR14; body>=1.00 ATR14; range>=1.75 ATR14; previous10H movement<=-0.90 ATR14; RR4.25; stop low-10 ticks; p0."},
            {"topic": "source", "value": "OANDA MID H1; D1 fetched only to enforce the same frozen source fingerprint. D1 clipped to exact Pass5 last open 2026-09-23T21:00Z before hashing."},
            {"topic": "execution", "value": "Reference entry=signal close; target from reference risk; exits next H1 candle onward; frozen intrabar tie convention; exit-candle re-entry eligible; p0."},
            {"topic": "costs", "value": "10T=1 pip primary live-parity, 20T=2 pips stress, 40T=4 pips extreme diagnostic. These are assumed historical adverse MID fills, not observed historical executable prices."},
            {"topic": "gate", "value": "PASS requires exact H1/D1/raw-signal fingerprints AND exact full accepted-ledger SHA at all three costs for BOTH final strategies."},
            {"topic": "interpretation", "value": "Passing proves implementation parity on repeatedly examined in-sample history only. It is not fresh OOS validation or live-trading authorisation."},
            {"topic": "next_gate", "value": "Only after both final strategies independently PASS should they enter the exact Portfolio29->Portfolio30 admission replay under the frozen same-pair non-hedging rule."},
        ])
        if not all_pass:
            raise RuntimeError("Independent full-ledger confirmation FAILED; do not run portfolio admission")

        pack_results()
        set_status(
            state="complete", progress=100,
            message="Independent confirmation PASS for both frozen strategies at all costs",
            parity_passed=True,
            results_zip=str(BUNDLE),
        )

    except Exception as exc:
        error = {
            "time": iso(datetime.now(timezone.utc)),
            "error_type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        }
        write_csv(OUTPUTS["errors"], [error])
        try:
            pack_results()
        except Exception:
            pass
        set_status(
            state="failed", progress=100,
            message=f"{type(exc).__name__}: {exc}",
            parity_passed=False,
            results_zip=str(BUNDLE) if BUNDLE.exists() else None,
        )


_RUN_LOCK = threading.Lock()
_RUN_STARTED = False


def launch_once():
    global _RUN_STARTED
    with _RUN_LOCK:
        if _RUN_STARTED:
            return False
        _RUN_STARTED = True
        threading.Thread(target=run_research, daemon=True, name="audjpy-h1-pass6-independent").start()
        return True


@app.route("/")
def root():
    return jsonify({
        "service": "AUD/JPY H1 LONG Pass 6 independent confirmation",
        "version": PASS_VERSION,
        "pair": PAIR,
        "timeframe": TIMEFRAME,
        "side": SIDE,
        "state": STATUS["state"],
        "orders_supported": False,
        "trading_enabled": False,
        "strategies": {
            "AUDJPY_H1_LONG_BROAD_CORE": "RR3.50",
            "AUDJPY_H1_LONG_TIGHT_QUALITY": "RR4.25",
        },
        "required_gate": "exact full-ledger parity at 10T/20T/40T for both strategies",
        "routes": [
            "/audjpy-h1-long-pass6/start",
            "/audjpy-h1-long-pass6/status",
            "/audjpy-h1-long-pass6/results",
        ],
    })


@app.route("/audjpy-h1-long-pass6/start")
def start_route():
    return jsonify({"started_now": launch_once(), "state": STATUS["state"], "orders_supported": False})


@app.route("/audjpy-h1-long-pass6/status")
def status_route():
    with STATUS_LOCK:
        return jsonify(dict(STATUS))


@app.route("/audjpy-h1-long-pass6/results")
def results_route():
    if not BUNDLE.exists():
        return jsonify({"error": "results not ready", "state": STATUS["state"]}), 404
    return send_file(BUNDLE, as_attachment=True, download_name=BUNDLE.name)


if __name__ == "__main__":
    launch_once()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)
