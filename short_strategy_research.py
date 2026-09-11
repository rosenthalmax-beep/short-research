import os
import csv
import math
import time
import zipfile
import threading
from bisect import bisect_left
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from statistics import median
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import requests
from flask import Flask, jsonify, send_file


# ============================================================
# EUR/JPY H1 SHORT — CONTROLLED CONTEXT FILTER RESEARCH
# ============================================================
#
# RAW FAMILY ALREADY REFINED:
#
#   COMPRESSION -> DOWNSIDE BREAKOUT
#
# Common geometry:
#   previous H1 ATR14 / previous-20 mean ATR <= 0.80
#   bearish body >= 1.10 ATR14
#   signal range >= 1.65 ATR14
#
# Three nearby frozen raw seeds:
#
#   SEED_A_LB2_RR7_25
#     close < previous 2-bar low
#     RR 7.25
#
#   SEED_B_LB3_RR7_25
#     close < previous 3-bar low
#     RR 7.25
#
#   SEED_C_LB3_RR7_50
#     close < previous 3-bar low
#     RR 7.50
#
# PURPOSE
# -------
# Test CONTEXT only.
#
# Stage 1:
#   every context factor is tested ONE AT A TIME on all 3 raw seeds.
#
# Stage 2:
#   only factors with broad cross-seed support are eligible for limited
#   TWO-FACTOR interactions. Same-category filters are not combined.
#
# Factors:
#   - previous completed daily close below EMA
#   - previous completed daily EMA bearish alignment
#   - previous completed H4 close below EMA
#   - previous completed H4 EMA bearish alignment
#   - daily volatility regime
#   - previous completed H1 volatility regime
#   - individual NY-hour exclusions
#   - broad NY inclusion windows
#   - weekday exclusions
#
# CAUSAL / NO-LOOKAHEAD
# ---------------------
# H1 signal candle timestamp is its OPEN time.
#
# Daily and H4 context state is restricted to candles whose completion
# time is <= the H1 SIGNAL OPEN. This is intentionally stricter than
# merely using data available at signal close.
#
# Previous-H1 volatility uses the previous completed H1 candle.
# NY hour/day uses the signal candle OPEN converted with DST-aware
# America/New_York time.
#
# HISTORICAL EXECUTION
# --------------------
# Instrument: EUR_JPY
# H1 midpoint candles
# ATR14: Wilder/RMA with SMA seed
#
# Historical adverse fill:
#   -0.5 pip for SHORT
#
# Stop:
#   signal high + 10 ticks
#
# Target:
#   reference signal close - seed RR * reference-close risk
#
# Realised R:
#   measured from adverse fill
#
# Exit:
#   first subsequent H1 candle touching stop/target
#
# Same-bar target+stop SHORT convention:
#   if high is closer to candle open => STOP first
#   otherwise TARGET first
#
# Pyramiding:
#   0 per candidate
#
# Exact exit-candle signal:
#   eligible
#
# READ ONLY. NEVER SENDS ORDERS.
# ============================================================


app = Flask(__name__)

TOKEN = os.getenv("OANDA_TOKEN")
BASE = os.getenv("OANDA_API_URL", "https://api-fxtrade.oanda.com")

PAIR = "EUR_JPY"
NY = ZoneInfo("America/New_York")

REQUESTED_START = datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc)
WARMUP_START = datetime(2000, 1, 1, tzinfo=timezone.utc)
VALIDATION_START = datetime(2018, 1, 1, tzinfo=timezone.utc)
NOW = datetime.now(timezone.utc).replace(second=0, microsecond=0)

TICK = 0.001
PIP = 0.01
STOP_BUFFER_TICKS = 10
BASE_COST_PIPS = 0.50

COMPRESSION_MAX = 0.80
BODY_ATR_MIN = 1.10
RANGE_ATR_MIN = 1.65

SEEDS = [
    {
        "seed_id": "SEED_A_LB2_RR7_25",
        "lookback": 2,
        "rr": 7.25,
    },
    {
        "seed_id": "SEED_B_LB3_RR7_25",
        "lookback": 3,
        "rr": 7.25,
    },
    {
        "seed_id": "SEED_C_LB3_RR7_50",
        "lookback": 3,
        "rr": 7.50,
    },
]

SEED_BY_ID = {
    seed["seed_id"]: seed
    for seed in SEEDS
}
SEED_IDS = [seed["seed_id"] for seed in SEEDS]
SEED_LOOKBACKS = sorted(set(seed["lookback"] for seed in SEEDS))


OUT = {
    "coverage": "eurjpy_short_context_coverage.csv",
    "controls": "eurjpy_short_context_controls.csv",
    "factor_definitions": "eurjpy_short_context_factor_definitions.csv",
    "single_factor": "eurjpy_short_context_single_factor_results.csv",
    "single_factor_consensus": "eurjpy_short_context_single_factor_consensus.csv",
    "interaction": "eurjpy_short_context_interaction_results.csv",
    "interaction_consensus": "eurjpy_short_context_interaction_consensus.csv",
    "finalists": "eurjpy_short_context_finalists.csv",
    "periods": "eurjpy_short_context_finalist_periods.csv",
    "cost_stress": "eurjpy_short_context_finalist_cost_stress.csv",
    "calendar": "eurjpy_short_context_finalist_calendar.csv",
    "calendar_summary": "eurjpy_short_context_finalist_calendar_summary.csv",
    "rolling": "eurjpy_short_context_finalist_rolling.csv",
    "rolling_summary": "eurjpy_short_context_finalist_rolling_summary.csv",
    "trades": "eurjpy_short_context_finalist_trades.csv",
    "hour_diagnostics": "eurjpy_short_context_hour_diagnostics.csv",
    "weekday_diagnostics": "eurjpy_short_context_weekday_diagnostics.csv",
    "notes": "eurjpy_short_context_notes.csv",
}

BUNDLE = "EURJPY_H1_SHORT_COMPRESSION_CONTEXT_FILTER_RESEARCH_RESULTS.zip"

STATUS = {
    "state": "not_started",
    "message": "Not started",
    "pair": PAIR,
    "timeframe": "H1",
    "side": "SHORT",
    "orders_supported": False,
    "trading_enabled": False,
}


# ============================================================
# HELPERS
# ============================================================

def iso(dt):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_time(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def safe_pf(gross_profit, gross_loss):
    if gross_loss > 0:
        return gross_profit / gross_loss
    if gross_profit > 0:
        return 99.0
    return 0.0


def med(values):
    values = list(values)
    return median(values) if values else 0.0


def month_floor(dt):
    return datetime(dt.year, dt.month, 1, tzinfo=timezone.utc)


def add_months(dt, months):
    value = dt.year * 12 + dt.month - 1 + months
    return datetime(value // 12, value % 12 + 1, 1, tzinfo=timezone.utc)


def write_csv(path, rows):
    rows = list(rows)

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

    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def pack():
    with zipfile.ZipFile(BUNDLE, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in OUT.values():
            if os.path.exists(path):
                archive.write(path, arcname=os.path.basename(path))


def download(path):
    if not os.path.exists(path):
        return jsonify({
            "status": "not_ready",
            "state": STATUS["state"],
            "message": STATUS["message"],
        }), 404

    return send_file(
        os.path.abspath(path),
        as_attachment=True,
        download_name=os.path.basename(path),
    )


# ============================================================
# OANDA DATA
# ============================================================

def headers():
    if not TOKEN:
        raise RuntimeError("OANDA_TOKEN is not configured")
    return {"Authorization": "Bearer " + TOKEN.strip()}


def candle_params(granularity, start, end):
    params = {
        "price": "M",
        "granularity": granularity,
        "smooth": "false",
        "from": iso(start),
        "to": iso(end),
        "includeFirst": "true",
    }

    # Explicit New York 17:00 daily alignment for D and H4.
    # H4 alignment then occurs on the aligned trading-day grid.
    if granularity in {"D", "H4"}:
        params["alignmentTimezone"] = "America/New_York"
        params["dailyAlignment"] = 17

    return params


def fetch_chunk(granularity, start, end):
    response = requests.get(
        f"{BASE}/v3/instruments/{PAIR}/candles",
        headers=headers(),
        params=candle_params(granularity, start, end),
        timeout=60,
    )
    response.raise_for_status()

    rows = []

    for raw in response.json().get("candles", []):
        if not raw.get("complete", False):
            continue

        mid = raw.get("mid")
        if not mid:
            continue

        rows.append({
            "time": parse_time(raw["time"]),
            "open": float(mid["o"]),
            "high": float(mid["h"]),
            "low": float(mid["l"]),
            "close": float(mid["c"]),
        })

    return rows


def fetch_history(granularity, start, end, chunk_days):
    cursor = start
    by_time = {}
    chunk_no = 0

    while cursor < end:
        chunk_no += 1
        chunk_end = min(cursor + timedelta(days=chunk_days), end)

        STATUS.update({
            "state": "fetching",
            "message": (
                f"{granularity} chunk {chunk_no}: "
                f"{iso(cursor)} -> {iso(chunk_end)}"
            ),
        })

        rows = None
        last_error = None

        for attempt in range(1, 4):
            try:
                rows = fetch_chunk(granularity, cursor, chunk_end)
                last_error = None
                break
            except requests.HTTPError as error:
                status_code = (
                    error.response.status_code
                    if error.response is not None
                    else None
                )

                if status_code in (400, 404):
                    rows = []
                    last_error = None
                    break

                last_error = error
            except Exception as error:
                last_error = error

            if attempt < 3:
                time.sleep(0.5 * attempt)

        if last_error is not None:
            raise last_error

        for candle in rows or []:
            by_time[candle["time"]] = candle

        cursor = chunk_end
        time.sleep(0.02)

    return sorted(by_time.values(), key=lambda row: row["time"])


# ============================================================
# INDICATORS
# ============================================================

def atr_array(candles, period=14):
    n = len(candles)
    result = np.full(n, np.nan, dtype=float)

    if n == 0:
        return result

    high = np.array([c["high"] for c in candles], dtype=float)
    low = np.array([c["low"] for c in candles], dtype=float)
    close = np.array([c["close"] for c in candles], dtype=float)

    tr = np.full(n, np.nan, dtype=float)
    tr[0] = high[0] - low[0]

    for i in range(1, n):
        tr[i] = max(
            high[i] - low[i],
            abs(high[i] - close[i - 1]),
            abs(low[i] - close[i - 1]),
        )

    if n < period:
        return result

    result[period - 1] = float(np.mean(tr[:period]))

    for i in range(period, n):
        result[i] = (
            result[i - 1] * (period - 1)
            + tr[i]
        ) / period

    return result


def ema_array(values, period):
    values = np.asarray(values, dtype=float)
    n = len(values)
    result = np.full(n, np.nan, dtype=float)

    if n < period:
        return result

    seed = float(np.mean(values[:period]))
    result[period - 1] = seed
    alpha = 2.0 / (period + 1.0)

    for i in range(period, n):
        result[i] = alpha * values[i] + (1.0 - alpha) * result[i - 1]

    return result


def rolling_previous_low(values, lookback):
    values = np.asarray(values, dtype=float)
    n = len(values)
    result = np.full(n, np.nan, dtype=float)
    dq = deque()

    for i in range(n):
        add_index = i - 1

        if add_index >= 0:
            value = values[add_index]
            while dq and values[dq[-1]] >= value:
                dq.pop()
            dq.append(add_index)

        minimum_index = i - lookback
        while dq and dq[0] < minimum_index:
            dq.popleft()

        if i >= lookback and dq:
            result[i] = values[dq[0]]

    return result


def previous_mean(values, lookback):
    values = np.asarray(values, dtype=float)
    n = len(values)
    result = np.full(n, np.nan, dtype=float)

    q = deque()
    total = 0.0
    bad = 0

    for i in range(n):
        add_index = i - 1

        if add_index >= 0:
            value = values[add_index]
            q.append(value)
            if math.isfinite(value):
                total += value
            else:
                bad += 1

        if len(q) > lookback:
            old = q.popleft()
            if math.isfinite(old):
                total -= old
            else:
                bad -= 1

        if len(q) == lookback and bad == 0:
            result[i] = total / lookback

    return result


# ============================================================
# H1 RAW SEED FEATURES
# ============================================================

def build_h1_features(candles):
    opens = np.array([c["open"] for c in candles], dtype=float)
    highs = np.array([c["high"] for c in candles], dtype=float)
    lows = np.array([c["low"] for c in candles], dtype=float)
    closes = np.array([c["close"] for c in candles], dtype=float)
    times = [c["time"] for c in candles]

    atr = atr_array(candles, 14)
    atr_mean20_prev = previous_mean(atr, 20)
    atr_mean50_prev = previous_mean(atr, 50)

    body = opens - closes
    candle_range = highs - lows

    body_atr = np.divide(
        body,
        atr,
        out=np.zeros_like(closes),
        where=(body > 0) & np.isfinite(atr) & (atr > 0),
    )

    range_atr = np.divide(
        candle_range,
        atr,
        out=np.zeros_like(closes),
        where=np.isfinite(atr) & (atr > 0),
    )

    # Exact carry-forward convention from raw refinement:
    # previous completed ATR14 / previous-20 mean ATR <= 0.80.
    compression = np.full(len(candles), np.nan, dtype=float)
    for i in range(1, len(candles)):
        prev_atr = atr[i - 1]
        mean20 = atr_mean20_prev[i]
        if (
            math.isfinite(prev_atr)
            and math.isfinite(mean20)
            and mean20 > 0
        ):
            compression[i] = prev_atr / mean20

    prior_lows = {
        lookback: rolling_previous_low(lows, lookback)
        for lookback in SEED_LOOKBACKS
    }

    # Previous completed H1 ATR14 / previous-50 mean ATR.
    h1_prev_atr_ratio = np.full(len(candles), np.nan, dtype=float)
    for i in range(1, len(candles)):
        prev_atr = atr[i - 1]
        prev_mean = atr_mean50_prev[i - 1]
        if (
            math.isfinite(prev_atr)
            and math.isfinite(prev_mean)
            and prev_mean > 0
        ):
            h1_prev_atr_ratio[i] = prev_atr / prev_mean

    ny_hour = np.array(
        [t.astimezone(NY).hour for t in times],
        dtype=int,
    )
    ny_weekday = np.array(
        [t.astimezone(NY).weekday() for t in times],
        dtype=int,
    )

    return {
        "times": times,
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
        "atr": atr,
        "body_atr": body_atr,
        "range_atr": range_atr,
        "compression": compression,
        "prior_lows": prior_lows,
        "h1_prev_atr_ratio": h1_prev_atr_ratio,
        "ny_hour": ny_hour,
        "ny_weekday": ny_weekday,
    }


def raw_seed_signals(features, seed):
    lookback = seed["lookback"]
    prior_low = features["prior_lows"][lookback]

    mask = (
        (features["close"] < features["open"])
        & (features["body_atr"] >= BODY_ATR_MIN)
        & (features["range_atr"] >= RANGE_ATR_MIN)
        & np.isfinite(features["compression"])
        & (features["compression"] <= COMPRESSION_MAX)
        & np.isfinite(prior_low)
        & (features["close"] < prior_low)
    )

    return np.flatnonzero(mask).astype(int)


# ============================================================
# HTF STATE — STRICTLY AVAILABLE BEFORE H1 SIGNAL OPEN
# ============================================================

def htf_feature_table(candles):
    times = [c["time"] for c in candles]
    closes = np.array([c["close"] for c in candles], dtype=float)

    table = {
        "times": times,
        "close": closes,
        "atr14": atr_array(candles, 14),
    }

    for period in [20, 50, 100, 200]:
        table[f"ema{period}"] = ema_array(closes, period)

    table["atr50mean_prev"] = previous_mean(table["atr14"], 50)
    return table


def completed_at_times(candles, granularity):
    """
    Each candle becomes usable at its actual next candle open.
    This respects weekends / gaps rather than assuming fixed timedelta.
    The final candle gets a theoretical duration only as a terminal fallback.
    """
    times = [c["time"] for c in candles]
    result = []

    fallback = timedelta(hours=4) if granularity == "H4" else timedelta(days=1)

    for i, t in enumerate(times):
        if i + 1 < len(times):
            result.append(times[i + 1])
        else:
            result.append(t + fallback)

    return result


def map_strict_previous_htf_state(h1_times, htf_candles, granularity):
    table = htf_feature_table(htf_candles)
    complete_times = completed_at_times(htf_candles, granularity)

    # State index for an H1 signal open time t is the latest HTF candle with
    # completion_time <= t.
    state_index = np.full(len(h1_times), -1, dtype=int)

    j = -1

    for i, signal_open in enumerate(h1_times):
        while (
            j + 1 < len(complete_times)
            and complete_times[j + 1] <= signal_open
        ):
            j += 1

        state_index[i] = j

    mapped = {
        "close": np.full(len(h1_times), np.nan, dtype=float),
        "atr_ratio": np.full(len(h1_times), np.nan, dtype=float),
    }

    for period in [20, 50, 100, 200]:
        mapped[f"ema{period}"] = np.full(
            len(h1_times),
            np.nan,
            dtype=float,
        )

    for i, j in enumerate(state_index):
        if j < 0:
            continue

        mapped["close"][i] = table["close"][j]

        atr = table["atr14"][j]
        atr_mean = table["atr50mean_prev"][j]

        if math.isfinite(atr) and math.isfinite(atr_mean) and atr_mean > 0:
            mapped["atr_ratio"][i] = atr / atr_mean

        for period in [20, 50, 100, 200]:
            mapped[f"ema{period}"][i] = table[f"ema{period}"][j]

    mapped["state_index"] = state_index
    return mapped


# ============================================================
# CONTEXT FACTORS
# ============================================================

def factor_id(category, name):
    return f"{category}|{name}"


def build_factors():
    factors = [{
        "factor_id": "CONTROL",
        "category": "CONTROL",
        "kind": "CONTROL",
        "label": "No context filter",
    }]

    # Previous completed DAILY close below EMA.
    for period in [20, 50, 100, 200]:
        factors.append({
            "factor_id": factor_id("DAILY_TREND", f"CLOSE_LT_EMA{period}"),
            "category": "DAILY_TREND",
            "kind": "HTF_CLOSE_LT_EMA",
            "tf": "D",
            "ema_period": period,
            "label": f"Previous completed daily close < EMA{period}",
        })

    # Previous completed DAILY bearish EMA alignment.
    for fast, slow in [
        (20, 100),
        (50, 100),
        (20, 200),
        (50, 200),
        (100, 200),
    ]:
        factors.append({
            "factor_id": factor_id("DAILY_ALIGNMENT", f"EMA{fast}_LT_EMA{slow}"),
            "category": "DAILY_ALIGNMENT",
            "kind": "HTF_EMA_LT_EMA",
            "tf": "D",
            "fast": fast,
            "slow": slow,
            "label": f"Previous completed daily EMA{fast} < EMA{slow}",
        })

    # Previous completed H4 close below EMA.
    for period in [20, 50, 100, 200]:
        factors.append({
            "factor_id": factor_id("H4_TREND", f"CLOSE_LT_EMA{period}"),
            "category": "H4_TREND",
            "kind": "HTF_CLOSE_LT_EMA",
            "tf": "H4",
            "ema_period": period,
            "label": f"Previous completed H4 close < EMA{period}",
        })

    # Previous completed H4 bearish EMA alignment.
    for fast, slow in [
        (20, 100),
        (50, 100),
        (20, 200),
        (50, 200),
        (100, 200),
    ]:
        factors.append({
            "factor_id": factor_id("H4_ALIGNMENT", f"EMA{fast}_LT_EMA{slow}"),
            "category": "H4_ALIGNMENT",
            "kind": "HTF_EMA_LT_EMA",
            "tf": "H4",
            "fast": fast,
            "slow": slow,
            "label": f"Previous completed H4 EMA{fast} < EMA{slow}",
        })

    # Higher-timeframe volatility regime.
    for threshold in [0.80, 1.00, 1.20]:
        factors.append({
            "factor_id": factor_id("DAILY_VOL", f"ATR_RATIO_GE_{threshold:.2f}"),
            "category": "DAILY_VOL",
            "kind": "HTF_ATR_RATIO_GE",
            "tf": "D",
            "threshold": threshold,
            "label": (
                "Previous completed daily ATR14 / "
                f"previous-50 mean ATR >= {threshold:.2f}"
            ),
        })

    for threshold in [0.80, 1.00, 1.20]:
        factors.append({
            "factor_id": factor_id("H1_VOL", f"ATR_RATIO_GE_{threshold:.2f}"),
            "category": "H1_VOL",
            "kind": "H1_PREV_ATR_RATIO_GE",
            "threshold": threshold,
            "label": (
                "Previous completed H1 ATR14 / "
                f"previous-50 mean ATR >= {threshold:.2f}"
            ),
        })

    # Exclude one New York signal-open hour at a time.
    for hour in range(24):
        factors.append({
            "factor_id": factor_id("NY_HOUR_EXCLUDE", f"HOUR_{hour:02d}"),
            "category": "NY_HOUR_EXCLUDE",
            "kind": "NY_HOUR_EXCLUDE",
            "hour": hour,
            "label": f"Exclude NY signal-open hour {hour:02d}:00",
        })

    # Broad NY inclusion windows. Half-open [start,end).
    for start, end in [
        (0, 8),
        (4, 12),
        (8, 16),
        (12, 20),
        (16, 24),
        (7, 17),
        (8, 17),
    ]:
        factors.append({
            "factor_id": factor_id("NY_SESSION_INCLUDE", f"{start:02d}_{end:02d}"),
            "category": "NY_SESSION_INCLUDE",
            "kind": "NY_SESSION_INCLUDE",
            "start_hour": start,
            "end_hour": end,
            "label": f"Include NY hours [{start:02d}:00,{end:02d}:00)",
        })

    weekday_names = ["MON", "TUE", "WED", "THU", "FRI"]
    for weekday, name in enumerate(weekday_names):
        factors.append({
            "factor_id": factor_id("WEEKDAY_EXCLUDE", name),
            "category": "WEEKDAY_EXCLUDE",
            "kind": "WEEKDAY_EXCLUDE",
            "weekday": weekday,
            "label": f"Exclude {name} signal-open day in New York",
        })

    return factors


FACTORS = build_factors()
FACTOR_BY_ID = {factor["factor_id"]: factor for factor in FACTORS}


def factor_mask(factor, features, daily_state, h4_state):
    n = len(features["times"])

    if factor["kind"] == "CONTROL":
        return np.ones(n, dtype=bool)

    if factor["kind"] == "HTF_CLOSE_LT_EMA":
        state = daily_state if factor["tf"] == "D" else h4_state
        close = state["close"]
        ema = state[f"ema{factor['ema_period']}"]
        return (
            np.isfinite(close)
            & np.isfinite(ema)
            & (close < ema)
        )

    if factor["kind"] == "HTF_EMA_LT_EMA":
        state = daily_state if factor["tf"] == "D" else h4_state
        fast = state[f"ema{factor['fast']}"]
        slow = state[f"ema{factor['slow']}"]
        return (
            np.isfinite(fast)
            & np.isfinite(slow)
            & (fast < slow)
        )

    if factor["kind"] == "HTF_ATR_RATIO_GE":
        state = daily_state if factor["tf"] == "D" else h4_state
        ratio = state["atr_ratio"]
        return np.isfinite(ratio) & (ratio >= factor["threshold"])

    if factor["kind"] == "H1_PREV_ATR_RATIO_GE":
        ratio = features["h1_prev_atr_ratio"]
        return np.isfinite(ratio) & (ratio >= factor["threshold"])

    if factor["kind"] == "NY_HOUR_EXCLUDE":
        return features["ny_hour"] != factor["hour"]

    if factor["kind"] == "NY_SESSION_INCLUDE":
        hour = features["ny_hour"]
        return (
            (hour >= factor["start_hour"])
            & (hour < factor["end_hour"])
        )

    if factor["kind"] == "WEEKDAY_EXCLUDE":
        return features["ny_weekday"] != factor["weekday"]

    raise ValueError(f"Unknown factor kind: {factor['kind']}")


# ============================================================
# TRADE ENGINE
# ============================================================

def find_exit_index(features, signal_index, stop, target):
    high = features["high"]
    low = features["low"]
    n = len(high)
    block = 2048

    for left in range(signal_index + 1, n, block):
        right = min(left + block, n)
        touched = (
            (high[left:right] >= stop)
            | (low[left:right] <= target)
        )
        hits = np.flatnonzero(touched)

        if len(hits):
            return left + int(hits[0])

    return None


def exit_reason(features, index, stop, target):
    o = features["open"][index]
    h = features["high"][index]
    l = features["low"][index]

    stop_hit = h >= stop
    target_hit = l <= target

    if stop_hit and not target_hit:
        return "STOP"

    if target_hit and not stop_hit:
        return "TARGET"

    if stop_hit and target_hit:
        # Locked SHORT same-bar convention:
        # high closer to candle open => stop first, else target first.
        return (
            "STOP"
            if abs(h - o) < abs(o - l)
            else "TARGET"
        )

    return None


def backtest(seed_id, features, filtered_signals, cost_multiplier=1.0):
    seed = SEED_BY_ID[seed_id]
    rr = seed["rr"]
    lookback = seed["lookback"]

    adverse_cost = BASE_COST_PIPS * cost_multiplier * PIP
    close = features["close"]
    high = features["high"]
    times = features["times"]

    signals = np.asarray(filtered_signals, dtype=int)
    trades = []
    pointer = 0

    while pointer < len(signals):
        signal_index = int(signals[pointer])

        reference_entry = float(close[signal_index])
        stop = float(high[signal_index]) + STOP_BUFFER_TICKS * TICK
        reference_risk = stop - reference_entry

        if reference_risk <= 0:
            pointer += 1
            continue

        target = reference_entry - rr * reference_risk
        fill = reference_entry - adverse_cost
        actual_risk = stop - fill

        if actual_risk <= 0:
            pointer += 1
            continue

        exit_index = find_exit_index(
            features,
            signal_index,
            stop,
            target,
        )

        if exit_index is None:
            break

        reason = exit_reason(
            features,
            exit_index,
            stop,
            target,
        )

        if reason is None:
            raise RuntimeError("Could not resolve exit reason")

        exit_price = target if reason == "TARGET" else stop
        result_r = (fill - exit_price) / actual_risk

        trades.append({
            "seed_id": seed_id,
            "lookback": lookback,
            "signal_time": times[signal_index],
            "exit_time": times[exit_index],
            "signal_index": signal_index,
            "exit_index": exit_index,
            "reference_entry": reference_entry,
            "historical_fill": fill,
            "stop": stop,
            "target": target,
            "rr": rr,
            "cost_pips": BASE_COST_PIPS * cost_multiplier,
            "exit_reason": reason,
            "result_r": result_r,
            "duration_bars": exit_index - signal_index,
        })

        # Pyramiding=0, exact exit-candle re-entry eligible.
        pointer = bisect_left(
            signals,
            exit_index,
            lo=pointer + 1,
        )

    return trades


# ============================================================
# METRICS
# ============================================================

def metrics(trades):
    trades = list(trades)

    if not trades:
        return {
            "trades": 0,
            "winners": 0,
            "losers": 0,
            "win_rate_pct": 0.0,
            "profit_factor": 0.0,
            "total_r": 0.0,
            "expectancy_r": 0.0,
            "max_drawdown_r": 0.0,
            "longest_losing_streak": 0,
            "median_duration_bars": 0.0,
        }

    values = [float(t["result_r"]) for t in trades]
    winners = sum(value > 0 for value in values)
    losers = len(values) - winners

    gross_profit = sum(value for value in values if value > 0)
    gross_loss = -sum(value for value in values if value < 0)

    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0
    streak = 0
    max_streak = 0

    for value in values:
        cumulative += value
        peak = max(peak, cumulative)
        max_dd = min(max_dd, cumulative - peak)

        if value <= 0:
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 0

    return {
        "trades": len(trades),
        "winners": winners,
        "losers": losers,
        "win_rate_pct": 100.0 * winners / len(trades),
        "profit_factor": safe_pf(gross_profit, gross_loss),
        "total_r": sum(values),
        "expectancy_r": sum(values) / len(values),
        "max_drawdown_r": max_dd,
        "longest_losing_streak": max_streak,
        "median_duration_bars": med(t["duration_bars"] for t in trades),
    }


def period_trades(trades, start=None, end=None):
    output = []

    for trade in trades:
        t = trade["signal_time"]

        if start is not None and t < start:
            continue
        if end is not None and t >= end:
            continue

        output.append(trade)

    return output


def period_metrics(trades, start=None, end=None):
    return metrics(period_trades(trades, start, end))


def evaluate_result(seed_id, lookback, factor_label, trades, control_metrics=None):
    full = metrics(trades)

    dev = period_metrics(
        trades,
        None,
        VALIDATION_START,
    )

    validation = period_metrics(
        trades,
        VALIDATION_START,
        None,
    )

    era_ranges = [
        (
            datetime(2002, 1, 1, tzinfo=timezone.utc),
            datetime(2008, 1, 1, tzinfo=timezone.utc),
        ),
        (
            datetime(2008, 1, 1, tzinfo=timezone.utc),
            datetime(2014, 1, 1, tzinfo=timezone.utc),
        ),
        (
            datetime(2014, 1, 1, tzinfo=timezone.utc),
            datetime(2020, 1, 1, tzinfo=timezone.utc),
        ),
        (
            datetime(2020, 1, 1, tzinfo=timezone.utc),
            None,
        ),
    ]

    era_results = [
        period_metrics(trades, start, end)
        for start, end in era_ranges
    ]

    positive_eras = sum(
        result["trades"] > 0 and result["total_r"] > 0
        for result in era_results
    )

    last5 = period_metrics(
        trades,
        NOW - timedelta(days=365.25 * 5),
        None,
    )

    last2 = period_metrics(
        trades,
        NOW - timedelta(days=365.25 * 2),
        None,
    )

    row = {
        "seed_id": seed_id,
        "lookback": lookback,
        "factor": factor_label,
        "full_trades": full["trades"],
        "full_winners": full["winners"],
        "full_win_rate_pct": full["win_rate_pct"],
        "full_pf": full["profit_factor"],
        "full_total_r": full["total_r"],
        "full_expectancy_r": full["expectancy_r"],
        "full_max_dd_r": full["max_drawdown_r"],
        "full_longest_losing_streak": full["longest_losing_streak"],
        "dev_2002_2017_trades": dev["trades"],
        "dev_2002_2017_pf": dev["profit_factor"],
        "dev_2002_2017_r": dev["total_r"],
        "validation_2018_plus_trades": validation["trades"],
        "validation_2018_plus_pf": validation["profit_factor"],
        "validation_2018_plus_r": validation["total_r"],
        "both_temporal_splits_positive": (
            dev["trades"] > 0
            and validation["trades"] > 0
            and dev["total_r"] > 0
            and validation["total_r"] > 0
        ),
        "min_temporal_split_pf": min(
            dev["profit_factor"] if dev["trades"] else 0.0,
            validation["profit_factor"] if validation["trades"] else 0.0,
        ),
        "positive_eras": positive_eras,
        "era_2002_2007_r": era_results[0]["total_r"],
        "era_2008_2013_r": era_results[1]["total_r"],
        "era_2014_2019_r": era_results[2]["total_r"],
        "era_2020_now_r": era_results[3]["total_r"],
        "last5y_trades": last5["trades"],
        "last5y_pf": last5["profit_factor"],
        "last5y_r": last5["total_r"],
        "last2y_trades": last2["trades"],
        "last2y_pf": last2["profit_factor"],
        "last2y_r": last2["total_r"],
    }

    if control_metrics:
        row.update({
            "control_trades": control_metrics["trades"],
            "control_pf": control_metrics["profit_factor"],
            "control_total_r": control_metrics["total_r"],
            "control_expectancy_r": control_metrics["expectancy_r"],
            "control_max_dd_r": control_metrics["max_drawdown_r"],
            "trade_retention_pct": (
                100.0 * full["trades"] / control_metrics["trades"]
                if control_metrics["trades"] else 0.0
            ),
            "delta_pf": full["profit_factor"] - control_metrics["profit_factor"],
            "delta_total_r": full["total_r"] - control_metrics["total_r"],
            "delta_expectancy_r": (
                full["expectancy_r"] - control_metrics["expectancy_r"]
            ),
            "delta_max_dd_r": (
                full["max_drawdown_r"] - control_metrics["max_drawdown_r"]
            ),
        })

    return row


# ============================================================
# DIAGNOSTICS
# ============================================================

def diagnostics_by_bucket(seed_id, lookback, trades, features, field, labels):
    grouped = defaultdict(list)

    for trade in trades:
        index = trade["signal_index"]
        bucket = int(features[field][index])
        grouped[bucket].append(trade)

    rows = []

    for bucket, label in labels:
        result = metrics(grouped.get(bucket, []))

        rows.append({
            "seed_id": seed_id,
            "lookback": lookback,
            "bucket": bucket,
            "label": label,
            **result,
        })

    return rows


# ============================================================
# SINGLE FACTOR CONSENSUS
# ============================================================

def build_consensus(single_rows):
    by_factor = defaultdict(list)

    for row in single_rows:
        if row["factor_id"] == "CONTROL":
            continue
        by_factor[row["factor_id"]].append(row)

    output = []

    for factor_id, rows in by_factor.items():
        if len(rows) != len(SEEDS):
            continue

        pf_improved = sum(row["delta_pf"] > 0 for row in rows)
        exp_improved = sum(row["delta_expectancy_r"] > 0 for row in rows)
        dd_not_worse = sum(row["delta_max_dd_r"] >= -1e-9 for row in rows)
        split_positive = sum(row["both_temporal_splits_positive"] for row in rows)
        recent_positive = sum(row["last5y_r"] > 0 for row in rows)
        recent2_positive = sum(row["last2y_r"] > 0 for row in rows)

        median_retention = med(row["trade_retention_pct"] for row in rows)
        median_delta_pf = med(row["delta_pf"] for row in rows)
        median_delta_exp = med(row["delta_expectancy_r"] for row in rows)
        median_delta_r = med(row["delta_total_r"] for row in rows)
        median_pf = med(row["full_pf"] for row in rows)
        min_split_pf = min(row["min_temporal_split_pf"] for row in rows)
        min_eras = min(row["positive_eras"] for row in rows)

        # Predeclared cross-seed support gate.
        #
        # We allow total R to fall because a useful filter can improve
        # risk quality / expectancy while removing trades. But we require:
        # - PF improves in >=2/3 seeds
        # - expectancy improves in >=2/3 seeds
        # - all 3 remain positive in both temporal splits
        # - >=3 positive eras in every seed
        # - recent 5Y positive in every seed
        # - median trade retention >= 45%
        # - median PF improvement > 0
        eligible = (
            pf_improved >= 2
            and exp_improved >= 2
            and split_positive == 3
            and min_eras >= 3
            and recent_positive == 3
            and recent2_positive == 3
            and median_retention >= 45.0
            and median_delta_pf > 0
        )

        factor = FACTOR_BY_ID[factor_id]

        output.append({
            "factor_id": factor_id,
            "category": factor["category"],
            "label": factor["label"],
            "seed_rows": len(rows),
            "pf_improved_seeds": pf_improved,
            "expectancy_improved_seeds": exp_improved,
            "dd_not_worse_seeds": dd_not_worse,
            "both_split_positive_seeds": split_positive,
            "recent5y_positive_seeds": recent_positive,
            "recent2y_positive_seeds": recent2_positive,
            "recent2y_positive_seeds": recent2_positive,
            "minimum_positive_eras": min_eras,
            "median_trade_retention_pct": median_retention,
            "median_pf": median_pf,
            "median_delta_pf": median_delta_pf,
            "median_delta_expectancy_r": median_delta_exp,
            "median_delta_total_r": median_delta_r,
            "minimum_temporal_split_pf": min_split_pf,
            "interaction_eligible": eligible,
        })

    output.sort(
        key=lambda row: (
            1 if row["interaction_eligible"] else 0,
            row["pf_improved_seeds"],
            row["expectancy_improved_seeds"],
            row["median_delta_pf"],
            row["minimum_temporal_split_pf"],
            row["median_delta_expectancy_r"],
        ),
        reverse=True,
    )

    return output


# ============================================================
# INTERACTIONS
# ============================================================

def interaction_id(a, b):
    ids = sorted([a["factor_id"], b["factor_id"]])
    return " + ".join(ids)


def compatible_factors(a, b):
    if a["category"] == b["category"]:
        return False

    # Avoid redundant trend/alignment conditions within the same HTF
    # in the automatic interaction stage.
    daily_categories = {"DAILY_TREND", "DAILY_ALIGNMENT"}
    h4_categories = {"H4_TREND", "H4_ALIGNMENT"}

    if a["category"] in daily_categories and b["category"] in daily_categories:
        return False

    if a["category"] in h4_categories and b["category"] in h4_categories:
        return False

    # Do not combine session include with hour exclusion automatically.
    if {
        a["category"],
        b["category"],
    } == {"NY_SESSION_INCLUDE", "NY_HOUR_EXCLUDE"}:
        return False

    return True


def choose_interaction_factors(consensus_rows, maximum=12):
    chosen = []
    category_counts = defaultdict(int)

    for row in consensus_rows:
        if not row["interaction_eligible"]:
            continue

        # Preserve variety. Maximum 2 from a category.
        if category_counts[row["category"]] >= 2:
            continue

        chosen.append(FACTOR_BY_ID[row["factor_id"]])
        category_counts[row["category"]] += 1

        if len(chosen) >= maximum:
            break

    return chosen


def interaction_consensus(rows):
    grouped = defaultdict(list)

    for row in rows:
        grouped[row["interaction_id"]].append(row)

    output = []

    for iid, group in grouped.items():
        if len(group) != 3:
            continue

        pf_improved = sum(row["delta_pf"] > 0 for row in group)
        exp_improved = sum(row["delta_expectancy_r"] > 0 for row in group)
        split_positive = sum(row["both_temporal_splits_positive"] for row in group)
        recent_positive = sum(row["last5y_r"] > 0 for row in group)
        recent2_positive = sum(row["last2y_r"] > 0 for row in group)

        output.append({
            "interaction_id": iid,
            "factor_a_id": group[0]["factor_a_id"],
            "factor_b_id": group[0]["factor_b_id"],
            "factor_a_category": group[0]["factor_a_category"],
            "factor_b_category": group[0]["factor_b_category"],
            "pf_improved_seeds": pf_improved,
            "expectancy_improved_seeds": exp_improved,
            "both_split_positive_seeds": split_positive,
            "recent5y_positive_seeds": recent_positive,
            "median_trade_retention_pct": med(
                row["trade_retention_pct"] for row in group
            ),
            "median_pf": med(row["full_pf"] for row in group),
            "median_delta_pf": med(row["delta_pf"] for row in group),
            "median_delta_expectancy_r": med(
                row["delta_expectancy_r"] for row in group
            ),
            "median_delta_total_r": med(row["delta_total_r"] for row in group),
            "minimum_temporal_split_pf": min(
                row["min_temporal_split_pf"] for row in group
            ),
            "minimum_positive_eras": min(
                row["positive_eras"] for row in group
            ),
        })

    output.sort(
        key=lambda row: (
            row["pf_improved_seeds"],
            row["expectancy_improved_seeds"],
            row["minimum_temporal_split_pf"],
            row["median_delta_pf"],
            row["median_delta_expectancy_r"],
        ),
        reverse=True,
    )

    return output


# ============================================================
# DETAILED FINALIST ANALYSIS
# ============================================================

def period_rows(candidate_id, seed_id, trades):
    periods = [
        ("FULL", None, None),
        ("DEV_2002_2017", None, VALIDATION_START),
        ("VALIDATION_2018_PLUS", VALIDATION_START, None),
        (
            "2002_2007",
            datetime(2002, 1, 1, tzinfo=timezone.utc),
            datetime(2008, 1, 1, tzinfo=timezone.utc),
        ),
        (
            "2008_2013",
            datetime(2008, 1, 1, tzinfo=timezone.utc),
            datetime(2014, 1, 1, tzinfo=timezone.utc),
        ),
        (
            "2014_2019",
            datetime(2014, 1, 1, tzinfo=timezone.utc),
            datetime(2020, 1, 1, tzinfo=timezone.utc),
        ),
        (
            "2020_NOW",
            datetime(2020, 1, 1, tzinfo=timezone.utc),
            None,
        ),
        (
            "LAST_5Y",
            NOW - timedelta(days=365.25 * 5),
            None,
        ),
        (
            "LAST_2Y",
            NOW - timedelta(days=365.25 * 2),
            None,
        ),
    ]

    rows = []

    for label, start, end in periods:
        rows.append({
            "candidate_id": candidate_id,
            "seed_id": seed_id,
            "period": label,
            **period_metrics(trades, start, end),
        })

    return rows


def calendar_rows(candidate_id, seed_id, trades):
    if not trades:
        return []

    first_year = trades[0]["signal_time"].year
    last_year = trades[-1]["signal_time"].year
    rows = []

    for year in range(first_year, last_year + 1):
        start = datetime(year, 1, 1, tzinfo=timezone.utc)
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)

        rows.append({
            "candidate_id": candidate_id,
            "seed_id": seed_id,
            "year": year,
            **period_metrics(trades, start, end),
        })

    return rows


def rolling_rows(candidate_id, seed_id, trades):
    if not trades:
        return []

    first_month = month_floor(trades[0]["signal_time"])
    last_month = month_floor(trades[-1]["signal_time"])
    rows = []

    for months in [12, 24, 36]:
        start = first_month

        while True:
            end = add_months(start, months)

            if end > add_months(last_month, 1):
                break

            rows.append({
                "candidate_id": candidate_id,
                "seed_id": seed_id,
                "window_months": months,
                "window_start": iso(start),
                "window_end": iso(end),
                **period_metrics(trades, start, end),
            })

            start = add_months(start, 1)

    return rows


def calendar_summary(rows):
    grouped = defaultdict(list)

    for row in rows:
        grouped[(row["candidate_id"], row["seed_id"])].append(row)

    output = []

    for (cid, sid), group in grouped.items():
        completed = [
            row for row in group
            if row["year"] < NOW.year and row["trades"] > 0
        ]

        values = [row["total_r"] for row in completed]
        positive = [value for value in values if value > 0]

        output.append({
            "candidate_id": cid,
            "seed_id": sid,
            "active_completed_years": len(values),
            "positive_active_years": len(positive),
            "positive_active_years_pct": (
                100.0 * len(positive) / len(values)
                if values else 0.0
            ),
            "median_active_year_r": med(values),
            "worst_active_year_r": min(values) if values else 0.0,
            "best_active_year_r": max(values) if values else 0.0,
        })

    return output


def rolling_summary(rows):
    grouped = defaultdict(list)

    for row in rows:
        grouped[
            (
                row["candidate_id"],
                row["seed_id"],
                row["window_months"],
            )
        ].append(row)

    output = []

    for (cid, sid, months), group in grouped.items():
        active = [row for row in group if row["trades"] > 0]
        values = [row["total_r"] for row in active]
        positive = [value for value in values if value > 0]

        output.append({
            "candidate_id": cid,
            "seed_id": sid,
            "window_months": months,
            "active_windows": len(active),
            "positive_active_windows": len(positive),
            "positive_active_windows_pct": (
                100.0 * len(positive) / len(active)
                if active else 0.0
            ),
            "median_r_active": med(values),
            "worst_r_active": min(values) if values else 0.0,
            "best_r_active": max(values) if values else 0.0,
        })

    return output


# ============================================================
# MAIN RESEARCH
# ============================================================

def run_research():
    try:
        # ----------------------------------------------------
        # FETCH
        # ----------------------------------------------------
        h1 = fetch_history(
            "H1",
            REQUESTED_START,
            NOW,
            chunk_days=180,
        )

        h4 = fetch_history(
            "H4",
            WARMUP_START,
            NOW,
            chunk_days=365,
        )

        daily = fetch_history(
            "D",
            WARMUP_START,
            NOW,
            chunk_days=1500,
        )

        if len(h1) < 50000:
            raise RuntimeError(
                f"Unexpectedly small H1 history: {len(h1)}"
            )

        if len(h4) < 10000:
            raise RuntimeError(
                f"Unexpectedly small H4 history: {len(h4)}"
            )

        if len(daily) < 5000:
            raise RuntimeError(
                f"Unexpectedly small daily history: {len(daily)}"
            )

        write_csv(
            OUT["coverage"],
            [
                {
                    "granularity": "H1",
                    "candles": len(h1),
                    "first_utc": iso(h1[0]["time"]),
                    "last_utc": iso(h1[-1]["time"]),
                },
                {
                    "granularity": "H4",
                    "candles": len(h4),
                    "first_utc": iso(h4[0]["time"]),
                    "last_utc": iso(h4[-1]["time"]),
                    "alignment_timezone": "America/New_York",
                    "daily_alignment": 17,
                },
                {
                    "granularity": "D",
                    "candles": len(daily),
                    "first_utc": iso(daily[0]["time"]),
                    "last_utc": iso(daily[-1]["time"]),
                    "alignment_timezone": "America/New_York",
                    "daily_alignment": 17,
                },
            ],
        )

        # ----------------------------------------------------
        # FEATURES / STRICT HTF STATE
        # ----------------------------------------------------
        STATUS.update({
            "state": "features",
            "message": "Building H1 short compression features and strict HTF state",
        })

        features = build_h1_features(h1)

        daily_state = map_strict_previous_htf_state(
            features["times"],
            daily,
            "D",
        )

        h4_state = map_strict_previous_htf_state(
            features["times"],
            h4,
            "H4",
        )

        start_index = bisect_left(
            features["times"],
            REQUESTED_START,
        )

        raw_by_seed = {}

        for seed in SEEDS:
            raw = raw_seed_signals(features, seed)
            raw = raw[raw >= start_index]
            raw_by_seed[seed["seed_id"]] = raw

        # ----------------------------------------------------
        # FACTOR DEFINITIONS
        # ----------------------------------------------------
        factor_rows = []

        for factor in FACTORS:
            row = {
                "factor_id": factor["factor_id"],
                "category": factor["category"],
                "kind": factor["kind"],
                "label": factor["label"],
            }

            for key in [
                "tf",
                "ema_period",
                "fast",
                "slow",
                "threshold",
                "hour",
                "start_hour",
                "end_hour",
                "weekday",
            ]:
                row[key] = factor.get(key)

            factor_rows.append(row)

        write_csv(
            OUT["factor_definitions"],
            factor_rows,
        )

        # ----------------------------------------------------
        # CONTROLS
        # ----------------------------------------------------
        controls = {}
        control_metrics = {}
        control_rows = []
        hour_diag = []
        weekday_diag = []

        for seed in SEEDS:
            seed_id = seed["seed_id"]

            trades = backtest(
                seed_id,
                features,
                raw_by_seed[seed_id],
                cost_multiplier=1.0,
            )

            controls[seed_id] = trades
            control_metrics[seed_id] = metrics(trades)

            row = evaluate_result(
                seed_id,
                seed["lookback"],
                "CONTROL",
                trades,
            )
            row["rr"] = seed["rr"]
            control_rows.append(row)

            hour_diag.extend(
                diagnostics_by_bucket(
                    seed_id,
                    seed["lookback"],
                    trades,
                    features,
                    "ny_hour",
                    [(hour, f"{hour:02d}:00") for hour in range(24)],
                )
            )

            weekday_diag.extend(
                diagnostics_by_bucket(
                    seed_id,
                    seed["lookback"],
                    trades,
                    features,
                    "ny_weekday",
                    [
                        (0, "MON"),
                        (1, "TUE"),
                        (2, "WED"),
                        (3, "THU"),
                        (4, "FRI"),
                    ],
                )
            )

        write_csv(OUT["controls"], control_rows)
        write_csv(OUT["hour_diagnostics"], hour_diag)
        write_csv(OUT["weekday_diagnostics"], weekday_diag)

        # Carry-forward guard against the exact boundary-refinement results.
        expected = {
            "SEED_A_LB2_RR7_25": {
                "trades": 116,
                "pf": 2.150900,
                "total_r": 102.430109,
            },
            "SEED_B_LB3_RR7_25": {
                "trades": 114,
                "pf": 1.990868,
                "total_r": 88.187249,
            },
            "SEED_C_LB3_RR7_50": {
                "trades": 114,
                "pf": 1.954187,
                "total_r": 85.876819,
            },
        }

        parity_rows = []

        for seed in SEEDS:
            seed_id = seed["seed_id"]
            exp = expected[seed_id]
            actual = control_metrics[seed_id]

            parity_rows.append({
                "seed_id": seed_id,
                "lookback": seed["lookback"],
                "rr": seed["rr"],
                "expected_min_trades": exp["trades"],
                "actual_trades": actual["trades"],
                "expected_pf_reference": exp["pf"],
                "actual_pf": actual["profit_factor"],
                "expected_total_r_reference": exp["total_r"],
                "actual_total_r": actual["total_r"],
                "trade_count_pass": actual["trades"] >= exp["trades"],
            })

            if actual["trades"] < exp["trades"]:
                raise RuntimeError(
                    f"{seed_id} control fell below prior boundary-refinement "
                    f"trade count: {actual['trades']} < {exp['trades']}"
                )

        # ----------------------------------------------------
        # SINGLE FACTORS
        # ----------------------------------------------------
        single_rows = []

        noncontrol_factors = [
            factor for factor in FACTORS
            if factor["factor_id"] != "CONTROL"
        ]

        factor_masks = {}

        for factor in noncontrol_factors:
            factor_masks[factor["factor_id"]] = factor_mask(
                factor,
                features,
                daily_state,
                h4_state,
            )

        total_jobs = len(noncontrol_factors) * len(SEEDS)
        job = 0

        for factor in noncontrol_factors:
            mask = factor_masks[factor["factor_id"]]

            for seed in SEEDS:
                job += 1
                seed_id = seed["seed_id"]

                STATUS.update({
                    "state": "single_factor",
                    "message": (
                        f"{job}/{total_jobs} "
                        f"{seed_id} {factor['factor_id']}"
                    ),
                })

                raw = raw_by_seed[seed_id]
                filtered = raw[mask[raw]]

                trades = backtest(
                    seed_id,
                    features,
                    filtered,
                    cost_multiplier=1.0,
                )

                row = evaluate_result(
                    seed_id,
                    seed["lookback"],
                    factor["factor_id"],
                    trades,
                    control_metrics=control_metrics[seed_id],
                )

                row.update({
                    "rr": seed["rr"],
                    "factor_id": factor["factor_id"],
                    "category": factor["category"],
                    "label": factor["label"],
                })

                single_rows.append(row)

        write_csv(
            OUT["single_factor"],
            single_rows,
        )

        consensus = build_consensus(single_rows)

        write_csv(
            OUT["single_factor_consensus"],
            consensus,
        )

        # ----------------------------------------------------
        # LIMITED TWO-FACTOR INTERACTIONS
        # ----------------------------------------------------
        interaction_factors = choose_interaction_factors(
            consensus,
            maximum=12,
        )

        pairs = []

        for i in range(len(interaction_factors)):
            for j in range(i + 1, len(interaction_factors)):
                fa = interaction_factors[i]
                fb = interaction_factors[j]

                if compatible_factors(fa, fb):
                    pairs.append((fa, fb))

        interaction_rows = []

        total_jobs = len(pairs) * len(SEEDS)
        job = 0

        for fa, fb in pairs:
            mask = (
                factor_masks[fa["factor_id"]]
                & factor_masks[fb["factor_id"]]
            )

            iid = interaction_id(fa, fb)

            for seed in SEEDS:
                job += 1
                seed_id = seed["seed_id"]

                STATUS.update({
                    "state": "interactions",
                    "message": (
                        f"{job}/{max(total_jobs, 1)} "
                        f"{seed_id} {iid}"
                    ),
                })

                raw = raw_by_seed[seed_id]
                filtered = raw[mask[raw]]

                trades = backtest(
                    seed_id,
                    features,
                    filtered,
                    cost_multiplier=1.0,
                )

                row = evaluate_result(
                    seed_id,
                    seed["lookback"],
                    iid,
                    trades,
                    control_metrics=control_metrics[seed_id],
                )

                row.update({
                    "rr": seed["rr"],
                    "interaction_id": iid,
                    "factor_a_id": fa["factor_id"],
                    "factor_b_id": fb["factor_id"],
                    "factor_a_category": fa["category"],
                    "factor_b_category": fb["category"],
                    "factor_a_label": fa["label"],
                    "factor_b_label": fb["label"],
                })

                interaction_rows.append(row)

        write_csv(
            OUT["interaction"],
            interaction_rows,
        )

        interaction_cons = interaction_consensus(
            interaction_rows
        )

        write_csv(
            OUT["interaction_consensus"],
            interaction_cons,
        )

        # ----------------------------------------------------
        # FINALISTS
        # ----------------------------------------------------
        candidate_specs = [{
            "candidate_id": "CONTROL",
            "type": "CONTROL",
            "factor_ids": [],
        }]

        for row in [
            r for r in consensus
            if r["interaction_eligible"]
        ][:5]:
            candidate_specs.append({
                "candidate_id": row["factor_id"],
                "type": "SINGLE",
                "factor_ids": [row["factor_id"]],
            })

        # Interaction finalists must preserve recent positivity across
        # all three raw seeds and avoid severe over-pruning.
        eligible_interactions = [
            row for row in interaction_cons
            if (
                row["pf_improved_seeds"] >= 2
                and row["expectancy_improved_seeds"] >= 2
                and row["both_split_positive_seeds"] == 3
                and row.get("recent5y_positive_seeds", 0) == 3
                and row.get("recent2y_positive_seeds", 0) == 3
                and row["median_trade_retention_pct"] >= 35.0
                and row["minimum_positive_eras"] >= 3
            )
        ]

        for row in eligible_interactions[:5]:
            candidate_specs.append({
                "candidate_id": row["interaction_id"],
                "type": "INTERACTION",
                "factor_ids": [
                    row["factor_a_id"],
                    row["factor_b_id"],
                ],
            })

        # Deduplicate.
        unique_specs = {}
        for spec in candidate_specs:
            unique_specs[spec["candidate_id"]] = spec
        candidate_specs = list(unique_specs.values())

        finalist_rows = []
        detailed_periods = []
        cost_rows = []
        calendar = []
        rolling = []
        trade_rows = []

        for candidate_no, spec in enumerate(candidate_specs, 1):
            for seed in SEEDS:
                seed_id = seed["seed_id"]

                STATUS.update({
                    "state": "final_analysis",
                    "message": (
                        f"{candidate_no}/{len(candidate_specs)} "
                        f"{spec['candidate_id']} {seed_id}"
                    ),
                })

                raw = raw_by_seed[seed_id]

                if spec["type"] == "CONTROL":
                    filtered = raw
                else:
                    combined_mask = np.ones(
                        len(features["times"]),
                        dtype=bool,
                    )

                    for fid in spec["factor_ids"]:
                        combined_mask &= factor_masks[fid]

                    filtered = raw[combined_mask[raw]]

                trades = backtest(
                    seed_id,
                    features,
                    filtered,
                    cost_multiplier=1.0,
                )

                row = evaluate_result(
                    seed_id,
                    seed["lookback"],
                    spec["candidate_id"],
                    trades,
                    control_metrics=control_metrics[seed_id],
                )

                row.update({
                    "rr": seed["rr"],
                    "candidate_id": spec["candidate_id"],
                    "candidate_type": spec["type"],
                    "factor_ids": ";".join(spec["factor_ids"]),
                })

                finalist_rows.append(row)

                detailed_periods.extend(
                    period_rows(
                        spec["candidate_id"],
                        seed_id,
                        trades,
                    )
                )

                for multiplier in [0.5, 1.0, 1.5, 2.0]:
                    stressed = backtest(
                        seed_id,
                        features,
                        filtered,
                        cost_multiplier=multiplier,
                    )

                    cost_rows.append({
                        "candidate_id": spec["candidate_id"],
                        "seed_id": seed_id,
                        "lookback": seed["lookback"],
                        "rr": seed["rr"],
                        "cost_multiplier": multiplier,
                        "cost_pips": BASE_COST_PIPS * multiplier,
                        **metrics(stressed),
                    })

                calendar.extend(
                    calendar_rows(
                        spec["candidate_id"],
                        seed_id,
                        trades,
                    )
                )

                rolling.extend(
                    rolling_rows(
                        spec["candidate_id"],
                        seed_id,
                        trades,
                    )
                )

                for trade in trades:
                    output = dict(trade)
                    output["signal_time"] = iso(output["signal_time"])
                    output["exit_time"] = iso(output["exit_time"])
                    output.update({
                        "candidate_id": spec["candidate_id"],
                        "candidate_type": spec["type"],
                        "factor_ids": ";".join(spec["factor_ids"]),
                    })
                    trade_rows.append(output)

        write_csv(OUT["finalists"], finalist_rows)
        write_csv(OUT["periods"], detailed_periods)
        write_csv(OUT["cost_stress"], cost_rows)
        write_csv(OUT["calendar"], calendar)
        write_csv(OUT["calendar_summary"], calendar_summary(calendar))
        write_csv(OUT["rolling"], rolling)
        write_csv(OUT["rolling_summary"], rolling_summary(rolling))
        write_csv(OUT["trades"], trade_rows)

        notes = [
            {
                "topic": "frozen_raw_geometry",
                "note": (
                    "All seeds use compression<=0.80, bearish body>=1.10 ATR14, "
                    "range>=1.65 ATR14. Seed A uses LB2/RR7.25, Seed B LB3/RR7.25, "
                    "Seed C LB3/RR7.50."
                ),
            },
            {
                "topic": "causal_htf",
                "note": (
                    "Daily and H4 states are restricted to candles whose completion "
                    "time is <= H1 signal OPEN, preventing simultaneous HTF closes "
                    "from affecting a signal."
                ),
            },
            {
                "topic": "single_factor_first",
                "note": (
                    "Every bearish daily/H4 trend/alignment, volatility, NY hour, "
                    "broad session and weekday condition is tested independently "
                    "on all three frozen raw seeds before interaction eligibility."
                ),
            },
            {
                "topic": "interaction_gate",
                "note": (
                    "Two-factor interactions are limited to cross-category factors "
                    "with broad single-factor support. Final interaction candidates "
                    "must also keep all three seeds positive in the last 5Y and 2Y."
                ),
            },
            {
                "topic": "control_reference",
                "note": (
                    "Boundary-refinement references: A LB2/RR7.25 = 116 trades, "
                    "PF2.150900, +102.430109R; B LB3/RR7.25 = 114 trades, "
                    "PF1.990868, +88.187249R; C LB3/RR7.50 = 114 trades, "
                    "PF1.954187, +85.876819R. Current history may add trades."
                ),
            },
            {
                "topic": "selection_rule",
                "note": (
                    "Do not select on headline PF alone. Prefer a context effect "
                    "that improves at least two seeds, preserves all three temporal "
                    "splits and recent periods, retains a meaningful trade sample, "
                    "and survives 2x cost and rolling-window checks."
                ),
            },
            {
                "topic": "next_step",
                "note": (
                    "After context selection, freeze one EUR/JPY H1 SHORT candidate "
                    "and test its marginal effect against the existing 21-strategy "
                    "portfolio before any live deployment."
                ),
            },
            {
                "topic": "control_parity",
                "note": str(parity_rows),
            },
        ]

        write_csv(OUT["notes"], notes)

        STATUS.update({
            "state": "packaging",
            "message": "Building results ZIP",
        })

        pack()

        STATUS.update({
            "state": "complete",
            "message": "EUR/JPY H1 SHORT compression context-filter research complete",
            "h1_candles": len(h1),
            "h4_candles": len(h4),
            "daily_candles": len(daily),
            "seed_controls": len(SEEDS),
            "single_factors_tested": len(noncontrol_factors),
            "single_factor_seed_tests": len(single_rows),
            "interaction_factors_selected": len(interaction_factors),
            "interaction_pairs_tested": len(pairs),
            "interaction_seed_tests": len(interaction_rows),
            "detailed_candidate_specs": len(candidate_specs),
            "bundle": BUNDLE,
        })

    except Exception as error:
        STATUS.update({
            "state": "error",
            "message": str(error),
        })

        print(
            "EURJPY SHORT CONTEXT RESEARCH ERROR:",
            repr(error),
            flush=True,
        )


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def root():
    return jsonify({
        "service": "EUR/JPY H1 SHORT Compression Controlled Context Research",
        "status": STATUS["state"],
        "instrument": PAIR,
        "timeframe": "H1",
        "side": "SHORT",
        "raw_family": {
            "family": "COMPRESSION_BREAKOUT",
            "compression_max": COMPRESSION_MAX,
            "body_atr_min": BODY_ATR_MIN,
            "range_atr_min": RANGE_ATR_MIN,
            "seeds": SEEDS,
            "stop_buffer_ticks": STOP_BUFFER_TICKS,
            "historical_adverse_cost_pips": BASE_COST_PIPS,
        },
        "orders_supported": False,
        "trading_enabled": False,
        "routes": [
            "/eurjpy-short-context/status",
            "/eurjpy-short-context/results",
        ],
    })


@app.route("/eurjpy-short-context/status")
def context_status():
    return jsonify(STATUS)


@app.route("/eurjpy-short-context/results")
def context_results():
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
