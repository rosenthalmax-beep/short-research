
import os
import csv
import time
import bisect
import zipfile
import threading
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from flask import Flask, jsonify, send_file


# ============================================================
# USD/CAD M15 LONG — GEN2 ALTERNATIVE ARCHETYPES
#
# PURPOSE
# -------
# Gen1 broad engulf/sweep/context search failed.
# This script deliberately tests genuinely different LONG trigger
# archetypes instead of tuning the failed family more finely.
#
# TRIGGER FAMILIES
# ----------------
# 1) FAILED_BREAKDOWN_RECLAIM
#    - current low breaks a prior range low
#    - candle reclaims that prior low
#    - bullish close
#
# 2) BREAKOUT_PULLBACK
#    - prior bar(s) break a recent high
#    - current candle pulls back into breakout level
#    - current candle closes back above it
#
# 3) MEAN_REVERSION_RECLAIM
#    - prior close materially below EMA
#    - current candle reclaims EMA
#    - bullish current candle
#
# 4) SESSION_MOMENTUM
#    - pre-signal multi-hour bullish momentum
#    - current bullish displacement candle
#    - optional NY session windows
#
# 5) VOL_EXPANSION_REVERSAL
#    - current range expanded versus ATR
#    - lower-tail rejection
#    - bullish close in upper part of candle
#
# SEARCH PHILOSOPHY
# -----------------
# Exhaust distinct hypotheses first, then controlled interactions
# only when a single family independently shows promise.
#
# CORRECTNESS / CONVENTIONS
# -------------------------
# - OANDA midpoint
# - USD_CAD M15
# - LONG only
# - M15 development cost baseline = 1.0 pip adverse
# - stress = 0.5 / 1.0 / 1.5 / 2.0 pips
# - tick = 0.00001
# - pip = 0.0001
# - stop = signal low - 10 ticks
# - target based on REFERENCE signal-close risk
# - ATR14 Wilder/RMA SMA-seeded
# - signal timestamp = M15 candle OPEN
# - exits begin NEXT candle
# - exact exit-candle signal eligible
# - same-bar long tie:
#     high closer to open => target first
#     else stop first
# - pyramiding 0
#
# HTF NO-LOOKAHEAD
# ----------------
# - H1/H4/D state exposed only after ACTUAL completion
# - complete_at = next actual HTF candle OPEN
# - lookup uses bisect_right(completion_times, signal_time)-1
# - daily completion uses actual next OANDA daily OPEN
# - momentum ends on previous M15 close, never signal close
#
# OUTPUT
# ------
# One ZIP route:
#   /usdcad-m15-long-gen2/results
#
# READ ONLY. NEVER SENDS ORDERS.
# ============================================================


app = Flask(__name__)

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_BASE = os.getenv(
    "OANDA_API_URL",
    "https://api-fxtrade.oanda.com",
)

INSTRUMENT = "USD_CAD"

RESEARCH_FROM = datetime(
    2010, 1, 1, tzinfo=timezone.utc
)

RESEARCH_TO = (
    datetime.now(timezone.utc)
    .replace(minute=0, second=0, microsecond=0)
)

NY = ZoneInfo("America/New_York")

TICK_SIZE = 0.00001
PIP_SIZE = 0.0001
STOP_BUFFER_TICKS = 10

PRIMARY_COST_PIPS = 1.00
COST_PIPS_GRID = [0.50, 1.00, 1.50, 2.00]

RR_VALUES = [
    2.50,
    2.75,
    3.00,
    3.25,
    3.50,
    3.75,
    4.00,
]

MIN_TRADES_FOR_INTERACTION = 60


# ============================================================
# OUTPUTS
# ============================================================

OUTPUT_BASELINES = "usdcad_m15_long_gen2_baselines.csv"
OUTPUT_SINGLE = "usdcad_m15_long_gen2_single_family.csv"
OUTPUT_INTERACTIONS = "usdcad_m15_long_gen2_interactions.csv"
OUTPUT_TOP = "usdcad_m15_long_gen2_top.csv"
OUTPUT_ERAS = "usdcad_m15_long_gen2_eras.csv"
OUTPUT_DEVVAL = "usdcad_m15_long_gen2_dev_validation.csv"
OUTPUT_RECENT = "usdcad_m15_long_gen2_recent.csv"
OUTPUT_ROLLING = "usdcad_m15_long_gen2_rolling.csv"
OUTPUT_ROLLING_SUMMARY = "usdcad_m15_long_gen2_rolling_summary.csv"
OUTPUT_OVERLAP = "usdcad_m15_long_gen2_overlap.csv"
OUTPUT_BEST_TRADES = "usdcad_m15_long_gen2_best_trades.csv"
OUTPUT_BUNDLE = "usdcad_m15_long_GEN2_alternative_archetypes_RESULTS.zip"

STATUS = {
    "state": "not_started",
    "message": "USD/CAD M15 LONG Gen2 not started",
    "service": "USDCAD M15 Long Gen2 Alternative Archetypes",
    "orders_supported": False,
    "trading_enabled": False,
}


# ============================================================
# HELPERS
# ============================================================

def iso_utc(dt):
    return (
        dt.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def parse_oanda_time(value):
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    if "." in value:
        left, right = value.split(".", 1)
        if "+" in right:
            fraction, offset = right.split("+", 1)
            fraction = fraction[:6].ljust(6, "0")
            value = left + "." + fraction + "+" + offset

    return datetime.fromisoformat(value).astimezone(timezone.utc)


def years_ago_safe(dt, years):
    try:
        return dt.replace(year=dt.year - years)
    except ValueError:
        return dt.replace(
            month=2,
            day=28,
            year=dt.year - years,
        )


def month_start(dt):
    return datetime(
        dt.year,
        dt.month,
        1,
        tzinfo=timezone.utc,
    )


def add_months(dt, months):
    total = dt.year * 12 + dt.month - 1 + months
    return datetime(
        total // 12,
        total % 12 + 1,
        1,
        tzinfo=timezone.utc,
    )


def write_csv(path, rows):
    if not rows:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("")
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


def download_file(path):
    if not os.path.exists(path):
        return jsonify({
            "error": "Output not ready yet",
            "path": path,
        }), 404

    return send_file(
        os.path.abspath(path),
        as_attachment=True,
        download_name=os.path.basename(path),
    )


def build_results_bundle():
    files = [
        OUTPUT_BASELINES,
        OUTPUT_SINGLE,
        OUTPUT_INTERACTIONS,
        OUTPUT_TOP,
        OUTPUT_ERAS,
        OUTPUT_DEVVAL,
        OUTPUT_RECENT,
        OUTPUT_ROLLING,
        OUTPUT_ROLLING_SUMMARY,
        OUTPUT_OVERLAP,
        OUTPUT_BEST_TRADES,
    ]

    with zipfile.ZipFile(
        OUTPUT_BUNDLE,
        "w",
        compression=zipfile.ZIP_DEFLATED,
    ) as archive:
        for path in files:
            if os.path.exists(path):
                archive.write(path, arcname=os.path.basename(path))


# ============================================================
# OANDA FETCH
# ============================================================

def oanda_headers():
    if not OANDA_TOKEN:
        raise RuntimeError("OANDA_TOKEN is not configured")

    return {
        "Authorization": "Bearer " + OANDA_TOKEN.strip(),
        "Content-Type": "application/json",
    }


def fetch_chunk(
    granularity,
    start,
    end,
    daily_alignment=False,
):
    url = (
        f"{OANDA_BASE}/v3/instruments/"
        f"{INSTRUMENT}/candles"
    )

    params = {
        "price": "M",
        "granularity": granularity,
        "smooth": "false",
        "from": iso_utc(start),
        "to": iso_utc(end),
        "includeFirst": "true",
    }

    if daily_alignment:
        params["dailyAlignment"] = 17
        params["alignmentTimezone"] = "America/New_York"

    response = requests.get(
        url,
        headers=oanda_headers(),
        params=params,
        timeout=60,
    )
    response.raise_for_status()

    rows = []

    for item in response.json().get("candles", []):
        if not item.get("complete", False):
            continue

        mid = item["mid"]

        rows.append({
            "time": parse_oanda_time(item["time"]),
            "open": float(mid["o"]),
            "high": float(mid["h"]),
            "low": float(mid["l"]),
            "close": float(mid["c"]),
        })

    return rows


def fetch_history(
    granularity,
    start,
    end,
    chunk_days,
    daily_alignment=False,
):
    cursor = start
    by_time = {}
    chunk_number = 0

    while cursor < end:
        chunk_number += 1
        chunk_end = min(
            cursor + timedelta(days=chunk_days),
            end,
        )

        STATUS.update({
            "state": "fetching",
            "message": (
                f"Fetching {granularity} chunk "
                f"{chunk_number}: "
                f"{iso_utc(cursor)} -> "
                f"{iso_utc(chunk_end)}"
            ),
        })

        rows = fetch_chunk(
            granularity,
            cursor,
            chunk_end,
            daily_alignment=daily_alignment,
        )

        for row in rows:
            by_time[row["time"]] = row

        cursor = chunk_end
        time.sleep(0.02)

    result = list(by_time.values())
    result.sort(key=lambda row: row["time"])
    return result


# ============================================================
# INDICATORS
# ============================================================

def true_ranges(candles):
    result = [None] * len(candles)

    for i, candle in enumerate(candles):
        if i == 0:
            result[i] = candle["high"] - candle["low"]
        else:
            prev_close = candles[i - 1]["close"]
            result[i] = max(
                candle["high"] - candle["low"],
                abs(candle["high"] - prev_close),
                abs(candle["low"] - prev_close),
            )

    return result


def rma(values, length):
    result = [None] * len(values)

    if len(values) < length:
        return result

    seed = values[:length]

    if any(value is None for value in seed):
        return result

    result[length - 1] = sum(seed) / length

    for i in range(length, len(values)):
        if values[i] is None or result[i - 1] is None:
            continue

        result[i] = (
            result[i - 1] * (length - 1)
            + values[i]
        ) / length

    return result


def atr14(candles):
    return rma(true_ranges(candles), 14)


def ema(values, length):
    result = [None] * len(values)

    if len(values) < length:
        return result

    seed = values[:length]

    if any(value is None for value in seed):
        return result

    result[length - 1] = sum(seed) / length
    alpha = 2.0 / (length + 1.0)

    for i in range(length, len(values)):
        if values[i] is None or result[i - 1] is None:
            continue

        result[i] = (
            alpha * values[i]
            + (1.0 - alpha) * result[i - 1]
        )

    return result


def sma(values, length):
    result = [None] * len(values)
    queue = []
    running = 0.0
    valid_count = 0

    for i, value in enumerate(values):
        queue.append(value)

        if value is not None:
            running += value
            valid_count += 1

        if len(queue) > length:
            removed = queue.pop(0)
            if removed is not None:
                running -= removed
                valid_count -= 1

        if len(queue) == length and valid_count == length:
            result[i] = running / length

    return result


# ============================================================
# COMPLETED HTF STATE
# ============================================================

H1_EMAS = [20, 50, 100, 200]
H4_EMAS = [20, 50, 100, 200]
D_EMAS = [20, 50, 100, 200, 300]


def build_htf_state(candles, ema_lengths):
    closes = [c["close"] for c in candles]
    atr = atr14(candles)
    atr_mean50 = sma(atr, 50)

    ema_map = {
        length: ema(closes, length)
        for length in ema_lengths
    }

    rows = []

    for i, candle in enumerate(candles):
        complete_at = (
            candles[i + 1]["time"]
            if i + 1 < len(candles)
            else None
        )

        atr_ratio = None

        if (
            atr[i] is not None
            and atr_mean50[i] is not None
            and atr_mean50[i] > 0
        ):
            atr_ratio = atr[i] / atr_mean50[i]

        rows.append({
            "time": candle["time"],
            "complete_at": complete_at,
            "close": candle["close"],
            "atr14": atr[i],
            "atr_ratio_50": atr_ratio,
            "emas": {
                length: ema_map[length][i]
                for length in ema_lengths
            },
        })

    return rows


def previous_completed_state(
    rows,
    completion_times,
    signal_time,
):
    pos = bisect.bisect_right(
        completion_times,
        signal_time,
    ) - 1

    if pos < 0:
        return None

    return rows[pos]


# ============================================================
# M15 FEATURE CACHE
# ============================================================

def build_signal_cache(
    m15,
    m15_atr,
    h1_state,
    h4_state,
    d_state,
):
    closes = [c["close"] for c in m15]

    ema20 = ema(closes, 20)
    ema50 = ema(closes, 50)
    ema100 = ema(closes, 100)

    atr_mean50 = sma(m15_atr, 50)

    h1_rows = [
        row for row in h1_state
        if row["complete_at"] is not None
    ]
    h4_rows = [
        row for row in h4_state
        if row["complete_at"] is not None
    ]
    d_rows = [
        row for row in d_state
        if row["complete_at"] is not None
    ]

    h1_times = [row["complete_at"] for row in h1_rows]
    h4_times = [row["complete_at"] for row in h4_rows]
    d_times = [row["complete_at"] for row in d_rows]

    signals = []

    start_index = 205

    for i in range(start_index, len(m15)):
        current = m15[i]
        previous = m15[i - 1]
        atr = m15_atr[i]

        if atr is None or atr <= 0:
            continue

        candle_range = current["high"] - current["low"]

        if candle_range <= 0:
            continue

        body_signed = current["close"] - current["open"]
        body = abs(body_signed)
        bullish = body_signed > 0

        close_location = (
            current["close"] - current["low"]
        ) / candle_range

        lower_wick = (
            min(current["open"], current["close"])
            - current["low"]
        )

        lower_wick_body = (
            lower_wick / body
            if body > 0
            else 0.0
        )

        prior_low_20 = min(
            c["low"] for c in m15[i - 20:i]
        )
        prior_low_40 = min(
            c["low"] for c in m15[i - 40:i]
        )
        prior_low_60 = min(
            c["low"] for c in m15[i - 60:i]
        )
        prior_low_100 = min(
            c["low"] for c in m15[i - 100:i]
        )

        prior_high_20 = max(
            c["high"] for c in m15[i - 20:i]
        )
        prior_high_40 = max(
            c["high"] for c in m15[i - 40:i]
        )
        prior_high_60 = max(
            c["high"] for c in m15[i - 60:i]
        )

        # Breakout-pullback context uses only completed bars.
        breakout20_prev1 = (
            previous["close"]
            >
            max(c["high"] for c in m15[i - 21:i - 1])
        )

        breakout40_prev1 = (
            previous["close"]
            >
            max(c["high"] for c in m15[i - 41:i - 1])
        )

        breakout20_prev2 = (
            m15[i - 2]["close"]
            >
            max(c["high"] for c in m15[i - 22:i - 2])
        )

        breakout40_prev2 = (
            m15[i - 2]["close"]
            >
            max(c["high"] for c in m15[i - 42:i - 2])
        )

        breakout_level20_prev1 = max(
            c["high"] for c in m15[i - 21:i - 1]
        )
        breakout_level40_prev1 = max(
            c["high"] for c in m15[i - 41:i - 1]
        )

        breakout_level20_prev2 = max(
            c["high"] for c in m15[i - 22:i - 2]
        )
        breakout_level40_prev2 = max(
            c["high"] for c in m15[i - 42:i - 2]
        )

        # Strictly prior momentum.
        prior_close = m15[i - 1]["close"]

        mom4 = (
            prior_close - m15[i - 17]["close"]
        ) / atr

        mom8 = (
            prior_close - m15[i - 33]["close"]
        ) / atr

        mom12 = (
            prior_close - m15[i - 49]["close"]
        ) / atr

        mom24 = (
            prior_close - m15[i - 97]["close"]
        ) / atr

        m15_atr_ratio = None

        if (
            atr_mean50[i] is not None
            and atr_mean50[i] > 0
        ):
            m15_atr_ratio = (
                atr / atr_mean50[i]
            )

        ny = current["time"].astimezone(NY)

        h1_prev = previous_completed_state(
            h1_rows,
            h1_times,
            current["time"],
        )

        h4_prev = previous_completed_state(
            h4_rows,
            h4_times,
            current["time"],
        )

        d_prev = previous_completed_state(
            d_rows,
            d_times,
            current["time"],
        )

        signals.append({
            "signal_index": i,
            "time": current["time"],
            "bullish": bullish,
            "body_atr": body / atr,
            "range_atr": candle_range / atr,
            "close_location": close_location,
            "lower_wick_body": lower_wick_body,
            "m15_atr_ratio": m15_atr_ratio,

            "ema20": ema20[i],
            "ema50": ema50[i],
            "ema100": ema100[i],
            "prev_close": previous["close"],

            "prior_low_20": prior_low_20,
            "prior_low_40": prior_low_40,
            "prior_low_60": prior_low_60,
            "prior_low_100": prior_low_100,

            "prior_high_20": prior_high_20,
            "prior_high_40": prior_high_40,
            "prior_high_60": prior_high_60,

            "breakout20_prev1": breakout20_prev1,
            "breakout40_prev1": breakout40_prev1,
            "breakout20_prev2": breakout20_prev2,
            "breakout40_prev2": breakout40_prev2,

            "breakout_level20_prev1": breakout_level20_prev1,
            "breakout_level40_prev1": breakout_level40_prev1,
            "breakout_level20_prev2": breakout_level20_prev2,
            "breakout_level40_prev2": breakout_level40_prev2,

            "momentum_4h_atr": mom4,
            "momentum_8h_atr": mom8,
            "momentum_12h_atr": mom12,
            "momentum_24h_atr": mom24,

            "ny_hour": ny.hour,
            "ny_weekday": ny.weekday(),

            "h1_prev": h1_prev,
            "h4_prev": h4_prev,
            "d_prev": d_prev,
        })

    return signals


# ============================================================
# TRIGGER FAMILIES
# ============================================================

def trigger_passes(signal, candle, config):
    trigger = config["trigger"]

    if trigger == "FAILED_BREAKDOWN_RECLAIM":
        lb = config["structure_lookback"]
        prior_low = signal[f"prior_low_{lb}"]

        return (
            signal["bullish"]
            and candle["low"] < prior_low
            and candle["close"] > prior_low
        )

    if trigger == "BREAKOUT_PULLBACK":
        lb = config["breakout_lookback"]
        age = config["breakout_age"]

        broke = signal[
            f"breakout{lb}_prev{age}"
        ]

        level = signal[
            f"breakout_level{lb}_prev{age}"
        ]

        tolerance = (
            config["pullback_tolerance_atr"]
            * config["_atr"]
        )

        return (
            broke
            and signal["bullish"]
            and candle["low"] <= level + tolerance
            and candle["close"] > level
        )

    if trigger == "MEAN_REVERSION_RECLAIM":
        ema_len = config["ema_length"]
        ema_value = signal[f"ema{ema_len}"]

        if ema_value is None:
            return False

        previous_distance = (
            signal["prev_close"] - ema_value
        ) / config["_atr"]

        return (
            signal["bullish"]
            and previous_distance
            <= config["max_prev_ema_distance_atr"]
            and candle["close"] > ema_value
        )

    if trigger == "SESSION_MOMENTUM":
        horizon = config["momentum_horizon"]
        momentum = signal[
            f"momentum_{horizon}h_atr"
        ]

        return (
            signal["bullish"]
            and momentum
            >= config["minimum_momentum_atr"]
            and signal["body_atr"]
            >= config["minimum_body_atr"]
        )

    if trigger == "VOL_EXPANSION_REVERSAL":
        return (
            signal["bullish"]
            and signal["range_atr"]
            >= config["minimum_range_atr"]
            and signal["close_location"]
            >= config["minimum_close_location"]
            and signal["lower_wick_body"]
            >= config["minimum_lower_wick_body"]
        )

    return False


# ============================================================
# CONTEXT FILTERS
# ============================================================

def htf_close_above_ema(state, length):
    if state is None:
        return False

    value = state["emas"].get(length)

    return (
        value is not None
        and state["close"] > value
    )


def htf_bull_alignment(state, fast, slow):
    if state is None:
        return False

    f = state["emas"].get(fast)
    s = state["emas"].get(slow)

    return (
        f is not None
        and s is not None
        and f > s
    )


def htf_atr_passes(state, minimum, maximum):
    if minimum is None and maximum is None:
        return True

    if state is None or state["atr_ratio_50"] is None:
        return False

    value = state["atr_ratio_50"]

    if minimum is not None and value < minimum:
        return False

    if maximum is not None and value > maximum:
        return False

    return True


def signal_passes(
    signal,
    candle,
    config,
):
    config["_atr"] = config["_m15_atr_lookup"][signal["signal_index"]]

    if not trigger_passes(signal, candle, config):
        return False

    if (
        config.get("minimum_body_atr_filter") is not None
        and signal["body_atr"]
        < config["minimum_body_atr_filter"]
    ):
        return False

    if (
        config.get("minimum_range_atr_filter") is not None
        and signal["range_atr"]
        < config["minimum_range_atr_filter"]
    ):
        return False

    if (
        config.get("minimum_close_location_filter") is not None
        and signal["close_location"]
        < config["minimum_close_location_filter"]
    ):
        return False

    if (
        config.get("minimum_lower_wick_body_filter") is not None
        and signal["lower_wick_body"]
        < config["minimum_lower_wick_body_filter"]
    ):
        return False

    if (
        config.get("minimum_m15_atr_ratio") is not None
    ):
        value = signal["m15_atr_ratio"]
        if (
            value is None
            or value < config["minimum_m15_atr_ratio"]
        ):
            return False

    if (
        config.get("maximum_m15_atr_ratio") is not None
    ):
        value = signal["m15_atr_ratio"]
        if (
            value is None
            or value > config["maximum_m15_atr_ratio"]
        ):
            return False

    included = config.get("included_ny_hours")

    if (
        included is not None
        and signal["ny_hour"] not in included
    ):
        return False

    if signal["ny_hour"] in config.get(
        "excluded_ny_hours",
        set(),
    ):
        return False

    if signal["ny_weekday"] in config.get(
        "excluded_weekdays",
        set(),
    ):
        return False

    h1 = signal["h1_prev"]
    h4 = signal["h4_prev"]
    d = signal["d_prev"]

    if config.get("h1_close_above_ema") is not None:
        if not htf_close_above_ema(
            h1,
            config["h1_close_above_ema"],
        ):
            return False

    if (
        config.get("h1_fast_ema") is not None
        and config.get("h1_slow_ema") is not None
    ):
        if not htf_bull_alignment(
            h1,
            config["h1_fast_ema"],
            config["h1_slow_ema"],
        ):
            return False

    if not htf_atr_passes(
        h1,
        config.get("minimum_h1_atr_ratio"),
        config.get("maximum_h1_atr_ratio"),
    ):
        return False

    if config.get("h4_close_above_ema") is not None:
        if not htf_close_above_ema(
            h4,
            config["h4_close_above_ema"],
        ):
            return False

    if (
        config.get("h4_fast_ema") is not None
        and config.get("h4_slow_ema") is not None
    ):
        if not htf_bull_alignment(
            h4,
            config["h4_fast_ema"],
            config["h4_slow_ema"],
        ):
            return False

    if not htf_atr_passes(
        h4,
        config.get("minimum_h4_atr_ratio"),
        config.get("maximum_h4_atr_ratio"),
    ):
        return False

    if config.get("daily_close_above_ema") is not None:
        if not htf_close_above_ema(
            d,
            config["daily_close_above_ema"],
        ):
            return False

    if (
        config.get("daily_fast_ema") is not None
        and config.get("daily_slow_ema") is not None
    ):
        if not htf_bull_alignment(
            d,
            config["daily_fast_ema"],
            config["daily_slow_ema"],
        ):
            return False

    if not htf_atr_passes(
        d,
        config.get("minimum_daily_atr_ratio"),
        config.get("maximum_daily_atr_ratio"),
    ):
        return False

    return True


# ============================================================
# CONFIG BUILDERS
# ============================================================

def base_config(label, trigger, rr):
    return {
        "label": label,
        "trigger": trigger,
        "reward_risk": rr,

        "minimum_body_atr_filter": None,
        "minimum_range_atr_filter": None,
        "minimum_close_location_filter": None,
        "minimum_lower_wick_body_filter": None,

        "minimum_m15_atr_ratio": None,
        "maximum_m15_atr_ratio": None,

        "included_ny_hours": None,
        "excluded_ny_hours": set(),
        "excluded_weekdays": set(),

        "h1_close_above_ema": None,
        "h1_fast_ema": None,
        "h1_slow_ema": None,
        "minimum_h1_atr_ratio": None,
        "maximum_h1_atr_ratio": None,

        "h4_close_above_ema": None,
        "h4_fast_ema": None,
        "h4_slow_ema": None,
        "minimum_h4_atr_ratio": None,
        "maximum_h4_atr_ratio": None,

        "daily_close_above_ema": None,
        "daily_fast_ema": None,
        "daily_slow_ema": None,
        "minimum_daily_atr_ratio": None,
        "maximum_daily_atr_ratio": None,
    }


def clone_config(config, label):
    result = {}

    for key, value in config.items():
        if isinstance(value, set):
            result[key] = set(value)
        elif isinstance(value, tuple):
            result[key] = tuple(value)
        elif key not in ("_atr", "_m15_atr_lookup"):
            result[key] = value

    result["label"] = label
    return result


def build_baseline_configs():
    configs = []

    for rr in [3.0, 3.5]:
        for lb in [20, 40, 60, 100]:
            c = base_config(
                f"FBR_LB{lb}_RR{rr}",
                "FAILED_BREAKDOWN_RECLAIM",
                rr,
            )
            c["structure_lookback"] = lb
            configs.append(c)

        for lb in [20, 40]:
            for age in [1, 2]:
                for tol in [0.00, 0.10, 0.20]:
                    c = base_config(
                        f"BOP_LB{lb}_A{age}_T{tol}_RR{rr}",
                        "BREAKOUT_PULLBACK",
                        rr,
                    )
                    c["breakout_lookback"] = lb
                    c["breakout_age"] = age
                    c["pullback_tolerance_atr"] = tol
                    configs.append(c)

        for ema_len in [20, 50, 100]:
            for dist in [-0.25, -0.50, -0.75, -1.00]:
                c = base_config(
                    f"MR_EMA{ema_len}_D{dist}_RR{rr}",
                    "MEAN_REVERSION_RECLAIM",
                    rr,
                )
                c["ema_length"] = ema_len
                c["max_prev_ema_distance_atr"] = dist
                configs.append(c)

        for horizon in [4, 8, 12, 24]:
            for mom in [0.25, 0.50, 0.75, 1.00]:
                for body in [0.60, 0.80, 1.00]:
                    c = base_config(
                        f"SM_{horizon}H_M{mom}_B{body}_RR{rr}",
                        "SESSION_MOMENTUM",
                        rr,
                    )
                    c["momentum_horizon"] = horizon
                    c["minimum_momentum_atr"] = mom
                    c["minimum_body_atr"] = body
                    configs.append(c)

        for range_atr in [1.20, 1.40, 1.60, 1.80]:
            for close_loc in [0.65, 0.75, 0.85]:
                for wick in [0.10, 0.20, 0.30]:
                    c = base_config(
                        f"VER_R{range_atr}_C{close_loc}_W{wick}_RR{rr}",
                        "VOL_EXPANSION_REVERSAL",
                        rr,
                    )
                    c["minimum_range_atr"] = range_atr
                    c["minimum_close_location"] = close_loc
                    c["minimum_lower_wick_body"] = wick
                    configs.append(c)

    return configs


def build_context_variants(seed_configs):
    rows = []

    sessions = {
        "ASIA": set(range(18, 24)) | set(range(0, 3)),
        "LONDON_AM": {3, 4, 5, 6, 7},
        "NY_AM": {8, 9, 10, 11, 12},
        "NY_PM": {13, 14, 15, 16},
        "PRE_NY": {3, 4, 5, 6, 7},
    }

    for seed in seed_configs:
        # Candle geometry context.
        for body in [0.60, 0.80, 1.00, 1.20]:
            c = clone_config(
                seed,
                f"{seed['label']}__BODY{body}",
            )
            c["minimum_body_atr_filter"] = body
            rows.append(("BODY", c))

        for range_atr in [1.20, 1.40, 1.60]:
            c = clone_config(
                seed,
                f"{seed['label']}__RANGE{range_atr}",
            )
            c["minimum_range_atr_filter"] = range_atr
            rows.append(("RANGE", c))

        for close_loc in [0.65, 0.75, 0.85]:
            c = clone_config(
                seed,
                f"{seed['label']}__CLOSE{close_loc}",
            )
            c["minimum_close_location_filter"] = close_loc
            rows.append(("CLOSE", c))

        for wick in [0.10, 0.20, 0.30]:
            c = clone_config(
                seed,
                f"{seed['label']}__LW{wick}",
            )
            c["minimum_lower_wick_body_filter"] = wick
            rows.append(("LOWER_WICK", c))

        # M15 volatility.
        for atr_ratio in [0.80, 0.90, 1.00, 1.10, 1.20]:
            c = clone_config(
                seed,
                f"{seed['label']}__M15ATRMIN{atr_ratio}",
            )
            c["minimum_m15_atr_ratio"] = atr_ratio
            rows.append(("M15_ATR_MIN", c))

        for atr_ratio in [0.90, 1.00, 1.10, 1.20, 1.30]:
            c = clone_config(
                seed,
                f"{seed['label']}__M15ATRMAX{atr_ratio}",
            )
            c["maximum_m15_atr_ratio"] = atr_ratio
            rows.append(("M15_ATR_MAX", c))

        # Sessions / weekday.
        for label, hours in sessions.items():
            c = clone_config(
                seed,
                f"{seed['label']}__SESSION_{label}",
            )
            c["included_ny_hours"] = set(hours)
            rows.append(("SESSION", c))

        for day, name in [
            (0, "MON"),
            (1, "TUE"),
            (2, "WED"),
            (3, "THU"),
            (4, "FRI"),
        ]:
            c = clone_config(
                seed,
                f"{seed['label']}__EX_{name}",
            )
            c["excluded_weekdays"] = {day}
            rows.append(("WEEKDAY", c))

        # H1 trend / vol.
        for ema_len in [20, 50, 100, 200]:
            c = clone_config(
                seed,
                f"{seed['label']}__H1_CLOSE_EMA{ema_len}",
            )
            c["h1_close_above_ema"] = ema_len
            rows.append(("H1_CLOSE_EMA", c))

        for fast, slow in [(20, 50), (20, 100), (50, 100), (50, 200)]:
            c = clone_config(
                seed,
                f"{seed['label']}__H1_EMA{fast}_{slow}",
            )
            c["h1_fast_ema"] = fast
            c["h1_slow_ema"] = slow
            rows.append(("H1_ALIGNMENT", c))

        for value in [0.80, 0.90, 1.00, 1.10]:
            c = clone_config(
                seed,
                f"{seed['label']}__H1ATRMIN{value}",
            )
            c["minimum_h1_atr_ratio"] = value
            rows.append(("H1_ATR_MIN", c))

        # H4.
        for ema_len in [20, 50, 100, 200]:
            c = clone_config(
                seed,
                f"{seed['label']}__H4_CLOSE_EMA{ema_len}",
            )
            c["h4_close_above_ema"] = ema_len
            rows.append(("H4_CLOSE_EMA", c))

        for fast, slow in [(20, 50), (20, 100), (50, 100), (50, 200)]:
            c = clone_config(
                seed,
                f"{seed['label']}__H4_EMA{fast}_{slow}",
            )
            c["h4_fast_ema"] = fast
            c["h4_slow_ema"] = slow
            rows.append(("H4_ALIGNMENT", c))

        # Daily.
        for ema_len in [20, 50, 100, 200, 300]:
            c = clone_config(
                seed,
                f"{seed['label']}__D_CLOSE_EMA{ema_len}",
            )
            c["daily_close_above_ema"] = ema_len
            rows.append(("D_CLOSE_EMA", c))

        for fast, slow in [
            (20, 50),
            (20, 100),
            (50, 100),
            (50, 200),
        ]:
            c = clone_config(
                seed,
                f"{seed['label']}__D_EMA{fast}_{slow}",
            )
            c["daily_fast_ema"] = fast
            c["daily_slow_ema"] = slow
            rows.append(("D_ALIGNMENT", c))

        for value in [0.80, 0.90, 1.00, 1.10]:
            c = clone_config(
                seed,
                f"{seed['label']}__DATRMIN{value}",
            )
            c["minimum_daily_atr_ratio"] = value
            rows.append(("D_ATR_MIN", c))

    return rows


# ============================================================
# TRADE OUTCOMES
# ============================================================

def compute_trade_outcome(
    candles,
    signal_index,
    reward_risk,
    cost_pips,
):
    signal = candles[signal_index]

    reference_entry = signal["close"]

    stop = (
        signal["low"]
        - STOP_BUFFER_TICKS * TICK_SIZE
    )

    reference_risk = reference_entry - stop

    if reference_risk <= 0:
        return None

    target = (
        reference_entry
        + reward_risk * reference_risk
    )

    backtest_entry = (
        reference_entry
        + cost_pips * PIP_SIZE
    )

    actual_risk = backtest_entry - stop

    if actual_risk <= 0:
        return None

    for j in range(
        signal_index + 1,
        len(candles),
    ):
        candle = candles[j]

        hit_stop = candle["low"] <= stop
        hit_target = candle["high"] >= target

        if hit_stop and hit_target:
            distance_high = abs(
                candle["high"] - candle["open"]
            )
            distance_low = abs(
                candle["open"] - candle["low"]
            )

            if distance_high < distance_low:
                exit_price = target
                exit_reason = "TARGET"
            else:
                exit_price = stop
                exit_reason = "STOP"

        elif hit_stop:
            exit_price = stop
            exit_reason = "STOP"

        elif hit_target:
            exit_price = target
            exit_reason = "TARGET"

        else:
            continue

        result_r = (
            exit_price - backtest_entry
        ) / actual_risk

        return {
            "signal_index": signal_index,
            "exit_index": j,
            "entry_time": signal["time"],
            "exit_time": candle["time"],
            "entry_time_utc": iso_utc(signal["time"]),
            "exit_time_utc": iso_utc(candle["time"]),
            "result_r": result_r,
            "reward_risk": reward_risk,
            "cost_pips": cost_pips,
        }

    return None


def build_outcome_cache(
    candles,
    signals,
):
    cache = {}

    total = (
        len(signals)
        * len(RR_VALUES)
        * len(COST_PIPS_GRID)
    )

    done = 0

    for signal in signals:
        index = signal["signal_index"]

        for rr in RR_VALUES:
            for cost in COST_PIPS_GRID:
                done += 1

                if done % 2000 == 0:
                    STATUS.update({
                        "state": "precomputing",
                        "message": (
                            f"Caching outcomes {done}/{total}"
                        ),
                    })

                cache[(index, rr, cost)] = (
                    compute_trade_outcome(
                        candles,
                        index,
                        rr,
                        cost,
                    )
                )

    return cache


# ============================================================
# BACKTEST / CACHE
# ============================================================

CANDIDATE_CACHE = {}


def config_signature(config):
    parts = []

    for key, value in config.items():
        if key in ("label", "_atr", "_m15_atr_lookup"):
            continue

        if isinstance(value, set):
            value = tuple(sorted(value))

        parts.append((key, value))

    return tuple(sorted(parts))


def qualifying_candidates(
    signals,
    candles,
    config,
):
    key = config_signature(config)

    if key in CANDIDATE_CACHE:
        return CANDIDATE_CACHE[key]

    candidates = []

    for signal in signals:
        candle = candles[signal["signal_index"]]

        if signal_passes(
            signal,
            candle,
            config,
        ):
            candidates.append(signal)

    CANDIDATE_CACHE[key] = candidates
    return candidates


def run_config_cached(
    signals,
    candles,
    cache,
    config,
    cost_pips,
    start=None,
    end=None,
):
    candidates = qualifying_candidates(
        signals,
        candles,
        config,
    )

    if start is not None or end is not None:
        times = [s["time"] for s in candidates]

        left = (
            0
            if start is None
            else bisect.bisect_left(times, start)
        )

        right = (
            len(candidates)
            if end is None
            else bisect.bisect_left(times, end)
        )

        candidates = candidates[left:right]

    indices = [
        s["signal_index"]
        for s in candidates
    ]

    trades = []
    position = 0

    while position < len(candidates):
        signal = candidates[position]

        trade = cache.get(
            (
                signal["signal_index"],
                config["reward_risk"],
                cost_pips,
            )
        )

        if trade is None:
            position += 1
            continue

        trades.append(dict(trade))

        position = bisect.bisect_left(
            indices,
            trade["exit_index"],
            lo=position + 1,
        )

    return trades


# ============================================================
# STATS
# ============================================================

def stats_from_trades(trades):
    results = [
        float(t["result_r"])
        for t in trades
    ]

    winners = [r for r in results if r > 0]
    losers = [r for r in results if r < 0]

    gross_profit = sum(winners)
    gross_loss = abs(sum(losers))
    total_r = sum(results)

    pf = (
        gross_profit / gross_loss
        if gross_loss > 0
        else (
            999.0 if gross_profit > 0 else 0.0
        )
    )

    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    streak = 0
    longest = 0

    for result in results:
        equity += result
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)

        if result < 0:
            streak += 1
            longest = max(longest, streak)
        else:
            streak = 0

    return {
        "trades": len(results),
        "winners": len(winners),
        "losers": len(losers),
        "win_rate": (
            len(winners) / len(results) * 100.0
            if results else 0.0
        ),
        "profit_factor": pf,
        "total_r": total_r,
        "expectancy_r": (
            total_r / len(results)
            if results else 0.0
        ),
        "max_drawdown_r": max_dd,
        "longest_loss_streak": longest,
    }


def result_row(
    family,
    config,
    cost,
    trades,
):
    stats = stats_from_trades(trades)

    return {
        "family": family,
        "candidate": config["label"],
        "trigger": config["trigger"],
        "reward_risk": config["reward_risk"],
        "cost_pips": cost,
        "trades": stats["trades"],
        "winners": stats["winners"],
        "losers": stats["losers"],
        "win_rate": round(stats["win_rate"], 4),
        "profit_factor": round(stats["profit_factor"], 6),
        "total_r": round(stats["total_r"], 4),
        "expectancy_r": round(stats["expectancy_r"], 6),
        "max_drawdown_r": round(stats["max_drawdown_r"], 4),
        "longest_loss_streak": stats["longest_loss_streak"],
    }


# ============================================================
# WINDOWS
# ============================================================

def era_windows():
    return [
        (
            "ERA_2010_2013",
            datetime(2010, 1, 1, tzinfo=timezone.utc),
            datetime(2014, 1, 1, tzinfo=timezone.utc),
        ),
        (
            "ERA_2014_2017",
            datetime(2014, 1, 1, tzinfo=timezone.utc),
            datetime(2018, 1, 1, tzinfo=timezone.utc),
        ),
        (
            "ERA_2018_2021",
            datetime(2018, 1, 1, tzinfo=timezone.utc),
            datetime(2022, 1, 1, tzinfo=timezone.utc),
        ),
        (
            "ERA_2022_NOW",
            datetime(2022, 1, 1, tzinfo=timezone.utc),
            RESEARCH_TO,
        ),
    ]


def devval_windows():
    return [
        (
            "DEV_2010_2017",
            datetime(2010, 1, 1, tzinfo=timezone.utc),
            datetime(2018, 1, 1, tzinfo=timezone.utc),
        ),
        (
            "VALIDATION_2018_NOW",
            datetime(2018, 1, 1, tzinfo=timezone.utc),
            RESEARCH_TO,
        ),
    ]


def recent_windows():
    return [
        (
            "LAST_5Y",
            years_ago_safe(RESEARCH_TO, 5),
            RESEARCH_TO,
        ),
        (
            "LAST_2Y",
            years_ago_safe(RESEARCH_TO, 2),
            RESEARCH_TO,
        ),
    ]


def validation_rows(
    signals,
    candles,
    cache,
    configs,
    windows,
):
    rows = []

    for rank, config in enumerate(configs, start=1):
        for label, start, end in windows:
            trades = run_config_cached(
                signals,
                candles,
                cache,
                config,
                PRIMARY_COST_PIPS,
                start=start,
                end=end,
            )

            stats = stats_from_trades(trades)

            rows.append({
                "rank": rank,
                "window": label,
                "candidate": config["label"],
                "trades": stats["trades"],
                "profit_factor": round(
                    stats["profit_factor"],
                    6,
                ),
                "total_r": round(
                    stats["total_r"],
                    4,
                ),
                "expectancy_r": round(
                    stats["expectancy_r"],
                    6,
                ),
                "max_drawdown_r": round(
                    stats["max_drawdown_r"],
                    4,
                ),
            })

    return rows


# ============================================================
# ROLLING
# ============================================================

def monthly_rolling_rows(
    signals,
    candles,
    cache,
    config,
    months,
):
    rows = []

    cursor = month_start(RESEARCH_FROM)
    last_start = add_months(
        month_start(RESEARCH_TO),
        -months,
    )

    while cursor <= last_start:
        end = add_months(cursor, months)

        if end > RESEARCH_TO:
            break

        trades = run_config_cached(
            signals,
            candles,
            cache,
            config,
            PRIMARY_COST_PIPS,
            start=cursor,
            end=end,
        )

        stats = stats_from_trades(trades)

        rows.append({
            "candidate": config["label"],
            "months": months,
            "window": (
                f"{cursor:%Y-%m-%d}"
                f" -> {end:%Y-%m-%d}"
            ),
            "trades": stats["trades"],
            "profit_factor": round(
                stats["profit_factor"],
                6,
            ),
            "total_r": round(
                stats["total_r"],
                4,
            ),
            "positive": stats["total_r"] > 0,
        })

        cursor = add_months(cursor, 1)

    return rows


def median(values):
    ordered = sorted(values)

    if not ordered:
        return None

    n = len(ordered)

    if n % 2:
        return ordered[n // 2]

    return (
        ordered[n // 2 - 1]
        + ordered[n // 2]
    ) / 2.0


def rolling_summary(rows):
    if not rows:
        return {}

    pfs = [
        float(row["profit_factor"])
        for row in rows
    ]

    rs = [
        float(row["total_r"])
        for row in rows
    ]

    positive = sum(
        1 for row in rows
        if row["positive"]
    )

    return {
        "candidate": rows[0]["candidate"],
        "months": rows[0]["months"],
        "windows": len(rows),
        "positive_windows_pct": round(
            positive / len(rows) * 100.0,
            4,
        ),
        "worst_profit_factor": round(
            min(pfs),
            6,
        ),
        "median_profit_factor": round(
            median(pfs),
            6,
        ),
        "worst_total_r": round(
            min(rs),
            4,
        ),
        "median_total_r": round(
            median(rs),
            4,
        ),
    }


# ============================================================
# OVERLAP
# ============================================================

def trade_key(trade):
    return (
        trade["entry_time_utc"],
        trade["exit_time_utc"],
    )


def overlap_rows(
    signals,
    candles,
    cache,
    reference,
    finalist,
):
    ref_trades = run_config_cached(
        signals,
        candles,
        cache,
        reference,
        PRIMARY_COST_PIPS,
    )

    fin_trades = run_config_cached(
        signals,
        candles,
        cache,
        finalist,
        PRIMARY_COST_PIPS,
    )

    ref_keys = {trade_key(t) for t in ref_trades}
    fin_keys = {trade_key(t) for t in fin_trades}

    shared = ref_keys & fin_keys
    added = fin_keys - ref_keys
    removed = ref_keys - fin_keys

    groups = [
        ("REFERENCE_ALL", ref_trades),
        ("FINALIST_ALL", fin_trades),
        (
            "FINALIST_SHARED",
            [t for t in fin_trades if trade_key(t) in shared],
        ),
        (
            "FINALIST_ADDED",
            [t for t in fin_trades if trade_key(t) in added],
        ),
        (
            "REFERENCE_REMOVED",
            [t for t in ref_trades if trade_key(t) in removed],
        ),
    ]

    rows = []

    for subset, trades in groups:
        stats = stats_from_trades(trades)

        rows.append({
            "reference": reference["label"],
            "finalist": finalist["label"],
            "subset": subset,
            "trades": stats["trades"],
            "profit_factor": round(
                stats["profit_factor"],
                6,
            ),
            "total_r": round(
                stats["total_r"],
                4,
            ),
            "expectancy_r": round(
                stats["expectancy_r"],
                6,
            ),
        })

    return rows


# ============================================================
# RUNNER
# ============================================================

def run_research():
    try:
        m15 = fetch_history(
            "M15",
            RESEARCH_FROM,
            RESEARCH_TO,
            30,
        )

        h1 = fetch_history(
            "H1",
            RESEARCH_FROM - timedelta(days=700),
            RESEARCH_TO,
            120,
        )

        h4 = fetch_history(
            "H4",
            RESEARCH_FROM - timedelta(days=1200),
            RESEARCH_TO,
            500,
        )

        daily = fetch_history(
            "D",
            RESEARCH_FROM - timedelta(days=1800),
            RESEARCH_TO,
            2500,
            daily_alignment=True,
        )

        if len(m15) < 1000:
            raise RuntimeError(
                "Too few M15 candles returned"
            )

        STATUS.update({
            "state": "precomputing",
            "message": (
                "Building alternative-archetype feature cache"
            ),
            "m15_candles": len(m15),
        })

        m15_atr = atr14(m15)

        h1_state = build_htf_state(
            h1,
            H1_EMAS,
        )

        h4_state = build_htf_state(
            h4,
            H4_EMAS,
        )

        d_state = build_htf_state(
            daily,
            D_EMAS,
        )

        signals = build_signal_cache(
            m15,
            m15_atr,
            h1_state,
            h4_state,
            d_state,
        )

        outcome_cache = build_outcome_cache(
            m15,
            signals,
        )

        # M15 ATR lookup is injected once and ignored by signatures.
        atr_lookup = {
            i: m15_atr[i]
            for i in range(len(m15))
        }

        baselines = build_baseline_configs()

        for config in baselines:
            config["_m15_atr_lookup"] = atr_lookup

        STATUS.update({
            "state": "calculating",
            "message": (
                f"Running {len(baselines)} alternative trigger baselines"
            ),
        })

        baseline_rows = []

        for config in baselines:
            for cost in COST_PIPS_GRID:
                trades = run_config_cached(
                    signals,
                    m15,
                    outcome_cache,
                    config,
                    cost,
                )

                baseline_rows.append(
                    result_row(
                        "BASELINE",
                        config,
                        cost,
                        trades,
                    )
                )

        write_csv(
            OUTPUT_BASELINES,
            baseline_rows,
        )

        primary_baselines = [
            row
            for row in baseline_rows
            if (
                abs(
                    float(row["cost_pips"])
                    - PRIMARY_COST_PIPS
                ) < 1e-12
                and int(row["trades"]) >= 50
            )
        ]

        primary_baselines.sort(
            key=lambda row: (
                float(row["profit_factor"]),
                float(row["expectancy_r"]),
                float(row["total_r"]),
            ),
            reverse=True,
        )

        config_lookup = {
            config["label"]: config
            for config in baselines
        }

        # Take best distinct baseline per trigger family.
        best_by_trigger = {}

        for row in primary_baselines:
            trigger = row["trigger"]

            if trigger not in best_by_trigger:
                best_by_trigger[trigger] = row

        seed_configs = [
            config_lookup[row["candidate"]]
            for row in best_by_trigger.values()
        ]

        # Context testing around genuinely distinct seeds.
        context_variants = build_context_variants(
            seed_configs
        )

        for _, config in context_variants:
            config["_m15_atr_lookup"] = atr_lookup
            config_lookup[config["label"]] = config

        STATUS.update({
            "state": "calculating",
            "message": (
                f"Running {len(context_variants)} context variants"
            ),
        })

        single_rows = []

        for number, (family, config) in enumerate(
            context_variants,
            start=1,
        ):
            for cost in COST_PIPS_GRID:
                trades = run_config_cached(
                    signals,
                    m15,
                    outcome_cache,
                    config,
                    cost,
                )

                single_rows.append(
                    result_row(
                        family,
                        config,
                        cost,
                        trades,
                    )
                )

            if number % 100 == 0:
                STATUS["message"] = (
                    f"Context variants {number}/"
                    f"{len(context_variants)}"
                )

        write_csv(
            OUTPUT_SINGLE,
            single_rows,
        )

        single_primary = [
            row
            for row in single_rows
            if (
                abs(
                    float(row["cost_pips"])
                    - PRIMARY_COST_PIPS
                ) < 1e-12
                and int(row["trades"])
                >= MIN_TRADES_FOR_INTERACTION
            )
        ]

        # Controlled interactions:
        # only combine top independent filters around same trigger seed.
        independent_promising = [
            row
            for row in single_primary
            if float(row["profit_factor"]) >= 1.10
        ]

        independent_promising.sort(
            key=lambda row: (
                float(row["profit_factor"]),
                float(row["expectancy_r"]),
            ),
            reverse=True,
        )

        # Keep at most top 2 per family to limit arbitrary combinations.
        family_counts = {}
        selected = []

        for row in independent_promising:
            family = row["family"]
            count = family_counts.get(family, 0)

            if count >= 2:
                continue

            family_counts[family] = count + 1
            selected.append(row)

            if len(selected) >= 12:
                break

        interaction_configs = []
        seen = set()
        counter = 0

        def overlay(base, source):
            result = clone_config(
                base,
                base["label"],
            )

            for key, value in source.items():
                if key in (
                    "label",
                    "_atr",
                    "_m15_atr_lookup",
                ):
                    continue

                if isinstance(value, set):
                    result[key] = set(value)
                else:
                    result[key] = value

            return result

        # Combine only if trigger is same.
        for i in range(len(selected)):
            for j in range(i + 1, len(selected)):
                a = selected[i]
                b = selected[j]

                ca = config_lookup[a["candidate"]]
                cb = config_lookup[b["candidate"]]

                if ca["trigger"] != cb["trigger"]:
                    continue

                config = clone_config(
                    ca,
                    "TEMP",
                )

                config = overlay(
                    config,
                    cb,
                )

                sig = config_signature(config)

                if sig in seen:
                    continue

                seen.add(sig)
                counter += 1

                config["label"] = (
                    f"INT{counter:03d}_"
                    f"{a['family']}_"
                    f"{b['family']}"
                )

                config["_m15_atr_lookup"] = atr_lookup

                interaction_configs.append(
                    (
                        f"INTERACTION_{a['family']}_{b['family']}",
                        config,
                    )
                )

                config_lookup[
                    config["label"]
                ] = config

        interaction_rows = []

        STATUS.update({
            "state": "calculating",
            "message": (
                f"Running {len(interaction_configs)} controlled interactions"
            ),
        })

        for family, config in interaction_configs:
            for cost in COST_PIPS_GRID:
                trades = run_config_cached(
                    signals,
                    m15,
                    outcome_cache,
                    config,
                    cost,
                )

                interaction_rows.append(
                    result_row(
                        family,
                        config,
                        cost,
                        trades,
                    )
                )

        write_csv(
            OUTPUT_INTERACTIONS,
            interaction_rows,
        )

        combined_primary = (
            primary_baselines
            + single_primary
            + [
                row
                for row in interaction_rows
                if (
                    abs(
                        float(row["cost_pips"])
                        - PRIMARY_COST_PIPS
                    ) < 1e-12
                    and int(row["trades"]) >= 50
                )
            ]
        )

        combined_primary.sort(
            key=lambda row: (
                float(row["profit_factor"]),
                float(row["expectancy_r"]),
                float(row["total_r"]),
            ),
            reverse=True,
        )

        top_rows = combined_primary[:40]

        write_csv(
            OUTPUT_TOP,
            top_rows,
        )

        finalists = [
            config_lookup[row["candidate"]]
            for row in top_rows[:15]
        ]

        STATUS["message"] = "Running era validation"

        era_rows = validation_rows(
            signals,
            m15,
            outcome_cache,
            finalists,
            era_windows(),
        )

        STATUS["message"] = "Running dev / validation"

        devval_rows = validation_rows(
            signals,
            m15,
            outcome_cache,
            finalists,
            devval_windows(),
        )

        STATUS["message"] = "Running recent windows"

        recent_rows = validation_rows(
            signals,
            m15,
            outcome_cache,
            finalists,
            recent_windows(),
        )

        write_csv(OUTPUT_ERAS, era_rows)
        write_csv(OUTPUT_DEVVAL, devval_rows)
        write_csv(OUTPUT_RECENT, recent_rows)

        robust = []

        for config in finalists:
            label = config["label"]

            base = next(
                row
                for row in top_rows
                if row["candidate"] == label
            )

            eras = [
                row
                for row in era_rows
                if (
                    row["candidate"] == label
                    and int(row["trades"]) > 0
                )
            ]

            devval = [
                row
                for row in devval_rows
                if (
                    row["candidate"] == label
                    and int(row["trades"]) > 0
                )
            ]

            recent = [
                row
                for row in recent_rows
                if (
                    row["candidate"] == label
                    and int(row["trades"]) > 0
                )
            ]

            min_era = (
                min(float(row["profit_factor"]) for row in eras)
                if eras else 0.0
            )

            min_dev = (
                min(float(row["profit_factor"]) for row in devval)
                if devval else 0.0
            )

            min_recent = (
                min(float(row["profit_factor"]) for row in recent)
                if recent else 0.0
            )

            robust.append({
                "candidate": label,
                "trades": int(base["trades"]),
                "full_pf": float(base["profit_factor"]),
                "full_total_r": float(base["total_r"]),
                "minimum_era_pf": min_era,
                "minimum_devval_pf": min_dev,
                "minimum_recent_pf": min_recent,
                "score": (
                    min_era * 3.0
                    + min_dev * 2.0
                    + min_recent * 2.0
                    + float(base["profit_factor"])
                    + float(base["total_r"]) / 50.0
                ),
            })

        robust.sort(
            key=lambda row: row["score"],
            reverse=True,
        )

        robust_finalists = [
            config_lookup[row["candidate"]]
            for row in robust[:6]
        ]

        STATUS["message"] = "Running rolling 2Y / 3Y"

        rolling_rows = []
        rolling_summary_rows = []

        for config in robust_finalists:
            for months in [24, 36]:
                rows = monthly_rolling_rows(
                    signals,
                    m15,
                    outcome_cache,
                    config,
                    months,
                )

                rolling_rows.extend(rows)
                rolling_summary_rows.append(
                    rolling_summary(rows)
                )

        write_csv(
            OUTPUT_ROLLING,
            rolling_rows,
        )

        write_csv(
            OUTPUT_ROLLING_SUMMARY,
            rolling_summary_rows,
        )

        overlap = []

        if robust_finalists:
            best = robust_finalists[0]

            # compare best to strongest baseline from same trigger
            trigger = best["trigger"]

            trigger_baselines = [
                row
                for row in primary_baselines
                if row["trigger"] == trigger
            ]

            if trigger_baselines:
                reference = config_lookup[
                    trigger_baselines[0]["candidate"]
                ]

                overlap.extend(
                    overlap_rows(
                        signals,
                        m15,
                        outcome_cache,
                        reference,
                        best,
                    )
                )

            best_trades = run_config_cached(
                signals,
                m15,
                outcome_cache,
                best,
                PRIMARY_COST_PIPS,
            )
        else:
            best = None
            best_trades = []

        write_csv(
            OUTPUT_OVERLAP,
            overlap,
        )

        write_csv(
            OUTPUT_BEST_TRADES,
            best_trades,
        )

        STATUS.update({
            "state": "packaging",
            "message": "Building single ZIP bundle",
        })

        build_results_bundle()

        STATUS.update({
            "state": "complete",
            "message": (
                "USD/CAD M15 LONG Gen2 alternative-archetype research complete"
            ),
            "baseline_configs": len(baselines),
            "seed_trigger_families": len(seed_configs),
            "context_variants": len(context_variants),
            "interaction_configs": len(interaction_configs),
            "selected_best": best,
            "robust_ranking": robust[:12],
            "results_bundle": OUTPUT_BUNDLE,
        })

    except Exception as error:
        STATUS.update({
            "state": "error",
            "message": str(error),
        })

        print(
            "ERROR:",
            error,
            flush=True,
        )


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def root():
    return jsonify({
        "service":
            "USDCAD M15 Long Gen2 Alternative Archetypes",
        "status":
            STATUS["state"],
        "instrument":
            INSTRUMENT,
        "timeframe":
            "M15",
        "side":
            "BUY",
        "orders_supported":
            False,
        "trading_enabled":
            False,
        "routes": [
            "/usdcad-m15-long-gen2/status",
            "/usdcad-m15-long-gen2/results",
        ],
    })


@app.route(
    "/usdcad-m15-long-gen2/status"
)
def route_status():
    return jsonify(STATUS)


@app.route(
    "/usdcad-m15-long-gen2/results"
)
def route_results():
    return download_file(
        OUTPUT_BUNDLE
    )


if __name__ == "__main__":
    research_thread = threading.Thread(
        target=run_research,
        name="usdcad-m15-long-gen2",
        daemon=True,
    )

    research_thread.start()

    port = int(
        os.getenv("PORT", 5000)
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
    )
