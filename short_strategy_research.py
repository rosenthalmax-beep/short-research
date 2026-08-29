import os
import itertools
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# GBP/USD SHORT - FEATURE SUBSTITUTION DISCOVERY
#
# RESEARCH ONLY — NEVER SUBMITS ORDERS.
#
# Goal:
#   Find NEW information that can replace or relax restrictive
#   existing filters, rather than simply stacking more filters.
#
# Method:
#   1) Test several progressively relaxed "core" versions.
#   2) Test each new feature rule individually.
#   3) Test sensible PAIRS of feature rules from different
#      feature families.
#   4) Record full-history + era results for EVERY test.
#
# This lets us look for things such as:
#   - removing strong-close but replacing it with prior momentum
#   - loosening recent-high structure but adding a sweep feature
#   - removing fast EMA alignment but using EMA slope
#   - allowing more signals while keeping PF / era robustness
#
# IMPORTANT:
#   NO hour or weekday optimisation in this script.
#
# Backtest conventions remain identical:
#   OANDA midpoint H1 candles
#   Daily alignment = 17:00 America/New_York
#   Previous completed daily candle only
#   ATR14 = Wilder/RMA
#   Stop = signal high + 10 ticks
#   Adverse short slippage = 5 ticks
#   Target based on signal-close reference entry
#   Pyramiding = 0
# ============================================================


app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_URL = "https://api-fxtrade.oanda.com"

INSTRUMENT = "GBP_USD"
TICK_SIZE = 0.00001

NY_TZ = ZoneInfo("America/New_York")

DAILY_ALIGNMENT_HOUR = 17
DAILY_ALIGNMENT_TIMEZONE = "America/New_York"

STOP_BUFFER_TICKS = 10
BACKTEST_SLIPPAGE_TICKS = 5

REWARD_RISK = 2.75

H1_CHUNK_DAYS = 180

RESEARCH_FROM = datetime(
    2002, 5, 6, 20, 0,
    tzinfo=timezone.utc,
)

RESEARCH_TO = (
    datetime.now(timezone.utc)
    .replace(
        minute=0,
        second=0,
        microsecond=0,
    )
)

H1_WARMUP_DAYS = 150
DAILY_WARMUP_DAYS = 2200

OUTPUT_FILE = (
    "gbpusd_short_feature_substitution_discovery.csv"
)

NY_HOURS_USED = "ALL"
WEEKDAYS_USED = "ALL"


# ============================================================
# CORE VARIANTS
#
# "benchmark" = current pre-timing candidate.
#
# The others intentionally REMOVE / RELAX existing filters.
# A new feature only interests us if it can rescue one of these
# looser cores without destroying frequency.
# ============================================================

CORE_VARIANTS = [
    {
        "name": "benchmark",
        "body_ratio": 1.10,
        "structure_lookback": 65,
        "max_distance_atr": 0.10,
        "strong_close_max": 0.35,
        "slow_ema": 100,
        "fast_ema": 40,
    },
    {
        "name": "no_strong_close",
        "body_ratio": 1.10,
        "structure_lookback": 65,
        "max_distance_atr": 0.10,
        "strong_close_max": None,
        "slow_ema": 100,
        "fast_ema": 40,
    },
    {
        "name": "no_fast_ema",
        "body_ratio": 1.10,
        "structure_lookback": 65,
        "max_distance_atr": 0.10,
        "strong_close_max": 0.35,
        "slow_ema": 100,
        "fast_ema": None,
    },
    {
        "name": "looser_structure",
        "body_ratio": 1.10,
        "structure_lookback": 45,
        "max_distance_atr": 0.20,
        "strong_close_max": 0.35,
        "slow_ema": 100,
        "fast_ema": 40,
    },
    {
        "name": "looser_body_structure",
        "body_ratio": 1.00,
        "structure_lookback": 45,
        "max_distance_atr": 0.20,
        "strong_close_max": 0.45,
        "slow_ema": 100,
        "fast_ema": 40,
    },
    {
        "name": "minimal_daily_regime",
        "body_ratio": 1.00,
        "structure_lookback": None,
        "max_distance_atr": None,
        "strong_close_max": None,
        "slow_ema": 100,
        "fast_ema": None,
    },
]


# ============================================================
# ERA WINDOWS
# ============================================================

ERAS = [
    (
        "2002_2009",
        datetime(
            2002, 5, 6, 20, 0,
            tzinfo=timezone.utc,
        ),
        datetime(
            2010, 1, 1, 0, 0,
            tzinfo=timezone.utc,
        ),
    ),
    (
        "2010_2017",
        datetime(
            2010, 1, 1, 0, 0,
            tzinfo=timezone.utc,
        ),
        datetime(
            2018, 1, 1, 0, 0,
            tzinfo=timezone.utc,
        ),
    ),
    (
        "2018_2023",
        datetime(
            2018, 1, 1, 0, 0,
            tzinfo=timezone.utc,
        ),
        datetime(
            2024, 1, 1, 0, 0,
            tzinfo=timezone.utc,
        ),
    ),
    (
        "2024_present",
        datetime(
            2024, 1, 1, 0, 0,
            tzinfo=timezone.utc,
        ),
        None,
    ),
]


# ============================================================
# FEATURE RULES
#
# Each rule has a family. Pair-testing never combines two rules
# from the SAME family, which reduces redundant curve-fitting.
#
# Rules are evaluated against precomputed candidate features.
# ============================================================

FEATURE_RULES = []


def add_rule(
    family,
    name,
    feature,
    operator,
    threshold,
):
    FEATURE_RULES.append({
        "family": family,
        "name": name,
        "feature": feature,
        "operator": operator,
        "threshold": threshold,
    })


# ---- Daily fast EMA slope over 5 completed D bars,
# normalized by Daily ATR14.
add_rule("ema40_slope", "ema40_slope_le_0", "ema40_slope_5_atr", "<=", 0.00)
add_rule("ema40_slope", "ema40_slope_le_m002", "ema40_slope_5_atr", "<=", -0.02)
add_rule("ema40_slope", "ema40_slope_le_m005", "ema40_slope_5_atr", "<=", -0.05)
add_rule("ema40_slope", "ema40_slope_le_m010", "ema40_slope_5_atr", "<=", -0.10)

# ---- Slow EMA100 slope over 5 D bars.
add_rule("ema100_slope", "ema100_slope_le_0", "ema100_slope_5_atr", "<=", 0.00)
add_rule("ema100_slope", "ema100_slope_le_m001", "ema100_slope_5_atr", "<=", -0.01)
add_rule("ema100_slope", "ema100_slope_le_m003", "ema100_slope_5_atr", "<=", -0.03)
add_rule("ema100_slope", "ema100_slope_le_m005", "ema100_slope_5_atr", "<=", -0.05)

# ---- How stretched price is below daily EMA100.
add_rule("ema100_stretch", "ema100_stretch_le_050", "ema100_stretch_atr", "<=", 0.50)
add_rule("ema100_stretch", "ema100_stretch_le_075", "ema100_stretch_atr", "<=", 0.75)
add_rule("ema100_stretch", "ema100_stretch_le_100", "ema100_stretch_atr", "<=", 1.00)
add_rule("ema100_stretch", "ema100_stretch_le_150", "ema100_stretch_atr", "<=", 1.50)
add_rule("ema100_stretch", "ema100_stretch_le_200", "ema100_stretch_atr", "<=", 2.00)

# ---- Daily ATR14 relative to its 50-day SMA.
add_rule("daily_vol", "daily_atr_ratio_ge_080", "daily_atr_ratio_50", ">=", 0.80)
add_rule("daily_vol", "daily_atr_ratio_ge_100", "daily_atr_ratio_50", ">=", 1.00)
add_rule("daily_vol", "daily_atr_ratio_ge_120", "daily_atr_ratio_50", ">=", 1.20)
add_rule("daily_vol", "daily_atr_ratio_le_100", "daily_atr_ratio_50", "<=", 1.00)
add_rule("daily_vol", "daily_atr_ratio_le_120", "daily_atr_ratio_50", "<=", 1.20)
add_rule("daily_vol", "daily_atr_ratio_le_140", "daily_atr_ratio_50", "<=", 1.40)

# ---- H1 ATR14 relative to its prior 50-bar mean.
add_rule("h1_vol", "h1_atr_ratio_ge_080", "h1_atr_ratio_50", ">=", 0.80)
add_rule("h1_vol", "h1_atr_ratio_ge_100", "h1_atr_ratio_50", ">=", 1.00)
add_rule("h1_vol", "h1_atr_ratio_ge_120", "h1_atr_ratio_50", ">=", 1.20)
add_rule("h1_vol", "h1_atr_ratio_ge_140", "h1_atr_ratio_50", ">=", 1.40)

# ---- Prior bullish move before the reversal.
add_rule("prior_move_5", "prior_move5_ge_000", "prior_move_5_atr", ">=", 0.00)
add_rule("prior_move_5", "prior_move5_ge_050", "prior_move_5_atr", ">=", 0.50)
add_rule("prior_move_5", "prior_move5_ge_100", "prior_move_5_atr", ">=", 1.00)
add_rule("prior_move_5", "prior_move5_ge_150", "prior_move_5_atr", ">=", 1.50)

add_rule("prior_move_10", "prior_move10_ge_000", "prior_move_10_atr", ">=", 0.00)
add_rule("prior_move_10", "prior_move10_ge_075", "prior_move_10_atr", ">=", 0.75)
add_rule("prior_move_10", "prior_move10_ge_150", "prior_move_10_atr", ">=", 1.50)
add_rule("prior_move_10", "prior_move10_ge_225", "prior_move_10_atr", ">=", 2.25)

# ---- Actual sweep through a prior H1 high.
add_rule("high_sweep", "sweep_prev20", "sweep_prev20", "==", 1.0)
add_rule("high_sweep", "sweep_prev40", "sweep_prev40", "==", 1.0)
add_rule("high_sweep", "sweep_prev65", "sweep_prev65", "==", 1.0)

# ---- How far the bearish close penetrated beyond previous open.
add_rule("engulf_depth", "engulf_depth_ge_010", "engulf_depth_prev_body", ">=", 0.10)
add_rule("engulf_depth", "engulf_depth_ge_025", "engulf_depth_prev_body", ">=", 0.25)
add_rule("engulf_depth", "engulf_depth_ge_050", "engulf_depth_prev_body", ">=", 0.50)
add_rule("engulf_depth", "engulf_depth_ge_075", "engulf_depth_prev_body", ">=", 0.75)

# ---- Current candle body as fraction of total range.
add_rule("body_fraction", "body_fraction_ge_050", "body_fraction", ">=", 0.50)
add_rule("body_fraction", "body_fraction_ge_060", "body_fraction", ">=", 0.60)
add_rule("body_fraction", "body_fraction_ge_070", "body_fraction", ">=", 0.70)
add_rule("body_fraction", "body_fraction_ge_080", "body_fraction", ">=", 0.80)

# ---- Previous bullish candle size.
add_rule("previous_range", "previous_range_ge_050", "previous_range_atr", ">=", 0.50)
add_rule("previous_range", "previous_range_ge_075", "previous_range_atr", ">=", 0.75)
add_rule("previous_range", "previous_range_ge_100", "previous_range_atr", ">=", 1.00)
add_rule("previous_range", "previous_range_ge_125", "previous_range_atr", ">=", 1.25)

# ---- Signal candle range (retested as a possible substitute).
add_rule("signal_range", "signal_range_ge_080", "signal_range_atr", ">=", 0.80)
add_rule("signal_range", "signal_range_ge_090", "signal_range_atr", ">=", 0.90)
add_rule("signal_range", "signal_range_ge_100", "signal_range_atr", ">=", 1.00)
add_rule("signal_range", "signal_range_ge_110", "signal_range_atr", ">=", 1.10)

# ---- Number of rising closes immediately before signal.
add_rule("rising_closes", "rising_closes3_ge_2", "rising_closes_3", ">=", 2.0)
add_rule("rising_closes", "rising_closes5_ge_3", "rising_closes_5", ">=", 3.0)
add_rule("rising_closes", "rising_closes5_ge_4", "rising_closes_5", ">=", 4.0)

# ---- Previous completed daily candle context.
add_rule("daily_prev_direction", "previous_day_bullish", "previous_day_bullish", "==", 1.0)
add_rule("daily_prev_close_location", "previous_day_close_ge_050", "previous_day_close_location", ">=", 0.50)
add_rule("daily_prev_close_location", "previous_day_close_ge_070", "previous_day_close_location", ">=", 0.70)


# ============================================================
# TEST SETS
#
# BASELINE = no new feature.
# SINGLES  = every feature individually.
# PAIRS    = every pair from DIFFERENT feature families.
# ============================================================

TEST_RULE_SETS = [
    {
        "test_type": "BASELINE",
        "rule_1": None,
        "rule_2": None,
    }
]

for rule in FEATURE_RULES:
    TEST_RULE_SETS.append({
        "test_type": "SINGLE",
        "rule_1": rule,
        "rule_2": None,
    })

for i in range(len(FEATURE_RULES)):
    for j in range(i + 1, len(FEATURE_RULES)):
        first = FEATURE_RULES[i]
        second = FEATURE_RULES[j]

        if first["family"] == second["family"]:
            continue

        TEST_RULE_SETS.append({
            "test_type": "PAIR",
            "rule_1": first,
            "rule_2": second,
        })


TOTAL_TESTS = (
    len(CORE_VARIANTS)
    * len(TEST_RULE_SETS)
)


# ============================================================
# STATUS
# ============================================================

STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "service": "GBPUSD Short Feature Substitution Discovery",
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "reward_risk": REWARD_RISK,
    "core_variants": len(CORE_VARIANTS),
    "feature_rules": len(FEATURE_RULES),
    "rule_sets_per_core": len(TEST_RULE_SETS),
    "total_tests": TOTAL_TESTS,
    "completed_tests": 0,
    "rows_saved": 0,
    "output_file": None,
}


# ============================================================
# OANDA
# ============================================================

def headers():
    if not OANDA_TOKEN:
        raise RuntimeError(
            "OANDA_TOKEN is not configured"
        )

    return {
        "Authorization": f"Bearer {OANDA_TOKEN}"
    }


def iso_utc(dt):
    return (
        dt.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def oanda_get(path, params):
    response = requests.get(
        OANDA_URL + path,
        headers=headers(),
        params=params,
        timeout=30,
    )

    if not response.ok:
        raise RuntimeError(
            f"OANDA {response.status_code}: "
            f"{response.text[:500]}"
        )

    return response.json()


def parse_candle(raw):
    if not raw.get("complete", False):
        return None

    mid = raw.get("mid")
    if not mid:
        return None

    return {
        "time": datetime.fromisoformat(
            raw["time"].replace("Z", "+00:00")
        ),
        "open": float(mid["o"]),
        "high": float(mid["h"]),
        "low": float(mid["l"]),
        "close": float(mid["c"]),
    }


def fetch_range(
    instrument,
    granularity,
    start,
    end,
):
    params = {
        "price": "M",
        "granularity": granularity,
        "from": iso_utc(start),
        "to": iso_utc(end),
        "smooth": "false",
        "includeFirst": "true",
        "dailyAlignment": DAILY_ALIGNMENT_HOUR,
        "alignmentTimezone": DAILY_ALIGNMENT_TIMEZONE,
    }

    data = oanda_get(
        f"/v3/instruments/{instrument}/candles",
        params,
    )

    candles = []

    for raw in data.get("candles", []):
        candle = parse_candle(raw)

        if candle is not None:
            candles.append(candle)

    return candles


def fetch_chunked_history(
    instrument,
    granularity,
    start,
    end,
):
    candles_by_time = {}
    cursor = start

    while cursor < end:
        chunk_end = min(
            cursor + timedelta(days=H1_CHUNK_DAYS),
            end,
        )

        print(
            f"Fetching {granularity}: "
            f"{cursor.date()} -> {chunk_end.date()}",
            flush=True,
        )

        chunk = fetch_range(
            instrument,
            granularity,
            cursor,
            chunk_end,
        )

        for candle in chunk:
            candles_by_time[candle["time"]] = candle

        cursor = chunk_end

    candles = list(
        candles_by_time.values()
    )

    candles.sort(
        key=lambda item: item["time"]
    )

    return candles


# ============================================================
# INDICATORS
# ============================================================

def sma_series(values, length):
    result = [None] * len(values)

    if len(values) < length:
        return result

    running = sum(values[:length])
    result[length - 1] = running / length

    for index in range(length, len(values)):
        running += values[index]
        running -= values[index - length]
        result[index] = running / length

    return result


def ema_series(values, length):
    result = [None] * len(values)

    if len(values) < length:
        return result

    initial = sum(values[:length]) / length
    result[length - 1] = initial

    multiplier = 2.0 / (length + 1.0)
    previous = initial

    for index in range(length, len(values)):
        current = (
            (values[index] - previous)
            * multiplier
            + previous
        )

        result[index] = current
        previous = current

    return result


def true_ranges(candles):
    result = []

    for index, candle in enumerate(candles):
        if index == 0:
            tr = (
                candle["high"]
                - candle["low"]
            )

        else:
            previous_close = (
                candles[index - 1]["close"]
            )

            tr = max(
                candle["high"] - candle["low"],
                abs(
                    candle["high"]
                    - previous_close
                ),
                abs(
                    candle["low"]
                    - previous_close
                ),
            )

        result.append(tr)

    return result


def rma_series(values, length):
    result = [None] * len(values)

    if len(values) < length:
        return result

    initial = (
        sum(values[:length])
        / length
    )

    result[length - 1] = initial
    previous = initial

    for index in range(length, len(values)):
        current = (
            previous * (length - 1)
            + values[index]
        ) / length

        result[index] = current
        previous = current

    return result


def atr_series(candles, length=14):
    return rma_series(
        true_ranges(candles),
        length,
    )


# ============================================================
# DAILY ALIGNMENT / STATE
# ============================================================

def current_daily_start(timestamp_utc):
    ny_time = (
        timestamp_utc
        .astimezone(NY_TZ)
    )

    candidate = ny_time.replace(
        hour=DAILY_ALIGNMENT_HOUR,
        minute=0,
        second=0,
        microsecond=0,
    )

    if ny_time < candidate:
        candidate -= timedelta(days=1)

    return candidate.astimezone(
        timezone.utc
    )


def build_daily_state(daily):
    closes = [
        candle["close"]
        for candle in daily
    ]

    ema40 = ema_series(
        closes,
        40,
    )

    ema100 = ema_series(
        closes,
        100,
    )

    atr14 = atr_series(
        daily,
        14,
    )

    atr14_sma50 = sma_series(
        [
            value if value is not None else 0.0
            for value in atr14
        ],
        50,
    )

    # The SMA above is only valid after ATR itself exists for
    # all 50 bars. Blank out the warmup area explicitly.
    for i in range(len(atr14_sma50)):
        if i < 62:
            atr14_sma50[i] = None

    return {
        "ema40": ema40,
        "ema100": ema100,
        "atr14": atr14,
        "atr14_sma50": atr14_sma50,
    }


def build_h1_daily_lookup(
    h1,
    daily,
    daily_state,
):
    lookup = [None] * len(h1)
    daily_index = -1

    for h1_index, candle in enumerate(h1):
        session_start = current_daily_start(
            candle["time"]
        )

        while (
            daily_index + 1 < len(daily)
            and daily[daily_index + 1]["time"]
            < session_start
        ):
            daily_index += 1

        if daily_index < 0:
            continue

        d = daily[daily_index]

        ema40 = daily_state["ema40"][daily_index]
        ema100 = daily_state["ema100"][daily_index]
        datr = daily_state["atr14"][daily_index]
        datr_sma50 = (
            daily_state["atr14_sma50"][daily_index]
        )

        ema40_slope = None
        ema100_slope = None

        if (
            daily_index >= 5
            and datr is not None
            and datr > 0
        ):
            old40 = daily_state["ema40"][daily_index - 5]
            old100 = daily_state["ema100"][daily_index - 5]

            if (
                ema40 is not None
                and old40 is not None
            ):
                ema40_slope = (
                    ema40 - old40
                ) / datr

            if (
                ema100 is not None
                and old100 is not None
            ):
                ema100_slope = (
                    ema100 - old100
                ) / datr

        ema100_stretch = None

        if (
            ema100 is not None
            and datr is not None
            and datr > 0
        ):
            ema100_stretch = (
                ema100 - d["close"]
            ) / datr

        daily_atr_ratio = None

        if (
            datr is not None
            and datr_sma50 is not None
            and datr_sma50 > 0
        ):
            daily_atr_ratio = (
                datr / datr_sma50
            )

        day_range = (
            d["high"] - d["low"]
        )

        previous_day_close_location = None

        if day_range > 0:
            previous_day_close_location = (
                d["close"] - d["low"]
            ) / day_range

        lookup[h1_index] = {
            "close": d["close"],
            "open": d["open"],
            "high": d["high"],
            "low": d["low"],
            "ema40": ema40,
            "ema100": ema100,
            "atr14": datr,
            "ema40_slope_5_atr": ema40_slope,
            "ema100_slope_5_atr": ema100_slope,
            "ema100_stretch_atr": ema100_stretch,
            "daily_atr_ratio_50": daily_atr_ratio,
            "previous_day_bullish": (
                1.0
                if d["close"] > d["open"]
                else 0.0
            ),
            "previous_day_close_location": (
                previous_day_close_location
            ),
        }

    return lookup


# ============================================================
# CANDIDATE FEATURE SET
# ============================================================

def count_rising_closes(
    h1,
    signal_index,
    transitions,
):
    count = 0

    start = (
        signal_index
        - transitions
    )

    for i in range(
        start + 1,
        signal_index,
    ):
        if (
            h1[i]["close"]
            > h1[i - 1]["close"]
        ):
            count += 1

    return count


def build_candidates(
    h1,
    h1_atr,
    h1_atr_sma50,
    daily_lookup,
):
    candidates = []

    max_needed = 65

    for index in range(
        max_needed,
        len(h1),
    ):
        signal = h1[index]

        if signal["time"] < RESEARCH_FROM:
            continue

        if signal["time"] >= RESEARCH_TO:
            break

        previous = h1[index - 1]
        atr = h1_atr[index]
        atr_sma50 = h1_atr_sma50[index]
        daily = daily_lookup[index]

        if (
            atr is None
            or atr <= 0
            or daily is None
        ):
            continue

        previous_body = abs(
            previous["close"]
            - previous["open"]
        )

        current_body = abs(
            signal["close"]
            - signal["open"]
        )

        if (
            previous_body <= 0
            or current_body <= 0
        ):
            continue

        bearish_engulfing = (
            previous["close"] > previous["open"]
            and signal["close"] < signal["open"]
            and signal["open"] >= previous["close"]
            and signal["close"] <= previous["open"]
        )

        if not bearish_engulfing:
            continue

        signal_range = (
            signal["high"]
            - signal["low"]
        )

        previous_range = (
            previous["high"]
            - previous["low"]
        )

        if signal_range <= 0:
            continue

        structure_distances = {}

        prior_highs = {}

        for lookback in [20, 40, 45, 65]:
            prior_high = max(
                candle["high"]
                for candle in h1[
                    index - lookback:index
                ]
            )

            prior_highs[lookback] = prior_high

            structure_distances[lookback] = (
                prior_high - signal["high"]
            ) / atr

        engulf_depth = (
            previous["open"]
            - signal["close"]
        ) / previous_body

        body_fraction = (
            current_body
            / signal_range
        )

        h1_atr_ratio = None

        if (
            atr_sma50 is not None
            and atr_sma50 > 0
        ):
            h1_atr_ratio = (
                atr / atr_sma50
            )

        prior_move_5 = (
            previous["close"]
            - h1[index - 6]["close"]
        ) / atr

        prior_move_10 = (
            previous["close"]
            - h1[index - 11]["close"]
        ) / atr

        candidates.append({
            "index": index,
            "time": signal["time"],

            # Existing/core features
            "body_ratio": (
                current_body
                / previous_body
            ),
            "structure_distances": structure_distances,
            "strong_close": (
                signal["close"]
                - signal["low"]
            ) / signal_range,
            "daily": daily,

            # New/substitution features
            "ema40_slope_5_atr": (
                daily["ema40_slope_5_atr"]
            ),
            "ema100_slope_5_atr": (
                daily["ema100_slope_5_atr"]
            ),
            "ema100_stretch_atr": (
                daily["ema100_stretch_atr"]
            ),
            "daily_atr_ratio_50": (
                daily["daily_atr_ratio_50"]
            ),
            "h1_atr_ratio_50": h1_atr_ratio,
            "prior_move_5_atr": prior_move_5,
            "prior_move_10_atr": prior_move_10,
            "sweep_prev20": (
                1.0
                if signal["high"] >= prior_highs[20]
                else 0.0
            ),
            "sweep_prev40": (
                1.0
                if signal["high"] >= prior_highs[40]
                else 0.0
            ),
            "sweep_prev65": (
                1.0
                if signal["high"] >= prior_highs[65]
                else 0.0
            ),
            "engulf_depth_prev_body": engulf_depth,
            "body_fraction": body_fraction,
            "previous_range_atr": (
                previous_range / atr
            ),
            "signal_range_atr": (
                signal_range / atr
            ),
            "rising_closes_3": float(
                count_rising_closes(
                    h1,
                    index,
                    3,
                )
            ),
            "rising_closes_5": float(
                count_rising_closes(
                    h1,
                    index,
                    5,
                )
            ),
            "previous_day_bullish": (
                daily["previous_day_bullish"]
            ),
            "previous_day_close_location": (
                daily[
                    "previous_day_close_location"
                ]
            ),
        })

    return candidates


# ============================================================
# CORE FILTERS
# ============================================================

def core_allowed(
    candidate,
    core,
):
    if (
        candidate["body_ratio"]
        < core["body_ratio"]
    ):
        return False

    if (
        core["structure_lookback"]
        is not None
    ):
        distance = (
            candidate[
                "structure_distances"
            ][
                core["structure_lookback"]
            ]
        )

        if (
            distance
            > core["max_distance_atr"]
        ):
            return False

    if (
        core["strong_close_max"]
        is not None
        and candidate["strong_close"]
        > core["strong_close_max"]
    ):
        return False

    daily = candidate["daily"]

    slow = daily.get(
        "ema100"
    )

    if slow is None:
        return False

    if not (
        daily["close"]
        < slow
    ):
        return False

    if (
        core["fast_ema"]
        is not None
    ):
        fast = daily.get(
            "ema40"
        )

        if fast is None:
            return False

        if not (
            fast < slow
        ):
            return False

    return True


# ============================================================
# FEATURE RULE EVALUATION
# ============================================================

def rule_passes(
    candidate,
    rule,
):
    if rule is None:
        return True

    value = candidate.get(
        rule["feature"]
    )

    if value is None:
        return False

    threshold = rule[
        "threshold"
    ]

    operator = rule[
        "operator"
    ]

    if operator == ">=":
        return value >= threshold

    if operator == "<=":
        return value <= threshold

    if operator == "==":
        return value == threshold

    raise RuntimeError(
        f"Unknown operator: {operator}"
    )


def rules_pass(
    candidate,
    rule_1,
    rule_2,
):
    return (
        rule_passes(
            candidate,
            rule_1,
        )
        and
        rule_passes(
            candidate,
            rule_2,
        )
    )


# ============================================================
# EXIT SIMULATION
# ============================================================

EXIT_CACHE = {}


def calculate_trade_exit(
    h1,
    signal_index,
):
    cache_key = (
        signal_index,
        REWARD_RISK,
    )

    if cache_key in EXIT_CACHE:
        return EXIT_CACHE[
            cache_key
        ]

    signal = h1[
        signal_index
    ]

    reference_entry = (
        signal["close"]
    )

    backtest_entry = (
        reference_entry
        - BACKTEST_SLIPPAGE_TICKS
        * TICK_SIZE
    )

    stop = (
        signal["high"]
        + STOP_BUFFER_TICKS
        * TICK_SIZE
    )

    reference_risk = (
        stop
        - reference_entry
    )

    if reference_risk <= 0:
        raise RuntimeError(
            "Invalid short reference risk"
        )

    target = (
        reference_entry
        - reference_risk
        * REWARD_RISK
    )

    actual_risk = (
        stop
        - backtest_entry
    )

    if actual_risk <= 0:
        raise RuntimeError(
            "Invalid short actual risk"
        )

    for index in range(
        signal_index + 1,
        len(h1),
    ):
        candle = h1[index]

        if (
            candle["time"]
            >= RESEARCH_TO
        ):
            break

        stop_hit = (
            candle["high"]
            >= stop
        )

        target_hit = (
            candle["low"]
            <= target
        )

        if not (
            stop_hit
            or target_hit
        ):
            continue

        if (
            stop_hit
            and target_hit
        ):
            distance_to_high = abs(
                candle["high"]
                - candle["open"]
            )

            distance_to_low = abs(
                candle["open"]
                - candle["low"]
            )

            if (
                distance_to_high
                < distance_to_low
            ):
                exit_price = stop
                exit_reason = "STOP"

            else:
                exit_price = target
                exit_reason = "TARGET"

        elif stop_hit:
            exit_price = stop
            exit_reason = "STOP"

        else:
            exit_price = target
            exit_reason = "TARGET"

        result_r = (
            backtest_entry
            - exit_price
        ) / actual_risk

        result = {
            "status": "CLOSED",
            "signal_index": signal_index,
            "signal_time": signal["time"],
            "exit_index": index,
            "exit_time": candle["time"],
            "exit_reason": exit_reason,
            "result_r": result_r,
        }

        EXIT_CACHE[
            cache_key
        ] = result

        return result

    result = {
        "status": "OPEN",
        "signal_index": signal_index,
        "signal_time": signal["time"],
        "exit_index": None,
        "exit_time": None,
        "exit_reason": None,
        "result_r": None,
    }

    EXIT_CACHE[
        cache_key
    ] = result

    return result


def simulate(
    h1,
    candidates,
):
    trades = []
    position_exit_index = -1
    ignored = 0
    still_open = False

    for candidate in candidates:
        signal_index = candidate[
            "index"
        ]

        # Same exact convention as prior research.
        if (
            signal_index
            < position_exit_index
        ):
            ignored += 1
            continue

        trade = calculate_trade_exit(
            h1,
            signal_index,
        )

        if (
            trade["status"]
            == "OPEN"
        ):
            still_open = True
            break

        trades.append(
            trade
        )

        position_exit_index = (
            trade["exit_index"]
        )

    return (
        trades,
        ignored,
        still_open,
    )


# ============================================================
# STATS
# ============================================================

def stats_for_trades(
    trades,
    start=None,
    end=None,
):
    filtered = []

    for trade in trades:
        signal_time = trade[
            "signal_time"
        ]

        if (
            start is not None
            and signal_time < start
        ):
            continue

        if (
            end is not None
            and signal_time >= end
        ):
            continue

        filtered.append(
            trade
        )

    if not filtered:
        return {
            "trades": 0,
            "winners": 0,
            "losers": 0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "total_r": 0.0,
            "expectancy_r": 0.0,
            "max_drawdown_r": 0.0,
            "longest_loss_streak": 0,
        }

    results = [
        trade["result_r"]
        for trade in filtered
    ]

    winners = [
        value
        for value in results
        if value > 0
    ]

    losers = [
        value
        for value in results
        if value < 0
    ]

    gross_profit = sum(
        winners
    )

    gross_loss = abs(
        sum(losers)
    )

    total_r = sum(
        results
    )

    if gross_loss > 0:
        pf = (
            gross_profit
            / gross_loss
        )
    elif gross_profit > 0:
        pf = 999.0
    else:
        pf = 0.0

    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0

    for result in results:
        equity += result
        peak = max(
            peak,
            equity,
        )
        max_drawdown = min(
            max_drawdown,
            equity - peak,
        )

    current_streak = 0
    longest_streak = 0

    for result in results:
        if result < 0:
            current_streak += 1
            longest_streak = max(
                longest_streak,
                current_streak,
            )
        else:
            current_streak = 0

    return {
        "trades": len(results),
        "winners": len(winners),
        "losers": len(losers),
        "win_rate": round(
            len(winners)
            / len(results)
            * 100.0,
            2,
        ),
        "profit_factor": round(
            pf,
            3,
        ),
        "total_r": round(
            total_r,
            2,
        ),
        "expectancy_r": round(
            total_r / len(results),
            3,
        ),
        "max_drawdown_r": round(
            max_drawdown,
            2,
        ),
        "longest_loss_streak": (
            longest_streak
        ),
    }


# ============================================================
# RESEARCH
# ============================================================

def run_research():
    global STATUS

    try:
        print()
        print("=" * 74)
        print(
            "GBP/USD SHORT - FEATURE SUBSTITUTION DISCOVERY"
        )
        print("=" * 74)
        print("ALL HOURS")
        print("ALL WEEKDAYS")
        print(
            "RR fixed at:",
            REWARD_RISK,
        )
        print(
            "Core variants:",
            len(CORE_VARIANTS),
        )
        print(
            "Feature rules:",
            len(FEATURE_RULES),
        )
        print(
            "Rule sets per core:",
            len(TEST_RULE_SETS),
        )
        print(
            "Total tests:",
            TOTAL_TESTS,
        )
        print()

        # ----------------------------------------------------
        # FETCH
        # ----------------------------------------------------

        STATUS.update({
            "state": "fetching_data",
            "message": (
                "Fetching GBP/USD OANDA history"
            ),
        })

        h1 = fetch_chunked_history(
            INSTRUMENT,
            "H1",
            RESEARCH_FROM
            - timedelta(
                days=H1_WARMUP_DAYS
            ),
            RESEARCH_TO,
        )

        daily = fetch_chunked_history(
            INSTRUMENT,
            "D",
            RESEARCH_FROM
            - timedelta(
                days=DAILY_WARMUP_DAYS
            ),
            RESEARCH_TO,
        )

        if not h1:
            raise RuntimeError(
                "No GBP/USD H1 candles returned"
            )

        if not daily:
            raise RuntimeError(
                "No GBP/USD daily candles returned"
            )

        print(
            "H1 candles:",
            len(h1),
        )
        print(
            "Earliest H1:",
            h1[0]["time"].isoformat(),
        )
        print(
            "Latest H1:",
            h1[-1]["time"].isoformat(),
        )
        print(
            "Daily candles:",
            len(daily),
        )
        print()

        # ----------------------------------------------------
        # PRECOMPUTE
        # ----------------------------------------------------

        STATUS.update({
            "state": "precomputing",
            "message": (
                "Building indicators and "
                "feature matrix"
            ),
        })

        h1_atr = atr_series(
            h1,
            14,
        )

        # H1 ATR ratio uses prior/rolling 50-bar mean of ATR14.
        h1_atr_for_sma = [
            value if value is not None else 0.0
            for value in h1_atr
        ]

        h1_atr_sma50 = sma_series(
            h1_atr_for_sma,
            50,
        )

        for i in range(
            min(
                63,
                len(h1_atr_sma50),
            )
        ):
            h1_atr_sma50[i] = None

        daily_state = (
            build_daily_state(
                daily
            )
        )

        daily_lookup = (
            build_h1_daily_lookup(
                h1,
                daily,
                daily_state,
            )
        )

        candidates = (
            build_candidates(
                h1,
                h1_atr,
                h1_atr_sma50,
                daily_lookup,
            )
        )

        STATUS[
            "base_bearish_engulfings"
        ] = len(candidates)

        print(
            "Base bearish engulfings:",
            len(candidates),
        )
        print()

        # ----------------------------------------------------
        # PRECOMPUTE CORE CANDIDATES
        # ----------------------------------------------------

        core_candidate_cache = {}

        for core in CORE_VARIANTS:
            core_candidate_cache[
                core["name"]
            ] = [
                candidate
                for candidate in candidates
                if core_allowed(
                    candidate,
                    core,
                )
            ]

            print(
                f"{core['name']}: "
                f"{len(core_candidate_cache[core['name']])} "
                f"raw eligible signals",
                flush=True,
            )

        print()

        # ----------------------------------------------------
        # TESTS
        # ----------------------------------------------------

        STATUS.update({
            "state": "running",
            "message": (
                "Testing feature substitutions "
                "and feature pairs"
            ),
        })

        rows = []
        completed = 0

        years = (
            RESEARCH_TO
            - RESEARCH_FROM
        ).total_seconds() / (
            365.2425
            * 24
            * 60
            * 60
        )

        for core in CORE_VARIANTS:
            core_name = core[
                "name"
            ]

            core_candidates = (
                core_candidate_cache[
                    core_name
                ]
            )

            for test_set in TEST_RULE_SETS:
                rule_1 = test_set[
                    "rule_1"
                ]

                rule_2 = test_set[
                    "rule_2"
                ]

                eligible = [
                    candidate
                    for candidate in core_candidates
                    if rules_pass(
                        candidate,
                        rule_1,
                        rule_2,
                    )
                ]

                (
                    trades,
                    ignored,
                    still_open,
                ) = simulate(
                    h1,
                    eligible,
                )

                full = stats_for_trades(
                    trades
                )

                row = {
                    "core_name": core_name,
                    "test_type": (
                        test_set[
                            "test_type"
                        ]
                    ),

                    # Core settings
                    "core_body_ratio": (
                        core["body_ratio"]
                    ),
                    "core_structure_lookback": (
                        core["structure_lookback"]
                    ),
                    "core_max_distance_atr": (
                        core["max_distance_atr"]
                    ),
                    "core_strong_close_max": (
                        core["strong_close_max"]
                    ),
                    "core_slow_ema": (
                        core["slow_ema"]
                    ),
                    "core_fast_ema": (
                        core["fast_ema"]
                    ),

                    # New rule 1
                    "rule1_family": (
                        None
                        if rule_1 is None
                        else rule_1["family"]
                    ),
                    "rule1_name": (
                        None
                        if rule_1 is None
                        else rule_1["name"]
                    ),

                    # New rule 2
                    "rule2_family": (
                        None
                        if rule_2 is None
                        else rule_2["family"]
                    ),
                    "rule2_name": (
                        None
                        if rule_2 is None
                        else rule_2["name"]
                    ),

                    "raw_signals": len(
                        eligible
                    ),
                    "ignored_due_to_open_trade": (
                        ignored
                    ),
                    "still_open_at_end": (
                        still_open
                    ),

                    "trades": full[
                        "trades"
                    ],
                    "trades_per_year": round(
                        full["trades"] / years,
                        2,
                    ),
                    "winners": full[
                        "winners"
                    ],
                    "losers": full[
                        "losers"
                    ],
                    "win_rate": full[
                        "win_rate"
                    ],
                    "profit_factor": full[
                        "profit_factor"
                    ],
                    "total_r": full[
                        "total_r"
                    ],
                    "expectancy_r": full[
                        "expectancy_r"
                    ],
                    "max_drawdown_r": full[
                        "max_drawdown_r"
                    ],
                    "longest_loss_streak": full[
                        "longest_loss_streak"
                    ],
                }

                profitable_eras = 0
                eras_with_5_plus = 0
                profitable_eras_with_5_plus = 0
                minimum_era_pf_5_plus = None
                minimum_era_expectancy_5_plus = None

                for (
                    era_name,
                    era_start,
                    era_end,
                ) in ERAS:
                    era = stats_for_trades(
                        trades,
                        era_start,
                        era_end,
                    )

                    row[
                        f"{era_name}_trades"
                    ] = era["trades"]

                    row[
                        f"{era_name}_pf"
                    ] = era[
                        "profit_factor"
                    ]

                    row[
                        f"{era_name}_r"
                    ] = era[
                        "total_r"
                    ]

                    row[
                        f"{era_name}_expectancy"
                    ] = era[
                        "expectancy_r"
                    ]

                    row[
                        f"{era_name}_win_rate"
                    ] = era[
                        "win_rate"
                    ]

                    if (
                        era["total_r"]
                        > 0
                    ):
                        profitable_eras += 1

                    if (
                        era["trades"]
                        >= 5
                    ):
                        eras_with_5_plus += 1

                        if (
                            era["total_r"]
                            > 0
                        ):
                            profitable_eras_with_5_plus += 1

                        if (
                            minimum_era_pf_5_plus
                            is None
                        ):
                            minimum_era_pf_5_plus = (
                                era[
                                    "profit_factor"
                                ]
                            )
                        else:
                            minimum_era_pf_5_plus = min(
                                minimum_era_pf_5_plus,
                                era[
                                    "profit_factor"
                                ],
                            )

                        if (
                            minimum_era_expectancy_5_plus
                            is None
                        ):
                            minimum_era_expectancy_5_plus = (
                                era[
                                    "expectancy_r"
                                ]
                            )
                        else:
                            minimum_era_expectancy_5_plus = min(
                                minimum_era_expectancy_5_plus,
                                era[
                                    "expectancy_r"
                                ],
                            )

                row[
                    "profitable_eras"
                ] = profitable_eras

                row[
                    "eras_with_5_plus_trades"
                ] = eras_with_5_plus

                row[
                    "profitable_eras_with_5_plus_trades"
                ] = (
                    profitable_eras_with_5_plus
                )

                row[
                    "minimum_era_pf_5_plus"
                ] = minimum_era_pf_5_plus

                row[
                    "minimum_era_expectancy_5_plus"
                ] = (
                    minimum_era_expectancy_5_plus
                )

                rows.append(
                    row
                )

                completed += 1

                STATUS[
                    "completed_tests"
                ] = completed

                if completed % 500 == 0:
                    print(
                        f"Progress: "
                        f"{completed}/{TOTAL_TESTS}",
                        flush=True,
                    )

        # ----------------------------------------------------
        # OUTPUT
        # ----------------------------------------------------

        df = pd.DataFrame(
            rows
        )

        if df.empty:
            raise RuntimeError(
                "No feature-substitution results generated"
            )

        df[
            "adequate_80"
        ] = (
            df["trades"]
            >= 80
        )

        df[
            "adequate_100"
        ] = (
            df["trades"]
            >= 100
        )

        df[
            "adequate_120"
        ] = (
            df["trades"]
            >= 120
        )

        # Rank in a way that favours:
        # 1. decent sample
        # 2. all-era robustness
        # 3. worst-era quality
        # 4. full PF
        # 5. expectancy
        # 6. frequency
        df = df.sort_values(
            by=[
                "adequate_100",
                "profitable_eras_with_5_plus_trades",
                "minimum_era_pf_5_plus",
                "profit_factor",
                "expectancy_r",
                "trades",
            ],
            ascending=[
                False,
                False,
                False,
                False,
                False,
                False,
            ],
        )

        df.to_csv(
            OUTPUT_FILE,
            index=False,
        )

        # ----------------------------------------------------
        # LOG SUMMARY
        # ----------------------------------------------------

        print()
        print("=" * 74)
        print(
            "FEATURE SUBSTITUTION DISCOVERY COMPLETE"
        )
        print("=" * 74)

        display_columns = [
            "core_name",
            "test_type",
            "rule1_name",
            "rule2_name",
            "trades",
            "trades_per_year",
            "win_rate",
            "profit_factor",
            "total_r",
            "expectancy_r",
            "max_drawdown_r",
            "longest_loss_streak",
            "profitable_eras_with_5_plus_trades",
            "minimum_era_pf_5_plus",
            "2002_2009_pf",
            "2010_2017_pf",
            "2018_2023_pf",
            "2024_present_pf",
        ]

        print(
            df[
                df["trades"] >= 100
            ][
                display_columns
            ]
            .head(50)
            .to_string(
                index=False
            )
        )

        STATUS.update({
            "state": "complete",
            "message": (
                "GBP/USD feature substitution "
                "discovery completed successfully"
            ),
            "completed_tests": TOTAL_TESTS,
            "rows_saved": len(df),
            "output_file": OUTPUT_FILE,
            "earliest_h1": (
                h1[0]["time"].isoformat()
            ),
            "latest_h1": (
                h1[-1]["time"].isoformat()
            ),
        })

        print()
        print(
            "Saved:",
            OUTPUT_FILE,
            flush=True,
        )

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
def home():
    return jsonify({
        "service": (
            "GBPUSD Short Feature "
            "Substitution Discovery"
        ),
        "status": STATUS,
        "instrument": INSTRUMENT,
        "direction": "SHORT",
        "reward_risk": REWARD_RISK,
        "hours": NY_HOURS_USED,
        "weekdays": WEEKDAYS_USED,
        "method": (
            "Relax existing filters and test new "
            "features singly and in cross-family pairs"
        ),
        "core_variants": [
            core["name"]
            for core in CORE_VARIANTS
        ],
        "feature_rule_count": len(
            FEATURE_RULES
        ),
        "rule_sets_per_core": len(
            TEST_RULE_SETS
        ),
        "total_tests": TOTAL_TESTS,
        "download": "/download",
        "trading_enabled": False,
        "orders_supported": False,
        "executor_connected": False,
    })


@app.route("/status")
def status():
    return jsonify(
        STATUS
    )


@app.route("/download")
def download():
    if not os.path.exists(
        OUTPUT_FILE
    ):
        return jsonify({
            "status": "not_ready",
            "message": "CSV is not ready yet",
        }), 404

    return send_file(
        OUTPUT_FILE,
        as_attachment=True,
        download_name=OUTPUT_FILE,
    )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    research_thread = threading.Thread(
        target=run_research,
        name=(
            "gbpusd-short-feature-"
            "substitution-discovery"
        ),
        daemon=True,
    )

    research_thread.start()

    port = int(
        os.getenv(
            "PORT",
            5000,
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
    )
