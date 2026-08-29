import os
import itertools
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# USD/JPY SHORT - FEATURE / REGIME DISCOVERY
#
# RESEARCH ONLY — NEVER SUBMITS ORDERS.
#
# Purpose:
#   The broad and tight structural sweeps found a coherent but
#   not yet robust USD/JPY short edge. This run tests whether
#   market-state / candle-quality features can improve or
#   substitute for some of that structure.
#
# IMPORTANT:
#   - No timing / weekday optimisation.
#   - Multiple cores are tested, including relaxed structure.
#   - Features are combined systematically rather than simply
#     stacked onto one already-tight core.
#
# Feature families:
#   1. Strong bearish close
#   2. Fast EMA below slow EMA
#   3. Slow EMA 5-day slope
#   4. Daily ATR regime vs 50-day ATR mean
#   5. Minimum signal range / H1 ATR
#   6. Minimum upper wick / body
#   7. Prior bullish move over 5 H1 bars / ATR
#
# Exact backtest conventions retained:
#   OANDA midpoint H1
#   Daily alignment = 17:00 America/New_York
#   Previous completed daily candle only
#   ATR14 = Wilder/RMA
#   Daily EMA = SMA-seeded EMA
#   Stop = signal high + 10 ticks
#   Adverse short slippage = 5 ticks
#   Target from reference signal close
#   Pyramiding = 0
#   Same-bar SL/TP tie rule retained
# ============================================================


app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_URL = "https://api-fxtrade.oanda.com"

INSTRUMENT = "USD_JPY"
TICK_SIZE = 0.001

NY_TZ = ZoneInfo("America/New_York")

DAILY_ALIGNMENT_HOUR = 17
DAILY_ALIGNMENT_TIMEZONE = "America/New_York"

STOP_BUFFER_TICKS = 10
BACKTEST_SLIPPAGE_TICKS = 5

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

H1_WARMUP_DAYS = 220
DAILY_WARMUP_DAYS = 2600

OUTPUT_FILE = "usdjpy_short_feature_regime_discovery.csv"


# ============================================================
# CORE STRUCTURAL VARIANTS
# ============================================================

# These deliberately include:
#   - the closest four-era near-miss
#   - the stronger high-RR variant
#   - a balanced middle core
#   - relaxed-body / relaxed-structure versions
#   - an almost-unstructured core
#
# structure_lookback=None means the structure filter is OFF.

CORES = [
    {
        "core_name": "NEAR_MISS",
        "body_ratio": 1.50,
        "structure_lookback": 90,
        "max_distance_atr": 0.300,
        "slow_ema": 90,
        "reward_risk": 2.75,
    },
    {
        "core_name": "HIGH_RR",
        "body_ratio": 1.50,
        "structure_lookback": 80,
        "max_distance_atr": 0.325,
        "slow_ema": 100,
        "reward_risk": 3.50,
    },
    {
        "core_name": "BALANCED",
        "body_ratio": 1.40,
        "structure_lookback": 85,
        "max_distance_atr": 0.300,
        "slow_ema": 100,
        "reward_risk": 3.25,
    },
    {
        "core_name": "RELAXED_BODY",
        "body_ratio": 1.25,
        "structure_lookback": 85,
        "max_distance_atr": 0.300,
        "slow_ema": 100,
        "reward_risk": 3.25,
    },
    {
        "core_name": "RELAXED_STRUCTURE",
        "body_ratio": 1.40,
        "structure_lookback": 50,
        "max_distance_atr": 0.400,
        "slow_ema": 100,
        "reward_risk": 3.25,
    },
    {
        "core_name": "STRUCTURE_OFF",
        "body_ratio": 1.40,
        "structure_lookback": None,
        "max_distance_atr": None,
        "slow_ema": 100,
        "reward_risk": 3.25,
    },
]


# ============================================================
# FEATURE GRID
# ============================================================

# Close location for a bearish candle:
#   (close - low) / (high - low)
# Smaller = stronger bearish close.

MAX_CLOSE_LOCATION = [
    None,
    0.40,
    0.35,
    0.30,
]

# Fast EMA condition is:
#   fast EMA < core slow EMA

FAST_EMAS = [
    None,
    40,
    60,
    80,
]

# Normalized slow-EMA slope:
#   (EMA_now - EMA_5_daily_bars_ago) / DailyATR14
# For shorts, <= threshold.

MAX_SLOW_EMA_SLOPE_5D_ATR = [
    None,
    0.00,
    -0.03,
    -0.05,
]

# Daily ATR14 / 50-day mean of Daily ATR14
# Minimum ratio.

MIN_DAILY_ATR_RATIO_50 = [
    None,
    0.75,
    0.85,
    1.00,
]

# Signal H1 range / H1 ATR14
MIN_SIGNAL_RANGE_ATR = [
    None,
    0.75,
    1.00,
]

# Signal upper wick / signal body
MIN_UPPER_WICK_BODY = [
    None,
    0.10,
    0.25,
]

# Prior 5-bar bullish move:
#   (signal open - close 5 bars ago) / H1 ATR14
MIN_PRIOR_5BAR_UPMOVE_ATR = [
    None,
    0.50,
    1.00,
]


TOTAL_FEATURE_COMBINATIONS_PER_CORE = (
    len(MAX_CLOSE_LOCATION)
    * len(FAST_EMAS)
    * len(MAX_SLOW_EMA_SLOPE_5D_ATR)
    * len(MIN_DAILY_ATR_RATIO_50)
    * len(MIN_SIGNAL_RANGE_ATR)
    * len(MIN_UPPER_WICK_BODY)
    * len(MIN_PRIOR_5BAR_UPMOVE_ATR)
)

TOTAL_COMBINATIONS = (
    len(CORES)
    * TOTAL_FEATURE_COMBINATIONS_PER_CORE
)


# ============================================================
# INDICATOR LENGTHS
# ============================================================

ALL_DAILY_EMAS = sorted(
    {
        core["slow_ema"]
        for core in CORES
    }
    | {
        fast
        for fast in FAST_EMAS
        if fast is not None
    }
)

STRUCTURE_LOOKBACKS = sorted(
    {
        core["structure_lookback"]
        for core in CORES
        if core["structure_lookback"] is not None
    }
)

MAX_STRUCTURE_LOOKBACK = max(
    STRUCTURE_LOOKBACKS
)


# ============================================================
# ERAS
# ============================================================

ERAS = [
    (
        "2002_2009",
        datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc),
        datetime(2010, 1, 1, 0, 0, tzinfo=timezone.utc),
    ),
    (
        "2010_2017",
        datetime(2010, 1, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2018, 1, 1, 0, 0, tzinfo=timezone.utc),
    ),
    (
        "2018_2023",
        datetime(2018, 1, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
    ),
    (
        "2024_present",
        datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
        None,
    ),
]


# ============================================================
# STATUS
# ============================================================

STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "service": "USDJPY Short Feature Regime Discovery",
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "core_count": len(CORES),
    "feature_combinations_per_core": TOTAL_FEATURE_COMBINATIONS_PER_CORE,
    "total_combinations": TOTAL_COMBINATIONS,
    "completed_combinations": 0,
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
            candles_by_time[
                candle["time"]
            ] = candle

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

def ema_series(values, length):
    result = [None] * len(values)

    if len(values) < length:
        return result

    initial = (
        sum(values[:length])
        / length
    )

    result[length - 1] = initial

    multiplier = (
        2.0 / (length + 1.0)
    )

    previous = initial

    for index in range(
        length,
        len(values),
    ):
        current = (
            (
                values[index]
                - previous
            )
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
                candle["high"]
                - candle["low"],
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

    for index in range(
        length,
        len(values),
    ):
        current = (
            (
                previous
                * (length - 1)
            )
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


def rolling_mean(
    values,
    length,
):
    result = [None] * len(values)
    running = 0.0
    valid_count = 0
    window = []

    for index, value in enumerate(values):
        window.append(value)

        if value is not None:
            running += value
            valid_count += 1

        if len(window) > length:
            removed = window.pop(0)

            if removed is not None:
                running -= removed
                valid_count -= 1

        if (
            len(window) == length
            and valid_count == length
        ):
            result[index] = (
                running / length
            )

    return result


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

    ema_map = {
        length: ema_series(
            closes,
            length,
        )
        for length in ALL_DAILY_EMAS
    }

    daily_atr = atr_series(
        daily,
        14,
    )

    daily_atr_mean_50 = rolling_mean(
        daily_atr,
        50,
    )

    return (
        ema_map,
        daily_atr,
        daily_atr_mean_50,
    )


def build_h1_daily_lookup(
    h1,
    daily,
    daily_ema_map,
    daily_atr,
    daily_atr_mean_50,
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

        atr_now = daily_atr[
            daily_index
        ]

        atr_mean_50 = (
            daily_atr_mean_50[
                daily_index
            ]
        )

        atr_ratio_50 = None

        if (
            atr_now is not None
            and atr_mean_50 is not None
            and atr_mean_50 > 0
        ):
            atr_ratio_50 = (
                atr_now
                / atr_mean_50
            )

        lookup[h1_index] = {
            "daily_index": daily_index,
            "close": daily[
                daily_index
            ]["close"],
            "daily_atr14": atr_now,
            "daily_atr_ratio_50": (
                atr_ratio_50
            ),
            "emas": {
                length:
                daily_ema_map[
                    length
                ][daily_index]
                for length
                in ALL_DAILY_EMAS
            },
        }

    return lookup


# ============================================================
# SIGNAL FEATURE MATRIX
# ============================================================

def build_candidates(
    h1,
    h1_atr,
    daily_lookup,
    daily_ema_map,
):
    candidates = []

    for index in range(
        max(
            MAX_STRUCTURE_LOOKBACK,
            5,
        ),
        len(h1),
    ):
        signal = h1[index]

        if signal["time"] < RESEARCH_FROM:
            continue

        if signal["time"] >= RESEARCH_TO:
            break

        previous = h1[index - 1]
        atr = h1_atr[index]
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

        signal_range = (
            signal["high"]
            - signal["low"]
        )

        if (
            previous_body <= 0
            or current_body <= 0
            or signal_range <= 0
        ):
            continue

        bearish_engulfing = (
            previous["close"]
            > previous["open"]
            and signal["close"]
            < signal["open"]
            and signal["open"]
            >= previous["close"]
            and signal["close"]
            <= previous["open"]
        )

        if not bearish_engulfing:
            continue

        structure_distances = {}

        for lookback in STRUCTURE_LOOKBACKS:
            previous_highest = max(
                candle["high"]
                for candle in h1[
                    index - lookback:index
                ]
            )

            structure_distances[
                lookback
            ] = (
                previous_highest
                - signal["high"]
            ) / atr

        upper_wick = (
            signal["high"]
            - max(
                signal["open"],
                signal["close"],
            )
        )

        close_location = (
            signal["close"]
            - signal["low"]
        ) / signal_range

        prior_5bar_upmove_atr = (
            signal["open"]
            - h1[index - 5]["close"]
        ) / atr

        daily_index = (
            daily["daily_index"]
        )

        slow_slopes = {}

        for slow_ema in {
            core["slow_ema"]
            for core in CORES
        }:
            ema_now = daily_ema_map[
                slow_ema
            ][daily_index]

            ema_5ago = None

            if daily_index >= 5:
                ema_5ago = daily_ema_map[
                    slow_ema
                ][daily_index - 5]

            slope = None

            if (
                ema_now is not None
                and ema_5ago is not None
                and daily["daily_atr14"] is not None
                and daily["daily_atr14"] > 0
            ):
                slope = (
                    ema_now
                    - ema_5ago
                ) / daily[
                    "daily_atr14"
                ]

            slow_slopes[
                slow_ema
            ] = slope

        candidates.append({
            "index": index,
            "time": signal["time"],
            "body_ratio": (
                current_body
                / previous_body
            ),
            "signal_range_atr": (
                signal_range / atr
            ),
            "upper_wick_body": (
                upper_wick
                / current_body
            ),
            "close_location": (
                close_location
            ),
            "prior_5bar_upmove_atr": (
                prior_5bar_upmove_atr
            ),
            "structure_distances": (
                structure_distances
            ),
            "daily": daily,
            "slow_slopes": (
                slow_slopes
            ),
        })

    return candidates


# ============================================================
# FILTER
# ============================================================

def candidate_allowed(
    candidate,
    core,
    maximum_close_location,
    fast_ema,
    maximum_slow_ema_slope_5d_atr,
    minimum_daily_atr_ratio_50,
    minimum_signal_range_atr,
    minimum_upper_wick_body,
    minimum_prior_5bar_upmove_atr,
):
    if (
        candidate["body_ratio"]
        < core["body_ratio"]
    ):
        return False

    structure_lookback = (
        core["structure_lookback"]
    )

    if structure_lookback is not None:
        if (
            candidate[
                "structure_distances"
            ][structure_lookback]
            > core[
                "max_distance_atr"
            ]
        ):
            return False

    daily = candidate["daily"]

    slow_ema = core[
        "slow_ema"
    ]

    slow_value = daily[
        "emas"
    ].get(
        slow_ema
    )

    if slow_value is None:
        return False

    if not (
        daily["close"]
        < slow_value
    ):
        return False

    if (
        maximum_close_location
        is not None
        and candidate[
            "close_location"
        ] > maximum_close_location
    ):
        return False

    if fast_ema is not None:
        fast_value = daily[
            "emas"
        ].get(
            fast_ema
        )

        if (
            fast_value is None
            or not (
                fast_value
                < slow_value
            )
        ):
            return False

    if (
        maximum_slow_ema_slope_5d_atr
        is not None
    ):
        slope = candidate[
            "slow_slopes"
        ].get(
            slow_ema
        )

        if (
            slope is None
            or slope
            > maximum_slow_ema_slope_5d_atr
        ):
            return False

    if (
        minimum_daily_atr_ratio_50
        is not None
    ):
        atr_ratio = daily[
            "daily_atr_ratio_50"
        ]

        if (
            atr_ratio is None
            or atr_ratio
            < minimum_daily_atr_ratio_50
        ):
            return False

    if (
        minimum_signal_range_atr
        is not None
        and candidate[
            "signal_range_atr"
        ] < minimum_signal_range_atr
    ):
        return False

    if (
        minimum_upper_wick_body
        is not None
        and candidate[
            "upper_wick_body"
        ] < minimum_upper_wick_body
    ):
        return False

    if (
        minimum_prior_5bar_upmove_atr
        is not None
        and candidate[
            "prior_5bar_upmove_atr"
        ] < minimum_prior_5bar_upmove_atr
    ):
        return False

    return True


# ============================================================
# EXIT SIMULATION
# ============================================================

EXIT_CACHE = {}


def calculate_trade_exit(
    h1,
    signal_index,
    reward_risk,
):
    cache_key = (
        signal_index,
        reward_risk,
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
        * reward_risk
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

        if candle["time"] >= RESEARCH_TO:
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

        if stop_hit and target_hit:
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

        result = {
            "status": "CLOSED",
            "signal_index": signal_index,
            "signal_time": signal["time"],
            "exit_index": index,
            "exit_time": candle["time"],
            "exit_reason": exit_reason,
            "result_r": (
                backtest_entry
                - exit_price
            ) / actual_risk,
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
    reward_risk,
):
    trades = []
    position_exit_index = -1
    ignored = 0
    still_open = False

    for candidate in candidates:
        signal_index = (
            candidate["index"]
        )

        if (
            signal_index
            < position_exit_index
        ):
            ignored += 1
            continue

        trade = calculate_trade_exit(
            h1,
            signal_index,
            reward_risk,
        )

        if trade["status"] == "OPEN":
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
        result
        for result in results
        if result > 0
    ]

    losers = [
        result
        for result in results
        if result < 0
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
        profit_factor = (
            gross_profit
            / gross_loss
        )

    elif gross_profit > 0:
        profit_factor = 999.0

    else:
        profit_factor = 0.0

    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    current_streak = 0
    longest_streak = 0

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
            profit_factor,
            3,
        ),
        "total_r": round(
            total_r,
            2,
        ),
        "expectancy_r": round(
            total_r
            / len(results),
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
# RESULT ROW
# ============================================================

def make_result_row(
    core,
    maximum_close_location,
    fast_ema,
    maximum_slow_ema_slope_5d_atr,
    minimum_daily_atr_ratio_50,
    minimum_signal_range_atr,
    minimum_upper_wick_body,
    minimum_prior_5bar_upmove_atr,
    eligible,
    trades,
    ignored,
    still_open,
    years,
):
    full = stats_for_trades(
        trades
    )

    active_feature_count = sum(
        value is not None
        for value in [
            maximum_close_location,
            fast_ema,
            maximum_slow_ema_slope_5d_atr,
            minimum_daily_atr_ratio_50,
            minimum_signal_range_atr,
            minimum_upper_wick_body,
            minimum_prior_5bar_upmove_atr,
        ]
    )

    row = {
        "core_name": (
            core["core_name"]
        ),
        "core_body_ratio": (
            core["body_ratio"]
        ),
        "core_structure_lookback": (
            core["structure_lookback"]
        ),
        "core_max_distance_atr": (
            core["max_distance_atr"]
        ),
        "core_slow_ema": (
            core["slow_ema"]
        ),
        "reward_risk": (
            core["reward_risk"]
        ),
        "maximum_close_location": (
            maximum_close_location
        ),
        "fast_ema": fast_ema,
        "maximum_slow_ema_slope_5d_atr": (
            maximum_slow_ema_slope_5d_atr
        ),
        "minimum_daily_atr_ratio_50": (
            minimum_daily_atr_ratio_50
        ),
        "minimum_signal_range_atr": (
            minimum_signal_range_atr
        ),
        "minimum_upper_wick_body": (
            minimum_upper_wick_body
        ),
        "minimum_prior_5bar_upmove_atr": (
            minimum_prior_5bar_upmove_atr
        ),
        "active_feature_count": (
            active_feature_count
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
        "trades": (
            full["trades"]
        ),
        "trades_per_year": round(
            full["trades"]
            / years,
            2,
        ),
        "winners": (
            full["winners"]
        ),
        "losers": (
            full["losers"]
        ),
        "win_rate": (
            full["win_rate"]
        ),
        "profit_factor": (
            full["profit_factor"]
        ),
        "total_r": (
            full["total_r"]
        ),
        "expectancy_r": (
            full["expectancy_r"]
        ),
        "max_drawdown_r": (
            full["max_drawdown_r"]
        ),
        "longest_loss_streak": (
            full[
                "longest_loss_streak"
            ]
        ),
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

        if era["total_r"] > 0:
            profitable_eras += 1

        if era["trades"] >= 5:
            eras_with_5_plus += 1

            if era["total_r"] > 0:
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
    ] = profitable_eras_with_5_plus

    row[
        "minimum_era_pf_5_plus"
    ] = minimum_era_pf_5_plus

    row[
        "minimum_era_expectancy_5_plus"
    ] = minimum_era_expectancy_5_plus

    return row


# ============================================================
# RESEARCH
# ============================================================

def run_research():
    global STATUS

    try:
        print()
        print("=" * 78)
        print(
            "USD/JPY SHORT - FEATURE / REGIME DISCOVERY"
        )
        print("=" * 78)
        print(
            "Cores:",
            len(CORES),
        )
        print(
            "Feature combinations per core:",
            TOTAL_FEATURE_COMBINATIONS_PER_CORE,
        )
        print(
            "Total combinations:",
            TOTAL_COMBINATIONS,
        )
        print(
            "NO TIMING / WEEKDAY FILTERS"
        )
        print()

        STATUS.update({
            "state": "fetching_data",
            "message": (
                "Fetching USD/JPY OANDA history"
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
                "No USD/JPY H1 candles returned"
            )

        if not daily:
            raise RuntimeError(
                "No USD/JPY daily candles returned"
            )

        STATUS.update({
            "state": "precomputing",
            "message": (
                "Building indicators and feature matrix"
            ),
        })

        h1_atr = atr_series(
            h1,
            14,
        )

        (
            daily_ema_map,
            daily_atr,
            daily_atr_mean_50,
        ) = build_daily_state(
            daily
        )

        daily_lookup = (
            build_h1_daily_lookup(
                h1,
                daily,
                daily_ema_map,
                daily_atr,
                daily_atr_mean_50,
            )
        )

        candidates = build_candidates(
            h1,
            h1_atr,
            daily_lookup,
            daily_ema_map,
        )

        STATUS[
            "base_bearish_engulfings"
        ] = len(candidates)

        print(
            "Base bearish engulfings:",
            len(candidates),
        )

        years = (
            RESEARCH_TO
            - RESEARCH_FROM
        ).total_seconds() / (
            365.2425
            * 24
            * 60
            * 60
        )

        rows = []
        completed = 0

        STATUS.update({
            "state": "running",
            "message": (
                "Running USD/JPY feature/regime sweep"
            ),
        })

        feature_grid = list(
            itertools.product(
                MAX_CLOSE_LOCATION,
                FAST_EMAS,
                MAX_SLOW_EMA_SLOPE_5D_ATR,
                MIN_DAILY_ATR_RATIO_50,
                MIN_SIGNAL_RANGE_ATR,
                MIN_UPPER_WICK_BODY,
                MIN_PRIOR_5BAR_UPMOVE_ATR,
            )
        )

        for core in CORES:
            print()
            print(
                "Running core:",
                core["core_name"],
                flush=True,
            )

            # Skip impossible fast EMA combinations where
            # fast >= slow; they do not make sense as a
            # fast-below-slow trend-alignment condition.
            for (
                maximum_close_location,
                fast_ema,
                maximum_slow_ema_slope_5d_atr,
                minimum_daily_atr_ratio_50,
                minimum_signal_range_atr,
                minimum_upper_wick_body,
                minimum_prior_5bar_upmove_atr,
            ) in feature_grid:

                completed += 1
                STATUS[
                    "completed_combinations"
                ] = completed

                if (
                    fast_ema is not None
                    and fast_ema
                    >= core["slow_ema"]
                ):
                    continue

                eligible = [
                    candidate
                    for candidate in candidates
                    if candidate_allowed(
                        candidate,
                        core,
                        maximum_close_location,
                        fast_ema,
                        maximum_slow_ema_slope_5d_atr,
                        minimum_daily_atr_ratio_50,
                        minimum_signal_range_atr,
                        minimum_upper_wick_body,
                        minimum_prior_5bar_upmove_atr,
                    )
                ]

                (
                    trades,
                    ignored,
                    still_open,
                ) = simulate(
                    h1,
                    eligible,
                    core[
                        "reward_risk"
                    ],
                )

                rows.append(
                    make_result_row(
                        core,
                        maximum_close_location,
                        fast_ema,
                        maximum_slow_ema_slope_5d_atr,
                        minimum_daily_atr_ratio_50,
                        minimum_signal_range_atr,
                        minimum_upper_wick_body,
                        minimum_prior_5bar_upmove_atr,
                        eligible,
                        trades,
                        ignored,
                        still_open,
                        years,
                    )
                )

                if (
                    completed % 500
                    == 0
                ):
                    print(
                        f"Progress: "
                        f"{completed}/"
                        f"{TOTAL_COMBINATIONS}",
                        flush=True,
                    )

        df = pd.DataFrame(
            rows
        )

        if df.empty:
            raise RuntimeError(
                "No USD/JPY feature rows generated"
            )

        df[
            "adequate_60"
        ] = (
            df["trades"]
            >= 60
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
            "all_four_eras_profitable"
        ] = (
            df[
                "profitable_eras_with_5_plus_trades"
            ]
            >= 4
        )

        df[
            "robust_era_pf_120"
        ] = (
            df[
                "minimum_era_pf_5_plus"
            ].fillna(0)
            >= 1.20
        )

        df[
            "robust_era_pf_150"
        ] = (
            df[
                "minimum_era_pf_5_plus"
            ].fillna(0)
            >= 1.50
        )

        df[
            "annual_r_linear"
        ] = (
            df[
                "expectancy_r"
            ]
            * df[
                "trades_per_year"
            ]
        )

        df = df.sort_values(
            by=[
                "all_four_eras_profitable",
                "adequate_80",
                "robust_era_pf_120",
                "minimum_era_pf_5_plus",
                "profit_factor",
                "annual_r_linear",
                "trades_per_year",
                "active_feature_count",
            ],
            ascending=[
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                True,
            ],
        )

        df.to_csv(
            OUTPUT_FILE,
            index=False,
        )

        all_era_count = int(
            df[
                "all_four_eras_profitable"
            ].sum()
        )

        robust_120_count = int(
            (
                df[
                    "all_four_eras_profitable"
                ]
                & df[
                    "robust_era_pf_120"
                ]
            ).sum()
        )

        robust_150_count = int(
            (
                df[
                    "all_four_eras_profitable"
                ]
                & df[
                    "robust_era_pf_150"
                ]
            ).sum()
        )

        STATUS.update({
            "state": "complete",
            "message": (
                "USD/JPY feature/regime discovery "
                "completed successfully"
            ),
            "completed_combinations": (
                TOTAL_COMBINATIONS
            ),
            "rows_saved": len(df),
            "all_four_eras_profitable": (
                all_era_count
            ),
            "all_four_eras_and_worst_pf_120": (
                robust_120_count
            ),
            "all_four_eras_and_worst_pf_150": (
                robust_150_count
            ),
            "output_file": OUTPUT_FILE,
            "earliest_h1": (
                h1[0]["time"].isoformat()
            ),
            "latest_h1": (
                h1[-1]["time"].isoformat()
            ),
        })

        print()
        print("=" * 78)
        print(
            "USD/JPY FEATURE / REGIME DISCOVERY COMPLETE"
        )
        print("=" * 78)
        print(
            "Rows saved:",
            len(df),
        )
        print(
            "All four eras profitable:",
            all_era_count,
        )
        print(
            "All four + worst era PF >= 1.20:",
            robust_120_count,
        )
        print(
            "All four + worst era PF >= 1.50:",
            robust_150_count,
        )
        print(
            "Saved:",
            OUTPUT_FILE,
        )
        print()

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
            "USDJPY Short Feature Regime Discovery"
        ),
        "status": STATUS,
        "instrument": INSTRUMENT,
        "direction": "SHORT",
        "timing_filters": (
            "NONE - all hours and weekdays"
        ),
        "cores": CORES,
        "feature_grid": {
            "maximum_close_location": (
                MAX_CLOSE_LOCATION
            ),
            "fast_emas": FAST_EMAS,
            "maximum_slow_ema_slope_5d_atr": (
                MAX_SLOW_EMA_SLOPE_5D_ATR
            ),
            "minimum_daily_atr_ratio_50": (
                MIN_DAILY_ATR_RATIO_50
            ),
            "minimum_signal_range_atr": (
                MIN_SIGNAL_RANGE_ATR
            ),
            "minimum_upper_wick_body": (
                MIN_UPPER_WICK_BODY
            ),
            "minimum_prior_5bar_upmove_atr": (
                MIN_PRIOR_5BAR_UPMOVE_ATR
            ),
            "feature_combinations_per_core": (
                TOTAL_FEATURE_COMBINATIONS_PER_CORE
            ),
            "total_combinations": (
                TOTAL_COMBINATIONS
            ),
        },
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
            "message": (
                "USD/JPY feature/regime CSV "
                "is not ready yet"
            ),
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
            "usdjpy-short-feature-regime-discovery"
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
