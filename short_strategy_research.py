import os
import itertools
import threading
import math
import requests
import pandas as pd

from flask import Flask, send_file, jsonify
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ==================================================
# FLASK
# ==================================================

app = Flask(__name__)


# ==================================================
# CONFIG
# ==================================================

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_URL = "https://api-fxtrade.oanda.com"

INSTRUMENT = "EUR_USD"

TICK_SIZE = 0.00001

NY_TZ = ZoneInfo("America/New_York")

DAILY_ALIGNMENT_HOUR = 17
DAILY_ALIGNMENT_TIMEZONE = "America/New_York"

STOP_BUFFER_TICKS = 10
BACKTEST_SLIPPAGE_TICKS = 5

H1_CHUNK_DAYS = 180

RESEARCH_FROM = datetime(
    2002, 5, 6, 20, 0,
    tzinfo=timezone.utc
)

RESEARCH_TO = (
    datetime.now(timezone.utc)
    .replace(
        minute=0,
        second=0,
        microsecond=0
    )
)

H1_WARMUP_DAYS = 60
DAILY_WARMUP_DAYS = 1500

OUTPUT_FILE = (
    "eurusd_short_final_structural_refinement.csv"
)


# ==================================================
# FINAL STRUCTURAL REFINEMENT GRID
# ==================================================

BODY_RATIOS = [
    1.20,
    1.25,
    1.30,
    1.35,
    1.40
]

STRUCTURE_LOOKBACKS = [
    40,
    45,
    50,
    55,
    60,
    65
]

MAX_DISTANCE_ATR_VALUES = [
    0.15,
    0.20,
    0.25,
    0.30,
    0.35
]

REWARD_RISKS = [
    3.75,
    4.00,
    4.25
]

SLOW_EMA_LENGTHS = [
    75,
    100,
    125,
    150
]

FAST_EMA_LENGTHS = [
    50,
    60,
    70,
    80,
    90,
    100
]

STRONG_CLOSE_LEVELS = [
    0.20,
    0.225,
    0.25,
    0.275,
    0.30
]


# ==================================================
# TOTAL COMBINATIONS
#
# 5 × 6 × 5 × 3 × 4 × 6 × 5
# = 54,000
# ==================================================

TOTAL_COMBINATIONS = (
    len(BODY_RATIOS)
    * len(STRUCTURE_LOOKBACKS)
    * len(MAX_DISTANCE_ATR_VALUES)
    * len(REWARD_RISKS)
    * len(SLOW_EMA_LENGTHS)
    * len(FAST_EMA_LENGTHS)
    * len(STRONG_CLOSE_LEVELS)
)


# ==================================================
# STATUS
# ==================================================

RESEARCH_STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "total_combinations": TOTAL_COMBINATIONS,
    "completed_combinations": 0,
    "rows_saved": 0,
    "base_signal_candidates": 0,
    "parity_test": "not_started",
    "parity_cases_completed": 0
}


# ==================================================
# OANDA
# ==================================================

def headers():

    if not OANDA_TOKEN:
        raise RuntimeError(
            "OANDA_TOKEN is not configured"
        )

    return {
        "Authorization":
            f"Bearer {OANDA_TOKEN}"
    }


def iso_utc(dt):

    return (
        dt.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def oanda_get(
    path,
    params
):

    response = requests.get(
        OANDA_URL + path,
        headers=headers(),
        params=params,
        timeout=30
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
        "time":
            datetime.fromisoformat(
                raw["time"].replace(
                    "Z",
                    "+00:00"
                )
            ),
        "open":
            float(mid["o"]),
        "high":
            float(mid["h"]),
        "low":
            float(mid["l"]),
        "close":
            float(mid["c"])
    }


def fetch_range(
    instrument,
    granularity,
    start,
    end
):

    params = {
        "price":
            "M",

        "granularity":
            granularity,

        "from":
            iso_utc(start),

        "to":
            iso_utc(end),

        "smooth":
            "false",

        "includeFirst":
            "true",

        "dailyAlignment":
            DAILY_ALIGNMENT_HOUR,

        "alignmentTimezone":
            DAILY_ALIGNMENT_TIMEZONE
    }

    data = oanda_get(
        f"/v3/instruments/"
        f"{instrument}/candles",
        params
    )

    candles = []

    for raw in data.get(
        "candles",
        []
    ):

        candle = parse_candle(raw)

        if candle is not None:
            candles.append(candle)

    return candles


def fetch_chunked_history(
    instrument,
    granularity,
    start,
    end
):

    candles_by_time = {}

    cursor = start

    while cursor < end:

        chunk_end = min(
            cursor
            + timedelta(
                days=H1_CHUNK_DAYS
            ),
            end
        )

        print(
            f"Fetching {granularity}: "
            f"{cursor.date()} -> "
            f"{chunk_end.date()}",
            flush=True
        )

        chunk = fetch_range(
            instrument,
            granularity,
            cursor,
            chunk_end
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
        key=lambda item:
            item["time"]
    )

    return candles


# ==================================================
# INDICATORS
# ==================================================

def ema_series(
    values,
    length
):

    result = [
        None
    ] * len(values)

    if len(values) < length:
        return result

    initial = (
        sum(values[:length])
        / length
    )

    result[
        length - 1
    ] = initial

    multiplier = (
        2.0
        / (
            length + 1.0
        )
    )

    previous = initial

    for index in range(
        length,
        len(values)
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


def true_ranges(
    candles
):

    result = []

    for index, candle in enumerate(
        candles
    ):

        if index == 0:

            value = (
                candle["high"]
                - candle["low"]
            )

        else:

            previous_close = (
                candles[
                    index - 1
                ]["close"]
            )

            value = max(
                candle["high"]
                - candle["low"],

                abs(
                    candle["high"]
                    - previous_close
                ),

                abs(
                    candle["low"]
                    - previous_close
                )
            )

        result.append(value)

    return result


def rma_series(
    values,
    length
):

    result = [
        None
    ] * len(values)

    if len(values) < length:
        return result

    initial = (
        sum(values[:length])
        / length
    )

    result[
        length - 1
    ] = initial

    previous = initial

    for index in range(
        length,
        len(values)
    ):

        current = (
            (
                previous
                * (
                    length - 1
                )
            )
            + values[index]
        ) / length

        result[index] = current
        previous = current

    return result


def atr_series(
    candles,
    length=14
):

    return rma_series(
        true_ranges(candles),
        length
    )


# ==================================================
# DAILY ALIGNMENT
# ==================================================

def current_daily_start(
    timestamp_utc
):

    ny_time = (
        timestamp_utc
        .astimezone(
            NY_TZ
        )
    )

    candidate = (
        ny_time.replace(
            hour=
                DAILY_ALIGNMENT_HOUR,
            minute=0,
            second=0,
            microsecond=0
        )
    )

    if ny_time < candidate:

        candidate -= timedelta(
            days=1
        )

    return candidate.astimezone(
        timezone.utc
    )


def build_daily_indicator_cache(
    daily
):

    closes = [
        candle["close"]
        for candle in daily
    ]

    lengths = sorted(
        set(
            SLOW_EMA_LENGTHS
            + FAST_EMA_LENGTHS
        )
    )

    cache = {}

    for length in lengths:

        cache[length] = ema_series(
            closes,
            length
        )

    return cache


def build_h1_daily_lookup(
    h1,
    daily,
    daily_ema_cache
):

    print(
        "Building H1 -> previous completed "
        "daily state lookup...",
        flush=True
    )

    lookup = [
        None
    ] * len(h1)

    daily_index = -1

    for h1_index, candle in enumerate(
        h1
    ):

        session_start = (
            current_daily_start(
                candle["time"]
            )
        )

        while (
            daily_index + 1
            < len(daily)
            and
            daily[
                daily_index + 1
            ]["time"]
            < session_start
        ):

            daily_index += 1

        if daily_index < 0:
            continue

        row = {
            "close":
                daily[
                    daily_index
                ]["close"]
        }

        for length, series in (
            daily_ema_cache.items()
        ):

            row[
                f"ema_{length}"
            ] = series[
                daily_index
            ]

        lookup[
            h1_index
        ] = row

    return lookup


# ==================================================
# PRECOMPUTED SIGNAL CANDIDATES
# ==================================================

def build_signal_candidates(
    h1,
    atr,
    daily_lookup
):

    print(
        "Precomputing bearish engulfing "
        "candidates...",
        flush=True
    )

    candidates = []

    minimum_index = max(
        STRUCTURE_LOOKBACKS
    )

    for index in range(
        minimum_index,
        len(h1)
    ):

        signal = h1[index]

        if signal["time"] < RESEARCH_FROM:
            continue

        if signal["time"] >= RESEARCH_TO:
            break

        previous = h1[
            index - 1
        ]

        current_atr = atr[index]

        if current_atr is None:
            continue

        daily = daily_lookup[index]

        if daily is None:
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

            and

            signal["close"]
            < signal["open"]

            and

            signal["open"]
            >= previous["close"]

            and

            signal["close"]
            <= previous["open"]
        )

        if not bearish_engulfing:
            continue

        body_ratio = (
            current_body
            / previous_body
        )

        close_location = (
            (
                signal["close"]
                - signal["low"]
            )
            / signal_range
        )

        structure_distances = {}

        for lookback in (
            STRUCTURE_LOOKBACKS
        ):

            previous_highest = max(
                candle["high"]
                for candle in h1[
                    index - lookback:
                    index
                ]
            )

            structure_distances[
                lookback
            ] = (
                (
                    previous_highest
                    - signal["high"]
                )
                / current_atr
            )

        candidates.append({
            "index":
                index,

            "time":
                signal["time"],

            "body_ratio":
                body_ratio,

            "close_location":
                close_location,

            "structure_distances":
                structure_distances,

            "daily":
                daily
        })

    return candidates


# ==================================================
# FAST CANDIDATE FILTER
# ==================================================

def fast_candidate_allowed(
    candidate,
    body_ratio,
    structure_lookback,
    max_distance_atr,
    slow_ema,
    fast_ema,
    strong_close
):

    if (
        candidate["body_ratio"]
        < body_ratio
    ):
        return False

    if (
        candidate[
            "structure_distances"
        ][structure_lookback]
        > max_distance_atr
    ):
        return False

    daily = candidate["daily"]

    slow_value = daily.get(
        f"ema_{slow_ema}"
    )

    fast_value = daily.get(
        f"ema_{fast_ema}"
    )

    if (
        slow_value is None
        or fast_value is None
    ):
        return False

    # Bearish regime
    if not (
        daily["close"]
        < slow_value
    ):
        return False

    # Bearish EMA alignment
    if not (
        fast_value
        < slow_value
    ):
        return False

    # Strong bearish close
    if (
        candidate["close_location"]
        > strong_close
    ):
        return False

    return True


# ==================================================
# EXIT CACHE
# ==================================================

EXIT_CACHE = {}


def calculate_trade_exit(
    h1,
    signal_index,
    reward_risk
):

    cache_key = (
        signal_index,
        reward_risk
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
        - (
            BACKTEST_SLIPPAGE_TICKS
            * TICK_SIZE
        )
    )

    stop = (
        signal["high"]
        + (
            STOP_BUFFER_TICKS
            * TICK_SIZE
        )
    )

    reference_risk = (
        stop
        - reference_entry
    )

    if reference_risk <= 0:

        EXIT_CACHE[
            cache_key
        ] = None

        return None

    target = (
        reference_entry
        - (
            reference_risk
            * reward_risk
        )
    )

    actual_risk = (
        stop
        - backtest_entry
    )

    if actual_risk <= 0:

        EXIT_CACHE[
            cache_key
        ] = None

        return None

    for index in range(
        signal_index + 1,
        len(h1)
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
            (
                backtest_entry
                - exit_price
            )
            / actual_risk
        )

        result = {
            "signal_index":
                signal_index,

            "exit_index":
                index,

            "signal_time":
                signal["time"],

            "exit_time":
                candle["time"],

            "exit_reason":
                exit_reason,

            "result_r":
                result_r
        }

        EXIT_CACHE[
            cache_key
        ] = result

        return result

    EXIT_CACHE[
        cache_key
    ] = None

    return None


# ==================================================
# FAST SIMULATOR
# ==================================================

def simulate_fast(
    h1,
    candidates,
    reward_risk
):

    trades = []

    position_exit_index = -1

    for candidate in candidates:

        signal_index = (
            candidate["index"]
        )

        # Same-candle re-entry is allowed after
        # an existing position exits.
        if (
            signal_index
            < position_exit_index
        ):
            continue

        trade = calculate_trade_exit(
            h1,
            signal_index,
            reward_risk
        )

        if trade is None:
            continue

        trades.append(
            trade
        )

        position_exit_index = (
            trade["exit_index"]
        )

    return trades


# ==================================================
# SLOW REFERENCE SIGNAL
# ==================================================

def slow_signal_allowed(
    h1,
    atr,
    daily_lookup,
    index,
    body_ratio,
    structure_lookback,
    max_distance_atr,
    slow_ema,
    fast_ema,
    strong_close
):

    if index < max(
        14,
        structure_lookback
    ):
        return False

    signal = h1[index]
    previous = h1[
        index - 1
    ]

    if signal["time"] < RESEARCH_FROM:
        return False

    if signal["time"] >= RESEARCH_TO:
        return False

    current_atr = atr[index]

    if current_atr is None:
        return False

    daily = daily_lookup[index]

    if daily is None:
        return False

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
        return False

    # Bearish engulf
    if not (
        previous["close"]
        > previous["open"]

        and

        signal["close"]
        < signal["open"]

        and

        signal["open"]
        >= previous["close"]

        and

        signal["close"]
        <= previous["open"]
    ):
        return False

    # Body ratio
    if not (
        current_body
        >= previous_body
        * body_ratio
    ):
        return False

    # Structure
    previous_highest = max(
        candle["high"]
        for candle in h1[
            index - structure_lookback:
            index
        ]
    )

    distance_from_high = (
        previous_highest
        - signal["high"]
    )

    if (
        distance_from_high
        > current_atr
        * max_distance_atr
    ):
        return False

    # Daily regime / alignment
    slow_value = daily.get(
        f"ema_{slow_ema}"
    )

    fast_value = daily.get(
        f"ema_{fast_ema}"
    )

    if (
        slow_value is None
        or fast_value is None
    ):
        return False

    if not (
        daily["close"]
        < slow_value
    ):
        return False

    if not (
        fast_value
        < slow_value
    ):
        return False

    # Strong bearish close
    close_location = (
        (
            signal["close"]
            - signal["low"]
        )
        / signal_range
    )

    if close_location > strong_close:
        return False

    return True


# ==================================================
# SLOW REFERENCE SIMULATOR
# ==================================================

def simulate_slow_reference(
    h1,
    atr,
    daily_lookup,
    body_ratio,
    structure_lookback,
    max_distance_atr,
    reward_risk,
    slow_ema,
    fast_ema,
    strong_close
):

    trades = []

    open_trade = None

    start_index = max(
        14,
        structure_lookback
    )

    for index in range(
        start_index,
        len(h1)
    ):

        candle = h1[index]
        candle_time = (
            candle["time"]
        )

        if candle_time < RESEARCH_FROM:
            continue

        if candle_time >= RESEARCH_TO:
            break

        # ==========================================
        # EXIT FIRST
        # ==========================================

        if open_trade is not None:

            stop_hit = (
                candle["high"]
                >= open_trade["stop"]
            )

            target_hit = (
                candle["low"]
                <= open_trade["target"]
            )

            if stop_hit or target_hit:

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

                        exit_price = (
                            open_trade["stop"]
                        )

                        exit_reason = "STOP"

                    else:

                        exit_price = (
                            open_trade["target"]
                        )

                        exit_reason = "TARGET"

                elif stop_hit:

                    exit_price = (
                        open_trade["stop"]
                    )

                    exit_reason = "STOP"

                else:

                    exit_price = (
                        open_trade["target"]
                    )

                    exit_reason = "TARGET"

                actual_risk = (
                    open_trade["stop"]
                    - open_trade[
                        "backtest_entry"
                    ]
                )

                result_r = (
                    (
                        open_trade[
                            "backtest_entry"
                        ]
                        - exit_price
                    )
                    / actual_risk
                )

                trades.append({
                    "signal_index":
                        open_trade[
                            "signal_index"
                        ],

                    "exit_index":
                        index,

                    "signal_time":
                        open_trade[
                            "signal_time"
                        ],

                    "exit_time":
                        candle_time,

                    "exit_reason":
                        exit_reason,

                    "result_r":
                        result_r
                })

                open_trade = None

        if open_trade is not None:
            continue

        # ==========================================
        # NEW SIGNAL
        # ==========================================

        if not slow_signal_allowed(
            h1,
            atr,
            daily_lookup,
            index,
            body_ratio,
            structure_lookback,
            max_distance_atr,
            slow_ema,
            fast_ema,
            strong_close
        ):
            continue

        signal = h1[index]

        reference_entry = (
            signal["close"]
        )

        backtest_entry = (
            reference_entry
            - (
                BACKTEST_SLIPPAGE_TICKS
                * TICK_SIZE
            )
        )

        stop = (
            signal["high"]
            + (
                STOP_BUFFER_TICKS
                * TICK_SIZE
            )
        )

        reference_risk = (
            stop
            - reference_entry
        )

        if reference_risk <= 0:
            continue

        target = (
            reference_entry
            - (
                reference_risk
                * reward_risk
            )
        )

        open_trade = {
            "signal_index":
                index,

            "signal_time":
                signal["time"],

            "backtest_entry":
                backtest_entry,

            "stop":
                stop,

            "target":
                target
        }

    return trades


# ==================================================
# PARITY CHECK
# ==================================================

def trades_match(
    slow_trades,
    fast_trades
):

    if (
        len(slow_trades)
        != len(fast_trades)
    ):

        return (
            False,
            (
                f"Trade count mismatch: "
                f"slow={len(slow_trades)}, "
                f"fast={len(fast_trades)}"
            )
        )

    for number, (
        slow_trade,
        fast_trade
    ) in enumerate(
        zip(
            slow_trades,
            fast_trades
        ),
        start=1
    ):

        for key in [
            "signal_index",
            "exit_index",
            "exit_reason"
        ]:

            if (
                slow_trade[key]
                != fast_trade[key]
            ):

                return (
                    False,
                    (
                        f"Trade {number}: "
                        f"{key} mismatch "
                        f"{slow_trade[key]} "
                        f"vs {fast_trade[key]}"
                    )
                )

        if not math.isclose(
            slow_trade["result_r"],
            fast_trade["result_r"],
            rel_tol=1e-12,
            abs_tol=1e-12
        ):

            return (
                False,
                (
                    f"Trade {number}: "
                    f"R mismatch "
                    f"{slow_trade['result_r']} "
                    f"vs "
                    f"{fast_trade['result_r']}"
                )
            )

    return (
        True,
        "Exact match"
    )


def run_parity_test(
    h1,
    atr,
    daily_lookup,
    all_candidates
):

    print()
    print(
        "========================================"
    )
    print(
        "RUNNING PARITY TEST"
    )
    print(
        "========================================"
    )

    RESEARCH_STATUS.update({
        "state":
            "parity_test",

        "message":
            "Checking fast engine against slow reference",

        "parity_test":
            "running",

        "parity_cases_completed":
            0
    })

    parity_cases = [

        {
            "body_ratio": 1.20,
            "structure_lookback": 40,
            "max_distance_atr": 0.15,
            "reward_risk": 3.75,
            "slow_ema": 75,
            "fast_ema": 50,
            "strong_close": 0.20
        },

        {
            "body_ratio": 1.30,
            "structure_lookback": 40,
            "max_distance_atr": 0.15,
            "reward_risk": 4.00,
            "slow_ema": 100,
            "fast_ema": 70,
            "strong_close": 0.25
        },

        {
            "body_ratio": 1.40,
            "structure_lookback": 65,
            "max_distance_atr": 0.35,
            "reward_risk": 4.25,
            "slow_ema": 150,
            "fast_ema": 100,
            "strong_close": 0.30
        },

        {
            "body_ratio": 1.25,
            "structure_lookback": 50,
            "max_distance_atr": 0.25,
            "reward_risk": 4.00,
            "slow_ema": 125,
            "fast_ema": 60,
            "strong_close": 0.225
        },

        {
            "body_ratio": 1.35,
            "structure_lookback": 55,
            "max_distance_atr": 0.20,
            "reward_risk": 4.25,
            "slow_ema": 100,
            "fast_ema": 80,
            "strong_close": 0.275
        },

        {
            "body_ratio": 1.20,
            "structure_lookback": 60,
            "max_distance_atr": 0.30,
            "reward_risk": 3.75,
            "slow_ema": 150,
            "fast_ema": 90,
            "strong_close": 0.30
        },

        {
            "body_ratio": 1.40,
            "structure_lookback": 45,
            "max_distance_atr": 0.15,
            "reward_risk": 4.00,
            "slow_ema": 75,
            "fast_ema": 50,
            "strong_close": 0.20
        },

        {
            "body_ratio": 1.30,
            "structure_lookback": 65,
            "max_distance_atr": 0.35,
            "reward_risk": 4.25,
            "slow_ema": 125,
            "fast_ema": 100,
            "strong_close": 0.25
        }
    ]

    for case_number, case in enumerate(
        parity_cases,
        start=1
    ):

        print(
            f"Parity case "
            f"{case_number}/"
            f"{len(parity_cases)}",
            flush=True
        )

        slow_trades = simulate_slow_reference(
            h1,
            atr,
            daily_lookup,
            case["body_ratio"],
            case["structure_lookback"],
            case["max_distance_atr"],
            case["reward_risk"],
            case["slow_ema"],
            case["fast_ema"],
            case["strong_close"]
        )

        eligible = [
            candidate
            for candidate in all_candidates
            if fast_candidate_allowed(
                candidate,
                case["body_ratio"],
                case["structure_lookback"],
                case["max_distance_atr"],
                case["slow_ema"],
                case["fast_ema"],
                case["strong_close"]
            )
        ]

        fast_trades = simulate_fast(
            h1,
            eligible,
            case["reward_risk"]
        )

        match, message = trades_match(
            slow_trades,
            fast_trades
        )

        print(
            f"Slow: {len(slow_trades)} | "
            f"Fast: {len(fast_trades)} | "
            f"{message}",
            flush=True
        )

        if not match:

            RESEARCH_STATUS.update({
                "parity_test":
                    "FAILED",

                "message":
                    (
                        f"Parity failed in "
                        f"case {case_number}: "
                        f"{message}"
                    )
            })

            raise RuntimeError(
                (
                    f"PARITY FAILED IN CASE "
                    f"{case_number}: "
                    f"{message}"
                )
            )

        RESEARCH_STATUS[
            "parity_cases_completed"
        ] = case_number

    RESEARCH_STATUS[
        "parity_test"
    ] = "PASSED"

    print()
    print(
        "PARITY TEST PASSED",
        flush=True
    )


# ==================================================
# PERFORMANCE
# ==================================================

def calculate_stats(
    trades
):

    if not trades:
        return None

    results = [
        trade["result_r"]
        for trade in trades
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

    total_r = sum(results)

    gross_profit = sum(
        winners
    )

    gross_loss = abs(
        sum(losers)
    )

    profit_factor = (
        gross_profit
        / gross_loss
        if gross_loss > 0
        else float("inf")
    )

    win_rate = (
        len(winners)
        / len(results)
        * 100
    )

    expectancy = (
        total_r
        / len(results)
    )

    # Drawdown
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0

    for result in results:

        equity += result

        peak = max(
            peak,
            equity
        )

        max_drawdown = min(
            max_drawdown,
            equity - peak
        )

    # Losing streak
    longest_loss_streak = 0
    current_loss_streak = 0

    for result in results:

        if result < 0:

            current_loss_streak += 1

            longest_loss_streak = max(
                longest_loss_streak,
                current_loss_streak
            )

        else:

            current_loss_streak = 0

    years = (
        (
            RESEARCH_TO
            - RESEARCH_FROM
        ).total_seconds()
        / (
            365.2425
            * 24
            * 60
            * 60
        )
    )

    trades_per_year = (
        len(results)
        / years
    )

    return {
        "trades":
            len(results),

        "trades_per_year":
            round(
                trades_per_year,
                2
            ),

        "winners":
            len(winners),

        "losers":
            len(losers),

        "win_rate":
            round(
                win_rate,
                2
            ),

        "profit_factor":
            round(
                profit_factor,
                3
            ),

        "total_r":
            round(
                total_r,
                2
            ),

        "expectancy_r":
            round(
                expectancy,
                3
            ),

        "max_drawdown_r":
            round(
                max_drawdown,
                2
            ),

        "longest_loss_streak":
            longest_loss_streak
    }


# ==================================================
# RESEARCH
# ==================================================

def run_research():

    global RESEARCH_STATUS

    try:

        print()
        print(
            "========================================"
        )
        print(
            "EUR/USD FINAL STRUCTURAL REFINEMENT"
        )
        print(
            "========================================"
        )
        print()

        print(
            "ALL HOURS ENABLED"
        )

        print(
            "ALL WEEKDAYS ENABLED"
        )

        print(
            "NO TIME/DAY FILTERS"
        )

        print(
            "NO MIN RANGE FILTER"
        )

        print(
            "NO UPPER-WICK FILTER"
        )

        print(
            "Total combinations:",
            TOTAL_COMBINATIONS
        )

        # ==========================================
        # DATA
        # ==========================================

        RESEARCH_STATUS.update({
            "state":
                "fetching_data",

            "message":
                "Fetching full EUR/USD history"
        })

        h1 = fetch_chunked_history(
            INSTRUMENT,
            "H1",
            RESEARCH_FROM
            - timedelta(
                days=H1_WARMUP_DAYS
            ),
            RESEARCH_TO
        )

        daily = fetch_chunked_history(
            INSTRUMENT,
            "D",
            RESEARCH_FROM
            - timedelta(
                days=DAILY_WARMUP_DAYS
            ),
            RESEARCH_TO
        )

        print()
        print(
            "H1 candles:",
            len(h1)
        )

        print(
            "Daily candles:",
            len(daily)
        )

        # ==========================================
        # PRECOMPUTE
        # ==========================================

        RESEARCH_STATUS.update({
            "state":
                "precomputing",

            "message":
                "Building ATR / EMA / signal cache"
        })

        atr = atr_series(
            h1,
            14
        )

        daily_ema_cache = (
            build_daily_indicator_cache(
                daily
            )
        )

        daily_lookup = (
            build_h1_daily_lookup(
                h1,
                daily,
                daily_ema_cache
            )
        )

        all_candidates = (
            build_signal_candidates(
                h1,
                atr,
                daily_lookup
            )
        )

        RESEARCH_STATUS[
            "base_signal_candidates"
        ] = len(
            all_candidates
        )

        print(
            "Base bearish engulfing candidates:",
            len(all_candidates)
        )

        # ==========================================
        # PARITY
        # ==========================================

        run_parity_test(
            h1,
            atr,
            daily_lookup,
            all_candidates
        )

        # ==========================================
        # GRID
        # ==========================================

        combinations = itertools.product(
            BODY_RATIOS,
            STRUCTURE_LOOKBACKS,
            MAX_DISTANCE_ATR_VALUES,
            REWARD_RISKS,
            SLOW_EMA_LENGTHS,
            FAST_EMA_LENGTHS,
            STRONG_CLOSE_LEVELS
        )

        RESEARCH_STATUS.update({
            "state":
                "running",

            "message":
                (
                    "Parity passed. "
                    "Running 54,000 combinations."
                ),

            "completed_combinations":
                0
        })

        results = []

        for number, combo in enumerate(
            combinations,
            start=1
        ):

            (
                body_ratio,
                structure_lookback,
                max_distance_atr,
                reward_risk,
                slow_ema,
                fast_ema,
                strong_close
            ) = combo

            eligible = [
                candidate
                for candidate
                in all_candidates
                if fast_candidate_allowed(
                    candidate,
                    body_ratio,
                    structure_lookback,
                    max_distance_atr,
                    slow_ema,
                    fast_ema,
                    strong_close
                )
            ]

            trades = simulate_fast(
                h1,
                eligible,
                reward_risk
            )

            stats = calculate_stats(
                trades
            )

            if stats is not None:

                row = {
                    "body_ratio":
                        body_ratio,

                    "structure_lookback":
                        structure_lookback,

                    "max_distance_atr":
                        max_distance_atr,

                    "reward_risk":
                        reward_risk,

                    "slow_ema":
                        slow_ema,

                    "fast_ema":
                        fast_ema,

                    "strong_bearish_close":
                        strong_close,

                    "raw_signals":
                        len(eligible)
                }

                row.update(stats)

                results.append(row)

            RESEARCH_STATUS[
                "completed_combinations"
            ] = number

            if (
                number % 1000
                == 0
            ):

                print(
                    f"Progress: "
                    f"{number}/"
                    f"{TOTAL_COMBINATIONS}",
                    flush=True
                )

        # ==========================================
        # SAVE
        # ==========================================

        df = pd.DataFrame(
            results
        )

        if df.empty:

            raise RuntimeError(
                "No results generated"
            )

        df = df.sort_values(
            by=[
                "profit_factor",
                "expectancy_r",
                "total_r",
                "trades"
            ],

            ascending=[
                False,
                False,
                False,
                False
            ]
        )

        df.to_csv(
            OUTPUT_FILE,
            index=False
        )

        RESEARCH_STATUS.update({
            "state":
                "complete",

            "message":
                (
                    "Final structural refinement "
                    "completed successfully."
                ),

            "completed_combinations":
                TOTAL_COMBINATIONS,

            "rows_saved":
                len(df),

            "output_file":
                OUTPUT_FILE,

            "parity_test":
                "PASSED"
        })

        # ==========================================
        # LOG
        # ==========================================

        print()
        print(
            "========================================"
        )
        print(
            "TOP >= 100 TRADES"
        )
        print(
            "========================================"
        )

        print(
            df[
                df["trades"]
                >= 100
            ]
            .head(30)
            .to_string(
                index=False
            )
        )

        print()
        print(
            "========================================"
        )
        print(
            "TOP >= 75 TRADES"
        )
        print(
            "========================================"
        )

        print(
            df[
                df["trades"]
                >= 75
            ]
            .head(30)
            .to_string(
                index=False
            )
        )

        print()
        print(
            "Saved:",
            OUTPUT_FILE,
            flush=True
        )

    except Exception as error:

        RESEARCH_STATUS.update({
            "state":
                "error",

            "message":
                str(error)
        })

        print(
            "ERROR:",
            error,
            flush=True
        )


# ==================================================
# ROUTES
# ==================================================

@app.route("/")
def home():

    return jsonify({
        "service":
            "EURUSD Final Structural Refinement",

        "status":
            RESEARCH_STATUS,

        "research":
            {
                "all_hours":
                    True,

                "all_weekdays":
                    True,

                "session_filter":
                    False,

                "weekday_filter":
                    False,

                "minimum_range_filter":
                    False,

                "upper_wick_filter":
                    False,

                "total_combinations":
                    TOTAL_COMBINATIONS,

                "parity_required":
                    True
            },

        "trading_enabled":
            False,

        "orders_supported":
            False,

        "executor_connected":
            False,

        "status_endpoint":
            "/status",

        "download_endpoint":
            "/download"
    })


@app.route("/status")
def status():

    return jsonify(
        RESEARCH_STATUS
    )


@app.route("/download")
def download():

    if not os.path.exists(
        OUTPUT_FILE
    ):

        return jsonify({
            "status":
                "not_ready",

            "message":
                "CSV has not been generated yet."
        }), 404

    return send_file(
        OUTPUT_FILE,
        as_attachment=True,

        download_name=
            "eurusd_short_final_structural_refinement.csv"
    )


# ==================================================
# START
# ==================================================

if __name__ == "__main__":

    research_thread = threading.Thread(
        target=run_research,
        name=
            "eurusd-final-refinement",
        daemon=True
    )

    research_thread.start()

    port = int(
        os.getenv(
            "PORT",
            5000
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False
    )
