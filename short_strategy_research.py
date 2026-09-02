import os
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from itertools import product


# ============================================================
# EUR/GBP SHORT
# ROBUST TRIGGER + HIGH-PF CONFIRMATION FREQUENCY MATRIX
#
# RESEARCH ONLY — NEVER SUBMITS ORDERS.
#
# PURPOSE
# ------------------------------------------------------------
# Keep the ROBUST strategy as the fixed primary trigger.
#
# Then vary ONLY the HIGH-PF confirmation layer to see whether
# we can increase frequency while retaining most of the quality
# of the fully CONFIRMED system.
#
# Two experiment families:
#
# 1) ALL_REQUIRED
#    Every confirmation condition must pass.
#    We sweep the thresholds of the four distinctive HIGH-PF
#    confirmation conditions.
#
# 2) SCORE
#    ROBUST must still pass.
#    Then require 2/4, 3/4, or 4/4 of the four HIGH-PF
#    confirmation conditions to pass.
#
# This means the ROBUST trigger itself NEVER changes.
#
# ============================================================
# FIXED ROBUST TRIGGER
# ------------------------------------------------------------
# bearish engulfing
# body ratio >= 1.00
# structure lookback = 90
# structure distance <= 0.15 ATR14
# range >= 1.10 ATR14
# close location <= 0.20
# 12h upward momentum >= 0.25 ATR14
# 48h upward momentum >= 0.40 ATR14
# stop size <= 2.50 ATR14
# exclude NY hour 09
#
# ============================================================
# HIGH-PF CONFIRMATION COMPONENTS
# ------------------------------------------------------------
# C1: tighter structure distance
# C2: stronger 48h upward momentum
# C3: upper wick/body
# C4: ATR14 / 50-bar ATR14 mean
#
# ALL_REQUIRED thresholds:
#   structure: 0.075, 0.10, 0.125, 0.15
#   momentum48: 0.70, 0.80, 0.90, 1.00
#   wick/body: 0.00, 0.05, 0.075, 0.10
#   ATR ratio: 0.00, 0.70, 0.75, 0.80
#
# SCORE thresholds:
#   structure: 0.075, 0.10, 0.125, 0.15
#   momentum48: 0.70, 0.80, 0.90, 1.00
#   wick/body: 0.05, 0.075, 0.10, 0.125
#   ATR ratio: 0.70, 0.75, 0.80, 0.90
#   required score: 2, 3, 4
#
# Current CONFIRMED model appears explicitly as:
#   ALL_REQUIRED
#   structure <= 0.075
#   momentum48 >= 1.00
#   wick/body >= 0.10
#   ATR ratio >= 0.80
#
# ============================================================
# EXECUTION
# ------------------------------------------------------------
# OANDA EUR_GBP midpoint H1
# RR = 3.00
# stop = signal high + 10 ticks
# adverse short slippage = 5 ticks
# pyramiding = 0
#
# Same-bar target/stop:
#   compare open->high vs open->low
#   high closer => stop first
#
# Locked position convention:
#   signal_index < position_exit_index => ignored
#   signal on exact H1 candle where previous trade exits allowed
#
# ============================================================
# OUTPUT
# ------------------------------------------------------------
# eurgbp_short_confirmation_frequency_matrix.csv
#
# Includes:
#   full-history stats
#   four-era stats
#   recent 2/5/10-year stats
#   worst rolling 3-year PF / expectancy / total R
#   frequency and quality flags
#   deltas versus current CONFIRMED strategy
# ============================================================


app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_URL = "https://api-fxtrade.oanda.com"

INSTRUMENT = "EUR_GBP"
TICK_SIZE = 0.00001

STOP_BUFFER_TICKS = 10
BACKTEST_SLIPPAGE_TICKS = 5
REWARD_RISK = 3.00

MIN_BODY_RATIO = 1.00
STRUCTURE_LOOKBACK = 90

NY_TZ = ZoneInfo("America/New_York")
EXCLUDED_NY_HOURS = {9}

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

H1_WARMUP_DAYS = 700

OUTPUT_FILE = (
    "eurgbp_short_confirmation_frequency_matrix.csv"
)


# ============================================================
# FIXED ROBUST TRIGGER
# ============================================================

ROBUST_MAX_DISTANCE_ATR = 0.15
ROBUST_MIN_RANGE_ATR = 1.10
ROBUST_MAX_CLOSE_LOCATION = 0.20
ROBUST_MIN_MOMENTUM_12 = 0.25
ROBUST_MIN_MOMENTUM_48 = 0.40
ROBUST_MAX_STOP_SIZE_ATR = 2.50


# ============================================================
# MATRIX GRIDS
# ============================================================

ALL_STRUCTURE = [
    0.075,
    0.10,
    0.125,
    0.15,
]

ALL_MOM48 = [
    0.70,
    0.80,
    0.90,
    1.00,
]

ALL_WICK = [
    0.00,
    0.05,
    0.075,
    0.10,
]

ALL_ATR_RATIO = [
    0.00,
    0.70,
    0.75,
    0.80,
]

SCORE_STRUCTURE = [
    0.075,
    0.10,
    0.125,
    0.15,
]

SCORE_MOM48 = [
    0.70,
    0.80,
    0.90,
    1.00,
]

SCORE_WICK = [
    0.05,
    0.075,
    0.10,
    0.125,
]

SCORE_ATR_RATIO = [
    0.70,
    0.75,
    0.80,
    0.90,
]

SCORE_REQUIRED = [
    2,
    3,
    4,
]


# ============================================================
# REFERENCE CONFIRMED MODEL
# ============================================================

REFERENCE = {
    "structure_threshold": 0.075,
    "momentum48_threshold": 1.00,
    "wick_threshold": 0.10,
    "atr_ratio_threshold": 0.80,
}


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
# MATRIX DEFINITIONS
# ============================================================

MATRIX_TESTS = []

for (
    structure,
    mom48,
    wick,
    atr_ratio,
) in product(
    ALL_STRUCTURE,
    ALL_MOM48,
    ALL_WICK,
    ALL_ATR_RATIO,
):
    MATRIX_TESTS.append({
        "family": "ALL_REQUIRED",
        "required_score": 4,
        "structure_threshold": structure,
        "momentum48_threshold": mom48,
        "wick_threshold": wick,
        "atr_ratio_threshold": atr_ratio,
    })

for (
    structure,
    mom48,
    wick,
    atr_ratio,
    required_score,
) in product(
    SCORE_STRUCTURE,
    SCORE_MOM48,
    SCORE_WICK,
    SCORE_ATR_RATIO,
    SCORE_REQUIRED,
):
    MATRIX_TESTS.append({
        "family": "SCORE",
        "required_score": required_score,
        "structure_threshold": structure,
        "momentum48_threshold": mom48,
        "wick_threshold": wick,
        "atr_ratio_threshold": atr_ratio,
    })


TOTAL_TESTS = len(
    MATRIX_TESTS
)


# ============================================================
# STATUS
# ============================================================

STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "service": (
        "EURGBP Short Confirmation Frequency Matrix"
    ),
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "reward_risk": REWARD_RISK,
    "total_tests": TOTAL_TESTS,
    "completed_tests": 0,
    "robust_eligible_signals": 0,
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
        "Authorization": (
            f"Bearer {OANDA_TOKEN}"
        )
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
    if not raw.get(
        "complete",
        False,
    ):
        return None

    mid = raw.get(
        "mid"
    )

    if not mid:
        return None

    return {
        "time": datetime.fromisoformat(
            raw["time"].replace(
                "Z",
                "+00:00",
            )
        ),
        "open": float(
            mid["o"]
        ),
        "high": float(
            mid["h"]
        ),
        "low": float(
            mid["l"]
        ),
        "close": float(
            mid["c"]
        ),
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
        "from": iso_utc(
            start
        ),
        "to": iso_utc(
            end
        ),
        "smooth": "false",
        "includeFirst": "true",
    }

    data = oanda_get(
        f"/v3/instruments/{instrument}/candles",
        params,
    )

    candles = []

    for raw in data.get(
        "candles",
        [],
    ):
        candle = (
            parse_candle(
                raw
            )
        )

        if (
            candle
            is not None
        ):
            candles.append(
                candle
            )

    return candles


def fetch_chunked_history(
    instrument,
    granularity,
    start,
    end,
):
    candles_by_time = {}
    cursor = start

    while (
        cursor < end
    ):
        chunk_end = min(
            cursor
            + timedelta(
                days=H1_CHUNK_DAYS
            ),
            end,
        )

        print(
            f"Fetching {granularity}: "
            f"{cursor.date()} -> "
            f"{chunk_end.date()}",
            flush=True,
        )

        chunk = (
            fetch_range(
                instrument,
                granularity,
                cursor,
                chunk_end,
            )
        )

        for candle in (
            chunk
        ):
            candles_by_time[
                candle["time"]
            ] = candle

        cursor = (
            chunk_end
        )

    candles = list(
        candles_by_time.values()
    )

    candles.sort(
        key=lambda item: (
            item["time"]
        )
    )

    return candles


# ============================================================
# INDICATORS
# ============================================================

def true_ranges(
    candles,
):
    result = []

    for (
        index,
        candle,
    ) in enumerate(
        candles
    ):
        if index == 0:
            tr = (
                candle["high"]
                - candle["low"]
            )
        else:
            previous_close = (
                candles[
                    index - 1
                ]["close"]
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

        result.append(
            tr
        )

    return result


def rma_series(
    values,
    length,
):
    result = [
        None
    ] * len(
        values
    )

    if (
        len(values)
        < length
    ):
        return result

    initial = (
        sum(
            values[
                :length
            ]
        )
        / length
    )

    result[
        length - 1
    ] = initial

    previous = (
        initial
    )

    for index in range(
        length,
        len(values),
    ):
        current = (
            (
                previous
                * (
                    length - 1
                )
            )
            + values[
                index
            ]
        ) / length

        result[
            index
        ] = current

        previous = (
            current
        )

    return result


def atr_series(
    candles,
    length=14,
):
    return rma_series(
        true_ranges(
            candles
        ),
        length,
    )


def rolling_mean_optional(
    values,
    length,
):
    result = [
        None
    ] * len(
        values
    )

    for index in range(
        length - 1,
        len(values),
    ):
        window = values[
            index
            - length
            + 1:
            index
            + 1
        ]

        if any(
            value is None
            for value
            in window
        ):
            continue

        result[
            index
        ] = (
            sum(
                window
            )
            / length
        )

    return result


# ============================================================
# BUILD RAW BEARISH ENGULFING SIGNALS
# ============================================================

def build_raw_candidates(
    h1,
    h1_atr,
    atr_mean_50,
):
    candidates = []

    max_lookback = max(
        STRUCTURE_LOOKBACK,
        48,
        50,
    )

    for index in range(
        max_lookback,
        len(h1),
    ):
        signal = (
            h1[
                index
            ]
        )

        if (
            signal["time"]
            < RESEARCH_FROM
        ):
            continue

        if (
            signal["time"]
            >= RESEARCH_TO
        ):
            break

        previous = (
            h1[
                index - 1
            ]
        )

        atr = (
            h1_atr[
                index
            ]
        )

        if (
            atr is None
            or atr <= 0
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

        candle_range = (
            signal["high"]
            - signal["low"]
        )

        if (
            previous_body <= 0
            or current_body <= 0
            or candle_range <= 0
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

        if not (
            bearish_engulfing
        ):
            continue

        body_ratio = (
            current_body
            / previous_body
        )

        if (
            body_ratio
            < MIN_BODY_RATIO
        ):
            continue

        previous_highest = max(
            candle["high"]
            for candle
            in h1[
                index
                - STRUCTURE_LOOKBACK:
                index
            ]
        )

        structure_distance_atr = (
            previous_highest
            - signal["high"]
        ) / atr

        range_atr = (
            candle_range
            / atr
        )

        close_location = (
            signal["close"]
            - signal["low"]
        ) / candle_range

        momentum_12 = (
            signal["close"]
            - h1[
                index - 12
            ]["close"]
        ) / atr

        momentum_48 = (
            signal["close"]
            - h1[
                index - 48
            ]["close"]
        ) / atr

        upper_wick = max(
            0.0,
            signal["high"]
            - max(
                signal["open"],
                signal["close"],
            ),
        )

        upper_wick_body = (
            upper_wick
            / current_body
        )

        stop = (
            signal["high"]
            + STOP_BUFFER_TICKS
            * TICK_SIZE
        )

        stop_size_atr = (
            stop
            - signal["close"]
        ) / atr

        atr_ratio_50 = None

        if (
            atr_mean_50[
                index
            ] is not None
            and atr_mean_50[
                index
            ] > 0
        ):
            atr_ratio_50 = (
                atr
                / atr_mean_50[
                    index
                ]
            )

        ny_time = (
            signal["time"]
            .astimezone(
                NY_TZ
            )
        )

        candidates.append({
            "index": index,
            "time": (
                signal["time"]
            ),
            "ny_hour": (
                ny_time.hour
            ),
            "structure_distance_atr": (
                structure_distance_atr
            ),
            "range_atr": (
                range_atr
            ),
            "close_location": (
                close_location
            ),
            "momentum_12": (
                momentum_12
            ),
            "momentum_48": (
                momentum_48
            ),
            "upper_wick_body": (
                upper_wick_body
            ),
            "stop_size_atr": (
                stop_size_atr
            ),
            "atr_ratio_50": (
                atr_ratio_50
            ),
        })

    return candidates


# ============================================================
# FIXED ROBUST TRIGGER
# ============================================================

def passes_robust(
    signal,
):
    if (
        signal["ny_hour"]
        in EXCLUDED_NY_HOURS
    ):
        return False

    if (
        signal[
            "structure_distance_atr"
        ]
        > ROBUST_MAX_DISTANCE_ATR
    ):
        return False

    if (
        signal[
            "range_atr"
        ]
        < ROBUST_MIN_RANGE_ATR
    ):
        return False

    if (
        signal[
            "close_location"
        ]
        > ROBUST_MAX_CLOSE_LOCATION
    ):
        return False

    if (
        signal[
            "momentum_12"
        ]
        < ROBUST_MIN_MOMENTUM_12
    ):
        return False

    if (
        signal[
            "momentum_48"
        ]
        < ROBUST_MIN_MOMENTUM_48
    ):
        return False

    if (
        signal[
            "stop_size_atr"
        ]
        > ROBUST_MAX_STOP_SIZE_ATR
    ):
        return False

    return True


# ============================================================
# CONFIRMATION LAYER
# ============================================================

def confirmation_components(
    signal,
    test,
):
    structure_pass = (
        signal[
            "structure_distance_atr"
        ]
        <= test[
            "structure_threshold"
        ]
    )

    momentum_pass = (
        signal[
            "momentum_48"
        ]
        >= test[
            "momentum48_threshold"
        ]
    )

    wick_pass = (
        signal[
            "upper_wick_body"
        ]
        >= test[
            "wick_threshold"
        ]
    )

    atr_pass = (
        signal[
            "atr_ratio_50"
        ] is not None
        and signal[
            "atr_ratio_50"
        ]
        >= test[
            "atr_ratio_threshold"
        ]
    )

    return (
        structure_pass,
        momentum_pass,
        wick_pass,
        atr_pass,
    )


def passes_confirmation(
    signal,
    test,
):
    components = (
        confirmation_components(
            signal,
            test,
        )
    )

    score = sum(
        1
        for component
        in components
        if component
    )

    if (
        test[
            "family"
        ] == "ALL_REQUIRED"
    ):
        return (
            score == 4
        )

    return (
        score
        >= test[
            "required_score"
        ]
    )


# ============================================================
# TRADE EXIT
# ============================================================

EXIT_CACHE = {}


def calculate_trade_exit(
    h1,
    signal_index,
):
    if (
        signal_index
        in EXIT_CACHE
    ):
        return (
            EXIT_CACHE[
                signal_index
            ]
        )

    signal = (
        h1[
            signal_index
        ]
    )

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

    if (
        reference_risk
        <= 0
    ):
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

    if (
        actual_risk
        <= 0
    ):
        raise RuntimeError(
            "Invalid short actual risk"
        )

    for index in range(
        signal_index + 1,
        len(h1),
    ):
        candle = (
            h1[
                index
            ]
        )

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
                exit_price = (
                    stop
                )
            else:
                exit_price = (
                    target
                )

        elif (
            stop_hit
        ):
            exit_price = (
                stop
            )

        else:
            exit_price = (
                target
            )

        result = {
            "status": (
                "CLOSED"
            ),
            "signal_index": (
                signal_index
            ),
            "signal_time": (
                signal[
                    "time"
                ]
            ),
            "exit_index": (
                index
            ),
            "exit_time": (
                candle[
                    "time"
                ]
            ),
            "result_r": (
                backtest_entry
                - exit_price
            ) / actual_risk,
        }

        EXIT_CACHE[
            signal_index
        ] = result

        return result

    result = {
        "status": (
            "OPEN"
        ),
        "signal_index": (
            signal_index
        ),
        "signal_time": (
            signal[
                "time"
            ]
        ),
        "exit_index": None,
        "exit_time": None,
        "result_r": None,
    }

    EXIT_CACHE[
        signal_index
    ] = result

    return result


def simulate(
    h1,
    eligible,
):
    trades = []
    position_exit_index = -1
    ignored = 0
    still_open = False

    for signal in (
        eligible
    ):
        signal_index = (
            signal[
                "index"
            ]
        )

        if (
            signal_index
            < position_exit_index
        ):
            ignored += 1
            continue

        trade = (
            calculate_trade_exit(
                h1,
                signal_index,
            )
        )

        if (
            trade[
                "status"
            ] == "OPEN"
        ):
            still_open = True
            break

        trades.append(
            trade
        )

        position_exit_index = (
            trade[
                "exit_index"
            ]
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

    for trade in (
        trades
    ):
        signal_time = (
            trade[
                "signal_time"
            ]
        )

        if (
            start is not None
            and signal_time
            < start
        ):
            continue

        if (
            end is not None
            and signal_time
            >= end
        ):
            continue

        filtered.append(
            trade
        )

    if not (
        filtered
    ):
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
        trade[
            "result_r"
        ]
        for trade
        in filtered
    ]

    winners = [
        result
        for result
        in results
        if result > 0
    ]

    losers = [
        result
        for result
        in results
        if result < 0
    ]

    gross_profit = sum(
        winners
    )

    gross_loss = abs(
        sum(
            losers
        )
    )

    total_r = sum(
        results
    )

    if (
        gross_loss > 0
    ):
        profit_factor = (
            gross_profit
            / gross_loss
        )
    elif (
        gross_profit > 0
    ):
        profit_factor = (
            999.0
        )
    else:
        profit_factor = (
            0.0
        )

    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0

    current_streak = 0
    longest_streak = 0

    for result in (
        results
    ):
        equity += (
            result
        )

        peak = max(
            peak,
            equity,
        )

        max_drawdown = min(
            max_drawdown,
            equity - peak,
        )

        if (
            result < 0
        ):
            current_streak += 1

            longest_streak = max(
                longest_streak,
                current_streak,
            )
        else:
            current_streak = 0

    return {
        "trades": len(
            results
        ),
        "winners": len(
            winners
        ),
        "losers": len(
            losers
        ),
        "win_rate": round(
            len(
                winners
            )
            / len(
                results
            )
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
            / len(
                results
            ),
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
# WINDOWS
# ============================================================

def subtract_years_safe(
    dt,
    years,
):
    try:
        return (
            dt.replace(
                year=(
                    dt.year
                    - years
                )
            )
        )
    except ValueError:
        return (
            dt.replace(
                month=2,
                day=28,
                year=(
                    dt.year
                    - years
                ),
            )
        )


def rolling_3y_worst(
    trades,
):
    rows = []

    for start_year in range(
        2002,
        RESEARCH_TO.year - 1,
    ):
        start = datetime(
            start_year,
            1,
            1,
            tzinfo=timezone.utc,
        )

        end = datetime(
            start_year + 3,
            1,
            1,
            tzinfo=timezone.utc,
        )

        actual_start = max(
            start,
            RESEARCH_FROM,
        )

        actual_end = min(
            end,
            RESEARCH_TO,
        )

        if (
            actual_start
            >= actual_end
        ):
            continue

        stats = (
            stats_for_trades(
                trades,
                actual_start,
                actual_end,
            )
        )

        if (
            stats[
                "trades"
            ] >= 5
        ):
            rows.append({
                "label": (
                    f"{start_year}_"
                    f"{start_year + 2}"
                ),
                "pf": (
                    stats[
                        "profit_factor"
                    ]
                ),
                "expectancy": (
                    stats[
                        "expectancy_r"
                    ]
                ),
                "total_r": (
                    stats[
                        "total_r"
                    ]
                ),
                "trades": (
                    stats[
                        "trades"
                    ]
                ),
            })

    if not (
        rows
    ):
        return {
            "worst_rolling_3y_pf": None,
            "worst_rolling_3y_pf_label": None,
            "worst_rolling_3y_expectancy": None,
            "worst_rolling_3y_expectancy_label": None,
            "worst_rolling_3y_total_r": None,
            "worst_rolling_3y_total_r_label": None,
        }

    worst_pf = min(
        rows,
        key=lambda row: (
            row[
                "pf"
            ]
        ),
    )

    worst_exp = min(
        rows,
        key=lambda row: (
            row[
                "expectancy"
            ]
        ),
    )

    worst_total = min(
        rows,
        key=lambda row: (
            row[
                "total_r"
            ]
        ),
    )

    return {
        "worst_rolling_3y_pf": (
            worst_pf[
                "pf"
            ]
        ),
        "worst_rolling_3y_pf_label": (
            worst_pf[
                "label"
            ]
        ),
        "worst_rolling_3y_expectancy": (
            worst_exp[
                "expectancy"
            ]
        ),
        "worst_rolling_3y_expectancy_label": (
            worst_exp[
                "label"
            ]
        ),
        "worst_rolling_3y_total_r": (
            worst_total[
                "total_r"
            ]
        ),
        "worst_rolling_3y_total_r_label": (
            worst_total[
                "label"
            ]
        ),
    }


# ============================================================
# RESULT ROW
# ============================================================

def build_result_row(
    test,
    eligible,
    trades,
    ignored,
    still_open,
    years,
):
    full = (
        stats_for_trades(
            trades
        )
    )

    row = {
        "family": (
            test[
                "family"
            ]
        ),
        "required_score": (
            test[
                "required_score"
            ]
        ),
        "confirmation_structure_max_atr": (
            test[
                "structure_threshold"
            ]
        ),
        "confirmation_momentum48_min_atr": (
            test[
                "momentum48_threshold"
            ]
        ),
        "confirmation_wick_min_body": (
            test[
                "wick_threshold"
            ]
        ),
        "confirmation_atr_ratio_min": (
            test[
                "atr_ratio_threshold"
            ]
        ),
        "eligible_signals": len(
            eligible
        ),
        "ignored_due_to_open_trade": (
            ignored
        ),
        "still_open_at_end": (
            still_open
        ),
        "trades": (
            full[
                "trades"
            ]
        ),
        "trades_per_year": round(
            full[
                "trades"
            ]
            / years,
            2,
        ),
        "winners": (
            full[
                "winners"
            ]
        ),
        "losers": (
            full[
                "losers"
            ]
        ),
        "win_rate": (
            full[
                "win_rate"
            ]
        ),
        "profit_factor": (
            full[
                "profit_factor"
            ]
        ),
        "total_r": (
            full[
                "total_r"
            ]
        ),
        "expectancy_r": (
            full[
                "expectancy_r"
            ]
        ),
        "max_drawdown_r": (
            full[
                "max_drawdown_r"
            ]
        ),
        "longest_loss_streak": (
            full[
                "longest_loss_streak"
            ]
        ),
        "annual_r_linear": round(
            full[
                "expectancy_r"
            ]
            * (
                full[
                    "trades"
                ]
                / years
            ),
            3,
        ),
    }

    profitable_eras = 0
    minimum_era_pf = None
    minimum_era_expectancy = None

    for (
        era_name,
        era_start,
        era_end,
    ) in ERAS:
        era_stats = (
            stats_for_trades(
                trades,
                era_start,
                era_end,
            )
        )

        row[
            f"{era_name}_trades"
        ] = (
            era_stats[
                "trades"
            ]
        )

        row[
            f"{era_name}_pf"
        ] = (
            era_stats[
                "profit_factor"
            ]
        )

        row[
            f"{era_name}_r"
        ] = (
            era_stats[
                "total_r"
            ]
        )

        row[
            f"{era_name}_expectancy"
        ] = (
            era_stats[
                "expectancy_r"
            ]
        )

        if (
            era_stats[
                "trades"
            ] >= 5
        ):
            if (
                era_stats[
                    "total_r"
                ] > 0
            ):
                profitable_eras += 1

            if (
                minimum_era_pf
                is None
            ):
                minimum_era_pf = (
                    era_stats[
                        "profit_factor"
                    ]
                )
            else:
                minimum_era_pf = min(
                    minimum_era_pf,
                    era_stats[
                        "profit_factor"
                    ],
                )

            if (
                minimum_era_expectancy
                is None
            ):
                minimum_era_expectancy = (
                    era_stats[
                        "expectancy_r"
                    ]
                )
            else:
                minimum_era_expectancy = min(
                    minimum_era_expectancy,
                    era_stats[
                        "expectancy_r"
                    ],
                )

    row[
        "profitable_eras_with_5_plus_trades"
    ] = (
        profitable_eras
    )

    row[
        "minimum_era_pf_5_plus"
    ] = (
        minimum_era_pf
    )

    row[
        "minimum_era_expectancy_5_plus"
    ] = (
        minimum_era_expectancy
    )

    row[
        "all_four_eras_profitable"
    ] = (
        profitable_eras
        >= 4
    )

    for years_back in [
        2,
        5,
        10,
    ]:
        recent_start = (
            subtract_years_safe(
                RESEARCH_TO,
                years_back,
            )
        )

        recent = (
            stats_for_trades(
                trades,
                recent_start,
                RESEARCH_TO,
            )
        )

        row[
            f"last_{years_back}y_trades"
        ] = (
            recent[
                "trades"
            ]
        )

        row[
            f"last_{years_back}y_pf"
        ] = (
            recent[
                "profit_factor"
            ]
        )

        row[
            f"last_{years_back}y_r"
        ] = (
            recent[
                "total_r"
            ]
        )

        row[
            f"last_{years_back}y_expectancy"
        ] = (
            recent[
                "expectancy_r"
            ]
        )

    row.update(
        rolling_3y_worst(
            trades
        )
    )

    row[
        "frequency_3py"
    ] = (
        row[
            "trades_per_year"
        ] >= 3.0
    )

    row[
        "frequency_35py"
    ] = (
        row[
            "trades_per_year"
        ] >= 3.5
    )

    row[
        "frequency_4py"
    ] = (
        row[
            "trades_per_year"
        ] >= 4.0
    )

    row[
        "pf_180"
    ] = (
        row[
            "profit_factor"
        ] >= 1.80
    )

    row[
        "pf_200"
    ] = (
        row[
            "profit_factor"
        ] >= 2.00
    )

    row[
        "expectancy_050"
    ] = (
        row[
            "expectancy_r"
        ] >= 0.50
    )

    row[
        "expectancy_060"
    ] = (
        row[
            "expectancy_r"
        ] >= 0.60
    )

    row[
        "worst_era_pf_130"
    ] = (
        minimum_era_pf
        is not None
        and minimum_era_pf
        >= 1.30
    )

    row[
        "worst_era_pf_140"
    ] = (
        minimum_era_pf
        is not None
        and minimum_era_pf
        >= 1.40
    )

    row[
        "dd_below_6r"
    ] = (
        row[
            "max_drawdown_r"
        ] >= -6.0
    )

    return row


# ============================================================
# REFERENCE DELTAS
# ============================================================

def add_reference_deltas(
    df,
):
    reference_mask = (
        (
            df[
                "family"
            ]
            == "ALL_REQUIRED"
        )
        & (
            df[
                "confirmation_structure_max_atr"
            ]
            == REFERENCE[
                "structure_threshold"
            ]
        )
        & (
            df[
                "confirmation_momentum48_min_atr"
            ]
            == REFERENCE[
                "momentum48_threshold"
            ]
        )
        & (
            df[
                "confirmation_wick_min_body"
            ]
            == REFERENCE[
                "wick_threshold"
            ]
        )
        & (
            df[
                "confirmation_atr_ratio_min"
            ]
            == REFERENCE[
                "atr_ratio_threshold"
            ]
        )
    )

    reference_rows = (
        df[
            reference_mask
        ]
    )

    if (
        len(
            reference_rows
        ) != 1
    ):
        raise RuntimeError(
            "Could not uniquely locate "
            "the current CONFIRMED reference row"
        )

    reference = (
        reference_rows.iloc[
            0
        ]
    )

    delta_metrics = [
        "trades",
        "trades_per_year",
        "profit_factor",
        "total_r",
        "expectancy_r",
        "max_drawdown_r",
        "minimum_era_pf_5_plus",
        "last_5y_pf",
        "last_10y_pf",
        "annual_r_linear",
    ]

    for metric in (
        delta_metrics
    ):
        df[
            f"delta_{metric}_vs_confirmed"
        ] = (
            df[
                metric
            ]
            - reference[
                metric
            ]
        )

    df[
        "is_current_confirmed"
    ] = (
        reference_mask
    )

    return df


# ============================================================
# RUN RESEARCH
# ============================================================

def run_research():
    global STATUS

    try:
        print()
        print("=" * 90)
        print(
            "EUR/GBP SHORT - CONFIRMATION FREQUENCY MATRIX"
        )
        print("=" * 90)
        print(
            f"Total matrix tests: {TOTAL_TESTS}"
        )
        print()

        STATUS.update({
            "state": (
                "fetching_data"
            ),
            "message": (
                "Fetching EUR/GBP OANDA H1 history"
            ),
        })

        h1 = (
            fetch_chunked_history(
                INSTRUMENT,
                "H1",
                RESEARCH_FROM
                - timedelta(
                    days=H1_WARMUP_DAYS
                ),
                RESEARCH_TO,
            )
        )

        if not h1:
            raise RuntimeError(
                "No EUR/GBP H1 candles returned"
            )

        STATUS.update({
            "state": (
                "precomputing"
            ),
            "message": (
                "Building ATR14 and fixed ROBUST trigger set"
            ),
        })

        h1_atr = (
            atr_series(
                h1,
                14,
            )
        )

        atr_mean_50 = (
            rolling_mean_optional(
                h1_atr,
                50,
            )
        )

        raw_candidates = (
            build_raw_candidates(
                h1,
                h1_atr,
                atr_mean_50,
            )
        )

        robust_signals = [
            signal
            for signal
            in raw_candidates
            if passes_robust(
                signal
            )
        ]

        STATUS[
            "raw_bearish_engulfing_signals"
        ] = len(
            raw_candidates
        )

        STATUS[
            "robust_eligible_signals"
        ] = len(
            robust_signals
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

        STATUS.update({
            "state": (
                "running"
            ),
            "message": (
                "Running confirmation matrix"
            ),
        })

        rows = []

        for (
            completed,
            test,
        ) in enumerate(
            MATRIX_TESTS,
            start=1,
        ):
            eligible = [
                signal
                for signal
                in robust_signals
                if passes_confirmation(
                    signal,
                    test,
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

            row = (
                build_result_row(
                    test,
                    eligible,
                    trades,
                    ignored,
                    still_open,
                    years,
                )
            )

            rows.append(
                row
            )

            STATUS[
                "completed_tests"
            ] = completed

            if (
                completed % 100 == 0
                or completed == TOTAL_TESTS
            ):
                print(
                    f"{completed}/{TOTAL_TESTS}",
                    flush=True,
                )

        df = (
            pd.DataFrame(
                rows
            )
        )

        if (
            df.empty
        ):
            raise RuntimeError(
                "No matrix rows generated"
            )

        df = (
            add_reference_deltas(
                df
            )
        )

        # Balanced ranking:
        # first preserve broad robustness,
        # then reward >=3.5 trades/year,
        # high worst-era PF,
        # expectancy / PF,
        # and annual R.
        df = (
            df.sort_values(
                by=[
                    "is_current_confirmed",
                    "all_four_eras_profitable",
                    "frequency_35py",
                    "worst_era_pf_140",
                    "worst_era_pf_130",
                    "expectancy_060",
                    "expectancy_050",
                    "pf_200",
                    "pf_180",
                    "minimum_era_pf_5_plus",
                    "annual_r_linear",
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
                    False,
                    False,
                    False,
                    False,
                    False,
                    False,
                    False,
                    False,
                ],
            )
        )

        df.to_csv(
            OUTPUT_FILE,
            index=False,
        )

        quality_mask = (
            (
                df[
                    "trades_per_year"
                ] >= 3.5
            )
            & (
                df[
                    "profit_factor"
                ] >= 1.80
            )
            & (
                df[
                    "expectancy_r"
                ] >= 0.50
            )
            & (
                df[
                    "minimum_era_pf_5_plus"
                ] >= 1.30
            )
            & (
                df[
                    "all_four_eras_profitable"
                ]
            )
        )

        STATUS.update({
            "state": (
                "complete"
            ),
            "message": (
                "EUR/GBP confirmation frequency matrix "
                "completed successfully"
            ),
            "completed_tests": (
                TOTAL_TESTS
            ),
            "rows_saved": len(
                df
            ),
            "quality_candidates": int(
                quality_mask.sum()
            ),
            "output_file": (
                OUTPUT_FILE
            ),
        })

        print()
        print("=" * 90)
        print(
            "CONFIRMATION FREQUENCY MATRIX COMPLETE"
        )
        print("=" * 90)
        print(
            f"Rows: {len(df)}"
        )
        print(
            f"Quality candidates: "
            f"{int(quality_mask.sum())}"
        )
        print(
            f"Saved: {OUTPUT_FILE}"
        )
        print()

        columns = [
            "family",
            "required_score",
            "confirmation_structure_max_atr",
            "confirmation_momentum48_min_atr",
            "confirmation_wick_min_body",
            "confirmation_atr_ratio_min",
            "trades",
            "trades_per_year",
            "profit_factor",
            "total_r",
            "expectancy_r",
            "max_drawdown_r",
            "minimum_era_pf_5_plus",
            "last_5y_pf",
            "last_10y_pf",
            "worst_rolling_3y_pf",
            "annual_r_linear",
            "delta_trades_vs_confirmed",
        ]

        print(
            df[
                columns
            ].head(
                25
            ).to_string(
                index=False
            ),
            flush=True,
        )

    except Exception as error:
        STATUS.update({
            "state": (
                "error"
            ),
            "message": str(
                error
            ),
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
            "EURGBP Short Confirmation Frequency Matrix"
        ),
        "status": (
            STATUS
        ),
        "instrument": (
            INSTRUMENT
        ),
        "direction": (
            "SHORT"
        ),
        "reward_risk": (
            REWARD_RISK
        ),
        "timezone": (
            "America/New_York"
        ),
        "timing_basis": (
            "signal candle open time"
        ),
        "excluded_ny_hours": sorted(
            EXCLUDED_NY_HOURS
        ),
        "trading_enabled": False,
        "orders_supported": False,
        "executor_connected": False,

        "fixed_robust_trigger": {
            "structure_lookback": (
                STRUCTURE_LOOKBACK
            ),
            "max_distance_atr": (
                ROBUST_MAX_DISTANCE_ATR
            ),
            "min_range_atr": (
                ROBUST_MIN_RANGE_ATR
            ),
            "max_close_location": (
                ROBUST_MAX_CLOSE_LOCATION
            ),
            "min_momentum_12_atr": (
                ROBUST_MIN_MOMENTUM_12
            ),
            "min_momentum_48_atr": (
                ROBUST_MIN_MOMENTUM_48
            ),
            "max_stop_size_atr": (
                ROBUST_MAX_STOP_SIZE_ATR
            ),
        },

        "reference_confirmed": (
            REFERENCE
        ),

        "matrix": {
            "all_required_tests": (
                len(
                    ALL_STRUCTURE
                )
                * len(
                    ALL_MOM48
                )
                * len(
                    ALL_WICK
                )
                * len(
                    ALL_ATR_RATIO
                )
            ),
            "score_tests": (
                len(
                    SCORE_STRUCTURE
                )
                * len(
                    SCORE_MOM48
                )
                * len(
                    SCORE_WICK
                )
                * len(
                    SCORE_ATR_RATIO
                )
                * len(
                    SCORE_REQUIRED
                )
            ),
            "total_tests": (
                TOTAL_TESTS
            ),
        },

        "download": (
            "/download"
        ),
    })


@app.route("/status")
def status():
    return jsonify(
        STATUS
    )


@app.route("/download")
def download():
    if not (
        os.path.exists(
            OUTPUT_FILE
        )
    ):
        return jsonify({
            "status": (
                "not_ready"
            ),
            "message": (
                "EUR/GBP confirmation frequency matrix "
                "CSV is not ready yet"
            ),
        }), 404

    return send_file(
        OUTPUT_FILE,
        as_attachment=True,
        download_name=(
            OUTPUT_FILE
        ),
    )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    research_thread = (
        threading.Thread(
            target=run_research,
            name=(
                "eurgbp-short-confirmation-frequency-matrix"
            ),
            daemon=True,
        )
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
