import os
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta


# ============================================================
# EUR/GBP SHORT - DUAL-BRANCH FREQUENCY RECOVERY
# + SECONDARY REGIME REFINEMENT
#
# RESEARCH ONLY — NEVER SUBMITS ORDERS.
#
# Purpose:
#   Continue BOTH surviving EUR/GBP short branches:
#
#   A) ROBUST BRANCH
#      12h upward momentum >= 0.25 ATR14
#      48h upward momentum >= 0.50 ATR14
#      stop size <= 2.50 ATR14
#      no wick filter
#
#   B) HIGH-PF BRANCH
#      48h upward momentum >= 1.00 ATR14
#      upper wick/body >= 0.10
#      no 12h momentum filter
#      no stop cap
#
# Shared core:
#   bearish engulfing
#   body ratio >= 1.00
#   RR = 3.00
#   stop = signal high + 10 ticks
#   adverse short slippage = 5 ticks
#   pyramiding = 0
#
# Frequency-recovery geometry:
#   structure lookback: 80, 90, 100
#   max distance ATR:   0.075, 0.10, 0.125
#   min range ATR:      1.00, 1.05, 1.10
#   max close location: 0.20, 0.225, 0.25
#
# Secondary modifier profiles:
#   NONE
#   ATR14 / mean(ATR14, 50) >= 0.80
#   ATR14 / mean(ATR14, 50) >= 0.90
#   ATR14 / mean(ATR14, 50) >= 1.00
#   EMA20 12h slope >= 0.00 ATR
#   EMA20 12h slope >= 0.10 ATR
#   EMA20 12h slope >= 0.20 ATR
#   prior structure high within 72 bars
#   prior structure high within 48 bars
#   prior structure high within 36 bars
#
# IMPORTANT:
#   Only ONE secondary modifier is applied at a time.
#   We are not stacking ATR regime + EMA slope + high recency.
#
# Total:
#   3*3*3*3 = 81 geometry sets
#   10 modifier profiles
#   2 branches
#   = 1,620 tests
#
# Output:
#   eurgbp_short_dual_branch_frequency_recovery.csv
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

STRUCTURE_LOOKBACKS = [
    80,
    90,
    100,
]

MAX_DISTANCE_ATR_VALUES = [
    0.075,
    0.10,
    0.125,
]

MIN_RANGE_ATR_VALUES = [
    1.00,
    1.05,
    1.10,
]

MAX_CLOSE_LOCATION_VALUES = [
    0.20,
    0.225,
    0.25,
]

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

OUTPUT_FILE = "eurgbp_short_dual_branch_frequency_recovery.csv"


# ============================================================
# BRANCH DEFINITIONS
# ============================================================

BRANCHES = [
    {
        "branch": "ROBUST",
        "min_momentum_12": 0.25,
        "min_momentum_48": 0.50,
        "min_upper_wick_body": None,
        "max_stop_size_atr": 2.50,
    },
    {
        "branch": "HIGH_PF",
        "min_momentum_12": None,
        "min_momentum_48": 1.00,
        "min_upper_wick_body": 0.10,
        "max_stop_size_atr": None,
    },
]


# ============================================================
# SECONDARY MODIFIERS
# ============================================================

MODIFIERS = [
    {
        "modifier_family": "NONE",
        "modifier_label": "none",
        "min_atr_ratio_50": None,
        "min_ema20_slope_12h_atr": None,
        "max_bars_since_structure_high": None,
    },

    {
        "modifier_family": "ATR_REGIME",
        "modifier_label": "atr_ratio_gte_0.80",
        "min_atr_ratio_50": 0.80,
        "min_ema20_slope_12h_atr": None,
        "max_bars_since_structure_high": None,
    },
    {
        "modifier_family": "ATR_REGIME",
        "modifier_label": "atr_ratio_gte_0.90",
        "min_atr_ratio_50": 0.90,
        "min_ema20_slope_12h_atr": None,
        "max_bars_since_structure_high": None,
    },
    {
        "modifier_family": "ATR_REGIME",
        "modifier_label": "atr_ratio_gte_1.00",
        "min_atr_ratio_50": 1.00,
        "min_ema20_slope_12h_atr": None,
        "max_bars_since_structure_high": None,
    },

    {
        "modifier_family": "EMA20_SLOPE_12H",
        "modifier_label": "ema20_slope12_gte_0.00",
        "min_atr_ratio_50": None,
        "min_ema20_slope_12h_atr": 0.00,
        "max_bars_since_structure_high": None,
    },
    {
        "modifier_family": "EMA20_SLOPE_12H",
        "modifier_label": "ema20_slope12_gte_0.10",
        "min_atr_ratio_50": None,
        "min_ema20_slope_12h_atr": 0.10,
        "max_bars_since_structure_high": None,
    },
    {
        "modifier_family": "EMA20_SLOPE_12H",
        "modifier_label": "ema20_slope12_gte_0.20",
        "min_atr_ratio_50": None,
        "min_ema20_slope_12h_atr": 0.20,
        "max_bars_since_structure_high": None,
    },

    {
        "modifier_family": "HIGH_RECENCY",
        "modifier_label": "prior_high_within_72_bars",
        "min_atr_ratio_50": None,
        "min_ema20_slope_12h_atr": None,
        "max_bars_since_structure_high": 72,
    },
    {
        "modifier_family": "HIGH_RECENCY",
        "modifier_label": "prior_high_within_48_bars",
        "min_atr_ratio_50": None,
        "min_ema20_slope_12h_atr": None,
        "max_bars_since_structure_high": 48,
    },
    {
        "modifier_family": "HIGH_RECENCY",
        "modifier_label": "prior_high_within_36_bars",
        "min_atr_ratio_50": None,
        "min_ema20_slope_12h_atr": None,
        "max_bars_since_structure_high": 36,
    },
]


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

GEOMETRY_TESTS = (
    len(STRUCTURE_LOOKBACKS)
    * len(MAX_DISTANCE_ATR_VALUES)
    * len(MIN_RANGE_ATR_VALUES)
    * len(MAX_CLOSE_LOCATION_VALUES)
)

TOTAL_TESTS = (
    GEOMETRY_TESTS
    * len(MODIFIERS)
    * len(BRANCHES)
)

STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "service": "EURGBP Short Dual-Branch Frequency Recovery",
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "reward_risk": REWARD_RISK,
    "geometry_tests_per_modifier": GEOMETRY_TESTS,
    "modifier_profiles": len(MODIFIERS),
    "branches": len(BRANCHES),
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

    result[
        length - 1
    ] = initial

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

    for index, candle in enumerate(
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


def rma_series(values, length):
    result = [None] * len(values)

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
        len(values),
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
    result = [None] * len(values)

    for index in range(
        len(values)
    ):
        if index < length - 1:
            continue

        window = values[
            index - length + 1:
            index + 1
        ]

        if any(
            value is None
            for value in window
        ):
            continue

        result[index] = (
            sum(window)
            / length
        )

    return result


# ============================================================
# RAW SIGNAL FEATURES
# ============================================================

def build_candidates(
    h1,
    h1_atr,
    atr_mean_50,
    ema20,
):
    candidates = []

    max_structure = max(
        STRUCTURE_LOOKBACKS
    )

    max_lookback = max(
        max_structure,
        48,
        12,
        50,
    )

    for index in range(
        max_lookback,
        len(h1),
    ):
        signal = h1[index]

        if signal["time"] < RESEARCH_FROM:
            continue

        if signal["time"] >= RESEARCH_TO:
            break

        previous = h1[index - 1]
        atr = h1_atr[index]

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

        if not bearish_engulfing:
            continue

        body_ratio = (
            current_body
            / previous_body
        )

        if body_ratio < MIN_BODY_RATIO:
            continue

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
            )
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
            atr_mean_50[index]
            is not None
            and atr_mean_50[index] > 0
        ):
            atr_ratio_50 = (
                atr
                / atr_mean_50[index]
            )

        ema20_slope_12h_atr = None

        if (
            ema20[index] is not None
            and ema20[
                index - 12
            ] is not None
        ):
            ema20_slope_12h_atr = (
                ema20[index]
                - ema20[
                    index - 12
                ]
            ) / atr

        structures = {}
        bars_since_high = {}

        for lookback in (
            STRUCTURE_LOOKBACKS
        ):
            slice_start = (
                index - lookback
            )

            previous_slice = h1[
                slice_start:
                index
            ]

            previous_highest = max(
                candle["high"]
                for candle
                in previous_slice
            )

            structures[
                lookback
            ] = (
                previous_highest
                - signal["high"]
            ) / atr

            most_recent_offset = None

            for offset in range(
                1,
                lookback + 1,
            ):
                candidate_index = (
                    index - offset
                )

                if (
                    abs(
                        h1[
                            candidate_index
                        ]["high"]
                        - previous_highest
                    )
                    <= 1e-12
                ):
                    most_recent_offset = (
                        offset
                    )
                    break

            bars_since_high[
                lookback
            ] = (
                most_recent_offset
            )

        candidates.append({
            "index": index,
            "time": signal["time"],
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
            "ema20_slope_12h_atr": (
                ema20_slope_12h_atr
            ),
            "structure": (
                structures
            ),
            "bars_since_high": (
                bars_since_high
            ),
        })

    return candidates


# ============================================================
# EXIT SIMULATION
# ============================================================

EXIT_CACHE = {}


def calculate_trade_exit(
    h1,
    signal_index,
):
    if signal_index in EXIT_CACHE:
        return EXIT_CACHE[
            signal_index
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
            signal_index
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

    for candidate in eligible:
        signal_index = (
            candidate["index"]
        )

        # Locked convention:
        # signal on exact exit candle is allowed.
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

        if trade[
            "status"
        ] == "OPEN":
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
        signal_time = (
            trade["signal_time"]
        )

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
    branch,
    modifier,
    lookback,
    max_distance_atr,
    min_range_atr,
    max_close_location,
    raw_candidates,
    eligible,
    trades,
    ignored,
    still_open,
    years,
):
    full = stats_for_trades(
        trades
    )

    row = {
        "branch": (
            branch["branch"]
        ),

        "structure_lookback": (
            lookback
        ),
        "max_distance_atr": (
            max_distance_atr
        ),
        "min_range_atr": (
            min_range_atr
        ),
        "max_close_location": (
            max_close_location
        ),

        "branch_min_momentum_12h_atr": (
            branch[
                "min_momentum_12"
            ]
        ),
        "branch_min_momentum_48h_atr": (
            branch[
                "min_momentum_48"
            ]
        ),
        "branch_min_upper_wick_body": (
            branch[
                "min_upper_wick_body"
            ]
        ),
        "branch_max_stop_size_atr": (
            branch[
                "max_stop_size_atr"
            ]
        ),

        "modifier_family": (
            modifier[
                "modifier_family"
            ]
        ),
        "modifier_label": (
            modifier[
                "modifier_label"
            ]
        ),
        "modifier_min_atr_ratio_50": (
            modifier[
                "min_atr_ratio_50"
            ]
        ),
        "modifier_min_ema20_slope_12h_atr": (
            modifier[
                "min_ema20_slope_12h_atr"
            ]
        ),
        "modifier_max_bars_since_structure_high": (
            modifier[
                "max_bars_since_structure_high"
            ]
        ),

        "raw_signals": len(
            raw_candidates
        ),
        "eligible_signals": len(
            eligible
        ),
        "signal_retention_pct": round(
            len(eligible)
            / len(raw_candidates)
            * 100.0,
            2,
        ) if raw_candidates else 0.0,

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
            full[
                "trades"
            ] / years,
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
        ] = era[
            "trades"
        ]

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

        if era[
            "trades"
        ] >= 5:
            if era[
                "total_r"
            ] > 0:
                profitable_eras_with_5_plus += 1

            pf = era[
                "profit_factor"
            ]

            expectancy = era[
                "expectancy_r"
            ]

            if (
                minimum_era_pf_5_plus
                is None
            ):
                minimum_era_pf_5_plus = pf
            else:
                minimum_era_pf_5_plus = min(
                    minimum_era_pf_5_plus,
                    pf,
                )

            if (
                minimum_era_expectancy_5_plus
                is None
            ):
                minimum_era_expectancy_5_plus = expectancy
            else:
                minimum_era_expectancy_5_plus = min(
                    minimum_era_expectancy_5_plus,
                    expectancy,
                )

    row[
        "profitable_eras_with_5_plus_trades"
    ] = profitable_eras_with_5_plus

    row[
        "minimum_era_pf_5_plus"
    ] = minimum_era_pf_5_plus

    row[
        "minimum_era_expectancy_5_plus"
    ] = minimum_era_expectancy_5_plus

    row[
        "all_four_eras_profitable"
    ] = (
        profitable_eras_with_5_plus
        >= 4
    )

    row[
        "adequate_90_trades"
    ] = (
        full["trades"] >= 90
    )

    row[
        "adequate_100_trades"
    ] = (
        full["trades"] >= 100
    )

    row[
        "frequency_4py"
    ] = (
        full["trades"]
        / years
        >= 4.0
    )

    row[
        "frequency_45py"
    ] = (
        full["trades"]
        / years
        >= 4.5
    )

    row[
        "frequency_5py"
    ] = (
        full["trades"]
        / years
        >= 5.0
    )

    row[
        "worst_era_pf_120"
    ] = (
        minimum_era_pf_5_plus is not None
        and minimum_era_pf_5_plus >= 1.20
    )

    row[
        "worst_era_pf_130"
    ] = (
        minimum_era_pf_5_plus is not None
        and minimum_era_pf_5_plus >= 1.30
    )

    row[
        "worst_era_pf_140"
    ] = (
        minimum_era_pf_5_plus is not None
        and minimum_era_pf_5_plus >= 1.40
    )

    row[
        "pf_150"
    ] = (
        full[
            "profit_factor"
        ] >= 1.50
    )

    row[
        "pf_160"
    ] = (
        full[
            "profit_factor"
        ] >= 1.60
    )

    row[
        "pf_170"
    ] = (
        full[
            "profit_factor"
        ] >= 1.70
    )

    row[
        "pf_180"
    ] = (
        full[
            "profit_factor"
        ] >= 1.80
    )

    row[
        "dd_better_than_8r"
    ] = (
        full[
            "max_drawdown_r"
        ] >= -8.0
    )

    row[
        "annual_r_linear"
    ] = round(
        full[
            "expectancy_r"
        ]
        * (
            full[
                "trades"
            ] / years
        ),
        3,
    )

    return row


# ============================================================
# ELIGIBILITY
# ============================================================

def passes_branch(
    candidate,
    branch,
):
    if (
        branch[
            "min_momentum_12"
        ] is not None
        and candidate[
            "momentum_12"
        ] < branch[
            "min_momentum_12"
        ]
    ):
        return False

    if (
        branch[
            "min_momentum_48"
        ] is not None
        and candidate[
            "momentum_48"
        ] < branch[
            "min_momentum_48"
        ]
    ):
        return False

    if (
        branch[
            "min_upper_wick_body"
        ] is not None
        and candidate[
            "upper_wick_body"
        ] < branch[
            "min_upper_wick_body"
        ]
    ):
        return False

    if (
        branch[
            "max_stop_size_atr"
        ] is not None
        and candidate[
            "stop_size_atr"
        ] > branch[
            "max_stop_size_atr"
        ]
    ):
        return False

    return True


def passes_modifier(
    candidate,
    modifier,
    lookback,
):
    min_atr_ratio = (
        modifier[
            "min_atr_ratio_50"
        ]
    )

    if min_atr_ratio is not None:
        if (
            candidate[
                "atr_ratio_50"
            ] is None
            or candidate[
                "atr_ratio_50"
            ] < min_atr_ratio
        ):
            return False

    min_slope = (
        modifier[
            "min_ema20_slope_12h_atr"
        ]
    )

    if min_slope is not None:
        if (
            candidate[
                "ema20_slope_12h_atr"
            ] is None
            or candidate[
                "ema20_slope_12h_atr"
            ] < min_slope
        ):
            return False

    max_bars = (
        modifier[
            "max_bars_since_structure_high"
        ]
    )

    if max_bars is not None:
        bars_since = (
            candidate[
                "bars_since_high"
            ][lookback]
        )

        if (
            bars_since is None
            or bars_since > max_bars
        ):
            return False

    return True


# ============================================================
# RESEARCH
# ============================================================

def run_research():
    global STATUS

    try:
        print()
        print("=" * 78)
        print(
            "EUR/GBP SHORT - DUAL-BRANCH FREQUENCY RECOVERY"
        )
        print("=" * 78)
        print(
            f"Geometry sets: {GEOMETRY_TESTS}"
        )
        print(
            f"Modifiers: {len(MODIFIERS)}"
        )
        print(
            f"Branches: {len(BRANCHES)}"
        )
        print(
            f"Total tests: {TOTAL_TESTS}"
        )
        print()

        STATUS.update({
            "state": "fetching_data",
            "message": (
                "Fetching EUR/GBP OANDA H1 history"
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

        if not h1:
            raise RuntimeError(
                "No EUR/GBP H1 candles returned"
            )

        STATUS.update({
            "state": "precomputing",
            "message": (
                "Building ATR14, EMA20 and candidate features"
            ),
        })

        h1_atr = atr_series(
            h1,
            14,
        )

        atr_mean_50 = (
            rolling_mean_optional(
                h1_atr,
                50,
            )
        )

        closes = [
            candle[
                "close"
            ]
            for candle in h1
        ]

        ema20 = ema_series(
            closes,
            20,
        )

        raw_candidates = (
            build_candidates(
                h1,
                h1_atr,
                atr_mean_50,
                ema20,
            )
        )

        STATUS[
            "raw_bearish_engulfing_signals"
        ] = len(
            raw_candidates
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
            "state": "running",
            "message": (
                "Running dual-branch frequency-recovery scan"
            ),
        })

        rows = []
        completed = 0

        for branch in BRANCHES:
            print(
                f"Starting branch: {branch['branch']}",
                flush=True,
            )

            branch_candidates = [
                candidate
                for candidate
                in raw_candidates
                if passes_branch(
                    candidate,
                    branch,
                )
            ]

            for lookback in (
                STRUCTURE_LOOKBACKS
            ):
                for max_distance_atr in (
                    MAX_DISTANCE_ATR_VALUES
                ):
                    for min_range_atr in (
                        MIN_RANGE_ATR_VALUES
                    ):
                        for max_close_location in (
                            MAX_CLOSE_LOCATION_VALUES
                        ):

                            geometry_candidates = [
                                candidate
                                for candidate
                                in branch_candidates
                                if (
                                    candidate[
                                        "structure"
                                    ][lookback]
                                    <= max_distance_atr
                                    and candidate[
                                        "range_atr"
                                    ]
                                    >= min_range_atr
                                    and candidate[
                                        "close_location"
                                    ]
                                    <= max_close_location
                                )
                            ]

                            for modifier in (
                                MODIFIERS
                            ):
                                eligible = [
                                    candidate
                                    for candidate
                                    in geometry_candidates
                                    if passes_modifier(
                                        candidate,
                                        modifier,
                                        lookback,
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

                                rows.append(
                                    make_result_row(
                                        branch,
                                        modifier,
                                        lookback,
                                        max_distance_atr,
                                        min_range_atr,
                                        max_close_location,
                                        raw_candidates,
                                        eligible,
                                        trades,
                                        ignored,
                                        still_open,
                                        years,
                                    )
                                )

                                completed += 1

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

        df = pd.DataFrame(
            rows
        )

        if df.empty:
            raise RuntimeError(
                "No result rows generated"
            )

        df = df.sort_values(
            by=[
                "all_four_eras_profitable",
                "adequate_100_trades",
                "frequency_45py",
                "frequency_4py",
                "worst_era_pf_140",
                "worst_era_pf_130",
                "worst_era_pf_120",
                "dd_better_than_8r",
                "pf_180",
                "pf_170",
                "pf_160",
                "pf_150",
                "minimum_era_pf_5_plus",
                "profit_factor",
                "expectancy_r",
                "annual_r_linear",
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
                False,
                False,
                False,
            ],
        )

        df.to_csv(
            OUTPUT_FILE,
            index=False,
        )

        robust_df = df[
            df[
                "branch"
            ] == "ROBUST"
        ]

        high_pf_df = df[
            df[
                "branch"
            ] == "HIGH_PF"
        ]

        STATUS.update({
            "state": "complete",
            "message": (
                "EUR/GBP dual-branch frequency recovery "
                "completed successfully"
            ),
            "completed_tests": TOTAL_TESTS,
            "rows_saved": len(
                df
            ),
            "raw_bearish_engulfing_signals": (
                len(raw_candidates)
            ),
            "robust_all_four_eras_count": int(
                robust_df[
                    "all_four_eras_profitable"
                ].sum()
            ),
            "high_pf_all_four_eras_count": int(
                high_pf_df[
                    "all_four_eras_profitable"
                ].sum()
            ),
            "robust_100_trades_pf150_count": int(
                (
                    robust_df[
                        "adequate_100_trades"
                    ]
                    & robust_df[
                        "pf_150"
                    ]
                    & robust_df[
                        "all_four_eras_profitable"
                    ]
                ).sum()
            ),
            "high_pf_100_trades_pf170_count": int(
                (
                    high_pf_df[
                        "adequate_100_trades"
                    ]
                    & high_pf_df[
                        "pf_170"
                    ]
                    & high_pf_df[
                        "all_four_eras_profitable"
                    ]
                ).sum()
            ),
            "output_file": (
                OUTPUT_FILE
            ),
        })

        print()
        print("=" * 78)
        print(
            "EUR/GBP DUAL-BRANCH FREQUENCY RECOVERY COMPLETE"
        )
        print("=" * 78)
        print(
            "Rows:",
            len(df),
        )
        print(
            "Robust all-four-era rows:",
            int(
                robust_df[
                    "all_four_eras_profitable"
                ].sum()
            ),
        )
        print(
            "High-PF all-four-era rows:",
            int(
                high_pf_df[
                    "all_four_eras_profitable"
                ].sum()
            ),
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
            "EURGBP Short Dual-Branch Frequency Recovery"
        ),
        "status": STATUS,
        "instrument": INSTRUMENT,
        "direction": "SHORT",
        "reward_risk": REWARD_RISK,
        "trading_enabled": False,
        "orders_supported": False,
        "executor_connected": False,

        "branches": BRANCHES,

        "geometry": {
            "structure_lookbacks": (
                STRUCTURE_LOOKBACKS
            ),
            "max_distance_atr_values": (
                MAX_DISTANCE_ATR_VALUES
            ),
            "min_range_atr_values": (
                MIN_RANGE_ATR_VALUES
            ),
            "max_close_location_values": (
                MAX_CLOSE_LOCATION_VALUES
            ),
        },

        "modifiers": MODIFIERS,

        "total_tests": TOTAL_TESTS,
        "download": "/download",
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
                "EUR/GBP dual-branch CSV "
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
            "eurgbp-short-dual-branch-frequency-recovery"
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
