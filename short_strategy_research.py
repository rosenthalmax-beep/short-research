import os
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# EUR/GBP SHORT - POST-NY09 ROBUSTNESS NEIGHBOURHOOD
#
# RESEARCH ONLY — NEVER SUBMITS ORDERS.
#
# Purpose:
#   Tight robustness test around the two improved EUR/GBP
#   candidates found after excluding NY hour 09 and recovering
#   frequency.
#
# Timing:
#   EXCLUDE signal candles opening in NY hour 09.
#
# RR:
#   Fixed at 3.00.
#
# Shared execution:
#   - OANDA EUR_GBP
#   - H1 midpoint candles
#   - bearish engulfing
#   - body ratio >= 1.00
#   - stop = signal high + 10 ticks
#   - adverse short slippage = 5 ticks
#   - pyramiding = 0
#   - same-bar target/stop tie:
#       compare open->high vs open->low
#       high closer = stop first
#
# ============================================================
# BRANCH A — ROBUST / STRUCTURE-RELAXED
#
# Current centre:
#   structure lookback = 90
#   distance <= 0.15 ATR14
#   range >= 1.10 ATR14
#   close location <= 0.20
#   12h upward momentum >= 0.25 ATR14
#   48h upward momentum >= 0.50 ATR14
#   stop size <= 2.50 ATR14
#   exclude NY09
#
# Sweep:
#   structure lookback:
#       80, 90, 100
#
#   distance:
#       0.10, 0.125, 0.15, 0.175, 0.20
#
#   range:
#       1.05, 1.10, 1.15
#
#   close location:
#       0.175, 0.20, 0.225
#
#   12h momentum:
#       0.20, 0.25, 0.30
#
#   48h momentum:
#       0.40, 0.50, 0.60
#
# Stop cap fixed:
#       <= 2.50 ATR14
#
# ============================================================
# BRANCH B — HIGH-PF / RANGE-RELAXED
#
# Current centre:
#   structure lookback = 90
#   distance <= 0.075 ATR14
#   range >= 0.95 ATR14
#   close location <= 0.20
#   48h upward momentum >= 1.00 ATR14
#   upper wick/body >= 0.10
#   ATR14 / 50-bar ATR14 mean >= 0.80
#   exclude NY09
#
# Sweep:
#   structure lookback:
#       80, 90, 100
#
#   distance:
#       0.05, 0.075, 0.10
#
#   range:
#       0.90, 0.95, 1.00, 1.05
#
#   close location:
#       0.175, 0.20, 0.225
#
#   48h momentum:
#       0.90, 1.00, 1.10
#
#   upper wick/body:
#       0.05, 0.10, 0.15
#
# ATR regime fixed:
#       ATR14 / mean ATR14(50) >= 0.80
#
# Output:
#   eurgbp_short_post_ny09_robustness_neighbourhood.csv
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
    "eurgbp_short_post_ny09_robustness_neighbourhood.csv"
)


# ============================================================
# BRANCH A GRID
# ============================================================

ROBUST_STRUCTURE_LOOKBACKS = [
    80,
    90,
    100,
]

ROBUST_MAX_DISTANCE_VALUES = [
    0.10,
    0.125,
    0.15,
    0.175,
    0.20,
]

ROBUST_MIN_RANGE_VALUES = [
    1.05,
    1.10,
    1.15,
]

ROBUST_MAX_CLOSE_VALUES = [
    0.175,
    0.20,
    0.225,
]

ROBUST_MOM12_VALUES = [
    0.20,
    0.25,
    0.30,
]

ROBUST_MOM48_VALUES = [
    0.40,
    0.50,
    0.60,
]

ROBUST_MAX_STOP_SIZE_ATR = 2.50


# ============================================================
# BRANCH B GRID
# ============================================================

HIGH_PF_STRUCTURE_LOOKBACKS = [
    80,
    90,
    100,
]

HIGH_PF_MAX_DISTANCE_VALUES = [
    0.05,
    0.075,
    0.10,
]

HIGH_PF_MIN_RANGE_VALUES = [
    0.90,
    0.95,
    1.00,
    1.05,
]

HIGH_PF_MAX_CLOSE_VALUES = [
    0.175,
    0.20,
    0.225,
]

HIGH_PF_MOM48_VALUES = [
    0.90,
    1.00,
    1.10,
]

HIGH_PF_UPPER_WICK_VALUES = [
    0.05,
    0.10,
    0.15,
]

HIGH_PF_MIN_ATR_RATIO_50 = 0.80


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

ROBUST_TESTS = (
    len(ROBUST_STRUCTURE_LOOKBACKS)
    * len(ROBUST_MAX_DISTANCE_VALUES)
    * len(ROBUST_MIN_RANGE_VALUES)
    * len(ROBUST_MAX_CLOSE_VALUES)
    * len(ROBUST_MOM12_VALUES)
    * len(ROBUST_MOM48_VALUES)
)

HIGH_PF_TESTS = (
    len(HIGH_PF_STRUCTURE_LOOKBACKS)
    * len(HIGH_PF_MAX_DISTANCE_VALUES)
    * len(HIGH_PF_MIN_RANGE_VALUES)
    * len(HIGH_PF_MAX_CLOSE_VALUES)
    * len(HIGH_PF_MOM48_VALUES)
    * len(HIGH_PF_UPPER_WICK_VALUES)
)

TOTAL_TESTS = ROBUST_TESTS + HIGH_PF_TESTS

STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "service": "EURGBP Short Post-NY09 Robustness Neighbourhood",
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "reward_risk": REWARD_RISK,
    "excluded_ny_hours": sorted(EXCLUDED_NY_HOURS),
    "robust_tests": ROBUST_TESTS,
    "high_pf_tests": HIGH_PF_TESTS,
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

        result.append(tr)

    return result


def rma_series(
    values,
    length,
):
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
        true_ranges(candles),
        length,
    )


def rolling_mean_optional(
    values,
    length,
):
    result = [None] * len(values)

    for index in range(
        length - 1,
        len(values),
    ):
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

def build_raw_candidates(
    h1,
    h1_atr,
    atr_mean_50,
):
    candidates = []

    max_structure = max(
        max(
            ROBUST_STRUCTURE_LOOKBACKS
        ),
        max(
            HIGH_PF_STRUCTURE_LOOKBACKS
        ),
    )

    max_lookback = max(
        max_structure,
        48,
        50,
    )

    all_structure_lookbacks = sorted(
        set(
            ROBUST_STRUCTURE_LOOKBACKS
            + HIGH_PF_STRUCTURE_LOOKBACKS
        )
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

        previous = h1[
            index - 1
        ]

        atr = h1_atr[
            index
        ]

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

        if (
            body_ratio
            < MIN_BODY_RATIO
        ):
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

        structure_distances = {}

        for lookback in (
            all_structure_lookbacks
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
                previous_highest
                - signal["high"]
            ) / atr

        ny_time = (
            signal["time"]
            .astimezone(NY_TZ)
        )

        candidates.append({
            "index": index,
            "time": signal["time"],
            "ny_hour": ny_time.hour,
            "range_atr": range_atr,
            "close_location": close_location,
            "momentum_12": momentum_12,
            "momentum_48": momentum_48,
            "upper_wick_body": upper_wick_body,
            "stop_size_atr": stop_size_atr,
            "atr_ratio_50": atr_ratio_50,
            "structure": structure_distances,
        })

    return candidates


# ============================================================
# FILTERS
# ============================================================

def passes_robust(
    candidate,
    structure_lookback,
    max_distance_atr,
    min_range_atr,
    max_close_location,
    min_momentum_12,
    min_momentum_48,
):
    if (
        candidate[
            "ny_hour"
        ] in EXCLUDED_NY_HOURS
    ):
        return False

    if (
        candidate[
            "structure"
        ][structure_lookback]
        > max_distance_atr
    ):
        return False

    if (
        candidate[
            "range_atr"
        ] < min_range_atr
    ):
        return False

    if (
        candidate[
            "close_location"
        ] > max_close_location
    ):
        return False

    if (
        candidate[
            "momentum_12"
        ] < min_momentum_12
    ):
        return False

    if (
        candidate[
            "momentum_48"
        ] < min_momentum_48
    ):
        return False

    if (
        candidate[
            "stop_size_atr"
        ] > ROBUST_MAX_STOP_SIZE_ATR
    ):
        return False

    return True


def passes_high_pf(
    candidate,
    structure_lookback,
    max_distance_atr,
    min_range_atr,
    max_close_location,
    min_momentum_48,
    min_upper_wick_body,
):
    if (
        candidate[
            "ny_hour"
        ] in EXCLUDED_NY_HOURS
    ):
        return False

    if (
        candidate[
            "structure"
        ][structure_lookback]
        > max_distance_atr
    ):
        return False

    if (
        candidate[
            "range_atr"
        ] < min_range_atr
    ):
        return False

    if (
        candidate[
            "close_location"
        ] > max_close_location
    ):
        return False

    if (
        candidate[
            "momentum_48"
        ] < min_momentum_48
    ):
        return False

    if (
        candidate[
            "upper_wick_body"
        ] < min_upper_wick_body
    ):
        return False

    if (
        candidate[
            "atr_ratio_50"
        ] is None
        or candidate[
            "atr_ratio_50"
        ] < HIGH_PF_MIN_ATR_RATIO_50
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
        candle = h1[index]

        if (
            candle[
                "time"
            ] >= RESEARCH_TO
        ):
            break

        stop_hit = (
            candle[
                "high"
            ] >= stop
        )

        target_hit = (
            candle[
                "low"
            ] <= target
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
                candle[
                    "high"
                ]
                - candle[
                    "open"
                ]
            )

            distance_to_low = abs(
                candle[
                    "open"
                ]
                - candle[
                    "low"
                ]
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
            "signal_time": signal[
                "time"
            ],
            "exit_index": index,
            "exit_time": candle[
                "time"
            ],
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
        "signal_time": signal[
            "time"
        ],
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

    for candidate in (
        eligible
    ):
        signal_index = (
            candidate[
                "index"
            ]
        )

        # Locked convention:
        # a signal on the exact H1 candle where
        # the previous position exits is allowed.
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

    for trade in trades:
        signal_time = (
            trade[
                "signal_time"
            ]
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
        trade[
            "result_r"
        ]
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
        sum(
            losers
        )
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
# RESULT ROW
# ============================================================

def make_result_row(
    branch_name,
    parameters,
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
        "branch": branch_name,
        "reward_risk": REWARD_RISK,
        "excluded_ny_hours": "09",
        "min_body_ratio": MIN_BODY_RATIO,

        "structure_lookback": (
            parameters[
                "structure_lookback"
            ]
        ),
        "max_distance_atr": (
            parameters[
                "max_distance_atr"
            ]
        ),
        "min_range_atr": (
            parameters[
                "min_range_atr"
            ]
        ),
        "max_close_location": (
            parameters[
                "max_close_location"
            ]
        ),
        "min_momentum_12h_atr": (
            parameters.get(
                "min_momentum_12"
            )
        ),
        "min_momentum_48h_atr": (
            parameters.get(
                "min_momentum_48"
            )
        ),
        "max_stop_size_atr": (
            parameters.get(
                "max_stop_size_atr"
            )
        ),
        "min_upper_wick_body": (
            parameters.get(
                "min_upper_wick_body"
            )
        ),
        "min_atr_ratio_50": (
            parameters.get(
                "min_atr_ratio_50"
            )
        ),

        "raw_signals": (
            len(
                raw_candidates
            )
        ),
        "eligible_signals": (
            len(
                eligible
            )
        ),
        "signal_retention_pct": round(
            (
                len(
                    eligible
                )
                / len(
                    raw_candidates
                )
                * 100.0
            )
            if raw_candidates
            else 0.0,
            2,
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
    }

    profitable_eras = 0
    minimum_era_pf = None
    minimum_era_expectancy = None

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

        if (
            era[
                "trades"
            ] >= 5
        ):
            if (
                era[
                    "total_r"
                ] > 0
            ):
                profitable_eras += 1

            if (
                minimum_era_pf
                is None
            ):
                minimum_era_pf = (
                    era[
                        "profit_factor"
                    ]
                )
            else:
                minimum_era_pf = min(
                    minimum_era_pf,
                    era[
                        "profit_factor"
                    ],
                )

            if (
                minimum_era_expectancy
                is None
            ):
                minimum_era_expectancy = (
                    era[
                        "expectancy_r"
                    ]
                )
            else:
                minimum_era_expectancy = min(
                    minimum_era_expectancy,
                    era[
                        "expectancy_r"
                    ],
                )

    row[
        "profitable_eras_with_5_plus_trades"
    ] = profitable_eras

    row[
        "minimum_era_pf_5_plus"
    ] = minimum_era_pf

    row[
        "minimum_era_expectancy_5_plus"
    ] = minimum_era_expectancy

    row[
        "all_four_eras_profitable"
    ] = (
        profitable_eras >= 4
    )

    row[
        "adequate_90_trades"
    ] = (
        full[
            "trades"
        ] >= 90
    )

    row[
        "adequate_100_trades"
    ] = (
        full[
            "trades"
        ] >= 100
    )

    row[
        "frequency_4py"
    ] = (
        full[
            "trades"
        ]
        / years
        >= 4.0
    )

    row[
        "frequency_45py"
    ] = (
        full[
            "trades"
        ]
        / years
        >= 4.5
    )

    row[
        "worst_era_pf_120"
    ] = (
        minimum_era_pf is not None
        and minimum_era_pf >= 1.20
    )

    row[
        "worst_era_pf_130"
    ] = (
        minimum_era_pf is not None
        and minimum_era_pf >= 1.30
    )

    row[
        "worst_era_pf_140"
    ] = (
        minimum_era_pf is not None
        and minimum_era_pf >= 1.40
    )

    row[
        "worst_era_pf_150"
    ] = (
        minimum_era_pf is not None
        and minimum_era_pf >= 1.50
    )

    row[
        "pf_160"
    ] = (
        full[
            "profit_factor"
        ] >= 1.60
    )

    row[
        "pf_180"
    ] = (
        full[
            "profit_factor"
        ] >= 1.80
    )

    row[
        "pf_200"
    ] = (
        full[
            "profit_factor"
        ] >= 2.00
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
            ]
            / years
        ),
        3,
    )

    return row


# ============================================================
# RESEARCH
# ============================================================

def run_research():
    global STATUS

    try:
        print()
        print("=" * 86)
        print(
            "EUR/GBP SHORT - POST-NY09 ROBUSTNESS NEIGHBOURHOOD"
        )
        print("=" * 86)
        print(
            f"ROBUST tests: {ROBUST_TESTS}"
        )
        print(
            f"HIGH_PF tests: {HIGH_PF_TESTS}"
        )
        print(
            f"TOTAL tests: {TOTAL_TESTS}"
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
                "Building ATR14 and raw bearish-engulfing features"
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

        raw_candidates = (
            build_raw_candidates(
                h1,
                h1_atr,
                atr_mean_50,
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
                "Running post-NY09 robustness neighbourhood"
            ),
        })

        rows = []
        completed = 0

        # ====================================================
        # ROBUST BRANCH
        # ====================================================

        for structure_lookback in (
            ROBUST_STRUCTURE_LOOKBACKS
        ):
            for max_distance_atr in (
                ROBUST_MAX_DISTANCE_VALUES
            ):
                for min_range_atr in (
                    ROBUST_MIN_RANGE_VALUES
                ):
                    for max_close_location in (
                        ROBUST_MAX_CLOSE_VALUES
                    ):
                        for min_momentum_12 in (
                            ROBUST_MOM12_VALUES
                        ):
                            for min_momentum_48 in (
                                ROBUST_MOM48_VALUES
                            ):
                                eligible = [
                                    candidate
                                    for candidate
                                    in raw_candidates
                                    if passes_robust(
                                        candidate,
                                        structure_lookback,
                                        max_distance_atr,
                                        min_range_atr,
                                        max_close_location,
                                        min_momentum_12,
                                        min_momentum_48,
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

                                params = {
                                    "structure_lookback": (
                                        structure_lookback
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
                                    "min_momentum_12": (
                                        min_momentum_12
                                    ),
                                    "min_momentum_48": (
                                        min_momentum_48
                                    ),
                                    "max_stop_size_atr": (
                                        ROBUST_MAX_STOP_SIZE_ATR
                                    ),
                                    "min_upper_wick_body": None,
                                    "min_atr_ratio_50": None,
                                }

                                rows.append(
                                    make_result_row(
                                        "ROBUST",
                                        params,
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
                                ):
                                    print(
                                        f"{completed}/{TOTAL_TESTS}",
                                        flush=True,
                                    )

        # ====================================================
        # HIGH-PF BRANCH
        # ====================================================

        for structure_lookback in (
            HIGH_PF_STRUCTURE_LOOKBACKS
        ):
            for max_distance_atr in (
                HIGH_PF_MAX_DISTANCE_VALUES
            ):
                for min_range_atr in (
                    HIGH_PF_MIN_RANGE_VALUES
                ):
                    for max_close_location in (
                        HIGH_PF_MAX_CLOSE_VALUES
                    ):
                        for min_momentum_48 in (
                            HIGH_PF_MOM48_VALUES
                        ):
                            for min_upper_wick_body in (
                                HIGH_PF_UPPER_WICK_VALUES
                            ):
                                eligible = [
                                    candidate
                                    for candidate
                                    in raw_candidates
                                    if passes_high_pf(
                                        candidate,
                                        structure_lookback,
                                        max_distance_atr,
                                        min_range_atr,
                                        max_close_location,
                                        min_momentum_48,
                                        min_upper_wick_body,
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

                                params = {
                                    "structure_lookback": (
                                        structure_lookback
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
                                    "min_momentum_12": None,
                                    "min_momentum_48": (
                                        min_momentum_48
                                    ),
                                    "max_stop_size_atr": None,
                                    "min_upper_wick_body": (
                                        min_upper_wick_body
                                    ),
                                    "min_atr_ratio_50": (
                                        HIGH_PF_MIN_ATR_RATIO_50
                                    ),
                                }

                                rows.append(
                                    make_result_row(
                                        "HIGH_PF",
                                        params,
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
                "branch",
                "all_four_eras_profitable",
                "adequate_100_trades",
                "frequency_4py",
                "worst_era_pf_150",
                "worst_era_pf_140",
                "worst_era_pf_130",
                "worst_era_pf_120",
                "minimum_era_pf_5_plus",
                "pf_200",
                "pf_180",
                "pf_160",
                "profit_factor",
                "expectancy_r",
                "annual_r_linear",
                "trades",
            ],
            ascending=[
                True,
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
                "EUR/GBP post-NY09 robustness neighbourhood "
                "completed successfully"
            ),
            "completed_tests": TOTAL_TESTS,
            "rows_saved": len(
                df
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
            "robust_100trades_worstpf140_count": int(
                (
                    robust_df[
                        "adequate_100_trades"
                    ]
                    & robust_df[
                        "worst_era_pf_140"
                    ]
                ).sum()
            ),
            "highpf_100trades_pf180_count": int(
                (
                    high_pf_df[
                        "adequate_100_trades"
                    ]
                    & high_pf_df[
                        "pf_180"
                    ]
                ).sum()
            ),
            "output_file": (
                OUTPUT_FILE
            ),
        })

        print()
        print("=" * 86)
        print(
            "EUR/GBP POST-NY09 ROBUSTNESS NEIGHBOURHOOD COMPLETE"
        )
        print("=" * 86)
        print(
            "Rows:",
            len(
                df
            ),
        )
        print(
            "Saved:",
            OUTPUT_FILE,
        )

        for branch_name in [
            "ROBUST",
            "HIGH_PF",
        ]:
            subset = df[
                df[
                    "branch"
                ] == branch_name
            ]

            print()
            print(
                f"--- {branch_name} TOP 15 ---"
            )

            print(
                subset[
                    [
                        "structure_lookback",
                        "max_distance_atr",
                        "min_range_atr",
                        "max_close_location",
                        "min_momentum_12h_atr",
                        "min_momentum_48h_atr",
                        "min_upper_wick_body",
                        "trades",
                        "trades_per_year",
                        "profit_factor",
                        "total_r",
                        "expectancy_r",
                        "max_drawdown_r",
                        "minimum_era_pf_5_plus",
                        "2024_present_pf",
                    ]
                ].head(
                    15
                ).to_string(
                    index=False
                ),
                flush=True,
            )

    except Exception as error:
        STATUS.update({
            "state": "error",
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
            "EURGBP Short Post-NY09 Robustness Neighbourhood"
        ),
        "status": STATUS,
        "instrument": INSTRUMENT,
        "direction": "SHORT",
        "reward_risk": REWARD_RISK,
        "excluded_ny_hours": sorted(
            EXCLUDED_NY_HOURS
        ),
        "timezone": (
            "America/New_York"
        ),
        "timing_basis": (
            "signal candle open time"
        ),

        "trading_enabled": False,
        "orders_supported": False,
        "executor_connected": False,

        "robust_grid": {
            "structure_lookbacks": (
                ROBUST_STRUCTURE_LOOKBACKS
            ),
            "max_distance_atr": (
                ROBUST_MAX_DISTANCE_VALUES
            ),
            "min_range_atr": (
                ROBUST_MIN_RANGE_VALUES
            ),
            "max_close_location": (
                ROBUST_MAX_CLOSE_VALUES
            ),
            "momentum_12h": (
                ROBUST_MOM12_VALUES
            ),
            "momentum_48h": (
                ROBUST_MOM48_VALUES
            ),
            "max_stop_size_atr": (
                ROBUST_MAX_STOP_SIZE_ATR
            ),
        },

        "high_pf_grid": {
            "structure_lookbacks": (
                HIGH_PF_STRUCTURE_LOOKBACKS
            ),
            "max_distance_atr": (
                HIGH_PF_MAX_DISTANCE_VALUES
            ),
            "min_range_atr": (
                HIGH_PF_MIN_RANGE_VALUES
            ),
            "max_close_location": (
                HIGH_PF_MAX_CLOSE_VALUES
            ),
            "momentum_48h": (
                HIGH_PF_MOM48_VALUES
            ),
            "upper_wick_body": (
                HIGH_PF_UPPER_WICK_VALUES
            ),
            "min_atr_ratio_50": (
                HIGH_PF_MIN_ATR_RATIO_50
            ),
        },

        "robust_tests": (
            ROBUST_TESTS
        ),
        "high_pf_tests": (
            HIGH_PF_TESTS
        ),
        "total_tests": (
            TOTAL_TESTS
        ),

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
    if not os.path.exists(
        OUTPUT_FILE
    ):
        return jsonify({
            "status": "not_ready",
            "message": (
                "EUR/GBP post-NY09 robustness CSV "
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
            "eurgbp-short-post-ny09-robustness-neighbourhood"
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
