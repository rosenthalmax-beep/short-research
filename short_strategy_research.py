import os
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# EUR/GBP SHORT - DUAL-BRANCH HOUR QUALITY DIAGNOSTIC
#
# RESEARCH ONLY — NEVER SUBMITS ORDERS.
#
# Purpose:
#   Diagnose the quality of EVERY New York signal hour for BOTH
#   frozen EUR/GBP short branches before deciding which hours,
#   if any, deserve exclusion.
#
# For each branch + NY hour:
#   A) Stats of trades whose signal candle opened in that hour
#   B) Era-by-era stats for those hour-specific trades
#   C) Full-strategy stats if that single hour is excluded
#   D) Deltas versus untouched branch baseline
#
# This is NOT an optimisation pass.
# It is a diagnostic table for selecting only hours that:
#   - have a meaningful sample,
#   - show consistently poor behaviour,
#   - and ideally hurt more than one era.
#
# Shared frozen geometry:
#   bearish engulfing
#   body ratio >= 1.00
#   structure lookback = 90
#   distance <= 0.075 ATR14
#   range >= 1.10 ATR14
#   close location <= 0.20
#   RR = 3.00
#   stop = signal high + 10 ticks
#   adverse short slippage = 5 ticks
#   pyramiding = 0
#
# ROBUST branch:
#   12h upward momentum >= 0.25 ATR14
#   48h upward momentum >= 0.50 ATR14
#   stop size <= 2.50 ATR14
#
# HIGH_PF branch:
#   48h upward momentum >= 1.00 ATR14
#   upper wick/body >= 0.10
#   ATR14 / 50-bar ATR14 mean >= 0.80
#
# Timing convention:
#   signal candle OPEN time converted to America/New_York.
#
# Output:
#   eurgbp_short_dual_branch_hour_quality.csv
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
MAX_DISTANCE_ATR = 0.075
MIN_RANGE_ATR = 1.10
MAX_CLOSE_LOCATION = 0.20

NY_TZ = ZoneInfo("America/New_York")

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

OUTPUT_FILE = "eurgbp_short_dual_branch_hour_quality.csv"


# ============================================================
# BRANCHES
# ============================================================

BRANCHES = [
    {
        "branch": "ROBUST",
        "min_momentum_12": 0.25,
        "min_momentum_48": 0.50,
        "min_upper_wick_body": None,
        "max_stop_size_atr": 2.50,
        "min_atr_ratio_50": None,
    },
    {
        "branch": "HIGH_PF",
        "min_momentum_12": None,
        "min_momentum_48": 1.00,
        "min_upper_wick_body": 0.10,
        "max_stop_size_atr": None,
        "min_atr_ratio_50": 0.80,
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

TOTAL_TESTS = len(BRANCHES) * 24

STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "service": "EURGBP Short Dual-Branch Hour Quality",
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "reward_risk": REWARD_RISK,
    "branches": len(BRANCHES),
    "hours_per_branch": 24,
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
                * (length - 1)
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
# CANDIDATES
# ============================================================

def build_candidates(
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

        if body_ratio < MIN_BODY_RATIO:
            continue

        range_atr = (
            candle_range
            / atr
        )

        if (
            range_atr
            < MIN_RANGE_ATR
        ):
            continue

        close_location = (
            signal["close"]
            - signal["low"]
        ) / candle_range

        if (
            close_location
            > MAX_CLOSE_LOCATION
        ):
            continue

        previous_highest = max(
            candle["high"]
            for candle in h1[
                index - STRUCTURE_LOOKBACK:
                index
            ]
        )

        structure_distance_atr = (
            previous_highest
            - signal["high"]
        ) / atr

        if (
            structure_distance_atr
            > MAX_DISTANCE_ATR
        ):
            continue

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

        ny_time = (
            signal["time"]
            .astimezone(NY_TZ)
        )

        candidates.append({
            "index": index,
            "time": signal["time"],
            "ny_hour": ny_time.hour,
            "momentum_12": momentum_12,
            "momentum_48": momentum_48,
            "upper_wick_body": upper_wick_body,
            "stop_size_atr": stop_size_atr,
            "atr_ratio_50": atr_ratio_50,
        })

    return candidates


def passes_branch(
    candidate,
    branch,
):
    value = branch[
        "min_momentum_12"
    ]

    if (
        value is not None
        and candidate[
            "momentum_12"
        ] < value
    ):
        return False

    value = branch[
        "min_momentum_48"
    ]

    if (
        value is not None
        and candidate[
            "momentum_48"
        ] < value
    ):
        return False

    value = branch[
        "min_upper_wick_body"
    ]

    if (
        value is not None
        and candidate[
            "upper_wick_body"
        ] < value
    ):
        return False

    value = branch[
        "max_stop_size_atr"
    ]

    if (
        value is not None
        and candidate[
            "stop_size_atr"
        ] > value
    ):
        return False

    value = branch[
        "min_atr_ratio_50"
    ]

    if value is not None:
        if (
            candidate[
                "atr_ratio_50"
            ] is None
            or candidate[
                "atr_ratio_50"
            ] < value
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
    eligible_candidates,
):
    trades = []
    position_exit_index = -1
    ignored = 0
    still_open = False

    for candidate in (
        eligible_candidates
    ):
        signal_index = (
            candidate[
                "index"
            ]
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

        if (
            trade[
                "status"
            ] == "OPEN"
        ):
            still_open = True
            break

        trade = dict(
            trade
        )

        trade[
            "ny_hour"
        ] = candidate[
            "ny_hour"
        ]

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


def add_era_stats(
    row,
    prefix,
    trades,
):
    positive_eras = 0
    losing_eras = 0
    era_count_with_trades = 0

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
            f"{prefix}_{era_name}_trades"
        ] = era[
            "trades"
        ]

        row[
            f"{prefix}_{era_name}_pf"
        ] = era[
            "profit_factor"
        ]

        row[
            f"{prefix}_{era_name}_r"
        ] = era[
            "total_r"
        ]

        row[
            f"{prefix}_{era_name}_expectancy"
        ] = era[
            "expectancy_r"
        ]

        if (
            era[
                "trades"
            ] > 0
        ):
            era_count_with_trades += 1

            if (
                era[
                    "total_r"
                ] > 0
            ):
                positive_eras += 1

            if (
                era[
                    "total_r"
                ] < 0
            ):
                losing_eras += 1

    row[
        f"{prefix}_eras_with_trades"
    ] = era_count_with_trades

    row[
        f"{prefix}_positive_eras"
    ] = positive_eras

    row[
        f"{prefix}_losing_eras"
    ] = losing_eras


# ============================================================
# DIAGNOSTIC ROW
# ============================================================

def build_hour_row(
    branch,
    hour,
    baseline_trades,
    excluded_trades,
    years,
):
    hour_trades = [
        trade
        for trade in baseline_trades
        if trade[
            "ny_hour"
        ] == hour
    ]

    hour_stats = stats_for_trades(
        hour_trades
    )

    baseline_stats = stats_for_trades(
        baseline_trades
    )

    excluded_stats = stats_for_trades(
        excluded_trades
    )

    row = {
        "branch": branch[
            "branch"
        ],
        "ny_hour": hour,
        "ny_hour_label": (
            f"{hour:02d}:00-{hour:02d}:59"
        ),

        "reward_risk": REWARD_RISK,

        "structure_lookback": (
            STRUCTURE_LOOKBACK
        ),
        "max_distance_atr": (
            MAX_DISTANCE_ATR
        ),
        "min_range_atr": (
            MIN_RANGE_ATR
        ),
        "max_close_location": (
            MAX_CLOSE_LOCATION
        ),
        "min_body_ratio": (
            MIN_BODY_RATIO
        ),

        "min_momentum_12h_atr": (
            branch[
                "min_momentum_12"
            ]
        ),
        "min_momentum_48h_atr": (
            branch[
                "min_momentum_48"
            ]
        ),
        "min_upper_wick_body": (
            branch[
                "min_upper_wick_body"
            ]
        ),
        "max_stop_size_atr": (
            branch[
                "max_stop_size_atr"
            ]
        ),
        "min_atr_ratio_50": (
            branch[
                "min_atr_ratio_50"
            ]
        ),

        # Baseline
        "baseline_trades": (
            baseline_stats[
                "trades"
            ]
        ),
        "baseline_trades_per_year": round(
            baseline_stats[
                "trades"
            ]
            / years,
            2,
        ),
        "baseline_profit_factor": (
            baseline_stats[
                "profit_factor"
            ]
        ),
        "baseline_total_r": (
            baseline_stats[
                "total_r"
            ]
        ),
        "baseline_expectancy_r": (
            baseline_stats[
                "expectancy_r"
            ]
        ),
        "baseline_max_drawdown_r": (
            baseline_stats[
                "max_drawdown_r"
            ]
        ),

        # Hour-specific quality
        "hour_trades": (
            hour_stats[
                "trades"
            ]
        ),
        "hour_share_pct": round(
            (
                hour_stats[
                    "trades"
                ]
                / baseline_stats[
                    "trades"
                ]
                * 100.0
            )
            if baseline_stats[
                "trades"
            ] else 0.0,
            2,
        ),
        "hour_winners": (
            hour_stats[
                "winners"
            ]
        ),
        "hour_losers": (
            hour_stats[
                "losers"
            ]
        ),
        "hour_win_rate": (
            hour_stats[
                "win_rate"
            ]
        ),
        "hour_profit_factor": (
            hour_stats[
                "profit_factor"
            ]
        ),
        "hour_total_r": (
            hour_stats[
                "total_r"
            ]
        ),
        "hour_expectancy_r": (
            hour_stats[
                "expectancy_r"
            ]
        ),
        "hour_max_drawdown_r": (
            hour_stats[
                "max_drawdown_r"
            ]
        ),
        "hour_longest_loss_streak": (
            hour_stats[
                "longest_loss_streak"
            ]
        ),

        # Strategy after excluding hour
        "exclude_hour_trades": (
            excluded_stats[
                "trades"
            ]
        ),
        "exclude_hour_trades_per_year": round(
            excluded_stats[
                "trades"
            ]
            / years,
            2,
        ),
        "exclude_hour_profit_factor": (
            excluded_stats[
                "profit_factor"
            ]
        ),
        "exclude_hour_total_r": (
            excluded_stats[
                "total_r"
            ]
        ),
        "exclude_hour_expectancy_r": (
            excluded_stats[
                "expectancy_r"
            ]
        ),
        "exclude_hour_max_drawdown_r": (
            excluded_stats[
                "max_drawdown_r"
            ]
        ),
        "exclude_hour_longest_loss_streak": (
            excluded_stats[
                "longest_loss_streak"
            ]
        ),

        # Direct deltas
        "trades_removed_if_excluded": (
            baseline_stats[
                "trades"
            ]
            - excluded_stats[
                "trades"
            ]
        ),
        "delta_pf_if_excluded": round(
            excluded_stats[
                "profit_factor"
            ]
            - baseline_stats[
                "profit_factor"
            ],
            3,
        ),
        "delta_total_r_if_excluded": round(
            excluded_stats[
                "total_r"
            ]
            - baseline_stats[
                "total_r"
            ],
            2,
        ),
        "delta_expectancy_if_excluded": round(
            excluded_stats[
                "expectancy_r"
            ]
            - baseline_stats[
                "expectancy_r"
            ],
            3,
        ),
        "delta_max_dd_if_excluded": round(
            excluded_stats[
                "max_drawdown_r"
            ]
            - baseline_stats[
                "max_drawdown_r"
            ],
            2,
        ),
    }

    add_era_stats(
        row,
        "hour",
        hour_trades,
    )

    add_era_stats(
        row,
        "excluded_strategy",
        excluded_trades,
    )

    # Simple diagnostic flags, not selection rules.
    row[
        "hour_sample_ge_5"
    ] = (
        hour_stats[
            "trades"
        ] >= 5
    )

    row[
        "hour_sample_ge_8"
    ] = (
        hour_stats[
            "trades"
        ] >= 8
    )

    row[
        "hour_sample_ge_10"
    ] = (
        hour_stats[
            "trades"
        ] >= 10
    )

    row[
        "hour_negative_expectancy"
    ] = (
        hour_stats[
            "trades"
        ] > 0
        and hour_stats[
            "expectancy_r"
        ] < 0
    )

    row[
        "hour_pf_below_1"
    ] = (
        hour_stats[
            "trades"
        ] > 0
        and hour_stats[
            "profit_factor"
        ] < 1.0
    )

    row[
        "hour_loses_in_2plus_eras"
    ] = (
        row[
            "hour_losing_eras"
        ] >= 2
    )

    row[
        "hour_loses_in_3plus_eras"
    ] = (
        row[
            "hour_losing_eras"
        ] >= 3
    )

    row[
        "exclusion_improves_pf"
    ] = (
        row[
            "delta_pf_if_excluded"
        ] > 0
    )

    row[
        "exclusion_improves_expectancy"
    ] = (
        row[
            "delta_expectancy_if_excluded"
        ] > 0
    )

    row[
        "exclusion_improves_dd"
    ] = (
        row[
            "delta_max_dd_if_excluded"
        ] > 0
    )

    row[
        "exclusion_keeps_4py"
    ] = (
        row[
            "exclude_hour_trades_per_year"
        ] >= 4.0
    )

    return row


# ============================================================
# RESEARCH
# ============================================================

def run_research():
    global STATUS

    try:
        print()
        print("=" * 82)
        print(
            "EUR/GBP SHORT - DUAL-BRANCH HOUR QUALITY DIAGNOSTIC"
        )
        print("=" * 82)
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
                "Building ATR14 and frozen branch candidates"
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

        base_candidates = (
            build_candidates(
                h1,
                h1_atr,
                atr_mean_50,
            )
        )

        STATUS[
            "shared_geometry_signals"
        ] = len(
            base_candidates
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
                "Profiling NY hour quality for both branches"
            ),
        })

        for branch in BRANCHES:
            frozen_candidates = [
                candidate
                for candidate
                in base_candidates
                if passes_branch(
                    candidate,
                    branch,
                )
            ]

            baseline_trades, baseline_ignored, baseline_open = simulate(
                h1,
                frozen_candidates,
            )

            STATUS[
                f"{branch['branch'].lower()}_signals"
            ] = len(
                frozen_candidates
            )

            STATUS[
                f"{branch['branch'].lower()}_baseline_trades"
            ] = len(
                baseline_trades
            )

            print()
            print(
                f"{branch['branch']} baseline trades: "
                f"{len(baseline_trades)}",
                flush=True,
            )

            for hour in range(24):
                excluded_candidates = [
                    candidate
                    for candidate
                    in frozen_candidates
                    if candidate[
                        "ny_hour"
                    ] != hour
                ]

                excluded_trades, _, _ = simulate(
                    h1,
                    excluded_candidates,
                )

                row = build_hour_row(
                    branch,
                    hour,
                    baseline_trades,
                    excluded_trades,
                    years,
                )

                row[
                    "baseline_ignored_due_to_open_trade"
                ] = baseline_ignored

                row[
                    "baseline_still_open_at_end"
                ] = baseline_open

                rows.append(
                    row
                )

                completed += 1

                STATUS[
                    "completed_tests"
                ] = completed

                print(
                    f"{completed}/{TOTAL_TESTS} | "
                    f"{branch['branch']} | "
                    f"NY {hour:02d}",
                    flush=True,
                )

        df = pd.DataFrame(
            rows
        )

        if df.empty:
            raise RuntimeError(
                "No result rows generated"
            )

        # Diagnostic sort:
        # larger samples first, then worse hour expectancy/PF,
        # then larger benefit from exclusion.
        df = df.sort_values(
            by=[
                "branch",
                "hour_sample_ge_10",
                "hour_sample_ge_8",
                "hour_sample_ge_5",
                "hour_loses_in_3plus_eras",
                "hour_loses_in_2plus_eras",
                "hour_negative_expectancy",
                "hour_expectancy_r",
                "hour_profit_factor",
                "delta_pf_if_excluded",
                "delta_expectancy_if_excluded",
                "hour_trades",
            ],
            ascending=[
                True,
                False,
                False,
                False,
                False,
                False,
                False,
                True,
                True,
                False,
                False,
                False,
            ],
        )

        df.to_csv(
            OUTPUT_FILE,
            index=False,
        )

        STATUS.update({
            "state": "complete",
            "message": (
                "EUR/GBP hour-quality diagnostic "
                "completed successfully"
            ),
            "completed_tests": TOTAL_TESTS,
            "rows_saved": len(
                df
            ),
            "output_file": (
                OUTPUT_FILE
            ),
        })

        print()
        print("=" * 82)
        print(
            "EUR/GBP HOUR QUALITY DIAGNOSTIC COMPLETE"
        )
        print("=" * 82)
        print(
            "Rows:",
            len(df),
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
            ].copy()

            print()
            print(
                f"--- {branch_name}: MOST INTERESTING HOURS ---"
            )

            interesting = subset[
                subset[
                    "hour_trades"
                ] >= 5
            ].copy()

            interesting = interesting.sort_values(
                by=[
                    "hour_loses_in_3plus_eras",
                    "hour_loses_in_2plus_eras",
                    "hour_expectancy_r",
                    "hour_profit_factor",
                    "hour_trades",
                ],
                ascending=[
                    False,
                    False,
                    True,
                    True,
                    False,
                ],
            )

            print(
                interesting[
                    [
                        "ny_hour",
                        "hour_trades",
                        "hour_win_rate",
                        "hour_profit_factor",
                        "hour_total_r",
                        "hour_expectancy_r",
                        "hour_losing_eras",
                        "exclude_hour_trades",
                        "exclude_hour_trades_per_year",
                        "exclude_hour_profit_factor",
                        "exclude_hour_expectancy_r",
                        "delta_pf_if_excluded",
                        "delta_expectancy_if_excluded",
                    ]
                ].head(12).to_string(
                    index=False
                ),
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
            "EURGBP Short Dual-Branch Hour Quality"
        ),
        "status": STATUS,
        "instrument": INSTRUMENT,
        "direction": "SHORT",
        "reward_risk": REWARD_RISK,
        "timezone": "America/New_York",
        "timing_basis": (
            "signal candle open time"
        ),
        "trading_enabled": False,
        "orders_supported": False,
        "executor_connected": False,

        "shared_geometry": {
            "minimum_body_ratio": MIN_BODY_RATIO,
            "structure_lookback": STRUCTURE_LOOKBACK,
            "max_distance_atr": MAX_DISTANCE_ATR,
            "min_range_atr": MIN_RANGE_ATR,
            "max_close_location": MAX_CLOSE_LOCATION,
            "stop_buffer_ticks": STOP_BUFFER_TICKS,
            "backtest_slippage_ticks": BACKTEST_SLIPPAGE_TICKS,
        },

        "branches": BRANCHES,
        "hours": list(range(24)),
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
                "EUR/GBP hour-quality CSV "
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
            "eurgbp-short-dual-branch-hour-quality"
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
