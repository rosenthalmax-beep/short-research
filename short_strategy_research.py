import os
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta


# ============================================================
# EUR/GBP SHORT - WINNER FEATURE COMBINATIONS
#
# RESEARCH ONLY — NEVER SUBMITS ORDERS.
#
# Fixed viable geometry:
#
#   bearish engulfing
#   body ratio >= 1.00
#   structure lookback = 90 H1 bars
#   signal high within 0.075 ATR14 of previous 90-bar highest high
#   signal range >= 1.10 ATR14
#   bearish close location <= 0.20
#   RR = 3.00
#   stop = signal high + 10 ticks
#   adverse short slippage = 5 ticks
#   pyramiding = 0
#
# Winner feature families from discovery:
#
#   12h upward momentum
#   48h upward momentum
#   upper wick / body
#   maximum stop size / ATR14
#
# Matrix:
#
#   12h momentum:
#       NONE, 0.00, 0.25, 0.50, 0.75 ATR
#
#   48h momentum:
#       NONE, 0.50, 0.75, 1.00, 1.25, 1.50 ATR
#
#   upper wick / body:
#       NONE, 0.10, 0.20
#
#   stop-size cap:
#       NONE, 2.50, 2.25 ATR
#
# Total = 5 * 6 * 3 * 3 = 270 tests
#
# Goal:
#   Find robust combinations that improve:
#     - overall PF
#     - worst-era PF
#     - recent-era PF
#     - drawdown
#   while preserving roughly >= 90 trades and >= 4 trades/year.
#
# Output:
#   eurgbp_short_winner_feature_combinations.csv
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

MOMENTUM_12_VALUES = [
    None,
    0.00,
    0.25,
    0.50,
    0.75,
]

MOMENTUM_48_VALUES = [
    None,
    0.50,
    0.75,
    1.00,
    1.25,
    1.50,
]

MIN_UPPER_WICK_BODY_VALUES = [
    None,
    0.10,
    0.20,
]

MAX_STOP_SIZE_ATR_VALUES = [
    None,
    2.50,
    2.25,
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

H1_WARMUP_DAYS = 600

OUTPUT_FILE = "eurgbp_short_winner_feature_combinations.csv"


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

TOTAL_TESTS = (
    len(MOMENTUM_12_VALUES)
    * len(MOMENTUM_48_VALUES)
    * len(MIN_UPPER_WICK_BODY_VALUES)
    * len(MAX_STOP_SIZE_ATR_VALUES)
)

STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "service": "EURGBP Short Winner Feature Combinations",
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "reward_risk": REWARD_RISK,
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


# ============================================================
# FIXED CORE + FEATURES
# ============================================================

def build_candidates(
    h1,
    h1_atr,
):
    candidates = []

    max_lookback = max(
        STRUCTURE_LOOKBACK,
        48,
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

        # ------------------------------------------
        # FIXED VIABLE CORE
        # ------------------------------------------

        if (
            structure_distance_atr
            > MAX_DISTANCE_ATR
        ):
            continue

        if range_atr < MIN_RANGE_ATR:
            continue

        if (
            close_location
            > MAX_CLOSE_LOCATION
        ):
            continue

        # ------------------------------------------
        # CONDITIONAL FEATURES
        # ------------------------------------------

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

        candidates.append({
            "index": index,
            "time": signal["time"],
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
        # a signal on the exact H1 bar where the previous trade
        # exits is allowed.
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
    momentum_12_threshold,
    momentum_48_threshold,
    upper_wick_threshold,
    max_stop_size_atr,
    base_candidates,
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
        "min_up_momentum_12h_atr": (
            momentum_12_threshold
        ),
        "min_up_momentum_48h_atr": (
            momentum_48_threshold
        ),
        "min_upper_wick_body": (
            upper_wick_threshold
        ),
        "max_stop_size_atr": (
            max_stop_size_atr
        ),
        "base_signals": len(
            base_candidates
        ),
        "eligible_signals": len(
            eligible
        ),
        "retention_vs_base_pct": round(
            len(eligible)
            / len(base_candidates)
            * 100.0,
            2,
        ) if base_candidates else 0.0,
        "ignored_due_to_open_trade": (
            ignored
        ),
        "still_open_at_end": (
            still_open
        ),
        "trades": full["trades"],
        "trades_per_year": round(
            full["trades"]
            / years,
            2,
        ),
        "winners": full["winners"],
        "losers": full["losers"],
        "win_rate": full["win_rate"],
        "profit_factor": full[
            "profit_factor"
        ],
        "total_r": full["total_r"],
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
        ] = era["trades"]

        row[
            f"{era_name}_pf"
        ] = era["profit_factor"]

        row[
            f"{era_name}_r"
        ] = era["total_r"]

        row[
            f"{era_name}_expectancy"
        ] = era["expectancy_r"]

        if era["trades"] >= 5:
            if era["total_r"] > 0:
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
        "frequency_4py"
    ] = (
        full["trades"]
        / years
        >= 4.0
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
        "annual_r_linear"
    ] = round(
        full["expectancy_r"]
        * (
            full["trades"]
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
        print("=" * 76)
        print(
            "EUR/GBP SHORT - WINNER FEATURE COMBINATIONS"
        )
        print("=" * 76)
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
                "Building ATR14 and fixed-core features"
            ),
        })

        h1_atr = atr_series(
            h1,
            14,
        )

        base_candidates = (
            build_candidates(
                h1,
                h1_atr,
            )
        )

        STATUS[
            "fixed_core_signals"
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

        STATUS.update({
            "state": "running",
            "message": (
                "Running winner-feature combination matrix"
            ),
        })

        rows = []
        completed = 0

        for momentum_12_threshold in (
            MOMENTUM_12_VALUES
        ):
            for momentum_48_threshold in (
                MOMENTUM_48_VALUES
            ):
                for upper_wick_threshold in (
                    MIN_UPPER_WICK_BODY_VALUES
                ):
                    for max_stop_size_atr in (
                        MAX_STOP_SIZE_ATR_VALUES
                    ):

                        eligible = []

                        for candidate in (
                            base_candidates
                        ):
                            if (
                                momentum_12_threshold
                                is not None
                                and candidate[
                                    "momentum_12"
                                ]
                                < momentum_12_threshold
                            ):
                                continue

                            if (
                                momentum_48_threshold
                                is not None
                                and candidate[
                                    "momentum_48"
                                ]
                                < momentum_48_threshold
                            ):
                                continue

                            if (
                                upper_wick_threshold
                                is not None
                                and candidate[
                                    "upper_wick_body"
                                ]
                                < upper_wick_threshold
                            ):
                                continue

                            if (
                                max_stop_size_atr
                                is not None
                                and candidate[
                                    "stop_size_atr"
                                ]
                                > max_stop_size_atr
                            ):
                                continue

                            eligible.append(
                                candidate
                            )

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
                                momentum_12_threshold,
                                momentum_48_threshold,
                                upper_wick_threshold,
                                max_stop_size_atr,
                                base_candidates,
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
                            completed % 20 == 0
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
                "adequate_90_trades",
                "frequency_4py",
                "worst_era_pf_140",
                "worst_era_pf_130",
                "worst_era_pf_120",
                "pf_170",
                "pf_160",
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
            ],
        )

        df.to_csv(
            OUTPUT_FILE,
            index=False,
        )

        STATUS.update({
            "state": "complete",
            "message": (
                "EUR/GBP winner-feature combinations "
                "completed successfully"
            ),
            "completed_tests": TOTAL_TESTS,
            "rows_saved": len(
                df
            ),
            "fixed_core_signals": (
                len(base_candidates)
            ),
            "all_four_eras_profitable_count": int(
                df[
                    "all_four_eras_profitable"
                ].sum()
            ),
            "all_four_eras_90_trades_count": int(
                (
                    df[
                        "all_four_eras_profitable"
                    ]
                    & df[
                        "adequate_90_trades"
                    ]
                ).sum()
            ),
            "all_four_eras_4py_count": int(
                (
                    df[
                        "all_four_eras_profitable"
                    ]
                    & df[
                        "frequency_4py"
                    ]
                ).sum()
            ),
            "output_file": (
                OUTPUT_FILE
            ),
        })

        print()
        print("=" * 76)
        print(
            "EUR/GBP WINNER FEATURE COMBINATIONS COMPLETE"
        )
        print("=" * 76)
        print(
            "Fixed-core signals:",
            len(base_candidates),
        )
        print(
            "Rows:",
            len(df),
        )
        print(
            "All-four-era profitable:",
            int(
                df[
                    "all_four_eras_profitable"
                ].sum()
            ),
        )
        print(
            "All-four-era + >=90 trades:",
            int(
                (
                    df[
                        "all_four_eras_profitable"
                    ]
                    & df[
                        "adequate_90_trades"
                    ]
                ).sum()
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
            "EURGBP Short Winner Feature Combinations"
        ),
        "status": STATUS,
        "instrument": INSTRUMENT,
        "direction": "SHORT",
        "trading_enabled": False,
        "orders_supported": False,
        "executor_connected": False,
        "fixed_core": {
            "minimum_body_ratio": MIN_BODY_RATIO,
            "structure_lookback": STRUCTURE_LOOKBACK,
            "max_distance_atr": MAX_DISTANCE_ATR,
            "min_range_atr": MIN_RANGE_ATR,
            "max_close_location": MAX_CLOSE_LOCATION,
            "reward_risk": REWARD_RISK,
            "stop_buffer_ticks": STOP_BUFFER_TICKS,
            "backtest_slippage_ticks": (
                BACKTEST_SLIPPAGE_TICKS
            ),
        },
        "matrix": {
            "momentum_12_values": (
                MOMENTUM_12_VALUES
            ),
            "momentum_48_values": (
                MOMENTUM_48_VALUES
            ),
            "min_upper_wick_body_values": (
                MIN_UPPER_WICK_BODY_VALUES
            ),
            "max_stop_size_atr_values": (
                MAX_STOP_SIZE_ATR_VALUES
            ),
            "total_tests": TOTAL_TESTS,
        },
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
                "EUR/GBP winner-combinations CSV "
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
            "eurgbp-short-winner-feature-combinations"
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
