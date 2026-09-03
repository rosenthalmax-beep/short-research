import os
import threading
import itertools
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# GBP/USD LONG - TIGHT ROBUSTNESS SWEEP
#
# RESEARCH ONLY - NEVER SUBMITS ORDERS.
#
# PURPOSE
# ------------------------------------------------------------
# Tight sweep around the strongest simple branch found in the
# core interaction matrix:
#
#   structure lookback:
#       35, 40, 45, 50, 55, 60
#
#   structure distance / ATR:
#       0.10, 0.125, 0.15, 0.175, 0.20
#
#   minimum body / ATR:
#       0.90, 1.00, 1.10, 1.20, 1.30
#
#   strong close:
#       off, 0.60, 0.65, 0.70
#
#   body ratio:
#       1.00, 1.20, 1.40
#
#   session:
#       ALL
#       exclude 14:00-18:59 America/New_York
#
# No daily EMA regime.
# No daily EMA alignment.
# No range filter.
# No weekday exclusions.
#
# RR fixed at 4.25.
#
# Total sweep:
#   6 * 5 * 5 * 4 * 3 * 2 = 3,600 configs
#
# ============================================================
# LOCKED EXECUTION CONVENTIONS
#
# OANDA midpoint H1.
#
# Bullish engulfing:
#   previous bearish
#   current bullish
#   current open <= previous close
#   current close >= previous open
#
# ATR14 = Wilder/RMA, SMA-seeded.
# GBP/USD tick size = 0.00001.
#
# Reference entry = signal close.
# Historical adverse long fill = close + 5 ticks.
#
# Stop = signal low - 10 ticks.
#
# Target based on REFERENCE signal-close risk:
#   target = close + (close - stop) * 4.25
#
# Actual R uses adverse fill.
#
# Pyramiding = 0.
#
# Same-bar tie for LONG:
#   compare open->high vs open->low
#   high closer => target first
#   else stop first.
#
# Signals signal_index < position_exit_index ignored.
# Exact exit-candle signal allowed.
#
# Exits begin signal_index + 1.
#
# History:
#   2002-05-06 20:00 UTC -> current completed UTC hour.
#
# ============================================================


app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_URL = "https://api-fxtrade.oanda.com"

INSTRUMENT = "GBP_USD"

TICK_SIZE = 0.00001
STOP_BUFFER_TICKS = 10
BACKTEST_SLIPPAGE_TICKS = 5
REWARD_RISK = 4.25

ATR_LENGTH = 14

NY_TZ = ZoneInfo("America/New_York")

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

H1_CHUNK_DAYS = 180
H1_WARMUP_DAYS = 260

OUTPUT_FILE = (
    "gbpusd_long_tight_robustness_sweep.csv"
)


# ============================================================
# GRID
# ============================================================

STRUCTURE_LOOKBACK_VALUES = [
    35, 40, 45, 50, 55, 60,
]

STRUCTURE_DISTANCE_VALUES = [
    0.10, 0.125, 0.15, 0.175, 0.20,
]

BODY_ATR_VALUES = [
    0.90, 1.00, 1.10, 1.20, 1.30,
]

STRONG_CLOSE_VALUES = [
    None, 0.60, 0.65, 0.70,
]

BODY_RATIO_VALUES = [
    1.00, 1.20, 1.40,
]

SESSION_OPTIONS = [
    ("ALL", False),
    ("EXCL_NY_14_19", True),
]

CONFIGS = list(
    itertools.product(
        STRUCTURE_LOOKBACK_VALUES,
        STRUCTURE_DISTANCE_VALUES,
        BODY_ATR_VALUES,
        STRONG_CLOSE_VALUES,
        BODY_RATIO_VALUES,
        SESSION_OPTIONS,
    )
)

TOTAL_TESTS = len(CONFIGS)


# ============================================================
# ERAS
# ============================================================

ERAS = [
    (
        "2002_2009",
        RESEARCH_FROM,
        datetime(
            2010, 1, 1,
            tzinfo=timezone.utc,
        ),
    ),
    (
        "2010_2017",
        datetime(
            2010, 1, 1,
            tzinfo=timezone.utc,
        ),
        datetime(
            2018, 1, 1,
            tzinfo=timezone.utc,
        ),
    ),
    (
        "2018_2023",
        datetime(
            2018, 1, 1,
            tzinfo=timezone.utc,
        ),
        datetime(
            2024, 1, 1,
            tzinfo=timezone.utc,
        ),
    ),
    (
        "2024_present",
        datetime(
            2024, 1, 1,
            tzinfo=timezone.utc,
        ),
        None,
    ),
]


STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "service": (
        "GBP/USD Long Tight Robustness Sweep"
    ),
    "instrument": INSTRUMENT,
    "tests": TOTAL_TESTS,
    "orders_supported": False,
    "trading_enabled": False,
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
        "Authorization":
            f"Bearer {OANDA_TOKEN}"
    }


def iso_utc(dt):
    return (
        dt
        .astimezone(timezone.utc)
        .isoformat()
        .replace(
            "+00:00",
            "Z",
        )
    )


def oanda_get(
    path,
    params,
):
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

    mid = raw.get("mid")

    if not mid:
        return None

    return {
        "time":
            datetime.fromisoformat(
                raw["time"].replace(
                    "Z",
                    "+00:00",
                )
            ),
        "open":
            float(mid["o"]),
        "high":
            float(mid["h"]),
        "low":
            float(mid["l"]),
        "close":
            float(mid["c"]),
    }


def fetch_range(
    start,
    end,
):
    params = {
        "price": "M",
        "granularity": "H1",
        "from": iso_utc(start),
        "to": iso_utc(end),
        "smooth": "false",
        "includeFirst": "true",
    }

    data = oanda_get(
        f"/v3/instruments/"
        f"{INSTRUMENT}/candles",
        params,
    )

    candles = []

    for raw in data.get(
        "candles",
        [],
    ):
        candle = parse_candle(raw)

        if candle is not None:
            candles.append(
                candle
            )

    return candles


def fetch_chunked(
    start,
    end,
):
    by_time = {}
    cursor = start

    while cursor < end:
        chunk_end = min(
            cursor
            + timedelta(
                days=H1_CHUNK_DAYS
            ),
            end,
        )

        print(
            f"Fetching H1: "
            f"{cursor.date()} "
            f"-> {chunk_end.date()}",
            flush=True,
        )

        chunk = fetch_range(
            cursor,
            chunk_end,
        )

        for candle in chunk:
            by_time[
                candle["time"]
            ] = candle

        cursor = chunk_end

    candles = list(
        by_time.values()
    )

    candles.sort(
        key=lambda item:
            item["time"]
    )

    return candles


# ============================================================
# INDICATORS
# ============================================================

def true_ranges(candles):
    values = []

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

        values.append(tr)

    return values


def rma_series(
    values,
    length,
):
    result = [
        None
    ] * len(values)

    if len(values) < length:
        return result

    initial = (
        sum(
            values[:length]
        )
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
):
    return rma_series(
        true_ranges(
            candles
        ),
        ATR_LENGTH,
    )


# ============================================================
# PRECOMPUTE CANDIDATES
# ============================================================

MAX_STRUCTURE_LOOKBACK = max(
    STRUCTURE_LOOKBACK_VALUES
)


def build_candidates(
    h1,
    atr,
):
    candidates = []

    start_index = max(
        ATR_LENGTH,
        MAX_STRUCTURE_LOOKBACK,
    )

    for index in range(
        start_index,
        len(h1),
    ):
        signal = h1[index]

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

        previous = h1[
            index - 1
        ]

        current_atr = atr[
            index
        ]

        if (
            current_atr is None
            or current_atr <= 0
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

        bullish_engulfing = (
            previous["close"]
            < previous["open"]
            and
            signal["close"]
            > signal["open"]
            and
            signal["open"]
            <= previous["close"]
            and
            signal["close"]
            >= previous["open"]
        )

        if not bullish_engulfing:
            continue

        body_ratio = (
            current_body
            / previous_body
        )

        if (
            body_ratio < 1.00
        ):
            continue

        close_location = (
            signal["close"]
            - signal["low"]
        ) / signal_range

        body_atr = (
            current_body
            / current_atr
        )

        structure_distances = {}

        for lookback in (
            STRUCTURE_LOOKBACK_VALUES
        ):
            previous_lowest = min(
                candle["low"]
                for candle
                in h1[
                    index - lookback:
                    index
                ]
            )

            structure_distances[
                lookback
            ] = (
                signal["low"]
                - previous_lowest
            ) / current_atr

        ny = (
            signal["time"]
            .astimezone(
                NY_TZ
            )
        )

        candidates.append({
            "index": index,
            "time": signal["time"],
            "body_ratio": body_ratio,
            "close_location":
                close_location,
            "body_atr": body_atr,
            "structure_distances":
                structure_distances,
            "ny_hour": ny.hour,
        })

    return candidates


# ============================================================
# TRADE SIMULATION
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
        + BACKTEST_SLIPPAGE_TICKS
        * TICK_SIZE
    )

    stop = (
        signal["low"]
        - STOP_BUFFER_TICKS
        * TICK_SIZE
    )

    reference_risk = (
        reference_entry
        - stop
    )

    if (
        reference_risk <= 0
    ):
        EXIT_CACHE[
            signal_index
        ] = None
        return None

    target = (
        reference_entry
        + reference_risk
        * REWARD_RISK
    )

    actual_risk = (
        backtest_entry
        - stop
    )

    if (
        actual_risk <= 0
    ):
        EXIT_CACHE[
            signal_index
        ] = None
        return None

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
            candle["low"]
            <= stop
        )

        target_hit = (
            candle["high"]
            >= target
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
                exit_price = target
            else:
                exit_price = stop

        elif target_hit:
            exit_price = target

        else:
            exit_price = stop

        result = {
            "signal_index":
                signal_index,
            "signal_time":
                signal["time"],
            "exit_index":
                index,
            "exit_time":
                candle["time"],
            "result_r":
                (
                    exit_price
                    - backtest_entry
                ) / actual_risk,
        }

        EXIT_CACHE[
            signal_index
        ] = result

        return result

    EXIT_CACHE[
        signal_index
    ] = None

    return None


def simulate_variant(
    h1,
    eligible,
):
    trades = []
    ignored = 0
    position_exit_index = -1

    for signal in eligible:
        signal_index = (
            signal["index"]
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

        if trade is None:
            break

        trades.append(trade)

        position_exit_index = (
            trade["exit_index"]
        )

    return trades, ignored


# ============================================================
# STATS
# ============================================================

def stats_for_trades(
    trades,
    start=None,
    end=None,
):
    selected = []

    for trade in trades:
        t = trade["signal_time"]

        if (
            start is not None
            and t < start
        ):
            continue

        if (
            end is not None
            and t >= end
        ):
            continue

        selected.append(trade)

    if not selected:
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
        for trade
        in selected
    ]

    winners = [
        r for r in results
        if r > 0
    ]

    losers = [
        r for r in results
        if r < 0
    ]

    gross_profit = sum(winners)
    gross_loss = abs(sum(losers))

    if gross_loss > 0:
        pf = gross_profit / gross_loss
    elif gross_profit > 0:
        pf = 999.0
    else:
        pf = 0.0

    total_r = sum(results)

    equity = 0.0
    peak = 0.0
    max_dd = 0.0

    current_streak = 0
    longest_streak = 0

    for r in results:
        equity += r
        peak = max(peak, equity)
        max_dd = min(
            max_dd,
            equity - peak,
        )

        if r < 0:
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
            total_r
            / len(results),
            3,
        ),
        "max_drawdown_r": round(
            max_dd,
            2,
        ),
        "longest_loss_streak":
            longest_streak,
    }


def subtract_years_safe(
    dt,
    years,
):
    try:
        return dt.replace(
            year=dt.year - years
        )
    except ValueError:
        return dt.replace(
            month=2,
            day=28,
            year=dt.year - years,
        )


def rolling_3y_worst(
    trades,
):
    rows = []

    for start_year in range(
        2002,
        RESEARCH_TO.year - 1,
    ):
        start = max(
            RESEARCH_FROM,
            datetime(
                start_year,
                1,
                1,
                tzinfo=timezone.utc,
            ),
        )

        end = min(
            RESEARCH_TO,
            datetime(
                start_year + 3,
                1,
                1,
                tzinfo=timezone.utc,
            ),
        )

        if start >= end:
            continue

        stats = stats_for_trades(
            trades,
            start,
            end,
        )

        if (
            stats["trades"]
            >= 5
        ):
            rows.append({
                "label":
                    f"{start_year}_"
                    f"{start_year + 2}",
                "pf":
                    stats[
                        "profit_factor"
                    ],
                "expectancy":
                    stats[
                        "expectancy_r"
                    ],
                "total_r":
                    stats[
                        "total_r"
                    ],
            })

    if not rows:
        return {
            "worst_rolling_3y_pf":
                None,
            "worst_rolling_3y_pf_label":
                None,
            "worst_rolling_3y_expectancy":
                None,
            "worst_rolling_3y_expectancy_label":
                None,
            "worst_rolling_3y_total_r":
                None,
            "worst_rolling_3y_total_r_label":
                None,
        }

    worst_pf = min(
        rows,
        key=lambda row:
            row["pf"],
    )

    worst_exp = min(
        rows,
        key=lambda row:
            row["expectancy"],
    )

    worst_total = min(
        rows,
        key=lambda row:
            row["total_r"],
    )

    return {
        "worst_rolling_3y_pf":
            worst_pf["pf"],
        "worst_rolling_3y_pf_label":
            worst_pf["label"],
        "worst_rolling_3y_expectancy":
            worst_exp[
                "expectancy"
            ],
        "worst_rolling_3y_expectancy_label":
            worst_exp["label"],
        "worst_rolling_3y_total_r":
            worst_total[
                "total_r"
            ],
        "worst_rolling_3y_total_r_label":
            worst_total["label"],
    }


def make_result_row(
    label,
    eligible,
    trades,
    ignored,
    years,
    structure_lookback,
    maximum_distance_atr,
    minimum_body_atr,
    strong_close,
    body_ratio,
    session_name,
):
    full = stats_for_trades(
        trades
    )

    row = {
        "label": label,
        "structure_lookback":
            structure_lookback,
        "maximum_distance_atr":
            maximum_distance_atr,
        "minimum_body_atr":
            minimum_body_atr,
        "strong_close":
            strong_close,
        "body_ratio":
            body_ratio,
        "session":
            session_name,
        "eligible_signals":
            len(eligible),
        "ignored_due_to_open_trade":
            ignored,
        "trades":
            full["trades"],
        "trades_per_year":
            round(
                full["trades"]
                / years,
                3,
            ),
        "winners":
            full["winners"],
        "losers":
            full["losers"],
        "win_rate":
            full["win_rate"],
        "profit_factor":
            full["profit_factor"],
        "total_r":
            full["total_r"],
        "expectancy_r":
            full["expectancy_r"],
        "max_drawdown_r":
            full["max_drawdown_r"],
        "longest_loss_streak":
            full[
                "longest_loss_streak"
            ],
        "annual_r_linear":
            round(
                full["total_r"]
                / years,
                3,
            ),
    }

    minimum_era_pf = None
    profitable_eras = 0

    for (
        era_name,
        era_start,
        era_end,
    ) in ERAS:
        stats = stats_for_trades(
            trades,
            era_start,
            (
                RESEARCH_TO
                if era_end is None
                else min(
                    era_end,
                    RESEARCH_TO,
                )
            ),
        )

        row[
            f"{era_name}_trades"
        ] = stats["trades"]

        row[
            f"{era_name}_pf"
        ] = stats[
            "profit_factor"
        ]

        row[
            f"{era_name}_r"
        ] = stats["total_r"]

        row[
            f"{era_name}_expectancy"
        ] = stats[
            "expectancy_r"
        ]

        if (
            stats["trades"]
            >= 5
        ):
            if (
                minimum_era_pf
                is None
            ):
                minimum_era_pf = (
                    stats[
                        "profit_factor"
                    ]
                )
            else:
                minimum_era_pf = min(
                    minimum_era_pf,
                    stats[
                        "profit_factor"
                    ],
                )

            if (
                stats["total_r"] > 0
            ):
                profitable_eras += 1

    row[
        "minimum_era_pf_5_plus"
    ] = minimum_era_pf

    row[
        "profitable_eras"
    ] = profitable_eras

    for years_back in [
        2, 5, 10,
    ]:
        start = subtract_years_safe(
            RESEARCH_TO,
            years_back,
        )

        stats = stats_for_trades(
            trades,
            start,
            RESEARCH_TO,
        )

        row[
            f"last_{years_back}y_trades"
        ] = stats["trades"]

        row[
            f"last_{years_back}y_pf"
        ] = stats[
            "profit_factor"
        ]

        row[
            f"last_{years_back}y_r"
        ] = stats["total_r"]

        row[
            f"last_{years_back}y_expectancy"
        ] = stats[
            "expectancy_r"
        ]

    row.update(
        rolling_3y_worst(
            trades
        )
    )

    return row


# ============================================================
# RUN
# ============================================================

def run_research():
    try:
        STATUS.update({
            "state":
                "fetching_h1",
            "message":
                "Fetching GBP/USD H1 history",
        })

        h1 = fetch_chunked(
            RESEARCH_FROM
            - timedelta(
                days=H1_WARMUP_DAYS
            ),
            RESEARCH_TO,
        )

        if not h1:
            raise RuntimeError(
                "No H1 candles returned"
            )

        STATUS.update({
            "state":
                "precomputing",
            "message":
                "Precomputing GBP/USD sweep features",
        })

        atr = atr_series(h1)

        candidates = (
            build_candidates(
                h1,
                atr,
            )
        )

        STATUS[
            "raw_candidates"
        ] = len(
            candidates
        )

        years = (
            RESEARCH_TO
            - RESEARCH_FROM
        ).total_seconds() / (
            365.2425
            * 86400
        )

        rows = []

        STATUS.update({
            "state":
                "running",
            "message":
                f"Running {TOTAL_TESTS} tight sweep configs",
            "completed_tests":
                0,
        })

        for test_number, config in enumerate(
            CONFIGS,
            start=1,
        ):
            (
                structure_lookback,
                maximum_distance_atr,
                minimum_body_atr,
                strong_close,
                body_ratio,
                session_option,
            ) = config

            (
                session_name,
                exclude_ny_14_19,
            ) = session_option

            eligible = []

            for signal in candidates:
                if (
                    signal[
                        "structure_distances"
                    ][
                        structure_lookback
                    ]
                    > maximum_distance_atr
                ):
                    continue

                if (
                    signal[
                        "body_atr"
                    ]
                    < minimum_body_atr
                ):
                    continue

                if (
                    signal[
                        "body_ratio"
                    ]
                    < body_ratio
                ):
                    continue

                if (
                    strong_close
                    is not None
                    and signal[
                        "close_location"
                    ] < strong_close
                ):
                    continue

                if (
                    exclude_ny_14_19
                    and signal[
                        "ny_hour"
                    ] >= 14
                    and signal[
                        "ny_hour"
                    ] < 19
                ):
                    continue

                eligible.append(
                    signal
                )

            (
                trades,
                ignored,
            ) = simulate_variant(
                h1,
                eligible,
            )

            strong_close_label = (
                "OFF"
                if strong_close is None
                else f"{strong_close:.2f}"
            )

            label = (
                f"S{structure_lookback}_"
                f"D{maximum_distance_atr:.3f}_"
                f"BODYATR{minimum_body_atr:.2f}_"
                f"SC{strong_close_label}_"
                f"BR{body_ratio:.2f}_"
                f"{session_name}"
            )

            rows.append(
                make_result_row(
                    label,
                    eligible,
                    trades,
                    ignored,
                    years,
                    structure_lookback,
                    maximum_distance_atr,
                    minimum_body_atr,
                    strong_close,
                    body_ratio,
                    session_name,
                )
            )

            STATUS[
                "completed_tests"
            ] = test_number

            if (
                test_number % 250 == 0
                or test_number
                == TOTAL_TESTS
            ):
                print(
                    f"Sweep "
                    f"{test_number}/"
                    f"{TOTAL_TESTS}",
                    flush=True,
                )

        df = pd.DataFrame(rows)

        df = df.sort_values(
            by=[
                "profitable_eras",
                "minimum_era_pf_5_plus",
                "worst_rolling_3y_pf",
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
        ).reset_index(
            drop=True
        )

        df.to_csv(
            os.path.abspath(
                OUTPUT_FILE
            ),
            index=False,
        )

        STATUS.update({
            "state":
                "complete",
            "message":
                "GBP/USD tight robustness sweep complete",
            "rows_saved":
                len(df),
            "output_file":
                OUTPUT_FILE,
        })

        print()
        print("=" * 95)
        print(
            "GBP/USD LONG TIGHT ROBUSTNESS SWEEP COMPLETE"
        )
        print("=" * 95)
        print(
            f"Rows saved: {len(df)}"
        )
        print(
            f"Output: {OUTPUT_FILE}"
        )

    except Exception as error:
        STATUS.update({
            "state":
                "error",
            "message":
                str(error),
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
        "service":
            "GBP/USD Long Tight Robustness Sweep",
        "status":
            STATUS,
        "mode":
            "READ_ONLY_RESEARCH",
        "orders_supported":
            False,
        "trading_enabled":
            False,
        "download":
            "/download",
    })


@app.route("/status")
def status():
    return jsonify(
        STATUS
    )


@app.route("/download")
def download():
    path = os.path.abspath(
        OUTPUT_FILE
    )

    if not os.path.exists(
        path
    ):
        return jsonify({
            "status":
                "not_ready",
            "message":
                "CSV is not ready yet",
        }), 404

    return send_file(
        path,
        as_attachment=True,
        download_name=OUTPUT_FILE,
    )


if __name__ == "__main__":
    thread = threading.Thread(
        target=run_research,
        name=(
            "gbpusd-long-tight-robustness-sweep"
        ),
        daemon=True,
    )

    thread.start()

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
