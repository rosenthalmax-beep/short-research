import os
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# USD/JPY LONG - FINAL VALIDATION
#
# READ-ONLY RESEARCH - NEVER SUBMITS ORDERS
#
# CURRENT LIVE
# ------------------------------------------------------------
# exact bullish engulfing
# body ratio >= 1.00
# structure lookback 17
# structure distance <= 0.55 ATR14
# previous completed daily close > EMA425
# exclude NY 01:00-02:59
# exclude Wednesday + Thursday (NY weekday)
# strong close OFF
# RR 3.75
# stop signal low -10 ticks
# adverse historical fill close +5 ticks
#
# CANDIDATE B
# ------------------------------------------------------------
# exact bullish engulfing
# body ratio >= 1.00
# body >= 0.80 ATR14
# range >= 1.20 ATR14
# structure lookback 80
# structure distance <= 0.50 ATR14
# strong close OFF
# no daily EMA regime
# no weekday exclusions
# no NY-hour exclusions
# RR 3.75
# stop signal low -10 ticks
# adverse historical fill close +5 ticks
#
# ============================================================
# LOCKED EXECUTION CONVENTIONS
#
# OANDA midpoint H1
# ATR14 Wilder/RMA, SMA-seeded
# USD/JPY tick size = 0.001
# reference entry = signal close
# adverse historical long fill = close +5 ticks
# target based on REFERENCE signal-close risk
# actual R based on adverse fill
# pyramiding = 0
# exits begin signal_index +1
# exact exit-candle signal allowed
#
# Same-bar long tie:
#   compare open->high vs open->low
#   high closer => target first
#   otherwise stop first
#
# Daily candles:
#   dailyAlignment=17 America/New_York
#   previous completed daily candle only
#
# Timing filters use SIGNAL CANDLE OPEN TIME in America/New_York
#
# History:
#   2002-05-06 20:00 UTC -> current completed UTC hour
# ============================================================


app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_URL = "https://api-fxtrade.oanda.com"

INSTRUMENT = "USD_JPY"

TICK_SIZE = 0.001
STOP_BUFFER_TICKS = 10
BACKTEST_SLIPPAGE_TICKS = 5
REWARD_RISK = 3.75

ATR_LENGTH = 14

NY_TZ = ZoneInfo("America/New_York")

DAILY_ALIGNMENT_HOUR = 17
DAILY_ALIGNMENT_TIMEZONE = "America/New_York"

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
D_CHUNK_DAYS = 1500

H1_WARMUP_DAYS = 260
D_WARMUP_DAYS = 3000

OUTPUT_SUMMARY = (
    "usdjpy_long_final_validation_summary.csv"
)

OUTPUT_YEARLY = (
    "usdjpy_long_final_validation_yearly.csv"
)

OUTPUT_ROLLING = (
    "usdjpy_long_final_validation_rolling3y.csv"
)

OUTPUT_TRADES = (
    "usdjpy_long_final_validation_trades.csv"
)

OUTPUT_OVERLAP = (
    "usdjpy_long_final_validation_overlap.csv"
)


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
        "USD/JPY Long Final Validation"
    ),
    "instrument": INSTRUMENT,
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
    granularity,
    start,
    end,
):
    params = {
        "price": "M",
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
    }

    if granularity == "D":
        params[
            "dailyAlignment"
        ] = DAILY_ALIGNMENT_HOUR

        params[
            "alignmentTimezone"
        ] = DAILY_ALIGNMENT_TIMEZONE

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
            candles.append(candle)

    return candles


def fetch_chunked(
    granularity,
    start,
    end,
    chunk_days,
):
    by_time = {}
    cursor = start

    while cursor < end:
        chunk_end = min(
            cursor
            + timedelta(
                days=chunk_days
            ),
            end,
        )

        print(
            f"Fetching {granularity}: "
            f"{cursor.date()} "
            f"-> {chunk_end.date()}",
            flush=True,
        )

        chunk = fetch_range(
            granularity,
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
):
    return rma_series(
        true_ranges(candles),
        ATR_LENGTH,
    )


def ema_series(
    values,
    length,
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


# ============================================================
# DAILY STATE
# ============================================================

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
            hour=DAILY_ALIGNMENT_HOUR,
            minute=0,
            second=0,
            microsecond=0,
        )
    )

    if ny_time < candidate:
        candidate = (
            candidate
            - timedelta(
                days=1
            )
        )

    return candidate.astimezone(
        timezone.utc
    )


def prepare_daily(
    daily
):
    closes = [
        candle["close"]
        for candle
        in daily
    ]

    ema425 = ema_series(
        closes,
        425,
    )

    rows = []

    for index, candle in enumerate(
        daily
    ):
        rows.append({
            "time":
                candle["time"],
            "close":
                candle["close"],
            "ema425":
                ema425[index],
        })

    return rows


def previous_completed_daily(
    signal_time,
    daily_state,
):
    session_start = (
        current_daily_start(
            signal_time
        )
    )

    selected = None

    for row in daily_state:
        if (
            row["time"]
            < session_start
        ):
            selected = row
        else:
            break

    return selected


# ============================================================
# COMMON SIGNAL FEATURES
# ============================================================

def common_features(
    h1,
    atr,
    index,
):
    signal = h1[index]

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
        return None

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
        return None

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
        return None

    body_ratio = (
        current_body
        / previous_body
    )

    if body_ratio < 1.00:
        return None

    ny = (
        signal["time"]
        .astimezone(
            NY_TZ
        )
    )

    return {
        "body_ratio":
            body_ratio,
        "body_atr":
            current_body
            / current_atr,
        "range_atr":
            signal_range
            / current_atr,
        "close_location":
            (
                signal["close"]
                - signal["low"]
            ) / signal_range,
        "current_atr":
            current_atr,
        "ny_hour":
            ny.hour,
        "ny_weekday":
            ny.weekday(),
    }


# ============================================================
# CURRENT LIVE SIGNALS
# ============================================================

def build_current_live_signals(
    h1,
    atr,
    daily_state,
):
    eligible = []

    start_index = max(
        ATR_LENGTH,
        17,
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

        f = common_features(
            h1,
            atr,
            index,
        )

        if f is None:
            continue

        previous_lowest = min(
            candle["low"]
            for candle
            in h1[
                index - 17:
                index
            ]
        )

        structure_distance = (
            signal["low"]
            - previous_lowest
        ) / f[
            "current_atr"
        ]

        if (
            structure_distance
            > 0.55
        ):
            continue

        daily = (
            previous_completed_daily(
                signal["time"],
                daily_state,
            )
        )

        if (
            daily is None
            or daily[
                "ema425"
            ] is None
        ):
            continue

        if not (
            daily["close"]
            > daily["ema425"]
        ):
            continue

        if (
            f["ny_hour"]
            in {1, 2}
        ):
            continue

        if (
            f["ny_weekday"]
            in {2, 3}
        ):
            continue

        eligible.append({
            "index":
                index,
            "time":
                signal["time"],
            "body_ratio":
                f["body_ratio"],
            "body_atr":
                f["body_atr"],
            "range_atr":
                f["range_atr"],
            "close_location":
                f["close_location"],
            "structure_distance_atr":
                structure_distance,
            "daily_close":
                daily["close"],
            "daily_ema425":
                daily["ema425"],
            "ny_hour":
                f["ny_hour"],
            "ny_weekday":
                f["ny_weekday"],
        })

    return eligible


# ============================================================
# CANDIDATE B SIGNALS
# ============================================================

def build_candidate_b_signals(
    h1,
    atr,
):
    eligible = []

    start_index = max(
        ATR_LENGTH,
        80,
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

        f = common_features(
            h1,
            atr,
            index,
        )

        if f is None:
            continue

        if (
            f["body_atr"]
            < 0.80
        ):
            continue

        if (
            f["range_atr"]
            < 1.20
        ):
            continue

        previous_lowest = min(
            candle["low"]
            for candle
            in h1[
                index - 80:
                index
            ]
        )

        structure_distance = (
            signal["low"]
            - previous_lowest
        ) / f[
            "current_atr"
        ]

        if (
            structure_distance
            > 0.50
        ):
            continue

        eligible.append({
            "index":
                index,
            "time":
                signal["time"],
            "body_ratio":
                f["body_ratio"],
            "body_atr":
                f["body_atr"],
            "range_atr":
                f["range_atr"],
            "close_location":
                f["close_location"],
            "structure_distance_atr":
                structure_distance,
            "daily_close":
                None,
            "daily_ema425":
                None,
            "ny_hour":
                f["ny_hour"],
            "ny_weekday":
                f["ny_weekday"],
        })

    return eligible


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
                exit_reason = (
                    "TARGET_TIE_FIRST"
                )
            else:
                exit_price = stop
                exit_reason = (
                    "STOP_TIE_FIRST"
                )

        elif target_hit:
            exit_price = target
            exit_reason = "TARGET"

        else:
            exit_price = stop
            exit_reason = "STOP"

        result = {
            "signal_index":
                signal_index,
            "signal_time":
                signal["time"],
            "exit_index":
                index,
            "exit_time":
                candle["time"],
            "entry_reference":
                reference_entry,
            "backtest_entry":
                backtest_entry,
            "stop":
                stop,
            "target":
                target,
            "exit_price":
                exit_price,
            "exit_reason":
                exit_reason,
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
    strategy_name,
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

        row = dict(trade)

        row[
            "strategy"
        ] = strategy_name

        for key in [
            "body_ratio",
            "body_atr",
            "range_atr",
            "close_location",
            "structure_distance_atr",
            "daily_close",
            "daily_ema425",
            "ny_hour",
            "ny_weekday",
        ]:
            row[key] = signal.get(
                key
            )

        trades.append(row)

        position_exit_index = (
            row["exit_index"]
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
        t = trade[
            "signal_time"
        ]

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
        r
        for r in results
        if r > 0
    ]

    losers = [
        r
        for r in results
        if r < 0
    ]

    gross_profit = sum(
        winners
    )

    gross_loss = abs(
        sum(losers)
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

    total_r = sum(
        results
    )

    equity = 0.0
    peak = 0.0
    max_dd = 0.0

    current_streak = 0
    longest_streak = 0

    for result in results:
        equity += result

        peak = max(
            peak,
            equity,
        )

        max_dd = min(
            max_dd,
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
        "trades":
            len(results),
        "winners":
            len(winners),
        "losers":
            len(losers),
        "win_rate":
            round(
                len(winners)
                / len(results)
                * 100.0,
                2,
            ),
        "profit_factor":
            round(
                pf,
                3,
            ),
        "total_r":
            round(
                total_r,
                2,
            ),
        "expectancy_r":
            round(
                total_r
                / len(results),
                3,
            ),
        "max_drawdown_r":
            round(
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


def build_summary_row(
    strategy_name,
    trades,
    ignored,
):
    years = (
        RESEARCH_TO
        - RESEARCH_FROM
    ).total_seconds() / (
        365.2425
        * 86400
    )

    full = stats_for_trades(
        trades
    )

    row = {
        "strategy":
            strategy_name,
        "research_from":
            RESEARCH_FROM.isoformat(),
        "research_to":
            RESEARCH_TO.isoformat(),
        "trades":
            full["trades"],
        "ignored_due_to_open_trade":
            ignored,
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
                stats["total_r"]
                > 0
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

    return row


def build_yearly_rows(
    strategy_name,
    trades,
):
    rows = []

    for year in range(
        RESEARCH_FROM.year,
        RESEARCH_TO.year + 1,
    ):
        start = max(
            RESEARCH_FROM,
            datetime(
                year,
                1,
                1,
                tzinfo=timezone.utc,
            ),
        )

        end = min(
            RESEARCH_TO,
            datetime(
                year + 1,
                1,
                1,
                tzinfo=timezone.utc,
            ),
        )

        if start >= end:
            continue

        rows.append({
            "strategy":
                strategy_name,
            "year":
                year,
            **stats_for_trades(
                trades,
                start,
                end,
            ),
        })

    return rows


def build_rolling_rows(
    strategy_name,
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

        rows.append({
            "strategy":
                strategy_name,
            "window":
                f"{start_year}_"
                f"{start_year + 2}",
            "start":
                start.isoformat(),
            "end":
                end.isoformat(),
            **stats_for_trades(
                trades,
                start,
                end,
            ),
        })

    return rows


# ============================================================
# OVERLAP / EXCLUSIVE ANALYSIS
#
# EXECUTED-trade overlap after each strategy's own pyramiding-0
# handling.
# ============================================================

def overlap_analysis(
    current_trades,
    candidate_trades,
):
    current_map = {
        trade[
            "signal_time"
        ]:
            trade
        for trade
        in current_trades
    }

    candidate_map = {
        trade[
            "signal_time"
        ]:
            trade
        for trade
        in candidate_trades
    }

    current_times = set(
        current_map
    )

    candidate_times = set(
        candidate_map
    )

    shared_times = sorted(
        current_times
        & candidate_times
    )

    current_only_times = sorted(
        current_times
        - candidate_times
    )

    candidate_only_times = sorted(
        candidate_times
        - current_times
    )

    groups = [
        (
            "SHARED_CURRENT",
            [
                current_map[t]
                for t
                in shared_times
            ],
        ),
        (
            "SHARED_CANDIDATE",
            [
                candidate_map[t]
                for t
                in shared_times
            ],
        ),
        (
            "CURRENT_ONLY",
            [
                current_map[t]
                for t
                in current_only_times
            ],
        ),
        (
            "CANDIDATE_ONLY",
            [
                candidate_map[t]
                for t
                in candidate_only_times
            ],
        ),
    ]

    summary_rows = []

    for (
        group_name,
        trades,
    ) in groups:
        summary_rows.append({
            "group":
                group_name,
            **stats_for_trades(
                trades
            ),
        })

    detail_rows = []

    all_times = sorted(
        current_times
        | candidate_times
    )

    for t in all_times:
        current_trade = (
            current_map.get(
                t
            )
        )

        candidate_trade = (
            candidate_map.get(
                t
            )
        )

        if (
            current_trade
            is not None
            and candidate_trade
            is not None
        ):
            group = "SHARED"
        elif (
            current_trade
            is not None
        ):
            group = "CURRENT_ONLY"
        else:
            group = "CANDIDATE_ONLY"

        detail_rows.append({
            "signal_time":
                t.isoformat(),
            "group":
                group,
            "current_result_r":
                (
                    current_trade[
                        "result_r"
                    ]
                    if current_trade
                    is not None
                    else None
                ),
            "candidate_result_r":
                (
                    candidate_trade[
                        "result_r"
                    ]
                    if candidate_trade
                    is not None
                    else None
                ),
            "current_exit_time":
                (
                    current_trade[
                        "exit_time"
                    ].isoformat()
                    if current_trade
                    is not None
                    else None
                ),
            "candidate_exit_time":
                (
                    candidate_trade[
                        "exit_time"
                    ].isoformat()
                    if candidate_trade
                    is not None
                    else None
                ),
        })

    return (
        pd.DataFrame(
            summary_rows
        ),
        pd.DataFrame(
            detail_rows
        ),
    )


# ============================================================
# TRADE EXPORT
# ============================================================

def trades_dataframe(
    trades,
):
    rows = []

    for trade in trades:
        rows.append({
            "strategy":
                trade[
                    "strategy"
                ],
            "signal_time":
                trade[
                    "signal_time"
                ].isoformat(),
            "exit_time":
                trade[
                    "exit_time"
                ].isoformat(),
            "signal_index":
                trade[
                    "signal_index"
                ],
            "exit_index":
                trade[
                    "exit_index"
                ],
            "entry_reference":
                round(
                    trade[
                        "entry_reference"
                    ],
                    6,
                ),
            "backtest_entry":
                round(
                    trade[
                        "backtest_entry"
                    ],
                    6,
                ),
            "stop":
                round(
                    trade[
                        "stop"
                    ],
                    6,
                ),
            "target":
                round(
                    trade[
                        "target"
                    ],
                    6,
                ),
            "exit_price":
                round(
                    trade[
                        "exit_price"
                    ],
                    6,
                ),
            "exit_reason":
                trade[
                    "exit_reason"
                ],
            "result_r":
                round(
                    trade[
                        "result_r"
                    ],
                    6,
                ),
            "body_ratio":
                round(
                    trade[
                        "body_ratio"
                    ],
                    6,
                ),
            "body_atr":
                round(
                    trade[
                        "body_atr"
                    ],
                    6,
                ),
            "range_atr":
                round(
                    trade[
                        "range_atr"
                    ],
                    6,
                ),
            "close_location":
                round(
                    trade[
                        "close_location"
                    ],
                    6,
                ),
            "structure_distance_atr":
                round(
                    trade[
                        "structure_distance_atr"
                    ],
                    6,
                ),
            "daily_close":
                trade.get(
                    "daily_close"
                ),
            "daily_ema425":
                trade.get(
                    "daily_ema425"
                ),
            "ny_hour":
                trade[
                    "ny_hour"
                ],
            "ny_weekday":
                trade[
                    "ny_weekday"
                ],
        })

    return pd.DataFrame(
        rows
    )


# ============================================================
# RUN
# ============================================================

def run_research():
    try:
        STATUS.update({
            "state":
                "fetching_h1",
            "message":
                "Fetching USD/JPY H1 history",
        })

        h1 = fetch_chunked(
            "H1",
            RESEARCH_FROM
            - timedelta(
                days=H1_WARMUP_DAYS
            ),
            RESEARCH_TO,
            H1_CHUNK_DAYS,
        )

        if not h1:
            raise RuntimeError(
                "No H1 candles returned"
            )

        STATUS.update({
            "state":
                "fetching_daily",
            "message":
                "Fetching USD/JPY daily history",
        })

        daily = fetch_chunked(
            "D",
            RESEARCH_FROM
            - timedelta(
                days=D_WARMUP_DAYS
            ),
            RESEARCH_TO,
            D_CHUNK_DAYS,
        )

        if not daily:
            raise RuntimeError(
                "No daily candles returned"
            )

        STATUS.update({
            "state":
                "precomputing",
            "message":
                "Calculating exact current and Candidate B signals",
        })

        atr = atr_series(
            h1
        )

        daily_state = (
            prepare_daily(
                daily
            )
        )

        current_signals = (
            build_current_live_signals(
                h1,
                atr,
                daily_state,
            )
        )

        candidate_signals = (
            build_candidate_b_signals(
                h1,
                atr,
            )
        )

        STATUS.update({
            "state":
                "simulating",
            "message":
                "Simulating current live and Candidate B",
            "current_eligible_signals":
                len(
                    current_signals
                ),
            "candidate_eligible_signals":
                len(
                    candidate_signals
                ),
        })

        (
            current_trades,
            current_ignored,
        ) = simulate_variant(
            h1,
            current_signals,
            "CURRENT_LIVE",
        )

        (
            candidate_trades,
            candidate_ignored,
        ) = simulate_variant(
            h1,
            candidate_signals,
            "CANDIDATE_B",
        )

        summary = pd.DataFrame([
            build_summary_row(
                "CURRENT_LIVE",
                current_trades,
                current_ignored,
            ),
            build_summary_row(
                "CANDIDATE_B",
                candidate_trades,
                candidate_ignored,
            ),
        ])

        yearly = pd.DataFrame(
            build_yearly_rows(
                "CURRENT_LIVE",
                current_trades,
            )
            +
            build_yearly_rows(
                "CANDIDATE_B",
                candidate_trades,
            )
        )

        rolling = pd.DataFrame(
            build_rolling_rows(
                "CURRENT_LIVE",
                current_trades,
            )
            +
            build_rolling_rows(
                "CANDIDATE_B",
                candidate_trades,
            )
        )

        trades = trades_dataframe(
            current_trades
            + candidate_trades
        )

        (
            overlap_summary,
            overlap_detail,
        ) = overlap_analysis(
            current_trades,
            candidate_trades,
        )

        # Combine summary and detail into one CSV with a row_type
        # column so /download/overlap remains one exact artifact.
        overlap_summary_out = (
            overlap_summary.copy()
        )

        overlap_summary_out.insert(
            0,
            "row_type",
            "SUMMARY",
        )

        overlap_detail_out = (
            overlap_detail.copy()
        )

        overlap_detail_out.insert(
            0,
            "row_type",
            "DETAIL",
        )

        overlap = pd.concat(
            [
                overlap_summary_out,
                overlap_detail_out,
            ],
            ignore_index=True,
            sort=False,
        )

        summary.to_csv(
            os.path.abspath(
                OUTPUT_SUMMARY
            ),
            index=False,
        )

        yearly.to_csv(
            os.path.abspath(
                OUTPUT_YEARLY
            ),
            index=False,
        )

        rolling.to_csv(
            os.path.abspath(
                OUTPUT_ROLLING
            ),
            index=False,
        )

        trades.to_csv(
            os.path.abspath(
                OUTPUT_TRADES
            ),
            index=False,
        )

        overlap.to_csv(
            os.path.abspath(
                OUTPUT_OVERLAP
            ),
            index=False,
        )

        STATUS.update({
            "state":
                "complete",
            "message":
                "USD/JPY final validation complete",
            "current_trades":
                len(
                    current_trades
                ),
            "candidate_trades":
                len(
                    candidate_trades
                ),
            "outputs": {
                "summary":
                    OUTPUT_SUMMARY,
                "yearly":
                    OUTPUT_YEARLY,
                "rolling":
                    OUTPUT_ROLLING,
                "trades":
                    OUTPUT_TRADES,
                "overlap":
                    OUTPUT_OVERLAP,
            },
        })

        print()
        print("=" * 100)
        print(
            "USD/JPY LONG FINAL VALIDATION COMPLETE"
        )
        print("=" * 100)
        print(
            summary.to_string(
                index=False
            ),
            flush=True,
        )
        print()
        print(
            "EXECUTED-TRADE OVERLAP"
        )
        print(
            overlap_summary.to_string(
                index=False
            ),
            flush=True,
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
            "USD/JPY Long Final Validation",
        "status":
            STATUS,
        "mode":
            "READ_ONLY_RESEARCH",
        "orders_supported":
            False,
        "trading_enabled":
            False,
        "candidate_b": {
            "minimum_body_ratio":
                1.00,
            "minimum_body_atr":
                0.80,
            "minimum_range_atr":
                1.20,
            "strong_close_enabled":
                False,
            "structure_lookback":
                80,
            "maximum_distance_atr":
                0.50,
            "daily_regime":
                None,
            "weekday_exclusions":
                [],
            "session_exclusions":
                [],
            "reward_risk":
                3.75,
            "stop_buffer_ticks":
                10,
            "historical_adverse_fill_ticks":
                5,
        },
        "downloads": {
            "summary":
                "/download/summary",
            "yearly":
                "/download/yearly",
            "rolling":
                "/download/rolling",
            "trades":
                "/download/trades",
            "overlap":
                "/download/overlap",
        },
    })


@app.route("/status")
def status():
    return jsonify(
        STATUS
    )


def send_output(
    filename,
):
    path = os.path.abspath(
        filename
    )

    if not os.path.exists(
        path
    ):
        return jsonify({
            "status":
                "not_ready",
            "message":
                f"{filename} is not ready yet",
        }), 404

    return send_file(
        path,
        as_attachment=True,
        download_name=filename,
    )


@app.route(
    "/download/summary"
)
def download_summary():
    return send_output(
        OUTPUT_SUMMARY
    )


@app.route(
    "/download/yearly"
)
def download_yearly():
    return send_output(
        OUTPUT_YEARLY
    )


@app.route(
    "/download/rolling"
)
def download_rolling():
    return send_output(
        OUTPUT_ROLLING
    )


@app.route(
    "/download/trades"
)
def download_trades():
    return send_output(
        OUTPUT_TRADES
    )


@app.route(
    "/download/overlap"
)
def download_overlap():
    return send_output(
        OUTPUT_OVERLAP
    )


if __name__ == "__main__":
    thread = threading.Thread(
        target=run_research,
        name=(
            "usdjpy-long-final-validation"
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

