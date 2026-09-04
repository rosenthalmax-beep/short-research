import os
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# USD/JPY LONG - CURRENT LIVE CONTROL BASELINE
#
# RESEARCH ONLY - NEVER SUBMITS ORDERS.
#
# EXACT CURRENT LIVE STRATEGY
# ------------------------------------------------------------
# OANDA:USD_JPY
# H1
# LONG ONLY
#
# Bullish engulfing:
#   previous candle bearish
#   current candle bullish
#   current body engulfs previous body
#
# Minimum body ratio:
#   >= 1.00
#
# Strong close:
#   disabled
#
# Structure:
#   previous 17 bars excluding current
#   signal low within 0.55 ATR14
#   of previous lowest low
#
# Daily regime:
#   previous completed daily close > EMA425
#
# Daily alignment:
#   none
#
# Session:
#   exclude NY 01:00-02:59
#
# Weekdays:
#   exclude Wednesday and Thursday
#   America/New_York
#
# RR:
#   3.75
#
# Stop:
#   signal low - 10 ticks
#
# Historical adverse fill:
#   signal close + 5 ticks
#
# Pyramiding:
#   0
#
# ============================================================
# LOCKED EXECUTION CONVENTIONS
#
# OANDA midpoint H1 candles.
#
# ATR14 = Wilder/RMA, SMA-seeded.
#
# USD/JPY tick size = 0.001.
#
# Reference entry = signal close.
# Historical adverse long fill =
#       signal close + 5 ticks.
#
# Stop =
#       signal low - 10 ticks.
#
# Target based on REFERENCE signal-close risk:
#       target = signal close
#              + (signal close - stop) * 3.75
#
# Actual R uses adverse backtest fill.
#
# Same-bar target/stop tie for LONG:
#   compare candle open->high vs open->low
#   high closer => target first
#   otherwise stop first.
#
# Signals with signal_index < position_exit_index ignored.
# Exact exit-candle signal allowed.
#
# Exit checks begin signal_index + 1.
#
# Daily candles:
#   dailyAlignment = 17
#   alignmentTimezone = America/New_York
#   previous completed daily candle only.
#
# Timing filters use SIGNAL CANDLE OPEN TIME converted
# to America/New_York / DST-aware.
#
# History:
#   2002-05-06 20:00 UTC
#   -> current completed UTC hour.
#
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

BODY_RATIO_MIN = 1.00

STRUCTURE_LOOKBACK = 17
MAX_STRUCTURE_DISTANCE_ATR = 0.55

DAILY_EMA_LENGTH = 425

NY_TZ = ZoneInfo("America/New_York")

EXCLUDED_NY_HOURS = {
    1,
    2,
}

# Python weekday():
# Monday=0 Tuesday=1 Wednesday=2 Thursday=3 Friday=4
EXCLUDED_WEEKDAYS = {
    2,
    3,
}

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

# Plenty of history for EMA425 convergence.
D_WARMUP_DAYS = 3000

OUTPUT_SUMMARY = (
    "usdjpy_long_current_control_summary.csv"
)

OUTPUT_YEARLY = (
    "usdjpy_long_current_control_yearly.csv"
)

OUTPUT_ROLLING = (
    "usdjpy_long_current_control_rolling3y.csv"
)

OUTPUT_TRADES = (
    "usdjpy_long_current_control_trades.csv"
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
        "USD/JPY Long Current Control Baseline"
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
            candles.append(
                candle
            )

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
        sum(
            values[:length]
        )
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
        DAILY_EMA_LENGTH,
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
# SIGNALS
# ============================================================

def build_signals(
    h1,
    atr,
    daily_state,
):
    signals = []

    start_index = max(
        ATR_LENGTH,
        STRUCTURE_LOOKBACK,
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

        if (
            previous_body <= 0
            or current_body <= 0
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
            body_ratio
            < BODY_RATIO_MIN
        ):
            continue

        previous_lowest = min(
            candle["low"]
            for candle
            in h1[
                index
                - STRUCTURE_LOOKBACK:
                index
            ]
        )

        structure_distance = (
            signal["low"]
            - previous_lowest
        ) / current_atr

        if (
            structure_distance
            > MAX_STRUCTURE_DISTANCE_ATR
        ):
            continue

        daily = (
            previous_completed_daily(
                signal["time"],
                daily_state,
            )
        )

        if daily is None:
            continue

        if (
            daily["ema425"]
            is None
        ):
            continue

        if not (
            daily["close"]
            > daily["ema425"]
        ):
            continue

        ny = (
            signal["time"]
            .astimezone(
                NY_TZ
            )
        )

        if (
            ny.hour
            in EXCLUDED_NY_HOURS
        ):
            continue

        if (
            ny.weekday()
            in EXCLUDED_WEEKDAYS
        ):
            continue

        signal_range = (
            signal["high"]
            - signal["low"]
        )

        close_location = (
            (
                signal["close"]
                - signal["low"]
            ) / signal_range
            if signal_range > 0
            else None
        )

        signals.append({
            "index":
                index,
            "time":
                signal["time"],
            "body_ratio":
                body_ratio,
            "body_atr":
                current_body
                / current_atr,
            "range_atr":
                signal_range
                / current_atr
                if current_atr > 0
                else None,
            "close_location":
                close_location,
            "structure_distance_atr":
                structure_distance,
            "daily_close":
                daily["close"],
            "daily_ema425":
                daily["ema425"],
            "ny_hour":
                ny.hour,
            "ny_weekday":
                ny.weekday(),
        })

    return signals


# ============================================================
# TRADE SIMULATION
# ============================================================

def calculate_trade_exit(
    h1,
    signal_index,
):
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

        return {
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

    return None


def simulate(
    h1,
    signals,
):
    trades = []
    ignored = 0
    position_exit_index = -1

    for signal in signals:
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

        selected.append(
            trade
        )

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

    gross_profit = (
        sum(winners)
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


def build_summary(
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
            "CURRENT_LIVE",
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

    return pd.DataFrame(
        [row]
    )


def build_yearly(
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
            "year":
                year,
            **stats_for_trades(
                trades,
                start,
                end,
            ),
        })

    return pd.DataFrame(
        rows
    )


def build_rolling(
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

    return pd.DataFrame(
        rows
    )


def build_trade_export(
    trades,
):
    rows = []

    for trade in trades:
        rows.append({
            "strategy":
                "CURRENT_LIVE",
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
                    trade["stop"],
                    6,
                ),
            "target":
                round(
                    trade["target"],
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
                )
                if trade[
                    "close_location"
                ] is not None
                else None,
            "structure_distance_atr":
                round(
                    trade[
                        "structure_distance_atr"
                    ],
                    6,
                ),
            "daily_close":
                round(
                    trade[
                        "daily_close"
                    ],
                    6,
                ),
            "daily_ema425":
                round(
                    trade[
                        "daily_ema425"
                    ],
                    6,
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
                "Calculating exact current-control indicators",
        })

        atr = atr_series(
            h1
        )

        daily_state = (
            prepare_daily(
                daily
            )
        )

        signals = build_signals(
            h1,
            atr,
            daily_state,
        )

        STATUS.update({
            "state":
                "simulating",
            "message":
                "Simulating current USD/JPY long control",
            "eligible_signals":
                len(signals),
        })

        (
            trades,
            ignored,
        ) = simulate(
            h1,
            signals,
        )

        summary = build_summary(
            trades,
            ignored,
        )

        yearly = build_yearly(
            trades
        )

        rolling = build_rolling(
            trades
        )

        trade_export = (
            build_trade_export(
                trades
            )
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

        trade_export.to_csv(
            os.path.abspath(
                OUTPUT_TRADES
            ),
            index=False,
        )

        STATUS.update({
            "state":
                "complete",
            "message":
                "USD/JPY current-control baseline complete",
            "trades":
                len(trades),
            "ignored_due_to_open_trade":
                ignored,
            "outputs": {
                "summary":
                    OUTPUT_SUMMARY,
                "yearly":
                    OUTPUT_YEARLY,
                "rolling":
                    OUTPUT_ROLLING,
                "trades":
                    OUTPUT_TRADES,
            },
        })

        print()
        print(
            "=" * 100
        )
        print(
            "USD/JPY LONG CURRENT CONTROL BASELINE"
        )
        print(
            "=" * 100
        )
        print(
            summary.to_string(
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
            "USD/JPY Long Current Control Baseline",
        "status":
            STATUS,
        "mode":
            "READ_ONLY_RESEARCH",
        "orders_supported":
            False,
        "trading_enabled":
            False,
        "current_live": {
            "minimum_body_ratio":
                BODY_RATIO_MIN,
            "strong_close_enabled":
                False,
            "structure_lookback":
                STRUCTURE_LOOKBACK,
            "maximum_distance_atr":
                MAX_STRUCTURE_DISTANCE_ATR,
            "daily_close_above_ema":
                DAILY_EMA_LENGTH,
            "excluded_ny_hours":
                sorted(
                    EXCLUDED_NY_HOURS
                ),
            "excluded_weekdays":
                sorted(
                    EXCLUDED_WEEKDAYS
                ),
            "reward_risk":
                REWARD_RISK,
            "stop_buffer_ticks":
                STOP_BUFFER_TICKS,
            "historical_adverse_fill_ticks":
                BACKTEST_SLIPPAGE_TICKS,
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


if __name__ == "__main__":
    thread = threading.Thread(
        target=run_research,
        name=(
            "usdjpy-long-current-control"
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
