import os
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# USD/JPY LONG - SINGLE FACTOR EDGE DISCOVERY
#
# RESEARCH ONLY - NEVER SUBMITS ORDERS.
#
# PURPOSE
# ------------------------------------------------------------
# Establish the raw bullish-engulfing baseline, then test each
# candidate filter independently at locked RR 3.75.
#
# This is NOT an optimisation matrix.
# Every row = RAW bullish engulfing + ONE factor only,
# except CURRENT_LIVE which reproduces the deployed control.
#
# Families tested:
#
#   1) Raw bullish engulfing
#
#   2) Body ratio
#      1.10 / 1.20 / 1.30 / 1.40 / 1.50 / 1.75 / 2.00
#
#   3) Strong close
#      0.55 / 0.60 / 0.65 / 0.70 / 0.75 / 0.80
#
#   4) Lower wick / body
#      0.10 / 0.20 / 0.30 / 0.40 / 0.50
#
#   5) Upper wick / body MAX
#      0.10 / 0.20 / 0.30 / 0.40 / 0.50 / 0.75 / 1.00
#
#   6) Body size / ATR14
#      0.40 / 0.60 / 0.80 / 1.00 / 1.20 / 1.40
#
#   7) Range / ATR14
#      0.70 / 0.90 / 1.10 / 1.20 / 1.30 / 1.40 / 1.50
#
#   8) Maximum stop distance / ATR14
#      0.75 / 1.00 / 1.25 / 1.50 / 1.75 / 2.00 / 2.50
#
#   9) Structure proximity
#      lookback:
#          10, 15, 20, 30, 40, 50, 60, 80, 100
#      max distance ATR:
#          0.00, 0.05, 0.10, 0.20, 0.30, 0.40,
#          0.55, 0.75, 1.00
#
#  10) Upward momentum before signal
#      6h / 12h / 24h / 48h
#      >= 0.25 / 0.50 / 0.75 / 1.00 ATR14
#
#  11) Previous completed daily close > EMA
#      EMA 20 / 30 / 50 / 70 / 100 / 150 / 200 /
#      250 / 300 / 350 / 400 / 425 / 500
#
#  12) Daily EMA alignment
#      EMA20 > EMA50
#      EMA20 > EMA100
#      EMA30 > EMA100
#      EMA50 > EMA100
#      EMA50 > EMA150
#      EMA50 > EMA200
#      EMA100 > EMA200
#      EMA100 > EMA300
#      EMA200 > EMA400
#
#  13) Daily ATR14 / 50-day mean ATR
#      >= 0.70 / 0.80 / 0.90 / 1.00 / 1.10 / 1.20 / 1.30
#
#  14) Individual NY hour exclusions
#      Exclude one hour at a time, 0 through 23
#
#  15) Individual weekday exclusions
#      Monday through Friday
#
#  16) Current live control
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
#
# USD/JPY tick size = 0.001.
#
# Reference entry = signal close.
# Historical adverse long fill = close + 5 ticks.
#
# Stop = signal low - 10 ticks.
#
# Target based on REFERENCE signal-close risk:
#   target = close + (close - stop) * 3.75
#
# Actual R uses adverse fill.
#
# Pyramiding = 0.
#
# Same-bar tie for LONG:
#   compare open->high vs open->low
#   high closer => target first
#   otherwise stop first.
#
# Signals signal_index < position_exit_index ignored.
# Exact exit-candle signal allowed.
#
# Exits begin signal_index + 1.
#
# Daily candles:
#   dailyAlignment = 17
#   alignmentTimezone = America/New_York
#   previous completed daily candle only.
#
# Timing uses SIGNAL CANDLE OPEN TIME converted to NY.
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
D_WARMUP_DAYS = 3500

OUTPUT_FILE = (
    "usdjpy_long_single_factor_edges.csv"
)


# ============================================================
# TEST VALUES
# ============================================================

BODY_RATIO_VALUES = [
    1.10, 1.20, 1.30, 1.40,
    1.50, 1.75, 2.00,
]

STRONG_CLOSE_VALUES = [
    0.55, 0.60, 0.65,
    0.70, 0.75, 0.80,
]

LOWER_WICK_VALUES = [
    0.10, 0.20, 0.30,
    0.40, 0.50,
]

UPPER_WICK_MAX_VALUES = [
    0.10, 0.20, 0.30,
    0.40, 0.50, 0.75, 1.00,
]

BODY_ATR_VALUES = [
    0.40, 0.60, 0.80,
    1.00, 1.20, 1.40,
]

RANGE_ATR_VALUES = [
    0.70, 0.90, 1.10,
    1.20, 1.30, 1.40, 1.50,
]

MAX_STOP_ATR_VALUES = [
    0.75, 1.00, 1.25,
    1.50, 1.75, 2.00, 2.50,
]

STRUCTURE_LOOKBACK_VALUES = [
    10, 15, 20, 30, 40,
    50, 60, 80, 100,
]

STRUCTURE_DISTANCE_VALUES = [
    0.00, 0.05, 0.10,
    0.20, 0.30, 0.40,
    0.55, 0.75, 1.00,
]

MOMENTUM_HOURS = [
    6, 12, 24, 48,
]

MOMENTUM_ATR_VALUES = [
    0.25, 0.50, 0.75, 1.00,
]

DAILY_EMA_VALUES = [
    20, 30, 50, 70, 100,
    150, 200, 250, 300,
    350, 400, 425, 500,
]

DAILY_ALIGNMENTS = [
    (20, 50),
    (20, 100),
    (30, 100),
    (50, 100),
    (50, 150),
    (50, 200),
    (100, 200),
    (100, 300),
    (200, 400),
]

DAILY_ATR_RATIO_VALUES = [
    0.70, 0.80, 0.90,
    1.00, 1.10, 1.20, 1.30,
]

MAX_STRUCTURE_LOOKBACK = max(
    STRUCTURE_LOOKBACK_VALUES
)

MAX_MOMENTUM_HOURS = max(
    MOMENTUM_HOURS
)

MAX_DAILY_EMA = max(
    max(DAILY_EMA_VALUES),
    max(
        max(fast, slow)
        for fast, slow
        in DAILY_ALIGNMENTS
    ),
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
        "USD/JPY Long Single-Factor Edge Discovery"
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


def sma_series(
    values,
    length,
):
    result = [
        None
    ] * len(values)

    if len(values) < length:
        return result

    running = sum(
        values[:length]
    )

    result[
        length - 1
    ] = (
        running
        / length
    )

    for index in range(
        length,
        len(values),
    ):
        running += (
            values[index]
            - values[
                index - length
            ]
        )

        result[index] = (
            running
            / length
        )

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

    ema_map = {
        length:
            ema_series(
                closes,
                length,
            )
        for length
        in sorted(
            set(
                DAILY_EMA_VALUES
                + [
                    x
                    for pair
                    in DAILY_ALIGNMENTS
                    for x in pair
                ]
            )
        )
    }

    daily_atr = atr_series(
        daily
    )

    daily_atr_clean = [
        (
            value
            if value is not None
            else 0.0
        )
        for value in daily_atr
    ]

    daily_atr_mean50 = (
        sma_series(
            daily_atr_clean,
            50,
        )
    )

    rows = []

    for index, candle in enumerate(
        daily
    ):
        atr_ratio = None

        if (
            daily_atr[index]
            is not None
            and daily_atr_mean50[index]
            is not None
            and daily_atr_mean50[index]
            > 0
        ):
            atr_ratio = (
                daily_atr[index]
                / daily_atr_mean50[index]
            )

        row = {
            "time":
                candle["time"],
            "close":
                candle["close"],
            "atr14":
                daily_atr[index],
            "atr14_mean50":
                daily_atr_mean50[index],
            "atr_ratio50":
                atr_ratio,
        }

        for length, series in (
            ema_map.items()
        ):
            row[
                f"ema{length}"
            ] = series[index]

        rows.append(row)

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
# RAW CANDIDATES
# ============================================================

def build_raw_candidates(
    h1,
    atr,
    daily_state,
):
    rows = []

    start_index = max(
        ATR_LENGTH,
        MAX_STRUCTURE_LOOKBACK,
        MAX_MOMENTUM_HOURS,
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

        if body_ratio < 1.00:
            continue

        lower_wick = (
            min(
                signal["open"],
                signal["close"],
            )
            - signal["low"]
        )

        upper_wick = (
            signal["high"]
            - max(
                signal["open"],
                signal["close"],
            )
        )

        stop = (
            signal["low"]
            - STOP_BUFFER_TICKS
            * TICK_SIZE
        )

        stop_distance = (
            signal["close"]
            - stop
        )

        ny = (
            signal["time"]
            .astimezone(
                NY_TZ
            )
        )

        structure = {}

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

            structure[
                lookback
            ] = (
                signal["low"]
                - previous_lowest
            ) / current_atr

        momentum = {}

        for hours in (
            MOMENTUM_HOURS
        ):
            momentum[
                hours
            ] = (
                signal["close"]
                - h1[
                    index - hours
                ]["close"]
            ) / current_atr

        daily = (
            previous_completed_daily(
                signal["time"],
                daily_state,
            )
        )

        rows.append({
            "index":
                index,
            "time":
                signal["time"],
            "body_ratio":
                body_ratio,
            "close_location":
                (
                    signal["close"]
                    - signal["low"]
                ) / signal_range,
            "lower_wick_body":
                lower_wick
                / current_body,
            "upper_wick_body":
                upper_wick
                / current_body,
            "body_atr":
                current_body
                / current_atr,
            "range_atr":
                signal_range
                / current_atr,
            "stop_atr":
                stop_distance
                / current_atr,
            "structure":
                structure,
            "momentum":
                momentum,
            "daily":
                daily,
            "ny_hour":
                ny.hour,
            "ny_weekday":
                ny.weekday(),
        })

    return rows


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

    if reference_risk <= 0:
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

    if actual_risk <= 0:
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
        r
        for r in results
        if r > 0
    ]

    losers = [
        r
        for r in results
        if r < 0
    ]

    gross_profit = sum(winners)
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

    total_r = sum(results)

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
            worst_exp[
                "label"
            ],
        "worst_rolling_3y_total_r":
            worst_total[
                "total_r"
            ],
        "worst_rolling_3y_total_r_label":
            worst_total[
                "label"
            ],
    }


def make_result_row(
    family,
    label,
    eligible,
    trades,
    ignored,
    years,
    parameter_name=None,
    parameter_value=None,
):
    full = stats_for_trades(
        trades
    )

    row = {
        "family":
            family,
        "label":
            label,
        "parameter_name":
            parameter_name,
        "parameter_value":
            parameter_value,
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

    row.update(
        rolling_3y_worst(
            trades
        )
    )

    return row


# ============================================================
# FILTER HELPERS
# ============================================================

def run_filter(
    rows,
    predicate,
):
    return [
        row
        for row in rows
        if predicate(row)
    ]


def daily_valid(
    signal,
):
    return (
        signal["daily"]
        is not None
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
                "Precomputing raw bullish engulfing features",
        })

        atr = atr_series(
            h1
        )

        daily_state = (
            prepare_daily(
                daily
            )
        )

        raw = build_raw_candidates(
            h1,
            atr,
            daily_state,
        )

        STATUS[
            "raw_candidates"
        ] = len(raw)

        years = (
            RESEARCH_TO
            - RESEARCH_FROM
        ).total_seconds() / (
            365.2425
            * 86400
        )

        rows = []

        def test(
            family,
            label,
            eligible,
            parameter_name=None,
            parameter_value=None,
        ):
            trades, ignored = (
                simulate_variant(
                    h1,
                    eligible,
                )
            )

            rows.append(
                make_result_row(
                    family,
                    label,
                    eligible,
                    trades,
                    ignored,
                    years,
                    parameter_name,
                    parameter_value,
                )
            )

            print(
                f"{family}: {label} "
                f"-> {len(trades)} trades",
                flush=True,
            )

        STATUS.update({
            "state":
                "running",
            "message":
                "Running single-factor discovery tests",
        })

        # ----------------------------------------------------
        # RAW
        # ----------------------------------------------------

        test(
            "RAW",
            "RAW_BR1.00",
            raw,
        )

        # ----------------------------------------------------
        # BODY RATIO
        # ----------------------------------------------------

        for value in (
            BODY_RATIO_VALUES
        ):
            test(
                "BODY_RATIO",
                f"BR_GE_{value:.2f}",
                run_filter(
                    raw,
                    lambda x, v=value:
                        x["body_ratio"]
                        >= v,
                ),
                "minimum_body_ratio",
                value,
            )

        # ----------------------------------------------------
        # STRONG CLOSE
        # ----------------------------------------------------

        for value in (
            STRONG_CLOSE_VALUES
        ):
            test(
                "STRONG_CLOSE",
                f"CLOSE_LOC_GE_{value:.2f}",
                run_filter(
                    raw,
                    lambda x, v=value:
                        x["close_location"]
                        >= v,
                ),
                "minimum_close_location",
                value,
            )

        # ----------------------------------------------------
        # LOWER WICK
        # ----------------------------------------------------

        for value in (
            LOWER_WICK_VALUES
        ):
            test(
                "LOWER_WICK",
                f"LOWER_WICK_BODY_GE_{value:.2f}",
                run_filter(
                    raw,
                    lambda x, v=value:
                        x["lower_wick_body"]
                        >= v,
                ),
                "minimum_lower_wick_body_ratio",
                value,
            )

        # ----------------------------------------------------
        # UPPER WICK MAX
        # ----------------------------------------------------

        for value in (
            UPPER_WICK_MAX_VALUES
        ):
            test(
                "UPPER_WICK_MAX",
                f"UPPER_WICK_BODY_LE_{value:.2f}",
                run_filter(
                    raw,
                    lambda x, v=value:
                        x["upper_wick_body"]
                        <= v,
                ),
                "maximum_upper_wick_body_ratio",
                value,
            )

        # ----------------------------------------------------
        # BODY ATR
        # ----------------------------------------------------

        for value in (
            BODY_ATR_VALUES
        ):
            test(
                "BODY_ATR",
                f"BODY_ATR_GE_{value:.2f}",
                run_filter(
                    raw,
                    lambda x, v=value:
                        x["body_atr"]
                        >= v,
                ),
                "minimum_body_atr",
                value,
            )

        # ----------------------------------------------------
        # RANGE ATR
        # ----------------------------------------------------

        for value in (
            RANGE_ATR_VALUES
        ):
            test(
                "RANGE_ATR",
                f"RANGE_ATR_GE_{value:.2f}",
                run_filter(
                    raw,
                    lambda x, v=value:
                        x["range_atr"]
                        >= v,
                ),
                "minimum_range_atr",
                value,
            )

        # ----------------------------------------------------
        # MAX STOP ATR
        # ----------------------------------------------------

        for value in (
            MAX_STOP_ATR_VALUES
        ):
            test(
                "MAX_STOP_ATR",
                f"STOP_ATR_LE_{value:.2f}",
                run_filter(
                    raw,
                    lambda x, v=value:
                        x["stop_atr"]
                        <= v,
                ),
                "maximum_stop_atr",
                value,
            )

        # ----------------------------------------------------
        # STRUCTURE
        # ----------------------------------------------------

        for lookback in (
            STRUCTURE_LOOKBACK_VALUES
        ):
            for distance in (
                STRUCTURE_DISTANCE_VALUES
            ):
                test(
                    "STRUCTURE",
                    (
                        f"STRUCT_{lookback}_"
                        f"D{distance:.2f}"
                    ),
                    run_filter(
                        raw,
                        lambda x,
                        lb=lookback,
                        d=distance:
                            x["structure"][lb]
                            <= d,
                    ),
                    (
                        "structure_lookback_"
                        "max_distance_atr"
                    ),
                    (
                        f"{lookback}/"
                        f"{distance:.2f}"
                    ),
                )

        # ----------------------------------------------------
        # MOMENTUM
        # ----------------------------------------------------

        for hours in (
            MOMENTUM_HOURS
        ):
            for value in (
                MOMENTUM_ATR_VALUES
            ):
                test(
                    "UP_MOMENTUM",
                    (
                        f"MOM_{hours}H_"
                        f"GE_{value:.2f}ATR"
                    ),
                    run_filter(
                        raw,
                        lambda x,
                        h=hours,
                        v=value:
                            x["momentum"][h]
                            >= v,
                    ),
                    (
                        f"minimum_"
                        f"{hours}h_momentum_atr"
                    ),
                    value,
                )

        # ----------------------------------------------------
        # DAILY CLOSE > EMA
        # ----------------------------------------------------

        for length in (
            DAILY_EMA_VALUES
        ):
            test(
                "DAILY_CLOSE_EMA",
                (
                    f"PREV_D_CLOSE_GT_"
                    f"EMA{length}"
                ),
                run_filter(
                    raw,
                    lambda x, n=length:
                        daily_valid(x)
                        and x["daily"].get(
                            f"ema{n}"
                        ) is not None
                        and x["daily"]["close"]
                        > x["daily"][
                            f"ema{n}"
                        ],
                ),
                "daily_close_ema",
                length,
            )

        # ----------------------------------------------------
        # DAILY ALIGNMENT
        # ----------------------------------------------------

        for (
            fast,
            slow,
        ) in DAILY_ALIGNMENTS:
            test(
                "DAILY_ALIGNMENT",
                (
                    f"PREV_D_EMA{fast}_"
                    f"GT_EMA{slow}"
                ),
                run_filter(
                    raw,
                    lambda x,
                    f=fast,
                    s=slow:
                        daily_valid(x)
                        and x["daily"].get(
                            f"ema{f}"
                        ) is not None
                        and x["daily"].get(
                            f"ema{s}"
                        ) is not None
                        and x["daily"][
                            f"ema{f}"
                        ]
                        > x["daily"][
                            f"ema{s}"
                        ],
                ),
                "daily_alignment",
                f"{fast}>{slow}",
            )

        # ----------------------------------------------------
        # DAILY ATR RATIO
        # ----------------------------------------------------

        for value in (
            DAILY_ATR_RATIO_VALUES
        ):
            test(
                "DAILY_ATR_RATIO",
                (
                    f"PREV_D_ATR14_DIV_"
                    f"MEAN50_GE_{value:.2f}"
                ),
                run_filter(
                    raw,
                    lambda x, v=value:
                        daily_valid(x)
                        and x["daily"].get(
                            "atr_ratio50"
                        ) is not None
                        and x["daily"][
                            "atr_ratio50"
                        ] >= v,
                ),
                "minimum_daily_atr_ratio_50",
                value,
            )

        # ----------------------------------------------------
        # NY HOUR EXCLUSIONS
        # ----------------------------------------------------

        for hour in range(24):
            test(
                "NY_HOUR_EXCLUSION",
                f"EXCLUDE_NY_HOUR_{hour:02d}",
                run_filter(
                    raw,
                    lambda x, h=hour:
                        x["ny_hour"] != h,
                ),
                "excluded_ny_hour",
                hour,
            )

        # ----------------------------------------------------
        # WEEKDAY EXCLUSIONS
        # ----------------------------------------------------

        weekday_names = {
            0: "MON",
            1: "TUE",
            2: "WED",
            3: "THU",
            4: "FRI",
        }

        for weekday in range(5):
            test(
                "WEEKDAY_EXCLUSION",
                (
                    f"EXCLUDE_"
                    f"{weekday_names[weekday]}"
                ),
                run_filter(
                    raw,
                    lambda x, d=weekday:
                        x["ny_weekday"]
                        != d,
                ),
                "excluded_weekday",
                weekday,
            )

        # ----------------------------------------------------
        # CURRENT LIVE CONTROL
        # ----------------------------------------------------

        current_live = run_filter(
            raw,
            lambda x:
                x["structure"][17]
                <= 0.55
                and daily_valid(x)
                and x["daily"].get(
                    "ema425"
                ) is not None
                and x["daily"]["close"]
                > x["daily"]["ema425"]
                and x["ny_hour"]
                not in {1, 2}
                and x["ny_weekday"]
                not in {2, 3},
        )

        test(
            "CURRENT_CONTROL",
            "CURRENT_LIVE",
            current_live,
        )

        # ----------------------------------------------------
        # SAVE
        # ----------------------------------------------------

        df = pd.DataFrame(
            rows
        )

        df = df.sort_values(
            by=[
                "profit_factor",
                "expectancy_r",
                "total_r",
            ],
            ascending=[
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
                "USD/JPY single-factor discovery complete",
            "rows_saved":
                len(df),
            "output_file":
                OUTPUT_FILE,
        })

        print()
        print("=" * 100)
        print(
            "USD/JPY LONG SINGLE-FACTOR DISCOVERY COMPLETE"
        )
        print("=" * 100)
        print(
            f"Rows saved: {len(df)}"
        )
        print(
            f"Output: {OUTPUT_FILE}"
        )
        print()
        print(
            df.head(
                30
            ).to_string(
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
            "USD/JPY Long Single-Factor Edge Discovery",
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
            "usdjpy-long-single-factor"
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
