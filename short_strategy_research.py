import os
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# USD/CAD LONG - SINGLE-FACTOR EDGE DISCOVERY
#
# RESEARCH ONLY - NEVER SUBMITS ORDERS.
#
# PURPOSE
# ------------------------------------------------------------
# Start from a RAW bullish-engulfing long baseline and test
# individual filters ONE AT A TIME.
#
# This is NOT a combination optimiser.
#
# We are looking for:
#   - independent edge
#   - broad monotonic / plateau behaviour
#   - stability across eras
#   - useful frequency/quality trade-offs
#
# ============================================================
# LOCKED COMMON BACKTEST CONVENTIONS
#
# OANDA USD_CAD midpoint H1
# Bullish engulfing:
#   previous candle bearish
#   current candle bullish
#   current open <= previous close
#   current close >= previous open
#
# ATR14:
#   Wilder/RMA, SMA-seeded
#
# Tick size:
#   0.00001
#
# Entry reference:
#   signal close
#
# Backtest adverse long fill:
#   signal close + 5 ticks
#
# Stop:
#   signal low - 10 ticks
#
# Target:
#   reference close + (reference close - stop) * RR
#
# Actual R:
#   (exit - actual fill) / (actual fill - stop)
#
# Pyramiding:
#   0
#
# Same-bar target + stop:
#   compare candle open->high vs open->low
#   high closer => target first
#   otherwise stop first
#
# New signal on exact H1 exit bar:
#   allowed
#
# Research window:
#   2002-05-06 20:00 UTC -> current completed UTC hour
#
# Daily alignment:
#   17:00 America/New_York
#   previous COMPLETED daily candle only
#
# ============================================================
# RAW BASELINE
#
# Bullish engulfing
# minimum body ratio = 1.00
# RR = 3.50
# stop buffer = 10 ticks
# slippage = 5 ticks
#
# NO:
#   structure
#   wick
#   close quality
#   range
#   momentum
#   stop-size
#   daily regime
#   EMA alignment
#   ATR regime
#   timing
#   weekday
#
# ============================================================
# FACTOR FAMILIES
#
# BODY RATIO
# CLOSE LOCATION
# LOWER WICK / BODY
# RANGE / ATR
# STOP SIZE / ATR
# STRUCTURE LOOKBACK + DISTANCE
# MOMENTUM 6/12/24/48H
# DAILY CLOSE VS EMA
# DAILY EMA ALIGNMENT
# DAILY ATR REGIME
# NY HOUR EXCLUSIONS
# WEEKDAY EXCLUSIONS
#
# ============================================================
# OUTPUT
#
# /download
#
# One CSV with:
#   family
#   variant
#   threshold/config
#   full-history stats
#   four-era stats
#   recent 2/5/10-year stats
#   worst rolling 3y
#
# ============================================================


app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_URL = "https://api-fxtrade.oanda.com"

INSTRUMENT = "USD_CAD"

TICK_SIZE = 0.00001
STOP_BUFFER_TICKS = 10
BACKTEST_SLIPPAGE_TICKS = 5
REWARD_RISK = 3.50

ATR_LENGTH = 14

NY_TZ = ZoneInfo("America/New_York")

DAILY_ALIGNMENT_HOUR = 17
DAILY_ALIGNMENT_TIMEZONE = "America/New_York"

H1_CHUNK_DAYS = 180
D_CHUNK_DAYS = 1500

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

H1_WARMUP_DAYS = 200
D_WARMUP_DAYS = 2500

OUTPUT_FILE = (
    "usdcad_long_single_factor_edges.csv"
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


# ============================================================
# TEST DEFINITIONS
# ============================================================

TESTS = []


def add_test(
    family,
    variant,
    **kwargs
):
    row = {
        "family": family,
        "variant": variant,
    }

    row.update(
        kwargs
    )

    TESTS.append(
        row
    )


# RAW
add_test(
    "RAW",
    "BASELINE",
)

# BODY RATIO
for value in [
    1.00,
    1.05,
    1.10,
    1.20,
    1.30,
    1.40,
    1.50,
    1.75,
    2.00,
]:
    add_test(
        "BODY_RATIO",
        f">={value:.2f}",
        minimum_body_ratio=value,
    )

# CLOSE LOCATION
for value in [
    0.50,
    0.55,
    0.60,
    0.65,
    0.70,
    0.75,
    0.80,
    0.85,
    0.90,
]:
    add_test(
        "CLOSE_LOCATION",
        f">={value:.2f}",
        minimum_close_location=value,
    )

# LOWER WICK / BODY
for value in [
    0.00,
    0.05,
    0.10,
    0.15,
    0.20,
    0.25,
    0.30,
    0.40,
    0.50,
    0.75,
    1.00,
]:
    add_test(
        "LOWER_WICK_BODY",
        f">={value:.2f}",
        minimum_lower_wick_body=value,
    )

# RANGE / ATR
for value in [
    0.50,
    0.70,
    0.90,
    1.10,
    1.30,
    1.50,
    1.75,
    2.00,
]:
    add_test(
        "RANGE_ATR",
        f">={value:.2f}",
        minimum_range_atr=value,
    )

# STOP SIZE / ATR
for value in [
    0.75,
    1.00,
    1.25,
    1.50,
    1.75,
    2.00,
    2.50,
    3.00,
]:
    add_test(
        "STOP_SIZE_ATR",
        f"<={value:.2f}",
        maximum_stop_size_atr=value,
    )

# STRUCTURE
for lookback in [
    10,
    20,
    30,
    40,
    60,
    90,
    120,
]:
    for distance in [
        0.05,
        0.10,
        0.15,
        0.20,
        0.25,
        0.35,
        0.50,
        0.75,
    ]:
        add_test(
            "STRUCTURE",
            (
                f"lb{lookback}_"
                f"d{distance:.2f}"
            ),
            structure_lookback=lookback,
            maximum_structure_distance_atr=distance,
        )

# MOMENTUM
for lookback in [
    6,
    12,
    24,
    48,
]:
    for minimum in [
        -1.00,
        -0.50,
        -0.25,
        0.00,
        0.25,
        0.50,
        0.75,
        1.00,
        1.50,
    ]:
        add_test(
            f"MOMENTUM_{lookback}H",
            f">={minimum:.2f}",
            momentum_lookback=lookback,
            minimum_momentum_atr=minimum,
        )

# DAILY CLOSE ABOVE EMA
for length in [
    20,
    30,
    40,
    50,
    70,
    100,
    150,
    200,
    250,
    300,
    400,
]:
    add_test(
        "DAILY_CLOSE_ABOVE_EMA",
        f"EMA{length}",
        daily_close_above_ema=length,
    )

# DAILY EMA ALIGNMENT
for fast, slow in [
    (10, 50),
    (20, 50),
    (20, 100),
    (30, 100),
    (50, 100),
    (50, 150),
    (50, 200),
    (100, 200),
]:
    add_test(
        "DAILY_EMA_ALIGNMENT",
        f"EMA{fast}>EMA{slow}",
        daily_fast_ema=fast,
        daily_slow_ema=slow,
    )

# DAILY ATR REGIME
for threshold in [
    0.70,
    0.80,
    0.90,
    1.00,
    1.10,
    1.20,
]:
    add_test(
        "DAILY_ATR_RATIO",
        f">={threshold:.2f}",
        minimum_daily_atr_ratio_50=threshold,
    )

# SINGLE NY HOUR EXCLUSIONS
for hour in range(24):
    add_test(
        "NY_HOUR_EXCLUDE",
        f"exclude_{hour:02d}",
        excluded_ny_hour=hour,
    )

# WEEKDAY EXCLUSIONS
weekday_names = {
    0: "Mon",
    1: "Tue",
    2: "Wed",
    3: "Thu",
    4: "Fri",
}

for weekday in range(5):
    add_test(
        "WEEKDAY_EXCLUDE",
        f"exclude_{weekday_names[weekday]}",
        excluded_weekday=weekday,
    )


TOTAL_TESTS = len(
    TESTS
)


STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "service": "USD/CAD Long Single-Factor Edge Discovery",
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "total_tests": TOTAL_TESTS,
    "completed_tests": 0,
    "rows_saved": 0,
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
        "Authorization": (
            f"Bearer {OANDA_TOKEN}"
        )
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


def parse_candle(
    raw
):
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
        "dailyAlignment": (
            DAILY_ALIGNMENT_HOUR
        ),
        "alignmentTimezone": (
            DAILY_ALIGNMENT_TIMEZONE
        ),
    }

    data = oanda_get(
        (
            f"/v3/instruments/"
            f"{INSTRUMENT}/candles"
        ),
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
            candle is not None
        ):
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

    while (
        cursor < end
    ):
        chunk_end = min(
            cursor
            + timedelta(
                days=chunk_days
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
                granularity,
                cursor,
                chunk_end,
            )
        )

        for candle in (
            chunk
        ):
            by_time[
                candle["time"]
            ] = candle

        cursor = (
            chunk_end
        )

    candles = list(
        by_time.values()
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
    candles
):
    values = []

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

        values.append(
            tr
        )

    return values


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
    ] = (
        initial
    )

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

        previous = current

    return result


def atr_series(
    candles,
    length,
):
    return rma_series(
        true_ranges(
            candles
        ),
        length,
    )


def ema_series(
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

    multiplier = (
        2.0
        / (
            length + 1.0
        )
    )

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
    ] = (
        initial
    )

    previous = initial

    for index in range(
        length,
        len(values),
    ):
        current = (
            (
                values[
                    index
                ]
                - previous
            )
            * multiplier
            + previous
        )

        result[
            index
        ] = current

        previous = current

    return result


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
        window = (
            values[
                index
                - length
                + 1:
                index
                + 1
            ]
        )

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
# DAILY PRECOMPUTE
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

    if (
        ny_time < candidate
    ):
        candidate = (
            candidate
            - timedelta(
                days=1
            )
        )

    return (
        candidate
        .astimezone(
            timezone.utc
        )
    )


def prepare_daily(
    daily
):
    closes = [
        candle[
            "close"
        ]
        for candle
        in daily
    ]

    ema_lengths = set()

    for test in (
        TESTS
    ):
        if test.get(
            "daily_close_above_ema"
        ) is not None:
            ema_lengths.add(
                int(
                    test[
                        "daily_close_above_ema"
                    ]
                )
            )

        if test.get(
            "daily_fast_ema"
        ) is not None:
            ema_lengths.add(
                int(
                    test[
                        "daily_fast_ema"
                    ]
                )
            )

        if test.get(
            "daily_slow_ema"
        ) is not None:
            ema_lengths.add(
                int(
                    test[
                        "daily_slow_ema"
                    ]
                )
            )

    emas = {
        length: ema_series(
            closes,
            length,
        )
        for length
        in sorted(
            ema_lengths
        )
    }

    daily_atr = (
        atr_series(
            daily,
            14,
        )
    )

    daily_atr_mean50 = (
        rolling_mean_optional(
            daily_atr,
            50,
        )
    )

    rows = []

    for index, candle in enumerate(
        daily
    ):
        row = {
            "time": (
                candle[
                    "time"
                ]
            ),
            "close": (
                candle[
                    "close"
                ]
            ),
            "atr14": (
                daily_atr[
                    index
                ]
            ),
            "atr_ratio_50": (
                (
                    daily_atr[
                        index
                    ]
                    / daily_atr_mean50[
                        index
                    ]
                )
                if (
                    daily_atr[
                        index
                    ] is not None
                    and daily_atr_mean50[
                        index
                    ] is not None
                    and daily_atr_mean50[
                        index
                    ] > 0
                )
                else None
            ),
            "emas": {
                length: (
                    emas[
                        length
                    ][
                        index
                    ]
                )
                for length
                in emas
            },
        }

        rows.append(
            row
        )

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

    for row in (
        daily_state
    ):
        if (
            row[
                "time"
            ]
            < session_start
        ):
            selected = row
        else:
            break

    return selected


# ============================================================
# RAW CANDIDATE PRECOMPUTE
# ============================================================

def build_raw_candidates(
    h1,
    atr,
    daily_state,
):
    candidates = []

    max_structure = 120
    max_momentum = 48

    start_index = max(
        ATR_LENGTH,
        max_structure,
        max_momentum,
    )

    for index in range(
        start_index,
        len(h1),
    ):
        signal = h1[
            index
        ]

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

        current_atr = (
            atr[
                index
            ]
        )

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
            and signal["close"]
            > signal["open"]
            and signal["open"]
            <= previous["close"]
            and signal["close"]
            >= previous["open"]
        )

        if not (
            bullish_engulfing
        ):
            continue

        body_ratio = (
            current_body
            / previous_body
        )

        close_location = (
            signal["close"]
            - signal["low"]
        ) / signal_range

        lower_wick = (
            min(
                signal["open"],
                signal["close"],
            )
            - signal["low"]
        )

        lower_wick_body = (
            lower_wick
            / current_body
        )

        range_atr = (
            signal_range
            / current_atr
        )

        stop = (
            signal["low"]
            - STOP_BUFFER_TICKS
            * TICK_SIZE
        )

        stop_size_atr = (
            signal["close"]
            - stop
        ) / current_atr

        momentum = {}

        for lookback in [
            6,
            12,
            24,
            48,
        ]:
            momentum[
                lookback
            ] = (
                signal["close"]
                - h1[
                    index
                    - lookback
                ]["close"]
            ) / current_atr

        structure_distances = {}

        for lookback in [
            10,
            20,
            30,
            40,
            60,
            90,
            120,
        ]:
            previous_low = min(
                candle["low"]
                for candle
                in h1[
                    index
                    - lookback:
                    index
                ]
            )

            structure_distances[
                lookback
            ] = (
                signal["low"]
                - previous_low
            ) / current_atr

        daily = (
            previous_completed_daily(
                signal[
                    "time"
                ],
                daily_state,
            )
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
                signal[
                    "time"
                ]
            ),
            "body_ratio": (
                body_ratio
            ),
            "close_location": (
                close_location
            ),
            "lower_wick_body": (
                lower_wick_body
            ),
            "range_atr": (
                range_atr
            ),
            "stop_size_atr": (
                stop_size_atr
            ),
            "momentum": (
                momentum
            ),
            "structure_distances": (
                structure_distances
            ),
            "daily": (
                daily
            ),
            "ny_hour": (
                ny_time.hour
            ),
            "weekday": (
                ny_time.weekday()
            ),
        })

    return candidates


# ============================================================
# FILTER TEST
# ============================================================

def passes_test(
    signal,
    test,
):
    # RAW baseline still requires body ratio >= 1.00.
    minimum_body_ratio = (
        test.get(
            "minimum_body_ratio",
            1.00,
        )
    )

    if (
        signal[
            "body_ratio"
        ]
        < minimum_body_ratio
    ):
        return False

    minimum_close = test.get(
        "minimum_close_location"
    )

    if (
        minimum_close is not None
        and signal[
            "close_location"
        ] < minimum_close
    ):
        return False

    minimum_wick = test.get(
        "minimum_lower_wick_body"
    )

    if (
        minimum_wick is not None
        and signal[
            "lower_wick_body"
        ] < minimum_wick
    ):
        return False

    minimum_range = test.get(
        "minimum_range_atr"
    )

    if (
        minimum_range is not None
        and signal[
            "range_atr"
        ] < minimum_range
    ):
        return False

    maximum_stop = test.get(
        "maximum_stop_size_atr"
    )

    if (
        maximum_stop is not None
        and signal[
            "stop_size_atr"
        ] > maximum_stop
    ):
        return False

    structure_lookback = test.get(
        "structure_lookback"
    )

    if (
        structure_lookback
        is not None
    ):
        maximum_distance = test[
            "maximum_structure_distance_atr"
        ]

        if (
            signal[
                "structure_distances"
            ][
                structure_lookback
            ]
            > maximum_distance
        ):
            return False

    momentum_lookback = test.get(
        "momentum_lookback"
    )

    if (
        momentum_lookback
        is not None
    ):
        minimum_momentum = test[
            "minimum_momentum_atr"
        ]

        if (
            signal[
                "momentum"
            ][
                momentum_lookback
            ]
            < minimum_momentum
        ):
            return False

    daily_close_ema = test.get(
        "daily_close_above_ema"
    )

    if (
        daily_close_ema
        is not None
    ):
        daily = signal[
            "daily"
        ]

        if (
            daily is None
            or daily[
                "emas"
            ].get(
                daily_close_ema
            ) is None
            or not (
                daily[
                    "close"
                ]
                > daily[
                    "emas"
                ][
                    daily_close_ema
                ]
            )
        ):
            return False

    fast_ema = test.get(
        "daily_fast_ema"
    )

    slow_ema = test.get(
        "daily_slow_ema"
    )

    if (
        fast_ema is not None
        and slow_ema is not None
    ):
        daily = signal[
            "daily"
        ]

        if (
            daily is None
            or daily[
                "emas"
            ].get(
                fast_ema
            ) is None
            or daily[
                "emas"
            ].get(
                slow_ema
            ) is None
            or not (
                daily[
                    "emas"
                ][
                    fast_ema
                ]
                > daily[
                    "emas"
                ][
                    slow_ema
                ]
            )
        ):
            return False

    daily_atr_ratio = test.get(
        "minimum_daily_atr_ratio_50"
    )

    if (
        daily_atr_ratio
        is not None
    ):
        daily = signal[
            "daily"
        ]

        if (
            daily is None
            or daily[
                "atr_ratio_50"
            ] is None
            or daily[
                "atr_ratio_50"
            ] < daily_atr_ratio
        ):
            return False

    excluded_hour = test.get(
        "excluded_ny_hour"
    )

    if (
        excluded_hour is not None
        and signal[
            "ny_hour"
        ] == excluded_hour
    ):
        return False

    excluded_weekday = test.get(
        "excluded_weekday"
    )

    if (
        excluded_weekday
        is not None
        and signal[
            "weekday"
        ] == excluded_weekday
    ):
        return False

    return True


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
        return (
            EXIT_CACHE[
                signal_index
            ]
        )

    signal = h1[
        signal_index
    ]

    reference_entry = (
        signal[
            "close"
        ]
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
        reference_risk
        <= 0
    ):
        raise RuntimeError(
            "Invalid reference risk"
        )

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
        actual_risk
        <= 0
    ):
        raise RuntimeError(
            "Invalid actual risk"
        )

    for index in range(
        signal_index + 1,
        len(h1),
    ):
        candle = h1[
            index
        ]

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
            distance_high = abs(
                candle["high"]
                - candle["open"]
            )

            distance_low = abs(
                candle["open"]
                - candle["low"]
            )

            if (
                distance_high
                < distance_low
            ):
                exit_price = (
                    target
                )
            else:
                exit_price = (
                    stop
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
            "signal_index": (
                signal_index
            ),
            "signal_time": (
                signal["time"]
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
                exit_price
                - backtest_entry
            ) / actual_risk,
        }

        EXIT_CACHE[
            signal_index
        ] = result

        return result

    result = {
        "signal_index": (
            signal_index
        ),
        "signal_time": (
            signal["time"]
        ),
        "exit_index": None,
        "exit_time": None,
        "result_r": None,
    }

    EXIT_CACHE[
        signal_index
    ] = result

    return result


def simulate_variant(
    h1,
    eligible,
):
    trades = []

    position_exit_index = (
        -1
    )

    ignored = 0

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
                "result_r"
            ] is None
        ):
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
    )


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

        selected.append(
            trade
        )

    if not (
        selected
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
        in selected
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

    gross_profit = (
        sum(
            winners
        )
    )

    gross_loss = abs(
        sum(
            losers
        )
    )

    if (
        gross_loss > 0
    ):
        pf = (
            gross_profit
            / gross_loss
        )
    elif (
        gross_profit > 0
    ):
        pf = 999.0
    else:
        pf = 0.0

    total_r = (
        sum(
            results
        )
    )

    equity = 0.0
    peak = 0.0
    max_dd = 0.0

    current_streak = 0
    longest_streak = 0

    for result in (
        results
    ):
        equity += result

        peak = max(
            peak,
            equity,
        )

        max_dd = min(
            max_dd,
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
            pf,
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
            max_dd,
            2,
        ),
        "longest_loss_streak": (
            longest_streak
        ),
    }


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
    trades
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

        if (
            start >= end
        ):
            continue

        stats = (
            stats_for_trades(
                trades,
                start,
                end,
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
            })

    if not rows:
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
            row["pf"]
        ),
    )

    worst_exp = min(
        rows,
        key=lambda row: (
            row["expectancy"]
        ),
    )

    worst_total = min(
        rows,
        key=lambda row: (
            row["total_r"]
        ),
    )

    return {
        "worst_rolling_3y_pf": (
            worst_pf["pf"]
        ),
        "worst_rolling_3y_pf_label": (
            worst_pf["label"]
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


def build_result_row(
    test,
    eligible,
    trades,
    ignored,
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
        "variant": (
            test[
                "variant"
            ]
        ),
        "eligible_signals": (
            len(
                eligible
            )
        ),
        "ignored_due_to_open_trade": (
            ignored
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
                "total_r"
            ]
            / years,
            3,
        ),
    }

    # Preserve tested parameters in output.
    for key, value in (
        test.items()
    ):
        if key in {
            "family",
            "variant",
        }:
            continue

        row[
            key
        ] = value

    minimum_era_pf = None
    profitable_eras = 0

    for (
        era_name,
        era_start,
        era_end,
    ) in ERAS:
        stats = (
            stats_for_trades(
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
        )

        row[
            f"{era_name}_trades"
        ] = (
            stats[
                "trades"
            ]
        )

        row[
            f"{era_name}_pf"
        ] = (
            stats[
                "profit_factor"
            ]
        )

        row[
            f"{era_name}_r"
        ] = (
            stats[
                "total_r"
            ]
        )

        row[
            f"{era_name}_expectancy"
        ] = (
            stats[
                "expectancy_r"
            ]
        )

        if (
            stats[
                "trades"
            ] >= 5
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
                stats[
                    "total_r"
                ] > 0
            ):
                profitable_eras += 1

    row[
        "minimum_era_pf_5_plus"
    ] = (
        minimum_era_pf
    )

    row[
        "profitable_eras"
    ] = (
        profitable_eras
    )

    for years_back in [
        2,
        5,
        10,
    ]:
        start = (
            subtract_years_safe(
                RESEARCH_TO,
                years_back,
            )
        )

        stats = (
            stats_for_trades(
                trades,
                start,
                RESEARCH_TO,
            )
        )

        row[
            f"last_{years_back}y_trades"
        ] = (
            stats[
                "trades"
            ]
        )

        row[
            f"last_{years_back}y_pf"
        ] = (
            stats[
                "profit_factor"
            ]
        )

        row[
            f"last_{years_back}y_r"
        ] = (
            stats[
                "total_r"
            ]
        )

        row[
            f"last_{years_back}y_expectancy"
        ] = (
            stats[
                "expectancy_r"
            ]
        )

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
            "state": (
                "fetching_h1"
            ),
            "message": (
                "Fetching USD/CAD H1 history"
            ),
        })

        h1 = (
            fetch_chunked(
                "H1",
                RESEARCH_FROM
                - timedelta(
                    days=H1_WARMUP_DAYS
                ),
                RESEARCH_TO,
                H1_CHUNK_DAYS,
            )
        )

        if not h1:
            raise RuntimeError(
                "No H1 candles returned"
            )

        STATUS.update({
            "state": (
                "fetching_daily"
            ),
            "message": (
                "Fetching USD/CAD daily history"
            ),
        })

        daily = (
            fetch_chunked(
                "D",
                RESEARCH_FROM
                - timedelta(
                    days=D_WARMUP_DAYS
                ),
                RESEARCH_TO,
                D_CHUNK_DAYS,
            )
        )

        if not daily:
            raise RuntimeError(
                "No daily candles returned"
            )

        STATUS.update({
            "state": (
                "precomputing"
            ),
            "message": (
                "Precomputing raw engulfing features"
            ),
        })

        h1_atr = (
            atr_series(
                h1,
                ATR_LENGTH,
            )
        )

        daily_state = (
            prepare_daily(
                daily
            )
        )

        raw_candidates = (
            build_raw_candidates(
                h1,
                h1_atr,
                daily_state,
            )
        )

        STATUS[
            "raw_bullish_engulfing_candidates"
        ] = len(
            raw_candidates
        )

        years = (
            RESEARCH_TO
            - RESEARCH_FROM
        ).total_seconds() / (
            365.2425
            * 86400
        )

        STATUS.update({
            "state": (
                "running"
            ),
            "message": (
                "Running single-factor edge discovery"
            ),
        })

        rows = []

        for (
            completed,
            test,
        ) in enumerate(
            TESTS,
            start=1,
        ):
            eligible = [
                signal
                for signal
                in raw_candidates
                if passes_test(
                    signal,
                    test,
                )
            ]

            (
                trades,
                ignored,
            ) = (
                simulate_variant(
                    h1,
                    eligible,
                )
            )

            row = (
                build_result_row(
                    test,
                    eligible,
                    trades,
                    ignored,
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
                completed % 50 == 0
                or completed
                == TOTAL_TESTS
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

        # Keep families grouped and variants readable.
        df = (
            df.sort_values(
                by=[
                    "family",
                    "profit_factor",
                    "expectancy_r",
                    "trades",
                ],
                ascending=[
                    True,
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

        STATUS.update({
            "state": (
                "complete"
            ),
            "message": (
                "USD/CAD long single-factor discovery complete"
            ),
            "completed_tests": (
                TOTAL_TESTS
            ),
            "rows_saved": (
                len(
                    df
                )
            ),
            "output_file": (
                OUTPUT_FILE
            ),
        })

        print()
        print("=" * 90)
        print(
            "USD/CAD LONG SINGLE-FACTOR DISCOVERY COMPLETE"
        )
        print("=" * 90)
        print(
            f"Rows: {len(df)}"
        )
        print(
            f"Saved: {OUTPUT_FILE}"
        )
        print()

    except Exception as error:
        STATUS.update({
            "state": (
                "error"
            ),
            "message": (
                str(
                    error
                )
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
            "USD/CAD Long Single-Factor Edge Discovery"
        ),
        "status": (
            STATUS
        ),
        "instrument": (
            INSTRUMENT
        ),
        "direction": (
            "LONG"
        ),
        "mode": (
            "READ_ONLY_RESEARCH"
        ),
        "orders_supported": (
            False
        ),
        "trading_enabled": (
            False
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


if __name__ == "__main__":
    research_thread = (
        threading.Thread(
            target=run_research,
            name=(
                "usdcad-long-single-factor-discovery"
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
