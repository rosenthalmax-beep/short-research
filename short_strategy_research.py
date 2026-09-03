import os
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# EUR/GBP LONG - SINGLE FACTOR EDGE DISCOVERY
#
# RESEARCH ONLY - NEVER SUBMITS ORDERS.
#
# PURPOSE
# ------------------------------------------------------------
# Start from a RAW bullish-engulfing baseline and test one
# factor at a time.
#
# Locked execution conventions:
# - OANDA midpoint H1 candles
# - bullish engulfing:
#     previous candle bearish
#     current candle bullish
#     current open <= previous close
#     current close >= previous open
# - minimum body ratio baseline = 1.00
# - ATR14 = Wilder/RMA, SMA-seeded
# - tick size = 0.00001
# - reference entry = signal close
# - adverse long fill = signal close + 5 ticks
# - stop = signal low - 10 ticks
# - target = signal close + reference risk * 3.00
# - actual R uses adverse fill
# - pyramiding = 0
# - same-bar target/stop tie:
#     compare open->high vs open->low
#     high closer => target first, else stop first
# - signal on exact exit candle is allowed
# - exits begin from the candle AFTER the signal candle
#
# Daily candles:
# - OANDA dailyAlignment = 17
# - alignmentTimezone = America/New_York
# - previous completed daily candle only
#
# Research:
# - 2002-05-06 20:00 UTC -> current completed UTC hour
#
# SINGLE-FACTOR FAMILIES
# ------------------------------------------------------------
# body ratio
# close location
# lower wick/body
# upper wick/body
# range/ATR
# body/ATR
# stop-size/ATR
# structure lookback x distance
# 6h / 12h / 24h / 48h momentum
# previous completed daily close > EMA
# daily EMA alignment
# daily ATR14 / 50-day mean
# single NY-hour exclusion
# single London-hour exclusion
# single weekday exclusion
#
# Current live control is also included as a reference row.
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
BASE_BODY_RATIO = 1.00

ATR_LENGTH = 14

NY_TZ = ZoneInfo("America/New_York")
LONDON_TZ = ZoneInfo("Europe/London")

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

H1_WARMUP_DAYS = 200
D_WARMUP_DAYS = 2500

OUTPUT_FILE = (
    "eurgbp_long_single_factor_edges.csv"
)

# ============================================================
# ERAS
# ============================================================

ERAS = [
    (
        "2002_2009",
        RESEARCH_FROM,
        datetime(2010, 1, 1, tzinfo=timezone.utc),
    ),
    (
        "2010_2017",
        datetime(2010, 1, 1, tzinfo=timezone.utc),
        datetime(2018, 1, 1, tzinfo=timezone.utc),
    ),
    (
        "2018_2023",
        datetime(2018, 1, 1, tzinfo=timezone.utc),
        datetime(2024, 1, 1, tzinfo=timezone.utc),
    ),
    (
        "2024_present",
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        None,
    ),
]


STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "service": "EUR/GBP Long Single-Factor Edge Discovery",
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
        "Authorization": f"Bearer {OANDA_TOKEN}"
    }


def iso_utc(dt):
    return (
        dt
        .astimezone(timezone.utc)
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
            raw["time"].replace(
                "Z",
                "+00:00",
            )
        ),
        "open": float(mid["o"]),
        "high": float(mid["h"]),
        "low": float(mid["l"]),
        "close": float(mid["c"]),
    }


def fetch_range(
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
        "dailyAlignment": DAILY_ALIGNMENT_HOUR,
        "alignmentTimezone": DAILY_ALIGNMENT_TIMEZONE,
    }

    data = oanda_get(
        f"/v3/instruments/{INSTRUMENT}/candles",
        params,
    )

    candles = []

    for raw in data.get("candles", []):
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
            + timedelta(days=chunk_days),
            end,
        )

        print(
            f"Fetching {granularity}: "
            f"{cursor.date()} -> {chunk_end.date()}",
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

    candles = list(by_time.values())

    candles.sort(
        key=lambda item: item["time"]
    )

    return candles


# ============================================================
# INDICATORS
# ============================================================

def true_ranges(candles):
    values = []

    for index, candle in enumerate(candles):
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
    length,
):
    return rma_series(
        true_ranges(candles),
        length,
    )


def ema_series(
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

    candidate = ny_time.replace(
        hour=DAILY_ALIGNMENT_HOUR,
        minute=0,
        second=0,
        microsecond=0,
    )

    if ny_time < candidate:
        candidate = (
            candidate
            - timedelta(days=1)
        )

    return candidate.astimezone(
        timezone.utc
    )


def prepare_daily(daily):
    closes = [
        candle["close"]
        for candle in daily
    ]

    ema_lengths = [
        20,
        30,
        40,
        50,
        70,
        100,
        125,
        150,
        175,
        200,
        250,
        300,
        400,
    ]

    ema_map = {
        length: ema_series(
            closes,
            length,
        )
        for length in ema_lengths
    }

    daily_atr = atr_series(
        daily,
        14,
    )

    daily_atr_mean50 = (
        rolling_mean_optional(
            daily_atr,
            50,
        )
    )

    rows = []

    for index, candle in enumerate(daily):
        atr_ratio_50 = None

        if (
            daily_atr[index] is not None
            and daily_atr_mean50[index] is not None
            and daily_atr_mean50[index] > 0
        ):
            atr_ratio_50 = (
                daily_atr[index]
                / daily_atr_mean50[index]
            )

        rows.append({
            "time": candle["time"],
            "close": candle["close"],
            "emas": {
                length: ema_map[
                    length
                ][index]
                for length in ema_lengths
            },
            "atr14": daily_atr[index],
            "atr_ratio_50": atr_ratio_50,
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
        if row["time"] < session_start:
            selected = row
        else:
            break

    return selected


# ============================================================
# RAW CANDIDATES
# ============================================================

STRUCTURE_LOOKBACKS = [
    10,
    20,
    30,
    40,
    60,
    90,
    120,
]

def build_raw_candidates(
    h1,
    atr,
    daily_state,
):
    candidates = []

    start_index = max(
        ATR_LENGTH,
        max(STRUCTURE_LOOKBACKS),
        48,
    )

    for index in range(
        start_index,
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

        current_atr = atr[index]

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

        if not bullish_engulfing:
            continue

        body_ratio = (
            current_body
            / previous_body
        )

        if body_ratio < BASE_BODY_RATIO:
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

        close_location = (
            signal["close"]
            - signal["low"]
        ) / signal_range

        lower_wick_body = (
            lower_wick
            / current_body
        )

        upper_wick_body = (
            upper_wick
            / current_body
        )

        range_atr = (
            signal_range
            / current_atr
        )

        body_atr = (
            current_body
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
            momentum[lookback] = (
                signal["close"]
                - h1[
                    index - lookback
                ]["close"]
            ) / current_atr

        structure_distances = {}

        for lookback in STRUCTURE_LOOKBACKS:
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

        daily = previous_completed_daily(
            signal["time"],
            daily_state,
        )

        ny = (
            signal["time"]
            .astimezone(NY_TZ)
        )

        london = (
            signal["time"]
            .astimezone(LONDON_TZ)
        )

        candidates.append({
            "index": index,
            "time": signal["time"],
            "body_ratio": body_ratio,
            "close_location": close_location,
            "lower_wick_body": (
                lower_wick_body
            ),
            "upper_wick_body": (
                upper_wick_body
            ),
            "range_atr": range_atr,
            "body_atr": body_atr,
            "stop_size_atr": (
                stop_size_atr
            ),
            "momentum": momentum,
            "structure_distances": (
                structure_distances
            ),
            "daily": daily,
            "ny_hour": ny.hour,
            "ny_weekday": ny.weekday(),
            "london_hour": london.hour,
            "london_weekday": (
                london.weekday()
            ),
        })

    return candidates


# ============================================================
# TEST DEFINITIONS
# ============================================================

TESTS = []

# RAW baseline.
TESTS.append({
    "family": "RAW",
    "variant": "BODY>=1.00",
})

# Current live exact control.
TESTS.append({
    "family": "CONTROL",
    "variant": "CURRENT_LIVE",
    "current_live": True,
})

# Body ratio.
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
    TESTS.append({
        "family": "BODY_RATIO",
        "variant": f">={value:.2f}",
        "minimum_body_ratio": value,
    })

# Close location.
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
    TESTS.append({
        "family": "CLOSE_LOCATION",
        "variant": f">={value:.2f}",
        "minimum_close_location": value,
    })

# Lower wick.
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
    TESTS.append({
        "family": "LOWER_WICK_BODY",
        "variant": f">={value:.2f}",
        "minimum_lower_wick_body": value,
    })

# Upper wick maximum.
for value in [
    0.10,
    0.20,
    0.30,
    0.40,
    0.50,
    0.75,
    1.00,
]:
    TESTS.append({
        "family": "UPPER_WICK_BODY",
        "variant": f"<={value:.2f}",
        "maximum_upper_wick_body": value,
    })

# Range/ATR.
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
    TESTS.append({
        "family": "RANGE_ATR",
        "variant": f">={value:.2f}",
        "minimum_range_atr": value,
    })

# Body/ATR.
for value in [
    0.25,
    0.40,
    0.50,
    0.60,
    0.70,
    0.80,
    1.00,
    1.20,
]:
    TESTS.append({
        "family": "BODY_ATR",
        "variant": f">={value:.2f}",
        "minimum_body_atr": value,
    })

# Stop size / ATR cap.
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
    TESTS.append({
        "family": "STOP_SIZE_ATR",
        "variant": f"<={value:.2f}",
        "maximum_stop_size_atr": value,
    })

# Structure.
for lookback in STRUCTURE_LOOKBACKS:
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
        TESTS.append({
            "family": "STRUCTURE",
            "variant": (
                f"{lookback}_"
                f"{distance:.2f}"
            ),
            "structure_lookback": (
                lookback
            ),
            "maximum_distance_atr": (
                distance
            ),
        })

# Momentum.
for lookback in [
    6,
    12,
    24,
    48,
]:
    for value in [
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
        TESTS.append({
            "family": (
                f"MOMENTUM_{lookback}H"
            ),
            "variant": (
                f">={value:.2f}"
            ),
            "momentum_lookback": (
                lookback
            ),
            "minimum_momentum_atr": (
                value
            ),
        })

# Daily close > EMA.
for length in [
    20,
    30,
    40,
    50,
    70,
    100,
    125,
    150,
    175,
    200,
    250,
    300,
    400,
]:
    TESTS.append({
        "family": "DAILY_CLOSE_ABOVE_EMA",
        "variant": f"EMA{length}",
        "daily_close_above_ema": length,
    })

# Daily EMA alignment.
for fast, slow in [
    (20, 50),
    (20, 70),
    (20, 100),
    (20, 125),
    (20, 150),
    (30, 100),
    (30, 150),
    (40, 100),
    (50, 100),
    (50, 150),
    (70, 150),
    (100, 150),
    (100, 200),
]:
    TESTS.append({
        "family": "DAILY_EMA_ALIGNMENT",
        "variant": (
            f"EMA{fast}>EMA{slow}"
        ),
        "daily_fast_ema": fast,
        "daily_slow_ema": slow,
    })

# Daily ATR regime.
for value in [
    0.70,
    0.80,
    0.90,
    1.00,
    1.10,
    1.20,
]:
    TESTS.append({
        "family": "DAILY_ATR_RATIO",
        "variant": f">={value:.2f}",
        "minimum_daily_atr_ratio": value,
    })

# Exclude single NY hour.
for hour in range(24):
    TESTS.append({
        "family": "NY_HOUR_EXCLUDE",
        "variant": f"exclude_{hour:02d}",
        "excluded_ny_hour": hour,
    })

# Exclude single London hour.
for hour in range(24):
    TESTS.append({
        "family": "LONDON_HOUR_EXCLUDE",
        "variant": f"exclude_{hour:02d}",
        "excluded_london_hour": hour,
    })

# Exclude one weekday - London local weekday.
for weekday, name in [
    (0, "Mon"),
    (1, "Tue"),
    (2, "Wed"),
    (3, "Thu"),
    (4, "Fri"),
]:
    TESTS.append({
        "family": "WEEKDAY_EXCLUDE",
        "variant": f"exclude_{name}",
        "excluded_weekday": weekday,
    })


# ============================================================
# FILTERS
# ============================================================

def passes_test(
    signal,
    test,
):
    if test.get(
        "current_live",
        False,
    ):
        # Exact current live EUR/GBP long.
        if (
            signal[
                "close_location"
            ] < 0.75
        ):
            return False

        if (
            signal[
                "structure_distances"
            ][20] > 0.20
        ):
            return False

        daily = signal[
            "daily"
        ]

        if daily is None:
            return False

        ema20 = (
            daily[
                "emas"
            ].get(20)
        )

        ema150 = (
            daily[
                "emas"
            ].get(150)
        )

        if (
            ema20 is None
            or ema150 is None
        ):
            return False

        if not (
            daily["close"]
            > ema150
        ):
            return False

        if not (
            ema20
            > ema150
        ):
            return False

        if not (
            signal[
                "london_hour"
            ] >= 8
            and signal[
                "london_hour"
            ] < 17
        ):
            return False

        if (
            signal[
                "london_weekday"
            ] in {
                3,
                4,
            }
        ):
            return False

        return True

    minimum_body_ratio = test.get(
        "minimum_body_ratio"
    )

    if (
        minimum_body_ratio is not None
        and signal[
            "body_ratio"
        ] < minimum_body_ratio
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

    minimum_lower_wick = test.get(
        "minimum_lower_wick_body"
    )

    if (
        minimum_lower_wick is not None
        and signal[
            "lower_wick_body"
        ] < minimum_lower_wick
    ):
        return False

    maximum_upper_wick = test.get(
        "maximum_upper_wick_body"
    )

    if (
        maximum_upper_wick is not None
        and signal[
            "upper_wick_body"
        ] > maximum_upper_wick
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

    minimum_body_atr = test.get(
        "minimum_body_atr"
    )

    if (
        minimum_body_atr is not None
        and signal[
            "body_atr"
        ] < minimum_body_atr
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

    if structure_lookback is not None:
        if (
            signal[
                "structure_distances"
            ][
                structure_lookback
            ]
            > test[
                "maximum_distance_atr"
            ]
        ):
            return False

    momentum_lookback = test.get(
        "momentum_lookback"
    )

    if (
        momentum_lookback is not None
        and signal[
            "momentum"
        ][
            momentum_lookback
        ] < test[
            "minimum_momentum_atr"
        ]
    ):
        return False

    daily_close_ema = test.get(
        "daily_close_above_ema"
    )

    if daily_close_ema is not None:
        daily = signal["daily"]

        if daily is None:
            return False

        ema = daily[
            "emas"
        ].get(
            daily_close_ema
        )

        if (
            ema is None
            or not (
                daily[
                    "close"
                ] > ema
            )
        ):
            return False

    daily_fast = test.get(
        "daily_fast_ema"
    )

    if daily_fast is not None:
        daily = signal["daily"]

        if daily is None:
            return False

        fast = daily[
            "emas"
        ].get(
            daily_fast
        )

        slow = daily[
            "emas"
        ].get(
            test[
                "daily_slow_ema"
            ]
        )

        if (
            fast is None
            or slow is None
            or not (
                fast > slow
            )
        ):
            return False

    minimum_daily_atr = test.get(
        "minimum_daily_atr_ratio"
    )

    if minimum_daily_atr is not None:
        daily = signal["daily"]

        if (
            daily is None
            or daily[
                "atr_ratio_50"
            ] is None
            or daily[
                "atr_ratio_50"
            ] < minimum_daily_atr
        ):
            return False

    excluded_ny_hour = test.get(
        "excluded_ny_hour"
    )

    if (
        excluded_ny_hour is not None
        and signal[
            "ny_hour"
        ] == excluded_ny_hour
    ):
        return False

    excluded_london_hour = test.get(
        "excluded_london_hour"
    )

    if (
        excluded_london_hour is not None
        and signal[
            "london_hour"
        ] == excluded_london_hour
    ):
        return False

    excluded_weekday = test.get(
        "excluded_weekday"
    )

    if (
        excluded_weekday is not None
        and signal[
            "london_weekday"
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
        result = {
            "signal_index": signal_index,
            "signal_time": signal["time"],
            "exit_index": None,
            "exit_time": None,
            "result_r": None,
        }

        EXIT_CACHE[
            signal_index
        ] = result

        return result

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
        result = {
            "signal_index": signal_index,
            "signal_time": signal["time"],
            "exit_index": None,
            "exit_time": None,
            "result_r": None,
        }

        EXIT_CACHE[
            signal_index
        ] = result

        return result

    for index in range(
        signal_index + 1,
        len(h1),
    ):
        candle = h1[index]

        if candle["time"] >= RESEARCH_TO:
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
            "signal_index": signal_index,
            "signal_time": signal["time"],
            "exit_index": index,
            "exit_time": candle["time"],
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
        "signal_index": signal_index,
        "signal_time": signal["time"],
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

        trade = calculate_trade_exit(
            h1,
            signal_index,
        )

        if (
            trade["result_r"]
            is None
        ):
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
        for trade in selected
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
    gross_loss = abs(sum(losers))

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

    for r in results:
        equity += r

        peak = max(
            peak,
            equity,
        )

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
        "longest_loss_streak": (
            longest_streak
        ),
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

        if start >= end:
            continue

        stats = stats_for_trades(
            trades,
            start,
            end,
        )

        if stats["trades"] >= 5:
            rows.append({
                "label": (
                    f"{start_year}_"
                    f"{start_year + 2}"
                ),
                "pf": stats[
                    "profit_factor"
                ],
                "expectancy": stats[
                    "expectancy_r"
                ],
                "total_r": stats[
                    "total_r"
                ],
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
        key=lambda row: row["pf"],
    )

    worst_exp = min(
        rows,
        key=lambda row: row["expectancy"],
    )

    worst_total = min(
        rows,
        key=lambda row: row["total_r"],
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
            worst_exp["label"]
        ),
        "worst_rolling_3y_total_r": (
            worst_total[
                "total_r"
            ]
        ),
        "worst_rolling_3y_total_r_label": (
            worst_total["label"]
        ),
    }


def make_row(
    test,
    eligible,
    trades,
    ignored,
    years,
):
    full = stats_for_trades(trades)

    row = {
        "family": test["family"],
        "variant": test["variant"],
        "eligible_signals": len(eligible),
        "ignored_due_to_open_trade": ignored,
        "trades": full["trades"],
        "trades_per_year": round(
            full["trades"]
            / years,
            2,
        ),
        "winners": full["winners"],
        "losers": full["losers"],
        "win_rate": full["win_rate"],
        "profit_factor": (
            full["profit_factor"]
        ),
        "total_r": full["total_r"],
        "expectancy_r": (
            full["expectancy_r"]
        ),
        "max_drawdown_r": (
            full["max_drawdown_r"]
        ),
        "longest_loss_streak": (
            full[
                "longest_loss_streak"
            ]
        ),
        "annual_r_linear": round(
            full["total_r"]
            / years,
            3,
        ),
    }

    for key, value in test.items():
        if key in {
            "family",
            "variant",
            "current_live",
        }:
            continue

        row[key] = value

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

        if stats["trades"] >= 5:
            if minimum_era_pf is None:
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

            if stats["total_r"] > 0:
                profitable_eras += 1

    row[
        "minimum_era_pf_5_plus"
    ] = minimum_era_pf

    row[
        "profitable_eras"
    ] = profitable_eras

    for years_back in [
        2,
        5,
        10,
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
        ] = stats[
            "total_r"
        ]

        row[
            f"last_{years_back}y_expectancy"
        ] = stats[
            "expectancy_r"
        ]

    row.update(
        rolling_3y_worst(trades)
    )

    return row


# ============================================================
# RUN
# ============================================================

def run_research():
    try:
        STATUS.update({
            "state": "fetching_h1",
            "message": (
                "Fetching EUR/GBP H1 history"
            ),
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

        STATUS.update({
            "state": "fetching_daily",
            "message": (
                "Fetching EUR/GBP daily history"
            ),
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

        if not h1:
            raise RuntimeError(
                "No H1 candles returned"
            )

        if not daily:
            raise RuntimeError(
                "No daily candles returned"
            )

        STATUS.update({
            "state": "precomputing",
            "message": (
                "Precomputing EUR/GBP features"
            ),
        })

        h1_atr = atr_series(
            h1,
            ATR_LENGTH,
        )

        daily_state = prepare_daily(
            daily
        )

        raw_candidates = build_raw_candidates(
            h1,
            h1_atr,
            daily_state,
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
            "state": "running_tests",
            "message": (
                f"Running {len(TESTS)} "
                f"single-factor tests"
            ),
            "total_tests": len(TESTS),
            "completed_tests": 0,
            "raw_candidates": (
                len(raw_candidates)
            ),
        })

        for completed, test in enumerate(
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
            ) = simulate_variant(
                h1,
                eligible,
            )

            row = make_row(
                test,
                eligible,
                trades,
                ignored,
                years,
            )

            rows.append(row)

            STATUS[
                "completed_tests"
            ] = completed

            if (
                completed % 25 == 0
                or completed
                == len(TESTS)
            ):
                print(
                    f"{completed}/{len(TESTS)}",
                    flush=True,
                )

        df = pd.DataFrame(rows)

        # Keep RAW and CONTROL first, then sort research rows
        # by robustness first.
        reference = df[
            df["family"].isin(
                [
                    "RAW",
                    "CONTROL",
                ]
            )
        ]

        research = df[
            ~df["family"].isin(
                [
                    "RAW",
                    "CONTROL",
                ]
            )
        ].copy()

        research = research.sort_values(
            by=[
                "profitable_eras",
                "minimum_era_pf_5_plus",
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
            ],
        )

        df = pd.concat(
            [
                reference,
                research,
            ],
            ignore_index=True,
        )

        df.to_csv(
            OUTPUT_FILE,
            index=False,
        )

        STATUS.update({
            "state": "complete",
            "message": (
                "EUR/GBP long single-factor "
                "edge discovery complete"
            ),
            "rows_saved": len(df),
            "output_file": (
                OUTPUT_FILE
            ),
        })

        print()
        print("=" * 90)
        print(
            "EUR/GBP LONG SINGLE-FACTOR "
            "EDGE DISCOVERY COMPLETE"
        )
        print("=" * 90)
        print(
            f"Tests: {len(TESTS)}"
        )
        print(
            f"Rows saved: {len(df)}"
        )
        print(
            f"Output: {OUTPUT_FILE}"
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
            "EUR/GBP Long Single-Factor Edge Discovery"
        ),
        "status": STATUS,
        "instrument": INSTRUMENT,
        "direction": "LONG",
        "mode": "READ_ONLY_RESEARCH",
        "orders_supported": False,
        "trading_enabled": False,
        "download": "/download",
    })


@app.route("/status")
def status():
    return jsonify(STATUS)


@app.route("/download")
def download():
    if not os.path.exists(
        OUTPUT_FILE
    ):
        return jsonify({
            "status": "not_ready",
            "message": (
                "CSV is not ready yet"
            ),
        }), 404

    return send_file(
        OUTPUT_FILE,
        as_attachment=True,
        download_name=OUTPUT_FILE,
    )


if __name__ == "__main__":
    thread = threading.Thread(
        target=run_research,
        name=(
            "eurgbp-long-single-factor"
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
