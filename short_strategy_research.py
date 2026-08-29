import os
import itertools
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# USD/JPY SHORT - FREQUENCY EXPANSION + FILTER SUBSTITUTION
#
# RESEARCH ONLY — NEVER SUBMITS ORDERS.
#
# Goal:
#   Increase trade frequency without throwing away the all-era
#   robustness found in the narrow stability sweep.
#
# Instead of simply weakening every filter, this run tests
# whether more informative market-state features can replace
# restrictive candle filters such as strong-close / upper-wick.
#
# Structural grid:
#   Body:       1.25 -> 1.50
#   Structure:  50 -> 90
#   Distance:   .30 -> .50 ATR
#   Slow EMA:   80 -> 125
#   RR:         2.50 / 2.75 / 3.00
#
# Replacement feature recipes include:
#   - no extra candle filter
#   - strong close only
#   - wick only
#   - lighter strong-close + wick
#   - actual sweep above prior structure high
#   - prior 5-bar bullish pullback
#   - daily ATR regime
#   - fast/slow EMA alignment
#   - not-too-extended below daily EMA
#   - combinations of the above
#
# NO timing / weekday optimisation.
#
# Exact execution conventions retained:
#   OANDA midpoint H1
#   Previous completed daily candle only
#   Daily alignment = 17:00 America/New_York
#   ATR14 = Wilder/RMA
#   Daily EMA = SMA-seeded
#   Stop = signal high + 10 ticks
#   Adverse short slippage = 5 ticks
#   Target from reference signal close
#   Pyramiding = 0
#   Same-bar SL/TP tie rule retained
# ============================================================


app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_URL = "https://api-fxtrade.oanda.com"

INSTRUMENT = "USD_JPY"
TICK_SIZE = 0.001

NY_TZ = ZoneInfo("America/New_York")

DAILY_ALIGNMENT_HOUR = 17
DAILY_ALIGNMENT_TIMEZONE = "America/New_York"

STOP_BUFFER_TICKS = 10
BACKTEST_SLIPPAGE_TICKS = 5

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

H1_WARMUP_DAYS = 220
DAILY_WARMUP_DAYS = 2600

OUTPUT_FILE = "usdjpy_short_frequency_substitution.csv"


# ============================================================
# FREQUENCY-EXPANSION STRUCTURAL GRID
# ============================================================

BODY_RATIOS = [
    1.25,
    1.30,
    1.35,
    1.40,
    1.45,
    1.50,
]

STRUCTURE_LOOKBACKS = [
    50,
    60,
    70,
    80,
    90,
]

MAX_DISTANCE_ATR_VALUES = [
    0.30,
    0.35,
    0.40,
    0.45,
    0.50,
]

SLOW_EMAS = [
    80,
    90,
    100,
    110,
    125,
]

REWARD_RISKS = [
    2.50,
    2.75,
    3.00,
]


# ============================================================
# FEATURE RECIPES
# ============================================================
#
# None = feature OFF.
#
# maximum_close_location:
#   (close-low)/(high-low), lower is stronger bearish close.
#
# minimum_upper_wick_body:
#   upper wick / body.
#
# minimum_sweep_atr:
#   (signal high - previous structure high) / H1 ATR.
#   0.00 means signal must actually reach/exceed prior high.
#
# minimum_prior_5bar_upmove_atr:
#   (signal open - close 5 H1 bars ago) / H1 ATR.
#
# minimum_daily_atr_ratio_50:
#   Daily ATR14 / 50-day average of Daily ATR14.
#
# fast_ema:
#   require fast EMA < slow EMA.
#
# maximum_daily_extension_atr:
#   (slow EMA - previous daily close) / Daily ATR14.
#   Caps how far price is already stretched below the slow EMA.
#
# These recipes intentionally test SUBSTITUTION, not only
# increasingly restrictive combinations.
# ============================================================

FEATURE_RECIPES = [
    {
        "name": "NONE",
        "maximum_close_location": None,
        "minimum_upper_wick_body": None,
        "minimum_sweep_atr": None,
        "minimum_prior_5bar_upmove_atr": None,
        "minimum_daily_atr_ratio_50": None,
        "fast_ema": None,
        "maximum_daily_extension_atr": None,
    },
    {
        "name": "CLOSE_045",
        "maximum_close_location": 0.45,
        "minimum_upper_wick_body": None,
        "minimum_sweep_atr": None,
        "minimum_prior_5bar_upmove_atr": None,
        "minimum_daily_atr_ratio_50": None,
        "fast_ema": None,
        "maximum_daily_extension_atr": None,
    },
    {
        "name": "CLOSE_040",
        "maximum_close_location": 0.40,
        "minimum_upper_wick_body": None,
        "minimum_sweep_atr": None,
        "minimum_prior_5bar_upmove_atr": None,
        "minimum_daily_atr_ratio_50": None,
        "fast_ema": None,
        "maximum_daily_extension_atr": None,
    },
    {
        "name": "WICK_015",
        "maximum_close_location": None,
        "minimum_upper_wick_body": 0.15,
        "minimum_sweep_atr": None,
        "minimum_prior_5bar_upmove_atr": None,
        "minimum_daily_atr_ratio_50": None,
        "fast_ema": None,
        "maximum_daily_extension_atr": None,
    },
    {
        "name": "WICK_025",
        "maximum_close_location": None,
        "minimum_upper_wick_body": 0.25,
        "minimum_sweep_atr": None,
        "minimum_prior_5bar_upmove_atr": None,
        "minimum_daily_atr_ratio_50": None,
        "fast_ema": None,
        "maximum_daily_extension_atr": None,
    },
    {
        "name": "LIGHT_CANDLE",
        "maximum_close_location": 0.45,
        "minimum_upper_wick_body": 0.15,
        "minimum_sweep_atr": None,
        "minimum_prior_5bar_upmove_atr": None,
        "minimum_daily_atr_ratio_50": None,
        "fast_ema": None,
        "maximum_daily_extension_atr": None,
    },
    {
        "name": "SWEEP_PRIOR_HIGH",
        "maximum_close_location": None,
        "minimum_upper_wick_body": None,
        "minimum_sweep_atr": 0.00,
        "minimum_prior_5bar_upmove_atr": None,
        "minimum_daily_atr_ratio_50": None,
        "fast_ema": None,
        "maximum_daily_extension_atr": None,
    },
    {
        "name": "SWEEP_005_ATR",
        "maximum_close_location": None,
        "minimum_upper_wick_body": None,
        "minimum_sweep_atr": 0.05,
        "minimum_prior_5bar_upmove_atr": None,
        "minimum_daily_atr_ratio_50": None,
        "fast_ema": None,
        "maximum_daily_extension_atr": None,
    },
    {
        "name": "PULLBACK_050",
        "maximum_close_location": None,
        "minimum_upper_wick_body": None,
        "minimum_sweep_atr": None,
        "minimum_prior_5bar_upmove_atr": 0.50,
        "minimum_daily_atr_ratio_50": None,
        "fast_ema": None,
        "maximum_daily_extension_atr": None,
    },
    {
        "name": "DAILY_ATR_080",
        "maximum_close_location": None,
        "minimum_upper_wick_body": None,
        "minimum_sweep_atr": None,
        "minimum_prior_5bar_upmove_atr": None,
        "minimum_daily_atr_ratio_50": 0.80,
        "fast_ema": None,
        "maximum_daily_extension_atr": None,
    },
    {
        "name": "FAST40_ALIGNMENT",
        "maximum_close_location": None,
        "minimum_upper_wick_body": None,
        "minimum_sweep_atr": None,
        "minimum_prior_5bar_upmove_atr": None,
        "minimum_daily_atr_ratio_50": None,
        "fast_ema": 40,
        "maximum_daily_extension_atr": None,
    },
    {
        "name": "MAX_EXTENSION_075",
        "maximum_close_location": None,
        "minimum_upper_wick_body": None,
        "minimum_sweep_atr": None,
        "minimum_prior_5bar_upmove_atr": None,
        "minimum_daily_atr_ratio_50": None,
        "fast_ema": None,
        "maximum_daily_extension_atr": 0.75,
    },
    {
        "name": "SWEEP_PLUS_CLOSE045",
        "maximum_close_location": 0.45,
        "minimum_upper_wick_body": None,
        "minimum_sweep_atr": 0.00,
        "minimum_prior_5bar_upmove_atr": None,
        "minimum_daily_atr_ratio_50": None,
        "fast_ema": None,
        "maximum_daily_extension_atr": None,
    },
    {
        "name": "PULLBACK_PLUS_CLOSE045",
        "maximum_close_location": 0.45,
        "minimum_upper_wick_body": None,
        "minimum_sweep_atr": None,
        "minimum_prior_5bar_upmove_atr": 0.50,
        "minimum_daily_atr_ratio_50": None,
        "fast_ema": None,
        "maximum_daily_extension_atr": None,
    },
    {
        "name": "ATR_PLUS_CLOSE045",
        "maximum_close_location": 0.45,
        "minimum_upper_wick_body": None,
        "minimum_sweep_atr": None,
        "minimum_prior_5bar_upmove_atr": None,
        "minimum_daily_atr_ratio_50": 0.80,
        "fast_ema": None,
        "maximum_daily_extension_atr": None,
    },
    {
        "name": "ALIGNMENT_PLUS_CLOSE045",
        "maximum_close_location": 0.45,
        "minimum_upper_wick_body": None,
        "minimum_sweep_atr": None,
        "minimum_prior_5bar_upmove_atr": None,
        "minimum_daily_atr_ratio_50": None,
        "fast_ema": 40,
        "maximum_daily_extension_atr": None,
    },
    {
        "name": "SWEEP_PLUS_ATR",
        "maximum_close_location": None,
        "minimum_upper_wick_body": None,
        "minimum_sweep_atr": 0.00,
        "minimum_prior_5bar_upmove_atr": None,
        "minimum_daily_atr_ratio_50": 0.80,
        "fast_ema": None,
        "maximum_daily_extension_atr": None,
    },
    {
        "name": "PULLBACK_PLUS_ATR",
        "maximum_close_location": None,
        "minimum_upper_wick_body": None,
        "minimum_sweep_atr": None,
        "minimum_prior_5bar_upmove_atr": 0.50,
        "minimum_daily_atr_ratio_50": 0.80,
        "fast_ema": None,
        "maximum_daily_extension_atr": None,
    },
    {
        "name": "EXTENSION_PLUS_CLOSE045",
        "maximum_close_location": 0.45,
        "minimum_upper_wick_body": None,
        "minimum_sweep_atr": None,
        "minimum_prior_5bar_upmove_atr": None,
        "minimum_daily_atr_ratio_50": None,
        "fast_ema": None,
        "maximum_daily_extension_atr": 0.75,
    },
    {
        "name": "SWEEP_PULLBACK",
        "maximum_close_location": None,
        "minimum_upper_wick_body": None,
        "minimum_sweep_atr": 0.00,
        "minimum_prior_5bar_upmove_atr": 0.50,
        "minimum_daily_atr_ratio_50": None,
        "fast_ema": None,
        "maximum_daily_extension_atr": None,
    },
]

TOTAL_STRUCTURAL_COMBINATIONS = (
    len(BODY_RATIOS)
    * len(STRUCTURE_LOOKBACKS)
    * len(MAX_DISTANCE_ATR_VALUES)
    * len(SLOW_EMAS)
    * len(REWARD_RISKS)
)

TOTAL_COMBINATIONS = (
    TOTAL_STRUCTURAL_COMBINATIONS
    * len(FEATURE_RECIPES)
)

ALL_DAILY_EMAS = sorted(
    set(SLOW_EMAS + [40])
)


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

STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "service": "USDJPY Short Frequency Expansion Substitution",
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "structural_combinations": TOTAL_STRUCTURAL_COMBINATIONS,
    "feature_recipes": len(FEATURE_RECIPES),
    "total_combinations": TOTAL_COMBINATIONS,
    "completed_combinations": 0,
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
        "dailyAlignment": DAILY_ALIGNMENT_HOUR,
        "alignmentTimezone": DAILY_ALIGNMENT_TIMEZONE,
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

    result[length - 1] = initial

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

    for index, candle in enumerate(candles):
        if index == 0:
            tr = (
                candle["high"]
                - candle["low"]
            )

        else:
            previous_close = (
                candles[index - 1]["close"]
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

    result[length - 1] = initial
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


def atr_series(candles, length=14):
    return rma_series(
        true_ranges(candles),
        length,
    )


def rolling_mean(values, length):
    result = [None] * len(values)
    window = []
    running = 0.0
    valid = 0

    for index, value in enumerate(values):
        window.append(value)

        if value is not None:
            running += value
            valid += 1

        if len(window) > length:
            removed = window.pop(0)

            if removed is not None:
                running -= removed
                valid -= 1

        if (
            len(window) == length
            and valid == length
        ):
            result[index] = (
                running / length
            )

    return result


# ============================================================
# DAILY STATE
# ============================================================

def current_daily_start(timestamp_utc):
    ny_time = (
        timestamp_utc
        .astimezone(NY_TZ)
    )

    candidate = ny_time.replace(
        hour=DAILY_ALIGNMENT_HOUR,
        minute=0,
        second=0,
        microsecond=0,
    )

    if ny_time < candidate:
        candidate -= timedelta(days=1)

    return candidate.astimezone(
        timezone.utc
    )


def build_daily_state(daily):
    closes = [
        candle["close"]
        for candle in daily
    ]

    ema_map = {
        length: ema_series(
            closes,
            length,
        )
        for length
        in ALL_DAILY_EMAS
    }

    daily_atr = atr_series(
        daily,
        14,
    )

    daily_atr_mean_50 = rolling_mean(
        daily_atr,
        50,
    )

    return (
        ema_map,
        daily_atr,
        daily_atr_mean_50,
    )


def build_h1_daily_lookup(
    h1,
    daily,
    daily_ema_map,
    daily_atr,
    daily_atr_mean_50,
):
    lookup = [None] * len(h1)
    daily_index = -1

    for h1_index, candle in enumerate(h1):
        session_start = current_daily_start(
            candle["time"]
        )

        while (
            daily_index + 1 < len(daily)
            and daily[daily_index + 1]["time"]
            < session_start
        ):
            daily_index += 1

        if daily_index < 0:
            continue

        atr_now = daily_atr[
            daily_index
        ]

        atr_mean = (
            daily_atr_mean_50[
                daily_index
            ]
        )

        atr_ratio = None

        if (
            atr_now is not None
            and atr_mean is not None
            and atr_mean > 0
        ):
            atr_ratio = (
                atr_now / atr_mean
            )

        lookup[h1_index] = {
            "close": daily[
                daily_index
            ]["close"],
            "daily_atr14": atr_now,
            "daily_atr_ratio_50": (
                atr_ratio
            ),
            "emas": {
                length:
                daily_ema_map[
                    length
                ][daily_index]
                for length
                in ALL_DAILY_EMAS
            },
        }

    return lookup


# ============================================================
# SIGNAL FEATURE MATRIX
# ============================================================

def build_candidates(
    h1,
    h1_atr,
    daily_lookup,
):
    candidates = []

    max_lookback = max(
        STRUCTURE_LOOKBACKS
    )

    for index in range(
        max(
            max_lookback,
            5,
        ),
        len(h1),
    ):
        signal = h1[index]

        if signal["time"] < RESEARCH_FROM:
            continue

        if signal["time"] >= RESEARCH_TO:
            break

        previous = h1[index - 1]
        atr = h1_atr[index]
        daily = daily_lookup[index]

        if (
            atr is None
            or atr <= 0
            or daily is None
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

        structure_data = {}

        for lookback in STRUCTURE_LOOKBACKS:
            previous_highest = max(
                candle["high"]
                for candle in h1[
                    index - lookback:index
                ]
            )

            structure_data[
                lookback
            ] = {
                "distance_atr": (
                    previous_highest
                    - signal["high"]
                ) / atr,
                "sweep_atr": (
                    signal["high"]
                    - previous_highest
                ) / atr,
            }

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

        prior_5bar_upmove_atr = (
            signal["open"]
            - h1[index - 5]["close"]
        ) / atr

        candidates.append({
            "index": index,
            "time": signal["time"],
            "body_ratio": (
                current_body
                / previous_body
            ),
            "close_location": (
                close_location
            ),
            "upper_wick_body": (
                upper_wick
                / current_body
            ),
            "prior_5bar_upmove_atr": (
                prior_5bar_upmove_atr
            ),
            "structure": (
                structure_data
            ),
            "daily": daily,
        })

    return candidates


# ============================================================
# FILTER
# ============================================================

def candidate_allowed(
    candidate,
    body_ratio,
    structure_lookback,
    max_distance_atr,
    slow_ema,
    recipe,
):
    if (
        candidate["body_ratio"]
        < body_ratio
    ):
        return False

    structure = candidate[
        "structure"
    ][structure_lookback]

    if (
        structure[
            "distance_atr"
        ] > max_distance_atr
    ):
        return False

    daily = candidate["daily"]

    slow_value = daily[
        "emas"
    ].get(
        slow_ema
    )

    if slow_value is None:
        return False

    if not (
        daily["close"]
        < slow_value
    ):
        return False

    maximum_close_location = recipe[
        "maximum_close_location"
    ]

    if (
        maximum_close_location
        is not None
        and candidate[
            "close_location"
        ] > maximum_close_location
    ):
        return False

    minimum_upper_wick_body = recipe[
        "minimum_upper_wick_body"
    ]

    if (
        minimum_upper_wick_body
        is not None
        and candidate[
            "upper_wick_body"
        ] < minimum_upper_wick_body
    ):
        return False

    minimum_sweep_atr = recipe[
        "minimum_sweep_atr"
    ]

    if (
        minimum_sweep_atr
        is not None
        and structure[
            "sweep_atr"
        ] < minimum_sweep_atr
    ):
        return False

    minimum_prior_5bar_upmove_atr = recipe[
        "minimum_prior_5bar_upmove_atr"
    ]

    if (
        minimum_prior_5bar_upmove_atr
        is not None
        and candidate[
            "prior_5bar_upmove_atr"
        ] < minimum_prior_5bar_upmove_atr
    ):
        return False

    minimum_daily_atr_ratio_50 = recipe[
        "minimum_daily_atr_ratio_50"
    ]

    if (
        minimum_daily_atr_ratio_50
        is not None
    ):
        ratio = daily[
            "daily_atr_ratio_50"
        ]

        if (
            ratio is None
            or ratio
            < minimum_daily_atr_ratio_50
        ):
            return False

    fast_ema = recipe[
        "fast_ema"
    ]

    if fast_ema is not None:
        fast_value = daily[
            "emas"
        ].get(
            fast_ema
        )

        if (
            fast_value is None
            or not (
                fast_value
                < slow_value
            )
        ):
            return False

    maximum_daily_extension_atr = recipe[
        "maximum_daily_extension_atr"
    ]

    if (
        maximum_daily_extension_atr
        is not None
    ):
        daily_atr = daily[
            "daily_atr14"
        ]

        if (
            daily_atr is None
            or daily_atr <= 0
        ):
            return False

        extension = (
            slow_value
            - daily["close"]
        ) / daily_atr

        if (
            extension
            > maximum_daily_extension_atr
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
    reward_risk,
):
    cache_key = (
        signal_index,
        reward_risk,
    )

    if cache_key in EXIT_CACHE:
        return EXIT_CACHE[
            cache_key
        ]

    signal = h1[signal_index]

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
        * reward_risk
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
            cache_key
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
        cache_key
    ] = result

    return result


def simulate(
    h1,
    candidates,
    reward_risk,
):
    trades = []
    position_exit_index = -1
    ignored = 0
    still_open = False

    for candidate in candidates:
        signal_index = (
            candidate["index"]
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
            reward_risk,
        )

        if trade["status"] == "OPEN":
            still_open = True
            break

        trades.append(trade)

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
        signal_time = trade[
            "signal_time"
        ]

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

        filtered.append(trade)

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

    gross_profit = sum(winners)
    gross_loss = abs(sum(losers))
    total_r = sum(results)

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
    body_ratio,
    structure_lookback,
    max_distance_atr,
    slow_ema,
    reward_risk,
    recipe,
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
        "feature_recipe": recipe[
            "name"
        ],
        "body_ratio": body_ratio,
        "structure_lookback": (
            structure_lookback
        ),
        "max_distance_atr": (
            max_distance_atr
        ),
        "slow_daily_ema": (
            slow_ema
        ),
        "reward_risk": (
            reward_risk
        ),
        "maximum_close_location": recipe[
            "maximum_close_location"
        ],
        "minimum_upper_wick_body": recipe[
            "minimum_upper_wick_body"
        ],
        "minimum_sweep_atr": recipe[
            "minimum_sweep_atr"
        ],
        "minimum_prior_5bar_upmove_atr": recipe[
            "minimum_prior_5bar_upmove_atr"
        ],
        "minimum_daily_atr_ratio_50": recipe[
            "minimum_daily_atr_ratio_50"
        ],
        "fast_ema": recipe[
            "fast_ema"
        ],
        "maximum_daily_extension_atr": recipe[
            "maximum_daily_extension_atr"
        ],
        "raw_signals": len(
            eligible
        ),
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
            full["trades"]
            / years,
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

    profitable_eras = 0
    eras_with_5_plus = 0
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

        if era["total_r"] > 0:
            profitable_eras += 1

        if era["trades"] >= 5:
            eras_with_5_plus += 1

            if era["total_r"] > 0:
                profitable_eras_with_5_plus += 1

            if (
                minimum_era_pf_5_plus
                is None
            ):
                minimum_era_pf_5_plus = (
                    era[
                        "profit_factor"
                    ]
                )
            else:
                minimum_era_pf_5_plus = min(
                    minimum_era_pf_5_plus,
                    era[
                        "profit_factor"
                    ],
                )

            if (
                minimum_era_expectancy_5_plus
                is None
            ):
                minimum_era_expectancy_5_plus = (
                    era[
                        "expectancy_r"
                    ]
                )
            else:
                minimum_era_expectancy_5_plus = min(
                    minimum_era_expectancy_5_plus,
                    era[
                        "expectancy_r"
                    ],
                )

    row[
        "profitable_eras"
    ] = profitable_eras

    row[
        "eras_with_5_plus_trades"
    ] = eras_with_5_plus

    row[
        "profitable_eras_with_5_plus_trades"
    ] = profitable_eras_with_5_plus

    row[
        "minimum_era_pf_5_plus"
    ] = minimum_era_pf_5_plus

    row[
        "minimum_era_expectancy_5_plus"
    ] = minimum_era_expectancy_5_plus

    return row


# ============================================================
# RESEARCH
# ============================================================

def run_research():
    global STATUS

    try:
        print()
        print("=" * 80)
        print(
            "USD/JPY SHORT - FREQUENCY EXPANSION + FILTER SUBSTITUTION"
        )
        print("=" * 80)
        print(
            "Structural combinations:",
            TOTAL_STRUCTURAL_COMBINATIONS,
        )
        print(
            "Feature recipes:",
            len(FEATURE_RECIPES),
        )
        print(
            "Total combinations:",
            TOTAL_COMBINATIONS,
        )
        print(
            "NO TIMING / WEEKDAY FILTERS"
        )
        print()

        STATUS.update({
            "state": "fetching_data",
            "message": (
                "Fetching USD/JPY OANDA history"
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

        daily = fetch_chunked_history(
            INSTRUMENT,
            "D",
            RESEARCH_FROM
            - timedelta(
                days=DAILY_WARMUP_DAYS
            ),
            RESEARCH_TO,
        )

        if not h1:
            raise RuntimeError(
                "No USD/JPY H1 candles returned"
            )

        if not daily:
            raise RuntimeError(
                "No USD/JPY daily candles returned"
            )

        STATUS.update({
            "state": "precomputing",
            "message": (
                "Building indicators and feature matrix"
            ),
        })

        h1_atr = atr_series(
            h1,
            14,
        )

        (
            daily_ema_map,
            daily_atr,
            daily_atr_mean_50,
        ) = build_daily_state(
            daily
        )

        daily_lookup = (
            build_h1_daily_lookup(
                h1,
                daily,
                daily_ema_map,
                daily_atr,
                daily_atr_mean_50,
            )
        )

        candidates = (
            build_candidates(
                h1,
                h1_atr,
                daily_lookup,
            )
        )

        STATUS[
            "base_bearish_engulfings"
        ] = len(
            candidates
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
                "Running frequency/substitution sweep"
            ),
        })

        structural_grid = list(
            itertools.product(
                BODY_RATIOS,
                STRUCTURE_LOOKBACKS,
                MAX_DISTANCE_ATR_VALUES,
                SLOW_EMAS,
                REWARD_RISKS,
            )
        )

        for recipe in FEATURE_RECIPES:
            print()
            print(
                "Recipe:",
                recipe["name"],
                flush=True,
            )

            for (
                body_ratio,
                structure_lookback,
                max_distance_atr,
                slow_ema,
                reward_risk,
            ) in structural_grid:
                completed += 1

                eligible = [
                    candidate
                    for candidate in candidates
                    if candidate_allowed(
                        candidate,
                        body_ratio,
                        structure_lookback,
                        max_distance_atr,
                        slow_ema,
                        recipe,
                    )
                ]

                (
                    trades,
                    ignored,
                    still_open,
                ) = simulate(
                    h1,
                    eligible,
                    reward_risk,
                )

                rows.append(
                    make_result_row(
                        body_ratio,
                        structure_lookback,
                        max_distance_atr,
                        slow_ema,
                        reward_risk,
                        recipe,
                        eligible,
                        trades,
                        ignored,
                        still_open,
                        years,
                    )
                )

                STATUS[
                    "completed_combinations"
                ] = completed

                if completed % 1000 == 0:
                    print(
                        f"Progress: "
                        f"{completed}/"
                        f"{TOTAL_COMBINATIONS}",
                        flush=True,
                    )

        df = pd.DataFrame(
            rows
        )

        if df.empty:
            raise RuntimeError(
                "No USD/JPY frequency/substitution rows generated"
            )

        df[
            "frequency_5_plus"
        ] = (
            df["trades_per_year"]
            >= 5.0
        )

        df[
            "frequency_6_plus"
        ] = (
            df["trades_per_year"]
            >= 6.0
        )

        df[
            "frequency_8_plus"
        ] = (
            df["trades_per_year"]
            >= 8.0
        )

        df[
            "all_four_eras_profitable"
        ] = (
            df[
                "profitable_eras_with_5_plus_trades"
            ]
            >= 4
        )

        df[
            "worst_era_pf_115"
        ] = (
            df[
                "minimum_era_pf_5_plus"
            ].fillna(0)
            >= 1.15
        )

        df[
            "worst_era_pf_120"
        ] = (
            df[
                "minimum_era_pf_5_plus"
            ].fillna(0)
            >= 1.20
        )

        df[
            "overall_pf_140"
        ] = (
            df[
                "profit_factor"
            ]
            >= 1.40
        )

        df[
            "expectancy_020"
        ] = (
            df[
                "expectancy_r"
            ]
            >= 0.20
        )

        df[
            "target_zone"
        ] = (
            df[
                "frequency_5_plus"
            ]
            & df[
                "all_four_eras_profitable"
            ]
            & df[
                "worst_era_pf_115"
            ]
            & df[
                "overall_pf_140"
            ]
            & df[
                "expectancy_020"
            ]
        )

        df[
            "annual_r_linear"
        ] = (
            df[
                "expectancy_r"
            ]
            * df[
                "trades_per_year"
            ]
        )

        df = df.sort_values(
            by=[
                "target_zone",
                "frequency_6_plus",
                "all_four_eras_profitable",
                "minimum_era_pf_5_plus",
                "profit_factor",
                "annual_r_linear",
                "trades_per_year",
            ],
            ascending=[
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
                "USD/JPY frequency expansion / "
                "filter substitution completed"
            ),
            "completed_combinations": (
                TOTAL_COMBINATIONS
            ),
            "rows_saved": len(df),
            "target_zone_rows": int(
                df["target_zone"].sum()
            ),
            "all_four_eras_profitable": int(
                df[
                    "all_four_eras_profitable"
                ].sum()
            ),
            "five_plus_per_year": int(
                df[
                    "frequency_5_plus"
                ].sum()
            ),
            "six_plus_per_year": int(
                df[
                    "frequency_6_plus"
                ].sum()
            ),
            "output_file": OUTPUT_FILE,
        })

        print()
        print("=" * 80)
        print(
            "USD/JPY FREQUENCY / SUBSTITUTION COMPLETE"
        )
        print("=" * 80)
        print(
            "Rows:",
            len(df),
        )
        print(
            "Target-zone rows:",
            int(
                df[
                    "target_zone"
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
            "USDJPY Short Frequency Expansion Substitution"
        ),
        "status": STATUS,
        "instrument": INSTRUMENT,
        "direction": "SHORT",
        "timing_filters": (
            "NONE - all hours and weekdays"
        ),
        "structural_grid": {
            "body_ratios": BODY_RATIOS,
            "structure_lookbacks": (
                STRUCTURE_LOOKBACKS
            ),
            "max_distance_atr": (
                MAX_DISTANCE_ATR_VALUES
            ),
            "slow_emas": SLOW_EMAS,
            "reward_risks": (
                REWARD_RISKS
            ),
        },
        "feature_recipes": [
            recipe["name"]
            for recipe
            in FEATURE_RECIPES
        ],
        "total_combinations": (
            TOTAL_COMBINATIONS
        ),
        "download": "/download",
        "trading_enabled": False,
        "orders_supported": False,
        "executor_connected": False,
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
                "USD/JPY frequency/substitution "
                "CSV is not ready yet"
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
            "usdjpy-short-frequency-substitution"
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
