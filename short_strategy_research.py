import os
import threading
import itertools
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# USD/CAD LONG - CONTROLLED CONFIRMATION MATRIX
#
# RESEARCH ONLY - NEVER SUBMITS ORDERS.
#
# PURPOSE
# ------------------------------------------------------------
# Stage 2 of the USD/CAD long audit.
#
# The single-factor scan showed that STRUCTURE was the clear
# independent edge, with possible supporting value from:
#   - range / ATR
#   - lower wick / body
#   - 48h momentum
#   - daily ATR regime
#
# This script tests those factors in combination.
#
# IMPORTANT:
#   - It does NOT optimise body ratio.
#   - It does NOT optimise strong-close.
#   - It does NOT optimise weekdays.
#   - It does NOT optimise NY-hour timing.
#   - It does NOT optimise EMA regime.
#
# Those can be tested later only if justified.
#
# A separate CURRENT_LIVE_CONTROL row is included so every
# matrix result can be compared directly with the live strategy.
#
# ============================================================
# LOCKED COMMON BACKTEST CONVENTIONS
#
# OANDA midpoint H1
#
# Bullish engulfing:
#   previous candle bearish
#   current candle bullish
#   current open <= previous close
#   current close >= previous open
#
# Minimum body ratio:
#   >= 1.00
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
#   reference close + (reference close - stop) * 3.50
#
# Actual R:
#   (exit - backtest fill) / (backtest fill - stop)
#
# Pyramiding:
#   0
#
# Same-bar stop + target:
#   compare candle open->high vs open->low
#   high closer => target first
#   otherwise stop first
#
# New signal on exact H1 bar where previous trade exits:
#   allowed
#
# Research:
#   2002-05-06 20:00 UTC -> current completed UTC hour
#
# Daily candles:
#   OANDA 17:00 America/New_York alignment
#   previous completed daily candle only
#
# ============================================================
# MATRIX
#
# Structure lookback:
#   20, 30, 40, 60
#
# Structure distance:
#   0.05, 0.10, 0.15, 0.20 ATR
#
# Minimum range:
#   none, 0.90, 1.10, 1.30, 1.50 ATR
#
# Minimum lower wick/body:
#   none, 0.10, 0.20, 0.30
#
# Minimum 48h momentum:
#   none, 0.50, 1.00, 1.50 ATR
#
# Minimum daily ATR14 / 50-day ATR14 mean:
#   none, 0.90, 1.00, 1.10, 1.20
#
# Total matrix combinations:
#   4 * 4 * 5 * 4 * 4 * 5 = 6,400
#
# PLUS:
#   CURRENT_LIVE_CONTROL
#
# ============================================================
# CURRENT LIVE CONTROL
#
# body >= 1.00
# lower wick >= 0.20 x body
# structure 40 / 0.20 ATR
# previous completed daily close > EMA200
# exclude NY hours 00-04
# RR 3.50
# stop buffer 10 ticks
# adverse historical fill 5 ticks
#
# ============================================================
# OUTPUT
#
# /download
#
# CSV includes:
#   full-history metrics
#   four eras
#   recent 2y / 5y / 10y
#   worst rolling 3-year PF / expectancy / total R
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
MINIMUM_BODY_RATIO = 1.00

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
    "usdcad_long_controlled_confirmation_matrix.csv"
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
# MATRIX VALUES
# ============================================================

STRUCTURE_LOOKBACKS = [
    40,
    60,
]

# These are the robust core geometries identified in the
# refined sweep. They are treated as fixed anchor candidates,
# NOT re-optimised here.
ANCHORS = [
    {
        "anchor": "S40_D025_R120_W20_V095",
        "structure_lookback": 40,
        "structure_distance_atr": 0.025,
        "minimum_range_atr": 1.20,
        "minimum_lower_wick_body": 0.20,
        "minimum_daily_atr_ratio_50": 0.95,
    },
    {
        "anchor": "S40_D050_R120_W20_V095",
        "structure_lookback": 40,
        "structure_distance_atr": 0.050,
        "minimum_range_atr": 1.20,
        "minimum_lower_wick_body": 0.20,
        "minimum_daily_atr_ratio_50": 0.95,
    },
    {
        "anchor": "S60_D025_R120_W20_V095",
        "structure_lookback": 60,
        "structure_distance_atr": 0.025,
        "minimum_range_atr": 1.20,
        "minimum_lower_wick_body": 0.20,
        "minimum_daily_atr_ratio_50": 0.95,
    },
    {
        "anchor": "S60_D050_R130_W20_V090",
        "structure_lookback": 60,
        "structure_distance_atr": 0.050,
        "minimum_range_atr": 1.30,
        "minimum_lower_wick_body": 0.20,
        "minimum_daily_atr_ratio_50": 0.90,
    },
    {
        "anchor": "S60_D100_R130_W20_V090",
        "structure_lookback": 60,
        "structure_distance_atr": 0.100,
        "minimum_range_atr": 1.30,
        "minimum_lower_wick_body": 0.20,
        "minimum_daily_atr_ratio_50": 0.90,
    },
]


DAILY_EMA_VALUES = [
    None,
    50,
    75,
    100,
    125,
]

BODY_ATR_VALUES = [
    None,
    0.50,
    0.60,
    0.70,
]

BODY_RATIO_VALUES = [
    1.00,
    1.05,
    1.10,
    1.15,
]

# Timing is included only as a binary final overlay:
# no exclusion vs exclude NY hour 01.
NY_HOUR_01_OPTIONS = [
    False,
    True,
]


CONFIRMATION_CONFIGS = []

for anchor in ANCHORS:
    for ema_length in DAILY_EMA_VALUES:
        for minimum_body_atr in BODY_ATR_VALUES:
            for minimum_body_ratio in BODY_RATIO_VALUES:
                for exclude_ny_01 in NY_HOUR_01_OPTIONS:
                    CONFIRMATION_CONFIGS.append({
                        "anchor": anchor["anchor"],
                        "structure_lookback": (
                            anchor["structure_lookback"]
                        ),
                        "structure_distance_atr": (
                            anchor["structure_distance_atr"]
                        ),
                        "minimum_range_atr": (
                            anchor["minimum_range_atr"]
                        ),
                        "minimum_lower_wick_body": (
                            anchor["minimum_lower_wick_body"]
                        ),
                        "minimum_daily_atr_ratio_50": (
                            anchor["minimum_daily_atr_ratio_50"]
                        ),
                        "daily_close_above_ema": (
                            ema_length
                        ),
                        "minimum_body_atr": (
                            minimum_body_atr
                        ),
                        "minimum_body_ratio_overlay": (
                            minimum_body_ratio
                        ),
                        "exclude_ny_01": (
                            exclude_ny_01
                        ),
                    })


TOTAL_CONFIRMATION_TESTS = len(
    CONFIRMATION_CONFIGS
)

TOTAL_TESTS = (
    TOTAL_CONFIRMATION_TESTS
    + 1
)


STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "service": (
        "USD/CAD Long Controlled Confirmation Matrix"
    ),
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "confirmation_tests": TOTAL_CONFIRMATION_TESTS,
    "total_tests_including_control": TOTAL_TESTS,
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

    for raw in data.get(
        "candles",
        [],
    ):
        candle = parse_candle(
            raw
        )

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

    if len(values) < length:
        return result

    multiplier = (
        2.0
        / (
            length + 1.0
        )
    )

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
    result = [
        None
    ] * len(
        values
    )

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

    candidate = ny_time.replace(
        hour=DAILY_ALIGNMENT_HOUR,
        minute=0,
        second=0,
        microsecond=0,
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
        for candle in daily
    ]

    ema_lengths = [
        50,
        75,
        100,
        125,
        150,
        200,
        250,
        300,
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

    for index, candle in enumerate(
        daily
    ):
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
            "ema200": ema_map[200][index],
            "emas": {
                length: (
                    ema_map[length][index]
                )
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

def build_raw_candidates(
    h1,
    atr,
    daily_state,
):
    candidates = []

    max_structure = max(
        STRUCTURE_LOOKBACKS
    )

    start_index = max(
        ATR_LENGTH,
        max_structure,
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

        if (
            body_ratio
            < MINIMUM_BODY_RATIO
        ):
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

        lower_wick_body = (
            lower_wick
            / current_body
        )

        upper_wick_body = (
            upper_wick
            / current_body
        )

        close_location = (
            signal["close"]
            - signal["low"]
        ) / signal_range

        range_atr = (
            signal_range
            / current_atr
        )

        body_atr = (
            current_body
            / current_atr
        )

        previous_body_atr = (
            previous_body
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

        for lookback in (
            STRUCTURE_LOOKBACKS
        ):
            previous_low = min(
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
                - previous_low
            ) / current_atr

        daily = previous_completed_daily(
            signal["time"],
            daily_state,
        )

        ny_time = (
            signal["time"]
            .astimezone(
                NY_TZ
            )
        )

        candidates.append({
            "index": index,
            "time": signal["time"],
            "body_ratio": body_ratio,
            "body_atr": body_atr,
            "previous_body_atr": (
                previous_body_atr
            ),
            "close_location": (
                close_location
            ),
            "structure_distances": (
                structure_distances
            ),
            "range_atr": range_atr,
            "lower_wick_body": (
                lower_wick_body
            ),
            "upper_wick_body": (
                upper_wick_body
            ),
            "stop_size_atr": (
                stop_size_atr
            ),
            "momentum": momentum,
            "daily": daily,
            "ny_hour": ny_time.hour,
            "weekday": ny_time.weekday(),
        })

    return candidates


# ============================================================
# FILTERS
# ============================================================

def passes_confirmation(
    signal,
    config,
):
    lookback = config[
        "structure_lookback"
    ]

    if (
        signal[
            "structure_distances"
        ][
            lookback
        ]
        > config[
            "structure_distance_atr"
        ]
    ):
        return False

    if (
        signal[
            "range_atr"
        ]
        < config[
            "minimum_range_atr"
        ]
    ):
        return False

    if (
        signal[
            "lower_wick_body"
        ]
        < config[
            "minimum_lower_wick_body"
        ]
    ):
        return False

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
        ] < config[
            "minimum_daily_atr_ratio_50"
        ]
    ):
        return False

    minimum_body_atr = config.get(
        "minimum_body_atr"
    )

    if (
        minimum_body_atr is not None
        and signal[
            "body_atr"
        ] < minimum_body_atr
    ):
        return False

    minimum_body_ratio = config.get(
        "minimum_body_ratio_overlay",
        1.00,
    )

    if (
        signal[
            "body_ratio"
        ] < minimum_body_ratio
    ):
        return False

    ema_length = config.get(
        "daily_close_above_ema"
    )

    if (
        ema_length is not None
    ):
        if (
            daily[
                "emas"
            ].get(
                ema_length
            ) is None
            or not (
                daily[
                    "close"
                ]
                > daily[
                    "emas"
                ][
                    ema_length
                ]
            )
        ):
            return False

    if (
        config.get(
            "exclude_ny_01",
            False,
        )
        and signal[
            "ny_hour"
        ] == 1
    ):
        return False

    return True


def passes_current_control(
    signal
):
    if (
        signal[
            "structure_distances"
        ][40]
        > 0.20
    ):
        return False

    if (
        signal[
            "lower_wick_body"
        ] < 0.20
    ):
        return False

    daily = signal[
        "daily"
    ]

    if (
        daily is None
        or daily[
            "ema200"
        ] is None
        or not (
            daily["close"]
            > daily["ema200"]
        )
    ):
        return False

    if (
        signal["ny_hour"]
        in {
            0, 1, 2, 3, 4
        }
    ):
        return False

    return True


# ============================================================
# EXIT CACHE / TRADE SIMULATION
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

    if actual_risk <= 0:
        raise RuntimeError(
            "Invalid actual risk"
        )

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

        elif stop_hit:
            exit_price = stop

        else:
            exit_price = target

        result = {
            "signal_index": (
                signal_index
            ),
            "signal_time": (
                signal["time"]
            ),
            "exit_index": index,
            "exit_time": (
                candle["time"]
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

        # Locked convention:
        # signals strictly BEFORE exit candle are ignored.
        # signal exactly ON exit candle is allowed.
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

        trades.append(
            trade
        )

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
        for trade in selected
    ]

    winners = [
        r for r in results
        if r > 0
    ]

    losers = [
        r for r in results
        if r < 0
    ]

    gross_profit = sum(
        winners
    )

    gross_loss = abs(
        sum(
            losers
        )
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
            year=(
                dt.year
                - years
            )
        )
    except ValueError:
        return dt.replace(
            month=2,
            day=28,
            year=(
                dt.year
                - years
            ),
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


def make_result_row(
    label,
    eligible,
    trades,
    ignored,
    years,
    parameters,
):
    full = stats_for_trades(
        trades
    )

    row = {
        "type": label,
        "eligible_signals": (
            len(eligible)
        ),
        "ignored_due_to_open_trade": (
            ignored
        ),
        "trades": (
            full["trades"]
        ),
        "trades_per_year": round(
            full["trades"]
            / years,
            2,
        ),
        "winners": (
            full["winners"]
        ),
        "losers": (
            full["losers"]
        ),
        "win_rate": (
            full["win_rate"]
        ),
        "profit_factor": (
            full["profit_factor"]
        ),
        "total_r": (
            full["total_r"]
        ),
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

    row.update(
        parameters
    )

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
        ] = (
            stats["trades"]
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
            stats["total_r"]
        )

        row[
            f"{era_name}_expectancy"
        ] = (
            stats[
                "expectancy_r"
            ]
        )

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
        ] = (
            stats["trades"]
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
            stats["total_r"]
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
            "state": "fetching_h1",
            "message": (
                "Fetching USD/CAD H1 history"
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

        if not h1:
            raise RuntimeError(
                "No H1 candles returned"
            )

        STATUS.update({
            "state": "fetching_daily",
            "message": (
                "Fetching USD/CAD daily history"
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

        if not daily:
            raise RuntimeError(
                "No daily candles returned"
            )

        STATUS.update({
            "state": "precomputing",
            "message": (
                "Precomputing engulfing features"
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

        rows = []

        # ----------------------------------------------------
        # CURRENT LIVE CONTROL
        # ----------------------------------------------------

        STATUS.update({
            "state": "running_control",
            "message": (
                "Running current live control"
            ),
        })

        control_eligible = [
            signal
            for signal
            in raw_candidates
            if passes_current_control(
                signal
            )
        ]

        (
            control_trades,
            control_ignored,
        ) = simulate_variant(
            h1,
            control_eligible,
        )

        control_row = make_result_row(
            "CURRENT_LIVE_CONTROL",
            control_eligible,
            control_trades,
            control_ignored,
            years,
            {
                "anchor": None,
                "structure_lookback": 40,
                "structure_distance_atr": 0.20,
                "minimum_range_atr": None,
                "minimum_lower_wick_body": 0.20,
                "minimum_daily_atr_ratio_50": None,
                "daily_close_above_ema": 200,
                "minimum_body_atr": None,
                "minimum_body_ratio_overlay": 1.00,
                "exclude_ny_01": False,
                "control_excludes_ny_00_04": True,
            },
        )

        rows.append(
            control_row
        )

        STATUS[
            "completed_tests"
        ] = 1

        # ----------------------------------------------------
        # CONTROLLED CONFIRMATION MATRIX
        # ----------------------------------------------------

        STATUS.update({
            "state": "running_confirmation",
            "message": (
                f"Running {TOTAL_CONFIRMATION_TESTS} "
                f"controlled confirmation tests"
            ),
        })

        completed = 1

        for config in CONFIRMATION_CONFIGS:
            eligible = [
                signal
                for signal
                in raw_candidates
                if passes_confirmation(
                    signal,
                    config,
                )
            ]

            (
                trades,
                ignored,
            ) = simulate_variant(
                h1,
                eligible,
            )

            row = make_result_row(
                "CONFIRMATION",
                eligible,
                trades,
                ignored,
                years,
                {
                    "anchor": (
                        config[
                            "anchor"
                        ]
                    ),
                    "structure_lookback": (
                        config[
                            "structure_lookback"
                        ]
                    ),
                    "structure_distance_atr": (
                        config[
                            "structure_distance_atr"
                        ]
                    ),
                    "minimum_range_atr": (
                        config[
                            "minimum_range_atr"
                        ]
                    ),
                    "minimum_lower_wick_body": (
                        config[
                            "minimum_lower_wick_body"
                        ]
                    ),
                    "minimum_daily_atr_ratio_50": (
                        config[
                            "minimum_daily_atr_ratio_50"
                        ]
                    ),
                    "daily_close_above_ema": (
                        config[
                            "daily_close_above_ema"
                        ]
                    ),
                    "minimum_body_atr": (
                        config[
                            "minimum_body_atr"
                        ]
                    ),
                    "minimum_body_ratio_overlay": (
                        config[
                            "minimum_body_ratio_overlay"
                        ]
                    ),
                    "exclude_ny_01": (
                        config[
                            "exclude_ny_01"
                        ]
                    ),
                    "control_excludes_ny_00_04": False,
                },
            )

            rows.append(
                row
            )

            completed += 1
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

        df = pd.DataFrame(
            rows
        )

        control_df = df[
            df["type"]
            == "CURRENT_LIVE_CONTROL"
        ]

        confirmation_df = df[
            df["type"]
            == "CONFIRMATION"
        ].copy()

        confirmation_df = (
            confirmation_df.sort_values(
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
        )

        df = pd.concat(
            [
                control_df,
                confirmation_df,
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
                "USD/CAD long controlled confirmation matrix complete"
            ),
            "completed_tests": (
                TOTAL_TESTS
            ),
            "rows_saved": (
                len(df)
            ),
            "output_file": (
                OUTPUT_FILE
            ),
            "control_summary": {
                "trades": int(
                    control_row[
                        "trades"
                    ]
                ),
                "profit_factor": (
                    control_row[
                        "profit_factor"
                    ]
                ),
                "total_r": (
                    control_row[
                        "total_r"
                    ]
                ),
                "expectancy_r": (
                    control_row[
                        "expectancy_r"
                    ]
                ),
                "max_drawdown_r": (
                    control_row[
                        "max_drawdown_r"
                    ]
                ),
            },
        })

        print()
        print("=" * 90)
        print(
            "USD/CAD LONG CONTROLLED CONFIRMATION MATRIX COMPLETE"
        )
        print("=" * 90)
        print(
            f"Confirmation tests: {TOTAL_CONFIRMATION_TESTS}"
        )
        print(
            f"Rows saved: {len(df)}"
        )
        print(
            f"Output: {OUTPUT_FILE}"
        )
        print()

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
            "USD/CAD Long Controlled Confirmation Matrix"
        ),
        "status": STATUS,
        "instrument": INSTRUMENT,
        "direction": "LONG",
        "mode": "READ_ONLY_RESEARCH",
        "orders_supported": False,
        "trading_enabled": False,
        "confirmation_tests": (
            TOTAL_CONFIRMATION_TESTS
        ),
        "total_tests_including_control": (
            TOTAL_TESTS
        ),
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
    research_thread = threading.Thread(
        target=run_research,
        name=(
            "usdcad-long-controlled-confirmation"
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
