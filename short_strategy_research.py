import os
import itertools
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# USD/CAD SHORT - FINAL GEOMETRY / REGIME FEATURE SWEEP
#
# RESEARCH ONLY — NEVER SUBMITS ORDERS.
#
# Purpose:
#   Give USD/CAD short one final serious attempt using genuinely
#   different information from the earlier searches.
#
# Starting point:
#   We already know that loosening filters can recover frequency
#   to ~4-5 trades/year, but PF tends to fall into ~1.35-1.39.
#
# This run asks whether geometry / regime information can improve
# that compromise without simply re-tightening the old filters.
#
# Structural baseline grid:
#   Body ratio:
#     1.40 / 1.50 / 1.55
#
#   Structure:
#     50 / 60 / 70
#
#   Distance to prior H1 high:
#     0.15 / 0.20 / 0.25 ATR
#
#   Daily close below slow EMA:
#     EMA250 / EMA300 / EMA325
#
#   24h upward momentum:
#     0.50 / 0.75 ATR
#
#   Minimum signal range:
#     OFF / 0.90 ATR
#
#   RR:
#     2.75 / 3.00 / 3.25
#
# NEW feature families tested ONE AT A TIME:
#
#   1) STOP_SIZE_MAX
#      stop distance / H1 ATR <=
#      0.80 / 1.00 / 1.20 / 1.40 / 1.60
#
#   2) RANGE_BAND
#      signal candle range / ATR inside:
#      0.80-1.20
#      0.90-1.30
#      1.00-1.40
#      1.00-1.60
#      1.10-1.50
#
#   3) DAILY_EMA_DISTANCE_BAND
#      (EMA - daily close) / daily ATR14 inside:
#      0.00-0.25
#      0.00-0.50
#      0.25-0.75
#      0.25-1.00
#      0.50-1.25
#
#   4) DAILY_SWING_HIGH_DISTANCE
#      prior completed daily close within X ATR14
#      of previous N-day high:
#      N = 5 / 10 / 20
#      X = 0.50 / 1.00 / 1.50
#
#   5) PREVIOUS_BULL_CANDLE_QUALITY
#      previous candle body / ATR >=
#      0.20 / 0.30 / 0.40 / 0.50
#
#   6) SHORT_RALLY_CONTEXT
#      close relative to N bars ago / ATR >=
#      N = 3 / 6 / 12
#      threshold = 0.25 / 0.50 / 0.75
#
#   7) DAILY_ATR_PERCENTILE
#      current daily ATR14 percentile over prior 100 days >=
#      40 / 50 / 60 / 70
#
#   8) EMA_ACCELERATION
#      slow EMA 5-day slope more negative than previous 5-day slope
#      by at least:
#      0.00 / 0.02 / 0.05 ATR
#
# One feature family is active at a time.
# NO timing / weekday optimisation.
#
# Exact backtest conventions:
#   OANDA midpoint H1
#   Daily alignment = 17:00 America/New_York
#   Previous completed daily candle only
#   ATR14 = Wilder/RMA
#   Daily EMA = SMA-seeded EMA
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

INSTRUMENT = "USD_CAD"
TICK_SIZE = 0.00001

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

H1_WARMUP_DAYS = 260
DAILY_WARMUP_DAYS = 3200

OUTPUT_FILE = "usdcad_short_geometry_regime_features.csv"


# ============================================================
# STRUCTURAL BASELINES
# ============================================================

BODY_RATIOS = [
    1.40,
    1.50,
    1.55,
]

STRUCTURE_LOOKBACKS = [
    50,
    60,
    70,
]

MAX_DISTANCE_ATR_VALUES = [
    0.15,
    0.20,
    0.25,
]

SLOW_EMAS = [
    250,
    300,
    325,
]

MOMENTUM_LOOKBACK = 24

MIN_UP_MOMENTUM_ATR = [
    0.50,
    0.75,
]

MIN_RANGE_ATR_OPTIONS = [
    None,
    0.90,
]

REWARD_RISKS = [
    2.75,
    3.00,
    3.25,
]


# ============================================================
# NEW FEATURE VALUES
# ============================================================

STOP_SIZE_MAX_VALUES = [
    0.80,
    1.00,
    1.20,
    1.40,
    1.60,
]

RANGE_BANDS = [
    (0.80, 1.20),
    (0.90, 1.30),
    (1.00, 1.40),
    (1.00, 1.60),
    (1.10, 1.50),
]

DAILY_EMA_DISTANCE_BANDS = [
    (0.00, 0.25),
    (0.00, 0.50),
    (0.25, 0.75),
    (0.25, 1.00),
    (0.50, 1.25),
]

DAILY_SWING_LOOKBACKS = [
    5,
    10,
    20,
]

DAILY_SWING_MAX_DISTANCE_ATR = [
    0.50,
    1.00,
    1.50,
]

PREVIOUS_BULL_BODY_ATR_MIN = [
    0.20,
    0.30,
    0.40,
    0.50,
]

SHORT_RALLY_LOOKBACKS = [
    3,
    6,
    12,
]

SHORT_RALLY_MIN_ATR = [
    0.25,
    0.50,
    0.75,
]

DAILY_ATR_PERCENTILE_MIN = [
    40,
    50,
    60,
    70,
]

EMA_ACCELERATION_MIN_ATR = [
    0.00,
    0.02,
    0.05,
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

STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "service": "USDCAD Short Final Geometry Regime Feature Sweep",
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
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

    initial = sum(values[:length]) / length
    result[length - 1] = initial

    multiplier = 2.0 / (length + 1.0)
    previous = initial

    for index in range(length, len(values)):
        current = (
            (values[index] - previous)
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
            tr = candle["high"] - candle["low"]
        else:
            previous_close = candles[index - 1]["close"]

            tr = max(
                candle["high"] - candle["low"],
                abs(candle["high"] - previous_close),
                abs(candle["low"] - previous_close),
            )

        result.append(tr)

    return result


def rma_series(values, length):
    result = [None] * len(values)

    if len(values) < length:
        return result

    initial = sum(values[:length]) / length
    result[length - 1] = initial
    previous = initial

    for index in range(length, len(values)):
        current = (
            previous * (length - 1)
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


def rolling_percentile_rank(values, lookback):
    result = [None] * len(values)

    for index in range(len(values)):
        current = values[index]

        if current is None:
            continue

        start = index - lookback

        if start < 0:
            continue

        window = [
            value
            for value in values[start:index]
            if value is not None
        ]

        if len(window) < lookback:
            continue

        less_equal = sum(
            1
            for value in window
            if value <= current
        )

        result[index] = (
            less_equal
            / len(window)
            * 100.0
        )

    return result


# ============================================================
# DAILY ALIGNMENT / FEATURES
# ============================================================

def current_daily_start(timestamp_utc):
    ny_time = timestamp_utc.astimezone(NY_TZ)

    candidate = ny_time.replace(
        hour=DAILY_ALIGNMENT_HOUR,
        minute=0,
        second=0,
        microsecond=0,
    )

    if ny_time < candidate:
        candidate -= timedelta(days=1)

    return candidate.astimezone(timezone.utc)


def build_daily_state(daily):
    closes = [
        candle["close"]
        for candle in daily
    ]

    daily_atr = atr_series(
        daily,
        14,
    )

    atr_percentile_100 = rolling_percentile_rank(
        daily_atr,
        100,
    )

    ema_map = {
        length: ema_series(
            closes,
            length,
        )
        for length in SLOW_EMAS
    }

    return {
        "emas": ema_map,
        "atr14": daily_atr,
        "atr_percentile_100": atr_percentile_100,
    }


def build_h1_daily_lookup(
    h1,
    daily,
    daily_state,
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

        lookup[h1_index] = {
            "daily_index": daily_index,
            "close": daily[
                daily_index
            ]["close"],
            "high": daily[
                daily_index
            ]["high"],
            "atr14": daily_state[
                "atr14"
            ][daily_index],
            "atr_percentile_100": daily_state[
                "atr_percentile_100"
            ][daily_index],
            "emas": {
                length:
                daily_state[
                    "emas"
                ][length][daily_index]
                for length in SLOW_EMAS
            },
            "ema_series": daily_state[
                "emas"
            ],
        }

    return lookup


# ============================================================
# SIGNAL MATRIX
# ============================================================

def build_candidates(
    h1,
    h1_atr,
    daily,
    daily_lookup,
):
    candidates = []

    max_lookback = max(
        max(STRUCTURE_LOOKBACKS),
        MOMENTUM_LOOKBACK,
        max(SHORT_RALLY_LOOKBACKS),
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
        daily_info = daily_lookup[index]

        if (
            atr is None
            or atr <= 0
            or daily_info is None
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
            previous["close"] > previous["open"]
            and signal["close"] < signal["open"]
            and signal["open"] >= previous["close"]
            and signal["close"] <= previous["open"]
        )

        if not bearish_engulfing:
            continue

        structure_distances = {}

        for lookback in STRUCTURE_LOOKBACKS:
            previous_highest = max(
                candle["high"]
                for candle in h1[
                    index - lookback:index
                ]
            )

            structure_distances[
                lookback
            ] = (
                previous_highest
                - signal["high"]
            ) / atr

        range_atr = (
            candle_range / atr
        )

        up_momentum_24 = (
            signal["close"]
            - h1[
                index - MOMENTUM_LOOKBACK
            ]["close"]
        ) / atr

        previous_body_atr = (
            previous_body / atr
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

        short_rally = {}

        for lookback in SHORT_RALLY_LOOKBACKS:
            short_rally[
                lookback
            ] = (
                signal["close"]
                - h1[
                    index - lookback
                ]["close"]
            ) / atr

        daily_index = daily_info[
            "daily_index"
        ]

        daily_swing_distances = {}

        daily_atr = daily_info[
            "atr14"
        ]

        if (
            daily_atr is not None
            and daily_atr > 0
        ):
            for lookback in DAILY_SWING_LOOKBACKS:
                start = (
                    daily_index
                    - lookback
                )

                if start >= 0:
                    previous_daily_high = max(
                        candle["high"]
                        for candle in daily[
                            start:daily_index
                        ]
                    )

                    daily_swing_distances[
                        lookback
                    ] = (
                        previous_daily_high
                        - daily_info["close"]
                    ) / daily_atr

                else:
                    daily_swing_distances[
                        lookback
                    ] = None
        else:
            for lookback in DAILY_SWING_LOOKBACKS:
                daily_swing_distances[
                    lookback
                ] = None

        candidates.append({
            "index": index,
            "time": signal["time"],
            "body_ratio": (
                current_body / previous_body
            ),
            "structure_distances": (
                structure_distances
            ),
            "range_atr": (
                range_atr
            ),
            "up_momentum_24": (
                up_momentum_24
            ),
            "previous_body_atr": (
                previous_body_atr
            ),
            "stop_size_atr": (
                stop_size_atr
            ),
            "short_rally": (
                short_rally
            ),
            "daily_swing_distances": (
                daily_swing_distances
            ),
            "daily": daily_info,
        })

    return candidates


# ============================================================
# BASELINE FILTER
# ============================================================

def baseline_allowed(
    candidate,
    baseline,
):
    if (
        candidate["body_ratio"]
        < baseline["body_ratio"]
    ):
        return False

    if (
        candidate[
            "structure_distances"
        ][
            baseline[
                "structure_lookback"
            ]
        ]
        > baseline[
            "max_distance_atr"
        ]
    ):
        return False

    slow = candidate[
        "daily"
    ]["emas"].get(
        baseline[
            "slow_ema"
        ]
    )

    if slow is None:
        return False

    if not (
        candidate[
            "daily"
        ]["close"]
        < slow
    ):
        return False

    if (
        candidate[
            "up_momentum_24"
        ]
        < baseline[
            "momentum_threshold"
        ]
    ):
        return False

    if (
        baseline[
            "min_range_atr"
        ]
        is not None
    ):
        if (
            candidate[
                "range_atr"
            ]
            < baseline[
                "min_range_atr"
            ]
        ):
            return False

    return True


# ============================================================
# NEW FEATURE FILTER
# ============================================================

def feature_allowed(
    candidate,
    feature_family,
    feature_value,
    slow_ema,
):
    if feature_family == "BASELINE":
        return True

    if feature_family == "STOP_SIZE_MAX":
        return (
            candidate[
                "stop_size_atr"
            ]
            <= feature_value
        )

    if feature_family == "RANGE_BAND":
        minimum, maximum = (
            feature_value
        )

        return (
            candidate[
                "range_atr"
            ]
            >= minimum
            and candidate[
                "range_atr"
            ]
            <= maximum
        )

    if feature_family == "DAILY_EMA_DISTANCE_BAND":
        minimum, maximum = (
            feature_value
        )

        daily = candidate[
            "daily"
        ]

        slow = daily[
            "emas"
        ].get(
            slow_ema
        )

        atr = daily[
            "atr14"
        ]

        if (
            slow is None
            or atr is None
            or atr <= 0
        ):
            return False

        distance = (
            slow
            - daily[
                "close"
            ]
        ) / atr

        return (
            distance >= minimum
            and distance <= maximum
        )

    if feature_family == "DAILY_SWING_HIGH_DISTANCE":
        lookback, maximum = (
            feature_value
        )

        distance = candidate[
            "daily_swing_distances"
        ].get(
            lookback
        )

        if distance is None:
            return False

        return (
            distance <= maximum
        )

    if feature_family == "PREVIOUS_BULL_CANDLE_QUALITY":
        return (
            candidate[
                "previous_body_atr"
            ]
            >= feature_value
        )

    if feature_family == "SHORT_RALLY_CONTEXT":
        lookback, minimum = (
            feature_value
        )

        return (
            candidate[
                "short_rally"
            ][lookback]
            >= minimum
        )

    if feature_family == "DAILY_ATR_PERCENTILE":
        percentile = candidate[
            "daily"
        ][
            "atr_percentile_100"
        ]

        if percentile is None:
            return False

        return (
            percentile
            >= feature_value
        )

    if feature_family == "EMA_ACCELERATION":
        minimum_acceleration = (
            feature_value
        )

        daily = candidate[
            "daily"
        ]

        daily_index = daily[
            "daily_index"
        ]

        if daily_index < 10:
            return False

        ema_values = daily[
            "ema_series"
        ][slow_ema]

        current = ema_values[
            daily_index
        ]

        five_ago = ema_values[
            daily_index - 5
        ]

        ten_ago = ema_values[
            daily_index - 10
        ]

        atr = daily[
            "atr14"
        ]

        if (
            current is None
            or five_ago is None
            or ten_ago is None
            or atr is None
            or atr <= 0
        ):
            return False

        recent_slope = (
            current
            - five_ago
        ) / atr

        previous_slope = (
            five_ago
            - ten_ago
        ) / atr

        acceleration = (
            previous_slope
            - recent_slope
        )

        return (
            recent_slope < 0
            and acceleration
            >= minimum_acceleration
        )

    raise RuntimeError(
        f"Unknown feature family: "
        f"{feature_family}"
    )


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
    baseline,
    feature_family,
    feature_label,
    feature_value_serialized,
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
        "body_ratio": (
            baseline[
                "body_ratio"
            ]
        ),
        "structure_lookback": (
            baseline[
                "structure_lookback"
            ]
        ),
        "max_distance_atr": (
            baseline[
                "max_distance_atr"
            ]
        ),
        "slow_daily_ema": (
            baseline[
                "slow_ema"
            ]
        ),
        "momentum_lookback_h": (
            MOMENTUM_LOOKBACK
        ),
        "min_up_momentum_atr": (
            baseline[
                "momentum_threshold"
            ]
        ),
        "baseline_min_range_atr": (
            baseline[
                "min_range_atr"
            ]
        ),
        "reward_risk": (
            baseline[
                "reward_risk"
            ]
        ),
        "feature_family": (
            feature_family
        ),
        "feature_label": (
            feature_label
        ),
        "feature_value": (
            feature_value_serialized
        ),
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

        if era[
            "trades"
        ] >= 5:
            if era[
                "total_r"
            ] > 0:
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

    return row


# ============================================================
# FEATURE TEST DEFINITIONS
# ============================================================

def feature_tests():
    tests = [
        (
            "BASELINE",
            "baseline",
            None,
            "none",
        ),
    ]

    for value in STOP_SIZE_MAX_VALUES:
        tests.append(
            (
                "STOP_SIZE_MAX",
                f"stop_size_atr_le_{value:.2f}",
                value,
                f"{value:.2f}",
            )
        )

    for minimum, maximum in RANGE_BANDS:
        tests.append(
            (
                "RANGE_BAND",
                (
                    f"range_atr_"
                    f"{minimum:.2f}_to_{maximum:.2f}"
                ),
                (
                    minimum,
                    maximum,
                ),
                (
                    f"{minimum:.2f},"
                    f"{maximum:.2f}"
                ),
            )
        )

    for minimum, maximum in DAILY_EMA_DISTANCE_BANDS:
        tests.append(
            (
                "DAILY_EMA_DISTANCE_BAND",
                (
                    f"daily_ema_distance_"
                    f"{minimum:.2f}_to_{maximum:.2f}_atr"
                ),
                (
                    minimum,
                    maximum,
                ),
                (
                    f"{minimum:.2f},"
                    f"{maximum:.2f}"
                ),
            )
        )

    for lookback in DAILY_SWING_LOOKBACKS:
        for maximum in DAILY_SWING_MAX_DISTANCE_ATR:
            tests.append(
                (
                    "DAILY_SWING_HIGH_DISTANCE",
                    (
                        f"daily_{lookback}d_high_"
                        f"distance_le_{maximum:.2f}_atr"
                    ),
                    (
                        lookback,
                        maximum,
                    ),
                    (
                        f"{lookback},"
                        f"{maximum:.2f}"
                    ),
                )
            )

    for value in PREVIOUS_BULL_BODY_ATR_MIN:
        tests.append(
            (
                "PREVIOUS_BULL_CANDLE_QUALITY",
                (
                    f"previous_bull_body_atr_"
                    f"ge_{value:.2f}"
                ),
                value,
                f"{value:.2f}",
            )
        )

    for lookback in SHORT_RALLY_LOOKBACKS:
        for minimum in SHORT_RALLY_MIN_ATR:
            tests.append(
                (
                    "SHORT_RALLY_CONTEXT",
                    (
                        f"rally_{lookback}h_"
                        f"ge_{minimum:.2f}_atr"
                    ),
                    (
                        lookback,
                        minimum,
                    ),
                    (
                        f"{lookback},"
                        f"{minimum:.2f}"
                    ),
                )
            )

    for value in DAILY_ATR_PERCENTILE_MIN:
        tests.append(
            (
                "DAILY_ATR_PERCENTILE",
                (
                    f"daily_atr_percentile_"
                    f"ge_{value}"
                ),
                value,
                str(value),
            )
        )

    for value in EMA_ACCELERATION_MIN_ATR:
        tests.append(
            (
                "EMA_ACCELERATION",
                (
                    f"ema_negative_acceleration_"
                    f"ge_{value:.2f}_atr"
                ),
                value,
                f"{value:.2f}",
            )
        )

    return tests


# ============================================================
# RESEARCH
# ============================================================

def run_research():
    global STATUS

    try:
        print()
        print("=" * 76)
        print(
            "USD/CAD SHORT - FINAL GEOMETRY / REGIME FEATURE SWEEP"
        )
        print("=" * 76)
        print(
            "One genuinely new feature family at a time"
        )
        print(
            "NO timing / weekday optimisation"
        )
        print()

        STATUS.update({
            "state": "fetching_data",
            "message": (
                "Fetching USD/CAD OANDA history"
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
                "No USD/CAD H1 candles returned"
            )

        if not daily:
            raise RuntimeError(
                "No USD/CAD daily candles returned"
            )

        STATUS.update({
            "state": "precomputing",
            "message": (
                "Building USD/CAD geometry/regime matrix"
            ),
        })

        h1_atr = atr_series(
            h1,
            14,
        )

        daily_state = build_daily_state(
            daily
        )

        daily_lookup = build_h1_daily_lookup(
            h1,
            daily,
            daily_state,
        )

        candidates = build_candidates(
            h1,
            h1_atr,
            daily,
            daily_lookup,
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

        baselines = []

        for (
            body_ratio,
            structure_lookback,
            max_distance_atr,
            slow_ema,
            momentum_threshold,
            min_range_atr,
            reward_risk,
        ) in itertools.product(
            BODY_RATIOS,
            STRUCTURE_LOOKBACKS,
            MAX_DISTANCE_ATR_VALUES,
            SLOW_EMAS,
            MIN_UP_MOMENTUM_ATR,
            MIN_RANGE_ATR_OPTIONS,
            REWARD_RISKS,
        ):
            baselines.append({
                "body_ratio": body_ratio,
                "structure_lookback": structure_lookback,
                "max_distance_atr": max_distance_atr,
                "slow_ema": slow_ema,
                "momentum_threshold": momentum_threshold,
                "min_range_atr": min_range_atr,
                "reward_risk": reward_risk,
            })

        tests = feature_tests()

        total_tests = (
            len(baselines)
            * len(tests)
        )

        STATUS[
            "total_baselines"
        ] = len(
            baselines
        )

        STATUS[
            "tests_per_baseline"
        ] = len(
            tests
        )

        STATUS[
            "total_tests"
        ] = total_tests

        STATUS.update({
            "state": "running",
            "message": (
                "Running final USD/CAD geometry/regime sweep"
            ),
        })

        rows = []
        test_number = 0

        for baseline in baselines:
            baseline_candidates = [
                candidate
                for candidate in candidates
                if baseline_allowed(
                    candidate,
                    baseline,
                )
            ]

            for (
                feature_family,
                feature_label,
                feature_value,
                feature_value_serialized,
            ) in tests:
                eligible = [
                    candidate
                    for candidate in baseline_candidates
                    if feature_allowed(
                        candidate,
                        feature_family,
                        feature_value,
                        baseline[
                            "slow_ema"
                        ],
                    )
                ]

                (
                    trades,
                    ignored,
                    still_open,
                ) = simulate(
                    h1,
                    eligible,
                    baseline[
                        "reward_risk"
                    ],
                )

                rows.append(
                    make_result_row(
                        baseline,
                        feature_family,
                        feature_label,
                        feature_value_serialized,
                        eligible,
                        trades,
                        ignored,
                        still_open,
                        years,
                    )
                )

                test_number += 1

                STATUS[
                    "completed_tests"
                ] = test_number

                if (
                    test_number
                    % 500
                    == 0
                ):
                    print(
                        f"Progress: "
                        f"{test_number}/"
                        f"{total_tests}",
                        flush=True,
                    )

        df = pd.DataFrame(
            rows
        )

        if df.empty:
            raise RuntimeError(
                "No USD/CAD geometry/regime rows generated"
            )

        df[
            "frequency_4py"
        ] = (
            df[
                "trades_per_year"
            ]
            >= 4.0
        )

        df[
            "frequency_5py"
        ] = (
            df[
                "trades_per_year"
            ]
            >= 5.0
        )

        df[
            "adequate_90"
        ] = (
            df[
                "trades"
            ]
            >= 90
        )

        df[
            "adequate_100"
        ] = (
            df[
                "trades"
            ]
            >= 100
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
            "pf_140"
        ] = (
            df[
                "profit_factor"
            ]
            >= 1.40
        )

        df[
            "pf_150"
        ] = (
            df[
                "profit_factor"
            ]
            >= 1.50
        )

        df[
            "target_profile"
        ] = (
            df[
                "frequency_4py"
            ]
            & df[
                "adequate_90"
            ]
            & df[
                "all_four_eras_profitable"
            ]
            & df[
                "pf_140"
            ]
            & df[
                "worst_era_pf_115"
            ]
        )

        df[
            "strong_target_profile"
        ] = (
            df[
                "frequency_4py"
            ]
            & df[
                "adequate_100"
            ]
            & df[
                "all_four_eras_profitable"
            ]
            & df[
                "pf_150"
            ]
            & df[
                "worst_era_pf_120"
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
                "strong_target_profile",
                "target_profile",
                "all_four_eras_profitable",
                "frequency_4py",
                "adequate_90",
                "worst_era_pf_120",
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
                "USD/CAD final geometry/regime sweep "
                "completed successfully"
            ),
            "completed_tests": (
                total_tests
            ),
            "rows_saved": len(
                df
            ),
            "target_profile_count": int(
                df[
                    "target_profile"
                ].sum()
            ),
            "strong_target_profile_count": int(
                df[
                    "strong_target_profile"
                ].sum()
            ),
            "all_four_eras_profitable": int(
                df[
                    "all_four_eras_profitable"
                ].sum()
            ),
            "frequency_4py_count": int(
                df[
                    "frequency_4py"
                ].sum()
            ),
            "output_file": (
                OUTPUT_FILE
            ),
        })

        print()
        print("=" * 76)
        print(
            "USD/CAD FINAL GEOMETRY / REGIME SWEEP COMPLETE"
        )
        print("=" * 76)
        print(
            "Rows:",
            len(df),
        )
        print(
            ">=4 trades/year:",
            int(
                df[
                    "frequency_4py"
                ].sum()
            ),
        )
        print(
            "Target profiles:",
            int(
                df[
                    "target_profile"
                ].sum()
            ),
        )
        print(
            "Strong target profiles:",
            int(
                df[
                    "strong_target_profile"
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
    tests = feature_tests()

    return jsonify({
        "service": (
            "USDCAD Short Final Geometry Regime Feature Sweep"
        ),
        "status": STATUS,
        "instrument": INSTRUMENT,
        "direction": "SHORT",
        "objective": (
            "Find >=4 trades/year with PF >=1.40 "
            "using genuinely new geometry/regime information"
        ),
        "timing_filters": (
            "NONE - all hours and weekdays"
        ),
        "baseline_grid": {
            "body_ratios": BODY_RATIOS,
            "structure_lookbacks": STRUCTURE_LOOKBACKS,
            "max_distance_atr": MAX_DISTANCE_ATR_VALUES,
            "slow_emas": SLOW_EMAS,
            "momentum_lookback_h": MOMENTUM_LOOKBACK,
            "min_up_momentum_atr": MIN_UP_MOMENTUM_ATR,
            "min_range_atr_options": MIN_RANGE_ATR_OPTIONS,
            "reward_risks": REWARD_RISKS,
        },
        "feature_families": sorted(
            list(
                set(
                    item[0]
                    for item in tests
                )
            )
        ),
        "tests_per_baseline": len(
            tests
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
                "USD/CAD geometry/regime CSV "
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
            "usdcad-short-final-geometry-regime"
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
