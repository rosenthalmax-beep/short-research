import os
import threading
import itertools
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# GBP/USD LONG - CORE INTERACTION MATRIX
#
# RESEARCH ONLY - NEVER SUBMITS ORDERS.
#
# PURPOSE
# ------------------------------------------------------------
# Based on single-factor discovery, the strongest standalone
# clue is DEEPER STRUCTURE, especially around 50-80 bars.
#
# Core matrix:
#
#   structure lookback:
#       40, 50, 60, 70, 80, 90
#
#   structure distance / ATR:
#       0.00, 0.05, 0.10, 0.15, 0.20
#
#   minimum body / ATR:
#       none, 0.80, 1.00, 1.20
#
#   minimum range / ATR:
#       none, 1.10, 1.20, 1.30, 1.40
#
#   previous completed daily close regime:
#       none, EMA50, EMA70, EMA100, EMA150
#
#   daily EMA alignment:
#       none
#       EMA20 > EMA70
#       EMA30 > EMA70
#       EMA50 > EMA70
#
# Total core:
#   6 * 5 * 4 * 5 * 5 * 4 = 12,000
#
# Controlled sidecars then overlay:
#   - strong close
#   - body ratio
#   - NY 14:00-18:59 exclusion
#
# RR fixed at 4.25.
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
# Raw minimum body ratio = 1.00.
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
# Same-bar tie:
#   compare open->high vs open->low
#   high closer => target first
#   else stop first.
#
# Signals signal_index < position_exit_index ignored.
# Exact exit-candle signal allowed.
#
# Exits begin signal_index + 1.
#
# Daily:
#   dailyAlignment = 17
#   alignmentTimezone = America/New_York
#   previous completed daily candle only.
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
RAW_MINIMUM_BODY_RATIO = 1.00

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
D_WARMUP_DAYS = 2500

OUTPUT_CORE = (
    "gbpusd_long_core_interaction_matrix.csv"
)

OUTPUT_SIDECARS = (
    "gbpusd_long_core_interaction_sidecars.csv"
)


# ============================================================
# CORE GRID
# ============================================================

STRUCTURE_LOOKBACK_VALUES = [
    40, 50, 60, 70, 80, 90,
]

STRUCTURE_DISTANCE_VALUES = [
    0.00, 0.05, 0.10, 0.15, 0.20,
]

BODY_ATR_VALUES = [
    None, 0.80, 1.00, 1.20,
]

RANGE_ATR_VALUES = [
    None, 1.10, 1.20, 1.30, 1.40,
]

DAILY_CLOSE_REGIMES = [
    None, 50, 70, 100, 150,
]

DAILY_ALIGNMENT_REGIMES = [
    None,
    (20, 70),
    (30, 70),
    (50, 70),
]

CORE_CONFIGS = list(
    itertools.product(
        STRUCTURE_LOOKBACK_VALUES,
        STRUCTURE_DISTANCE_VALUES,
        BODY_ATR_VALUES,
        RANGE_ATR_VALUES,
        DAILY_CLOSE_REGIMES,
        DAILY_ALIGNMENT_REGIMES,
    )
)

TOTAL_CORE_TESTS = len(
    CORE_CONFIGS
)


# ============================================================
# CONTROLLED SIDECARS
# ============================================================

STRONG_CLOSE_VALUES = [
    None, 0.60, 0.65, 0.70, 0.75,
]

BODY_RATIO_VALUES = [
    1.00, 1.20, 1.40, 1.60,
]

SESSION_OPTIONS = [
    (
        "ALL",
        False,
    ),
    (
        "EXCL_NY_14_19",
        True,
    ),
]

ANCHORS = [
    {
        "name": "A",
        "structure_lookback": 50,
        "maximum_distance_atr": 0.05,
        "minimum_body_atr": None,
        "minimum_range_atr": None,
        "daily_close_regime": None,
        "alignment": None,
    },
    {
        "name": "B",
        "structure_lookback": 60,
        "maximum_distance_atr": 0.05,
        "minimum_body_atr": 0.80,
        "minimum_range_atr": 1.10,
        "daily_close_regime": None,
        "alignment": None,
    },
    {
        "name": "C",
        "structure_lookback": 70,
        "maximum_distance_atr": 0.10,
        "minimum_body_atr": 1.00,
        "minimum_range_atr": 1.20,
        "daily_close_regime": 70,
        "alignment": (50, 70),
    },
    {
        "name": "D",
        "structure_lookback": 80,
        "maximum_distance_atr": 0.10,
        "minimum_body_atr": 1.00,
        "minimum_range_atr": 1.20,
        "daily_close_regime": None,
        "alignment": None,
    },
    {
        "name": "E",
        "structure_lookback": 80,
        "maximum_distance_atr": 0.05,
        "minimum_body_atr": 1.20,
        "minimum_range_atr": 1.30,
        "daily_close_regime": 100,
        "alignment": (30, 70),
    },
]


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
        "GBP/USD Long Core Interaction Matrix"
    ),
    "instrument": INSTRUMENT,
    "core_tests": TOTAL_CORE_TESTS,
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
        "dailyAlignment":
            DAILY_ALIGNMENT_HOUR,
        "alignmentTimezone":
            DAILY_ALIGNMENT_TIMEZONE,
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

def true_ranges(
    candles,
):
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

    ema_lengths = sorted(
        set(
            [
                20, 30, 50, 70,
                100, 150,
            ]
        )
    )

    ema_map = {
        length:
            ema_series(
                closes,
                length,
            )
        for length
        in ema_lengths
    }

    rows = []

    for index, candle in enumerate(
        daily
    ):
        rows.append({
            "time":
                candle["time"],
            "close":
                candle["close"],
            "emas": {
                length:
                    ema_map[
                        length
                    ][
                        index
                    ]
                for length
                in ema_lengths
            },
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
# RAW CANDIDATES
# ============================================================

MAX_STRUCTURE_LOOKBACK = max(
    STRUCTURE_LOOKBACK_VALUES
)


def build_raw_candidates(
    h1,
    atr,
    daily_state,
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
            body_ratio
            < RAW_MINIMUM_BODY_RATIO
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

        range_atr = (
            signal_range
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

        daily = (
            previous_completed_daily(
                signal["time"],
                daily_state,
            )
        )

        ny = (
            signal["time"]
            .astimezone(
                NY_TZ
            )
        )

        candidates.append({
            "index":
                index,
            "time":
                signal["time"],
            "body_ratio":
                body_ratio,
            "close_location":
                close_location,
            "body_atr":
                body_atr,
            "range_atr":
                range_atr,
            "structure_distances":
                structure_distances,
            "daily":
                daily,
            "ny_hour":
                ny.hour,
            "ny_weekday":
                ny.weekday(),
        })

    return candidates


# ============================================================
# FILTERS
# ============================================================

def passes_core(
    signal,
    structure_lookback,
    maximum_distance_atr,
    minimum_body_atr,
    minimum_range_atr,
    daily_close_regime,
    alignment,
):
    if (
        signal[
            "structure_distances"
        ][
            structure_lookback
        ] > maximum_distance_atr
    ):
        return False

    if (
        minimum_body_atr
        is not None
        and signal[
            "body_atr"
        ] < minimum_body_atr
    ):
        return False

    if (
        minimum_range_atr
        is not None
        and signal[
            "range_atr"
        ] < minimum_range_atr
    ):
        return False

    if (
        daily_close_regime is None
        and alignment is None
    ):
        return True

    daily = signal[
        "daily"
    ]

    if daily is None:
        return False

    if (
        daily_close_regime
        is not None
    ):
        ema = (
            daily[
                "emas"
            ].get(
                daily_close_regime
            )
        )

        if (
            ema is None
            or not (
                daily["close"]
                > ema
            )
        ):
            return False

    if alignment is not None:
        (
            fast_length,
            slow_length,
        ) = alignment

        fast = (
            daily[
                "emas"
            ].get(
                fast_length
            )
        )

        slow = (
            daily[
                "emas"
            ].get(
                slow_length
            )
        )

        if (
            fast is None
            or slow is None
            or not (
                fast > slow
            )
        ):
            return False

    return True


def passes_current_control(
    signal,
):
    if (
        signal["body_ratio"]
        < 1.40
    ):
        return False

    if (
        signal["close_location"]
        < 0.65
    ):
        return False

    if (
        signal["body_atr"]
        < 0.0
    ):
        return False

    if (
        signal["range_atr"]
        < 0.90
    ):
        return False

    # Current live uses lookback20, which is not in core grid.
    # Recompute from available features is not possible here,
    # so current control is handled separately below using
    # direct H1 feature construction in a dedicated helper.
    return True


def passes_sidecar(
    signal,
    anchor,
    strong_close,
    body_ratio,
    exclude_ny_14_19,
):
    if not passes_core(
        signal,
        anchor[
            "structure_lookback"
        ],
        anchor[
            "maximum_distance_atr"
        ],
        anchor[
            "minimum_body_atr"
        ],
        anchor[
            "minimum_range_atr"
        ],
        anchor[
            "daily_close_regime"
        ],
        anchor[
            "alignment"
        ],
    ):
        return False

    if (
        strong_close is not None
        and signal[
            "close_location"
        ] < strong_close
    ):
        return False

    if (
        signal[
            "body_ratio"
        ] < body_ratio
    ):
        return False

    if (
        exclude_ny_14_19
        and signal[
            "ny_hour"
        ] >= 14
        and signal[
            "ny_hour"
        ] < 19
    ):
        return False

    return True


# ============================================================
# CURRENT LIVE FEATURE BUILD
# ============================================================

def build_current_live_signals(
    h1,
    atr,
    daily_state,
):
    eligible = []

    for index in range(
        max(
            ATR_LENGTH,
            20,
        ),
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

        if not (
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
        ):
            continue

        body_ratio = (
            current_body
            / previous_body
        )

        if (
            body_ratio < 1.40
        ):
            continue

        close_location = (
            signal["close"]
            - signal["low"]
        ) / signal_range

        if (
            close_location < 0.65
        ):
            continue

        range_atr = (
            signal_range
            / current_atr
        )

        if (
            range_atr < 0.90
        ):
            continue

        previous_lowest = min(
            candle["low"]
            for candle
            in h1[
                index - 20:
                index
            ]
        )

        structure_distance = (
            signal["low"]
            - previous_lowest
        ) / current_atr

        if (
            structure_distance > 0.25
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

        ema50 = (
            daily[
                "emas"
            ].get(50)
        )

        ema70 = (
            daily[
                "emas"
            ].get(70)
        )

        if (
            ema50 is None
            or ema70 is None
        ):
            continue

        if not (
            daily["close"]
            > ema70
        ):
            continue

        if not (
            ema50 > ema70
        ):
            continue

        ny = (
            signal["time"]
            .astimezone(
                NY_TZ
            )
        )

        if (
            ny.hour >= 14
            and ny.hour < 19
        ):
            continue

        eligible.append({
            "index": index,
            "time": signal["time"],
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
            year=
                dt.year
                - years
        )
    except ValueError:
        return dt.replace(
            month=2,
            day=28,
            year=
                dt.year
                - years,
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
    row_type,
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
        "type":
            row_type,
        "label":
            label,
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
            full["longest_loss_streak"],
        "annual_r_linear":
            round(
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
        ] = stats["trades"]

        row[
            f"{era_name}_pf"
        ] = stats["profit_factor"]

        row[
            f"{era_name}_r"
        ] = stats["total_r"]

        row[
            f"{era_name}_expectancy"
        ] = stats["expectancy_r"]

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
        ] = stats["profit_factor"]

        row[
            f"last_{years_back}y_r"
        ] = stats["total_r"]

        row[
            f"last_{years_back}y_expectancy"
        ] = stats["expectancy_r"]

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
                "Fetching GBP/USD daily history",
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
                "Precomputing GBP/USD features",
        })

        atr = atr_series(
            h1,
            ATR_LENGTH,
        )

        daily_state = (
            prepare_daily(
                daily
            )
        )

        raw_candidates = (
            build_raw_candidates(
                h1,
                atr,
                daily_state,
            )
        )

        STATUS[
            "raw_candidates"
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

        core_rows = []
        sidecar_rows = []

        # CURRENT LIVE REFERENCE
        current_live = (
            build_current_live_signals(
                h1,
                atr,
                daily_state,
            )
        )

        (
            live_trades,
            live_ignored,
        ) = simulate_variant(
            h1,
            current_live,
        )

        reference_row = (
            make_result_row(
                "REFERENCE",
                "CURRENT_LIVE_CONTROL",
                current_live,
                live_trades,
                live_ignored,
                years,
                {
                    "structure_lookback":
                        20,
                    "maximum_distance_atr":
                        0.25,
                    "minimum_body_atr":
                        None,
                    "minimum_range_atr":
                        0.90,
                    "daily_close_regime":
                        70,
                    "alignment_fast":
                        50,
                    "alignment_slow":
                        70,
                    "strong_close":
                        0.65,
                    "body_ratio":
                        1.40,
                    "session":
                        "EXCL_NY_14_19",
                },
            )
        )

        core_rows.append(
            reference_row
        )

        sidecar_rows.append(
            reference_row.copy()
        )

        # CORE MATRIX
        STATUS.update({
            "state":
                "running_core",
            "message":
                f"Running {TOTAL_CORE_TESTS} core tests",
            "completed_core_tests":
                0,
        })

        for test_number, config in enumerate(
            CORE_CONFIGS,
            start=1,
        ):
            (
                structure_lookback,
                maximum_distance_atr,
                minimum_body_atr,
                minimum_range_atr,
                daily_close_regime,
                alignment,
            ) = config

            eligible = [
                signal
                for signal
                in raw_candidates
                if passes_core(
                    signal,
                    structure_lookback,
                    maximum_distance_atr,
                    minimum_body_atr,
                    minimum_range_atr,
                    daily_close_regime,
                    alignment,
                )
            ]

            (
                trades,
                ignored,
            ) = simulate_variant(
                h1,
                eligible,
            )

            fast = None
            slow = None

            if alignment is not None:
                fast = alignment[0]
                slow = alignment[1]

            core_rows.append(
                make_result_row(
                    "CORE",
                    (
                        f"S{structure_lookback}_"
                        f"D{maximum_distance_atr:.2f}_"
                        f"B{minimum_body_atr}_"
                        f"R{minimum_range_atr}_"
                        f"CLOSEEMA{daily_close_regime}_"
                        f"ALIGN{fast}_{slow}"
                    ),
                    eligible,
                    trades,
                    ignored,
                    years,
                    {
                        "structure_lookback":
                            structure_lookback,
                        "maximum_distance_atr":
                            maximum_distance_atr,
                        "minimum_body_atr":
                            minimum_body_atr,
                        "minimum_range_atr":
                            minimum_range_atr,
                        "daily_close_regime":
                            daily_close_regime,
                        "alignment_fast":
                            fast,
                        "alignment_slow":
                            slow,
                        "strong_close":
                            None,
                        "body_ratio":
                            1.00,
                        "session":
                            "ALL",
                    },
                )
            )

            STATUS[
                "completed_core_tests"
            ] = test_number

            if (
                test_number % 500 == 0
                or test_number
                == TOTAL_CORE_TESTS
            ):
                print(
                    f"Core "
                    f"{test_number}/"
                    f"{TOTAL_CORE_TESTS}",
                    flush=True,
                )

        # SIDECARS
        sidecar_total = (
            len(ANCHORS)
            * len(STRONG_CLOSE_VALUES)
            * len(BODY_RATIO_VALUES)
            * len(SESSION_OPTIONS)
        )

        STATUS.update({
            "state":
                "running_sidecars",
            "message":
                f"Running {sidecar_total} controlled sidecars",
            "completed_sidecars":
                0,
        })

        sidecar_count = 0

        for anchor in ANCHORS:
            for strong_close in (
                STRONG_CLOSE_VALUES
            ):
                for body_ratio in (
                    BODY_RATIO_VALUES
                ):
                    for (
                        session_name,
                        exclude_ny_14_19,
                    ) in SESSION_OPTIONS:

                        eligible = [
                            signal
                            for signal
                            in raw_candidates
                            if passes_sidecar(
                                signal,
                                anchor,
                                strong_close,
                                body_ratio,
                                exclude_ny_14_19,
                            )
                        ]

                        (
                            trades,
                            ignored,
                        ) = simulate_variant(
                            h1,
                            eligible,
                        )

                        alignment = (
                            anchor[
                                "alignment"
                            ]
                        )

                        fast = None
                        slow = None

                        if (
                            alignment
                            is not None
                        ):
                            fast = (
                                alignment[0]
                            )
                            slow = (
                                alignment[1]
                            )

                        sidecar_rows.append(
                            make_result_row(
                                "SIDECAR",
                                (
                                    f"{anchor['name']}_"
                                    f"SC{strong_close}_"
                                    f"BR{body_ratio}_"
                                    f"{session_name}"
                                ),
                                eligible,
                                trades,
                                ignored,
                                years,
                                {
                                    "anchor":
                                        anchor[
                                            "name"
                                        ],
                                    "structure_lookback":
                                        anchor[
                                            "structure_lookback"
                                        ],
                                    "maximum_distance_atr":
                                        anchor[
                                            "maximum_distance_atr"
                                        ],
                                    "minimum_body_atr":
                                        anchor[
                                            "minimum_body_atr"
                                        ],
                                    "minimum_range_atr":
                                        anchor[
                                            "minimum_range_atr"
                                        ],
                                    "daily_close_regime":
                                        anchor[
                                            "daily_close_regime"
                                        ],
                                    "alignment_fast":
                                        fast,
                                    "alignment_slow":
                                        slow,
                                    "strong_close":
                                        strong_close,
                                    "body_ratio":
                                        body_ratio,
                                    "session":
                                        session_name,
                                },
                            )
                        )

                        sidecar_count += 1

                        STATUS[
                            "completed_sidecars"
                        ] = sidecar_count

        # SAVE
        core_df = pd.DataFrame(
            core_rows
        )

        reference = core_df[
            core_df[
                "type"
            ] == "REFERENCE"
        ]

        research = (
            core_df[
                core_df[
                    "type"
                ] == "CORE"
            ]
            .copy()
            .sort_values(
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
            )
        )

        core_df = pd.concat(
            [
                reference,
                research,
            ],
            ignore_index=True,
        )

        sidecar_df = pd.DataFrame(
            sidecar_rows
        )

        sidecar_reference = (
            sidecar_df[
                sidecar_df[
                    "type"
                ] == "REFERENCE"
            ]
        )

        sidecar_research = (
            sidecar_df[
                sidecar_df[
                    "type"
                ] == "SIDECAR"
            ]
            .copy()
            .sort_values(
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
            )
        )

        sidecar_df = pd.concat(
            [
                sidecar_reference,
                sidecar_research,
            ],
            ignore_index=True,
        )

        core_df.to_csv(
            os.path.abspath(
                OUTPUT_CORE
            ),
            index=False,
        )

        sidecar_df.to_csv(
            os.path.abspath(
                OUTPUT_SIDECARS
            ),
            index=False,
        )

        STATUS.update({
            "state":
                "complete",
            "message":
                "GBP/USD core interaction matrix complete",
            "core_rows_saved":
                len(core_df),
            "sidecar_rows_saved":
                len(sidecar_df),
            "outputs": {
                "core":
                    OUTPUT_CORE,
                "sidecars":
                    OUTPUT_SIDECARS,
            },
        })

        print()
        print("=" * 95)
        print(
            "GBP/USD LONG CORE INTERACTION MATRIX COMPLETE"
        )
        print("=" * 95)
        print(
            f"Core rows: "
            f"{len(core_df)}"
        )
        print(
            f"Sidecar rows: "
            f"{len(sidecar_df)}"
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
            "GBP/USD Long Core Interaction Matrix",
        "status":
            STATUS,
        "mode":
            "READ_ONLY_RESEARCH",
        "orders_supported":
            False,
        "trading_enabled":
            False,
        "downloads": {
            "core":
                "/download/core",
            "sidecars":
                "/download/sidecars",
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
    "/download/core"
)
def download_core():
    return send_output(
        OUTPUT_CORE
    )


@app.route(
    "/download/sidecars"
)
def download_sidecars():
    return send_output(
        OUTPUT_SIDECARS
    )


if __name__ == "__main__":
    thread = threading.Thread(
        target=run_research,
        name=(
            "gbpusd-long-core-interaction-matrix"
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
