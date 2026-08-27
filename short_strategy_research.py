import os
import itertools
import threading
import math
import requests
import pandas as pd

from flask import Flask, send_file, jsonify
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ==================================================
# FLASK
# ==================================================

app = Flask(__name__)


# ==================================================
# CONFIG
# ==================================================

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_URL = "https://api-fxtrade.oanda.com"

INSTRUMENT = "EUR_USD"

TICK_SIZE = 0.00001

NY_TZ = ZoneInfo("America/New_York")

DAILY_ALIGNMENT_HOUR = 17
DAILY_ALIGNMENT_TIMEZONE = "America/New_York"

STOP_BUFFER_TICKS = 10
BACKTEST_SLIPPAGE_TICKS = 5

H1_CHUNK_DAYS = 180

# Earliest EUR/USD H1 candle confirmed from OANDA
RESEARCH_FROM = datetime(
    2002, 5, 6, 20, 0,
    tzinfo=timezone.utc
)

# Latest completed H1 boundary
RESEARCH_TO = (
    datetime.now(timezone.utc)
    .replace(
        minute=0,
        second=0,
        microsecond=0
    )
)

H1_WARMUP_DAYS = 60
DAILY_WARMUP_DAYS = 1500

OUTPUT_FILE = (
    "eurusd_short_refinement_sweep.csv"
)


# ==================================================
# REFINEMENT GRID
#
# IMPORTANT:
# ALL HOURS
# ALL WEEKDAYS
#
# NO SESSION FILTER
# NO WEEKDAY FILTER
# NO MINIMUM RANGE FILTER
# NO UPPER-WICK FILTER
# ==================================================

BODY_RATIOS = [
    1.00,
    1.10,
    1.20,
    1.30,
    1.40
]

STRUCTURE_LOOKBACKS = [
    30,
    40,
    50,
    60
]

MAX_DISTANCE_ATR_VALUES = [
    0.05,
    0.10,
    0.15,
    0.20,
    0.25
]

REWARD_RISKS = [
    3.0,
    3.5,
    4.0,
    4.5,
    5.0
]

SLOW_EMA_LENGTHS = [
    100,
    150,
    200
]

# None = alignment disabled
FAST_EMA_LENGTHS = [
    None,
    30,
    50,
    70
]

# None = strong-close filter disabled
#
# 0.15 = close must be inside bottom 15%
# 0.25 = close must be inside bottom 25%
# 0.35 = close must be inside bottom 35%
STRONG_CLOSE_LEVELS = [
    None,
    0.15,
    0.25,
    0.35
]


# ==================================================
# TOTAL COMBINATIONS
#
# 5 x 4 x 5 x 5 x 3 x 4 x 4
# = 24,000
# ==================================================

TOTAL_COMBINATIONS = (
    len(BODY_RATIOS)
    * len(STRUCTURE_LOOKBACKS)
    * len(MAX_DISTANCE_ATR_VALUES)
    * len(REWARD_RISKS)
    * len(SLOW_EMA_LENGTHS)
    * len(FAST_EMA_LENGTHS)
    * len(STRONG_CLOSE_LEVELS)
)


# ==================================================
# STATUS
# ==================================================

RESEARCH_STATUS = {
    "state":
        "not_started",

    "message":
        "Research has not started",

    "research_from":
        RESEARCH_FROM.isoformat(),

    "research_to":
        RESEARCH_TO.isoformat(),

    "total_combinations":
        TOTAL_COMBINATIONS,

    "completed_combinations":
        0,

    "rows_saved":
        0,

    "base_signal_candidates":
        0,

    "parity_test":
        "not_started",

    "parity_cases_completed":
        0
}


# ==================================================
# OANDA
# ==================================================

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
        dt.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def oanda_get(
    path,
    params
):

    response = requests.get(
        OANDA_URL + path,
        headers=headers(),
        params=params,
        timeout=30
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
        False
    ):

        return None

    mid = raw.get(
        "mid"
    )

    if not mid:

        return None

    return {
        "time":
            datetime.fromisoformat(
                raw["time"].replace(
                    "Z",
                    "+00:00"
                )
            ),

        "open":
            float(
                mid["o"]
            ),

        "high":
            float(
                mid["h"]
            ),

        "low":
            float(
                mid["l"]
            ),

        "close":
            float(
                mid["c"]
            )
    }


def fetch_range(
    instrument,
    granularity,
    start,
    end
):

    params = {
        "price":
            "M",

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
            DAILY_ALIGNMENT_TIMEZONE
    }

    data = oanda_get(
        f"/v3/instruments/"
        f"{instrument}/candles",
        params
    )

    candles = []

    for raw in data.get(
        "candles",
        []
    ):

        candle = parse_candle(
            raw
        )

        if candle is not None:

            candles.append(
                candle
            )

    return candles


def fetch_chunked_history(
    instrument,
    granularity,
    start,
    end
):

    candles_by_time = {}

    cursor = start

    while cursor < end:

        chunk_end = min(
            cursor
            + timedelta(
                days=H1_CHUNK_DAYS
            ),
            end
        )

        print(
            f"Fetching {granularity}: "
            f"{cursor.date()} -> "
            f"{chunk_end.date()}",
            flush=True
        )

        chunk = fetch_range(
            instrument,
            granularity,
            cursor,
            chunk_end
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
        key=lambda item:
            item["time"]
    )

    return candles


# ==================================================
# INDICATORS
# ==================================================

def ema_series(
    values,
    length
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
        len(values)
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

    for index, candle in enumerate(
        candles
    ):

        if index == 0:

            value = (
                candle["high"]
                - candle["low"]
            )

        else:

            previous_close = (
                candles[
                    index - 1
                ]["close"]
            )

            value = max(
                candle["high"]
                - candle["low"],

                abs(
                    candle["high"]
                    - previous_close
                ),

                abs(
                    candle["low"]
                    - previous_close
                )
            )

        result.append(
            value
        )

    return result


def rma_series(
    values,
    length
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
        len(values)
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
    length=14
):

    return rma_series(
        true_ranges(
            candles
        ),
        length
    )


# ==================================================
# DAILY ALIGNMENT
# ==================================================

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
            hour=
                DAILY_ALIGNMENT_HOUR,
            minute=0,
            second=0,
            microsecond=0
        )
    )

    if ny_time < candidate:

        candidate -= timedelta(
            days=1
        )

    return candidate.astimezone(
        timezone.utc
    )


def build_daily_indicator_cache(
    daily
):

    closes = [
        candle["close"]
        for candle in daily
    ]

    lengths = sorted(
        set(
            SLOW_EMA_LENGTHS
            + [
                length
                for length
                in FAST_EMA_LENGTHS
                if length is not None
            ]
        )
    )

    cache = {}

    for length in lengths:

        cache[length] = ema_series(
            closes,
            length
        )

    return cache


def build_h1_daily_lookup(
    h1,
    daily,
    daily_ema_cache
):

    print(
        "Building H1 -> previous completed "
        "daily candle lookup...",
        flush=True
    )

    lookup = [
        None
    ] * len(h1)

    daily_index = -1

    for h1_index, candle in enumerate(
        h1
    ):

        session_start = (
            current_daily_start(
                candle["time"]
            )
        )

        while (
            daily_index + 1
            < len(daily)
            and
            daily[
                daily_index + 1
            ]["time"]
            < session_start
        ):

            daily_index += 1

        if daily_index < 0:

            continue

        row = {
            "close":
                daily[
                    daily_index
                ]["close"]
        }

        for length, series in (
            daily_ema_cache.items()
        ):

            row[
                f"ema_{length}"
            ] = series[
                daily_index
            ]

        lookup[
            h1_index
        ] = row

    return lookup


# ==================================================
# FAST ENGINE:
# PRECOMPUTED SIGNAL CANDIDATES
# ==================================================

def build_signal_candidates(
    h1,
    atr,
    daily_lookup
):

    print(
        "Precomputing bearish engulfing "
        "candidates...",
        flush=True
    )

    candidates = []

    minimum_index = max(
        STRUCTURE_LOOKBACKS
    )

    for index in range(
        minimum_index,
        len(h1)
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

        if current_atr is None:

            continue

        daily = daily_lookup[
            index
        ]

        if daily is None:

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
            or
            current_body <= 0
            or
            signal_range <= 0
        ):

            continue

        # Base engulfing only.
        # Ratio threshold comes later.
        bearish_engulfing = (
            previous["close"]
            > previous["open"]

            and
            signal["close"]
            < signal["open"]

            and
            signal["open"]
            >= previous["close"]

            and
            signal["close"]
            <= previous["open"]
        )

        if not bearish_engulfing:

            continue

        body_ratio = (
            current_body
            / previous_body
        )

        close_location = (
            (
                signal["close"]
                - signal["low"]
            )
            / signal_range
        )

        structure_distances = {}

        for lookback in (
            STRUCTURE_LOOKBACKS
        ):

            previous_highest = max(
                candle["high"]
                for candle in h1[
                    index - lookback:
                    index
                ]
            )

            structure_distances[
                lookback
            ] = (
                (
                    previous_highest
                    - signal["high"]
                )
                / current_atr
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

            "structure_distances":
                structure_distances,

            "daily":
                daily
        })

    return candidates


def fast_candidate_allowed(
    candidate,
    body_ratio,
    structure_lookback,
    max_distance_atr,
    slow_ema,
    fast_ema,
    strong_close
):

    if (
        candidate["body_ratio"]
        < body_ratio
    ):

        return False

    if (
        candidate[
            "structure_distances"
        ][structure_lookback]
        > max_distance_atr
    ):

        return False

    daily = candidate[
        "daily"
    ]

    slow_value = daily.get(
        f"ema_{slow_ema}"
    )

    if slow_value is None:

        return False

    # Previous completed daily close
    # must be below slow EMA.
    if not (
        daily["close"]
        < slow_value
    ):

        return False

    # Optional bearish EMA alignment
    if fast_ema is not None:

        fast_value = daily.get(
            f"ema_{fast_ema}"
        )

        if fast_value is None:

            return False

        if not (
            fast_value
            < slow_value
        ):

            return False

    # Optional strong bearish close
    if strong_close is not None:

        if (
            candidate[
                "close_location"
            ]
            > strong_close
        ):

            return False

    return True


# ==================================================
# EXIT CACHE
# ==================================================

EXIT_CACHE = {}


def calculate_trade_exit(
    h1,
    signal_index,
    reward_risk
):

    cache_key = (
        signal_index,
        reward_risk
    )

    if (
        cache_key
        in EXIT_CACHE
    ):

        return EXIT_CACHE[
            cache_key
        ]

    signal = h1[
        signal_index
    ]

    reference_entry = (
        signal["close"]
    )

    # Adverse short slippage:
    # fill 5 ticks below reference close.
    backtest_entry = (
        reference_entry
        - (
            BACKTEST_SLIPPAGE_TICKS
            * TICK_SIZE
        )
    )

    stop = (
        signal["high"]
        + (
            STOP_BUFFER_TICKS
            * TICK_SIZE
        )
    )

    reference_risk = (
        stop
        - reference_entry
    )

    if reference_risk <= 0:

        EXIT_CACHE[
            cache_key
        ] = None

        return None

    target = (
        reference_entry
        - (
            reference_risk
            * reward_risk
        )
    )

    actual_risk = (
        stop
        - backtest_entry
    )

    if actual_risk <= 0:

        EXIT_CACHE[
            cache_key
        ] = None

        return None

    # Entry occurs on signal close.
    # Therefore first possible exit is
    # the NEXT H1 candle.
    for index in range(
        signal_index + 1,
        len(h1)
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
            candle["high"]
            >= stop
        )

        target_hit = (
            candle["low"]
            <= target
        )

        if not (
            stop_hit
            or
            target_hit
        ):

            continue

        if (
            stop_hit
            and
            target_hit
        ):

            distance_to_high = abs(
                candle["high"]
                - candle["open"]
            )

            distance_to_low = abs(
                candle["open"]
                - candle["low"]
            )

            # Same intrabar approximation
            # used in previous research.
            #
            # Short:
            # high first = stop
            # low first = target
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

        result_r = (
            (
                backtest_entry
                - exit_price
            )
            / actual_risk
        )

        result = {
            "signal_index":
                signal_index,

            "exit_index":
                index,

            "signal_time":
                signal["time"],

            "exit_time":
                candle["time"],

            "exit_reason":
                exit_reason,

            "result_r":
                result_r
        }

        EXIT_CACHE[
            cache_key
        ] = result

        return result

    EXIT_CACHE[
        cache_key
    ] = None

    return None


# ==================================================
# FAST SIMULATOR
# ==================================================

def simulate_fast(
    h1,
    candidates,
    reward_risk
):

    trades = []

    position_exit_index = -1

    for candidate in candidates:

        signal_index = (
            candidate["index"]
        )

        # IMPORTANT:
        #
        # "<", not "<="
        #
        # If the existing trade exits during
        # this candle, the original simulator
        # is allowed to evaluate a fresh signal
        # at this candle's close.
        if (
            signal_index
            < position_exit_index
        ):

            continue

        trade = calculate_trade_exit(
            h1,
            signal_index,
            reward_risk
        )

        if trade is None:

            continue

        trades.append(
            trade
        )

        position_exit_index = (
            trade["exit_index"]
        )

    return trades


# ==================================================
# SLOW REFERENCE SIGNAL
#
# This deliberately calculates everything directly
# from the raw candle data.
#
# It does NOT use the precomputed candidate filter.
#
# This gives us an independent reference implementation
# for the parity test.
# ==================================================

def slow_signal_allowed(
    h1,
    atr,
    daily_lookup,
    index,
    body_ratio,
    structure_lookback,
    max_distance_atr,
    slow_ema,
    fast_ema,
    strong_close
):

    if index < max(
        14,
        structure_lookback
    ):

        return False

    signal = h1[
        index
    ]

    previous = h1[
        index - 1
    ]

    if (
        signal["time"]
        < RESEARCH_FROM
    ):

        return False

    if (
        signal["time"]
        >= RESEARCH_TO
    ):

        return False

    current_atr = atr[
        index
    ]

    if current_atr is None:

        return False

    daily = daily_lookup[
        index
    ]

    if daily is None:

        return False

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
        or
        current_body <= 0
        or
        signal_range <= 0
    ):

        return False

    # ==============================================
    # BEARISH ENGULF
    # ==============================================

    if not (
        previous["close"]
        > previous["open"]

        and
        signal["close"]
        < signal["open"]

        and
        signal["open"]
        >= previous["close"]

        and
        signal["close"]
        <= previous["open"]
    ):

        return False

    # ==============================================
    # BODY RATIO
    # ==============================================

    if not (
        current_body
        >= previous_body
        * body_ratio
    ):

        return False

    # ==============================================
    # STRUCTURE
    # ==============================================

    previous_highest = max(
        candle["high"]
        for candle in h1[
            index - structure_lookback:
            index
        ]
    )

    distance_from_high = (
        previous_highest
        - signal["high"]
    )

    if (
        distance_from_high
        > current_atr
        * max_distance_atr
    ):

        return False

    # ==============================================
    # DAILY REGIME
    # ==============================================

    slow_value = daily.get(
        f"ema_{slow_ema}"
    )

    if slow_value is None:

        return False

    if not (
        daily["close"]
        < slow_value
    ):

        return False

    # ==============================================
    # FAST EMA ALIGNMENT
    # ==============================================

    if fast_ema is not None:

        fast_value = daily.get(
            f"ema_{fast_ema}"
        )

        if fast_value is None:

            return False

        if not (
            fast_value
            < slow_value
        ):

            return False

    # ==============================================
    # STRONG BEARISH CLOSE
    # ==============================================

    if strong_close is not None:

        close_location = (
            (
                signal["close"]
                - signal["low"]
            )
            / signal_range
        )

        if (
            close_location
            > strong_close
        ):

            return False

    return True


# ==================================================
# SLOW REFERENCE SIMULATOR
# ==================================================

def simulate_slow_reference(
    h1,
    atr,
    daily_lookup,
    body_ratio,
    structure_lookback,
    max_distance_atr,
    reward_risk,
    slow_ema,
    fast_ema,
    strong_close
):

    trades = []

    open_trade = None

    start_index = max(
        14,
        structure_lookback
    )

    for index in range(
        start_index,
        len(h1)
    ):

        candle = h1[
            index
        ]

        candle_time = (
            candle["time"]
        )

        if (
            candle_time
            < RESEARCH_FROM
        ):

            continue

        if (
            candle_time
            >= RESEARCH_TO
        ):

            break

        # ==========================================
        # EXIT EXISTING POSITION FIRST
        # ==========================================

        if open_trade is not None:

            stop_hit = (
                candle["high"]
                >= open_trade[
                    "stop"
                ]
            )

            target_hit = (
                candle["low"]
                <= open_trade[
                    "target"
                ]
            )

            if (
                stop_hit
                or
                target_hit
            ):

                if (
                    stop_hit
                    and
                    target_hit
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

                        exit_price = (
                            open_trade[
                                "stop"
                            ]
                        )

                        exit_reason = (
                            "STOP"
                        )

                    else:

                        exit_price = (
                            open_trade[
                                "target"
                            ]
                        )

                        exit_reason = (
                            "TARGET"
                        )

                elif stop_hit:

                    exit_price = (
                        open_trade[
                            "stop"
                        ]
                    )

                    exit_reason = (
                        "STOP"
                    )

                else:

                    exit_price = (
                        open_trade[
                            "target"
                        ]
                    )

                    exit_reason = (
                        "TARGET"
                    )

                actual_risk = (
                    open_trade[
                        "stop"
                    ]
                    - open_trade[
                        "backtest_entry"
                    ]
                )

                result_r = (
                    (
                        open_trade[
                            "backtest_entry"
                        ]
                        - exit_price
                    )
                    / actual_risk
                )

                trades.append({
                    "signal_index":
                        open_trade[
                            "signal_index"
                        ],

                    "exit_index":
                        index,

                    "signal_time":
                        open_trade[
                            "signal_time"
                        ],

                    "exit_time":
                        candle_time,

                    "exit_reason":
                        exit_reason,

                    "result_r":
                        result_r
                })

                open_trade = None

        # ==========================================
        # PYRAMIDING = 0
        # ==========================================

        if open_trade is not None:

            continue

        # ==========================================
        # CHECK SIGNAL
        # ==========================================

        if not slow_signal_allowed(
            h1,
            atr,
            daily_lookup,
            index,
            body_ratio,
            structure_lookback,
            max_distance_atr,
            slow_ema,
            fast_ema,
            strong_close
        ):

            continue

        signal = h1[
            index
        ]

        reference_entry = (
            signal["close"]
        )

        backtest_entry = (
            reference_entry
            - (
                BACKTEST_SLIPPAGE_TICKS
                * TICK_SIZE
            )
        )

        stop = (
            signal["high"]
            + (
                STOP_BUFFER_TICKS
                * TICK_SIZE
            )
        )

        reference_risk = (
            stop
            - reference_entry
        )

        if reference_risk <= 0:

            continue

        target = (
            reference_entry
            - (
                reference_risk
                * reward_risk
            )
        )

        open_trade = {
            "signal_index":
                index,

            "signal_time":
                signal["time"],

            "backtest_entry":
                backtest_entry,

            "stop":
                stop,

            "target":
                target
        }

    return trades


# ==================================================
# PARITY TEST
# ==================================================

def trades_match(
    slow_trades,
    fast_trades
):

    if (
        len(slow_trades)
        != len(fast_trades)
    ):

        return (
            False,
            (
                f"Trade count mismatch: "
                f"slow={len(slow_trades)}, "
                f"fast={len(fast_trades)}"
            )
        )

    for number, (
        slow_trade,
        fast_trade
    ) in enumerate(
        zip(
            slow_trades,
            fast_trades
        ),
        start=1
    ):

        if (
            slow_trade[
                "signal_index"
            ]
            !=
            fast_trade[
                "signal_index"
            ]
        ):

            return (
                False,
                (
                    f"Trade {number}: "
                    f"signal index mismatch "
                    f"{slow_trade['signal_index']} "
                    f"vs "
                    f"{fast_trade['signal_index']}"
                )
            )

        if (
            slow_trade[
                "exit_index"
            ]
            !=
            fast_trade[
                "exit_index"
            ]
        ):

            return (
                False,
                (
                    f"Trade {number}: "
                    f"exit index mismatch "
                    f"{slow_trade['exit_index']} "
                    f"vs "
                    f"{fast_trade['exit_index']}"
                )
            )

        if (
            slow_trade[
                "exit_reason"
            ]
            !=
            fast_trade[
                "exit_reason"
            ]
        ):

            return (
                False,
                (
                    f"Trade {number}: "
                    f"exit reason mismatch "
                    f"{slow_trade['exit_reason']} "
                    f"vs "
                    f"{fast_trade['exit_reason']}"
                )
            )

        if not math.isclose(
            slow_trade[
                "result_r"
            ],
            fast_trade[
                "result_r"
            ],
            rel_tol=1e-12,
            abs_tol=1e-12
        ):

            return (
                False,
                (
                    f"Trade {number}: "
                    f"R mismatch "
                    f"{slow_trade['result_r']} "
                    f"vs "
                    f"{fast_trade['result_r']}"
                )
            )

    return (
        True,
        "Exact match"
    )


def run_parity_test(
    h1,
    atr,
    daily_lookup,
    all_candidates
):

    print()
    print(
        "========================================"
    )
    print(
        "RUNNING SLOW / FAST PARITY TEST"
    )
    print(
        "========================================"
    )

    RESEARCH_STATUS.update({
        "state":
            "parity_test",

        "message":
            "Validating optimised engine against slow reference",

        "parity_test":
            "running",

        "parity_cases_completed":
            0
    })

    # Deliberately varied cases:
    # loose / strict / different EMA / RR /
    # structure / strong-close settings.
    parity_cases = [

        {
            "body_ratio": 1.00,
            "structure_lookback": 30,
            "max_distance_atr": 0.25,
            "reward_risk": 3.0,
            "slow_ema": 100,
            "fast_ema": None,
            "strong_close": None
        },

        {
            "body_ratio": 1.20,
            "structure_lookback": 40,
            "max_distance_atr": 0.15,
            "reward_risk": 4.0,
            "slow_ema": 150,
            "fast_ema": 50,
            "strong_close": 0.25
        },

        {
            "body_ratio": 1.40,
            "structure_lookback": 60,
            "max_distance_atr": 0.05,
            "reward_risk": 5.0,
            "slow_ema": 200,
            "fast_ema": 70,
            "strong_close": 0.15
        },

        {
            "body_ratio": 1.10,
            "structure_lookback": 50,
            "max_distance_atr": 0.20,
            "reward_risk": 3.5,
            "slow_ema": 150,
            "fast_ema": 30,
            "strong_close": 0.35
        },

        {
            "body_ratio": 1.30,
            "structure_lookback": 40,
            "max_distance_atr": 0.15,
            "reward_risk": 4.0,
            "slow_ema": 100,
            "fast_ema": 50,
            "strong_close": 0.25
        },

        {
            "body_ratio": 1.00,
            "structure_lookback": 60,
            "max_distance_atr": 0.10,
            "reward_risk": 4.5,
            "slow_ema": 200,
            "fast_ema": None,
            "strong_close": 0.35
        },

        {
            "body_ratio": 1.40,
            "structure_lookback": 30,
            "max_distance_atr": 0.25,
            "reward_risk": 3.0,
            "slow_ema": 100,
            "fast_ema": 30,
            "strong_close": None
        },

        {
            "body_ratio": 1.20,
            "structure_lookback": 50,
            "max_distance_atr": 0.05,
            "reward_risk": 5.0,
            "slow_ema": 150,
            "fast_ema": 70,
            "strong_close": 0.15
        }
    ]

    for case_number, case in enumerate(
        parity_cases,
        start=1
    ):

        print()
        print(
            f"Parity case "
            f"{case_number}/"
            f"{len(parity_cases)}",
            flush=True
        )

        # ==========================================
        # SLOW REFERENCE
        # ==========================================

        slow_trades = (
            simulate_slow_reference(
                h1,
                atr,
                daily_lookup,

                case[
                    "body_ratio"
                ],

                case[
                    "structure_lookback"
                ],

                case[
                    "max_distance_atr"
                ],

                case[
                    "reward_risk"
                ],

                case[
                    "slow_ema"
                ],

                case[
                    "fast_ema"
                ],

                case[
                    "strong_close"
                ]
            )
        )

        # ==========================================
        # FAST ENGINE
        # ==========================================

        eligible = [
            candidate
            for candidate
            in all_candidates
            if fast_candidate_allowed(
                candidate,

                case[
                    "body_ratio"
                ],

                case[
                    "structure_lookback"
                ],

                case[
                    "max_distance_atr"
                ],

                case[
                    "slow_ema"
                ],

                case[
                    "fast_ema"
                ],

                case[
                    "strong_close"
                ]
            )
        ]

        fast_trades = simulate_fast(
            h1,
            eligible,
            case[
                "reward_risk"
            ]
        )

        match, message = trades_match(
            slow_trades,
            fast_trades
        )

        print(
            f"Slow trades: "
            f"{len(slow_trades)}"
        )

        print(
            f"Fast trades: "
            f"{len(fast_trades)}"
        )

        print(
            f"Result: {message}",
            flush=True
        )

        if not match:

            RESEARCH_STATUS.update({
                "parity_test":
                    "FAILED",

                "message":
                    (
                        f"Parity failure in "
                        f"case {case_number}: "
                        f"{message}"
                    )
            })

            raise RuntimeError(
                (
                    f"FAST ENGINE PARITY FAILED "
                    f"IN CASE {case_number}: "
                    f"{message}"
                )
            )

        RESEARCH_STATUS[
            "parity_cases_completed"
        ] = case_number

    RESEARCH_STATUS.update({
        "parity_test":
            "PASSED",

        "message":
            (
                "Slow and fast engines match. "
                "Starting refinement sweep."
            )
    })

    print()
    print(
        "========================================"
    )
    print(
        "PARITY TEST PASSED"
    )
    print(
        "========================================"
    )
    print(
        "Fast engine exactly matched "
        "slow reference on all test cases.",
        flush=True
    )


# ==================================================
# PERFORMANCE
# ==================================================

def calculate_stats(
    trades
):

    if not trades:

        return None

    results = [
        trade["result_r"]
        for trade in trades
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

    total_r = sum(
        results
    )

    gross_profit = sum(
        winners
    )

    gross_loss = abs(
        sum(
            losers
        )
    )

    profit_factor = (
        gross_profit
        / gross_loss
        if gross_loss > 0
        else float("inf")
    )

    win_rate = (
        len(winners)
        / len(results)
        * 100.0
    )

    expectancy = (
        total_r
        / len(results)
    )

    # ==============================================
    # MAX DRAWDOWN
    # ==============================================

    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0

    for result in results:

        equity += result

        peak = max(
            peak,
            equity
        )

        drawdown = (
            equity
            - peak
        )

        max_drawdown = min(
            max_drawdown,
            drawdown
        )

    # ==============================================
    # LONGEST LOSING STREAK
    # ==============================================

    longest_loss_streak = 0
    current_loss_streak = 0

    for result in results:

        if result < 0:

            current_loss_streak += 1

            longest_loss_streak = max(
                longest_loss_streak,
                current_loss_streak
            )

        else:

            current_loss_streak = 0

    years = (
        (
            RESEARCH_TO
            - RESEARCH_FROM
        ).total_seconds()
        / (
            365.2425
            * 24
            * 60
            * 60
        )
    )

    trades_per_year = (
        len(results)
        / years
    )

    return {
        "trades":
            len(results),

        "trades_per_year":
            round(
                trades_per_year,
                2
            ),

        "winners":
            len(winners),

        "losers":
            len(losers),

        "win_rate":
            round(
                win_rate,
                2
            ),

        "profit_factor":
            round(
                profit_factor,
                3
            ),

        "total_r":
            round(
                total_r,
                2
            ),

        "expectancy_r":
            round(
                expectancy,
                3
            ),

        "max_drawdown_r":
            round(
                max_drawdown,
                2
            ),

        "longest_loss_streak":
            longest_loss_streak
    }


# ==================================================
# RESEARCH
# ==================================================

def run_research():

    global RESEARCH_STATUS

    try:

        print()
        print(
            "========================================"
        )
        print(
            "EUR/USD SHORT REFINEMENT SWEEP"
        )
        print(
            "========================================"
        )
        print()

        print(
            "ALL HOURS ENABLED"
        )

        print(
            "ALL WEEKDAYS ENABLED"
        )

        print(
            "Minimum range filter: OFF"
        )

        print(
            "Upper-wick filter: OFF"
        )

        print(
            "Total combinations:",
            TOTAL_COMBINATIONS
        )

        print()

        # ==========================================
        # FETCH DATA
        # ==========================================

        RESEARCH_STATUS.update({
            "state":
                "fetching_data",

            "message":
                "Fetching full EUR/USD history"
        })

        h1 = fetch_chunked_history(
            INSTRUMENT,
            "H1",

            RESEARCH_FROM
            - timedelta(
                days=
                    H1_WARMUP_DAYS
            ),

            RESEARCH_TO
        )

        daily = fetch_chunked_history(
            INSTRUMENT,
            "D",

            RESEARCH_FROM
            - timedelta(
                days=
                    DAILY_WARMUP_DAYS
            ),

            RESEARCH_TO
        )

        print()
        print(
            "H1 candles loaded:",
            len(h1)
        )

        print(
            "Daily candles loaded:",
            len(daily)
        )

        # ==========================================
        # INDICATORS
        # ==========================================

        RESEARCH_STATUS.update({
            "state":
                "precomputing",

            "message":
                "Precomputing ATR, EMA and signal data"
        })

        print()
        print(
            "Calculating ATR14...",
            flush=True
        )

        atr = atr_series(
            h1,
            14
        )

        print(
            "Calculating daily EMAs...",
            flush=True
        )

        daily_ema_cache = (
            build_daily_indicator_cache(
                daily
            )
        )

        daily_lookup = (
            build_h1_daily_lookup(
                h1,
                daily,
                daily_ema_cache
            )
        )

        all_candidates = (
            build_signal_candidates(
                h1,
                atr,
                daily_lookup
            )
        )

        RESEARCH_STATUS[
            "base_signal_candidates"
        ] = len(
            all_candidates
        )

        print()
        print(
            "Base bearish-engulfing candidates:",
            len(
                all_candidates
            )
        )

        # ==========================================
        # PARITY CHECK
        # ==========================================

        run_parity_test(
            h1,
            atr,
            daily_lookup,
            all_candidates
        )

        # If parity fails, an exception is raised
        # above and the sweep NEVER starts.

        # ==========================================
        # GRID
        # ==========================================

        combinations = itertools.product(
            BODY_RATIOS,
            STRUCTURE_LOOKBACKS,
            MAX_DISTANCE_ATR_VALUES,
            REWARD_RISKS,
            SLOW_EMA_LENGTHS,
            FAST_EMA_LENGTHS,
            STRONG_CLOSE_LEVELS
        )

        RESEARCH_STATUS.update({
            "state":
                "running",

            "message":
                (
                    "Parity passed. "
                    "Running 24,000-combination "
                    "refinement sweep."
                ),

            "completed_combinations":
                0,

            "total_combinations":
                TOTAL_COMBINATIONS
        })

        results = []

        # ==========================================
        # SWEEP
        # ==========================================

        for number, combo in enumerate(
            combinations,
            start=1
        ):

            (
                body_ratio,
                structure_lookback,
                max_distance_atr,
                reward_risk,
                slow_ema,
                fast_ema,
                strong_close
            ) = combo

            eligible = [
                candidate
                for candidate
                in all_candidates
                if fast_candidate_allowed(
                    candidate,
                    body_ratio,
                    structure_lookback,
                    max_distance_atr,
                    slow_ema,
                    fast_ema,
                    strong_close
                )
            ]

            trades = simulate_fast(
                h1,
                eligible,
                reward_risk
            )

            stats = calculate_stats(
                trades
            )

            if stats is not None:

                row = {
                    "body_ratio":
                        body_ratio,

                    "structure_lookback":
                        structure_lookback,

                    "max_distance_atr":
                        max_distance_atr,

                    "reward_risk":
                        reward_risk,

                    "slow_ema":
                        slow_ema,

                    "fast_ema":
                        (
                            "OFF"
                            if fast_ema
                            is None
                            else fast_ema
                        ),

                    "strong_bearish_close":
                        (
                            "OFF"
                            if strong_close
                            is None
                            else strong_close
                        ),

                    "raw_signals":
                        len(eligible)
                }

                row.update(
                    stats
                )

                results.append(
                    row
                )

            RESEARCH_STATUS[
                "completed_combinations"
            ] = number

            if (
                number % 500
                == 0
            ):

                print(
                    f"Progress: "
                    f"{number}/"
                    f"{TOTAL_COMBINATIONS}",
                    flush=True
                )

        # ==========================================
        # DATAFRAME
        # ==========================================

        df = pd.DataFrame(
            results
        )

        if df.empty:

            raise RuntimeError(
                "No results generated"
            )

        # Keep EVERYTHING in the CSV.
        # We can impose trade-count thresholds
        # during analysis rather than deleting data.
        df = df.sort_values(
            by=[
                "profit_factor",
                "expectancy_r",
                "total_r",
                "trades"
            ],

            ascending=[
                False,
                False,
                False,
                False
            ]
        )

        df.to_csv(
            OUTPUT_FILE,
            index=False
        )

        # ==========================================
        # COMPLETE
        # ==========================================

        RESEARCH_STATUS.update({
            "state":
                "complete",

            "message":
                (
                    "Parity passed and refinement "
                    "sweep completed successfully."
                ),

            "completed_combinations":
                TOTAL_COMBINATIONS,

            "rows_saved":
                len(df),

            "output_file":
                OUTPUT_FILE,

            "parity_test":
                "PASSED"
        })

        # ==========================================
        # LOG RESULTS
        # ==========================================

        print()
        print(
            "========================================"
        )
        print(
            "TOP RESULTS >= 100 TRADES"
        )
        print(
            "========================================"
        )

        print(
            df[
                df["trades"]
                >= 100
            ]
            .head(30)
            .to_string(
                index=False
            )
        )

        print()
        print(
            "========================================"
        )
        print(
            "TOP RESULTS >= 75 TRADES"
        )
        print(
            "========================================"
        )

        print(
            df[
                df["trades"]
                >= 75
            ]
            .head(30)
            .to_string(
                index=False
            )
        )

        print()
        print(
            "CSV saved:"
        )

        print(
            OUTPUT_FILE,
            flush=True
        )

    except Exception as error:

        RESEARCH_STATUS.update({
            "state":
                "error",

            "message":
                str(error)
        })

        print()
        print(
            "ERROR:",
            error,
            flush=True
        )


# ==================================================
# ROUTES
# ==================================================

@app.route("/")
def home():

    return jsonify({
        "service":
            "EURUSD Short Refinement Research",

        "status":
            RESEARCH_STATUS,

        "research":
            {
                "all_hours":
                    True,

                "all_weekdays":
                    True,

                "session_filter":
                    False,

                "weekday_filter":
                    False,

                "minimum_range_filter":
                    False,

                "upper_wick_filter":
                    False,

                "total_combinations":
                    TOTAL_COMBINATIONS,

                "slow_fast_parity_required":
                    True
            },

        "trading_enabled":
            False,

        "orders_supported":
            False,

        "executor_connected":
            False,

        "status_endpoint":
            "/status",

        "download_endpoint":
            "/download"
    })


@app.route("/status")
def status():

    return jsonify(
        RESEARCH_STATUS
    )


@app.route("/download")
def download():

    if not os.path.exists(
        OUTPUT_FILE
    ):

        return jsonify({
            "status":
                "not_ready",

            "message":
                (
                    "Research CSV has not "
                    "been generated yet."
                )
        }), 404

    return send_file(
        OUTPUT_FILE,
        as_attachment=True,

        download_name=
            "eurusd_short_refinement_sweep.csv"
    )


# ==================================================
# START
# ==================================================

if __name__ == "__main__":

    research_thread = threading.Thread(
        target=run_research,
        name=
            "eurusd-short-refinement",
        daemon=True
    )

    research_thread.start()

    port = int(
        os.getenv(
            "PORT",
            5000
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False
    )
