import os
import threading
import itertools
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# EUR/GBP LONG - CORE INTERACTION MATRIX + CONDITIONAL SIDECARS
#
# RESEARCH ONLY - NEVER SUBMITS ORDERS.
#
# PURPOSE
# ------------------------------------------------------------
# The single-factor scan showed:
#
# - RAW bullish engulfing is poor.
# - Body/ATR and range/ATR are the clearest candle-quality clues.
# - Daily regime/alignment may add conditional value.
# - Structure, strong close, timing, and weekdays did not show
#   strong independent edge, so they are tested as sidecars.
#
# CORE MATRIX
# ------------------------------------------------------------
# body / ATR:
#   0.70, 0.80, 1.00, 1.20
#
# range / ATR:
#   0.90, 1.10, 1.30, 1.50
#
# previous completed daily close:
#   none / >EMA150 / >EMA175 / >EMA200 / >EMA250
#
# previous completed daily EMA alignment:
#   none / EMA20>150 / EMA30>150 / EMA50>150
#
# Total core combinations:
#   4 * 4 * 5 * 4 = 320
#
# CONDITIONAL SIDECARS
# ------------------------------------------------------------
# Applied one-at-a-time around several robust core anchors:
#
# - structure lookback/distance
# - close location
# - upper wick / body
# - 48h momentum
# - London session windows
# - Thursday exclusion
# - Friday exclusion
# - Thursday + Friday exclusion
#
# CURRENT LIVE CONTROL INCLUDED AS REFERENCE.
#
# ============================================================
# LOCKED EXECUTION CONVENTIONS
#
# OANDA midpoint H1.
#
# Bullish engulfing:
#   previous candle bearish
#   signal candle bullish
#   signal open <= previous close
#   signal close >= previous open
#
# Baseline body ratio >= 1.00.
# ATR14 = Wilder/RMA, SMA-seeded.
#
# Entry reference = signal close.
# Adverse long fill = signal close + 5 ticks.
# Stop = signal low - 10 ticks.
# Target = signal close + reference risk * 3.00.
# Actual R uses adverse fill.
#
# Pyramiding = 0.
#
# Same-bar stop + target:
#   compare candle open->high vs open->low
#   high closer => target first
#   otherwise stop first.
#
# Signal on exact exit candle is allowed.
# Exit search begins on bar AFTER signal bar.
#
# Daily:
#   OANDA dailyAlignment = 17
#   alignmentTimezone = America/New_York
#   previous completed daily candle only.
#
# Research:
#   2002-05-06 20:00 UTC -> current completed UTC hour.
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
MINIMUM_BODY_RATIO = 1.00

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
    "eurgbp_long_core_interaction_matrix.csv"
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
# CORE MATRIX
# ============================================================

BODY_ATR_VALUES = [
    0.70,
    0.80,
    1.00,
    1.20,
]

RANGE_ATR_VALUES = [
    0.90,
    1.10,
    1.30,
    1.50,
]

DAILY_CLOSE_EMA_VALUES = [
    None,
    150,
    175,
    200,
    250,
]

DAILY_ALIGNMENT_VALUES = [
    None,
    (20, 150),
    (30, 150),
    (50, 150),
]

CORE_CONFIGS = list(
    itertools.product(
        BODY_ATR_VALUES,
        RANGE_ATR_VALUES,
        DAILY_CLOSE_EMA_VALUES,
        DAILY_ALIGNMENT_VALUES,
    )
)

TOTAL_CORE_TESTS = len(
    CORE_CONFIGS
)


# ============================================================
# CONDITIONAL SIDECARS
# ============================================================

# Anchor set chosen to span the central quality/regime region
# rather than one single best row.
ANCHORS = [
    {
        "anchor": "B080_R110_NOEMA_NOALIGN",
        "minimum_body_atr": 0.80,
        "minimum_range_atr": 1.10,
        "daily_close_above_ema": None,
        "daily_alignment": None,
    },
    {
        "anchor": "B100_R110_EMA175_NOALIGN",
        "minimum_body_atr": 1.00,
        "minimum_range_atr": 1.10,
        "daily_close_above_ema": 175,
        "daily_alignment": None,
    },
    {
        "anchor": "B100_R130_EMA175_30GT150",
        "minimum_body_atr": 1.00,
        "minimum_range_atr": 1.30,
        "daily_close_above_ema": 175,
        "daily_alignment": (30, 150),
    },
    {
        "anchor": "B120_R130_EMA200_30GT150",
        "minimum_body_atr": 1.20,
        "minimum_range_atr": 1.30,
        "daily_close_above_ema": 200,
        "daily_alignment": (30, 150),
    },
    {
        "anchor": "B100_R150_EMA200_50GT150",
        "minimum_body_atr": 1.00,
        "minimum_range_atr": 1.50,
        "daily_close_above_ema": 200,
        "daily_alignment": (50, 150),
    },
]

SIDECARS = [
    {
        "family": "NONE",
        "variant": "ANCHOR_ONLY",
    },
]

# Structure overlays.
for lookback in [
    10,
    20,
    30,
    40,
    60,
]:
    for distance in [
        0.05,
        0.10,
        0.15,
        0.20,
        0.30,
    ]:
        SIDECARS.append({
            "family": "STRUCTURE",
            "variant": (
                f"{lookback}_{distance:.2f}"
            ),
            "structure_lookback": lookback,
            "maximum_distance_atr": distance,
        })

# Strong close overlays.
for value in [
    0.60,
    0.65,
    0.70,
    0.75,
    0.80,
    0.85,
    0.90,
]:
    SIDECARS.append({
        "family": "CLOSE_LOCATION",
        "variant": f">={value:.2f}",
        "minimum_close_location": value,
    })

# Upper wick caps.
for value in [
    0.10,
    0.20,
    0.30,
    0.40,
    0.50,
]:
    SIDECARS.append({
        "family": "UPPER_WICK_BODY",
        "variant": f"<={value:.2f}",
        "maximum_upper_wick_body": value,
    })

# 48h momentum.
for value in [
    0.00,
    0.25,
    0.50,
    0.75,
    1.00,
    1.50,
]:
    SIDECARS.append({
        "family": "MOMENTUM_48H",
        "variant": f">={value:.2f}",
        "minimum_momentum_48_atr": value,
    })

# London sessions.
for start_hour, end_hour in [
    (6, 17),
    (7, 17),
    (8, 17),
    (9, 17),
    (8, 16),
    (8, 18),
    (10, 17),
]:
    SIDECARS.append({
        "family": "LONDON_SESSION",
        "variant": (
            f"{start_hour:02d}_"
            f"{end_hour:02d}"
        ),
        "session_start_hour": start_hour,
        "session_end_hour": end_hour,
    })

# Weekdays.
SIDECARS.extend([
    {
        "family": "WEEKDAY_EXCLUDE",
        "variant": "exclude_Thu",
        "excluded_weekdays": {3},
    },
    {
        "family": "WEEKDAY_EXCLUDE",
        "variant": "exclude_Fri",
        "excluded_weekdays": {4},
    },
    {
        "family": "WEEKDAY_EXCLUDE",
        "variant": "exclude_Thu_Fri",
        "excluded_weekdays": {3, 4},
    },
])

TOTAL_SIDECAR_TESTS = (
    len(ANCHORS)
    * len(SIDECARS)
)

TOTAL_TESTS = (
    TOTAL_CORE_TESTS
    + TOTAL_SIDECAR_TESTS
    + 1
)


STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "service": (
        "EUR/GBP Long Core Interaction Matrix"
    ),
    "instrument": INSTRUMENT,
    "core_tests": TOTAL_CORE_TESTS,
    "sidecar_tests": TOTAL_SIDECAR_TESTS,
    "total_tests_including_control": TOTAL_TESTS,
    "completed_tests": 0,
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
        key=lambda item: item["time"]
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


def prepare_daily(daily):
    closes = [
        candle["close"]
        for candle in daily
    ]

    ema_lengths = [
        20,
        30,
        50,
        150,
        175,
        200,
        250,
    ]

    ema_map = {
        length: ema_series(
            closes,
            length,
        )
        for length in ema_lengths
    }

    rows = []

    for index, candle in enumerate(
        daily
    ):
        rows.append({
            "time": candle["time"],
            "close": candle["close"],
            "emas": {
                length: (
                    ema_map[
                        length
                    ][index]
                )
                for length in ema_lengths
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

        if (
            body_ratio
            < MINIMUM_BODY_RATIO
        ):
            continue

        body_atr = (
            current_body
            / current_atr
        )

        range_atr = (
            signal_range
            / current_atr
        )

        close_location = (
            signal["close"]
            - signal["low"]
        ) / signal_range

        upper_wick = (
            signal["high"]
            - max(
                signal["open"],
                signal["close"],
            )
        )

        upper_wick_body = (
            upper_wick
            / current_body
        )

        momentum_48 = (
            signal["close"]
            - h1[
                index - 48
            ]["close"]
        ) / current_atr

        structure_distances = {}

        for lookback in (
            STRUCTURE_LOOKBACKS
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

        daily = previous_completed_daily(
            signal["time"],
            daily_state,
        )

        london = (
            signal["time"]
            .astimezone(
                LONDON_TZ
            )
        )

        candidates.append({
            "index": index,
            "time": signal["time"],
            "body_atr": body_atr,
            "range_atr": range_atr,
            "close_location": (
                close_location
            ),
            "upper_wick_body": (
                upper_wick_body
            ),
            "momentum_48": (
                momentum_48
            ),
            "structure_distances": (
                structure_distances
            ),
            "daily": daily,
            "london_hour": (
                london.hour
            ),
            "london_weekday": (
                london.weekday()
            ),
        })

    return candidates


# ============================================================
# FILTERS
# ============================================================

def passes_core(
    signal,
    minimum_body_atr,
    minimum_range_atr,
    daily_close_above_ema,
    daily_alignment,
):
    if (
        signal[
            "body_atr"
        ] < minimum_body_atr
    ):
        return False

    if (
        signal[
            "range_atr"
        ] < minimum_range_atr
    ):
        return False

    if (
        daily_close_above_ema is not None
        or daily_alignment is not None
    ):
        daily = signal[
            "daily"
        ]

        if daily is None:
            return False

        if (
            daily_close_above_ema
            is not None
        ):
            ema = daily[
                "emas"
            ].get(
                daily_close_above_ema
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

        if daily_alignment is not None:
            (
                fast_length,
                slow_length,
            ) = daily_alignment

            fast = daily[
                "emas"
            ].get(
                fast_length
            )

            slow = daily[
                "emas"
            ].get(
                slow_length
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


def passes_anchor(
    signal,
    anchor,
):
    return passes_core(
        signal,
        anchor[
            "minimum_body_atr"
        ],
        anchor[
            "minimum_range_atr"
        ],
        anchor[
            "daily_close_above_ema"
        ],
        anchor[
            "daily_alignment"
        ],
    )


def passes_sidecar(
    signal,
    sidecar,
):
    structure_lookback = (
        sidecar.get(
            "structure_lookback"
        )
    )

    if structure_lookback is not None:
        if (
            signal[
                "structure_distances"
            ][
                structure_lookback
            ]
            > sidecar[
                "maximum_distance_atr"
            ]
        ):
            return False

    minimum_close = sidecar.get(
        "minimum_close_location"
    )

    if (
        minimum_close is not None
        and signal[
            "close_location"
        ] < minimum_close
    ):
        return False

    maximum_upper_wick = (
        sidecar.get(
            "maximum_upper_wick_body"
        )
    )

    if (
        maximum_upper_wick
        is not None
        and signal[
            "upper_wick_body"
        ] > maximum_upper_wick
    ):
        return False

    minimum_momentum = sidecar.get(
        "minimum_momentum_48_atr"
    )

    if (
        minimum_momentum is not None
        and signal[
            "momentum_48"
        ] < minimum_momentum
    ):
        return False

    session_start = sidecar.get(
        "session_start_hour"
    )

    if session_start is not None:
        if not (
            signal[
                "london_hour"
            ]
            >= session_start
            and signal[
                "london_hour"
            ]
            < sidecar[
                "session_end_hour"
            ]
        ):
            return False

    excluded_weekdays = (
        sidecar.get(
            "excluded_weekdays"
        )
    )

    if (
        excluded_weekdays is not None
        and signal[
            "london_weekday"
        ] in excluded_weekdays
    ):
        return False

    return True


def passes_current_live(
    signal,
):
    # body ratio >=1 already in raw candidate set.

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

    daily = signal["daily"]

    if daily is None:
        return False

    ema20 = daily[
        "emas"
    ].get(20)

    ema150 = daily[
        "emas"
    ].get(150)

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


# ============================================================
# EXIT CACHE / SIMULATION
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
    row_type,
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
        "type": row_type,
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

        if not h1:
            raise RuntimeError(
                "No H1 candles returned"
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
            if passes_current_live(
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
                "sidecar_family": None,
                "sidecar_variant": None,
                "minimum_body_atr": None,
                "minimum_range_atr": None,
                "daily_close_above_ema": 150,
                "daily_alignment_fast": 20,
                "daily_alignment_slow": 150,
                "strong_close": 0.75,
                "structure_lookback": 20,
                "maximum_distance_atr": 0.20,
                "london_session": "08_17",
                "excluded_weekdays": "Thu,Fri",
            },
        )

        rows.append(
            control_row
        )

        completed = 1

        # ----------------------------------------------------
        # CORE MATRIX
        # ----------------------------------------------------

        STATUS.update({
            "state": "running_core",
            "message": (
                f"Running {TOTAL_CORE_TESTS} "
                f"core combinations"
            ),
        })

        for config in CORE_CONFIGS:
            (
                minimum_body_atr,
                minimum_range_atr,
                daily_close_above_ema,
                daily_alignment,
            ) = config

            eligible = [
                signal
                for signal
                in raw_candidates
                if passes_core(
                    signal,
                    minimum_body_atr,
                    minimum_range_atr,
                    daily_close_above_ema,
                    daily_alignment,
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
                "CORE_MATRIX",
                eligible,
                trades,
                ignored,
                years,
                {
                    "anchor": None,
                    "sidecar_family": None,
                    "sidecar_variant": None,
                    "minimum_body_atr": (
                        minimum_body_atr
                    ),
                    "minimum_range_atr": (
                        minimum_range_atr
                    ),
                    "daily_close_above_ema": (
                        daily_close_above_ema
                    ),
                    "daily_alignment_fast": (
                        None
                        if daily_alignment is None
                        else daily_alignment[0]
                    ),
                    "daily_alignment_slow": (
                        None
                        if daily_alignment is None
                        else daily_alignment[1]
                    ),
                    "strong_close": None,
                    "structure_lookback": None,
                    "maximum_distance_atr": None,
                    "london_session": None,
                    "excluded_weekdays": None,
                },
            )

            rows.append(
                row
            )

            completed += 1

            STATUS[
                "completed_tests"
            ] = completed

        # ----------------------------------------------------
        # CONDITIONAL SIDECARS
        # ----------------------------------------------------

        STATUS.update({
            "state": "running_sidecars",
            "message": (
                f"Running {TOTAL_SIDECAR_TESTS} "
                f"conditional sidecars"
            ),
        })

        for anchor in ANCHORS:
            anchor_signals = [
                signal
                for signal
                in raw_candidates
                if passes_anchor(
                    signal,
                    anchor,
                )
            ]

            for sidecar in SIDECARS:
                eligible = [
                    signal
                    for signal
                    in anchor_signals
                    if passes_sidecar(
                        signal,
                        sidecar,
                    )
                ]

                (
                    trades,
                    ignored,
                ) = simulate_variant(
                    h1,
                    eligible,
                )

                daily_alignment = (
                    anchor[
                        "daily_alignment"
                    ]
                )

                parameters = {
                    "anchor": (
                        anchor[
                            "anchor"
                        ]
                    ),
                    "sidecar_family": (
                        sidecar[
                            "family"
                        ]
                    ),
                    "sidecar_variant": (
                        sidecar[
                            "variant"
                        ]
                    ),
                    "minimum_body_atr": (
                        anchor[
                            "minimum_body_atr"
                        ]
                    ),
                    "minimum_range_atr": (
                        anchor[
                            "minimum_range_atr"
                        ]
                    ),
                    "daily_close_above_ema": (
                        anchor[
                            "daily_close_above_ema"
                        ]
                    ),
                    "daily_alignment_fast": (
                        None
                        if daily_alignment is None
                        else daily_alignment[0]
                    ),
                    "daily_alignment_slow": (
                        None
                        if daily_alignment is None
                        else daily_alignment[1]
                    ),
                    "strong_close": (
                        sidecar.get(
                            "minimum_close_location"
                        )
                    ),
                    "structure_lookback": (
                        sidecar.get(
                            "structure_lookback"
                        )
                    ),
                    "maximum_distance_atr": (
                        sidecar.get(
                            "maximum_distance_atr"
                        )
                    ),
                    "london_session": (
                        None
                        if sidecar.get(
                            "session_start_hour"
                        ) is None
                        else (
                            f"{sidecar['session_start_hour']:02d}_"
                            f"{sidecar['session_end_hour']:02d}"
                        )
                    ),
                    "excluded_weekdays": (
                        None
                        if sidecar.get(
                            "excluded_weekdays"
                        ) is None
                        else ",".join(
                            str(x)
                            for x in sorted(
                                sidecar[
                                    "excluded_weekdays"
                                ]
                            )
                        )
                    ),
                    "maximum_upper_wick_body": (
                        sidecar.get(
                            "maximum_upper_wick_body"
                        )
                    ),
                    "minimum_momentum_48_atr": (
                        sidecar.get(
                            "minimum_momentum_48_atr"
                        )
                    ),
                }

                row = make_result_row(
                    "CONDITIONAL_SIDECAR",
                    eligible,
                    trades,
                    ignored,
                    years,
                    parameters,
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
                    or completed == TOTAL_TESTS
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

        core_df = df[
            df["type"]
            == "CORE_MATRIX"
        ].copy()

        sidecar_df = df[
            df["type"]
            == "CONDITIONAL_SIDECAR"
        ].copy()

        core_df = core_df.sort_values(
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

        sidecar_df = sidecar_df.sort_values(
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
                control_df,
                core_df,
                sidecar_df,
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
                "EUR/GBP long interaction matrix complete"
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
        })

        print()
        print("=" * 90)
        print(
            "EUR/GBP LONG CORE INTERACTION MATRIX COMPLETE"
        )
        print("=" * 90)
        print(
            f"Core tests: {TOTAL_CORE_TESTS}"
        )
        print(
            f"Sidecar tests: {TOTAL_SIDECAR_TESTS}"
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
            "EUR/GBP Long Core Interaction Matrix"
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
    thread = threading.Thread(
        target=run_research,
        name=(
            "eurgbp-long-core-interaction"
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
