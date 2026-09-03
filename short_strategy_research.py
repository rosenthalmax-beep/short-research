import os
import threading
import itertools
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# EUR/GBP LONG - CONTROLLED CONFIRMATION MATRIX
#
# RESEARCH ONLY - NEVER SUBMITS ORDERS.
#
# PURPOSE
# ------------------------------------------------------------
# Start from Candidate A:
#
#   body ratio >= 1.00
#   body >= 1.10 ATR14
#   range >= 1.40 ATR14
#   structure 40 bars
#   distance <= 0.05 ATR14
#   previous completed daily close > EMA200
#   previous completed daily EMA20 > EMA150
#   no strong-close filter
#   no session restriction
#   no weekday restriction
#   RR 3.00
#   stop buffer 10 ticks
#   adverse historical fill 5 ticks
#
# Controlled goal:
#   recover frequency without materially weakening robustness.
#
# We vary:
#
#   body / ATR:
#       1.00, 1.05, 1.10, 1.15
#
#   range / ATR:
#       1.20, 1.25, 1.30, 1.35, 1.40, 1.45
#
#   structure lookback:
#       30, 35, 40, 45, 50
#
#   structure distance / ATR:
#       0.04, 0.05, 0.06, 0.075, 0.10
#
#   daily close regime:
#       none, EMA175, EMA200, EMA225
#
#   EMA alignment:
#       none
#       EMA20>150
#       EMA30>150
#
# Total:
#   4 * 6 * 5 * 5 * 4 * 3 = 7,200 configs
#
# Candidate A and current live control are both included.
#
# ============================================================
# LOCKED EXECUTION CONVENTIONS
#
# OANDA midpoint H1.
#
# Bullish engulfing:
#   previous candle bearish
#   current candle bullish
#   current open <= previous close
#   current close >= previous open
#
# minimum body ratio baseline = 1.00
#
# ATR14 = Wilder/RMA, SMA-seeded.
# Tick size = 0.00001.
#
# Reference entry = signal close.
# Backtest adverse long fill = signal close + 5 ticks.
# Stop = signal low - 10 ticks.
# Target = signal close + (signal close - stop) * RR.
#
# Actual R =
#   (exit - backtest_entry)
#   /
#   (backtest_entry - stop)
#
# Pyramiding = 0.
#
# Same-bar target/stop tie:
#   compare candle open->high vs open->low
#   high closer => target first
#   otherwise stop first.
#
# New signal on exact exit candle is allowed.
# Exit checks begin on signal_index + 1.
#
# Daily candles:
#   OANDA dailyAlignment = 17
#   alignmentTimezone = America/New_York
#   previous completed daily candle only.
#
# Research:
#   2002-05-06 20:00 UTC -> current completed UTC hour.
#
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
    "eurgbp_long_controlled_confirmation_matrix.csv"
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
# CONFIRMATION GRID
# ============================================================

BODY_ATR_VALUES = [
    1.00,
    1.05,
    1.10,
    1.15,
]

RANGE_ATR_VALUES = [
    1.20,
    1.25,
    1.30,
    1.35,
    1.40,
    1.45,
]

STRUCTURE_LOOKBACK_VALUES = [
    30,
    35,
    40,
    45,
    50,
]

STRUCTURE_DISTANCE_VALUES = [
    0.04,
    0.05,
    0.06,
    0.075,
    0.10,
]

DAILY_CLOSE_REGIMES = [
    None,
    175,
    200,
    225,
]

DAILY_ALIGNMENT_REGIMES = [
    None,
    (20, 150),
    (30, 150),
]

CONFIGS = list(
    itertools.product(
        BODY_ATR_VALUES,
        RANGE_ATR_VALUES,
        STRUCTURE_LOOKBACK_VALUES,
        STRUCTURE_DISTANCE_VALUES,
        DAILY_CLOSE_REGIMES,
        DAILY_ALIGNMENT_REGIMES,
    )
)

TOTAL_MATRIX_TESTS = len(
    CONFIGS
)

TOTAL_TESTS = (
    TOTAL_MATRIX_TESTS
    + 2
)


STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "service": (
        "EUR/GBP Long Controlled Confirmation Matrix"
    ),
    "instrument": INSTRUMENT,
    "matrix_tests": TOTAL_MATRIX_TESTS,
    "total_tests_including_references": TOTAL_TESTS,
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

        result[
            index
        ] = current

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

    ema_lengths = [
        20,
        30,
        150,
        175,
        200,
        225,
    ]

    ema_map = {
        length: ema_series(
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
                candle[
                    "time"
                ],
            "close":
                candle[
                    "close"
                ],
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
            previous[
                "close"
            ]
            - previous[
                "open"
            ]
        )

        current_body = abs(
            signal[
                "close"
            ]
            - signal[
                "open"
            ]
        )

        signal_range = (
            signal[
                "high"
            ]
            - signal[
                "low"
            ]
        )

        if (
            previous_body <= 0
            or current_body <= 0
            or signal_range <= 0
        ):
            continue

        bullish_engulfing = (
            previous[
                "close"
            ]
            < previous[
                "open"
            ]
            and
            signal[
                "close"
            ]
            > signal[
                "open"
            ]
            and
            signal[
                "open"
            ]
            <= previous[
                "close"
            ]
            and
            signal[
                "close"
            ]
            >= previous[
                "open"
            ]
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
            signal[
                "close"
            ]
            - signal[
                "low"
            ]
        ) / signal_range

        structure_distances = {}

        for lookback in (
            STRUCTURE_LOOKBACK_VALUES
        ):
            previous_lowest = min(
                candle[
                    "low"
                ]
                for candle
                in h1[
                    index - lookback:
                    index
                ]
            )

            structure_distances[
                lookback
            ] = (
                signal[
                    "low"
                ]
                - previous_lowest
            ) / current_atr

        daily = (
            previous_completed_daily(
                signal[
                    "time"
                ],
                daily_state,
            )
        )

        london = (
            signal[
                "time"
            ]
            .astimezone(
                LONDON_TZ
            )
        )

        candidates.append({
            "index":
                index,
            "time":
                signal[
                    "time"
                ],
            "body_atr":
                body_atr,
            "range_atr":
                range_atr,
            "close_location":
                close_location,
            "structure_distances":
                structure_distances,
            "daily":
                daily,
            "london_hour":
                london.hour,
            "london_weekday":
                london.weekday(),
        })

    return candidates


# ============================================================
# FILTERS
# ============================================================

def passes_matrix_config(
    signal,
    minimum_body_atr,
    minimum_range_atr,
    structure_lookback,
    maximum_distance_atr,
    daily_close_regime,
    daily_alignment_regime,
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
        signal[
            "structure_distances"
        ][
            structure_lookback
        ] > maximum_distance_atr
    ):
        return False

    if (
        daily_close_regime is None
        and daily_alignment_regime is None
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
                daily[
                    "close"
                ] > ema
            )
        ):
            return False

    if (
        daily_alignment_regime
        is not None
    ):
        (
            fast_length,
            slow_length,
        ) = daily_alignment_regime

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


def passes_candidate_a(
    signal,
):
    return passes_matrix_config(
        signal,
        1.10,
        1.40,
        40,
        0.05,
        200,
        (20, 150),
    )


def passes_current_live(
    signal,
):
    if (
        signal[
            "close_location"
        ] < 0.75
    ):
        return False

    # current live uses structure 20 / 0.20,
    # so compute it directly here because matrix precompute
    # starts at 30.
    return None


def build_current_live_candidates(
    h1,
    atr,
    daily_state,
):
    eligible = []

    start_index = max(
        ATR_LENGTH,
        20,
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
            previous[
                "close"
            ]
            - previous[
                "open"
            ]
        )

        current_body = abs(
            signal[
                "close"
            ]
            - signal[
                "open"
            ]
        )

        signal_range = (
            signal[
                "high"
            ]
            - signal[
                "low"
            ]
        )

        if (
            previous_body <= 0
            or current_body <= 0
            or signal_range <= 0
        ):
            continue

        if not (
            previous[
                "close"
            ] < previous[
                "open"
            ]
            and signal[
                "close"
            ] > signal[
                "open"
            ]
            and signal[
                "open"
            ] <= previous[
                "close"
            ]
            and signal[
                "close"
            ] >= previous[
                "open"
            ]
        ):
            continue

        body_ratio = (
            current_body
            / previous_body
        )

        if (
            body_ratio
            < 1.00
        ):
            continue

        close_location = (
            signal[
                "close"
            ]
            - signal[
                "low"
            ]
        ) / signal_range

        if (
            close_location
            < 0.75
        ):
            continue

        previous_lowest = min(
            candle[
                "low"
            ]
            for candle
            in h1[
                index - 20:
                index
            ]
        )

        distance = (
            signal[
                "low"
            ]
            - previous_lowest
        ) / current_atr

        if (
            distance > 0.20
        ):
            continue

        daily = (
            previous_completed_daily(
                signal[
                    "time"
                ],
                daily_state,
            )
        )

        if daily is None:
            continue

        ema20 = (
            daily[
                "emas"
            ].get(
                20
            )
        )

        ema150 = (
            daily[
                "emas"
            ].get(
                150
            )
        )

        if (
            ema20 is None
            or ema150 is None
        ):
            continue

        if not (
            daily[
                "close"
            ] > ema150
        ):
            continue

        if not (
            ema20 > ema150
        ):
            continue

        london = (
            signal[
                "time"
            ]
            .astimezone(
                LONDON_TZ
            )
        )

        if not (
            london.hour >= 8
            and london.hour < 17
        ):
            continue

        if (
            london.weekday()
            in {
                3,
                4,
            }
        ):
            continue

        eligible.append({
            "index":
                index,
            "time":
                signal[
                    "time"
                ],
        })

    return eligible


# ============================================================
# EXIT CACHE / SIMULATION
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
        signal[
            "low"
        ]
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
        result = {
            "signal_index":
                signal_index,
            "signal_time":
                signal[
                    "time"
                ],
            "exit_index":
                None,
            "exit_time":
                None,
            "result_r":
                None,
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

    if (
        actual_risk <= 0
    ):
        result = {
            "signal_index":
                signal_index,
            "signal_time":
                signal[
                    "time"
                ],
            "exit_index":
                None,
            "exit_time":
                None,
            "result_r":
                None,
        }

        EXIT_CACHE[
            signal_index
        ] = result

        return result

    for index in range(
        signal_index + 1,
        len(h1),
    ):
        candle = h1[
            index
        ]

        if (
            candle[
                "time"
            ] >= RESEARCH_TO
        ):
            break

        stop_hit = (
            candle[
                "low"
            ] <= stop
        )

        target_hit = (
            candle[
                "high"
            ] >= target
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
                candle[
                    "high"
                ]
                - candle[
                    "open"
                ]
            )

            distance_to_low = abs(
                candle[
                    "open"
                ]
                - candle[
                    "low"
                ]
            )

            if (
                distance_to_high
                < distance_to_low
            ):
                exit_price = (
                    target
                )
            else:
                exit_price = (
                    stop
                )

        elif target_hit:
            exit_price = target

        else:
            exit_price = stop

        result = {
            "signal_index":
                signal_index,
            "signal_time":
                signal[
                    "time"
                ],
            "exit_index":
                index,
            "exit_time":
                candle[
                    "time"
                ],
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

    result = {
        "signal_index":
            signal_index,
        "signal_time":
            signal[
                "time"
            ],
        "exit_index":
            None,
        "exit_time":
            None,
        "result_r":
            None,
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
        trade[
            "result_r"
        ]
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
            len(
                results
            ),
        "winners":
            len(
                winners
            ),
        "losers":
            len(
                losers
            ),
        "win_rate":
            round(
                len(
                    winners
                )
                / len(
                    results
                )
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
                / len(
                    results
                ),
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
                "label":
                    (
                        f"{start_year}_"
                        f"{start_year + 2}"
                    ),
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
            row[
                "pf"
            ],
    )

    worst_exp = min(
        rows,
        key=lambda row:
            row[
                "expectancy"
            ],
    )

    worst_total = min(
        rows,
        key=lambda row:
            row[
                "total_r"
            ],
    )

    return {
        "worst_rolling_3y_pf":
            worst_pf[
                "pf"
            ],
        "worst_rolling_3y_pf_label":
            worst_pf[
                "label"
            ],
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
    row_type,
    label,
    eligible,
    trades,
    ignored,
    years,
    parameters,
):
    full = (
        stats_for_trades(
            trades
        )
    )

    row = {
        "type":
            row_type,
        "label":
            label,
        "eligible_signals":
            len(
                eligible
            ),
        "ignored_due_to_open_trade":
            ignored,
        "trades":
            full[
                "trades"
            ],
        "trades_per_year":
            round(
                full[
                    "trades"
                ]
                / years,
                2,
            ),
        "winners":
            full[
                "winners"
            ],
        "losers":
            full[
                "losers"
            ],
        "win_rate":
            full[
                "win_rate"
            ],
        "profit_factor":
            full[
                "profit_factor"
            ],
        "total_r":
            full[
                "total_r"
            ],
        "expectancy_r":
            full[
                "expectancy_r"
            ],
        "max_drawdown_r":
            full[
                "max_drawdown_r"
            ],
        "longest_loss_streak":
            full[
                "longest_loss_streak"
            ],
        "annual_r_linear":
            round(
                full[
                    "total_r"
                ]
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
        ] = stats[
            "trades"
        ]

        row[
            f"{era_name}_pf"
        ] = stats[
            "profit_factor"
        ]

        row[
            f"{era_name}_r"
        ] = stats[
            "total_r"
        ]

        row[
            f"{era_name}_expectancy"
        ] = stats[
            "expectancy_r"
        ]

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
    ] = minimum_era_pf

    row[
        "profitable_eras"
    ] = profitable_eras

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
        ] = stats[
            "trades"
        ]

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
                "Fetching EUR/GBP H1 history",
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
                "Fetching EUR/GBP daily history",
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
                "Precomputing EUR/GBP features",
        })

        h1_atr = atr_series(
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
                h1_atr,
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

        rows = []
        completed = 0

        # ----------------------------------------------------
        # CURRENT LIVE CONTROL
        # ----------------------------------------------------

        current_live = (
            build_current_live_candidates(
                h1,
                h1_atr,
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

        rows.append(
            make_result_row(
                "REFERENCE",
                "CURRENT_LIVE_CONTROL",
                current_live,
                live_trades,
                live_ignored,
                years,
                {
                    "minimum_body_atr":
                        None,
                    "minimum_range_atr":
                        None,
                    "structure_lookback":
                        20,
                    "maximum_distance_atr":
                        0.20,
                    "daily_close_regime":
                        150,
                    "daily_alignment_fast":
                        20,
                    "daily_alignment_slow":
                        150,
                    "strong_close":
                        0.75,
                    "session":
                        "LONDON_08_17",
                    "excluded_weekdays":
                        "Thu,Fri",
                },
            )
        )

        completed += 1

        # ----------------------------------------------------
        # CANDIDATE A
        # ----------------------------------------------------

        candidate_a = [
            signal
            for signal
            in raw_candidates
            if passes_candidate_a(
                signal
            )
        ]

        (
            candidate_a_trades,
            candidate_a_ignored,
        ) = simulate_variant(
            h1,
            candidate_a,
        )

        rows.append(
            make_result_row(
                "REFERENCE",
                "CANDIDATE_A",
                candidate_a,
                candidate_a_trades,
                candidate_a_ignored,
                years,
                {
                    "minimum_body_atr":
                        1.10,
                    "minimum_range_atr":
                        1.40,
                    "structure_lookback":
                        40,
                    "maximum_distance_atr":
                        0.05,
                    "daily_close_regime":
                        200,
                    "daily_alignment_fast":
                        20,
                    "daily_alignment_slow":
                        150,
                    "strong_close":
                        None,
                    "session":
                        "ALL",
                    "excluded_weekdays":
                        None,
                },
            )
        )

        completed += 1

        # ----------------------------------------------------
        # MATRIX
        # ----------------------------------------------------

        STATUS.update({
            "state":
                "running_matrix",
            "message":
                f"Running {TOTAL_MATRIX_TESTS} controlled confirmation tests",
        })

        for config in CONFIGS:
            (
                minimum_body_atr,
                minimum_range_atr,
                structure_lookback,
                maximum_distance_atr,
                daily_close_regime,
                daily_alignment_regime,
            ) = config

            eligible = [
                signal
                for signal
                in raw_candidates
                if passes_matrix_config(
                    signal,
                    minimum_body_atr,
                    minimum_range_atr,
                    structure_lookback,
                    maximum_distance_atr,
                    daily_close_regime,
                    daily_alignment_regime,
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

            if (
                daily_alignment_regime
                is not None
            ):
                fast = (
                    daily_alignment_regime[
                        0
                    ]
                )
                slow = (
                    daily_alignment_regime[
                        1
                    ]
                )

            rows.append(
                make_result_row(
                    "CONFIRMATION_MATRIX",
                    (
                        f"B{minimum_body_atr:.2f}_"
                        f"R{minimum_range_atr:.2f}_"
                        f"S{structure_lookback}_"
                        f"D{maximum_distance_atr:.3f}_"
                        f"CLOSEEMA{daily_close_regime}_"
                        f"ALIGN{fast}_{slow}"
                    ),
                    eligible,
                    trades,
                    ignored,
                    years,
                    {
                        "minimum_body_atr":
                            minimum_body_atr,
                        "minimum_range_atr":
                            minimum_range_atr,
                        "structure_lookback":
                            structure_lookback,
                        "maximum_distance_atr":
                            maximum_distance_atr,
                        "daily_close_regime":
                            daily_close_regime,
                        "daily_alignment_fast":
                            fast,
                        "daily_alignment_slow":
                            slow,
                        "strong_close":
                            None,
                        "session":
                            "ALL",
                        "excluded_weekdays":
                            None,
                    },
                )
            )

            completed += 1

            STATUS[
                "completed_tests"
            ] = completed

            if (
                completed % 250 == 0
                or completed == TOTAL_TESTS
            ):
                print(
                    f"{completed}/{TOTAL_TESTS}",
                    flush=True,
                )

        df = pd.DataFrame(
            rows
        )

        refs = df[
            df[
                "type"
            ] == "REFERENCE"
        ]

        research = df[
            df[
                "type"
            ] == "CONFIRMATION_MATRIX"
        ].copy()

        research = (
            research
            .sort_values(
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
                refs,
                research,
            ],
            ignore_index=True,
        )

        df.to_csv(
            OUTPUT_FILE,
            index=False,
        )

        STATUS.update({
            "state":
                "complete",
            "message":
                "EUR/GBP long controlled confirmation matrix complete",
            "completed_tests":
                TOTAL_TESTS,
            "rows_saved":
                len(
                    df
                ),
            "output_file":
                OUTPUT_FILE,
        })

        print()
        print("=" * 90)
        print(
            "EUR/GBP LONG CONTROLLED CONFIRMATION MATRIX COMPLETE"
        )
        print("=" * 90)
        print(
            f"Matrix tests: {TOTAL_MATRIX_TESTS}"
        )
        print(
            f"Rows saved: {len(df)}"
        )
        print(
            f"Output: {OUTPUT_FILE}"
        )

    except Exception as error:
        STATUS.update({
            "state":
                "error",
            "message":
                str(
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
        "service":
            "EUR/GBP Long Controlled Confirmation Matrix",
        "status":
            STATUS,
        "instrument":
            INSTRUMENT,
        "direction":
            "LONG",
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
    if not os.path.exists(
        OUTPUT_FILE
    ):
        return jsonify({
            "status":
                "not_ready",
            "message":
                "CSV is not ready yet",
        }), 404

    return send_file(
        os.path.abspath(
            OUTPUT_FILE
        ),
        as_attachment=True,
        download_name=(
            OUTPUT_FILE
        ),
    )


if __name__ == "__main__":
    thread = threading.Thread(
        target=run_research,
        name=(
            "eurgbp-long-controlled-confirmation"
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
