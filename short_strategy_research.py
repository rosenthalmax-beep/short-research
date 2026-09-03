import os
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# EUR/USD LONG - SINGLE FACTOR DISCOVERY
#
# RESEARCH ONLY - NEVER SUBMITS ORDERS.
#
# PURPOSE
# ------------------------------------------------------------
# Start from the RAW bullish engulfing setup:
#
#   previous bearish
#   current bullish
#   body engulfing
#   minimum body ratio >= 1.00
#
# Keep RR fixed at 3.50.
#
# Then test ONE filter family at a time:
#
# - current live control
# - body ratio
# - strong close
# - lower wick
# - upper wick
# - body / ATR
# - range / ATR
# - stop size / ATR
# - structure grids
# - 6h / 12h / 24h / 48h momentum
# - previous daily close > EMA
# - previous daily EMA alignment
# - daily ATR ratio
# - single New York hour exclusions
# - single weekday exclusions
#
# This is discovery only. Do not freeze a strategy from this
# output. We are looking for broad independent clues that can
# be taken into the interaction stage.
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
# ATR14 = Wilder/RMA, SMA-seeded.
#
# Tick size EUR/USD = 0.00001.
#
# Reference entry = signal close.
# Historical adverse long fill =
#       signal close + 5 ticks.
#
# Stop =
#       signal low - 10 ticks.
#
# Target based on REFERENCE signal close:
#       target = signal close
#              + (signal close - stop) * 3.50
#
# Actual R uses adverse fill.
#
# Pyramiding = 0.
#
# Same-bar tie for LONG:
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
#   2002-05-06 20:00 UTC
#   -> current completed UTC hour.
#
# ============================================================


app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_URL = "https://api-fxtrade.oanda.com"

INSTRUMENT = "EUR_USD"

TICK_SIZE = 0.00001
STOP_BUFFER_TICKS = 10
BACKTEST_SLIPPAGE_TICKS = 5
REWARD_RISK = 3.50

ATR_LENGTH = 14
RAW_MINIMUM_BODY_RATIO = 1.00

NY_TZ = ZoneInfo(
    "America/New_York"
)

DAILY_ALIGNMENT_HOUR = 17
DAILY_ALIGNMENT_TIMEZONE = (
    "America/New_York"
)

RESEARCH_FROM = datetime(
    2002,
    5,
    6,
    20,
    0,
    tzinfo=timezone.utc,
)

RESEARCH_TO = (
    datetime.now(
        timezone.utc
    )
    .replace(
        minute=0,
        second=0,
        microsecond=0,
    )
)

H1_CHUNK_DAYS = 180
D_CHUNK_DAYS = 1500

H1_WARMUP_DAYS = 220
D_WARMUP_DAYS = 2500

OUTPUT_FILE = (
    "eurusd_long_single_factor_edges.csv"
)


# ============================================================
# TEST GRIDS
# ============================================================

BODY_RATIO_VALUES = [
    1.00,
    1.05,
    1.10,
    1.20,
    1.30,
    1.40,
    1.50,
]

CLOSE_LOCATION_VALUES = [
    0.55,
    0.60,
    0.65,
    0.70,
    0.75,
    0.80,
    0.85,
]

LOWER_WICK_BODY_VALUES = [
    0.10,
    0.20,
    0.30,
    0.40,
    0.50,
]

UPPER_WICK_BODY_MAX_VALUES = [
    0.50,
    0.40,
    0.30,
    0.20,
    0.10,
]

BODY_ATR_VALUES = [
    0.40,
    0.50,
    0.60,
    0.70,
    0.80,
    1.00,
    1.20,
]

RANGE_ATR_VALUES = [
    0.70,
    0.80,
    0.90,
    1.00,
    1.10,
    1.20,
    1.30,
    1.40,
    1.50,
]

MAX_STOP_ATR_VALUES = [
    0.80,
    1.00,
    1.20,
    1.40,
    1.60,
    1.80,
    2.00,
    2.50,
]

STRUCTURE_LOOKBACK_VALUES = [
    10,
    15,
    20,
    30,
    40,
    50,
    60,
    80,
]

STRUCTURE_DISTANCE_VALUES = [
    0.00,
    0.05,
    0.10,
    0.15,
    0.20,
    0.25,
    0.35,
    0.50,
]

MOMENTUM_LOOKBACKS = [
    6,
    12,
    24,
    48,
]

MOMENTUM_THRESHOLDS = [
    0.00,
    0.25,
    0.50,
    0.75,
    1.00,
]

DAILY_CLOSE_EMAS = [
    50,
    75,
    100,
    125,
    150,
    175,
    187,
    200,
    225,
    250,
    300,
]

DAILY_ALIGNMENT_PAIRS = [
    (10, 100),
    (20, 100),
    (30, 100),
    (20, 150),
    (30, 150),
    (50, 150),
    (20, 187),
    (30, 187),
    (50, 187),
    (30, 200),
    (50, 200),
]

DAILY_ATR_RATIO_VALUES = [
    0.70,
    0.80,
    0.90,
    1.00,
    1.10,
    1.20,
]

NY_HOURS = list(
    range(
        24
    )
)

WEEKDAYS = [
    0,
    1,
    2,
    3,
    4,
]


# ============================================================
# ERAS
# ============================================================

ERAS = [
    (
        "2002_2009",
        RESEARCH_FROM,
        datetime(
            2010,
            1,
            1,
            tzinfo=timezone.utc,
        ),
    ),
    (
        "2010_2017",
        datetime(
            2010,
            1,
            1,
            tzinfo=timezone.utc,
        ),
        datetime(
            2018,
            1,
            1,
            tzinfo=timezone.utc,
        ),
    ),
    (
        "2018_2023",
        datetime(
            2018,
            1,
            1,
            tzinfo=timezone.utc,
        ),
        datetime(
            2024,
            1,
            1,
            tzinfo=timezone.utc,
        ),
    ),
    (
        "2024_present",
        datetime(
            2024,
            1,
            1,
            tzinfo=timezone.utc,
        ),
        None,
    ),
]


STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "service": (
        "EUR/USD Long Single Factor Discovery"
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
        .astimezone(
            timezone.utc
        )
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
            ),
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
            iso_utc(
                start
            ),
        "to":
            iso_utc(
                end
            ),
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


def sma_series(
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

        result[
            index
        ] = (
            running
            / length
        )

    return result


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
        ny_time
        .replace(
            hour=
                DAILY_ALIGNMENT_HOUR,
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

    daily_atr = atr_series(
        daily,
        ATR_LENGTH,
    )

    daily_atr_for_sma = [
        (
            value
            if value is not None
            else 0.0
        )
        for value
        in daily_atr
    ]

    daily_atr_sma50_raw = (
        sma_series(
            daily_atr_for_sma,
            50,
        )
    )

    # Do not treat periods before ATR itself is ready
    # as valid daily ATR-ratio history.
    daily_atr_sma50 = [
        (
            daily_atr_sma50_raw[index]
            if (
                index >= (
                    ATR_LENGTH - 1
                    + 50 - 1
                )
            )
            else None
        )
        for index
        in range(
            len(
                daily
            )
        )
    ]

    ema_lengths = sorted(
        set(
            DAILY_CLOSE_EMAS
            + [
                fast
                for (
                    fast,
                    slow
                )
                in DAILY_ALIGNMENT_PAIRS
            ]
            + [
                slow
                for (
                    fast,
                    slow
                )
                in DAILY_ALIGNMENT_PAIRS
            ]
            + [
                30,
                187,
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
            "daily_atr":
                daily_atr[
                    index
                ],
            "daily_atr_sma50":
                daily_atr_sma50[
                    index
                ],
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

MAX_MOMENTUM_LOOKBACK = max(
    MOMENTUM_LOOKBACKS
)


def build_raw_candidates(
    h1,
    atr,
    h1_atr_sma50,
    daily_state,
):
    candidates = []

    start_index = max(
        ATR_LENGTH,
        MAX_STRUCTURE_LOOKBACK,
        MAX_MOMENTUM_LOOKBACK,
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
            < RAW_MINIMUM_BODY_RATIO
        ):
            continue

        lower_wick = (
            min(
                signal[
                    "open"
                ],
                signal[
                    "close"
                ],
            )
            - signal[
                "low"
            ]
        )

        upper_wick = (
            signal[
                "high"
            ]
            - max(
                signal[
                    "open"
                ],
                signal[
                    "close"
                ],
            )
        )

        close_location = (
            signal[
                "close"
            ]
            - signal[
                "low"
            ]
        ) / signal_range

        body_atr = (
            current_body
            / current_atr
        )

        range_atr = (
            signal_range
            / current_atr
        )

        reference_entry = (
            signal[
                "close"
            ]
        )

        stop = (
            signal[
                "low"
            ]
            - STOP_BUFFER_TICKS
            * TICK_SIZE
        )

        stop_size_atr = (
            (
                reference_entry
                - stop
            )
            / current_atr
        )

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

        momentum = {}

        for lookback in (
            MOMENTUM_LOOKBACKS
        ):
            momentum[
                lookback
            ] = (
                signal[
                    "close"
                ]
                - h1[
                    index - lookback
                ][
                    "close"
                ]
            ) / current_atr

        daily = (
            previous_completed_daily(
                signal[
                    "time"
                ],
                daily_state,
            )
        )

        ny = (
            signal[
                "time"
            ]
            .astimezone(
                NY_TZ
            )
        )

        h1_atr_ratio_50 = None

        if (
            h1_atr_sma50[
                index
            ] is not None
            and h1_atr_sma50[
                index
            ] > 0
        ):
            h1_atr_ratio_50 = (
                current_atr
                / h1_atr_sma50[
                    index
                ]
            )

        candidates.append({
            "index":
                index,
            "time":
                signal[
                    "time"
                ],
            "body_ratio":
                body_ratio,
            "close_location":
                close_location,
            "lower_wick_body":
                (
                    lower_wick
                    / current_body
                ),
            "upper_wick_body":
                (
                    upper_wick
                    / current_body
                ),
            "body_atr":
                body_atr,
            "range_atr":
                range_atr,
            "stop_size_atr":
                stop_size_atr,
            "structure_distances":
                structure_distances,
            "momentum":
                momentum,
            "daily":
                daily,
            "ny_hour":
                ny.hour,
            "ny_weekday":
                ny.weekday(),
            "h1_atr_ratio_50":
                h1_atr_ratio_50,
        })

    return candidates


# ============================================================
# CURRENT CONTROL
# ============================================================

def passes_current_control(
    signal,
):
    if (
        signal[
            "body_ratio"
        ] < 1.05
    ):
        return False

    if (
        signal[
            "close_location"
        ] < 0.70
    ):
        return False

    if (
        signal[
            "structure_distances"
        ][20] > 0.15
    ):
        return False

    daily = signal[
        "daily"
    ]

    if daily is None:
        return False

    ema30 = (
        daily[
            "emas"
        ].get(
            30
        )
    )

    ema187 = (
        daily[
            "emas"
        ].get(
            187
        )
    )

    if (
        ema30 is None
        or ema187 is None
    ):
        return False

    if not (
        daily[
            "close"
        ] > ema187
    ):
        return False

    if not (
        ema30 > ema187
    ):
        return False

    if not (
        signal[
            "ny_hour"
        ] >= 8
        and signal[
            "ny_hour"
        ] < 17
    ):
        return False

    if (
        signal[
            "ny_weekday"
        ]
        in {
            1,
            4,
        }
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
            stats[
                "trades"
            ] >= 5
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
    family,
    label,
    eligible,
    trades,
    ignored,
    years,
    parameters=None,
):
    parameters = (
        parameters
        or {}
    )

    full = stats_for_trades(
        trades
    )

    row = {
        "family":
            family,
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
                3,
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
# FILTER TEST RUNNER
# ============================================================

def run_variant(
    rows,
    family,
    label,
    raw_candidates,
    predicate,
    h1,
    years,
    parameters=None,
):
    eligible = [
        signal
        for signal
        in raw_candidates
        if predicate(
            signal
        )
    ]

    (
        trades,
        ignored,
    ) = simulate_variant(
        h1,
        eligible,
    )

    rows.append(
        make_result_row(
            family,
            label,
            eligible,
            trades,
            ignored,
            years,
            parameters,
        )
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
                "Fetching EUR/USD H1 history",
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
                "Fetching EUR/USD daily history",
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
                "Precomputing raw EUR/USD engulfing features",
        })

        atr = atr_series(
            h1,
            ATR_LENGTH,
        )

        atr_for_sma = [
            (
                value
                if value is not None
                else 0.0
            )
            for value
            in atr
        ]

        h1_atr_sma50_raw = (
            sma_series(
                atr_for_sma,
                50,
            )
        )

        h1_atr_sma50 = [
            (
                h1_atr_sma50_raw[
                    index
                ]
                if (
                    index >= (
                        ATR_LENGTH - 1
                        + 50 - 1
                    )
                )
                else None
            )
            for index
            in range(
                len(
                    h1
                )
            )
        ]

        daily_state = (
            prepare_daily(
                daily
            )
        )

        raw_candidates = (
            build_raw_candidates(
                h1,
                atr,
                h1_atr_sma50,
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

        # ----------------------------------------------------
        # RAW BASELINE
        # ----------------------------------------------------

        STATUS.update({
            "state":
                "running",
            "message":
                "Running raw baseline and single-factor tests",
        })

        run_variant(
            rows,
            "REFERENCE",
            "RAW_BULLISH_ENGULFING_BR100",
            raw_candidates,
            lambda s:
                True,
            h1,
            years,
            {
                "parameter":
                    "raw",
                "value":
                    1.00,
            },
        )

        # ----------------------------------------------------
        # CURRENT LIVE CONTROL
        # ----------------------------------------------------

        run_variant(
            rows,
            "REFERENCE",
            "CURRENT_LIVE_CONTROL",
            raw_candidates,
            passes_current_control,
            h1,
            years,
            {
                "parameter":
                    "current_live",
                "value":
                    None,
            },
        )

        # ----------------------------------------------------
        # BODY RATIO
        # ----------------------------------------------------

        for value in (
            BODY_RATIO_VALUES
        ):
            run_variant(
                rows,
                "BODY_RATIO",
                f"BODY_RATIO_GTE_{value:.2f}",
                raw_candidates,
                lambda s, v=value:
                    s[
                        "body_ratio"
                    ] >= v,
                h1,
                years,
                {
                    "parameter":
                        "minimum_body_ratio",
                    "value":
                        value,
                },
            )

        # ----------------------------------------------------
        # STRONG CLOSE
        # ----------------------------------------------------

        for value in (
            CLOSE_LOCATION_VALUES
        ):
            run_variant(
                rows,
                "STRONG_CLOSE",
                f"CLOSE_LOCATION_GTE_{value:.2f}",
                raw_candidates,
                lambda s, v=value:
                    s[
                        "close_location"
                    ] >= v,
                h1,
                years,
                {
                    "parameter":
                        "minimum_close_location",
                    "value":
                        value,
                },
            )

        # ----------------------------------------------------
        # LOWER WICK
        # ----------------------------------------------------

        for value in (
            LOWER_WICK_BODY_VALUES
        ):
            run_variant(
                rows,
                "LOWER_WICK",
                f"LOWER_WICK_BODY_GTE_{value:.2f}",
                raw_candidates,
                lambda s, v=value:
                    s[
                        "lower_wick_body"
                    ] >= v,
                h1,
                years,
                {
                    "parameter":
                        "minimum_lower_wick_body_ratio",
                    "value":
                        value,
                },
            )

        # ----------------------------------------------------
        # UPPER WICK
        # ----------------------------------------------------

        for value in (
            UPPER_WICK_BODY_MAX_VALUES
        ):
            run_variant(
                rows,
                "UPPER_WICK",
                f"UPPER_WICK_BODY_LTE_{value:.2f}",
                raw_candidates,
                lambda s, v=value:
                    s[
                        "upper_wick_body"
                    ] <= v,
                h1,
                years,
                {
                    "parameter":
                        "maximum_upper_wick_body_ratio",
                    "value":
                        value,
                },
            )

        # ----------------------------------------------------
        # BODY / ATR
        # ----------------------------------------------------

        for value in (
            BODY_ATR_VALUES
        ):
            run_variant(
                rows,
                "BODY_ATR",
                f"BODY_ATR_GTE_{value:.2f}",
                raw_candidates,
                lambda s, v=value:
                    s[
                        "body_atr"
                    ] >= v,
                h1,
                years,
                {
                    "parameter":
                        "minimum_body_atr",
                    "value":
                        value,
                },
            )

        # ----------------------------------------------------
        # RANGE / ATR
        # ----------------------------------------------------

        for value in (
            RANGE_ATR_VALUES
        ):
            run_variant(
                rows,
                "RANGE_ATR",
                f"RANGE_ATR_GTE_{value:.2f}",
                raw_candidates,
                lambda s, v=value:
                    s[
                        "range_atr"
                    ] >= v,
                h1,
                years,
                {
                    "parameter":
                        "minimum_range_atr",
                    "value":
                        value,
                },
            )

        # ----------------------------------------------------
        # STOP SIZE / ATR
        # ----------------------------------------------------

        for value in (
            MAX_STOP_ATR_VALUES
        ):
            run_variant(
                rows,
                "STOP_SIZE_ATR",
                f"STOP_ATR_LTE_{value:.2f}",
                raw_candidates,
                lambda s, v=value:
                    s[
                        "stop_size_atr"
                    ] <= v,
                h1,
                years,
                {
                    "parameter":
                        "maximum_stop_size_atr",
                    "value":
                        value,
                },
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
                run_variant(
                    rows,
                    "STRUCTURE",
                    (
                        f"STRUCTURE_"
                        f"{lookback}_"
                        f"{distance:.2f}"
                    ),
                    raw_candidates,
                    (
                        lambda s,
                        lb=lookback,
                        d=distance:
                            s[
                                "structure_distances"
                            ][lb] <= d
                    ),
                    h1,
                    years,
                    {
                        "parameter":
                            "structure",
                        "structure_lookback":
                            lookback,
                        "maximum_distance_atr":
                            distance,
                    },
                )

        # ----------------------------------------------------
        # MOMENTUM
        # ----------------------------------------------------

        for lookback in (
            MOMENTUM_LOOKBACKS
        ):
            for threshold in (
                MOMENTUM_THRESHOLDS
            ):
                run_variant(
                    rows,
                    f"MOMENTUM_{lookback}H",
                    (
                        f"MOMENTUM_"
                        f"{lookback}H_"
                        f"GTE_{threshold:.2f}"
                    ),
                    raw_candidates,
                    (
                        lambda s,
                        lb=lookback,
                        th=threshold:
                            s[
                                "momentum"
                            ][lb] >= th
                    ),
                    h1,
                    years,
                    {
                        "parameter":
                            f"momentum_{lookback}h_atr",
                        "value":
                            threshold,
                    },
                )

        # ----------------------------------------------------
        # DAILY CLOSE > EMA
        # ----------------------------------------------------

        for ema_length in (
            DAILY_CLOSE_EMAS
        ):
            run_variant(
                rows,
                "DAILY_CLOSE_EMA",
                (
                    f"DAILY_CLOSE_GT_"
                    f"EMA{ema_length}"
                ),
                raw_candidates,
                (
                    lambda s,
                    length=ema_length:
                        (
                            s[
                                "daily"
                            ]
                            is not None
                            and
                            s[
                                "daily"
                            ][
                                "emas"
                            ].get(
                                length
                            )
                            is not None
                            and
                            s[
                                "daily"
                            ][
                                "close"
                            ]
                            >
                            s[
                                "daily"
                            ][
                                "emas"
                            ][length]
                        )
                ),
                h1,
                years,
                {
                    "parameter":
                        "daily_close_ema",
                    "value":
                        ema_length,
                },
            )

        # ----------------------------------------------------
        # DAILY EMA ALIGNMENT
        # ----------------------------------------------------

        for (
            fast_length,
            slow_length,
        ) in DAILY_ALIGNMENT_PAIRS:
            run_variant(
                rows,
                "DAILY_ALIGNMENT",
                (
                    f"EMA{fast_length}_"
                    f"GT_EMA{slow_length}"
                ),
                raw_candidates,
                (
                    lambda s,
                    fast=fast_length,
                    slow=slow_length:
                        (
                            s[
                                "daily"
                            ]
                            is not None
                            and
                            s[
                                "daily"
                            ][
                                "emas"
                            ].get(
                                fast
                            )
                            is not None
                            and
                            s[
                                "daily"
                            ][
                                "emas"
                            ].get(
                                slow
                            )
                            is not None
                            and
                            s[
                                "daily"
                            ][
                                "emas"
                            ][fast]
                            >
                            s[
                                "daily"
                            ][
                                "emas"
                            ][slow]
                        )
                ),
                h1,
                years,
                {
                    "parameter":
                        "daily_alignment",
                    "fast_ema":
                        fast_length,
                    "slow_ema":
                        slow_length,
                },
            )

        # ----------------------------------------------------
        # DAILY ATR RATIO
        # ----------------------------------------------------

        for threshold in (
            DAILY_ATR_RATIO_VALUES
        ):
            run_variant(
                rows,
                "DAILY_ATR_RATIO",
                (
                    f"DAILY_ATR14_"
                    f"RATIO50_GTE_"
                    f"{threshold:.2f}"
                ),
                raw_candidates,
                (
                    lambda s,
                    th=threshold:
                        (
                            s[
                                "daily"
                            ]
                            is not None
                            and
                            s[
                                "daily"
                            ][
                                "daily_atr"
                            ]
                            is not None
                            and
                            s[
                                "daily"
                            ][
                                "daily_atr_sma50"
                            ]
                            is not None
                            and
                            s[
                                "daily"
                            ][
                                "daily_atr_sma50"
                            ] > 0
                            and
                            (
                                s[
                                    "daily"
                                ][
                                    "daily_atr"
                                ]
                                /
                                s[
                                    "daily"
                                ][
                                    "daily_atr_sma50"
                                ]
                            ) >= th
                        )
                ),
                h1,
                years,
                {
                    "parameter":
                        "minimum_daily_atr_ratio_50",
                    "value":
                        threshold,
                },
            )

        # ----------------------------------------------------
        # SINGLE NY HOUR EXCLUSION
        # ----------------------------------------------------

        for hour in NY_HOURS:
            run_variant(
                rows,
                "NY_HOUR_EXCLUSION",
                (
                    f"EXCLUDE_NY_HOUR_"
                    f"{hour:02d}"
                ),
                raw_candidates,
                lambda s, h=hour:
                    s[
                        "ny_hour"
                    ] != h,
                h1,
                years,
                {
                    "parameter":
                        "excluded_ny_hour",
                    "value":
                        hour,
                },
            )

        # ----------------------------------------------------
        # SINGLE WEEKDAY EXCLUSION
        # ----------------------------------------------------

        weekday_names = {
            0:
                "MON",
            1:
                "TUE",
            2:
                "WED",
            3:
                "THU",
            4:
                "FRI",
        }

        for weekday in WEEKDAYS:
            run_variant(
                rows,
                "WEEKDAY_EXCLUSION",
                (
                    f"EXCLUDE_"
                    f"{weekday_names[weekday]}"
                ),
                raw_candidates,
                lambda s, wd=weekday:
                    s[
                        "ny_weekday"
                    ] != wd,
                h1,
                years,
                {
                    "parameter":
                        "excluded_weekday",
                    "value":
                        weekday,
                },
            )

        # ----------------------------------------------------
        # OUTPUT
        # ----------------------------------------------------

        df = pd.DataFrame(
            rows
        )

        df = df.sort_values(
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
                "EUR/USD long single-factor discovery complete",
            "rows_saved":
                len(
                    df
                ),
            "raw_candidates":
                len(
                    raw_candidates
                ),
            "output_file":
                OUTPUT_FILE,
        })

        print()
        print("=" * 95)
        print(
            "EUR/USD LONG SINGLE FACTOR DISCOVERY COMPLETE"
        )
        print("=" * 95)
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
            "EUR/USD Long Single Factor Discovery",
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
            "eurusd-long-single-factor-discovery"
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
