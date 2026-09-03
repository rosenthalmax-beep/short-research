import os
import threading
import itertools
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# EUR/GBP LONG - TIGHT ROBUSTNESS SWEEP
#
# RESEARCH ONLY - NEVER SUBMITS ORDERS.
#
# PURPOSE
# ------------------------------------------------------------
# Explore the promising branch discovered in the prior matrix:
#
#   larger bullish engulfing candles
#   stronger range
#   bullish daily regime
#   EMA alignment
#   deeper structure lookback
#
# We deliberately sweep a tight neighborhood around that branch
# to look for a BROAD ROBUST PLATEAU rather than a single peak.
#
# CORE SWEEP
# ------------------------------------------------------------
# body/ATR:
#   1.00, 1.10, 1.20, 1.30
#
# range/ATR:
#   1.10, 1.20, 1.30, 1.40, 1.50
#
# structure lookback:
#   40, 50, 60, 70, 80
#
# structure distance:
#   0.025, 0.05, 0.075, 0.10, 0.15, 0.20
#
# daily close > EMA:
#   175, 200, 225
#
# EMA alignment:
#   EMA20>150
#   EMA30>150
#   EMA40>150
#   EMA50>150
#
# Total:
#   4 * 5 * 5 * 6 * 3 * 4 = 7,200 combinations
#
# TIMING OVERLAYS
# ------------------------------------------------------------
# After the core scan, several representative robust anchors are
# re-tested with London session windows:
#
#   all day
#   06-17
#   07-17
#   08-17
#   09-17
#   10-17
#   08-16
#   08-18
#
# CURRENT LIVE CONTROL INCLUDED.
#
# ============================================================
# LOCKED EXECUTION CONVENTIONS
#
# OANDA midpoint H1 candles.
#
# Bullish engulfing:
#   previous bearish
#   current bullish
#   current open <= previous close
#   current close >= previous open
#
# Minimum body ratio fixed at 1.00.
# ATR14 = Wilder/RMA, SMA-seeded.
#
# Entry reference = signal close.
# Backtest adverse fill = signal close + 5 ticks.
# Stop = signal low - 10 ticks.
# Target = reference close + reference risk * 3.00.
# Actual R uses adverse fill.
#
# Pyramiding 0.
# Signal on exact exit candle allowed.
# Exits begin on signal_index + 1.
#
# Same-bar target/stop:
#   open->high closer => target first
#   else stop first.
#
# Daily:
#   previous completed OANDA daily candle only
#   dailyAlignment=17
#   alignmentTimezone=America/New_York
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

OUTPUT_CORE = (
    "eurgbp_long_tight_robustness_core.csv"
)

OUTPUT_TIMING = (
    "eurgbp_long_tight_robustness_timing.csv"
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
# CORE GRID
# ============================================================

BODY_ATR_VALUES = [
    1.00,
    1.10,
    1.20,
    1.30,
]

RANGE_ATR_VALUES = [
    1.10,
    1.20,
    1.30,
    1.40,
    1.50,
]

STRUCTURE_LOOKBACKS = [
    40,
    50,
    60,
    70,
    80,
]

STRUCTURE_DISTANCES = [
    0.025,
    0.05,
    0.075,
    0.10,
    0.15,
    0.20,
]

DAILY_CLOSE_EMAS = [
    175,
    200,
    225,
]

ALIGNMENTS = [
    (20, 150),
    (30, 150),
    (40, 150),
    (50, 150),
]

CORE_CONFIGS = list(
    itertools.product(
        BODY_ATR_VALUES,
        RANGE_ATR_VALUES,
        STRUCTURE_LOOKBACKS,
        STRUCTURE_DISTANCES,
        DAILY_CLOSE_EMAS,
        ALIGNMENTS,
    )
)

TOTAL_CORE = len(
    CORE_CONFIGS
)


# ============================================================
# TIMING ANCHORS
#
# Representative setups spanning the promising neighborhood.
# ============================================================

TIMING_ANCHORS = [
    {
        "anchor": "A_100_120_S60D050_E200_A30",
        "body_atr": 1.00,
        "range_atr": 1.20,
        "structure_lookback": 60,
        "structure_distance": 0.05,
        "daily_close_ema": 200,
        "alignment": (30, 150),
    },
    {
        "anchor": "B_110_120_S60D050_E200_A30",
        "body_atr": 1.10,
        "range_atr": 1.20,
        "structure_lookback": 60,
        "structure_distance": 0.05,
        "daily_close_ema": 200,
        "alignment": (30, 150),
    },
    {
        "anchor": "C_120_130_S60D050_E200_A30",
        "body_atr": 1.20,
        "range_atr": 1.30,
        "structure_lookback": 60,
        "structure_distance": 0.05,
        "daily_close_ema": 200,
        "alignment": (30, 150),
    },
    {
        "anchor": "D_110_130_S60D075_E200_A30",
        "body_atr": 1.10,
        "range_atr": 1.30,
        "structure_lookback": 60,
        "structure_distance": 0.075,
        "daily_close_ema": 200,
        "alignment": (30, 150),
    },
    {
        "anchor": "E_110_130_S70D075_E200_A40",
        "body_atr": 1.10,
        "range_atr": 1.30,
        "structure_lookback": 70,
        "structure_distance": 0.075,
        "daily_close_ema": 200,
        "alignment": (40, 150),
    },
]

TIMING_WINDOWS = [
    ("ALL", None, None),
    ("06_17", 6, 17),
    ("07_17", 7, 17),
    ("08_17", 8, 17),
    ("09_17", 9, 17),
    ("10_17", 10, 17),
    ("08_16", 8, 16),
    ("08_18", 8, 18),
]

TOTAL_TIMING = (
    len(TIMING_ANCHORS)
    * len(TIMING_WINDOWS)
)


STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "service": "EUR/GBP Long Tight Robustness Sweep",
    "instrument": INSTRUMENT,
    "core_tests": TOTAL_CORE,
    "timing_tests": TOTAL_TIMING,
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
            f"{cursor.date()} -> "
            f"{chunk_end.date()}",
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
    result = []

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

        result.append(
            tr
        )

    return result


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

        result[index] = (
            current
        )

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

        result[index] = (
            current
        )

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
        40,
        50,
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
                candle["time"],
            "close":
                candle["close"],
            "emas": {
                length:
                    ema_map[
                        length
                    ][index]
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
    STRUCTURE_LOOKBACKS
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
            "index":
                index,
            "time":
                signal["time"],
            "body_atr":
                body_atr,
            "range_atr":
                range_atr,
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

def passes_core(
    signal,
    body_atr,
    range_atr,
    structure_lookback,
    structure_distance,
    daily_close_ema,
    alignment,
):
    if (
        signal["body_atr"]
        < body_atr
    ):
        return False

    if (
        signal["range_atr"]
        < range_atr
    ):
        return False

    if (
        signal[
            "structure_distances"
        ][
            structure_lookback
        ]
        > structure_distance
    ):
        return False

    daily = signal[
        "daily"
    ]

    if daily is None:
        return False

    daily_ema = (
        daily[
            "emas"
        ].get(
            daily_close_ema
        )
    )

    if (
        daily_ema is None
        or not (
            daily[
                "close"
            ] > daily_ema
        )
    ):
        return False

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


def passes_session(
    signal,
    start_hour,
    end_hour,
):
    if start_hour is None:
        return True

    return (
        signal[
            "london_hour"
        ] >= start_hour
        and
        signal[
            "london_hour"
        ] < end_hour
    )


def passes_current_live(
    signal,
):
    # Current live strong-close and structure/session/weekday
    # require additional fields not needed by the tight sweep.
    # Recompute directly from H1 signal data in dedicated control
    # candidate construction below instead.
    raise RuntimeError(
        "Use build_current_live_eligible()"
    )


# ============================================================
# CURRENT LIVE CONTROL ELIGIBLE
# ============================================================

def build_current_live_eligible(
    h1,
    atr,
    daily_state,
):
    eligible = []

    for signal in build_raw_candidates(
        h1,
        atr,
        daily_state,
    ):
        index = signal[
            "index"
        ]

        candle = h1[
            index
        ]

        signal_range = (
            candle["high"]
            - candle["low"]
        )

        if signal_range <= 0:
            continue

        close_location = (
            candle["close"]
            - candle["low"]
        ) / signal_range

        if close_location < 0.75:
            continue

        # Current live structure 20 / 0.20
        current_atr = atr[
            index
        ]

        previous_lowest = min(
            item["low"]
            for item
            in h1[
                index - 20:
                index
            ]
        )

        distance = (
            candle["low"]
            - previous_lowest
        ) / current_atr

        if distance > 0.20:
            continue

        daily = signal[
            "daily"
        ]

        if daily is None:
            continue

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

        if not (
            signal[
                "london_hour"
            ] >= 8
            and signal[
                "london_hour"
            ] < 17
        ):
            continue

        if (
            signal[
                "london_weekday"
            ]
            in {
                3,
                4,
            }
        ):
            continue

        eligible.append(
            signal
        )

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
        signal["close"]
    )

    backtest_entry = (
        reference_entry
        + (
            BACKTEST_SLIPPAGE_TICKS
            * TICK_SIZE
        )
    )

    stop = (
        signal["low"]
        - (
            STOP_BUFFER_TICKS
            * TICK_SIZE
        )
    )

    reference_risk = (
        reference_entry
        - stop
    )

    if reference_risk <= 0:
        result = {
            "signal_index":
                signal_index,
            "signal_time":
                signal["time"],
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
        + (
            reference_risk
            * REWARD_RISK
        )
    )

    actual_risk = (
        backtest_entry
        - stop
    )

    if actual_risk <= 0:
        result = {
            "signal_index":
                signal_index,
            "signal_time":
                signal["time"],
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
                exit_price = (
                    target
                )
            else:
                exit_price = (
                    stop
                )

        elif target_hit:
            exit_price = (
                target
            )

        else:
            exit_price = (
                stop
            )

        result = {
            "signal_index":
                signal_index,
            "signal_time":
                signal["time"],
            "exit_index":
                index,
            "exit_time":
                candle["time"],
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
        "signal_index":
            signal_index,
        "signal_time":
            signal["time"],
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

        trade = calculate_trade_exit(
            h1,
            signal_index,
        )

        if (
            trade[
                "result_r"
            ]
            is None
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

    for result_r in results:
        equity += (
            result_r
        )

        peak = max(
            peak,
            equity,
        )

        max_dd = min(
            max_dd,
            equity - peak,
        )

        if result_r < 0:
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
        key=lambda row: row["pf"],
    )

    worst_exp = min(
        rows,
        key=lambda row: row[
            "expectancy"
        ],
    )

    worst_total = min(
        rows,
        key=lambda row: row[
            "total_r"
        ],
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


def make_row(
    row_type,
    eligible,
    trades,
    ignored,
    years,
    params,
):
    full = stats_for_trades(
        trades
    )

    row = {
        "type":
            row_type,
        "eligible_signals":
            len(
                eligible
            ),
        "ignored_due_to_open_trade":
            ignored,
        "trades":
            full["trades"],
        "trades_per_year": round(
            full["trades"]
            / years,
            2,
        ),
        "winners":
            full["winners"],
        "losers":
            full["losers"],
        "win_rate":
            full["win_rate"],
        "profit_factor":
            full[
                "profit_factor"
            ],
        "total_r":
            full["total_r"],
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
        "annual_r_linear": round(
            full["total_r"]
            / years,
            3,
        ),
    }

    row.update(
        params
    )

    profitable_eras = 0
    minimum_era_pf = None

    for (
        era_name,
        era_start,
        era_end,
    ) in ERAS:
        era = stats_for_trades(
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

        if (
            era[
                "trades"
            ] >= 5
        ):
            if (
                minimum_era_pf
                is None
            ):
                minimum_era_pf = (
                    era[
                        "profit_factor"
                    ]
                )
            else:
                minimum_era_pf = min(
                    minimum_era_pf,
                    era[
                        "profit_factor"
                    ],
                )

            if (
                era[
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

        recent = stats_for_trades(
            trades,
            start,
            RESEARCH_TO,
        )

        row[
            f"last_{years_back}y_trades"
        ] = recent[
            "trades"
        ]

        row[
            f"last_{years_back}y_pf"
        ] = recent[
            "profit_factor"
        ]

        row[
            f"last_{years_back}y_r"
        ] = recent[
            "total_r"
        ]

        row[
            f"last_{years_back}y_expectancy"
        ] = recent[
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

        atr = atr_series(
            h1,
            ATR_LENGTH,
        )

        daily_state = (
            prepare_daily(
                daily
            )
        )

        raw = (
            build_raw_candidates(
                h1,
                atr,
                daily_state,
            )
        )

        STATUS[
            "raw_candidates"
        ] = len(
            raw
        )

        years = (
            RESEARCH_TO
            - RESEARCH_FROM
        ).total_seconds() / (
            365.2425
            * 86400
        )

        # ----------------------------------------------------
        # CURRENT LIVE CONTROL
        # ----------------------------------------------------

        STATUS.update({
            "state":
                "running_control",
            "message":
                "Running exact current live control",
        })

        current_live = (
            build_current_live_eligible(
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

        control_row = make_row(
            "CURRENT_LIVE_CONTROL",
            current_live,
            live_trades,
            live_ignored,
            years,
            {
                "body_atr":
                    None,
                "range_atr":
                    None,
                "structure_lookback":
                    20,
                "structure_distance":
                    0.20,
                "daily_close_ema":
                    150,
                "alignment_fast":
                    20,
                "alignment_slow":
                    150,
                "anchor":
                    None,
                "session":
                    "08_17",
            },
        )

        # ----------------------------------------------------
        # CORE GRID
        # ----------------------------------------------------

        STATUS.update({
            "state":
                "running_core",
            "message":
                f"Running {TOTAL_CORE} "
                f"tight robustness combinations",
            "completed_core":
                0,
        })

        core_rows = []

        for completed, config in enumerate(
            CORE_CONFIGS,
            start=1,
        ):
            (
                body_atr,
                range_atr,
                structure_lookback,
                structure_distance,
                daily_close_ema,
                alignment,
            ) = config

            eligible = [
                signal
                for signal
                in raw
                if passes_core(
                    signal,
                    body_atr,
                    range_atr,
                    structure_lookback,
                    structure_distance,
                    daily_close_ema,
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

            row = make_row(
                "CORE",
                eligible,
                trades,
                ignored,
                years,
                {
                    "body_atr":
                        body_atr,
                    "range_atr":
                        range_atr,
                    "structure_lookback":
                        structure_lookback,
                    "structure_distance":
                        structure_distance,
                    "daily_close_ema":
                        daily_close_ema,
                    "alignment_fast":
                        alignment[0],
                    "alignment_slow":
                        alignment[1],
                    "anchor":
                        None,
                    "session":
                        "ALL",
                },
            )

            core_rows.append(
                row
            )

            STATUS[
                "completed_core"
            ] = completed

            if (
                completed % 250 == 0
                or completed
                == TOTAL_CORE
            ):
                print(
                    f"CORE "
                    f"{completed}/"
                    f"{TOTAL_CORE}",
                    flush=True,
                )

        core_df = pd.DataFrame(
            core_rows
        )

        # Add a simple robustness score that strongly favours:
        # 4 profitable eras, minimum-era PF, full PF, then frequency.
        core_df[
            "robustness_score"
        ] = (
            core_df[
                "profitable_eras"
            ] * 1000
            + core_df[
                "minimum_era_pf_5_plus"
            ].fillna(
                0
            ) * 100
            + core_df[
                "profit_factor"
            ] * 10
            + core_df[
                "trades"
            ] / 1000.0
        )

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

        core_df.to_csv(
            OUTPUT_CORE,
            index=False,
        )

        # ----------------------------------------------------
        # TIMING OVERLAYS
        # ----------------------------------------------------

        STATUS.update({
            "state":
                "running_timing",
            "message":
                f"Running {TOTAL_TIMING} "
                f"timing overlays",
            "completed_timing":
                0,
        })

        timing_rows = []

        for anchor in TIMING_ANCHORS:
            anchor_eligible = [
                signal
                for signal
                in raw
                if passes_core(
                    signal,
                    anchor[
                        "body_atr"
                    ],
                    anchor[
                        "range_atr"
                    ],
                    anchor[
                        "structure_lookback"
                    ],
                    anchor[
                        "structure_distance"
                    ],
                    anchor[
                        "daily_close_ema"
                    ],
                    anchor[
                        "alignment"
                    ],
                )
            ]

            for (
                session_name,
                start_hour,
                end_hour,
            ) in TIMING_WINDOWS:
                eligible = [
                    signal
                    for signal
                    in anchor_eligible
                    if passes_session(
                        signal,
                        start_hour,
                        end_hour,
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
                    "TIMING",
                    eligible,
                    trades,
                    ignored,
                    years,
                    {
                        "body_atr":
                            anchor[
                                "body_atr"
                            ],
                        "range_atr":
                            anchor[
                                "range_atr"
                            ],
                        "structure_lookback":
                            anchor[
                                "structure_lookback"
                            ],
                        "structure_distance":
                            anchor[
                                "structure_distance"
                            ],
                        "daily_close_ema":
                            anchor[
                                "daily_close_ema"
                            ],
                        "alignment_fast":
                            anchor[
                                "alignment"
                            ][0],
                        "alignment_slow":
                            anchor[
                                "alignment"
                            ][1],
                        "anchor":
                            anchor[
                                "anchor"
                            ],
                        "session":
                            session_name,
                    },
                )

                timing_rows.append(
                    row
                )

                STATUS[
                    "completed_timing"
                ] += 1

        timing_df = pd.DataFrame(
            timing_rows
        )

        timing_df = timing_df.sort_values(
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

        timing_df.to_csv(
            OUTPUT_TIMING,
            index=False,
        )

        STATUS.update({
            "state":
                "complete",
            "message":
                "EUR/GBP tight robustness sweep complete",
            "output_files": [
                OUTPUT_CORE,
                OUTPUT_TIMING,
            ],
            "current_live_control": (
                control_row
            ),
        })

        print()
        print("=" * 90)
        print(
            "EUR/GBP LONG TIGHT ROBUSTNESS SWEEP COMPLETE"
        )
        print("=" * 90)
        print(
            f"Core rows: "
            f"{len(core_df)}"
        )
        print(
            f"Timing rows: "
            f"{len(timing_df)}"
        )
        print(
            f"Core output: "
            f"{OUTPUT_CORE}"
        )
        print(
            f"Timing output: "
            f"{OUTPUT_TIMING}"
        )
        print()
        print(
            "CURRENT LIVE CONTROL:"
        )
        print(
            pd.DataFrame(
                [control_row]
            ).to_string(
                index=False
            )
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
            "EUR/GBP Long Tight Robustness Sweep",
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
        "downloads": {
            "core":
                "/download/core",
            "timing":
                "/download/timing",
        },
    })


@app.route("/status")
def status():
    return jsonify(
        STATUS
    )


def download_file(
    filename
):
    if not os.path.exists(
        filename
    ):
        return jsonify({
            "status":
                "not_ready",
            "message":
                f"{filename} "
                f"is not ready yet",
        }), 404

    return send_file(
        filename,
        as_attachment=True,
        download_name=filename,
    )


@app.route("/download/core")
def download_core():
    return download_file(
        OUTPUT_CORE
    )


@app.route("/download/timing")
def download_timing():
    return download_file(
        OUTPUT_TIMING
    )


if __name__ == "__main__":
    thread = threading.Thread(
        target=run_research,
        name=(
            "eurgbp-long-tight-robustness"
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
