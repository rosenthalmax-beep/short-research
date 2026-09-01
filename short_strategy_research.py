import os
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# EUR/GBP SHORT - CONDITIONAL EDGE SCAN
#
# RESEARCH ONLY — NEVER SUBMITS ORDERS.
#
# Purpose:
#   The raw signal had no edge. Structure + range created a
#   genuine but regime-dependent improvement, especially around
#   120-bar prior-high structure.
#
#   This script tests ONE CONDITIONAL FILTER FAMILY AT A TIME
#   inside several representative 120-bar structure/range bases.
#
#   We are NOT doing a full combination explosion.
#
# Fixed execution:
#   OANDA EUR_GBP H1
#   bearish engulfing
#   body >= 1.00 baseline
#   stop = signal high + 10 ticks
#   adverse short slippage = 5 ticks
#   RR = 3.00
#   pyramiding = 0
#
# Representative structural bases:
#   A: 120 bars, within 0.10 ATR, range >= 1.10 ATR
#   B: 120 bars, within 0.15 ATR, range >= 1.10 ATR
#   C: 120 bars, within 0.20 ATR, range >= 1.00 ATR
#   D: 120 bars, within 0.10 ATR, range >= 1.50 ATR
#
# Conditional families:
#   1) Body ratio minimum
#   2) Maximum close location
#   3) Upward momentum (6h/12h/24h/48h)
#   4) Daily close below EMA
#   5) Daily EMA fast < slow
#   6) NY single-hour inclusion
#   7) London single-hour inclusion
#   8) NY single-hour exclusion
#   9) London single-hour exclusion
#  10) Weekday exclusion
#
# Goal:
#   Find a factor that improves ERA ROBUSTNESS, especially
#   2002-2009 and 2024-present, without destroying frequency.
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

STRUCTURE_LOOKBACK = 120

BASES = [
    {
        "base_name": "A_120_010_110",
        "max_distance_atr": 0.10,
        "min_range_atr": 1.10,
    },
    {
        "base_name": "B_120_015_110",
        "max_distance_atr": 0.15,
        "min_range_atr": 1.10,
    },
    {
        "base_name": "C_120_020_100",
        "max_distance_atr": 0.20,
        "min_range_atr": 1.00,
    },
    {
        "base_name": "D_120_010_150",
        "max_distance_atr": 0.10,
        "min_range_atr": 1.50,
    },
]

BODY_THRESHOLDS = [
    1.00, 1.05, 1.10, 1.20, 1.30, 1.40, 1.50, 1.75
]

MAX_CLOSE_LOCATION_THRESHOLDS = [
    0.50, 0.45, 0.40, 0.35, 0.30, 0.25, 0.20
]

MOMENTUM_LOOKBACKS = [
    6, 12, 24, 48
]

MIN_UP_MOMENTUM_ATR_THRESHOLDS = [
    0.00, 0.25, 0.50, 0.75, 1.00, 1.50
]

DAILY_EMA_LENGTHS = [
    20, 30, 40, 50, 70, 100, 150, 200, 250, 300, 400
]

DAILY_ALIGNMENT_PAIRS = [
    (10, 30),
    (20, 50),
    (20, 100),
    (30, 100),
    (40, 100),
    (50, 100),
    (50, 150),
    (50, 200),
    (100, 200),
    (100, 300),
]

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

H1_WARMUP_DAYS = 500
DAILY_WARMUP_DAYS = 3500

NY_TZ = ZoneInfo("America/New_York")
LONDON_TZ = ZoneInfo("Europe/London")

DAILY_ALIGNMENT_HOUR = 17
DAILY_ALIGNMENT_TIMEZONE = "America/New_York"

OUTPUT_FILE = "eurgbp_short_conditional_edge_scan.csv"


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
    "service": "EURGBP Short Conditional Edge Scan",
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "reward_risk": REWARD_RISK,
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

    result[
        length - 1
    ] = initial

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
    length=14,
):
    return rma_series(
        true_ranges(
            candles
        ),
        length,
    )


# ============================================================
# DAILY LOOKUP
# ============================================================

def current_daily_start(
    timestamp_utc
):
    ny_time = (
        timestamp_utc.astimezone(
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
        candidate -= timedelta(
            days=1
        )

    return candidate.astimezone(
        timezone.utc
    )


def build_daily_rows(
    daily
):
    closes = [
        candle["close"]
        for candle in daily
    ]

    lengths = set(
        DAILY_EMA_LENGTHS
    )

    for fast, slow in (
        DAILY_ALIGNMENT_PAIRS
    ):
        lengths.add(fast)
        lengths.add(slow)

    ema_map = {
        length: ema_series(
            closes,
            length,
        )
        for length in lengths
    }

    rows = []

    for index, candle in enumerate(
        daily
    ):
        rows.append({
            "time": candle["time"],
            "close": candle["close"],
            "emas": {
                length:
                    ema_map[length][index]
                for length in lengths
            },
        })

    return rows


def build_h1_daily_lookup(
    h1,
    daily_rows,
):
    lookup = [None] * len(h1)
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
            < len(daily_rows)
            and daily_rows[
                daily_index + 1
            ]["time"] < session_start
        ):
            daily_index += 1

        if daily_index < 0:
            continue

        lookup[h1_index] = (
            daily_rows[daily_index]
        )

    return lookup


# ============================================================
# CANDIDATES + FEATURES
# ============================================================

def build_candidates(
    h1,
    h1_atr,
    daily_lookup,
):
    candidates = []

    max_lookback = max(
        STRUCTURE_LOOKBACK,
        max(MOMENTUM_LOOKBACKS),
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

        if (
            atr is None
            or atr <= 0
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

        body_ratio = (
            current_body
            / previous_body
        )

        if body_ratio < 1.00:
            continue

        close_location = (
            signal["close"]
            - signal["low"]
        ) / candle_range

        range_atr = (
            candle_range
            / atr
        )

        previous_highest = max(
            candle["high"]
            for candle in h1[
                index - STRUCTURE_LOOKBACK:
                index
            ]
        )

        structure_distance_atr = (
            previous_highest
            - signal["high"]
        ) / atr

        momentum = {}

        for lookback in (
            MOMENTUM_LOOKBACKS
        ):
            momentum[
                lookback
            ] = (
                signal["close"]
                - h1[
                    index - lookback
                ]["close"]
            ) / atr

        ny_time = (
            signal["time"]
            .astimezone(
                NY_TZ
            )
        )

        london_time = (
            signal["time"]
            .astimezone(
                LONDON_TZ
            )
        )

        candidates.append({
            "index": index,
            "time": signal["time"],
            "body_ratio": body_ratio,
            "close_location": close_location,
            "range_atr": range_atr,
            "structure_distance_atr": (
                structure_distance_atr
            ),
            "momentum": momentum,
            "daily": daily_lookup[index],
            "ny_hour": ny_time.hour,
            "london_hour": london_time.hour,
            "weekday": london_time.weekday(),
        })

    return candidates


# ============================================================
# EXIT SIMULATION
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
        * REWARD_RISK
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
            signal_index
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
        signal_index
    ] = result

    return result


def simulate(
    h1,
    eligible,
):
    trades = []
    position_exit_index = -1
    ignored = 0
    still_open = False

    for candidate in eligible:
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
    base,
    family,
    test_label,
    parameter_1_name,
    parameter_1_value,
    parameter_2_name,
    parameter_2_value,
    base_candidates,
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
        "base_name": base["base_name"],
        "base_structure_lookback": (
            STRUCTURE_LOOKBACK
        ),
        "base_max_distance_atr": (
            base["max_distance_atr"]
        ),
        "base_min_range_atr": (
            base["min_range_atr"]
        ),
        "family": family,
        "test_label": test_label,
        "parameter_1_name": (
            parameter_1_name
        ),
        "parameter_1_value": (
            parameter_1_value
        ),
        "parameter_2_name": (
            parameter_2_name
        ),
        "parameter_2_value": (
            parameter_2_value
        ),
        "base_signals": len(
            base_candidates
        ),
        "eligible_signals": len(
            eligible
        ),
        "retention_vs_base_pct": round(
            len(eligible)
            / len(base_candidates)
            * 100.0,
            2,
        ) if base_candidates else 0.0,
        "ignored_due_to_open_trade": (
            ignored
        ),
        "still_open_at_end": (
            still_open
        ),
        "trades": full["trades"],
        "trades_per_year": round(
            full["trades"]
            / years,
            2,
        ),
        "winners": full["winners"],
        "losers": full["losers"],
        "win_rate": full["win_rate"],
        "profit_factor": full[
            "profit_factor"
        ],
        "total_r": full["total_r"],
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
        ] = era["trades"]

        row[
            f"{era_name}_pf"
        ] = era["profit_factor"]

        row[
            f"{era_name}_r"
        ] = era["total_r"]

        row[
            f"{era_name}_expectancy"
        ] = era["expectancy_r"]

        if era["trades"] >= 5:
            if era["total_r"] > 0:
                profitable_eras_with_5_plus += 1

            pf = era["profit_factor"]
            expectancy = era["expectancy_r"]

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

    row[
        "all_four_eras_profitable"
    ] = (
        profitable_eras_with_5_plus
        >= 4
    )

    row[
        "annual_r_linear"
    ] = round(
        full["expectancy_r"]
        * (
            full["trades"]
            / years
        ),
        3,
    )

    return row


# ============================================================
# RUN ONE TEST
# ============================================================

def run_test(
    rows,
    h1,
    years,
    base,
    base_candidates,
    family,
    test_label,
    predicate,
    parameter_1_name=None,
    parameter_1_value=None,
    parameter_2_name=None,
    parameter_2_value=None,
):
    eligible = [
        candidate
        for candidate
        in base_candidates
        if predicate(
            candidate
        )
    ]

    (
        trades,
        ignored,
        still_open,
    ) = simulate(
        h1,
        eligible,
    )

    rows.append(
        make_result_row(
            base,
            family,
            test_label,
            parameter_1_name,
            parameter_1_value,
            parameter_2_name,
            parameter_2_value,
            base_candidates,
            eligible,
            trades,
            ignored,
            still_open,
            years,
        )
    )


# ============================================================
# RESEARCH
# ============================================================

def run_research():
    global STATUS

    try:
        print()
        print("=" * 76)
        print(
            "EUR/GBP SHORT - CONDITIONAL EDGE SCAN"
        )
        print("=" * 76)
        print()

        STATUS.update({
            "state": "fetching_data",
            "message": (
                "Fetching EUR/GBP H1 and daily history"
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
                "No EUR/GBP H1 candles returned"
            )

        if not daily:
            raise RuntimeError(
                "No EUR/GBP daily candles returned"
            )

        STATUS.update({
            "state": "precomputing",
            "message": (
                "Building indicators and features"
            ),
        })

        h1_atr = atr_series(
            h1,
            14,
        )

        daily_rows = (
            build_daily_rows(
                daily
            )
        )

        daily_lookup = (
            build_h1_daily_lookup(
                h1,
                daily_rows,
            )
        )

        raw_candidates = (
            build_candidates(
                h1,
                h1_atr,
                daily_lookup,
            )
        )

        STATUS[
            "raw_bearish_engulfing_signals"
        ] = len(
            raw_candidates
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

        for base in BASES:
            print(
                f"Running base {base['base_name']}",
                flush=True,
            )

            base_candidates = [
                candidate
                for candidate
                in raw_candidates
                if (
                    candidate[
                        "structure_distance_atr"
                    ] <= base[
                        "max_distance_atr"
                    ]
                    and candidate[
                        "range_atr"
                    ] >= base[
                        "min_range_atr"
                    ]
                )
            ]

            # ------------------------------------------
            # BASELINE FOR THIS STRUCTURE/RANGE BASE
            # ------------------------------------------

            run_test(
                rows,
                h1,
                years,
                base,
                base_candidates,
                "BASELINE",
                "base_only",
                lambda candidate: True,
            )

            # ------------------------------------------
            # BODY RATIO
            # ------------------------------------------

            for threshold in BODY_THRESHOLDS:
                run_test(
                    rows,
                    h1,
                    years,
                    base,
                    base_candidates,
                    "BODY_RATIO",
                    f"body_gte_{threshold:.2f}",
                    lambda candidate, t=threshold:
                        candidate[
                            "body_ratio"
                        ] >= t,
                    "min_body_ratio",
                    threshold,
                )

            # ------------------------------------------
            # CLOSE LOCATION
            # ------------------------------------------

            for threshold in (
                MAX_CLOSE_LOCATION_THRESHOLDS
            ):
                run_test(
                    rows,
                    h1,
                    years,
                    base,
                    base_candidates,
                    "CLOSE_LOCATION",
                    (
                        f"close_location_lte_"
                        f"{threshold:.2f}"
                    ),
                    lambda candidate, t=threshold:
                        candidate[
                            "close_location"
                        ] <= t,
                    "max_close_location",
                    threshold,
                )

            # ------------------------------------------
            # UPWARD MOMENTUM
            # ------------------------------------------

            for lookback in (
                MOMENTUM_LOOKBACKS
            ):
                for threshold in (
                    MIN_UP_MOMENTUM_ATR_THRESHOLDS
                ):
                    run_test(
                        rows,
                        h1,
                        years,
                        base,
                        base_candidates,
                        "UP_MOMENTUM",
                        (
                            f"up_{lookback}h_"
                            f"gte_{threshold:.2f}"
                        ),
                        lambda candidate,
                        lb=lookback,
                        t=threshold:
                            candidate[
                                "momentum"
                            ][lb] >= t,
                        "lookback_h",
                        lookback,
                        "min_up_momentum_atr",
                        threshold,
                    )

            # ------------------------------------------
            # DAILY CLOSE BELOW EMA
            # ------------------------------------------

            for length in (
                DAILY_EMA_LENGTHS
            ):
                run_test(
                    rows,
                    h1,
                    years,
                    base,
                    base_candidates,
                    "DAILY_CLOSE_BELOW_EMA",
                    (
                        f"daily_close_below_"
                        f"ema_{length}"
                    ),
                    lambda candidate,
                    length=length:
                        (
                            candidate[
                                "daily"
                            ] is not None
                            and candidate[
                                "daily"
                            ][
                                "emas"
                            ][length]
                            is not None
                            and candidate[
                                "daily"
                            ][
                                "close"
                            ]
                            < candidate[
                                "daily"
                            ][
                                "emas"
                            ][length]
                        ),
                    "ema_length",
                    length,
                )

            # ------------------------------------------
            # DAILY EMA ALIGNMENT
            # ------------------------------------------

            for (
                fast,
                slow,
            ) in DAILY_ALIGNMENT_PAIRS:
                run_test(
                    rows,
                    h1,
                    years,
                    base,
                    base_candidates,
                    "DAILY_EMA_ALIGNMENT",
                    (
                        f"ema_{fast}_below_"
                        f"ema_{slow}"
                    ),
                    lambda candidate,
                    fast=fast,
                    slow=slow:
                        (
                            candidate[
                                "daily"
                            ] is not None
                            and candidate[
                                "daily"
                            ][
                                "emas"
                            ][fast]
                            is not None
                            and candidate[
                                "daily"
                            ][
                                "emas"
                            ][slow]
                            is not None
                            and candidate[
                                "daily"
                            ][
                                "emas"
                            ][fast]
                            < candidate[
                                "daily"
                            ][
                                "emas"
                            ][slow]
                        ),
                    "fast_ema",
                    fast,
                    "slow_ema",
                    slow,
                )

            # ------------------------------------------
            # SINGLE-HOUR INCLUSION
            # ------------------------------------------

            for hour in range(24):
                run_test(
                    rows,
                    h1,
                    years,
                    base,
                    base_candidates,
                    "NY_HOUR_INCLUDE",
                    f"include_ny_{hour:02d}",
                    lambda candidate,
                    hour=hour:
                        candidate[
                            "ny_hour"
                        ] == hour,
                    "ny_hour",
                    hour,
                )

                run_test(
                    rows,
                    h1,
                    years,
                    base,
                    base_candidates,
                    "LONDON_HOUR_INCLUDE",
                    (
                        f"include_london_"
                        f"{hour:02d}"
                    ),
                    lambda candidate,
                    hour=hour:
                        candidate[
                            "london_hour"
                        ] == hour,
                    "london_hour",
                    hour,
                )

            # ------------------------------------------
            # SINGLE-HOUR EXCLUSION
            # ------------------------------------------

            for hour in range(24):
                run_test(
                    rows,
                    h1,
                    years,
                    base,
                    base_candidates,
                    "NY_HOUR_EXCLUDE",
                    f"exclude_ny_{hour:02d}",
                    lambda candidate,
                    hour=hour:
                        candidate[
                            "ny_hour"
                        ] != hour,
                    "excluded_ny_hour",
                    hour,
                )

                run_test(
                    rows,
                    h1,
                    years,
                    base,
                    base_candidates,
                    "LONDON_HOUR_EXCLUDE",
                    (
                        f"exclude_london_"
                        f"{hour:02d}"
                    ),
                    lambda candidate,
                    hour=hour:
                        candidate[
                            "london_hour"
                        ] != hour,
                    "excluded_london_hour",
                    hour,
                )

            # ------------------------------------------
            # WEEKDAY EXCLUSION
            # ------------------------------------------

            weekday_names = {
                0: "MON",
                1: "TUE",
                2: "WED",
                3: "THU",
                4: "FRI",
            }

            for weekday in range(5):
                run_test(
                    rows,
                    h1,
                    years,
                    base,
                    base_candidates,
                    "WEEKDAY_EXCLUDE",
                    (
                        f"exclude_"
                        f"{weekday_names[weekday]}"
                    ),
                    lambda candidate,
                    weekday=weekday:
                        candidate[
                            "weekday"
                        ] != weekday,
                    "excluded_weekday",
                    weekday_names[
                        weekday
                    ],
                )

        df = pd.DataFrame(
            rows
        )

        if df.empty:
            raise RuntimeError(
                "No result rows generated"
            )

        df[
            "adequate_90_trades"
        ] = (
            df[
                "trades"
            ] >= 90
        )

        df[
            "frequency_4py"
        ] = (
            df[
                "trades_per_year"
            ] >= 4.0
        )

        df[
            "worst_era_pf_100"
        ] = (
            df[
                "minimum_era_pf_5_plus"
            ].fillna(0)
            >= 1.00
        )

        df[
            "worst_era_pf_110"
        ] = (
            df[
                "minimum_era_pf_5_plus"
            ].fillna(0)
            >= 1.10
        )

        df[
            "pf_120"
        ] = (
            df[
                "profit_factor"
            ] >= 1.20
        )

        df[
            "pf_130"
        ] = (
            df[
                "profit_factor"
            ] >= 1.30
        )

        df = df.sort_values(
            by=[
                "all_four_eras_profitable",
                "adequate_90_trades",
                "frequency_4py",
                "worst_era_pf_110",
                "worst_era_pf_100",
                "pf_130",
                "pf_120",
                "minimum_era_pf_5_plus",
                "profit_factor",
                "expectancy_r",
                "annual_r_linear",
                "trades",
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
                "EUR/GBP conditional edge scan "
                "completed successfully"
            ),
            "rows_saved": len(
                df
            ),
            "raw_bearish_engulfing_signals": (
                len(raw_candidates)
            ),
            "all_four_eras_profitable_count": int(
                df[
                    "all_four_eras_profitable"
                ].sum()
            ),
            "output_file": (
                OUTPUT_FILE
            ),
        })

        print()
        print("=" * 76)
        print(
            "EUR/GBP CONDITIONAL EDGE SCAN COMPLETE"
        )
        print("=" * 76)
        print(
            "Rows:",
            len(df),
        )
        print(
            "All-four-era profitable rows:",
            int(
                df[
                    "all_four_eras_profitable"
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
            "EURGBP Short Conditional Edge Scan"
        ),
        "status": STATUS,
        "instrument": INSTRUMENT,
        "direction": "SHORT",
        "reward_risk": REWARD_RISK,
        "trading_enabled": False,
        "orders_supported": False,
        "executor_connected": False,
        "bases": BASES,
        "families": [
            "BASELINE",
            "BODY_RATIO",
            "CLOSE_LOCATION",
            "UP_MOMENTUM",
            "DAILY_CLOSE_BELOW_EMA",
            "DAILY_EMA_ALIGNMENT",
            "NY_HOUR_INCLUDE",
            "LONDON_HOUR_INCLUDE",
            "NY_HOUR_EXCLUDE",
            "LONDON_HOUR_EXCLUDE",
            "WEEKDAY_EXCLUDE",
        ],
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
                "EUR/GBP conditional edge CSV "
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
            "eurgbp-short-conditional-edge-scan"
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
