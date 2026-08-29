import os
import itertools
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# GBP/USD SHORT - CONSERVATIVE TIMING / WEEKDAY VALIDATION
#
# RESEARCH ONLY — NEVER SUBMITS ORDERS.
#
# IMPORTANT:
#   Timing exclusions are OPTIONAL.
#   The baseline with NO exclusions is the default.
#   We only want to keep exclusions if there is a genuine,
#   repeatable edge rather than a cosmetic backtest improvement.
#
# Frozen structural core:
#   Bearish engulfing
#   Body ratio >= 1.00
#   Structure lookback = 70 H1 bars
#   Signal high within 0.175 ATR14 of prior 70-bar high
#   Strong close OFF
#   Previous completed D close < EMA100
#   EMA40 < EMA100
#   EMA100 5-day slope <= -0.05 Daily ATR14
#   Daily ATR14 >= 0.80 x its 50-day SMA
#   RR = 2.50
#   Stop = signal high + 10 ticks
#   Adverse short slippage = 5 ticks
#
# Timing methodology:
#   1) Build baseline with ALL hours / ALL weekdays.
#   2) Measure each NY hour and weekday individually.
#   3) Test every SINGLE-hour exclusion.
#   4) Test every SINGLE-weekday exclusion.
#   5) Only allow multi-exclusion combinations using periods that
#      independently look genuinely weak.
#
# A period is considered a "candidate bad period" only if:
#   - at least 6 baseline trades occurred in that bucket
#   - bucket total R < 0
#   - bucket PF < 0.85
#
# This is deliberately conservative.
#
# Multi-exclusion tests:
#   - baseline
#   - each candidate hour alone
#   - each candidate weekday alone
#   - up to 2 candidate hours together
#   - up to 2 candidate hours + 1 candidate weekday
#
# Every timing rule gets full-history + four-era results.
#
# OANDA midpoint candles
# Daily alignment = 17:00 America/New_York
# ============================================================


app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_URL = "https://api-fxtrade.oanda.com"

INSTRUMENT = "GBP_USD"
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

H1_WARMUP_DAYS = 170
DAILY_WARMUP_DAYS = 2200

# Frozen GBP/USD short structural core
BODY_RATIO = 1.00
STRUCTURE_LOOKBACK = 70
MAX_DISTANCE_ATR = 0.175
SLOW_EMA = 100
FAST_EMA = 40
EMA100_SLOPE_MAX = -0.05
DAILY_ATR_RATIO_MIN = 0.80
REWARD_RISK = 2.50

# Conservative threshold for even considering a timing exclusion
MIN_BUCKET_TRADES = 6
BAD_BUCKET_MAX_PF = 0.85

OUTPUT_DIAGNOSTICS = "gbpusd_short_timing_diagnostics.csv"
OUTPUT_RULES = "gbpusd_short_timing_rules.csv"
OUTPUT_TRADES = "gbpusd_short_baseline_trades.csv"


# ============================================================
# ERA WINDOWS
# ============================================================

ERAS = [
    (
        "2002_2009",
        datetime(
            2002, 5, 6, 20, 0,
            tzinfo=timezone.utc,
        ),
        datetime(
            2010, 1, 1, 0, 0,
            tzinfo=timezone.utc,
        ),
    ),
    (
        "2010_2017",
        datetime(
            2010, 1, 1, 0, 0,
            tzinfo=timezone.utc,
        ),
        datetime(
            2018, 1, 1, 0, 0,
            tzinfo=timezone.utc,
        ),
    ),
    (
        "2018_2023",
        datetime(
            2018, 1, 1, 0, 0,
            tzinfo=timezone.utc,
        ),
        datetime(
            2024, 1, 1, 0, 0,
            tzinfo=timezone.utc,
        ),
    ),
    (
        "2024_present",
        datetime(
            2024, 1, 1, 0, 0,
            tzinfo=timezone.utc,
        ),
        None,
    ),
]


# ============================================================
# STATUS
# ============================================================

STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "service": "GBPUSD Short Conservative Timing Validation",
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "baseline_trades": 0,
    "candidate_bad_hours": [],
    "candidate_bad_weekdays": [],
    "timing_rules_total": 0,
    "timing_rules_completed": 0,
    "diagnostic_rows": 0,
    "rule_rows": 0,
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

    for raw in data.get(
        "candles",
        [],
    ):
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
            cursor
            + timedelta(
                days=H1_CHUNK_DAYS
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

def sma_series(
    values,
    length,
):
    result = [None] * len(values)

    if len(values) < length:
        return result

    running = sum(
        values[:length]
    )

    result[
        length - 1
    ] = running / length

    for index in range(
        length,
        len(values),
    ):
        running += values[index]
        running -= values[
            index - length
        ]

        result[index] = (
            running / length
        )

    return result


def ema_series(
    values,
    length,
):
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
        2.0
        / (length + 1.0)
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
                * (length - 1)
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
        true_ranges(candles),
        length,
    )


# ============================================================
# DAILY ALIGNMENT
# ============================================================

def current_daily_start(
    timestamp_utc,
):
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
        candidate -= timedelta(
            days=1
        )

    return candidate.astimezone(
        timezone.utc
    )


def build_daily_state(daily):
    closes = [
        candle["close"]
        for candle in daily
    ]

    ema40 = ema_series(
        closes,
        FAST_EMA,
    )

    ema100 = ema_series(
        closes,
        SLOW_EMA,
    )

    atr14 = atr_series(
        daily,
        14,
    )

    atr_for_sma = [
        value
        if value is not None
        else 0.0
        for value in atr14
    ]

    atr14_sma50 = sma_series(
        atr_for_sma,
        50,
    )

    for index in range(
        min(
            63,
            len(atr14_sma50),
        )
    ):
        atr14_sma50[
            index
        ] = None

    return {
        "ema40": ema40,
        "ema100": ema100,
        "atr14": atr14,
        "atr14_sma50": atr14_sma50,
    }


def build_h1_daily_lookup(
    h1,
    daily,
    daily_state,
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
            < len(daily)
            and daily[
                daily_index + 1
            ]["time"]
            < session_start
        ):
            daily_index += 1

        if daily_index < 0:
            continue

        d = daily[daily_index]

        ema40 = (
            daily_state[
                "ema40"
            ][daily_index]
        )

        ema100 = (
            daily_state[
                "ema100"
            ][daily_index]
        )

        daily_atr = (
            daily_state[
                "atr14"
            ][daily_index]
        )

        daily_atr_sma50 = (
            daily_state[
                "atr14_sma50"
            ][daily_index]
        )

        ema100_slope_5_atr = None

        if (
            daily_index >= 5
            and ema100 is not None
            and daily_state[
                "ema100"
            ][
                daily_index - 5
            ] is not None
            and daily_atr is not None
            and daily_atr > 0
        ):
            ema100_slope_5_atr = (
                ema100
                - daily_state[
                    "ema100"
                ][
                    daily_index - 5
                ]
            ) / daily_atr

        daily_atr_ratio_50 = None

        if (
            daily_atr is not None
            and daily_atr_sma50
            is not None
            and daily_atr_sma50 > 0
        ):
            daily_atr_ratio_50 = (
                daily_atr
                / daily_atr_sma50
            )

        lookup[h1_index] = {
            "close": d["close"],
            "ema40": ema40,
            "ema100": ema100,
            "ema100_slope_5_atr": (
                ema100_slope_5_atr
            ),
            "daily_atr_ratio_50": (
                daily_atr_ratio_50
            ),
        }

    return lookup


# ============================================================
# SIGNALS
# ============================================================

def build_candidates(
    h1,
    h1_atr,
    daily_lookup,
):
    candidates = []

    for index in range(
        STRUCTURE_LOOKBACK,
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

        atr = h1_atr[index]

        daily = daily_lookup[
            index
        ]

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

        if (
            previous_body <= 0
            or current_body <= 0
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

        if (
            current_body
            / previous_body
            < BODY_RATIO
        ):
            continue

        previous_highest = max(
            candle["high"]
            for candle in h1[
                index
                - STRUCTURE_LOOKBACK:index
            ]
        )

        distance = (
            previous_highest
            - signal["high"]
        ) / atr

        if (
            distance
            > MAX_DISTANCE_ATR
        ):
            continue

        ema40 = daily.get(
            "ema40"
        )

        ema100 = daily.get(
            "ema100"
        )

        if (
            ema40 is None
            or ema100 is None
        ):
            continue

        if not (
            daily["close"]
            < ema100
        ):
            continue

        if not (
            ema40
            < ema100
        ):
            continue

        slope = daily.get(
            "ema100_slope_5_atr"
        )

        if (
            slope is None
            or slope
            > EMA100_SLOPE_MAX
        ):
            continue

        daily_atr_ratio = (
            daily.get(
                "daily_atr_ratio_50"
            )
        )

        if (
            daily_atr_ratio is None
            or daily_atr_ratio
            < DAILY_ATR_RATIO_MIN
        ):
            continue

        ny_time = (
            signal["time"]
            .astimezone(NY_TZ)
        )

        candidates.append({
            "index": index,
            "time": signal["time"],
            "ny_hour": ny_time.hour,
            "ny_weekday": ny_time.weekday(),
            "ny_weekday_name": ny_time.strftime("%A"),
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

    if (
        reference_risk
        <= 0
    ):
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
    candidates,
    excluded_hours=None,
    excluded_weekdays=None,
):
    if excluded_hours is None:
        excluded_hours = set()

    if excluded_weekdays is None:
        excluded_weekdays = set()

    trades = []
    position_exit_index = -1
    ignored = 0
    filtered_by_timing = 0
    still_open = False

    for candidate in candidates:
        if (
            candidate["ny_hour"]
            in excluded_hours
            or candidate["ny_weekday"]
            in excluded_weekdays
        ):
            filtered_by_timing += 1
            continue

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

        if (
            trade["status"]
            == "OPEN"
        ):
            still_open = True
            break

        enriched = dict(trade)

        enriched[
            "ny_hour"
        ] = candidate[
            "ny_hour"
        ]

        enriched[
            "ny_weekday"
        ] = candidate[
            "ny_weekday"
        ]

        enriched[
            "ny_weekday_name"
        ] = candidate[
            "ny_weekday_name"
        ]

        trades.append(
            enriched
        )

        position_exit_index = (
            trade["exit_index"]
        )

    return (
        trades,
        ignored,
        filtered_by_timing,
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


def bucket_stats(
    trades,
    key,
    value,
):
    subset = [
        trade
        for trade in trades
        if trade[key] == value
    ]

    return stats_for_trades(
        subset
    )


# ============================================================
# TIMING RULE HELPERS
# ============================================================

def rule_label(
    excluded_hours,
    excluded_weekdays,
):
    if (
        not excluded_hours
        and not excluded_weekdays
    ):
        return "BASELINE_NO_EXCLUSIONS"

    parts = []

    if excluded_hours:
        parts.append(
            "EXCLUDE_HOURS_"
            + "_".join(
                f"{hour:02d}"
                for hour in sorted(
                    excluded_hours
                )
            )
        )

    if excluded_weekdays:
        names = [
            [
                "Mon",
                "Tue",
                "Wed",
                "Thu",
                "Fri",
                "Sat",
                "Sun",
            ][day]
            for day in sorted(
                excluded_weekdays
            )
        ]

        parts.append(
            "EXCLUDE_"
            + "_".join(names)
        )

    return "__".join(
        parts
    )


def make_rule_row(
    h1,
    candidates,
    excluded_hours,
    excluded_weekdays,
    years,
):
    (
        trades,
        ignored,
        filtered_by_timing,
        still_open,
    ) = simulate(
        h1,
        candidates,
        excluded_hours,
        excluded_weekdays,
    )

    full = stats_for_trades(
        trades
    )

    row = {
        "rule": rule_label(
            excluded_hours,
            excluded_weekdays,
        ),
        "excluded_hours_ny": (
            ",".join(
                str(hour)
                for hour in sorted(
                    excluded_hours
                )
            )
        ),
        "excluded_weekdays_ny": (
            ",".join(
                str(day)
                for day in sorted(
                    excluded_weekdays
                )
            )
        ),
        "number_excluded_hours": (
            len(excluded_hours)
        ),
        "number_excluded_weekdays": (
            len(excluded_weekdays)
        ),
        "filtered_by_timing": (
            filtered_by_timing
        ),
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
        ] = era["trades"]

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
            era["total_r"]
            > 0
        ):
            profitable_eras += 1

        if (
            era["trades"]
            >= 5
        ):
            eras_with_5_plus += 1

            if (
                era["total_r"]
                > 0
            ):
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
        print("=" * 74)
        print(
            "GBP/USD SHORT - CONSERVATIVE TIMING VALIDATION"
        )
        print("=" * 74)
        print(
            "Baseline is ALL HOURS / ALL WEEKDAYS"
        )
        print(
            "No timing exclusion is required unless "
            "the data genuinely supports one."
        )
        print()

        STATUS.update({
            "state": "fetching_data",
            "message": (
                "Fetching GBP/USD OANDA history"
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
                "No GBP/USD H1 candles returned"
            )

        if not daily:
            raise RuntimeError(
                "No GBP/USD daily candles returned"
            )

        STATUS.update({
            "state": "precomputing",
            "message": (
                "Building frozen structural signal set"
            ),
        })

        h1_atr = atr_series(
            h1,
            14,
        )

        daily_state = (
            build_daily_state(
                daily
            )
        )

        daily_lookup = (
            build_h1_daily_lookup(
                h1,
                daily,
                daily_state,
            )
        )

        candidates = (
            build_candidates(
                h1,
                h1_atr,
                daily_lookup,
            )
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

        # ----------------------------------------------------
        # BASELINE
        # ----------------------------------------------------

        (
            baseline_trades,
            baseline_ignored,
            baseline_timing_filtered,
            baseline_still_open,
        ) = simulate(
            h1,
            candidates,
            set(),
            set(),
        )

        STATUS[
            "baseline_trades"
        ] = len(
            baseline_trades
        )

        baseline_stats = (
            stats_for_trades(
                baseline_trades
            )
        )

        print(
            "Baseline trades:",
            baseline_stats[
                "trades"
            ],
        )

        print(
            "Baseline PF:",
            baseline_stats[
                "profit_factor"
            ],
        )

        print(
            "Baseline total R:",
            baseline_stats[
                "total_r"
            ],
        )

        # Save baseline trade list for inspection.
        baseline_trade_rows = []

        for trade in baseline_trades:
            baseline_trade_rows.append({
                "signal_time_utc": (
                    trade[
                        "signal_time"
                    ].isoformat()
                ),
                "exit_time_utc": (
                    trade[
                        "exit_time"
                    ].isoformat()
                ),
                "ny_hour": (
                    trade[
                        "ny_hour"
                    ]
                ),
                "ny_weekday": (
                    trade[
                        "ny_weekday"
                    ]
                ),
                "ny_weekday_name": (
                    trade[
                        "ny_weekday_name"
                    ]
                ),
                "exit_reason": (
                    trade[
                        "exit_reason"
                    ]
                ),
                "result_r": (
                    trade[
                        "result_r"
                    ]
                ),
            })

        pd.DataFrame(
            baseline_trade_rows
        ).to_csv(
            OUTPUT_TRADES,
            index=False,
        )

        # ----------------------------------------------------
        # BUCKET DIAGNOSTICS
        # ----------------------------------------------------

        STATUS.update({
            "state": "diagnostics",
            "message": (
                "Measuring individual NY hours and weekdays"
            ),
        })

        diagnostics = []

        candidate_bad_hours = []
        candidate_bad_weekdays = []

        for hour in range(24):
            stats = bucket_stats(
                baseline_trades,
                "ny_hour",
                hour,
            )

            is_bad_candidate = (
                stats["trades"]
                >= MIN_BUCKET_TRADES
                and stats[
                    "total_r"
                ] < 0
                and stats[
                    "profit_factor"
                ]
                < BAD_BUCKET_MAX_PF
            )

            if is_bad_candidate:
                candidate_bad_hours.append(
                    hour
                )

            diagnostics.append({
                "bucket_type": (
                    "NY_HOUR"
                ),
                "bucket_value": hour,
                "bucket_name": (
                    f"{hour:02d}:00"
                ),
                **stats,
                "candidate_bad_period": (
                    is_bad_candidate
                ),
            })

        weekday_names = [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
        ]

        for weekday in range(5):
            stats = bucket_stats(
                baseline_trades,
                "ny_weekday",
                weekday,
            )

            is_bad_candidate = (
                stats["trades"]
                >= MIN_BUCKET_TRADES
                and stats[
                    "total_r"
                ] < 0
                and stats[
                    "profit_factor"
                ]
                < BAD_BUCKET_MAX_PF
            )

            if is_bad_candidate:
                candidate_bad_weekdays.append(
                    weekday
                )

            diagnostics.append({
                "bucket_type": (
                    "NY_WEEKDAY"
                ),
                "bucket_value": (
                    weekday
                ),
                "bucket_name": (
                    weekday_names[
                        weekday
                    ]
                ),
                **stats,
                "candidate_bad_period": (
                    is_bad_candidate
                ),
            })

        diagnostics_df = (
            pd.DataFrame(
                diagnostics
            )
        )

        diagnostics_df.to_csv(
            OUTPUT_DIAGNOSTICS,
            index=False,
        )

        STATUS[
            "candidate_bad_hours"
        ] = candidate_bad_hours

        STATUS[
            "candidate_bad_weekdays"
        ] = candidate_bad_weekdays

        STATUS[
            "diagnostic_rows"
        ] = len(
            diagnostics_df
        )

        print(
            "Candidate bad hours:",
            candidate_bad_hours,
        )

        print(
            "Candidate bad weekdays:",
            candidate_bad_weekdays,
        )

        # ----------------------------------------------------
        # CONSERVATIVE RULE SET
        # ----------------------------------------------------

        rule_specs = [
            (
                set(),
                set(),
            )
        ]

        # Every single hour exclusion,
        # even if not a candidate, for transparent diagnosis.
        for hour in range(24):
            rule_specs.append(
                (
                    {hour},
                    set(),
                )
            )

        # Every single weekday exclusion.
        for weekday in range(5):
            rule_specs.append(
                (
                    set(),
                    {weekday},
                )
            )

        # Multi-hour combinations ONLY from independently bad hours.
        for number_hours in [
            2
        ]:
            for hours in itertools.combinations(
                candidate_bad_hours,
                number_hours,
            ):
                rule_specs.append(
                    (
                        set(hours),
                        set(),
                    )
                )

        # Candidate hours + candidate weekday.
        for weekday in (
            candidate_bad_weekdays
        ):
            for hour in (
                candidate_bad_hours
            ):
                rule_specs.append(
                    (
                        {hour},
                        {weekday},
                    )
                )

            for hours in itertools.combinations(
                candidate_bad_hours,
                2,
            ):
                rule_specs.append(
                    (
                        set(hours),
                        {weekday},
                    )
                )

        # Remove duplicates.
        unique_specs = []
        seen = set()

        for (
            hours,
            weekdays,
        ) in rule_specs:
            key = (
                tuple(
                    sorted(hours)
                ),
                tuple(
                    sorted(weekdays)
                ),
            )

            if key in seen:
                continue

            seen.add(key)

            unique_specs.append(
                (
                    hours,
                    weekdays,
                )
            )

        STATUS[
            "timing_rules_total"
        ] = len(
            unique_specs
        )

        STATUS.update({
            "state": "testing_rules",
            "message": (
                "Testing conservative timing exclusions"
            ),
        })

        rule_rows = []

        for number, (
            excluded_hours,
            excluded_weekdays,
        ) in enumerate(
            unique_specs,
            start=1,
        ):
            row = make_rule_row(
                h1,
                candidates,
                excluded_hours,
                excluded_weekdays,
                years,
            )

            rule_rows.append(
                row
            )

            STATUS[
                "timing_rules_completed"
            ] = number

        rules_df = pd.DataFrame(
            rule_rows
        )

        # Baseline comparison.
        baseline_pf = (
            baseline_stats[
                "profit_factor"
            ]
        )

        baseline_expectancy = (
            baseline_stats[
                "expectancy_r"
            ]
        )

        baseline_total_r = (
            baseline_stats[
                "total_r"
            ]
        )

        baseline_trade_count = (
            baseline_stats[
                "trades"
            ]
        )

        rules_df[
            "pf_change_vs_baseline"
        ] = (
            rules_df[
                "profit_factor"
            ]
            - baseline_pf
        )

        rules_df[
            "expectancy_change_vs_baseline"
        ] = (
            rules_df[
                "expectancy_r"
            ]
            - baseline_expectancy
        )

        rules_df[
            "total_r_change_vs_baseline"
        ] = (
            rules_df[
                "total_r"
            ]
            - baseline_total_r
        )

        rules_df[
            "trades_removed_vs_baseline"
        ] = (
            baseline_trade_count
            - rules_df[
                "trades"
            ]
        )

        rules_df[
            "trade_retention_pct"
        ] = (
            rules_df[
                "trades"
            ]
            / baseline_trade_count
            * 100.0
        ).round(2)

        # Conservative "genuine edge" flag.
        #
        # We are NOT requiring an exclusion.
        # To earn this flag it must:
        #   - improve PF by >= 0.10
        #   - improve expectancy
        #   - keep >= 85% of baseline trades
        #   - keep all four eras profitable
        #   - worst era PF >= baseline worst-era PF - 0.10
        baseline_row = rules_df[
            rules_df[
                "rule"
            ]
            == "BASELINE_NO_EXCLUSIONS"
        ].iloc[0]

        baseline_worst_era_pf = (
            baseline_row[
                "minimum_era_pf_5_plus"
            ]
        )

        rules_df[
            "genuine_timing_edge_candidate"
        ] = (
            (
                rules_df[
                    "pf_change_vs_baseline"
                ]
                >= 0.10
            )
            &
            (
                rules_df[
                    "expectancy_change_vs_baseline"
                ]
                > 0
            )
            &
            (
                rules_df[
                    "trade_retention_pct"
                ]
                >= 85.0
            )
            &
            (
                rules_df[
                    "profitable_eras_with_5_plus_trades"
                ]
                >= 4
            )
            &
            (
                rules_df[
                    "minimum_era_pf_5_plus"
                ]
                >= (
                    baseline_worst_era_pf
                    - 0.10
                )
            )
        )

        # Baseline must never be marked as an "edge candidate".
        rules_df.loc[
            rules_df[
                "rule"
            ]
            == "BASELINE_NO_EXCLUSIONS",
            "genuine_timing_edge_candidate",
        ] = False

        rules_df = (
            rules_df.sort_values(
                by=[
                    "genuine_timing_edge_candidate",
                    "minimum_era_pf_5_plus",
                    "profit_factor",
                    "total_r",
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

        rules_df.to_csv(
            OUTPUT_RULES,
            index=False,
        )

        genuine_count = int(
            rules_df[
                "genuine_timing_edge_candidate"
            ].sum()
        )

        if genuine_count == 0:
            conclusion = (
                "No timing exclusion passed the "
                "conservative genuine-edge test. "
                "Baseline with no exclusions remains preferred."
            )

        else:
            conclusion = (
                f"{genuine_count} timing rule(s) passed the "
                "conservative genuine-edge test. "
                "Manual robustness review still required."
            )

        STATUS.update({
            "state": "complete",
            "message": (
                "GBP/USD conservative timing "
                "validation completed successfully"
            ),
            "conclusion": conclusion,
            "rule_rows": len(
                rules_df
            ),
            "genuine_timing_edge_candidates": (
                genuine_count
            ),
            "output_diagnostics": (
                OUTPUT_DIAGNOSTICS
            ),
            "output_rules": (
                OUTPUT_RULES
            ),
            "output_trades": (
                OUTPUT_TRADES
            ),
            "earliest_h1": (
                h1[0][
                    "time"
                ].isoformat()
            ),
            "latest_h1": (
                h1[-1][
                    "time"
                ].isoformat()
            ),
        })

        print()
        print("=" * 74)
        print(
            "TIMING VALIDATION COMPLETE"
        )
        print("=" * 74)
        print(conclusion)
        print(
            "Saved:",
            OUTPUT_DIAGNOSTICS,
        )
        print(
            "Saved:",
            OUTPUT_RULES,
        )
        print(
            "Saved:",
            OUTPUT_TRADES,
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
            "GBPUSD Short Conservative Timing Validation"
        ),
        "status": STATUS,
        "instrument": INSTRUMENT,
        "direction": "SHORT",
        "structural_core": {
            "body_ratio": BODY_RATIO,
            "structure_lookback": STRUCTURE_LOOKBACK,
            "max_distance_atr": MAX_DISTANCE_ATR,
            "strong_close": "OFF",
            "fast_ema": FAST_EMA,
            "slow_ema": SLOW_EMA,
            "ema100_slope_max_5d_atr": EMA100_SLOPE_MAX,
            "daily_atr_ratio_min_50d": DAILY_ATR_RATIO_MIN,
            "reward_risk": REWARD_RISK,
        },
        "timing_method": (
            "Baseline first. Exclusions optional. "
            "Only independently weak NY hours/weekdays "
            "are allowed into multi-exclusion tests."
        ),
        "downloads": {
            "diagnostics": (
                "/download/diagnostics"
            ),
            "rules": (
                "/download/rules"
            ),
            "baseline_trades": (
                "/download/trades"
            ),
        },
        "trading_enabled": False,
        "orders_supported": False,
        "executor_connected": False,
    })


@app.route("/status")
def status():
    return jsonify(
        STATUS
    )


@app.route("/download/diagnostics")
def download_diagnostics():
    if not os.path.exists(
        OUTPUT_DIAGNOSTICS
    ):
        return jsonify({
            "status": "not_ready",
            "message": (
                "Diagnostics CSV is not ready yet"
            ),
        }), 404

    return send_file(
        OUTPUT_DIAGNOSTICS,
        as_attachment=True,
        download_name=OUTPUT_DIAGNOSTICS,
    )


@app.route("/download/rules")
def download_rules():
    if not os.path.exists(
        OUTPUT_RULES
    ):
        return jsonify({
            "status": "not_ready",
            "message": (
                "Timing-rules CSV is not ready yet"
            ),
        }), 404

    return send_file(
        OUTPUT_RULES,
        as_attachment=True,
        download_name=OUTPUT_RULES,
    )


@app.route("/download/trades")
def download_trades():
    if not os.path.exists(
        OUTPUT_TRADES
    ):
        return jsonify({
            "status": "not_ready",
            "message": (
                "Baseline trades CSV is not ready yet"
            ),
        }), 404

    return send_file(
        OUTPUT_TRADES,
        as_attachment=True,
        download_name=OUTPUT_TRADES,
    )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    research_thread = (
        threading.Thread(
            target=run_research,
            name=(
                "gbpusd-short-conservative-"
                "timing-validation"
            ),
            daemon=True,
        )
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
