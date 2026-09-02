import os
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# USD/CAD LONG - CURRENT LIVE CONTROL BASELINE
#
# RESEARCH ONLY - NEVER SUBMITS ORDERS.
#
# This is the exact CURRENT LIVE USD/CAD LONG strategy.
# It is the frozen CONTROL for the new long-strategy audit.
#
# ============================================================
# CURRENT LIVE RULES
#
# Instrument:
#   OANDA USD_CAD midpoint H1
#
# Bullish engulfing:
#   previous candle bearish
#   current candle bullish
#   current body engulfs previous body
#
# Minimum body ratio:
#   >= 1.00
#
# Lower wick:
#   >= 0.20 x current body
#
# ATR:
#   Wilder/RMA ATR14, SMA-seeded
#
# Structure:
#   previous 40 H1 bars, excluding current
#   signal low within 0.20 ATR14 of previous lowest low
#
# Daily regime:
#   previous COMPLETED OANDA daily close > EMA200
#   OANDA daily alignment:
#       17:00 America/New_York
#
# Timing:
#   signal candle OPEN time
#   America/New_York
#   exclude 00:00-04:59
#
# Weekdays:
#   no exclusions
#
# Strong close:
#   disabled
#
# Minimum range:
#   disabled
#
# Reward:risk:
#   3.50R
#
# Stop:
#   signal low - 10 ticks
#
# Backtest slippage:
#   adverse long fill = signal close + 5 ticks
#
# Pyramiding:
#   0
#
# Same-bar stop + target:
#   compare candle open->high vs open->low
#   high closer => target first
#   otherwise stop first
#
# Position convention:
#   a signal on the exact H1 bar where the previous trade exits
#   IS allowed.
#
# ============================================================
# RESEARCH WINDOW / ERAS
#
# 2002-05-06 20:00 UTC -> current completed UTC hour
#
# Four eras:
#   2002_2009
#   2010_2017
#   2018_2023
#   2024_present
#
# ============================================================
# OUTPUTS
#
# /download/summary
# /download/calendar
# /download/rolling
# /download/slices
# /download/recent
# /download/drawdowns
# /download/trades
# ============================================================


app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_URL = "https://api-fxtrade.oanda.com"

INSTRUMENT = "USD_CAD"

TICK_SIZE = 0.00001
PRICE_PRECISION = 5

MINIMUM_BODY_RATIO = 1.00

LOWER_WICK_FILTER_ENABLED = True
MINIMUM_LOWER_WICK_BODY_RATIO = 0.20

ATR_LENGTH = 14

STRUCTURE_LOOKBACK = 40
MAXIMUM_DISTANCE_ATR = 0.20

DAILY_EMA_LENGTH = 200

SESSION_TZ = ZoneInfo("America/New_York")
EXCLUDED_NY_HOURS = {
    0, 1, 2, 3, 4
}

REWARD_RISK = 3.50

STOP_BUFFER_TICKS = 10
BACKTEST_SLIPPAGE_TICKS = 5

DAILY_ALIGNMENT_HOUR = 17
DAILY_ALIGNMENT_TIMEZONE = "America/New_York"

H1_CHUNK_DAYS = 180
DAILY_CHUNK_DAYS = 1500

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

H1_WARMUP_DAYS = 120
DAILY_WARMUP_DAYS = 1500

SUMMARY_FILE = "usdcad_long_current_control_summary.csv"
CALENDAR_FILE = "usdcad_long_current_control_calendar.csv"
ROLLING_FILE = "usdcad_long_current_control_rolling_3y.csv"
SLICES_FILE = "usdcad_long_current_control_slices.csv"
RECENT_FILE = "usdcad_long_current_control_recent.csv"
DRAWDOWN_FILE = "usdcad_long_current_control_drawdowns.csv"
TRADES_FILE = "usdcad_long_current_control_trades.csv"


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
    "service": "USD/CAD Long Current Live Control Baseline",
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "orders_supported": False,
    "trading_enabled": False,
    "output_files": [],
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
        dt
        .astimezone(timezone.utc)
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
        key=lambda candle: (
            candle["time"]
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


# ============================================================
# DAILY STATE
# ============================================================

def current_daily_start(
    timestamp_utc
):
    ny_time = (
        timestamp_utc
        .astimezone(
            SESSION_TZ
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


def build_daily_state(
    daily
):
    closes = [
        candle["close"]
        for candle in daily
    ]

    ema200 = ema_series(
        closes,
        DAILY_EMA_LENGTH,
    )

    result = []

    for index, candle in enumerate(
        daily
    ):
        result.append({
            "time": candle["time"],
            "close": candle["close"],
            "ema200": ema200[index],
        })

    return result


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
            if (
                row["ema200"]
                is not None
            ):
                selected = row
        else:
            break

    return selected


# ============================================================
# SIGNAL
# ============================================================

def evaluate_signal(
    h1,
    atr,
    daily_state,
    index,
):
    if index < max(
        ATR_LENGTH,
        STRUCTURE_LOOKBACK,
    ):
        return None

    signal = h1[index]
    previous = h1[
        index - 1
    ]

    current_atr = atr[index]

    if (
        current_atr is None
        or current_atr <= 0
    ):
        return None

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
        return None

    body_ratio = (
        current_body
        / previous_body
    )

    bullish_engulfing = (
        previous["close"]
        < previous["open"]
        and signal["close"]
        > signal["open"]
        and signal["open"]
        <= previous["close"]
        and signal["close"]
        >= previous["open"]
        and body_ratio
        >= MINIMUM_BODY_RATIO
    )

    lower_wick = (
        min(
            signal["open"],
            signal["close"],
        )
        - signal["low"]
    )

    lower_wick_body_ratio = (
        lower_wick
        / current_body
    )

    lower_wick_allowed = (
        lower_wick_body_ratio
        >= MINIMUM_LOWER_WICK_BODY_RATIO
    )

    previous_lowest_low = min(
        candle["low"]
        for candle
        in h1[
            index
            - STRUCTURE_LOOKBACK:
            index
        ]
    )

    distance_atr = (
        signal["low"]
        - previous_lowest_low
    ) / current_atr

    structure_allowed = (
        distance_atr
        <= MAXIMUM_DISTANCE_ATR
    )

    daily = (
        previous_completed_daily(
            signal["time"],
            daily_state,
        )
    )

    if daily is None:
        return None

    daily_regime_allowed = (
        daily["close"]
        > daily["ema200"]
    )

    ny_time = (
        signal["time"]
        .astimezone(
            SESSION_TZ
        )
    )

    timing_allowed = (
        ny_time.hour
        not in EXCLUDED_NY_HOURS
    )

    qualified = all([
        bullish_engulfing,
        lower_wick_allowed,
        structure_allowed,
        daily_regime_allowed,
        timing_allowed,
    ])

    return {
        "qualified": qualified,
        "index": index,
        "signal_time": signal["time"],
        "open": signal["open"],
        "high": signal["high"],
        "low": signal["low"],
        "close": signal["close"],
        "atr14": current_atr,
        "body_ratio": body_ratio,
        "lower_wick_body_ratio": (
            lower_wick_body_ratio
        ),
        "structure_distance_atr": (
            distance_atr
        ),
        "daily_close": daily["close"],
        "daily_ema200": daily["ema200"],
        "ny_hour": ny_time.hour,
        "weekday": ny_time.weekday(),
    }


# ============================================================
# TRADE
# ============================================================

def create_trade(
    signal_result
):
    reference_entry = (
        signal_result["close"]
    )

    backtest_entry = (
        reference_entry
        + BACKTEST_SLIPPAGE_TICKS
        * TICK_SIZE
    )

    stop = (
        signal_result["low"]
        - STOP_BUFFER_TICKS
        * TICK_SIZE
    )

    reference_risk = (
        reference_entry
        - stop
    )

    if reference_risk <= 0:
        raise RuntimeError(
            "Invalid long reference risk"
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
            "Invalid long actual risk"
        )

    return {
        "signal_index": (
            signal_result[
                "index"
            ]
        ),
        "signal_time": (
            signal_result[
                "signal_time"
            ]
        ),
        "reference_entry": (
            reference_entry
        ),
        "backtest_entry": (
            backtest_entry
        ),
        "stop": stop,
        "target": target,
        "actual_risk": (
            actual_risk
        ),
        "exit_index": None,
        "exit_time": None,
        "exit_reason": None,
        "result_r": None,
    }


def determine_exit(
    trade,
    candle
):
    stop_hit = (
        candle["low"]
        <= trade["stop"]
    )

    target_hit = (
        candle["high"]
        >= trade["target"]
    )

    if not (
        stop_hit
        or target_hit
    ):
        return None

    if (
        stop_hit
        and not target_hit
    ):
        return (
            "STOP",
            trade["stop"],
        )

    if (
        target_hit
        and not stop_hit
    ):
        return (
            "TARGET",
            trade["target"],
        )

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
        return (
            "TARGET",
            trade["target"],
        )

    return (
        "STOP",
        trade["stop"],
    )


def simulate(
    h1,
    atr,
    daily_state,
):
    trades = []
    raw_signals = 0
    ignored_signals = 0

    open_trade = None

    for index in range(
        max(
            ATR_LENGTH,
            STRUCTURE_LOOKBACK,
        ),
        len(h1),
    ):
        candle = h1[index]
        candle_time = (
            candle["time"]
        )

        if candle_time < RESEARCH_FROM:
            continue

        if candle_time >= RESEARCH_TO:
            break

        # Existing position is evaluated FIRST.
        # Therefore a fresh signal on the exact H1 exit bar
        # can be taken, matching the locked convention.
        if open_trade is not None:
            exit_info = (
                determine_exit(
                    open_trade,
                    candle,
                )
            )

            if exit_info is not None:
                (
                    exit_reason,
                    exit_price,
                ) = exit_info

                open_trade[
                    "exit_reason"
                ] = exit_reason

                open_trade[
                    "exit_index"
                ] = index

                open_trade[
                    "exit_time"
                ] = candle_time

                open_trade[
                    "result_r"
                ] = (
                    exit_price
                    - open_trade[
                        "backtest_entry"
                    ]
                ) / open_trade[
                    "actual_risk"
                ]

                open_trade = None

        result = evaluate_signal(
            h1,
            atr,
            daily_state,
            index,
        )

        if (
            result is None
            or not result[
                "qualified"
            ]
        ):
            continue

        raw_signals += 1

        if open_trade is not None:
            ignored_signals += 1
            continue

        new_trade = (
            create_trade(
                result
            )
        )

        # Keep the diagnostic signal features in the trade log.
        new_trade.update({
            "atr14": (
                result["atr14"]
            ),
            "body_ratio": (
                result[
                    "body_ratio"
                ]
            ),
            "lower_wick_body_ratio": (
                result[
                    "lower_wick_body_ratio"
                ]
            ),
            "structure_distance_atr": (
                result[
                    "structure_distance_atr"
                ]
            ),
            "daily_close": (
                result[
                    "daily_close"
                ]
            ),
            "daily_ema200": (
                result[
                    "daily_ema200"
                ]
            ),
            "ny_hour": (
                result["ny_hour"]
            ),
            "weekday": (
                result["weekday"]
            ),
        })

        trades.append(
            new_trade
        )

        open_trade = (
            new_trade
        )

    closed = [
        trade
        for trade in trades
        if trade[
            "result_r"
        ] is not None
    ]

    return {
        "trades": closed,
        "raw_signal_count": (
            raw_signals
        ),
        "ignored_signal_count": (
            ignored_signals
        ),
        "position_still_open": (
            open_trade is not None
        ),
    }


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

        selected.append(trade)

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

    total_r = sum(results)

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
            year=dt.year - years
        )
    except ValueError:
        return dt.replace(
            month=2,
            day=28,
            year=dt.year - years,
        )


# ============================================================
# OUTPUTS
# ============================================================

def make_summary(
    trades,
    simulation,
):
    full = stats_for_trades(
        trades
    )

    years = (
        RESEARCH_TO
        - RESEARCH_FROM
    ).total_seconds() / (
        365.2425
        * 86400
    )

    row = {
        "strategy": (
            "CURRENT_LIVE_CONTROL"
        ),
        "research_from": (
            RESEARCH_FROM.isoformat()
        ),
        "research_to": (
            RESEARCH_TO.isoformat()
        ),
        "raw_signals": (
            simulation[
                "raw_signal_count"
            ]
        ),
        "ignored_while_position_open": (
            simulation[
                "ignored_signal_count"
            ]
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
            full[
                "expectancy_r"
            ]
        ),
        "max_drawdown_r": (
            full[
                "max_drawdown_r"
            ]
        ),
        "longest_loss_streak": (
            full[
                "longest_loss_streak"
            ]
        ),
        "annual_r_linear": round(
            full[
                "total_r"
            ] / years,
            3,
        ),
    }

    minimum_era_pf = None
    profitable_eras = 0

    for (
        name,
        start,
        end,
    ) in ERAS:
        era = stats_for_trades(
            trades,
            start,
            (
                RESEARCH_TO
                if end is None
                else min(
                    end,
                    RESEARCH_TO,
                )
            ),
        )

        row[
            f"{name}_trades"
        ] = era["trades"]

        row[
            f"{name}_pf"
        ] = (
            era[
                "profit_factor"
            ]
        )

        row[
            f"{name}_r"
        ] = era["total_r"]

        row[
            f"{name}_expectancy"
        ] = (
            era[
                "expectancy_r"
            ]
        )

        if era["trades"] >= 5:
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

            if era["total_r"] > 0:
                profitable_eras += 1

    row[
        "minimum_era_pf_5_plus"
    ] = minimum_era_pf

    row[
        "profitable_eras"
    ] = profitable_eras

    return pd.DataFrame([
        row
    ])


def make_calendar(
    trades
):
    rows = []

    for year in range(
        RESEARCH_FROM.year,
        RESEARCH_TO.year + 1,
    ):
        start = max(
            RESEARCH_FROM,
            datetime(
                year,
                1,
                1,
                tzinfo=timezone.utc,
            ),
        )

        end = min(
            RESEARCH_TO,
            datetime(
                year + 1,
                1,
                1,
                tzinfo=timezone.utc,
            ),
        )

        if start >= end:
            continue

        row = {
            "year": year,
        }

        row.update(
            stats_for_trades(
                trades,
                start,
                end,
            )
        )

        rows.append(row)

    return pd.DataFrame(rows)


def make_rolling(
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

        row = {
            "window": (
                f"{start_year}_"
                f"{start_year + 2}"
            ),
            "start": start.isoformat(),
            "end": end.isoformat(),
        }

        row.update(
            stats_for_trades(
                trades,
                start,
                end,
            )
        )

        rows.append(row)

    return pd.DataFrame(rows)


def make_slices(
    trades
):
    slices = [
        (
            "first_half_2002_2013",
            RESEARCH_FROM,
            datetime(
                2014, 1, 1,
                tzinfo=timezone.utc,
            ),
        ),
        (
            "second_half_2014_present",
            datetime(
                2014, 1, 1,
                tzinfo=timezone.utc,
            ),
            RESEARCH_TO,
        ),
    ]

    for era in ERAS:
        (
            name,
            start,
            end,
        ) = era

        slices.append((
            name,
            start,
            (
                RESEARCH_TO
                if end is None
                else min(
                    end,
                    RESEARCH_TO,
                )
            ),
        ))

    rows = []

    for (
        label,
        start,
        end,
    ) in slices:
        row = {
            "slice": label,
            "start": start.isoformat(),
            "end": end.isoformat(),
        }

        row.update(
            stats_for_trades(
                trades,
                start,
                end,
            )
        )

        rows.append(row)

    return pd.DataFrame(rows)


def make_recent(
    trades
):
    rows = []

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

        row = {
            "window": (
                f"last_{years_back}_years"
            ),
            "start": (
                start.isoformat()
            ),
            "end": (
                RESEARCH_TO.isoformat()
            ),
        }

        row.update(
            stats_for_trades(
                trades,
                start,
                RESEARCH_TO,
            )
        )

        rows.append(row)

    return pd.DataFrame(rows)


def make_drawdown(
    trades
):
    if not trades:
        return pd.DataFrame([])

    equity = 0.0
    peak = 0.0
    peak_time = (
        trades[0]["signal_time"]
    )

    worst_dd = 0.0
    worst_start = None
    worst_end = None
    worst_recovery = None

    last_equity_high_time = (
        trades[0]["signal_time"]
    )

    longest_flat_days = 0.0
    longest_flat_start = None
    longest_flat_end = None

    in_worst_drawdown = False

    for trade in trades:
        equity += (
            trade["result_r"]
        )

        t = trade[
            "signal_time"
        ]

        if equity >= peak:
            if (
                in_worst_drawdown
                and worst_recovery is None
            ):
                worst_recovery = t
                in_worst_drawdown = False

            gap = (
                t
                - last_equity_high_time
            ).total_seconds() / 86400.0

            if gap > longest_flat_days:
                longest_flat_days = gap
                longest_flat_start = (
                    last_equity_high_time
                )
                longest_flat_end = t

            peak = equity
            peak_time = t
            last_equity_high_time = t

        else:
            dd = (
                equity
                - peak
            )

            if dd < worst_dd:
                worst_dd = dd
                worst_start = peak_time
                worst_end = t
                worst_recovery = None
                in_worst_drawdown = True

    return pd.DataFrame([
        {
            "max_drawdown_r": round(
                worst_dd,
                2,
            ),
            "max_drawdown_start": (
                worst_start.isoformat()
                if worst_start
                else None
            ),
            "max_drawdown_end": (
                worst_end.isoformat()
                if worst_end
                else None
            ),
            "max_drawdown_recovery": (
                worst_recovery.isoformat()
                if worst_recovery
                else None
            ),
            "longest_flat_days": round(
                longest_flat_days,
                1,
            ),
            "longest_flat_start": (
                longest_flat_start.isoformat()
                if longest_flat_start
                else None
            ),
            "longest_flat_end": (
                longest_flat_end.isoformat()
                if longest_flat_end
                else None
            ),
        }
    ])


def make_trade_log(
    trades
):
    rows = []

    for trade in trades:
        rows.append({
            "signal_time": (
                trade[
                    "signal_time"
                ].isoformat()
            ),
            "exit_time": (
                trade[
                    "exit_time"
                ].isoformat()
                if trade[
                    "exit_time"
                ] is not None
                else None
            ),
            "exit_reason": (
                trade[
                    "exit_reason"
                ]
            ),
            "reference_entry": round(
                trade[
                    "reference_entry"
                ],
                PRICE_PRECISION,
            ),
            "backtest_entry": round(
                trade[
                    "backtest_entry"
                ],
                PRICE_PRECISION,
            ),
            "stop": round(
                trade["stop"],
                PRICE_PRECISION,
            ),
            "target": round(
                trade["target"],
                PRICE_PRECISION,
            ),
            "result_r": round(
                trade["result_r"],
                6,
            ),
            "atr14": round(
                trade["atr14"],
                8,
            ),
            "body_ratio": round(
                trade[
                    "body_ratio"
                ],
                6,
            ),
            "lower_wick_body_ratio": round(
                trade[
                    "lower_wick_body_ratio"
                ],
                6,
            ),
            "structure_distance_atr": round(
                trade[
                    "structure_distance_atr"
                ],
                6,
            ),
            "daily_close": round(
                trade[
                    "daily_close"
                ],
                5,
            ),
            "daily_ema200": round(
                trade[
                    "daily_ema200"
                ],
                6,
            ),
            "ny_hour": (
                trade["ny_hour"]
            ),
            "weekday": (
                trade["weekday"]
            ),
        })

    return pd.DataFrame(rows)


# ============================================================
# RUN
# ============================================================

def run_research():
    try:
        STATUS.update({
            "state": (
                "fetching_h1"
            ),
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
            "state": (
                "fetching_daily"
            ),
            "message": (
                "Fetching USD/CAD daily history"
            ),
        })

        daily = fetch_chunked(
            "D",
            RESEARCH_FROM
            - timedelta(
                days=DAILY_WARMUP_DAYS
            ),
            RESEARCH_TO,
            DAILY_CHUNK_DAYS,
        )

        if not daily:
            raise RuntimeError(
                "No daily candles returned"
            )

        STATUS.update({
            "state": (
                "calculating"
            ),
            "message": (
                "Running exact current USD/CAD long control"
            ),
        })

        atr = atr_series(
            h1,
            ATR_LENGTH,
        )

        daily_state = (
            build_daily_state(
                daily
            )
        )

        simulation = simulate(
            h1,
            atr,
            daily_state,
        )

        trades = (
            simulation[
                "trades"
            ]
        )

        summary = make_summary(
            trades,
            simulation,
        )

        calendar = make_calendar(
            trades
        )

        rolling = make_rolling(
            trades
        )

        slices = make_slices(
            trades
        )

        recent = make_recent(
            trades
        )

        drawdowns = make_drawdown(
            trades
        )

        trade_log = make_trade_log(
            trades
        )

        summary.to_csv(
            SUMMARY_FILE,
            index=False,
        )

        calendar.to_csv(
            CALENDAR_FILE,
            index=False,
        )

        rolling.to_csv(
            ROLLING_FILE,
            index=False,
        )

        slices.to_csv(
            SLICES_FILE,
            index=False,
        )

        recent.to_csv(
            RECENT_FILE,
            index=False,
        )

        drawdowns.to_csv(
            DRAWDOWN_FILE,
            index=False,
        )

        trade_log.to_csv(
            TRADES_FILE,
            index=False,
        )

        output_files = [
            SUMMARY_FILE,
            CALENDAR_FILE,
            ROLLING_FILE,
            SLICES_FILE,
            RECENT_FILE,
            DRAWDOWN_FILE,
            TRADES_FILE,
        ]

        STATUS.update({
            "state": "complete",
            "message": (
                "USD/CAD current-live long control "
                "baseline completed"
            ),
            "orders_supported": False,
            "trading_enabled": False,
            "h1_candles_loaded": (
                len(h1)
            ),
            "daily_candles_loaded": (
                len(daily)
            ),
            "trades": (
                len(trades)
            ),
            "output_files": (
                output_files
            ),
            "summary": (
                summary.iloc[
                    0
                ].to_dict()
            ),
        })

        print()
        print("=" * 90)
        print(
            "USD/CAD LONG CURRENT CONTROL COMPLETE"
        )
        print("=" * 90)
        print(
            summary.to_string(
                index=False
            ),
            flush=True,
        )

    except Exception as error:
        STATUS.update({
            "state": "error",
            "message": str(error),
            "orders_supported": False,
            "trading_enabled": False,
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
            "USD/CAD Long Current Live Control Baseline"
        ),
        "status": STATUS,
        "instrument": INSTRUMENT,
        "direction": "LONG",
        "mode": "READ_ONLY_RESEARCH",
        "orders_supported": False,
        "trading_enabled": False,
        "current_live_rules": {
            "minimum_body_ratio": (
                MINIMUM_BODY_RATIO
            ),
            "lower_wick_body_ratio_min": (
                MINIMUM_LOWER_WICK_BODY_RATIO
            ),
            "atr_length": (
                ATR_LENGTH
            ),
            "structure_lookback": (
                STRUCTURE_LOOKBACK
            ),
            "maximum_distance_atr": (
                MAXIMUM_DISTANCE_ATR
            ),
            "daily_close_above_ema": (
                DAILY_EMA_LENGTH
            ),
            "excluded_ny_hours": sorted(
                EXCLUDED_NY_HOURS
            ),
            "reward_risk": (
                REWARD_RISK
            ),
            "stop_buffer_ticks": (
                STOP_BUFFER_TICKS
            ),
            "backtest_slippage_ticks": (
                BACKTEST_SLIPPAGE_TICKS
            ),
        },
        "downloads": {
            "summary": "/download/summary",
            "calendar": "/download/calendar",
            "rolling": "/download/rolling",
            "slices": "/download/slices",
            "recent": "/download/recent",
            "drawdowns": "/download/drawdowns",
            "trades": "/download/trades",
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
            "status": "not_ready",
            "message": (
                f"{filename} is not ready yet"
            ),
        }), 404

    return send_file(
        filename,
        as_attachment=True,
        download_name=filename,
    )


@app.route("/download/summary")
def download_summary():
    return download_file(
        SUMMARY_FILE
    )


@app.route("/download/calendar")
def download_calendar():
    return download_file(
        CALENDAR_FILE
    )


@app.route("/download/rolling")
def download_rolling():
    return download_file(
        ROLLING_FILE
    )


@app.route("/download/slices")
def download_slices():
    return download_file(
        SLICES_FILE
    )


@app.route("/download/recent")
def download_recent():
    return download_file(
        RECENT_FILE
    )


@app.route("/download/drawdowns")
def download_drawdowns():
    return download_file(
        DRAWDOWN_FILE
    )


@app.route("/download/trades")
def download_trades():
    return download_file(
        TRADES_FILE
    )


if __name__ == "__main__":
    research_thread = threading.Thread(
        target=run_research,
        name=(
            "usdcad-long-current-control-baseline"
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
