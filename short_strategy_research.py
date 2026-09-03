import os
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# GBP/USD LONG - FINAL VALIDATION
#
# RESEARCH ONLY - NEVER SUBMITS ORDERS.
#
# CURRENT LIVE CONTROL
# ------------------------------------------------------------
# exact bullish engulfing
# body ratio >= 1.40
# strong close >= 0.65
# structure lookback 20
# structure distance <= 0.25 ATR14
# signal range >= 0.90 ATR14
# previous completed daily close > EMA70
# previous completed daily EMA50 > EMA70
# exclude NY 14:00-18:59
# no weekday exclusions
# RR 4.25
#
# CANDIDATE C
# ------------------------------------------------------------
# exact bullish engulfing
# body ratio >= 1.20
# body >= 1.10 ATR14
# no strong-close filter
# structure lookback 45
# structure distance <= 0.15 ATR14
# no range filter
# no daily EMA regime
# no daily EMA alignment
# exclude NY 14:00-18:59
# no weekday exclusions
# RR 4.25
#
# ============================================================
# LOCKED EXECUTION CONVENTIONS
#
# OANDA midpoint H1 candles.
#
# ATR14 = Wilder/RMA, SMA-seeded.
#
# GBP/USD tick size = 0.00001.
#
# Reference entry = signal close.
# Historical adverse long fill =
#       signal close + 5 ticks.
#
# Stop =
#       signal low - 10 ticks.
#
# Target based on REFERENCE signal-close risk:
#       target = signal close
#              + (signal close - stop) * RR
#
# Actual R uses adverse backtest fill.
#
# Pyramiding = 0.
#
# Same-bar target/stop tie for LONG:
#   compare candle open->high vs open->low
#   high closer => target first
#   otherwise stop first.
#
# Signals with signal_index < position_exit_index are ignored.
# Signal on exact exit candle is allowed.
#
# Exit checks begin signal_index + 1.
#
# Daily candles:
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

INSTRUMENT = "GBP_USD"

TICK_SIZE = 0.00001
STOP_BUFFER_TICKS = 10
BACKTEST_SLIPPAGE_TICKS = 5
REWARD_RISK = 4.25

ATR_LENGTH = 14

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

OUTPUT_SUMMARY = (
    "gbpusd_long_final_validation_summary.csv"
)

OUTPUT_YEARLY = (
    "gbpusd_long_final_validation_yearly.csv"
)

OUTPUT_ROLLING = (
    "gbpusd_long_final_validation_rolling3y.csv"
)

OUTPUT_TRADES = (
    "gbpusd_long_final_validation_trades.csv"
)

OUTPUT_OVERLAP = (
    "gbpusd_long_final_validation_overlap.csv"
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


STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "service": (
        "GBP/USD Long Final Validation"
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
):
    return rma_series(
        true_ranges(candles),
        ATR_LENGTH,
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

    ema50 = ema_series(
        closes,
        50,
    )

    ema70 = ema_series(
        closes,
        70,
    )

    rows = []

    for index, candle in enumerate(
        daily
    ):
        rows.append({
            "time":
                candle["time"],
            "close":
                candle["close"],
            "ema50":
                ema50[index],
            "ema70":
                ema70[index],
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
# SIGNAL BUILDERS
# ============================================================

def common_engulfing_features(
    h1,
    atr,
    index,
):
    signal = h1[index]
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
        return None

    ny = (
        signal["time"]
        .astimezone(
            NY_TZ
        )
    )

    return {
        "body_ratio":
            current_body
            / previous_body,
        "close_location":
            (
                signal["close"]
                - signal["low"]
            ) / signal_range,
        "body_atr":
            current_body
            / current_atr,
        "range_atr":
            signal_range
            / current_atr,
        "current_atr":
            current_atr,
        "ny_hour":
            ny.hour,
        "ny_weekday":
            ny.weekday(),
    }


def build_current_live_signals(
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

        f = common_engulfing_features(
            h1,
            atr,
            index,
        )

        if f is None:
            continue

        if (
            f["body_ratio"]
            < 1.40
        ):
            continue

        if (
            f["close_location"]
            < 0.65
        ):
            continue

        if (
            f["range_atr"]
            < 0.90
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
        ) / f[
            "current_atr"
        ]

        if (
            structure_distance
            > 0.25
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

        if (
            daily["ema50"] is None
            or daily["ema70"] is None
        ):
            continue

        if not (
            daily["close"]
            > daily["ema70"]
        ):
            continue

        if not (
            daily["ema50"]
            > daily["ema70"]
        ):
            continue

        if (
            f["ny_hour"] >= 14
            and f["ny_hour"] < 19
        ):
            continue

        eligible.append({
            "index":
                index,
            "time":
                signal["time"],
            "body_ratio":
                f["body_ratio"],
            "close_location":
                f["close_location"],
            "body_atr":
                f["body_atr"],
            "range_atr":
                f["range_atr"],
            "structure_distance_atr":
                structure_distance,
            "daily_close":
                daily["close"],
            "daily_ema50":
                daily["ema50"],
            "daily_ema70":
                daily["ema70"],
            "session_hour":
                f["ny_hour"],
            "session_weekday":
                f["ny_weekday"],
        })

    return eligible


def build_candidate_c_signals(
    h1,
    atr,
):
    eligible = []

    start_index = max(
        ATR_LENGTH,
        45,
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

        f = common_engulfing_features(
            h1,
            atr,
            index,
        )

        if f is None:
            continue

        if (
            f["body_ratio"]
            < 1.20
        ):
            continue

        if (
            f["body_atr"]
            < 1.10
        ):
            continue

        previous_lowest = min(
            candle["low"]
            for candle
            in h1[
                index - 45:
                index
            ]
        )

        structure_distance = (
            signal["low"]
            - previous_lowest
        ) / f[
            "current_atr"
        ]

        if (
            structure_distance
            > 0.15
        ):
            continue

        if (
            f["ny_hour"] >= 14
            and f["ny_hour"] < 19
        ):
            continue

        eligible.append({
            "index":
                index,
            "time":
                signal["time"],
            "body_ratio":
                f["body_ratio"],
            "close_location":
                f["close_location"],
            "body_atr":
                f["body_atr"],
            "range_atr":
                f["range_atr"],
            "structure_distance_atr":
                structure_distance,
            "daily_close":
                None,
            "daily_ema50":
                None,
            "daily_ema70":
                None,
            "session_hour":
                f["ny_hour"],
            "session_weekday":
                f["ny_weekday"],
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
                exit_reason = (
                    "TARGET_TIE_FIRST"
                )
            else:
                exit_price = stop
                exit_reason = (
                    "STOP_TIE_FIRST"
                )

        elif target_hit:
            exit_price = target
            exit_reason = "TARGET"

        else:
            exit_price = stop
            exit_reason = "STOP"

        result = {
            "signal_index":
                signal_index,
            "signal_time":
                signal["time"],
            "exit_index":
                index,
            "exit_time":
                candle["time"],
            "entry_reference":
                reference_entry,
            "backtest_entry":
                backtest_entry,
            "stop":
                stop,
            "target":
                target,
            "exit_price":
                exit_price,
            "exit_reason":
                exit_reason,
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
    strategy_name,
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

        row = dict(trade)

        row[
            "strategy"
        ] = strategy_name

        for key in [
            "body_ratio",
            "close_location",
            "body_atr",
            "range_atr",
            "structure_distance_atr",
            "daily_close",
            "daily_ema50",
            "daily_ema70",
            "session_hour",
            "session_weekday",
        ]:
            row[key] = signal.get(
                key
            )

        trades.append(row)

        position_exit_index = (
            row["exit_index"]
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
        t = trade["signal_time"]

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

    gross_profit = sum(winners)
    gross_loss = abs(sum(losers))

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
            year=dt.year - years
        )
    except ValueError:
        return dt.replace(
            month=2,
            day=28,
            year=dt.year - years,
        )


def build_summary_row(
    strategy_name,
    trades,
    ignored,
):
    years = (
        RESEARCH_TO
        - RESEARCH_FROM
    ).total_seconds() / (
        365.2425
        * 86400
    )

    full = stats_for_trades(
        trades
    )

    row = {
        "strategy":
            strategy_name,
        "research_from":
            RESEARCH_FROM.isoformat(),
        "research_to":
            RESEARCH_TO.isoformat(),
        "trades":
            full["trades"],
        "ignored_due_to_open_trade":
            ignored,
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
            full[
                "longest_loss_streak"
            ],
    }

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
        ] = stats[
            "profit_factor"
        ]

        row[
            f"{era_name}_r"
        ] = stats["total_r"]

        row[
            f"{era_name}_expectancy"
        ] = stats[
            "expectancy_r"
        ]

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
                stats["total_r"]
                > 0
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
        ] = stats[
            "profit_factor"
        ]

        row[
            f"last_{years_back}y_r"
        ] = stats["total_r"]

        row[
            f"last_{years_back}y_expectancy"
        ] = stats[
            "expectancy_r"
        ]

    return row


def build_yearly_rows(
    strategy_name,
    trades,
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

        stats = stats_for_trades(
            trades,
            start,
            end,
        )

        rows.append({
            "strategy":
                strategy_name,
            "year":
                year,
            **stats,
        })

    return rows


def build_rolling_rows(
    strategy_name,
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

        rows.append({
            "strategy":
                strategy_name,
            "window":
                f"{start_year}_"
                f"{start_year + 2}",
            "start":
                start.isoformat(),
            "end":
                end.isoformat(),
            **stats,
        })

    return rows


# ============================================================
# OVERLAP / EXCLUSIVE
#
# NOTE:
# This compares EXECUTED simulated trades after each strategy's
# own pyramiding-0 handling. Therefore "shared/current-only/
# candidate-only" means executed-trade overlap, not merely
# raw eligible-signal overlap.
# ============================================================

def overlap_analysis(
    current_trades,
    candidate_trades,
):
    current_map = {
        trade["signal_time"]:
            trade
        for trade
        in current_trades
    }

    candidate_map = {
        trade["signal_time"]:
            trade
        for trade
        in candidate_trades
    }

    current_times = set(
        current_map
    )

    candidate_times = set(
        candidate_map
    )

    shared_times = sorted(
        current_times
        & candidate_times
    )

    current_only_times = sorted(
        current_times
        - candidate_times
    )

    candidate_only_times = sorted(
        candidate_times
        - current_times
    )

    groups = [
        (
            "SHARED_CURRENT",
            [
                current_map[t]
                for t in shared_times
            ],
        ),
        (
            "SHARED_CANDIDATE",
            [
                candidate_map[t]
                for t in shared_times
            ],
        ),
        (
            "CURRENT_ONLY",
            [
                current_map[t]
                for t in current_only_times
            ],
        ),
        (
            "CANDIDATE_ONLY",
            [
                candidate_map[t]
                for t in candidate_only_times
            ],
        ),
    ]

    rows = []

    for (
        group_name,
        trades,
    ) in groups:
        stats = stats_for_trades(
            trades
        )

        rows.append({
            "group":
                group_name,
            **stats,
        })

    return pd.DataFrame(
        rows
    )


# ============================================================
# TRADE EXPORT
# ============================================================

def trades_dataframe(
    all_trades,
):
    rows = []

    for trade in all_trades:
        rows.append({
            "strategy":
                trade["strategy"],
            "signal_time":
                trade[
                    "signal_time"
                ].isoformat(),
            "exit_time":
                trade[
                    "exit_time"
                ].isoformat(),
            "signal_index":
                trade[
                    "signal_index"
                ],
            "exit_index":
                trade[
                    "exit_index"
                ],
            "entry_reference":
                round(
                    trade[
                        "entry_reference"
                    ],
                    6,
                ),
            "backtest_entry":
                round(
                    trade[
                        "backtest_entry"
                    ],
                    6,
                ),
            "stop":
                round(
                    trade["stop"],
                    6,
                ),
            "target":
                round(
                    trade["target"],
                    6,
                ),
            "exit_price":
                round(
                    trade[
                        "exit_price"
                    ],
                    6,
                ),
            "exit_reason":
                trade[
                    "exit_reason"
                ],
            "result_r":
                round(
                    trade["result_r"],
                    6,
                ),
            "body_ratio":
                (
                    round(
                        trade[
                            "body_ratio"
                        ],
                        6,
                    )
                    if trade.get(
                        "body_ratio"
                    ) is not None
                    else None
                ),
            "close_location":
                (
                    round(
                        trade[
                            "close_location"
                        ],
                        6,
                    )
                    if trade.get(
                        "close_location"
                    ) is not None
                    else None
                ),
            "body_atr":
                (
                    round(
                        trade[
                            "body_atr"
                        ],
                        6,
                    )
                    if trade.get(
                        "body_atr"
                    ) is not None
                    else None
                ),
            "range_atr":
                (
                    round(
                        trade[
                            "range_atr"
                        ],
                        6,
                    )
                    if trade.get(
                        "range_atr"
                    ) is not None
                    else None
                ),
            "structure_distance_atr":
                (
                    round(
                        trade[
                            "structure_distance_atr"
                        ],
                        6,
                    )
                    if trade.get(
                        "structure_distance_atr"
                    ) is not None
                    else None
                ),
            "daily_close":
                trade.get(
                    "daily_close"
                ),
            "daily_ema50":
                trade.get(
                    "daily_ema50"
                ),
            "daily_ema70":
                trade.get(
                    "daily_ema70"
                ),
            "session_hour":
                trade.get(
                    "session_hour"
                ),
            "session_weekday":
                trade.get(
                    "session_weekday"
                ),
        })

    return pd.DataFrame(rows)


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
                "Calculating indicators and signals",
        })

        atr = atr_series(
            h1
        )

        daily_state = (
            prepare_daily(
                daily
            )
        )

        current_signals = (
            build_current_live_signals(
                h1,
                atr,
                daily_state,
            )
        )

        candidate_signals = (
            build_candidate_c_signals(
                h1,
                atr,
            )
        )

        STATUS.update({
            "state":
                "simulating",
            "message":
                "Simulating current live and Candidate C",
            "current_eligible_signals":
                len(
                    current_signals
                ),
            "candidate_eligible_signals":
                len(
                    candidate_signals
                ),
        })

        (
            current_trades,
            current_ignored,
        ) = simulate_variant(
            h1,
            current_signals,
            "CURRENT_LIVE",
        )

        (
            candidate_trades,
            candidate_ignored,
        ) = simulate_variant(
            h1,
            candidate_signals,
            "CANDIDATE_C",
        )

        summary = pd.DataFrame([
            build_summary_row(
                "CURRENT_LIVE",
                current_trades,
                current_ignored,
            ),
            build_summary_row(
                "CANDIDATE_C",
                candidate_trades,
                candidate_ignored,
            ),
        ])

        yearly = pd.DataFrame(
            build_yearly_rows(
                "CURRENT_LIVE",
                current_trades,
            )
            +
            build_yearly_rows(
                "CANDIDATE_C",
                candidate_trades,
            )
        )

        rolling = pd.DataFrame(
            build_rolling_rows(
                "CURRENT_LIVE",
                current_trades,
            )
            +
            build_rolling_rows(
                "CANDIDATE_C",
                candidate_trades,
            )
        )

        trades = trades_dataframe(
            current_trades
            + candidate_trades
        )

        overlap = overlap_analysis(
            current_trades,
            candidate_trades,
        )

        summary.to_csv(
            os.path.abspath(
                OUTPUT_SUMMARY
            ),
            index=False,
        )

        yearly.to_csv(
            os.path.abspath(
                OUTPUT_YEARLY
            ),
            index=False,
        )

        rolling.to_csv(
            os.path.abspath(
                OUTPUT_ROLLING
            ),
            index=False,
        )

        trades.to_csv(
            os.path.abspath(
                OUTPUT_TRADES
            ),
            index=False,
        )

        overlap.to_csv(
            os.path.abspath(
                OUTPUT_OVERLAP
            ),
            index=False,
        )

        STATUS.update({
            "state":
                "complete",
            "message":
                "GBP/USD final validation complete",
            "current_trades":
                len(current_trades),
            "candidate_trades":
                len(candidate_trades),
            "outputs": {
                "summary":
                    OUTPUT_SUMMARY,
                "yearly":
                    OUTPUT_YEARLY,
                "rolling":
                    OUTPUT_ROLLING,
                "trades":
                    OUTPUT_TRADES,
                "overlap":
                    OUTPUT_OVERLAP,
            },
        })

        print()
        print("=" * 100)
        print(
            "GBP/USD LONG FINAL VALIDATION COMPLETE"
        )
        print("=" * 100)
        print(
            summary.to_string(
                index=False
            ),
            flush=True,
        )
        print()
        print(
            "EXECUTED-TRADE OVERLAP"
        )
        print(
            overlap.to_string(
                index=False
            ),
            flush=True,
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
            "GBP/USD Long Final Validation",
        "status":
            STATUS,
        "mode":
            "READ_ONLY_RESEARCH",
        "orders_supported":
            False,
        "trading_enabled":
            False,
        "candidate_c": {
            "minimum_body_ratio":
                1.20,
            "minimum_body_atr":
                1.10,
            "strong_close_enabled":
                False,
            "structure_lookback":
                45,
            "maximum_distance_atr":
                0.15,
            "minimum_range_enabled":
                False,
            "daily_regime":
                None,
            "daily_alignment":
                None,
            "session":
                "exclude NY 14:00-18:59",
            "reward_risk":
                4.25,
            "stop_buffer_ticks":
                10,
        },
        "downloads": {
            "summary":
                "/download/summary",
            "yearly":
                "/download/yearly",
            "rolling":
                "/download/rolling",
            "trades":
                "/download/trades",
            "overlap":
                "/download/overlap",
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
    "/download/summary"
)
def download_summary():
    return send_output(
        OUTPUT_SUMMARY
    )


@app.route(
    "/download/yearly"
)
def download_yearly():
    return send_output(
        OUTPUT_YEARLY
    )


@app.route(
    "/download/rolling"
)
def download_rolling():
    return send_output(
        OUTPUT_ROLLING
    )


@app.route(
    "/download/trades"
)
def download_trades():
    return send_output(
        OUTPUT_TRADES
    )


@app.route(
    "/download/overlap"
)
def download_overlap():
    return send_output(
        OUTPUT_OVERLAP
    )


if __name__ == "__main__":
    thread = threading.Thread(
        target=run_research,
        name=(
            "gbpusd-long-final-validation"
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
