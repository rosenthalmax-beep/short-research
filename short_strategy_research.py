import os
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# EUR/GBP SHORT - FINAL 73 vs 77 HEAD-TO-HEAD VALIDATION
#
# RESEARCH ONLY — NEVER SUBMITS ORDERS.
#
# Purpose:
#   Final direct comparison of:
#
#   A) CURRENT_CONFIRMED
#      73-trade model
#
#   B) RELAXED_77
#      Slightly looser structure distance but tighter wick
#
# No further optimisation in this script.
#
# ============================================================
# SHARED FIXED ROBUST TRIGGER
#
# bearish engulfing
# body ratio >= 1.00
# structure lookback = 90
# range >= 1.10 ATR14
# close location <= 0.20
# 12h upward momentum >= 0.25 ATR14
# 48h upward momentum >= 0.40 ATR14
# stop size <= 2.50 ATR14
# exclude NY hour 09
#
# ============================================================
# A) CURRENT_CONFIRMED
#
# structure distance <= 0.075 ATR14
# 48h momentum >= 1.00 ATR14
# upper wick/body >= 0.10
# ATR14 / mean ATR14(50) >= 0.80
#
# ============================================================
# B) RELAXED_77
#
# structure distance <= 0.125 ATR14
# 48h momentum >= 1.00 ATR14
# upper wick/body >= 0.125
# ATR14 / mean ATR14(50) >= 0.80
#
# ============================================================
# EXECUTION
#
# OANDA EUR_GBP midpoint H1
# RR = 3.00
# stop = signal high + 10 ticks
# adverse short slippage = 5 ticks
# pyramiding = 0
#
# Same-bar target/stop:
# compare open->high vs open->low
# high closer => stop first
#
# signal_index < prior exit_index => ignore
# signal on exact H1 candle where prior trade exits is allowed
#
# ============================================================
# OUTPUTS
#
# eurgbp_short_73_vs_77_summary.csv
# eurgbp_short_73_vs_77_calendar_years.csv
# eurgbp_short_73_vs_77_rolling_3y.csv
# eurgbp_short_73_vs_77_slices.csv
# eurgbp_short_73_vs_77_recent_windows.csv
# eurgbp_short_73_vs_77_drawdowns.csv
# eurgbp_short_73_vs_77_overlap.csv
# eurgbp_short_73_vs_77_trade_log.csv
# ============================================================


app = Flask(__name__)

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_URL = "https://api-fxtrade.oanda.com"

INSTRUMENT = "EUR_GBP"
TICK_SIZE = 0.00001

STOP_BUFFER_TICKS = 10
BACKTEST_SLIPPAGE_TICKS = 5
REWARD_RISK = 3.00

MIN_BODY_RATIO = 1.00
STRUCTURE_LOOKBACK = 90

NY_TZ = ZoneInfo("America/New_York")
EXCLUDED_NY_HOURS = {9}

H1_CHUNK_DAYS = 180
H1_WARMUP_DAYS = 700

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

# ============================================================
# FIXED ROBUST TRIGGER
# ============================================================

ROBUST_MAX_DISTANCE_ATR = 0.15
ROBUST_MIN_RANGE_ATR = 1.10
ROBUST_MAX_CLOSE_LOCATION = 0.20
ROBUST_MIN_MOMENTUM_12 = 0.25
ROBUST_MIN_MOMENTUM_48 = 0.40
ROBUST_MAX_STOP_SIZE_ATR = 2.50

# ============================================================
# FINAL CANDIDATES
# ============================================================

CANDIDATES = {
    "CURRENT_CONFIRMED": {
        "max_distance_atr": 0.075,
        "min_momentum_48": 1.00,
        "min_upper_wick_body": 0.10,
        "min_atr_ratio_50": 0.80,
    },
    "RELAXED_77": {
        "max_distance_atr": 0.125,
        "min_momentum_48": 1.00,
        "min_upper_wick_body": 0.125,
        "min_atr_ratio_50": 0.80,
    },
}

SUMMARY_FILE = "eurgbp_short_73_vs_77_summary.csv"
CALENDAR_FILE = "eurgbp_short_73_vs_77_calendar_years.csv"
ROLLING_FILE = "eurgbp_short_73_vs_77_rolling_3y.csv"
SLICES_FILE = "eurgbp_short_73_vs_77_slices.csv"
RECENT_FILE = "eurgbp_short_73_vs_77_recent_windows.csv"
DRAWDOWN_FILE = "eurgbp_short_73_vs_77_drawdowns.csv"
OVERLAP_FILE = "eurgbp_short_73_vs_77_overlap.csv"
TRADE_LOG_FILE = "eurgbp_short_73_vs_77_trade_log.csv"

FIXED_SLICES = [
    (
        "first_half_2002_2013",
        RESEARCH_FROM,
        datetime(2014, 1, 1, tzinfo=timezone.utc),
    ),
    (
        "second_half_2014_present",
        datetime(2014, 1, 1, tzinfo=timezone.utc),
        None,
    ),
    (
        "2002_2009",
        RESEARCH_FROM,
        datetime(2010, 1, 1, tzinfo=timezone.utc),
    ),
    (
        "2010_2017",
        datetime(2010, 1, 1, tzinfo=timezone.utc),
        datetime(2018, 1, 1, tzinfo=timezone.utc),
    ),
    (
        "2018_2023",
        datetime(2018, 1, 1, tzinfo=timezone.utc),
        datetime(2024, 1, 1, tzinfo=timezone.utc),
    ),
    (
        "2024_present",
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        None,
    ),
]

STATUS = {
    "state": "not_started",
    "message": "Validation has not started",
    "service": "EURGBP Short Final 73 vs 77 Validation",
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "reward_risk": REWARD_RISK,
    "excluded_ny_hours": sorted(EXCLUDED_NY_HOURS),
    "output_files": [],
}


# ============================================================
# OANDA
# ============================================================

def headers():
    if not OANDA_TOKEN:
        raise RuntimeError("OANDA_TOKEN is not configured")
    return {"Authorization": f"Bearer {OANDA_TOKEN}"}


def iso_utc(dt):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


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
            candles_by_time[candle["time"]] = candle

        cursor = chunk_end

    candles = list(candles_by_time.values())

    candles.sort(
        key=lambda item: item["time"]
    )

    return candles


# ============================================================
# INDICATORS
# ============================================================

def true_ranges(candles):
    result = []

    for index, candle in enumerate(candles):
        if index == 0:
            tr = candle["high"] - candle["low"]
        else:
            previous_close = candles[index - 1]["close"]

            tr = max(
                candle["high"] - candle["low"],
                abs(candle["high"] - previous_close),
                abs(candle["low"] - previous_close),
            )

        result.append(tr)

    return result


def rma_series(
    values,
    length,
):
    result = [None] * len(values)

    if len(values) < length:
        return result

    initial = sum(values[:length]) / length
    result[length - 1] = initial

    previous = initial

    for index in range(length, len(values)):
        current = (
            previous * (length - 1)
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


def rolling_mean_optional(
    values,
    length,
):
    result = [None] * len(values)

    for index in range(
        length - 1,
        len(values),
    ):
        window = values[
            index - length + 1:
            index + 1
        ]

        if any(
            value is None
            for value in window
        ):
            continue

        result[index] = (
            sum(window)
            / length
        )

    return result


# ============================================================
# RAW SIGNALS
# ============================================================

def build_raw_candidates(
    h1,
    h1_atr,
    atr_mean_50,
):
    candidates = []

    max_lookback = max(
        STRUCTURE_LOOKBACK,
        48,
        50,
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

        if atr is None or atr <= 0:
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
            previous["close"] > previous["open"]
            and signal["close"] < signal["open"]
            and signal["open"] >= previous["close"]
            and signal["close"] <= previous["open"]
        )

        if not bearish_engulfing:
            continue

        body_ratio = (
            current_body
            / previous_body
        )

        if (
            body_ratio
            < MIN_BODY_RATIO
        ):
            continue

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

        range_atr = (
            candle_range
            / atr
        )

        close_location = (
            signal["close"]
            - signal["low"]
        ) / candle_range

        momentum_12 = (
            signal["close"]
            - h1[
                index - 12
            ]["close"]
        ) / atr

        momentum_48 = (
            signal["close"]
            - h1[
                index - 48
            ]["close"]
        ) / atr

        upper_wick = max(
            0.0,
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

        stop = (
            signal["high"]
            + STOP_BUFFER_TICKS
            * TICK_SIZE
        )

        stop_size_atr = (
            stop
            - signal["close"]
        ) / atr

        atr_ratio_50 = None

        if (
            atr_mean_50[index] is not None
            and atr_mean_50[index] > 0
        ):
            atr_ratio_50 = (
                atr
                / atr_mean_50[index]
            )

        ny_time = (
            signal["time"]
            .astimezone(NY_TZ)
        )

        candidates.append({
            "index": index,
            "time": signal["time"],
            "ny_hour": ny_time.hour,
            "body_ratio": body_ratio,
            "structure_distance_atr": structure_distance_atr,
            "range_atr": range_atr,
            "close_location": close_location,
            "momentum_12": momentum_12,
            "momentum_48": momentum_48,
            "upper_wick_body": upper_wick_body,
            "stop_size_atr": stop_size_atr,
            "atr_ratio_50": atr_ratio_50,
        })

    return candidates


# ============================================================
# FILTERS
# ============================================================

def passes_robust(
    signal,
):
    if (
        signal["ny_hour"]
        in EXCLUDED_NY_HOURS
    ):
        return False

    if (
        signal["structure_distance_atr"]
        > ROBUST_MAX_DISTANCE_ATR
    ):
        return False

    if (
        signal["range_atr"]
        < ROBUST_MIN_RANGE_ATR
    ):
        return False

    if (
        signal["close_location"]
        > ROBUST_MAX_CLOSE_LOCATION
    ):
        return False

    if (
        signal["momentum_12"]
        < ROBUST_MIN_MOMENTUM_12
    ):
        return False

    if (
        signal["momentum_48"]
        < ROBUST_MIN_MOMENTUM_48
    ):
        return False

    if (
        signal["stop_size_atr"]
        > ROBUST_MAX_STOP_SIZE_ATR
    ):
        return False

    return True


def passes_candidate(
    signal,
    rules,
):
    if not passes_robust(
        signal
    ):
        return False

    if (
        signal["structure_distance_atr"]
        > rules[
            "max_distance_atr"
        ]
    ):
        return False

    if (
        signal["momentum_48"]
        < rules[
            "min_momentum_48"
        ]
    ):
        return False

    if (
        signal["upper_wick_body"]
        < rules[
            "min_upper_wick_body"
        ]
    ):
        return False

    if (
        signal["atr_ratio_50"] is None
        or signal["atr_ratio_50"]
        < rules[
            "min_atr_ratio_50"
        ]
    ):
        return False

    return True


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
            "reference_entry": round(
                reference_entry,
                5,
            ),
            "backtest_entry": round(
                backtest_entry,
                5,
            ),
            "stop": round(
                stop,
                5,
            ),
            "target": round(
                target,
                5,
            ),
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
        "reference_entry": round(
            reference_entry,
            5,
        ),
        "backtest_entry": round(
            backtest_entry,
            5,
        ),
        "stop": round(
            stop,
            5,
        ),
        "target": round(
            target,
            5,
        ),
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
            trade["status"]
            == "OPEN"
        ):
            still_open = True
            break

        enriched = dict(trade)

        enriched.update({
            "signal_id": (
                signal["time"]
                .astimezone(timezone.utc)
                .isoformat()
            ),
            "ny_hour": (
                signal[
                    "ny_hour"
                ]
            ),
            "structure_distance_atr": (
                signal[
                    "structure_distance_atr"
                ]
            ),
            "range_atr": (
                signal[
                    "range_atr"
                ]
            ),
            "close_location": (
                signal[
                    "close_location"
                ]
            ),
            "momentum_12": (
                signal[
                    "momentum_12"
                ]
            ),
            "momentum_48": (
                signal[
                    "momentum_48"
                ]
            ),
            "upper_wick_body": (
                signal[
                    "upper_wick_body"
                ]
            ),
            "stop_size_atr": (
                signal[
                    "stop_size_atr"
                ]
            ),
            "atr_ratio_50": (
                signal[
                    "atr_ratio_50"
                ]
            ),
        })

        trades.append(
            enriched
        )

        position_exit_index = (
            trade[
                "exit_index"
            ]
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
            len(
                winners
            )
            / len(
                results
            )
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
            / len(
                results
            ),
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


def stats_row(
    strategy,
    label,
    start,
    end,
    trades,
):
    row = {
        "strategy": strategy,
        "label": label,
        "start": (
            start.isoformat()
            if start is not None
            else None
        ),
        "end": (
            end.isoformat()
            if end is not None
            else None
        ),
    }

    row.update(
        stats_for_trades(
            trades,
            start,
            end,
        )
    )

    return row


# ============================================================
# DRAWDOWN DIAGNOSTICS
# ============================================================

def drawdown_diagnostics(
    strategy,
    trades,
):
    if not trades:
        return {
            "strategy": strategy,
            "trades": 0,
            "max_drawdown_r": 0.0,
            "max_drawdown_start": None,
            "max_drawdown_end": None,
            "max_drawdown_recovery": None,
            "max_drawdown_duration_days": 0.0,
            "longest_time_between_equity_highs_days": 0.0,
            "longest_time_between_equity_highs_start": None,
            "longest_time_between_equity_highs_end": None,
        }

    equity = 0.0
    peak_equity = 0.0
    peak_time = (
        trades[0][
            "signal_time"
        ]
    )

    worst_dd = 0.0
    worst_dd_start = peak_time
    worst_dd_end = peak_time
    worst_dd_recovery = None

    active_dd = False
    active_dd_low = 0.0

    last_high_time = (
        trades[0][
            "signal_time"
        ]
    )

    longest_flat_days = 0.0
    longest_flat_start = None
    longest_flat_end = None

    for trade in trades:
        equity += (
            trade[
                "result_r"
            ]
        )

        t = trade[
            "signal_time"
        ]

        if (
            equity >= peak_equity
        ):
            if (
                active_dd
                and worst_dd_recovery is None
                and abs(
                    active_dd_low
                    - worst_dd
                ) < 1e-9
            ):
                worst_dd_recovery = t

            active_dd = False
            active_dd_low = 0.0

            gap_days = (
                t
                - last_high_time
            ).total_seconds() / 86400.0

            if (
                gap_days
                > longest_flat_days
            ):
                longest_flat_days = (
                    gap_days
                )

                longest_flat_start = (
                    last_high_time
                )

                longest_flat_end = t

            peak_equity = equity
            peak_time = t
            last_high_time = t

        else:
            dd = (
                equity
                - peak_equity
            )

            if not active_dd:
                active_dd = True
                active_dd_low = dd
            else:
                active_dd_low = min(
                    active_dd_low,
                    dd,
                )

            if dd < worst_dd:
                worst_dd = dd
                worst_dd_start = peak_time
                worst_dd_end = t
                worst_dd_recovery = None

    if active_dd:
        end_time = (
            trades[-1][
                "signal_time"
            ]
        )

        gap_days = (
            end_time
            - last_high_time
        ).total_seconds() / 86400.0

        if (
            gap_days
            > longest_flat_days
        ):
            longest_flat_days = gap_days
            longest_flat_start = last_high_time
            longest_flat_end = end_time

    dd_end = (
        worst_dd_recovery
        if worst_dd_recovery
        is not None
        else trades[-1][
            "signal_time"
        ]
    )

    dd_duration_days = (
        dd_end
        - worst_dd_start
    ).total_seconds() / 86400.0

    return {
        "strategy": strategy,
        "trades": len(
            trades
        ),
        "max_drawdown_r": round(
            worst_dd,
            2,
        ),
        "max_drawdown_start": (
            worst_dd_start.isoformat()
            if worst_dd_start is not None
            else None
        ),
        "max_drawdown_end": (
            worst_dd_end.isoformat()
            if worst_dd_end is not None
            else None
        ),
        "max_drawdown_recovery": (
            worst_dd_recovery.isoformat()
            if worst_dd_recovery is not None
            else None
        ),
        "max_drawdown_duration_days": round(
            dd_duration_days,
            1,
        ),
        "longest_time_between_equity_highs_days": round(
            longest_flat_days,
            1,
        ),
        "longest_time_between_equity_highs_start": (
            longest_flat_start.isoformat()
            if longest_flat_start is not None
            else None
        ),
        "longest_time_between_equity_highs_end": (
            longest_flat_end.isoformat()
            if longest_flat_end is not None
            else None
        ),
    }


# ============================================================
# OUTPUT BUILDERS
# ============================================================

def build_summary(
    trades_by_strategy,
):
    years = (
        RESEARCH_TO
        - RESEARCH_FROM
    ).total_seconds() / (
        365.2425
        * 86400
    )

    rows = []

    for strategy, trades in (
        trades_by_strategy.items()
    ):
        s = stats_for_trades(
            trades
        )

        rows.append({
            "strategy": strategy,
            "research_from": (
                RESEARCH_FROM.isoformat()
            ),
            "research_to": (
                RESEARCH_TO.isoformat()
            ),
            "reward_risk": (
                REWARD_RISK
            ),
            "excluded_ny_hours": (
                "09"
            ),
            "trades": (
                s[
                    "trades"
                ]
            ),
            "trades_per_year": round(
                s[
                    "trades"
                ]
                / years,
                2,
            ),
            "winners": (
                s[
                    "winners"
                ]
            ),
            "losers": (
                s[
                    "losers"
                ]
            ),
            "win_rate": (
                s[
                    "win_rate"
                ]
            ),
            "profit_factor": (
                s[
                    "profit_factor"
                ]
            ),
            "total_r": (
                s[
                    "total_r"
                ]
            ),
            "expectancy_r": (
                s[
                    "expectancy_r"
                ]
            ),
            "max_drawdown_r": (
                s[
                    "max_drawdown_r"
                ]
            ),
            "longest_loss_streak": (
                s[
                    "longest_loss_streak"
                ]
            ),
            "annual_r_linear": round(
                s[
                    "expectancy_r"
                ]
                * (
                    s[
                        "trades"
                    ]
                    / years
                ),
                3,
            ),
        })

    return pd.DataFrame(
        rows
    )


def build_calendar_years(
    trades_by_strategy,
):
    rows = []

    for strategy, trades in (
        trades_by_strategy.items()
    ):
        for year in range(
            RESEARCH_FROM.year,
            RESEARCH_TO.year + 1,
        ):
            start = datetime(
                year,
                1,
                1,
                tzinfo=timezone.utc,
            )

            end = datetime(
                year + 1,
                1,
                1,
                tzinfo=timezone.utc,
            )

            actual_start = max(
                start,
                RESEARCH_FROM,
            )

            actual_end = min(
                end,
                RESEARCH_TO,
            )

            if (
                actual_start
                >= actual_end
            ):
                continue

            rows.append(
                stats_row(
                    strategy,
                    str(year),
                    actual_start,
                    actual_end,
                    trades,
                )
            )

    return pd.DataFrame(
        rows
    )


def build_rolling_3y(
    trades_by_strategy,
):
    rows = []

    last_start_year = (
        RESEARCH_TO.year - 2
    )

    for strategy, trades in (
        trades_by_strategy.items()
    ):
        for start_year in range(
            2002,
            last_start_year + 1,
        ):
            start = datetime(
                start_year,
                1,
                1,
                tzinfo=timezone.utc,
            )

            end = datetime(
                start_year + 3,
                1,
                1,
                tzinfo=timezone.utc,
            )

            actual_start = max(
                start,
                RESEARCH_FROM,
            )

            actual_end = min(
                end,
                RESEARCH_TO,
            )

            if (
                actual_start
                >= actual_end
            ):
                continue

            row = stats_row(
                strategy,
                f"{start_year}_{start_year + 2}",
                actual_start,
                actual_end,
                trades,
            )

            span_years = (
                actual_end
                - actual_start
            ).total_seconds() / (
                365.2425
                * 86400
            )

            row[
                "trades_per_year"
            ] = round(
                row[
                    "trades"
                ]
                / span_years,
                2,
            )

            rows.append(
                row
            )

    return pd.DataFrame(
        rows
    )


def build_slices(
    trades_by_strategy,
):
    rows = []

    for strategy, trades in (
        trades_by_strategy.items()
    ):
        for (
            label,
            start,
            end,
        ) in FIXED_SLICES:
            actual_end = (
                RESEARCH_TO
                if end is None
                else min(
                    end,
                    RESEARCH_TO,
                )
            )

            rows.append(
                stats_row(
                    strategy,
                    label,
                    start,
                    actual_end,
                    trades,
                )
            )

    return pd.DataFrame(
        rows
    )


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


def build_recent_windows(
    trades_by_strategy,
):
    rows = []

    for strategy, trades in (
        trades_by_strategy.items()
    ):
        for years_back in [
            2,
            5,
            10,
        ]:
            start = subtract_years_safe(
                RESEARCH_TO,
                years_back,
            )

            row = stats_row(
                strategy,
                f"last_{years_back}_years",
                start,
                RESEARCH_TO,
                trades,
            )

            row[
                "trades_per_year"
            ] = round(
                row[
                    "trades"
                ]
                / years_back,
                2,
            )

            rows.append(
                row
            )

    return pd.DataFrame(
        rows
    )


def build_drawdowns(
    trades_by_strategy,
):
    rows = []

    for strategy, trades in (
        trades_by_strategy.items()
    ):
        rows.append(
            drawdown_diagnostics(
                strategy,
                trades,
            )
        )

    return pd.DataFrame(
        rows
    )


def build_overlap(
    trades_by_strategy,
):
    current = (
        trades_by_strategy[
            "CURRENT_CONFIRMED"
        ]
    )

    relaxed = (
        trades_by_strategy[
            "RELAXED_77"
        ]
    )

    current_map = {
        trade[
            "signal_id"
        ]: trade
        for trade
        in current
    }

    relaxed_map = {
        trade[
            "signal_id"
        ]: trade
        for trade
        in relaxed
    }

    current_ids = set(
        current_map.keys()
    )

    relaxed_ids = set(
        relaxed_map.keys()
    )

    shared_ids = (
        current_ids
        & relaxed_ids
    )

    current_only = (
        current_ids
        - relaxed_ids
    )

    relaxed_only = (
        relaxed_ids
        - current_ids
    )

    def result_stats(
        ids,
        source_map,
    ):
        results = [
            source_map[
                signal_id
            ][
                "result_r"
            ]
            for signal_id
            in sorted(
                ids
            )
        ]

        if not results:
            return {
                "count": 0,
                "total_r": 0.0,
                "expectancy_r": 0.0,
                "win_rate": 0.0,
            }

        winners = [
            r
            for r in results
            if r > 0
        ]

        return {
            "count": len(
                results
            ),
            "total_r": round(
                sum(
                    results
                ),
                2,
            ),
            "expectancy_r": round(
                sum(
                    results
                )
                / len(
                    results
                ),
                3,
            ),
            "win_rate": round(
                len(
                    winners
                )
                / len(
                    results
                )
                * 100.0,
                2,
            ),
        }

    shared_stats = (
        result_stats(
            shared_ids,
            current_map,
        )
    )

    current_only_stats = (
        result_stats(
            current_only,
            current_map,
        )
    )

    relaxed_only_stats = (
        result_stats(
            relaxed_only,
            relaxed_map,
        )
    )

    jaccard = (
        len(
            shared_ids
        )
        / len(
            current_ids
            | relaxed_ids
        )
        if (
            current_ids
            | relaxed_ids
        )
        else 0.0
    )

    return pd.DataFrame([
        {
            "current_confirmed_trades": (
                len(
                    current_ids
                )
            ),
            "relaxed_77_trades": (
                len(
                    relaxed_ids
                )
            ),
            "shared_trades": (
                len(
                    shared_ids
                )
            ),
            "current_only_trades": (
                len(
                    current_only
                )
            ),
            "relaxed_only_trades": (
                len(
                    relaxed_only
                )
            ),
            "shared_pct_of_current": round(
                len(
                    shared_ids
                )
                / len(
                    current_ids
                )
                * 100.0
                if current_ids
                else 0.0,
                2,
            ),
            "shared_pct_of_relaxed": round(
                len(
                    shared_ids
                )
                / len(
                    relaxed_ids
                )
                * 100.0
                if relaxed_ids
                else 0.0,
                2,
            ),
            "jaccard_overlap": round(
                jaccard,
                3,
            ),
            "shared_total_r": (
                shared_stats[
                    "total_r"
                ]
            ),
            "shared_expectancy_r": (
                shared_stats[
                    "expectancy_r"
                ]
            ),
            "shared_win_rate": (
                shared_stats[
                    "win_rate"
                ]
            ),
            "current_only_total_r": (
                current_only_stats[
                    "total_r"
                ]
            ),
            "current_only_expectancy_r": (
                current_only_stats[
                    "expectancy_r"
                ]
            ),
            "current_only_win_rate": (
                current_only_stats[
                    "win_rate"
                ]
            ),
            "relaxed_only_total_r": (
                relaxed_only_stats[
                    "total_r"
                ]
            ),
            "relaxed_only_expectancy_r": (
                relaxed_only_stats[
                    "expectancy_r"
                ]
            ),
            "relaxed_only_win_rate": (
                relaxed_only_stats[
                    "win_rate"
                ]
            ),
        }
    ])


def build_trade_log(
    trades_by_strategy,
):
    rows = []

    for strategy, trades in (
        trades_by_strategy.items()
    ):
        for trade in trades:
            rows.append({
                "strategy": (
                    strategy
                ),
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
                "result_r": round(
                    trade[
                        "result_r"
                    ],
                    4,
                ),
                "ny_hour": (
                    trade[
                        "ny_hour"
                    ]
                ),
                "structure_distance_atr": round(
                    trade[
                        "structure_distance_atr"
                    ],
                    4,
                ),
                "range_atr": round(
                    trade[
                        "range_atr"
                    ],
                    4,
                ),
                "close_location": round(
                    trade[
                        "close_location"
                    ],
                    4,
                ),
                "momentum_12_atr": round(
                    trade[
                        "momentum_12"
                    ],
                    4,
                ),
                "momentum_48_atr": round(
                    trade[
                        "momentum_48"
                    ],
                    4,
                ),
                "upper_wick_body": round(
                    trade[
                        "upper_wick_body"
                    ],
                    4,
                ),
                "stop_size_atr": round(
                    trade[
                        "stop_size_atr"
                    ],
                    4,
                ),
                "atr_ratio_50": round(
                    trade[
                        "atr_ratio_50"
                    ],
                    4,
                )
                if trade[
                    "atr_ratio_50"
                ] is not None
                else None,
            })

    return pd.DataFrame(
        rows
    )


# ============================================================
# RUN
# ============================================================

def run_validation():
    global STATUS

    try:
        print()
        print("=" * 88)
        print(
            "EUR/GBP SHORT - FINAL 73 vs 77 HEAD-TO-HEAD"
        )
        print("=" * 88)
        print()

        STATUS.update({
            "state": "fetching_data",
            "message": (
                "Fetching EUR/GBP OANDA H1 history"
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

        if not h1:
            raise RuntimeError(
                "No EUR/GBP H1 candles returned"
            )

        STATUS.update({
            "state": "precomputing",
            "message": (
                "Building ATR14 and fixed signals"
            ),
        })

        h1_atr = atr_series(
            h1,
            14,
        )

        atr_mean_50 = (
            rolling_mean_optional(
                h1_atr,
                50,
            )
        )

        raw_candidates = (
            build_raw_candidates(
                h1,
                h1_atr,
                atr_mean_50,
            )
        )

        trades_by_strategy = {}

        STATUS[
            "raw_bearish_engulfing_signals"
        ] = len(
            raw_candidates
        )

        for (
            strategy,
            rules,
        ) in CANDIDATES.items():
            eligible = [
                signal
                for signal
                in raw_candidates
                if passes_candidate(
                    signal,
                    rules,
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

            trades_by_strategy[
                strategy
            ] = trades

            STATUS[
                f"{strategy.lower()}_eligible_signals"
            ] = len(
                eligible
            )

            STATUS[
                f"{strategy.lower()}_trades"
            ] = len(
                trades
            )

            STATUS[
                f"{strategy.lower()}_ignored_due_to_open_trade"
            ] = ignored

            STATUS[
                f"{strategy.lower()}_still_open_at_end"
            ] = still_open

            print(
                f"{strategy}: "
                f"{len(trades)} trades",
                flush=True,
            )

        STATUS.update({
            "state": "building_outputs",
            "message": (
                "Building final head-to-head outputs"
            ),
        })

        summary_df = (
            build_summary(
                trades_by_strategy
            )
        )

        calendar_df = (
            build_calendar_years(
                trades_by_strategy
            )
        )

        rolling_df = (
            build_rolling_3y(
                trades_by_strategy
            )
        )

        slices_df = (
            build_slices(
                trades_by_strategy
            )
        )

        recent_df = (
            build_recent_windows(
                trades_by_strategy
            )
        )

        drawdown_df = (
            build_drawdowns(
                trades_by_strategy
            )
        )

        overlap_df = (
            build_overlap(
                trades_by_strategy
            )
        )

        trade_log_df = (
            build_trade_log(
                trades_by_strategy
            )
        )

        summary_df.to_csv(
            SUMMARY_FILE,
            index=False,
        )

        calendar_df.to_csv(
            CALENDAR_FILE,
            index=False,
        )

        rolling_df.to_csv(
            ROLLING_FILE,
            index=False,
        )

        slices_df.to_csv(
            SLICES_FILE,
            index=False,
        )

        recent_df.to_csv(
            RECENT_FILE,
            index=False,
        )

        drawdown_df.to_csv(
            DRAWDOWN_FILE,
            index=False,
        )

        overlap_df.to_csv(
            OVERLAP_FILE,
            index=False,
        )

        trade_log_df.to_csv(
            TRADE_LOG_FILE,
            index=False,
        )

        output_files = [
            SUMMARY_FILE,
            CALENDAR_FILE,
            ROLLING_FILE,
            SLICES_FILE,
            RECENT_FILE,
            DRAWDOWN_FILE,
            OVERLAP_FILE,
            TRADE_LOG_FILE,
        ]

        STATUS.update({
            "state": "complete",
            "message": (
                "EUR/GBP 73 vs 77 final validation "
                "completed successfully"
            ),
            "output_files": (
                output_files
            ),
        })

        print()
        print(
            summary_df.to_string(
                index=False
            ),
            flush=True,
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
            "EURGBP Short Final 73 vs 77 Validation"
        ),
        "status": (
            STATUS
        ),
        "instrument": (
            INSTRUMENT
        ),
        "direction": (
            "SHORT"
        ),
        "reward_risk": (
            REWARD_RISK
        ),
        "timezone": (
            "America/New_York"
        ),
        "timing_basis": (
            "signal candle open time"
        ),
        "excluded_ny_hours": sorted(
            EXCLUDED_NY_HOURS
        ),
        "trading_enabled": False,
        "orders_supported": False,
        "executor_connected": False,
        "candidates": (
            CANDIDATES
        ),
        "downloads": {
            "summary": "/download/summary",
            "calendar": "/download/calendar",
            "rolling": "/download/rolling",
            "slices": "/download/slices",
            "recent": "/download/recent",
            "drawdowns": "/download/drawdowns",
            "overlap": "/download/overlap",
            "trades": "/download/trades",
        },
    })


@app.route("/status")
def status():
    return jsonify(
        STATUS
    )


def file_download(
    filename,
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
    return file_download(
        SUMMARY_FILE
    )


@app.route("/download/calendar")
def download_calendar():
    return file_download(
        CALENDAR_FILE
    )


@app.route("/download/rolling")
def download_rolling():
    return file_download(
        ROLLING_FILE
    )


@app.route("/download/slices")
def download_slices():
    return file_download(
        SLICES_FILE
    )


@app.route("/download/recent")
def download_recent():
    return file_download(
        RECENT_FILE
    )


@app.route("/download/drawdowns")
def download_drawdowns():
    return file_download(
        DRAWDOWN_FILE
    )


@app.route("/download/overlap")
def download_overlap():
    return file_download(
        OVERLAP_FILE
    )


@app.route("/download/trades")
def download_trades():
    return file_download(
        TRADE_LOG_FILE
    )


if __name__ == "__main__":
    validation_thread = (
        threading.Thread(
            target=run_validation,
            name=(
                "eurgbp-short-final-73-vs-77"
            ),
            daemon=True,
        )
    )

    validation_thread.start()

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
