import os
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# EUR/GBP SHORT - FINAL DUAL-CANDIDATE VALIDATION
#
# RESEARCH ONLY — NEVER SUBMITS ORDERS.
#
# Purpose:
#   Compare the two final EUR/GBP short candidates without
#   changing any parameters.
#
# Candidate A — ROBUST
#   bearish engulfing
#   body ratio >= 1.00
#   structure lookback = 90
#   distance <= 0.15 ATR14
#   range >= 1.10 ATR14
#   close location <= 0.20
#   12h upward momentum >= 0.25 ATR14
#   48h upward momentum >= 0.40 ATR14
#   stop size <= 2.50 ATR14
#   exclude NY hour 09
#   RR = 3.00
#
# Candidate B — HIGH_PF
#   bearish engulfing
#   body ratio >= 1.00
#   structure lookback = 90
#   distance <= 0.075 ATR14
#   range >= 1.00 ATR14
#   close location <= 0.225
#   48h upward momentum >= 1.00 ATR14
#   upper wick/body >= 0.10
#   ATR14 / mean(ATR14, 50) >= 0.80
#   exclude NY hour 09
#   RR = 3.00
#
# Shared execution:
#   OANDA EUR_GBP midpoint H1
#   stop = signal high + 10 ticks
#   adverse short slippage = 5 ticks
#   pyramiding = 0
#   same-bar stop/target tie:
#       compare candle open->high vs open->low
#       high closer => stop first
#
# Validation outputs:
#   1) summary
#   2) calendar-year stats
#   3) rolling 3-year windows
#   4) fixed historical slices
#   5) recent 2/5/10-year windows
#   6) drawdown / flat-period diagnostics
#   7) trade-overlap comparison
#
# Output:
#   eurgbp_short_final_validation_summary.csv
#   eurgbp_short_final_validation_calendar_years.csv
#   eurgbp_short_final_validation_rolling_3y.csv
#   eurgbp_short_final_validation_slices.csv
#   eurgbp_short_final_validation_recent_windows.csv
#   eurgbp_short_final_validation_drawdowns.csv
#   eurgbp_short_final_validation_overlap.csv
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

MIN_BODY_RATIO = 1.00
STRUCTURE_LOOKBACK = 90

NY_TZ = ZoneInfo("America/New_York")
EXCLUDED_NY_HOURS = {9}

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

H1_WARMUP_DAYS = 700

SUMMARY_FILE = "eurgbp_short_final_validation_summary.csv"
CALENDAR_FILE = "eurgbp_short_final_validation_calendar_years.csv"
ROLLING_FILE = "eurgbp_short_final_validation_rolling_3y.csv"
SLICES_FILE = "eurgbp_short_final_validation_slices.csv"
RECENT_FILE = "eurgbp_short_final_validation_recent_windows.csv"
DRAWDOWN_FILE = "eurgbp_short_final_validation_drawdowns.csv"
OVERLAP_FILE = "eurgbp_short_final_validation_overlap.csv"


# ============================================================
# FINAL CANDIDATES
# ============================================================

CANDIDATES = [
    {
        "candidate": "ROBUST",
        "max_distance_atr": 0.15,
        "min_range_atr": 1.10,
        "max_close_location": 0.20,
        "min_momentum_12": 0.25,
        "min_momentum_48": 0.40,
        "max_stop_size_atr": 2.50,
        "min_upper_wick_body": None,
        "min_atr_ratio_50": None,
    },
    {
        "candidate": "HIGH_PF",
        "max_distance_atr": 0.075,
        "min_range_atr": 1.00,
        "max_close_location": 0.225,
        "min_momentum_12": None,
        "min_momentum_48": 1.00,
        "max_stop_size_atr": None,
        "min_upper_wick_body": 0.10,
        "min_atr_ratio_50": 0.80,
    },
]


# ============================================================
# FIXED SLICES
# ============================================================

FIXED_SLICES = [
    (
        "first_half_2002_2013",
        RESEARCH_FROM,
        datetime(2014, 1, 1, 0, 0, tzinfo=timezone.utc),
    ),
    (
        "second_half_2014_present",
        datetime(2014, 1, 1, 0, 0, tzinfo=timezone.utc),
        None,
    ),
    (
        "2002_2009",
        RESEARCH_FROM,
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
    "message": "Validation has not started",
    "service": "EURGBP Short Final Dual-Candidate Validation",
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "reward_risk": REWARD_RISK,
    "excluded_ny_hours": sorted(EXCLUDED_NY_HOURS),
    "candidates": [c["candidate"] for c in CANDIDATES],
    "output_files": [],
}


# ============================================================
# OANDA
# ============================================================

def headers():
    if not OANDA_TOKEN:
        raise RuntimeError("OANDA_TOKEN is not configured")

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


def rma_series(values, length):
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


def atr_series(candles, length=14):
    return rma_series(
        true_ranges(candles),
        length,
    )


def rolling_mean_optional(values, length):
    result = [None] * len(values)

    for index in range(length - 1, len(values)):
        window = values[
            index - length + 1:
            index + 1
        ]

        if any(value is None for value in window):
            continue

        result[index] = sum(window) / length

    return result


# ============================================================
# RAW CANDIDATES
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

        if body_ratio < MIN_BODY_RATIO:
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
            - h1[index - 12]["close"]
        ) / atr

        momentum_48 = (
            signal["close"]
            - h1[index - 48]["close"]
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
# CANDIDATE FILTERS
# ============================================================

def passes_candidate(
    signal,
    candidate,
):
    if (
        signal["ny_hour"]
        in EXCLUDED_NY_HOURS
    ):
        return False

    if (
        signal["structure_distance_atr"]
        > candidate["max_distance_atr"]
    ):
        return False

    if (
        signal["range_atr"]
        < candidate["min_range_atr"]
    ):
        return False

    if (
        signal["close_location"]
        > candidate["max_close_location"]
    ):
        return False

    value = candidate[
        "min_momentum_12"
    ]

    if (
        value is not None
        and signal["momentum_12"] < value
    ):
        return False

    value = candidate[
        "min_momentum_48"
    ]

    if (
        value is not None
        and signal["momentum_48"] < value
    ):
        return False

    value = candidate[
        "max_stop_size_atr"
    ]

    if (
        value is not None
        and signal["stop_size_atr"] > value
    ):
        return False

    value = candidate[
        "min_upper_wick_body"
    ]

    if (
        value is not None
        and signal["upper_wick_body"] < value
    ):
        return False

    value = candidate[
        "min_atr_ratio_50"
    ]

    if value is not None:
        if (
            signal["atr_ratio_50"] is None
            or signal["atr_ratio_50"] < value
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
        return EXIT_CACHE[signal_index]

    signal = h1[signal_index]

    reference_entry = signal["close"]

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

        EXIT_CACHE[signal_index] = result

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

    EXIT_CACHE[signal_index] = result

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
        signal_index = signal["index"]

        # Locked convention:
        # signal on exact candle where prior trade exits is allowed.
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

        if trade["status"] == "OPEN":
            still_open = True
            break

        trade = dict(trade)

        trade["signal_id"] = (
            trade["signal_time"]
            .astimezone(timezone.utc)
            .isoformat()
        )

        trades.append(trade)

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

        filtered.append(trade)

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

    gross_profit = sum(winners)
    gross_loss = abs(sum(losers))
    total_r = sum(results)

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


def stats_row(
    candidate_name,
    label,
    start,
    end,
    trades,
):
    stats = stats_for_trades(
        trades,
        start,
        end,
    )

    row = {
        "candidate": candidate_name,
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

    row.update(stats)

    return row


# ============================================================
# DRAWDOWN / FLAT DIAGNOSTICS
# ============================================================

def drawdown_diagnostics(
    candidate_name,
    trades,
):
    if not trades:
        return {
            "candidate": candidate_name,
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
    peak_time = trades[0]["signal_time"]

    worst_dd = 0.0
    worst_dd_start = peak_time
    worst_dd_end = peak_time
    worst_dd_recovery = None

    active_dd_start = None
    active_dd_low = 0.0
    active_dd_low_time = None

    last_equity_high_time = trades[0]["signal_time"]
    longest_flat_days = 0.0
    longest_flat_start = None
    longest_flat_end = None

    for trade in trades:
        equity += trade["result_r"]
        t = trade["signal_time"]

        if equity >= peak_equity:
            if active_dd_start is not None:
                if (
                    worst_dd_recovery is None
                    and abs(active_dd_low - worst_dd) < 1e-9
                ):
                    worst_dd_recovery = t

                active_dd_start = None
                active_dd_low = 0.0
                active_dd_low_time = None

            gap_days = (
                t - last_equity_high_time
            ).total_seconds() / 86400.0

            if gap_days > longest_flat_days:
                longest_flat_days = gap_days
                longest_flat_start = last_equity_high_time
                longest_flat_end = t

            peak_equity = equity
            peak_time = t
            last_equity_high_time = t

        else:
            dd = (
                equity
                - peak_equity
            )

            if active_dd_start is None:
                active_dd_start = peak_time
                active_dd_low = dd
                active_dd_low_time = t

            if dd < active_dd_low:
                active_dd_low = dd
                active_dd_low_time = t

            if dd < worst_dd:
                worst_dd = dd
                worst_dd_start = peak_time
                worst_dd_end = t
                worst_dd_recovery = None

    if active_dd_start is not None:
        end_time = trades[-1]["signal_time"]

        gap_days = (
            end_time
            - last_equity_high_time
        ).total_seconds() / 86400.0

        if gap_days > longest_flat_days:
            longest_flat_days = gap_days
            longest_flat_start = last_equity_high_time
            longest_flat_end = end_time

    dd_duration_days = (
        (
            worst_dd_recovery
            if worst_dd_recovery is not None
            else trades[-1]["signal_time"]
        )
        - worst_dd_start
    ).total_seconds() / 86400.0

    return {
        "candidate": candidate_name,
        "trades": len(trades),
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
# VALIDATION BUILDERS
# ============================================================

def build_summary(
    trades_by_candidate,
):
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

    for candidate_name, trades in (
        trades_by_candidate.items()
    ):
        stats = stats_for_trades(
            trades
        )

        row = {
            "candidate": candidate_name,
            "research_from": RESEARCH_FROM.isoformat(),
            "research_to": RESEARCH_TO.isoformat(),
            "reward_risk": REWARD_RISK,
            "excluded_ny_hours": "09",
            "trades": stats["trades"],
            "trades_per_year": round(
                stats["trades"]
                / years,
                2,
            ),
            "winners": stats["winners"],
            "losers": stats["losers"],
            "win_rate": stats["win_rate"],
            "profit_factor": stats["profit_factor"],
            "total_r": stats["total_r"],
            "expectancy_r": stats["expectancy_r"],
            "max_drawdown_r": stats["max_drawdown_r"],
            "longest_loss_streak": stats["longest_loss_streak"],
        }

        rows.append(row)

    return pd.DataFrame(rows)


def build_calendar_years(
    trades_by_candidate,
):
    rows = []

    first_year = RESEARCH_FROM.year
    last_year = RESEARCH_TO.year

    for candidate_name, trades in (
        trades_by_candidate.items()
    ):
        for year in range(
            first_year,
            last_year + 1,
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

            if end <= RESEARCH_FROM:
                continue

            actual_start = max(
                start,
                RESEARCH_FROM,
            )

            actual_end = min(
                end,
                RESEARCH_TO,
            )

            if actual_start >= actual_end:
                continue

            rows.append(
                stats_row(
                    candidate_name,
                    str(year),
                    actual_start,
                    actual_end,
                    trades,
                )
            )

    return pd.DataFrame(rows)


def build_rolling_3y(
    trades_by_candidate,
):
    rows = []

    first_start_year = 2002
    last_start_year = (
        RESEARCH_TO.year - 2
    )

    for candidate_name, trades in (
        trades_by_candidate.items()
    ):
        for start_year in range(
            first_start_year,
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

            if actual_start >= actual_end:
                continue

            row = stats_row(
                candidate_name,
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
                * 24
                * 60
                * 60
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

            rows.append(row)

    return pd.DataFrame(rows)


def build_fixed_slices(
    trades_by_candidate,
):
    rows = []

    for candidate_name, trades in (
        trades_by_candidate.items()
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
                    candidate_name,
                    label,
                    start,
                    actual_end,
                    trades,
                )
            )

    return pd.DataFrame(rows)


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
    trades_by_candidate,
):
    rows = []

    for candidate_name, trades in (
        trades_by_candidate.items()
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
                candidate_name,
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

            rows.append(row)

    return pd.DataFrame(rows)


def build_drawdowns(
    trades_by_candidate,
):
    rows = []

    for candidate_name, trades in (
        trades_by_candidate.items()
    ):
        rows.append(
            drawdown_diagnostics(
                candidate_name,
                trades,
            )
        )

    return pd.DataFrame(rows)


def build_overlap(
    trades_by_candidate,
):
    robust = trades_by_candidate[
        "ROBUST"
    ]

    high_pf = trades_by_candidate[
        "HIGH_PF"
    ]

    robust_map = {
        trade[
            "signal_id"
        ]: trade
        for trade in robust
    }

    high_pf_map = {
        trade[
            "signal_id"
        ]: trade
        for trade in high_pf
    }

    robust_ids = set(
        robust_map.keys()
    )

    high_pf_ids = set(
        high_pf_map.keys()
    )

    shared_ids = (
        robust_ids
        & high_pf_ids
    )

    robust_only_ids = (
        robust_ids
        - high_pf_ids
    )

    high_pf_only_ids = (
        high_pf_ids
        - robust_ids
    )

    shared_robust_results = [
        robust_map[
            signal_id
        ][
            "result_r"
        ]
        for signal_id in sorted(
            shared_ids
        )
    ]

    robust_only_results = [
        robust_map[
            signal_id
        ][
            "result_r"
        ]
        for signal_id in sorted(
            robust_only_ids
        )
    ]

    high_pf_only_results = [
        high_pf_map[
            signal_id
        ][
            "result_r"
        ]
        for signal_id in sorted(
            high_pf_only_ids
        )
    ]

    def simple_result_stats(
        results,
    ):
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
        simple_result_stats(
            shared_robust_results
        )
    )

    robust_only_stats = (
        simple_result_stats(
            robust_only_results
        )
    )

    high_pf_only_stats = (
        simple_result_stats(
            high_pf_only_results
        )
    )

    jaccard = (
        len(
            shared_ids
        )
        / len(
            robust_ids
            | high_pf_ids
        )
        if (
            robust_ids
            | high_pf_ids
        )
        else 0.0
    )

    row = {
        "robust_trades": len(
            robust_ids
        ),
        "high_pf_trades": len(
            high_pf_ids
        ),
        "shared_trades": len(
            shared_ids
        ),
        "robust_only_trades": len(
            robust_only_ids
        ),
        "high_pf_only_trades": len(
            high_pf_only_ids
        ),
        "shared_pct_of_robust": round(
            len(
                shared_ids
            )
            / len(
                robust_ids
            )
            * 100.0
            if robust_ids
            else 0.0,
            2,
        ),
        "shared_pct_of_high_pf": round(
            len(
                shared_ids
            )
            / len(
                high_pf_ids
            )
            * 100.0
            if high_pf_ids
            else 0.0,
            2,
        ),
        "jaccard_overlap": round(
            jaccard,
            3,
        ),
        "shared_total_r": shared_stats[
            "total_r"
        ],
        "shared_expectancy_r": shared_stats[
            "expectancy_r"
        ],
        "shared_win_rate": shared_stats[
            "win_rate"
        ],
        "robust_only_total_r": robust_only_stats[
            "total_r"
        ],
        "robust_only_expectancy_r": robust_only_stats[
            "expectancy_r"
        ],
        "robust_only_win_rate": robust_only_stats[
            "win_rate"
        ],
        "high_pf_only_total_r": high_pf_only_stats[
            "total_r"
        ],
        "high_pf_only_expectancy_r": high_pf_only_stats[
            "expectancy_r"
        ],
        "high_pf_only_win_rate": high_pf_only_stats[
            "win_rate"
        ],
    }

    return pd.DataFrame(
        [row]
    )


# ============================================================
# RESEARCH
# ============================================================

def run_validation():
    global STATUS

    try:
        print()
        print("=" * 86)
        print(
            "EUR/GBP SHORT - FINAL DUAL-CANDIDATE VALIDATION"
        )
        print("=" * 86)
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
                "Building ATR14 and final candidate signals"
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

        STATUS[
            "raw_bearish_engulfing_signals"
        ] = len(
            raw_candidates
        )

        trades_by_candidate = {}

        STATUS.update({
            "state": "simulating",
            "message": (
                "Simulating both frozen final candidates"
            ),
        })

        for candidate in (
            CANDIDATES
        ):
            name = candidate[
                "candidate"
            ]

            eligible = [
                signal
                for signal in raw_candidates
                if passes_candidate(
                    signal,
                    candidate,
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

            trades_by_candidate[
                name
            ] = trades

            STATUS[
                f"{name.lower()}_eligible_signals"
            ] = len(
                eligible
            )

            STATUS[
                f"{name.lower()}_trades"
            ] = len(
                trades
            )

            STATUS[
                f"{name.lower()}_ignored_due_to_open_trade"
            ] = ignored

            STATUS[
                f"{name.lower()}_still_open_at_end"
            ] = still_open

            print(
                f"{name}: "
                f"{len(trades)} trades",
                flush=True,
            )

        STATUS.update({
            "state": "building_outputs",
            "message": (
                "Building validation tables"
            ),
        })

        summary_df = (
            build_summary(
                trades_by_candidate
            )
        )

        calendar_df = (
            build_calendar_years(
                trades_by_candidate
            )
        )

        rolling_df = (
            build_rolling_3y(
                trades_by_candidate
            )
        )

        slices_df = (
            build_fixed_slices(
                trades_by_candidate
            )
        )

        recent_df = (
            build_recent_windows(
                trades_by_candidate
            )
        )

        drawdown_df = (
            build_drawdowns(
                trades_by_candidate
            )
        )

        overlap_df = (
            build_overlap(
                trades_by_candidate
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

        output_files = [
            SUMMARY_FILE,
            CALENDAR_FILE,
            ROLLING_FILE,
            SLICES_FILE,
            RECENT_FILE,
            DRAWDOWN_FILE,
            OVERLAP_FILE,
        ]

        STATUS.update({
            "state": "complete",
            "message": (
                "EUR/GBP final validation "
                "completed successfully"
            ),
            "output_files": output_files,
        })

        print()
        print("=" * 86)
        print(
            "EUR/GBP FINAL VALIDATION COMPLETE"
        )
        print("=" * 86)

        print()
        print(
            summary_df.to_string(
                index=False
            ),
            flush=True,
        )

        print()
        print(
            "Saved files:"
        )

        for filename in (
            output_files
        ):
            print(
                " -",
                filename,
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
            "EURGBP Short Final Dual-Candidate Validation"
        ),
        "status": STATUS,
        "instrument": INSTRUMENT,
        "direction": "SHORT",
        "reward_risk": REWARD_RISK,
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
        "candidates": CANDIDATES,
        "downloads": {
            "summary": "/download/summary",
            "calendar_years": "/download/calendar",
            "rolling_3y": "/download/rolling",
            "slices": "/download/slices",
            "recent_windows": "/download/recent",
            "drawdowns": "/download/drawdowns",
            "overlap": "/download/overlap",
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


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    validation_thread = threading.Thread(
        target=run_validation,
        name=(
            "eurgbp-short-final-dual-candidate-validation"
        ),
        daemon=True,
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
