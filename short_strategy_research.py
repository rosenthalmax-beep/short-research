import os
import itertools
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# GBP/USD SHORT - TARGETED FINAL STRUCTURAL VALIDATION
#
# RESEARCH ONLY — THIS SCRIPT NEVER SUBMITS ORDERS.
#
# Purpose:
#   Final structural/quality validation before timing.
#   EVERY parameter combination is broken down by era.
#
# Still NO hour or weekday optimisation.
#
# Conventions:
#   OANDA midpoint H1 candles
#   Daily alignment = 17:00 America/New_York
#   Previous completed daily candle only
#   ATR14 = Wilder/RMA
#   Stop = signal high + 10 ticks
#   Adverse short slippage = 5 ticks
#   Target based on reference signal close
#   Pyramiding = 0
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

RESEARCH_FROM = datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc)
RESEARCH_TO = datetime.now(timezone.utc).replace(
    minute=0,
    second=0,
    microsecond=0,
)

H1_WARMUP_DAYS = 120
DAILY_WARMUP_DAYS = 2000

OUTPUT_FILE = "gbpusd_short_targeted_final_validation.csv"


# ============================================================
# TARGETED GRID
#
# 3 x 3 x 3 x 2 x 3 x 2 x 3 x 4 x 2
# = 7,776 combinations
# ============================================================

BODY_RATIOS = [1.00, 1.10, 1.15]

STRUCTURE_LOOKBACKS = [65, 70, 75]

MAX_DISTANCE_ATR_VALUES = [0.10, 0.125, 0.15]

REWARD_RISKS = [2.50, 2.75]

SLOW_EMA_LENGTHS = [90, 100, 110]

STRONG_CLOSE_THRESHOLDS = [0.35, 0.40]

FAST_EMA_LENGTHS = [40, 50, 60]

MIN_RANGE_ATR_VALUES = [None, 0.90, 1.00, 1.10]

EMA_SEPARATION_THRESHOLDS = [None, 0.075]

ALL_DAILY_EMA_LENGTHS = sorted(
    set(SLOW_EMA_LENGTHS + FAST_EMA_LENGTHS)
)

TOTAL_COMBINATIONS = (
    len(BODY_RATIOS)
    * len(STRUCTURE_LOOKBACKS)
    * len(MAX_DISTANCE_ATR_VALUES)
    * len(REWARD_RISKS)
    * len(SLOW_EMA_LENGTHS)
    * len(STRONG_CLOSE_THRESHOLDS)
    * len(FAST_EMA_LENGTHS)
    * len(MIN_RANGE_ATR_VALUES)
    * len(EMA_SEPARATION_THRESHOLDS)
)


# ============================================================
# ERA WINDOWS
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
    "service": "GBPUSD Short Targeted Final Structural Validation",
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "total_combinations": TOTAL_COMBINATIONS,
    "completed_combinations": 0,
    "rows_saved": 0,
    "output_file": None,
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
            f"OANDA {response.status_code}: {response.text[:500]}"
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


def fetch_range(instrument, granularity, start, end):
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


def fetch_chunked_history(instrument, granularity, start, end):
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
    candles.sort(key=lambda item: item["time"])

    return candles


# ============================================================
# INDICATORS
# ============================================================

def ema_series(values, length):
    result = [None] * len(values)

    if len(values) < length:
        return result

    initial = sum(values[:length]) / length
    result[length - 1] = initial

    multiplier = 2.0 / (length + 1.0)
    previous = initial

    for index in range(length, len(values)):
        current = (
            (values[index] - previous)
            * multiplier
            + previous
        )

        result[index] = current
        previous = current

    return result


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


# ============================================================
# DAILY ALIGNMENT
# ============================================================

def current_daily_start(timestamp_utc):
    ny_time = timestamp_utc.astimezone(NY_TZ)

    candidate = ny_time.replace(
        hour=DAILY_ALIGNMENT_HOUR,
        minute=0,
        second=0,
        microsecond=0,
    )

    if ny_time < candidate:
        candidate -= timedelta(days=1)

    return candidate.astimezone(timezone.utc)


def build_daily_state(daily):
    closes = [
        candle["close"]
        for candle in daily
    ]

    ema_cache = {}

    for length in ALL_DAILY_EMA_LENGTHS:
        ema_cache[length] = ema_series(
            closes,
            length,
        )

    return {
        "ema": ema_cache,
        "atr14": atr_series(daily, 14),
    }


def build_h1_daily_lookup(h1, daily, daily_state):
    lookup = [None] * len(h1)
    daily_index = -1

    for h1_index, candle in enumerate(h1):
        session_start = current_daily_start(
            candle["time"]
        )

        while (
            daily_index + 1 < len(daily)
            and daily[daily_index + 1]["time"] < session_start
        ):
            daily_index += 1

        if daily_index < 0:
            continue

        row = {
            "close": daily[daily_index]["close"],
            "atr14": daily_state["atr14"][daily_index],
        }

        for length, series in daily_state["ema"].items():
            row[f"ema_{length}"] = series[daily_index]

        lookup[h1_index] = row

    return lookup


# ============================================================
# SIGNAL FEATURES
# ============================================================

def build_candidates(h1, h1_atr, daily_lookup):
    candidates = []

    max_lookback = max(STRUCTURE_LOOKBACKS)

    for index in range(max_lookback, len(h1)):
        signal = h1[index]

        if signal["time"] < RESEARCH_FROM:
            continue

        if signal["time"] >= RESEARCH_TO:
            break

        previous = h1[index - 1]
        atr = h1_atr[index]
        daily = daily_lookup[index]

        if (
            atr is None
            or atr <= 0
            or daily is None
        ):
            continue

        previous_body = abs(
            previous["close"] - previous["open"]
        )

        current_body = abs(
            signal["close"] - signal["open"]
        )

        if (
            previous_body <= 0
            or current_body <= 0
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

        candle_range = (
            signal["high"] - signal["low"]
        )

        if candle_range <= 0:
            continue

        structure_distances = {}

        for lookback in STRUCTURE_LOOKBACKS:
            previous_highest = max(
                candle["high"]
                for candle in h1[
                    index - lookback:index
                ]
            )

            # Negative = signal high exceeded prior high,
            # which still correctly passes a "within X ATR"
            # recent-high test.
            structure_distances[lookback] = (
                previous_highest
                - signal["high"]
            ) / atr

        candidates.append({
            "index": index,
            "time": signal["time"],
            "body_ratio": (
                current_body / previous_body
            ),
            "structure_distances": structure_distances,
            "strong_close": (
                signal["close"] - signal["low"]
            ) / candle_range,
            "range_atr": candle_range / atr,
            "daily": daily,
        })

    return candidates


# ============================================================
# FILTER
# ============================================================

def candidate_allowed(
    candidate,
    body_ratio,
    structure_lookback,
    maximum_distance_atr,
    slow_ema,
    strong_close_max,
    fast_ema,
    minimum_range_atr,
    ema_separation_min,
):
    if candidate["body_ratio"] < body_ratio:
        return False

    distance = (
        candidate["structure_distances"][
            structure_lookback
        ]
    )

    if distance > maximum_distance_atr:
        return False

    if candidate["strong_close"] > strong_close_max:
        return False

    if (
        minimum_range_atr is not None
        and candidate["range_atr"] < minimum_range_atr
    ):
        return False

    daily = candidate["daily"]

    slow = daily.get(f"ema_{slow_ema}")
    fast = daily.get(f"ema_{fast_ema}")

    if slow is None or fast is None:
        return False

    # Previous completed daily close below slow EMA.
    if not (daily["close"] < slow):
        return False

    # Bearish fast/slow daily alignment.
    if not (fast < slow):
        return False

    if ema_separation_min is not None:
        daily_atr = daily.get("atr14")

        if daily_atr is None or daily_atr <= 0:
            return False

        separation = (
            slow - fast
        ) / daily_atr

        if separation < ema_separation_min:
            return False

    return True


# ============================================================
# EXIT SIMULATION
# ============================================================

EXIT_CACHE = {}


def calculate_trade_exit(h1, signal_index, reward_risk):
    cache_key = (
        signal_index,
        reward_risk,
    )

    if cache_key in EXIT_CACHE:
        return EXIT_CACHE[cache_key]

    signal = h1[signal_index]

    reference_entry = signal["close"]

    # For a short, a LOWER fill is adverse.
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
        stop - reference_entry
    )

    if reference_risk <= 0:
        raise RuntimeError(
            "Invalid short reference risk"
        )

    target = (
        reference_entry
        - reference_risk * reward_risk
    )

    actual_risk = (
        stop - backtest_entry
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

        stop_hit = candle["high"] >= stop
        target_hit = candle["low"] <= target

        if not (stop_hit or target_hit):
            continue

        if stop_hit and target_hit:
            distance_to_high = abs(
                candle["high"] - candle["open"]
            )

            distance_to_low = abs(
                candle["open"] - candle["low"]
            )

            if distance_to_high < distance_to_low:
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

        result_r = (
            backtest_entry - exit_price
        ) / actual_risk

        result = {
            "status": "CLOSED",
            "signal_index": signal_index,
            "signal_time": signal["time"],
            "exit_index": index,
            "exit_time": candle["time"],
            "exit_reason": exit_reason,
            "result_r": result_r,
        }

        EXIT_CACHE[cache_key] = result
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

    EXIT_CACHE[cache_key] = result
    return result


def simulate(h1, candidates, reward_risk):
    trades = []
    position_exit_index = -1
    ignored = 0
    still_open = False

    for candidate in candidates:
        signal_index = candidate["index"]

        # Exact convention used in prior research:
        # "<", not "<=".
        if signal_index < position_exit_index:
            ignored += 1
            continue

        trade = calculate_trade_exit(
            h1,
            signal_index,
            reward_risk,
        )

        if trade["status"] == "OPEN":
            still_open = True
            break

        trades.append(trade)
        position_exit_index = trade["exit_index"]

    return trades, ignored, still_open


# ============================================================
# STATISTICS
# ============================================================

def stats_for_trades(trades, start=None, end=None):
    filtered = []

    for trade in trades:
        signal_time = trade["signal_time"]

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
        x for x in results
        if x > 0
    ]

    losers = [
        x for x in results
        if x < 0
    ]

    gross_profit = sum(winners)
    gross_loss = abs(sum(losers))
    total_r = sum(results)

    if gross_loss > 0:
        pf = gross_profit / gross_loss
    elif gross_profit > 0:
        pf = 999.0
    else:
        pf = 0.0

    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0

    for result in results:
        equity += result
        peak = max(peak, equity)
        max_drawdown = min(
            max_drawdown,
            equity - peak,
        )

    current_streak = 0
    longest_streak = 0

    for result in results:
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
            len(winners) / len(results) * 100.0,
            2,
        ),
        "profit_factor": round(pf, 3),
        "total_r": round(total_r, 2),
        "expectancy_r": round(
            total_r / len(results),
            3,
        ),
        "max_drawdown_r": round(
            max_drawdown,
            2,
        ),
        "longest_loss_streak": longest_streak,
    }


# ============================================================
# RESEARCH
# ============================================================

def run_research():
    global STATUS

    try:
        print()
        print("=" * 72)
        print(
            "GBP/USD SHORT - TARGETED FINAL STRUCTURAL VALIDATION"
        )
        print("=" * 72)
        print("ALL HOURS / ALL WEEKDAYS")
        print("ERA TEST ON EVERY COMBINATION")
        print(
            "Total combinations:",
            TOTAL_COMBINATIONS,
        )
        print()

        STATUS.update({
            "state": "fetching_data",
            "message": "Fetching GBP/USD OANDA history",
        })

        h1 = fetch_chunked_history(
            INSTRUMENT,
            "H1",
            RESEARCH_FROM
            - timedelta(days=H1_WARMUP_DAYS),
            RESEARCH_TO,
        )

        daily = fetch_chunked_history(
            INSTRUMENT,
            "D",
            RESEARCH_FROM
            - timedelta(days=DAILY_WARMUP_DAYS),
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

        print("H1 candles:", len(h1))
        print(
            "Earliest H1:",
            h1[0]["time"].isoformat(),
        )
        print(
            "Latest H1:",
            h1[-1]["time"].isoformat(),
        )
        print("Daily candles:", len(daily))
        print()

        STATUS.update({
            "state": "precomputing",
            "message": "Building indicators and signal features",
        })

        h1_atr = atr_series(h1, 14)
        daily_state = build_daily_state(daily)

        daily_lookup = build_h1_daily_lookup(
            h1,
            daily,
            daily_state,
        )

        candidates = build_candidates(
            h1,
            h1_atr,
            daily_lookup,
        )

        STATUS[
            "base_bearish_engulfings"
        ] = len(candidates)

        print(
            "Base bearish engulfings:",
            len(candidates),
        )

        STATUS.update({
            "state": "running",
            "message": "Running targeted final validation",
        })

        rows = []

        combinations = itertools.product(
            BODY_RATIOS,
            STRUCTURE_LOOKBACKS,
            MAX_DISTANCE_ATR_VALUES,
            REWARD_RISKS,
            SLOW_EMA_LENGTHS,
            STRONG_CLOSE_THRESHOLDS,
            FAST_EMA_LENGTHS,
            MIN_RANGE_ATR_VALUES,
            EMA_SEPARATION_THRESHOLDS,
        )

        for number, combo in enumerate(
            combinations,
            start=1,
        ):
            (
                body,
                lookback,
                distance,
                rr,
                slow,
                strong_close,
                fast,
                min_range,
                separation,
            ) = combo

            eligible = [
                candidate
                for candidate in candidates
                if candidate_allowed(
                    candidate,
                    body,
                    lookback,
                    distance,
                    slow,
                    strong_close,
                    fast,
                    min_range,
                    separation,
                )
            ]

            (
                trades,
                ignored,
                still_open,
            ) = simulate(
                h1,
                eligible,
                rr,
            )

            full = stats_for_trades(
                trades
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

            row = {
                "body_ratio": body,
                "structure_lookback": lookback,
                "maximum_distance_atr": distance,
                "reward_risk": rr,
                "slow_daily_ema": slow,
                "strong_close_max": strong_close,
                "fast_daily_ema": fast,
                "minimum_signal_range_atr": min_range,
                "ema_separation_min_daily_atr": separation,
                "raw_signals": len(eligible),
                "ignored_due_to_open_trade": ignored,
                "still_open_at_end": still_open,
                "trades": full["trades"],
                "trades_per_year": round(
                    full["trades"] / years,
                    2,
                ),
                "winners": full["winners"],
                "losers": full["losers"],
                "win_rate": full["win_rate"],
                "profit_factor": full["profit_factor"],
                "total_r": full["total_r"],
                "expectancy_r": full["expectancy_r"],
                "max_drawdown_r": full["max_drawdown_r"],
                "longest_loss_streak": full["longest_loss_streak"],
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

                row[f"{era_name}_trades"] = era["trades"]
                row[f"{era_name}_pf"] = era["profit_factor"]
                row[f"{era_name}_r"] = era["total_r"]
                row[f"{era_name}_expectancy"] = era["expectancy_r"]
                row[f"{era_name}_win_rate"] = era["win_rate"]
                row[f"{era_name}_max_drawdown_r"] = era["max_drawdown_r"]

                if era["total_r"] > 0:
                    profitable_eras += 1

                if era["trades"] >= 5:
                    eras_with_5_plus += 1

                    if era["total_r"] > 0:
                        profitable_eras_with_5_plus += 1

                    if minimum_era_pf_5_plus is None:
                        minimum_era_pf_5_plus = era["profit_factor"]
                    else:
                        minimum_era_pf_5_plus = min(
                            minimum_era_pf_5_plus,
                            era["profit_factor"],
                        )

                    if minimum_era_expectancy_5_plus is None:
                        minimum_era_expectancy_5_plus = era["expectancy_r"]
                    else:
                        minimum_era_expectancy_5_plus = min(
                            minimum_era_expectancy_5_plus,
                            era["expectancy_r"],
                        )

            row["profitable_eras"] = profitable_eras
            row["eras_with_5_plus_trades"] = eras_with_5_plus
            row[
                "profitable_eras_with_5_plus_trades"
            ] = profitable_eras_with_5_plus
            row[
                "minimum_era_pf_5_plus"
            ] = minimum_era_pf_5_plus
            row[
                "minimum_era_expectancy_5_plus"
            ] = minimum_era_expectancy_5_plus

            rows.append(row)

            STATUS[
                "completed_combinations"
            ] = number

            if number % 250 == 0:
                print(
                    f"Progress: "
                    f"{number}/{TOTAL_COMBINATIONS}",
                    flush=True,
                )

        df = pd.DataFrame(rows)

        if df.empty:
            raise RuntimeError(
                "No validation rows generated"
            )

        df["adequate_80"] = (
            df["trades"] >= 80
        )

        df["adequate_100"] = (
            df["trades"] >= 100
        )

        # Rank robustness first, then full-history quality.
        df = df.sort_values(
            by=[
                "adequate_80",
                "profitable_eras_with_5_plus_trades",
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
                "GBP/USD targeted final structural "
                "validation complete"
            ),
            "completed_combinations": TOTAL_COMBINATIONS,
            "rows_saved": len(df),
            "output_file": OUTPUT_FILE,
            "earliest_h1": h1[0]["time"].isoformat(),
            "latest_h1": h1[-1]["time"].isoformat(),
        })

        print()
        print("=" * 72)
        print("TARGETED VALIDATION COMPLETE")
        print("=" * 72)
        print("Rows saved:", len(df))
        print("Saved:", OUTPUT_FILE)
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
            "GBPUSD Short Targeted "
            "Final Structural Validation"
        ),
        "status": STATUS,
        "instrument": INSTRUMENT,
        "direction": "SHORT",
        "timing_filters": (
            "NONE - all hours and all weekdays"
        ),
        "grid": {
            "body_ratios": BODY_RATIOS,
            "structure_lookbacks": STRUCTURE_LOOKBACKS,
            "maximum_distance_atr": MAX_DISTANCE_ATR_VALUES,
            "reward_risks": REWARD_RISKS,
            "slow_daily_ema": SLOW_EMA_LENGTHS,
            "strong_close_max": STRONG_CLOSE_THRESHOLDS,
            "fast_daily_ema": FAST_EMA_LENGTHS,
            "minimum_signal_range_atr": MIN_RANGE_ATR_VALUES,
            "ema_separation_min_daily_atr": EMA_SEPARATION_THRESHOLDS,
            "total_combinations": TOTAL_COMBINATIONS,
        },
        "eras": [
            "2002-2009",
            "2010-2017",
            "2018-2023",
            "2024-present",
        ],
        "download": "/download",
        "trading_enabled": False,
        "orders_supported": False,
        "executor_connected": False,
    })


@app.route("/status")
def status():
    return jsonify(STATUS)


@app.route("/download")
def download():
    if not os.path.exists(OUTPUT_FILE):
        return jsonify({
            "status": "not_ready",
            "message": "CSV is not ready yet",
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
        name="gbpusd-short-targeted-final-validation",
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
