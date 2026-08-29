import os
import itertools
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# GBP/USD SHORT - DEEP STRUCTURAL + FILTER REFINEMENT
#
# RESEARCH ONLY. THIS SCRIPT DOES NOT SUBMIT ORDERS.
#
# Stage 1:
#   Broad refinement around every useful edge from the first
#   GBP/USD sweep.
#
# Stage 2:
#   Takes the strongest adequately-sized Stage-1 cores and tests
#   strong-close, fast EMA alignment, EMA separation, minimum
#   signal range and upper-wick filters in combination.
#
# OANDA midpoint H1 candles
# Daily alignment: 17:00 America/New_York
# All hours and all weekdays (timing comes later)
# Stop: signal high + 10 ticks
# Adverse simulated short slippage: 5 ticks
# Pyramiding: 0
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
RESEARCH_TO = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

H1_WARMUP_DAYS = 100
DAILY_WARMUP_DAYS = 1800

STAGE1_OUTPUT = "gbpusd_short_stage1_deep_core.csv"
STAGE2_OUTPUT = "gbpusd_short_stage2_filter_combinations.csv"

MIN_STAGE2_TRADES = 150
SEED_COUNT = 20


# ============================================================
# STAGE 1 GRID
#
# Explicitly extends beyond every winning edge from broad sweep:
# - body below 1.00 and above 1.40
# - structure beyond 40
# - distance below 0.15
# - RR below 2.0 and above 2.0
# - EMA between/beyond original 50/100/150/200 points
# ============================================================

BODY_RATIOS = [
    0.90, 1.00, 1.10, 1.20, 1.30, 1.40, 1.50
]

STRUCTURE_LOOKBACKS = [
    30, 35, 40, 45, 50, 55, 60
]

MAX_DISTANCE_ATR_VALUES = [
    0.05, 0.10, 0.15, 0.20, 0.25, 0.30
]

REWARD_RISKS = [
    1.50, 1.75, 2.00, 2.25, 2.50, 2.75, 3.00
]

SLOW_EMA_LENGTHS = [
    50, 75, 100, 125, 150, 175, 200, 225
]

STAGE1_TOTAL = (
    len(BODY_RATIOS)
    * len(STRUCTURE_LOOKBACKS)
    * len(MAX_DISTANCE_ATR_VALUES)
    * len(REWARD_RISKS)
    * len(SLOW_EMA_LENGTHS)
)


# ============================================================
# STAGE 2 FILTER GRID
#
# None = filter disabled.
#
# strong_close:
#   (close - low) / (high - low) <= threshold
#
# fast EMA:
#   previous completed daily EMAfast < EMAslow
#
# separation:
#   (EMAslow - EMAfast) / Daily ATR14 >= threshold
#
# min range:
#   signal H1 range / H1 ATR14 >= threshold
#
# upper wick:
#   (high - max(open, close)) / body >= threshold
# ============================================================

STRONG_CLOSE_THRESHOLDS = [
    None, 0.20, 0.25, 0.30, 0.35
]

FAST_EMA_LENGTHS = [
    None, 20, 30, 50, 70, 85, 100
]

EMA_SEPARATION_THRESHOLDS = [
    None, 0.025, 0.050, 0.075
]

MIN_RANGE_ATR_VALUES = [
    None, 0.70, 0.90, 1.10
]

MIN_UPPER_WICK_BODY_VALUES = [
    None, 0.10, 0.20, 0.30
]

ALL_DAILY_EMA_LENGTHS = sorted(set(
    SLOW_EMA_LENGTHS
    + [x for x in FAST_EMA_LENGTHS if x is not None]
))


# ============================================================
# STATUS
# ============================================================

STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "stage1_total": STAGE1_TOTAL,
    "stage1_completed": 0,
    "stage2_total": None,
    "stage2_completed": 0,
    "stage1_output": None,
    "stage2_output": None,
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
        "time": datetime.fromisoformat(raw["time"].replace("Z", "+00:00")),
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
    candles.sort(key=lambda x: x["time"])
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

    for i in range(length, len(values)):
        current = ((values[i] - previous) * multiplier) + previous
        result[i] = current
        previous = current

    return result


def true_ranges(candles):
    values = []

    for i, candle in enumerate(candles):
        if i == 0:
            tr = candle["high"] - candle["low"]
        else:
            previous_close = candles[i - 1]["close"]
            tr = max(
                candle["high"] - candle["low"],
                abs(candle["high"] - previous_close),
                abs(candle["low"] - previous_close),
            )
        values.append(tr)

    return values


def rma_series(values, length):
    result = [None] * len(values)

    if len(values) < length:
        return result

    initial = sum(values[:length]) / length
    result[length - 1] = initial
    previous = initial

    for i in range(length, len(values)):
        current = ((previous * (length - 1)) + values[i]) / length
        result[i] = current
        previous = current

    return result


def atr_series(candles, length=14):
    return rma_series(true_ranges(candles), length)


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
    closes = [c["close"] for c in daily]

    ema_cache = {}
    for length in ALL_DAILY_EMA_LENGTHS:
        ema_cache[length] = ema_series(closes, length)

    daily_atr = atr_series(daily, 14)

    return {
        "ema": ema_cache,
        "atr14": daily_atr,
    }


def build_h1_daily_lookup(h1, daily, daily_state):
    lookup = [None] * len(h1)
    daily_index = -1

    for h1_index, candle in enumerate(h1):
        session_start = current_daily_start(candle["time"])

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
# BASE BEARISH ENGULFING FEATURES
# ============================================================

def build_candidates(h1, h1_atr, daily_lookup):
    candidates = []
    max_lookback = max(STRUCTURE_LOOKBACKS)

    for i in range(max_lookback, len(h1)):
        signal = h1[i]

        if signal["time"] < RESEARCH_FROM:
            continue

        if signal["time"] >= RESEARCH_TO:
            break

        previous = h1[i - 1]
        atr = h1_atr[i]
        daily = daily_lookup[i]

        if atr is None or atr <= 0 or daily is None:
            continue

        previous_body = abs(previous["close"] - previous["open"])
        current_body = abs(signal["close"] - signal["open"])

        if previous_body <= 0 or current_body <= 0:
            continue

        bearish_engulfing = (
            previous["close"] > previous["open"]
            and signal["close"] < signal["open"]
            and signal["open"] >= previous["close"]
            and signal["close"] <= previous["open"]
        )

        if not bearish_engulfing:
            continue

        candle_range = signal["high"] - signal["low"]
        if candle_range <= 0:
            continue

        structure_distances = {}

        for lookback in STRUCTURE_LOOKBACKS:
            previous_highest = max(
                candle["high"]
                for candle in h1[i - lookback:i]
            )

            # Negative values mean the signal high exceeded
            # the prior lookback high. That correctly passes a
            # "within X ATR of previous high" test.
            structure_distances[lookback] = (
                previous_highest - signal["high"]
            ) / atr

        strong_close = (
            signal["close"] - signal["low"]
        ) / candle_range

        upper_wick = (
            signal["high"]
            - max(signal["open"], signal["close"])
        )

        upper_wick_body = upper_wick / current_body
        range_atr = candle_range / atr

        candidates.append({
            "index": i,
            "time": signal["time"],
            "body_ratio": current_body / previous_body,
            "structure_distances": structure_distances,
            "strong_close": strong_close,
            "upper_wick_body": upper_wick_body,
            "range_atr": range_atr,
            "daily": daily,
        })

    return candidates


# ============================================================
# FILTERS
# ============================================================

def core_allowed(
    candidate,
    body_ratio,
    structure_lookback,
    maximum_distance_atr,
    slow_ema,
):
    if candidate["body_ratio"] < body_ratio:
        return False

    distance = candidate["structure_distances"][structure_lookback]

    if distance > maximum_distance_atr:
        return False

    daily = candidate["daily"]
    slow = daily.get(f"ema_{slow_ema}")

    if slow is None:
        return False

    if not (daily["close"] < slow):
        return False

    return True


def extra_filters_allowed(
    candidate,
    slow_ema,
    strong_close,
    fast_ema,
    ema_separation,
    min_range_atr,
    min_upper_wick_body,
):
    if (
        strong_close is not None
        and candidate["strong_close"] > strong_close
    ):
        return False

    if (
        min_range_atr is not None
        and candidate["range_atr"] < min_range_atr
    ):
        return False

    if (
        min_upper_wick_body is not None
        and candidate["upper_wick_body"] < min_upper_wick_body
    ):
        return False

    if fast_ema is not None:
        # A fast EMA equal to or slower than the selected slow EMA
        # is not a meaningful alignment test.
        if fast_ema >= slow_ema:
            return False

        daily = candidate["daily"]

        fast = daily.get(f"ema_{fast_ema}")
        slow = daily.get(f"ema_{slow_ema}")

        if fast is None or slow is None:
            return False

        if not (fast < slow):
            return False

        if ema_separation is not None:
            daily_atr = daily.get("atr14")

            if daily_atr is None or daily_atr <= 0:
                return False

            separation = (slow - fast) / daily_atr

            if separation < ema_separation:
                return False

    elif ema_separation is not None:
        # Separation makes no sense without a fast EMA.
        return False

    return True


# ============================================================
# EXIT SIMULATION
# ============================================================

EXIT_CACHE = {}


def calculate_trade_exit(h1, signal_index, reward_risk):
    cache_key = (signal_index, reward_risk)

    if cache_key in EXIT_CACHE:
        return EXIT_CACHE[cache_key]

    signal = h1[signal_index]

    reference_entry = signal["close"]

    # Adverse short slippage = LOWER short fill.
    backtest_entry = (
        reference_entry
        - BACKTEST_SLIPPAGE_TICKS * TICK_SIZE
    )

    stop = (
        signal["high"]
        + STOP_BUFFER_TICKS * TICK_SIZE
    )

    reference_risk = stop - reference_entry

    if reference_risk <= 0:
        raise RuntimeError("Invalid short reference risk")

    target = (
        reference_entry
        - reference_risk * reward_risk
    )

    actual_risk = stop - backtest_entry

    if actual_risk <= 0:
        raise RuntimeError("Invalid short actual risk")

    for i in range(signal_index + 1, len(h1)):
        candle = h1[i]

        if candle["time"] >= RESEARCH_TO:
            break

        stop_hit = candle["high"] >= stop
        target_hit = candle["low"] <= target

        if not (stop_hit or target_hit):
            continue

        if stop_hit and target_hit:
            distance_to_high = abs(candle["high"] - candle["open"])
            distance_to_low = abs(candle["open"] - candle["low"])

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
            "exit_index": i,
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

        # Exact convention used in the EUR/USD research:
        # a signal on the same candle an old trade exits is allowed.
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

def calculate_stats(trades):
    if not trades:
        return {
            "trades": 0,
            "trades_per_year": 0.0,
            "winners": 0,
            "losers": 0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "total_r": 0.0,
            "expectancy_r": 0.0,
            "max_drawdown_r": 0.0,
            "longest_loss_streak": 0,
        }

    results = [trade["result_r"] for trade in trades]

    winners = [x for x in results if x > 0]
    losers = [x for x in results if x < 0]

    gross_profit = sum(winners)
    gross_loss = abs(sum(losers))
    total_r = sum(results)

    if gross_loss > 0:
        profit_factor = gross_profit / gross_loss
    elif gross_profit > 0:
        profit_factor = 999.0
    else:
        profit_factor = 0.0

    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0

    for result in results:
        equity += result
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity - peak)

    current_streak = 0
    longest_streak = 0

    for result in results:
        if result < 0:
            current_streak += 1
            longest_streak = max(longest_streak, current_streak)
        else:
            current_streak = 0

    years = (
        (RESEARCH_TO - RESEARCH_FROM).total_seconds()
        / (365.2425 * 24 * 60 * 60)
    )

    return {
        "trades": len(results),
        "trades_per_year": round(len(results) / years, 2),
        "winners": len(winners),
        "losers": len(losers),
        "win_rate": round(len(winners) / len(results) * 100.0, 2),
        "profit_factor": round(profit_factor, 3),
        "total_r": round(total_r, 2),
        "expectancy_r": round(total_r / len(results), 3),
        "max_drawdown_r": round(max_drawdown, 2),
        "longest_loss_streak": longest_streak,
    }


# ============================================================
# RESULT HELPERS
# ============================================================

def make_stage1_row(
    body,
    lookback,
    distance,
    rr,
    slow,
    eligible,
    trades,
    ignored,
    still_open,
):
    return {
        "body_ratio": body,
        "structure_lookback": lookback,
        "maximum_distance_atr": distance,
        "reward_risk": rr,
        "slow_daily_ema": slow,
        "raw_signals": len(eligible),
        "ignored_due_to_open_trade": ignored,
        "still_open_at_end": still_open,
        **calculate_stats(trades),
    }


def make_stage2_row(
    seed_number,
    seed,
    strong_close,
    fast_ema,
    separation,
    min_range,
    upper_wick,
    eligible,
    trades,
    ignored,
    still_open,
):
    return {
        "seed_number": seed_number,
        "body_ratio": seed["body_ratio"],
        "structure_lookback": seed["structure_lookback"],
        "maximum_distance_atr": seed["maximum_distance_atr"],
        "reward_risk": seed["reward_risk"],
        "slow_daily_ema": seed["slow_daily_ema"],
        "strong_close_max": strong_close,
        "fast_daily_ema": fast_ema,
        "ema_separation_min_daily_atr": separation,
        "minimum_signal_range_atr": min_range,
        "minimum_upper_wick_body": upper_wick,
        "raw_signals": len(eligible),
        "ignored_due_to_open_trade": ignored,
        "still_open_at_end": still_open,
        **calculate_stats(trades),
    }


# ============================================================
# RESEARCH
# ============================================================

def run_research():
    global STATUS

    try:
        print()
        print("=" * 64)
        print("GBP/USD SHORT - DEEP STRUCTURAL + FILTER REFINEMENT")
        print("=" * 64)
        print("ALL HOURS / ALL WEEKDAYS")
        print("NO TIMING OPTIMISATION IN THIS RUN")
        print("Stage 1 combinations:", STAGE1_TOTAL)
        print()

        STATUS.update({
            "state": "fetching_data",
            "message": "Fetching GBP/USD OANDA history",
        })

        h1 = fetch_chunked_history(
            INSTRUMENT,
            "H1",
            RESEARCH_FROM - timedelta(days=H1_WARMUP_DAYS),
            RESEARCH_TO,
        )

        daily = fetch_chunked_history(
            INSTRUMENT,
            "D",
            RESEARCH_FROM - timedelta(days=DAILY_WARMUP_DAYS),
            RESEARCH_TO,
        )

        if not h1:
            raise RuntimeError("No GBP/USD H1 candles returned")

        if not daily:
            raise RuntimeError("No GBP/USD daily candles returned")

        print("H1 candles:", len(h1))
        print("Earliest H1:", h1[0]["time"].isoformat())
        print("Latest H1:", h1[-1]["time"].isoformat())
        print("Daily candles:", len(daily))
        print()

        STATUS.update({
            "state": "precomputing",
            "message": "Building indicators and bearish engulfing feature set",
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

        STATUS["base_bearish_engulfings"] = len(candidates)

        print("Base bearish engulfings:", len(candidates))
        print()

        # ====================================================
        # STAGE 1
        # ====================================================

        STATUS.update({
            "state": "stage1",
            "message": "Running deep core refinement",
        })

        stage1_rows = []

        combos = itertools.product(
            BODY_RATIOS,
            STRUCTURE_LOOKBACKS,
            MAX_DISTANCE_ATR_VALUES,
            REWARD_RISKS,
            SLOW_EMA_LENGTHS,
        )

        for number, combo in enumerate(combos, start=1):
            body, lookback, distance, rr, slow = combo

            eligible = [
                c for c in candidates
                if core_allowed(
                    c,
                    body,
                    lookback,
                    distance,
                    slow,
                )
            ]

            trades, ignored, still_open = simulate(
                h1,
                eligible,
                rr,
            )

            stage1_rows.append(
                make_stage1_row(
                    body,
                    lookback,
                    distance,
                    rr,
                    slow,
                    eligible,
                    trades,
                    ignored,
                    still_open,
                )
            )

            STATUS["stage1_completed"] = number

            if number % 500 == 0:
                print(
                    f"Stage 1: {number}/{STAGE1_TOTAL}",
                    flush=True,
                )

        stage1 = pd.DataFrame(stage1_rows)

        stage1["adequate_100"] = stage1["trades"] >= 100
        stage1["adequate_150"] = stage1["trades"] >= MIN_STAGE2_TRADES

        stage1 = stage1.sort_values(
            by=[
                "adequate_150",
                "profit_factor",
                "expectancy_r",
                "trades",
            ],
            ascending=[
                False,
                False,
                False,
                False,
            ],
        )

        stage1.to_csv(
            STAGE1_OUTPUT,
            index=False,
        )

        STATUS["stage1_output"] = STAGE1_OUTPUT

        print()
        print("=" * 64)
        print("STAGE 1 COMPLETE")
        print("=" * 64)

        display_cols = [
            "body_ratio",
            "structure_lookback",
            "maximum_distance_atr",
            "reward_risk",
            "slow_daily_ema",
            "trades",
            "trades_per_year",
            "win_rate",
            "profit_factor",
            "total_r",
            "expectancy_r",
            "max_drawdown_r",
            "longest_loss_streak",
        ]

        print(
            stage1[
                stage1["trades"] >= MIN_STAGE2_TRADES
            ][display_cols]
            .head(30)
            .to_string(index=False)
        )

        # Select top adequately-sized seeds.
        seeds_df = (
            stage1[
                stage1["trades"] >= MIN_STAGE2_TRADES
            ]
            .head(SEED_COUNT)
            .copy()
        )

        if seeds_df.empty:
            # Safety fallback if sample count unexpectedly collapses.
            seeds_df = stage1.head(SEED_COUNT).copy()

        seed_records = seeds_df.to_dict("records")

        # ====================================================
        # STAGE 2 COUNT
        # ====================================================

        stage2_parameter_sets = []

        for seed_number, seed in enumerate(seed_records, start=1):
            slow = int(seed["slow_daily_ema"])

            for (
                strong_close,
                fast_ema,
                separation,
                min_range,
                upper_wick,
            ) in itertools.product(
                STRONG_CLOSE_THRESHOLDS,
                FAST_EMA_LENGTHS,
                EMA_SEPARATION_THRESHOLDS,
                MIN_RANGE_ATR_VALUES,
                MIN_UPPER_WICK_BODY_VALUES,
            ):
                # Separation requires a fast EMA.
                if fast_ema is None and separation is not None:
                    continue

                # Fast EMA must actually be faster than slow EMA.
                if fast_ema is not None and fast_ema >= slow:
                    continue

                stage2_parameter_sets.append((
                    seed_number,
                    seed,
                    strong_close,
                    fast_ema,
                    separation,
                    min_range,
                    upper_wick,
                ))

        STATUS["stage2_total"] = len(stage2_parameter_sets)

        print()
        print(
            "Stage 2 combinations:",
            len(stage2_parameter_sets),
        )
        print()

        # ====================================================
        # STAGE 2
        # ====================================================

        STATUS.update({
            "state": "stage2",
            "message": "Testing combined quality and daily-alignment filters",
        })

        stage2_rows = []

        # Cache core candidate sets per seed because only extra filters vary.
        seed_candidate_cache = {}

        for seed_number, seed in enumerate(seed_records, start=1):
            seed_candidate_cache[seed_number] = [
                c for c in candidates
                if core_allowed(
                    c,
                    float(seed["body_ratio"]),
                    int(seed["structure_lookback"]),
                    float(seed["maximum_distance_atr"]),
                    int(seed["slow_daily_ema"]),
                )
            ]

        for number, params in enumerate(stage2_parameter_sets, start=1):
            (
                seed_number,
                seed,
                strong_close,
                fast_ema,
                separation,
                min_range,
                upper_wick,
            ) = params

            slow = int(seed["slow_daily_ema"])
            rr = float(seed["reward_risk"])

            core_candidates = seed_candidate_cache[seed_number]

            eligible = [
                c for c in core_candidates
                if extra_filters_allowed(
                    c,
                    slow,
                    strong_close,
                    fast_ema,
                    separation,
                    min_range,
                    upper_wick,
                )
            ]

            trades, ignored, still_open = simulate(
                h1,
                eligible,
                rr,
            )

            stage2_rows.append(
                make_stage2_row(
                    seed_number,
                    seed,
                    strong_close,
                    fast_ema,
                    separation,
                    min_range,
                    upper_wick,
                    eligible,
                    trades,
                    ignored,
                    still_open,
                )
            )

            STATUS["stage2_completed"] = number

            if number % 1000 == 0:
                print(
                    f"Stage 2: {number}/{len(stage2_parameter_sets)}",
                    flush=True,
                )

        stage2 = pd.DataFrame(stage2_rows)

        stage2["adequate_100"] = stage2["trades"] >= 100
        stage2["adequate_120"] = stage2["trades"] >= 120
        stage2["adequate_150"] = stage2["trades"] >= 150

        stage2 = stage2.sort_values(
            by=[
                "adequate_120",
                "profit_factor",
                "expectancy_r",
                "trades",
            ],
            ascending=[
                False,
                False,
                False,
                False,
            ],
        )

        stage2.to_csv(
            STAGE2_OUTPUT,
            index=False,
        )

        STATUS["stage2_output"] = STAGE2_OUTPUT

        print()
        print("=" * 64)
        print("STAGE 2 COMPLETE - TOP >= 120 TRADES")
        print("=" * 64)

        stage2_display = [
            "seed_number",
            "body_ratio",
            "structure_lookback",
            "maximum_distance_atr",
            "reward_risk",
            "slow_daily_ema",
            "strong_close_max",
            "fast_daily_ema",
            "ema_separation_min_daily_atr",
            "minimum_signal_range_atr",
            "minimum_upper_wick_body",
            "trades",
            "trades_per_year",
            "win_rate",
            "profit_factor",
            "total_r",
            "expectancy_r",
            "max_drawdown_r",
            "longest_loss_streak",
        ]

        print(
            stage2[
                stage2["trades"] >= 120
            ][stage2_display]
            .head(40)
            .to_string(index=False)
        )

        print()
        print("Stage 1 saved:", STAGE1_OUTPUT)
        print("Stage 2 saved:", STAGE2_OUTPUT)
        print()

        STATUS.update({
            "state": "complete",
            "message": (
                "GBP/USD deep core and filter refinement completed successfully"
            ),
            "stage1_completed": STAGE1_TOTAL,
            "stage2_completed": len(stage2_parameter_sets),
            "stage1_rows": len(stage1),
            "stage2_rows": len(stage2),
            "earliest_h1": h1[0]["time"].isoformat(),
            "latest_h1": h1[-1]["time"].isoformat(),
        })

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
        "service": "GBPUSD Short Deep Refinement",
        "status": STATUS,
        "instrument": INSTRUMENT,
        "direction": "SHORT",
        "timing_filters": "NONE - all hours and all weekdays",
        "stage1_grid": {
            "body_ratios": BODY_RATIOS,
            "structure_lookbacks": STRUCTURE_LOOKBACKS,
            "maximum_distance_atr": MAX_DISTANCE_ATR_VALUES,
            "reward_risks": REWARD_RISKS,
            "slow_daily_ema": SLOW_EMA_LENGTHS,
            "total": STAGE1_TOTAL,
        },
        "stage2_filters": {
            "strong_close_max": STRONG_CLOSE_THRESHOLDS,
            "fast_daily_ema": FAST_EMA_LENGTHS,
            "ema_separation_min_daily_atr": EMA_SEPARATION_THRESHOLDS,
            "minimum_signal_range_atr": MIN_RANGE_ATR_VALUES,
            "minimum_upper_wick_body": MIN_UPPER_WICK_BODY_VALUES,
            "seed_count": SEED_COUNT,
            "minimum_seed_trades": MIN_STAGE2_TRADES,
        },
        "downloads": {
            "stage1": "/download/stage1",
            "stage2": "/download/stage2",
        },
        "trading_enabled": False,
        "orders_supported": False,
        "executor_connected": False,
    })


@app.route("/status")
def status():
    return jsonify(STATUS)


@app.route("/download/stage1")
def download_stage1():
    if not os.path.exists(STAGE1_OUTPUT):
        return jsonify({
            "status": "not_ready",
            "message": "Stage 1 CSV is not ready yet",
        }), 404

    return send_file(
        STAGE1_OUTPUT,
        as_attachment=True,
        download_name=STAGE1_OUTPUT,
    )


@app.route("/download/stage2")
def download_stage2():
    if not os.path.exists(STAGE2_OUTPUT):
        return jsonify({
            "status": "not_ready",
            "message": "Stage 2 CSV is not ready yet",
        }), 404

    return send_file(
        STAGE2_OUTPUT,
        as_attachment=True,
        download_name=STAGE2_OUTPUT,
    )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    research_thread = threading.Thread(
        target=run_research,
        name="gbpusd-short-deep-refinement",
        daemon=True,
    )

    research_thread.start()

    port = int(os.getenv("PORT", 5000))

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
    )

