import os
import itertools
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# GBP/USD SHORT - EDGE EXTENSION + QUALITY REFINEMENT
#
# RESEARCH ONLY — NEVER SUBMITS ORDERS.
#
# STAGE 1:
#   Fine-map the remaining core edges:
#   body, structure, distance, RR, slow daily EMA.
#
# STAGE 2:
#   Test useful quality/alignment filters around the best cores:
#   strong bearish close, fast daily EMA, EMA separation,
#   minimum signal range.
#
#   Upper-wick filter is intentionally REMOVED because the
#   previous run showed it degraded the population.
#
# STAGE 3:
#   Era validation of the strongest adequately-sized Stage-2
#   candidates. No timing or weekday optimisation yet.
#
# Conventions kept identical to previous short research:
#   OANDA midpoint candles
#   H1
#   Daily alignment 17:00 America/New_York
#   Previous completed daily candle only
#   ATR14 = Wilder/RMA
#   Stop = signal high + 10 ticks
#   Adverse backtest short slippage = 5 ticks
#   Target based on reference signal close
#   Pyramiding = 0
#   Same-H1 SL/TP path approximation retained
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

STAGE1_OUTPUT = "gbpusd_short_edge_extension_core.csv"
STAGE2_OUTPUT = "gbpusd_short_edge_extension_filters.csv"
STAGE3_OUTPUT = "gbpusd_short_era_validation.csv"

# We want frequency, not a tiny hyper-selective strategy.
MIN_STAGE1_SEED_TRADES = 120
MIN_STAGE2_FINAL_TRADES = 80

STAGE1_SEED_COUNT = 16
ERA_CANDIDATE_COUNT = 60


# ============================================================
# STAGE 1 — FINE CORE GRID
#
# This explicitly pushes through the remaining boundaries:
# - structure beyond 60, out to 90
# - distance finely around 0.10–0.15
# - RR around 2.5–2.75, with neighbours
# - EMA around 100, with neighbours
# - body around 1.00–1.20
#
# 5 * 8 * 7 * 5 * 7 = 9,800 combinations
# ============================================================

BODY_RATIOS = [
    1.00,
    1.05,
    1.10,
    1.15,
    1.20,
]

STRUCTURE_LOOKBACKS = [
    50,
    55,
    60,
    65,
    70,
    75,
    80,
    90,
]

MAX_DISTANCE_ATR_VALUES = [
    0.050,
    0.075,
    0.100,
    0.125,
    0.150,
    0.175,
    0.200,
]

REWARD_RISKS = [
    2.25,
    2.50,
    2.75,
    3.00,
    3.25,
]

SLOW_EMA_LENGTHS = [
    75,
    90,
    100,
    110,
    125,
    150,
    175,
]

STAGE1_TOTAL = (
    len(BODY_RATIOS)
    * len(STRUCTURE_LOOKBACKS)
    * len(MAX_DISTANCE_ATR_VALUES)
    * len(REWARD_RISKS)
    * len(SLOW_EMA_LENGTHS)
)


# ============================================================
# STAGE 2 — QUALITY / ALIGNMENT FILTERS
#
# None = OFF.
#
# Strong bearish close:
#   (close - low) / (high - low) <= threshold
#
# Fast EMA alignment:
#   EMAfast < EMAslow
#
# EMA separation:
#   (EMAslow - EMAfast) / Daily ATR14 >= threshold
#
# Minimum signal range:
#   H1 signal range / H1 ATR14 >= threshold
# ============================================================

STRONG_CLOSE_THRESHOLDS = [
    None,
    0.30,
    0.35,
    0.40,
    0.45,
]

FAST_EMA_LENGTHS = [
    None,
    30,
    40,
    50,
    60,
    70,
    85,
]

EMA_SEPARATION_THRESHOLDS = [
    None,
    0.025,
    0.050,
    0.075,
]

MIN_RANGE_ATR_VALUES = [
    None,
    0.70,
    0.90,
    1.00,
    1.10,
    1.20,
    1.30,
]

ALL_DAILY_EMA_LENGTHS = sorted(
    set(
        SLOW_EMA_LENGTHS
        + [x for x in FAST_EMA_LENGTHS if x is not None]
    )
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
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "stage1_total": STAGE1_TOTAL,
    "stage1_completed": 0,
    "stage2_total": None,
    "stage2_completed": 0,
    "stage3_total": None,
    "stage3_completed": 0,
    "stage1_output": None,
    "stage2_output": None,
    "stage3_output": None,
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


def build_h1_daily_lookup(
    h1,
    daily,
    daily_state,
):
    lookup = [None] * len(h1)
    daily_index = -1

    for h1_index, candle in enumerate(h1):
        session_start = current_daily_start(
            candle["time"]
        )

        while (
            daily_index + 1 < len(daily)
            and daily[daily_index + 1]["time"]
            < session_start
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
# BASE SIGNAL FEATURES
# ============================================================

def build_candidates(
    h1,
    h1_atr,
    daily_lookup,
):
    candidates = []

    max_lookback = max(
        STRUCTURE_LOOKBACKS
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
        daily = daily_lookup[index]

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
            previous["close"] > previous["open"]
            and signal["close"] < signal["open"]
            and signal["open"] >= previous["close"]
            and signal["close"] <= previous["open"]
        )

        if not bearish_engulfing:
            continue

        candle_range = (
            signal["high"]
            - signal["low"]
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

            structure_distances[lookback] = (
                previous_highest
                - signal["high"]
            ) / atr

        strong_close = (
            signal["close"]
            - signal["low"]
        ) / candle_range

        range_atr = (
            candle_range
            / atr
        )

        candidates.append({
            "index": index,
            "time": signal["time"],
            "body_ratio": (
                current_body
                / previous_body
            ),
            "structure_distances": structure_distances,
            "strong_close": strong_close,
            "range_atr": range_atr,
            "daily": daily,
        })

    return candidates


# ============================================================
# CORE FILTER
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

    distance = (
        candidate[
            "structure_distances"
        ][
            structure_lookback
        ]
    )

    if distance > maximum_distance_atr:
        return False

    daily = candidate["daily"]

    slow = daily.get(
        f"ema_{slow_ema}"
    )

    if slow is None:
        return False

    # Previous completed daily close must be below slow EMA.
    if not (
        daily["close"]
        < slow
    ):
        return False

    return True


# ============================================================
# EXTRA FILTERS
# ============================================================

def extra_filters_allowed(
    candidate,
    slow_ema,
    strong_close_max,
    fast_ema,
    ema_separation_min,
    minimum_range_atr,
):
    if (
        strong_close_max is not None
        and candidate["strong_close"]
        > strong_close_max
    ):
        return False

    if (
        minimum_range_atr is not None
        and candidate["range_atr"]
        < minimum_range_atr
    ):
        return False

    if fast_ema is None:
        # Separation requires a fast EMA.
        if ema_separation_min is not None:
            return False

        return True

    if fast_ema >= slow_ema:
        return False

    daily = candidate["daily"]

    fast = daily.get(
        f"ema_{fast_ema}"
    )

    slow = daily.get(
        f"ema_{slow_ema}"
    )

    if (
        fast is None
        or slow is None
    ):
        return False

    # Bearish daily EMA alignment.
    if not (
        fast < slow
    ):
        return False

    if ema_separation_min is not None:
        daily_atr = daily.get(
            "atr14"
        )

        if (
            daily_atr is None
            or daily_atr <= 0
        ):
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


def calculate_trade_exit(
    h1,
    signal_index,
    reward_risk,
):
    cache_key = (
        signal_index,
        reward_risk,
    )

    if cache_key in EXIT_CACHE:
        return EXIT_CACHE[
            cache_key
        ]

    signal = h1[
        signal_index
    ]

    reference_entry = (
        signal["close"]
    )

    # For a short, a lower fill is adverse.
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
        * reward_risk
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

            # Same convention as prior short research:
            # whichever extreme is closer to the bar open
            # is treated as occurring first.
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

        result_r = (
            backtest_entry
            - exit_price
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

        EXIT_CACHE[
            cache_key
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
        cache_key
    ] = result

    return result


def simulate(
    h1,
    candidates,
    reward_risk,
):
    trades = []
    position_exit_index = -1
    ignored = 0
    still_open = False

    for candidate in candidates:
        signal_index = candidate["index"]

        # Important exact convention:
        # "<", NOT "<=".
        if (
            signal_index
            < position_exit_index
        ):
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

        position_exit_index = (
            trade["exit_index"]
        )

    return (
        trades,
        ignored,
        still_open,
    )


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

    results = [
        trade["result_r"]
        for trade in trades
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

    years = (
        (
            RESEARCH_TO
            - RESEARCH_FROM
        ).total_seconds()
        / (
            365.2425
            * 24
            * 60
            * 60
        )
    )

    return {
        "trades": len(results),
        "trades_per_year": round(
            len(results) / years,
            2,
        ),
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
        "longest_loss_streak": longest_streak,
    }


def calculate_era_stats(
    trades,
    start,
    end,
):
    era_trades = []

    for trade in trades:
        signal_time = trade[
            "signal_time"
        ]

        if signal_time < start:
            continue

        if (
            end is not None
            and signal_time >= end
        ):
            continue

        era_trades.append(
            trade
        )

    if not era_trades:
        return {
            "trades": 0,
            "pf": 0.0,
            "r": 0.0,
            "expectancy": 0.0,
            "win_rate": 0.0,
        }

    results = [
        trade["result_r"]
        for trade in era_trades
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

    if gross_loss > 0:
        pf = gross_profit / gross_loss
    elif gross_profit > 0:
        pf = 999.0
    else:
        pf = 0.0

    total_r = sum(results)

    return {
        "trades": len(results),
        "pf": round(pf, 3),
        "r": round(total_r, 2),
        "expectancy": round(
            total_r / len(results),
            3,
        ),
        "win_rate": round(
            len(winners)
            / len(results)
            * 100.0,
            2,
        ),
    }


# ============================================================
# ROW HELPERS
# ============================================================

def stage1_row(
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


def stage2_row(
    seed_number,
    seed,
    strong_close,
    fast_ema,
    separation,
    min_range,
    eligible,
    trades,
    ignored,
    still_open,
):
    return {
        "seed_number": seed_number,
        "body_ratio": float(seed["body_ratio"]),
        "structure_lookback": int(seed["structure_lookback"]),
        "maximum_distance_atr": float(
            seed["maximum_distance_atr"]
        ),
        "reward_risk": float(seed["reward_risk"]),
        "slow_daily_ema": int(seed["slow_daily_ema"]),
        "strong_close_max": strong_close,
        "fast_daily_ema": fast_ema,
        "ema_separation_min_daily_atr": separation,
        "minimum_signal_range_atr": min_range,
        "raw_signals": len(eligible),
        "ignored_due_to_open_trade": ignored,
        "still_open_at_end": still_open,
        **calculate_stats(trades),
    }


# ============================================================
# STAGE 3 REBUILD CANDIDATE TRADES
# ============================================================

def trades_for_stage2_row(
    h1,
    all_candidates,
    row,
):
    body = float(row["body_ratio"])
    lookback = int(row["structure_lookback"])
    distance = float(
        row["maximum_distance_atr"]
    )
    rr = float(row["reward_risk"])
    slow = int(row["slow_daily_ema"])

    strong_close = row[
        "strong_close_max"
    ]
    fast_ema = row[
        "fast_daily_ema"
    ]
    separation = row[
        "ema_separation_min_daily_atr"
    ]
    min_range = row[
        "minimum_signal_range_atr"
    ]

    # Pandas turns None into NaN.
    if pd.isna(strong_close):
        strong_close = None

    if pd.isna(fast_ema):
        fast_ema = None
    else:
        fast_ema = int(fast_ema)

    if pd.isna(separation):
        separation = None

    if pd.isna(min_range):
        min_range = None

    eligible = []

    for candidate in all_candidates:
        if not core_allowed(
            candidate,
            body,
            lookback,
            distance,
            slow,
        ):
            continue

        if not extra_filters_allowed(
            candidate,
            slow,
            strong_close,
            fast_ema,
            separation,
            min_range,
        ):
            continue

        eligible.append(candidate)

    trades, ignored, still_open = simulate(
        h1,
        eligible,
        rr,
    )

    return (
        eligible,
        trades,
        ignored,
        still_open,
    )


# ============================================================
# RESEARCH
# ============================================================

def run_research():
    global STATUS

    try:
        print()
        print("=" * 70)
        print("GBP/USD SHORT — EDGE EXTENSION + QUALITY REFINEMENT")
        print("=" * 70)
        print("ALL HOURS")
        print("ALL WEEKDAYS")
        print("NO TIMING OPTIMISATION")
        print("UPPER-WICK FILTER REMOVED")
        print("Stage 1 combinations:", STAGE1_TOTAL)
        print()

        # ----------------------------------------------------
        # DATA
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # PRECOMPUTE
        # ----------------------------------------------------

        STATUS.update({
            "state": "precomputing",
            "message": (
                "Building H1 ATR, daily EMA/ATR "
                "and bearish-engulfing feature set"
            ),
        })

        h1_atr = atr_series(
            h1,
            14,
        )

        daily_state = build_daily_state(
            daily
        )

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
        print()

        # ====================================================
        # STAGE 1
        # ====================================================

        STATUS.update({
            "state": "stage1",
            "message": (
                "Fine-mapping core structural edges"
            ),
        })

        stage1_rows = []

        combinations = itertools.product(
            BODY_RATIOS,
            STRUCTURE_LOOKBACKS,
            MAX_DISTANCE_ATR_VALUES,
            REWARD_RISKS,
            SLOW_EMA_LENGTHS,
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
            ) = combo

            eligible = [
                candidate
                for candidate in candidates
                if core_allowed(
                    candidate,
                    body,
                    lookback,
                    distance,
                    slow,
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

            stage1_rows.append(
                stage1_row(
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

            STATUS[
                "stage1_completed"
            ] = number

            if number % 500 == 0:
                print(
                    f"Stage 1: "
                    f"{number}/{STAGE1_TOTAL}",
                    flush=True,
                )

        stage1 = pd.DataFrame(
            stage1_rows
        )

        stage1[
            "adequate_100"
        ] = (
            stage1["trades"]
            >= 100
        )

        stage1[
            "adequate_120"
        ] = (
            stage1["trades"]
            >= MIN_STAGE1_SEED_TRADES
        )

        stage1 = stage1.sort_values(
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

        stage1.to_csv(
            STAGE1_OUTPUT,
            index=False,
        )

        STATUS[
            "stage1_output"
        ] = STAGE1_OUTPUT

        print()
        print("=" * 70)
        print(
            "STAGE 1 COMPLETE — TOP >= 120 TRADES"
        )
        print("=" * 70)

        stage1_display = [
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
                stage1["trades"]
                >= MIN_STAGE1_SEED_TRADES
            ][stage1_display]
            .head(40)
            .to_string(index=False)
        )

        seeds_df = (
            stage1[
                stage1["trades"]
                >= MIN_STAGE1_SEED_TRADES
            ]
            .head(STAGE1_SEED_COUNT)
            .copy()
        )

        if seeds_df.empty:
            seeds_df = (
                stage1
                .head(STAGE1_SEED_COUNT)
                .copy()
            )

        seed_records = (
            seeds_df
            .to_dict("records")
        )

        # ====================================================
        # STAGE 2 PARAMETER LIST
        # ====================================================

        stage2_parameter_sets = []

        for seed_number, seed in enumerate(
            seed_records,
            start=1,
        ):
            slow = int(
                seed["slow_daily_ema"]
            )

            for (
                strong_close,
                fast_ema,
                separation,
                min_range,
            ) in itertools.product(
                STRONG_CLOSE_THRESHOLDS,
                FAST_EMA_LENGTHS,
                EMA_SEPARATION_THRESHOLDS,
                MIN_RANGE_ATR_VALUES,
            ):
                if (
                    fast_ema is None
                    and separation is not None
                ):
                    continue

                if (
                    fast_ema is not None
                    and fast_ema >= slow
                ):
                    continue

                stage2_parameter_sets.append((
                    seed_number,
                    seed,
                    strong_close,
                    fast_ema,
                    separation,
                    min_range,
                ))

        STATUS[
            "stage2_total"
        ] = len(
            stage2_parameter_sets
        )

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
            "message": (
                "Testing close quality, daily alignment, "
                "EMA separation and minimum range"
            ),
        })

        # Cache each seed's core candidates.
        seed_candidate_cache = {}

        for seed_number, seed in enumerate(
            seed_records,
            start=1,
        ):
            seed_candidate_cache[
                seed_number
            ] = [
                candidate
                for candidate in candidates
                if core_allowed(
                    candidate,
                    float(seed["body_ratio"]),
                    int(seed["structure_lookback"]),
                    float(
                        seed[
                            "maximum_distance_atr"
                        ]
                    ),
                    int(seed["slow_daily_ema"]),
                )
            ]

        stage2_rows = []

        for number, params in enumerate(
            stage2_parameter_sets,
            start=1,
        ):
            (
                seed_number,
                seed,
                strong_close,
                fast_ema,
                separation,
                min_range,
            ) = params

            slow = int(
                seed["slow_daily_ema"]
            )

            rr = float(
                seed["reward_risk"]
            )

            core_candidates = (
                seed_candidate_cache[
                    seed_number
                ]
            )

            eligible = [
                candidate
                for candidate in core_candidates
                if extra_filters_allowed(
                    candidate,
                    slow,
                    strong_close,
                    fast_ema,
                    separation,
                    min_range,
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

            stage2_rows.append(
                stage2_row(
                    seed_number,
                    seed,
                    strong_close,
                    fast_ema,
                    separation,
                    min_range,
                    eligible,
                    trades,
                    ignored,
                    still_open,
                )
            )

            STATUS[
                "stage2_completed"
            ] = number

            if number % 1000 == 0:
                print(
                    f"Stage 2: "
                    f"{number}/"
                    f"{len(stage2_parameter_sets)}",
                    flush=True,
                )

        stage2 = pd.DataFrame(
            stage2_rows
        )

        stage2[
            "adequate_80"
        ] = (
            stage2["trades"]
            >= MIN_STAGE2_FINAL_TRADES
        )

        stage2[
            "adequate_100"
        ] = (
            stage2["trades"]
            >= 100
        )

        stage2[
            "adequate_120"
        ] = (
            stage2["trades"]
            >= 120
        )

        stage2 = stage2.sort_values(
            by=[
                "adequate_100",
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

        STATUS[
            "stage2_output"
        ] = STAGE2_OUTPUT

        print()
        print("=" * 70)
        print(
            "STAGE 2 COMPLETE — TOP >= 80 TRADES"
        )
        print("=" * 70)

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
                stage2["trades"]
                >= MIN_STAGE2_FINAL_TRADES
            ][stage2_display]
            .head(50)
            .to_string(index=False)
        )

        # ====================================================
        # STAGE 3 — ERA VALIDATION
        # ====================================================

        era_candidates = (
            stage2[
                stage2["trades"]
                >= MIN_STAGE2_FINAL_TRADES
            ]
            .head(ERA_CANDIDATE_COUNT)
            .copy()
        )

        if era_candidates.empty:
            era_candidates = (
                stage2
                .head(ERA_CANDIDATE_COUNT)
                .copy()
            )

        STATUS[
            "stage3_total"
        ] = len(
            era_candidates
        )

        STATUS.update({
            "state": "stage3",
            "message": (
                "Running era robustness on strongest "
                "adequately-sized candidates"
            ),
        })

        era_rows = []

        for number, (_, row) in enumerate(
            era_candidates.iterrows(),
            start=1,
        ):
            (
                eligible,
                trades,
                ignored,
                still_open,
            ) = trades_for_stage2_row(
                h1,
                candidates,
                row,
            )

            output = {
                "candidate_rank": number,
                "body_ratio": float(
                    row["body_ratio"]
                ),
                "structure_lookback": int(
                    row["structure_lookback"]
                ),
                "maximum_distance_atr": float(
                    row[
                        "maximum_distance_atr"
                    ]
                ),
                "reward_risk": float(
                    row["reward_risk"]
                ),
                "slow_daily_ema": int(
                    row["slow_daily_ema"]
                ),
                "strong_close_max": (
                    None
                    if pd.isna(
                        row["strong_close_max"]
                    )
                    else float(
                        row["strong_close_max"]
                    )
                ),
                "fast_daily_ema": (
                    None
                    if pd.isna(
                        row["fast_daily_ema"]
                    )
                    else int(
                        row["fast_daily_ema"]
                    )
                ),
                "ema_separation_min_daily_atr": (
                    None
                    if pd.isna(
                        row[
                            "ema_separation_min_daily_atr"
                        ]
                    )
                    else float(
                        row[
                            "ema_separation_min_daily_atr"
                        ]
                    )
                ),
                "minimum_signal_range_atr": (
                    None
                    if pd.isna(
                        row[
                            "minimum_signal_range_atr"
                        ]
                    )
                    else float(
                        row[
                            "minimum_signal_range_atr"
                        ]
                    )
                ),
                "full_trades": len(trades),
                "full_profit_factor": float(
                    row["profit_factor"]
                ),
                "full_total_r": float(
                    row["total_r"]
                ),
                "full_expectancy_r": float(
                    row["expectancy_r"]
                ),
                "full_max_drawdown_r": float(
                    row["max_drawdown_r"]
                ),
                "full_longest_loss_streak": int(
                    row["longest_loss_streak"]
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
                stats = calculate_era_stats(
                    trades,
                    era_start,
                    era_end,
                )

                output[
                    f"{era_name}_trades"
                ] = stats["trades"]

                output[
                    f"{era_name}_pf"
                ] = stats["pf"]

                output[
                    f"{era_name}_r"
                ] = stats["r"]

                output[
                    f"{era_name}_expectancy"
                ] = stats["expectancy"]

                output[
                    f"{era_name}_win_rate"
                ] = stats["win_rate"]

                if stats["r"] > 0:
                    profitable_eras += 1

                if stats["trades"] >= 5:
                    eras_with_5_plus += 1

                    if stats["r"] > 0:
                        profitable_eras_with_5_plus += 1

                    if minimum_era_pf_5_plus is None:
                        minimum_era_pf_5_plus = stats["pf"]
                    else:
                        minimum_era_pf_5_plus = min(
                            minimum_era_pf_5_plus,
                            stats["pf"],
                        )

                    if minimum_era_expectancy_5_plus is None:
                        minimum_era_expectancy_5_plus = (
                            stats["expectancy"]
                        )
                    else:
                        minimum_era_expectancy_5_plus = min(
                            minimum_era_expectancy_5_plus,
                            stats["expectancy"],
                        )

            output[
                "profitable_eras"
            ] = profitable_eras

            output[
                "eras_with_5_plus_trades"
            ] = eras_with_5_plus

            output[
                "profitable_eras_with_5_plus_trades"
            ] = profitable_eras_with_5_plus

            output[
                "minimum_era_pf_5_plus"
            ] = minimum_era_pf_5_plus

            output[
                "minimum_era_expectancy_5_plus"
            ] = minimum_era_expectancy_5_plus

            era_rows.append(
                output
            )

            STATUS[
                "stage3_completed"
            ] = number

        stage3 = pd.DataFrame(
            era_rows
        )

        # Put broad era consistency before headline full-history PF.
        stage3 = stage3.sort_values(
            by=[
                "profitable_eras_with_5_plus_trades",
                "minimum_era_pf_5_plus",
                "full_profit_factor",
                "full_expectancy_r",
                "full_trades",
            ],
            ascending=[
                False,
                False,
                False,
                False,
                False,
            ],
        )

        stage3.to_csv(
            STAGE3_OUTPUT,
            index=False,
        )

        STATUS[
            "stage3_output"
        ] = STAGE3_OUTPUT

        print()
        print("=" * 70)
        print("STAGE 3 COMPLETE — ERA ROBUSTNESS")
        print("=" * 70)

        era_display = [
            "body_ratio",
            "structure_lookback",
            "maximum_distance_atr",
            "reward_risk",
            "slow_daily_ema",
            "strong_close_max",
            "fast_daily_ema",
            "ema_separation_min_daily_atr",
            "minimum_signal_range_atr",
            "full_trades",
            "full_profit_factor",
            "full_total_r",
            "full_expectancy_r",
            "full_max_drawdown_r",
            "profitable_eras",
            "minimum_era_pf_5_plus",
            "minimum_era_expectancy_5_plus",
            "2002_2009_trades",
            "2002_2009_pf",
            "2002_2009_r",
            "2010_2017_trades",
            "2010_2017_pf",
            "2010_2017_r",
            "2018_2023_trades",
            "2018_2023_pf",
            "2018_2023_r",
            "2024_present_trades",
            "2024_present_pf",
            "2024_present_r",
        ]

        print(
            stage3[
                era_display
            ]
            .head(30)
            .to_string(index=False)
        )

        STATUS.update({
            "state": "complete",
            "message": (
                "GBP/USD edge-extension, filter refinement "
                "and era validation completed successfully"
            ),
            "stage1_completed": STAGE1_TOTAL,
            "stage2_completed": len(
                stage2_parameter_sets
            ),
            "stage3_completed": len(
                era_candidates
            ),
            "stage1_rows": len(stage1),
            "stage2_rows": len(stage2),
            "stage3_rows": len(stage3),
            "earliest_h1": (
                h1[0]["time"].isoformat()
            ),
            "latest_h1": (
                h1[-1]["time"].isoformat()
            ),
        })

        print()
        print(
            "Saved:",
            STAGE1_OUTPUT,
        )
        print(
            "Saved:",
            STAGE2_OUTPUT,
        )
        print(
            "Saved:",
            STAGE3_OUTPUT,
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
            "GBPUSD Short Edge Extension "
            "+ Era Validation"
        ),
        "status": STATUS,
        "instrument": INSTRUMENT,
        "direction": "SHORT",
        "timing_filters": (
            "NONE — all hours and all weekdays"
        ),
        "upper_wick_filter": (
            "REMOVED after prior population test"
        ),
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
            "seed_count": STAGE1_SEED_COUNT,
            "minimum_seed_trades": MIN_STAGE1_SEED_TRADES,
        },
        "era_windows": [
            "2002-2009",
            "2010-2017",
            "2018-2023",
            "2024-present",
        ],
        "downloads": {
            "stage1": "/download/stage1",
            "stage2": "/download/stage2",
            "stage3": "/download/stage3",
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


@app.route("/download/stage1")
def download_stage1():
    if not os.path.exists(
        STAGE1_OUTPUT
    ):
        return jsonify({
            "status": "not_ready",
            "message": (
                "Stage 1 CSV is not ready yet"
            ),
        }), 404

    return send_file(
        STAGE1_OUTPUT,
        as_attachment=True,
        download_name=STAGE1_OUTPUT,
    )


@app.route("/download/stage2")
def download_stage2():
    if not os.path.exists(
        STAGE2_OUTPUT
    ):
        return jsonify({
            "status": "not_ready",
            "message": (
                "Stage 2 CSV is not ready yet"
            ),
        }), 404

    return send_file(
        STAGE2_OUTPUT,
        as_attachment=True,
        download_name=STAGE2_OUTPUT,
    )


@app.route("/download/stage3")
def download_stage3():
    if not os.path.exists(
        STAGE3_OUTPUT
    ):
        return jsonify({
            "status": "not_ready",
            "message": (
                "Stage 3 CSV is not ready yet"
            ),
        }), 404

    return send_file(
        STAGE3_OUTPUT,
        as_attachment=True,
        download_name=STAGE3_OUTPUT,
    )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    research_thread = threading.Thread(
        target=run_research,
        name=(
            "gbpusd-short-edge-extension"
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
