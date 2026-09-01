import os
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# EUR/GBP SHORT - FEATURE DISCOVERY
#
# RESEARCH ONLY — NEVER SUBMITS ORDERS.
#
# Fixed viable core entering this stage:
#
#   bearish engulfing
#   minimum body ratio >= 1.00
#   structure lookback = 90 H1 bars
#   signal high within 0.075 ATR14 of previous 90-bar highest high
#   signal range >= 1.10 ATR14
#   bearish close location <= 0.20
#   RR = 3.00
#   stop = signal high + 10 ticks
#   adverse short slippage = 5 ticks
#   pyramiding = 0
#
# Goal:
#   Test additional feature families ONE AT A TIME around this
#   already-viable core, mirroring the successful USD/CAD process.
#
# Feature families:
#   1) Daily close below EMA
#   2) Upward momentum over 12h / 24h / 48h
#   3) Maximum stop size / ATR14
#   4) Minimum signal body size / ATR14
#   5) Minimum upper wick / body
#   6) ATR14 relative to 50-bar ATR mean
#   7) H1 close relative to EMA20 / EMA50
#   8) H1 EMA20 slope over 6 / 12 / 24 bars
#   9) Time since prior 90-bar high
#
# IMPORTANT:
#   - No feature combinations yet.
#   - The fixed core remains identical for every test.
#   - Baseline control row included.
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
MAX_DISTANCE_ATR = 0.075
MIN_RANGE_ATR = 1.10
MAX_CLOSE_LOCATION = 0.20

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

H1_WARMUP_DAYS = 600
DAILY_WARMUP_DAYS = 3500

NY_TZ = ZoneInfo("America/New_York")
DAILY_ALIGNMENT_HOUR = 17
DAILY_ALIGNMENT_TIMEZONE = "America/New_York"

OUTPUT_FILE = "eurgbp_short_feature_discovery.csv"


# ============================================================
# FEATURE SWEEP VALUES
# ============================================================

DAILY_EMA_LENGTHS = [
    50, 70, 100, 150, 200, 250, 300, 400
]

MOMENTUM_LOOKBACKS = [
    12, 24, 48
]

MIN_UP_MOMENTUM_ATR_THRESHOLDS = [
    -0.50, -0.25, 0.00, 0.25, 0.50,
    0.75, 1.00, 1.25, 1.50
]

MAX_STOP_SIZE_ATR_THRESHOLDS = [
    0.90, 1.00, 1.10, 1.20, 1.30,
    1.40, 1.50, 1.60, 1.80, 2.00, 2.50
]

MIN_BODY_ATR_THRESHOLDS = [
    0.40, 0.50, 0.60, 0.70, 0.80,
    0.90, 1.00, 1.10, 1.25, 1.50
]

MIN_UPPER_WICK_BODY_THRESHOLDS = [
    0.00, 0.10, 0.20, 0.30, 0.40,
    0.50, 0.75
]

ATR_RATIO_THRESHOLDS = [
    0.70, 0.80, 0.90, 1.00, 1.10, 1.20, 1.30
]

H1_EMA_LENGTHS = [
    20, 50
]

EMA20_SLOPE_LOOKBACKS = [
    6, 12, 24
]

EMA20_SLOPE_THRESHOLDS_ATR = [
    -0.50, -0.25, 0.00, 0.10, 0.20, 0.30, 0.50
]

MAX_BARS_SINCE_PRIOR_HIGH = [
    1, 2, 3, 5, 8, 12, 18, 24, 36, 48, 72, 89
]


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
    "service": "EURGBP Short Feature Discovery",
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

        result.append(
            tr
        )

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


def rolling_mean(values, length):
    result = [None] * len(values)

    running_sum = 0.0
    queue = []

    for index, value in enumerate(
        values
    ):
        if value is None:
            queue.append(None)
        else:
            queue.append(value)
            running_sum += value

        if len(queue) > length:
            removed = queue.pop(0)
            if removed is not None:
                running_sum -= removed

        if (
            len(queue) == length
            and all(
                item is not None
                for item in queue
            )
        ):
            result[index] = (
                running_sum
                / length
            )

    return result


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

    ema_map = {
        length: ema_series(
            closes,
            length,
        )
        for length in DAILY_EMA_LENGTHS
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
                for length
                in DAILY_EMA_LENGTHS
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
# CANDIDATE FEATURES
# ============================================================

def build_candidates(
    h1,
    h1_atr,
    atr_mean_50,
    h1_ema_map,
    daily_lookup,
):
    candidates = []

    max_lookback = max(
        STRUCTURE_LOOKBACK,
        max(MOMENTUM_LOOKBACKS),
        max(EMA20_SLOPE_LOOKBACKS),
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

        if body_ratio < MIN_BODY_RATIO:
            continue

        close_location = (
            signal["close"]
            - signal["low"]
        ) / candle_range

        range_atr = (
            candle_range
            / atr
        )

        previous_slice = h1[
            index - STRUCTURE_LOOKBACK:
            index
        ]

        previous_highest = max(
            candle["high"]
            for candle
            in previous_slice
        )

        structure_distance_atr = (
            previous_highest
            - signal["high"]
        ) / atr

        # Fixed viable core.
        if (
            structure_distance_atr
            > MAX_DISTANCE_ATR
        ):
            continue

        if range_atr < MIN_RANGE_ATR:
            continue

        if (
            close_location
            > MAX_CLOSE_LOCATION
        ):
            continue

        stop = (
            signal["high"]
            + STOP_BUFFER_TICKS
            * TICK_SIZE
        )

        stop_size_atr = (
            stop
            - signal["close"]
        ) / atr

        body_atr = (
            current_body
            / atr
        )

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

        atr_ratio_50 = None

        if (
            atr_mean_50[index]
            is not None
            and atr_mean_50[index] > 0
        ):
            atr_ratio_50 = (
                atr
                / atr_mean_50[index]
            )

        ema20 = h1_ema_map[
            20
        ][index]

        ema50 = h1_ema_map[
            50
        ][index]

        close_vs_ema20_atr = None
        close_vs_ema50_atr = None

        if ema20 is not None:
            close_vs_ema20_atr = (
                signal["close"]
                - ema20
            ) / atr

        if ema50 is not None:
            close_vs_ema50_atr = (
                signal["close"]
                - ema50
            ) / atr

        ema20_slopes = {}

        for lookback in (
            EMA20_SLOPE_LOOKBACKS
        ):
            past_ema = h1_ema_map[
                20
            ][
                index - lookback
            ]

            if (
                ema20 is None
                or past_ema is None
            ):
                ema20_slopes[
                    lookback
                ] = None

            else:
                ema20_slopes[
                    lookback
                ] = (
                    ema20
                    - past_ema
                ) / atr

        prior_high_index = None

        for offset in range(
            1,
            STRUCTURE_LOOKBACK + 1,
        ):
            candidate_index = (
                index - offset
            )

            if (
                abs(
                    h1[
                        candidate_index
                    ]["high"]
                    - previous_highest
                )
                <= 1e-12
            ):
                prior_high_index = (
                    candidate_index
                )
                break

        bars_since_prior_high = None

        if prior_high_index is not None:
            bars_since_prior_high = (
                index
                - prior_high_index
            )

        candidates.append({
            "index": index,
            "time": signal["time"],
            "daily": daily_lookup[index],
            "momentum": momentum,
            "stop_size_atr": (
                stop_size_atr
            ),
            "body_atr": body_atr,
            "upper_wick_body": (
                upper_wick_body
            ),
            "atr_ratio_50": (
                atr_ratio_50
            ),
            "close_vs_ema20_atr": (
                close_vs_ema20_atr
            ),
            "close_vs_ema50_atr": (
                close_vs_ema50_atr
            ),
            "ema20_slopes": (
                ema20_slopes
            ),
            "bars_since_prior_high": (
                bars_since_prior_high
            ),
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
        "family": family,
        "test_label": test_label,
        "parameter_1_name": parameter_1_name,
        "parameter_1_value": parameter_1_value,
        "parameter_2_name": parameter_2_name,
        "parameter_2_value": parameter_2_value,
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

            pf = era[
                "profit_factor"
            ]

            expectancy = era[
                "expectancy_r"
            ]

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
        "adequate_90_trades"
    ] = (
        full["trades"] >= 90
    )

    row[
        "frequency_4py"
    ] = (
        full["trades"]
        / years
        >= 4.0
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
            "EUR/GBP SHORT - FEATURE DISCOVERY"
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
                "Building indicators and fixed-core candidates"
            ),
        })

        h1_atr = atr_series(
            h1,
            14,
        )

        atr_mean_50 = rolling_mean(
            h1_atr,
            50,
        )

        h1_closes = [
            candle["close"]
            for candle in h1
        ]

        h1_ema_map = {
            length: ema_series(
                h1_closes,
                length,
            )
            for length in H1_EMA_LENGTHS
        }

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

        base_candidates = (
            build_candidates(
                h1,
                h1_atr,
                atr_mean_50,
                h1_ema_map,
                daily_lookup,
            )
        )

        STATUS[
            "fixed_core_signals"
        ] = len(
            base_candidates
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

        STATUS.update({
            "state": "running",
            "message": (
                "Running EUR/GBP feature discovery"
            ),
        })

        # ----------------------------------------------------
        # BASELINE CONTROL
        # ----------------------------------------------------

        run_test(
            rows,
            h1,
            years,
            base_candidates,
            "BASELINE",
            "fixed_core_only",
            lambda candidate: True,
        )

        # ----------------------------------------------------
        # DAILY CLOSE BELOW EMA
        # ----------------------------------------------------

        for length in DAILY_EMA_LENGTHS:
            run_test(
                rows,
                h1,
                years,
                base_candidates,
                "DAILY_CLOSE_BELOW_EMA",
                f"daily_close_below_ema_{length}",
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
                "slow_daily_ema",
                length,
            )

        # ----------------------------------------------------
        # UPWARD MOMENTUM
        # ----------------------------------------------------

        for lookback in MOMENTUM_LOOKBACKS:
            for threshold in (
                MIN_UP_MOMENTUM_ATR_THRESHOLDS
            ):
                run_test(
                    rows,
                    h1,
                    years,
                    base_candidates,
                    "UP_MOMENTUM",
                    (
                        f"up_momentum_{lookback}h_"
                        f"gte_{threshold:.2f}"
                    ),
                    lambda candidate,
                    lb=lookback,
                    t=threshold:
                        candidate[
                            "momentum"
                        ][lb] >= t,
                    "momentum_lookback_h",
                    lookback,
                    "min_up_momentum_atr",
                    threshold,
                )

        # ----------------------------------------------------
        # STOP SIZE
        # ----------------------------------------------------

        for threshold in (
            MAX_STOP_SIZE_ATR_THRESHOLDS
        ):
            run_test(
                rows,
                h1,
                years,
                base_candidates,
                "MAX_STOP_SIZE_ATR",
                f"stop_size_lte_{threshold:.2f}",
                lambda candidate,
                t=threshold:
                    candidate[
                        "stop_size_atr"
                    ] <= t,
                "max_stop_size_atr",
                threshold,
            )

        # ----------------------------------------------------
        # BODY SIZE / ATR
        # ----------------------------------------------------

        for threshold in (
            MIN_BODY_ATR_THRESHOLDS
        ):
            run_test(
                rows,
                h1,
                years,
                base_candidates,
                "MIN_BODY_ATR",
                f"body_atr_gte_{threshold:.2f}",
                lambda candidate,
                t=threshold:
                    candidate[
                        "body_atr"
                    ] >= t,
                "min_body_atr",
                threshold,
            )

        # ----------------------------------------------------
        # UPPER WICK / BODY
        # ----------------------------------------------------

        for threshold in (
            MIN_UPPER_WICK_BODY_THRESHOLDS
        ):
            run_test(
                rows,
                h1,
                years,
                base_candidates,
                "MIN_UPPER_WICK_BODY",
                (
                    f"upper_wick_body_gte_"
                    f"{threshold:.2f}"
                ),
                lambda candidate,
                t=threshold:
                    candidate[
                        "upper_wick_body"
                    ] >= t,
                "min_upper_wick_body",
                threshold,
            )

        # ----------------------------------------------------
        # ATR REGIME
        # ----------------------------------------------------

        for threshold in (
            ATR_RATIO_THRESHOLDS
        ):
            run_test(
                rows,
                h1,
                years,
                base_candidates,
                "MIN_ATR14_VS_ATR50_MEAN",
                (
                    f"atr14_vs_mean50_gte_"
                    f"{threshold:.2f}"
                ),
                lambda candidate,
                t=threshold:
                    (
                        candidate[
                            "atr_ratio_50"
                        ] is not None
                        and candidate[
                            "atr_ratio_50"
                        ] >= t
                    ),
                "min_atr14_ratio_50",
                threshold,
            )

        # ----------------------------------------------------
        # H1 CLOSE VS EMA
        # ----------------------------------------------------

        run_test(
            rows,
            h1,
            years,
            base_candidates,
            "H1_CLOSE_VS_EMA20",
            "close_below_ema20",
            lambda candidate:
                (
                    candidate[
                        "close_vs_ema20_atr"
                    ] is not None
                    and candidate[
                        "close_vs_ema20_atr"
                    ] < 0
                ),
            "condition",
            "close_below_ema20",
        )

        run_test(
            rows,
            h1,
            years,
            base_candidates,
            "H1_CLOSE_VS_EMA20",
            "close_above_ema20",
            lambda candidate:
                (
                    candidate[
                        "close_vs_ema20_atr"
                    ] is not None
                    and candidate[
                        "close_vs_ema20_atr"
                    ] > 0
                ),
            "condition",
            "close_above_ema20",
        )

        run_test(
            rows,
            h1,
            years,
            base_candidates,
            "H1_CLOSE_VS_EMA50",
            "close_below_ema50",
            lambda candidate:
                (
                    candidate[
                        "close_vs_ema50_atr"
                    ] is not None
                    and candidate[
                        "close_vs_ema50_atr"
                    ] < 0
                ),
            "condition",
            "close_below_ema50",
        )

        run_test(
            rows,
            h1,
            years,
            base_candidates,
            "H1_CLOSE_VS_EMA50",
            "close_above_ema50",
            lambda candidate:
                (
                    candidate[
                        "close_vs_ema50_atr"
                    ] is not None
                    and candidate[
                        "close_vs_ema50_atr"
                    ] > 0
                ),
            "condition",
            "close_above_ema50",
        )

        # ----------------------------------------------------
        # EMA20 SLOPE
        # ----------------------------------------------------

        for lookback in (
            EMA20_SLOPE_LOOKBACKS
        ):
            for threshold in (
                EMA20_SLOPE_THRESHOLDS_ATR
            ):
                run_test(
                    rows,
                    h1,
                    years,
                    base_candidates,
                    "EMA20_SLOPE",
                    (
                        f"ema20_slope_{lookback}h_"
                        f"gte_{threshold:.2f}_atr"
                    ),
                    lambda candidate,
                    lb=lookback,
                    t=threshold:
                        (
                            candidate[
                                "ema20_slopes"
                            ][lb] is not None
                            and candidate[
                                "ema20_slopes"
                            ][lb] >= t
                        ),
                    "slope_lookback_h",
                    lookback,
                    "min_slope_atr",
                    threshold,
                )

        # ----------------------------------------------------
        # TIME SINCE PRIOR 90-BAR HIGH
        # ----------------------------------------------------

        for threshold in (
            MAX_BARS_SINCE_PRIOR_HIGH
        ):
            run_test(
                rows,
                h1,
                years,
                base_candidates,
                "MAX_BARS_SINCE_PRIOR_HIGH",
                (
                    f"bars_since_prior_high_lte_"
                    f"{threshold}"
                ),
                lambda candidate,
                t=threshold:
                    (
                        candidate[
                            "bars_since_prior_high"
                        ] is not None
                        and candidate[
                            "bars_since_prior_high"
                        ] <= t
                    ),
                "max_bars_since_prior_high",
                threshold,
            )

        df = pd.DataFrame(
            rows
        )

        if df.empty:
            raise RuntimeError(
                "No result rows generated"
            )

        df[
            "pf_150"
        ] = (
            df[
                "profit_factor"
            ] >= 1.50
        )

        df[
            "pf_160"
        ] = (
            df[
                "profit_factor"
            ] >= 1.60
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
            "worst_era_pf_120"
        ] = (
            df[
                "minimum_era_pf_5_plus"
            ].fillna(0)
            >= 1.20
        )

        df = df.sort_values(
            by=[
                "all_four_eras_profitable",
                "adequate_90_trades",
                "frequency_4py",
                "worst_era_pf_120",
                "worst_era_pf_110",
                "pf_160",
                "pf_150",
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
                "EUR/GBP feature discovery "
                "completed successfully"
            ),
            "rows_saved": len(
                df
            ),
            "fixed_core_signals": (
                len(base_candidates)
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
            "EUR/GBP FEATURE DISCOVERY COMPLETE"
        )
        print("=" * 76)
        print(
            "Fixed-core signals:",
            len(base_candidates),
        )
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
            "EURGBP Short Feature Discovery"
        ),
        "status": STATUS,
        "instrument": INSTRUMENT,
        "direction": "SHORT",
        "trading_enabled": False,
        "orders_supported": False,
        "executor_connected": False,
        "fixed_core": {
            "minimum_body_ratio": MIN_BODY_RATIO,
            "structure_lookback": STRUCTURE_LOOKBACK,
            "max_distance_atr": MAX_DISTANCE_ATR,
            "min_range_atr": MIN_RANGE_ATR,
            "max_close_location": MAX_CLOSE_LOCATION,
            "reward_risk": REWARD_RISK,
            "stop_buffer_ticks": STOP_BUFFER_TICKS,
            "backtest_slippage_ticks": (
                BACKTEST_SLIPPAGE_TICKS
            ),
        },
        "families": [
            "BASELINE",
            "DAILY_CLOSE_BELOW_EMA",
            "UP_MOMENTUM",
            "MAX_STOP_SIZE_ATR",
            "MIN_BODY_ATR",
            "MIN_UPPER_WICK_BODY",
            "MIN_ATR14_VS_ATR50_MEAN",
            "H1_CLOSE_VS_EMA20",
            "H1_CLOSE_VS_EMA50",
            "EMA20_SLOPE",
            "MAX_BARS_SINCE_PRIOR_HIGH",
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
                "EUR/GBP feature-discovery CSV "
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
            "eurgbp-short-feature-discovery"
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
