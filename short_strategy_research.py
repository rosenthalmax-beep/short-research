import os
import itertools
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# GBP/USD SHORT - FINAL NARROW STABILITY SWEEP
#
# RESEARCH ONLY — NEVER SUBMITS ORDERS.
#
# PURPOSE
# -------
# Validate whether the newly discovered 5–6 trades/year,
# PF ~1.9–2.0 region is a BROAD PLATEAU rather than one
# isolated parameter point.
#
# Frozen logic going into this test:
#   - bearish engulfing
#   - body ratio >= 1.00
#   - strong-close OFF
#   - previous completed daily close < EMA100
#   - daily EMA40 < EMA100
#   - EMA100 slope filter enabled
#   - daily ATR regime enabled
#
# Narrow sweep:
#   Structure:      60 / 65 / 70 / 75 / 80
#   Distance:       .125 / .15 / .175 / .20 / .225 ATR
#   EMA100 slope:  -.03 / -.04 / -.05 / -.06 / -.07
#   Daily ATR:      .70 / .75 / .80 / .85 / .90
#   RR:             2.25 / 2.50 / 2.75
#
# Total = 1,875 combinations.
#
# NO hour or weekday optimisation.
#
# Backtest conventions are unchanged:
#   OANDA midpoint H1
#   Daily alignment = 17:00 America/New_York
#   Previous completed daily candle only
#   ATR14 = Wilder/RMA
#   Stop = signal high + 10 ticks
#   Adverse short slippage = 5 ticks
#   Target based on reference signal close
#   Same-bar SL/TP rule retained
#   Pyramiding = 0
#   signal_index < position_exit_index
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

H1_WARMUP_DAYS = 160
DAILY_WARMUP_DAYS = 2200

BODY_RATIO = 1.00
SLOW_EMA = 100
FAST_EMA = 40

OUTPUT_ALL = "gbpusd_short_final_stability_sweep.csv"
OUTPUT_PLATEAU = "gbpusd_short_final_stability_plateau.csv"


# ============================================================
# GRID
# ============================================================

STRUCTURE_LOOKBACKS = [
    60,
    65,
    70,
    75,
    80,
]

MAX_DISTANCE_ATR_VALUES = [
    0.125,
    0.150,
    0.175,
    0.200,
    0.225,
]

EMA100_SLOPE_MAX_VALUES = [
    -0.03,
    -0.04,
    -0.05,
    -0.06,
    -0.07,
]

DAILY_ATR_RATIO_MIN_VALUES = [
    0.70,
    0.75,
    0.80,
    0.85,
    0.90,
]

REWARD_RISKS = [
    2.25,
    2.50,
    2.75,
]

TOTAL_COMBINATIONS = (
    len(STRUCTURE_LOOKBACKS)
    * len(MAX_DISTANCE_ATR_VALUES)
    * len(EMA100_SLOPE_MAX_VALUES)
    * len(DAILY_ATR_RATIO_MIN_VALUES)
    * len(REWARD_RISKS)
)


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
    "service": "GBPUSD Short Final Narrow Stability Sweep",
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "total_combinations": TOTAL_COMBINATIONS,
    "completed_combinations": 0,
    "rows_saved": 0,
    "plateau_rows": 0,
    "output_all": None,
    "output_plateau": None,
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

    # ATR14 needs its own warmup first.
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
# SIGNAL FEATURES
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

        structure_distances = {}

        for lookback in (
            STRUCTURE_LOOKBACKS
        ):
            previous_highest = max(
                candle["high"]
                for candle in h1[
                    index - lookback:index
                ]
            )

            structure_distances[
                lookback
            ] = (
                previous_highest
                - signal["high"]
            ) / atr

        candidates.append({
            "index": index,
            "time": signal["time"],
            "body_ratio": (
                current_body
                / previous_body
            ),
            "structure_distances": (
                structure_distances
            ),
            "daily": daily,
        })

    return candidates


# ============================================================
# FILTERS
# ============================================================

def candidate_allowed(
    candidate,
    structure_lookback,
    max_distance_atr,
    ema100_slope_max,
    daily_atr_ratio_min,
):
    if (
        candidate["body_ratio"]
        < BODY_RATIO
    ):
        return False

    distance = (
        candidate[
            "structure_distances"
        ][structure_lookback]
    )

    if (
        distance
        > max_distance_atr
    ):
        return False

    daily = candidate[
        "daily"
    ]

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
        return False

    # Previous completed daily close below EMA100.
    if not (
        daily["close"]
        < ema100
    ):
        return False

    # Daily bearish alignment.
    if not (
        ema40 < ema100
    ):
        return False

    slope = daily.get(
        "ema100_slope_5_atr"
    )

    if slope is None:
        return False

    if (
        slope
        > ema100_slope_max
    ):
        return False

    atr_ratio = daily.get(
        "daily_atr_ratio_50"
    )

    if atr_ratio is None:
        return False

    if (
        atr_ratio
        < daily_atr_ratio_min
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

    # Adverse short fill = lower fill.
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

        result_r = (
            backtest_entry
            - exit_price
        ) / actual_risk

        result = {
            "status": "CLOSED",
            "signal_index": (
                signal_index
            ),
            "signal_time": (
                signal["time"]
            ),
            "exit_index": index,
            "exit_time": (
                candle["time"]
            ),
            "exit_reason": (
                exit_reason
            ),
            "result_r": (
                result_r
            ),
        }

        EXIT_CACHE[
            cache_key
        ] = result

        return result

    result = {
        "status": "OPEN",
        "signal_index": (
            signal_index
        ),
        "signal_time": (
            signal["time"]
        ),
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
        signal_index = (
            candidate["index"]
        )

        # Same convention as prior GBP/USD/EUR/USD work.
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
                reward_risk,
            )
        )

        if (
            trade["status"]
            == "OPEN"
        ):
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
        value
        for value in results
        if value > 0
    ]

    losers = [
        value
        for value in results
        if value < 0
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
    structure,
    distance,
    slope,
    daily_atr_ratio,
    rr,
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
        "body_ratio": BODY_RATIO,
        "strong_close": "OFF",
        "fast_ema": FAST_EMA,
        "slow_ema": SLOW_EMA,

        "structure_lookback": (
            structure
        ),
        "max_distance_atr": (
            distance
        ),
        "ema100_slope_max_5d_atr": (
            slope
        ),
        "daily_atr_ratio_min_50d": (
            daily_atr_ratio
        ),
        "reward_risk": rr,

        "raw_signals": len(
            eligible
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

        row[
            f"{era_name}_win_rate"
        ] = era[
            "win_rate"
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
        print("=" * 72)
        print(
            "GBP/USD SHORT - FINAL NARROW STABILITY SWEEP"
        )
        print("=" * 72)
        print(
            "Total combinations:",
            TOTAL_COMBINATIONS,
        )
        print(
            "ALL HOURS / ALL WEEKDAYS"
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
                "Building indicators "
                "and bearish-engulfing features"
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

        STATUS[
            "base_bearish_engulfings"
        ] = len(candidates)

        print(
            "Base bearish engulfings:",
            len(candidates),
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

        STATUS.update({
            "state": "running",
            "message": (
                "Running final narrow stability sweep"
            ),
        })

        rows = []

        combinations = itertools.product(
            STRUCTURE_LOOKBACKS,
            MAX_DISTANCE_ATR_VALUES,
            EMA100_SLOPE_MAX_VALUES,
            DAILY_ATR_RATIO_MIN_VALUES,
            REWARD_RISKS,
        )

        for number, combination in enumerate(
            combinations,
            start=1,
        ):
            (
                structure,
                distance,
                slope,
                daily_atr_ratio,
                rr,
            ) = combination

            eligible = [
                candidate
                for candidate in candidates
                if candidate_allowed(
                    candidate,
                    structure,
                    distance,
                    slope,
                    daily_atr_ratio,
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

            rows.append(
                make_result_row(
                    structure,
                    distance,
                    slope,
                    daily_atr_ratio,
                    rr,
                    eligible,
                    trades,
                    ignored,
                    still_open,
                    years,
                )
            )

            STATUS[
                "completed_combinations"
            ] = number

            if (
                number % 100
                == 0
            ):
                print(
                    f"Progress: "
                    f"{number}/"
                    f"{TOTAL_COMBINATIONS}",
                    flush=True,
                )

        df = pd.DataFrame(
            rows
        )

        if df.empty:
            raise RuntimeError(
                "No stability-sweep rows generated"
            )

        df[
            "all_four_eras_profitable"
        ] = (
            df[
                "profitable_eras_with_5_plus_trades"
            ]
            >= 4
        )

        df[
            "target_frequency_5_to_7"
        ] = (
            (
                df[
                    "trades_per_year"
                ]
                >= 5.0
            )
            &
            (
                df[
                    "trades_per_year"
                ]
                <= 7.0
            )
        )

        df[
            "pf_at_least_1_75"
        ] = (
            df[
                "profit_factor"
            ]
            >= 1.75
        )

        df[
            "worst_era_pf_at_least_1_50"
        ] = (
            df[
                "minimum_era_pf_5_plus"
            ]
            >= 1.50
        )

        # Plateau definition:
        # enough frequency for portfolio use,
        # good full-history PF,
        # all eras profitable,
        # and no weak era.
        df[
            "plateau_candidate"
        ] = (
            df[
                "target_frequency_5_to_7"
            ]
            &
            df[
                "pf_at_least_1_75"
            ]
            &
            df[
                "all_four_eras_profitable"
            ]
            &
            df[
                "worst_era_pf_at_least_1_50"
            ]
        )

        # Useful efficiency measurements.
        df[
            "annual_r_linear"
        ] = (
            df[
                "expectancy_r"
            ]
            * df[
                "trades_per_year"
            ]
        )

        df[
            "pf_x_frequency"
        ] = (
            df[
                "profit_factor"
            ]
            * df[
                "trades_per_year"
            ]
        )

        # Main file:
        # robustness first, then quality/frequency.
        df = df.sort_values(
            by=[
                "plateau_candidate",
                "all_four_eras_profitable",
                "minimum_era_pf_5_plus",
                "profit_factor",
                "annual_r_linear",
                "trades_per_year",
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
            OUTPUT_ALL,
            index=False,
        )

        plateau = (
            df[
                df[
                    "plateau_candidate"
                ]
            ]
            .copy()
        )

        plateau = plateau.sort_values(
            by=[
                "minimum_era_pf_5_plus",
                "profit_factor",
                "annual_r_linear",
                "trades_per_year",
            ],
            ascending=[
                False,
                False,
                False,
                False,
            ],
        )

        plateau.to_csv(
            OUTPUT_PLATEAU,
            index=False,
        )

        STATUS.update({
            "state": "complete",
            "message": (
                "GBP/USD final narrow "
                "stability sweep completed successfully"
            ),
            "completed_combinations": (
                TOTAL_COMBINATIONS
            ),
            "rows_saved": len(df),
            "plateau_rows": len(
                plateau
            ),
            "output_all": (
                OUTPUT_ALL
            ),
            "output_plateau": (
                OUTPUT_PLATEAU
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
        print("=" * 72)
        print(
            "FINAL STABILITY SWEEP COMPLETE"
        )
        print("=" * 72)
        print(
            "All rows:",
            len(df),
        )
        print(
            "Plateau rows:",
            len(plateau),
        )
        print(
            "Saved:",
            OUTPUT_ALL,
        )
        print(
            "Saved:",
            OUTPUT_PLATEAU,
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
            "GBPUSD Short Final "
            "Narrow Stability Sweep"
        ),
        "status": STATUS,
        "instrument": INSTRUMENT,
        "direction": "SHORT",
        "body_ratio": BODY_RATIO,
        "strong_close": "OFF",
        "daily_regime": (
            "previous completed daily close < EMA100"
        ),
        "daily_alignment": (
            "EMA40 < EMA100"
        ),
        "timing_filters": (
            "NONE - all hours and all weekdays"
        ),
        "grid": {
            "structure_lookbacks": (
                STRUCTURE_LOOKBACKS
            ),
            "max_distance_atr": (
                MAX_DISTANCE_ATR_VALUES
            ),
            "ema100_slope_max_5d_atr": (
                EMA100_SLOPE_MAX_VALUES
            ),
            "daily_atr_ratio_min_50d": (
                DAILY_ATR_RATIO_MIN_VALUES
            ),
            "reward_risks": (
                REWARD_RISKS
            ),
            "total_combinations": (
                TOTAL_COMBINATIONS
            ),
        },
        "downloads": {
            "all_results": (
                "/download/all"
            ),
            "plateau": (
                "/download/plateau"
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


@app.route("/download/all")
def download_all():
    if not os.path.exists(
        OUTPUT_ALL
    ):
        return jsonify({
            "status": "not_ready",
            "message": (
                "All-results CSV "
                "is not ready yet"
            ),
        }), 404

    return send_file(
        OUTPUT_ALL,
        as_attachment=True,
        download_name=OUTPUT_ALL,
    )


@app.route("/download/plateau")
def download_plateau():
    if not os.path.exists(
        OUTPUT_PLATEAU
    ):
        return jsonify({
            "status": "not_ready",
            "message": (
                "Plateau CSV "
                "is not ready yet"
            ),
        }), 404

    return send_file(
        OUTPUT_PLATEAU,
        as_attachment=True,
        download_name=OUTPUT_PLATEAU,
    )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    research_thread = (
        threading.Thread(
            target=run_research,
            name=(
                "gbpusd-short-final-"
                "stability-sweep"
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
