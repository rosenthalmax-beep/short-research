import os
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# USD/CAD SHORT - LIGHT TIMING REFINEMENT
#
# RESEARCH ONLY — NEVER SUBMITS ORDERS.
#
# Frozen structural setup from the final geometry/regime sweep:
#
#   bearish engulfing
#   body ratio >= 1.40
#   structure lookback = 60 H1 bars
#   signal high within 0.25 ATR14 of prior 60-bar high
#   previous completed daily close < EMA300
#   24h upward momentum >= 0.50 ATR14
#   signal range >= 0.90 ATR14
#   stop size <= 1.60 ATR14
#   RR = 3.25
#   stop = signal high + 10 ticks
#   adverse short slippage = 5 ticks
#
# Baseline result from prior sweep:
#   ~100 trades
#   ~4.11 trades/year
#   PF ~1.466
#
# Purpose:
#   Test whether there is one genuinely bad hour / weekday /
#   adjacent two-hour window that can be excluded WITHOUT
#   destroying frequency.
#
# Timing tests:
#   1) BASELINE - exclude nothing
#   2) Exclude each single NY hour 0..23
#   3) Exclude each NY weekday Mon..Fri
#   4) Exclude each adjacent 2-hour NY block
#
# Total tests = 54
#
# We are intentionally NOT doing a huge timing optimisation.
#
# Exact backtest conventions:
#   OANDA midpoint H1
#   Daily alignment = 17:00 America/New_York
#   Previous completed daily candle only
#   ATR14 = Wilder/RMA
#   Daily EMA300 = SMA-seeded EMA
#   Entry reference = signal close
#   Backtest short fill = signal close - 5 ticks
#   Stop = signal high + 10 ticks
#   Target from reference signal close
#   Pyramiding = 0
#   Same-bar SL/TP tie rule retained
#   Exit before same-candle next-signal handling retained
# ============================================================


app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_URL = "https://api-fxtrade.oanda.com"

INSTRUMENT = "USD_CAD"
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

H1_WARMUP_DAYS = 220
DAILY_WARMUP_DAYS = 3000

OUTPUT_FILE = "usdcad_short_light_timing_refinement.csv"


# ============================================================
# FROZEN STRATEGY
# ============================================================

BODY_RATIO = 1.40
STRUCTURE_LOOKBACK = 60
MAX_DISTANCE_ATR = 0.25

SLOW_EMA = 300

MOMENTUM_LOOKBACK = 24
MIN_UP_MOMENTUM_ATR = 0.50

MIN_RANGE_ATR = 0.90
MAX_STOP_SIZE_ATR = 1.60

REWARD_RISK = 3.25


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
    "service": "USDCAD Short Light Timing Refinement",
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "total_tests": 54,
    "completed_tests": 0,
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
            tr = (
                candle["high"]
                - candle["low"]
            )

        else:
            previous_close = (
                candles[index - 1]["close"]
            )

            tr = max(
                candle["high"] - candle["low"],
                abs(
                    candle["high"]
                    - previous_close
                ),
                abs(
                    candle["low"]
                    - previous_close
                ),
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


def build_daily_ema(daily):
    closes = [
        candle["close"]
        for candle in daily
    ]

    return ema_series(
        closes,
        SLOW_EMA,
    )


def build_h1_daily_lookup(
    h1,
    daily,
    daily_ema,
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

        lookup[h1_index] = {
            "close": daily[
                daily_index
            ]["close"],
            "ema": daily_ema[
                daily_index
            ],
        }

    return lookup


# ============================================================
# SIGNAL MATRIX
# ============================================================

def build_candidates(
    h1,
    h1_atr,
    daily_lookup,
):
    candidates = []

    max_lookback = max(
        STRUCTURE_LOOKBACK,
        MOMENTUM_LOOKBACK,
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
            or daily["ema"] is None
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

        if body_ratio < BODY_RATIO:
            continue

        previous_highest = max(
            candle["high"]
            for candle in h1[
                index - STRUCTURE_LOOKBACK:index
            ]
        )

        structure_distance = (
            previous_highest
            - signal["high"]
        ) / atr

        if (
            structure_distance
            > MAX_DISTANCE_ATR
        ):
            continue

        if not (
            daily["close"]
            < daily["ema"]
        ):
            continue

        up_momentum_24 = (
            signal["close"]
            - h1[
                index - MOMENTUM_LOOKBACK
            ]["close"]
        ) / atr

        if (
            up_momentum_24
            < MIN_UP_MOMENTUM_ATR
        ):
            continue

        range_atr = (
            candle_range
            / atr
        )

        if (
            range_atr
            < MIN_RANGE_ATR
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

        if (
            stop_size_atr
            > MAX_STOP_SIZE_ATR
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
        })

    return candidates


# ============================================================
# TIMING TESTS
# ============================================================

WEEKDAY_NAMES = {
    0: "MON",
    1: "TUE",
    2: "WED",
    3: "THU",
    4: "FRI",
}


def timing_tests():
    tests = []

    tests.append({
        "test_type": "BASELINE",
        "test_label": "exclude_nothing",
        "excluded_hours": set(),
        "excluded_weekdays": set(),
    })

    for hour in range(24):
        tests.append({
            "test_type": "SINGLE_HOUR",
            "test_label": (
                f"exclude_ny_hour_{hour:02d}"
            ),
            "excluded_hours": {
                hour
            },
            "excluded_weekdays": set(),
        })

    for weekday in range(5):
        tests.append({
            "test_type": "SINGLE_WEEKDAY",
            "test_label": (
                f"exclude_{WEEKDAY_NAMES[weekday]}"
            ),
            "excluded_hours": set(),
            "excluded_weekdays": {
                weekday
            },
        })

    for start_hour in range(24):
        second_hour = (
            start_hour + 1
        ) % 24

        tests.append({
            "test_type": "ADJACENT_2H_BLOCK",
            "test_label": (
                f"exclude_ny_hours_"
                f"{start_hour:02d}_"
                f"{second_hour:02d}"
            ),
            "excluded_hours": {
                start_hour,
                second_hour,
            },
            "excluded_weekdays": set(),
        })

    return tests


def candidate_allowed_by_timing(
    candidate,
    test,
):
    if (
        candidate["ny_hour"]
        in test[
            "excluded_hours"
        ]
    ):
        return False

    if (
        candidate["ny_weekday"]
        in test[
            "excluded_weekdays"
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
    candidates,
):
    trades = []
    position_exit_index = -1
    ignored = 0
    still_open = False

    for candidate in candidates:
        signal_index = (
            candidate["index"]
        )

        # Allows a new signal on the same H1 bar that the
        # previous trade exits, matching the locked convention.
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
    test,
    eligible,
    trades,
    ignored,
    still_open,
    years,
):
    full = stats_for_trades(
        trades
    )

    excluded_hours = sorted(
        list(
            test[
                "excluded_hours"
            ]
        )
    )

    excluded_weekdays = sorted(
        list(
            test[
                "excluded_weekdays"
            ]
        )
    )

    row = {
        "test_type": (
            test[
                "test_type"
            ]
        ),
        "test_label": (
            test[
                "test_label"
            ]
        ),
        "excluded_ny_hours": (
            ",".join(
                f"{hour:02d}"
                for hour in excluded_hours
            )
            if excluded_hours
            else ""
        ),
        "excluded_ny_weekdays": (
            ",".join(
                WEEKDAY_NAMES[
                    weekday
                ]
                for weekday
                in excluded_weekdays
            )
            if excluded_weekdays
            else ""
        ),
        "raw_signals": len(
            eligible
        ),
        "ignored_due_to_open_trade": (
            ignored
        ),
        "still_open_at_end": (
            still_open
        ),
        "trades": full[
            "trades"
        ],
        "trades_per_year": round(
            full[
                "trades"
            ] / years,
            2,
        ),
        "winners": full[
            "winners"
        ],
        "losers": full[
            "losers"
        ],
        "win_rate": full[
            "win_rate"
        ],
        "profit_factor": full[
            "profit_factor"
        ],
        "total_r": full[
            "total_r"
        ],
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
        ] = era[
            "trades"
        ]

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

        if era[
            "trades"
        ] >= 5:
            if era[
                "total_r"
            ] > 0:
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

    return row


# ============================================================
# RESEARCH
# ============================================================

def run_research():
    global STATUS

    try:
        print()
        print("=" * 76)
        print(
            "USD/CAD SHORT - LIGHT TIMING REFINEMENT"
        )
        print("=" * 76)
        print(
            "Frozen structural strategy"
        )
        print(
            "54 timing tests only"
        )
        print()

        STATUS.update({
            "state": "fetching_data",
            "message": (
                "Fetching USD/CAD OANDA history"
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
                "No USD/CAD H1 candles returned"
            )

        if not daily:
            raise RuntimeError(
                "No USD/CAD daily candles returned"
            )

        STATUS.update({
            "state": "precomputing",
            "message": (
                "Building frozen USD/CAD signal set"
            ),
        })

        h1_atr = atr_series(
            h1,
            14,
        )

        daily_ema = build_daily_ema(
            daily
        )

        daily_lookup = (
            build_h1_daily_lookup(
                h1,
                daily,
                daily_ema,
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
            "frozen_strategy_signals"
        ] = len(
            candidates
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

        tests = timing_tests()

        if len(tests) != 54:
            raise RuntimeError(
                f"Expected 54 timing tests, "
                f"got {len(tests)}"
            )

        STATUS.update({
            "state": "running",
            "message": (
                "Running light timing refinement"
            ),
        })

        rows = []

        for number, test in enumerate(
            tests,
            start=1,
        ):
            eligible = [
                candidate
                for candidate in candidates
                if candidate_allowed_by_timing(
                    candidate,
                    test,
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
                    test,
                    eligible,
                    trades,
                    ignored,
                    still_open,
                    years,
                )
            )

            STATUS[
                "completed_tests"
            ] = number

        df = pd.DataFrame(
            rows
        )

        if df.empty:
            raise RuntimeError(
                "No timing rows generated"
            )

        df[
            "frequency_4py"
        ] = (
            df[
                "trades_per_year"
            ]
            >= 4.0
        )

        df[
            "adequate_90"
        ] = (
            df[
                "trades"
            ]
            >= 90
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
            "worst_era_pf_115"
        ] = (
            df[
                "minimum_era_pf_5_plus"
            ].fillna(0)
            >= 1.15
        )

        df[
            "worst_era_pf_120"
        ] = (
            df[
                "minimum_era_pf_5_plus"
            ].fillna(0)
            >= 1.20
        )

        df[
            "pf_150"
        ] = (
            df[
                "profit_factor"
            ]
            >= 1.50
        )

        df[
            "preferred_profile"
        ] = (
            df[
                "frequency_4py"
            ]
            & df[
                "adequate_90"
            ]
            & df[
                "all_four_eras_profitable"
            ]
            & df[
                "worst_era_pf_115"
            ]
        )

        df[
            "strong_profile"
        ] = (
            df[
                "frequency_4py"
            ]
            & df[
                "adequate_90"
            ]
            & df[
                "all_four_eras_profitable"
            ]
            & df[
                "worst_era_pf_120"
            ]
            & df[
                "pf_150"
            ]
        )

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

        df = df.sort_values(
            by=[
                "strong_profile",
                "preferred_profile",
                "all_four_eras_profitable",
                "frequency_4py",
                "adequate_90",
                "worst_era_pf_120",
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
                "USD/CAD light timing refinement "
                "completed successfully"
            ),
            "completed_tests": len(
                tests
            ),
            "rows_saved": len(
                df
            ),
            "preferred_profile_count": int(
                df[
                    "preferred_profile"
                ].sum()
            ),
            "strong_profile_count": int(
                df[
                    "strong_profile"
                ].sum()
            ),
            "output_file": (
                OUTPUT_FILE
            ),
        })

        print()
        print("=" * 76)
        print(
            "USD/CAD LIGHT TIMING REFINEMENT COMPLETE"
        )
        print("=" * 76)
        print(
            "Rows:",
            len(df),
        )
        print(
            "Preferred profiles:",
            int(
                df[
                    "preferred_profile"
                ].sum()
            ),
        )
        print(
            "Strong profiles:",
            int(
                df[
                    "strong_profile"
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
            "USDCAD Short Light Timing Refinement"
        ),
        "status": STATUS,
        "instrument": INSTRUMENT,
        "direction": "SHORT",
        "frozen_strategy": {
            "body_ratio_min": (
                BODY_RATIO
            ),
            "structure_lookback": (
                STRUCTURE_LOOKBACK
            ),
            "max_distance_atr": (
                MAX_DISTANCE_ATR
            ),
            "slow_daily_ema": (
                SLOW_EMA
            ),
            "momentum_lookback_h": (
                MOMENTUM_LOOKBACK
            ),
            "min_up_momentum_atr": (
                MIN_UP_MOMENTUM_ATR
            ),
            "min_range_atr": (
                MIN_RANGE_ATR
            ),
            "max_stop_size_atr": (
                MAX_STOP_SIZE_ATR
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
        "timing_tests": {
            "baseline": 1,
            "single_hour_exclusions": 24,
            "single_weekday_exclusions": 5,
            "adjacent_2h_exclusions": 24,
            "total": 54,
        },
        "download": "/download",
        "trading_enabled": False,
        "orders_supported": False,
        "executor_connected": False,
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
                "USD/CAD timing CSV "
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
            "usdcad-short-light-timing-refinement"
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
