import os
import itertools
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# USD/JPY SHORT - TIMING / WEEKDAY DIAGNOSTIC
#
# RESEARCH ONLY — NEVER SUBMITS ORDERS.
#
# Frozen high-frequency core:
#   Bearish engulfing
#   Body >= 1.45
#   Structure lookback = 90 H1 bars
#   Signal high within 0.50 ATR14 of prior 90-bar high
#   Previous completed daily close < EMA90
#   RR = 2.50
#   NO strong-close filter
#   NO upper-wick filter
#
# Purpose:
#   Diagnose whether a small number of New York signal hours
#   or weekdays are independently destructive enough to remove.
#
# IMPORTANT:
#   - Baseline starts with ALL hours and ALL weekdays.
#   - Exclusions are re-simulated from the signal stream rather
#     than simply deleting trades from the baseline result.
#   - "Independently bad" bucket threshold:
#         >= 6 baseline trades
#         total R < 0
#         PF < 0.85
#   - We test:
#         baseline
#         each single hour exclusion
#         each single weekday exclusion
#         all pairs of independently bad hours
#         all independently bad hours together
#         each independently bad weekday + bad-hour set
#
# Exact execution conventions retained:
#   OANDA midpoint H1
#   Daily alignment = 17:00 America/New_York
#   Previous completed daily candle only
#   ATR14 = Wilder/RMA
#   Daily EMA = SMA-seeded EMA
#   Stop = signal high + 10 ticks
#   Adverse short slippage = 5 ticks
#   Target from reference signal close
#   Pyramiding = 0
#   Same-bar SL/TP tie rule retained
# ============================================================


app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_URL = "https://api-fxtrade.oanda.com"

INSTRUMENT = "USD_JPY"
TICK_SIZE = 0.001

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
DAILY_WARMUP_DAYS = 2600

OUTPUT_FILE = "usdjpy_short_timing_diagnostic.csv"
BUCKET_FILE = "usdjpy_short_timing_buckets.csv"


# ============================================================
# FROZEN CORE
# ============================================================

BODY_RATIO = 1.45
STRUCTURE_LOOKBACK = 90
MAX_DISTANCE_ATR = 0.50
SLOW_DAILY_EMA = 90
REWARD_RISK = 2.50


# ============================================================
# BAD-BUCKET RULE
# ============================================================

MIN_BUCKET_TRADES = 6
MAX_BAD_BUCKET_PF = 0.85


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
    "service": "USDJPY Short Timing Diagnostic",
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "completed_scenarios": 0,
    "output_file": None,
    "bucket_file": None,
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

    result[length - 1] = initial

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

    initial = (
        sum(values[:length])
        / length
    )

    result[length - 1] = initial
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


def atr_series(candles, length=14):
    return rma_series(
        true_ranges(candles),
        length,
    )


# ============================================================
# DAILY ALIGNMENT
# ============================================================

def current_daily_start(timestamp_utc):
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
        candidate -= timedelta(days=1)

    return candidate.astimezone(
        timezone.utc
    )


def build_h1_daily_lookup(
    h1,
    daily,
):
    closes = [
        candle["close"]
        for candle in daily
    ]

    ema90 = ema_series(
        closes,
        SLOW_DAILY_EMA,
    )

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
            "ema90": ema90[
                daily_index
            ],
        }

    return lookup


# ============================================================
# SIGNALS
# ============================================================

def build_candidates(
    h1,
    h1_atr,
    daily_lookup,
):
    candidates = []

    for index in range(
        STRUCTURE_LOOKBACK,
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
            or daily["ema90"] is None
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

        if (
            current_body
            / previous_body
            < BODY_RATIO
        ):
            continue

        previous_highest = max(
            candle["high"]
            for candle in h1[
                index - STRUCTURE_LOOKBACK:index
            ]
        )

        distance_atr = (
            previous_highest
            - signal["high"]
        ) / atr

        if (
            distance_atr
            > MAX_DISTANCE_ATR
        ):
            continue

        if not (
            daily["close"]
            < daily["ema90"]
        ):
            continue

        ny_time = signal[
            "time"
        ].astimezone(
            NY_TZ
        )

        candidates.append({
            "index": index,
            "time": signal["time"],
            "ny_hour": ny_time.hour,
            "ny_weekday": ny_time.weekday(),
            "ny_weekday_name": ny_time.strftime("%A"),
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
    candidates,
    excluded_hours=None,
    excluded_weekdays=None,
):
    excluded_hours = set(
        excluded_hours or []
    )

    excluded_weekdays = set(
        excluded_weekdays or []
    )

    filtered = [
        candidate
        for candidate in candidates
        if candidate["ny_hour"]
        not in excluded_hours
        and candidate["ny_weekday"]
        not in excluded_weekdays
    ]

    trades = []
    position_exit_index = -1
    ignored = 0
    still_open = False

    for candidate in filtered:
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
        ).copy()

        trade[
            "ny_hour"
        ] = candidate[
            "ny_hour"
        ]

        trade[
            "ny_weekday"
        ] = candidate[
            "ny_weekday"
        ]

        trade[
            "ny_weekday_name"
        ] = candidate[
            "ny_weekday_name"
        ]

        if (
            trade["status"]
            == "OPEN"
        ):
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
# BUCKET DIAGNOSTICS
# ============================================================

def bucket_stats(
    baseline_trades,
):
    rows = []

    for hour in range(24):
        trades = [
            trade
            for trade in baseline_trades
            if trade[
                "ny_hour"
            ] == hour
        ]

        s = stats_for_trades(
            trades
        )

        independently_bad = (
            s["trades"]
            >= MIN_BUCKET_TRADES
            and s["total_r"] < 0
            and s["profit_factor"]
            < MAX_BAD_BUCKET_PF
        )

        rows.append({
            "bucket_type": "NY_HOUR",
            "bucket_value": hour,
            "bucket_name": (
                f"{hour:02d}:00"
            ),
            **s,
            "independently_bad": (
                independently_bad
            ),
        })

    weekday_names = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
    ]

    for weekday in range(5):
        trades = [
            trade
            for trade in baseline_trades
            if trade[
                "ny_weekday"
            ] == weekday
        ]

        s = stats_for_trades(
            trades
        )

        independently_bad = (
            s["trades"]
            >= MIN_BUCKET_TRADES
            and s["total_r"] < 0
            and s["profit_factor"]
            < MAX_BAD_BUCKET_PF
        )

        rows.append({
            "bucket_type": "WEEKDAY",
            "bucket_value": weekday,
            "bucket_name": (
                weekday_names[
                    weekday
                ]
            ),
            **s,
            "independently_bad": (
                independently_bad
            ),
        })

    return pd.DataFrame(
        rows
    )


# ============================================================
# SCENARIO OUTPUT
# ============================================================

def scenario_row(
    name,
    scenario_type,
    excluded_hours,
    excluded_weekdays,
    trades,
    ignored,
    still_open,
    years,
):
    full = stats_for_trades(
        trades
    )

    row = {
        "scenario": name,
        "scenario_type": scenario_type,
        "excluded_hours": ",".join(
            str(value)
            for value in sorted(
                excluded_hours
            )
        ),
        "excluded_weekdays": ",".join(
            str(value)
            for value in sorted(
                excluded_weekdays
            )
        ),
        "trades": full[
            "trades"
        ],
        "trades_per_year": round(
            full["trades"]
            / years,
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
        "ignored_due_to_open_trade": (
            ignored
        ),
        "still_open_at_end": (
            still_open
        ),
    }

    profitable_eras = 0
    min_era_pf = None

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

        if (
            era["trades"] >= 5
            and era["total_r"] > 0
        ):
            profitable_eras += 1

        if era["trades"] >= 5:
            if min_era_pf is None:
                min_era_pf = era[
                    "profit_factor"
                ]
            else:
                min_era_pf = min(
                    min_era_pf,
                    era[
                        "profit_factor"
                    ],
                )

    row[
        "profitable_eras_with_5_plus_trades"
    ] = profitable_eras

    row[
        "minimum_era_pf_5_plus"
    ] = min_era_pf

    row[
        "all_four_eras_profitable"
    ] = (
        profitable_eras >= 4
    )

    return row


# ============================================================
# RESEARCH
# ============================================================

def run_research():
    global STATUS

    try:
        print()
        print("=" * 78)
        print(
            "USD/JPY SHORT - TIMING / WEEKDAY DIAGNOSTIC"
        )
        print("=" * 78)
        print(
            "Frozen core:",
            {
                "body": BODY_RATIO,
                "structure": STRUCTURE_LOOKBACK,
                "distance_atr": MAX_DISTANCE_ATR,
                "daily_ema": SLOW_DAILY_EMA,
                "rr": REWARD_RISK,
            },
        )
        print()

        STATUS.update({
            "state": "fetching_data",
            "message": (
                "Fetching USD/JPY OANDA history"
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
                "No USD/JPY H1 candles returned"
            )

        if not daily:
            raise RuntimeError(
                "No USD/JPY daily candles returned"
            )

        STATUS.update({
            "state": "precomputing",
            "message": (
                "Building frozen-core signal stream"
            ),
        })

        h1_atr = atr_series(
            h1,
            14,
        )

        daily_lookup = (
            build_h1_daily_lookup(
                h1,
                daily,
            )
        )

        candidates = build_candidates(
            h1,
            h1_atr,
            daily_lookup,
        )

        STATUS[
            "raw_frozen_core_signals"
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

        STATUS.update({
            "state": "running",
            "message": (
                "Running timing and weekday diagnostics"
            ),
        })

        scenario_rows = []

        # ---------------- BASELINE ----------------

        (
            baseline_trades,
            baseline_ignored,
            baseline_open,
        ) = simulate(
            h1,
            candidates,
        )

        scenario_rows.append(
            scenario_row(
                "BASELINE_ALL_HOURS_ALL_WEEKDAYS",
                "BASELINE",
                set(),
                set(),
                baseline_trades,
                baseline_ignored,
                baseline_open,
                years,
            )
        )

        # ---------------- BUCKET STATS ----------------

        buckets = bucket_stats(
            baseline_trades
        )

        buckets.to_csv(
            BUCKET_FILE,
            index=False,
        )

        bad_hours = sorted(
            buckets[
                (
                    buckets[
                        "bucket_type"
                    ] == "NY_HOUR"
                )
                & (
                    buckets[
                        "independently_bad"
                    ] == True
                )
            ][
                "bucket_value"
            ].astype(
                int
            ).tolist()
        )

        bad_weekdays = sorted(
            buckets[
                (
                    buckets[
                        "bucket_type"
                    ] == "WEEKDAY"
                )
                & (
                    buckets[
                        "independently_bad"
                    ] == True
                )
            ][
                "bucket_value"
            ].astype(
                int
            ).tolist()
        )

        STATUS[
            "independently_bad_hours"
        ] = bad_hours

        STATUS[
            "independently_bad_weekdays"
        ] = bad_weekdays

        # ---------------- SINGLE HOUR EXCLUSIONS ----------------

        for hour in range(24):
            (
                trades,
                ignored,
                still_open,
            ) = simulate(
                h1,
                candidates,
                excluded_hours={
                    hour
                },
            )

            scenario_rows.append(
                scenario_row(
                    f"EXCLUDE_HOUR_{hour:02d}",
                    "SINGLE_HOUR",
                    {hour},
                    set(),
                    trades,
                    ignored,
                    still_open,
                    years,
                )
            )

        # ---------------- SINGLE WEEKDAY EXCLUSIONS ----------------

        weekday_names = [
            "MONDAY",
            "TUESDAY",
            "WEDNESDAY",
            "THURSDAY",
            "FRIDAY",
        ]

        for weekday in range(5):
            (
                trades,
                ignored,
                still_open,
            ) = simulate(
                h1,
                candidates,
                excluded_weekdays={
                    weekday
                },
            )

            scenario_rows.append(
                scenario_row(
                    f"EXCLUDE_{weekday_names[weekday]}",
                    "SINGLE_WEEKDAY",
                    set(),
                    {weekday},
                    trades,
                    ignored,
                    still_open,
                    years,
                )
            )

        # ---------------- PAIRS OF INDEPENDENTLY BAD HOURS ----------------

        for hour_a, hour_b in itertools.combinations(
            bad_hours,
            2,
        ):
            excluded = {
                hour_a,
                hour_b,
            }

            (
                trades,
                ignored,
                still_open,
            ) = simulate(
                h1,
                candidates,
                excluded_hours=excluded,
            )

            scenario_rows.append(
                scenario_row(
                    (
                        f"EXCLUDE_BAD_HOURS_"
                        f"{hour_a:02d}_{hour_b:02d}"
                    ),
                    "BAD_HOUR_PAIR",
                    excluded,
                    set(),
                    trades,
                    ignored,
                    still_open,
                    years,
                )
            )

        # ---------------- ALL INDEPENDENTLY BAD HOURS ----------------

        if bad_hours:
            bad_hour_set = set(
                bad_hours
            )

            (
                trades,
                ignored,
                still_open,
            ) = simulate(
                h1,
                candidates,
                excluded_hours=bad_hour_set,
            )

            scenario_rows.append(
                scenario_row(
                    "EXCLUDE_ALL_INDEPENDENTLY_BAD_HOURS",
                    "ALL_BAD_HOURS",
                    bad_hour_set,
                    set(),
                    trades,
                    ignored,
                    still_open,
                    years,
                )
            )

            # Combine bad-hour set with each independently
            # bad weekday, but only those weekdays that passed
            # the same mechanical independent-bucket rule.
            for weekday in bad_weekdays:
                (
                    trades,
                    ignored,
                    still_open,
                ) = simulate(
                    h1,
                    candidates,
                    excluded_hours=bad_hour_set,
                    excluded_weekdays={
                        weekday
                    },
                )

                scenario_rows.append(
                    scenario_row(
                        (
                            "BAD_HOURS_PLUS_"
                            f"{weekday_names[weekday]}"
                        ),
                        "BAD_HOURS_PLUS_BAD_WEEKDAY",
                        bad_hour_set,
                        {weekday},
                        trades,
                        ignored,
                        still_open,
                        years,
                    )
                )

        df = pd.DataFrame(
            scenario_rows
        )

        baseline_pf = float(
            df.loc[
                df[
                    "scenario_type"
                ] == "BASELINE",
                "profit_factor",
            ].iloc[0]
        )

        baseline_tpy = float(
            df.loc[
                df[
                    "scenario_type"
                ] == "BASELINE",
                "trades_per_year",
            ].iloc[0]
        )

        df[
            "pf_change_vs_baseline"
        ] = (
            df[
                "profit_factor"
            ]
            - baseline_pf
        ).round(
            3
        )

        df[
            "trades_per_year_change_vs_baseline"
        ] = (
            df[
                "trades_per_year"
            ]
            - baseline_tpy
        ).round(
            2
        )

        df[
            "preferred_zone"
        ] = (
            df[
                "all_four_eras_profitable"
            ]
            & (
                df[
                    "profit_factor"
                ] >= 1.35
            )
            & (
                df[
                    "trades_per_year"
                ] >= 6.0
            )
            & (
                df[
                    "minimum_era_pf_5_plus"
                ].fillna(0)
                >= 1.10
            )
        )

        df = df.sort_values(
            by=[
                "preferred_zone",
                "all_four_eras_profitable",
                "minimum_era_pf_5_plus",
                "profit_factor",
                "trades_per_year",
                "total_r",
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
                "USD/JPY timing diagnostic completed"
            ),
            "completed_scenarios": len(
                df
            ),
            "independently_bad_hours": (
                bad_hours
            ),
            "independently_bad_weekdays": (
                bad_weekdays
            ),
            "preferred_zone_rows": int(
                df[
                    "preferred_zone"
                ].sum()
            ),
            "output_file": (
                OUTPUT_FILE
            ),
            "bucket_file": (
                BUCKET_FILE
            ),
        })

        print()
        print("=" * 78)
        print(
            "USD/JPY TIMING DIAGNOSTIC COMPLETE"
        )
        print("=" * 78)
        print(
            "Independent bad hours:",
            bad_hours,
        )
        print(
            "Independent bad weekdays:",
            bad_weekdays,
        )
        print(
            "Scenario rows:",
            len(df),
        )
        print(
            "Saved:",
            OUTPUT_FILE,
        )
        print(
            "Buckets:",
            BUCKET_FILE,
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
            "USDJPY Short Timing Diagnostic"
        ),
        "status": STATUS,
        "instrument": INSTRUMENT,
        "direction": "SHORT",
        "frozen_core": {
            "minimum_body_ratio": (
                BODY_RATIO
            ),
            "structure_lookback": (
                STRUCTURE_LOOKBACK
            ),
            "maximum_distance_atr": (
                MAX_DISTANCE_ATR
            ),
            "previous_daily_close_below_ema": (
                SLOW_DAILY_EMA
            ),
            "reward_risk": (
                REWARD_RISK
            ),
            "strong_close_filter": False,
            "upper_wick_filter": False,
        },
        "bad_bucket_rule": {
            "minimum_trades": (
                MIN_BUCKET_TRADES
            ),
            "total_r_must_be_negative": True,
            "profit_factor_below": (
                MAX_BAD_BUCKET_PF
            ),
        },
        "downloads": {
            "scenarios": "/download",
            "buckets": "/download-buckets",
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


@app.route("/download")
def download():
    if not os.path.exists(
        OUTPUT_FILE
    ):
        return jsonify({
            "status": "not_ready",
        }), 404

    return send_file(
        OUTPUT_FILE,
        as_attachment=True,
        download_name=OUTPUT_FILE,
    )


@app.route("/download-buckets")
def download_buckets():
    if not os.path.exists(
        BUCKET_FILE
    ):
        return jsonify({
            "status": "not_ready",
        }), 404

    return send_file(
        BUCKET_FILE,
        as_attachment=True,
        download_name=BUCKET_FILE,
    )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    research_thread = threading.Thread(
        target=run_research,
        name=(
            "usdjpy-short-timing-diagnostic"
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
