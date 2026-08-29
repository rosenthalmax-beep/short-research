import os
import itertools
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# USD/JPY SHORT - FINAL TIMING STABILITY CHECK
#
# RESEARCH ONLY — NEVER SUBMITS ORDERS.
#
# Frozen structural core:
#   Bearish engulfing
#   Body >= 1.45
#   Structure lookback = 90 H1 bars
#   Signal high within 0.50 ATR14 of prior 90-bar high
#   Previous completed daily close < EMA90
#   RR = 2.50
#   NO strong-close filter
#   NO upper-wick filter
#   ALL weekdays
#
# Candidate excluded NY hours from prior diagnostic:
#   01, 02, 05, 06, 10, 11
#
# This script tests:
#   1. Baseline: no hour exclusions
#   2. Full six-hour exclusion
#   3. Leave-one-back-in variants
#   4. Individual bad-hour exclusions
#   5. Natural pairs: 01+02, 05+06, 10+11
#   6. All combinations of those natural pairs
#   7. All combinations of the six candidate hours
#      containing at least 2 exclusions
#
# This lets us find the simplest timing rule that retains most
# of the robustness of the full six-hour exclusion.
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

OUTPUT_FILE = "usdjpy_short_final_timing_stability.csv"
HOUR_ERAS_FILE = "usdjpy_short_bad_hours_by_era.csv"


# ============================================================
# FROZEN CORE
# ============================================================

BODY_RATIO = 1.45
STRUCTURE_LOOKBACK = 90
MAX_DISTANCE_ATR = 0.50
SLOW_DAILY_EMA = 90
REWARD_RISK = 2.50


# ============================================================
# TIMING CANDIDATES
# ============================================================

CANDIDATE_BAD_HOURS = [
    1,
    2,
    5,
    6,
    10,
    11,
]

NATURAL_PAIRS = {
    "PAIR_01_02": {1, 2},
    "PAIR_05_06": {5, 6},
    "PAIR_10_11": {10, 11},
}


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
    "service": "USDJPY Short Final Timing Stability",
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "candidate_bad_hours": CANDIDATE_BAD_HOURS,
    "completed_scenarios": 0,
    "output_file": None,
    "hour_eras_file": None,
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
):
    excluded_hours = set(
        excluded_hours or []
    )

    filtered = [
        candidate
        for candidate in candidates
        if candidate[
            "ny_hour"
        ] not in excluded_hours
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
            total_r / len(results),
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
# ERA / HOUR DIAGNOSTIC
# ============================================================

def build_hour_era_table(
    baseline_trades,
):
    rows = []

    for hour in CANDIDATE_BAD_HOURS:
        hour_trades = [
            trade
            for trade in baseline_trades
            if trade[
                "ny_hour"
            ] == hour
        ]

        full = stats_for_trades(
            hour_trades
        )

        row = {
            "ny_hour": hour,
            "full_trades": full[
                "trades"
            ],
            "full_pf": full[
                "profit_factor"
            ],
            "full_r": full[
                "total_r"
            ],
            "full_expectancy": full[
                "expectancy_r"
            ],
        }

        positive_eras = 0
        negative_eras = 0

        for (
            era_name,
            era_start,
            era_end,
        ) in ERAS:
            era = stats_for_trades(
                hour_trades,
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

            if era["trades"] > 0:
                if era["total_r"] > 0:
                    positive_eras += 1
                elif era["total_r"] < 0:
                    negative_eras += 1

        row[
            "positive_eras"
        ] = positive_eras

        row[
            "negative_eras"
        ] = negative_eras

        rows.append(
            row
        )

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
        "scenario_type": (
            scenario_type
        ),
        "excluded_hours": ",".join(
            str(value)
            for value in sorted(
                excluded_hours
            )
        ),
        "excluded_hour_count": len(
            excluded_hours
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
# SCENARIO GENERATION
# ============================================================

def generate_scenarios():
    scenarios = {}

    def add(
        name,
        scenario_type,
        excluded,
    ):
        key = tuple(
            sorted(
                excluded
            )
        )

        if key not in scenarios:
            scenarios[
                key
            ] = {
                "name": name,
                "type": scenario_type,
                "excluded": set(
                    excluded
                ),
            }

    # Baseline.
    add(
        "BASELINE_ALL_HOURS",
        "BASELINE",
        set(),
    )

    # Full six-hour rule.
    all_bad = set(
        CANDIDATE_BAD_HOURS
    )

    add(
        "EXCLUDE_ALL_01_02_05_06_10_11",
        "FULL_SIX",
        all_bad,
    )

    # Individual exclusions.
    for hour in CANDIDATE_BAD_HOURS:
        add(
            f"EXCLUDE_{hour:02d}",
            "SINGLE_HOUR",
            {hour},
        )

    # Leave one hour back in from full six.
    for hour in CANDIDATE_BAD_HOURS:
        excluded = (
            all_bad
            - {hour}
        )

        add(
            f"FULL_SIX_LEAVE_{hour:02d}_BACK_IN",
            "LEAVE_ONE_BACK_IN",
            excluded,
        )

    # Natural pairs.
    for pair_name, pair_hours in (
        NATURAL_PAIRS.items()
    ):
        add(
            f"EXCLUDE_{pair_name}",
            "NATURAL_PAIR",
            pair_hours,
        )

    # Combinations of natural pairs.
    pair_items = list(
        NATURAL_PAIRS.items()
    )

    for pair_count in range(
        2,
        len(pair_items) + 1,
    ):
        for combo in itertools.combinations(
            pair_items,
            pair_count,
        ):
            names = [
                item[0]
                for item in combo
            ]

            excluded = set()

            for _, hours in combo:
                excluded |= hours

            add(
                "EXCLUDE_"
                + "__".join(
                    names
                ),
                "PAIR_COMBINATION",
                excluded,
            )

    # Exhaustive combinations of candidate hours,
    # at least 2 exclusions.
    for count in range(
        2,
        len(CANDIDATE_BAD_HOURS) + 1,
    ):
        for combo in itertools.combinations(
            CANDIDATE_BAD_HOURS,
            count,
        ):
            excluded = set(
                combo
            )

            add(
                "EXCLUDE_"
                + "_".join(
                    f"{hour:02d}"
                    for hour in combo
                ),
                "EXHAUSTIVE_BAD_HOUR_COMBO",
                excluded,
            )

    return list(
        scenarios.values()
    )


# ============================================================
# RESEARCH
# ============================================================

def run_research():
    global STATUS

    try:
        print()
        print("=" * 78)
        print(
            "USD/JPY SHORT - FINAL TIMING STABILITY CHECK"
        )
        print("=" * 78)
        print(
            "Candidate bad hours:",
            CANDIDATE_BAD_HOURS,
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

        candidates = (
            build_candidates(
                h1,
                h1_atr,
                daily_lookup,
            )
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
                "Running final timing stability scenarios"
            ),
        })

        # Baseline first for per-hour era breakdown.
        (
            baseline_trades,
            baseline_ignored,
            baseline_open,
        ) = simulate(
            h1,
            candidates,
            excluded_hours=set(),
        )

        hour_era_df = (
            build_hour_era_table(
                baseline_trades
            )
        )

        hour_era_df.to_csv(
            HOUR_ERAS_FILE,
            index=False,
        )

        scenarios = (
            generate_scenarios()
        )

        rows = []

        for number, scenario in enumerate(
            scenarios,
            start=1,
        ):
            (
                trades,
                ignored,
                still_open,
            ) = simulate(
                h1,
                candidates,
                excluded_hours=scenario[
                    "excluded"
                ],
            )

            rows.append(
                scenario_row(
                    scenario[
                        "name"
                    ],
                    scenario[
                        "type"
                    ],
                    scenario[
                        "excluded"
                    ],
                    trades,
                    ignored,
                    still_open,
                    years,
                )
            )

            STATUS[
                "completed_scenarios"
            ] = number

        df = pd.DataFrame(
            rows
        )

        # Baseline comparisons.
        baseline = df[
            df[
                "excluded_hour_count"
            ] == 0
        ].iloc[0]

        full_six = df[
            df[
                "excluded_hours"
            ] == "1,2,5,6,10,11"
        ].iloc[0]

        baseline_pf = float(
            baseline[
                "profit_factor"
            ]
        )

        baseline_tpy = float(
            baseline[
                "trades_per_year"
            ]
        )

        full_six_pf = float(
            full_six[
                "profit_factor"
            ]
        )

        full_six_tpy = float(
            full_six[
                "trades_per_year"
            ]
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
            "tpy_change_vs_baseline"
        ] = (
            df[
                "trades_per_year"
            ]
            - baseline_tpy
        ).round(
            2
        )

        df[
            "pf_change_vs_full_six"
        ] = (
            df[
                "profit_factor"
            ]
            - full_six_pf
        ).round(
            3
        )

        df[
            "tpy_change_vs_full_six"
        ] = (
            df[
                "trades_per_year"
            ]
            - full_six_tpy
        ).round(
            2
        )

        # Main target:
        # keep all eras positive, preserve at least 6 trades/yr,
        # PF >= 1.50, worst-era PF >= 1.10.
        df[
            "strong_candidate"
        ] = (
            df[
                "all_four_eras_profitable"
            ]
            & (
                df[
                    "trades_per_year"
                ] >= 6.0
            )
            & (
                df[
                    "profit_factor"
                ] >= 1.50
            )
            & (
                df[
                    "minimum_era_pf_5_plus"
                ].fillna(0)
                >= 1.10
            )
        )

        # Simplicity preference:
        # among robust rules, prefer fewer excluded hours.
        df = df.sort_values(
            by=[
                "strong_candidate",
                "all_four_eras_profitable",
                "minimum_era_pf_5_plus",
                "profit_factor",
                "trades_per_year",
                "excluded_hour_count",
                "total_r",
            ],
            ascending=[
                False,
                False,
                False,
                False,
                False,
                True,
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
                "USD/JPY final timing stability check completed"
            ),
            "completed_scenarios": len(
                df
            ),
            "strong_candidate_rows": int(
                df[
                    "strong_candidate"
                ].sum()
            ),
            "output_file": (
                OUTPUT_FILE
            ),
            "hour_eras_file": (
                HOUR_ERAS_FILE
            ),
            "full_six_pf": (
                full_six_pf
            ),
            "full_six_trades_per_year": (
                full_six_tpy
            ),
        })

        print()
        print("=" * 78)
        print(
            "USD/JPY FINAL TIMING STABILITY COMPLETE"
        )
        print("=" * 78)
        print(
            "Scenarios:",
            len(df),
        )
        print(
            "Strong candidates:",
            int(
                df[
                    "strong_candidate"
                ].sum()
            ),
        )
        print(
            "Saved:",
            OUTPUT_FILE,
        )
        print(
            "Hour-by-era file:",
            HOUR_ERAS_FILE,
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
            "USDJPY Short Final Timing Stability"
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
            "weekdays": "ALL",
        },
        "candidate_bad_hours": (
            CANDIDATE_BAD_HOURS
        ),
        "natural_pairs": {
            key: sorted(
                value
            )
            for key, value
            in NATURAL_PAIRS.items()
        },
        "downloads": {
            "scenarios": "/download",
            "hour_eras": "/download-hour-eras",
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


@app.route("/download-hour-eras")
def download_hour_eras():
    if not os.path.exists(
        HOUR_ERAS_FILE
    ):
        return jsonify({
            "status": "not_ready",
        }), 404

    return send_file(
        HOUR_ERAS_FILE,
        as_attachment=True,
        download_name=HOUR_ERAS_FILE,
    )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    research_thread = threading.Thread(
        target=run_research,
        name=(
            "usdjpy-short-final-timing-stability"
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
