
import os
import csv
import time
import bisect
import threading
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from flask import Flask, jsonify, send_file


# ============================================================
# EUR/USD M15 SHORT - STAGE 1 FAST DISCOVERY
#
# READ-ONLY RESEARCH. NEVER SENDS ORDERS.
#
# Built completely from scratch.
#
# Exact bearish engulfing:
#   previous candle bullish
#   current candle bearish
#   current open >= previous close
#   current close <= previous open
#
# Stage 1:
#   1) raw engulfing baseline
#   2) body-ratio sweep
#   3) body/ATR sweep
#   4) range/ATR sweep
#   5) bearish close-location sweep
#   6) structure sweep near prior highs
#   7) RR sweep
#   8) NY hour breakdown
#   9) weekday breakdown
#
# Costs:
#   0.50 / 1.00 / 1.50 / 2.00 pips adverse entry
#
# Primary development cost:
#   1.00 pip
#
# Historical conventions:
#   - OANDA midpoint M15
#   - ATR14 Wilder/RMA, SMA seeded
#   - short stop = signal high + 10 ticks
#   - target based on REFERENCE signal-close risk
#   - adverse short entry = signal close - cost
#   - exits begin NEXT candle
#   - same-bar short tie:
#       high closer => STOP first
#       otherwise TARGET first
#   - pyramiding 0
#   - exact exit-candle signal eligible
# ============================================================


app = Flask(__name__)

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_BASE = os.getenv(
    "OANDA_API_URL",
    "https://api-fxtrade.oanda.com"
)

INSTRUMENT = "EUR_USD"
GRANULARITY = "M15"

RESEARCH_FROM = datetime(
    2010, 1, 1, 0, 0,
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

NY = ZoneInfo("America/New_York")

TICK_SIZE = 0.00001
PIP_SIZE = 0.0001
STOP_BUFFER_TICKS = 10

PRIMARY_COST_PIPS = 1.00

COST_PIPS_GRID = [
    0.50,
    1.00,
    1.50,
    2.00,
]

OUTPUT_SUMMARY = (
    "eurusd_m15_short_stage1_summary.csv"
)

OUTPUT_TOP = (
    "eurusd_m15_short_stage1_top.csv"
)

OUTPUT_HOURS = (
    "eurusd_m15_short_stage1_hours.csv"
)

OUTPUT_WEEKDAYS = (
    "eurusd_m15_short_stage1_weekdays.csv"
)

OUTPUT_RAW_TRADES = (
    "eurusd_m15_short_stage1_raw_trades.csv"
)

STATUS = {
    "state": "not_started",
    "message": "EUR/USD M15 short Stage 1 has not started",
    "service": "EURUSD M15 Short Stage 1 Fast Discovery",
    "orders_supported": False,
    "trading_enabled": False,
}


# ============================================================
# HELPERS
# ============================================================

def iso_utc(dt):
    return (
        dt.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def parse_oanda_time(value):
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    if "." in value:
        left, right = value.split(".", 1)

        if "+" in right:
            fraction, offset = right.split("+", 1)
            fraction = fraction[:6].ljust(6, "0")
            value = left + "." + fraction + "+" + offset

    return datetime.fromisoformat(
        value
    ).astimezone(
        timezone.utc
    )


def write_csv(path, rows):
    if not rows:
        with open(
            path,
            "w",
            encoding="utf-8",
        ) as handle:
            handle.write("")
        return

    fieldnames = []
    seen = set()

    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)

    with open(
        path,
        "w",
        newline="",
        encoding="utf-8",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
        )
        writer.writeheader()
        writer.writerows(rows)


def download_file(path):
    if not os.path.exists(path):
        return jsonify({
            "error": "Output not ready yet",
            "path": path,
        }), 404

    return send_file(
        os.path.abspath(path),
        as_attachment=True,
        download_name=os.path.basename(path),
    )


# ============================================================
# OANDA FETCH
# ============================================================

def oanda_headers():
    if not OANDA_TOKEN:
        raise RuntimeError(
            "OANDA_TOKEN is not configured"
        )

    return {
        "Authorization":
            "Bearer " + OANDA_TOKEN.strip(),
        "Content-Type":
            "application/json",
    }


def fetch_chunk(start, end):
    url = (
        f"{OANDA_BASE}"
        f"/v3/instruments/"
        f"{INSTRUMENT}/candles"
    )

    params = {
        "price": "M",
        "granularity": GRANULARITY,
        "smooth": "false",
        "from": iso_utc(start),
        "to": iso_utc(end),
        "includeFirst": "true",
    }

    response = requests.get(
        url,
        headers=oanda_headers(),
        params=params,
        timeout=60,
    )

    response.raise_for_status()

    candles = []

    for item in response.json().get(
        "candles",
        [],
    ):
        if not item.get(
            "complete",
            False,
        ):
            continue

        mid = item["mid"]

        candles.append({
            "time":
                parse_oanda_time(
                    item["time"]
                ),
            "open":
                float(mid["o"]),
            "high":
                float(mid["h"]),
            "low":
                float(mid["l"]),
            "close":
                float(mid["c"]),
        })

    return candles


def fetch_full_history(start, end):
    cursor = start
    by_time = {}
    chunk_number = 0

    while cursor < end:
        chunk_number += 1

        chunk_end = min(
            cursor + timedelta(days=30),
            end,
        )

        STATUS.update({
            "state": "fetching",
            "message": (
                f"Fetching chunk {chunk_number}: "
                f"{iso_utc(cursor)} -> "
                f"{iso_utc(chunk_end)}"
            ),
            "chunk": chunk_number,
        })

        chunk = fetch_chunk(
            cursor,
            chunk_end,
        )

        for candle in chunk:
            by_time[
                candle["time"]
            ] = candle

        cursor = chunk_end
        time.sleep(0.03)

    candles = list(
        by_time.values()
    )

    candles.sort(
        key=lambda row:
            row["time"]
    )

    return candles


# ============================================================
# ATR
# ============================================================

def add_atr14(candles):
    n = len(candles)

    true_ranges = [None] * n
    atr = [None] * n

    for i in range(n):
        high = candles[i]["high"]
        low = candles[i]["low"]

        if i == 0:
            tr = high - low
        else:
            previous_close = candles[
                i - 1
            ]["close"]

            tr = max(
                high - low,
                abs(high - previous_close),
                abs(low - previous_close),
            )

        true_ranges[i] = tr

    if n < 14:
        return atr

    atr[13] = (
        sum(true_ranges[:14])
        / 14.0
    )

    for i in range(14, n):
        atr[i] = (
            atr[i - 1] * 13.0
            + true_ranges[i]
        ) / 14.0

    return atr


# ============================================================
# SIGNAL CACHE
# ============================================================

def bearish_engulfing(candles, i):
    if i < 1:
        return False

    previous = candles[i - 1]
    current = candles[i]

    return (
        previous["close"] > previous["open"]
        and
        current["close"] < current["open"]
        and
        current["open"] >= previous["close"]
        and
        current["close"] <= previous["open"]
    )


STRUCTURE_LOOKBACKS = [
    10,
    20,
    40,
    60,
    100,
    150,
]


def build_signal_cache(
    candles,
    atr14,
):
    signals = []

    max_lookback = max(
        STRUCTURE_LOOKBACKS
    )

    for i in range(
        max(
            14,
            max_lookback,
        ),
        len(candles),
    ):
        if not bearish_engulfing(
            candles,
            i,
        ):
            continue

        current = candles[i]
        previous = candles[i - 1]
        atr = atr14[i]

        if (
            atr is None
            or atr <= 0
        ):
            continue

        body = (
            current["open"]
            - current["close"]
        )

        previous_body = abs(
            previous["close"]
            - previous["open"]
        )

        candle_range = (
            current["high"]
            - current["low"]
        )

        body_ratio = (
            body / previous_body
            if previous_body > 0
            else 999.0
        )

        body_atr = (
            body / atr
        )

        range_atr = (
            candle_range / atr
        )

        # For a bearish candle, lower close location is stronger.
        # 0.0 = closes at low, 1.0 = closes at high.
        close_location = (
            (
                current["close"]
                - current["low"]
            )
            / candle_range
            if candle_range > 0
            else 1.0
        )

        ny_time = (
            current["time"]
            .astimezone(NY)
        )

        structure_distance_atr = {}

        for lookback in STRUCTURE_LOOKBACKS:
            previous_high = max(
                candle["high"]
                for candle in candles[
                    i - lookback:i
                ]
            )

            structure_distance_atr[
                lookback
            ] = (
                abs(
                    current["high"]
                    - previous_high
                )
                / atr
            )

        signals.append({
            "signal_index":
                i,
            "time":
                current["time"],
            "body_ratio":
                body_ratio,
            "body_atr":
                body_atr,
            "range_atr":
                range_atr,
            "close_location":
                close_location,
            "ny_hour":
                ny_time.hour,
            "ny_weekday":
                ny_time.weekday(),
            "structure_distance_atr":
                structure_distance_atr,
        })

    return signals


# ============================================================
# TRADE OUTCOME CACHE
# ============================================================

RR_GRID = [
    1.50,
    2.00,
    2.50,
    3.00,
    3.50,
    4.00,
    4.50,
    5.00,
]


def compute_trade_outcome(
    candles,
    signal_index,
    reward_risk,
    cost_pips,
):
    signal = candles[
        signal_index
    ]

    reference_entry = (
        signal["close"]
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
        return None

    target = (
        reference_entry
        - reward_risk
        * reference_risk
    )

    backtest_entry = (
        reference_entry
        - cost_pips
        * PIP_SIZE
    )

    actual_risk = (
        stop
        - backtest_entry
    )

    if actual_risk <= 0:
        return None

    for j in range(
        signal_index + 1,
        len(candles),
    ):
        candle = candles[j]

        hit_stop = (
            candle["high"]
            >= stop
        )

        hit_target = (
            candle["low"]
            <= target
        )

        if (
            hit_stop
            and
            hit_target
        ):
            distance_high = abs(
                candle["high"]
                - candle["open"]
            )

            distance_low = abs(
                candle["open"]
                - candle["low"]
            )

            # Locked short same-bar tie:
            # high closer => stop first,
            # otherwise target first.
            if distance_high < distance_low:
                exit_price = stop
                exit_reason = "STOP"
            else:
                exit_price = target
                exit_reason = "TARGET"

        elif hit_stop:
            exit_price = stop
            exit_reason = "STOP"

        elif hit_target:
            exit_price = target
            exit_reason = "TARGET"

        else:
            continue

        result_r = (
            backtest_entry
            - exit_price
        ) / actual_risk

        return {
            "signal_index":
                signal_index,
            "exit_index":
                j,
            "entry_time":
                signal["time"],
            "exit_time":
                candle["time"],
            "entry_time_utc":
                iso_utc(
                    signal["time"]
                ),
            "exit_time_utc":
                iso_utc(
                    candle["time"]
                ),
            "reference_entry":
                reference_entry,
            "backtest_entry":
                backtest_entry,
            "stop":
                stop,
            "target":
                target,
            "exit_reason":
                exit_reason,
            "result_r":
                result_r,
            "reward_risk":
                reward_risk,
            "cost_pips":
                cost_pips,
        }

    return None


def build_outcome_cache(
    candles,
    signals,
):
    cache = {}

    total = (
        len(signals)
        * len(RR_GRID)
        * len(COST_PIPS_GRID)
    )

    done = 0

    for signal in signals:
        signal_index = signal[
            "signal_index"
        ]

        for rr in RR_GRID:
            for cost in COST_PIPS_GRID:
                done += 1

                if done % 1000 == 0:
                    STATUS.update({
                        "state":
                            "precomputing",
                        "message":
                            (
                                "Caching trade outcomes "
                                f"{done}/{total}"
                            ),
                        "outcomes_done":
                            done,
                        "outcomes_total":
                            total,
                    })

                cache[
                    (
                        signal_index,
                        rr,
                        cost,
                    )
                ] = compute_trade_outcome(
                    candles,
                    signal_index,
                    rr,
                    cost,
                )

    return cache


# ============================================================
# FAST EXECUTION
# ============================================================

def signal_passes(
    signal,
    config,
):
    if (
        signal[
            "body_ratio"
        ]
        <
        config.get(
            "minimum_body_ratio",
            1.0,
        )
    ):
        return False

    body_atr = config.get(
        "minimum_body_atr"
    )

    if (
        body_atr is not None
        and
        signal[
            "body_atr"
        ] < body_atr
    ):
        return False

    range_atr = config.get(
        "minimum_range_atr"
    )

    if (
        range_atr is not None
        and
        signal[
            "range_atr"
        ] < range_atr
    ):
        return False

    max_close_location = config.get(
        "maximum_close_location"
    )

    if (
        max_close_location is not None
        and
        signal[
            "close_location"
        ] > max_close_location
    ):
        return False

    lookback = config.get(
        "structure_lookback"
    )

    distance = config.get(
        "maximum_distance_atr"
    )

    if (
        lookback is not None
        and
        distance is not None
        and
        signal[
            "structure_distance_atr"
        ][
            lookback
        ] > distance
    ):
        return False

    included_hours = config.get(
        "included_ny_hours"
    )

    if (
        included_hours is not None
        and
        signal[
            "ny_hour"
        ] not in included_hours
    ):
        return False

    excluded_weekdays = config.get(
        "excluded_weekdays",
        set(),
    )

    if (
        signal[
            "ny_weekday"
        ] in excluded_weekdays
    ):
        return False

    return True


def run_config_cached(
    signals,
    outcome_cache,
    config,
    cost_pips,
):
    candidates = [
        signal
        for signal in signals
        if signal_passes(
            signal,
            config,
        )
    ]

    signal_indices = [
        signal[
            "signal_index"
        ]
        for signal in candidates
    ]

    trades = []

    position = 0

    while position < len(
        candidates
    ):
        signal = candidates[
            position
        ]

        trade = outcome_cache.get(
            (
                signal[
                    "signal_index"
                ],
                config[
                    "reward_risk"
                ],
                cost_pips,
            )
        )

        if trade is None:
            position += 1
            continue

        trades.append(
            dict(trade)
        )

        position = bisect.bisect_left(
            signal_indices,
            trade[
                "exit_index"
            ],
            lo=position + 1,
        )

    return trades


# ============================================================
# STATS
# ============================================================

def stats_from_trades(trades):
    results = [
        float(
            trade["result_r"]
        )
        for trade in trades
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
        sum(losers)
    )

    pf = (
        gross_profit
        / gross_loss
        if gross_loss > 0
        else (
            999.0
            if gross_profit > 0
            else 0.0
        )
    )

    total_r = sum(
        results
    )

    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    streak = 0
    longest = 0

    for result in results:
        equity += result
        peak = max(
            peak,
            equity,
        )
        max_dd = min(
            max_dd,
            equity - peak,
        )

        if result < 0:
            streak += 1
            longest = max(
                longest,
                streak,
            )
        else:
            streak = 0

    return {
        "trades":
            len(results),
        "winners":
            len(winners),
        "losers":
            len(losers),
        "win_rate":
            (
                len(winners)
                / len(results)
                * 100.0
                if results
                else 0.0
            ),
        "profit_factor":
            pf,
        "total_r":
            total_r,
        "expectancy_r":
            (
                total_r
                / len(results)
                if results
                else 0.0
            ),
        "max_drawdown_r":
            max_dd,
        "longest_loss_streak":
            longest,
    }


def result_row(
    family,
    config,
    cost,
    trades,
):
    stats = stats_from_trades(
        trades
    )

    return {
        "family":
            family,
        "candidate":
            config[
                "label"
            ],
        "cost_pips":
            cost,
        "minimum_body_ratio":
            config.get(
                "minimum_body_ratio"
            ),
        "minimum_body_atr":
            config.get(
                "minimum_body_atr"
            ),
        "minimum_range_atr":
            config.get(
                "minimum_range_atr"
            ),
        "maximum_close_location":
            config.get(
                "maximum_close_location"
            ),
        "structure_lookback":
            config.get(
                "structure_lookback"
            ),
        "maximum_distance_atr":
            config.get(
                "maximum_distance_atr"
            ),
        "reward_risk":
            config[
                "reward_risk"
            ],
        "trades":
            stats[
                "trades"
            ],
        "winners":
            stats[
                "winners"
            ],
        "losers":
            stats[
                "losers"
            ],
        "win_rate":
            round(
                stats[
                    "win_rate"
                ],
                4,
            ),
        "profit_factor":
            round(
                stats[
                    "profit_factor"
                ],
                6,
            ),
        "total_r":
            round(
                stats[
                    "total_r"
                ],
                4,
            ),
        "expectancy_r":
            round(
                stats[
                    "expectancy_r"
                ],
                6,
            ),
        "max_drawdown_r":
            round(
                stats[
                    "max_drawdown_r"
                ],
                4,
            ),
        "longest_loss_streak":
            stats[
                "longest_loss_streak"
            ],
    }


# ============================================================
# DISCOVERY CONFIGS
# ============================================================

def base_config():
    return {
        "label":
            "RAW_BASELINE",
        "minimum_body_ratio":
            1.00,
        "minimum_body_atr":
            None,
        "minimum_range_atr":
            None,
        "maximum_close_location":
            None,
        "structure_lookback":
            None,
        "maximum_distance_atr":
            None,
        "included_ny_hours":
            None,
        "excluded_weekdays":
            set(),
        "reward_risk":
            2.50,
    }


def discovery_configs():
    configs = []

    configs.append((
        "BASELINE",
        base_config(),
    ))

    for value in [
        1.00,
        1.10,
        1.20,
        1.30,
        1.40,
        1.60,
        1.80,
        2.00,
    ]:
        cfg = base_config()
        cfg[
            "minimum_body_ratio"
        ] = value
        cfg[
            "label"
        ] = f"BR_{value:.2f}"

        configs.append((
            "BODY_RATIO",
            cfg,
        ))

    for value in [
        0.30,
        0.40,
        0.50,
        0.60,
        0.70,
        0.80,
        1.00,
        1.20,
    ]:
        cfg = base_config()
        cfg[
            "minimum_body_atr"
        ] = value
        cfg[
            "label"
        ] = f"BODY_ATR_{value:.2f}"

        configs.append((
            "BODY_ATR",
            cfg,
        ))

    for value in [
        0.60,
        0.80,
        1.00,
        1.20,
        1.40,
        1.60,
        1.80,
    ]:
        cfg = base_config()
        cfg[
            "minimum_range_atr"
        ] = value
        cfg[
            "label"
        ] = f"RANGE_ATR_{value:.2f}"

        configs.append((
            "RANGE_ATR",
            cfg,
        ))

    # Lower close-location = stronger bearish close.
    for value in [
        0.45,
        0.40,
        0.35,
        0.30,
        0.25,
        0.20,
        0.15,
    ]:
        cfg = base_config()
        cfg[
            "maximum_close_location"
        ] = value
        cfg[
            "label"
        ] = f"BEAR_CLOSE_{value:.2f}"

        configs.append((
            "CLOSE_LOCATION",
            cfg,
        ))

    for lookback in STRUCTURE_LOOKBACKS:
        for distance in [
            0.05,
            0.10,
            0.20,
            0.30,
            0.50,
            0.75,
        ]:
            cfg = base_config()

            cfg[
                "structure_lookback"
            ] = lookback

            cfg[
                "maximum_distance_atr"
            ] = distance

            cfg[
                "label"
            ] = (
                f"STRUCT_{lookback}_"
                f"{distance:.2f}"
            )

            configs.append((
                "STRUCTURE",
                cfg,
            ))

    for rr in RR_GRID:
        cfg = base_config()

        cfg[
            "reward_risk"
        ] = rr

        cfg[
            "label"
        ] = f"RR_{rr:.2f}"

        configs.append((
            "REWARD_RISK",
            cfg,
        ))

    return configs


# ============================================================
# RAW HOUR / WEEKDAY BREAKDOWN
# ============================================================

def raw_signal_trades(
    signals,
    outcome_cache,
):
    config = base_config()

    candidates = [
        signal
        for signal in signals
        if signal_passes(
            signal,
            config,
        )
    ]

    signal_indices = [
        s[
            "signal_index"
        ]
        for s in candidates
    ]

    trades = []

    position = 0

    while position < len(
        candidates
    ):
        signal = candidates[
            position
        ]

        trade = outcome_cache.get(
            (
                signal[
                    "signal_index"
                ],
                config[
                    "reward_risk"
                ],
                PRIMARY_COST_PIPS,
            )
        )

        if trade is None:
            position += 1
            continue

        row = dict(
            trade
        )

        row.update({
            "body_ratio":
                signal[
                    "body_ratio"
                ],
            "body_atr":
                signal[
                    "body_atr"
                ],
            "range_atr":
                signal[
                    "range_atr"
                ],
            "close_location":
                signal[
                    "close_location"
                ],
            "ny_hour":
                signal[
                    "ny_hour"
                ],
            "ny_weekday":
                signal[
                    "ny_weekday"
                ],
        })

        trades.append(
            row
        )

        position = bisect.bisect_left(
            signal_indices,
            trade[
                "exit_index"
            ],
            lo=position + 1,
        )

    return trades


def grouped_stats(
    trades,
    field,
):
    values = sorted(
        set(
            trade[field]
            for trade in trades
        )
    )

    rows = []

    for value in values:
        subset = [
            trade
            for trade in trades
            if trade[field] == value
        ]

        stats = stats_from_trades(
            subset
        )

        rows.append({
            field:
                value,
            "cost_pips":
                PRIMARY_COST_PIPS,
            "trades":
                stats[
                    "trades"
                ],
            "win_rate":
                round(
                    stats[
                        "win_rate"
                    ],
                    4,
                ),
            "profit_factor":
                round(
                    stats[
                        "profit_factor"
                    ],
                    6,
                ),
            "total_r":
                round(
                    stats[
                        "total_r"
                    ],
                    4,
                ),
            "expectancy_r":
                round(
                    stats[
                        "expectancy_r"
                    ],
                    6,
                ),
            "max_drawdown_r":
                round(
                    stats[
                        "max_drawdown_r"
                    ],
                    4,
                ),
        })

    return rows


# ============================================================
# RUNNER
# ============================================================

def run_research():
    try:
        STATUS.update({
            "state":
                "fetching",
            "message":
                "Fetching EUR/USD M15 history",
            "from":
                iso_utc(
                    RESEARCH_FROM
                ),
            "to":
                iso_utc(
                    RESEARCH_TO
                ),
        })

        candles = fetch_full_history(
            RESEARCH_FROM,
            RESEARCH_TO,
        )

        if len(candles) < 1000:
            raise RuntimeError(
                "Too few M15 candles returned"
            )

        STATUS.update({
            "state":
                "precomputing",
            "message":
                "Calculating ATR and caching bearish engulfing signals",
            "candles":
                len(candles),
        })

        atr14 = add_atr14(
            candles
        )

        signals = build_signal_cache(
            candles,
            atr14,
        )

        STATUS.update({
            "state":
                "precomputing",
            "message":
                "Caching reusable short trade outcomes",
            "engulfing_signals":
                len(signals),
        })

        outcome_cache = build_outcome_cache(
            candles,
            signals,
        )

        configs = discovery_configs()

        summary_rows = []

        total_runs = (
            len(configs)
            * len(COST_PIPS_GRID)
        )

        run_number = 0

        for family, config in configs:
            for cost in COST_PIPS_GRID:
                run_number += 1

                STATUS.update({
                    "state":
                        "calculating",
                    "message":
                        (
                            f"{family} / "
                            f"{config['label']} / "
                            f"{cost:.2f} pip"
                        ),
                    "run":
                        run_number,
                    "runs_total":
                        total_runs,
                })

                trades = run_config_cached(
                    signals,
                    outcome_cache,
                    config,
                    cost,
                )

                summary_rows.append(
                    result_row(
                        family,
                        config,
                        cost,
                        trades,
                    )
                )

        write_csv(
            OUTPUT_SUMMARY,
            summary_rows,
        )

        primary = [
            row
            for row in summary_rows
            if (
                abs(
                    float(
                        row[
                            "cost_pips"
                        ]
                    )
                    - PRIMARY_COST_PIPS
                ) < 1e-12
                and
                int(
                    row[
                        "trades"
                    ]
                ) >= 100
            )
        ]

        primary.sort(
            key=lambda row: (
                float(
                    row[
                        "profit_factor"
                    ]
                ),
                float(
                    row[
                        "expectancy_r"
                    ]
                ),
                float(
                    row[
                        "total_r"
                    ]
                ),
            ),
            reverse=True,
        )

        top_rows = primary[:30]

        write_csv(
            OUTPUT_TOP,
            top_rows,
        )

        raw_trades = raw_signal_trades(
            signals,
            outcome_cache,
        )

        write_csv(
            OUTPUT_RAW_TRADES,
            raw_trades,
        )

        hour_rows = grouped_stats(
            raw_trades,
            "ny_hour",
        )

        weekday_rows = grouped_stats(
            raw_trades,
            "ny_weekday",
        )

        weekday_names = {
            0: "Monday",
            1: "Tuesday",
            2: "Wednesday",
            3: "Thursday",
            4: "Friday",
            5: "Saturday",
            6: "Sunday",
        }

        for row in weekday_rows:
            row[
                "weekday_name"
            ] = weekday_names.get(
                int(
                    row[
                        "ny_weekday"
                    ]
                ),
                "Unknown",
            )

        write_csv(
            OUTPUT_HOURS,
            hour_rows,
        )

        write_csv(
            OUTPUT_WEEKDAYS,
            weekday_rows,
        )

        baseline = next(
            (
                row
                for row in summary_rows
                if (
                    row[
                        "candidate"
                    ] == "RAW_BASELINE"
                    and
                    abs(
                        float(
                            row[
                                "cost_pips"
                            ]
                        )
                        - PRIMARY_COST_PIPS
                    ) < 1e-12
                )
            ),
            None,
        )

        STATUS.update({
            "state":
                "complete",
            "message":
                "EUR/USD M15 short Stage 1 complete",
            "candles":
                len(candles),
            "engulfing_signals":
                len(signals),
            "candidate_runs":
                len(summary_rows),
            "primary_cost_pips":
                PRIMARY_COST_PIPS,
            "baseline_primary":
                baseline,
            "top_primary_candidates":
                top_rows[:10],
            "outputs": {
                "summary":
                    OUTPUT_SUMMARY,
                "top":
                    OUTPUT_TOP,
                "hours":
                    OUTPUT_HOURS,
                "weekdays":
                    OUTPUT_WEEKDAYS,
                "raw_trades":
                    OUTPUT_RAW_TRADES,
            },
        })

        print()
        print("=" * 100)
        print(
            "EUR/USD M15 SHORT STAGE 1 COMPLETE"
        )
        print("=" * 100)
        print(
            "Candles:",
            len(candles),
        )
        print(
            "Bearish engulfing signals:",
            len(signals),
        )
        print(
            "Baseline:",
            baseline,
        )
        print()
        print(
            "Top 10 candidates:"
        )

        for row in top_rows[:10]:
            print(row)

    except Exception as error:
        STATUS.update({
            "state":
                "error",
            "message":
                str(error),
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
def root():
    return jsonify({
        "service":
            "EURUSD M15 Short Stage 1 Fast Discovery",
        "status":
            STATUS[
                "state"
            ],
        "instrument":
            INSTRUMENT,
        "timeframe":
            GRANULARITY,
        "side":
            "SELL",
        "orders_supported":
            False,
        "trading_enabled":
            False,
        "routes": [
            "/m15-short/status",
            "/m15-short/summary",
            "/m15-short/top",
            "/m15-short/hours",
            "/m15-short/weekdays",
            "/m15-short/raw-trades",
        ],
    })


@app.route("/m15-short/status")
def route_status():
    return jsonify(
        STATUS
    )


@app.route("/m15-short/summary")
def route_summary():
    return download_file(
        OUTPUT_SUMMARY
    )


@app.route("/m15-short/top")
def route_top():
    return download_file(
        OUTPUT_TOP
    )


@app.route("/m15-short/hours")
def route_hours():
    return download_file(
        OUTPUT_HOURS
    )


@app.route("/m15-short/weekdays")
def route_weekdays():
    return download_file(
        OUTPUT_WEEKDAYS
    )


@app.route("/m15-short/raw-trades")
def route_raw_trades():
    return download_file(
        OUTPUT_RAW_TRADES
    )


if __name__ == "__main__":
    research_thread = threading.Thread(
        target=run_research,
        name="eurusd-m15-short-stage1-fast",
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
