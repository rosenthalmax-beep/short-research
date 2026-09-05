
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
# EUR/USD M15 SHORT - STAGE 2 FAST INTERACTION TEST
#
# READ-ONLY RESEARCH. NEVER SENDS ORDERS.
#
# Stage 1 findings:
#   - raw bearish engulfing weak
#   - large range/body improved results materially
#   - early NY hours ~01:00-05:00 were least bad
#   - structure was not a dominant standalone edge
#
# Stage 2 therefore focuses on:
#   - range ATR
#   - body ATR
#   - early NY hour sets
#   - RR
#   - light structure overlay
#
# Costs:
#   0.50 / 1.00 / 1.50 / 2.00 pips adverse entry
#
# Primary development cost:
#   1.00 pip
#
# Validation:
#   full history
#   4 eras
#   recent 5Y / 2Y
#
# Historical conventions:
#   - OANDA midpoint M15
#   - exact bearish engulfing
#   - ATR14 Wilder/RMA, SMA seeded
#   - stop = signal high + 10 ticks
#   - target from REFERENCE signal close
#   - adverse entry = signal close - cost
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
    "eurusd_m15_short_stage2_summary.csv"
)

OUTPUT_TOP = (
    "eurusd_m15_short_stage2_top.csv"
)

OUTPUT_ERAS = (
    "eurusd_m15_short_stage2_eras.csv"
)

OUTPUT_RECENT = (
    "eurusd_m15_short_stage2_recent.csv"
)

OUTPUT_TRADES = (
    "eurusd_m15_short_stage2_best_trades.csv"
)

STATUS = {
    "state": "not_started",
    "message": "EUR/USD M15 short Stage 2 has not started",
    "service": "EURUSD M15 Short Stage 2 Fast Interaction",
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


def years_ago_safe(dt, years):
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


def write_csv(path, rows):
    if not rows:
        with open(
            path,
            "w",
            encoding="utf-8",
        ) as handle:
            handle.write("")
        return

    fields = []
    seen = set()

    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fields.append(key)

    with open(
        path,
        "w",
        newline="",
        encoding="utf-8",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fields,
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
# OANDA DATA
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

        for candle in fetch_chunk(
            cursor,
            chunk_end,
        ):
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
# ATR / SIGNAL CACHE
# ============================================================

def add_atr14(candles):
    n = len(candles)

    trs = [None] * n
    atr = [None] * n

    for i in range(n):
        high = candles[i]["high"]
        low = candles[i]["low"]

        if i == 0:
            trs[i] = high - low
        else:
            previous_close = candles[
                i - 1
            ]["close"]

            trs[i] = max(
                high - low,
                abs(high - previous_close),
                abs(low - previous_close),
            )

    if n < 14:
        return atr

    atr[13] = (
        sum(trs[:14])
        / 14.0
    )

    for i in range(14, n):
        atr[i] = (
            atr[i - 1] * 13.0
            + trs[i]
        ) / 14.0

    return atr


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
    20,
    40,
    60,
    100,
]

RR_GRID = [
    1.50,
    2.00,
    2.50,
    3.00,
    3.50,
    4.00,
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
        max(14, max_lookback),
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
            candle["high"] >= stop
        )

        hit_target = (
            candle["low"] <= target
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
                                "Caching short trade outcomes "
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
# CONFIG GENERATION
# ============================================================

def stage2_configs():
    configs = []

    hour_sets = [
        None,
        {1, 2, 3, 4, 5},
        {2, 3, 4},
        {1, 2, 3, 4},
        {2, 3, 4, 5},
        {2, 3},
        {3, 4},
    ]

    body_atrs = [
        0.80,
        1.00,
        1.20,
        1.40,
    ]

    range_atrs = [
        1.20,
        1.40,
        1.60,
        1.80,
        2.00,
    ]

    # Light structure overlay only.
    structure_options = [
        (None, None),
        (40, 0.20),
        (60, 0.20),
        (60, 0.30),
        (100, 0.30),
    ]

    counter = 0

    # Main family: body + range + hours + RR
    for hours in hour_sets:
        for body_atr in body_atrs:
            for range_atr in range_atrs:
                for rr in RR_GRID:
                    counter += 1

                    configs.append({
                        "label":
                            (
                                f"C{counter:04d}_"
                                f"H{('ALL' if hours is None else ''.join(str(h) for h in sorted(hours)))}_"
                                f"BA{body_atr:.2f}_"
                                f"RA{range_atr:.2f}_"
                                f"RR{rr:.2f}"
                            ),
                        "minimum_body_ratio":
                            1.00,
                        "minimum_body_atr":
                            body_atr,
                        "minimum_range_atr":
                            range_atr,
                        "maximum_close_location":
                            None,
                        "structure_lookback":
                            None,
                        "maximum_distance_atr":
                            None,
                        "included_ny_hours":
                            hours,
                        "excluded_weekdays":
                            set(),
                        "reward_risk":
                            rr,
                    })

    # Secondary family: add mild structure to the stronger
    # large-candle combinations.
    for hours in [
        {1, 2, 3, 4, 5},
        {2, 3, 4},
        {2, 3, 4, 5},
    ]:
        for body_atr in [
            1.00,
            1.20,
            1.40,
        ]:
            for range_atr in [
                1.40,
                1.60,
                1.80,
            ]:
                for (
                    lookback,
                    distance,
                ) in structure_options[1:]:
                    for rr in RR_GRID:
                        counter += 1

                        configs.append({
                            "label":
                                (
                                    f"C{counter:04d}_"
                                    f"H{''.join(str(h) for h in sorted(hours))}_"
                                    f"BA{body_atr:.2f}_"
                                    f"RA{range_atr:.2f}_"
                                    f"S{lookback}_"
                                    f"D{distance:.2f}_"
                                    f"RR{rr:.2f}"
                                ),
                            "minimum_body_ratio":
                                1.00,
                            "minimum_body_atr":
                                body_atr,
                            "minimum_range_atr":
                                range_atr,
                            "maximum_close_location":
                                None,
                            "structure_lookback":
                                lookback,
                            "maximum_distance_atr":
                                distance,
                            "included_ny_hours":
                                hours,
                            "excluded_weekdays":
                                set(),
                            "reward_risk":
                                rr,
                        })

    return configs


def signal_passes(
    signal,
    config,
):
    if (
        signal[
            "body_ratio"
        ]
        <
        config[
            "minimum_body_ratio"
        ]
    ):
        return False

    if (
        signal[
            "body_atr"
        ]
        <
        config[
            "minimum_body_atr"
        ]
    ):
        return False

    if (
        signal[
            "range_atr"
        ]
        <
        config[
            "minimum_range_atr"
        ]
    ):
        return False

    hours = config[
        "included_ny_hours"
    ]

    if (
        hours is not None
        and
        signal[
            "ny_hour"
        ] not in hours
    ):
        return False

    lookback = config[
        "structure_lookback"
    ]

    distance = config[
        "maximum_distance_atr"
    ]

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

    return True


# ============================================================
# FAST EXECUTION
# ============================================================

def run_config_cached(
    signals,
    outcome_cache,
    config,
    cost_pips,
    start=None,
    end=None,
):
    candidates = [
        signal
        for signal in signals
        if (
            signal_passes(
                signal,
                config,
            )
            and
            (
                start is None
                or signal[
                    "time"
                ] >= start
            )
            and
            (
                end is None
                or signal[
                    "time"
                ] < end
            )
        )
    ]

    indices = [
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
            indices,
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
            trade[
                "result_r"
            ]
        )
        for trade in trades
    ]

    winners = [
        r for r in results
        if r > 0
    ]

    losers = [
        r for r in results
        if r < 0
    ]

    gross_profit = sum(winners)
    gross_loss = abs(sum(losers))
    total_r = sum(results)

    pf = (
        gross_profit / gross_loss
        if gross_loss > 0
        else (
            999.0
            if gross_profit > 0
            else 0.0
        )
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
    config,
    cost,
    trades,
):
    stats = stats_from_trades(
        trades
    )

    return {
        "candidate":
            config[
                "label"
            ],
        "cost_pips":
            cost,
        "minimum_body_atr":
            config[
                "minimum_body_atr"
            ],
        "minimum_range_atr":
            config[
                "minimum_range_atr"
            ],
        "structure_lookback":
            config[
                "structure_lookback"
            ],
        "maximum_distance_atr":
            config[
                "maximum_distance_atr"
            ],
        "included_ny_hours":
            (
                "ALL"
                if config[
                    "included_ny_hours"
                ] is None
                else ",".join(
                    str(hour)
                    for hour in sorted(
                        config[
                            "included_ny_hours"
                        ]
                    )
                )
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
# VALIDATION WINDOWS
# ============================================================

def era_windows():
    return [
        (
            "ERA_2010_2013",
            datetime(
                2010, 1, 1,
                tzinfo=timezone.utc,
            ),
            datetime(
                2014, 1, 1,
                tzinfo=timezone.utc,
            ),
        ),
        (
            "ERA_2014_2017",
            datetime(
                2014, 1, 1,
                tzinfo=timezone.utc,
            ),
            datetime(
                2018, 1, 1,
                tzinfo=timezone.utc,
            ),
        ),
        (
            "ERA_2018_2021",
            datetime(
                2018, 1, 1,
                tzinfo=timezone.utc,
            ),
            datetime(
                2022, 1, 1,
                tzinfo=timezone.utc,
            ),
        ),
        (
            "ERA_2022_NOW",
            datetime(
                2022, 1, 1,
                tzinfo=timezone.utc,
            ),
            RESEARCH_TO,
        ),
    ]


def recent_windows():
    return [
        (
            "LAST_5Y",
            years_ago_safe(
                RESEARCH_TO,
                5,
            ),
            RESEARCH_TO,
        ),
        (
            "LAST_2Y",
            years_ago_safe(
                RESEARCH_TO,
                2,
            ),
            RESEARCH_TO,
        ),
    ]


def validation_rows(
    signals,
    cache,
    finalists,
    windows,
):
    rows = []

    for rank, config in enumerate(
        finalists,
        start=1,
    ):
        for (
            label,
            start,
            end,
        ) in windows:
            trades = run_config_cached(
                signals,
                cache,
                config,
                PRIMARY_COST_PIPS,
                start=start,
                end=end,
            )

            stats = stats_from_trades(
                trades
            )

            rows.append({
                "rank":
                    rank,
                "window":
                    label,
                "candidate":
                    config[
                        "label"
                    ],
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
                "longest_loss_streak":
                    stats[
                        "longest_loss_streak"
                    ],
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
                "Calculating ATR and caching bearish signals",
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

        configs = stage2_configs()

        summary_rows = []

        total_runs = (
            len(configs)
            * len(COST_PIPS_GRID)
        )

        run_number = 0

        for config in configs:
            for cost in COST_PIPS_GRID:
                run_number += 1

                STATUS.update({
                    "state":
                        "calculating",
                    "message":
                        (
                            f"{config['label']} "
                            f"at {cost:.2f} pip"
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

        top_rows = primary[
            :30
        ]

        write_csv(
            OUTPUT_TOP,
            top_rows,
        )

        config_map = {
            config[
                "label"
            ]:
                config
            for config in configs
        }

        finalists = [
            config_map[
                row[
                    "candidate"
                ]
            ]
            for row in top_rows[
                :15
            ]
        ]

        era_rows = validation_rows(
            signals,
            outcome_cache,
            finalists,
            era_windows(),
        )

        recent_rows = validation_rows(
            signals,
            outcome_cache,
            finalists,
            recent_windows(),
        )

        write_csv(
            OUTPUT_ERAS,
            era_rows,
        )

        write_csv(
            OUTPUT_RECENT,
            recent_rows,
        )

        provisional = None

        for row in top_rows:
            label = row[
                "candidate"
            ]

            eras = [
                item
                for item in era_rows
                if item[
                    "candidate"
                ] == label
            ]

            recent = [
                item
                for item in recent_rows
                if item[
                    "candidate"
                ] == label
            ]

            positive_eras = sum(
                1
                for item in eras
                if (
                    int(
                        item[
                            "trades"
                        ]
                    ) >= 20
                    and
                    float(
                        item[
                            "profit_factor"
                        ]
                    ) > 1.0
                )
            )

            recent_ok = (
                len(recent) == 2
                and
                all(
                    int(
                        item[
                            "trades"
                        ]
                    ) >= 20
                    and
                    float(
                        item[
                            "profit_factor"
                        ]
                    ) > 1.0
                    for item in recent
                )
            )

            if (
                positive_eras >= 3
                and recent_ok
            ):
                provisional = row
                break

        if provisional is None and top_rows:
            provisional = top_rows[0]

        best_trades = []

        if provisional is not None:
            config = config_map[
                provisional[
                    "candidate"
                ]
            ]

            best_trades = run_config_cached(
                signals,
                outcome_cache,
                config,
                PRIMARY_COST_PIPS,
            )

        write_csv(
            OUTPUT_TRADES,
            best_trades,
        )

        STATUS.update({
            "state":
                "complete",
            "message":
                "EUR/USD M15 short Stage 2 complete",
            "candles":
                len(candles),
            "engulfing_signals":
                len(signals),
            "configs":
                len(configs),
            "candidate_runs":
                len(summary_rows),
            "primary_cost_pips":
                PRIMARY_COST_PIPS,
            "provisional_best":
                provisional,
            "outputs": {
                "summary":
                    OUTPUT_SUMMARY,
                "top":
                    OUTPUT_TOP,
                "eras":
                    OUTPUT_ERAS,
                "recent":
                    OUTPUT_RECENT,
                "best_trades":
                    OUTPUT_TRADES,
            },
        })

        print()
        print("=" * 100)
        print(
            "EUR/USD M15 SHORT STAGE 2 COMPLETE"
        )
        print("=" * 100)
        print(
            "Configs:",
            len(configs),
        )
        print(
            "Provisional best:",
            provisional,
        )

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
            "EURUSD M15 Short Stage 2 Fast Interaction",
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
            "/m15-short-stage2/status",
            "/m15-short-stage2/summary",
            "/m15-short-stage2/top",
            "/m15-short-stage2/eras",
            "/m15-short-stage2/recent",
            "/m15-short-stage2/best-trades",
        ],
    })


@app.route("/m15-short-stage2/status")
def route_status():
    return jsonify(
        STATUS
    )


@app.route("/m15-short-stage2/summary")
def route_summary():
    return download_file(
        OUTPUT_SUMMARY
    )


@app.route("/m15-short-stage2/top")
def route_top():
    return download_file(
        OUTPUT_TOP
    )


@app.route("/m15-short-stage2/eras")
def route_eras():
    return download_file(
        OUTPUT_ERAS
    )


@app.route("/m15-short-stage2/recent")
def route_recent():
    return download_file(
        OUTPUT_RECENT
    )


@app.route("/m15-short-stage2/best-trades")
def route_best_trades():
    return download_file(
        OUTPUT_TRADES
    )


if __name__ == "__main__":
    research_thread = threading.Thread(
        target=run_research,
        name="eurusd-m15-short-stage2-fast",
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
