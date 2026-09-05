
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
# EUR/USD M15 SHORT - FAST FINAL ROBUSTNESS SWEEP
#
# READ-ONLY RESEARCH. NEVER SENDS ORDERS.
#
# Stage 2 leader family:
#   exact bearish engulfing
#   body >= ~1.00 ATR14
#   range >= ~1.40 ATR14
#   structure ~60 bars / ~0.30 ATR from prior high
#   NY signal-open hours 02:00, 03:00, 04:00
#   RR ~3.5-4.0
#
# This script only tests the local neighbourhood.
#
# Costs:
#   0.50 / 1.00 / 1.50 / 2.00 pips adverse entry
#
# Primary development cost:
#   1.00 pip
#
# Validation:
#   - full history
#   - 4 eras
#   - DEV 2010-2017 vs VALIDATION 2018-now
#   - recent 5Y / 2Y
#   - monthly rolling 2Y / 3Y
#   - overlap/exclusive comparison for leading finalists
#
# Locked short conventions:
#   - OANDA midpoint M15
#   - exact bearish engulfing
#   - ATR14 Wilder/RMA, SMA seeded
#   - stop = signal high + 10 ticks
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

LOOKBACKS = [
    50,
    60,
    70,
]

DISTANCES = [
    0.25,
    0.30,
    0.35,
]

BODY_ATRS = [
    0.90,
    1.00,
    1.10,
]

RANGE_ATRS = [
    1.30,
    1.40,
    1.50,
    1.60,
]

REWARD_RISKS = [
    3.25,
    3.50,
    3.75,
    4.00,
]

INCLUDED_NY_HOURS = {
    2,
    3,
    4,
}

OUTPUT_SUMMARY = (
    "eurusd_m15_short_final_summary.csv"
)

OUTPUT_TOP = (
    "eurusd_m15_short_final_top.csv"
)

OUTPUT_ERAS = (
    "eurusd_m15_short_final_eras.csv"
)

OUTPUT_DEVVAL = (
    "eurusd_m15_short_final_dev_validation.csv"
)

OUTPUT_RECENT = (
    "eurusd_m15_short_final_recent.csv"
)

OUTPUT_ROLLING = (
    "eurusd_m15_short_final_rolling.csv"
)

OUTPUT_ROLLING_SUMMARY = (
    "eurusd_m15_short_final_rolling_summary.csv"
)

OUTPUT_OVERLAP = (
    "eurusd_m15_short_final_overlap.csv"
)

OUTPUT_TRADES = (
    "eurusd_m15_short_final_best_trades.csv"
)

STATUS = {
    "state": "not_started",
    "message": "EUR/USD M15 short final sweep has not started",
    "service": "EURUSD M15 Short Fast Final Robustness",
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


def month_start(dt):
    return datetime(
        dt.year,
        dt.month,
        1,
        tzinfo=timezone.utc,
    )


def add_months(dt, months):
    total = (
        dt.year * 12
        + dt.month - 1
        + months
    )

    return datetime(
        total // 12,
        total % 12 + 1,
        1,
        tzinfo=timezone.utc,
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
# OANDA
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


def build_signal_cache(
    candles,
    atr14,
):
    signals = []

    max_lookback = max(
        LOOKBACKS
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

        ny_time = (
            current["time"]
            .astimezone(NY)
        )

        structure_distance_atr = {}

        for lookback in LOOKBACKS:
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
            "ny_hour":
                ny_time.hour,
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
        * len(REWARD_RISKS)
        * len(COST_PIPS_GRID)
    )

    done = 0

    for signal in signals:
        signal_index = signal[
            "signal_index"
        ]

        for rr in REWARD_RISKS:
            for cost in COST_PIPS_GRID:
                done += 1

                if done % 1000 == 0:
                    STATUS.update({
                        "state":
                            "precomputing",
                        "message":
                            (
                                "Caching reusable short outcomes "
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
# CONFIGS
# ============================================================

def build_configs():
    configs = []
    counter = 0

    for lookback in LOOKBACKS:
        for distance in DISTANCES:
            for body_atr in BODY_ATRS:
                for range_atr in RANGE_ATRS:
                    for rr in REWARD_RISKS:
                        counter += 1

                        configs.append({
                            "label":
                                (
                                    f"F{counter:03d}_"
                                    f"S{lookback}_"
                                    f"D{distance:.2f}_"
                                    f"BA{body_atr:.2f}_"
                                    f"RA{range_atr:.2f}_"
                                    f"RR{rr:.2f}"
                                ),
                            "structure_lookback":
                                lookback,
                            "maximum_distance_atr":
                                distance,
                            "minimum_body_ratio":
                                1.00,
                            "minimum_body_atr":
                                body_atr,
                            "minimum_range_atr":
                                range_atr,
                            "included_ny_hours":
                                INCLUDED_NY_HOURS,
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

    if (
        signal[
            "ny_hour"
        ]
        not in
        config[
            "included_ny_hours"
        ]
    ):
        return False

    lookback = config[
        "structure_lookback"
    ]

    if (
        signal[
            "structure_distance_atr"
        ][
            lookback
        ]
        >
        config[
            "maximum_distance_atr"
        ]
    ):
        return False

    return True


# ============================================================
# FAST CONFIG EXECUTION
# ============================================================

def run_config_cached(
    signals,
    cache,
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

        trade = cache.get(
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
        "structure_lookback":
            config[
                "structure_lookback"
            ],
        "maximum_distance_atr":
            config[
                "maximum_distance_atr"
            ],
        "minimum_body_atr":
            config[
                "minimum_body_atr"
            ],
        "minimum_range_atr":
            config[
                "minimum_range_atr"
            ],
        "included_ny_hours":
            "2,3,4",
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


def devval_windows():
    return [
        (
            "DEV_2010_2017",
            datetime(
                2010, 1, 1,
                tzinfo=timezone.utc,
            ),
            datetime(
                2018, 1, 1,
                tzinfo=timezone.utc,
            ),
        ),
        (
            "VALIDATION_2018_NOW",
            datetime(
                2018, 1, 1,
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
# MONTHLY ROLLING
# ============================================================

def monthly_rolling_rows(
    signals,
    cache,
    config,
    months,
):
    rows = []

    cursor = month_start(
        RESEARCH_FROM
    )

    last_start = add_months(
        month_start(
            RESEARCH_TO
        ),
        -months,
    )

    while cursor <= last_start:
        end = add_months(
            cursor,
            months,
        )

        if end > RESEARCH_TO:
            break

        trades = run_config_cached(
            signals,
            cache,
            config,
            PRIMARY_COST_PIPS,
            start=cursor,
            end=end,
        )

        stats = stats_from_trades(
            trades
        )

        rows.append({
            "candidate":
                config[
                    "label"
                ],
            "months":
                months,
            "window":
                (
                    f"{cursor:%Y-%m-%d}"
                    " -> "
                    f"{end:%Y-%m-%d}"
                ),
            "trades":
                stats[
                    "trades"
                ],
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
            "positive":
                stats[
                    "total_r"
                ] > 0,
        })

        cursor = add_months(
            cursor,
            1,
        )

    return rows


def median(values):
    ordered = sorted(values)
    n = len(ordered)

    if n == 0:
        return None

    if n % 2:
        return ordered[
            n // 2
        ]

    return (
        ordered[
            n // 2 - 1
        ]
        + ordered[
            n // 2
        ]
    ) / 2.0


def rolling_summary(rows):
    if not rows:
        return {}

    pfs = [
        float(
            row[
                "profit_factor"
            ]
        )
        for row in rows
    ]

    rs = [
        float(
            row[
                "total_r"
            ]
        )
        for row in rows
    ]

    positive = sum(
        1
        for row in rows
        if row[
            "positive"
        ]
    )

    worst_pf_row = min(
        rows,
        key=lambda row:
            float(
                row[
                    "profit_factor"
                ]
            ),
    )

    worst_r_row = min(
        rows,
        key=lambda row:
            float(
                row[
                    "total_r"
                ]
            ),
    )

    return {
        "candidate":
            rows[0][
                "candidate"
            ],
        "months":
            rows[0][
                "months"
            ],
        "windows":
            len(rows),
        "positive_windows":
            positive,
        "positive_windows_pct":
            round(
                positive
                / len(rows)
                * 100.0,
                4,
            ),
        "worst_profit_factor":
            round(
                min(pfs),
                6,
            ),
        "median_profit_factor":
            round(
                median(pfs),
                6,
            ),
        "worst_total_r":
            round(
                min(rs),
                4,
            ),
        "median_total_r":
            round(
                median(rs),
                4,
            ),
        "worst_pf_window":
            worst_pf_row[
                "window"
            ],
        "worst_r_window":
            worst_r_row[
                "window"
            ],
    }


# ============================================================
# OVERLAP
# ============================================================

def trade_key(trade):
    return (
        trade[
            "entry_time_utc"
        ],
        trade[
            "exit_time_utc"
        ],
    )


def overlap_rows(
    signals,
    cache,
    finalists,
):
    if len(finalists) < 2:
        return []

    a = finalists[0]
    b = finalists[1]

    trades_a = run_config_cached(
        signals,
        cache,
        a,
        PRIMARY_COST_PIPS,
    )

    trades_b = run_config_cached(
        signals,
        cache,
        b,
        PRIMARY_COST_PIPS,
    )

    keys_a = {
        trade_key(t)
        for t in trades_a
    }

    keys_b = {
        trade_key(t)
        for t in trades_b
    }

    shared = (
        keys_a & keys_b
    )

    only_a = (
        keys_a - keys_b
    )

    only_b = (
        keys_b - keys_a
    )

    subsets = [
        (
            "A_ALL",
            a[
                "label"
            ],
            trades_a,
        ),
        (
            "B_ALL",
            b[
                "label"
            ],
            trades_b,
        ),
        (
            "SHARED_A_OUTCOMES",
            a[
                "label"
            ],
            [
                t
                for t in trades_a
                if trade_key(t)
                in shared
            ],
        ),
        (
            "A_EXCLUSIVE",
            a[
                "label"
            ],
            [
                t
                for t in trades_a
                if trade_key(t)
                in only_a
            ],
        ),
        (
            "B_EXCLUSIVE",
            b[
                "label"
            ],
            [
                t
                for t in trades_b
                if trade_key(t)
                in only_b
            ],
        ),
    ]

    rows = []

    for label, candidate, trades in subsets:
        stats = stats_from_trades(
            trades
        )

        rows.append({
            "subset":
                label,
            "candidate":
                candidate,
            "trades":
                stats[
                    "trades"
                ],
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
                "Caching reusable short outcomes",
            "engulfing_signals":
                len(signals),
        })

        cache = build_outcome_cache(
            candles,
            signals,
        )

        configs = build_configs()

        STATUS.update({
            "state":
                "calculating",
            "message":
                "Running fast final robustness grid",
            "configs":
                len(configs),
        })

        summary_rows = []

        for number, config in enumerate(
            configs,
            start=1,
        ):
            for cost in COST_PIPS_GRID:
                trades = run_config_cached(
                    signals,
                    cache,
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

            if number % 20 == 0:
                STATUS.update({
                    "state":
                        "calculating",
                    "message":
                        (
                            f"Evaluated {number}/"
                            f"{len(configs)} configs"
                        ),
                    "config":
                        number,
                    "configs_total":
                        len(configs),
                })

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

        top_rows = primary[:20]

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
            for row in top_rows[:10]
        ]

        era_rows = validation_rows(
            signals,
            cache,
            finalists,
            era_windows(),
        )

        devval_rows = validation_rows(
            signals,
            cache,
            finalists,
            devval_windows(),
        )

        recent_rows = validation_rows(
            signals,
            cache,
            finalists,
            recent_windows(),
        )

        write_csv(
            OUTPUT_ERAS,
            era_rows,
        )

        write_csv(
            OUTPUT_DEVVAL,
            devval_rows,
        )

        write_csv(
            OUTPUT_RECENT,
            recent_rows,
        )

        robust_scores = []

        for config in finalists:
            label = config[
                "label"
            ]

            base = next(
                row
                for row in top_rows
                if row[
                    "candidate"
                ] == label
            )

            eras = [
                row
                for row in era_rows
                if row[
                    "candidate"
                ] == label
            ]

            devval = [
                row
                for row in devval_rows
                if row[
                    "candidate"
                ] == label
            ]

            recent = [
                row
                for row in recent_rows
                if row[
                    "candidate"
                ] == label
            ]

            era_pfs = [
                float(
                    row[
                        "profit_factor"
                    ]
                )
                for row in eras
                if int(
                    row[
                        "trades"
                    ]
                ) > 0
            ]

            devval_pfs = [
                float(
                    row[
                        "profit_factor"
                    ]
                )
                for row in devval
                if int(
                    row[
                        "trades"
                    ]
                ) > 0
            ]

            recent_pfs = [
                float(
                    row[
                        "profit_factor"
                    ]
                )
                for row in recent
                if int(
                    row[
                        "trades"
                    ]
                ) > 0
            ]

            min_era_pf = (
                min(era_pfs)
                if era_pfs
                else 0.0
            )

            min_devval_pf = (
                min(devval_pfs)
                if devval_pfs
                else 0.0
            )

            min_recent_pf = (
                min(recent_pfs)
                if recent_pfs
                else 0.0
            )

            robust_scores.append({
                "candidate":
                    label,
                "full_pf":
                    float(
                        base[
                            "profit_factor"
                        ]
                    ),
                "full_expectancy":
                    float(
                        base[
                            "expectancy_r"
                        ]
                    ),
                "full_total_r":
                    float(
                        base[
                            "total_r"
                        ]
                    ),
                "minimum_era_pf":
                    min_era_pf,
                "minimum_devval_pf":
                    min_devval_pf,
                "minimum_recent_pf":
                    min_recent_pf,
                "score":
                    (
                        min_era_pf * 3.0
                        + min_devval_pf * 2.0
                        + min_recent_pf * 2.0
                        + float(
                            base[
                                "profit_factor"
                            ]
                        )
                    ),
            })

        robust_scores.sort(
            key=lambda row:
                row[
                    "score"
                ],
            reverse=True,
        )

        robust_finalists = [
            config_map[
                row[
                    "candidate"
                ]
            ]
            for row in robust_scores[:3]
        ]

        rolling_rows = []
        rolling_summary_rows = []

        for config in robust_finalists:
            for months in [
                24,
                36,
            ]:
                rows = monthly_rolling_rows(
                    signals,
                    cache,
                    config,
                    months,
                )

                rolling_rows.extend(
                    rows
                )

                summary = rolling_summary(
                    rows
                )

                summary[
                    "structure_lookback"
                ] = config[
                    "structure_lookback"
                ]

                summary[
                    "maximum_distance_atr"
                ] = config[
                    "maximum_distance_atr"
                ]

                summary[
                    "minimum_body_atr"
                ] = config[
                    "minimum_body_atr"
                ]

                summary[
                    "minimum_range_atr"
                ] = config[
                    "minimum_range_atr"
                ]

                summary[
                    "reward_risk"
                ] = config[
                    "reward_risk"
                ]

                rolling_summary_rows.append(
                    summary
                )

        write_csv(
            OUTPUT_ROLLING,
            rolling_rows,
        )

        write_csv(
            OUTPUT_ROLLING_SUMMARY,
            rolling_summary_rows,
        )

        overlap = overlap_rows(
            signals,
            cache,
            robust_finalists,
        )

        write_csv(
            OUTPUT_OVERLAP,
            overlap,
        )

        best_config = (
            robust_finalists[0]
            if robust_finalists
            else finalists[0]
        )

        best_trades = run_config_cached(
            signals,
            cache,
            best_config,
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
                "EUR/USD M15 short final robustness complete",
            "candles":
                len(candles),
            "engulfing_signals":
                len(signals),
            "configs":
                len(configs),
            "primary_cost_pips":
                PRIMARY_COST_PIPS,
            "robust_ranking":
                robust_scores[:10],
            "selected_best":
                best_config,
            "outputs": {
                "summary":
                    OUTPUT_SUMMARY,
                "top":
                    OUTPUT_TOP,
                "eras":
                    OUTPUT_ERAS,
                "dev_validation":
                    OUTPUT_DEVVAL,
                "recent":
                    OUTPUT_RECENT,
                "rolling":
                    OUTPUT_ROLLING,
                "rolling_summary":
                    OUTPUT_ROLLING_SUMMARY,
                "overlap":
                    OUTPUT_OVERLAP,
                "best_trades":
                    OUTPUT_TRADES,
            },
        })

        print()
        print("=" * 100)
        print(
            "EUR/USD M15 SHORT FINAL ROBUSTNESS COMPLETE"
        )
        print("=" * 100)
        print(
            "Configs:",
            len(configs),
        )
        print(
            "Selected best:",
            best_config,
        )
        print(
            "Robust ranking:"
        )

        for row in robust_scores[:10]:
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
            "EURUSD M15 Short Fast Final Robustness",
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
            "/m15-short-final/status",
            "/m15-short-final/summary",
            "/m15-short-final/top",
            "/m15-short-final/eras",
            "/m15-short-final/dev-validation",
            "/m15-short-final/recent",
            "/m15-short-final/rolling",
            "/m15-short-final/rolling-summary",
            "/m15-short-final/overlap",
            "/m15-short-final/best-trades",
        ],
    })


@app.route("/m15-short-final/status")
def route_status():
    return jsonify(
        STATUS
    )


@app.route("/m15-short-final/summary")
def route_summary():
    return download_file(
        OUTPUT_SUMMARY
    )


@app.route("/m15-short-final/top")
def route_top():
    return download_file(
        OUTPUT_TOP
    )


@app.route("/m15-short-final/eras")
def route_eras():
    return download_file(
        OUTPUT_ERAS
    )


@app.route("/m15-short-final/dev-validation")
def route_devval():
    return download_file(
        OUTPUT_DEVVAL
    )


@app.route("/m15-short-final/recent")
def route_recent():
    return download_file(
        OUTPUT_RECENT
    )


@app.route("/m15-short-final/rolling")
def route_rolling():
    return download_file(
        OUTPUT_ROLLING
    )


@app.route("/m15-short-final/rolling-summary")
def route_rolling_summary():
    return download_file(
        OUTPUT_ROLLING_SUMMARY
    )


@app.route("/m15-short-final/overlap")
def route_overlap():
    return download_file(
        OUTPUT_OVERLAP
    )


@app.route("/m15-short-final/best-trades")
def route_best_trades():
    return download_file(
        OUTPUT_TRADES
    )


if __name__ == "__main__":
    research_thread = threading.Thread(
        target=run_research,
        name="eurusd-m15-short-final-fast",
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
