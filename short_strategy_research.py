
import os
import csv
import math
import time
import threading
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from flask import Flask, jsonify, send_file


# ============================================================
# EUR/USD M15 LONG - STAGE 3 TIGHT ROBUSTNESS SWEEP
#
# READ-ONLY RESEARCH. NEVER SENDS ORDERS.
#
# Built from Stage 1 evidence:
#   - raw engulfing was negative
#   - structure near prior lows was the strongest single factor
#   - certain NY hours were materially less bad
#
# Stage 2 purpose:
#   Test controlled interactions around:
#     structure lookback
#     structure distance
#     NY session/hour sets
#     body/ATR
#     range/ATR
#     strong close
#     body ratio
#     RR
#
# Every candidate is tested at:
#   0.50 / 1.00 / 1.50 / 2.00 pips adverse entry cost
#
# Primary ranking cost:
#   1.00 pip
#
# Validation:
#   full history
#   era splits
#   recent 5Y
#   recent 2Y
#
# Historical conventions:
#   - OANDA midpoint M15
#   - exact bullish engulfing
#   - ATR14 Wilder/RMA, SMA seeded
#   - stop = signal low - 10 ticks
#   - target based on REFERENCE signal close risk
#   - adverse historical entry = close + cost
#   - exits begin NEXT candle
#   - same-bar tie:
#       high closer => target first
#       otherwise stop first
#   - pyramiding 0
#   - exit-candle signal eligible
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

NY = ZoneInfo(
    "America/New_York"
)

TICK_SIZE = 0.00001
PIP_SIZE = 0.0001
STOP_BUFFER_TICKS = 10

COST_PIPS_GRID = [
    0.50,
    1.00,
    1.50,
    2.00,
]

PRIMARY_COST_PIPS = 1.00

OUTPUT_SUMMARY = (
    "eurusd_m15_long_stage3_summary.csv"
)

OUTPUT_PRIMARY = (
    "eurusd_m15_long_stage3_primary.csv"
)

OUTPUT_TOP = (
    "eurusd_m15_long_stage3_top_candidates.csv"
)

OUTPUT_ERAS = (
    "eurusd_m15_long_stage3_era_validation.csv"
)

OUTPUT_RECENT = (
    "eurusd_m15_long_stage3_recent_validation.csv"
)

OUTPUT_TRADES = (
    "eurusd_m15_long_stage3_best_trade_log.csv"
)

STATUS = {
    "state": "not_started",
    "message": (
        "EUR/USD M15 Stage 2 has not started"
    ),
    "service": (
        "EURUSD M15 Long Stage 3 Tight Robustness Sweep"
    ),
    "orders_supported": False,
    "trading_enabled": False,
}


# ============================================================
# HELPERS
# ============================================================

def iso_utc(dt):
    return (
        dt.astimezone(
            timezone.utc
        )
        .isoformat()
        .replace(
            "+00:00",
            "Z"
        )
    )


def parse_oanda_time(value):
    if value.endswith("Z"):
        value = (
            value[:-1]
            + "+00:00"
        )

    if "." in value:
        left, right = value.split(
            ".",
            1,
        )

        if "+" in right:
            fraction, offset = right.split(
                "+",
                1,
            )

            fraction = (
                fraction[:6]
                .ljust(
                    6,
                    "0",
                )
            )

            value = (
                left
                + "."
                + fraction
                + "+"
                + offset
            )

    return datetime.fromisoformat(
        value
    ).astimezone(
        timezone.utc
    )


def write_csv(
    path,
    rows,
):
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
        for key in row.keys():
            if key not in seen:
                seen.add(
                    key
                )
                fieldnames.append(
                    key
                )

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

        for row in rows:
            writer.writerow(
                row
            )


def download_file(path):
    if not os.path.exists(
        path
    ):
        return jsonify({
            "error": "Output not ready yet",
            "path": path,
        }), 404

    return send_file(
        os.path.abspath(
            path
        ),
        as_attachment=True,
        download_name=os.path.basename(
            path
        ),
    )


def years_ago_safe(
    dt,
    years,
):
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
            "Bearer "
            + OANDA_TOKEN.strip(),
        "Content-Type":
            "application/json",
    }


def fetch_m15_chunk(
    start,
    end,
):
    url = (
        f"{OANDA_BASE}"
        f"/v3/instruments/"
        f"{INSTRUMENT}/candles"
    )

    params = {
        "price": "M",
        "granularity": GRANULARITY,
        "smooth": "false",
        "from": iso_utc(
            start
        ),
        "to": iso_utc(
            end
        ),
        "includeFirst": "true",
    }

    response = requests.get(
        url,
        headers=oanda_headers(),
        params=params,
        timeout=60,
    )

    response.raise_for_status()

    payload = response.json()

    candles = []

    for item in payload.get(
        "candles",
        []
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
                float(
                    mid["o"]
                ),
            "high":
                float(
                    mid["h"]
                ),
            "low":
                float(
                    mid["l"]
                ),
            "close":
                float(
                    mid["c"]
                ),
            "volume":
                int(
                    item.get(
                        "volume",
                        0,
                    )
                ),
        })

    return candles


def fetch_full_m15(
    start,
    end,
):
    cursor = start
    by_time = {}
    chunk_days = 30
    chunk_number = 0

    while cursor < end:
        chunk_number += 1

        chunk_end = min(
            cursor
            + timedelta(
                days=chunk_days
            ),
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

        chunk = fetch_m15_chunk(
            cursor,
            chunk_end,
        )

        for candle in chunk:
            by_time[
                candle["time"]
            ] = candle

        cursor = chunk_end

        time.sleep(
            0.03
        )

    candles = list(
        by_time.values()
    )

    candles.sort(
        key=lambda row:
            row["time"]
    )

    return candles


# ============================================================
# INDICATORS
# ============================================================

def add_atr14(
    candles,
):
    n = len(
        candles
    )

    trs = [
        None
    ] * n

    for i in range(
        n
    ):
        high = candles[i][
            "high"
        ]

        low = candles[i][
            "low"
        ]

        if i == 0:
            tr = high - low
        else:
            previous_close = candles[
                i - 1
            ]["close"]

            tr = max(
                high - low,
                abs(
                    high
                    - previous_close
                ),
                abs(
                    low
                    - previous_close
                ),
            )

        trs[i] = tr

    atr = [
        None
    ] * n

    if n < 14:
        return atr

    atr[13] = (
        sum(
            trs[
                0:14
            ]
        )
        / 14.0
    )

    for i in range(
        14,
        n
    ):
        atr[i] = (
            atr[
                i - 1
            ]
            * 13.0
            + trs[i]
        ) / 14.0

    return atr


# ============================================================
# SIGNAL FEATURES
# ============================================================

def bullish_engulfing(
    candles,
    i,
):
    if i < 1:
        return False

    previous = candles[
        i - 1
    ]

    current = candles[
        i
    ]

    return (
        previous["close"]
        < previous["open"]
        and
        current["close"]
        > current["open"]
        and
        current["open"]
        <= previous["close"]
        and
        current["close"]
        >= previous["open"]
    )


def signal_features(
    candles,
    atr14,
    i,
):
    previous = candles[
        i - 1
    ]

    current = candles[
        i
    ]

    current_body = (
        current["close"]
        - current["open"]
    )

    previous_body = abs(
        previous["close"]
        - previous["open"]
    )

    candle_range = (
        current["high"]
        - current["low"]
    )

    atr = atr14[
        i
    ]

    ny_time = (
        current["time"]
        .astimezone(
            NY
        )
    )

    return {
        "body_ratio":
            (
                current_body
                / previous_body
                if previous_body > 0
                else 999.0
            ),
        "body_atr":
            (
                current_body
                / atr
                if atr
                and atr > 0
                else None
            ),
        "range_atr":
            (
                candle_range
                / atr
                if atr
                and atr > 0
                else None
            ),
        "close_location":
            (
                (
                    current["close"]
                    - current["low"]
                )
                / candle_range
                if candle_range > 0
                else 0.0
            ),
        "ny_hour":
            ny_time.hour,
        "ny_weekday":
            ny_time.weekday(),
    }


def structure_ok(
    candles,
    atr14,
    i,
    lookback,
    maximum_distance_atr,
):
    if i < lookback:
        return False

    atr = atr14[
        i
    ]

    if (
        atr is None
        or atr <= 0
    ):
        return False

    previous_low = min(
        candle["low"]
        for candle in candles[
            i - lookback:i
        ]
    )

    distance = abs(
        candles[
            i
        ]["low"]
        - previous_low
    )

    return (
        distance
        <= maximum_distance_atr
        * atr
    )


def passes_config(
    candles,
    atr14,
    i,
    config,
):
    if not bullish_engulfing(
        candles,
        i,
    ):
        return False

    features = signal_features(
        candles,
        atr14,
        i,
    )

    if (
        features[
            "body_ratio"
        ]
        <
        config[
            "minimum_body_ratio"
        ]
    ):
        return False

    minimum_body_atr = (
        config[
            "minimum_body_atr"
        ]
    )

    if (
        minimum_body_atr
        is not None
        and
        (
            features[
                "body_atr"
            ]
            is None
            or
            features[
                "body_atr"
            ]
            < minimum_body_atr
        )
    ):
        return False

    minimum_range_atr = (
        config[
            "minimum_range_atr"
        ]
    )

    if (
        minimum_range_atr
        is not None
        and
        (
            features[
                "range_atr"
            ]
            is None
            or
            features[
                "range_atr"
            ]
            < minimum_range_atr
        )
    ):
        return False

    minimum_close_location = (
        config[
            "minimum_close_location"
        ]
    )

    if (
        minimum_close_location
        is not None
        and
        features[
            "close_location"
        ]
        <
        minimum_close_location
    ):
        return False

    if not structure_ok(
        candles,
        atr14,
        i,
        config[
            "structure_lookback"
        ],
        config[
            "maximum_distance_atr"
        ],
    ):
        return False

    included_hours = (
        config[
            "included_ny_hours"
        ]
    )

    if (
        included_hours is not None
        and
        features[
            "ny_hour"
        ]
        not in included_hours
    ):
        return False

    excluded_weekdays = (
        config[
            "excluded_weekdays"
        ]
    )

    if (
        features[
            "ny_weekday"
        ]
        in excluded_weekdays
    ):
        return False

    return True


# ============================================================
# TRADE SIMULATION
# ============================================================

def simulate_trade(
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
        signal["low"]
        - STOP_BUFFER_TICKS
        * TICK_SIZE
    )

    reference_risk = (
        reference_entry
        - stop
    )

    if reference_risk <= 0:
        return None

    target = (
        reference_entry
        + reward_risk
        * reference_risk
    )

    backtest_entry = (
        reference_entry
        + cost_pips
        * PIP_SIZE
    )

    actual_risk = (
        backtest_entry
        - stop
    )

    if actual_risk <= 0:
        return None

    for j in range(
        signal_index + 1,
        len(candles),
    ):
        candle = candles[
            j
        ]

        hit_stop = (
            candle["low"]
            <= stop
        )

        hit_target = (
            candle["high"]
            >= target
        )

        exit_reason = None
        exit_price = None

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

            if (
                distance_high
                < distance_low
            ):
                exit_reason = (
                    "TARGET"
                )

                exit_price = (
                    target
                )
            else:
                exit_reason = (
                    "STOP"
                )

                exit_price = (
                    stop
                )

        elif hit_target:
            exit_reason = (
                "TARGET"
            )

            exit_price = (
                target
            )

        elif hit_stop:
            exit_reason = (
                "STOP"
            )

            exit_price = (
                stop
            )

        if exit_reason is not None:
            result_r = (
                exit_price
                - backtest_entry
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


def run_config(
    candles,
    atr14,
    config,
    cost_pips,
    start=None,
    end=None,
    include_trade_log=False,
):
    trades = []

    i = max(
        14,
        int(
            config[
                "structure_lookback"
            ]
        ),
    )

    while i < len(
        candles
    ):
        candle_time = candles[
            i
        ]["time"]

        if (
            start is not None
            and candle_time < start
        ):
            i += 1
            continue

        if (
            end is not None
            and candle_time >= end
        ):
            break

        if not passes_config(
            candles,
            atr14,
            i,
            config,
        ):
            i += 1
            continue

        trade = simulate_trade(
            candles,
            i,
            config[
                "reward_risk"
            ],
            cost_pips,
        )

        if trade is None:
            i += 1
            continue

        if include_trade_log:
            features = signal_features(
                candles,
                atr14,
                i,
            )

            row = dict(
                trade
            )

            row.update({
                "instrument":
                    INSTRUMENT,
                "side":
                    "BUY",
                "timeframe":
                    GRANULARITY,
                "candidate":
                    config[
                        "label"
                    ],
                "body_ratio":
                    features[
                        "body_ratio"
                    ],
                "body_atr":
                    features[
                        "body_atr"
                    ],
                "range_atr":
                    features[
                        "range_atr"
                    ],
                "close_location":
                    features[
                        "close_location"
                    ],
                "ny_hour":
                    features[
                        "ny_hour"
                    ],
                "ny_weekday":
                    features[
                        "ny_weekday"
                    ],
            })

            trades.append(
                row
            )
        else:
            trades.append(
                trade
            )

        # exact exit-candle signal allowed
        i = trade[
            "exit_index"
        ]

    return trades


# ============================================================
# STATS
# ============================================================

def stats_from_trades(
    trades
):
    results = [
        float(
            trade[
                "result_r"
            ]
        )
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

    gross_profit = sum(
        winners
    )

    gross_loss = abs(
        sum(
            losers
        )
    )

    profit_factor = (
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

    loss_streak = 0
    longest_loss_streak = 0

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
            loss_streak += 1
            longest_loss_streak = max(
                longest_loss_streak,
                loss_streak,
            )
        else:
            loss_streak = 0

    return {
        "trades":
            len(
                results
            ),
        "winners":
            len(
                winners
            ),
        "losers":
            len(
                losers
            ),
        "win_rate":
            (
                len(
                    winners
                )
                / len(
                    results
                )
                * 100.0
                if results
                else 0.0
            ),
        "profit_factor":
            profit_factor,
        "total_r":
            total_r,
        "expectancy_r":
            (
                total_r
                / len(
                    results
                )
                if results
                else 0.0
            ),
        "max_drawdown_r":
            max_dd,
        "longest_loss_streak":
            longest_loss_streak,
    }


def make_row(
    config,
    cost_pips,
    trades,
):
    stats = stats_from_trades(
        trades
    )

    hours_text = (
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
    )

    weekdays_text = ",".join(
        str(day)
        for day in sorted(
            config[
                "excluded_weekdays"
            ]
        )
    )

    return {
        "candidate":
            config[
                "label"
            ],
        "cost_pips":
            cost_pips,
        "structure_lookback":
            config[
                "structure_lookback"
            ],
        "maximum_distance_atr":
            config[
                "maximum_distance_atr"
            ],
        "minimum_body_ratio":
            config[
                "minimum_body_ratio"
            ],
        "minimum_body_atr":
            config[
                "minimum_body_atr"
            ],
        "minimum_range_atr":
            config[
                "minimum_range_atr"
            ],
        "minimum_close_location":
            config[
                "minimum_close_location"
            ],
        "included_ny_hours":
            hours_text,
        "excluded_weekdays":
            weekdays_text,
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
# CONTROLLED STAGE 2 CANDIDATE GENERATION
# ============================================================

def stage2_configs():
    # Stage 3 deliberately stays in the neighbourhood of the
    # strongest Stage 2 family:
    #
    #   structure ~150
    #   distance ~0.10 ATR
    #   body >= ~0.80 ATR
    #   all NY hours
    #
    # No new filters are introduced here.
    configs = []

    structure_lookbacks = [
        120,
        135,
        150,
        165,
        180,
    ]

    structure_distances = [
        0.075,
        0.100,
        0.125,
        0.150,
    ]

    body_atrs = [
        0.70,
        0.75,
        0.80,
        0.85,
        0.90,
    ]

    reward_risks = [
        2.50,
        2.75,
        3.00,
        3.25,
        3.50,
        3.75,
        4.00,
        4.25,
    ]

    counter = 0

    for lookback in structure_lookbacks:
        for distance in structure_distances:
            for body_atr in body_atrs:
                for rr in reward_risks:
                    counter += 1

                    config = {
                        "label":
                            (
                                f"R{counter:04d}_"
                                f"S{lookback}_"
                                f"D{distance:.3f}_"
                                f"BA{body_atr:.2f}_"
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
                            None,
                        "minimum_close_location":
                            None,
                        "included_ny_hours":
                            None,
                        "excluded_weekdays":
                            set(),
                        "reward_risk":
                            rr,
                    }

                    configs.append(
                        config
                    )

    return configs


# ============================================================
# ERA / RECENT VALIDATION
# ============================================================

def validation_windows():
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


def validation_row(
    label,
    config,
    trades,
):
    stats = stats_from_trades(
        trades
    )

    return {
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
    }


# ============================================================
# MONTHLY ROLLING VALIDATION
# ============================================================

OUTPUT_ROLLING = (
    "eurusd_m15_long_stage3_rolling_validation.csv"
)

OUTPUT_ROLLING_SUMMARY = (
    "eurusd_m15_long_stage3_rolling_summary.csv"
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
        + (dt.month - 1)
        + months
    )

    year = total // 12
    month = (
        total % 12
        + 1
    )

    return datetime(
        year,
        month,
        1,
        tzinfo=timezone.utc,
    )


def monthly_rolling_rows(
    candles,
    atr14,
    config,
    months,
):
    rows = []

    cursor = month_start(
        RESEARCH_FROM
    )

    if cursor < RESEARCH_FROM:
        cursor = add_months(
            cursor,
            1,
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

        trades = run_config(
            candles,
            atr14,
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
            "start_utc":
                iso_utc(
                    cursor
                ),
            "end_utc":
                iso_utc(
                    end
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
                (
                    stats[
                        "total_r"
                    ] > 0
                ),
        })

        cursor = add_months(
            cursor,
            1,
        )

    return rows


def rolling_summary_row(
    candidate,
    months,
    rows,
):
    if not rows:
        return {
            "candidate":
                candidate,
            "months":
                months,
            "windows":
                0,
        }

    pfs = [
        float(
            row[
                "profit_factor"
            ]
        )
        for row in rows
    ]

    total_rs = [
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
            candidate,
        "months":
            months,
        "windows":
            len(
                rows
            ),
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
                min(
                    pfs
                ),
                6,
            ),
        "median_profit_factor":
            round(
                sorted(
                    pfs
                )[
                    len(pfs) // 2
                ],
                6,
            ),
        "worst_total_r":
            round(
                min(
                    total_rs
                ),
                4,
            ),
        "median_total_r":
            round(
                sorted(
                    total_rs
                )[
                    len(
                        total_rs
                    ) // 2
                ],
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

        candles = fetch_full_m15(
            RESEARCH_FROM,
            RESEARCH_TO,
        )

        if len(
            candles
        ) < 1000:
            raise RuntimeError(
                "Too few M15 candles returned"
            )

        atr14 = add_atr14(
            candles
        )

        configs = stage2_configs()

        STATUS.update({
            "state":
                "calculating",
            "message":
                "Running Stage 3 tight robustness sweep",
            "candles":
                len(
                    candles
                ),
            "candidates":
                len(
                    configs
                ),
        })

        summary_rows = []
        primary_rows = []

        total_runs = (
            len(
                configs
            )
            * len(
                COST_PIPS_GRID
            )
        )

        run_number = 0

        for config in configs:
            for cost_pips in COST_PIPS_GRID:
                run_number += 1

                STATUS.update({
                    "state":
                        "calculating",
                    "message":
                        (
                            f"{config['label']} "
                            f"at {cost_pips:.2f} pip"
                        ),
                    "run":
                        run_number,
                    "runs_total":
                        total_runs,
                })

                trades = run_config(
                    candles,
                    atr14,
                    config,
                    cost_pips,
                )

                row = make_row(
                    config,
                    cost_pips,
                    trades,
                )

                summary_rows.append(
                    row
                )

                if abs(
                    cost_pips
                    - PRIMARY_COST_PIPS
                ) < 1e-12:
                    primary_rows.append(
                        row
                    )

        write_csv(
            OUTPUT_SUMMARY,
            summary_rows,
        )

        write_csv(
            OUTPUT_PRIMARY,
            primary_rows,
        )

        # Require enough trades to be credible at this stage.
        eligible = [
            row
            for row in primary_rows
            if int(
                row[
                    "trades"
                ]
            ) >= 120
        ]

        # Score robustness-oriented rather than pure PF.
        # PF and expectancy dominate, with total R as a tie-breaker.
        eligible.sort(
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
                int(
                    row[
                        "trades"
                    ]
                ),
            ),
            reverse=True,
        )

        top_rows = eligible[
            :40
        ]

        write_csv(
            OUTPUT_TOP,
            top_rows,
        )

        # Map label back to config.
        config_by_label = {
            config[
                "label"
            ]:
                config
            for config in configs
        }

        # Validate top 20 across eras and recent windows.
        era_rows = []
        recent_rows = []

        for rank, top_row in enumerate(
            top_rows[
                :20
            ],
            start=1,
        ):
            config = config_by_label[
                top_row[
                    "candidate"
                ]
            ]

            for (
                label,
                start,
                end,
            ) in validation_windows():
                trades = run_config(
                    candles,
                    atr14,
                    config,
                    PRIMARY_COST_PIPS,
                    start=start,
                    end=end,
                )

                row = validation_row(
                    label,
                    config,
                    trades,
                )

                row[
                    "rank"
                ] = rank

                era_rows.append(
                    row
                )

            for (
                label,
                start,
                end,
            ) in recent_windows():
                trades = run_config(
                    candles,
                    atr14,
                    config,
                    PRIMARY_COST_PIPS,
                    start=start,
                    end=end,
                )

                row = validation_row(
                    label,
                    config,
                    trades,
                )

                row[
                    "rank"
                ] = rank

                recent_rows.append(
                    row
                )

        write_csv(
            OUTPUT_ERAS,
            era_rows,
        )

        write_csv(
            OUTPUT_RECENT,
            recent_rows,
        )


        # Monthly rolling validation on the top 10 only.
        # This is deliberately narrower because each candidate
        # is evaluated over every possible month-start 2Y/3Y window.
        rolling_rows = []
        rolling_summary_rows = []

        for top_row in top_rows[:10]:
            config = config_by_label[
                top_row[
                    "candidate"
                ]
            ]

            for months in [
                24,
                36,
            ]:
                rows = monthly_rolling_rows(
                    candles,
                    atr14,
                    config,
                    months,
                )

                rolling_rows.extend(
                    rows
                )

                rolling_summary_rows.append(
                    rolling_summary_row(
                        config[
                            "label"
                        ],
                        months,
                        rows,
                    )
                )

        write_csv(
            OUTPUT_ROLLING,
            rolling_rows,
        )

        write_csv(
            OUTPUT_ROLLING_SUMMARY,
            rolling_summary_rows,
        )

        # Pick a provisional best candidate using:
        #  - top primary ranking
        #  - at least 3/4 positive-PF eras
        #  - recent 5Y and 2Y PF > 1
        provisional = None

        for top_row in top_rows:
            label = top_row[
                "candidate"
            ]

            candidate_eras = [
                row
                for row in era_rows
                if row[
                    "candidate"
                ] == label
            ]

            candidate_recent = [
                row
                for row in recent_rows
                if row[
                    "candidate"
                ] == label
            ]

            positive_eras = sum(
                1
                for row in candidate_eras
                if (
                    int(
                        row[
                            "trades"
                        ]
                    ) >= 20
                    and
                    float(
                        row[
                            "profit_factor"
                        ]
                    ) > 1.0
                )
            )

            recent_ok = (
                len(
                    candidate_recent
                ) == 2
                and
                all(
                    int(
                        row[
                            "trades"
                        ]
                    ) >= 20
                    and
                    float(
                        row[
                            "profit_factor"
                        ]
                    ) > 1.0
                    for row in candidate_recent
                )
            )

            if (
                positive_eras >= 3
                and recent_ok
            ):
                provisional = (
                    top_row
                )
                break

        if (
            provisional is None
            and top_rows
        ):
            provisional = (
                top_rows[0]
            )

        best_trade_log = []

        if provisional is not None:
            best_config = config_by_label[
                provisional[
                    "candidate"
                ]
            ]

            best_trade_log = run_config(
                candles,
                atr14,
                best_config,
                PRIMARY_COST_PIPS,
                include_trade_log=True,
            )

        write_csv(
            OUTPUT_TRADES,
            best_trade_log,
        )

        STATUS.update({
            "state":
                "complete",
            "message":
                "EUR/USD M15 Stage 3 complete",
            "candles":
                len(
                    candles
                ),
            "candidates":
                len(
                    configs
                ),
            "candidate_runs":
                len(
                    summary_rows
                ),
            "primary_cost_pips":
                PRIMARY_COST_PIPS,
            "provisional_best":
                provisional,
            "outputs": {
                "summary":
                    OUTPUT_SUMMARY,
                "primary":
                    OUTPUT_PRIMARY,
                "top":
                    OUTPUT_TOP,
                "eras":
                    OUTPUT_ERAS,
                "recent":
                    OUTPUT_RECENT,
                "best_trades":
                    OUTPUT_TRADES,
                "rolling":
                    OUTPUT_ROLLING,
                "rolling_summary":
                    OUTPUT_ROLLING_SUMMARY,
            },
        })

        print()
        print(
            "=" * 100
        )
        print(
            "EUR/USD M15 LONG STAGE 3 COMPLETE"
        )
        print(
            "=" * 100
        )
        print(
            "Candidates:",
            len(
                configs
            ),
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
                str(
                    error
                ),
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
            "EURUSD M15 Long Stage 3 Tight Robustness Sweep",
        "status":
            STATUS[
                "state"
            ],
        "instrument":
            INSTRUMENT,
        "timeframe":
            GRANULARITY,
        "side":
            "BUY",
        "orders_supported":
            False,
        "trading_enabled":
            False,
        "routes": [
            "/m15-stage3/status",
            "/m15-stage3/summary",
            "/m15-stage3/primary",
            "/m15-stage3/top",
            "/m15-stage3/eras",
            "/m15-stage3/recent",
            "/m15-stage3/best-trades",
            "/m15-stage3/rolling",
            "/m15-stage3/rolling-summary",
        ],
    })


@app.route(
    "/m15-stage3/status"
)
def stage2_status():
    return jsonify(
        STATUS
    )


@app.route(
    "/m15-stage3/summary"
)
def stage2_summary():
    return download_file(
        OUTPUT_SUMMARY
    )


@app.route(
    "/m15-stage3/primary"
)
def stage2_primary():
    return download_file(
        OUTPUT_PRIMARY
    )


@app.route(
    "/m15-stage3/top"
)
def stage2_top():
    return download_file(
        OUTPUT_TOP
    )


@app.route(
    "/m15-stage3/eras"
)
def stage2_eras():
    return download_file(
        OUTPUT_ERAS
    )


@app.route(
    "/m15-stage3/recent"
)
def stage2_recent():
    return download_file(
        OUTPUT_RECENT
    )


@app.route(
    "/m15-stage3/best-trades"
)
def stage2_best_trades():
    return download_file(
        OUTPUT_TRADES
    )


@app.route(
    "/m15-stage3/rolling"
)
def stage3_rolling():
    return download_file(
        OUTPUT_ROLLING
    )


@app.route(
    "/m15-stage3/rolling-summary"
)
def stage3_rolling_summary():
    return download_file(
        OUTPUT_ROLLING_SUMMARY
    )


if __name__ == "__main__":
    research_thread = threading.Thread(
        target=run_research,
        name="eurusd-m15-long-stage3",
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
