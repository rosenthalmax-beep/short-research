
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
# EUR/USD M15 LONG - STAGE 1 DISCOVERY
#
# READ-ONLY RESEARCH. NEVER SENDS ORDERS.
#
# Purpose:
#   Build EUR/USD M15 LONG from scratch.
#
# Signal family:
#   Exact bullish engulfing:
#     previous candle bearish
#     current candle bullish
#     current open <= previous close
#     current close >= previous open
#
# Research sequence in this script:
#   1) Raw engulfing baseline
#   2) Body-ratio sweep
#   3) Body/ATR sweep
#   4) Range/ATR sweep
#   5) Strong-close sweep
#   6) Structure matrix
#   7) RR sweep
#   8) New York hour breakdown
#   9) Weekday breakdown
#
# Cost stress on EVERY candidate:
#   0.50 / 1.00 / 1.50 / 2.00 pips adverse entry.
#
# Historical conventions:
#   - OANDA midpoint M15 candles
#   - ATR14 Wilder/RMA, SMA seeded
#   - stop = signal low - 10 ticks
#   - target based on REFERENCE signal close risk
#   - stressed historical entry = signal close + cost
#   - exits begin NEXT candle
#   - same-bar tie:
#       compare distance from candle open to high vs low
#       high closer => target first
#       otherwise stop first
#   - pyramiding = 0
# ============================================================


app = Flask(__name__)

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_BASE = os.getenv(
    "OANDA_API_URL",
    "https://api-fxtrade.oanda.com"
)

INSTRUMENT = "EUR_USD"
GRANULARITY = "M15"

# Discovery history. Broad enough to span multiple regimes,
# but not so enormous that first-stage iteration becomes painful.
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

# Use 1 pip as the main ranking cost for discovery.
PRIMARY_COST_PIPS = 1.00

OUTPUT_SUMMARY = (
    "eurusd_m15_long_stage1_summary.csv"
)

OUTPUT_TRADES = (
    "eurusd_m15_long_stage1_primary_trades.csv"
)

OUTPUT_HOURS = (
    "eurusd_m15_long_stage1_hour_breakdown.csv"
)

OUTPUT_WEEKDAYS = (
    "eurusd_m15_long_stage1_weekday_breakdown.csv"
)

OUTPUT_TOP = (
    "eurusd_m15_long_stage1_top_candidates.csv"
)

STATUS = {
    "state": "not_started",
    "message": (
        "EUR/USD M15 long discovery has not started"
    ),
    "service": (
        "EURUSD M15 Long Stage 1 Discovery"
    ),
    "orders_supported": False,
    "trading_enabled": False,
}


# ============================================================
# BASIC HELPERS
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
    # OANDA RFC3339 may contain nanoseconds.
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


def safe_float(value):
    return float(
        value
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
            "error": (
                "Output not ready yet"
            ),
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


# ============================================================
# OANDA DATA
# ============================================================

def oanda_headers():
    if not OANDA_TOKEN:
        raise RuntimeError(
            "OANDA_API_TOKEN is not configured"
        )

    return {
        "Authorization": (
            "Bearer "
            + OANDA_TOKEN.strip()
        ),
        "Content-Type": (
            "application/json"
        ),
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
                safe_float(
                    mid["o"]
                ),
            "high":
                safe_float(
                    mid["h"]
                ),
            "low":
                safe_float(
                    mid["l"]
                ),
            "close":
                safe_float(
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
    # 30 days ~= 2880 M15 candles, safely under OANDA's
    # per-request candle limit.
    chunk_days = 30

    cursor = start

    by_time = {}

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
                "Fetching EUR/USD M15 "
                f"{iso_utc(cursor)} "
                f"to {iso_utc(chunk_end)}"
            ),
            "chunk":
                chunk_number,
        })

        candles = fetch_m15_chunk(
            cursor,
            chunk_end,
        )

        for candle in candles:
            by_time[
                candle["time"]
            ] = candle

        cursor = chunk_end

        # Be polite to the API.
        time.sleep(
            0.03
        )

    result = list(
        by_time.values()
    )

    result.sort(
        key=lambda row:
            row["time"]
    )

    return result


# ============================================================
# INDICATORS
# ============================================================

def add_atr14(
    candles,
):
    n = len(
        candles
    )

    true_ranges = [
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
            tr = (
                high - low
            )
        else:
            previous_close = (
                candles[
                    i - 1
                ]["close"]
            )

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

        true_ranges[i] = tr

    atr = [
        None
    ] * n

    if n < 14:
        return atr

    seed = sum(
        true_ranges[
            0:14
        ]
    ) / 14.0

    atr[13] = seed

    for i in range(
        14,
        n
    ):
        atr[i] = (
            (
                atr[
                    i - 1
                ]
                * 13.0
            )
            + true_ranges[i]
        ) / 14.0

    return atr


# ============================================================
# SIGNAL / FILTER LOGIC
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

    previous_bearish = (
        previous["close"]
        < previous["open"]
    )

    current_bullish = (
        current["close"]
        > current["open"]
    )

    body_engulf = (
        current["open"]
        <= previous["close"]
        and
        current["close"]
        >= previous["open"]
    )

    return (
        previous_bearish
        and
        current_bullish
        and
        body_engulf
    )


def signal_features(
    candles,
    atr14,
    i,
):
    current = candles[
        i
    ]

    previous = candles[
        i - 1
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

    body_ratio = (
        current_body
        / previous_body
        if previous_body > 0
        else 999.0
    )

    body_atr = (
        current_body
        / atr
        if atr
        and atr > 0
        else None
    )

    range_atr = (
        candle_range
        / atr
        if atr
        and atr > 0
        else None
    )

    close_location = (
        (
            current["close"]
            - current["low"]
        )
        / candle_range
        if candle_range > 0
        else 0.0
    )

    ny_time = (
        current["time"]
        .astimezone(
            NY
        )
    )

    return {
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
    }


def structure_ok(
    candles,
    atr14,
    i,
    lookback,
    maximum_distance_atr,
):
    if (
        lookback is None
        or maximum_distance_atr is None
    ):
        return True

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

    signal_low = candles[
        i
    ]["low"]

    distance = abs(
        signal_low
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
        features["body_ratio"]
        <
        config.get(
            "minimum_body_ratio",
            1.0,
        )
    ):
        return False

    minimum_body_atr = (
        config.get(
            "minimum_body_atr"
        )
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
        config.get(
            "minimum_range_atr"
        )
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
        config.get(
            "minimum_close_location"
        )
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
        config.get(
            "structure_lookback"
        ),
        config.get(
            "maximum_distance_atr"
        ),
    ):
        return False

    included_hours = (
        config.get(
            "included_ny_hours"
        )
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
        config.get(
            "excluded_weekdays",
            set(),
        )
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

    exit_index = None
    exit_reason = None
    exit_price = None

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

            # Locked long same-bar tie rule:
            # high closer => target first,
            # otherwise stop first.
            if (
                distance_high
                < distance_low
            ):
                exit_index = j
                exit_reason = (
                    "TARGET"
                )
                exit_price = (
                    target
                )
            else:
                exit_index = j
                exit_reason = (
                    "STOP"
                )
                exit_price = (
                    stop
                )

            break

        if hit_target:
            exit_index = j
            exit_reason = (
                "TARGET"
            )
            exit_price = (
                target
            )
            break

        if hit_stop:
            exit_index = j
            exit_reason = (
                "STOP"
            )
            exit_price = (
                stop
            )
            break

    if exit_index is None:
        return None

    result_r = (
        (
            exit_price
            - backtest_entry
        )
        / actual_risk
    )

    return {
        "signal_index":
            signal_index,
        "entry_time_utc":
            iso_utc(
                signal["time"]
            ),
        "exit_time_utc":
            iso_utc(
                candles[
                    exit_index
                ]["time"]
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
        "exit_index":
            exit_index,
    }


def run_config(
    candles,
    atr14,
    config,
    cost_pips,
    include_trade_log=False,
):
    trades = []

    i = 14

    while i < len(
        candles
    ):
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
            features = (
                signal_features(
                    candles,
                    atr14,
                    i,
                )
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

        # Pyramiding = 0:
        # exact exit-candle signal remains eligible,
        # therefore resume at exit_index rather than +1.
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
            trade["result_r"]
        )
        for trade in trades
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
        sum(
            losers
        )
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

    longest_loss_streak = 0
    current_loss_streak = 0

    for result in results:
        equity += result

        peak = max(
            peak,
            equity,
        )

        dd = (
            equity
            - peak
        )

        max_dd = min(
            max_dd,
            dd,
        )

        if result < 0:
            current_loss_streak += 1
            longest_loss_streak = max(
                longest_loss_streak,
                current_loss_streak,
            )
        else:
            current_loss_streak = 0

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
            pf,
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


def candidate_row(
    family,
    config,
    cost_pips,
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
            cost_pips,
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
        "minimum_close_location":
            config.get(
                "minimum_close_location"
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
            config.get(
                "reward_risk"
            ),
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
        "minimum_close_location":
            None,
        "structure_lookback":
            None,
        "maximum_distance_atr":
            None,
        "reward_risk":
            2.50,
        "included_ny_hours":
            None,
        "excluded_weekdays":
            set(),
    }


def discovery_configs():
    configs = []

    baseline = base_config()

    configs.append((
        "BASELINE",
        baseline,
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
        ] = (
            f"BR_{value:.2f}"
        )

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
        ] = (
            f"BODY_ATR_{value:.2f}"
        )

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
        ] = (
            f"RANGE_ATR_{value:.2f}"
        )

        configs.append((
            "RANGE_ATR",
            cfg,
        ))

    for value in [
        0.55,
        0.60,
        0.65,
        0.70,
        0.75,
        0.80,
        0.85,
    ]:
        cfg = base_config()
        cfg[
            "minimum_close_location"
        ] = value
        cfg[
            "label"
        ] = (
            f"STRONG_CLOSE_{value:.2f}"
        )

        configs.append((
            "STRONG_CLOSE",
            cfg,
        ))

    for lookback in [
        10,
        20,
        40,
        60,
        100,
        150,
    ]:
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

    for rr in [
        1.50,
        2.00,
        2.50,
        3.00,
        3.50,
        4.00,
        4.50,
        5.00,
    ]:
        cfg = base_config()

        cfg[
            "reward_risk"
        ] = rr

        cfg[
            "label"
        ] = (
            f"RR_{rr:.2f}"
        )

        configs.append((
            "REWARD_RISK",
            cfg,
        ))

    return configs


# ============================================================
# HOUR / WEEKDAY BREAKDOWNS
# ============================================================

def raw_signal_trade_log(
    candles,
    atr14,
):
    config = base_config()

    config[
        "label"
    ] = (
        "RAW_BASELINE"
    )

    return run_config(
        candles,
        atr14,
        config,
        PRIMARY_COST_PIPS,
        include_trade_log=True,
    )


def grouped_stats(
    trades,
    field,
):
    values = sorted(
        set(
            trade[
                field
            ]
            for trade in trades
        )
    )

    rows = []

    for value in values:
        subset = [
            trade
            for trade in trades
            if trade[
                field
            ] == value
        ]

        stats = stats_from_trades(
            subset
        )

        row = {
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
        }

        rows.append(
            row
        )

    return rows


# ============================================================
# RESEARCH RUNNER
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

        STATUS.update({
            "state":
                "calculating",
            "message":
                "Calculating ATR14 and Stage 1 discovery",
            "candles":
                len(
                    candles
                ),
        })

        atr14 = add_atr14(
            candles
        )

        summary_rows = []

        configs = discovery_configs()

        total_runs = (
            len(
                configs
            )
            * len(
                COST_PIPS_GRID
            )
        )

        run_number = 0

        for family, config in configs:
            for cost_pips in COST_PIPS_GRID:
                run_number += 1

                STATUS.update({
                    "state":
                        "calculating",
                    "message":
                        (
                            f"{family} / "
                            f"{config['label']} / "
                            f"{cost_pips:.2f} pip"
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
                    include_trade_log=False,
                )

                summary_rows.append(
                    candidate_row(
                        family,
                        config,
                        cost_pips,
                        trades,
                    )
                )

        write_csv(
            OUTPUT_SUMMARY,
            summary_rows,
        )

        primary_rows = [
            row
            for row in summary_rows
            if abs(
                float(
                    row[
                        "cost_pips"
                    ]
                )
                - PRIMARY_COST_PIPS
            ) < 1e-12
        ]

        # Avoid tiny-sample junk dominating the first sort.
        eligible = [
            row
            for row in primary_rows
            if int(
                row[
                    "trades"
                ]
            ) >= 100
        ]

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
            ),
            reverse=True,
        )

        top_rows = eligible[
            :30
        ]

        write_csv(
            OUTPUT_TOP,
            top_rows,
        )

        raw_trades = (
            raw_signal_trade_log(
                candles,
                atr14,
            )
        )

        write_csv(
            OUTPUT_TRADES,
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

        baseline_primary = next(
            (
                row
                for row in summary_rows
                if (
                    row[
                        "candidate"
                    ]
                    == "RAW_BASELINE"
                    and
                    abs(
                        float(
                            row[
                                "cost_pips"
                            ]
                        )
                        - PRIMARY_COST_PIPS
                    )
                    < 1e-12
                )
            ),
            None,
        )

        STATUS.update({
            "state":
                "complete",
            "message":
                "EUR/USD M15 Stage 1 discovery complete",
            "candles":
                len(
                    candles
                ),
            "candidate_runs":
                len(
                    summary_rows
                ),
            "primary_cost_pips":
                PRIMARY_COST_PIPS,
            "baseline_primary":
                baseline_primary,
            "top_primary_candidates":
                top_rows[
                    :10
                ],
            "outputs": {
                "summary":
                    OUTPUT_SUMMARY,
                "top":
                    OUTPUT_TOP,
                "raw_trades":
                    OUTPUT_TRADES,
                "hours":
                    OUTPUT_HOURS,
                "weekdays":
                    OUTPUT_WEEKDAYS,
            },
        })

        print()
        print(
            "=" * 100
        )
        print(
            "EUR/USD M15 LONG - STAGE 1 COMPLETE"
        )
        print(
            "=" * 100
        )
        print()
        print(
            "Candles:",
            len(
                candles
            ),
        )
        print(
            "Primary discovery cost:",
            PRIMARY_COST_PIPS,
            "pip",
        )
        print()
        print(
            "Baseline:"
        )
        print(
            baseline_primary
        )
        print()
        print(
            "Top 10 primary-cost candidates:"
        )

        for row in top_rows[
            :10
        ]:
            print(
                row
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
            "EURUSD M15 Long Stage 1 Discovery",
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
            "/m15/status",
            "/m15/summary",
            "/m15/top",
            "/m15/raw-trades",
            "/m15/hours",
            "/m15/weekdays",
        ],
    })


@app.route("/m15/status")
def m15_status():
    return jsonify(
        STATUS
    )


@app.route("/m15/summary")
def m15_summary():
    return download_file(
        OUTPUT_SUMMARY
    )


@app.route("/m15/top")
def m15_top():
    return download_file(
        OUTPUT_TOP
    )


@app.route("/m15/raw-trades")
def m15_raw_trades():
    return download_file(
        OUTPUT_TRADES
    )


@app.route("/m15/hours")
def m15_hours():
    return download_file(
        OUTPUT_HOURS
    )


@app.route("/m15/weekdays")
def m15_weekdays():
    return download_file(
        OUTPUT_WEEKDAYS
    )


if __name__ == "__main__":
    research_thread = threading.Thread(
        target=run_research,
        name="eurusd-m15-long-stage1",
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
