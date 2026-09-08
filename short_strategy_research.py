
import os
import csv
import time
import bisect
import zipfile
import threading
from copy import deepcopy
from collections import deque, defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median
from zoneinfo import ZoneInfo

import numpy as np
import requests
from flask import Flask, jsonify, send_file


# ============================================================
# EUR/USD M15 LONG — FINALIST CONFIRMATION
#
# PURPOSE
# -------
# Confirm the full-history EUR/USD M15 LONG candidate found by
# the fresh 2002-present re-examination.
#
# THIS IS NOT ANOTHER BROAD RESEARCH PASS.
#
# ANCHOR CANDIDATE
# ----------------
# exact bullish engulf
# body ratio >= 1.20
# body >= 1.00 ATR14
# previous 165-bar low structure
# signal low within 0.10 ATR14 of prior 165-bar low
# exclude Tuesday (America/New_York)
# NO NY-hour exclusion
# RR 3.75
# stop = signal low - 10 ticks
#
# Expected prior result at 1-pip adverse cost:
#   ~87 trades full history
#   ~19 pre-2010
#   ~68 from 2010+
#
# PREVIOUS LOCKED BENCHMARK
# -------------------------
# exact bullish engulf
# BR >= 1.35
# body >= 0.75 ATR14
# structure 165 / 0.10 ATR
# exclude Tuesday
# exclude NY hour 07
# RR 3.75
#
# CONFIRMATION TESTS
# ------------------
# 1) Exact anchor parity.
#
# 2) One-at-a-time plateau slices:
#    BR:
#       1.05 / 1.10 / 1.15 / 1.20 / 1.25 / 1.30 / 1.35
#
#    Body ATR:
#       0.80 / 0.90 / 1.00 / 1.10 / 1.20
#
#    Structure distance:
#       0.05 / 0.075 / 0.10 / 0.125 / 0.15
#
#    Structure lookback:
#       120 / 165 / 200
#
#    RR:
#       3.25 / 3.50 / 3.75 / 4.00 / 4.25
#
#    Old NY 07 exclusion:
#       OFF vs ON
#
# 3) Small LOCAL interaction grid only:
#    BR:    1.15 / 1.20 / 1.25
#    Body:  0.90 / 1.00 / 1.10
#    Dist:  0.075 / 0.10 / 0.125
#    RR:    3.50 / 3.75 / 4.00
#    NY07:  OFF / ON
#
#    Fixed:
#      structure lookback = 165
#      Tuesday excluded
#
#    Total = 162 local configs.
#
# FINAL CHECKS
# ------------
# - full history
# - pre-2010
# - 2010+
# - 2002-07
# - 2008-13
# - 2014-19
# - 2020-now
# - 2002-17 vs 2018+
# - last 5Y / 2Y
# - 0.5 / 1 / 1.5 / 2 pip cost stress
# - rolling 12 / 24 / 36M
# - completed calendar years
#
# CONVENTIONS
# -----------
# OANDA midpoint.
# ATR14 Wilder/RMA, SMA seeded.
# Signal timestamp = M15 candle OPEN.
# Tuesday/hour filters use America/New_York.
# Reference entry = signal close.
# Historical long fill = signal close + adverse cost.
# Target based on REFERENCE signal-close risk.
# Stop = signal low - 10 ticks.
# Exit begins next candle.
# Pyramiding 0.
# Exact exit-candle signal eligible.
# Long same-bar:
#   high closer to candle open => target first
#   otherwise stop first.
#
# ONE ZIP:
#   /eurusd-m15-long-final-confirmation/results
#
# READ ONLY. NEVER SENDS ORDERS.
# ============================================================


app = Flask(__name__)

TOKEN = os.getenv("OANDA_TOKEN")
BASE = os.getenv(
    "OANDA_API_URL",
    "https://api-fxtrade.oanda.com",
)

PAIR = "EUR_USD"

START = datetime(
    2002, 5, 6, 20, 0,
    tzinfo=timezone.utc,
)

NOW = (
    datetime.now(timezone.utc)
    .replace(second=0, microsecond=0)
)

NY = ZoneInfo(
    "America/New_York"
)

TICK = 0.00001
PIP = 0.0001

STOP_BUFFER_TICKS = 10

PRIMARY_COST_PIPS = 1.00

COST_GRID = [
    0.50,
    1.00,
    1.50,
    2.00,
]

ANCHOR = {
    "config_id":
        "ANCHOR_BR1.20_BODY1.00_S165_D0.10_RR3.75_NO07",

    "br_min":
        1.20,

    "body_atr_min":
        1.00,

    "structure_lb":
        165,

    "structure_dist_atr_max":
        0.10,

    "rr":
        3.75,

    "exclude_tuesday":
        True,

    "exclude_ny_hour_07":
        False,
}

OLD_BENCHMARK = {
    "config_id":
        "OLD_LOCKED_BR1.35_BODY0.75_S165_D0.10_RR3.75_EX07",

    "br_min":
        1.35,

    "body_atr_min":
        0.75,

    "structure_lb":
        165,

    "structure_dist_atr_max":
        0.10,

    "rr":
        3.75,

    "exclude_tuesday":
        True,

    "exclude_ny_hour_07":
        True,
}


# ============================================================
# OUTPUTS
# ============================================================

OUTPUT_COVERAGE = (
    "eurusd_m15_long_final_confirmation_coverage.csv"
)

OUTPUT_PARITY = (
    "eurusd_m15_long_final_confirmation_parity.csv"
)

OUTPUT_ANCHOR_BENCHMARK = (
    "eurusd_m15_long_final_confirmation_anchor_vs_benchmark.csv"
)

OUTPUT_SLICES = (
    "eurusd_m15_long_final_confirmation_one_way_slices.csv"
)

OUTPUT_GRID = (
    "eurusd_m15_long_final_confirmation_local_grid.csv"
)

OUTPUT_FINALISTS = (
    "eurusd_m15_long_final_confirmation_finalists.csv"
)

OUTPUT_PERIODS = (
    "eurusd_m15_long_final_confirmation_periods.csv"
)

OUTPUT_COST = (
    "eurusd_m15_long_final_confirmation_cost_stress.csv"
)

OUTPUT_ROLLING = (
    "eurusd_m15_long_final_confirmation_rolling.csv"
)

OUTPUT_ROLLING_SUMMARY = (
    "eurusd_m15_long_final_confirmation_rolling_summary.csv"
)

OUTPUT_CALENDAR = (
    "eurusd_m15_long_final_confirmation_calendar_years.csv"
)

OUTPUT_CALENDAR_SUMMARY = (
    "eurusd_m15_long_final_confirmation_calendar_summary.csv"
)

OUTPUT_TRADES = (
    "eurusd_m15_long_final_confirmation_trades.csv"
)

OUTPUT_BUNDLE = (
    "EURUSD_M15_LONG_FINALIST_CONFIRMATION_RESULTS.zip"
)

STATUS = {
    "state":
        "not_started",

    "message":
        "EUR/USD M15 LONG finalist confirmation not started",

    "orders_supported":
        False,

    "trading_enabled":
        False,
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
            "Z",
        )
    )


def parse_oanda_time(
    value,
):
    if value.endswith(
        "Z"
    ):
        value = (
            value[:-1]
            + "+00:00"
        )

    if "." in value:
        left, right = (
            value.split(
                ".",
                1,
            )
        )

        sign = None
        offset = None

        if "+" in right:
            fraction, offset = (
                right.split(
                    "+",
                    1,
                )
            )

            sign = "+"

        elif "-" in right:
            fraction, offset = (
                right.split(
                    "-",
                    1,
                )
            )

            sign = "-"

        else:
            fraction = right

        fraction = (
            fraction[
                :6
            ]
            .ljust(
                6,
                "0",
            )
        )

        value = (
            left
            + "."
            + fraction
        )

        if sign is not None:
            value += (
                sign
                + offset
            )

    return (
        datetime.fromisoformat(
            value
        )
        .astimezone(
            timezone.utc
        )
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
            handle.write(
                ""
            )

        return

    fields = []
    seen = set()

    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(
                    key
                )

                fields.append(
                    key
                )

    with open(
        path,
        "w",
        newline="",
        encoding="utf-8",
    ) as handle:
        writer = (
            csv.DictWriter(
                handle,
                fieldnames=fields,
            )
        )

        writer.writeheader()
        writer.writerows(
            rows
        )


def download_file(
    path,
):
    if not os.path.exists(
        path
    ):
        return jsonify({
            "error":
                "Results not ready yet",
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


def build_bundle():
    files = [
        OUTPUT_COVERAGE,
        OUTPUT_PARITY,
        OUTPUT_ANCHOR_BENCHMARK,
        OUTPUT_SLICES,
        OUTPUT_GRID,
        OUTPUT_FINALISTS,
        OUTPUT_PERIODS,
        OUTPUT_COST,
        OUTPUT_ROLLING,
        OUTPUT_ROLLING_SUMMARY,
        OUTPUT_CALENDAR,
        OUTPUT_CALENDAR_SUMMARY,
        OUTPUT_TRADES,
    ]

    with zipfile.ZipFile(
        OUTPUT_BUNDLE,
        "w",
        compression=zipfile.ZIP_DEFLATED,
    ) as archive:
        for path in files:
            if os.path.exists(
                path
            ):
                archive.write(
                    path,
                    arcname=os.path.basename(
                        path
                    ),
                )


def safe_median(
    values,
):
    values = list(
        values
    )

    if not values:
        return 0.0

    return median(
        values
    )


def add_months(
    dt,
    months,
):
    absolute = (
        dt.year * 12
        + dt.month
        - 1
        + months
    )

    return datetime(
        absolute // 12,
        absolute % 12 + 1,
        1,
        tzinfo=timezone.utc,
    )


def month_floor(
    dt,
):
    return datetime(
        dt.year,
        dt.month,
        1,
        tzinfo=timezone.utc,
    )


# ============================================================
# OANDA FETCH
# ============================================================

def oanda_headers():
    if not TOKEN:
        raise RuntimeError(
            "OANDA_TOKEN is not configured"
        )

    return {
        "Authorization":
            "Bearer "
            + TOKEN.strip(),
    }


def fetch_chunk(
    start,
    end,
):
    url = (
        f"{BASE}/v3/instruments/"
        f"{PAIR}/candles"
    )

    params = {
        "price":
            "M",

        "granularity":
            "M15",

        "smooth":
            "false",

        "from":
            iso_utc(
                start
            ),

        "to":
            iso_utc(
                end
            ),

        "includeFirst":
            "true",
    }

    response = requests.get(
        url,
        headers=oanda_headers(),
        params=params,
        timeout=60,
    )

    response.raise_for_status()

    rows = []

    for item in response.json().get(
        "candles",
        [],
    ):
        if not item.get(
            "complete",
            False,
        ):
            continue

        mid = item[
            "mid"
        ]

        rows.append({
            "time":
                parse_oanda_time(
                    item[
                        "time"
                    ]
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
        })

    return rows


def fetch_history():
    cursor = START
    by_time = {}
    chunk_number = 0

    while cursor < NOW:
        chunk_number += 1

        chunk_end = min(
            cursor
            + timedelta(
                days=35
            ),
            NOW,
        )

        STATUS.update({
            "state":
                "fetching",

            "message": (
                f"M15 chunk {chunk_number}: "
                f"{iso_utc(cursor)} -> "
                f"{iso_utc(chunk_end)}"
            ),
        })

        try:
            rows = (
                fetch_chunk(
                    cursor,
                    chunk_end,
                )
            )

        except requests.HTTPError as error:
            status_code = (
                error.response.status_code
                if error.response
                is not None
                else None
            )

            if status_code in (
                400,
                404,
            ):
                rows = []

            else:
                raise

        for row in rows:
            by_time[
                row["time"]
            ] = row

        cursor = (
            chunk_end
        )

        time.sleep(
            0.02
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

def true_ranges(
    candles,
):
    result = np.full(
        len(candles),
        np.nan,
        dtype=float,
    )

    for i, candle in enumerate(
        candles
    ):
        if i == 0:
            result[i] = (
                candle["high"]
                - candle["low"]
            )

        else:
            previous_close = (
                candles[
                    i - 1
                ]["close"]
            )

            result[i] = max(
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

    return result


def rma(
    values,
    length,
):
    result = np.full(
        len(values),
        np.nan,
        dtype=float,
    )

    if len(
        values
    ) < length:
        return result

    seed = (
        values[
            :length
        ]
    )

    if np.isnan(
        seed
    ).any():
        return result

    result[
        length - 1
    ] = np.mean(
        seed
    )

    for i in range(
        length,
        len(values),
    ):
        if (
            np.isfinite(
                values[i]
            )
            and np.isfinite(
                result[
                    i - 1
                ]
            )
        ):
            result[i] = (
                result[
                    i - 1
                ]
                * (
                    length - 1
                )
                + values[i]
            ) / length

    return result


def atr14(
    candles,
):
    return rma(
        true_ranges(
            candles
        ),
        14,
    )


def rolling_previous_low(
    values,
    lookback,
):
    result = np.full(
        len(values),
        np.nan,
        dtype=float,
    )

    dq = deque()

    for i in range(
        len(values)
    ):
        oldest = (
            i
            - lookback
        )

        while (
            dq
            and dq[0]
            < oldest
        ):
            dq.popleft()

        if (
            i >= lookback
            and dq
        ):
            result[i] = (
                values[
                    dq[0]
                ]
            )

        while (
            dq
            and values[
                dq[-1]
            ]
            >= values[i]
        ):
            dq.pop()

        dq.append(
            i
        )

    return result


# ============================================================
# FEATURE CACHE
# ============================================================

def build_features(
    candles,
):
    n = len(
        candles
    )

    opens = np.array(
        [
            candle["open"]
            for candle
            in candles
        ],
        dtype=float,
    )

    highs = np.array(
        [
            candle["high"]
            for candle
            in candles
        ],
        dtype=float,
    )

    lows = np.array(
        [
            candle["low"]
            for candle
            in candles
        ],
        dtype=float,
    )

    closes = np.array(
        [
            candle["close"]
            for candle
            in candles
        ],
        dtype=float,
    )

    atr = atr14(
        candles
    )

    bullish = (
        closes > opens
    )

    exact_bullish_engulf = (
        np.zeros(
            n,
            dtype=bool,
        )
    )

    exact_bullish_engulf[
        1:
    ] = (
        (
            closes[
                :-1
            ]
            < opens[
                :-1
            ]
        )
        & (
            closes[
                1:
            ]
            > opens[
                1:
            ]
        )
        & (
            opens[
                1:
            ]
            <= closes[
                :-1
            ]
        )
        & (
            closes[
                1:
            ]
            >= opens[
                :-1
            ]
        )
    )

    body = (
        closes
        - opens
    )

    previous_body = np.full(
        n,
        np.nan,
        dtype=float,
    )

    previous_body[
        1:
    ] = np.abs(
        closes[
            :-1
        ]
        - opens[
            :-1
        ]
    )

    body_ratio = np.full(
        n,
        np.nan,
        dtype=float,
    )

    valid_previous_body = (
        previous_body > 0
    )

    body_ratio[
        valid_previous_body
    ] = (
        body[
            valid_previous_body
        ]
        / previous_body[
            valid_previous_body
        ]
    )

    valid_atr = (
        np.isfinite(
            atr
        )
        & (
            atr > 0
        )
    )

    body_atr = np.full(
        n,
        np.nan,
        dtype=float,
    )

    body_atr[
        valid_atr
    ] = (
        body[
            valid_atr
        ]
        / atr[
            valid_atr
        ]
    )

    previous_lows = {}

    structure_distance = {}

    for lookback in (
        120,
        165,
        200,
    ):
        previous_lows[
            lookback
        ] = (
            rolling_previous_low(
                lows,
                lookback,
            )
        )

        distance = np.full(
            n,
            np.nan,
            dtype=float,
        )

        valid = (
            valid_atr
            & np.isfinite(
                previous_lows[
                    lookback
                ]
            )
        )

        distance[
            valid
        ] = (
            np.abs(
                lows[
                    valid
                ]
                - previous_lows[
                    lookback
                ][
                    valid
                ]
            )
            / atr[
                valid
            ]
        )

        structure_distance[
            lookback
        ] = (
            distance
        )

    ny_hour = np.zeros(
        n,
        dtype=np.int16,
    )

    ny_weekday = np.zeros(
        n,
        dtype=np.int16,
    )

    for i, candle in enumerate(
        candles
    ):
        local = (
            candle[
                "time"
            ]
            .astimezone(
                NY
            )
        )

        ny_hour[i] = (
            local.hour
        )

        ny_weekday[i] = (
            local.weekday()
        )

    return {
        "n":
            n,

        "exact_bullish_engulf":
            exact_bullish_engulf,

        "body_ratio":
            body_ratio,

        "body_atr":
            body_atr,

        "structure_distance":
            structure_distance,

        "ny_hour":
            ny_hour,

        "ny_weekday":
            ny_weekday,
    }


# ============================================================
# SIGNALS
# ============================================================

def signal_indices(
    config,
    features,
):
    mask = (
        features[
            "exact_bullish_engulf"
        ].copy()
    )

    mask &= (
        features[
            "body_ratio"
        ]
        >= config[
            "br_min"
        ]
    )

    mask &= (
        features[
            "body_atr"
        ]
        >= config[
            "body_atr_min"
        ]
    )

    mask &= (
        features[
            "structure_distance"
        ][
            config[
                "structure_lb"
            ]
        ]
        <= config[
            "structure_dist_atr_max"
        ]
    )

    if config.get(
        "exclude_tuesday",
        True,
    ):
        mask &= (
            features[
                "ny_weekday"
            ]
            != 1
        )

    if config.get(
        "exclude_ny_hour_07",
        False,
    ):
        mask &= (
            features[
                "ny_hour"
            ]
            != 7
        )

    mask[
        :200
    ] = False

    return np.flatnonzero(
        mask
    ).tolist()


# ============================================================
# TRADE OUTCOMES
# ============================================================

OUTCOME_CACHE = {}


def compute_outcome(
    candles,
    signal_index,
    rr,
    cost_pips,
):
    signal = (
        candles[
            signal_index
        ]
    )

    reference_entry = (
        signal["close"]
    )

    stop = (
        signal["low"]
        - STOP_BUFFER_TICKS
        * TICK
    )

    reference_risk = (
        reference_entry
        - stop
    )

    if reference_risk <= 0:
        return None

    target = (
        reference_entry
        + rr
        * reference_risk
    )

    fill = (
        reference_entry
        + cost_pips
        * PIP
    )

    actual_risk = (
        fill
        - stop
    )

    if actual_risk <= 0:
        return None

    for j in range(
        signal_index + 1,
        len(candles),
    ):
        candle = (
            candles[j]
        )

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
            and hit_target
        ):
            high_distance = abs(
                candle["high"]
                - candle["open"]
            )

            low_distance = abs(
                candle["open"]
                - candle["low"]
            )

            if (
                high_distance
                < low_distance
            ):
                exit_price = (
                    target
                )

                reason = (
                    "TARGET"
                )

            else:
                exit_price = (
                    stop
                )

                reason = (
                    "STOP"
                )

        elif hit_target:
            exit_price = (
                target
            )

            reason = (
                "TARGET"
            )

        elif hit_stop:
            exit_price = (
                stop
            )

            reason = (
                "STOP"
            )

        else:
            continue

        result_r = (
            exit_price
            - fill
        ) / actual_risk

        return {
            "signal_index":
                signal_index,

            "exit_index":
                j,

            "entry_time":
                signal[
                    "time"
                ],

            "exit_time":
                candle[
                    "time"
                ],

            "entry_time_utc":
                iso_utc(
                    signal[
                        "time"
                    ]
                ),

            "exit_time_utc":
                iso_utc(
                    candle[
                        "time"
                    ]
                ),

            "result_r":
                result_r,

            "exit_reason":
                reason,

            "rr":
                rr,

            "cost_pips":
                cost_pips,
        }

    return None


def get_outcome(
    candles,
    signal_index,
    rr,
    cost_pips,
):
    key = (
        signal_index,
        round(
            rr,
            4,
        ),
        round(
            cost_pips,
            4,
        ),
    )

    if key not in (
        OUTCOME_CACHE
    ):
        OUTCOME_CACHE[
            key
        ] = compute_outcome(
            candles,
            signal_index,
            rr,
            cost_pips,
        )

    return OUTCOME_CACHE[
        key
    ]


def run_backtest(
    candles,
    indices,
    rr,
    cost_pips,
    start=None,
    end=None,
):
    use = indices

    if (
        start is not None
        or end is not None
    ):
        times = [
            candles[
                i
            ][
                "time"
            ]
            for i
            in indices
        ]

        left = (
            0
            if start is None
            else bisect.bisect_left(
                times,
                start,
            )
        )

        right = (
            len(
                indices
            )
            if end is None
            else bisect.bisect_left(
                times,
                end,
            )
        )

        use = (
            indices[
                left:right
            ]
        )

    trades = []
    position = 0

    while (
        position
        < len(use)
    ):
        signal_index = (
            use[
                position
            ]
        )

        trade = get_outcome(
            candles,
            signal_index,
            rr,
            cost_pips,
        )

        if trade is None:
            position += 1
            continue

        trades.append(
            dict(
                trade
            )
        )

        position = (
            bisect.bisect_left(
                use,
                trade[
                    "exit_index"
                ],
                lo=position + 1,
            )
        )

    return trades


# ============================================================
# STATS
# ============================================================

def stats_from_trades(
    trades,
):
    results = [
        float(
            trade[
                "result_r"
            ]
        )
        for trade
        in trades
    ]

    winners = [
        value
        for value
        in results
        if value > 0
    ]

    losers = [
        value
        for value
        in results
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

    if gross_loss > 0:
        profit_factor = (
            gross_profit
            / gross_loss
        )

    elif gross_profit > 0:
        profit_factor = (
            999.0
        )

    else:
        profit_factor = (
            0.0
        )

    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0

    current_streak = 0
    longest_streak = 0

    for result in results:
        equity += (
            result
        )

        peak = max(
            peak,
            equity,
        )

        max_drawdown = min(
            max_drawdown,
            equity
            - peak,
        )

        if result < 0:
            current_streak += 1

            longest_streak = max(
                longest_streak,
                current_streak,
            )

        else:
            current_streak = 0

    total_r = sum(
        results
    )

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
            max_drawdown,

        "longest_loss_streak":
            longest_streak,
    }


ERAS = [
    (
        "ERA_2002_2007",
        START,
        datetime(
            2008, 1, 1,
            tzinfo=timezone.utc,
        ),
    ),

    (
        "ERA_2008_2013",
        datetime(
            2008, 1, 1,
            tzinfo=timezone.utc,
        ),
        datetime(
            2014, 1, 1,
            tzinfo=timezone.utc,
        ),
    ),

    (
        "ERA_2014_2019",
        datetime(
            2014, 1, 1,
            tzinfo=timezone.utc,
        ),
        datetime(
            2020, 1, 1,
            tzinfo=timezone.utc,
        ),
    ),

    (
        "ERA_2020_NOW",
        datetime(
            2020, 1, 1,
            tzinfo=timezone.utc,
        ),
        NOW,
    ),
]


def evaluation_row(
    config,
    candles,
    indices,
):
    full = stats_from_trades(
        run_backtest(
            candles,
            indices,
            config[
                "rr"
            ],
            PRIMARY_COST_PIPS,
            candles[0][
                "time"
            ],
            NOW,
        )
    )

    pre = stats_from_trades(
        run_backtest(
            candles,
            indices,
            config[
                "rr"
            ],
            PRIMARY_COST_PIPS,
            candles[0][
                "time"
            ],
            datetime(
                2010, 1, 1,
                tzinfo=timezone.utc,
            ),
        )
    )

    post = stats_from_trades(
        run_backtest(
            candles,
            indices,
            config[
                "rr"
            ],
            PRIMARY_COST_PIPS,
            datetime(
                2010, 1, 1,
                tzinfo=timezone.utc,
            ),
            NOW,
        )
    )

    era_stats = []

    for _, start, end in ERAS:
        era_stats.append(
            stats_from_trades(
                run_backtest(
                    candles,
                    indices,
                    config[
                        "rr"
                    ],
                    PRIMARY_COST_PIPS,
                    start,
                    end,
                )
            )
        )

    positive_eras = sum(
        1
        for s in era_stats
        if s[
            "total_r"
        ] > 0
    )

    min_era_pf = min(
        s[
            "profit_factor"
        ]
        for s
        in era_stats
    )

    score = (
        1.5
        * min(
            full[
                "profit_factor"
            ],
            3.0,
        )
        + 0.8
        * min(
            pre[
                "profit_factor"
            ],
            2.5,
        )
        + 0.8
        * min(
            post[
                "profit_factor"
            ],
            2.5,
        )
        + 0.40
        * positive_eras
        + 0.20
        * min(
            max(
                min_era_pf,
                0.0,
            ),
            2.0,
        )
        + 0.15
        * min(
            full[
                "trades"
            ]
            / 100.0,
            2.0,
        )
    )

    row = {
        "config_id":
            config[
                "config_id"
            ],

        "br_min":
            config[
                "br_min"
            ],

        "body_atr_min":
            config[
                "body_atr_min"
            ],

        "structure_lb":
            config[
                "structure_lb"
            ],

        "structure_dist_atr_max":
            config[
                "structure_dist_atr_max"
            ],

        "rr":
            config[
                "rr"
            ],

        "exclude_tuesday":
            config[
                "exclude_tuesday"
            ],

        "exclude_ny_hour_07":
            config[
                "exclude_ny_hour_07"
            ],

        "full_trades":
            full[
                "trades"
            ],

        "full_pf":
            round(
                full[
                    "profit_factor"
                ],
                6,
            ),

        "full_r":
            round(
                full[
                    "total_r"
                ],
                4,
            ),

        "full_exp":
            round(
                full[
                    "expectancy_r"
                ],
                6,
            ),

        "full_dd":
            round(
                full[
                    "max_drawdown_r"
                ],
                4,
            ),

        "pre2010_trades":
            pre[
                "trades"
            ],

        "pre2010_pf":
            round(
                pre[
                    "profit_factor"
                ],
                6,
            ),

        "pre2010_r":
            round(
                pre[
                    "total_r"
                ],
                4,
            ),

        "post2010_trades":
            post[
                "trades"
            ],

        "post2010_pf":
            round(
                post[
                    "profit_factor"
                ],
                6,
            ),

        "post2010_r":
            round(
                post[
                    "total_r"
                ],
                4,
            ),

        "positive_eras":
            positive_eras,

        "min_era_pf":
            round(
                min_era_pf,
                6,
            ),

        "robust_score":
            round(
                score,
                6,
            ),
    }

    for i, s in enumerate(
        era_stats,
        1,
    ):
        row[
            f"era{i}_trades"
        ] = s[
            "trades"
        ]

        row[
            f"era{i}_pf"
        ] = round(
            s[
                "profit_factor"
            ],
            6,
        )

        row[
            f"era{i}_r"
        ] = round(
            s[
                "total_r"
            ],
            4,
        )

    return row


def sort_rows(
    rows,
):
    return sorted(
        rows,
        key=lambda row: (
            row[
                "positive_eras"
            ],

            row[
                "pre2010_r"
            ] > 0,

            row[
                "post2010_r"
            ] > 0,

            row[
                "robust_score"
            ],

            row[
                "full_r"
            ],
        ),
        reverse=True,
    )


# ============================================================
# CONFIG GENERATION
# ============================================================

def clone_anchor(
    config_id,
):
    config = deepcopy(
        ANCHOR
    )

    config[
        "config_id"
    ] = config_id

    return config


def one_way_configs():
    configs = []

    for value in [
        1.05,
        1.10,
        1.15,
        1.20,
        1.25,
        1.30,
        1.35,
    ]:
        config = clone_anchor(
            f"SLICE_BR_{value:.2f}"
        )

        config[
            "br_min"
        ] = value

        config[
            "slice"
        ] = "BR"

        configs.append(
            config
        )

    for value in [
        0.80,
        0.90,
        1.00,
        1.10,
        1.20,
    ]:
        config = clone_anchor(
            f"SLICE_BODY_{value:.2f}"
        )

        config[
            "body_atr_min"
        ] = value

        config[
            "slice"
        ] = "BODY_ATR"

        configs.append(
            config
        )

    for value in [
        0.05,
        0.075,
        0.10,
        0.125,
        0.15,
    ]:
        config = clone_anchor(
            f"SLICE_DIST_{value}"
        )

        config[
            "structure_dist_atr_max"
        ] = value

        config[
            "slice"
        ] = "STRUCTURE_DISTANCE"

        configs.append(
            config
        )

    for value in [
        120,
        165,
        200,
    ]:
        config = clone_anchor(
            f"SLICE_LB_{value}"
        )

        config[
            "structure_lb"
        ] = value

        config[
            "slice"
        ] = "STRUCTURE_LOOKBACK"

        configs.append(
            config
        )

    for value in [
        3.25,
        3.50,
        3.75,
        4.00,
        4.25,
    ]:
        config = clone_anchor(
            f"SLICE_RR_{value:.2f}"
        )

        config[
            "rr"
        ] = value

        config[
            "slice"
        ] = "RR"

        configs.append(
            config
        )

    for exclude_07 in [
        False,
        True,
    ]:
        config = clone_anchor(
            (
                "SLICE_NY07_EXCLUDED"
                if exclude_07
                else "SLICE_NY07_ALLOWED"
            )
        )

        config[
            "exclude_ny_hour_07"
        ] = exclude_07

        config[
            "slice"
        ] = "NY07"

        configs.append(
            config
        )

    return configs


def local_grid_configs():
    configs = []

    counter = 0

    for br in [
        1.15,
        1.20,
        1.25,
    ]:
        for body in [
            0.90,
            1.00,
            1.10,
        ]:
            for distance in [
                0.075,
                0.10,
                0.125,
            ]:
                for rr in [
                    3.50,
                    3.75,
                    4.00,
                ]:
                    for exclude_07 in [
                        False,
                        True,
                    ]:
                        counter += 1

                        config = clone_anchor(
                            f"GRID_{counter:03d}"
                        )

                        config[
                            "br_min"
                        ] = br

                        config[
                            "body_atr_min"
                        ] = body

                        config[
                            "structure_dist_atr_max"
                        ] = distance

                        config[
                            "rr"
                        ] = rr

                        config[
                            "exclude_ny_hour_07"
                        ] = exclude_07

                        configs.append(
                            config
                        )

    return configs


# ============================================================
# DEEP FINALIST ANALYSIS
# ============================================================

def stats_row(
    config,
    label,
    trades,
):
    s = stats_from_trades(
        trades
    )

    return {
        "config_id":
            config[
                "config_id"
            ],

        "period":
            label,

        "trades":
            s[
                "trades"
            ],

        "winners":
            s[
                "winners"
            ],

        "losers":
            s[
                "losers"
            ],

        "win_rate":
            round(
                s[
                    "win_rate"
                ],
                4,
            ),

        "profit_factor":
            round(
                s[
                    "profit_factor"
                ],
                6,
            ),

        "total_r":
            round(
                s[
                    "total_r"
                ],
                4,
            ),

        "expectancy_r":
            round(
                s[
                    "expectancy_r"
                ],
                6,
            ),

        "max_drawdown_r":
            round(
                s[
                    "max_drawdown_r"
                ],
                4,
            ),

        "longest_loss_streak":
            s[
                "longest_loss_streak"
            ],
    }


def period_rows(
    config,
    candles,
    indices,
):
    periods = [
        (
            "FULL_HISTORY",
            candles[0][
                "time"
            ],
            NOW,
        ),

        (
            "PRE_2010",
            candles[0][
                "time"
            ],
            datetime(
                2010, 1, 1,
                tzinfo=timezone.utc,
            ),
        ),

        (
            "2010_PLUS",
            datetime(
                2010, 1, 1,
                tzinfo=timezone.utc,
            ),
            NOW,
        ),

        *ERAS,

        (
            "DEV_2002_2017",
            candles[0][
                "time"
            ],
            datetime(
                2018, 1, 1,
                tzinfo=timezone.utc,
            ),
        ),

        (
            "VALIDATION_2018_PLUS",
            datetime(
                2018, 1, 1,
                tzinfo=timezone.utc,
            ),
            NOW,
        ),

        (
            "LAST_5Y",
            NOW
            - timedelta(
                days=365.2425
                * 5
            ),
            NOW,
        ),

        (
            "LAST_2Y",
            NOW
            - timedelta(
                days=365.2425
                * 2
            ),
            NOW,
        ),
    ]

    rows = []

    for (
        label,
        start,
        end,
    ) in periods:
        trades = run_backtest(
            candles,
            indices,
            config[
                "rr"
            ],
            PRIMARY_COST_PIPS,
            start,
            end,
        )

        row = stats_row(
            config,
            label,
            trades,
        )

        row[
            "start_utc"
        ] = iso_utc(
            start
        )

        row[
            "end_utc"
        ] = iso_utc(
            end
        )

        rows.append(
            row
        )

    return rows


def cost_rows(
    config,
    candles,
    indices,
):
    rows = []

    for (
        label,
        start,
        end,
    ) in [
        (
            "FULL_HISTORY",
            candles[0][
                "time"
            ],
            NOW,
        ),

        (
            "PRE_2010",
            candles[0][
                "time"
            ],
            datetime(
                2010, 1, 1,
                tzinfo=timezone.utc,
            ),
        ),

        (
            "2010_PLUS",
            datetime(
                2010, 1, 1,
                tzinfo=timezone.utc,
            ),
            NOW,
        ),
    ]:
        for cost in (
            COST_GRID
        ):
            trades = (
                run_backtest(
                    candles,
                    indices,
                    config[
                        "rr"
                    ],
                    cost,
                    start,
                    end,
                )
            )

            row = stats_row(
                config,
                label,
                trades,
            )

            row[
                "cost_pips"
            ] = cost

            rows.append(
                row
            )

    return rows


def rolling_rows(
    config,
    candles,
    indices,
):
    rows = []

    first_month = month_floor(
        max(
            candles[0][
                "time"
            ],
            START,
        )
    )

    last_month = month_floor(
        NOW
    )

    for months in [
        12,
        24,
        36,
    ]:
        start = (
            first_month
        )

        while (
            add_months(
                start,
                months,
            )
            <= last_month
        ):
            end = add_months(
                start,
                months,
            )

            s = stats_from_trades(
                run_backtest(
                    candles,
                    indices,
                    config[
                        "rr"
                    ],
                    PRIMARY_COST_PIPS,
                    start,
                    end,
                )
            )

            rows.append({
                "config_id":
                    config[
                        "config_id"
                    ],

                "months":
                    months,

                "start_utc":
                    iso_utc(
                        start
                    ),

                "end_utc":
                    iso_utc(
                        end
                    ),

                "trades":
                    s[
                        "trades"
                    ],

                "profit_factor":
                    round(
                        s[
                            "profit_factor"
                        ],
                        6,
                    ),

                "total_r":
                    round(
                        s[
                            "total_r"
                        ],
                        4,
                    ),

                "positive":
                    s[
                        "total_r"
                    ] > 0,

                "zero_trade":
                    s[
                        "trades"
                    ] == 0,
            })

            start = add_months(
                start,
                1,
            )

    return rows


def rolling_summary_rows(
    rows,
):
    grouped = defaultdict(
        list
    )

    for row in rows:
        grouped[
            (
                row[
                    "config_id"
                ],
                row[
                    "months"
                ],
            )
        ].append(
            row
        )

    output = []

    for (
        config_id,
        months,
    ), subset in grouped.items():
        active = [
            row
            for row in subset
            if row[
                "trades"
            ] > 0
        ]

        positive = [
            row
            for row in subset
            if row[
                "positive"
            ]
        ]

        positive_active = [
            row
            for row in active
            if row[
                "positive"
            ]
        ]

        output.append({
            "config_id":
                config_id,

            "months":
                months,

            "windows":
                len(
                    subset
                ),

            "active_windows":
                len(
                    active
                ),

            "zero_trade_windows":
                len(
                    subset
                )
                - len(
                    active
                ),

            "positive_windows_pct":
                round(
                    100.0
                    * len(
                        positive
                    )
                    / len(
                        subset
                    ),
                    4,
                )
                if subset
                else 0.0,

            "positive_active_windows_pct":
                round(
                    100.0
                    * len(
                        positive_active
                    )
                    / len(
                        active
                    ),
                    4,
                )
                if active
                else 0.0,

            "median_r_all":
                round(
                    safe_median([
                        row[
                            "total_r"
                        ]
                        for row
                        in subset
                    ]),
                    4,
                ),

            "median_r_active":
                round(
                    safe_median([
                        row[
                            "total_r"
                        ]
                        for row
                        in active
                    ]),
                    4,
                ),

            "median_pf_active":
                round(
                    safe_median([
                        row[
                            "profit_factor"
                        ]
                        for row
                        in active
                    ]),
                    6,
                ),

            "worst_r":
                round(
                    min(
                        row[
                            "total_r"
                        ]
                        for row
                        in subset
                    ),
                    4,
                ),

            "best_r":
                round(
                    max(
                        row[
                            "total_r"
                        ]
                        for row
                        in subset
                    ),
                    4,
                ),
        })

    return output


def calendar_rows(
    config,
    candles,
    indices,
):
    rows = []

    first_year = max(
        candles[0][
            "time"
        ].year,
        START.year,
    )

    last_completed_year = (
        NOW.year
        - 1
    )

    for year in range(
        first_year,
        last_completed_year
        + 1,
    ):
        start = datetime(
            year,
            1,
            1,
            tzinfo=timezone.utc,
        )

        end = datetime(
            year + 1,
            1,
            1,
            tzinfo=timezone.utc,
        )

        s = stats_from_trades(
            run_backtest(
                candles,
                indices,
                config[
                    "rr"
                ],
                PRIMARY_COST_PIPS,
                start,
                end,
            )
        )

        rows.append({
            "config_id":
                config[
                    "config_id"
                ],

            "year":
                year,

            "trades":
                s[
                    "trades"
                ],

            "profit_factor":
                round(
                    s[
                        "profit_factor"
                    ],
                    6,
                ),

            "total_r":
                round(
                    s[
                        "total_r"
                    ],
                    4,
                ),

            "positive":
                s[
                    "total_r"
                ] > 0,

            "negative":
                s[
                    "total_r"
                ] < 0,

            "zero_trade":
                s[
                    "trades"
                ] == 0,
        })

    return rows


def calendar_summary_rows(
    rows,
):
    grouped = defaultdict(
        list
    )

    for row in rows:
        grouped[
            row[
                "config_id"
            ]
        ].append(
            row
        )

    output = []

    for (
        config_id,
        subset,
    ) in grouped.items():
        active = [
            row
            for row in subset
            if row[
                "trades"
            ] > 0
        ]

        positive = [
            row
            for row in subset
            if row[
                "positive"
            ]
        ]

        positive_active = [
            row
            for row in active
            if row[
                "positive"
            ]
        ]

        negative = [
            row
            for row in subset
            if row[
                "negative"
            ]
        ]

        output.append({
            "config_id":
                config_id,

            "completed_years":
                len(
                    subset
                ),

            "active_years":
                len(
                    active
                ),

            "positive_years":
                len(
                    positive
                ),

            "negative_years":
                len(
                    negative
                ),

            "zero_trade_years":
                len(
                    subset
                )
                - len(
                    active
                ),

            "positive_years_pct":
                round(
                    100.0
                    * len(
                        positive
                    )
                    / len(
                        subset
                    ),
                    4,
                )
                if subset
                else 0.0,

            "positive_active_years_pct":
                round(
                    100.0
                    * len(
                        positive_active
                    )
                    / len(
                        active
                    ),
                    4,
                )
                if active
                else 0.0,

            "median_trades_year":
                round(
                    safe_median([
                        row[
                            "trades"
                        ]
                        for row
                        in subset
                    ]),
                    4,
                ),

            "median_year_r":
                round(
                    safe_median([
                        row[
                            "total_r"
                        ]
                        for row
                        in subset
                    ]),
                    4,
                ),

            "worst_year_r":
                round(
                    min(
                        row[
                            "total_r"
                        ]
                        for row
                        in subset
                    ),
                    4,
                ),

            "best_year_r":
                round(
                    max(
                        row[
                            "total_r"
                        ]
                        for row
                        in subset
                    ),
                    4,
                ),
        })

    return output


# ============================================================
# MAIN
# ============================================================

def run_research():
    try:
        candles = (
            fetch_history()
        )

        if not candles:
            raise RuntimeError(
                "No EUR_USD M15 history returned"
            )

        write_csv(
            OUTPUT_COVERAGE,
            [{
                "instrument":
                    PAIR,

                "requested_start_utc":
                    iso_utc(
                        START
                    ),

                "actual_first_m15_utc":
                    iso_utc(
                        candles[0][
                            "time"
                        ]
                    ),

                "actual_last_m15_utc":
                    iso_utc(
                        candles[-1][
                            "time"
                        ]
                    ),

                "m15_candles":
                    len(
                        candles
                    ),
            }],
        )

        STATUS.update({
            "state":
                "precomputing",

            "message":
                "Building frozen M15 feature cache",
        })

        features = (
            build_features(
                candles
            )
        )

        # ----------------------------------------------------
        # Anchor + old benchmark
        # ----------------------------------------------------
        comparison_rows = []

        for config in [
            OLD_BENCHMARK,
            ANCHOR,
        ]:
            idx = signal_indices(
                config,
                features,
            )

            comparison_rows.append(
                evaluation_row(
                    config,
                    candles,
                    idx,
                )
            )

        write_csv(
            OUTPUT_ANCHOR_BENCHMARK,
            comparison_rows,
        )

        anchor_row = next(
            row
            for row
            in comparison_rows
            if row[
                "config_id"
            ] == ANCHOR[
                "config_id"
            ]
        )

        benchmark_row = next(
            row
            for row
            in comparison_rows
            if row[
                "config_id"
            ] == OLD_BENCHMARK[
                "config_id"
            ]
        )

        parity_rows = [{
            "config_id":
                ANCHOR[
                    "config_id"
                ],

            "expected_full_trades":
                87,

            "actual_full_trades":
                anchor_row[
                    "full_trades"
                ],

            "expected_pre2010_trades":
                19,

            "actual_pre2010_trades":
                anchor_row[
                    "pre2010_trades"
                ],

            "expected_post2010_trades":
                68,

            "actual_post2010_trades":
                anchor_row[
                    "post2010_trades"
                ],

            "status":
                (
                    "MATCH"
                    if (
                        anchor_row[
                            "full_trades"
                        ] == 87
                        and anchor_row[
                            "pre2010_trades"
                        ] == 19
                        and anchor_row[
                            "post2010_trades"
                        ] == 68
                    )
                    else "CHECK_OR_NEWER_COMPLETED_TRADES"
                ),
        }, {
            "config_id":
                OLD_BENCHMARK[
                    "config_id"
                ],

            "expected_full_trades":
                111,

            "actual_full_trades":
                benchmark_row[
                    "full_trades"
                ],

            "expected_post2010_trades":
                85,

            "actual_post2010_trades":
                benchmark_row[
                    "post2010_trades"
                ],

            "status":
                (
                    "MATCH"
                    if (
                        benchmark_row[
                            "full_trades"
                        ] == 111
                        and benchmark_row[
                            "post2010_trades"
                        ] == 85
                    )
                    else "CHECK_OR_NEWER_COMPLETED_TRADES"
                ),
        }]

        write_csv(
            OUTPUT_PARITY,
            parity_rows,
        )

        # ----------------------------------------------------
        # One-way plateau slices
        # ----------------------------------------------------
        slice_configs = (
            one_way_configs()
        )

        slice_rows = []

        for i, config in enumerate(
            slice_configs,
            1,
        ):
            STATUS.update({
                "state":
                    "slices",

                "message": (
                    f"One-way slice "
                    f"{i}/{len(slice_configs)} "
                    f"{config['config_id']}"
                ),
            })

            idx = signal_indices(
                config,
                features,
            )

            row = evaluation_row(
                config,
                candles,
                idx,
            )

            row[
                "slice"
            ] = config[
                "slice"
            ]

            slice_rows.append(
                row
            )

        write_csv(
            OUTPUT_SLICES,
            slice_rows,
        )

        # ----------------------------------------------------
        # Small local interaction grid
        # ----------------------------------------------------
        grid_configs = (
            local_grid_configs()
        )

        grid_rows = []

        grid_by_id = {
            config[
                "config_id"
            ]:
                config
            for config
            in grid_configs
        }

        for i, config in enumerate(
            grid_configs,
            1,
        ):
            STATUS.update({
                "state":
                    "local_grid",

                "message": (
                    f"Local grid "
                    f"{i}/{len(grid_configs)} "
                    f"{config['config_id']}"
                ),
            })

            idx = signal_indices(
                config,
                features,
            )

            grid_rows.append(
                evaluation_row(
                    config,
                    candles,
                    idx,
                )
            )

        grid_rows = sort_rows(
            grid_rows
        )

        write_csv(
            OUTPUT_GRID,
            grid_rows,
        )

        # ----------------------------------------------------
        # Finalists:
        # anchor always included + strongest nearby alternatives.
        # ----------------------------------------------------
        eligible = [
            row
            for row
            in grid_rows
            if (
                row[
                    "full_trades"
                ] >= 70
                and row[
                    "pre2010_r"
                ] > 0
                and row[
                    "post2010_r"
                ] > 0
                and row[
                    "positive_eras"
                ] == 4
            )
        ]

        selected_rows = []

        # Anchor first.
        selected_rows.append(
            anchor_row
        )

        for row in eligible:
            if (
                row[
                    "config_id"
                ]
                == ANCHOR[
                    "config_id"
                ]
            ):
                continue

            selected_rows.append(
                row
            )

            if len(
                selected_rows
            ) >= 8:
                break

        # If fewer than 8 strict finalists, fill with best grid rows.
        if len(
            selected_rows
        ) < 8:
            existing = {
                row[
                    "config_id"
                ]
                for row
                in selected_rows
            }

            for row in grid_rows:
                if row[
                    "config_id"
                ] in existing:
                    continue

                selected_rows.append(
                    row
                )

                existing.add(
                    row[
                        "config_id"
                    ]
                )

                if len(
                    selected_rows
                ) >= 8:
                    break

        write_csv(
            OUTPUT_FINALISTS,
            selected_rows,
        )

        selected_configs = [
            ANCHOR
        ]

        selected_ids = {
            ANCHOR[
                "config_id"
            ]
        }

        for row in selected_rows[
            1:
        ]:
            config = grid_by_id.get(
                row[
                    "config_id"
                ]
            )

            if (
                config is not None
                and config[
                    "config_id"
                ] not in selected_ids
            ):
                selected_configs.append(
                    config
                )

                selected_ids.add(
                    config[
                        "config_id"
                    ]
                )

        # ----------------------------------------------------
        # Deep final checks
        # ----------------------------------------------------
        period_output = []
        cost_output = []
        rolling_output = []
        calendar_output = []
        trade_output = []

        for i, config in enumerate(
            selected_configs,
            1,
        ):
            STATUS.update({
                "state":
                    "deep_validation",

                "message": (
                    f"Deep validation "
                    f"{i}/{len(selected_configs)} "
                    f"{config['config_id']}"
                ),
            })

            idx = signal_indices(
                config,
                features,
            )

            period_output.extend(
                period_rows(
                    config,
                    candles,
                    idx,
                )
            )

            cost_output.extend(
                cost_rows(
                    config,
                    candles,
                    idx,
                )
            )

            rolling_output.extend(
                rolling_rows(
                    config,
                    candles,
                    idx,
                )
            )

            calendar_output.extend(
                calendar_rows(
                    config,
                    candles,
                    idx,
                )
            )

            full_trades = (
                run_backtest(
                    candles,
                    idx,
                    config[
                        "rr"
                    ],
                    PRIMARY_COST_PIPS,
                    candles[0][
                        "time"
                    ],
                    NOW,
                )
            )

            for trade in (
                full_trades
            ):
                row = dict(
                    trade
                )

                row[
                    "config_id"
                ] = config[
                    "config_id"
                ]

                trade_output.append(
                    row
                )

        write_csv(
            OUTPUT_PERIODS,
            period_output,
        )

        write_csv(
            OUTPUT_COST,
            cost_output,
        )

        write_csv(
            OUTPUT_ROLLING,
            rolling_output,
        )

        write_csv(
            OUTPUT_ROLLING_SUMMARY,
            rolling_summary_rows(
                rolling_output
            ),
        )

        write_csv(
            OUTPUT_CALENDAR,
            calendar_output,
        )

        write_csv(
            OUTPUT_CALENDAR_SUMMARY,
            calendar_summary_rows(
                calendar_output
            ),
        )

        write_csv(
            OUTPUT_TRADES,
            trade_output,
        )

        STATUS.update({
            "state":
                "packaging",

            "message":
                "Building one ZIP results bundle",
        })

        build_bundle()

        STATUS.update({
            "state":
                "complete",

            "message":
                "EUR/USD M15 LONG finalist confirmation complete",

            "m15_candles":
                len(
                    candles
                ),

            "slice_configs":
                len(
                    slice_configs
                ),

            "grid_configs":
                len(
                    grid_configs
                ),

            "deep_finalists":
                len(
                    selected_configs
                ),

            "results_bundle":
                OUTPUT_BUNDLE,
        })

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
            "EURUSD M15 LONG Finalist Confirmation",

        "status":
            STATUS[
                "state"
            ],

        "instrument":
            PAIR,

        "timeframe":
            "M15",

        "side":
            "BUY",

        "anchor": {
            "br_min":
                1.20,

            "body_atr_min":
                1.00,

            "structure_lb":
                165,

            "structure_dist_atr_max":
                0.10,

            "exclude_tuesday":
                True,

            "exclude_ny_hour_07":
                False,

            "rr":
                3.75,
        },

        "orders_supported":
            False,

        "trading_enabled":
            False,

        "routes": [
            "/eurusd-m15-long-final-confirmation/status",
            "/eurusd-m15-long-final-confirmation/results",
        ],
    })


@app.route(
    "/eurusd-m15-long-final-confirmation/status"
)
def route_status():
    return jsonify(
        STATUS
    )


@app.route(
    "/eurusd-m15-long-final-confirmation/results"
)
def route_results():
    return download_file(
        OUTPUT_BUNDLE
    )


if __name__ == "__main__":
    research_thread = (
        threading.Thread(
            target=run_research,
            name=(
                "eurusd-m15-long-"
                "final-confirmation"
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
