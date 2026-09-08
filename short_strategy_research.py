
import os
import csv
import time
import bisect
import zipfile
import threading
from collections import deque, defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median

import numpy as np
import requests
from flask import Flask, jsonify, send_file


# ============================================================
# USD/JPY M15 LONG — FINAL DEEP VALIDATION ONLY
#
# PURPOSE
# -------
# No new optimisation.
# No broad search.
# No interaction grid.
#
# This script performs deep robustness only on:
#
# 1) Frozen benchmark
# 2) Sweep30 / RR4.00
# 3) Sweep30 / RR4.25
# 4) Sweep30 / RR4.50
# 5) Sweep20 / RR4.25
# 6) Sweep40 / RR4.25
#
# All new candidates use:
#   bullish candle
#   NO body-ratio requirement
#   body >= 1.25 ATR14
#   close > previous M15 high
#   lower wick/body >= 0.25
#   prior 4h momentum <= -1.75 ATR14
#   previous completed H1 ATR14 / H1 50-ATR mean >= 0.80
#   no time filter
#   no weekday filter
#   stop = signal low - 10 ticks
#   historical adverse cost baseline = 1 pip
#   pyramiding = 0
#
# CRITICAL PARITY RULE
# --------------------
# Frozen benchmark body-ratio convention:
#   previous candle zero body -> body_ratio = 999
#
# Frozen benchmark MUST reproduce:
#   FULL      100 trades
#   PRE2010    25 trades
#   2010+      75 trades
#
# If parity fails, script stops before deep validation.
#
# OUTPUTS
# -------
# - parity
# - candidate comparison
# - full period stats
# - 0.5/1/1.5/2 pip cost stress
# - rolling 12/24/36M
# - rolling summaries
# - calendar years
# - calendar summaries
# - trade list
#
# ONE ZIP ROUTE
# -------------
# /usdjpy-m15-long-final-deep/results
#
# READ ONLY. NEVER SENDS ORDERS.
# ============================================================


app = Flask(__name__)

TOKEN = os.getenv("OANDA_TOKEN")
BASE = os.getenv(
    "OANDA_API_URL",
    "https://api-fxtrade.oanda.com",
)

PAIR = "USD_JPY"

START = datetime(
    2002, 5, 6, 20, 0,
    tzinfo=timezone.utc,
)

NOW = (
    datetime.now(timezone.utc)
    .replace(second=0, microsecond=0)
)

H1_WARMUP_START = (
    START - timedelta(days=900)
)

TICK_SIZE = 0.001
PIP_SIZE = 0.01

STOP_BUFFER_TICKS = 10
PRIMARY_COST_PIPS = 1.00

COST_GRID = [
    0.50,
    1.00,
    1.50,
    2.00,
]


# ============================================================
# CONFIGURATIONS
# ============================================================

FROZEN = {
    "config_id":
        "FROZEN_BENCHMARK",

    "sweep_mode":
        "ANY_20_40_60_100",

    "sweep_lb":
        None,

    "br_min":
        1.00,

    "body_atr_min":
        1.25,

    "lower_wick_body_min":
        0.25,

    "mom4_max":
        -1.75,

    "h1_atr_ratio_min":
        None,

    "rr":
        4.00,
}


def candidate(
    config_id,
    sweep_lb,
    rr,
):
    return {
        "config_id":
            config_id,

        "sweep_mode":
            "SINGLE",

        "sweep_lb":
            sweep_lb,

        "br_min":
            None,

        "body_atr_min":
            1.25,

        "lower_wick_body_min":
            0.25,

        "mom4_max":
            -1.75,

        "h1_atr_ratio_min":
            0.80,

        "rr":
            rr,
    }


CONFIGS = [
    FROZEN,

    candidate(
        "SWEEP30_RR4.00",
        30,
        4.00,
    ),

    candidate(
        "SWEEP30_RR4.25",
        30,
        4.25,
    ),

    candidate(
        "SWEEP30_RR4.50",
        30,
        4.50,
    ),

    candidate(
        "SWEEP20_RR4.25",
        20,
        4.25,
    ),

    candidate(
        "SWEEP40_RR4.25",
        40,
        4.25,
    ),
]


# ============================================================
# OUTPUTS
# ============================================================

OUT_COVERAGE = (
    "usdjpy_m15_long_final_deep_coverage.csv"
)

OUT_PARITY = (
    "usdjpy_m15_long_final_deep_parity.csv"
)

OUT_COMPARISON = (
    "usdjpy_m15_long_final_deep_comparison.csv"
)

OUT_PERIODS = (
    "usdjpy_m15_long_final_deep_periods.csv"
)

OUT_COST = (
    "usdjpy_m15_long_final_deep_cost_stress.csv"
)

OUT_ROLLING = (
    "usdjpy_m15_long_final_deep_rolling.csv"
)

OUT_ROLLING_SUMMARY = (
    "usdjpy_m15_long_final_deep_rolling_summary.csv"
)

OUT_CALENDAR = (
    "usdjpy_m15_long_final_deep_calendar_years.csv"
)

OUT_CALENDAR_SUMMARY = (
    "usdjpy_m15_long_final_deep_calendar_summary.csv"
)

OUT_TRADES = (
    "usdjpy_m15_long_final_deep_trades.csv"
)

OUT_BUNDLE = (
    "USDJPY_M15_LONG_FINAL_DEEP_VALIDATION_RESULTS.zip"
)


STATUS = {
    "state":
        "not_started",

    "message":
        "USD/JPY M15 LONG final deep validation not started",

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
        dt.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
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

        sign = None
        offset = None

        if "+" in right:
            fraction, offset = right.split(
                "+",
                1,
            )
            sign = "+"

        elif "-" in right:
            fraction, offset = right.split(
                "-",
                1,
            )
            sign = "-"

        else:
            fraction = right

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
        )

        if sign is not None:
            value += (
                sign
                + offset
            )

    return (
        datetime.fromisoformat(value)
        .astimezone(timezone.utc)
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


def build_bundle():
    files = [
        OUT_COVERAGE,
        OUT_PARITY,
        OUT_COMPARISON,
        OUT_PERIODS,
        OUT_COST,
        OUT_ROLLING,
        OUT_ROLLING_SUMMARY,
        OUT_CALENDAR,
        OUT_CALENDAR_SUMMARY,
        OUT_TRADES,
    ]

    with zipfile.ZipFile(
        OUT_BUNDLE,
        "w",
        compression=zipfile.ZIP_DEFLATED,
    ) as archive:
        for path in files:
            if os.path.exists(path):
                archive.write(
                    path,
                    arcname=os.path.basename(path),
                )


def safe_median(values):
    values = list(values)

    return (
        median(values)
        if values
        else 0.0
    )


def month_floor(dt):
    return datetime(
        dt.year,
        dt.month,
        1,
        tzinfo=timezone.utc,
    )


def add_months(dt, months):
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


# ============================================================
# OANDA
# ============================================================

def headers():
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
    granularity,
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
            granularity,

        "smooth":
            "false",

        "from":
            iso_utc(start),

        "to":
            iso_utc(end),

        "includeFirst":
            "true",
    }

    response = requests.get(
        url,
        headers=headers(),
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

        mid = item["mid"]

        rows.append({
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

    return rows


def fetch_history(
    granularity,
    start,
    end,
    chunk_days,
):
    cursor = start
    by_time = {}
    chunk_no = 0

    while cursor < end:
        chunk_no += 1

        chunk_end = min(
            cursor
            + timedelta(
                days=chunk_days
            ),
            end,
        )

        STATUS.update({
            "state":
                "fetching",

            "message": (
                f"Fetching {granularity} "
                f"{chunk_no}: "
                f"{iso_utc(cursor)} -> "
                f"{iso_utc(chunk_end)}"
            ),
        })

        try:
            rows = fetch_chunk(
                granularity,
                cursor,
                chunk_end,
            )

        except requests.HTTPError as error:
            status_code = (
                error.response.status_code
                if error.response is not None
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

        cursor = chunk_end

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

def true_ranges(candles):
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


def rma(values, length):
    result = np.full(
        len(values),
        np.nan,
        dtype=float,
    )

    if len(values) < length:
        return result

    seed = values[:length]

    if np.isnan(seed).any():
        return result

    result[
        length - 1
    ] = (
        np.mean(seed)
    )

    for i in range(
        length,
        len(values),
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


def atr14(candles):
    return rma(
        true_ranges(candles),
        14,
    )


def sma_np(values, length):
    result = np.full(
        len(values),
        np.nan,
        dtype=float,
    )

    vals = np.nan_to_num(
        values,
        nan=0.0,
    )

    valid = np.isfinite(
        values
    ).astype(
        int
    )

    csum = np.cumsum(
        vals
    )

    ccount = np.cumsum(
        valid
    )

    for i in range(
        length - 1,
        len(values),
    ):
        total = csum[i]
        count = ccount[i]

        if i >= length:
            total -= csum[
                i - length
            ]

            count -= ccount[
                i - length
            ]

        if count == length:
            result[i] = (
                total
                / length
            )

    return result


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
            i - lookback
        )

        while (
            dq
            and dq[0] < oldest
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
            ] >= values[i]
        ):
            dq.pop()

        dq.append(
            i
        )

    return result


# ============================================================
# H1 NO-LOOKAHEAD STATE
# ============================================================

def build_h1_state(h1):
    atr = atr14(
        h1
    )

    atr_mean50 = sma_np(
        atr,
        50,
    )

    rows = []

    for i, candle in enumerate(
        h1
    ):
        complete_at = (
            h1[
                i + 1
            ]["time"]
            if (
                i + 1
                < len(h1)
            )
            else None
        )

        ratio = None

        if (
            np.isfinite(
                atr[i]
            )
            and np.isfinite(
                atr_mean50[i]
            )
            and atr_mean50[i] > 0
        ):
            ratio = (
                atr[i]
                / atr_mean50[i]
            )

        rows.append({
            "complete_at":
                complete_at,

            "atr_ratio50":
                ratio,
        })

    return rows


def align_h1_to_m15(
    m15_times,
    h1_state,
):
    eligible = [
        row
        for row in h1_state
        if row[
            "complete_at"
        ] is not None
    ]

    completion_times = [
        row[
            "complete_at"
        ]
        for row in eligible
    ]

    result = np.full(
        len(m15_times),
        np.nan,
        dtype=float,
    )

    for i, signal_time in enumerate(
        m15_times
    ):
        position = (
            bisect.bisect_right(
                completion_times,
                signal_time,
            )
            - 1
        )

        if position < 0:
            continue

        value = (
            eligible[
                position
            ][
                "atr_ratio50"
            ]
        )

        if value is not None:
            result[
                i
            ] = value

    return result


# ============================================================
# FEATURE CACHE
# ============================================================

SWEEP_LOOKBACKS = [
    20,
    30,
    40,
    60,
    100,
]


def build_features(
    m15,
    h1_atr_ratio50,
):
    n = len(
        m15
    )

    opens = np.array(
        [
            candle["open"]
            for candle in m15
        ],
        dtype=float,
    )

    highs = np.array(
        [
            candle["high"]
            for candle in m15
        ],
        dtype=float,
    )

    lows = np.array(
        [
            candle["low"]
            for candle in m15
        ],
        dtype=float,
    )

    closes = np.array(
        [
            candle["close"]
            for candle in m15
        ],
        dtype=float,
    )

    atr = atr14(
        m15
    )

    bullish = (
        closes
        > opens
    )

    current_body = (
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
        closes[:-1]
        - opens[:-1]
    )

    body_ratio = np.full(
        n,
        np.nan,
        dtype=float,
    )

    positive_prev = (
        previous_body
        > 0
    )

    body_ratio[
        positive_prev
    ] = (
        current_body[
            positive_prev
        ]
        / previous_body[
            positive_prev
        ]
    )

    # Frozen parity convention.
    zero_prev = (
        previous_body
        == 0
    )

    body_ratio[
        zero_prev
    ] = 999.0

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
        current_body[
            valid_atr
        ]
        / atr[
            valid_atr
        ]
    )

    lower_wick = (
        np.minimum(
            opens,
            closes,
        )
        - lows
    )

    lower_wick_body = np.full(
        n,
        np.nan,
        dtype=float,
    )

    valid_body = (
        current_body
        > 0
    )

    lower_wick_body[
        valid_body
    ] = (
        lower_wick[
            valid_body
        ]
        / current_body[
            valid_body
        ]
    )

    previous_high = np.full(
        n,
        np.nan,
        dtype=float,
    )

    previous_high[
        1:
    ] = highs[
        :-1
    ]

    previous_lows = {}

    for lookback in SWEEP_LOOKBACKS:
        previous_lows[
            lookback
        ] = (
            rolling_previous_low(
                lows,
                lookback,
            )
        )

    # Strict PRIOR 4-hour momentum.
    mom4 = np.full(
        n,
        np.nan,
        dtype=float,
    )

    for i in range(
        17,
        n,
    ):
        if valid_atr[i]:
            mom4[i] = (
                closes[
                    i - 1
                ]
                - closes[
                    i - 17
                ]
            ) / atr[i]

    return {
        "n":
            n,

        "bullish":
            bullish,

        "body_ratio":
            body_ratio,

        "body_atr":
            body_atr,

        "lower_wick_body":
            lower_wick_body,

        "previous_high":
            previous_high,

        "previous_lows":
            previous_lows,

        "mom4":
            mom4,

        "h1_atr_ratio50":
            h1_atr_ratio50,

        "low":
            lows,

        "close":
            closes,
    }


# ============================================================
# SIGNALS
# ============================================================

def signal_indices(
    cfg,
    f,
):
    mask = (
        f[
            "bullish"
        ].copy()
    )

    if cfg.get(
        "br_min"
    ) is not None:
        mask &= (
            f[
                "body_ratio"
            ]
            >= cfg[
                "br_min"
            ]
        )

    mask &= (
        f[
            "body_atr"
        ]
        >= cfg[
            "body_atr_min"
        ]
    )

    mask &= (
        f[
            "lower_wick_body"
        ]
        >= cfg[
            "lower_wick_body_min"
        ]
    )

    mask &= (
        f[
            "mom4"
        ]
        <= cfg[
            "mom4_max"
        ]
    )

    mask &= (
        f[
            "close"
        ]
        > f[
            "previous_high"
        ]
    )

    if (
        cfg[
            "sweep_mode"
        ]
        == "ANY_20_40_60_100"
    ):
        sweep = (
            (
                f["low"]
                < f[
                    "previous_lows"
                ][20]
            )
            | (
                f["low"]
                < f[
                    "previous_lows"
                ][40]
            )
            | (
                f["low"]
                < f[
                    "previous_lows"
                ][60]
            )
            | (
                f["low"]
                < f[
                    "previous_lows"
                ][100]
            )
        )

        mask &= sweep

    else:
        lookback = (
            cfg[
                "sweep_lb"
            ]
        )

        mask &= (
            f[
                "low"
            ]
            < f[
                "previous_lows"
            ][
                lookback
            ]
        )

    threshold = cfg.get(
        "h1_atr_ratio_min"
    )

    if threshold is not None:
        mask &= (
            f[
                "h1_atr_ratio50"
            ]
            >= threshold
        )

    mask[
        :220
    ] = False

    return np.flatnonzero(
        mask
    ).tolist()


# ============================================================
# HISTORICAL LONG ENGINE
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
        signal[
            "close"
        ]
    )

    stop = (
        signal[
            "low"
        ]
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
        + rr
        * reference_risk
    )

    fill = (
        reference_entry
        + cost_pips
        * PIP_SIZE
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
            candles[
                j
            ]
        )

        hit_stop = (
            candle[
                "low"
            ]
            <= stop
        )

        hit_target = (
            candle[
                "high"
            ]
            >= target
        )

        if (
            hit_stop
            and hit_target
        ):
            high_distance = abs(
                candle[
                    "high"
                ]
                - candle[
                    "open"
                ]
            )

            low_distance = abs(
                candle[
                    "open"
                ]
                - candle[
                    "low"
                ]
            )

            if (
                high_distance
                < low_distance
            ):
                exit_price = target
                reason = "TARGET"

            else:
                exit_price = stop
                reason = "STOP"

        elif hit_target:
            exit_price = target
            reason = "TARGET"

        elif hit_stop:
            exit_price = stop
            reason = "STOP"

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

    if key not in OUTCOME_CACHE:
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
                index
            ][
                "time"
            ]
            for index in indices
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

        # Exact exit-candle signal eligible.
        position = bisect.bisect_left(
            use,
            trade[
                "exit_index"
            ],
            lo=position + 1,
        )

    return trades


# ============================================================
# STATS
# ============================================================

def stats_from_trades(
    trades
):
    values = [
        float(
            trade[
                "result_r"
            ]
        )
        for trade in trades
    ]

    winners = [
        value
        for value in values
        if value > 0
    ]

    losers = [
        value
        for value in values
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
        pf = (
            gross_profit
            / gross_loss
        )

    elif gross_profit > 0:
        pf = 999.0

    else:
        pf = 0.0

    total_r = sum(
        values
    )

    equity = 0.0
    peak = 0.0
    max_dd = 0.0

    streak = 0
    longest = 0

    for value in values:
        equity += value

        peak = max(
            peak,
            equity,
        )

        max_dd = min(
            max_dd,
            equity - peak,
        )

        if value < 0:
            streak += 1

            longest = max(
                longest,
                streak,
            )

        else:
            streak = 0

    return {
        "trades":
            len(
                values
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
                    values
                )
                * 100.0
                if values
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
                    values
                )
                if values
                else 0.0
            ),

        "max_drawdown_r":
            max_dd,

        "longest_loss_streak":
            longest,
    }


ERAS = [
    (
        "ERA_2002_2007",
        START,
        datetime(
            2008,
            1,
            1,
            tzinfo=timezone.utc,
        ),
    ),

    (
        "ERA_2008_2013",
        datetime(
            2008,
            1,
            1,
            tzinfo=timezone.utc,
        ),
        datetime(
            2014,
            1,
            1,
            tzinfo=timezone.utc,
        ),
    ),

    (
        "ERA_2014_2019",
        datetime(
            2014,
            1,
            1,
            tzinfo=timezone.utc,
        ),
        datetime(
            2020,
            1,
            1,
            tzinfo=timezone.utc,
        ),
    ),

    (
        "ERA_2020_NOW",
        datetime(
            2020,
            1,
            1,
            tzinfo=timezone.utc,
        ),
        NOW,
    ),
]


def evaluation_row(
    cfg,
    candles,
    indices,
):
    full = stats_from_trades(
        run_backtest(
            candles,
            indices,
            cfg[
                "rr"
            ],
            PRIMARY_COST_PIPS,
            candles[
                0
            ][
                "time"
            ],
            NOW,
        )
    )

    pre = stats_from_trades(
        run_backtest(
            candles,
            indices,
            cfg[
                "rr"
            ],
            PRIMARY_COST_PIPS,
            candles[
                0
            ][
                "time"
            ],
            datetime(
                2010,
                1,
                1,
                tzinfo=timezone.utc,
            ),
        )
    )

    post = stats_from_trades(
        run_backtest(
            candles,
            indices,
            cfg[
                "rr"
            ],
            PRIMARY_COST_PIPS,
            datetime(
                2010,
                1,
                1,
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
                    cfg[
                        "rr"
                    ],
                    PRIMARY_COST_PIPS,
                    start,
                    end,
                )
            )
        )

    row = {
        "config_id":
            cfg[
                "config_id"
            ],

        "sweep_mode":
            cfg[
                "sweep_mode"
            ],

        "sweep_lb":
            cfg.get(
                "sweep_lb"
            ),

        "br_min":
            cfg.get(
                "br_min"
            ),

        "body_atr_min":
            cfg[
                "body_atr_min"
            ],

        "lower_wick_body_min":
            cfg[
                "lower_wick_body_min"
            ],

        "mom4_max":
            cfg[
                "mom4_max"
            ],

        "h1_atr_ratio_min":
            cfg.get(
                "h1_atr_ratio_min"
            ),

        "rr":
            cfg[
                "rr"
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
    }

    for i, stats in enumerate(
        era_stats,
        1,
    ):
        row[
            f"era{i}_trades"
        ] = stats[
            "trades"
        ]

        row[
            f"era{i}_pf"
        ] = round(
            stats[
                "profit_factor"
            ],
            6,
        )

        row[
            f"era{i}_r"
        ] = round(
            stats[
                "total_r"
            ],
            4,
        )

    return row


# ============================================================
# DEEP OUTPUTS
# ============================================================

def stats_row(
    cfg,
    period,
    trades,
):
    stats = stats_from_trades(
        trades
    )

    return {
        "config_id":
            cfg[
                "config_id"
            ],

        "period":
            period,

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


def period_rows(
    cfg,
    candles,
    indices,
):
    periods = [
        (
            "FULL_HISTORY",
            candles[
                0
            ][
                "time"
            ],
            NOW,
        ),

        (
            "PRE_2010",
            candles[
                0
            ][
                "time"
            ],
            datetime(
                2010,
                1,
                1,
                tzinfo=timezone.utc,
            ),
        ),

        (
            "2010_PLUS",
            datetime(
                2010,
                1,
                1,
                tzinfo=timezone.utc,
            ),
            NOW,
        ),

        *ERAS,

        (
            "DEV_2002_2017",
            candles[
                0
            ][
                "time"
            ],
            datetime(
                2018,
                1,
                1,
                tzinfo=timezone.utc,
            ),
        ),

        (
            "VALIDATION_2018_PLUS",
            datetime(
                2018,
                1,
                1,
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
            cfg[
                "rr"
            ],
            PRIMARY_COST_PIPS,
            start,
            end,
        )

        row = stats_row(
            cfg,
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
    cfg,
    candles,
    indices,
):
    rows = []

    periods = [
        (
            "FULL_HISTORY",
            candles[
                0
            ][
                "time"
            ],
            NOW,
        ),

        (
            "PRE_2010",
            candles[
                0
            ][
                "time"
            ],
            datetime(
                2010,
                1,
                1,
                tzinfo=timezone.utc,
            ),
        ),

        (
            "2010_PLUS",
            datetime(
                2010,
                1,
                1,
                tzinfo=timezone.utc,
            ),
            NOW,
        ),
    ]

    for (
        period,
        start,
        end,
    ) in periods:
        for cost in COST_GRID:
            trades = run_backtest(
                candles,
                indices,
                cfg[
                    "rr"
                ],
                cost,
                start,
                end,
            )

            row = stats_row(
                cfg,
                period,
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
    cfg,
    candles,
    indices,
):
    rows = []

    first_month = month_floor(
        max(
            candles[
                0
            ][
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
        start = first_month

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

            stats = stats_from_trades(
                run_backtest(
                    candles,
                    indices,
                    cfg[
                        "rr"
                    ],
                    PRIMARY_COST_PIPS,
                    start,
                    end,
                )
            )

            rows.append({
                "config_id":
                    cfg[
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

                "positive":
                    stats[
                        "total_r"
                    ] > 0,

                "zero_trade":
                    stats[
                        "trades"
                    ] == 0,
            })

            start = add_months(
                start,
                1,
            )

    return rows


def rolling_summary_rows(
    rows
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

        positive_active = [
            row
            for row in active
            if row[
                "positive"
            ]
        ]

        positive_all = [
            row
            for row in subset
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
                        positive_all
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

            "median_r_active":
                round(
                    safe_median([
                        row[
                            "total_r"
                        ]
                        for row in active
                    ]),
                    4,
                ),

            "median_pf_active":
                round(
                    safe_median([
                        row[
                            "profit_factor"
                        ]
                        for row in active
                    ]),
                    6,
                ),

            "worst_r":
                round(
                    min(
                        row[
                            "total_r"
                        ]
                        for row in subset
                    ),
                    4,
                ),

            "best_r":
                round(
                    max(
                        row[
                            "total_r"
                        ]
                        for row in subset
                    ),
                    4,
                ),
        })

    return output


def calendar_rows(
    cfg,
    candles,
    indices,
):
    rows = []

    first_year = max(
        candles[
            0
        ][
            "time"
        ].year,
        START.year,
    )

    last_year = (
        NOW.year
        - 1
    )

    for year in range(
        first_year,
        last_year + 1,
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

        stats = stats_from_trades(
            run_backtest(
                candles,
                indices,
                cfg[
                    "rr"
                ],
                PRIMARY_COST_PIPS,
                start,
                end,
            )
        )

        rows.append({
            "config_id":
                cfg[
                    "config_id"
                ],

            "year":
                year,

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

            "positive":
                stats[
                    "total_r"
                ] > 0,

            "negative":
                stats[
                    "total_r"
                ] < 0,

            "zero_trade":
                stats[
                    "trades"
                ] == 0,
        })

    return rows


def calendar_summary_rows(
    rows
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

            "negative_years":
                len(
                    negative
                ),

            "median_trades_year":
                round(
                    safe_median([
                        row[
                            "trades"
                        ]
                        for row in subset
                    ]),
                    4,
                ),

            "median_year_r":
                round(
                    safe_median([
                        row[
                            "total_r"
                        ]
                        for row in subset
                    ]),
                    4,
                ),

            "worst_year_r":
                round(
                    min(
                        row[
                            "total_r"
                        ]
                        for row in subset
                    ),
                    4,
                ),

            "best_year_r":
                round(
                    max(
                        row[
                            "total_r"
                        ]
                        for row in subset
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
        m15 = fetch_history(
            "M15",
            START,
            NOW,
            35,
        )

        h1 = fetch_history(
            "H1",
            H1_WARMUP_START,
            NOW,
            180,
        )

        if not m15 or not h1:
            raise RuntimeError(
                "Missing USDJPY M15/H1 history"
            )

        write_csv(
            OUT_COVERAGE,
            [{
                "instrument":
                    PAIR,

                "requested_start_utc":
                    iso_utc(
                        START
                    ),

                "actual_first_m15_utc":
                    iso_utc(
                        m15[
                            0
                        ][
                            "time"
                        ]
                    ),

                "actual_last_m15_utc":
                    iso_utc(
                        m15[
                            -1
                        ][
                            "time"
                        ]
                    ),

                "m15_candles":
                    len(
                        m15
                    ),

                "h1_candles":
                    len(
                        h1
                    ),
            }],
        )

        STATUS.update({
            "state":
                "precomputing",

            "message":
                "Building corrected M15 features and completed-H1 volatility state",
        })

        h1_aligned = align_h1_to_m15(
            [
                candle[
                    "time"
                ]
                for candle in m15
            ],
            build_h1_state(
                h1
            ),
        )

        features = build_features(
            m15,
            h1_aligned,
        )

        # ----------------------------------------------------
        # Parity gate
        # ----------------------------------------------------
        frozen_indices = signal_indices(
            FROZEN,
            features,
        )

        frozen_eval = evaluation_row(
            FROZEN,
            m15,
            frozen_indices,
        )

        parity_ok = (
            frozen_eval[
                "full_trades"
            ] == 100
            and frozen_eval[
                "pre2010_trades"
            ] == 25
            and frozen_eval[
                "post2010_trades"
            ] == 75
        )

        write_csv(
            OUT_PARITY,
            [{
                "config_id":
                    FROZEN[
                        "config_id"
                    ],

                "expected_full_trades":
                    100,

                "actual_full_trades":
                    frozen_eval[
                        "full_trades"
                    ],

                "expected_pre2010_trades":
                    25,

                "actual_pre2010_trades":
                    frozen_eval[
                        "pre2010_trades"
                    ],

                "expected_post2010_trades":
                    75,

                "actual_post2010_trades":
                    frozen_eval[
                        "post2010_trades"
                    ],

                "status":
                    (
                        "MATCH"
                        if parity_ok
                        else "FAIL_PARITY_DO_NOT_TRUST"
                    ),
            }],
        )

        if not parity_ok:
            raise RuntimeError(
                "Frozen USDJPY M15 LONG parity failed. "
                "Deep validation stopped."
            )

        # ----------------------------------------------------
        # Exact candidate comparison
        # ----------------------------------------------------
        comparison_rows = []
        indices_by_id = {}

        for cfg in CONFIGS:
            STATUS.update({
                "state":
                    "comparison",

                "message":
                    f"Evaluating {cfg['config_id']}",
            })

            indices = signal_indices(
                cfg,
                features,
            )

            indices_by_id[
                cfg[
                    "config_id"
                ]
            ] = indices

            comparison_rows.append(
                evaluation_row(
                    cfg,
                    m15,
                    indices,
                )
            )

        write_csv(
            OUT_COMPARISON,
            comparison_rows,
        )

        # ----------------------------------------------------
        # Deep validation
        # ----------------------------------------------------
        period_output = []
        cost_output = []
        rolling_output = []
        calendar_output = []
        trade_output = []

        for i, cfg in enumerate(
            CONFIGS,
            1,
        ):
            STATUS.update({
                "state":
                    "deep_validation",

                "message": (
                    f"Deep validation "
                    f"{i}/{len(CONFIGS)} "
                    f"{cfg['config_id']}"
                ),
            })

            indices = (
                indices_by_id[
                    cfg[
                        "config_id"
                    ]
                ]
            )

            period_output.extend(
                period_rows(
                    cfg,
                    m15,
                    indices,
                )
            )

            cost_output.extend(
                cost_rows(
                    cfg,
                    m15,
                    indices,
                )
            )

            rolling_output.extend(
                rolling_rows(
                    cfg,
                    m15,
                    indices,
                )
            )

            calendar_output.extend(
                calendar_rows(
                    cfg,
                    m15,
                    indices,
                )
            )

            full_trades = run_backtest(
                m15,
                indices,
                cfg[
                    "rr"
                ],
                PRIMARY_COST_PIPS,
                m15[
                    0
                ][
                    "time"
                ],
                NOW,
            )

            for trade in full_trades:
                row = dict(
                    trade
                )

                row[
                    "config_id"
                ] = cfg[
                    "config_id"
                ]

                trade_output.append(
                    row
                )

        write_csv(
            OUT_PERIODS,
            period_output,
        )

        write_csv(
            OUT_COST,
            cost_output,
        )

        write_csv(
            OUT_ROLLING,
            rolling_output,
        )

        write_csv(
            OUT_ROLLING_SUMMARY,
            rolling_summary_rows(
                rolling_output
            ),
        )

        write_csv(
            OUT_CALENDAR,
            calendar_output,
        )

        write_csv(
            OUT_CALENDAR_SUMMARY,
            calendar_summary_rows(
                calendar_output
            ),
        )

        write_csv(
            OUT_TRADES,
            trade_output,
        )

        STATUS.update({
            "state":
                "packaging",

            "message":
                "Building single ZIP results bundle",
        })

        build_bundle()

        STATUS.update({
            "state":
                "complete",

            "message":
                "USD/JPY M15 LONG final deep validation complete",

            "benchmark_parity":
                "MATCH",

            "configs_tested":
                len(
                    CONFIGS
                ),

            "results_bundle":
                OUT_BUNDLE,
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
            "USDJPY M15 LONG Final Deep Validation",

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

        "configs": [
            cfg[
                "config_id"
            ]
            for cfg in CONFIGS
        ],

        "parity_gate": {
            "full":
                100,

            "pre2010":
                25,

            "post2010":
                75,
        },

        "orders_supported":
            False,

        "trading_enabled":
            False,

        "routes": [
            "/usdjpy-m15-long-final-deep/status",
            "/usdjpy-m15-long-final-deep/results",
        ],
    })


@app.route(
    "/usdjpy-m15-long-final-deep/status"
)
def route_status():
    return jsonify(
        STATUS
    )


@app.route(
    "/usdjpy-m15-long-final-deep/results"
)
def route_results():
    if not os.path.exists(
        OUT_BUNDLE
    ):
        return jsonify({
            "error":
                "Results not ready yet",
        }), 404

    return send_file(
        os.path.abspath(
            OUT_BUNDLE
        ),
        as_attachment=True,
        download_name=OUT_BUNDLE,
    )


if __name__ == "__main__":
    thread = threading.Thread(
        target=run_research,
        name=(
            "usdjpy-m15-long-"
            "final-deep"
        ),
        daemon=True,
    )

    thread.start()

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
