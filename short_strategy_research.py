
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

import numpy as np
import requests
from flask import Flask, jsonify, send_file


# ============================================================
# USD/JPY M15 LONG — FINAL HEAD-TO-HEAD CONFIRMATION
#
# PURPOSE
# -------
# Final focused confirmation comparing:
#
# A) the existing frozen full-history benchmark
# B) the new, more selective 40-bar sweep + H1 volatility
#    candidate found in the full-history re-examination
#
# This is NOT another broad archetype search.
#
# CRITICAL PARITY FIX
# -------------------
# The previous vectorised research script treated a zero-body
# previous candle as NaN for body ratio.
#
# The frozen historical implementation instead treats:
#
#     previous_body == 0  ->  body_ratio = 999
#
# That convention is restored here.
#
# Frozen benchmark must reproduce:
#
#     FULL      100 trades
#     PRE2010    25 trades
#     2010+      75 trades
#
# before any replacement decision is trusted.
#
# ============================================================
#
# FROZEN BENCHMARK
# ----------------
# current candle bullish
# current body / abs(previous body) >= 1.00
#   with previous_body==0 -> body_ratio=999
#
# body >= 1.25 ATR14
#
# sweep ANY prior:
#   20-bar low
#   40-bar low
#   60-bar low
#   100-bar low
#
# close > previous candle high
# lower wick / body >= 0.25
#
# strict PRIOR 4-hour momentum:
#   (close[i-1] - close[i-17]) / ATR14[i] <= -1.75
#
# no H1/H4/D filter
# no time filter
# no weekday filter
#
# RR 4.00
# stop = signal low - 10 ticks
# 1 pip adverse historical cost
# pyramiding 0
#
# ============================================================
#
# NEW CANDIDATE ANCHOR
# --------------------
# current candle bullish
# NO body-ratio requirement
# body >= 1.25 ATR14
# sweep prior 40-bar low
# close > previous candle high
# lower wick / body >= 0.25
# strict PRIOR 4-hour momentum <= -1.75 ATR14
#
# previous COMPLETED H1 ATR14 / H1 50-period mean ATR >= 0.80
#
# no time filter
# no weekday filter
# RR 4.00
#
# Prior broad reference:
#   ~68 trades
#   PF ~3.35
#   +84.62R
#   DD ~-5R
#
# ============================================================
#
# FOCUSED TESTS
# -------------
#
# Sweep lookback:
#   20 / 30 / 40 / 50 / 60
#
# H1 ATR regime:
#   NONE / 0.70 / 0.80 / 0.90 / 1.00
#
# Prior 4h momentum:
#   -1.50 / -1.625 / -1.75 / -1.875 / -2.00
#
# Body ATR:
#   1.10 / 1.20 / 1.25 / 1.30 / 1.40
#
# Lower wick/body:
#   0.15 / 0.20 / 0.25 / 0.30 / 0.35
#
# Body ratio:
#   NONE / 1.00 / 1.10 / 1.20
#
# RR:
#   3.50 / 3.75 / 4.00 / 4.25 / 4.50 / 4.75 / 5.00
#
# ============================================================
#
# LOCAL INTERACTION GRID
# ----------------------
# Sweep:
#   30 / 40 / 50
#
# H1 ATR:
#   NONE / 0.70 / 0.80 / 0.90
#
# Momentum:
#   -1.50 / -1.75 / -2.00
#
# Body:
#   1.15 / 1.25 / 1.35
#
# Wick:
#   0.20 / 0.25 / 0.30
#
# BR:
#   NONE / 1.00
#
# RR:
#   3.75 / 4.00 / 4.25 / 4.50 / 4.75
#
# total = 3240 configurations
#
# ============================================================
#
# DEEP ROBUSTNESS
# ---------------
# anchor + frozen benchmark + selected finalists:
#
# - full history
# - pre-2010
# - 2010+
# - 2002-07
# - 2008-13
# - 2014-19
# - 2020-now
# - 2002-17 vs 2018+
# - last 5Y / last 2Y
# - 0.5 / 1 / 1.5 / 2 pip cost
# - rolling 12 / 24 / 36M
# - completed calendar years
#
# ============================================================
#
# HISTORICAL CONVENTIONS
# ----------------------
# OANDA midpoint.
# ATR14 Wilder/RMA, SMA seeded.
#
# USDJPY:
#   tick = .001
#   pip  = .01
#
# signal timestamp = M15 candle OPEN
# reference entry = signal close
# historical long fill = close + adverse cost
# stop = signal low - 10 ticks
# target uses REFERENCE signal-close risk
#
# exit starts next candle
# pyramiding 0
# exact exit-candle signal eligible
#
# same-bar LONG tie:
#   if high is closer to candle open => TARGET first
#   otherwise STOP first
#
# ============================================================
#
# H1 NO LOOKAHEAD
# ----------------
# Each H1 row becomes available only at the NEXT ACTUAL H1
# candle open.
#
# lookup:
#   bisect_right(completion_times, signal_time) - 1
#
# ============================================================
#
# ONE ZIP ROUTE
# -------------
# /usdjpy-m15-long-final-confirmation/results
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
# LOCKED CONTROLS
# ============================================================

FROZEN_BENCHMARK = {
    "config_id":
        "FROZEN_BENCHMARK_FULL_HISTORY",

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


NEW_ANCHOR = {
    "config_id":
        "NEW_ANCHOR_SWEEP40_H1ATR080",

    "sweep_mode":
        "SINGLE",

    "sweep_lb":
        40,

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
        4.00,
}


# ============================================================
# OUTPUTS
# ============================================================

OUT_COVERAGE = (
    "usdjpy_m15_long_final_confirmation_coverage.csv"
)

OUT_PARITY = (
    "usdjpy_m15_long_final_confirmation_parity.csv"
)

OUT_CONTROLS = (
    "usdjpy_m15_long_final_confirmation_controls.csv"
)

OUT_SLICES = (
    "usdjpy_m15_long_final_confirmation_one_way_slices.csv"
)

OUT_GRID = (
    "usdjpy_m15_long_final_confirmation_local_grid.csv"
)

OUT_FINALISTS = (
    "usdjpy_m15_long_final_confirmation_finalists.csv"
)

OUT_PERIODS = (
    "usdjpy_m15_long_final_confirmation_periods.csv"
)

OUT_COST = (
    "usdjpy_m15_long_final_confirmation_cost_stress.csv"
)

OUT_ROLLING = (
    "usdjpy_m15_long_final_confirmation_rolling.csv"
)

OUT_ROLLING_SUMMARY = (
    "usdjpy_m15_long_final_confirmation_rolling_summary.csv"
)

OUT_CALENDAR = (
    "usdjpy_m15_long_final_confirmation_calendar_years.csv"
)

OUT_CALENDAR_SUMMARY = (
    "usdjpy_m15_long_final_confirmation_calendar_summary.csv"
)

OUT_TRADES = (
    "usdjpy_m15_long_final_confirmation_trades.csv"
)

OUT_BUNDLE = (
    "USDJPY_M15_LONG_FINAL_CONFIRMATION_RESULTS.zip"
)

STATUS = {
    "state":
        "not_started",

    "message":
        "USD/JPY M15 LONG final confirmation not started",

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
        value = value[:-1] + "+00:00"

    if "." in value:
        left, right = value.split(".", 1)

        sign = None
        offset = None

        if "+" in right:
            fraction, offset = right.split("+", 1)
            sign = "+"

        elif "-" in right:
            fraction, offset = right.split("-", 1)
            sign = "-"

        else:
            fraction = right

        fraction = (
            fraction[:6]
            .ljust(6, "0")
        )

        value = left + "." + fraction

        if sign is not None:
            value += sign + offset

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
        OUT_CONTROLS,
        OUT_SLICES,
        OUT_GRID,
        OUT_FINALISTS,
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
    return median(values) if values else 0.0


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
            "Bearer " + TOKEN.strip(),
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
            cursor + timedelta(days=chunk_days),
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

            if status_code in (400, 404):
                rows = []
            else:
                raise

        for row in rows:
            by_time[
                row["time"]
            ] = row

        cursor = chunk_end
        time.sleep(0.02)

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

    for i, candle in enumerate(candles):
        if i == 0:
            result[i] = (
                candle["high"]
                - candle["low"]
            )

        else:
            previous_close = (
                candles[i - 1]["close"]
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

    result[length - 1] = (
        np.mean(seed)
    )

    for i in range(
        length,
        len(values),
    ):
        result[i] = (
            result[i - 1]
            * (length - 1)
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
    ).astype(int)

    csum = np.cumsum(vals)
    ccount = np.cumsum(valid)

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
                total / length
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

    for i in range(len(values)):
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

        dq.append(i)

    return result


# ============================================================
# H1 STATE — NO LOOKAHEAD
# ============================================================

def build_h1_state(h1):
    atr = atr14(h1)
    atr_mean50 = sma_np(
        atr,
        50,
    )

    rows = []

    for i, candle in enumerate(h1):
        complete_at = (
            h1[i + 1]["time"]
            if i + 1 < len(h1)
            else None
        )

        ratio = None

        if (
            np.isfinite(atr[i])
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

        value = eligible[
            position
        ][
            "atr_ratio50"
        ]

        if value is not None:
            result[i] = value

    return result


# ============================================================
# FEATURE CACHE
# ============================================================

SWEEP_LOOKBACKS = [
    20,
    30,
    40,
    50,
    60,
    100,
]


def build_features(
    m15,
    h1_atr_ratio50,
):
    n = len(m15)

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

    atr = atr14(m15)

    bullish = (
        closes > opens
    )

    current_body = (
        closes - opens
    )

    previous_body = np.full(
        n,
        np.nan,
        dtype=float,
    )

    previous_body[1:] = np.abs(
        closes[:-1]
        - opens[:-1]
    )

    # --------------------------------------------------------
    # CRITICAL PARITY FIX:
    #
    # old implementation treats previous zero-body candle as
    # an effectively infinite body ratio.
    # --------------------------------------------------------
    body_ratio = np.full(
        n,
        np.nan,
        dtype=float,
    )

    positive_prev_body = (
        previous_body > 0
    )

    body_ratio[
        positive_prev_body
    ] = (
        current_body[
            positive_prev_body
        ]
        / previous_body[
            positive_prev_body
        ]
    )

    zero_prev_body = (
        previous_body == 0
    )

    body_ratio[
        zero_prev_body
    ] = 999.0

    valid_atr = (
        np.isfinite(atr)
        & (atr > 0)
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
        current_body > 0
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

    previous_high[1:] = (
        highs[:-1]
    )

    previous_lows = {}

    for lookback in SWEEP_LOOKBACKS:
        previous_lows[
            lookback
        ] = rolling_previous_low(
            lows,
            lookback,
        )

    # Strict prior 4h momentum.
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
                closes[i - 1]
                - closes[i - 17]
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
        f["bullish"].copy()
    )

    if cfg.get(
        "br_min"
    ) is not None:
        mask &= (
            f["body_ratio"]
            >= cfg[
                "br_min"
            ]
        )

    mask &= (
        f["body_atr"]
        >= cfg[
            "body_atr_min"
        ]
    )

    mask &= (
        f["lower_wick_body"]
        >= cfg[
            "lower_wick_body_min"
        ]
    )

    mask &= (
        f["mom4"]
        <= cfg[
            "mom4_max"
        ]
    )

    mask &= (
        f["close"]
        > f["previous_high"]
    )

    if (
        cfg[
            "sweep_mode"
        ]
        == "ANY_20_40_60_100"
    ):
        sweep_mask = (
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

        mask &= sweep_mask

    else:
        lb = cfg[
            "sweep_lb"
        ]

        mask &= (
            f["low"]
            < f[
                "previous_lows"
            ][lb]
        )

    h1_threshold = cfg.get(
        "h1_atr_ratio_min"
    )

    if h1_threshold is not None:
        mask &= (
            f["h1_atr_ratio50"]
            >= h1_threshold
        )

    mask[:220] = False

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
        + rr
        * reference_risk
    )

    fill = (
        reference_entry
        + cost_pips
        * PIP_SIZE
    )

    actual_risk = (
        fill - stop
    )

    if actual_risk <= 0:
        return None

    for j in range(
        signal_index + 1,
        len(candles),
    ):
        candle = candles[j]

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
        round(rr, 4),
        round(cost_pips, 4),
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
            candles[index]["time"]
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
            len(indices)
            if end is None
            else bisect.bisect_left(
                times,
                end,
            )
        )

        use = indices[
            left:right
        ]

    trades = []
    position = 0

    while position < len(use):
        signal_index = use[
            position
        ]

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
            dict(trade)
        )

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

def stats_from_trades(trades):
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

    gross_profit = sum(winners)
    gross_loss = abs(sum(losers))

    if gross_loss > 0:
        pf = (
            gross_profit
            / gross_loss
        )

    elif gross_profit > 0:
        pf = 999.0

    else:
        pf = 0.0

    total_r = sum(values)

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
            len(values),

        "winners":
            len(winners),

        "losers":
            len(losers),

        "win_rate":
            (
                len(winners)
                / len(values)
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
                / len(values)
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
    cfg,
    candles,
    indices,
):
    full = stats_from_trades(
        run_backtest(
            candles,
            indices,
            cfg["rr"],
            PRIMARY_COST_PIPS,
            candles[0]["time"],
            NOW,
        )
    )

    pre = stats_from_trades(
        run_backtest(
            candles,
            indices,
            cfg["rr"],
            PRIMARY_COST_PIPS,
            candles[0]["time"],
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
            cfg["rr"],
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
                    cfg["rr"],
                    PRIMARY_COST_PIPS,
                    start,
                    end,
                )
            )
        )

    positive_eras = sum(
        1
        for s in era_stats
        if s["total_r"] > 0
    )

    min_era_pf = min(
        s["profit_factor"]
        for s in era_stats
    )

    score = (
        1.5
        * min(
            full[
                "profit_factor"
            ],
            4.0,
        )
        + 0.9
        * min(
            pre[
                "profit_factor"
            ],
            3.0,
        )
        + 0.9
        * min(
            post[
                "profit_factor"
            ],
            4.0,
        )
        + 0.45
        * positive_eras
        + 0.25
        * min(
            max(
                min_era_pf,
                0.0,
            ),
            3.0,
        )
        + 0.20
        * min(
            full["trades"]
            / 100.0,
            1.5,
        )
    )

    row = {
        "config_id":
            cfg["config_id"],

        "sweep_mode":
            cfg["sweep_mode"],

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
            cfg["rr"],

        "full_trades":
            full["trades"],

        "full_pf":
            round(
                full[
                    "profit_factor"
                ],
                6,
            ),

        "full_r":
            round(
                full["total_r"],
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
            pre["trades"],

        "pre2010_pf":
            round(
                pre[
                    "profit_factor"
                ],
                6,
            ),

        "pre2010_r":
            round(
                pre["total_r"],
                4,
            ),

        "post2010_trades":
            post["trades"],

        "post2010_pf":
            round(
                post[
                    "profit_factor"
                ],
                6,
            ),

        "post2010_r":
            round(
                post["total_r"],
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
        ] = s["trades"]

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
            s["total_r"],
            4,
        )

    return row


def sort_rows(rows):
    return sorted(
        rows,
        key=lambda row: (
            row["positive_eras"],
            row["pre2010_r"] > 0,
            row["post2010_r"] > 0,
            row["robust_score"],
            row["full_r"],
        ),
        reverse=True,
    )


# ============================================================
# CONFIG GENERATION
# ============================================================

def clone_anchor(config_id):
    cfg = deepcopy(
        NEW_ANCHOR
    )

    cfg[
        "config_id"
    ] = config_id

    return cfg


def one_way_configs():
    configs = []

    for value in [
        20,
        30,
        40,
        50,
        60,
    ]:
        cfg = clone_anchor(
            f"SLICE_SWEEP_{value}"
        )
        cfg[
            "sweep_lb"
        ] = value
        cfg["slice"] = "SWEEP_LB"
        configs.append(cfg)

    for value in [
        None,
        0.70,
        0.80,
        0.90,
        1.00,
    ]:
        label = (
            "NONE"
            if value is None
            else f"{value:.2f}"
        )

        cfg = clone_anchor(
            f"SLICE_H1ATR_{label}"
        )
        cfg[
            "h1_atr_ratio_min"
        ] = value
        cfg["slice"] = "H1_ATR"
        configs.append(cfg)

    for value in [
        -1.50,
        -1.625,
        -1.75,
        -1.875,
        -2.00,
    ]:
        cfg = clone_anchor(
            f"SLICE_MOM_{value}"
        )
        cfg[
            "mom4_max"
        ] = value
        cfg["slice"] = "MOM4"
        configs.append(cfg)

    for value in [
        1.10,
        1.20,
        1.25,
        1.30,
        1.40,
    ]:
        cfg = clone_anchor(
            f"SLICE_BODY_{value:.2f}"
        )
        cfg[
            "body_atr_min"
        ] = value
        cfg["slice"] = "BODY_ATR"
        configs.append(cfg)

    for value in [
        0.15,
        0.20,
        0.25,
        0.30,
        0.35,
    ]:
        cfg = clone_anchor(
            f"SLICE_WICK_{value:.2f}"
        )
        cfg[
            "lower_wick_body_min"
        ] = value
        cfg["slice"] = "LOWER_WICK_BODY"
        configs.append(cfg)

    for value in [
        None,
        1.00,
        1.10,
        1.20,
    ]:
        label = (
            "NONE"
            if value is None
            else f"{value:.2f}"
        )

        cfg = clone_anchor(
            f"SLICE_BR_{label}"
        )
        cfg[
            "br_min"
        ] = value
        cfg["slice"] = "BODY_RATIO"
        configs.append(cfg)

    for value in [
        3.50,
        3.75,
        4.00,
        4.25,
        4.50,
        4.75,
        5.00,
    ]:
        cfg = clone_anchor(
            f"SLICE_RR_{value:.2f}"
        )
        cfg["rr"] = value
        cfg["slice"] = "RR"
        configs.append(cfg)

    return configs


def local_grid_configs():
    configs = []
    counter = 0

    for sweep_lb in [
        30,
        40,
        50,
    ]:
        for h1_atr in [
            None,
            0.70,
            0.80,
            0.90,
        ]:
            for mom4 in [
                -1.50,
                -1.75,
                -2.00,
            ]:
                for body in [
                    1.15,
                    1.25,
                    1.35,
                ]:
                    for wick in [
                        0.20,
                        0.25,
                        0.30,
                    ]:
                        for br in [
                            None,
                            1.00,
                        ]:
                            for rr in [
                                3.75,
                                4.00,
                                4.25,
                                4.50,
                                4.75,
                            ]:
                                counter += 1

                                cfg = clone_anchor(
                                    f"GRID_{counter:04d}"
                                )

                                cfg[
                                    "sweep_lb"
                                ] = sweep_lb

                                cfg[
                                    "h1_atr_ratio_min"
                                ] = h1_atr

                                cfg[
                                    "mom4_max"
                                ] = mom4

                                cfg[
                                    "body_atr_min"
                                ] = body

                                cfg[
                                    "lower_wick_body_min"
                                ] = wick

                                cfg[
                                    "br_min"
                                ] = br

                                cfg[
                                    "rr"
                                ] = rr

                                configs.append(cfg)

    return configs


# ============================================================
# DEEP VALIDATION
# ============================================================

def stats_row(
    cfg,
    label,
    trades,
):
    s = stats_from_trades(
        trades
    )

    return {
        "config_id":
            cfg["config_id"],

        "period":
            label,

        "trades":
            s["trades"],

        "winners":
            s["winners"],

        "losers":
            s["losers"],

        "win_rate":
            round(
                s["win_rate"],
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
                s["total_r"],
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
    cfg,
    candles,
    indices,
):
    periods = [
        (
            "FULL_HISTORY",
            candles[0]["time"],
            NOW,
        ),

        (
            "PRE_2010",
            candles[0]["time"],
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
            candles[0]["time"],
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
                days=365.2425 * 5
            ),
            NOW,
        ),

        (
            "LAST_2Y",
            NOW
            - timedelta(
                days=365.2425 * 2
            ),
            NOW,
        ),
    ]

    rows = []

    for label, start, end in periods:
        trades = run_backtest(
            candles,
            indices,
            cfg["rr"],
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
        ] = iso_utc(start)

        row[
            "end_utc"
        ] = iso_utc(end)

        rows.append(row)

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
            candles[0]["time"],
            NOW,
        ),

        (
            "PRE_2010",
            candles[0]["time"],
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
    ]

    for label, start, end in periods:
        for cost in COST_GRID:
            trades = run_backtest(
                candles,
                indices,
                cfg["rr"],
                cost,
                start,
                end,
            )

            row = stats_row(
                cfg,
                label,
                trades,
            )

            row[
                "cost_pips"
            ] = cost

            rows.append(row)

    return rows


def rolling_rows(
    cfg,
    candles,
    indices,
):
    rows = []

    first_month = month_floor(
        max(
            candles[0]["time"],
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

            s = stats_from_trades(
                run_backtest(
                    candles,
                    indices,
                    cfg["rr"],
                    PRIMARY_COST_PIPS,
                    start,
                    end,
                )
            )

            rows.append({
                "config_id":
                    cfg["config_id"],

                "months":
                    months,

                "start_utc":
                    iso_utc(start),

                "end_utc":
                    iso_utc(end),

                "trades":
                    s["trades"],

                "profit_factor":
                    round(
                        s[
                            "profit_factor"
                        ],
                        6,
                    ),

                "total_r":
                    round(
                        s["total_r"],
                        4,
                    ),

                "positive":
                    s["total_r"] > 0,

                "zero_trade":
                    s["trades"] == 0,
            })

            start = add_months(
                start,
                1,
            )

    return rows


def rolling_summary_rows(rows):
    grouped = defaultdict(list)

    for row in rows:
        grouped[
            (
                row["config_id"],
                row["months"],
            )
        ].append(row)

    output = []

    for (
        config_id,
        months,
    ), subset in grouped.items():
        active = [
            row
            for row in subset
            if row["trades"] > 0
        ]

        positive_active = [
            row
            for row in active
            if row["positive"]
        ]

        positive_all = [
            row
            for row in subset
            if row["positive"]
        ]

        output.append({
            "config_id":
                config_id,

            "months":
                months,

            "windows":
                len(subset),

            "active_windows":
                len(active),

            "zero_trade_windows":
                len(subset)
                - len(active),

            "positive_windows_pct":
                round(
                    100.0
                    * len(positive_all)
                    / len(subset),
                    4,
                )
                if subset
                else 0.0,

            "positive_active_windows_pct":
                round(
                    100.0
                    * len(positive_active)
                    / len(active),
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
                        row["total_r"]
                        for row in subset
                    ),
                    4,
                ),

            "best_r":
                round(
                    max(
                        row["total_r"]
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
        candles[0]["time"].year,
        START.year,
    )

    last_year = (
        NOW.year - 1
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

        s = stats_from_trades(
            run_backtest(
                candles,
                indices,
                cfg["rr"],
                PRIMARY_COST_PIPS,
                start,
                end,
            )
        )

        rows.append({
            "config_id":
                cfg["config_id"],

            "year":
                year,

            "trades":
                s["trades"],

            "profit_factor":
                round(
                    s[
                        "profit_factor"
                    ],
                    6,
                ),

            "total_r":
                round(
                    s["total_r"],
                    4,
                ),

            "positive":
                s["total_r"] > 0,

            "negative":
                s["total_r"] < 0,

            "zero_trade":
                s["trades"] == 0,
        })

    return rows


def calendar_summary_rows(rows):
    grouped = defaultdict(list)

    for row in rows:
        grouped[
            row["config_id"]
        ].append(row)

    output = []

    for config_id, subset in grouped.items():
        active = [
            row
            for row in subset
            if row["trades"] > 0
        ]

        positive_active = [
            row
            for row in active
            if row["positive"]
        ]

        negative = [
            row
            for row in subset
            if row["negative"]
        ]

        output.append({
            "config_id":
                config_id,

            "completed_years":
                len(subset),

            "active_years":
                len(active),

            "positive_active_years_pct":
                round(
                    100.0
                    * len(positive_active)
                    / len(active),
                    4,
                )
                if active
                else 0.0,

            "negative_years":
                len(negative),

            "median_trades_year":
                round(
                    safe_median([
                        row["trades"]
                        for row in subset
                    ]),
                    4,
                ),

            "median_year_r":
                round(
                    safe_median([
                        row["total_r"]
                        for row in subset
                    ]),
                    4,
                ),

            "worst_year_r":
                round(
                    min(
                        row["total_r"]
                        for row in subset
                    ),
                    4,
                ),

            "best_year_r":
                round(
                    max(
                        row["total_r"]
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
                "Missing USDJPY M15 or H1 history"
            )

        write_csv(
            OUT_COVERAGE,
            [{
                "instrument":
                    PAIR,

                "requested_start_utc":
                    iso_utc(START),

                "actual_first_m15_utc":
                    iso_utc(
                        m15[0]["time"]
                    ),

                "actual_last_m15_utc":
                    iso_utc(
                        m15[-1]["time"]
                    ),

                "m15_candles":
                    len(m15),

                "h1_candles":
                    len(h1),
            }],
        )

        STATUS.update({
            "state":
                "precomputing",

            "message":
                "Building corrected M15 features and completed-H1 state",
        })

        h1_state = build_h1_state(
            h1
        )

        h1_aligned = align_h1_to_m15(
            [
                candle["time"]
                for candle in m15
            ],
            h1_state,
        )

        features = build_features(
            m15,
            h1_aligned,
        )

        # ----------------------------------------------------
        # Controls and parity
        # ----------------------------------------------------
        control_rows = []

        for cfg in [
            FROZEN_BENCHMARK,
            NEW_ANCHOR,
        ]:
            idx = signal_indices(
                cfg,
                features,
            )

            control_rows.append(
                evaluation_row(
                    cfg,
                    m15,
                    idx,
                )
            )

        write_csv(
            OUT_CONTROLS,
            control_rows,
        )

        benchmark_row = next(
            row
            for row in control_rows
            if row[
                "config_id"
            ] == FROZEN_BENCHMARK[
                "config_id"
            ]
        )

        anchor_row = next(
            row
            for row in control_rows
            if row[
                "config_id"
            ] == NEW_ANCHOR[
                "config_id"
            ]
        )

        parity_rows = [{
            "config_id":
                FROZEN_BENCHMARK[
                    "config_id"
                ],

            "expected_full_trades":
                100,

            "actual_full_trades":
                benchmark_row[
                    "full_trades"
                ],

            "expected_pre2010_trades":
                25,

            "actual_pre2010_trades":
                benchmark_row[
                    "pre2010_trades"
                ],

            "expected_post2010_trades":
                75,

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
                        ] == 100
                        and benchmark_row[
                            "pre2010_trades"
                        ] == 25
                        and benchmark_row[
                            "post2010_trades"
                        ] == 75
                    )
                    else "FAIL_PARITY_DO_NOT_TRUST_RESEARCH"
                ),
        }, {
            "config_id":
                NEW_ANCHOR[
                    "config_id"
                ],

            "reference_full_trades":
                68,

            "actual_full_trades":
                anchor_row[
                    "full_trades"
                ],

            "status":
                (
                    "MATCH"
                    if anchor_row[
                        "full_trades"
                    ] == 68
                    else "CHECK_OR_NEWER_COMPLETED_TRADES"
                ),
        }]

        write_csv(
            OUT_PARITY,
            parity_rows,
        )

        # Hard stop if frozen parity fails.
        if not (
            benchmark_row[
                "full_trades"
            ] == 100
            and benchmark_row[
                "pre2010_trades"
            ] == 25
            and benchmark_row[
                "post2010_trades"
            ] == 75
        ):
            raise RuntimeError(
                "Frozen benchmark parity failed. "
                "Research stopped before parameter testing."
            )

        # ----------------------------------------------------
        # One-way slices
        # ----------------------------------------------------
        slice_configs = (
            one_way_configs()
        )

        slice_rows = []

        for i, cfg in enumerate(
            slice_configs,
            1,
        ):
            STATUS.update({
                "state":
                    "slices",

                "message": (
                    f"Slice {i}/{len(slice_configs)} "
                    f"{cfg['config_id']}"
                ),
            })

            idx = signal_indices(
                cfg,
                features,
            )

            row = evaluation_row(
                cfg,
                m15,
                idx,
            )

            row[
                "slice"
            ] = cfg[
                "slice"
            ]

            slice_rows.append(row)

        write_csv(
            OUT_SLICES,
            slice_rows,
        )

        # ----------------------------------------------------
        # Interaction grid
        # ----------------------------------------------------
        grid_configs = (
            local_grid_configs()
        )

        grid_by_id = {
            cfg[
                "config_id"
            ]:
                cfg
            for cfg in grid_configs
        }

        grid_rows = []

        for i, cfg in enumerate(
            grid_configs,
            1,
        ):
            STATUS.update({
                "state":
                    "local_grid",

                "message": (
                    f"Grid {i}/{len(grid_configs)} "
                    f"{cfg['config_id']}"
                ),
            })

            idx = signal_indices(
                cfg,
                features,
            )

            grid_rows.append(
                evaluation_row(
                    cfg,
                    m15,
                    idx,
                )
            )

        grid_rows = sort_rows(
            grid_rows
        )

        write_csv(
            OUT_GRID,
            grid_rows,
        )

        # ----------------------------------------------------
        # Select deep finalists
        # ----------------------------------------------------
        eligible = [
            row
            for row in grid_rows
            if (
                55
                <= row[
                    "full_trades"
                ]
                <= 120
                and row[
                    "pre2010_trades"
                ] >= 12
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

        selected_rows = [
            benchmark_row,
            anchor_row,
        ]

        selected_ids = {
            benchmark_row[
                "config_id"
            ],
            anchor_row[
                "config_id"
            ],
        }

        for row in eligible:
            if row[
                "config_id"
            ] in selected_ids:
                continue

            selected_rows.append(
                row
            )

            selected_ids.add(
                row[
                    "config_id"
                ]
            )

            if len(
                selected_rows
            ) >= 10:
                break

        if len(
            selected_rows
        ) < 10:
            for row in grid_rows:
                if row[
                    "config_id"
                ] in selected_ids:
                    continue

                selected_rows.append(
                    row
                )

                selected_ids.add(
                    row[
                        "config_id"
                    ]
                )

                if len(
                    selected_rows
                ) >= 10:
                    break

        write_csv(
            OUT_FINALISTS,
            selected_rows,
        )

        selected_configs = [
            FROZEN_BENCHMARK,
            NEW_ANCHOR,
        ]

        for row in selected_rows[2:]:
            cfg = grid_by_id.get(
                row[
                    "config_id"
                ]
            )

            if cfg is not None:
                selected_configs.append(
                    cfg
                )

        # ----------------------------------------------------
        # Deep robustness
        # ----------------------------------------------------
        period_output = []
        cost_output = []
        rolling_output = []
        calendar_output = []
        trade_output = []

        for i, cfg in enumerate(
            selected_configs,
            1,
        ):
            STATUS.update({
                "state":
                    "deep_validation",

                "message": (
                    f"Deep validation "
                    f"{i}/{len(selected_configs)} "
                    f"{cfg['config_id']}"
                ),
            })

            idx = signal_indices(
                cfg,
                features,
            )

            period_output.extend(
                period_rows(
                    cfg,
                    m15,
                    idx,
                )
            )

            cost_output.extend(
                cost_rows(
                    cfg,
                    m15,
                    idx,
                )
            )

            rolling_output.extend(
                rolling_rows(
                    cfg,
                    m15,
                    idx,
                )
            )

            calendar_output.extend(
                calendar_rows(
                    cfg,
                    m15,
                    idx,
                )
            )

            full_trades = run_backtest(
                m15,
                idx,
                cfg["rr"],
                PRIMARY_COST_PIPS,
                m15[0]["time"],
                NOW,
            )

            for trade in full_trades:
                row = dict(trade)

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
                "Building single ZIP bundle",
        })

        build_bundle()

        STATUS.update({
            "state":
                "complete",

            "message":
                "USD/JPY M15 LONG final confirmation complete",

            "benchmark_parity":
                "MATCH",

            "benchmark_full_trades":
                benchmark_row[
                    "full_trades"
                ],

            "benchmark_pre2010_trades":
                benchmark_row[
                    "pre2010_trades"
                ],

            "benchmark_post2010_trades":
                benchmark_row[
                    "post2010_trades"
                ],

            "anchor_full_trades":
                anchor_row[
                    "full_trades"
                ],

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
                OUT_BUNDLE,
        })

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
            "USDJPY M15 LONG Final Head-to-Head Confirmation",

        "status":
            STATUS["state"],

        "instrument":
            PAIR,

        "timeframe":
            "M15",

        "side":
            "BUY",

        "frozen_benchmark": {
            "body_ratio_min":
                1.00,

            "previous_zero_body_ratio":
                999.0,

            "body_atr_min":
                1.25,

            "sweep":
                "ANY prior 20/40/60/100 low",

            "close_gt_previous_high":
                True,

            "lower_wick_body_min":
                0.25,

            "prior4h_momentum_atr_max":
                -1.75,

            "rr":
                4.00,

            "expected_full_trades":
                100,

            "expected_pre2010_trades":
                25,

            "expected_post2010_trades":
                75,
        },

        "new_anchor": {
            "body_ratio_min":
                None,

            "body_atr_min":
                1.25,

            "sweep_lb":
                40,

            "close_gt_previous_high":
                True,

            "lower_wick_body_min":
                0.25,

            "prior4h_momentum_atr_max":
                -1.75,

            "h1_atr_ratio_min":
                0.80,

            "rr":
                4.00,
        },

        "orders_supported":
            False,

        "trading_enabled":
            False,

        "routes": [
            "/usdjpy-m15-long-final-confirmation/status",
            "/usdjpy-m15-long-final-confirmation/results",
        ],
    })


@app.route(
    "/usdjpy-m15-long-final-confirmation/status"
)
def route_status():
    return jsonify(
        STATUS
    )


@app.route(
    "/usdjpy-m15-long-final-confirmation/results"
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
            "final-confirmation"
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
