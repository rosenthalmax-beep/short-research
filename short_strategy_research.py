
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
# USD/CAD M15 LONG — COMPLEMENTARY FREQUENCY TEST
#
# PURPOSE
# -------
# The locked-quality core is FROZEN:
#
#   EMA50_REGIME_BR170
#
# We are NOT loosening or retuning the core.
#
# This script searches ONLY for a structurally different
# complementary long trigger whose job is to:
#
#   1) add genuinely non-overlapping trades,
#   2) reduce inactive calendar years,
#   3) reduce zero-trade rolling windows,
#   4) preserve acceptable combined PF/DD/cost robustness.
#
# ============================================================
# FROZEN CORE — DO NOT ALTER
# ============================================================
#
# USD_CAD M15 BUY
#
# Exact bullish engulf:
#   previous bearish
#   current bullish
#   open <= previous close
#   close >= previous open
#
# BR >= 1.70
# body >= 1.25 ATR14
# range >= 1.50 ATR14
# prior 165 completed M15 bars, current excluded
# signal low within 0.175 ATR14 of previous 165-bar low
# previous COMPLETED daily EMA50 > EMA200
#
# No session filter
# No weekday filter
#
# RR = 5.00
# stop = signal low - 10 ticks
# reference entry = signal close
# historical adverse fill = close + 1.0 pip
# pyramiding = 0
#
# Known frozen-core headline:
#   70 trades
#   PF ~2.085
#   +52.06R
#   DD ~-12R
#   6 inactive completed years
#
# ============================================================
# COMPLEMENTARY FAMILIES
# ============================================================
#
# A) BULL_OUTSIDE_REVERSAL
#
# Broad research showed a real but weaker standalone edge near:
#
#   bullish outside candle
#   body ~1.25 ATR
#   close location ~0.75
#   structure ~100 bars / 0.20 ATR
#   daily EMA50 > EMA200
#
# We test a controlled neighbourhood around that geometry.
#
# ------------------------------------------------------------
# B) COMPRESSION_BREAKOUT
#
# Broad research showed a real but weaker standalone edge near:
#
#   previous ATR compression ~0.70
#   body ~1.00 ATR
#   range ~1.40 ATR
#   close > prior 10-bar high
#   H1 EMA50 > EMA200 or H4 close > EMA100
#
# Again: controlled neighbourhood only.
#
# ============================================================
# STAGED PROCESS
# ============================================================
#
# STAGE 1 — BASE COMPLEMENT SCREEN
#
# Outside reversal:
#   body       1.15 / 1.25 / 1.35
#   close loc  0.70 / 0.75 / 0.80
#   structure  80 / 100 / 120
#   distance   0.175 / 0.200 / 0.225 ATR
#   context:
#       D EMA50 > EMA200
#       D close > EMA200
#       H4 close > EMA100
#   RR fixed 4.75
#
# Compression breakout:
#   compression 0.65 / 0.70 / 0.75
#   body        0.90 / 1.00 / 1.10 ATR
#   range       1.30 / 1.40 / 1.50 ATR
#   breakout    5 / 10 / 20 bars
#   context:
#       H1 EMA50 > EMA200
#       H4 close > EMA100
#       D close > EMA200
#   RR fixed 4.75
#
# Total Stage-1 configs:
#   243 outside + 243 compression = 486
#
# ------------------------------------------------------------
# STAGE 2 — RR CONFIRMATION
#
# Take the best complementary geometries by COMBINED utility,
# not standalone PF, then test:
#
#   RR 3.50 / 4.00 / 4.50 / 4.75 / 5.00 / 5.25
#
# ------------------------------------------------------------
# DEEP FINALISTS
#
# Candidate-only AND core+non-overlap:
#
# - full history
# - pre-2010
# - 2010+
# - 4 eras
# - 2002-17 / 2018+
# - last 5Y / last 2Y
#
# Cost stress:
#   0.5 / 1.0 / 1.5 / 2.0 pips
#
# Rolling:
#   12 / 24 / 36 months
#   exact window-local reruns
#
# Calendar:
#   inactive years
#   positive active years
#   years specifically filled versus frozen core
#
# ============================================================
# SELECTION PRINCIPLE
# ============================================================
#
# DO NOT select the candidate with the highest standalone PF.
#
# Prefer:
#
#   - fewer combined inactive years,
#   - fewer 12/24/36M zero-trade windows,
#   - sufficient genuinely non-overlapping adds,
#   - candidate full/pre/post edge,
#   - broad era support,
#   - 2-pip survival,
#   - no severe combined DD deterioration.
#
# A candidate that merely increases trade count but does not
# improve coverage should be rejected.
#
# ============================================================
# NO LOOKAHEAD
# ============================================================
#
# H1 / H4 / Daily:
#   complete_at = next ACTUAL HTF candle OPEN
#   lookup = bisect_right(completion_times, signal_time) - 1
#
# Daily:
#   dailyAlignment=17
#   alignmentTimezone=America/New_York
#
# Signal timestamp:
#   M15 candle OPEN
#
# ============================================================
# M15 HISTORICAL CONVENTIONS
# ============================================================
#
# OANDA midpoint
# ATR14 Wilder/RMA, SMA seeded
#
# USD/CAD:
#   tick = 0.00001
#   pip  = 0.0001
#
# Reference entry = signal close
# Historical long adverse fill = close + cost
# Stop = signal low - 10 ticks
# Target based on REFERENCE close risk
# Actual R based on adverse fill
#
# Baseline = 1.0 pip adverse
# Stress = 0.5 / 1.0 / 1.5 / 2.0 pips
#
# Pyramiding 0.
# Exit checking begins next M15 candle.
# Exact exit-candle signal eligible.
#
# Same-bar LONG:
#   high closer to candle open => target first
#   otherwise stop first
#
# Overlay interval:
#   [signal_index, exit_index)
#
# ============================================================
# PARITY
# ============================================================
#
# Freeze parity dataset through:
#   2026-09-09 16:15 UTC M15 open
#
# Required core count:
#   70 trades
#
# ============================================================
# ONE ZIP
# ============================================================
#
# /usdcad-m15-long-complementary-frequency/results
#
# READ ONLY. NEVER SENDS ORDERS.
# ============================================================


app = Flask(__name__)

TOKEN = os.getenv("OANDA_TOKEN")
BASE = os.getenv(
    "OANDA_API_URL",
    "https://api-fxtrade.oanda.com",
)

PAIR = "USD_CAD"

START = datetime(
    2002, 5, 6, 20, 0,
    tzinfo=timezone.utc,
)

NOW = (
    datetime.now(timezone.utc)
    .replace(second=0, microsecond=0)
)

PARITY_LAST_M15_OPEN = datetime(
    2026, 9, 9, 16, 15,
    tzinfo=timezone.utc,
)

HTF_WARMUP_START = (
    START - timedelta(days=900)
)

TICK_SIZE = 0.00001
PIP_SIZE = 0.0001

STOP_BUFFER_TICKS = 10

PRIMARY_COST_PIPS = 1.00

COST_GRID = [
    0.50,
    1.00,
    1.50,
    2.00,
]

STAGE1_RR = 4.75

STAGE1_KEEP = 20
FINALIST_KEEP = 12

RR_VALUES = [
    3.50,
    4.00,
    4.50,
    4.75,
    5.00,
    5.25,
]


# ============================================================
# FROZEN CORE
# ============================================================

CORE = {
    "config_id":
        "CORE_EMA50_REGIME_BR170",

    "br_min":
        1.70,

    "body_atr_min":
        1.25,

    "range_atr_min":
        1.50,

    "structure_lb":
        165,

    "structure_dist_atr_max":
        0.175,

    "context":
        "D_EMA50_GT_EMA200",

    "rr":
        5.00,
}


# ============================================================
# OUTPUTS
# ============================================================

OUT_COVERAGE = (
    "usdcad_m15_long_complementary_frequency_coverage.csv"
)

OUT_PARITY = (
    "usdcad_m15_long_complementary_frequency_parity.csv"
)

OUT_CORE = (
    "usdcad_m15_long_complementary_frequency_core_baseline.csv"
)

OUT_STAGE1 = (
    "usdcad_m15_long_complementary_frequency_stage1.csv"
)

OUT_STAGE2 = (
    "usdcad_m15_long_complementary_frequency_stage2_rr.csv"
)

OUT_FINALISTS = (
    "usdcad_m15_long_complementary_frequency_finalists.csv"
)

OUT_PERIODS = (
    "usdcad_m15_long_complementary_frequency_periods.csv"
)

OUT_COST = (
    "usdcad_m15_long_complementary_frequency_cost_stress.csv"
)

OUT_ROLLING = (
    "usdcad_m15_long_complementary_frequency_rolling.csv"
)

OUT_ROLLING_SUMMARY = (
    "usdcad_m15_long_complementary_frequency_rolling_summary.csv"
)

OUT_CALENDAR = (
    "usdcad_m15_long_complementary_frequency_calendar_years.csv"
)

OUT_CALENDAR_SUMMARY = (
    "usdcad_m15_long_complementary_frequency_calendar_summary.csv"
)

OUT_OVERLAP = (
    "usdcad_m15_long_complementary_frequency_overlap.csv"
)

OUT_TRADES = (
    "usdcad_m15_long_complementary_frequency_finalist_trades.csv"
)

OUT_NOTES = (
    "usdcad_m15_long_complementary_frequency_notes.csv"
)

OUT_BUNDLE = (
    "USDCAD_M15_LONG_COMPLEMENTARY_FREQUENCY_RESULTS.zip"
)

STATUS = {
    "state":
        "not_started",

    "message":
        "USD/CAD M15 LONG complementary frequency test not started",

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
    paths = [
        OUT_COVERAGE,
        OUT_PARITY,
        OUT_CORE,
        OUT_STAGE1,
        OUT_STAGE2,
        OUT_FINALISTS,
        OUT_PERIODS,
        OUT_COST,
        OUT_ROLLING,
        OUT_ROLLING_SUMMARY,
        OUT_CALENDAR,
        OUT_CALENDAR_SUMMARY,
        OUT_OVERLAP,
        OUT_TRADES,
        OUT_NOTES,
    ]

    with zipfile.ZipFile(
        OUT_BUNDLE,
        "w",
        compression=zipfile.ZIP_DEFLATED,
    ) as archive:
        for path in paths:
            if os.path.exists(path):
                archive.write(
                    path,
                    arcname=os.path.basename(
                        path
                    ),
                )


def safe_median(values):
    values = list(
        values
    )

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

    if granularity == "D":
        params[
            "dailyAlignment"
        ] = 17

        params[
            "alignmentTimezone"
        ] = "America/New_York"

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
                    mid[
                        "o"
                    ]
                ),

            "high":
                float(
                    mid[
                        "h"
                    ]
                ),

            "low":
                float(
                    mid[
                        "l"
                    ]
                ),

            "close":
                float(
                    mid[
                        "c"
                    ]
                ),
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
                row[
                    "time"
                ]
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
            row[
                "time"
            ]
    )

    return result


# ============================================================
# INDICATORS
# ============================================================

def true_ranges(candles):
    result = np.full(
        len(
            candles
        ),
        np.nan,
        dtype=float,
    )

    for i, candle in enumerate(
        candles
    ):
        if i == 0:
            result[
                i
            ] = (
                candle[
                    "high"
                ]
                - candle[
                    "low"
                ]
            )

        else:
            previous_close = (
                candles[
                    i - 1
                ][
                    "close"
                ]
            )

            result[
                i
            ] = max(
                candle[
                    "high"
                ]
                - candle[
                    "low"
                ],

                abs(
                    candle[
                        "high"
                    ]
                    - previous_close
                ),

                abs(
                    candle[
                        "low"
                    ]
                    - previous_close
                ),
            )

    return result


def rma(values, length):
    result = np.full(
        len(
            values
        ),
        np.nan,
        dtype=float,
    )

    if len(
        values
    ) < length:
        return result

    seed = values[
        :length
    ]

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
        len(
            values
        ),
    ):
        result[
            i
        ] = (
            result[
                i - 1
            ]
            * (
                length - 1
            )
            + values[
                i
            ]
        ) / length

    return result


def atr14(candles):
    return rma(
        true_ranges(
            candles
        ),
        14,
    )


def sma_np(values, length):
    result = np.full(
        len(
            values
        ),
        np.nan,
        dtype=float,
    )

    numeric = np.nan_to_num(
        values,
        nan=0.0,
    )

    valid = np.isfinite(
        values
    ).astype(
        int
    )

    csum = np.cumsum(
        numeric
    )

    ccount = np.cumsum(
        valid
    )

    for i in range(
        length - 1,
        len(
            values
        ),
    ):
        total = csum[
            i
        ]

        count = ccount[
            i
        ]

        if i >= length:
            total -= csum[
                i - length
            ]

            count -= ccount[
                i - length
            ]

        if count == length:
            result[
                i
            ] = (
                total
                / length
            )

    return result


def ema_list(values, length):
    result = [
        None
    ] * len(
        values
    )

    if len(
        values
    ) < length:
        return result

    result[
        length - 1
    ] = (
        sum(
            values[
                :length
            ]
        )
        / length
    )

    alpha = (
        2.0
        / (
            length
            + 1.0
        )
    )

    for i in range(
        length,
        len(
            values
        ),
    ):
        result[
            i
        ] = (
            alpha
            * values[
                i
            ]
            + (
                1.0
                - alpha
            )
            * result[
                i - 1
            ]
        )

    return result


def rolling_previous_extreme(
    values,
    lookback,
    mode,
):
    result = np.full(
        len(
            values
        ),
        np.nan,
        dtype=float,
    )

    dq = deque()

    for i in range(
        len(
            values
        )
    ):
        oldest = (
            i
            - lookback
        )

        while (
            dq
            and dq[
                0
            ] < oldest
        ):
            dq.popleft()

        if (
            i >= lookback
            and dq
        ):
            result[
                i
            ] = (
                values[
                    dq[
                        0
                    ]
                ]
            )

        if mode == "min":
            while (
                dq
                and values[
                    dq[
                        -1
                    ]
                ] >= values[
                    i
                ]
            ):
                dq.pop()

        else:
            while (
                dq
                and values[
                    dq[
                        -1
                    ]
                ] <= values[
                    i
                ]
            ):
                dq.pop()

        dq.append(
            i
        )

    return result


# ============================================================
# HTF STATE — NO LOOKAHEAD
# ============================================================

def build_htf_state(
    candles,
):
    closes = [
        candle[
            "close"
        ]
        for candle in candles
    ]

    ema50 = ema_list(
        closes,
        50,
    )

    ema100 = ema_list(
        closes,
        100,
    )

    ema200 = ema_list(
        closes,
        200,
    )

    rows = []

    for i, candle in enumerate(
        candles
    ):
        complete_at = (
            candles[
                i + 1
            ][
                "time"
            ]
            if (
                i + 1
                < len(
                    candles
                )
            )
            else None
        )

        rows.append({
            "complete_at":
                complete_at,

            "close":
                candle[
                    "close"
                ],

            "ema50":
                ema50[
                    i
                ],

            "ema100":
                ema100[
                    i
                ],

            "ema200":
                ema200[
                    i
                ],
        })

    return rows


def align_htf_to_m15(
    m15_times,
    state,
):
    eligible = [
        row
        for row in state
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

    result = {
        key:
            np.full(
                len(
                    m15_times
                ),
                np.nan,
                dtype=float,
            )
        for key in [
            "close",
            "ema50",
            "ema100",
            "ema200",
        ]
    }

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

        row = eligible[
            position
        ]

        for key in result:
            value = row[
                key
            ]

            if value is not None:
                result[
                    key
                ][
                    i
                ] = value

    return result


# ============================================================
# FEATURES
# ============================================================

OUTSIDE_LBS = [
    80,
    100,
    120,
]

BREAKOUT_LBS = [
    5,
    10,
    20,
]


def build_features(
    m15,
    h1,
    h4,
    daily,
):
    n = len(
        m15
    )

    opens = np.array(
        [
            candle[
                "open"
            ]
            for candle in m15
        ],
        dtype=float,
    )

    highs = np.array(
        [
            candle[
                "high"
            ]
            for candle in m15
        ],
        dtype=float,
    )

    lows = np.array(
        [
            candle[
                "low"
            ]
            for candle in m15
        ],
        dtype=float,
    )

    closes = np.array(
        [
            candle[
                "close"
            ]
            for candle in m15
        ],
        dtype=float,
    )

    atr = atr14(
        m15
    )

    atr_mean20 = sma_np(
        atr,
        20,
    )

    valid_atr = (
        np.isfinite(
            atr
        )
        & (
            atr
            > 0
        )
    )

    bullish = (
        closes
        > opens
    )

    current_body = (
        closes
        - opens
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

    candle_range = (
        highs
        - lows
    )

    range_atr = np.full(
        n,
        np.nan,
        dtype=float,
    )

    range_atr[
        valid_atr
    ] = (
        candle_range[
            valid_atr
        ]
        / atr[
            valid_atr
        ]
    )

    close_location = np.full(
        n,
        np.nan,
        dtype=float,
    )

    valid_range = (
        candle_range
        > 0
    )

    close_location[
        valid_range
    ] = (
        closes[
            valid_range
        ]
        - lows[
            valid_range
        ]
    ) / candle_range[
        valid_range
    ]

    previous_atr = np.full(
        n,
        np.nan,
        dtype=float,
    )

    previous_atr_mean20 = np.full(
        n,
        np.nan,
        dtype=float,
    )

    previous_atr[
        1:
    ] = atr[
        :-1
    ]

    previous_atr_mean20[
        1:
    ] = atr_mean20[
        :-1
    ]

    compression = np.full(
        n,
        np.nan,
        dtype=float,
    )

    valid_compression = (
        np.isfinite(
            previous_atr
        )
        & np.isfinite(
            previous_atr_mean20
        )
        & (
            previous_atr_mean20
            > 0
        )
    )

    compression[
        valid_compression
    ] = (
        previous_atr[
            valid_compression
        ]
        / previous_atr_mean20[
            valid_compression
        ]
    )

    exact_bull_engulf = np.zeros(
        n,
        dtype=bool,
    )

    exact_bull_engulf[
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
        previous_body
        > 0
    )

    body_ratio[
        valid_previous_body
    ] = (
        current_body[
            valid_previous_body
        ]
        / previous_body[
            valid_previous_body
        ]
    )

    body_ratio[
        previous_body
        == 0
    ] = 999.0

    previous_low165 = (
        rolling_previous_extreme(
            lows,
            165,
            "min",
        )
    )

    core_structure_distance = np.full(
        n,
        np.nan,
        dtype=float,
    )

    valid_core_structure = (
        valid_atr
        & np.isfinite(
            previous_low165
        )
    )

    core_structure_distance[
        valid_core_structure
    ] = (
        np.abs(
            lows[
                valid_core_structure
            ]
            - previous_low165[
                valid_core_structure
            ]
        )
        / atr[
            valid_core_structure
        ]
    )

    outside_structure_distance = {}

    for lookback in OUTSIDE_LBS:
        prior_low = (
            rolling_previous_extreme(
                lows,
                lookback,
                "min",
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
                prior_low
            )
        )

        distance[
            valid
        ] = (
            np.abs(
                lows[
                    valid
                ]
                - prior_low[
                    valid
                ]
            )
            / atr[
                valid
            ]
        )

        outside_structure_distance[
            lookback
        ] = distance

    previous_high = {}

    for lookback in BREAKOUT_LBS:
        previous_high[
            lookback
        ] = rolling_previous_extreme(
            highs,
            lookback,
            "max",
        )

    previous_candle_high = np.full(
        n,
        np.nan,
        dtype=float,
    )

    previous_candle_low = np.full(
        n,
        np.nan,
        dtype=float,
    )

    previous_candle_high[
        1:
    ] = highs[
        :-1
    ]

    previous_candle_low[
        1:
    ] = lows[
        :-1
    ]

    return {
        "n":
            n,

        "open":
            opens,

        "high":
            highs,

        "low":
            lows,

        "close":
            closes,

        "atr":
            atr,

        "valid_atr":
            valid_atr,

        "bullish":
            bullish,

        "body_atr":
            body_atr,

        "range_atr":
            range_atr,

        "close_location":
            close_location,

        "compression":
            compression,

        "exact_bull_engulf":
            exact_bull_engulf,

        "body_ratio":
            body_ratio,

        "core_structure_distance":
            core_structure_distance,

        "outside_structure_distance":
            outside_structure_distance,

        "previous_high":
            previous_high,

        "previous_candle_high":
            previous_candle_high,

        "previous_candle_low":
            previous_candle_low,

        "h1_close":
            h1[
                "close"
            ],

        "h1_ema50":
            h1[
                "ema50"
            ],

        "h1_ema100":
            h1[
                "ema100"
            ],

        "h1_ema200":
            h1[
                "ema200"
            ],

        "h4_close":
            h4[
                "close"
            ],

        "h4_ema100":
            h4[
                "ema100"
            ],

        "d_close":
            daily[
                "close"
            ],

        "d_ema50":
            daily[
                "ema50"
            ],

        "d_ema200":
            daily[
                "ema200"
            ],
    }


# ============================================================
# CONTEXTS
# ============================================================

def apply_context(
    mask,
    context,
    f,
):
    if context == "D_EMA50_GT_EMA200":
        mask &= (
            f[
                "d_ema50"
            ]
            > f[
                "d_ema200"
            ]
        )

    elif context == "D_CLOSE_GT_EMA200":
        mask &= (
            f[
                "d_close"
            ]
            > f[
                "d_ema200"
            ]
        )

    elif context == "H4_CLOSE_GT_EMA100":
        mask &= (
            f[
                "h4_close"
            ]
            > f[
                "h4_ema100"
            ]
        )

    elif context == "H1_EMA50_GT_EMA200":
        mask &= (
            f[
                "h1_ema50"
            ]
            > f[
                "h1_ema200"
            ]
        )

    else:
        raise RuntimeError(
            f"Unknown context: {context}"
        )

    return mask


# ============================================================
# SIGNAL LOGIC
# ============================================================

def core_signal_indices(
    f,
):
    mask = (
        f[
            "valid_atr"
        ].copy()
        & f[
            "exact_bull_engulf"
        ]
    )

    mask &= (
        f[
            "body_ratio"
        ]
        >= CORE[
            "br_min"
        ]
    )

    mask &= (
        f[
            "body_atr"
        ]
        >= CORE[
            "body_atr_min"
        ]
    )

    mask &= (
        f[
            "range_atr"
        ]
        >= CORE[
            "range_atr_min"
        ]
    )

    mask &= (
        f[
            "core_structure_distance"
        ]
        <= CORE[
            "structure_dist_atr_max"
        ]
    )

    mask = apply_context(
        mask,
        CORE[
            "context"
        ],
        f,
    )

    mask[
        :220
    ] = False

    return np.flatnonzero(
        mask
    ).tolist()


def candidate_signal_indices(
    cfg,
    f,
):
    family = cfg[
        "family"
    ]

    mask = (
        f[
            "valid_atr"
        ].copy()
        & f[
            "bullish"
        ]
    )

    if (
        family
        == "BULL_OUTSIDE_REVERSAL"
    ):
        mask &= (
            f[
                "high"
            ]
            > f[
                "previous_candle_high"
            ]
        )

        mask &= (
            f[
                "low"
            ]
            < f[
                "previous_candle_low"
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
                "close_location"
            ]
            >= cfg[
                "close_loc_min"
            ]
        )

        mask &= (
            f[
                "outside_structure_distance"
            ][
                cfg[
                    "structure_lb"
                ]
            ]
            <= cfg[
                "structure_dist_atr_max"
            ]
        )

    elif (
        family
        == "COMPRESSION_BREAKOUT"
    ):
        mask &= (
            f[
                "compression"
            ]
            <= cfg[
                "compression_max"
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
                "range_atr"
            ]
            >= cfg[
                "range_atr_min"
            ]
        )

        mask &= (
            f[
                "close"
            ]
            > f[
                "previous_high"
            ][
                cfg[
                    "breakout_lb"
                ]
            ]
        )

    else:
        raise RuntimeError(
            f"Unknown family: {family}"
        )

    mask = apply_context(
        mask,
        cfg[
            "context"
        ],
        f,
    )

    mask[
        :220
    ] = False

    return np.flatnonzero(
        mask
    ).tolist()


# ============================================================
# CONFIG GENERATION
# ============================================================

def build_stage1_configs():
    configs = []
    counter = 0

    # --------------------------------------------------------
    # A) OUTSIDE REVERSAL
    # --------------------------------------------------------
    for body in [
        1.15,
        1.25,
        1.35,
    ]:
        for close_loc in [
            0.70,
            0.75,
            0.80,
        ]:
            for lookback in [
                80,
                100,
                120,
            ]:
                for distance in [
                    0.175,
                    0.200,
                    0.225,
                ]:
                    for context in [
                        "D_EMA50_GT_EMA200",
                        "D_CLOSE_GT_EMA200",
                        "H4_CLOSE_GT_EMA100",
                    ]:
                        counter += 1

                        configs.append({
                            "config_id":
                                f"OUT_{counter:04d}",

                            "family":
                                "BULL_OUTSIDE_REVERSAL",

                            "body_atr_min":
                                body,

                            "close_loc_min":
                                close_loc,

                            "structure_lb":
                                lookback,

                            "structure_dist_atr_max":
                                distance,

                            "compression_max":
                                None,

                            "range_atr_min":
                                None,

                            "breakout_lb":
                                None,

                            "context":
                                context,

                            "rr":
                                STAGE1_RR,
                        })

    # --------------------------------------------------------
    # B) COMPRESSION BREAKOUT
    # --------------------------------------------------------
    comp_counter = 0

    for compression in [
        0.65,
        0.70,
        0.75,
    ]:
        for body in [
            0.90,
            1.00,
            1.10,
        ]:
            for range_atr in [
                1.30,
                1.40,
                1.50,
            ]:
                for breakout_lb in [
                    5,
                    10,
                    20,
                ]:
                    for context in [
                        "H1_EMA50_GT_EMA200",
                        "H4_CLOSE_GT_EMA100",
                        "D_CLOSE_GT_EMA200",
                    ]:
                        comp_counter += 1

                        configs.append({
                            "config_id":
                                f"COMP_{comp_counter:04d}",

                            "family":
                                "COMPRESSION_BREAKOUT",

                            "body_atr_min":
                                body,

                            "close_loc_min":
                                None,

                            "structure_lb":
                                None,

                            "structure_dist_atr_max":
                                None,

                            "compression_max":
                                compression,

                            "range_atr_min":
                                range_atr,

                            "breakout_lb":
                                breakout_lb,

                            "context":
                                context,

                            "rr":
                                STAGE1_RR,
                        })

    return configs


def build_stage2_configs(
    stage1_by_id,
    top_rows,
):
    configs = []

    for rank, row in enumerate(
        top_rows,
        1,
    ):
        base = deepcopy(
            stage1_by_id[
                row[
                    "config_id"
                ]
            ]
        )

        for rr in RR_VALUES:
            cfg = deepcopy(
                base
            )

            cfg[
                "rr"
            ] = rr

            cfg[
                "config_id"
            ] = (
                f"S2_{rank:02d}_"
                f"{base['config_id']}_"
                f"RR{rr:.2f}"
            )

            configs.append(
                cfg
            )

    return configs


# ============================================================
# BACKTEST ENGINE
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
        len(
            candles
        ),
    ):
        candle = candles[
            j
        ]

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
        id(
            candles
        ),
        len(
            candles
        ),
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

        use = indices[
            left:right
        ]

    trades = []
    position = 0

    while position < len(
        use
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
# OVERLAY
# ============================================================

def nonoverlap_overlay(
    core_trades,
    candidate_trades,
):
    core = sorted(
        core_trades,
        key=lambda trade:
            trade[
                "signal_index"
            ],
    )

    candidate = sorted(
        candidate_trades,
        key=lambda trade:
            trade[
                "signal_index"
            ],
    )

    accepted = []
    rejected = []

    pointer = 0

    for trade in candidate:
        start = trade[
            "signal_index"
        ]

        end = trade[
            "exit_index"
        ]

        while (
            pointer
            < len(
                core
            )
            and core[
                pointer
            ][
                "exit_index"
            ] <= start
        ):
            pointer += 1

        overlaps = False

        if pointer < len(
            core
        ):
            core_trade = (
                core[
                    pointer
                ]
            )

            overlaps = (
                core_trade[
                    "signal_index"
                ] < end
                and start
                < core_trade[
                    "exit_index"
                ]
            )

        if overlaps:
            rejected.append(
                trade
            )
        else:
            accepted.append(
                trade
            )

    combined = []

    for trade in core:
        item = dict(
            trade
        )

        item[
            "source"
        ] = "CORE"

        combined.append(
            item
        )

    for trade in accepted:
        item = dict(
            trade
        )

        item[
            "source"
        ] = "COMPLEMENT"

        combined.append(
            item
        )

    combined.sort(
        key=lambda trade: (
            trade[
                "signal_index"
            ],
            0
            if trade[
                "source"
            ] == "CORE"
            else 1,
        )
    )

    return (
        combined,
        accepted,
        rejected,
    )


# ============================================================
# STATS
# ============================================================

def stats_from_trades(
    trades,
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
            equity
            - peak,
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
                100.0
                * len(
                    winners
                )
                / len(
                    values
                )
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


def filter_trades(
    trades,
    start,
    end,
):
    return [
        trade
        for trade in trades
        if (
            trade[
                "entry_time"
            ] >= start
            and trade[
                "entry_time"
            ] < end
        )
    ]


def completed_calendar_summary(
    trades,
):
    years = []

    for year in range(
        START.year,
        NOW.year,
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

        subset = filter_trades(
            trades,
            start,
            end,
        )

        s = stats_from_trades(
            subset
        )

        years.append({
            "year":
                year,

            "trades":
                s[
                    "trades"
                ],

            "total_r":
                s[
                    "total_r"
                ],

            "profit_factor":
                s[
                    "profit_factor"
                ],
        })

    active = [
        row
        for row in years
        if row[
            "trades"
        ] > 0
    ]

    inactive = [
        row[
            "year"
        ]
        for row in years
        if row[
            "trades"
        ] == 0
    ]

    positive_active = [
        row
        for row in active
        if row[
            "total_r"
        ] > 0
    ]

    negative = [
        row[
            "year"
        ]
        for row in active
        if row[
            "total_r"
        ] < 0
    ]

    return {
        "years":
            years,

        "active_years":
            len(
                active
            ),

        "inactive_years":
            len(
                inactive
            ),

        "inactive_year_list":
            inactive,

        "positive_active_years":
            len(
                positive_active
            ),

        "positive_active_years_pct":
            (
                100.0
                * len(
                    positive_active
                )
                / len(
                    active
                )
                if active
                else 0.0
            ),

        "negative_years":
            len(
                negative
            ),

        "negative_year_list":
            negative,
    }


def quick_coverage_score(
    cfg,
    candidate_trades,
    accepted,
    combined,
    core_calendar,
):
    candidate_stats = (
        stats_from_trades(
            candidate_trades
        )
    )

    accepted_stats = (
        stats_from_trades(
            accepted
        )
    )

    combined_stats = (
        stats_from_trades(
            combined
        )
    )

    candidate_pre = (
        stats_from_trades(
            filter_trades(
                candidate_trades,
                START,
                datetime(
                    2010,
                    1,
                    1,
                    tzinfo=timezone.utc,
                ),
            )
        )
    )

    candidate_post = (
        stats_from_trades(
            filter_trades(
                candidate_trades,
                datetime(
                    2010,
                    1,
                    1,
                    tzinfo=timezone.utc,
                ),
                NOW,
            )
        )
    )

    era_stats = [
        stats_from_trades(
            filter_trades(
                candidate_trades,
                start,
                end,
            )
        )
        for _, start, end
        in ERAS
    ]

    positive_eras = sum(
        1
        for stat in era_stats
        if stat[
            "total_r"
        ] > 0
    )

    combined_calendar = (
        completed_calendar_summary(
            combined
        )
    )

    core_inactive = set(
        core_calendar[
            "inactive_year_list"
        ]
    )

    combined_inactive = set(
        combined_calendar[
            "inactive_year_list"
        ]
    )

    filled_years = sorted(
        core_inactive
        - combined_inactive
    )

    inactive_reduction = (
        core_calendar[
            "inactive_years"
        ]
        - combined_calendar[
            "inactive_years"
        ]
    )

    dd_penalty = max(
        0.0,
        abs(
            combined_stats[
                "max_drawdown_r"
            ]
        )
        - 14.0,
    )

    # Coverage-led selection.
    score = (
        8.0
        * inactive_reduction
        + 0.20
        * len(
            accepted
        )
        + 1.25
        * positive_eras
        + 1.00
        * (
            1
            if candidate_pre[
                "total_r"
            ] > 0
            else 0
        )
        + 1.00
        * (
            1
            if candidate_post[
                "total_r"
            ] > 0
            else 0
        )
        + 1.50
        * min(
            max(
                accepted_stats[
                    "profit_factor"
                ]
                - 1.0,
                -0.5,
            ),
            1.5,
        )
        + 0.80
        * min(
            max(
                combined_stats[
                    "profit_factor"
                ]
                - 1.0,
                0.0,
            ),
            2.0,
        )
        - 0.35
        * dd_penalty
    )

    row = {
        "config_id":
            cfg[
                "config_id"
            ],

        "family":
            cfg[
                "family"
            ],

        "context":
            cfg[
                "context"
            ],

        "rr":
            cfg[
                "rr"
            ],

        "candidate_trades":
            candidate_stats[
                "trades"
            ],

        "candidate_pf":
            round(
                candidate_stats[
                    "profit_factor"
                ],
                6,
            ),

        "candidate_r":
            round(
                candidate_stats[
                    "total_r"
                ],
                4,
            ),

        "candidate_pre2010_pf":
            round(
                candidate_pre[
                    "profit_factor"
                ],
                6,
            ),

        "candidate_pre2010_r":
            round(
                candidate_pre[
                    "total_r"
                ],
                4,
            ),

        "candidate_post2010_pf":
            round(
                candidate_post[
                    "profit_factor"
                ],
                6,
            ),

        "candidate_post2010_r":
            round(
                candidate_post[
                    "total_r"
                ],
                4,
            ),

        "candidate_positive_eras":
            positive_eras,

        "accepted_adds":
            len(
                accepted
            ),

        "rejected_overlap":
            (
                len(
                    candidate_trades
                )
                - len(
                    accepted
                )
            ),

        "accepted_pf":
            round(
                accepted_stats[
                    "profit_factor"
                ],
                6,
            ),

        "accepted_r":
            round(
                accepted_stats[
                    "total_r"
                ],
                4,
            ),

        "combined_trades":
            combined_stats[
                "trades"
            ],

        "combined_pf":
            round(
                combined_stats[
                    "profit_factor"
                ],
                6,
            ),

        "combined_r":
            round(
                combined_stats[
                    "total_r"
                ],
                4,
            ),

        "combined_dd":
            round(
                combined_stats[
                    "max_drawdown_r"
                ],
                4,
            ),

        "combined_inactive_years":
            combined_calendar[
                "inactive_years"
            ],

        "inactive_years_reduced":
            inactive_reduction,

        "filled_core_inactive_years":
            ",".join(
                str(
                    year
                )
                for year in filled_years
            ),

        "combined_positive_active_years_pct":
            round(
                combined_calendar[
                    "positive_active_years_pct"
                ],
                4,
            ),

        "coverage_score":
            round(
                score,
                6,
            ),
    }

    for field in [
        "body_atr_min",
        "close_loc_min",
        "structure_lb",
        "structure_dist_atr_max",
        "compression_max",
        "range_atr_min",
        "breakout_lb",
    ]:
        row[
            field
        ] = cfg.get(
            field
        )

    return row


def sort_screen_rows(
    rows,
):
    return sorted(
        rows,
        key=lambda row: (
            row[
                "inactive_years_reduced"
            ],
            row[
                "combined_inactive_years"
            ]
            * -1,
            row[
                "candidate_pre2010_r"
            ] > 0,
            row[
                "candidate_post2010_r"
            ] > 0,
            row[
                "candidate_positive_eras"
            ],
            row[
                "coverage_score"
            ],
            row[
                "accepted_adds"
            ],
        ),
        reverse=True,
    )


# ============================================================
# EXACT WINDOW-LOCAL ROLLING
# ============================================================

def rolling_exact(
    cfg,
    candles,
    core_indices,
    candidate_indices,
    months,
    cost_pips,
):
    rows = []

    first_month = (
        month_floor(
            START
        )
    )

    last_month = (
        month_floor(
            NOW
        )
    )

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

        core = run_backtest(
            candles,
            core_indices,
            CORE[
                "rr"
            ],
            cost_pips,
            start,
            end,
        )

        candidate = run_backtest(
            candles,
            candidate_indices,
            cfg[
                "rr"
            ],
            cost_pips,
            start,
            end,
        )

        (
            combined,
            accepted,
            rejected,
        ) = nonoverlap_overlay(
            core,
            candidate,
        )

        for mode, trades in [
            (
                "CORE_ONLY",
                core,
            ),
            (
                "CANDIDATE_ONLY",
                candidate,
            ),
            (
                "CORE_PLUS_NONOVERLAP",
                combined,
            ),
        ]:
            stat = stats_from_trades(
                trades
            )

            rows.append({
                "config_id":
                    cfg[
                        "config_id"
                    ],

                "mode":
                    mode,

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
                    stat[
                        "trades"
                    ],

                "profit_factor":
                    round(
                        stat[
                            "profit_factor"
                        ],
                        6,
                    ),

                "total_r":
                    round(
                        stat[
                            "total_r"
                        ],
                        4,
                    ),

                "positive":
                    stat[
                        "total_r"
                    ] > 0,

                "zero_trade":
                    stat[
                        "trades"
                    ] == 0,

                "accepted_adds":
                    (
                        len(
                            accepted
                        )
                        if mode
                        == "CORE_PLUS_NONOVERLAP"
                        else None
                    ),

                "rejected_overlap":
                    (
                        len(
                            rejected
                        )
                        if mode
                        == "CORE_PLUS_NONOVERLAP"
                        else None
                    ),
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
                    "mode"
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
        mode,
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

            "mode":
                mode,

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
                    (
                        100.0
                        * len(
                            positive_all
                        )
                        / len(
                            subset
                        )
                    )
                    if subset
                    else 0.0,
                    4,
                ),

            "positive_active_windows_pct":
                round(
                    (
                        100.0
                        * len(
                            positive_active
                        )
                        / len(
                            active
                        )
                    )
                    if active
                    else 0.0,
                    4,
                ),

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


# ============================================================
# DEEP PERIOD / COST / CALENDAR
# ============================================================

def period_definitions():
    return [
        (
            "FULL_HISTORY",
            START,
            NOW,
        ),

        (
            "PRE_2010",
            START,
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
            START,
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


def deep_period_rows(
    cfg,
    candles,
    core_indices,
    candidate_indices,
):
    rows = []

    for (
        label,
        start,
        end,
    ) in period_definitions():
        core = run_backtest(
            candles,
            core_indices,
            CORE[
                "rr"
            ],
            PRIMARY_COST_PIPS,
            start,
            end,
        )

        candidate = run_backtest(
            candles,
            candidate_indices,
            cfg[
                "rr"
            ],
            PRIMARY_COST_PIPS,
            start,
            end,
        )

        (
            combined,
            accepted,
            rejected,
        ) = nonoverlap_overlay(
            core,
            candidate,
        )

        for mode, trades in [
            (
                "CORE_ONLY",
                core,
            ),
            (
                "CANDIDATE_ONLY",
                candidate,
            ),
            (
                "CORE_PLUS_NONOVERLAP",
                combined,
            ),
        ]:
            s = stats_from_trades(
                trades
            )

            rows.append({
                "config_id":
                    cfg[
                        "config_id"
                    ],

                "mode":
                    mode,

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

                "accepted_adds":
                    (
                        len(
                            accepted
                        )
                        if mode
                        == "CORE_PLUS_NONOVERLAP"
                        else None
                    ),

                "rejected_overlap":
                    (
                        len(
                            rejected
                        )
                        if mode
                        == "CORE_PLUS_NONOVERLAP"
                        else None
                    ),
            })

    return rows


def deep_cost_rows(
    cfg,
    candles,
    core_indices,
    candidate_indices,
):
    rows = []

    for (
        period,
        start,
        end,
    ) in [
        (
            "FULL_HISTORY",
            START,
            NOW,
        ),
        (
            "PRE_2010",
            START,
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
    ]:
        for cost in COST_GRID:
            core = run_backtest(
                candles,
                core_indices,
                CORE[
                    "rr"
                ],
                cost,
                start,
                end,
            )

            candidate = run_backtest(
                candles,
                candidate_indices,
                cfg[
                    "rr"
                ],
                cost,
                start,
                end,
            )

            (
                combined,
                accepted,
                rejected,
            ) = nonoverlap_overlay(
                core,
                candidate,
            )

            for mode, trades in [
                (
                    "CANDIDATE_ONLY",
                    candidate,
                ),
                (
                    "CORE_PLUS_NONOVERLAP",
                    combined,
                ),
            ]:
                s = stats_from_trades(
                    trades
                )

                rows.append({
                    "config_id":
                        cfg[
                            "config_id"
                        ],

                    "mode":
                        mode,

                    "period":
                        period,

                    "cost_pips":
                        cost,

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

                    "max_drawdown_r":
                        round(
                            s[
                                "max_drawdown_r"
                            ],
                            4,
                        ),

                    "accepted_adds":
                        (
                            len(
                                accepted
                            )
                            if mode
                            == "CORE_PLUS_NONOVERLAP"
                            else None
                        ),

                    "rejected_overlap":
                        (
                            len(
                                rejected
                            )
                            if mode
                            == "CORE_PLUS_NONOVERLAP"
                            else None
                        ),
                })

    return rows


def deep_calendar_rows(
    cfg,
    candles,
    core_indices,
    candidate_indices,
):
    rows = []
    core_inactive_years = set()

    baseline_years = {}

    for year in range(
        START.year,
        NOW.year,
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

        core = run_backtest(
            candles,
            core_indices,
            CORE[
                "rr"
            ],
            PRIMARY_COST_PIPS,
            start,
            end,
        )

        candidate = run_backtest(
            candles,
            candidate_indices,
            cfg[
                "rr"
            ],
            PRIMARY_COST_PIPS,
            start,
            end,
        )

        (
            combined,
            accepted,
            rejected,
        ) = nonoverlap_overlay(
            core,
            candidate,
        )

        baseline_years[
            year
        ] = (
            len(
                core
            )
        )

        if not core:
            core_inactive_years.add(
                year
            )

        for mode, trades in [
            (
                "CORE_ONLY",
                core,
            ),
            (
                "CANDIDATE_ONLY",
                candidate,
            ),
            (
                "CORE_PLUS_NONOVERLAP",
                combined,
            ),
        ]:
            s = stats_from_trades(
                trades
            )

            rows.append({
                "config_id":
                    cfg[
                        "config_id"
                    ],

                "mode":
                    mode,

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

                "core_was_inactive":
                    year
                    in core_inactive_years,

                "core_inactive_year_filled":
                    (
                        year
                        in core_inactive_years
                        and mode
                        == "CORE_PLUS_NONOVERLAP"
                        and s[
                            "trades"
                        ] > 0
                    ),

                "accepted_adds":
                    (
                        len(
                            accepted
                        )
                        if mode
                        == "CORE_PLUS_NONOVERLAP"
                        else None
                    ),

                "rejected_overlap":
                    (
                        len(
                            rejected
                        )
                        if mode
                        == "CORE_PLUS_NONOVERLAP"
                        else None
                    ),
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
            (
                row[
                    "config_id"
                ],
                row[
                    "mode"
                ],
            )
        ].append(
            row
        )

    output = []

    for (
        config_id,
        mode,
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

        zero_years = [
            row[
                "year"
            ]
            for row in subset
            if row[
                "zero_trade"
            ]
        ]

        losing_years = [
            row[
                "year"
            ]
            for row in subset
            if row[
                "negative"
            ]
        ]

        filled = [
            row[
                "year"
            ]
            for row in subset
            if row[
                "core_inactive_year_filled"
            ]
        ]

        output.append({
            "config_id":
                config_id,

            "mode":
                mode,

            "completed_years":
                len(
                    subset
                ),

            "active_years":
                len(
                    active
                ),

            "inactive_years":
                len(
                    zero_years
                ),

            "inactive_year_list":
                ",".join(
                    str(
                        year
                    )
                    for year in zero_years
                ),

            "positive_active_years":
                len(
                    positive_active
                ),

            "positive_active_years_pct":
                round(
                    (
                        100.0
                        * len(
                            positive_active
                        )
                        / len(
                            active
                        )
                    )
                    if active
                    else 0.0,
                    4,
                ),

            "losing_years":
                len(
                    losing_years
                ),

            "losing_year_list":
                ",".join(
                    str(
                        year
                    )
                    for year in losing_years
                ),

            "core_inactive_years_filled":
                len(
                    filled
                ),

            "filled_year_list":
                ",".join(
                    str(
                        year
                    )
                    for year in filled
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
            HTF_WARMUP_START,
            NOW,
            180,
        )

        h4 = fetch_history(
            "H4",
            HTF_WARMUP_START,
            NOW,
            700,
        )

        daily = fetch_history(
            "D",
            HTF_WARMUP_START,
            NOW,
            3500,
        )

        if not all([
            m15,
            h1,
            h4,
            daily,
        ]):
            raise RuntimeError(
                "Missing required USD_CAD history"
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

                "parity_last_m15_open_utc":
                    iso_utc(
                        PARITY_LAST_M15_OPEN
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

                "h4_candles":
                    len(
                        h4
                    ),

                "daily_candles":
                    len(
                        daily
                    ),
            }],
        )

        STATUS.update({
            "state":
                "precomputing",

            "message":
                "Building no-lookahead H1/H4/D state and M15 feature cache",
        })

        m15_times = [
            candle[
                "time"
            ]
            for candle in m15
        ]

        h1_aligned = (
            align_htf_to_m15(
                m15_times,
                build_htf_state(
                    h1
                ),
            )
        )

        h4_aligned = (
            align_htf_to_m15(
                m15_times,
                build_htf_state(
                    h4
                ),
            )
        )

        daily_aligned = (
            align_htf_to_m15(
                m15_times,
                build_htf_state(
                    daily
                ),
            )
        )

        features = build_features(
            m15,
            h1_aligned,
            h4_aligned,
            daily_aligned,
        )

        core_indices = (
            core_signal_indices(
                features
            )
        )

        # ----------------------------------------------------
        # HARD CORE PARITY
        # ----------------------------------------------------
        parity_count = (
            bisect.bisect_right(
                m15_times,
                PARITY_LAST_M15_OPEN,
            )
        )

        parity_candles = (
            m15[
                :parity_count
            ]
        )

        parity_indices = [
            index
            for index in core_indices
            if index < parity_count
        ]

        parity_end = (
            PARITY_LAST_M15_OPEN
            + timedelta(
                minutes=15
            )
        )

        parity_core = run_backtest(
            parity_candles,
            parity_indices,
            CORE[
                "rr"
            ],
            PRIMARY_COST_PIPS,
            START,
            parity_end,
        )

        parity_match = (
            len(
                parity_core
            ) == 70
        )

        write_csv(
            OUT_PARITY,
            [{
                "test":
                    "FROZEN_CORE_EMA50_REGIME_BR170",

                "expected_trades":
                    70,

                "actual_trades":
                    len(
                        parity_core
                    ),

                "status":
                    (
                        "MATCH"
                        if parity_match
                        else "FAIL"
                    ),
            }],
        )

        if not parity_match:
            raise RuntimeError(
                "Frozen core parity failed. "
                "Do not trust complementary-frequency results."
            )

        # ----------------------------------------------------
        # CORE BASELINE
        # ----------------------------------------------------
        core_trades = run_backtest(
            m15,
            core_indices,
            CORE[
                "rr"
            ],
            PRIMARY_COST_PIPS,
            START,
            NOW,
        )

        core_stats = stats_from_trades(
            core_trades
        )

        core_calendar = (
            completed_calendar_summary(
                core_trades
            )
        )

        write_csv(
            OUT_CORE,
            [{
                "config_id":
                    CORE[
                        "config_id"
                    ],

                "trades":
                    core_stats[
                        "trades"
                    ],

                "profit_factor":
                    round(
                        core_stats[
                            "profit_factor"
                        ],
                        6,
                    ),

                "total_r":
                    round(
                        core_stats[
                            "total_r"
                        ],
                        4,
                    ),

                "max_drawdown_r":
                    round(
                        core_stats[
                            "max_drawdown_r"
                        ],
                        4,
                    ),

                "inactive_years":
                    core_calendar[
                        "inactive_years"
                    ],

                "inactive_year_list":
                    ",".join(
                        str(
                            year
                        )
                        for year
                        in core_calendar[
                            "inactive_year_list"
                        ]
                    ),

                "positive_active_years_pct":
                    round(
                        core_calendar[
                            "positive_active_years_pct"
                        ],
                        4,
                    ),
            }],
        )

        # ----------------------------------------------------
        # STAGE 1
        # ----------------------------------------------------
        stage1_configs = (
            build_stage1_configs()
        )

        if len(
            stage1_configs
        ) != 486:
            raise RuntimeError(
                f"Expected 486 Stage-1 configs, got "
                f"{len(stage1_configs)}"
            )

        stage1_by_id = {
            cfg[
                "config_id"
            ]:
                cfg
            for cfg in stage1_configs
        }

        stage1_rows = []

        for i, cfg in enumerate(
            stage1_configs,
            1,
        ):
            STATUS.update({
                "state":
                    "stage1",

                "message": (
                    f"Complement screen "
                    f"{i}/{len(stage1_configs)} "
                    f"{cfg['config_id']}"
                ),
            })

            indices = (
                candidate_signal_indices(
                    cfg,
                    features,
                )
            )

            candidate = run_backtest(
                m15,
                indices,
                cfg[
                    "rr"
                ],
                PRIMARY_COST_PIPS,
                START,
                NOW,
            )

            (
                combined,
                accepted,
                rejected,
            ) = nonoverlap_overlay(
                core_trades,
                candidate,
            )

            stage1_rows.append(
                quick_coverage_score(
                    cfg,
                    candidate,
                    accepted,
                    combined,
                    core_calendar,
                )
            )

        stage1_rows = (
            sort_screen_rows(
                stage1_rows
            )
        )

        write_csv(
            OUT_STAGE1,
            stage1_rows,
        )

        # Do NOT use an 80-trade gate.
        #
        # Complement selection needs sufficient additional trades
        # and baseline robustness, not arbitrary standalone frequency.
        eligible_stage1 = [
            row
            for row in stage1_rows
            if (
                row[
                    "candidate_trades"
                ] >= 25
                and row[
                    "accepted_adds"
                ] >= 8
                and row[
                    "candidate_pf"
                ] >= 1.02
                and row[
                    "accepted_r"
                ] > 0
                and row[
                    "candidate_positive_eras"
                ] >= 3
            )
        ]

        top_stage1 = (
            eligible_stage1[
                :STAGE1_KEEP
            ]
        )

        if len(
            top_stage1
        ) < STAGE1_KEEP:
            selected = {
                row[
                    "config_id"
                ]
                for row in top_stage1
            }

            for row in stage1_rows:
                if row[
                    "config_id"
                ] in selected:
                    continue

                if row[
                    "accepted_adds"
                ] < 5:
                    continue

                top_stage1.append(
                    row
                )

                selected.add(
                    row[
                        "config_id"
                    ]
                )

                if len(
                    top_stage1
                ) >= STAGE1_KEEP:
                    break

        # ----------------------------------------------------
        # STAGE 2 — RR
        # ----------------------------------------------------
        stage2_configs = (
            build_stage2_configs(
                stage1_by_id,
                top_stage1,
            )
        )

        stage2_by_id = {
            cfg[
                "config_id"
            ]:
                cfg
            for cfg in stage2_configs
        }

        stage2_rows = []

        for i, cfg in enumerate(
            stage2_configs,
            1,
        ):
            STATUS.update({
                "state":
                    "stage2",

                "message": (
                    f"RR confirmation "
                    f"{i}/{len(stage2_configs)} "
                    f"{cfg['config_id']}"
                ),
            })

            indices = (
                candidate_signal_indices(
                    cfg,
                    features,
                )
            )

            candidate = run_backtest(
                m15,
                indices,
                cfg[
                    "rr"
                ],
                PRIMARY_COST_PIPS,
                START,
                NOW,
            )

            (
                combined,
                accepted,
                rejected,
            ) = nonoverlap_overlay(
                core_trades,
                candidate,
            )

            stage2_rows.append(
                quick_coverage_score(
                    cfg,
                    candidate,
                    accepted,
                    combined,
                    core_calendar,
                )
            )

        stage2_rows = (
            sort_screen_rows(
                stage2_rows
            )
        )

        write_csv(
            OUT_STAGE2,
            stage2_rows,
        )

        # ----------------------------------------------------
        # FINALISTS
        # ----------------------------------------------------
        eligible_finalists = [
            row
            for row in stage2_rows
            if (
                row[
                    "accepted_adds"
                ] >= 8
                and row[
                    "accepted_r"
                ] > 0
                and row[
                    "candidate_pf"
                ] >= 1.02
                and row[
                    "candidate_pre2010_r"
                ] > 0
                and row[
                    "candidate_post2010_r"
                ] > 0
                and row[
                    "candidate_positive_eras"
                ] >= 3
                and row[
                    "combined_pf"
                ] >= 1.25
                and row[
                    "combined_dd"
                ] >= -18.0
            )
        ]

        finalist_rows = (
            eligible_finalists[
                :FINALIST_KEEP
            ]
        )

        if len(
            finalist_rows
        ) < FINALIST_KEEP:
            selected = {
                row[
                    "config_id"
                ]
                for row in finalist_rows
            }

            for row in stage2_rows:
                if row[
                    "config_id"
                ] in selected:
                    continue

                if row[
                    "accepted_adds"
                ] < 5:
                    continue

                finalist_rows.append(
                    row
                )

                selected.add(
                    row[
                        "config_id"
                    ]
                )

                if len(
                    finalist_rows
                ) >= FINALIST_KEEP:
                    break

        write_csv(
            OUT_FINALISTS,
            finalist_rows,
        )

        finalist_configs = [
            stage2_by_id[
                row[
                    "config_id"
                ]
            ]
            for row in finalist_rows
        ]

        # ----------------------------------------------------
        # DEEP VALIDATION
        # ----------------------------------------------------
        period_output = []
        cost_output = []
        rolling_output = []
        calendar_output = []
        overlap_output = []
        trade_output = []

        for i, cfg in enumerate(
            finalist_configs,
            1,
        ):
            STATUS.update({
                "state":
                    "deep_validation",

                "message": (
                    f"Deep complement validation "
                    f"{i}/{len(finalist_configs)} "
                    f"{cfg['config_id']}"
                ),
            })

            candidate_indices = (
                candidate_signal_indices(
                    cfg,
                    features,
                )
            )

            candidate_trades = (
                run_backtest(
                    m15,
                    candidate_indices,
                    cfg[
                        "rr"
                    ],
                    PRIMARY_COST_PIPS,
                    START,
                    NOW,
                )
            )

            (
                combined,
                accepted,
                rejected,
            ) = nonoverlap_overlay(
                core_trades,
                candidate_trades,
            )

            overlap_output.append({
                "config_id":
                    cfg[
                        "config_id"
                    ],

                "family":
                    cfg[
                        "family"
                    ],

                "candidate_trades":
                    len(
                        candidate_trades
                    ),

                "accepted_nonoverlap":
                    len(
                        accepted
                    ),

                "rejected_overlap":
                    len(
                        rejected
                    ),

                "overlap_pct":
                    round(
                        (
                            100.0
                            * len(
                                rejected
                            )
                            / len(
                                candidate_trades
                            )
                        )
                        if candidate_trades
                        else 0.0,
                        4,
                    ),

                "combined_trades":
                    len(
                        combined
                    ),
            })

            period_output.extend(
                deep_period_rows(
                    cfg,
                    m15,
                    core_indices,
                    candidate_indices,
                )
            )

            cost_output.extend(
                deep_cost_rows(
                    cfg,
                    m15,
                    core_indices,
                    candidate_indices,
                )
            )

            for months in [
                12,
                24,
                36,
            ]:
                rolling_output.extend(
                    rolling_exact(
                        cfg,
                        m15,
                        core_indices,
                        candidate_indices,
                        months,
                        PRIMARY_COST_PIPS,
                    )
                )

            calendar_output.extend(
                deep_calendar_rows(
                    cfg,
                    m15,
                    core_indices,
                    candidate_indices,
                )
            )

            for trade in combined:
                row = dict(
                    trade
                )

                row[
                    "config_id"
                ] = cfg[
                    "config_id"
                ]

                row[
                    "family"
                ] = cfg[
                    "family"
                ]

                row[
                    "context"
                ] = cfg[
                    "context"
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
            OUT_OVERLAP,
            overlap_output,
        )

        write_csv(
            OUT_TRADES,
            trade_output,
        )

        write_csv(
            OUT_NOTES,
            [{
                "item":
                    "Frozen core",

                "value":
                    "EMA50_REGIME_BR170 remains unchanged: exact bullish engulf, BR>=1.70, body>=1.25ATR, range>=1.50ATR, LB165/0.175ATR, previous completed daily EMA50>EMA200, RR5.00.",
            }, {
                "item":
                    "Complement families",

                "value":
                    "Bullish outside reversal near structure and compression breakout with HTF trend context only.",
            }, {
                "item":
                    "Selection objective",

                "value":
                    "Reduce inactive years and zero-trade rolling windows using genuinely non-overlapping additional trades, not merely increase raw signal count.",
            }, {
                "item":
                    "No arbitrary frequency gate",

                "value":
                    "Unlike the earlier USDJPY complement issue, finalists are not excluded simply for having fewer than 80 standalone trades.",
            }, {
                "item":
                    "Historical holdout",

                "value":
                    "No pristine historical holdout remains because full history has already been used in development.",
            }, {
                "item":
                    "Decision",

                "value":
                    "Reject the add-on if it does not materially improve coverage or if combined drawdown/cost/temporal robustness deteriorates too much.",
            }],
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
                "USD/CAD M15 LONG complementary-frequency test complete",

            "core_parity":
                "MATCH",

            "stage1_configs":
                len(
                    stage1_configs
                ),

            "stage2_configs":
                len(
                    stage2_configs
                ),

            "deep_finalists":
                len(
                    finalist_configs
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
            "USDCAD M15 LONG Complementary Frequency Test",

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

        "frozen_core":
            "EMA50_REGIME_BR170",

        "core_rules": {
            "br_min":
                1.70,

            "body_atr_min":
                1.25,

            "range_atr_min":
                1.50,

            "structure_lb":
                165,

            "structure_distance_atr_max":
                0.175,

            "daily_regime":
                "previous completed EMA50 > EMA200",

            "rr":
                5.00,
        },

        "complement_families": [
            "BULL_OUTSIDE_REVERSAL",
            "COMPRESSION_BREAKOUT",
        ],

        "orders_supported":
            False,

        "trading_enabled":
            False,

        "routes": [
            "/usdcad-m15-long-complementary-frequency/status",
            "/usdcad-m15-long-complementary-frequency/results",
        ],
    })


@app.route(
    "/usdcad-m15-long-complementary-frequency/status"
)
def route_status():
    return jsonify(
        STATUS
    )


@app.route(
    "/usdcad-m15-long-complementary-frequency/results"
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
            "usdcad-m15-long-"
            "complementary-frequency"
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
