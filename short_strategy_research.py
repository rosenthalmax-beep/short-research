
import os
import csv
import time
import bisect
import zipfile
import threading
from collections import deque, defaultdict
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from statistics import median

import numpy as np
import requests
from flask import Flask, jsonify, send_file


# ============================================================
# USD/CAD M15 SHORT — COMPLEMENTARY FREQUENCY SEARCH
#
# PURPOSE
# -------
# Preserve the FINAL locked-quality core exactly and search only
# for a structurally different complementary short trigger.
#
# CORE — FROZEN / NEVER LOOSENED:
#
#   bearish outside candle
#   body >= 0.75 ATR14
#   close location <= 0.35
#   previous 120-bar high, current excluded
#   abs(signal high - prior120 high) <= 0.05 ATR14
#   previous COMPLETED H4 close < H4 EMA100
#   RR4.00
#
# Historical headline anchor:
#   54 trades
#
# Meaningful tradeless completed years to try to fill:
#   2007 / 2010 / 2014
#
# 2002 / 2003 / 2004 are NOT optimization targets.
#
# ============================================================
# COMPLEMENT SEARCH
# ============================================================
#
# Distinct families only:
#
#   A) COMPRESSION_BREAKDOWN
#   B) BEAR_ENGULF_STRUCTURE
#   C) HIGH_SWEEP_REJECTION
#   D) RALLY_FAILURE_BREAKDOWN
#
# No BEAR_OUTSIDE_REVERSAL in the complement search.
#
# Stage 1:
#   modest structural grids
#   fixed RR4.00
#   candidate backtested independently with pyramiding0
#   then reject candidate trades overlapping frozen core
#   score accepted complement + combined system
#
# Stage 2:
#   top 20 Stage-1 configs
#   RR = 3.00 / 3.50 / 4.00 / 4.50 / 5.00
#
# Deep finalists:
#   top 12
#   full/pre/post/eras/recent
#   0.5/1/1.5/2 pip costs
#   rolling 12/24/36M
#   completed calendar years
#   explicit filled-core-inactive-year reporting
#   combined trade ledger
#
# ============================================================
# ACCEPTANCE INTENT
# ============================================================
#
# Prefer complements that:
#
#   - add >= 8 genuinely non-overlapping trades
#   - accepted complement total R > 0
#   - candidate pre-2010 R > 0
#   - candidate 2010+ R > 0
#   - candidate has >= 3 positive long eras
#   - survives 2-pip costs
#   - materially reduces 2007 / 2010 / 2014 inactivity
#   - does not wreck combined PF / DD
#
# Do NOT force a complement if none is clean.
#
# ============================================================
# M15 HISTORICAL CONVENTIONS
# ============================================================
#
# OANDA midpoint.
# ATR14 = Wilder/RMA, SMA seeded.
#
# USD/CAD:
#   tick = 0.00001
#   pip  = 0.0001
#
# Reference entry:
#   signal close
#
# Historical SHORT adverse fill:
#   signal close - adverse cost
#
# Baseline:
#   1.0 pip
#
# Cost stress:
#   0.5 / 1.0 / 1.5 / 2.0 pips
#
# Stop:
#   signal high + 10 ticks
#
# Target:
#   based on REFERENCE signal-close risk
#
# Actual R:
#   based on adverse fill
#
# Pyramiding:
#   0
#
# Exit testing:
#   begins next M15 candle
#
# Exact exit-candle signal:
#   eligible
#
# Same-bar SHORT:
#   if candle high is closer to candle open => STOP first
#   otherwise TARGET first
#
# ============================================================
# NO LOOKAHEAD
# ============================================================
#
# H1 / H4:
#   complete_at = next ACTUAL HTF candle OPEN
#   lookup = bisect_right(completion_times, signal_time) - 1
#
# Prior M15 momentum:
#   ends at close[i-1], never signal close.
#
# Signal timestamp:
#   M15 candle OPEN
#
# ============================================================
# HARD PARITY
# ============================================================
#
# Frozen core must reproduce:
#
#   54 trades
#
# through:
#   2026-09-09 20:30 UTC M15 open
#
# Abort if parity fails.
#
# ============================================================
# OVERLAP RULE
# ============================================================
#
# Candidate is backtested independently with pyramiding0.
#
# Reject candidate trade if its interval:
#   [signal_index, exit_index)
#
# overlaps any frozen core interval.
#
# Exact core exit-candle candidate signal remains eligible.
#
# ============================================================
# ONE ZIP
# ============================================================
#
# /usdcad-m15-short-complement-final-local/results
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
    2026, 9, 9, 20, 30,
    tzinfo=timezone.utc,
)

ANCHOR_LAST_M15_OPEN = datetime(
    2026, 9, 9, 21, 15,
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

STAGE1_RR = 3.50

RR_VALUES = [
    3.25,
    3.50,
    3.75,
    4.00,
]

MEANINGFUL_CORE_INACTIVE_YEARS = {
    2007,
    2010,
    2014,
}

STAGE1_KEEP = 30
FINALIST_KEEP = 16

ALL_LOOKBACKS = [
    5,
    10,
    15,
    20,
    30,
    40,
    60,
    80,
    100,
    120,
    165,
]


# ============================================================
# FROZEN CORE
# ============================================================

CORE = {
    "config_id":
        "CORE_OUTSIDE_H4EMA100_LB120_D005_RR400",

    "body_atr_min":
        0.75,

    "close_loc_max":
        0.35,

    "structure_lb":
        120,

    "structure_dist_atr_max":
        0.05,

    "rr":
        4.00,
}


# Frozen reference from the completed complementary-frequency run.
# This is S2_0022 exactly and is used only as a parity/control anchor.
ANCHOR = {
    "config_id": "ANCHOR_S2_0022",
    "family": "HIGH_SWEEP_REJECTION",
    "context": "H1_CLOSE_LT_EMA100",
    "sweep_lb": 20,
    "body_atr_min": 1.25,
    "close_loc_max": 0.40,
    "upper_wick_body_min": 0.25,
    "rr": 3.50,
}


# ============================================================
# OUTPUTS
# ============================================================

OUT_COVERAGE = (
    "usdcad_m15_short_complement_final_local_coverage.csv"
)

OUT_PARITY = (
    "usdcad_m15_short_complement_final_local_parity.csv"
)

OUT_CORE = (
    "usdcad_m15_short_complement_final_local_core_baseline.csv"
)

OUT_STAGE1 = (
    "usdcad_m15_short_complement_final_local_stage1_geometry.csv"
)

OUT_STAGE2 = (
    "usdcad_m15_short_complement_final_local_stage2_rr.csv"
)

OUT_FINALISTS = (
    "usdcad_m15_short_complement_final_local_finalists.csv"
)

OUT_PERIODS = (
    "usdcad_m15_short_complement_final_local_periods.csv"
)

OUT_COST = (
    "usdcad_m15_short_complement_final_local_cost_stress.csv"
)

OUT_ROLLING = (
    "usdcad_m15_short_complement_final_local_rolling.csv"
)

OUT_ROLLING_SUMMARY = (
    "usdcad_m15_short_complement_final_local_rolling_summary.csv"
)

OUT_CALENDAR = (
    "usdcad_m15_short_complement_final_local_calendar_years.csv"
)

OUT_CALENDAR_SUMMARY = (
    "usdcad_m15_short_complement_final_local_calendar_summary.csv"
)

OUT_OVERLAP = (
    "usdcad_m15_short_complement_final_local_overlap.csv"
)

OUT_TRADES = (
    "usdcad_m15_short_complement_final_local_finalist_trades.csv"
)

OUT_NOTES = (
    "usdcad_m15_short_complement_final_local_notes.csv"
)

OUT_BUNDLE = (
    "USDCAD_M15_SHORT_COMPLEMENT_FINAL_LOCAL_RESULTS.zip"
)

STATUS = {
    "state":
        "not_started",

    "message":
        "USD/CAD M15 SHORT complement final local confirmation not started",

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

        cursor = chunk_end
        time.sleep(0.02)

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
        len(candles),
        np.nan,
        dtype=float,
    )

    for i, candle in enumerate(candles):
        if i == 0:
            result[i] = (
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

            result[i] = max(
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
        len(values),
        np.nan,
        dtype=float,
    )

    if len(values) < length:
        return result

    seed = values[
        :length
    ]

    if np.isnan(seed).any():
        return result

    result[
        length - 1
    ] = np.mean(seed)

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
                total
                / length
            )

    return result


def ema_list(values, length):
    result = [
        None
    ] * len(values)

    if len(values) < length:
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
            length + 1.0
        )
    )

    for i in range(
        length,
        len(values),
    ):
        result[i] = (
            alpha
            * values[i]
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
            and dq[
                0
            ] < oldest
        ):
            dq.popleft()

        if (
            i >= lookback
            and dq
        ):
            result[i] = (
                values[
                    dq[
                        0
                    ]
                ]
            )

        if mode == "max":
            while (
                dq
                and values[
                    dq[
                        -1
                    ]
                ] <= values[i]
            ):
                dq.pop()

        else:
            while (
                dq
                and values[
                    dq[
                        -1
                    ]
                ] >= values[i]
            ):
                dq.pop()

        dq.append(i)

    return result


# ============================================================
# HTF STATE — NO LOOKAHEAD
# ============================================================

def build_htf_state(candles):
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

    for i, candle in enumerate(candles):
        complete_at = (
            candles[
                i + 1
            ][
                "time"
            ]
            if (
                i + 1
                < len(candles)
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
                ema50[i],

            "ema100":
                ema100[i],

            "ema200":
                ema200[i],
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

    fields = [
        "close",
        "ema50",
        "ema100",
        "ema200",
    ]

    result = {
        field:
            np.full(
                len(m15_times),
                np.nan,
                dtype=float,
            )
        for field in fields
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

        for field in fields:
            value = row[
                field
            ]

            if value is not None:
                result[
                    field
                ][i] = value

    return result


# ============================================================
# FEATURES
# ============================================================

def build_features(
    m15,
    h1,
    h4,
):
    n = len(m15)

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
            atr > 0
        )
    )

    bearish = (
        closes < opens
    )

    body = (
        opens - closes
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

    candle_range = (
        highs - lows
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
        candle_range > 0
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

    upper_wick = (
        highs
        - np.maximum(
            opens,
            closes,
        )
    )

    upper_wick_body = np.full(
        n,
        np.nan,
        dtype=float,
    )

    valid_body = (
        body > 0
    )

    upper_wick_body[
        valid_body
    ] = (
        upper_wick[
            valid_body
        ]
        / body[
            valid_body
        ]
    )

    previous_high = np.full(
        n,
        np.nan,
        dtype=float,
    )

    previous_low = np.full(
        n,
        np.nan,
        dtype=float,
    )

    previous_high[
        1:
    ] = highs[:-1]

    previous_low[
        1:
    ] = lows[:-1]

    exact_bear_engulf = np.zeros(
        n,
        dtype=bool,
    )

    exact_bear_engulf[
        1:
    ] = (
        (
            closes[:-1]
            > opens[:-1]
        )
        & (
            closes[1:]
            < opens[1:]
        )
        & (
            opens[1:]
            >= closes[:-1]
        )
        & (
            closes[1:]
            <= opens[:-1]
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
        closes[:-1]
        - opens[:-1]
    )

    body_ratio = np.full(
        n,
        np.nan,
        dtype=float,
    )

    valid_prev_body = (
        previous_body > 0
    )

    body_ratio[
        valid_prev_body
    ] = (
        body[
            valid_prev_body
        ]
        / previous_body[
            valid_prev_body
        ]
    )

    body_ratio[
        previous_body == 0
    ] = 999.0

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
    ] = atr[:-1]

    previous_atr_mean20[
        1:
    ] = atr_mean20[:-1]

    compression = np.full(
        n,
        np.nan,
        dtype=float,
    )

    valid_comp = (
        np.isfinite(
            previous_atr
        )
        & np.isfinite(
            previous_atr_mean20
        )
        & (
            previous_atr_mean20 > 0
        )
    )

    compression[
        valid_comp
    ] = (
        previous_atr[
            valid_comp
        ]
        / previous_atr_mean20[
            valid_comp
        ]
    )

    prev_high = {}
    prev_low = {}

    for lookback in ALL_LOOKBACKS:
        prev_high[
            lookback
        ] = rolling_previous_extreme(
            highs,
            lookback,
            "max",
        )

        prev_low[
            lookback
        ] = rolling_previous_extreme(
            lows,
            lookback,
            "min",
        )

    # Strict prior 12H rally, ending at close[i-1].
    rally_12h = np.full(
        n,
        np.nan,
        dtype=float,
    )

    for i in range(
        49,
        n,
    ):
        if (
            valid_atr[i]
            and atr[i] > 0
        ):
            rally_12h[i] = (
                closes[
                    i - 1
                ]
                - closes[
                    i - 49
                ]
            ) / atr[i]

    return {
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

        "bearish":
            bearish,

        "body_atr":
            body_atr,

        "range_atr":
            range_atr,

        "close_location":
            close_location,

        "upper_wick_body":
            upper_wick_body,

        "previous_high":
            previous_high,

        "previous_low":
            previous_low,

        "exact_bear_engulf":
            exact_bear_engulf,

        "body_ratio":
            body_ratio,

        "compression":
            compression,

        "prev_high":
            prev_high,

        "prev_low":
            prev_low,

        "rally_12h":
            rally_12h,

        "h1_close":
            h1[
                "close"
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

        "h4_ema200":
            h4[
                "ema200"
            ],
    }


# ============================================================
# FROZEN CORE SIGNAL
# ============================================================

def core_signal_indices(f):
    mask = (
        f[
            "valid_atr"
        ].copy()
        & f[
            "bearish"
        ]
    )

    mask &= (
        f[
            "high"
        ]
        > f[
            "previous_high"
        ]
    )

    mask &= (
        f[
            "low"
        ]
        < f[
            "previous_low"
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
            "close_location"
        ]
        <= CORE[
            "close_loc_max"
        ]
    )

    prior_high = (
        f[
            "prev_high"
        ][
            CORE[
                "structure_lb"
            ]
        ]
    )

    distance = np.full(
        len(mask),
        np.nan,
        dtype=float,
    )

    valid = (
        f[
            "valid_atr"
        ]
        & np.isfinite(
            prior_high
        )
    )

    distance[
        valid
    ] = (
        np.abs(
            f[
                "high"
            ][
                valid
            ]
            - prior_high[
                valid
            ]
        )
        / f[
            "atr"
        ][
            valid
        ]
    )

    mask &= (
        distance
        <= CORE[
            "structure_dist_atr_max"
        ]
    )

    mask &= (
        f[
            "h4_close"
        ]
        < f[
            "h4_ema100"
        ]
    )

    mask[
        :220
    ] = False

    return np.flatnonzero(
        mask
    ).tolist()


# ============================================================
# COMPLEMENT CONFIGS
# ============================================================

def build_stage1_configs():
    """
    Final local geometry confirmation around S2_0022 only.

    Frozen hypothesis:
      HIGH_SWEEP_REJECTION + H1 close < EMA100.

    Stage 1 varies only the four unresolved geometry boundaries at
    RR3.50. Stage 2 then RR-confirms the strongest Stage-1 rows.
    """
    configs = []
    counter = 0

    for sweep_lb in [
        10,
        15,
        20,
        30,
        40,
    ]:
        for body in [
            1.15,
            1.25,
            1.35,
        ]:
            for close_loc in [
                0.35,
                0.40,
                0.45,
            ]:
                for wick in [
                    0.20,
                    0.25,
                    0.30,
                ]:
                    counter += 1

                    configs.append({
                        "config_id":
                            f"L1_SWEEP_{counter:04d}",

                        "family":
                            "HIGH_SWEEP_REJECTION",

                        "sweep_lb":
                            sweep_lb,

                        "body_atr_min":
                            body,

                        "close_loc_max":
                            close_loc,

                        "upper_wick_body_min":
                            wick,

                        "context":
                            "H1_CLOSE_LT_EMA100",

                        "rr":
                            STAGE1_RR,
                    })

    return configs


def apply_context(
    mask,
    context,
    f,
):
    if context == "H1_CLOSE_LT_EMA100":
        mask &= (
            f[
                "h1_close"
            ]
            < f[
                "h1_ema100"
            ]
        )

    elif context == "H4_CLOSE_LT_EMA100":
        mask &= (
            f[
                "h4_close"
            ]
            < f[
                "h4_ema100"
            ]
        )

    else:
        raise RuntimeError(
            f"Unknown context: {context}"
        )

    return mask


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
            "bearish"
        ]
    )

    if (
        family
        == "COMPRESSION_BREAKDOWN"
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
            < f[
                "prev_low"
            ][
                cfg[
                    "breakout_lb"
                ]
            ]
        )

    elif (
        family
        == "BEAR_ENGULF_STRUCTURE"
    ):
        mask &= (
            f[
                "exact_bear_engulf"
            ]
        )

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

        prior_high = (
            f[
                "prev_high"
            ][
                cfg[
                    "structure_lb"
                ]
            ]
        )

        distance = np.full(
            len(mask),
            np.nan,
            dtype=float,
        )

        valid = (
            f[
                "valid_atr"
            ]
            & np.isfinite(
                prior_high
            )
        )

        distance[
            valid
        ] = (
            np.abs(
                f[
                    "high"
                ][
                    valid
                ]
                - prior_high[
                    valid
                ]
            )
            / f[
                "atr"
            ][
                valid
            ]
        )

        mask &= (
            distance
            <= cfg[
                "structure_dist_atr_max"
            ]
        )

    elif (
        family
        == "HIGH_SWEEP_REJECTION"
    ):
        prior_high = (
            f[
                "prev_high"
            ][
                cfg[
                    "sweep_lb"
                ]
            ]
        )

        mask &= (
            f[
                "high"
            ]
            > prior_high
        )

        mask &= (
            f[
                "close"
            ]
            < prior_high
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
            <= cfg[
                "close_loc_max"
            ]
        )

        mask &= (
            f[
                "upper_wick_body"
            ]
            >= cfg[
                "upper_wick_body_min"
            ]
        )

    elif (
        family
        == "RALLY_FAILURE_BREAKDOWN"
    ):
        mask &= (
            f[
                "rally_12h"
            ]
            >= cfg[
                "rally_12h_min"
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
            <= cfg[
                "close_loc_max"
            ]
        )

        mask &= (
            f[
                "close"
            ]
            < f[
                "prev_low"
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
# BACKTEST
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
            "high"
        ]
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
        - rr
        * reference_risk
    )

    fill = (
        reference_entry
        - cost_pips
        * PIP_SIZE
    )

    actual_risk = (
        stop
        - fill
    )

    if actual_risk <= 0:
        return None

    for j in range(
        signal_index + 1,
        len(candles),
    ):
        candle = candles[j]

        hit_stop = (
            candle[
                "high"
            ]
            >= stop
        )

        hit_target = (
            candle[
                "low"
            ]
            <= target
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
                exit_price = stop
                reason = "STOP"

            else:
                exit_price = target
                reason = "TARGET"

        elif hit_stop:
            exit_price = stop
            reason = "STOP"

        elif hit_target:
            exit_price = target
            reason = "TARGET"

        else:
            continue

        result_r = (
            fill
            - exit_price
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
        id(candles),
        len(candles),
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
        signal_times = [
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
                signal_times,
                start,
            )
        )

        right = (
            len(indices)
            if end is None
            else bisect.bisect_left(
                signal_times,
                end,
            )
        )

        use = indices[
            left:right
        ]

    trades = []
    position = 0

    while position < len(use):
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

    core_pointer = 0

    for trade in candidate:
        start = trade[
            "signal_index"
        ]

        end = trade[
            "exit_index"
        ]

        while (
            core_pointer
            < len(core)
            and core[
                core_pointer
            ][
                "exit_index"
            ] <= start
        ):
            core_pointer += 1

        overlaps = False

        if core_pointer < len(core):
            core_trade = core[
                core_pointer
            ]

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
        key=lambda trade:
            trade[
                "signal_index"
            ]
    )

    return (
        combined,
        accepted,
        rejected,
    )


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


def year_trade_map(
    trades,
):
    result = defaultdict(
        list
    )

    for trade in trades:
        result[
            trade[
                "entry_time"
            ].year
        ].append(
            trade
        )

    return result


def evaluation_row(
    cfg,
    core_trades,
    candidate_trades,
):
    (
        combined,
        accepted,
        rejected,
    ) = nonoverlap_overlay(
        core_trades,
        candidate_trades,
    )

    candidate_stats = stats_from_trades(
        candidate_trades
    )

    accepted_stats = stats_from_trades(
        accepted
    )

    combined_stats = stats_from_trades(
        combined
    )

    pre_candidate = stats_from_trades(
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

    post_candidate = stats_from_trades(
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

    era_stats = [
        stats_from_trades(
            filter_trades(
                candidate_trades,
                start,
                end,
            )
        )
        for (
            _,
            start,
            end,
        ) in ERAS
    ]

    positive_eras = sum(
        1
        for stat in era_stats
        if stat[
            "total_r"
        ] > 0
    )

    accepted_by_year = year_trade_map(
        accepted
    )

    filled = sorted([
        year
        for year
        in MEANINGFUL_CORE_INACTIVE_YEARS
        if len(
            accepted_by_year[
                year
            ]
        ) > 0
    ])

    profitable_filled = sorted([
        year
        for year
        in filled
        if sum(
            trade[
                "result_r"
            ]
            for trade
            in accepted_by_year[
                year
            ]
        ) > 0
    ])

    score = (
        7.0
        * len(
            profitable_filled
        )
        + 4.0
        * (
            len(
                filled
            )
            - len(
                profitable_filled
            )
        )
        + 0.10
        * min(
            len(
                accepted
            ),
            40,
        )
        + 1.2
        * min(
            max(
                accepted_stats[
                    "profit_factor"
                ],
                0.0,
            ),
            3.0,
        )
        + 1.0
        * min(
            max(
                combined_stats[
                    "profit_factor"
                ],
                0.0,
            ),
            3.0,
        )
        + 0.6
        * positive_eras
        + 1.2
        * (
            pre_candidate[
                "total_r"
            ] > 0
        )
        + 1.2
        * (
            post_candidate[
                "total_r"
            ] > 0
        )
        + 0.5
        * min(
            max(
                accepted_stats[
                    "total_r"
                ],
                0.0,
            ) / 10.0,
            3.0,
        )
        - 0.5
        * max(
            0.0,
            abs(
                combined_stats[
                    "max_drawdown_r"
                ]
            )
            - 10.0,
        )
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

        "candidate_dd":
            round(
                candidate_stats[
                    "max_drawdown_r"
                ],
                4,
            ),

        "accepted_adds":
            len(
                accepted
            ),

        "rejected_overlap":
            len(
                rejected
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

        "candidate_pre2010_r":
            round(
                pre_candidate[
                    "total_r"
                ],
                4,
            ),

        "candidate_post2010_r":
            round(
                post_candidate[
                    "total_r"
                ],
                4,
            ),

        "candidate_positive_eras":
            positive_eras,

        "filled_meaningful_years":
            ",".join(
                str(
                    year
                )
                for year in filled
            ),

        "filled_meaningful_year_count":
            len(
                filled
            ),

        "profitably_filled_years":
            ",".join(
                str(
                    year
                )
                for year in profitable_filled
            ),

        "profitably_filled_year_count":
            len(
                profitable_filled
            ),

        "coverage_score":
            round(
                score,
                6,
            ),
    }

    for field in [
        "compression_max",
        "body_atr_min",
        "range_atr_min",
        "breakout_lb",
        "br_min",
        "structure_lb",
        "structure_dist_atr_max",
        "sweep_lb",
        "close_loc_max",
        "upper_wick_body_min",
        "rally_12h_min",
    ]:
        row[
            field
        ] = cfg.get(
            field
        )

    return row


def sort_rows(rows):
    return sorted(
        rows,
        key=lambda row: (
            row[
                "profitably_filled_year_count"
            ],
            row[
                "filled_meaningful_year_count"
            ],
            row[
                "accepted_r"
            ] > 0,
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
                "combined_pf"
            ],
            row[
                "accepted_adds"
            ],
        ),
        reverse=True,
    )


# ============================================================
# PERIOD / ROLLING / CALENDAR
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


def rolling_exact(
    cfg,
    m15,
    core_indices,
    candidate_indices,
):
    rows = []

    first_month = month_floor(
        START
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

            core = run_backtest(
                m15,
                core_indices,
                CORE[
                    "rr"
                ],
                PRIMARY_COST_PIPS,
                start,
                end,
            )

            candidate = run_backtest(
                m15,
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
                    "ACCEPTED_COMPLEMENT",
                    accepted,
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


def rolling_summary_rows(rows):
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

        if not all([
            m15,
            h1,
            h4,
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
            }],
        )

        STATUS.update({
            "state":
                "precomputing",

            "message":
                "Building frozen-core and S2_0022 local feature cache",
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

        features = build_features(
            m15,
            h1_aligned,
            h4_aligned,
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

        parity_m15 = (
            m15[
                :parity_count
            ]
        )

        parity_core_indices = [
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
            parity_m15,
            parity_core_indices,
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
            )
            == 54
        )

        # ----------------------------------------------------
        # HARD S2_0022 ANCHOR PARITY
        # ----------------------------------------------------
        anchor_count = bisect.bisect_right(
            m15_times,
            ANCHOR_LAST_M15_OPEN,
        )

        anchor_m15 = m15[:anchor_count]
        anchor_end = (
            ANCHOR_LAST_M15_OPEN
            + timedelta(minutes=15)
        )

        anchor_indices_all = candidate_signal_indices(
            ANCHOR,
            features,
        )
        anchor_indices = [
            index
            for index in anchor_indices_all
            if index < anchor_count
        ]

        anchor_candidate = run_backtest(
            anchor_m15,
            anchor_indices,
            ANCHOR["rr"],
            PRIMARY_COST_PIPS,
            START,
            anchor_end,
        )

        # Use the already-validated frozen core trades whose signals are
        # within the same historical cutoff. All historical exits for the
        # reference S2_0022 sample occurred before this cutoff.
        anchor_core_indices = [
            index
            for index in core_indices
            if index < anchor_count
        ]
        anchor_core = run_backtest(
            anchor_m15,
            anchor_core_indices,
            CORE["rr"],
            PRIMARY_COST_PIPS,
            START,
            anchor_end,
        )

        (
            anchor_combined,
            anchor_accepted,
            anchor_rejected,
        ) = nonoverlap_overlay(
            anchor_core,
            anchor_candidate,
        )

        anchor_match = (
            len(anchor_candidate) == 130
            and len(anchor_accepted) == 128
            and len(anchor_rejected) == 2
            and len(anchor_combined) == 182
        )

        write_csv(
            OUT_PARITY,
            [{
                "config_id": CORE["config_id"],
                "parity_cutoff_m15_open_utc":
                    iso_utc(PARITY_LAST_M15_OPEN),
                "expected_candidate_trades": None,
                "actual_candidate_trades": None,
                "expected_accepted_nonoverlap": None,
                "actual_accepted_nonoverlap": None,
                "expected_combined_trades": 54,
                "actual_combined_trades": len(parity_core),
                "status": "MATCH" if parity_match else "FAIL",
            }, {
                "config_id": ANCHOR["config_id"],
                "parity_cutoff_m15_open_utc":
                    iso_utc(ANCHOR_LAST_M15_OPEN),
                "expected_candidate_trades": 130,
                "actual_candidate_trades": len(anchor_candidate),
                "expected_accepted_nonoverlap": 128,
                "actual_accepted_nonoverlap": len(anchor_accepted),
                "expected_combined_trades": 182,
                "actual_combined_trades": len(anchor_combined),
                "rejected_overlap": len(anchor_rejected),
                "status": "MATCH" if anchor_match else "FAIL",
            }],
        )

        if not parity_match:
            raise RuntimeError(
                "Frozen core parity failed: "
                f"expected 54 trades, got {len(parity_core)}."
            )

        if not anchor_match:
            raise RuntimeError(
                "S2_0022 anchor parity failed: expected "
                "130 candidate / 128 accepted / 2 overlap / "
                "182 combined, got "
                f"{len(anchor_candidate)} / {len(anchor_accepted)} / "
                f"{len(anchor_rejected)} / {len(anchor_combined)}."
            )

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

        core_by_year = year_trade_map(
            core_trades
        )

        write_csv(
            OUT_CORE,
            [{
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

                "meaningful_inactive_years":
                    ",".join(
                        str(
                            year
                        )
                        for year
                        in sorted(
                            MEANINGFUL_CORE_INACTIVE_YEARS
                        )
                        if not core_by_year[
                            year
                        ]
                    ),
            }],
        )

        # ----------------------------------------------------
        # STAGE 1
        # ----------------------------------------------------
        stage1_configs = (
            build_stage1_configs()
        )

        stage1_map = {
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
                    f"Final-local geometry Stage 1 "
                    f"{i}/{len(stage1_configs)} "
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

            stage1_rows.append(
                evaluation_row(
                    cfg,
                    core_trades,
                    candidate_trades,
                )
            )

        stage1_rows = sort_rows(
            stage1_rows
        )

        write_csv(
            OUT_STAGE1,
            stage1_rows,
        )

        eligible_stage1 = [
            row
            for row in stage1_rows
            if (
                row[
                    "accepted_adds"
                ] >= 8
                and row[
                    "accepted_r"
                ] > 0
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
                ] >= -16.0
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
                for row
                in top_stage1
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
        # STAGE 2 RR
        # ----------------------------------------------------
        stage2_configs = []
        seen = set()
        counter = 0

        for row in top_stage1:
            base = stage1_map[
                row[
                    "config_id"
                ]
            ]

            for rr in RR_VALUES:
                cfg = deepcopy(
                    base
                )

                counter += 1

                cfg[
                    "config_id"
                ] = (
                    f"S2_{counter:04d}"
                )

                cfg[
                    "rr"
                ] = rr

                signature = tuple(
                    (
                        key,
                        repr(
                            cfg.get(
                                key
                            )
                        ),
                    )
                    for key in sorted(
                        cfg.keys()
                    )
                    if key
                    != "config_id"
                )

                if signature in seen:
                    continue

                seen.add(
                    signature
                )

                stage2_configs.append(
                    cfg
                )

        stage2_map = {
            cfg[
                "config_id"
            ]:
                cfg
            for cfg
            in stage2_configs
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
                    f"Final-local RR confirmation "
                    f"{i}/{len(stage2_configs)} "
                    f"{cfg['config_id']}"
                ),
            })

            candidate_indices = (
                candidate_signal_indices(
                    cfg,
                    features,
                )
            )

            candidate_trades = run_backtest(
                m15,
                candidate_indices,
                cfg[
                    "rr"
                ],
                PRIMARY_COST_PIPS,
                START,
                NOW,
            )

            stage2_rows.append(
                evaluation_row(
                    cfg,
                    core_trades,
                    candidate_trades,
                )
            )

        stage2_rows = sort_rows(
            stage2_rows
        )

        write_csv(
            OUT_STAGE2,
            stage2_rows,
        )

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
                ] >= -16.0
            )
        ]

        finalist_rows = (
            eligible_finalists[
                :FINALIST_KEEP
            ]
        )

        # Always deep-test the exact S2_0022 geometry at RR3.50, even if
        # the ranking changes slightly because a new final candle appears.
        anchor_stage2_row = next(
            (
                row
                for row in stage2_rows
                if (
                    row.get("family") == "HIGH_SWEEP_REJECTION"
                    and row.get("context") == "H1_CLOSE_LT_EMA100"
                    and row.get("sweep_lb") == 20
                    and row.get("body_atr_min") == 1.25
                    and row.get("close_loc_max") == 0.40
                    and row.get("upper_wick_body_min") == 0.25
                    and row.get("rr") == 3.50
                )
            ),
            None,
        )

        if (
            anchor_stage2_row is not None
            and anchor_stage2_row not in finalist_rows
        ):
            if len(finalist_rows) >= FINALIST_KEEP:
                finalist_rows[-1] = anchor_stage2_row
            else:
                finalist_rows.append(anchor_stage2_row)

        if len(
            finalist_rows
        ) < FINALIST_KEEP:
            selected = {
                row[
                    "config_id"
                ]
                for row
                in finalist_rows
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
            stage2_map[
                row[
                    "config_id"
                ]
            ]
            for row
            in finalist_rows
        ]

        # ----------------------------------------------------
        # DEEP
        # ----------------------------------------------------
        period_rows = []
        cost_rows = []
        rolling_rows_out = []
        calendar_rows_out = []
        calendar_summary_out = []
        overlap_rows = []
        trade_rows = []

        for i, cfg in enumerate(
            finalist_configs,
            1,
        ):
            STATUS.update({
                "state":
                    "deep_validation",

                "message": (
                    f"Final-local deep finalist "
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

            overlap_rows.append({
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

            # Periods, exact window-local rerun.
            for (
                label,
                start,
                end,
            ) in period_definitions():
                core_period = run_backtest(
                    m15,
                    core_indices,
                    CORE[
                        "rr"
                    ],
                    PRIMARY_COST_PIPS,
                    start,
                    end,
                )

                candidate_period = run_backtest(
                    m15,
                    candidate_indices,
                    cfg[
                        "rr"
                    ],
                    PRIMARY_COST_PIPS,
                    start,
                    end,
                )

                (
                    combined_period,
                    accepted_period,
                    rejected_period,
                ) = nonoverlap_overlay(
                    core_period,
                    candidate_period,
                )

                for mode, trades in [
                    (
                        "CORE_ONLY",
                        core_period,
                    ),
                    (
                        "CANDIDATE_ONLY",
                        candidate_period,
                    ),
                    (
                        "ACCEPTED_COMPLEMENT",
                        accepted_period,
                    ),
                    (
                        "CORE_PLUS_NONOVERLAP",
                        combined_period,
                    ),
                ]:
                    s = stats_from_trades(
                        trades
                    )

                    period_rows.append({
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

                        "accepted_adds":
                            (
                                len(
                                    accepted_period
                                )
                                if mode
                                == "CORE_PLUS_NONOVERLAP"
                                else None
                            ),

                        "rejected_overlap":
                            (
                                len(
                                    rejected_period
                                )
                                if mode
                                == "CORE_PLUS_NONOVERLAP"
                                else None
                            ),
                    })

            # Cost stress.
            for (
                label,
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
                    core_cost = run_backtest(
                        m15,
                        core_indices,
                        CORE[
                            "rr"
                        ],
                        cost,
                        start,
                        end,
                    )

                    candidate_cost = run_backtest(
                        m15,
                        candidate_indices,
                        cfg[
                            "rr"
                        ],
                        cost,
                        start,
                        end,
                    )

                    (
                        combined_cost,
                        accepted_cost,
                        rejected_cost,
                    ) = nonoverlap_overlay(
                        core_cost,
                        candidate_cost,
                    )

                    for mode, trades in [
                        (
                            "CANDIDATE_ONLY",
                            candidate_cost,
                        ),
                        (
                            "ACCEPTED_COMPLEMENT",
                            accepted_cost,
                        ),
                        (
                            "CORE_PLUS_NONOVERLAP",
                            combined_cost,
                        ),
                    ]:
                        s = stats_from_trades(
                            trades
                        )

                        cost_rows.append({
                            "config_id":
                                cfg[
                                    "config_id"
                                ],

                            "mode":
                                mode,

                            "period":
                                label,

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
                        })

            rolling_rows_out.extend(
                rolling_exact(
                    cfg,
                    m15,
                    core_indices,
                    candidate_indices,
                )
            )

            # Calendar exact.
            summary_modes = defaultdict(
                list
            )

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

                core_year = run_backtest(
                    m15,
                    core_indices,
                    CORE[
                        "rr"
                    ],
                    PRIMARY_COST_PIPS,
                    start,
                    end,
                )

                candidate_year = run_backtest(
                    m15,
                    candidate_indices,
                    cfg[
                        "rr"
                    ],
                    PRIMARY_COST_PIPS,
                    start,
                    end,
                )

                (
                    combined_year,
                    accepted_year,
                    rejected_year,
                ) = nonoverlap_overlay(
                    core_year,
                    candidate_year,
                )

                core_inactive = (
                    len(
                        core_year
                    )
                    == 0
                )

                for mode, trades in [
                    (
                        "CORE_ONLY",
                        core_year,
                    ),
                    (
                        "CANDIDATE_ONLY",
                        candidate_year,
                    ),
                    (
                        "ACCEPTED_COMPLEMENT",
                        accepted_year,
                    ),
                    (
                        "CORE_PLUS_NONOVERLAP",
                        combined_year,
                    ),
                ]:
                    s = stats_from_trades(
                        trades
                    )

                    row = {
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
                            core_inactive,

                        "core_inactive_year_filled":
                            (
                                core_inactive
                                and mode
                                == "CORE_PLUS_NONOVERLAP"
                                and s[
                                    "trades"
                                ] > 0
                            ),

                        "meaningful_target_year":
                            year
                            in MEANINGFUL_CORE_INACTIVE_YEARS,
                    }

                    calendar_rows_out.append(
                        row
                    )

                    summary_modes[
                        mode
                    ].append(
                        row
                    )

            for mode, subset in summary_modes.items():
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

                filled = [
                    row[
                        "year"
                    ]
                    for row in subset
                    if row[
                        "core_inactive_year_filled"
                    ]
                ]

                profitable_target_fills = [
                    row[
                        "year"
                    ]
                    for row in subset
                    if (
                        row[
                            "meaningful_target_year"
                        ]
                        and row[
                            "trades"
                        ] > 0
                        and row[
                            "total_r"
                        ] > 0
                    )
                ]

                calendar_summary_out.append({
                    "config_id":
                        cfg[
                            "config_id"
                        ],

                    "mode":
                        mode,

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

                    "profitable_meaningful_target_fills":
                        len(
                            profitable_target_fills
                        ),

                    "profitable_meaningful_target_year_list":
                        ",".join(
                            str(
                                year
                            )
                            for year
                            in profitable_target_fills
                        ),
                })

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

                trade_rows.append(
                    row
                )

        write_csv(
            OUT_PERIODS,
            period_rows,
        )

        write_csv(
            OUT_COST,
            cost_rows,
        )

        write_csv(
            OUT_ROLLING,
            rolling_rows_out,
        )

        write_csv(
            OUT_ROLLING_SUMMARY,
            rolling_summary_rows(
                rolling_rows_out
            ),
        )

        write_csv(
            OUT_CALENDAR,
            calendar_rows_out,
        )

        write_csv(
            OUT_CALENDAR_SUMMARY,
            calendar_summary_out,
        )

        write_csv(
            OUT_OVERLAP,
            overlap_rows,
        )

        write_csv(
            OUT_TRADES,
            trade_rows,
        )

        write_csv(
            OUT_NOTES,
            [{
                "item":
                    "Frozen core",

                "value":
                    "Bearish outside; body>=0.75ATR; closeLoc<=0.35; LB120; distance<=0.05ATR; previous completed H4 close<EMA100; RR4.00.",
            }, {
                "item":
                    "Core parity",

                "value":
                    "Must reproduce 54 trades through 2026-09-09 20:30 UTC M15 open.",
            }, {
                "item":
                    "Meaningful frequency targets",

                "value":
                    "2007, 2010 and 2014. 2002-04 are not optimized targets.",
            }, {
                "item":
                    "Complement families",

                "value":
                    "Final local confirmation only: HIGH_SWEEP_REJECTION around S2_0022; no other family is reopened.",
            }, {
                "item":
                    "Overlap",

                "value":
                    "Candidate trade rejected if [signal_index, exit_index) overlaps a frozen core trade. Exact core exit-candle candidate signal remains eligible.",
            }, {
                "item":
                    "Selection",

                "value":
                    "Ranking keeps the original coverage logic but the search is restricted to the S2_0022 local geometry; exact anchor is forced into deep validation.",
            }, {
                "item":
                    "Final local grid",

                "value":
                    "Sweep LB 10/15/20/30/40; body 1.15/1.25/1.35 ATR; closeLoc 0.35/0.40/0.45; upper wick/body 0.20/0.25/0.30 at RR3.50, then RR 3.25/3.50/3.75/4.00 on the strongest geometries.",
            }, {
                "item":
                    "Historical holdout",

                "value":
                    "No pristine historical holdout remains; interpret as full-history robustness/temporal validation.",
            }],
        )

        STATUS.update({
            "state":
                "packaging",

            "message":
                "Building final-local ZIP results bundle",
        })

        build_bundle()

        STATUS.update({
            "state":
                "complete",

            "message":
                "USD/CAD M15 SHORT complement final local confirmation complete",

            "hard_core_parity":
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
            "USDCAD M15 SHORT Complement Final Local Confirmation",

        "status":
            STATUS[
                "state"
            ],

        "instrument":
            PAIR,

        "timeframe":
            "M15",

        "side":
            "SELL",

        "frozen_core":
            CORE[
                "config_id"
            ],

        "meaningful_target_years":
            sorted(
                MEANINGFUL_CORE_INACTIVE_YEARS
            ),

        "complement_families": [
            "HIGH_SWEEP_REJECTION_FINAL_LOCAL_ONLY",
        ],

        "orders_supported":
            False,

        "trading_enabled":
            False,

        "routes": [
            "/usdcad-m15-short-complement-final-local/status",
            "/usdcad-m15-short-complement-final-local/results",
        ],
    })


@app.route(
    "/usdcad-m15-short-complement-final-local/status"
)
def route_status():
    return jsonify(
        STATUS
    )


@app.route(
    "/usdcad-m15-short-complement-final-local/results"
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
            "usdcad-m15-short-"
            "complement-final-local"
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
