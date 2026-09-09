
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
# USD/CAD M15 LONG — FINALIST CONFIRMATION
#
# PURPOSE
# -------
# Focused confirmation of the full-history bullish-engulfing
# structure edge discovered in the broad USD/CAD M15 LONG run.
#
# This is NOT a fresh archetype search.
#
# Broad-run anchor:
#
#   exact bullish engulf
#   BR >= 1.40
#   body >= 1.25 ATR14
#   range >= 1.50 ATR14
#   structure lookback 165
#   signal low within 0.15 ATR14 of previous 165-bar low
#   previous COMPLETED daily EMA50 > EMA200
#   no session filter
#   no weekday filter
#   RR 5.00
#
# Broad-run anchor result on dataset ending
# 2026-09-09 16:15 UTC:
#
#   71 trades
#   9 pre-2010
#   62 2010+
#
# Strong tighter-BR control:
#
#   same rules but BR >= 1.60, RR 5.00
#
#   64 trades
#   7 pre-2010
#   57 2010+
#
# ============================================================
# WHY THIS SCRIPT EXISTS
# ============================================================
#
# Broad research showed:
#
# - BR 1.50 and 1.60 improved the system.
# - 1.60 was the highest BR tested.
# - RR 5.00 was the highest RR tested.
#
# Therefore both apparent optima were on search boundaries.
# This script extends those boundaries in a controlled way.
#
# ============================================================
# STAGED CONFIRMATION
# ============================================================
#
# STAGE A — ONE-WAY SLICES AROUND THE ANCHOR
#
# BR:
#   1.30 / 1.40 / 1.50 / 1.60 / 1.70
#
# RR:
#   4.00 / 4.25 / 4.50 / 4.75 / 5.00 /
#   5.25 / 5.50 / 5.75 / 6.00
#
# Structure lookback:
#   120 / 165 / 200
#
# Structure distance:
#   0.125 / 0.150 / 0.175 / 0.200 ATR
#
# Body:
#   1.15 / 1.25 / 1.35 ATR
#
# Range:
#   1.30 / 1.50 / 1.70 ATR
#
# Daily context:
#   EMA50 > EMA200             [primary hypothesis]
#   daily close > EMA200       [control]
#
# ------------------------------------------------------------
# STAGE B — STRUCTURE / BR / RR INTERACTION GRID
#
#   5 BR
# x 9 RR
# x 3 structure lookbacks
# x 4 structure distances
#
# = 540 configs
#
# Body remains 1.25 ATR
# Range remains 1.50 ATR
# Context remains daily EMA50 > EMA200
#
# ------------------------------------------------------------
# STAGE C — LOCAL BODY / RANGE / CONTEXT CONFIRMATION
#
# Take the best Stage-B geometries and test:
#
#   body 1.15 / 1.25 / 1.35
#   range 1.30 / 1.50 / 1.70
#   context:
#       EMA50 > EMA200
#       daily close > EMA200
#
# This is intentionally staged rather than a giant combinatorial
# search across every filter at once.
#
# ============================================================
# DEEP FINALIST VALIDATION
# ============================================================
#
# - full history
# - pre-2010
# - 2010+
# - 2002-07
# - 2008-13
# - 2014-19
# - 2020-now
# - 2002-17
# - 2018+
# - last 5Y
# - last 2Y
#
# Costs:
#   0.5 / 1.0 / 1.5 / 2.0 pips
#   full / pre / post
#
# Rolling:
#   12 / 24 / 36 months
#
# Calendar:
#   completed years
#   no-trade years
#   positive active years
#   losing years
#
# ============================================================
# NO LOOKAHEAD
# ============================================================
#
# Daily state:
#   complete_at = next ACTUAL OANDA daily candle OPEN
#   lookup = bisect_right(completion_times, signal_time) - 1
#
# OANDA daily:
#   dailyAlignment=17
#   alignmentTimezone=America/New_York
#
# Signal timestamp:
#   M15 candle OPEN.
#
# ============================================================
# M15 LONG HISTORICAL CONVENTIONS
# ============================================================
#
# OANDA midpoint.
# ATR14 = Wilder/RMA, SMA seeded.
#
# USD/CAD:
#   tick = 0.00001
#   pip  = 0.0001
#
# Exact bullish engulf:
#   previous candle bearish
#   current candle bullish
#   current open <= previous close
#   current close >= previous open
#
# Doji convention:
#   previous body == 0 => body_ratio = 999
#
# Structure:
#   previous N completed M15 bars only
#   current signal candle excluded
#
# Reference entry = signal close.
# Historical long fill = close + adverse cost.
# Stop = signal low - 10 ticks.
# Target based on REFERENCE signal-close risk.
# Actual R based on adverse fill.
#
# Baseline cost = 1.0 pip adverse.
# Stress = 0.5 / 1.0 / 1.5 / 2.0 pips.
#
# Pyramiding = 0.
# Exit checking starts next M15 candle.
# Exact exit-candle signal eligible.
#
# Same-bar LONG tie:
#   if high is closer to candle open => TARGET first
#   otherwise STOP first.
#
# ============================================================
# HISTORICAL INTERPRETATION
# ============================================================
#
# Full history has already been used for research.
# Results are robust in-sample / temporal validation,
# not pristine untouched OOS.
#
# ============================================================
# ONE ZIP
# ============================================================
#
# /usdcad-m15-long-final-confirmation/results
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

# Broad-run dataset endpoint used for exact parity.
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


# ============================================================
# ANCHORS
# ============================================================

ANCHOR = {
    "config_id":
        "ANCHOR_BR140_RR500",

    "br_min":
        1.40,

    "body_atr_min":
        1.25,

    "range_atr_min":
        1.50,

    "structure_lb":
        165,

    "structure_dist_atr_max":
        0.150,

    "context":
        "D_EMA50_GT_EMA200",

    "rr":
        5.00,
}


TIGHT_BR_CONTROL = {
    "config_id":
        "CONTROL_BR160_RR500",

    "br_min":
        1.60,

    "body_atr_min":
        1.25,

    "range_atr_min":
        1.50,

    "structure_lb":
        165,

    "structure_dist_atr_max":
        0.150,

    "context":
        "D_EMA50_GT_EMA200",

    "rr":
        5.00,
}


# ============================================================
# SEARCH VALUES
# ============================================================

BR_VALUES = [
    1.30,
    1.40,
    1.50,
    1.60,
    1.70,
]

RR_VALUES = [
    4.00,
    4.25,
    4.50,
    4.75,
    5.00,
    5.25,
    5.50,
    5.75,
    6.00,
]

STRUCTURE_LBS = [
    120,
    165,
    200,
]

STRUCTURE_DISTANCES = [
    0.125,
    0.150,
    0.175,
    0.200,
]

BODY_VALUES = [
    1.15,
    1.25,
    1.35,
]

RANGE_VALUES = [
    1.30,
    1.50,
    1.70,
]

CONTEXT_VALUES = [
    "D_EMA50_GT_EMA200",
    "D_CLOSE_GT_EMA200",
]

STAGE_B_KEEP = 12
STAGE_C_BASE_KEEP = 8
FINALIST_KEEP = 10


# ============================================================
# OUTPUTS
# ============================================================

OUT_COVERAGE = (
    "usdcad_m15_long_final_confirmation_coverage.csv"
)

OUT_PARITY = (
    "usdcad_m15_long_final_confirmation_parity.csv"
)

OUT_SLICES = (
    "usdcad_m15_long_final_confirmation_one_way_slices.csv"
)

OUT_STRUCTURAL_GRID = (
    "usdcad_m15_long_final_confirmation_structural_rr_grid.csv"
)

OUT_LOCAL_GRID = (
    "usdcad_m15_long_final_confirmation_local_geometry_grid.csv"
)

OUT_FINALISTS = (
    "usdcad_m15_long_final_confirmation_finalists.csv"
)

OUT_PERIODS = (
    "usdcad_m15_long_final_confirmation_periods.csv"
)

OUT_COST = (
    "usdcad_m15_long_final_confirmation_cost_stress.csv"
)

OUT_ROLLING = (
    "usdcad_m15_long_final_confirmation_rolling.csv"
)

OUT_ROLLING_SUMMARY = (
    "usdcad_m15_long_final_confirmation_rolling_summary.csv"
)

OUT_CALENDAR = (
    "usdcad_m15_long_final_confirmation_calendar_years.csv"
)

OUT_CALENDAR_SUMMARY = (
    "usdcad_m15_long_final_confirmation_calendar_summary.csv"
)

OUT_TRADES = (
    "usdcad_m15_long_final_confirmation_trades.csv"
)

OUT_NOTES = (
    "usdcad_m15_long_final_confirmation_notes.csv"
)

OUT_BUNDLE = (
    "USDCAD_M15_LONG_FINAL_CONFIRMATION_RESULTS.zip"
)

STATUS = {
    "state":
        "not_started",

    "message":
        "USD/CAD M15 LONG final confirmation not started",

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

        fraction = fraction[:6].ljust(6, "0")
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
        OUT_SLICES,
        OUT_STRUCTURAL_GRID,
        OUT_LOCAL_GRID,
        OUT_FINALISTS,
        OUT_PERIODS,
        OUT_COST,
        OUT_ROLLING,
        OUT_ROLLING_SUMMARY,
        OUT_CALENDAR,
        OUT_CALENDAR_SUMMARY,
        OUT_TRADES,
        OUT_NOTES,
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


def rolling_previous_low(
    values,
    lookback,
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

        dq.append(
            i
        )

    return result


# ============================================================
# DAILY STATE — NO LOOKAHEAD
# ============================================================

def build_daily_state(
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

            "ema200":
                ema200[
                    i
                ],
        })

    return rows


def align_daily_to_m15(
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
        "close":
            np.full(
                len(
                    m15_times
                ),
                np.nan,
                dtype=float,
            ),

        "ema50":
            np.full(
                len(
                    m15_times
                ),
                np.nan,
                dtype=float,
            ),

        "ema200":
            np.full(
                len(
                    m15_times
                ),
                np.nan,
                dtype=float,
            ),
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

        result[
            "close"
        ][
            i
        ] = row[
            "close"
        ]

        if row[
            "ema50"
        ] is not None:
            result[
                "ema50"
            ][
                i
            ] = row[
                "ema50"
            ]

        if row[
            "ema200"
        ] is not None:
            result[
                "ema200"
            ][
                i
            ] = row[
                "ema200"
            ]

    return result


# ============================================================
# FEATURE CACHE
# ============================================================

def build_features(
    m15,
    daily_aligned,
):
    n = len(
        m15
    )

    times = [
        candle[
            "time"
        ]
        for candle in m15
    ]

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

    positive_prev_body = (
        previous_body
        > 0
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

    # Locked project doji convention.
    body_ratio[
        previous_body
        == 0
    ] = 999.0

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

    previous_lows = {}

    structure_distance = {}

    for lookback in STRUCTURE_LBS:
        previous_low = (
            rolling_previous_low(
                lows,
                lookback,
            )
        )

        previous_lows[
            lookback
        ] = previous_low

        distance = np.full(
            n,
            np.nan,
            dtype=float,
        )

        valid = (
            valid_atr
            & np.isfinite(
                previous_low
            )
        )

        distance[
            valid
        ] = (
            np.abs(
                lows[
                    valid
                ]
                - previous_low[
                    valid
                ]
            )
            / atr[
                valid
            ]
        )

        structure_distance[
            lookback
        ] = distance

    return {
        "times":
            times,

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

        "exact_bull_engulf":
            exact_bull_engulf,

        "body_ratio":
            body_ratio,

        "body_atr":
            body_atr,

        "range_atr":
            range_atr,

        "previous_lows":
            previous_lows,

        "structure_distance":
            structure_distance,

        "d_close":
            daily_aligned[
                "close"
            ],

        "d_ema50":
            daily_aligned[
                "ema50"
            ],

        "d_ema200":
            daily_aligned[
                "ema200"
            ],
    }


# ============================================================
# SIGNAL LOGIC
# ============================================================

def signal_indices(
    cfg,
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
            "range_atr"
        ]
        >= cfg[
            "range_atr_min"
        ]
    )

    mask &= (
        f[
            "structure_distance"
        ][
            cfg[
                "structure_lb"
            ]
        ]
        <= cfg[
            "structure_dist_atr_max"
        ]
    )

    context = cfg[
        "context"
    ]

    if (
        context
        == "D_EMA50_GT_EMA200"
    ):
        mask &= (
            f[
                "d_ema50"
            ]
            > f[
                "d_ema200"
            ]
        )

    elif (
        context
        == "D_CLOSE_GT_EMA200"
    ):
        mask &= (
            f[
                "d_close"
            ]
            > f[
                "d_ema200"
            ]
        )

    else:
        raise RuntimeError(
            f"Unknown context: {context}"
        )

    mask[
        :220
    ] = False

    return np.flatnonzero(
        mask
    ).tolist()


# ============================================================
# LONG BACKTEST
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

    loss_streak = 0
    longest_loss_streak = 0

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
            longest_loss_streak,
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
            START,
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
            START,
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

    for (
        _,
        start,
        end,
    ) in ERAS:
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
        for s in era_stats
    )

    # Balanced score: rewards full/pre/post edge and long-era
    # stability without selecting solely on headline PF.
    score = (
        1.40
        * min(
            full[
                "profit_factor"
            ],
            3.0,
        )
        + 0.90
        * min(
            pre[
                "profit_factor"
            ],
            3.0,
        )
        + 0.90
        * min(
            post[
                "profit_factor"
            ],
            3.0,
        )
        + 0.45
        * positive_eras
        + 0.30
        * min(
            max(
                min_era_pf,
                0.0,
            ),
            2.5,
        )
        + 0.20
        * min(
            full[
                "trades"
            ]
            / 80.0,
            1.5,
        )
        + 0.15
        * max(
            full[
                "expectancy_r"
            ],
            -1.0,
        )
    )

    row = {
        "config_id":
            cfg[
                "config_id"
            ],

        "br_min":
            cfg[
                "br_min"
            ],

        "body_atr_min":
            cfg[
                "body_atr_min"
            ],

        "range_atr_min":
            cfg[
                "range_atr_min"
            ],

        "structure_lb":
            cfg[
                "structure_lb"
            ],

        "structure_dist_atr_max":
            cfg[
                "structure_dist_atr_max"
            ],

        "context":
            cfg[
                "context"
            ],

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
    cfg = deepcopy(
        ANCHOR
    )

    cfg[
        "config_id"
    ] = config_id

    return cfg


def build_one_way_slices():
    configs = []

    for value in BR_VALUES:
        cfg = clone_anchor(
            f"SLICE_BR_{value:.2f}"
        )

        cfg[
            "br_min"
        ] = value

        cfg[
            "slice"
        ] = "BR"

        configs.append(
            cfg
        )

    for value in RR_VALUES:
        cfg = clone_anchor(
            f"SLICE_RR_{value:.2f}"
        )

        cfg[
            "rr"
        ] = value

        cfg[
            "slice"
        ] = "RR"

        configs.append(
            cfg
        )

    for value in STRUCTURE_LBS:
        cfg = clone_anchor(
            f"SLICE_LB_{value}"
        )

        cfg[
            "structure_lb"
        ] = value

        cfg[
            "slice"
        ] = "STRUCTURE_LB"

        configs.append(
            cfg
        )

    for value in STRUCTURE_DISTANCES:
        cfg = clone_anchor(
            f"SLICE_DIST_{value:.3f}"
        )

        cfg[
            "structure_dist_atr_max"
        ] = value

        cfg[
            "slice"
        ] = "STRUCTURE_DISTANCE"

        configs.append(
            cfg
        )

    for value in BODY_VALUES:
        cfg = clone_anchor(
            f"SLICE_BODY_{value:.2f}"
        )

        cfg[
            "body_atr_min"
        ] = value

        cfg[
            "slice"
        ] = "BODY"

        configs.append(
            cfg
        )

    for value in RANGE_VALUES:
        cfg = clone_anchor(
            f"SLICE_RANGE_{value:.2f}"
        )

        cfg[
            "range_atr_min"
        ] = value

        cfg[
            "slice"
        ] = "RANGE"

        configs.append(
            cfg
        )

    for value in CONTEXT_VALUES:
        cfg = clone_anchor(
            f"SLICE_CONTEXT_{value}"
        )

        cfg[
            "context"
        ] = value

        cfg[
            "slice"
        ] = "CONTEXT"

        configs.append(
            cfg
        )

    return configs


def build_structural_rr_grid():
    configs = []
    counter = 0

    for br in BR_VALUES:
        for rr in RR_VALUES:
            for structure_lb in STRUCTURE_LBS:
                for distance in STRUCTURE_DISTANCES:
                    counter += 1

                    cfg = clone_anchor(
                        f"STRUCT_{counter:04d}"
                    )

                    cfg[
                        "br_min"
                    ] = br

                    cfg[
                        "rr"
                    ] = rr

                    cfg[
                        "structure_lb"
                    ] = structure_lb

                    cfg[
                        "structure_dist_atr_max"
                    ] = distance

                    configs.append(
                        cfg
                    )

    return configs


def build_local_geometry_grid(
    structural_by_id,
    top_rows,
):
    configs = []
    seen = set()
    counter = 0

    for rank, row in enumerate(
        top_rows[
            :STAGE_C_BASE_KEEP
        ]
    ):
        base = deepcopy(
            structural_by_id[
                row[
                    "config_id"
                ]
            ]
        )

        for body in BODY_VALUES:
            for rng in RANGE_VALUES:
                for context in CONTEXT_VALUES:
                    cfg = deepcopy(
                        base
                    )

                    cfg[
                        "body_atr_min"
                    ] = body

                    cfg[
                        "range_atr_min"
                    ] = rng

                    cfg[
                        "context"
                    ] = context

                    signature = (
                        cfg[
                            "br_min"
                        ],
                        cfg[
                            "rr"
                        ],
                        cfg[
                            "structure_lb"
                        ],
                        cfg[
                            "structure_dist_atr_max"
                        ],
                        cfg[
                            "body_atr_min"
                        ],
                        cfg[
                            "range_atr_min"
                        ],
                        cfg[
                            "context"
                        ],
                    )

                    if signature in seen:
                        continue

                    seen.add(
                        signature
                    )

                    counter += 1

                    cfg[
                        "config_id"
                    ] = (
                        f"LOCAL_{counter:04d}_"
                        f"B{rank:02d}"
                    )

                    configs.append(
                        cfg
                    )

    return configs


# ============================================================
# DEEP OUTPUTS
# ============================================================

def result_row(
    cfg,
    label,
    trades,
):
    s = stats_from_trades(
        trades
    )

    return {
        "config_id":
            cfg[
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


def period_rows(
    cfg,
    candles,
    indices,
):
    rows = []

    for (
        label,
        start,
        end,
    ) in period_definitions():
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

        row = result_row(
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
    ]

    for (
        label,
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

            row = result_row(
                cfg,
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
    cfg,
    candles,
    indices,
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

            s = stats_from_trades(
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


def calendar_rows(
    cfg,
    candles,
    indices,
):
    rows = []

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

        s = stats_from_trades(
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

        negative_years = [
            row[
                "year"
            ]
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

            "no_trade_years":
                len(
                    zero_years
                ),

            "no_trade_year_list":
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

            "negative_years":
                len(
                    negative_years
                ),

            "negative_year_list":
                ",".join(
                    str(
                        year
                    )
                    for year in negative_years
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

        daily = fetch_history(
            "D",
            HTF_WARMUP_START,
            NOW,
            3500,
        )

        if not all([
            m15,
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
                "Building completed daily state and M15 finalist feature cache",
        })

        m15_times = [
            candle[
                "time"
            ]
            for candle in m15
        ]

        daily_aligned = (
            align_daily_to_m15(
                m15_times,
                build_daily_state(
                    daily
                ),
            )
        )

        features = build_features(
            m15,
            daily_aligned,
        )

        # ----------------------------------------------------
        # PARITY AGAINST BROAD RUN
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

        parity_end = (
            PARITY_LAST_M15_OPEN
            + timedelta(
                minutes=15
            )
        )

        parity_rows = []

        parity_ok = True

        for (
            cfg,
            expected_full,
            expected_pre,
            expected_post,
        ) in [
            (
                ANCHOR,
                71,
                9,
                62,
            ),
            (
                TIGHT_BR_CONTROL,
                64,
                7,
                57,
            ),
        ]:
            full_indices = signal_indices(
                cfg,
                features,
            )

            parity_indices = [
                index
                for index in full_indices
                if index < parity_count
            ]

            trades = run_backtest(
                parity_candles,
                parity_indices,
                cfg[
                    "rr"
                ],
                PRIMARY_COST_PIPS,
                START,
                parity_end,
            )

            pre = stats_from_trades([
                trade
                for trade in trades
                if trade[
                    "entry_time"
                ] < datetime(
                    2010,
                    1,
                    1,
                    tzinfo=timezone.utc,
                )
            ])

            post = stats_from_trades([
                trade
                for trade in trades
                if trade[
                    "entry_time"
                ] >= datetime(
                    2010,
                    1,
                    1,
                    tzinfo=timezone.utc,
                )
            ])

            match = (
                len(
                    trades
                ) == expected_full
                and pre[
                    "trades"
                ] == expected_pre
                and post[
                    "trades"
                ] == expected_post
            )

            parity_ok = (
                parity_ok
                and match
            )

            parity_rows.append({
                "config_id":
                    cfg[
                        "config_id"
                    ],

                "expected_full":
                    expected_full,

                "actual_full":
                    len(
                        trades
                    ),

                "expected_pre2010":
                    expected_pre,

                "actual_pre2010":
                    pre[
                        "trades"
                    ],

                "expected_post2010":
                    expected_post,

                "actual_post2010":
                    post[
                        "trades"
                    ],

                "status":
                    (
                        "MATCH"
                        if match
                        else "FAIL"
                    ),
            })

        write_csv(
            OUT_PARITY,
            parity_rows,
        )

        if not parity_ok:
            raise RuntimeError(
                "Broad-run parity failed. "
                "Do not trust USD/CAD M15 LONG confirmation results."
            )

        # ----------------------------------------------------
        # STAGE A — ONE-WAY SLICES
        # ----------------------------------------------------
        slice_configs = (
            build_one_way_slices()
        )

        slice_rows = []

        for i, cfg in enumerate(
            slice_configs,
            1,
        ):
            STATUS.update({
                "state":
                    "one_way_slices",

                "message": (
                    f"Slice "
                    f"{i}/{len(slice_configs)} "
                    f"{cfg['config_id']}"
                ),
            })

            indices = signal_indices(
                cfg,
                features,
            )

            row = evaluation_row(
                cfg,
                m15,
                indices,
            )

            row[
                "slice"
            ] = cfg[
                "slice"
            ]

            slice_rows.append(
                row
            )

        write_csv(
            OUT_SLICES,
            slice_rows,
        )

        # ----------------------------------------------------
        # STAGE B — BR / RR / STRUCTURE GRID
        # ----------------------------------------------------
        structural_configs = (
            build_structural_rr_grid()
        )

        structural_by_id = {
            cfg[
                "config_id"
            ]:
                cfg
            for cfg in structural_configs
        }

        structural_rows = []

        for i, cfg in enumerate(
            structural_configs,
            1,
        ):
            STATUS.update({
                "state":
                    "structural_rr_grid",

                "message": (
                    f"Structural grid "
                    f"{i}/{len(structural_configs)} "
                    f"{cfg['config_id']}"
                ),
            })

            indices = signal_indices(
                cfg,
                features,
            )

            structural_rows.append(
                evaluation_row(
                    cfg,
                    m15,
                    indices,
                )
            )

        structural_rows = sort_rows(
            structural_rows
        )

        write_csv(
            OUT_STRUCTURAL_GRID,
            structural_rows,
        )

        structural_eligible = [
            row
            for row in structural_rows
            if (
                row[
                    "full_trades"
                ] >= 55
                and row[
                    "pre2010_trades"
                ] >= 6
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

        top_structural = (
            structural_eligible[
                :STAGE_B_KEEP
            ]
        )

        if len(
            top_structural
        ) < STAGE_B_KEEP:
            selected_ids = {
                row[
                    "config_id"
                ]
                for row in top_structural
            }

            for row in structural_rows:
                if row[
                    "config_id"
                ] in selected_ids:
                    continue

                top_structural.append(
                    row
                )

                selected_ids.add(
                    row[
                        "config_id"
                    ]
                )

                if len(
                    top_structural
                ) >= STAGE_B_KEEP:
                    break

        # ----------------------------------------------------
        # STAGE C — LOCAL BODY / RANGE / CONTEXT
        # ----------------------------------------------------
        local_configs = (
            build_local_geometry_grid(
                structural_by_id,
                top_structural,
            )
        )

        local_by_id = {
            cfg[
                "config_id"
            ]:
                cfg
            for cfg in local_configs
        }

        local_rows = []

        for i, cfg in enumerate(
            local_configs,
            1,
        ):
            STATUS.update({
                "state":
                    "local_geometry_grid",

                "message": (
                    f"Local grid "
                    f"{i}/{len(local_configs)} "
                    f"{cfg['config_id']}"
                ),
            })

            indices = signal_indices(
                cfg,
                features,
            )

            local_rows.append(
                evaluation_row(
                    cfg,
                    m15,
                    indices,
                )
            )

        local_rows = sort_rows(
            local_rows
        )

        write_csv(
            OUT_LOCAL_GRID,
            local_rows,
        )

        # ----------------------------------------------------
        # FINALISTS
        #
        # Always deep-test:
        #   - original anchor
        #   - BR1.60 / RR5 control
        #
        # Then add best local configs.
        # ----------------------------------------------------
        finalist_configs = [
            deepcopy(
                ANCHOR
            ),
            deepcopy(
                TIGHT_BR_CONTROL
            ),
        ]

        finalist_rows = []

        for cfg in finalist_configs:
            indices = signal_indices(
                cfg,
                features,
            )

            finalist_rows.append(
                evaluation_row(
                    cfg,
                    m15,
                    indices,
                )
            )

        selected_signatures = {
            (
                cfg[
                    "br_min"
                ],
                cfg[
                    "rr"
                ],
                cfg[
                    "structure_lb"
                ],
                cfg[
                    "structure_dist_atr_max"
                ],
                cfg[
                    "body_atr_min"
                ],
                cfg[
                    "range_atr_min"
                ],
                cfg[
                    "context"
                ],
            )
            for cfg in finalist_configs
        }

        eligible_local = [
            row
            for row in local_rows
            if (
                row[
                    "full_trades"
                ] >= 50
                and row[
                    "pre2010_trades"
                ] >= 5
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

        for row in eligible_local:
            cfg = local_by_id[
                row[
                    "config_id"
                ]
            ]

            signature = (
                cfg[
                    "br_min"
                ],
                cfg[
                    "rr"
                ],
                cfg[
                    "structure_lb"
                ],
                cfg[
                    "structure_dist_atr_max"
                ],
                cfg[
                    "body_atr_min"
                ],
                cfg[
                    "range_atr_min"
                ],
                cfg[
                    "context"
                ],
            )

            if signature in selected_signatures:
                continue

            finalist_configs.append(
                deepcopy(
                    cfg
                )
            )

            finalist_rows.append(
                row
            )

            selected_signatures.add(
                signature
            )

            if len(
                finalist_configs
            ) >= FINALIST_KEEP:
                break

        if len(
            finalist_configs
        ) < FINALIST_KEEP:
            for row in local_rows:
                cfg = local_by_id[
                    row[
                        "config_id"
                    ]
                ]

                signature = (
                    cfg[
                        "br_min"
                    ],
                    cfg[
                        "rr"
                    ],
                    cfg[
                        "structure_lb"
                    ],
                    cfg[
                        "structure_dist_atr_max"
                    ],
                    cfg[
                        "body_atr_min"
                    ],
                    cfg[
                        "range_atr_min"
                    ],
                    cfg[
                        "context"
                    ],
                )

                if signature in selected_signatures:
                    continue

                finalist_configs.append(
                    deepcopy(
                        cfg
                    )
                )

                finalist_rows.append(
                    row
                )

                selected_signatures.add(
                    signature
                )

                if len(
                    finalist_configs
                ) >= FINALIST_KEEP:
                    break

        write_csv(
            OUT_FINALISTS,
            sort_rows(
                finalist_rows
            ),
        )

        # ----------------------------------------------------
        # DEEP VALIDATION
        # ----------------------------------------------------
        period_output = []
        cost_output = []
        rolling_output = []
        calendar_output = []
        trade_output = []

        for i, cfg in enumerate(
            finalist_configs,
            1,
        ):
            STATUS.update({
                "state":
                    "deep_validation",

                "message": (
                    f"Deep validation "
                    f"{i}/{len(finalist_configs)} "
                    f"{cfg['config_id']}"
                ),
            })

            indices = signal_indices(
                cfg,
                features,
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
                START,
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

        write_csv(
            OUT_NOTES,
            [{
                "item":
                    "Purpose",

                "value":
                    "Focused USD/CAD M15 LONG confirmation of the bullish-engulfing structure edge; no fresh archetype search.",
            }, {
                "item":
                    "Boundary extension",

                "value":
                    "BR extended through 1.70 and RR through 6.00 because broad-run optima BR1.60 and RR5.00 were search-boundary values.",
            }, {
                "item":
                    "Staged method",

                "value":
                    "One-way slices -> 540-config BR/RR/structure grid -> local body/range/context confirmation around top structural candidates.",
            }, {
                "item":
                    "Primary regime hypothesis",

                "value":
                    "Previous completed daily EMA50 > EMA200.",
            }, {
                "item":
                    "Regime control",

                "value":
                    "Previous completed daily close > EMA200.",
            }, {
                "item":
                    "Historical holdout",

                "value":
                    "No pristine historical holdout remains because full history is already part of the research universe.",
            }, {
                "item":
                    "Decision rule",

                "value":
                    "Do not lock solely on maximum PF or R. Prefer interior/plateau parameters, cost survival, long-era balance, recent 2Y/5Y behaviour, rolling consistency, calendar consistency and sane trade count.",
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
                "USD/CAD M15 LONG final confirmation complete",

            "parity":
                "MATCH",

            "one_way_configs":
                len(
                    slice_configs
                ),

            "structural_rr_configs":
                len(
                    structural_configs
                ),

            "local_geometry_configs":
                len(
                    local_configs
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
            "USDCAD M15 LONG Finalist Confirmation",

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
            "exact_bullish_engulf":
                True,

            "br_min":
                1.40,

            "body_atr_min":
                1.25,

            "range_atr_min":
                1.50,

            "structure_lb":
                165,

            "structure_dist_atr_max":
                0.150,

            "context":
                "previous completed daily EMA50 > EMA200",

            "rr":
                5.00,
        },

        "boundary_extension": {
            "br":
                BR_VALUES,

            "rr":
                RR_VALUES,
        },

        "orders_supported":
            False,

        "trading_enabled":
            False,

        "routes": [
            "/usdcad-m15-long-final-confirmation/status",
            "/usdcad-m15-long-final-confirmation/results",
        ],
    })


@app.route(
    "/usdcad-m15-long-final-confirmation/status"
)
def route_status():
    return jsonify(
        STATUS
    )


@app.route(
    "/usdcad-m15-long-final-confirmation/results"
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
