
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
# USD/CAD M15 LONG — FINAL HEAD-TO-HEAD
#
# PURPOSE
# -------
# FINAL comparison only.
#
# No fresh archetype search.
# No broad optimisation.
#
# Compare the two remaining regime variants:
#
# A) EMA50 > EMA200 regime
#    body >= 1.25 ATR
#    range >= 1.50 ATR
#    structure LB165 / distance 0.175 ATR
#    RR 5.00
#
# B) Daily close > EMA200 regime
#    body >= 1.25 ATR
#    range >= 1.30 ATR
#    structure LB165 / distance 0.175 ATR
#    RR 4.75
#
# For each:
#    BR 1.60 / 1.70 / 1.80 / 1.90
#
# Total exact configs:
#    8
#
# ============================================================
# DECISION OUTPUTS
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
#   0.5 / 1.0 / 1.5 / 2.0 pip
#   full / pre / post
#
# Rolling:
#   12 / 24 / 36 months
#
# Calendar:
#   active years
#   positive active years
#   losing years
#   no-trade years
#
# ============================================================
# PARITY
# ============================================================
#
# Original confirmation dataset:
#   through 2026-09-09 16:15 UTC M15 open
#
# Broad anchor:
#   BR1.40 / body1.25 / range1.50 /
#   LB165 / dist0.150 / EMA50>EMA200 / RR5
#   expected 71 / 9 / 62
#
# Tight-BR control:
#   same but BR1.60
#   expected 64 / 7 / 57
#
# Head-to-head reference counts:
#
# EMA50 winner geometry:
#   BR1.70 / body1.25 / range1.50 /
#   LB165 / dist0.175 / RR5
#   expected ~70 full trades
#
# Daily-close candidate:
#   BR1.70 / body1.25 / range1.30 /
#   LB165 / dist0.175 / close>EMA200 / RR4.75
#   expected ~61 full trades
#
# ============================================================
# NO LOOKAHEAD
# ============================================================
#
# Daily state:
#   complete_at = next ACTUAL OANDA daily candle OPEN
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
# M15 LONG HISTORICAL CONVENTIONS
# ============================================================
#
# OANDA midpoint
# ATR14 Wilder/RMA, SMA seeded
#
# USD/CAD:
#   tick = 0.00001
#   pip  = 0.0001
#
# Exact bullish engulf:
#   previous bearish
#   current bullish
#   current open <= previous close
#   current close >= previous open
#
# Doji convention:
#   previous body == 0 => BR = 999
#
# Reference entry:
#   signal close
#
# Historical long adverse fill:
#   close + adverse cost
#
# Stop:
#   signal low - 10 ticks
#
# Target:
#   based on REFERENCE close risk
#
# Actual R:
#   based on adverse fill
#
# Pyramiding:
#   0
#
# Exit:
#   begins next candle
#   exact exit-candle signal eligible
#
# Same-bar LONG:
#   high closer to candle open => TARGET first
#   otherwise STOP first
#
# ============================================================
# ONE ZIP
# ============================================================
#
# /usdcad-m15-long-final-head-to-head/results
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


# ============================================================
# PARITY CONTROLS
# ============================================================

PARITY_ANCHOR = {
    "config_id":
        "PARITY_ANCHOR",

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


PARITY_TIGHT = {
    "config_id":
        "PARITY_TIGHT_BR160",

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
# FINAL 8 CONFIGS
# ============================================================

FINAL_CONFIGS = []

for br in [
    1.60,
    1.70,
    1.80,
    1.90,
]:
    FINAL_CONFIGS.append({
        "config_id":
            f"EMA50_REGIME_BR{int(br * 100):03d}",

        "family":
            "EMA50_GT_EMA200",

        "br_min":
            br,

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
    })

for br in [
    1.60,
    1.70,
    1.80,
    1.90,
]:
    FINAL_CONFIGS.append({
        "config_id":
            f"DAILY_CLOSE_REGIME_BR{int(br * 100):03d}",

        "family":
            "D_CLOSE_GT_EMA200",

        "br_min":
            br,

        "body_atr_min":
            1.25,

        "range_atr_min":
            1.30,

        "structure_lb":
            165,

        "structure_dist_atr_max":
            0.175,

        "context":
            "D_CLOSE_GT_EMA200",

        "rr":
            4.75,
    })


# ============================================================
# OUTPUTS
# ============================================================

OUT_COVERAGE = (
    "usdcad_m15_long_final_head_to_head_coverage.csv"
)

OUT_PARITY = (
    "usdcad_m15_long_final_head_to_head_parity.csv"
)

OUT_EXACT = (
    "usdcad_m15_long_final_head_to_head_exact.csv"
)

OUT_PERIODS = (
    "usdcad_m15_long_final_head_to_head_periods.csv"
)

OUT_COST = (
    "usdcad_m15_long_final_head_to_head_cost_stress.csv"
)

OUT_ROLLING = (
    "usdcad_m15_long_final_head_to_head_rolling.csv"
)

OUT_ROLLING_SUMMARY = (
    "usdcad_m15_long_final_head_to_head_rolling_summary.csv"
)

OUT_CALENDAR = (
    "usdcad_m15_long_final_head_to_head_calendar_years.csv"
)

OUT_CALENDAR_SUMMARY = (
    "usdcad_m15_long_final_head_to_head_calendar_summary.csv"
)

OUT_TRADES = (
    "usdcad_m15_long_final_head_to_head_trades.csv"
)

OUT_NOTES = (
    "usdcad_m15_long_final_head_to_head_notes.csv"
)

OUT_BUNDLE = (
    "USDCAD_M15_LONG_FINAL_HEAD_TO_HEAD_RESULTS.zip"
)

STATUS = {
    "state":
        "not_started",

    "message":
        "USD/CAD M15 LONG final head-to-head not started",

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
        OUT_EXACT,
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
# FEATURES
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

    # Project-standard doji convention.
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

    previous_low165 = (
        rolling_previous_low(
            lows,
            165,
        )
    )

    structure_distance165 = np.full(
        n,
        np.nan,
        dtype=float,
    )

    valid_structure = (
        valid_atr
        & np.isfinite(
            previous_low165
        )
    )

    structure_distance165[
        valid_structure
    ] = (
        np.abs(
            lows[
                valid_structure
            ]
            - previous_low165[
                valid_structure
            ]
        )
        / atr[
            valid_structure
        ]
    )

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

        "exact_bull_engulf":
            exact_bull_engulf,

        "body_ratio":
            body_ratio,

        "body_atr":
            body_atr,

        "range_atr":
            range_atr,

        "previous_low165":
            previous_low165,

        "structure_distance165":
            structure_distance165,

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
            "structure_distance165"
        ]
        <= cfg[
            "structure_dist_atr_max"
        ]
    )

    if (
        cfg[
            "context"
        ]
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
        cfg[
            "context"
        ]
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
            "Unknown context"
        )

    mask[
        :220
    ] = False

    return np.flatnonzero(
        mask
    ).tolist()


# ============================================================
# LONG BACKTEST ENGINE
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


def result_row(
    cfg,
    period,
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

        "family":
            cfg[
                "family"
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

        "period":
            period,

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


def rolling_rows_for_trades(
    cfg,
    trades,
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

            subset = filter_trades(
                trades,
                start,
                end,
            )

            s = stats_from_trades(
                subset
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


def calendar_rows_for_trades(
    cfg,
    trades,
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

        subset = filter_trades(
            trades,
            start,
            end,
        )

        s = stats_from_trades(
            subset
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
                "Building completed daily state and exact 8-config feature cache",
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
        # HARD PARITY
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
        hard_parity_ok = True

        for (
            cfg,
            expected_full,
            expected_pre,
            expected_post,
        ) in [
            (
                PARITY_ANCHOR,
                71,
                9,
                62,
            ),
            (
                PARITY_TIGHT,
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

            pre = [
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
            ]

            post = [
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
            ]

            match = (
                len(
                    trades
                ) == expected_full
                and len(
                    pre
                ) == expected_pre
                and len(
                    post
                ) == expected_post
            )

            hard_parity_ok = (
                hard_parity_ok
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
                    len(
                        pre
                    ),

                "expected_post2010":
                    expected_post,

                "actual_post2010":
                    len(
                        post
                    ),

                "status":
                    (
                        "MATCH"
                        if match
                        else "FAIL"
                    ),
            })

        if not hard_parity_ok:
            write_csv(
                OUT_PARITY,
                parity_rows,
            )

            raise RuntimeError(
                "Hard parity failed. "
                "Do not trust final head-to-head results."
            )

        # Soft reference counts for the two headline BR1.70 configs.
        for cfg, expected_full in [
            (
                next(
                    c
                    for c in FINAL_CONFIGS
                    if c[
                        "config_id"
                    ] == "EMA50_REGIME_BR170"
                ),
                70,
            ),
            (
                next(
                    c
                    for c in FINAL_CONFIGS
                    if c[
                        "config_id"
                    ] == "DAILY_CLOSE_REGIME_BR170"
                ),
                61,
            ),
        ]:
            indices = signal_indices(
                cfg,
                features,
            )

            parity_indices = [
                index
                for index in indices
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

                "status":
                    (
                        "MATCH"
                        if len(
                            trades
                        ) == expected_full
                        else "CHECK"
                    ),
            })

        write_csv(
            OUT_PARITY,
            parity_rows,
        )

        # ----------------------------------------------------
        # EXACT 8-CONFIG DEEP VALIDATION
        # ----------------------------------------------------
        exact_rows = []
        period_output = []
        cost_output = []
        rolling_output = []
        calendar_output = []
        trade_output = []

        for i, cfg in enumerate(
            FINAL_CONFIGS,
            1,
        ):
            STATUS.update({
                "state":
                    "deep_validation",

                "message": (
                    f"Deep validation "
                    f"{i}/{len(FINAL_CONFIGS)} "
                    f"{cfg['config_id']}"
                ),
            })

            indices = signal_indices(
                cfg,
                features,
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

            full_stats = stats_from_trades(
                full_trades
            )

            era_stats = []

            for (
                _,
                era_start,
                era_end,
            ) in ERAS:
                era_stats.append(
                    stats_from_trades(
                        filter_trades(
                            full_trades,
                            era_start,
                            era_end,
                        )
                    )
                )

            exact_rows.append({
                "config_id":
                    cfg[
                        "config_id"
                    ],

                "family":
                    cfg[
                        "family"
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

                "trades":
                    full_stats[
                        "trades"
                    ],

                "profit_factor":
                    round(
                        full_stats[
                            "profit_factor"
                        ],
                        6,
                    ),

                "total_r":
                    round(
                        full_stats[
                            "total_r"
                        ],
                        4,
                    ),

                "expectancy_r":
                    round(
                        full_stats[
                            "expectancy_r"
                        ],
                        6,
                    ),

                "max_drawdown_r":
                    round(
                        full_stats[
                            "max_drawdown_r"
                        ],
                        4,
                    ),

                "positive_eras":
                    sum(
                        1
                        for s in era_stats
                        if s[
                            "total_r"
                        ] > 0
                    ),

                "min_era_pf":
                    round(
                        min(
                            s[
                                "profit_factor"
                            ]
                            for s in era_stats
                        ),
                        6,
                    ),

                "era1_pf":
                    round(
                        era_stats[
                            0
                        ][
                            "profit_factor"
                        ],
                        6,
                    ),

                "era2_pf":
                    round(
                        era_stats[
                            1
                        ][
                            "profit_factor"
                        ],
                        6,
                    ),

                "era3_pf":
                    round(
                        era_stats[
                            2
                        ][
                            "profit_factor"
                        ],
                        6,
                    ),

                "era4_pf":
                    round(
                        era_stats[
                            3
                        ][
                            "profit_factor"
                        ],
                        6,
                    ),
            })

            for (
                label,
                period_start,
                period_end,
            ) in period_definitions():
                subset = filter_trades(
                    full_trades,
                    period_start,
                    period_end,
                )

                row = result_row(
                    cfg,
                    label,
                    subset,
                )

                row[
                    "start_utc"
                ] = iso_utc(
                    period_start
                )

                row[
                    "end_utc"
                ] = iso_utc(
                    period_end
                )

                period_output.append(
                    row
                )

            # Cost stress is re-run because adverse cost can affect
            # actual R even when stop/target geometry is unchanged.
            for (
                period_label,
                period_start,
                period_end,
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
                    cost_trades = run_backtest(
                        m15,
                        indices,
                        cfg[
                            "rr"
                        ],
                        cost,
                        period_start,
                        period_end,
                    )

                    s = stats_from_trades(
                        cost_trades
                    )

                    cost_output.append({
                        "config_id":
                            cfg[
                                "config_id"
                            ],

                        "period":
                            period_label,

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

            rolling_output.extend(
                rolling_rows_for_trades(
                    cfg,
                    full_trades,
                )
            )

            calendar_output.extend(
                calendar_rows_for_trades(
                    cfg,
                    full_trades,
                )
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

                row[
                    "family"
                ] = cfg[
                    "family"
                ]

                trade_output.append(
                    row
                )

        write_csv(
            OUT_EXACT,
            exact_rows,
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
                    "Final USD/CAD M15 LONG head-to-head only. No new broad optimisation.",
            }, {
                "item":
                    "EMA50 regime",

                "value":
                    "BR1.60/1.70/1.80/1.90, body1.25, range1.50, LB165, distance0.175ATR, prior completed daily EMA50>EMA200, RR5.00.",
            }, {
                "item":
                    "Daily-close regime",

                "value":
                    "BR1.60/1.70/1.80/1.90, body1.25, range1.30, LB165, distance0.175ATR, prior completed daily close>EMA200, RR4.75.",
            }, {
                "item":
                    "Decision",

                "value":
                    "Prefer the version with the best balance of full/pre/post edge, era balance, 2-pip cost survival, recent 2Y/5Y behaviour, rolling consistency, calendar consistency, drawdown and an interior/non-fragile BR threshold.",
            }, {
                "item":
                    "Historical holdout",

                "value":
                    "No pristine historical holdout remains because full history has already been used in development.",
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
                "USD/CAD M15 LONG final head-to-head complete",

            "hard_parity":
                "MATCH",

            "configs":
                len(
                    FINAL_CONFIGS
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
            "USDCAD M15 LONG Final Head-to-Head",

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

        "configs":
            8,

        "comparison": [
            "EMA50>EMA200 regime at BR1.60/1.70/1.80/1.90",
            "Daily close>EMA200 regime at BR1.60/1.70/1.80/1.90",
        ],

        "orders_supported":
            False,

        "trading_enabled":
            False,

        "routes": [
            "/usdcad-m15-long-final-head-to-head/status",
            "/usdcad-m15-long-final-head-to-head/results",
        ],
    })


@app.route(
    "/usdcad-m15-long-final-head-to-head/status"
)
def route_status():
    return jsonify(
        STATUS
    )


@app.route(
    "/usdcad-m15-long-final-head-to-head/results"
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
            "final-head-to-head"
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
