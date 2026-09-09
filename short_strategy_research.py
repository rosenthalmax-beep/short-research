
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
# USD/JPY M15 SHORT — COMPLEMENTARY FINAL DEEP VALIDATION
#
# PURPOSE
# -------
# Deep-validation only.
#
# Core GRID_0731 remains FROZEN and unchanged.
#
# Main complementary candidate:
#
#   CAND_0322
#   failed breakout / rejection
#   prior-high lookback = 165
#   body >= 1.00 ATR14
#   close location <= 0.20
#   previous COMPLETED H4 close < H4 EMA100
#   RR = 3.00
#
# Previously observed standalone headline:
#   62 trades
#   PF ~1.542
#   +21.67R
#   DD ~-8R
#   pre-2010 PF ~1.895
#   2010+ PF ~1.390
#   all four eras positive
#
# Previously observed overlay:
#   zero overlapping trades with GRID_0731
#   combined trades ~131
#   combined PF ~1.867
#   combined R ~+74.54R
#   combined no-trade years reduced 8 -> 3
#
# ============================================================
# EXACT DEEP SET
# ============================================================
#
# 1) PRIMARY:
#    LB165 / body1.00 / closeLoc.20 / H4<EMA100 / RR3.00
#
# 2) RR neighbours:
#    same geometry RR2.50
#    same geometry RR3.50
#
# 3) Body neighbours:
#    body0.90 / RR3.00
#    body1.10 / RR3.00
#
# 4) Close-location neighbours:
#    closeLoc.25 / RR3.00
#    closeLoc.30 / RR3.00
#
# 5) Structural neighbour:
#    LB200 / body1.00 / closeLoc.20 / H4<EMA100 / RR3.00
#
# No optimisation beyond these 8 exact configs.
#
# ============================================================
# DEEP OUTPUTS
# ============================================================
#
# For candidate-only AND core+non-overlap:
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
# Cost stress:
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
#   negative years
#
# Overlap:
#   candidate trades
#   accepted non-overlap
#   rejected overlap
#
# ============================================================
# CORE GRID_0731 — FROZEN
# ============================================================
#
# current candle bearish
# previous M15 ATR14 / previous 20-period mean ATR14 <= 0.80
# body >= 1.25 ATR14
# range >= 1.60 ATR14
# close < previous 40-bar low
# previous COMPLETED daily close < daily EMA200
# no time / weekday filter
# RR 4.75
# stop signal high + 10 ticks
# reference signal close
# historical short adverse fill close - 1 pip
# pyramiding 0
#
# ============================================================
# NO LOOKAHEAD
# ============================================================
#
# H4 / Daily state:
#   complete_at = next ACTUAL HTF candle OPEN
#   lookup = bisect_right(completion_times, signal_time) - 1
#
# Daily:
#   dailyAlignment=17
#   alignmentTimezone=America/New_York
#
# ============================================================
# HISTORICAL CONVENTIONS
# ============================================================
#
# OANDA midpoint.
# ATR14 Wilder/RMA, SMA seeded.
#
# USDJPY:
#   tick = 0.001
#   pip  = 0.01
#
# Reference entry = signal close.
# Historical short fill = signal close - adverse cost.
# Stop = signal high + 10 ticks.
# Target based on REFERENCE signal-close risk.
# Actual R based on adverse fill.
#
# Pyramiding 0.
# Exit checking starts next M15 candle.
# Exact exit-candle signal eligible.
#
# Same-bar SHORT:
#   if high is closer to candle open => STOP first
#   otherwise TARGET first.
#
# Overlay intervals:
#   [signal_index, exit_index)
#
# ============================================================
# PARITY
# ============================================================
#
# Original confirmation dataset ended at:
#   2026-09-09 14:15 UTC M15 open
#
# Required core parity:
#   69 full / 19 pre-2010 / 50 post-2010
#
# Required primary complementary parity:
#   62 full trades
#
# ============================================================
# ONE ZIP
# ============================================================
#
# /usdjpy-m15-short-complementary-final-deep/results
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

PARITY_LAST_M15_OPEN = datetime(
    2026, 9, 9, 14, 15,
    tzinfo=timezone.utc,
)

HTF_WARMUP_START = (
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


CORE = {
    "config_id": "CORE_GRID_0731",
    "compression_max": 0.80,
    "body_atr_min": 1.25,
    "range_atr_min": 1.60,
    "breakdown_lb": 40,
    "rr": 4.75,
}


FINAL_CONFIGS = [
    {
        "config_id": "PRIMARY_CAND_0322",
        "lookback": 165,
        "body_atr_min": 1.00,
        "close_loc_max": 0.20,
        "rr": 3.00,
    },
    {
        "config_id": "RR_2_50",
        "lookback": 165,
        "body_atr_min": 1.00,
        "close_loc_max": 0.20,
        "rr": 2.50,
    },
    {
        "config_id": "RR_3_50",
        "lookback": 165,
        "body_atr_min": 1.00,
        "close_loc_max": 0.20,
        "rr": 3.50,
    },
    {
        "config_id": "BODY_0_90",
        "lookback": 165,
        "body_atr_min": 0.90,
        "close_loc_max": 0.20,
        "rr": 3.00,
    },
    {
        "config_id": "BODY_1_10",
        "lookback": 165,
        "body_atr_min": 1.10,
        "close_loc_max": 0.20,
        "rr": 3.00,
    },
    {
        "config_id": "CLOSELOC_0_25",
        "lookback": 165,
        "body_atr_min": 1.00,
        "close_loc_max": 0.25,
        "rr": 3.00,
    },
    {
        "config_id": "CLOSELOC_0_30",
        "lookback": 165,
        "body_atr_min": 1.00,
        "close_loc_max": 0.30,
        "rr": 3.00,
    },
    {
        "config_id": "LOOKBACK_200",
        "lookback": 200,
        "body_atr_min": 1.00,
        "close_loc_max": 0.20,
        "rr": 3.00,
    },
]


# ============================================================
# OUTPUTS
# ============================================================

OUT_COVERAGE = (
    "usdjpy_m15_short_complementary_final_deep_coverage.csv"
)

OUT_PARITY = (
    "usdjpy_m15_short_complementary_final_deep_parity.csv"
)

OUT_EXACT = (
    "usdjpy_m15_short_complementary_final_deep_exact_comparison.csv"
)

OUT_PERIODS = (
    "usdjpy_m15_short_complementary_final_deep_periods.csv"
)

OUT_COST = (
    "usdjpy_m15_short_complementary_final_deep_cost_stress.csv"
)

OUT_ROLLING = (
    "usdjpy_m15_short_complementary_final_deep_rolling.csv"
)

OUT_ROLLING_SUMMARY = (
    "usdjpy_m15_short_complementary_final_deep_rolling_summary.csv"
)

OUT_CALENDAR = (
    "usdjpy_m15_short_complementary_final_deep_calendar_years.csv"
)

OUT_CALENDAR_SUMMARY = (
    "usdjpy_m15_short_complementary_final_deep_calendar_summary.csv"
)

OUT_OVERLAP = (
    "usdjpy_m15_short_complementary_final_deep_overlap.csv"
)

OUT_TRADES = (
    "usdjpy_m15_short_complementary_final_deep_trades.csv"
)

OUT_NOTES = (
    "usdjpy_m15_short_complementary_final_deep_notes.csv"
)

OUT_BUNDLE = (
    "USDJPY_M15_SHORT_COMPLEMENTARY_FINAL_DEEP_RESULTS.zip"
)

STATUS = {
    "state": "not_started",
    "message": "USD/JPY M15 SHORT complementary final deep validation not started",
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
        OUT_OVERLAP,
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
        "price": "M",
        "granularity": granularity,
        "smooth": "false",
        "from": iso_utc(start),
        "to": iso_utc(end),
        "includeFirst": "true",
    }

    if granularity == "D":
        params["dailyAlignment"] = 17
        params["alignmentTimezone"] = (
            "America/New_York"
        )

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
            "state": "fetching",
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

    seed = values[
        :length
    ]

    if np.isnan(seed).any():
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
        true_ranges(
            candles
        ),
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

        dq.append(i)

    return result


def rolling_previous_high(
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
            ] <= values[i]
        ):
            dq.pop()

        dq.append(i)

    return result


# ============================================================
# HTF STATE — NO LOOKAHEAD
# ============================================================

def build_htf_state(
    candles,
    ema_length,
):
    closes = [
        candle[
            "close"
        ]
        for candle in candles
    ]

    ema = ema_list(
        closes,
        ema_length,
    )

    rows = []

    for i, candle in enumerate(
        candles
    ):
        complete_at = (
            candles[
                i + 1
            ]["time"]
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
            "ema":
                ema[i],
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

    close_result = np.full(
        len(
            m15_times
        ),
        np.nan,
        dtype=float,
    )

    ema_result = np.full(
        len(
            m15_times
        ),
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

        row = eligible[
            position
        ]

        close_result[i] = (
            row[
                "close"
            ]
        )

        if row[
            "ema"
        ] is not None:
            ema_result[i] = (
                row[
                    "ema"
                ]
            )

    return {
        "close":
            close_result,
        "ema":
            ema_result,
    }


# ============================================================
# FEATURES
# ============================================================

def build_features(
    m15,
    h4_aligned,
    daily_aligned,
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

    bearish = (
        closes
        < opens
    )

    bearish_body = (
        opens
        - closes
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
        bearish_body[
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

    close_loc = np.full(
        n,
        np.nan,
        dtype=float,
    )

    valid_range = (
        candle_range
        > 0
    )

    close_loc[
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

    valid_comp = (
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
        valid_comp
    ] = (
        previous_atr[
            valid_comp
        ]
        / previous_atr_mean20[
            valid_comp
        ]
    )

    previous_low40 = (
        rolling_previous_low(
            lows,
            40,
        )
    )

    previous_high165 = (
        rolling_previous_high(
            highs,
            165,
        )
    )

    previous_high200 = (
        rolling_previous_high(
            highs,
            200,
        )
    )

    return {
        "bearish":
            bearish,
        "body_atr":
            body_atr,
        "range_atr":
            range_atr,
        "close_loc":
            close_loc,
        "compression":
            compression,
        "high":
            highs,
        "close":
            closes,
        "previous_low40":
            previous_low40,
        "previous_high165":
            previous_high165,
        "previous_high200":
            previous_high200,
        "h4_close":
            h4_aligned[
                "close"
            ],
        "h4_ema100":
            h4_aligned[
                "ema"
            ],
        "d_close":
            daily_aligned[
                "close"
            ],
        "d_ema200":
            daily_aligned[
                "ema"
            ],
    }


# ============================================================
# SIGNALS
# ============================================================

def core_signal_indices(
    f,
):
    mask = (
        f[
            "bearish"
        ].copy()
    )

    mask &= (
        f[
            "compression"
        ]
        <= CORE[
            "compression_max"
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
            "close"
        ]
        < f[
            "previous_low40"
        ]
    )

    mask &= (
        f[
            "d_close"
        ]
        < f[
            "d_ema200"
        ]
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
    previous_high = (
        f[
            "previous_high165"
        ]
        if cfg[
            "lookback"
        ] == 165
        else f[
            "previous_high200"
        ]
    )

    mask = (
        f[
            "bearish"
        ].copy()
    )

    mask &= (
        f[
            "high"
        ]
        > previous_high
    )

    mask &= (
        f[
            "close"
        ]
        < previous_high
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
            "close_loc"
        ]
        <= cfg[
            "close_loc_max"
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
        len(
            candles
        ),
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

        elif hit_target:
            exit_price = target
            reason = "TARGET"

        elif hit_stop:
            exit_price = stop
            reason = "STOP"

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
# OVERLAY
# ============================================================

def nonoverlap_overlay(
    core_trades,
    candidate_trades,
):
    core_sorted = sorted(
        core_trades,
        key=lambda t:
            t[
                "signal_index"
            ],
    )

    candidate_sorted = sorted(
        candidate_trades,
        key=lambda t:
            t[
                "signal_index"
            ],
    )

    accepted = []
    rejected = []

    pointer = 0

    for candidate in (
        candidate_sorted
    ):
        c_start = (
            candidate[
                "signal_index"
            ]
        )

        c_exit = (
            candidate[
                "exit_index"
            ]
        )

        while (
            pointer
            < len(
                core_sorted
            )
            and core_sorted[
                pointer
            ][
                "exit_index"
            ]
            <= c_start
        ):
            pointer += 1

        overlaps = False

        if (
            pointer
            < len(
                core_sorted
            )
        ):
            core = (
                core_sorted[
                    pointer
                ]
            )

            overlaps = (
                core[
                    "signal_index"
                ]
                < c_exit
                and c_start
                < core[
                    "exit_index"
                ]
            )

        if overlaps:
            rejected.append(
                candidate
            )
        else:
            accepted.append(
                candidate
            )

    combined = []

    for trade in (
        core_sorted
    ):
        item = dict(
            trade
        )
        item[
            "source"
        ] = "CORE"
        combined.append(
            item
        )

    for trade in (
        accepted
    ):
        item = dict(
            trade
        )
        item[
            "source"
        ] = "CANDIDATE"
        combined.append(
            item
        )

    combined.sort(
        key=lambda t:
            (
                t[
                    "signal_index"
                ],
                0
                if t[
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


def periods():
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
            START,
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


def result_row(
    config_id,
    mode,
    period,
    trades,
):
    s = stats_from_trades(
        trades
    )

    return {
        "config_id":
            config_id,
        "mode":
            mode,
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
    config_id,
    mode,
    trades,
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
                    config_id,
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


def calendar_rows_for_trades(
    config_id,
    mode,
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
                config_id,
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
            h4,
            daily,
        ]):
            raise RuntimeError(
                "Missing required USD_JPY history"
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
                "Building completed H4/D state and M15 feature cache",
        })

        m15_times = [
            candle[
                "time"
            ]
            for candle in m15
        ]

        h4_aligned = (
            align_htf_to_m15(
                m15_times,
                build_htf_state(
                    h4,
                    100,
                ),
            )
        )

        daily_aligned = (
            align_htf_to_m15(
                m15_times,
                build_htf_state(
                    daily,
                    200,
                ),
            )
        )

        features = build_features(
            m15,
            h4_aligned,
            daily_aligned,
        )

        core_indices = (
            core_signal_indices(
                features
            )
        )

        candidate_indices_by_id = {}

        for cfg in FINAL_CONFIGS:
            candidate_indices_by_id[
                cfg[
                    "config_id"
                ]
            ] = candidate_signal_indices(
                cfg,
                features,
            )

        # ----------------------------------------------------
        # PARITY
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

        parity_core_indices = [
            index
            for index in core_indices
            if index < parity_count
        ]

        parity_core = run_backtest(
            parity_candles,
            parity_core_indices,
            CORE[
                "rr"
            ],
            PRIMARY_COST_PIPS,
            START,
            parity_end,
        )

        parity_core_pre = (
            stats_from_trades(
                filter_trades(
                    parity_core,
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

        parity_core_post = (
            stats_from_trades(
                filter_trades(
                    parity_core,
                    datetime(
                        2010,
                        1,
                        1,
                        tzinfo=timezone.utc,
                    ),
                    parity_end,
                )
            )
        )

        primary = (
            FINAL_CONFIGS[
                0
            ]
        )

        primary_indices = (
            candidate_indices_by_id[
                primary[
                    "config_id"
                ]
            ]
        )

        parity_primary_indices = [
            index
            for index in primary_indices
            if index < parity_count
        ]

        parity_primary = run_backtest(
            parity_candles,
            parity_primary_indices,
            primary[
                "rr"
            ],
            PRIMARY_COST_PIPS,
            START,
            parity_end,
        )

        core_match = (
            len(
                parity_core
            ) == 69
            and parity_core_pre[
                "trades"
            ] == 19
            and parity_core_post[
                "trades"
            ] == 50
        )

        primary_match = (
            len(
                parity_primary
            ) == 62
        )

        write_csv(
            OUT_PARITY,
            [{
                "test":
                    "CORE_GRID_0731",
                "expected_full":
                    69,
                "actual_full":
                    len(
                        parity_core
                    ),
                "expected_pre2010":
                    19,
                "actual_pre2010":
                    parity_core_pre[
                        "trades"
                    ],
                "expected_post2010":
                    50,
                "actual_post2010":
                    parity_core_post[
                        "trades"
                    ],
                "status":
                    (
                        "MATCH"
                        if core_match
                        else "FAIL"
                    ),
            }, {
                "test":
                    "PRIMARY_CAND_0322",
                "expected_full":
                    62,
                "actual_full":
                    len(
                        parity_primary
                    ),
                "status":
                    (
                        "MATCH"
                        if primary_match
                        else "FAIL"
                    ),
            }],
        )

        if not (
            core_match
            and primary_match
        ):
            raise RuntimeError(
                "Parity failed. "
                "Do not trust complementary final-deep results."
            )

        # ----------------------------------------------------
        # CURRENT CORE FULL
        # ----------------------------------------------------
        core_trades = (
            run_backtest(
                m15,
                core_indices,
                CORE[
                    "rr"
                ],
                PRIMARY_COST_PIPS,
                START,
                NOW,
            )
        )

        # ----------------------------------------------------
        # EXACT COMPARISON + DEEP OUTPUTS
        # ----------------------------------------------------
        exact_rows = []
        period_output = []
        cost_output = []
        rolling_output = []
        calendar_output = []
        overlap_output = []
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

            candidate_indices = (
                candidate_indices_by_id[
                    cfg[
                        "config_id"
                    ]
                ]
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

            candidate_stats = (
                stats_from_trades(
                    candidate_trades
                )
            )

            combined_stats = (
                stats_from_trades(
                    combined
                )
            )

            exact_rows.append({
                "config_id":
                    cfg[
                        "config_id"
                    ],
                "lookback":
                    cfg[
                        "lookback"
                    ],
                "body_atr_min":
                    cfg[
                        "body_atr_min"
                    ],
                "close_loc_max":
                    cfg[
                        "close_loc_max"
                    ],
                "context":
                    "H4_CLOSE_LT_EMA100",
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
                "accepted_nonoverlap":
                    len(
                        accepted
                    ),
                "rejected_overlap":
                    len(
                        rejected
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
            })

            overlap_output.append({
                "config_id":
                    cfg[
                        "config_id"
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

            # Periods
            for (
                label,
                start,
                end,
            ) in periods():
                candidate_subset = (
                    filter_trades(
                        candidate_trades,
                        start,
                        end,
                    )
                )

                combined_subset = (
                    filter_trades(
                        combined,
                        start,
                        end,
                    )
                )

                core_subset = (
                    filter_trades(
                        core_trades,
                        start,
                        end,
                    )
                )

                period_output.append(
                    result_row(
                        cfg[
                            "config_id"
                        ],
                        "CORE_ONLY",
                        label,
                        core_subset,
                    )
                )

                period_output.append(
                    result_row(
                        cfg[
                            "config_id"
                        ],
                        "CANDIDATE_ONLY",
                        label,
                        candidate_subset,
                    )
                )

                period_output.append(
                    result_row(
                        cfg[
                            "config_id"
                        ],
                        "CORE_PLUS_NONOVERLAP",
                        label,
                        combined_subset,
                    )
                )

            # Cost stress
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

                    candidate_cost = (
                        run_backtest(
                            m15,
                            candidate_indices,
                            cfg[
                                "rr"
                            ],
                            cost,
                            start,
                            end,
                        )
                    )

                    (
                        combined_cost,
                        accepted_cost,
                        rejected_cost,
                    ) = nonoverlap_overlay(
                        core_cost,
                        candidate_cost,
                    )

                    for (
                        mode,
                        trades,
                    ) in [
                        (
                            "CANDIDATE_ONLY",
                            candidate_cost,
                        ),
                        (
                            "CORE_PLUS_NONOVERLAP",
                            combined_cost,
                        ),
                    ]:
                        s = stats_from_trades(
                            trades
                        )

                        cost_output.append({
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
                            "accepted_nonoverlap":
                                (
                                    len(
                                        accepted_cost
                                    )
                                    if mode
                                    == "CORE_PLUS_NONOVERLAP"
                                    else None
                                ),
                            "rejected_overlap":
                                (
                                    len(
                                        rejected_cost
                                    )
                                    if mode
                                    == "CORE_PLUS_NONOVERLAP"
                                    else None
                                ),
                        })

            # Rolling
            rolling_output.extend(
                rolling_rows_for_trades(
                    cfg[
                        "config_id"
                    ],
                    "CORE_ONLY",
                    core_trades,
                )
            )

            rolling_output.extend(
                rolling_rows_for_trades(
                    cfg[
                        "config_id"
                    ],
                    "CANDIDATE_ONLY",
                    candidate_trades,
                )
            )

            rolling_output.extend(
                rolling_rows_for_trades(
                    cfg[
                        "config_id"
                    ],
                    "CORE_PLUS_NONOVERLAP",
                    combined,
                )
            )

            # Calendar
            calendar_output.extend(
                calendar_rows_for_trades(
                    cfg[
                        "config_id"
                    ],
                    "CORE_ONLY",
                    core_trades,
                )
            )

            calendar_output.extend(
                calendar_rows_for_trades(
                    cfg[
                        "config_id"
                    ],
                    "CANDIDATE_ONLY",
                    candidate_trades,
                )
            )

            calendar_output.extend(
                calendar_rows_for_trades(
                    cfg[
                        "config_id"
                    ],
                    "CORE_PLUS_NONOVERLAP",
                    combined,
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
                    "Purpose",
                "value":
                    "Deep-validation only. No new broad optimisation.",
            }, {
                "item":
                    "Core",
                "value":
                    "GRID_0731 remains frozen and unchanged.",
            }, {
                "item":
                    "Primary complement",
                "value":
                    "CAND_0322 = failed breakout of prior165 high, body>=1.00 ATR, closeLoc<=0.20, previous completed H4 close<EMA100, RR3.00.",
            }, {
                "item":
                    "Decision rule",
                "value":
                    "Only add the complementary trigger if candidate survives 2-pip costs and core+overlay materially improves rolling/calendar consistency versus the frozen core.",
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
                "USD/JPY M15 SHORT complementary final deep validation complete",
            "parity":
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
            "USDJPY M15 SHORT Complementary Final Deep Validation",
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
        "core":
            "GRID_0731 frozen",
        "primary_complement":
            "CAND_0322",
        "deep_configs":
            len(
                FINAL_CONFIGS
            ),
        "orders_supported":
            False,
        "trading_enabled":
            False,
        "routes": [
            "/usdjpy-m15-short-complementary-final-deep/status",
            "/usdjpy-m15-short-complementary-final-deep/results",
        ],
    })


@app.route(
    "/usdjpy-m15-short-complementary-final-deep/status"
)
def route_status():
    return jsonify(
        STATUS
    )


@app.route(
    "/usdjpy-m15-short-complementary-final-deep/results"
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
            "usdjpy-m15-short-"
            "complementary-final-deep"
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
