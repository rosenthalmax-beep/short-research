
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
# EUR/GBP M15 SHORT — FINAL HIGH-SWEEP CONFIRMATION
#
# PURPOSE
# -------
# Fresh full-history M15 SHORT research from earliest reliable
# OANDA M15 history (~May 2002) to present.
# This is the tenth/final M15 pair-direction research side.
#
# This is NOT a mechanical inversion of the locked long strategy.
# It searches distinct bearish hypotheses and only adds context
# after an archetype shows some standalone full-span merit.
#
# ============================================================
# RESEARCH PROCESS
# ============================================================
#
# STAGE 1 — BROAD ARCHETYPES
#
#   1) BEAR_ENGULF_STRUCTURE
#   2) HIGH_SWEEP_REJECTION
#   3) FAILED_BREAKOUT_RECLAIM
#   4) BEAR_OUTSIDE_REVERSAL
#   5) COMPRESSION_BREAKDOWN
#   6) RALLY_FAILURE_BREAKDOWN
#
# Stage 1 uses no session/weekday/HTF trend mining.
# Fixed RR = 3.50.
#
# ------------------------------------------------------------
# STAGE 2 — CONTROLLED CONTEXTS
#
# Apply plausible broad contexts to the strongest Stage-1
# geometries only:
#
#   NONE
#   H1 close < EMA100
#   H1 close < EMA200
#   H1 EMA50 < EMA200
#   H4 close < EMA100
#   H4 close < EMA200
#   Daily close < EMA200
#   Daily EMA50 < EMA200
#   H1 ATR14 / 50-mean >= 0.80
#   H4 ATR14 / 50-mean >= 0.80
#   Daily ATR14 / 50-mean >= 0.80
#
# Plus broad London and NY 4-hour blocks and single-weekday
# exclusions as diagnostics only, not preferred structural filters.
#
# ------------------------------------------------------------
# STAGE 3 — LOCAL GEOMETRY + RR
#
# Local neighbours only around strongest Stage-2 candidates.
# RR:
#   2.50 / 3.00 / 3.50 / 4.00 / 4.50 / 5.00
#
# ------------------------------------------------------------
# DEEP FINALISTS
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
#
# Rolling:
#   12 / 24 / 36 months
#
# Calendar:
#   completed years
#
# Parameter neighbourhood:
#   explicit local plateau output
#
# ============================================================
# M15 HISTORICAL CONVENTIONS
# ============================================================
#
# OANDA midpoint.
# ATR14 = Wilder/RMA, SMA seeded.
#
# EUR/GBP:
#   tick = 0.00001
#   pip  = 0.0001
#
# Reference entry:
#   signal close
#
# Historical SHORT fill:
#   signal close - adverse cost
#
# Baseline adverse cost:
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
#   starts next M15 candle
#
# Exact exit-candle signal:
#   eligible
#
# Same-bar SHORT tie:
#   if candle high is closer to candle open => STOP first
#   otherwise TARGET first
#
# Signal timestamp:
#   M15 candle OPEN
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
#   dailyAlignment = 17
#   alignmentTimezone = America/New_York
#
# Prior momentum features end at M15 close[i-1].
#
# ============================================================
# HISTORICAL INTERPRETATION
# ============================================================
#
# Full 2002+ history has already been used across this M15
# research programme, so no pristine historical holdout remains.
#
# Treat results as robust full-history / temporal validation,
# not untouched OOS.
#
# ============================================================
# ONE ZIP
# ============================================================
#
# /eurgbp-m15-short-final-high-sweep/results
#
# READ ONLY. NEVER SENDS ORDERS.
# ============================================================


app = Flask(__name__)

TOKEN = os.getenv("OANDA_TOKEN")
BASE = os.getenv(
    "OANDA_API_URL",
    "https://api-fxtrade.oanda.com",
)

PAIR = "EUR_GBP"

START = datetime(
    2002, 5, 6, 20, 0,
    tzinfo=timezone.utc,
)

NOW = (
    datetime.now(timezone.utc)
    .replace(second=0, microsecond=0)
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

STAGE1_KEEP = 18
STAGE2_BASE_KEEP = 9
STAGE2_KEEP = 14
STAGE3_BASE_KEEP = 7
FINALIST_KEEP = 10

RR_VALUES = [
    2.50,
    3.00,
    3.50,
    4.00,
    4.50,
    5.00,
]

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
    140,
    165,
    200,
]


# ============================================================
# OUTPUTS
# ============================================================

OUT_COVERAGE = (
    "eurgbp_m15_short_final_high_sweep_coverage.csv"
)

OUT_STAGE1 = (
    "eurgbp_m15_short_final_high_sweep_stage1_geometry.csv"
)

OUT_STAGE2 = (
    "eurgbp_m15_short_final_high_sweep_stage2_wick.csv"
)

OUT_STAGE3 = (
    "eurgbp_m15_short_final_high_sweep_stage3_weekday.csv"
)

OUT_FINALISTS = (
    "eurgbp_m15_short_final_high_sweep_finalists.csv"
)

OUT_PERIODS = (
    "eurgbp_m15_short_final_high_sweep_periods.csv"
)

OUT_COST = (
    "eurgbp_m15_short_final_high_sweep_cost_stress.csv"
)

OUT_ROLLING = (
    "eurgbp_m15_short_final_high_sweep_rolling.csv"
)

OUT_ROLLING_SUMMARY = (
    "eurgbp_m15_short_final_high_sweep_rolling_summary.csv"
)

OUT_CALENDAR = (
    "eurgbp_m15_short_final_high_sweep_calendar_years.csv"
)

OUT_CALENDAR_SUMMARY = (
    "eurgbp_m15_short_final_high_sweep_calendar_summary.csv"
)

OUT_PLATEAU = (
    "eurgbp_m15_short_final_high_sweep_parameter_summary.csv"
)

OUT_TRADES = (
    "eurgbp_m15_short_final_high_sweep_finalist_trades.csv"
)

OUT_NOTES = (
    "eurgbp_m15_short_final_high_sweep_notes.csv"
)

OUT_BUNDLE = (
    "EURGBP_M15_SHORT_FINAL_HIGH_SWEEP_CONFIRMATION_RESULTS.zip"
)

STATUS = {
    "state":
        "not_started",

    "message":
        "EUR/GBP M15 SHORT full-history re-examination not started",

    "orders_supported":
        False,

    "trading_enabled":
        False,
}


# ============================================================
# BASIC HELPERS
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
    paths = [
        OUT_COVERAGE,
        OUT_STAGE1,
        OUT_STAGE2,
        OUT_STAGE3,
        OUT_FINALISTS,
        OUT_PERIODS,
        OUT_COST,
        OUT_ROLLING,
        OUT_ROLLING_SUMMARY,
        OUT_CALENDAR,
        OUT_CALENDAR_SUMMARY,
        OUT_PLATEAU,
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


def safe_median(values):
    values = list(values)

    return (
        median(values)
        if values
        else 0.0
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
                candle["high"]
                - candle["low"]
            )
        else:
            prev_close = (
                candles[
                    i - 1
                ][
                    "close"
                ]
            )

            result[i] = max(
                candle["high"]
                - candle["low"],

                abs(
                    candle["high"]
                    - prev_close
                ),

                abs(
                    candle["low"]
                    - prev_close
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
            values[:length]
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

        if mode == "min":
            while (
                dq
                and values[
                    dq[-1]
                ] >= values[i]
            ):
                dq.pop()
        else:
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
# HTF STATE — CORRECT COMPLETION
# ============================================================

def build_htf_state(candles):
    closes = [
        candle["close"]
        for candle in candles
    ]

    atr = atr14(candles)
    atr_mean50 = sma_np(
        atr,
        50,
    )

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

        atr_ratio50 = None

        if (
            np.isfinite(atr[i])
            and np.isfinite(
                atr_mean50[i]
            )
            and atr_mean50[i] > 0
        ):
            atr_ratio50 = (
                atr[i]
                / atr_mean50[i]
            )

        rows.append({
            "complete_at":
                complete_at,

            "close":
                candle["close"],

            "ema50":
                ema50[i],

            "ema100":
                ema100[i],

            "ema200":
                ema200[i],

            "atr_ratio50":
                atr_ratio50,
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
        "atr_ratio50",
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
# M15 FEATURES
# ============================================================

def build_features(
    m15,
    h1,
    h4,
    daily,
):
    n = len(m15)

    opens = np.array(
        [c["open"] for c in m15],
        dtype=float,
    )

    highs = np.array(
        [c["high"] for c in m15],
        dtype=float,
    )

    lows = np.array(
        [c["low"] for c in m15],
        dtype=float,
    )

    closes = np.array(
        [c["close"] for c in m15],
        dtype=float,
    )

    atr = atr14(m15)
    atr_mean20 = sma_np(
        atr,
        20,
    )

    valid_atr = (
        np.isfinite(atr)
        & (
            atr > 0
        )
    )

    bearish = (
        closes
        < opens
    )

    current_body = (
        opens
        - closes
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
        current_body
        > 0
    )

    upper_wick_body[
        valid_body
    ] = (
        upper_wick[
            valid_body
        ]
        / current_body[
            valid_body
        ]
    )

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
        previous_body
        > 0
    )

    body_ratio[
        valid_prev_body
    ] = (
        current_body[
            valid_prev_body
        ]
        / previous_body[
            valid_prev_body
        ]
    )

    # Project doji convention.
    body_ratio[
        previous_body
        == 0
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

    # Strict prior momentum: ends at close[i-1].
    rally_12h = np.full(
        n,
        np.nan,
        dtype=float,
    )

    rally_24h = np.full(
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

    for i in range(
        97,
        n,
    ):
        if (
            valid_atr[i]
            and atr[i] > 0
        ):
            rally_24h[i] = (
                closes[
                    i - 1
                ]
                - closes[
                    i - 97
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

        "previous_high":
            previous_high,

        "previous_low":
            previous_low,

        "rally_12h":
            rally_12h,

        "rally_24h":
            rally_24h,

        "h1":
            h1,

        "h4":
            h4,

        "d":
            daily,
    }


# ============================================================
# STAGE-1 ARCHETYPES
# ============================================================

def build_stage1_configs():
    configs = []
    counter = 0

    # --------------------------------------------------------
    # 1) BEAR_ENGULF_STRUCTURE
    # --------------------------------------------------------
    for br in [
        1.00,
        1.20,
        1.40,
    ]:
        for body in [
            0.75,
            1.00,
            1.25,
        ]:
            for lookback in [
                60,
                100,
                165,
            ]:
                for distance in [
                    0.10,
                    0.20,
                    0.30,
                ]:
                    counter += 1

                    configs.append({
                        "config_id":
                            f"S1_ENG_{counter:04d}",

                        "family":
                            "BEAR_ENGULF_STRUCTURE",

                        "br_min":
                            br,

                        "body_atr_min":
                            body,

                        "range_atr_min":
                            None,

                        "close_loc_max":
                            None,

                        "upper_wick_body_min":
                            None,

                        "structure_lb":
                            lookback,

                        "structure_dist_atr_max":
                            distance,

                        "sweep_lb":
                            None,

                        "breakout_lb":
                            None,

                        "compression_max":
                            None,

                        "rally_12h_min":
                            None,

                        "rr":
                            STAGE1_RR,
                    })

    # --------------------------------------------------------
    # 2) HIGH_SWEEP_REJECTION
    # --------------------------------------------------------
    for sweep_lb in [
        20,
        40,
        60,
        100,
    ]:
        for body in [
            0.75,
            1.00,
            1.25,
        ]:
            for close_loc in [
                0.20,
                0.30,
                0.40,
            ]:
                for wick in [
                    0.10,
                    0.25,
                ]:
                    counter += 1

                    configs.append({
                        "config_id":
                            f"S1_SWEEP_{counter:04d}",

                        "family":
                            "HIGH_SWEEP_REJECTION",

                        "br_min":
                            None,

                        "body_atr_min":
                            body,

                        "range_atr_min":
                            None,

                        "close_loc_max":
                            close_loc,

                        "upper_wick_body_min":
                            wick,

                        "structure_lb":
                            None,

                        "structure_dist_atr_max":
                            None,

                        "sweep_lb":
                            sweep_lb,

                        "breakout_lb":
                            None,

                        "compression_max":
                            None,

                        "rally_12h_min":
                            None,

                        "rr":
                            STAGE1_RR,
                    })

    # --------------------------------------------------------
    # 3) FAILED_BREAKOUT_RECLAIM
    # --------------------------------------------------------
    for lookback in [
        60,
        100,
        165,
        200,
    ]:
        for body in [
            0.75,
            1.00,
            1.25,
        ]:
            for close_loc in [
                0.20,
                0.30,
                0.40,
            ]:
                counter += 1

                configs.append({
                    "config_id":
                        f"S1_FAIL_{counter:04d}",

                    "family":
                        "FAILED_BREAKOUT_RECLAIM",

                    "br_min":
                        None,

                    "body_atr_min":
                        body,

                    "range_atr_min":
                        None,

                    "close_loc_max":
                        close_loc,

                    "upper_wick_body_min":
                        None,

                    "structure_lb":
                        lookback,

                    "structure_dist_atr_max":
                        None,

                    "sweep_lb":
                        None,

                    "breakout_lb":
                        None,

                    "compression_max":
                        None,

                    "rally_12h_min":
                        None,

                    "rr":
                        STAGE1_RR,
                })

    # --------------------------------------------------------
    # 4) BEAR_OUTSIDE_REVERSAL
    # --------------------------------------------------------
    for body in [
        0.75,
        1.00,
        1.25,
    ]:
        for close_loc in [
            0.20,
            0.30,
            0.40,
        ]:
            for lookback in [
                60,
                100,
                165,
            ]:
                for distance in [
                    0.10,
                    0.20,
                    0.30,
                ]:
                    counter += 1

                    configs.append({
                        "config_id":
                            f"S1_OUT_{counter:04d}",

                        "family":
                            "BEAR_OUTSIDE_REVERSAL",

                        "br_min":
                            None,

                        "body_atr_min":
                            body,

                        "range_atr_min":
                            None,

                        "close_loc_max":
                            close_loc,

                        "upper_wick_body_min":
                            None,

                        "structure_lb":
                            lookback,

                        "structure_dist_atr_max":
                            distance,

                        "sweep_lb":
                            None,

                        "breakout_lb":
                            None,

                        "compression_max":
                            None,

                        "rally_12h_min":
                            None,

                        "rr":
                            STAGE1_RR,
                    })

    # --------------------------------------------------------
    # 5) COMPRESSION_BREAKDOWN
    # --------------------------------------------------------
    for compression in [
        0.65,
        0.75,
        0.85,
    ]:
        for body in [
            0.90,
            1.10,
            1.30,
        ]:
            for range_atr in [
                1.20,
                1.40,
                1.60,
            ]:
                for breakout_lb in [
                    5,
                    10,
                    20,
                ]:
                    counter += 1

                configs.append({
                    "config_id":
                        f"S1_COMP_{counter:04d}",

                    "family":
                        "COMPRESSION_BREAKDOWN",

                    "br_min":
                        None,

                    "body_atr_min":
                        body,

                    "range_atr_min":
                        range_atr,

                    "close_loc_max":
                        None,

                    "upper_wick_body_min":
                        None,

                    "structure_lb":
                        None,

                    "structure_dist_atr_max":
                        None,

                    "sweep_lb":
                        None,

                    "breakout_lb":
                        breakout_lb,

                    "compression_max":
                        compression,

                    "rally_12h_min":
                        None,

                    "rr":
                        STAGE1_RR,
                })

    # --------------------------------------------------------
    # 6) RALLY_FAILURE_BREAKDOWN
    # --------------------------------------------------------
    for rally in [
        0.50,
        1.00,
        1.50,
        2.00,
    ]:
        for body in [
            0.90,
            1.10,
            1.30,
        ]:
            for breakout_lb in [
                5,
                10,
                20,
            ]:
                for close_loc in [
                    0.25,
                    0.35,
                ]:
                    counter += 1

                    configs.append({
                        "config_id":
                            f"S1_RALLY_{counter:04d}",

                        "family":
                            "RALLY_FAILURE_BREAKDOWN",

                        "br_min":
                            None,

                        "body_atr_min":
                            body,

                        "range_atr_min":
                            None,

                        "close_loc_max":
                            close_loc,

                        "upper_wick_body_min":
                            None,

                        "structure_lb":
                            None,

                        "structure_dist_atr_max":
                            None,

                        "sweep_lb":
                            None,

                        "breakout_lb":
                            breakout_lb,

                        "compression_max":
                            None,

                        "rally_12h_min":
                            rally,

                        "rr":
                            STAGE1_RR,
                    })

    return configs


# ============================================================
# SIGNAL LOGIC
# ============================================================

def base_signal_mask(
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
        == "FAILED_BREAKOUT_RECLAIM"
    ):
        prior_high = (
            f[
                "prev_high"
            ][
                cfg[
                    "structure_lb"
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

    elif (
        family
        == "BEAR_OUTSIDE_REVERSAL"
    ):
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

    mask[:220] = False

    return mask


def apply_context(
    mask,
    context,
    f,
):
    mode = context[
        "type"
    ]

    if mode == "NONE":
        return mask

    if mode == "H1_CLOSE_LT_EMA100":
        mask &= (
            f["h1"]["close"]
            < f["h1"]["ema100"]
        )

    elif mode == "H1_CLOSE_LT_EMA200":
        mask &= (
            f["h1"]["close"]
            < f["h1"]["ema200"]
        )

    elif mode == "H1_EMA50_LT_EMA200":
        mask &= (
            f["h1"]["ema50"]
            < f["h1"]["ema200"]
        )

    elif mode == "H4_CLOSE_LT_EMA100":
        mask &= (
            f["h4"]["close"]
            < f["h4"]["ema100"]
        )

    elif mode == "H4_CLOSE_LT_EMA200":
        mask &= (
            f["h4"]["close"]
            < f["h4"]["ema200"]
        )

    elif mode == "D_CLOSE_LT_EMA200":
        mask &= (
            f["d"]["close"]
            < f["d"]["ema200"]
        )

    elif mode == "D_EMA50_LT_EMA200":
        mask &= (
            f["d"]["ema50"]
            < f["d"]["ema200"]
        )

    elif mode == "H1_ATR_RATIO_GE_080":
        mask &= (
            f["h1"]["atr_ratio50"]
            >= 0.80
        )

    elif mode == "H4_ATR_RATIO_GE_080":
        mask &= (
            f["h4"]["atr_ratio50"]
            >= 0.80
        )

    elif mode == "D_ATR_RATIO_GE_080":
        mask &= (
            f["d"]["atr_ratio50"]
            >= 0.80
        )

    elif mode == "NY_BLOCK":
        start_hour = context[
            "start_hour"
        ]

        end_hour = context[
            "end_hour"
        ]

        allowed = np.zeros(
            len(mask),
            dtype=bool,
        )

        # Python standard library timezone conversion keeps DST.
        from zoneinfo import ZoneInfo
        ny = ZoneInfo(
            "America/New_York"
        )

        for i, candle_open in enumerate(
            f[
                "times"
            ]
        ):
            hour = (
                candle_open
                .astimezone(ny)
                .hour
            )

            allowed[i] = (
                start_hour
                <= hour
                < end_hour
            )

        mask &= allowed

    elif mode == "LONDON_BLOCK":
        start_hour = context[
            "start_hour"
        ]

        end_hour = context[
            "end_hour"
        ]

        allowed = np.zeros(
            len(mask),
            dtype=bool,
        )

        from zoneinfo import ZoneInfo
        london = ZoneInfo(
            "Europe/London"
        )

        for i, candle_open in enumerate(
            f[
                "times"
            ]
        ):
            hour = (
                candle_open
                .astimezone(london)
                .hour
            )

            allowed[i] = (
                start_hour
                <= hour
                < end_hour
            )

        mask &= allowed

    elif mode == "EXCLUDE_WEEKDAY":
        excluded = context[
            "weekday"
        ]

        allowed = np.ones(
            len(mask),
            dtype=bool,
        )

        from zoneinfo import ZoneInfo
        london = ZoneInfo(
            "Europe/London"
        )

        for i, candle_open in enumerate(
            f[
                "times"
            ]
        ):
            allowed[i] = (
                candle_open
                .astimezone(london)
                .weekday()
                != excluded
            )

        mask &= allowed

    else:
        raise RuntimeError(
            f"Unknown context: {mode}"
        )

    return mask


def signal_indices(
    cfg,
    f,
):
    mask = base_signal_mask(
        cfg,
        f,
    )

    context = cfg.get(
        "context",
        {
            "type":
                "NONE",
        },
    )

    mask = apply_context(
        mask,
        context,
        f,
    )

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

            # SHORT convention:
            # high closer => stop first.
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
        signal_index = (
            use[position]
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
                100.0
                * len(winners)
                / len(values)
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
                2010, 1, 1,
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

    score = (
        1.30
        * min(
            full[
                "profit_factor"
            ],
            3.0,
        )
        + 0.80
        * min(
            pre[
                "profit_factor"
            ],
            3.0,
        )
        + 0.80
        * min(
            post[
                "profit_factor"
            ],
            3.0,
        )
        + 0.50
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
            / 100.0,
            1.5,
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
            cfg.get(
                "context",
                {
                    "type":
                        "NONE",
                },
            )[
                "type"
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

    for field in [
        "br_min",
        "body_atr_min",
        "range_atr_min",
        "close_loc_max",
        "upper_wick_body_min",
        "structure_lb",
        "structure_dist_atr_max",
        "sweep_lb",
        "breakout_lb",
        "compression_max",
        "rally_12h_min",
    ]:
        row[
            field
        ] = cfg.get(
            field
        )

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


def sort_rows(rows):
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
# STAGE 2 CONTEXTS
# ============================================================

def build_contexts():
    contexts = [
        {
            "type":
                "NONE",
        },

        {
            "type":
                "H1_CLOSE_LT_EMA100",
        },

        {
            "type":
                "H1_CLOSE_LT_EMA200",
        },

        {
            "type":
                "H1_EMA50_LT_EMA200",
        },

        {
            "type":
                "H4_CLOSE_LT_EMA100",
        },

        {
            "type":
                "H4_CLOSE_LT_EMA200",
        },

        {
            "type":
                "D_CLOSE_LT_EMA200",
        },

        {
            "type":
                "D_EMA50_LT_EMA200",
        },

        {
            "type":
                "H1_ATR_RATIO_GE_080",
        },

        {
            "type":
                "H4_ATR_RATIO_GE_080",
        },

        {
            "type":
                "D_ATR_RATIO_GE_080",
        },
    ]

    for start_hour in [
        0,
        4,
        8,
        12,
        16,
        20,
    ]:
        contexts.append({
            "type":
                "NY_BLOCK",

            "start_hour":
                start_hour,

            "end_hour":
                start_hour + 4,
        })

    for start_hour in [
        0,
        4,
        8,
        12,
        16,
        20,
    ]:
        contexts.append({
            "type":
                "LONDON_BLOCK",

            "start_hour":
                start_hour,

            "end_hour":
                start_hour + 4,
        })

    for weekday in range(5):
        contexts.append({
            "type":
                "EXCLUDE_WEEKDAY",

            "weekday":
                weekday,
        })

    return contexts


def build_stage2_configs(
    stage1_by_id,
    top_stage1,
):
    configs = []
    counter = 0

    contexts = build_contexts()

    for row in top_stage1[
        :STAGE2_BASE_KEEP
    ]:
        base = deepcopy(
            stage1_by_id[
                row[
                    "config_id"
                ]
            ]
        )

        for context in contexts:
            counter += 1

            cfg = deepcopy(
                base
            )

            cfg[
                "config_id"
            ] = (
                f"S2_{counter:04d}"
            )

            cfg[
                "context"
            ] = deepcopy(
                context
            )

            configs.append(
                cfg
            )

    return configs


# ============================================================
# STAGE 3 LOCAL NEIGHBOURS
# ============================================================

def nearby(
    value,
    options,
):
    if value is None:
        return [
            None
        ]

    ordered = sorted(
        options
    )

    if value not in ordered:
        ordered.append(
            value
        )

        ordered = sorted(
            ordered
        )

    index = ordered.index(
        value
    )

    lo = max(
        0,
        index - 1,
    )

    hi = min(
        len(ordered),
        index + 2,
    )

    return ordered[
        lo:hi
    ]


def local_variants(base):
    family = base[
        "family"
    ]

    variants = []

    def add(
        **updates
    ):
        cfg = deepcopy(
            base
        )

        cfg.update(
            updates
        )

        variants.append(
            cfg
        )

    # Always include exact base at all RRs.
    for rr in RR_VALUES:
        add(
            rr=rr
        )

    if family == "BEAR_ENGULF_STRUCTURE":
        for br in nearby(
            base[
                "br_min"
            ],
            [
                0.90,
                1.00,
                1.10,
                1.20,
                1.30,
                1.40,
                1.50,
            ],
        ):
            for body in nearby(
                base[
                    "body_atr_min"
                ],
                [
                    0.65,
                    0.75,
                    0.90,
                    1.00,
                    1.10,
                    1.25,
                    1.40,
                ],
            ):
                for lb in nearby(
                    base[
                        "structure_lb"
                    ],
                    [
                        40,
                        60,
                        80,
                        100,
                        120,
                        165,
                        200,
                    ],
                ):
                    for dist in nearby(
                        base[
                            "structure_dist_atr_max"
                        ],
                        [
                            0.05,
                            0.10,
                            0.15,
                            0.20,
                            0.25,
                            0.30,
                            0.35,
                        ],
                    ):
                        for rr in RR_VALUES:
                            add(
                                br_min=br,
                                body_atr_min=body,
                                structure_lb=lb,
                                structure_dist_atr_max=dist,
                                rr=rr,
                            )

    elif family in (
        "HIGH_SWEEP_REJECTION",
        "FAILED_BREAKOUT_RECLAIM",
        "BEAR_OUTSIDE_REVERSAL",
    ):
        for body in nearby(
            base[
                "body_atr_min"
            ],
            [
                0.65,
                0.75,
                0.90,
                1.00,
                1.10,
                1.25,
                1.40,
            ],
        ):
            for close_loc in nearby(
                base[
                    "close_loc_max"
                ],
                [
                    0.15,
                    0.20,
                    0.25,
                    0.30,
                    0.35,
                    0.40,
                    0.45,
                ],
            ):
                for rr in RR_VALUES:
                    add(
                        body_atr_min=body,
                        close_loc_max=close_loc,
                        rr=rr,
                    )

    elif family == "COMPRESSION_BREAKDOWN":
        for compression in nearby(
            base[
                "compression_max"
            ],
            [
                0.60,
                0.65,
                0.70,
                0.75,
                0.80,
                0.85,
                0.90,
            ],
        ):
            for body in nearby(
                base[
                    "body_atr_min"
                ],
                [
                    0.80,
                    0.90,
                    1.00,
                    1.10,
                    1.20,
                    1.30,
                    1.40,
                ],
            ):
                for rng in nearby(
                    base[
                        "range_atr_min"
                    ],
                    [
                        1.10,
                        1.20,
                        1.30,
                        1.40,
                        1.50,
                        1.60,
                        1.70,
                    ],
                ):
                    for lb in nearby(
                        base[
                            "breakout_lb"
                        ],
                        [
                            5,
                            10,
                            15,
                            20,
                            30,
                        ],
                    ):
                        for rr in RR_VALUES:
                            add(
                                compression_max=(
                                    compression
                                ),
                                body_atr_min=body,
                                range_atr_min=rng,
                                breakout_lb=lb,
                                rr=rr,
                            )

    elif family == "RALLY_FAILURE_BREAKDOWN":
        for rally in nearby(
            base[
                "rally_12h_min"
            ],
            [
                0.25,
                0.50,
                0.75,
                1.00,
                1.25,
                1.50,
                2.00,
                2.50,
            ],
        ):
            for body in nearby(
                base[
                    "body_atr_min"
                ],
                [
                    0.80,
                    0.90,
                    1.00,
                    1.10,
                    1.20,
                    1.30,
                    1.40,
                ],
            ):
                for lb in nearby(
                    base[
                        "breakout_lb"
                    ],
                    [
                        5,
                        10,
                        15,
                        20,
                        30,
                    ],
                ):
                    for rr in RR_VALUES:
                        add(
                            rally_12h_min=rally,
                            body_atr_min=body,
                            breakout_lb=lb,
                            rr=rr,
                        )

    # Deduplicate signatures.
    output = []
    seen = set()

    for cfg in variants:
        signature = tuple(
            (
                key,
                repr(
                    cfg.get(key)
                ),
            )
            for key in sorted(
                cfg.keys()
            )
            if key != "config_id"
        )

        if signature in seen:
            continue

        seen.add(
            signature
        )

        output.append(
            cfg
        )

    return output


# ============================================================
# DEEP OUTPUT HELPERS
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

        "family":
            cfg[
                "family"
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

            s = stats_from_trades(
                trades
            )

            rows.append({
                "config_id":
                    cfg[
                        "config_id"
                    ],

                "months":
                    months,

                "start_utc":
                    iso_utc(start),

                "end_utc":
                    iso_utc(end),

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
                    "months"
                ],
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

            "months":
                months,

            "windows":
                len(subset),

            "active_windows":
                len(active),

            "zero_trade_windows":
                len(subset)
                - len(active),

            "positive_active_windows_pct":
                round(
                    (
                        100.0
                        * len(
                            positive_active
                        )
                        / len(active)
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
            year, 1, 1,
            tzinfo=timezone.utc,
        )

        end = datetime(
            year + 1, 1, 1,
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


def calendar_summary_rows(rows):
    grouped = defaultdict(
        list
    )

    for row in rows:
        grouped[
            row[
                "config_id"
            ]
        ].append(row)

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

        losing_years = [
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

            "active_years":
                len(active),

            "inactive_years":
                len(zero_years),

            "inactive_year_list":
                ",".join(
                    str(year)
                    for year in zero_years
                ),

            "positive_active_years_pct":
                round(
                    (
                        100.0
                        * len(
                            positive_active
                        )
                        / len(active)
                    )
                    if active
                    else 0.0,
                    4,
                ),

            "losing_years":
                len(losing_years),

            "losing_year_list":
                ",".join(
                    str(year)
                    for year in losing_years
                ),
        })

    return output


# ============================================================
# PLATEAU
# ============================================================

def plateau_rows(
    finalist_cfg,
    candles,
    f,
):
    rows = []

    variants = local_variants(
        finalist_cfg
    )

    for i, cfg in enumerate(
        variants,
        1,
    ):
        cfg = deepcopy(cfg)

        cfg[
            "config_id"
        ] = (
            f"{finalist_cfg['config_id']}"
            f"_PLATEAU_{i:04d}"
        )

        indices = signal_indices(
            cfg,
            f,
        )

        row = evaluation_row(
            cfg,
            candles,
            indices,
        )

        row[
            "parent_finalist"
        ] = finalist_cfg[
            "config_id"
        ]

        rows.append(row)

    return rows


# ============================================================
# MAIN
# ============================================================


# ============================================================
# FINAL HIGH-SWEEP CONFIRMATION OVERRIDES
# ============================================================
import traceback
from zoneinfo import ZoneInfo

OUT_PARITY = "eurgbp_m15_short_final_high_sweep_parity.csv"
OUT_STAGE4 = "eurgbp_m15_short_final_high_sweep_stage4_rr.csv"

PARITY_CUTOFF = datetime(2026, 6, 16, 0, 0, tzinfo=timezone.utc)
ANCHOR_EXPECTED_TRADES = 59

SWEEP_GRID = [60, 80, 100, 120, 140, 165, 200]
BODY_GRID = [1.10, 1.20, 1.25, 1.30, 1.40, 1.50, 1.60]
CLOSE_GRID = [0.20, 0.25, 0.30, 0.35]
WICK_GRID = [0.15, 0.20, 0.25, 0.30, 0.35]
RR_GRID_FINAL = [3.75, 4.00, 4.25, 4.50, 4.75, 5.00, 5.25]

ANCHOR_SWEEP = 100
ANCHOR_BODY = 1.40
ANCHOR_CLOSE = 0.25
ANCHOR_WICK = 0.25
ANCHOR_WEEKDAY_EXCLUDE = 2  # Wednesday, Europe/London
ANCHOR_RR = 4.50

_GEOMETRY_INDEX_CACHE = {}


def high_sweep_cfg(config_id, sweep_lb, body, close_loc, wick, rr=ANCHOR_RR, excluded_weekday=ANCHOR_WEEKDAY_EXCLUDE):
    context = {"type": "NONE"} if excluded_weekday is None else {
        "type": "EXCLUDE_WEEKDAY",
        "weekday": int(excluded_weekday),
    }
    return {
        "config_id": config_id,
        "family": "HIGH_SWEEP_REJECTION",
        "br_min": None,
        "body_atr_min": float(body),
        "range_atr_min": None,
        "close_loc_max": float(close_loc),
        "upper_wick_body_min": float(wick),
        "structure_lb": None,
        "structure_dist_atr_max": None,
        "sweep_lb": int(sweep_lb),
        "breakout_lb": None,
        "compression_max": None,
        "rally_12h_min": None,
        "rr": float(rr),
        "context": context,
    }


def context_label(cfg):
    c = cfg.get("context", {"type": "NONE"})
    if c["type"] == "EXCLUDE_WEEKDAY":
        names = ["MON", "TUE", "WED", "THU", "FRI"]
        return "EXCLUDE_" + names[int(c["weekday"])]
    return c["type"]


def geometry_indices(cfg, f):
    key = (
        int(cfg["sweep_lb"]),
        round(float(cfg["body_atr_min"]), 5),
        round(float(cfg["close_loc_max"]), 5),
        round(float(cfg["upper_wick_body_min"]), 5),
    )
    if key not in _GEOMETRY_INDEX_CACHE:
        prior_high = f["prev_high"][key[0]]
        mask = (
            f["valid_atr"]
            & f["bearish"]
            & np.isfinite(prior_high)
            & (f["high"] > prior_high)
            & (f["close"] < prior_high)
            & (f["body_atr"] >= key[1])
            & (f["close_location"] <= key[2])
            & (f["upper_wick_body"] >= key[3])
        )
        mask[:220] = False
        _GEOMETRY_INDEX_CACHE[key] = np.flatnonzero(mask).astype(int)
    return _GEOMETRY_INDEX_CACHE[key]


def focused_signal_indices(cfg, f):
    indices = geometry_indices(cfg, f)
    context = cfg.get("context", {"type": "NONE"})
    if context["type"] == "NONE":
        return indices.tolist()
    if context["type"] == "EXCLUDE_WEEKDAY":
        excluded = int(context["weekday"])
        weekdays = f["london_weekday"]
        return indices[weekdays[indices] != excluded].tolist()
    raise RuntimeError("Unsupported final-confirmation context: " + str(context))


def decorate(row, cfg):
    row = dict(row)
    row["context_detail"] = context_label(cfg)
    row["excluded_weekday"] = (
        cfg.get("context", {}).get("weekday")
        if cfg.get("context", {}).get("type") == "EXCLUDE_WEEKDAY"
        else None
    )
    return row


def evaluate_cfg(cfg, candles, features):
    indices = focused_signal_indices(cfg, features)
    return decorate(evaluation_row(cfg, candles, indices), cfg)


def unique_cfgs(configs):
    seen = set()
    out = []
    for cfg in configs:
        c = cfg.get("context", {"type": "NONE"})
        key = (
            cfg["sweep_lb"], cfg["body_atr_min"], cfg["close_loc_max"],
            cfg["upper_wick_body_min"], cfg["rr"],
            c.get("type"), c.get("weekday"),
        )
        if key not in seen:
            seen.add(key)
            out.append(cfg)
    return out


def selected_bases(rows, by_id, top_n, ensure_anchor=True):
    ranked = sort_rows(rows)
    chosen = []
    ids = set()

    # Primary robust shortlist.
    for row in ranked:
        if int(row["full_trades"]) < 35:
            continue
        if int(row["positive_eras"]) < 3:
            continue
        cid = row["config_id"]
        if cid not in ids:
            chosen.append(deepcopy(by_id[cid]))
            ids.add(cid)
        if len(chosen) >= top_n:
            break

    # Preserve sweep-lookback diversity so the boundary question remains visible.
    for lb in SWEEP_GRID:
        candidates = [r for r in ranked if int(float(r.get("sweep_lb") or 0)) == lb]
        if candidates:
            cid = candidates[0]["config_id"]
            if cid not in ids:
                chosen.append(deepcopy(by_id[cid]))
                ids.add(cid)

    # Preserve body-threshold diversity.
    for body in BODY_GRID:
        candidates = [r for r in ranked if abs(float(r.get("body_atr_min") or -99) - body) < 1e-9]
        if candidates:
            cid = candidates[0]["config_id"]
            if cid not in ids:
                chosen.append(deepcopy(by_id[cid]))
                ids.add(cid)

    if ensure_anchor:
        anchor_key = (ANCHOR_SWEEP, ANCHOR_BODY, ANCHOR_CLOSE, ANCHOR_WICK, ANCHOR_RR, "EXCLUDE_WEEKDAY", ANCHOR_WEEKDAY_EXCLUDE)
        found = False
        for cfg in chosen:
            c = cfg.get("context", {})
            key = (cfg["sweep_lb"], cfg["body_atr_min"], cfg["close_loc_max"], cfg["upper_wick_body_min"], cfg["rr"], c.get("type"), c.get("weekday"))
            if key == anchor_key:
                found = True
                break
        if not found:
            chosen.append(high_sweep_cfg(
                "ANCHOR_BASE", ANCHOR_SWEEP, ANCHOR_BODY, ANCHOR_CLOSE,
                ANCHOR_WICK, ANCHOR_RR, ANCHOR_WEEKDAY_EXCLUDE,
            ))
    return unique_cfgs(chosen)


def best_by(rows, field):
    grouped = {}
    for row in sort_rows(rows):
        val = row.get(field)
        if val not in grouped:
            grouped[val] = row
    out = []
    for val, row in grouped.items():
        x = dict(row)
        x["summary_field"] = field
        x["summary_value"] = val
        out.append(x)
    return out


def build_bundle():
    paths = [
        OUT_COVERAGE, OUT_PARITY, OUT_STAGE1, OUT_STAGE2, OUT_STAGE3,
        OUT_STAGE4, OUT_FINALISTS, OUT_PERIODS, OUT_COST,
        OUT_ROLLING, OUT_ROLLING_SUMMARY, OUT_CALENDAR,
        OUT_CALENDAR_SUMMARY, OUT_PLATEAU, OUT_TRADES, OUT_NOTES,
    ]
    with zipfile.ZipFile(OUT_BUNDLE, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in paths:
            if os.path.exists(path):
                archive.write(path, arcname=os.path.basename(path))


def run_research():
    try:
        STATUS.clear()
        STATUS.update({
            "state": "downloading",
            "message": "Downloading EUR/GBP M15 history",
            "orders_supported": False,
            "trading_enabled": False,
        })

        # Final confirmation uses M15 only. No HTF condition is being tested,
        # so downloading H1/H4/D again adds runtime without adding evidence.
        m15 = fetch_history("M15", START, NOW, 35)
        if not m15:
            raise RuntimeError("Missing required EUR_GBP M15 history")

        write_csv(OUT_COVERAGE, [{
            "instrument": PAIR,
            "requested_start_utc": iso_utc(START),
            "actual_first_m15_utc": iso_utc(m15[0]["time"]),
            "actual_last_m15_utc": iso_utc(m15[-1]["time"]),
            "m15_candles": len(m15),
            "htf_downloaded": False,
            "purpose": "Final HIGH_SWEEP_REJECTION local confirmation only",
        }])

        STATUS.update({"state": "precomputing", "message": "Building focused M15 feature cache"})
        features = build_features(m15, {}, {}, {})
        features["times"] = [c["time"] for c in m15]
        london = ZoneInfo("Europe/London")
        features["london_weekday"] = np.array(
            [c["time"].astimezone(london).weekday() for c in m15],
            dtype=np.int8,
        )

        # ----------------------------------------------------
        # HARD PARITY: previous winning finalist
        # ----------------------------------------------------
        anchor = high_sweep_cfg(
            "ANCHOR_S3_00053", ANCHOR_SWEEP, ANCHOR_BODY,
            ANCHOR_CLOSE, ANCHOR_WICK, ANCHOR_RR,
            ANCHOR_WEEKDAY_EXCLUDE,
        )
        anchor_indices = focused_signal_indices(anchor, features)
        anchor_trades = run_backtest(
            m15, anchor_indices, ANCHOR_RR, PRIMARY_COST_PIPS,
            START, PARITY_CUTOFF,
        )
        parity_status = "MATCH" if len(anchor_trades) == ANCHOR_EXPECTED_TRADES else "FAIL"
        parity_row = {
            "anchor": "S3_00053",
            "sweep_lb": ANCHOR_SWEEP,
            "body_atr_min": ANCHOR_BODY,
            "close_loc_max": ANCHOR_CLOSE,
            "upper_wick_body_min": ANCHOR_WICK,
            "excluded_weekday_london": "Wednesday",
            "rr": ANCHOR_RR,
            "cost_pips": PRIMARY_COST_PIPS,
            "parity_cutoff_utc": iso_utc(PARITY_CUTOFF),
            "expected_trades": ANCHOR_EXPECTED_TRADES,
            "actual_trades": len(anchor_trades),
            "status": parity_status,
        }
        write_csv(OUT_PARITY, [parity_row])
        if parity_status != "MATCH":
            raise RuntimeError(
                f"Anchor parity failed: expected {ANCHOR_EXPECTED_TRADES}, got {len(anchor_trades)} through {iso_utc(PARITY_CUTOFF)}"
            )

        # ----------------------------------------------------
        # STAGE 1: sweep/body/close neighbourhood
        # Wick, weekday and RR frozen at the prior winner.
        # ----------------------------------------------------
        stage1_configs = []
        counter = 0
        for lb in SWEEP_GRID:
            for body in BODY_GRID:
                for close_loc in CLOSE_GRID:
                    counter += 1
                    stage1_configs.append(high_sweep_cfg(
                        f"G_{counter:04d}", lb, body, close_loc,
                        ANCHOR_WICK, ANCHOR_RR, ANCHOR_WEEKDAY_EXCLUDE,
                    ))

        stage1_rows = []
        for i, cfg in enumerate(stage1_configs, 1):
            STATUS.update({"state": "stage1_geometry", "message": f"Geometry {i}/{len(stage1_configs)}"})
            stage1_rows.append(evaluate_cfg(cfg, m15, features))
        stage1_rows = sort_rows(stage1_rows)
        write_csv(OUT_STAGE1, stage1_rows)
        stage1_by_id = {c["config_id"]: c for c in stage1_configs}

        # ----------------------------------------------------
        # STAGE 2: upper-wick boundary confirmation
        # ----------------------------------------------------
        bases2 = selected_bases(stage1_rows, stage1_by_id, top_n=18)
        stage2_configs = []
        counter = 0
        for base in bases2:
            for wick in WICK_GRID:
                counter += 1
                cfg = deepcopy(base)
                cfg["config_id"] = f"W_{counter:04d}"
                cfg["upper_wick_body_min"] = wick
                stage2_configs.append(cfg)
        stage2_configs = unique_cfgs(stage2_configs)

        stage2_rows = []
        for i, cfg in enumerate(stage2_configs, 1):
            STATUS.update({"state": "stage2_wick", "message": f"Wick {i}/{len(stage2_configs)}"})
            stage2_rows.append(evaluate_cfg(cfg, m15, features))
        stage2_rows = sort_rows(stage2_rows)
        write_csv(OUT_STAGE2, stage2_rows)
        stage2_by_id = {c["config_id"]: c for c in stage2_configs}

        # ----------------------------------------------------
        # STAGE 3: weekday confirmation
        # NONE + exclude each weekday in Europe/London.
        # ----------------------------------------------------
        bases3 = selected_bases(stage2_rows, stage2_by_id, top_n=16)
        stage3_configs = []
        counter = 0
        for base in bases3:
            for excluded in [None, 0, 1, 2, 3, 4]:
                counter += 1
                cfg = deepcopy(base)
                cfg["config_id"] = f"D_{counter:04d}"
                cfg["context"] = {"type": "NONE"} if excluded is None else {
                    "type": "EXCLUDE_WEEKDAY", "weekday": excluded,
                }
                stage3_configs.append(cfg)
        stage3_configs = unique_cfgs(stage3_configs)

        stage3_rows = []
        for i, cfg in enumerate(stage3_configs, 1):
            STATUS.update({"state": "stage3_weekday", "message": f"Weekday {i}/{len(stage3_configs)}"})
            stage3_rows.append(evaluate_cfg(cfg, m15, features))
        stage3_rows = sort_rows(stage3_rows)
        write_csv(OUT_STAGE3, stage3_rows)
        stage3_by_id = {c["config_id"]: c for c in stage3_configs}

        # ----------------------------------------------------
        # STAGE 4: RR only around strongest geometry/context.
        # ----------------------------------------------------
        bases4 = selected_bases(stage3_rows, stage3_by_id, top_n=14)
        stage4_configs = []
        counter = 0
        for base in bases4:
            for rr in RR_GRID_FINAL:
                counter += 1
                cfg = deepcopy(base)
                cfg["config_id"] = f"R_{counter:04d}"
                cfg["rr"] = rr
                stage4_configs.append(cfg)
        stage4_configs = unique_cfgs(stage4_configs)

        stage4_rows = []
        for i, cfg in enumerate(stage4_configs, 1):
            STATUS.update({"state": "stage4_rr", "message": f"RR {i}/{len(stage4_configs)}"})
            stage4_rows.append(evaluate_cfg(cfg, m15, features))
        stage4_rows = sort_rows(stage4_rows)
        write_csv(OUT_STAGE4, stage4_rows)
        stage4_by_id = {c["config_id"]: c for c in stage4_configs}

        # ----------------------------------------------------
        # FINALISTS: prefer 4 positive eras and sane sample size.
        # Keep anchor as a direct comparison even if it falls below top 12.
        # ----------------------------------------------------
        finalists = []
        finalist_ids = set()
        for row in stage4_rows:
            if int(row["positive_eras"]) < 4:
                continue
            if int(row["full_trades"]) < 40:
                continue
            cid = row["config_id"]
            finalists.append(deepcopy(stage4_by_id[cid]))
            finalist_ids.add(cid)
            if len(finalists) >= 12:
                break

        if len(finalists) < 12:
            for row in stage4_rows:
                cid = row["config_id"]
                if cid in finalist_ids:
                    continue
                finalists.append(deepcopy(stage4_by_id[cid]))
                finalist_ids.add(cid)
                if len(finalists) >= 12:
                    break

        # Exact prior anchor, deep-tested again under current data.
        finalists.append(anchor)
        finalists = unique_cfgs(finalists)

        finalist_rows = [evaluate_cfg(cfg, m15, features) for cfg in finalists]
        finalist_rows = sort_rows(finalist_rows)
        write_csv(OUT_FINALISTS, finalist_rows)

        # ----------------------------------------------------
        # DEEP VALIDATION
        # ----------------------------------------------------
        period_output = []
        cost_output = []
        rolling_output = []
        calendar_output = []
        trade_output = []

        for i, cfg in enumerate(finalists, 1):
            STATUS.update({"state": "deep_validation", "message": f"Deep finalist {i}/{len(finalists)}"})
            indices = focused_signal_indices(cfg, features)

            for label, start, end in period_definitions():
                row = result_row(cfg, label, run_backtest(
                    m15, indices, cfg["rr"], PRIMARY_COST_PIPS, start, end,
                ))
                period_output.append(decorate(row, cfg))

            for cost in COST_GRID:
                for label, start, end in [
                    ("FULL_HISTORY", START, NOW),
                    ("PRE_2010", START, datetime(2010,1,1,tzinfo=timezone.utc)),
                    ("2010_PLUS", datetime(2010,1,1,tzinfo=timezone.utc), NOW),
                ]:
                    row = result_row(cfg, label, run_backtest(
                        m15, indices, cfg["rr"], cost, start, end,
                    ))
                    row = decorate(row, cfg)
                    row["cost_pips"] = cost
                    cost_output.append(row)

            rr = rolling_rows(cfg, m15, indices)
            for row in rr:
                row.update({
                    "context_detail": context_label(cfg),
                    "sweep_lb": cfg["sweep_lb"],
                    "body_atr_min": cfg["body_atr_min"],
                    "close_loc_max": cfg["close_loc_max"],
                    "upper_wick_body_min": cfg["upper_wick_body_min"],
                    "rr": cfg["rr"],
                })
            rolling_output.extend(rr)

            cr = calendar_rows(cfg, m15, indices)
            for row in cr:
                row.update({
                    "context_detail": context_label(cfg),
                    "sweep_lb": cfg["sweep_lb"],
                    "body_atr_min": cfg["body_atr_min"],
                    "close_loc_max": cfg["close_loc_max"],
                    "upper_wick_body_min": cfg["upper_wick_body_min"],
                    "rr": cfg["rr"],
                })
            calendar_output.extend(cr)

            full_trades = run_backtest(m15, indices, cfg["rr"], PRIMARY_COST_PIPS, START, NOW)
            for trade in full_trades:
                row = dict(trade)
                row.update({
                    "config_id": cfg["config_id"],
                    "family": cfg["family"],
                    "context_detail": context_label(cfg),
                    "sweep_lb": cfg["sweep_lb"],
                    "body_atr_min": cfg["body_atr_min"],
                    "close_loc_max": cfg["close_loc_max"],
                    "upper_wick_body_min": cfg["upper_wick_body_min"],
                    "rr": cfg["rr"],
                })
                trade_output.append(row)

        write_csv(OUT_PERIODS, period_output)
        write_csv(OUT_COST, cost_output)
        write_csv(OUT_ROLLING, rolling_output)
        write_csv(OUT_ROLLING_SUMMARY, rolling_summary_rows(rolling_output))
        write_csv(OUT_CALENDAR, calendar_output)
        write_csv(OUT_CALENDAR_SUMMARY, calendar_summary_rows(calendar_output))
        write_csv(OUT_TRADES, trade_output)

        parameter_summary = []
        parameter_summary += best_by(stage1_rows, "sweep_lb")
        parameter_summary += best_by(stage1_rows, "body_atr_min")
        parameter_summary += best_by(stage1_rows, "close_loc_max")
        parameter_summary += best_by(stage2_rows, "upper_wick_body_min")
        parameter_summary += best_by(stage3_rows, "context_detail")
        parameter_summary += best_by(stage4_rows, "rr")
        write_csv(OUT_PLATEAU, parameter_summary)

        write_csv(OUT_NOTES, [
            {"item": "Purpose", "value": "Final focused confirmation of EUR/GBP M15 SHORT HIGH_SWEEP_REJECTION only; no new archetypes."},
            {"item": "Previous winner", "value": "Sweep100; body>=1.40ATR; close location<=0.25; upper wick/body>=0.25; exclude Wednesday Europe/London; RR4.50; 59 trades in prior broad run."},
            {"item": "Hard parity", "value": f"Previous winner must reproduce {ANCHOR_EXPECTED_TRADES} trades through {iso_utc(PARITY_CUTOFF)} at 1-pip adverse cost."},
            {"item": "Stage 1", "value": "Sweep 60/80/100/120/140/165/200 x body 1.10/1.20/1.25/1.30/1.40/1.50/1.60 x close location 0.20/0.25/0.30/0.35; wick0.25, Wednesday excluded, RR4.50 frozen."},
            {"item": "Stage 2", "value": "Upper-wick/body 0.15/0.20/0.25/0.30/0.35 only around robust Stage-1 geometries."},
            {"item": "Stage 3", "value": "No weekday exclusion versus excluding Mon/Tue/Wed/Thu/Fri, all evaluated in Europe/London local time."},
            {"item": "Stage 4", "value": "RR3.75/4.00/4.25/4.50/4.75/5.00/5.25 only around robust geometry/context survivors."},
            {"item": "Railway safety", "value": "M15 only; unique geometry signal indices cached and reused across weekday/RR variants; no HTF downloads; no giant Cartesian product."},
            {"item": "Historical convention", "value": "SHORT fill = signal close - adverse cost; stop = high +10 ticks; target from reference-close risk; actual R from adverse fill; exit scan begins next bar; exact exit-candle signal eligible; pyramiding0."},
            {"item": "Interpretation", "value": "Full-history robustness confirmation; no pristine historical OOS remains."},
        ])

        STATUS.update({"state": "packaging", "message": "Building one results ZIP"})
        build_bundle()
        STATUS.update({
            "state": "complete",
            "message": "EUR/GBP M15 SHORT final high-sweep confirmation complete",
            "stage1_geometry_configs": len(stage1_configs),
            "stage2_wick_configs": len(stage2_configs),
            "stage3_weekday_configs": len(stage3_configs),
            "stage4_rr_configs": len(stage4_configs),
            "deep_finalists": len(finalists),
            "geometry_cache_entries": len(_GEOMETRY_INDEX_CACHE),
            "results_bundle": OUT_BUNDLE,
            "orders_supported": False,
            "trading_enabled": False,
        })

    except Exception as error:
        tb = traceback.format_exc()
        STATUS.update({
            "state": "error",
            "error_type": type(error).__name__,
            "message": str(error),
            "traceback": tb,
            "orders_supported": False,
            "trading_enabled": False,
        })
        print(tb, flush=True)


@app.route("/")
def final_root():
    return jsonify({
        "service": "EUR/GBP M15 SHORT Final High-Sweep Confirmation",
        "state": STATUS.get("state"),
        "message": STATUS.get("message"),
        "error_type": STATUS.get("error_type"),
        "instrument": PAIR,
        "timeframe": "M15",
        "side": "SELL",
        "family": "HIGH_SWEEP_REJECTION",
        "orders_supported": False,
        "trading_enabled": False,
        "routes": [
            "/eurgbp-m15-short-final-high-sweep/status",
            "/eurgbp-m15-short-final-high-sweep/results",
        ],
    })


@app.route("/eurgbp-m15-short-final-high-sweep/status")
def final_status():
    return jsonify(STATUS)


@app.route("/eurgbp-m15-short-final-high-sweep/results")
def final_results():
    if not os.path.exists(OUT_BUNDLE):
        return jsonify({"error": "Results not ready yet", "state": STATUS.get("state"), "message": STATUS.get("message")}), 404
    return send_file(os.path.abspath(OUT_BUNDLE), as_attachment=True, download_name=OUT_BUNDLE)


if __name__ == "__main__":
    thread = threading.Thread(
        target=run_research,
        name="eurgbp-m15-short-final-high-sweep",
        daemon=True,
    )
    thread.start()
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
