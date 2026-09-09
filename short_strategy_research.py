
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
# USD/JPY M15 SHORT — FULL-HISTORY RE-EXAMINATION
#
# PURPOSE
# -------
# Fresh USD/JPY M15 SHORT research from earliest reliable
# OANDA M15 history (~May 2002) to present.
#
# There is NO prior M15 USDJPY short strategy being treated as
# a core locked benchmark here. This is a from-scratch search.
#
# HISTORICAL COST
# ---------------
# M15 baseline = 1.0 pip adverse fill.
# Stress = 0.5 / 1.0 / 1.5 / 2.0 pips.
#
# LONG-ERA TESTS
# --------------
# 2002-07
# 2008-13
# 2014-19
# 2020-now
#
# Also:
# 2002-17 vs 2018+
# last 5Y
# last 2Y
# rolling 12/24/36M
# completed calendar years
#
# IMPORTANT
# ---------
# Full history is now part of the research universe, so this
# is robustness/temporal validation rather than pristine OOS.
#
# ============================================================
# SIX SHORT ARCHETYPES
# ============================================================
#
# 1) BEAR_ENGULF_STRUCTURE
# 2) HIGH_SWEEP_DISPLACEMENT
# 3) FAILED_BREAKOUT_REJECTION
# 4) BEAR_OUTSIDE_REVERSAL
# 5) COMPRESSION_BREAKDOWN
# 6) BLOWOFF_REJECTION
#
# ============================================================
# NO-LOOKAHEAD CONTEXT
# ============================================================
#
# H1/H4:
#   complete_at = next ACTUAL HTF candle OPEN
#   lookup = bisect_right(completion_times, signal_time) - 1
#
# Daily:
#   OANDA D
#   dailyAlignment=17
#   alignmentTimezone=America/New_York
#
# Prior momentum:
#   ALWAYS ends at close[i-1]
#   NEVER uses signal candle close.
#
# Signal timestamp:
#   M15 candle OPEN.
#
# ============================================================
# SHORT HISTORICAL CONVENTIONS
# ============================================================
#
# OANDA midpoint.
# ATR14 = Wilder/RMA, SMA seeded.
#
# USDJPY:
#   tick = 0.001
#   pip  = 0.01
#
# Reference entry = signal close.
# Historical short fill = close - adverse cost.
# Stop = signal high + 10 ticks.
# Target = based on REFERENCE signal-close risk.
# Actual R = based on adverse fill.
#
# Pyramiding = 0.
# Exit checking begins next candle.
# Exact exit-candle signal eligible.
#
# Same-bar SHORT tie:
#   if high is closer to candle open => STOP first
#   otherwise TARGET first.
#
# ============================================================
# ONE ZIP
# ============================================================
#
# /usdjpy-m15-short-full-history/results
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

HTF_WARMUP_START = (
    START - timedelta(days=900)
)

NY = ZoneInfo("America/New_York")

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

STAGE1_RR = 3.50

STAGE1_KEEP = 16
STAGE2_BASE_KEEP = 8
STAGE2_KEEP = 12
STAGE3_BASE_KEEP = 6
FINALIST_KEEP = 10

MIN_STAGE1_TRADES = 55
MIN_FINAL_TRADES = 65


# ============================================================
# OUTPUTS
# ============================================================

OUT_COVERAGE = (
    "usdjpy_m15_short_full_history_coverage.csv"
)

OUT_STAGE1 = (
    "usdjpy_m15_short_full_history_stage1_raw.csv"
)

OUT_STAGE2 = (
    "usdjpy_m15_short_full_history_stage2_context.csv"
)

OUT_STAGE3 = (
    "usdjpy_m15_short_full_history_stage3_local_rr.csv"
)

OUT_FINALISTS = (
    "usdjpy_m15_short_full_history_finalists.csv"
)

OUT_PERIODS = (
    "usdjpy_m15_short_full_history_periods.csv"
)

OUT_COST = (
    "usdjpy_m15_short_full_history_cost_stress.csv"
)

OUT_ROLLING = (
    "usdjpy_m15_short_full_history_rolling.csv"
)

OUT_ROLLING_SUMMARY = (
    "usdjpy_m15_short_full_history_rolling_summary.csv"
)

OUT_CALENDAR = (
    "usdjpy_m15_short_full_history_calendar_years.csv"
)

OUT_CALENDAR_SUMMARY = (
    "usdjpy_m15_short_full_history_calendar_summary.csv"
)

OUT_ABLATION = (
    "usdjpy_m15_short_full_history_ablation.csv"
)

OUT_PLATEAU = (
    "usdjpy_m15_short_full_history_plateau.csv"
)

OUT_TRADES = (
    "usdjpy_m15_short_full_history_finalist_trades.csv"
)

OUT_NOTES = (
    "usdjpy_m15_short_full_history_notes.csv"
)

OUT_BUNDLE = (
    "USDJPY_M15_SHORT_FULL_HISTORY_REEXAMINATION_RESULTS.zip"
)

STATUS = {
    "state":
        "not_started",

    "message":
        "USD/JPY M15 SHORT full-history re-examination not started",

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
        with open(path, "w", encoding="utf-8") as handle:
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
        OUT_ABLATION,
        OUT_PLATEAU,
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
        if not item.get("complete", False):
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

    result = list(by_time.values())
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
            total -= csum[i - length]
            count -= ccount[i - length]

        if count == length:
            result[i] = (
                total / length
            )

    return result


def ema_list(values, length):
    result = [
        None
    ] * len(values)

    if len(values) < length:
        return result

    result[length - 1] = (
        sum(values[:length])
        / length
    )

    alpha = (
        2.0
        / (length + 1.0)
    )

    for i in range(
        length,
        len(values),
    ):
        result[i] = (
            alpha * values[i]
            + (1.0 - alpha)
            * result[i - 1]
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
                values[dq[0]]
            )

        if mode == "min":
            while (
                dq
                and values[dq[-1]]
                >= values[i]
            ):
                dq.pop()

        else:
            while (
                dq
                and values[dq[-1]]
                <= values[i]
            ):
                dq.pop()

        dq.append(i)

    return result


# ============================================================
# HTF STATE — NO LOOKAHEAD
# ============================================================

def build_htf_state(candles):
    closes = [
        candle["close"]
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

    atr = atr14(candles)

    atr_mean50 = sma_np(
        atr,
        50,
    )

    rows = []

    for i, candle in enumerate(candles):
        complete_at = (
            candles[i + 1]["time"]
            if i + 1 < len(candles)
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
        if row["complete_at"] is not None
    ]

    completion_times = [
        row["complete_at"]
        for row in eligible
    ]

    keys = [
        "close",
        "ema50",
        "ema100",
        "ema200",
        "atr_ratio50",
    ]

    result = {
        key:
            np.full(
                len(m15_times),
                np.nan,
                dtype=float,
            )
        for key in keys
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

        row = eligible[position]

        for key in keys:
            value = row[key]

            if value is not None:
                result[key][i] = value

    return result


# ============================================================
# FEATURE CACHE
# ============================================================

def build_features(
    m15,
    h1,
    h4,
    daily,
):
    n = len(m15)

    times = [
        candle["time"]
        for candle in m15
    ]

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

    atr_mean20 = sma_np(
        atr,
        20,
    )

    bearish = (
        closes < opens
    )

    exact_bear_engulf = np.zeros(
        n,
        dtype=bool,
    )

    exact_bear_engulf[1:] = (
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

    bearish_body = (
        opens - closes
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
        bearish_body[
            positive_prev_body
        ]
        / previous_body[
            positive_prev_body
        ]
    )

    # Consistent doji convention used elsewhere in the project.
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
        bearish_body[
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

    close_loc = np.full(
        n,
        np.nan,
        dtype=float,
    )

    valid_range = (
        candle_range > 0
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
        bearish_body > 0
    )

    upper_wick_body[
        valid_body
    ] = (
        upper_wick[
            valid_body
        ]
        / bearish_body[
            valid_body
        ]
    )

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

    previous_atr[1:] = atr[:-1]
    previous_atr_mean20[1:] = (
        atr_mean20[:-1]
    )

    compression = np.full(
        n,
        np.nan,
        dtype=float,
    )

    valid_comp = (
        np.isfinite(previous_atr)
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

    lookbacks = [
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
        200,
    ]

    previous_low = {}
    previous_high = {}

    for lookback in lookbacks:
        previous_low[
            lookback
        ] = rolling_previous_extreme(
            lows,
            lookback,
            "min",
        )

        previous_high[
            lookback
        ] = rolling_previous_extreme(
            highs,
            lookback,
            "max",
        )

    structure_distance = {}

    for lookback in [
        40,
        60,
        80,
        100,
        120,
        165,
        200,
    ]:
        distance = np.full(
            n,
            np.nan,
            dtype=float,
        )

        valid = (
            valid_atr
            & np.isfinite(
                previous_high[
                    lookback
                ]
            )
        )

        distance[
            valid
        ] = (
            np.abs(
                highs[
                    valid
                ]
                - previous_high[
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
        ] = distance

    # Strict PRIOR momentum; signal close excluded.
    mom4_up = np.full(
        n,
        np.nan,
        dtype=float,
    )

    mom12_up = np.full(
        n,
        np.nan,
        dtype=float,
    )

    mom24_up = np.full(
        n,
        np.nan,
        dtype=float,
    )

    for i in range(
        17,
        n,
    ):
        if valid_atr[i]:
            mom4_up[i] = (
                closes[i - 1]
                - closes[i - 17]
            ) / atr[i]

    for i in range(
        49,
        n,
    ):
        if valid_atr[i]:
            mom12_up[i] = (
                closes[i - 1]
                - closes[i - 49]
            ) / atr[i]

    for i in range(
        97,
        n,
    ):
        if valid_atr[i]:
            mom24_up[i] = (
                closes[i - 1]
                - closes[i - 97]
            ) / atr[i]

    previous_candle_low = np.full(
        n,
        np.nan,
        dtype=float,
    )

    previous_candle_low[1:] = lows[:-1]

    ny_hour = np.zeros(
        n,
        dtype=np.int16,
    )

    ny_weekday = np.zeros(
        n,
        dtype=np.int16,
    )

    for i, timestamp in enumerate(times):
        local = timestamp.astimezone(NY)
        ny_hour[i] = local.hour
        ny_weekday[i] = local.weekday()

    return {
        "n":
            n,

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

        "bearish":
            bearish,

        "exact_bear_engulf":
            exact_bear_engulf,

        "body_ratio":
            body_ratio,

        "body_atr":
            body_atr,

        "range_atr":
            range_atr,

        "close_loc":
            close_loc,

        "upper_wick_body":
            upper_wick_body,

        "compression":
            compression,

        "previous_low":
            previous_low,

        "previous_high":
            previous_high,

        "structure_distance":
            structure_distance,

        "mom4_up":
            mom4_up,

        "mom12_up":
            mom12_up,

        "mom24_up":
            mom24_up,

        "previous_candle_low":
            previous_candle_low,

        "ny_hour":
            ny_hour,

        "ny_weekday":
            ny_weekday,

        "h1_close":
            h1["close"],

        "h1_ema50":
            h1["ema50"],

        "h1_ema100":
            h1["ema100"],

        "h1_ema200":
            h1["ema200"],

        "h1_atr_ratio50":
            h1["atr_ratio50"],

        "h4_close":
            h4["close"],

        "h4_ema50":
            h4["ema50"],

        "h4_ema100":
            h4["ema100"],

        "h4_ema200":
            h4["ema200"],

        "h4_atr_ratio50":
            h4["atr_ratio50"],

        "d_close":
            daily["close"],

        "d_ema50":
            daily["ema50"],

        "d_ema100":
            daily["ema100"],

        "d_ema200":
            daily["ema200"],

        "d_atr_ratio50":
            daily["atr_ratio50"],
    }


# ============================================================
# CONFIGS
# ============================================================

def base_config(
    config_id,
    family,
    rr=STAGE1_RR,
):
    return {
        "config_id":
            config_id,

        "family":
            family,

        "rr":
            rr,

        "br_min":
            None,

        "body_atr_min":
            None,

        "range_atr_min":
            None,

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

        "breakdown_lb":
            None,

        "compression_max":
            None,

        "mom4_min":
            None,

        "mom12_min":
            None,

        "mom24_min":
            None,

        "require_close_lt_prev_low":
            False,

        "context":
            "NONE",
    }


def build_stage1_configs():
    configs = []

    # --------------------------------------------------------
    # 1) BEARISH ENGULF + STRUCTURE
    # --------------------------------------------------------
    for i, values in enumerate([
        (1.00, 0.75, 1.10, 60, 0.10),
        (1.00, 1.00, 1.20, 100, 0.10),
        (1.10, 1.00, 1.30, 100, 0.15),
        (1.20, 1.00, 1.40, 120, 0.10),
        (1.20, 1.25, 1.40, 165, 0.10),
        (1.30, 1.00, 1.40, 120, 0.15),
        (1.30, 1.25, 1.50, 165, 0.10),
        (1.40, 1.25, 1.50, 165, 0.15),
        (1.50, 1.25, 1.60, 200, 0.10),
    ]):
        br, body, rng, lb, dist = values

        cfg = base_config(
            f"S1_ENG_{i:02d}",
            "BEAR_ENGULF_STRUCTURE",
        )

        cfg.update({
            "br_min":
                br,

            "body_atr_min":
                body,

            "range_atr_min":
                rng,

            "structure_lb":
                lb,

            "structure_dist_atr_max":
                dist,
        })

        configs.append(cfg)

    # --------------------------------------------------------
    # 2) HIGH SWEEP + DISPLACEMENT
    # --------------------------------------------------------
    for i, values in enumerate([
        (20, 0.75, 0.15, 0.75, False),
        (20, 1.00, 0.20, 1.00, True),
        (40, 1.00, 0.20, 1.25, True),
        (40, 1.25, 0.25, 1.50, True),
        (60, 1.00, 0.25, 1.25, True),
        (60, 1.25, 0.25, 1.50, True),
        (100, 1.00, 0.25, 1.50, True),
        (100, 1.25, 0.25, 1.75, True),
        (100, 1.50, 0.25, 2.00, True),
    ]):
        sweep_lb, body, wick, mom4, close_prev_low = values

        cfg = base_config(
            f"S1_SWEEP_{i:02d}",
            "HIGH_SWEEP_DISPLACEMENT",
        )

        cfg.update({
            "sweep_lb":
                sweep_lb,

            "body_atr_min":
                body,

            "upper_wick_body_min":
                wick,

            "mom4_min":
                mom4,

            "require_close_lt_prev_low":
                close_prev_low,
        })

        configs.append(cfg)

    # --------------------------------------------------------
    # 3) FAILED BREAKOUT + REJECTION
    # --------------------------------------------------------
    for i, values in enumerate([
        (20, 0.50, 0.40),
        (20, 0.75, 0.30),
        (40, 0.75, 0.35),
        (40, 1.00, 0.30),
        (60, 0.75, 0.30),
        (60, 1.00, 0.25),
        (100, 0.75, 0.30),
        (100, 1.00, 0.25),
        (165, 1.00, 0.25),
    ]):
        lb, body, close_loc_max = values

        cfg = base_config(
            f"S1_FAIL_{i:02d}",
            "FAILED_BREAKOUT_REJECTION",
        )

        cfg.update({
            "sweep_lb":
                lb,

            "body_atr_min":
                body,

            "close_loc_max":
                close_loc_max,
        })

        configs.append(cfg)

    # --------------------------------------------------------
    # 4) BEAR OUTSIDE REVERSAL
    # --------------------------------------------------------
    for i, values in enumerate([
        (0.50, 0.40, 40, 0.20),
        (0.75, 0.35, 40, 0.15),
        (0.75, 0.30, 60, 0.20),
        (1.00, 0.35, 60, 0.15),
        (1.00, 0.25, 80, 0.20),
        (1.25, 0.30, 80, 0.15),
        (1.25, 0.25, 100, 0.20),
        (1.50, 0.25, 100, 0.15),
    ]):
        body, close_loc_max, lb, dist = values

        cfg = base_config(
            f"S1_OUT_{i:02d}",
            "BEAR_OUTSIDE_REVERSAL",
        )

        cfg.update({
            "body_atr_min":
                body,

            "close_loc_max":
                close_loc_max,

            "structure_lb":
                lb,

            "structure_dist_atr_max":
                dist,
        })

        configs.append(cfg)

    # --------------------------------------------------------
    # 5) COMPRESSION BREAKDOWN
    # --------------------------------------------------------
    for i, values in enumerate([
        (0.60, 0.75, 1.20, 10),
        (0.65, 0.75, 1.30, 10),
        (0.65, 1.00, 1.40, 10),
        (0.70, 1.00, 1.40, 10),
        (0.70, 1.25, 1.50, 10),
        (0.75, 1.00, 1.40, 10),
        (0.75, 1.25, 1.50, 20),
        (0.80, 1.25, 1.60, 20),
    ]):
        comp, body, rng, breakdown = values

        cfg = base_config(
            f"S1_COMP_{i:02d}",
            "COMPRESSION_BREAKDOWN",
        )

        cfg.update({
            "compression_max":
                comp,

            "body_atr_min":
                body,

            "range_atr_min":
                rng,

            "breakdown_lb":
                breakdown,
        })

        configs.append(cfg)

    # --------------------------------------------------------
    # 6) BLOWOFF REJECTION
    # --------------------------------------------------------
    for i, values in enumerate([
        (20, 0.50, 0.75, 0.35),
        (20, 0.75, 1.00, 0.30),
        (40, 0.75, 1.00, 0.30),
        (40, 1.00, 1.25, 0.25),
        (60, 0.75, 1.25, 0.30),
        (60, 1.00, 1.50, 0.25),
        (100, 1.00, 1.50, 0.25),
        (100, 1.25, 1.75, 0.20),
    ]):
        lb, body, mom4, close_loc_max = values

        cfg = base_config(
            f"S1_BLOW_{i:02d}",
            "BLOWOFF_REJECTION",
        )

        cfg.update({
            "sweep_lb":
                lb,

            "body_atr_min":
                body,

            "mom4_min":
                mom4,

            "close_loc_max":
                close_loc_max,
        })

        configs.append(cfg)

    return configs


# ============================================================
# CONTEXTS
# ============================================================

CONTEXTS = [
    "NONE",

    "H1_CLOSE_LT_EMA100",
    "H1_CLOSE_LT_EMA200",
    "H1_EMA50_LT_EMA200",

    "H4_CLOSE_LT_EMA100",
    "H4_CLOSE_LT_EMA200",

    "D_CLOSE_LT_EMA200",
    "D_EMA50_LT_EMA200",

    "H1_ATR_GE_080",
    "H4_ATR_GE_080",
    "D_ATR_GE_080",

    "NY_BLOCK_00-03",
    "NY_BLOCK_04-07",
    "NY_BLOCK_08-11",
    "NY_BLOCK_12-15",
    "NY_BLOCK_16-19",
    "NY_BLOCK_20-23",

    "EXCLUDE_WEEKDAY_0",
    "EXCLUDE_WEEKDAY_1",
    "EXCLUDE_WEEKDAY_2",
    "EXCLUDE_WEEKDAY_3",
    "EXCLUDE_WEEKDAY_4",
]


def apply_context(
    mask,
    cfg,
    f,
):
    context = cfg.get(
        "context",
        "NONE",
    )

    if context == "H1_CLOSE_LT_EMA100":
        mask &= (
            f["h1_close"]
            < f["h1_ema100"]
        )

    elif context == "H1_CLOSE_LT_EMA200":
        mask &= (
            f["h1_close"]
            < f["h1_ema200"]
        )

    elif context == "H1_EMA50_LT_EMA200":
        mask &= (
            f["h1_ema50"]
            < f["h1_ema200"]
        )

    elif context == "H4_CLOSE_LT_EMA100":
        mask &= (
            f["h4_close"]
            < f["h4_ema100"]
        )

    elif context == "H4_CLOSE_LT_EMA200":
        mask &= (
            f["h4_close"]
            < f["h4_ema200"]
        )

    elif context == "D_CLOSE_LT_EMA200":
        mask &= (
            f["d_close"]
            < f["d_ema200"]
        )

    elif context == "D_EMA50_LT_EMA200":
        mask &= (
            f["d_ema50"]
            < f["d_ema200"]
        )

    elif context == "H1_ATR_GE_080":
        mask &= (
            f["h1_atr_ratio50"]
            >= 0.80
        )

    elif context == "H4_ATR_GE_080":
        mask &= (
            f["h4_atr_ratio50"]
            >= 0.80
        )

    elif context == "D_ATR_GE_080":
        mask &= (
            f["d_atr_ratio50"]
            >= 0.80
        )

    elif context.startswith("NY_BLOCK_"):
        block = context.split("_")[-1]

        start_hour, end_hour = map(
            int,
            block.split("-"),
        )

        mask &= (
            (
                f["ny_hour"]
                >= start_hour
            )
            & (
                f["ny_hour"]
                <= end_hour
            )
        )

    elif context.startswith(
        "EXCLUDE_WEEKDAY_"
    ):
        weekday = int(
            context.split("_")[-1]
        )

        mask &= (
            f["ny_weekday"]
            != weekday
        )

    return mask


# ============================================================
# SIGNAL LOGIC
# ============================================================

def signal_indices(
    cfg,
    f,
):
    family = cfg["family"]

    mask = (
        f["valid_atr"].copy()
        & f["bearish"]
    )

    if family == "BEAR_ENGULF_STRUCTURE":
        mask &= (
            f["exact_bear_engulf"]
        )

        mask &= (
            f["body_ratio"]
            >= cfg["br_min"]
        )

        mask &= (
            f["body_atr"]
            >= cfg["body_atr_min"]
        )

        mask &= (
            f["range_atr"]
            >= cfg["range_atr_min"]
        )

        mask &= (
            f["structure_distance"][
                cfg["structure_lb"]
            ]
            <= cfg[
                "structure_dist_atr_max"
            ]
        )

    elif family == "HIGH_SWEEP_DISPLACEMENT":
        lb = cfg["sweep_lb"]

        mask &= (
            f["high"]
            > f["previous_high"][lb]
        )

        mask &= (
            f["body_atr"]
            >= cfg["body_atr_min"]
        )

        mask &= (
            f["upper_wick_body"]
            >= cfg[
                "upper_wick_body_min"
            ]
        )

        mask &= (
            f["mom4_up"]
            >= cfg["mom4_min"]
        )

        if cfg.get(
            "require_close_lt_prev_low",
            False,
        ):
            mask &= (
                f["close"]
                < f["previous_candle_low"]
            )

    elif family == "FAILED_BREAKOUT_REJECTION":
        lb = cfg["sweep_lb"]

        prior_high = (
            f["previous_high"][lb]
        )

        mask &= (
            f["high"]
            > prior_high
        )

        mask &= (
            f["close"]
            < prior_high
        )

        mask &= (
            f["body_atr"]
            >= cfg["body_atr_min"]
        )

        mask &= (
            f["close_loc"]
            <= cfg["close_loc_max"]
        )

    elif family == "BEAR_OUTSIDE_REVERSAL":
        previous_high = np.roll(
            f["high"],
            1,
        )

        previous_low = np.roll(
            f["low"],
            1,
        )

        mask[0] = False

        mask &= (
            f["high"]
            > previous_high
        )

        mask &= (
            f["low"]
            < previous_low
        )

        mask &= (
            f["body_atr"]
            >= cfg["body_atr_min"]
        )

        mask &= (
            f["close_loc"]
            <= cfg["close_loc_max"]
        )

        mask &= (
            f["structure_distance"][
                cfg["structure_lb"]
            ]
            <= cfg[
                "structure_dist_atr_max"
            ]
        )

    elif family == "COMPRESSION_BREAKDOWN":
        mask &= (
            f["compression"]
            <= cfg["compression_max"]
        )

        mask &= (
            f["body_atr"]
            >= cfg["body_atr_min"]
        )

        mask &= (
            f["range_atr"]
            >= cfg["range_atr_min"]
        )

        mask &= (
            f["close"]
            < f["previous_low"][
                cfg["breakdown_lb"]
            ]
        )

    elif family == "BLOWOFF_REJECTION":
        lb = cfg["sweep_lb"]

        prior_high = (
            f["previous_high"][lb]
        )

        mask &= (
            f["high"]
            > prior_high
        )

        mask &= (
            f["close"]
            < f["previous_high"][10]
        )

        mask &= (
            f["body_atr"]
            >= cfg["body_atr_min"]
        )

        mask &= (
            f["mom4_up"]
            >= cfg["mom4_min"]
        )

        mask &= (
            f["close_loc"]
            <= cfg["close_loc_max"]
        )

    else:
        raise RuntimeError(
            f"Unknown family: {family}"
        )

    mask = apply_context(
        mask,
        cfg,
        f,
    )

    mask[:220] = False

    return np.flatnonzero(
        mask
    ).tolist()


# ============================================================
# SHORT BACKTEST ENGINE
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
        signal["high"]
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
            candle["high"]
            >= stop
        )

        hit_target = (
            candle["low"]
            <= target
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
                signal["time"],

            "exit_time":
                candle["time"],

            "entry_time_utc":
                iso_utc(signal["time"]),

            "exit_time_utc":
                iso_utc(candle["time"]),

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
        OUTCOME_CACHE[key] = (
            compute_outcome(
                candles,
                signal_index,
                rr,
                cost_pips,
            )
        )

    return OUTCOME_CACHE[key]


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

        use = indices[left:right]

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
            trade["exit_index"],
            lo=position + 1,
        )

    return trades


# ============================================================
# STATS
# ============================================================

def stats_from_trades(trades):
    values = [
        float(
            trade["result_r"]
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
                total_r / len(values)
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


def config_fields(cfg):
    return {
        "br_min":
            cfg.get("br_min"),

        "body_atr_min":
            cfg.get(
                "body_atr_min"
            ),

        "range_atr_min":
            cfg.get(
                "range_atr_min"
            ),

        "close_loc_max":
            cfg.get(
                "close_loc_max"
            ),

        "upper_wick_body_min":
            cfg.get(
                "upper_wick_body_min"
            ),

        "structure_lb":
            cfg.get(
                "structure_lb"
            ),

        "structure_dist_atr_max":
            cfg.get(
                "structure_dist_atr_max"
            ),

        "sweep_lb":
            cfg.get(
                "sweep_lb"
            ),

        "breakdown_lb":
            cfg.get(
                "breakdown_lb"
            ),

        "compression_max":
            cfg.get(
                "compression_max"
            ),

        "mom4_min":
            cfg.get(
                "mom4_min"
            ),

        "mom12_min":
            cfg.get(
                "mom12_min"
            ),

        "mom24_min":
            cfg.get(
                "mom24_min"
            ),

        "require_close_lt_prev_low":
            cfg.get(
                "require_close_lt_prev_low"
            ),
    }


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
            full["profit_factor"],
            3.5,
        )
        + 0.9
        * min(
            pre["profit_factor"],
            3.0,
        )
        + 0.9
        * min(
            post["profit_factor"],
            3.0,
        )
        + 0.45
        * positive_eras
        + 0.25
        * min(
            max(
                min_era_pf,
                0.0,
            ),
            2.5,
        )
        + 0.15
        * min(
            full["trades"]
            / 100.0,
            2.0,
        )
    )

    row = {
        "config_id":
            cfg["config_id"],

        "family":
            cfg["family"],

        "context":
            cfg.get(
                "context",
                "NONE",
            ),

        "rr":
            cfg["rr"],

        "full_trades":
            full["trades"],

        "full_pf":
            round(
                full["profit_factor"],
                6,
            ),

        "full_r":
            round(
                full["total_r"],
                4,
            ),

        "full_exp":
            round(
                full["expectancy_r"],
                6,
            ),

        "full_dd":
            round(
                full["max_drawdown_r"],
                4,
            ),

        "pre2010_trades":
            pre["trades"],

        "pre2010_pf":
            round(
                pre["profit_factor"],
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
                post["profit_factor"],
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

    for i, stats in enumerate(
        era_stats,
        1,
    ):
        row[
            f"era{i}_trades"
        ] = stats["trades"]

        row[
            f"era{i}_pf"
        ] = round(
            stats["profit_factor"],
            6,
        )

        row[
            f"era{i}_r"
        ] = round(
            stats["total_r"],
            4,
        )

    row.update(
        config_fields(cfg)
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
# STAGE 2
# ============================================================

def build_stage2(
    stage1_by_id,
    top_rows,
):
    configs = []

    for rank, row in enumerate(
        top_rows[
            :STAGE2_BASE_KEEP
        ]
    ):
        base = deepcopy(
            stage1_by_id[
                row["config_id"]
            ]
        )

        for context in CONTEXTS:
            cfg = deepcopy(base)

            cfg["config_id"] = (
                f"S2_{rank:02d}_"
                f"{context}"
            )

            cfg["context"] = context

            configs.append(cfg)

    return configs


# ============================================================
# STAGE 3
# ============================================================

def local_variants(
    base,
    rank,
):
    configs = []

    for rr in [
        2.50,
        2.75,
        3.00,
        3.25,
        3.50,
        3.75,
        4.00,
        4.25,
        4.50,
    ]:
        cfg = deepcopy(base)
        cfg["rr"] = rr

        cfg["config_id"] = (
            f"S3_{rank:02d}_"
            f"RR_{rr:.2f}"
        )

        configs.append(cfg)

    perturbations = {
        "br_min":
            [-0.20, -0.10, 0.10, 0.20],

        "body_atr_min":
            [-0.25, -0.10, 0.10, 0.25],

        "range_atr_min":
            [-0.20, -0.10, 0.10, 0.20],

        "close_loc_max":
            [-0.10, -0.05, 0.05, 0.10],

        "upper_wick_body_min":
            [-0.10, -0.05, 0.05, 0.10],

        "structure_dist_atr_max":
            [-0.05, -0.025, 0.025, 0.05],

        "compression_max":
            [-0.05, -0.025, 0.025, 0.05],

        "mom4_min":
            [-0.50, -0.25, 0.25, 0.50],
    }

    for field, deltas in perturbations.items():
        value = base.get(field)

        if value is None:
            continue

        for delta in deltas:
            new_value = round(
                value + delta,
                4,
            )

            if (
                field
                not in (
                    "close_loc_max",
                    "mom4_min",
                )
                and new_value <= 0
            ):
                continue

            if (
                field == "close_loc_max"
                and not (
                    0 < new_value < 1
                )
            ):
                continue

            if (
                field == "mom4_min"
                and new_value <= 0
            ):
                continue

            cfg = deepcopy(base)

            cfg[field] = new_value

            cfg["config_id"] = (
                f"S3_{rank:02d}_"
                f"{field}_{new_value}"
            )

            configs.append(cfg)

    # Structural lookback neighbours where relevant.
    if base.get("structure_lb") is not None:
        for lookback in [
            40,
            60,
            80,
            100,
            120,
            165,
            200,
        ]:
            cfg = deepcopy(base)
            cfg["structure_lb"] = lookback
            cfg["config_id"] = (
                f"S3_{rank:02d}_"
                f"structure_lb_{lookback}"
            )
            configs.append(cfg)

    if base.get("sweep_lb") is not None:
        for lookback in [
            20,
            40,
            60,
            80,
            100,
            120,
            165,
        ]:
            cfg = deepcopy(base)
            cfg["sweep_lb"] = lookback
            cfg["config_id"] = (
                f"S3_{rank:02d}_"
                f"sweep_lb_{lookback}"
            )
            configs.append(cfg)

    if base.get("breakdown_lb") is not None:
        for lookback in [
            5,
            10,
            15,
            20,
            30,
            40,
        ]:
            cfg = deepcopy(base)
            cfg["breakdown_lb"] = lookback
            cfg["config_id"] = (
                f"S3_{rank:02d}_"
                f"breakdown_lb_{lookback}"
            )
            configs.append(cfg)

    return configs


def build_stage3(
    stage2_by_id,
    top_rows,
):
    configs = []
    seen = set()

    for rank, row in enumerate(
        top_rows[
            :STAGE3_BASE_KEEP
        ]
    ):
        base = deepcopy(
            stage2_by_id[
                row["config_id"]
            ]
        )

        for cfg in local_variants(
            base,
            rank,
        ):
            signature = tuple(
                str(
                    cfg.get(field)
                )
                for field in [
                    "family",
                    "br_min",
                    "body_atr_min",
                    "range_atr_min",
                    "close_loc_max",
                    "upper_wick_body_min",
                    "structure_lb",
                    "structure_dist_atr_max",
                    "sweep_lb",
                    "breakdown_lb",
                    "compression_max",
                    "mom4_min",
                    "context",
                    "rr",
                ]
            )

            if signature in seen:
                continue

            seen.add(signature)
            configs.append(cfg)

    return configs


# ============================================================
# DEEP ROBUSTNESS
# ============================================================

def stats_row(
    cfg,
    label,
    trades,
):
    stats = stats_from_trades(
        trades
    )

    return {
        "config_id":
            cfg["config_id"],

        "family":
            cfg["family"],

        "context":
            cfg.get(
                "context",
                "NONE",
            ),

        "rr":
            cfg["rr"],

        "period":
            label,

        "trades":
            stats["trades"],

        "winners":
            stats["winners"],

        "losers":
            stats["losers"],

        "win_rate":
            round(
                stats["win_rate"],
                4,
            ),

        "profit_factor":
            round(
                stats["profit_factor"],
                6,
            ),

        "total_r":
            round(
                stats["total_r"],
                4,
            ),

        "expectancy_r":
            round(
                stats["expectancy_r"],
                6,
            ),

        "max_drawdown_r":
            round(
                stats["max_drawdown_r"],
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

        row["start_utc"] = iso_utc(start)
        row["end_utc"] = iso_utc(end)

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

            row["cost_pips"] = cost
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

    last_month = month_floor(NOW)

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
                    stats["trades"],

                "profit_factor":
                    round(
                        stats["profit_factor"],
                        6,
                    ),

                "total_r":
                    round(
                        stats["total_r"],
                        4,
                    ),

                "positive":
                    stats["total_r"] > 0,

                "zero_trade":
                    stats["trades"] == 0,
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
                        row["total_r"]
                        for row in active
                    ]),
                    4,
                ),

            "median_pf_active":
                round(
                    safe_median([
                        row["profit_factor"]
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
            year, 1, 1,
            tzinfo=timezone.utc,
        )

        end = datetime(
            year + 1, 1, 1,
            tzinfo=timezone.utc,
        )

        stats = stats_from_trades(
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
                stats["trades"],

            "profit_factor":
                round(
                    stats["profit_factor"],
                    6,
                ),

            "total_r":
                round(
                    stats["total_r"],
                    4,
                ),

            "positive":
                stats["total_r"] > 0,

            "negative":
                stats["total_r"] < 0,

            "zero_trade":
                stats["trades"] == 0,
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
# ABLATION / PLATEAU
# ============================================================

def ablation_configs(finalist):
    configs = []

    if finalist.get(
        "context",
        "NONE",
    ) != "NONE":
        cfg = deepcopy(finalist)

        cfg["config_id"] = (
            finalist["config_id"]
            + "_ABLATE_CONTEXT"
        )

        cfg["context"] = "NONE"

        configs.append(
            (
                "REMOVE_CONTEXT",
                cfg,
            )
        )

    removable = [
        "br_min",
        "body_atr_min",
        "range_atr_min",
        "close_loc_max",
        "upper_wick_body_min",
        "mom4_min",
    ]

    for field in removable:
        if finalist.get(field) is None:
            continue

        cfg = deepcopy(finalist)

        cfg["config_id"] = (
            finalist["config_id"]
            + "_ABLATE_"
            + field
        )

        cfg[field] = None

        configs.append(
            (
                "REMOVE_" + field,
                cfg,
            )
        )

    return configs


def plateau_configs(finalist):
    configs = []

    for cfg in local_variants(
        finalist,
        99,
    ):
        cfg["config_id"] = (
            finalist["config_id"]
            + "_PLATEAU_"
            + cfg["config_id"]
        )

        configs.append(cfg)

    return configs


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
                "Missing required USD_JPY history"
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

                "h4_candles":
                    len(h4),

                "daily_candles":
                    len(daily),
            }],
        )

        STATUS.update({
            "state":
                "precomputing",

            "message":
                "Building completed H1/H4/D no-lookahead state",
        })

        m15_times = [
            candle["time"]
            for candle in m15
        ]

        h1_aligned = align_htf_to_m15(
            m15_times,
            build_htf_state(h1),
        )

        h4_aligned = align_htf_to_m15(
            m15_times,
            build_htf_state(h4),
        )

        daily_aligned = align_htf_to_m15(
            m15_times,
            build_htf_state(daily),
        )

        STATUS.update({
            "state":
                "precomputing",

            "message":
                "Building USD/JPY M15 SHORT feature cache",
        })

        features = build_features(
            m15,
            h1_aligned,
            h4_aligned,
            daily_aligned,
        )

        # ----------------------------------------------------
        # STAGE 1
        # ----------------------------------------------------
        stage1_configs = (
            build_stage1_configs()
        )

        stage1_by_id = {
            cfg["config_id"]:
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
                    f"Stage 1 "
                    f"{i}/{len(stage1_configs)} "
                    f"{cfg['config_id']}"
                ),
            })

            indices = signal_indices(
                cfg,
                features,
            )

            stage1_rows.append(
                evaluation_row(
                    cfg,
                    m15,
                    indices,
                )
            )

        stage1_rows = sort_rows(
            stage1_rows
        )

        write_csv(
            OUT_STAGE1,
            stage1_rows,
        )

        top_stage1 = [
            row
            for row in stage1_rows
            if row[
                "full_trades"
            ] >= MIN_STAGE1_TRADES
        ][
            :STAGE1_KEEP
        ]

        if len(top_stage1) < STAGE1_KEEP:
            top_stage1 = (
                stage1_rows[
                    :STAGE1_KEEP
                ]
            )

        # ----------------------------------------------------
        # STAGE 2
        # ----------------------------------------------------
        stage2_configs = build_stage2(
            stage1_by_id,
            top_stage1,
        )

        stage2_by_id = {
            cfg["config_id"]:
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
                    f"Stage 2 "
                    f"{i}/{len(stage2_configs)} "
                    f"{cfg['config_id']}"
                ),
            })

            indices = signal_indices(
                cfg,
                features,
            )

            stage2_rows.append(
                evaluation_row(
                    cfg,
                    m15,
                    indices,
                )
            )

        stage2_rows = sort_rows(
            stage2_rows
        )

        write_csv(
            OUT_STAGE2,
            stage2_rows,
        )

        top_stage2 = [
            row
            for row in stage2_rows
            if row[
                "full_trades"
            ] >= MIN_STAGE1_TRADES
        ][
            :STAGE2_KEEP
        ]

        if len(top_stage2) < STAGE2_KEEP:
            top_stage2 = (
                stage2_rows[
                    :STAGE2_KEEP
                ]
            )

        # ----------------------------------------------------
        # STAGE 3
        # ----------------------------------------------------
        stage3_configs = build_stage3(
            stage2_by_id,
            top_stage2,
        )

        stage3_by_id = {
            cfg["config_id"]:
                cfg
            for cfg in stage3_configs
        }

        stage3_rows = []

        for i, cfg in enumerate(
            stage3_configs,
            1,
        ):
            STATUS.update({
                "state":
                    "stage3",

                "message": (
                    f"Stage 3 "
                    f"{i}/{len(stage3_configs)} "
                    f"{cfg['config_id']}"
                ),
            })

            indices = signal_indices(
                cfg,
                features,
            )

            stage3_rows.append(
                evaluation_row(
                    cfg,
                    m15,
                    indices,
                )
            )

        stage3_rows = sort_rows(
            stage3_rows
        )

        write_csv(
            OUT_STAGE3,
            stage3_rows,
        )

        # ----------------------------------------------------
        # FINALIST SELECTION
        # ----------------------------------------------------
        eligible = [
            row
            for row in stage3_rows
            if (
                row["full_trades"]
                >= MIN_FINAL_TRADES
                and row["pre2010_r"] > 0
                and row["post2010_r"] > 0
                and row["positive_eras"] == 4
            )
        ]

        finalist_rows = (
            eligible[:FINALIST_KEEP]
        )

        if len(
            finalist_rows
        ) < FINALIST_KEEP:
            selected = {
                row["config_id"]
                for row in finalist_rows
            }

            for row in stage3_rows:
                if row["config_id"] in selected:
                    continue

                finalist_rows.append(row)
                selected.add(
                    row["config_id"]
                )

                if (
                    len(finalist_rows)
                    >= FINALIST_KEEP
                ):
                    break

        write_csv(
            OUT_FINALISTS,
            finalist_rows,
        )

        finalist_configs = []

        for row in finalist_rows:
            cfg = stage3_by_id.get(
                row["config_id"]
            )

            if cfg is not None:
                finalist_configs.append(cfg)

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
                    "final_validation",

                "message": (
                    f"Final validation "
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
                cfg["rr"],
                PRIMARY_COST_PIPS,
                m15[0]["time"],
                NOW,
            )

            for trade in full_trades:
                row = dict(trade)

                row["config_id"] = (
                    cfg["config_id"]
                )

                row["family"] = (
                    cfg["family"]
                )

                row["context"] = (
                    cfg.get(
                        "context",
                        "NONE",
                    )
                )

                trade_output.append(row)

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

        # ----------------------------------------------------
        # ABLATION + PLATEAU ON TOP FINALIST
        # ----------------------------------------------------
        top_finalist = (
            finalist_configs[0]
            if finalist_configs
            else None
        )

        ablation_rows = []
        plateau_rows = []

        if top_finalist is not None:
            for label, cfg in ablation_configs(
                top_finalist
            ):
                try:
                    indices = signal_indices(
                        cfg,
                        features,
                    )

                    row = evaluation_row(
                        cfg,
                        m15,
                        indices,
                    )

                    row["ablation"] = label
                    ablation_rows.append(row)

                except Exception:
                    pass

            seen = set()

            for cfg in plateau_configs(
                top_finalist
            ):
                signature = tuple(
                    str(
                        cfg.get(field)
                    )
                    for field in [
                        "family",
                        "br_min",
                        "body_atr_min",
                        "range_atr_min",
                        "close_loc_max",
                        "upper_wick_body_min",
                        "structure_lb",
                        "structure_dist_atr_max",
                        "sweep_lb",
                        "breakdown_lb",
                        "compression_max",
                        "mom4_min",
                        "context",
                        "rr",
                    ]
                )

                if signature in seen:
                    continue

                seen.add(signature)

                try:
                    indices = signal_indices(
                        cfg,
                        features,
                    )

                    plateau_rows.append(
                        evaluation_row(
                            cfg,
                            m15,
                            indices,
                        )
                    )

                except Exception:
                    pass

        write_csv(
            OUT_ABLATION,
            sort_rows(
                ablation_rows
            ),
        )

        write_csv(
            OUT_PLATEAU,
            sort_rows(
                plateau_rows
            ),
        )

        write_csv(
            OUT_NOTES,
            [{
                "item":
                    "Search type",

                "value":
                    "Fresh full-history USDJPY M15 SHORT research; no pre-existing M15 short treated as a core benchmark.",
            }, {
                "item":
                    "Cost convention",

                "value":
                    "1.0 pip adverse baseline; 0.5/1/1.5/2 pip stress.",
            }, {
                "item":
                    "No-lookahead",

                "value":
                    "H1/H4/D state available only after next actual HTF open; prior momentum ends at close[i-1].",
            }, {
                "item":
                    "Selection warning",

                "value":
                    "Do not lock automatically from headline PF. Require full/pre/post positivity, era stability, costs, rolling/calendar consistency, ablation and local plateau.",
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
                "USD/JPY M15 SHORT full-history re-examination complete",

            "m15_candles":
                len(m15),

            "stage1_configs":
                len(stage1_configs),

            "stage2_configs":
                len(stage2_configs),

            "stage3_configs":
                len(stage3_configs),

            "finalists":
                len(finalist_configs),

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
            "USDJPY M15 SHORT Full-History Re-examination",

        "status":
            STATUS["state"],

        "instrument":
            PAIR,

        "timeframe":
            "M15",

        "side":
            "SELL",

        "requested_start_utc":
            iso_utc(START),

        "tick_size":
            TICK_SIZE,

        "pip_size":
            PIP_SIZE,

        "primary_cost_pips":
            PRIMARY_COST_PIPS,

        "families": [
            "BEAR_ENGULF_STRUCTURE",
            "HIGH_SWEEP_DISPLACEMENT",
            "FAILED_BREAKOUT_REJECTION",
            "BEAR_OUTSIDE_REVERSAL",
            "COMPRESSION_BREAKDOWN",
            "BLOWOFF_REJECTION",
        ],

        "orders_supported":
            False,

        "trading_enabled":
            False,

        "routes": [
            "/usdjpy-m15-short-full-history/status",
            "/usdjpy-m15-short-full-history/results",
        ],
    })


@app.route(
    "/usdjpy-m15-short-full-history/status"
)
def route_status():
    return jsonify(STATUS)


@app.route(
    "/usdjpy-m15-short-full-history/results"
)
def route_results():
    if not os.path.exists(OUT_BUNDLE):
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
            "full-history"
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
