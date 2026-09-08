
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
# EUR/USD M15 SHORT — FULL-HISTORY RE-EXAMINATION
#
# Research universe:
#   requested OANDA history from:
#       2002-05-06 20:00 UTC
#   through present / earliest available.
#
# CURRENT LOCKED BENCHMARK
# ------------------------
# exact bearish engulf
# body ratio >= 1.00
# body >= 1.00 ATR14
# range >= 1.70 ATR14
# previous 60-bar high structure
# signal high within 0.30 ATR14 of prior 60-bar high
# signal-open NY hours {02,03,04}
# exclude Thursday
# RR 3.75
# stop = signal high + 10 ticks
#
# This benchmark is retained unchanged for comparison.
#
# FULL-HISTORY SHORT FAMILIES
# ---------------------------
#   1) BEAR_ENGULF_STRUCTURE
#   2) HIGH_SWEEP_DISPLACEMENT
#   3) FAILED_BREAKOUT_REJECTION
#   4) BEAR_OUTSIDE_REVERSAL
#   5) COMPRESSION_BREAKDOWN
#   6) BLOWOFF_REJECTION
#
# STAGED PROCESS
# --------------
# STAGE 1:
#   broad but controlled raw geometry templates.
#
# STAGE 2:
#   broad context hypotheses on survivors:
#     H1/H4/D bearish trend
#     H1/H4/D volatility
#     broad NY blocks
#     weekday exclusions
#
# STAGE 3:
#   local one-at-a-time geometry perturbations
#   + RR 3.00 / 3.25 / 3.50 / 3.75 / 4.00 / 4.25
#
# FINAL ROBUSTNESS:
#   - full history
#   - pre-2010
#   - 2010+
#   - 2002-07
#   - 2008-13
#   - 2014-19
#   - 2020-now
#   - 2002-17 vs 2018+
#   - recent 2Y / 5Y
#   - 0.5 / 1 / 1.5 / 2 pip adverse costs
#   - rolling 12 / 24 / 36M
#   - completed calendar years
#   - context ablation
#   - local plateau
#
# HISTORICAL M15 CONVENTIONS
# --------------------------
# OANDA midpoint.
# ATR14 = Wilder/RMA, SMA seeded.
# Signal timestamp = M15 candle OPEN.
# Historical baseline adverse fill = 1.0 pip.
#
# SHORT:
#   reference entry = signal close
#   historical fill = signal close - adverse cost
#   stop = signal high + 10 ticks
#   target based on REFERENCE signal-close risk
#
# Exit begins next candle.
# Pyramiding = 0.
# Exact exit-candle signal eligible.
#
# SHORT SAME-BAR TIE:
#   if high is closer to candle open => STOP first
#   otherwise TARGET first.
#
# HIGHER-TIMEFRAME NO LOOKAHEAD
# -----------------------------
# H1/H4/D:
#   complete_at = next ACTUAL HTF candle OPEN
#   lookup = bisect_right(completion_times, signal_open_time) - 1
#
# Last HTF row without a next-open completion marker is never exposed.
#
# DAILY:
#   OANDA D
#   dailyAlignment=17
#   alignmentTimezone=America/New_York
#
# ONE ZIP:
#   /eurusd-m15-short-full-history/results
#
# READ ONLY. NEVER SENDS ORDERS.
# ============================================================


app = Flask(__name__)

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_BASE = os.getenv(
    "OANDA_API_URL",
    "https://api-fxtrade.oanda.com",
)

INSTRUMENT = "EUR_USD"

REQUESTED_FROM = datetime(
    2002, 5, 6, 20, 0,
    tzinfo=timezone.utc,
)

RESEARCH_TO = (
    datetime.now(timezone.utc)
    .replace(second=0, microsecond=0)
)

HTF_WARMUP_FROM = (
    REQUESTED_FROM
    - timedelta(days=900)
)

NY = ZoneInfo("America/New_York")

TICK_SIZE = 0.00001
PIP_SIZE = 0.0001
STOP_BUFFER_TICKS = 10

PRIMARY_COST_PIPS = 1.00
COST_GRID = [0.50, 1.00, 1.50, 2.00]

STAGE1_KEEP = 14
STAGE2_BASE_KEEP = 8
STAGE2_KEEP = 10
STAGE3_BASE_KEEP = 5
FINALIST_KEEP = 10

MIN_STAGE1_TRADES = 50
MIN_FINAL_TRADES = 60

STAGE1_RR = 3.50


# ============================================================
# FROZEN EXISTING BENCHMARK
# ============================================================

BENCHMARK = {
    "config_id":
        "BENCHMARK_LOCKED_2010_PLUS",

    "family":
        "BEAR_ENGULF_STRUCTURE",

    "rr":
        3.75,

    "require_exact_bear_engulf":
        True,

    "br_min":
        1.00,

    "body_atr_min":
        1.00,

    "range_atr_min":
        1.70,

    "close_loc_max":
        None,

    "upper_wick_body_min":
        None,

    "structure_lb":
        60,

    "structure_dist_atr_max":
        0.30,

    "sweep_lb":
        None,

    "breakdown_lb":
        None,

    "compression_max":
        None,

    "mom4_min":
        None,

    "context":
        "BENCHMARK_TIME",

    "included_ny_hours":
        {2, 3, 4},

    "excluded_ny_hours":
        set(),

    "excluded_weekdays":
        {3},  # Thursday
}


# ============================================================
# OUTPUTS
# ============================================================

OUTPUT_COVERAGE = (
    "eurusd_m15_short_full_history_coverage.csv"
)

OUTPUT_BENCHMARK = (
    "eurusd_m15_short_full_history_benchmark.csv"
)

OUTPUT_STAGE1 = (
    "eurusd_m15_short_full_history_stage1_raw.csv"
)

OUTPUT_STAGE2 = (
    "eurusd_m15_short_full_history_stage2_context.csv"
)

OUTPUT_STAGE3 = (
    "eurusd_m15_short_full_history_stage3_local_rr.csv"
)

OUTPUT_FINAL = (
    "eurusd_m15_short_full_history_finalists.csv"
)

OUTPUT_PERIODS = (
    "eurusd_m15_short_full_history_periods.csv"
)

OUTPUT_COST = (
    "eurusd_m15_short_full_history_cost_stress.csv"
)

OUTPUT_ROLLING = (
    "eurusd_m15_short_full_history_rolling.csv"
)

OUTPUT_ROLLING_SUMMARY = (
    "eurusd_m15_short_full_history_rolling_summary.csv"
)

OUTPUT_CALENDAR = (
    "eurusd_m15_short_full_history_calendar_years.csv"
)

OUTPUT_CALENDAR_SUMMARY = (
    "eurusd_m15_short_full_history_calendar_summary.csv"
)

OUTPUT_ABLATION = (
    "eurusd_m15_short_full_history_ablation.csv"
)

OUTPUT_PLATEAU = (
    "eurusd_m15_short_full_history_plateau.csv"
)

OUTPUT_TRADES = (
    "eurusd_m15_short_full_history_finalist_trades.csv"
)

OUTPUT_NOTES = (
    "eurusd_m15_short_full_history_notes.csv"
)

OUTPUT_BUNDLE = (
    "EURUSD_M15_SHORT_FULL_HISTORY_REEXAMINATION_RESULTS.zip"
)

STATUS = {
    "state":
        "not_started",

    "message":
        "EUR/USD M15 SHORT full-history re-examination not started",

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


def download_file(path):
    if not os.path.exists(path):
        return jsonify({
            "error":
                "Results not ready yet",
        }), 404

    return send_file(
        os.path.abspath(path),
        as_attachment=True,
        download_name=os.path.basename(path),
    )


def build_bundle():
    files = [
        OUTPUT_COVERAGE,
        OUTPUT_BENCHMARK,
        OUTPUT_STAGE1,
        OUTPUT_STAGE2,
        OUTPUT_STAGE3,
        OUTPUT_FINAL,
        OUTPUT_PERIODS,
        OUTPUT_COST,
        OUTPUT_ROLLING,
        OUTPUT_ROLLING_SUMMARY,
        OUTPUT_CALENDAR,
        OUTPUT_CALENDAR_SUMMARY,
        OUTPUT_ABLATION,
        OUTPUT_PLATEAU,
        OUTPUT_TRADES,
        OUTPUT_NOTES,
    ]

    with zipfile.ZipFile(
        OUTPUT_BUNDLE,
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

    if not values:
        return 0.0

    return median(values)


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


def month_floor(dt):
    return datetime(
        dt.year,
        dt.month,
        1,
        tzinfo=timezone.utc,
    )


# ============================================================
# OANDA
# ============================================================

def oanda_headers():
    if not OANDA_TOKEN:
        raise RuntimeError(
            "OANDA_TOKEN is not configured"
        )

    return {
        "Authorization":
            "Bearer "
            + OANDA_TOKEN.strip(),
    }


def fetch_chunk(
    granularity,
    start,
    end,
):
    url = (
        f"{OANDA_BASE}/v3/instruments/"
        f"{INSTRUMENT}/candles"
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
    chunk_number = 0

    while cursor < end:
        chunk_number += 1

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
                f"chunk {chunk_number}: "
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
    ] = np.mean(seed)

    for i in range(
        length,
        len(values),
    ):
        if (
            np.isfinite(values[i])
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


def ema_list(values, length):
    result = [
        None
    ] * len(values)

    if len(values) < length:
        return result

    seed = values[:length]

    result[
        length - 1
    ] = (
        sum(seed)
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
            alpha * values[i]
            + (
                1.0 - alpha
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
    """
    result[i] = min/max(values[i-lookback:i])
    Current candle excluded.
    """
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
# HTF COMPLETION
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

    atr_values = atr14(
        candles
    )

    atr_mean50 = sma_np(
        atr_values,
        50,
    )

    rows = []

    for i, candle in enumerate(
        candles
    ):
        complete_at = (
            candles[
                i + 1
            ]["time"]
            if i + 1
            < len(candles)
            else None
        )

        atr_ratio50 = None

        if (
            np.isfinite(
                atr_values[i]
            )
            and np.isfinite(
                atr_mean50[i]
            )
            and atr_mean50[i] > 0
        ):
            atr_ratio50 = (
                atr_values[i]
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

        row = eligible[
            position
        ]

        for key in keys:
            value = row[
                key
            ]

            if value is not None:
                result[
                    key
                ][i] = value

    return result


# ============================================================
# M15 FEATURE CACHE
# ============================================================

def build_feature_cache(
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

    exact_bear_engulf = (
        np.zeros(
            n,
            dtype=bool,
        )
    )

    exact_bear_engulf[
        1:
    ] = (
        (
            closes[
                :-1
            ]
            > opens[
                :-1
            ]
        )
        & (
            closes[
                1:
            ]
            < opens[
                1:
            ]
        )
        & (
            opens[
                1:
            ]
            >= closes[
                :-1
            ]
        )
        & (
            closes[
                1:
            ]
            <= opens[
                :-1
            ]
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
        bearish_body[
            valid_previous_body
        ]
        / previous_body[
            valid_previous_body
        ]
    )

    valid_atr = (
        np.isfinite(atr)
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

    positive_bear_body = (
        bearish_body > 0
    )

    upper_wick_body[
        positive_bear_body
    ] = (
        upper_wick[
            positive_bear_body
        ]
        / bearish_body[
            positive_bear_body
        ]
    )

    compression = np.full(
        n,
        np.nan,
        dtype=float,
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

    lookbacks = [
        10,
        20,
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

    # Strictly prior 4h momentum:
    # previous completed M15 close vs 16 bars before that.
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

    ny_hour = np.zeros(
        n,
        dtype=np.int16,
    )

    ny_weekday = np.zeros(
        n,
        dtype=np.int16,
    )

    for i, timestamp in enumerate(
        times
    ):
        local = timestamp.astimezone(
            NY
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

        "mom4":
            mom4,

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
# CONFIG BUILDERS
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

        "require_exact_bear_engulf":
            False,

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

        "context":
            "NONE",

        "included_ny_hours":
            None,

        "excluded_ny_hours":
            set(),

        "excluded_weekdays":
            set(),
    }


def build_stage1_configs():
    configs = []

    # --------------------------------------------------------
    # 1) BEAR ENGULF + STRUCTURE
    # --------------------------------------------------------
    templates = [
        (1.00, 0.75, 1.40, 40, 0.20),
        (1.00, 1.00, 1.50, 60, 0.20),
        (1.00, 1.00, 1.70, 60, 0.30),
        (1.10, 0.75, 1.50, 60, 0.20),
        (1.10, 1.00, 1.70, 60, 0.25),
        (1.20, 0.75, 1.50, 80, 0.20),
        (1.20, 1.00, 1.70, 80, 0.25),
        (1.20, 1.20, 1.80, 100, 0.20),
        (1.35, 1.00, 1.70, 100, 0.20),
        (1.35, 1.20, 1.80, 120, 0.20),
        (1.50, 1.00, 1.80, 120, 0.15),
        (1.50, 1.20, 1.90, 165, 0.15),
    ]

    for i, (
        br,
        body,
        range_atr,
        lookback,
        distance,
    ) in enumerate(
        templates
    ):
        cfg = base_config(
            f"S1_ENG_{i:02d}",
            "BEAR_ENGULF_STRUCTURE",
        )

        cfg.update({
            "require_exact_bear_engulf":
                True,

            "br_min":
                br,

            "body_atr_min":
                body,

            "range_atr_min":
                range_atr,

            "structure_lb":
                lookback,

            "structure_dist_atr_max":
                distance,
        })

        configs.append(cfg)

    # --------------------------------------------------------
    # 2) HIGH SWEEP + DISPLACEMENT
    # --------------------------------------------------------
    templates = [
        (20, 0.75, 0.15, 0.50),
        (20, 1.00, 0.25, 1.00),
        (40, 0.75, 0.25, 0.50),
        (40, 1.00, 0.25, 1.00),
        (40, 1.25, 0.25, 1.25),
        (60, 0.75, 0.25, 0.75),
        (60, 1.00, 0.35, 1.00),
        (60, 1.25, 0.25, 1.50),
        (100, 1.00, 0.35, 1.25),
        (100, 1.25, 0.35, 1.50),
        (100, 1.50, 0.25, 1.75),
    ]

    for i, (
        sweep_lb,
        body,
        wick,
        mom4,
    ) in enumerate(
        templates
    ):
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
        })

        configs.append(cfg)

    # --------------------------------------------------------
    # 3) FAILED BREAKOUT + REJECTION
    # --------------------------------------------------------
    templates = [
        (20, 0.50, 0.40),
        (20, 0.75, 0.30),
        (40, 0.50, 0.40),
        (40, 0.75, 0.30),
        (40, 1.00, 0.25),
        (60, 0.50, 0.35),
        (60, 0.75, 0.30),
        (60, 1.00, 0.25),
        (100, 0.75, 0.30),
        (100, 1.00, 0.20),
        (165, 1.00, 0.25),
    ]

    for i, (
        sweep_lb,
        body,
        close_loc,
    ) in enumerate(
        templates
    ):
        cfg = base_config(
            f"S1_FAIL_{i:02d}",
            "FAILED_BREAKOUT_REJECTION",
        )

        cfg.update({
            "sweep_lb":
                sweep_lb,

            "body_atr_min":
                body,

            "close_loc_max":
                close_loc,
        })

        configs.append(cfg)

    # --------------------------------------------------------
    # 4) BEARISH OUTSIDE REVERSAL
    # --------------------------------------------------------
    templates = [
        (0.50, 0.40, 40, 0.20),
        (0.75, 0.35, 40, 0.15),
        (0.75, 0.30, 60, 0.20),
        (1.00, 0.35, 60, 0.15),
        (1.00, 0.25, 80, 0.20),
        (1.25, 0.30, 80, 0.15),
        (1.25, 0.25, 100, 0.20),
        (1.50, 0.25, 100, 0.15),
        (1.00, 0.20, 120, 0.20),
    ]

    for i, (
        body,
        close_loc,
        lookback,
        distance,
    ) in enumerate(
        templates
    ):
        cfg = base_config(
            f"S1_OUT_{i:02d}",
            "BEAR_OUTSIDE_REVERSAL",
        )

        cfg.update({
            "body_atr_min":
                body,

            "close_loc_max":
                close_loc,

            "structure_lb":
                lookback,

            "structure_dist_atr_max":
                distance,
        })

        configs.append(cfg)

    # --------------------------------------------------------
    # 5) COMPRESSION BREAKDOWN
    # --------------------------------------------------------
    templates = [
        (0.60, 0.75, 1.20, 10),
        (0.65, 0.75, 1.30, 10),
        (0.65, 1.00, 1.40, 10),
        (0.70, 0.75, 1.30, 10),
        (0.70, 1.00, 1.40, 10),
        (0.70, 1.25, 1.50, 10),
        (0.75, 0.75, 1.30, 10),
        (0.75, 1.00, 1.40, 10),
        (0.75, 1.25, 1.50, 20),
        (0.80, 1.00, 1.50, 20),
        (0.80, 1.25, 1.60, 20),
    ]

    for i, (
        compression,
        body,
        range_atr,
        breakdown_lb,
    ) in enumerate(
        templates
    ):
        cfg = base_config(
            f"S1_COMP_{i:02d}",
            "COMPRESSION_BREAKDOWN",
        )

        cfg.update({
            "compression_max":
                compression,

            "body_atr_min":
                body,

            "range_atr_min":
                range_atr,

            "breakdown_lb":
                breakdown_lb,
        })

        configs.append(cfg)

    # --------------------------------------------------------
    # 6) BLOWOFF + REJECTION
    # --------------------------------------------------------
    templates = [
        (20, 0.50, 0.75, 0.35),
        (20, 0.75, 1.00, 0.30),
        (40, 0.50, 1.00, 0.35),
        (40, 0.75, 1.25, 0.30),
        (40, 1.00, 1.50, 0.25),
        (60, 0.75, 1.25, 0.30),
        (60, 1.00, 1.50, 0.25),
        (100, 1.00, 1.50, 0.25),
        (100, 1.25, 1.75, 0.20),
    ]

    for i, (
        sweep_lb,
        body,
        mom4,
        close_loc,
    ) in enumerate(
        templates
    ):
        cfg = base_config(
            f"S1_BLOW_{i:02d}",
            "BLOWOFF_REJECTION",
        )

        cfg.update({
            "sweep_lb":
                sweep_lb,

            "body_atr_min":
                body,

            "mom4_min":
                mom4,

            "close_loc_max":
                close_loc,
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


def apply_context_mask(
    mask,
    config,
    f,
):
    context = config.get(
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

    elif context.startswith(
        "NY_BLOCK_"
    ):
        block = context.split(
            "_"
        )[-1]

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
            context.split(
                "_"
            )[-1]
        )

        mask &= (
            f["ny_weekday"]
            != weekday
        )

    included = config.get(
        "included_ny_hours"
    )

    if included is not None:
        allowed = np.zeros(
            f["n"],
            dtype=bool,
        )

        for hour in included:
            allowed |= (
                f["ny_hour"]
                == hour
            )

        mask &= allowed

    for hour in config.get(
        "excluded_ny_hours",
        set(),
    ):
        mask &= (
            f["ny_hour"]
            != hour
        )

    for weekday in config.get(
        "excluded_weekdays",
        set(),
    ):
        mask &= (
            f["ny_weekday"]
            != weekday
        )

    return mask


# ============================================================
# SIGNAL MASK
# ============================================================

def signal_indices_for_config(
    config,
    f,
):
    mask = (
        f["valid_atr"].copy()
        & f["bearish"]
    )

    family = config[
        "family"
    ]

    if family == "BEAR_ENGULF_STRUCTURE":
        if config.get(
            "require_exact_bear_engulf",
            False,
        ):
            mask &= (
                f[
                    "exact_bear_engulf"
                ]
            )

        if config.get(
            "br_min"
        ) is not None:
            mask &= (
                f["body_ratio"]
                >= config[
                    "br_min"
                ]
            )

        if config.get(
            "body_atr_min"
        ) is not None:
            mask &= (
                f["body_atr"]
                >= config[
                    "body_atr_min"
                ]
            )

        if config.get(
            "range_atr_min"
        ) is not None:
            mask &= (
                f["range_atr"]
                >= config[
                    "range_atr_min"
                ]
            )

        mask &= (
            f[
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

    elif family == "HIGH_SWEEP_DISPLACEMENT":
        lookback = config[
            "sweep_lb"
        ]

        mask &= (
            f["high"]
            > f[
                "previous_high"
            ][
                lookback
            ]
        )

        previous_low = np.roll(
            f["low"],
            1,
        )

        mask[0] = False

        mask &= (
            f["close"]
            < previous_low
        )

        mask &= (
            f["body_atr"]
            >= config[
                "body_atr_min"
            ]
        )

        mask &= (
            f[
                "upper_wick_body"
            ]
            >= config[
                "upper_wick_body_min"
            ]
        )

        mask &= (
            f["mom4"]
            >= config[
                "mom4_min"
            ]
        )

    elif family == "FAILED_BREAKOUT_REJECTION":
        lookback = config[
            "sweep_lb"
        ]

        previous_high = (
            f[
                "previous_high"
            ][
                lookback
            ]
        )

        mask &= (
            f["high"]
            > previous_high
        )

        mask &= (
            f["close"]
            < previous_high
        )

        mask &= (
            f["body_atr"]
            >= config[
                "body_atr_min"
            ]
        )

        mask &= (
            f["close_loc"]
            <= config[
                "close_loc_max"
            ]
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
            >= config[
                "body_atr_min"
            ]
        )

        mask &= (
            f["close_loc"]
            <= config[
                "close_loc_max"
            ]
        )

        mask &= (
            f[
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

    elif family == "COMPRESSION_BREAKDOWN":
        mask &= (
            f["compression"]
            <= config[
                "compression_max"
            ]
        )

        mask &= (
            f["body_atr"]
            >= config[
                "body_atr_min"
            ]
        )

        mask &= (
            f["range_atr"]
            >= config[
                "range_atr_min"
            ]
        )

        mask &= (
            f["close"]
            < f[
                "previous_low"
            ][
                config[
                    "breakdown_lb"
                ]
            ]
        )

    elif family == "BLOWOFF_REJECTION":
        lookback = config[
            "sweep_lb"
        ]

        previous_high = (
            f[
                "previous_high"
            ][
                lookback
            ]
        )

        mask &= (
            f["high"]
            > previous_high
        )

        mask &= (
            f["close"]
            < f[
                "previous_high"
            ][10]
        )

        mask &= (
            f["body_atr"]
            >= config[
                "body_atr_min"
            ]
        )

        mask &= (
            f["mom4"]
            >= config[
                "mom4_min"
            ]
        )

        mask &= (
            f["close_loc"]
            <= config[
                "close_loc_max"
            ]
        )

    else:
        raise RuntimeError(
            f"Unknown family: {family}"
        )

    mask = apply_context_mask(
        mask,
        config,
        f,
    )

    mask[
        :200
    ] = False

    return np.flatnonzero(
        mask
    ).tolist()


# ============================================================
# HISTORICAL TRADE ENGINE
# ============================================================

OUTCOME_CACHE = {}


def compute_short_outcome(
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

    backtest_fill = (
        reference_entry
        - cost_pips
        * PIP_SIZE
    )

    actual_risk = (
        stop
        - backtest_fill
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

            # Short convention:
            # high closer => STOP first
            # otherwise TARGET first
            if (
                high_distance
                < low_distance
            ):
                exit_price = stop
                exit_reason = "STOP"

            else:
                exit_price = target
                exit_reason = "TARGET"

        elif hit_target:
            exit_price = target
            exit_reason = "TARGET"

        elif hit_stop:
            exit_price = stop
            exit_reason = "STOP"

        else:
            continue

        result_r = (
            backtest_fill
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
                exit_reason,

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
        ] = compute_short_outcome(
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
    signal_indices,
    rr,
    cost_pips,
    start=None,
    end=None,
):
    use = signal_indices

    if (
        start is not None
        or end is not None
    ):
        times = [
            candles[
                index
            ]["time"]
            for index in signal_indices
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
            len(signal_indices)
            if end is None
            else bisect.bisect_left(
                times,
                end,
            )
        )

        use = (
            signal_indices[
                left:right
            ]
        )

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
# STATS / EVALUATION
# ============================================================

def stats_from_trades(trades):
    results = [
        float(
            trade["result_r"]
        )
        for trade in trades
    ]

    winners = [
        value
        for value in results
        if value > 0
    ]

    losers = [
        value
        for value in results
        if value < 0
    ]

    gross_profit = sum(winners)
    gross_loss = abs(sum(losers))

    if gross_loss > 0:
        profit_factor = (
            gross_profit
            / gross_loss
        )

    elif gross_profit > 0:
        profit_factor = 999.0

    else:
        profit_factor = 0.0

    total_r = sum(results)

    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0

    streak = 0
    longest_streak = 0

    for result in results:
        equity += result

        peak = max(
            peak,
            equity,
        )

        max_drawdown = min(
            max_drawdown,
            equity - peak,
        )

        if result < 0:
            streak += 1

            longest_streak = max(
                longest_streak,
                streak,
            )

        else:
            streak = 0

    return {
        "trades":
            len(results),

        "winners":
            len(winners),

        "losers":
            len(losers),

        "win_rate":
            (
                len(winners)
                / len(results)
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
                / len(results)
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
        REQUESTED_FROM,
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
        RESEARCH_TO,
    ),
]


def config_fields(config):
    return {
        "br_min":
            config.get("br_min"),

        "body_atr_min":
            config.get(
                "body_atr_min"
            ),

        "range_atr_min":
            config.get(
                "range_atr_min"
            ),

        "close_loc_max":
            config.get(
                "close_loc_max"
            ),

        "upper_wick_body_min":
            config.get(
                "upper_wick_body_min"
            ),

        "structure_lb":
            config.get(
                "structure_lb"
            ),

        "structure_dist_atr_max":
            config.get(
                "structure_dist_atr_max"
            ),

        "sweep_lb":
            config.get(
                "sweep_lb"
            ),

        "breakdown_lb":
            config.get(
                "breakdown_lb"
            ),

        "compression_max":
            config.get(
                "compression_max"
            ),

        "mom4_min":
            config.get(
                "mom4_min"
            ),
    }


def evaluation_row(
    config,
    candles,
    signal_indices,
):
    full = stats_from_trades(
        run_backtest(
            candles,
            signal_indices,
            config["rr"],
            PRIMARY_COST_PIPS,
            candles[0]["time"],
            RESEARCH_TO,
        )
    )

    pre = stats_from_trades(
        run_backtest(
            candles,
            signal_indices,
            config["rr"],
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
            signal_indices,
            config["rr"],
            PRIMARY_COST_PIPS,
            datetime(
                2010, 1, 1,
                tzinfo=timezone.utc,
            ),
            RESEARCH_TO,
        )
    )

    era_stats = []

    for _, start, end in ERAS:
        era_stats.append(
            stats_from_trades(
                run_backtest(
                    candles,
                    signal_indices,
                    config["rr"],
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

    robust_score = (
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
            full["trades"]
            / 100.0,
            2.0,
        )
    )

    row = {
        "config_id":
            config["config_id"],

        "family":
            config["family"],

        "context":
            config.get(
                "context",
                "NONE",
            ),

        "rr":
            config["rr"],

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
                robust_score,
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

    row.update(
        config_fields(
            config
        )
    )

    return row


def sort_evaluation_rows(rows):
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
# STAGE 2
# ============================================================

def build_stage2_configs(
    stage1_by_id,
    top_stage1_rows,
):
    configs = []

    for rank, row in enumerate(
        top_stage1_rows[
            :STAGE2_BASE_KEEP
        ]
    ):
        base = deepcopy(
            stage1_by_id[
                row[
                    "config_id"
                ]
            ]
        )

        for context in CONTEXTS:
            cfg = deepcopy(base)

            cfg["config_id"] = (
                f"S2_{rank:02d}_"
                f"{context}"
            )

            cfg["context"] = (
                context
            )

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
        3.00,
        3.25,
        3.50,
        3.75,
        4.00,
        4.25,
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
            [
                -0.15,
                -0.05,
                0.05,
                0.15,
            ],

        "body_atr_min":
            [
                -0.20,
                -0.10,
                0.10,
                0.20,
            ],

        "range_atr_min":
            [
                -0.20,
                -0.10,
                0.10,
                0.20,
            ],

        "close_loc_max":
            [
                -0.10,
                -0.05,
                0.05,
                0.10,
            ],

        "upper_wick_body_min":
            [
                -0.10,
                -0.05,
                0.05,
                0.10,
            ],

        "structure_dist_atr_max":
            [
                -0.05,
                -0.025,
                0.025,
                0.05,
            ],

        "compression_max":
            [
                -0.05,
                -0.025,
                0.025,
                0.05,
            ],

        "mom4_min":
            [
                -0.50,
                -0.25,
                0.25,
                0.50,
            ],
    }

    for field, deltas in (
        perturbations.items()
    ):
        value = base.get(field)

        if value is None:
            continue

        for delta in deltas:
            new_value = round(
                value + delta,
                4,
            )

            if (
                field != "mom4_min"
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

            cfg = deepcopy(base)

            cfg[field] = (
                new_value
            )

            cfg["config_id"] = (
                f"S3_{rank:02d}_"
                f"{field}_{new_value}"
            )

            configs.append(cfg)

    allowed_lookbacks = [
        10,
        20,
        40,
        60,
        80,
        100,
        120,
        165,
        200,
    ]

    for field in [
        "structure_lb",
        "sweep_lb",
        "breakdown_lb",
    ]:
        value = base.get(field)

        if value not in allowed_lookbacks:
            continue

        position = (
            allowed_lookbacks.index(
                value
            )
        )

        neighbours = []

        if position > 0:
            neighbours.append(
                allowed_lookbacks[
                    position - 1
                ]
            )

        if position + 1 < len(
            allowed_lookbacks
        ):
            neighbours.append(
                allowed_lookbacks[
                    position + 1
                ]
            )

        for new_value in neighbours:
            cfg = deepcopy(base)

            cfg[field] = (
                new_value
            )

            cfg["config_id"] = (
                f"S3_{rank:02d}_"
                f"{field}_{new_value}"
            )

            configs.append(cfg)

    return configs


def build_stage3_configs(
    stage2_by_id,
    top_stage2_rows,
):
    configs = []
    seen = set()

    for rank, row in enumerate(
        top_stage2_rows[
            :STAGE3_BASE_KEEP
        ]
    ):
        base = deepcopy(
            stage2_by_id[
                row[
                    "config_id"
                ]
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
# FINAL ROBUSTNESS
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
            config["config_id"],

        "family":
            config["family"],

        "context":
            config.get(
                "context",
                "NONE",
            ),

        "rr":
            config["rr"],

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
    config,
    candles,
    indices,
):
    periods = [
        (
            "FULL_HISTORY",
            candles[0]["time"],
            RESEARCH_TO,
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
            RESEARCH_TO,
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
            RESEARCH_TO,
        ),

        (
            "LAST_5Y",
            RESEARCH_TO
            - timedelta(
                days=365.2425 * 5
            ),
            RESEARCH_TO,
        ),

        (
            "LAST_2Y",
            RESEARCH_TO
            - timedelta(
                days=365.2425 * 2
            ),
            RESEARCH_TO,
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
            config["rr"],
            PRIMARY_COST_PIPS,
            start,
            end,
        )

        row = stats_row(
            config,
            label,
            trades,
        )

        row["start_utc"] = (
            iso_utc(start)
        )

        row["end_utc"] = (
            iso_utc(end)
        )

        rows.append(row)

    return rows


def cost_rows(
    config,
    candles,
    indices,
):
    rows = []

    periods = [
        (
            "FULL_HISTORY",
            candles[0]["time"],
            RESEARCH_TO,
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
            RESEARCH_TO,
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
                config["rr"],
                cost,
                start,
                end,
            )

            row = stats_row(
                config,
                label,
                trades,
            )

            row["cost_pips"] = (
                cost
            )

            rows.append(row)

    return rows


def rolling_rows(
    config,
    candles,
    indices,
):
    rows = []

    first_month = month_floor(
        max(
            candles[0]["time"],
            REQUESTED_FROM,
        )
    )

    last_month = month_floor(
        RESEARCH_TO
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
                    config["rr"],
                    PRIMARY_COST_PIPS,
                    start,
                    end,
                )
            )

            rows.append({
                "config_id":
                    config["config_id"],

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

        positive = [
            row
            for row in subset
            if row["positive"]
        ]

        positive_active = [
            row
            for row in active
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
                    * len(positive)
                    / len(subset),
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
                    / len(active),
                    4,
                )
                if active
                else 0.0,

            "median_r_all":
                round(
                    safe_median([
                        row["total_r"]
                        for row in subset
                    ]),
                    4,
                ),

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
    config,
    candles,
    indices,
):
    rows = []

    first_year = max(
        candles[0]["time"].year,
        REQUESTED_FROM.year,
    )

    last_completed_year = (
        RESEARCH_TO.year
        - 1
    )

    for year in range(
        first_year,
        last_completed_year + 1,
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
                config["rr"],
                PRIMARY_COST_PIPS,
                start,
                end,
            )
        )

        rows.append({
            "config_id":
                config["config_id"],

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

    for (
        config_id,
        subset,
    ) in grouped.items():
        active = [
            row
            for row in subset
            if row["trades"] > 0
        ]

        positive = [
            row
            for row in subset
            if row["positive"]
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

            "positive_years":
                len(positive),

            "negative_years":
                len(negative),

            "zero_trade_years":
                len(subset)
                - len(active),

            "positive_years_pct":
                round(
                    100.0
                    * len(positive)
                    / len(subset),
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
                    / len(active),
                    4,
                )
                if active
                else 0.0,

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

        cfg[
            "config_id"
        ] = (
            finalist["config_id"]
            + "_ABLATE_CONTEXT"
        )

        cfg["context"] = (
            "NONE"
        )

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
        if finalist.get(
            field
        ) is None:
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
                "REMOVE_"
                + field,
                cfg,
            )
        )

    return configs


def plateau_configs(finalist):
    configs = local_variants(
        finalist,
        99,
    )

    for cfg in configs:
        cfg["rr"] = (
            finalist["rr"]
        )

        cfg["config_id"] = (
            finalist["config_id"]
            + "_PLATEAU_"
            + cfg["config_id"]
        )

    return configs


# ============================================================
# MAIN
# ============================================================

def run_research():
    try:
        m15 = fetch_history(
            "M15",
            REQUESTED_FROM,
            RESEARCH_TO,
            35,
        )

        h1 = fetch_history(
            "H1",
            HTF_WARMUP_FROM,
            RESEARCH_TO,
            180,
        )

        h4 = fetch_history(
            "H4",
            HTF_WARMUP_FROM,
            RESEARCH_TO,
            700,
        )

        daily = fetch_history(
            "D",
            HTF_WARMUP_FROM,
            RESEARCH_TO,
            3500,
        )

        if not all([
            m15,
            h1,
            h4,
            daily,
        ]):
            raise RuntimeError(
                "Missing required EUR_USD history"
            )

        write_csv(
            OUTPUT_COVERAGE,
            [{
                "instrument":
                    INSTRUMENT,

                "requested_start_utc":
                    iso_utc(
                        REQUESTED_FROM
                    ),

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
                "Building no-lookahead H1/H4/D state",
        })

        m15_times = [
            candle["time"]
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

        STATUS.update({
            "state":
                "precomputing",

            "message":
                "Building M15 feature cache",
        })

        features = (
            build_feature_cache(
                m15,
                h1_aligned,
                h4_aligned,
                daily_aligned,
            )
        )

        # ----------------------------------------------------
        # Existing benchmark
        # ----------------------------------------------------
        STATUS.update({
            "state":
                "benchmark",

            "message":
                "Running existing locked EUR/USD M15 SHORT benchmark",
        })

        benchmark_indices = (
            signal_indices_for_config(
                BENCHMARK,
                features,
            )
        )

        benchmark_rows = period_rows(
            BENCHMARK,
            m15,
            benchmark_indices,
        )

        benchmark_2010_plus = (
            len(
                run_backtest(
                    m15,
                    benchmark_indices,
                    BENCHMARK["rr"],
                    PRIMARY_COST_PIPS,
                    datetime(
                        2010, 1, 1,
                        tzinfo=timezone.utc,
                    ),
                    RESEARCH_TO,
                )
            )
        )

        for row in benchmark_rows:
            row[
                "locked_2010_reference_trades"
            ] = 89

            row[
                "current_2010_plus_trades"
            ] = benchmark_2010_plus

            row[
                "parity_status"
            ] = (
                "MATCH"
                if benchmark_2010_plus == 89
                else (
                    "CURRENT_RUN_HAS_NEWER_TRADES"
                    if benchmark_2010_plus > 89
                    else "CHECK_PARITY"
                )
            )

        write_csv(
            OUTPUT_BENCHMARK,
            benchmark_rows,
        )

        # ----------------------------------------------------
        # Stage 1
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

            indices = (
                signal_indices_for_config(
                    cfg,
                    features,
                )
            )

            stage1_rows.append(
                evaluation_row(
                    cfg,
                    m15,
                    indices,
                )
            )

        stage1_rows = (
            sort_evaluation_rows(
                stage1_rows
            )
        )

        write_csv(
            OUTPUT_STAGE1,
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
        # Stage 2
        # ----------------------------------------------------
        stage2_configs = (
            build_stage2_configs(
                stage1_by_id,
                top_stage1,
            )
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

            indices = (
                signal_indices_for_config(
                    cfg,
                    features,
                )
            )

            stage2_rows.append(
                evaluation_row(
                    cfg,
                    m15,
                    indices,
                )
            )

        stage2_rows = (
            sort_evaluation_rows(
                stage2_rows
            )
        )

        write_csv(
            OUTPUT_STAGE2,
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
        # Stage 3
        # ----------------------------------------------------
        stage3_configs = (
            build_stage3_configs(
                stage2_by_id,
                top_stage2,
            )
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

            indices = (
                signal_indices_for_config(
                    cfg,
                    features,
                )
            )

            stage3_rows.append(
                evaluation_row(
                    cfg,
                    m15,
                    indices,
                )
            )

        stage3_rows = (
            sort_evaluation_rows(
                stage3_rows
            )
        )

        write_csv(
            OUTPUT_STAGE3,
            stage3_rows,
        )

        eligible = [
            row
            for row in stage3_rows
            if (
                row[
                    "full_trades"
                ] >= MIN_FINAL_TRADES
                and row[
                    "pre2010_r"
                ] > 0
                and row[
                    "post2010_r"
                ] > 0
                and row[
                    "positive_eras"
                ] >= 3
            )
        ]

        finalist_rows = (
            eligible
            if eligible
            else stage3_rows
        )[
            :FINALIST_KEEP
        ]

        finalist_configs = [
            stage3_by_id[
                row["config_id"]
            ]
            for row in finalist_rows
        ]

        write_csv(
            OUTPUT_FINAL,
            finalist_rows,
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

            indices = (
                signal_indices_for_config(
                    cfg,
                    features,
                )
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
                RESEARCH_TO,
            )

            for trade in full_trades:
                row = dict(trade)

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
                ] = cfg.get(
                    "context",
                    "NONE",
                )

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

        # ----------------------------------------------------
        # Ablation + plateau top finalist
        # ----------------------------------------------------
        if finalist_configs:
            top = finalist_configs[0]

            ablation_rows = []

            for label, cfg in (
                ablation_configs(
                    top
                )
            ):
                try:
                    indices = (
                        signal_indices_for_config(
                            cfg,
                            features,
                        )
                    )

                    row = evaluation_row(
                        cfg,
                        m15,
                        indices,
                    )

                    row[
                        "ablation"
                    ] = label

                    ablation_rows.append(
                        row
                    )

                except Exception:
                    pass

            write_csv(
                OUTPUT_ABLATION,
                sort_evaluation_rows(
                    ablation_rows
                ),
            )

            plateau_rows = []

            seen = set()

            for cfg in plateau_configs(
                top
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
                    indices = (
                        signal_indices_for_config(
                            cfg,
                            features,
                        )
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
                OUTPUT_PLATEAU,
                sort_evaluation_rows(
                    plateau_rows
                ),
            )

        else:
            write_csv(
                OUTPUT_ABLATION,
                [],
            )

            write_csv(
                OUTPUT_PLATEAU,
                [],
            )

        # ----------------------------------------------------
        # Notes
        # ----------------------------------------------------
        write_csv(
            OUTPUT_NOTES,
            [{
                "item":
                    "Research standard",

                "value": (
                    "Fresh full-history EUR/USD M15 SHORT "
                    "re-examination from earliest available "
                    "OANDA history. Existing locked short retained "
                    "unchanged as benchmark."
                ),
            }, {
                "item":
                    "Historical cost",

                "value":
                    "1.0 pip adverse baseline; 0.5/1/1.5/2 stress.",
            }, {
                "item":
                    "HTF completion",

                "value": (
                    "H1/H4/D exposed only after next actual HTF "
                    "candle open via bisect_right."
                ),
            }, {
                "item":
                    "Daily alignment",

                "value":
                    "OANDA D dailyAlignment=17 America/New_York.",
            }, {
                "item":
                    "Selection warning",

                "value": (
                    "2018+ is useful forward-style validation but "
                    "not pristine unseen data because previous M15 "
                    "research already examined that period."
                ),
            }, {
                "item":
                    "Locking",

                "value": (
                    "Do not auto-lock the top row. Review eras, "
                    "rolling, calendar, cost stress, ablation and "
                    "plateau first."
                ),
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

            "message": (
                "EUR/USD M15 SHORT full-history "
                "re-examination complete"
            ),

            "m15_candles":
                len(m15),

            "stage1_configs":
                len(
                    stage1_configs
                ),

            "stage2_configs":
                len(
                    stage2_configs
                ),

            "stage3_configs":
                len(
                    stage3_configs
                ),

            "finalists":
                len(
                    finalist_configs
                ),

            "benchmark_2010_plus_trades":
                benchmark_2010_plus,

            "results_bundle":
                OUTPUT_BUNDLE,
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
            "EURUSD M15 SHORT Full-History Re-examination",

        "status":
            STATUS["state"],

        "instrument":
            INSTRUMENT,

        "timeframe":
            "M15",

        "side":
            "SELL",

        "requested_start_utc":
            iso_utc(
                REQUESTED_FROM
            ),

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
            "/eurusd-m15-short-full-history/status",
            "/eurusd-m15-short-full-history/results",
        ],
    })


@app.route(
    "/eurusd-m15-short-full-history/status"
)
def route_status():
    return jsonify(
        STATUS
    )


@app.route(
    "/eurusd-m15-short-full-history/results"
)
def route_results():
    return download_file(
        OUTPUT_BUNDLE
    )


if __name__ == "__main__":
    thread = threading.Thread(
        target=run_research,
        name=(
            "eurusd-m15-short-"
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
