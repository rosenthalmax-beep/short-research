
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
# USD/JPY M15 SHORT — COMPLEMENTARY FREQUENCY TEST
#
# PURPOSE
# -------
# GRID_0731 remains completely untouched as the core strategy.
#
# This script asks one narrow question:
#
#   Can a DISTINCT failed-breakout/rejection short trigger add
#   useful trades during periods where GRID_0731 is dormant,
#   while retaining its own edge and avoiding overlap with the
#   core strategy?
#
# This is NOT a broad USDJPY-short re-optimisation.
#
# ============================================================
# CORE — FROZEN / UNCHANGED
# ============================================================
#
# GRID_0731:
#
# - current M15 candle bearish
# - previous completed M15 ATR14 /
#   previous 20-period ATR14 mean <= 0.80
# - body >= 1.25 ATR14
# - range >= 1.60 ATR14
# - close < previous 40-bar low, current excluded
# - previous COMPLETED daily close < daily EMA200
# - no session filter
# - no weekday filter
# - RR 4.75
# - stop = signal high + 10 ticks
# - reference entry = signal close
# - historical adverse short fill = close - 1 pip
# - pyramiding 0
#
# Expected parity on the original confirmation dataset ending
# at 2026-09-09 14:15 UTC:
#
#   69 full trades
#   19 pre-2010
#   50 2010+
#
# ============================================================
# COMPLEMENTARY TRIGGER
# ============================================================
#
# FAILED BREAKOUT / REJECTION:
#
# - current candle bearish
# - signal high > previous N-bar high
# - signal close < previous N-bar high
# - bearish body >= X ATR14
# - close location within candle <= Y
# - optional broad HTF context only
#
# This is deliberately structurally different from the core
# compression-breakdown trigger.
#
# ============================================================
# TARGETED GRID
# ============================================================
#
# Previous-high lookback:
#   100 / 120 / 165 / 200
#
# Body ATR:
#   0.90 / 1.00 / 1.10
#
# Close location max:
#   0.20 / 0.25 / 0.30
#
# Context:
#   NONE
#   H4_CLOSE_LT_EMA100
#   D_CLOSE_LT_EMA200
#
# RR:
#   2.50 / 3.00 / 3.50 / 4.00 / 4.50
#
# Total:
#   4 * 3 * 3 * 3 * 5 = 540 candidates
#
# ============================================================
# WHY LOWER RR IS INCLUDED
# ============================================================
#
# The goal here is CONSISTENCY / FREQUENCY rather than simply
# maximising standalone PF. A lower RR may raise hit-rate and
# rolling-year consistency, so 2.5R-4.5R is tested.
#
# ============================================================
# COMBINED / OVERLAP LOGIC
# ============================================================
#
# The core strategy is never altered.
#
# Candidate trades are first backtested independently with
# pyramiding 0.
#
# Then a NON-OVERLAP OVERLAY is built:
#
#   A candidate trade is added only if its trade interval does
#   not overlap ANY core trade interval.
#
# Intervals are treated as:
#
#   [signal_index, exit_index)
#
# This preserves the project's exact exit-candle eligibility:
# a candidate may exit on the same candle a core signal occurs,
# or vice versa, without being classed as overlapping.
#
# The combined statistics therefore measure genuinely additive
# trades, rather than double-counting nearly simultaneous
# USDJPY short exposure.
#
# ============================================================
# SELECTION PRIORITIES
# ============================================================
#
# We care about:
#
# 1) fewer no-trade calendar years
# 2) better rolling 12/24/36M consistency
# 3) useful number of genuinely non-overlapping added trades
# 4) candidate itself profitable full / pre / post
# 5) candidate temporal stability across long eras
# 6) 2-pip cost survival
#
# We do NOT choose a candidate simply because headline PF is
# highest.
#
# ============================================================
# NO LOOKAHEAD
# ============================================================
#
# H4 / Daily:
#   complete_at = next ACTUAL HTF candle OPEN
#   lookup = bisect_right(completion_times, signal_time) - 1
#
# Daily:
#   OANDA dailyAlignment=17
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
# Short reference entry = signal close.
# Historical adverse fill = close - adverse cost.
# Stop = signal high + 10 ticks.
# Target based on REFERENCE signal-close risk.
# Actual R based on adverse fill.
#
# Pyramiding 0.
# Exit checking starts next M15 candle.
# Exact exit-candle signal eligible.
#
# Same-bar SHORT:
#   high closer to candle open => STOP first
#   otherwise TARGET first.
#
# ============================================================
# COSTS
# ============================================================
#
# Baseline:
#   1.0 pip adverse
#
# Stress:
#   0.5 / 1.0 / 1.5 / 2.0 pips
#
# ============================================================
# ONE ZIP
# ============================================================
#
# /usdjpy-m15-short-complementary-frequency/results
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

# Dataset used for the already-confirmed GRID_0731 result.
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
    "config_id":
        "CORE_GRID_0731",

    "compression_max":
        0.80,

    "body_atr_min":
        1.25,

    "range_atr_min":
        1.60,

    "breakdown_lb":
        40,

    "context":
        "D_CLOSE_LT_EMA200",

    "rr":
        4.75,
}

RAW_FAILED_BREAKOUT_CONTROL = {
    "config_id":
        "FAILED_BREAKOUT_REFERENCE",

    "lookback":
        165,

    "body_atr_min":
        1.00,

    "close_loc_max":
        0.25,

    "context":
        "NONE",

    "rr":
        3.50,
}


# ============================================================
# OUTPUTS
# ============================================================

OUT_COVERAGE = (
    "usdjpy_m15_short_complementary_coverage.csv"
)

OUT_PARITY = (
    "usdjpy_m15_short_complementary_parity.csv"
)

OUT_CORE_BASELINE = (
    "usdjpy_m15_short_complementary_core_baseline.csv"
)

OUT_GRID = (
    "usdjpy_m15_short_complementary_candidate_grid.csv"
)

OUT_FINALISTS = (
    "usdjpy_m15_short_complementary_finalists.csv"
)

OUT_PERIODS = (
    "usdjpy_m15_short_complementary_periods.csv"
)

OUT_COST = (
    "usdjpy_m15_short_complementary_cost_stress.csv"
)

OUT_ROLLING = (
    "usdjpy_m15_short_complementary_rolling.csv"
)

OUT_ROLLING_SUMMARY = (
    "usdjpy_m15_short_complementary_rolling_summary.csv"
)

OUT_CALENDAR = (
    "usdjpy_m15_short_complementary_calendar_years.csv"
)

OUT_CALENDAR_SUMMARY = (
    "usdjpy_m15_short_complementary_calendar_summary.csv"
)

OUT_OVERLAP = (
    "usdjpy_m15_short_complementary_overlap.csv"
)

OUT_TRADES = (
    "usdjpy_m15_short_complementary_overlay_trades.csv"
)

OUT_NOTES = (
    "usdjpy_m15_short_complementary_notes.csv"
)

OUT_BUNDLE = (
    "USDJPY_M15_SHORT_COMPLEMENTARY_FREQUENCY_RESULTS.zip"
)

STATUS = {
    "state":
        "not_started",

    "message":
        "USD/JPY M15 SHORT complementary-frequency test not started",

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
        OUT_CORE_BASELINE,
        OUT_GRID,
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
            cursor + timedelta(
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


def ema_list(values, length):
    result = [
        None
    ] * len(values)

    if len(values) < length:
        return result

    result[
        length - 1
    ] = (
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
    ema_lengths,
):
    closes = [
        candle["close"]
        for candle in candles
    ]

    emas = {
        length:
            ema_list(
                closes,
                length,
            )
        for length in ema_lengths
    }

    rows = []

    for i, candle in enumerate(candles):
        complete_at = (
            candles[
                i + 1
            ]["time"]
            if (
                i + 1
                < len(candles)
            )
            else None
        )

        row = {
            "complete_at":
                complete_at,

            "close":
                candle["close"],
        }

        for length in ema_lengths:
            row[
                f"ema{length}"
            ] = emas[
                length
            ][i]

        rows.append(row)

    return rows


def align_htf_to_m15(
    m15_times,
    state,
    ema_lengths,
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
                len(m15_times),
                np.nan,
                dtype=float,
            )
    }

    for length in ema_lengths:
        result[
            f"ema{length}"
        ] = np.full(
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

        row = eligible[
            position
        ]

        result[
            "close"
        ][i] = row[
            "close"
        ]

        for length in ema_lengths:
            value = row[
                f"ema{length}"
            ]

            if value is not None:
                result[
                    f"ema{length}"
                ][i] = value

    return result


# ============================================================
# FEATURE CACHE
# ============================================================

CORE_BREAKDOWN_LOOKBACK = 40

CANDIDATE_HIGH_LOOKBACKS = [
    100,
    120,
    165,
    200,
]


def build_features(
    m15,
    h4_aligned,
    daily_aligned,
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

    atr = atr14(
        m15
    )

    atr_mean20 = sma_np(
        atr,
        20,
    )

    bearish = (
        closes < opens
    )

    bearish_body = (
        opens - closes
    )

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

    # Core compression uses previous completed M15 ATR /
    # previous 20-period ATR mean.
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

    previous_atr[1:] = (
        atr[:-1]
    )

    previous_atr_mean20[1:] = (
        atr_mean20[:-1]
    )

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

    core_previous_low = (
        rolling_previous_low(
            lows,
            CORE_BREAKDOWN_LOOKBACK,
        )
    )

    candidate_previous_highs = {}

    for lookback in (
        CANDIDATE_HIGH_LOOKBACKS
    ):
        candidate_previous_highs[
            lookback
        ] = rolling_previous_high(
            highs,
            lookback,
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

        "core_previous_low":
            core_previous_low,

        "candidate_previous_highs":
            candidate_previous_highs,

        "h4_close":
            h4_aligned[
                "close"
            ],

        "h4_ema100":
            h4_aligned[
                "ema100"
            ],

        "d_close":
            daily_aligned[
                "close"
            ],

        "d_ema200":
            daily_aligned[
                "ema200"
            ],
    }


# ============================================================
# SIGNALS
# ============================================================

def core_signal_indices(
    f,
):
    mask = (
        f["bearish"].copy()
    )

    mask &= (
        f["compression"]
        <= CORE[
            "compression_max"
        ]
    )

    mask &= (
        f["body_atr"]
        >= CORE[
            "body_atr_min"
        ]
    )

    mask &= (
        f["range_atr"]
        >= CORE[
            "range_atr_min"
        ]
    )

    mask &= (
        f["close"]
        < f[
            "core_previous_low"
        ]
    )

    mask &= (
        f["d_close"]
        < f["d_ema200"]
    )

    mask[:220] = False

    return np.flatnonzero(
        mask
    ).tolist()


def candidate_signal_indices(
    cfg,
    f,
):
    previous_high = (
        f[
            "candidate_previous_highs"
        ][
            cfg[
                "lookback"
            ]
        ]
    )

    mask = (
        f["bearish"].copy()
    )

    # Failed breakout:
    # wick/high trades above prior N-bar high,
    # but candle closes back beneath that level.
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
        >= cfg[
            "body_atr_min"
        ]
    )

    mask &= (
        f["close_loc"]
        <= cfg[
            "close_loc_max"
        ]
    )

    context = cfg[
        "context"
    ]

    if context == "H4_CLOSE_LT_EMA100":
        mask &= (
            f["h4_close"]
            < f["h4_ema100"]
        )

    elif context == "D_CLOSE_LT_EMA200":
        mask &= (
            f["d_close"]
            < f["d_ema200"]
        )

    elif context != "NONE":
        raise RuntimeError(
            f"Unknown context: {context}"
        )

    mask[:220] = False

    return np.flatnonzero(
        mask
    ).tolist()


# ============================================================
# SHORT BACKTEST
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
        stop - fill
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
            ]["time"]
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
    """
    Keep candidate trades only when [signal, exit) does not
    overlap any core [signal, exit) interval.
    """
    core_sorted = sorted(
        core_trades,
        key=lambda t:
            t["signal_index"],
    )

    candidate_sorted = sorted(
        candidate_trades,
        key=lambda t:
            t["signal_index"],
    )

    accepted = []
    rejected = []

    pointer = 0

    for candidate in candidate_sorted:
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
            < len(core_sorted)
            and core_sorted[
                pointer
            ][
                "exit_index"
            ]
            <= c_start
        ):
            pointer += 1

        overlaps = False

        if pointer < len(core_sorted):
            core = core_sorted[
                pointer
            ]

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

    for trade in core_sorted:
        item = dict(trade)
        item["source"] = "CORE"
        combined.append(item)

    for trade in accepted:
        item = dict(trade)
        item["source"] = "CANDIDATE"
        combined.append(item)

    combined.sort(
        key=lambda t:
            (
                t["signal_index"],
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


def calendar_from_trades(
    config_id,
    trades,
):
    rows = []

    first_year = START.year
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


def calendar_summary(
    config_id,
    rows,
):
    active = [
        row
        for row in rows
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

    zero_trade_years = [
        row[
            "year"
        ]
        for row in rows
        if row[
            "zero_trade"
        ]
    ]

    negative_years = [
        row[
            "year"
        ]
        for row in rows
        if row[
            "negative"
        ]
    ]

    return {
        "config_id":
            config_id,

        "completed_years":
            len(rows),

        "active_years":
            len(active),

        "no_trade_years":
            len(
                zero_trade_years
            ),

        "no_trade_year_list":
            ",".join(
                str(year)
                for year in zero_trade_years
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
                    / len(active)
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
                str(year)
                for year in negative_years
            ),

        "median_year_r":
            round(
                safe_median([
                    row[
                        "total_r"
                    ]
                    for row in rows
                ]),
                4,
            ),

        "worst_year_r":
            round(
                min(
                    row[
                        "total_r"
                    ]
                    for row in rows
                ),
                4,
            ),

        "best_year_r":
            round(
                max(
                    row[
                        "total_r"
                    ]
                    for row in rows
                ),
                4,
            ),
    }


# ============================================================
# CANDIDATE GRID
# ============================================================

def build_candidate_grid():
    configs = []
    counter = 0

    for lookback in [
        100,
        120,
        165,
        200,
    ]:
        for body in [
            0.90,
            1.00,
            1.10,
        ]:
            for close_loc in [
                0.20,
                0.25,
                0.30,
            ]:
                for context in [
                    "NONE",
                    "H4_CLOSE_LT_EMA100",
                    "D_CLOSE_LT_EMA200",
                ]:
                    for rr in [
                        2.50,
                        3.00,
                        3.50,
                        4.00,
                        4.50,
                    ]:
                        counter += 1

                        configs.append({
                            "config_id":
                                f"CAND_{counter:04d}",

                            "lookback":
                                lookback,

                            "body_atr_min":
                                body,

                            "close_loc_max":
                                close_loc,

                            "context":
                                context,

                            "rr":
                                rr,
                        })

    return configs


def candidate_evaluation(
    cfg,
    candles,
    candidate_indices,
    core_trades,
):
    candidate_trades = run_backtest(
        candles,
        candidate_indices,
        cfg[
            "rr"
        ],
        PRIMARY_COST_PIPS,
        candles[
            0
        ]["time"],
        NOW,
    )

    candidate_stats = (
        stats_from_trades(
            candidate_trades
        )
    )

    combined, accepted, rejected = (
        nonoverlap_overlay(
            core_trades,
            candidate_trades,
        )
    )

    combined_stats = (
        stats_from_trades(
            combined
        )
    )

    pre = stats_from_trades(
        filter_trades(
            candidate_trades,
            candles[
                0
            ]["time"],
            datetime(
                2010, 1, 1,
                tzinfo=timezone.utc,
            ),
        )
    )

    post = stats_from_trades(
        filter_trades(
            candidate_trades,
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
                filter_trades(
                    candidate_trades,
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

    combined_calendar = (
        calendar_from_trades(
            cfg[
                "config_id"
            ],
            combined,
        )
    )

    combined_cal_summary = (
        calendar_summary(
            cfg[
                "config_id"
            ],
            combined_calendar,
        )
    )

    overlap_pct = (
        100.0
        * len(rejected)
        / len(candidate_trades)
        if candidate_trades
        else 0.0
    )

    # Frequency/consistency score.
    # No-trade year reduction is deliberately weighted heavily.
    coverage_score = (
        (
            8
            - combined_cal_summary[
                "no_trade_years"
            ]
        )
        * 5.0
        + combined_cal_summary[
            "positive_active_years_pct"
        ]
        / 20.0
        + len(accepted)
        / 40.0
        + min(
            candidate_stats[
                "profit_factor"
            ],
            2.5,
        )
        + 0.5
        * min(
            pre[
                "profit_factor"
            ],
            2.0,
        )
        + 0.5
        * min(
            post[
                "profit_factor"
            ],
            2.0,
        )
        + 0.40
        * positive_eras
        + combined_stats[
            "total_r"
        ]
        / 100.0
        - overlap_pct
        / 100.0
    )

    row = {
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

        "candidate_pre2010_trades":
            pre[
                "trades"
            ],

        "candidate_pre2010_pf":
            round(
                pre[
                    "profit_factor"
                ],
                6,
            ),

        "candidate_pre2010_r":
            round(
                pre[
                    "total_r"
                ],
                4,
            ),

        "candidate_post2010_trades":
            post[
                "trades"
            ],

        "candidate_post2010_pf":
            round(
                post[
                    "profit_factor"
                ],
                6,
            ),

        "candidate_post2010_r":
            round(
                post[
                    "total_r"
                ],
                4,
            ),

        "candidate_positive_eras":
            positive_eras,

        "candidate_era1_pf":
            round(
                era_stats[
                    0
                ][
                    "profit_factor"
                ],
                6,
            ),

        "candidate_era2_pf":
            round(
                era_stats[
                    1
                ][
                    "profit_factor"
                ],
                6,
            ),

        "candidate_era3_pf":
            round(
                era_stats[
                    2
                ][
                    "profit_factor"
                ],
                6,
            ),

        "candidate_era4_pf":
            round(
                era_stats[
                    3
                ][
                    "profit_factor"
                ],
                6,
            ),

        "candidate_overlap_trades":
            len(
                rejected
            ),

        "candidate_overlap_pct":
            round(
                overlap_pct,
                4,
            ),

        "added_nonoverlap_trades":
            len(
                accepted
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

        "combined_no_trade_years":
            combined_cal_summary[
                "no_trade_years"
            ],

        "combined_no_trade_year_list":
            combined_cal_summary[
                "no_trade_year_list"
            ],

        "combined_positive_active_years_pct":
            combined_cal_summary[
                "positive_active_years_pct"
            ],

        "coverage_score":
            round(
                coverage_score,
                6,
            ),
    }

    return (
        row,
        candidate_trades,
        combined,
        accepted,
        rejected,
    )


def sort_grid(rows):
    return sorted(
        rows,
        key=lambda row: (
            row[
                "candidate_pre2010_r"
            ] > 0,
            row[
                "candidate_post2010_r"
            ] > 0,
            row[
                "candidate_positive_eras"
            ],
            -row[
                "combined_no_trade_years"
            ],
            row[
                "combined_positive_active_years_pct"
            ],
            row[
                "coverage_score"
            ],
        ),
        reverse=True,
    )


# ============================================================
# DEEP VALIDATION
# ============================================================

def period_definition():
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


def build_overlay_for_period(
    candles,
    core_indices,
    candidate_indices,
    candidate_rr,
    cost_pips,
    start,
    end,
):
    core_trades = run_backtest(
        candles,
        core_indices,
        CORE[
            "rr"
        ],
        cost_pips,
        start,
        end,
    )

    candidate_trades = (
        run_backtest(
            candles,
            candidate_indices,
            candidate_rr,
            cost_pips,
            start,
            end,
        )
    )

    combined, accepted, rejected = (
        nonoverlap_overlay(
            core_trades,
            candidate_trades,
        )
    )

    return (
        core_trades,
        candidate_trades,
        combined,
        accepted,
        rejected,
    )


def deep_period_rows(
    cfg,
    candles,
    core_indices,
    candidate_indices,
):
    rows = []

    for label, start, end in (
        period_definition()
    ):
        (
            core_trades,
            candidate_trades,
            combined,
            accepted,
            rejected,
        ) = build_overlay_for_period(
            candles,
            core_indices,
            candidate_indices,
            cfg[
                "rr"
            ],
            PRIMARY_COST_PIPS,
            start,
            end,
        )

        for mode, trades in [
            (
                "CORE_ONLY",
                core_trades,
            ),
            (
                "CANDIDATE_ONLY",
                candidate_trades,
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

                "added_nonoverlap":
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
            (
                core_trades,
                candidate_trades,
                combined,
                accepted,
                rejected,
            ) = build_overlay_for_period(
                candles,
                core_indices,
                candidate_indices,
                cfg[
                    "rr"
                ],
                cost,
                start,
                end,
            )

            for mode, trades in [
                (
                    "CANDIDATE_ONLY",
                    candidate_trades,
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

                    "added_nonoverlap":
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


def deep_rolling_rows(
    cfg,
    candles,
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

            (
                core_trades,
                candidate_trades,
                combined,
                accepted,
                rejected,
            ) = build_overlay_for_period(
                candles,
                core_indices,
                candidate_indices,
                cfg[
                    "rr"
                ],
                PRIMARY_COST_PIPS,
                start,
                end,
            )

            for mode, trades in [
                (
                    "CORE_ONLY",
                    core_trades,
                ),
                (
                    "CANDIDATE_ONLY",
                    candidate_trades,
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
                })

            start = add_months(
                start,
                1,
            )

    return rows


def rolling_summary_rows(
    rows,
):
    grouped = defaultdict(list)

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
        ].append(row)

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


def deep_calendar_rows(
    cfg,
    candles,
    core_indices,
    candidate_indices,
):
    rows = []

    last_year = (
        NOW.year - 1
    )

    for year in range(
        START.year,
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

        (
            core_trades,
            candidate_trades,
            combined,
            accepted,
            rejected,
        ) = build_overlay_for_period(
            candles,
            core_indices,
            candidate_indices,
            cfg[
                "rr"
            ],
            PRIMARY_COST_PIPS,
            start,
            end,
        )

        for mode, trades in [
            (
                "CORE_ONLY",
                core_trades,
            ),
            (
                "CANDIDATE_ONLY",
                candidate_trades,
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
            })

    return rows


def deep_calendar_summary_rows(
    rows,
):
    grouped = defaultdict(list)

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
        ].append(row)

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
                    str(year)
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
                        ]["time"]
                    ),

                "actual_last_m15_utc":
                    iso_utc(
                        m15[
                            -1
                        ]["time"]
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

        h4_aligned = align_htf_to_m15(
            m15_times,
            build_htf_state(
                h4,
                [100],
            ),
            [100],
        )

        daily_aligned = (
            align_htf_to_m15(
                m15_times,
                build_htf_state(
                    daily,
                    [200],
                ),
                [200],
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

        # ----------------------------------------------------
        # PARITY ON ORIGINAL DATASET
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

        parity_core_indices = [
            index
            for index in core_indices
            if index < parity_count
        ]

        parity_core_trades = (
            run_backtest(
                parity_candles,
                parity_core_indices,
                CORE[
                    "rr"
                ],
                PRIMARY_COST_PIPS,
                START,
                PARITY_LAST_M15_OPEN
                + timedelta(
                    minutes=15
                ),
            )
        )

        parity_pre = (
            stats_from_trades(
                filter_trades(
                    parity_core_trades,
                    START,
                    datetime(
                        2010, 1, 1,
                        tzinfo=timezone.utc,
                    ),
                )
            )
        )

        parity_post = (
            stats_from_trades(
                filter_trades(
                    parity_core_trades,
                    datetime(
                        2010, 1, 1,
                        tzinfo=timezone.utc,
                    ),
                    PARITY_LAST_M15_OPEN
                    + timedelta(
                        minutes=15
                    ),
                )
            )
        )

        raw_indices = (
            candidate_signal_indices(
                RAW_FAILED_BREAKOUT_CONTROL,
                features,
            )
        )

        parity_raw_indices = [
            index
            for index in raw_indices
            if index < parity_count
        ]

        parity_raw_trades = (
            run_backtest(
                parity_candles,
                parity_raw_indices,
                RAW_FAILED_BREAKOUT_CONTROL[
                    "rr"
                ],
                PRIMARY_COST_PIPS,
                START,
                PARITY_LAST_M15_OPEN
                + timedelta(
                    minutes=15
                ),
            )
        )

        parity_ok = (
            len(
                parity_core_trades
            ) == 69
            and parity_pre[
                "trades"
            ] == 19
            and parity_post[
                "trades"
            ] == 50
            and len(
                parity_raw_trades
            ) == 317
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
                        parity_core_trades
                    ),

                "expected_pre2010":
                    19,

                "actual_pre2010":
                    parity_pre[
                        "trades"
                    ],

                "expected_post2010":
                    50,

                "actual_post2010":
                    parity_post[
                        "trades"
                    ],

                "status":
                    (
                        "MATCH"
                        if (
                            len(
                                parity_core_trades
                            ) == 69
                            and parity_pre[
                                "trades"
                            ] == 19
                            and parity_post[
                                "trades"
                            ] == 50
                        )
                        else "FAIL"
                    ),
            }, {
                "test":
                    "FAILED_BREAKOUT_REFERENCE",

                "expected_full":
                    317,

                "actual_full":
                    len(
                        parity_raw_trades
                    ),

                "status":
                    (
                        "MATCH"
                        if len(
                            parity_raw_trades
                        ) == 317
                        else "FAIL"
                    ),
            }],
        )

        if not parity_ok:
            raise RuntimeError(
                "Parity failed on original 2026-09-09 14:15 UTC dataset. "
                "Do not trust complementary-frequency research."
            )

        # ----------------------------------------------------
        # CORE BASELINE ON CURRENT FULL DATA
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

        core_stats = (
            stats_from_trades(
                core_trades
            )
        )

        core_calendar = (
            calendar_from_trades(
                "CORE_GRID_0731",
                core_trades,
            )
        )

        core_cal_summary = (
            calendar_summary(
                "CORE_GRID_0731",
                core_calendar,
            )
        )

        write_csv(
            OUT_CORE_BASELINE,
            [{
                "config_id":
                    "CORE_GRID_0731",

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

                "active_years":
                    core_cal_summary[
                        "active_years"
                    ],

                "no_trade_years":
                    core_cal_summary[
                        "no_trade_years"
                    ],

                "no_trade_year_list":
                    core_cal_summary[
                        "no_trade_year_list"
                    ],

                "positive_active_years_pct":
                    core_cal_summary[
                        "positive_active_years_pct"
                    ],
            }],
        )

        # ----------------------------------------------------
        # TARGETED 540-CANDIDATE GRID
        # ----------------------------------------------------
        configs = (
            build_candidate_grid()
        )

        grid_rows = []
        cached_indices = {}

        for i, cfg in enumerate(
            configs,
            1,
        ):
            STATUS.update({
                "state":
                    "candidate_grid",

                "message": (
                    f"Candidate {i}/{len(configs)} "
                    f"{cfg['config_id']}"
                ),
            })

            geometry_key = (
                cfg[
                    "lookback"
                ],
                cfg[
                    "body_atr_min"
                ],
                cfg[
                    "close_loc_max"
                ],
                cfg[
                    "context"
                ],
            )

            if (
                geometry_key
                not in cached_indices
            ):
                cached_indices[
                    geometry_key
                ] = candidate_signal_indices(
                    cfg,
                    features,
                )

            indices = (
                cached_indices[
                    geometry_key
                ]
            )

            row, _, _, _, _ = (
                candidate_evaluation(
                    cfg,
                    m15,
                    indices,
                    core_trades,
                )
            )

            grid_rows.append(
                row
            )

        grid_rows = sort_grid(
            grid_rows
        )

        write_csv(
            OUT_GRID,
            grid_rows,
        )

        # ----------------------------------------------------
        # FINALIST SELECTION
        # ----------------------------------------------------
        #
        # Require a real standalone edge. This prevents the
        # frequency overlay from becoming a collection of weak
        # trades that merely fills empty years.
        # ----------------------------------------------------
        eligible = [
            row
            for row in grid_rows
            if (
                row[
                    "candidate_trades"
                ] >= 80
                and row[
                    "candidate_pf"
                ] >= 1.10
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
                    "added_nonoverlap_trades"
                ] >= 50
            )
        ]

        finalists = (
            eligible[:8]
        )

        if len(
            finalists
        ) < 8:
            selected = {
                row[
                    "config_id"
                ]
                for row in finalists
            }

            for row in grid_rows:
                if row[
                    "config_id"
                ] in selected:
                    continue

                finalists.append(
                    row
                )

                selected.add(
                    row[
                        "config_id"
                    ]
                )

                if len(
                    finalists
                ) >= 8:
                    break

        write_csv(
            OUT_FINALISTS,
            finalists,
        )

        config_by_id = {
            cfg[
                "config_id"
            ]:
                cfg
            for cfg in configs
        }

        # ----------------------------------------------------
        # DEEP VALIDATION
        # ----------------------------------------------------
        period_output = []
        cost_output = []
        rolling_output = []
        calendar_output = []
        overlap_output = []
        trade_output = []

        for i, finalist_row in enumerate(
            finalists,
            1,
        ):
            cfg = config_by_id[
                finalist_row[
                    "config_id"
                ]
            ]

            geometry_key = (
                cfg[
                    "lookback"
                ],
                cfg[
                    "body_atr_min"
                ],
                cfg[
                    "close_loc_max"
                ],
                cfg[
                    "context"
                ],
            )

            candidate_indices = (
                cached_indices[
                    geometry_key
                ]
            )

            STATUS.update({
                "state":
                    "deep_validation",

                "message": (
                    f"Deep {i}/{len(finalists)} "
                    f"{cfg['config_id']}"
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

            rolling_output.extend(
                deep_rolling_rows(
                    cfg,
                    m15,
                    core_indices,
                    candidate_indices,
                )
            )

            calendar_rows = (
                deep_calendar_rows(
                    cfg,
                    m15,
                    core_indices,
                    candidate_indices,
                )
            )

            calendar_output.extend(
                calendar_rows
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
            deep_calendar_summary_rows(
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
                    "Core strategy",

                "value":
                    "GRID_0731 is frozen and never loosened or altered.",
            }, {
                "item":
                    "Complementary trigger",

                "value":
                    "Failed-breakout/rejection: high > prior N-bar high, close back below prior N-bar high, bearish body threshold, close-location threshold.",
            }, {
                "item":
                    "Overlay rule",

                "value":
                    "Candidate trade is additive only if [signal_index, exit_index) does not overlap any core trade interval.",
            }, {
                "item":
                    "Research goal",

                "value":
                    "Reduce no-trade years and improve rolling consistency without adding a weak or heavily overlapping second strategy.",
            }, {
                "item":
                    "Selection warning",

                "value":
                    "Do not add a second trigger unless combined rolling/calendar consistency genuinely improves and candidate remains profitable under 2-pip stress.",
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
                "USD/JPY M15 SHORT complementary-frequency test complete",

            "parity":
                "MATCH",

            "core_trades":
                len(
                    core_trades
                ),

            "core_no_trade_years":
                core_cal_summary[
                    "no_trade_years"
                ],

            "candidate_configs":
                len(
                    configs
                ),

            "deep_finalists":
                len(
                    finalists
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
            "USDJPY M15 SHORT Complementary Frequency Test",

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
            "GRID_0731 — frozen/unchanged",

        "candidate_family":
            "FAILED_BREAKOUT_REJECTION",

        "candidate_configs":
            540,

        "overlay":
            "candidate trades added only when they do not overlap any core trade",

        "orders_supported":
            False,

        "trading_enabled":
            False,

        "routes": [
            "/usdjpy-m15-short-complementary-frequency/status",
            "/usdjpy-m15-short-complementary-frequency/results",
        ],
    })


@app.route(
    "/usdjpy-m15-short-complementary-frequency/status"
)
def route_status():
    return jsonify(
        STATUS
    )


@app.route(
    "/usdjpy-m15-short-complementary-frequency/results"
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
