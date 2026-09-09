
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
# USD/CAD M15 SHORT — FINALIST CONFIRMATION
#
# PURPOSE
# -------
# Resolve the remaining boundaries around the broad full-history
# winner before any lock decision.
#
# Frozen broad anchor:
#
#   BEAR_OUTSIDE_REVERSAL
#   bearish outside candle
#   body >= 0.75 ATR14
#   close location <= 0.40
#   structure LB100
#   high within 0.10 ATR14 of previous 100-bar high
#   previous COMPLETED H4 close < H4 EMA100
#   RR4.50
#
# Broad-run headline:
#   88 trades
#   PF 1.685869
#   +42.5239R
#   DD -9R
#
# ============================================================
# FOCUSED PROCESS
# ============================================================
#
# STAGE A — STRUCTURE / H4 REGIME
#
# Hold:
#   body >= 0.75 ATR
#   close loc <= 0.40
#   RR4.50
#
# Test:
#   lookback: 60 / 80 / 100 / 120 / 165
#   distance: .05 / .075 / .10 / .125 / .15 ATR
#   H4 EMA: 75 / 100 / 125 / 150
#
# Total = 100
#
# ------------------------------------------------------------
# STAGE B — CANDLE QUALITY
#
# Take best 8 Stage-A geometries.
#
# Test:
#   body: .65 / .75 / .85 ATR
#   close loc: .30 / .35 / .40
#
# RR remains 4.50.
#
# Total <= 72
#
# ------------------------------------------------------------
# STAGE C — RR
#
# Take best 8 Stage-B geometries.
#
# Test:
#   RR4.00 / 4.25 / 4.50 / 4.75
#
# Total <= 32
#
# ------------------------------------------------------------
# DEEP FINALISTS
#
# Best 8 Stage-C configs:
#
#   full history
#   pre-2010
#   2010+
#   2002-07
#   2008-13
#   2014-19
#   2020-now
#   2002-17
#   2018+
#   last 5Y
#   last 2Y
#
# Costs:
#   .5 / 1 / 1.5 / 2 pips
#
# Rolling:
#   12 / 24 / 36M
#
# Calendar:
#   completed years
#
# Explicit neighbourhood tables are preserved so we can see
# whether LB100 / dist.10 / H4 EMA100 is a plateau or boundary.
#
# ============================================================
# M15 HISTORICAL CONVENTIONS
# ============================================================
#
# OANDA midpoint
# ATR14 Wilder/RMA, SMA seeded
#
# USD/CAD:
#   tick = .00001
#   pip  = .0001
#
# reference entry = signal close
# historical short adverse fill = close - cost
# baseline cost = 1 pip
# stop = signal high + 10 ticks
# target based on REFERENCE close risk
# actual R based on adverse fill
#
# pyramiding0
# exits start next candle
# exact exit-candle signal eligible
#
# same-bar SHORT:
#   if high is closer to open => STOP first
#   otherwise TARGET first
#
# ============================================================
# NO LOOKAHEAD
# ============================================================
#
# H4:
#   complete_at = next ACTUAL H4 candle OPEN
#   lookup = bisect_right(completion_times, signal_time)-1
#
# Signal timestamp:
#   M15 candle OPEN
#
# ============================================================
# HARD PARITY
# ============================================================
#
# Reproduce the broad winner through the exact broad-run data end:
#
#   last M15 open = 2026-09-09 20:30 UTC
#   expected trades = 88
#
# The script aborts if this does not match.
#
# ============================================================
# ONE ZIP
# ============================================================
#
# /usdcad-m15-short-finalist-confirmation/results
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

LOOKBACKS = [
    60,
    80,
    100,
    120,
    165,
]

DISTANCES = [
    0.05,
    0.075,
    0.10,
    0.125,
    0.15,
]

H4_EMAS = [
    75,
    100,
    125,
    150,
]

BODY_VALUES = [
    0.65,
    0.75,
    0.85,
]

CLOSE_LOC_VALUES = [
    0.30,
    0.35,
    0.40,
]

RR_VALUES = [
    4.00,
    4.25,
    4.50,
    4.75,
]

STAGE_A_KEEP = 8
STAGE_B_KEEP = 8
FINALIST_KEEP = 8


# ============================================================
# OUTPUTS
# ============================================================

OUT_COVERAGE = (
    "usdcad_m15_short_finalist_confirmation_coverage.csv"
)

OUT_PARITY = (
    "usdcad_m15_short_finalist_confirmation_parity.csv"
)

OUT_STAGE_A = (
    "usdcad_m15_short_finalist_confirmation_stageA_structure_h4.csv"
)

OUT_STAGE_B = (
    "usdcad_m15_short_finalist_confirmation_stageB_candle.csv"
)

OUT_STAGE_C = (
    "usdcad_m15_short_finalist_confirmation_stageC_rr.csv"
)

OUT_FINALISTS = (
    "usdcad_m15_short_finalist_confirmation_finalists.csv"
)

OUT_PERIODS = (
    "usdcad_m15_short_finalist_confirmation_periods.csv"
)

OUT_COST = (
    "usdcad_m15_short_finalist_confirmation_cost_stress.csv"
)

OUT_ROLLING = (
    "usdcad_m15_short_finalist_confirmation_rolling.csv"
)

OUT_ROLLING_SUMMARY = (
    "usdcad_m15_short_finalist_confirmation_rolling_summary.csv"
)

OUT_CALENDAR = (
    "usdcad_m15_short_finalist_confirmation_calendar_years.csv"
)

OUT_CALENDAR_SUMMARY = (
    "usdcad_m15_short_finalist_confirmation_calendar_summary.csv"
)

OUT_TRADES = (
    "usdcad_m15_short_finalist_confirmation_trades.csv"
)

OUT_NOTES = (
    "usdcad_m15_short_finalist_confirmation_notes.csv"
)

OUT_BUNDLE = (
    "USDCAD_M15_SHORT_FINALIST_CONFIRMATION_RESULTS.zip"
)

STATUS = {
    "state":
        "not_started",

    "message":
        "USD/CAD M15 SHORT finalist confirmation not started",

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
    paths = [
        OUT_COVERAGE,
        OUT_PARITY,
        OUT_STAGE_A,
        OUT_STAGE_B,
        OUT_STAGE_C,
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
                ][
                    "close"
                ]
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

        if mode == "max":
            while (
                dq
                and values[
                    dq[-1]
                ] <= values[i]
            ):
                dq.pop()

        else:
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
# H4 STATE — MULTIPLE EMA LENGTHS, NO LOOKAHEAD
# ============================================================

def build_h4_state(h4):
    closes = [
        candle[
            "close"
        ]
        for candle in h4
    ]

    ema_map = {
        length:
            ema_list(
                closes,
                length,
            )
        for length in H4_EMAS
    }

    rows = []

    for i, candle in enumerate(h4):
        complete_at = (
            h4[
                i + 1
            ][
                "time"
            ]
            if (
                i + 1
                < len(h4)
            )
            else None
        )

        row = {
            "complete_at":
                complete_at,

            "close":
                candle[
                    "close"
                ],
        }

        for length in H4_EMAS:
            row[
                f"ema{length}"
            ] = (
                ema_map[
                    length
                ][i]
            )

        rows.append(row)

    return rows


def align_h4_to_m15(
    m15_times,
    h4_state,
):
    eligible = [
        row
        for row in h4_state
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

    for length in H4_EMAS:
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

        for length in H4_EMAS:
            value = row[
                f"ema{length}"
            ]

            if value is not None:
                result[
                    f"ema{length}"
                ][i] = value

    return result


# ============================================================
# FEATURES
# ============================================================

def build_features(
    m15,
    h4_aligned,
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

    body = (
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
        body[
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

    prior_highs = {
        lookback:
            rolling_previous_extreme(
                highs,
                lookback,
                "max",
            )
        for lookback in LOOKBACKS
    }

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

        "close_location":
            close_location,

        "previous_high":
            previous_high,

        "previous_low":
            previous_low,

        "prior_highs":
            prior_highs,

        "h4":
            h4_aligned,
    }


# ============================================================
# CONFIG / SIGNAL LOGIC
# ============================================================

def config_id(
    *,
    prefix,
    body,
    close_loc,
    lookback,
    distance,
    h4_ema,
    rr,
):
    return (
        f"{prefix}_"
        f"B{body:.2f}_"
        f"CL{close_loc:.3f}_"
        f"LB{lookback}_"
        f"D{distance:.3f}_"
        f"H4E{h4_ema}_"
        f"RR{rr:.2f}"
    )


def make_config(
    *,
    prefix,
    body,
    close_loc,
    lookback,
    distance,
    h4_ema,
    rr,
):
    return {
        "config_id":
            config_id(
                prefix=prefix,
                body=body,
                close_loc=close_loc,
                lookback=lookback,
                distance=distance,
                h4_ema=h4_ema,
                rr=rr,
            ),

        "body_atr_min":
            body,

        "close_loc_max":
            close_loc,

        "structure_lb":
            lookback,

        "structure_dist_atr_max":
            distance,

        "h4_ema_length":
            h4_ema,

        "rr":
            rr,
    }


def signal_indices(
    cfg,
    f,
):
    mask = (
        f[
            "valid_atr"
        ].copy()
        & f[
            "bearish"
        ]
    )

    # Bearish outside candle.
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
            "prior_highs"
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

    h4_ema = (
        f[
            "h4"
        ][
            f"ema{cfg['h4_ema_length']}"
        ]
    )

    mask &= (
        f[
            "h4"
        ][
            "close"
        ]
        < h4_ema
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

        # exact exit-candle signal eligible
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


def evaluate_config(
    cfg,
    m15,
    indices,
):
    full = stats_from_trades(
        run_backtest(
            m15,
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
            m15,
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
            m15,
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
                    m15,
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
        for stat in era_stats
        if stat[
            "total_r"
        ] > 0
    )

    minimum_era_pf = min(
        stat[
            "profit_factor"
        ]
        for stat in era_stats
    )

    robust_score = (
        1.5
        * min(
            full[
                "profit_factor"
            ],
            3.0,
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
            3.0,
        )
        + 0.55
        * positive_eras
        + 0.35
        * min(
            max(
                minimum_era_pf,
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

        "body_atr_min":
            cfg[
                "body_atr_min"
            ],

        "close_loc_max":
            cfg[
                "close_loc_max"
            ],

        "structure_lb":
            cfg[
                "structure_lb"
            ],

        "structure_dist_atr_max":
            cfg[
                "structure_dist_atr_max"
            ],

        "h4_ema_length":
            cfg[
                "h4_ema_length"
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
                minimum_era_pf,
                6,
            ),

        "robust_score":
            round(
                robust_score,
                6,
            ),
    }

    for i, stat in enumerate(
        era_stats,
        1,
    ):
        row[
            f"era{i}_trades"
        ] = stat[
            "trades"
        ]

        row[
            f"era{i}_pf"
        ] = round(
            stat[
                "profit_factor"
            ],
            6,
        )

        row[
            f"era{i}_r"
        ] = round(
            stat[
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
                "min_era_pf"
            ],
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
    m15,
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
                m15,
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
    m15,
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

        trades = run_backtest(
            m15,
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

    for config_id, subset in grouped.items():
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

        if not m15:
            raise RuntimeError(
                "No USD_CAD M15 candles returned"
            )

        if not h4:
            raise RuntimeError(
                "No USD_CAD H4 candles returned"
            )

        write_csv(
            OUT_COVERAGE,
            [{
                "instrument":
                    PAIR,

                "requested_start_utc":
                    iso_utc(START),

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
                    len(m15),

                "h4_candles":
                    len(h4),
            }],
        )

        STATUS.update({
            "state":
                "precomputing",

            "message":
                "Building M15 features and no-lookahead H4 EMA states",
        })

        m15_times = [
            candle[
                "time"
            ]
            for candle in m15
        ]

        h4_aligned = (
            align_h4_to_m15(
                m15_times,
                build_h4_state(
                    h4
                ),
            )
        )

        features = build_features(
            m15,
            h4_aligned,
        )

        # ----------------------------------------------------
        # HARD PARITY
        # ----------------------------------------------------
        anchor = make_config(
            prefix="ANCHOR",
            body=0.75,
            close_loc=0.40,
            lookback=100,
            distance=0.10,
            h4_ema=100,
            rr=4.50,
        )

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

        parity_features = {
            key:
                (
                    value[
                        :parity_count
                    ]
                    if isinstance(
                        value,
                        np.ndarray,
                    )
                    else {
                        subkey:
                            subvalue[
                                :parity_count
                            ]
                        for (
                            subkey,
                            subvalue,
                        )
                        in value.items()
                    }
                )
            for key, value in features.items()
        }

        anchor_indices = signal_indices(
            anchor,
            parity_features,
        )

        parity_end = (
            PARITY_LAST_M15_OPEN
            + timedelta(
                minutes=15
            )
        )

        anchor_trades = run_backtest(
            parity_m15,
            anchor_indices,
            anchor[
                "rr"
            ],
            PRIMARY_COST_PIPS,
            START,
            parity_end,
        )

        parity_match = (
            len(
                anchor_trades
            ) == 88
        )

        write_csv(
            OUT_PARITY,
            [{
                "config_id":
                    anchor[
                        "config_id"
                    ],

                "expected_trades":
                    88,

                "actual_trades":
                    len(
                        anchor_trades
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
                "Hard broad-anchor parity failed: "
                f"expected 88 trades, got "
                f"{len(anchor_trades)}."
            )

        # ----------------------------------------------------
        # STAGE A
        # ----------------------------------------------------
        stage_a_configs = []

        for lookback in LOOKBACKS:
            for distance in DISTANCES:
                for h4_ema in H4_EMAS:
                    stage_a_configs.append(
                        make_config(
                            prefix="A",
                            body=0.75,
                            close_loc=0.40,
                            lookback=lookback,
                            distance=distance,
                            h4_ema=h4_ema,
                            rr=4.50,
                        )
                    )

        if len(
            stage_a_configs
        ) != 100:
            raise RuntimeError(
                "Expected exactly 100 Stage-A configs"
            )

        stage_a_rows = []

        for i, cfg in enumerate(
            stage_a_configs,
            1,
        ):
            STATUS.update({
                "state":
                    "stage_a",

                "message": (
                    f"Structure/H4 confirmation "
                    f"{i}/100 "
                    f"{cfg['config_id']}"
                ),
            })

            indices = signal_indices(
                cfg,
                features,
            )

            stage_a_rows.append(
                evaluate_config(
                    cfg,
                    m15,
                    indices,
                )
            )

        stage_a_rows = sort_rows(
            stage_a_rows
        )

        write_csv(
            OUT_STAGE_A,
            stage_a_rows,
        )

        stage_a_map = {
            cfg[
                "config_id"
            ]:
                cfg
            for cfg in stage_a_configs
        }

        top_a = (
            stage_a_rows[
                :STAGE_A_KEEP
            ]
        )

        # ----------------------------------------------------
        # STAGE B
        # ----------------------------------------------------
        stage_b_configs = []
        seen_b = set()

        for rank, row in enumerate(
            top_a,
            1,
        ):
            base = stage_a_map[
                row[
                    "config_id"
                ]
            ]

            for body in BODY_VALUES:
                for close_loc in CLOSE_LOC_VALUES:
                    cfg = make_config(
                        prefix=f"B{rank:02d}",
                        body=body,
                        close_loc=close_loc,
                        lookback=base[
                            "structure_lb"
                        ],
                        distance=base[
                            "structure_dist_atr_max"
                        ],
                        h4_ema=base[
                            "h4_ema_length"
                        ],
                        rr=4.50,
                    )

                    signature = (
                        cfg[
                            "body_atr_min"
                        ],
                        cfg[
                            "close_loc_max"
                        ],
                        cfg[
                            "structure_lb"
                        ],
                        cfg[
                            "structure_dist_atr_max"
                        ],
                        cfg[
                            "h4_ema_length"
                        ],
                        cfg[
                            "rr"
                        ],
                    )

                    if signature in seen_b:
                        continue

                    seen_b.add(
                        signature
                    )

                    stage_b_configs.append(
                        cfg
                    )

        stage_b_rows = []

        for i, cfg in enumerate(
            stage_b_configs,
            1,
        ):
            STATUS.update({
                "state":
                    "stage_b",

                "message": (
                    f"Candle confirmation "
                    f"{i}/{len(stage_b_configs)} "
                    f"{cfg['config_id']}"
                ),
            })

            indices = signal_indices(
                cfg,
                features,
            )

            stage_b_rows.append(
                evaluate_config(
                    cfg,
                    m15,
                    indices,
                )
            )

        stage_b_rows = sort_rows(
            stage_b_rows
        )

        write_csv(
            OUT_STAGE_B,
            stage_b_rows,
        )

        stage_b_map = {
            cfg[
                "config_id"
            ]:
                cfg
            for cfg in stage_b_configs
        }

        top_b = (
            stage_b_rows[
                :STAGE_B_KEEP
            ]
        )

        # ----------------------------------------------------
        # STAGE C — RR
        # ----------------------------------------------------
        stage_c_configs = []
        seen_c = set()

        for rank, row in enumerate(
            top_b,
            1,
        ):
            base = stage_b_map[
                row[
                    "config_id"
                ]
            ]

            for rr in RR_VALUES:
                cfg = make_config(
                    prefix=f"C{rank:02d}",
                    body=base[
                        "body_atr_min"
                    ],
                    close_loc=base[
                        "close_loc_max"
                    ],
                    lookback=base[
                        "structure_lb"
                    ],
                    distance=base[
                        "structure_dist_atr_max"
                    ],
                    h4_ema=base[
                        "h4_ema_length"
                    ],
                    rr=rr,
                )

                signature = (
                    cfg[
                        "body_atr_min"
                    ],
                    cfg[
                        "close_loc_max"
                    ],
                    cfg[
                        "structure_lb"
                    ],
                    cfg[
                        "structure_dist_atr_max"
                    ],
                    cfg[
                        "h4_ema_length"
                    ],
                    cfg[
                        "rr"
                    ],
                )

                if signature in seen_c:
                    continue

                seen_c.add(
                    signature
                )

                stage_c_configs.append(
                    cfg
                )

        stage_c_rows = []

        for i, cfg in enumerate(
            stage_c_configs,
            1,
        ):
            STATUS.update({
                "state":
                    "stage_c",

                "message": (
                    f"RR confirmation "
                    f"{i}/{len(stage_c_configs)} "
                    f"{cfg['config_id']}"
                ),
            })

            indices = signal_indices(
                cfg,
                features,
            )

            stage_c_rows.append(
                evaluate_config(
                    cfg,
                    m15,
                    indices,
                )
            )

        stage_c_rows = sort_rows(
            stage_c_rows
        )

        write_csv(
            OUT_STAGE_C,
            stage_c_rows,
        )

        stage_c_map = {
            cfg[
                "config_id"
            ]:
                cfg
            for cfg in stage_c_configs
        }

        eligible_finalists = [
            row
            for row in stage_c_rows
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
                and row[
                    "full_pf"
                ] >= 1.20
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

            for row in stage_c_rows:
                if row[
                    "config_id"
                ] in selected:
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
            stage_c_map[
                row[
                    "config_id"
                ]
            ]
            for row in finalist_rows
        ]

        # ----------------------------------------------------
        # DEEP
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
                    f"Deep finalist "
                    f"{i}/{len(finalist_configs)} "
                    f"{cfg['config_id']}"
                ),
            })

            indices = signal_indices(
                cfg,
                features,
            )

            for (
                label,
                start,
                end,
            ) in period_definitions():
                trades = run_backtest(
                    m15,
                    indices,
                    cfg[
                        "rr"
                    ],
                    PRIMARY_COST_PIPS,
                    start,
                    end,
                )

                period_output.append(
                    result_row(
                        cfg,
                        label,
                        trades,
                    )
                )

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
            ]:
                for cost in COST_GRID:
                    trades = run_backtest(
                        m15,
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

                    cost_output.append(
                        row
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
                    "Broad anchor",

                "value":
                    "Bearish outside reversal; body>=0.75ATR; close location<=0.40; LB100; distance<=0.10ATR; previous completed H4 close<EMA100; RR4.50.",
            }, {
                "item":
                    "Hard parity",

                "value":
                    "Anchor must reproduce 88 trades through 2026-09-09 20:30 UTC M15 open or the run aborts.",
            }, {
                "item":
                    "Stage A",

                "value":
                    "100 configs only: LB60/80/100/120/165 x distance .05/.075/.10/.125/.15 x H4 EMA75/100/125/150, body.75 closeLoc.40 RR4.5 frozen.",
            }, {
                "item":
                    "Stage B",

                "value":
                    "Best 8 Stage-A structures only; body .65/.75/.85 and close location .30/.35/.40.",
            }, {
                "item":
                    "Stage C",

                "value":
                    "Best 8 Stage-B geometries only; RR4.0/4.25/4.5/4.75.",
            }, {
                "item":
                    "Interpretation",

                "value":
                    "Prefer parameter plateau and temporal balance over the highest isolated PF or recent-period fit.",
            }, {
                "item":
                    "Historical holdout",

                "value":
                    "No pristine historical holdout remains; this is full-history robustness/temporal confirmation.",
            }],
        )

        STATUS.update({
            "state":
                "packaging",

            "message":
                "Building one ZIP results bundle",
        })

        build_bundle()

        STATUS.update({
            "state":
                "complete",

            "message":
                "USD/CAD M15 SHORT finalist confirmation complete",

            "hard_parity":
                "MATCH",

            "stage_a_configs":
                len(stage_a_configs),

            "stage_b_configs":
                len(stage_b_configs),

            "stage_c_configs":
                len(stage_c_configs),

            "deep_finalists":
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
            "USDCAD M15 SHORT Finalist Confirmation",

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

        "anchor": {
            "family":
                "BEAR_OUTSIDE_REVERSAL",

            "body_atr_min":
                0.75,

            "close_location_max":
                0.40,

            "structure_lookback":
                100,

            "distance_atr_max":
                0.10,

            "h4_regime":
                "previous completed close < EMA100",

            "rr":
                4.50,
        },

        "orders_supported":
            False,

        "trading_enabled":
            False,

        "routes": [
            "/usdcad-m15-short-finalist-confirmation/status",
            "/usdcad-m15-short-finalist-confirmation/results",
        ],
    })


@app.route(
    "/usdcad-m15-short-finalist-confirmation/status"
)
def route_status():
    return jsonify(
        STATUS
    )


@app.route(
    "/usdcad-m15-short-finalist-confirmation/results"
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
            "finalist-confirmation"
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
