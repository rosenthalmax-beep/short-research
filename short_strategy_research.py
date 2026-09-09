
import os
import csv
import time
import bisect
import zipfile
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median

import numpy as np
import requests
from flask import Flask, jsonify, send_file


# ============================================================
# USD/CAD M15 LONG — COMPLEMENT FINAL H4 vs H1 DEEP CHECK
#
# FINAL TINY COMPARISON ONLY.
#
# FROZEN CORE:
#   exact bullish engulf
#   BR >= 1.70
#   body >= 1.25 ATR14
#   range >= 1.50 ATR14
#   prior165 low structure <= 0.175 ATR14
#   previous completed Daily EMA50 > EMA200
#   RR5.00
#
# COMPLEMENT COMPARISON:
#
# H1 references:
#
# H1_FREQ
#   compression <= 0.70
#   body >= 1.00 ATR
#   range >= 1.40 ATR
#   close > prior5 high
#   previous completed H1 EMA50 > EMA200
#   RR5.25
#
# H1_QUALITY
#   compression <= 0.70
#   body >= 1.00 ATR
#   range >= 1.50 ATR
#   close > prior5 high
#   previous completed H1 EMA50 > EMA200
#   RR5.25
#
# H4 variants:
#
#   body 1.00 / range1.50 / breakout5 / RR5.25
#
#   body 1.10 / range1.50 / breakout5 /
#       RR4.75 / 5.00 / 5.25
#
#   body 1.10 / range1.50 / breakout10 /
#       RR4.00 / 5.25
#
# TOTAL = 8 complement configs.
#
# ============================================================
# OBJECTIVE
# ============================================================
#
# Compare candidate-only AND core+non-overlap on:
#
# - full / pre2010 / post2010
# - 4 long eras
# - 2002-17 / 2018+
# - last5Y / last2Y
# - 0.5 / 1 / 1.5 / 2 pip costs
# - rolling 12 / 24 / 36M, window-local reruns
# - completed calendar years
# - inactive years filled
# - overlap
# - combined DD
#
# Do NOT choose on max R alone.
#
# Prefer:
# - still fills 2006 / 2010 / 2021 where possible
# - fewer zero-trade rolling windows
# - better candidate pre/post balance
# - better combined PF
# - acceptable DD
# - 2-pip survival
#
# ============================================================
# NO LOOKAHEAD
# ============================================================
#
# H1/H4/D:
#   complete_at = next ACTUAL HTF candle OPEN
#   lookup = bisect_right(completion_times, signal_time)-1
#
# Daily:
#   dailyAlignment=17
#   alignmentTimezone=America/New_York
#
# ============================================================
# HISTORICAL M15 CONVENTIONS
# ============================================================
#
# OANDA midpoint
# ATR14 Wilder/RMA SMA-seeded
#
# USD/CAD:
#   tick 0.00001
#   pip  0.0001
#
# reference entry = signal close
# historical long fill = close + adverse cost
# stop = signal low - 10 ticks
# target based on REFERENCE risk
# actual R based on adverse fill
#
# baseline cost = 1 pip
# stress = 0.5 / 1 / 1.5 / 2 pips
#
# pyramiding0
# exits start next candle
# exact exit-candle signal eligible
#
# same-bar LONG:
#   high closer to open => target first
#   otherwise stop first
#
# overlay interval:
#   [signal_index, exit_index)
#
# ============================================================
# PARITY
# ============================================================
#
# Core parity dataset:
#   through 2026-09-09 16:15 UTC M15 open
#
# hard expected core:
#   70 trades
#
# Soft complement references from prior frequency run:
#
# H1_FREQ:
#   ~82 candidate trades
#
# H1_QUALITY:
#   ~71 candidate trades
#
# H4 body1.00/range1.50/LB5/RR5.25:
#   ~79 candidate trades
#
# H4 body1.10/range1.50/LB5/RR5.25:
#   ~72 candidate trades
#
# ============================================================
# ONE ZIP
# ============================================================
#
# /usdcad-m15-long-complement-final-h4-vs-h1/results
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

    "rr":
        5.00,
}


# ============================================================
# EXACT 8 COMPLEMENT CONFIGS
# ============================================================

FINAL_CONFIGS = [
    {
        "config_id":
            "H1_FREQ_BODY100_RANGE140_LB5_RR525",

        "context":
            "H1_EMA50_GT_EMA200",

        "compression_max":
            0.70,

        "body_atr_min":
            1.00,

        "range_atr_min":
            1.40,

        "breakout_lb":
            5,

        "rr":
            5.25,
    },

    {
        "config_id":
            "H1_QUALITY_BODY100_RANGE150_LB5_RR525",

        "context":
            "H1_EMA50_GT_EMA200",

        "compression_max":
            0.70,

        "body_atr_min":
            1.00,

        "range_atr_min":
            1.50,

        "breakout_lb":
            5,

        "rr":
            5.25,
    },

    {
        "config_id":
            "H4_BODY100_RANGE150_LB5_RR525",

        "context":
            "H4_CLOSE_GT_EMA100",

        "compression_max":
            0.70,

        "body_atr_min":
            1.00,

        "range_atr_min":
            1.50,

        "breakout_lb":
            5,

        "rr":
            5.25,
    },

    {
        "config_id":
            "H4_BODY110_RANGE150_LB5_RR475",

        "context":
            "H4_CLOSE_GT_EMA100",

        "compression_max":
            0.70,

        "body_atr_min":
            1.10,

        "range_atr_min":
            1.50,

        "breakout_lb":
            5,

        "rr":
            4.75,
    },

    {
        "config_id":
            "H4_BODY110_RANGE150_LB5_RR500",

        "context":
            "H4_CLOSE_GT_EMA100",

        "compression_max":
            0.70,

        "body_atr_min":
            1.10,

        "range_atr_min":
            1.50,

        "breakout_lb":
            5,

        "rr":
            5.00,
    },

    {
        "config_id":
            "H4_BODY110_RANGE150_LB5_RR525",

        "context":
            "H4_CLOSE_GT_EMA100",

        "compression_max":
            0.70,

        "body_atr_min":
            1.10,

        "range_atr_min":
            1.50,

        "breakout_lb":
            5,

        "rr":
            5.25,
    },

    {
        "config_id":
            "H4_BODY110_RANGE150_LB10_RR400",

        "context":
            "H4_CLOSE_GT_EMA100",

        "compression_max":
            0.70,

        "body_atr_min":
            1.10,

        "range_atr_min":
            1.50,

        "breakout_lb":
            10,

        "rr":
            4.00,
    },

    {
        "config_id":
            "H4_BODY110_RANGE150_LB10_RR525",

        "context":
            "H4_CLOSE_GT_EMA100",

        "compression_max":
            0.70,

        "body_atr_min":
            1.10,

        "range_atr_min":
            1.50,

        "breakout_lb":
            10,

        "rr":
            5.25,
    },
]


# ============================================================
# OUTPUTS
# ============================================================

OUT_COVERAGE = (
    "usdcad_m15_long_complement_final_h4_vs_h1_coverage.csv"
)

OUT_PARITY = (
    "usdcad_m15_long_complement_final_h4_vs_h1_parity.csv"
)

OUT_EXACT = (
    "usdcad_m15_long_complement_final_h4_vs_h1_exact.csv"
)

OUT_PERIODS = (
    "usdcad_m15_long_complement_final_h4_vs_h1_periods.csv"
)

OUT_COST = (
    "usdcad_m15_long_complement_final_h4_vs_h1_cost_stress.csv"
)

OUT_ROLLING = (
    "usdcad_m15_long_complement_final_h4_vs_h1_rolling.csv"
)

OUT_ROLLING_SUMMARY = (
    "usdcad_m15_long_complement_final_h4_vs_h1_rolling_summary.csv"
)

OUT_CALENDAR = (
    "usdcad_m15_long_complement_final_h4_vs_h1_calendar_years.csv"
)

OUT_CALENDAR_SUMMARY = (
    "usdcad_m15_long_complement_final_h4_vs_h1_calendar_summary.csv"
)

OUT_OVERLAP = (
    "usdcad_m15_long_complement_final_h4_vs_h1_overlap.csv"
)

OUT_TRADES = (
    "usdcad_m15_long_complement_final_h4_vs_h1_trades.csv"
)

OUT_NOTES = (
    "usdcad_m15_long_complement_final_h4_vs_h1_notes.csv"
)

OUT_BUNDLE = (
    "USDCAD_M15_LONG_COMPLEMENT_FINAL_H4_VS_H1_RESULTS.zip"
)

STATUS = {
    "state":
        "not_started",

    "message":
        "USD/CAD M15 LONG complement final H4-vs-H1 deep check not started",

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

    for i, candle in enumerate(
        candles
    ):
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
            length
            + 1.0
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

    from collections import deque
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
# HTF STATE
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
                ][i] = value

    return result


# ============================================================
# FEATURES
# ============================================================

def build_features(
    m15,
    h1,
    h4,
    daily,
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

    prior_high5 = (
        rolling_previous_extreme(
            highs,
            5,
            "max",
        )
    )

    prior_high10 = (
        rolling_previous_extreme(
            highs,
            10,
            "max",
        )
    )

    # Core exact engulf.
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

    valid_prev_body = (
        previous_body > 0
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

    body_ratio[
        previous_body
        == 0
    ] = 999.0

    prior_low165 = (
        rolling_previous_extreme(
            lows,
            165,
            "min",
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
            prior_low165
        )
    )

    structure_distance165[
        valid_structure
    ] = (
        np.abs(
            lows[
                valid_structure
            ]
            - prior_low165[
                valid_structure
            ]
        )
        / atr[
            valid_structure
        ]
    )

    return {
        "open":
            opens,

        "high":
            highs,

        "low":
            lows,

        "close":
            closes,

        "valid_atr":
            valid_atr,

        "body_atr":
            body_atr,

        "range_atr":
            range_atr,

        "compression":
            compression,

        "prior_high5":
            prior_high5,

        "prior_high10":
            prior_high10,

        "exact_bull_engulf":
            exact_bull_engulf,

        "body_ratio":
            body_ratio,

        "structure_distance165":
            structure_distance165,

        "h1_ema50":
            h1[
                "ema50"
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
# SIGNALS
# ============================================================

def core_signal_indices(f):
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
            "structure_distance165"
        ]
        <= CORE[
            "structure_dist_atr_max"
        ]
    )

    mask &= (
        f[
            "d_ema50"
        ]
        > f[
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
    mask = (
        f[
            "valid_atr"
        ].copy()
        & (
            f[
                "close"
            ]
            > f[
                "open"
            ]
        )
    )

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

    prior_high = (
        f[
            "prior_high5"
        ]
        if cfg[
            "breakout_lb"
        ] == 5
        else f[
            "prior_high10"
        ]
    )

    mask &= (
        f[
            "close"
        ]
        > prior_high
    )

    if (
        cfg[
            "context"
        ]
        == "H1_EMA50_GT_EMA200"
    ):
        mask &= (
            f[
                "h1_ema50"
            ]
            > f[
                "h1_ema200"
            ]
        )

    elif (
        cfg[
            "context"
        ]
        == "H4_CLOSE_GT_EMA100"
    ):
        mask &= (
            f[
                "h4_close"
            ]
            > f[
                "h4_ema100"
            ]
        )

    else:
        raise RuntimeError(
            "Unknown complement context"
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
        len(candles),
    ):
        candle = candles[j]

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
            < len(core)
            and core[
                pointer
            ][
                "exit_index"
            ] <= start
        ):
            pointer += 1

        overlaps = False

        if pointer < len(core):
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
        item = dict(trade)
        item[
            "source"
        ] = "CORE"
        combined.append(item)

    for trade in accepted:
        item = dict(trade)
        item[
            "source"
        ] = "COMPLEMENT"
        combined.append(item)

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


def rolling_exact(
    cfg,
    candles,
    core_indices,
    candidate_indices,
):
    rows = []

    first_month = (
        month_floor(START)
    )

    last_month = (
        month_floor(NOW)
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

                    "accepted_adds":
                        (
                            len(accepted)
                            if mode
                            == "CORE_PLUS_NONOVERLAP"
                            else None
                        ),

                    "rejected_overlap":
                        (
                            len(rejected)
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
                len(subset),

            "active_windows":
                len(active),

            "zero_trade_windows":
                len(subset)
                - len(active),

            "positive_windows_pct":
                round(
                    (
                        100.0
                        * len(positive_all)
                        / len(subset)
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
                        / len(active)
                    )
                    if active
                    else 0.0,
                    4,
                ),

            "median_r_active":
                round(
                    median([
                        row[
                            "total_r"
                        ]
                        for row in active
                    ])
                    if active
                    else 0.0,
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
                "Building no-lookahead H1/H4/D state and exact feature cache",
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
                build_htf_state(h1),
            )
        )

        h4_aligned = (
            align_htf_to_m15(
                m15_times,
                build_htf_state(h4),
            )
        )

        daily_aligned = (
            align_htf_to_m15(
                m15_times,
                build_htf_state(daily),
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

        parity_core_indices = [
            index
            for index in core_indices
            if index < parity_count
        ]

        parity_end = (
            PARITY_LAST_M15_OPEN
            + timedelta(minutes=15)
        )

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

        core_match = (
            len(parity_core)
            == 70
        )

        parity_rows = [{
            "config_id":
                CORE[
                    "config_id"
                ],

            "expected_trades":
                70,

            "actual_trades":
                len(parity_core),

            "parity_type":
                "HARD",

            "status":
                (
                    "MATCH"
                    if core_match
                    else "FAIL"
                ),
        }]

        if not core_match:
            write_csv(
                OUT_PARITY,
                parity_rows,
            )

            raise RuntimeError(
                "Frozen core parity failed."
            )

        # ----------------------------------------------------
        # FULL CORE
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

        # Soft reference counts.
        soft_expected = {
            "H1_FREQ_BODY100_RANGE140_LB5_RR525":
                82,

            "H1_QUALITY_BODY100_RANGE150_LB5_RR525":
                71,

            "H4_BODY100_RANGE150_LB5_RR525":
                79,

            "H4_BODY110_RANGE150_LB5_RR525":
                72,
        }

        exact_rows = []
        period_rows = []
        cost_rows = []
        rolling_rows = []
        calendar_rows = []
        overlap_rows = []
        trade_rows = []

        for i, cfg in enumerate(
            FINAL_CONFIGS,
            1,
        ):
            STATUS.update({
                "state":
                    "deep_validation",

                "message": (
                    f"Final H4-vs-H1 "
                    f"{i}/{len(FINAL_CONFIGS)} "
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

                "context":
                    cfg[
                        "context"
                    ],

                "compression_max":
                    cfg[
                        "compression_max"
                    ],

                "body_atr_min":
                    cfg[
                        "body_atr_min"
                    ],

                "range_atr_min":
                    cfg[
                        "range_atr_min"
                    ],

                "breakout_lb":
                    cfg[
                        "breakout_lb"
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

                "accepted_nonoverlap":
                    len(accepted),

                "rejected_overlap":
                    len(rejected),

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

            # Soft count parity on same old endpoint.
            if (
                cfg[
                    "config_id"
                ]
                in soft_expected
            ):
                old_indices = [
                    index
                    for index in candidate_indices
                    if index < parity_count
                ]

                old_trades = run_backtest(
                    parity_candles,
                    old_indices,
                    cfg[
                        "rr"
                    ],
                    PRIMARY_COST_PIPS,
                    START,
                    parity_end,
                )

                expected = (
                    soft_expected[
                        cfg[
                            "config_id"
                        ]
                    ]
                )

                parity_rows.append({
                    "config_id":
                        cfg[
                            "config_id"
                        ],

                    "expected_trades":
                        expected,

                    "actual_trades":
                        len(old_trades),

                    "parity_type":
                        "SOFT_REFERENCE",

                    "status":
                        (
                            "MATCH"
                            if len(old_trades)
                            == expected
                            else "CHECK"
                        ),
                })

            # Periods.
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

            # Costs.
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

            # Rolling.
            rolling_rows.extend(
                rolling_exact(
                    cfg,
                    m15,
                    core_indices,
                    candidate_indices,
                )
            )

            # Calendar exact.
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
                    len(core_year)
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
                        "CORE_PLUS_NONOVERLAP",
                        combined_year,
                    ),
                ]:
                    s = stats_from_trades(
                        trades
                    )

                    calendar_rows.append({
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
                    })

            overlap_rows.append({
                "config_id":
                    cfg[
                        "config_id"
                    ],

                "candidate_trades":
                    len(candidate_trades),

                "accepted_nonoverlap":
                    len(accepted),

                "rejected_overlap":
                    len(rejected),

                "overlap_pct":
                    round(
                        (
                            100.0
                            * len(rejected)
                            / len(
                                candidate_trades
                            )
                        )
                        if candidate_trades
                        else 0.0,
                        4,
                    ),

                "combined_trades":
                    len(combined),
            })

            for trade in combined:
                row = dict(trade)

                row[
                    "config_id"
                ] = cfg[
                    "config_id"
                ]

                trade_rows.append(
                    row
                )

        # Calendar summaries.
        calendar_summary = []

        grouped = defaultdict(
            list
        )

        for row in calendar_rows:
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

            calendar_summary.append({
                "config_id":
                    config_id,

                "mode":
                    mode,

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

                "core_inactive_years_filled":
                    len(filled),

                "filled_year_list":
                    ",".join(
                        str(year)
                        for year in filled
                    ),
            })

        write_csv(
            OUT_PARITY,
            parity_rows,
        )

        write_csv(
            OUT_EXACT,
            exact_rows,
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
            rolling_rows,
        )

        write_csv(
            OUT_ROLLING_SUMMARY,
            rolling_summary_rows(
                rolling_rows
            ),
        )

        write_csv(
            OUT_CALENDAR,
            calendar_rows,
        )

        write_csv(
            OUT_CALENDAR_SUMMARY,
            calendar_summary,
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
                    "Purpose",

                "value":
                    "Final tiny deep comparison only: two H1 references vs six H4 compression complements around the promising H4 neighbourhood.",
            }, {
                "item":
                    "Core",

                "value":
                    "Frozen EMA50_REGIME_BR170 unchanged.",
            }, {
                "item":
                    "Decision",

                "value":
                    "Prefer the complement that materially improves inactive-year/rolling coverage while retaining the best balance of candidate pre/post edge, combined PF, recent 2Y/5Y behaviour, 2-pip survival and acceptable DD.",
            }, {
                "item":
                    "No further broad search",

                "value":
                    "No geometry or context outside these eight exact complement configs is tested.",
            }],
        )

        STATUS.update({
            "state":
                "packaging",

            "message":
                "Building one ZIP result bundle",
        })

        build_bundle()

        STATUS.update({
            "state":
                "complete",

            "message":
                "USD/CAD M15 LONG final H4-vs-H1 complement deep check complete",

            "core_parity":
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
            "USDCAD M15 LONG Complement Final H4 vs H1",

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
            CORE[
                "config_id"
            ],

        "configs":
            len(
                FINAL_CONFIGS
            ),

        "orders_supported":
            False,

        "trading_enabled":
            False,

        "routes": [
            "/usdcad-m15-long-complement-final-h4-vs-h1/status",
            "/usdcad-m15-long-complement-final-h4-vs-h1/results",
        ],
    })


@app.route(
    "/usdcad-m15-long-complement-final-h4-vs-h1/status"
)
def route_status():
    return jsonify(
        STATUS
    )


@app.route(
    "/usdcad-m15-long-complement-final-h4-vs-h1/results"
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
            "complement-final-h4-vs-h1"
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
