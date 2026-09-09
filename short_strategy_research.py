
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
# USD/CAD M15 SHORT — FINAL DISTANCE CONFIRMATION
#
# PURPOSE
# -------
# Final boundary check before locking USD/CAD M15 SHORT.
#
# Everything is now FROZEN except:
#
#   structure distance:
#       0.025 / 0.0375 / 0.050 / 0.0625 / 0.075 ATR14
#
#   RR:
#       4.00 / 4.25
#
# Total = 10 exact configs.
#
# ============================================================
# FROZEN GEOMETRY
# ============================================================
#
# Instrument:
#   USD_CAD
#
# Timeframe:
#   M15
#
# Side:
#   SELL
#
# Trigger:
#   bearish outside candle:
#       current high > previous high
#       current low  < previous low
#       current close < current open
#
# Body:
#   >= 0.75 ATR14
#
# Close location:
#   (close - low) / (high - low) <= 0.35
#
# Structure:
#   previous 120 completed M15 bars
#   current candle excluded
#
#   abs(signal high - previous 120-bar high) / ATR14
#       <= tested distance
#
# H4 regime:
#   previous COMPLETED H4 close < H4 EMA100
#
# No session filter.
# No weekday filter.
#
# ============================================================
# HISTORICAL M15 CONVENTIONS
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
# Signal timestamp:
#   M15 candle OPEN
#
# ============================================================
# NO LOOKAHEAD
# ============================================================
#
# H4:
#   complete_at = next ACTUAL H4 candle OPEN
#   lookup = bisect_right(completion_times, signal_time) - 1
#
# ============================================================
# HARD PARITY
# ============================================================
#
# Confirm the already-validated leader:
#
#   body 0.75
#   close location 0.35
#   LB120
#   distance 0.05 ATR
#   H4 EMA100 bearish regime
#   RR4.00
#
# Through:
#   2026-09-09 20:30 UTC M15 open
#
# Expected:
#   54 trades
#
# Abort if parity fails.
#
# ============================================================
# DEEP OUTPUT FOR ALL 10 CONFIGS
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
#   0.5 / 1 / 1.5 / 2 pips
#
# Rolling:
#   12 / 24 / 36 months
#
# Calendar:
#   completed years
#
# Decision intent:
#   establish whether 0.05 ATR is an interior sweet spot,
#   whether tighter thresholds improve monotonically,
#   and whether 0.075's extra frequency is worth lower quality.
#
# ============================================================
# ONE ZIP
# ============================================================
#
# /usdcad-m15-short-final-distance/results
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

DISTANCES = [
    0.025,
    0.0375,
    0.050,
    0.0625,
    0.075,
]

RR_VALUES = [
    4.00,
    4.25,
]

BODY_ATR_MIN = 0.75
CLOSE_LOCATION_MAX = 0.35
STRUCTURE_LOOKBACK = 120
H4_EMA_LENGTH = 100


# ============================================================
# OUTPUTS
# ============================================================

OUT_COVERAGE = (
    "usdcad_m15_short_final_distance_coverage.csv"
)

OUT_PARITY = (
    "usdcad_m15_short_final_distance_parity.csv"
)

OUT_EXACT = (
    "usdcad_m15_short_final_distance_exact.csv"
)

OUT_PERIODS = (
    "usdcad_m15_short_final_distance_periods.csv"
)

OUT_COST = (
    "usdcad_m15_short_final_distance_cost_stress.csv"
)

OUT_ROLLING = (
    "usdcad_m15_short_final_distance_rolling.csv"
)

OUT_ROLLING_SUMMARY = (
    "usdcad_m15_short_final_distance_rolling_summary.csv"
)

OUT_CALENDAR = (
    "usdcad_m15_short_final_distance_calendar_years.csv"
)

OUT_CALENDAR_SUMMARY = (
    "usdcad_m15_short_final_distance_calendar_summary.csv"
)

OUT_TRADES = (
    "usdcad_m15_short_final_distance_trades.csv"
)

OUT_NOTES = (
    "usdcad_m15_short_final_distance_notes.csv"
)

OUT_BUNDLE = (
    "USDCAD_M15_SHORT_FINAL_DISTANCE_RESULTS.zip"
)

STATUS = {
    "state":
        "not_started",

    "message":
        "USD/CAD M15 SHORT final distance confirmation not started",

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

        while (
            dq
            and values[
                dq[
                    -1
                ]
            ] <= values[i]
        ):
            dq.pop()

        dq.append(i)

    return result


# ============================================================
# H4 STATE — NO LOOKAHEAD
# ============================================================

def build_h4_state(h4):
    closes = [
        candle[
            "close"
        ]
        for candle in h4
    ]

    ema100 = ema_list(
        closes,
        H4_EMA_LENGTH,
    )

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

        rows.append({
            "complete_at":
                complete_at,

            "close":
                candle[
                    "close"
                ],

            "ema100":
                ema100[i],
        })

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
            ),

        "ema100":
            np.full(
                len(m15_times),
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
        ][i] = row[
            "close"
        ]

        if row[
            "ema100"
        ] is not None:
            result[
                "ema100"
            ][i] = row[
                "ema100"
            ]

    return result


# ============================================================
# M15 FEATURES
# ============================================================

def build_features(
    m15,
    h4_aligned,
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

    valid_atr = (
        np.isfinite(
            atr
        )
        & (
            atr
            > 0
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
    ] = highs[
        :-1
    ]

    previous_low[
        1:
    ] = lows[
        :-1
    ]

    previous_120_high = (
        rolling_previous_high(
            highs,
            STRUCTURE_LOOKBACK,
        )
    )

    structure_distance_atr = np.full(
        n,
        np.nan,
        dtype=float,
    )

    valid_structure = (
        valid_atr
        & np.isfinite(
            previous_120_high
        )
    )

    structure_distance_atr[
        valid_structure
    ] = (
        np.abs(
            highs[
                valid_structure
            ]
            - previous_120_high[
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

        "previous_120_high":
            previous_120_high,

        "structure_distance_atr":
            structure_distance_atr,

        "h4_close":
            h4_aligned[
                "close"
            ],

        "h4_ema100":
            h4_aligned[
                "ema100"
            ],
    }


# ============================================================
# CONFIGS / SIGNAL
# ============================================================

def make_config(
    distance,
    rr,
):
    return {
        "config_id":
            (
                f"DIST_{distance:.4f}_"
                f"RR_{rr:.2f}"
            ),

        "distance_atr_max":
            distance,

        "rr":
            rr,
    }


def all_configs():
    return [
        make_config(
            distance,
            rr,
        )
        for distance in DISTANCES
        for rr in RR_VALUES
    ]


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

    # Body >= 0.75 ATR14.
    mask &= (
        f[
            "body_atr"
        ]
        >= BODY_ATR_MIN
    )

    # Close location <= 0.35.
    mask &= (
        f[
            "close_location"
        ]
        <= CLOSE_LOCATION_MAX
    )

    # Structure distance.
    mask &= (
        f[
            "structure_distance_atr"
        ]
        <= cfg[
            "distance_atr_max"
        ]
    )

    # Previous COMPLETED H4 close < EMA100.
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
                exit_price = (
                    stop
                )

                reason = (
                    "STOP"
                )

            else:
                exit_price = (
                    target
                )

                reason = (
                    "TARGET"
                )

        elif hit_stop:
            exit_price = (
                stop
            )

            reason = (
                "STOP"
            )

        elif hit_target:
            exit_price = (
                target
            )

            reason = (
                "TARGET"
            )

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

        "distance_atr_max":
            cfg[
                "distance_atr_max"
            ],

        "rr":
            cfg[
                "rr"
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


# ============================================================
# ROLLING / CALENDAR
# ============================================================

def rolling_rows(
    cfg,
    m15,
    indices,
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

                "distance_atr_max":
                    cfg[
                        "distance_atr_max"
                    ],

                "rr":
                    cfg[
                        "rr"
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

        zero_trade = [
            row
            for row in subset
            if row[
                "zero_trade"
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
                    zero_trade
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

            "distance_atr_max":
                cfg[
                    "distance_atr_max"
                ],

            "rr":
                cfg[
                    "rr"
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

            "losing_years":
                len(
                    losing_years
                ),

            "losing_year_list":
                ",".join(
                    str(
                        year
                    )
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
                "No USD_CAD M15 history returned"
            )

        if not h4:
            raise RuntimeError(
                "No USD_CAD H4 history returned"
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
            }],
        )

        STATUS.update({
            "state":
                "precomputing",

            "message":
                "Building M15 features and completed-H4 EMA100 state",
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
            0.050,
            4.00,
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

        parity_features = {}

        for key, value in features.items():
            if isinstance(
                value,
                np.ndarray,
            ):
                parity_features[
                    key
                ] = value[
                    :parity_count
                ]
            else:
                parity_features[
                    key
                ] = value

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
            )
            == 54
        )

        write_csv(
            OUT_PARITY,
            [{
                "config_id":
                    anchor[
                        "config_id"
                    ],

                "expected_trades":
                    54,

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
                "Hard parity failed for confirmed "
                "0.05 / RR4.00 leader: "
                f"expected 54 trades, got "
                f"{len(anchor_trades)}."
            )

        configs = all_configs()

        exact_rows = []
        period_rows = []
        cost_rows = []
        rolling_output = []
        calendar_output = []
        trade_output = []

        for i, cfg in enumerate(
            configs,
            1,
        ):
            STATUS.update({
                "state":
                    "deep_validation",

                "message": (
                    f"Final distance check "
                    f"{i}/{len(configs)} "
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

            pre_stats = stats_from_trades(
                run_backtest(
                    m15,
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

            post_stats = stats_from_trades(
                run_backtest(
                    m15,
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

            exact_rows.append({
                "config_id":
                    cfg[
                        "config_id"
                    ],

                "distance_atr_max":
                    cfg[
                        "distance_atr_max"
                    ],

                "rr":
                    cfg[
                        "rr"
                    ],

                "full_trades":
                    full_stats[
                        "trades"
                    ],

                "full_pf":
                    round(
                        full_stats[
                            "profit_factor"
                        ],
                        6,
                    ),

                "full_r":
                    round(
                        full_stats[
                            "total_r"
                        ],
                        4,
                    ),

                "full_exp":
                    round(
                        full_stats[
                            "expectancy_r"
                        ],
                        6,
                    ),

                "full_dd":
                    round(
                        full_stats[
                            "max_drawdown_r"
                        ],
                        4,
                    ),

                "pre2010_trades":
                    pre_stats[
                        "trades"
                    ],

                "pre2010_pf":
                    round(
                        pre_stats[
                            "profit_factor"
                        ],
                        6,
                    ),

                "pre2010_r":
                    round(
                        pre_stats[
                            "total_r"
                        ],
                        4,
                    ),

                "post2010_trades":
                    post_stats[
                        "trades"
                    ],

                "post2010_pf":
                    round(
                        post_stats[
                            "profit_factor"
                        ],
                        6,
                    ),

                "post2010_r":
                    round(
                        post_stats[
                            "total_r"
                        ],
                        4,
                    ),

                "positive_eras":
                    sum(
                        1
                        for stat in era_stats
                        if stat[
                            "total_r"
                        ] > 0
                    ),

                "min_era_pf":
                    round(
                        min(
                            stat[
                                "profit_factor"
                            ]
                            for stat in era_stats
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

                "era1_r":
                    round(
                        era_stats[
                            0
                        ][
                            "total_r"
                        ],
                        4,
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

                "era2_r":
                    round(
                        era_stats[
                            1
                        ][
                            "total_r"
                        ],
                        4,
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

                "era3_r":
                    round(
                        era_stats[
                            2
                        ][
                            "total_r"
                        ],
                        4,
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

                "era4_r":
                    round(
                        era_stats[
                            3
                        ][
                            "total_r"
                        ],
                        4,
                    ),
            })

            # Periods.
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

                period_rows.append(
                    result_row(
                        cfg,
                        label,
                        trades,
                    )
                )

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

                    cost_rows.append(
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
                    "distance_atr_max"
                ] = cfg[
                    "distance_atr_max"
                ]

                row[
                    "rr"
                ] = cfg[
                    "rr"
                ]

                trade_output.append(
                    row
                )

        exact_rows.sort(
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
                    "full_pf"
                ],
                row[
                    "full_r"
                ],
            ),
            reverse=True,
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
                    "Frozen geometry",

                "value":
                    "Bearish outside candle; body>=0.75ATR14; close location<=0.35; previous120-bar high; previous completed H4 close<EMA100.",
            }, {
                "item":
                    "Only variables",

                "value":
                    "Distance .025/.0375/.05/.0625/.075 ATR and RR4.00/4.25.",
            }, {
                "item":
                    "Hard parity",

                "value":
                    "Distance .05 / RR4.00 must reproduce 54 trades through 2026-09-09 20:30 UTC M15 open.",
            }, {
                "item":
                    "Selection intent",

                "value":
                    "Do not chase the tightest threshold automatically. Prefer the interior distance/RR with the best full/pre/post/era/rolling/cost balance and sensible frequency.",
            }, {
                "item":
                    "Historical holdout",

                "value":
                    "No pristine historical holdout remains. Interpret as full-history temporal/parameter robustness confirmation.",
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
                "USD/CAD M15 SHORT final distance confirmation complete",

            "hard_parity":
                "MATCH",

            "configs":
                len(
                    configs
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
            "USDCAD M15 SHORT Final Distance Confirmation",

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

        "frozen_geometry": {
            "outside_bearish":
                True,

            "body_atr_min":
                BODY_ATR_MIN,

            "close_location_max":
                CLOSE_LOCATION_MAX,

            "structure_lookback":
                STRUCTURE_LOOKBACK,

            "h4_regime":
                "previous completed close < EMA100",
        },

        "tested_distances":
            DISTANCES,

        "tested_rr":
            RR_VALUES,

        "orders_supported":
            False,

        "trading_enabled":
            False,

        "routes": [
            "/usdcad-m15-short-final-distance/status",
            "/usdcad-m15-short-final-distance/results",
        ],
    })


@app.route(
    "/usdcad-m15-short-final-distance/status"
)
def route_status():
    return jsonify(
        STATUS
    )


@app.route(
    "/usdcad-m15-short-final-distance/results"
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
            "final-distance"
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
