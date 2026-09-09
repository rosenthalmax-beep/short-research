
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
# USD/JPY M15 SHORT — FINAL COMPRESSION/BREAKDOWN CONFIRMATION
#
# PURPOSE
# -------
# Final focused confirmation only.
# No new archetype search.
#
# Anchor discovered in the full-history re-examination:
#
#   current candle bearish
#   previous M15 ATR14 / previous 20-period mean ATR14 <= 0.80
#   body >= 1.25 ATR14
#   range >= 1.60 ATR14
#   close < previous 40-bar low
#   previous COMPLETED daily close < daily EMA200
#   no time filter
#   no weekday filter
#   RR 3.50
#   stop = signal high + 10 ticks
#   historical adverse cost = 1 pip
#
# Prior anchor reference:
#   ~69 trades
#   ~19 pre-2010
#   ~50 post-2010
#   PF ~1.91
#   +39.18R
#
# Raw underlying edge without daily context:
#   ~131 trades
#   PF ~1.50
#   +44.65R
#   all four eras positive
#
# ============================================================
# FOCUSED GRID
# ============================================================
#
# Breakdown lookback:
#   30 / 40 / 50 / 60
#
# Compression:
#   0.775 / 0.800 / 0.825
#
# Body ATR:
#   1.15 / 1.25 / 1.35
#
# Range ATR:
#   1.50 / 1.60 / 1.70
#
# Context:
#   NONE
#   D_CLOSE_LT_EMA200
#   H4_CLOSE_LT_EMA200
#
# RR:
#   3.50 / 4.00 / 4.25 / 4.50 / 4.75 / 5.00
#
# Total local grid:
#   4 * 3 * 3 * 3 * 3 * 6 = 1944 configurations
#
# ============================================================
# DEEP VALIDATION
# ============================================================
#
# - full history
# - pre-2010
# - 2010+
# - 2002-07
# - 2008-13
# - 2014-19
# - 2020-now
# - 2002-17 vs 2018+
# - last 5Y
# - last 2Y
# - 0.5 / 1 / 1.5 / 2 pip cost stress
# - rolling 12 / 24 / 36M
# - completed calendar years
#
# ============================================================
# NO LOOKAHEAD
# ============================================================
#
# H4 / Daily:
#   complete_at = next ACTUAL HTF candle open
#   lookup = bisect_right(completion_times, signal_time) - 1
#
# Daily OANDA alignment:
#   dailyAlignment=17
#   alignmentTimezone=America/New_York
#
# M15 signal timestamp = candle OPEN.
#
# ============================================================
# SHORT HISTORICAL CONVENTIONS
# ============================================================
#
# OANDA midpoint.
# ATR14 Wilder/RMA, SMA seeded.
#
# USDJPY:
#   tick = .001
#   pip  = .01
#
# Reference entry = signal close.
# Historical short fill = signal close - adverse cost.
# Stop = signal high + 10 ticks.
# Target based on REFERENCE signal-close risk.
# Actual R based on adverse fill.
#
# Pyramiding = 0.
# Exit checking starts next candle.
# Exact exit-candle signal eligible.
#
# Same-bar SHORT tie:
#   high closer to candle open => STOP first
#   otherwise TARGET first.
#
# ============================================================
# ONE ZIP
# ============================================================
#
# /usdjpy-m15-short-final-confirmation/results
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


ANCHOR = {
    "config_id":
        "ANCHOR_LB40_COMP080_BODY125_RANGE160_DLTEMA200_RR350",

    "breakdown_lb":
        40,

    "compression_max":
        0.80,

    "body_atr_min":
        1.25,

    "range_atr_min":
        1.60,

    "context":
        "D_CLOSE_LT_EMA200",

    "rr":
        3.50,
}


RAW_CONTROL = {
    "config_id":
        "RAW_CONTROL_LB40_COMP080_BODY125_RANGE160_NONE_RR350",

    "breakdown_lb":
        40,

    "compression_max":
        0.80,

    "body_atr_min":
        1.25,

    "range_atr_min":
        1.60,

    "context":
        "NONE",

    "rr":
        3.50,
}


OUT_COVERAGE = (
    "usdjpy_m15_short_final_confirmation_coverage.csv"
)

OUT_PARITY = (
    "usdjpy_m15_short_final_confirmation_parity.csv"
)

OUT_CONTROLS = (
    "usdjpy_m15_short_final_confirmation_controls.csv"
)

OUT_SLICES = (
    "usdjpy_m15_short_final_confirmation_one_way_slices.csv"
)

OUT_GRID = (
    "usdjpy_m15_short_final_confirmation_local_grid.csv"
)

OUT_FINALISTS = (
    "usdjpy_m15_short_final_confirmation_finalists.csv"
)

OUT_PERIODS = (
    "usdjpy_m15_short_final_confirmation_periods.csv"
)

OUT_COST = (
    "usdjpy_m15_short_final_confirmation_cost_stress.csv"
)

OUT_ROLLING = (
    "usdjpy_m15_short_final_confirmation_rolling.csv"
)

OUT_ROLLING_SUMMARY = (
    "usdjpy_m15_short_final_confirmation_rolling_summary.csv"
)

OUT_CALENDAR = (
    "usdjpy_m15_short_final_confirmation_calendar_years.csv"
)

OUT_CALENDAR_SUMMARY = (
    "usdjpy_m15_short_final_confirmation_calendar_summary.csv"
)

OUT_TRADES = (
    "usdjpy_m15_short_final_confirmation_trades.csv"
)

OUT_BUNDLE = (
    "USDJPY_M15_SHORT_FINAL_CONFIRMATION_RESULTS.zip"
)

STATUS = {
    "state":
        "not_started",

    "message":
        "USD/JPY M15 SHORT final confirmation not started",

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
        OUT_CONTROLS,
        OUT_SLICES,
        OUT_GRID,
        OUT_FINALISTS,
        OUT_PERIODS,
        OUT_COST,
        OUT_ROLLING,
        OUT_ROLLING_SUMMARY,
        OUT_CALENDAR,
        OUT_CALENDAR_SUMMARY,
        OUT_TRADES,
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


# ============================================================
# HTF — NO LOOKAHEAD
# ============================================================

def build_htf_state(candles):
    closes = [
        candle["close"]
        for candle in candles
    ]

    ema200 = ema_list(
        closes,
        200,
    )

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

        rows.append({
            "complete_at":
                complete_at,

            "close":
                candle["close"],

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

    close_result = np.full(
        len(m15_times),
        np.nan,
        dtype=float,
    )

    ema200_result = np.full(
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

        row = eligible[position]

        if row["close"] is not None:
            close_result[i] = (
                row["close"]
            )

        if row["ema200"] is not None:
            ema200_result[i] = (
                row["ema200"]
            )

    return {
        "close":
            close_result,

        "ema200":
            ema200_result,
    }


# ============================================================
# FEATURE CACHE
# ============================================================

BREAKDOWN_LOOKBACKS = [
    30,
    40,
    50,
    60,
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

    atr = atr14(m15)
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

    # Compression uses PREVIOUS completed M15 ATR relative to
    # the PREVIOUS 20-period ATR mean. Signal candle ATR itself
    # is not used in the compression ratio.
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

    previous_lows = {}

    for lookback in BREAKDOWN_LOOKBACKS:
        previous_lows[
            lookback
        ] = rolling_previous_low(
            lows,
            lookback,
        )

    return {
        "bearish":
            bearish,

        "body_atr":
            body_atr,

        "range_atr":
            range_atr,

        "compression":
            compression,

        "close":
            closes,

        "previous_lows":
            previous_lows,

        "h4_close":
            h4_aligned[
                "close"
            ],

        "h4_ema200":
            h4_aligned[
                "ema200"
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

def signal_indices(
    cfg,
    f,
):
    mask = (
        f["bearish"].copy()
    )

    mask &= (
        f["compression"]
        <= cfg[
            "compression_max"
        ]
    )

    mask &= (
        f["body_atr"]
        >= cfg[
            "body_atr_min"
        ]
    )

    mask &= (
        f["range_atr"]
        >= cfg[
            "range_atr_min"
        ]
    )

    mask &= (
        f["close"]
        < f[
            "previous_lows"
        ][
            cfg[
                "breakdown_lb"
            ]
        ]
    )

    context = cfg[
        "context"
    ]

    if context == "D_CLOSE_LT_EMA200":
        mask &= (
            f["d_close"]
            < f["d_ema200"]
        )

    elif context == "H4_CLOSE_LT_EMA200":
        mask &= (
            f["h4_close"]
            < f["h4_ema200"]
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
            cfg[
                "config_id"
            ],

        "breakdown_lb":
            cfg[
                "breakdown_lb"
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

        "context":
            cfg[
                "context"
            ],

        "rr":
            cfg[
                "rr"
            ],

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
                pre[
                    "total_r"
                ],
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
            row["positive_eras"],
            row["pre2010_r"] > 0,
            row["post2010_r"] > 0,
            row["robust_score"],
            row["full_r"],
        ),
        reverse=True,
    )


# ============================================================
# CONFIG GENERATION
# ============================================================

def clone_anchor(config_id):
    cfg = dict(ANCHOR)
    cfg[
        "config_id"
    ] = config_id
    return cfg


def one_way_configs():
    configs = []

    for value in [
        30,
        40,
        50,
        60,
    ]:
        cfg = clone_anchor(
            f"SLICE_LB_{value}"
        )
        cfg[
            "breakdown_lb"
        ] = value
        cfg["slice"] = "BREAKDOWN_LB"
        configs.append(cfg)

    for value in [
        0.775,
        0.800,
        0.825,
    ]:
        cfg = clone_anchor(
            f"SLICE_COMP_{value:.3f}"
        )
        cfg[
            "compression_max"
        ] = value
        cfg["slice"] = "COMPRESSION"
        configs.append(cfg)

    for value in [
        1.15,
        1.25,
        1.35,
    ]:
        cfg = clone_anchor(
            f"SLICE_BODY_{value:.2f}"
        )
        cfg[
            "body_atr_min"
        ] = value
        cfg["slice"] = "BODY_ATR"
        configs.append(cfg)

    for value in [
        1.50,
        1.60,
        1.70,
    ]:
        cfg = clone_anchor(
            f"SLICE_RANGE_{value:.2f}"
        )
        cfg[
            "range_atr_min"
        ] = value
        cfg["slice"] = "RANGE_ATR"
        configs.append(cfg)

    for value in [
        "NONE",
        "D_CLOSE_LT_EMA200",
        "H4_CLOSE_LT_EMA200",
    ]:
        cfg = clone_anchor(
            f"SLICE_CONTEXT_{value}"
        )
        cfg[
            "context"
        ] = value
        cfg["slice"] = "CONTEXT"
        configs.append(cfg)

    for value in [
        3.50,
        4.00,
        4.25,
        4.50,
        4.75,
        5.00,
    ]:
        cfg = clone_anchor(
            f"SLICE_RR_{value:.2f}"
        )
        cfg["rr"] = value
        cfg["slice"] = "RR"
        configs.append(cfg)

    return configs


def local_grid_configs():
    configs = []
    counter = 0

    for breakdown_lb in [
        30,
        40,
        50,
        60,
    ]:
        for compression in [
            0.775,
            0.800,
            0.825,
        ]:
            for body in [
                1.15,
                1.25,
                1.35,
            ]:
                for range_atr in [
                    1.50,
                    1.60,
                    1.70,
                ]:
                    for context in [
                        "NONE",
                        "D_CLOSE_LT_EMA200",
                        "H4_CLOSE_LT_EMA200",
                    ]:
                        for rr in [
                            3.50,
                            4.00,
                            4.25,
                            4.50,
                            4.75,
                            5.00,
                        ]:
                            counter += 1

                            cfg = clone_anchor(
                                f"GRID_{counter:04d}"
                            )

                            cfg[
                                "breakdown_lb"
                            ] = breakdown_lb

                            cfg[
                                "compression_max"
                            ] = compression

                            cfg[
                                "body_atr_min"
                            ] = body

                            cfg[
                                "range_atr_min"
                            ] = range_atr

                            cfg[
                                "context"
                            ] = context

                            cfg[
                                "rr"
                            ] = rr

                            configs.append(cfg)

    return configs


# ============================================================
# DEEP OUTPUTS
# ============================================================

def stats_row(
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

        "period":
            period,

        "trades":
            s["trades"],

        "winners":
            s["winners"],

        "losers":
            s["losers"],

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


def period_rows(
    cfg,
    candles,
    indices,
):
    periods = [
        (
            "FULL_HISTORY",
            candles[
                0
            ]["time"],
            NOW,
        ),

        (
            "PRE_2010",
            candles[
                0
            ]["time"],
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
            candles[
                0
            ]["time"],
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

        row[
            "start_utc"
        ] = iso_utc(start)

        row[
            "end_utc"
        ] = iso_utc(end)

        rows.append(row)

    return rows


def cost_rows(
    cfg,
    candles,
    indices,
):
    rows = []

    for label, start, end in [
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
    ]:
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

            row[
                "cost_pips"
            ] = cost

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
    grouped = defaultdict(list)

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
                len(subset),

            "active_windows":
                len(active),

            "zero_trade_windows":
                len(subset)
                - len(active),

            "positive_windows_pct":
                round(
                    100.0
                    * len(
                        positive_all
                    )
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

    first_year = max(
        candles[
            0
        ]["time"].year,
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

        s = stats_from_trades(
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


def calendar_summary_rows(rows):
    grouped = defaultdict(list)

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

        negative = [
            row
            for row in subset
            if row[
                "negative"
            ]
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
                    * len(
                        positive_active
                    )
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
                        row[
                            "trades"
                        ]
                        for row in subset
                    ]),
                    4,
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
                    iso_utc(START),

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
                    len(m15),

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
                "Building completed H4/D state and M15 compression/breakdown cache",
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
                h4
            ),
        )

        daily_aligned = align_htf_to_m15(
            m15_times,
            build_htf_state(
                daily
            ),
        )

        features = build_features(
            m15,
            h4_aligned,
            daily_aligned,
        )

        # ----------------------------------------------------
        # Controls / parity
        # ----------------------------------------------------
        control_rows = []

        for cfg in [
            ANCHOR,
            RAW_CONTROL,
        ]:
            idx = signal_indices(
                cfg,
                features,
            )

            control_rows.append(
                evaluation_row(
                    cfg,
                    m15,
                    idx,
                )
            )

        write_csv(
            OUT_CONTROLS,
            control_rows,
        )

        anchor_row = next(
            row
            for row in control_rows
            if row[
                "config_id"
            ] == ANCHOR[
                "config_id"
            ]
        )

        raw_row = next(
            row
            for row in control_rows
            if row[
                "config_id"
            ] == RAW_CONTROL[
                "config_id"
            ]
        )

        write_csv(
            OUT_PARITY,
            [{
                "config_id":
                    ANCHOR[
                        "config_id"
                    ],

                "expected_full_trades":
                    69,

                "actual_full_trades":
                    anchor_row[
                        "full_trades"
                    ],

                "expected_pre2010_trades":
                    19,

                "actual_pre2010_trades":
                    anchor_row[
                        "pre2010_trades"
                    ],

                "expected_post2010_trades":
                    50,

                "actual_post2010_trades":
                    anchor_row[
                        "post2010_trades"
                    ],

                "status":
                    (
                        "MATCH"
                        if (
                            anchor_row[
                                "full_trades"
                            ] == 69
                            and anchor_row[
                                "pre2010_trades"
                            ] == 19
                            and anchor_row[
                                "post2010_trades"
                            ] == 50
                        )
                        else "CHECK_OR_NEWER_COMPLETED_TRADES"
                    ),
            }, {
                "config_id":
                    RAW_CONTROL[
                        "config_id"
                    ],

                "reference_full_trades":
                    131,

                "actual_full_trades":
                    raw_row[
                        "full_trades"
                    ],

                "status":
                    (
                        "MATCH"
                        if raw_row[
                            "full_trades"
                        ] == 131
                        else "CHECK_OR_NEWER_COMPLETED_TRADES"
                    ),
            }],
        )

        # ----------------------------------------------------
        # One-way
        # ----------------------------------------------------
        slice_configs = (
            one_way_configs()
        )

        slice_rows = []

        for i, cfg in enumerate(
            slice_configs,
            1,
        ):
            STATUS.update({
                "state":
                    "slices",

                "message": (
                    f"Slice {i}/{len(slice_configs)} "
                    f"{cfg['config_id']}"
                ),
            })

            idx = signal_indices(
                cfg,
                features,
            )

            row = evaluation_row(
                cfg,
                m15,
                idx,
            )

            row[
                "slice"
            ] = cfg[
                "slice"
            ]

            slice_rows.append(row)

        write_csv(
            OUT_SLICES,
            slice_rows,
        )

        # ----------------------------------------------------
        # Local interaction grid
        # ----------------------------------------------------
        grid_configs = (
            local_grid_configs()
        )

        grid_by_id = {
            cfg[
                "config_id"
            ]:
                cfg
            for cfg in grid_configs
        }

        grid_rows = []

        for i, cfg in enumerate(
            grid_configs,
            1,
        ):
            STATUS.update({
                "state":
                    "local_grid",

                "message": (
                    f"Grid {i}/{len(grid_configs)} "
                    f"{cfg['config_id']}"
                ),
            })

            idx = signal_indices(
                cfg,
                features,
            )

            grid_rows.append(
                evaluation_row(
                    cfg,
                    m15,
                    idx,
                )
            )

        grid_rows = sort_rows(
            grid_rows
        )

        write_csv(
            OUT_GRID,
            grid_rows,
        )

        # ----------------------------------------------------
        # Select finalists
        # ----------------------------------------------------
        eligible = [
            row
            for row in grid_rows
            if (
                55
                <= row[
                    "full_trades"
                ]
                <= 150
                and row[
                    "pre2010_trades"
                ] >= 12
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

        selected_rows = [
            anchor_row,
            raw_row,
        ]

        selected_ids = {
            anchor_row[
                "config_id"
            ],
            raw_row[
                "config_id"
            ],
        }

        for row in eligible:
            if row[
                "config_id"
            ] in selected_ids:
                continue

            selected_rows.append(
                row
            )

            selected_ids.add(
                row[
                    "config_id"
                ]
            )

            if len(
                selected_rows
            ) >= 10:
                break

        if len(
            selected_rows
        ) < 10:
            for row in grid_rows:
                if row[
                    "config_id"
                ] in selected_ids:
                    continue

                selected_rows.append(
                    row
                )

                selected_ids.add(
                    row[
                        "config_id"
                    ]
                )

                if len(
                    selected_rows
                ) >= 10:
                    break

        write_csv(
            OUT_FINALISTS,
            selected_rows,
        )

        selected_configs = [
            ANCHOR,
            RAW_CONTROL,
        ]

        for row in selected_rows[
            2:
        ]:
            cfg = grid_by_id.get(
                row[
                    "config_id"
                ]
            )

            if cfg is not None:
                selected_configs.append(
                    cfg
                )

        # ----------------------------------------------------
        # Deep validation
        # ----------------------------------------------------
        period_output = []
        cost_output = []
        rolling_output = []
        calendar_output = []
        trade_output = []

        for i, cfg in enumerate(
            selected_configs,
            1,
        ):
            STATUS.update({
                "state":
                    "deep_validation",

                "message": (
                    f"Deep validation "
                    f"{i}/{len(selected_configs)} "
                    f"{cfg['config_id']}"
                ),
            })

            idx = signal_indices(
                cfg,
                features,
            )

            period_output.extend(
                period_rows(
                    cfg,
                    m15,
                    idx,
                )
            )

            cost_output.extend(
                cost_rows(
                    cfg,
                    m15,
                    idx,
                )
            )

            rolling_output.extend(
                rolling_rows(
                    cfg,
                    m15,
                    idx,
                )
            )

            calendar_output.extend(
                calendar_rows(
                    cfg,
                    m15,
                    idx,
                )
            )

            full_trades = run_backtest(
                m15,
                idx,
                cfg["rr"],
                PRIMARY_COST_PIPS,
                m15[
                    0
                ]["time"],
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
                "USD/JPY M15 SHORT final confirmation complete",

            "anchor_full_trades":
                anchor_row[
                    "full_trades"
                ],

            "raw_control_full_trades":
                raw_row[
                    "full_trades"
                ],

            "slice_configs":
                len(
                    slice_configs
                ),

            "grid_configs":
                len(
                    grid_configs
                ),

            "deep_finalists":
                len(
                    selected_configs
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
            "USDJPY M15 SHORT Final Confirmation",

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
            "bearish":
                True,

            "compression_max":
                0.80,

            "body_atr_min":
                1.25,

            "range_atr_min":
                1.60,

            "breakdown_lb":
                40,

            "context":
                "previous completed daily close < EMA200",

            "rr":
                3.50,
        },

        "grid_configs":
            1944,

        "orders_supported":
            False,

        "trading_enabled":
            False,

        "routes": [
            "/usdjpy-m15-short-final-confirmation/status",
            "/usdjpy-m15-short-final-confirmation/results",
        ],
    })


@app.route(
    "/usdjpy-m15-short-final-confirmation/status"
)
def route_status():
    return jsonify(
        STATUS
    )


@app.route(
    "/usdjpy-m15-short-final-confirmation/results"
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
