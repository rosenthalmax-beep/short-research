
import os
import csv
import time
import bisect
import zipfile
import threading
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from flask import Flask, jsonify, send_file


# ============================================================
# USD/JPY M15 LONG — GEN2 TARGETED SWEEP / DISPLACEMENT SEARCH
#
# Seed from exhaustive pass:
#   exact bullish engulfing
#   large bullish displacement body
#   sweep / structural-low behaviour
#   body >= ~1.25 ATR14
#
# Seed result (1.0 pip adverse cost):
#   251 trades
#   PF 1.194
#   +35.71R
#
# Weak eras:
#   2010-13
#   2018-21
#
# Strong recent:
#   last 5Y PF ~1.63
#   last 2Y PF ~2.08
#
# Purpose:
#   Find whether a SIMPLE, economically sensible regime/context
#   filter can stabilize the setup without fitting dates.
#
# Targeted hypotheses:
#   1) Body ATR neighbourhood 1.10-1.50
#   2) Structure lookback 60-150
#   3) Sweep depth / reclaim
#   4) Prior 4h / 12h selloff
#   5) M15 volatility regime
#   6) H1 / H4 / Daily bullish regime
#   7) H1 / H4 / Daily ATR regime
#   8) Session / hour effects
#   9) Weekday exclusions
#  10) RR neighbourhood
#  11) Controlled two-family interactions
#
# Correctness:
#   - OANDA midpoint M15
#   - exact bullish engulfing
#   - signal timestamp = candle OPEN
#   - ATR14 Wilder/RMA, SMA seeded
#   - HTF state uses ONLY completed candles
#   - H1/H4 completion = next actual OANDA candle open
#   - Daily completion = next actual OANDA daily candle open
#   - dailyAlignment 17 America/New_York
#   - prior momentum ends at previous completed M15 candle
#   - stop = signal low - 10 ticks
#   - target based on REFERENCE signal close
#   - adverse historical entry = signal close + cost
#   - exits begin NEXT candle
#   - same-bar long tie:
#       if high closer to candle open => TARGET first
#       else STOP first
#   - pyramiding 0
#   - exact exit-candle signal eligible
#
# USDJPY:
#   tick = 0.001
#   pip = 0.01
#
# Development cost:
#   1.0 pip adverse
#
# Stress:
#   0.5 / 1.0 / 1.5 / 2.0 pips
#
# Validation:
#   4 eras
#   DEV 2010-17 / VALIDATION 2018-now
#   recent 5Y / 2Y
#   rolling 2Y / 3Y
#   overlap against seed
#
# READ ONLY. NEVER SENDS ORDERS.
# ============================================================


app = Flask(__name__)

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_BASE = os.getenv(
    "OANDA_API_URL",
    "https://api-fxtrade.oanda.com",
)

INSTRUMENT = "USD_JPY"

RESEARCH_FROM = datetime(
    2010, 1, 1, 0, 0,
    tzinfo=timezone.utc,
)

RESEARCH_TO = (
    datetime.now(timezone.utc)
    .replace(
        minute=0,
        second=0,
        microsecond=0,
    )
)

NY = ZoneInfo("America/New_York")

TICK_SIZE = 0.001
PIP_SIZE = 0.01
STOP_BUFFER_TICKS = 10

PRIMARY_COST_PIPS = 1.00
COST_PIPS_GRID = [
    0.50,
    1.00,
    1.50,
    2.00,
]

RR_VALUES = [
    2.75,
    3.00,
    3.25,
    3.50,
    3.75,
    4.00,
    4.25,
]

BODY_ATR_VALUES = [
    1.10,
    1.15,
    1.20,
    1.25,
    1.30,
    1.35,
    1.40,
    1.50,
]

STRUCTURE_LOOKBACKS = [
    60,
    90,
    120,
    150,
]

STRUCTURE_DISTANCES = [
    0.05,
    0.075,
    0.10,
    0.15,
    0.20,
    0.30,
]

MIN_TRADES_PRIMARY = 50


# ============================================================
# OUTPUTS
# ============================================================

OUTPUT_SEED = (
    "usdjpy_m15_long_gen2_seed.csv"
)

OUTPUT_LOCAL = (
    "usdjpy_m15_long_gen2_local_structure_body_rr.csv"
)

OUTPUT_SINGLE = (
    "usdjpy_m15_long_gen2_single_family.csv"
)

OUTPUT_INTERACTIONS = (
    "usdjpy_m15_long_gen2_interactions.csv"
)

OUTPUT_TOP = (
    "usdjpy_m15_long_gen2_top.csv"
)

OUTPUT_ERAS = (
    "usdjpy_m15_long_gen2_eras.csv"
)

OUTPUT_DEVVAL = (
    "usdjpy_m15_long_gen2_dev_validation.csv"
)

OUTPUT_RECENT = (
    "usdjpy_m15_long_gen2_recent.csv"
)

OUTPUT_ROLLING = (
    "usdjpy_m15_long_gen2_rolling.csv"
)

OUTPUT_ROLLING_SUMMARY = (
    "usdjpy_m15_long_gen2_rolling_summary.csv"
)

OUTPUT_OVERLAP = (
    "usdjpy_m15_long_gen2_overlap.csv"
)

OUTPUT_BEST_TRADES = (
    "usdjpy_m15_long_gen2_best_trades.csv"
)

OUTPUT_BUNDLE = (
    "usdjpy_m15_long_gen2_RESULTS.zip"
)

STATUS = {
    "state": "not_started",
    "message": "USDJPY M15 long Gen2 not started",
    "service": "USDJPY M15 Long Gen2 Targeted Sweep Displacement",
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

    if "." in value:
        left, right = value.split(".", 1)

        if "+" in right:
            fraction, offset = right.split("+", 1)
            fraction = fraction[:6].ljust(6, "0")
            value = left + "." + fraction + "+" + offset

    return datetime.fromisoformat(
        value
    ).astimezone(
        timezone.utc
    )


def years_ago_safe(dt, years):
    try:
        return dt.replace(
            year=dt.year - years
        )
    except ValueError:
        return dt.replace(
            month=2,
            day=28,
            year=dt.year - years,
        )


def month_start(dt):
    return datetime(
        dt.year,
        dt.month,
        1,
        tzinfo=timezone.utc,
    )


def add_months(dt, months):
    total = (
        dt.year * 12
        + dt.month - 1
        + months
    )

    return datetime(
        total // 12,
        total % 12 + 1,
        1,
        tzinfo=timezone.utc,
    )


def clone_config(base, label):
    result = {}

    for key, value in base.items():
        if isinstance(value, set):
            result[key] = set(value)
        else:
            result[key] = value

    result["label"] = label
    return result


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
            "error": "Output not ready yet",
            "path": path,
        }), 404

    return send_file(
        os.path.abspath(path),
        as_attachment=True,
        download_name=os.path.basename(path),
    )


def build_results_bundle():
    files = [
        OUTPUT_SEED,
        OUTPUT_LOCAL,
        OUTPUT_SINGLE,
        OUTPUT_INTERACTIONS,
        OUTPUT_TOP,
        OUTPUT_ERAS,
        OUTPUT_DEVVAL,
        OUTPUT_RECENT,
        OUTPUT_ROLLING,
        OUTPUT_ROLLING_SUMMARY,
        OUTPUT_OVERLAP,
        OUTPUT_BEST_TRADES,
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


# ============================================================
# OANDA FETCH
# ============================================================

def oanda_headers():
    if not OANDA_TOKEN:
        raise RuntimeError(
            "OANDA_TOKEN is not configured"
        )

    return {
        "Authorization":
            "Bearer " + OANDA_TOKEN.strip(),
        "Content-Type":
            "application/json",
    }


def fetch_chunk(
    granularity,
    start,
    end,
    daily_alignment=False,
):
    url = (
        f"{OANDA_BASE}"
        f"/v3/instruments/"
        f"{INSTRUMENT}/candles"
    )

    params = {
        "price": "M",
        "granularity": granularity,
        "smooth": "false",
        "from": iso_utc(start),
        "to": iso_utc(end),
        "includeFirst": "true",
    }

    if daily_alignment:
        params["dailyAlignment"] = 17
        params["alignmentTimezone"] = (
            "America/New_York"
        )

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
    daily_alignment=False,
):
    cursor = start
    by_time = {}
    chunk_number = 0

    while cursor < end:
        chunk_number += 1

        chunk_end = min(
            cursor + timedelta(days=chunk_days),
            end,
        )

        STATUS.update({
            "state": "fetching",
            "message": (
                f"Fetching {granularity} chunk "
                f"{chunk_number}: "
                f"{iso_utc(cursor)} -> "
                f"{iso_utc(chunk_end)}"
            ),
        })

        rows = fetch_chunk(
            granularity,
            cursor,
            chunk_end,
            daily_alignment=daily_alignment,
        )

        for row in rows:
            by_time[row["time"]] = row

        cursor = chunk_end
        time.sleep(0.02)

    result = list(by_time.values())
    result.sort(key=lambda row: row["time"])

    return result


# ============================================================
# INDICATORS
# ============================================================

def true_ranges(candles):
    result = [None] * len(candles)

    for i, candle in enumerate(candles):
        if i == 0:
            result[i] = (
                candle["high"]
                - candle["low"]
            )
        else:
            previous_close = candles[
                i - 1
            ]["close"]

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
    result = [None] * len(values)

    if len(values) < length:
        return result

    seed = values[:length]

    if any(
        value is None
        for value in seed
    ):
        return result

    result[length - 1] = (
        sum(seed) / length
    )

    for i in range(
        length,
        len(values),
    ):
        if (
            values[i] is None
            or result[i - 1] is None
        ):
            continue

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


def ema(values, length):
    result = [None] * len(values)

    if len(values) < length:
        return result

    seed = values[:length]

    if any(v is None for v in seed):
        return result

    result[length - 1] = (
        sum(seed) / length
    )

    alpha = 2.0 / (length + 1.0)

    for i in range(
        length,
        len(values),
    ):
        if (
            values[i] is None
            or result[i - 1] is None
        ):
            continue

        result[i] = (
            alpha * values[i]
            +
            (1.0 - alpha)
            * result[i - 1]
        )

    return result


def sma(values, length):
    result = [None] * len(values)
    queue = []
    running = 0.0

    for i, value in enumerate(values):
        queue.append(value)

        if value is not None:
            running += value

        if len(queue) > length:
            removed = queue.pop(0)

            if removed is not None:
                running -= removed

        if (
            len(queue) == length
            and all(
                item is not None
                for item in queue
            )
        ):
            result[i] = (
                running / length
            )

    return result


def bullish_engulfing(
    candles,
    index,
):
    if index < 1:
        return False

    previous = candles[
        index - 1
    ]

    current = candles[
        index
    ]

    return (
        previous["close"]
        <
        previous["open"]
        and
        current["close"]
        >
        current["open"]
        and
        current["open"]
        <=
        previous["close"]
        and
        current["close"]
        >=
        previous["open"]
    )


# ============================================================
# HTF STATE — COMPLETED ONLY
# ============================================================

H1_EMAS = [
    20,
    50,
    100,
    200,
]

H4_EMAS = [
    20,
    50,
    100,
    200,
]

DAILY_EMAS = [
    20,
    50,
    100,
    200,
    300,
]


def build_htf_state(
    candles,
    ema_lengths,
):
    closes = [
        candle["close"]
        for candle in candles
    ]

    atr = atr14(candles)
    atr_mean50 = sma(
        atr,
        50,
    )

    ema_map = {
        length:
            ema(closes, length)
        for length in ema_lengths
    }

    rows = []

    for i, candle in enumerate(candles):
        complete_at = (
            candles[i + 1]["time"]
            if i + 1 < len(candles)
            else None
        )

        atr_ratio = None

        if (
            atr[i] is not None
            and
            atr_mean50[i] is not None
            and
            atr_mean50[i] > 0
        ):
            atr_ratio = (
                atr[i]
                /
                atr_mean50[i]
            )

        rows.append({
            "time":
                candle["time"],
            "complete_at":
                complete_at,
            "open":
                candle["open"],
            "high":
                candle["high"],
            "low":
                candle["low"],
            "close":
                candle["close"],
            "atr14":
                atr[i],
            "atr_ratio_50":
                atr_ratio,
            "emas":
                {
                    length:
                        ema_map[
                            length
                        ][i]
                    for length
                    in ema_lengths
                },
        })

    return rows


def previous_completed_state(
    rows,
    completion_times,
    signal_time,
):
    position = bisect.bisect_right(
        completion_times,
        signal_time,
    ) - 1

    if position < 0:
        return None

    return rows[position]


# ============================================================
# SIGNAL FEATURES
# ============================================================

def build_signal_cache(
    m15,
    m15_atr,
    h1_state,
    h4_state,
    daily_state,
):
    signals = []

    m15_atr_mean50 = sma(
        m15_atr,
        50,
    )

    h1_rows = [
        row
        for row in h1_state
        if row["complete_at"] is not None
    ]

    h4_rows = [
        row
        for row in h4_state
        if row["complete_at"] is not None
    ]

    daily_rows = [
        row
        for row in daily_state
        if row["complete_at"] is not None
    ]

    h1_times = [
        row["complete_at"]
        for row in h1_rows
    ]

    h4_times = [
        row["complete_at"]
        for row in h4_rows
    ]

    daily_times = [
        row["complete_at"]
        for row in daily_rows
    ]

    max_lookback = max(
        max(STRUCTURE_LOOKBACKS),
        97,
    )

    for i in range(
        max(14, max_lookback),
        len(m15),
    ):
        if not bullish_engulfing(
            m15,
            i,
        ):
            continue

        current = m15[i]
        previous = m15[i - 1]
        atr = m15_atr[i]

        if atr is None or atr <= 0:
            continue

        body = (
            current["close"]
            - current["open"]
        )

        previous_body = abs(
            previous["close"]
            - previous["open"]
        )

        body_ratio = (
            body / previous_body
            if previous_body > 0
            else 999.0
        )

        candle_range = (
            current["high"]
            - current["low"]
        )

        body_atr = body / atr
        range_atr = (
            candle_range / atr
        )

        close_location = (
            (
                current["close"]
                -
                current["low"]
            )
            /
            candle_range
            if candle_range > 0
            else 0.0
        )

        lower_wick = (
            min(
                current["open"],
                current["close"],
            )
            -
            current["low"]
        )

        lower_wick_body = (
            lower_wick / body
            if body > 0
            else 0.0
        )

        stop = (
            current["low"]
            -
            STOP_BUFFER_TICKS
            * TICK_SIZE
        )

        stop_atr = (
            current["close"]
            -
            stop
        ) / atr

        m15_atr_ratio = None

        if (
            m15_atr_mean50[i] is not None
            and
            m15_atr_mean50[i] > 0
        ):
            m15_atr_ratio = (
                atr
                /
                m15_atr_mean50[i]
            )

        # Strictly PRE-SIGNAL momentum.
        prior_close = (
            m15[i - 1]["close"]
        )

        momentum_4h = (
            prior_close
            -
            m15[i - 17]["close"]
        ) / atr

        momentum_12h = (
            prior_close
            -
            m15[i - 49]["close"]
        ) / atr

        momentum_24h = (
            prior_close
            -
            m15[i - 97]["close"]
        ) / atr

        structure_distance = {}
        structure_sweep_depth = {}
        structure_swept = {}
        structure_reclaimed = {}

        for lookback in STRUCTURE_LOOKBACKS:
            prior_low = min(
                candle["low"]
                for candle
                in m15[
                    i - lookback:i
                ]
            )

            distance = (
                abs(
                    current["low"]
                    -
                    prior_low
                )
                /
                atr
            )

            sweep_depth = (
                (
                    prior_low
                    -
                    current["low"]
                )
                /
                atr
                if current["low"] < prior_low
                else 0.0
            )

            swept = (
                current["low"]
                <
                prior_low
            )

            reclaimed = (
                swept
                and
                current["close"]
                >
                prior_low
            )

            structure_distance[
                lookback
            ] = distance

            structure_sweep_depth[
                lookback
            ] = sweep_depth

            structure_swept[
                lookback
            ] = swept

            structure_reclaimed[
                lookback
            ] = reclaimed

        ny = current[
            "time"
        ].astimezone(NY)

        h1_prev = previous_completed_state(
            h1_rows,
            h1_times,
            current["time"],
        )

        h4_prev = previous_completed_state(
            h4_rows,
            h4_times,
            current["time"],
        )

        daily_prev = previous_completed_state(
            daily_rows,
            daily_times,
            current["time"],
        )

        signals.append({
            "signal_index":
                i,
            "time":
                current["time"],
            "body_ratio":
                body_ratio,
            "body_atr":
                body_atr,
            "range_atr":
                range_atr,
            "close_location":
                close_location,
            "lower_wick_body":
                lower_wick_body,
            "stop_atr":
                stop_atr,
            "m15_atr_ratio":
                m15_atr_ratio,
            "momentum_4h_atr":
                momentum_4h,
            "momentum_12h_atr":
                momentum_12h,
            "momentum_24h_atr":
                momentum_24h,
            "structure_distance_atr":
                structure_distance,
            "structure_sweep_depth_atr":
                structure_sweep_depth,
            "structure_swept":
                structure_swept,
            "structure_reclaimed":
                structure_reclaimed,
            "ny_hour":
                ny.hour,
            "ny_weekday":
                ny.weekday(),
            "h1_prev":
                h1_prev,
            "h4_prev":
                h4_prev,
            "daily_prev":
                daily_prev,
        })

    return signals


# ============================================================
# CONFIG
# ============================================================

SEED = {
    "label":
        "SEED_BODY1.25_SWEEP_DISPLACEMENT",

    "minimum_body_atr":
        1.25,

    "structure_lookback":
        90,

    "maximum_structure_distance_atr":
        999.0,

    "require_structure_sweep":
        True,

    "require_structure_reclaim":
        False,

    "minimum_sweep_depth_atr":
        None,

    "maximum_sweep_depth_atr":
        None,

    "minimum_range_atr":
        None,

    "minimum_close_location":
        None,

    "minimum_lower_wick_body":
        None,

    "minimum_m15_atr_ratio":
        None,

    "maximum_m15_atr_ratio":
        None,

    "minimum_momentum_4h_atr":
        None,

    "maximum_momentum_4h_atr":
        None,

    "minimum_momentum_12h_atr":
        None,

    "maximum_momentum_12h_atr":
        None,

    "minimum_momentum_24h_atr":
        None,

    "maximum_momentum_24h_atr":
        None,

    "included_ny_hours":
        None,

    "excluded_ny_hours":
        set(),

    "excluded_weekdays":
        set(),

    "h1_close_above_ema":
        None,

    "h1_fast_ema":
        None,

    "h1_slow_ema":
        None,

    "minimum_h1_atr_ratio":
        None,

    "maximum_h1_atr_ratio":
        None,

    "h4_close_above_ema":
        None,

    "h4_fast_ema":
        None,

    "h4_slow_ema":
        None,

    "minimum_h4_atr_ratio":
        None,

    "maximum_h4_atr_ratio":
        None,

    "daily_close_above_ema":
        None,

    "daily_fast_ema":
        None,

    "daily_slow_ema":
        None,

    "minimum_daily_atr_ratio":
        None,

    "maximum_daily_atr_ratio":
        None,

    "reward_risk":
        3.50,
}


def state_close_above_ema(
    state,
    length,
):
    if state is None:
        return False

    value = state[
        "emas"
    ].get(length)

    return (
        value is not None
        and
        state["close"] > value
    )


def state_bull_alignment(
    state,
    fast,
    slow,
):
    if state is None:
        return False

    fast_value = state[
        "emas"
    ].get(fast)

    slow_value = state[
        "emas"
    ].get(slow)

    return (
        fast_value is not None
        and
        slow_value is not None
        and
        fast_value > slow_value
    )


def state_atr_ratio_passes(
    state,
    minimum,
    maximum,
):
    if (
        minimum is None
        and
        maximum is None
    ):
        return True

    if (
        state is None
        or
        state[
            "atr_ratio_50"
        ] is None
    ):
        return False

    value = state[
        "atr_ratio_50"
    ]

    if (
        minimum is not None
        and value < minimum
    ):
        return False

    if (
        maximum is not None
        and value > maximum
    ):
        return False

    return True


def signal_passes(
    signal,
    config,
):
    if (
        signal["body_atr"]
        <
        config["minimum_body_atr"]
    ):
        return False

    lookback = (
        config[
            "structure_lookback"
        ]
    )

    if (
        signal[
            "structure_distance_atr"
        ][lookback]
        >
        config[
            "maximum_structure_distance_atr"
        ]
    ):
        return False

    if (
        config[
            "require_structure_sweep"
        ]
        and
        not signal[
            "structure_swept"
        ][lookback]
    ):
        return False

    if (
        config[
            "require_structure_reclaim"
        ]
        and
        not signal[
            "structure_reclaimed"
        ][lookback]
    ):
        return False

    sweep_depth = signal[
        "structure_sweep_depth_atr"
    ][lookback]

    if (
        config[
            "minimum_sweep_depth_atr"
        ]
        is not None
        and
        sweep_depth
        <
        config[
            "minimum_sweep_depth_atr"
        ]
    ):
        return False

    if (
        config[
            "maximum_sweep_depth_atr"
        ]
        is not None
        and
        sweep_depth
        >
        config[
            "maximum_sweep_depth_atr"
        ]
    ):
        return False

    if (
        config[
            "minimum_range_atr"
        ]
        is not None
        and
        signal[
            "range_atr"
        ]
        <
        config[
            "minimum_range_atr"
        ]
    ):
        return False

    if (
        config[
            "minimum_close_location"
        ]
        is not None
        and
        signal[
            "close_location"
        ]
        <
        config[
            "minimum_close_location"
        ]
    ):
        return False

    if (
        config[
            "minimum_lower_wick_body"
        ]
        is not None
        and
        signal[
            "lower_wick_body"
        ]
        <
        config[
            "minimum_lower_wick_body"
        ]
    ):
        return False

    if (
        config[
            "minimum_m15_atr_ratio"
        ]
        is not None
    ):
        value = signal[
            "m15_atr_ratio"
        ]

        if (
            value is None
            or
            value
            <
            config[
                "minimum_m15_atr_ratio"
            ]
        ):
            return False

    if (
        config[
            "maximum_m15_atr_ratio"
        ]
        is not None
    ):
        value = signal[
            "m15_atr_ratio"
        ]

        if (
            value is None
            or
            value
            >
            config[
                "maximum_m15_atr_ratio"
            ]
        ):
            return False

    for horizon in [
        "4h",
        "12h",
        "24h",
    ]:
        value = signal[
            f"momentum_{horizon}_atr"
        ]

        minimum = config[
            f"minimum_momentum_{horizon}_atr"
        ]

        maximum = config[
            f"maximum_momentum_{horizon}_atr"
        ]

        if (
            minimum is not None
            and
            value < minimum
        ):
            return False

        if (
            maximum is not None
            and
            value > maximum
        ):
            return False

    included = config[
        "included_ny_hours"
    ]

    if (
        included is not None
        and
        signal["ny_hour"]
        not in included
    ):
        return False

    if (
        signal["ny_hour"]
        in config[
            "excluded_ny_hours"
        ]
    ):
        return False

    if (
        signal["ny_weekday"]
        in config[
            "excluded_weekdays"
        ]
    ):
        return False

    h1 = signal["h1_prev"]

    if (
        config[
            "h1_close_above_ema"
        ]
        is not None
        and
        not state_close_above_ema(
            h1,
            config[
                "h1_close_above_ema"
            ],
        )
    ):
        return False

    if (
        config[
            "h1_fast_ema"
        ]
        is not None
        and
        config[
            "h1_slow_ema"
        ]
        is not None
        and
        not state_bull_alignment(
            h1,
            config[
                "h1_fast_ema"
            ],
            config[
                "h1_slow_ema"
            ],
        )
    ):
        return False

    if not state_atr_ratio_passes(
        h1,
        config[
            "minimum_h1_atr_ratio"
        ],
        config[
            "maximum_h1_atr_ratio"
        ],
    ):
        return False

    h4 = signal["h4_prev"]

    if (
        config[
            "h4_close_above_ema"
        ]
        is not None
        and
        not state_close_above_ema(
            h4,
            config[
                "h4_close_above_ema"
            ],
        )
    ):
        return False

    if (
        config[
            "h4_fast_ema"
        ]
        is not None
        and
        config[
            "h4_slow_ema"
        ]
        is not None
        and
        not state_bull_alignment(
            h4,
            config[
                "h4_fast_ema"
            ],
            config[
                "h4_slow_ema"
            ],
        )
    ):
        return False

    if not state_atr_ratio_passes(
        h4,
        config[
            "minimum_h4_atr_ratio"
        ],
        config[
            "maximum_h4_atr_ratio"
        ],
    ):
        return False

    daily = signal["daily_prev"]

    if (
        config[
            "daily_close_above_ema"
        ]
        is not None
        and
        not state_close_above_ema(
            daily,
            config[
                "daily_close_above_ema"
            ],
        )
    ):
        return False

    if (
        config[
            "daily_fast_ema"
        ]
        is not None
        and
        config[
            "daily_slow_ema"
        ]
        is not None
        and
        not state_bull_alignment(
            daily,
            config[
                "daily_fast_ema"
            ],
            config[
                "daily_slow_ema"
            ],
        )
    ):
        return False

    if not state_atr_ratio_passes(
        daily,
        config[
            "minimum_daily_atr_ratio"
        ],
        config[
            "maximum_daily_atr_ratio"
        ],
    ):
        return False

    return True


# ============================================================
# OUTCOME CACHE
# ============================================================

def compute_trade_outcome(
    candles,
    signal_index,
    reward_risk,
    cost_pips,
):
    signal = candles[
        signal_index
    ]

    reference_entry = (
        signal["close"]
    )

    stop = (
        signal["low"]
        -
        STOP_BUFFER_TICKS
        * TICK_SIZE
    )

    reference_risk = (
        reference_entry - stop
    )

    if reference_risk <= 0:
        return None

    target = (
        reference_entry
        +
        reward_risk
        * reference_risk
    )

    backtest_entry = (
        reference_entry
        +
        cost_pips
        * PIP_SIZE
    )

    actual_risk = (
        backtest_entry - stop
    )

    if actual_risk <= 0:
        return None

    for j in range(
        signal_index + 1,
        len(candles),
    ):
        candle = candles[j]

        hit_stop = (
            candle["low"] <= stop
        )

        hit_target = (
            candle["high"] >= target
        )

        if hit_stop and hit_target:
            distance_high = abs(
                candle["high"]
                - candle["open"]
            )

            distance_low = abs(
                candle["open"]
                - candle["low"]
            )

            if (
                distance_high
                <
                distance_low
            ):
                exit_price = target
                exit_reason = "TARGET"
            else:
                exit_price = stop
                exit_reason = "STOP"

        elif hit_stop:
            exit_price = stop
            exit_reason = "STOP"

        elif hit_target:
            exit_price = target
            exit_reason = "TARGET"

        else:
            continue

        result_r = (
            exit_price
            - backtest_entry
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
            "reference_entry":
                reference_entry,
            "backtest_entry":
                backtest_entry,
            "stop":
                stop,
            "target":
                target,
            "exit_reason":
                exit_reason,
            "result_r":
                result_r,
            "reward_risk":
                reward_risk,
            "cost_pips":
                cost_pips,
        }

    return None


def build_outcome_cache(
    candles,
    signals,
):
    cache = {}

    total = (
        len(signals)
        *
        len(RR_VALUES)
        *
        len(COST_PIPS_GRID)
    )

    done = 0

    for signal in signals:
        index = signal[
            "signal_index"
        ]

        for rr in RR_VALUES:
            for cost in COST_PIPS_GRID:
                done += 1

                if done % 1000 == 0:
                    STATUS.update({
                        "state":
                            "precomputing",
                        "message": (
                            "Caching outcomes "
                            f"{done}/{total}"
                        ),
                        "outcomes_done":
                            done,
                        "outcomes_total":
                            total,
                    })

                cache[
                    (
                        index,
                        rr,
                        cost,
                    )
                ] = compute_trade_outcome(
                    candles,
                    index,
                    rr,
                    cost,
                )

    return cache


# ============================================================
# CONFIG CACHE / BACKTEST
# ============================================================

def config_signature(config):
    return tuple(
        sorted(
            (
                key,
                tuple(sorted(value))
                if isinstance(value, set)
                else value,
            )
            for key, value in config.items()
            if key != "label"
        )
    )


CANDIDATE_CACHE = {}


def qualifying_candidates(
    signals,
    config,
):
    key = config_signature(config)

    if key in CANDIDATE_CACHE:
        return CANDIDATE_CACHE[key]

    candidates = [
        signal
        for signal in signals
        if signal_passes(
            signal,
            config,
        )
    ]

    CANDIDATE_CACHE[key] = candidates
    return candidates


def run_config_cached(
    signals,
    cache,
    config,
    cost_pips,
    start=None,
    end=None,
):
    candidates = qualifying_candidates(
        signals,
        config,
    )

    if (
        start is not None
        or
        end is not None
    ):
        times = [
            signal["time"]
            for signal in candidates
        ]

        left = (
            0
            if start is None
            else
            bisect.bisect_left(
                times,
                start,
            )
        )

        right = (
            len(candidates)
            if end is None
            else
            bisect.bisect_left(
                times,
                end,
            )
        )

        candidates = candidates[
            left:right
        ]

    indices = [
        signal["signal_index"]
        for signal in candidates
    ]

    trades = []
    position = 0

    while position < len(candidates):
        signal = candidates[position]

        trade = cache.get(
            (
                signal[
                    "signal_index"
                ],
                config[
                    "reward_risk"
                ],
                cost_pips,
            )
        )

        if trade is None:
            position += 1
            continue

        trades.append(dict(trade))

        position = bisect.bisect_left(
            indices,
            trade["exit_index"],
            lo=position + 1,
        )

    return trades


# ============================================================
# STATS
# ============================================================

def stats_from_trades(trades):
    results = [
        float(
            trade["result_r"]
        )
        for trade in trades
    ]

    winners = [
        r for r in results
        if r > 0
    ]

    losers = [
        r for r in results
        if r < 0
    ]

    gross_profit = sum(winners)
    gross_loss = abs(sum(losers))
    total_r = sum(results)

    pf = (
        gross_profit / gross_loss
        if gross_loss > 0
        else (
            999.0
            if gross_profit > 0
            else 0.0
        )
    )

    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    streak = 0
    longest = 0

    for result in results:
        equity += result

        peak = max(
            peak,
            equity,
        )

        max_dd = min(
            max_dd,
            equity - peak,
        )

        if result < 0:
            streak += 1
            longest = max(
                longest,
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
                /
                len(results)
                *
                100.0
                if results
                else 0.0
            ),
        "profit_factor":
            pf,
        "total_r":
            total_r,
        "expectancy_r":
            (
                total_r
                /
                len(results)
                if results
                else 0.0
            ),
        "max_drawdown_r":
            max_dd,
        "longest_loss_streak":
            longest,
    }


def result_row(
    family,
    config,
    cost,
    trades,
):
    stats = stats_from_trades(
        trades
    )

    row = {
        "family":
            family,
        "candidate":
            config["label"],
        "cost_pips":
            cost,
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
            stats["longest_loss_streak"],
    }

    for key, value in config.items():
        if key == "label":
            continue

        if isinstance(value, set):
            row[key] = ",".join(
                str(item)
                for item in sorted(value)
            )
        else:
            row[key] = value

    return row


# ============================================================
# LOCAL SEED NEIGHBOURHOOD
# ============================================================

def local_configs():
    rows = []

    counter = 0

    for body_atr in BODY_ATR_VALUES:
        for lookback in STRUCTURE_LOOKBACKS:
            for distance in STRUCTURE_DISTANCES:
                for rr in RR_VALUES:
                    counter += 1

                    config = clone_config(
                        SEED,
                        (
                            f"LOCAL_{counter:04d}_"
                            f"BA{body_atr:.2f}_"
                            f"S{lookback}_"
                            f"D{distance:.3f}_"
                            f"RR{rr:.2f}"
                        ),
                    )

                    config[
                        "minimum_body_atr"
                    ] = body_atr

                    config[
                        "structure_lookback"
                    ] = lookback

                    config[
                        "maximum_structure_distance_atr"
                    ] = distance

                    config[
                        "reward_risk"
                    ] = rr

                    rows.append(
                        (
                            "LOCAL",
                            config,
                        )
                    )

    return rows


# ============================================================
# TARGETED SINGLE FAMILIES AROUND SEED
# ============================================================

def single_family_configs():
    rows = []

    def add(
        family,
        label,
        mutator,
    ):
        config = clone_config(
            SEED,
            label,
        )

        mutator(config)
        rows.append(
            (
                family,
                config,
            )
        )

    # Reclaim vs sweep-only.
    add(
        "RECLAIM",
        "REQUIRE_RECLAIM",
        lambda c:
            c.__setitem__(
                "require_structure_reclaim",
                True,
            ),
    )

    # Sweep depth.
    for value in [
        0.025,
        0.05,
        0.075,
        0.10,
        0.15,
        0.20,
        0.30,
    ]:
        add(
            "SWEEP_DEPTH_MIN",
            f"SWEEP_DEPTH_MIN_{value:.3f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_sweep_depth_atr",
                    v,
                ),
        )

    for value in [
        0.05,
        0.075,
        0.10,
        0.15,
        0.20,
        0.30,
        0.50,
    ]:
        add(
            "SWEEP_DEPTH_MAX",
            f"SWEEP_DEPTH_MAX_{value:.3f}",
            lambda c, v=value:
                c.__setitem__(
                    "maximum_sweep_depth_atr",
                    v,
                ),
        )

    # Range / close / wick.
    for value in [
        1.20,
        1.40,
        1.60,
        1.80,
        2.00,
    ]:
        add(
            "RANGE_ATR",
            f"RANGE_ATR_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_range_atr",
                    v,
                ),
        )

    for value in [
        0.60,
        0.65,
        0.70,
        0.75,
        0.80,
        0.85,
    ]:
        add(
            "CLOSE_LOCATION",
            f"CLOSE_LOC_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_close_location",
                    v,
                ),
        )

    for value in [
        0.05,
        0.10,
        0.20,
        0.30,
        0.50,
    ]:
        add(
            "LOWER_WICK",
            f"LOWER_WICK_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_lower_wick_body",
                    v,
                ),
        )

    # M15 volatility regime.
    for value in [
        0.80,
        0.90,
        1.00,
        1.10,
        1.20,
        1.30,
    ]:
        add(
            "M15_ATR_MIN",
            f"M15_ATR_MIN_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_m15_atr_ratio",
                    v,
                ),
        )

    for value in [
        0.90,
        1.00,
        1.10,
        1.20,
        1.30,
        1.50,
    ]:
        add(
            "M15_ATR_MAX",
            f"M15_ATR_MAX_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "maximum_m15_atr_ratio",
                    v,
                ),
        )

    # Prior selloff / momentum.
    for horizon in [
        "4h",
        "12h",
        "24h",
    ]:
        for value in [
            -3.0,
            -2.0,
            -1.5,
            -1.0,
            -0.5,
            0.0,
        ]:
            key = (
                f"maximum_momentum_"
                f"{horizon}_atr"
            )

            add(
                f"{horizon}_SELL_OFF_MAX",
                (
                    f"{horizon}_MOM_MAX_"
                    f"{value:+.2f}"
                ),
                lambda c,
                k=key,
                v=value:
                    c.__setitem__(
                        k,
                        v,
                    ),
            )

        for value in [
            -2.0,
            -1.5,
            -1.0,
            -0.5,
            0.0,
            0.5,
        ]:
            key = (
                f"minimum_momentum_"
                f"{horizon}_atr"
            )

            add(
                f"{horizon}_MOM_MIN",
                (
                    f"{horizon}_MOM_MIN_"
                    f"{value:+.2f}"
                ),
                lambda c,
                k=key,
                v=value:
                    c.__setitem__(
                        k,
                        v,
                    ),
            )

    # Sessions.
    sessions = {
        "NY_00_04":
            {0, 1, 2, 3, 4},
        "NY_00_06":
            {0, 1, 2, 3, 4, 5, 6},
        "NY_02_06":
            {2, 3, 4, 5, 6},
        "NY_07_11":
            {7, 8, 9, 10, 11},
        "NY_08_12":
            {8, 9, 10, 11, 12},
        "NY_08_15":
            {8, 9, 10, 11, 12, 13, 14, 15},
        "NY_12_16":
            {12, 13, 14, 15, 16},
        "NY_18_23":
            {18, 19, 20, 21, 22, 23},
    }

    for label, hours in sessions.items():
        add(
            "SESSION_INCLUDE",
            label,
            lambda c, h=hours:
                c.__setitem__(
                    "included_ny_hours",
                    set(h),
                ),
        )

    for hour in range(24):
        add(
            "HOUR_EXCLUDE",
            f"EX_H{hour:02d}",
            lambda c, h=hour:
                c.__setitem__(
                    "excluded_ny_hours",
                    {h},
                ),
        )

    for day, name in [
        (0, "MON"),
        (1, "TUE"),
        (2, "WED"),
        (3, "THU"),
        (4, "FRI"),
    ]:
        add(
            "WEEKDAY_EXCLUDE",
            f"EX_{name}",
            lambda c, d=day:
                c.__setitem__(
                    "excluded_weekdays",
                    {d},
                ),
        )

    # H1 trend.
    for length in [
        20,
        50,
        100,
        200,
    ]:
        add(
            "H1_CLOSE_EMA",
            f"H1_CLOSE_GT_EMA{length}",
            lambda c, v=length:
                c.__setitem__(
                    "h1_close_above_ema",
                    v,
                ),
        )

    for fast, slow in [
        (20, 50),
        (20, 100),
        (50, 100),
        (50, 200),
    ]:
        def mutate_h1(
            c,
            f=fast,
            s=slow,
        ):
            c["h1_fast_ema"] = f
            c["h1_slow_ema"] = s

        add(
            "H1_ALIGNMENT",
            f"H1_EMA{fast}_GT_{slow}",
            mutate_h1,
        )

    for value in [
        0.80,
        0.90,
        1.00,
        1.10,
        1.20,
    ]:
        add(
            "H1_ATR_MIN",
            f"H1_ATR_MIN_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_h1_atr_ratio",
                    v,
                ),
        )

    for value in [
        0.90,
        1.00,
        1.10,
        1.20,
        1.30,
    ]:
        add(
            "H1_ATR_MAX",
            f"H1_ATR_MAX_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "maximum_h1_atr_ratio",
                    v,
                ),
        )

    # H4 trend.
    for length in [
        20,
        50,
        100,
        200,
    ]:
        add(
            "H4_CLOSE_EMA",
            f"H4_CLOSE_GT_EMA{length}",
            lambda c, v=length:
                c.__setitem__(
                    "h4_close_above_ema",
                    v,
                ),
        )

    for fast, slow in [
        (20, 50),
        (20, 100),
        (50, 100),
        (50, 200),
    ]:
        def mutate_h4(
            c,
            f=fast,
            s=slow,
        ):
            c["h4_fast_ema"] = f
            c["h4_slow_ema"] = s

        add(
            "H4_ALIGNMENT",
            f"H4_EMA{fast}_GT_{slow}",
            mutate_h4,
        )

    for value in [
        0.80,
        0.90,
        1.00,
        1.10,
        1.20,
    ]:
        add(
            "H4_ATR_MIN",
            f"H4_ATR_MIN_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_h4_atr_ratio",
                    v,
                ),
        )

    for value in [
        0.90,
        1.00,
        1.10,
        1.20,
        1.30,
    ]:
        add(
            "H4_ATR_MAX",
            f"H4_ATR_MAX_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "maximum_h4_atr_ratio",
                    v,
                ),
        )

    # Daily trend.
    for length in [
        20,
        50,
        100,
        200,
        300,
    ]:
        add(
            "DAILY_CLOSE_EMA",
            f"D_CLOSE_GT_EMA{length}",
            lambda c, v=length:
                c.__setitem__(
                    "daily_close_above_ema",
                    v,
                ),
        )

    for fast, slow in [
        (20, 50),
        (20, 100),
        (50, 100),
        (50, 200),
        (100, 200),
    ]:
        def mutate_daily(
            c,
            f=fast,
            s=slow,
        ):
            c["daily_fast_ema"] = f
            c["daily_slow_ema"] = s

        add(
            "DAILY_ALIGNMENT",
            f"D_EMA{fast}_GT_{slow}",
            mutate_daily,
        )

    for value in [
        0.80,
        0.90,
        1.00,
        1.10,
        1.20,
    ]:
        add(
            "DAILY_ATR_MIN",
            f"D_ATR_MIN_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_daily_atr_ratio",
                    v,
                ),
        )

    for value in [
        0.90,
        1.00,
        1.10,
        1.20,
        1.30,
    ]:
        add(
            "DAILY_ATR_MAX",
            f"D_ATR_MAX_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "maximum_daily_atr_ratio",
                    v,
                ),
        )

    # RR.
    for rr in RR_VALUES:
        add(
            "RR",
            f"RR_{rr:.2f}",
            lambda c, v=rr:
                c.__setitem__(
                    "reward_risk",
                    v,
                ),
        )

    return rows


# ============================================================
# CONTROLLED INTERACTIONS
# ============================================================

def build_controlled_interactions(
    primary_rows,
    config_lookup,
):
    family_best = {}

    for row in primary_rows:
        family = row["family"]

        if family in family_best:
            continue

        if (
            float(
                row["profit_factor"]
            )
            >= 1.12
            and
            int(
                row["trades"]
            )
            >= MIN_TRADES_PRIMARY
        ):
            family_best[
                family
            ] = row

    ranked = sorted(
        family_best.items(),
        key=lambda item: (
            float(
                item[1][
                    "profit_factor"
                ]
            ),
            float(
                item[1][
                    "expectancy_r"
                ]
            ),
        ),
        reverse=True,
    )[:10]

    pool = [
        (
            family,
            config_lookup[
                row["candidate"]
            ],
        )
        for family, row
        in ranked
    ]

    def overlay(
        base,
        source,
    ):
        result = clone_config(
            base,
            base["label"],
        )

        for key, value in source.items():
            if key == "label":
                continue

            seed_value = SEED.get(
                key
            )

            if value != seed_value:
                if isinstance(value, set):
                    result[key] = set(value)
                else:
                    result[key] = value

        return result

    interactions = []
    seen = set()
    counter = 0

    for i in range(len(pool)):
        for j in range(
            i + 1,
            len(pool),
        ):
            family_a, config_a = (
                pool[i]
            )

            family_b, config_b = (
                pool[j]
            )

            config = clone_config(
                SEED,
                "TEMP",
            )

            config = overlay(
                config,
                config_a,
            )

            config = overlay(
                config,
                config_b,
            )

            signature = config_signature(
                config
            )

            if signature in seen:
                continue

            seen.add(signature)
            counter += 1

            config["label"] = (
                f"INT{counter:03d}_"
                f"{family_a}_"
                f"{family_b}"
            )

            interactions.append(
                (
                    (
                        f"INTERACTION_"
                        f"{family_a}_"
                        f"{family_b}"
                    ),
                    config,
                )
            )

    return interactions


# ============================================================
# VALIDATION WINDOWS
# ============================================================

def era_windows():
    return [
        (
            "ERA_2010_2013",
            datetime(
                2010, 1, 1,
                tzinfo=timezone.utc,
            ),
            datetime(
                2014, 1, 1,
                tzinfo=timezone.utc,
            ),
        ),
        (
            "ERA_2014_2017",
            datetime(
                2014, 1, 1,
                tzinfo=timezone.utc,
            ),
            datetime(
                2018, 1, 1,
                tzinfo=timezone.utc,
            ),
        ),
        (
            "ERA_2018_2021",
            datetime(
                2018, 1, 1,
                tzinfo=timezone.utc,
            ),
            datetime(
                2022, 1, 1,
                tzinfo=timezone.utc,
            ),
        ),
        (
            "ERA_2022_NOW",
            datetime(
                2022, 1, 1,
                tzinfo=timezone.utc,
            ),
            RESEARCH_TO,
        ),
    ]


def devval_windows():
    return [
        (
            "DEV_2010_2017",
            datetime(
                2010, 1, 1,
                tzinfo=timezone.utc,
            ),
            datetime(
                2018, 1, 1,
                tzinfo=timezone.utc,
            ),
        ),
        (
            "VALIDATION_2018_NOW",
            datetime(
                2018, 1, 1,
                tzinfo=timezone.utc,
            ),
            RESEARCH_TO,
        ),
    ]


def recent_windows():
    return [
        (
            "LAST_5Y",
            years_ago_safe(
                RESEARCH_TO,
                5,
            ),
            RESEARCH_TO,
        ),
        (
            "LAST_2Y",
            years_ago_safe(
                RESEARCH_TO,
                2,
            ),
            RESEARCH_TO,
        ),
    ]


def validation_rows(
    signals,
    cache,
    configs,
    windows,
):
    rows = []

    for rank, config in enumerate(
        configs,
        start=1,
    ):
        for label, start, end in windows:
            trades = run_config_cached(
                signals,
                cache,
                config,
                PRIMARY_COST_PIPS,
                start=start,
                end=end,
            )

            stats = stats_from_trades(
                trades
            )

            rows.append({
                "rank":
                    rank,
                "window":
                    label,
                "candidate":
                    config["label"],
                "trades":
                    stats["trades"],
                "profit_factor":
                    round(
                        stats[
                            "profit_factor"
                        ],
                        6,
                    ),
                "total_r":
                    round(
                        stats["total_r"],
                        4,
                    ),
                "expectancy_r":
                    round(
                        stats[
                            "expectancy_r"
                        ],
                        6,
                    ),
                "max_drawdown_r":
                    round(
                        stats[
                            "max_drawdown_r"
                        ],
                        4,
                    ),
                "longest_loss_streak":
                    stats[
                        "longest_loss_streak"
                    ],
            })

    return rows


# ============================================================
# ROLLING
# ============================================================

def monthly_rolling_rows(
    signals,
    cache,
    config,
    months,
):
    rows = []

    cursor = month_start(
        RESEARCH_FROM
    )

    last_start = add_months(
        month_start(
            RESEARCH_TO
        ),
        -months,
    )

    while cursor <= last_start:
        end = add_months(
            cursor,
            months,
        )

        if end > RESEARCH_TO:
            break

        trades = run_config_cached(
            signals,
            cache,
            config,
            PRIMARY_COST_PIPS,
            start=cursor,
            end=end,
        )

        stats = stats_from_trades(
            trades
        )

        rows.append({
            "candidate":
                config["label"],
            "months":
                months,
            "window":
                (
                    f"{cursor:%Y-%m-%d}"
                    " -> "
                    f"{end:%Y-%m-%d}"
                ),
            "trades":
                stats["trades"],
            "profit_factor":
                round(
                    stats[
                        "profit_factor"
                    ],
                    6,
                ),
            "total_r":
                round(
                    stats[
                        "total_r"
                    ],
                    4,
                ),
            "expectancy_r":
                round(
                    stats[
                        "expectancy_r"
                    ],
                    6,
                ),
            "max_drawdown_r":
                round(
                    stats[
                        "max_drawdown_r"
                    ],
                    4,
                ),
            "positive":
                stats[
                    "total_r"
                ] > 0,
        })

        cursor = add_months(
            cursor,
            1,
        )

    return rows


def median(values):
    ordered = sorted(values)
    n = len(ordered)

    if n == 0:
        return None

    if n % 2:
        return ordered[n // 2]

    return (
        ordered[n // 2 - 1]
        +
        ordered[n // 2]
    ) / 2.0


def rolling_summary(rows):
    if not rows:
        return {}

    pfs = [
        float(
            row["profit_factor"]
        )
        for row in rows
    ]

    rs = [
        float(
            row["total_r"]
        )
        for row in rows
    ]

    positive = sum(
        1
        for row in rows
        if row["positive"]
    )

    worst_pf = min(
        rows,
        key=lambda row:
            float(
                row[
                    "profit_factor"
                ]
            ),
    )

    worst_r = min(
        rows,
        key=lambda row:
            float(
                row["total_r"]
            ),
    )

    return {
        "candidate":
            rows[0]["candidate"],
        "months":
            rows[0]["months"],
        "windows":
            len(rows),
        "positive_windows":
            positive,
        "positive_windows_pct":
            round(
                positive
                /
                len(rows)
                * 100.0,
                4,
            ),
        "worst_profit_factor":
            round(
                min(pfs),
                6,
            ),
        "median_profit_factor":
            round(
                median(pfs),
                6,
            ),
        "worst_total_r":
            round(
                min(rs),
                4,
            ),
        "median_total_r":
            round(
                median(rs),
                4,
            ),
        "worst_pf_window":
            worst_pf["window"],
        "worst_r_window":
            worst_r["window"],
    }


# ============================================================
# OVERLAP
# ============================================================

def trade_key(trade):
    return (
        trade[
            "entry_time_utc"
        ],
        trade[
            "exit_time_utc"
        ],
    )


def overlap_rows(
    signals,
    cache,
    reference,
    finalist,
):
    reference_trades = run_config_cached(
        signals,
        cache,
        reference,
        PRIMARY_COST_PIPS,
    )

    finalist_trades = run_config_cached(
        signals,
        cache,
        finalist,
        PRIMARY_COST_PIPS,
    )

    reference_keys = {
        trade_key(t)
        for t in reference_trades
    }

    finalist_keys = {
        trade_key(t)
        for t in finalist_trades
    }

    shared = (
        reference_keys
        &
        finalist_keys
    )

    added = (
        finalist_keys
        -
        reference_keys
    )

    removed = (
        reference_keys
        -
        finalist_keys
    )

    groups = [
        (
            "REFERENCE_ALL",
            reference_trades,
        ),
        (
            "FINALIST_ALL",
            finalist_trades,
        ),
        (
            "FINALIST_SHARED",
            [
                t
                for t in finalist_trades
                if trade_key(t)
                in shared
            ],
        ),
        (
            "FINALIST_ADDED",
            [
                t
                for t in finalist_trades
                if trade_key(t)
                in added
            ],
        ),
        (
            "REFERENCE_REMOVED",
            [
                t
                for t in reference_trades
                if trade_key(t)
                in removed
            ],
        ),
    ]

    rows = []

    for subset, trades in groups:
        stats = stats_from_trades(
            trades
        )

        rows.append({
            "reference":
                reference["label"],
            "finalist":
                finalist["label"],
            "subset":
                subset,
            "trades":
                stats["trades"],
            "profit_factor":
                round(
                    stats[
                        "profit_factor"
                    ],
                    6,
                ),
            "total_r":
                round(
                    stats["total_r"],
                    4,
                ),
            "expectancy_r":
                round(
                    stats[
                        "expectancy_r"
                    ],
                    6,
                ),
            "max_drawdown_r":
                round(
                    stats[
                        "max_drawdown_r"
                    ],
                    4,
                ),
        })

    return rows


# ============================================================
# RUNNER
# ============================================================

def run_research():
    try:
        m15 = fetch_history(
            "M15",
            RESEARCH_FROM,
            RESEARCH_TO,
            30,
        )

        h1 = fetch_history(
            "H1",
            RESEARCH_FROM
            - timedelta(days=700),
            RESEARCH_TO,
            120,
        )

        h4 = fetch_history(
            "H4",
            RESEARCH_FROM
            - timedelta(days=1200),
            RESEARCH_TO,
            500,
        )

        daily = fetch_history(
            "D",
            RESEARCH_FROM
            - timedelta(days=1800),
            RESEARCH_TO,
            2500,
            daily_alignment=True,
        )

        if len(m15) < 1000:
            raise RuntimeError(
                "Too few M15 candles returned"
            )

        STATUS.update({
            "state":
                "precomputing",
            "message":
                "Building corrected Gen2 features",
            "m15_candles":
                len(m15),
            "h1_candles":
                len(h1),
            "h4_candles":
                len(h4),
            "daily_candles":
                len(daily),
        })

        m15_atr = atr14(m15)

        h1_state = build_htf_state(
            h1,
            H1_EMAS,
        )

        h4_state = build_htf_state(
            h4,
            H4_EMAS,
        )

        daily_state = build_htf_state(
            daily,
            DAILY_EMAS,
        )

        signals = build_signal_cache(
            m15,
            m15_atr,
            h1_state,
            h4_state,
            daily_state,
        )

        STATUS.update({
            "state":
                "precomputing",
            "message":
                "Caching reusable trade outcomes",
            "engulfing_signals":
                len(signals),
        })

        cache = build_outcome_cache(
            m15,
            signals,
        )

        # Seed.
        seed_rows = []

        for cost in COST_PIPS_GRID:
            trades = run_config_cached(
                signals,
                cache,
                SEED,
                cost,
            )

            seed_rows.append(
                result_row(
                    "SEED",
                    SEED,
                    cost,
                    trades,
                )
            )

        write_csv(
            OUTPUT_SEED,
            seed_rows,
        )

        # Local body / structure / RR grid.
        local = local_configs()

        STATUS.update({
            "state":
                "calculating",
            "message":
                "Running local body/structure/RR grid",
            "local_configs":
                len(local),
        })

        local_rows = []

        for number, (
            family,
            config,
        ) in enumerate(
            local,
            start=1,
        ):
            for cost in COST_PIPS_GRID:
                trades = run_config_cached(
                    signals,
                    cache,
                    config,
                    cost,
                )

                local_rows.append(
                    result_row(
                        family,
                        config,
                        cost,
                        trades,
                    )
                )

            if number % 100 == 0:
                STATUS["message"] = (
                    "Local grid "
                    f"{number}/{len(local)}"
                )

        write_csv(
            OUTPUT_LOCAL,
            local_rows,
        )

        # Single context families.
        singles = single_family_configs()

        STATUS.update({
            "state":
                "calculating",
            "message":
                "Running targeted context families",
            "single_configs":
                len(singles),
        })

        single_rows = []

        for number, (
            family,
            config,
        ) in enumerate(
            singles,
            start=1,
        ):
            for cost in COST_PIPS_GRID:
                trades = run_config_cached(
                    signals,
                    cache,
                    config,
                    cost,
                )

                single_rows.append(
                    result_row(
                        family,
                        config,
                        cost,
                        trades,
                    )
                )

            if number % 20 == 0:
                STATUS["message"] = (
                    "Targeted context families "
                    f"{number}/{len(singles)}"
                )

        write_csv(
            OUTPUT_SINGLE,
            single_rows,
        )

        # Primary pool.
        primary_rows = [
            row
            for row in (
                local_rows
                +
                single_rows
            )
            if (
                abs(
                    float(
                        row["cost_pips"]
                    )
                    -
                    PRIMARY_COST_PIPS
                )
                < 1e-12
                and
                int(
                    row["trades"]
                )
                >= MIN_TRADES_PRIMARY
            )
        ]

        primary_rows.sort(
            key=lambda row: (
                float(
                    row["profit_factor"]
                ),
                float(
                    row["expectancy_r"]
                ),
                float(
                    row["total_r"]
                ),
            ),
            reverse=True,
        )

        config_lookup = {
            config["label"]:
                config
            for _, config
            in (
                local
                +
                singles
            )
        }

        config_lookup[
            SEED["label"]
        ] = SEED

        # Controlled interactions.
        interactions = (
            build_controlled_interactions(
                primary_rows,
                config_lookup,
            )
        )

        STATUS.update({
            "state":
                "calculating",
            "message":
                "Running controlled interactions",
            "interaction_configs":
                len(interactions),
        })

        interaction_rows = []

        for number, (
            family,
            config,
        ) in enumerate(
            interactions,
            start=1,
        ):
            for cost in COST_PIPS_GRID:
                trades = run_config_cached(
                    signals,
                    cache,
                    config,
                    cost,
                )

                interaction_rows.append(
                    result_row(
                        family,
                        config,
                        cost,
                        trades,
                    )
                )

            STATUS["message"] = (
                "Controlled interactions "
                f"{number}/{len(interactions)}"
            )

        write_csv(
            OUTPUT_INTERACTIONS,
            interaction_rows,
        )

        for _, config in interactions:
            config_lookup[
                config["label"]
            ] = config

        # Combined ranking.
        combined = list(
            primary_rows
        )

        combined.extend(
            row
            for row
            in interaction_rows
            if (
                abs(
                    float(
                        row["cost_pips"]
                    )
                    -
                    PRIMARY_COST_PIPS
                )
                < 1e-12
                and
                int(
                    row["trades"]
                )
                >= MIN_TRADES_PRIMARY
            )
        )

        seed_primary = next(
            row
            for row in seed_rows
            if (
                abs(
                    float(
                        row["cost_pips"]
                    )
                    -
                    PRIMARY_COST_PIPS
                )
                < 1e-12
            )
        )

        combined.append(
            seed_primary
        )

        combined.sort(
            key=lambda row: (
                float(
                    row["profit_factor"]
                ),
                float(
                    row["expectancy_r"]
                ),
                float(
                    row["total_r"]
                ),
            ),
            reverse=True,
        )

        top_rows = (
            combined[:40]
        )

        write_csv(
            OUTPUT_TOP,
            top_rows,
        )

        finalists = [
            config_lookup[
                row["candidate"]
            ]
            for row
            in top_rows[:15]
        ]

        # Validation.
        STATUS.update({
            "state":
                "validating",
            "message":
                "Running 4-era validation",
        })

        era_rows = validation_rows(
            signals,
            cache,
            finalists,
            era_windows(),
        )

        STATUS["message"] = (
            "Running dev / validation split"
        )

        devval_rows = validation_rows(
            signals,
            cache,
            finalists,
            devval_windows(),
        )

        STATUS["message"] = (
            "Running recent 5Y / 2Y"
        )

        recent_rows = validation_rows(
            signals,
            cache,
            finalists,
            recent_windows(),
        )

        write_csv(
            OUTPUT_ERAS,
            era_rows,
        )

        write_csv(
            OUTPUT_DEVVAL,
            devval_rows,
        )

        write_csv(
            OUTPUT_RECENT,
            recent_rows,
        )

        # Robust ranking.
        robust = []

        for config in finalists:
            label = config[
                "label"
            ]

            base = next(
                row
                for row in top_rows
                if row[
                    "candidate"
                ] == label
            )

            eras = [
                row
                for row in era_rows
                if (
                    row["candidate"]
                    == label
                    and
                    int(
                        row["trades"]
                    ) > 0
                )
            ]

            devval = [
                row
                for row in devval_rows
                if (
                    row["candidate"]
                    == label
                    and
                    int(
                        row["trades"]
                    ) > 0
                )
            ]

            recent = [
                row
                for row in recent_rows
                if (
                    row["candidate"]
                    == label
                    and
                    int(
                        row["trades"]
                    ) > 0
                )
            ]

            era_pfs = [
                float(
                    row["profit_factor"]
                )
                for row in eras
            ]

            devval_pfs = [
                float(
                    row["profit_factor"]
                )
                for row in devval
            ]

            recent_pfs = [
                float(
                    row["profit_factor"]
                )
                for row in recent
            ]

            min_era = (
                min(era_pfs)
                if era_pfs
                else 0.0
            )

            min_devval = (
                min(devval_pfs)
                if devval_pfs
                else 0.0
            )

            min_recent = (
                min(recent_pfs)
                if recent_pfs
                else 0.0
            )

            robust.append({
                "candidate":
                    label,
                "trades":
                    int(
                        base["trades"]
                    ),
                "full_pf":
                    float(
                        base[
                            "profit_factor"
                        ]
                    ),
                "full_total_r":
                    float(
                        base["total_r"]
                    ),
                "full_expectancy":
                    float(
                        base[
                            "expectancy_r"
                        ]
                    ),
                "minimum_era_pf":
                    min_era,
                "minimum_devval_pf":
                    min_devval,
                "minimum_recent_pf":
                    min_recent,
                "score": (
                    min_era * 3.0
                    +
                    min_devval * 2.0
                    +
                    min_recent * 2.0
                    +
                    float(
                        base[
                            "profit_factor"
                        ]
                    )
                ),
            })

        robust.sort(
            key=lambda row:
                row["score"],
            reverse=True,
        )

        robust_finalists = [
            config_lookup[
                row["candidate"]
            ]
            for row
            in robust[:6]
        ]

        # Rolling.
        STATUS["message"] = (
            "Running rolling 2Y / 3Y"
        )

        rolling_rows = []
        rolling_summary_rows = []

        for config in robust_finalists:
            for months in [
                24,
                36,
            ]:
                rows = monthly_rolling_rows(
                    signals,
                    cache,
                    config,
                    months,
                )

                rolling_rows.extend(
                    rows
                )

                rolling_summary_rows.append(
                    rolling_summary(
                        rows
                    )
                )

        write_csv(
            OUTPUT_ROLLING,
            rolling_rows,
        )

        write_csv(
            OUTPUT_ROLLING_SUMMARY,
            rolling_summary_rows,
        )

        # Overlap against seed.
        STATUS["message"] = (
            "Running overlap vs seed"
        )

        overlap = []

        for config in robust_finalists:
            overlap.extend(
                overlap_rows(
                    signals,
                    cache,
                    SEED,
                    config,
                )
            )

        write_csv(
            OUTPUT_OVERLAP,
            overlap,
        )

        best = (
            robust_finalists[0]
            if robust_finalists
            else SEED
        )

        best_trades = run_config_cached(
            signals,
            cache,
            best,
            PRIMARY_COST_PIPS,
        )

        write_csv(
            OUTPUT_BEST_TRADES,
            best_trades,
        )

        STATUS.update({
            "state":
                "packaging",
            "message":
                "Building single ZIP results bundle",
        })

        build_results_bundle()

        STATUS.update({
            "state":
                "complete",
            "message":
                "USDJPY M15 long Gen2 complete",
            "m15_candles":
                len(m15),
            "h1_candles":
                len(h1),
            "h4_candles":
                len(h4),
            "daily_candles":
                len(daily),
            "engulfing_signals":
                len(signals),
            "local_configs":
                len(local),
            "single_configs":
                len(singles),
            "interaction_configs":
                len(interactions),
            "seed_stats":
                stats_from_trades(
                    run_config_cached(
                        signals,
                        cache,
                        SEED,
                        PRIMARY_COST_PIPS,
                    )
                ),
            "robust_ranking":
                robust[:12],
            "selected_best":
                best,
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
            "USDJPY M15 Long Gen2 Targeted Sweep Displacement",
        "status":
            STATUS["state"],
        "instrument":
            INSTRUMENT,
        "timeframe":
            "M15",
        "side":
            "BUY",
        "orders_supported":
            False,
        "trading_enabled":
            False,
        "routes": [
            "/usdjpy-m15-long-gen2/status",
            "/usdjpy-m15-long-gen2/results",
        ],
    })


@app.route(
    "/usdjpy-m15-long-gen2/status"
)
def route_status():
    return jsonify(
        STATUS
    )


@app.route(
    "/usdjpy-m15-long-gen2/results"
)
def route_results():
    return download_file(
        OUTPUT_BUNDLE
    )


if __name__ == "__main__":
    research_thread = threading.Thread(
        target=run_research,
        name="usdjpy-m15-long-gen2",
        daemon=True,
    )

    research_thread.start()

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
