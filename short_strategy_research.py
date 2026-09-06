
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
# GBP/USD M15 SHORT
# EXHAUSTIVE-BUT-CONTROLLED RESEARCH
#
# READ-ONLY. NEVER SENDS ORDERS.
#
# Corrected from outset:
#   - HTF state uses only candles completed by M15 signal OPEN
#   - H1/H4/Daily completion based on NEXT actual OANDA candle open
#   - Daily DST-safe via actual OANDA timestamps
#   - momentum context ends on previous M15 candle
#   - candidate lists cached per config
#   - one ZIP download route
#
# Core:
#   - exact bearish engulfing
#   - OANDA midpoint M15
#   - ATR14 Wilder/RMA, SMA seeded
#   - stop = signal high + 10 ticks
#   - target based on REFERENCE signal close
#   - adverse short entry = signal close - cost
#   - exits begin NEXT candle
#   - same-bar short tie:
#       high closer => STOP first
#       otherwise TARGET first
#   - pyramiding 0
#   - exact exit-candle signal eligible
#
# Development cost:
#   1.0 pip adverse
#
# Cost stress:
#   0.5 / 1.0 / 1.5 / 2.0 pips
#
# Families:
#   - body ratio
#   - body ATR
#   - range ATR
#   - bearish close location
#   - upper wick/body
#   - structure lookback/distance
#   - M15 ATR regime
#   - prior 4h/12h/24h momentum
#   - stop-size / ATR
#   - session includes / hour exclusions
#   - weekday exclusions
#   - H1 trend/alignment/ATR
#   - H4 trend/alignment/ATR
#   - Daily trend/alignment/ATR
#   - RR
#
# Then:
#   - controlled interactions
#   - 4 eras
#   - DEV 2010-2017 / VALIDATION 2018-now
#   - recent 5Y / 2Y
#   - rolling 2Y / 3Y
# ============================================================


app = Flask(__name__)

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_BASE = os.getenv(
    "OANDA_API_URL",
    "https://api-fxtrade.oanda.com",
)

INSTRUMENT = "GBP_USD"

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

TICK_SIZE = 0.00001
PIP_SIZE = 0.0001
STOP_BUFFER_TICKS = 10

PRIMARY_COST_PIPS = 1.00

COST_PIPS_GRID = [
    0.50,
    1.00,
    1.50,
    2.00,
]

RR_VALUES = [
    2.00,
    2.50,
    3.00,
    3.25,
    3.50,
    3.75,
    4.00,
    4.25,
    4.50,
    5.00,
]

MIN_TRADES_PRIMARY = 60


# ============================================================
# OUTPUTS
# ============================================================

OUTPUT_SINGLE = (
    "gbpusd_m15_short_exhaustive_single_family.csv"
)

OUTPUT_SINGLE_TOP = (
    "gbpusd_m15_short_exhaustive_single_top.csv"
)

OUTPUT_INTERACTIONS = (
    "gbpusd_m15_short_exhaustive_interactions.csv"
)

OUTPUT_TOP = (
    "gbpusd_m15_short_exhaustive_top.csv"
)

OUTPUT_ERAS = (
    "gbpusd_m15_short_exhaustive_eras.csv"
)

OUTPUT_DEVVAL = (
    "gbpusd_m15_short_exhaustive_dev_validation.csv"
)

OUTPUT_RECENT = (
    "gbpusd_m15_short_exhaustive_recent.csv"
)

OUTPUT_ROLLING = (
    "gbpusd_m15_short_exhaustive_rolling.csv"
)

OUTPUT_ROLLING_SUMMARY = (
    "gbpusd_m15_short_exhaustive_rolling_summary.csv"
)

OUTPUT_BEST_TRADES = (
    "gbpusd_m15_short_exhaustive_best_trades.csv"
)

OUTPUT_BUNDLE = (
    "gbpusd_m15_short_exhaustive_RESULTS.zip"
)

STATUS = {
    "state": "not_started",
    "message": "GBP/USD M15 short research has not started",
    "service": "GBPUSD M15 Short Exhaustive Controlled",
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
        OUTPUT_SINGLE,
        OUTPUT_SINGLE_TOP,
        OUTPUT_INTERACTIONS,
        OUTPUT_TOP,
        OUTPUT_ERAS,
        OUTPUT_DEVVAL,
        OUTPUT_RECENT,
        OUTPUT_ROLLING,
        OUTPUT_ROLLING_SUMMARY,
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


def clone_config(base, label):
    result = {}

    for key, value in base.items():
        if isinstance(value, set):
            result[key] = set(value)
        else:
            result[key] = value

    result["label"] = label
    return result


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


def fetch_candles_chunk(
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
    daily_alignment=False,
):
    cursor = start
    by_time = {}
    chunk_number = 0

    while cursor < end:
        chunk_number += 1

        chunk_end = min(
            cursor + timedelta(
                days=chunk_days
            ),
            end,
        )

        STATUS.update({
            "state": "fetching",
            "message": (
                f"Fetching {granularity} "
                f"chunk {chunk_number}: "
                f"{iso_utc(cursor)} -> "
                f"{iso_utc(chunk_end)}"
            ),
            "granularity": granularity,
            "chunk": chunk_number,
        })

        rows = fetch_candles_chunk(
            granularity,
            cursor,
            chunk_end,
            daily_alignment=daily_alignment,
        )

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
    result = [
        None
    ] * len(candles)

    for i in range(len(candles)):
        high = candles[i]["high"]
        low = candles[i]["low"]

        if i == 0:
            result[i] = high - low
        else:
            prev_close = candles[
                i - 1
            ]["close"]

            result[i] = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close),
            )

    return result


def rma_from_values(values, length):
    result = [
        None
    ] * len(values)

    if len(values) < length:
        return result

    seed = values[:length]

    if any(
        value is None
        for value in seed
    ):
        return result

    result[
        length - 1
    ] = (
        sum(seed)
        / length
    )

    for i in range(
        length,
        len(values),
    ):
        value = values[i]

        if (
            value is None
            or
            result[
                i - 1
            ] is None
        ):
            continue

        result[i] = (
            result[
                i - 1
            ]
            * (
                length - 1
            )
            + value
        ) / length

    return result


def atr14(candles):
    return rma_from_values(
        true_ranges(candles),
        14,
    )


def ema(values, length):
    result = [
        None
    ] * len(values)

    if len(values) < length:
        return result

    seed = values[:length]

    if any(
        value is None
        for value in seed
    ):
        return result

    result[
        length - 1
    ] = (
        sum(seed)
        / length
    )

    alpha = (
        2.0
        /
        (
            length + 1.0
        )
    )

    for i in range(
        length,
        len(values),
    ):
        if (
            values[i] is None
            or
            result[
                i - 1
            ] is None
        ):
            continue

        result[i] = (
            alpha
            * values[i]
            +
            (
                1.0 - alpha
            )
            * result[
                i - 1
            ]
        )

    return result


def sma(values, length):
    result = [
        None
    ] * len(values)

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
            and
            all(
                item is not None
                for item in queue
            )
        ):
            result[i] = (
                running / length
            )

    return result


def bearish_engulfing(candles, i):
    if i < 1:
        return False

    previous = candles[i - 1]
    current = candles[i]

    return (
        previous["close"]
        >
        previous["open"]
        and
        current["close"]
        <
        current["open"]
        and
        current["open"]
        >=
        previous["close"]
        and
        current["close"]
        <=
        previous["open"]
    )


# ============================================================
# HTF STATE - COMPLETED ONLY
# ============================================================

H1_EMA_LENGTHS = [
    20,
    40,
    50,
    70,
    100,
    150,
    200,
]

H4_EMA_LENGTHS = [
    20,
    50,
    100,
    200,
]

DAILY_EMA_LENGTHS = [
    20,
    40,
    50,
    70,
    85,
    100,
    150,
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
            ema(
                closes,
                length,
            )
        for length in ema_lengths
    }

    rows = []

    for i, candle in enumerate(
        candles
    ):
        complete_at = (
            candles[
                i + 1
            ]["time"]
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
                    for length in ema_lengths
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

    return rows[
        position
    ]


# ============================================================
# SIGNAL FEATURE CACHE
# ============================================================

STRUCTURE_LOOKBACKS = [
    20,
    40,
    60,
    70,
    90,
    120,
    150,
]

MOMENTUM_BARS = {
    "4h": 16,
    "12h": 48,
    "24h": 96,
}


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
        if row[
            "complete_at"
        ] is not None
    ]

    h4_rows = [
        row
        for row in h4_state
        if row[
            "complete_at"
        ] is not None
    ]

    daily_rows = [
        row
        for row in daily_state
        if row[
            "complete_at"
        ] is not None
    ]

    h1_completion_times = [
        row["complete_at"]
        for row in h1_rows
    ]

    h4_completion_times = [
        row["complete_at"]
        for row in h4_rows
    ]

    daily_completion_times = [
        row["complete_at"]
        for row in daily_rows
    ]

    max_lookback = max(
        max(
            STRUCTURE_LOOKBACKS
        ),
        max(
            MOMENTUM_BARS.values()
        ) + 1,
    )

    for i in range(
        max(
            14,
            max_lookback,
        ),
        len(m15),
    ):
        if not bearish_engulfing(
            m15,
            i,
        ):
            continue

        current = m15[i]
        previous = m15[
            i - 1
        ]
        atr = m15_atr[i]

        if (
            atr is None
            or atr <= 0
        ):
            continue

        body = (
            current["open"]
            -
            current["close"]
        )

        previous_body = abs(
            previous["close"]
            -
            previous["open"]
        )

        body_ratio = (
            body / previous_body
            if previous_body > 0
            else 999.0
        )

        body_atr = (
            body / atr
        )

        candle_range = (
            current["high"]
            -
            current["low"]
        )

        range_atr = (
            candle_range / atr
        )

        # For bearish candles:
        # 0 = close at low, 1 = close at high.
        close_location = (
            (
                current["close"]
                -
                current["low"]
            )
            / candle_range
            if candle_range > 0
            else 1.0
        )

        upper_wick = (
            current["high"]
            -
            max(
                current["open"],
                current["close"],
            )
        )

        upper_wick_body = (
            upper_wick / body
            if body > 0
            else 0.0
        )

        stop = (
            current["high"]
            +
            STOP_BUFFER_TICKS
            * TICK_SIZE
        )

        stop_atr = (
            (
                stop
                -
                current["close"]
            )
            / atr
        )

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

        # Strictly PRIOR context.
        prior_close = m15[
            i - 1
        ]["close"]

        momentum = {}

        for name, bars in MOMENTUM_BARS.items():
            old_close = m15[
                i - 1 - bars
            ]["close"]

            momentum[name] = (
                prior_close
                -
                old_close
            ) / atr

        structure_distance_atr = {}

        for lookback in STRUCTURE_LOOKBACKS:
            previous_high = max(
                candle["high"]
                for candle in m15[
                    i - lookback:i
                ]
            )

            structure_distance_atr[
                lookback
            ] = (
                abs(
                    current["high"]
                    -
                    previous_high
                )
                / atr
            )

        ny_time = (
            current["time"]
            .astimezone(NY)
        )

        h1_prev = previous_completed_state(
            h1_rows,
            h1_completion_times,
            current["time"],
        )

        h4_prev = previous_completed_state(
            h4_rows,
            h4_completion_times,
            current["time"],
        )

        daily_prev = previous_completed_state(
            daily_rows,
            daily_completion_times,
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
            "upper_wick_body":
                upper_wick_body,
            "stop_atr":
                stop_atr,
            "m15_atr_ratio":
                m15_atr_ratio,
            "momentum_4h_atr":
                momentum["4h"],
            "momentum_12h_atr":
                momentum["12h"],
            "momentum_24h_atr":
                momentum["24h"],
            "structure_distance_atr":
                structure_distance_atr,
            "ny_hour":
                ny_time.hour,
            "ny_weekday":
                ny_time.weekday(),
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

BASELINE = {
    "label":
        "BASELINE",
    "minimum_body_ratio":
        1.00,
    "minimum_body_atr":
        None,
    "minimum_range_atr":
        None,
    "maximum_close_location":
        None,
    "minimum_upper_wick_body":
        None,

    "structure_lookback":
        None,
    "maximum_distance_atr":
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

    "minimum_stop_atr":
        None,
    "maximum_stop_atr":
        None,

    "included_ny_hours":
        None,
    "excluded_ny_hours":
        set(),
    "excluded_weekdays":
        set(),

    "h1_close_below_ema":
        None,
    "h1_fast_ema":
        None,
    "h1_slow_ema":
        None,
    "maximum_h1_atr_ratio":
        None,
    "minimum_h1_atr_ratio":
        None,

    "h4_close_below_ema":
        None,
    "h4_fast_ema":
        None,
    "h4_slow_ema":
        None,
    "maximum_h4_atr_ratio":
        None,
    "minimum_h4_atr_ratio":
        None,

    "daily_close_below_ema":
        None,
    "daily_fast_ema":
        None,
    "daily_slow_ema":
        None,
    "maximum_daily_atr_ratio":
        None,
    "minimum_daily_atr_ratio":
        None,

    "reward_risk":
        3.00,
}


# ============================================================
# FILTER HELPERS
# ============================================================

def state_close_below_ema(
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
        state["close"] < value
    )


def state_bear_alignment(
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
        fast_value < slow_value
    )


def atr_ratio_passes(
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

    return True


def signal_passes(
    signal,
    config,
):
    if (
        signal["body_ratio"]
        <
        config["minimum_body_ratio"]
    ):
        return False

    if (
        config["minimum_body_atr"]
        is not None
        and
        signal["body_atr"]
        <
        config["minimum_body_atr"]
    ):
        return False

    if (
        config["minimum_range_atr"]
        is not None
        and
        signal["range_atr"]
        <
        config["minimum_range_atr"]
    ):
        return False

    if (
        config["maximum_close_location"]
        is not None
        and
        signal["close_location"]
        >
        config["maximum_close_location"]
    ):
        return False

    if (
        config["minimum_upper_wick_body"]
        is not None
        and
        signal["upper_wick_body"]
        <
        config["minimum_upper_wick_body"]
    ):
        return False

    lookback = config[
        "structure_lookback"
    ]

    if lookback is not None:
        if (
            signal[
                "structure_distance_atr"
            ][lookback]
            >
            config[
                "maximum_distance_atr"
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

    if (
        config["minimum_stop_atr"]
        is not None
        and
        signal["stop_atr"]
        <
        config["minimum_stop_atr"]
    ):
        return False

    if (
        config["maximum_stop_atr"]
        is not None
        and
        signal["stop_atr"]
        >
        config["maximum_stop_atr"]
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
        in
        config[
            "excluded_ny_hours"
        ]
    ):
        return False

    if (
        signal["ny_weekday"]
        in
        config[
            "excluded_weekdays"
        ]
    ):
        return False

    h1 = signal["h1_prev"]

    if (
        config["h1_close_below_ema"]
        is not None
        and
        not state_close_below_ema(
            h1,
            config[
                "h1_close_below_ema"
            ],
        )
    ):
        return False

    if (
        config["h1_fast_ema"]
        is not None
        and
        config["h1_slow_ema"]
        is not None
        and
        not state_bear_alignment(
            h1,
            config["h1_fast_ema"],
            config["h1_slow_ema"],
        )
    ):
        return False

    if not atr_ratio_passes(
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
        config["h4_close_below_ema"]
        is not None
        and
        not state_close_below_ema(
            h4,
            config[
                "h4_close_below_ema"
            ],
        )
    ):
        return False

    if (
        config["h4_fast_ema"]
        is not None
        and
        config["h4_slow_ema"]
        is not None
        and
        not state_bear_alignment(
            h4,
            config["h4_fast_ema"],
            config["h4_slow_ema"],
        )
    ):
        return False

    if not atr_ratio_passes(
        h4,
        config[
            "minimum_h4_atr_ratio"
        ],
        config[
            "maximum_h4_atr_ratio"
        ],
    ):
        return False

    daily = signal[
        "daily_prev"
    ]

    if (
        config[
            "daily_close_below_ema"
        ]
        is not None
        and
        not state_close_below_ema(
            daily,
            config[
                "daily_close_below_ema"
            ],
        )
    ):
        return False

    if (
        config["daily_fast_ema"]
        is not None
        and
        config["daily_slow_ema"]
        is not None
        and
        not state_bear_alignment(
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

    if not atr_ratio_passes(
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
# TRADE OUTCOME CACHE
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
        signal["high"]
        +
        STOP_BUFFER_TICKS
        * TICK_SIZE
    )

    reference_risk = (
        stop
        -
        reference_entry
    )

    if reference_risk <= 0:
        return None

    target = (
        reference_entry
        -
        reward_risk
        * reference_risk
    )

    backtest_entry = (
        reference_entry
        -
        cost_pips
        * PIP_SIZE
    )

    actual_risk = (
        stop
        -
        backtest_entry
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
            and
            hit_target
        ):
            distance_high = abs(
                candle["high"]
                -
                candle["open"]
            )

            distance_low = abs(
                candle["open"]
                -
                candle["low"]
            )

            if (
                distance_high
                <
                distance_low
            ):
                exit_price = stop
                exit_reason = "STOP"
            else:
                exit_price = target
                exit_reason = "TARGET"

        elif hit_stop:
            exit_price = stop
            exit_reason = "STOP"

        elif hit_target:
            exit_price = target
            exit_reason = "TARGET"

        else:
            continue

        result_r = (
            backtest_entry
            -
            exit_price
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
        * len(RR_VALUES)
        * len(COST_PIPS_GRID)
    )

    done = 0

    for signal in signals:
        signal_index = signal[
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
                        signal_index,
                        rr,
                        cost,
                    )
                ] = compute_trade_outcome(
                    candles,
                    signal_index,
                    rr,
                    cost,
                )

    return cache


# ============================================================
# CONFIG CACHING / BACKTEST
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
            for key, value
            in config.items()
            if key != "label"
        )
    )


CANDIDATE_CACHE = {}


def qualifying_candidates(
    signals,
    config,
):
    key = config_signature(
        config
    )

    cached = CANDIDATE_CACHE.get(
        key
    )

    if cached is not None:
        return cached

    candidates = [
        signal
        for signal in signals
        if signal_passes(
            signal,
            config,
        )
    ]

    CANDIDATE_CACHE[
        key
    ] = candidates

    return candidates


def run_config_cached(
    signals,
    cache,
    config,
    cost_pips,
    start=None,
    end=None,
):
    all_candidates = (
        qualifying_candidates(
            signals,
            config,
        )
    )

    if (
        start is None
        and
        end is None
    ):
        candidates = (
            all_candidates
        )
    else:
        times = [
            signal["time"]
            for signal
            in all_candidates
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
            len(all_candidates)
            if end is None
            else bisect.bisect_left(
                times,
                end,
            )
        )

        candidates = (
            all_candidates[
                left:right
            ]
        )

    indices = [
        signal[
            "signal_index"
        ]
        for signal in candidates
    ]

    trades = []
    position = 0

    while position < len(
        candidates
    ):
        signal = candidates[
            position
        ]

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

        trades.append(
            dict(trade)
        )

        position = (
            bisect.bisect_left(
                indices,
                trade[
                    "exit_index"
                ],
                lo=position + 1,
            )
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
    gross_loss = abs(
        sum(losers)
    )

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
                * 100.0
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
                stats[
                    "max_drawdown_r"
                ],
                4,
            ),
        "longest_loss_streak":
            stats[
                "longest_loss_streak"
            ],
    }

    for key, value in config.items():
        if key == "label":
            continue

        if isinstance(value, set):
            row[key] = ",".join(
                str(item)
                for item in sorted(
                    value
                )
            )
        else:
            row[key] = value

    return row


# ============================================================
# SINGLE-FAMILY SWEEP
# ============================================================

def single_family_configs():
    rows = []

    def add(
        family,
        label,
        mutator,
    ):
        config = clone_config(
            BASELINE,
            label,
        )

        mutator(config)

        rows.append(
            (
                family,
                config,
            )
        )

    for value in [
        1.00,
        1.05,
        1.10,
        1.20,
        1.30,
        1.40,
        1.50,
        1.60,
    ]:
        add(
            "BODY_RATIO",
            f"BR_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_body_ratio",
                    v,
                ),
        )

    for value in [
        0.30,
        0.40,
        0.50,
        0.60,
        0.70,
        0.80,
        0.90,
        1.00,
        1.10,
        1.20,
        1.40,
    ]:
        add(
            "BODY_ATR",
            f"BA_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_body_atr",
                    v,
                ),
        )

    for value in [
        0.60,
        0.80,
        1.00,
        1.20,
        1.40,
        1.60,
        1.80,
        2.00,
    ]:
        add(
            "RANGE_ATR",
            f"RA_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_range_atr",
                    v,
                ),
        )

    for value in [
        0.10,
        0.15,
        0.20,
        0.25,
        0.30,
        0.35,
        0.40,
        0.45,
    ]:
        add(
            "CLOSE_LOCATION",
            f"CL_MAX_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "maximum_close_location",
                    v,
                ),
        )

    for value in [
        0.05,
        0.10,
        0.20,
        0.30,
        0.40,
        0.50,
        0.75,
        1.00,
    ]:
        add(
            "UPPER_WICK",
            f"UW_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_upper_wick_body",
                    v,
                ),
        )

    for lookback in (
        STRUCTURE_LOOKBACKS
    ):
        for distance in [
            0.05,
            0.075,
            0.10,
            0.15,
            0.20,
            0.25,
            0.30,
            0.40,
            0.50,
            0.75,
        ]:
            def mutate_structure(
                c,
                lb=lookback,
                d=distance,
            ):
                c[
                    "structure_lookback"
                ] = lb

                c[
                    "maximum_distance_atr"
                ] = d

            add(
                "STRUCTURE",
                (
                    f"S{lookback}_"
                    f"D{distance:.3f}"
                ),
                mutate_structure,
            )

    for value in [
        0.70,
        0.80,
        0.90,
        1.00,
        1.10,
        1.20,
        1.30,
    ]:
        add(
            "M15_ATR_MIN",
            f"M15ATR_MIN_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_m15_atr_ratio",
                    v,
                ),
        )

    for value in [
        0.80,
        0.90,
        1.00,
        1.10,
        1.20,
        1.30,
        1.50,
    ]:
        add(
            "M15_ATR_MAX",
            f"M15ATR_MAX_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "maximum_m15_atr_ratio",
                    v,
                ),
        )

    for horizon in [
        "4h",
        "12h",
        "24h",
    ]:
        for value in [
            -3.0,
            -2.0,
            -1.0,
            -0.5,
            0.0,
            0.5,
            1.0,
            2.0,
        ]:
            key = (
                f"minimum_momentum_"
                f"{horizon}_atr"
            )

            add(
                f"MOM_{horizon}_MIN",
                (
                    f"MOM_{horizon}_"
                    f"MIN_{value:+.2f}"
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
            -1.0,
            -0.5,
            0.0,
            0.5,
            1.0,
            2.0,
            3.0,
        ]:
            key = (
                f"maximum_momentum_"
                f"{horizon}_atr"
            )

            add(
                f"MOM_{horizon}_MAX",
                (
                    f"MOM_{horizon}_"
                    f"MAX_{value:+.2f}"
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
        0.50,
        0.60,
        0.70,
        0.80,
        1.00,
    ]:
        add(
            "STOP_MIN",
            f"STOP_MIN_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_stop_atr",
                    v,
                ),
        )

    for value in [
        0.80,
        1.00,
        1.20,
        1.40,
        1.60,
        2.00,
        2.50,
    ]:
        add(
            "STOP_MAX",
            f"STOP_MAX_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "maximum_stop_atr",
                    v,
                ),
        )

    session_sets = {
        "NY_00_05":
            set(range(0, 6)),
        "NY_02_06":
            set(range(2, 7)),
        "NY_07_11":
            set(range(7, 12)),
        "NY_08_12":
            set(range(8, 13)),
        "NY_08_15":
            set(range(8, 16)),
        "NY_09_13":
            set(range(9, 14)),
        "NY_12_16":
            set(range(12, 17)),
        "NY_14_18":
            set(range(14, 19)),
    }

    for label, hours in (
        session_sets.items()
    ):
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

    for length in [
        40,
        50,
        70,
        100,
        150,
        200,
    ]:
        add(
            "H1_CLOSE_EMA",
            f"H1_CLOSE_LT_EMA{length}",
            lambda c, v=length:
                c.__setitem__(
                    "h1_close_below_ema",
                    v,
                ),
        )

    for fast, slow in [
        (20, 50),
        (40, 100),
        (50, 100),
        (50, 200),
        (70, 150),
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
            f"H1_EMA{fast}_LT_{slow}",
            mutate_h1,
        )

    for value in [
        0.70,
        0.80,
        0.90,
        1.00,
        1.10,
        1.20,
    ]:
        add(
            "H1_ATR_MIN",
            f"H1ATR_MIN_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_h1_atr_ratio",
                    v,
                ),
        )

    for value in [
        0.80,
        0.90,
        1.00,
        1.10,
        1.20,
    ]:
        add(
            "H1_ATR_MAX",
            f"H1ATR_MAX_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "maximum_h1_atr_ratio",
                    v,
                ),
        )

    for length in [
        50,
        100,
        200,
    ]:
        add(
            "H4_CLOSE_EMA",
            f"H4_CLOSE_LT_EMA{length}",
            lambda c, v=length:
                c.__setitem__(
                    "h4_close_below_ema",
                    v,
                ),
        )

    for fast, slow in [
        (20, 50),
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
            f"H4_EMA{fast}_LT_{slow}",
            mutate_h4,
        )

    for value in [
        0.70,
        0.80,
        0.90,
        1.00,
        1.10,
        1.20,
    ]:
        add(
            "H4_ATR_MIN",
            f"H4ATR_MIN_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_h4_atr_ratio",
                    v,
                ),
        )

    for value in [
        0.80,
        0.90,
        1.00,
        1.10,
        1.20,
    ]:
        add(
            "H4_ATR_MAX",
            f"H4ATR_MAX_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "maximum_h4_atr_ratio",
                    v,
                ),
        )

    for length in [
        40,
        50,
        70,
        85,
        100,
        150,
        200,
        300,
    ]:
        add(
            "DAILY_CLOSE_EMA",
            f"D_CLOSE_LT_EMA{length}",
            lambda c, v=length:
                c.__setitem__(
                    "daily_close_below_ema",
                    v,
                ),
        )

    for fast, slow in [
        (20, 50),
        (40, 100),
        (50, 70),
        (50, 100),
        (50, 200),
        (85, 100),
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
            f"D_EMA{fast}_LT_{slow}",
            mutate_daily,
        )

    for value in [
        0.70,
        0.80,
        0.90,
        1.00,
        1.10,
        1.20,
    ]:
        add(
            "DAILY_ATR_MIN",
            f"DATR_MIN_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_daily_atr_ratio",
                    v,
                ),
        )

    for value in [
        0.80,
        0.90,
        1.00,
        1.10,
        1.20,
    ]:
        add(
            "DAILY_ATR_MAX",
            f"DATR_MAX_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "maximum_daily_atr_ratio",
                    v,
                ),
        )

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
    family_best,
):
    pool = []
    seen_families = set()

    for family, config in (
        family_best
    ):
        if family in seen_families:
            continue

        seen_families.add(
            family
        )

        pool.append(
            (
                family,
                config,
            )
        )

        if len(pool) >= 12:
            break

    interactions = []
    seen = set()
    counter = 0

    def overlay(
        base,
        source,
    ):
        result = clone_config(
            base,
            base["label"],
        )

        for key, value in (
            source.items()
        ):
            if key == "label":
                continue

            baseline_value = (
                BASELINE.get(key)
            )

            if value != baseline_value:
                if isinstance(
                    value,
                    set,
                ):
                    result[key] = set(
                        value
                    )
                else:
                    result[key] = value

        return result

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
                BASELINE,
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

            signature = (
                config_signature(
                    config
                )
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
# VALIDATION
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
        for (
            label,
            start,
            end,
        ) in windows:
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
            "positive":
                stats["total_r"] > 0,
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
        return ordered[
            n // 2
        ]

    return (
        ordered[
            n // 2 - 1
        ]
        +
        ordered[
            n // 2
        ]
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
                row["profit_factor"]
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
                "Building corrected M15/H1/H4/Daily features",
            "m15_candles":
                len(m15),
            "h1_candles":
                len(h1),
            "h4_candles":
                len(h4),
            "daily_candles":
                len(daily),
        })

        m15_atr = atr14(
            m15
        )

        h1_state = build_htf_state(
            h1,
            H1_EMA_LENGTHS,
        )

        h4_state = build_htf_state(
            h4,
            H4_EMA_LENGTHS,
        )

        daily_state = build_htf_state(
            daily,
            DAILY_EMA_LENGTHS,
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
                "Caching reusable GBPUSD M15 short outcomes",
            "engulfing_signals":
                len(signals),
        })

        cache = build_outcome_cache(
            m15,
            signals,
        )

        singles = (
            single_family_configs()
        )

        STATUS.update({
            "state":
                "calculating",
            "message":
                "Running single-family sweep",
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

            if number % 25 == 0:
                STATUS.update({
                    "state":
                        "calculating",
                    "message": (
                        "Single-family sweep "
                        f"{number}/"
                        f"{len(singles)}"
                    ),
                })

        write_csv(
            OUTPUT_SINGLE,
            single_rows,
        )

        primary_single = [
            row
            for row in single_rows
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

        primary_single.sort(
            key=lambda row: (
                float(
                    row[
                        "profit_factor"
                    ]
                ),
                float(
                    row[
                        "expectancy_r"
                    ]
                ),
                float(
                    row["total_r"]
                ),
            ),
            reverse=True,
        )

        write_csv(
            OUTPUT_SINGLE_TOP,
            primary_single[:150],
        )

        config_by_label = {
            config["label"]:
                config
            for _, config in singles
        }

        family_best_rows = {}

        for row in primary_single:
            family = row[
                "family"
            ]

            if (
                family
                in family_best_rows
            ):
                continue

            if (
                float(
                    row[
                        "profit_factor"
                    ]
                )
                >= 1.10
            ):
                family_best_rows[
                    family
                ] = row

        family_best = []

        for family, row in sorted(
            family_best_rows.items(),
            key=lambda item:
                float(
                    item[1][
                        "profit_factor"
                    ]
                ),
            reverse=True,
        ):
            family_best.append(
                (
                    family,
                    config_by_label[
                        row["candidate"]
                    ],
                )
            )

        interactions = (
            build_controlled_interactions(
                family_best
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

            STATUS.update({
                "state":
                    "calculating",
                "message": (
                    "Controlled interactions "
                    f"{number}/"
                    f"{len(interactions)}"
                ),
            })

        write_csv(
            OUTPUT_INTERACTIONS,
            interaction_rows,
        )

        baseline_trades = (
            run_config_cached(
                signals,
                cache,
                BASELINE,
                PRIMARY_COST_PIPS,
            )
        )

        baseline_row = result_row(
            "BASELINE",
            BASELINE,
            PRIMARY_COST_PIPS,
            baseline_trades,
        )

        combined_primary = [
            baseline_row
        ]

        combined_primary.extend(
            primary_single
        )

        combined_primary.extend(
            row
            for row
            in interaction_rows
            if (
                abs(
                    float(
                        row[
                            "cost_pips"
                        ]
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

        combined_primary.sort(
            key=lambda row: (
                float(
                    row[
                        "profit_factor"
                    ]
                ),
                float(
                    row[
                        "expectancy_r"
                    ]
                ),
                float(
                    row["total_r"]
                ),
            ),
            reverse=True,
        )

        top_rows = (
            combined_primary[:40]
        )

        write_csv(
            OUTPUT_TOP,
            top_rows,
        )

        all_configs = {
            "BASELINE":
                BASELINE
        }

        for _, config in singles:
            all_configs[
                config["label"]
            ] = config

        for _, config in interactions:
            all_configs[
                config["label"]
            ] = config

        finalists = [
            all_configs[
                row["candidate"]
            ]
            for row
            in top_rows[:15]
        ]

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

        STATUS.update({
            "state":
                "validating",
            "message":
                "Running dev / validation split",
        })

        devval_rows = (
            validation_rows(
                signals,
                cache,
                finalists,
                devval_windows(),
            )
        )

        STATUS.update({
            "state":
                "validating",
            "message":
                "Running recent 5Y / 2Y validation",
        })

        recent_rows = (
            validation_rows(
                signals,
                cache,
                finalists,
                recent_windows(),
            )
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
                if row[
                    "candidate"
                ] == label
            ]

            devval = [
                row
                for row
                in devval_rows
                if row[
                    "candidate"
                ] == label
            ]

            recent = [
                row
                for row
                in recent_rows
                if row[
                    "candidate"
                ] == label
            ]

            era_pfs = [
                float(
                    row[
                        "profit_factor"
                    ]
                )
                for row in eras
                if int(
                    row["trades"]
                ) > 0
            ]

            devval_pfs = [
                float(
                    row[
                        "profit_factor"
                    ]
                )
                for row in devval
                if int(
                    row["trades"]
                ) > 0
            ]

            recent_pfs = [
                float(
                    row[
                        "profit_factor"
                    ]
                )
                for row in recent
                if int(
                    row["trades"]
                ) > 0
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
                "full_pf":
                    float(
                        base[
                            "profit_factor"
                        ]
                    ),
                "full_expectancy":
                    float(
                        base[
                            "expectancy_r"
                        ]
                    ),
                "full_total_r":
                    float(
                        base[
                            "total_r"
                        ]
                    ),
                "trades":
                    int(
                        base["trades"]
                    ),
                "minimum_era_pf":
                    min_era,
                "minimum_devval_pf":
                    min_devval,
                "minimum_recent_pf":
                    min_recent,
                "score":
                    (
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
            all_configs[
                row["candidate"]
            ]
            for row
            in robust[:6]
        ]

        STATUS.update({
            "state":
                "validating",
            "message":
                "Running rolling 2Y / 3Y validation",
        })

        rolling_rows = []
        rolling_summary_rows = []

        for config in (
            robust_finalists
        ):
            for months in [
                24,
                36,
            ]:
                rows = (
                    monthly_rolling_rows(
                        signals,
                        cache,
                        config,
                        months,
                    )
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

        best = (
            robust_finalists[0]
            if robust_finalists
            else finalists[0]
            if finalists
            else BASELINE
        )

        best_trades = (
            run_config_cached(
                signals,
                cache,
                best,
                PRIMARY_COST_PIPS,
            )
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
                "GBP/USD M15 short exhaustive controlled pass complete",
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
            "single_configs":
                len(singles),
            "interaction_configs":
                len(interactions),
            "baseline":
                baseline_row,
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
            "GBPUSD M15 Short Exhaustive Controlled",
        "status":
            STATUS["state"],
        "instrument":
            INSTRUMENT,
        "timeframe":
            "M15",
        "side":
            "SELL",
        "orders_supported":
            False,
        "trading_enabled":
            False,
        "routes": [
            "/gbpusd-m15-short/status",
            "/gbpusd-m15-short/results",
        ],
    })


@app.route(
    "/gbpusd-m15-short/status"
)
def route_status():
    return jsonify(
        STATUS
    )


@app.route(
    "/gbpusd-m15-short/results"
)
def route_results():
    return download_file(
        OUTPUT_BUNDLE
    )


if __name__ == "__main__":
    research_thread = (
        threading.Thread(
            target=run_research,
            name=(
                "gbpusd-m15-short-"
                "exhaustive"
            ),
            daemon=True,
        )
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
