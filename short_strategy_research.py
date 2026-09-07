
import os
import csv
import time
import bisect
import threading
import zipfile
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from flask import Flask, jsonify, send_file


# ============================================================
# USD/CAD M15 LONG
# BROAD EXHAUSTIVE CONTROLLED RESEARCH
#
# ONE SELF-CONTAINED RESEARCH SCRIPT
# READ-ONLY. NEVER SENDS ORDERS.
#
# Purpose:
#   First broad USD/CAD M15 LONG research pass.
#   Exhaust distinct trigger / structure / momentum / volatility /
#   higher-timeframe / session hypotheses before any local tuning.
#   Do not import the locked H1 USD/CAD rules into M15 by default.
#
# Research families:
#
# A) TRIGGER FAMILIES
#   1. exact bullish engulfing
#   2. bullish body-engulf only
#   3. bullish close above previous high
#   4. bullish displacement after sweep
#
# B) LIQUIDITY SWEEP / RECLAIM
#   - prior 20 / 40 / 60 / 100-bar low sweep
#   - reclaim back above prior low
#   - previous-day-low sweep / reclaim
#
# C) PREVIOUS-DAY LOCATION
#   - lower 20/30/40% of previous day range
#   - near previous day low
#
# D) SELLOFF -> REVERSAL CONTEXT
#   - prior 4h / 8h / 12h move negative by ATR thresholds
#   - consecutive bearish bars before signal
#
# E) HTF PULLBACK / TREND
#   - prior completed H1 close above EMA50/100/200
#   - H1 EMA20 > EMA50 etc.
#   - signal price below/near H1 EMA20 after bullish H1 trend
#   - same on H4
#
# F) HTF STRUCTURAL LOWS
#   - proximity to prior H1 20/50 bar low
#   - proximity to prior H4 10/20 bar low
#
# G) COMPRESSION -> EXPANSION
#   - prior 4/8/12 bar average range compressed vs ATR
#   - signal range expands vs prior average
#
# H) MULTI-CANDLE REVERSAL
#   - N prior bearish candles
#   - signal closes above prior 2/3-bar high
#
# I) SESSION TRANSITIONS
#   - London open
#   - pre-NY / NY open
#   - London/NY overlap
#   - early US
#
# J) PRIOR-DAY STATE
#   - prior day bearish
#   - prior day range high/low vs ATR
#   - prior day close in lower part of own range
#
# K) CONTROLLED INTERACTIONS
#   Only independently useful families are paired.
#
# M15 development convention:
#   - 1.0 pip adverse entry baseline
#   - stress at 0.5 / 1.0 / 1.5 / 2.0 pips
#
# Anti-overfit:
#   - 1 pip development cost
#   - cost stress 0.5 / 1 / 1.5 / 2 pips
#   - 4 eras
#   - dev 2010-2017 vs validation 2018-now
#   - last 5Y / 2Y
#   - rolling monthly-start 2Y / 3Y
#   - trade count minimums
#
# Higher-timeframe correctness:
#   - H1/H4/D states become eligible only at actual completion
#   - complete_at = next actual HTF candle OPEN
#   - lookup uses completion timestamps (no raw-open lookahead)
#   - Daily uses actual next OANDA daily open, DST-safe
#   - prior momentum ends at M15[i-1] close, never signal close
#
# Historical conventions:
#   - OANDA midpoint candles
#   - M15 signal time = candle OPEN
#   - ATR14 Wilder/RMA, SMA-seeded
#   - stop = signal low - 10 ticks
#   - target based on REFERENCE signal-close risk
#   - adverse long entry = signal close + cost
#   - exits begin next candle
#   - same-bar tie:
#       high closer => target first
#       otherwise stop first
#   - pyramiding 0
#   - exact exit-candle signal eligible
#   - Daily alignment 17 America/New_York
# ============================================================


app = Flask(__name__)

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_BASE = os.getenv(
    "OANDA_API_URL",
    "https://api-fxtrade.oanda.com",
)

INSTRUMENT = "USD_CAD"

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
LONDON = ZoneInfo("Europe/London")

TICK_SIZE = 0.00001
PIP_SIZE = 0.0001
# USDCAD: 10 ticks = 0.0001 = 1 pip. M15 development cost remains 1.0 pip.
STOP_BUFFER_TICKS = 10

PRIMARY_COST_PIPS = 1.00

COST_PIPS_GRID = [
    0.50,
    1.00,
    1.50,
    2.00,
]

RR_VALUES = [
    2.50,
    3.00,
    3.50,
    3.75,
    4.00,
    4.50,
]

MIN_TRADES_PRIMARY = 60


# ============================================================
# OUTPUTS
# ============================================================

OUTPUT_BASELINES = (
    "usdcad_m15_long_exhaustive_baselines.csv"
)

OUTPUT_SINGLE = (
    "usdcad_m15_long_exhaustive_single_family.csv"
)

OUTPUT_SINGLE_TOP = (
    "usdcad_m15_long_exhaustive_single_top.csv"
)

OUTPUT_INTERACTIONS = (
    "usdcad_m15_long_exhaustive_interactions.csv"
)

OUTPUT_TOP = (
    "usdcad_m15_long_exhaustive_top.csv"
)

OUTPUT_ERAS = (
    "usdcad_m15_long_exhaustive_eras.csv"
)

OUTPUT_DEVVAL = (
    "usdcad_m15_long_exhaustive_dev_validation.csv"
)

OUTPUT_RECENT = (
    "usdcad_m15_long_exhaustive_recent.csv"
)

OUTPUT_ROLLING = (
    "usdcad_m15_long_exhaustive_rolling.csv"
)

OUTPUT_ROLLING_SUMMARY = (
    "usdcad_m15_long_exhaustive_rolling_summary.csv"
)

OUTPUT_BEST_TRADES = (
    "usdcad_m15_long_exhaustive_best_trades.csv"
)

OUTPUT_BUNDLE = (
    "usdcad_m15_long_exhaustive_RESULTS.zip"
)

STATUS = {
    "state": "not_started",
    "message": "USD/CAD M15 exhaustive research has not started",
    "service": "USDCAD M15 Long Exhaustive Controlled",
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
        OUTPUT_BASELINES,
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
        v is None
        for v in seed
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
        if (
            values[i] is None
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
            +
            values[i]
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
        v is None
        for v in seed
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
            length
            + 1.0
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
                1.0
                - alpha
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
                running
                / length
            )

    return result


def bullish_exact_engulf(
    previous,
    current,
):
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


def bullish_body_engulf(
    previous,
    current,
):
    previous_body_low = min(
        previous["open"],
        previous["close"],
    )

    previous_body_high = max(
        previous["open"],
        previous["close"],
    )

    current_body_low = min(
        current["open"],
        current["close"],
    )

    current_body_high = max(
        current["open"],
        current["close"],
    )

    return (
        previous["close"]
        <
        previous["open"]
        and
        current["close"]
        >
        current["open"]
        and
        current_body_low
        <=
        previous_body_low
        and
        current_body_high
        >=
        previous_body_high
    )


# ============================================================
# HTF STATE
# ============================================================

H1_EMA_LENGTHS = [
    20,
    50,
    100,
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
    50,
    100,
    200,
]


def build_htf_state(
    candles,
    ema_lengths,
    structural_lookbacks=None,
):
    """
    Build higher-timeframe state with an explicit completion timestamp.

    IMPORTANT:
    OANDA candle timestamps are candle OPEN times.

    A higher-timeframe candle is only available to an M15 signal when
    that HTF candle has fully completed. The safest historical mapping
    is therefore:

        complete_at = next HTF candle's OPEN time

    Example:
        H1 09:00 candle completes at 10:00.
        M15 signal opening 10:00 may use the 09:00 H1 candle.
        M15 signal opening 09:45 may NOT use it.

    This also handles OANDA daily candles aligned to 17:00 New York
    correctly across DST because the next actual daily candle timestamp
    defines completion instead of assuming a fixed 24-hour duration.
    """

    closes = [
        candle["close"]
        for candle in candles
    ]

    atr = atr14(candles)

    ema_map = {
        length:
            ema(
                closes,
                length,
            )
        for length in ema_lengths
    }

    rows = []

    # Last HTF candle has no observed next-open completion marker.
    # We intentionally do not expose it as completed state.
    for i, candle in enumerate(
        candles
    ):
        structural_lows = {}

        if structural_lookbacks:
            for lookback in structural_lookbacks:
                if i >= lookback:
                    structural_lows[
                        lookback
                    ] = min(
                        c["low"]
                        for c in candles[
                            i - lookback:i
                        ]
                    )
                else:
                    structural_lows[
                        lookback
                    ] = None

        complete_at = (
            candles[i + 1]["time"]
            if i + 1 < len(candles)
            else None
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
            "emas":
                {
                    length:
                        ema_map[
                            length
                        ][i]
                    for length in ema_lengths
                },
            "structural_lows":
                structural_lows,
        })

    return rows


def completed_state_index(
    completion_times,
    signal_time,
):
    """
    Return index of latest HTF candle whose completion time is
    <= the M15 signal candle OPEN time.
    """

    position = bisect.bisect_right(
        completion_times,
        signal_time,
    ) - 1

    return position


def previous_completed_state(
    state_rows,
    completion_times,
    signal_time,
):
    position = completed_state_index(
        completion_times,
        signal_time,
    )

    if position < 0:
        return None

    return state_rows[
        position
    ]


def previous_completed_daily_state(
    state_rows,
    completion_times,
    signal_time,
):
    return previous_completed_state(
        state_rows,
        completion_times,
        signal_time,
    )


# ============================================================
# FEATURE CACHE
# ============================================================

M15_SWEEP_LOOKBACKS = [
    20,
    40,
    60,
    100,
]

PRIOR_BEAR_SEQUENCE_LENGTHS = [
    2,
    3,
    4,
]

COMPRESSION_WINDOWS = [
    4,
    8,
    12,
]


def build_signal_universe(
    m15,
    m15_atr,
    h1_state,
    h4_state,
    daily_state,
):
    signals = []

    # Only states with a known observed completion time are
    # eligible. Since complete_at is the next candle OPEN, these arrays
    # are naturally chronological and safe for bisect.
    h1_completion_times = [
        row["complete_at"]
        for row in h1_state
        if row["complete_at"] is not None
    ]

    h4_completion_times = [
        row["complete_at"]
        for row in h4_state
        if row["complete_at"] is not None
    ]

    daily_completion_times = [
        row["complete_at"]
        for row in daily_state
        if row["complete_at"] is not None
    ]

    # State arrays aligned to the completion-time arrays above.
    h1_completed_rows = [
        row
        for row in h1_state
        if row["complete_at"] is not None
    ]

    h4_completed_rows = [
        row
        for row in h4_state
        if row["complete_at"] is not None
    ]

    daily_completed_rows = [
        row
        for row in daily_state
        if row["complete_at"] is not None
    ]

    max_lookback = max(
        max(M15_SWEEP_LOOKBACKS),
        96,
        12,
    )

    for i in range(
        max(
            20,
            max_lookback,
        ),
        len(m15),
    ):
        current = m15[i]
        previous = m15[i - 1]
        atr = m15_atr[i]

        if (
            atr is None
            or atr <= 0
        ):
            continue

        bullish = (
            current["close"]
            >
            current["open"]
        )

        if not bullish:
            continue

        body = (
            current["close"]
            -
            current["open"]
        )

        candle_range = (
            current["high"]
            -
            current["low"]
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

        range_atr = (
            candle_range / atr
        )

        close_location = (
            (
                current["close"]
                -
                current["low"]
            )
            / candle_range
            if candle_range > 0
            else 0.0
        )

        exact_engulf = bullish_exact_engulf(
            previous,
            current,
        )

        body_engulf = bullish_body_engulf(
            previous,
            current,
        )

        close_above_prev_high = (
            current["close"]
            >
            previous["high"]
        )

        sweep_info = {}

        for lookback in M15_SWEEP_LOOKBACKS:
            prior_low = min(
                c["low"]
                for c in m15[
                    i - lookback:i
                ]
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

            sweep_info[
                lookback
            ] = {
                "prior_low":
                    prior_low,
                "swept":
                    swept,
                "reclaimed":
                    reclaimed,
                "distance_atr":
                    (
                        abs(
                            current["low"]
                            -
                            prior_low
                        )
                        /
                        atr
                    ),
            }

        # Strictly PRE-SIGNAL selloff context.
        # End at the previous completed M15 candle, so the current
        # bullish reversal candle cannot manufacture its own context.
        prior_close = m15[
            i - 1
        ]["close"]

        selloff_4h = (
            prior_close
            -
            m15[
                i - 1 - 16
            ]["close"]
        ) / atr

        selloff_8h = (
            prior_close
            -
            m15[
                i - 1 - 32
            ]["close"]
        ) / atr

        selloff_12h = (
            prior_close
            -
            m15[
                i - 1 - 48
            ]["close"]
        ) / atr

        bearish_sequence = {}

        for n in PRIOR_BEAR_SEQUENCE_LENGTHS:
            bearish_sequence[
                n
            ] = all(
                m15[j]["close"]
                <
                m15[j]["open"]
                for j in range(
                    i - n,
                    i,
                )
            )

        close_above_prior_2_high = (
            current["close"]
            >
            max(
                m15[
                    i - 1
                ]["high"],
                m15[
                    i - 2
                ]["high"],
            )
        )

        close_above_prior_3_high = (
            current["close"]
            >
            max(
                m15[
                    i - 1
                ]["high"],
                m15[
                    i - 2
                ]["high"],
                m15[
                    i - 3
                ]["high"],
            )
        )

        compression = {}

        for window in COMPRESSION_WINDOWS:
            prior_ranges = [
                (
                    m15[j]["high"]
                    -
                    m15[j]["low"]
                )
                for j in range(
                    i - window,
                    i,
                )
            ]

            avg_prior_range = (
                sum(prior_ranges)
                / len(prior_ranges)
            )

            compression[
                window
            ] = {
                "avg_range_atr":
                    (
                        avg_prior_range
                        /
                        atr
                    ),
                "expansion_multiple":
                    (
                        candle_range
                        /
                        avg_prior_range
                        if avg_prior_range > 0
                        else 0.0
                    ),
            }

        ny_time = (
            current["time"]
            .astimezone(NY)
        )

        london_time = (
            current["time"]
            .astimezone(LONDON)
        )

        h1_prev = previous_completed_state(
            h1_completed_rows,
            h1_completion_times,
            current["time"],
        )

        h4_prev = previous_completed_state(
            h4_completed_rows,
            h4_completion_times,
            current["time"],
        )

        daily_prev = (
            previous_completed_daily_state(
                daily_completed_rows,
                daily_completion_times,
                current["time"],
            )
        )

        prev_day_low_sweep = False
        prev_day_low_reclaim = False
        prev_day_location = None
        prev_day_near_low_atr = None
        prior_day_bearish = None
        prior_day_close_location = None
        prior_day_range_atr = None

        if (
            daily_prev is not None
            and
            daily_prev["atr14"] is not None
            and
            daily_prev["atr14"] > 0
        ):
            prev_day_low = daily_prev["low"]
            prev_day_high = daily_prev["high"]
            prev_day_range = (
                prev_day_high
                -
                prev_day_low
            )

            prev_day_low_sweep = (
                current["low"]
                <
                prev_day_low
            )

            prev_day_low_reclaim = (
                prev_day_low_sweep
                and
                current["close"]
                >
                prev_day_low
            )

            if prev_day_range > 0:
                prev_day_location = (
                    current["close"]
                    -
                    prev_day_low
                ) / prev_day_range

                prior_day_close_location = (
                    daily_prev["close"]
                    -
                    prev_day_low
                ) / prev_day_range

            prev_day_near_low_atr = (
                abs(
                    current["low"]
                    -
                    prev_day_low
                )
                /
                atr
            )

            prior_day_bearish = (
                daily_prev["close"]
                <
                daily_prev["open"]
            )

            prior_day_range_atr = (
                prev_day_range
                /
                daily_prev["atr14"]
            )

        h1_pullback_distance_ema20_atr = None
        h4_pullback_distance_ema20_atr = None
        h1_low_distance = {}
        h4_low_distance = {}

        if (
            h1_prev is not None
            and
            h1_prev["atr14"] is not None
            and
            h1_prev["atr14"] > 0
        ):
            ema20 = h1_prev["emas"].get(20)

            if ema20 is not None:
                h1_pullback_distance_ema20_atr = (
                    current["close"]
                    -
                    ema20
                ) / h1_prev["atr14"]

            for lb in [
                20,
                50,
            ]:
                structural_low = (
                    h1_prev[
                        "structural_lows"
                    ].get(lb)
                )

                if structural_low is not None:
                    h1_low_distance[
                        lb
                    ] = (
                        abs(
                            current["low"]
                            -
                            structural_low
                        )
                        /
                        h1_prev["atr14"]
                    )
                else:
                    h1_low_distance[
                        lb
                    ] = None

        if (
            h4_prev is not None
            and
            h4_prev["atr14"] is not None
            and
            h4_prev["atr14"] > 0
        ):
            ema20 = h4_prev["emas"].get(20)

            if ema20 is not None:
                h4_pullback_distance_ema20_atr = (
                    current["close"]
                    -
                    ema20
                ) / h4_prev["atr14"]

            for lb in [
                10,
                20,
            ]:
                structural_low = (
                    h4_prev[
                        "structural_lows"
                    ].get(lb)
                )

                if structural_low is not None:
                    h4_low_distance[
                        lb
                    ] = (
                        abs(
                            current["low"]
                            -
                            structural_low
                        )
                        /
                        h4_prev["atr14"]
                    )
                else:
                    h4_low_distance[
                        lb
                    ] = None

        signals.append({
            "signal_index":
                i,
            "time":
                current["time"],
            "exact_engulf":
                exact_engulf,
            "body_engulf":
                body_engulf,
            "close_above_prev_high":
                close_above_prev_high,
            "body_ratio":
                body_ratio,
            "body_atr":
                body_atr,
            "range_atr":
                range_atr,
            "close_location":
                close_location,
            "sweep_info":
                sweep_info,
            "selloff_4h_atr":
                selloff_4h,
            "selloff_8h_atr":
                selloff_8h,
            "selloff_12h_atr":
                selloff_12h,
            "bearish_sequence":
                bearish_sequence,
            "close_above_prior_2_high":
                close_above_prior_2_high,
            "close_above_prior_3_high":
                close_above_prior_3_high,
            "compression":
                compression,
            "ny_hour":
                ny_time.hour,
            "ny_weekday":
                ny_time.weekday(),
            "london_hour":
                london_time.hour,
            "h1_prev":
                h1_prev,
            "h4_prev":
                h4_prev,
            "daily_prev":
                daily_prev,
            "prev_day_low_sweep":
                prev_day_low_sweep,
            "prev_day_low_reclaim":
                prev_day_low_reclaim,
            "prev_day_location":
                prev_day_location,
            "prev_day_near_low_atr":
                prev_day_near_low_atr,
            "prior_day_bearish":
                prior_day_bearish,
            "prior_day_close_location":
                prior_day_close_location,
            "prior_day_range_atr":
                prior_day_range_atr,
            "h1_pullback_distance_ema20_atr":
                h1_pullback_distance_ema20_atr,
            "h4_pullback_distance_ema20_atr":
                h4_pullback_distance_ema20_atr,
            "h1_low_distance":
                h1_low_distance,
            "h4_low_distance":
                h4_low_distance,
        })

    return signals


# ============================================================
# CONFIG MODEL
# ============================================================

BASE_CONFIG = {
    "label":
        "BASE",
    "trigger":
        "EXACT_ENGULF",
    "minimum_body_ratio":
        1.00,
    "minimum_body_atr":
        None,
    "minimum_range_atr":
        None,
    "minimum_close_location":
        None,

    "sweep_lookback":
        None,
    "require_sweep":
        False,
    "require_reclaim":
        False,

    "require_prev_day_low_sweep":
        False,
    "require_prev_day_low_reclaim":
        False,
    "maximum_prev_day_location":
        None,
    "maximum_prev_day_low_distance_atr":
        None,

    "maximum_selloff_4h_atr":
        None,
    "maximum_selloff_8h_atr":
        None,
    "maximum_selloff_12h_atr":
        None,

    "minimum_prior_bearish_bars":
        None,
    "require_close_above_prior_2_high":
        False,
    "require_close_above_prior_3_high":
        False,

    "compression_window":
        None,
    "maximum_prior_avg_range_atr":
        None,
    "minimum_expansion_multiple":
        None,

    "included_ny_hours":
        None,
    "excluded_weekdays":
        set(),

    "h1_close_above_ema":
        None,
    "h1_fast_ema":
        None,
    "h1_slow_ema":
        None,
    "minimum_h1_pullback_ema20_atr":
        None,
    "maximum_h1_pullback_ema20_atr":
        None,
    "maximum_h1_low_distance_20_atr":
        None,
    "maximum_h1_low_distance_50_atr":
        None,

    "h4_close_above_ema":
        None,
    "h4_fast_ema":
        None,
    "h4_slow_ema":
        None,
    "minimum_h4_pullback_ema20_atr":
        None,
    "maximum_h4_pullback_ema20_atr":
        None,
    "maximum_h4_low_distance_10_atr":
        None,
    "maximum_h4_low_distance_20_atr":
        None,

    "require_prior_day_bearish":
        False,
    "maximum_prior_day_close_location":
        None,
    "minimum_prior_day_range_atr":
        None,
    "maximum_prior_day_range_atr":
        None,

    "reward_risk":
        3.50,
}


# ============================================================
# FILTER HELPERS
# ============================================================

def state_close_above_ema(
    state,
    length,
):
    if state is None:
        return False

    value = state["emas"].get(
        length
    )

    return (
        value is not None
        and
        state["close"] > value
    )


def state_ema_alignment(
    state,
    fast,
    slow,
):
    if state is None:
        return False

    fast_value = state["emas"].get(
        fast
    )

    slow_value = state["emas"].get(
        slow
    )

    return (
        fast_value is not None
        and
        slow_value is not None
        and
        fast_value > slow_value
    )


def trigger_passes(
    signal,
    config,
):
    trigger = config["trigger"]

    if trigger == "EXACT_ENGULF":
        return signal["exact_engulf"]

    if trigger == "BODY_ENGULF":
        return signal["body_engulf"]

    if trigger == "CLOSE_ABOVE_PREV_HIGH":
        return (
            signal["close_above_prev_high"]
        )

    if trigger == "SWEEP_DISPLACEMENT":
        # broad displacement definition:
        # any 20/40/60/100 sweep + bullish candle
        # + close above previous high.
        swept_any = any(
            info["swept"]
            for info in signal[
                "sweep_info"
            ].values()
        )

        return (
            swept_any
            and
            signal["close_above_prev_high"]
        )

    return False


def signal_passes(
    signal,
    config,
):
    if not trigger_passes(
        signal,
        config,
    ):
        return False

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
        config["minimum_close_location"]
        is not None
        and
        signal["close_location"]
        <
        config["minimum_close_location"]
    ):
        return False

    sweep_lb = config["sweep_lookback"]

    if sweep_lb is not None:
        info = signal[
            "sweep_info"
        ][sweep_lb]

        if (
            config["require_sweep"]
            and
            not info["swept"]
        ):
            return False

        if (
            config["require_reclaim"]
            and
            not info["reclaimed"]
        ):
            return False

    if (
        config["require_prev_day_low_sweep"]
        and
        not signal["prev_day_low_sweep"]
    ):
        return False

    if (
        config["require_prev_day_low_reclaim"]
        and
        not signal["prev_day_low_reclaim"]
    ):
        return False

    if (
        config["maximum_prev_day_location"]
        is not None
    ):
        if (
            signal["prev_day_location"]
            is None
            or
            signal["prev_day_location"]
            >
            config[
                "maximum_prev_day_location"
            ]
        ):
            return False

    if (
        config[
            "maximum_prev_day_low_distance_atr"
        ]
        is not None
    ):
        if (
            signal[
                "prev_day_near_low_atr"
            ]
            is None
            or
            signal[
                "prev_day_near_low_atr"
            ]
            >
            config[
                "maximum_prev_day_low_distance_atr"
            ]
        ):
            return False

    for horizon in [
        "4h",
        "8h",
        "12h",
    ]:
        maximum = config[
            f"maximum_selloff_{horizon}_atr"
        ]

        if maximum is not None:
            if (
                signal[
                    f"selloff_{horizon}_atr"
                ]
                >
                maximum
            ):
                return False

    if (
        config[
            "minimum_prior_bearish_bars"
        ]
        is not None
    ):
        n = config[
            "minimum_prior_bearish_bars"
        ]

        if not signal[
            "bearish_sequence"
        ][n]:
            return False

    if (
        config[
            "require_close_above_prior_2_high"
        ]
        and
        not signal[
            "close_above_prior_2_high"
        ]
    ):
        return False

    if (
        config[
            "require_close_above_prior_3_high"
        ]
        and
        not signal[
            "close_above_prior_3_high"
        ]
    ):
        return False

    comp_window = config[
        "compression_window"
    ]

    if comp_window is not None:
        comp = signal[
            "compression"
        ][comp_window]

        if (
            config[
                "maximum_prior_avg_range_atr"
            ]
            is not None
            and
            comp[
                "avg_range_atr"
            ]
            >
            config[
                "maximum_prior_avg_range_atr"
            ]
        ):
            return False

        if (
            config[
                "minimum_expansion_multiple"
            ]
            is not None
            and
            comp[
                "expansion_multiple"
            ]
            <
            config[
                "minimum_expansion_multiple"
            ]
        ):
            return False

    if (
        config["included_ny_hours"]
        is not None
        and
        signal["ny_hour"]
        not in
        config["included_ny_hours"]
    ):
        return False

    if (
        signal["ny_weekday"]
        in
        config["excluded_weekdays"]
    ):
        return False

    h1 = signal["h1_prev"]

    if (
        config["h1_close_above_ema"]
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
        config["h1_fast_ema"]
        is not None
        and
        config["h1_slow_ema"]
        is not None
        and
        not state_ema_alignment(
            h1,
            config["h1_fast_ema"],
            config["h1_slow_ema"],
        )
    ):
        return False

    if (
        config[
            "minimum_h1_pullback_ema20_atr"
        ]
        is not None
    ):
        value = signal[
            "h1_pullback_distance_ema20_atr"
        ]

        if (
            value is None
            or
            value
            <
            config[
                "minimum_h1_pullback_ema20_atr"
            ]
        ):
            return False

    if (
        config[
            "maximum_h1_pullback_ema20_atr"
        ]
        is not None
    ):
        value = signal[
            "h1_pullback_distance_ema20_atr"
        ]

        if (
            value is None
            or
            value
            >
            config[
                "maximum_h1_pullback_ema20_atr"
            ]
        ):
            return False

    for lb in [
        20,
        50,
    ]:
        threshold = config[
            f"maximum_h1_low_distance_{lb}_atr"
        ]

        if threshold is not None:
            value = signal[
                "h1_low_distance"
            ].get(lb)

            if (
                value is None
                or
                value > threshold
            ):
                return False

    h4 = signal["h4_prev"]

    if (
        config["h4_close_above_ema"]
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
        config["h4_fast_ema"]
        is not None
        and
        config["h4_slow_ema"]
        is not None
        and
        not state_ema_alignment(
            h4,
            config["h4_fast_ema"],
            config["h4_slow_ema"],
        )
    ):
        return False

    if (
        config[
            "minimum_h4_pullback_ema20_atr"
        ]
        is not None
    ):
        value = signal[
            "h4_pullback_distance_ema20_atr"
        ]

        if (
            value is None
            or
            value
            <
            config[
                "minimum_h4_pullback_ema20_atr"
            ]
        ):
            return False

    if (
        config[
            "maximum_h4_pullback_ema20_atr"
        ]
        is not None
    ):
        value = signal[
            "h4_pullback_distance_ema20_atr"
        ]

        if (
            value is None
            or
            value
            >
            config[
                "maximum_h4_pullback_ema20_atr"
            ]
        ):
            return False

    for lb in [
        10,
        20,
    ]:
        threshold = config[
            f"maximum_h4_low_distance_{lb}_atr"
        ]

        if threshold is not None:
            value = signal[
                "h4_low_distance"
            ].get(lb)

            if (
                value is None
                or
                value > threshold
            ):
                return False

    if (
        config["require_prior_day_bearish"]
        and
        signal["prior_day_bearish"]
        is not True
    ):
        return False

    if (
        config[
            "maximum_prior_day_close_location"
        ]
        is not None
    ):
        value = signal[
            "prior_day_close_location"
        ]

        if (
            value is None
            or
            value
            >
            config[
                "maximum_prior_day_close_location"
            ]
        ):
            return False

    if (
        config[
            "minimum_prior_day_range_atr"
        ]
        is not None
    ):
        value = signal[
            "prior_day_range_atr"
        ]

        if (
            value is None
            or
            value
            <
            config[
                "minimum_prior_day_range_atr"
            ]
        ):
            return False

    if (
        config[
            "maximum_prior_day_range_atr"
        ]
        is not None
    ):
        value = signal[
            "prior_day_range_atr"
        ]

        if (
            value is None
            or
            value
            >
            config[
                "maximum_prior_day_range_atr"
            ]
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
        reference_entry
        -
        stop
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
        backtest_entry
        -
        stop
    )

    if actual_risk <= 0:
        return None

    for j in range(
        signal_index + 1,
        len(candles),
    ):
        candle = candles[j]

        hit_stop = (
            candle["low"]
            <= stop
        )

        hit_target = (
            candle["high"]
            >= target
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
                exit_price = target
                exit_reason = "TARGET"
            else:
                exit_price = stop
                exit_reason = "STOP"

        elif hit_target:
            exit_price = target
            exit_reason = "TARGET"

        elif hit_stop:
            exit_price = stop
            exit_reason = "STOP"

        else:
            continue

        result_r = (
            exit_price
            -
            backtest_entry
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
                        "state": "precomputing",
                        "message": (
                            "Caching reusable outcomes "
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
# BACKTEST ENGINE
# ============================================================

CANDIDATE_CACHE = {}


def candidate_cache_key(
    config,
):
    return config_signature(
        config
    )


def qualifying_candidates(
    signals,
    config,
):
    """
    Cache the full-history qualifying signal list for each configuration.
    Windowed validation then slices this much smaller list rather than
    re-running every filter against the entire signal universe.
    """

    key = candidate_cache_key(
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
    all_candidates = qualifying_candidates(
        signals,
        config,
    )

    if (
        start is None
        and
        end is None
    ):
        candidates = all_candidates
    else:
        candidate_times = [
            signal["time"]
            for signal in all_candidates
        ]

        left = (
            0
            if start is None
            else bisect.bisect_left(
                candidate_times,
                start,
            )
        )

        right = (
            len(all_candidates)
            if end is None
            else bisect.bisect_left(
                candidate_times,
                end,
            )
        )

        candidates = all_candidates[
            left:right
        ]

    indices = [
        signal["signal_index"]
        for signal in candidates
    ]

    trades = []
    position = 0

    while position < len(candidates):
        signal = candidates[
            position
        ]

        trade = cache.get(
            (
                signal["signal_index"],
                config["reward_risk"],
                cost_pips,
            )
        )

        if trade is None:
            position += 1
            continue

        trades.append(
            dict(trade)
        )

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

    return {
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
            stats[
                "longest_loss_streak"
            ],
        "trigger":
            config["trigger"],
        "minimum_body_ratio":
            config["minimum_body_ratio"],
        "minimum_body_atr":
            config["minimum_body_atr"],
        "minimum_range_atr":
            config["minimum_range_atr"],
        "minimum_close_location":
            config["minimum_close_location"],
        "sweep_lookback":
            config["sweep_lookback"],
        "require_sweep":
            config["require_sweep"],
        "require_reclaim":
            config["require_reclaim"],
        "require_prev_day_low_sweep":
            config[
                "require_prev_day_low_sweep"
            ],
        "require_prev_day_low_reclaim":
            config[
                "require_prev_day_low_reclaim"
            ],
        "maximum_prev_day_location":
            config[
                "maximum_prev_day_location"
            ],
        "maximum_prev_day_low_distance_atr":
            config[
                "maximum_prev_day_low_distance_atr"
            ],
        "maximum_selloff_4h_atr":
            config[
                "maximum_selloff_4h_atr"
            ],
        "maximum_selloff_8h_atr":
            config[
                "maximum_selloff_8h_atr"
            ],
        "maximum_selloff_12h_atr":
            config[
                "maximum_selloff_12h_atr"
            ],
        "minimum_prior_bearish_bars":
            config[
                "minimum_prior_bearish_bars"
            ],
        "require_close_above_prior_2_high":
            config[
                "require_close_above_prior_2_high"
            ],
        "require_close_above_prior_3_high":
            config[
                "require_close_above_prior_3_high"
            ],
        "compression_window":
            config[
                "compression_window"
            ],
        "maximum_prior_avg_range_atr":
            config[
                "maximum_prior_avg_range_atr"
            ],
        "minimum_expansion_multiple":
            config[
                "minimum_expansion_multiple"
            ],
        "included_ny_hours":
            (
                None
                if config["included_ny_hours"]
                is None
                else ",".join(
                    str(h)
                    for h in sorted(
                        config[
                            "included_ny_hours"
                        ]
                    )
                )
            ),
        "excluded_weekdays":
            ",".join(
                str(d)
                for d in sorted(
                    config[
                        "excluded_weekdays"
                    ]
                )
            ),
        "h1_close_above_ema":
            config[
                "h1_close_above_ema"
            ],
        "h1_fast_ema":
            config[
                "h1_fast_ema"
            ],
        "h1_slow_ema":
            config[
                "h1_slow_ema"
            ],
        "minimum_h1_pullback_ema20_atr":
            config[
                "minimum_h1_pullback_ema20_atr"
            ],
        "maximum_h1_pullback_ema20_atr":
            config[
                "maximum_h1_pullback_ema20_atr"
            ],
        "maximum_h1_low_distance_20_atr":
            config[
                "maximum_h1_low_distance_20_atr"
            ],
        "maximum_h1_low_distance_50_atr":
            config[
                "maximum_h1_low_distance_50_atr"
            ],
        "h4_close_above_ema":
            config[
                "h4_close_above_ema"
            ],
        "h4_fast_ema":
            config[
                "h4_fast_ema"
            ],
        "h4_slow_ema":
            config[
                "h4_slow_ema"
            ],
        "minimum_h4_pullback_ema20_atr":
            config[
                "minimum_h4_pullback_ema20_atr"
            ],
        "maximum_h4_pullback_ema20_atr":
            config[
                "maximum_h4_pullback_ema20_atr"
            ],
        "maximum_h4_low_distance_10_atr":
            config[
                "maximum_h4_low_distance_10_atr"
            ],
        "maximum_h4_low_distance_20_atr":
            config[
                "maximum_h4_low_distance_20_atr"
            ],
        "require_prior_day_bearish":
            config[
                "require_prior_day_bearish"
            ],
        "maximum_prior_day_close_location":
            config[
                "maximum_prior_day_close_location"
            ],
        "minimum_prior_day_range_atr":
            config[
                "minimum_prior_day_range_atr"
            ],
        "maximum_prior_day_range_atr":
            config[
                "maximum_prior_day_range_atr"
            ],
        "reward_risk":
            config["reward_risk"],
    }


# ============================================================
# BASELINES / SINGLE FAMILY CONFIGS
# ============================================================

def baseline_configs():
    rows = []

    for trigger in [
        "EXACT_ENGULF",
        "BODY_ENGULF",
        "CLOSE_ABOVE_PREV_HIGH",
        "SWEEP_DISPLACEMENT",
    ]:
        config = clone_config(
            BASE_CONFIG,
            f"BASE_{trigger}",
        )

        config["trigger"] = trigger

        rows.append(
            (
                "TRIGGER_BASELINE",
                config,
            )
        )

    return rows


def single_family_configs():
    rows = []

    def add(
        family,
        label,
        mutator,
        trigger="EXACT_ENGULF",
    ):
        config = clone_config(
            BASE_CONFIG,
            label,
        )

        config["trigger"] = trigger
        mutator(config)

        rows.append(
            (
                family,
                config,
            )
        )

    # --------------------------------------------------------
    # Trigger + candle-quality variants
    # --------------------------------------------------------

    for trigger in [
        "EXACT_ENGULF",
        "BODY_ENGULF",
        "CLOSE_ABOVE_PREV_HIGH",
        "SWEEP_DISPLACEMENT",
    ]:
        for body_atr in [
            0.50,
            0.75,
            1.00,
            1.25,
        ]:
            add(
                "TRIGGER_BODY",
                (
                    f"{trigger}_"
                    f"BA{body_atr:.2f}"
                ),
                lambda c,
                v=body_atr:
                    c.__setitem__(
                        "minimum_body_atr",
                        v,
                    ),
                trigger=trigger,
            )

    # --------------------------------------------------------
    # Liquidity sweep / reclaim
    # --------------------------------------------------------

    for lb in M15_SWEEP_LOOKBACKS:
        add(
            "SWEEP",
            f"SWEEP_{lb}",
            lambda c,
            lookback=lb:
                (
                    c.__setitem__(
                        "sweep_lookback",
                        lookback,
                    ),
                    c.__setitem__(
                        "require_sweep",
                        True,
                    )
                ),
        )

        add(
            "SWEEP_RECLAIM",
            f"SWEEP_RECLAIM_{lb}",
            lambda c,
            lookback=lb:
                (
                    c.__setitem__(
                        "sweep_lookback",
                        lookback,
                    ),
                    c.__setitem__(
                        "require_sweep",
                        True,
                    ),
                    c.__setitem__(
                        "require_reclaim",
                        True,
                    )
                ),
        )

    add(
        "PREV_DAY_SWEEP",
        "PREV_DAY_LOW_SWEEP",
        lambda c:
            c.__setitem__(
                "require_prev_day_low_sweep",
                True,
            ),
    )

    add(
        "PREV_DAY_RECLAIM",
        "PREV_DAY_LOW_RECLAIM",
        lambda c:
            c.__setitem__(
                "require_prev_day_low_reclaim",
                True,
            ),
    )

    # --------------------------------------------------------
    # Prior-day location
    # --------------------------------------------------------

    for value in [
        0.20,
        0.30,
        0.40,
    ]:
        add(
            "PREV_DAY_LOCATION",
            f"PREV_DAY_LOC_MAX_{value:.2f}",
            lambda c,
            v=value:
                c.__setitem__(
                    "maximum_prev_day_location",
                    v,
                ),
        )

    for value in [
        0.25,
        0.50,
        0.75,
        1.00,
    ]:
        add(
            "PREV_DAY_LOW_DISTANCE",
            f"PREV_DAY_LOW_DIST_{value:.2f}",
            lambda c,
            v=value:
                c.__setitem__(
                    "maximum_prev_day_low_distance_atr",
                    v,
                ),
        )

    # --------------------------------------------------------
    # Selloff -> reversal
    # --------------------------------------------------------

    for horizon in [
        "4h",
        "8h",
        "12h",
    ]:
        for threshold in [
            -0.50,
            -1.00,
            -1.50,
            -2.00,
            -3.00,
        ]:
            key = (
                f"maximum_selloff_"
                f"{horizon}_atr"
            )

            add(
                f"SELLOFF_{horizon}",
                (
                    f"SELLOFF_{horizon}_"
                    f"{threshold:+.2f}"
                ),
                lambda c,
                k=key,
                v=threshold:
                    c.__setitem__(
                        k,
                        v,
                    ),
            )

    # --------------------------------------------------------
    # Multi-candle reversal
    # --------------------------------------------------------

    for n in PRIOR_BEAR_SEQUENCE_LENGTHS:
        add(
            "BEAR_SEQUENCE",
            f"PRIOR_{n}_BEAR",
            lambda c,
            value=n:
                c.__setitem__(
                    "minimum_prior_bearish_bars",
                    value,
                ),
        )

    add(
        "DISPLACEMENT",
        "CLOSE_ABOVE_PRIOR_2_HIGH",
        lambda c:
            c.__setitem__(
                "require_close_above_prior_2_high",
                True,
            ),
    )

    add(
        "DISPLACEMENT",
        "CLOSE_ABOVE_PRIOR_3_HIGH",
        lambda c:
            c.__setitem__(
                "require_close_above_prior_3_high",
                True,
            ),
    )

    # --------------------------------------------------------
    # Compression -> expansion
    # --------------------------------------------------------

    for window in COMPRESSION_WINDOWS:
        for max_avg in [
            0.60,
            0.75,
            0.90,
            1.00,
        ]:
            add(
                "COMPRESSION",
                (
                    f"COMP_{window}_"
                    f"MAX{max_avg:.2f}"
                ),
                lambda c,
                w=window,
                v=max_avg:
                    (
                        c.__setitem__(
                            "compression_window",
                            w,
                        ),
                        c.__setitem__(
                            "maximum_prior_avg_range_atr",
                            v,
                        )
                    ),
            )

        for expansion in [
            1.25,
            1.50,
            1.75,
            2.00,
        ]:
            add(
                "EXPANSION",
                (
                    f"EXP_{window}_"
                    f"MIN{expansion:.2f}"
                ),
                lambda c,
                w=window,
                v=expansion:
                    (
                        c.__setitem__(
                            "compression_window",
                            w,
                        ),
                        c.__setitem__(
                            "minimum_expansion_multiple",
                            v,
                        )
                    ),
            )

    # --------------------------------------------------------
    # Session transitions
    # --------------------------------------------------------

    session_sets = {
        "LONDON_OPEN":
            set(range(3, 7)),
        "LONDON_MORNING":
            set(range(3, 9)),
        "PRE_NY":
            set(range(7, 9)),
        "NY_OPEN":
            set(range(8, 11)),
        "LONDON_NY_OVERLAP":
            set(range(8, 12)),
        "EARLY_US":
            set(range(9, 13)),
    }

    for label, hours in session_sets.items():
        add(
            "SESSION",
            label,
            lambda c,
            h=hours:
                c.__setitem__(
                    "included_ny_hours",
                    set(h),
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
            "WEEKDAY",
            f"EX_{name}",
            lambda c,
            d=day:
                c.__setitem__(
                    "excluded_weekdays",
                    {d},
                ),
        )

    # --------------------------------------------------------
    # H1 trend / pullback / structure
    # --------------------------------------------------------

    for length in [
        50,
        100,
        200,
    ]:
        add(
            "H1_TREND",
            f"H1_CLOSE_GT_EMA{length}",
            lambda c,
            v=length:
                c.__setitem__(
                    "h1_close_above_ema",
                    v,
                ),
        )

    for fast, slow in [
        (20, 50),
        (50, 100),
        (50, 200),
    ]:
        def mutate_h1_align(
            c,
            f=fast,
            s=slow,
        ):
            c["h1_fast_ema"] = f
            c["h1_slow_ema"] = s

        add(
            "H1_ALIGNMENT",
            f"H1_EMA{fast}_GT_{slow}",
            mutate_h1_align,
        )

    for maximum in [
        -0.25,
        0.00,
        0.25,
        0.50,
    ]:
        add(
            "H1_PULLBACK",
            f"H1_EMA20_PULLBACK_MAX_{maximum:+.2f}",
            lambda c,
            v=maximum:
                c.__setitem__(
                    "maximum_h1_pullback_ema20_atr",
                    v,
                ),
        )

    for lb in [
        20,
        50,
    ]:
        for value in [
            0.25,
            0.50,
            0.75,
            1.00,
        ]:
            key = (
                f"maximum_h1_low_distance_"
                f"{lb}_atr"
            )

            add(
                "H1_STRUCT_LOW",
                f"H1_LOW{lb}_DIST_{value:.2f}",
                lambda c,
                k=key,
                v=value:
                    c.__setitem__(
                        k,
                        v,
                    ),
            )

    # --------------------------------------------------------
    # H4 trend / pullback / structure
    # --------------------------------------------------------

    for length in [
        50,
        100,
        200,
    ]:
        add(
            "H4_TREND",
            f"H4_CLOSE_GT_EMA{length}",
            lambda c,
            v=length:
                c.__setitem__(
                    "h4_close_above_ema",
                    v,
                ),
        )

    for fast, slow in [
        (20, 50),
        (50, 100),
        (50, 200),
    ]:
        def mutate_h4_align(
            c,
            f=fast,
            s=slow,
        ):
            c["h4_fast_ema"] = f
            c["h4_slow_ema"] = s

        add(
            "H4_ALIGNMENT",
            f"H4_EMA{fast}_GT_{slow}",
            mutate_h4_align,
        )

    for maximum in [
        -0.25,
        0.00,
        0.25,
        0.50,
    ]:
        add(
            "H4_PULLBACK",
            f"H4_EMA20_PULLBACK_MAX_{maximum:+.2f}",
            lambda c,
            v=maximum:
                c.__setitem__(
                    "maximum_h4_pullback_ema20_atr",
                    v,
                ),
        )

    for lb in [
        10,
        20,
    ]:
        for value in [
            0.25,
            0.50,
            0.75,
            1.00,
        ]:
            key = (
                f"maximum_h4_low_distance_"
                f"{lb}_atr"
            )

            add(
                "H4_STRUCT_LOW",
                f"H4_LOW{lb}_DIST_{value:.2f}",
                lambda c,
                k=key,
                v=value:
                    c.__setitem__(
                        k,
                        v,
                    ),
            )

    # --------------------------------------------------------
    # Prior-day state
    # --------------------------------------------------------

    add(
        "PRIOR_DAY_STATE",
        "PRIOR_DAY_BEARISH",
        lambda c:
            c.__setitem__(
                "require_prior_day_bearish",
                True,
            ),
    )

    for value in [
        0.25,
        0.40,
        0.50,
    ]:
        add(
            "PRIOR_DAY_STATE",
            f"PRIOR_DAY_CLOSE_LOC_MAX_{value:.2f}",
            lambda c,
            v=value:
                c.__setitem__(
                    "maximum_prior_day_close_location",
                    v,
                ),
        )

    for value in [
        0.80,
        1.00,
        1.20,
        1.40,
    ]:
        add(
            "PRIOR_DAY_RANGE_MIN",
            f"PRIOR_DAY_RANGE_MIN_{value:.2f}",
            lambda c,
            v=value:
                c.__setitem__(
                    "minimum_prior_day_range_atr",
                    v,
                ),
        )

    for value in [
        0.80,
        1.00,
        1.20,
    ]:
        add(
            "PRIOR_DAY_RANGE_MAX",
            f"PRIOR_DAY_RANGE_MAX_{value:.2f}",
            lambda c,
            v=value:
                c.__setitem__(
                    "maximum_prior_day_range_atr",
                    v,
                ),
        )

    # --------------------------------------------------------
    # RR
    # --------------------------------------------------------

    for rr in RR_VALUES:
        add(
            "RR",
            f"RR_{rr:.2f}",
            lambda c,
            v=rr:
                c.__setitem__(
                    "reward_risk",
                    v,
                ),
        )

    return rows


# ============================================================
# CONTROLLED INTERACTIONS
# ============================================================

def config_signature(config):
    return tuple(
        sorted(
            (
                key,
                tuple(
                    sorted(value)
                )
                if isinstance(value, set)
                else value,
            )
            for key, value in config.items()
            if key != "label"
        )
    )


def build_controlled_interactions(
    family_best,
):
    pool = []
    seen_families = set()

    for family, config in family_best:
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

        for key, value in source.items():
            if key == "label":
                continue

            baseline_value = BASE_CONFIG.get(
                key
            )

            if value != baseline_value:
                if isinstance(value, set):
                    result[key] = set(value)
                else:
                    result[key] = value

        return result

    for i in range(len(pool)):
        for j in range(
            i + 1,
            len(pool),
        ):
            family_a, config_a = pool[i]
            family_b, config_b = pool[j]

            config = clone_config(
                BASE_CONFIG,
                "TEMP",
            )

            # Preserve trigger from first if changed.
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
            - timedelta(days=500),
            RESEARCH_TO,
            120,
        )

        h4 = fetch_history(
            "H4",
            RESEARCH_FROM
            - timedelta(days=1000),
            RESEARCH_TO,
            500,
        )

        daily = fetch_history(
            "D",
            RESEARCH_FROM
            - timedelta(days=1500),
            RESEARCH_TO,
            2500,
            daily_alignment=True,
        )

        if len(m15) < 1000:
            raise RuntimeError(
                "Too few M15 candles returned"
            )

        STATUS.update({
            "state": "precomputing",
            "message":
                "Building M15 / H1 / H4 / Daily features",
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
            H1_EMA_LENGTHS,
            structural_lookbacks=[
                20,
                50,
            ],
        )

        h4_state = build_htf_state(
            h4,
            H4_EMA_LENGTHS,
            structural_lookbacks=[
                10,
                20,
            ],
        )

        daily_state = build_htf_state(
            daily,
            DAILY_EMA_LENGTHS,
            structural_lookbacks=None,
        )

        signals = build_signal_universe(
            m15,
            m15_atr,
            h1_state,
            h4_state,
            daily_state,
        )

        STATUS.update({
            "state": "precomputing",
            "message":
                "Caching reusable trade outcomes",
            "signal_universe":
                len(signals),
        })

        cache = build_outcome_cache(
            m15,
            signals,
        )

        # ----------------------------------------------------
        # BASELINES
        # ----------------------------------------------------

        baselines = baseline_configs()
        baseline_rows = []

        for family, config in baselines:
            for cost in COST_PIPS_GRID:
                trades = run_config_cached(
                    signals,
                    cache,
                    config,
                    cost,
                )

                baseline_rows.append(
                    result_row(
                        family,
                        config,
                        cost,
                        trades,
                    )
                )

        write_csv(
            OUTPUT_BASELINES,
            baseline_rows,
        )

        # ----------------------------------------------------
        # SINGLE FAMILY
        # ----------------------------------------------------

        singles = single_family_configs()

        STATUS.update({
            "state": "calculating",
            "message":
                "Running Gen2 single-family hypotheses",
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
                    "state": "calculating",
                    "message": (
                        "Gen2 single-family "
                        f"{number}/{len(singles)}"
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

        write_csv(
            OUTPUT_SINGLE_TOP,
            primary_single[:150],
        )

        config_by_label = {
            config["label"]:
                config
            for _, config in singles
        }

        # Best result per family only if it shows independent
        # evidence above a mild floor.
        family_best_rows = {}

        for row in primary_single:
            family = row["family"]

            if family in family_best_rows:
                continue

            if (
                float(
                    row["profit_factor"]
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

        # ----------------------------------------------------
        # CONTROLLED INTERACTIONS
        # ----------------------------------------------------

        interactions = (
            build_controlled_interactions(
                family_best
            )
        )

        STATUS.update({
            "state": "calculating",
            "message":
                "Running controlled Gen2 interactions",
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

            if number % 20 == 0:
                STATUS.update({
                    "state": "calculating",
                    "message": (
                        "Gen2 interactions "
                        f"{number}/{len(interactions)}"
                    ),
                })

        write_csv(
            OUTPUT_INTERACTIONS,
            interaction_rows,
        )

        # ----------------------------------------------------
        # FINALIST POOL
        # ----------------------------------------------------

        combined_primary = []

        combined_primary.extend(
            row
            for row in baseline_rows
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

        combined_primary.extend(
            primary_single
        )

        combined_primary.extend(
            row
            for row in interaction_rows
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

        combined_primary.sort(
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

        top_rows = combined_primary[:40]

        write_csv(
            OUTPUT_TOP,
            top_rows,
        )

        all_configs = {}

        for _, config in baselines:
            all_configs[
                config["label"]
            ] = config

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
            for row in top_rows[:15]
        ]

        # ----------------------------------------------------
        # VALIDATION
        # ----------------------------------------------------

        STATUS.update({
            "state": "validating",
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
            "state": "validating",
            "message":
                "Running dev / validation split",
        })

        devval_rows = validation_rows(
            signals,
            cache,
            finalists,
            devval_windows(),
        )

        STATUS.update({
            "state": "validating",
            "message":
                "Running recent 5Y / 2Y validation",
        })

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

        robust = []

        for config in finalists:
            label = config["label"]

            base = next(
                row
                for row in top_rows
                if row["candidate"]
                == label
            )

            eras = [
                row
                for row in era_rows
                if row["candidate"]
                == label
            ]

            devval = [
                row
                for row in devval_rows
                if row["candidate"]
                == label
            ]

            recent = [
                row
                for row in recent_rows
                if row["candidate"]
                == label
            ]

            era_pfs = [
                float(
                    row["profit_factor"]
                )
                for row in eras
                if int(
                    row["trades"]
                ) > 0
            ]

            devval_pfs = [
                float(
                    row["profit_factor"]
                )
                for row in devval
                if int(
                    row["trades"]
                ) > 0
            ]

            recent_pfs = [
                float(
                    row["profit_factor"]
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
                        base["total_r"]
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
            for row in robust[:6]
        ]

        STATUS.update({
            "state": "validating",
            "message":
                "Running rolling 2Y / 3Y validation",
        })

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

                rolling_rows.extend(rows)

                rolling_summary_rows.append(
                    rolling_summary(rows)
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
            else clone_config(
                BASE_CONFIG,
                "NO_WINNER",
            )
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
            "state": "packaging",
            "message":
                "Building single ZIP results bundle",
        })

        build_results_bundle()

        STATUS.update({
            "state": "complete",
            "message":
                "USD/CAD M15 Gen2 structural/context research complete",
            "m15_candles":
                len(m15),
            "h1_candles":
                len(h1),
            "h4_candles":
                len(h4),
            "daily_candles":
                len(daily),
            "signal_universe":
                len(signals),
            "single_configs":
                len(singles),
            "interaction_configs":
                len(interactions),
            "robust_ranking":
                robust[:12],
            "selected_best":
                best,
            "outputs": {
                "baselines":
                    OUTPUT_BASELINES,
                "single_family":
                    OUTPUT_SINGLE,
                "single_top":
                    OUTPUT_SINGLE_TOP,
                "interactions":
                    OUTPUT_INTERACTIONS,
                "top":
                    OUTPUT_TOP,
                "eras":
                    OUTPUT_ERAS,
                "dev_validation":
                    OUTPUT_DEVVAL,
                "recent":
                    OUTPUT_RECENT,
                "rolling":
                    OUTPUT_ROLLING,
                "rolling_summary":
                    OUTPUT_ROLLING_SUMMARY,
                "best_trades":
                    OUTPUT_BEST_TRADES,
                "results_bundle":
                    OUTPUT_BUNDLE,
            },
        })

        print()
        print("=" * 100)
        print(
            "USD/CAD M15 LONG EXHAUSTIVE COMPLETE"
        )
        print("=" * 100)
        print(
            "Signals:",
            len(signals),
        )
        print(
            "Single configs:",
            len(singles),
        )
        print(
            "Interactions:",
            len(interactions),
        )
        print(
            "Selected best:",
            best,
        )
        print(
            "Robust ranking:"
        )

        for row in robust[:12]:
            print(row)

    except Exception as error:
        STATUS.update({
            "state": "error",
            "message": str(error),
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
            "USDCAD M15 Long Exhaustive Controlled",
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
            "/usdcad-m15-long/status",
            "/usdcad-m15-long/baselines",
            "/usdcad-m15-long/single-family",
            "/usdcad-m15-long/single-top",
            "/usdcad-m15-long/interactions",
            "/usdcad-m15-long/top",
            "/usdcad-m15-long/eras",
            "/usdcad-m15-long/dev-validation",
            "/usdcad-m15-long/recent",
            "/usdcad-m15-long/rolling",
            "/usdcad-m15-long/rolling-summary",
            "/usdcad-m15-long/best-trades",
            "/usdcad-m15-long/results",
        ],
    })


@app.route(
    "/usdcad-m15-long/status"
)
def route_status():
    return jsonify(
        STATUS
    )


@app.route(
    "/usdcad-m15-long/baselines"
)
def route_baselines():
    return download_file(
        OUTPUT_BASELINES
    )


@app.route(
    "/usdcad-m15-long/single-family"
)
def route_single_family():
    return download_file(
        OUTPUT_SINGLE
    )


@app.route(
    "/usdcad-m15-long/single-top"
)
def route_single_top():
    return download_file(
        OUTPUT_SINGLE_TOP
    )


@app.route(
    "/usdcad-m15-long/interactions"
)
def route_interactions():
    return download_file(
        OUTPUT_INTERACTIONS
    )


@app.route(
    "/usdcad-m15-long/top"
)
def route_top():
    return download_file(
        OUTPUT_TOP
    )


@app.route(
    "/usdcad-m15-long/eras"
)
def route_eras():
    return download_file(
        OUTPUT_ERAS
    )


@app.route(
    "/usdcad-m15-long/dev-validation"
)
def route_devval():
    return download_file(
        OUTPUT_DEVVAL
    )


@app.route(
    "/usdcad-m15-long/recent"
)
def route_recent():
    return download_file(
        OUTPUT_RECENT
    )


@app.route(
    "/usdcad-m15-long/rolling"
)
def route_rolling():
    return download_file(
        OUTPUT_ROLLING
    )


@app.route(
    "/usdcad-m15-long/rolling-summary"
)
def route_rolling_summary():
    return download_file(
        OUTPUT_ROLLING_SUMMARY
    )


@app.route(
    "/usdcad-m15-long/best-trades"
)
def route_best_trades():
    return download_file(
        OUTPUT_BEST_TRADES
    )


@app.route(
    "/usdcad-m15-long/results"
)
def route_results():
    return download_file(
        OUTPUT_BUNDLE
    )


if __name__ == "__main__":
    research_thread = threading.Thread(
        target=run_research,
        name="usdcad-m15-long-exhaustive",
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
