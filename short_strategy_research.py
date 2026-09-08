
import os
import csv
import time
import bisect
import zipfile
import threading
from datetime import datetime, timedelta, timezone

import requests
from flask import Flask, jsonify, send_file


# ============================================================
# USD/CAD M15 LONG — FROZEN FULL-HISTORY VALIDATION
#
# PURPOSE
# -------
# Validate the ALREADY-LOCKED USD/CAD M15 LONG strategy on
# additional history that played no role in strategy development.
#
# THERE IS NO OPTIMIZATION IN THIS SCRIPT.
# PARAMETERS MUST NOT BE CHANGED BASED ON THE PRE-2010 RESULT.
#
# REQUESTED HISTORY
# -----------------
# Start request:
#   2002-05-06 20:00 UTC
#
# OANDA may not provide M15 candles all the way back to that
# timestamp for USD_CAD. The script therefore reports:
#   - requested start
#   - actual first M15 candle returned
#   - actual first H4 candle returned
#
# FROZEN STRATEGY
# ---------------
# Instrument:
#   OANDA USD_CAD
#
# Timeframe:
#   M15
#
# Side:
#   LONG / BUY
#
# Trigger:
#   COMPRESSION_BREAKOUT
#
# Locked rules:
#   1) Signal candle bullish
#   2) Previous completed M15 ATR14 /
#      previous completed 20-bar ATR14 mean <= 0.72
#   3) Signal body >= 1.30 ATR14
#   4) Signal range >= 1.60 ATR14
#   5) Signal close > previous 10-bar high
#   6) Previous COMPLETED H4 close > H4 EMA100
#
# RR:
#   4.00R
#
# Stop:
#   signal low - 10 ticks
#
# USD/CAD:
#   tick = 0.00001
#   pip  = 0.0001
#
# Historical M15 development cost:
#   1.0 pip adverse long fill
#
# Cost stress:
#   0.5 / 1.0 / 1.5 / 2.0 pips
#
# Target:
#   based on REFERENCE signal-close risk
#
# ATR:
#   Wilder / RMA
#   SMA seeded
#
# Pyramiding:
#   0
#
# Exit convention:
#   exits start NEXT candle
#   exact exit-candle signal is eligible
#
# Same-bar tie:
#   long:
#     if candle high is closer to candle open -> target first
#     otherwise -> stop first
#
# HTF NO-LOOKAHEAD
# ----------------
# H4 state becomes eligible only when that H4 candle has actually
# completed.
#
# complete_at = next actual H4 candle OPEN
# lookup = bisect_right(completion_times, signal_open_time) - 1
#
# OUTPUT
# ------
# One ZIP route:
#   /usdcad-m15-long-frozen-history/results
#
# READ ONLY. NEVER SENDS ORDERS.
# ============================================================


app = Flask(__name__)

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_BASE = os.getenv(
    "OANDA_API_URL",
    "https://api-fxtrade.oanda.com",
)

INSTRUMENT = "USD_CAD"

REQUESTED_FROM = datetime(
    2002, 5, 6, 20, 0,
    tzinfo=timezone.utc,
)

DEVELOPMENT_FROM = datetime(
    2010, 1, 1,
    tzinfo=timezone.utc,
)

RESEARCH_TO = (
    datetime.now(timezone.utc)
    .replace(minute=0, second=0, microsecond=0)
)

# H4 warm-up before requested M15 start.
H4_FETCH_FROM = (
    REQUESTED_FROM
    - timedelta(days=180)
)

TICK_SIZE = 0.00001
PIP_SIZE = 0.0001
STOP_BUFFER_TICKS = 10

LOCKED_COMPRESSION = 0.72
LOCKED_BODY_ATR = 1.30
LOCKED_RANGE_ATR = 1.60
LOCKED_BREAKOUT_LOOKBACK = 10
LOCKED_H4_EMA = 100
LOCKED_RR = 4.00

PRIMARY_COST_PIPS = 1.00
COST_PIPS_GRID = [
    0.50,
    1.00,
    1.50,
    2.00,
]

OUTPUT_COVERAGE = (
    "usdcad_m15_long_frozen_history_coverage.csv"
)

OUTPUT_SUMMARY = (
    "usdcad_m15_long_frozen_history_summary.csv"
)

OUTPUT_WINDOWS = (
    "usdcad_m15_long_frozen_history_windows.csv"
)

OUTPUT_COST = (
    "usdcad_m15_long_frozen_history_cost_stress.csv"
)

OUTPUT_YEARS = (
    "usdcad_m15_long_frozen_history_calendar_years.csv"
)

OUTPUT_YEAR_SUMMARY = (
    "usdcad_m15_long_frozen_history_calendar_summary.csv"
)

OUTPUT_TRADES = (
    "usdcad_m15_long_frozen_history_trades.csv"
)

OUTPUT_BUNDLE = (
    "usdcad_m15_long_FROZEN_full_history_validation_RESULTS.zip"
)

STATUS = {
    "state": "not_started",
    "message": (
        "USD/CAD M15 LONG frozen-history validation not started"
    ),
    "service": (
        "USDCAD M15 Long Frozen Full-History Validation"
    ),
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
            value = (
                left
                + "."
                + fraction
                + "+"
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
            "error": "Output not ready yet",
            "path": path,
        }), 404

    return send_file(
        os.path.abspath(path),
        as_attachment=True,
        download_name=os.path.basename(path),
    )


def build_bundle():
    files = [
        OUTPUT_COVERAGE,
        OUTPUT_SUMMARY,
        OUTPUT_WINDOWS,
        OUTPUT_COST,
        OUTPUT_YEARS,
        OUTPUT_YEAR_SUMMARY,
        OUTPUT_TRADES,
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


def median(values):
    ordered = sorted(values)

    if not ordered:
        return 0.0

    n = len(ordered)

    if n % 2:
        return ordered[n // 2]

    return (
        ordered[n // 2 - 1]
        + ordered[n // 2]
    ) / 2.0


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
):
    url = (
        f"{OANDA_BASE}/v3/instruments/"
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

    response = requests.get(
        url,
        headers=oanda_headers(),
        params=params,
        timeout=60,
    )

    # Old OANDA history may simply not exist for some requested
    # intervals. Treat an empty successful response as no data.
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
            "state": "fetching",
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
            # If the API rejects an unavailable very-old interval,
            # report it but keep moving forward through history.
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

    for i, candle in enumerate(
        candles
    ):
        if i == 0:
            result[i] = (
                candle["high"]
                - candle["low"]
            )
        else:
            previous_close = (
                candles[i - 1][
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


def rma(
    values,
    length,
):
    result = [
        None
    ] * len(values)

    if len(values) < length:
        return result

    seed = values[
        :length
    ]

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
        if (
            values[i] is None
            or result[i - 1]
            is None
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
        true_ranges(
            candles
        ),
        14,
    )


def sma(
    values,
    length,
):
    result = [
        None
    ] * len(values)

    queue = []
    running = 0.0
    valid = 0

    for i, value in enumerate(
        values
    ):
        queue.append(
            value
        )

        if value is not None:
            running += value
            valid += 1

        if len(queue) > length:
            removed = (
                queue.pop(0)
            )

            if removed is not None:
                running -= removed
                valid -= 1

        if (
            len(queue) == length
            and valid == length
        ):
            result[i] = (
                running
                / length
            )

    return result


def ema(
    values,
    length,
):
    result = [
        None
    ] * len(values)

    if len(values) < length:
        return result

    seed = values[
        :length
    ]

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
        / (
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
            or result[i - 1]
            is None
        ):
            continue

        result[i] = (
            alpha
            * values[i]
            + (
                1.0 - alpha
            )
            * result[i - 1]
        )

    return result


# ============================================================
# COMPLETED H4 STATE
# ============================================================

def build_h4_state(
    h4,
):
    closes = [
        candle["close"]
        for candle in h4
    ]

    ema100_values = ema(
        closes,
        LOCKED_H4_EMA,
    )

    rows = []

    for i, candle in enumerate(
        h4
    ):
        # Last H4 candle cannot be exposed because we do not
        # know its actual next-open completion marker.
        complete_at = (
            h4[i + 1]["time"]
            if i + 1 < len(h4)
            else None
        )

        rows.append({
            "time":
                candle["time"],
            "complete_at":
                complete_at,
            "close":
                candle["close"],
            "ema100":
                ema100_values[i],
        })

    return rows


def previous_completed_h4(
    rows,
    completion_times,
    signal_time,
):
    pos = bisect.bisect_right(
        completion_times,
        signal_time,
    ) - 1

    if pos < 0:
        return None

    return rows[pos]


# ============================================================
# FROZEN SIGNAL CACHE
# ============================================================

def build_signal_cache(
    m15,
    m15_atr,
    h4_state,
):
    atr_mean20 = sma(
        m15_atr,
        20,
    )

    h4_rows = [
        row
        for row in h4_state
        if row[
            "complete_at"
        ] is not None
    ]

    h4_times = [
        row["complete_at"]
        for row in h4_rows
    ]

    signals = []

    # Need at least:
    #   ATR warmup + ATR mean20 + prior10 high.
    for i in range(
        40,
        len(m15),
    ):
        current = m15[i]
        atr = m15_atr[i]

        if (
            atr is None
            or atr <= 0
        ):
            continue

        prior_atr = (
            m15_atr[
                i - 1
            ]
        )

        prior_atr_mean20 = (
            atr_mean20[
                i - 1
            ]
        )

        if (
            prior_atr is None
            or prior_atr_mean20
            is None
            or prior_atr_mean20
            <= 0
        ):
            continue

        # 1) Previous completed M15 compression.
        compression = (
            prior_atr
            / prior_atr_mean20
        )

        if (
            compression
            > LOCKED_COMPRESSION
        ):
            continue

        # 2) Bullish signal candle.
        body_signed = (
            current["close"]
            - current["open"]
        )

        if body_signed <= 0:
            continue

        body_atr = (
            body_signed
            / atr
        )

        if (
            body_atr
            < LOCKED_BODY_ATR
        ):
            continue

        candle_range = (
            current["high"]
            - current["low"]
        )

        if candle_range <= 0:
            continue

        range_atr = (
            candle_range
            / atr
        )

        if (
            range_atr
            < LOCKED_RANGE_ATR
        ):
            continue

        # 5) Close > previous 10-bar high.
        prior_high10 = max(
            candle["high"]
            for candle in m15[
                i
                - LOCKED_BREAKOUT_LOOKBACK:
                i
            ]
        )

        if not (
            current["close"]
            > prior_high10
        ):
            continue

        # 6) Previous COMPLETED H4 close > EMA100.
        h4_prev = (
            previous_completed_h4(
                h4_rows,
                h4_times,
                current["time"],
            )
        )

        if h4_prev is None:
            continue

        h4_ema100 = (
            h4_prev["ema100"]
        )

        if h4_ema100 is None:
            continue

        if not (
            h4_prev["close"]
            > h4_ema100
        ):
            continue

        signals.append({
            "signal_index":
                i,
            "time":
                current["time"],
            "compression_ratio":
                compression,
            "body_atr":
                body_atr,
            "range_atr":
                range_atr,
            "prior_high10":
                prior_high10,
            "h4_close":
                h4_prev["close"],
            "h4_ema100":
                h4_ema100,
        })

    return signals


# ============================================================
# TRADE OUTCOME
# ============================================================

OUTCOME_CACHE = {}


def compute_trade_outcome(
    candles,
    signal_index,
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
        signal["low"]
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
        + LOCKED_RR
        * reference_risk
    )

    backtest_entry = (
        reference_entry
        + cost_pips
        * PIP_SIZE
    )

    actual_risk = (
        backtest_entry
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
            candle["low"]
            <= stop
        )

        hit_target = (
            candle["high"]
            >= target
        )

        if (
            hit_stop
            and hit_target
        ):
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
                < distance_low
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
            "result_r":
                result_r,
            "reward_risk":
                LOCKED_RR,
            "cost_pips":
                cost_pips,
            "exit_reason":
                exit_reason,
        }

    return None


def get_outcome(
    candles,
    signal_index,
    cost_pips,
):
    key = (
        signal_index,
        cost_pips,
    )

    if key not in OUTCOME_CACHE:
        OUTCOME_CACHE[
            key
        ] = compute_trade_outcome(
            candles,
            signal_index,
            cost_pips,
        )

    return OUTCOME_CACHE[
        key
    ]


# ============================================================
# PYRAMIDING-0 BACKTEST
# ============================================================

def run_backtest(
    candles,
    signals,
    cost_pips,
    start=None,
    end=None,
):
    candidates = signals

    if (
        start is not None
        or end is not None
    ):
        times = [
            signal["time"]
            for signal
            in candidates
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
            len(candidates)
            if end is None
            else bisect.bisect_left(
                times,
                end,
            )
        )

        candidates = (
            candidates[
                left:right
            ]
        )

    indices = [
        signal[
            "signal_index"
        ]
        for signal
        in candidates
    ]

    trades = []
    position = 0

    while (
        position
        < len(candidates)
    ):
        signal = (
            candidates[
                position
            ]
        )

        trade = get_outcome(
            candles,
            signal[
                "signal_index"
            ],
            cost_pips,
        )

        if trade is None:
            position += 1
            continue

        trades.append(
            dict(trade)
        )

        # Exact exit-candle signal is eligible.
        position = bisect.bisect_left(
            indices,
            trade[
                "exit_index"
            ],
            lo=position + 1,
        )

    return trades


# ============================================================
# STATS
# ============================================================

def stats_from_trades(
    trades,
):
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

    gross_profit = sum(
        winners
    )

    gross_loss = abs(
        sum(losers)
    )

    total_r = sum(
        results
    )

    profit_factor = (
        gross_profit
        / gross_loss
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
            max_dd,
        "longest_loss_streak":
            longest,
    }


def stats_row(
    label,
    start,
    end,
    trades,
    cost_pips,
):
    stats = stats_from_trades(
        trades
    )

    return {
        "window":
            label,
        "start_utc":
            iso_utc(start)
            if start is not None
            else "",
        "end_utc":
            iso_utc(end)
            if end is not None
            else "",
        "cost_pips":
            cost_pips,
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
    }


# ============================================================
# CALENDAR YEARS
# ============================================================

def calendar_year_rows(
    candles,
    signals,
):
    rows = []

    if not candles:
        return rows

    first_year = max(
        candles[0]["time"].year,
        REQUESTED_FROM.year,
    )

    # Completed years only.
    last_year = (
        RESEARCH_TO.year
        - 1
    )

    for year in range(
        first_year,
        last_year + 1,
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
            candles,
            signals,
            PRIMARY_COST_PIPS,
            start=start,
            end=end,
        )

        stats = stats_from_trades(
            trades
        )

        rows.append({
            "year":
                year,
            "period":
                (
                    "PRE_2010"
                    if year < 2010
                    else "DEVELOPMENT_2010_PLUS"
                ),
            "trades":
                stats["trades"],
            "winners":
                stats["winners"],
            "losers":
                stats["losers"],
            "win_rate":
                round(
                    stats[
                        "win_rate"
                    ],
                    4,
                ),
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
            "positive_year":
                stats["total_r"] > 0,
            "negative_year":
                stats["total_r"] < 0,
            "zero_trade_year":
                stats["trades"] == 0,
        })

    return rows


def calendar_summary(
    rows,
):
    output = []

    for label, selector in [
        (
            "PRE_2010",
            lambda row:
                int(row["year"])
                < 2010,
        ),
        (
            "2010_PLUS",
            lambda row:
                int(row["year"])
                >= 2010,
        ),
        (
            "ALL_COMPLETED_YEARS",
            lambda row:
                True,
        ),
    ]:
        subset = [
            row
            for row in rows
            if selector(row)
        ]

        if not subset:
            continue

        active = [
            row
            for row in subset
            if int(
                row["trades"]
            ) > 0
        ]

        positive = sum(
            1
            for row in subset
            if row[
                "positive_year"
            ]
        )

        negative = sum(
            1
            for row in subset
            if row[
                "negative_year"
            ]
        )

        zero = sum(
            1
            for row in subset
            if row[
                "zero_trade_year"
            ]
        )

        positive_active = sum(
            1
            for row in active
            if row[
                "positive_year"
            ]
        )

        output.append({
            "period":
                label,
            "completed_years":
                len(subset),
            "active_years":
                len(active),
            "positive_years":
                positive,
            "negative_years":
                negative,
            "zero_trade_years":
                zero,
            "positive_years_pct":
                round(
                    positive
                    / len(subset)
                    * 100.0,
                    4,
                ),
            "positive_active_years_pct":
                round(
                    positive_active
                    / len(active)
                    * 100.0,
                    4,
                )
                if active
                else 0.0,
            "median_trades_per_year":
                round(
                    median([
                        int(
                            row[
                                "trades"
                            ]
                        )
                        for row
                        in subset
                    ]),
                    4,
                ),
            "median_calendar_r":
                round(
                    median([
                        float(
                            row[
                                "total_r"
                            ]
                        )
                        for row
                        in subset
                    ]),
                    4,
                ),
            "worst_calendar_r":
                round(
                    min(
                        float(
                            row[
                                "total_r"
                            ]
                        )
                        for row
                        in subset
                    ),
                    4,
                ),
            "best_calendar_r":
                round(
                    max(
                        float(
                            row[
                                "total_r"
                            ]
                        )
                        for row
                        in subset
                    ),
                    4,
                ),
        })

    return output


# ============================================================
# RUNNER
# ============================================================

def run_research():
    try:
        # ----------------------------------------------------
        # FETCH
        # ----------------------------------------------------
        m15 = fetch_history(
            "M15",
            REQUESTED_FROM,
            RESEARCH_TO,
            45,
        )

        h4 = fetch_history(
            "H4",
            H4_FETCH_FROM,
            RESEARCH_TO,
            700,
        )

        if not m15:
            raise RuntimeError(
                "No M15 candles returned"
            )

        if not h4:
            raise RuntimeError(
                "No H4 candles returned"
            )

        actual_m15_start = (
            m15[0]["time"]
        )

        actual_m15_end = (
            m15[-1]["time"]
        )

        actual_h4_start = (
            h4[0]["time"]
        )

        actual_h4_end = (
            h4[-1]["time"]
        )

        coverage_rows = [{
            "instrument":
                INSTRUMENT,
            "requested_m15_start_utc":
                iso_utc(
                    REQUESTED_FROM
                ),
            "actual_first_m15_utc":
                iso_utc(
                    actual_m15_start
                ),
            "actual_last_m15_utc":
                iso_utc(
                    actual_m15_end
                ),
            "actual_first_h4_utc":
                iso_utc(
                    actual_h4_start
                ),
            "actual_last_h4_utc":
                iso_utc(
                    actual_h4_end
                ),
            "m15_candles":
                len(m15),
            "h4_candles":
                len(h4),
            "pre_2010_history_available":
                actual_m15_start
                < DEVELOPMENT_FROM,
        }]

        write_csv(
            OUTPUT_COVERAGE,
            coverage_rows,
        )

        # ----------------------------------------------------
        # FEATURES
        # ----------------------------------------------------
        STATUS.update({
            "state":
                "precomputing",
            "message":
                "Computing frozen strategy signals",
            "m15_candles":
                len(m15),
            "h4_candles":
                len(h4),
            "actual_first_m15":
                iso_utc(
                    actual_m15_start
                ),
        })

        m15_atr = atr14(
            m15
        )

        h4_state = (
            build_h4_state(
                h4
            )
        )

        signals = (
            build_signal_cache(
                m15,
                m15_atr,
                h4_state,
            )
        )

        # ----------------------------------------------------
        # PRIMARY 1-PIP HISTORY WINDOWS
        # ----------------------------------------------------
        STATUS["message"] = (
            "Running frozen full-history windows"
        )

        full_start = (
            actual_m15_start
        )

        pre2010_end = min(
            DEVELOPMENT_FROM,
            RESEARCH_TO,
        )

        windows = [
            (
                "FULL_AVAILABLE_HISTORY",
                full_start,
                RESEARCH_TO,
            ),
        ]

        if (
            actual_m15_start
            < DEVELOPMENT_FROM
        ):
            windows.append(
                (
                    "PRE_2010_PSEUDO_OOS",
                    actual_m15_start,
                    pre2010_end,
                )
            )

        windows.append(
            (
                "DEVELOPMENT_2010_PLUS",
                max(
                    DEVELOPMENT_FROM,
                    actual_m15_start,
                ),
                RESEARCH_TO,
            )
        )

        # Also show post-lock recent segments for context.
        windows.extend([
            (
                "2010_2013",
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
                "2014_2017",
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
                "2018_2021",
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
                "2022_NOW",
                datetime(
                    2022, 1, 1,
                    tzinfo=timezone.utc,
                ),
                RESEARCH_TO,
            ),
        ])

        window_rows = []

        for (
            label,
            start,
            end,
        ) in windows:
            if start >= end:
                continue

            trades = run_backtest(
                m15,
                signals,
                PRIMARY_COST_PIPS,
                start=start,
                end=end,
            )

            window_rows.append(
                stats_row(
                    label,
                    start,
                    end,
                    trades,
                    PRIMARY_COST_PIPS,
                )
            )

        write_csv(
            OUTPUT_WINDOWS,
            window_rows,
        )

        # Main summary is intentionally the three key windows only.
        summary_rows = [
            row
            for row in window_rows
            if row["window"] in (
                "FULL_AVAILABLE_HISTORY",
                "PRE_2010_PSEUDO_OOS",
                "DEVELOPMENT_2010_PLUS",
            )
        ]

        write_csv(
            OUTPUT_SUMMARY,
            summary_rows,
        )

        # ----------------------------------------------------
        # COST STRESS — FULL / PRE2010 / 2010+
        # ----------------------------------------------------
        STATUS["message"] = (
            "Running frozen-strategy cost stress"
        )

        cost_rows = []

        key_periods = [
            (
                "FULL_AVAILABLE_HISTORY",
                actual_m15_start,
                RESEARCH_TO,
            ),
            (
                "DEVELOPMENT_2010_PLUS",
                max(
                    DEVELOPMENT_FROM,
                    actual_m15_start,
                ),
                RESEARCH_TO,
            ),
        ]

        if (
            actual_m15_start
            < DEVELOPMENT_FROM
        ):
            key_periods.insert(
                1,
                (
                    "PRE_2010_PSEUDO_OOS",
                    actual_m15_start,
                    DEVELOPMENT_FROM,
                ),
            )

        for (
            label,
            start,
            end,
        ) in key_periods:
            for cost in (
                COST_PIPS_GRID
            ):
                trades = run_backtest(
                    m15,
                    signals,
                    cost,
                    start=start,
                    end=end,
                )

                cost_rows.append(
                    stats_row(
                        label,
                        start,
                        end,
                        trades,
                        cost,
                    )
                )

        write_csv(
            OUTPUT_COST,
            cost_rows,
        )

        # ----------------------------------------------------
        # CALENDAR YEARS
        # ----------------------------------------------------
        STATUS["message"] = (
            "Running calendar-year analysis"
        )

        year_rows = (
            calendar_year_rows(
                m15,
                signals,
            )
        )

        year_summary_rows = (
            calendar_summary(
                year_rows
            )
        )

        write_csv(
            OUTPUT_YEARS,
            year_rows,
        )

        write_csv(
            OUTPUT_YEAR_SUMMARY,
            year_summary_rows,
        )

        # ----------------------------------------------------
        # FULL TRADE EXPORT
        # ----------------------------------------------------
        STATUS["message"] = (
            "Exporting full frozen trade list"
        )

        all_trades = run_backtest(
            m15,
            signals,
            PRIMARY_COST_PIPS,
            start=actual_m15_start,
            end=RESEARCH_TO,
        )

        trade_rows = []

        for trade in all_trades:
            out = dict(
                trade
            )

            out["period"] = (
                "PRE_2010_PSEUDO_OOS"
                if trade[
                    "entry_time"
                ] < DEVELOPMENT_FROM
                else "DEVELOPMENT_2010_PLUS"
            )

            trade_rows.append(
                out
            )

        write_csv(
            OUTPUT_TRADES,
            trade_rows,
        )

        # ----------------------------------------------------
        # PACKAGE
        # ----------------------------------------------------
        STATUS.update({
            "state":
                "packaging",
            "message":
                "Building single ZIP bundle",
        })

        build_bundle()

        STATUS.update({
            "state":
                "complete",
            "message": (
                "USD/CAD M15 LONG frozen full-history "
                "validation complete"
            ),
            "actual_first_m15":
                iso_utc(
                    actual_m15_start
                ),
            "actual_last_m15":
                iso_utc(
                    actual_m15_end
                ),
            "signals_found":
                len(signals),
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
            "USDCAD M15 Long Frozen Full-History Validation",
        "status":
            STATUS["state"],
        "instrument":
            INSTRUMENT,
        "timeframe":
            "M15",
        "side":
            "BUY",
        "frozen_parameters": {
            "compression_max":
                LOCKED_COMPRESSION,
            "body_atr_min":
                LOCKED_BODY_ATR,
            "range_atr_min":
                LOCKED_RANGE_ATR,
            "breakout_lookback":
                LOCKED_BREAKOUT_LOOKBACK,
            "h4_close_above_ema":
                LOCKED_H4_EMA,
            "reward_risk":
                LOCKED_RR,
        },
        "orders_supported":
            False,
        "trading_enabled":
            False,
        "routes": [
            "/usdcad-m15-long-frozen-history/status",
            "/usdcad-m15-long-frozen-history/results",
        ],
    })


@app.route(
    "/usdcad-m15-long-frozen-history/status"
)
def route_status():
    return jsonify(
        STATUS
    )


@app.route(
    "/usdcad-m15-long-frozen-history/results"
)
def route_results():
    return download_file(
        OUTPUT_BUNDLE
    )


if __name__ == "__main__":
    research_thread = threading.Thread(
        target=run_research,
        name=(
            "usdcad-m15-long-"
            "frozen-history"
        ),
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
