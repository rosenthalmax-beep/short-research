
import os
import csv
import time
import bisect
import zipfile
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median

import requests
from flask import Flask, jsonify, send_file


# ============================================================
# USD/CAD M15 LONG — FROZEN REGIME DIAGNOSTIC
#
# PURPOSE
# -------
# Diagnose WHY the already-frozen USD/CAD M15 LONG strategy
# performs poorly pre-2010 but well from 2010 onward.
#
# THIS SCRIPT DOES NOT OPTIMIZE OR CHANGE PARAMETERS.
#
# Frozen rules remain:
#   compression <= 0.72
#   body >= 1.30 ATR14
#   range >= 1.60 ATR14
#   close > prior 10-bar high
#   previous COMPLETED H4 close > EMA100
#   RR = 4.00
#   stop = signal low - 10 ticks
#   1.0 pip adverse baseline
#
# Diagnostic comparisons:
#   - PRE_2010 vs 2010_PLUS
#   - 2010_2013 / 2014_2017 / 2018_2021 / 2022_NOW
#   - signal compression distribution
#   - body ATR / range ATR
#   - ATR level
#   - stop/risk size in pips and ATR
#   - H4 close distance above EMA100
#   - H4 ATR regime
#   - signal hour UTC and America/New_York
#   - weekday
#   - trade duration
#   - winner vs loser feature profiles
#   - per-year concentration
#
# NO NEW FILTERS ARE CREATED.
# NO "BEST" SUBSET IS SELECTED.
#
# OUTPUT
# ------
# One ZIP route:
#   /usdcad-m15-long-regime-diagnostic/results
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

H4_FETCH_FROM = (
    REQUESTED_FROM
    - timedelta(days=365)
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

OUTPUT_COVERAGE = (
    "usdcad_m15_long_regime_diagnostic_coverage.csv"
)

OUTPUT_TRADES = (
    "usdcad_m15_long_regime_diagnostic_trades.csv"
)

OUTPUT_PERIOD_SUMMARY = (
    "usdcad_m15_long_regime_diagnostic_period_summary.csv"
)

OUTPUT_FEATURE_SUMMARY = (
    "usdcad_m15_long_regime_diagnostic_feature_summary.csv"
)

OUTPUT_WINLOSS_SUMMARY = (
    "usdcad_m15_long_regime_diagnostic_winloss_summary.csv"
)

OUTPUT_HOUR = (
    "usdcad_m15_long_regime_diagnostic_hour.csv"
)

OUTPUT_WEEKDAY = (
    "usdcad_m15_long_regime_diagnostic_weekday.csv"
)

OUTPUT_YEARS = (
    "usdcad_m15_long_regime_diagnostic_years.csv"
)

OUTPUT_H4 = (
    "usdcad_m15_long_regime_diagnostic_h4_context.csv"
)

OUTPUT_BUNDLE = (
    "usdcad_m15_long_FROZEN_regime_diagnostic_RESULTS.zip"
)

STATUS = {
    "state": "not_started",
    "message": (
        "USD/CAD M15 LONG frozen regime diagnostic not started"
    ),
    "service": (
        "USDCAD M15 Long Frozen Regime Diagnostic"
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
        OUTPUT_TRADES,
        OUTPUT_PERIOD_SUMMARY,
        OUTPUT_FEATURE_SUMMARY,
        OUTPUT_WINLOSS_SUMMARY,
        OUTPUT_HOUR,
        OUTPUT_WEEKDAY,
        OUTPUT_YEARS,
        OUTPUT_H4,
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


def mean(values):
    values = list(values)

    if not values:
        return 0.0

    return sum(values) / len(values)


def percentile(values, p):
    values = sorted(values)

    if not values:
        return 0.0

    if len(values) == 1:
        return values[0]

    position = (
        (len(values) - 1)
        * p
    )

    lower = int(position)
    upper = min(
        lower + 1,
        len(values) - 1,
    )

    weight = (
        position
        - lower
    )

    return (
        values[lower]
        * (1.0 - weight)
        + values[upper]
        * weight
    )


def period_label(dt):
    if dt < DEVELOPMENT_FROM:
        return "PRE_2010"

    if dt < datetime(
        2014, 1, 1,
        tzinfo=timezone.utc,
    ):
        return "2010_2013"

    if dt < datetime(
        2018, 1, 1,
        tzinfo=timezone.utc,
    ):
        return "2014_2017"

    if dt < datetime(
        2022, 1, 1,
        tzinfo=timezone.utc,
    ):
        return "2018_2021"

    return "2022_NOW"


def development_label(dt):
    return (
        "PRE_2010"
        if dt < DEVELOPMENT_FROM
        else "2010_PLUS"
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

    h4_atr = atr14(
        h4
    )

    h4_atr_mean50 = sma(
        h4_atr,
        50,
    )

    rows = []

    for i, candle in enumerate(
        h4
    ):
        complete_at = (
            h4[i + 1]["time"]
            if i + 1 < len(h4)
            else None
        )

        atr_ratio = None

        if (
            h4_atr[i] is not None
            and h4_atr_mean50[i]
            is not None
            and h4_atr_mean50[i]
            > 0
        ):
            atr_ratio = (
                h4_atr[i]
                / h4_atr_mean50[i]
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
            "atr14":
                h4_atr[i],
            "atr_ratio50":
                atr_ratio,
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
# FROZEN SIGNALS
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

        compression = (
            prior_atr
            / prior_atr_mean20
        )

        if (
            compression
            > LOCKED_COMPRESSION
        ):
            continue

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

        h4_atr = (
            h4_prev["atr14"]
        )

        h4_distance_atr = None

        if (
            h4_atr is not None
            and h4_atr > 0
        ):
            h4_distance_atr = (
                h4_prev["close"]
                - h4_ema100
            ) / h4_atr

        reference_entry = (
            current["close"]
        )

        stop = (
            current["low"]
            - STOP_BUFFER_TICKS
            * TICK_SIZE
        )

        reference_risk = (
            reference_entry
            - stop
        )

        signals.append({
            "signal_index":
                i,
            "time":
                current["time"],
            "period":
                period_label(
                    current["time"]
                ),
            "development_period":
                development_label(
                    current["time"]
                ),
            "year":
                current["time"].year,
            "utc_hour":
                current["time"].hour,
            "weekday":
                current["time"].weekday(),

            "compression_ratio":
                compression,
            "signal_atr_pips":
                atr / PIP_SIZE,
            "body_atr":
                body_atr,
            "range_atr":
                range_atr,
            "reference_risk_pips":
                reference_risk
                / PIP_SIZE,
            "reference_risk_atr":
                reference_risk
                / atr,

            "h4_close":
                h4_prev["close"],
            "h4_ema100":
                h4_ema100,
            "h4_distance_atr":
                h4_distance_atr,
            "h4_atr_ratio50":
                h4_prev[
                    "atr_ratio50"
                ],
        })

    return signals


# ============================================================
# TRADE OUTCOMES
# ============================================================

OUTCOME_CACHE = {}


def compute_trade_outcome(
    candles,
    signal_index,
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
        + PRIMARY_COST_PIPS
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

        duration_bars = (
            j
            - signal_index
        )

        duration_hours = (
            duration_bars
            * 0.25
        )

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
            "duration_bars":
                duration_bars,
            "duration_hours":
                duration_hours,
        }

    return None


def get_outcome(
    candles,
    signal_index,
):
    if signal_index not in OUTCOME_CACHE:
        OUTCOME_CACHE[
            signal_index
        ] = compute_trade_outcome(
            candles,
            signal_index,
        )

    return OUTCOME_CACHE[
        signal_index
    ]


def run_backtest(
    candles,
    signals,
):
    indices = [
        signal[
            "signal_index"
        ]
        for signal in signals
    ]

    trades = []
    position = 0

    while (
        position
        < len(signals)
    ):
        signal = (
            signals[
                position
            ]
        )

        trade = get_outcome(
            candles,
            signal[
                "signal_index"
            ],
        )

        if trade is None:
            position += 1
            continue

        merged = dict(
            signal
        )

        merged.update(
            trade
        )

        trades.append(
            merged
        )

        position = bisect.bisect_left(
            indices,
            trade[
                "exit_index"
            ],
            lo=position + 1,
        )

    return trades


# ============================================================
# SUMMARY FUNCTIONS
# ============================================================

FEATURES = [
    "compression_ratio",
    "signal_atr_pips",
    "body_atr",
    "range_atr",
    "reference_risk_pips",
    "reference_risk_atr",
    "h4_distance_atr",
    "h4_atr_ratio50",
    "duration_hours",
]


def subset_stats(
    rows,
):
    results = [
        float(
            row["result_r"]
        )
        for row in rows
    ]

    winners = [
        result
        for result in results
        if result > 0
    ]

    losers = [
        result
        for result in results
        if result < 0
    ]

    gross_profit = sum(
        winners
    )

    gross_loss = abs(
        sum(losers)
    )

    pf = (
        gross_profit
        / gross_loss
        if gross_loss > 0
        else (
            999.0
            if gross_profit > 0
            else 0.0
        )
    )

    return {
        "trades":
            len(rows),
        "winners":
            len(winners),
        "losers":
            len(losers),
        "win_rate":
            (
                len(winners)
                / len(rows)
                * 100.0
                if rows
                else 0.0
            ),
        "profit_factor":
            pf,
        "total_r":
            sum(results),
        "expectancy_r":
            (
                mean(results)
                if results
                else 0.0
            ),
    }


def feature_summary_rows(
    trades,
):
    rows = []

    group_defs = [
        (
            "PRE_2010",
            lambda row:
                row["development_period"]
                == "PRE_2010",
        ),
        (
            "2010_PLUS",
            lambda row:
                row["development_period"]
                == "2010_PLUS",
        ),
        (
            "2010_2013",
            lambda row:
                row["period"]
                == "2010_2013",
        ),
        (
            "2014_2017",
            lambda row:
                row["period"]
                == "2014_2017",
        ),
        (
            "2018_2021",
            lambda row:
                row["period"]
                == "2018_2021",
        ),
        (
            "2022_NOW",
            lambda row:
                row["period"]
                == "2022_NOW",
        ),
    ]

    for label, selector in group_defs:
        subset = [
            row
            for row in trades
            if selector(row)
        ]

        for feature in FEATURES:
            values = [
                float(
                    row[feature]
                )
                for row in subset
                if row.get(
                    feature
                ) is not None
            ]

            if not values:
                continue

            rows.append({
                "group":
                    label,
                "feature":
                    feature,
                "n":
                    len(values),
                "mean":
                    round(
                        mean(values),
                        6,
                    ),
                "median":
                    round(
                        median(values),
                        6,
                    ),
                "p10":
                    round(
                        percentile(
                            values,
                            0.10,
                        ),
                        6,
                    ),
                "p25":
                    round(
                        percentile(
                            values,
                            0.25,
                        ),
                        6,
                    ),
                "p75":
                    round(
                        percentile(
                            values,
                            0.75,
                        ),
                        6,
                    ),
                "p90":
                    round(
                        percentile(
                            values,
                            0.90,
                        ),
                        6,
                    ),
                "min":
                    round(
                        min(values),
                        6,
                    ),
                "max":
                    round(
                        max(values),
                        6,
                    ),
            })

    return rows


def winloss_feature_rows(
    trades,
):
    rows = []

    groups = [
        (
            "PRE_2010_WIN",
            lambda row:
                row["development_period"]
                == "PRE_2010"
                and row["result_r"] > 0,
        ),
        (
            "PRE_2010_LOSS",
            lambda row:
                row["development_period"]
                == "PRE_2010"
                and row["result_r"] < 0,
        ),
        (
            "2010_PLUS_WIN",
            lambda row:
                row["development_period"]
                == "2010_PLUS"
                and row["result_r"] > 0,
        ),
        (
            "2010_PLUS_LOSS",
            lambda row:
                row["development_period"]
                == "2010_PLUS"
                and row["result_r"] < 0,
        ),
    ]

    for label, selector in groups:
        subset = [
            row
            for row in trades
            if selector(row)
        ]

        for feature in FEATURES:
            values = [
                float(
                    row[feature]
                )
                for row in subset
                if row.get(
                    feature
                ) is not None
            ]

            if not values:
                continue

            rows.append({
                "group":
                    label,
                "feature":
                    feature,
                "n":
                    len(values),
                "mean":
                    round(
                        mean(values),
                        6,
                    ),
                "median":
                    round(
                        median(values),
                        6,
                    ),
            })

    return rows


def bucket_summary(
    trades,
    key,
    bucket_name,
):
    grouped = defaultdict(
        list
    )

    for row in trades:
        grouped[
            (
                row[
                    "development_period"
                ],
                row[key],
            )
        ].append(
            row
        )

    output = []

    for (
        dev_period,
        bucket,
    ), subset in sorted(
        grouped.items(),
        key=lambda item:
            (
                item[0][0],
                item[0][1],
            )
    ):
        stats = subset_stats(
            subset
        )

        output.append({
            "development_period":
                dev_period,
            bucket_name:
                bucket,
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
            "expectancy_r":
                round(
                    stats[
                        "expectancy_r"
                    ],
                    6,
                ),
        })

    return output


def period_summary_rows(
    trades,
):
    rows = []

    groups = [
        (
            "PRE_2010",
            lambda row:
                row["development_period"]
                == "PRE_2010",
        ),
        (
            "2010_PLUS",
            lambda row:
                row["development_period"]
                == "2010_PLUS",
        ),
        (
            "2010_2013",
            lambda row:
                row["period"]
                == "2010_2013",
        ),
        (
            "2014_2017",
            lambda row:
                row["period"]
                == "2014_2017",
        ),
        (
            "2018_2021",
            lambda row:
                row["period"]
                == "2018_2021",
        ),
        (
            "2022_NOW",
            lambda row:
                row["period"]
                == "2022_NOW",
        ),
    ]

    for label, selector in groups:
        subset = [
            row
            for row in trades
            if selector(row)
        ]

        stats = subset_stats(
            subset
        )

        rows.append({
            "period":
                label,
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
            "expectancy_r":
                round(
                    stats[
                        "expectancy_r"
                    ],
                    6,
                ),
        })

    return rows


def yearly_rows(
    trades,
):
    grouped = defaultdict(
        list
    )

    for row in trades:
        grouped[
            row["year"]
        ].append(
            row
        )

    rows = []

    for year in sorted(
        grouped
    ):
        subset = grouped[
            year
        ]

        stats = subset_stats(
            subset
        )

        rows.append({
            "year":
                year,
            "period":
                (
                    "PRE_2010"
                    if year < 2010
                    else "2010_PLUS"
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
            "mean_compression":
                round(
                    mean([
                        row[
                            "compression_ratio"
                        ]
                        for row
                        in subset
                    ]),
                    6,
                ),
            "mean_signal_atr_pips":
                round(
                    mean([
                        row[
                            "signal_atr_pips"
                        ]
                        for row
                        in subset
                    ]),
                    6,
                ),
            "mean_h4_distance_atr":
                round(
                    mean([
                        row[
                            "h4_distance_atr"
                        ]
                        for row
                        in subset
                        if row[
                            "h4_distance_atr"
                        ] is not None
                    ]),
                    6,
                ),
        })

    return rows


def h4_context_rows(
    trades,
):
    """
    Descriptive H4 context buckets only.
    These are diagnostics, NOT candidate filters.
    """
    rows = []

    def distance_bucket(value):
        if value is None:
            return "NA"

        if value < 0.25:
            return "0.00_0.25"

        if value < 0.50:
            return "0.25_0.50"

        if value < 1.00:
            return "0.50_1.00"

        return "1.00_PLUS"

    def vol_bucket(value):
        if value is None:
            return "NA"

        if value < 0.80:
            return "LT_0.80"

        if value < 1.00:
            return "0.80_1.00"

        if value < 1.20:
            return "1.00_1.20"

        return "1.20_PLUS"

    for row in trades:
        row = dict(
            row
        )

        row[
            "h4_distance_bucket"
        ] = distance_bucket(
            row[
                "h4_distance_atr"
            ]
        )

        row[
            "h4_vol_bucket"
        ] = vol_bucket(
            row[
                "h4_atr_ratio50"
            ]
        )

        rows.append(
            row
        )

    out = []

    for key, name in [
        (
            "h4_distance_bucket",
            "H4_DISTANCE_ATR",
        ),
        (
            "h4_vol_bucket",
            "H4_ATR_RATIO50",
        ),
    ]:
        grouped = defaultdict(
            list
        )

        for row in rows:
            grouped[
                (
                    row[
                        "development_period"
                    ],
                    row[key],
                )
            ].append(
                row
            )

        for (
            period,
            bucket,
        ), subset in sorted(
            grouped.items(),
            key=lambda item:
                (
                    item[0][0],
                    item[0][1],
                )
        ):
            stats = subset_stats(
                subset
            )

            out.append({
                "metric":
                    name,
                "development_period":
                    period,
                "bucket":
                    bucket,
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
            })

    return out


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
                        m15[0][
                            "time"
                        ]
                    ),
                "actual_last_m15_utc":
                    iso_utc(
                        m15[-1][
                            "time"
                        ]
                    ),
                "m15_candles":
                    len(m15),
                "h4_candles":
                    len(h4),
            }],
        )

        # ----------------------------------------------------
        # FEATURES / SIGNALS / TRADES
        # ----------------------------------------------------
        STATUS.update({
            "state":
                "precomputing",
            "message":
                "Computing frozen signals and diagnostic features",
            "m15_candles":
                len(m15),
            "h4_candles":
                len(h4),
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

        STATUS.update({
            "state":
                "calculating",
            "message":
                "Running frozen pyramiding-0 trade stream",
            "raw_signals":
                len(signals),
        })

        trades = run_backtest(
            m15,
            signals,
        )

        # ----------------------------------------------------
        # Add New York hour for diagnostics only.
        # Uses zoneinfo lazily here to avoid changing signal logic.
        # ----------------------------------------------------
        from zoneinfo import ZoneInfo

        ny = ZoneInfo(
            "America/New_York"
        )

        trade_rows = []

        for row in trades:
            out = dict(
                row
            )

            local = (
                row["entry_time"]
                .astimezone(ny)
            )

            out[
                "ny_hour"
            ] = local.hour

            out[
                "ny_weekday"
            ] = local.weekday()

            trade_rows.append(
                out
            )

        write_csv(
            OUTPUT_TRADES,
            trade_rows,
        )

        # ----------------------------------------------------
        # PERIOD / FEATURE / WIN-LOSS
        # ----------------------------------------------------
        STATUS["message"] = (
            "Building period and feature diagnostics"
        )

        write_csv(
            OUTPUT_PERIOD_SUMMARY,
            period_summary_rows(
                trade_rows
            ),
        )

        write_csv(
            OUTPUT_FEATURE_SUMMARY,
            feature_summary_rows(
                trade_rows
            ),
        )

        write_csv(
            OUTPUT_WINLOSS_SUMMARY,
            winloss_feature_rows(
                trade_rows
            ),
        )

        # ----------------------------------------------------
        # TIME DIAGNOSTICS
        # ----------------------------------------------------
        hour_rows = []

        hour_rows.extend(
            bucket_summary(
                trade_rows,
                "utc_hour",
                "utc_hour",
            )
        )

        ny_hour_trades = []

        for row in trade_rows:
            row2 = dict(
                row
            )

            row2[
                "hour_bucket"
            ] = row[
                "ny_hour"
            ]

            ny_hour_trades.append(
                row2
            )

        ny_rows = bucket_summary(
            ny_hour_trades,
            "hour_bucket",
            "ny_hour",
        )

        for row in ny_rows:
            row[
                "development_period"
            ] = (
                row[
                    "development_period"
                ]
            )

        # Tag clock source to keep both tables in one CSV.
        for row in hour_rows:
            row[
                "clock"
            ] = "UTC"

        for row in ny_rows:
            row[
                "clock"
            ] = "AMERICA_NEW_YORK"

        write_csv(
            OUTPUT_HOUR,
            hour_rows
            + ny_rows,
        )

        weekday_rows = (
            bucket_summary(
                trade_rows,
                "weekday",
                "weekday",
            )
        )

        write_csv(
            OUTPUT_WEEKDAY,
            weekday_rows,
        )

        # ----------------------------------------------------
        # YEAR / H4 CONTEXT
        # ----------------------------------------------------
        STATUS["message"] = (
            "Building yearly and H4-context diagnostics"
        )

        write_csv(
            OUTPUT_YEARS,
            yearly_rows(
                trade_rows
            ),
        )

        write_csv(
            OUTPUT_H4,
            h4_context_rows(
                trade_rows
            ),
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
                "USD/CAD M15 LONG frozen regime diagnostic complete"
            ),
            "raw_signals":
                len(signals),
            "pyramiding0_trades":
                len(trades),
            "pre2010_trades":
                sum(
                    1
                    for row in trades
                    if row[
                        "development_period"
                    ] == "PRE_2010"
                ),
            "post2010_trades":
                sum(
                    1
                    for row in trades
                    if row[
                        "development_period"
                    ] == "2010_PLUS"
                ),
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
            "USDCAD M15 Long Frozen Regime Diagnostic",
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
            "/usdcad-m15-long-regime-diagnostic/status",
            "/usdcad-m15-long-regime-diagnostic/results",
        ],
    })


@app.route(
    "/usdcad-m15-long-regime-diagnostic/status"
)
def route_status():
    return jsonify(
        STATUS
    )


@app.route(
    "/usdcad-m15-long-regime-diagnostic/results"
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
            "regime-diagnostic"
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
