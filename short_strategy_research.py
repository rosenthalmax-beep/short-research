
import os
import csv
import time
import bisect
import zipfile
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median
from zoneinfo import ZoneInfo

import requests
from flask import Flask, jsonify, send_file


# ============================================================
# LOCKED M15 STRATEGIES — PRE-2010 FROZEN VALIDATION BATCH
#
# PURPOSE
# -------
# Extend the FOUR already-locked M15 strategies back to the
# earliest reliable OANDA M15 history available.
#
# Strategies:
#   1) EUR_USD_M15_LONG
#   2) EUR_USD_M15_SHORT
#   3) GBP_USD_M15_SHORT
#   4) USD_JPY_M15_LONG
#
# IMPORTANT
# ---------
# This is a FROZEN validation script.
#
# NO PARAMETER SEARCH.
# NO OPTIMIZATION.
# NO FILTER DISCOVERY.
#
# Pre-2010 data must NOT be used to modify the frozen rules.
#
# Requested history start:
#   2002-05-06 20:00 UTC
#
# Development split:
#   PRE_2010_PSEUDO_OOS = before 2010-01-01
#   DEVELOPMENT_2010_PLUS = 2010-01-01 onward
#
# Historical M15 cost convention:
#   1.0 pip adverse fill baseline
#
# Cost stress:
#   0.5 / 1.0 / 1.5 / 2.0 pips adverse
#
# Execution conventions:
#   - OANDA midpoint candles
#   - ATR14 Wilder/RMA, SMA seeded
#   - signal time = M15 candle OPEN
#   - reference entry = signal close
#   - target geometry uses reference signal-close risk
#   - stop buffer = 10 ticks
#   - exits begin on NEXT candle
#   - pyramiding = 0
#   - exact exit-candle signal is eligible
#
# Same-bar tie:
#   LONG:
#       if high is closer to candle open => TARGET first
#       otherwise STOP first
#
#   SHORT:
#       if high is closer to candle open => STOP first
#       otherwise TARGET first
#
# ONE ZIP RESULTS ROUTE:
#   /m15-frozen-history-batch/results
#
# READ ONLY. NEVER SENDS ORDERS.
# ============================================================


app = Flask(__name__)

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_BASE = os.getenv(
    "OANDA_API_URL",
    "https://api-fxtrade.oanda.com",
)

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
    .replace(second=0, microsecond=0)
)

NY = ZoneInfo("America/New_York")

PRIMARY_COST_PIPS = 1.00
COST_GRID = [0.50, 1.00, 1.50, 2.00]


# ============================================================
# FROZEN STRATEGY DEFINITIONS
# ============================================================

STRATEGIES = {
    "EUR_USD_M15_LONG": {
        "strategy_id": "EUR_USD_M15_LONG",
        "instrument": "EUR_USD",
        "side": "BUY",
        "tick_size": 0.00001,
        "pip_size": 0.0001,

        # Exact bullish engulfing
        "signal_type": "EURUSD_LONG",

        "body_ratio_min": 1.35,
        "body_atr_min": 0.75,

        "structure_lookback": 165,
        "structure_distance_atr_max": 0.10,

        "excluded_ny_hours": {7},
        "included_ny_hours": None,
        "excluded_weekdays": {1},  # Tuesday

        "rr": 3.75,
        "stop_buffer_ticks": 10,

        # Previously locked 2010+ reference result.
        "locked_reference_trades": 85,
    },

    "EUR_USD_M15_SHORT": {
        "strategy_id": "EUR_USD_M15_SHORT",
        "instrument": "EUR_USD",
        "side": "SELL",
        "tick_size": 0.00001,
        "pip_size": 0.0001,

        # Exact bearish engulfing
        "signal_type": "EURUSD_SHORT",

        "body_ratio_min": 1.00,
        "body_atr_min": 1.00,
        "range_atr_min": 1.70,

        "structure_lookback": 60,
        "structure_distance_atr_max": 0.30,

        "excluded_ny_hours": set(),
        "included_ny_hours": {2, 3, 4},
        "excluded_weekdays": {3},  # Thursday

        "rr": 3.75,
        "stop_buffer_ticks": 10,

        "locked_reference_trades": 89,
    },

    "GBP_USD_M15_SHORT": {
        "strategy_id": "GBP_USD_M15_SHORT",
        "instrument": "GBP_USD",
        "side": "SELL",
        "tick_size": 0.00001,
        "pip_size": 0.0001,

        # Exact bearish engulfing
        "signal_type": "GBPUSD_SHORT",

        "body_atr_min": 1.00,

        "structure_lookback": 180,
        "structure_distance_atr_max": 0.05,

        "excluded_ny_hours": {6, 7},
        "included_ny_hours": None,
        "excluded_weekdays": set(),

        "rr": 3.00,
        "stop_buffer_ticks": 10,

        "locked_reference_trades": 53,
    },

    "USD_JPY_M15_LONG": {
        "strategy_id": "USD_JPY_M15_LONG",
        "instrument": "USD_JPY",
        "side": "BUY",
        "tick_size": 0.001,
        "pip_size": 0.01,

        # NOT engulfing.
        "signal_type": "USDJPY_LONG",

        "body_ratio_min": 1.00,
        "body_atr_min": 1.25,

        "sweep_lookbacks": (20, 40, 60, 100),
        "lower_wick_body_min": 0.25,
        "prior_4h_momentum_atr_max": -1.75,

        "excluded_ny_hours": set(),
        "included_ny_hours": None,
        "excluded_weekdays": set(),

        "rr": 4.00,
        "stop_buffer_ticks": 10,

        "locked_reference_trades": 75,
    },
}


INSTRUMENTS = sorted({
    config["instrument"]
    for config in STRATEGIES.values()
})


# ============================================================
# OUTPUTS
# ============================================================

OUTPUT_COVERAGE = (
    "m15_frozen_history_batch_coverage.csv"
)

OUTPUT_FULL_HISTORY = (
    "m15_frozen_history_batch_full_history.csv"
)

OUTPUT_PERIODS = (
    "m15_frozen_history_batch_periods.csv"
)

OUTPUT_COST = (
    "m15_frozen_history_batch_cost_stress.csv"
)

OUTPUT_CALENDAR_YEARS = (
    "m15_frozen_history_batch_calendar_years.csv"
)

OUTPUT_CALENDAR_SUMMARY = (
    "m15_frozen_history_batch_calendar_summary.csv"
)

OUTPUT_ROLLING = (
    "m15_frozen_history_batch_rolling.csv"
)

OUTPUT_ROLLING_SUMMARY = (
    "m15_frozen_history_batch_rolling_summary.csv"
)

OUTPUT_TRADES = (
    "m15_frozen_history_batch_all_trades.csv"
)

OUTPUT_PARITY = (
    "m15_frozen_history_batch_parity.csv"
)

OUTPUT_BUNDLE = (
    "M15_LOCKED_pre2010_FROZEN_VALIDATION_RESULTS.zip"
)

STATUS = {
    "state": "not_started",
    "message": "Frozen M15 history batch not started",
    "orders_supported": False,
    "trading_enabled": False,
}


# ============================================================
# BASIC HELPERS
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

    # Python accepts OANDA timestamps once nanoseconds are
    # reduced to microseconds.
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
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("")
        return

    fields = []
    seen = set()

    for row in rows:
        for key in row:
            if key not in seen:
                fields.append(key)
                seen.add(key)

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
        OUTPUT_FULL_HISTORY,
        OUTPUT_PERIODS,
        OUTPUT_COST,
        OUTPUT_CALENDAR_YEARS,
        OUTPUT_CALENDAR_SUMMARY,
        OUTPUT_ROLLING,
        OUTPUT_ROLLING_SUMMARY,
        OUTPUT_TRADES,
        OUTPUT_PARITY,
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


def safe_median(values):
    values = list(values)

    if not values:
        return 0.0

    return median(values)


# ============================================================
# OANDA HISTORY
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
    instrument,
    start,
    end,
):
    url = (
        f"{OANDA_BASE}/v3/instruments/"
        f"{instrument}/candles"
    )

    params = {
        "price": "M",
        "granularity": "M15",
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

    candles = []

    for item in response.json().get("candles", []):
        if not item.get("complete", False):
            continue

        mid = item["mid"]

        candles.append({
            "time": parse_oanda_time(item["time"]),
            "open": float(mid["o"]),
            "high": float(mid["h"]),
            "low": float(mid["l"]),
            "close": float(mid["c"]),
        })

    return candles


def fetch_history(
    instrument,
    start,
    end,
):
    # 35 calendar days keeps M15 request size comfortably
    # below OANDA's candle limit even during dense periods.
    chunk_days = 35

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
            "instrument": instrument,
            "message": (
                f"Fetching {instrument} M15 "
                f"chunk {chunk_number}: "
                f"{iso_utc(cursor)} -> {iso_utc(chunk_end)}"
            ),
        })

        try:
            rows = fetch_chunk(
                instrument,
                cursor,
                chunk_end,
            )
        except requests.HTTPError as error:
            status_code = (
                error.response.status_code
                if error.response is not None
                else None
            )

            # Old intervals may be unavailable.
            if status_code in (400, 404):
                rows = []
            else:
                raise

        for row in rows:
            by_time[row["time"]] = row

        cursor = chunk_end

        # Gentle API pacing.
        time.sleep(0.02)

    result = list(by_time.values())
    result.sort(key=lambda row: row["time"])

    return result


# ============================================================
# ATR14 — WILDER/RMA, SMA SEEDED
# ============================================================

def true_ranges(candles):
    result = [None] * len(candles)

    for i, candle in enumerate(candles):
        if i == 0:
            result[i] = (
                candle["high"]
                - candle["low"]
            )
            continue

        previous_close = candles[i - 1]["close"]

        result[i] = max(
            candle["high"] - candle["low"],
            abs(candle["high"] - previous_close),
            abs(candle["low"] - previous_close),
        )

    return result


def rma(values, length):
    result = [None] * len(values)

    if len(values) < length:
        return result

    seed = values[:length]

    if any(value is None for value in seed):
        return result

    result[length - 1] = (
        sum(seed) / length
    )

    for i in range(length, len(values)):
        previous = result[i - 1]
        current = values[i]

        if previous is None or current is None:
            continue

        result[i] = (
            previous * (length - 1)
            + current
        ) / length

    return result


def atr14(candles):
    return rma(
        true_ranges(candles),
        14,
    )


# ============================================================
# EXACT FROZEN SIGNAL LOGIC
# ============================================================

def exact_bullish_engulf(previous, current):
    return (
        previous["close"] < previous["open"]
        and current["close"] > current["open"]
        and current["open"] <= previous["close"]
        and current["close"] >= previous["open"]
    )


def exact_bearish_engulf(previous, current):
    return (
        previous["close"] > previous["open"]
        and current["close"] < current["open"]
        and current["open"] >= previous["close"]
        and current["close"] <= previous["open"]
    )


def passes_time_filters(config, signal_time):
    ny_time = signal_time.astimezone(NY)

    if (
        ny_time.weekday()
        in config.get("excluded_weekdays", set())
    ):
        return False

    included = config.get("included_ny_hours")

    if (
        included is not None
        and ny_time.hour not in included
    ):
        return False

    if (
        ny_time.hour
        in config.get("excluded_ny_hours", set())
    ):
        return False

    return True


def signal_eurusd_long(
    config,
    candles,
    atr_values,
    i,
):
    if i < 165:
        return False

    if not passes_time_filters(
        config,
        candles[i]["time"],
    ):
        return False

    previous = candles[i - 1]
    current = candles[i]

    if not exact_bullish_engulf(
        previous,
        current,
    ):
        return False

    previous_body = abs(
        previous["close"]
        - previous["open"]
    )

    if previous_body <= 0:
        return False

    body = (
        current["close"]
        - current["open"]
    )

    body_ratio = body / previous_body

    if body_ratio < 1.35:
        return False

    atr = atr_values[i]

    if atr is None or atr <= 0:
        return False

    if body / atr < 0.75:
        return False

    previous_low = min(
        candle["low"]
        for candle in candles[i - 165:i]
    )

    structure_distance_atr = (
        abs(
            current["low"]
            - previous_low
        )
        / atr
    )

    if structure_distance_atr > 0.10:
        return False

    return True


def signal_eurusd_short(
    config,
    candles,
    atr_values,
    i,
):
    if i < 60:
        return False

    if not passes_time_filters(
        config,
        candles[i]["time"],
    ):
        return False

    previous = candles[i - 1]
    current = candles[i]

    if not exact_bearish_engulf(
        previous,
        current,
    ):
        return False

    previous_body = abs(
        previous["close"]
        - previous["open"]
    )

    if previous_body <= 0:
        return False

    body = (
        current["open"]
        - current["close"]
    )

    body_ratio = body / previous_body

    if body_ratio < 1.00:
        return False

    atr = atr_values[i]

    if atr is None or atr <= 0:
        return False

    if body / atr < 1.00:
        return False

    signal_range = (
        current["high"]
        - current["low"]
    )

    if signal_range / atr < 1.70:
        return False

    previous_high = max(
        candle["high"]
        for candle in candles[i - 60:i]
    )

    structure_distance_atr = (
        abs(
            current["high"]
            - previous_high
        )
        / atr
    )

    if structure_distance_atr > 0.30:
        return False

    return True


def signal_gbpusd_short(
    config,
    candles,
    atr_values,
    i,
):
    if i < 180:
        return False

    if not passes_time_filters(
        config,
        candles[i]["time"],
    ):
        return False

    previous = candles[i - 1]
    current = candles[i]

    if not exact_bearish_engulf(
        previous,
        current,
    ):
        return False

    atr = atr_values[i]

    if atr is None or atr <= 0:
        return False

    body = (
        current["open"]
        - current["close"]
    )

    if body < 1.00 * atr:
        return False

    previous_high = max(
        candle["high"]
        for candle in candles[i - 180:i]
    )

    structure_distance_atr = (
        abs(
            current["high"]
            - previous_high
        )
        / atr
    )

    if structure_distance_atr > 0.05:
        return False

    return True


def signal_usdjpy_long(
    config,
    candles,
    atr_values,
    i,
):
    if i < 100:
        return False

    current = candles[i]
    previous = candles[i - 1]

    atr = atr_values[i]

    if atr is None or atr <= 0:
        return False

    # 1) Current bullish.
    if not (
        current["close"]
        > current["open"]
    ):
        return False

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

    # 2) Body ratio >= 1.00.
    if body_ratio < 1.00:
        return False

    # 3) Body >= 1.25 ATR14.
    if body / atr < 1.25:
        return False

    # 4) Current low sweeps ANY prior 20/40/60/100 low.
    swept_any = False

    for lookback in (20, 40, 60, 100):
        prior_low = min(
            candle["low"]
            for candle in candles[
                i - lookback:i
            ]
        )

        if current["low"] < prior_low:
            swept_any = True
            break

    if not swept_any:
        return False

    # 5) Close above previous M15 high.
    if not (
        current["close"]
        > previous["high"]
    ):
        return False

    # 6) Lower wick >= 0.25 * bullish body.
    lower_wick = (
        min(
            current["open"],
            current["close"],
        )
        - current["low"]
    )

    lower_wick_body_ratio = (
        lower_wick / body
        if body > 0
        else 0.0
    )

    if lower_wick_body_ratio < 0.25:
        return False

    # 7) STRICTLY PRE-SIGNAL 4h momentum <= -1.75 ATR.
    #
    # Previous completed M15 close = i-1
    # 16 completed M15 bars earlier = i-17
    #
    # Signal candle close is deliberately excluded.
    prior_4h_momentum_atr = (
        candles[i - 1]["close"]
        - candles[i - 17]["close"]
    ) / atr

    if prior_4h_momentum_atr > -1.75:
        return False

    return True


SIGNAL_FUNCTIONS = {
    "EURUSD_LONG": signal_eurusd_long,
    "EURUSD_SHORT": signal_eurusd_short,
    "GBPUSD_SHORT": signal_gbpusd_short,
    "USDJPY_LONG": signal_usdjpy_long,
}


def build_signals(
    config,
    candles,
    atr_values,
):
    signal_function = SIGNAL_FUNCTIONS[
        config["signal_type"]
    ]

    minimum_index = max(
        180,
        max(
            config.get(
                "sweep_lookbacks",
                (0,),
            )
        ),
    )

    # Starting at 180 is harmless for strategies needing less
    # warmup and keeps all calculations safely mature.
    signals = []

    for i in range(
        minimum_index,
        len(candles),
    ):
        if signal_function(
            config,
            candles,
            atr_values,
            i,
        ):
            signals.append({
                "signal_index": i,
                "time": candles[i]["time"],
            })

    return signals


# ============================================================
# TRADE GEOMETRY / OUTCOMES
# ============================================================

OUTCOME_CACHE = {}


def trade_levels(
    config,
    signal_candle,
):
    reference_entry = signal_candle["close"]

    if config["side"] == "BUY":
        stop = (
            signal_candle["low"]
            - config["stop_buffer_ticks"]
            * config["tick_size"]
        )

        reference_risk = (
            reference_entry
            - stop
        )

        if reference_risk <= 0:
            return None

        target = (
            reference_entry
            + config["rr"]
            * reference_risk
        )

    else:
        stop = (
            signal_candle["high"]
            + config["stop_buffer_ticks"]
            * config["tick_size"]
        )

        reference_risk = (
            stop
            - reference_entry
        )

        if reference_risk <= 0:
            return None

        target = (
            reference_entry
            - config["rr"]
            * reference_risk
        )

    return {
        "reference_entry": reference_entry,
        "stop": stop,
        "target": target,
        "reference_risk": reference_risk,
    }


def compute_trade_outcome(
    config,
    candles,
    signal_index,
    cost_pips,
):
    signal = candles[signal_index]

    levels = trade_levels(
        config,
        signal,
    )

    if levels is None:
        return None

    reference_entry = levels["reference_entry"]
    stop = levels["stop"]
    target = levels["target"]

    adverse_cost = (
        cost_pips
        * config["pip_size"]
    )

    if config["side"] == "BUY":
        backtest_entry = (
            reference_entry
            + adverse_cost
        )

        actual_risk = (
            backtest_entry
            - stop
        )
    else:
        backtest_entry = (
            reference_entry
            - adverse_cost
        )

        actual_risk = (
            stop
            - backtest_entry
        )

    if actual_risk <= 0:
        return None

    for j in range(
        signal_index + 1,
        len(candles),
    ):
        candle = candles[j]

        hit_stop = False
        hit_target = False

        if config["side"] == "BUY":
            hit_stop = (
                candle["low"]
                <= stop
            )

            hit_target = (
                candle["high"]
                >= target
            )

            if hit_stop and hit_target:
                high_distance = abs(
                    candle["high"]
                    - candle["open"]
                )

                low_distance = abs(
                    candle["open"]
                    - candle["low"]
                )

                # Long convention:
                # high closer => target first,
                # otherwise stop first.
                if high_distance < low_distance:
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
                - backtest_entry
            ) / actual_risk

        else:
            hit_stop = (
                candle["high"]
                >= stop
            )

            hit_target = (
                candle["low"]
                <= target
            )

            if hit_stop and hit_target:
                high_distance = abs(
                    candle["high"]
                    - candle["open"]
                )

                low_distance = abs(
                    candle["open"]
                    - candle["low"]
                )

                # Short convention:
                # high closer => stop first,
                # otherwise target first.
                if high_distance < low_distance:
                    exit_price = stop
                    exit_reason = "STOP"
                else:
                    exit_price = target
                    exit_reason = "TARGET"

            elif hit_target:
                exit_price = target
                exit_reason = "TARGET"

            elif hit_stop:
                exit_price = stop
                exit_reason = "STOP"

            else:
                continue

            result_r = (
                backtest_entry
                - exit_price
            ) / actual_risk

        return {
            "signal_index": signal_index,
            "exit_index": j,
            "entry_time": signal["time"],
            "exit_time": candle["time"],
            "entry_time_utc": iso_utc(signal["time"]),
            "exit_time_utc": iso_utc(candle["time"]),
            "reference_entry": reference_entry,
            "backtest_entry": backtest_entry,
            "stop": stop,
            "target": target,
            "reference_risk": levels["reference_risk"],
            "actual_risk": actual_risk,
            "cost_pips": cost_pips,
            "result_r": result_r,
            "exit_reason": exit_reason,
        }

    return None


def get_outcome(
    config,
    candles,
    signal_index,
    cost_pips,
):
    key = (
        config["strategy_id"],
        signal_index,
        float(cost_pips),
    )

    if key not in OUTCOME_CACHE:
        OUTCOME_CACHE[key] = (
            compute_trade_outcome(
                config,
                candles,
                signal_index,
                cost_pips,
            )
        )

    return OUTCOME_CACHE[key]


# ============================================================
# PYRAMIDING = 0
# ============================================================

def run_backtest(
    config,
    candles,
    signals,
    cost_pips,
    start=None,
    end=None,
):
    candidates = signals

    if start is not None or end is not None:
        times = [
            signal["time"]
            for signal in signals
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
            len(signals)
            if end is None
            else bisect.bisect_left(
                times,
                end,
            )
        )

        candidates = signals[left:right]

    signal_indices = [
        signal["signal_index"]
        for signal in candidates
    ]

    trades = []
    position = 0

    while position < len(candidates):
        signal = candidates[position]

        trade = get_outcome(
            config,
            candles,
            signal["signal_index"],
            cost_pips,
        )

        if trade is None:
            position += 1
            continue

        trades.append(dict(trade))

        # Exact exit-candle signal is eligible.
        position = bisect.bisect_left(
            signal_indices,
            trade["exit_index"],
            lo=position + 1,
        )

    return trades


# ============================================================
# STATS
# ============================================================

def stats_from_trades(trades):
    results = [
        float(trade["result_r"])
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

    if gross_loss > 0:
        profit_factor = (
            gross_profit
            / gross_loss
        )
    elif gross_profit > 0:
        profit_factor = 999.0
    else:
        profit_factor = 0.0

    total_r = sum(results)

    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0

    current_loss_streak = 0
    longest_loss_streak = 0

    for result in results:
        equity += result

        peak = max(
            peak,
            equity,
        )

        max_drawdown = min(
            max_drawdown,
            equity - peak,
        )

        if result < 0:
            current_loss_streak += 1
            longest_loss_streak = max(
                longest_loss_streak,
                current_loss_streak,
            )
        else:
            current_loss_streak = 0

    return {
        "trades": len(results),
        "winners": len(winners),
        "losers": len(losers),
        "win_rate": (
            len(winners)
            / len(results)
            * 100.0
            if results
            else 0.0
        ),
        "profit_factor": profit_factor,
        "total_r": total_r,
        "expectancy_r": (
            total_r / len(results)
            if results
            else 0.0
        ),
        "max_drawdown_r": max_drawdown,
        "longest_loss_streak": longest_loss_streak,
    }


def make_stats_row(
    config,
    label,
    start,
    end,
    trades,
    cost_pips,
):
    stats = stats_from_trades(trades)

    return {
        "strategy_id": config["strategy_id"],
        "instrument": config["instrument"],
        "side": config["side"],
        "period": label,
        "start_utc": (
            iso_utc(start)
            if start is not None
            else ""
        ),
        "end_utc": (
            iso_utc(end)
            if end is not None
            else ""
        ),
        "cost_pips": cost_pips,
        "trades": stats["trades"],
        "winners": stats["winners"],
        "losers": stats["losers"],
        "win_rate": round(
            stats["win_rate"],
            4,
        ),
        "profit_factor": round(
            stats["profit_factor"],
            6,
        ),
        "total_r": round(
            stats["total_r"],
            4,
        ),
        "expectancy_r": round(
            stats["expectancy_r"],
            6,
        ),
        "max_drawdown_r": round(
            stats["max_drawdown_r"],
            4,
        ),
        "longest_loss_streak": (
            stats["longest_loss_streak"]
        ),
    }


# ============================================================
# MONTH HELPERS / ROLLING WINDOWS
# ============================================================

def add_months(dt, months):
    month_index = (
        dt.year * 12
        + (dt.month - 1)
        + months
    )

    year = month_index // 12
    month = (
        month_index % 12
        + 1
    )

    return datetime(
        year,
        month,
        1,
        tzinfo=timezone.utc,
    )


def month_floor(dt):
    return datetime(
        dt.year,
        dt.month,
        1,
        tzinfo=timezone.utc,
    )


def rolling_rows_for_strategy(
    config,
    candles,
    signals,
):
    rows = []

    if not candles:
        return rows

    available_start = candles[0]["time"]
    first_month = month_floor(
        max(
            available_start,
            REQUESTED_FROM,
        )
    )

    final_month = month_floor(
        RESEARCH_TO
    )

    for months in (12, 24, 36):
        start = first_month

        while (
            add_months(start, months)
            <= final_month
        ):
            end = add_months(
                start,
                months,
            )

            trades = run_backtest(
                config,
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
                "strategy_id":
                    config["strategy_id"],
                "months":
                    months,
                "start_utc":
                    iso_utc(start),
                "end_utc":
                    iso_utc(end),
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
                "positive_window":
                    stats["total_r"] > 0,
                "zero_trade_window":
                    stats["trades"] == 0,
                "period_class":
                    (
                        "PRE_2010_ONLY"
                        if end <= DEVELOPMENT_FROM
                        else (
                            "2010_PLUS_ONLY"
                            if start >= DEVELOPMENT_FROM
                            else "CROSSES_2010"
                        )
                    ),
            })

            start = add_months(
                start,
                1,
            )

    return rows


def rolling_summary_rows(
    all_rolling_rows,
):
    output = []

    grouped = defaultdict(list)

    for row in all_rolling_rows:
        grouped[
            (
                row["strategy_id"],
                int(row["months"]),
                row["period_class"],
            )
        ].append(row)

    for (
        strategy_id,
        months,
        period_class,
    ), rows in sorted(
        grouped.items()
    ):
        active = [
            row
            for row in rows
            if int(row["trades"]) > 0
        ]

        positive = [
            row
            for row in rows
            if row["positive_window"]
        ]

        positive_active = [
            row
            for row in active
            if row["positive_window"]
        ]

        r_all = [
            float(row["total_r"])
            for row in rows
        ]

        r_active = [
            float(row["total_r"])
            for row in active
        ]

        pf_active = [
            float(row["profit_factor"])
            for row in active
        ]

        output.append({
            "strategy_id":
                strategy_id,
            "months":
                months,
            "period_class":
                period_class,
            "windows":
                len(rows),
            "active_windows":
                len(active),
            "zero_trade_windows":
                sum(
                    1
                    for row in rows
                    if row[
                        "zero_trade_window"
                    ]
                ),
            "positive_windows_pct":
                round(
                    len(positive)
                    / len(rows)
                    * 100.0,
                    4,
                )
                if rows
                else 0.0,
            "positive_active_windows_pct":
                round(
                    len(positive_active)
                    / len(active)
                    * 100.0,
                    4,
                )
                if active
                else 0.0,
            "median_r_all":
                round(
                    safe_median(r_all),
                    4,
                ),
            "median_r_active":
                round(
                    safe_median(r_active),
                    4,
                ),
            "worst_r":
                round(
                    min(r_all),
                    4,
                )
                if r_all
                else 0.0,
            "best_r":
                round(
                    max(r_all),
                    4,
                )
                if r_all
                else 0.0,
            "median_pf_active":
                round(
                    safe_median(pf_active),
                    6,
                ),
        })

    return output


# ============================================================
# CALENDAR-YEAR ANALYSIS
# ============================================================

def calendar_year_rows(
    config,
    candles,
    signals,
):
    rows = []

    if not candles:
        return rows

    first_year = max(
        REQUESTED_FROM.year,
        candles[0]["time"].year,
    )

    # Completed years only.
    last_year = RESEARCH_TO.year - 1

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
            config,
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
            "strategy_id":
                config["strategy_id"],
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
            "positive_year":
                stats["total_r"] > 0,
            "negative_year":
                stats["total_r"] < 0,
            "zero_trade_year":
                stats["trades"] == 0,
        })

    return rows


def calendar_summary_rows(
    all_year_rows,
):
    output = []

    grouped = defaultdict(list)

    for row in all_year_rows:
        grouped[
            (
                row["strategy_id"],
                row["period"],
            )
        ].append(row)

    for (
        strategy_id,
        period,
    ), rows in sorted(
        grouped.items()
    ):
        active = [
            row
            for row in rows
            if int(row["trades"]) > 0
        ]

        positive = [
            row
            for row in rows
            if row["positive_year"]
        ]

        negative = [
            row
            for row in rows
            if row["negative_year"]
        ]

        positive_active = [
            row
            for row in active
            if row["positive_year"]
        ]

        trade_counts = [
            int(row["trades"])
            for row in rows
        ]

        yearly_r = [
            float(row["total_r"])
            for row in rows
        ]

        output.append({
            "strategy_id":
                strategy_id,
            "period":
                period,
            "completed_years":
                len(rows),
            "active_years":
                len(active),
            "positive_years":
                len(positive),
            "negative_years":
                len(negative),
            "zero_trade_years":
                sum(
                    1
                    for row in rows
                    if row["zero_trade_year"]
                ),
            "positive_years_pct":
                round(
                    len(positive)
                    / len(rows)
                    * 100.0,
                    4,
                )
                if rows
                else 0.0,
            "positive_active_years_pct":
                round(
                    len(positive_active)
                    / len(active)
                    * 100.0,
                    4,
                )
                if active
                else 0.0,
            "median_trades_per_year":
                round(
                    safe_median(
                        trade_counts
                    ),
                    4,
                ),
            "median_calendar_r":
                round(
                    safe_median(
                        yearly_r
                    ),
                    4,
                ),
            "worst_calendar_r":
                round(
                    min(yearly_r),
                    4,
                )
                if yearly_r
                else 0.0,
            "best_calendar_r":
                round(
                    max(yearly_r),
                    4,
                )
                if yearly_r
                else 0.0,
        })

    return output


# ============================================================
# VALIDATION WINDOWS
# ============================================================

def validation_periods(
    actual_start,
):
    periods = [
        (
            "FULL_AVAILABLE_HISTORY",
            actual_start,
            RESEARCH_TO,
        ),
    ]

    if actual_start < DEVELOPMENT_FROM:
        periods.append(
            (
                "PRE_2010_PSEUDO_OOS",
                actual_start,
                DEVELOPMENT_FROM,
            )
        )

    periods.extend([
        (
            "DEVELOPMENT_2010_PLUS",
            max(
                DEVELOPMENT_FROM,
                actual_start,
            ),
            RESEARCH_TO,
        ),
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

    return [
        (
            label,
            start,
            end,
        )
        for (
            label,
            start,
            end,
        ) in periods
        if start < end
    ]


# ============================================================
# MAIN RUNNER
# ============================================================

def run_research():
    try:
        # ----------------------------------------------------
        # FETCH EACH INSTRUMENT ONCE
        # ----------------------------------------------------
        history = {}
        atr_cache = {}
        signals_by_strategy = {}

        coverage_rows = []

        for instrument in INSTRUMENTS:
            candles = fetch_history(
                instrument,
                REQUESTED_FROM,
                RESEARCH_TO,
            )

            if not candles:
                raise RuntimeError(
                    f"No M15 history returned for {instrument}"
                )

            history[instrument] = candles
            atr_cache[instrument] = atr14(
                candles
            )

            coverage_rows.append({
                "instrument":
                    instrument,
                "requested_start_utc":
                    iso_utc(
                        REQUESTED_FROM
                    ),
                "actual_first_m15_utc":
                    iso_utc(
                        candles[0]["time"]
                    ),
                "actual_last_m15_utc":
                    iso_utc(
                        candles[-1]["time"]
                    ),
                "m15_candles":
                    len(candles),
                "pre_2010_history_available":
                    candles[0]["time"]
                    < DEVELOPMENT_FROM,
            })

        write_csv(
            OUTPUT_COVERAGE,
            coverage_rows,
        )

        # ----------------------------------------------------
        # BUILD FROZEN SIGNALS
        # ----------------------------------------------------
        for strategy_id, config in STRATEGIES.items():
            STATUS.update({
                "state":
                    "precomputing",
                "strategy_id":
                    strategy_id,
                "message":
                    f"Computing frozen signals for {strategy_id}",
            })

            candles = history[
                config["instrument"]
            ]

            atr_values = atr_cache[
                config["instrument"]
            ]

            signals_by_strategy[
                strategy_id
            ] = build_signals(
                config,
                candles,
                atr_values,
            )

        # ----------------------------------------------------
        # FULL / PERIOD STATS
        # ----------------------------------------------------
        full_rows = []
        period_rows = []
        cost_rows = []
        all_trade_rows = []
        parity_rows = []
        all_calendar_rows = []
        all_rolling_rows = []

        for strategy_id, config in STRATEGIES.items():
            candles = history[
                config["instrument"]
            ]

            signals = signals_by_strategy[
                strategy_id
            ]

            actual_start = candles[0]["time"]

            STATUS.update({
                "state":
                    "calculating",
                "strategy_id":
                    strategy_id,
                "message":
                    f"Running frozen validation for {strategy_id}",
                "raw_signals":
                    len(signals),
            })

            # ----------------------------------------------
            # Period stats at primary 1-pip cost
            # ----------------------------------------------
            key_periods = validation_periods(
                actual_start
            )

            strategy_period_rows = []

            for (
                label,
                start,
                end,
            ) in key_periods:
                trades = run_backtest(
                    config,
                    candles,
                    signals,
                    PRIMARY_COST_PIPS,
                    start=start,
                    end=end,
                )

                row = make_stats_row(
                    config,
                    label,
                    start,
                    end,
                    trades,
                    PRIMARY_COST_PIPS,
                )

                period_rows.append(row)
                strategy_period_rows.append(row)

                if label == "FULL_AVAILABLE_HISTORY":
                    full_rows.append(row)

            # ----------------------------------------------
            # Cost stress:
            # full / pre2010 / 2010+
            # ----------------------------------------------
            stress_periods = [
                (
                    "FULL_AVAILABLE_HISTORY",
                    actual_start,
                    RESEARCH_TO,
                ),
                (
                    "DEVELOPMENT_2010_PLUS",
                    max(
                        actual_start,
                        DEVELOPMENT_FROM,
                    ),
                    RESEARCH_TO,
                ),
            ]

            if actual_start < DEVELOPMENT_FROM:
                stress_periods.insert(
                    1,
                    (
                        "PRE_2010_PSEUDO_OOS",
                        actual_start,
                        DEVELOPMENT_FROM,
                    ),
                )

            for (
                label,
                start,
                end,
            ) in stress_periods:
                for cost in COST_GRID:
                    trades = run_backtest(
                        config,
                        candles,
                        signals,
                        cost,
                        start=start,
                        end=end,
                    )

                    cost_rows.append(
                        make_stats_row(
                            config,
                            label,
                            start,
                            end,
                            trades,
                            cost,
                        )
                    )

            # ----------------------------------------------
            # Full 1-pip trade export
            # ----------------------------------------------
            full_trades = run_backtest(
                config,
                candles,
                signals,
                PRIMARY_COST_PIPS,
                start=actual_start,
                end=RESEARCH_TO,
            )

            for trade in full_trades:
                row = dict(trade)

                row[
                    "strategy_id"
                ] = strategy_id

                row[
                    "instrument"
                ] = config[
                    "instrument"
                ]

                row[
                    "side"
                ] = config[
                    "side"
                ]

                row[
                    "period"
                ] = (
                    "PRE_2010_PSEUDO_OOS"
                    if trade[
                        "entry_time"
                    ] < DEVELOPMENT_FROM
                    else "DEVELOPMENT_2010_PLUS"
                )

                all_trade_rows.append(
                    row
                )

            # ----------------------------------------------
            # Calendar years
            # ----------------------------------------------
            all_calendar_rows.extend(
                calendar_year_rows(
                    config,
                    candles,
                    signals,
                )
            )

            # ----------------------------------------------
            # Rolling 12/24/36M
            # ----------------------------------------------
            all_rolling_rows.extend(
                rolling_rows_for_strategy(
                    config,
                    candles,
                    signals,
                )
            )

            # ----------------------------------------------
            # Development-period parity reference
            # ----------------------------------------------
            dev_row = next(
                (
                    row
                    for row
                    in strategy_period_rows
                    if row["period"]
                    == "DEVELOPMENT_2010_PLUS"
                ),
                None,
            )

            actual_dev_trades = (
                int(
                    dev_row["trades"]
                )
                if dev_row
                else 0
            )

            reference = int(
                config[
                    "locked_reference_trades"
                ]
            )

            parity_rows.append({
                "strategy_id":
                    strategy_id,
                "locked_reference_trades":
                    reference,
                "current_2010_plus_trades":
                    actual_dev_trades,
                "difference":
                    actual_dev_trades
                    - reference,
                "status":
                    (
                        "MATCH"
                        if actual_dev_trades
                        == reference
                        else (
                            "CURRENT_RUN_HAS_NEWER_TRADES"
                            if actual_dev_trades
                            > reference
                            else "CHECK_PARITY"
                        )
                    ),
                "note": (
                    "The locked reference count was recorded when "
                    "the strategy was frozen. A higher current count "
                    "can be legitimate because newer 2026 candles "
                    "have since completed. A lower count should be "
                    "investigated."
                ),
            })

        # ----------------------------------------------------
        # SUMMARIES
        # ----------------------------------------------------
        calendar_summary = (
            calendar_summary_rows(
                all_calendar_rows
            )
        )

        rolling_summary = (
            rolling_summary_rows(
                all_rolling_rows
            )
        )

        # ----------------------------------------------------
        # WRITE FILES
        # ----------------------------------------------------
        STATUS.update({
            "state":
                "writing",
            "message":
                "Writing frozen batch validation outputs",
        })

        write_csv(
            OUTPUT_FULL_HISTORY,
            full_rows,
        )

        write_csv(
            OUTPUT_PERIODS,
            period_rows,
        )

        write_csv(
            OUTPUT_COST,
            cost_rows,
        )

        write_csv(
            OUTPUT_CALENDAR_YEARS,
            all_calendar_rows,
        )

        write_csv(
            OUTPUT_CALENDAR_SUMMARY,
            calendar_summary,
        )

        write_csv(
            OUTPUT_ROLLING,
            all_rolling_rows,
        )

        write_csv(
            OUTPUT_ROLLING_SUMMARY,
            rolling_summary,
        )

        write_csv(
            OUTPUT_TRADES,
            all_trade_rows,
        )

        write_csv(
            OUTPUT_PARITY,
            parity_rows,
        )

        # ----------------------------------------------------
        # ZIP
        # ----------------------------------------------------
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
            "message": (
                "Four-strategy frozen M15 "
                "pre-2010 validation complete"
            ),
            "strategies": list(
                STRATEGIES.keys()
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
            "Locked M15 Pre-2010 Frozen Validation Batch",
        "status":
            STATUS["state"],
        "strategies": list(
            STRATEGIES.keys()
        ),
        "requested_start_utc":
            iso_utc(
                REQUESTED_FROM
            ),
        "development_split_utc":
            iso_utc(
                DEVELOPMENT_FROM
            ),
        "primary_cost_pips":
            PRIMARY_COST_PIPS,
        "cost_stress_pips":
            COST_GRID,
        "orders_supported":
            False,
        "trading_enabled":
            False,
        "routes": [
            "/m15-frozen-history-batch/status",
            "/m15-frozen-history-batch/results",
        ],
    })


@app.route(
    "/m15-frozen-history-batch/status"
)
def route_status():
    return jsonify(STATUS)


@app.route(
    "/m15-frozen-history-batch/results"
)
def route_results():
    return download_file(
        OUTPUT_BUNDLE
    )


if __name__ == "__main__":
    thread = threading.Thread(
        target=run_research,
        name="m15-frozen-history-batch",
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
