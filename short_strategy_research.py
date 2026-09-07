
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
# USD/JPY M15 LONG — FINAL LOCAL CONFIRMATION
#
# Base trigger: ORIGINAL corrected SWEEP_DISPLACEMENT seed
#   - current M15 candle bullish
#   - body ratio >= 1.00 vs previous candle body
#   - body >= candidate ATR threshold
#   - sweep of ANY prior 20/40/60/100-bar low
#   - close > previous M15 high
#   - no exact-engulfing requirement
#
# Final local confirmation around strongest Gen2 interaction:
#   lower wick/body >= ~0.30
#   prior 4h momentum <= ~-2.0 ATR
#
# Local grid:
#   - lower wick/body:
#       0.20 / 0.25 / 0.30 / 0.35 / 0.40
#   - prior 4h momentum max:
#       -1.50 / -1.75 / -2.00 / -2.25 / -2.50 ATR
#   - body ATR:
#       1.15 / 1.20 / 1.25 / 1.30 / 1.35
#   - RR:
#       3.25 / 3.50 / 3.75
#
# Also include:
#   - exact Gen2 winner reference
#   - body-only neighbourhood references
#   - ablation / overlap vs original seed
#   - cost stress 0.5 / 1 / 1.5 / 2 pip
#   - 4 eras
#   - DEV 2010-17 / VALIDATION 2018-now
#   - recent 5Y / 2Y
#   - rolling 2Y / 3Y
#
# Anti-overfit intent:
#   - Prefer a broad plateau, not isolated PF max
#   - Do not add further time/day filters here
#   - Do not change sweep lookbacks here
#
# Correctness:
#   - OANDA midpoint
#   - M15 signal timestamp = candle OPEN
#   - ATR14 Wilder/RMA, SMA seeded
#   - prior 4h momentum uses PREVIOUS completed M15 close
#   - stop = signal low - 10 ticks
#   - target based on REFERENCE signal close risk
#   - adverse long entry = signal close + cost
#   - exits begin NEXT candle
#   - exact exit-candle signal eligible
#   - same-bar LONG tie:
#       high closer to open => TARGET first
#       else STOP first
#   - pyramiding 0
#
# USDJPY:
#   tick = 0.001
#   pip  = 0.01
#
# M15 development cost:
#   1.0 pip adverse
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
    2010, 1, 1, tzinfo=timezone.utc
)

RESEARCH_TO = (
    datetime.now(timezone.utc)
    .replace(minute=0, second=0, microsecond=0)
)

NY = ZoneInfo("America/New_York")

TICK_SIZE = 0.001
PIP_SIZE = 0.01
STOP_BUFFER_TICKS = 10

PRIMARY_COST_PIPS = 1.00
COST_PIPS_GRID = [0.50, 1.00, 1.50, 2.00]

LOWER_WICK_VALUES = [
    0.20,
    0.25,
    0.30,
    0.35,
    0.40,
]

MOM4H_MAX_VALUES = [
    -1.50,
    -1.75,
    -2.00,
    -2.25,
    -2.50,
]

BODY_ATR_VALUES = [
    1.15,
    1.20,
    1.25,
    1.30,
    1.35,
]

RR_VALUES = [
    3.25,
    3.50,
    3.75,
]

TRIGGER_SWEEP_LOOKBACKS = (
    20,
    40,
    60,
    100,
)

OUTPUT_SUMMARY = (
    "usdjpy_m15_long_final_local_summary.csv"
)

OUTPUT_TOP = (
    "usdjpy_m15_long_final_local_top.csv"
)

OUTPUT_ERAS = (
    "usdjpy_m15_long_final_local_eras.csv"
)

OUTPUT_DEVVAL = (
    "usdjpy_m15_long_final_local_dev_validation.csv"
)

OUTPUT_RECENT = (
    "usdjpy_m15_long_final_local_recent.csv"
)

OUTPUT_ROLLING = (
    "usdjpy_m15_long_final_local_rolling.csv"
)

OUTPUT_ROLLING_SUMMARY = (
    "usdjpy_m15_long_final_local_rolling_summary.csv"
)

OUTPUT_OVERLAP = (
    "usdjpy_m15_long_final_local_overlap.csv"
)

OUTPUT_BEST_TRADES = (
    "usdjpy_m15_long_final_local_best_trades.csv"
)

OUTPUT_BUNDLE = (
    "usdjpy_m15_long_final_local_RESULTS.zip"
)

STATUS = {
    "state": "not_started",
    "message": "USDJPY M15 Long final local not started",
    "service": "USDJPY M15 Long Final Local Confirmation",
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

    return datetime.fromisoformat(value).astimezone(
        timezone.utc
    )


def years_ago_safe(dt, years):
    try:
        return dt.replace(year=dt.year - years)
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
    total = dt.year * 12 + dt.month - 1 + months

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
        elif isinstance(value, tuple):
            result[key] = tuple(value)
        else:
            result[key] = value

    result["label"] = label
    return result


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
        OUTPUT_SUMMARY,
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


def fetch_chunk(start, end):
    url = (
        f"{OANDA_BASE}/v3/instruments/"
        f"{INSTRUMENT}/candles"
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

    rows = []

    for item in response.json().get("candles", []):
        if not item.get("complete", False):
            continue

        mid = item["mid"]

        rows.append({
            "time": parse_oanda_time(item["time"]),
            "open": float(mid["o"]),
            "high": float(mid["h"]),
            "low": float(mid["l"]),
            "close": float(mid["c"]),
        })

    return rows


def fetch_history(start, end):
    cursor = start
    by_time = {}
    chunk_number = 0

    while cursor < end:
        chunk_number += 1

        chunk_end = min(
            cursor + timedelta(days=30),
            end,
        )

        STATUS.update({
            "state": "fetching",
            "message": (
                f"Fetching M15 chunk {chunk_number}: "
                f"{iso_utc(cursor)} -> "
                f"{iso_utc(chunk_end)}"
            ),
        })

        for row in fetch_chunk(
            cursor,
            chunk_end,
        ):
            by_time[row["time"]] = row

        cursor = chunk_end
        time.sleep(0.02)

    rows = list(by_time.values())
    rows.sort(key=lambda row: row["time"])

    return rows


# ============================================================
# INDICATORS
# ============================================================

def true_ranges(candles):
    result = [None] * len(candles)

    for i, candle in enumerate(candles):
        if i == 0:
            result[i] = (
                candle["high"] - candle["low"]
            )
        else:
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

    result[length - 1] = sum(seed) / length

    for i in range(length, len(values)):
        if (
            values[i] is None
            or result[i - 1] is None
        ):
            continue

        result[i] = (
            result[i - 1] * (length - 1)
            + values[i]
        ) / length

    return result


def atr14(candles):
    return rma(
        true_ranges(candles),
        14,
    )


# ============================================================
# SIGNAL FEATURES
# ============================================================

def build_signal_cache(
    candles,
    atr,
):
    signals = []

    max_lookback = max(
        max(TRIGGER_SWEEP_LOOKBACKS),
        17,
    )

    for i in range(
        max(14, max_lookback),
        len(candles),
    ):
        current = candles[i]
        previous = candles[i - 1]

        # Original trigger starts from bullish candle only.
        if not (
            current["close"] > current["open"]
        ):
            continue

        value_atr = atr[i]

        if (
            value_atr is None
            or value_atr <= 0
        ):
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

        lower_wick = (
            min(
                current["open"],
                current["close"],
            )
            - current["low"]
        )

        lower_wick_body = (
            lower_wick / body
            if body > 0
            else 0.0
        )

        close_above_prev_high = (
            current["close"]
            >
            previous["high"]
        )

        # Strictly pre-signal 4h momentum:
        # previous M15 close vs close 16 completed M15 bars earlier.
        prior_close = candles[i - 1]["close"]

        momentum_4h_atr = (
            prior_close
            -
            candles[i - 17]["close"]
        ) / value_atr

        sweep_info = {}

        for lookback in TRIGGER_SWEEP_LOOKBACKS:
            prior_low = min(
                candle["low"]
                for candle in candles[
                    i - lookback:i
                ]
            )

            swept = (
                current["low"] < prior_low
            )

            sweep_info[lookback] = {
                "prior_low": prior_low,
                "swept": swept,
                "sweep_depth_atr": (
                    (
                        prior_low - current["low"]
                    ) / value_atr
                    if swept
                    else 0.0
                ),
            }

        swept_any = any(
            sweep_info[lookback]["swept"]
            for lookback
            in TRIGGER_SWEEP_LOOKBACKS
        )

        signals.append({
            "signal_index": i,
            "time": current["time"],
            "body_ratio": body_ratio,
            "body_atr": body / value_atr,
            "lower_wick_body": lower_wick_body,
            "close_above_prev_high":
                close_above_prev_high,
            "momentum_4h_atr":
                momentum_4h_atr,
            "sweep_info":
                sweep_info,
            "swept_any":
                swept_any,
        })

    return signals


# ============================================================
# CONFIGS
# ============================================================

ORIGINAL_SEED = {
    "label":
        "REFERENCE_ORIGINAL_SEED",
    "minimum_body_ratio":
        1.00,
    "minimum_body_atr":
        1.25,
    "minimum_lower_wick_body":
        None,
    "maximum_momentum_4h_atr":
        None,
    "require_close_above_prev_high":
        True,
    "require_any_sweep":
        True,
    "sweep_lookbacks":
        tuple(TRIGGER_SWEEP_LOOKBACKS),
    "reward_risk":
        3.50,
}

GEN2_WINNER_REFERENCE = {
    "label":
        "REFERENCE_GEN2_LW0.30_MOM4H_NEG2.00",
    "minimum_body_ratio":
        1.00,
    "minimum_body_atr":
        1.25,
    "minimum_lower_wick_body":
        0.30,
    "maximum_momentum_4h_atr":
        -2.00,
    "require_close_above_prev_high":
        True,
    "require_any_sweep":
        True,
    "sweep_lookbacks":
        tuple(TRIGGER_SWEEP_LOOKBACKS),
    "reward_risk":
        3.50,
}


def build_configs():
    configs = []
    counter = 0

    for lower_wick in LOWER_WICK_VALUES:
        for momentum_max in MOM4H_MAX_VALUES:
            for body_atr in BODY_ATR_VALUES:
                for rr in RR_VALUES:
                    counter += 1

                    config = {
                        "label": (
                            f"C{counter:04d}_"
                            f"LW{lower_wick:.2f}_"
                            f"M4MAX{momentum_max:.2f}_"
                            f"BA{body_atr:.2f}_"
                            f"RR{rr:.2f}"
                        ),
                        "minimum_body_ratio":
                            1.00,
                        "minimum_body_atr":
                            body_atr,
                        "minimum_lower_wick_body":
                            lower_wick,
                        "maximum_momentum_4h_atr":
                            momentum_max,
                        "require_close_above_prev_high":
                            True,
                        "require_any_sweep":
                            True,
                        "sweep_lookbacks":
                            tuple(
                                TRIGGER_SWEEP_LOOKBACKS
                            ),
                        "reward_risk":
                            rr,
                    }

                    configs.append(config)

    return configs


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
        signal["body_atr"]
        <
        config["minimum_body_atr"]
    ):
        return False

    if (
        config[
            "minimum_lower_wick_body"
        ] is not None
        and
        signal["lower_wick_body"]
        <
        config[
            "minimum_lower_wick_body"
        ]
    ):
        return False

    if (
        config[
            "maximum_momentum_4h_atr"
        ] is not None
        and
        signal["momentum_4h_atr"]
        >
        config[
            "maximum_momentum_4h_atr"
        ]
    ):
        return False

    if (
        config[
            "require_close_above_prev_high"
        ]
        and
        not signal[
            "close_above_prev_high"
        ]
    ):
        return False

    if config["require_any_sweep"]:
        swept_any = any(
            signal["sweep_info"][
                lookback
            ]["swept"]
            for lookback
            in config["sweep_lookbacks"]
        )

        if not swept_any:
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
    signal = candles[signal_index]

    reference_entry = signal["close"]

    stop = (
        signal["low"]
        - STOP_BUFFER_TICKS * TICK_SIZE
    )

    reference_risk = (
        reference_entry - stop
    )

    if reference_risk <= 0:
        return None

    target = (
        reference_entry
        + reward_risk * reference_risk
    )

    backtest_entry = (
        reference_entry
        + cost_pips * PIP_SIZE
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

            if distance_high < distance_low:
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
                iso_utc(signal["time"]),
            "exit_time_utc":
                iso_utc(candle["time"]),
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

    rr_values = sorted(
        set(
            RR_VALUES
            + [3.50]
        )
    )

    total = (
        len(signals)
        * len(rr_values)
        * len(COST_PIPS_GRID)
    )

    done = 0

    for signal in signals:
        index = signal["signal_index"]

        for rr in rr_values:
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
                    (index, rr, cost)
                ] = compute_trade_outcome(
                    candles,
                    index,
                    rr,
                    cost,
                )

    return cache


# ============================================================
# BACKTEST
# ============================================================

def config_signature(config):
    return tuple(
        sorted(
            (
                key,
                tuple(value)
                if isinstance(value, tuple)
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

    if start is not None or end is not None:
        times = [
            signal["time"]
            for signal in candidates
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
                signal["signal_index"],
                config["reward_risk"],
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
    total_r = sum(results)

    profit_factor = (
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
        peak = max(peak, equity)

        max_dd = min(
            max_dd,
            equity - peak,
        )

        if result < 0:
            streak += 1
            longest = max(longest, streak)
        else:
            streak = 0

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
        "profit_factor":
            profit_factor,
        "total_r":
            total_r,
        "expectancy_r": (
            total_r / len(results)
            if results
            else 0.0
        ),
        "max_drawdown_r":
            max_dd,
        "longest_loss_streak":
            longest,
    }


def result_row(
    config,
    cost,
    trades,
):
    stats = stats_from_trades(
        trades
    )

    return {
        "candidate":
            config["label"],
        "cost_pips":
            cost,
        "minimum_body_ratio":
            config[
                "minimum_body_ratio"
            ],
        "minimum_body_atr":
            config[
                "minimum_body_atr"
            ],
        "minimum_lower_wick_body":
            config[
                "minimum_lower_wick_body"
            ],
        "maximum_momentum_4h_atr":
            config[
                "maximum_momentum_4h_atr"
            ],
        "reward_risk":
            config[
                "reward_risk"
            ],
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
            "window": (
                f"{cursor:%Y-%m-%d}"
                f" -> {end:%Y-%m-%d}"
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
        return ordered[n // 2]

    return (
        ordered[n // 2 - 1]
        + ordered[n // 2]
    ) / 2.0


def rolling_summary(rows):
    if not rows:
        return {}

    pfs = [
        float(row["profit_factor"])
        for row in rows
    ]

    rs = [
        float(row["total_r"])
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
            float(row["profit_factor"]),
    )

    worst_r = min(
        rows,
        key=lambda row:
            float(row["total_r"]),
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
                positive / len(rows) * 100.0,
                4,
            ),
        "worst_profit_factor":
            round(min(pfs), 6),
        "median_profit_factor":
            round(median(pfs), 6),
        "worst_total_r":
            round(min(rs), 4),
        "median_total_r":
            round(median(rs), 4),
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
        trade["entry_time_utc"],
        trade["exit_time_utc"],
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
        & finalist_keys
    )

    added = (
        finalist_keys
        - reference_keys
    )

    removed = (
        reference_keys
        - finalist_keys
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
        })

    return rows


# ============================================================
# RUNNER
# ============================================================

def run_research():
    try:
        candles = fetch_history(
            RESEARCH_FROM,
            RESEARCH_TO,
        )

        if len(candles) < 1000:
            raise RuntimeError(
                "Too few M15 candles returned"
            )

        STATUS.update({
            "state":
                "precomputing",
            "message":
                "Building ATR and exact-trigger features",
            "m15_candles":
                len(candles),
        })

        atr = atr14(candles)

        signals = build_signal_cache(
            candles,
            atr,
        )

        STATUS.update({
            "state":
                "precomputing",
            "message":
                "Caching reusable trade outcomes",
            "bullish_signal_universe":
                len(signals),
        })

        cache = build_outcome_cache(
            candles,
            signals,
        )

        # Guardrail: original seed should still reproduce.
        original_seed_trades = (
            run_config_cached(
                signals,
                cache,
                ORIGINAL_SEED,
                PRIMARY_COST_PIPS,
            )
        )

        original_seed_stats = (
            stats_from_trades(
                original_seed_trades
            )
        )

        if not (
            240
            <= original_seed_stats[
                "trades"
            ]
            <= 265
        ):
            raise RuntimeError(
                "Original seed reproduction failed: "
                f"expected ~251 trades, got "
                f"{original_seed_stats['trades']}. "
                "Do not trust this run."
            )

        configs = build_configs()

        STATUS.update({
            "state":
                "calculating",
            "message":
                "Running final local grid",
            "configs":
                len(configs),
        })

        summary_rows = []

        all_configs = (
            configs
            + [
                ORIGINAL_SEED,
                GEN2_WINNER_REFERENCE,
            ]
        )

        for number, config in enumerate(
            all_configs,
            start=1,
        ):
            for cost in COST_PIPS_GRID:
                trades = run_config_cached(
                    signals,
                    cache,
                    config,
                    cost,
                )

                summary_rows.append(
                    result_row(
                        config,
                        cost,
                        trades,
                    )
                )

            if number % 50 == 0:
                STATUS["message"] = (
                    "Final local grid "
                    f"{number}/{len(all_configs)}"
                )

        write_csv(
            OUTPUT_SUMMARY,
            summary_rows,
        )

        primary = [
            row
            for row in summary_rows
            if (
                abs(
                    float(row["cost_pips"])
                    - PRIMARY_COST_PIPS
                )
                < 1e-12
                and
                int(row["trades"]) >= 40
            )
        ]

        primary.sort(
            key=lambda row: (
                float(row["profit_factor"]),
                float(row["expectancy_r"]),
                float(row["total_r"]),
                int(row["trades"]),
            ),
            reverse=True,
        )

        top_rows = primary[:40]

        write_csv(
            OUTPUT_TOP,
            top_rows,
        )

        config_lookup = {
            config["label"]: config
            for config in all_configs
        }

        finalists = [
            config_lookup[
                row["candidate"]
            ]
            for row in top_rows[:15]
        ]

        STATUS["message"] = (
            "Running 4-era validation"
        )

        era_rows = validation_rows(
            signals,
            cache,
            finalists,
            era_windows(),
        )

        STATUS["message"] = (
            "Running dev / validation"
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

        robust = []

        for config in finalists:
            label = config["label"]

            base = next(
                row
                for row in top_rows
                if row["candidate"] == label
            )

            eras = [
                row
                for row in era_rows
                if (
                    row["candidate"] == label
                    and int(row["trades"]) > 0
                )
            ]

            devval = [
                row
                for row in devval_rows
                if (
                    row["candidate"] == label
                    and int(row["trades"]) > 0
                )
            ]

            recent = [
                row
                for row in recent_rows
                if (
                    row["candidate"] == label
                    and int(row["trades"]) > 0
                )
            ]

            era_pfs = [
                float(row["profit_factor"])
                for row in eras
            ]

            dev_pfs = [
                float(row["profit_factor"])
                for row in devval
            ]

            recent_pfs = [
                float(row["profit_factor"])
                for row in recent
            ]

            min_era = (
                min(era_pfs)
                if era_pfs
                else 0.0
            )

            min_dev = (
                min(dev_pfs)
                if dev_pfs
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
                    int(base["trades"]),
                "full_pf":
                    float(
                        base["profit_factor"]
                    ),
                "full_total_r":
                    float(base["total_r"]),
                "full_expectancy":
                    float(
                        base["expectancy_r"]
                    ),
                "minimum_era_pf":
                    min_era,
                "minimum_devval_pf":
                    min_dev,
                "minimum_recent_pf":
                    min_recent,
                "score": (
                    min_era * 3.0
                    + min_dev * 2.0
                    + min_recent * 2.0
                    + float(
                        base["profit_factor"]
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
            for row in robust[:6]
        ]

        STATUS["message"] = (
            "Running rolling 2Y / 3Y"
        )

        rolling_rows = []
        rolling_summary_rows = []

        for config in robust_finalists:
            for months in [24, 36]:
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

        STATUS["message"] = (
            "Running overlap / ablation"
        )

        overlap = []

        for config in robust_finalists:
            overlap.extend(
                overlap_rows(
                    signals,
                    cache,
                    ORIGINAL_SEED,
                    config,
                )
            )

            overlap.extend(
                overlap_rows(
                    signals,
                    cache,
                    GEN2_WINNER_REFERENCE,
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
            else GEN2_WINNER_REFERENCE
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
                "Building ZIP results bundle",
        })

        build_results_bundle()

        STATUS.update({
            "state":
                "complete",
            "message":
                "USDJPY M15 Long final local complete",
            "original_seed_reproduced":
                True,
            "original_seed_stats":
                original_seed_stats,
            "configs":
                len(configs),
            "robust_ranking":
                robust[:12],
            "selected_best":
                best,
            "results_bundle":
                OUTPUT_BUNDLE,
        })

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
            "USDJPY M15 Long Final Local Confirmation",
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
            "/usdjpy-m15-long-final/status",
            "/usdjpy-m15-long-final/results",
        ],
    })


@app.route(
    "/usdjpy-m15-long-final/status"
)
def route_status():
    return jsonify(STATUS)


@app.route(
    "/usdjpy-m15-long-final/results"
)
def route_results():
    return download_file(
        OUTPUT_BUNDLE
    )


if __name__ == "__main__":
    research_thread = threading.Thread(
        target=run_research,
        name="usdjpy-m15-long-final-local",
        daemon=True,
    )

    research_thread.start()

    port = int(
        os.getenv("PORT", 5000)
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
    )
