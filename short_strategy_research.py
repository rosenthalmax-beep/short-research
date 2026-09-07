
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
# USD/JPY M15 LONG — FREQUENCY EXPANSION PASS
#
# Exact corrected base trigger is PRESERVED:
#   - current M15 candle bullish
#   - body ratio >= 1.00 vs previous candle body
#   - sweep of ANY prior 20/40/60/100-bar low
#   - close > previous M15 high
#   - NO exact engulfing requirement
#
# Frequency-expansion search:
#   body ATR:
#     1.10 / 1.15 / 1.20 / 1.25
#
#   lower wick/body:
#     0.20 / 0.25 / 0.30 / 0.35
#
#   prior 4h momentum max:
#     -1.25 / -1.50 / -1.75
#
#   RR:
#     3.25 / 3.50 / 3.75 / 4.00
#
# Goal:
#   find a broader, simpler variant with materially more trades
#   while preserving strong robustness.
#
# Ranking emphasis:
#   1) worst-era PF
#   2) total R
#   3) trade count
#   4) full-history PF
#
# Correctness:
#   - OANDA midpoint
#   - M15 signal timestamp = candle OPEN
#   - ATR14 Wilder/RMA, SMA seeded
#   - prior 4h momentum uses previous completed M15 close
#   - stop = signal low - 10 ticks
#   - target based on REFERENCE signal close
#   - adverse long entry = signal close + cost
#   - exits start NEXT candle
#   - same-bar long tie:
#       high closer to open => TARGET first
#       else STOP first
#   - exact exit-candle signal eligible
#   - pyramiding 0
#
# USDJPY:
#   tick = 0.001
#   pip  = 0.01
#
# Development cost:
#   1.0 pip adverse
#
# Stress:
#   0.5 / 1.0 / 1.5 / 2.0 pips
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

TICK_SIZE = 0.001
PIP_SIZE = 0.01
STOP_BUFFER_TICKS = 10

PRIMARY_COST_PIPS = 1.00
COST_PIPS_GRID = [0.50, 1.00, 1.50, 2.00]

BODY_ATR_VALUES = [
    1.10,
    1.15,
    1.20,
    1.25,
]

LOWER_WICK_VALUES = [
    0.20,
    0.25,
    0.30,
    0.35,
]

MOM4H_MAX_VALUES = [
    -1.25,
    -1.50,
    -1.75,
]

RR_VALUES = [
    3.25,
    3.50,
    3.75,
    4.00,
]

TRIGGER_SWEEP_LOOKBACKS = (
    20,
    40,
    60,
    100,
)

OUTPUT_SUMMARY = (
    "usdjpy_m15_long_frequency_expansion_summary.csv"
)

OUTPUT_TOP = (
    "usdjpy_m15_long_frequency_expansion_top.csv"
)

OUTPUT_ERAS = (
    "usdjpy_m15_long_frequency_expansion_eras.csv"
)

OUTPUT_DEVVAL = (
    "usdjpy_m15_long_frequency_expansion_dev_validation.csv"
)

OUTPUT_RECENT = (
    "usdjpy_m15_long_frequency_expansion_recent.csv"
)

OUTPUT_ROLLING = (
    "usdjpy_m15_long_frequency_expansion_rolling.csv"
)

OUTPUT_ROLLING_SUMMARY = (
    "usdjpy_m15_long_frequency_expansion_rolling_summary.csv"
)

OUTPUT_OVERLAP = (
    "usdjpy_m15_long_frequency_expansion_overlap.csv"
)

OUTPUT_BEST_TRADES = (
    "usdjpy_m15_long_frequency_expansion_best_trades.csv"
)

OUTPUT_BUNDLE = (
    "usdjpy_m15_long_frequency_expansion_RESULTS.zip"
)

STATUS = {
    "state": "not_started",
    "message": "USDJPY M15 Long frequency expansion not started",
    "service": "USDJPY M15 Long Frequency Expansion",
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
            > previous["high"]
        )

        prior_close = (
            candles[i - 1]["close"]
        )

        momentum_4h_atr = (
            prior_close
            - candles[i - 17]["close"]
        ) / value_atr

        sweep_info = {}

        for lookback in TRIGGER_SWEEP_LOOKBACKS:
            prior_low = min(
                candle["low"]
                for candle in candles[
                    i - lookback:i
                ]
            )

            sweep_info[lookback] = {
                "swept":
                    current["low"] < prior_low,
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
            "lower_wick_body":
                lower_wick_body,
            "close_above_prev_high":
                close_above_prev_high,
            "momentum_4h_atr":
                momentum_4h_atr,
            "swept_any":
                swept_any,
        })

    return signals


# ============================================================
# CONFIGS
# ============================================================

ORIGINAL_SEED = {
    "label": "REFERENCE_ORIGINAL_SEED",
    "minimum_body_ratio": 1.00,
    "minimum_body_atr": 1.25,
    "minimum_lower_wick_body": None,
    "maximum_momentum_4h_atr": None,
    "reward_risk": 3.50,
}

CURRENT_LOCK_CANDIDATE = {
    "label": "REFERENCE_45T_PF3.8",
    "minimum_body_ratio": 1.00,
    "minimum_body_atr": 1.30,
    "minimum_lower_wick_body": 0.35,
    "maximum_momentum_4h_atr": -1.75,
    "reward_risk": 3.25,
}

FREQUENCY_REFERENCE = {
    "label": "REFERENCE_70T_FREQ",
    "minimum_body_ratio": 1.00,
    "minimum_body_atr": 1.20,
    "minimum_lower_wick_body": 0.35,
    "maximum_momentum_4h_atr": -1.50,
    "reward_risk": 3.75,
}


def build_configs():
    configs = []
    counter = 0

    for body_atr in BODY_ATR_VALUES:
        for lower_wick in LOWER_WICK_VALUES:
            for momentum_max in MOM4H_MAX_VALUES:
                for rr in RR_VALUES:
                    counter += 1

                    configs.append({
                        "label": (
                            f"C{counter:03d}_"
                            f"BA{body_atr:.2f}_"
                            f"LW{lower_wick:.2f}_"
                            f"M4MAX{momentum_max:.2f}_"
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
                        "reward_risk":
                            rr,
                    })

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

    if not signal["close_above_prev_high"]:
        return False

    if not signal["swept_any"]:
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
            else:
                exit_price = stop

        elif hit_stop:
            exit_price = stop

        elif hit_target:
            exit_price = target

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
            RR_VALUES + [3.25, 3.50, 3.75]
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

CANDIDATE_CACHE = {}


def config_signature(config):
    return tuple(
        sorted(
            (
                key,
                value,
            )
            for key, value in config.items()
            if key != "label"
        )
    )


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

        candidates = candidates[left:right]

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
        float(t["result_r"])
        for t in trades
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
            pf,
        "total_r":
            total_r,
        "expectancy_r":
            (
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
        "minimum_body_atr":
            config["minimum_body_atr"],
        "minimum_lower_wick_body":
            config[
                "minimum_lower_wick_body"
            ],
        "maximum_momentum_4h_atr":
            config[
                "maximum_momentum_4h_atr"
            ],
        "reward_risk":
            config["reward_risk"],
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
                "rank": rank,
                "window": label,
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
        month_start(RESEARCH_TO),
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

    return {
        "candidate":
            rows[0]["candidate"],
        "months":
            rows[0]["months"],
        "windows":
            len(rows),
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
                if trade_key(t) in shared
            ],
        ),
        (
            "FINALIST_ADDED",
            [
                t
                for t in finalist_trades
                if trade_key(t) in added
            ],
        ),
        (
            "REFERENCE_REMOVED",
            [
                t
                for t in reference_trades
                if trade_key(t) in removed
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
                "Building exact-trigger features",
            "m15_candles":
                len(candles),
        })

        atr = atr14(candles)

        signals = build_signal_cache(
            candles,
            atr,
        )

        cache = build_outcome_cache(
            candles,
            signals,
        )

        original_seed_trades = run_config_cached(
            signals,
            cache,
            ORIGINAL_SEED,
            PRIMARY_COST_PIPS,
        )

        original_seed_stats = stats_from_trades(
            original_seed_trades
        )

        if not (
            240
            <= original_seed_stats["trades"]
            <= 265
        ):
            raise RuntimeError(
                "Original seed reproduction failed: "
                f"expected ~251 trades, got "
                f"{original_seed_stats['trades']}. "
                "Do not trust this run."
            )

        configs = build_configs()

        all_configs = (
            configs
            + [
                ORIGINAL_SEED,
                CURRENT_LOCK_CANDIDATE,
                FREQUENCY_REFERENCE,
            ]
        )

        STATUS.update({
            "state":
                "calculating",
            "message":
                "Running frequency-expansion grid",
            "configs":
                len(configs),
        })

        summary_rows = []

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

            if number % 25 == 0:
                STATUS["message"] = (
                    "Frequency-expansion grid "
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
                int(row["trades"]) >= 55
            )
        ]

        config_lookup = {
            config["label"]: config
            for config in all_configs
        }

        # First pass: validate top by full history,
        # then rerank with worst-era emphasis.
        primary.sort(
            key=lambda row: (
                float(row["total_r"]),
                int(row["trades"]),
                float(row["profit_factor"]),
            ),
            reverse=True,
        )

        preliminary = primary[:30]

        prelim_configs = [
            config_lookup[
                row["candidate"]
            ]
            for row in preliminary
        ]

        era_rows = validation_rows(
            signals,
            cache,
            prelim_configs,
            era_windows(),
        )

        devval_rows = validation_rows(
            signals,
            cache,
            prelim_configs,
            devval_windows(),
        )

        recent_rows = validation_rows(
            signals,
            cache,
            prelim_configs,
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

        ranked = []

        for row in preliminary:
            label = row["candidate"]

            eras = [
                item
                for item in era_rows
                if (
                    item["candidate"] == label
                    and int(item["trades"]) > 0
                )
            ]

            devval = [
                item
                for item in devval_rows
                if (
                    item["candidate"] == label
                    and int(item["trades"]) > 0
                )
            ]

            recent = [
                item
                for item in recent_rows
                if (
                    item["candidate"] == label
                    and int(item["trades"]) > 0
                )
            ]

            min_era_pf = min(
                float(item["profit_factor"])
                for item in eras
            ) if eras else 0.0

            min_devval_pf = min(
                float(item["profit_factor"])
                for item in devval
            ) if devval else 0.0

            min_recent_pf = min(
                float(item["profit_factor"])
                for item in recent
            ) if recent else 0.0

            score = (
                min_era_pf * 6.0
                + min_devval_pf * 3.0
                + min_recent_pf * 2.0
                + float(row["total_r"]) / 20.0
                + int(row["trades"]) / 100.0
                + float(row["profit_factor"])
            )

            ranked.append({
                **row,
                "minimum_era_pf":
                    round(min_era_pf, 6),
                "minimum_devval_pf":
                    round(min_devval_pf, 6),
                "minimum_recent_pf":
                    round(min_recent_pf, 6),
                "robust_frequency_score":
                    round(score, 6),
            })

        ranked.sort(
            key=lambda row:
                row["robust_frequency_score"],
            reverse=True,
        )

        top_rows = ranked[:20]

        write_csv(
            OUTPUT_TOP,
            top_rows,
        )

        robust_finalists = [
            config_lookup[
                row["candidate"]
            ]
            for row in top_rows[:6]
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
            "Running overlap comparisons"
        )

        overlap = []

        for config in robust_finalists:
            overlap.extend(
                overlap_rows(
                    signals,
                    cache,
                    CURRENT_LOCK_CANDIDATE,
                    config,
                )
            )

            overlap.extend(
                overlap_rows(
                    signals,
                    cache,
                    FREQUENCY_REFERENCE,
                    config,
                )
            )

            overlap.extend(
                overlap_rows(
                    signals,
                    cache,
                    ORIGINAL_SEED,
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
            else FREQUENCY_REFERENCE
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
                "USDJPY M15 Long frequency expansion complete",
            "original_seed_reproduced":
                True,
            "original_seed_stats":
                original_seed_stats,
            "configs":
                len(configs),
            "selected_best":
                best,
            "top_ranked":
                top_rows[:10],
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
            "USDJPY M15 Long Frequency Expansion",
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
            "/usdjpy-m15-long-frequency/status",
            "/usdjpy-m15-long-frequency/results",
        ],
    })


@app.route(
    "/usdjpy-m15-long-frequency/status"
)
def route_status():
    return jsonify(STATUS)


@app.route(
    "/usdjpy-m15-long-frequency/results"
)
def route_results():
    return download_file(
        OUTPUT_BUNDLE
    )


if __name__ == "__main__":
    research_thread = threading.Thread(
        target=run_research,
        name="usdjpy-m15-long-frequency",
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
