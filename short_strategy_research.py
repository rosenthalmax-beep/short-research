
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
# USD/JPY M15 LONG — FINALIST COMPARISON / 12M VERIFICATION
#
# Compare exactly:
#
# A) HIGHER-FREQUENCY VERSION
#    BA >= 1.25
#    lower wick/body >= 0.25
#    prior 4h momentum <= -1.75 ATR
#    RR = 4.00
#
# B) HIGH-PF VERSION
#    BA >= 1.30
#    lower wick/body >= 0.35
#    prior 4h momentum <= -1.75 ATR
#    RR = 3.25
#
# Shared exact trigger:
#   - bullish M15 candle
#   - body ratio >= 1.00
#   - sweep ANY prior 20/40/60/100-bar low
#   - close > previous M15 high
#   - no engulfing requirement
#
# Outputs:
#   - full-history stats
#   - cost stress 0.5 / 1 / 1.5 / 2 pip
#   - rolling 12M / 24M / 36M
#   - rolling summaries
#   - completed calendar-year stats
#   - profitable-year %
#   - active-year profitable %
#
# Correctness:
#   - OANDA midpoint
#   - M15 candle OPEN timestamps
#   - ATR14 Wilder/RMA, SMA seeded
#   - 4h momentum ends at previous completed M15 candle
#   - stop = signal low - 10 ticks
#   - target uses REFERENCE signal-close risk
#   - adverse long fill = signal close + cost
#   - exits start NEXT candle
#   - same-bar long tie:
#       high closer to open => target first
#       else stop first
#   - pyramiding 0
#
# USDJPY:
#   tick = 0.001
#   pip  = 0.01
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
    2010, 1, 1,
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

TICK_SIZE = 0.001
PIP_SIZE = 0.01
STOP_BUFFER_TICKS = 10

PRIMARY_COST_PIPS = 1.00
COST_PIPS_GRID = [0.50, 1.00, 1.50, 2.00]

TRIGGER_SWEEP_LOOKBACKS = (
    20,
    40,
    60,
    100,
)

HIGH_FREQ = {
    "label":
        "HIGH_FREQ_75T_BA1.25_LW0.25_M4NEG1.75_RR4.00",
    "minimum_body_ratio":
        1.00,
    "minimum_body_atr":
        1.25,
    "minimum_lower_wick_body":
        0.25,
    "maximum_momentum_4h_atr":
        -1.75,
    "reward_risk":
        4.00,
}

HIGH_PF = {
    "label":
        "HIGH_PF_45T_BA1.30_LW0.35_M4NEG1.75_RR3.25",
    "minimum_body_ratio":
        1.00,
    "minimum_body_atr":
        1.30,
    "minimum_lower_wick_body":
        0.35,
    "maximum_momentum_4h_atr":
        -1.75,
    "reward_risk":
        3.25,
}

CONFIGS = [
    HIGH_FREQ,
    HIGH_PF,
]

OUTPUT_FULL = (
    "usdjpy_m15_long_finalist_full_history.csv"
)

OUTPUT_COST = (
    "usdjpy_m15_long_finalist_cost_stress.csv"
)

OUTPUT_ROLLING = (
    "usdjpy_m15_long_finalist_rolling.csv"
)

OUTPUT_ROLLING_SUMMARY = (
    "usdjpy_m15_long_finalist_rolling_summary.csv"
)

OUTPUT_CALENDAR = (
    "usdjpy_m15_long_finalist_calendar_years.csv"
)

OUTPUT_CALENDAR_SUMMARY = (
    "usdjpy_m15_long_finalist_calendar_summary.csv"
)

OUTPUT_BUNDLE = (
    "usdjpy_m15_long_finalist_12M_verification_RESULTS.zip"
)

STATUS = {
    "state": "not_started",
    "message": "USDJPY finalist verification not started",
    "service": "USDJPY M15 Long Finalist 12M Verification",
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


def build_bundle():
    files = [
        OUTPUT_FULL,
        OUTPUT_COST,
        OUTPUT_ROLLING,
        OUTPUT_ROLLING_SUMMARY,
        OUTPUT_CALENDAR,
        OUTPUT_CALENDAR_SUMMARY,
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
            by_time[
                row["time"]
            ] = row

        cursor = chunk_end
        time.sleep(0.02)

    rows = list(
        by_time.values()
    )

    rows.sort(
        key=lambda row:
            row["time"]
    )

    return rows


# ============================================================
# ATR
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
                -
                candle["low"]
            )
        else:
            previous_close = candles[
                i - 1
            ]["close"]

            result[i] = max(
                candle["high"]
                -
                candle["low"],
                abs(
                    candle["high"]
                    -
                    previous_close
                ),
                abs(
                    candle["low"]
                    -
                    previous_close
                ),
            )

    return result


def rma(values, length):
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
            ] * (
                length - 1
            )
            +
            values[i]
        ) / length

    return result


def atr14(candles):
    return rma(
        true_ranges(candles),
        14,
    )


# ============================================================
# SIGNAL CACHE
# ============================================================

def build_signal_cache(
    candles,
    atr,
):
    signals = []

    start_index = max(
        max(
            TRIGGER_SWEEP_LOOKBACKS
        ),
        17,
        14,
    )

    for i in range(
        start_index,
        len(candles),
    ):
        current = candles[i]
        previous = candles[
            i - 1
        ]

        if not (
            current["close"]
            >
            current["open"]
        ):
            continue

        value_atr = atr[i]

        if (
            value_atr is None
            or
            value_atr <= 0
        ):
            continue

        body = (
            current["close"]
            -
            current["open"]
        )

        previous_body = abs(
            previous["close"]
            -
            previous["open"]
        )

        body_ratio = (
            body
            /
            previous_body
            if previous_body > 0
            else 999.0
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
            lower_wick
            /
            body
            if body > 0
            else 0.0
        )

        momentum_4h_atr = (
            candles[
                i - 1
            ]["close"]
            -
            candles[
                i - 17
            ]["close"]
        ) / value_atr

        swept_any = False

        for lookback in (
            TRIGGER_SWEEP_LOOKBACKS
        ):
            prior_low = min(
                candle["low"]
                for candle in candles[
                    i - lookback:i
                ]
            )

            if (
                current["low"]
                <
                prior_low
            ):
                swept_any = True
                break

        signals.append({
            "signal_index":
                i,
            "time":
                current["time"],
            "body_ratio":
                body_ratio,
            "body_atr":
                body
                /
                value_atr,
            "lower_wick_body":
                lower_wick_body,
            "momentum_4h_atr":
                momentum_4h_atr,
            "close_above_prev_high":
                (
                    current["close"]
                    >
                    previous["high"]
                ),
            "swept_any":
                swept_any,
        })

    return signals


def signal_passes(
    signal,
    config,
):
    return (
        signal["body_ratio"]
        >=
        config[
            "minimum_body_ratio"
        ]
        and
        signal["body_atr"]
        >=
        config[
            "minimum_body_atr"
        ]
        and
        signal[
            "lower_wick_body"
        ]
        >=
        config[
            "minimum_lower_wick_body"
        ]
        and
        signal[
            "momentum_4h_atr"
        ]
        <=
        config[
            "maximum_momentum_4h_atr"
        ]
        and
        signal[
            "close_above_prev_high"
        ]
        and
        signal["swept_any"]
    )


# ============================================================
# TRADE OUTCOMES
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
        *
        TICK_SIZE
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
        *
        reference_risk
    )

    backtest_entry = (
        reference_entry
        +
        cost_pips
        *
        PIP_SIZE
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
            "result_r":
                result_r,
        }

    return None


def build_outcome_cache(
    candles,
    signals,
):
    cache = {}

    rr_values = sorted(
        {
            config["reward_risk"]
            for config in CONFIGS
        }
    )

    for signal in signals:
        index = signal[
            "signal_index"
        ]

        for rr in rr_values:
            for cost in COST_PIPS_GRID:
                cache[
                    (
                        index,
                        rr,
                        cost,
                    )
                ] = (
                    compute_trade_outcome(
                        candles,
                        index,
                        rr,
                        cost,
                    )
                )

    return cache


# ============================================================
# BACKTEST
# ============================================================

def qualifying_candidates(
    signals,
    config,
):
    return [
        signal
        for signal in signals
        if signal_passes(
            signal,
            config,
        )
    ]


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
        candidates = [
            signal
            for signal in candidates
            if (
                (
                    start is None
                    or
                    signal["time"]
                    >=
                    start
                )
                and
                (
                    end is None
                    or
                    signal["time"]
                    <
                    end
                )
            )
        ]

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

    total_r = sum(
        results
    )

    pf = (
        gross_profit
        /
        gross_loss
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


def stats_row(
    config,
    label,
    trades,
    cost=None,
):
    stats = stats_from_trades(
        trades
    )

    return {
        "strategy":
            config["label"],
        "window":
            label,
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
# ROLLING
# ============================================================

def rolling_windows(
    signals,
    cache,
    config,
    months,
):
    rows = []

    cursor = month_start(
        RESEARCH_FROM
    )

    final_start = add_months(
        month_start(
            RESEARCH_TO
        ),
        -months,
    )

    while cursor <= final_start:
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
            "strategy":
                config["label"],
            "months":
                months,
            "start":
                cursor.date().isoformat(),
            "end":
                end.date().isoformat(),
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
            "positive":
                stats["total_r"] > 0,
            "active":
                stats["trades"] > 0,
        })

        cursor = add_months(
            cursor,
            1,
        )

    return rows


def median(values):
    ordered = sorted(values)

    if not ordered:
        return None

    n = len(ordered)

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


def rolling_summary(
    config,
    months,
    rows,
):
    active = [
        row
        for row in rows
        if row["active"]
    ]

    positive = [
        row
        for row in rows
        if row["positive"]
    ]

    positive_active = [
        row
        for row in active
        if row["positive"]
    ]

    pfs = [
        float(
            row["profit_factor"]
        )
        for row in active
    ]

    rs = [
        float(
            row["total_r"]
        )
        for row in rows
    ]

    return {
        "strategy":
            config["label"],
        "months":
            months,
        "windows":
            len(rows),
        "active_windows":
            len(active),
        "positive_windows":
            len(positive),
        "positive_windows_pct":
            round(
                len(positive)
                /
                len(rows)
                * 100.0,
                4,
            )
            if rows
            else 0.0,
        "positive_active_windows":
            len(positive_active),
        "positive_active_windows_pct":
            round(
                len(positive_active)
                /
                len(active)
                * 100.0,
                4,
            )
            if active
            else 0.0,
        "median_pf_active":
            round(
                median(pfs),
                6,
            )
            if pfs
            else None,
        "median_total_r":
            round(
                median(rs),
                4,
            )
            if rs
            else None,
        "worst_total_r":
            round(
                min(rs),
                4,
            )
            if rs
            else None,
    }


# ============================================================
# CALENDAR YEARS
# ============================================================

def calendar_year_rows(
    signals,
    cache,
    config,
):
    rows = []

    first_year = (
        RESEARCH_FROM.year
    )

    last_complete_year = (
        RESEARCH_TO.year - 1
    )

    for year in range(
        first_year,
        last_complete_year + 1,
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
            "strategy":
                config["label"],
            "year":
                year,
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
            "positive":
                stats["total_r"] > 0,
            "active":
                stats["trades"] > 0,
        })

    return rows


def calendar_summary(
    config,
    rows,
):
    active = [
        row
        for row in rows
        if row["active"]
    ]

    profitable = [
        row
        for row in rows
        if row["positive"]
    ]

    profitable_active = [
        row
        for row in active
        if row["positive"]
    ]

    return {
        "strategy":
            config["label"],
        "completed_years":
            len(rows),
        "active_years":
            len(active),
        "profitable_years":
            len(profitable),
        "profitable_years_pct":
            round(
                len(profitable)
                /
                len(rows)
                * 100.0,
                4,
            )
            if rows
            else 0.0,
        "profitable_active_years":
            len(profitable_active),
        "profitable_active_years_pct":
            round(
                len(profitable_active)
                /
                len(active)
                * 100.0,
                4,
            )
            if active
            else 0.0,
    }


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
                "Building ATR and finalist signal cache",
            "m15_candles":
                len(candles),
        })

        atr = atr14(
            candles
        )

        signals = build_signal_cache(
            candles,
            atr,
        )

        cache = build_outcome_cache(
            candles,
            signals,
        )

        full_rows = []
        cost_rows = []
        rolling_rows = []
        rolling_summary_rows = []
        calendar_rows_all = []
        calendar_summary_rows = []

        for config in CONFIGS:
            trades = run_config_cached(
                signals,
                cache,
                config,
                PRIMARY_COST_PIPS,
            )

            full_rows.append(
                stats_row(
                    config,
                    "FULL_HISTORY",
                    trades,
                    PRIMARY_COST_PIPS,
                )
            )

            for cost in COST_PIPS_GRID:
                cost_trades = run_config_cached(
                    signals,
                    cache,
                    config,
                    cost,
                )

                cost_rows.append(
                    stats_row(
                        config,
                        "FULL_HISTORY",
                        cost_trades,
                        cost,
                    )
                )

            for months in [
                12,
                24,
                36,
            ]:
                rows = rolling_windows(
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
                        config,
                        months,
                        rows,
                    )
                )

            years = calendar_year_rows(
                signals,
                cache,
                config,
            )

            calendar_rows_all.extend(
                years
            )

            calendar_summary_rows.append(
                calendar_summary(
                    config,
                    years,
                )
            )

        write_csv(
            OUTPUT_FULL,
            full_rows,
        )

        write_csv(
            OUTPUT_COST,
            cost_rows,
        )

        write_csv(
            OUTPUT_ROLLING,
            rolling_rows,
        )

        write_csv(
            OUTPUT_ROLLING_SUMMARY,
            rolling_summary_rows,
        )

        write_csv(
            OUTPUT_CALENDAR,
            calendar_rows_all,
        )

        write_csv(
            OUTPUT_CALENDAR_SUMMARY,
            calendar_summary_rows,
        )

        build_bundle()

        STATUS.update({
            "state":
                "complete",
            "message":
                "USDJPY finalist verification complete",
            "full_history":
                full_rows,
            "rolling_summary":
                rolling_summary_rows,
            "calendar_summary":
                calendar_summary_rows,
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
            "USDJPY M15 Long Finalist 12M Verification",
        "status":
            STATUS["state"],
        "instrument":
            INSTRUMENT,
        "timeframe":
            "M15",
        "side":
            "BUY",
        "routes": [
            "/usdjpy-m15-long-finalists/status",
            "/usdjpy-m15-long-finalists/results",
        ],
    })


@app.route(
    "/usdjpy-m15-long-finalists/status"
)
def route_status():
    return jsonify(
        STATUS
    )


@app.route(
    "/usdjpy-m15-long-finalists/results"
)
def route_results():
    return download_file(
        OUTPUT_BUNDLE
    )


if __name__ == "__main__":
    research_thread = threading.Thread(
        target=run_research,
        name="usdjpy-m15-long-finalists",
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
