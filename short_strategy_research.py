
import os
import csv
import time
import bisect
import threading
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from flask import Flask, jsonify, send_file


# ============================================================
# EUR/USD M15 LONG
# FINAL LOCAL ANTI-OVERFIT CONFIRMATION
#
# READ-ONLY RESEARCH. NEVER SENDS ORDERS.
#
# Core held fixed from exhaustive pass:
#   - exact bullish engulfing
#   - body >= 0.75 ATR14
#   - no range filter
#   - no strong-close filter
#   - no wick filter
#   - no H1 / Daily regime
#   - stop = signal low - 10 ticks
#   - pyramiding 0
#
# Local confirmation dimensions:
#   - body ratio:
#       1.20 / 1.25 / 1.30 / 1.35 / 1.40
#   - structure:
#       150 / 165 / 180
#   - distance:
#       0.075 / 0.100 / 0.125 ATR
#   - RR:
#       3.50 / 3.75 / 4.00
#   - time filters:
#       none
#       Tue only
#       Tue+Fri
#       Tue+Wed
#       Tue+Thu
#       exclude H06
#       exclude H07
#       exclude H08
#       exclude H06+H07
#       exclude H07+H08
#       Tue + H06
#       Tue + H07
#       Tue + H08
#       Tue + H06+H07
#       Tue + H07+H08
#
# Costs:
#   0.50 / 1.00 / 1.50 / 2.00 pips adverse entry
#
# Primary development cost:
#   1.00 pip
#
# Validation:
#   - 4 eras
#   - DEV 2010-2017 vs VALIDATION 2018-now
#   - recent 5Y / 2Y
#   - monthly rolling 2Y / 3Y
#   - overlap/exclusive versus old incumbent
#   - overlap/exclusive versus current lead
#
# Historical conventions:
#   - OANDA midpoint M15
#   - ATR14 Wilder/RMA, SMA seeded
#   - exact bullish engulfing
#   - signal time = M15 candle OPEN
#   - target based on REFERENCE signal-close risk
#   - adverse long entry = signal close + cost
#   - exits begin NEXT candle
#   - same-bar long tie:
#       high closer => target first
#       otherwise stop first
#   - exact exit-candle signal eligible
# ============================================================


app = Flask(__name__)

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_BASE = os.getenv(
    "OANDA_API_URL",
    "https://api-fxtrade.oanda.com",
)

INSTRUMENT = "EUR_USD"
GRANULARITY = "M15"

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

BODY_ATR_FIXED = 0.75

BODY_RATIOS = [
    1.20,
    1.25,
    1.30,
    1.35,
    1.40,
]

LOOKBACKS = [
    150,
    165,
    180,
]

DISTANCES = [
    0.075,
    0.100,
    0.125,
]

REWARD_RISKS = [
    3.50,
    3.75,
    4.00,
]

# Python weekday(): Mon=0 ... Fri=4
TIME_FILTERS = [
    {
        "name": "NONE",
        "excluded_weekdays": set(),
        "excluded_hours": set(),
    },
    {
        "name": "EX_TUE",
        "excluded_weekdays": {1},
        "excluded_hours": set(),
    },
    {
        "name": "EX_TUE_FRI",
        "excluded_weekdays": {1, 4},
        "excluded_hours": set(),
    },
    {
        "name": "EX_TUE_WED",
        "excluded_weekdays": {1, 2},
        "excluded_hours": set(),
    },
    {
        "name": "EX_TUE_THU",
        "excluded_weekdays": {1, 3},
        "excluded_hours": set(),
    },
    {
        "name": "EX_H06",
        "excluded_weekdays": set(),
        "excluded_hours": {6},
    },
    {
        "name": "EX_H07",
        "excluded_weekdays": set(),
        "excluded_hours": {7},
    },
    {
        "name": "EX_H08",
        "excluded_weekdays": set(),
        "excluded_hours": {8},
    },
    {
        "name": "EX_H06_H07",
        "excluded_weekdays": set(),
        "excluded_hours": {6, 7},
    },
    {
        "name": "EX_H07_H08",
        "excluded_weekdays": set(),
        "excluded_hours": {7, 8},
    },
    {
        "name": "EX_TUE_H06",
        "excluded_weekdays": {1},
        "excluded_hours": {6},
    },
    {
        "name": "EX_TUE_H07",
        "excluded_weekdays": {1},
        "excluded_hours": {7},
    },
    {
        "name": "EX_TUE_H08",
        "excluded_weekdays": {1},
        "excluded_hours": {8},
    },
    {
        "name": "EX_TUE_H06_H07",
        "excluded_weekdays": {1},
        "excluded_hours": {6, 7},
    },
    {
        "name": "EX_TUE_H07_H08",
        "excluded_weekdays": {1},
        "excluded_hours": {7, 8},
    },
]

OLD_INCUMBENT = {
    "label": "OLD_INCUMBENT",
    "minimum_body_ratio": 1.00,
    "minimum_body_atr": 0.75,
    "structure_lookback": 165,
    "maximum_distance_atr": 0.10,
    "excluded_weekdays": set(),
    "excluded_hours": set(),
    "reward_risk": 3.75,
}

CURRENT_LEAD = {
    "label": "CURRENT_LEAD_EX_TUE_H07",
    "minimum_body_ratio": 1.00,
    "minimum_body_atr": 0.75,
    "structure_lookback": 165,
    "maximum_distance_atr": 0.10,
    "excluded_weekdays": {1},
    "excluded_hours": {7},
    "reward_risk": 3.75,
}


OUTPUT_SUMMARY = (
    "eurusd_m15_long_final_local_summary.csv"
)

OUTPUT_TOP = (
    "eurusd_m15_long_final_local_top.csv"
)

OUTPUT_ERAS = (
    "eurusd_m15_long_final_local_eras.csv"
)

OUTPUT_DEVVAL = (
    "eurusd_m15_long_final_local_dev_validation.csv"
)

OUTPUT_RECENT = (
    "eurusd_m15_long_final_local_recent.csv"
)

OUTPUT_ROLLING = (
    "eurusd_m15_long_final_local_rolling.csv"
)

OUTPUT_ROLLING_SUMMARY = (
    "eurusd_m15_long_final_local_rolling_summary.csv"
)

OUTPUT_OVERLAP = (
    "eurusd_m15_long_final_local_overlap.csv"
)

OUTPUT_BEST_TRADES = (
    "eurusd_m15_long_final_local_best_trades.csv"
)

STATUS = {
    "state": "not_started",
    "message": "Final local confirmation has not started",
    "service": "EURUSD M15 Long Final Local Confirmation",
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
        f"{OANDA_BASE}"
        f"/v3/instruments/"
        f"{INSTRUMENT}/candles"
    )

    params = {
        "price": "M",
        "granularity": GRANULARITY,
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

        candles.append({
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

    return candles


def fetch_full_history(start, end):
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
                f"Fetching chunk {chunk_number}: "
                f"{iso_utc(cursor)} -> "
                f"{iso_utc(chunk_end)}"
            ),
            "chunk": chunk_number,
        })

        for candle in fetch_chunk(
            cursor,
            chunk_end,
        ):
            by_time[
                candle["time"]
            ] = candle

        cursor = chunk_end
        time.sleep(0.02)

    candles = list(
        by_time.values()
    )

    candles.sort(
        key=lambda row:
            row["time"]
    )

    return candles


# ============================================================
# INDICATORS / SIGNAL CACHE
# ============================================================

def atr14(candles):
    n = len(candles)

    trs = [None] * n
    atr = [None] * n

    for i in range(n):
        high = candles[i]["high"]
        low = candles[i]["low"]

        if i == 0:
            trs[i] = high - low
        else:
            previous_close = candles[
                i - 1
            ]["close"]

            trs[i] = max(
                high - low,
                abs(high - previous_close),
                abs(low - previous_close),
            )

    if n < 14:
        return atr

    atr[13] = (
        sum(trs[:14])
        / 14.0
    )

    for i in range(14, n):
        atr[i] = (
            atr[i - 1] * 13.0
            + trs[i]
        ) / 14.0

    return atr


def bullish_engulfing(candles, i):
    if i < 1:
        return False

    previous = candles[i - 1]
    current = candles[i]

    return (
        previous["close"] < previous["open"]
        and
        current["close"] > current["open"]
        and
        current["open"] <= previous["close"]
        and
        current["close"] >= previous["open"]
    )


def build_signal_cache(
    candles,
    atr,
):
    signals = []

    max_lookback = max(
        LOOKBACKS
    )

    for i in range(
        max(
            14,
            max_lookback,
        ),
        len(candles),
    ):
        if not bullish_engulfing(
            candles,
            i,
        ):
            continue

        current = candles[i]
        previous = candles[i - 1]
        atr_value = atr[i]

        if (
            atr_value is None
            or atr_value <= 0
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

        body_atr = (
            body / atr_value
        )

        structure = {}

        for lookback in LOOKBACKS:
            previous_low = min(
                candle["low"]
                for candle in candles[
                    i - lookback:i
                ]
            )

            structure[
                lookback
            ] = (
                abs(
                    current["low"]
                    - previous_low
                )
                / atr_value
            )

        ny_time = (
            current["time"]
            .astimezone(NY)
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
            "structure_distance_atr":
                structure,
            "ny_hour":
                ny_time.hour,
            "ny_weekday":
                ny_time.weekday(),
        })

    return signals


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
        + reward_risk
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
            candle["low"] <= stop
        )

        hit_target = (
            candle["high"] >= target
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

            if distance_high < distance_low:
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
        * len(REWARD_RISKS)
        * len(COST_PIPS_GRID)
    )

    done = 0

    for signal in signals:
        signal_index = signal[
            "signal_index"
        ]

        for rr in REWARD_RISKS:
            for cost in COST_PIPS_GRID:
                done += 1

                if done % 1000 == 0:
                    STATUS.update({
                        "state": "precomputing",
                        "message": (
                            "Caching outcomes "
                            f"{done}/{total}"
                        ),
                        "outcomes_done": done,
                        "outcomes_total": total,
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
# CONFIGS
# ============================================================

def build_configs():
    configs = []
    counter = 0

    for time_filter in TIME_FILTERS:
        for br in BODY_RATIOS:
            for lookback in LOOKBACKS:
                for distance in DISTANCES:
                    for rr in REWARD_RISKS:
                        counter += 1

                        configs.append({
                            "label":
                                (
                                    f"L{counter:04d}_"
                                    f"{time_filter['name']}_"
                                    f"BR{br:.2f}_"
                                    f"S{lookback}_"
                                    f"D{distance:.3f}_"
                                    f"RR{rr:.2f}"
                                ),
                            "minimum_body_ratio":
                                br,
                            "minimum_body_atr":
                                BODY_ATR_FIXED,
                            "structure_lookback":
                                lookback,
                            "maximum_distance_atr":
                                distance,
                            "excluded_weekdays":
                                set(
                                    time_filter[
                                        "excluded_weekdays"
                                    ]
                                ),
                            "excluded_hours":
                                set(
                                    time_filter[
                                        "excluded_hours"
                                    ]
                                ),
                            "time_filter_name":
                                time_filter[
                                    "name"
                                ],
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

    lookback = config[
        "structure_lookback"
    ]

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
        signal["ny_weekday"]
        in
        config[
            "excluded_weekdays"
        ]
    ):
        return False

    if (
        signal["ny_hour"]
        in
        config[
            "excluded_hours"
        ]
    ):
        return False

    return True


def run_config_cached(
    signals,
    cache,
    config,
    cost_pips,
    start=None,
    end=None,
):
    candidates = [
        signal
        for signal in signals
        if (
            signal_passes(
                signal,
                config,
            )
            and
            (
                start is None
                or signal["time"] >= start
            )
            and
            (
                end is None
                or signal["time"] < end
            )
        )
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
        "trades": len(results),
        "winners": len(winners),
        "losers": len(losers),
        "win_rate":
            (
                len(winners)
                / len(results)
                * 100.0
                if results
                else 0.0
            ),
        "profit_factor": pf,
        "total_r": total_r,
        "expectancy_r":
            (
                total_r
                / len(results)
                if results
                else 0.0
            ),
        "max_drawdown_r": max_dd,
        "longest_loss_streak": longest,
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
        "time_filter":
            config.get(
                "time_filter_name",
                "REFERENCE",
            ),
        "cost_pips":
            cost,
        "minimum_body_ratio":
            config["minimum_body_ratio"],
        "minimum_body_atr":
            config["minimum_body_atr"],
        "structure_lookback":
            config["structure_lookback"],
        "maximum_distance_atr":
            config["maximum_distance_atr"],
        "excluded_weekdays":
            ",".join(
                str(day)
                for day in sorted(
                    config[
                        "excluded_weekdays"
                    ]
                )
            ),
        "excluded_hours":
            ",".join(
                str(hour)
                for hour in sorted(
                    config[
                        "excluded_hours"
                    ]
                )
            ),
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
            stats[
                "longest_loss_streak"
            ],
    }


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
    finalists,
    windows,
):
    rows = []

    for rank, config in enumerate(
        finalists,
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
        + ordered[
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
                / len(rows)
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
# OVERLAP / EXCLUSIVE
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


def compare_against_reference(
    signals,
    cache,
    reference,
    finalist,
    reference_name,
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
        trade_key(trade)
        for trade in reference_trades
    }

    finalist_keys = {
        trade_key(trade)
        for trade in finalist_trades
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

    subsets = [
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
                trade
                for trade in finalist_trades
                if trade_key(trade)
                in shared
            ],
        ),
        (
            "FINALIST_ADDED",
            [
                trade
                for trade in finalist_trades
                if trade_key(trade)
                in added
            ],
        ),
        (
            "REFERENCE_REMOVED",
            [
                trade
                for trade in reference_trades
                if trade_key(trade)
                in removed
            ],
        ),
    ]

    rows = []

    for subset_name, subset in subsets:
        stats = stats_from_trades(
            subset
        )

        rows.append({
            "reference":
                reference_name,
            "finalist":
                finalist["label"],
            "subset":
                subset_name,
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
        candles = fetch_full_history(
            RESEARCH_FROM,
            RESEARCH_TO,
        )

        if len(candles) < 1000:
            raise RuntimeError(
                "Too few M15 candles returned"
            )

        STATUS.update({
            "state": "precomputing",
            "message":
                "Building ATR and signal cache",
            "candles":
                len(candles),
        })

        atr = atr14(
            candles
        )

        signals = build_signal_cache(
            candles,
            atr,
        )

        STATUS.update({
            "state": "precomputing",
            "message":
                "Caching reusable outcomes",
            "engulfing_signals":
                len(signals),
        })

        cache = build_outcome_cache(
            candles,
            signals,
        )

        configs = build_configs()

        STATUS.update({
            "state": "calculating",
            "message":
                "Running final local grid",
            "configs":
                len(configs),
        })

        summary_rows = []

        for number, config in enumerate(
            configs,
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
                STATUS.update({
                    "state": "calculating",
                    "message": (
                        f"Evaluated {number}/"
                        f"{len(configs)} configs"
                    ),
                    "config":
                        number,
                    "configs_total":
                        len(configs),
                })

        # Add reference strategies explicitly.
        reference_configs = [
            OLD_INCUMBENT,
            CURRENT_LEAD,
        ]

        for ref in reference_configs:
            for cost in COST_PIPS_GRID:
                trades = run_config_cached(
                    signals,
                    cache,
                    ref,
                    cost,
                )

                summary_rows.append(
                    result_row(
                        ref,
                        cost,
                        trades,
                    )
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
                    float(
                        row[
                            "cost_pips"
                        ]
                    )
                    - PRIMARY_COST_PIPS
                ) < 1e-12
                and
                int(
                    row["trades"]
                ) >= 80
            )
        ]

        primary.sort(
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
                int(
                    row["trades"]
                ),
            ),
            reverse=True,
        )

        top_rows = primary[:30]

        write_csv(
            OUTPUT_TOP,
            top_rows,
        )

        config_map = {
            config["label"]:
                config
            for config in configs
        }

        config_map[
            OLD_INCUMBENT["label"]
        ] = OLD_INCUMBENT

        config_map[
            CURRENT_LEAD["label"]
        ] = CURRENT_LEAD

        finalists = [
            config_map[
                row["candidate"]
            ]
            for row in top_rows[:12]
        ]

        era_rows = validation_rows(
            signals,
            cache,
            finalists,
            era_windows(),
        )

        devval_rows = validation_rows(
            signals,
            cache,
            finalists,
            devval_windows(),
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
                        + min_devval * 2.0
                        + min_recent * 2.0
                        + float(
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
            config_map[
                row["candidate"]
            ]
            for row in robust[:5]
        ]

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

                summary = rolling_summary(
                    rows
                )

                summary[
                    "minimum_body_ratio"
                ] = config[
                    "minimum_body_ratio"
                ]

                summary[
                    "structure_lookback"
                ] = config[
                    "structure_lookback"
                ]

                summary[
                    "maximum_distance_atr"
                ] = config[
                    "maximum_distance_atr"
                ]

                summary[
                    "excluded_weekdays"
                ] = ",".join(
                    str(day)
                    for day in sorted(
                        config[
                            "excluded_weekdays"
                        ]
                    )
                )

                summary[
                    "excluded_hours"
                ] = ",".join(
                    str(hour)
                    for hour in sorted(
                        config[
                            "excluded_hours"
                        ]
                    )
                )

                summary[
                    "reward_risk"
                ] = config[
                    "reward_risk"
                ]

                rolling_summary_rows.append(
                    summary
                )

        write_csv(
            OUTPUT_ROLLING,
            rolling_rows,
        )

        write_csv(
            OUTPUT_ROLLING_SUMMARY,
            rolling_summary_rows,
        )

        overlap_rows = []

        for finalist in robust_finalists:
            overlap_rows.extend(
                compare_against_reference(
                    signals,
                    cache,
                    OLD_INCUMBENT,
                    finalist,
                    "OLD_INCUMBENT",
                )
            )

            overlap_rows.extend(
                compare_against_reference(
                    signals,
                    cache,
                    CURRENT_LEAD,
                    finalist,
                    "CURRENT_LEAD_EX_TUE_H07",
                )
            )

        write_csv(
            OUTPUT_OVERLAP,
            overlap_rows,
        )

        best = (
            robust_finalists[0]
            if robust_finalists
            else CURRENT_LEAD
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
            "state": "complete",
            "message":
                "EUR/USD M15 long final local confirmation complete",
            "candles":
                len(candles),
            "engulfing_signals":
                len(signals),
            "configs":
                len(configs),
            "primary_cost_pips":
                PRIMARY_COST_PIPS,
            "robust_ranking":
                robust[:10],
            "selected_best":
                best,
            "outputs": {
                "summary":
                    OUTPUT_SUMMARY,
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
                "overlap":
                    OUTPUT_OVERLAP,
                "best_trades":
                    OUTPUT_BEST_TRADES,
            },
        })

        print()
        print("=" * 100)
        print(
            "EUR/USD M15 LONG FINAL LOCAL CONFIRMATION COMPLETE"
        )
        print("=" * 100)
        print(
            "Configs:",
            len(configs),
        )
        print(
            "Selected best:",
            best,
        )
        print(
            "Robust ranking:"
        )

        for row in robust[:10]:
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
            "EURUSD M15 Long Final Local Confirmation",
        "status":
            STATUS["state"],
        "instrument":
            INSTRUMENT,
        "timeframe":
            GRANULARITY,
        "side":
            "BUY",
        "orders_supported":
            False,
        "trading_enabled":
            False,
        "routes": [
            "/m15-long-final-local/status",
            "/m15-long-final-local/summary",
            "/m15-long-final-local/top",
            "/m15-long-final-local/eras",
            "/m15-long-final-local/dev-validation",
            "/m15-long-final-local/recent",
            "/m15-long-final-local/rolling",
            "/m15-long-final-local/rolling-summary",
            "/m15-long-final-local/overlap",
            "/m15-long-final-local/best-trades",
        ],
    })


@app.route(
    "/m15-long-final-local/status"
)
def route_status():
    return jsonify(
        STATUS
    )


@app.route(
    "/m15-long-final-local/summary"
)
def route_summary():
    return download_file(
        OUTPUT_SUMMARY
    )


@app.route(
    "/m15-long-final-local/top"
)
def route_top():
    return download_file(
        OUTPUT_TOP
    )


@app.route(
    "/m15-long-final-local/eras"
)
def route_eras():
    return download_file(
        OUTPUT_ERAS
    )


@app.route(
    "/m15-long-final-local/dev-validation"
)
def route_devval():
    return download_file(
        OUTPUT_DEVVAL
    )


@app.route(
    "/m15-long-final-local/recent"
)
def route_recent():
    return download_file(
        OUTPUT_RECENT
    )


@app.route(
    "/m15-long-final-local/rolling"
)
def route_rolling():
    return download_file(
        OUTPUT_ROLLING
    )


@app.route(
    "/m15-long-final-local/rolling-summary"
)
def route_rolling_summary():
    return download_file(
        OUTPUT_ROLLING_SUMMARY
    )


@app.route(
    "/m15-long-final-local/overlap"
)
def route_overlap():
    return download_file(
        OUTPUT_OVERLAP
    )


@app.route(
    "/m15-long-final-local/best-trades"
)
def route_best_trades():
    return download_file(
        OUTPUT_BEST_TRADES
    )


if __name__ == "__main__":
    research_thread = threading.Thread(
        target=run_research,
        name="eurusd-m15-long-final-local",
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
