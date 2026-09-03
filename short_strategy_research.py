import os
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# EUR/GBP LONG - FINAL VALIDATION
#
# RESEARCH ONLY - NEVER SUBMITS ORDERS.
#
# VALIDATES:
#
# CURRENT LIVE CONTROL
# --------------------
# body ratio >= 1.00
# strong close >= 0.75
# structure 20 / 0.20 ATR14
# prior completed daily close > EMA150
# prior completed daily EMA20 > EMA150
# London 08:00-16:59
# exclude Thursday + Friday
# RR 3.00
#
# CANDIDATE B
# -----------
# body ratio >= 1.00
# body >= 1.10 ATR14
# range >= 1.40 ATR14
# structure 30 / 0.075 ATR14
# prior completed daily close > EMA200
# prior completed daily EMA20 > EMA150
# no strong-close filter
# all hours
# all weekdays
# RR 3.00
#
# SIMPLIFIED CHALLENGER
# ---------------------
# same as Candidate B except:
# NO daily EMA alignment requirement
#
# Outputs:
# - summary comparison
# - yearly stats
# - rolling 3y stats
# - exact trade list
# - overlap/exclusive-trade analysis
#
# ============================================================
# LOCKED EXECUTION CONVENTIONS
#
# OANDA midpoint H1.
#
# Bullish engulfing:
#   previous bearish
#   current bullish
#   current open <= previous close
#   current close >= previous open
#
# ATR14 = Wilder/RMA, SMA-seeded
# tick = 0.00001
#
# entry reference = signal close
# adverse fill = signal close + 5 ticks
# stop = signal low - 10 ticks
# target = signal close + reference risk * 3.00
#
# actual R =
#   (exit - backtest_entry) /
#   (backtest_entry - stop)
#
# pyramiding 0
# signal on exact exit candle allowed
# exits start signal_index + 1
#
# same-bar tie:
#   compare open->high vs open->low
#   high closer => target first
#   else stop first
#
# Daily:
#   dailyAlignment=17
#   alignmentTimezone=America/New_York
#   previous completed daily candle only
#
# Research:
#   2002-05-06 20:00 UTC -> current completed UTC hour
# ============================================================


app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_URL = "https://api-fxtrade.oanda.com"

INSTRUMENT = "EUR_GBP"

TICK_SIZE = 0.00001
STOP_BUFFER_TICKS = 10
BACKTEST_SLIPPAGE_TICKS = 5
REWARD_RISK = 3.00
MINIMUM_BODY_RATIO = 1.00

ATR_LENGTH = 14

NY_TZ = ZoneInfo("America/New_York")
LONDON_TZ = ZoneInfo("Europe/London")

DAILY_ALIGNMENT_HOUR = 17
DAILY_ALIGNMENT_TIMEZONE = "America/New_York"

RESEARCH_FROM = datetime(
    2002, 5, 6, 20, 0,
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

H1_CHUNK_DAYS = 180
D_CHUNK_DAYS = 1500

H1_WARMUP_DAYS = 200
D_WARMUP_DAYS = 2500

OUTPUT_SUMMARY = (
    "eurgbp_long_final_validation_summary.csv"
)
OUTPUT_YEARLY = (
    "eurgbp_long_final_validation_yearly.csv"
)
OUTPUT_ROLLING = (
    "eurgbp_long_final_validation_rolling3y.csv"
)
OUTPUT_TRADES = (
    "eurgbp_long_final_validation_trades.csv"
)
OUTPUT_OVERLAP = (
    "eurgbp_long_final_validation_overlap.csv"
)


# ============================================================
# ERAS
# ============================================================

ERAS = [
    (
        "2002_2009",
        RESEARCH_FROM,
        datetime(
            2010, 1, 1,
            tzinfo=timezone.utc,
        ),
    ),
    (
        "2010_2017",
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
        "2018_2023",
        datetime(
            2018, 1, 1,
            tzinfo=timezone.utc,
        ),
        datetime(
            2024, 1, 1,
            tzinfo=timezone.utc,
        ),
    ),
    (
        "2024_present",
        datetime(
            2024, 1, 1,
            tzinfo=timezone.utc,
        ),
        None,
    ),
]


STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "service": (
        "EUR/GBP Long Final Validation"
    ),
    "instrument": INSTRUMENT,
    "orders_supported": False,
    "trading_enabled": False,
}


# ============================================================
# OANDA
# ============================================================

def headers():
    if not OANDA_TOKEN:
        raise RuntimeError(
            "OANDA_TOKEN is not configured"
        )

    return {
        "Authorization": (
            f"Bearer {OANDA_TOKEN}"
        )
    }


def iso_utc(dt):
    return (
        dt
        .astimezone(timezone.utc)
        .isoformat()
        .replace(
            "+00:00",
            "Z",
        )
    )


def oanda_get(
    path,
    params,
):
    response = requests.get(
        OANDA_URL + path,
        headers=headers(),
        params=params,
        timeout=30,
    )

    if not response.ok:
        raise RuntimeError(
            f"OANDA {response.status_code}: "
            f"{response.text[:500]}"
        )

    return response.json()


def parse_candle(raw):
    if not raw.get(
        "complete",
        False,
    ):
        return None

    mid = raw.get("mid")

    if not mid:
        return None

    return {
        "time": datetime.fromisoformat(
            raw["time"].replace(
                "Z",
                "+00:00",
            )
        ),
        "open": float(mid["o"]),
        "high": float(mid["h"]),
        "low": float(mid["l"]),
        "close": float(mid["c"]),
    }


def fetch_range(
    granularity,
    start,
    end,
):
    params = {
        "price": "M",
        "granularity": granularity,
        "from": iso_utc(start),
        "to": iso_utc(end),
        "smooth": "false",
        "includeFirst": "true",
        "dailyAlignment": DAILY_ALIGNMENT_HOUR,
        "alignmentTimezone": DAILY_ALIGNMENT_TIMEZONE,
    }

    data = oanda_get(
        f"/v3/instruments/{INSTRUMENT}/candles",
        params,
    )

    candles = []

    for raw in data.get(
        "candles",
        [],
    ):
        candle = parse_candle(
            raw
        )

        if candle is not None:
            candles.append(
                candle
            )

    return candles


def fetch_chunked(
    granularity,
    start,
    end,
    chunk_days,
):
    by_time = {}
    cursor = start

    while cursor < end:
        chunk_end = min(
            cursor
            + timedelta(
                days=chunk_days
            ),
            end,
        )

        print(
            f"Fetching {granularity}: "
            f"{cursor.date()} -> {chunk_end.date()}",
            flush=True,
        )

        chunk = fetch_range(
            granularity,
            cursor,
            chunk_end,
        )

        for candle in chunk:
            by_time[
                candle["time"]
            ] = candle

        cursor = chunk_end

    candles = list(
        by_time.values()
    )

    candles.sort(
        key=lambda x: x["time"]
    )

    return candles


# ============================================================
# INDICATORS
# ============================================================

def true_ranges(candles):
    result = []

    for index, candle in enumerate(
        candles
    ):
        if index == 0:
            tr = (
                candle["high"]
                - candle["low"]
            )
        else:
            prev_close = (
                candles[
                    index - 1
                ]["close"]
            )

            tr = max(
                candle["high"]
                - candle["low"],
                abs(
                    candle["high"]
                    - prev_close
                ),
                abs(
                    candle["low"]
                    - prev_close
                ),
            )

        result.append(
            tr
        )

    return result


def rma_series(
    values,
    length,
):
    result = [
        None
    ] * len(values)

    if len(values) < length:
        return result

    initial = (
        sum(
            values[:length]
        )
        / length
    )

    result[
        length - 1
    ] = initial

    previous = initial

    for index in range(
        length,
        len(values),
    ):
        current = (
            (
                previous
                * (
                    length - 1
                )
            )
            + values[index]
        ) / length

        result[index] = current
        previous = current

    return result


def atr_series(
    candles,
    length,
):
    return rma_series(
        true_ranges(
            candles
        ),
        length,
    )


def ema_series(
    values,
    length,
):
    result = [
        None
    ] * len(values)

    if len(values) < length:
        return result

    initial = (
        sum(
            values[:length]
        )
        / length
    )

    result[
        length - 1
    ] = initial

    multiplier = (
        2.0
        / (
            length + 1.0
        )
    )

    previous = initial

    for index in range(
        length,
        len(values),
    ):
        current = (
            (
                values[index]
                - previous
            )
            * multiplier
            + previous
        )

        result[index] = current
        previous = current

    return result


# ============================================================
# DAILY STATE
# ============================================================

def current_daily_start(
    timestamp_utc
):
    ny_time = (
        timestamp_utc
        .astimezone(
            NY_TZ
        )
    )

    candidate = ny_time.replace(
        hour=DAILY_ALIGNMENT_HOUR,
        minute=0,
        second=0,
        microsecond=0,
    )

    if ny_time < candidate:
        candidate = (
            candidate
            - timedelta(
                days=1
            )
        )

    return candidate.astimezone(
        timezone.utc
    )


def prepare_daily(
    daily
):
    closes = [
        candle["close"]
        for candle
        in daily
    ]

    ema_lengths = [
        20,
        150,
        200,
    ]

    ema_map = {
        length: ema_series(
            closes,
            length,
        )
        for length
        in ema_lengths
    }

    rows = []

    for index, candle in enumerate(
        daily
    ):
        rows.append({
            "time":
                candle["time"],
            "close":
                candle["close"],
            "ema20":
                ema_map[20][index],
            "ema150":
                ema_map[150][index],
            "ema200":
                ema_map[200][index],
        })

    return rows


def previous_completed_daily(
    signal_time,
    daily_state,
):
    session_start = (
        current_daily_start(
            signal_time
        )
    )

    selected = None

    for row in daily_state:
        if (
            row["time"]
            < session_start
        ):
            selected = row
        else:
            break

    return selected


# ============================================================
# RAW SIGNAL PRECOMPUTE
# ============================================================

def build_raw_candidates(
    h1,
    atr,
    daily_state,
):
    candidates = []

    start_index = max(
        ATR_LENGTH,
        30,
    )

    for index in range(
        start_index,
        len(h1),
    ):
        signal = h1[index]

        if signal["time"] < RESEARCH_FROM:
            continue

        if signal["time"] >= RESEARCH_TO:
            break

        previous = h1[
            index - 1
        ]

        current_atr = atr[
            index
        ]

        if (
            current_atr is None
            or current_atr <= 0
        ):
            continue

        previous_body = abs(
            previous["close"]
            - previous["open"]
        )

        current_body = abs(
            signal["close"]
            - signal["open"]
        )

        signal_range = (
            signal["high"]
            - signal["low"]
        )

        if (
            previous_body <= 0
            or current_body <= 0
            or signal_range <= 0
        ):
            continue

        bullish_engulfing = (
            previous["close"]
            < previous["open"]
            and signal["close"]
            > signal["open"]
            and signal["open"]
            <= previous["close"]
            and signal["close"]
            >= previous["open"]
        )

        if not bullish_engulfing:
            continue

        body_ratio = (
            current_body
            / previous_body
        )

        if body_ratio < 1.00:
            continue

        body_atr = (
            current_body
            / current_atr
        )

        range_atr = (
            signal_range
            / current_atr
        )

        close_location = (
            signal["close"]
            - signal["low"]
        ) / signal_range

        previous_low20 = min(
            candle["low"]
            for candle
            in h1[
                index - 20:
                index
            ]
        )

        previous_low30 = min(
            candle["low"]
            for candle
            in h1[
                index - 30:
                index
            ]
        )

        distance20 = (
            signal["low"]
            - previous_low20
        ) / current_atr

        distance30 = (
            signal["low"]
            - previous_low30
        ) / current_atr

        daily = (
            previous_completed_daily(
                signal["time"],
                daily_state,
            )
        )

        london = (
            signal["time"]
            .astimezone(
                LONDON_TZ
            )
        )

        candidates.append({
            "index": index,
            "time": signal["time"],
            "body_atr": body_atr,
            "range_atr": range_atr,
            "close_location": (
                close_location
            ),
            "distance20": (
                distance20
            ),
            "distance30": (
                distance30
            ),
            "daily": daily,
            "london_hour": (
                london.hour
            ),
            "london_weekday": (
                london.weekday()
            ),
        })

    return candidates


# ============================================================
# STRATEGY FILTERS
# ============================================================

def filter_control(
    candidates,
):
    eligible = []

    for s in candidates:
        if (
            s["close_location"]
            < 0.75
        ):
            continue

        if (
            s["distance20"]
            > 0.20
        ):
            continue

        daily = s["daily"]

        if daily is None:
            continue

        if (
            daily["ema20"] is None
            or daily["ema150"] is None
        ):
            continue

        if not (
            daily["close"]
            > daily["ema150"]
        ):
            continue

        if not (
            daily["ema20"]
            > daily["ema150"]
        ):
            continue

        if not (
            s["london_hour"] >= 8
            and s["london_hour"] < 17
        ):
            continue

        if (
            s["london_weekday"]
            in {
                3,
                4,
            }
        ):
            continue

        eligible.append(
            s
        )

    return eligible


def filter_candidate_b(
    candidates,
):
    eligible = []

    for s in candidates:
        if (
            s["body_atr"]
            < 1.10
        ):
            continue

        if (
            s["range_atr"]
            < 1.40
        ):
            continue

        if (
            s["distance30"]
            > 0.075
        ):
            continue

        daily = s["daily"]

        if daily is None:
            continue

        if (
            daily["ema20"] is None
            or daily["ema150"] is None
            or daily["ema200"] is None
        ):
            continue

        if not (
            daily["close"]
            > daily["ema200"]
        ):
            continue

        if not (
            daily["ema20"]
            > daily["ema150"]
        ):
            continue

        eligible.append(
            s
        )

    return eligible


def filter_simplified(
    candidates,
):
    eligible = []

    for s in candidates:
        if (
            s["body_atr"]
            < 1.10
        ):
            continue

        if (
            s["range_atr"]
            < 1.40
        ):
            continue

        if (
            s["distance30"]
            > 0.075
        ):
            continue

        daily = s["daily"]

        if daily is None:
            continue

        if (
            daily["ema200"]
            is None
        ):
            continue

        if not (
            daily["close"]
            > daily["ema200"]
        ):
            continue

        eligible.append(
            s
        )

    return eligible


# ============================================================
# TRADE SIMULATION
# ============================================================

EXIT_CACHE = {}


def calculate_trade_exit(
    h1,
    signal_index,
):
    if signal_index in EXIT_CACHE:
        return EXIT_CACHE[
            signal_index
        ]

    signal = h1[
        signal_index
    ]

    reference_entry = (
        signal["close"]
    )

    backtest_entry = (
        reference_entry
        + BACKTEST_SLIPPAGE_TICKS
        * TICK_SIZE
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
        result = {
            "signal_index": (
                signal_index
            ),
            "signal_time": (
                signal["time"]
            ),
            "exit_index": None,
            "exit_time": None,
            "result_r": None,
        }

        EXIT_CACHE[
            signal_index
        ] = result

        return result

    target = (
        reference_entry
        + reference_risk
        * REWARD_RISK
    )

    actual_risk = (
        backtest_entry
        - stop
    )

    if actual_risk <= 0:
        result = {
            "signal_index": (
                signal_index
            ),
            "signal_time": (
                signal["time"]
            ),
            "exit_index": None,
            "exit_time": None,
            "result_r": None,
        }

        EXIT_CACHE[
            signal_index
        ] = result

        return result

    for index in range(
        signal_index + 1,
        len(h1),
    ):
        candle = h1[
            index
        ]

        if (
            candle["time"]
            >= RESEARCH_TO
        ):
            break

        stop_hit = (
            candle["low"]
            <= stop
        )

        target_hit = (
            candle["high"]
            >= target
        )

        if not (
            stop_hit
            or target_hit
        ):
            continue

        if (
            stop_hit
            and target_hit
        ):
            distance_to_high = abs(
                candle["high"]
                - candle["open"]
            )

            distance_to_low = abs(
                candle["open"]
                - candle["low"]
            )

            if (
                distance_to_high
                < distance_to_low
            ):
                exit_price = target
            else:
                exit_price = stop

        elif target_hit:
            exit_price = target

        else:
            exit_price = stop

        result = {
            "signal_index": (
                signal_index
            ),
            "signal_time": (
                signal["time"]
            ),
            "exit_index": index,
            "exit_time": (
                candle["time"]
            ),
            "entry_reference": (
                reference_entry
            ),
            "backtest_entry": (
                backtest_entry
            ),
            "stop": stop,
            "target": target,
            "exit_price": (
                exit_price
            ),
            "result_r": (
                exit_price
                - backtest_entry
            ) / actual_risk,
        }

        EXIT_CACHE[
            signal_index
        ] = result

        return result

    result = {
        "signal_index": (
            signal_index
        ),
        "signal_time": (
            signal["time"]
        ),
        "exit_index": None,
        "exit_time": None,
        "result_r": None,
    }

    EXIT_CACHE[
        signal_index
    ] = result

    return result


def simulate_variant(
    h1,
    eligible,
):
    trades = []
    ignored = 0
    position_exit_index = -1

    for signal in eligible:
        signal_index = (
            signal["index"]
        )

        if (
            signal_index
            < position_exit_index
        ):
            ignored += 1
            continue

        trade = (
            calculate_trade_exit(
                h1,
                signal_index,
            )
        )

        if (
            trade["result_r"]
            is None
        ):
            break

        trades.append(
            trade
        )

        position_exit_index = (
            trade["exit_index"]
        )

    return (
        trades,
        ignored,
    )


# ============================================================
# STATS
# ============================================================

def stats_for_trades(
    trades,
    start=None,
    end=None,
):
    selected = []

    for trade in trades:
        t = trade[
            "signal_time"
        ]

        if (
            start is not None
            and t < start
        ):
            continue

        if (
            end is not None
            and t >= end
        ):
            continue

        selected.append(
            trade
        )

    if not selected:
        return {
            "trades": 0,
            "winners": 0,
            "losers": 0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "total_r": 0.0,
            "expectancy_r": 0.0,
            "max_drawdown_r": 0.0,
            "longest_loss_streak": 0,
        }

    results = [
        trade["result_r"]
        for trade
        in selected
    ]

    winners = [
        r
        for r in results
        if r > 0
    ]

    losers = [
        r
        for r in results
        if r < 0
    ]

    gross_profit = sum(
        winners
    )

    gross_loss = abs(
        sum(
            losers
        )
    )

    if gross_loss > 0:
        pf = (
            gross_profit
            / gross_loss
        )
    elif gross_profit > 0:
        pf = 999.0
    else:
        pf = 0.0

    total_r = sum(
        results
    )

    equity = 0.0
    peak = 0.0
    max_dd = 0.0

    current_streak = 0
    longest_streak = 0

    for r in results:
        equity += r

        peak = max(
            peak,
            equity,
        )

        max_dd = min(
            max_dd,
            equity - peak,
        )

        if r < 0:
            current_streak += 1

            longest_streak = max(
                longest_streak,
                current_streak,
            )
        else:
            current_streak = 0

    return {
        "trades": (
            len(
                results
            )
        ),
        "winners": (
            len(
                winners
            )
        ),
        "losers": (
            len(
                losers
            )
        ),
        "win_rate": round(
            len(winners)
            / len(results)
            * 100.0,
            2,
        ),
        "profit_factor": round(
            pf,
            3,
        ),
        "total_r": round(
            total_r,
            2,
        ),
        "expectancy_r": round(
            total_r
            / len(results),
            3,
        ),
        "max_drawdown_r": round(
            max_dd,
            2,
        ),
        "longest_loss_streak": (
            longest_streak
        ),
    }


def subtract_years_safe(
    dt,
    years,
):
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


# ============================================================
# OUTPUT BUILDERS
# ============================================================

def build_summary_row(
    name,
    trades,
    ignored,
):
    years = (
        RESEARCH_TO
        - RESEARCH_FROM
    ).total_seconds() / (
        365.2425
        * 86400
    )

    full = stats_for_trades(
        trades
    )

    row = {
        "strategy": name,
        "research_from": (
            RESEARCH_FROM.isoformat()
        ),
        "research_to": (
            RESEARCH_TO.isoformat()
        ),
        "trades": (
            full["trades"]
        ),
        "winners": (
            full["winners"]
        ),
        "losers": (
            full["losers"]
        ),
        "win_rate": (
            full["win_rate"]
        ),
        "profit_factor": (
            full["profit_factor"]
        ),
        "total_r": (
            full["total_r"]
        ),
        "expectancy_r": (
            full["expectancy_r"]
        ),
        "max_drawdown_r": (
            full["max_drawdown_r"]
        ),
        "longest_loss_streak": (
            full[
                "longest_loss_streak"
            ]
        ),
        "ignored_due_to_open_trade": (
            ignored
        ),
        "trades_per_year": round(
            full["trades"]
            / years,
            3,
        ),
        "annual_r_linear": round(
            full["total_r"]
            / years,
            3,
        ),
    }

    minimum_era_pf = None
    profitable_eras = 0

    for (
        era_name,
        era_start,
        era_end,
    ) in ERAS:
        stats = stats_for_trades(
            trades,
            era_start,
            (
                RESEARCH_TO
                if era_end is None
                else min(
                    era_end,
                    RESEARCH_TO,
                )
            ),
        )

        row[
            f"{era_name}_trades"
        ] = stats["trades"]

        row[
            f"{era_name}_pf"
        ] = stats[
            "profit_factor"
        ]

        row[
            f"{era_name}_r"
        ] = stats["total_r"]

        row[
            f"{era_name}_expectancy"
        ] = stats[
            "expectancy_r"
        ]

        if stats["trades"] >= 5:
            if minimum_era_pf is None:
                minimum_era_pf = (
                    stats[
                        "profit_factor"
                    ]
                )
            else:
                minimum_era_pf = min(
                    minimum_era_pf,
                    stats[
                        "profit_factor"
                    ],
                )

            if stats["total_r"] > 0:
                profitable_eras += 1

    row[
        "minimum_era_pf_5_plus"
    ] = minimum_era_pf

    row[
        "profitable_eras"
    ] = profitable_eras

    for years_back in [
        2,
        5,
        10,
    ]:
        start = subtract_years_safe(
            RESEARCH_TO,
            years_back,
        )

        stats = stats_for_trades(
            trades,
            start,
            RESEARCH_TO,
        )

        row[
            f"last_{years_back}y_trades"
        ] = stats["trades"]

        row[
            f"last_{years_back}y_pf"
        ] = stats[
            "profit_factor"
        ]

        row[
            f"last_{years_back}y_r"
        ] = stats["total_r"]

        row[
            f"last_{years_back}y_expectancy"
        ] = stats[
            "expectancy_r"
        ]

    return row


def build_yearly_rows(
    name,
    trades,
):
    rows = []

    for year in range(
        RESEARCH_FROM.year,
        RESEARCH_TO.year + 1,
    ):
        start = max(
            RESEARCH_FROM,
            datetime(
                year,
                1,
                1,
                tzinfo=timezone.utc,
            ),
        )

        end = min(
            RESEARCH_TO,
            datetime(
                year + 1,
                1,
                1,
                tzinfo=timezone.utc,
            ),
        )

        if start >= end:
            continue

        stats = stats_for_trades(
            trades,
            start,
            end,
        )

        rows.append({
            "strategy": name,
            "year": year,
            **stats,
        })

    return rows


def build_rolling_rows(
    name,
    trades,
):
    rows = []

    for start_year in range(
        2002,
        RESEARCH_TO.year - 1,
    ):
        start = max(
            RESEARCH_FROM,
            datetime(
                start_year,
                1,
                1,
                tzinfo=timezone.utc,
            ),
        )

        end = min(
            RESEARCH_TO,
            datetime(
                start_year + 3,
                1,
                1,
                tzinfo=timezone.utc,
            ),
        )

        if start >= end:
            continue

        stats = stats_for_trades(
            trades,
            start,
            end,
        )

        rows.append({
            "strategy": name,
            "window": (
                f"{start_year}_"
                f"{start_year + 2}"
            ),
            "start": start.isoformat(),
            "end": end.isoformat(),
            **stats,
        })

    return rows


def build_trade_rows(
    name,
    trades,
):
    rows = []

    for trade in trades:
        rows.append({
            "strategy": name,
            "signal_time": (
                trade[
                    "signal_time"
                ].isoformat()
            ),
            "exit_time": (
                trade[
                    "exit_time"
                ].isoformat()
            ),
            "entry_reference": (
                round(
                    trade[
                        "entry_reference"
                    ],
                    6,
                )
            ),
            "backtest_entry": (
                round(
                    trade[
                        "backtest_entry"
                    ],
                    6,
                )
            ),
            "stop": round(
                trade["stop"],
                6,
            ),
            "target": round(
                trade["target"],
                6,
            ),
            "exit_price": round(
                trade[
                    "exit_price"
                ],
                6,
            ),
            "result_r": round(
                trade[
                    "result_r"
                ],
                6,
            ),
        })

    return rows


def overlap_stats(
    name_a,
    trades_a,
    name_b,
    trades_b,
):
    map_a = {
        trade[
            "signal_time"
        ]: trade
        for trade
        in trades_a
    }

    map_b = {
        trade[
            "signal_time"
        ]: trade
        for trade
        in trades_b
    }

    keys_a = set(
        map_a.keys()
    )

    keys_b = set(
        map_b.keys()
    )

    shared = sorted(
        keys_a
        & keys_b
    )

    only_a = sorted(
        keys_a
        - keys_b
    )

    only_b = sorted(
        keys_b
        - keys_a
    )

    shared_trades = [
        map_a[t]
        for t in shared
    ]

    only_a_trades = [
        map_a[t]
        for t in only_a
    ]

    only_b_trades = [
        map_b[t]
        for t in only_b
    ]

    shared_stats = (
        stats_for_trades(
            shared_trades
        )
    )

    a_only_stats = (
        stats_for_trades(
            only_a_trades
        )
    )

    b_only_stats = (
        stats_for_trades(
            only_b_trades
        )
    )

    return [
        {
            "comparison": (
                f"{name_a}_VS_{name_b}"
            ),
            "segment": "SHARED",
            **shared_stats,
        },
        {
            "comparison": (
                f"{name_a}_VS_{name_b}"
            ),
            "segment": (
                f"{name_a}_ONLY"
            ),
            **a_only_stats,
        },
        {
            "comparison": (
                f"{name_a}_VS_{name_b}"
            ),
            "segment": (
                f"{name_b}_ONLY"
            ),
            **b_only_stats,
        },
    ]


# ============================================================
# RUN
# ============================================================

def run_research():
    try:
        STATUS.update({
            "state": "fetching_h1",
            "message": (
                "Fetching EUR/GBP H1 history"
            ),
        })

        h1 = fetch_chunked(
            "H1",
            RESEARCH_FROM
            - timedelta(
                days=H1_WARMUP_DAYS
            ),
            RESEARCH_TO,
            H1_CHUNK_DAYS,
        )

        if not h1:
            raise RuntimeError(
                "No H1 candles returned"
            )

        STATUS.update({
            "state": "fetching_daily",
            "message": (
                "Fetching EUR/GBP daily history"
            ),
        })

        daily = fetch_chunked(
            "D",
            RESEARCH_FROM
            - timedelta(
                days=D_WARMUP_DAYS
            ),
            RESEARCH_TO,
            D_CHUNK_DAYS,
        )

        if not daily:
            raise RuntimeError(
                "No daily candles returned"
            )

        STATUS.update({
            "state": "precomputing",
            "message": (
                "Precomputing features"
            ),
        })

        atr = atr_series(
            h1,
            ATR_LENGTH,
        )

        daily_state = (
            prepare_daily(
                daily
            )
        )

        raw_candidates = (
            build_raw_candidates(
                h1,
                atr,
                daily_state,
            )
        )

        STATUS[
            "raw_candidates"
        ] = len(
            raw_candidates
        )

        strategies = {
            "CURRENT_LIVE_CONTROL": (
                filter_control(
                    raw_candidates
                )
            ),
            "CANDIDATE_B": (
                filter_candidate_b(
                    raw_candidates
                )
            ),
            "SIMPLIFIED_NO_ALIGNMENT": (
                filter_simplified(
                    raw_candidates
                )
            ),
        }

        simulated = {}

        for name, eligible in (
            strategies.items()
        ):
            STATUS.update({
                "state": "simulating",
                "message": (
                    f"Simulating {name}"
                ),
            })

            (
                trades,
                ignored,
            ) = simulate_variant(
                h1,
                eligible,
            )

            simulated[name] = {
                "eligible": eligible,
                "trades": trades,
                "ignored": ignored,
            }

        summary_rows = []
        yearly_rows = []
        rolling_rows = []
        trade_rows = []

        for (
            name,
            result,
        ) in simulated.items():
            summary_rows.append(
                build_summary_row(
                    name,
                    result["trades"],
                    result["ignored"],
                )
            )

            yearly_rows.extend(
                build_yearly_rows(
                    name,
                    result["trades"],
                )
            )

            rolling_rows.extend(
                build_rolling_rows(
                    name,
                    result["trades"],
                )
            )

            trade_rows.extend(
                build_trade_rows(
                    name,
                    result["trades"],
                )
            )

        overlap_rows = []

        overlap_rows.extend(
            overlap_stats(
                "CURRENT_LIVE_CONTROL",
                simulated[
                    "CURRENT_LIVE_CONTROL"
                ]["trades"],
                "CANDIDATE_B",
                simulated[
                    "CANDIDATE_B"
                ]["trades"],
            )
        )

        overlap_rows.extend(
            overlap_stats(
                "CURRENT_LIVE_CONTROL",
                simulated[
                    "CURRENT_LIVE_CONTROL"
                ]["trades"],
                "SIMPLIFIED_NO_ALIGNMENT",
                simulated[
                    "SIMPLIFIED_NO_ALIGNMENT"
                ]["trades"],
            )
        )

        overlap_rows.extend(
            overlap_stats(
                "CANDIDATE_B",
                simulated[
                    "CANDIDATE_B"
                ]["trades"],
                "SIMPLIFIED_NO_ALIGNMENT",
                simulated[
                    "SIMPLIFIED_NO_ALIGNMENT"
                ]["trades"],
            )
        )

        pd.DataFrame(
            summary_rows
        ).to_csv(
            OUTPUT_SUMMARY,
            index=False,
        )

        pd.DataFrame(
            yearly_rows
        ).to_csv(
            OUTPUT_YEARLY,
            index=False,
        )

        pd.DataFrame(
            rolling_rows
        ).to_csv(
            OUTPUT_ROLLING,
            index=False,
        )

        pd.DataFrame(
            trade_rows
        ).to_csv(
            OUTPUT_TRADES,
            index=False,
        )

        pd.DataFrame(
            overlap_rows
        ).to_csv(
            OUTPUT_OVERLAP,
            index=False,
        )

        STATUS.update({
            "state": "complete",
            "message": (
                "EUR/GBP long final validation complete"
            ),
            "outputs": {
                "summary": OUTPUT_SUMMARY,
                "yearly": OUTPUT_YEARLY,
                "rolling": OUTPUT_ROLLING,
                "trades": OUTPUT_TRADES,
                "overlap": OUTPUT_OVERLAP,
            },
        })

        print()
        print("=" * 90)
        print(
            "EUR/GBP LONG FINAL VALIDATION COMPLETE"
        )
        print("=" * 90)
        print(
            pd.DataFrame(
                summary_rows
            ).to_string(
                index=False
            ),
            flush=True,
        )

    except Exception as error:
        STATUS.update({
            "state": "error",
            "message": str(
                error
            ),
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
def home():
    return jsonify({
        "service": (
            "EUR/GBP Long Final Validation"
        ),
        "status": STATUS,
        "mode": "READ_ONLY_RESEARCH",
        "orders_supported": False,
        "trading_enabled": False,
        "downloads": {
            "summary": (
                "/download/summary"
            ),
            "yearly": (
                "/download/yearly"
            ),
            "rolling": (
                "/download/rolling"
            ),
            "trades": (
                "/download/trades"
            ),
            "overlap": (
                "/download/overlap"
            ),
        },
    })


@app.route("/status")
def status():
    return jsonify(
        STATUS
    )


def download_named(
    filename
):
    path = os.path.abspath(
        filename
    )

    if not os.path.exists(
        path
    ):
        return jsonify({
            "status": "not_ready",
            "message": (
                f"{filename} is not ready yet"
            ),
        }), 404

    return send_file(
        path,
        as_attachment=True,
        download_name=filename,
    )


@app.route("/download/summary")
def download_summary():
    return download_named(
        OUTPUT_SUMMARY
    )


@app.route("/download/yearly")
def download_yearly():
    return download_named(
        OUTPUT_YEARLY
    )


@app.route("/download/rolling")
def download_rolling():
    return download_named(
        OUTPUT_ROLLING
    )


@app.route("/download/trades")
def download_trades():
    return download_named(
        OUTPUT_TRADES
    )


@app.route("/download/overlap")
def download_overlap():
    return download_named(
        OUTPUT_OVERLAP
    )


if __name__ == "__main__":
    thread = threading.Thread(
        target=run_research,
        name=(
            "eurgbp-long-final-validation"
        ),
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
