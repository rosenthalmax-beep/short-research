import os
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# EUR/USD LONG - FINAL VALIDATION
#
# RESEARCH ONLY - NEVER SUBMITS ORDERS.
#
# COMPARES
# ------------------------------------------------------------
# 1) CURRENT LIVE CONTROL
#
#   body ratio >= 1.05
#   strong close >= 0.70
#   structure 20 / 0.15 ATR14
#   previous completed daily close > EMA187
#   previous completed daily EMA30 > EMA187
#   New York 08:00-16:59
#   exclude Tuesday + Friday
#   RR 3.50
#
# 2) CANDIDATE A
#
#   body ratio >= 1.00
#   strong close >= 0.60
#   structure 15 / 0.10 ATR14
#   NO daily EMA regime
#   New York 08:00-15:59
#   exclude Tuesday + Friday
#   RR 3.50
#
# OUTPUTS
# ------------------------------------------------------------
# - summary comparison
# - yearly comparison
# - rolling 3-year comparison
# - exact trade logs
# - overlap / exclusive trade analysis
#
# ============================================================
# LOCKED BACKTEST CONVENTIONS
#
# OANDA midpoint H1.
#
# Bullish engulfing:
#   previous bearish
#   current bullish
#   current open <= previous close
#   current close >= previous open
#
# ATR14 = Wilder/RMA, SMA-seeded.
#
# EUR/USD tick size = 0.00001.
#
# Reference entry = signal close.
# Historical adverse long fill =
#       signal close + 5 ticks.
#
# Stop =
#       signal low - 10 ticks.
#
# Target based on REFERENCE signal close:
#       target = signal close
#              + (signal close - stop) * 3.50
#
# Actual R uses adverse fill.
#
# Pyramiding = 0.
#
# Same-bar tie:
#   compare open->high vs open->low
#   high closer => target first
#   else stop first.
#
# Signals with signal_index < position_exit_index ignored.
# Exact exit-candle signal allowed.
#
# Exits begin signal_index + 1.
#
# Daily:
#   dailyAlignment = 17
#   alignmentTimezone = America/New_York
#   previous completed daily candle only.
#
# History:
#   2002-05-06 20:00 UTC
#   -> current completed UTC hour.
#
# ============================================================


app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_URL = "https://api-fxtrade.oanda.com"

INSTRUMENT = "EUR_USD"

TICK_SIZE = 0.00001
STOP_BUFFER_TICKS = 10
BACKTEST_SLIPPAGE_TICKS = 5
REWARD_RISK = 3.50

ATR_LENGTH = 14

NY_TZ = ZoneInfo("America/New_York")

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

H1_WARMUP_DAYS = 220
D_WARMUP_DAYS = 2500

OUTPUT_SUMMARY = (
    "eurusd_long_final_validation_summary.csv"
)

OUTPUT_YEARLY = (
    "eurusd_long_final_validation_yearly.csv"
)

OUTPUT_ROLLING = (
    "eurusd_long_final_validation_rolling3y.csv"
)

OUTPUT_TRADES = (
    "eurusd_long_final_validation_trades.csv"
)

OUTPUT_OVERLAP = (
    "eurusd_long_final_validation_overlap.csv"
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
        "EUR/USD Long Final Validation"
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
        "Authorization":
            f"Bearer {OANDA_TOKEN}"
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
        "time":
            datetime.fromisoformat(
                raw["time"].replace(
                    "Z",
                    "+00:00",
                )
            ),
        "open":
            float(mid["o"]),
        "high":
            float(mid["h"]),
        "low":
            float(mid["l"]),
        "close":
            float(mid["c"]),
    }


def fetch_range(
    granularity,
    start,
    end,
):
    params = {
        "price": "M",
        "granularity":
            granularity,
        "from":
            iso_utc(start),
        "to":
            iso_utc(end),
        "smooth":
            "false",
        "includeFirst":
            "true",
        "dailyAlignment":
            DAILY_ALIGNMENT_HOUR,
        "alignmentTimezone":
            DAILY_ALIGNMENT_TIMEZONE,
    }

    data = oanda_get(
        f"/v3/instruments/"
        f"{INSTRUMENT}/candles",
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
            f"{cursor.date()} "
            f"-> {chunk_end.date()}",
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
        key=lambda item:
            item["time"]
    )

    return candles


# ============================================================
# INDICATORS
# ============================================================

def true_ranges(
    candles,
):
    values = []

    for index, candle in enumerate(
        candles
    ):
        if index == 0:
            tr = (
                candle["high"]
                - candle["low"]
            )
        else:
            previous_close = (
                candles[
                    index - 1
                ]["close"]
            )

            tr = max(
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

        values.append(
            tr
        )

    return values


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

    candidate = (
        ny_time.replace(
            hour=DAILY_ALIGNMENT_HOUR,
            minute=0,
            second=0,
            microsecond=0,
        )
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

    ema30 = ema_series(
        closes,
        30,
    )

    ema187 = ema_series(
        closes,
        187,
    )

    rows = []

    for index, candle in enumerate(
        daily
    ):
        rows.append({
            "time":
                candle["time"],
            "close":
                candle["close"],
            "ema30":
                ema30[index],
            "ema187":
                ema187[index],
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
# RAW SIGNAL FEATURES
# ============================================================

STRUCTURE_LOOKBACKS = [
    15,
    20,
]


def build_raw_candidates(
    h1,
    atr,
    daily_state,
):
    candidates = []

    start_index = max(
        ATR_LENGTH,
        max(
            STRUCTURE_LOOKBACKS
        ),
    )

    for index in range(
        start_index,
        len(h1),
    ):
        signal = h1[index]

        if (
            signal["time"]
            < RESEARCH_FROM
        ):
            continue

        if (
            signal["time"]
            >= RESEARCH_TO
        ):
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
            and
            signal["close"]
            > signal["open"]
            and
            signal["open"]
            <= previous["close"]
            and
            signal["close"]
            >= previous["open"]
        )

        if not bullish_engulfing:
            continue

        body_ratio = (
            current_body
            / previous_body
        )

        close_location = (
            signal["close"]
            - signal["low"]
        ) / signal_range

        structure_distances = {}

        for lookback in (
            STRUCTURE_LOOKBACKS
        ):
            previous_lowest = min(
                candle["low"]
                for candle
                in h1[
                    index - lookback:
                    index
                ]
            )

            structure_distances[
                lookback
            ] = (
                signal["low"]
                - previous_lowest
            ) / current_atr

        daily = (
            previous_completed_daily(
                signal["time"],
                daily_state,
            )
        )

        ny = (
            signal["time"]
            .astimezone(
                NY_TZ
            )
        )

        candidates.append({
            "index":
                index,
            "time":
                signal["time"],
            "body_ratio":
                body_ratio,
            "close_location":
                close_location,
            "structure_distances":
                structure_distances,
            "daily":
                daily,
            "ny_hour":
                ny.hour,
            "ny_weekday":
                ny.weekday(),
        })

    return candidates


# ============================================================
# STRATEGY FILTERS
# ============================================================

def passes_current_live(
    signal,
):
    if (
        signal["body_ratio"]
        < 1.05
    ):
        return False

    if (
        signal["close_location"]
        < 0.70
    ):
        return False

    if (
        signal[
            "structure_distances"
        ][20] > 0.15
    ):
        return False

    daily = signal[
        "daily"
    ]

    if daily is None:
        return False

    if (
        daily["ema30"] is None
        or daily["ema187"] is None
    ):
        return False

    if not (
        daily["close"]
        > daily["ema187"]
    ):
        return False

    if not (
        daily["ema30"]
        > daily["ema187"]
    ):
        return False

    if not (
        signal["ny_hour"]
        >= 8
        and signal["ny_hour"]
        < 17
    ):
        return False

    if (
        signal["ny_weekday"]
        in {
            1,
            4,
        }
    ):
        return False

    return True


def passes_candidate_a(
    signal,
):
    if (
        signal["body_ratio"]
        < 1.00
    ):
        return False

    if (
        signal["close_location"]
        < 0.60
    ):
        return False

    if (
        signal[
            "structure_distances"
        ][15] > 0.10
    ):
        return False

    if not (
        signal["ny_hour"]
        >= 8
        and signal["ny_hour"]
        < 16
    ):
        return False

    if (
        signal["ny_weekday"]
        in {
            1,
            4,
        }
    ):
        return False

    return True


# ============================================================
# TRADE SIMULATION
# ============================================================

EXIT_CACHE = {}


def calculate_trade_exit(
    h1,
    signal_index,
):
    if (
        signal_index
        in EXIT_CACHE
    ):
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

    if (
        reference_risk <= 0
    ):
        EXIT_CACHE[
            signal_index
        ] = None
        return None

    target = (
        reference_entry
        + reference_risk
        * REWARD_RISK
    )

    actual_risk = (
        backtest_entry
        - stop
    )

    if (
        actual_risk <= 0
    ):
        EXIT_CACHE[
            signal_index
        ] = None
        return None

    for index in range(
        signal_index + 1,
        len(h1),
    ):
        candle = h1[index]

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
                exit_reason = (
                    "TARGET_TIE_FIRST"
                )
            else:
                exit_price = stop
                exit_reason = (
                    "STOP_TIE_FIRST"
                )

        elif target_hit:
            exit_price = target
            exit_reason = "TARGET"

        else:
            exit_price = stop
            exit_reason = "STOP"

        result = {
            "signal_index":
                signal_index,
            "signal_time":
                signal["time"],
            "exit_index":
                index,
            "exit_time":
                candle["time"],
            "entry_reference":
                reference_entry,
            "backtest_entry":
                backtest_entry,
            "stop":
                stop,
            "target":
                target,
            "exit_price":
                exit_price,
            "exit_reason":
                exit_reason,
            "result_r":
                (
                    exit_price
                    - backtest_entry
                ) / actual_risk,
        }

        EXIT_CACHE[
            signal_index
        ] = result

        return result

    EXIT_CACHE[
        signal_index
    ] = None

    return None


def simulate_variant(
    h1,
    eligible,
    strategy_name,
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

        if trade is None:
            break

        row = dict(
            trade
        )

        row["strategy"] = (
            strategy_name
        )

        row["body_ratio"] = (
            signal[
                "body_ratio"
            ]
        )

        row["close_location"] = (
            signal[
                "close_location"
            ]
        )

        row[
            "structure15_distance_atr"
        ] = (
            signal[
                "structure_distances"
            ][15]
        )

        row[
            "structure20_distance_atr"
        ] = (
            signal[
                "structure_distances"
            ][20]
        )

        row["ny_hour"] = (
            signal["ny_hour"]
        )

        row["ny_weekday"] = (
            signal["ny_weekday"]
        )

        daily = signal[
            "daily"
        ]

        row["daily_close"] = (
            None
            if daily is None
            else daily["close"]
        )

        row["daily_ema30"] = (
            None
            if daily is None
            else daily["ema30"]
        )

        row["daily_ema187"] = (
            None
            if daily is None
            else daily["ema187"]
        )

        trades.append(
            row
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
        "trades":
            len(results),
        "winners":
            len(winners),
        "losers":
            len(losers),
        "win_rate":
            round(
                len(winners)
                / len(results)
                * 100.0,
                2,
            ),
        "profit_factor":
            round(
                pf,
                3,
            ),
        "total_r":
            round(
                total_r,
                2,
            ),
        "expectancy_r":
            round(
                total_r
                / len(results),
                3,
            ),
        "max_drawdown_r":
            round(
                max_dd,
                2,
            ),
        "longest_loss_streak":
            longest_streak,
    }


def subtract_years_safe(
    dt,
    years,
):
    try:
        return dt.replace(
            year=
                dt.year
                - years
        )
    except ValueError:
        return dt.replace(
            month=2,
            day=28,
            year=
                dt.year
                - years,
        )


# ============================================================
# SUMMARY / YEARLY / ROLLING
# ============================================================

def build_summary_row(
    strategy_name,
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
        "strategy":
            strategy_name,
        "research_from":
            RESEARCH_FROM.isoformat(),
        "research_to":
            RESEARCH_TO.isoformat(),
        "eligible_signals":
            len(trades)
            + ignored,
        "ignored_due_to_open_trade":
            ignored,
        "trades":
            full["trades"],
        "trades_per_year":
            round(
                full["trades"]
                / years,
                3,
            ),
        "winners":
            full["winners"],
        "losers":
            full["losers"],
        "win_rate":
            full["win_rate"],
        "profit_factor":
            full[
                "profit_factor"
            ],
        "total_r":
            full["total_r"],
        "expectancy_r":
            full[
                "expectancy_r"
            ],
        "max_drawdown_r":
            full[
                "max_drawdown_r"
            ],
        "longest_loss_streak":
            full[
                "longest_loss_streak"
            ],
        "annual_r_linear":
            round(
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

        if (
            stats["trades"]
            >= 5
        ):
            if (
                minimum_era_pf
                is None
            ):
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

            if (
                stats["total_r"]
                > 0
            ):
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
    strategy_name,
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
            "strategy":
                strategy_name,
            "year":
                year,
            **stats,
        })

    return rows


def build_rolling_rows(
    strategy_name,
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
            "strategy":
                strategy_name,
            "window":
                f"{start_year}_"
                f"{start_year + 2}",
            "start":
                start.isoformat(),
            "end":
                end.isoformat(),
            **stats,
        })

    return rows


# ============================================================
# TRADE OUTPUT
# ============================================================

def trade_rows(
    trades,
):
    rows = []

    for trade in trades:
        rows.append({
            "strategy":
                trade["strategy"],
            "signal_time":
                trade[
                    "signal_time"
                ].isoformat(),
            "exit_time":
                trade[
                    "exit_time"
                ].isoformat(),
            "signal_index":
                trade[
                    "signal_index"
                ],
            "exit_index":
                trade[
                    "exit_index"
                ],
            "entry_reference":
                round(
                    trade[
                        "entry_reference"
                    ],
                    6,
                ),
            "backtest_entry":
                round(
                    trade[
                        "backtest_entry"
                    ],
                    6,
                ),
            "stop":
                round(
                    trade["stop"],
                    6,
                ),
            "target":
                round(
                    trade["target"],
                    6,
                ),
            "exit_price":
                round(
                    trade[
                        "exit_price"
                    ],
                    6,
                ),
            "exit_reason":
                trade[
                    "exit_reason"
                ],
            "result_r":
                round(
                    trade[
                        "result_r"
                    ],
                    6,
                ),
            "body_ratio":
                round(
                    trade[
                        "body_ratio"
                    ],
                    6,
                ),
            "close_location":
                round(
                    trade[
                        "close_location"
                    ],
                    6,
                ),
            "structure15_distance_atr":
                round(
                    trade[
                        "structure15_distance_atr"
                    ],
                    6,
                ),
            "structure20_distance_atr":
                round(
                    trade[
                        "structure20_distance_atr"
                    ],
                    6,
                ),
            "ny_hour":
                trade["ny_hour"],
            "ny_weekday":
                trade[
                    "ny_weekday"
                ],
            "daily_close":
                (
                    None
                    if trade[
                        "daily_close"
                    ] is None
                    else round(
                        trade[
                            "daily_close"
                        ],
                        6,
                    )
                ),
            "daily_ema30":
                (
                    None
                    if trade[
                        "daily_ema30"
                    ] is None
                    else round(
                        trade[
                            "daily_ema30"
                        ],
                        6,
                    )
                ),
            "daily_ema187":
                (
                    None
                    if trade[
                        "daily_ema187"
                    ] is None
                    else round(
                        trade[
                            "daily_ema187"
                        ],
                        6,
                    )
                ),
        })

    return rows


# ============================================================
# OVERLAP ANALYSIS
# ============================================================

def overlap_stats(
    trades,
):
    return stats_for_trades(
        trades
    )


def build_overlap_rows(
    current_trades,
    candidate_trades,
):
    current_by_time = {
        trade["signal_time"]:
            trade
        for trade
        in current_trades
    }

    candidate_by_time = {
        trade["signal_time"]:
            trade
        for trade
        in candidate_trades
    }

    current_times = set(
        current_by_time.keys()
    )

    candidate_times = set(
        candidate_by_time.keys()
    )

    shared_times = (
        current_times
        & candidate_times
    )

    current_only_times = (
        current_times
        - candidate_times
    )

    candidate_only_times = (
        candidate_times
        - current_times
    )

    shared_current = [
        current_by_time[t]
        for t in sorted(
            shared_times
        )
    ]

    shared_candidate = [
        candidate_by_time[t]
        for t in sorted(
            shared_times
        )
    ]

    current_only = [
        current_by_time[t]
        for t in sorted(
            current_only_times
        )
    ]

    candidate_only = [
        candidate_by_time[t]
        for t in sorted(
            candidate_only_times
        )
    ]

    groups = [
        (
            "SHARED_CURRENT",
            shared_current,
        ),
        (
            "SHARED_CANDIDATE_A",
            shared_candidate,
        ),
        (
            "CURRENT_ONLY",
            current_only,
        ),
        (
            "CANDIDATE_A_ONLY",
            candidate_only,
        ),
    ]

    rows = []

    for (
        label,
        group,
    ) in groups:
        stats = overlap_stats(
            group
        )

        rows.append({
            "group":
                label,
            **stats,
        })

    return rows


# ============================================================
# RUN
# ============================================================

def run_research():
    try:
        STATUS.update({
            "state":
                "fetching_h1",
            "message":
                "Fetching EUR/USD H1 history",
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
            "state":
                "fetching_daily",
            "message":
                "Fetching EUR/USD daily history",
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
            "state":
                "precomputing",
            "message":
                "Precomputing EUR/USD signal features",
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

        current_signals = [
            signal
            for signal
            in raw_candidates
            if passes_current_live(
                signal
            )
        ]

        candidate_signals = [
            signal
            for signal
            in raw_candidates
            if passes_candidate_a(
                signal
            )
        ]

        STATUS.update({
            "state":
                "simulating",
            "message":
                "Simulating current live and Candidate A",
            "raw_candidates":
                len(
                    raw_candidates
                ),
            "current_eligible":
                len(
                    current_signals
                ),
            "candidate_a_eligible":
                len(
                    candidate_signals
                ),
        })

        (
            current_trades,
            current_ignored,
        ) = simulate_variant(
            h1,
            current_signals,
            "CURRENT_LIVE",
        )

        (
            candidate_trades,
            candidate_ignored,
        ) = simulate_variant(
            h1,
            candidate_signals,
            "CANDIDATE_A",
        )

        summary_rows = [
            build_summary_row(
                "CURRENT_LIVE",
                current_trades,
                current_ignored,
            ),
            build_summary_row(
                "CANDIDATE_A",
                candidate_trades,
                candidate_ignored,
            ),
        ]

        yearly_rows = (
            build_yearly_rows(
                "CURRENT_LIVE",
                current_trades,
            )
            + build_yearly_rows(
                "CANDIDATE_A",
                candidate_trades,
            )
        )

        rolling_rows = (
            build_rolling_rows(
                "CURRENT_LIVE",
                current_trades,
            )
            + build_rolling_rows(
                "CANDIDATE_A",
                candidate_trades,
            )
        )

        trades_rows = (
            trade_rows(
                current_trades
            )
            + trade_rows(
                candidate_trades
            )
        )

        overlap_rows = (
            build_overlap_rows(
                current_trades,
                candidate_trades,
            )
        )

        pd.DataFrame(
            summary_rows
        ).to_csv(
            os.path.abspath(
                OUTPUT_SUMMARY
            ),
            index=False,
        )

        pd.DataFrame(
            yearly_rows
        ).to_csv(
            os.path.abspath(
                OUTPUT_YEARLY
            ),
            index=False,
        )

        pd.DataFrame(
            rolling_rows
        ).to_csv(
            os.path.abspath(
                OUTPUT_ROLLING
            ),
            index=False,
        )

        pd.DataFrame(
            trades_rows
        ).to_csv(
            os.path.abspath(
                OUTPUT_TRADES
            ),
            index=False,
        )

        pd.DataFrame(
            overlap_rows
        ).to_csv(
            os.path.abspath(
                OUTPUT_OVERLAP
            ),
            index=False,
        )

        STATUS.update({
            "state":
                "complete",
            "message":
                "EUR/USD final validation complete",
            "current_trades":
                len(
                    current_trades
                ),
            "candidate_a_trades":
                len(
                    candidate_trades
                ),
            "outputs": {
                "summary":
                    OUTPUT_SUMMARY,
                "yearly":
                    OUTPUT_YEARLY,
                "rolling":
                    OUTPUT_ROLLING,
                "trades":
                    OUTPUT_TRADES,
                "overlap":
                    OUTPUT_OVERLAP,
            },
        })

        print()
        print("=" * 95)
        print(
            "EUR/USD LONG FINAL VALIDATION COMPLETE"
        )
        print("=" * 95)

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
            "state":
                "error",
            "message":
                str(
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
        "service":
            "EUR/USD Long Final Validation",
        "status":
            STATUS,
        "mode":
            "READ_ONLY_RESEARCH",
        "orders_supported":
            False,
        "trading_enabled":
            False,
        "downloads": {
            "summary":
                "/download/summary",
            "yearly":
                "/download/yearly",
            "rolling":
                "/download/rolling",
            "trades":
                "/download/trades",
            "overlap":
                "/download/overlap",
        },
    })


@app.route("/status")
def status():
    return jsonify(
        STATUS
    )


def send_output(
    filename,
):
    path = os.path.abspath(
        filename
    )

    if not os.path.exists(
        path
    ):
        return jsonify({
            "status":
                "not_ready",
            "message":
                f"{filename} is not ready yet",
        }), 404

    return send_file(
        path,
        as_attachment=True,
        download_name=filename,
    )


@app.route(
    "/download/summary"
)
def download_summary():
    return send_output(
        OUTPUT_SUMMARY
    )


@app.route(
    "/download/yearly"
)
def download_yearly():
    return send_output(
        OUTPUT_YEARLY
    )


@app.route(
    "/download/rolling"
)
def download_rolling():
    return send_output(
        OUTPUT_ROLLING
    )


@app.route(
    "/download/trades"
)
def download_trades():
    return send_output(
        OUTPUT_TRADES
    )


@app.route(
    "/download/overlap"
)
def download_overlap():
    return send_output(
        OUTPUT_OVERLAP
    )


if __name__ == "__main__":
    thread = threading.Thread(
        target=run_research,
        name=(
            "eurusd-long-final-validation"
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
