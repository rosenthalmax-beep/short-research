import os
import itertools
import threading
import requests
import pandas as pd

from flask import Flask, send_file, jsonify
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ==================================================
# FLASK
# ==================================================

app = Flask(__name__)


# ==================================================
# CONFIG
# ==================================================

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_URL = "https://api-fxtrade.oanda.com"

INSTRUMENT = "EUR_USD"

TICK_SIZE = 0.00001

NY_TZ = ZoneInfo("America/New_York")

DAILY_ALIGNMENT_HOUR = 17
DAILY_ALIGNMENT_TIMEZONE = "America/New_York"

STOP_BUFFER_TICKS = 10
BACKTEST_SLIPPAGE_TICKS = 5

H1_CHUNK_DAYS = 180

RESEARCH_FROM = datetime(
    2002, 5, 6, 20, 0,
    tzinfo=timezone.utc
)

RESEARCH_TO = (
    datetime.now(timezone.utc)
    .replace(
        minute=0,
        second=0,
        microsecond=0
    )
)

H1_WARMUP_DAYS = 90
DAILY_WARMUP_DAYS = 1500

SUMMARY_OUTPUT_FILE = (
    "eurusd_short_frequency_consistency_sweep.csv"
)

YEARLY_OUTPUT_FILE = (
    "eurusd_short_frequency_consistency_yearly.csv"
)


# ==================================================
# FIXED CORE
# ==================================================

STRUCTURE_LOOKBACK = 55

SLOW_EMA_LENGTH = 100

REWARD_RISK = 4.00


# ==================================================
# FREQUENCY / CONSISTENCY GRID
# ==================================================

BODY_RATIOS = [
    1.10,
    1.20,
    1.30
]

RECENT_HIGH_DISTANCE_ATR_VALUES = [
    0.25,
    0.30,
    0.35,
    0.40
]

FAST_EMA_LENGTHS = [
    75,
    80,
    85,
    90
]

STRONG_CLOSE_LEVELS = [
    0.275,
    0.30,
    0.325,
    0.35
]

EMA_SEPARATION_ATR_VALUES = [
    None,
    0.025,
    0.05,
    0.075
]


# ==================================================
# TIMING VARIANTS
#
# Signal candle opening hour in America/New_York.
# No weekday exclusions.
# ==================================================

TIMING_VARIANTS = {

    "ALL_HOURS":
        tuple(),

    "EXCLUDE_10":
        (
            10,
        ),

    "EXCLUDE_10_14":
        (
            10,
            14
        ),

    "EXCLUDE_10_12_14":
        (
            10,
            12,
            14
        ),

    "EXCLUDE_02_10_12_14":
        (
            2,
            10,
            12,
            14
        )
}


# ==================================================
# HISTORICAL ERAS
# ==================================================

ERAS = [

    (
        "2002_2009",
        datetime(
            2002, 5, 6, 20, 0,
            tzinfo=timezone.utc
        ),
        datetime(
            2010, 1, 1, 0, 0,
            tzinfo=timezone.utc
        )
    ),

    (
        "2010_2017",
        datetime(
            2010, 1, 1, 0, 0,
            tzinfo=timezone.utc
        ),
        datetime(
            2018, 1, 1, 0, 0,
            tzinfo=timezone.utc
        )
    ),

    (
        "2018_2023",
        datetime(
            2018, 1, 1, 0, 0,
            tzinfo=timezone.utc
        ),
        datetime(
            2024, 1, 1, 0, 0,
            tzinfo=timezone.utc
        )
    ),

    (
        "2024_PRESENT",
        datetime(
            2024, 1, 1, 0, 0,
            tzinfo=timezone.utc
        ),
        RESEARCH_TO
    )
]


# ==================================================
# TOTAL COMBINATIONS
#
# 3 x 4 x 4 x 4 x 4 x 5
# = 3,840
# ==================================================

TOTAL_COMBINATIONS = (
    len(BODY_RATIOS)
    * len(RECENT_HIGH_DISTANCE_ATR_VALUES)
    * len(FAST_EMA_LENGTHS)
    * len(STRONG_CLOSE_LEVELS)
    * len(EMA_SEPARATION_ATR_VALUES)
    * len(TIMING_VARIANTS)
)


# ==================================================
# STATUS
# ==================================================

RESEARCH_STATUS = {

    "state":
        "not_started",

    "message":
        "Research has not started",

    "research_from":
        RESEARCH_FROM.isoformat(),

    "research_to":
        RESEARCH_TO.isoformat(),

    "total_combinations":
        TOTAL_COMBINATIONS,

    "completed_combinations":
        0,

    "rows_saved":
        0,

    "base_signal_candidates":
        0
}


# ==================================================
# OANDA
# ==================================================

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
        dt.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def oanda_get(
    path,
    params
):

    response = requests.get(
        OANDA_URL + path,
        headers=headers(),
        params=params,
        timeout=30
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
        False
    ):

        return None

    mid = raw.get(
        "mid"
    )

    if not mid:

        return None

    return {

        "time":
            datetime.fromisoformat(
                raw["time"].replace(
                    "Z",
                    "+00:00"
                )
            ),

        "open":
            float(
                mid["o"]
            ),

        "high":
            float(
                mid["h"]
            ),

        "low":
            float(
                mid["l"]
            ),

        "close":
            float(
                mid["c"]
            )
    }


def fetch_range(
    instrument,
    granularity,
    start,
    end
):

    params = {

        "price":
            "M",

        "granularity":
            granularity,

        "from":
            iso_utc(
                start
            ),

        "to":
            iso_utc(
                end
            ),

        "smooth":
            "false",

        "includeFirst":
            "true",

        "dailyAlignment":
            DAILY_ALIGNMENT_HOUR,

        "alignmentTimezone":
            DAILY_ALIGNMENT_TIMEZONE
    }

    data = oanda_get(

        f"/v3/instruments/"
        f"{instrument}/candles",

        params
    )

    candles = []

    for raw in data.get(
        "candles",
        []
    ):

        candle = parse_candle(
            raw
        )

        if candle is not None:

            candles.append(
                candle
            )

    return candles


def fetch_chunked_history(
    instrument,
    granularity,
    start,
    end
):

    candles_by_time = {}

    cursor = start

    while cursor < end:

        chunk_end = min(

            cursor
            + timedelta(
                days=H1_CHUNK_DAYS
            ),

            end
        )

        print(
            f"Fetching {granularity}: "
            f"{cursor.date()} -> "
            f"{chunk_end.date()}",
            flush=True
        )

        chunk = fetch_range(

            instrument,
            granularity,
            cursor,
            chunk_end
        )

        for candle in chunk:

            candles_by_time[
                candle["time"]
            ] = candle

        cursor = chunk_end

    candles = list(
        candles_by_time.values()
    )

    candles.sort(
        key=lambda item:
            item["time"]
    )

    return candles


# ==================================================
# INDICATORS
# ==================================================

def ema_series(
    values,
    length
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
        len(values)
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


def true_ranges(
    candles
):

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
                )
            )

        result.append(
            tr
        )

    return result


def rma_series(
    values,
    length
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
        len(values)
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
    length=14
):

    return rma_series(

        true_ranges(
            candles
        ),

        length
    )


# ==================================================
# DAILY ALIGNMENT
# ==================================================

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
            hour=
                DAILY_ALIGNMENT_HOUR,
            minute=0,
            second=0,
            microsecond=0
        )
    )

    if ny_time < candidate:

        candidate -= timedelta(
            days=1
        )

    return candidate.astimezone(
        timezone.utc
    )


# ==================================================
# DAILY STATE
# ==================================================

def build_daily_state(
    daily
):

    closes = [

        candle["close"]

        for candle in daily
    ]

    lengths = sorted(
        set(
            [
                SLOW_EMA_LENGTH
            ]
            + FAST_EMA_LENGTHS
        )
    )

    ema_cache = {}

    for length in lengths:

        ema_cache[
            length
        ] = ema_series(
            closes,
            length
        )

    daily_atr = atr_series(
        daily,
        14
    )

    return {

        "ema":
            ema_cache,

        "daily_atr":
            daily_atr
    }


def build_h1_daily_lookup(
    h1,
    daily,
    daily_state
):

    print(
        "Building previous completed "
        "daily-state lookup...",
        flush=True
    )

    lookup = [
        None
    ] * len(h1)

    daily_index = -1

    for h1_index, candle in enumerate(
        h1
    ):

        session_start = (
            current_daily_start(
                candle["time"]
            )
        )

        while (

            daily_index + 1
            < len(daily)

            and

            daily[
                daily_index + 1
            ]["time"]
            < session_start
        ):

            daily_index += 1

        if daily_index < 0:

            continue

        row = {

            "close":
                daily[
                    daily_index
                ]["close"],

            "daily_atr":
                daily_state[
                    "daily_atr"
                ][daily_index]
        }

        for (
            length,
            series
        ) in daily_state[
            "ema"
        ].items():

            row[
                f"ema_{length}"
            ] = series[
                daily_index
            ]

        lookup[
            h1_index
        ] = row

    return lookup


# ==================================================
# PRECOMPUTE BASE BEARISH ENGULFINGS
# ==================================================

def build_base_candidates(
    h1,
    h1_atr,
    daily_lookup
):

    candidates = []

    for index in range(
        STRUCTURE_LOOKBACK,
        len(h1)
    ):

        signal = h1[
            index
        ]

        if signal["time"] < RESEARCH_FROM:

            continue

        if signal["time"] >= RESEARCH_TO:

            break

        previous = h1[
            index - 1
        ]

        current_atr = (
            h1_atr[
                index
            ]
        )

        daily = (
            daily_lookup[
                index
            ]
        )

        if (
            current_atr is None
            or daily is None
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

        # ==========================================
        # BASE BEARISH ENGULF
        # ==========================================

        if not (

            previous["close"]
            > previous["open"]

            and

            signal["close"]
            < signal["open"]

            and

            signal["open"]
            >= previous["close"]

            and

            signal["close"]
            <= previous["open"]
        ):

            continue

        previous_highest = max(

            candle["high"]

            for candle in h1[

                index
                - STRUCTURE_LOOKBACK:

                index
            ]
        )

        body_ratio = (

            current_body
            / previous_body
        )

        close_location = (

            (
                signal["close"]
                - signal["low"]
            )

            / signal_range
        )

        recent_high_distance_atr = (

            previous_highest
            - signal["high"]

        ) / current_atr

        ny_time = (

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

            "recent_high_distance_atr":
                recent_high_distance_atr,

            "ny_hour":
                ny_time.hour,

            "daily":
                daily
        })

    return candidates


# ==================================================
# PARAMETER FILTER
# ==================================================

def candidate_allowed(
    candidate,
    body_ratio,
    recent_high_distance_atr,
    fast_ema_length,
    strong_close,
    ema_separation_atr,
    excluded_hours
):

    # ==============================================
    # BODY
    # ==============================================

    if (
        candidate[
            "body_ratio"
        ]
        < body_ratio
    ):

        return False

    # ==============================================
    # STRONG CLOSE
    # ==============================================

    if (
        candidate[
            "close_location"
        ]
        > strong_close
    ):

        return False

    # ==============================================
    # RECENT HIGH
    # ==============================================

    if (
        candidate[
            "recent_high_distance_atr"
        ]
        > recent_high_distance_atr
    ):

        return False

    # ==============================================
    # TIMING
    # ==============================================

    if (
        candidate[
            "ny_hour"
        ]
        in excluded_hours
    ):

        return False

    # ==============================================
    # DAILY REGIME
    # ==============================================

    daily = candidate[
        "daily"
    ]

    slow_ema = daily.get(
        f"ema_{SLOW_EMA_LENGTH}"
    )

    fast_ema = daily.get(
        f"ema_{fast_ema_length}"
    )

    daily_atr = daily.get(
        "daily_atr"
    )

    if (
        slow_ema is None
        or fast_ema is None
        or daily_atr is None
        or daily_atr <= 0
    ):

        return False

    if not (
        daily["close"]
        < slow_ema
    ):

        return False

    if not (
        fast_ema
        < slow_ema
    ):

        return False

    # ==============================================
    # EMA SEPARATION
    # ==============================================

    if (
        ema_separation_atr
        is not None
    ):

        separation = (

            slow_ema
            - fast_ema

        ) / daily_atr

        if (
            separation
            < ema_separation_atr
        ):

            return False

    return True


# ==================================================
# EXIT CACHE
# ==================================================

EXIT_CACHE = {}


def calculate_trade_exit(
    h1,
    signal_index
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

        - (
            BACKTEST_SLIPPAGE_TICKS
            * TICK_SIZE
        )
    )

    stop = (

        signal["high"]

        + (
            STOP_BUFFER_TICKS
            * TICK_SIZE
        )
    )

    reference_risk = (

        stop
        - reference_entry
    )

    if reference_risk <= 0:

        raise RuntimeError(
            "Invalid reference risk"
        )

    target = (

        reference_entry

        - (
            reference_risk
            * REWARD_RISK
        )
    )

    actual_risk = (

        stop
        - backtest_entry
    )

    if actual_risk <= 0:

        raise RuntimeError(
            "Invalid actual risk"
        )

    for index in range(

        signal_index + 1,

        len(h1)
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
            candle["high"]
            >= stop
        )

        target_hit = (
            candle["low"]
            <= target
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

                exit_price = stop
                exit_reason = "STOP"

            else:

                exit_price = target
                exit_reason = "TARGET"

        elif stop_hit:

            exit_price = stop
            exit_reason = "STOP"

        else:

            exit_price = target
            exit_reason = "TARGET"

        result_r = (

            (
                backtest_entry
                - exit_price
            )

            / actual_risk
        )

        result = {

            "status":
                "CLOSED",

            "signal_index":
                signal_index,

            "signal_time":
                signal["time"],

            "exit_index":
                index,

            "exit_time":
                candle["time"],

            "exit_reason":
                exit_reason,

            "result_r":
                result_r
        }

        EXIT_CACHE[
            signal_index
        ] = result

        return result

    result = {

        "status":
            "OPEN",

        "signal_index":
            signal_index,

        "signal_time":
            signal["time"],

        "exit_index":
            None,

        "exit_time":
            None,

        "exit_reason":
            None,

        "result_r":
            None
    }

    EXIT_CACHE[
        signal_index
    ] = result

    return result


# ==================================================
# SIMULATOR
# ==================================================

def simulate(
    h1,
    candidates
):

    trades = []

    position_exit_index = -1

    still_open = False

    ignored_signals = 0

    for candidate in candidates:

        signal_index = (
            candidate[
                "index"
            ]
        )

        if (
            signal_index
            < position_exit_index
        ):

            ignored_signals += 1

            continue

        trade = calculate_trade_exit(
            h1,
            signal_index
        )

        if (
            trade[
                "status"
            ]
            == "OPEN"
        ):

            still_open = True

            break

        trades.append(
            trade
        )

        position_exit_index = (
            trade[
                "exit_index"
            ]
        )

    return (
        trades,
        still_open,
        ignored_signals
    )


# ==================================================
# BASIC PERFORMANCE
# ==================================================

def calculate_basic_stats(
    trades
):

    if not trades:

        return {

            "trades":
                0,

            "winners":
                0,

            "losers":
                0,

            "win_rate":
                0.0,

            "profit_factor":
                0.0,

            "total_r":
                0.0,

            "expectancy_r":
                0.0,

            "max_drawdown_r":
                0.0,

            "longest_loss_streak":
                0
        }

    results = [

        trade[
            "result_r"
        ]

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

    total_r = sum(
        results
    )

    gross_profit = sum(
        winners
    )

    gross_loss = abs(
        sum(
            losers
        )
    )

    if gross_loss > 0:

        profit_factor = (

            gross_profit
            / gross_loss
        )

    elif gross_profit > 0:

        profit_factor = float(
            "inf"
        )

    else:

        profit_factor = 0.0

    win_rate = (

        len(winners)
        / len(results)

        * 100.0
    )

    expectancy = (

        total_r
        / len(results)
    )

    # ==============================================
    # DRAWDOWN
    # ==============================================

    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0

    for result in results:

        equity += result

        peak = max(
            peak,
            equity
        )

        max_drawdown = min(

            max_drawdown,

            equity - peak
        )

    # ==============================================
    # LOSING STREAK
    # ==============================================

    current_streak = 0
    longest_streak = 0

    for result in results:

        if result < 0:

            current_streak += 1

            longest_streak = max(

                longest_streak,

                current_streak
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
                win_rate,
                2
            ),

        "profit_factor":
            round(
                profit_factor,
                3
            )
            if profit_factor
            != float("inf")
            else 999.0,

        "total_r":
            round(
                total_r,
                2
            ),

        "expectancy_r":
            round(
                expectancy,
                3
            ),

        "max_drawdown_r":
            round(
                max_drawdown,
                2
            ),

        "longest_loss_streak":
            longest_streak
    }


# ==================================================
# YEARLY CONSISTENCY
#
# Years are assigned by SIGNAL ENTRY date.
# This avoids splitting one trade across years.
# ==================================================

def calculate_yearly_stats(
    trades
):

    first_year = (
        RESEARCH_FROM.year
    )

    last_year = (
        RESEARCH_TO.year
    )

    yearly = {}

    for year in range(
        first_year,
        last_year + 1
    ):

        yearly[
            year
        ] = {
            "trades":
                0,

            "total_r":
                0.0
        }

    for trade in trades:

        year = (
            trade[
                "signal_time"
            ].year
        )

        if year not in yearly:

            continue

        yearly[
            year
        ][
            "trades"
        ] += 1

        yearly[
            year
        ][
            "total_r"
        ] += trade[
            "result_r"
        ]

    active_years = [

        year

        for year, values
        in yearly.items()

        if values[
            "trades"
        ] > 0
    ]

    profitable_active_years = [

        year

        for year
        in active_years

        if yearly[
            year
        ][
            "total_r"
        ] > 0
    ]

    losing_active_years = [

        year

        for year
        in active_years

        if yearly[
            year
        ][
            "total_r"
        ] < 0
    ]

    flat_active_years = [

        year

        for year
        in active_years

        if yearly[
            year
        ][
            "total_r"
        ] == 0
    ]

    zero_trade_years = [

        year

        for year, values
        in yearly.items()

        if values[
            "trades"
        ] == 0
    ]

    active_returns = [

        yearly[
            year
        ][
            "total_r"
        ]

        for year
        in active_years
    ]

    if active_returns:

        median_annual_r = float(
            pd.Series(
                active_returns
            ).median()
        )

        worst_year_r = min(
            active_returns
        )

        best_year_r = max(
            active_returns
        )

    else:

        median_annual_r = 0.0
        worst_year_r = 0.0
        best_year_r = 0.0

    if active_years:

        profitable_year_pct = (

            len(
                profitable_active_years
            )

            / len(
                active_years
            )

            * 100.0
        )

    else:

        profitable_year_pct = 0.0

    # ==============================================
    # CONSECUTIVE LOSING CALENDAR YEARS
    #
    # Zero-trade years break the losing streak.
    # ==============================================

    longest_losing_year_streak = 0
    current_losing_year_streak = 0

    for year in range(
        first_year,
        last_year + 1
    ):

        values = yearly[
            year
        ]

        if (
            values[
                "trades"
            ] > 0

            and

            values[
                "total_r"
            ] < 0
        ):

            current_losing_year_streak += 1

            longest_losing_year_streak = max(

                longest_losing_year_streak,

                current_losing_year_streak
            )

        else:

            current_losing_year_streak = 0

    return (
        yearly,
        {

            "active_years":
                len(
                    active_years
                ),

            "profitable_active_years":
                len(
                    profitable_active_years
                ),

            "losing_active_years":
                len(
                    losing_active_years
                ),

            "flat_active_years":
                len(
                    flat_active_years
                ),

            "zero_trade_years":
                len(
                    zero_trade_years
                ),

            "profitable_active_year_pct":
                round(
                    profitable_year_pct,
                    1
                ),

            "median_active_year_r":
                round(
                    median_annual_r,
                    2
                ),

            "worst_active_year_r":
                round(
                    worst_year_r,
                    2
                ),

            "best_active_year_r":
                round(
                    best_year_r,
                    2
                ),

            "longest_losing_year_streak":
                longest_losing_year_streak
        }
    )


# ==================================================
# ROLLING 3-YEAR CONSISTENCY
# ==================================================

def calculate_rolling_3y_stats(
    yearly
):

    years = sorted(
        yearly.keys()
    )

    rolling_returns = []

    for i in range(
        len(years) - 2
    ):

        y1 = years[
            i
        ]

        y2 = years[
            i + 1
        ]

        y3 = years[
            i + 2
        ]

        # Ensure consecutive calendar years.
        if not (
            y2 == y1 + 1
            and
            y3 == y2 + 1
        ):

            continue

        total_r = (

            yearly[
                y1
            ][
                "total_r"
            ]

            + yearly[
                y2
            ][
                "total_r"
            ]

            + yearly[
                y3
            ][
                "total_r"
            ]
        )

        rolling_returns.append(
            total_r
        )

    if not rolling_returns:

        return {

            "rolling_3y_windows":
                0,

            "profitable_rolling_3y_windows":
                0,

            "profitable_rolling_3y_pct":
                0.0,

            "worst_rolling_3y_r":
                0.0,

            "median_rolling_3y_r":
                0.0,

            "best_rolling_3y_r":
                0.0
        }

    profitable = sum(

        1

        for value in rolling_returns

        if value > 0
    )

    return {

        "rolling_3y_windows":
            len(
                rolling_returns
            ),

        "profitable_rolling_3y_windows":
            profitable,

        "profitable_rolling_3y_pct":
            round(
                profitable
                / len(
                    rolling_returns
                )
                * 100.0,
                1
            ),

        "worst_rolling_3y_r":
            round(
                min(
                    rolling_returns
                ),
                2
            ),

        "median_rolling_3y_r":
            round(
                float(
                    pd.Series(
                        rolling_returns
                    ).median()
                ),
                2
            ),

        "best_rolling_3y_r":
            round(
                max(
                    rolling_returns
                ),
                2
            )
    }


# ==================================================
# ERA CONSISTENCY
# ==================================================

def calculate_era_stats(
    trades
):

    era_returns = []
    era_pfs = []
    profitable_eras = 0

    era_details = {}

    for (
        era_name,
        start,
        end
    ) in ERAS:

        era_trades = [

            trade

            for trade in trades

            if (
                trade[
                    "signal_time"
                ] >= start

                and

                trade[
                    "signal_time"
                ] < end
            )
        ]

        stats = calculate_basic_stats(
            era_trades
        )

        era_details[
            era_name
        ] = stats

        era_returns.append(
            stats[
                "total_r"
            ]
        )

        if (
            stats[
                "trades"
            ] > 0
        ):

            era_pfs.append(
                stats[
                    "profit_factor"
                ]
            )

        if (
            stats[
                "total_r"
            ] > 0
        ):

            profitable_eras += 1

    active_era_pfs = [

        pf

        for pf in era_pfs

        if pf != 999.0
    ]

    worst_era_pf = (
        min(
            active_era_pfs
        )
        if active_era_pfs
        else 0.0
    )

    return (
        era_details,
        {

            "profitable_eras":
                profitable_eras,

            "total_eras":
                len(
                    ERAS
                ),

            "worst_era_r":
                round(
                    min(
                        era_returns
                    ),
                    2
                ),

            "best_era_r":
                round(
                    max(
                        era_returns
                    ),
                    2
                ),

            "worst_era_pf":
                round(
                    worst_era_pf,
                    3
                )
        }
    )


# ==================================================
# FULL CONSISTENCY STATS
# ==================================================

def calculate_full_stats(
    trades
):

    stats = calculate_basic_stats(
        trades
    )

    years = (

        (
            RESEARCH_TO
            - RESEARCH_FROM
        ).total_seconds()

        /

        (
            365.2425
            * 24
            * 60
            * 60
        )
    )

    trades_per_year = (

        stats[
            "trades"
        ]
        / years

        if years > 0

        else 0.0
    )

    yearly, yearly_stats = (
        calculate_yearly_stats(
            trades
        )
    )

    rolling_stats = (
        calculate_rolling_3y_stats(
            yearly
        )
    )

    era_details, era_stats = (
        calculate_era_stats(
            trades
        )
    )

    stats[
        "trades_per_year"
    ] = round(
        trades_per_year,
        2
    )

    stats.update(
        yearly_stats
    )

    stats.update(
        rolling_stats
    )

    stats.update(
        era_stats
    )

    return (
        stats,
        yearly,
        era_details
    )


# ==================================================
# LABEL
# ==================================================

def excluded_hours_label(
    excluded_hours
):

    if not excluded_hours:

        return "NONE"

    return ",".join(

        f"{hour:02d}:00"

        for hour in excluded_hours
    )


# ==================================================
# RESEARCH
# ==================================================

def run_research():

    global RESEARCH_STATUS

    try:

        print()
        print(
            "========================================"
        )
        print(
            "EUR/USD FREQUENCY / CONSISTENCY SWEEP"
        )
        print(
            "========================================"
        )
        print()

        print(
            "Objective:"
        )

        print(
            "More trades + smoother returns "
            "without destroying the edge."
        )

        print()

        print(
            "Structure fixed:",
            STRUCTURE_LOOKBACK
        )

        print(
            "Slow EMA fixed:",
            SLOW_EMA_LENGTH
        )

        print(
            "RR fixed:",
            REWARD_RISK
        )

        print(
            "Weekdays: ALL"
        )

        print(
            "Total combinations:",
            TOTAL_COMBINATIONS
        )

        # ==========================================
        # FETCH
        # ==========================================

        RESEARCH_STATUS.update({

            "state":
                "fetching_data",

            "message":
                "Fetching full EUR/USD history"
        })

        h1 = fetch_chunked_history(

            INSTRUMENT,
            "H1",

            RESEARCH_FROM
            - timedelta(
                days=
                    H1_WARMUP_DAYS
            ),

            RESEARCH_TO
        )

        daily = fetch_chunked_history(

            INSTRUMENT,
            "D",

            RESEARCH_FROM
            - timedelta(
                days=
                    DAILY_WARMUP_DAYS
            ),

            RESEARCH_TO
        )

        print(
            "H1 candles:",
            len(
                h1
            )
        )

        print(
            "Daily candles:",
            len(
                daily
            )
        )

        # ==========================================
        # PRECOMPUTE
        # ==========================================

        RESEARCH_STATUS.update({

            "state":
                "precomputing",

            "message":
                "Building indicator and signal cache"
        })

        h1_atr = atr_series(
            h1,
            14
        )

        daily_state = (
            build_daily_state(
                daily
            )
        )

        daily_lookup = (
            build_h1_daily_lookup(

                h1,
                daily,
                daily_state
            )
        )

        candidates = (
            build_base_candidates(

                h1,
                h1_atr,
                daily_lookup
            )
        )

        RESEARCH_STATUS[
            "base_signal_candidates"
        ] = len(
            candidates
        )

        print(
            "Base bearish engulfings:",
            len(
                candidates
            )
        )

        # ==========================================
        # GRID
        # ==========================================

        combinations = itertools.product(

            BODY_RATIOS,

            RECENT_HIGH_DISTANCE_ATR_VALUES,

            FAST_EMA_LENGTHS,

            STRONG_CLOSE_LEVELS,

            EMA_SEPARATION_ATR_VALUES,

            TIMING_VARIANTS.items()
        )

        RESEARCH_STATUS.update({

            "state":
                "running",

            "message":
                (
                    "Running frequency / "
                    "consistency sweep"
                ),

            "completed_combinations":
                0
        })

        summary_rows = []

        yearly_rows = []

        for number, combo in enumerate(
            combinations,
            start=1
        ):

            (
                body_ratio,
                recent_high_distance,
                fast_ema,
                strong_close,
                ema_separation,
                timing_item
            ) = combo

            (
                timing_name,
                excluded_hours
            ) = timing_item

            eligible = [

                candidate

                for candidate in candidates

                if candidate_allowed(

                    candidate,

                    body_ratio,

                    recent_high_distance,

                    fast_ema,

                    strong_close,

                    ema_separation,

                    excluded_hours
                )
            ]

            trades, still_open, ignored = (
                simulate(
                    h1,
                    eligible
                )
            )

            stats, yearly, era_details = (
                calculate_full_stats(
                    trades
                )
            )

            config_id = number

            row = {

                "config_id":
                    config_id,

                "body_ratio":
                    body_ratio,

                "structure_lookback":
                    STRUCTURE_LOOKBACK,

                "recent_high_distance_atr":
                    recent_high_distance,

                "reward_risk":
                    REWARD_RISK,

                "slow_ema":
                    SLOW_EMA_LENGTH,

                "fast_ema":
                    fast_ema,

                "strong_bearish_close":
                    strong_close,

                "ema_separation_atr":
                    (
                        "OFF"

                        if ema_separation
                        is None

                        else ema_separation
                    ),

                "timing_variant":
                    timing_name,

                "excluded_ny_hours":
                    excluded_hours_label(
                        excluded_hours
                    ),

                "raw_signals":
                    len(
                        eligible
                    ),

                "ignored_due_to_open_trade":
                    ignored,

                "still_open_at_end":
                    still_open
            }

            row.update(
                stats
            )

            # ======================================
            # ERA DETAIL COLUMNS
            # ======================================

            for (
                era_name,
                era_stats
            ) in era_details.items():

                row[
                    f"{era_name}_trades"
                ] = era_stats[
                    "trades"
                ]

                row[
                    f"{era_name}_pf"
                ] = era_stats[
                    "profit_factor"
                ]

                row[
                    f"{era_name}_total_r"
                ] = era_stats[
                    "total_r"
                ]

                row[
                    f"{era_name}_expectancy"
                ] = era_stats[
                    "expectancy_r"
                ]

            summary_rows.append(
                row
            )

            # ======================================
            # YEARLY DETAIL OUTPUT
            # ======================================

            for (
                year,
                year_values
            ) in yearly.items():

                yearly_rows.append({

                    "config_id":
                        config_id,

                    "year":
                        year,

                    "body_ratio":
                        body_ratio,

                    "recent_high_distance_atr":
                        recent_high_distance,

                    "fast_ema":
                        fast_ema,

                    "strong_bearish_close":
                        strong_close,

                    "ema_separation_atr":
                        (
                            "OFF"

                            if ema_separation
                            is None

                            else ema_separation
                        ),

                    "timing_variant":
                        timing_name,

                    "trades":
                        year_values[
                            "trades"
                        ],

                    "total_r":
                        round(
                            year_values[
                                "total_r"
                            ],
                            3
                        )
                })

            RESEARCH_STATUS[
                "completed_combinations"
            ] = number

            if (
                number % 100
                == 0
            ):

                print(

                    f"Progress: "
                    f"{number}/"
                    f"{TOTAL_COMBINATIONS}",

                    flush=True
                )

        # ==========================================
        # DATAFRAMES
        # ==========================================

        summary_df = pd.DataFrame(
            summary_rows
        )

        yearly_df = pd.DataFrame(
            yearly_rows
        )

        if summary_df.empty:

            raise RuntimeError(
                "No strategy results generated"
            )

        # ==========================================
        # ADD USEFUL SCREENING FLAGS
        # ==========================================

        summary_df[
            "pf_above_1_5"
        ] = (
            summary_df[
                "profit_factor"
            ] >= 1.50
        )

        summary_df[
            "pf_above_1_7"
        ] = (
            summary_df[
                "profit_factor"
            ] >= 1.70
        )

        summary_df[
            "at_least_5_trades_per_year"
        ] = (
            summary_df[
                "trades_per_year"
            ] >= 5.0
        )

        summary_df[
            "at_least_6_trades_per_year"
        ] = (
            summary_df[
                "trades_per_year"
            ] >= 6.0
        )

        summary_df[
            "all_eras_profitable"
        ] = (
            summary_df[
                "profitable_eras"
            ] == len(
                ERAS
            )
        )

        summary_df[
            "no_negative_rolling_3y"
        ] = (
            summary_df[
                "worst_rolling_3y_r"
            ] >= 0
        )

        # ==========================================
        # CONSISTENCY SCORE
        #
        # This is deliberately NOT a pure
        # optimisation target.
        #
        # It is just a useful sorting aid.
        #
        # Rewards:
        # - expectancy
        # - frequency
        # - profitable active years
        # - profitable rolling 3y windows
        #
        # PF is capped at 3 so tiny-sample giant PF
        # doesn't dominate the score.
        # ==========================================

        capped_pf = (
            summary_df[
                "profit_factor"
            ]
            .clip(
                upper=3.0
            )
        )

        summary_df[
            "consistency_score"
        ] = (

            capped_pf

            * summary_df[
                "expectancy_r"
            ].clip(
                lower=0
            )

            * (
                summary_df[
                    "profitable_active_year_pct"
                ]
                / 100.0
            )

            * (
                summary_df[
                    "profitable_rolling_3y_pct"
                ]
                / 100.0
            )

            * (
                summary_df[
                    "trades_per_year"
                ]
                ** 0.5
            )

        ).round(
            4
        )

        # ==========================================
        # SORT SUMMARY
        # ==========================================

        summary_df = (
            summary_df
            .sort_values(

                by=[

                    "consistency_score",

                    "profitable_rolling_3y_pct",

                    "profitable_active_year_pct",

                    "profit_factor",

                    "trades_per_year"
                ],

                ascending=[

                    False,
                    False,
                    False,
                    False,
                    False
                ]
            )
        )

        # ==========================================
        # SAVE
        # ==========================================

        summary_df.to_csv(

            SUMMARY_OUTPUT_FILE,

            index=False
        )

        yearly_df.to_csv(

            YEARLY_OUTPUT_FILE,

            index=False
        )

        RESEARCH_STATUS.update({

            "state":
                "complete",

            "message":
                (
                    "Frequency / consistency "
                    "research completed successfully."
                ),

            "completed_combinations":
                TOTAL_COMBINATIONS,

            "rows_saved":
                len(
                    summary_df
                ),

            "summary_output_file":
                SUMMARY_OUTPUT_FILE,

            "yearly_output_file":
                YEARLY_OUTPUT_FILE
        })

        # ==========================================
        # PRINT CURRENT HIGH-PF REFERENCE AREA
        # ==========================================

        print()
        print(
            "========================================"
        )
        print(
            "TOP CONSISTENCY SCORE"
        )
        print(
            "========================================"
        )

        columns = [

            "config_id",

            "body_ratio",

            "recent_high_distance_atr",

            "fast_ema",

            "strong_bearish_close",

            "ema_separation_atr",

            "timing_variant",

            "trades",

            "trades_per_year",

            "profit_factor",

            "total_r",

            "expectancy_r",

            "profitable_active_year_pct",

            "losing_active_years",

            "median_active_year_r",

            "worst_active_year_r",

            "profitable_rolling_3y_pct",

            "worst_rolling_3y_r",

            "profitable_eras",

            "max_drawdown_r",

            "consistency_score"
        ]

        print(

            summary_df[
                columns
            ]
            .head(
                30
            )
            .to_string(
                index=False
            )
        )

        # ==========================================
        # HIGHER FREQUENCY + DECENT EDGE
        # ==========================================

        print()
        print(
            "========================================"
        )
        print(
            "PF >= 1.70 AND >= 5 TRADES/YEAR"
        )
        print(
            "========================================"
        )

        higher_frequency = (

            summary_df[
                (
                    summary_df[
                        "profit_factor"
                    ] >= 1.70
                )
                &
                (
                    summary_df[
                        "trades_per_year"
                    ] >= 5.0
                )
            ]
        )

        print(

            higher_frequency[
                columns
            ]
            .head(
                30
            )
            .to_string(
                index=False
            )
        )

        # ==========================================
        # VERY ROBUST ROLLING PERIODS
        # ==========================================

        print()
        print(
            "========================================"
        )
        print(
            "ALL ERAS PROFITABLE + "
            "NO NEGATIVE ROLLING 3-YEAR WINDOW"
        )
        print(
            "========================================"
        )

        very_robust = (

            summary_df[
                (
                    summary_df[
                        "all_eras_profitable"
                    ]
                )
                &
                (
                    summary_df[
                        "no_negative_rolling_3y"
                    ]
                )
            ]
        )

        print(

            very_robust[
                columns
            ]
            .head(
                30
            )
            .to_string(
                index=False
            )
        )

        print()
        print(
            "Saved:"
        )

        print(
            SUMMARY_OUTPUT_FILE
        )

        print(
            YEARLY_OUTPUT_FILE,
            flush=True
        )

    except Exception as error:

        RESEARCH_STATUS.update({

            "state":
                "error",

            "message":
                str(
                    error
                )
        })

        print(
            "ERROR:",
            error,
            flush=True
        )


# ==================================================
# ROUTES
# ==================================================

@app.route("/")
def home():

    return jsonify({

        "service":
            "EURUSD Frequency / Consistency Research",

        "status":
            RESEARCH_STATUS,

        "objective":
            (
                "Find a higher-frequency, "
                "more consistent strategy while "
                "retaining a healthy edge."
            ),

        "fixed":
            {

                "structure_lookback":
                    STRUCTURE_LOOKBACK,

                "slow_ema":
                    SLOW_EMA_LENGTH,

                "reward_risk":
                    REWARD_RISK,

                "weekday_exclusions":
                    "NONE"
            },

        "grid":
            {

                "body_ratios":
                    BODY_RATIOS,

                "recent_high_distance_atr":
                    RECENT_HIGH_DISTANCE_ATR_VALUES,

                "fast_ema":
                    FAST_EMA_LENGTHS,

                "strong_close":
                    STRONG_CLOSE_LEVELS,

                "ema_separation_atr":
                    [
                        "OFF"
                        if value is None
                        else value

                        for value
                        in EMA_SEPARATION_ATR_VALUES
                    ],

                "timing_variants":
                    {

                        name:
                            list(
                                hours
                            )

                        for name, hours
                        in TIMING_VARIANTS.items()
                    },

                "total_combinations":
                    TOTAL_COMBINATIONS
            },

        "summary_download":
            "/download-summary",

        "yearly_download":
            "/download-yearly",

        "trading_enabled":
            False,

        "orders_supported":
            False,

        "executor_connected":
            False
    })


@app.route("/status")
def status():

    return jsonify(
        RESEARCH_STATUS
    )


@app.route("/download-summary")
def download_summary():

    if not os.path.exists(
        SUMMARY_OUTPUT_FILE
    ):

        return jsonify({

            "status":
                "not_ready",

            "message":
                "Summary CSV is not ready yet."
        }), 404

    return send_file(

        SUMMARY_OUTPUT_FILE,

        as_attachment=True,

        download_name=
            "eurusd_short_frequency_consistency_sweep.csv"
    )


@app.route("/download-yearly")
def download_yearly():

    if not os.path.exists(
        YEARLY_OUTPUT_FILE
    ):

        return jsonify({

            "status":
                "not_ready",

            "message":
                "Yearly CSV is not ready yet."
        }), 404

    return send_file(

        YEARLY_OUTPUT_FILE,

        as_attachment=True,

        download_name=
            "eurusd_short_frequency_consistency_yearly.csv"
    )


# ==================================================
# START
# ==================================================

if __name__ == "__main__":

    research_thread = threading.Thread(

        target=
            run_research,

        name=
            "eurusd-frequency-consistency",

        daemon=True
    )

    research_thread.start()

    port = int(
        os.getenv(
            "PORT",
            5000
        )
    )

    app.run(

        host=
            "0.0.0.0",

        port=
            port,

        debug=
            False
    )
