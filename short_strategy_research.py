import os
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


# ==================================================
# OUTPUT FILES
# ==================================================

SUMMARY_FILE = (
    "eurusd_short_three_finalists_summary.csv"
)

ERA_FILE = (
    "eurusd_short_three_finalists_eras.csv"
)

YEARLY_FILE = (
    "eurusd_short_three_finalists_yearly.csv"
)

ROLLING_FILE = (
    "eurusd_short_three_finalists_rolling_3y.csv"
)

TRADES_FILE = (
    "eurusd_short_three_finalists_trades.csv"
)


# ==================================================
# FIXED STRATEGY COMPONENTS
# ==================================================

STRUCTURE_LOOKBACK = 55

SLOW_EMA_LENGTH = 100

REWARD_RISK = 4.00

EXCLUDED_NY_HOURS = (
    2,
    10,
    12,
    14
)


# ==================================================
# THREE FINALISTS
# ==================================================

FINALISTS = {

    # ----------------------------------------------
    # Original highly selective version
    #
    # Historical reference:
    # ~79 trades
    # ~3.25 / year
    # PF ~2.59
    # ----------------------------------------------

    "LOW_FREQ_HIGH_PF": {

        "body_ratio":
            1.30,

        "recent_high_distance_atr":
            0.25,

        "fast_ema":
            85,

        "strong_close":
            0.275,

        "ema_separation_atr":
            0.05
    },

    # ----------------------------------------------
    # Config 815
    #
    # Historical reference:
    # 122 trades
    # ~5.02 / year
    # PF ~2.08
    # +85.4R
    # ----------------------------------------------

    "BALANCED_815": {

        "body_ratio":
            1.10,

        "recent_high_distance_atr":
            0.35,

        "fast_ema":
            85,

        "strong_close":
            0.275,

        "ema_separation_atr":
            0.05
    },

    # ----------------------------------------------
    # Config 965
    #
    # Historical reference:
    # 148 trades
    # ~6.09 / year
    # PF ~1.73
    # +73.98R
    # ----------------------------------------------

    "HIGH_FREQ_965": {

        "body_ratio":
            1.10,

        "recent_high_distance_atr":
            0.40,

        "fast_ema":
            75,

        "strong_close":
            0.275,

        "ema_separation_atr":
            None
    }
}


# ==================================================
# ERAS
# ==================================================

ERAS = [

    (
        "2002_2009",
        datetime(
            2002, 5, 6, 20, 0,
            tzinfo=timezone.utc
        ),
        datetime(
            2010, 1, 1,
            tzinfo=timezone.utc
        )
    ),

    (
        "2010_2017",
        datetime(
            2010, 1, 1,
            tzinfo=timezone.utc
        ),
        datetime(
            2018, 1, 1,
            tzinfo=timezone.utc
        )
    ),

    (
        "2018_2023",
        datetime(
            2018, 1, 1,
            tzinfo=timezone.utc
        ),
        datetime(
            2024, 1, 1,
            tzinfo=timezone.utc
        )
    ),

    (
        "2024_PRESENT",
        datetime(
            2024, 1, 1,
            tzinfo=timezone.utc
        ),
        RESEARCH_TO
    )
]


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

    "finalists":
        list(
            FINALISTS.keys()
        ),

    "completed_finalists":
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
        key=lambda x:
            x["time"]
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

    required_lengths = sorted(
        set(
            [
                SLOW_EMA_LENGTH
            ]
            +
            [
                config[
                    "fast_ema"
                ]

                for config
                in FINALISTS.values()
            ]
        )
    )

    ema_cache = {}

    for length in required_lengths:

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
# BASE BEARISH ENGULF CANDIDATES
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
        # BEARISH ENGULFING
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

            "ny_weekday":
                ny_time.weekday(),

            "daily":
                daily
        })

    return candidates


# ==================================================
# FINALIST FILTER
# ==================================================

def candidate_allowed(
    candidate,
    config
):

    if (
        candidate[
            "body_ratio"
        ]
        < config[
            "body_ratio"
        ]
    ):

        return False

    if (
        candidate[
            "close_location"
        ]
        > config[
            "strong_close"
        ]
    ):

        return False

    if (
        candidate[
            "recent_high_distance_atr"
        ]
        > config[
            "recent_high_distance_atr"
        ]
    ):

        return False

    # ==============================================
    # TIME FILTER
    # ==============================================

    if (
        candidate[
            "ny_hour"
        ]
        in EXCLUDED_NY_HOURS
    ):

        return False

    # All weekdays remain enabled.

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
        f"ema_{config['fast_ema']}"
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

    # Previous completed daily close < EMA100
    if not (
        daily["close"]
        < slow_ema
    ):

        return False

    # Fast EMA < EMA100
    if not (
        fast_ema
        < slow_ema
    ):

        return False

    # ==============================================
    # OPTIONAL EMA SEPARATION
    # ==============================================

    separation_threshold = (
        config[
            "ema_separation_atr"
        ]
    )

    if (
        separation_threshold
        is not None
    ):

        separation = (

            slow_ema
            - fast_ema

        ) / daily_atr

        if (
            separation
            < separation_threshold
        ):

            return False

    return True


# ==================================================
# TRADE EXIT CACHE
#
# RR is fixed across all finalists, so one cache
# can safely be shared.
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

    # Short adverse slippage:
    # fill 5 ticks below signal close.
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
            "Invalid short reference risk"
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
            "Invalid short actual risk"
        )

    # Entry occurs on signal close,
    # so exit checking begins on next H1.
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

        # ==========================================
        # SAME H1 STOP + TARGET
        # ==============================================

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

        trade = {

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

            "reference_entry":
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
                result_r
        }

        EXIT_CACHE[
            signal_index
        ] = trade

        return trade

    # ==============================================
    # STILL OPEN AT END OF HISTORY
    # ==============================================

    trade = {

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

        "reference_entry":
            reference_entry,

        "backtest_entry":
            backtest_entry,

        "stop":
            stop,

        "target":
            target,

        "exit_price":
            None,

        "exit_reason":
            None,

        "result_r":
            None
    }

    EXIT_CACHE[
        signal_index
    ] = trade

    return trade


# ==================================================
# SIMULATOR
# ==================================================

def simulate(
    h1,
    candidates
):

    trades = []

    position_exit_index = -1

    ignored_signals = 0

    still_open = False

    for candidate in candidates:

        signal_index = (
            candidate[
                "index"
            ]
        )

        # Existing position still open.
        #
        # "<" allows a new signal on the same H1
        # candle in which the previous trade exits,
        # matching our original simulator.
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

            # Pyramiding = 0:
            # this position blocks every later
            # signal until end of available data.
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
        ignored_signals,
        still_open
    )


# ==================================================
# BASIC STATISTICS
# ==================================================

def calculate_stats(
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

        result

        for result in results

        if result > 0
    ]

    losers = [

        result

        for result in results

        if result < 0
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

    expectancy = (

        total_r
        / len(results)
    )

    win_rate = (

        len(winners)
        / len(results)

        * 100.0
    )

    # ==============================================
    # MAX DRAWDOWN
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

        drawdown = (
            equity
            - peak
        )

        max_drawdown = min(
            max_drawdown,
            drawdown
        )

    # ==============================================
    # LONGEST LOSING TRADE STREAK
    # ==============================================

    longest_loss_streak = 0

    current_loss_streak = 0

    for result in results:

        if result < 0:

            current_loss_streak += 1

            longest_loss_streak = max(

                longest_loss_streak,

                current_loss_streak
            )

        else:

            current_loss_streak = 0

    return {

        "trades":
            len(
                results
            ),

        "winners":
            len(
                winners
            ),

        "losers":
            len(
                losers
            ),

        "win_rate":
            round(
                win_rate,
                2
            ),

        "profit_factor":
            (
                round(
                    profit_factor,
                    3
                )

                if profit_factor
                != float("inf")

                else 999.0
            ),

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
            longest_loss_streak
    }


# ==================================================
# YEARLY RESULTS
#
# Trade assigned according to signal date.
# ==================================================

def build_yearly_results(
    strategy_name,
    trades
):

    rows = []

    first_year = (
        RESEARCH_FROM.year
    )

    final_year = (
        RESEARCH_TO.year
    )

    for year in range(
        first_year,
        final_year + 1
    ):

        year_trades = [

            trade

            for trade in trades

            if (
                trade[
                    "signal_time"
                ].year
                == year
            )
        ]

        stats = calculate_stats(
            year_trades
        )

        rows.append({

            "strategy":
                strategy_name,

            "year":
                year,

            **stats
        })

    return rows


# ==================================================
# ERA RESULTS
# ==================================================

def build_era_results(
    strategy_name,
    trades
):

    rows = []

    for (
        era_name,
        era_start,
        era_end
    ) in ERAS:

        era_trades = [

            trade

            for trade in trades

            if (
                trade[
                    "signal_time"
                ]
                >= era_start

                and

                trade[
                    "signal_time"
                ]
                < era_end
            )
        ]

        stats = calculate_stats(
            era_trades
        )

        rows.append({

            "strategy":
                strategy_name,

            "era":
                era_name,

            "start":
                era_start.isoformat(),

            "end":
                era_end.isoformat(),

            **stats
        })

    return rows


# ==================================================
# ROLLING 3-YEAR RESULTS
# ==================================================

def build_rolling_results(
    strategy_name,
    yearly_rows
):

    strategy_years = [

        row

        for row in yearly_rows

        if row[
            "strategy"
        ] == strategy_name
    ]

    by_year = {

        row[
            "year"
        ]:
            row

        for row in strategy_years
    }

    years = sorted(
        by_year.keys()
    )

    rows = []

    for index in range(
        len(years) - 2
    ):

        y1 = years[
            index
        ]

        y2 = years[
            index + 1
        ]

        y3 = years[
            index + 2
        ]

        if not (

            y2 == y1 + 1

            and

            y3 == y2 + 1
        ):

            continue

        total_r = (

            by_year[
                y1
            ][
                "total_r"
            ]

            + by_year[
                y2
            ][
                "total_r"
            ]

            + by_year[
                y3
            ][
                "total_r"
            ]
        )

        total_trades = (

            by_year[
                y1
            ][
                "trades"
            ]

            + by_year[
                y2
            ][
                "trades"
            ]

            + by_year[
                y3
            ][
                "trades"
            ]
        )

        rows.append({

            "strategy":
                strategy_name,

            "start_year":
                y1,

            "end_year":
                y3,

            "trades":
                total_trades,

            "total_r":
                round(
                    total_r,
                    2
                ),

            "profitable":
                total_r > 0
        })

    return rows


# ==================================================
# CONSISTENCY SUMMARY
# ==================================================

def calculate_consistency_summary(
    strategy_name,
    full_stats,
    yearly_rows,
    era_rows,
    rolling_rows
):

    active_years = [

        row

        for row in yearly_rows

        if (
            row[
                "strategy"
            ]
            == strategy_name

            and

            row[
                "trades"
            ]
            > 0
        )
    ]

    profitable_years = [

        row

        for row in active_years

        if (
            row[
                "total_r"
            ]
            > 0
        )
    ]

    losing_years = [

        row

        for row in active_years

        if (
            row[
                "total_r"
            ]
            < 0
        )
    ]

    zero_trade_years = [

        row

        for row in yearly_rows

        if (
            row[
                "strategy"
            ]
            == strategy_name

            and

            row[
                "trades"
            ]
            == 0
        )
    ]

    active_returns = [

        row[
            "total_r"
        ]

        for row in active_years
    ]

    if active_returns:

        median_year = float(
            pd.Series(
                active_returns
            ).median()
        )

        worst_year = min(
            active_returns
        )

        best_year = max(
            active_returns
        )

    else:

        median_year = 0.0
        worst_year = 0.0
        best_year = 0.0

    profitable_year_pct = (

        len(
            profitable_years
        )

        / len(
            active_years
        )

        * 100.0

        if active_years

        else 0.0
    )

    # ==============================================
    # CONSECUTIVE LOSING YEARS
    # ==============================================

    relevant_years = [

        row

        for row in yearly_rows

        if row[
            "strategy"
        ] == strategy_name
    ]

    relevant_years.sort(
        key=lambda row:
            row[
                "year"
            ]
    )

    current_streak = 0

    longest_streak = 0

    for row in relevant_years:

        if (
            row[
                "trades"
            ] > 0

            and

            row[
                "total_r"
            ] < 0
        ):

            current_streak += 1

            longest_streak = max(

                longest_streak,

                current_streak
            )

        else:

            current_streak = 0

    # ==============================================
    # ROLLING
    # ==============================================

    strategy_rolling = [

        row

        for row in rolling_rows

        if row[
            "strategy"
        ] == strategy_name
    ]

    profitable_rolling = [

        row

        for row in strategy_rolling

        if row[
            "profitable"
        ]
    ]

    rolling_pct = (

        len(
            profitable_rolling
        )

        / len(
            strategy_rolling
        )

        * 100.0

        if strategy_rolling

        else 0.0
    )

    rolling_returns = [

        row[
            "total_r"
        ]

        for row in strategy_rolling
    ]

    # ==============================================
    # ERAS
    # ==============================================

    strategy_eras = [

        row

        for row in era_rows

        if row[
            "strategy"
        ] == strategy_name
    ]

    profitable_eras = [

        row

        for row in strategy_eras

        if row[
            "total_r"
        ] > 0
    ]

    era_pfs = [

        row[
            "profit_factor"
        ]

        for row in strategy_eras

        if (
            row[
                "trades"
            ] > 0

            and

            row[
                "profit_factor"
            ] != 999.0
        )
    ]

    years_elapsed = (

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

    return {

        **full_stats,

        "trades_per_year":
            round(
                full_stats[
                    "trades"
                ]
                / years_elapsed,
                2
            ),

        "active_years":
            len(
                active_years
            ),

        "profitable_active_years":
            len(
                profitable_years
            ),

        "losing_active_years":
            len(
                losing_years
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
                median_year,
                2
            ),

        "worst_active_year_r":
            round(
                worst_year,
                2
            ),

        "best_active_year_r":
            round(
                best_year,
                2
            ),

        "longest_losing_year_streak":
            longest_streak,

        "rolling_3y_windows":
            len(
                strategy_rolling
            ),

        "profitable_rolling_3y_windows":
            len(
                profitable_rolling
            ),

        "profitable_rolling_3y_pct":
            round(
                rolling_pct,
                1
            ),

        "worst_rolling_3y_r":
            (
                round(
                    min(
                        rolling_returns
                    ),
                    2
                )

                if rolling_returns

                else 0.0
            ),

        "median_rolling_3y_r":
            (
                round(
                    float(
                        pd.Series(
                            rolling_returns
                        ).median()
                    ),
                    2
                )

                if rolling_returns

                else 0.0
            ),

        "profitable_eras":
            len(
                profitable_eras
            ),

        "total_eras":
            len(
                strategy_eras
            ),

        "worst_era_pf":
            (
                round(
                    min(
                        era_pfs
                    ),
                    3
                )

                if era_pfs

                else 0.0
            ),

        "worst_era_r":
            (
                round(
                    min(
                        row[
                            "total_r"
                        ]

                        for row
                        in strategy_eras
                    ),
                    2
                )

                if strategy_eras

                else 0.0
            )
    }


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
            "EUR/USD THREE FINALISTS"
        )
        print(
            "========================================"
        )
        print()

        print(
            "Comparing:"
        )

        for name in FINALISTS:

            print(
                "-",
                name
            )

        print()
        print(
            "All weekdays enabled"
        )

        print(
            "Excluded NY hours:",
            EXCLUDED_NY_HOURS
        )

        # ==========================================
        # FETCH HISTORY
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
        # INDICATORS
        # ==========================================

        RESEARCH_STATUS.update({

            "state":
                "precomputing",

            "message":
                "Building indicators and signals"
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

        base_candidates = (
            build_base_candidates(

                h1,
                h1_atr,
                daily_lookup
            )
        )

        print(
            "Base bearish engulfings:",
            len(
                base_candidates
            )
        )

        # ==========================================
        # OUTPUT CONTAINERS
        # ==========================================

        summary_rows = []
        yearly_rows = []
        era_rows = []
        rolling_rows = []
        trade_rows = []

        RESEARCH_STATUS.update({

            "state":
                "running",

            "message":
                "Comparing three finalists"
        })

        # ==========================================
        # RUN EACH FINALIST
        # ==========================================

        for number, (
            strategy_name,
            config
        ) in enumerate(
            FINALISTS.items(),
            start=1
        ):

            print()
            print(
                "----------------------------------------"
            )

            print(
                strategy_name
            )

            print(
                "----------------------------------------"
            )

            eligible = [

                candidate

                for candidate
                in base_candidates

                if candidate_allowed(
                    candidate,
                    config
                )
            ]

            trades, ignored, still_open = (
                simulate(
                    h1,
                    eligible
                )
            )

            full_stats = calculate_stats(
                trades
            )

            # ======================================
            # YEARLY
            # ======================================

            strategy_yearly = (
                build_yearly_results(
                    strategy_name,
                    trades
                )
            )

            yearly_rows.extend(
                strategy_yearly
            )

            # ======================================
            # ERAS
            # ======================================

            strategy_eras = (
                build_era_results(
                    strategy_name,
                    trades
                )
            )

            era_rows.extend(
                strategy_eras
            )

            # ======================================
            # ROLLING 3Y
            # ======================================

            strategy_rolling = (
                build_rolling_results(
                    strategy_name,
                    strategy_yearly
                )
            )

            rolling_rows.extend(
                strategy_rolling
            )

            # ======================================
            # SUMMARY
            # ======================================

            consistency = (
                calculate_consistency_summary(

                    strategy_name,

                    full_stats,

                    strategy_yearly,

                    strategy_eras,

                    strategy_rolling
                )
            )

            summary_row = {

                "strategy":
                    strategy_name,

                "body_ratio":
                    config[
                        "body_ratio"
                    ],

                "structure_lookback":
                    STRUCTURE_LOOKBACK,

                "recent_high_distance_atr":
                    config[
                        "recent_high_distance_atr"
                    ],

                "reward_risk":
                    REWARD_RISK,

                "slow_ema":
                    SLOW_EMA_LENGTH,

                "fast_ema":
                    config[
                        "fast_ema"
                    ],

                "strong_bearish_close":
                    config[
                        "strong_close"
                    ],

                "ema_separation_atr":
                    (
                        "OFF"

                        if config[
                            "ema_separation_atr"
                        ]
                        is None

                        else config[
                            "ema_separation_atr"
                        ]
                    ),

                "excluded_ny_hours":
                    "02:00,10:00,12:00,14:00",

                "raw_signals":
                    len(
                        eligible
                    ),

                "ignored_due_to_open_trade":
                    ignored,

                "still_open_at_end":
                    still_open
            }

            summary_row.update(
                consistency
            )

            summary_rows.append(
                summary_row
            )

            # ======================================
            # TRADE EXPORT
            # ======================================

            for trade_number, trade in enumerate(
                trades,
                start=1
            ):

                signal_time = trade[
                    "signal_time"
                ]

                ny_time = (
                    signal_time
                    .astimezone(
                        NY_TZ
                    )
                )

                trade_rows.append({

                    "strategy":
                        strategy_name,

                    "trade_number":
                        trade_number,

                    "signal_time_utc":
                        signal_time.isoformat(),

                    "signal_time_ny":
                        ny_time.isoformat(),

                    "signal_ny_weekday":
                        ny_time.strftime(
                            "%A"
                        ),

                    "signal_ny_hour":
                        ny_time.hour,

                    "exit_time_utc":
                        trade[
                            "exit_time"
                        ].isoformat(),

                    "reference_entry":
                        trade[
                            "reference_entry"
                        ],

                    "backtest_entry":
                        trade[
                            "backtest_entry"
                        ],

                    "stop":
                        trade[
                            "stop"
                        ],

                    "target":
                        trade[
                            "target"
                        ],

                    "exit_price":
                        trade[
                            "exit_price"
                        ],

                    "exit_reason":
                        trade[
                            "exit_reason"
                        ],

                    "result_r":
                        round(
                            trade[
                                "result_r"
                            ],
                            6
                        )
                })

            print(
                "Trades:",
                full_stats[
                    "trades"
                ]
            )

            print(
                "PF:",
                full_stats[
                    "profit_factor"
                ]
            )

            print(
                "Total R:",
                full_stats[
                    "total_r"
                ]
            )

            print(
                "Expectancy:",
                full_stats[
                    "expectancy_r"
                ]
            )

            RESEARCH_STATUS[
                "completed_finalists"
            ] = number

        # ==========================================
        # DATAFRAMES
        # ==========================================

        summary_df = pd.DataFrame(
            summary_rows
        )

        era_df = pd.DataFrame(
            era_rows
        )

        yearly_df = pd.DataFrame(
            yearly_rows
        )

        rolling_df = pd.DataFrame(
            rolling_rows
        )

        trades_df = pd.DataFrame(
            trade_rows
        )

        # ==========================================
        # SORT
        # ==========================================

        summary_df = (
            summary_df
            .sort_values(

                by=[
                    "trades_per_year"
                ],

                ascending=True
            )
        )

        era_df = (
            era_df
            .sort_values(
                by=[
                    "era",
                    "strategy"
                ]
            )
        )

        yearly_df = (
            yearly_df
            .sort_values(
                by=[
                    "year",
                    "strategy"
                ]
            )
        )

        rolling_df = (
            rolling_df
            .sort_values(
                by=[
                    "start_year",
                    "strategy"
                ]
            )
        )

        # ==========================================
        # SAVE
        # ==========================================

        summary_df.to_csv(
            SUMMARY_FILE,
            index=False
        )

        era_df.to_csv(
            ERA_FILE,
            index=False
        )

        yearly_df.to_csv(
            YEARLY_FILE,
            index=False
        )

        rolling_df.to_csv(
            ROLLING_FILE,
            index=False
        )

        trades_df.to_csv(
            TRADES_FILE,
            index=False
        )

        # ==========================================
        # LOG SUMMARY
        # ==========================================

        display_columns = [

            "strategy",

            "trades",

            "trades_per_year",

            "winners",

            "losers",

            "win_rate",

            "profit_factor",

            "total_r",

            "expectancy_r",

            "max_drawdown_r",

            "longest_loss_streak",

            "active_years",

            "profitable_active_years",

            "losing_active_years",

            "profitable_active_year_pct",

            "median_active_year_r",

            "worst_active_year_r",

            "longest_losing_year_streak",

            "profitable_rolling_3y_pct",

            "worst_rolling_3y_r",

            "median_rolling_3y_r",

            "profitable_eras",

            "worst_era_pf"
        ]

        print()
        print(
            "========================================"
        )
        print(
            "FINAL COMPARISON"
        )
        print(
            "========================================"
        )

        print(

            summary_df[
                display_columns
            ]
            .to_string(
                index=False
            )
        )

        # ==========================================
        # COMPLETE
        # ==========================================

        RESEARCH_STATUS.update({

            "state":
                "complete",

            "message":
                (
                    "Three-finalist robustness "
                    "comparison completed."
                ),

            "summary_file":
                SUMMARY_FILE,

            "era_file":
                ERA_FILE,

            "yearly_file":
                YEARLY_FILE,

            "rolling_file":
                ROLLING_FILE,

            "trades_file":
                TRADES_FILE
        })

        print()
        print(
            "Saved:"
        )

        print(
            SUMMARY_FILE
        )

        print(
            ERA_FILE
        )

        print(
            YEARLY_FILE
        )

        print(
            ROLLING_FILE
        )

        print(
            TRADES_FILE,
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
            "EURUSD Three Finalists",

        "status":
            RESEARCH_STATUS,

        "finalists":
            FINALISTS,

        "fixed":
            {

                "structure":
                    STRUCTURE_LOOKBACK,

                "slow_ema":
                    SLOW_EMA_LENGTH,

                "reward_risk":
                    REWARD_RISK,

                "excluded_ny_hours":
                    list(
                        EXCLUDED_NY_HOURS
                    ),

                "weekday_exclusions":
                    "NONE"
            },

        "downloads":
            {

                "summary":
                    "/download-summary",

                "eras":
                    "/download-eras",

                "yearly":
                    "/download-yearly",

                "rolling_3y":
                    "/download-rolling",

                "trades":
                    "/download-trades"
            }
    })


@app.route("/status")
def status():

    return jsonify(
        RESEARCH_STATUS
    )


# ==================================================
# DOWNLOAD HELPER
# ==================================================

def send_csv(
    filename
):

    if not os.path.exists(
        filename
    ):

        return jsonify({

            "status":
                "not_ready",

            "message":
                (
                    f"{filename} "
                    f"is not ready yet."
                )
        }), 404

    return send_file(

        filename,

        as_attachment=True,

        download_name=
            filename
    )


@app.route("/download-summary")
def download_summary():

    return send_csv(
        SUMMARY_FILE
    )


@app.route("/download-eras")
def download_eras():

    return send_csv(
        ERA_FILE
    )


@app.route("/download-yearly")
def download_yearly():

    return send_csv(
        YEARLY_FILE
    )


@app.route("/download-rolling")
def download_rolling():

    return send_csv(
        ROLLING_FILE
    )


@app.route("/download-trades")
def download_trades():

    return send_csv(
        TRADES_FILE
    )


# ==================================================
# START
# ==================================================

if __name__ == "__main__":

    research_thread = threading.Thread(

        target=
            run_research,

        name=
            "eurusd-three-finalists",

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

        debug=False
    )
