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

OUTPUT_FILE = (
    "eurusd_short_individual_time_day_sweep.csv"
)


# ==================================================
# FROZEN STRUCTURAL CORE
# ==================================================

BODY_RATIO = 1.30

STRUCTURE_LOOKBACK = 55

RECENT_HIGH_DISTANCE_ATR = 0.25

REWARD_RISK = 4.00

SLOW_EMA_LENGTH = 100

FAST_EMA_LENGTH = 85

STRONG_CLOSE_LEVEL = 0.275

EMA_SEPARATION_ATR = 0.05


# ==================================================
# TIMING TESTS
#
# First pass ONLY:
#
# 1 baseline
# 24 individual NY-hour exclusions
# 5 individual weekday exclusions
#
# Total = 30 tests
# ==================================================

WEEKDAY_NAMES = {
    0: "MONDAY",
    1: "TUESDAY",
    2: "WEDNESDAY",
    3: "THURSDAY",
    4: "FRIDAY"
}

TOTAL_TESTS = 30


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

    "completed_tests":
        0,

    "total_tests":
        TOTAL_TESTS,

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
# DAILY INDICATORS
# ==================================================

def build_daily_state(
    daily
):

    closes = [
        candle["close"]
        for candle in daily
    ]

    slow_ema = ema_series(
        closes,
        SLOW_EMA_LENGTH
    )

    fast_ema = ema_series(
        closes,
        FAST_EMA_LENGTH
    )

    daily_atr = atr_series(
        daily,
        14
    )

    return {

        "slow_ema":
            slow_ema,

        "fast_ema":
            fast_ema,

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

        lookup[
            h1_index
        ] = {

            "close":
                daily[
                    daily_index
                ]["close"],

            "slow_ema":
                daily_state[
                    "slow_ema"
                ][daily_index],

            "fast_ema":
                daily_state[
                    "fast_ema"
                ][daily_index],

            "daily_atr":
                daily_state[
                    "daily_atr"
                ][daily_index]
        }

    return lookup


# ==================================================
# BUILD FROZEN CORE SIGNALS
# ==================================================

def build_core_candidates(
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
            or
            daily is None
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
            or
            current_body <= 0
            or
            signal_range <= 0
        ):

            continue

        # ==========================================
        # BEARISH ENGULF
        # ==========================================

        bearish_engulfing = (

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
        )

        if not bearish_engulfing:

            continue

        # ==========================================
        # BODY RATIO
        # ==========================================

        if (
            current_body
            < previous_body
            * BODY_RATIO
        ):

            continue

        # ==========================================
        # STRONG BEARISH CLOSE
        # ==========================================

        close_location = (
            (
                signal["close"]
                - signal["low"]
            )
            / signal_range
        )

        if (
            close_location
            > STRONG_CLOSE_LEVEL
        ):

            continue

        # ==========================================
        # RECENT HIGH STRUCTURE
        # ==========================================

        previous_highest = max(

            candle["high"]

            for candle in h1[
                index
                - STRUCTURE_LOOKBACK:
                index
            ]
        )

        distance_from_high = (
            previous_highest
            - signal["high"]
        )

        if (
            distance_from_high
            > current_atr
            * RECENT_HIGH_DISTANCE_ATR
        ):

            continue

        # ==========================================
        # DAILY REGIME
        # ==========================================

        slow_ema = daily[
            "slow_ema"
        ]

        fast_ema = daily[
            "fast_ema"
        ]

        daily_atr = daily[
            "daily_atr"
        ]

        if (
            slow_ema is None
            or
            fast_ema is None
            or
            daily_atr is None
            or
            daily_atr <= 0
        ):

            continue

        if not (
            daily["close"]
            < slow_ema
        ):

            continue

        if not (
            fast_ema
            < slow_ema
        ):

            continue

        # ==========================================
        # EMA SEPARATION
        # ==========================================

        separation = (
            slow_ema
            - fast_ema
        ) / daily_atr

        if (
            separation
            < EMA_SEPARATION_ATR
        ):

            continue

        # ==========================================
        # LOCAL TIME INFORMATION
        # ==========================================

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

            "ny_hour":
                ny_time.hour,

            "ny_weekday":
                ny_time.weekday()
        })

    return candidates


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

        EXIT_CACHE[
            signal_index
        ] = None

        return None

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

        EXIT_CACHE[
            signal_index
        ] = None

        return None

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
            or
            target_hit
        ):

            continue

        if (
            stop_hit
            and
            target_hit
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

            "signal_index":
                signal_index,

            "exit_index":
                index,

            "signal_time":
                signal["time"],

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

    EXIT_CACHE[
        signal_index
    ] = None

    return None


# ==================================================
# SIMULATOR
# ==================================================

def simulate(
    h1,
    candidates
):

    trades = []

    position_exit_index = -1

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

            continue

        trade = calculate_trade_exit(
            h1,
            signal_index
        )

        if trade is None:

            continue

        trades.append(
            trade
        )

        position_exit_index = (
            trade[
                "exit_index"
            ]
        )

    return trades


# ==================================================
# STATS
# ==================================================

def calculate_stats(
    trades
):

    if not trades:

        return None

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

    profit_factor = (
        gross_profit
        / gross_loss
        if gross_loss > 0
        else float("inf")
    )

    win_rate = (
        len(winners)
        / len(results)
        * 100
    )

    expectancy = (
        total_r
        / len(results)
    )

    # ==============================================
    # DRAWDOWN
    # ==========================================

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
    # ==========================================

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
        len(results)
        / years
    )

    return {

        "trades":
            len(results),

        "trades_per_year":
            round(
                trades_per_year,
                2
            ),

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
# TEST HELPER
# ==================================================

def run_test(
    h1,
    candidates,
    filter_type,
    filter_name,
    excluded_hour=None,
    excluded_weekday=None
):

    filtered = []

    for candidate in candidates:

        if (
            excluded_hour is not None
            and
            candidate[
                "ny_hour"
            ] == excluded_hour
        ):

            continue

        if (
            excluded_weekday is not None
            and
            candidate[
                "ny_weekday"
            ] == excluded_weekday
        ):

            continue

        filtered.append(
            candidate
        )

    trades = simulate(
        h1,
        filtered
    )

    stats = calculate_stats(
        trades
    )

    if stats is None:

        return None

    row = {

        "filter_type":
            filter_type,

        "filter_name":
            filter_name,

        "excluded_ny_hour":
            (
                ""
                if excluded_hour
                is None
                else excluded_hour
            ),

        "excluded_weekday":
            (
                ""
                if excluded_weekday
                is None
                else WEEKDAY_NAMES[
                    excluded_weekday
                ]
            ),

        "raw_signals_after_filter":
            len(filtered)
    }

    row.update(
        stats
    )

    return row


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
            "EUR/USD INDIVIDUAL TIME / DAY SWEEP"
        )
        print(
            "========================================"
        )
        print()

        print(
            "STRUCTURAL CORE FROZEN"
        )

        print(
            "Testing timing filters independently."
        )

        print()

        # ==========================================
        # DATA
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

        # ==========================================
        # INDICATORS
        # ==========================================

        RESEARCH_STATUS.update({

            "state":
                "precomputing",

            "message":
                "Building frozen structural signals"
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
            build_core_candidates(
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
            "Frozen core raw signals:",
            len(candidates),
            flush=True
        )

        # ==========================================
        # TESTS
        # ==========================================

        RESEARCH_STATUS.update({

            "state":
                "running",

            "message":
                "Testing individual hour and weekday exclusions",

            "completed_tests":
                0
        })

        results = []

        test_number = 0

        # ==========================================
        # BASELINE
        # ==========================================

        baseline = run_test(

            h1,
            candidates,

            "BASELINE",

            "ALL_HOURS_ALL_WEEKDAYS"
        )

        if baseline is not None:

            results.append(
                baseline
            )

        test_number += 1

        RESEARCH_STATUS[
            "completed_tests"
        ] = test_number

        # ==========================================
        # INDIVIDUAL NY HOURS
        # ==========================================

        for hour in range(
            24
        ):

            name = (
                f"EXCLUDE_NY_"
                f"{hour:02d}:00_"
                f"{hour:02d}:59"
            )

            row = run_test(

                h1,
                candidates,

                "HOUR",

                name,

                excluded_hour=
                    hour
            )

            if row is not None:

                results.append(
                    row
                )

            test_number += 1

            RESEARCH_STATUS[
                "completed_tests"
            ] = test_number

        # ==========================================
        # INDIVIDUAL WEEKDAYS
        # ==========================================

        for weekday in range(
            5
        ):

            name = (
                f"EXCLUDE_"
                f"{WEEKDAY_NAMES[weekday]}"
            )

            row = run_test(

                h1,
                candidates,

                "WEEKDAY",

                name,

                excluded_weekday=
                    weekday
            )

            if row is not None:

                results.append(
                    row
                )

            test_number += 1

            RESEARCH_STATUS[
                "completed_tests"
            ] = test_number

        # ==========================================
        # SAVE
        # ==========================================

        df = pd.DataFrame(
            results
        )

        if df.empty:

            raise RuntimeError(
                "No timing results generated"
            )

        # Find baseline for improvement columns
        baseline_row = df[
            df[
                "filter_type"
            ] == "BASELINE"
        ].iloc[0]

        baseline_pf = (
            baseline_row[
                "profit_factor"
            ]
        )

        baseline_r = (
            baseline_row[
                "total_r"
            ]
        )

        baseline_dd = (
            baseline_row[
                "max_drawdown_r"
            ]
        )

        baseline_trades = (
            baseline_row[
                "trades"
            ]
        )

        df[
            "pf_change_vs_baseline"
        ] = (
            df[
                "profit_factor"
            ]
            - baseline_pf
        ).round(
            3
        )

        df[
            "total_r_change_vs_baseline"
        ] = (
            df[
                "total_r"
            ]
            - baseline_r
        ).round(
            2
        )

        df[
            "drawdown_change_vs_baseline"
        ] = (
            df[
                "max_drawdown_r"
            ]
            - baseline_dd
        ).round(
            2
        )

        df[
            "trades_removed"
        ] = (
            baseline_trades
            - df[
                "trades"
            ]
        )

        df = df.sort_values(

            by=[
                "profit_factor",
                "expectancy_r",
                "total_r"
            ],

            ascending=[
                False,
                False,
                False
            ]
        )

        df.to_csv(
            OUTPUT_FILE,
            index=False
        )

        RESEARCH_STATUS.update({

            "state":
                "complete",

            "message":
                (
                    "Individual timing-filter "
                    "sweep completed successfully."
                ),

            "completed_tests":
                TOTAL_TESTS,

            "rows_saved":
                len(df),

            "output_file":
                OUTPUT_FILE
        })

        print()
        print(
            "========================================"
        )
        print(
            "BASELINE"
        )
        print(
            "========================================"
        )

        print(
            baseline_row.to_string()
        )

        print()
        print(
            "========================================"
        )
        print(
            "BEST INDIVIDUAL EXCLUSIONS"
        )
        print(
            "========================================"
        )

        print(
            df[
                df[
                    "filter_type"
                ] != "BASELINE"
            ]
            .head(15)
            .to_string(
                index=False
            )
        )

        print()
        print(
            "Saved:",
            OUTPUT_FILE,
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
            "EURUSD Individual Time / Day Research",

        "status":
            RESEARCH_STATUS,

        "frozen_core":
            {

                "body_ratio":
                    BODY_RATIO,

                "structure_lookback":
                    STRUCTURE_LOOKBACK,

                "recent_high_distance_atr":
                    RECENT_HIGH_DISTANCE_ATR,

                "reward_risk":
                    REWARD_RISK,

                "slow_ema":
                    SLOW_EMA_LENGTH,

                "fast_ema":
                    FAST_EMA_LENGTH,

                "strong_close":
                    STRONG_CLOSE_LEVEL,

                "ema_separation_atr":
                    EMA_SEPARATION_ATR
            },

        "research":
            {

                "baseline":
                    True,

                "individual_ny_hours":
                    True,

                "individual_weekdays":
                    True,

                "hour_day_combinations":
                    False
            },

        "trading_enabled":
            False,

        "orders_supported":
            False,

        "executor_connected":
            False,

        "download_endpoint":
            "/download"
    })


@app.route("/status")
def status():

    return jsonify(
        RESEARCH_STATUS
    )


@app.route("/download")
def download():

    if not os.path.exists(
        OUTPUT_FILE
    ):

        return jsonify({

            "status":
                "not_ready",

            "message":
                "CSV has not been generated yet."
        }), 404

    return send_file(

        OUTPUT_FILE,

        as_attachment=True,

        download_name=
            "eurusd_short_individual_time_day_sweep.csv"
    )


# ==================================================
# START
# ==================================================

if __name__ == "__main__":

    research_thread = threading.Thread(

        target=
            run_research,

        name=
            "eurusd-individual-timing",

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
