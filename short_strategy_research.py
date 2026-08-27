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

OUTPUT_FILE = (
    "eurusd_short_combined_time_day_sweep.csv"
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
# TIMING CANDIDATES
# ==================================================

CANDIDATE_HOURS = [
    2,
    10,
    12,
    14
]


# weekday():
# Monday    = 0
# Tuesday   = 1
# Wednesday = 2
# Thursday  = 3
# Friday    = 4

WEEKDAY_STATES = [

    (
        "NONE",
        tuple()
    ),

    (
        "EXCLUDE_WEDNESDAY",
        (2,)
    ),

    (
        "EXCLUDE_TUESDAY",
        (1,)
    ),

    (
        "EXCLUDE_FRIDAY",
        (4,)
    ),

    (
        "EXCLUDE_TUESDAY_WEDNESDAY",
        (1, 2)
    ),

    (
        "EXCLUDE_WEDNESDAY_FRIDAY",
        (2, 4)
    )
]


WEEKDAY_NAMES = {

    0: "MONDAY",
    1: "TUESDAY",
    2: "WEDNESDAY",
    3: "THURSDAY",
    4: "FRIDAY"
}


# 16 possible subsets of four candidate hours.
HOUR_COMBINATIONS = []

for subset_size in range(
    len(CANDIDATE_HOURS) + 1
):

    for combination in itertools.combinations(
        CANDIDATE_HOURS,
        subset_size
    ):

        HOUR_COMBINATIONS.append(
            tuple(combination)
        )


TOTAL_TESTS = (
    len(HOUR_COMBINATIONS)
    * len(WEEKDAY_STATES)
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
# DAILY STATE
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
# BUILD FROZEN STRUCTURAL SIGNALS
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

        # ==========================================
        # BODY RATIO
        # ==========================================

        if (

            current_body

            <

            previous_body
            * BODY_RATIO
        ):

            continue

        # ==========================================
        # STRONG CLOSE
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
        # RECENT HIGH
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

            >

            current_atr
            * RECENT_HIGH_DISTANCE_ATR
        ):

            continue

        # ==========================================
        # DAILY FILTERS
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
        # NY TIME
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
# TRADE EXIT CACHE
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
    # entry is lower than signal close.
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

    # Entry occurs at signal close.
    # Earliest exit is next H1.
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
        # BOTH HIT SAME H1
        # ==========================================

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

    # ==============================================
    # STILL OPEN AT END OF DATA
    #
    # Important:
    # this trade must block later signals.
    # ==========================================

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

    for candidate in candidates:

        signal_index = (
            candidate[
                "index"
            ]
        )

        # Existing position has not yet exited.
        if (

            signal_index
            < position_exit_index
        ):

            continue

        trade = calculate_trade_exit(

            h1,
            signal_index
        )

        # If this signal remains open through the
        # end of the backtest, pyramiding=0 means
        # every subsequent signal must be ignored.
        if (
            trade["status"]
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
        still_open
    )


# ==================================================
# STATISTICS
# ==================================================

def calculate_stats(
    trades
):

    if not trades:

        return None

    results = [

        trade["result_r"]

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
    # LONGEST LOSS STREAK
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
# LABEL HELPERS
# ==================================================

def hour_label(
    excluded_hours
):

    if not excluded_hours:

        return "NONE"

    return ",".join(

        f"{hour:02d}:00"

        for hour in excluded_hours
    )


def weekday_label(
    weekdays
):

    if not weekdays:

        return "NONE"

    return ",".join(

        WEEKDAY_NAMES[
            day
        ]

        for day in weekdays
    )


# ==================================================
# RUN ONE TIMING CONFIGURATION
# ==================================================

def run_timing_test(
    h1,
    core_candidates,
    excluded_hours,
    excluded_weekdays
):

    eligible = []

    for candidate in core_candidates:

        if (

            candidate[
                "ny_hour"
            ]

            in excluded_hours
        ):

            continue

        if (

            candidate[
                "ny_weekday"
            ]

            in excluded_weekdays
        ):

            continue

        eligible.append(
            candidate
        )

    trades, still_open = simulate(
        h1,
        eligible
    )

    stats = calculate_stats(
        trades
    )

    if stats is None:

        return None

    row = {

        "excluded_hours":
            hour_label(
                excluded_hours
            ),

        "number_hours_excluded":
            len(
                excluded_hours
            ),

        "weekday_filter":
            weekday_label(
                excluded_weekdays
            ),

        "number_weekdays_excluded":
            len(
                excluded_weekdays
            ),

        "raw_signals_after_filter":
            len(
                eligible
            ),

        "still_open_at_end":
            still_open
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
            "EUR/USD COMBINED TIMING SWEEP"
        )
        print(
            "========================================"
        )
        print()

        print(
            "STRUCTURAL CORE FROZEN"
        )

        print(
            "Candidate NY hours:",
            CANDIDATE_HOURS
        )

        print(
            "Hour combinations:",
            len(
                HOUR_COMBINATIONS
            )
        )

        print(
            "Weekday states:",
            len(
                WEEKDAY_STATES
            )
        )

        print(
            "Total tests:",
            TOTAL_TESTS
        )

        # ==========================================
        # FETCH DATA
        # ==========================================

        RESEARCH_STATUS.update({

            "state":
                "fetching_data",

            "message":
                "Fetching EUR/USD history"
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
            len(h1)
        )

        print(
            "Daily candles:",
            len(daily)
        )

        # ==========================================
        # BUILD FROZEN CORE
        # ==========================================

        RESEARCH_STATUS.update({

            "state":
                "precomputing",

            "message":
                "Building frozen core signals"
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

        core_candidates = (
            build_core_candidates(

                h1,
                h1_atr,
                daily_lookup
            )
        )

        RESEARCH_STATUS[
            "base_signal_candidates"
        ] = len(
            core_candidates
        )

        print(
            "Frozen core raw signals:",
            len(
                core_candidates
            )
        )

        # ==========================================
        # TIMING TESTS
        # ==========================================

        RESEARCH_STATUS.update({

            "state":
                "running",

            "message":
                "Testing combined timing filters",

            "completed_tests":
                0
        })

        rows = []

        test_number = 0

        for (
            weekday_state_name,
            excluded_weekdays
        ) in WEEKDAY_STATES:

            for excluded_hours in (
                HOUR_COMBINATIONS
            ):

                row = run_timing_test(

                    h1,
                    core_candidates,

                    excluded_hours,

                    excluded_weekdays
                )

                if row is not None:

                    row[
                        "weekday_state_name"
                    ] = weekday_state_name

                    rows.append(
                        row
                    )

                test_number += 1

                RESEARCH_STATUS[
                    "completed_tests"
                ] = test_number

                if (
                    test_number % 10
                    == 0
                ):

                    print(

                        f"Progress: "
                        f"{test_number}/"
                        f"{TOTAL_TESTS}",

                        flush=True
                    )

        # ==========================================
        # DATAFRAME
        # ==========================================

        df = pd.DataFrame(
            rows
        )

        if df.empty:

            raise RuntimeError(
                "No timing results generated"
            )

        # ==========================================
        # FIND TRUE BASELINE
        # ==========================================

        baseline_matches = df[
            (
                df[
                    "number_hours_excluded"
                ] == 0
            )
            &
            (
                df[
                    "number_weekdays_excluded"
                ] == 0
            )
        ]

        if len(
            baseline_matches
        ) != 1:

            raise RuntimeError(
                "Could not uniquely identify baseline"
            )

        baseline = (
            baseline_matches
            .iloc[0]
        )

        baseline_pf = (
            baseline[
                "profit_factor"
            ]
        )

        baseline_r = (
            baseline[
                "total_r"
            ]
        )

        baseline_dd = (
            baseline[
                "max_drawdown_r"
            ]
        )

        baseline_expectancy = (
            baseline[
                "expectancy_r"
            ]
        )

        baseline_trades = (
            baseline[
                "trades"
            ]
        )

        # ==========================================
        # COMPARISON COLUMNS
        # ==========================================

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
            "expectancy_change_vs_baseline"
        ] = (

            df[
                "expectancy_r"
            ]

            - baseline_expectancy

        ).round(
            3
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

        df[
            "trade_retention_pct"
        ] = (

            df[
                "trades"
            ]

            / baseline_trades

            * 100.0

        ).round(
            1
        )

        # ==========================================
        # SIMPLE SCORE
        #
        # Not used to choose automatically.
        # Just useful as an extra sorting reference.
        #
        # Rewards:
        # PF
        # expectancy
        # trade retention
        #
        # We will still inspect manually.
        # ==========================================

        df[
            "robustness_score"
        ] = (

            df[
                "profit_factor"
            ]

            * df[
                "expectancy_r"
            ]

            * (
                df[
                    "trade_retention_pct"
                ]
                / 100.0
            )
        ).round(
            4
        )

        # ==========================================
        # SORT
        # ==========================================

        df = df.sort_values(

            by=[

                "profit_factor",

                "expectancy_r",

                "total_r",

                "trades"
            ],

            ascending=[

                False,
                False,
                False,
                False
            ]
        )

        df.to_csv(
            OUTPUT_FILE,
            index=False
        )

        # ==========================================
        # COMPLETE
        # ==========================================

        RESEARCH_STATUS.update({

            "state":
                "complete",

            "message":
                (
                    "Combined timing sweep "
                    "completed successfully."
                ),

            "completed_tests":
                TOTAL_TESTS,

            "rows_saved":
                len(
                    df
                ),

            "output_file":
                OUTPUT_FILE
        })

        # ==========================================
        # LOG BASELINE
        # ==========================================

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
            baseline.to_string()
        )

        # ==========================================
        # TOP ALL
        # ==========================================

        print()
        print(
            "========================================"
        )
        print(
            "TOP 25 BY PROFIT FACTOR"
        )
        print(
            "========================================"
        )

        print(

            df.head(
                25
            ).to_string(
                index=False
            )
        )

        # ==========================================
        # TOP WITH >= 75 TRADES
        # ==========================================

        print()
        print(
            "========================================"
        )
        print(
            "TOP >= 75 TRADES"
        )
        print(
            "========================================"
        )

        print(

            df[
                df[
                    "trades"
                ] >= 75
            ]
            .head(
                25
            )
            .to_string(
                index=False
            )
        )

        # ==========================================
        # TOP WITH >= 80% TRADE RETENTION
        # ==========================================

        print()
        print(
            "========================================"
        )
        print(
            "TOP >= 80% TRADE RETENTION"
        )
        print(
            "========================================"
        )

        print(

            df[
                df[
                    "trade_retention_pct"
                ] >= 80
            ]
            .head(
                25
            )
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
            "EURUSD Combined Time / Day Sweep",

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

        "timing_research":
            {

                "candidate_hours":
                    CANDIDATE_HOURS,

                "hour_combinations":
                    len(
                        HOUR_COMBINATIONS
                    ),

                "weekday_states":
                    len(
                        WEEKDAY_STATES
                    ),

                "total_tests":
                    TOTAL_TESTS,

                "timezone":
                    "America/New_York"
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
            "eurusd_short_combined_time_day_sweep.csv"
    )


# ==================================================
# START
# ==================================================

if __name__ == "__main__":

    research_thread = threading.Thread(

        target=
            run_research,

        name=
            "eurusd-combined-timing",

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
    
