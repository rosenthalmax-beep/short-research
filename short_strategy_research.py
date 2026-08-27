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

ERA_OUTPUT_FILE = (
    "eurusd_short_era_robustness.csv"
)

YEARLY_OUTPUT_FILE = (
    "eurusd_short_yearly_robustness.csv"
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

TIMING_VARIANTS = {

    "BASELINE_ALL_HOURS":
        tuple(),

    "A_EXCLUDE_10_12_14":
        (
            10,
            12,
            14
        ),

    "B_EXCLUDE_02_10_12_14":
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
    ),

    (
        "FULL_HISTORY",
        RESEARCH_FROM,
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

    "base_signal_candidates":
        0,

    "era_tests_completed":
        0,

    "year_tests_completed":
        0,

    "era_output_file":
        None,

    "yearly_output_file":
        None
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
# FROZEN CORE SIGNALS
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
        # BEARISH ENGULF
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
        # BODY
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
        # STRUCTURE
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
                ny_time.weekday(),

            "utc_year":
                signal["time"].year
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

    # Still open at end of history.
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

        # Pyramiding = 0
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
        still_open,
        ignored_signals
    )


# ==================================================
# STATISTICS
# ==================================================

def calculate_stats(
    trades,
    period_start,
    period_end
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
                0,

            "trades_per_year":
                0.0
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

    gross_profit = sum(
        winners
    )

    gross_loss = abs(
        sum(
            losers
        )
    )

    total_r = sum(
        results
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

    years = (

        (
            period_end
            - period_start
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

        if years > 0

        else 0.0
    )

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
            (
                round(
                    profit_factor,
                    3
                )

                if profit_factor
                != float("inf")

                else "INF"
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
            longest_streak,

        "trades_per_year":
            round(
                trades_per_year,
                2
            )
    }


# ==================================================
# FILTER CANDIDATES
# ==================================================

def filter_candidates(
    candidates,
    excluded_hours,
    start_time,
    end_time
):

    return [

        candidate

        for candidate in candidates

        if (

            candidate[
                "time"
            ]
            >= start_time

            and

            candidate[
                "time"
            ]
            < end_time

            and

            candidate[
                "ny_hour"
            ]
            not in excluded_hours
        )
    ]


# ==================================================
# ERA TEST
# ==================================================

def run_era_tests(
    h1,
    candidates
):

    rows = []

    completed = 0

    for (
        era_name,
        era_start,
        era_end
    ) in ERAS:

        actual_start = max(
            era_start,
            RESEARCH_FROM
        )

        actual_end = min(
            era_end,
            RESEARCH_TO
        )

        if actual_end <= actual_start:

            continue

        for (
            variant_name,
            excluded_hours
        ) in TIMING_VARIANTS.items():

            eligible = filter_candidates(

                candidates,

                excluded_hours,

                actual_start,

                actual_end
            )

            trades, still_open, ignored = simulate(

                h1,
                eligible
            )

            stats = calculate_stats(

                trades,

                actual_start,

                actual_end
            )

            row = {

                "era":
                    era_name,

                "variant":
                    variant_name,

                "excluded_ny_hours":
                    (
                        "NONE"
                        if not excluded_hours
                        else ",".join(
                            f"{hour:02d}:00"
                            for hour
                            in excluded_hours
                        )
                    ),

                "period_start":
                    actual_start.isoformat(),

                "period_end":
                    actual_end.isoformat(),

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

            rows.append(
                row
            )

            completed += 1

            RESEARCH_STATUS[
                "era_tests_completed"
            ] = completed

    return pd.DataFrame(
        rows
    )


# ==================================================
# YEARLY TEST
# ==================================================

def run_yearly_tests(
    h1,
    candidates
):

    rows = []

    completed = 0

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

        year_start = datetime(
            year,
            1,
            1,
            tzinfo=timezone.utc
        )

        year_end = datetime(
            year + 1,
            1,
            1,
            tzinfo=timezone.utc
        )

        actual_start = max(
            year_start,
            RESEARCH_FROM
        )

        actual_end = min(
            year_end,
            RESEARCH_TO
        )

        if actual_end <= actual_start:

            continue

        for (
            variant_name,
            excluded_hours
        ) in TIMING_VARIANTS.items():

            eligible = filter_candidates(

                candidates,

                excluded_hours,

                actual_start,

                actual_end
            )

            trades, still_open, ignored = simulate(

                h1,
                eligible
            )

            stats = calculate_stats(

                trades,

                actual_start,

                actual_end
            )

            row = {

                "year":
                    year,

                "variant":
                    variant_name,

                "excluded_ny_hours":
                    (
                        "NONE"
                        if not excluded_hours
                        else ",".join(
                            f"{hour:02d}:00"
                            for hour
                            in excluded_hours
                        )
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

            rows.append(
                row
            )

            completed += 1

            RESEARCH_STATUS[
                "year_tests_completed"
            ] = completed

    return pd.DataFrame(
        rows
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
            "EUR/USD HISTORICAL ROBUSTNESS"
        )
        print(
            "========================================"
        )
        print()

        print(
            "STRUCTURAL CORE FROZEN"
        )

        print(
            "No weekday exclusions"
        )

        print(
            "Testing:"
        )

        print(
            "BASELINE = all hours"
        )

        print(
            "A = exclude 10,12,14 NY"
        )

        print(
            "B = exclude 02,10,12,14 NY"
        )

        # ==========================================
        # FETCH
        # ==========================================

        RESEARCH_STATUS.update({

            "state":
                "fetching_data",

            "message":
                "Fetching complete EUR/USD history"
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
        # BUILD STRATEGY
        # ==========================================

        RESEARCH_STATUS.update({

            "state":
                "precomputing",

            "message":
                "Building frozen strategy signals"
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
            len(
                candidates
            )
        )

        # ==========================================
        # ERA TESTS
        # ==========================================

        RESEARCH_STATUS.update({

            "state":
                "era_tests",

            "message":
                "Running historical era tests"
        })

        era_df = run_era_tests(

            h1,
            candidates
        )

        era_df.to_csv(

            ERA_OUTPUT_FILE,

            index=False
        )

        # ==========================================
        # YEARLY TESTS
        # ==========================================

        RESEARCH_STATUS.update({

            "state":
                "yearly_tests",

            "message":
                "Running individual yearly tests"
        })

        yearly_df = run_yearly_tests(

            h1,
            candidates
        )

        yearly_df.to_csv(

            YEARLY_OUTPUT_FILE,

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
                    "Historical era and yearly "
                    "tests completed successfully."
                ),

            "era_output_file":
                ERA_OUTPUT_FILE,

            "yearly_output_file":
                YEARLY_OUTPUT_FILE
        })

        # ==========================================
        # PRINT ERA RESULTS
        # ==========================================

        print()
        print(
            "========================================"
        )
        print(
            "ERA RESULTS"
        )
        print(
            "========================================"
        )

        print(

            era_df[
                [
                    "era",
                    "variant",
                    "trades",
                    "winners",
                    "losers",
                    "win_rate",
                    "profit_factor",
                    "total_r",
                    "expectancy_r",
                    "max_drawdown_r",
                    "longest_loss_streak"
                ]
            ]
            .to_string(
                index=False
            )
        )

        # ==========================================
        # PRINT YEARLY RESULTS
        # ==========================================

        print()
        print(
            "========================================"
        )
        print(
            "YEARLY RESULTS"
        )
        print(
            "========================================"
        )

        print(

            yearly_df[
                [
                    "year",
                    "variant",
                    "trades",
                    "win_rate",
                    "profit_factor",
                    "total_r",
                    "expectancy_r"
                ]
            ]
            .to_string(
                index=False
            )
        )

        print()
        print(
            "Saved:"
        )

        print(
            ERA_OUTPUT_FILE
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
            "EURUSD Historical Robustness Research",

        "status":
            RESEARCH_STATUS,

        "frozen_strategy":
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
                    EMA_SEPARATION_ATR,

                "weekday_exclusions":
                    "NONE"
            },

        "variants":
            {

                name:
                    list(hours)

                for name, hours
                in TIMING_VARIANTS.items()
            },

        "era_download":
            "/download-era",

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


@app.route("/download-era")
def download_era():

    if not os.path.exists(
        ERA_OUTPUT_FILE
    ):

        return jsonify({

            "status":
                "not_ready",

            "message":
                "Era CSV is not ready yet."
        }), 404

    return send_file(

        ERA_OUTPUT_FILE,

        as_attachment=True,

        download_name=
            "eurusd_short_era_robustness.csv"
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
            "eurusd_short_yearly_robustness.csv"
    )


# ==================================================
# START
# ==================================================

if __name__ == "__main__":

    research_thread = threading.Thread(

        target=
            run_research,

        name=
            "eurusd-historical-robustness",

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
    
