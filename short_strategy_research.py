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

# Earliest available EUR/USD H1 history found
RESEARCH_FROM = datetime(
    2002, 5, 6, 20, 0,
    tzinfo=timezone.utc
)

# Latest completed H1 boundary
RESEARCH_TO = (
    datetime.now(timezone.utc)
    .replace(
        minute=0,
        second=0,
        microsecond=0
    )
)

H1_WARMUP_DAYS = 60
DAILY_WARMUP_DAYS = 1500

OUTPUT_FILE = (
    "eurusd_short_combined_structural_sweep.csv"
)


# ==================================================
# PARAMETER GRID
#
# NO SESSION FILTERS
# NO WEEKDAY FILTERS
# ==================================================

BODY_RATIOS = [
    1.20,
    1.30,
    1.40
]

STRUCTURE_LOOKBACKS = [
    20,
    30,
    40
]

MAX_DISTANCE_ATR_VALUES = [
    0.15,
    0.25
]

REWARD_RISKS = [
    2.0,
    3.0,
    4.0
]

SLOW_EMA_LENGTHS = [
    100,
    150,
    200
]

# None = second EMA disabled
FAST_EMA_LENGTHS = [
    None,
    20,
    30,
    50
]

# None = strong-close filter disabled
#
# 0.25 means signal close must be within
# bottom 25% of its H1 range.
STRONG_CLOSE_LEVELS = [
    None,
    0.25
]

# None = minimum-range filter disabled
MINIMUM_RANGE_ATR_VALUES = [
    None,
    0.90
]

# None = upper-wick filter disabled
UPPER_WICK_BODY_RATIOS = [
    None,
    0.20
]


# ==================================================
# TOTAL COMBINATIONS
# ==================================================

TOTAL_COMBINATIONS = (
    len(BODY_RATIOS)
    * len(STRUCTURE_LOOKBACKS)
    * len(MAX_DISTANCE_ATR_VALUES)
    * len(REWARD_RISKS)
    * len(SLOW_EMA_LENGTHS)
    * len(FAST_EMA_LENGTHS)
    * len(STRONG_CLOSE_LEVELS)
    * len(MINIMUM_RANGE_ATR_VALUES)
    * len(UPPER_WICK_BODY_RATIOS)
)


# ==================================================
# STATUS
# ==================================================

RESEARCH_STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "research_from":
        RESEARCH_FROM.isoformat(),
    "research_to":
        RESEARCH_TO.isoformat(),
    "total_combinations":
        TOTAL_COMBINATIONS,
    "completed_combinations": 0,
    "rows_saved": 0,
    "base_signal_candidates": 0
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

    mid = raw.get("mid")

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
            float(mid["o"]),

        "high":
            float(mid["h"]),

        "low":
            float(mid["l"]),

        "close":
            float(mid["c"])
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

        candle = parse_candle(raw)

        if candle is not None:

            candles.append(candle)

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
        sum(values[:length])
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


def true_ranges(candles):

    values = []

    for index, candle in enumerate(
        candles
    ):

        if index == 0:

            value = (
                candle["high"]
                - candle["low"]
            )

        else:

            previous_close = (
                candles[
                    index - 1
                ]["close"]
            )

            value = max(
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

        values.append(value)

    return values


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
        sum(values[:length])
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
        true_ranges(candles),
        length
    )


# ==================================================
# DAILY STATE
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

    candidate = ny_time.replace(
        hour=
            DAILY_ALIGNMENT_HOUR,
        minute=0,
        second=0,
        microsecond=0
    )

    if ny_time < candidate:

        candidate -= timedelta(
            days=1
        )

    return candidate.astimezone(
        timezone.utc
    )


def build_daily_indicator_cache(
    daily
):

    closes = [
        candle["close"]
        for candle in daily
    ]

    required_lengths = sorted(
        set(
            SLOW_EMA_LENGTHS
            + [
                value
                for value
                in FAST_EMA_LENGTHS
                if value is not None
            ]
        )
    )

    cache = {}

    for length in required_lengths:

        cache[length] = ema_series(
            closes,
            length
        )

    return cache


def build_h1_daily_lookup(
    h1,
    daily,
    daily_ema_cache
):

    print(
        "Building H1 -> previous daily "
        "state lookup...",
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
                ]["close"]
        }

        for length, values in (
            daily_ema_cache.items()
        ):

            row[
                f"ema_{length}"
            ] = values[
                daily_index
            ]

        lookup[h1_index] = row

    return lookup


# ==================================================
# PRECOMPUTE SIGNAL INFORMATION
# ==================================================

def build_signal_candidates(
    h1,
    atr,
    daily_lookup
):

    print(
        "Precomputing bearish-engulfing "
        "signal candidates...",
        flush=True
    )

    candidates = []

    maximum_lookback = max(
        STRUCTURE_LOOKBACKS
    )

    for index in range(
        maximum_lookback,
        len(h1)
    ):

        signal = h1[index]

        if signal["time"] < RESEARCH_FROM:

            continue

        if signal["time"] >= RESEARCH_TO:

            break

        previous = h1[
            index - 1
        ]

        current_atr = atr[index]

        if current_atr is None:

            continue

        daily_state = (
            daily_lookup[index]
        )

        if daily_state is None:

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

        if previous_body <= 0:

            continue

        if current_body <= 0:

            continue

        if signal_range <= 0:

            continue

        # Basic bearish engulfing.
        # Body-ratio threshold is applied later.
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

        range_atr = (
            signal_range
            / current_atr
        )

        upper_wick = (
            signal["high"]
            - max(
                signal["open"],
                signal["close"]
            )
        )

        upper_wick_body_ratio = (
            upper_wick
            / current_body
        )

        structure_distances = {}

        for lookback in (
            STRUCTURE_LOOKBACKS
        ):

            previous_highest = max(
                candle["high"]
                for candle in h1[
                    index - lookback:
                    index
                ]
            )

            distance_atr = (
                previous_highest
                - signal["high"]
            ) / current_atr

            structure_distances[
                lookback
            ] = distance_atr

        candidates.append({
            "index":
                index,

            "time":
                signal["time"],

            "body_ratio":
                body_ratio,

            "close_location":
                close_location,

            "range_atr":
                range_atr,

            "upper_wick_body_ratio":
                upper_wick_body_ratio,

            "structure_distances":
                structure_distances,

            "daily":
                daily_state
        })

    return candidates


# ==================================================
# FILTER SIGNAL CANDIDATES
# ==================================================

def candidate_allowed(
    candidate,
    body_ratio,
    structure_lookback,
    max_distance_atr,
    slow_ema,
    fast_ema,
    strong_close,
    minimum_range_atr,
    minimum_upper_wick_ratio
):

    if (
        candidate["body_ratio"]
        < body_ratio
    ):

        return False

    if (
        candidate[
            "structure_distances"
        ][structure_lookback]
        > max_distance_atr
    ):

        return False

    daily = candidate["daily"]

    slow_value = daily.get(
        f"ema_{slow_ema}"
    )

    if slow_value is None:

        return False

    # Main bearish daily regime
    if not (
        daily["close"]
        < slow_value
    ):

        return False

    # Optional fast/slow bearish EMA alignment
    if fast_ema is not None:

        fast_value = daily.get(
            f"ema_{fast_ema}"
        )

        if fast_value is None:

            return False

        if not (
            fast_value
            < slow_value
        ):

            return False

    # Optional strong bearish close
    if strong_close is not None:

        if (
            candidate[
                "close_location"
            ]
            > strong_close
        ):

            return False

    # Optional large candle filter
    if minimum_range_atr is not None:

        if (
            candidate[
                "range_atr"
            ]
            < minimum_range_atr
        ):

            return False

    # Optional upper-wick rejection
    if (
        minimum_upper_wick_ratio
        is not None
    ):

        if (
            candidate[
                "upper_wick_body_ratio"
            ]
            < minimum_upper_wick_ratio
        ):

            return False

    return True


# ==================================================
# TRADE EXIT CACHE
# ==================================================

EXIT_CACHE = {}


def calculate_trade_exit(
    h1,
    signal_index,
    reward_risk
):

    cache_key = (
        signal_index,
        reward_risk
    )

    if cache_key in EXIT_CACHE:

        return EXIT_CACHE[
            cache_key
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
            cache_key
        ] = None

        return None

    target = (
        reference_entry
        - (
            reference_risk
            * reward_risk
        )
    )

    actual_risk = (
        stop
        - backtest_entry
    )

    if actual_risk <= 0:

        EXIT_CACHE[
            cache_key
        ] = None

        return None

    # First possible exit is the candle AFTER
    # the signal candle.
    for index in range(
        signal_index + 1,
        len(h1)
    ):

        candle = h1[index]

        if candle["time"] >= RESEARCH_TO:

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

            # Same approximation used previously:
            # for a short:
            # high first -> stop
            # low first -> target
            if (
                distance_to_high
                < distance_to_low
            ):

                exit_price = stop

            else:

                exit_price = target

        elif stop_hit:

            exit_price = stop

        else:

            exit_price = target

        result_r = (
            (
                backtest_entry
                - exit_price
            )
            / actual_risk
        )

        result = {
            "exit_index":
                index,

            "result_r":
                result_r
        }

        EXIT_CACHE[
            cache_key
        ] = result

        return result

    EXIT_CACHE[
        cache_key
    ] = None

    return None


# ==================================================
# SIMULATE SIGNAL LIST
# ==================================================

def simulate_candidates(
    h1,
    candidates,
    reward_risk
):

    results = []

    # Mimics pyramiding=0.
    #
    # Signals occurring before the current trade
    # exits are ignored.
    position_exit_index = -1

    for candidate in candidates:

        signal_index = (
            candidate["index"]
        )

        if (
            signal_index
            <= position_exit_index
        ):

            continue

        trade = calculate_trade_exit(
            h1,
            signal_index,
            reward_risk
        )

        if trade is None:

            continue

        results.append(
            trade["result_r"]
        )

        position_exit_index = (
            trade["exit_index"]
        )

    return results


# ==================================================
# PERFORMANCE
# ==================================================

def calculate_stats(results):

    if not results:

        return None

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

    total_r = sum(results)

    gross_profit = sum(
        winners
    )

    gross_loss = abs(
        sum(losers)
    )

    if gross_loss > 0:

        profit_factor = (
            gross_profit
            / gross_loss
        )

    else:

        profit_factor = (
            float("inf")
        )

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
    # MAX DRAWDOWN
    # ==============================================

    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0

    for value in results:

        equity += value

        peak = max(
            peak,
            equity
        )

        max_drawdown = min(
            max_drawdown,
            equity - peak
        )

    # ==============================================
    # LONGEST LOSING STREAK
    # ==============================================

    longest_losing_streak = 0
    current_losing_streak = 0

    for value in results:

        if value < 0:

            current_losing_streak += 1

            longest_losing_streak = max(
                longest_losing_streak,
                current_losing_streak
            )

        else:

            current_losing_streak = 0

    years = (
        (
            RESEARCH_TO
            - RESEARCH_FROM
        ).total_seconds()
        / (
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
            longest_losing_streak
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
            "EUR/USD SHORT COMBINED STRUCTURAL SWEEP"
        )
        print(
            "========================================"
        )
        print()

        print(
            "ALL HOURS ENABLED"
        )

        print(
            "ALL WEEKDAYS ENABLED"
        )

        print(
            "Total combinations:",
            TOTAL_COMBINATIONS
        )

        print()

        # ==========================================
        # FETCH DATA
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

        print()
        print(
            "H1 candles:",
            len(h1)
        )

        print(
            "Daily candles:",
            len(daily)
        )

        # ==========================================
        # INDICATORS
        # ==========================================

        RESEARCH_STATUS.update({
            "state":
                "precomputing",

            "message":
                "Precomputing ATR, EMA and signal data"
        })

        print()
        print(
            "Calculating ATR14...",
            flush=True
        )

        atr = atr_series(
            h1,
            14
        )

        print(
            "Calculating daily EMAs...",
            flush=True
        )

        daily_ema_cache = (
            build_daily_indicator_cache(
                daily
            )
        )

        daily_lookup = (
            build_h1_daily_lookup(
                h1,
                daily,
                daily_ema_cache
            )
        )

        candidates = (
            build_signal_candidates(
                h1,
                atr,
                daily_lookup
            )
        )

        RESEARCH_STATUS[
            "base_signal_candidates"
        ] = len(candidates)

        print()
        print(
            "Base bearish-engulfing candidates:",
            len(candidates)
        )

        # ==========================================
        # COMBINATIONS
        # ==========================================

        combinations = list(
            itertools.product(
                BODY_RATIOS,
                STRUCTURE_LOOKBACKS,
                MAX_DISTANCE_ATR_VALUES,
                REWARD_RISKS,
                SLOW_EMA_LENGTHS,
                FAST_EMA_LENGTHS,
                STRONG_CLOSE_LEVELS,
                MINIMUM_RANGE_ATR_VALUES,
                UPPER_WICK_BODY_RATIOS
            )
        )

        RESEARCH_STATUS.update({
            "state":
                "running",

            "message":
                "Running 10,368 structural combinations",

            "completed_combinations":
                0,

            "total_combinations":
                len(combinations)
        })

        results = []

        # ==========================================
        # SWEEP
        # ==========================================

        for number, combo in enumerate(
            combinations,
            start=1
        ):

            (
                body_ratio,
                structure_lookback,
                max_distance_atr,
                reward_risk,
                slow_ema,
                fast_ema,
                strong_close,
                minimum_range_atr,
                upper_wick_ratio
            ) = combo

            eligible = [
                candidate
                for candidate
                in candidates
                if candidate_allowed(
                    candidate,
                    body_ratio,
                    structure_lookback,
                    max_distance_atr,
                    slow_ema,
                    fast_ema,
                    strong_close,
                    minimum_range_atr,
                    upper_wick_ratio
                )
            ]

            trade_results = (
                simulate_candidates(
                    h1,
                    eligible,
                    reward_risk
                )
            )

            stats = calculate_stats(
                trade_results
            )

            if stats is not None:

                row = {
                    "body_ratio":
                        body_ratio,

                    "structure_lookback":
                        structure_lookback,

                    "max_distance_atr":
                        max_distance_atr,

                    "reward_risk":
                        reward_risk,

                    "slow_ema":
                        slow_ema,

                    "fast_ema":
                        (
                            "OFF"
                            if fast_ema is None
                            else fast_ema
                        ),

                    "strong_bearish_close":
                        (
                            "OFF"
                            if strong_close
                            is None
                            else strong_close
                        ),

                    "minimum_range_atr":
                        (
                            "OFF"
                            if minimum_range_atr
                            is None
                            else minimum_range_atr
                        ),

                    "upper_wick_body_ratio":
                        (
                            "OFF"
                            if upper_wick_ratio
                            is None
                            else upper_wick_ratio
                        ),

                    "raw_signals":
                        len(eligible)
                }

                row.update(stats)

                results.append(row)

            RESEARCH_STATUS[
                "completed_combinations"
            ] = number

            if number % 250 == 0:

                print(
                    f"Progress: "
                    f"{number}/"
                    f"{len(combinations)}",
                    flush=True
                )

        # ==========================================
        # SAVE
        # ==========================================

        df = pd.DataFrame(
            results
        )

        if df.empty:

            raise RuntimeError(
                "No results generated"
            )

        # Do NOT throw away low-trade variants here.
        #
        # We want the complete dataset so we can
        # analyse whether filters improve robustness
        # while reducing frequency.
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

        RESEARCH_STATUS.update({
            "state":
                "complete",

            "message":
                "Combined structural sweep complete",

            "completed_combinations":
                len(combinations),

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
            "TOP RESULTS WITH >= 75 TRADES"
        )
        print(
            "========================================"
        )

        meaningful = df[
            df["trades"]
            >= 75
        ]

        print(
            meaningful
            .head(30)
            .to_string(
                index=False
            )
        )

        print()
        print(
            "========================================"
        )
        print(
            "TOP RESULTS WITH >= 100 TRADES"
        )
        print(
            "========================================"
        )

        meaningful_100 = df[
            df["trades"]
            >= 100
        ]

        print(
            meaningful_100
            .head(30)
            .to_string(
                index=False
            )
        )

        print()
        print(
            "Saved:"
        )

        print(
            OUTPUT_FILE
        )

    except Exception as error:

        RESEARCH_STATUS.update({
            "state":
                "error",

            "message":
                str(error)
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
            "EURUSD Short Combined Structural Research",

        "status":
            RESEARCH_STATUS,

        "research":
            {
                "all_hours":
                    True,

                "all_weekdays":
                    True,

                "session_filters":
                    False,

                "weekday_filters":
                    False,

                "total_combinations":
                    TOTAL_COMBINATIONS
            },

        "trading_enabled":
            False,

        "orders_supported":
            False,

        "executor_connected":
            False,

        "status_endpoint":
            "/status",

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
                "Research CSV has not "
                "been generated yet."
        }), 404

    return send_file(
        OUTPUT_FILE,
        as_attachment=True,

        download_name=
            "eurusd_short_combined_structural_sweep.csv"
    )


# ==================================================
# START
# ==================================================

if __name__ == "__main__":

    research_thread = threading.Thread(
        target=run_research,
        name=
            "eurusd-short-combined-research",
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
        host="0.0.0.0",
        port=port,
        debug=False
    )
    
