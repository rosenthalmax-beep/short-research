import os
import itertools
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
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

INSTRUMENT = "GBP_USD"

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
    "gbpusd_short_broad_structural_sweep.csv"
)


# ==================================================
# BROAD GRID
#
# 5 × 4 × 4 × 5 × 4 = 1,600
# ==================================================

BODY_RATIOS = [
    1.00,
    1.10,
    1.20,
    1.30,
    1.40
]

STRUCTURE_LOOKBACKS = [
    10,
    20,
    30,
    40
]

MAX_DISTANCE_ATR_VALUES = [
    0.15,
    0.25,
    0.40,
    0.60
]

REWARD_RISKS = [
    2.00,
    2.50,
    3.00,
    3.50,
    4.00
]

SLOW_EMA_LENGTHS = [
    50,
    100,
    150,
    200
]

TOTAL_COMBINATIONS = (
    len(BODY_RATIOS)
    * len(STRUCTURE_LOOKBACKS)
    * len(MAX_DISTANCE_ATR_VALUES)
    * len(REWARD_RISKS)
    * len(SLOW_EMA_LENGTHS)
)


# ==================================================
# STATUS
# ==================================================

RESEARCH_STATUS = {

    "state":
        "not_started",

    "message":
        "Research has not started",

    "instrument":
        INSTRUMENT,

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

    "output_file":
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
                )
            )

        values.append(
            tr
        )

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

    ema_cache = {}

    for length in SLOW_EMA_LENGTHS:

        ema_cache[
            length
        ] = ema_series(
            closes,
            length
        )

    return {

        "ema":
            ema_cache
    }


def build_h1_daily_lookup(
    h1,
    daily,
    daily_state
):

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
# BASE BEARISH ENGULFINGS
# ==================================================

def build_base_candidates(
    h1,
    h1_atr,
    daily_lookup
):

    candidates = []

    max_lookback = max(
        STRUCTURE_LOOKBACKS
    )

    for index in range(
        max_lookback,
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

            or current_atr <= 0

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

        if (

            previous_body <= 0

            or current_body <= 0
        ):

            continue

        # ==========================================
        # BEARISH BODY ENGULFING
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

        structure_distances = {}

        for lookback in STRUCTURE_LOOKBACKS:

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
                (
                    current_body
                    / previous_body
                ),

            "structure_distances":
                structure_distances,

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
    structure_lookback,
    maximum_distance_atr,
    slow_ema
):

    if (

        candidate[
            "body_ratio"
        ]

        < body_ratio
    ):

        return False

    distance = (

        candidate[
            "structure_distances"
        ][
            structure_lookback
        ]
    )

    if (

        distance

        > maximum_distance_atr
    ):

        return False

    daily = candidate[
        "daily"
    ]

    ema_value = daily.get(

        f"ema_{slow_ema}"
    )

    if ema_value is None:

        return False

    # Bearish daily regime
    if not (

        daily["close"]
        < ema_value
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

    # 5-tick adverse simulated short slippage
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
            * reward_risk
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

            "exit_index":
                index,

            "result_r":
                result_r,

            "exit_reason":
                exit_reason
        }

        EXIT_CACHE[
            cache_key
        ] = result

        return result

    result = {

        "status":
            "OPEN",

        "exit_index":
            None,

        "result_r":
            None,

        "exit_reason":
            None
    }

    EXIT_CACHE[
        cache_key
    ] = result

    return result


# ==================================================
# SIMULATOR
# ==================================================

def simulate(
    h1,
    candidates,
    reward_risk
):

    trades = []

    position_exit_index = -1

    ignored = 0

    still_open = False

    for candidate in candidates:

        signal_index = (
            candidate[
                "index"
            ]
        )

        # IMPORTANT:
        # "<" not "<="
        if (
            signal_index
            < position_exit_index
        ):

            ignored += 1

            continue

        trade = calculate_trade_exit(

            h1,
            signal_index,
            reward_risk
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
        ignored,
        still_open
    )


# ==================================================
# STATISTICS
# ==================================================

def calculate_stats(
    trades
):

    if not trades:

        return {

            "trades":
                0,

            "trades_per_year":
                0,

            "winners":
                0,

            "losers":
                0,

            "win_rate":
                0,

            "profit_factor":
                0,

            "total_r":
                0,

            "expectancy_r":
                0,

            "max_drawdown_r":
                0,

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

    profit_factor = (

        gross_profit
        / gross_loss

        if gross_loss > 0

        else 999.0
    )

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

    current_streak = 0
    longest_streak = 0

    for value in results:

        if value < 0:

            current_streak += 1

            longest_streak = max(

                longest_streak,

                current_streak
            )

        else:

            current_streak = 0

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

    return {

        "trades":
            len(results),

        "trades_per_year":
            round(
                len(results)
                / years,
                2
            ),

        "winners":
            len(winners),

        "losers":
            len(losers),

        "win_rate":
            round(

                len(winners)
                / len(results)
                * 100,

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

                total_r
                / len(results),

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
# RESEARCH
# ==================================================

def run_research():

    global RESEARCH_STATUS

    try:

        print()
        print(
            "======================================"
        )
        print(
            "GBP/USD SHORT BROAD SWEEP"
        )
        print(
            "======================================"
        )

        print(
            "All hours"
        )

        print(
            "All weekdays"
        )

        print(
            "No strong-close filter"
        )

        print(
            "No wick filter"
        )

        print(
            "No minimum-range filter"
        )

        print(
            "Total combinations:",
            TOTAL_COMBINATIONS
        )

        RESEARCH_STATUS.update({

            "state":
                "fetching_data",

            "message":
                "Fetching GBP/USD history"
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

        if not h1:

            raise RuntimeError(
                "No H1 candles returned"
            )

        if not daily:

            raise RuntimeError(
                "No daily candles returned"
            )

        print(
            "H1 candles:",
            len(h1)
        )

        print(
            "Earliest H1:",
            h1[0][
                "time"
            ].isoformat()
        )

        print(
            "Daily candles:",
            len(daily)
        )

        RESEARCH_STATUS.update({

            "state":
                "precomputing",

            "message":
                "Building indicators"
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

        print(
            "Base bearish engulfings:",
            len(candidates)
        )

        RESEARCH_STATUS[
            "base_bearish_engulfings"
        ] = len(
            candidates
        )

        RESEARCH_STATUS.update({

            "state":
                "running",

            "message":
                "Running broad sweep"
        })

        rows = []

        combinations = itertools.product(

            BODY_RATIOS,

            STRUCTURE_LOOKBACKS,

            MAX_DISTANCE_ATR_VALUES,

            REWARD_RISKS,

            SLOW_EMA_LENGTHS
        )

        for number, combo in enumerate(
            combinations,
            start=1
        ):

            (
                body_ratio,
                lookback,
                distance,
                rr,
                ema
            ) = combo

            eligible = [

                candidate

                for candidate
                in candidates

                if candidate_allowed(

                    candidate,

                    body_ratio,

                    lookback,

                    distance,

                    ema
                )
            ]

            (
                trades,
                ignored,
                still_open
            ) = simulate(

                h1,
                eligible,
                rr
            )

            stats = calculate_stats(
                trades
            )

            rows.append({

                "body_ratio":
                    body_ratio,

                "structure_lookback":
                    lookback,

                "maximum_distance_atr":
                    distance,

                "reward_risk":
                    rr,

                "slow_daily_ema":
                    ema,

                "raw_signals":
                    len(
                        eligible
                    ),

                "ignored_due_to_open_trade":
                    ignored,

                "still_open_at_end":
                    still_open,

                **stats
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

        df = pd.DataFrame(
            rows
        )

        df[
            "adequate_sample"
        ] = (

            df[
                "trades"
            ]

            >= 100
        )

        df = (

            df.sort_values(

                by=[

                    "adequate_sample",

                    "profit_factor",

                    "expectancy_r",

                    "trades"
                ],

                ascending=[

                    False,
                    False,
                    False,
                    False
                ]
            )
        )

        df.to_csv(

            OUTPUT_FILE,

            index=False
        )

        print()
        print(
            "======================================"
        )
        print(
            "TOP RESULTS >= 100 TRADES"
        )
        print(
            "======================================"
        )

        columns = [

            "body_ratio",

            "structure_lookback",

            "maximum_distance_atr",

            "reward_risk",

            "slow_daily_ema",

            "trades",

            "trades_per_year",

            "win_rate",

            "profit_factor",

            "total_r",

            "expectancy_r",

            "max_drawdown_r",

            "longest_loss_streak"
        ]

        print(

            df[
                df[
                    "trades"
                ] >= 100
            ][columns]

            .head(
                30
            )

            .to_string(
                index=False
            )
        )

        print()
        print(
            "PF > 1:",
            int(
                (
                    df[
                        "profit_factor"
                    ] > 1
                ).sum()
            )
        )

        print(
            "PF > 1.05:",
            int(
                (
                    df[
                        "profit_factor"
                    ] > 1.05
                ).sum()
            )
        )

        print(
            "PF > 1.10:",
            int(
                (
                    df[
                        "profit_factor"
                    ] > 1.10
                ).sum()
            )
        )

        print(
            "Median PF:",
            round(
                float(
                    df[
                        "profit_factor"
                    ].median()
                ),
                3
            )
        )

        RESEARCH_STATUS.update({

            "state":
                "complete",

            "message":
                (
                    "GBP/USD broad sweep complete"
                ),

            "completed_combinations":
                TOTAL_COMBINATIONS,

            "rows_saved":
                len(df),

            "output_file":
                OUTPUT_FILE,

            "earliest_h1":
                h1[0][
                    "time"
                ].isoformat()
        })

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
            "GBPUSD Short Broad Research",

        "status":
            RESEARCH_STATUS,

        "instrument":
            INSTRUMENT,

        "direction":
            "SHORT",

        "grid":
            {

                "body_ratios":
                    BODY_RATIOS,

                "structure_lookbacks":
                    STRUCTURE_LOOKBACKS,

                "maximum_distance_atr":
                    MAX_DISTANCE_ATR_VALUES,

                "reward_risks":
                    REWARD_RISKS,

                "slow_daily_ema":
                    SLOW_EMA_LENGTHS,

                "total_combinations":
                    TOTAL_COMBINATIONS
            },

        "download":
            "/download",

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


@app.route("/download")
def download():

    if not os.path.exists(
        OUTPUT_FILE
    ):

        return jsonify({

            "status":
                "not_ready",

            "message":
                "CSV is not ready yet"
        }), 404

    return send_file(

        OUTPUT_FILE,

        as_attachment=True,

        download_name=
            "gbpusd_short_broad_structural_sweep.csv"
    )


# ==================================================
# START
# ==================================================

if __name__ == "__main__":

    research_thread = threading.Thread(

        target=
            run_research,

        name=
            "gbpusd-short-broad-sweep",

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
