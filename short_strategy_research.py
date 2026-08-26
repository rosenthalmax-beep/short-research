import os
import itertools
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
PRICE_PRECISION = 5

NY_TZ = ZoneInfo("America/New_York")

DAILY_ALIGNMENT_HOUR = 17
DAILY_ALIGNMENT_TIMEZONE = "America/New_York"

STOP_BUFFER_TICKS = 10
BACKTEST_SLIPPAGE_TICKS = 5

H1_CHUNK_DAYS = 180

# Earliest EUR/USD H1 candle found from OANDA
RESEARCH_FROM = datetime(
    2002, 5, 6, 20, 0,
    tzinfo=timezone.utc
)

# Exclusive end
RESEARCH_TO = datetime(
    2026, 8, 27, 0, 0,
    tzinfo=timezone.utc
)

# H1 warm-up for ATR / structure
H1_WARMUP_DAYS = 60

# Daily warm-up for EMA200
DAILY_WARMUP_DAYS = 1000

OUTPUT_FILE = "eurusd_short_full_history_sweep.csv"


# ==================================================
# PARAMETER GRID
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
    2.0,
    2.5,
    3.0,
    3.5,
    4.0
]

DAILY_EMA_LENGTHS = [
    50,
    100,
    150,
    200
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

    "research_to_exclusive":
        RESEARCH_TO.isoformat(),

    "total_combinations":
        1600,

    "completed_combinations":
        0,

    "rows_saved":
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

    response.raise_for_status()

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

    candles.sort(
        key=lambda item:
            item["time"]
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
            f"{chunk_end.date()}"
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

    result = list(
        candles_by_time.values()
    )

    result.sort(
        key=lambda item:
            item["time"]
    )

    return result


def fetch_h1_history():

    return fetch_chunked_history(

        INSTRUMENT,
        "H1",

        RESEARCH_FROM
        - timedelta(
            days=
                H1_WARMUP_DAYS
        ),

        RESEARCH_TO
    )


def fetch_daily_history():

    # D1 has far fewer candles, but chunking makes
    # the code safe regardless of the requested span.

    return fetch_chunked_history(

        INSTRUMENT,
        "D",

        RESEARCH_FROM
        - timedelta(
            days=
                DAILY_WARMUP_DAYS
        ),

        RESEARCH_TO
    )


# ==================================================
# INDICATORS
# ==================================================

def ema_series(
    values,
    length
):

    result = [
        None
    ] * len(
        values
    )

    if len(values) < length:

        return result

    initial = (
        sum(
            values[
                :length
            ]
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

        result[
            index
        ] = current

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
    ] * len(
        values
    )

    if len(values) < length:

        return result

    initial = (
        sum(
            values[
                :length
            ]
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

        result[
            index
        ] = current

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
# DAILY REGIME
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


def build_daily_state(
    daily,
    ema_length
):

    closes = [
        candle["close"]
        for candle in daily
    ]

    ema = ema_series(
        closes,
        ema_length
    )

    result = []

    for index, candle in enumerate(
        daily
    ):

        result.append({

            "time":
                candle["time"],

            "close":
                candle["close"],

            "ema":
                ema[index]
        })

    return result


def previous_daily_state(
    signal_time,
    daily_state
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
            and
            row["ema"]
            is not None
        ):

            selected = row

        elif (
            row["time"]
            >= session_start
        ):

            break

    return selected


# ==================================================
# SHORT SIGNAL
# ==================================================

def short_signal(
    h1,
    atr,
    index,
    daily_state,
    body_ratio_min,
    structure_lookback,
    max_distance_atr
):

    if index < max(
        14,
        structure_lookback
    ):

        return False

    signal = (
        h1[index]
    )

    previous = (
        h1[
            index - 1
        ]
    )

    current_atr = (
        atr[index]
    )

    if current_atr is None:

        return False

    previous_body = abs(
        previous["close"]
        - previous["open"]
    )

    current_body = abs(
        signal["close"]
        - signal["open"]
    )

    if previous_body <= 0:

        return False

    # ==============================================
    # BODY RATIO
    # ==============================================

    body_allowed = (
        current_body
        >= (
            previous_body
            * body_ratio_min
        )
    )

    # ==============================================
    # BEARISH ENGULFING
    # ==============================================

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

        and

        body_allowed
    )

    if not bearish_engulfing:

        return False

    # ==============================================
    # STRUCTURE
    #
    # Signal high must be near the previous highest
    # high, excluding the signal candle itself.
    # ==============================================

    previous_highest = max(

        candle["high"]

        for candle in h1[
            index
            - structure_lookback:
            index
        ]
    )

    distance_from_high = (
        previous_highest
        - signal["high"]
    )

    structure_allowed = (
        distance_from_high
        <= (
            current_atr
            * max_distance_atr
        )
    )

    if not structure_allowed:

        return False

    # ==============================================
    # DAILY BEARISH REGIME
    # ==============================================

    daily = previous_daily_state(
        signal["time"],
        daily_state
    )

    if daily is None:

        return False

    daily_allowed = (
        daily["close"]
        < daily["ema"]
    )

    if not daily_allowed:

        return False

    return True


# ==================================================
# TRADE SIMULATION
# ==================================================

def simulate(
    h1,
    atr,
    daily_state,
    body_ratio_min,
    structure_lookback,
    max_distance_atr,
    reward_risk
):

    trades = []

    open_trade = None

    start_index = max(
        14,
        structure_lookback
    )

    for index in range(
        start_index,
        len(h1)
    ):

        candle = (
            h1[index]
        )

        candle_time = (
            candle["time"]
        )

        # ==========================================
        # WARM-UP PROTECTION
        #
        # Warm-up bars calculate ATR/structure but
        # NEVER become trades.
        # ==========================================

        if candle_time < RESEARCH_FROM:

            continue

        if candle_time >= RESEARCH_TO:

            break

        # ==========================================
        # EXIT EXISTING TRADE FIRST
        # ==========================================

        if open_trade is not None:

            stop_hit = (
                candle["high"]
                >= open_trade[
                    "stop"
                ]
            )

            target_hit = (
                candle["low"]
                <= open_trade[
                    "target"
                ]
            )

            if (
                stop_hit
                or
                target_hit
            ):

                exit_reason = None
                exit_price = None

                # ==================================
                # BOTH TOUCHED
                # ==================================

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

                    # TradingView-style broker
                    # emulator path approximation.
                    #
                    # SHORT:
                    # High first = stop first.
                    # Low first = target first.

                    if (
                        distance_to_high
                        < distance_to_low
                    ):

                        exit_reason = (
                            "STOP"
                        )

                        exit_price = (
                            open_trade[
                                "stop"
                            ]
                        )

                    else:

                        exit_reason = (
                            "TARGET"
                        )

                        exit_price = (
                            open_trade[
                                "target"
                            ]
                        )

                elif stop_hit:

                    exit_reason = (
                        "STOP"
                    )

                    exit_price = (
                        open_trade[
                            "stop"
                        ]
                    )

                else:

                    exit_reason = (
                        "TARGET"
                    )

                    exit_price = (
                        open_trade[
                            "target"
                        ]
                    )

                # ==================================
                # ACTUAL R AFTER ENTRY SLIPPAGE
                # ==================================

                actual_risk = (
                    open_trade["stop"]
                    - open_trade[
                        "backtest_entry"
                    ]
                )

                if actual_risk <= 0:

                    result_r = 0.0

                else:

                    result_r = (
                        open_trade[
                            "backtest_entry"
                        ]
                        - exit_price
                    ) / actual_risk

                open_trade.update({

                    "exit_time":
                        candle_time
                        + timedelta(
                            hours=1
                        ),

                    "exit_reason":
                        exit_reason,

                    "exit_price":
                        exit_price,

                    "result_r":
                        result_r
                })

                trades.append(
                    open_trade
                )

                open_trade = None

        # ==========================================
        # PYRAMIDING = 0
        # ==========================================

        if open_trade is not None:

            continue

        # ==========================================
        # SIGNAL
        # ==========================================

        if not short_signal(

            h1,
            atr,
            index,
            daily_state,

            body_ratio_min,
            structure_lookback,
            max_distance_atr
        ):

            continue

        signal = (
            h1[index]
        )

        # ==========================================
        # ENTRY
        # ==========================================

        reference_entry = (
            signal["close"]
        )

        # Adverse slippage for a short means
        # selling LOWER than the reference close.
        backtest_entry = (
            reference_entry
            - (
                BACKTEST_SLIPPAGE_TICKS
                * TICK_SIZE
            )
        )

        # ==========================================
        # STOP
        # ==========================================

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

            continue

        # ==========================================
        # TARGET
        #
        # Target remains based on reference close,
        # matching the convention used in our
        # existing TradingView/Python strategies.
        # ==========================================

        target = (
            reference_entry
            - (
                reference_risk
                * reward_risk
            )
        )

        open_trade = {

            "signal_time":
                signal["time"],

            "entry_time":
                signal["time"]
                + timedelta(
                    hours=1
                ),

            "reference_entry":
                reference_entry,

            "backtest_entry":
                backtest_entry,

            "stop":
                stop,

            "target":
                target,

            "exit_time":
                None,

            "exit_reason":
                None,

            "exit_price":
                None,

            "result_r":
                None
        }

    return trades


# ==================================================
# PERFORMANCE
# ==================================================

def calculate_stats(
    trades
):

    completed = [

        trade

        for trade in trades

        if trade[
            "result_r"
        ] is not None
    ]

    if not completed:

        return None

    results = [
        trade["result_r"]
        for trade in completed
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

    profit_factor = (

        gross_profit
        / gross_loss

        if gross_loss > 0

        else float(
            "inf"
        )
    )

    win_rate = (
        len(
            winners
        )
        / len(
            results
        )
        * 100
    )

    expectancy = (
        total_r
        / len(
            results
        )
    )

    # ==============================================
    # MAX DRAWDOWN IN R
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
    # LONGEST LOSING STREAK
    # ==============================================

    longest_losing_streak = 0
    current_losing_streak = 0

    for result in results:

        if result < 0:

            current_losing_streak += 1

            longest_losing_streak = max(
                longest_losing_streak,
                current_losing_streak
            )

        else:

            current_losing_streak = 0

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

        RESEARCH_STATUS.update({

            "state":
                "fetching_data",

            "message":
                "Fetching full OANDA history"
        })

        print()
        print(
            "========================================"
        )
        print(
            "EUR/USD SHORT FULL-HISTORY SWEEP"
        )
        print(
            "========================================"
        )

        print()
        print(
            "Research period:"
        )

        print(
            iso_utc(
                RESEARCH_FROM
            )
        )

        print(
            "to"
        )

        print(
            iso_utc(
                RESEARCH_TO
            )
        )

        # ==========================================
        # LOAD DATA
        # ==========================================

        h1 = fetch_h1_history()

        daily = fetch_daily_history()

        print()
        print(
            "H1 candles loaded:",
            len(h1)
        )

        print(
            "Daily candles loaded:",
            len(daily)
        )

        # ==========================================
        # ATR
        # ==========================================

        print()
        print(
            "Calculating ATR14..."
        )

        atr = atr_series(
            h1,
            14
        )

        # ==========================================
        # DAILY EMA CACHE
        # ==========================================

        print(
            "Building daily EMA cache..."
        )

        daily_cache = {}

        for ema_length in (
            DAILY_EMA_LENGTHS
        ):

            daily_cache[
                ema_length
            ] = build_daily_state(
                daily,
                ema_length
            )

        # ==========================================
        # GRID
        # ==========================================

        combinations = list(
            itertools.product(

                BODY_RATIOS,

                STRUCTURE_LOOKBACKS,

                MAX_DISTANCE_ATR_VALUES,

                REWARD_RISKS,

                DAILY_EMA_LENGTHS
            )
        )

        total_combinations = len(
            combinations
        )

        RESEARCH_STATUS.update({

            "state":
                "running",

            "message":
                "Running full-history parameter sweep",

            "total_combinations":
                total_combinations,

            "completed_combinations":
                0
        })

        print()
        print(
            f"Testing "
            f"{total_combinations} "
            f"combinations..."
        )

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
                daily_ema
            ) = combo

            trades = simulate(

                h1,

                atr,

                daily_cache[
                    daily_ema
                ],

                body_ratio,

                structure_lookback,

                max_distance_atr,

                reward_risk
            )

            stats = calculate_stats(
                trades
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

                    "daily_ema":
                        daily_ema
                }

                row.update(
                    stats
                )

                results.append(
                    row
                )

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
                    f"{total_combinations}"
                )

        # ==========================================
        # RESULTS
        # ==========================================

        df = pd.DataFrame(
            results
        )

        if df.empty:

            raise RuntimeError(
                "No results generated"
            )

        # With 24 years of data there is no reason
        # to consider extremely tiny samples.
        df = df[
            df["trades"]
            >= 100
        ].copy()

        # ==========================================
        # RANK
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

        # ==========================================
        # SAVE
        # ==========================================

        df.to_csv(
            OUTPUT_FILE,
            index=False
        )

        RESEARCH_STATUS.update({

            "state":
                "complete",

            "message":
                "Full-history sweep complete",

            "rows_saved":
                len(
                    df
                ),

            "output_file":
                OUTPUT_FILE
        })

        # ==========================================
        # PRINT TOP 30
        # ==========================================

        columns = [

            "body_ratio",

            "structure_lookback",

            "max_distance_atr",

            "reward_risk",

            "daily_ema",

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

        print()
        print(
            "========================================"
        )
        print(
            "TOP 30 FULL-HISTORY RESULTS"
        )
        print(
            "========================================"
        )

        print(
            df[
                columns
            ]
            .head(30)
            .to_string(
                index=False
            )
        )

        print()
        print(
            "CSV saved:"
        )
        print(
            OUTPUT_FILE
        )
        print()

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
            error
        )


# ==================================================
# ROUTES
# ==================================================

@app.route("/")
def home():

    return jsonify({

        "service":
            "ERF EURUSD Short Full-History Research",

        "status":
            RESEARCH_STATUS,

        "research_period":
            {
                "from":
                    iso_utc(
                        RESEARCH_FROM
                    ),

                "to_exclusive":
                    iso_utc(
                        RESEARCH_TO
                    )
            },

        "parameter_combinations":
            1600,

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


@app.route(
    "/status"
)
def status():

    return jsonify(
        RESEARCH_STATUS
    )


@app.route(
    "/download"
)
def download():

    if not os.path.exists(
        OUTPUT_FILE
    ):

        return jsonify({

            "status":
                "not_ready",

            "message":
                "Research CSV has "
                "not been generated yet."
        }), 404

    return send_file(

        OUTPUT_FILE,

        as_attachment=True,

        download_name=
            "eurusd_short_full_history_sweep.csv"
    )


# ==================================================
# START
# ==================================================

if __name__ == "__main__":

    run_research()

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
