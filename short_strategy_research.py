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

DAILY_ALIGNMENT_HOUR = 17
DAILY_ALIGNMENT_TIMEZONE = "America/New_York"

NY_TZ = ZoneInfo("America/New_York")

FROM_DATE = "2021-01-01"
TO_DATE = "2026-08-26"

H1_CHUNK_DAYS = 180

STOP_BUFFER_TICKS = 10
BACKTEST_SLIPPAGE_TICKS = 5

OUTPUT_FILE = "eurusd_short_sweep.csv"


# ==================================================
# PARAMETER SWEEP
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
    "state": "not_started",
    "message": "Research has not started yet",
    "total_combinations": 0,
    "completed_combinations": 0,
    "rows_saved": 0
}


# ==================================================
# OANDA
# ==================================================

def headers():

    if not OANDA_TOKEN:
        raise RuntimeError(
            "OANDA_TOKEN is not set"
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


def parse_date(value):

    dt = datetime.fromisoformat(
        value
    )

    return dt.replace(
        tzinfo=timezone.utc
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


def fetch_h1_history(
    instrument,
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

        chunk = fetch_range(
            instrument,
            "H1",
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


def fetch_daily_history(
    instrument,
    start,
    end
):

    warmup_start = (
        start
        - timedelta(
            days=1000
        )
    )

    return fetch_range(
        instrument,
        "D",
        warmup_start,
        end
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

        candidate = (
            candidate
            - timedelta(
                days=1
            )
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

    state = []

    for index, candle in enumerate(
        daily
    ):

        state.append({

            "time":
                candle["time"],

            "close":
                candle["close"],

            "ema":
                ema[index]
        })

    return state


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

    body_ok = (
        current_body
        >= previous_body
        * body_ratio_min
    )

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

        body_ok
    )

    if not bearish_engulfing:

        return False

    previous_highest_high = max(
        candle["high"]
        for candle in h1[
            index
            - structure_lookback:
            index
        ]
    )

    distance_from_recent_high = (
        previous_highest_high
        - signal["high"]
    )

    structure_ok = (
        distance_from_recent_high
        <= current_atr
        * max_distance_atr
    )

    if not structure_ok:

        return False

    daily = previous_daily_state(
        signal["time"],
        daily_state
    )

    if daily is None:

        return False

    # Bearish daily regime
    daily_ok = (
        daily["close"]
        < daily["ema"]
    )

    if not daily_ok:

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

    for index in range(
        max(
            14,
            structure_lookback
        ),
        len(h1)
    ):

        candle = (
            h1[index]
        )

        # ==========================================
        # EXIT FIRST
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

                    # Short trade:
                    # high first = stop first
                    if (
                        distance_to_high
                        < distance_to_low
                    ):

                        result_r = -1.0

                    else:

                        result_r = (
                            reward_risk
                        )

                elif stop_hit:

                    result_r = -1.0

                else:

                    result_r = (
                        reward_risk
                    )

                open_trade[
                    "result_r"
                ] = result_r

                trades.append(
                    open_trade
                )

                open_trade = None

        # ==========================================
        # NO PYRAMIDING
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

        risk = (
            stop
            - reference_entry
        )

        if risk <= 0:

            continue

        target = (
            reference_entry
            - (
                risk
                * reward_risk
            )
        )

        open_trade = {

            "signal_time":
                signal["time"],

            "reference_entry":
                round(
                    reference_entry,
                    PRICE_PRECISION
                ),

            "backtest_entry":
                round(
                    backtest_entry,
                    PRICE_PRECISION
                ),

            "stop":
                round(
                    stop,
                    PRICE_PRECISION
                ),

            "target":
                round(
                    target,
                    PRICE_PRECISION
                ),

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

    if not trades:

        return None

    results = [
        trade["result_r"]
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

    else:

        profit_factor = float(
            "inf"
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

    expectancy_r = (
        total_r
        / len(
            results
        )
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
                expectancy_r,
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
# RESEARCH RUN
# ==================================================

def run_research():

    global RESEARCH_STATUS

    try:

        RESEARCH_STATUS = {
            "state":
                "fetching_data",

            "message":
                "Fetching OANDA candle data",

            "total_combinations":
                0,

            "completed_combinations":
                0,

            "rows_saved":
                0
        }

        start = parse_date(
            FROM_DATE
        )

        end = parse_date(
            TO_DATE
        )

        print()
        print(
            "========================================"
        )
        print(
            "EUR/USD SHORT STRATEGY RESEARCH"
        )
        print(
            "========================================"
        )

        print(
            f"Fetching H1 data from "
            f"{FROM_DATE} to {TO_DATE}..."
        )

        h1 = fetch_h1_history(

            INSTRUMENT,

            start
            - timedelta(
                days=60
            ),

            end
        )

        print(
            f"H1 candles loaded: "
            f"{len(h1)}"
        )

        print(
            "Fetching daily candles..."
        )

        daily = fetch_daily_history(
            INSTRUMENT,
            start,
            end
        )

        print(
            f"Daily candles loaded: "
            f"{len(daily)}"
        )

        print(
            "Calculating ATR..."
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
        # COMBINATIONS
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
                "Running parameter sweep",

            "total_combinations":
                total_combinations,

            "completed_combinations":
                0
        })

        print(
            f"Testing "
            f"{total_combinations} "
            f"combinations..."
        )

        results = []

        # ==========================================
        # RUN SWEEP
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
        # DATAFRAME
        # ==========================================

        df = pd.DataFrame(
            results
        )

        if df.empty:

            raise RuntimeError(
                "No strategy results "
                "were generated"
            )

        # ==========================================
        # REMOVE TINY SAMPLE SIZES
        # ==========================================

        df = df[
            df["trades"]
            >= 40
        ].copy()

        # ==========================================
        # SORT
        # ==========================================

        df = df.sort_values(

            by=[
                "profit_factor",
                "total_r",
                "trades"
            ],

            ascending=[
                False,
                False,
                False
            ]
        )

        # ==========================================
        # SAVE CSV
        # ==========================================

        df.to_csv(
            OUTPUT_FILE,
            index=False
        )

        RESEARCH_STATUS.update({

            "state":
                "complete",

            "message":
                "Research complete",

            "rows_saved":
                len(
                    df
                )
        })

        # ==========================================
        # PRINT TOP 25
        # ==========================================

        print()
        print(
            "========================================"
        )
        print(
            "TOP 25 RESULTS"
        )
        print(
            "========================================"
        )

        columns = [

            "body_ratio",

            "structure_lookback",

            "max_distance_atr",

            "reward_risk",

            "daily_ema",

            "trades",

            "winners",

            "win_rate",

            "profit_factor",

            "total_r",

            "expectancy_r",

            "max_drawdown_r",

            "longest_loss_streak"
        ]

        print(
            df[
                columns
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
            f"Saved CSV: "
            f"{OUTPUT_FILE}"
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
            f"ERROR: {error}"
        )


# ==================================================
# WEB ROUTES
# ==================================================

@app.route("/")
def home():

    file_exists = os.path.exists(
        OUTPUT_FILE
    )

    return jsonify({

        "service":
            "ERF EURUSD Short Research",

        "status":
            RESEARCH_STATUS,

        "csv_ready":
            file_exists,

        "download_endpoint":
            "/download",

        "status_endpoint":
            "/status",

        "trading_enabled":
            False,

        "orders_supported":
            False
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
                "CSV has not been "
                "generated yet. "
                "Check /status."
        }), 404

    return send_file(
        OUTPUT_FILE,
        as_attachment=True,
        download_name=
            "eurusd_short_sweep.csv"
    )


# ==================================================
# START
# ==================================================

if __name__ == "__main__":

    # Run research first.
    run_research()

    # Then start the tiny web server so the
    # result can be downloaded.
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
