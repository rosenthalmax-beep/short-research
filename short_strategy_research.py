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
LONDON_TZ = ZoneInfo("Europe/London")

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

H1_WARMUP_DAYS = 60
DAILY_WARMUP_DAYS = 1000

OUTPUT_FILE = "eurusd_short_session_weekday_sweep.csv"


# ==================================================
# TIGHT CORE PARAMETERS
# ==================================================

BODY_RATIOS = [
    1.20,
    1.30,
    1.40
]

STRUCTURE_LOOKBACKS = [
    40
]

MAX_DISTANCE_ATR_VALUES = [
    0.15,
    0.25
]

DAILY_EMA_LENGTHS = [
    150
]

REWARD_RISKS = [
    2.0,
    3.0,
    4.0
]


# ==================================================
# SESSION TESTS
#
# Each tuple:
# (
#   name,
#   timezone,
#   mode,
#   start_hour,
#   end_hour
# )
# ==================================================

SESSION_CONFIGS = [

    (
        "ALL_HOURS",
        "America/New_York",
        "all",
        None,
        None
    ),

    (
        "NY_00_05_EXCLUDED",
        "America/New_York",
        "exclude",
        0,
        5
    ),

    (
        "NY_01_03_EXCLUDED",
        "America/New_York",
        "exclude",
        1,
        3
    ),

    (
        "NY_08_17_INCLUDED",
        "America/New_York",
        "include",
        8,
        17
    ),

    (
        "NY_07_17_INCLUDED",
        "America/New_York",
        "include",
        7,
        17
    ),

    (
        "NY_08_14_INCLUDED",
        "America/New_York",
        "include",
        8,
        14
    ),

    (
        "NY_14_19_EXCLUDED",
        "America/New_York",
        "exclude",
        14,
        19
    ),

    (
        "LONDON_08_17_INCLUDED",
        "Europe/London",
        "include",
        8,
        17
    ),

    (
        "LONDON_07_17_INCLUDED",
        "Europe/London",
        "include",
        7,
        17
    ),

    (
        "LONDON_08_14_INCLUDED",
        "Europe/London",
        "include",
        8,
        14
    )
]


# ==================================================
# WEEKDAY TESTS
#
# Python weekday:
# Monday=0
# Tuesday=1
# Wednesday=2
# Thursday=3
# Friday=4
# ==================================================

WEEKDAY_CONFIGS = [

    (
        "ALL_DAYS",
        set()
    ),

    (
        "EXCLUDE_MONDAY",
        {0}
    ),

    (
        "EXCLUDE_TUESDAY",
        {1}
    ),

    (
        "EXCLUDE_WEDNESDAY",
        {2}
    ),

    (
        "EXCLUDE_THURSDAY",
        {3}
    ),

    (
        "EXCLUDE_FRIDAY",
        {4}
    ),

    (
        "EXCLUDE_MON_FRI",
        {0, 4}
    ),

    (
        "EXCLUDE_TUE_FRI",
        {1, 4}
    ),

    (
        "EXCLUDE_WED_THU",
        {2, 3}
    ),

    (
        "EXCLUDE_THU_FRI",
        {3, 4}
    )
]


# ==================================================
# STATUS
# ==================================================

RESEARCH_STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "completed_combinations": 0,
    "total_combinations": 0,
    "rows_saved": 0
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

        result.append(tr)

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
# SESSION + WEEKDAY
# ==================================================

def timezone_from_name(
    name
):

    if name == "America/New_York":
        return NY_TZ

    if name == "Europe/London":
        return LONDON_TZ

    return ZoneInfo(name)


def session_allowed(
    signal_time,
    timezone_name,
    mode,
    start_hour,
    end_hour
):

    if mode == "all":
        return True

    local_time = (
        signal_time
        .astimezone(
            timezone_from_name(
                timezone_name
            )
        )
    )

    inside = (
        local_time.hour
        >= start_hour
        and
        local_time.hour
        < end_hour
    )

    if mode == "include":
        return inside

    if mode == "exclude":
        return not inside

    raise ValueError(
        f"Unknown session mode: {mode}"
    )


def weekday_allowed(
    signal_time,
    timezone_name,
    excluded_weekdays
):

    local_time = (
        signal_time
        .astimezone(
            timezone_from_name(
                timezone_name
            )
        )
    )

    return (
        local_time.weekday()
        not in excluded_weekdays
    )


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
    max_distance_atr,
    session_timezone,
    session_mode,
    session_start,
    session_end,
    excluded_weekdays
):

    if index < max(
        14,
        structure_lookback
    ):
        return False

    signal = h1[index]

    previous = h1[
        index - 1
    ]

    current_atr = atr[index]

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

    body_allowed = (
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
        body_allowed
    )

    if not bearish_engulfing:
        return False

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
        <= current_atr
        * max_distance_atr
    )

    if not structure_allowed:
        return False

    daily = previous_daily_state(
        signal["time"],
        daily_state
    )

    if daily is None:
        return False

    if not (
        daily["close"]
        < daily["ema"]
    ):
        return False

    if not session_allowed(
        signal["time"],
        session_timezone,
        session_mode,
        session_start,
        session_end
    ):
        return False

    if not weekday_allowed(
        signal["time"],
        session_timezone,
        excluded_weekdays
    ):
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
    reward_risk,
    session_timezone,
    session_mode,
    session_start,
    session_end,
    excluded_weekdays
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

        candle = h1[index]
        candle_time = candle["time"]

        if candle_time < RESEARCH_FROM:
            continue

        if candle_time >= RESEARCH_TO:
            break

        # ==========================================
        # EXIT EXISTING POSITION
        # ==========================================

        if open_trade is not None:

            stop_hit = (
                candle["high"]
                >= open_trade["stop"]
            )

            target_hit = (
                candle["low"]
                <= open_trade["target"]
            )

            if stop_hit or target_hit:

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

                        exit_price = (
                            open_trade["stop"]
                        )

                    else:

                        exit_price = (
                            open_trade["target"]
                        )

                elif stop_hit:

                    exit_price = (
                        open_trade["stop"]
                    )

                else:

                    exit_price = (
                        open_trade["target"]
                    )

                actual_risk = (
                    open_trade["stop"]
                    - open_trade[
                        "backtest_entry"
                    ]
                )

                result_r = (
                    (
                        open_trade[
                            "backtest_entry"
                        ]
                        - exit_price
                    )
                    / actual_risk
                )

                open_trade[
                    "result_r"
                ] = result_r

                trades.append(
                    open_trade
                )

                open_trade = None

        # pyramiding 0
        if open_trade is not None:
            continue

        # ==========================================
        # ENTRY
        # ==========================================

        if not short_signal(
            h1,
            atr,
            index,
            daily_state,
            body_ratio_min,
            structure_lookback,
            max_distance_atr,
            session_timezone,
            session_mode,
            session_start,
            session_end,
            excluded_weekdays
        ):
            continue

        signal = h1[index]

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
            continue

        target = (
            reference_entry
            - (
                reference_risk
                * reward_risk
            )
        )

        open_trade = {
            "backtest_entry":
                backtest_entry,

            "stop":
                stop,

            "target":
                target,

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

    results = [
        trade["result_r"]
        for trade in trades
        if trade["result_r"]
        is not None
    ]

    if not results:
        return None

    winners = [
        x
        for x in results
        if x > 0
    ]

    losers = [
        x
        for x in results
        if x < 0
    ]

    total_r = sum(results)

    gross_profit = sum(winners)
    gross_loss = abs(sum(losers))

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
# RESEARCH
# ==================================================

def run_research():

    global RESEARCH_STATUS

    try:

        RESEARCH_STATUS[
            "state"
        ] = "fetching_data"

        print(
            "Fetching full EUR/USD H1 history..."
        )

        h1 = fetch_chunked_history(
            INSTRUMENT,
            "H1",
            RESEARCH_FROM
            - timedelta(
                days=H1_WARMUP_DAYS
            ),
            RESEARCH_TO
        )

        print(
            "Fetching daily history..."
        )

        daily = fetch_chunked_history(
            INSTRUMENT,
            "D",
            RESEARCH_FROM
            - timedelta(
                days=DAILY_WARMUP_DAYS
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

        atr = atr_series(
            h1,
            14
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

        combinations = list(
            itertools.product(
                BODY_RATIOS,
                STRUCTURE_LOOKBACKS,
                MAX_DISTANCE_ATR_VALUES,
                REWARD_RISKS,
                DAILY_EMA_LENGTHS,
                SESSION_CONFIGS,
                WEEKDAY_CONFIGS
            )
        )

        total = len(
            combinations
        )

        RESEARCH_STATUS.update({
            "state":
                "running",

            "message":
                "Testing session and weekday filters",

            "total_combinations":
                total,

            "completed_combinations":
                0
        })

        print(
            "Total combinations:",
            total
        )

        results = []

        for number, combo in enumerate(
            combinations,
            start=1
        ):

            (
                body_ratio,
                lookback,
                distance,
                rr,
                ema_length,
                session_config,
                weekday_config
            ) = combo

            (
                session_name,
                session_timezone,
                session_mode,
                session_start,
                session_end
            ) = session_config

            (
                weekday_name,
                excluded_weekdays
            ) = weekday_config

            trades = simulate(
                h1,
                atr,
                daily_cache[
                    ema_length
                ],
                body_ratio,
                lookback,
                distance,
                rr,
                session_timezone,
                session_mode,
                session_start,
                session_end,
                excluded_weekdays
            )

            stats = calculate_stats(
                trades
            )

            if stats is not None:

                row = {
                    "body_ratio":
                        body_ratio,

                    "structure_lookback":
                        lookback,

                    "max_distance_atr":
                        distance,

                    "reward_risk":
                        rr,

                    "daily_ema":
                        ema_length,

                    "session":
                        session_name,

                    "session_timezone":
                        session_timezone,

                    "weekday_filter":
                        weekday_name
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

            if number % 100 == 0:

                print(
                    f"{number}/{total}"
                )

        df = pd.DataFrame(
            results
        )

        if df.empty:

            raise RuntimeError(
                "No results generated"
            )

        # Keep enough sample size to avoid tiny,
        # meaningless optimised results.
        df = df[
            df["trades"]
            >= 100
        ].copy()

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
                "Session/weekday sweep complete",

            "rows_saved":
                len(df),

            "output_file":
                OUTPUT_FILE
        })

        print()
        print(
            "============================="
        )
        print(
            "TOP 30 RESULTS"
        )
        print(
            "============================="
        )

        print(
            df.head(30)
            .to_string(
                index=False
            )
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
            error
        )


# ==================================================
# ROUTES
# ==================================================

@app.route("/")
def home():

    return jsonify({
        "service":
            "EURUSD Short Session Research",

        "status":
            RESEARCH_STATUS,

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
                "not_ready"
        }), 404

    return send_file(
        OUTPUT_FILE,
        as_attachment=True,
        download_name=
            "eurusd_short_session_weekday_sweep.csv"
    )


# ==================================================
# START
# ==================================================

if __name__ == "__main__":

    research_thread = threading.Thread(
        target=run_research,
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
