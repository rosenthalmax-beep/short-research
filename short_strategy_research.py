from flask import Flask, jsonify, request, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
import os
import time
import threading
import requests

app = Flask(__name__)

# ==================================================
# ENVIRONMENT
# ==================================================

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_ACCOUNT_ID = os.getenv("OANDA_ACCOUNT_ID")

OANDA_URL = "https://api-fxtrade.oanda.com"

EXECUTOR_WEBHOOK_URL = os.getenv(
    "EXECUTOR_WEBHOOK_URL",
    "https://erf-oanda-executor-production-6f52.up.railway.app/webhook",
)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")

LIVE_SUBMISSION_ENABLED = (
    os.getenv("LIVE_SUBMISSION_ENABLED", "false")
    .strip()
    .lower()
    == "true"
)

# Short strategy submissions are kept behind their own switch
# so the strategy can be deployed/validated before SELL support
# is enabled in the executor.
SHORT_LIVE_SUBMISSION_ENABLED = (
    os.getenv("SHORT_LIVE_SUBMISSION_ENABLED", "false")
    .strip()
    .lower()
    == "true"
)

# GBP/USD short has its own final activation gate so the code can
# be deployed and read-only validated before it is allowed to trade.
GBPUSD_SHORT_LIVE_ENABLED = (
    os.getenv("GBPUSD_SHORT_LIVE_ENABLED", "false")
    .strip()
    .lower()
    == "true"
)

USDJPY_SHORT_LIVE_ENABLED = (
    os.getenv("USDJPY_SHORT_LIVE_ENABLED", "false")
    .strip()
    .lower()
    == "true"
)

USDCAD_SHORT_LIVE_ENABLED = (
    os.getenv("USDCAD_SHORT_LIVE_ENABLED", "false")
    .strip()
    .lower()
    == "true"
)

EURGBP_SHORT_LIVE_ENABLED = (
    os.getenv("EURGBP_SHORT_LIVE_ENABLED", "false")
    .strip()
    .lower()
    == "true"
)

LIVE_WATCHER_ENABLED = (
    os.getenv("LIVE_WATCHER_ENABLED", "true")
    .strip()
    .lower()
    == "true"
)

NY_TZ = ZoneInfo("America/New_York")

DAILY_ALIGNMENT_HOUR = 17
DAILY_ALIGNMENT_TIMEZONE = "America/New_York"

MAX_HISTORY_DAYS = 800
H1_CHUNK_DAYS = 180

BACKTEST_SLIPPAGE_TICKS = 5

# ==================================================
# LIVE WATCHER SETTINGS
# ==================================================

# Start looking just after the top of each UTC hour.
LIVE_POLL_OFFSET_SECONDS = 0.35

# If OANDA has not published the completed candle yet,
# retry this frequently.
LIVE_POLL_INTERVAL_SECONDS = 0.75

# Give OANDA up to 20 seconds to expose the new candle.
LIVE_POLL_WINDOW_SECONDS = 20

# Never submit an old signal after a restart/deployment.
MAX_LIVE_SIGNAL_AGE_SECONDS = 120

# Recent live events retained in memory for inspection.
LIVE_EVENTS = deque(maxlen=250)

LIVE_STATE_LOCK = threading.Lock()

LAST_PROCESSED_CANDLE = {}
LAST_PAIR_STATUS = {}

WATCHER_STARTED = False


# ==================================================
# LOCKED STRATEGIES
# ==================================================

STRATEGIES = {

    # ==============================================
    # EUR/USD
    # ==============================================

    "EUR_USD": {

        "tick_size": 0.00001,
        "price_precision": 5,
        "signal_id_prefix": "EURUSD",

        "minimum_body_ratio": 1.05,

        "strong_close_enabled": True,
        "minimum_close_location": 0.70,

        "lower_wick_filter_enabled": False,
        "minimum_lower_wick_body_ratio": None,

        "atr_length": 14,

        "structure_lookback": 20,
        "maximum_distance_atr": 0.15,

        "minimum_range_enabled": False,
        "minimum_range_atr": None,

        "fast_daily_ema": 30,
        "slow_daily_ema": 187,

        "require_daily_close_above_slow": True,
        "require_daily_fast_above_slow": True,

        "session_timezone": "America/New_York",

        # Include 08:00–16:59 New York
        "session_mode": "include",
        "session_start_hour": 8,
        "session_end_hour": 17,

        # Tuesday + Friday excluded
        "excluded_weekdays": {
            1,
            4
        },

        "reward_risk": 3.50,
        "stop_buffer_ticks": 10
    },

    # ==============================================
    # GBP/USD
    # ==============================================

    "GBP_USD": {

        "tick_size": 0.00001,
        "price_precision": 5,
        "signal_id_prefix": "GBPUSD",

        "minimum_body_ratio": 1.40,

        "strong_close_enabled": True,
        "minimum_close_location": 0.65,

        "lower_wick_filter_enabled": False,
        "minimum_lower_wick_body_ratio": None,

        "atr_length": 14,

        "structure_lookback": 20,
        "maximum_distance_atr": 0.25,

        "minimum_range_enabled": True,
        "minimum_range_atr": 0.90,

        "fast_daily_ema": 50,
        "slow_daily_ema": 70,

        "require_daily_close_above_slow": True,
        "require_daily_fast_above_slow": True,

        "session_timezone": "America/New_York",

        # Exclude 14:00–18:59 New York
        "session_mode": "exclude",
        "session_start_hour": 14,
        "session_end_hour": 19,

        "excluded_weekdays": set(),

        "reward_risk": 4.25,
        "stop_buffer_ticks": 10
    },

    # ==============================================
    # USD/JPY
    # ==============================================

    "USD_JPY": {

        "tick_size": 0.001,
        "price_precision": 3,
        "signal_id_prefix": "USDJPY",

        "minimum_body_ratio": 1.00,

        # Strong close disabled
        "strong_close_enabled": False,
        "minimum_close_location": 0.55,

        "lower_wick_filter_enabled": False,
        "minimum_lower_wick_body_ratio": None,

        "atr_length": 14,

        "structure_lookback": 17,
        "maximum_distance_atr": 0.55,

        "minimum_range_enabled": False,
        "minimum_range_atr": None,

        "fast_daily_ema": None,
        "slow_daily_ema": 425,

        "require_daily_close_above_slow": True,
        "require_daily_fast_above_slow": False,

        "session_timezone": "America/New_York",

        # Exclude 01:00–02:59 New York
        "session_mode": "exclude",
        "session_start_hour": 1,
        "session_end_hour": 3,

        # Wednesday + Thursday excluded
        "excluded_weekdays": {
            2,
            3
        },

        "reward_risk": 3.75,
        "stop_buffer_ticks": 10
    },

    # ==============================================
    # USD/CAD
    # ==============================================

    "USD_CAD": {

        "tick_size": 0.00001,
        "price_precision": 5,
        "signal_id_prefix": "USDCAD",

        "minimum_body_ratio": 1.00,

        # Strong close disabled
        "strong_close_enabled": False,
        "minimum_close_location": 0.75,

        # Lower wick >= 0.20 x current body
        "lower_wick_filter_enabled": True,
        "minimum_lower_wick_body_ratio": 0.20,

        "atr_length": 14,

        "structure_lookback": 40,
        "maximum_distance_atr": 0.20,

        "minimum_range_enabled": False,
        "minimum_range_atr": None,

        "fast_daily_ema": None,
        "slow_daily_ema": 200,

        "require_daily_close_above_slow": True,
        "require_daily_fast_above_slow": False,

        "session_timezone": "America/New_York",

        # Exclude 00:00–04:59 New York
        "session_mode": "exclude",
        "session_start_hour": 0,
        "session_end_hour": 5,

        "excluded_weekdays": set(),

        "reward_risk": 3.50,
        "stop_buffer_ticks": 10
    },

    # ==============================================
    # EUR/GBP
    # ==============================================

    "EUR_GBP": {

        "tick_size": 0.00001,
        "price_precision": 5,
        "signal_id_prefix": "EURGBP",

        "minimum_body_ratio": 1.00,

        "strong_close_enabled": True,
        "minimum_close_location": 0.75,

        "lower_wick_filter_enabled": False,
        "minimum_lower_wick_body_ratio": None,

        "atr_length": 14,

        "structure_lookback": 20,
        "maximum_distance_atr": 0.20,

        "minimum_range_enabled": False,
        "minimum_range_atr": None,

        "fast_daily_ema": 20,
        "slow_daily_ema": 150,

        "require_daily_close_above_slow": True,
        "require_daily_fast_above_slow": True,

        "session_timezone": "Europe/London",

        # Include 08:00–16:59 London
        "session_mode": "include",
        "session_start_hour": 8,
        "session_end_hour": 17,

        # Thursday + Friday excluded
        "excluded_weekdays": {
            3,
            4
        },

        "reward_risk": 3.00,
        "stop_buffer_ticks": 10
    }
}


# ==================================================
# LOCKED SHORT STRATEGIES
# ==================================================

SHORT_STRATEGIES = {

    # ==============================================
    # EUR/USD BALANCED_815 SHORT
    # ==============================================

    "EUR_USD": {

        "strategy_name": "BALANCED_815",
        "tick_size": 0.00001,
        "price_precision": 5,
        "signal_id_prefix": "EURUSDSHORT",

        "minimum_body_ratio": 1.10,
        "maximum_close_location": 0.275,

        "atr_length": 14,
        "structure_lookback": 55,
        "maximum_distance_atr": 0.35,

        "fast_daily_ema": 85,
        "slow_daily_ema": 100,
        "require_daily_fast_below_slow": True,

        # EUR/USD requires EMA separation, but not slope/volatility.
        "minimum_daily_ema_separation_atr": 0.05,
        "maximum_slow_ema_slope_5d_atr": None,
        "minimum_daily_atr_ratio_50": None,

        "session_timezone": "America/New_York",
        "excluded_hours": {2, 10, 12, 14},
        "excluded_weekdays": set(),

        "reward_risk": 4.00,
        "stop_buffer_ticks": 10
    },

    # ==============================================
    # GBP/USD FINAL SHORT
    # ==============================================

    "GBP_USD": {

        "strategy_name": "GBPUSD_FINAL_SHORT",
        "tick_size": 0.00001,
        "price_precision": 5,
        "signal_id_prefix": "GBPUSDSHORT",

        "minimum_body_ratio": 1.00,

        # Strong-close filter is intentionally OFF.
        "maximum_close_location": None,

        "atr_length": 14,
        "structure_lookback": 70,
        "maximum_distance_atr": 0.175,

        "fast_daily_ema": 40,
        "slow_daily_ema": 100,
        "require_daily_fast_below_slow": True,

        # No EMA-separation filter.
        "minimum_daily_ema_separation_atr": None,

        # EMA100 must have fallen by at least 0.05 Daily ATR14
        # over the previous five completed daily bars.
        "maximum_slow_ema_slope_5d_atr": -0.05,

        # Daily ATR14 must be at least 80% of its 50-day mean.
        "minimum_daily_atr_ratio_50": 0.80,

        "session_timezone": "America/New_York",

        # Only the two independently validated weak hours.
        "excluded_hours": {3, 15},
        "excluded_weekdays": set(),

        "reward_risk": 2.50,
        "stop_buffer_ticks": 10
    },

    "USD_JPY": {
        "strategy_name": "USDJPY_FINAL_SHORT",
        "tick_size": 0.001,
        "price_precision": 3,
        "signal_id_prefix": "USDJPYSHORT",

        "minimum_body_ratio": 1.45,
        "maximum_close_location": None,

        "atr_length": 14,
        "structure_lookback": 90,
        "maximum_distance_atr": 0.50,

        "fast_daily_ema": 90,
        "slow_daily_ema": 90,
        "require_daily_fast_below_slow": False,

        "minimum_daily_ema_separation_atr": None,
        "maximum_slow_ema_slope_5d_atr": None,
        "minimum_daily_atr_ratio_50": None,

        "session_timezone": "America/New_York",
        "excluded_hours": {1, 5, 6, 10, 11},
        "excluded_weekdays": set(),

        "reward_risk": 2.50,
        "stop_buffer_ticks": 10
    },

    # ==============================================
    # USD/CAD FINAL SHORT
    # ==============================================
    "USD_CAD": {
        "strategy_name": "USDCAD_FINAL_SHORT",
        "tick_size": 0.00001,
        "price_precision": 5,
        "signal_id_prefix": "USDCADSHORT",

        "minimum_body_ratio": 1.40,
        "maximum_close_location": None,

        "atr_length": 14,
        "structure_lookback": 60,
        "maximum_distance_atr": 0.25,

        # Previous completed daily close must be below EMA300.
        # Fast EMA is set to the same length only so the existing daily-state
        # builder remains unchanged; alignment is disabled for this strategy.
        "fast_daily_ema": 300,
        "slow_daily_ema": 300,
        "require_daily_fast_below_slow": False,

        "minimum_daily_ema_separation_atr": None,
        "maximum_slow_ema_slope_5d_atr": None,
        "minimum_daily_atr_ratio_50": None,

        "momentum_lookback_bars": 24,
        "minimum_upward_momentum_atr": 0.50,
        "minimum_signal_range_atr": 0.90,
        "maximum_stop_size_atr": 1.60,

        "session_timezone": "America/New_York",
        "excluded_hours": {18},
        "excluded_weekdays": set(),

        "reward_risk": 3.25,
        "stop_buffer_ticks": 10
    },

    # ==============================================
    # EUR/GBP FINAL CONFIRMED SHORT
    # ==============================================
    "EUR_GBP": {
        "strategy_name": "EURGBP_CONFIRMED_SHORT",
        "tick_size": 0.00001,
        "price_precision": 5,
        "signal_id_prefix": "EURGBPSHORT",

        "minimum_body_ratio": 1.00,
        "maximum_close_location": 0.20,

        "atr_length": 14,
        "structure_lookback": 90,
        "maximum_distance_atr": 0.075,

        # No daily EMA regime in the frozen EUR/GBP short.
        # Length-1 placeholders keep the shared daily builder intact.
        "fast_daily_ema": 1,
        "slow_daily_ema": 1,
        "require_daily_close_below_slow": False,
        "require_daily_fast_below_slow": False,
        "minimum_daily_ema_separation_atr": None,
        "maximum_slow_ema_slope_5d_atr": None,
        "minimum_daily_atr_ratio_50": None,

        # ROBUST trigger + HIGH-PF confirmation intersection.
        "momentum_requirements": {12: 0.25, 48: 1.00},
        "minimum_signal_range_atr": 1.10,
        "maximum_stop_size_atr": 2.50,
        "minimum_upper_wick_body_ratio": 0.10,
        "minimum_h1_atr_ratio_50": 0.80,

        "session_timezone": "America/New_York",
        "excluded_hours": {9},
        "excluded_weekdays": set(),

        "reward_risk": 3.00,
        "stop_buffer_ticks": 10
    }

}


# ==================================================
# OANDA HTTP
# ==================================================

def oanda_headers():

    if not OANDA_TOKEN:
        raise RuntimeError(
            "OANDA_TOKEN is not configured"
        )

    return {
        "Authorization": f"Bearer {OANDA_TOKEN}",
        "Content-Type": "application/json"
    }


def oanda_get(path, params=None):

    response = requests.get(
        OANDA_URL + path,
        headers=oanda_headers(),
        params=params,
        timeout=20
    )

    response.raise_for_status()

    return response.json()


# ==================================================
# GENERAL HELPERS
# ==================================================

def utc_now():

    return datetime.now(
        timezone.utc
    )


def iso_utc(dt):

    return (
        dt
        .astimezone(timezone.utc)
        .isoformat()
        .replace(
            "+00:00",
            "Z"
        )
    )


def iso_oanda(dt):

    return iso_utc(
        dt
    )


def round_price(
    value,
    config
):

    return round(
        value,
        config[
            "price_precision"
        ]
    )


def parse_date(value):

    try:

        parsed = datetime.fromisoformat(
            value.replace(
                "Z",
                "+00:00"
            )
        )

    except Exception:

        try:

            parsed = datetime.strptime(
                value,
                "%Y-%m-%d"
            ).replace(
                tzinfo=timezone.utc
            )

        except Exception:

            raise ValueError(
                "Date must be YYYY-MM-DD "
                "or ISO-8601"
            )

    if parsed.tzinfo is None:

        parsed = parsed.replace(
            tzinfo=timezone.utc
        )

    return parsed.astimezone(
        timezone.utc
    )


def strategy_timezone(
    config
):

    return ZoneInfo(
        config[
            "session_timezone"
        ]
    )


def signal_id_for(
    instrument,
    signal_close_utc
):

    config = (
        STRATEGIES[
            instrument
        ]
    )

    milliseconds = int(
        signal_close_utc.timestamp()
        * 1000
    )

    return (
        f'{config["signal_id_prefix"]}-'
        f"{milliseconds}"
    )


def short_signal_id_for(
    instrument,
    signal_close_utc
):

    config = (
        SHORT_STRATEGIES[
            instrument
        ]
    )

    milliseconds = int(
        signal_close_utc.timestamp()
        * 1000
    )

    return (
        f'{config["signal_id_prefix"]}-'
        f"{milliseconds}"
    )


def add_live_event(
    event_type,
    instrument=None,
    **extra
):

    event = {

        "recorded_at_utc":
            iso_utc(
                utc_now()
            ),

        "event":
            event_type
    }

    if instrument is not None:

        event[
            "instrument"
        ] = instrument

    event.update(
        extra
    )

    with LIVE_STATE_LOCK:

        LIVE_EVENTS.appendleft(
            event
        )

        if instrument is not None:

            LAST_PAIR_STATUS[
                instrument
            ] = event

    return event


# ==================================================
# CANDLE PARSING
# ==================================================

def parse_candle(
    candle
):

    if not candle.get(
        "complete",
        False
    ):

        return None

    mid = candle.get(
        "mid"
    )

    if not mid:

        return None

    return {

        "time":
            datetime.fromisoformat(
                candle[
                    "time"
                ].replace(
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
            ),

        "volume":
            int(
                candle.get(
                    "volume",
                    0
                )
            )
    }


def candle_params(
    granularity
):

    return {

        "price":
            "M",

        "granularity":
            granularity,

        "smooth":
            "false",

        "dailyAlignment":
            DAILY_ALIGNMENT_HOUR,

        "alignmentTimezone":
            DAILY_ALIGNMENT_TIMEZONE
    }


# ==================================================
# CANDLE FETCHING
# ==================================================

def fetch_candles_count(
    instrument,
    granularity,
    count
):

    params = candle_params(
        granularity
    )

    params[
        "count"
    ] = count

    data = oanda_get(

        f"/v3/instruments/"
        f"{instrument}/candles",

        params=params
    )

    result = []

    for raw in data.get(
        "candles",
        []
    ):

        candle = parse_candle(
            raw
        )

        if candle is not None:

            result.append(
                candle
            )

    result.sort(
        key=lambda item:
            item["time"]
    )

    return result


def fetch_candles_range(
    instrument,
    granularity,
    start,
    end
):

    params = candle_params(
        granularity
    )

    params.update({

        "from":
            iso_oanda(
                start
            ),

        "to":
            iso_oanda(
                end
            ),

        "includeFirst":
            "true"
    })

    data = oanda_get(

        f"/v3/instruments/"
        f"{instrument}/candles",

        params=params
    )

    result = []

    for raw in data.get(
        "candles",
        []
    ):

        candle = parse_candle(
            raw
        )

        if candle is not None:

            result.append(
                candle
            )

    result.sort(
        key=lambda item:
            item["time"]
    )

    return result


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

        chunk = fetch_candles_range(
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

    # Keep the validated long warm-up.
    warmup_start = (
        start
        - timedelta(
            days=1500
        )
    )

    return fetch_candles_range(
        instrument,
        "D",
        warmup_start,
        end
    )


def fetch_latest_complete_h1(
    instrument
):

    candles = fetch_candles_count(
        instrument,
        "H1",
        3
    )

    if not candles:

        return None

    return candles[-1]


# ==================================================
# INDICATORS
# ==================================================

def ema_series(
    values,
    length
):

    if len(values) < length:

        raise ValueError(
            f"Not enough values "
            f"for EMA{length}"
        )

    result = [
        None
    ] * len(
        values
    )

    multiplier = (
        2.0
        / (
            length + 1.0
        )
    )

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

    if len(values) < length:

        raise ValueError(
            f"Not enough values "
            f"for RMA{length}"
        )

    result = [
        None
    ] * len(
        values
    )

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
    length
):

    return rma_series(
        true_ranges(
            candles
        ),
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
    config
):

    closes = [
        candle["close"]
        for candle in daily
    ]

    fast_length = (
        config[
            "fast_daily_ema"
        ]
    )

    slow_length = (
        config[
            "slow_daily_ema"
        ]
    )

    if fast_length is not None:

        fast_ema = ema_series(
            closes,
            fast_length
        )

    else:

        fast_ema = [
            None
        ] * len(
            closes
        )

    slow_ema = ema_series(
        closes,
        slow_length
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

            "fast_ema":
                fast_ema[
                    index
                ],

            "slow_ema":
                slow_ema[
                    index
                ]
        })

    return result


def previous_daily_values(
    signal_time,
    daily_state,
    config
):

    session_start = (
        current_daily_start(
            signal_time
        )
    )

    selected = None

    require_fast = (
        config[
            "require_daily_fast_above_slow"
        ]
    )

    for row in daily_state:

        slow_ready = (
            row[
                "slow_ema"
            ]
            is not None
        )

        fast_ready = (
            not require_fast
            or
            row[
                "fast_ema"
            ]
            is not None
        )

        if (
            row["time"]
            < session_start
            and
            slow_ready
            and
            fast_ready
        ):

            selected = row

        elif (
            row["time"]
            >= session_start
        ):

            break

    return selected


# ==================================================
# SHORT DAILY STATE
# ==================================================

def build_short_daily_state(
    daily,
    config
):

    closes = [
        candle["close"]
        for candle in daily
    ]

    fast_ema = ema_series(
        closes,
        config[
            "fast_daily_ema"
        ]
    )

    slow_ema = ema_series(
        closes,
        config[
            "slow_daily_ema"
        ]
    )

    daily_atr = atr_series(
        daily,
        14
    )

    # 50-day simple average of Daily ATR14.
    daily_atr_sma50 = [None] * len(daily_atr)
    rolling_sum = 0.0
    rolling_values = []

    for index, value in enumerate(daily_atr):

        if value is None:
            rolling_values.append(None)
            continue

        rolling_values.append(value)
        rolling_sum += value

        if len(rolling_values) > 50:
            removed = rolling_values[-51]
            if removed is not None:
                rolling_sum -= removed

        window = rolling_values[-50:]

        if (
            len(window) == 50
            and all(
                item is not None
                for item in window
            )
        ):
            daily_atr_sma50[index] = (
                rolling_sum / 50.0
            )

    result = []

    for index, candle in enumerate(
        daily
    ):

        slow_slope_5d_atr = None

        if (
            index >= 5
            and slow_ema[index] is not None
            and slow_ema[index - 5] is not None
            and daily_atr[index] is not None
            and daily_atr[index] > 0
        ):
            slow_slope_5d_atr = (
                slow_ema[index]
                - slow_ema[index - 5]
            ) / daily_atr[index]

        daily_atr_ratio_50 = None

        # Match the research warmup convention exactly:
        # ATR14 first becomes valid after its 14-bar seed, then
        # the 50-day ATR mean is not exposed until index 63.
        if (
            index >= 63
            and daily_atr[index] is not None
            and daily_atr_sma50[index] is not None
            and daily_atr_sma50[index] > 0
        ):
            daily_atr_ratio_50 = (
                daily_atr[index]
                / daily_atr_sma50[index]
            )

        result.append({

            "time":
                candle["time"],

            "close":
                candle["close"],

            "fast_ema":
                fast_ema[index],

            "slow_ema":
                slow_ema[index],

            "daily_atr":
                daily_atr[index],

            "slow_ema_slope_5d_atr":
                slow_slope_5d_atr,

            "daily_atr_ratio_50":
                daily_atr_ratio_50
        })

    return result


def previous_short_daily_values(
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

        ready = (
            row["fast_ema"] is not None
            and row["slow_ema"] is not None
            and row["daily_atr"] is not None
        )

        if (
            row["time"] < session_start
            and ready
        ):
            selected = row

        elif row["time"] >= session_start:
            break

    return selected


# ==================================================
# SESSION / WEEKDAY
# ==================================================

def local_signal_time(
    signal_time,
    config
):

    return signal_time.astimezone(
        strategy_timezone(
            config
        )
    )


def session_allowed_for(
    signal_time,
    config
):

    local_time = (
        local_signal_time(
            signal_time,
            config
        )
    )

    hour = (
        local_time.hour
    )

    inside_window = (
        hour
        >= config[
            "session_start_hour"
        ]
        and
        hour
        < config[
            "session_end_hour"
        ]
    )

    mode = (
        config[
            "session_mode"
        ]
    )

    if mode == "include":

        return inside_window

    if mode == "exclude":

        return not inside_window

    raise ValueError(
        f"Unknown session mode: "
        f"{mode}"
    )


def weekday_allowed_for(
    signal_time,
    config
):

    local_time = (
        local_signal_time(
            signal_time,
            config
        )
    )

    return (
        local_time.weekday()
        not in config[
            "excluded_weekdays"
        ]
    )


# ==================================================
# SIGNAL LOGIC
# ==================================================

def evaluate_signal_at_index(
    instrument,
    h1,
    atr,
    index,
    daily_state
):

    config = (
        STRATEGIES[
            instrument
        ]
    )

    minimum_index = max(

        config[
            "atr_length"
        ],

        config[
            "structure_lookback"
        ]
    )

    if index < minimum_index:

        return None

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

        return None

    # ==============================================
    # CANDLE VALUES
    # ==============================================

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

    lower_wick = (
        min(
            signal["open"],
            signal["close"]
        )
        - signal["low"]
    )

    close_location = (

        (
            signal["close"]
            - signal["low"]
        )
        / signal_range

        if signal_range > 0

        else 0.0
    )

    body_ratio = (

        current_body
        / previous_body

        if previous_body > 0

        else None
    )

    # ==============================================
    # BODY RATIO
    # ==============================================

    body_ratio_allowed = (
        previous_body > 0
        and
        current_body
        >= (
            previous_body
            * config[
                "minimum_body_ratio"
            ]
        )
    )

    # ==============================================
    # BULLISH ENGULFING
    # ==============================================

    bullish_engulfing = (

        previous[
            "close"
        ]
        < previous[
            "open"
        ]

        and

        signal[
            "close"
        ]
        > signal[
            "open"
        ]

        and

        signal[
            "open"
        ]
        <= previous[
            "close"
        ]

        and

        signal[
            "close"
        ]
        >= previous[
            "open"
        ]

        and

        body_ratio_allowed
    )

    # ==============================================
    # STRONG CLOSE
    # ==============================================

    if config[
        "strong_close_enabled"
    ]:

        strong_close_allowed = (
            close_location
            >= config[
                "minimum_close_location"
            ]
        )

    else:

        strong_close_allowed = True

    # ==============================================
    # LOWER WICK
    # ==============================================

    if config[
        "lower_wick_filter_enabled"
    ]:

        lower_wick_allowed = (
            lower_wick
            >= (
                current_body
                * config[
                    "minimum_lower_wick_body_ratio"
                ]
            )
        )

    else:

        lower_wick_allowed = True

    # ==============================================
    # MINIMUM RANGE
    # ==============================================

    if config[
        "minimum_range_enabled"
    ]:

        minimum_range_allowed = (
            signal_range
            >= (
                current_atr
                * config[
                    "minimum_range_atr"
                ]
            )
        )

    else:

        minimum_range_allowed = True

    # ==============================================
    # STRUCTURE
    # ==============================================

    lookback = (
        config[
            "structure_lookback"
        ]
    )

    previous_bars = (
        h1[
            index - lookback:
            index
        ]
    )

    previous_lowest_low = min(
        candle["low"]
        for candle
        in previous_bars
    )

    distance_from_recent_low = (
        signal["low"]
        - previous_lowest_low
    )

    maximum_distance = (
        current_atr
        * config[
            "maximum_distance_atr"
        ]
    )

    structure_allowed = (
        distance_from_recent_low
        <= maximum_distance
    )

    # ==============================================
    # DAILY
    # ==============================================

    daily = (
        previous_daily_values(
            signal["time"],
            daily_state,
            config
        )
    )

    if daily is None:

        return None

    if config[
        "require_daily_close_above_slow"
    ]:

        daily_regime_allowed = (
            daily["close"]
            > daily["slow_ema"]
        )

    else:

        daily_regime_allowed = True

    if config[
        "require_daily_fast_above_slow"
    ]:

        daily_alignment_allowed = (
            daily["fast_ema"]
            > daily["slow_ema"]
        )

    else:

        daily_alignment_allowed = True

    # ==============================================
    # SESSION / WEEKDAY
    # ==============================================

    session_allowed = (
        session_allowed_for(
            signal["time"],
            config
        )
    )

    weekday_allowed = (
        weekday_allowed_for(
            signal["time"],
            config
        )
    )

    local_time = (
        local_signal_time(
            signal["time"],
            config
        )
    )

    # ==============================================
    # FINAL
    # ==============================================

    qualified = all([

        bullish_engulfing,
        strong_close_allowed,
        lower_wick_allowed,
        minimum_range_allowed,
        structure_allowed,
        daily_regime_allowed,
        daily_alignment_allowed,
        session_allowed,
        weekday_allowed
    ])

    return {

        "qualified":
            qualified,

        "signal_start_utc":
            signal["time"],

        "signal_close_utc":
            signal["time"]
            + timedelta(
                hours=1
            ),

        "open":
            signal["open"],

        "high":
            signal["high"],

        "low":
            signal["low"],

        "close":
            signal["close"],

        "atr":
            current_atr,

        "body_ratio":
            body_ratio,

        "close_location":
            close_location,

        "lower_wick":
            lower_wick,

        "lower_wick_body_ratio":
            (
                lower_wick
                / current_body

                if current_body > 0

                else None
            ),

        "local_timezone":
            config[
                "session_timezone"
            ],

        "local_hour":
            local_time.hour,

        "weekday":
            local_time.strftime(
                "%A"
            ),

        "previous_daily_close":
            daily[
                "close"
            ],

        "previous_daily_fast_ema":
            daily[
                "fast_ema"
            ],

        "previous_daily_slow_ema":
            daily[
                "slow_ema"
            ]
    }


# ==================================================
# SHORT SIGNAL LOGIC
# ==================================================

def evaluate_short_signal_at_index(
    instrument,
    h1,
    atr,
    index,
    daily_state
):

    if instrument not in SHORT_STRATEGIES:
        raise ValueError(
            f"{instrument} has no short strategy"
        )

    config = SHORT_STRATEGIES[instrument]

    momentum_requirements = config.get(
        "momentum_requirements",
        {}
    )

    maximum_momentum_lookback = max(
        momentum_requirements.keys(),
        default=0
    )

    h1_atr_ratio_warmup = (
        config["atr_length"] + 49
        if config.get("minimum_h1_atr_ratio_50") is not None
        else 0
    )

    minimum_index = max(
        config["atr_length"],
        config["structure_lookback"],
        config.get("momentum_lookback_bars", 0),
        maximum_momentum_lookback,
        h1_atr_ratio_warmup
    )

    if index < minimum_index:
        return None

    signal = h1[index]
    previous = h1[index - 1]
    current_atr = atr[index]

    if current_atr is None:
        return None

    previous_body = abs(
        previous["close"] - previous["open"]
    )
    current_body = abs(
        signal["close"] - signal["open"]
    )
    signal_range = (
        signal["high"] - signal["low"]
    )

    close_location = (
        (signal["close"] - signal["low"])
        / signal_range
        if signal_range > 0
        else 1.0
    )

    body_ratio = (
        current_body / previous_body
        if previous_body > 0
        else None
    )

    body_ratio_allowed = (
        previous_body > 0
        and current_body >= (
            previous_body
            * config["minimum_body_ratio"]
        )
    )

    bearish_engulfing = (
        previous["close"] > previous["open"]
        and signal["close"] < signal["open"]
        and signal["open"] >= previous["close"]
        and signal["close"] <= previous["open"]
        and body_ratio_allowed
    )

    maximum_close_location = config.get(
        "maximum_close_location"
    )
    strong_close_allowed = (
        True
        if maximum_close_location is None
        else close_location <= maximum_close_location
    )

    lookback = config["structure_lookback"]
    previous_bars = h1[index - lookback:index]
    previous_highest_high = max(
        candle["high"]
        for candle in previous_bars
    )
    distance_from_recent_high = (
        previous_highest_high
        - signal["high"]
    )
    maximum_distance = (
        current_atr
        * config["maximum_distance_atr"]
    )
    structure_allowed = (
        distance_from_recent_high
        <= maximum_distance
    )

    daily = previous_short_daily_values(
        signal["time"],
        daily_state
    )
    if daily is None:
        return None

    require_daily_close_below_slow = config.get(
        "require_daily_close_below_slow",
        True
    )
    daily_regime_allowed = (
        True
        if not require_daily_close_below_slow
        else daily["close"] < daily["slow_ema"]
    )
    require_fast_below_slow = config.get(
        "require_daily_fast_below_slow",
        True
    )
    daily_alignment_allowed = (
        True
        if not require_fast_below_slow
        else daily["fast_ema"] < daily["slow_ema"]
    )

    daily_separation = (
        (
            daily["slow_ema"]
            - daily["fast_ema"]
        ) / daily["daily_atr"]
        if daily["daily_atr"] > 0
        else None
    )

    minimum_separation = config.get(
        "minimum_daily_ema_separation_atr"
    )
    daily_separation_allowed = (
        True
        if minimum_separation is None
        else (
            daily_separation is not None
            and daily_separation >= minimum_separation
        )
    )

    maximum_slope = config.get(
        "maximum_slow_ema_slope_5d_atr"
    )
    daily_slope = daily.get(
        "slow_ema_slope_5d_atr"
    )
    daily_slope_allowed = (
        True
        if maximum_slope is None
        else (
            daily_slope is not None
            and daily_slope <= maximum_slope
        )
    )

    minimum_atr_ratio = config.get(
        "minimum_daily_atr_ratio_50"
    )
    daily_atr_ratio = daily.get(
        "daily_atr_ratio_50"
    )
    daily_atr_ratio_allowed = (
        True
        if minimum_atr_ratio is None
        else (
            daily_atr_ratio is not None
            and daily_atr_ratio >= minimum_atr_ratio
        )
    )

    # Optional H1 filters. Existing strategies leave these absent unless used.
    momentum_lookback = config.get("momentum_lookback_bars")
    minimum_upward_momentum_atr = config.get(
        "minimum_upward_momentum_atr"
    )
    upward_momentum = None
    upward_momentum_atr = None

    if momentum_lookback is not None:
        momentum_reference_close = h1[index - momentum_lookback]["close"]
        upward_momentum = signal["close"] - momentum_reference_close
        upward_momentum_atr = (
            upward_momentum / current_atr
            if current_atr > 0
            else None
        )

    upward_momentum_allowed = (
        True
        if minimum_upward_momentum_atr is None
        else (
            upward_momentum_atr is not None
            and upward_momentum_atr >= minimum_upward_momentum_atr
        )
    )

    momentum_requirement_values = {}
    momentum_requirements_allowed = True

    for lookback_bars, minimum_atr in momentum_requirements.items():
        momentum_atr_value = (
            signal["close"] - h1[index - lookback_bars]["close"]
        ) / current_atr
        momentum_requirement_values[lookback_bars] = momentum_atr_value
        if momentum_atr_value < minimum_atr:
            momentum_requirements_allowed = False

    minimum_signal_range_atr = config.get("minimum_signal_range_atr")
    signal_range_atr = (
        signal_range / current_atr
        if current_atr > 0
        else None
    )
    signal_range_allowed = (
        True
        if minimum_signal_range_atr is None
        else (
            signal_range_atr is not None
            and signal_range_atr >= minimum_signal_range_atr
        )
    )

    maximum_stop_size_atr = config.get("maximum_stop_size_atr")
    stop_price_for_filter = (
        signal["high"]
        + config["stop_buffer_ticks"] * config["tick_size"]
    )
    stop_size = stop_price_for_filter - signal["close"]
    stop_size_atr = (
        stop_size / current_atr
        if current_atr > 0
        else None
    )
    stop_size_allowed = (
        True
        if maximum_stop_size_atr is None
        else (
            stop_size_atr is not None
            and stop_size_atr <= maximum_stop_size_atr
        )
    )

    upper_wick = max(
        0.0,
        signal["high"] - max(signal["open"], signal["close"])
    )
    upper_wick_body_ratio = (
        upper_wick / current_body
        if current_body > 0
        else None
    )
    minimum_upper_wick_body_ratio = config.get(
        "minimum_upper_wick_body_ratio"
    )
    upper_wick_allowed = (
        True
        if minimum_upper_wick_body_ratio is None
        else (
            upper_wick_body_ratio is not None
            and upper_wick_body_ratio >= minimum_upper_wick_body_ratio
        )
    )

    minimum_h1_atr_ratio_50 = config.get(
        "minimum_h1_atr_ratio_50"
    )
    h1_atr_ratio_50 = None

    if minimum_h1_atr_ratio_50 is not None:
        atr_window = atr[index - 49:index + 1]
        if (
            len(atr_window) == 50
            and all(value is not None for value in atr_window)
        ):
            atr_mean_50 = sum(atr_window) / 50.0
            if atr_mean_50 > 0:
                h1_atr_ratio_50 = current_atr / atr_mean_50

    h1_atr_ratio_allowed = (
        True
        if minimum_h1_atr_ratio_50 is None
        else (
            h1_atr_ratio_50 is not None
            and h1_atr_ratio_50 >= minimum_h1_atr_ratio_50
        )
    )

    local_time = signal["time"].astimezone(
        ZoneInfo(config["session_timezone"])
    )
    session_allowed = (
        local_time.hour
        not in config["excluded_hours"]
    )
    weekday_allowed = (
        local_time.weekday()
        not in config["excluded_weekdays"]
    )

    qualified = all([
        bearish_engulfing,
        strong_close_allowed,
        structure_allowed,
        daily_regime_allowed,
        daily_alignment_allowed,
        daily_separation_allowed,
        daily_slope_allowed,
        daily_atr_ratio_allowed,
        upward_momentum_allowed,
        momentum_requirements_allowed,
        signal_range_allowed,
        stop_size_allowed,
        upper_wick_allowed,
        h1_atr_ratio_allowed,
        session_allowed,
        weekday_allowed
    ])

    return {
        "qualified": qualified,
        "side": "SELL",
        "strategy_name": config["strategy_name"],
        "signal_start_utc": signal["time"],
        "signal_close_utc": signal["time"] + timedelta(hours=1),
        "open": signal["open"],
        "high": signal["high"],
        "low": signal["low"],
        "close": signal["close"],
        "atr": current_atr,
        "body_ratio": body_ratio,
        "close_location": close_location,
        "previous_highest_high": previous_highest_high,
        "distance_from_recent_high": distance_from_recent_high,
        "distance_from_recent_high_atr": (
            distance_from_recent_high / current_atr
            if current_atr > 0
            else None
        ),
        "local_timezone": config["session_timezone"],
        "local_hour": local_time.hour,
        "weekday": local_time.strftime("%A"),
        "previous_daily_close": daily["close"],
        "previous_daily_fast_ema": daily["fast_ema"],
        "previous_daily_slow_ema": daily["slow_ema"],
        "previous_daily_atr14": daily["daily_atr"],
        "daily_ema_separation_atr": daily_separation,
        "slow_ema_slope_5d_atr": daily_slope,
        "daily_atr_ratio_50": daily_atr_ratio,
        "upward_momentum": upward_momentum,
        "upward_momentum_atr": upward_momentum_atr,
        "momentum_requirements_atr": momentum_requirement_values,
        "signal_range_atr": signal_range_atr,
        "stop_size": stop_size,
        "stop_size_atr": stop_size_atr,
        "upper_wick_body_ratio": upper_wick_body_ratio,
        "h1_atr_ratio_50": h1_atr_ratio_50
    }


def evaluate_latest_short(
    instrument
):

    if instrument not in SHORT_STRATEGIES:

        raise ValueError(
            f"{instrument} has no short strategy"
        )

    config = (
        SHORT_STRATEGIES[
            instrument
        ]
    )

    started = (
        time.perf_counter()
    )

    h1_started = (
        time.perf_counter()
    )

    h1 = fetch_candles_count(
        instrument,
        "H1",
        750
    )

    h1_fetch_ms = (
        time.perf_counter()
        - h1_started
    ) * 1000

    daily_started = (
        time.perf_counter()
    )

    daily = fetch_candles_count(
        instrument,
        "D",
        2500
    )

    daily_fetch_ms = (
        time.perf_counter()
        - daily_started
    ) * 1000

    if not h1:

        raise ValueError(
            "No completed H1 candles"
        )

    atr = atr_series(
        h1,
        config[
            "atr_length"
        ]
    )

    daily_state = (
        build_short_daily_state(
            daily,
            config
        )
    )

    result = (
        evaluate_short_signal_at_index(
            instrument,
            h1,
            atr,
            len(h1) - 1,
            daily_state
        )
    )

    if result is None:

        raise ValueError(
            "Could not evaluate latest "
            "completed candle for short strategy"
        )

    result[
        "timing"
    ] = {

        "h1_fetch_ms":
            round(
                h1_fetch_ms,
                2
            ),

        "daily_fetch_ms":
            round(
                daily_fetch_ms,
                2
            ),

        "total_evaluation_ms":
            round(
                (
                    time.perf_counter()
                    - started
                )
                * 1000,
                2
            )
    }

    return result


# ==================================================
# LATEST FULL EVALUATION
# ==================================================

def evaluate_latest(
    instrument
):

    if instrument not in STRATEGIES:

        raise ValueError(
            f"{instrument} is not supported"
        )

    config = (
        STRATEGIES[
            instrument
        ]
    )

    started = (
        time.perf_counter()
    )

    # ==============================================
    # H1
    # ==============================================

    h1_started = (
        time.perf_counter()
    )

    h1 = fetch_candles_count(
        instrument,
        "H1",
        750
    )

    h1_fetch_ms = (
        time.perf_counter()
        - h1_started
    ) * 1000

    # ==============================================
    # DAILY
    # ==============================================

    daily_started = (
        time.perf_counter()
    )

    daily = fetch_candles_count(
        instrument,
        "D",
        2500
    )

    daily_fetch_ms = (
        time.perf_counter()
        - daily_started
    ) * 1000

    if not h1:

        raise ValueError(
            "No completed H1 candles"
        )

    atr = atr_series(
        h1,
        config[
            "atr_length"
        ]
    )

    daily_state = (
        build_daily_state(
            daily,
            config
        )
    )

    result = (
        evaluate_signal_at_index(
            instrument,
            h1,
            atr,
            len(h1) - 1,
            daily_state
        )
    )

    if result is None:

        raise ValueError(
            "Could not evaluate latest "
            "completed candle"
        )

    result[
        "timing"
    ] = {

        "h1_fetch_ms":
            round(
                h1_fetch_ms,
                2
            ),

        "daily_fetch_ms":
            round(
                daily_fetch_ms,
                2
            ),

        "total_evaluation_ms":
            round(
                (
                    time.perf_counter()
                    - started
                )
                * 1000,
                2
            )
    }

    return result


# ==================================================
# OPEN OANDA TRADE CHECK
# ==================================================

def has_open_trade_for(
    instrument
):

    if not OANDA_ACCOUNT_ID:

        raise RuntimeError(
            "OANDA_ACCOUNT_ID "
            "is not configured"
        )

    data = oanda_get(

        f"/v3/accounts/"
        f"{OANDA_ACCOUNT_ID}/"
        f"openTrades"
    )

    for trade in data.get(
        "trades",
        []
    ):

        if (
            trade.get(
                "instrument"
            )
            == instrument
        ):

            return True

    return False


# ==================================================
# WEBHOOK PAYLOAD
# ==================================================

def build_live_payload(
    instrument,
    signal_result
):

    if not WEBHOOK_SECRET:

        raise RuntimeError(
            "WEBHOOK_SECRET "
            "is not configured"
        )

    config = (
        STRATEGIES[
            instrument
        ]
    )

    tick = (
        config[
            "tick_size"
        ]
    )

    stop = (
        signal_result[
            "low"
        ]
        - (
            config[
                "stop_buffer_ticks"
            ]
            * tick
        )
    )

    stop = round_price(
        stop,
        config
    )

    entry = round_price(
        signal_result[
            "close"
        ],
        config
    )

    signal_close = (
        signal_result[
            "signal_close_utc"
        ]
    )

    return {

        "secret":
            WEBHOOK_SECRET,

        "pair":
            instrument,

        "side":
            "BUY",

        "entry":
            entry,

        "stop":
            stop,

        "rr":
            config[
                "reward_risk"
            ],

        "signal_id":
            signal_id_for(
                instrument,
                signal_close
            ),

        "timestamp":
            signal_close
            .astimezone(
                timezone.utc
            )
            .strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
    }


# ==================================================
# SHORT WEBHOOK PAYLOAD
# ==================================================

def build_short_live_payload(
    instrument,
    signal_result
):

    if not WEBHOOK_SECRET:

        raise RuntimeError(
            "WEBHOOK_SECRET "
            "is not configured"
        )

    config = (
        SHORT_STRATEGIES[
            instrument
        ]
    )

    tick = config[
        "tick_size"
    ]

    stop = (
        signal_result[
            "high"
        ]
        + (
            config[
                "stop_buffer_ticks"
            ]
            * tick
        )
    )

    stop = round_price(
        stop,
        config
    )

    entry = round_price(
        signal_result[
            "close"
        ],
        config
    )

    signal_close = (
        signal_result[
            "signal_close_utc"
        ]
    )

    return {

        "secret":
            WEBHOOK_SECRET,

        "pair":
            instrument,

        "side":
            "SELL",

        "entry":
            entry,

        "stop":
            stop,

        "rr":
            config[
                "reward_risk"
            ],

        "signal_id":
            short_signal_id_for(
                instrument,
                signal_close
            ),

        "timestamp":
            signal_close
            .astimezone(
                timezone.utc
            )
            .strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
    }


# ==================================================
# EXECUTOR POST
# ==================================================

def post_to_executor(
    payload
):

    if not EXECUTOR_WEBHOOK_URL:

        raise RuntimeError(
            "EXECUTOR_WEBHOOK_URL "
            "is not configured"
        )

    last_error = None

    # Three quick attempts in case of a temporary
    # network/Railway hiccup.
    for attempt in range(
        1,
        4
    ):

        try:

            started = (
                time.perf_counter()
            )

            response = requests.post(
                EXECUTOR_WEBHOOK_URL,
                json=payload,
                timeout=15
            )

            latency_ms = (
                time.perf_counter()
                - started
            ) * 1000

            response_text = (
                response.text[
                    :2000
                ]
            )

            if response.ok:

                try:

                    body = (
                        response.json()
                    )

                except Exception:

                    body = (
                        response_text
                    )

                return {

                    "ok":
                        True,

                    "status_code":
                        response.status_code,

                    "latency_ms":
                        round(
                            latency_ms,
                            2
                        ),

                    "response":
                        body,

                    "attempt":
                        attempt
                }

            last_error = (
                f"HTTP "
                f"{response.status_code}: "
                f"{response_text}"
            )

        except Exception as error:

            last_error = str(
                error
            )

        if attempt < 3:

            time.sleep(
                0.6
                * attempt
            )

    return {

        "ok":
            False,

        "error":
            last_error
    }


# ==================================================
# LIVE SIGNAL PROCESSING
# ==================================================

def process_live_instrument(
    instrument,
    expected_close_utc=None,
    force=False,
    allow_submission=True
):

    started = (
        time.perf_counter()
    )

    result = evaluate_latest(
        instrument
    )

    signal_close = (
        result[
            "signal_close_utc"
        ]
        .astimezone(
            timezone.utc
        )
    )

    signal_start = (
        result[
            "signal_start_utc"
        ]
        .astimezone(
            timezone.utc
        )
    )

    # When called by the hourly watcher,
    # make sure OANDA has actually exposed
    # the candle we are waiting for.
    if (
        expected_close_utc
        is not None
        and
        signal_close
        < expected_close_utc
    ):

        return {

            "instrument":
                instrument,

            "ready":
                False,

            "latest_completed_close_utc":
                iso_utc(
                    signal_close
                )
        }

    candle_key = (
        iso_utc(
            signal_close
        )
    )

    # ==============================================
    # DUPLICATE CANDLE GUARD
    # ==============================================

    with LIVE_STATE_LOCK:

        already_processed = (
            LAST_PROCESSED_CANDLE.get(
                instrument
            )
            == candle_key
        )

        if (
            already_processed
            and
            not force
        ):

            return {

                "instrument":
                    instrument,

                "ready":
                    True,

                "already_processed":
                    True,

                "signal_close_utc":
                    candle_key
            }

        LAST_PROCESSED_CANDLE[
            instrument
        ] = candle_key

    now = (
        utc_now()
    )

    close_age_seconds = (
        now
        - signal_close
    ).total_seconds()

    base = {

        "signal_start_utc":
            iso_utc(
                signal_start
            ),

        "signal_close_utc":
            candle_key,

        "signal":
            bool(
                result[
                    "qualified"
                ]
            ),

        "close_age_seconds":
            round(
                close_age_seconds,
                3
            ),

        "evaluation_ms":
            result[
                "timing"
            ][
                "total_evaluation_ms"
            ]
    }

    # ==============================================
    # NO SIGNAL
    # ==============================================

    if not result[
        "qualified"
    ]:

        event = add_live_event(
            "NO_SIGNAL",
            instrument,
            **base
        )

        return {

            "instrument":
                instrument,

            "ready":
                True,

            **event
        }

    # ==============================================
    # SIGNAL VALUES
    # ==============================================

    config = (
        STRATEGIES[
            instrument
        ]
    )

    stop = round_price(

        result["low"]
        - (
            config[
                "stop_buffer_ticks"
            ]
            * config[
                "tick_size"
            ]
        ),

        config
    )

    base.update({

        "entry":
            round_price(
                result[
                    "close"
                ],
                config
            ),

        "stop":
            stop,

        "rr":
            config[
                "reward_risk"
            ],

        "signal_id":
            signal_id_for(
                instrument,
                signal_close
            )
    })

    # ==============================================
    # STALE SIGNAL GUARD
    # ==============================================

    if (
        close_age_seconds < -2
        or
        close_age_seconds
        > MAX_LIVE_SIGNAL_AGE_SECONDS
    ):

        event = add_live_event(
            "SIGNAL_STALE_NOT_SUBMITTED",
            instrument,
            **base
        )

        return {

            "instrument":
                instrument,

            "ready":
                True,

            **event
        }

    # ==============================================
    # LIVE PYRAMIDING=0 CHECK
    #
    # Do not send a new signal if OANDA already
    # has an open trade on this pair.
    # ==============================================

    if has_open_trade_for(
        instrument
    ):

        event = add_live_event(
            "SIGNAL_SKIPPED_OPEN_POSITION",
            instrument,
            **base
        )

        return {

            "instrument":
                instrument,

            "ready":
                True,

            **event
        }

    # ==============================================
    # DRY RUN
    # ==============================================

    if (
        not LIVE_SUBMISSION_ENABLED
        or
        not allow_submission
    ):

        event = add_live_event(
            "DRY_RUN_SIGNAL",
            instrument,
            **base
        )

        return {

            "instrument":
                instrument,

            "ready":
                True,

            **event
        }

    # ==============================================
    # LIVE SUBMISSION
    # ==============================================

    payload = build_live_payload(
        instrument,
        result
    )

    executor_result = (
        post_to_executor(
            payload
        )
    )

    total_pipeline_ms = (
        time.perf_counter()
        - started
    ) * 1000

    if executor_result.get(
        "ok"
    ):

        event = add_live_event(

            "SIGNAL_SUBMITTED",
            instrument,

            **base,

            executor_status_code=
                executor_result[
                    "status_code"
                ],

            executor_latency_ms=
                executor_result[
                    "latency_ms"
                ],

            executor_attempt=
                executor_result[
                    "attempt"
                ],

            total_pipeline_ms=
                round(
                    total_pipeline_ms,
                    2
                )
        )

    else:

        event = add_live_event(

            "SUBMISSION_ERROR",
            instrument,

            **base,

            error=
                executor_result.get(
                    "error"
                ),

            total_pipeline_ms=
                round(
                    total_pipeline_ms,
                    2
                )
        )

    return {

        "instrument":
            instrument,

        "ready":
            True,

        **event
    }


# ==================================================
# LIVE SHORT SIGNAL PROCESSING
# ==================================================

def short_live_enabled_for(instrument):

    if not SHORT_LIVE_SUBMISSION_ENABLED:
        return False

    if instrument == "GBP_USD":
        return GBPUSD_SHORT_LIVE_ENABLED

    if instrument == "USD_JPY":
        return USDJPY_SHORT_LIVE_ENABLED

    if instrument == "USD_CAD":
        return USDCAD_SHORT_LIVE_ENABLED

    if instrument == "EUR_GBP":
        return EURGBP_SHORT_LIVE_ENABLED

    return True


def process_live_short(
    instrument,
    expected_close_utc=None,
    force=False,
    allow_submission=True
):

    if instrument not in SHORT_STRATEGIES:

        raise ValueError(
            f"{instrument} has no short strategy"
        )

    started = (
        time.perf_counter()
    )

    result = evaluate_latest_short(
        instrument
    )

    signal_close = (
        result[
            "signal_close_utc"
        ]
        .astimezone(
            timezone.utc
        )
    )

    signal_start = (
        result[
            "signal_start_utc"
        ]
        .astimezone(
            timezone.utc
        )
    )

    if (
        expected_close_utc
        is not None
        and
        signal_close
        < expected_close_utc
    ):

        return {

            "instrument":
                instrument,

            "strategy":
                f"{SHORT_STRATEGIES[instrument]['strategy_name']}_SHORT",

            "ready":
                False,

            "latest_completed_close_utc":
                iso_utc(
                    signal_close
                )
        }

    candle_key = iso_utc(
        signal_close
    )

    state_key = (
        f"{instrument}_SHORT"
    )

    with LIVE_STATE_LOCK:

        already_processed = (
            LAST_PROCESSED_CANDLE.get(
                state_key
            )
            == candle_key
        )

        if (
            already_processed
            and
            not force
        ):

            return {

                "instrument":
                    instrument,

                "strategy":
                    f"{SHORT_STRATEGIES[instrument]['strategy_name']}_SHORT",

                "ready":
                    True,

                "already_processed":
                    True,

                "signal_close_utc":
                    candle_key
            }

        LAST_PROCESSED_CANDLE[
            state_key
        ] = candle_key

    now = utc_now()

    close_age_seconds = (
        now
        - signal_close
    ).total_seconds()

    base = {

        "strategy":
            f"{SHORT_STRATEGIES[instrument]['strategy_name']}_SHORT",

        "side":
            "SELL",

        "signal_start_utc":
            iso_utc(
                signal_start
            ),

        "signal_close_utc":
            candle_key,

        "signal":
            bool(
                result[
                    "qualified"
                ]
            ),

        "close_age_seconds":
            round(
                close_age_seconds,
                3
            ),

        "evaluation_ms":
            result[
                "timing"
            ][
                "total_evaluation_ms"
            ]
    }

    event_instrument = (
        f"{instrument}_SHORT"
    )

    if not result[
        "qualified"
    ]:

        event = add_live_event(
            "NO_SIGNAL",
            event_instrument,
            **base
        )

        return {

            "instrument":
                instrument,

            "strategy":
                f"{SHORT_STRATEGIES[instrument]['strategy_name']}_SHORT",

            "ready":
                True,

            **event
        }

    config = (
        SHORT_STRATEGIES[
            instrument
        ]
    )

    stop = round_price(

        result[
            "high"
        ]
        + (
            config[
                "stop_buffer_ticks"
            ]
            * config[
                "tick_size"
            ]
        ),

        config
    )

    base.update({

        "entry":
            round_price(
                result[
                    "close"
                ],
                config
            ),

        "stop":
            stop,

        "rr":
            config[
                "reward_risk"
            ],

        "signal_id":
            short_signal_id_for(
                instrument,
                signal_close
            )
    })

    if (
        close_age_seconds < -2
        or
        close_age_seconds
        > MAX_LIVE_SIGNAL_AGE_SECONDS
    ):

        event = add_live_event(
            "SIGNAL_STALE_NOT_SUBMITTED",
            event_instrument,
            **base
        )

        return {

            "instrument":
                instrument,

            "strategy":
                f"{SHORT_STRATEGIES[instrument]['strategy_name']}_SHORT",

            "ready":
                True,

            **event
        }

    # Same-instrument pyramiding guard applies across
    # both the EUR/USD long and EUR/USD short systems.
    if has_open_trade_for(
        instrument
    ):

        event = add_live_event(
            "SIGNAL_SKIPPED_OPEN_POSITION",
            event_instrument,
            **base
        )

        return {

            "instrument":
                instrument,

            "strategy":
                f"{SHORT_STRATEGIES[instrument]['strategy_name']}_SHORT",

            "ready":
                True,

            **event
        }

    if (
        not short_live_enabled_for(
            instrument
        )
        or
        not allow_submission
    ):

        event = add_live_event(
            "DRY_RUN_SIGNAL",
            event_instrument,
            **base
        )

        return {

            "instrument":
                instrument,

            "strategy":
                f"{SHORT_STRATEGIES[instrument]['strategy_name']}_SHORT",

            "ready":
                True,

            **event
        }

    payload = build_short_live_payload(
        instrument,
        result
    )

    executor_result = (
        post_to_executor(
            payload
        )
    )

    total_pipeline_ms = (
        time.perf_counter()
        - started
    ) * 1000

    if executor_result.get(
        "ok"
    ):

        event = add_live_event(

            "SIGNAL_SUBMITTED",
            event_instrument,

            **base,

            executor_status_code=
                executor_result[
                    "status_code"
                ],

            executor_latency_ms=
                executor_result[
                    "latency_ms"
                ],

            executor_attempt=
                executor_result[
                    "attempt"
                ],

            total_pipeline_ms=
                round(
                    total_pipeline_ms,
                    2
                )
        )

    else:

        event = add_live_event(

            "SUBMISSION_ERROR",
            event_instrument,

            **base,

            error=
                executor_result.get(
                    "error"
                ),

            total_pipeline_ms=
                round(
                    total_pipeline_ms,
                    2
                )
        )

    return {

        "instrument":
            instrument,

        "strategy":
            f"{SHORT_STRATEGIES[instrument]['strategy_name']}_SHORT",

        "ready":
            True,

        **event
    }


# ==================================================
# LIVE WATCHER
# ==================================================

def next_hour_boundary(
    now=None
):

    if now is None:

        now = (
            utc_now()
        )

    return (
        now.replace(
            minute=0,
            second=0,
            microsecond=0
        )
        + timedelta(
            hours=1
        )
    )


def wait_for_new_h1_and_process(
    expected_close_utc
):

    pending = set(
        STRATEGIES.keys()
    )

    deadline = (
        time.monotonic()
        + LIVE_POLL_WINDOW_SECONDS
    )

    while (
        pending
        and
        time.monotonic()
        < deadline
    ):

        ready = []

        # ==========================================
        # LIGHTWEIGHT CANDLE AVAILABILITY CHECK
        # ==========================================

        with ThreadPoolExecutor(
            max_workers=len(
                pending
            )
        ) as pool:

            futures = {

                pool.submit(
                    fetch_latest_complete_h1,
                    instrument
                ):
                    instrument

                for instrument
                in pending
            }

            for future in as_completed(
                futures
            ):

                instrument = (
                    futures[
                        future
                    ]
                )

                try:

                    candle = (
                        future.result()
                    )

                    if candle is None:

                        continue

                    candle_close = (
                        candle["time"]
                        + timedelta(
                            hours=1
                        )
                    )

                    if (
                        candle_close
                        >= expected_close_utc
                    ):

                        ready.append(
                            instrument
                        )

                except Exception as error:

                    add_live_event(
                        "CANDLE_READY_CHECK_ERROR",
                        instrument,
                        error=str(
                            error
                        )
                    )

        # ==========================================
        # FULL STRATEGY EVALUATION
        # ==========================================

        if ready:

            with ThreadPoolExecutor(
                max_workers=len(
                    ready
                )
            ) as pool:

                futures = {

                    pool.submit(
                        process_live_instrument,
                        instrument,
                        expected_close_utc,
                        False,
                        True
                    ):
                        instrument

                    for instrument
                    in ready
                }

                for future in as_completed(
                    futures
                ):

                    instrument = (
                        futures[
                            future
                        ]
                    )

                    try:

                        future.result()

                    except Exception as error:

                        add_live_event(
                            "LIVE_PROCESS_ERROR",
                            instrument,
                            error=str(
                                error
                            )
                        )

            # Run any short strategies whose underlying
            # instrument candle is also ready.
            for short_instrument in SHORT_STRATEGIES:

                if short_instrument not in ready:

                    continue

                try:

                    process_live_short(
                        short_instrument,
                        expected_close_utc,
                        False,
                        True
                    )

                except Exception as error:

                    add_live_event(
                        "LIVE_PROCESS_ERROR",
                        f"{short_instrument}_SHORT",
                        error=str(
                            error
                        )
                    )

            pending.difference_update(
                ready
            )

        if pending:

            time.sleep(
                LIVE_POLL_INTERVAL_SECONDS
            )

    # Weekend / unavailable candle / API lag.
    for instrument in sorted(
        pending
    ):

        add_live_event(
            "NEW_H1_NOT_AVAILABLE_WITHIN_WINDOW",
            instrument,
            expected_close_utc=
                iso_utc(
                    expected_close_utc
                )
        )


def live_watcher_loop():

    add_live_event(

        "WATCHER_STARTED",

        submission_enabled=
            LIVE_SUBMISSION_ENABLED
    )

    # ==============================================
    # STARTUP CHECK
    #
    # Evaluates latest completed candle immediately.
    #
    # Stale protection means deploying/restarting
    # mid-hour cannot accidentally enter an old trade.
    # ==============================================

    with ThreadPoolExecutor(
        max_workers=len(
            STRATEGIES
        )
    ) as pool:

        futures = {

            pool.submit(
                process_live_instrument,
                instrument,
                None,
                False,
                True
            ):
                instrument

            for instrument
            in STRATEGIES
        }

        for future in as_completed(
            futures
        ):

            instrument = (
                futures[
                    future
                ]
            )

            try:

                future.result()

            except Exception as error:

                add_live_event(
                    "STARTUP_CHECK_ERROR",
                    instrument,
                    error=str(
                        error
                    )
                )

    # Evaluate the EUR/USD short strategy separately.
    # It uses the same completed H1 candle but maintains
    # its own duplicate-candle state.
    for short_instrument in SHORT_STRATEGIES:

        try:

            process_live_short(
                short_instrument,
                None,
                False,
                True
            )

        except Exception as error:

            add_live_event(
                "STARTUP_CHECK_ERROR",
                f"{short_instrument}_SHORT",
                error=str(
                    error
                )
            )

    # ==============================================
    # HOURLY LOOP
    # ==============================================

    while True:

        now = (
            utc_now()
        )

        expected_close = (
            next_hour_boundary(
                now
            )
        )

        wake_time = (
            expected_close
            + timedelta(
                seconds=
                    LIVE_POLL_OFFSET_SECONDS
            )
        )

        sleep_seconds = max(

            0.0,

            (
                wake_time
                - utc_now()
            ).total_seconds()
        )

        time.sleep(
            sleep_seconds
        )

        wait_for_new_h1_and_process(
            expected_close
        )


def start_live_watcher_once():

    global WATCHER_STARTED

    if not LIVE_WATCHER_ENABLED:

        return

    if WATCHER_STARTED:

        return

    WATCHER_STARTED = True

    thread = threading.Thread(

        target=
            live_watcher_loop,

        name=
            "erf-live-watcher",

        daemon=
            True
    )

    thread.start()


# ==================================================
# HISTORICAL TRADE SIMULATION
# ==================================================

def create_trade(
    instrument,
    signal_result
):

    config = (
        STRATEGIES[
            instrument
        ]
    )

    tick = (
        config[
            "tick_size"
        ]
    )

    reference_entry = (
        signal_result[
            "close"
        ]
    )

    backtest_entry = (
        reference_entry
        + (
            BACKTEST_SLIPPAGE_TICKS
            * tick
        )
    )

    stop = (
        signal_result[
            "low"
        ]
        - (
            config[
                "stop_buffer_ticks"
            ]
            * tick
        )
    )

    trade_risk = (
        reference_entry
        - stop
    )

    target = (
        reference_entry
        + (
            trade_risk
            * config[
                "reward_risk"
            ]
        )
    )

    return {

        "instrument":
            instrument,

        "signal_start_utc":
            signal_result[
                "signal_start_utc"
            ],

        "entry_time_utc":
            signal_result[
                "signal_close_utc"
            ],

        "reference_entry":
            round_price(
                reference_entry,
                config
            ),

        "backtest_entry":
            round_price(
                backtest_entry,
                config
            ),

        "stop":
            round_price(
                stop,
                config
            ),

        "target":
            round_price(
                target,
                config
            ),

        "exit_bar_start_utc":
            None,

        "exit_time_utc":
            None,

        "exit_reason":
            None
    }


def determine_exit_on_bar(
    trade,
    candle
):

    stop = (
        trade[
            "stop"
        ]
    )

    target = (
        trade[
            "target"
        ]
    )

    stop_touched = (
        candle["low"]
        <= stop
    )

    target_touched = (
        candle["high"]
        >= target
    )

    if (
        not stop_touched
        and
        not target_touched
    ):

        return None

    if (
        stop_touched
        and
        not target_touched
    ):

        return "STOP"

    if (
        target_touched
        and
        not stop_touched
    ):

        return "TARGET"

    # TradingView historical broker-emulator
    # same-bar path approximation.
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

        return "TARGET"

    return "STOP"


def simulate_trades(
    instrument,
    h1,
    atr,
    daily_state,
    start,
    end
):

    config = (
        STRATEGIES[
            instrument
        ]
    )

    raw_signals = []
    trades = []
    ignored_signals = []

    open_trade = None

    start_index = max(

        config[
            "atr_length"
        ],

        config[
            "structure_lookback"
        ]
    )

    evaluated_bars = 0

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

        if candle_time < start:

            continue

        if candle_time >= end:

            break

        evaluated_bars += 1

        # ==========================================
        # EXISTING POSITION
        # ==========================================

        if open_trade is not None:

            exit_reason = (
                determine_exit_on_bar(
                    open_trade,
                    candle
                )
            )

            if exit_reason is not None:

                open_trade[
                    "exit_reason"
                ] = exit_reason

                open_trade[
                    "exit_bar_start_utc"
                ] = candle_time

                open_trade[
                    "exit_time_utc"
                ] = (
                    candle_time
                    + timedelta(
                        hours=1
                    )
                )

                open_trade = None

        # ==========================================
        # SIGNAL
        # ==========================================

        result = (
            evaluate_signal_at_index(
                instrument,
                h1,
                atr,
                index,
                daily_state
            )
        )

        if (
            result is None
            or
            not result[
                "qualified"
            ]
        ):

            continue

        raw_signals.append(
            result
        )

        # ==========================================
        # PYRAMIDING=0
        # ==========================================

        if open_trade is not None:

            ignored_signals.append({

                "signal_start_utc":
                    result[
                        "signal_start_utc"
                    ],

                "signal_close_utc":
                    result[
                        "signal_close_utc"
                    ],

                "reason":
                    "POSITION_ALREADY_OPEN",

                "existing_trade_entry_time":
                    open_trade[
                        "entry_time_utc"
                    ]
            })

            continue

        new_trade = (
            create_trade(
                instrument,
                result
            )
        )

        trades.append(
            new_trade
        )

        open_trade = (
            new_trade
        )

    return {

        "evaluated_bars":
            evaluated_bars,

        "raw_signals":
            raw_signals,

        "trades":
            trades,

        "ignored_signals":
            ignored_signals,

        "position_still_open_at_end":
            open_trade is not None
    }


# ==================================================
# HISTORICAL TEST ENGINE
# ==================================================

def historical_test_engine(
    instrument,
    start,
    end
):

    if instrument not in STRATEGIES:

        raise ValueError(
            f"{instrument} is not supported"
        )

    config = (
        STRATEGIES[
            instrument
        ]
    )

    if end <= start:

        raise ValueError(
            "'to' must be after 'from'"
        )

    days = (
        end - start
    ).total_seconds() / 86400

    if days > MAX_HISTORY_DAYS:

        raise ValueError(
            f"Maximum historical "
            f"window is "
            f"{MAX_HISTORY_DAYS} days"
        )

    started = (
        time.perf_counter()
    )

    # ==============================================
    # H1 HISTORY
    # ==============================================

    h1_started = (
        time.perf_counter()
    )

    h1 = fetch_h1_history(

        instrument,

        start
        - timedelta(
            days=60
        ),

        end
    )

    h1_fetch_ms = (
        time.perf_counter()
        - h1_started
    ) * 1000

    # ==============================================
    # DAILY HISTORY
    # ==============================================

    daily_started = (
        time.perf_counter()
    )

    daily = fetch_daily_history(
        instrument,
        start,
        end
    )

    daily_fetch_ms = (
        time.perf_counter()
        - daily_started
    ) * 1000

    atr = atr_series(
        h1,
        config[
            "atr_length"
        ]
    )

    daily_state = (
        build_daily_state(
            daily,
            config
        )
    )

    simulation = (
        simulate_trades(
            instrument,
            h1,
            atr,
            daily_state,
            start,
            end
        )
    )

    # ==============================================
    # RAW SIGNAL OUTPUT
    # ==============================================

    raw_signal_output = []

    for signal in simulation[
        "raw_signals"
    ]:

        raw_signal_output.append({

            "signal_start_utc":
                iso_utc(
                    signal[
                        "signal_start_utc"
                    ]
                ),

            "signal_close_utc":
                iso_utc(
                    signal[
                        "signal_close_utc"
                    ]
                ),

            "open":
                signal[
                    "open"
                ],

            "high":
                signal[
                    "high"
                ],

            "low":
                signal[
                    "low"
                ],

            "close":
                signal[
                    "close"
                ],

            "atr14":
                round(
                    signal[
                        "atr"
                    ],
                    8
                ),

            "body_ratio":
                (
                    round(
                        signal[
                            "body_ratio"
                        ],
                        6
                    )

                    if signal[
                        "body_ratio"
                    ] is not None

                    else None
                ),

            "close_location":
                round(
                    signal[
                        "close_location"
                    ],
                    6
                ),

            "lower_wick":
                round(
                    signal[
                        "lower_wick"
                    ],
                    8
                ),

            "lower_wick_body_ratio":
                (
                    round(
                        signal[
                            "lower_wick_body_ratio"
                        ],
                        6
                    )

                    if signal[
                        "lower_wick_body_ratio"
                    ] is not None

                    else None
                ),

            "local_timezone":
                signal[
                    "local_timezone"
                ],

            "local_hour":
                signal[
                    "local_hour"
                ],

            "weekday":
                signal[
                    "weekday"
                ]
        })

    # ==============================================
    # TRADE OUTPUT
    # ==============================================

    trade_output = []

    winners = 0
    losers = 0
    still_open = 0

    for number, trade in enumerate(
        simulation[
            "trades"
        ],
        start=1
    ):

        if (
            trade[
                "exit_reason"
            ]
            == "TARGET"
        ):

            winners += 1

        elif (
            trade[
                "exit_reason"
            ]
            == "STOP"
        ):

            losers += 1

        else:

            still_open += 1

        trade_output.append({

            "trade_number":
                number,

            "signal_start_utc":
                iso_utc(
                    trade[
                        "signal_start_utc"
                    ]
                ),

            "entry_time_utc":
                iso_utc(
                    trade[
                        "entry_time_utc"
                    ]
                ),

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

            "exit_bar_start_utc":
                (
                    iso_utc(
                        trade[
                            "exit_bar_start_utc"
                        ]
                    )

                    if trade[
                        "exit_bar_start_utc"
                    ] is not None

                    else None
                ),

            "exit_time_utc":
                (
                    iso_utc(
                        trade[
                            "exit_time_utc"
                        ]
                    )

                    if trade[
                        "exit_time_utc"
                    ] is not None

                    else None
                ),

            "exit_reason":
                trade[
                    "exit_reason"
                ]
        })

    # ==============================================
    # IGNORED OUTPUT
    # ==============================================

    ignored_output = []

    for ignored in simulation[
        "ignored_signals"
    ]:

        ignored_output.append({

            "signal_start_utc":
                iso_utc(
                    ignored[
                        "signal_start_utc"
                    ]
                ),

            "signal_close_utc":
                iso_utc(
                    ignored[
                        "signal_close_utc"
                    ]
                ),

            "reason":
                ignored[
                    "reason"
                ],

            "existing_trade_entry_time":
                iso_utc(
                    ignored[
                        "existing_trade_entry_time"
                    ]
                )
        })

    total_ms = (
        time.perf_counter()
        - started
    ) * 1000

    return {

        "status":
            "success",

        "mode":
            "READ_ONLY",

        "instrument":
            instrument,

        "from":
            iso_utc(
                start
            ),

        "to":
            iso_utc(
                end
            ),

        "simulation_settings": {

            "pyramiding":
                0,

            "backtest_slippage_ticks":
                BACKTEST_SLIPPAGE_TICKS,

            "stop_buffer_ticks":
                config[
                    "stop_buffer_ticks"
                ],

            "reward_risk":
                config[
                    "reward_risk"
                ],

            "tick_size":
                config[
                    "tick_size"
                ]
        },

        "h1_candles_loaded":
            len(
                h1
            ),

        "daily_candles_loaded":
            len(
                daily
            ),

        "evaluated_h1_bars":
            simulation[
                "evaluated_bars"
            ],

        "raw_signal_count":
            len(
                simulation[
                    "raw_signals"
                ]
            ),

        "trade_count":
            len(
                simulation[
                    "trades"
                ]
            ),

        "ignored_while_position_open":
            len(
                simulation[
                    "ignored_signals"
                ]
            ),

        "winners":
            winners,

        "losers":
            losers,

        "still_open":
            still_open,

        "position_still_open_at_end":
            simulation[
                "position_still_open_at_end"
            ],

        "trades":
            trade_output,

        "ignored_signals":
            ignored_output,

        "raw_signals":
            raw_signal_output,

        "timing": {

            "h1_fetch_ms":
                round(
                    h1_fetch_ms,
                    2
                ),

            "daily_fetch_ms":
                round(
                    daily_fetch_ms,
                    2
                ),

            "total_ms":
                round(
                    total_ms,
                    2
                )
        }
    }


# ==================================================
# ROUTES
# ==================================================

@app.route("/")
def home():

    return jsonify({

        "status":
            "online",

        "service":
            "ERF strategy engine",

        "supported":
            sorted(
                STRATEGIES.keys()
            ),

        "historical_trade_simulation":
            True,

        "live_watcher_enabled":
            LIVE_WATCHER_ENABLED,

        "live_submission_enabled":
            LIVE_SUBMISSION_ENABLED,

        "short_live_submission_enabled":
            SHORT_LIVE_SUBMISSION_ENABLED,

        "gbpusd_short_live_enabled":
            GBPUSD_SHORT_LIVE_ENABLED,

        "usdjpy_short_live_enabled":
            USDJPY_SHORT_LIVE_ENABLED,

        "usdcad_short_live_enabled":
            USDCAD_SHORT_LIVE_ENABLED,

        "eurgbp_short_live_enabled":
            EURGBP_SHORT_LIVE_ENABLED,

        "short_strategies":
            sorted(
                SHORT_STRATEGIES.keys()
            ),

        "oanda_token_configured":
            bool(
                OANDA_TOKEN
            ),

        "oanda_account_id_configured":
            bool(
                OANDA_ACCOUNT_ID
            ),

        "executor_url_configured":
            bool(
                EXECUTOR_WEBHOOK_URL
            ),

        "webhook_secret_configured":
            bool(
                WEBHOOK_SECRET
            ),

        "orders_sent_directly_by_this_service":
            False,

        "live_pipeline_test_supported":
            True,

        "execution_path":
            (
                "OANDA candles -> "
                "Python strategy engine -> "
                "existing executor -> OANDA"
            )
    })


@app.route(
    "/strategy-test/<instrument>"
)
def strategy_test(
    instrument
):

    instrument = (
        instrument.upper()
    )

    try:

        result = (
            evaluate_latest(
                instrument
            )
        )

        return jsonify({

            "status":
                "success",

            "mode":
                "READ_ONLY",

            "instrument":
                instrument,

            "signal":
                result[
                    "qualified"
                ],

            "signal_start_utc":
                iso_utc(
                    result[
                        "signal_start_utc"
                    ]
                ),

            "signal_close_utc":
                iso_utc(
                    result[
                        "signal_close_utc"
                    ]
                ),

            "open":
                result[
                    "open"
                ],

            "high":
                result[
                    "high"
                ],

            "low":
                result[
                    "low"
                ],

            "close":
                result[
                    "close"
                ],

            "atr14":
                round(
                    result[
                        "atr"
                    ],
                    8
                ),

            "body_ratio":
                (
                    round(
                        result[
                            "body_ratio"
                        ],
                        6
                    )

                    if result[
                        "body_ratio"
                    ] is not None

                    else None
                ),

            "close_location":
                round(
                    result[
                        "close_location"
                    ],
                    6
                ),

            "local_timezone":
                result[
                    "local_timezone"
                ],

            "local_hour":
                result[
                    "local_hour"
                ],

            "local_weekday":
                result[
                    "weekday"
                ],

            "timing":
                result[
                    "timing"
                ]
        })

    except Exception as error:

        return jsonify({

            "status":
                "error",

            "mode":
                "READ_ONLY",

            "instrument":
                instrument,

            "message":
                str(
                    error
                )
        }), 500


@app.route(
    "/live-pipeline-test/<instrument>/SELL"
)
def live_pipeline_test_short(
    instrument
):
    instrument = instrument.upper()

    if instrument not in SHORT_STRATEGIES:
        return jsonify({
            "status": "error",
            "read_only": True,
            "message":
                f"{instrument} has no short strategy"
        }), 400

    if not EXECUTOR_WEBHOOK_URL:
        return jsonify({
            "status": "error",
            "read_only": True,
            "message":
                "EXECUTOR_WEBHOOK_URL is not configured"
        }), 500

    try:
        # Ask the actual executor for its current live price.
        executor_root = (
            EXECUTOR_WEBHOOK_URL
            .rsplit("/webhook", 1)[0]
        )

        price_url = (
            f"{executor_root}/price-test/{instrument}"
        )

        price_response = requests.get(
            price_url,
            timeout=15
        )

        if not price_response.ok:
            raise RuntimeError(
                f"Executor price-test returned "
                f"HTTP {price_response.status_code}: "
                f"{price_response.text[:1000]}"
            )

        price_data = price_response.json()

        live_bid = float(
            price_data["live_bid"]
        )

        config = SHORT_STRATEGIES[
            instrument
        ]

        tick = config[
            "tick_size"
        ]

        # Synthetic stop distance used only for validation.
        # 200 ticks gives sensible sizing on both JPY and
        # non-JPY pairs and is never submitted as an order.
        synthetic_stop_distance_ticks = 200

        # build_short_live_payload() adds the strategy's own
        # 10-tick stop buffer to signal_result["high"].
        synthetic_high = (
            live_bid
            + (
                synthetic_stop_distance_ticks
                - config[
                    "stop_buffer_ticks"
                ]
            )
            * tick
        )

        now_utc = datetime.now(
            timezone.utc
        )

        synthetic_result = {
            "close":
                live_bid,
            "high":
                synthetic_high,
            "signal_close_utc":
                now_utc
        }

        # IMPORTANT: this is the exact same payload builder used
        # by genuine qualifying live short signals.
        payload = build_short_live_payload(
            instrument,
            synthetic_result
        )

        # Make the synthetic signal ID unique and obvious.
        payload["signal_id"] = (
            f"PIPELINE-TEST-"
            f"{instrument}-"
            f"{int(now_utc.timestamp())}"
        )

        payload["timestamp"] = (
            now_utc
            .strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        )

        # Executor recognizes this only after all normal webhook
        # validation and execution calculations have succeeded.
        payload["pipeline_test"] = True

        executor_result = post_to_executor(
            payload
        )

        if not executor_result.get(
            "ok"
        ):
            return jsonify({
                "status":
                    "pipeline_test_failed",
                "read_only":
                    True,
                "instrument":
                    instrument,
                "side":
                    "SELL",
                "executor_result":
                    executor_result
            }), 502

        return jsonify({
            "status":
                "pipeline_test_passed",
            "read_only":
                True,
            "instrument":
                instrument,
            "side":
                "SELL",
            "used_real_short_payload_builder":
                True,
            "used_real_executor_webhook":
                True,
            "order_submitted":
                False,
            "executor_price_test":
                {
                    "live_bid":
                        price_data.get(
                            "live_bid"
                        ),
                    "live_ask":
                        price_data.get(
                            "live_ask"
                        ),
                    "locked_rr":
                        price_data.get(
                            "locked_rr"
                        )
                },
            "executor_result":
                executor_result
        })

    except Exception as error:
        return jsonify({
            "status":
                "pipeline_test_failed",
            "read_only":
                True,
            "instrument":
                instrument,
            "side":
                "SELL",
            "message":
                str(error)
        }), 500


@app.route(
    "/short-strategy-test/<instrument>"
)
def short_strategy_test(
    instrument
):

    instrument = (
        instrument.upper()
    )

    try:

        result = (
            evaluate_latest_short(
                instrument
            )
        )

        return jsonify({

            "status":
                "success",

            "mode":
                "READ_ONLY",

            "instrument":
                instrument,

            "strategy":
                f"{SHORT_STRATEGIES[instrument]['strategy_name']}_SHORT",

            "side":
                "SELL",

            "signal":
                result[
                    "qualified"
                ],

            "signal_start_utc":
                iso_utc(
                    result[
                        "signal_start_utc"
                    ]
                ),

            "signal_close_utc":
                iso_utc(
                    result[
                        "signal_close_utc"
                    ]
                ),

            "open":
                result[
                    "open"
                ],

            "high":
                result[
                    "high"
                ],

            "low":
                result[
                    "low"
                ],

            "close":
                result[
                    "close"
                ],

            "atr14":
                round(
                    result[
                        "atr"
                    ],
                    8
                ),

            "body_ratio":
                (
                    round(
                        result[
                            "body_ratio"
                        ],
                        6
                    )
                    if result[
                        "body_ratio"
                    ] is not None
                    else None
                ),

            "close_location":
                round(
                    result[
                        "close_location"
                    ],
                    6
                ),

            "distance_from_recent_high_atr":
                (
                    round(
                        result[
                            "distance_from_recent_high_atr"
                        ],
                        6
                    )
                    if result[
                        "distance_from_recent_high_atr"
                    ] is not None
                    else None
                ),

            "previous_daily_close":
                result[
                    "previous_daily_close"
                ],

            "previous_daily_fast_ema":
                result[
                    "previous_daily_fast_ema"
                ],

            "previous_daily_slow_ema":
                result[
                    "previous_daily_slow_ema"
                ],

            "daily_ema_separation_atr":
                (
                    round(
                        result[
                            "daily_ema_separation_atr"
                        ],
                        6
                    )
                    if result[
                        "daily_ema_separation_atr"
                    ] is not None
                    else None
                ),

            "local_timezone":
                result[
                    "local_timezone"
                ],

            "local_hour":
                result[
                    "local_hour"
                ],

            "local_weekday":
                result[
                    "weekday"
                ],

            "timing":
                result[
                    "timing"
                ]
        })

    except Exception as error:

        return jsonify({

            "status":
                "error",

            "mode":
                "READ_ONLY",

            "instrument":
                instrument,

            "message":
                str(
                    error
                )
        }), 500


@app.route(
    "/historical-test/<instrument>"
)
def historical_test(
    instrument
):

    instrument = (
        instrument.upper()
    )

    try:

        start_text = (
            request.args.get(
                "from"
            )
        )

        end_text = (
            request.args.get(
                "to"
            )
        )

        if not start_text:

            raise ValueError(
                "Missing 'from' date"
            )

        if not end_text:

            raise ValueError(
                "Missing 'to' date"
            )

        start = parse_date(
            start_text
        )

        end = parse_date(
            end_text
        )

        return jsonify(
            historical_test_engine(
                instrument,
                start,
                end
            )
        )

    except Exception as error:

        return jsonify({

            "status":
                "error",

            "mode":
                "READ_ONLY",

            "instrument":
                instrument,

            "message":
                str(
                    error
                )
        }), 400


# ==================================================
# LIVE STATUS
# ==================================================

@app.route(
    "/live-status"
)
def live_status():

    with LIVE_STATE_LOCK:

        last_pair_status = dict(
            LAST_PAIR_STATUS
        )

        last_processed = dict(
            LAST_PROCESSED_CANDLE
        )

    return jsonify({

        "status":
            "online",

        "live_watcher_enabled":
            LIVE_WATCHER_ENABLED,

        "live_submission_enabled":
            LIVE_SUBMISSION_ENABLED,

        "short_live_submission_enabled":
            SHORT_LIVE_SUBMISSION_ENABLED,

        "gbpusd_short_live_enabled":
            GBPUSD_SHORT_LIVE_ENABLED,

        "usdjpy_short_live_enabled":
            USDJPY_SHORT_LIVE_ENABLED,

        "usdcad_short_live_enabled":
            USDCAD_SHORT_LIVE_ENABLED,

        "eurgbp_short_live_enabled":
            EURGBP_SHORT_LIVE_ENABLED,

        "max_live_signal_age_seconds":
            MAX_LIVE_SIGNAL_AGE_SECONDS,

        "pairs":
            sorted(
                STRATEGIES.keys()
            ),

        "short_pairs":
            sorted(
                SHORT_STRATEGIES.keys()
            ),

        "last_processed_candle":
            last_processed,

        "last_pair_status":
            last_pair_status
    })


# ==================================================
# RECENT LIVE EVENTS
# ==================================================

@app.route(
    "/live-events"
)
def live_events():

    with LIVE_STATE_LOCK:

        events = list(
            LIVE_EVENTS
        )

    return jsonify({

        "count":
            len(
                events
            ),

        "events":
            events
    })


# ==================================================
# MANUAL LIVE CHECK
#
# IMPORTANT:
# This endpoint NEVER sends an order, even after
# LIVE_SUBMISSION_ENABLED becomes true.
# ==================================================

@app.route(
    "/live-check"
)
def live_check():

    results = []

    with ThreadPoolExecutor(
        max_workers=len(
            STRATEGIES
        )
    ) as pool:

        futures = {

            pool.submit(
                process_live_instrument,
                instrument,
                None,
                True,
                False
            ):
                instrument

            for instrument
            in STRATEGIES
        }

        for future in as_completed(
            futures
        ):

            instrument = (
                futures[
                    future
                ]
            )

            try:

                results.append(
                    future.result()
                )

            except Exception as error:

                results.append({

                    "instrument":
                        instrument,

                    "status":
                        "error",

                    "message":
                        str(
                            error
                        )
                })

    for short_instrument in SHORT_STRATEGIES:

        try:

            results.append(
                process_live_short(
                    short_instrument,
                    None,
                    True,
                    False
                )
            )

        except Exception as error:

            results.append({

                "instrument":
                    short_instrument,

                "strategy":
                    f"{SHORT_STRATEGIES[short_instrument]['strategy_name']}_SHORT",

                "status":
                    "error",

                "message":
                    str(
                        error
                    )
            })

    results.sort(
        key=lambda item:
            item[
                "instrument"
            ]
    )

    return jsonify({

        "manual_check_is_submission_safe":
            True,

        "live_submission_enabled":
            LIVE_SUBMISSION_ENABLED,

        "results":
            results
    })



# ==================================================
# FULL LIVE-PORTFOLIO TWO-YEAR BACKTEST
# RESEARCH ONLY — NO ORDERS, NO WEBHOOK SUBMISSIONS
# ==================================================

PORTFOLIO_BACKTEST_FILE = "full_strategy_two_year_backtest.csv"
PORTFOLIO_SUMMARY_FILE = "full_strategy_two_year_summary.csv"
PORTFOLIO_BACKTEST_LOCK = threading.Lock()
PORTFOLIO_BACKTEST_CACHE = None


def portfolio_short_trade_from_signal(
    instrument,
    signal_result
):
    config = SHORT_STRATEGIES[instrument]
    tick = config["tick_size"]

    reference_entry = signal_result["close"]

    # Locked research convention:
    # adverse short fill = signal close - 5 ticks.
    backtest_entry = (
        reference_entry
        - BACKTEST_SLIPPAGE_TICKS * tick
    )

    stop = (
        signal_result["high"]
        + config["stop_buffer_ticks"] * tick
    )

    reference_risk = stop - reference_entry

    if reference_risk <= 0:
        raise RuntimeError(
            f"Invalid short reference risk for {instrument}"
        )

    target = (
        reference_entry
        - reference_risk * config["reward_risk"]
    )

    return {
        "instrument": instrument,
        "side": "SELL",
        "strategy": "SHORT",
        "signal_start_utc": signal_result["signal_start_utc"],
        "entry_time_utc": signal_result["signal_close_utc"],
        "reference_entry": round_price(reference_entry, config),
        "backtest_entry": round_price(backtest_entry, config),
        "stop": round_price(stop, config),
        "target": round_price(target, config),
        "exit_bar_start_utc": None,
        "exit_time_utc": None,
        "exit_reason": None,
    }


def portfolio_short_exit_on_bar(
    trade,
    candle
):
    stop_touched = (
        candle["high"] >= trade["stop"]
    )

    target_touched = (
        candle["low"] <= trade["target"]
    )

    if not stop_touched and not target_touched:
        return None

    if stop_touched and not target_touched:
        return "STOP"

    if target_touched and not stop_touched:
        return "TARGET"

    # Locked short same-bar rule:
    # if high is closer to candle open, stop is assumed first;
    # otherwise target is assumed first.
    distance_to_high = abs(
        candle["high"] - candle["open"]
    )

    distance_to_low = abs(
        candle["open"] - candle["low"]
    )

    if distance_to_high < distance_to_low:
        return "STOP"

    return "TARGET"


def portfolio_simulate_shorts(
    instrument,
    h1,
    atr,
    daily_state,
    start,
    end
):
    config = SHORT_STRATEGIES[instrument]

    required_lookbacks = [
        config["atr_length"],
        config["structure_lookback"],
    ]

    momentum_lookback = config.get(
        "momentum_lookback_bars"
    )

    if momentum_lookback is not None:
        required_lookbacks.append(
            momentum_lookback
        )

    momentum_requirements = config.get(
        "momentum_requirements",
        {}
    )

    for lookback_bars in momentum_requirements.keys():
        required_lookbacks.append(
            int(lookback_bars)
        )

    if config.get(
        "minimum_h1_atr_ratio_50"
    ) is not None:
        required_lookbacks.append(50)

    start_index = max(
        required_lookbacks
    )

    trades = []
    open_trade = None
    raw_signal_count = 0
    ignored_signal_count = 0

    for index in range(
        start_index,
        len(h1)
    ):
        candle = h1[index]
        candle_time = candle["time"]

        if candle_time < start:
            continue

        if candle_time >= end:
            break

        # Existing short position is evaluated first.
        # This preserves the locked convention that a new
        # signal on the exact H1 candle where the old trade
        # exits is allowed.
        if open_trade is not None:
            exit_reason = (
                portfolio_short_exit_on_bar(
                    open_trade,
                    candle
                )
            )

            if exit_reason is not None:
                open_trade["exit_reason"] = (
                    exit_reason
                )
                open_trade[
                    "exit_bar_start_utc"
                ] = candle_time
                open_trade[
                    "exit_time_utc"
                ] = (
                    candle_time
                    + timedelta(hours=1)
                )
                open_trade = None

        result = evaluate_short_signal_at_index(
            instrument,
            h1,
            atr,
            index,
            daily_state
        )

        if (
            result is None
            or not result["qualified"]
        ):
            continue

        raw_signal_count += 1

        if open_trade is not None:
            ignored_signal_count += 1
            continue

        new_trade = (
            portfolio_short_trade_from_signal(
                instrument,
                result
            )
        )

        trades.append(
            new_trade
        )

        open_trade = (
            new_trade
        )

    return {
        "trades": trades,
        "raw_signal_count": raw_signal_count,
        "ignored_signal_count": ignored_signal_count,
        "position_still_open_at_end": (
            open_trade is not None
        ),
    }


def portfolio_long_trade_r(
    trade
):
    if trade["exit_reason"] not in {
        "TARGET",
        "STOP",
    }:
        return None

    entry = float(
        trade["backtest_entry"]
    )
    stop = float(
        trade["stop"]
    )
    target = float(
        trade["target"]
    )

    actual_risk = entry - stop

    if actual_risk <= 0:
        return None

    if trade["exit_reason"] == "STOP":
        exit_price = stop
    else:
        exit_price = target

    return (
        exit_price - entry
    ) / actual_risk


def portfolio_short_trade_r(
    trade
):
    if trade["exit_reason"] not in {
        "TARGET",
        "STOP",
    }:
        return None

    entry = float(
        trade["backtest_entry"]
    )
    stop = float(
        trade["stop"]
    )
    target = float(
        trade["target"]
    )

    actual_risk = stop - entry

    if actual_risk <= 0:
        return None

    if trade["exit_reason"] == "STOP":
        exit_price = stop
    else:
        exit_price = target

    return (
        entry - exit_price
    ) / actual_risk


def portfolio_iso(
    value
):
    if value is None:
        return None

    if isinstance(
        value,
        str
    ):
        return value

    return iso_utc(
        value
    )


def portfolio_collect_strategy_trades(
    start,
    end
):
    combined = []
    strategy_summaries = []

    instruments = sorted(
        STRATEGIES.keys()
    )

    for instrument in instruments:
        print(
            f"Portfolio backtest: loading {instrument}",
            flush=True,
        )

        # One common H1 history is sufficient for both
        # the long and short strategy for this pair.
        h1 = fetch_h1_history(
            instrument,
            start - timedelta(days=120),
            end
        )

        daily = fetch_daily_history(
            instrument,
            start,
            end
        )

        # --------------------------
        # LONG
        # --------------------------
        long_config = STRATEGIES[
            instrument
        ]

        long_atr = atr_series(
            h1,
            long_config["atr_length"]
        )

        long_daily_state = (
            build_daily_state(
                daily,
                long_config
            )
        )

        long_sim = simulate_trades(
            instrument,
            h1,
            long_atr,
            long_daily_state,
            start,
            end
        )

        long_closed = 0
        long_total_r = 0.0

        for trade in long_sim["trades"]:
            result_r = (
                portfolio_long_trade_r(
                    trade
                )
            )

            if result_r is None:
                continue

            long_closed += 1
            long_total_r += result_r

            combined.append({
                "instrument": instrument,
                "side": "BUY",
                "strategy": "LONG",
                "entry_time_utc": (
                    portfolio_iso(
                        trade[
                            "entry_time_utc"
                        ]
                    )
                ),
                "exit_time_utc": (
                    portfolio_iso(
                        trade[
                            "exit_time_utc"
                        ]
                    )
                ),
                "reference_entry": (
                    trade[
                        "reference_entry"
                    ]
                ),
                "backtest_entry": (
                    trade[
                        "backtest_entry"
                    ]
                ),
                "stop": trade["stop"],
                "target": trade["target"],
                "exit_reason": (
                    trade[
                        "exit_reason"
                    ]
                ),
                "result_r": result_r,
                "reward_risk": (
                    long_config[
                        "reward_risk"
                    ]
                ),
            })

        strategy_summaries.append({
            "instrument": instrument,
            "side": "BUY",
            "strategy": "LONG",
            "trades": long_closed,
            "total_r": long_total_r,
        })

        # --------------------------
        # SHORT
        # --------------------------
        if instrument in SHORT_STRATEGIES:
            short_config = (
                SHORT_STRATEGIES[
                    instrument
                ]
            )

            short_atr = atr_series(
                h1,
                short_config[
                    "atr_length"
                ]
            )

            short_daily_state = (
                build_short_daily_state(
                    daily,
                    short_config
                )
            )

            short_sim = (
                portfolio_simulate_shorts(
                    instrument,
                    h1,
                    short_atr,
                    short_daily_state,
                    start,
                    end
                )
            )

            short_closed = 0
            short_total_r = 0.0

            for trade in (
                short_sim["trades"]
            ):
                result_r = (
                    portfolio_short_trade_r(
                        trade
                    )
                )

                if result_r is None:
                    continue

                short_closed += 1
                short_total_r += (
                    result_r
                )

                combined.append({
                    "instrument": instrument,
                    "side": "SELL",
                    "strategy": "SHORT",
                    "entry_time_utc": (
                        portfolio_iso(
                            trade[
                                "entry_time_utc"
                            ]
                        )
                    ),
                    "exit_time_utc": (
                        portfolio_iso(
                            trade[
                                "exit_time_utc"
                            ]
                        )
                    ),
                    "reference_entry": (
                        trade[
                            "reference_entry"
                        ]
                    ),
                    "backtest_entry": (
                        trade[
                            "backtest_entry"
                        ]
                    ),
                    "stop": (
                        trade["stop"]
                    ),
                    "target": (
                        trade["target"]
                    ),
                    "exit_reason": (
                        trade[
                            "exit_reason"
                        ]
                    ),
                    "result_r": (
                        result_r
                    ),
                    "reward_risk": (
                        short_config[
                            "reward_risk"
                        ]
                    ),
                })

            strategy_summaries.append({
                "instrument": instrument,
                "side": "SELL",
                "strategy": "SHORT",
                "trades": short_closed,
                "total_r": short_total_r,
            })

    return (
        combined,
        strategy_summaries,
    )


def portfolio_parse_iso(
    value
):
    return datetime.fromisoformat(
        value.replace(
            "Z",
            "+00:00"
        )
    )


def portfolio_compound_by_entry_balance(
    trades,
    starting_balance=100.0,
    risk_percent=0.01,
):
    # Event-based compounding:
    # risk is fixed at entry as 1% of then-realised balance.
    # If trades overlap, each retains its own entry risk amount.
    #
    # The real executor sizes from OANDA NAV, which can include
    # unrealised P/L. This historical calculation deliberately
    # uses realised balance at entry so it remains deterministic
    # from the closed-trade log alone.
    events = []

    for index, trade in enumerate(
        trades
    ):
        events.append((
            portfolio_parse_iso(
                trade[
                    "entry_time_utc"
                ]
            ),
            1,
            "ENTRY",
            index,
        ))

        events.append((
            portfolio_parse_iso(
                trade[
                    "exit_time_utc"
                ]
            ),
            0,
            "EXIT",
            index,
        ))

    # EXIT before ENTRY on identical timestamps.
    events.sort(
        key=lambda item: (
            item[0],
            item[1],
        )
    )

    balance = float(
        starting_balance
    )

    entry_risk = {}
    peak = balance
    max_drawdown_pct = 0.0

    equity_curve = []

    for (
        event_time,
        _priority,
        event_type,
        trade_index,
    ) in events:
        trade = trades[
            trade_index
        ]

        if event_type == "ENTRY":
            entry_risk[
                trade_index
            ] = (
                balance
                * risk_percent
            )

        else:
            risk_amount = (
                entry_risk.get(
                    trade_index
                )
            )

            if risk_amount is None:
                continue

            pnl = (
                risk_amount
                * float(
                    trade[
                        "result_r"
                    ]
                )
            )

            balance += pnl

            peak = max(
                peak,
                balance
            )

            if peak > 0:
                drawdown_pct = (
                    (
                        balance
                        - peak
                    )
                    / peak
                    * 100.0
                )

                max_drawdown_pct = min(
                    max_drawdown_pct,
                    drawdown_pct,
                )

            equity_curve.append({
                "time": (
                    iso_utc(
                        event_time
                    )
                ),
                "balance": (
                    balance
                ),
                "pnl": pnl,
                "instrument": (
                    trade[
                        "instrument"
                    ]
                ),
                "side": (
                    trade[
                        "side"
                    ]
                ),
                "result_r": (
                    trade[
                        "result_r"
                    ]
                ),
            })

    return {
        "starting_balance": (
            starting_balance
        ),
        "ending_balance": (
            balance
        ),
        "return_pct": (
            (
                balance
                / starting_balance
            )
            - 1.0
        ) * 100.0,
        "max_closed_equity_drawdown_pct": (
            max_drawdown_pct
        ),
        "equity_curve": (
            equity_curve
        ),
    }


def portfolio_stats_for_subset(
    trades
):
    if not trades:
        return {
            "trades": 0,
            "winners": 0,
            "losers": 0,
            "win_rate": 0.0,
            "total_r": 0.0,
            "expectancy_r": 0.0,
            "profit_factor": 0.0,
        }

    results = [
        float(
            trade["result_r"]
        )
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

    profit_factor = (
        gross_profit
        / gross_loss
        if gross_loss > 0
        else (
            999.0
            if gross_profit > 0
            else 0.0
        )
    )

    return {
        "trades": len(
            results
        ),
        "winners": len(
            winners
        ),
        "losers": len(
            losers
        ),
        "win_rate": (
            len(winners)
            / len(results)
            * 100.0
        ),
        "total_r": (
            total_r
        ),
        "expectancy_r": (
            total_r
            / len(results)
        ),
        "profit_factor": (
            profit_factor
        ),
    }


def run_full_two_year_portfolio_backtest():
    # Exact two-year window ending at the most recent completed UTC hour.
    end = utc_now().replace(
        minute=0,
        second=0,
        microsecond=0
    )

    try:
        start = end.replace(
            year=end.year - 2
        )
    except ValueError:
        start = end.replace(
            month=2,
            day=28,
            year=end.year - 2
        )

    trades, strategy_summaries = (
        portfolio_collect_strategy_trades(
            start,
            end
        )
    )

    trades = [
        trade
        for trade in trades
        if (
            trade[
                "entry_time_utc"
            ] is not None
            and trade[
                "exit_time_utc"
            ] is not None
        )
    ]

    trades.sort(
        key=lambda trade: (
            portfolio_parse_iso(
                trade[
                    "entry_time_utc"
                ]
            ),
            trade["instrument"],
            trade["side"],
        )
    )

    overall = (
        portfolio_stats_for_subset(
            trades
        )
    )

    compound = (
        portfolio_compound_by_entry_balance(
            trades,
            starting_balance=100.0,
            risk_percent=0.01,
        )
    )

    midpoint = (
        start
        + (
            end - start
        ) / 2
    )

    first_year_trades = [
        trade
        for trade in trades
        if (
            portfolio_parse_iso(
                trade[
                    "entry_time_utc"
                ]
            )
            < midpoint
        )
    ]

    second_year_trades = [
        trade
        for trade in trades
        if (
            portfolio_parse_iso(
                trade[
                    "entry_time_utc"
                ]
            )
            >= midpoint
        )
    ]

    first_year = (
        portfolio_stats_for_subset(
            first_year_trades
        )
    )

    second_year = (
        portfolio_stats_for_subset(
            second_year_trades
        )
    )

    # Add display fields to per-strategy summaries.
    summary_rows = []

    for row in strategy_summaries:
        trades_count = int(
            row["trades"]
        )

        total_r = float(
            row["total_r"]
        )

        summary_rows.append({
            "instrument": (
                row[
                    "instrument"
                ]
            ),
            "side": (
                row["side"]
            ),
            "strategy": (
                row[
                    "strategy"
                ]
            ),
            "trades": (
                trades_count
            ),
            "total_r": round(
                total_r,
                4
            ),
            "uncompounded_return_at_1pct": round(
                total_r,
                4
            ),
            "average_r_per_trade": round(
                (
                    total_r
                    / trades_count
                )
                if trades_count
                else 0.0,
                4
            ),
        })

    summary_rows.sort(
        key=lambda row: (
            row["instrument"],
            row["side"],
        )
    )

    # Save full trade log.
    import csv

    with open(
        PORTFOLIO_BACKTEST_FILE,
        "w",
        newline="",
        encoding="utf-8",
    ) as handle:
        fieldnames = [
            "instrument",
            "side",
            "strategy",
            "entry_time_utc",
            "exit_time_utc",
            "reference_entry",
            "backtest_entry",
            "stop",
            "target",
            "exit_reason",
            "result_r",
            "reward_risk",
        ]

        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
        )

        writer.writeheader()

        for trade in trades:
            output = dict(
                trade
            )

            output[
                "result_r"
            ] = round(
                float(
                    output[
                        "result_r"
                    ]
                ),
                6,
            )

            writer.writerow(
                output
            )

    with open(
        PORTFOLIO_SUMMARY_FILE,
        "w",
        newline="",
        encoding="utf-8",
    ) as handle:
        fieldnames = [
            "instrument",
            "side",
            "strategy",
            "trades",
            "total_r",
            "uncompounded_return_at_1pct",
            "average_r_per_trade",
        ]

        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
        )

        writer.writeheader()

        for row in summary_rows:
            writer.writerow(
                row
            )

    result = {
        "status": "success",
        "mode": "READ_ONLY",
        "orders_submitted": False,
        "window": {
            "from": iso_utc(start),
            "to": iso_utc(end),
        },
        "portfolio": {
            "strategies": len(
                strategy_summaries
            ),
            "closed_trades": (
                overall[
                    "trades"
                ]
            ),
            "winners": (
                overall[
                    "winners"
                ]
            ),
            "losers": (
                overall[
                    "losers"
                ]
            ),
            "win_rate": round(
                overall[
                    "win_rate"
                ],
                2,
            ),
            "profit_factor": round(
                overall[
                    "profit_factor"
                ],
                3,
            ),
            "total_r": round(
                overall[
                    "total_r"
                ],
                3,
            ),
            "expectancy_r": round(
                overall[
                    "expectancy_r"
                ],
                3,
            ),
            "uncompounded_return_at_1pct_risk_pct": round(
                overall[
                    "total_r"
                ],
                2,
            ),
            "balance_compounded_return_at_1pct_risk_pct": round(
                compound[
                    "return_pct"
                ],
                2,
            ),
            "balance_compounded_100_to": round(
                compound[
                    "ending_balance"
                ],
                2,
            ),
            "max_closed_equity_drawdown_pct": round(
                compound[
                    "max_closed_equity_drawdown_pct"
                ],
                2,
            ),
        },
        "first_12_months": {
            "trades": first_year[
                "trades"
            ],
            "total_r": round(
                first_year[
                    "total_r"
                ],
                3,
            ),
            "uncompounded_return_at_1pct_pct": round(
                first_year[
                    "total_r"
                ],
                2,
            ),
        },
        "second_12_months": {
            "trades": second_year[
                "trades"
            ],
            "total_r": round(
                second_year[
                    "total_r"
                ],
                3,
            ),
            "uncompounded_return_at_1pct_pct": round(
                second_year[
                    "total_r"
                ],
                2,
            ),
        },
        "by_strategy": summary_rows,
        "method_note": (
            "Uncompounded return maps 1R to 1% of account risk. "
            "Balance-compounded return fixes each trade's risk at entry "
            "to 1% of then-realised balance; the live executor uses OANDA "
            "NAV, which may include unrealised P/L on overlapping positions."
        ),
        "trade_log_download": (
            "/download-full-strategy-two-year"
        ),
        "summary_download": (
            "/download-full-strategy-two-year-summary"
        ),
    }

    return result


PORTFOLIO_STATUS = {
    "state": "not_started",
    "message": "Backtest has not started",
    "orders_submitted": False,
}


def run_portfolio_backtest_background():
    global PORTFOLIO_BACKTEST_CACHE

    try:
        PORTFOLIO_STATUS.update({
            "state": "running",
            "message": (
                "Running full 10-strategy two-year backtest"
            ),
            "orders_submitted": False,
        })

        with PORTFOLIO_BACKTEST_LOCK:
            PORTFOLIO_BACKTEST_CACHE = (
                run_full_two_year_portfolio_backtest()
            )

        PORTFOLIO_STATUS.update({
            "state": "complete",
            "message": (
                "Full 10-strategy two-year backtest complete"
            ),
            "orders_submitted": False,
            "result": PORTFOLIO_BACKTEST_CACHE,
        })

    except Exception as error:
        PORTFOLIO_STATUS.update({
            "state": "error",
            "message": str(error),
            "orders_submitted": False,
        })

        print(
            "PORTFOLIO BACKTEST ERROR:",
            error,
            flush=True,
        )


@app.route(
    "/full-strategy-two-year"
)
def full_strategy_two_year():
    return jsonify(
        PORTFOLIO_STATUS
    )


@app.route(
    "/status"
)
def portfolio_status():
    return jsonify(
        PORTFOLIO_STATUS
    )


@app.route(
    "/download-full-strategy-two-year"
)
def download_full_strategy_two_year():
    if not os.path.exists(
        PORTFOLIO_BACKTEST_FILE
    ):
        return jsonify({
            "status": "not_ready",
            "message": (
                "Run /full-strategy-two-year first"
            ),
        }), 404

    return send_file(
        PORTFOLIO_BACKTEST_FILE,
        as_attachment=True,
        download_name=(
            PORTFOLIO_BACKTEST_FILE
        ),
    )


@app.route(
    "/download-full-strategy-two-year-summary"
)
def download_full_strategy_two_year_summary():
    if not os.path.exists(
        PORTFOLIO_SUMMARY_FILE
    ):
        return jsonify({
            "status": "not_ready",
            "message": (
                "Run /full-strategy-two-year first"
            ),
        }), 404

    return send_file(
        PORTFOLIO_SUMMARY_FILE,
        as_attachment=True,
        download_name=(
            PORTFOLIO_SUMMARY_FILE
        ),
    )



# ==================================================
# START BACKGROUND WATCHER
# ==================================================

# Research copy: live watcher deliberately disabled.
# start_live_watcher_once()


# ==================================================
# LOCAL START
# ==================================================

if __name__ == "__main__":
    research_thread = threading.Thread(
        target=run_portfolio_backtest_background,
        name="full-10-strategy-two-year-backtest",
        daemon=True,
    )

    research_thread.start()

    port = int(
        os.getenv(
            "PORT",
            5000,
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
    )
