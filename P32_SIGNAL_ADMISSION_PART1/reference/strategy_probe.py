from flask import Flask, jsonify, request
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
import bisect
import math
import os
import time
import threading
import sqlite3
import json
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

# EUR/JPY H1 short strategy #22 has its own activation gate.
EURJPY_SHORT_LIVE_ENABLED = (
    os.getenv("EURJPY_SHORT_LIVE_ENABLED", "false")
    .strip()
    .lower()
    == "true"
)

# EUR/JPY M15 short strategy #24 has its own activation gate so the
# code can be deployed and pipeline-tested without pausing the other M15
# strategies. Set true only after the executor has been redeployed with #24.
EURJPY_M15_SHORT_LIVE_ENABLED = (
    os.getenv("EURJPY_M15_SHORT_LIVE_ENABLED", "false")
    .strip()
    .lower()
    == "true"
)


# AUD/USD H1 LONG strategy #25 has its own activation gate so the
# executor + strategy engine can be deployed and pipeline-tested first.
AUDUSD_H1_LONG_LIVE_ENABLED = (
    os.getenv("AUDUSD_H1_LONG_LIVE_ENABLED", "false")
    .strip()
    .lower()
    == "true"
)


# AUD/USD H1 SHORT strategy #26 has its own activation gate so the
# executor + strategy engine can be deployed and pipeline-tested before
# live SELL submission is enabled.
AUDUSD_H1_SHORT_LIVE_ENABLED = (
    os.getenv("AUDUSD_H1_SHORT_LIVE_ENABLED", "false")
    .strip()
    .lower()
    == "true"
)


# AUD/USD M15 SHORT strategy #27: deploy and verify the frozen evaluator
# with this switch OFF. Enable only after the live pipeline has been checked.
AUDUSD_M15_SHORT_LIVE_ENABLED = (
    os.getenv("AUDUSD_M15_SHORT_LIVE_ENABLED", "false")
    .strip()
    .lower()
    == "true"
)

# AUD/JPY M15 LONG #28: only the frozen D<=0.50 ATR core is eligible.
# Separate from all existing H1/M15 activation switches. OFF by default.
AUDJPY_M15_LONG_LIVE_ENABLED = (
    os.getenv("AUDJPY_M15_LONG_LIVE_ENABLED", "false")
    .strip().lower() == "true"
)

# AUD/JPY M15 SHORT #29: frozen rally-rejection CORE only.
# Fail-closed by default; enable after the executor containing #29 is deployed.
AUDJPY_M15_SHORT_LIVE_ENABLED = (
    os.getenv("AUDJPY_M15_SHORT_LIVE_ENABLED", "false")
    .strip().lower() == "true"
)

# AUD/JPY H1 LONG TIGHT QUALITY #30: frozen low-frequency portfolio addition.
# Deploy and validate with this OFF, then enable only after the P30 executor
# containing AUD_JPY_H1_LONG_TIGHT_QUALITY is live.
AUDJPY_H1_LONG_TIGHT_LIVE_ENABLED = (
    os.getenv("AUDJPY_H1_LONG_TIGHT_LIVE_ENABLED", "false")
    .strip().lower() == "true"
)

# AUD/JPY H1 SHORT PRIMARY RR3.25 #31: frozen portfolio-admitted strategy.
# Deploy and pipeline-test with this OFF, then enable only after the P31 executor
# containing AUD_JPY_H1_SHORT_PRIMARY_RR3P25 is live.
AUDJPY_H1_SHORT_PRIMARY_LIVE_ENABLED = (
    os.getenv("AUDJPY_H1_SHORT_PRIMARY_LIVE_ENABLED", "false")
    .strip().lower() == "true"
)

LIVE_WATCHER_ENABLED = (
    os.getenv("LIVE_WATCHER_ENABLED", "true")
    .strip()
    .lower()
    == "true"
)

# M15 watcher runs independently of the existing H1 watcher.
# Keep submission disabled for the first redeploy/pipeline test, then set true.
M15_LIVE_WATCHER_ENABLED = (
    os.getenv("M15_LIVE_WATCHER_ENABLED", "true")
    .strip()
    .lower()
    == "true"
)

M15_LIVE_SUBMISSION_ENABLED = (
    os.getenv("M15_LIVE_SUBMISSION_ENABLED", "false")
    .strip()
    .lower()
    == "true"
)

M15_POLL_OFFSET_SECONDS = 0.35
M15_POLL_INTERVAL_SECONDS = 0.75
M15_POLL_WINDOW_SECONDS = 20

M15_LAST_PROCESSED = {}
M15_WATCHER_STARTED = False
M15_STATE_LOCK = threading.Lock()

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

WATCHER_RETRY_FIX_VERSION = "WATCHER_RETRY_FIX_V1_2026_10_06"
WATCHER_LAST_CYCLE = {"H1": None, "M15": None}

LIVE_STATE_LOCK = threading.Lock()

LAST_PROCESSED_CANDLE = {}
LAST_PAIR_STATUS = {}

WATCHER_STARTED = False


# ==================================================
# PERSISTENT QUALIFYING-SIGNAL AUDIT
# ==================================================
# Monitoring only. This DB is never consulted by signal generation, sizing,
# gating or order submission. Mount a Railway persistent volume at /data (or
# set SIGNAL_AUDIT_DB_PATH to another persistent path) to retain history across
# deployments. Any audit write failure is fail-open and cannot block trading.
SIGNAL_AUDIT_VERSION = "P31_SIGNAL_AUDIT_V1"
SIGNAL_AUDIT_DB_PATH = os.getenv(
    "SIGNAL_AUDIT_DB_PATH",
    "/data/signal_audit.db",
).strip()
SIGNAL_AUDIT_LOCK = threading.Lock()
SIGNAL_AUDIT_LAST_ERROR = None



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

        # Final replacement long - Candidate A.
        # Bullish engulfing body ratio >= 1.00.
        "minimum_body_ratio": 1.00,

        # Close location >= 0.60.
        "strong_close_enabled": True,
        "minimum_close_location": 0.60,

        # No wick filter.
        "lower_wick_filter_enabled": False,
        "minimum_lower_wick_body_ratio": None,

        "atr_length": 14,

        # Previous 15 H1 bars excluding current;
        # signal low within 0.10 ATR14 of previous lowest low.
        "structure_lookback": 15,
        "maximum_distance_atr": 0.10,

        # No minimum-range filter.
        "minimum_range_enabled": False,
        "minimum_range_atr": None,

        # No daily EMA regime or alignment.
        # EMA1 placeholders preserve the shared daily-state builder.
        "fast_daily_ema": 1,
        "slow_daily_ema": 1,
        "daily_close_ema": None,
        "require_daily_close_above_slow": False,
        "require_daily_fast_above_slow": False,

        # No daily ATR regime.
        "minimum_daily_atr_ratio_50": None,

        "session_timezone": "America/New_York",

        # Include 08:00-15:59 New York.
        "session_mode": "include",
        "session_start_hour": 8,
        "session_end_hour": 16,

        # Tuesday + Friday excluded.
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
    # USD/JPY - FINAL REPLACEMENT LONG
    # ==============================================

    "USD_JPY": {

        "tick_size": 0.001,
        "price_precision": 3,
        "signal_id_prefix": "USDJPY",

        # Exact bullish engulfing body ratio >= 1.00.
        "minimum_body_ratio": 1.00,

        # Strong close >= 0.60.
        "strong_close_enabled": True,
        "minimum_close_location": 0.60,

        # No wick filter.
        "lower_wick_filter_enabled": False,
        "minimum_lower_wick_body_ratio": None,

        "atr_length": 14,

        # Signal body >= 0.80 ATR14.
        "minimum_body_atr": 0.80,

        # Previous 100 H1 bars excluding current;
        # signal low within 0.55 ATR14 of previous lowest low.
        "structure_lookback": 100,
        "maximum_distance_atr": 0.55,

        # No minimum-range filter.
        "minimum_range_enabled": False,
        "minimum_range_atr": None,

        # No daily EMA regime or alignment.
        # EMA1 placeholders preserve the shared daily-state builder.
        "fast_daily_ema": 1,
        "slow_daily_ema": 1,
        "daily_close_ema": None,
        "require_daily_close_above_slow": False,
        "require_daily_fast_above_slow": False,

        # No daily ATR regime.
        "minimum_daily_atr_ratio_50": None,

        # No session exclusion.
        "session_timezone": "America/New_York",
        "session_mode": "all",
        "session_start_hour": None,
        "session_end_hour": None,

        # No weekday exclusions.
        "excluded_weekdays": set(),

        "reward_risk": 3.75,
        "stop_buffer_ticks": 10
    },

    # ==============================================
    # USD/CAD - FINAL REPLACEMENT LONG (CANDIDATE B)
    # ==============================================

    "USD_CAD": {

        "tick_size": 0.00001,
        "price_precision": 5,
        "signal_id_prefix": "USDCAD",

        # Bullish engulfing body ratio >= 1.10
        "minimum_body_ratio": 1.10,

        # Strong close disabled
        "strong_close_enabled": False,
        "minimum_close_location": 0.75,

        # Lower wick >= 0.20 x current body
        "lower_wick_filter_enabled": True,
        "minimum_lower_wick_body_ratio": 0.20,

        "atr_length": 14,

        # Signal body >= 0.60 ATR14
        "minimum_body_atr": 0.60,

        # Previous 40 H1 bars excluding current;
        # signal low within 0.05 ATR14 of previous lowest low
        "structure_lookback": 40,
        "maximum_distance_atr": 0.05,

        # Signal range >= 1.20 ATR14
        "minimum_range_enabled": True,
        "minimum_range_atr": 1.20,

        # No daily EMA regime/alignment.
        # EMA1 placeholders preserve the shared daily-state builder.
        "fast_daily_ema": 1,
        "slow_daily_ema": 1,
        "require_daily_close_above_slow": False,
        "require_daily_fast_above_slow": False,

        # Previous completed Daily ATR14 / 50-day mean >= 0.95
        "minimum_daily_atr_ratio_50": 0.95,

        # No session exclusion
        "session_timezone": "America/New_York",
        "session_mode": "all",
        "session_start_hour": None,
        "session_end_hour": None,

        # No weekday exclusions
        "excluded_weekdays": set(),

        "reward_risk": 3.50,
        "stop_buffer_ticks": 10
    },

    # ==============================================
    # EUR/GBP - FINAL REPLACEMENT LONG (CANDIDATE B)
    # ==============================================

    "EUR_GBP": {

        "tick_size": 0.00001,
        "price_precision": 5,
        "signal_id_prefix": "EURGBP",

        # Bullish engulfing body ratio >= 1.00
        "minimum_body_ratio": 1.00,

        # Strong-close filter disabled.
        "strong_close_enabled": False,
        "minimum_close_location": 0.75,

        # No wick filter.
        "lower_wick_filter_enabled": False,
        "minimum_lower_wick_body_ratio": None,

        "atr_length": 14,

        # Signal body >= 1.10 ATR14.
        "minimum_body_atr": 1.10,

        # Previous 30 H1 bars excluding current;
        # signal low within 0.075 ATR14 of previous lowest low.
        "structure_lookback": 30,
        "maximum_distance_atr": 0.075,

        # Signal range >= 1.40 ATR14.
        "minimum_range_enabled": True,
        "minimum_range_atr": 1.40,

        # Candidate B uses two separate daily conditions:
        # previous completed daily close > EMA200
        # AND previous completed daily EMA20 > EMA150.
        "daily_close_ema": 200,
        "fast_daily_ema": 20,
        "slow_daily_ema": 150,

        # Disable the old shared close>slow test because
        # the close regime is handled by daily_close_ema.
        "require_daily_close_above_slow": False,
        "require_daily_fast_above_slow": True,

        # No daily ATR regime.
        "minimum_daily_atr_ratio_50": None,

        # No session restriction.
        "session_timezone": "Europe/London",
        "session_mode": "all",
        "session_start_hour": None,
        "session_end_hour": None,

        # No weekday exclusions.
        "excluded_weekdays": set(),

        "reward_risk": 3.00,
        "stop_buffer_ticks": 10
    },

    # ==============================================
    # EUR/JPY — FINAL LOCKED H1 LONG (strategy #21)
    # ==============================================

    "EUR_JPY": {

        "tick_size": 0.001,
        "price_precision": 3,
        "signal_id_prefix": "EURJPY",

        # Exact bullish engulfing body ratio >= 1.30.
        "minimum_body_ratio": 1.30,

        # No strong-close filter.
        "strong_close_enabled": False,
        "minimum_close_location": None,

        # No wick filter.
        "lower_wick_filter_enabled": False,
        "minimum_lower_wick_body_ratio": None,

        "atr_length": 14,

        # Signal body >= 1.25 ATR14.
        "minimum_body_atr": 1.25,

        # Previous 15 H1 bars excluding current.
        # ABS(signal low - prior 15-bar low) <= 0.15 ATR14.
        "structure_lookback": 15,
        "maximum_distance_atr": 0.15,
        "structure_absolute_distance": True,

        # No minimum-range filter.
        "minimum_range_enabled": False,
        "minimum_range_atr": None,

        # No daily EMA regime/alignment.
        # EMA1 placeholders preserve the shared daily-state builder.
        "fast_daily_ema": 1,
        "slow_daily_ema": 1,
        "daily_close_ema": None,
        "require_daily_close_above_slow": False,
        "require_daily_fast_above_slow": False,

        # No daily ATR regime.
        "minimum_daily_atr_ratio_50": None,

        # Context filter:
        # exclude signal candles opening 08:00-08:59 New York.
        "session_timezone": "America/New_York",
        "session_mode": "exclude",
        "session_start_hour": 8,
        "session_end_hour": 9,

        # Exclude Friday in New York time.
        "excluded_weekdays": {
            4
        },

        "reward_risk": 5.50,
        "stop_buffer_ticks": 10
    },

    # ==============================================
    # AUD/USD — FINAL LOCKED H1 LONG (strategy #25)
    # ==============================================
    "AUD_USD": {

        "signal_family": "outside_reversal",
        "tick_size": 0.00001,
        "price_precision": 5,
        "signal_id_prefix": "AUDUSD",

        # OUTSIDE_REVERSAL does not use engulf body ratio.
        # 0.0 is retained only for compatibility with the shared evaluator.
        "minimum_body_ratio": 0.0,

        # Signal must be bullish with body >= 0.75 ATR14.
        "minimum_body_atr": 0.75,

        # Close location >= 0.75.
        "strong_close_enabled": True,
        "minimum_close_location": 0.75,

        # No wick filter.
        "lower_wick_filter_enabled": False,
        "minimum_lower_wick_body_ratio": None,

        "atr_length": 14,

        # Previous 25 H1 bars excluding current.
        # ABS(signal low - prior 25-bar low) <= 0.20 ATR14.
        "structure_lookback": 25,
        "maximum_distance_atr": 0.20,
        "structure_absolute_distance": True,

        # No minimum-range filter.
        "minimum_range_enabled": False,
        "minimum_range_atr": None,

        # No daily regime/alignment.
        "fast_daily_ema": 1,
        "slow_daily_ema": 1,
        "daily_close_ema": None,
        "require_daily_close_above_slow": False,
        "require_daily_fast_above_slow": False,
        "minimum_daily_atr_ratio_50": None,

        # Final session lock:
        # signal-open must be inside Tokyo 08:00-16:59
        # OR Sydney 08:00-16:59, DST-aware.
        "session_timezone": "Asia/Tokyo",
        "session_mode": "asia_union",
        "session_start_hour": None,
        "session_end_hour": None,

        # No weekday exclusions.
        "excluded_weekdays": set(),

        "reward_risk": 3.25,
        "stop_buffer_ticks": 10
    },

    # ==============================================
    # AUD/JPY — FINAL LOCKED H1 LONG TIGHT QUALITY
    # strategy #30, admitted by exact P29 -> P30 replay
    # ==============================================
    "AUD_JPY": {

        "strategy_id": "AUD_JPY_H1_LONG_TIGHT_QUALITY",
        "trigger_id": "H1_LONG_TIGHT_QUALITY",
        "signal_family": "bullish_engulfing",
        "tick_size": 0.001,
        "price_precision": 3,
        "signal_id_prefix": "AUDJPYH1TIGHT",

        # Exact bullish engulf. The exact engulf geometry itself guarantees
        # body ratio >= 1.00, retained explicitly for shared evaluator parity.
        "minimum_body_ratio": 1.00,

        # Signal body >= 1.00 ATR14.
        "minimum_body_atr": 1.00,

        # No strong-close filter.
        "strong_close_enabled": False,
        "minimum_close_location": None,

        # No lower-wick filter.
        "lower_wick_filter_enabled": False,
        "minimum_lower_wick_body_ratio": None,

        "atr_length": 14,

        # Previous 12 H1 bars excluding current; ABS(signal low - prior
        # 12-bar low) <= 0.15 ATR14.
        "structure_lookback": 12,
        "maximum_distance_atr": 0.15,
        "structure_absolute_distance": True,

        # Signal range >= 1.75 ATR14.
        "minimum_range_enabled": True,
        "minimum_range_atr": 1.75,

        # Previous 10 completed H1 bars must show a fall of at least
        # 0.90 current-signal ATR14:
        # (close[i-1] - close[i-11]) / ATR14[i] <= -0.90.
        "prior_movement_lookback_bars": 10,
        "maximum_prior_movement_atr": -0.90,

        # No daily regime/alignment. EMA1 placeholders preserve the shared
        # daily-state builder without adding a filter.
        "fast_daily_ema": 1,
        "slow_daily_ema": 1,
        "daily_close_ema": None,
        "require_daily_close_above_slow": False,
        "require_daily_fast_above_slow": False,
        "minimum_daily_atr_ratio_50": None,

        # No session or weekday filter.
        "session_timezone": "UTC",
        "session_mode": "all",
        "session_start_hour": None,
        "session_end_hour": None,
        "excluded_weekdays": set(),

        "reward_risk": 4.25,
        "stop_buffer_ticks": 10
    },


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
,

    # ==============================================
    # AUD/USD FINAL LOCKED H1 SHORT — strategy #26
    # ==============================================
    "AUD_USD": {
        "strategy_name": "AUDUSD_FINAL_TWO_TRIGGER",
        "signal_family": "audusd_two_trigger",
        "tick_size": 0.00001,
        "price_precision": 5,
        "signal_id_prefix": "AUDUSDSHORT",

        "atr_length": 14,

        # Trigger A — COMPRESSION_BREAKOUT.
        "compression_mean_lookback": 20,
        "maximum_previous_atr_ratio_20": 0.85,
        "minimum_body_atr": 1.25,
        "minimum_signal_range_atr": 1.50,
        "breakdown_lookback": 15,

        # Trigger B — SWEEP_DISPLACEMENT.
        "sweep_lookback": 15,
        "minimum_upper_wick_body_ratio": 0.25,
        "prior_momentum_lookback_bars": 4,
        "minimum_prior_upward_momentum_atr": 0.25,

        # Shared structure/warm-up compatibility field.
        "structure_lookback": 15,

        # Shared short evaluator compatibility fields; inactive for this family.
        "minimum_body_ratio": 1.00,
        "maximum_close_location": None,
        "maximum_distance_atr": 999.0,
        "fast_daily_ema": 1,
        "slow_daily_ema": 1,
        "require_daily_close_below_slow": False,
        "require_daily_fast_below_slow": False,
        "minimum_daily_ema_separation_atr": None,
        "maximum_slow_ema_slope_5d_atr": None,
        "minimum_daily_atr_ratio_50": None,

        # No session or weekday filter.
        "session_timezone": "UTC",
        "excluded_hours": set(),
        "excluded_weekdays": set(),

        "reward_risk": 3.50,
        "stop_buffer_ticks": 10
    },

    # ==============================================
    # EUR/JPY FINAL LOCKED H1 SHORT — strategy #22
    # ==============================================
    "EUR_JPY": {
        "strategy_name": "EURJPY_COMPRESSION_BREAKDOWN_SHORT",
        "signal_family": "compression_breakdown",
        "tick_size": 0.001,
        "price_precision": 3,
        "signal_id_prefix": "EURJPYSHORT",

        "atr_length": 14,

        # Previous completed H1 ATR14 / previous-20 ATR14 mean <= 0.80.
        "compression_mean_lookback": 20,
        "maximum_previous_atr_ratio_20": 0.80,

        # Signal candle must be bearish with body >= 1.10 ATR14
        # and total range >= 1.65 ATR14.
        "minimum_body_atr": 1.10,
        "minimum_signal_range_atr": 1.65,

        # Close must break below the previous 2 H1 lows, current excluded.
        "structure_lookback": 2,
        "breakdown_lookback": 2,

        # Fields retained only for compatibility with the shared short
        # evaluator/daily-state builder; they are NOT active for this family.
        "minimum_body_ratio": 1.00,
        "maximum_close_location": None,
        "maximum_distance_atr": 999.0,
        "fast_daily_ema": 1,
        "slow_daily_ema": 1,
        "require_daily_close_below_slow": False,
        "require_daily_fast_below_slow": False,
        "minimum_daily_ema_separation_atr": None,
        "maximum_slow_ema_slope_5d_atr": None,
        "minimum_daily_atr_ratio_50": None,

        # Candidate B context lock: exclude NY 19:00-19:59 only.
        "session_timezone": "America/New_York",
        "excluded_hours": {19},
        "excluded_weekdays": set(),

        "reward_risk": 7.25,
        "stop_buffer_ticks": 10
    },

    # ==============================================
    # AUD/JPY FINAL LOCKED H1 SHORT PRIMARY — strategy #31
    # admitted by exact Portfolio 30 -> 31 replay
    # ==============================================
    "AUD_JPY": {
        "strategy_id": "AUD_JPY_H1_SHORT_PRIMARY_RR3P25",
        "strategy_name": "AUDJPY_H1_SHORT_PRIMARY_RR3P25",
        "trigger_id": "H1_SHORT_PRIMARY_PREVCLOSE050",
        "signal_family": "audjpy_h1_short_primary",
        "tick_size": 0.001,
        "price_precision": 3,
        "signal_id_prefix": "AUDJPYH1SHORTPRIMARY",

        "atr_length": 14,
        "structure_lookback": 175,
        "maximum_distance_atr": 0.10,
        "structure_absolute_distance": True,
        "minimum_body_atr": 0.60,
        "minimum_signal_range_atr": 1.25,
        "minimum_previous_close_location": 0.50,

        # Exact bearish engulf itself is the body-engulf condition.
        "minimum_body_ratio": 1.00,
        "maximum_close_location": None,

        # No D1, session, weekday, EMA, volatility or momentum filter.
        "requires_daily_state": False,
        "fast_daily_ema": 1,
        "slow_daily_ema": 1,
        "require_daily_close_below_slow": False,
        "require_daily_fast_below_slow": False,
        "minimum_daily_ema_separation_atr": None,
        "maximum_slow_ema_slope_5d_atr": None,
        "minimum_daily_atr_ratio_50": None,
        "session_timezone": "UTC",
        "excluded_hours": set(),
        "excluded_weekdays": set(),

        "reward_risk": 3.25,
        "stop_buffer_ticks": 10
    }

}


# ==================================================
# OANDA HTTP
# ==================================================

# EUR/CHF primary: frozen research rule, separate observed state and audit.
from contextlib import contextmanager
EURCHF_PRIMARY_ID = "EURCHF_H1_SHORT_PRIMARY_L060_D0075_SC035_PC075_RR3P50_V1"
EURCHF_PASS14_VERSION = "EURCHF_PASS14_PROBE_V1_2026_10_05"
EURCHF_H1_SHORT_PRIMARY_LIVE_ENABLED = (
    os.getenv("EURCHF_H1_SHORT_PRIMARY_LIVE_ENABLED", "false").strip().lower() == "true"
)
EURCHF_PASS14_PREVIEW_ENABLED = (
    os.getenv("EURCHF_PASS14_PREVIEW_ENABLED", "true").strip().lower() == "true"
)
EURCHF_PASS14_DB_PATH = os.getenv("EURCHF_PASS14_DB_PATH", "/data/eurchf_pass14.db")
EURCHF_PASS14_LOCK = threading.RLock()
EURCHF_PASS14_LAST_ERROR = None
# Replaced by the build with the verified final ATR and prior 60 source bars.
EURCHF_FROZEN_ANCHOR = {'source_sha256': '862bf19a2e6a0e8d3c27b961c68c9fc0d104a5d64bd6eaa87fc2665afa38c196', 'observed_count': 137819, 'atr14': 0.0007041777811075741, 'previous_atr14': 0.0007460376104235375, 'bars': [{'open': 0.94573, 'high': 0.9465, 'low': 0.9455, 'close': 0.94623, 'time': '2026-09-28T11:00:00Z'}, {'open': 0.94624, 'high': 0.9466, 'low': 0.94582, 'close': 0.94588, 'time': '2026-09-28T12:00:00Z'}, {'open': 0.94586, 'high': 0.94623, 'low': 0.94476, 'close': 0.94514, 'time': '2026-09-28T13:00:00Z'}, {'open': 0.94518, 'high': 0.9457, 'low': 0.94489, 'close': 0.9457, 'time': '2026-09-28T14:00:00Z'}, {'open': 0.94568, 'high': 0.94616, 'low': 0.94546, 'close': 0.94594, 'time': '2026-09-28T15:00:00Z'}, {'open': 0.94593, 'high': 0.94608, 'low': 0.94552, 'close': 0.94588, 'time': '2026-09-28T16:00:00Z'}, {'open': 0.9459, 'high': 0.94605, 'low': 0.9456, 'close': 0.94594, 'time': '2026-09-28T17:00:00Z'}, {'open': 0.94596, 'high': 0.94602, 'low': 0.94569, 'close': 0.94576, 'time': '2026-09-28T18:00:00Z'}, {'open': 0.94576, 'high': 0.94604, 'low': 0.94569, 'close': 0.94578, 'time': '2026-09-28T19:00:00Z'}, {'open': 0.9458, 'high': 0.94636, 'low': 0.94569, 'close': 0.94612, 'time': '2026-09-28T20:00:00Z'}, {'open': 0.9444, 'high': 0.94584, 'low': 0.9444, 'close': 0.94568, 'time': '2026-09-28T21:00:00Z'}, {'open': 0.94568, 'high': 0.94632, 'low': 0.94568, 'close': 0.94594, 'time': '2026-09-28T22:00:00Z'}, {'open': 0.94594, 'high': 0.94598, 'low': 0.9458, 'close': 0.94588, 'time': '2026-09-28T23:00:00Z'}, {'open': 0.94586, 'high': 0.94624, 'low': 0.94586, 'close': 0.94606, 'time': '2026-09-29T00:00:00Z'}, {'open': 0.94608, 'high': 0.94627, 'low': 0.94589, 'close': 0.94615, 'time': '2026-09-29T01:00:00Z'}, {'open': 0.94616, 'high': 0.94624, 'low': 0.94594, 'close': 0.9461, 'time': '2026-09-29T02:00:00Z'}, {'open': 0.9461, 'high': 0.94632, 'low': 0.94599, 'close': 0.94628, 'time': '2026-09-29T03:00:00Z'}, {'open': 0.94626, 'high': 0.94678, 'low': 0.94622, 'close': 0.94676, 'time': '2026-09-29T04:00:00Z'}, {'open': 0.94676, 'high': 0.94715, 'low': 0.94615, 'close': 0.94615, 'time': '2026-09-29T05:00:00Z'}, {'open': 0.94616, 'high': 0.94698, 'low': 0.94593, 'close': 0.94688, 'time': '2026-09-29T06:00:00Z'}, {'open': 0.94689, 'high': 0.94704, 'low': 0.94524, 'close': 0.94588, 'time': '2026-09-29T07:00:00Z'}, {'open': 0.9459, 'high': 0.94646, 'low': 0.9458, 'close': 0.9464, 'time': '2026-09-29T08:00:00Z'}, {'open': 0.9464, 'high': 0.94692, 'low': 0.94582, 'close': 0.94618, 'time': '2026-09-29T09:00:00Z'}, {'open': 0.94616, 'high': 0.94656, 'low': 0.94564, 'close': 0.94564, 'time': '2026-09-29T10:00:00Z'}, {'open': 0.94563, 'high': 0.94606, 'low': 0.9455, 'close': 0.94584, 'time': '2026-09-29T11:00:00Z'}, {'open': 0.94586, 'high': 0.9462, 'low': 0.9455, 'close': 0.94582, 'time': '2026-09-29T12:00:00Z'}, {'open': 0.94581, 'high': 0.94618, 'low': 0.94552, 'close': 0.94589, 'time': '2026-09-29T13:00:00Z'}, {'open': 0.94586, 'high': 0.94618, 'low': 0.94554, 'close': 0.94593, 'time': '2026-09-29T14:00:00Z'}, {'open': 0.94594, 'high': 0.94642, 'low': 0.94579, 'close': 0.94614, 'time': '2026-09-29T15:00:00Z'}, {'open': 0.94613, 'high': 0.94641, 'low': 0.94552, 'close': 0.94559, 'time': '2026-09-29T16:00:00Z'}, {'open': 0.94558, 'high': 0.94576, 'low': 0.94548, 'close': 0.94568, 'time': '2026-09-29T17:00:00Z'}, {'open': 0.94568, 'high': 0.9457, 'low': 0.94514, 'close': 0.94535, 'time': '2026-09-29T18:00:00Z'}, {'open': 0.94538, 'high': 0.94563, 'low': 0.94534, 'close': 0.9456, 'time': '2026-09-29T19:00:00Z'}, {'open': 0.94562, 'high': 0.94579, 'low': 0.94538, 'close': 0.94572, 'time': '2026-09-29T20:00:00Z'}, {'open': 0.9463, 'high': 0.94655, 'low': 0.94552, 'close': 0.94583, 'time': '2026-09-29T21:00:00Z'}, {'open': 0.94598, 'high': 0.94612, 'low': 0.94556, 'close': 0.9461, 'time': '2026-09-29T22:00:00Z'}, {'open': 0.94612, 'high': 0.94618, 'low': 0.94604, 'close': 0.94606, 'time': '2026-09-29T23:00:00Z'}, {'open': 0.94606, 'high': 0.94606, 'low': 0.94578, 'close': 0.94593, 'time': '2026-09-30T00:00:00Z'}, {'open': 0.94594, 'high': 0.94628, 'low': 0.94525, 'close': 0.94535, 'time': '2026-09-30T01:00:00Z'}, {'open': 0.94534, 'high': 0.9455, 'low': 0.94486, 'close': 0.94522, 'time': '2026-09-30T02:00:00Z'}, {'open': 0.94524, 'high': 0.94557, 'low': 0.94518, 'close': 0.94556, 'time': '2026-09-30T03:00:00Z'}, {'open': 0.94556, 'high': 0.9458, 'low': 0.94545, 'close': 0.9458, 'time': '2026-09-30T04:00:00Z'}, {'open': 0.94579, 'high': 0.94586, 'low': 0.9455, 'close': 0.94578, 'time': '2026-09-30T05:00:00Z'}, {'open': 0.9458, 'high': 0.9462, 'low': 0.94551, 'close': 0.94583, 'time': '2026-09-30T06:00:00Z'}, {'open': 0.94584, 'high': 0.94632, 'low': 0.94536, 'close': 0.94582, 'time': '2026-09-30T07:00:00Z'}, {'open': 0.94582, 'high': 0.9461, 'low': 0.94552, 'close': 0.94564, 'time': '2026-09-30T08:00:00Z'}, {'open': 0.94563, 'high': 0.94688, 'low': 0.94559, 'close': 0.9468, 'time': '2026-09-30T09:00:00Z'}, {'open': 0.94681, 'high': 0.94738, 'low': 0.94666, 'close': 0.94718, 'time': '2026-09-30T10:00:00Z'}, {'open': 0.94717, 'high': 0.94771, 'low': 0.94709, 'close': 0.9473, 'time': '2026-09-30T11:00:00Z'}, {'open': 0.94729, 'high': 0.94796, 'low': 0.94676, 'close': 0.94736, 'time': '2026-09-30T12:00:00Z'}, {'open': 0.94732, 'high': 0.94774, 'low': 0.94675, 'close': 0.94747, 'time': '2026-09-30T13:00:00Z'}, {'open': 0.94746, 'high': 0.94862, 'low': 0.94712, 'close': 0.94814, 'time': '2026-09-30T14:00:00Z'}, {'open': 0.94814, 'high': 0.94841, 'low': 0.94703, 'close': 0.94703, 'time': '2026-09-30T15:00:00Z'}, {'open': 0.94704, 'high': 0.9472, 'low': 0.94662, 'close': 0.94697, 'time': '2026-09-30T16:00:00Z'}, {'open': 0.94698, 'high': 0.94706, 'low': 0.94674, 'close': 0.94689, 'time': '2026-09-30T17:00:00Z'}, {'open': 0.94691, 'high': 0.9471, 'low': 0.94663, 'close': 0.94668, 'time': '2026-09-30T18:00:00Z'}, {'open': 0.94669, 'high': 0.94689, 'low': 0.94634, 'close': 0.94642, 'time': '2026-09-30T19:00:00Z'}, {'open': 0.94642, 'high': 0.94699, 'low': 0.94642, 'close': 0.9469, 'time': '2026-09-30T20:00:00Z'}, {'open': 0.94693, 'high': 0.94713, 'low': 0.94558, 'close': 0.94658, 'time': '2026-09-30T21:00:00Z'}, {'open': 0.9468, 'high': 0.94686, 'low': 0.94663, 'close': 0.94666, 'time': '2026-09-30T22:00:00Z'}, {'open': 0.94666, 'high': 0.94671, 'low': 0.94655, 'close': 0.94656, 'time': '2026-09-30T23:00:00Z'}]}
SHORT_STRATEGIES["EUR_CHF"] = {
    "strategy_id": EURCHF_PRIMARY_ID, "strategy_name": "EURCHF_H1_SHORT_PRIMARY",
    "trigger_id": "H1_SHORT_L060_D0075_SC035_PC075",
    "signal_family": "eurchf_h1_short_primary", "tick_size": 0.00001,
    "price_precision": 5, "signal_id_prefix": "EURCHFH1SHORTPRIMARY",
    "atr_length": 14, "structure_lookback": 60, "maximum_distance_atr": 0.075,
    "structure_absolute_distance": True, "minimum_body_atr": 0.75,
    "minimum_signal_range_atr": 1.25, "maximum_close_location": 0.35,
    "minimum_previous_close_location": 0.75, "warmup_observed_bars": 200,
    "requires_daily_state": False, "reward_risk": 3.5, "stop_buffer_ticks": 10,
    "session_timezone": "UTC", "excluded_hours": set(), "excluded_weekdays": set(),
}


def eurchf_wilder_series(candles):
    """Exact research seed TR1..14, including no range-only TR0."""
    result = [None] * len(candles)
    if len(candles) <= 14:
        return result
    ranges = [None]
    for previous, current in zip(candles, candles[1:]):
        ranges.append(max(current["high"] - current["low"],
                          abs(current["high"] - previous["close"]),
                          abs(current["low"] - previous["close"])))
    result[14] = sum(ranges[1:15]) / 14
    for i in range(15, len(candles)):
        result[i] = (13 * result[i - 1] + ranges[i]) / 14
    return result


def eurchf_signal_at_index(h1, atr, index, observed_index=None):
    """Pure evaluator; full precision and prior high excluding signal."""
    seen_index = index if observed_index is None else observed_index
    if index < 60 or seen_index < 200:
        return None
    signal, previous, value = h1[index], h1[index - 1], atr[index]
    if value is None or not math.isfinite(value) or value <= 0:
        return None
    span, previous_span = signal["high"] - signal["low"], previous["high"] - previous["low"]
    prior = max(c["high"] for c in h1[index - 60:index])
    body = (signal["open"] - signal["close"]) / value
    range_atr = span / value
    distance = abs(signal["high"] - prior) / value
    close_location = (signal["close"] - signal["low"]) / span if span > 0 else None
    previous_location = ((previous["close"] - previous["low"]) / previous_span
                         if previous_span > 0 else None)
    engulf = (previous["close"] > previous["open"] and signal["close"] < signal["open"]
              and signal["open"] >= previous["close"] and signal["close"] <= previous["open"])
    gates = {"exact_bearish_engulf": engulf, "body_atr": body >= 0.75,
             "range_atr": range_atr >= 1.25, "absolute_prior60_distance": distance <= 0.075,
             "signal_close_location": close_location is not None and close_location <= 0.35,
             "previous_close_location": previous_location is not None and previous_location >= 0.75,
             "positive_candle_ranges": span > 0 and previous_span > 0}
    return {"qualified": all(gates.values()), "gates": gates, "side": "SELL",
            "strategy_id": EURCHF_PRIMARY_ID, "strategy_name": "EURCHF_H1_SHORT_PRIMARY",
            "trigger_id": SHORT_STRATEGIES["EUR_CHF"]["trigger_id"],
            "signal_start_utc": signal["time"],
            "signal_close_utc": signal["time"] + timedelta(hours=1),
            **{k: signal[k] for k in ("open", "high", "low", "close")},
            "atr": value, "body_atr": body, "signal_range_atr": range_atr,
            "previous_highest_high": prior, "structure_distance_atr": distance,
            "distance_from_recent_high_atr": distance, "close_location": close_location,
            "previous_close_location": previous_location, "reward_risk": 3.5}


@contextmanager
def eurchf_db():
    path = os.path.abspath(EURCHF_PASS14_DB_PATH)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    connection = sqlite3.connect(path, timeout=10)
    connection.row_factory = sqlite3.Row
    try:
        with connection:
            yield connection
    finally:
        connection.close()


def eurchf_initialize():
    global EURCHF_PASS14_LAST_ERROR
    with EURCHF_PASS14_LOCK:
        try:
            with eurchf_db() as connection:
                connection.execute("CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY,value TEXT NOT NULL)")
                connection.execute("CREATE TABLE IF NOT EXISTS observations (id INTEGER PRIMARY KEY,recorded_at TEXT NOT NULL,kind TEXT NOT NULL,signal_close TEXT NOT NULL,qualified INTEGER NOT NULL,prospective INTEGER NOT NULL,data_json TEXT NOT NULL,UNIQUE(kind,signal_close))")
                pinned = {"version": EURCHF_PASS14_VERSION, "strategy_id": EURCHF_PRIMARY_ID,
                          "anchor_source_sha256": EURCHF_FROZEN_ANCHOR["source_sha256"]}
                for key, value in pinned.items():
                    row = connection.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
                    if row is not None and row["value"] != value:
                        raise ValueError("EURCHF persistent state/version mismatch; preserve DB and use a new versioned path")
                    connection.execute("INSERT OR IGNORE INTO metadata VALUES (?,?)", (key, value))
                connection.execute("INSERT OR IGNORE INTO metadata VALUES ('observation_started_at_utc',?)", (iso_utc(utc_now()),))
                connection.execute("INSERT OR IGNORE INTO metadata VALUES ('state',?)", (json.dumps(EURCHF_FROZEN_ANCHOR, sort_keys=True),))
            EURCHF_PASS14_LAST_ERROR = None
            return True
        except Exception as error:
            EURCHF_PASS14_LAST_ERROR = str(error)
            return False


def eurchf_decode_bars(rows):
    return [{**r, "time": datetime.fromisoformat(r["time"].replace("Z", "+00:00"))} for r in rows]


def evaluate_latest_eurchf():
    """Advance pinned research ATR causally, with broker overlap verification."""
    with EURCHF_PASS14_LOCK:
        if not eurchf_initialize():
            raise RuntimeError(EURCHF_PASS14_LAST_ERROR)
        with eurchf_db() as connection:
            metadata = dict(connection.execute("SELECT key,value FROM metadata").fetchall())
        state = json.loads(metadata["state"])
        if (state.get("source_sha256") != EURCHF_FROZEN_ANCHOR["source_sha256"]
                or state.get("observed_count", 0) < EURCHF_FROZEN_ANCHOR["observed_count"]
                or len(state.get("bars", [])) != 61
                or not math.isfinite(state["atr14"]) or state["atr14"] <= 0):
            raise ValueError("Invalid/corrupt EURCHF persisted anchor state; preserve evidence and investigate")
        bars = eurchf_decode_bars(state["bars"])
        cursor, end = bars[-1]["time"], utc_now()
        if cursor + timedelta(hours=1) > end:
            raise ValueError("Frozen EURCHF anchor is not yet completed at observation time")
        fetched = []
        while cursor < end:
            chunk_end = min(cursor + timedelta(days=7), end)
            fetched.extend(fetch_candles_range("EUR_CHF", "H1", cursor, chunk_end))
            cursor = chunk_end
        # Duplicate times must have equal OHLC. No silently overwritten data.
        unique = {}
        for candle in fetched:
            values = [candle[k] for k in ("open", "high", "low", "close")]
            if (candle["time"].tzinfo is None or candle["time"].minute or candle["time"].second
                    or candle["time"].microsecond or candle["time"] + timedelta(hours=1) > end
                    or not all(math.isfinite(v) and v > 0 for v in values)
                    or not candle["low"] <= min(candle["open"], candle["close"])
                    or not max(candle["open"], candle["close"]) <= candle["high"]):
                raise ValueError("Invalid/forming EURCHF candle in state update")
            key = candle["time"]
            if key in unique and any(unique[key][k] != candle[k] for k in ("open", "high", "low", "close")):
                raise ValueError("Conflicting duplicate EURCHF candles")
            unique[key] = candle
        anchor = unique.get(bars[-1]["time"])
        if anchor is None or any(anchor[k] != bars[-1][k] for k in ("open", "high", "low", "close")):
            raise ValueError("EURCHF broker overlap differs from pinned state; no automatic reseeding")
        value = state["atr14"]
        count = state["observed_count"]
        previous_atr = state["previous_atr14"]
        for candle in sorted(unique.values(), key=lambda c: c["time"]):
            if candle["time"] <= bars[-1]["time"]:
                continue
            previous = bars[-1]
            tr = max(candle["high"] - candle["low"], abs(candle["high"] - previous["close"]),
                     abs(candle["low"] - previous["close"]))
            previous_atr, value = value, (13 * value + tr) / 14
            bars.append(candle)
            bars = bars[-61:]
            count += 1
        state.update(atr14=value, previous_atr14=previous_atr, observed_count=count,
                     bars=[{**{k: c[k] for k in ("open", "high", "low", "close")},
                            "time": iso_utc(c["time"])} for c in bars])
        result = eurchf_signal_at_index(bars, [None] * (len(bars) - 1) + [value], len(bars) - 1,
                                       observed_index=count - 1)
        if result is None:
            raise ValueError("EURCHF state has insufficient signal history")
        with eurchf_db() as connection:
            connection.execute("UPDATE metadata SET value=? WHERE key='state'", (json.dumps(state, sort_keys=True),))
        start = datetime.fromisoformat(metadata["observation_started_at_utc"].replace("Z", "+00:00"))
        result.update(observation_started_at_utc=metadata["observation_started_at_utc"],
                      prospective=result["signal_close_utc"] >= start,
                      source_sha256=state["source_sha256"], state_observed_count=count,
                      atr_seed="PINNED_FULL_HISTORY_TR1_TO14_THEN_CAUSAL_RECURRENCE",
                      pass14_version=EURCHF_PASS14_VERSION)
        return result


def eurchf_post_preview(payload):
    """POST only to the observation endpoint, with no live webhook fallback."""
    root = EXECUTOR_WEBHOOK_URL.rsplit("/webhook", 1)[0]
    response = requests.post(root + "/eurchf-execution-preview", json=payload, timeout=20)
    try:
        body = response.json()
    except Exception:
        body = {"status": "preview_response_invalid", "reason": "Executor returned non-JSON"}
    if body.get("order_submitted") is not False or body.get("read_only") is not True:
        raise RuntimeError("Executor response does not verify read-only preview; update executor first")
    return {"http_status": response.status_code, "response": body}


def process_eurchf_live_short(expected_close_utc=None, force=False, allow_submission=True):
    """Hourly primary lifecycle; observation is separate from optional activation."""
    with EURCHF_PASS14_LOCK:
        result = evaluate_latest_eurchf()
        close = result["signal_close_utc"]
        if expected_close_utc is not None and close < expected_close_utc:
            return {"instrument": "EUR_CHF", "ready": False, "latest_completed_close_utc": iso_utc(close)}
        kind = "MANUAL_READ_ONLY" if force or not allow_submission else "WATCHER"
        with eurchf_db() as connection:
            row = connection.execute("SELECT data_json FROM observations WHERE kind=? AND signal_close=?",
                                     (kind, iso_utc(close))).fetchone()
        if row is not None:
            return {**json.loads(row["data_json"]), "already_recorded": True}
        age = (utc_now() - close).total_seconds()
        config = SHORT_STRATEGIES["EUR_CHF"]
        stop = round_price(result["high"] + 10 * config["tick_size"], config)
        entry = round_price(result["close"], config)
        out = {**result, "signal_start_utc": iso_utc(result["signal_start_utc"]),
               "signal_close_utc": iso_utc(close), "ready": True, "instrument": "EUR_CHF",
               "signal_id": short_signal_id_for("EUR_CHF", close), "entry": entry,
               "stop": stop, "target": round_price(entry - 3.5 * (stop - entry), config),
               "close_age_seconds": age, "order_submitted": False,
               "hypothetical_execution": True, "status": "NO_SIGNAL", "kind": kind}
        if result["qualified"]:
            if not result["prospective"] or age < -2 or age > MAX_LIVE_SIGNAL_AGE_SECONDS:
                out["status"] = "RETROSPECTIVE_OR_STALE_SIGNAL"
            else:
                payload = build_short_live_payload("EUR_CHF", result)
                if (EURCHF_H1_SHORT_PRIMARY_LIVE_ENABLED and SHORT_LIVE_SUBMISSION_ENABLED
                        and allow_submission and not force):
                    submitted = post_to_executor(payload)
                    response = submitted.get("response")
                    out.update(status="LIVE_SUBMISSION_RESULT", executor=submitted,
                               hypothetical_execution=False,
                               order_submitted=(response.get("order_submitted")
                                                if isinstance(response, dict) else None),
                               actual_fill=(response.get("actual_fill")
                                            if isinstance(response, dict) else None))
                elif EURCHF_PASS14_PREVIEW_ENABLED:
                    try:
                        out["executor_preview"] = eurchf_post_preview(payload)
                        out["status"] = out["executor_preview"]["response"].get("status", "PREVIEW_UNKNOWN")
                    except Exception as error:
                        out.update(status="PREVIEW_ERROR", error=str(error))
                else:
                    out["status"] = "QUALIFIED_PREVIEW_DISABLED"
        with eurchf_db() as connection:
            connection.execute("INSERT INTO observations(recorded_at,kind,signal_close,qualified,prospective,data_json) VALUES (?,?,?,?,?,?)",
                               (iso_utc(utc_now()), kind, iso_utc(close), int(result["qualified"]),
                                int(result["prospective"]), json.dumps(out, sort_keys=True, allow_nan=False)))
        add_live_event(out["status"], "EUR_CHF_SHORT", signal_id=out["signal_id"],
                       strategy_id=EURCHF_PRIMARY_ID, signal_close_utc=iso_utc(close),
                       qualified=result["qualified"], order_submitted=out["order_submitted"])
        return out


@app.route("/eurchf-pass14/status")
def eurchf_pass14_status():
    try:
        if not eurchf_initialize():
            raise RuntimeError(EURCHF_PASS14_LAST_ERROR)
        with eurchf_db() as connection:
            metadata = dict(connection.execute("SELECT key,value FROM metadata WHERE key!='state'").fetchall())
            counts = [dict(r) for r in connection.execute("SELECT kind,prospective,qualified,COUNT(*) AS count FROM observations GROUP BY kind,prospective,qualified")]
        return jsonify({"status": "online", "read_only": True, "version": EURCHF_PASS14_VERSION,
                        "strategy_id": EURCHF_PRIMARY_ID, "metadata": metadata, "counts": counts,
                        "candidate_live_enabled": EURCHF_H1_SHORT_PRIMARY_LIVE_ENABLED,
                        "short_submission_enabled": SHORT_LIVE_SUBMISSION_ENABLED,
                        "preview_enabled": EURCHF_PASS14_PREVIEW_ENABLED,
                        "db_path": os.path.abspath(EURCHF_PASS14_DB_PATH),
                        "persistent_volume_path": os.path.abspath(EURCHF_PASS14_DB_PATH).startswith("/data/"),
                        "scope": "Signal parity and hypothetical execution; prospective profitability unproven"})
    except Exception as error:
        return jsonify({"status": "error", "read_only": True, "reason": str(error)}), 503


@app.route("/eurchf-pass14/check", methods=["POST"])
def eurchf_pass14_check():
    data = request.get_json(silent=True) or {}
    if not WEBHOOK_SECRET or data.get("secret") != WEBHOOK_SECRET:
        return jsonify({"status": "rejected", "reason": "Invalid webhook secret"}), 403
    try:
        return jsonify(process_eurchf_live_short(force=True, allow_submission=False))
    except Exception as error:
        return jsonify({"status": "error", "read_only": True, "order_submitted": False,
                        "reason": str(error)}), 503


@app.route("/eurchf-pass14/observations")
def eurchf_pass14_observations():
    try:
        if not WEBHOOK_SECRET or request.headers.get("X-Webhook-Secret") != WEBHOOK_SECRET:
            return jsonify({"status": "rejected", "reason": "Invalid webhook secret"}), 403
        after = max(0, int(request.args.get("after_id", 0)))
        limit = min(1000, max(1, int(request.args.get("limit", 200))))
        with eurchf_db() as connection:
            rows = connection.execute("SELECT * FROM observations WHERE id>? ORDER BY id LIMIT ?", (after, limit)).fetchall()
        return jsonify({"read_only": True, "version": EURCHF_PASS14_VERSION,
                        "observations": [{"id": r["id"], "recorded_at": r["recorded_at"],
                                          **json.loads(r["data_json"])} for r in rows],
                        "next_after_id": rows[-1]["id"] if rows else after})
    except Exception as error:
        return jsonify({"status": "error", "read_only": True, "reason": str(error)}), 503


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


def _signal_audit_connect(read_only=False):
    path = os.path.abspath(SIGNAL_AUDIT_DB_PATH)
    parent = os.path.dirname(path)
    if parent and not read_only:
        os.makedirs(parent, exist_ok=True)
    if read_only:
        connection = sqlite3.connect(
            "file:" + path + "?mode=ro",
            uri=True,
            timeout=5.0,
        )
    else:
        connection = sqlite3.connect(path, timeout=5.0)
    connection.row_factory = sqlite3.Row
    return connection


def _signal_audit_init():
    global SIGNAL_AUDIT_LAST_ERROR
    with SIGNAL_AUDIT_LOCK:
        try:
            connection = _signal_audit_connect(False)
            try:
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute("PRAGMA synchronous=NORMAL")
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS signal_audit_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        recorded_at_utc TEXT NOT NULL,
                        event TEXT NOT NULL,
                        instrument TEXT,
                        pair TEXT,
                        strategy_id TEXT,
                        trigger_id TEXT,
                        timeframe TEXT,
                        side TEXT,
                        signal_id TEXT,
                        signal_start_utc TEXT,
                        signal_close_utc TEXT,
                        entry REAL,
                        stop REAL,
                        rr REAL,
                        close_age_seconds REAL,
                        executor_http_ok INTEGER,
                        executor_status_code INTEGER,
                        executor_status TEXT,
                        executor_order_submitted INTEGER,
                        executor_reason TEXT,
                        executor_response_json TEXT,
                        error TEXT
                    )
                    """
                )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS idx_signal_audit_signal_id "
                    "ON signal_audit_events(signal_id)"
                )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS idx_signal_audit_recorded "
                    "ON signal_audit_events(recorded_at_utc)"
                )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS idx_signal_audit_strategy "
                    "ON signal_audit_events(strategy_id)"
                )
                connection.commit()
                SIGNAL_AUDIT_LAST_ERROR = None
            finally:
                connection.close()
            return True
        except Exception as error:
            SIGNAL_AUDIT_LAST_ERROR = str(error)
            return False


def _executor_audit_fields(executor_response):
    if not isinstance(executor_response, dict):
        return {
            "executor_status": None,
            "executor_order_submitted": None,
            "executor_reason": None,
        }
    if executor_response.get("reason") == "Duplicate signal" and isinstance(
            executor_response.get("original_signal"), dict):
        executor_response = {**executor_response, **executor_response["original_signal"]}
    reason = (
        executor_response.get("reason")
        or executor_response.get("message")
        or executor_response.get("error")
    )
    return {
        "executor_status": executor_response.get("status"),
        "executor_order_submitted": executor_response.get("order_submitted"),
        "executor_reason": reason,
    }


def persist_signal_audit_event(
    event_type,
    instrument,
    data,
    executor_response=None,
):
    """Best-effort append-only telemetry. Never raises into trading logic."""
    global SIGNAL_AUDIT_LAST_ERROR
    try:
        # Only qualifying-signal lifecycle events belong here. NO_SIGNAL checks
        # remain in the existing in-memory /live-events buffer.
        signal_id = data.get("signal_id")
        if not signal_id:
            return False
        if not _signal_audit_init():
            return False

        pair = data.get("pair")
        if not pair:
            candidate = str(instrument or "")
            if candidate.endswith("_SHORT"):
                candidate = candidate[:-6]
            if candidate.endswith("_M15"):
                candidate = candidate[:-4]
            pair = candidate or None

        ex = _executor_audit_fields(executor_response)
        response_json = None
        if executor_response is not None:
            try:
                response_json = json.dumps(
                    executor_response,
                    separators=(",", ":"),
                    sort_keys=True,
                    default=str,
                )[:12000]
            except Exception:
                response_json = str(executor_response)[:12000]

        values = (
            data.get("recorded_at_utc") or iso_utc(utc_now()),
            event_type,
            instrument,
            pair,
            data.get("strategy_id") or data.get("strategy"),
            data.get("trigger_id"),
            data.get("timeframe"),
            data.get("side"),
            signal_id,
            data.get("signal_start_utc"),
            data.get("signal_close_utc"),
            data.get("entry"),
            data.get("stop"),
            data.get("rr"),
            data.get("close_age_seconds"),
            data.get("executor_http_ok"),
            data.get("executor_status_code"),
            ex["executor_status"] or data.get("executor_status"),
            (
                int(bool(ex["executor_order_submitted"]))
                if ex["executor_order_submitted"] is not None
                else data.get("executor_order_submitted")
            ),
            ex["executor_reason"] or data.get("executor_reason"),
            response_json,
            data.get("error") or data.get("executor_error"),
        )
        with SIGNAL_AUDIT_LOCK:
            connection = _signal_audit_connect(False)
            try:
                connection.execute(
                    """
                    INSERT INTO signal_audit_events (
                        recorded_at_utc,event,instrument,pair,strategy_id,
                        trigger_id,timeframe,side,signal_id,signal_start_utc,
                        signal_close_utc,entry,stop,rr,close_age_seconds,
                        executor_http_ok,executor_status_code,executor_status,
                        executor_order_submitted,executor_reason,
                        executor_response_json,error
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    values,
                )
                connection.commit()
                SIGNAL_AUDIT_LAST_ERROR = None
            finally:
                connection.close()
        return True
    except Exception as error:
        SIGNAL_AUDIT_LAST_ERROR = str(error)
        return False


def add_live_event(
    event_type,
    instrument=None,
    **extra
):

    audit_persist = extra.pop("_audit_persist", True)
    audit_executor_response = extra.pop("_audit_executor_response", None)

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

    if (
        audit_persist
        and event.get("signal_id")
        and "NO_SIGNAL" not in event_type
    ):
        persist_signal_audit_event(
            event_type,
            instrument,
            event,
            audit_executor_response,
        )

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

    # Optional independent daily-close EMA regime for long
    # strategies. Existing longs leave this unset.
    daily_close_ema_length = config.get(
        "daily_close_ema"
    )

    if daily_close_ema_length is not None:

        daily_close_ema = ema_series(
            closes,
            daily_close_ema_length
        )

    else:

        daily_close_ema = [
            None
        ] * len(
            closes
        )

    # Daily volatility regime support for long strategies.
    # Existing longs ignore this unless
    # minimum_daily_atr_ratio_50 is configured.
    daily_atr = atr_series(
        daily,
        14
    )

    daily_atr_sma50 = [
        None
    ] * len(
        daily_atr
    )

    rolling_sum = 0.0
    rolling_values = []

    for index, value in enumerate(
        daily_atr
    ):

        if value is None:
            rolling_values.append(
                None
            )
            continue

        rolling_values.append(
            value
        )
        rolling_sum += value

        if len(
            rolling_values
        ) > 50:

            removed = (
                rolling_values[
                    -51
                ]
            )

            if removed is not None:
                rolling_sum -= removed

        window = (
            rolling_values[
                -50:
            ]
        )

        if (
            len(
                window
            ) == 50
            and
            all(
                item is not None
                for item in window
            )
        ):

            daily_atr_sma50[
                index
            ] = (
                rolling_sum
                / 50.0
            )

    result = []

    for index, candle in enumerate(
        daily
    ):

        daily_atr_ratio_50 = None

        # Match the short/research warm-up convention:
        # ATR14 seed + 50 valid ATR observations.
        if (
            index >= 63
            and
            daily_atr[
                index
            ] is not None
            and
            daily_atr_sma50[
                index
            ] is not None
            and
            daily_atr_sma50[
                index
            ] > 0
        ):

            daily_atr_ratio_50 = (
                daily_atr[
                    index
                ]
                / daily_atr_sma50[
                    index
                ]
            )

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
                ],

            "daily_close_ema":
                daily_close_ema[
                    index
                ],

            "daily_atr":
                daily_atr[
                    index
                ],

            "daily_atr_ratio_50":
                daily_atr_ratio_50
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

    require_daily_close_ema = (
        config.get(
            "daily_close_ema"
        )
        is not None
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

        daily_close_ema_ready = (
            not require_daily_close_ema
            or
            row[
                "daily_close_ema"
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
            and
            daily_close_ema_ready
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

    mode = (
        config[
            "session_mode"
        ]
    )

    if mode == "all":

        return True

    if mode == "asia_union":

        tokyo_time = signal_time.astimezone(
            ZoneInfo("Asia/Tokyo")
        )

        sydney_time = signal_time.astimezone(
            ZoneInfo("Australia/Sydney")
        )

        tokyo_allowed = (
            8 <= tokyo_time.hour < 17
        )

        sydney_allowed = (
            8 <= sydney_time.hour < 17
        )

        return (
            tokyo_allowed
            or sydney_allowed
        )

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

    prior_movement_lookback = config.get(
        "prior_movement_lookback_bars"
    )

    minimum_index = max(

        config[
            "atr_length"
        ],

        config[
            "structure_lookback"
        ],

        (
            prior_movement_lookback + 1
            if prior_movement_lookback is not None
            else 0
        )
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

    body_atr = (

        current_body
        / current_atr

        if current_atr > 0

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

    signal_family = config.get(
        "signal_family",
        "bullish_engulfing"
    )

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

    bullish_outside_reversal = (

        signal[
            "close"
        ]
        > signal[
            "open"
        ]

        and

        signal[
            "high"
        ]
        > previous[
            "high"
        ]

        and

        signal[
            "low"
        ]
        < previous[
            "low"
        ]
    )

    if signal_family == "outside_reversal":

        primary_pattern_allowed = (
            bullish_outside_reversal
        )

    elif signal_family == "bullish_engulfing":

        primary_pattern_allowed = (
            bullish_engulfing
        )

    else:

        raise ValueError(
            f"Unknown long signal family: "
            f"{signal_family}"
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
    # MINIMUM BODY / ATR
    # ==============================================

    minimum_body_atr = config.get(
        "minimum_body_atr"
    )

    minimum_body_atr_allowed = (

        True

        if minimum_body_atr is None

        else (
            body_atr is not None
            and
            body_atr >= minimum_body_atr
        )
    )

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

    raw_distance_from_recent_low = (
        signal["low"]
        - previous_lowest_low
    )

    distance_from_recent_low = (
        abs(raw_distance_from_recent_low)
        if config.get(
            "structure_absolute_distance",
            False
        )
        else raw_distance_from_recent_low
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
    # PRIOR H1 MOVEMENT (optional)
    # ==============================================

    maximum_prior_movement_atr = config.get(
        "maximum_prior_movement_atr"
    )

    if prior_movement_lookback is None:

        prior_movement_atr = None
        prior_movement_allowed = True

    else:

        prior_close = h1[index - 1]["close"]
        earlier_close = h1[
            index - 1 - prior_movement_lookback
        ]["close"]

        prior_movement_atr = (
            (prior_close - earlier_close)
            / current_atr
        )

        prior_movement_allowed = (
            maximum_prior_movement_atr is not None
            and
            prior_movement_atr
            <= maximum_prior_movement_atr
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

    daily_close_ema_length = config.get(
        "daily_close_ema"
    )

    daily_close_ema_value = daily.get(
        "daily_close_ema"
    )

    daily_close_ema_allowed = (

        True

        if daily_close_ema_length is None

        else (
            daily_close_ema_value is not None
            and
            daily["close"] > daily_close_ema_value
        )
    )

    minimum_daily_atr_ratio_50 = (
        config.get(
            "minimum_daily_atr_ratio_50"
        )
    )

    daily_atr_ratio_50 = (
        daily.get(
            "daily_atr_ratio_50"
        )
    )

    daily_atr_ratio_allowed = (

        True

        if minimum_daily_atr_ratio_50 is None

        else (
            daily_atr_ratio_50 is not None
            and
            daily_atr_ratio_50
            >= minimum_daily_atr_ratio_50
        )
    )

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

        primary_pattern_allowed,
        strong_close_allowed,
        lower_wick_allowed,
        minimum_range_allowed,
        minimum_body_atr_allowed,
        structure_allowed,
        prior_movement_allowed,
        daily_regime_allowed,
        daily_alignment_allowed,
        daily_close_ema_allowed,
        daily_atr_ratio_allowed,
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

        "signal_family":
            signal_family,

        "outside_reversal":
            bullish_outside_reversal,

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

        "body_atr":
            body_atr,

        "prior_movement_atr":
            prior_movement_atr,

        "prior_movement_lookback_bars":
            prior_movement_lookback,

        "daily_atr_ratio_50":
            daily_atr_ratio_50,

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
            ],

        "previous_daily_close_ema":
            daily.get(
                "daily_close_ema"
            )
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

    if instrument == "EUR_CHF":
        return eurchf_signal_at_index(h1, atr, index)

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

    # ==================================================
    # AUD/USD H1 SHORT — FINAL TWO-TRIGGER FAMILY (#26)
    # ==================================================
    # Trigger A has same-candle priority over Trigger B. Both triggers share
    # one strategy_id in the executor, so strategy-level pyramiding=0 applies
    # across the combined system exactly as validated historically.
    if config.get("signal_family") == "audusd_two_trigger":

        compression_lookback = int(
            config["compression_mean_lookback"]
        )
        breakdown_lookback = int(
            config["breakdown_lookback"]
        )
        sweep_lookback = int(
            config["sweep_lookback"]
        )
        prior_momentum_lookback = int(
            config["prior_momentum_lookback_bars"]
        )

        warmup = max(
            config["atr_length"] + compression_lookback,
            breakdown_lookback,
            sweep_lookback,
            prior_momentum_lookback + 1,
        )

        if index < warmup:
            return None

        previous_atr = atr[index - 1]
        atr_window = atr[
            index - compression_lookback:
            index
        ]

        if (
            previous_atr is None
            or len(atr_window) != compression_lookback
            or any(value is None for value in atr_window)
        ):
            return None

        atr_mean_20 = (
            sum(atr_window)
            / float(compression_lookback)
        )

        if atr_mean_20 <= 0 or current_atr <= 0:
            return None

        previous_atr_ratio_20 = (
            previous_atr
            / atr_mean_20
        )

        signal_range = (
            signal["high"]
            - signal["low"]
        )
        bearish_body = (
            signal["open"]
            - signal["close"]
        )
        body_atr = (
            bearish_body
            / current_atr
        )
        signal_range_atr = (
            signal_range
            / current_atr
        )

        breakdown_bars = h1[
            index - breakdown_lookback:
            index
        ]
        previous_lowest_low = min(
            candle["low"]
            for candle in breakdown_bars
        )

        sweep_bars = h1[
            index - sweep_lookback:
            index
        ]
        previous_highest_high = max(
            candle["high"]
            for candle in sweep_bars
        )

        upper_wick = (
            signal["high"]
            - max(
                signal["open"],
                signal["close"],
            )
        )
        upper_wick_body_ratio = (
            upper_wick / bearish_body
            if bearish_body > 0
            else None
        )

        # Exact research definition: prior ~4H rally uses only completed H1
        # closes, normalized by the CURRENT signal ATR14.
        prior_upward_momentum_atr = (
            h1[index - 1]["close"]
            - h1[
                index
                - prior_momentum_lookback
                - 1
            ]["close"]
        ) / current_atr

        bearish = (
            signal["close"]
            < signal["open"]
        )

        core_qualified = all([
            bearish,
            body_atr
            >= config["minimum_body_atr"],
            signal_range_atr
            >= config["minimum_signal_range_atr"],
            previous_atr_ratio_20
            <= config["maximum_previous_atr_ratio_20"],
            signal["close"]
            < previous_lowest_low,
        ])

        # Use boolean short-circuiting here rather than all([...]).
        # On a doji/non-bearish candle bearish_body can be <= 0, which makes
        # upper_wick_body_ratio None. all([...]) eagerly evaluates every list
        # element, so the old form could still execute None >= float during
        # the startup/no-signal check even though "bearish" was already false.
        sweep_qualified = (
            bearish
            and body_atr
            >= config["minimum_body_atr"]
            and signal["high"]
            > previous_highest_high
            and signal["close"]
            < previous["low"]
            and upper_wick_body_ratio is not None
            and upper_wick_body_ratio
            >= config["minimum_upper_wick_body_ratio"]
            and prior_upward_momentum_atr
            >= config["minimum_prior_upward_momentum_atr"]
        )

        # Frozen same-candle priority: Trigger A before Trigger B.
        trigger_id = None
        if core_qualified:
            trigger_id = "COMPRESSION_BREAKOUT"
        elif sweep_qualified:
            trigger_id = "SWEEP_DISPLACEMENT"

        close_location = (
            (signal["close"] - signal["low"])
            / signal_range
            if signal_range > 0
            else 1.0
        )

        local_time = signal["time"].astimezone(
            timezone.utc
        )

        return {
            "qualified": bool(trigger_id),
            "side": "SELL",
            "strategy_name": config["strategy_name"],
            "strategy_id": "AUD_USD_H1_SHORT",
            "trigger_id": trigger_id,
            "signal_family": "audusd_two_trigger",
            "core_qualified": core_qualified,
            "sweep_qualified": sweep_qualified,
            "signal_start_utc": signal["time"],
            "signal_close_utc": signal["time"] + timedelta(hours=1),
            "open": signal["open"],
            "high": signal["high"],
            "low": signal["low"],
            "close": signal["close"],
            "atr": current_atr,
            "body_atr": body_atr,
            "signal_range_atr": signal_range_atr,
            "previous_atr_ratio_20": previous_atr_ratio_20,
            "previous_lowest_low": previous_lowest_low,
            "previous_highest_high": previous_highest_high,
            "upper_wick_body_ratio": upper_wick_body_ratio,
            "prior_upward_momentum_atr": prior_upward_momentum_atr,
            "breakdown_lookback": breakdown_lookback,
            "sweep_lookback": sweep_lookback,

            # Compatibility diagnostics used by the existing read-only route.
            "body_ratio": None,
            "close_location": close_location,
            "distance_from_recent_high_atr": (
                (previous_highest_high - signal["high"])
                / current_atr
            ),
            "previous_daily_close": None,
            "previous_daily_fast_ema": None,
            "previous_daily_slow_ema": None,
            "daily_ema_separation_atr": None,
            "local_timezone": "UTC",
            "local_hour": local_time.hour,
            "weekday": local_time.strftime("%A"),
        }

    # ==================================================
    # EUR/JPY H1 SHORT — COMPRESSION BREAKDOWN FAMILY
    # ==================================================
    # This is intentionally separate from the existing bearish-engulfing
    # short family so strategy #22 reproduces the validated raw geometry
    # exactly instead of inheriting irrelevant engulf/structure rules.
    if config.get("signal_family") == "compression_breakdown":

        compression_lookback = int(
            config["compression_mean_lookback"]
        )

        if index < compression_lookback + config["atr_length"]:
            return None

        previous_atr = atr[index - 1]
        atr_window = atr[index - compression_lookback:index]

        if (
            previous_atr is None
            or len(atr_window) != compression_lookback
            or any(value is None for value in atr_window)
        ):
            return None

        atr_mean_20 = sum(atr_window) / float(compression_lookback)

        if atr_mean_20 <= 0:
            return None

        previous_atr_ratio_20 = previous_atr / atr_mean_20

        signal_range = signal["high"] - signal["low"]
        bearish_body = signal["open"] - signal["close"]
        body_atr = (
            bearish_body / current_atr
            if current_atr > 0
            else None
        )
        signal_range_atr = (
            signal_range / current_atr
            if current_atr > 0
            else None
        )

        breakdown_lookback = int(
            config["breakdown_lookback"]
        )
        previous_bars = h1[index - breakdown_lookback:index]
        previous_lowest_low = min(
            candle["low"] for candle in previous_bars
        )

        local_time = signal["time"].astimezone(
            ZoneInfo(config["session_timezone"])
        )

        session_allowed = (
            local_time.hour not in config["excluded_hours"]
        )
        weekday_allowed = (
            local_time.weekday() not in config["excluded_weekdays"]
        )

        qualified = all([
            signal["close"] < signal["open"],
            body_atr is not None
            and body_atr >= config["minimum_body_atr"],
            signal_range_atr is not None
            and signal_range_atr >= config["minimum_signal_range_atr"],
            previous_atr_ratio_20
            <= config["maximum_previous_atr_ratio_20"],
            signal["close"] < previous_lowest_low,
            session_allowed,
            weekday_allowed,
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
            "body_atr": body_atr,
            "signal_range_atr": signal_range_atr,
            "previous_atr_ratio_20": previous_atr_ratio_20,
            "previous_lowest_low": previous_lowest_low,
            "breakdown_lookback": breakdown_lookback,
            "local_timezone": config["session_timezone"],
            "local_hour": local_time.hour,
            "weekday": local_time.strftime("%A"),
        }

    # ==================================================
    # AUD/JPY H1 SHORT PRIMARY RR3.25 — strategy #31
    # ==================================================
    # Dedicated branch preserves the independently confirmed research semantics:
    # exact bearish engulf; ABS distance to previous 175-bar high <=0.10 ATR14;
    # bearish body>=0.60 ATR14; range>=1.25 ATR14; previous candle close
    # location>=0.50. No D1/session/weekday filters.
    if config.get("signal_family") == "audjpy_h1_short_primary":
        lookback = int(config["structure_lookback"])
        if index < max(config["atr_length"], lookback):
            return None

        signal_range = signal["high"] - signal["low"]
        bearish_body = signal["open"] - signal["close"]
        body_atr = bearish_body / current_atr if current_atr > 0 else None
        signal_range_atr = signal_range / current_atr if current_atr > 0 else None

        previous_range = previous["high"] - previous["low"]
        previous_close_location = (
            (previous["close"] - previous["low"]) / previous_range
            if previous_range > 0 else None
        )

        previous_highest_high = max(
            candle["high"] for candle in h1[index - lookback:index]
        )
        structure_distance_atr = (
            abs(signal["high"] - previous_highest_high) / current_atr
            if current_atr > 0 else None
        )

        exact_bearish_engulf = (
            previous["close"] > previous["open"]
            and signal["close"] < signal["open"]
            and signal["open"] >= previous["close"]
            and signal["close"] <= previous["open"]
        )

        qualified = all([
            exact_bearish_engulf,
            body_atr is not None and body_atr >= config["minimum_body_atr"],
            signal_range_atr is not None and signal_range_atr >= config["minimum_signal_range_atr"],
            structure_distance_atr is not None and structure_distance_atr <= config["maximum_distance_atr"],
            previous_close_location is not None and previous_close_location >= config["minimum_previous_close_location"],
        ])

        return {
            "qualified": qualified,
            "side": "SELL",
            "strategy_name": config["strategy_name"],
            "strategy_id": config["strategy_id"],
            "trigger_id": config["trigger_id"],
            "signal_start_utc": signal["time"],
            "signal_close_utc": signal["time"] + timedelta(hours=1),
            "open": signal["open"],
            "high": signal["high"],
            "low": signal["low"],
            "close": signal["close"],
            "atr": current_atr,
            "body_atr": body_atr,
            "signal_range_atr": signal_range_atr,
            "previous_close_location": previous_close_location,
            "previous_highest_high": previous_highest_high,
            "structure_distance_atr": structure_distance_atr,
        }

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

    if instrument == "EUR_CHF":
        return evaluate_latest_eurchf()

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

    requires_daily_state = config.get(
        "requires_daily_state",
        True
    )

    daily = (
        fetch_candles_count(
            instrument,
            "D",
            2500
        )
        if requires_daily_state
        else []
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
        if requires_daily_state
        else []
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

        "strategy_id":
            config.get(
                "strategy_id",
                f"{instrument}_H1_LONG"
            ),

        "timeframe":
            "H1",

        "trigger_id":
            config.get(
                "trigger_id",
                "H1_LONG"
            ),

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

        "strategy_id":
            config.get(
                "strategy_id",
                f"{instrument}_H1_SHORT"
            ),

        "timeframe":
            "H1",

        "trigger_id":
            signal_result.get(
                "trigger_id",
                "H1_SHORT"
            ),

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

SUBMISSION_REPORTING_VERSION = "PROBE_SUBMISSION_REPORTING_V2_2026_10_14"


def executor_failure_audit_fields(result):
    body = result.get("response")
    ex = _executor_audit_fields(body)
    return {
        "executor_http_ok": False,
        "executor_status_code": result.get("status_code"),
        "executor_latency_ms": result.get("latency_ms"),
        "executor_attempt": result.get("attempt"),
        "executor_status": ex["executor_status"],
        "executor_order_submitted": ex["executor_order_submitted"],
        "executor_reason": ex["executor_reason"],
        "_audit_executor_response": body,
    }


def _persist_executor_attempt(payload, result):
    if payload.get("pipeline_test") or str(payload.get("signal_id", "")).startswith("PIPELINE-TEST-"):
        return
    persist_signal_audit_event(
        "EXECUTOR_RESPONSE", payload.get("strategy_id") or payload.get("pair"),
        {"signal_id": payload.get("signal_id"), "pair": payload.get("pair"),
         "strategy_id": payload.get("strategy_id"), "timeframe": payload.get("timeframe"),
         "trigger_id": payload.get("trigger_id"), "side": payload.get("side"),
         "signal_close_utc": payload.get("timestamp"), "entry": payload.get("entry"),
         "stop": payload.get("stop"), "rr": payload.get("rr"),
         "executor_http_ok": int(bool(result.get("ok"))),
         "executor_status_code": result.get("status_code"),
         "error": result.get("error")}, result.get("response"))


def post_to_executor(payload):
    if not EXECUTOR_WEBHOOK_URL:
        raise RuntimeError("EXECUTOR_WEBHOOK_URL is not configured")
    attempts = []
    first_http_failure = None
    for attempt in range(1, 4):
        started = time.perf_counter()
        try:
            response = requests.post(EXECUTOR_WEBHOOK_URL, json=payload, timeout=15)
            try:
                body = response.json()
            except (ValueError, TypeError):
                body = response.text[:2000]
            result = {"ok": response.ok, "status_code": response.status_code,
                      "latency_ms": round((time.perf_counter()-started)*1000, 2),
                      "response": body, "attempt": attempt}
            if not response.ok:
                result["error"] = f"HTTP {response.status_code}: {response.text[:2000]}"
                if first_http_failure is None:
                    first_http_failure = dict(result)
            _persist_executor_attempt(payload, result)
            attempts.append(dict(result))
            if response.ok:
                return {**result, "attempts": attempts,
                        "first_http_failure": first_http_failure}
            # 400/403/409/etc are terminal. Retry only transport failures and
            # explicitly transient HTTP statuses with no processed order result.
            processed = isinstance(body, dict) and (
                body.get("order_submitted") or body.get("oanda_order_id") or
                body.get("oanda_trade_id") or body.get("status") == "order_failed")
            retry = (response.status_code in {408, 425, 429, 500, 502, 503, 504}
                     and not processed)
            if not retry:
                return {**result, "attempts": attempts,
                        "first_http_failure": first_http_failure}
        except requests.RequestException as error:
            result = {"ok": False, "error": str(error), "attempt": attempt,
                      "status_code": None, "response": None,
                      "latency_ms": round((time.perf_counter()-started)*1000, 2)}
            _persist_executor_attempt(payload, result)
            attempts.append(dict(result))
        if attempt < 3:
            time.sleep(0.6 * attempt)
    return {**result, "attempts": attempts,
            "first_http_failure": first_http_failure}

# ==================================================
# LIVE SIGNAL PROCESSING
# ==================================================

def long_live_enabled_for(
    instrument
):

    if instrument == "AUD_USD":

        return (
            AUDUSD_H1_LONG_LIVE_ENABLED
        )

    if instrument == "AUD_JPY":

        return (
            AUDJPY_H1_LONG_TIGHT_LIVE_ENABLED
        )

    return True


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
            ),

        "pair":
            instrument,

        "strategy_id":
            config.get(
                "strategy_id",
                f"{instrument}_H1_LONG"
            ),

        "trigger_id":
            config.get(
                "trigger_id",
                "H1_LONG"
            ),

        "timeframe":
            "H1",

        "side":
            "BUY",

        "_audit_persist":
            not force
    })

    if not force:
        persist_signal_audit_event(
            "QUALIFIED",
            instrument,
            {k: v for k, v in base.items() if k != "_audit_persist"},
        )

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
    # DRY RUN
    # ==============================================

    if (
        not LIVE_SUBMISSION_ENABLED
        or
        not allow_submission
        or
        not long_live_enabled_for(
            instrument
        )
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

            executor_http_ok=
                True,

            executor_status=
                (executor_result.get("response") or {}).get("status")
                if isinstance(executor_result.get("response"), dict)
                else None,

            executor_order_submitted=
                (executor_result.get("response") or {}).get("order_submitted")
                if isinstance(executor_result.get("response"), dict)
                else None,

            _audit_executor_response=
                executor_result.get("response"),

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

            **executor_failure_audit_fields(executor_result),

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

    if instrument == "EUR_CHF":
        return SHORT_LIVE_SUBMISSION_ENABLED and EURCHF_H1_SHORT_PRIMARY_LIVE_ENABLED

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

    if instrument == "EUR_JPY":
        return EURJPY_SHORT_LIVE_ENABLED

    if instrument == "AUD_USD":
        return AUDUSD_H1_SHORT_LIVE_ENABLED

    if instrument == "AUD_JPY":
        return AUDJPY_H1_SHORT_PRIMARY_LIVE_ENABLED

    return True


def process_live_short(
    instrument,
    expected_close_utc=None,
    force=False,
    allow_submission=True
):

    if instrument == "EUR_CHF":
        return process_eurchf_live_short(expected_close_utc, force, allow_submission)

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

        "trigger_id":
            result.get(
                "trigger_id"
            ),

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
            ),

        "pair":
            instrument,

        "strategy_id":
            config.get(
                "strategy_id",
                f"{instrument}_H1_SHORT"
            ),

        "timeframe":
            "H1",

        "_audit_persist":
            not force
    })

    if not force:
        persist_signal_audit_event(
            "QUALIFIED",
            event_instrument,
            {k: v for k, v in base.items() if k != "_audit_persist"},
        )

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

            executor_http_ok=
                True,

            executor_status=
                (executor_result.get("response") or {}).get("status")
                if isinstance(executor_result.get("response"), dict)
                else None,

            executor_order_submitted=
                (executor_result.get("response") or {}).get("order_submitted")
                if isinstance(executor_result.get("response"), dict)
                else None,

            _audit_executor_response=
                executor_result.get("response"),

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

            **executor_failure_audit_fields(executor_result),

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


def _watcher_result_complete(result, expected_close_utc):
    """A readiness precheck cannot stand in for the full evaluator's result."""
    if not isinstance(result, dict) or result.get("ready") is not True:
        return False
    value = result.get("signal_close_utc")
    if value is None:
        # Qualified M15 returns one lifecycle event per signal, rather than a
        # top-level candle clock. Every returned signal must cover this target.
        children = result.get("results")
        if isinstance(children, list) and children:
            return all(
                isinstance(child, dict) and _watcher_result_complete(
                    {"ready": True, "signal_close_utc": child.get("signal_close_utc")},
                    expected_close_utc,
                ) for child in children
            )
        return False
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return False
    if not isinstance(value, datetime) or value.tzinfo is None:
        return False
    return value >= expected_close_utc

def _watcher_retry_detail(result):
    if not isinstance(result, dict):
        return {"reason": "INVALID_PROCESS_RESULT"}
    return {
        "reason": "FULL_EVALUATION_NOT_READY" if result.get("ready") is False else "PROCESS_RESULT_COMPLETION_UNVERIFIED",
        "latest_completed_close_utc": result.get(
            "latest_completed_close_utc", result.get("signal_close_utc")
        ),
    }

def _watcher_store_cycle(timeframe, expected_close_utc, required, pending, attempts, details):
    # Publish an immutable end-of-cycle snapshot. These are processing counts,
    # not signal, submission or fill counts.
    cycle = {
        "version": WATCHER_RETRY_FIX_VERSION,
        "expected_close_utc": iso_utc(expected_close_utc),
        "finished_at_utc": iso_utc(utc_now()),
        "required_evaluations": required,
        "completed_evaluations": required - len(pending),
        "all_evaluations_completed": not pending,
        "outstanding": [
            {"evaluation": key, "attempts": attempts.get(key, 0),
             **details.get(key, {"reason": "TARGET_CANDLE_NOT_AVAILABLE"})}
            for key in sorted(pending)
        ],
    }
    with LIVE_STATE_LOCK:
        WATCHER_LAST_CYCLE[timeframe] = cycle

def wait_for_new_h1_and_process(expected_close_utc):
    # Completion belongs to each side: LONG success never completes SHORT.
    pending = {(pair, "BUY") for pair in STRATEGIES}
    pending.update((pair, "SELL") for pair in SHORT_STRATEGIES)
    required = len(pending)
    attempts, details = {}, {}
    process_errors, retry_logged, available = set(), set(), set()
    deadline = time.monotonic() + LIVE_POLL_WINDOW_SECONDS

    def event_fields(key):
        pair, side = key
        config = (STRATEGIES if side == "BUY" else SHORT_STRATEGIES)[pair]
        return {
            "pair": pair, "side": side, "timeframe": "H1",
            "strategy_id": config.get(
                "strategy_id", f"{pair}_H1_{'LONG' if side == 'BUY' else 'SHORT'}"
            ),
            "expected_close_utc": iso_utc(expected_close_utc),
        }

    def handle_result(key, result):
        if _watcher_result_complete(result, expected_close_utc):
            pending.discard(key)
            return
        details[key] = _watcher_retry_detail(result)
        if key not in retry_logged:
            pair, side = key
            add_live_event(
                "H1_EVALUATION_NOT_READY_RETRYING",
                pair if side == "BUY" else f"{pair}_SHORT",
                **event_fields(key), **details[key], attempts=attempts[key],
            )
            retry_logged.add(key)

    def handle_error(key, error):
        # An exception can follow a live request. Keep it outstanding and log
        # the failure, but do not turn a polling retry into an order retry.
        process_errors.add(key)
        details[key] = {"reason": "PROCESS_ERROR_NO_AUTOMATIC_RETRY", "error": str(error)}
        pair, side = key
        add_live_event(
            "LIVE_PROCESS_ERROR", pair if side == "BUY" else f"{pair}_SHORT",
            **event_fields(key), **details[key], attempts=attempts[key],
        )

    while pending and time.monotonic() < deadline:
        eligible = pending - process_errors
        pairs = sorted({pair for pair, side in eligible})
        ready = []
        if pairs:
            with ThreadPoolExecutor(max_workers=len(pairs)) as pool:
                futures = {
                    pool.submit(fetch_latest_complete_h1, pair): pair for pair in pairs
                }
                for future in as_completed(futures):
                    pair = futures[future]
                    try:
                        candle = future.result()
                        if candle and candle["time"] + timedelta(hours=1) >= expected_close_utc:
                            ready.append(pair)
                            available.add(pair)
                    except Exception as error:
                        add_live_event(
                            "CANDLE_READY_CHECK_ERROR", pair,
                            expected_close_utc=iso_utc(expected_close_utc), error=str(error),
                        )

        long_keys = [(pair, "BUY") for pair in ready if (pair, "BUY") in eligible]
        if long_keys:
            with ThreadPoolExecutor(max_workers=len(long_keys)) as pool:
                futures = {}
                for key in long_keys:
                    attempts[key] = attempts.get(key, 0) + 1
                    futures[pool.submit(
                        process_live_instrument, key[0], expected_close_utc, False, True
                    )] = key
                for future in as_completed(futures):
                    key = futures[future]
                    try:
                        handle_result(key, future.result())
                    except Exception as error:
                        handle_error(key, error)

        # Keep the original LONG-then-SHORT submission ordering.
        for pair in SHORT_STRATEGIES:
            key = (pair, "SELL")
            if pair not in ready or key not in eligible:
                continue
            attempts[key] = attempts.get(key, 0) + 1
            try:
                handle_result(key, process_live_short(pair, expected_close_utc, False, True))
            except Exception as error:
                handle_error(key, error)

        if pending:
            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(LIVE_POLL_INTERVAL_SECONDS, remaining))

    for pair in sorted({pair for pair, side in pending if pair not in available}):
        add_live_event(
            "NEW_H1_NOT_AVAILABLE_WITHIN_WINDOW", pair,
            expected_close_utc=iso_utc(expected_close_utc),
            pending_sides=sorted(side for p, side in pending if p == pair),
        )
    for key in sorted(pending):
        pair, side = key
        add_live_event(
            "H1_EVALUATION_NOT_COMPLETED_WITHIN_WINDOW",
            pair if side == "BUY" else f"{pair}_SHORT",
            **event_fields(key), attempts=attempts.get(key, 0),
            **details.get(key, {"reason": "TARGET_CANDLE_NOT_AVAILABLE"}),
        )
    labels = {key: f"{key[0]}_{'LONG' if key[1] == 'BUY' else 'SHORT'}" for key in pending}
    _watcher_store_cycle(
        "H1", expected_close_utc, required, set(labels.values()),
        {labels[k]: attempts.get(k, 0) for k in pending},
        {labels[k]: details[k] for k in pending if k in details},
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

    if instrument.upper() == "EUR_CHF":
        raise ValueError("EURCHF historical rules/ledgers are frozen in Pass13; use Pass14 observation routes")

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

        "eurjpy_short_live_enabled":
            EURJPY_SHORT_LIVE_ENABLED,

        "audusd_h1_long_live_enabled":
            AUDUSD_H1_LONG_LIVE_ENABLED,

        "audusd_h1_short_live_enabled":
            AUDUSD_H1_SHORT_LIVE_ENABLED,

        "audusd_m15_short_live_enabled":
            AUDUSD_M15_SHORT_LIVE_ENABLED,

        "audjpy_h1_long_tight_live_enabled":
            AUDJPY_H1_LONG_TIGHT_LIVE_ENABLED,

        "audjpy_h1_short_primary_live_enabled":
            AUDJPY_H1_SHORT_PRIMARY_LIVE_ENABLED,

        "eurchf_h1_short_primary_live_enabled": EURCHF_H1_SHORT_PRIMARY_LIVE_ENABLED,
        "eurchf_pass14_preview_enabled": EURCHF_PASS14_PREVIEW_ENABLED,
        "eurchf_pass14_version": EURCHF_PASS14_VERSION,
        "prepared_portfolio_version": "PORTFOLIO32_2026_10_05_EURCHF_H1_SHORT_PRIMARY_RR3P50_V1",
        "current_live_portfolio_version": (
            "PORTFOLIO32_2026_10_05_EURCHF_H1_SHORT_PRIMARY_RR3P50_V1"
            if EURCHF_H1_SHORT_PRIMARY_LIVE_ENABLED and SHORT_LIVE_SUBMISSION_ENABLED
            else "PORTFOLIO31_2026_09_30_AUDJPY_H1_SHORT_PRIMARY_RR3P25_V1"),

        "h1_long_strategy_count":
            len(STRATEGIES),

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
    "/strategy-config/<instrument>"
)
def strategy_config(
    instrument
):
    instrument = (
        instrument.upper()
    )

    if instrument not in STRATEGIES:

        return jsonify({
            "status":
                "error",

            "instrument":
                instrument,

            "message":
                f"{instrument} has no long strategy"
        }), 400

    config = (
        STRATEGIES[
            instrument
        ]
    )

    return jsonify({
        "status":
            "success",

        "mode":
            "READ_ONLY",

        "instrument":
            instrument,

        "strategy_config": {
            "strategy_id":
                config.get(
                    "strategy_id",
                    f"{instrument}_H1_LONG"
                ),

            "trigger_id":
                config.get(
                    "trigger_id",
                    "H1_LONG"
                ),

            "signal_family":
                config.get(
                    "signal_family",
                    "bullish_engulfing"
                ),

            "structure_absolute_distance":
                config.get(
                    "structure_absolute_distance",
                    False
                ),

            "minimum_body_ratio":
                config.get(
                    "minimum_body_ratio"
                ),

            "minimum_body_atr":
                config.get(
                    "minimum_body_atr"
                ),

            "strong_close_enabled":
                config.get(
                    "strong_close_enabled"
                ),

            "minimum_close_location":
                config.get(
                    "minimum_close_location"
                ),

            "lower_wick_filter_enabled":
                config.get(
                    "lower_wick_filter_enabled"
                ),

            "minimum_lower_wick_body_ratio":
                config.get(
                    "minimum_lower_wick_body_ratio"
                ),

            "atr_length":
                config.get(
                    "atr_length"
                ),

            "structure_lookback":
                config.get(
                    "structure_lookback"
                ),

            "maximum_distance_atr":
                config.get(
                    "maximum_distance_atr"
                ),

            "minimum_range_enabled":
                config.get(
                    "minimum_range_enabled"
                ),

            "minimum_range_atr":
                config.get(
                    "minimum_range_atr"
                ),

            "fast_daily_ema":
                config.get(
                    "fast_daily_ema"
                ),

            "slow_daily_ema":
                config.get(
                    "slow_daily_ema"
                ),

            "daily_close_ema":
                config.get(
                    "daily_close_ema"
                ),

            "require_daily_close_above_slow":
                config.get(
                    "require_daily_close_above_slow"
                ),

            "require_daily_fast_above_slow":
                config.get(
                    "require_daily_fast_above_slow"
                ),

            "minimum_daily_atr_ratio_50":
                config.get(
                    "minimum_daily_atr_ratio_50"
                ),

            "prior_movement_lookback_bars":
                config.get(
                    "prior_movement_lookback_bars"
                ),

            "maximum_prior_movement_atr":
                config.get(
                    "maximum_prior_movement_atr"
                ),

            "session_timezone":
                config.get(
                    "session_timezone"
                ),

            "session_mode":
                config.get(
                    "session_mode"
                ),

            "session_start_hour":
                config.get(
                    "session_start_hour"
                ),

            "session_end_hour":
                config.get(
                    "session_end_hour"
                ),

            "excluded_weekdays":
                sorted(
                    config.get(
                        "excluded_weekdays",
                        set()
                    )
                ),

            "reward_risk":
                config.get(
                    "reward_risk"
                ),

            "stop_buffer_ticks":
                config.get(
                    "stop_buffer_ticks"
                )
        },

        "orders_sent":
            False
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
    "/live-pipeline-test/<instrument>/BUY"
)
def live_pipeline_test_long(
    instrument
):
    """
    READ-ONLY end-to-end executor validation.

    Uses the real H1 LONG payload builder and real executor webhook, but
    pipeline_test=True makes the executor stop after validation/sizing.
    No OANDA order is submitted.
    """
    instrument = instrument.upper()

    if instrument not in STRATEGIES:
        return jsonify({
            "status": "error",
            "read_only": True,
            "message":
                f"{instrument} has no long strategy"
        }), 400

    if not EXECUTOR_WEBHOOK_URL:
        return jsonify({
            "status": "error",
            "read_only": True,
            "message":
                "EXECUTOR_WEBHOOK_URL is not configured"
        }), 500

    try:
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

        live_ask = float(
            price_data["live_ask"]
        )

        config = STRATEGIES[
            instrument
        ]

        tick = config[
            "tick_size"
        ]

        synthetic_stop_distance_ticks = 200

        # build_live_payload() subtracts the strategy stop buffer from low.
        synthetic_low = (
            live_ask
            - (
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
                live_ask,
            "low":
                synthetic_low,
            "signal_close_utc":
                now_utc
        }

        payload = build_live_payload(
            instrument,
            synthetic_result
        )

        payload["signal_id"] = (
            f"PIPELINE-TEST-"
            f"{instrument}-BUY-"
            f"{int(now_utc.timestamp())}"
        )

        payload["timestamp"] = (
            now_utc
            .strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        )

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
                    "BUY",
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
                "BUY",
            "strategy_id":
                config.get(
                    "strategy_id",
                    f"{instrument}_H1_LONG"
                ),
            "used_real_long_payload_builder":
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
                "BUY",
            "message":
                str(error)
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
    "/short-strategy-config/<instrument>"
)
def short_strategy_config(
    instrument
):
    instrument = instrument.upper()

    if instrument not in SHORT_STRATEGIES:
        return jsonify({
            "status": "error",
            "mode": "READ_ONLY",
            "instrument": instrument,
            "message": f"{instrument} has no short strategy",
        }), 400

    config = SHORT_STRATEGIES[instrument]

    return jsonify({
        "status": "success",
        "mode": "READ_ONLY",
        "instrument": instrument,
        "strategy_id": config.get(
            "strategy_id",
            f"{instrument}_H1_SHORT"
        ),
        "live_enabled": short_live_enabled_for(instrument),
        "strategy_config": {
            key: (
                sorted(value)
                if isinstance(value, set)
                else value
            )
            for key, value in config.items()
        },
        "orders_sent": False,
    })


@app.route(
    "/short-strategy-test/<instrument>"
)
def short_strategy_test(
    instrument
):

    instrument = (
        instrument.upper()
    )

    if instrument == "EUR_CHF":
        try:
            result = evaluate_latest_eurchf()
            return jsonify({"status": "success", "mode": "READ_ONLY", "orders_sent": False,
                            "instrument": instrument, "signal": result["qualified"],
                            **{k: iso_utc(v) if isinstance(v, datetime) else v for k,v in result.items()}})
        except Exception as error:
            return jsonify({"status": "error", "mode": "READ_ONLY", "orders_sent": False,
                            "reason": str(error)}), 503

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

            "trigger_id":
                result.get(
                    "trigger_id"
                ),

            "core_qualified":
                result.get(
                    "core_qualified"
                ),

            "sweep_qualified":
                result.get(
                    "sweep_qualified"
                ),

            "previous_atr_ratio_20":
                result.get(
                    "previous_atr_ratio_20"
                ),

            "signal_range_atr":
                result.get(
                    "signal_range_atr"
                ),

            "upper_wick_body_ratio":
                result.get(
                    "upper_wick_body_ratio"
                ),

            "prior_upward_momentum_atr":
                result.get(
                    "prior_upward_momentum_atr"
                ),

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

    with LIVE_STATE_LOCK:
        watcher_cycle = WATCHER_LAST_CYCLE["H1"]

    return jsonify({
        "watcher_retry_fix_version": WATCHER_RETRY_FIX_VERSION,
        "last_watcher_cycle": watcher_cycle,


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

        "eurjpy_short_live_enabled":
            EURJPY_SHORT_LIVE_ENABLED,

        "audusd_h1_long_live_enabled":
            AUDUSD_H1_LONG_LIVE_ENABLED,

        "audusd_h1_short_live_enabled":
            AUDUSD_H1_SHORT_LIVE_ENABLED,

        "audusd_m15_short_live_enabled":
            AUDUSD_M15_SHORT_LIVE_ENABLED,

        "audjpy_h1_long_tight_live_enabled":
            AUDJPY_H1_LONG_TIGHT_LIVE_ENABLED,

        "audjpy_h1_short_primary_live_enabled":
            AUDJPY_H1_SHORT_PRIMARY_LIVE_ENABLED,

        "eurchf_h1_short_primary_live_enabled": EURCHF_H1_SHORT_PRIMARY_LIVE_ENABLED,
        "eurchf_pass14_preview_enabled": EURCHF_PASS14_PREVIEW_ENABLED,
        "eurchf_pass14_version": EURCHF_PASS14_VERSION,
        "prepared_portfolio_version": "PORTFOLIO32_2026_10_05_EURCHF_H1_SHORT_PRIMARY_RR3P50_V1",
        "current_live_portfolio_version": (
            "PORTFOLIO32_2026_10_05_EURCHF_H1_SHORT_PRIMARY_RR3P50_V1"
            if EURCHF_H1_SHORT_PRIMARY_LIVE_ENABLED and SHORT_LIVE_SUBMISSION_ENABLED
            else "PORTFOLIO31_2026_09_30_AUDJPY_H1_SHORT_PRIMARY_RR3P25_V1"),

        "max_live_signal_age_seconds":
            MAX_LIVE_SIGNAL_AGE_SECONDS,

        "submission_reporting_version": SUBMISSION_REPORTING_VERSION,

        "signal_audit_version":
            SIGNAL_AUDIT_VERSION,

        "signal_audit_db_path":
            os.path.abspath(SIGNAL_AUDIT_DB_PATH),

        "signal_audit_persistent_volume_path":
            os.path.abspath(SIGNAL_AUDIT_DB_PATH).startswith("/data/"),

        "signal_audit_last_error":
            SIGNAL_AUDIT_LAST_ERROR,

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
# PERSISTENT SIGNAL AUDIT — READ ONLY
# ==================================================

@app.route("/live-signal-audit-status")
def live_signal_audit_status():
    try:
        if not _signal_audit_init():
            raise RuntimeError(SIGNAL_AUDIT_LAST_ERROR or "audit DB unavailable")
        connection = _signal_audit_connect(True)
        try:
            row = connection.execute(
                "SELECT COUNT(*) AS n, MIN(recorded_at_utc) AS first_utc, "
                "MAX(recorded_at_utc) AS last_utc FROM signal_audit_events"
            ).fetchone()
        finally:
            connection.close()
        return jsonify({
            "status": "success",
            "mode": "READ_ONLY",
            "audit_version": SIGNAL_AUDIT_VERSION,
            "database_path": os.path.abspath(SIGNAL_AUDIT_DB_PATH),
            "persistent_volume_path": os.path.abspath(SIGNAL_AUDIT_DB_PATH).startswith("/data/"),
            "event_count": int(row["n"]),
            "first_event_utc": row["first_utc"],
            "last_event_utc": row["last_utc"],
            "last_error": SIGNAL_AUDIT_LAST_ERROR,
            "orders_sent": False,
        })
    except Exception as error:
        return jsonify({
            "status": "error",
            "mode": "READ_ONLY",
            "message": str(error),
            "orders_sent": False,
        }), 500


@app.route("/live-signal-audit")
def live_signal_audit():
    try:
        if not _signal_audit_init():
            raise RuntimeError(SIGNAL_AUDIT_LAST_ERROR or "audit DB unavailable")

        limit = max(1, min(int(request.args.get("limit", "250")), 1000))
        pair = request.args.get("pair", "").strip().upper()
        strategy_id = request.args.get("strategy_id", "").strip()
        event_filter = request.args.get("event", "").strip().upper()
        signal_id = request.args.get("signal_id", "").strip()
        since_input = request.args.get("since", "").strip()

        where = ["1=1"]
        params = []
        if pair:
            where.append("pair = ?"); params.append(pair)
        if strategy_id:
            where.append("strategy_id = ?"); params.append(strategy_id)
        if event_filter:
            where.append("upper(event) = ?"); params.append(event_filter)
        if signal_id:
            where.append("signal_id = ?"); params.append(signal_id)
        since_utc = None
        if since_input:
            parsed = datetime.fromisoformat(since_input.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise ValueError("since must include Z or a timezone offset")
            since_utc = parsed.astimezone(timezone.utc).isoformat()
            where.append("recorded_at_utc >= ?"); params.append(since_utc)

        predicate = " AND ".join(where)
        connection = _signal_audit_connect(True)
        try:
            total = connection.execute(
                "SELECT COUNT(*) AS n FROM signal_audit_events WHERE " + predicate,
                params,
            ).fetchone()["n"]
            counts = connection.execute(
                "SELECT event, COUNT(*) AS n FROM signal_audit_events WHERE "
                + predicate + " GROUP BY event ORDER BY n DESC",
                params,
            ).fetchall()
            rows = connection.execute(
                "SELECT * FROM signal_audit_events WHERE " + predicate
                + " ORDER BY id DESC LIMIT ?",
                params + [limit],
            ).fetchall()
        finally:
            connection.close()

        return jsonify({
            "status": "success",
            "mode": "READ_ONLY",
            "audit_version": SIGNAL_AUDIT_VERSION,
            "filters": {
                "since_utc": since_utc,
                "pair": pair or None,
                "strategy_id": strategy_id or None,
                "event": event_filter or None,
                "signal_id": signal_id or None,
                "limit": limit,
            },
            "matched_count": int(total),
            "returned_count": len(rows),
            "more_available": int(total) > len(rows),
            "counts_by_event": {row["event"]: int(row["n"]) for row in counts},
            "events": [dict(row) for row in rows],
            "orders_sent": False,
        })
    except (ValueError, OverflowError) as error:
        return jsonify({
            "status": "error", "mode": "READ_ONLY",
            "message": "Invalid query parameter: " + str(error),
            "orders_sent": False,
        }), 400
    except Exception as error:
        return jsonify({
            "status": "error", "mode": "READ_ONLY",
            "message": str(error), "orders_sent": False,
        }), 500


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
# FINAL LOCKED M15 LIVE PORTFOLIO
# ==================================================
#
# Exact final full-history rules used in the live M15 portfolio.
# There is deliberately NO cross-strategy / pair-wide gate here.
# Pyramiding=0 is enforced centrally by the executor per strategy_id.
# ==================================================

M15_STRATEGY_IDS = {
    "EUR_USD_M15_LONG",
    "EUR_USD_M15_SHORT",
    "GBP_USD_M15_LONG",
    "GBP_USD_M15_SHORT",
    "USD_JPY_M15_LONG",
    "USD_JPY_M15_SHORT",
    "USD_CAD_M15_LONG",
    "USD_CAD_M15_SHORT",
    "EUR_GBP_M15_LONG",
    "EUR_GBP_M15_SHORT",
    "EUR_JPY_M15_LONG",
    "EUR_JPY_M15_SHORT",
    "AUD_USD_M15_SHORT",
    "AUD_JPY_M15_LONG",
    "AUD_JPY_M15_SHORT",
}

M15_PAIR_IDS = {
    "EUR_USD": ("EUR_USD_M15_LONG", "EUR_USD_M15_SHORT"),
    "GBP_USD": ("GBP_USD_M15_LONG", "GBP_USD_M15_SHORT"),
    "USD_JPY": ("USD_JPY_M15_LONG", "USD_JPY_M15_SHORT"),
    "USD_CAD": ("USD_CAD_M15_LONG", "USD_CAD_M15_SHORT"),
    "EUR_GBP": ("EUR_GBP_M15_LONG", "EUR_GBP_M15_SHORT"),
    "EUR_JPY": ("EUR_JPY_M15_LONG", "EUR_JPY_M15_SHORT"),
    "AUD_USD": ("AUD_USD_M15_SHORT",),
    "AUD_JPY": ("AUD_JPY_M15_LONG", "AUD_JPY_M15_SHORT"),
}

M15_RR_ALLOWED = {
    "EUR_USD_M15_LONG": {3.75},
    "EUR_USD_M15_SHORT": {3.50},
    "GBP_USD_M15_LONG": {4.25},
    "GBP_USD_M15_SHORT": {3.00},
    "USD_JPY_M15_LONG": {4.00},
    "USD_JPY_M15_SHORT": {4.75, 3.00},
    "USD_CAD_M15_LONG": {5.00, 5.25},
    "USD_CAD_M15_SHORT": {4.00, 3.50},
    "EUR_GBP_M15_LONG": {2.75, 2.25},
    "EUR_GBP_M15_SHORT": {4.50, 2.50},
    "EUR_JPY_M15_LONG": {4.25},
    "EUR_JPY_M15_SHORT": {4.75, 3.00},
    "AUD_USD_M15_SHORT": {3.50},
    "AUD_JPY_M15_LONG": {3.50},
    "AUD_JPY_M15_SHORT": {4.00},
}

LONDON_TZ = ZoneInfo("Europe/London")


def m15_fetch_recent(instrument, granularity, count, complete_only=True):
    params = candle_params(granularity)
    params["count"] = count

    data = oanda_get(
        f"/v3/instruments/{instrument}/candles",
        params=params
    )

    result = []
    for raw in data.get("candles", []):
        if complete_only and not raw.get("complete", False):
            continue

        mid = raw.get("mid")
        if not mid:
            continue

        result.append({
            "time": datetime.fromisoformat(
                raw["time"].replace("Z", "+00:00")
            ),
            "open": float(mid["o"]),
            "high": float(mid["h"]),
            "low": float(mid["l"]),
            "close": float(mid["c"]),
            "complete": bool(raw.get("complete", False)),
        })

    result.sort(key=lambda item: item["time"])
    return result


def m15_sma_optional(values, length):
    out = [None] * len(values)
    window = deque()
    total = 0.0
    invalid = 0

    for i, value in enumerate(values):
        window.append(value)
        if value is None or not math.isfinite(value):
            invalid += 1
        else:
            total += value

        if len(window) > length:
            old = window.popleft()
            if old is None or not math.isfinite(old):
                invalid -= 1
            else:
                total -= old

        if len(window) == length and invalid == 0:
            out[i] = total / length

    return out


def m15_previous_extreme(candles, index, lookback, field, want_max):
    if index < lookback:
        return None
    values = [
        float(candles[j][field])
        for j in range(index - lookback, index)
    ]
    return max(values) if want_max else min(values)


def m15_audjpy_short_core(candles, index, atr14_value):
    """
    Frozen AUD/JPY M15 SHORT #29 CORE signal.

    Exact research predicates:
      - bearish signal candle
      - signal high strictly above previous 60-bar high
      - signal close back below that previous high
      - bearish body >= 1.50 ATR14
      - signal range >= 2.25 ATR14
      - prior-16 completed-bar rise >= 1.50 ATR14

    The current signal candle is excluded from the 60-bar high and from the
    prior-rise measurement. ATR14 is the current candle's Wilder/RMA ATR.
    """
    if atr14_value is None or atr14_value <= 0 or index < 60 or index < 17:
        return False, {}

    candle = candles[index]
    previous_60_high = m15_previous_extreme(
        candles, index, 60, "high", True
    )
    if previous_60_high is None:
        return False, {}

    prior_16_rise_atr = (
        candles[index - 1]["close"] - candles[index - 17]["close"]
    ) / atr14_value
    bearish_body_atr = (candle["open"] - candle["close"]) / atr14_value
    signal_range_atr = (candle["high"] - candle["low"]) / atr14_value

    qualified = (
        candle["close"] < candle["open"]
        and candle["high"] > previous_60_high
        and candle["close"] < previous_60_high
        and bearish_body_atr >= 1.50
        and signal_range_atr >= 2.25
        and prior_16_rise_atr >= 1.50
    )

    return qualified, {
        "geometry": "CORE_LB60_M1.50",
        "previous_60_high": previous_60_high,
        "body_atr": bearish_body_atr,
        "range_atr": signal_range_atr,
        "prior_16_bar_rise_atr": prior_16_rise_atr,
        "signal_high": candle["high"],
        "signal_reference_close": candle["close"],
        "research_cost_grid": "0/5/10/20/40 adverse ticks; 10T live limit",
    }


def m15_exact_bullish_engulf(previous, current):
    return (
        previous["close"] < previous["open"]
        and current["close"] > current["open"]
        and current["open"] <= previous["close"]
        and current["close"] >= previous["open"]
    )


def m15_exact_bearish_engulf(previous, current):
    return (
        previous["close"] > previous["open"]
        and current["close"] < current["open"]
        and current["open"] >= previous["close"]
        and current["close"] <= previous["open"]
    )


def m15_htf_snapshot(candles, signal_time, need_atr_ratio=False):
    """
    State of the previous COMPLETED HTF candle.

    Completion is defined causally by the NEXT ACTUAL HTF candle open:
        complete_at = candles[i + 1]["time"]
        eligible only when complete_at <= signal M15 open time.
    """
    if len(candles) < 3:
        return None

    closes = [c["close"] for c in candles]
    ema50 = ema_series(closes, 50) if len(closes) >= 50 else [None] * len(closes)
    ema100 = ema_series(closes, 100) if len(closes) >= 100 else [None] * len(closes)
    ema200 = ema_series(closes, 200) if len(closes) >= 200 else [None] * len(closes)

    atr_values = atr_series(candles, 14)
    atr_mean50 = (
        m15_sma_optional(atr_values, 50)
        if need_atr_ratio
        else [None] * len(candles)
    )

    selected = None
    for i in range(len(candles) - 1):
        complete_at = candles[i + 1]["time"]
        if complete_at <= signal_time:
            selected = i
        else:
            break

    if selected is None:
        return None

    i = selected
    atr_ratio50 = None
    if (
        need_atr_ratio
        and atr_values[i] is not None
        and atr_mean50[i] is not None
        and atr_mean50[i] > 0
    ):
        atr_ratio50 = atr_values[i] / atr_mean50[i]

    return {
        "time": candles[i]["time"],
        "complete_at": candles[i + 1]["time"],
        "close": closes[i],
        "ema50": ema50[i],
        "ema100": ema100[i],
        "ema200": ema200[i],
        "atr14": atr_values[i],
        "atr_ratio50": atr_ratio50,
    }


def m15_signal_record(
    *,
    strategy_id,
    trigger_id,
    pair,
    side,
    candle,
    rr,
    stop,
    atr14_value,
    diagnostics=None,
):
    if strategy_id not in M15_STRATEGY_IDS:
        raise ValueError(f"Unknown M15 strategy_id {strategy_id}")

    if rr not in M15_RR_ALLOWED[strategy_id]:
        raise ValueError(f"RR {rr} is not locked for {strategy_id}")

    reference_entry = candle["close"]
    reference_risk = (
        reference_entry - stop
        if side == "BUY"
        else stop - reference_entry
    )
    if reference_risk <= 0:
        return None

    target = (
        reference_entry + rr * reference_risk
        if side == "BUY"
        else reference_entry - rr * reference_risk
    )

    signal_close = candle["time"] + timedelta(minutes=15)

    return {
        "qualified": True,
        "strategy_id": strategy_id,
        "trigger_id": trigger_id,
        "timeframe": "M15",
        "pair": pair,
        "side": side,
        "signal_start_utc": candle["time"],
        "signal_close_utc": signal_close,
        "entry": reference_entry,
        "stop": stop,
        "target": target,
        "rr": rr,
        "atr14": atr14_value,
        "diagnostics": diagnostics or {},
    }


def evaluate_latest_m15_pair(instrument):
    """
    Evaluate the final locked M15 strategy/strategies for one pair on the latest
    completed M15 candle. Returns zero, one or more independent signals.
    """
    m15 = m15_fetch_recent(
        instrument, "M15", 2000 if instrument == "AUD_JPY" else 260,
        complete_only=True,
    )
    if len(m15) < 200:
        raise RuntimeError(
            f"Not enough completed M15 candles for {instrument}: {len(m15)}"
        )

    atr_values = atr_series(m15, 14)
    i = len(m15) - 1
    current = m15[i]
    previous = m15[i - 1]
    atr = atr_values[i]

    if atr is None or atr <= 0:
        return {
            "instrument": instrument,
            "signal_start_utc": current["time"],
            "signal_close_utc": current["time"] + timedelta(minutes=15),
            "signals": [],
            "reason": "ATR_NOT_READY",
        }

    o = current["open"]
    h = current["high"]
    l = current["low"]
    c = current["close"]

    body = c - o
    bull = body > 0
    bear = body < 0
    bull_body = max(body, 0.0)
    bear_body = max(-body, 0.0)
    candle_range = h - l

    bull_atr = bull_body / atr if bull_body > 0 else 0.0
    bear_atr = bear_body / atr if bear_body > 0 else 0.0
    range_atr = candle_range / atr if candle_range > 0 else 0.0
    close_loc = (c - l) / candle_range if candle_range > 0 else 0.0
    lower_wick = min(o, c) - l
    upper_wick = h - max(o, c)
    lwb = lower_wick / bull_body if bull_body > 0 else 0.0
    uwb = upper_wick / bear_body if bear_body > 0 else 0.0

    prev_body_abs = abs(previous["close"] - previous["open"])
    bull_br = bull_body / prev_body_abs if prev_body_abs > 0 else None
    bear_br = bear_body / prev_body_abs if prev_body_abs > 0 else None

    exact_bull = m15_exact_bullish_engulf(previous, current)
    exact_bear = m15_exact_bearish_engulf(previous, current)

    prev_atr = atr_values[i - 1] if i >= 1 else None
    atr_mean20 = m15_sma_optional(atr_values, 20)
    prev_atr_mean20 = atr_mean20[i - 1] if i >= 1 else None
    compression = (
        prev_atr / prev_atr_mean20
        if (
            prev_atr is not None
            and prev_atr_mean20 is not None
            and prev_atr_mean20 > 0
        )
        else None
    )

    mom4 = None
    if i >= 17:
        mom4 = (m15[i - 1]["close"] - m15[i - 17]["close"]) / atr

    ny = current["time"].astimezone(NY_TZ)
    london = current["time"].astimezone(LONDON_TZ)

    # Fetch only the HTFs needed by the locked rules for this pair.
    h1_state = None
    h4_state = None
    daily_state = None

    if instrument in {"USD_JPY", "USD_CAD", "EUR_GBP", "EUR_JPY", "AUD_USD"}:
        h1_all = m15_fetch_recent(
            instrument, "H1", 1000, complete_only=False
        )
        h1_state = m15_htf_snapshot(
            h1_all,
            current["time"],
            need_atr_ratio=(instrument == "USD_JPY"),
        )

    if instrument in {"USD_JPY", "USD_CAD"}:
        h4_all = m15_fetch_recent(
            instrument, "H4", 1000, complete_only=False
        )
        h4_state = m15_htf_snapshot(
            h4_all,
            current["time"],
            need_atr_ratio=False,
        )

        daily_all = m15_fetch_recent(
            instrument, "D", 1500, complete_only=False
        )
        daily_state = m15_htf_snapshot(
            daily_all,
            current["time"],
            need_atr_ratio=False,
        )

    signals = []

    # --------------------------------------------------
    # EUR/USD
    # --------------------------------------------------
    if instrument == "EUR_USD":
        p165_low = m15_previous_extreme(m15, i, 165, "low", False)
        p60_high = m15_previous_extreme(m15, i, 60, "high", True)

        if (
            exact_bull
            and bull_br is not None and bull_br >= 1.20
            and bull_atr >= 1.00
            and abs(l - p165_low) / atr <= 0.10
            and ny.weekday() != 1
        ):
            signals.append(m15_signal_record(
                strategy_id="EUR_USD_M15_LONG",
                trigger_id="CORE",
                pair=instrument,
                side="BUY",
                candle=current,
                rr=3.75,
                stop=l - 10 * 0.00001,
                atr14_value=atr,
                diagnostics={
                    "body_ratio": bull_br,
                    "body_atr": bull_atr,
                    "structure_distance_atr": abs(l - p165_low) / atr,
                    "ny_weekday": ny.weekday(),
                },
            ))

        if (
            exact_bear
            and bear_br is not None and bear_br >= 1.10
            and bear_atr >= 1.30
            and range_atr >= 1.70
            and abs(h - p60_high) / atr <= 0.225
            and ny.hour in {2, 3}
        ):
            signals.append(m15_signal_record(
                strategy_id="EUR_USD_M15_SHORT",
                trigger_id="CORE",
                pair=instrument,
                side="SELL",
                candle=current,
                rr=3.50,
                stop=h + 10 * 0.00001,
                atr14_value=atr,
                diagnostics={
                    "body_ratio": bear_br,
                    "body_atr": bear_atr,
                    "range_atr": range_atr,
                    "structure_distance_atr": abs(h - p60_high) / atr,
                    "ny_hour": ny.hour,
                },
            ))

    # --------------------------------------------------
    # GBP/USD
    # --------------------------------------------------
    elif instrument == "GBP_USD":
        p165_low = m15_previous_extreme(m15, i, 165, "low", False)
        p100_high = m15_previous_extreme(m15, i, 100, "high", True)

        if (
            bull
            and bull_atr >= 1.00
            and close_loc >= 0.75
            and l < p165_low
            and c > p165_low
            and ny.hour in {4, 5, 6, 7}
        ):
            signals.append(m15_signal_record(
                strategy_id="GBP_USD_M15_LONG",
                trigger_id="FAILED_BREAKDOWN_RECLAIM",
                pair=instrument,
                side="BUY",
                candle=current,
                rr=4.25,
                stop=l - 10 * 0.00001,
                atr14_value=atr,
                diagnostics={
                    "body_atr": bull_atr,
                    "close_location": close_loc,
                    "prior_165_low": p165_low,
                    "ny_hour": ny.hour,
                },
            ))

        if (
            exact_bear
            and bear_br is not None and bear_br >= 1.30
            and bear_atr >= 1.10
            and range_atr >= 1.60
            and abs(h - p100_high) / atr <= 0.075
        ):
            signals.append(m15_signal_record(
                strategy_id="GBP_USD_M15_SHORT",
                trigger_id="GRID_0240",
                pair=instrument,
                side="SELL",
                candle=current,
                rr=3.00,
                stop=h + 10 * 0.00001,
                atr14_value=atr,
                diagnostics={
                    "body_ratio": bear_br,
                    "body_atr": bear_atr,
                    "range_atr": range_atr,
                    "structure_distance_atr": abs(h - p100_high) / atr,
                },
            ))

    # --------------------------------------------------
    # USD/JPY
    # --------------------------------------------------
    elif instrument == "USD_JPY":
        p30_low = m15_previous_extreme(m15, i, 30, "low", False)
        p40_low = m15_previous_extreme(m15, i, 40, "low", False)
        p165_high = m15_previous_extreme(m15, i, 165, "high", True)

        h1_ratio_ok = (
            h1_state is not None
            and h1_state["atr_ratio50"] is not None
            and h1_state["atr_ratio50"] >= 0.80
        )

        if (
            bull
            and bull_atr >= 1.25
            and l < p30_low
            and c > previous["high"]
            and lwb >= 0.25
            and mom4 is not None and mom4 <= -1.75
            and h1_ratio_ok
        ):
            signals.append(m15_signal_record(
                strategy_id="USD_JPY_M15_LONG",
                trigger_id="SWEEP30_RR4.00",
                pair=instrument,
                side="BUY",
                candle=current,
                rr=4.00,
                stop=l - 10 * 0.001,
                atr14_value=atr,
                diagnostics={
                    "body_atr": bull_atr,
                    "lower_wick_body": lwb,
                    "prior_4h_momentum_atr": mom4,
                    "h1_atr_ratio50": h1_state["atr_ratio50"],
                },
            ))

        core_short = (
            bear
            and compression is not None and compression <= 0.80
            and bear_atr >= 1.25
            and range_atr >= 1.60
            and c < p40_low
            and daily_state is not None
            and daily_state["ema200"] is not None
            and daily_state["close"] < daily_state["ema200"]
        )

        comp_short = (
            bear
            and h > p165_high
            and c < p165_high
            and bear_atr >= 1.00
            and close_loc <= 0.20
            and h4_state is not None
            and h4_state["ema100"] is not None
            and h4_state["close"] < h4_state["ema100"]
        )

        if core_short:
            signals.append(m15_signal_record(
                strategy_id="USD_JPY_M15_SHORT",
                trigger_id="CORE_GRID_0731",
                pair=instrument,
                side="SELL",
                candle=current,
                rr=4.75,
                stop=h + 10 * 0.001,
                atr14_value=atr,
                diagnostics={
                    "compression": compression,
                    "body_atr": bear_atr,
                    "range_atr": range_atr,
                    "daily_close": daily_state["close"],
                    "daily_ema200": daily_state["ema200"],
                },
            ))
        elif comp_short:
            signals.append(m15_signal_record(
                strategy_id="USD_JPY_M15_SHORT",
                trigger_id="COMPLEMENT_CAND_0322",
                pair=instrument,
                side="SELL",
                candle=current,
                rr=3.00,
                stop=h + 10 * 0.001,
                atr14_value=atr,
                diagnostics={
                    "body_atr": bear_atr,
                    "close_location": close_loc,
                    "h4_close": h4_state["close"],
                    "h4_ema100": h4_state["ema100"],
                },
            ))

    # --------------------------------------------------
    # USD/CAD
    # --------------------------------------------------
    elif instrument == "USD_CAD":
        p165_low = m15_previous_extreme(m15, i, 165, "low", False)
        p5_high = m15_previous_extreme(m15, i, 5, "high", True)
        p120_high = m15_previous_extreme(m15, i, 120, "high", True)
        p20_high = m15_previous_extreme(m15, i, 20, "high", True)

        core_long = (
            exact_bull
            and bull_br is not None and bull_br >= 1.70
            and bull_atr >= 1.25
            and range_atr >= 1.50
            and abs(l - p165_low) / atr <= 0.175
            and daily_state is not None
            and daily_state["ema50"] is not None
            and daily_state["ema200"] is not None
            and daily_state["ema50"] > daily_state["ema200"]
        )

        comp_long = (
            bull
            and compression is not None and compression <= 0.70
            and bull_atr >= 1.00
            and range_atr >= 1.50
            and c > p5_high
            and h4_state is not None
            and h4_state["ema100"] is not None
            and h4_state["close"] > h4_state["ema100"]
        )

        if core_long:
            signals.append(m15_signal_record(
                strategy_id="USD_CAD_M15_LONG",
                trigger_id="CORE_EMA50_REGIME_BR170",
                pair=instrument,
                side="BUY",
                candle=current,
                rr=5.00,
                stop=l - 10 * 0.00001,
                atr14_value=atr,
                diagnostics={
                    "body_ratio": bull_br,
                    "body_atr": bull_atr,
                    "range_atr": range_atr,
                    "structure_distance_atr": abs(l - p165_low) / atr,
                    "daily_ema50": daily_state["ema50"],
                    "daily_ema200": daily_state["ema200"],
                },
            ))
        elif comp_long:
            signals.append(m15_signal_record(
                strategy_id="USD_CAD_M15_LONG",
                trigger_id="COMPLEMENT_H4_COMPRESSION_BREAKOUT",
                pair=instrument,
                side="BUY",
                candle=current,
                rr=5.25,
                stop=l - 10 * 0.00001,
                atr14_value=atr,
                diagnostics={
                    "compression": compression,
                    "body_atr": bull_atr,
                    "range_atr": range_atr,
                    "prior_5_high": p5_high,
                    "h4_close": h4_state["close"],
                    "h4_ema100": h4_state["ema100"],
                },
            ))

        core_short = (
            bear
            and h > previous["high"]
            and l < previous["low"]
            and bear_atr >= 0.75
            and close_loc <= 0.35
            and abs(h - p120_high) / atr <= 0.05
            and h4_state is not None
            and h4_state["ema100"] is not None
            and h4_state["close"] < h4_state["ema100"]
        )

        comp_short = (
            bear
            and h > p20_high
            and c < p20_high
            and bear_atr >= 1.25
            and close_loc <= 0.40
            and uwb >= 0.25
            and h1_state is not None
            and h1_state["ema100"] is not None
            and h1_state["close"] < h1_state["ema100"]
        )

        if core_short:
            signals.append(m15_signal_record(
                strategy_id="USD_CAD_M15_SHORT",
                trigger_id="CORE_OUTSIDE_H4EMA100_LB120_D005_RR400",
                pair=instrument,
                side="SELL",
                candle=current,
                rr=4.00,
                stop=h + 10 * 0.00001,
                atr14_value=atr,
                diagnostics={
                    "body_atr": bear_atr,
                    "close_location": close_loc,
                    "structure_distance_atr": abs(h - p120_high) / atr,
                    "h4_close": h4_state["close"],
                    "h4_ema100": h4_state["ema100"],
                },
            ))
        elif comp_short:
            signals.append(m15_signal_record(
                strategy_id="USD_CAD_M15_SHORT",
                trigger_id="COMPLEMENT_HIGH_SWEEP_H1EMA100_LB20_RR350",
                pair=instrument,
                side="SELL",
                candle=current,
                rr=3.50,
                stop=h + 10 * 0.00001,
                atr14_value=atr,
                diagnostics={
                    "body_atr": bear_atr,
                    "close_location": close_loc,
                    "upper_wick_body": uwb,
                    "h1_close": h1_state["close"],
                    "h1_ema100": h1_state["ema100"],
                },
            ))

    # --------------------------------------------------
    # EUR/GBP
    # --------------------------------------------------
    elif instrument == "EUR_GBP":
        p60_low = m15_previous_extreme(m15, i, 60, "low", False)
        p165_low = m15_previous_extreme(m15, i, 165, "low", False)
        p120_high = m15_previous_extreme(m15, i, 120, "high", True)
        p100_high = m15_previous_extreme(m15, i, 100, "high", True)

        core_long = (
            bull
            and bull_atr >= 1.20
            and l < p60_low
            and c > previous["high"]
            and lwb >= 0.25
            and mom4 is not None and mom4 <= -1.25
            and ny.hour in {1, 2, 3}
        )

        comp_long = (
            exact_bull
            and bull_br is not None and bull_br >= 1.20
            and bull_atr >= 1.10
            and abs(l - p165_low) / atr <= 0.20
            and london.hour in {3, 4, 5, 6, 7}
        )

        if core_long:
            signals.append(m15_signal_record(
                strategy_id="EUR_GBP_M15_LONG",
                trigger_id="CORE_SWEEP_DISPLACEMENT",
                pair=instrument,
                side="BUY",
                candle=current,
                rr=2.75,
                stop=l - 10 * 0.00001,
                atr14_value=atr,
                diagnostics={
                    "body_atr": bull_atr,
                    "lower_wick_body": lwb,
                    "prior_4h_momentum_atr": mom4,
                    "ny_hour": ny.hour,
                },
            ))
        elif comp_long:
            signals.append(m15_signal_record(
                strategy_id="EUR_GBP_M15_LONG",
                trigger_id="COMPLEMENT_LONDON_ENGULF_STRUCTURE",
                pair=instrument,
                side="BUY",
                candle=current,
                rr=2.25,
                stop=l - 10 * 0.00001,
                atr14_value=atr,
                diagnostics={
                    "body_ratio": bull_br,
                    "body_atr": bull_atr,
                    "structure_distance_atr": abs(l - p165_low) / atr,
                    "london_hour": london.hour,
                },
            ))

        core_short = (
            bear
            and h > p120_high
            and c < p120_high
            and bear_atr >= 1.25
            and close_loc <= 0.20
            and uwb >= 0.40
            and london.weekday() != 2
        )

        comp_short = (
            exact_bear
            and bear_br is not None and bear_br >= 1.60
            and bear_atr >= 1.00
            and abs(h - p100_high) / atr <= 0.15
            and h1_state is not None
            and h1_state["ema100"] is not None
            and h1_state["close"] < h1_state["ema100"]
        )

        if core_short:
            signals.append(m15_signal_record(
                strategy_id="EUR_GBP_M15_SHORT",
                trigger_id="CORE_HIGH_SWEEP_REJECTION",
                pair=instrument,
                side="SELL",
                candle=current,
                rr=4.50,
                stop=h + 10 * 0.00001,
                atr14_value=atr,
                diagnostics={
                    "body_atr": bear_atr,
                    "close_location": close_loc,
                    "upper_wick_body": uwb,
                    "london_weekday": london.weekday(),
                },
            ))
        elif comp_short:
            signals.append(m15_signal_record(
                strategy_id="EUR_GBP_M15_SHORT",
                trigger_id="COMPLEMENT_BEAR_ENGULF_H1EMA100",
                pair=instrument,
                side="SELL",
                candle=current,
                rr=2.50,
                stop=h + 10 * 0.00001,
                atr14_value=atr,
                diagnostics={
                    "body_ratio": bear_br,
                    "body_atr": bear_atr,
                    "structure_distance_atr": abs(h - p100_high) / atr,
                    "h1_close": h1_state["close"],
                    "h1_ema100": h1_state["ema100"],
                },
            ))

    # --------------------------------------------------
    # EUR/JPY — FINAL LOCKED M15 LONG (#23) + SHORT (#24)
    # --------------------------------------------------
    elif instrument == "EUR_JPY":
        p100_low = m15_previous_extreme(m15, i, 100, "low", False)
        p40_high = m15_previous_extreme(m15, i, 40, "high", True)
        p10_high = m15_previous_extreme(m15, i, 10, "high", True)
        p60_high = m15_previous_extreme(m15, i, 60, "high", True)

        # Strategy #23 — final locked M15 LONG.
        if (
            exact_bull
            and bull_br is not None and bull_br >= 1.50
            and bull_atr >= 0.75
            and p100_low is not None
            and abs(l - p100_low) / atr <= 0.10
            and h1_state is not None
            and h1_state["ema50"] is not None
            and h1_state["ema200"] is not None
            and h1_state["ema50"] > h1_state["ema200"]
        ):
            signals.append(m15_signal_record(
                strategy_id="EUR_JPY_M15_LONG",
                trigger_id="CORE_ENGULF_H1_EMA50_GT_EMA200",
                pair=instrument,
                side="BUY",
                candle=current,
                rr=4.25,
                stop=l - 10 * 0.001,
                atr14_value=atr,
                diagnostics={
                    "body_ratio": bull_br,
                    "body_atr": bull_atr,
                    "structure_distance_atr": abs(l - p100_low) / atr,
                    "h1_close": h1_state["close"],
                    "h1_ema50": h1_state["ema50"],
                    "h1_ema200": h1_state["ema200"],
                },
            ))

        # Strategy #24 — final frozen M15 SHORT.
        #
        # B is deliberately evaluated first because the locked combined
        # historical strategy gives HIGH_SWEEP_DISPLACEMENT priority when
        # A and B qualify on the same signal candle.
        b_short = (
            bear
            and p60_high is not None
            and h > p60_high
            and c < previous["low"]
            and bear_atr >= 1.00
            and uwb >= 0.35
            and mom4 is not None and mom4 >= 1.00
            and h1_state is not None
            and h1_state["ema100"] is not None
            and h1_state["close"] < h1_state["ema100"]
        )

        a_short = (
            bear
            and p40_high is not None
            and p10_high is not None
            and h > p40_high
            and c < p10_high
            and bear_atr >= 0.75
            and mom4 is not None and mom4 >= 1.25
            and close_loc <= 0.30
            and ny.hour in {16, 17, 18, 19}
        )

        if b_short:
            signals.append(m15_signal_record(
                strategy_id="EUR_JPY_M15_SHORT",
                trigger_id="B_HIGH_SWEEP_DISPLACEMENT_H1EMA100",
                pair=instrument,
                side="SELL",
                candle=current,
                rr=3.00,
                stop=h + 10 * 0.001,
                atr14_value=atr,
                diagnostics={
                    "family": "HIGH_SWEEP_DISPLACEMENT",
                    "body_atr": bear_atr,
                    "upper_wick_body": uwb,
                    "prior_4h_momentum_atr": mom4,
                    "prior_60_high": p60_high,
                    "previous_low": previous["low"],
                    "h1_close": h1_state["close"],
                    "h1_ema100": h1_state["ema100"],
                    "same_candle_priority": "B",
                },
            ))
        elif a_short:
            signals.append(m15_signal_record(
                strategy_id="EUR_JPY_M15_SHORT",
                trigger_id="A_RALLY_REJECTION_NY16_19",
                pair=instrument,
                side="SELL",
                candle=current,
                rr=4.75,
                stop=h + 10 * 0.001,
                atr14_value=atr,
                diagnostics={
                    "family": "RALLY_REJECTION",
                    "body_atr": bear_atr,
                    "prior_4h_momentum_atr": mom4,
                    "close_location": close_loc,
                    "prior_40_high": p40_high,
                    "prior_10_high": p10_high,
                    "ny_hour": ny.hour,
                    "same_candle_priority": "B",
                },
            ))

    # --------------------------------------------------
    # AUD/USD — FINAL FROZEN M15 SHORT (#27)
    # --------------------------------------------------
    elif instrument == "AUD_USD":
        p120_high = m15_previous_extreme(m15, i, 120, "high", True)

        # H1 snapshot uses only a strictly completed H1 candle as of the
        # signal M15 OPEN (the same causal convention as the frozen research).
        if (
            exact_bear
            and bear_br is not None and bear_br >= 1.35
            and bear_atr >= 1.00
            and p120_high is not None
            and abs(h - p120_high) / atr <= 0.10
            and h1_state is not None
            and h1_state["ema50"] is not None
            and h1_state["ema200"] is not None
            and h1_state["ema50"] < h1_state["ema200"]
        ):
            signals.append(m15_signal_record(
                strategy_id="AUD_USD_M15_SHORT",
                trigger_id="CORE_ENGULF_H1_EMA50_LT_EMA200",
                pair=instrument,
                side="SELL",
                candle=current,
                rr=3.50,
                stop=h + 10 * 0.00001,
                atr14_value=atr,
                diagnostics={
                    "body_ratio": bear_br,
                    "body_atr": bear_atr,
                    "structure_distance_atr": abs(h - p120_high) / atr,
                    "prior_120_high": p120_high,
                    "h1_complete_at": iso_utc(h1_state["complete_at"]),
                    "h1_ema50": h1_state["ema50"],
                    "h1_ema200": h1_state["ema200"],
                },
            ))

    # --------------------------------------------------
    # AUD/JPY — FROZEN M15 LONG CORE (#28)
    # --------------------------------------------------
    elif instrument == "AUD_JPY":
        # All references exclude current signal candle except current ATR14:
        # previous 60 lows = candles [i-60, i),
        # momentum = (close[i-1] - close[i-193]) / ATR14[i].
        # 192 M15 bar intervals are 48 *market* hours, not elapsed clock hours.
        p60_low = m15_previous_extreme(m15, i, 60, "low", False)
        prior_192_momentum = (
            (m15[i - 1]["close"] - m15[i - 193]["close"]) / atr
            if i >= 193 else None
        )
        previous_bear_body = previous["open"] - previous["close"]
        current_bull_body = current["close"] - current["open"]
        # Research requires the current bullish body >= prior bearish body.
        # Exact engulfing wick-independent body boundaries are in exact_bull.
        if (
            exact_bull
            and previous_bear_body > 0
            and current_bull_body >= previous_bear_body
            and bull_atr >= 1.00
            and range_atr >= 1.80
            and p60_low is not None
            and abs(l - p60_low) / atr <= 0.50
            and prior_192_momentum is not None
            and prior_192_momentum >= 0.50
        ):
            signals.append(m15_signal_record(
                strategy_id="AUD_JPY_M15_LONG",
                trigger_id="CORE_ENGULF_LB60_D050_RANGE180_MOM192",
                pair=instrument,
                side="BUY",
                candle=current,
                rr=3.50,
                stop=l - 10 * 0.001,
                atr14_value=atr,
                diagnostics={
                    "geometry": "CORE_D050_NOT_FREQUENCY_D075",
                    "body_ratio": current_bull_body / previous_bear_body,
                    "body_atr": bull_atr,
                    "range_atr": range_atr,
                    "structure_distance_atr": abs(l - p60_low) / atr,
                    "prior_60_low": p60_low,
                    "prior_192_bar_momentum_atr": prior_192_momentum,
                    "atr14": atr,
                    "signal_low": l,
                    "signal_reference_close": c,
                    "research_2_4_pip_fills": "historical assumptions only",
                },
            ))

        # Strategy #29 — frozen rally-rejection SHORT CORE.
        short_ok, short_diag = m15_audjpy_short_core(m15, i, atr)
        if short_ok:
            signals.append(m15_signal_record(
                strategy_id="AUD_JPY_M15_SHORT",
                trigger_id="CORE_RALLY_REJECTION_LB60_B150_R225_M150",
                pair=instrument,
                side="SELL",
                candle=current,
                rr=4.00,
                stop=h + 10 * 0.001,
                atr14_value=atr,
                diagnostics=short_diag,
            ))

    else:
        raise ValueError(f"Unsupported M15 pair: {instrument}")

    return {
        "instrument": instrument,
        "signal_start_utc": current["time"],
        "signal_close_utc": current["time"] + timedelta(minutes=15),
        "signals": [x for x in signals if x is not None],
    }


def m15_signal_id(signal):
    milliseconds = int(
        signal["signal_close_utc"].timestamp() * 1000
    )
    return (
        f'{signal["strategy_id"]}-'
        f'{signal["trigger_id"]}-'
        f'{milliseconds}'
    )


def build_m15_live_payload(signal):
    if not WEBHOOK_SECRET:
        raise RuntimeError("WEBHOOK_SECRET is not configured")

    return {
        "secret": WEBHOOK_SECRET,
        "strategy_id": signal["strategy_id"],
        "timeframe": "M15",
        "trigger_id": signal["trigger_id"],
        "pair": signal["pair"],
        "side": signal["side"],
        "entry": round(
            signal["entry"],
            3 if signal["pair"] in {"USD_JPY", "EUR_JPY", "AUD_JPY"} else 5,
        ),
        "stop": round(
            signal["stop"],
            3 if signal["pair"] in {"USD_JPY", "EUR_JPY", "AUD_JPY"} else 5,
        ),
        "rr": signal["rr"],
        "signal_id": m15_signal_id(signal),
        "timestamp": signal["signal_close_utc"]
            .astimezone(timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def process_live_m15_pair(
    instrument,
    expected_close_utc=None,
    force=False,
    allow_submission=True,
):
    started = time.perf_counter()
    result = evaluate_latest_m15_pair(instrument)

    signal_close = result["signal_close_utc"].astimezone(timezone.utc)
    signal_start = result["signal_start_utc"].astimezone(timezone.utc)

    if (
        expected_close_utc is not None
        and signal_close < expected_close_utc
    ):
        return {
            "instrument": instrument,
            "timeframe": "M15",
            "ready": False,
            "latest_completed_close_utc": iso_utc(signal_close),
        }

    candle_key = iso_utc(signal_close)
    state_key = f"{instrument}_M15_PAIR"

    with M15_STATE_LOCK:
        already_processed = (
            M15_LAST_PROCESSED.get(state_key) == candle_key
        )

        if already_processed and not force:
            return {
                "instrument": instrument,
                "timeframe": "M15",
                "ready": True,
                "already_processed": True,
                "signal_close_utc": candle_key,
            }

        M15_LAST_PROCESSED[state_key] = candle_key

    close_age_seconds = (
        utc_now() - signal_close
    ).total_seconds()

    if not result["signals"]:
        event = add_live_event(
            "M15_NO_SIGNAL",
            f"{instrument}_M15",
            timeframe="M15",
            signal_start_utc=iso_utc(signal_start),
            signal_close_utc=candle_key,
            close_age_seconds=round(close_age_seconds, 3),
            evaluation_ms=round(
                (time.perf_counter() - started) * 1000,
                2,
            ),
        )
        return {
            "instrument": instrument,
            "timeframe": "M15",
            "ready": True,
            **event,
        }

    outputs = []

    for signal in result["signals"]:
        base = {
            "strategy_id": signal["strategy_id"],
            "trigger_id": signal["trigger_id"],
            "timeframe": "M15",
            "side": signal["side"],
            "signal_start_utc": iso_utc(signal_start),
            "signal_close_utc": candle_key,
            "entry": signal["entry"],
            "stop": signal["stop"],
            "rr": signal["rr"],
            "signal_id": m15_signal_id(signal),
            "close_age_seconds": round(close_age_seconds, 3),
            "diagnostics": signal.get("diagnostics", {}),
            "pair": signal["pair"],
            "_audit_persist": not force,
        }

        if not force:
            persist_signal_audit_event(
                "QUALIFIED",
                signal["strategy_id"],
                {k: v for k, v in base.items() if k != "_audit_persist"},
            )

        if (
            close_age_seconds < -2
            or close_age_seconds > MAX_LIVE_SIGNAL_AGE_SECONDS
        ):
            outputs.append(
                add_live_event(
                    "M15_SIGNAL_STALE_NOT_SUBMITTED",
                    signal["strategy_id"],
                    **base,
                )
            )
            continue

        strategy_live_enabled = (
            not (
                signal["strategy_id"] == "EUR_JPY_M15_SHORT"
                and not EURJPY_M15_SHORT_LIVE_ENABLED
            )
            and not (
                signal["strategy_id"] == "AUD_USD_M15_SHORT"
                and not AUDUSD_M15_SHORT_LIVE_ENABLED
            )
            and not (
                signal["strategy_id"] == "AUD_JPY_M15_LONG"
                and not AUDJPY_M15_LONG_LIVE_ENABLED
            )
            and not (
                signal["strategy_id"] == "AUD_JPY_M15_SHORT"
                and not AUDJPY_M15_SHORT_LIVE_ENABLED
            )
        )

        if (
            not M15_LIVE_SUBMISSION_ENABLED
            or not allow_submission
            or not strategy_live_enabled
        ):
            outputs.append(
                add_live_event(
                    "M15_DRY_RUN_SIGNAL",
                    signal["strategy_id"],
                    strategy_live_enabled=strategy_live_enabled,
                    **base,
                )
            )
            continue

        payload = build_m15_live_payload(signal)
        executor_result = post_to_executor(payload)

        if executor_result.get("ok"):
            outputs.append(
                add_live_event(
                    "M15_SIGNAL_SUBMITTED",
                    signal["strategy_id"],
                    **base,
                    executor_status_code=executor_result.get("status_code"),
                    executor_latency_ms=executor_result.get("latency_ms"),
                    executor_attempt=executor_result.get("attempt"),
                    executor_http_ok=True,
                    executor_status=(
                        executor_result.get("response", {}).get("status")
                        if isinstance(executor_result.get("response"), dict)
                        else None
                    ),
                    executor_order_submitted=(
                        executor_result.get("response", {}).get("order_submitted")
                        if isinstance(executor_result.get("response"), dict)
                        else None
                    ),
                    _audit_executor_response=executor_result.get("response"),
                )
            )
        else:
            outputs.append(
                add_live_event(
                    "M15_SIGNAL_SUBMISSION_FAILED",
                    signal["strategy_id"],
                    **base,
                    executor_error=executor_result.get("error"),
                    **executor_failure_audit_fields(executor_result),
                )
            )

    return {
        "instrument": instrument,
        "timeframe": "M15",
        "ready": True,
        "signal_count": len(result["signals"]),
        "results": outputs,
    }


def next_quarter_boundary(now=None):
    if now is None:
        now = utc_now()

    base = now.replace(second=0, microsecond=0)
    minute = (base.minute // 15 + 1) * 15

    if minute >= 60:
        return (
            base.replace(minute=0)
            + timedelta(hours=1)
        )

    return base.replace(minute=minute)


def wait_for_new_m15_and_process(expected_close_utc):
    pending = set(M15_PAIR_IDS)
    required = len(pending)
    attempts, details = {}, {}
    process_errors, retry_logged, available = set(), set(), set()
    deadline = time.monotonic() + M15_POLL_WINDOW_SECONDS

    while pending and time.monotonic() < deadline:
        eligible = pending - process_errors
        ready = []
        if eligible:
            with ThreadPoolExecutor(max_workers=len(eligible)) as pool:
                futures = {
                    pool.submit(m15_fetch_recent, pair, "M15", 3, True): pair
                    for pair in eligible
                }
                for future in as_completed(futures):
                    pair = futures[future]
                    try:
                        candles = future.result()
                        if candles and candles[-1]["time"] + timedelta(minutes=15) >= expected_close_utc:
                            ready.append(pair)
                            available.add(pair)
                    except Exception as error:
                        add_live_event(
                            "M15_CANDLE_READY_CHECK_ERROR", f"{pair}_M15",
                            expected_close_utc=iso_utc(expected_close_utc), error=str(error),
                        )

        if ready:
            with ThreadPoolExecutor(max_workers=len(ready)) as pool:
                futures = {}
                for pair in ready:
                    attempts[pair] = attempts.get(pair, 0) + 1
                    futures[pool.submit(
                        process_live_m15_pair, pair, expected_close_utc, False, True
                    )] = pair
                for future in as_completed(futures):
                    pair = futures[future]
                    fields = {"pair": pair, "timeframe": "M15",
                              "expected_close_utc": iso_utc(expected_close_utc)}
                    try:
                        result = future.result()
                        if _watcher_result_complete(result, expected_close_utc):
                            pending.discard(pair)
                        else:
                            details[pair] = _watcher_retry_detail(result)
                            if pair not in retry_logged:
                                add_live_event(
                                    "M15_EVALUATION_NOT_READY_RETRYING", f"{pair}_M15",
                                    **fields, **details[pair], attempts=attempts[pair],
                                )
                                retry_logged.add(pair)
                    except Exception as error:
                        process_errors.add(pair)
                        details[pair] = {"reason": "PROCESS_ERROR_NO_AUTOMATIC_RETRY", "error": str(error)}
                        add_live_event(
                            "M15_LIVE_PROCESS_ERROR", f"{pair}_M15",
                            **fields, **details[pair], attempts=attempts[pair],
                        )

        if pending:
            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(M15_POLL_INTERVAL_SECONDS, remaining))

    for pair in sorted(pending):
        if pair not in available:
            add_live_event(
                "NEW_M15_NOT_AVAILABLE_WITHIN_WINDOW", f"{pair}_M15",
                expected_close_utc=iso_utc(expected_close_utc),
            )
        add_live_event(
            "M15_EVALUATION_NOT_COMPLETED_WITHIN_WINDOW", f"{pair}_M15",
            pair=pair, timeframe="M15", expected_close_utc=iso_utc(expected_close_utc),
            attempts=attempts.get(pair, 0),
            **details.get(pair, {"reason": "TARGET_CANDLE_NOT_AVAILABLE"}),
        )
    _watcher_store_cycle("M15", expected_close_utc, required, pending, attempts, details)


def m15_live_watcher_loop():
    add_live_event(
        "M15_WATCHER_STARTED",
        timeframe="M15",
        submission_enabled=M15_LIVE_SUBMISSION_ENABLED,
        eurjpy_m15_short_live_enabled=EURJPY_M15_SHORT_LIVE_ENABLED,
        audusd_m15_short_live_enabled=AUDUSD_M15_SHORT_LIVE_ENABLED,
        audjpy_m15_long_live_enabled=AUDJPY_M15_LONG_LIVE_ENABLED,
        audjpy_m15_short_live_enabled=AUDJPY_M15_SHORT_LIVE_ENABLED,
    )

    # Startup evaluation is safe: stale guard prevents old entry after restart.
    with ThreadPoolExecutor(
        max_workers=len(M15_PAIR_IDS)
    ) as pool:
        futures = {
            pool.submit(
                process_live_m15_pair,
                instrument,
                None,
                False,
                True,
            ): instrument
            for instrument in M15_PAIR_IDS
        }

        for future in as_completed(futures):
            instrument = futures[future]
            try:
                future.result()
            except Exception as error:
                add_live_event(
                    "M15_STARTUP_CHECK_ERROR",
                    f"{instrument}_M15",
                    error=str(error),
                )

    while True:
        expected_close = next_quarter_boundary(utc_now())
        wake_time = (
            expected_close
            + timedelta(seconds=M15_POLL_OFFSET_SECONDS)
        )

        sleep_seconds = max(
            0.0,
            (wake_time - utc_now()).total_seconds(),
        )
        time.sleep(sleep_seconds)

        wait_for_new_m15_and_process(expected_close)


def start_m15_live_watcher_once():
    global M15_WATCHER_STARTED

    if not M15_LIVE_WATCHER_ENABLED:
        return

    if M15_WATCHER_STARTED:
        return

    M15_WATCHER_STARTED = True

    thread = threading.Thread(
        target=m15_live_watcher_loop,
        name="erf-m15-live-watcher",
        daemon=True,
    )
    thread.start()


@app.route("/m15-live-status")
def m15_live_status():
    with M15_STATE_LOCK:
        processed = dict(M15_LAST_PROCESSED)

    with LIVE_STATE_LOCK:
        watcher_cycle = WATCHER_LAST_CYCLE["M15"]

    return jsonify({
        "watcher_retry_fix_version": WATCHER_RETRY_FIX_VERSION,
        "last_watcher_cycle": watcher_cycle,

        "status": "online",
        "timeframe": "M15",
        "m15_live_watcher_enabled": M15_LIVE_WATCHER_ENABLED,
        "m15_live_submission_enabled": M15_LIVE_SUBMISSION_ENABLED,
        "eurjpy_m15_short_live_enabled": EURJPY_M15_SHORT_LIVE_ENABLED,
        "audusd_m15_short_live_enabled": AUDUSD_M15_SHORT_LIVE_ENABLED,
        "audjpy_m15_long_live_enabled": AUDJPY_M15_LONG_LIVE_ENABLED,
        "audjpy_m15_short_live_enabled": AUDJPY_M15_SHORT_LIVE_ENABLED,
        "audjpy_h1_long_tight_live_enabled": AUDJPY_H1_LONG_TIGHT_LIVE_ENABLED,
        "audjpy_h1_short_primary_live_enabled": AUDJPY_H1_SHORT_PRIMARY_LIVE_ENABLED,
        "pair_gate_enabled": False,
        "audjpy_pair_gate_location": (
            "executor: AUD_JPY opposite-direction gate; "
            "same-direction strategy overlaps allowed"
        ),
        "pyramiding_guard_location": "executor_per_strategy_id",
        "risk_target": "executor default 1% NAV; EUR_JPY_M15_SHORT override 0.75%",
        "strategies": sorted(M15_STRATEGY_IDS),
        "last_processed": processed,
    })


@app.route("/m15-strategy-test/<instrument>")
def m15_strategy_test(instrument):
    instrument = instrument.upper()

    if instrument not in M15_PAIR_IDS:
        return jsonify({
            "status": "error",
            "message": f"{instrument} is not an M15 portfolio pair",
        }), 400

    try:
        result = evaluate_latest_m15_pair(instrument)
        output = dict(result)
        output["signal_start_utc"] = iso_utc(
            output["signal_start_utc"]
        )
        output["signal_close_utc"] = iso_utc(
            output["signal_close_utc"]
        )

        serialised = []
        for signal in output["signals"]:
            row = dict(signal)
            row["signal_start_utc"] = iso_utc(row["signal_start_utc"])
            row["signal_close_utc"] = iso_utc(row["signal_close_utc"])
            serialised.append(row)
        output["signals"] = serialised

        return jsonify({
            "status": "success",
            "mode": "READ_ONLY",
            "orders_sent": False,
            **output,
        })

    except Exception as error:
        return jsonify({
            "status": "error",
            "mode": "READ_ONLY",
            "orders_sent": False,
            "instrument": instrument,
            "message": str(error),
        }), 500


@app.route("/m15-pipeline-test/<instrument>/<side>")
def m15_pipeline_test(instrument, side):
    instrument = instrument.upper()
    side = side.upper()

    if instrument not in M15_PAIR_IDS:
        return jsonify({
            "status": "error",
            "message": f"{instrument} is not supported",
        }), 400

    try:
        result = evaluate_latest_m15_pair(instrument)
        candidates = [
            x for x in result["signals"]
            if x["side"] == side
        ]

        # This endpoint is for checking a REAL current qualifying payload.
        # It does not fabricate strategy geometry.
        if not candidates:
            return jsonify({
                "status": "no_current_signal",
                "mode": "READ_ONLY",
                "orders_sent": False,
                "instrument": instrument,
                "side": side,
                "message": "Latest completed M15 candle does not qualify.",
            })

        signal = candidates[0]
        payload = build_m15_live_payload(signal)
        payload["pipeline_test"] = True

        result = post_to_executor(payload)

        return jsonify({
            "status": (
                "pipeline_test_passed"
                if result.get("ok")
                else "pipeline_test_failed"
            ),
            "mode": "READ_ONLY",
            "order_submitted": False,
            "strategy_id": signal["strategy_id"],
            "trigger_id": signal["trigger_id"],
            "executor_result": result,
        })

    except Exception as error:
        return jsonify({
            "status": "error",
            "mode": "READ_ONLY",
            "orders_sent": False,
            "instrument": instrument,
            "side": side,
            "message": str(error),
        }), 500




# ==================================================
# START BACKGROUND WATCHER
# ==================================================

eurchf_initialize()
start_live_watcher_once()
start_m15_live_watcher_once()


# ==================================================
# LOCAL START
# ==================================================

if __name__ == "__main__":

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

