# Pure financial definitions extracted verbatim from the supplied probe.
# No Flask app, watchers, executor client, account client or database code.
from datetime import datetime,timezone,timedelta
from zoneinfo import ZoneInfo
from collections import deque
import math
import time
WEBHOOK_SECRET="REPLAY_ONLY_NO_SUBMISSION_PATH"

NY_TZ = ZoneInfo("America/New_York")


DAILY_ALIGNMENT_HOUR = 17


DAILY_ALIGNMENT_TIMEZONE = "America/New_York"


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


EURCHF_PRIMARY_ID = "EURCHF_H1_SHORT_PRIMARY_L060_D0075_SC035_PC075_RR3P50_V1"


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

