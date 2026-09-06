
# ============================================================
# GBP/USD M15 SHORT — FINAL LOCKED STRATEGY BLOCK
# ============================================================
#
# LOCKED PARAMETERS
#
# Instrument:
#   OANDA:GBPUSD
#
# Timeframe:
#   M15
#
# Side:
#   SHORT ONLY
#
# Signal:
#   Exact bearish engulfing
#     - previous candle bullish
#     - current candle bearish
#     - current open >= previous close
#     - current close <= previous open
#
# Candle quality:
#   - signal body >= 1.00 * ATR14
#
# Structure:
#   - prior 180 M15 bars
#   - current signal high must be within 0.05 * ATR14
#     of the highest high from those PRIOR 180 bars
#   - current signal candle excluded from structure lookback
#
# Time filter:
#   - signal timestamp = candle OPEN
#   - America/New_York
#   - exclude NY hours 06:00–06:59
#   - exclude NY hours 07:00–07:59
#   - all other hours allowed
#
# Weekdays:
#   - no weekday exclusions
#
# Exit:
#   - RR = 3.00
#   - stop = signal high + 10 ticks
#
# Historical development cost:
#   - 1.0 pip adverse short fill
#   - reference entry remains signal close for target calculation
#
# Execution conventions:
#   - pyramiding = 0
#   - exits begin from NEXT candle
#   - exact exit-candle signal is eligible
#   - same-bar short tie:
#       if high is closer to candle open => STOP first
#       otherwise TARGET first
#
# Indicator:
#   - ATR14 = Wilder / RMA
#   - SMA-seeded
#
# Final local confirmation at 1.0 pip:
#   - 53 trades
#   - PF 2.304
#   - +37.80R
#   - expectancy +0.713R/trade
#   - max DD -6R
#   - DEV PF 2.10
#   - VALIDATION PF 2.45
#   - last 5Y PF 2.28
#   - last 2Y PF 2.70
#   - rolling 2Y positive windows 86.4%
#   - rolling 3Y positive windows 97.0%
#
# Strategy ID:
#   GBP_USD_M15_SHORT
#
# DO NOT ALTER WITHOUT EXPLICITLY REOPENING RESEARCH
# ============================================================


STRATEGY = {
    "strategy_id": "GBP_USD_M15_SHORT",

    "instrument": "GBP_USD",
    "timeframe": "M15",
    "side": "SELL",

    # Exact bearish engulfing:
    # previous bullish
    # current bearish
    # current open >= previous close
    # current close <= previous open
    "pattern": "EXACT_BEARISH_ENGULFING",

    # Candle quality
    "minimum_body_atr": 1.00,

    # Structure
    "structure_lookback": 180,
    "maximum_structure_distance_atr": 0.05,

    # Time filter
    "timezone": "America/New_York",
    "excluded_ny_hours": {6, 7},
    "excluded_weekdays": set(),

    # Risk / reward
    "reward_risk": 3.00,

    # Stop
    "stop_buffer_ticks": 10,

    # Historical research cost convention
    "development_adverse_cost_pips": 1.00,

    # Execution conventions
    "pyramiding": 0,
    "process_orders_on_close": True,
    "exit_checks_start_next_bar": True,
    "exit_candle_signal_eligible": True,

    # Same-bar ambiguity convention for SHORT:
    # if high is closer to candle open -> stop first
    # otherwise -> target first
    "same_bar_tie_rule": "SHORT_HIGH_CLOSER_STOP_ELSE_TARGET",

    # Indicator convention
    "atr_length": 14,
    "atr_method": "WILDER_RMA_SMA_SEEDED",

    # Reference-price convention
    "target_reference_entry": "SIGNAL_CLOSE",
    "stop_reference": "SIGNAL_HIGH_PLUS_BUFFER",
}


def exact_bearish_engulfing(previous, current):
    """
    previous/current are dict-like candle objects with:
        open, high, low, close

    Exact locked bearish engulfing definition.
    """
    return (
        previous["close"] > previous["open"]
        and current["close"] < current["open"]
        and current["open"] >= previous["close"]
        and current["close"] <= previous["open"]
    )


def passes_locked_filters(
    candles,
    index,
    atr14_values,
    ny_hour,
):
    """
    Evaluate the LOCKED GBP/USD M15 SHORT signal conditions.

    candles:
        chronological M15 candle list

    index:
        index of current signal candle

    atr14_values:
        aligned ATR14 Wilder/RMA values

    ny_hour:
        current signal candle OPEN hour in America/New_York

    Returns:
        bool
    """

    if index < 180:
        return False

    previous = candles[index - 1]
    current = candles[index]

    # Exact bearish engulf
    if not exact_bearish_engulfing(
        previous,
        current,
    ):
        return False

    atr = atr14_values[index]

    if atr is None or atr <= 0:
        return False

    # Body >= 1.00 ATR14
    body = (
        current["open"]
        - current["close"]
    )

    if body < 1.00 * atr:
        return False

    # Structure:
    # prior 180 bars ONLY, current excluded.
    prior_high = max(
        candle["high"]
        for candle in candles[
            index - 180:index
        ]
    )

    structure_distance_atr = (
        abs(
            current["high"]
            - prior_high
        )
        / atr
    )

    if structure_distance_atr > 0.05:
        return False

    # Exclude NY 06:00 and 07:00 signal-open hours
    if ny_hour in {6, 7}:
        return False

    return True


def locked_trade_levels(
    signal_candle,
    tick_size=0.00001,
):
    """
    Return locked reference trade levels.

    Target is based on REFERENCE signal close risk.
    Historical/live execution cost treatment is handled separately.
    """

    reference_entry = (
        signal_candle["close"]
    )

    stop = (
        signal_candle["high"]
        + 10 * tick_size
    )

    reference_risk = (
        stop
        - reference_entry
    )

    if reference_risk <= 0:
        return None

    target = (
        reference_entry
        - 3.00 * reference_risk
    )

    return {
        "reference_entry":
            reference_entry,
        "stop":
            stop,
        "target":
            target,
        "reference_risk":
            reference_risk,
        "reward_risk":
            3.00,
    }
