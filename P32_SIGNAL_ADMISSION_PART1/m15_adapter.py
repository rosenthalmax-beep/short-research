"""M15 predicate body with explicit index and prepared inputs; generated from locked_rules."""
from locked_rules import *

def evaluate_m15_at_index(instrument, m15, i, atr_values, atr_mean20, h1_state, h4_state, daily_state):
    current = m15[i]
    previous = m15[i - 1]
    atr = atr_values[i]
    if atr is None or atr <= 0:
        return {'instrument': instrument, 'signal_start_utc': current['time'], 'signal_close_utc': current['time'] + timedelta(minutes=15), 'signals': [], 'reason': 'ATR_NOT_READY'}
    o = current['open']
    h = current['high']
    l = current['low']
    c = current['close']
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
    prev_body_abs = abs(previous['close'] - previous['open'])
    bull_br = bull_body / prev_body_abs if prev_body_abs > 0 else None
    bear_br = bear_body / prev_body_abs if prev_body_abs > 0 else None
    exact_bull = m15_exact_bullish_engulf(previous, current)
    exact_bear = m15_exact_bearish_engulf(previous, current)
    prev_atr = atr_values[i - 1] if i >= 1 else None
    prev_atr_mean20 = atr_mean20[i - 1] if i >= 1 else None
    compression = prev_atr / prev_atr_mean20 if prev_atr is not None and prev_atr_mean20 is not None and (prev_atr_mean20 > 0) else None
    mom4 = None
    if i >= 17:
        mom4 = (m15[i - 1]['close'] - m15[i - 17]['close']) / atr
    ny = current['time'].astimezone(NY_TZ)
    london = current['time'].astimezone(LONDON_TZ)
    signals = []
    if instrument == 'EUR_USD':
        p165_low = m15_previous_extreme(m15, i, 165, 'low', False)
        p60_high = m15_previous_extreme(m15, i, 60, 'high', True)
        if exact_bull and bull_br is not None and (bull_br >= 1.2) and (bull_atr >= 1.0) and (abs(l - p165_low) / atr <= 0.1) and (ny.weekday() != 1):
            signals.append(m15_signal_record(strategy_id='EUR_USD_M15_LONG', trigger_id='CORE', pair=instrument, side='BUY', candle=current, rr=3.75, stop=l - 10 * 1e-05, atr14_value=atr, diagnostics={'body_ratio': bull_br, 'body_atr': bull_atr, 'structure_distance_atr': abs(l - p165_low) / atr, 'ny_weekday': ny.weekday()}))
        if exact_bear and bear_br is not None and (bear_br >= 1.1) and (bear_atr >= 1.3) and (range_atr >= 1.7) and (abs(h - p60_high) / atr <= 0.225) and (ny.hour in {2, 3}):
            signals.append(m15_signal_record(strategy_id='EUR_USD_M15_SHORT', trigger_id='CORE', pair=instrument, side='SELL', candle=current, rr=3.5, stop=h + 10 * 1e-05, atr14_value=atr, diagnostics={'body_ratio': bear_br, 'body_atr': bear_atr, 'range_atr': range_atr, 'structure_distance_atr': abs(h - p60_high) / atr, 'ny_hour': ny.hour}))
    elif instrument == 'GBP_USD':
        p165_low = m15_previous_extreme(m15, i, 165, 'low', False)
        p100_high = m15_previous_extreme(m15, i, 100, 'high', True)
        if bull and bull_atr >= 1.0 and (close_loc >= 0.75) and (l < p165_low) and (c > p165_low) and (ny.hour in {4, 5, 6, 7}):
            signals.append(m15_signal_record(strategy_id='GBP_USD_M15_LONG', trigger_id='FAILED_BREAKDOWN_RECLAIM', pair=instrument, side='BUY', candle=current, rr=4.25, stop=l - 10 * 1e-05, atr14_value=atr, diagnostics={'body_atr': bull_atr, 'close_location': close_loc, 'prior_165_low': p165_low, 'ny_hour': ny.hour}))
        if exact_bear and bear_br is not None and (bear_br >= 1.3) and (bear_atr >= 1.1) and (range_atr >= 1.6) and (abs(h - p100_high) / atr <= 0.075):
            signals.append(m15_signal_record(strategy_id='GBP_USD_M15_SHORT', trigger_id='GRID_0240', pair=instrument, side='SELL', candle=current, rr=3.0, stop=h + 10 * 1e-05, atr14_value=atr, diagnostics={'body_ratio': bear_br, 'body_atr': bear_atr, 'range_atr': range_atr, 'structure_distance_atr': abs(h - p100_high) / atr}))
    elif instrument == 'USD_JPY':
        p30_low = m15_previous_extreme(m15, i, 30, 'low', False)
        p40_low = m15_previous_extreme(m15, i, 40, 'low', False)
        p165_high = m15_previous_extreme(m15, i, 165, 'high', True)
        h1_ratio_ok = h1_state is not None and h1_state['atr_ratio50'] is not None and (h1_state['atr_ratio50'] >= 0.8)
        if bull and bull_atr >= 1.25 and (l < p30_low) and (c > previous['high']) and (lwb >= 0.25) and (mom4 is not None) and (mom4 <= -1.75) and h1_ratio_ok:
            signals.append(m15_signal_record(strategy_id='USD_JPY_M15_LONG', trigger_id='SWEEP30_RR4.00', pair=instrument, side='BUY', candle=current, rr=4.0, stop=l - 10 * 0.001, atr14_value=atr, diagnostics={'body_atr': bull_atr, 'lower_wick_body': lwb, 'prior_4h_momentum_atr': mom4, 'h1_atr_ratio50': h1_state['atr_ratio50']}))
        core_short = bear and compression is not None and (compression <= 0.8) and (bear_atr >= 1.25) and (range_atr >= 1.6) and (c < p40_low) and (daily_state is not None) and (daily_state['ema200'] is not None) and (daily_state['close'] < daily_state['ema200'])
        comp_short = bear and h > p165_high and (c < p165_high) and (bear_atr >= 1.0) and (close_loc <= 0.2) and (h4_state is not None) and (h4_state['ema100'] is not None) and (h4_state['close'] < h4_state['ema100'])
        if core_short:
            signals.append(m15_signal_record(strategy_id='USD_JPY_M15_SHORT', trigger_id='CORE_GRID_0731', pair=instrument, side='SELL', candle=current, rr=4.75, stop=h + 10 * 0.001, atr14_value=atr, diagnostics={'compression': compression, 'body_atr': bear_atr, 'range_atr': range_atr, 'daily_close': daily_state['close'], 'daily_ema200': daily_state['ema200']}))
        elif comp_short:
            signals.append(m15_signal_record(strategy_id='USD_JPY_M15_SHORT', trigger_id='COMPLEMENT_CAND_0322', pair=instrument, side='SELL', candle=current, rr=3.0, stop=h + 10 * 0.001, atr14_value=atr, diagnostics={'body_atr': bear_atr, 'close_location': close_loc, 'h4_close': h4_state['close'], 'h4_ema100': h4_state['ema100']}))
    elif instrument == 'USD_CAD':
        p165_low = m15_previous_extreme(m15, i, 165, 'low', False)
        p5_high = m15_previous_extreme(m15, i, 5, 'high', True)
        p120_high = m15_previous_extreme(m15, i, 120, 'high', True)
        p20_high = m15_previous_extreme(m15, i, 20, 'high', True)
        core_long = exact_bull and bull_br is not None and (bull_br >= 1.7) and (bull_atr >= 1.25) and (range_atr >= 1.5) and (abs(l - p165_low) / atr <= 0.175) and (daily_state is not None) and (daily_state['ema50'] is not None) and (daily_state['ema200'] is not None) and (daily_state['ema50'] > daily_state['ema200'])
        comp_long = bull and compression is not None and (compression <= 0.7) and (bull_atr >= 1.0) and (range_atr >= 1.5) and (c > p5_high) and (h4_state is not None) and (h4_state['ema100'] is not None) and (h4_state['close'] > h4_state['ema100'])
        if core_long:
            signals.append(m15_signal_record(strategy_id='USD_CAD_M15_LONG', trigger_id='CORE_EMA50_REGIME_BR170', pair=instrument, side='BUY', candle=current, rr=5.0, stop=l - 10 * 1e-05, atr14_value=atr, diagnostics={'body_ratio': bull_br, 'body_atr': bull_atr, 'range_atr': range_atr, 'structure_distance_atr': abs(l - p165_low) / atr, 'daily_ema50': daily_state['ema50'], 'daily_ema200': daily_state['ema200']}))
        elif comp_long:
            signals.append(m15_signal_record(strategy_id='USD_CAD_M15_LONG', trigger_id='COMPLEMENT_H4_COMPRESSION_BREAKOUT', pair=instrument, side='BUY', candle=current, rr=5.25, stop=l - 10 * 1e-05, atr14_value=atr, diagnostics={'compression': compression, 'body_atr': bull_atr, 'range_atr': range_atr, 'prior_5_high': p5_high, 'h4_close': h4_state['close'], 'h4_ema100': h4_state['ema100']}))
        core_short = bear and h > previous['high'] and (l < previous['low']) and (bear_atr >= 0.75) and (close_loc <= 0.35) and (abs(h - p120_high) / atr <= 0.05) and (h4_state is not None) and (h4_state['ema100'] is not None) and (h4_state['close'] < h4_state['ema100'])
        comp_short = bear and h > p20_high and (c < p20_high) and (bear_atr >= 1.25) and (close_loc <= 0.4) and (uwb >= 0.25) and (h1_state is not None) and (h1_state['ema100'] is not None) and (h1_state['close'] < h1_state['ema100'])
        if core_short:
            signals.append(m15_signal_record(strategy_id='USD_CAD_M15_SHORT', trigger_id='CORE_OUTSIDE_H4EMA100_LB120_D005_RR400', pair=instrument, side='SELL', candle=current, rr=4.0, stop=h + 10 * 1e-05, atr14_value=atr, diagnostics={'body_atr': bear_atr, 'close_location': close_loc, 'structure_distance_atr': abs(h - p120_high) / atr, 'h4_close': h4_state['close'], 'h4_ema100': h4_state['ema100']}))
        elif comp_short:
            signals.append(m15_signal_record(strategy_id='USD_CAD_M15_SHORT', trigger_id='COMPLEMENT_HIGH_SWEEP_H1EMA100_LB20_RR350', pair=instrument, side='SELL', candle=current, rr=3.5, stop=h + 10 * 1e-05, atr14_value=atr, diagnostics={'body_atr': bear_atr, 'close_location': close_loc, 'upper_wick_body': uwb, 'h1_close': h1_state['close'], 'h1_ema100': h1_state['ema100']}))
    elif instrument == 'EUR_GBP':
        p60_low = m15_previous_extreme(m15, i, 60, 'low', False)
        p165_low = m15_previous_extreme(m15, i, 165, 'low', False)
        p120_high = m15_previous_extreme(m15, i, 120, 'high', True)
        p100_high = m15_previous_extreme(m15, i, 100, 'high', True)
        core_long = bull and bull_atr >= 1.2 and (l < p60_low) and (c > previous['high']) and (lwb >= 0.25) and (mom4 is not None) and (mom4 <= -1.25) and (ny.hour in {1, 2, 3})
        comp_long = exact_bull and bull_br is not None and (bull_br >= 1.2) and (bull_atr >= 1.1) and (abs(l - p165_low) / atr <= 0.2) and (london.hour in {3, 4, 5, 6, 7})
        if core_long:
            signals.append(m15_signal_record(strategy_id='EUR_GBP_M15_LONG', trigger_id='CORE_SWEEP_DISPLACEMENT', pair=instrument, side='BUY', candle=current, rr=2.75, stop=l - 10 * 1e-05, atr14_value=atr, diagnostics={'body_atr': bull_atr, 'lower_wick_body': lwb, 'prior_4h_momentum_atr': mom4, 'ny_hour': ny.hour}))
        elif comp_long:
            signals.append(m15_signal_record(strategy_id='EUR_GBP_M15_LONG', trigger_id='COMPLEMENT_LONDON_ENGULF_STRUCTURE', pair=instrument, side='BUY', candle=current, rr=2.25, stop=l - 10 * 1e-05, atr14_value=atr, diagnostics={'body_ratio': bull_br, 'body_atr': bull_atr, 'structure_distance_atr': abs(l - p165_low) / atr, 'london_hour': london.hour}))
        core_short = bear and h > p120_high and (c < p120_high) and (bear_atr >= 1.25) and (close_loc <= 0.2) and (uwb >= 0.4) and (london.weekday() != 2)
        comp_short = exact_bear and bear_br is not None and (bear_br >= 1.6) and (bear_atr >= 1.0) and (abs(h - p100_high) / atr <= 0.15) and (h1_state is not None) and (h1_state['ema100'] is not None) and (h1_state['close'] < h1_state['ema100'])
        if core_short:
            signals.append(m15_signal_record(strategy_id='EUR_GBP_M15_SHORT', trigger_id='CORE_HIGH_SWEEP_REJECTION', pair=instrument, side='SELL', candle=current, rr=4.5, stop=h + 10 * 1e-05, atr14_value=atr, diagnostics={'body_atr': bear_atr, 'close_location': close_loc, 'upper_wick_body': uwb, 'london_weekday': london.weekday()}))
        elif comp_short:
            signals.append(m15_signal_record(strategy_id='EUR_GBP_M15_SHORT', trigger_id='COMPLEMENT_BEAR_ENGULF_H1EMA100', pair=instrument, side='SELL', candle=current, rr=2.5, stop=h + 10 * 1e-05, atr14_value=atr, diagnostics={'body_ratio': bear_br, 'body_atr': bear_atr, 'structure_distance_atr': abs(h - p100_high) / atr, 'h1_close': h1_state['close'], 'h1_ema100': h1_state['ema100']}))
    elif instrument == 'EUR_JPY':
        p100_low = m15_previous_extreme(m15, i, 100, 'low', False)
        p40_high = m15_previous_extreme(m15, i, 40, 'high', True)
        p10_high = m15_previous_extreme(m15, i, 10, 'high', True)
        p60_high = m15_previous_extreme(m15, i, 60, 'high', True)
        if exact_bull and bull_br is not None and (bull_br >= 1.5) and (bull_atr >= 0.75) and (p100_low is not None) and (abs(l - p100_low) / atr <= 0.1) and (h1_state is not None) and (h1_state['ema50'] is not None) and (h1_state['ema200'] is not None) and (h1_state['ema50'] > h1_state['ema200']):
            signals.append(m15_signal_record(strategy_id='EUR_JPY_M15_LONG', trigger_id='CORE_ENGULF_H1_EMA50_GT_EMA200', pair=instrument, side='BUY', candle=current, rr=4.25, stop=l - 10 * 0.001, atr14_value=atr, diagnostics={'body_ratio': bull_br, 'body_atr': bull_atr, 'structure_distance_atr': abs(l - p100_low) / atr, 'h1_close': h1_state['close'], 'h1_ema50': h1_state['ema50'], 'h1_ema200': h1_state['ema200']}))
        b_short = bear and p60_high is not None and (h > p60_high) and (c < previous['low']) and (bear_atr >= 1.0) and (uwb >= 0.35) and (mom4 is not None) and (mom4 >= 1.0) and (h1_state is not None) and (h1_state['ema100'] is not None) and (h1_state['close'] < h1_state['ema100'])
        a_short = bear and p40_high is not None and (p10_high is not None) and (h > p40_high) and (c < p10_high) and (bear_atr >= 0.75) and (mom4 is not None) and (mom4 >= 1.25) and (close_loc <= 0.3) and (ny.hour in {16, 17, 18, 19})
        if b_short:
            signals.append(m15_signal_record(strategy_id='EUR_JPY_M15_SHORT', trigger_id='B_HIGH_SWEEP_DISPLACEMENT_H1EMA100', pair=instrument, side='SELL', candle=current, rr=3.0, stop=h + 10 * 0.001, atr14_value=atr, diagnostics={'family': 'HIGH_SWEEP_DISPLACEMENT', 'body_atr': bear_atr, 'upper_wick_body': uwb, 'prior_4h_momentum_atr': mom4, 'prior_60_high': p60_high, 'previous_low': previous['low'], 'h1_close': h1_state['close'], 'h1_ema100': h1_state['ema100'], 'same_candle_priority': 'B'}))
        elif a_short:
            signals.append(m15_signal_record(strategy_id='EUR_JPY_M15_SHORT', trigger_id='A_RALLY_REJECTION_NY16_19', pair=instrument, side='SELL', candle=current, rr=4.75, stop=h + 10 * 0.001, atr14_value=atr, diagnostics={'family': 'RALLY_REJECTION', 'body_atr': bear_atr, 'prior_4h_momentum_atr': mom4, 'close_location': close_loc, 'prior_40_high': p40_high, 'prior_10_high': p10_high, 'ny_hour': ny.hour, 'same_candle_priority': 'B'}))
    elif instrument == 'AUD_USD':
        p120_high = m15_previous_extreme(m15, i, 120, 'high', True)
        if exact_bear and bear_br is not None and (bear_br >= 1.35) and (bear_atr >= 1.0) and (p120_high is not None) and (abs(h - p120_high) / atr <= 0.1) and (h1_state is not None) and (h1_state['ema50'] is not None) and (h1_state['ema200'] is not None) and (h1_state['ema50'] < h1_state['ema200']):
            signals.append(m15_signal_record(strategy_id='AUD_USD_M15_SHORT', trigger_id='CORE_ENGULF_H1_EMA50_LT_EMA200', pair=instrument, side='SELL', candle=current, rr=3.5, stop=h + 10 * 1e-05, atr14_value=atr, diagnostics={'body_ratio': bear_br, 'body_atr': bear_atr, 'structure_distance_atr': abs(h - p120_high) / atr, 'prior_120_high': p120_high, 'h1_complete_at': iso_utc(h1_state['complete_at']), 'h1_ema50': h1_state['ema50'], 'h1_ema200': h1_state['ema200']}))
    elif instrument == 'AUD_JPY':
        p60_low = m15_previous_extreme(m15, i, 60, 'low', False)
        prior_192_momentum = (m15[i - 1]['close'] - m15[i - 193]['close']) / atr if i >= 193 else None
        previous_bear_body = previous['open'] - previous['close']
        current_bull_body = current['close'] - current['open']
        if exact_bull and previous_bear_body > 0 and (current_bull_body >= previous_bear_body) and (bull_atr >= 1.0) and (range_atr >= 1.8) and (p60_low is not None) and (abs(l - p60_low) / atr <= 0.5) and (prior_192_momentum is not None) and (prior_192_momentum >= 0.5):
            signals.append(m15_signal_record(strategy_id='AUD_JPY_M15_LONG', trigger_id='CORE_ENGULF_LB60_D050_RANGE180_MOM192', pair=instrument, side='BUY', candle=current, rr=3.5, stop=l - 10 * 0.001, atr14_value=atr, diagnostics={'geometry': 'CORE_D050_NOT_FREQUENCY_D075', 'body_ratio': current_bull_body / previous_bear_body, 'body_atr': bull_atr, 'range_atr': range_atr, 'structure_distance_atr': abs(l - p60_low) / atr, 'prior_60_low': p60_low, 'prior_192_bar_momentum_atr': prior_192_momentum, 'atr14': atr, 'signal_low': l, 'signal_reference_close': c, 'research_2_4_pip_fills': 'historical assumptions only'}))
        short_ok, short_diag = m15_audjpy_short_core(m15, i, atr)
        if short_ok:
            signals.append(m15_signal_record(strategy_id='AUD_JPY_M15_SHORT', trigger_id='CORE_RALLY_REJECTION_LB60_B150_R225_M150', pair=instrument, side='SELL', candle=current, rr=4.0, stop=h + 10 * 0.001, atr14_value=atr, diagnostics=short_diag))
    else:
        raise ValueError(f'Unsupported M15 pair: {instrument}')
    return {'instrument': instrument, 'signal_start_utc': current['time'], 'signal_close_utc': current['time'] + timedelta(minutes=15), 'signals': [x for x in signals if x is not None]}
