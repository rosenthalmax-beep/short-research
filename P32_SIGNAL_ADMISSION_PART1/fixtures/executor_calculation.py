import math
from datetime import datetime, timezone
RISK_PERCENT = 0.01
STRATEGY_RISK_PERCENT = {'EUR_JPY_M15_SHORT': 0.0075}
MAX_ENTRY_DEVIATION_TICKS = 10
PRICE_PRECISION = {p: 3 if p.endswith('_JPY') else 5 for p in ('AUD_JPY','AUD_USD','EUR_CHF','EUR_GBP','EUR_JPY','EUR_USD','GBP_USD','USD_CAD','USD_JPY')}
TICK_SIZE = {p: 10 ** (-v) for p,v in PRICE_PRECISION.items()}
def risk_percent_for_strategy(strategy_id):
    return STRATEGY_RISK_PERCENT.get(strategy_id, RISK_PERCENT)

def round_price(instrument, price):
    return round(float(price), PRICE_PRECISION[instrument])

def calculate_execution(instrument, side, reference_entry, stop, rr, strategy_id=None, price_snapshot=None, account_snapshot=None):
    tick_size = TICK_SIZE[instrument]
    if instrument == 'EUR_CHF' and price_snapshot is None:
        snapshot = eurchf_quote_snapshot()
        price_snapshot = (snapshot['bid'], snapshot['ask'], snapshot['chf_to_account_loss'])
    live_bid, live_ask, quote_to_home = price_snapshot if price_snapshot is not None else get_price_and_conversion(instrument)
    live_bid = round_price(instrument, live_bid)
    live_ask = round_price(instrument, live_ask)
    quote_observed_at = datetime.now(timezone.utc).isoformat()
    account = account_snapshot if account_snapshot is not None else get_account_summary()
    nav = float(account['NAV'])
    if instrument == 'EUR_CHF' and (not (math.isfinite(nav) and nav > 0)):
        raise ValueError('EUR_CHF sizing requires finite positive NAV')
    risk_percent = risk_percent_for_strategy(strategy_id)
    risk_budget = nav * risk_percent
    if side == 'BUY':
        live_price = live_ask
        execution_side_price = 'ASK'
        adverse_price_movement = max(0.0, live_ask - reference_entry)
        adverse_deviation_ticks = adverse_price_movement / tick_size
        price_bound = round_price(instrument, reference_entry + MAX_ENTRY_DEVIATION_TICKS * tick_size)
        if adverse_deviation_ticks > MAX_ENTRY_DEVIATION_TICKS + 0.0001:
            raise ExecutionPreviewRejected(f'Adverse BUY execution deviation too large: {round(adverse_deviation_ticks, 2)} ticks', execution_rejection_evidence(instrument, side, live_bid, live_ask, quote_observed_at, nav, risk_budget, reference_entry, stop, rr, price_bound, adverse_deviation_ticks))
        if stop >= live_ask:
            raise ValueError('BUY stop is not below live ask')
        worst_case_stop_distance = price_bound - stop
        if worst_case_stop_distance <= 0:
            raise ValueError('Invalid BUY worst-case stop distance')
        reference_risk = reference_entry - stop
        if reference_risk <= 0:
            raise ValueError('Invalid BUY reference risk')
        target = round_price(instrument, reference_entry + reference_risk * rr)
        signed_units_multiplier = 1
    elif side == 'SELL':
        live_price = live_bid
        execution_side_price = 'BID'
        adverse_price_movement = max(0.0, reference_entry - live_bid)
        adverse_deviation_ticks = adverse_price_movement / tick_size
        price_bound = round_price(instrument, reference_entry - MAX_ENTRY_DEVIATION_TICKS * tick_size)
        if adverse_deviation_ticks > MAX_ENTRY_DEVIATION_TICKS + 0.0001:
            raise ExecutionPreviewRejected(f'Adverse SELL execution deviation too large: {round(adverse_deviation_ticks, 2)} ticks', execution_rejection_evidence(instrument, side, live_bid, live_ask, quote_observed_at, nav, risk_budget, reference_entry, stop, rr, price_bound, adverse_deviation_ticks))
        if stop <= live_bid:
            raise ValueError('SELL stop is not above live bid')
        worst_case_stop_distance = stop - price_bound
        if worst_case_stop_distance <= 0:
            raise ValueError('Invalid SELL worst-case stop distance')
        reference_risk = stop - reference_entry
        if reference_risk <= 0:
            raise ValueError('Invalid SELL reference risk')
        target = round_price(instrument, reference_entry - reference_risk * rr)
        signed_units_multiplier = -1
    else:
        raise ValueError(f'Unsupported side: {side}')
    if instrument == 'EUR_CHF' and live_bid <= target:
        raise ValueError('Executable bid has already reached/passed the fixed target')
    loss_per_unit_home = worst_case_stop_distance * quote_to_home
    if loss_per_unit_home <= 0:
        raise ValueError('Invalid loss-per-unit calculation')
    raw_units = risk_budget / loss_per_unit_home
    absolute_units = math.floor(raw_units)
    if absolute_units < 1:
        raise ValueError('Calculated size is below 1 unit')
    signed_units = absolute_units * signed_units_multiplier
    estimated_risk = absolute_units * worst_case_stop_distance * quote_to_home
    return {'nav': round(nav, 2), 'risk_percent': round(risk_percent * 100, 4), 'risk_budget': round(risk_budget, 4), 'live_bid': live_bid, 'quote_observed_at': quote_observed_at, 'live_ask': live_ask, 'live_price': live_price, 'execution_side_price': execution_side_price, 'price_bound': price_bound, 'deviation_ticks': round(adverse_deviation_ticks, 2), 'stop': round_price(instrument, stop), 'target': target, 'units': signed_units, 'absolute_units': absolute_units, 'estimated_risk': round(estimated_risk, 4)}

class ExecutionPreviewRejected(ValueError):
    """The existing deviation rejection, carrying observation evidence."""

    def __init__(self, message, evidence):
        super().__init__(message)
        self.execution = evidence

def execution_rejection_evidence(pair, side, bid, ask, observed_at, nav, budget, reference, stop, rr, bound, ticks):
    return {'live_bid': bid, 'live_ask': ask, 'live_price': ask if side == 'BUY' else bid, 'execution_side_price': 'ASK' if side == 'BUY' else 'BID', 'quote_observed_at': observed_at, 'nav': round(nav, 4), 'risk_budget': round(budget, 4), 'price_bound': bound, 'deviation_ticks': round(ticks, 2), 'stop': stop, 'target': fallback_target(pair, side, reference, stop, rr)}

def fallback_target(pair, side, reference_entry, stop, rr):
    if side == 'BUY':
        risk = reference_entry - stop
        return round_price(pair, reference_entry + risk * rr)
    if side == 'SELL':
        risk = stop - reference_entry
        return round_price(pair, reference_entry - risk * rr)
    return None
