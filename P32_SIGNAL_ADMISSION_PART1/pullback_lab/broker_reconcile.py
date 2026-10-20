"""Independently reconcile the saved broker cash records; never infer live NAV."""
import math
import statistics
from collections import defaultdict


def number(value):
    result = float(value or 0)
    if not math.isfinite(result):
        raise ValueError('Non-finite broker amount')
    return result


def factor(record, key):
    item = record.get('homeConversionFactors', {}).get(key)
    return number(item['factor']) if item else None


def reconcile(evidence):
    txs = sorted(evidence['transactions'], key=lambda t: int(t['id']))
    if len({t['id'] for t in txs}) != len(txs):
        raise ValueError('Duplicate broker transactions')
    terms = {i['name']: i for i in evidence['instruments']}
    openings, daily, closed, rows, markups = {}, defaultdict(float), defaultdict(float), [], []
    observed_funding_days = []
    previous_balance = initial = None
    checks = defaultdict(int)

    def check(kind, actual, expected, tolerance, **extra):
        error = actual - expected
        passed = abs(error) <= tolerance
        checks[kind + ('_pass' if passed else '_fail')] += 1
        rows.append(dict(check=kind, actual=actual, expected=expected,
                         difference=error, tolerance=tolerance, passed=passed, **extra))

    for t in txs:
        kind = t['type']
        if kind not in ('ORDER_FILL', 'DAILY_FINANCING'):
            # Other supplied records have no cash movement. Unknown cash types
            # are rejected rather than silently dropped.
            if 'accountBalance' in t:
                raise ValueError('Unsupported cash transaction type: ' + kind)
            continue
        delta = number(t.get('financing'))
        if kind == 'ORDER_FILL':
            delta += number(t.get('pl')) - number(t.get('commission')) - number(t.get('guaranteedExecutionFee'))
            for prefix in ('Base', 'Quote'):
                gain, loss = factor(t, 'gain' + prefix + 'Home'), factor(t, 'loss' + prefix + 'Home')
                currency = t['instrument'].split('_')[0 if prefix == 'Base' else 1]
                if gain and loss and currency != 'GBP':
                    markups.append((loss - gain) / (loss + gain))
            q = number(t.get('quotePL'))
            if q:
                f = factor(t, 'gainQuoteHome' if q > 0 else 'lossQuoteHome')
                if f is None:
                    raise ValueError('Missing broker P/L conversion factor')
                check('QUOTE_PL_TO_GBP', number(t['pl']), q * f, .000151, transaction_id=t['id'])
            item = t.get('tradeOpened')
            if item:
                openings[item['tradeID']] = dict(pair=t['instrument'], units=number(item['units']), price=number(item['price']))
                gain, loss = factor(t, 'gainBaseHome'), factor(t, 'lossBaseHome')
                if gain is None or loss is None:
                    raise ValueError('Missing base/home margin factors')
                expected = abs(number(item['units'])) * (gain + loss) / 2 * number(terms[t['instrument']]['marginRate'])
                check('INITIAL_MARGIN_MID_CONVERSION', number(item['initialMarginRequired']), expected, .000151,
                      transaction_id=t['id'], trade_id=item['tradeID'])
                check('WHOLE_UNITS', abs(number(item['units'])), math.floor(abs(number(item['units']))), 0,
                      transaction_id=t['id'])
            items = list(t.get('tradesClosed', []))
            if t.get('tradeReduced'):
                items.append(t['tradeReduced'])
            reconstructed_quote = 0.
            for item in items:
                trade_id = item['tradeID']
                if trade_id not in openings:
                    raise ValueError('Close predates saved opening: ' + trade_id)
                op = openings[trade_id]
                movement = (number(item['price']) - op['price']) * (1 if op['units'] > 0 else -1)
                reconstructed_quote += abs(number(item['units'])) * movement
                closed[trade_id] += number(item['realizedPL']) + number(item.get('financing')) - number(item.get('guaranteedExecutionFee'))
            if items:
                check('UNITS_TIMES_EXIT_MINUS_ENTRY', q, reconstructed_quote, .000001,
                      transaction_id=t['id'])
        else:
            allocated = 0.
            for position in t.get('positionFinancings', []):
                for item in position.get('openTradeFinancings', []):
                    amount = number(item['financing']); allocated += amount
                    daily[item['tradeID']] += amount
                    base = number(item.get('baseFinancing'))
                    rate = number(item.get('financingRate'))
                    opened = openings.get(item['tradeID'])
                    if opened and rate:
                        implied = base * 365 / (abs(opened['units']) * rate)
                        observed_funding_days.append(dict(transaction_id=t['id'], trade_id=item['tradeID'],
                                                          time=t['time'], pair=position['instrument'],
                                                          inferred_charged_days=implied,
                                                          basis='Inferred from broker base financing, recorded annual rate and units; not an independently sourced holiday calendar'))
                    f = factor(position, 'gainBaseHome' if base >= 0 else 'lossBaseHome')
                    if f is not None:
                        check('FINANCING_BASE_TO_GBP', amount, base * f, .000151,
                              transaction_id=t['id'], trade_id=item['tradeID'])
            check('DAILY_FINANCING_ALLOCATION', number(t['financing']), allocated, .000151,
                  transaction_id=t['id'])
        balance = number(t['accountBalance'])
        if previous_balance is None:
            initial = balance - delta
        else:
            check('BROKER_BALANCE_CHANGE', balance, previous_balance + delta, .000151,
                  transaction_id=t['id'])
        previous_balance = balance
    for t in evidence['closed_trades']:
        trade_id = str(t['oanda_trade_id'])
        if trade_id not in closed:
            raise ValueError('Outcome absent from saved transactions')
        check('CLOSED_TRADE_NET_INCLUDING_DAILY_FINANCING', number(t['net_pl_including_financing']),
              closed[trade_id] + daily[trade_id], .000251, trade_id=trade_id)
    check('ENDING_SAVED_ACCOUNT_BALANCE', number(evidence['account']['balance']), previous_balance, .000151)
    fraction = statistics.median(markups) if markups else None
    if fraction is None or not 0 <= fraction < .05:
        raise ValueError('Cannot establish saved conversion factor spread')
    failed = sum(v for k, v in checks.items() if k.endswith('_fail'))
    summary = dict(status='PASS' if not failed else 'FAILED', checks=dict(checks), failures=failed,
                   cash_transactions=sum(t['type'] in ('ORDER_FILL', 'DAILY_FINANCING') for t in txs),
                   source_collected_at_utc=evidence['collected_at_utc'],
                   first_transaction_id=txs[0]['id'], last_transaction_id=txs[-1]['id'],
                   transaction_ids_contiguous=[int(t['id']) for t in txs] == list(range(int(txs[0]['id']), int(txs[-1]['id']) + 1)),
                   reconstructed_starting_balance_gbp=initial, ending_balance_gbp=previous_balance,
                   transaction_slice_net_cash_gbp=previous_balance - initial,
                   daily_financing_gbp=sum(daily.values()),
                   observed_live_financing_day_multipliers=observed_funding_days,
                   observed_symmetric_conversion_factor_markup=fraction,
                   conversion_factor_markup_range=[min(markups), max(markups)],
                   conversion_factor_markup_uniform=max(markups) - min(markups) < .000001,
                   uses_recorded_broker_factors=True, recomputes_broker_pricing_quotes=False,
                   live_nav_sizing_exactly_reproduced=False,
                   limitation='Recorded preview NAV is rounded and preview conversion snapshots are unavailable; exact historical unit sizing/NAV cannot be proved from fill factors.',
                   no_extra_spread_or_conversion_debit=True)
    return summary, rows
