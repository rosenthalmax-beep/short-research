"""Conditional GBP NAV/whole-unit/margin replay of all raw qualifications.

Every scenario replays gates from scratch. Source gaps and proxy prices remain
assumptions; this module never labels its account curve a broker reconstruction.
"""
import heapq
import math
from collections import Counter, defaultdict
from datetime import timedelta
from . import admissions
from . import core as e
from .funding import rollovers, MissingHistoricalRate
from .market import QuoteUnavailable


def cash(value):
    e.require(math.isfinite(value), 'Non-finite account cash')
    return round(value, 4)


def fraction(signal):
    return .0075 if signal['strategy_id'] == 'EUR_JPY_M15_SHORT' else .01


def converted(amount, mid_factor, currency, markup):
    multiplier = 1 if currency == 'GBP' else 1 - markup if amount >= 0 else 1 + markup
    result = amount * mid_factor * multiplier
    return result, result - amount * mid_factor


class Replay:
    def __init__(self, signals, path_for, market, funding, instruments, scenario,
                 funding_mode, markup, initial=100., progress=lambda **x: None,
                 start='2005-01-01T00:00:00Z', end='2026-09-20T18:29:00Z', keep_nav=False, nav_sink=None):
        e.require(scenario in admissions.SCENARIOS, 'Unknown admission chronology')
        e.require(initial > 0 and 0 <= markup < .05, 'Invalid GBP balance/markup')
        self.signals, self.path_for, self.market, self.funding = signals, path_for, market, funding
        self.terms = {i['name']: i for i in instruments}
        self.scenario, self.mode, self.markup, self.initial = scenario, funding_mode, markup, initial
        self.progress, self.start, self.end, self.keep_nav = progress, e.dt(start), e.dt(end), keep_nav
        self.nav_sink = nav_sink
        self.balance = initial
        self.active = {}; self.events = []; self.seq = 0; self.mark_scheduled = None
        self.ledger = []; self.trades = []; self.finance_rows = []; self.cash_events = []
        self.nav_rows = []; self.months = {}; self.last_nav = None
        self.uncertain = False; self.first_uncertainty = None; self.causes = Counter()
        self.closed_peak = self.nav_peak = self.liquidation_peak = initial
        self.closed_dd = self.nav_dd = self.liquidation_dd = 0.
        self.worst_nav_at = self.worst_liquidation_at = None
        self.maximum_margin_ratio = 0.; self.max_positions = 0; self.nav_observations = 0
        self.marker_lags = 0; self.processed = 0; self.entries_processed = 0
        self.total_price = self.total_finance = self.total_conversion = 0.
        self.risk_rounding_shortfall = 0.; self.total_risk_budget = 0.
        self.margin_liquidations = 0; self.critical_stale_count = 0
        self.cutoff_valuation = None

    def flag(self, cause, when):
        self.uncertain = True; self.causes[cause] += 1
        if self.first_uncertainty is None:
            self.first_uncertainty = dict(time=e.stamp(when), cause=cause)

    def push(self, when, priority, kind, data=None):
        self.seq += 1
        heapq.heappush(self.events, (e.dt(when), priority, self.seq, kind, data))

    def valuation(self, when):
        upl_mid = upl_sided = margin = 0.
        quote_cache = {}; fx_cache = {}
        def fx(currency):
            if currency not in fx_cache:
                fx_cache[currency] = self.market.fx_mid(currency, when)
            return fx_cache[currency]
        for p in self.active.values():
            s = p['signal']; base, quote = s['pair'].split('_')
            if s['pair'] not in quote_cache:
                quote_cache[s['pair']] = self.market.quote(s['pair'], when)
            price = quote_cache[s['pair']]
            sign = 1 if s['side'] == 'BUY' else -1
            upl_mid += p['units'] * sign * (price['mid'] - p['fill']) * fx(quote)
            executable = price['bid'] if sign > 0 else price['ask']
            value, _ = converted(p['units'] * sign * (executable - p['fill']), fx(quote), quote, self.markup)
            upl_sided += value
            margin += p['units'] * fx(base) * float(self.terms[s['pair']]['marginRate'])
        nav = cash(self.balance + upl_mid)
        return dict(balance_gbp=self.balance, nav_mid_gbp=nav,
                    liquidation_equity_gbp=cash(self.balance + upl_sided),
                    margin_used_gbp=margin, margin_available_gbp=nav - margin,
                    margin_closeout_ratio=.5 * margin / nav if nav > 0 else (1. if margin else 0.))

    def observe(self, when, reason, liquidate=True):
        values = self.valuation(when)
        if getattr(self.market, 'source_disagreement_seen', False):
            self.flag('VALUATION_QUOTES_DIFFER_FROM_FROZEN_EXECUTION_SOURCE', when)
        if getattr(self.market, 'current_stale', False) and not e.conventional_weekend(when):
            self.critical_stale_count += 1
            self.flag('STALE_CAUSAL_M15_QUOTES_OVER_15_MINUTES', when)
        self.nav_observations += 1
        self.nav_peak = max(self.nav_peak, values['nav_mid_gbp'])
        self.liquidation_peak = max(self.liquidation_peak, values['liquidation_equity_gbp'])
        nav_dd = (values['nav_mid_gbp'] / self.nav_peak - 1) * 100
        liq_dd = (values['liquidation_equity_gbp'] / self.liquidation_peak - 1) * 100
        if nav_dd < self.nav_dd:
            self.nav_dd = nav_dd; self.worst_nav_at = e.stamp(when)
        if liq_dd < self.liquidation_dd:
            self.liquidation_dd = liq_dd; self.worst_liquidation_at = e.stamp(when)
        self.maximum_margin_ratio = max(self.maximum_margin_ratio, values['margin_closeout_ratio'])
        row = dict(time=e.stamp(when), event=reason, **values,
                   nav_mid_drawdown_pct=nav_dd, liquidation_drawdown_pct=liq_dd,
                   open_positions=len(self.active), uncertain_account_lineage=self.uncertain,
                   maximum_quote_age_seconds=getattr(self.market, 'current_max_age', 0))
        self.last_nav = row
        if self.keep_nav:
            if self.nav_sink is not None:
                self.nav_sink(row)
            else:
                self.nav_rows.append(row)
        month = e.stamp(when)[:7]
        if month not in self.months:
            self.months[month] = dict(month=month, first_observed_time=e.stamp(when),
                                      opening_observed_nav_gbp=values['nav_mid_gbp'],
                                      lowest_sampled_nav_gbp=values['nav_mid_gbp'],
                                      highest_sampled_nav_gbp=values['nav_mid_gbp'])
        m = self.months[month]
        m.update(last_observed_time=e.stamp(when), closing_observed_nav_gbp=values['nav_mid_gbp'],
                 lowest_sampled_nav_gbp=min(m['lowest_sampled_nav_gbp'], values['nav_mid_gbp']),
                 highest_sampled_nav_gbp=max(m['highest_sampled_nav_gbp'], values['nav_mid_gbp']),
                 cumulative_nav_drawdown_pct=nav_dd, cumulative_liquidation_drawdown_pct=liq_dd,
                 uncertain_account_lineage=self.uncertain)
        if liquidate and self.active and values['margin_closeout_ratio'] >= 1:
            self.flag('SAMPLED_MARGIN_CLOSEOUT_TIME_AND_FILL_PROXY', when)
            self.margin_liquidations += 1
            for key in list(self.active):
                p = self.active[key]; q = self.market.quote(p['signal']['pair'], when)
                self.close(key, when, q['bid'] if p['signal']['side'] == 'BUY' else q['ask'], 'SAMPLED_MARGIN_CLOSEOUT')
            return self.observe(when, 'AFTER_MARGIN_CLOSEOUT', liquidate=False)
        return values

    def close(self, key, when, exit_price, reason):
        if key not in self.active:
            return
        p = self.active[key]; s = p['signal']; quote = s['pair'].split('_')[1]
        sign = 1 if s['side'] == 'BUY' else -1
        quote_pl = p['units'] * sign * (exit_price - p['fill'])
        fx = self.market.fx_mid(quote, when)
        home, conversion = converted(quote_pl, fx, quote, self.markup)
        home = cash(home)
        before = self.balance; self.balance = cash(self.balance + home)
        self.total_price += home; self.total_conversion += conversion
        self.active.pop(key)
        self.closed_peak = max(self.closed_peak, self.balance)
        self.closed_dd = min(self.closed_dd, (self.balance / self.closed_peak - 1) * 100)
        net = cash(home + p['financing'])
        row = dict(signal_key=key, strategy_id=s['strategy_id'], pair=s['pair'], side=s['side'],
                   entry_utc=s['entry_utc'], exit_utc=e.stamp(when), exit_reason=reason,
                   units=p['units'], entry_price=p['fill'], exit_price=exit_price,
                   price_pl_quote=quote_pl, quote_to_gbp_mid_at_exit=fx,
                   price_pl_gbp=home, financing_gbp=p['financing'], net_pl_gbp=net,
                   modeled_price_conversion_gbp=conversion,
                   modeled_total_conversion_gbp=conversion + p['funding_conversion'],
                   entry_risk_budget_gbp=p['budget'], actual_entry_stop_risk_gbp=p['actual_risk'],
                   worst_bound_sizing_risk_gbp=p['sizing_risk'],
                   net_r_actual_entry_risk=net / p['actual_risk'] if p['actual_risk'] else None,
                   net_r_entry_risk_budget=net / p['budget'] if p['budget'] else None,
                   path_status=p['path']['status'], path_certainty=p['path'].get('certainty'),
                   uncertain_account_lineage=self.uncertain,
                   balance_before_gbp=before, balance_after_gbp=self.balance,
                   exit_conversion_quote_max_age_seconds=getattr(self.market, 'current_max_age', 0))
        self.trades.append(row)
        self.cash_events.append(dict(time=e.stamp(when), event='EXIT', signal_key=key,
                                     cash_gbp=home, balance_after_gbp=self.balance,
                                     financing_already_booked_gbp=p['financing'], exit_reason=reason))

    def schedule_mark(self, when):
        if self.active and self.mark_scheduled is None:
            nxt = e.floor_time(when, 900) + timedelta(minutes=15)
            if nxt <= self.end:
                self.mark_scheduled = nxt
                self.push(nxt, 2, 'MARK')

    def entry(self, signal, when):
        key = signal['signal_key']; self.entries_processed += 1
        base = dict(signal_key=key, strategy_id=signal['strategy_id'], pair=signal['pair'],
                    side=signal['side'], entry_utc=signal['entry_utc'],
                    uncertain_account_lineage=self.uncertain)
        blockers = [p for p in self.active.values() if p['signal']['strategy_id'] == signal['strategy_id']]
        gate = 'STRATEGY_PYRAMIDING_0'
        if not blockers:
            blockers = [p for p in self.active.values() if p['signal']['pair'] == signal['pair'] and p['signal']['side'] != signal['side']]
            gate = 'OPPOSITE_PAIR_NONHEDGING'
        if blockers:
            self.ledger.append(dict(base, decision='BLOCKED', reason=gate,
                                    blocking_signal_keys=[p['signal']['signal_key'] for p in blockers]))
            return
        path = self.path_for(signal)
        if path['status'] == 'ENTRY_PROXY_REJECTED':
            self.ledger.append(dict(base, decision='ENTRY_PROXY_REJECTED', reason=path['entry_status']))
            return
        if path['status'] in ('UNRESOLVED_ENTRY', 'ERROR', 'NEEDS_CANDLE_DATA'):
            self.flag('UNKNOWN_ENTRY_OMITTED_ACCOUNT_LINEAGE', when)
            self.ledger.append(dict(base, decision='UNRESOLVED_ENTRY_OMITTED', reason=path.get('entry_status', path['status']), uncertain_account_lineage=True))
            return
        nav = self.observe(when, 'BEFORE_ENTRY')['nav_mid_gbp']
        if nav <= 0:
            self.ledger.append(dict(base, decision='REJECTED_NONPOSITIVE_NAV', reason='NO_RISK_BUDGET'))
            return
        quote = signal['pair'].split('_')[1]; base_currency = signal['pair'].split('_')[0]
        fx_quote = self.market.fx_mid(quote, when)
        loss_factor = fx_quote * (1 if quote == 'GBP' else 1 + self.markup)
        # Deliberately use the executor's floating point subtraction and floor.
        distance = abs(float(path['model_price_bound']) - float(path['model_stop']))
        budget = nav * fraction(signal)
        units = math.floor(budget / (distance * loss_factor))
        term = self.terms[signal['pair']]
        if units < float(term.get('minimumTradeSize', 1)):
            self.ledger.append(dict(base, decision='REJECTED_BELOW_MINIMUM_UNITS', reason='WHOLE_UNIT_SIZE', nav_gbp=nav, raw_budget_gbp=budget))
            return
        if units > float(term['maximumOrderUnits']):
            self.ledger.append(dict(base, decision='REJECTED_MAXIMUM_ORDER_UNITS', reason='BROKER_ORDER_LIMIT', units=units))
            return
        fill = float(path['model_fill']); sign = 1 if signal['side'] == 'BUY' else -1
        q = self.market.quote(signal['pair'], when)
        native_difference = None
        if path.get('model_bid') is not None and path.get('model_ask') is not None:
            native_difference = max(abs(q['bid'] - float(path['model_bid'])), abs(q['ask'] - float(path['model_ask'])))
            if native_difference > 1.01 * float(e.tick(signal['pair'])):
                self.flag('NATIVE_ENTRY_VS_M15_VALUATION_QUOTE_DISAGREEMENT', when)
        base_fx = self.market.fx_mid(base_currency, when)
        current = self.valuation(when)
        extra_margin = units * base_fx * float(term['marginRate'])
        post_nav = cash(nav + units * sign * (q['mid'] - fill) * fx_quote)
        if post_nav < current['margin_used_gbp'] + extra_margin:
            self.ledger.append(dict(base, decision='REJECTED_INSUFFICIENT_MARGIN', reason='MODELED_POST_FILL_NAV_VS_MARGIN',
                                    units=units, nav_gbp=nav, modeled_post_fill_nav_gbp=post_nav,
                                    modeled_post_fill_margin_gbp=current['margin_used_gbp'] + extra_margin))
            return
        if path['status'] not in ('CLOSED_RESOLVED', 'OPEN_AT_CUTOFF'):
            self.flag('OBSERVED_EXIT_AFTER_UNEXPLAINED_GAP', when)
        release = self.end if path.get('kind') == 'OPEN_AT_FIXED_CUTOFF' else admissions.release_time(path, self.scenario)
        e.require(when <= release <= self.end, 'Modeled release outside entry/cutoff interval')
        exit_price = path.get('exit_price')
        if path.get('kind') == 'AMBIGUOUS_BOTH':
            exit_price = path['model_stop']
            self.flag('AMBIGUOUS_BARRIER_STOP_FIRST_ASSUMPTION', when)
        if exit_price is None and path.get('kind') != 'OPEN_AT_FIXED_CUTOFF':
            self.flag('UNPRICED_EXIT', when)
            raise QuoteUnavailable('Unpriced observed exit for ' + key)
        sizing_risk = units * distance * loss_factor
        actual_risk = units * abs(fill - float(path['model_stop'])) * loss_factor
        self.total_risk_budget += budget; self.risk_rounding_shortfall += budget - sizing_risk
        self.active[key] = dict(signal=signal, path=path, units=units, fill=fill, budget=budget,
                                actual_risk=actual_risk, sizing_risk=sizing_risk,
                                financing=0., funding_conversion=0.)
        self.max_positions = max(self.max_positions, len(self.active))
        self.ledger.append(dict(base, decision='ADMITTED', units=units, nav_gbp=nav,
                                risk_budget_gbp=budget, sizing_risk_gbp=sizing_risk,
                                quote_to_gbp_loss_factor=loss_factor, base_to_gbp_mid=base_fx,
                                native_vs_m15_entry_quote_max_difference=native_difference,
                                path_status=path['status'], modeled_release_utc=e.stamp(release),
                                uncertain_account_lineage=self.uncertain))
        self.cash_events.append(dict(time=e.stamp(when), event='ENTRY', signal_key=key,
                                     cash_gbp=0, balance_after_gbp=self.balance, units=units,
                                     risk_budget_gbp=budget, sizing_risk_gbp=sizing_risk))
        if exit_price is not None:
            if release <= when:
                self.close(key, when, float(exit_price), 'OBSERVED_' + str(path.get('kind')))
            else:
                self.push(release, 0, 'EXIT', (key, float(exit_price), 'OBSERVED_' + str(path.get('kind'))))
        self.observe(when, 'AFTER_ENTRY')
        self.schedule_mark(when)

    def finance(self, when):
        if not self.active or when.astimezone(e.NY).weekday() >= 5:
            return
        # Gather all terms/conversions before applying any cash. A missing rate
        # stops the scenario without a partially applied financing transaction.
        pending = []
        for key, p in self.active.items():
            s = p['signal']; terms = self.funding.terms(self.mode, s['pair'], s['side'], when)
            base_currency = s['pair'].split('_')[0]
            # Rate is per unit of BASE currency, not margin or stop-risk cash.
            base_amount = p['units'] * terms['annual_rate'] * terms['charged_days'] / 365
            fx = self.market.fx_mid(base_currency, when) if base_amount else 1.
            amount, fee = converted(base_amount, fx, base_currency, self.markup)
            pending.append((key, p, terms, base_amount, fx, cash(amount), fee))
        for key, p, terms, base_amount, fx, amount, fee in pending:
            self.balance = cash(self.balance + amount)
            p['financing'] = cash(p['financing'] + amount); p['funding_conversion'] += fee
            self.total_finance += amount; self.total_conversion += fee
            self.finance_rows.append(dict(time=e.stamp(when), ny_date=when.astimezone(e.NY).date().isoformat(),
                                          signal_key=key, pair=p['signal']['pair'], side=p['signal']['side'],
                                          strategy_id=p['signal']['strategy_id'],
                                          units=p['units'], base_financing=base_amount, base_to_gbp_mid=fx,
                                          financing_gbp=amount, modeled_conversion_gbp=fee, **terms,
                                          balance_after_gbp=self.balance,
                                          uncertain_account_lineage=self.uncertain))
            self.cash_events.append(dict(time=e.stamp(when), event='FINANCING', signal_key=key,
                                         cash_gbp=amount, balance_after_gbp=self.balance))
        self.closed_peak = max(self.closed_peak, self.balance)
        self.closed_dd = min(self.closed_dd, (self.balance / self.closed_peak - 1) * 100)
        self.observe(when, 'AFTER_FINANCING')

    def run(self):
        for order, signal in enumerate(admissions.ordered(self.signals, self.scenario)):
            when = e.dt(signal['entry_utc'])
            e.require(self.start <= when <= self.end, 'Raw signal outside the pinned evaluation window')
            self.push(when, 3, 'ENTRY', signal)
        for when in rollovers(self.start, self.end):
            self.push(when, 1, 'ROLLOVER')
        self.push(self.end, 4, 'CUTOFF')
        status = 'COMPLETE_CONDITIONAL'; stopped = None; last_time = self.start
        try:
            self.market.begin_event(); self.observe(self.start, 'START')
            while self.events:
                when, _, _, kind, data = heapq.heappop(self.events)
                last_time = when; self.market.begin_event()
                if kind == 'ENTRY':
                    self.entry(data, when)
                elif kind == 'EXIT':
                    self.close(data[0], when, data[1], data[2]); self.observe(when, 'AFTER_EXIT')
                elif kind == 'ROLLOVER':
                    self.finance(when)
                elif kind == 'MARK':
                    self.mark_scheduled = None
                    if self.active:
                        if not e.conventional_weekend(when):
                            self.observe(when, 'M15_SAMPLE')
                        self.schedule_mark(when)
                else:
                    self.cutoff_valuation = self.observe(when, 'CUTOFF')
                if getattr(self.market, 'current_stale', False) and not e.conventional_weekend(when):
                    self.flag('STALE_CAUSAL_M15_QUOTES_OVER_15_MINUTES', when)
                self.processed += 1
                if self.processed % 500 == 0 or kind == 'ENTRY' and self.entries_processed % 50 == 0:
                    self.progress(scenario=self.scenario, funding_mode=self.mode,
                                  qualifications_processed=self.entries_processed, qualifications_required=len(self.signals),
                                  through_utc=e.stamp(when), active_positions=len(self.active),
                                  account_events_processed=self.processed)
        except MissingHistoricalRate as exc:
            status = 'STOPPED_MISSING_HISTORICAL_RATE'; stopped = str(exc)
        except QuoteUnavailable as exc:
            status = 'STOPPED_MISSING_QUOTE_OR_UNPRICED_EXIT'; stopped = str(exc)
        decisions = Counter(r['decision'] for r in self.ledger)
        values = [r['net_r_actual_entry_risk'] for r in self.trades if r['net_r_actual_entry_risk'] is not None]
        wins = sum(r['net_pl_gbp'] > 0 for r in self.trades)
        pos = sum(r['net_pl_gbp'] for r in self.trades if r['net_pl_gbp'] > 0)
        neg = -sum(r['net_pl_gbp'] for r in self.trades if r['net_pl_gbp'] < 0)
        positive_r = sum(v for v in values if v > 0)
        negative_r = -sum(v for v in values if v < 0)
        years = (self.end - self.start).total_seconds() / (365.25 * 86400)
        complete = status == 'COMPLETE_CONDITIONAL' and self.cutoff_valuation is not None
        ending_nav = self.cutoff_valuation['nav_mid_gbp'] if complete else None
        ending_liq = self.cutoff_valuation['liquidation_equity_gbp'] if complete else None
        summary = dict(status=status, stopped_reason=stopped, last_processed_utc=e.stamp(last_time),
                       scenario=self.scenario, financing_mode=self.mode, initial_balance_gbp=self.initial,
                       conversion_factor_markup=self.markup,
                       qualifications=len(self.signals), qualifications_processed=self.entries_processed,
                       decisions=dict(decisions), closed_trades=len(self.trades), open_positions=len(self.active),
                       net_win_rate_pct=100 * wins / len(self.trades) if self.trades else None,
                       mean_net_r_actual_entry_risk=sum(values) / len(values) if values else None,
                       net_r_sum_actual_entry_risk=sum(values),
                       net_r_profit_factor=positive_r / negative_r if negative_r else None,
                       cash_profit_factor=pos / neg if neg else None,
                       price_pl_gbp=cash(self.total_price), financing_gbp=cash(self.total_finance),
                       net_booked_cash_gbp=cash(self.balance - self.initial),
                       modeled_conversion_component_gbp=self.total_conversion,
                       conversion_already_in_price_pl_and_financing=True,
                       ending_balance_gbp=self.balance, conditional_ending_nav_gbp=ending_nav,
                       conditional_ending_liquidation_equity_gbp=ending_liq,
                       conditional_nav_cagr_pct=((ending_nav / self.initial) ** (1 / years) - 1) * 100 if complete and ending_nav > 0 and years > 0 else None,
                       conditional_liquidation_equity_cagr_pct=((ending_liq / self.initial) ** (1 / years) - 1) * 100 if complete and ending_liq > 0 and years > 0 else None,
                       observed_closed_balance_drawdown_pct=self.closed_dd,
                       sampled_mid_nav_drawdown_pct=self.nav_dd,
                       sampled_liquidation_equity_drawdown_pct=self.liquidation_dd,
                       worst_sampled_mid_drawdown_time=self.worst_nav_at,
                       worst_sampled_liquidation_drawdown_time=self.worst_liquidation_at,
                       nav_observations=self.nav_observations,
                       sampling='M15 opens plus entry/exit/NY17 financing events; no intrabar worst-equity guarantee',
                       maximum_observed_margin_closeout_ratio=self.maximum_margin_ratio,
                       modeled_margin_liquidations=self.margin_liquidations, max_positions=self.max_positions,
                       whole_unit_risk_budget_shortfall_gbp=self.risk_rounding_shortfall,
                       fraction_of_total_entry_budgets_unused_by_floor=self.risk_rounding_shortfall / self.total_risk_budget if self.total_risk_budget else None,
                       uncertainty_causes=dict(self.causes), first_uncertainty=self.first_uncertainty,
                       uncertain_account_lineage=self.uncertain,
                       historical_margin_and_conversion_fee_schedules_known=False,
                       exact_broker_nav_and_historical_home_conversion_reconstructed=False,
                       actual_historical_financing_known=False, full_portfolio_validated=False,
                       current_terms_historical_price_replay=True,
                       NAV_basis='MID_MARK_TO_MARKET_FOR_V20_RISK_AND_MARGIN; SEPARATE_SIDED_LIQUIDATION_EQUITY',
                       omitted_costs='No additional commission (zero in supplied fills), tax, deposits, withdrawals or unused-margin interest; no GSLO.',
                       simultaneous_ordering='Existing exits, financing, M15 mark, raw strategy entries; own immediate exit after entry. Conditional ordering, not return bounds.')
        # Cash identity is exact to the saved GBP 0.0001 bookkeeping precision.
        e.require(abs(cash(self.initial + self.total_price + self.total_finance) - self.balance) <= .0002,
                  'GBP cash ledger does not reconcile')
        return dict(summary=summary, ledger=self.ledger, trades=self.trades,
                    financing=self.finance_rows, cash_events=self.cash_events,
                    monthly_observations=list(self.months.values()), nav_samples=self.nav_rows,
                    open_positions=[dict(signal_key=k, units=p['units'], entry_price=p['fill'], financing_gbp=p['financing']) for k, p in self.active.items()])
