"""Predeclared fresh GBP100 windows; causal end-of-window account censoring."""
from collections import Counter, defaultdict
from statistics import median
import admissions
import execution as e
import reports

POLICIES = ('FIXED_10', 'FIXED_15', 'STOP10_MAX30')
VERSION = 'P32_SMALL_ACCOUNT_ENTRY_LIMIT_V1_2026_10_19'


def windows(start, cutoff):
    """Annual Jan-1 starts, exact five-year duration, followed by recent diagnostic."""
    first, last = e.dt(start), e.dt(cutoff)
    rows = []
    for year in range(first.year, last.year + 1):
        a, b = f'{year:04d}-01-01T00:00:00Z', f'{year+5:04d}-01-01T00:00:00Z'
        if first <= e.dt(a) and e.dt(b) <= last:
            rows.append(dict(window_id=f'FIVE_YEAR_{year}_{year+5}', window_kind='ROLLING_FIVE_YEAR', start=a, end=b))
    a = '2021-01-01T00:00:00Z'
    if first <= e.dt(a) < last:
        rows.append(dict(window_id='RECENT_2021_TO_CUTOFF', window_kind='RECENT_DIAGNOSTIC', start=a, end=e.stamp(last)))
    e.require(bool(rows), 'No complete scheduled replay windows')
    return rows


def select_signals(signals, window):
    start, end = e.dt(window['start']), e.dt(window['end'])
    # End-valued account: an entry exactly at end belongs to the next window.
    return [s for s in signals if start <= e.dt(s['entry_utc']) < end]


class WindowPaths:
    def __init__(self, source, window, scenario):
        self.source, self.end, self.scenario = source, e.dt(window['end']), scenario
        self.evaluated = {}

    def get(self, signal):
        original = self.source(signal)
        row = dict(original)
        if row['status'] not in ('ENTRY_PROXY_REJECTED', 'UNRESOLVED_ENTRY', 'ERROR', 'NEEDS_CANDLE_DATA'):
            release = admissions.release_time(row, self.scenario)
            if row.get('kind') == 'OPEN_AT_FIXED_CUTOFF' or release > self.end:
                first = e.dt(row['interval_start_utc']) if row.get('interval_start_utc') else None
                overlaps = first is not None and first < self.end < release
                row.update(window_truncated=True, window_original_kind=row.get('kind'),
                           window_original_path_status=row['status'],
                           window_exit_interval_straddles_cutoff=overlaps,
                           kind='OPEN_AT_FIXED_CUTOFF', exit_price=None,
                           interval_start_utc=None, interval_end_utc=None,
                           observed_path_price_r=None, observed_path_worst_bound_budget_r=None,
                           certainty='OPEN_AT_WINDOW_END_UNDER_DECLARED_CHRONOLOGY',
                           status='OPEN_AT_CUTOFF' if row['status'] in ('CLOSED_RESOLVED', 'OPEN_AT_CUTOFF') and not overlaps else 'UNRESOLVED_PATH')
        self.evaluated[signal['signal_key']] = row
        return row


def audit(result, window):
    """Independent cash/financing/population identities and temporal checks."""
    summary = result['summary']; start, end = e.dt(window['start']), e.dt(window['end'])
    e.require(summary['status'] == 'COMPLETE_CONDITIONAL', 'Incomplete window replay: '+str(summary.get('stopped_reason')))
    admitted = sum(r['decision'] == 'ADMITTED' for r in result['ledger'])
    residual = summary['ending_balance_gbp'] - summary['initial_balance_gbp'] - sum(r['cash_gbp'] for r in result['cash_events'])
    financing = sum(r['financing_gbp'] for r in result['trades']) + sum(r['financing_gbp'] for r in result['open_positions'])
    e.require(abs(residual) < .001, 'Fresh account cash identity failed')
    e.require(abs(financing-summary['financing_gbp']) < .001, 'Open/closed financing identity failed')
    e.require(admitted == len(result['trades'])+len(result['open_positions']), 'Admitted population identity failed')
    e.require(all(start <= e.dt(r['entry_utc']) < end for r in result['ledger']), 'Entry outside half-open window')
    e.require(all(start <= e.dt(r['time']) <= end for r in result['cash_events']), 'Cash event outside window')
    e.require(all(e.dt(r['entry_utc']) <= e.dt(r['exit_utc']) <= end for r in result['trades']), 'Future exit leaked into window')
    return dict(status='PASS', cash_identity_residual_gbp=residual,
                maximum_order_unit_rejections=summary['decisions'].get('REJECTED_MAXIMUM_ORDER_UNITS', 0),
                open_positions_at_cutoff=len(result['open_positions']),
                no_future_exit_booked=True, financing_counted_once=True)


def comparison(control, result, window, policy, paths):
    changed, attribution = reports.admission_changes(control, result)
    checked = audit(result, window)
    periods = reports.period_tables(result, window['start'], window['end'])
    s, c = result['summary'], control['summary']
    newkeys = {r['signal_key'] for r in changed if r['category'] == 'ADDED'}
    added_closed = [r for r in result['trades'] if r['signal_key'] in newkeys]
    resolved_added = [r for r in added_closed if r['path_status']=='CLOSED_RESOLVED']
    budget_r = [r['net_r_entry_risk_budget'] for r in result['trades'] if r.get('net_r_entry_risk_budget') is not None]
    risk_fractions = [r['actual_entry_stop_risk_gbp']/r['entry_risk_budget_gbp'] for r in result['trades'] if r.get('entry_risk_budget_gbp')]
    summary = dict(s, **window, policy=policy, **attribution,
                   **reports.extra_summary(result, periods, window['start'], window['end']),
                   window_audit_status=checked['status'],
                   maximum_order_unit_rejections=checked['maximum_order_unit_rejections'],
                   capacity_limit_affected=checked['maximum_order_unit_rejections'] > 0,
                   total_mid_nav_return_pct=100*(s['conditional_ending_nav_gbp']/s['initial_balance_gbp']-1),
                   total_liquidation_return_pct=100*(s['conditional_ending_liquidation_equity_gbp']/s['initial_balance_gbp']-1),
                   delta_nav_cagr_pp=s['conditional_nav_cagr_pct']-c['conditional_nav_cagr_pct'] if s['conditional_nav_cagr_pct'] is not None and c['conditional_nav_cagr_pct'] is not None else None,
                   delta_liquidation_cagr_pp=s['conditional_liquidation_equity_cagr_pct']-c['conditional_liquidation_equity_cagr_pct'] if s['conditional_liquidation_equity_cagr_pct'] is not None and c['conditional_liquidation_equity_cagr_pct'] is not None else None,
                   delta_drawdown_pp=s['sampled_liquidation_equity_drawdown_pct']-c['sampled_liquidation_equity_drawdown_pct'],
                   delta_liquidation_equity_gbp=s['conditional_ending_liquidation_equity_gbp']-c['conditional_ending_liquidation_equity_gbp'],
                   end_open_positions_excluded_from_closed_win_rate=True,
                   mean_closed_net_r_entry_risk_budget=sum(budget_r)/len(budget_r) if budget_r else None,
                   mean_closed_actual_risk_fraction_of_budget=sum(risk_fractions)/len(risk_fractions) if risk_fractions else None,
                   added_closed_uncertain_account_lineage=sum(r['uncertain_account_lineage'] for r in added_closed),
                   **{'added_resolved_closed_'+k:v for k,v in reports.trade_stats(resolved_added).items()},
                   truncated_evaluated_paths=sum(bool(p.get('window_truncated')) for p in paths.values()),
                   cutoff_overlap_evaluated_paths=sum(bool(p.get('window_exit_interval_straddles_cutoff')) for p in paths.values()),
                   **{'added_closed_'+k:v for k,v in reports.trade_stats(added_closed).items()})
    return summary, periods, changed, checked


def robust_summary(comparisons):
    """One observation per overlapping five-year window. Never pool as independent."""
    grouped = defaultdict(list)
    for row in comparisons:
        if row['window_kind'] == 'ROLLING_FIVE_YEAR':
            grouped[row['policy'], row['scenario'], row['financing_mode']].append(row)
    out = []
    for (policy, scenario, mode), rows in sorted(grouped.items()):
        def values(field): return [r[field] for r in rows if r.get(field) is not None]
        cagrs, deltas = values('conditional_liquidation_equity_cagr_pct'), values('delta_liquidation_cagr_pp')
        draws = values('sampled_liquidation_equity_drawdown_pct')
        out.append(dict(policy=policy, scenario=scenario, financing_mode=mode, windows=len(rows),
                        overlapping_windows_are_independent=False,
                        median_liquidation_cagr_pct=median(cagrs) if cagrs else None,
                        worst_liquidation_cagr_pct=min(cagrs) if cagrs else None,
                        best_liquidation_cagr_pct=max(cagrs) if cagrs else None,
                        median_delta_liquidation_cagr_pp=median(deltas) if deltas else None,
                        worst_delta_liquidation_cagr_pp=min(deltas) if deltas else None,
                        windows_beating_control=sum(v>0 for v in deltas), windows_tying_control=sum(abs(v)<1e-10 for v in deltas),
                        worst_sampled_liquidation_drawdown_pct=min(draws),
                        median_delta_drawdown_pp=median(values('delta_drawdown_pp')),
                        windows_with_capacity_limit=sum(r['capacity_limit_affected'] for r in rows),
                        maximum_order_unit_rejections=sum(r['maximum_order_unit_rejections'] for r in rows),
                        median_admitted_per_year=median(values('admitted_per_year')),
                        median_added_closed_mean_net_r=median(values('added_closed_mean_net_r')) if values('added_closed_mean_net_r') else None,
                        selection='NO_AUTOMATIC_POLICY_SELECTION'))
    return out
