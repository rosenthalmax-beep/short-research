#!/usr/bin/env python3
"""Offline attribution of a completed AUD/USD matrix; no broker client or replay."""
import argparse
import csv
import fcntl
import gzip
import hashlib
import io
import json
import math
import os
import shutil
import threading
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from wsgiref.simple_server import WSGIRequestHandler, make_server

VERSION = 'AUDUSD_PULLBACK_RECONCILIATION_V1_2026_10_23'
MODES = ('FROZEN_14OCT2026_RATES', 'ADVERSE_6PCT_ANNUAL')
WINDOWS = ('FULL_HISTORY', 'ERA_2005_2009', 'ERA_2010_2014', 'ERA_2015_2019',
           'ERA_2020_2024', 'ERA_2025_2029', 'RECENT_2021_TO_CUTOFF')
ROOT = Path(__file__).resolve().parent
MAX_REPORT_BYTES = 20_000_000


def require(ok, message):
    if not ok:
        raise ValueError(message)


def canon(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(body):
    return hashlib.sha256(body).hexdigest()


def sha_file(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def utc(value):
    t = datetime.fromisoformat(value.replace('Z', '+00:00'))
    require(t.tzinfo is not None, 'Timezone missing: ' + value)
    return t.astimezone(timezone.utc)


def number(value):
    require(isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value),
            'Missing or non-finite numeric field')
    return float(value)


def near(actual, expected, label, tolerance=1e-8):
    require(math.isclose(number(actual), number(expected), rel_tol=1e-10, abs_tol=tolerance),
            'Reconciliation mismatch: ' + label)


def fsum(values):
    return math.fsum(number(v) for v in values)


def mean(values):
    return fsum(values) / len(values) if values else None


def safe_file(root, name):
    require(isinstance(name, str) and name and not Path(name).is_absolute(), 'Unsafe source filename')
    path = root / name
    require(not path.is_symlink() and path.resolve().is_relative_to(root.resolve()) and path.is_file(),
            'Missing or unsafe saved file: ' + name)
    return path


def json_file(path):
    return json.loads(Path(path).read_bytes())


def json_gz(path):
    with gzip.open(path, 'rb') as f:
        return json.load(f)


def jsonl_gz(path):
    with gzip.open(path, 'rb') as f:
        for line in f:
            require(len(line) < 2_000_000, 'Unexpected oversized JSONL record')
            if line.strip():
                yield json.loads(line)


def index_by(rows, key, label):
    result = {}
    for row in rows:
        require(key in row and row[key] not in result, 'Missing/duplicate key in ' + label)
        result[row[key]] = row
    return result


def csv_bytes(rows):
    fields = list(dict.fromkeys(k for row in rows for k in row)) or ['empty']
    out = io.StringIO(newline='')
    writer = csv.DictWriter(out, fields)
    writer.writeheader()
    for row in rows:
        writer.writerow({k: json.dumps(v, sort_keys=True) if isinstance(v, (list, dict)) else v
                         for k, v in row.items()})
    return out.getvalue().encode()


def shadow_r(opportunity, mode):
    shadow = opportunity['shadow']
    if shadow['shadow_status'] != 'CLOSED_RESOLVED':
        return None
    return number(shadow['frozen_net_r' if mode == MODES[0] else 'adverse6_net_r'])


def source_snapshot(source, pins, progress):
    """Verify the exact previously reviewed summary and selected completed files."""
    require(source.name == pins['source_signature'], 'Source directory must end in the reviewed matrix signature')
    before = {}
    for name, witness in pins['files'].items():
        path = safe_file(source, name)
        require(path.stat().st_size == witness['bytes'] and sha_file(path) == witness['sha256'],
                'Reviewed source summary changed: ' + name)
        before[name] = witness['sha256']
    state = json_file(source / 'run_state.json')
    require(state['status'] == 'COMPLETE_CONDITIONAL_MATRIX' and state['accounts_completed'] == 224
            and state['candidates_completed'] == 16 and state['signature'] == pins['source_signature'],
            'The reviewed 16-candidate, 224-account matrix must be complete')
    parity = json_file(source / 'baseline_parity.json')
    require(parity.get('status') == 'PASS' and parity.get('exact_signal_records_matched') == 4086,
            'Original baseline parity missing')
    directory = source / 'candidates' / pins['candidate_id']
    marker_path = safe_file(directory, 'COMPLETE.json')
    marker = json_file(marker_path)
    require(marker['signature'] == pins['source_signature'] and marker['candidate_id'] == pins['candidate_id']
            and marker['accounts'] == 14, 'Selected candidate is not complete')
    before['candidates/' + pins['candidate_id'] + '/COMPLETE.json'] = sha_file(marker_path)
    required = {'signals.jsonl.gz', 'opportunities.jsonl.gz', 'account_summaries.json'}
    required.update('accounts/' + window + '__' + mode + '.json.gz' for window in WINDOWS for mode in MODES)
    require(required.issubset(marker['files']), 'Candidate witness lacks required exports')
    for name, expected in marker['files'].items():
        path = safe_file(directory, name)
        require(sha_file(path) == expected, 'Candidate checksum failed: ' + name)
        before['candidates/' + pins['candidate_id'] + '/' + name] = expected
        progress(phase='VERIFY_COMPLETED_EXPORTS', verified_file=name)
    return directory, before


def load_paths(source, wanted, pins, progress):
    """Use the final immutable path export; never open the live path database."""
    path = safe_file(source, 'unique_price_paths.jsonl.gz')
    before = sha_file(path)
    result = {}
    for n, row in enumerate(jsonl_gz(path), 1):
        key = row.get('path_cache_key')
        if key in wanted:
            require(key not in result, 'Duplicate shared path export')
            require(row.get('native_source_sha256') == pins['source_native_sha256'],
                    'Selected path native history differs from reviewed history')
            result[key] = row
        if n % 2000 == 0:
            progress(phase='READ_SAVED_PATHS', saved_paths_scanned=n, selected_paths_found=len(result))
    require(set(result) == set(wanted), 'Selected shared price paths missing from completed export')
    require(sha_file(path) == before, 'Shared price path export changed during read')
    return result, before


def check_signals(signals, opps, paths, pins, provenance):
    si = index_by(signals, 'signal_key', 'signals')
    oi = index_by(opps, 'signal_key', 'opportunities')
    require(set(si) == set(oi), 'Signal/opportunity key sets differ')
    for key, s in si.items():
        require(s['candidate_id'] == pins['candidate_id'] and s['pair'] == 'AUD_USD' and s['side'] == 'BUY'
                and s['rr'] == 3 and s['timeframe'] == 'M15', 'Unexpected selected strategy geometry')
        require(s['features']['body_atr'] >= 1 and s['features']['h1_distance_atr'] <= 1.25,
                'Saved signal violates selected filters')
        op = oi[key]
        require(op['entry_utc'] == s['entry_utc'], 'Signal/opportunity entry time mismatch')
        p = paths[op['path_cache_key']]
        require(p['status'] == op['path_status'], 'Path and opportunity status disagree')
        identity = dict(signature=provenance['source_pins']['signature'],
                        native_sha256=p['native_source_sha256'], pair='AUD_USD', side='BUY',
                        entry=s['entry_utc'], reference=s['reference_entry'], stop=s['stop'], rr=3.,
                        end=p['source_horizon_utc'], limit=15)
        require(digest(canon(identity)) == p['path_cache_key'], 'Selected path geometry key mismatch')
    return si, oi


def check_opportunity_pins(opps, pins):
    checks = []
    for expected in pins['opportunity_rows']:
        selected = [o for o in opps if utc(expected['requested_period_start_utc']) <= utc(o['entry_utc'])
                    < utc(expected['period_end_utc'])]
        closed = [o for o in selected if o['shadow']['shadow_status'] == 'CLOSED_RESOLVED']
        require(len(selected) == int(expected['qualifications']) and
                len(closed) == int(expected['resolved_standalone_gbp100_opportunities']),
                'Reviewed opportunity population does not match saved rows')
        for field, mode in [('resolved_only_sum_frozen_net_r', MODES[0])]:
            near(fsum(shadow_r(o, mode) for o in closed), float(expected[field]), 'Opportunity ' + field)
        for field, mode in [('resolved_only_mean_frozen_net_r', MODES[0]),
                            ('resolved_only_mean_adverse6_net_r', MODES[1])]:
            actual = mean([shadow_r(o, mode) for o in closed])
            if expected[field] not in ('', None):
                near(actual, float(expected[field]), 'Opportunity ' + field)
        checks.append(dict(entry_cohort=expected['entry_cohort'], qualifications=len(selected),
                           resolved=len(closed), status='PASS'))
    return checks


def check_account(result, signals, expected):
    """Verify counts, key membership, GBP cash and R before making comparisons."""
    s = result['summary']
    require(s['status'] == 'COMPLETE_CONDITIONAL' and s['max_positions'] <= 1,
            'Incomplete or overlapping candidate account')
    ledger = index_by(result['ledger'], 'signal_key', 'account decisions')
    trades = index_by(result['trades'], 'signal_key', 'closed account trades')
    opens = index_by(result['open_positions'], 'signal_key', 'open account positions')
    lo, hi = utc(expected['effective_start']), utc(expected['end'])
    selected = {k for k, v in signals.items() if lo <= utc(v['entry_utc']) < hi}
    require(set(ledger) == selected, 'Account decisions omit or invent a qualifying signal')
    admitted = {k for k, v in ledger.items() if v['decision'] == 'ADMITTED'}
    require(not set(trades).intersection(opens) and set(trades).union(opens) == admitted,
            'Admitted positions do not reconcile to closed/open positions')
    require(s['decisions'] == dict(Counter(v['decision'] for v in ledger.values())), 'Decision count mismatch')
    require(s['closed_trades'] == len(trades) and s['open_positions'] == len(opens)
            and s['qualifications_processed'] == len(selected), 'Account population mismatch')
    price_by_key, finance_by_key = defaultdict(float), defaultdict(float)
    entry_events = Counter()
    balance = number(s['initial_balance_gbp'])
    last_time = None
    for row in result['cash_events']:
        key = row['signal_key']
        require(key in admitted, 'Cash event is orphaned or belongs to blocked signal')
        stamp = utc(row['time'])
        require(lo <= stamp <= hi and (last_time is None or last_time <= stamp), 'Cash event chronology mismatch')
        last_time = stamp
        value = number(row['cash_gbp'])
        balance = round(balance + value, 4)
        near(balance, row['balance_after_gbp'], 'Running cash balance', .00011)
        if row['event'] == 'EXIT':
            require(key in trades, 'Exit cash without closed position')
            price_by_key[key] += value
        elif row['event'] == 'FINANCING':
            finance_by_key[key] += value
        elif row['event'] == 'ENTRY':
            entry_events[key] += 1
            near(value, 0, 'Entry must not debit spread/conversion again')
        else:
            raise ValueError('Unknown saved cash event')
    require(set(entry_events) == admitted and all(v == 1 for v in entry_events.values()), 'Entry cash event mismatch')
    financing_rows = defaultdict(float)
    for row in result['financing']:
        require(row['signal_key'] in admitted, 'Orphan financing record')
        financing_rows[row['signal_key']] += number(row['financing_gbp'])
    for key in admitted:
        near(finance_by_key[key], financing_rows[key], 'Funding events for ' + key, .00011)
    for key, row in trades.items():
        require(row['entry_utc'] == signals[key]['entry_utc'] and utc(row['entry_utc']) <= utc(row['exit_utc']) <= hi,
                'Closed trade outside its window')
        near(row['price_pl_gbp'], price_by_key[key], 'Per-trade exit cash', .00011)
        near(row['financing_gbp'], finance_by_key[key], 'Per-trade financing', .00011)
        near(row['net_pl_gbp'], row['price_pl_gbp'] + row['financing_gbp'], 'Per-trade net cash', .00011)
        require(row['actual_entry_stop_risk_gbp'] > 0, 'Invalid account R denominator')
        near(row['net_r_actual_entry_risk'], row['net_pl_gbp'] / row['actual_entry_stop_risk_gbp'], 'Per-trade R')
    for key, row in opens.items():
        near(row['financing_gbp'], finance_by_key[key], 'Open-position financing', .00011)
    near(balance, s['ending_balance_gbp'], 'Ending cash', .00011)
    near(fsum(price_by_key.values()), s['price_pl_gbp'], 'Total price cash', .00011)
    near(fsum(finance_by_key.values()), s['financing_gbp'], 'Total funding cash', .00011)
    near(s['initial_balance_gbp'] + s['price_pl_gbp'] + s['financing_gbp'], s['ending_balance_gbp'], 'Cash identity', .00021)
    near(fsum(v['net_r_actual_entry_risk'] for v in trades.values()), s['net_r_sum_actual_entry_risk'], 'Total account R')
    # Pin actual results to the summary the user supplied, not just internal hashes.
    for field in ('closed_trades', 'open_positions', 'qualifications', 'ending_balance_gbp',
                  'conditional_ending_nav_gbp', 'price_pl_gbp', 'financing_gbp',
                  'net_r_sum_actual_entry_risk', 'mean_net_r_actual_entry_risk', 'sampled_mid_nav_drawdown_pct'):
        if expected.get(field, '') not in ('', None):
            near(s[field], float(expected[field]), 'Reviewed account ' + field)
    values = [r['net_r_actual_entry_risk'] for r in trades.values()]
    without = mean(sorted(values)[:-5]) if len(values) > 5 else None
    if expected.get('mean_net_r_without_best_five', '') not in ('', None):
        near(without, float(expected['mean_net_r_without_best_five']), 'Reviewed best-five removal')
    return ledger, trades, opens, dict(window_id=expected['window_id'], financing_mode=expected['financing_mode'],
        status='PASS', cash_events=len(result['cash_events']), closed_trades=len(trades),
        open_positions=len(opens), ending_balance_gbp=balance, mean_net_r_without_best_five=without)


def compare_account(signals, opportunities, paths, result, expected):
    ledger, trades, opens, cash_check = check_account(result, signals, expected)
    mode = expected['financing_mode']
    lo, hi = utc(expected['effective_start']), utc(expected['end'])
    selected = {k: o for k, o in opportunities.items() if lo <= utc(o['entry_utc']) < hi}
    shadow = {k: shadow_r(o, mode) for k, o in selected.items() if shadow_r(o, mode) is not None}
    shared = set(shadow).intersection(trades)
    shadow_only = set(shadow) - set(trades)
    account_only = set(trades) - set(shadow)
    a = fsum(shadow.values())
    removed = fsum(shadow[k] for k in shadow_only)
    delta = fsum(trades[k]['net_r_actual_entry_risk'] - shadow[k] for k in shared)
    extra = fsum(trades[k]['net_r_actual_entry_risk'] for k in account_only)
    final = fsum(t['net_r_actual_entry_risk'] for t in trades.values())
    near(a - removed + delta + extra, final, 'Additive R bridge')
    prefix = dict(window_id=expected['window_id'], financing_mode=mode)
    bridge = dict(prefix, standalone_resolved_count=len(shadow), standalone_net_r_sum=a,
        standalone_mean_net_r=mean(list(shadow.values())), shared_closed_count=len(shared),
        shadow_only_count=len(shadow_only), subtract_shadow_only_net_r_sum=removed,
        matched_account_minus_shadow_net_r_sum=delta, account_only_count=len(account_only),
        add_account_only_net_r_sum=extra, account_closed_count=len(trades), account_net_r_sum=final,
        account_mean_net_r=mean([t['net_r_actual_entry_risk'] for t in trades.values()]),
        bridge_residual=a - removed + delta + extra - final,
        accounting_check='PASS', scope='EXACT_SAVED_POPULATION_ATTRIBUTION_NOT_CAUSAL_RETURN_IMPACT')
    bridge['shared_shadow_mean_net_r'] = mean([shadow[k] for k in shared])
    bridge['shared_account_mean_net_r'] = mean([trades[k]['net_r_actual_entry_risk'] for k in shared])
    if shared:
        bridge['mean_change_selection'] = bridge['shared_shadow_mean_net_r'] - bridge['standalone_mean_net_r']
        bridge['mean_change_matched_results'] = bridge['shared_account_mean_net_r'] - bridge['shared_shadow_mean_net_r']
        bridge['mean_change_account_only_composition'] = bridge['account_mean_net_r'] - bridge['shared_account_mean_net_r']
        near(bridge['standalone_mean_net_r'] + bridge['mean_change_selection'] +
             bridge['mean_change_matched_results'] + bridge['mean_change_account_only_composition'],
             bridge['account_mean_net_r'], 'Mean attribution identity')
    joined = []
    for key in sorted(selected, key=lambda k: (signals[k]['entry_utc'], k)):
        s, o, d = signals[key], selected[key], ledger[key]
        p = paths[o['path_cache_key']]
        t = trades.get(key)
        sh = o['shadow']
        bucket = ('SHARED_CLOSED' if key in shared else 'SHADOW_ONLY_RESOLVED' if key in shadow_only
                  else 'ACCOUNT_ONLY_CLOSED' if key in account_only else 'NEITHER_CLOSED')
        after = bool(p.get('interval_end_utc') and utc(p['interval_end_utc']) > hi)
        row = dict(prefix, signal_key=key, entry_utc=s['entry_utc'], population=bucket,
            account_decision=d['decision'], decision_reason=d.get('reason'),
            blocking_signal_keys=d.get('blocking_signal_keys', []), account_open=key in opens,
            shadow_status=sh['shadow_status'], source_path_status=p['status'],
            path_certainty=p.get('certainty'), path_kind=p.get('kind'),
            prior_native_unexplained_slots=p.get('prior_native_unexplained_slots'),
            standalone_exit_after_account_window=after,
            source_path_interval_start_utc=p.get('interval_start_utc'),
            source_path_interval_end_utc=p.get('interval_end_utc'),
            shadow_net_r=shadow.get(key), account_net_r=t['net_r_actual_entry_risk'] if t else None,
            source_path_cache_key=o['path_cache_key'],
            conditional_unresolved_path_used=bool(t and p['status'] not in ('CLOSED_RESOLVED', 'OPEN_AT_CUTOFF')),
            ambiguous_stop_first=bool(t and p.get('kind') == 'AMBIGUOUS_BOTH'),
            shadow_stale_conversion_seen=sh.get('stale_conversion_quote_seen'),
            account_lineage_uncertain=d.get('uncertain_account_lineage'),
            direct_entry_quote_disagreement=d.get('native_vs_m15_entry_quote_max_difference'),
            body_atr=s['features']['body_atr'], h1_distance_atr=s['features']['h1_distance_atr'])
        if t:
            row.update(account_exit_utc=t['exit_utc'], account_exit_reason=t['exit_reason'],
                account_units=t['units'], account_price_pl_gbp=t['price_pl_gbp'],
                account_financing_gbp=t['financing_gbp'], account_net_pl_gbp=t['net_pl_gbp'],
                account_actual_entry_risk_gbp=t['actual_entry_stop_risk_gbp'],
                account_entry_price=t['entry_price'], account_exit_price=t['exit_price'],
                account_exit_conversion_quote_max_age_seconds=t.get('exit_conversion_quote_max_age_seconds'),
                direct_exit_quote_over_15min=bool(t.get('exit_conversion_quote_max_age_seconds', 0) > 900))
        if key in shared:
            risk_s, risk_a = number(sh['actual_stop_risk_gbp']), number(t['actual_entry_stop_risk_gbp'])
            require(risk_s > 0 and risk_a > 0, 'Nonpositive matched risk denominator')
            shadow_price_r = number(sh['price_pl_gbp']) / risk_s
            account_price_r = number(t['price_pl_gbp']) / risk_a
            price_delta = account_price_r - shadow_price_r
            funding_delta = t['financing_gbp'] / risk_a - (shadow[key] - shadow_price_r)
            net_delta = t['net_r_actual_entry_risk'] - shadow[key]
            residual = net_delta - price_delta - funding_delta
            near(residual, 0, 'Matched price/funding R components')
            row.update(shadow_units=sh['units'], shadow_actual_entry_risk_gbp=risk_s,
                shadow_price_pl_gbp=sh['price_pl_gbp'], shadow_price_r=shadow_price_r,
                shadow_funding_r=shadow[key] - shadow_price_r, account_price_r=account_price_r,
                account_funding_r=t['financing_gbp'] / risk_a,
                matched_price_r_difference=price_delta, matched_funding_r_difference=funding_delta,
                matched_net_r_difference=net_delta, matched_component_residual=residual,
                same_exit_price=math.isclose(t['exit_price'], p['exit_price'], abs_tol=1e-10, rel_tol=0),
                same_exit_time=utc(t['exit_utc']) == utc(p['interval_end_utc']),
                same_entry_price=math.isclose(t['entry_price'], p['model_fill'], abs_tol=1e-10, rel_tol=0))
        joined.append(row)
    grouped = []
    groups = defaultdict(list)
    for r in joined:
        groups[(r['population'], r['account_decision'], r['shadow_status'], r['source_path_status'])].append(r)
    for (population, decision, sh_status, path_status), rows in sorted(groups.items()):
        sr = [r['shadow_net_r'] for r in rows if r['shadow_net_r'] is not None]
        ar = [r['account_net_r'] for r in rows if r['account_net_r'] is not None]
        grouped.append(dict(prefix, population=population, account_decision=decision,
            shadow_status=sh_status, source_path_status=path_status, signals=len(rows),
            shadow_closed_count=len(sr), shadow_net_r_sum=fsum(sr), shadow_mean_net_r=mean(sr),
            account_closed_count=len(ar), account_net_r_sum=fsum(ar), account_mean_net_r=mean(ar)))
    # Source uncertainty counts are inherited account-lineage counts, not unique bad trades.
    uncertain = dict(prefix, first_uncertainty=result['summary'].get('first_uncertainty'),
        reported_account_uncertainty_causes=result['summary']['uncertainty_causes'],
        account_lineage_uncertain=result['summary']['uncertain_account_lineage'],
        direct_unresolved_path_closed_trades=sum(r['conditional_unresolved_path_used'] for r in joined),
        direct_ambiguous_stop_first_closed_trades=sum(r['ambiguous_stop_first'] for r in joined),
        saved_exit_quote_over_15min_closed_trades=sum(r.get('direct_exit_quote_over_15min', False) for r in joined),
        per_event_nav_quote_history_saved=bool(result.get('nav_samples')),
        warning='Lineage stays uncertain after a flagged event; it does not identify a direct defect on every later trade. '
                'No fresh quotes, repaired history, alternate account or clean-subset CAGR is computed.')
    return bridge, joined, grouped, cash_check, uncertain


README = '''# AUD/USD saved-record reconciliation

This audit reads one completed candidate (distance <=1.25 H1 ATR, body >=1 M15
ATR), all 14 saved account windows/modes and its shared price paths. It does not
fetch candles, recalculate exits, replay accounts, search parameters or trade.

`r_bridge.csv` is an exact additive accounting identity:
standalone resolved R - excluded standalone R + matched account-minus-shadow R
+ account-only closed R = saved account R. R sums describe trades normalized by
their own entry risk; they are not cash returns. `mean_change_*` is a sequential
descriptive identity across different populations, not causal policy impact.

`trade_join.csv` includes every qualification for each account window/mode,
including blocked, rejected, unresolved and open records. `decision_groups.csv`
partitions their counts and R. `matched_differences.csv` isolates price/funding R
differences on shared closed trades. Different units/GBP cash alone do not prove
an execution error: independent opportunities use fresh GBP100, whereas accounts
compound, floor whole units and may truncate paths at their window end.

`path_cases.jsonl.gz` preserves all selected signals, opportunity outcomes and
paths. `account_ledgers.jsonl.gz` preserves the selected decisions, trades,
financing and cash events, excluding bulky NAV/monthly tables. `uncertainty.csv`
keeps event-count and lineage limitations explicit. Stale quote counts do not
prove all of them are conversion quotes; historical valuation uses both pair
prices and GBP conversions. The saved engine did not retain a complete per-event
quote trace, so the exact effect of every stale mark cannot be inferred here.

Independent opportunity outcomes follow entries to the full study cutoff.
Era accounts reset flat and end earlier. `standalone_exit_after_account_window`
marks this boundary difference; their results must not be compared as identical
investment windows. Windows/funding modes overlap and are not independent tests.

No excluding-uncertain-trades CAGR is reported: removing trades would change
later NAV, sizing, admission and funding. A new controlled replay would be needed.
No assertion that this candidate passes robustness/plateau or live32 admission.

`validation.json` checks saved hashes, reviewed summary totals, checkpoint hashes,
all signal keys, running GBP balances, per-trade cash/funding/R and both bridge
identities. PASS proves this attribution agrees with the saved conditional run;
it does not establish that historical fills or broker terms are reconstructed.
'''


class Audit:
    def __init__(self, environ=None, pins=None):
        env = os.environ if environ is None else environ
        self.pins = json_file(ROOT / 'reconciliation_pins.json') if pins is None else pins
        base = Path(env.get('AUDUSD_MATRIX_OUTPUT_DIR', '/data/p32_audusd_long_matrix'))
        self.source = Path(env.get('AUDUSD_RECON_SOURCE_DIR', str(base / self.pins['source_signature']))).resolve()
        self.output = Path(env.get('AUDUSD_RECON_OUTPUT_DIR', '/data/p32_audusd_reconciliation')).resolve()
        require(not self.output.is_relative_to(base.resolve()) and not base.resolve().is_relative_to(self.output)
                and not self.output.is_relative_to(self.source) and not self.source.is_relative_to(self.output),
                'Keep reconciliation outputs separate from the matrix source tree')
        self.proof = dict(code_sha256=sha_file(Path(__file__)), pins_sha256=digest(canon(self.pins)))
        self.signature = digest(canon(dict(version=VERSION, **self.proof)))
        self.work = self.output / self.signature
        self.lock = threading.RLock()
        self.thread = None
        self.state = dict(service=VERSION, status='IDLE', phase='WAITING_FOR_STATUS',
            candidate_id=self.pins['candidate_id'], source_signature=self.pins['source_signature'],
            signature=self.signature, accounts_completed=0, accounts_required=14,
            orders_supported=False, orders_sent=False, candle_downloads=0,
            source_files_modified=False, result_ready=False)

    def progress(self, **values):
        with self.lock:
            self.state.update(values, last_progress_utc=datetime.now(timezone.utc).isoformat())

    def status(self, start=False):
        with self.lock:
            if start and self.thread is None and self.state['status'] == 'IDLE':
                self.state['status'] = 'STARTING'
                self.thread = threading.Thread(target=self.run, daemon=True, name='audusd-record-reconciliation')
                self.thread.start()
            value = dict(self.state, worker_alive=bool(self.thread and self.thread.is_alive()))
            if self.output.exists():
                value['free_disk_bytes'] = shutil.disk_usage(self.output).free
            return value

    def run(self):
        output_lock = None
        try:
            self.progress(status='RUNNING', phase='LOCATE_COMPLETED_MATRIX')
            require(self.source.is_dir(), 'Completed matrix not found. Keep AUDUSD_MATRIX_OUTPUT_DIR at its existing value.')
            self.output.mkdir(parents=True, exist_ok=True)
            output_lock = (self.output / 'runner.lock').open('ab')
            try:
                fcntl.flock(output_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ValueError('Another reconciliation process is running; wait for it to finish')
            # Reuse existing lock read-only. Opening it never creates or modifies a source file.
            lock_path = safe_file(self.source.parent, 'runner.lock')
            with lock_path.open('rb') as source_lock:
                try:
                    fcntl.flock(source_lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
                except BlockingIOError:
                    raise ValueError('Matrix writer is still running; wait for it to finish before this audit')
                directory, witness = source_snapshot(self.source, self.pins, self.progress)
                signals = list(jsonl_gz(directory / 'signals.jsonl.gz'))
                opportunities = list(jsonl_gz(directory / 'opportunities.jsonl.gz'))
                paths, path_sha = load_paths(self.source, {o['path_cache_key'] for o in opportunities}, self.pins, self.progress)
                provenance = json_file(self.source / 'source_provenance.json')
                si, oi = check_signals(signals, opportunities, paths, self.pins, provenance)
                op_checks = check_opportunity_pins(opportunities, self.pins)
                expected_rows = self.pins['account_rows']
                require({(r['window_id'], r['financing_mode']) for r in expected_rows} ==
                        {(w, m) for w in WINDOWS for m in MODES} and len(expected_rows) == 14,
                        'Expected exactly the reviewed 14 account rows')
                bridges, joins, groups, cash_checks, uncertainties, account_exports = [], [], [], [], [], []
                for n, expected in enumerate(expected_rows, 1):
                    window, mode = expected['window_id'], expected['financing_mode']
                    self.progress(phase='RECONCILE_SAVED_ACCOUNT', window_id=window, financing_mode=mode)
                    filename = 'accounts/' + window + '__' + mode + '.json.gz'
                    checkpoint = json_gz(directory / filename)
                    require(checkpoint['signature'] == self.pins['source_signature'] and
                            digest(canon(checkpoint['result'])) == checkpoint['sha256'], 'Account payload checksum failed')
                    result = checkpoint['result']
                    require(result['summary']['financing_mode'] == mode, 'Checkpoint funding label mismatch')
                    b, j, g, c, u = compare_account(si, oi, paths, result, expected)
                    bridges.append(b); joins.extend(j); groups.extend(g); cash_checks.append(c); uncertainties.append(u)
                    account_exports.append(dict(window_id=window, financing_mode=mode,
                        result={k: result[k] for k in ('summary', 'ledger', 'trades', 'open_positions', 'financing', 'cash_events')}))
                    self.progress(accounts_completed=n)
                for name, expected_hash in witness.items():
                    require(sha_file(safe_file(self.source, name)) == expected_hash, 'Source changed during reconciliation: ' + name)
                require(sha_file(self.source / 'unique_price_paths.jsonl.gz') == path_sha, 'Path export changed during reconciliation')
                validation = dict(status='PASS', accounts_checked=14, opportunity_cohorts_checked=op_checks,
                    account_checks=cash_checks, reviewed_summary_hashes_verified=True,
                    checkpoint_and_candidate_hashes_verified=True, source_snapshot_verified_unchanged=True,
                    shared_path_export_sha256=path_sha, selected_paths=len(paths),
                    path_export_validation='Geometry identities/native history checked; full export hash recorded. '
                        'Export is not independently anchored by the earlier small summary manifest.',
                    source_witness=witness, source_signature=self.pins['source_signature'],
                    candidate_id=self.pins['candidate_id'], code=self.proof, orders_supported=False,
                    orders_sent=False, candle_downloads=0, historical_price_reconstruction_proved=False)
                bodies = {'REVIEW.md': README.encode(), 'r_bridge.csv': csv_bytes(bridges),
                    'trade_join.csv': csv_bytes(joins), 'decision_groups.csv': csv_bytes(groups),
                    'matched_differences.csv': csv_bytes([r for r in joins if r['population'] == 'SHARED_CLOSED']),
                    'uncertainty.csv': csv_bytes(uncertainties), 'validation.json': canon(validation),
                    'path_cases.jsonl.gz': gzip.compress(b''.join(canon(dict(signal=s, opportunity=oi[s['signal_key']],
                        path=paths[oi[s['signal_key']]['path_cache_key']])) + b'\n' for s in signals)),
                    'account_ledgers.jsonl.gz': gzip.compress(b''.join(canon(r) + b'\n' for r in account_exports)),
                    'reviewed_account_summary_rows.json': canon(expected_rows)}
                require(sum(len(b) for b in bodies.values()) < 48_000_000, 'Unexpectedly large diagnostic payload')
                self.publish(bodies)
            self.progress(status='COMPLETE', phase='COMPLETE', result_ready=True, result='/results-reconciliation.zip',
                          validation='PASS', joined_rows=len(joins), accounts_completed=14)
        except Exception as exc:
            # Inputs contain no credentials; do not dump environment or arbitrary exception representations.
            message = str(exc) if isinstance(exc, (ValueError, FileNotFoundError, OSError)) else type(exc).__name__
            self.progress(status='ERROR', phase='STOPPED', result_ready=False, error=message)
        finally:
            if output_lock is not None:
                output_lock.close()

    def publish(self, bodies):
        self.progress(phase='WRITE_SMALL_RECONCILIATION_REPORT')
        self.output.mkdir(parents=True, exist_ok=True)
        require(shutil.disk_usage(self.output).free >= 48 * 1024 * 1024,
                'Need at least 48 MiB free for the small diagnostic; old reports were not changed')
        self.work.mkdir(parents=True, exist_ok=True)
        target = self.work / 'AUDUSD_RECONCILIATION.zip'
        temporary = self.work / 'AUDUSD_RECONCILIATION.zip.tmp'
        manifest = {n: dict(bytes=len(b), sha256=digest(b)) for n, b in bodies.items()}
        try:
            with zipfile.ZipFile(temporary, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as z:
                for name, body in sorted(bodies.items()):
                    z.writestr(name, body)
                z.writestr('report_manifest.json', canon(manifest))
                z.writestr('RUN_COMPLETE.json', canon(dict(service=VERSION, status='COMPLETE_SAVED_RECORD_RECONCILIATION',
                    signature=self.signature, accounts_completed=14, source_signature=self.pins['source_signature'],
                    candidate_id=self.pins['candidate_id'], candle_downloads=0, orders_supported=False, orders_sent=False)))
            require(temporary.stat().st_size < MAX_REPORT_BYTES, 'Diagnostic archive exceeds the 20 MB report limit')
            with zipfile.ZipFile(temporary) as z:
                require(z.testzip() is None, 'Diagnostic archive integrity failed')
            temporary.replace(target)
            self.progress(report_bytes=target.stat().st_size)
        finally:
            temporary.unlink(missing_ok=True)


def application(job):
    def app(environ, start_response):
        method, path = environ.get('REQUEST_METHOD', 'GET'), environ.get('PATH_INFO', '/')
        status = '200 OK'
        headers = [('Cache-Control', 'no-store')]
        if method not in ('GET', 'HEAD'):
            status, payload = '405 Method Not Allowed', dict(error='GET and HEAD only', orders_supported=False)
        elif path in ('/', '/health'):
            payload = dict(service=VERSION, candidate_id=job.pins['candidate_id'], saved_accounts=14,
                start_once='/status', result='/results-reconciliation.zip', orders_supported=False,
                orders_sent=False, candle_downloads=0, completed_records_only=True)
        elif path == '/status':
            payload = job.status(start=method == 'GET')
        elif path in ('/results-reconciliation.zip', '/results.zip'):
            target = job.work / 'AUDUSD_RECONCILIATION.zip'
            if not job.state.get('result_ready') or not target.is_file():
                status, payload = '404 Not Found', dict(error='No successful report from this process yet; open /status')
            else:
                start_response(status, headers + [('Content-Type', 'application/zip'),
                    ('Content-Length', str(target.stat().st_size)),
                    ('Content-Disposition', 'attachment; filename=AUDUSD_RECONCILIATION.zip')])
                if method == 'HEAD':
                    return []
                def chunks():
                    with target.open('rb') as f:
                        yield from iter(lambda: f.read(256 * 1024), b'')
                return chunks()
        else:
            status, payload = '404 Not Found', dict(error='Unknown route')
        body = canon(payload)
        start_response(status, headers + [('Content-Type', 'application/json'), ('Content-Length', str(len(body)))])
        return [] if method == 'HEAD' else [body]
    return app


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', action='store_true', help='Run once without starting the HTTP report server')
    args = parser.parse_args()
    job = Audit()
    if args.run:
        job.run()
        print(json.dumps(job.status(), indent=2))
        raise SystemExit(0 if job.state['status'] == 'COMPLETE' else 1)
    class Quiet(WSGIRequestHandler):
        def log_message(self, *args):
            pass
    port = int(os.getenv('PORT', '8080'))
    print(json.dumps(dict(service=VERSION, port=port, orders_supported=False, candle_downloads=0)), flush=True)
    with make_server('0.0.0.0', port, application(job), handler_class=Quiet) as server:
        server.serve_forever()


if __name__ == '__main__':
    main()
