#!/usr/bin/env python3
"""Read-only fresh GBP100 research accounts. Historical candle GETs only."""
import argparse
import collections
import csv
import fcntl
import gzip
import json
import os
import shutil
import tempfile
import threading
import zipfile
from datetime import datetime
from pathlib import Path
from wsgiref.simple_server import make_server, WSGIRequestHandler
import admissions
import control_checks
import execution as e
import gbp_support as support
import inputs
import reports
from accounting import Replay
from broker_reconcile import reconcile
from execution_sources import ExecutionWindows
from funding import Funding, MODES
from market import Market
from policy_paths import PolicyPaths
from policies import PROTOCOL as ENTRY_PROTOCOL, PRIMARY_SCENARIO, PRIMARY_FUNDING
from window_replay import VERSION, POLICIES, WindowPaths, windows, select_signals, comparison, robust_summary
from valuation_sources import ValuationWindows
from entry_limit_service import write_zip, source_signature, checkpoint_load

RESULT = 'P32_SMALL_ACCOUNT_ENTRY_LIMIT_RESULTS.zip'
LOCK = threading.RLock()
STATE = dict(status='IDLE', version=VERSION, orders_supported=False, orders_sent=False,
             full_portfolio_validated=False)
DIRECTORY = None
atomic = support.atomic


def output_dir():
    value = DIRECTORY or os.getenv('P32_GBP_OUTPUT_DIR') or os.getenv('P32_SIGNAL_OUTPUT_DIR')
    return Path(value or ('/data/p32_signal_admission' if Path('/data').is_dir() else str(Path(tempfile.gettempdir())/'p32_signal_admission'))).resolve()


def work_dir(): return output_dir()/'small_account_entry_limit_v1'


def progress(**values):
    with LOCK:
        if values.get('phase') and values['phase'] != STATE.get('phase'):
            for k in ('quote_pair', 'quote_from', 'quote_to', 'qualifications_processed', 'through_utc'):
                STATE.pop(k, None)
        STATE.update(values, last_progress_utc=e.stamp(datetime.now(e.UTC)))


def save_checkpoint(path, signature, result):
    atomic(path, gzip.compress(e.canon(dict(signature=signature, result=result,
                                         sha256=e.sha(e.canon(result)))), compresslevel=6))


def label(window, policy, scenario, mode):
    return dict(window, policy=policy, scenario=scenario, financing_mode=mode)


def outcome_rows(changes, control, result, evaluated, labels):
    old = {r['signal_key']:r for r in control['trades']}
    new = {r['signal_key']:r for r in result['trades']}
    old_open = {r['signal_key'] for r in control['open_positions']}
    new_open = {r['signal_key'] for r in result['open_positions']}
    for change in changes:
        if change['category'] == 'COMMON': continue
        key = change['signal_key']; isnew = change['category'] == 'ADDED'
        trade = (new if isnew else old).get(key)
        p = (evaluated if isnew else control.get('evaluated_paths', {})).get(key, {})
        yield dict(change, **labels,
                   outcome_basis='POLICY_ACCOUNT' if isnew else '10_TICK_ACCOUNT',
                   outcome_status='CLOSED' if trade else 'OPEN_AT_WINDOW_END' if key in (new_open if isnew else old_open) else 'UNAVAILABLE',
                   net_pl_gbp=trade['net_pl_gbp'] if trade else None,
                   net_r_actual_entry_risk=trade['net_r_actual_entry_risk'] if trade else None,
                   net_r_entry_risk_budget=trade['net_r_entry_risk_budget'] if trade else None,
                   financing_gbp=trade['financing_gbp'] if trade else None,
                   exit_utc=trade['exit_utc'] if trade else None,
                   exit_reason=trade['exit_reason'] if trade else None,
                   path_status=trade['path_status'] if trade else None,
                   uncertain_account_lineage=trade['uncertain_account_lineage'] if trade else None,
                   adverse_entry_ticks=p.get('model_adverse_ticks'),
                   window_truncated=p.get('window_truncated', False),
                   account_lineage_uncertain_at_window_end=(result if isnew else control)['summary']['uncertain_account_lineage'])


def run(directory=None, offline=False, policy_filter=None, scenario_filter=None, mode_filter=None, window_filter=None):
    root = Path(__file__).parent
    directory = Path(directory or output_dir()).resolve()
    work = directory/'small_account_entry_limit_v1'
    work.mkdir(parents=True, exist_ok=True)
    resources = []; checks = []; audit_rows = []; started = e.stamp(datetime.now(e.UTC))
    job_lock = (work/'runner.lock').open('a')
    try:
        fcntl.flock(job_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        job_lock.close()
        message = dict(status='BUSY', version=VERSION, error='Another process holds the research lock', orders_sent=False)
        progress(**message); return message
    try:
        progress(status='RUNNING', phase='VERIFY_PINNED_INPUTS', started_at_utc=started,
                 result_ready=False, version=VERSION, controls_passed=0, lanes_completed=0)
        signals, saved, baseline = inputs.baseline(root)
        expected = control_checks.load(root); evidence = inputs.broker(root)
        reconciliation, _ = reconcile(evidence)
        e.require(reconciliation['status']=='PASS' and reconciliation['conversion_factor_markup_uniform'], 'Saved broker reconciliation failed')
        e.require(evidence['account']['currency']=='GBP' and not evidence['account']['hedgingEnabled'], 'Expected saved GBP nonhedging terms')
        e.require({i['name'] for i in evidence['instruments']}==set(e.PAIRS) and all(int(i['tradeUnitsPrecision'])==0 for i in evidence['instruments']), 'Expected whole-unit terms for all nine pairs')
        e.require(os.getenv('OANDA_ENV', os.getenv('OANDA_ENVIRONMENT', 'live')).lower().strip()=='live', 'Use OANDA_ENV=live for source parity')
        e.require(float(os.getenv('P32_GBP_INITIAL_BALANCE', '100'))==100., 'Fresh account protocol requires GBP100')
        scheduled = windows(baseline['evaluation_start'], baseline['historical_cutoff'])
        selected = [w for w in scheduled if not window_filter or w['window_id']==window_filter]
        e.require(bool(selected), 'Unknown window')
        e.require(policy_filter is None or policy_filter in POLICIES, 'Unknown policy')
        policies = [p for p in POLICIES if policy_filter is None or p in ('FIXED_10', policy_filter)]
        scenarios = [scenario_filter] if scenario_filter else list(admissions.SCENARIOS)
        modes = [mode_filter] if mode_filter else list(MODES)
        e.require(all(s in admissions.SCENARIOS for s in scenarios) and all(m in MODES for m in modes), 'Unknown scenario/funding mode')
        required = len(selected)*len(policies)*len(scenarios)*len(modes)
        config = dict(version=VERSION, input_pins=inputs.PINS, initial_balance_gbp=100.,
                      entry_rules=ENTRY_PROTOCOL, policies=list(policies), scenarios=scenarios, funding_modes=modes,
                      windows=selected, window_schedule='EVERY_ELIGIBLE_JAN_1_START_EXACT_FIVE_YEARS_PLUS_RECENT_2021',
                      window_entries='START_INCLUSIVE_END_EXCLUSIVE', account_at_start='FLAT_WITH_NO_CARRIED_POSITIONS_OR_PRIOR_PNL',
                      indicator_history='ORIGINAL_WARMED_SIGNALS_NOT_REWARMED_AT_ACCOUNT_START',
                      cutoff='VALUE_STILL_OPEN_POSITIONS_AT_CAUSAL_CUTOFF_QUOTES; NO_FORCED_CLOSE_OR_FUTURE_EXIT',
                      original_controls='ALL_16_FULL_HISTORY_10_TICK_LANES_BEFORE_WINDOW_REPLAYS',
                      expected_control_sha256=e.sha((root/'control_expectations.json').read_bytes()),
                      rolling_windows_overlap=True, rolling_windows_independent=False,
                      historical_financing_known=False, historical_broker_limits_known=False,
                      recent_data_untouched_holdout=False, automatic_policy_selection=False, lanes_required=required)
        signature = source_signature(root, config)
        atomic(work/'protocol.json', e.canon(dict(signature=signature, **config)))
        token = next((os.getenv(k, '').strip() for k in ('OANDA_TOKEN','OANDA_API_KEY','OANDA_API_TOKEN','OANDA_ACCESS_TOKEN') if os.getenv(k, '').strip()), '')
        e.require(offline or token, 'Set OANDA_TOKEN on the separate research service')
        reader = None if offline else e.CandleReader(token, 'https://api-fxtrade.oanda.com', float(os.getenv('P32_SIGNAL_REQUESTS_PER_SECOND', '4')))
        shared = directory/'entry_limit_v1'; shared.mkdir(parents=True, exist_ok=True)
        frozen = e.CandleStore(directory/'gbp_execution_paths_v2', 'https://api-fxtrade.oanda.com'); resources.append(frozen)
        fresh = e.CandleStore(shared/'new_execution_windows', 'https://api-fxtrade.oanda.com', reader); resources.append(fresh)
        value_store = e.CandleStore(directory/'gbp_valuation_windows_v2', 'https://api-fxtrade.oanda.com', reader); resources.append(value_store)
        legacy = e.CandleStore(directory, 'https://api-fxtrade.oanda.com') if (directory/'replay_candles.sqlite3').is_file() else None
        if legacy: resources.append(legacy)
        progress(phase='IMPORT_FROZEN_EXECUTION_CANDLES', signature=signature,
                 persistence=support.persistence(directory, create=True), broker_reconciliation='PASS',
                 controls_required=16, lanes_required=required, windows_required=len(selected))
        imported = {r[0] for r in frozen.db.execute('SELECT sha256 FROM imports')}
        if not all(p['sha256'] in imported for p in e.SEED_MANIFEST): frozen.import_seeds(root)
        inputs.import_additions(root, frozen)
        execution_windows = ExecutionWindows(frozen, fresh, progress)
        value_windows = ValuationWindows(value_store, frozen, legacy, progress)
        def path_progress(**values):
            values.pop('phase', None); progress(**values)
        paths = PolicyPaths(shared, saved, execution_windows, path_progress); resources.append(paths)
        funding = Funding(evidence['funding_schedules'])
        markup = round(reconciliation['observed_symmetric_conversion_factor_markup'], 8)
        memo_paths = {}
        def source(signal):
            key = paths.policy, signal['signal_key']
            if key not in memo_paths: memo_paths[key] = paths.get(signal)
            return memo_paths[key]
        # Controls never use window censoring: exact original accounting must reproduce.
        paths.policy = 'FIXED_10'
        for scenario in admissions.SCENARIOS:
            for mode in MODES:
                name = 'CONTROL__'+scenario+'__'+mode
                progress(phase='VERIFY_FULL_HISTORY_CONTROLS', scenario=scenario, funding_mode=mode,
                         controls_passed=len(checks), qualifications_required=len(signals))
                checkpoint = work/(name+'__'+signature+'.json.gz')
                result = checkpoint_load(checkpoint, signature)
                if result is None:
                    result = Replay(signals, source, Market(value_windows, progress), funding, evidence['instruments'],
                                    scenario, mode, markup, start=baseline['evaluation_start'], end=baseline['historical_cutoff'], progress=progress).run()
                    e.require(result['summary']['status']=='COMPLETE_CONDITIONAL', 'Full history control did not complete')
                checked = control_checks.check(result, expected); checks.append(checked)
                atomic(work/'control_checks.json', e.canon(checks))
                e.require(checked['status']=='PASS', 'Original 10-tick control mismatch: '+checked['lane']+' '+','.join(checked['failed_components']))
                if not checkpoint.exists(): save_checkpoint(checkpoint, signature, result)
                progress(phase='VERIFY_FULL_HISTORY_CONTROLS', controls_passed=len(checks))
        comparisons = []; grouped = []; period_rows = collections.defaultdict(list)
        primary_files = {}; detail_files = {}; equity_files = {}; changes_rows = []; evaluated_rows = []
        completed = 0
        for window in selected:
            cohort = select_signals(signals, window); references = {}
            for policy in policies:
                paths.policy = policy
                for scenario in scenarios:
                    for mode in modes:
                        lane = window['window_id']+'__'+policy+'__'+scenario+'__'+mode
                        labels = label(window, policy, scenario, mode)
                        progress(phase='REPLAY_FRESH_ACCOUNTS', **labels, lanes_completed=completed,
                                 qualifications_processed=0, qualifications_required=len(cohort))
                        checkpoint = work/(lane+'__'+signature+'.json.gz')
                        result = checkpoint_load(checkpoint, signature)
                        keep_nav = scenario==PRIMARY_SCENARIO and mode==PRIMARY_FUNDING
                        nav_file = work/(lane+'__'+signature+'_nav.csv.gz')
                        if result is None:
                            wrapped = WindowPaths(source, window, scenario)
                            stream = gzip.open(str(nav_file)+'.tmp', 'wt', encoding='utf-8', newline='') if keep_nav else None
                            writer = None
                            def sink(row):
                                nonlocal writer
                                if writer is None:
                                    writer = csv.DictWriter(stream, fieldnames=list(row)); writer.writeheader()
                                writer.writerow(row)
                            try:
                                result = Replay(cohort, wrapped.get, Market(value_windows, progress), funding, evidence['instruments'],
                                                scenario, mode, markup, initial=100., start=window['start'], end=window['end'],
                                                progress=progress, keep_nav=keep_nav, nav_sink=sink if keep_nav else None).run()
                            finally:
                                if stream: stream.close()
                            e.require(result['summary']['status']=='COMPLETE_CONDITIONAL', 'Fresh account incomplete: '+str(result['summary'].get('stopped_reason')))
                            if keep_nav:
                                Path(str(nav_file)+'.tmp').replace(nav_file)
                                result.update(nav_file=nav_file.name, nav_sha256=e.sha(nav_file.read_bytes()))
                            result['evaluated_paths'] = wrapped.evaluated
                            save_checkpoint(checkpoint, signature, result)
                        reference = result if policy=='FIXED_10' else references[scenario, mode]
                        row, periods, changes, checked = comparison(reference, result, window, policy, result['evaluated_paths'])
                        comparisons.append(row); audit_rows.append(dict(checked, **labels))
                        for kind, rows in periods.items(): period_rows[kind].extend(dict(r, **labels) for r in rows)
                        grouped.extend(dict(r, **labels) for r in reports.grouped(result, result['evaluated_paths']))
                        if keep_nav:
                            e.require(nav_file.is_file() and e.sha(nav_file.read_bytes())==result['nav_sha256'], 'Saved window equity missing or changed')
                            prefix = window['window_id']+'/'+policy
                            for kind in ('ledger','trades','cash_events','financing','open_positions'):
                                primary_files['primary/'+prefix+'/'+kind+'.csv'] = support.csv_bytes(result[kind])
                            changes_rows.extend(outcome_rows(changes, reference, result, result['evaluated_paths'], labels))
                            evaluated_rows.extend(dict(p, **labels) for p in result['evaluated_paths'].values())
                            equity_files[prefix] = nav_file.name
                        if policy=='FIXED_10':
                            references[scenario, mode] = {k:result[k] for k in ('summary','ledger','cash_events','trades','open_positions','evaluated_paths')}
                        detail_files[lane] = checkpoint.name; completed += 1
                        progress(phase='REPLAY_FRESH_ACCOUNTS', lanes_completed=completed, windows_completed=selected.index(window))
        summary = dict(status='COMPLETE_WITH_LIMITATIONS', version=VERSION, signature=signature,
                       started_at_utc=started, finished_at_utc=e.stamp(datetime.now(e.UTC)),
                       policies=policies, raw_signals=len(signals), strategy_count=32,
                       windows_completed=len(selected), rolling_five_year_windows=sum(w['window_kind']=='ROLLING_FIVE_YEAR' for w in selected),
                       lanes_completed=completed, lanes_required=required, controls_passed=len(checks),
                       window_account_audits_passed=len(audit_rows),
                       complete_default_experiment=(len(selected)==18 and policies==list(POLICIES) and
                                                    scenarios==list(admissions.SCENARIOS) and modes==list(MODES)),
                       capacity_affected_window_lanes=sum(r['capacity_limit_affected'] for r in comparisons),
                       primary_scenario=PRIMARY_SCENARIO, primary_financing=PRIMARY_FUNDING,
                       valuation_policy=value_windows.summary(), original_unknown_entries=11, original_unresolved_paths=170,
                       orders_supported=False, orders_sent=False, live_services_contacted=False,
                       full_portfolio_validated=False, automatic_policy_selection=False,
                       recent_data_untouched_holdout=False, overlapping_windows_are_independent=False,
                       actual_broker_returns=False, historical_financing_known=False,
                       uncertainty='Fresh flat accounts isolate size/compounding effects; original source/path/proxy/frozen-terms assumptions remain conditional.')
        files = {'RUN_COMPLETE.json':e.canon(summary), 'protocol.json':(work/'protocol.json').read_bytes(),
                 'control_checks.json':e.canon(checks), 'window_account_audits.csv':support.csv_bytes(audit_rows),
                 'window_comparison.csv':support.csv_bytes(comparisons), 'rolling_window_summary.csv':support.csv_bytes(robust_summary(comparisons)),
                 'recent_comparison.csv':support.csv_bytes([r for r in comparisons if r['window_kind']=='RECENT_DIAGNOSTIC']),
                 'grouped_trade_stats.csv':support.csv_bytes(grouped),
                 'admission_outcomes_primary.csv':support.csv_bytes(changes_rows),
                 'evaluated_paths_primary.csv':support.csv_bytes(evaluated_rows),
                 'broker_reconciliation.json':e.canon(reconciliation),
                 'new_execution_source_differences.csv':support.csv_bytes(execution_windows.differences()),
                 'valuation_source_differences.csv':support.csv_bytes(value_windows.differences()),
                 'source_request_receipts.json':e.canon(reader.receipts if reader else []),
                 'persistence.json':e.canon(support.persistence(directory)),
                 'detail_downloads.json':e.canon({k:'/details/'+k+'.json.gz' for k in detail_files}),
                 'equity_downloads.json':e.canon({k:'/equity/'+k+'.csv.gz' for k in equity_files}),
                 'REPORT_GUIDE.md':(root/'REPORT_GUIDE.md').read_bytes(), **primary_files}
        for kind, rows in period_rows.items(): files[kind+'.csv'] = support.csv_bytes(rows)
        files['report_manifest.json'] = e.canon({n:dict(bytes=len(v), sha256=e.sha(v)) for n,v in files.items()})
        atomic(work/'detail_files.json', e.canon(detail_files)); atomic(work/'equity_files.json', e.canon(equity_files))
        progress(phase='PACKAGE_RESULTS'); write_zip(work/RESULT, files)
        atomic(work/'completed.json', e.canon(summary))
        progress(**summary, phase='FINISHED', result_ready=True, result='/results.zip')
        return summary
    except Exception as exc:
        failure = dict(status='ERROR', version=VERSION,
                       error=str(exc) if isinstance(exc, (ValueError,e.CandleFetchError)) else type(exc).__name__,
                       controls_passed=sum(r['status']=='PASS' for r in checks), control_checks=checks,
                       started_at_utc=started, orders_sent=False, full_portfolio_validated=False)
        if isinstance(exc, e.NeedData): failure['missing_candle_request'] = exc.identity
        write_zip(work/RESULT, {'ERROR.json':e.canon(failure), 'control_checks.json':e.canon(checks),
                              'window_account_audits.csv':support.csv_bytes(audit_rows)})
        progress(**failure, phase='FAILED', result_ready=True, result='/results.zip'); return failure
    finally:
        for resource in reversed(resources): resource.close()
        fcntl.flock(job_lock, fcntl.LOCK_UN); job_lock.close()


def start_once():
    with LOCK:
        if STATE['status']!='IDLE': return
        STATE.update(status='RUNNING', phase='STARTING')
    threading.Thread(target=lambda:run(offline=os.getenv('P32_SIGNAL_OFFLINE','').lower() in ('1','true')),
                     daemon=True, name='p32-small-account').start()


def application(environ, start_response):
    method, path = environ.get('REQUEST_METHOD','GET'), environ.get('PATH_INFO','/')
    code, body, content, file = '200 OK', b'', 'application/json', None
    if method not in ('GET','HEAD'):
        code, body = '405 Method Not Allowed', e.canon(dict(error='GET/HEAD only; no order routes'))
    elif path in ('/','/health'):
        body = e.canon(dict(service=VERSION, orders_supported=False, orders_sent=False,
                            start_once='/status', result='/results.zip', policies=list(POLICIES),
                            rolling_windows=17, recent_windows=1, window_lanes=864, original_control_lanes=16,
                            persistence=support.persistence(output_dir())))
    elif path=='/status':
        if method=='GET': start_once()
        with LOCK: body = e.canon(dict(STATE))
    elif path in ('/results.zip','/results','/download'):
        if STATE.get('result_ready'): file = work_dir()/RESULT
    elif path in ('/details','/equity'):
        name = 'detail_files.json' if path=='/details' else 'equity_files.json'
        mapping = work_dir()/name
        prefix, suffix = ('/details/','.json.gz') if path=='/details' else ('/equity/','.csv.gz')
        body = e.canon({k:prefix+k+suffix for k in json.loads(mapping.read_bytes())}) if mapping.is_file() else e.canon(dict(message='Available after completion'))
    elif path.startswith(('/details/','/equity/')):
        isdetail = path.startswith('/details/')
        mapping = work_dir()/('detail_files.json' if isdetail else 'equity_files.json')
        key = path.removeprefix('/details/' if isdetail else '/equity/').removesuffix('.json.gz' if isdetail else '.csv.gz')
        if mapping.is_file():
            selected = json.loads(mapping.read_bytes()).get(key)
            if selected: file = work_dir()/selected
    else: code, body = '404 Not Found', e.canon(dict(error='Unknown route'))
    download = method in ('GET','HEAD') and (path in ('/results.zip','/results','/download') or path.startswith(('/details/','/equity/')))
    if download:
        if file is None or not file.is_file():
            code, body, file = '409 Conflict', e.canon(dict(message='Download not ready; check /status')), None
        else: content = 'application/gzip' if file.suffix=='.gz' else 'application/zip'
    headers = [('Content-Type',content), ('Cache-Control','no-store'), ('Content-Length',str(file.stat().st_size if file else len(body)))]
    if file: headers.append(('Content-Disposition','attachment; filename='+file.name))
    if code.startswith('405'): headers.append(('Allow','GET, HEAD'))
    start_response(code, headers)
    if method=='HEAD': return [b'']
    if file is None: return [body]
    def chunks():
        with file.open('rb') as stream:
            while block:=stream.read(1024*1024): yield block
    return chunks()


class QuietHandler(WSGIRequestHandler):
    def log_message(self, *args): pass


def main():
    global DIRECTORY
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', action='store_true'); parser.add_argument('--offline', action='store_true')
    parser.add_argument('--output'); parser.add_argument('--policy', choices=POLICIES)
    parser.add_argument('--scenario', choices=admissions.SCENARIOS); parser.add_argument('--funding-mode', choices=MODES)
    parser.add_argument('--window'); args = parser.parse_args(); DIRECTORY = args.output
    if args.run:
        result = run(args.output, args.offline, args.policy, args.scenario, args.funding_mode, args.window)
        print(json.dumps(result, indent=2)); raise SystemExit(0 if result['status']=='COMPLETE_WITH_LIMITATIONS' else 1)
    e.require(not(args.offline or args.policy or args.scenario or args.funding_mode or args.window), 'Filters require --run; hosted service uses frozen full protocol')
    with make_server('0.0.0.0', int(os.getenv('PORT','8080')), application, handler_class=QuietHandler) as server:
        print(VERSION+' ready', flush=True); server.serve_forever()


if __name__=='__main__': main()
