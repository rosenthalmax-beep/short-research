#!/usr/bin/env python3
"""P32 conditional GBP account replay. Separate research service; candle GETs only."""
import argparse
import collections
import csv
import gzip
import io
import json
import os
import shutil
import sqlite3
import tempfile
import threading
import uuid
import zipfile
from datetime import datetime
from pathlib import Path
from wsgiref.simple_server import make_server, WSGIRequestHandler
import execution as e
import admissions
import inputs
from broker_reconcile import reconcile
from funding import Funding, MODES, rollovers
from market import Market
from accounting import Replay
from valuation_sources import ValuationWindows

VERSION = 'P32_GBP_ACCOUNT_REPLAY_V2_2026_10_16_SOURCE_WINDOWS'
RESULT = 'P32_GBP_ACCOUNT_REPLAY_RESULTS.zip'
EQUITY = 'P32_GBP_ACCOUNT_REPLAY_EQUITY.zip'
SOURCES = 'P32_GBP_ACCOUNT_REPLAY_SOURCES.zip'
LOCK = threading.RLock()
STATE = dict(status='IDLE', version=VERSION, orders_supported=False,
             scope='WARMED_RAW_SIGNALS_CONDITIONAL_GBP_NAV_MARGIN_AND_FUNDING', full_portfolio_validated=False)
DIRECTORY = None


def progress(**values):
    with LOCK:
        if values.get('phase') and STATE.get('phase') != values['phase']:
            for k in ('quote_pair', 'quote_from', 'quote_to', 'qualifications_processed', 'through_utc'):
                STATE.pop(k, None)
        STATE.update(values, last_progress_utc=e.stamp(datetime.now(e.UTC)))


def output_dir():
    value = DIRECTORY or os.getenv('P32_GBP_OUTPUT_DIR') or os.getenv('P32_SIGNAL_OUTPUT_DIR')
    return Path(value or ('/data/p32_signal_admission' if Path('/data').is_dir() else str(Path(tempfile.gettempdir()) / 'p32_signal_admission'))).resolve()


def csv_bytes(rows):
    fields = list(dict.fromkeys(k for r in rows for k in r)) or ['empty']
    stream = io.StringIO(newline=''); writer = csv.DictWriter(stream, fieldnames=fields); writer.writeheader()
    for row in rows:
        writer.writerow({k: json.dumps(v, sort_keys=True) if isinstance(v, (dict, list)) else v for k, v in row.items()})
    return stream.getvalue().encode()


def atomic(path, content):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_bytes(content); temporary.replace(path)


def source_signature(root, config):
    names = ('app.py', 'accounting.py', 'market.py', 'funding.py', 'broker_reconcile.py',
             'inputs.py', 'pinned_inputs.py', 'execution.py', 'admissions.py', 'history.py', 'locked_rules.py', 'reference_payload.py', 'valuation_sources.py')
    return e.sha(e.canon(dict(config=config, code={n: e.sha((root / n).read_bytes()) for n in names})))


def persistence(directory, create=False):
    directory = Path(directory)
    marker = directory / 'gbp_persistence_marker.json'
    if create and not marker.exists():
        atomic(marker, e.canon(dict(marker=str(uuid.uuid4()), created_utc=e.stamp(datetime.now(e.UTC)))))
    info = json.loads(marker.read_bytes()) if marker.exists() else None
    mount = None
    try:
        for line in Path('/proc/self/mountinfo').read_text().splitlines():
            left, right = line.split(' - ', 1)
            target = left.split()[4].replace('\\040', ' ')
            if str(directory) == target or str(directory).startswith(target.rstrip('/') + '/'):
                if mount is None or len(target) > len(mount['target']):
                    mount = dict(target=target, filesystem=right.split()[0])
    except (OSError, ValueError, IndexError):
        pass
    return dict(output_directory=str(directory), persistence_marker=info,
                containing_mount=mount, separate_mount_detected=bool(mount and mount['target'] != '/'),
                persistence_across_redeploy_not_proved=True,
                instruction='Keep this directory on a mounted persistent volume; compare the marker after a redeploy. No MID history regeneration is used by this update.')


class Paths:
    def __init__(self, directory, saved, store, signals=()):
        self.rows = saved; self.store = store; self.data = e.load_reference()
        self.computed = 0; self.saved_hits = 0
        self.db = sqlite3.connect(str(directory / 'gbp_new_candidate_paths.sqlite3'))
        self.db.execute('CREATE TABLE IF NOT EXISTS paths(key TEXT PRIMARY KEY, body BLOB, sha256 TEXT)'); self.db.commit()
        self.signature = e.sha(e.canon(dict(core=e.sha((Path(__file__).parent / 'execution.py').read_bytes()),
                                         baseline=inputs.PINS['warmed_signal_results.zip'],
                                         execution_source_scope='ISOLATED_V2_FROZEN_SEEDS_AND_ADMISSION_89')))
        for signal in signals:
            if signal['signal_key'] not in self.rows:
                identity = e.sha(e.canon(dict(signature=self.signature, signal=signal)))
                stored = self.db.execute('SELECT body,sha256 FROM paths WHERE key=?', (identity,)).fetchone()
                if stored:
                    e.require(e.sha(stored[0]) == stored[1], 'Extra path checkpoint checksum failed')
                    self.rows[signal['signal_key']] = json.loads(stored[0])

    def get(self, signal):
        key = signal['signal_key']
        if key in self.rows:
            self.saved_hits += 1
            return self.rows[key]
        identity = e.sha(e.canon(dict(signature=self.signature, signal=signal)))
        stored = self.db.execute('SELECT body,sha256 FROM paths WHERE key=?', (identity,)).fetchone()
        if stored:
            e.require(e.sha(stored[0]) == stored[1], 'Extra path checkpoint checksum failed')
            row = json.loads(stored[0])
        else:
            row = e.replay_one(self.store, admissions.candidate_data(self.data, signal), 0, e.VARIANTS[1])
            row = {k: v for k, v in row.items() if not k.startswith('archived_') and k not in ('index', 'extended_beyond_archived_exit')}
            row['signal_key'] = key
            if row['status'] == 'NEEDS_CANDLE_DATA':
                raise e.NeedData(row['pending_request'])
            if row['status'] == 'ERROR':
                raise e.CandleFetchError('Additional candidate path could not be computed: ' + row.get('error', 'unknown'))
            raw = e.canon(row)
            self.db.execute('INSERT OR REPLACE INTO paths VALUES (?,?,?)', (identity, raw, e.sha(raw))); self.db.commit()
            self.computed += 1
        self.rows[key] = row
        progress(additional_candidate_paths=self.computed, candidate_paths_available=len(self.rows))
        return row

    def close(self):
        self.db.close()


def potential_funding_coverage(signals, paths, funding, end):
    needed = set()
    for s in signals:
        p = paths.get(s['signal_key'])
        if not p or p['status'] in ('ENTRY_PROXY_REJECTED', 'UNRESOLVED_ENTRY'):
            continue
        start = e.dt(s['entry_utc'])
        finish = e.dt(p['interval_end_utc']) if p.get('interval_end_utc') else end
        for when in rollovers(start, finish):
            if start < when < finish and when.astimezone(e.NY).weekday() < 5:
                needed.add((when.astimezone(e.NY).date().isoformat(), s['pair']))
    rows = []
    for date, pair in sorted(needed | funding.coverage):
        item = funding.historical.get((date, pair))
        rows.append(dict(ny_date=date, pair=pair, supplied=bool(item),
                         long_rate=item['long_rate'] if item else None,
                         short_rate=item['short_rate'] if item else None,
                         charged_days=item['days'] if item else None,
                         source=item['source'] if item else None,
                         basis='FINALIZED_DAILY_RATE' if item else '',
                         rate_basis='ANNUAL_ONE_DAY_BASE_NOTIONAL' if item else '',
                         independent_verification=False,
                         scope='POTENTIAL_OBSERVED_CANDIDATE_ROLLOVER_NOT_ALL_ACTUALLY_ADMITTED'))
    return rows


def grouped_trade_stats(rows):
    groups = collections.defaultdict(list)
    for row in rows:
        for kind, key in (('strategy', row['strategy_id']), ('pair_side', row['pair'] + '_' + row['side']),
                          ('entry_year', row['entry_utc'][:4])):
            groups[(row['scenario'], row['financing_mode'], kind, key)].append(row)
    result = []
    for (scenario, mode, kind, key), group in sorted(groups.items()):
        result.append(dict(scenario=scenario, financing_mode=mode, grouping=kind, group=key,
                           closed_trades=len(group), net_wins=sum(r['net_pl_gbp'] > 0 for r in group),
                           net_win_rate_pct=100 * sum(r['net_pl_gbp'] > 0 for r in group) / len(group),
                           price_pl_gbp=sum(r['price_pl_gbp'] for r in group),
                           financing_gbp=sum(r['financing_gbp'] for r in group),
                           net_pl_gbp=sum(r['net_pl_gbp'] for r in group),
                           mean_net_r_actual_entry_risk=sum(r['net_r_actual_entry_risk'] for r in group) / len(group),
                           scope='Closed trades only; no allocation of finance on positions still open'))
    return result


def write_zip(path, files):
    temp = path.with_suffix(path.suffix + '.tmp')
    with zipfile.ZipFile(temp, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        for name, raw in sorted(files.items()):
            z.writestr(name, raw)
    temp.replace(path)


def run(directory=None, offline=False, scenario_filter=None, mode_filter=None):
    root = Path(__file__).parent
    directory = Path(directory or output_dir()); directory.mkdir(parents=True, exist_ok=True)
    progress(status='RUNNING', phase='VERIFY_PINNED_INPUTS', version=VERSION,
             started_at_utc=e.stamp(datetime.now(e.UTC)))
    signals, saved_paths, baseline = inputs.baseline(root)
    evidence = inputs.broker(root)
    reconciliation, broker_checks = reconcile(evidence)
    e.require(reconciliation['status'] == 'PASS', 'Saved broker cash/margin reconciliation failed')
    e.require(reconciliation['conversion_factor_markup_uniform'], 'Saved conversion markups differ; do not apply a single inferred markup')
    e.require(evidence['account']['currency'] == 'GBP' and not evidence['account']['hedgingEnabled'], 'Expected saved GBP nonhedging account')
    e.require({i['name'] for i in evidence['instruments']} == set(e.PAIRS) and all(int(i['tradeUnitsPrecision']) == 0 for i in evidence['instruments']), 'Expected whole-unit instrument terms for all nine portfolio pairs')
    e.require(os.getenv('OANDA_ENV', os.getenv('OANDA_ENVIRONMENT', 'live')).lower().strip() == 'live', 'Use OANDA_ENV=live to match source evidence')
    initial = float(os.getenv('P32_GBP_INITIAL_BALANCE', '100'))
    e.require(0 < initial <= 1e8, 'Invalid initial GBP balance')
    # Round only the inferred symmetric markup, not any broker P/L factor.
    markup = round(reconciliation['observed_symmetric_conversion_factor_markup'], 8)
    funding_file = root / 'funding_rates.csv'
    funding = Funding(evidence['funding_schedules'], funding_file)
    config = dict(initial_balance_gbp=initial, markup=markup,
                  funding_file_sha256=e.sha(funding_file.read_bytes()) if funding_file.is_file() else None,
                  scenario_filter=scenario_filter, mode_filter=mode_filter,
                  valuation_max_quote_age_seconds=7 * 86400)
    signature = source_signature(root, config)
    scenarios = [scenario_filter] if scenario_filter else list(admissions.SCENARIOS)
    modes = [mode_filter] if mode_filter else list(MODES) + (['HISTORICAL_FILE'] if funding.historical else [])
    e.require(all(s in admissions.SCENARIOS for s in scenarios) and all(m in MODES + ('HISTORICAL_FILE',) for m in modes), 'Unsupported scenario/mode filter')
    token = next((os.getenv(k, '').strip() for k in ('OANDA_TOKEN', 'OANDA_API_KEY', 'OANDA_API_TOKEN', 'OANDA_ACCESS_TOKEN') if os.getenv(k, '').strip()), '')
    e.require(offline or token, 'Set OANDA_TOKEN on the separate research service')
    reader = None if offline else e.CandleReader(token, 'https://api-fxtrade.oanda.com', float(os.getenv('P32_SIGNAL_REQUESTS_PER_SECOND', '4')))
    # Keep the V1 database intact. It may contain canonical valuation windows
    # that overlap its seed candles. Reuse only exact whole-window responses.
    legacy = e.CandleStore(directory, 'https://api-fxtrade.oanda.com') if (directory / 'replay_candles.sqlite3').is_file() else None
    store = e.CandleStore(directory / 'gbp_execution_paths_v2', 'https://api-fxtrade.oanda.com', reader)
    valuation_store = e.CandleStore(directory / 'gbp_valuation_windows_v2', 'https://api-fxtrade.oanda.com', reader)
    valuation_windows = ValuationWindows(valuation_store, store, legacy, progress)
    paths = Paths(directory, saved_paths, store, signals)
    market = Market(valuation_windows, progress)
    results = []; report_ready = False
    persistence_info = persistence(directory, create=True)
    progress(phase='IMPORT_SAVED_EXECUTION_CANDLES', persistence=persistence_info,
             broker_reconciliation=reconciliation['status'])
    try:
        store.import_seeds(root)
        inputs.import_additions(root, store)
        total = len(scenarios) * len(modes)
        equity_files = {}
        for scenario in scenarios:
            for mode in modes:
                name = scenario + '__' + mode
                checkpoint = directory / ('gbp_' + name + '_' + signature + '.json.gz')
                progress(phase='REPLAY_GBP_ACCOUNTS', scenario=scenario, funding_mode=mode,
                         scenarios_completed=len(results), scenarios_required=total,
                         qualifications_processed=0, qualifications_required=len(signals))
                if checkpoint.exists():
                    obj = json.loads(gzip.decompress(checkpoint.read_bytes()))
                    e.require(obj['signature'] == signature and e.sha(e.canon(obj['result'])) == obj['result_sha256'], 'Scenario checkpoint checksum failed')
                    result = obj['result']
                    result['scenario_checkpoint_reused'] = True
                    # A data-incomplete checkpoint is never treated as a finished
                    # scenario; adding historical rows changes the signature.
                    if result['summary']['status'] != 'COMPLETE_CONDITIONAL':
                        result = None
                else:
                    result = None
                if result is None:
                    keep_nav = scenario == 'END_ASC' and mode in MODES[:2]
                    nav_file = directory / ('gbp_nav_' + name + '_' + signature + '.csv.gz')
                    nav_stream = gzip.open(str(nav_file) + '.tmp', 'wt', encoding='utf-8', newline='') if keep_nav else None
                    nav_writer = None
                    def nav_sink(row):
                        nonlocal nav_writer
                        if nav_writer is None:
                            nav_writer = csv.DictWriter(nav_stream, fieldnames=list(row)); nav_writer.writeheader()
                        nav_writer.writerow(row)
                    try:
                        result = Replay(signals, paths.get, market, funding, evidence['instruments'], scenario, mode,
                                        markup, initial=initial, progress=progress,
                                        start=baseline['evaluation_start'], end=baseline['historical_cutoff'],
                                        keep_nav=keep_nav, nav_sink=nav_sink if keep_nav else None).run()
                    finally:
                        if nav_stream:
                            nav_stream.close()
                    if keep_nav:
                        Path(str(nav_file) + '.tmp').replace(nav_file)
                        result['nav_samples_saved_file'] = nav_file.name
                        result['nav_samples_saved_sha256'] = e.sha(nav_file.read_bytes())
                    result['valuation_source_summary'] = market.summary()
                    result['scenario_checkpoint_reused'] = False
                    raw = e.canon(dict(signature=signature, result=result, result_sha256=e.sha(e.canon(result))))
                    atomic(checkpoint, gzip.compress(raw, compresslevel=6))
                results.append(result)
                if result.get('nav_samples_saved_file'):
                    nav_file = directory / result['nav_samples_saved_file']
                    e.require(nav_file.is_file() and e.sha(nav_file.read_bytes()) == result['nav_samples_saved_sha256'], 'NAV samples checkpoint missing or changed')
                    equity_files[name + '_nav_samples.csv'] = nav_file
                progress(scenarios_completed=len(results), last_scenario_status=result['summary']['status'])
        summaries = [r['summary'] for r in results]
        all_rows = {k: [] for k in ('ledger', 'trades', 'financing', 'cash_events', 'monthly_observations', 'open_positions')}
        for result in results:
            s = result['summary']
            for kind in all_rows:
                all_rows[kind].extend(dict(row, scenario=s['scenario'], financing_mode=s['financing_mode']) for row in result[kind])
        coverage = potential_funding_coverage(signals, paths.rows, funding, e.dt(baseline['historical_cutoff']))
        summary = dict(status='COMPLETE_WITH_LIMITATIONS', version=VERSION,
                       started_at_utc=STATE['started_at_utc'], finished_at_utc=e.stamp(datetime.now(e.UTC)),
                       raw_signals=len(signals), strategy_count=32,
                       baseline_price_paths=3506, available_price_paths=len(paths.rows),
                       new_candidate_paths=paths.computed,
                       original_unresolved_path_count=170, original_unknown_entry_count=11,
                       evaluation_start=baseline['evaluation_start'], historical_cutoff=baseline['historical_cutoff'],
                       scenario_status_counts=dict(collections.Counter(s['status'] for s in summaries)),
                       historical_financing_file_supplied=bool(funding.historical),
                       potential_historical_pair_rollover_dates=len(coverage),
                       supplied_potential_historical_pair_rollover_dates=sum(r['supplied'] for r in coverage),
                       historical_financing_lane='RUN_WITH_EXPLICIT_MISSING_RATE_STOPS' if funding.historical else 'NOT_RUN_NO_HISTORICAL_RATE_FILE',
                       saved_account_terms_at=evidence['collected_at_utc'],
                       saved_account_currency='GBP', initial_balance_gbp=initial,
                       frozen_conversion_factor_markup=markup,
                       broker_reconciliation=reconciliation,
                       full_portfolio_validated=False, actual_broker_returns=False,
                       scenario_results_are_assumptions_not_bounds=True,
                       margin_and_conversion_terms_applied_across_history_are_frozen=True,
                       orders_supported=False, orders_sent=False, live_services_contacted=False,
                       no_account_id_required=True, only_new_network_requests='APPROVED_OANDA_HISTORICAL_CANDLE_GETS',
                       no_strategy_regeneration_or_mid_history_download=True,
                       signature=signature, pins=inputs.PINS, persistence=persistence_info,
                       valuation=market.summary(),
                       valuation_source_policy=valuation_windows.summary(),
                       scenario_source_summaries=[r.get('valuation_source_summary') for r in results],
                       reused_scenario_checkpoints=sum(r.get('scenario_checkpoint_reused', False) for r in results),
                       equity_sampling='Full M15/event samples for prespecified END_ASC zero/snapshot controls in /equity.zip; summaries for all lanes.',
                       offline=offline)
        comparison = []
        for scenario in scenarios:
            zero = next((s for s in summaries if s['scenario'] == scenario and s['financing_mode'] == MODES[0]), None)
            for s in [v for v in summaries if v['scenario'] == scenario]:
                comparison.append(dict(scenario=scenario, financing_mode=s['financing_mode'],
                                       status=s['status'], admitted=s['decisions'].get('ADMITTED', 0),
                                       margin_rejections=s['decisions'].get('REJECTED_INSUFFICIENT_MARGIN', 0),
                                       financing_gbp=s['financing_gbp'], conditional_ending_nav_gbp=s['conditional_ending_nav_gbp'],
                                       ending_nav_change_vs_zero_gbp=s['conditional_ending_nav_gbp'] - zero['conditional_ending_nav_gbp'] if zero and s['conditional_ending_nav_gbp'] is not None and zero['conditional_ending_nav_gbp'] is not None else None,
                                       interpretation='Includes financing-driven changes in units, gates and margin, not a fixed-trade fee subtraction'))
        files = {
            'RUN_COMPLETE.json': e.canon(summary), 'READ_FIRST.md': (root / 'REPORT_GUIDE.md').read_bytes(),
            'broker_reconciliation_summary.json': e.canon(reconciliation),
            'broker_reconciliation_checks.csv': csv_bytes(broker_checks),
            'gbp_scenario_summaries.json': e.canon(summaries), 'gbp_scenario_comparison.csv': csv_bytes(comparison),
            'gbp_admission_ledger.csv': csv_bytes(all_rows['ledger']), 'gbp_closed_trades.csv': csv_bytes(all_rows['trades']),
            'gbp_financing_events.csv': csv_bytes(all_rows['financing']), 'gbp_cash_events.csv': csv_bytes(all_rows['cash_events']),
            'gbp_monthly_observations.csv': csv_bytes(all_rows['monthly_observations']),
            'gbp_open_positions_at_cutoff.csv': csv_bytes(all_rows['open_positions']),
            'gbp_trade_group_summaries.csv': csv_bytes(grouped_trade_stats(all_rows['trades'])),
            'historical_funding_coverage.csv': csv_bytes(coverage),
            'account_and_instrument_terms.json': e.canon(dict(collected_at=evidence['collected_at_utc'], account=evidence['account'], instruments=evidence['instruments'], funding_schedules=evidence['funding_schedules'])),
            'baseline_run_summary.json': e.canon(baseline),
            'raw_signal_witnesses.jsonl': b''.join(e.canon(s) + b'\n' for s in signals),
            'candidate_price_paths.csv': csv_bytes(list(paths.rows.values())),
            'candle_request_receipts.json': e.canon(reader.receipts if reader else []),
            'persistence_status.json': e.canon(persistence_info),
            'valuation_vs_frozen_source_differences.csv': csv_bytes(valuation_windows.differences()),
            'valuation_source_policy.json': e.canon(valuation_windows.summary()),
            'valuation_window_origins.json': e.canon(valuation_windows.origins()),
        }
        # Source references are small; full raw MBA blocks are kept separately.
        index = []
        for key, identity, checksum, origin in store.db.execute('SELECT key,identity,sha256,origin FROM blocks ORDER BY key'):
            index.append(dict(key=key, identity=json.loads(identity), sha256=checksum, origin=origin))
        files['available_candle_source_index.json'] = e.canon(index)
        files['valuation_candle_source_index.json'] = e.canon([
            dict(key=key, identity=json.loads(identity), sha256=checksum, origin=origin)
            for key, identity, checksum, origin in valuation_store.db.execute('SELECT key,identity,sha256,origin FROM blocks ORDER BY key')])
        files['report_manifest.json'] = e.canon({n: dict(bytes=len(b), sha256=e.sha(b)) for n, b in files.items()})
        progress(phase='PACKAGE_REPORTS')
        write_zip(directory / RESULT, files); report_ready = True
        temporary = directory / (EQUITY + '.tmp')
        with zipfile.ZipFile(temporary, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as z:
            if not equity_files:
                z.writestr('README.txt', b'No full NAV samples were requested by this filtered run.')
            for name, local in equity_files.items():
                with gzip.open(local, 'rb') as source, z.open(name, 'w', force_zip64=True) as target:
                    shutil.copyfileobj(source, target, 1024 * 1024)
        temporary.replace(directory / EQUITY)
        progress(result_ready=True, equity_ready=True, result='/results.zip', equity='/equity.zip', phase='PACKAGE_SOURCE_ARCHIVE')
        if os.getenv('P32_GBP_PACKAGE_SOURCES', '1').lower() in ('1', 'true'):
            temporary = directory / (SOURCES + '.tmp')
            with zipfile.ZipFile(temporary, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as z:
                store.export(z)
                class Prefixed:
                    def writestr(self, name, raw):
                        z.writestr('valuation/' + name, raw)
                valuation_store.export(Prefixed())
                z.writestr('source_provenance.json', e.canon(dict(signature=signature, pins=inputs.PINS, seeds=e.SEED_MANIFEST)))
            temporary.replace(directory / SOURCES)
        atomic(directory / 'gbp_completion.json', e.canon(summary))
        progress(**summary, phase='FINISHED', result_ready=True, equity_ready=True,
                 sources_ready=(directory / SOURCES).is_file(), result='/results.zip')
        return summary
    except Exception as exc:
        message = str(exc) if isinstance(exc, (ValueError, e.CandleFetchError)) else type(exc).__name__
        error = dict(status='ERROR', version=VERSION, error=message,
                     reports_preserved=report_ready, orders_sent=False, signature=signature,
                     failed_source_context={k: STATE.get(k) for k in ('quote_pair', 'quote_from', 'quote_to', 'scenario', 'funding_mode', 'through_utc')},
                     valuation_source_policy=valuation_windows.summary())
        if isinstance(exc, e.NeedData):
            error['pending_request'] = exc.identity
        if report_ready:
            with zipfile.ZipFile(directory / RESULT, 'a', zipfile.ZIP_DEFLATED) as z:
                z.writestr('ERROR.json', e.canon(error))
        else:
            write_zip(directory / RESULT, {'ERROR.json': e.canon(error),
                      'broker_reconciliation_summary.json': e.canon(reconciliation),
                      'valuation_vs_frozen_source_differences.csv': csv_bytes(valuation_windows.differences()),
                      'completed_scenario_summaries.json': e.canon([r['summary'] for r in results])})
        progress(**error, phase='FAILED', result_ready=True, result='/results.zip')
        raise
    finally:
        paths.close(); store.close(); valuation_store.close()
        if legacy is not None:
            legacy.close()


def start():
    with LOCK:
        if STATE['status'] != 'IDLE':
            return
        STATE.update(status='RUNNING', phase='STARTING')
    def worker():
        try:
            run(offline=os.getenv('P32_SIGNAL_OFFLINE', '').lower() in ('true', '1'))
        except Exception as exc:
            if STATE.get('status') != 'ERROR':
                progress(status='ERROR', phase='FAILED', error=str(exc) if isinstance(exc, ValueError) else type(exc).__name__)
    threading.Thread(target=worker, daemon=True, name='p32-gbp-account-replay').start()


def application(environ, start_response):
    method, path = environ.get('REQUEST_METHOD', 'GET'), environ.get('PATH_INFO', '/')
    kind = 'application/json'; download = None
    if method not in ('GET', 'HEAD'):
        code, body = '405 Method Not Allowed', e.canon(dict(error='GET/HEAD only'))
    elif path in ('/', '/health'):
        code, body = '200 OK', e.canon(dict(service=VERSION, orders_supported=False,
                                          start_once='/status', result='/results.zip', equity='/equity.zip',
                                          persistence=persistence(output_dir())))
    elif path == '/status':
        if method == 'GET':
            start()
        with LOCK:
            snapshot = dict(STATE)
        if snapshot.get('last_progress_utc'):
            snapshot['seconds_since_last_progress'] = round((datetime.now(e.UTC) - e.dt(snapshot['last_progress_utc'])).total_seconds(), 1)
        code, body = '200 OK', e.canon(snapshot)
    elif path in ('/results.zip', '/equity.zip', '/sources.zip'):
        name = {'/results.zip': RESULT, '/equity.zip': EQUITY, '/sources.zip': SOURCES}[path]
        ready = STATE.get({'/results.zip': 'result_ready', '/equity.zip': 'equity_ready', '/sources.zip': 'sources_ready'}[path])
        candidate = output_dir() / name
        if ready and candidate.is_file():
            code, body, kind, download = '200 OK', b'', 'application/zip', candidate
        else:
            code, body = '409 Conflict', e.canon(dict(message='Not ready; check /status'))
    else:
        code, body = '404 Not Found', e.canon(dict(error='Unknown route'))
    headers = [('Content-Type', kind), ('Content-Length', str(download.stat().st_size if download else len(body))), ('Cache-Control', 'no-store')]
    if download:
        headers.append(('Content-Disposition', 'attachment; filename=' + download.name))
    if code.startswith('405'):
        headers.append(('Allow', 'GET, HEAD'))
    start_response(code, headers)
    if download and method == 'GET':
        def chunks():
            with download.open('rb') as f:
                while True:
                    block = f.read(65536)
                    if not block:
                        break
                    yield block
        return chunks()
    return [b'' if method == 'HEAD' else body]


class QuietHandler(WSGIRequestHandler):
    def log_message(self, *args):
        pass


def main():
    global DIRECTORY
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', action='store_true'); parser.add_argument('--offline', action='store_true')
    parser.add_argument('--reconcile-only', action='store_true'); parser.add_argument('--output')
    parser.add_argument('--scenario', choices=admissions.SCENARIOS)
    parser.add_argument('--funding-mode', choices=MODES + ('HISTORICAL_FILE',))
    args = parser.parse_args(); DIRECTORY = args.output
    if args.reconcile_only:
        summary, _ = reconcile(inputs.broker(Path(__file__).parent))
        print(json.dumps(summary, indent=2)); return
    if args.run:
        print(json.dumps(run(offline=args.offline, scenario_filter=args.scenario, mode_filter=args.funding_mode), indent=2)); return
    if args.offline:
        os.environ['P32_SIGNAL_OFFLINE'] = 'true'
    with make_server('0.0.0.0', int(os.getenv('PORT', '8080')), application, handler_class=QuietHandler) as server:
        print(VERSION + ' ready', flush=True); server.serve_forever()


if __name__ == '__main__':
    main()
