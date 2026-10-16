#!/usr/bin/env python3
"""P32 observed-bar signal recovery and chronological admission research.

Only historical broker candle GETs. GET /status starts one resumable run.
The live executor and probe are not imported or contacted.
"""
import argparse,collections,csv,gc,hashlib,io,json,os,sqlite3,tempfile,threading,zipfile
from datetime import datetime
from pathlib import Path
from wsgiref.simple_server import make_server,WSGIRequestHandler
import execution as e
from history import HistoryStore,MidReader,START,END,WARMUP_START
import signals as recovery
import admissions
import gap_audit

VERSION='P32_WARMUP_GAP_AUDIT_V1_2026_10_16'
RESULT='P32_WARMUP_GAP_AUDIT_RESULTS.zip'
SOURCES='P32_WARMUP_GAP_AUDIT_SOURCES.zip'
LOCK=threading.RLock()
STATE=dict(status='IDLE',version=VERSION,orders_supported=False,scope='ALL32_PRE2005_WARMUP_AND_171_GAP_SOURCE_INVESTIGATION')
DIRECTORY=None

PROTOCOL={
 'scope':'Regenerate all 32 locked current Python predicate streams on observed native bars, before position filtering; replay current nonhedging account gates.',
 'history':'Indicator warm-up from 2002-01-01; signal evaluation remains 2005-01-01 through 2026-09-20T18:29Z. MID native H1/M15/H4/D, unsmoothed; NY17 daily/H4 alignment; no synthetic bars.',
 'indicators':'Pre-2005 observed-history SMA-seeded EMA/RMA; missing warm-up is explicitly flagged. EURCHF retains research TR1..14 ATR. This canonical historical initialization is not asserted identical to live rolling fetch-window initialization.',
 'causality':'M15 HTF eligible only when next actual HTF open <= M15 signal OPEN, exactly as supplied probe. H1 daily filters use previous NY session only.',
 'rules':'Thresholds, sessions, filters, RR and core/complement priority extracted from supplied probe unchanged. No rule search or optimization.',
 'signals':'Every observed-bar qualification retained before pyramiding/opposition filtering. Native missing observations can hide additional qualifications.',
 'entry':'Existing 10 tick limit, broker rounding, first native bid/ask opening proxy. Arrival time, stale checks, fill cancellations and liquidity are not recovered.',
 'account':'Nonhedging observed in supplied 14-Oct account snapshot. Opposite direction same pair blocked; same direction overlaps allowed. One open position per strategy. No invented six-position cap.',
 'orders':'Four prespecified START/END exit-interval endpoints crossed with ascending/descending simultaneous strategy order. Within-strategy trigger order retained. These are assumption scenarios, not return bounds.',
 'uncertainty':'Gaps, unresolved barrier order and uncertain admission lineage remain explicit. Original accepted book and 456 archived EURUSD isolated paths are reconciliation controls, not forced targets.',
 'accounting':'Illustrative price-only research units: initial 100, 1% of closed balance reserved per entry, EURJPY M15 short .75%, P/L from worst-entry-bound budget R. No GBP conversion, units, margin, live NAV sizing, financing or open-equity drawdown.',
 'ambiguous_prices':'Separate STOP_FIRST and TARGET_FIRST values for unresolved same-S5 both-barrier candles. Earlier gaps use observed-barrier scenario, never asserted actual first exit.',
 'gap_diagnostics':'Investigate all 171 prior gap paths separately. Requery missing native/fine windows, inspect finer quotes and one control pair when empty. No absence is certified as a closure, and diagnostic quotes never automatically change exits.',
 'validation':'All32 observed streams can be recovered while source identity/coverage remains qualified. Conditional closed-equity CAGR/DD must not be presented as validated broker returns.',
}

def progress(**values):
    with LOCK:
        if values.get('phase') and values['phase']!=STATE.get('phase'):
            for key in ('bars_evaluated','bars_required','signals_found','granularity','timeframe','through_utc','native_bars','gap_query_pair','gap_query_granularity','gap_query_start','gap_query_end'):
                STATE.pop(key,None)
        STATE.update(values,last_progress_utc=e.stamp(datetime.now(e.UTC)))

def output_dir():
    if DIRECTORY:return Path(DIRECTORY).resolve()
    return Path(os.getenv('P32_SIGNAL_OUTPUT_DIR','/data/p32_signal_admission' if Path('/data').is_dir() else str(Path(tempfile.gettempdir())/'p32_signal_admission'))).resolve()

def fingerprint():
    return e.sha(e.canon({p:e.sha((Path(__file__).parent/p).read_bytes()) for p in ('app.py','history.py','signals.py','admissions.py','locked_rules.py','m15_adapter.py','execution.py','reference_payload.py','gap_audit.py')}))

def normalized_source_hash(path):
    # Windows upload/editing can change CRLF/BOM without changing Python code.
    return e.sha(Path(path).read_text(encoding='utf-8-sig').encode('utf-8'))

def csv_bytes(rows):
    fields=list(dict.fromkeys(k for r in rows for k in r)) or ['empty']
    buffer=io.StringIO(newline='');writer=csv.DictWriter(buffer,fieldnames=fields);writer.writeheader()
    for r in rows:writer.writerow({k:json.dumps(v,sort_keys=True) if isinstance(v,(dict,list)) else v for k,v in r.items()})
    return buffer.getvalue().encode()

def jsonl(rows):return b''.join(e.canon(r)+b'\n' for r in rows)

class PathCache:
    def __init__(self,directory,signature,store,data):
        self.db=sqlite3.connect(str(directory/'candidate_paths.sqlite3'))
        self.db.execute('CREATE TABLE IF NOT EXISTS paths (key TEXT PRIMARY KEY,body BLOB,sha256 TEXT)');self.db.commit()
        self.signature,self.store,self.data=signature,store,data;self.rows={};self.computed=0;self.hits=0
    def get(self,signal):
        key=e.sha(e.canon(dict(signature=self.signature,signal=signal)))
        if signal['signal_key'] in self.rows:return self.rows[signal['signal_key']]
        stored=self.db.execute('SELECT body,sha256 FROM paths WHERE key=?',(key,)).fetchone()
        if stored:
            e.require(e.sha(stored[0])==stored[1],'Candidate checkpoint checksum mismatch');row=json.loads(stored[0]);self.hits+=1
        else:
            if e.dt(signal['entry_utc'])+e.timedelta(seconds=e.STEP[signal['timeframe']])>END:
                row=dict(signal_key=signal['signal_key'],status='UNRESOLVED_ENTRY',entry_status='NATIVE_ENTRY_INTERVAL_EXCEEDS_FIXED_CUTOFF')
                self.rows[signal['signal_key']]=row
                return row
            row=e.replay_one(self.store,admissions.candidate_data(self.data,signal),0,e.VARIANTS[1])
            row={k:v for k,v in row.items() if not k.startswith('archived_') and k not in ('index','extended_beyond_archived_exit')}
            row.update(signal_key=signal['signal_key'],signal_start_utc=signal['signal_start_utc'])
            if row['status']=='ERROR' and self.store.reader and self.store.reader.fatal:raise e.CandleFetchError('Historical execution candles: authentication failed')
            if row['status'] not in ('ERROR','NEEDS_CANDLE_DATA'):
                raw=e.canon(row);self.db.execute('INSERT OR REPLACE INTO paths VALUES (?,?,?)',(key,raw,e.sha(raw)));self.db.commit()
            self.computed+=1
        self.rows[signal['signal_key']]=row
        progress(candidate_paths_computed=self.computed,candidate_path_cache_hits=self.hits,new_execution_blocks=self.store.downloads)
        return row
    def close(self):self.db.close()

def atomic_json(path,data):
    temporary=path.with_suffix('.tmp');temporary.write_bytes(e.canon(data));temporary.replace(path)

def run(directory=None,offline=False):
    directory=Path(directory or output_dir());directory.mkdir(parents=True,exist_ok=True)
    baseline_path=Path(__file__).parent/'prior_signal_admission_results.zip'
    baseline=gap_audit.load_baseline(baseline_path)
    data=e.load_reference();registry=recovery.validate_registry(data['registry']);signature=fingerprint()
    provenance=json.loads((Path(__file__).parent/'rules_provenance.json').read_text())
    e.require(normalized_source_hash(Path(__file__).parent/'locked_rules.py')==provenance['pure_rules_sha256'],'Locked rules file changed')
    e.require(normalized_source_hash(Path(__file__).parent/'m15_adapter.py')==provenance['m15_adapter_sha256'],'M15 adapter file changed')
    env=os.getenv('OANDA_ENV',os.getenv('OANDA_ENVIRONMENT','live')).strip().lower()
    e.require(env=='live','Use OANDA_ENV=live to match the saved evidence')
    token=next((os.getenv(k,'').strip() for k in ('OANDA_TOKEN','OANDA_API_KEY','OANDA_API_TOKEN','OANDA_ACCESS_TOKEN') if os.getenv(k,'').strip()),'')
    e.require(offline or bool(token),'Set OANDA_TOKEN on the separate research service')
    rate=float(os.getenv('P32_SIGNAL_REQUESTS_PER_SECOND','4'))
    mid_reader=None if offline else MidReader(token,'https://api-fxtrade.oanda.com',rate)
    price_reader=None if offline else e.CandleReader(token,'https://api-fxtrade.oanda.com',rate)
    history=HistoryStore(directory,mid_reader,progress)
    store=e.CandleStore(directory,'https://api-fxtrade.oanda.com',price_reader)
    paths=PathCache(directory,signature,store,data)
    raw=[];coverage=[];ledgers=[];accounting=[];equity=[];report_written=False
    progress(status='RUNNING',phase='IMPORT_SAVED_EXECUTION_CANDLES',version=VERSION,pairs_required=9,started_at_utc=e.stamp(datetime.now(e.UTC)))
    try:
        store.import_seeds(Path(__file__).parent)
        gap_audit.import_baseline_sources(baseline_path,store)
        gap_summary,gap_paths,gap_windows=gap_audit.run(directory,baseline,store,price_reader,progress)
        atomic_json(directory/'gap_investigation_checkpoint.json',dict(summary=gap_summary,paths=gap_paths,windows=gap_windows))
        for number,pair in enumerate(e.PAIRS,1):
            checkpoint=directory/('signals_'+pair+'_'+signature+'.json')
            if checkpoint.exists():
                saved=json.loads(checkpoint.read_text());e.require(e.sha(e.canon(saved['signals']))==saved['signals_sha256'],'Raw signal checkpoint checksum failed')
                recovered=saved['signals'];pair_coverage=saved['coverage']
            else:
                before=len(history.coverage)
                h1=history.series(pair,'H1')
                daily=history.series(pair,'D')
                m15_bars=history.series(pair,'M15') if pair!='EUR_CHF' else None
                h4=history.series(pair,'H4') if pair in ('USD_CAD','USD_JPY') else None
                e.require(len(h1)>=200 and len(daily)>=250 and (m15_bars is None or len(m15_bars)>=200),'Insufficient native history to evaluate locked rules')
                recovered=recovery.recover_pair(pair,h1,daily,m15_bars,h4,progress)
                pair_coverage=history.coverage[before:]
                atomic_json(checkpoint,dict(signature=signature,signals=recovered,signals_sha256=e.sha(e.canon(recovered)),coverage=pair_coverage))
                del h1,daily,m15_bars,h4;gc.collect()
            for s in recovered:
                rule=registry[s['strategy_id']]
                e.require(s['pair']==rule['pair'] and s['side']==rule['side'] and s['timeframe']==rule['timeframe'] and s['rr'] in [float(x) for x in rule['rr'].split(';')],'Recovered signal registry mismatch')
            raw.extend(recovered);coverage.extend(pair_coverage)
            progress(pairs_completed=number,raw_signals_recovered=len(raw))
        raw.sort(key=lambda r:(r['entry_utc'],r['strategy_id'],r['same_strategy_trigger_priority']))
        e.require(len({x['signal_key'] for x in raw})==len(raw),'Raw signal keys duplicated across pairs')
        e.require(all(e.dt(s['signal_start_utc'])>=START for s in raw),'Warm-up signal leaked into evaluation period')
        archive_control,old_raw_control=recovery.reconcile(data,raw)
        warmup_summary,warmup_changes,missing_followup=gap_audit.compare(baseline,raw,archive_control)
        for scenario in admissions.SCENARIOS:
            ledger=admissions.replay(raw,scenario,paths.get,progress,hedging=False);ledgers.extend(ledger)
            for ambiguity in ('STOP_FIRST','TARGET_FIRST'):
                summary,events=admissions.model_accounting(ledger,raw,paths.rows,ambiguity)
                summary['scenario']=scenario;accounting.append(summary)
                if ambiguity=='STOP_FIRST':equity.extend(dict(r,scenario=scenario,ambiguity_assumption=ambiguity) for r in events)
        streams=[]
        for sid,rule in sorted(registry.items()):
            selected=[s for s in raw if s['strategy_id']==sid]
            streams.append(dict(strategy_id=sid,pair=rule['pair'],side=rule['side'],timeframe=rule['timeframe'],scan_completed=True,
                                observed_bar_qualifications=len(selected),first_signal=selected[0]['entry_utc'] if selected else None,last_signal=selected[-1]['entry_utc'] if selected else None,
                                raw_signal_filtering='NO_POSITION_FILTER',historical_original_stream_identity_confirmed=False,missing_native_bars_could_hide_signals=True))
        summaries=[]
        for scenario in admissions.SCENARIOS:
            subset=[r for r in ledgers if r['scenario']==scenario]
            admitted=[r for r in subset if r['decision']=='ADMITTED_UNDER_SCENARIO']
            resolved=[paths.rows[r['signal_key']] for r in admitted if paths.rows[r['signal_key']]['status']=='CLOSED_RESOLVED']
            values=[r['observed_path_price_r'] for r in resolved]
            summaries.append(dict(scenario=scenario,qualified_candidates=len(subset),decisions=dict(collections.Counter(r['decision'] for r in subset)),
                                  admitted_path_statuses=dict(collections.Counter(r['path_status'] for r in admitted)),
                                  resolved_admitted=len(resolved),resolved_only_win_rate_pct=100*sum(x>0 for x in values)/len(values) if values else None,
                                  resolved_only_mean_price_r=sum(values)/len(values) if values else None,
                                  admitted_with_uncertain_lineage=sum(r['decision_has_uncertain_lineage'] for r in admitted),max_modeled_open_positions=max((r['active_positions'] for r in subset),default=0)))
        status='COMPLETE_WITH_LIMITATIONS'
        summary=dict(status=status,version=VERSION,scope=STATE['scope'],prior_admission_result_sha256=gap_audit.BASELINE_SHA,orders_sent=False,live_services_contacted=False,
                     historical_cutoff=e.stamp(END),evaluation_start=e.stamp(START),warmup_start=e.stamp(WARMUP_START),
                     warmup_coverage_insufficient=[dict(pair=r['pair'],granularity=r['granularity'],warmup_observed_bars=r.get('warmup_observed_bars',0)) for r in coverage if not r.get('warmup_sufficient',False)],
                     gap_investigation=gap_summary,warmup_comparison=warmup_summary,strategy_streams_scanned=32,observed_bar_signal_recovery_finished=True,
                     historical_original_stream_identity_confirmed=False,full_portfolio_validated=False,signature=signature,
                     raw_signals=len(raw),candidate_paths_priced=len(paths.rows),price_path_status_counts=dict(collections.Counter(r['status'] for r in paths.rows.values())),
                     archived_control_counts=dict(collections.Counter(r['status'] for r in archive_control)),eurusd_raw_control_counts=dict(collections.Counter(r['status'] for r in old_raw_control)),
                     mid_downloads=history.downloads,new_execution_blocks=store.downloads,scenarios=summaries,
                     finished_at_utc=e.stamp(datetime.now(e.UTC)),offline=offline)
        files={'gap_investigation_summary.json':e.canon(gap_summary),'gap_path_investigation.csv':csv_bytes(gap_paths),
               'gap_window_evidence.csv':csv_bytes(gap_windows),'warmup_comparison_summary.json':e.canon(warmup_summary),
               'warmup_signal_changes.csv':csv_bytes(warmup_changes),'prior_missing_32_followup.csv':csv_bytes(missing_followup),
               'prior_run_summary.json':e.canon(baseline['summary']),'RUN_COMPLETE.json':e.canon(summary),'protocol.json':e.canon(PROTOCOL),'rules_provenance.json':e.canon(provenance),
               'raw_signal_witnesses.jsonl':jsonl(raw),'raw_signals.csv':csv_bytes([{k:v for k,v in r.items() if k!='diagnostics'} for r in raw]),
               'strategy_stream_coverage.csv':csv_bytes(streams),'native_history_coverage.csv':csv_bytes(coverage),
               'archived_3391_signal_reconciliation.csv':csv_bytes(archive_control),'archived_eurusd_raw_reconciliation.csv':csv_bytes(old_raw_control),
               'admission_ledger.csv':csv_bytes(ledgers),'candidate_price_paths.csv':csv_bytes(list(paths.rows.values())),
               'admission_scenario_summaries.json':e.canon(summaries),'conditional_price_accounting.json':e.canon(accounting),
               'conditional_stop_first_equity_events.csv':csv_bytes(equity),
               'mid_history_source_index.json':e.canon(history.index()),'request_receipts.json':e.canon({'mid':mid_reader.receipts if mid_reader else [],'execution':price_reader.receipts if price_reader else []}),
               'frozen_original_accepted_book.csv':csv_bytes(data['reference']['book']),
               'prior_frozen_baseline_controls.json':e.canon(data['baseline_controls'])}
        text='''# P32 pre-2005 warm-up and gap source investigation

Indicators now use observed history from 2002-01-01; signal evaluation starts
on 2005-01-01 as before. Native_history_coverage.csv records available warm-up
and flags insufficient observations. Warmup_signal_changes.csv records added,
removed and changed signals, without forcing agreement with old trades.

Gap_path_investigation.csv covers all 171 prior uncertain paths.
Gap_window_evidence.csv distinguishes native, M1 and S5 gaps and reports fresh
source/finer quote checks. Empty quotes in one or two pairs do not establish
a historical closure. These diagnostics do not automatically promote exits
or mix new source revisions into the frozen execution cache.

All32 current locked predicate streams are evaluated on continuous native MID
history before any position filter. Thresholds were not optimized. Compare the
original archive and EURUSD isolated streams using the reconciliation CSVs.
Full-history indicator seeds and native price revisions can cause differences
from old archives or the live rolling fetch windows. Missing observations can
hide additional signals. Recovery from observed bars is not proof of exact
original stream identity.

The four predeclared admission chronologies vary exit-interval endpoints and
simultaneous strategy order. Current nonhedging pair gates and strategy p0 are
applied before entry previews. Original rejected/blocked signals are retained.
The first native opening quote is still an entry estimate. Latency, stale checks
and actual price-bound cancellations are not recovered.

Earlier gaps and uncertain exits remain flagged. Their observed barrier is a
conditional modeling assumption; subsequent pair admissions carry uncertainty
lineage. A gap-qualified observed STOP/TARGET is not a verified first exit.

conditional_price_accounting.json contains ILLUSTRATIVE research units, using
1% closed-balance risk budgets (.75% EURJPY M15 short) and worst-bound price R.
It assumes static quote/home conversion and fractional size, with no margin,
live NAV sizing, financing or unrealized equity. STOP_FIRST/TARGET_FIRST values
refer only to ambiguous same-S5 barriers. These scenarios are not return bounds.
Unpriced exits suppress whole-cohort accounting. Open positions are not forced
closed. Its CAGR/DD fields are conditional CLOSED-equity estimates, not actual
broker returns or validated portfolio metrics. No best scenario is selected.

The full native source archive is at /sources.zip. Keep it or the persistent
volume for reproduction. This report includes source hashes and every raw
qualification witness. Upload the results ZIP for review; source archive can be
requested separately if a source discrepancy requires it.

No trading service was modified or contacted, and no orders can be submitted.
'''
        files['READ_FIRST.md']=text.encode()
        files['report_manifest.json']=e.canon({n:dict(bytes=len(b),sha256=e.sha(b)) for n,b in files.items()})
        progress(phase='PACKAGE_REPORTS')
        temp=directory/(RESULT+'.tmp')
        with zipfile.ZipFile(temp,'w',zipfile.ZIP_DEFLATED,compresslevel=6) as z:
            for n,b in sorted(files.items()):z.writestr(n,b)
            store.export(z);gap_audit.export_evidence(directory,z)
        temp.replace(directory/RESULT)
        report_written=True
        progress(phase='PACKAGE_NATIVE_SOURCE_ARCHIVE',result_ready=True,result='/results.zip')
        temp=directory/(SOURCES+'.tmp')
        with zipfile.ZipFile(temp,'w',zipfile.ZIP_DEFLATED,compresslevel=6) as z:
            history.export(z);store.export(z);gap_audit.export_evidence(directory,z)
            z.writestr('source_provenance.json',e.canon(dict(signature=signature,seeds=e.SEED_MANIFEST,original_reference_sha256=e.REFERENCE_SHA256,prior_admission_result_sha256=gap_audit.BASELINE_SHA)))
        temp.replace(directory/SOURCES)
        atomic_json(directory/'completion.json',summary)
        progress(**summary,phase='FINISHED',result='/results.zip',source_archive='/sources.zip',result_ready=True)
        return summary
    except Exception as exc:
        message=str(exc) if isinstance(exc,(ValueError,e.CandleFetchError)) else type(exc).__name__
        error=dict(status='ERROR',version=VERSION,error=message,orders_sent=False,partial_raw_signals=len(raw),offline=offline,research_reports_preserved=report_written)
        if isinstance(exc,e.NeedData):error['pending_request']=exc.identity
        if report_written:
            # A source-archive/disk failure must not replace completed research
            # reports with an error-only file. Append the diagnostic instead.
            with zipfile.ZipFile(directory/RESULT,'a',zipfile.ZIP_DEFLATED) as z:z.writestr('ERROR.json',e.canon(error))
        else:
            temp=directory/(RESULT+'.tmp')
            with zipfile.ZipFile(temp,'w',zipfile.ZIP_DEFLATED) as z:
                z.writestr('ERROR.json',e.canon(error));z.writestr('partial_raw_signals.jsonl',jsonl(raw));z.writestr('rules_provenance.json',e.canon(provenance))
            temp.replace(directory/RESULT)
        progress(**error,phase='FAILED',result_ready=True,result='/results.zip')
        raise
    finally:
        paths.close();store.close();history.close()

def start():
    with LOCK:
        if STATE['status']!='IDLE':return
        STATE.update(status='RUNNING',phase='STARTING')
    def worker():
        try:run(offline=os.getenv('P32_SIGNAL_OFFLINE','').lower() in ('1','true'))
        except Exception as exc:
            if STATE.get('status')!='ERROR':progress(status='ERROR',phase='FAILED',error=str(exc) if isinstance(exc,ValueError) else type(exc).__name__)
    threading.Thread(target=worker,daemon=True,name='p32-raw-signal-recovery').start()

def application(environ,start_response):
    method,path=environ.get('REQUEST_METHOD','GET'),environ.get('PATH_INFO','/');kind='application/json';download=None
    if method not in ('GET','HEAD'):code,body='405 Method Not Allowed',e.canon({'error':'GET/HEAD only'})
    elif path in ('/','/health'):code,body='200 OK',e.canon(dict(service=VERSION,orders_supported=False,scope=STATE['scope'],start_once='/status',result='/results.zip',native_sources='/sources.zip'))
    elif path=='/status':
        if method=='GET':start()
        with LOCK:
            snapshot=dict(STATE)
        if snapshot.get('last_progress_utc'):snapshot['seconds_since_last_progress']=round((datetime.now(e.UTC)-e.dt(snapshot['last_progress_utc'])).total_seconds(),1)
        body=e.canon(snapshot)
        code='200 OK'
    elif path in ('/results.zip','/results','/download','/sources.zip'):
        name=SOURCES if path=='/sources.zip' else RESULT;file=output_dir()/name
        if file.is_file() and ((path=='/sources.zip' and STATE.get('phase')=='FINISHED') or (path!='/sources.zip' and STATE.get('result_ready'))):code,body,kind,download='200 OK',b'','application/zip',file
        else:code,body='409 Conflict',e.canon({'message':'Not ready; check /status'})
    else:code,body='404 Not Found',e.canon({'error':'Unknown route'})
    headers=[('Content-Type',kind),('Content-Length',str(download.stat().st_size if download else len(body))),('Cache-Control','no-store')]
    if kind=='application/zip':headers.append(('Content-Disposition','attachment; filename='+(SOURCES if path=='/sources.zip' else RESULT)))
    if code.startswith('405'):headers.append(('Allow','GET, HEAD'))
    start_response(code,headers)
    if download and method=='GET':
        def chunks():
            with download.open('rb') as file:
                while True:
                    block=file.read(65536)
                    if not block:break
                    yield block
        return chunks()
    return [b'' if method=='HEAD' else body]

class QuietHandler(WSGIRequestHandler):
    def log_message(self,*args):pass

def main():
    global DIRECTORY
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run',action='store_true');p.add_argument('--offline',action='store_true');p.add_argument('--output');args=p.parse_args();DIRECTORY=args.output
    if args.run:print(json.dumps(run(offline=args.offline),indent=2));return
    if args.offline:os.environ['P32_SIGNAL_OFFLINE']='true'
    with make_server('0.0.0.0',int(os.getenv('PORT','8080')),application,handler_class=QuietHandler) as server:
        print(VERSION+' ready',flush=True);server.serve_forever()

if __name__=='__main__':main()
