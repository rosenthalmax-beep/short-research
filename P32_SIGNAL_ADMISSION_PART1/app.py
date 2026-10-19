#!/usr/bin/env python3
"""Read-only P32 entry-limit research service. Historical candle GETs only."""
import argparse
import collections
import csv
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
import execution as e
import admissions
import inputs
import gbp_support as support
import control_checks
import reports
from accounting import Replay
from broker_reconcile import reconcile
from execution_sources import ExecutionWindows
from funding import Funding, MODES
from market import Market
from policies import POLICIES, PROTOCOL, PRIMARY_SCENARIO, PRIMARY_FUNDING
from policy_paths import PolicyPaths
from valuation_sources import ValuationWindows

VERSION='P32_ENTRY_LIMIT_RESEARCH_V1_2026_10_19'
RESULT='P32_ENTRY_LIMIT_RESEARCH_RESULTS.zip'
LOCK=threading.RLock()
STATE=dict(status='IDLE',version=VERSION,orders_supported=False,orders_sent=False,full_portfolio_validated=False)
DIRECTORY=None
atomic=support.atomic


def output_dir():
    value=DIRECTORY or os.getenv('P32_GBP_OUTPUT_DIR') or os.getenv('P32_SIGNAL_OUTPUT_DIR')
    return Path(value or ('/data/p32_signal_admission' if Path('/data').is_dir() else str(Path(tempfile.gettempdir())/'p32_signal_admission'))).resolve()


def work_dir():return output_dir()/'entry_limit_v1'


def progress(**values):
    with LOCK:STATE.update(values,last_progress_utc=e.stamp(datetime.now(e.UTC)))


def write_zip(path,files):
    temporary=path.with_suffix(path.suffix+'.tmp')
    with zipfile.ZipFile(temporary,'w',zipfile.ZIP_DEFLATED,compresslevel=6) as z:
        for name,payload in sorted(files.items()):
            if isinstance(payload,Path):z.write(payload,name)
            else:z.writestr(name,payload)
    temporary.replace(path)


def source_signature(root,config):
    return e.sha(e.canon(dict(config=config,code={p.name:e.sha(p.read_bytes()) for p in sorted(root.glob('*.py'))})))


def checkpoint_load(path,signature):
    if not path.exists():return None
    item=json.loads(gzip.decompress(path.read_bytes()))
    e.require(item['signature']==signature and item['sha256']==e.sha(e.canon(item['result'])), 'Account checkpoint checksum failed')
    return item['result'] if item['result']['summary']['status']=='COMPLETE_CONDITIONAL' else None


def run(directory=None,offline=False,policy_filter=None,scenario_filter=None,mode_filter=None):
    root=Path(__file__).parent;directory=Path(directory or output_dir());work=directory/'entry_limit_v1'
    resources=[];checks=[];started=e.stamp(datetime.now(e.UTC))
    try:
        work.mkdir(parents=True,exist_ok=True)
        progress(status='RUNNING',phase='VERIFY_PINNED_INPUTS',started_at_utc=started,version=VERSION,result_ready=False)
        signals,saved,baseline=inputs.baseline(root);expected=control_checks.load(root);evidence=inputs.broker(root)
        reconciliation,broker_checks=reconcile(evidence)
        e.require(reconciliation['status']=='PASS' and reconciliation['conversion_factor_markup_uniform'], 'Saved broker arithmetic reconciliation failed')
        e.require(evidence['account']['currency']=='GBP' and not evidence['account']['hedgingEnabled'], 'Expected saved GBP nonhedging account')
        e.require({i['name'] for i in evidence['instruments']}==set(e.PAIRS) and all(int(i['tradeUnitsPrecision'])==0 for i in evidence['instruments']), 'Expected whole-unit terms for all nine pairs')
        e.require(os.getenv('OANDA_ENV',os.getenv('OANDA_ENVIRONMENT','live')).lower().strip()=='live', 'Use OANDA_ENV=live for source parity')
        e.require(float(os.getenv('P32_GBP_INITIAL_BALANCE','100'))==100., 'Control comparison requires pinned GBP100 initial balance')
        policies=[p for p in POLICIES if p in ('FIXED_10',policy_filter)] if policy_filter else list(POLICIES)
        scenarios=[scenario_filter] if scenario_filter else list(admissions.SCENARIOS)
        modes=[mode_filter] if mode_filter else list(MODES)
        e.require(policy_filter is None or policy_filter in POLICIES, 'Unknown policy')
        e.require(all(s in admissions.SCENARIOS for s in scenarios) and all(m in MODES for m in modes), 'Unknown scenario/funding mode')
        config=dict(protocol=PROTOCOL,policies=policies,scenarios=scenarios,modes=modes,pins=inputs.PINS,
                    expected_control_sha256=e.sha((root/'control_expectations.json').read_bytes()),initial_balance_gbp=100.)
        signature=source_signature(root,config);atomic(work/'protocol.json',e.canon(dict(signature=signature,**config)))
        token=next((os.getenv(k,'').strip() for k in ('OANDA_TOKEN','OANDA_API_KEY','OANDA_API_TOKEN','OANDA_ACCESS_TOKEN') if os.getenv(k,'').strip()),'')
        e.require(offline or token, 'Set OANDA_TOKEN on the separate research service')
        reader=None if offline else e.CandleReader(token,'https://api-fxtrade.oanda.com',float(os.getenv('P32_SIGNAL_REQUESTS_PER_SECOND','4')))
        frozen=e.CandleStore(directory/'gbp_execution_paths_v2','https://api-fxtrade.oanda.com');resources.append(frozen)
        fresh=e.CandleStore(work/'new_execution_windows','https://api-fxtrade.oanda.com',reader);resources.append(fresh)
        value_store=e.CandleStore(directory/'gbp_valuation_windows_v2','https://api-fxtrade.oanda.com',reader);resources.append(value_store)
        legacy=e.CandleStore(directory,'https://api-fxtrade.oanda.com') if (directory/'replay_candles.sqlite3').is_file() else None
        if legacy:resources.append(legacy)
        progress(phase='IMPORT_FROZEN_EXECUTION_CANDLES',signature=signature,persistence=support.persistence(directory,create=True),
                 broker_reconciliation='PASS',lanes_completed=0,lanes_required=len(policies)*len(scenarios)*len(modes))
        imported={row[0] for row in frozen.db.execute('SELECT sha256 FROM imports')}
        if not all(part['sha256'] in imported for part in e.SEED_MANIFEST):frozen.import_seeds(root)
        inputs.import_additions(root,frozen)
        execution_windows=ExecutionWindows(frozen,fresh,progress);value_windows=ValuationWindows(value_store,frozen,legacy,progress)
        market=Market(value_windows,progress);paths=PolicyPaths(work,saved,execution_windows,progress);resources.append(paths)
        funding=Funding(evidence['funding_schedules']);markup=round(reconciliation['observed_symmetric_conversion_factor_markup'],8)
        results={};comparisons=[];groups=[];period_rows=collections.defaultdict(list);primary={};changes=[]
        equity_files={};detail_files={};completed=0
        for policy in policies:
            paths.policy=policy
            for scenario in scenarios:
                for mode in modes:
                    lane=policy+'__'+scenario+'__'+mode
                    progress(phase='REPLAY_ACCOUNTS',policy=policy,scenario=scenario,funding_mode=mode,
                             lanes_completed=completed,qualifications_processed=0,qualifications_required=len(signals))
                    checkpoint=work/(lane+'__'+signature+'.json.gz');result=checkpoint_load(checkpoint,signature)
                    keep_nav=scenario==PRIMARY_SCENARIO and mode==PRIMARY_FUNDING
                    nav_file=work/(lane+'__'+signature+'_nav.csv.gz')
                    if result is None:
                        stream=gzip.open(str(nav_file)+'.tmp','wt',encoding='utf-8',newline='') if keep_nav else None
                        writer=None
                        def sink(row):
                            nonlocal writer
                            if writer is None:writer=csv.DictWriter(stream,fieldnames=list(row));writer.writeheader()
                            writer.writerow(row)
                        try:
                            result=Replay(signals,paths.get,market,funding,evidence['instruments'],scenario,mode,markup,
                                start=baseline['evaluation_start'],end=baseline['historical_cutoff'],progress=progress,
                                keep_nav=keep_nav,nav_sink=sink if keep_nav else None).run()
                        finally:
                            if stream:stream.close()
                        e.require(result['summary']['status']=='COMPLETE_CONDITIONAL', 'Account did not complete: '+str(result['summary'].get('stopped_reason')))
                        if keep_nav:
                            Path(str(nav_file)+'.tmp').replace(nav_file)
                            result['nav_file']=nav_file.name;result['nav_sha256']=e.sha(nav_file.read_bytes())
                        result['evaluated_paths']={k:r for (p,k),r in paths.evaluated.items() if p==policy}
                        atomic(checkpoint,gzip.compress(e.canon(dict(signature=signature,result=result,sha256=e.sha(e.canon(result)))),compresslevel=6))
                    if policy=='FIXED_10':
                        checked=control_checks.check(result,expected);checks.append(checked);atomic(work/'control_checks.json',e.canon(checks))
                        e.require(checked['status']=='PASS', '10-tick control mismatch: '+checked['lane']+' '+','.join(checked['failed_components']))
                    reference=result if policy=='FIXED_10' else results['FIXED_10',scenario,mode]
                    changes_here,attribution=reports.admission_changes(reference,result)
                    periods=reports.period_tables(result,baseline['evaluation_start'],baseline['historical_cutoff'])
                    label=dict(policy=policy,scenario=scenario,financing_mode=mode)
                    comparisons.append(dict(result['summary'],policy=policy,**attribution,
                        **reports.extra_summary(result,periods,baseline['evaluation_start'],baseline['historical_cutoff'])))
                    for kind,rows in periods.items():period_rows[kind].extend(dict(row,**label) for row in rows)
                    groups.extend(dict(row,**label) for row in reports.grouped(result,result.get('evaluated_paths',{})))
                    if keep_nav:
                        e.require(nav_file.is_file() and e.sha(nav_file.read_bytes())==result['nav_sha256'], 'Saved equity checkpoint missing or changed')
                        equity_files[policy]=nav_file;primary[policy]=result;changes.extend(dict(row,**label) for row in changes_here)
                    if policy=='FIXED_10':
                        results[policy,scenario,mode]={k:result[k] for k in ('summary','ledger','cash_events')}
                    detail_files[lane]=checkpoint.name;completed+=1
                    progress(phase='REPLAY_ACCOUNTS',lanes_completed=completed,controls_passed=len(checks))
        summary=dict(status='COMPLETE_WITH_LIMITATIONS',version=VERSION,signature=signature,started_at_utc=started,
                     finished_at_utc=e.stamp(datetime.now(e.UTC)),raw_signals=len(signals),strategy_count=32,
                     evaluation_start=baseline['evaluation_start'],historical_cutoff=baseline['historical_cutoff'],
                     lanes_completed=completed,controls_passed=len(checks),policies=policies,
                     original_unknown_entry_count=11,original_unresolved_path_count=170,full_80_lane_experiment=completed==80,
                     full_portfolio_validated=False,orders_supported=False,orders_sent=False,live_services_contacted=False,
                     actual_broker_returns=False,historical_financing_known=False,primary_scenario=PRIMARY_SCENARIO,
                     primary_financing=PRIMARY_FUNDING,valuation_policy=value_windows.summary(),
                     new_execution_source_differences=len(execution_windows.differences()),
                     uncertainty='Source/path/account assumptions remain conditional. No live policy selection is made.')
        files={'RUN_COMPLETE.json':e.canon(summary),'protocol.json':(work/'protocol.json').read_bytes(),
               'control_checks.json':e.canon(checks),'control_expectations.json':(root/'control_expectations.json').read_bytes(),
               'policy_comparison.csv':support.csv_bytes(comparisons),'grouped_trade_stats.csv':support.csv_bytes(groups),
               'admission_changes_primary.csv':support.csv_bytes(changes),'broker_reconciliation.json':e.canon(reconciliation),
               'new_execution_source_differences.csv':support.csv_bytes(execution_windows.differences()),
               'valuation_source_differences.csv':support.csv_bytes(value_windows.differences()),
               'source_request_receipts.json':e.canon(reader.receipts if reader else []),
               'persistence.json':e.canon(support.persistence(directory)),
               'detail_downloads.json':e.canon({n:'/details/'+n+'.json.gz' for n in detail_files}),
               'REPORT_GUIDE.md':(root/'REPORT_GUIDE.md').read_bytes()}
        for kind,rows in period_rows.items():files[kind+'.csv']=support.csv_bytes(rows)
        evaluated={}
        for policy,result in primary.items():
            for key,row in result.get('evaluated_paths',{}).items():evaluated[policy,key]=dict(row,policy=policy)
            for kind in ('ledger','trades','cash_events','financing','open_positions'):
                files['primary/'+policy+'/'+kind+'.csv']=support.csv_bytes(result[kind])
            target=work/('EQUITY_'+policy+'.zip');temp=target.with_suffix('.zip.tmp')
            with gzip.open(equity_files[policy],'rb') as source:
                with zipfile.ZipFile(temp,'w',zipfile.ZIP_DEFLATED,compresslevel=6) as z:
                    with z.open(policy+'_nav_samples.csv','w') as out:shutil.copyfileobj(source,out,1024*1024)
            temp.replace(target)
        files['evaluated_paths_primary.csv']=support.csv_bytes(list(evaluated.values()))
        files['report_manifest.json']=e.canon({n:dict(bytes=len(v),sha256=e.sha(v)) for n,v in files.items() if isinstance(v,bytes)})
        atomic(work/'detail_files.json',e.canon(detail_files))
        if os.getenv('P32_CAP_PACKAGE_SOURCES','0')=='1':
            with zipfile.ZipFile(work/'NEW_EXECUTION_SOURCES.zip','w',zipfile.ZIP_DEFLATED) as z:fresh.export(z)
        progress(phase='PACKAGE_RESULTS');write_zip(work/RESULT,files)
        progress(**summary,phase='FINISHED',result_ready=True,result='/results.zip');return summary
    except Exception as exc:
        message=str(exc) if isinstance(exc,(ValueError,e.CandleFetchError)) else type(exc).__name__
        failure=dict(status='ERROR',version=VERSION,error=message,orders_sent=False,full_portfolio_validated=False,
                     control_checks=checks,started_at_utc=started)
        if isinstance(exc,e.NeedData):failure['missing_candle_request']=exc.identity
        try:
            work.mkdir(parents=True,exist_ok=True);write_zip(work/RESULT,{'ERROR.json':e.canon(failure),'control_checks.json':e.canon(checks)})
        except OSError:pass
        progress(**failure,phase='FAILED',result_ready=(work/RESULT).is_file(),result='/results.zip');return failure
    finally:
        for resource in reversed(resources):resource.close()


def start_once():
    with LOCK:
        if STATE['status']!='IDLE':return
        STATE.update(status='RUNNING',phase='STARTING')
    threading.Thread(target=lambda:run(offline=os.getenv('P32_SIGNAL_OFFLINE','').lower() in ('1','true')),daemon=True,name='p32-entry-limit').start()


def application(environ,start_response):
    method,path=environ.get('REQUEST_METHOD','GET'),environ.get('PATH_INFO','/')
    code,body,content='200 OK',b'','application/json';file=None
    if method not in ('GET','HEAD'):code,body='405 Method Not Allowed',e.canon(dict(error='GET/HEAD only; no order routes'))
    elif path in ('/','/health'):
        body=e.canon(dict(service=VERSION,orders_supported=False,orders_sent=False,start_once='/status',result='/results.zip',
                          persistence=support.persistence(output_dir()),policies=list(POLICIES),lanes=80))
    elif path=='/status':
        if method=='GET':start_once()
        with LOCK:body=e.canon(dict(STATE))
    elif path in ('/results.zip','/results','/download'):
        if STATE.get('result_ready'):file=work_dir()/RESULT
    elif path=='/details':
        mapping=work_dir()/'detail_files.json'
        body=e.canon({n:'/details/'+n+'.json.gz' for n in json.loads(mapping.read_bytes())}) if mapping.is_file() else e.canon(dict(message='Details available after completion'))
    elif path.startswith('/details/'):
        mapping=work_dir()/'detail_files.json';name=path.removeprefix('/details/').removesuffix('.json.gz')
        if mapping.is_file():
            selected=json.loads(mapping.read_bytes()).get(name)
            if selected:file=work_dir()/selected
    elif path.startswith('/equity/') and path.endswith('.zip'):
        policy=path.removeprefix('/equity/').removesuffix('.zip')
        if policy in POLICIES:file=work_dir()/('EQUITY_'+policy+'.zip')
    elif path=='/sources.zip':file=work_dir()/'NEW_EXECUTION_SOURCES.zip'
    else:code,body='404 Not Found',e.canon(dict(error='Unknown route'))
    download=method in ('GET','HEAD') and (path in ('/results.zip','/results','/download','/sources.zip') or path.startswith(('/equity/','/details/')))
    if download:
        if file is None or not file.is_file():code,body='409 Conflict',e.canon(dict(message='Download not ready; check /status'));file=None
        else:content='application/gzip' if file.suffix=='.gz' else 'application/zip'
    headers=[('Content-Type',content),('Cache-Control','no-store'),('Content-Length',str(file.stat().st_size if file else len(body)))]
    if file:headers.append(('Content-Disposition','attachment; filename='+file.name))
    if code.startswith('405'):headers.append(('Allow','GET, HEAD'))
    start_response(code,headers)
    if method=='HEAD':return [b'']
    if not file:return [body]
    def chunks():
        with file.open('rb') as stream:
            while block:=stream.read(1024*1024):yield block
    return chunks()


class QuietHandler(WSGIRequestHandler):
    def log_message(self,*args):pass


def main():
    global DIRECTORY
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',action='store_true');parser.add_argument('--offline',action='store_true')
    parser.add_argument('--output');parser.add_argument('--policy',choices=POLICIES)
    parser.add_argument('--scenario',choices=admissions.SCENARIOS);parser.add_argument('--funding-mode',choices=MODES)
    args=parser.parse_args();DIRECTORY=args.output
    if args.run:
        result=run(args.output,args.offline,args.policy,args.scenario,args.funding_mode);print(json.dumps(result,indent=2))
        raise SystemExit(1 if result['status']=='ERROR' else 0)
    e.require(not(args.policy or args.scenario or args.funding_mode or args.offline), 'Run filters require --run; hosted status runs all 80 lanes')
    with make_server('0.0.0.0',int(os.getenv('PORT','8080')),application,handler_class=QuietHandler) as server:
        print(VERSION+' ready',flush=True);server.serve_forever()


if __name__=='__main__':main()
