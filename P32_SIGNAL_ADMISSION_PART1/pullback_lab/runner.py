"""Resumable pass-one orchestration; credentials stay in memory only."""
import fcntl
import gc
import gzip
import json
import os
import sqlite3
import shutil
import threading
import time
import uuid
import zipfile
from datetime import datetime
from pathlib import Path
from . import core as e
from .accounting import Replay
from .broker_reconcile import reconcile
from .funding import Funding
from .market import Market
from .paths import Paths, shadow_gbp, truncate
from .reports import atomic, csv_bytes, feature_study, frequency, opportunity_summary, readme, windows, write_jsonl
from .signals import CONTROLS, FEATURE_FILTERS, RULES, gap_rows, generate
from .storage import BROKER, Store, load_bars

ROOT = Path(__file__).resolve().parent
MODES = ('FROZEN_14OCT2026_RATES', 'ADVERSE_6PCT_ANNUAL')
DEFAULT_START = '2005-01-01T00:00:00Z'
DEFAULT_END = '2026-09-20T18:15:00Z'


def configuration(environ=None):
    env = os.environ if environ is None else environ
    start = e.dt(env.get('PULLBACK_START_UTC', DEFAULT_START))
    end = e.dt(env.get('PULLBACK_END_UTC', DEFAULT_END))
    warmup = e.dt(env.get('PULLBACK_WARMUP_START_UTC', '2004-01-01T00:00:00Z'))
    pairs = tuple(p.strip() for p in env.get('PULLBACK_PAIRS', ','.join(e.PAIRS)).split(',') if p.strip())
    e.require(pairs and len(set(pairs))==len(pairs) and set(pairs)<=set(e.PAIRS), 'PULLBACK_PAIRS must be a unique subset of the nine pairs')
    e.require(warmup < start < end <= e.datetime.now(e.UTC), 'Invalid research/warm-up dates')
    e.require(all(e.floor_time(t,900)==t for t in (start,end,warmup)), 'Research dates must align to M15 boundaries')
    e.require(env.get('OANDA_ENV', env.get('OANDA_ENVIRONMENT','live')).strip().lower()=='live', 'Set OANDA_ENV=live for pinned broker terms and cache parity')
    return dict(version=e.VERSION, start=e.stamp(start), end=e.stamp(end), warmup_start=e.stamp(warmup),
                pairs=list(pairs), rules=RULES, feature_filters=FEATURE_FILTERS,
                controls=list(CONTROLS), account_modes=list(MODES), account_scenarios=['END_ASC'],
                account_windows=windows(start,end), initial_balance_gbp=100.,
                full_default_scope=pairs==e.PAIRS and e.stamp(start)==DEFAULT_START and e.stamp(end)==DEFAULT_END,
                historical_financing_known=False, untouched_holdout=False,
                feature_trial_count_including_unfiltered=len(FEATURE_FILTERS), automatic_strategy_selection=False,
                live_portfolio_admission_replayed=False, orders_supported=False, orders_sent=False)


def signature(protocol):
    files = sorted(ROOT.glob('*.py')) + [ROOT/'saved_broker_evidence.json', ROOT.parent/'pullback_app.py']
    return e.sha(e.canon(dict(protocol=protocol, source={p.name:e.sha(p.read_bytes()) for p in files})))


def persistence(directory, create=False):
    directory = Path(directory).resolve()
    marker = directory/'PERSISTENCE_MARKER.json'
    if create and not marker.exists():
        atomic(marker,e.canon(dict(marker=str(uuid.uuid4()),created_utc=e.stamp(e.datetime.now(e.UTC)))))
    mount = None
    try:
        for line in Path('/proc/self/mountinfo').read_text().splitlines():
            left,right=line.split(' - ',1); target=left.split()[4].replace('\\040',' ')
            if str(directory)==target or str(directory).startswith(target.rstrip('/')+'/'):
                if mount is None or len(target)>len(mount['target']):
                    mount=dict(target=target,filesystem=right.split()[0])
    except (OSError,ValueError,IndexError):
        pass
    return dict(output_directory=str(directory), containing_mount=mount,
                separate_mount_detected=bool(mount and mount['target']!='/'),
                persistence_marker=json.loads(marker.read_bytes()) if marker.is_file() else None,
                persistence_across_redeploy_not_proved=True)


class Research:
    def __init__(self, output=None, legacy=None, environ=None, store_factory=Store):
        self.env = dict(os.environ if environ is None else environ)
        self.protocol=configuration(self.env); self.signature=signature(self.protocol)
        self.directory=Path(output or self.env.get('PULLBACK_OUTPUT_DIR','/data/p32_pullback_pass1')).resolve()
        self.work=self.directory/self.signature
        self.legacy=Path(legacy or self.env.get('PULLBACK_LEGACY_CACHE_DIR') or self.env.get('P32_GBP_OUTPUT_DIR') or '/data/p32_signal_admission')
        self.lock=threading.RLock(); self.thread=None; self.store_factory=store_factory
        self.state=dict(status='IDLE',phase='WAITING_FOR_STATUS',version=e.VERSION,signature=self.signature,
                        pairs_completed=0,pairs_required=len(self.protocol['pairs']),orders_supported=False,orders_sent=False,
                        full_portfolio_validated=False,result_ready=False)

    def progress(self, **values):
        with self.lock:
            self.state.update(values,last_progress_utc=e.stamp(e.datetime.now(e.UTC)))
            if self.work.is_dir():
                atomic(self.work/'status.json',e.canon(self.state))

    def status(self, start=False):
        with self.lock:
            if start and self.thread is None and self.state['status']=='IDLE':
                self.thread=threading.Thread(target=self.run,name='pullback-pass1',daemon=True)
                self.state.update(status='STARTING',phase='VERIFY_PROTOCOL')
                self.thread.start()
            value=dict(self.state)
            if self.thread is None and (self.work/'status.json').is_file():
                old=json.loads((self.work/'status.json').read_bytes())
                value.update(previous_process_status=old.get('status'),resumable_previous_progress=old.get('phase'),
                             previous_pairs_completed=old.get('pairs_completed'))
            value['persistence']=persistence(self.directory)
            value['worker_alive']=bool(self.thread and self.thread.is_alive())
            disk_path=self.directory if self.directory.is_dir() else self.directory.parent
            if disk_path.is_dir():
                value['free_disk_bytes']=shutil.disk_usage(disk_path).free
            if value.get('last_progress_utc'):
                value['seconds_since_last_progress']=round((e.datetime.now(e.UTC)-e.dt(value['last_progress_utc'])).total_seconds(),1)
            return value

    def archive(self, complete=False):
        pair_dirs=[self.work/'pairs'/p for p in self.protocol['pairs'] if (self.work/'pairs'/p/'COMPLETE.json').is_file()]
        summaries=[]; feature_rows=[]
        for directory in pair_dirs:
            summaries.extend(json.loads((directory/'account_summaries.json').read_bytes()))
            feature_rows.extend(json.loads(gzip.decompress((directory/'feature_study.json.gz').read_bytes())))
        atomic(self.work/'control_accounts.csv',csv_bytes(summaries))
        atomic(self.work/'feature_study.csv',csv_bytes(feature_rows))
        atomic(self.work/'REVIEW.md',readme(self.protocol,[p.name for p in pair_dirs],summaries,feature_rows).encode())
        manifest={}
        target=self.work/'P32_PULLBACK_PASS1_RESULTS.zip'; temporary=target.with_suffix('.zip.tmp')
        with zipfile.ZipFile(temporary,'w',zipfile.ZIP_DEFLATED,compresslevel=6) as z:
            fixed=['protocol.json','broker_reconciliation.json','control_accounts.csv','feature_study.csv','REVIEW.md','status.json','ERROR.json','source_index.json.gz']
            files=[self.work/name for name in fixed if (self.work/name).is_file()]
            for directory in pair_dirs:
                files.extend(p for p in directory.rglob('*') if p.is_file() and p.suffix != '.sqlite3' and not p.name.endswith(('.tmp','-wal','-shm')))
            for path in sorted(files):
                name=path.relative_to(self.work).as_posix(); raw=path.read_bytes()
                z.writestr(name,raw);manifest[name]=dict(bytes=len(raw),sha256=e.sha(raw))
            z.writestr('report_manifest.json',e.canon(manifest))
            z.writestr('RUN_COMPLETE.json',e.canon(dict(status='COMPLETE_RESEARCH_PASS1' if complete else 'PARTIAL',
                        signature=self.signature,pairs_completed=len(pair_dirs),pairs_required=len(self.protocol['pairs']),
                        accounts=len(summaries),feature_rows=len(feature_rows),orders_supported=False,orders_sent=False,
                        full_portfolio_validated=False,automatic_strategy_selection=False)))
        temporary.replace(target)
        self.progress(result_ready=True,result='/results.zip')

    def run(self):
        resources=[]; handle=None; store=None
        try:
            self.work.mkdir(parents=True,exist_ok=True)
            handle=(self.directory/'runner.lock').open('a')
            try:
                fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:
                self.state.update(status='BUSY',phase='ANOTHER_PROCESS_RUNNING')
                return
            mounted=persistence(self.directory,create=True)
            e.require(self.env.get('PULLBACK_REQUIRE_PERSISTENCE','1')=='0' or mounted['separate_mount_detected'],
                      'Output directory needs a separate persistent mount; use /data/p32_pullback_pass1 on the mounted /data volume')
            atomic(self.work/'protocol.json',e.canon(dict(signature=self.signature,**self.protocol)))
            (self.work/'ERROR.json').unlink(missing_ok=True)
            self.progress(status='RUNNING',phase='RECONCILE_SAVED_BROKER',started_at_utc=e.stamp(e.datetime.now(e.UTC)),
                          accounts_completed=0,accounts_required=len(self.protocol['pairs'])*len(CONTROLS)*len(self.protocol['account_windows'])*len(MODES))
            evidence=json.loads((ROOT/'saved_broker_evidence.json').read_bytes())
            broker,checks=reconcile(evidence)
            e.require(broker['status']=='PASS' and broker['conversion_factor_markup_uniform'],'Saved broker accounting did not reconcile')
            e.require(evidence['account']['currency']=='GBP' and not evidence['account']['hedgingEnabled'],'Expected saved GBP nonhedging terms')
            atomic(self.work/'broker_reconciliation.json',e.canon(dict(summary=broker,checks=checks)))
            token=next((self.env.get(k,'').strip() for k in ('OANDA_TOKEN','OANDA_API_TOKEN','OANDA_API_KEY','OANDA_ACCESS_TOKEN') if self.env.get(k,'').strip()),'')
            offline=self.env.get('PULLBACK_OFFLINE','0')=='1'
            e.require(offline or token,'Set OANDA_TOKEN in the research service variables')
            reader=None if offline else e.CandleReader(token,BROKER,requests_per_second=4)
            store=self.store_factory(self.directory/'canonical_candles',reader=reader,legacy_root=self.legacy,progress=self.progress);resources.append(store)
            funding=Funding(evidence['funding_schedules'])
            markup=broker['observed_symmetric_conversion_factor_markup']
            terms={i['name']:i for i in evidence['instruments']}
            e.require(set(terms)==set(e.PAIRS) and all(int(i['tradeUnitsPrecision'])==0 for i in terms.values()),'Whole-unit broker terms missing')
            start,end,warm=(e.dt(self.protocol[k]) for k in ('start','end','warmup_start'))
            completed=0; account_count=0
            for pair in self.protocol['pairs']:
                pair_dir=self.work/'pairs'/pair;pair_dir.mkdir(parents=True,exist_ok=True)
                if (pair_dir/'COMPLETE.json').is_file():
                    pinned=json.loads((pair_dir/'COMPLETE.json').read_bytes())
                    e.require(pinned['signature']==self.signature,'Pair checkpoint signature changed')
                    for name,checksum in pinned['files'].items():
                        e.require((pair_dir/name).is_file() and e.sha((pair_dir/name).read_bytes())==checksum,'Pair checkpoint file missing or changed: '+name)
                    completed+=1;account_count+=pinned['accounts']
                    self.progress(pairs_completed=completed,accounts_completed=account_count,phase='REUSE_COMPLETE_PAIR',pair=pair)
                    continue
                self.progress(phase='LOAD_HISTORY',pair=pair,pairs_completed=completed)
                m15,mc=load_bars(store,pair,'M15',warm,start,end,self.progress)
                h1,hc=load_bars(store,pair,'H1',warm,start,end,self.progress)
                gaps=gap_rows(m15,900,pair,'M15',start,end)+gap_rows(h1,3600,pair,'H1',start,end)
                atomic(pair_dir/'coverage.json',e.canon(dict(streams=[mc,hc],gaps=gaps,
                           unexplained_slots=sum(g['unexplained_slots'] for g in gaps))))
                signals,counts=generate(pair,m15,h1,start,end,self.progress)
                write_jsonl(pair_dir/'qualifying_signals.jsonl.gz',signals)
                atomic(pair_dir/'generation_counts.json',e.canon(counts))
                del h1;gc.collect()
                paths=Paths(self.work,store,m15,pair,end,self.signature,self.progress);resources.append(paths)
                market=Market(store,self.progress);opportunities=[]
                database=sqlite3.connect(str(pair_dir/'opportunity_checkpoints.sqlite3'));resources.append(database)
                database.execute('CREATE TABLE IF NOT EXISTS items(key TEXT PRIMARY KEY, body BLOB, sha256 TEXT)');database.commit()
                for n,s in enumerate(signals):
                    key=s['signal_key'];saved=database.execute('SELECT body,sha256 FROM items WHERE key=?',(key,)).fetchone()
                    if saved:
                        e.require(e.sha(saved[0])==saved[1],'Opportunity checkpoint checksum failed')
                        opportunity=json.loads(saved[0])
                    else:
                        p=paths.get(s)
                        shadow=shadow_gbp(s,p,market,funding,terms[pair],markup,end)
                        opportunity=dict(signal_key=key,pair=pair,side=s['side'],control=s['control'],entry_utc=s['entry_utc'],features=s['features'],
                                         path_status=p['status'],shadow=shadow,
                                         counterfactual_outcome_status=p.get('counterfactual_outcome_status'),
                                         counterfactual_price_r=p.get('observed_path_price_r') if p['status']=='ENTRY_PROXY_REJECTED' else None)
                        raw=e.canon(opportunity);database.execute('INSERT INTO items VALUES(?,?,?)',(key,raw,e.sha(raw)))
                        if n%50==0:database.commit()
                    opportunities.append(opportunity)
                    if n%100==0:
                        self.progress(phase='PRICE_QUALIFICATIONS',pair=pair,qualifications_processed=n+1,qualifications_required=len(signals),
                                      native_paths_computed=paths.computed,path_cache_hits=paths.cached,through_utc=s['entry_utc'])
                database.commit();database.close();resources.remove(database)
                write_jsonl(pair_dir/'opportunities.jsonl.gz',opportunities)
                write_jsonl(pair_dir/'price_paths.jsonl.gz',(dict(signal_key=s['signal_key'],**paths.get(s)) for s in signals))
                feature_rows=feature_study(pair,opportunities,start,end)
                atomic(pair_dir/'feature_study.json.gz',gzip.compress(e.canon(feature_rows)))
                atomic(pair_dir/'opportunity_summary.csv',csv_bytes(opportunity_summary(pair,opportunities,start,end)))
                atomic(pair_dir/'monthly_qualifications.csv',csv_bytes(frequency(pair,signals,start,end)))
                del opportunities;gc.collect()
                summaries=[]
                for control in CONTROLS:
                    for window in self.protocol['account_windows']:
                        lo,hi=e.dt(window['start']),e.dt(window['end'])
                        selected=[s for s in signals if s['control']==control and window['start']<=s['entry_utc']<window['end']]
                        for mode in MODES:
                            label=control+'__'+window['window_id']+'__'+mode
                            checkpoint=pair_dir/'accounts'/(label+'.json.gz')
                            if checkpoint.is_file():
                                saved=json.loads(gzip.decompress(checkpoint.read_bytes()))
                                e.require(saved['signature']==self.signature and e.sha(e.canon(saved['result']))==saved['sha256'],'Account checkpoint failed')
                                result=saved['result']
                            else:
                                self.progress(phase='REPLAY_CONTROL_ACCOUNT',pair=pair,control=control,window_id=window['window_id'],funding_mode=mode)
                                market=Market(store,self.progress)
                                result=Replay(selected,lambda s:truncate(paths.get(s),hi),market,funding,evidence['instruments'],
                                              'END_ASC',mode,markup,initial=100.,start=lo,end=hi,progress=self.progress).run()
                                e.require(result['summary']['status']=='COMPLETE_CONDITIONAL','Account replay stopped: '+str(result['summary']['stopped_reason']))
                                saved=dict(signature=self.signature,result=result,sha256=e.sha(e.canon(result)))
                                atomic(checkpoint,gzip.compress(e.canon(saved)))
                            summaries.append(dict(pair=pair,control=control,window_id=window['window_id'],window_start=window['start'],window_end=window['end'],
                                                  **result['summary'],scope='ISOLATED_ONE_PAIR_BOTH_DIRECTIONS_NOT_P32_COMBINED_PORTFOLIO'))
                            account_count+=1;self.progress(accounts_completed=account_count)
                atomic(pair_dir/'account_summaries.json',e.canon(summaries))
                paths.close();resources.remove(paths);del paths,m15,signals;gc.collect()
                files={p.relative_to(pair_dir).as_posix():e.sha(p.read_bytes()) for p in pair_dir.rglob('*')
                       if p.is_file() and p.name not in ('COMPLETE.json','opportunity_checkpoints.sqlite3') and not p.name.endswith(('-wal','-shm','.tmp'))}
                atomic(pair_dir/'COMPLETE.json',e.canon(dict(signature=self.signature,accounts=len(summaries),files=files)))
                completed+=1;self.progress(pairs_completed=completed,phase='PAIR_COMPLETE',pair=pair)
                atomic(self.work/'source_index.json.gz',gzip.compress(e.canon(store.index())))
                self.archive(complete=False)
            self.progress(status='RUNNING',phase='FINALIZE_REPORTS',pairs_completed=completed)
            atomic(self.work/'source_index.json.gz',gzip.compress(e.canon(store.index())))
            self.archive(complete=True)
            self.progress(status='COMPLETE',phase='COMPLETE',pairs_completed=completed)
        except e.NeedData as exc:
            self.progress(status='NEEDS_DATA',phase='STOPPED_OFFLINE',pending_request=exc.identity)
            atomic(self.work/'ERROR.json',e.canon(dict(error='Additional candle data required',pending_request=exc.identity)))
        except Exception as exc:
            # Transport exceptions never print tokens, requests, or headers.
            safe=str(exc) if isinstance(exc,(ValueError,e.CandleFetchError)) else type(exc).__name__
            self.progress(status='ERROR',phase='FAILED',error=safe)
            atomic(self.work/'ERROR.json',e.canon(dict(error=safe,error_type=type(exc).__name__,signature=self.signature)))
        finally:
            if self.state['status'] in ('ERROR','NEEDS_DATA'):
                try:self.archive(complete=False)
                except Exception:pass
            for resource in reversed(resources):
                try:resource.close()
                except Exception:pass
            if handle:
                handle.close()
