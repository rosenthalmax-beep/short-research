"""Resumable focused matrix, fixed RR and frozen conditional GBP accounting."""
import fcntl
import gc
import gzip
import json
import os
import shutil
import threading
import zipfile
from pathlib import Path
from pullback_lab import core as e
from pullback_lab.storage import load_bars,BROKER
from pullback_lab.market import Market
from pullback_lab.funding import Funding
from pullback_lab.paths import truncate
from pullback_lab.accounting import Replay
from pullback_lab.broker_reconcile import reconcile
from pullback_lab.reports import windows
from . import io as x
from .signals import candidates,generate_grid,DISTANCE,BODY
from .pool import Pool
from .storage import frozen_store_class
from .cache import MIB,StoragePause
from .reporting import augment,opportunity_rows,regions,account_screen,SCREEN
from .download import write_archive

VERSION='AUDUSD_PULLBACK_LONG_MATRIX_V1_2026_10_22'
MODES=('FROZEN_14OCT2026_RATES','ADVERSE_6PCT_ANNUAL')


def configuration():
    return dict(version=VERSION,pair='AUD_USD',side='BUY',source_signature=x.PINS['signature'],
        candidate_rules=candidates(),distance_max_values=list(DISTANCE),body_min_values=list(BODY),
        rr=3.,entry_limit_ticks=15,stop_buffer_ticks=10,initial_gbp=100.,risk_percent=1.,
        source_window='2005-01-01T00:00:00Z THROUGH 2026-09-20T18:15:00Z; OBSERVED WARMUP MAY DELAY START',
        funding_modes=list(MODES),account_scenario='END_ASC',
        rejection_policy='FILTER_FAILURE_RETAINS_SETUP; ACCEPTED_SIGNAL_CONSUMES_SETUP_EVEN_IF_EXECUTION_LATER_REJECTS',
        reset_policy='ORIGINAL_EIGHT_BAR_EXPIRY_GAP_AND_H1_TREND_RESETS',
        entry_model='NATIVE_M15_OPEN_PROXY; M1/S5_BARRIER_REFINEMENT',
        plateau_screen=SCREEN,untouched_holdout=False,automatic_selection=False,
        full_portfolio_validated=False,orders_supported=False,orders_sent=False,
        historical_financing_known=False,source_history_modified=False,
        path_cache_identity='ORIGINAL_V2_CODE_SIGNATURE_AND_IDENTICAL_NATIVE_HISTORY_AND_GEOMETRY; NEW_STUDY_SIGNATURE_SEPARATE')


class Research:
    def __init__(self,environ=None):
        env=os.environ if environ is None else environ
        self.env={k:env[k] for k in ('PULLBACK_OUTPUT_DIR','AUDUSD_MATRIX_SOURCE_DIR','AUDUSD_MATRIX_OUTPUT_DIR',
            'AUDUSD_MATRIX_RAW_CACHE_MB','AUDUSD_MATRIX_RESERVE_MB','PULLBACK_LEGACY_CACHE_DIR',
            'OANDA_ENV','OANDA_ENVIRONMENT','OANDA_TOKEN','OANDA_API_TOKEN','OANDA_API_KEY','OANDA_ACCESS_TOKEN') if k in env}
        root=Path(__file__).resolve().parents[1];x.engine_check(root)
        self.source=Path(self.env.get('AUDUSD_MATRIX_SOURCE_DIR',str(Path(self.env.get('PULLBACK_OUTPUT_DIR','/data/p32_pullback_pass1'))/x.PINS['signature']))).resolve()
        self.directory=Path(self.env.get('AUDUSD_MATRIX_OUTPUT_DIR','/data/p32_audusd_long_matrix')).resolve()
        e.require(not self.directory.is_relative_to(self.source) and not self.source.is_relative_to(self.directory),'Matrix outputs must be separate from the original study')
        # Keep a new cache beside signatures, so a safe restart can reuse it.
        self.cache=self.directory/'canonical_candles'
        self.protocol=configuration();files=sorted(x.ROOT.glob('*.py'))+sorted(x.ROOT.glob('*.json'))+[root/'audusd_pullback_matrix.py']
        self.proof={p.name:x.sha_file(p) for p in files}
        self.signature=e.sha(e.canon(dict(protocol=self.protocol,source_code=self.proof,engine=x.ENGINE)))
        self.work=self.directory/self.signature;self.lock=threading.RLock();self.thread=None
        self.reserve=int(self.env.get('AUDUSD_MATRIX_RESERVE_MB','96'))*MIB
        self.raw_mb=int(self.env.get('AUDUSD_MATRIX_RAW_CACHE_MB','128'))
        e.require(64<=self.raw_mb<=256 and self.reserve>=32*MIB,'Use a raw cache of 64–256 MB and reserve of at least 32 MB')
        self.state=dict(status='IDLE',phase='WAITING_FOR_STATUS',version=VERSION,signature=self.signature,
            candidates_completed=0,candidates_required=16,accounts_completed=0,accounts_required=224,
            orders_supported=False,orders_sent=False,full_portfolio_validated=False,result_ready=False,
            raw_cache_limit_bytes=self.raw_mb*MIB,storage_reserve_bytes=self.reserve)
    def progress(self,**values):
        # Stop before a full disk; every finished account/path is independently
        # committed and checked on restart. Never delete an original source file.
        if self.state.get('status')=='RUNNING' and values.get('status') not in ('ERROR','COMPLETE','PAUSED_STORAGE') and self.directory.exists() and shutil.disk_usage(self.directory).free<self.reserve:
            raise StoragePause('Storage reserve reached; completed accounts and private path checkpoints are preserved')
        with self.lock:
            self.state.update(values,last_progress_utc=e.stamp(e.datetime.now(e.UTC)))
            if self.work.is_dir():x.atomic(self.work/'status.json',e.canon(self.state))
    def status(self,start=False):
        with self.lock:
            if start and self.thread is None and self.state['status']=='IDLE':
                self.state.update(status='STARTING');self.thread=threading.Thread(target=self.run,daemon=True,name='audusd-matrix');self.thread.start()
            result=dict(self.state,worker_alive=bool(self.thread and self.thread.is_alive()))
            if self.directory.exists():result['free_disk_bytes']=shutil.disk_usage(self.directory).free
            marker=self.directory/'PERSISTENCE_MARKER.json'
            result['persistence_marker']=json.loads(marker.read_bytes()) if marker.is_file() else None
            if result.get('last_progress_utc'):result['seconds_since_last_progress']=round((e.datetime.now(e.UTC)-e.dt(result['last_progress_utc'])).total_seconds(),1)
            return result
    def prepare_source(self):return x.source_check(self.source,self.progress)
    def make_store(self):
        e.require(self.env.get('OANDA_ENV',self.env.get('OANDA_ENVIRONMENT','live')).lower()=='live','Set OANDA_ENV=live for the frozen broker source')
        token=next((self.env.get(k,'').strip() for k in ('OANDA_TOKEN','OANDA_API_TOKEN','OANDA_API_KEY','OANDA_ACCESS_TOKEN') if self.env.get(k,'').strip()),'')
        e.require(token,'Keep the existing OANDA_TOKEN for read-only historical candle GETs')
        reader=e.CandleReader(token,BROKER,requests_per_second=4)
        cls=frozen_store_class(self.source,self.source/'source_index.json.gz',x.PINS['source_index_semantic_sha256'],
            legacy_root=self.env.get('PULLBACK_LEGACY_CACHE_DIR','/data/p32_signal_admission'),raw_mb=self.raw_mb,reserve_mb=self.reserve//MIB)
        return cls(self.cache,reader=reader,progress=self.progress)
    def load_history(self,store,source):
        start,end,warm=(e.dt(source[k]) for k in ('start','end','warmup_start'))
        m15,mc=load_bars(store,'AUD_USD','M15',warm,start,end,self.progress)
        h1,hc=load_bars(store,'AUD_USD','H1',warm,start,end,self.progress)
        ready=max(e.dt(mc['indicator_ready_from_utc']),e.dt(hc['indicator_ready_from_utc']))
        return m15,h1,ready,dict(m15=mc,h1=hc)
    def run(self):
        resources=[];handle=None
        try:
            self.directory.mkdir(parents=True,exist_ok=True);handle=(self.directory/'runner.lock').open('a')
            try:fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:self.state.update(status='BUSY',phase='ANOTHER_PROCESS_RUNNING');return
            e.require(shutil.disk_usage(self.directory).free>=self.reserve,'Insufficient free reserve for the matrix; original reports are untouched')
            from pullback_lab.runner import persistence
            mounted=persistence(self.directory,create=True)
            e.require(mounted['separate_mount_detected'],'Keep AUDUSD_MATRIX_OUTPUT_DIR on the mounted /data volume')
            self.work.mkdir(parents=True,exist_ok=True)
            x.atomic(self.work/'protocol.json',e.canon(dict(signature=self.signature,**self.protocol)))
            (self.work/'ERROR.json').unlink(missing_ok=True)
            self.progress(status='RUNNING',phase='VERIFY_FROZEN_SOURCE',started_at_utc=e.stamp(e.datetime.now(e.UTC)))
            source,coverage,original=self.prepare_source()
            evidence=json.loads((Path(__file__).resolve().parents[1]/'pullback_lab/saved_broker_evidence.json').read_bytes())
            broker,checks=reconcile(evidence)
            e.require(broker['status']=='PASS','Saved GBP broker accounting reconciliation failed')
            x.atomic(self.work/'broker_reconciliation.json',e.canon(dict(summary=broker,checks=checks)))
            store=self.make_store();resources.append(store)
            m15,h1,ready,history=self.load_history(store,source)
            e.require(e.stamp(ready)==coverage['eligibility']['effective_evaluation_start_utc'],'Observed warm-up readiness differs from frozen source')
            signals,counts=generate_grid(m15,h1,source['start'],source['end'],ready,self.progress)
            parity=x.baseline_parity(signals['BASELINE'],original);x.atomic(self.work/'baseline_parity.json',e.canon(parity))
            del original,h1;gc.collect()
            pool=Pool(self.work,store,m15,'AUD_USD',e.dt(source['end']),x.PINS['signature'],self.progress);resources.append(pool);self.pool=pool
            seeded=pool.seed_source(self.source/'pairs/AUD_USD',signals['BASELINE'],self.progress)
            x.atomic(self.work/'source_provenance.json',e.canon(dict(source_pins=x.PINS,engine_pins=x.ENGINE,
                matrix_code=self.proof,history=history,path_reuse=seeded,source_files_modified=False,
                all_history_seen=True,historical_broker_terms_reconstructed=False)))
            self.process_candidates(signals,counts,pool,store,evidence,broker,source,ready)
            x.atomic(self.work/'private_source_index.json.gz',gzip.compress(e.canon(store.index())))
            self.progress(phase='FINALIZE_REPORTS',candidates_completed=16)
            self.publish(complete=True)
            self.progress(status='COMPLETE',phase='COMPLETE',candidates_completed=16)
        except Exception as exc:
            safe=str(exc) if isinstance(exc,(ValueError,e.CandleFetchError,FileNotFoundError)) else type(exc).__name__
            self.state.update(status='PAUSED_STORAGE' if isinstance(exc,StoragePause) else 'ERROR',phase='STOPPED',error=safe)
            if self.work.is_dir():
                try:
                    x.atomic(self.work/'ERROR.json',e.canon(dict(error=safe,type=type(exc).__name__)))
                    x.atomic(self.work/'status.json',e.canon(self.state));self.publish(complete=False)
                except Exception:pass
        finally:
            for resource in reversed(resources):
                try:resource.close()
                except Exception:pass
            if handle:handle.close()
    def process_candidates(self,signals,counts,pool,store,evidence,broker,source,ready):
        funding=Funding(evidence['funding_schedules']);markup=broker['observed_symmetric_conversion_factor_markup']
        term=next(i for i in evidence['instruments'] if i['name']=='AUD_USD')
        ws=[dict(window_id='FULL_HISTORY',start=source['start'],end=source['end'])]+windows(source['start'],source['end'])
        e.require(len(ws)==7,'Expected full history, five eras and recent window')
        total=0;done=0
        for rule in candidates():
            cid=rule['candidate_id'];directory=self.work/'candidates'/cid;directory.mkdir(parents=True,exist_ok=True)
            if self.verify_complete(directory):
                done+=1;total+=14;self.progress(candidates_completed=done,accounts_completed=total,phase='REUSE_COMPLETE_CANDIDATE',candidate_id=cid);continue
            selected=signals[cid];self.progress(phase='PRICE_CANDIDATE',candidate_id=cid,candidate_number=done+1,qualifications_required=len(selected))
            x.write_jsonl(directory/'signals.jsonl.gz',selected);x.atomic(directory/'generation_counts.json',e.canon(counts[cid]))
            market=Market(store,self.progress);opps=[]
            for n,s in enumerate(selected):
                p=pool.get(s);shadow=pool.shadow(s,p,market,funding,term,markup)
                opps.append(dict(signal_key=s['signal_key'],pair='AUD_USD',side='BUY',control=s['control'],entry_utc=s['entry_utc'],features=s['features'],
                    path_cache_key=p['path_cache_key'],path_status=p['status'],shadow=shadow,counterfactual_outcome_status=p.get('counterfactual_outcome_status'),
                    counterfactual_price_r=p.get('observed_path_price_r') if p['status']=='ENTRY_PROXY_REJECTED' else None))
                if n%50==0:self.progress(phase='PRICE_CANDIDATE',candidate_id=cid,qualifications_processed=n+1,qualifications_required=len(selected),native_paths_computed=pool.computed,path_cache_hits=pool.cached)
            x.write_jsonl(directory/'opportunities.jsonl.gz',opps)
            x.atomic(directory/'opportunity_summaries.csv',x.csv_bytes(opportunity_rows(rule,opps,source['start'],source['end'],e.stamp(ready))))
            del opps;gc.collect();summaries=[]
            for window in ws:
                lo=max(e.dt(window['start']),ready);hi=e.dt(window['end'])
                subset=[s for s in selected if lo<=e.dt(s['entry_utc'])<hi]
                for mode in MODES:
                    label=window['window_id']+'__'+mode;path=directory/'accounts'/(label+'.json.gz')
                    if path.is_file():
                        checkpoint=json.loads(gzip.decompress(path.read_bytes()))
                        e.require(checkpoint['signature']==self.signature and e.sha(e.canon(checkpoint['result']))==checkpoint['sha256'],'Account checkpoint signature/checksum failed')
                        result=checkpoint['result']
                    else:
                        self.progress(phase='REPLAY_GBP_ACCOUNT',candidate_id=cid,window_id=window['window_id'],funding_mode=mode)
                        result=Replay(subset,lambda s:truncate(pool.get(s),hi),Market(store,self.progress),funding,evidence['instruments'],
                            'END_ASC',mode,markup,initial=100.,start=lo,end=hi,progress=self.progress).run()
                        e.require(result['summary']['status']=='COMPLETE_CONDITIONAL','Account stopped: '+str(result['summary']['stopped_reason']))
                        # Reconcile before saving a reusable successful checkpoint.
                        augment(result,lo,hi)
                        x.atomic(path,gzip.compress(e.canon(dict(signature=self.signature,result=result,sha256=e.sha(e.canon(result))))))
                    summary=augment(result,lo,hi)
                    summaries.append(dict(**rule,window_id=window['window_id'],requested_start=window['start'],effective_start=e.stamp(lo),end=window['end'],
                        complete_requested_window=lo==e.dt(window['start']),scope='ONE_CANDIDATE_AUDUSD_LONG_GBP100_NOT_LIVE32_COMBINED',**summary))
                    total+=1;self.progress(accounts_completed=total)
            x.atomic(directory/'account_summaries.json',e.canon(summaries))
            files={p.relative_to(directory).as_posix():x.sha_file(p) for p in directory.rglob('*') if p.is_file() and not p.name.endswith('.tmp') and p.name!='COMPLETE.json'}
            x.atomic(directory/'COMPLETE.json',e.canon(dict(signature=self.signature,candidate_id=cid,accounts=14,files=files)))
            done+=1;self.progress(candidates_completed=done,phase='CANDIDATE_COMPLETE',candidate_id=cid)
            self.publish(complete=False)
    def verify_complete(self,directory):
        marker=directory/'COMPLETE.json'
        if not marker.is_file():return False
        pin=json.loads(marker.read_bytes());e.require(pin['signature']==self.signature,'Completed candidate signature changed')
        for name,digest in pin['files'].items():
            p=(directory/name).resolve();e.require(p.is_relative_to(directory.resolve()) and p.is_file() and x.sha_file(p)==digest,'Completed candidate report changed: '+name)
        return True
    def publish(self,complete=False):
        dirs=[self.work/'candidates'/r['candidate_id'] for r in candidates() if (self.work/'candidates'/r['candidate_id']/'COMPLETE.json').is_file()]
        accounts=[];opps=[]
        import csv,io
        for directory in dirs:
            accounts+=json.loads((directory/'account_summaries.json').read_bytes())
            opps+=list(csv.DictReader(io.StringIO((directory/'opportunity_summaries.csv').read_text())))
        screens=[dict(**r,**account_screen([a for a in accounts if a['candidate_id']==r['candidate_id']])) for r in candidates() if any(a['candidate_id']==r['candidate_id'] for a in accounts)]
        blocks=regions(candidates(),accounts)
        text=f'''# AUD/USD long conditional pullback matrix

Completed candidates: {len(dirs)}/16. Completed fresh GBP100 accounts: {len(accounts)}/224.
RR3, 15-tick entry bound, 10-tick stop buffer and 1% default NAV risk are fixed.
Nine joint settings, six single-filter controls and one unfiltered baseline.
Each variant independently regenerates the causal eight-bar pullback state.
A filter failure leaves the setup alive; accepted qualifications consume it
regardless of later execution rejection or position blocking.

account_matrix.csv contains isolated candidate accounts with full history,
five reset eras and a recent reset window under two funding assumptions.
These windows overlap; results are descriptive and are not independent tests.
Source gaps, barrier timing assumptions and historical broker/funding limitations
remain visible in account-lineage flags. END_ASC and STOP_FIRST are conditional
assumptions, not mathematically guaranteed return bounds.

opportunity_matrix.csv is independent opportunity diagnosis, not an account
return. Price-only rejected counterfactuals stay separate from funded win rates.
plateau_regions.csv checks each adjacent 2x2 block; all four members must satisfy
the declared sample, full/recent, funding, best-five and era screens. Corners
cannot pass alone. Failed criteria and uncertainty counts remain visible.
No parameter or candidate is selected automatically. No combined live32
admission/overlap test, RR sweep or untouched out-of-sample claim is made.

Download /results-summary.zip for this small review. /results.zip streams full
completed-candidate signals, account ledgers and checkpoints without storing a
second large archive. The private candle cache is bounded; evicted bodies may
need re-fetching. Source windows previously used must match frozen checksums.
The original research directories are never deleted or rewritten.
'''
        pool=getattr(self,'pool',None)
        if pool is not None:
            def path_rows():
                for key,body,digest in pool.db.execute('SELECT key,body,sha256 FROM paths ORDER BY key'):
                    e.require(e.sha(body)==digest,'Shared path checkpoint checksum failed while exporting')
                    yield json.loads(body)
            x.write_jsonl(self.work/'unique_price_paths.jsonl.gz',path_rows())
        with self.lock:
            x.atomic(self.work/'account_matrix.csv',x.csv_bytes(accounts));x.atomic(self.work/'opportunity_matrix.csv',x.csv_bytes(opps))
            x.atomic(self.work/'candidate_screens.csv',x.csv_bytes(screens));x.atomic(self.work/'plateau_regions.csv',x.csv_bytes(blocks))
            x.atomic(self.work/'REVIEW.md',text.encode())
            x.atomic(self.work/'run_state.json',e.canon(dict(status='COMPLETE_CONDITIONAL_MATRIX' if complete else 'PARTIAL',signature=self.signature,
                published_candidates=[p.name for p in dirs],candidates_completed=len(dirs),candidates_required=16,
                accounts_completed=len(accounts),accounts_required=224,plateau_regions_passing=sum(b['conditional_plateau_screen_pass'] for b in blocks),
                automatic_selection=False,full_portfolio_validated=False)))
        target=self.work/'AUDUSD_MATRIX_SUMMARY.zip';temporary=target.with_suffix('.zip.tmp')
        with temporary.open('wb') as f:write_archive(self,f,summary=True)
        os.replace(temporary,target)
        self.state.update(result_ready=True,result='/results-summary.zip',full_result='/results.zip')
