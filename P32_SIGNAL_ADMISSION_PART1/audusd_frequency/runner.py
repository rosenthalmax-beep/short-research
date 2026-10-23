"""Bounded native-candle diagnostic, without path pricing or account replay."""
import fcntl
import gzip
import json
import os
import shutil
import sqlite3
import zipfile
from pathlib import Path
from pullback_lab import core as e
from pullback_lab.runner import persistence
from pullback_lab.reports import windows
from audusd_matrix import io as x
from audusd_matrix.runner import Research as MatrixResearch
from audusd_matrix.signals import generate_grid as original_generate
from audusd_matrix.storage import IDENTITY_FIELDS
from audusd_matrix.cache import StoragePause,MIB
from .analysis import Audit,RULES,summarize
from .generator import generate_grid

VERSION='AUDUSD_PULLBACK_FREQUENCY_DIAGNOSTIC_V1_2026_10_23'
HERE=Path(__file__).resolve().parent
PINS=json.loads((HERE/'reference_pins.json').read_bytes())


def protocol():
    return dict(version=VERSION,rules=RULES,baseline_lifetime_bars=8,post_expiry_observation_max_bar_age=16,
        observation_cutoffs=[12,16],observation_requires_unbroken_m15_and_h1_trend=True,
        source_cutoff_utc='2026-09-20T18:15:00Z',rr=3.,entry_limit_ticks=15,stop_buffer_ticks=10,
        body_floor_m15_atr=1.25,account_replays=0,new_strategy_signals=0,orders_supported=False,orders_sent=False,
        current_live_portfolio_modified=False,automatic_parameter_selection=False,
        observations_are_not_additional_trades=True,all_history_seen=True,untouched_holdout=False,
        finer_candles_downloaded=False,read_only_source_directories=True,
        max_new_native_window_requests=128,max_http_attempts_including_retries=384,private_raw_cache_mb=64,storage_reserve_mb=96)


class NativeReader:
    def __init__(self,reader):self.reader=reader;self.calls=0
    @property
    def receipts(self):return self.reader.receipts
    def get(self,ident):
        e.require(ident['pair']=='AUD_USD' and ident['granularity'] in ('M15','H1'),'Only native AUD/USD history is supported')
        e.require(self.calls<128,'128 native candle GET budget exhausted; keep existing caches and resume')
        self.calls+=1;return self.reader.get(ident)


def exact_control(directory,cid,signals):
    base=directory/'candidates'/cid
    marker=json.loads((base/'COMPLETE.json').read_bytes())
    e.require(marker['signature']==PINS['signature'] and marker['candidate_id']==cid,'Extension control signature changed')
    path=base/'signals.jsonl.gz'
    e.require(x.sha_file(path)==marker['files']['signals.jsonl.gz'],'Saved extension signal checksum changed')
    with gzip.open(path,'rb') as f:old=[json.loads(line) for line in f]
    e.require(e.canon(old)==e.canon(signals),'Eight-bar signals do not exactly match saved extension: '+cid)
    return dict(candidate_id=cid,status='PASS',all_signal_fields_checked=True,signals_matched=len(old),
                saved_signal_file_sha256=x.sha_file(path))


class Research(MatrixResearch):
    def __init__(self,environ=None):
        env=dict(os.environ if environ is None else environ)
        self.extension=(Path(env.get('AUDUSD_EXTENSION_OUTPUT_DIR','/data/p32_audusd_long_extension'))/PINS['signature']).resolve()
        self.old_matrix=Path(env.get('AUDUSD_MATRIX_OUTPUT_DIR','/data/p32_audusd_long_matrix')).resolve()
        output=Path(env.get('AUDUSD_FREQUENCY_OUTPUT_DIR','/data/p32_audusd_frequency')).resolve()
        matrix_env=dict(env,AUDUSD_MATRIX_OUTPUT_DIR=str(output),AUDUSD_MATRIX_RAW_CACHE_MB='64',AUDUSD_MATRIX_RESERVE_MB='96')
        super().__init__(matrix_env)
        for old in (self.source,self.extension.parent,self.old_matrix):
            e.require(not self.directory.is_relative_to(old) and not old.is_relative_to(self.directory),'Frequency outputs must be separate from previous research directories')
        self.protocol=protocol()
        self.proof={p.relative_to(HERE.parent).as_posix():x.sha_file(p) for p in sorted(HERE.rglob('*')) if p.suffix in ('.py','.json')}
        self.proof['audusd_pullback_frequency.py']=x.sha_file(HERE.parent/'audusd_pullback_frequency.py')
        self.signature=e.sha(e.canon(dict(protocol=self.protocol,code=self.proof,engine=x.ENGINE,reference=PINS)))
        self.work=self.directory/self.signature
        self.state=dict(status='IDLE',phase='WAITING_FOR_STATUS',version=VERSION,signature=self.signature,
            candidates_required=3,candidates_completed=0,accounts_required=0,orders_supported=False,orders_sent=False,
            result_ready=False,source_files_modified=False)
    def make_store(self):
        store=super().make_store();store.reader=NativeReader(store.reader)
        for parent in (self.extension.parent,self.old_matrix):
            path=parent/'canonical_candles/candles.sqlite3'
            if not path.is_file() or any(Path(str(path)+s).exists() and Path(str(path)+s).stat().st_size for s in ('-wal','-journal')):continue
            con=sqlite3.connect(path.resolve().as_uri()+'?mode=ro&immutable=1',uri=True)
            con.execute('PRAGMA query_only=ON')
            if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='blocks'").fetchone():store.legacy.insert(0,(path,con))
            else:con.close()
        return store
    def verify_extension(self):
        for name,digest in PINS['files'].items():
            e.require(x.sha_file(self.extension/name)==digest,'Frozen extension report missing or changed: '+name)
        source=json.loads((self.extension/'source_provenance.json').read_bytes())
        for name,digest in source['original_matrix_code'].items():
            path=HERE.parent/'audusd_pullback_matrix.py' if name=='audusd_pullback_matrix.py' else HERE.parent/'audusd_matrix'/name
            e.require(x.sha_file(path)==digest,'Pinned original matrix code changed: '+name)
        return dict(status='PASS',signature=PINS['signature'],files=PINS['files'])
    def resume_complete(self):
        marker=self.work/'RUN_COMPLETE.json'
        if not marker.is_file():return False
        saved=json.loads(marker.read_bytes());e.require(saved['signature']==self.signature,'Completed diagnostic signature changed')
        for name,digest in saved['files'].items():
            path=(self.work/name).resolve()
            e.require(path.is_relative_to(self.work.resolve()) and path.is_file() and x.sha_file(path)==digest,'Completed report missing or changed: '+name)
        self.state.update(status='COMPLETE',phase='COMPLETE',candidates_completed=3,result_ready=True,
            result='/results-summary.zip',full_result='/results.zip',resumed_without_history_load=True)
        return True
    def run(self):
        handles=[];store=None
        try:
            self.directory.mkdir(parents=True,exist_ok=True)
            h=(self.directory/'runner.lock').open('a');handles.append(h)
            fcntl.flock(h,fcntl.LOCK_EX|fcntl.LOCK_NB)
            e.require(persistence(self.directory,create=True)['separate_mount_detected'],'Keep frequency output directory on mounted /data')
            e.require(shutil.disk_usage(self.directory).free>=self.reserve,'Need 96 MiB free reserve; previous files are untouched')
            # Acquire the existing extension lock for shared reading. Do not create
            # or modify a lock inside the old research directory.
            old_lock=self.extension.parent/'runner.lock'
            e.require(old_lock.is_file(),'Completed extension runner.lock is missing')
            h=old_lock.open('rb');handles.append(h);fcntl.flock(h,fcntl.LOCK_SH|fcntl.LOCK_NB)
            witness=self.verify_extension()
            if self.resume_complete():return
            self.work.mkdir(parents=True,exist_ok=True)
            (self.work/'ERROR.json').unlink(missing_ok=True)
            self.progress(status='RUNNING',phase='VERIFY_FROZEN_SOURCE',started_at_utc=e.stamp(e.datetime.now(e.UTC)))
            x.atomic(self.work/'protocol.json',e.canon(dict(signature=self.signature,**self.protocol)))
            source,coverage,baseline=self.prepare_source()
            store=self.make_store()
            m15,h1,ready,history=self.load_history(store,source)
            e.require(e.stamp(ready)==coverage['eligibility']['effective_evaluation_start_utc'],'Warm-up differs from frozen source')
            audits={r['candidate_id']:Audit(r,m15.times,e.stamp(ready),source['end']) for r in RULES}
            signals,counts=generate_grid(m15,h1,source['start'],source['end'],ready,self.progress,rules=RULES,audits=audits)
            for rule in RULES:
                cid=rule['candidate_id'];audits[cid].annotate(signals[cid])
            regenerated,_=original_generate(m15,h1,source['start'],source['end'],ready,self.progress,
                rules=[dict(candidate_id='BASELINE',kind='BASELINE',distance_max=None,body_min=None)])
            baseline_check=x.baseline_parity(regenerated['BASELINE'],baseline)
            controls=[exact_control(self.extension,r['candidate_id'],signals[r['candidate_id']]) for r in RULES]
            x.atomic(self.work/'parity.json',e.canon(dict(status='PASS',baseline=baseline_check,extension=controls)))
            ws=[dict(window_id='FULL_HISTORY',start=e.stamp(ready),end=source['end'])]+windows(source['start'],source['end'])
            rows=[];near=[]
            for rule in RULES:
                cid=rule['candidate_id'];audit=audits[cid]
                for w in ws:
                    lo=e.stamp(max(e.dt(w['start']),ready));hi=w['end']
                    row=summarize(audit,lo,hi,w['window_id'])
                    e.require(row['setup_fates_reconcile'] and row['late_observations_reconcile'],'Lifecycle counts do not reconcile')
                    rows.append(row)
                    for margin in (.10,.25):
                        for typ in ('BODY_ONLY','DISTANCE_ONLY'):
                            field='body_shortfall_atr' if typ=='BODY_ONLY' else 'distance_excess_atr'
                            subset=[t for t in audit.triggers if lo<=t['signal_close_utc']<hi and t['outcome']==typ and 0<t[field]<=margin]
                            near.append(dict(candidate_id=cid,window_id=w['window_id'],failure_type=typ,
                                inclusive_shortfall_limit_atr=margin,attempts=len(subset),
                                distinct_setups=len({t['setup_start_utc'] for t in subset}),thresholds_not_changed=True))
                d=self.work/'candidates'/cid;d.mkdir(parents=True,exist_ok=True)
                x.write_jsonl(d/'signals.jsonl.gz',signals[cid]);x.write_jsonl(d/'setups.jsonl.gz',audit.setups)
                x.write_jsonl(d/'trigger_attempts.jsonl.gz',audit.triggers);x.write_jsonl(d/'post_expiry_observations.jsonl.gz',audit.late)
                x.atomic(d/'generation_counts.json',e.canon(counts[cid]))
                self.progress(phase='REPORT_CANDIDATE',candidates_completed=len([r for r in rows if r['window_id']=='FULL_HISTORY']))
            x.atomic(self.work/'frequency_summary.csv',x.csv_bytes(rows));x.atomic(self.work/'near_misses.csv',x.csv_bytes(near))
            x.atomic(self.work/'source_provenance.json',e.canon(dict(signature=self.signature,code=self.proof,
                extension_witness=witness,source_pins=x.PINS,history=history,source_files_modified=False,
                original_signal_payloads_reproduced=True,source_cache_read_only=True,
                native_window_requests=store.reader.calls,private_cache_limit_mb=64,
                exact_cache_reuses=store.reused,coverage_audit_data_used=False)))
            x.atomic(self.work/'private_source_index.json.gz',gzip.compress(e.canon(store.index())))
            x.atomic(self.work/'REVIEW.md',REVIEW.encode())
            reports={p.relative_to(self.work).as_posix():x.sha_file(p) for p in self.work.rglob('*') if p.is_file() and p.name not in ('status.json','RUN_COMPLETE.json','ERROR.json') and not p.name.endswith('.zip')}
            x.atomic(self.work/'RUN_COMPLETE.json',e.canon(dict(status='COMPLETE_DIAGNOSTIC',signature=self.signature,
                candidates_completed=3,exact_signal_parity='PASS',setup_reconciliation='PASS',files=reports,
                account_replays=0,orders_sent=False,late_observations_are_additional_trades=False)))
            self.progress(phase='FINALIZE_REPORTS')
            final_state=dict(self.state,status='COMPLETE',phase='COMPLETE',candidates_completed=3,result_ready=True,
                result='/results-summary.zip',full_result='/results.zip')
            x.atomic(self.work/'status.json',e.canon(final_state))
            self.archive(self.work/'AUDUSD_FREQUENCY_SUMMARY.zip',summary=True)
            self.archive(self.work/'AUDUSD_FREQUENCY_FULL.zip',summary=False)
            with self.lock:self.state.update(final_state)
        except BlockingIOError:
            self.state.update(status='BUSY',phase='RESEARCH_PROCESS_STILL_RUNNING',error='Stop the existing research process before redeploying this entry point')
        except Exception as exc:
            safe=str(exc) if isinstance(exc,(ValueError,FileNotFoundError,e.CandleFetchError)) else type(exc).__name__
            self.state.update(status='PAUSED_STORAGE' if isinstance(exc,StoragePause) else 'ERROR',phase='STOPPED',error=safe)
            if self.work.is_dir():
                x.atomic(self.work/'ERROR.json',e.canon(dict(error=safe,type=type(exc).__name__)))
                x.atomic(self.work/'status.json',e.canon(self.state))
        finally:
            if store:store.close()
            for h in reversed(handles):h.close()
    def archive(self,path,summary=True):
        temporary=path.with_suffix('.tmp')
        # Only compact diagnostics, never raw candles or an existing ZIP.
        with zipfile.ZipFile(temporary,'w',zipfile.ZIP_DEFLATED) as z:
            for p in sorted(self.work.rglob('*')):
                rel=p.relative_to(self.work)
                if not p.is_file() or p.suffix in ('.zip','.tmp') or p.name=='private_source_index.json.gz':continue
                if summary and len(rel.parts)>1:continue
                z.write(p,rel.as_posix())
        e.require(temporary.stat().st_size<20*MIB,'Diagnostic download exceeded the 20 MiB cap')
        os.replace(temporary,path)


REVIEW='''# AUD/USD stronger-body setup frequency diagnosis

Three fixed body-1.25 controls at H1 distance caps 1.00, 1.25 and 1.50.
The unchanged eight-bar generator must reproduce every saved extension signal
field, plus the original 4,086-signal unfiltered baseline, before reports pass.

frequency_summary.csv: setup cohorts are assigned by their touch time and
followed through the frozen full-history cutoff. Era-cohort outcomes can occur
beyond an era boundary; they are not fresh-account era results. Trigger attempts
are assigned by close time and have a separate denominator. FULL and RECENT
windows overlap; do not sum them. Gap/trend cancellations and right-censoring
are explicit. Setups begun before report eligibility are excluded.

A lifetime of eight bars means ages 1-8 can enter. Expiry occurs before age 9.
For an expired setup only, an independent observation follows the inherited
low through ages 9-16. A gap or failure of the original fresh H1 trend cancels
that observation. Record only its FIRST engulf above EMA20 which passes the
unchanged body and distance filters; include the expiry candle itself (age 9).
Counts by 12/16 are cumulative and overlap. Late observations can overlap each
other and a fresh baseline signal; the latter is identified explicitly.

IMPORTANT: These are hypothetical setup-continuation observations, not signals
from a rerun with a 12/16-bar lifetime, and not additional independent trades.
Longer lifetimes change subsequent arming, consumption, inherited lows, stop
geometry and execution/admission. No priced outcome, profitability or portfolio
uplift is inferred. A separate causal signal and fresh-account replay would be
needed before comparing changed lifetimes. No automatic expiry recommendation
or parameter selection is made.

near_misses.csv counts rejected trigger attempts with ONLY body or ONLY distance
failing, within inclusive 0.10/0.25 ATR shortfalls. Those bands overlap and do not
change thresholds. Full trigger rows and setup lifecycle rows are in /results.zip.

Cached AUD/USD native M15/H1 windows are read-only reused. At most 128 missing
native windows may be fetched, with frozen source checksums enforced. No M1/S5,
GBP conversion or path pricing, account replay, live executor or strategy change.
Private raw cache is bounded at 64 MiB with 96 MiB reserve. The original warm-up,
cutoff, previously seen history and source uncertainty remain unchanged. This
report cannot establish an edge or a robust plateau.
'''
