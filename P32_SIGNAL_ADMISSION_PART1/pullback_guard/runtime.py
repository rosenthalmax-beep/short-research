"""Storage-only overlay. The pinned V2 calculation modules are never modified."""
import fcntl
import gzip
import hashlib
import json
import os
import shutil
import time
from pathlib import Path
from . import cleanup as c

VERSION='P32_PULLBACK_STORAGE_GUARD_V1_2026_10_20'
MIB=1024*1024


class StoragePause(ValueError):
    pass


def source_check(root):
    pins=json.loads((Path(__file__).parent/'v2_source_sha256.json').read_bytes())
    for name,digest in pins.items():
        path=Path(root)/name
        if not path.is_file() or c.sha_file(path)!=digest:
            raise ValueError('Storage recovery requires the unchanged V2 calculation files: '+name)
    return dict(calculation_files_verified=len(pins),calculation_source_unchanged=True,
                overlay_version=VERSION,overlay_source_sha256={p.name:c.sha_file(p) for p in sorted(Path(__file__).parent.glob('*.py'))})


def cache_class(base,limit,reserve):
    class BoundedStore(base):
        def __init__(self,*args,**kwargs):
            directory=Path(args[0] if args else kwargs['directory'])
            directory.mkdir(parents=True,exist_ok=True)
            path=directory/'candles.sqlite3'
            if not path.exists():
                import sqlite3
                with sqlite3.connect(path) as con:con.execute('PRAGMA auto_vacuum=INCREMENTAL')
            super().__init__(*args,**kwargs)
            self.db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            self.db.execute('PRAGMA journal_mode=DELETE')
            self.db.execute(c.EVICT_SQL);self.db.execute(c.TOUCH_SQL);self.db.commit()
            self.raw_bytes=self.db.execute('SELECT coalesce(sum(length(body)),0) FROM blocks').fetchone()[0]
            self.clock=time.time_ns()
            self.trim()

        def trim(self,force=False):
            low=shutil.disk_usage(self.directory).free < reserve+16*MIB
            if not force and self.raw_bytes<=limit and not low:return
            # Keep native history in preference to fine execution windows. All
            # bodies may be evicted; their immutable identities/checksums survive.
            target=limit if not low else min(limit//2,max(0,self.raw_bytes-32*MIB))
            victims=self.db.execute('''SELECT b.key,b.identity,b.sha256,b.origin,length(b.body)
                FROM blocks b LEFT JOIN cache_touch t ON b.key=t.key
                ORDER BY CASE WHEN b.identity LIKE '%"granularity":"M15"%' OR b.identity LIKE '%"granularity":"H1"%' THEN 1 ELSE 0 END,
                coalesce(t.last_use,0),b.key''')
            keys=[]
            for key,ident,digest,origin,size in victims:
                if self.raw_bytes<=target:break
                keys.append((key,ident,digest,origin));self.raw_bytes-=size
            if keys:
                self.db.executemany('INSERT OR REPLACE INTO evicted_sources VALUES(?,?,?,?)',keys)
                self.db.executemany('DELETE FROM blocks WHERE key=?',[(r[0],) for r in keys])
                self.db.executemany('DELETE FROM cache_touch WHERE key=?',[(r[0],) for r in keys])
                self.db.commit()
                # Incremental auto-vacuum returns reusable free pages to disk.
                while self.db.execute('PRAGMA freelist_count').fetchone()[0]:
                    before=self.db.execute('PRAGMA freelist_count').fetchone()[0]
                    self.db.execute('PRAGMA incremental_vacuum(4096)');self.db.commit()
                    if self.db.execute('PRAGMA freelist_count').fetchone()[0]>=before:break
            if shutil.disk_usage(self.directory).free<reserve:
                raise StoragePause('Storage reserve reached. Checkpoints are retained; download results before changing storage settings.')

        def exact(self,pair,gran,start,end):
            from pullback_lab import core as e
            ident=self.identity(pair,gran,start,end);key=e.sha(e.canon(ident))
            self.trim()
            expected=self.db.execute('SELECT sha256 FROM evicted_sources WHERE key=?',(key,)).fetchone()
            present=self.db.execute('SELECT length(body) FROM blocks WHERE key=?',(key,)).fetchone()
            result=super().exact(pair,gran,start,end)
            row=self.db.execute('SELECT length(body),sha256 FROM blocks WHERE key=?',(key,)).fetchone()
            if expected and row and row[1]!=expected[0]:
                self.db.execute('DELETE FROM blocks WHERE key=?',(key,));self.db.commit();self.memo.pop(key,None)
                raise ValueError('Previously evicted candle window changed on re-fetch; recovery stopped to prevent mixing revised history: '+key)
            if row:
                if not present:self.raw_bytes+=row[0]
                self.clock+=1
                self.db.execute('INSERT OR REPLACE INTO cache_touch VALUES(?,?)',(key,self.clock))
                self.db.execute('DELETE FROM evicted_sources WHERE key=?',(key,));self.db.commit()
            self.trim()
            return result

        def index(self):
            result=super().index()
            for key,ident,digest,origin in self.db.execute('SELECT key,identity,sha256,origin FROM evicted_sources ORDER BY key'):
                result.append(dict(json.loads(ident),key=key,sha256=digest,origin=origin,
                                   raw_body_evicted=True,used_this_process=key in self.used))
            return sorted(result,key=lambda r:r['key'])
    return BoundedStore


def configure(job,root):
    """Patch this one Research instance, preserving its original study signature."""
    from pullback_lab import core as e
    from pullback_lab.reports import atomic,csv_bytes,readme
    from .download import write_archive
    proof=source_check(root)
    limit=int(job.env.get('PULLBACK_RAW_CACHE_MB','128'))*MIB
    reserve=int(job.env.get('PULLBACK_STORAGE_RESERVE_MB','96'))*MIB
    if not 16*MIB<=limit<=512*MIB or reserve<32*MIB:
        raise ValueError('Use a raw cache of 16–512 MB and a storage reserve of at least 32 MB')
    original_run=job.run;original_progress=job.progress
    job.store_factory=cache_class(job.store_factory,limit,reserve)
    job.storage_preparing=False;job.storage_archiving=False
    job.state.update(storage_overlay=proof,raw_cache_limit_bytes=limit,storage_reserve_bytes=reserve,
                     full_results_streamed=True,raw_candle_bodies_may_be_evicted=True)

    def progress(**values):
        if job.storage_preparing:
            with job.lock:job.state.update(values,last_progress_utc=e.stamp(e.datetime.now(e.UTC)))
            return
        if (job.state.get('status')=='RUNNING' and values.get('status') not in ('ERROR','NEEDS_DATA','COMPLETE')
                and not job.storage_archiving and shutil.disk_usage(job.directory).free<reserve):
            raise StoragePause('Storage reserve reached. The study stopped before exhausting disk; existing reports/checkpoints are retained.')
        original_progress(**values)
    job.progress=progress

    def archive(complete=False):
        job.storage_archiving=True
        try:
            # On errors the original runner still holds open checkpoint handles.
            # Cleanup is safe here only after a pair's resources have closed.
            pairs=c.completed_pairs(job.work,job.signature)
            if job.state.get('phase') in ('PAIR_COMPLETE','FINALIZE_REPORTS'):
                c.drop_duplicate_opportunity_databases(job.work,pairs,progress)
                c.prune_completed_paths(job.work,pairs,progress)
            summaries=[];features=[]
            for directory in pairs:
                summaries.extend(json.loads((directory/'account_summaries.json').read_bytes()))
                features.extend(json.loads(gzip.decompress((directory/'feature_study.json.gz').read_bytes())))
            with job.lock:
                atomic(job.work/'control_accounts.csv',csv_bytes(summaries))
                atomic(job.work/'feature_study.csv',csv_bytes(features))
                atomic(job.work/'REVIEW.md',readme(job.protocol,[p.name for p in pairs],summaries,features).encode())
                atomic(job.work/'STORAGE_OVERLAY.json',e.canon(dict(**proof,signature=job.signature,
                       raw_cache_limit_bytes=limit,refetch_checksum_enforced=True,full_report_bodies_retained=True)))
                atomic(job.work/'DOWNLOAD_STATE.json',e.canon(dict(status='COMPLETE_RESEARCH_PASS1' if complete else 'PARTIAL',
                    signature=job.signature,pairs_completed=len(pairs),pairs_required=len(job.protocol['pairs']),
                    accounts=len(summaries),feature_rows=len(features),published_pairs=[p.name for p in pairs],orders_supported=False,orders_sent=False,
                    full_portfolio_validated=False,automatic_strategy_selection=False)))
            target=job.work/'P32_PULLBACK_SUMMARY.zip';temporary=target.with_suffix('.zip.tmp')
            with temporary.open('wb') as handle:write_archive(job,handle,summary=True)
            os.replace(temporary,target)
            original_progress(result_ready=True,result='/results.zip',summary='/results-summary.zip')
        finally:job.storage_archiving=False
    job.archive=archive

    def run():
        handle=None
        try:
            job.storage_preparing=True
            job.directory.mkdir(parents=True,exist_ok=True)
            lockfile=job.directory/'runner.lock'
            handle=lockfile.open('r+' if lockfile.exists() else 'a')
            try:fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:
                job.state.update(status='BUSY',phase='ANOTHER_PROCESS_RUNNING');return
            progress(status='RECOVERING_STORAGE',phase='VERIFY_SAVED_REPORTS')
            pairs=c.completed_pairs(job.work,job.signature,progress)
            freed=c.remove_regenerable_archives(job.work,pairs)
            freed+=c.drop_duplicate_opportunity_databases(job.work,pairs,progress)
            # Rebuild into smaller files; never VACUUM an entire full-size DB.
            free=shutil.disk_usage(job.directory).free
            if free<reserve+32*MIB:
                raise StoragePause('Not enough scratch space for safe compaction. Verified duplicate ZIPs/checkpoints were reclaimed; all primary reports remain. Download the summary before further recovery.')
            candle_dir=job.directory/'canonical_candles'
            # Shrink the dominant raw cache first, before copying pending paths.
            result=c.compact_candles(candle_dir,min(limit,max(16*MIB,free//4)),progress)
            freed+=result['freed_bytes']
            result=c.prune_completed_paths(job.work,pairs,progress);freed+=result['freed_bytes']
            if shutil.disk_usage(job.directory).free<reserve:raise StoragePause('Insufficient free reserve after recovery; primary reports and pending checkpoints are preserved.')
            progress(storage_bytes_reclaimed=freed,storage_operation='RECOVERY_COMPLETE',
                     preserved_completed_pairs=[p.name for p in pairs],phase='RESUME_ORIGINAL_V2')
            handle.close();handle=None;job.storage_preparing=False
            original_run()
        except Exception as exc:
            # Do not leak request objects, headers, tokens, or transport details.
            safe=str(exc) if isinstance(exc,ValueError) else type(exc).__name__
            job.state.update(status='ERROR',phase='STORAGE_RECOVERY_STOPPED',error=safe)
        finally:
            job.storage_preparing=False
            if handle:handle.close()
    job.run=run
    return job
