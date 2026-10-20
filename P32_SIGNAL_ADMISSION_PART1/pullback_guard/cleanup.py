"""Reclaim only verified duplicate checkpoints and regenerable raw caches."""
import gzip
import hashlib
import json
import os
import sqlite3
import shutil
from pathlib import Path


def sha_file(path):
    digest=hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''):digest.update(chunk)
    return digest.hexdigest()


def canon(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def safe_file(path,root):
    path=Path(path);root=Path(root).resolve()
    if path.is_symlink() or not path.resolve().is_relative_to(root):
        raise ValueError('Recovery refuses a symlink or a path outside the study directory')
    return path


def completed_pairs(work,signature,progress=lambda **x:None):
    work=Path(work);result=[]
    pair_root=work/'pairs'
    if not pair_root.is_dir():return result
    for directory in sorted(pair_root.iterdir()):
        if not directory.is_dir() or not (directory/'COMPLETE.json').is_file():continue
        safe_file(directory,work)
        pin=json.loads((directory/'COMPLETE.json').read_bytes())
        if pin.get('signature')!=signature:raise ValueError('Completed pair belongs to another study')
        for name,digest in pin['files'].items():
            path=safe_file(directory/name,work)
            if not path.is_file() or sha_file(path)!=digest:
                raise ValueError('Recovery stopped: completed report missing or changed: '+directory.name+'/'+name)
        for required in ('opportunities.jsonl.gz','price_paths.jsonl.gz','qualifying_signals.jsonl.gz','account_summaries.json'):
            if required not in pin['files']:raise ValueError('Recovery requires complete saved reports: '+required)
        result.append(directory)
        progress(storage_operation='VERIFIED_COMPLETE_PAIR',storage_pair=directory.name)
    return result


def remove_regenerable_archives(work,pairs):
    """Reports on disk remain authoritative; duplicate download ZIPs can be rebuilt."""
    work=Path(work);freed=0
    for name in ('P32_PULLBACK_PASS1_RESULTS.zip','P32_PULLBACK_PASS1_RESULTS.zip.tmp'):
        path=safe_file(work/name,work)
        if not path.is_file():continue
        # A partial .tmp archive is never an authoritative report. A complete
        # download archive is removed only after its source pair files verified.
        if name.endswith('.zip') and not pairs:continue
        freed+=path.stat().st_size;path.unlink()
    return freed


def drop_duplicate_opportunity_databases(work,pairs,progress=lambda **x:None):
    freed=0
    for directory in pairs:
        path=safe_file(directory/'opportunity_checkpoints.sqlite3',work)
        if not path.is_file():continue
        # The JSONL export contains the exact checkpoint rows. Check every row
        # before dropping this redundant database, never an in-progress one.
        con=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)
        count=0
        try:
            expected=con.execute('SELECT count(*) FROM items').fetchone()[0]
            with gzip.open(directory/'opportunities.jsonl.gz','rb') as stream:
                for line in stream:
                    if not line.strip():continue
                    obj=json.loads(line);raw=canon(obj)
                    old=con.execute('SELECT body,sha256 FROM items WHERE key=?',(obj['signal_key'],)).fetchone()
                    if old is None or hashlib.sha256(old[0]).hexdigest()!=old[1] or old[0]!=raw:
                        raise ValueError('Recovery stopped: opportunity export/checkpoint mismatch')
                    count+=1
            if count!=expected:raise ValueError('Recovery stopped: opportunity row count mismatch')
        finally:con.close()
        for suffix in ('','-wal','-shm','-journal'):
            item=safe_file(Path(str(path)+suffix),work)
            if item.is_file():freed+=item.stat().st_size;item.unlink()
        progress(storage_operation='REMOVED_VERIFIED_DUPLICATE_DB',storage_pair=directory.name)
    return freed


def prune_completed_paths(work,pairs,progress=lambda **x:None):
    """Keep pending-pair rows byte-for-byte; completed paths have pinned exports."""
    work=Path(work);path=safe_file(work/'paths.sqlite3',work)
    if not path.is_file() or not pairs:return dict(removed=0,kept=0,freed_bytes=0)
    completed={}
    for directory in pairs:
        with gzip.open(directory/'price_paths.jsonl.gz','rb') as f:
            for line in f:
                row=json.loads(line);key=row.get('path_cache_key')
                if not key:raise ValueError('Completed path lacks a cache identity')
                row.pop('signal_key',None)
                digest=hashlib.sha256(canon(row)).hexdigest()
                if key in completed and completed[key]!=digest:
                    raise ValueError('Conflicting completed path exports')
                completed[key]=digest
    source=sqlite3.connect(path)
    temporary=safe_file(work/'paths.storage-compact.sqlite3',work)
    if temporary.is_file():temporary.unlink()
    target=None;removed=kept=0;before=path.stat().st_size
    try:
        source.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        source.execute('PRAGMA journal_mode=DELETE')
        target=sqlite3.connect(temporary)
        target.execute('CREATE TABLE paths(key TEXT PRIMARY KEY,body BLOB,sha256 TEXT)')
        for key,body,digest in source.execute('SELECT key,body,sha256 FROM paths'):
            if hashlib.sha256(body).hexdigest()!=digest:raise ValueError('Path checkpoint checksum failed during recovery')
            if key in completed:
                if completed[key]!=digest:raise ValueError('Completed path export/checkpoint mismatch')
                removed+=1
            else:
                target.execute('INSERT INTO paths VALUES(?,?,?)',(key,body,digest));kept+=1
                if kept%500==0:
                    target.commit()
                    if shutil.disk_usage(work).free<32*1024*1024:raise ValueError('Storage reserve reached while compacting paths; original checkpoints retained')
        target.commit()
        if target.execute('PRAGMA quick_check').fetchone()[0]!='ok':raise ValueError('Compacted pending path database failed its check')
        target.close();target=None;source.close();source=None
        os.replace(temporary,path)
        progress(storage_operation='COMPACTED_PATH_CHECKPOINTS',completed_paths_removed=removed,pending_paths_kept=kept)
        return dict(removed=removed,kept=kept,freed_bytes=max(0,before-path.stat().st_size))
    finally:
        if target is not None:target.close()
        if source is not None:source.close()
        if temporary.exists():temporary.unlink()


EVICT_SQL='''CREATE TABLE IF NOT EXISTS evicted_sources(
 key TEXT PRIMARY KEY,identity TEXT NOT NULL,sha256 TEXT NOT NULL,origin TEXT NOT NULL)'''
TOUCH_SQL='CREATE TABLE IF NOT EXISTS cache_touch(key TEXT PRIMARY KEY,last_use INTEGER NOT NULL)'


def compact_candles(directory,max_body_bytes,progress=lambda **x:None):
    """Bound raw bodies; preserve checksums/identities and all request receipts."""
    directory=Path(directory);path=safe_file(directory/'candles.sqlite3',directory)
    if not path.is_file():return dict(evicted=0,kept=0,freed_bytes=0)
    source=sqlite3.connect(path);target=None
    temporary=safe_file(directory/'candles.storage-compact.sqlite3',directory)
    if temporary.is_file():temporary.unlink()
    before=path.stat().st_size
    try:
        source.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        source.execute('PRAGMA journal_mode=DELETE')
        tables={r[0] for r in source.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
        if tables-{'blocks','receipts','evicted_sources','cache_touch'}:
            raise ValueError('Recovery refuses an unknown candle database schema')
        headers=[]
        for key,identity,digest,origin,size in source.execute('SELECT key,identity,sha256,origin,length(body) FROM blocks'):
            obj=json.loads(identity)
            headers.append((obj['granularity'] in ('M15','H1'),obj['end'],key,size))
        keep=set();used=0
        for _,_,key,size in sorted(headers,reverse=True):
            if used+size<=max_body_bytes:keep.add(key);used+=size
        target=sqlite3.connect(temporary)
        target.execute('PRAGMA auto_vacuum=INCREMENTAL')
        target.execute('CREATE TABLE blocks(key TEXT PRIMARY KEY,identity TEXT,body BLOB,sha256 TEXT,origin TEXT)')
        target.execute('CREATE TABLE receipts(id INTEGER PRIMARY KEY,body TEXT)')
        target.execute(EVICT_SQL);target.execute(TOUCH_SQL)
        if 'evicted_sources' in tables:
            target.executemany('INSERT INTO evicted_sources VALUES(?,?,?,?)',source.execute('SELECT key,identity,sha256,origin FROM evicted_sources'))
        if 'receipts' in tables:
            target.executemany('INSERT INTO receipts VALUES(?,?)',source.execute('SELECT id,body FROM receipts'))
        evicted=0;processed=0
        for row in source.execute('SELECT key,identity,body,sha256,origin FROM blocks'):
            key,identity,body,digest,origin=row
            raw=__import__('zlib').decompress(body)
            if hashlib.sha256(raw).hexdigest()!=digest:raise ValueError('Candle checksum failed during storage recovery')
            if key in keep:
                target.execute('INSERT INTO blocks VALUES(?,?,?,?,?)',row)
                target.execute('DELETE FROM evicted_sources WHERE key=?',(key,))
            else:
                target.execute('INSERT OR REPLACE INTO evicted_sources VALUES(?,?,?,?)',(key,identity,digest,origin));evicted+=1
            processed+=1
            if processed%500==0:
                target.commit()
                if shutil.disk_usage(directory).free<32*1024*1024:raise ValueError('Storage reserve reached while compacting candles; original cache retained')
        target.commit()
        if target.execute('PRAGMA quick_check').fetchone()[0]!='ok':raise ValueError('Compacted candle database failed its check')
        target.close();target=None;source.close();source=None
        os.replace(temporary,path)
        progress(storage_operation='BOUNDED_CANDLE_CACHE',raw_windows_evicted=evicted,raw_body_bytes_retained=used)
        return dict(evicted=evicted,kept=len(keep),freed_bytes=max(0,before-path.stat().st_size))
    finally:
        if target is not None:target.close()
        if source is not None:source.close()
        if temporary.exists():temporary.unlink()
