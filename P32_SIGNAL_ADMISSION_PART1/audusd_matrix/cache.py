"""Bounded private raw cache; source study and old caches stay read-only."""
import hashlib
import json
import shutil
import sqlite3
import time
from pathlib import Path
from types import SimpleNamespace

MIB=1024*1024
class StoragePause(ValueError):pass
c=SimpleNamespace(EVICT_SQL="CREATE TABLE IF NOT EXISTS evicted_sources(key TEXT PRIMARY KEY,identity TEXT NOT NULL,sha256 TEXT NOT NULL,origin TEXT NOT NULL)",TOUCH_SQL="CREATE TABLE IF NOT EXISTS cache_touch(key TEXT PRIMARY KEY,last_use INTEGER NOT NULL)")

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

