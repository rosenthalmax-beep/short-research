"""Private bounded candle cache with immutable-source checksum enforcement."""
import gzip
import json
import sqlite3
from pathlib import Path
from pullback_lab import core as e
from pullback_lab.storage import Store,validate
from .cache import cache_class,MIB

IDENTITY_FIELDS=('schema','broker','pair','granularity','start','end','price','smooth','includeFirst')


def frozen_store_class(source_root,index_path,index_sha,legacy_root=None,raw_mb=128,reserve_mb=96):
    Bounded=cache_class(Store,raw_mb*MIB,reserve_mb*MIB)
    class FrozenStore(Bounded):
        def __init__(self,directory,reader=None,legacy_root_unused=None,progress=lambda **x:None):
            # Never let the inherited Store open an old cache for writes.
            super().__init__(directory,reader=reader,legacy_root=None,progress=progress)
            self.db.execute('CREATE TABLE IF NOT EXISTS expected_sources(key TEXT PRIMARY KEY,identity TEXT,sha256 TEXT)')
            self.db.execute('CREATE TABLE IF NOT EXISTS source_meta(key TEXT PRIMARY KEY,value TEXT)');self.db.commit()
            if index_path is not None:
                rows=json.loads(gzip.decompress(Path(index_path).read_bytes()))
                imported=0
                for row in rows:
                    if row['pair'] not in ('AUD_USD','GBP_USD'):continue
                    ident={k:row[k] for k in IDENTITY_FIELDS};key=e.sha(e.canon(ident))
                    e.require(key==row['key'],'Original source-index identity checksum failed')
                    encoded=e.canon(ident).decode();old=self.db.execute('SELECT identity,sha256 FROM expected_sources WHERE key=?',(key,)).fetchone()
                    if old is not None:e.require(old==(encoded,row['sha256']),'Frozen expected window changed')
                    else:self.db.execute('INSERT INTO expected_sources VALUES(?,?,?)',(key,encoded,row['sha256']))
                    imported+=1
                    if imported%2000==0:self.db.commit();progress(phase='FREEZE_SOURCE_WINDOWS',expected_windows=imported)
                self.db.execute('INSERT OR REPLACE INTO source_meta VALUES(?,?)',('original_index_sha256',index_sha));self.db.commit()
                del rows
            old_paths=[Path(source_root).parent/'canonical_candles/candles.sqlite3'] if source_root else []
            if legacy_root:
                old_paths += [Path(legacy_root)/r for r in ('gbp_valuation_windows_v2/replay_candles.sqlite3',
                    'entry_limit_v1/new_execution_windows/replay_candles.sqlite3','gbp_execution_paths_v2/replay_candles.sqlite3','replay_candles.sqlite3')]
            for path in old_paths:
                if not path.is_file():continue
                wal=Path(str(path)+'-wal')
                # Immutable read mode cannot consume a live WAL. Skip that cache
                # and re-fetch against the source checksums instead.
                if wal.is_file() and wal.stat().st_size:continue
                con=sqlite3.connect(path.resolve().as_uri()+'?mode=ro&immutable=1',uri=True)
                con.execute('PRAGMA query_only=ON')
                if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='blocks'").fetchone():self.legacy.append((path,con))
                else:con.close()
            self.source_verified=set();self.new_source_keys=set()
        def exact(self,pair,gran,start,end):
            ident=self.identity(pair,gran,start,end);key=e.sha(e.canon(ident))
            expected=self.db.execute('SELECT sha256 FROM expected_sources WHERE key=?',(key,)).fetchone()
            result=super().exact(pair,gran,start,end)
            # Validate the exact returned canonical rows, including legacy reuse
            # and a window immediately evicted by the bounded parent Store.
            if key not in self.source_verified:
                digest=e.sha(e.canon(dict(instrument=pair,granularity=gran,candles=result)))
                if expected and digest!=expected[0]:
                    present=self.db.execute('SELECT length(body) FROM blocks WHERE key=?',(key,)).fetchone()
                    if present:self.raw_bytes-=present[0]
                    self.db.execute('DELETE FROM blocks WHERE key=?',(key,))
                    self.db.execute('INSERT OR REPLACE INTO evicted_sources VALUES(?,?,?,?)',(key,e.canon(ident).decode(),expected[0],'FROZEN_ORIGINAL_INDEX'))
                    self.db.commit();self.memo.pop(key,None)
                    raise ValueError('Frozen candle window changed on re-fetch; stopped before using revised history: '+key)
                self.source_verified.add(key)
                if expected is None:self.new_source_keys.add(key)
            return result
    return FrozenStore
