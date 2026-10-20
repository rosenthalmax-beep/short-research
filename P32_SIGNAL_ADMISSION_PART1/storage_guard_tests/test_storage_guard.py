import gzip
import hashlib
import io
import json
import os
import shutil
import sqlite3
import tempfile
import unittest
import zipfile
import zlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from pullback_guard import cleanup as c
from pullback_guard.runtime import cache_class,configure,source_check,StoragePause,MIB
from pullback_guard.download import Stream
from pullback_lab import core as e
from pullback_lab.storage import Store
from pullback_lab.runner import Research

ROOT=Path(os.environ['V2_ROOT'])

def gzrows(path,rows):
    with gzip.open(path,'wb') as f:
        for row in rows:f.write(c.canon(row)+b'\n')


def completed(root):
    d=root/'pairs'/'AUD_JPY';d.mkdir(parents=True)
    op=dict(signal_key='one',shadow={'net_r':1.25})
    result=dict(path_cache_key='done',outcome='WIN')
    gzrows(d/'opportunities.jsonl.gz',[op]);gzrows(d/'price_paths.jsonl.gz',[dict(signal_key='one',**result)])
    gzrows(d/'qualifying_signals.jsonl.gz',[{'signal_key':'one'}]);(d/'account_summaries.json').write_text('[]')
    pins={p.name:c.sha_file(p) for p in d.iterdir()}
    (d/'COMPLETE.json').write_bytes(c.canon(dict(signature='sig',files=pins)))
    with sqlite3.connect(d/'opportunity_checkpoints.sqlite3') as con:
        con.execute('CREATE TABLE items(key TEXT PRIMARY KEY,body BLOB,sha256 TEXT)')
        raw=c.canon(op);con.execute('INSERT INTO items VALUES(?,?,?)',('one',raw,hashlib.sha256(raw).hexdigest()))
    return d,result


class RecoveryTests(unittest.TestCase):
    def test_source_parity_and_deployed_signature(self):
        proof=source_check(ROOT);self.assertTrue(proof['calculation_source_unchanged'])
        from pullback_lab.runner import configuration,signature
        self.assertEqual(signature(configuration({})),'ede17fd5f452abcf4fa9fc2ec0f73aad0660d2d1f582a96059c841ee64d61dd6')

    def test_cleanup_preserves_reports_and_pending_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);d,result=completed(root)
            pending=root/'pairs'/'EUR_JPY';pending.mkdir();(pending/'opportunity_checkpoints.sqlite3').write_bytes(b'pending sentinel')
            pendingbytes=(pending/'opportunity_checkpoints.sqlite3').read_bytes()
            with sqlite3.connect(root/'paths.sqlite3') as con:
                con.execute('CREATE TABLE paths(key TEXT PRIMARY KEY,body BLOB,sha256 TEXT)')
                for key,obj in [('done',result),('pending',{'path_cache_key':'pending','important':'keep'})]:
                    raw=c.canon(obj);con.execute('INSERT INTO paths VALUES(?,?,?)',(key,raw,hashlib.sha256(raw).hexdigest()))
            with sqlite3.connect(root/'paths.sqlite3') as con:before=con.execute('SELECT * FROM paths WHERE key="pending"').fetchone()
            verified=c.completed_pairs(root,'sig');c.drop_duplicate_opportunity_databases(root,verified)
            stats=c.prune_completed_paths(root,verified)
            self.assertEqual(stats['removed'],1);self.assertEqual(stats['kept'],1)
            with sqlite3.connect(root/'paths.sqlite3') as con:self.assertEqual(con.execute('SELECT * FROM paths').fetchone(),before)
            self.assertEqual((pending/'opportunity_checkpoints.sqlite3').read_bytes(),pendingbytes)
            self.assertFalse((d/'opportunity_checkpoints.sqlite3').exists());self.assertEqual(c.completed_pairs(root,'sig'),verified)

    def test_corrupt_witness_blocks_cleanup(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);d,_=completed(root);(d/'account_summaries.json').write_text('[1]')
            with self.assertRaisesRegex(ValueError,'missing or changed'):c.completed_pairs(root,'sig')
            self.assertTrue((d/'opportunity_checkpoints.sqlite3').is_file())

    def test_different_export_never_removes_checkpoint(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);d,_=completed(root);gzrows(d/'opportunities.jsonl.gz',[dict(signal_key='one',shadow='different')])
            with self.assertRaisesRegex(ValueError,'mismatch'):c.drop_duplicate_opportunity_databases(root,[d])
            self.assertTrue((d/'opportunity_checkpoints.sqlite3').is_file())

    def test_stream_valid_manifest_no_disk_archive_and_summary_scope(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);completed(root);job=SimpleNamespace(work=root,signature='sig',protocol={'pairs':['AUD_JPY']})
            data=b''.join(Stream(job))
            with zipfile.ZipFile(io.BytesIO(data)) as z:
                self.assertIsNone(z.testzip());self.assertIn('pairs/AUD_JPY/opportunities.jsonl.gz',z.namelist())
                for name,meta in json.loads(z.read('report_manifest.json')).items():self.assertEqual(hashlib.sha256(z.read(name)).hexdigest(),meta['sha256'])
            with zipfile.ZipFile(io.BytesIO(b''.join(Stream(job,True)))) as z:
                self.assertNotIn('pairs/AUD_JPY/opportunities.jsonl.gz',z.namelist())
                self.assertEqual(json.loads(z.read('RUN_COMPLETE.json'))['archive_scope'],'SUMMARY_ONLY')
            self.assertFalse(list(root.glob('*.zip')))

    def test_head_health_do_not_start_or_cleanup(self):
        from pullback_storage_guard import application
        with tempfile.TemporaryDirectory() as tmp:
            job=configure(Research(output=tmp,environ={}),ROOT);app=application(job)
            for path,method in [('/health','GET'),('/status','HEAD')]:
                list(app({'PATH_INFO':path,'REQUEST_METHOD':method},lambda *x:None))
            self.assertIsNone(job.thread);self.assertEqual(job.state['status'],'IDLE')
            self.assertFalse((Path(tmp)/'canonical_candles').exists())


class Reader:
    def __init__(self):self.receipts=[];self.changed=False;self.calls=0
    def get(self,ident):
        self.calls+=1
        v='1.251' if self.changed else '1.25'
        ohlc={k:v for k in 'ohlc'}
        return dict(instrument=ident['pair'],granularity=ident['granularity'],candles=[dict(time=ident['start'],complete=True,mid=ohlc,bid=ohlc,ask=ohlc)])


class CacheTests(unittest.TestCase):
    def test_evicted_refetch_identical_then_revised_stops(self):
        with tempfile.TemporaryDirectory() as tmp:
            reader=Reader();cls=cache_class(Store,128*MIB,0);store=cls(tmp,reader=reader)
            a=e.dt('2020-01-01T00:00:00Z');b=e.dt('2020-01-02T00:00:00Z')
            first=store.exact('GBP_USD','M1',a,b);store.close()
            result=c.compact_candles(Path(tmp),0);self.assertEqual(result['evicted'],1)
            store=cls(tmp,reader=reader);self.assertEqual(store.exact('GBP_USD','M1',a,b),first);store.close()
            c.compact_candles(Path(tmp),0);reader.changed=True;store=cls(tmp,reader=reader)
            with self.assertRaisesRegex(ValueError,'changed on re-fetch'):store.exact('GBP_USD','M1',a,b)
            self.assertEqual(store.db.execute('SELECT count(*) FROM blocks').fetchone()[0],0)
            self.assertEqual(store.db.execute('SELECT count(*) FROM evicted_sources').fetchone()[0],1);store.close()

    def test_cache_compacts_physical_size_keeps_receipts_and_native_priority(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'candles.sqlite3'
            with sqlite3.connect(path) as con:
                con.execute('CREATE TABLE blocks(key TEXT PRIMARY KEY,identity TEXT,body BLOB,sha256 TEXT,origin TEXT)')
                con.execute('CREATE TABLE receipts(id INTEGER PRIMARY KEY,body TEXT)')
                con.execute('INSERT INTO receipts VALUES(1,?)',('receipt preserved',))
                for n in range(30):
                    raw=os.urandom(20000);ident=json.dumps(dict(granularity='M15' if n==0 else 'M1',end=f'{n:03}'))
                    con.execute('INSERT INTO blocks VALUES(?,?,?,?,?)',(str(n),ident,zlib.compress(raw),hashlib.sha256(raw).hexdigest(),'TEST'))
            old=path.stat().st_size;c.compact_candles(Path(tmp),60030)
            self.assertLess(path.stat().st_size,old//2)
            with sqlite3.connect(path) as con:
                self.assertEqual(con.execute('PRAGMA auto_vacuum').fetchone()[0],2)
                self.assertIsNotNone(con.execute('SELECT key FROM blocks WHERE key="0"').fetchone())
                self.assertEqual(con.execute('SELECT body FROM receipts').fetchone()[0],'receipt preserved')
                self.assertEqual(con.execute('SELECT count(*) FROM blocks').fetchone()[0]+con.execute('SELECT count(*) FROM evicted_sources').fetchone()[0],30)

    def test_runtime_cache_bound_and_reserve_stop(self):
        with tempfile.TemporaryDirectory() as tmp:
            cls=cache_class(Store,300,0);reader=Reader();store=cls(tmp,reader=reader)
            for n in range(20):
                a=e.dt('2020-01-01T00:00:00Z')+e.timedelta(days=n)
                store.exact('GBP_USD','M1',a,a+e.timedelta(days=1))
                self.assertLessEqual(store.db.execute('SELECT coalesce(sum(length(body)),0) FROM blocks').fetchone()[0],300)
            self.assertGreater(store.db.execute('SELECT count(*) FROM evicted_sources').fetchone()[0],0)
            self.assertEqual(len(store.index()),20);store.close()
            with patch('pullback_guard.runtime.shutil.disk_usage',return_value=SimpleNamespace(free=0)):
                with self.assertRaises(StoragePause):cache_class(Store,300,32*MIB)(tmp,reader=reader)


if __name__=='__main__':unittest.main()
