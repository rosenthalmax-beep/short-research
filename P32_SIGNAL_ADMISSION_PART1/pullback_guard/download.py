"""Stream full ZIPs with bounded memory and no duplicate archive on disk."""
import hashlib
from contextlib import nullcontext
import json
import queue
import threading
import zipfile
from . import cleanup as c

FIXED=('protocol.json','broker_reconciliation.json','control_accounts.csv','feature_study.csv',
       'REVIEW.md','status.json','ERROR.json','source_index.json.gz','STORAGE_OVERLAY.json')


def snapshot(job,summary=False):
    with getattr(job,'lock',nullcontext()):
        return _snapshot(job,summary)


def _snapshot(job,summary=False):
    files=[]
    state=job.work/'DOWNLOAD_STATE.json'
    published=json.loads(state.read_bytes()) if state.is_file() else None
    allowed=published.get('published_pairs',job.protocol['pairs']) if published else job.protocol['pairs']
    # Snapshot small changing top-level files, stream immutable completed reports.
    for name in FIXED:
        path=job.work/name
        if path.is_file():files.append((name,path.read_bytes(),None))
    for pair in allowed:
        directory=job.work/'pairs'/pair;marker=directory/'COMPLETE.json'
        if not marker.is_file():continue
        pin=json.loads(marker.read_bytes())
        if pin['signature']!=job.signature:raise ValueError('Archive pair signature mismatch')
        files.append((f'pairs/{pair}/COMPLETE.json',marker.read_bytes(),None))
        for name,digest in pin['files'].items():
            if summary and (name in ('opportunities.jsonl.gz','price_paths.jsonl.gz','qualifying_signals.jsonl.gz') or name.startswith('accounts/')):continue
            path=c.safe_file(directory/name,job.work)
            files.append((f'pairs/{pair}/{name}',path,digest))
    completed=sum(name.endswith('/COMPLETE.json') for name,_,_ in files)
    run=published if published else dict(status='PARTIAL',signature=job.signature,pairs_completed=completed,orders_supported=False,orders_sent=False)
    run.update(archive_scope='SUMMARY_ONLY' if summary else 'FULL_COMPLETED_PAIR_REPORTS',raw_cache_bodies_included=False)
    return files,run


def write_archive(job,handle,summary=False,prepared=None):
    files,run=prepared or snapshot(job,summary)
    manifest={}
    with zipfile.ZipFile(handle,'w',zipfile.ZIP_DEFLATED,compresslevel=1,allowZip64=True) as z:
        for name,source,expected in sorted(files):
            digest=hashlib.sha256();size=0
            with z.open(name,'w',force_zip64=True) as out:
                if isinstance(source,bytes):
                    out.write(source);digest.update(source);size=len(source)
                else:
                    with source.open('rb') as incoming:
                        for chunk in iter(lambda:incoming.read(256*1024),b''):
                            digest.update(chunk);size+=len(chunk);out.write(chunk)
            if expected and digest.hexdigest()!=expected:raise ValueError('A completed report changed during download: '+name)
            manifest[name]=dict(bytes=size,sha256=digest.hexdigest())
        z.writestr('report_manifest.json',c.canon(manifest))
        z.writestr('RUN_COMPLETE.json',c.canon(run))


class Stream:
    def __init__(self,job,summary=False):
        self.job=job;self.summary=summary;self.cancel=threading.Event();self.items=queue.Queue(maxsize=4)
        self.prepared=snapshot(job,summary);self.position=0;self.thread=None
    def write(self,data):
        for start in range(0,len(data),256*1024):
            block=data[start:start+256*1024]
            while not self.cancel.is_set():
                try:self.items.put(block,timeout=.2);break
                except queue.Full:pass
            if self.cancel.is_set():raise BrokenPipeError('Download closed')
        self.position+=len(data);return len(data)
    def tell(self):return self.position
    def flush(self):pass
    def _produce(self):
        try:
            write_archive(self.job,self,summary=self.summary,prepared=self.prepared)
            item=None
        except Exception as exc:item=exc
        while not self.cancel.is_set():
            try:self.items.put(item,timeout=.2);break
            except queue.Full:pass
    def __iter__(self):
        self.thread=threading.Thread(target=self._produce,daemon=True,name='stream-pullback-zip');self.thread.start()
        try:
            while True:
                item=self.items.get()
                if item is None:break
                if isinstance(item,Exception):raise item
                yield item
        finally:self.close()
    def close(self):self.cancel.set()
