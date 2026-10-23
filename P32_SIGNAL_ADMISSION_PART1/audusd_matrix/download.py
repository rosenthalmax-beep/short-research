"""Small summaries; full completed-candidate archives streamed without disk copy."""
import hashlib
import json
import queue
import threading
import zipfile
from pathlib import Path
from . import io as x

FIXED=('protocol.json','status.json','ERROR.json','source_provenance.json','baseline_parity.json',
       'broker_reconciliation.json','account_matrix.csv','opportunity_matrix.csv','plateau_regions.csv',
       'candidate_screens.csv','REVIEW.md','run_state.json')


def snapshot(job,summary=False):
    with job.lock:
        state=json.loads((job.work/'run_state.json').read_bytes()) if (job.work/'run_state.json').is_file() else dict(status='PARTIAL',published_candidates=[])
        files=[]
        for name in FIXED:
            p=job.work/name
            if p.is_file():files.append((name,p.read_bytes(),None))
        if not summary:
            for name in ('private_source_index.json.gz','unique_price_paths.jsonl.gz'):
                index=job.work/name
                if index.is_file():files.append((index.name,index.read_bytes(),None))
            for cid in state['published_candidates']:
                directory=job.work/'candidates'/cid;pin=json.loads((directory/'COMPLETE.json').read_bytes())
                if pin['signature']!=job.signature:raise ValueError('Completed candidate signature mismatch')
                files.append((f'candidates/{cid}/COMPLETE.json',(directory/'COMPLETE.json').read_bytes(),None))
                for name,digest in pin['files'].items():
                    path=(directory/name).resolve()
                    if not path.is_relative_to(directory.resolve()):raise ValueError('Unsafe report path')
                    files.append((f'candidates/{cid}/{name}',path,digest))
        state.update(archive_scope='SUMMARY_ONLY' if summary else 'FULL_COMPLETED_CANDIDATE_REPORTS',orders_supported=False,orders_sent=False)
        return files,state


def write_archive(job,handle,summary=False,prepared=None):
    files,state=prepared or snapshot(job,summary);manifest={}
    with zipfile.ZipFile(handle,'w',zipfile.ZIP_DEFLATED,compresslevel=1,allowZip64=True) as z:
        for name,source,expected in sorted(files):
            h=hashlib.sha256();size=0
            with z.open(name,'w',force_zip64=True) as out:
                if isinstance(source,bytes):out.write(source);h.update(source);size=len(source)
                else:
                    with source.open('rb') as f:
                        for block in iter(lambda:f.read(256*1024),b''):out.write(block);h.update(block);size+=len(block)
            if expected and h.hexdigest()!=expected:raise ValueError('Completed candidate changed during download: '+name)
            manifest[name]=dict(bytes=size,sha256=h.hexdigest())
        z.writestr('report_manifest.json',x.canon(manifest));z.writestr('RUN_COMPLETE.json',x.canon(state))


class Stream:
    def __init__(self,job,summary=False):
        self.job=job;self.summary=summary;self.prepared=snapshot(job,summary)
        self.cancel=threading.Event();self.queue=queue.Queue(maxsize=4);self.position=0
    def write(self,data):
        for i in range(0,len(data),256*1024):
            while not self.cancel.is_set():
                try:self.queue.put(data[i:i+256*1024],timeout=.2);break
                except queue.Full:pass
            if self.cancel.is_set():raise BrokenPipeError('Download closed')
        self.position+=len(data);return len(data)
    def tell(self):return self.position
    def flush(self):pass
    def produce(self):
        try:write_archive(self.job,self,self.summary,self.prepared);end=None
        except Exception as exc:end=exc
        while not self.cancel.is_set():
            try:self.queue.put(end,timeout=.2);break
            except queue.Full:pass
    def __iter__(self):
        thread=threading.Thread(target=self.produce,daemon=True,name='matrix-zip');thread.start()
        try:
            while True:
                item=self.queue.get()
                if item is None:break
                if isinstance(item,Exception):raise item
                yield item
        finally:self.close()
    def close(self):self.cancel.set()
