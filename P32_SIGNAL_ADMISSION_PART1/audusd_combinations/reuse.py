"""Read-only access to the reviewed matrix, preserving exact old price evidence."""
import csv
import gzip
import io
import json
import sqlite3
from pathlib import Path
from pullback_lab import core as e
from audusd_matrix import io as x
from audusd_matrix.pool import Pool
from .config import OVERLAPS
from .provenance_guard import verify_report

HERE=Path(__file__).resolve().parent
PINS=json.loads((HERE/'previous_pins.json').read_bytes())
FIELDS=('schema','broker','pair','granularity','start','end','price','smooth','includeFirst')


def safe_file(root,name):
    path=Path(root)/name
    e.require(not Path(name).is_absolute() and not path.is_symlink() and path.resolve().is_relative_to(Path(root).resolve())
              and path.is_file(),'Missing or unsafe prior matrix file: '+name)
    return path


def immutable_db(path,tables):
    path=Path(path)
    if not path.is_file():return None
    if any(Path(str(path)+suffix).is_file() and Path(str(path)+suffix).stat().st_size for suffix in ('-wal','-journal')):return None
    con=sqlite3.connect(path.resolve().as_uri()+'?mode=ro&immutable=1',uri=True)
    con.execute('PRAGMA query_only=ON')
    if not all(con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(t,)).fetchone() for t in tables):
        con.close();return None
    return con


def csv_value(value):
    if value is None:return ''
    return json.dumps(value,sort_keys=True) if isinstance(value,(dict,list)) else str(value)


class Previous:
    def __init__(self,root,progress=lambda **x:None,pins=None,lifetime_extension_root='/data/p32_audusd_lifetime_extension'):
        self.root=Path(root);self.lifetime_extension_root=Path(lifetime_extension_root);self.pins=PINS if pins is None else pins;self.witness={};self.provenance_checks=[];self.provenance_identity={}
        e.require(self.root.name==self.pins['signature'],'Previous extension path must end in its reviewed signature')
        for name,w in self.pins['files'].items():
            path=safe_file(self.root,name)
            observed,check=verify_report(path,w['sha256'],w['bytes'])
            if check:
                self.provenance_checks.append(check)
                self.provenance_identity[name]=dict(reviewed_sha256=w['sha256'],stable_fields_sha256=check['stable_fields_sha256'])
            self.witness[name]=observed;progress(phase='VERIFY_PREVIOUS_MATRIX',verified_previous_file=name)
        state=json.loads((self.root/'run_state.json').read_bytes())
        e.require(state['status']=='COMPLETE_CONDITIONAL_RR_MATRIX' and state['accounts_completed']==self.pins.get('completed_accounts',1106)
                  and state['candidates_completed']==self.pins.get('completed_candidates',79) and state['signature']==self.pins['signature'],'Previous extension is incomplete')
        self.top_rows={(r['candidate_id'],r['window_id'],r['financing_mode']):r for r in csv.DictReader(io.StringIO((self.root/'account_matrix.csv').read_text()))}
        e.require(len(self.top_rows)==self.pins.get('completed_accounts',1106),'Previous summary account count differs from reviewed manifest')
        self.controls={}
        for cid in OVERLAPS:
            directory=self.root/'candidates'/cid;marker=safe_file(directory,'COMPLETE.json')
            pin=json.loads(marker.read_bytes())
            e.require(pin['signature']==self.pins['signature'] and pin['candidate_id']==cid and pin['accounts']==14,'Previous overlap candidate incomplete')
            self.witness['candidates/'+cid+'/COMPLETE.json']=x.sha_file(marker)
            for name,checksum in pin['files'].items():
                path=safe_file(directory,name);e.require(x.sha_file(path)==checksum,'Previous overlap export changed: '+cid+'/'+name)
                self.witness['candidates/'+cid+'/'+name]=checksum
            with gzip.open(directory/'signals.jsonl.gz','rb') as f:signals=[json.loads(l) for l in f]
            with gzip.open(directory/'opportunities.jsonl.gz','rb') as f:opportunities=[json.loads(l) for l in f]
            summaries=json.loads((directory/'account_summaries.json').read_bytes())
            e.require(len(summaries)==14,'Previous overlap must contain 14 summaries')
            for row in summaries:
                expected=self.top_rows[(cid,row['window_id'],row['financing_mode'])]
                e.require(set(row)<=set(expected) and all(csv_value(row.get(k))==v for k,v in expected.items()),
                          'Previous overlap summaries differ from reviewed matrix CSV')
            self.controls[cid]=dict(signals=signals,opportunities=opportunities)
        self.index_path=safe_file(self.root,'private_source_index.json.gz')
        # Recompression headers/residency metadata do not change price identity.
        source_rows=json.loads(gzip.decompress(self.index_path.read_bytes()))
        relevant=[]
        for r in source_rows:
            if r['pair'] not in ('AUD_USD','GBP_USD'):continue
            ident={k:r[k] for k in FIELDS};e.require(e.sha(e.canon(ident))==r['key'],'Previous price-window identity changed')
            relevant.append(dict(ident,key=r['key'],sha256=r['sha256']))
        self.source_rows=sorted(relevant,key=lambda r:r['key'])
        e.require(len({r['key'] for r in self.source_rows})==len(self.source_rows),'Duplicate previous source-window identities')
        self.index_semantic_sha256=e.sha(e.canon(self.source_rows))
        self.path_db=safe_file(self.root,'paths.sqlite3')

    def ancestors(self,matrix_root,extension_root,lifetime_root):
        """Trace latest lifetime extension -> lifetime -> body extension -> matrix."""
        witness=json.loads(safe_file(self.root,'previous_matrix_witness.json').read_bytes())
        self.ancestor_indices=[];self.ancestor_files=[];result=[]
        roots=[self.lifetime_extension_root,Path(lifetime_root),Path(extension_root),Path(matrix_root)]
        for i,root in enumerate(roots):
            directory=root/witness['signature']
            for name,digest in witness['files'].items():
                if '/' in name:continue
                path=safe_file(directory,name)
                observed,check=verify_report(path,digest)
                if check:self.provenance_checks.append(check)
                self.ancestor_files.append((path,observed))
            index=safe_file(directory,'private_source_index.json.gz');rows=source_rows(index)
            digest=e.sha(e.canon(rows))
            e.require(digest==witness['private_source_index_semantic_sha256'],'Ancestor price windows differ from reviewed witness: '+str(directory))
            self.ancestor_indices.append((index,digest))
            result.append((type('AncestorSource',(),dict(source_rows=rows))(),root/'canonical_candles/candles.sqlite3'))
            if i+1<len(roots):witness=json.loads(safe_file(directory,'previous_matrix_witness.json').read_bytes())
        return result

    def checkpoint(self,cid,window,mode):
        path=safe_file(self.root/'candidates'/cid,'accounts/'+window+'__'+mode+'.json.gz')
        cp=json.loads(gzip.decompress(path.read_bytes()))
        e.require(cp['signature']==self.pins['signature'] and e.sha(e.canon(cp['result']))==cp['sha256'],'Previous account payload checksum failed')
        return cp['result']

    def assert_unchanged(self):
        for path,digest in getattr(self,'ancestor_files',[]):
            e.require(x.sha_file(path)==digest,'Ancestor extension report changed during replay')
        for path,digest in getattr(self,'ancestor_indices',[]):
            e.require(e.sha(e.canon(source_rows(path)))==digest,'Ancestor source index changed during replay')
        for name,checksum in self.witness.items():e.require(x.sha_file(safe_file(self.root,name))==checksum,'Previous matrix changed during extension: '+name)
        rows=json.loads(gzip.decompress(self.index_path.read_bytes()))
        selected=sorted((dict(**{k:r[k] for k in FIELDS},key=r['key'],sha256=r['sha256'])
                         for r in rows if r['pair'] in ('AUD_USD','GBP_USD')),key=lambda r:r['key'])
        e.require(e.sha(e.canon(selected))==self.index_semantic_sha256,'Previous price-source index changed during extension')

    def proof(self):
        # Stable identity retains the reviewed hash for the provenance file.
        # Observed byte hashes/counter changes live in the separate audit; actual
        # observed bytes remain locked by assert_unchanged for this running job.
        identity=dict(self.witness)
        for name,check in self.provenance_identity.items():identity[name]=check['reviewed_sha256']
        return dict(signature=self.pins['signature'],files=identity,
                    provenance_identity=self.provenance_identity,
                    private_source_index_semantic_sha256=self.index_semantic_sha256,
                    price_windows=len(self.source_rows),source_files_modified=False)


def add_previous_sources(store,previous,cache_path):
    for r in previous.source_rows:
        ident={k:r[k] for k in FIELDS};encoded=e.canon(ident).decode()
        old=store.db.execute('SELECT identity,sha256 FROM expected_sources WHERE key=?',(r['key'],)).fetchone()
        e.require(old is None or old==(encoded,r['sha256']),'Frozen original and previous matrix source windows disagree')
        if old is None:store.db.execute('INSERT INTO expected_sources VALUES(?,?,?)',(r['key'],encoded,r['sha256']))
    store.db.commit()
    con=immutable_db(cache_path,('blocks',))
    if con is not None:store.legacy.insert(0,(Path(cache_path),con))


class ReusePool(Pool):
    def __init__(self,*args,previous=None,**kwargs):
        super().__init__(*args,**kwargs)
        self.previous_db=immutable_db(previous.path_db,('paths','shadows')) if previous is not None else None
        e.require(previous is None or self.previous_db is not None,'Keep the completed previous matrix path database and stop its writer before extension')
        self.previous_checked=set();self.previous_paths_verified=0;self.previous_paths_copied=0

    def get(self,signal):
        key=self.key_for(signal)
        if self.previous_db is not None and key not in self.previous_checked:
            for table in ('paths','shadows'):
                row=self.previous_db.execute('SELECT body,sha256 FROM '+table+' WHERE key=?',(key,)).fetchone()
                if row is None:continue
                body,checksum=row;e.require(e.sha(body)==checksum,'Previous geometry checkpoint checksum failed')
                if table=='paths':
                    p=json.loads(body)
                    e.require(p['path_cache_key']==key and p['native_source_sha256']==self.source_sha,'Previous path geometry/native history mismatch')
                    self.previous_paths_verified+=1
                old=self.db.execute('SELECT sha256 FROM '+table+' WHERE key=?',(key,)).fetchone()
                e.require(old is None or old[0]==checksum,'Original and previous geometry checkpoints disagree')
                if old is None:
                    self.db.execute('INSERT INTO '+table+' VALUES(?,?,?)',(key,body,checksum))
                    if table=='paths':self.previous_paths_copied+=1
            self.db.commit();self.previous_checked.add(key)
        return super().get(signal)

    def close(self):
        if self.previous_db is not None:self.previous_db.close()
        super().close()


def source_rows(path):
    raw=json.loads(gzip.decompress(Path(path).read_bytes()));rows=[]
    for r in raw:
        if r['pair'] not in ('AUD_USD','GBP_USD'):continue
        ident={k:r[k] for k in FIELDS}
        e.require(e.sha(e.canon(ident))==r['key'],'Ancestor price-window identity changed')
        rows.append(dict(ident,key=r['key'],sha256=r['sha256']))
    rows.sort(key=lambda r:r['key'])
    e.require(len({r['key'] for r in rows})==len(rows),'Duplicate ancestor source-window identities')
    return rows
