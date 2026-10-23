"""Share immutable geometry paths and funding outcomes across matrix members."""
import gzip
import json
from pullback_lab import core as e
from pullback_lab.paths import Paths,shadow_gbp
from pullback_lab.market import QuoteUnavailable


class Pool(Paths):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.db.execute('CREATE TABLE IF NOT EXISTS shadows(key TEXT PRIMARY KEY,body BLOB,sha256 TEXT)');self.db.commit()
    def key_for(self,s):
        return e.sha(e.canon(dict(signature=self.signature,native_sha256=self.source_sha,pair=self.pair,
            side=s['side'],entry=s['entry_utc'],reference=s['reference_entry'],stop=s['stop'],rr=s['rr'],end=e.stamp(self.end),limit=15)))
    def seed_source(self,pair_dir,baseline,progress=lambda **x:None):
        by={s['signal_key']:s for s in baseline};mapping={};seen=0
        with gzip.open(pair_dir/'price_paths.jsonl.gz','rb') as f:
            for line in f:
                row=json.loads(line);sid=row.pop('signal_key')
                if sid not in by:continue
                e.require(row['native_source_sha256']==self.source_sha,'AUD/USD native-history hash differs from the frozen study')
                key=self.key_for(by[sid]);e.require(key==row['path_cache_key'],'Saved baseline path geometry identity mismatch')
                raw=e.canon(row);digest=e.sha(raw)
                old=self.db.execute('SELECT sha256 FROM paths WHERE key=?',(key,)).fetchone()
                if old:e.require(old[0]==digest,'Saved baseline path conflicts with existing private checkpoint')
                else:self.db.execute('INSERT INTO paths VALUES(?,?,?)',(key,raw,digest))
                mapping[sid]=key;seen+=1
                if seen%500==0:self.db.commit();progress(phase='REUSE_BASELINE_PATHS',source_paths_reused=seen)
        e.require(set(mapping)==set(by),'Frozen baseline path export is incomplete')
        shadow_count=0
        with gzip.open(pair_dir/'opportunities.jsonl.gz','rb') as f:
            for line in f:
                row=json.loads(line);sid=row['signal_key']
                if sid not in mapping:continue
                key=mapping[sid];raw=e.canon(row['shadow']);digest=e.sha(raw)
                old=self.db.execute('SELECT sha256 FROM shadows WHERE key=?',(key,)).fetchone()
                if old:e.require(old[0]==digest,'Saved baseline financing conflicts with private checkpoint')
                else:self.db.execute('INSERT INTO shadows VALUES(?,?,?)',(key,raw,digest))
                shadow_count+=1
        self.db.commit();e.require(shadow_count==len(by),'Frozen baseline opportunity export is incomplete')
        return dict(source_paths_reused=seen,source_standalone_outcomes_reused=shadow_count,native_sha256=self.source_sha)
    def shadow(self,s,path,market,funding,terms,markup):
        key=self.key_for(s);old=self.db.execute('SELECT body,sha256 FROM shadows WHERE key=?',(key,)).fetchone()
        if old:
            e.require(e.sha(old[0])==old[1],'Standalone funding checkpoint checksum failed');return json.loads(old[0])
        try:result=shadow_gbp(s,path,market,funding,terms,markup,self.end)
        except QuoteUnavailable as exc:result=dict(shadow_status='UNRESOLVED_CONVERSION',reason=str(exc))
        raw=e.canon(result);self.db.execute('INSERT INTO shadows VALUES(?,?,?)',(key,raw,e.sha(raw)));self.db.commit()
        return result
