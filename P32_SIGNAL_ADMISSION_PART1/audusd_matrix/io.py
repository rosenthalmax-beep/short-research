"""Checksums, small report writes and source/engine provenance."""
import gzip
import hashlib
import json
from pathlib import Path
from pullback_lab import core as e
from pullback_lab.reports import atomic,csv_bytes,write_jsonl

ROOT=Path(__file__).resolve().parent
PINS=json.loads((ROOT/'source_pins.json').read_bytes())
ENGINE=json.loads((ROOT/'engine_pins.json').read_bytes())
canon=e.canon


def sha_file(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
    return h.hexdigest()


def source_check(root,progress=lambda **x:None):
    root=Path(root);pair=root/'pairs/AUD_USD'
    e.require((root/'protocol.json').is_file(),'Completed source study not found; keep PULLBACK_OUTPUT_DIR=/data/p32_pullback_pass1')
    e.require(sha_file(root/'protocol.json')==PINS['protocol_sha256'],'Frozen source protocol checksum changed')
    source_rows=json.loads(gzip.decompress((root/'source_index.json.gz').read_bytes()))
    keys=('schema','broker','pair','granularity','start','end','price','smooth','includeFirst')
    relevant=[{**{k:r[k] for k in keys},'key':r['key'],'sha256':r['sha256']} for r in source_rows if r['pair'] in ('AUD_USD','GBP_USD')]
    relevant.sort(key=lambda r:r['key'])
    e.require(e.sha(canon(relevant))==PINS['source_index_semantic_sha256'],'Frozen price-source index changed')
    del source_rows,relevant
    protocol=json.loads((root/'protocol.json').read_bytes());marker=json.loads((pair/'COMPLETE.json').read_bytes())
    e.require(marker['signature']==PINS['signature'] and protocol['signature']==PINS['signature'],'Source study signature mismatch')
    for name,digest in PINS['files'].items():
        p=pair/name
        e.require(not p.is_symlink() and p.resolve().is_relative_to(root.resolve()),'Unsafe source export path')
        e.require(marker['files'].get(name)==digest and p.is_file() and sha_file(p)==digest,'Saved source file missing or changed: '+name)
        progress(phase='VERIFY_FROZEN_SOURCE',verified_source_file=name)
    baseline=[]
    with gzip.open(pair/'qualifying_signals.jsonl.gz','rb') as f:
        for line in f:
            r=json.loads(line)
            if (r['pair'],r['side'],r['control'])==('AUD_USD','BUY','TREND_PULLBACK_ENGULF'):baseline.append(r)
    return protocol,json.loads((pair/'coverage.json').read_bytes()),baseline


def engine_check(root):
    for name,digest in ENGINE.items():
        p=Path(root)/name
        e.require(p.is_file() and sha_file(p)==digest,'Pinned V2 pricing/accounting engine changed: '+name)
    return ENGINE


def baseline_parity(generated,original):
    clean=[{k:v for k,v in r.items() if k!='candidate_id'} for r in generated]
    e.require(len(clean)==len(original),'Unfiltered generated baseline count does not match frozen source')
    e.require(len({s['signal_key'] for s in clean})==len(clean),'Duplicate generated baseline signals')
    for new,old in zip(clean,original):e.require(canon(new)==canon(old),'Unfiltered generated baseline differs from frozen source: '+old['signal_key'])
    return dict(status='PASS',exact_signal_records_matched=len(original),all_signal_fields_checked=True)
