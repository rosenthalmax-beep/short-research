"""Read-only source investigation. Empty candles never certify a market closure."""
import csv,io,json,sqlite3,zipfile,zlib
from collections import Counter
from pathlib import Path
import execution as e

BASELINE_SHA='906769c9aafbc3430eea44aeff25f000832cbcdb91476c1b2f01e4d3831e676a'
VERSION='P32_GAP_EVIDENCE_V1_2026_10_16'

def decode(value):
    if value=='True':return True
    if value=='False':return False
    try:return json.loads(value)
    except (ValueError,TypeError):return value

def load_baseline(path):
    e.require(e.sha(Path(path).read_bytes())==BASELINE_SHA,'Prior replay archive checksum mismatch')
    with zipfile.ZipFile(path) as z:
        for name,row in json.loads(z.read('report_manifest.json')).items():
            body=z.read(name);e.require(len(body)==row['bytes'] and e.sha(body)==row['sha256'],'Prior report manifest mismatch')
        raw=[json.loads(line) for line in z.read('raw_signal_witnesses.jsonl').splitlines()]
        rows=list(csv.DictReader(io.StringIO(z.read('candidate_price_paths.csv').decode())))
        paths=[{k:decode(v) for k,v in row.items() if v!=''} for row in rows]
        summary=json.loads(z.read('RUN_COMPLETE.json'))
    e.require(summary['raw_signals']==len(raw)==3600,'Unexpected prior signal count')
    e.require(sum(p['status']=='UNRESOLVED_PATH' for p in paths)==171,'Unexpected prior gap cohort')
    return dict(raw=raw,paths=paths,summary=summary)

def import_baseline_sources(path,store):
    """Reuse the 89 saved source blocks even if the host lost its old cache."""
    with zipfile.ZipFile(path) as z:
        for row in json.loads(z.read('new_source_candle_index.json')):
            ident={k:v for k,v in row.items() if k not in ('file','sha256','origin')}
            key=e.sha(e.canon(ident));body=z.read(row['file'])
            e.require(row['file']=='source_candles/'+key+'.json' and e.sha(body)==row['sha256'],'Prior source identity/checksum mismatch')
            e.require(ident['broker']==store.broker,'Prior source broker mismatch')
            store.validate(json.loads(body),ident)
            existing=store.db.execute('SELECT sha256 FROM blocks WHERE key=?',(key,)).fetchone()
            e.require(existing is None or existing[0]==row['sha256'],'Prior source/cache conflict')
            if existing is None:
                store.db.execute('INSERT INTO blocks VALUES(?,?,?,?,?)',(key,e.canon(ident).decode(),zlib.compress(body,6),row['sha256'],'PRIOR_ADMISSION_REPORT'))
                store.index[(ident['pair'],ident['granularity'])].append((e.dt(ident['start']),e.dt(ident['end']),key))
        store.db.commit()

def missing_windows(rows,start,end,seconds):
    available={e.dt(r['time']) for r in rows};windows=[];begin=None;t=start
    while t<end:
        missing=t not in available and not e.conventional_weekend(t)
        if missing and begin is None:begin=t
        if not missing and begin is not None:windows.append((begin,t));begin=None
        t+=e.timedelta(seconds=seconds)
    if begin is not None:windows.append((begin,end))
    return windows

def locate_windows(store,path):
    pair=path['pair'];native=path['timeframe'];entry=e.dt(path['entry_utc'])
    parent=e.dt(path['native_interval_start_utc']);parent_end=e.dt(path['native_interval_end_utc'])
    if (parent_end-parent).total_seconds()==60:native='M1'
    result=[]
    def add(gran,start,end,tier):
        if start>=end:return
        rows=store.range(pair,gran,start,end)
        result.extend(dict(tier=tier,granularity=gran,start=e.stamp(a),end=e.stamp(b),missing_slots=int((b-a).total_seconds()/e.STEP[gran]))
                      for a,b in missing_windows(rows,start,end,e.STEP[gran]))
    # The original replay counts native missing slots before its touched parent.
    add(path['timeframe'],entry,parent,'NATIVE_BEFORE_TOUCH')
    hit=e.dt(path['interval_start_utc']);minute=e.floor_time(hit,60)
    if native!='M1':add('M1',parent,minute,'M1_BEFORE_REFINED_TOUCH')
    if path['resolution']=='S5':add('S5',minute,hit,'S5_BEFORE_REFINED_TOUCH')
    return result

def investigate_window(evidence,path,window,progress=lambda **x:None):
    pair=path['pair'];gran=window['granularity'];start=e.dt(window['start']);end=e.dt(window['end'])
    finer='M1' if gran in ('H1','M15') else 'S5'
    component='bid' if path['side']=='BUY' else 'ask'
    progress(gap_query_pair=pair,gap_query_granularity=gran,gap_query_start=window['start'],gap_query_end=window['end'])
    fresh=evidence.range(pair,gran,start,end)
    lower=fresh if finer==gran else evidence.range(pair,finer,start,end)
    native_hits=[(r,e.touch(r,component,path['side'],e.dec(path['model_stop']),e.dec(path['model_target']))) for r in fresh]
    native_hits=[(r,hit) for r,hit in native_hits if hit]
    hits=[(r,e.touch(r,component,path['side'],e.dec(path['model_stop']),e.dec(path['model_target']))) for r in lower]
    hits=[(r,hit) for r,hit in hits if hit]
    remaining=missing_windows(lower,start,end,e.STEP[finer])
    peer='USD_JPY' if pair=='EUR_USD' else 'EUR_USD';peer_rows=[]
    if not fresh and not lower:
        progress(gap_query_pair=peer,gap_query_granularity=finer)
        peer_rows=evidence.range(peer,finer,start,end)
        assessment='PAIR_SPECIFIC_ABSENCE' if peer_rows else 'COINCIDENT_PAIR_ABSENCE_UNPROVEN_SCHEDULE'
    elif hits or native_hits:assessment='RECOVERED_BARRIER_CANDIDATE_REQUIRES_REPLAY'
    elif not remaining:assessment='FINER_QUOTES_COVER_GAP_WITHOUT_BARRIER_TOUCH'
    else:assessment='PARTIAL_QUOTES_REMAIN_UNCERTAIN'
    first=hits[0] if hits else native_hits[0] if native_hits else None
    return dict(window,rechecked_native_quotes=len(fresh),finer_granularity=finer,finer_quotes=len(lower),
                finer_missing_nonweekend_slots=sum(int((b-a).total_seconds()/e.STEP[finer]) for a,b in remaining),
                assessment=assessment,control_pair=peer if not fresh and not lower else None,control_pair_quotes=len(peer_rows),
                first_recovered_touch_utc=first[0]['time'] if first else None,first_recovered_touch_kind=first[1]['kind'] if first else None,
                first_recovered_touch_price=first[1]['exit_price'] if first else None,
                recovered_touch_granularity=finer if hits else gran if native_hits else None,
                native_barrier_candidate_count=len(native_hits),finer_barrier_candidate_count=len(hits),
                near_ny17=any(abs((t.astimezone(e.NY).hour*60+t.astimezone(e.NY).minute)-1020)<=30 for t in (start,end-e.timedelta(seconds=1))),
                historical_closure_verified=False,original_path_automatically_promoted=False)

def run(directory,baseline,store,reader,progress=lambda **x:None):
    work=Path(directory)/'gap_evidence';work.mkdir(parents=True,exist_ok=True)
    evidence=e.CandleStore(work,store.broker,reader)
    signature=e.sha(e.canon(dict(version=VERSION,baseline=BASELINE_SHA,code=e.sha(Path(__file__).read_bytes()),execution=e.sha((Path(__file__).parent/'execution.py').read_bytes()))))
    paths=sorted([p for p in baseline['paths'] if p['status']=='UNRESOLVED_PATH'],key=lambda p:p['signal_key'])
    records=[];windows=[];hits=0
    try:
        for i,path in enumerate(paths):
            progress(phase='INVESTIGATE_171_PRIOR_GAPS',gap_paths_completed=i,gap_paths_required=len(paths),gap_signal_key=path['signal_key'],gap_source_downloads=evidence.downloads)
            checkpoint=work/(e.sha(e.canon(dict(signature=signature,path=path)))+'.json')
            if checkpoint.exists():
                saved=json.loads(checkpoint.read_text());e.require(e.sha(e.canon(saved['payload']))==saved['sha256'],'Gap checkpoint checksum mismatch');payload=saved['payload'];hits+=1
            else:
                located=locate_windows(store,path)
                details=[dict(investigate_window(evidence,path,w,progress),signal_key=path['signal_key'],pair=path['pair'],strategy_id=path['strategy_id']) for w in located]
                payload=dict(summary=dict(signal_key=path['signal_key'],pair=path['pair'],strategy_id=path['strategy_id'],entry_utc=path['entry_utc'],
                    original_kind=path['kind'],original_status=path['status'],original_resolution=path['resolution'],
                    original_native_missing_slots=path.get('prior_native_unexplained_slots',0),original_child_gap=path.get('earlier_gap',False),
                    gap_windows=len(details),native_windows=sum(w['tier']=='NATIVE_BEFORE_TOUCH' for w in details),
                    fine_windows=sum(w['tier']!='NATIVE_BEFORE_TOUCH' for w in details),
                    assessments=dict(Counter(w['assessment'] for w in details)),
                    classification='NO_GAP_WINDOW_RECONSTRUCTED_REVIEW_REQUIRED' if not details else 'EVIDENCE_COLLECTED_UNCERTAINTY_RETAINED',
                    path_status_changed=False,historical_closure_verified=False),windows=details)
                tmp=checkpoint.with_suffix('.tmp');tmp.write_bytes(e.canon(dict(payload=payload,sha256=e.sha(e.canon(payload)))));tmp.replace(checkpoint)
            records.append(payload['summary']);windows.extend(payload['windows'])
        summary=dict(version=VERSION,prior_gap_paths=len(paths),paths_investigated=len(records),gap_windows=len(windows),checkpoint_hits=hits,
                     original_paths_with_native_gaps=sum(p.get('prior_native_unexplained_slots',0)>0 for p in paths),
                     original_finer_only_gap_paths=sum(not p.get('prior_native_unexplained_slots',0) for p in paths),
                     window_assessments=dict(Counter(w['assessment'] for w in windows)),new_evidence_blocks=evidence.downloads,
                     automatic_status_promotions=0,historical_closures_verified=0,
                     scope='SOURCE_DIAGNOSTICS_ONLY_NOT_AUTOMATIC_EXIT_REPAIR',gap_evidence_kept_separate_from_price_replay=True)
        progress(gap_paths_completed=len(paths),gap_paths_required=len(paths),gap_windows_completed=len(windows),gap_source_downloads=evidence.downloads)
        return summary,records,windows
    finally:evidence.close()

def compare(baseline,current,reconciliation):
    old={s['signal_key']:s for s in baseline['raw']};new={s['signal_key']:s for s in current};changes=[]
    fields=('reference_entry','stop','target','rr','strategy_id','entry_utc')
    for key in sorted(old.keys()|new.keys()):
        before,after=old.get(key),new.get(key)
        kind='ADDED_WITH_WARMUP' if before is None else 'REMOVED_WITH_WARMUP' if after is None else 'GEOMETRY_CHANGED' if any(before[k]!=after[k] for k in fields) else 'UNCHANGED'
        changes.append(dict(signal_key=key,strategy_id=(after or before)['strategy_id'],entry_utc=(after or before)['entry_utc'],status=kind,
                            old_geometry={k:before[k] for k in fields} if before else None,new_geometry={k:after[k] for k in fields} if after else None))
    old_missing=set()
    with zipfile.ZipFile(Path(__file__).parent/'prior_signal_admission_results.zip') as z:
        for r in csv.DictReader(io.StringIO(z.read('archived_3391_signal_reconciliation.csv').decode())):
            if r['status']=='ARCHIVED_SIGNAL_NOT_REPRODUCED':old_missing.add(int(r['index']))
    followup=[dict(r,prior_status='ARCHIVED_SIGNAL_NOT_REPRODUCED') for r in reconciliation if r['index'] in old_missing]
    return dict(warmup_signal_changes=dict(Counter(r['status'] for r in changes)),prior_missing_32_now=dict(Counter(r['status'] for r in followup)),
                warmup_is_not_parameter_optimization=True,full_portfolio_validated=False),changes,followup

def export_evidence(directory,archive):
    folder=Path(directory)/'gap_evidence'
    if not (folder/'replay_candles.sqlite3').exists():return
    store=e.CandleStore(folder,'https://api-fxtrade.oanda.com',reader=None)
    class Prefix:
        def writestr(self,name,body):archive.writestr('gap_evidence/'+name,body)
    try:store.export(Prefix())
    finally:store.close()
