"""EURCHF M15 SHORT — Pass2 one conditional feature at a time,2026-10-08.
Python3.10+ standard library. Keep existing EURCHF_M15_PASS1_FROZEN_DATA.zip
alongside this script. No new variables,broker requests,accounts or orders.
TwofixedB references;56single-filter candidates;62configs/372model-cost cases.
RR3 and1/2/4pip assumed entry penalties unchanged. Near22year source/cutoff fixed.
Run python short_strategy_research.py (HTTP+autostart), --run (CLI), --self-test.
WSGI app exposed. PORT8080default. /status and /results. Oneworker recommended.
Allsix archived controls/independentlaggedfeatures/source gates must pass first.
No new-grid historical market outcomes have been run before delivery.
"""
from __future__ import annotations

import argparse
import base64
import bisect
import csv
import hashlib
import io
import json
import math
import os
import shutil
import statistics
import sys
import tempfile
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import zipfile
import zlib
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from socketserver import ThreadingMixIn
from wsgiref.simple_server import WSGIServer, make_server

VERSION = 'EURCHF_M15_SHORT_PASS2_CONDITIONAL_FEATURES_V1_2026_10_08'
PAIR, SIDE, TIMEFRAME = 'EUR_CHF', 'SELL', 'M15'
UTC = timezone.utc
START = datetime(2005, 1, 1, tzinfo=UTC)
END = datetime(2026, 10, 8, tzinfo=UTC)  # exclusive entry cutoff, frozen
BAR = timedelta(minutes=15)
H1_BAR = timedelta(hours=1)
PINNED_H1_END = datetime(2026, 10, 1, tzinfo=UTC)
PINNED_H1_COUNT = 137819
TICK, PIP, RR, WARMUP = 0.00001, 0.0001, 3.0, 200
COSTS = (10, 20, 40)
MODELS = ('NEAREST_OPEN_SENSITIVITY', 'STOP_FIRST_GAP_STRESS')
LOOKBACKS = (40, 60, 80, 100, 150, 200, 250, 300)
DISTANCES = (0.05, 0.075, 0.10, 0.15, 0.20)
BODIES = (0.75, 1.00, 1.25, 1.50, 1.75)
RANGES = (0.0, 1.00, 1.25, 1.50, 1.75)

PARENT_IDS = ('RAW_ENGULF', 'BASELINE_CONTROL', 'M_L200_D010_B075_R125', 'M_L200_D010_B125_R000', 'M_L060_D010_B125_R150', 'B_STRUCTURE_L200_D0075_B125')
FAMILIES = {}

ANCHORS = {'MAIN':'M_L200_D010_B125_R000','TIGHT':'B_STRUCTURE_L200_D0075_B125'}
MOMENTUM_WINDOWS=(48,96,192)
MOMENTUM_MINIMA=(0.0,0.5,1.0,1.5)
VOL_WINDOWS=(96,192)
VOL_LEVELS=(0.75,1.0,1.25,1.5)
FAMILIES={f'{branch}_{kind}':dict(axes={'feature_window':MOMENTUM_WINDOWS if kind=='MOMENTUM_UP' else VOL_WINDOWS,
          'threshold':MOMENTUM_MINIMA if kind=='MOMENTUM_UP' else VOL_LEVELS})
          for branch in ANCHORS for kind in ('MOMENTUM_UP','VOL_MIN','VOL_MAX')}

PINNED_H1_SHA256 = '9c8b5279629daee868ad924f52e998be7f6ce638a15e106ffb4851a2df76feac'
RESULT_NAME = 'EURCHF_M15_SHORT_PASS2_CONDITIONAL_FEATURES_RESULTS.zip'
OUT = Path(os.getenv('EURCHF_M15_PASS2_OUTPUT_DIR', '/tmp/eurchf_m15_short_pass2')).resolve()
PREFIX = '/eurchf-m15-short-pass2'
LOCK = threading.RLock()
STARTED = False
JOB_LOCK = None
RUN_CLOCK = None
STATUS = dict(state='not_started', progress=0, message='Ready', version=VERSION,
              orders_supported=False, trading_enabled=False, pair=PAIR)


def iso(value):
    return value.astimezone(UTC).isoformat().replace('+00:00', 'Z')


def when(value):
    return value if isinstance(value, datetime) else datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(UTC)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def file_sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024*1024), b''):
            digest.update(block)
    return digest.hexdigest()


def code_hash():
    return file_sha(Path(__file__))


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n', encoding='utf-8')


class CSVFile:
    """Streaming outputs keep memory bounded for full configuration ledgers."""
    def __init__(self, path, fields):
        self.file = path.open('w', newline='', encoding='utf-8')
        self.writer = csv.DictWriter(self.file, fieldnames=fields, extrasaction='raise')
        self.writer.writeheader()
        self.count = 0

    def add(self, row):
        self.writer.writerow(row)
        self.count += 1

    def close(self):
        self.file.close()


def write_csv(path, rows, fields=None):
    """Stream rows; never materialize the full multi-decade source or ledgers."""
    iterator = iter(rows)
    first = next(iterator, None)
    if fields is None:
        fields = list(first) if first is not None else []
    sink = CSVFile(path, fields)
    try:
        if first is not None:
            sink.add(first)
        for row in iterator:
            sink.add(row)
    finally:
        sink.close()


def set_status(**values):
    with LOCK:
        STATUS.update(values, updated_at=iso(datetime.now(UTC)))
        if RUN_CLOCK is not None:
            STATUS['elapsed_seconds'] = round(time.monotonic() - RUN_CLOCK, 1)
        OUT.mkdir(parents=True, exist_ok=True)
        pending = OUT / f'.status-{os.getpid()}.tmp'
        write_json(pending, STATUS)
        os.replace(pending, OUT / 'status.json')
    print(json.dumps({k: STATUS[k] for k in ('state', 'progress', 'message')}), flush=True)


def read_status():
    try:
        data = json.loads((OUT / 'status.json').read_text())
        if data.get('version') == VERSION:
            return data
    except (OSError, ValueError):
        pass
    with LOCK:
        return dict(STATUS)


def config_key(config):
    return tuple(config.get(k) for k in ('lookback','distance_atr','body_min_atr','range_min_atr','close_max',
                                         'feature_family','feature_window','threshold'))


def make_configs():
    configs,by_id,memberships=[],{},[]
    def add(cid,role,lookback=0,distance=None,body=0.,candle_range=0.,anchor_id=None,feature_family=None,feature_window=0,threshold=None):
        row=dict(config_id=cid,lookback=lookback,distance_atr=distance,body_min_atr=body,range_min_atr=candle_range,
                 close_max=None,anchor_id=anchor_id,feature_family=feature_family,feature_window=feature_window,threshold=threshold)
        if cid not in by_id:by_id[cid]=row;configs.append(row)
        else:assert by_id[cid]==row
        memberships.append(dict(requested_label=cid,stage_group=role,config_id=cid))
    add('RAW_ENGULF','PARENT_CONTROL')
    add('BASELINE_CONTROL','PARENT_CONTROL',60,.25,.75,1.25)
    add('M_L200_D010_B075_R125','PARENT_CONTROL',200,.10,.75,1.25)
    add('M_L200_D010_B125_R000','PARENT_CONTROL',200,.10,1.25,0.)
    add('M_L060_D010_B125_R150','PARENT_CONTROL',60,.10,1.25,1.50)
    add('B_STRUCTURE_L200_D0075_B125','PARENT_CONTROL',200,.075,1.25,0.)
    for branch,cid in ANCHORS.items():
        c=by_id[cid]
        memberships.append(dict(requested_label=branch+'_UNFILTERED',stage_group='ANCHOR_REFERENCE',config_id=cid))
        for kind in ('MOMENTUM_UP','VOL_MIN','VOL_MAX'):
            windows=MOMENTUM_WINDOWS if kind=='MOMENTUM_UP' else VOL_WINDOWS
            levels=MOMENTUM_MINIMA if kind=='MOMENTUM_UP' else VOL_LEVELS
            for window in windows:
                for threshold in levels:
                    label=f'{branch}_{kind}_W{window:03d}_T{round(threshold*100):03d}'
                    add(label,branch+'_'+kind,c['lookback'],c['distance_atr'],c['body_min_atr'],c['range_min_atr'],
                        cid,kind,window,threshold)
    assert len(configs)==62 and len(memberships)==64
    assert len({config_key(c) for c in configs})==62
    return configs,memberships


def atr14(bars):
    out = [None] * len(bars)
    tr = [None] * len(bars)
    for i in range(1, len(bars)):
        tr[i] = max(bars[i][2] - bars[i][3], abs(bars[i][2] - bars[i-1][4]), abs(bars[i][3] - bars[i-1][4]))
    if len(bars) > 14:
        out[14] = sum(tr[1:15]) / 14
        for i in range(15, len(bars)):
            out[i] = (out[i-1] * 13 + tr[i]) / 14
    return out


def previous_high(highs, lookback):
    """Monotonic queue; excludes the current signal candle."""
    out, queue = [None] * len(highs), deque()
    for i in range(len(highs)):
        while queue and queue[0] < i - lookback:
            queue.popleft()
        if i >= lookback:
            out[i] = highs[queue[0]]
        while queue and highs[queue[-1]] <= highs[i]:
            queue.pop()
        queue.append(i)
    return out


def bearish_engulf(bars, i):
    if i < 1:
        return False
    po, pc, op, cl = bars[i-1][1], bars[i-1][4], bars[i][1], bars[i][4]
    return pc > po and cl < op and op >= pc and cl <= po


def make_base_features(bars):
    atr = atr14(bars)
    prev = {lb: previous_high([b[2] for b in bars], lb) for lb in LOOKBACKS}
    raw = {}
    for i in range(WARMUP, len(bars)):
        if not bearish_engulf(bars, i) or atr[i] is None or atr[i] <= 0:
            continue
        op, hi, lo, cl = bars[i][1:]
        row = dict(signal_index=i, signal=iso(bars[i][0]), entry=iso(bars[i][0] + BAR),
                   atr14=atr[i], body_atr=abs(cl-op)/atr[i], range_atr=(hi-lo)/atr[i],
                   close_location=(cl-lo)/(hi-lo), reference_stop_pips=(hi + 10*TICK - cl)/PIP)
        for lb in LOOKBACKS:
            row[f'previous_high_{lb}'] = prev[lb][i]
            row[f'signed_distance_atr_{lb}'] = None if prev[lb][i] is None else (hi-prev[lb][i])/atr[i]
            row[f'abs_distance_atr_{lb}'] = None if prev[lb][i] is None else abs(hi-prev[lb][i])/atr[i]
        raw[i] = row
    return raw


def make_features(bars):
    raw=make_base_features(bars);atr=atr14(bars)
    for i,row in raw.items():
        row['prior_atr14']=atr[i-1]
        for window in MOMENTUM_WINDOWS:
            row[f'preceding_rise_{window}_prior_atr']=(bars[i-1][4]-bars[i-1-window][4])/atr[i-1] if i-1-window>=0 and atr[i-1] is not None and atr[i-1]>0 else None
        for window in VOL_WINDOWS:
            mean=None
            if i-window-1>=14:
                history=atr[i-window-1:i-1]
                if len(history)==window and all(v is not None and v>0 for v in history):mean=math.fsum(history)/window
            row[f'prior_atr_mean_{window}']=mean
            row[f'prior_atr_ratio_{window}']=atr[i-1]/mean if mean is not None and mean>0 else None
    return raw


def base_selected_indices(config, features):
    result = []
    for i, row in features.items():
        if row['body_atr'] < config['body_min_atr'] or row['range_atr'] < config['range_min_atr']:
            continue
        if config['lookback'] and (row[f"abs_distance_atr_{config['lookback']}"] is None or row[f"abs_distance_atr_{config['lookback']}"] > config['distance_atr']):
            continue
        if config['close_max'] is not None and row['close_location'] > config['close_max']:
            continue
        result.append(i)
    return result


def selected_indices(config,features):
    indices=base_selected_indices(config,features);kind=config.get('feature_family')
    if kind is None:return indices
    window=config['feature_window'];threshold=config['threshold']
    if kind not in ('MOMENTUM_UP','VOL_MIN','VOL_MAX'):raise RuntimeError('Unsupported conditional feature family')
    key=f'preceding_rise_{window}_prior_atr' if kind=='MOMENTUM_UP' else f'prior_atr_ratio_{window}'
    return [i for i in indices if features[i][key] is not None and
            (features[i][key]<=threshold if kind=='VOL_MAX' else features[i][key]>=threshold)]


def find_paths(bars, i):
    """Fixed stop/target first-touch is reusable; eligibility is replayed per cost."""
    reference = bars[i][4]
    stop = bars[i][2] + 10*TICK
    target = reference - RR*(stop-reference)
    common = dict(signal_index=i, signal=iso(bars[i][0]), entry=iso(bars[i][0]+BAR),
                  reference_entry=reference, stop=stop, target=target,
                  next_open=bars[i+1][1] if i+1 < len(bars) else None,
                  next_candle_delay_hours=(bars[i+1][0]-bars[i][0]-BAR).total_seconds()/3600 if i+1 < len(bars) else None)
    for j in range(i+1, len(bars)):
        op, hi, lo = bars[j][1:4]
        hit_stop, hit_target = hi >= stop, lo <= target
        if not hit_stop and not hit_target:
            continue
        both = hit_stop and hit_target
        if both:
            legacy_reason = 'TARGET' if (op-lo) < (hi-op) else 'STOP'
        else:
            legacy_reason = 'STOP' if hit_stop else 'TARGET'
        legacy = dict(common, exit_index=j, exit=iso(bars[j][0]+BAR),
                      exit_price=stop if legacy_reason == 'STOP' else target,
                      reason=legacy_reason, dual_touch=int(both), gap_stop=0, gap_target=0)
        # Opening prices are observed MID prints, not guaranteed broker fills.
        if op >= stop:
            reason, price, exit_time = 'STOP_GAP' if op > stop else 'STOP', max(op, stop), bars[j][0]
        elif op <= target:
            reason, price, exit_time = 'TARGET_GAP_CAPPED', target, bars[j][0]
        elif hit_stop:
            reason, price, exit_time = 'STOP', stop, bars[j][0]+BAR
        else:
            reason, price, exit_time = 'TARGET', target, bars[j][0]+BAR
        stress = dict(common, exit_index=j, exit=iso(exit_time), exit_price=price, reason=reason,
                      dual_touch=int(both), gap_stop=int(op > stop), gap_target=int(op <= target))
        return {'NEAREST_OPEN_SENSITIVITY': legacy, 'STOP_FIRST_GAP_STRESS': stress}
    opened = dict(common, exit_index=None, exit=None, exit_price=None,
                  reason='OPEN_AT_DATA_END', dual_touch=0, gap_stop=0, gap_target=0)
    return {model: dict(opened) for model in MODELS}


def geometry(path, cost):
    fill = path['reference_entry'] - cost*TICK
    if when(path['entry']) >= END:
        return None, 'ENTRY_AT_OR_AFTER_CUTOFF'
    if not 0 < path['target'] < fill < path['stop']:
        return None, 'INVALID_FILL_STOP_TARGET_GEOMETRY'
    return fill, None


def r_value(path, cost):
    fill, problem = geometry(path, cost)
    if problem or path['exit'] is None:
        return None
    if path['reason'] == 'STOP':
        return -1.0  # preserve archive exact -1 convention
    return (fill-path['exit_price'])/(path['stop']-fill)


def replay(indices, paths, model, cost):
    """p0 per exact geometry and assumed cost, including unresolved positions."""
    accepted, invalid, blocked = [], [], 0
    occupied_until = -1
    for i in indices:
        if i < occupied_until:
            blocked += 1
            continue
        path = paths[i][model]
        _, problem = geometry(path, cost)
        if problem:
            invalid.append((i, problem))
            continue
        accepted.append(i)
        occupied_until = path['exit_index'] if path['exit_index'] is not None else math.inf
    return accepted, invalid, blocked


def source_hash(bars):
    return sha('\n'.join(f'{iso(t)},{op:.5f},{hi:.5f},{lo:.5f},{cl:.5f}' for t,op,hi,lo,cl in bars).encode())


def signal_hash(indices, bars):
    return sha('\n'.join(iso(bars[i][0]) for i in indices).encode())


def load_base_frozen_history(work):
    if not FROZEN_SOURCE_FILE.is_file():
        raise RuntimeError('Missing EURCHF_M15_PASS1_FROZEN_DATA.zip. Upload this file alongside app.py in the same folder.')
    payload=FROZEN_SOURCE_FILE.read_bytes()
    if sha(payload)!=EMBEDDED_PAYLOAD_SHA256:
        raise RuntimeError('Frozen source archive SHA mismatch.')
    shutil.copyfile(FROZEN_SOURCE_FILE,work/'EURCHF_M15_PASS1_FROZEN_DATA.zip')
    with zipfile.ZipFile(io.BytesIO(payload)) as bundle:
        if set(bundle.namelist())!=set(EMBEDDED_MEMBER_PINS) or bundle.testzip() is not None:
            raise RuntimeError('Frozen source archive members/CRC mismatch.')
        for name,pin in EMBEDDED_MEMBER_PINS.items():
            data=bundle.read(name)
            if len(data)!=pin['bytes'] or sha(data)!=pin['sha256']:
                raise RuntimeError('Frozen source member SHA/size mismatch: '+name)
            destination='archived_pass1_fetch_receipts.csv' if name=='fetch_receipts.csv' else name
            (work/destination).write_bytes(data)
    def read_source(name,step):
        bars,volumes=[],{}
        with (work/name).open(newline='') as stream:
            for row in csv.DictReader(stream):
                t=when(row['time']);v=int(row['volume'])
                if v<0:raise RuntimeError('Negative archived price count.')
                bars.append((t,*[float(row[k]) for k in ('open','high','low','close')]))
                volumes[t]=v
        validate_bars(bars,step)
        return bars,volumes
    bars,volumes=read_source('source_candles.csv',BAR)
    h1,hvolumes=read_source('source_h1_crosscheck_candles.csv',H1_BAR)
    if len(bars)!=546647 or source_hash(bars)!='63eeea194c7106cfe0ebe248e96a392ab876611da5fc2fb569e3b2f304eaaa22':
        raise RuntimeError('Pinned Pass 1 M15 source mismatch.')
    if len(h1)!=137939 or source_hash(h1)!='b3b686ad3a92569d971d2d9412c8afe64a5ad8f54b5d097840c4a2d6cebbd2c0':
        raise RuntimeError('Pinned Pass 1 H1 source mismatch.')
    return bars,volumes,h1,hvolumes


def load_frozen_history(work):
    bars,volumes,h1,hvolumes=load_base_frozen_history(work)
    reference=zlib.decompress(base64.b85decode(TIGHT_REFERENCE_B85.encode()))
    if sha(reference)!=TIGHT_REFERENCE_SHA256:raise RuntimeError('TIGHT archived reference checksum failed')
    data=json.loads(reference)
    if len(data['rows'])!=480 or len(data['summaries'])!=6:raise RuntimeError('Incomplete TIGHT archived reference')
    ledger=work/'parent_pass1_accepted_ledgers.csv'
    with ledger.open(newline='') as source:fields=next(csv.reader(source))
    with ledger.open('a',newline='') as destination:
        writer=csv.DictWriter(destination,fieldnames=fields)
        for row in data['rows']:writer.writerow({k:row[k] for k in fields})
    provenance=json.loads((work/'parent_pass1_provenance.json').read_text())
    provenance['parent_summaries'].extend(data['summaries'])
    provenance['pass1b_tight_reference_source_zip_sha256']=data['source_zip_sha256']
    provenance['pass1b_tight_reference_source_version']=data['source_version']
    provenance['pass1b_tight_reference_sha256']=TIGHT_REFERENCE_SHA256
    provenance['combined_parent_accepted_rows']=115180
    write_json(work/'parent_pass1_provenance.json',provenance)
    write_json(work/'pass1b_tight_reference.json',data)
    return bars,volumes,h1,hvolumes


def archived_parent_parity(work,bars,features,paths,configs):
    """Match every accepted parent field to the actually returned Pass 1 archive."""
    expected=defaultdict(list)
    with (work/'parent_pass1_accepted_ledgers.csv').open(newline='') as source:
        for row in csv.DictReader(source):
            expected[(row['config_id'],row['execution_model'],int(row['cost_ticks']))].append(row)
    checks=[]
    provenance=json.loads((work/'parent_pass1_provenance.json').read_text())
    parent_summaries={(r['config_id'],r['execution_model'],int(r['cost_ticks'])):r for r in provenance['parent_summaries']}
    integer={'cost_ticks','accepted_sequence','signal_index','exit_index'}
    numeric={'reference_entry','historical_fill','stop','target','risk_price','exit_price','r'}
    expected_keys={(cid,model,cost) for cid in PARENT_IDS for model in MODELS for cost in COSTS}
    if not set(expected)<=expected_keys or set(parent_summaries)!=expected_keys:
        raise RuntimeError('Incomplete parent case archive.')
    for config in configs:
        cid=config['config_id']
        if cid not in PARENT_IDS:continue
        indices=selected_indices(config,features)
        for model in MODELS:
            for cost in COSTS:
                key=(cid,model,cost);old=expected[key]
                archived_count=int(parent_summaries[key]['closed_trades'])+int(parent_summaries[key]['open_at_data_end'])
                if len(old)!=archived_count:
                    raise RuntimeError('Incomplete parent accepted row archive for '+str(key))
                accepted,_,_=replay(indices,paths,model,cost)
                mismatches=abs(len(accepted)-len(old))
                for seq,(i,ref) in enumerate(zip(accepted,old),1):
                    p=paths[i][model];fill=geometry(p,cost)[0]
                    actual=dict(config_id=cid,execution_model=model,cost_ticks=cost,accepted_sequence=seq,
                        signal_index=i,exit_index=p['exit_index'],signal=p['signal'],entry=p['entry'],exit=p['exit'],
                        reference_entry=p['reference_entry'],historical_fill=fill,stop=p['stop'],target=p['target'],
                        risk_price=p['stop']-fill,exit_price=p['exit_price'],reason=p['reason'],r=r_value(p,cost))
                    if set(actual)!=set(ref):mismatches+=1;continue
                    for field,value in actual.items():
                        archived=ref[field]
                        if value is None:mismatches+=archived!=''
                        elif field in integer:mismatches+=int(archived)!=value
                        elif field in numeric:mismatches+=not archived or abs(float(archived)-value)>1e-10
                        else:mismatches+=str(value)!=archived
                checks.append(dict(config_id=cid,execution_model=model,cost_ticks=cost,
                    check='complete_archived_accepted_ledger_fields',status='PASS' if mismatches==0 else 'FAIL',
                    actual=mismatches,expected=0,reference_accepted=len(old)))
                raw_match=signal_hash(indices,bars)==parent_summaries[key]['raw_signal_sha256']
                checks.append(dict(config_id=cid,execution_model=model,cost_ticks=cost,
                    check='archived_qualifying_signal_fingerprint',status='PASS' if raw_match else 'FAIL',
                    actual=raw_match,expected=True,reference_accepted=len(old)))
    write_csv(work/'parent_pass1_parity.csv',checks)
    if any(r['status']!='PASS' for r in checks):
        raise RuntimeError('Pass 1 complete parent ledger/signal parity failed; no new-grid results are valid.')
    return checks


def validate_bars(bars, step=BAR):
    if not bars:
        raise RuntimeError('Empty dataset.')
    seconds=int(step.total_seconds())
    for i,(t,op,hi,lo,cl) in enumerate(bars):
        if t.tzinfo is None or t.second or t.microsecond or int(t.timestamp()) % seconds:
            raise RuntimeError('Candle timestamp is not aligned to the requested granularity.')
        if not all(math.isfinite(x) and x > 0 for x in (op,hi,lo,cl)) or not lo <= min(op,cl) <= max(op,cl) <= hi:
            raise RuntimeError('Invalid OHLC at '+iso(t))
        if i and t <= bars[i-1][0]:
            raise RuntimeError('Unsorted or duplicate candle timestamp.')
        if not START <= t < END:
            raise RuntimeError('Candle outside the frozen study window.')


def crosscheck_history(work, bars, volumes, h1, h1_volumes):
    """All observed H1 bars must match grouped native M15 OHLC and price counts.

    A sparse M15 hour is accepted only when its observed bars exhaust the H1
    price count and reproduce its OHLC. No missing candle is manufactured.
    """
    grouped=defaultdict(list)
    for bar in bars:
        grouped[bar[0].replace(minute=0)].append(bar)
    controls=[]
    def check(name,actual,expected):
        controls.append(dict(check=name,status='PASS' if actual==expected else 'FAIL',actual=actual,expected=expected))
    old=[bar for bar in h1 if bar[0]<PINNED_H1_END]
    check('historical_H1_count_matches_prior_research',len(old),PINNED_H1_COUNT)
    check('historical_H1_OHLC_hash_matches_prior_research',source_hash(old),PINNED_H1_SHA256)
    h1_times={b[0] for b in h1}
    check('native_M15_hour_set_equals_H1_hour_set',set(grouped)==h1_times,True)
    mismatch,sparse=0,0
    out=CSVFile(work/'h1_m15_crosschecks.csv',['hour','m15_bars','m15_price_count','h1_price_count','max_ohlc_error_ticks','status'])
    try:
        for t,op,hi,lo,cl in h1:
            rows=grouped.get(t,[])
            if not rows:
                count,error=0,None
                okay=False
            else:
                aggregated=(rows[0][1],max(r[2] for r in rows),min(r[3] for r in rows),rows[-1][4])
                error=max(abs(x-y)/TICK for x,y in zip(aggregated,(op,hi,lo,cl)))
                count=sum(volumes[r[0]] for r in rows)
                okay=len(rows)<=4 and error<=1e-6 and count==h1_volumes[t]
            mismatch+=not okay
            sparse+=bool(rows) and len(rows)<4
            out.add(dict(hour=iso(t),m15_bars=len(rows),m15_price_count=count,h1_price_count=h1_volumes[t],
                         max_ohlc_error_ticks=error,status='PASS_NATIVE_SPARSE_PRICE_INTERVALS' if okay and len(rows)<4 else ('PASS' if okay else 'FAIL')))
    finally:
        out.close()
    check('all_H1_native_M15_aggregation_mismatches',mismatch,0)
    check('first_M15_in_first_requested_week',START <= bars[0][0]<START+timedelta(days=7),True)
    check('last_M15_in_final_requested_day',END-timedelta(days=1)<=bars[-1][0]<END,True)
    annual=[]
    for y in range(START.year,END.year+1):
        subset=[b for b in bars if b[0].year==y]
        annual.append(dict(year=y,candles=len(subset),first=iso(subset[0][0]) if subset else None,
                           last=iso(subset[-1][0]) if subset else None,partial_year=int(y==END.year),
                           first_month=subset[0][0].month if subset else None,last_month=subset[-1][0].month if subset else None))
    write_csv(work/'yearly_data_coverage.csv',annual)
    check('every_requested_year_contains_candles',all(r['candles']>0 for r in annual),True)
    check('complete_years_reach_January_and_December',all(r['first_month']==1 and r['last_month']==12 for r in annual if not r['partial_year']),True)
    write_csv(work/'source_controls.csv',controls)
    write_json(work/'coverage_crosscheck_summary.json',dict(h1_hours=len(h1),native_m15_candles=len(bars),
               aggregation_mismatches=mismatch,sparse_hours_verified_by_price_count=sparse,synthetic_candles=0,
               h1_source_sha256=source_hash(h1),native_m15_source_sha256=source_hash(bars)))
    if any(row['status']!='PASS' for row in controls):
        raise RuntimeError('Full-history source/aggregation controls failed; inspect source_controls.csv and crosschecks. No discovery results are valid.')
    return controls


def hard_controls(bars, features, paths, configs):
    """Independent signal/ATR/ledger arithmetic on predeclared controls.

    Archived EURCHF M15 Pass 1 controls now exist. These are implementation controls
    on the same source, not fresh out-of-sample confirmation of a strategy.
    """
    rows=[]
    def check(name,actual,expected):
        rows.append(dict(check=name,status='PASS' if actual==expected else 'FAIL',actual=actual,expected=expected))
    check('configuration_count',len(configs),62)
    check('configuration_keys_unique',len({config_key(c) for c in configs}),62)
    check('complete_years_have_observed_candles',all(any(b[0].year==y for b in bars) for y in range(2005,2027)),True)
    check('first_candle_in_first_requested_week',START <= bars[0][0] < START+timedelta(days=7),True)
    check('last_candle_in_final_requested_day',END-timedelta(days=1) <= bars[-1][0] < END,True)
    # Different implementation: seed/recurrence and direct slices, without queues.
    atr_ref=[None]*len(bars)
    true_ranges=[max(bars[k][2]-bars[k][3],abs(bars[k][2]-bars[k-1][4]),abs(bars[k][3]-bars[k-1][4])) for k in range(1,len(bars))]
    if len(bars)>14:
        v=math.fsum(true_ranges[:14])/14
        atr_ref[14]=v
        for k in range(15,len(bars)):
            v += (true_ranges[k-1]-v)/14
            atr_ref[k]=v
    atr_prod=atr14(bars)
    error=max((abs(a-b) for a,b in zip(atr_ref,atr_prod) if a is not None),default=0)
    check('independent_ATR_absolute_error_le_1e12',error<=1e-12,True)
    raw_ref=[k for k in range(WARMUP,len(bars)) if bars[k-1][4]>bars[k-1][1] and bars[k][4]<bars[k][1]
             and bars[k][1]>=bars[k-1][4] and bars[k][4]<=bars[k-1][1] and atr_ref[k]>0]
    check('independent_all_raw_engulf_indices',list(features)==raw_ref,True)
    maximum_feature_error=0.
    maximum_normalized_distance_error=0.
    for k in raw_ref:
        high,low,op,cl=bars[k][2],bars[k][3],bars[k][1],bars[k][4]
        for lb in LOOKBACKS:
            if k<lb:
                check(f'unavailable_prior_high_{k}_{lb}_blank',features[k][f'previous_high_{lb}'] is None,True)
                continue
            previous=max(b[2] for b in bars[k-lb:k])
            maximum_feature_error=max(maximum_feature_error,abs(previous-features[k][f'previous_high_{lb}']))
            signed=(high-previous)/atr_ref[k]
            maximum_normalized_distance_error=max(maximum_normalized_distance_error,
                abs(signed-features[k][f'signed_distance_atr_{lb}']),
                abs(abs(signed)-features[k][f'abs_distance_atr_{lb}']))
    check('all_prior_high_features_match_direct_slices',maximum_feature_error==0,True)
    check('all_signed_absolute_distances_match_independent_ATR_and_slices',maximum_normalized_distance_error<=1e-10,True)
    for cid in PARENT_IDS:
        config=next(c for c in configs if c['config_id']==cid)
        indices=[]
        for k in raw_ref:
            atr=atr_ref[k]
            op,hi,lo,cl=bars[k][1:]
            if abs(cl-op)/atr < config['body_min_atr'] or (hi-lo)/atr < config['range_min_atr']:
                continue
            if config['lookback'] and (k<config['lookback'] or abs(hi-max(b[2] for b in bars[k-config['lookback']:k]))/atr > config['distance_atr']):
                continue
            if config['close_max'] is not None and (cl-lo)/(hi-lo)>config['close_max']:
                continue
            indices.append(k)
        check(cid+'_independent_qualifying_indices',selected_indices(config,features)==indices,True)
        for model in MODELS:
            for cost in COSTS:
                # Direct chronological reference: scan bars for each eligible entry.
                # Never consumes production cached paths or its accepted ledger.
                ref=[]
                busy=-1
                for k in indices:
                    if k < busy:
                        continue
                    ref_entry=bars[k][4]
                    stop=bars[k][2]+.00010
                    target=ref_entry-3*(stop-ref_entry)
                    fill=ref_entry-cost*.00001
                    if bars[k][0]+BAR>=END or not 0<target<fill<stop:
                        continue
                    exit_index,exit_price,reason,exit_time=None,None,'OPEN_AT_DATA_END',None
                    for j in range(k+1,len(bars)):
                        op,hi,lo=bars[j][1:4]
                        if hi<stop and lo>target:
                            continue
                        exit_index=j
                        if model=='NEAREST_OPEN_SENSITIVITY':
                            lose=hi>=stop and (lo>target or hi-op<=op-lo)
                            reason='STOP' if lose else 'TARGET'
                            exit_price=stop if lose else target
                            exit_time=bars[j][0]+BAR
                        elif op>=stop:
                            exit_price=op
                            reason='STOP_GAP' if op>stop else 'STOP'
                            exit_time=bars[j][0]
                        elif op<=target:
                            exit_price=target
                            reason='TARGET_GAP_CAPPED'
                            exit_time=bars[j][0]
                        elif hi>=stop:
                            exit_price,reason,exit_time=stop,'STOP',bars[j][0]+BAR
                        else:
                            exit_price,reason,exit_time=target,'TARGET',bars[j][0]+BAR
                        break
                    rr=None if exit_index is None else (-1. if reason=='STOP' else (fill-exit_price)/(stop-fill))
                    ref.append((k,exit_index,iso(exit_time) if exit_time else None,reason,fill,stop,target,exit_price,rr))
                    busy=exit_index if exit_index is not None else math.inf
                accepted,invalid,blocked=replay(indices,paths,model,cost)
                actual=[]
                for k in accepted:
                    p=paths[k][model]
                    actual.append((k,p['exit_index'],p['exit'],p['reason'],geometry(p,cost)[0],p['stop'],p['target'],p['exit_price'],r_value(p,cost)))
                mismatch=abs(len(actual)-len(ref))
                for a,b in zip(actual,ref):
                    mismatch+=sum(x!=y for x,y in zip(a[:4],b[:4]))
                    mismatch+=sum((x is None)!=(y is None) or (x is not None and y is not None and abs(x-y)>1e-10) for x,y in zip(a[4:],b[4:]))
                check(f'{cid}_{model}_{cost}T_complete_ledger_mismatches',mismatch,0)
    return rows


def conditional_feature_controls(bars,features):
    atr=[None]*len(bars)
    tr=[max(bars[j][2]-bars[j][3],abs(bars[j][2]-bars[j-1][4]),abs(bars[j][3]-bars[j-1][4])) for j in range(1,len(bars))]
    if len(bars)>14:
        atr[14]=math.fsum(tr[:14])/14
        for j in range(15,len(bars)):atr[j]=atr[j-1]+(tr[j-1]-atr[j-1])/14
    maxima=defaultdict(float);missing=defaultdict(int)
    for i,row in features.items():
        for window in MOMENTUM_WINDOWS:
            expected=(bars[i-1][4]-bars[i-1-window][4])/atr[i-1] if i-1-window>=0 and atr[i-1] is not None and atr[i-1]>0 else None
            actual=row[f'preceding_rise_{window}_prior_atr']
            if (expected is None)!=(actual is None):missing[f'MOMENTUM_{window}']+=1
            elif expected is not None:maxima[f'MOMENTUM_{window}']=max(maxima[f'MOMENTUM_{window}'],abs(expected-actual))
        for window in VOL_WINDOWS:
            expected=None
            if i-window-1>=14:
                history=[atr[j] for j in range(i-window-1,i-1)]
                if all(v is not None and v>0 for v in history):expected=atr[i-1]/(sum(history)/window)
            actual=row[f'prior_atr_ratio_{window}']
            if (expected is None)!=(actual is None):missing[f'VOL_{window}']+=1
            elif expected is not None:maxima[f'VOL_{window}']=max(maxima[f'VOL_{window}'],abs(expected-actual))
    rows=[]
    for label in [f'MOMENTUM_{w}' for w in MOMENTUM_WINDOWS]+[f'VOL_{w}' for w in VOL_WINDOWS]:
        okay=missing[label]==0 and maxima[label]<=1e-10
        rows.append(dict(check=label+'_independent_lagged_feature_values',status='PASS' if okay else 'FAIL',
                         missing_value_mismatches=missing[label],maximum_absolute_error=maxima[label],tolerance=1e-10))
    return rows


def stats(values):
    values = list(values)
    wins = [x for x in values if x > 0]
    losses = [x for x in values if x < 0]
    total = peak = dd = 0.
    streak = worst_streak = 0
    for x in values:
        total += x
        peak = max(peak,total)
        dd = min(dd,total-peak)
        streak = streak+1 if x < 0 else 0
        worst_streak = max(worst_streak,streak)
    return dict(closed_trades=len(values), wins=len(wins), losses=len(losses),
                win_rate_pct=100*len(wins)/len(values) if values else None,
                total_r=total, expectancy_r=total/len(values) if values else None,
                profit_factor=sum(wins)/-sum(losses) if losses else None,
                max_closed_dd_r=dd, max_losing_streak=worst_streak)


def quantile(values, fraction):
    if not values:
        return None
    values = sorted(values)
    index = (len(values)-1)*fraction
    lo, hi = math.floor(index), math.ceil(index)
    return values[lo] + (values[hi]-values[lo])*(index-lo)


def month_bounds():
    result = []
    year, month = START.year, START.month
    while datetime(year,month,1,tzinfo=UTC) <= END:
        result.append(datetime(year,month,1,tzinfo=UTC))
        year,month = (year+1,1) if month==12 else (year,month+1)
    if result[-1] < END:
        result.append(END)  # explicit partial final month, never relabelled a full month
    return result


BOUNDS = month_bounds()


def period_definitions():
    periods = []
    for year in range(2005,2027):
        periods.append((f'YEAR_{year}', 'calendar', datetime(year,1,1,tzinfo=UTC), min(datetime(year+1,1,1,tzinfo=UTC),END)))
    for a,b in ((2005,2010),(2010,2016),(2016,2022),(2022,2027)):
        periods.append((f'ERA_{a}_{b-1}', 'era', datetime(a,1,1,tzinfo=UTC),min(datetime(b,1,1,tzinfo=UTC),END)))
    for years in (1,2,3,5,10):
        periods.append((f'LATEST_{years}Y', 'recent', END.replace(year=END.year-years), END))
    # Calendar-day boundaries are diagnostic labels, not intraday announcement times.
    periods.extend([
        ('CHF_PRE_FLOOR','policy_diagnostic',START,datetime(2011,9,6,tzinfo=UTC)),
        ('CHF_FLOOR_PERIOD','policy_diagnostic',datetime(2011,9,6,tzinfo=UTC),datetime(2015,1,15,tzinfo=UTC)),
        ('CHF_2015_01_15','policy_diagnostic',datetime(2015,1,15,tzinfo=UTC),datetime(2015,1,16,tzinfo=UTC)),
        ('CHF_AFTER_2015_01_15','policy_diagnostic',datetime(2015,1,16,tzinfo=UTC),END),
    ])
    return periods


PERIODS = period_definitions()


PROTOCOL = "# EURCHF M15 SHORT — Pass 2 conditional features\nFrozen 8 October 2026 before inspecting new-grid historical outcomes.\nGuide: FOREX_STRATEGY_RESEARCH_TEMPLATE_AUDJPY_2026-09-24.md; flexible research.\n\nPass1B found a small2pip-positive B structure region but0/70positive at4pips.\nQuestion: can one causal context filter distinguish useful reversals within two\nfixed B research references? This bounded follow-up is not a final entry freeze\nor a promise to recover the failed stress. Repeated history remains exploratory.\n\n## Fixed references and predeclared design\nMAIN=M_L200_D010_B125_R000:lookback200,distance<=0.10ATR,body>=1.25ATR,no range filter.\nTIGHT=B_STRUCTURE_L200_D0075_B125:lookback200,distance<=0.075ATR,samebody/range.\nReference RR3. Signal/structure/body/stop/target/exits/costs unchanged.\nNo structure/body/range/RR/session/weekday/date optimisation or combined filters.\n\n28single-filter settings per reference,56new settings total,6archived controls:\n62unique configurations,64memberships,372model/cost cases.\n\n1.Preceding rise: windows48/96/192observed M15 bars (12/24/48market hours),\n minimum0.0/0.5/1.0/1.5priorATR. For signal indexi:\n movement_N=(close[i-1]-close[i-1-N])/ATR14[i-1].\n Signal candle is excluded from both movement and normalization. Threshold0\n requires nonnegative preceding movement; it does NOT disable the filter.\n 3windows x4thresholds=12tests per reference.\n\n2.Relative volatility: windows96/192observed ATR values (24/48market hours).\n ratio_W=ATR14[i-1]/mean(ATR14[i-W-1:i-1]). The mean contains exactly W values,\n excludes the signal candle AND the numerator's prior candle. Every baseline\n ATR must have completed its full-history seed; missing history is ineligible\n for that conditional branch, never a filled value. Same causal numerator for\n both windows. The signal ATR remains unchanged for base entry geometry.\n Test floor>=0.75/1.00/1.25/1.50 as VOL_MIN (expansion branch) and\n ceiling<=0.75/1.00/1.25/1.50 as VOL_MAX (suppression branch), separately.\n 2windows x4thresholds x2directions=16tests per reference.\n Inclusive equality; each test adds ONE condition only. Floors/ceilings are\n complementary hypotheses, not a joint interval or repeated combinations.\n\nAdjacency is within the same anchor and feature/direction family, changing one\nwindow/threshold by one frozen step. Export every edge and distinct raw-stream\ncount. Overlapping neighbour results are not independent experiments. Near-zero,\nsparse, isolated or boundary-only improvements do not establish a plateau.\n\n## Source and comparison gates\nReuse existing EURCHF_M15_PASS1_FROZEN_DATA.zip next to the Python file.\nNo new file, environment variable, token, API download/account read or order.\nThe Python code contains only a compact additional archived TIGHT reference.\nZIP SHA511215ae3b95e676829896d30baa1342bbca17e7be30745a22753dab5ba3b849.\nExact source/member pins,546647M15candles/137939H1hours, M15-H1 OHLC/price counts\nand137819pre-OctoberH1pin all required. Requested2005-01-01 to2026-10-08T00:00Z\nexclusive; actual2005-01-02T18:45Z to2026-10-07T23:45Z. Same near22years.\nM15 canonical SHA63eeea194c7106cfe0ebe248e96a392ab876611da5fc2fb569e3b2f304eaaa22;\nvolume CSV SHA59f3a83fb838d9921f9f2433cd928a77ef451301bec466a3fedd67744ae2f707.\n\nKeep original200observed-bar warmup for unfiltered controls; each conditional\nadditionally requires its own full lagged history. ATR14 uses TR1..14seed then\ncausal Wilder recurrence. Prior high excludes signal. No manufactured candles.\nAll6controls/36model-cost cases must reproduce complete archived accepted fields\nand qualifying fingerprints before conditional outcome analysis. Original30cases\ncome from the existing dataZIP;6TIGHTcases from the returned COMPLETE Pass1B.\nIndependent ATR/direct-slice/raw/control calculations and all lagged feature\nvalues are checked. Errors publish diagnostics without partial performance.\nThese are same-source software controls, not new out-of-sample evidence.\n\n## Execution unchanged\nExact bearish engulf:prevC>prevO,C<O,O>=prevC,C<=prevO;dojisreject.\nReferenceentry=completed signalclose,signalstart+15minutes. Stop=signalhigh+10ticks\n(1pip);target=close-3*(stop-close). Assumed SELL fill=close-10/20/40ticks\n(1/2/4pips),stop/targetfixed,R denominator=stop-fill. Require0<target<fill<stop.\nInvalid geometry/cutoff rejected before occupancy. Each config/model/cost gets\nits own full p0 replay. Open trade occupies through cutoff,blankR;exit-candle\nsignal may enter at that candle close. Filtering may reveal displaced later trades.\n\nPrimary STOP_FIRST_GAP_STRESS: stop opening gaps fill max(open,stop); favorable\ntarget opening gaps capped at target;both-barrier candles lose. Opening-gap exits\nuse candle start;others candle end. Alternate NEAREST_OPEN_SENSITIVITY uses the\nnearer extreme first,tie loses,barrier prices/candle end. Scan next observed bar.\nHypothetical close entry before closures is retained and next-open delays disclosed.\nMID+entry penalties omit bid/ask,ask-side short stops,financing,actual fill availability\nand intrabar jump prices. Both exit paths are assumptions,neither bounds loss.\n\n## Outputs and bounded decision\nEvery weak/empty case, config/membership, full accepted/source/path/control ledger,\nwithin-family neighbours/identical streams, full-replay adds/removals versus all\ncontrols, and an OWN-ANCHOR comparison subset with frequency/stressed-cost effects.\nCalendar/era/latest1/2/3/5/10years, blankyears and every complete12/24/36month window;\nentry-cohort eventualR differs from realized exit cash. Entry intervals[start,end),\nexit intervals(start,end]. Partial2026/October explicit;October never a complete\nrolling endpoint. Opentrades right-censored. R/closedDD additive units,notNAV% or\nfloating drawdown. CHF-event attribution includes all dates;no counterfactual deletion.\n\nReview4pip survival across neighbouring levels, counts/activity lost or displaced,\nweak eras/recent5/10years and worst rolling windows. No automatic winner. A large\nlifetime total on a sparse boundary is insufficient. Conditional research anchors\nmust earn any next limited confirmation; if stress stays negative or improvements\nare only sparse/isolated,park this tested engulfing branch. Do not combine filters\nor reopen structure/RR to repair disappointing results. Independent final ledgers,\nexact incumbent live32 admission and prospective execution remain later gates.\nNo live strategy or executor change is made or authorized by this research run.\n"


def write_inputs(work, bars, features, paths, configs, memberships, dataset_kind, volumes=None):
    write_csv(work/'source_candles.csv',
              (dict(time=iso(t),open=op,high=hi,low=lo,close=cl,volume=volumes[t] if volumes else None) for t,op,hi,lo,cl in bars))
    write_csv(work/'coverage.csv', [dict(pair=PAIR,timeframe=TIMEFRAME,price_type='MID',
              requested_start=iso(START),end_exclusive=iso(END),candles=len(bars),
              first=iso(bars[0][0]),last=iso(bars[-1][0]),warmup_bars=WARMUP,
              sha256=source_hash(bars),dataset_kind=dataset_kind)])
    write_csv(work/'data_gaps.csv',
              [dict(previous_candle=iso(a[0]),next_candle=iso(b[0]),
                    missing_wall_clock_hours=(b[0]-a[0]-BAR).total_seconds()/3600,
                    interpretation='Native source omission/closure retained; within-hour omissions verified against H1 price count; no interpolation')
               for a,b in zip(bars,bars[1:]) if b[0]-a[0] > BAR],
              ['previous_candle','next_candle','missing_wall_clock_hours','interpretation'])
    feature_fields = ['signal_index','signal','entry','atr14','body_atr','range_atr','close_location','reference_stop_pips']
    feature_fields += [k for lb in LOOKBACKS for k in (f'previous_high_{lb}',f'signed_distance_atr_{lb}',f'abs_distance_atr_{lb}')]
    feature_fields+=['prior_atr14']+[f'preceding_rise_{w}_prior_atr' for w in MOMENTUM_WINDOWS]+[k for w in VOL_WINDOWS for k in (f'prior_atr_mean_{w}',f'prior_atr_ratio_{w}')]
    write_csv(work/'raw_signal_features.csv', features.values(), feature_fields)
    write_csv(work/'configuration_grid.csv', configs)
    write_csv(work/'configuration_memberships.csv', memberships)
    path_fields = ['execution_model','signal_index','signal','entry','reference_entry','stop','target',
                   'next_open','next_candle_delay_hours','exit_index','exit','exit_price','reason','dual_touch','gap_stop','gap_target']
    write_csv(work/'signal_trade_paths.csv',
              (dict(execution_model=model,**paths[i][model]) for i in paths for model in MODELS), path_fields)
    write_csv(work/'period_definitions.csv', [dict(period=label,period_type=kind,start=iso(a),end=iso(b),
              entry_interval='[start,end)',exit_cash_interval='(start,end]',partial_calendar_year=int(kind=='calendar' and b==END and (END.month!=1 or END.day!=1)))
              for label,kind,a,b in PERIODS])


STAT_FIELDS = list(stats([]))
CASE = ['config_id','execution_model','cost_ticks']
SUMMARY_FIELDS = CASE + ['raw_signals','eligible_isolated_signals','isolated_completed',
    'isolated_open','isolated_total_r','isolated_profit_factor','geometry_invalid_all_signals',
    'p0_blocked_signals','invalid_unblocked_entries','open_at_data_end'] + STAT_FIELDS + [
    'dual_touch_closed','gap_stop_closed','gap_target_closed','entry_next_open_gap_count',
    'entries_before_market_closure','median_stop_pips','median_cost_fraction_reference_risk',
    'p90_cost_fraction_reference_risk','median_holding_hours','max_holding_hours',
    'maximum_inter_entry_gap_days','leading_no_entry_days','trailing_no_entry_days',
    'zero_entry_months','max_consecutive_zero_entry_months','positive_complete_entry_years',
    'negative_complete_entry_years','zero_entry_complete_years','raw_signal_sha256','accepted_ledger_sha256']
for _width in (12,24,36):
    SUMMARY_FIELDS += [f'worst_{_width}m_realized_r',f'worst_{_width}m_start',f'worst_{_width}m_end',f'zero_entry_{_width}m_windows']


def analyze(work, bars, features, paths, configs, *, progress=True):
    """Pure reporting/replay layer. Production calls this only after hard gates."""
    sinks = {}
    definitions = {
        'summary.csv': SUMMARY_FIELDS,
        'raw_signal_membership.csv': ['config_id','signal_index'],
        'accepted_trades.csv': CASE+['accepted_sequence','signal_index','historical_fill','risk_price','r'],
        'invalid_entries.csv': CASE+['signal_index','reason'],
        'open_at_data_end.csv': CASE+['signal_index','entry','historical_fill','stop','target','held_hours_to_cutoff'],
        'control_accepted_ledgers.csv': CASE+['accepted_sequence','signal_index','exit_index','signal','entry','exit',
             'reference_entry','historical_fill','stop','target','risk_price','exit_price','reason','r'],
        'period_results.csv': CASE+['period','period_type','start','end','partial_calendar_year','entry_count',
             'entry_cohort_completed','entry_cohort_eventual_r','entry_cohort_open',
             'exit_count','realized_r','realized_max_dd_r','open_at_period_end'],
        'monthly_results.csv': CASE+['month','start','end','partial_month','entry_count','exit_count','realized_r','entry_cohort_eventual_r','entry_cohort_open'],
        'rolling_windows.csv': CASE+['months','start','end','entry_count','exit_count','realized_r','zero_entry_window','zero_exit_window'],
        'accepted_comparisons.csv': CASE+['reference_config','common_entries','added_entries','removed_entries',
             'common_completed_r_candidate','common_completed_r_reference','added_completed_r','removed_completed_r',
             'candidate_total_r','reference_total_r','delta_total_r','candidate_open_count','reference_open_count'],
        'chf_event_exposure.csv': CASE+['signal_index','entry','exit','entered_on_event_day','exited_on_event_day',
             'held_across_event_day_start','eventual_r','outcome','attribution_warning'],
    }
    for name,fields in definitions.items():
        sinks[name] = CSVFile(work/name,fields)
    summaries = {}
    path_times = {(i,model):(when(p['entry']),when(p['exit']) if p['exit'] else None)
                  for i,models in paths.items() for model,p in models.items()}
    # Trade R only depends on signal path and assumed fill; p0 is still replayed each time.
    outcome = {(model,cost):{i:r_value(paths[i][model],cost) for i in paths} for model in MODELS for cost in COSTS}
    refs = {}
    for config in configs:
        if config['config_id'] in PARENT_IDS:
            indices = selected_indices(config,features)
            for model in MODELS:
                for cost in COSTS:
                    refs[(config['config_id'],model,cost)] = replay(indices,paths,model,cost)[0]
    shock_start = datetime(2015,1,15,tzinfo=UTC)
    shock_end = datetime(2015,1,16,tzinfo=UTC)
    try:
        for number,config in enumerate(configs,1):
            cid = config['config_id']
            indices = selected_indices(config,features)
            raw_digest = signal_hash(indices,bars)
            for i in indices:
                sinks['raw_signal_membership.csv'].add(dict(config_id=cid,signal_index=i))
            for model in MODELS:
                for cost in COSTS:
                    tag = dict(config_id=cid,execution_model=model,cost_ticks=cost)
                    rs_by_i = outcome[(model,cost)]
                    accepted,invalid,blocked = replay(indices,paths,model,cost)
                    closed = [i for i in accepted if paths[i][model]['exit'] is not None]
                    open_ids = [i for i in accepted if paths[i][model]['exit'] is None]
                    isolated = [i for i in indices if geometry(paths[i][model],cost)[1] is None]
                    isolated_rs = [rs_by_i[i] for i in isolated if rs_by_i[i] is not None]
                    rs = [rs_by_i[i] for i in closed]
                    digest = hashlib.sha256()
                    month_entries = [0]*(len(BOUNDS)-1)
                    month_exits = [0]*(len(BOUNDS)-1)
                    month_cash = [0.]*(len(BOUNDS)-1)
                    month_cohort = [0.]*(len(BOUNDS)-1)
                    month_open = [0]*(len(BOUNDS)-1)
                    for seq,i in enumerate(accepted,1):
                        p = paths[i][model]
                        fill = geometry(p,cost)[0]
                        full = dict(tag,accepted_sequence=seq,signal_index=i,exit_index=p['exit_index'],
                            signal=p['signal'],entry=p['entry'],exit=p['exit'],reference_entry=p['reference_entry'],
                            historical_fill=fill,stop=p['stop'],target=p['target'],risk_price=p['stop']-fill,
                            exit_price=p['exit_price'],reason=p['reason'],r=rs_by_i[i])
                        sinks['accepted_trades.csv'].add({k:full[k] for k in definitions['accepted_trades.csv']})
                        digest.update((json.dumps(full,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode())
                        if cid in PARENT_IDS:
                            sinks['control_accepted_ledgers.csv'].add(full)
                        et,xt = path_times[(i,model)]
                        bucket = bisect.bisect_right(BOUNDS,et)-1
                        month_entries[bucket] += 1
                        if xt is None:
                            month_open[bucket] += 1
                            sinks['open_at_data_end.csv'].add(dict(tag,signal_index=i,entry=p['entry'],historical_fill=fill,
                                stop=p['stop'],target=p['target'],held_hours_to_cutoff=(END-et).total_seconds()/3600))
                        else:
                            month_cohort[bucket] += rs_by_i[i]
                            exit_bucket = bisect.bisect_left(BOUNDS,xt)-1
                            if not 0 <= exit_bucket < len(month_cash):
                                raise RuntimeError('Exit outside the reporting months.')
                            month_cash[exit_bucket] += rs_by_i[i]
                            month_exits[exit_bucket] += 1
                        if et < shock_end and (xt is None or xt >= shock_start):
                            sinks['chf_event_exposure.csv'].add(dict(tag,signal_index=i,entry=p['entry'],exit=p['exit'],
                                entered_on_event_day=int(shock_start <= et < shock_end),
                                exited_on_event_day=int(xt is not None and shock_start < xt <= shock_end),
                                held_across_event_day_start=int(et < shock_start and (xt is None or xt > shock_start)),
                                eventual_r=rs_by_i[i],outcome=p['reason'],
                                attribution_warning='Exposure attribution only; not an event-removal counterfactual'))
                    for i,reason in invalid:
                        sinks['invalid_entries.csv'].add(dict(tag,signal_index=i,reason=reason))
                    for k in range(len(month_cash)):
                        sinks['monthly_results.csv'].add(dict(tag,month=BOUNDS[k].strftime('%Y-%m'),
                            start=iso(BOUNDS[k]),end=iso(BOUNDS[k+1]),partial_month=int(BOUNDS[k+1].day!=1),
                            entry_count=month_entries[k],exit_count=month_exits[k],realized_r=month_cash[k],
                            entry_cohort_eventual_r=month_cohort[k],entry_cohort_open=month_open[k]))
                    positive_years = negative_years = empty_years = 0
                    for label,kind,a,b in PERIODS:
                        entry_ids = [i for i in accepted if a <= path_times[(i,model)][0] < b]
                        exit_ids = [i for i in closed if a < path_times[(i,model)][1] <= b]
                        cohort_closed = [i for i in entry_ids if rs_by_i[i] is not None]
                        cohort_r = sum(rs_by_i[i] for i in cohort_closed)
                        period_stats = stats(rs_by_i[i] for i in exit_ids)
                        opened = sum(path_times[(i,model)][0] < b and
                                     (path_times[(i,model)][1] is None or path_times[(i,model)][1] > b) for i in accepted)
                        partial = int(kind == 'calendar' and b == END and (END.month != 1 or END.day != 1))
                        sinks['period_results.csv'].add(dict(tag,period=label,period_type=kind,start=iso(a),end=iso(b),
                            partial_calendar_year=partial,entry_count=len(entry_ids),entry_cohort_completed=len(cohort_closed),
                            entry_cohort_eventual_r=cohort_r,entry_cohort_open=len(entry_ids)-len(cohort_closed),
                            exit_count=len(exit_ids),realized_r=period_stats['total_r'],
                            realized_max_dd_r=period_stats['max_closed_dd_r'],open_at_period_end=opened))
                        if kind == 'calendar' and not partial:
                            positive_years += cohort_r > 0
                            negative_years += cohort_r < 0
                            empty_years += not entry_ids
                    prefixes = []
                    for series in (month_entries,month_exits,month_cash):
                        values = [0]
                        for v in series:
                            values.append(values[-1]+v)
                        prefixes.append(values)
                    rolled = {}
                    for width in (12,24,36):
                        worst, empty = None, 0
                        for stop in range(width,len(BOUNDS)):
                            if BOUNDS[stop].day != 1:
                                continue  # rolling windows use complete calendar months
                            start = stop-width
                            en,ex,cash = [p[stop]-p[start] for p in prefixes]
                            row = dict(tag,months=width,start=iso(BOUNDS[start]),end=iso(BOUNDS[stop]),
                                entry_count=en,exit_count=ex,realized_r=cash,zero_entry_window=int(en==0),zero_exit_window=int(ex==0))
                            sinks['rolling_windows.csv'].add(row)
                            empty += en == 0
                            if worst is None or cash < worst['realized_r']:
                                worst = row
                        rolled.update({f'worst_{width}m_realized_r':worst['realized_r'],
                                       f'worst_{width}m_start':worst['start'],f'worst_{width}m_end':worst['end'],
                                       f'zero_entry_{width}m_windows':empty})
                    for reference in PARENT_IDS:
                        reference_ids = refs.get((reference,model,cost))
                        if reference_ids is None:
                            continue  # only used by tiny synthetic subset tests
                        ours, theirs = set(accepted),set(reference_ids)
                        common, added, removed = ours & theirs, ours-theirs, theirs-ours
                        total = lambda ids: sum(rs_by_i[i] for i in sorted(ids) if rs_by_i[i] is not None)
                        reference_r = total(theirs)
                        sinks['accepted_comparisons.csv'].add(dict(tag,reference_config=reference,
                            common_entries=len(common),added_entries=len(added),removed_entries=len(removed),
                            common_completed_r_candidate=total(common),common_completed_r_reference=total(common),
                            added_completed_r=total(added),removed_completed_r=total(removed),
                            candidate_total_r=sum(rs),reference_total_r=reference_r,delta_total_r=sum(rs)-reference_r,
                            candidate_open_count=len(open_ids),reference_open_count=sum(rs_by_i[i] is None for i in reference_ids)))
                    entry_times = [path_times[(i,model)][0] for i in accepted]
                    holdings = [(path_times[(i,model)][1]-path_times[(i,model)][0]).total_seconds()/3600 for i in closed]
                    risk_sizes = [paths[i][model]['stop']-paths[i][model]['reference_entry'] for i in accepted]
                    fractions = [cost*TICK/v for v in risk_sizes]
                    run = maxrun = 0
                    for n in month_entries:
                        run = run+1 if n == 0 else 0
                        maxrun = max(maxrun,run)
                    isolated_stats = stats(isolated_rs)
                    row = dict(tag,raw_signals=len(indices),eligible_isolated_signals=len(isolated),
                        isolated_completed=len(isolated_rs),isolated_open=len(isolated)-len(isolated_rs),
                        isolated_total_r=isolated_stats['total_r'],isolated_profit_factor=isolated_stats['profit_factor'],
                        geometry_invalid_all_signals=len(indices)-len(isolated),p0_blocked_signals=blocked,
                        invalid_unblocked_entries=len(invalid),open_at_data_end=len(open_ids),**stats(rs),
                        dual_touch_closed=sum(paths[i][model]['dual_touch'] for i in closed),
                        gap_stop_closed=sum(paths[i][model]['gap_stop'] for i in closed),
                        gap_target_closed=sum(paths[i][model]['gap_target'] for i in closed),
                        entry_next_open_gap_count=sum(paths[i][model]['next_open'] is not None and
                            abs(paths[i][model]['next_open']-paths[i][model]['reference_entry']) > .5*TICK for i in accepted),
                        entries_before_market_closure=sum((paths[i][model]['next_candle_delay_hours'] or 0) > 0 for i in accepted),
                        median_stop_pips=quantile([v/PIP for v in risk_sizes],.5),
                        median_cost_fraction_reference_risk=quantile(fractions,.5),p90_cost_fraction_reference_risk=quantile(fractions,.9),
                        median_holding_hours=quantile(holdings,.5),max_holding_hours=max(holdings) if holdings else None,
                        maximum_inter_entry_gap_days=max(((b-a).total_seconds()/86400 for a,b in zip(entry_times,entry_times[1:])),default=None),
                        leading_no_entry_days=((entry_times[0] if entry_times else END)-START).total_seconds()/86400,
                        trailing_no_entry_days=(END-(entry_times[-1] if entry_times else START)).total_seconds()/86400,
                        zero_entry_months=sum(n==0 for n in month_entries),max_consecutive_zero_entry_months=maxrun,
                        positive_complete_entry_years=positive_years,negative_complete_entry_years=negative_years,
                        zero_entry_complete_years=empty_years,raw_signal_sha256=raw_digest,accepted_ledger_sha256=digest.hexdigest(),**rolled)
                    sinks['summary.csv'].add(row)
                    summaries[(cid,model,cost)] = row
            if progress and (number % 20 == 0 or number == len(configs)):
                set_status(state='analyzing',progress=round(40+52*number/len(configs)),
                           message=f'{number}/{len(configs)} entry configurations completed; all costs and exit assumptions')
        expected = len(configs)*len(MODELS)*len(COSTS)
        if sinks['summary.csv'].count != expected:
            raise RuntimeError('Missing configuration/cost/model results.')
    finally:
        for sink in sinks.values():
            sink.close()
    write_neighbours(work,configs,summaries)
    export_own_anchor_comparisons(work,configs)
    counts = {name:sink.count for name,sink in sinks.items()}
    write_json(work/'output_row_counts.json',counts)
    return summaries,counts


def export_own_anchor_comparisons(work,configs):
    anchors={c['config_id']:c.get('anchor_id') for c in configs if c.get('anchor_id')}
    with (work/'accepted_comparisons.csv').open(newline='') as source:
        reader=csv.DictReader(source);fields=reader.fieldnames
        write_csv(work/'own_anchor_comparisons.csv',(r for r in reader if anchors.get(r['config_id'])==r['reference_config']),fields)


def write_neighbours(work,configs,summaries):
    _,memberships=make_configs()
    by_id={c['config_id']:c for c in configs};rows=[]
    for family,definition in FAMILIES.items():
        ids={r['config_id'] for r in memberships if r['stage_group']==family and r['config_id'] in by_id}
        grid={config_key(by_id[cid]):by_id[cid] for cid in ids}
        for config in sorted(grid.values(),key=lambda c:c['config_id']):
            neighbours=set();edges=[]
            for axis,levels in definition['axes'].items():
                index=levels.index(config[axis])
                if index in (0,len(levels)-1):edges.append(axis+('=LOWER' if index==0 else '=UPPER'))
                for adjacent in (index-1,index+1):
                    if 0<=adjacent<len(levels):
                        other=dict(config,**{axis:levels[adjacent]})
                        if config_key(other) in grid:neighbours.add(grid[config_key(other)]['config_id'])
            for model in MODELS:
                for cost in COSTS:
                    values=[summaries[(cid,model,cost)] for cid in sorted(neighbours)]
                    ours=summaries[(config['config_id'],model,cost)]['raw_signal_sha256']
                    fingerprints={v['raw_signal_sha256'] for v in values}
                    rows.append(dict(study_family=family,config_id=config['config_id'],execution_model=model,cost_ticks=cost,
                        tested_boundaries=';'.join(edges),adjacent_configurations=len(values),
                        distinct_neighbour_raw_streams=len(fingerprints),neighbours_identical_to_this_raw_stream=sum(v['raw_signal_sha256']==ours for v in values),
                        positive_total_r_neighbours=sum(v['total_r']>0 for v in values),
                        distinct_positive_neighbour_raw_streams=len({v['raw_signal_sha256'] for v in values if v['total_r']>0}),
                        median_neighbour_total_r=quantile([v['total_r'] for v in values],.5),
                        minimum_neighbour_total_r=min((v['total_r'] for v in values),default=None),
                        minimum_neighbour_closed_trades=min((v['closed_trades'] for v in values),default=None),
                        neighbour_ids=';'.join(sorted(neighbours))))
    fields=['study_family']+CASE+['tested_boundaries','adjacent_configurations','distinct_neighbour_raw_streams',
        'neighbours_identical_to_this_raw_stream','positive_total_r_neighbours','distinct_positive_neighbour_raw_streams',
        'median_neighbour_total_r','minimum_neighbour_total_r','minimum_neighbour_closed_trades','neighbour_ids']
    write_csv(work/'neighbourhood_summary.csv',rows,fields)
    groups=defaultdict(list)
    for config in configs:
        digest=summaries[(config['config_id'],MODELS[0],COSTS[0])]['raw_signal_sha256']
        groups[digest].append(config['config_id'])
    write_csv(work/'distinct_raw_signal_groups.csv',[dict(raw_signal_sha256=digest,configuration_count=len(ids),
        config_ids=';'.join(sorted(ids)),interpretation='Shared qualifying stream, not independent evidence; full p0 replay reported separately') for digest,ids in sorted(groups.items())])


def self_checks():
    """Small mathematical/execution checks; no claims about market performance."""
    passed = []
    def check(condition, name):
        if not condition:
            raise AssertionError(name)
        passed.append(dict(check=name,status='PASS',evidence='SYNTHETIC_SOFTWARE_ONLY'))
    t = datetime(2020,1,1,tzinfo=UTC)
    bars = [(t,10.,11.,9.,10.5),(t+BAR,10.5,11.,9.,9.5)]
    check(bearish_engulf(bars,1),'Exact bearish engulf accepts boundary equality')
    check(not bearish_engulf([(t,10.,11.,9.,10.),bars[1]],1),'Previous doji rejected')
    vals = [1.,3.,2.,100.,4.,5.]
    ph = previous_high(vals,3)
    check(ph == [None,None,None,3.,100.,100.],'Prior extreme excludes signal high')
    for lb in (1,2,3,5):
        fast = previous_high(vals,lb)
        check(all(fast[i] == max(vals[i-lb:i]) for i in range(lb,len(vals))),f'Prior high agrees with direct slices LB{lb}')
    uniform = [(t+i*BAR,10.,11.,9.,10.) for i in range(25)]
    atr = atr14(uniform)
    check(atr[:14] == [None]*14 and all(x == 2. for x in atr[14:]),'Wilder ATR seed and recursion')
    c,m = make_configs()
    check(len(c)==62 and len(m)==64,'Complete grid and duplicate membership control')
    check(len([x for x in c if x['config_id']=='BASELINE_CONTROL'])==1,'Predeclared baseline has one immutable geometry')
    # Signal close 10, stop 11.0001, target 6.9997. Both barriers; low nearer open.
    two = [(t,10.5,11.,9.,10.),(t+BAR,7.2,11.2,6.8,8.)]
    paths = find_paths(two,0)
    check(paths['NEAREST_OPEN_SENSITIVITY']['reason']=='TARGET' and paths['STOP_FIRST_GAP_STRESS']['reason']=='STOP',
          'Dual-touch stop-first diagnostic differs from alternate heuristic')
    gapped = [(t,10.5,11.,9.,10.),(t+BAR,12.,12.1,11.5,11.8)]
    gp = find_paths(gapped,0)
    check(gp['STOP_FIRST_GAP_STRESS']['exit_price']==12. and r_value(gp['STOP_FIRST_GAP_STRESS'],10)<-1,
          'Adverse opening gap can lose more than 1R')
    check(gp['STOP_FIRST_GAP_STRESS']['exit']==iso(t+BAR),'Known opening-gap exit uses opening timestamp')
    capped = [(t,10.5,11.,9.,10.),(t+BAR,6.,6.5,5.5,6.2)]
    cp = find_paths(capped,0)['STOP_FIRST_GAP_STRESS']
    check(cp['exit_price']==cp['target'] and cp['gap_target']==1,'Favorable target gap gives no improvement')
    # Explicit replay fixtures isolate p0 from barrier detection.
    fixture = dict(signal_index=0,signal=iso(t),entry=iso(t+BAR),reference_entry=1.,stop=1.001,
                   target=.997,exit_index=None,exit=None,exit_price=None,reason='OPEN_AT_DATA_END',dual_touch=0,gap_stop=0,gap_target=0)
    mapping = {i:{model:dict(fixture,signal_index=i) for model in MODELS} for i in (0,1,2)}
    chosen,_,blocked = replay([0,1,2],mapping,'NEAREST_OPEN_SENSITIVITY',10)
    check(chosen==[0] and blocked==2,'Open trade occupies p0 through data end')
    for model in MODELS:
        mapping[0][model].update(exit_index=1,exit=iso(t+2*BAR),exit_price=1.001,reason='STOP')
    check(replay([0,1,2],mapping,'NEAREST_OPEN_SENSITIVITY',10)[0]==[0,1],'Exit-candle signal can re-enter')
    mapping[0]['NEAREST_OPEN_SENSITIVITY'].update(target=.9998)
    check(replay([0,1,2],mapping,'NEAREST_OPEN_SENSITIVITY',40)[0]==[1],'Invalid high-cost entry does not occupy p0')
    check(replay([0,1,2],mapping,'NEAREST_OPEN_SENSITIVITY',10)[0]==[0,1],'Cost-specific replay preserves valid lower-cost entry')
    check(geometry(dict(fixture,entry=iso(END)),10)[1]=='ENTRY_AT_OR_AFTER_CUTOFF','No new trade at exclusive data cutoff')
    check(r_value(fixture,10) is None,'Unclosed trade has no invented realized R')
    check(stats([])['closed_trades']==0 and stats([])['total_r']==0,'Empty case remains explicit')
    check(stats([2.,-1.,-1.,-1.,2.])['max_closed_dd_r']==-3.,'Closed R drawdown chronology')
    check(stats([-1.,-1.,2.,-1.])['max_losing_streak']==2,'Losing streak reset')
    check(len(BOUNDS)==263 and len(PERIODS)==35,'Complete zero-inclusive monthly and period universe')
    return passed


def package(work, completed):
    """Atomic publication. An error archive cannot include partial performance."""
    allowed_on_error = {'protocol.md','run_manifest.json','error_report.csv','hard_controls.csv',
                        'software_checks.csv','coverage.csv','data_gaps.csv','runner_source.py',
                        'source_controls.csv','h1_m15_crosschecks.csv','yearly_data_coverage.csv',
                        'coverage_crosscheck_summary.json','archived_pass1_fetch_receipts.csv','parent_pass1_parity.csv','parent_pass1_provenance.json','conditional_feature_controls.csv'}
    members = [p for p in sorted(work.iterdir()) if p.is_file() and p.name != 'file_manifest.json' and
               (completed or p.name in allowed_on_error)]
    files = [dict(file=p.name,bytes=p.stat().st_size,sha256=file_sha(p)) for p in members]
    write_json(work/'file_manifest.json',files)
    temporary = OUT / (RESULT_NAME+'.tmp')
    with zipfile.ZipFile(temporary,'w',zipfile.ZIP_DEFLATED,compresslevel=6,allowZip64=True) as z:
        for p in members+[work/'file_manifest.json']:
            z.write(p,p.name)
    os.replace(temporary,OUT/RESULT_NAME)


def run_job():
    global RUN_CLOCK, JOB_LOCK
    RUN_CLOCK=time.monotonic()
    OUT.mkdir(parents=True,exist_ok=True)
    work=Path(tempfile.mkdtemp(prefix='working-',dir=OUT))
    manifest=dict(version=VERSION,runner_sha256=code_hash(),pair=PAIR,side=SIDE,timeframe=TIMEFRAME,
        dataset_kind='COMPANION_ZIP_EXACT_PASS1_OANDA_MID',frozen_source_archive_sha256=EMBEDDED_PAYLOAD_SHA256,status='RUNNING',complete=False,
        study='PASS2_SINGLE_CONDITIONAL_FEATURES_FIXED_B_ANCHORS',start=iso(START),end_exclusive=iso(END),
        rr=RR,cost_ticks=list(COSTS),execution_models=list(MODELS),
        expected_configurations=62,expected_cases=372,
        incumbent_target='PORTFOLIO32_2026_10_05_EURCHF_H1_SHORT_PRIMARY_RR3P50_V1; admission deferred',
        template='FOREX_STRATEGY_RESEARCH_TEMPLATE_AUDJPY_2026-09-24.md',
        source_policy='Exact Pass1 companion M15/H1/member pins; archived complete parent ledgers and aggregation must pass',
        user_clarification='Template is a guide; justified pair/timeframe adaptations allowed.',
        orders_supported=False,trading_enabled=False)
    try:
        (work/'protocol.md').write_text(PROTOCOL,encoding='utf-8')
        shutil.copyfile(__file__,work/'runner_source.py')
        write_csv(work/'software_checks.csv',self_checks())
        set_status(state='validating',progress=1,message='Software checks passed; verifying companion ZIP with exact Pass 1 source')
        bars,volumes,h1,h1_volumes=load_frozen_history(work)
        crosscheck_history(work,bars,volumes,h1,h1_volumes)
        configs,memberships=make_configs()
        features=make_features(bars)
        paths={}
        set_status(state='building_paths',progress=28,message='Full-history coverage passed; building raw engulf paths')
        for number,i in enumerate(features,1):
            paths[i]=find_paths(bars,i)
            if number%2000==0:
                set_status(state='building_paths',progress=round(28+10*number/max(1,len(features))),
                           message=f'{number}/{len(features)} raw engulf paths built')
        set_status(state='validating',progress=38,message='Independently checking ATR, signals and complete control ledgers')
        hard=hard_controls(bars,features,paths,configs)
        write_csv(work/'hard_controls.csv',hard)
        if any(r['status']!='PASS' for r in hard):
            raise RuntimeError('Independent implementation control failed; no discovery conclusions are valid.')
        conditional=conditional_feature_controls(bars,features)
        write_csv(work/'conditional_feature_controls.csv',conditional)
        if any(r['status']!='PASS' for r in conditional):
            raise RuntimeError('Independent lagged conditional feature controls failed.')
        archived_parent_parity(work,bars,features,paths,configs)
        write_inputs(work,bars,features,paths,configs,memberships,manifest['dataset_kind'],volumes)
        (work/'README.md').write_text(RESULT_README,encoding='utf-8')
        manifest.update(source_sha256=source_hash(bars),source_volume_csv_sha256=file_sha(work/'source_candles.csv'),
                        source_h1_sha256=source_hash(h1),source_candles=len(bars),raw_engulf_signals=len(features),
                        source_controls='PASS',hard_controls='PASS',parent_pass1_parity='PASS',conditional_feature_controls='PASS',software_controls='PASS',protocol_sha256=sha(PROTOCOL.encode()))
        _,counts=analyze(work,bars,features,paths,configs)
        manifest.update(status='COMPLETE',complete=True,output_row_counts=counts,
                        completed_at=iso(datetime.now(UTC)),elapsed_seconds=round(time.monotonic()-RUN_CLOCK,1))
        write_json(work/'run_manifest.json',manifest)
        set_status(state='packaging',progress=96,message='All 372 cases complete; compressing ledgers and diagnostics')
        package(work,True)
        set_status(state='complete',progress=100,message='EUR/CHF M15 short Pass 2 complete; download the results ZIP',
                   hard_controls='PASS',source_controls='PASS',configurations=62,cases=372,result_path='/results',
                   result_bytes=(OUT/RESULT_NAME).stat().st_size)
        return True
    except Exception as exc:
        manifest.update(status='ERROR',complete=False,error_type=type(exc).__name__,error=str(exc))
        write_json(work/'run_manifest.json',manifest)
        write_csv(work/'error_report.csv',[dict(error_type=type(exc).__name__,message=str(exc),traceback=traceback.format_exc())])
        try:
            package(work,False)
            result_path='/results'
        except Exception:
            result_path=None
        set_status(state='error',progress=100,message=str(exc),result_path=result_path,hard_controls='NOT_COMPLETE')
        return False
    finally:
        shutil.rmtree(work,ignore_errors=True)
        if JOB_LOCK is not None:
            JOB_LOCK.close()
            JOB_LOCK=None


RESULT_README = "# EURCHF M15 SHORT Pass2 results\nBegin with run_manifest.json complete=true. Require source_controls.csv,\nhard_controls.csv,conditional_feature_controls.csv and parent_pass1_parity.csv allPASS.\n62configurations/64memberships/372cases. Sixcomplete parent controls, including\noriginalB and TIGHTB references, must reproduce before conditional outcome study.\n56new settings each addONEcausal condition; fixed entries/RR3/costs/history.\nRead protocol.md for exact lagged feature definitions and missing-history policy.\nown_anchor_comparisons.csv isolates each candidate's fixed-anchor replay changes;\naccepted_comparisons.csv includes allsixcontrols. Added entries can displace later\nentries even when the qualifying stream is a subset; isolated performance is separate.\nAllweak/empty rows, complete accepted/source/path/control ledgers, family-labelled\nneighbours, sharedrawstreams, yearly/blank/recent/rolling statistics exported.\nPartialOctober never a complete rolling endpoint. R/closedDD additive,notNAV%.\nEvery sourceZIP/memberSHA/size validated; same completed historicalsource retained.\nNo network/orders/account reads. The exact existing dataZIP is included for reruns;\nPython contains the compact additional TIGHTreference. MID/assumedcosts/exits are\nresearch assumptions, not executable bid/ask history. No auto winner. Review4pip\nneighbourhoods/activity/weakrecentyears before any next confirmation. Park tested\nbranch if stressed improvements stay absent,sparse or isolated. RR remains later.\n"


def launch(background=True):
    global STARTED, JOB_LOCK
    with LOCK:
        if STARTED:
            return False
        OUT.mkdir(parents=True,exist_ok=True)
        # Linux Railway/Gunicorn: avoid two workers writing one result archive.
        if os.name == 'posix':
            import fcntl
            handle = (OUT/'run.lock').open('a+')
            try:
                fcntl.flock(handle,fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                handle.close()
                STARTED = True
                return False
            JOB_LOCK = handle
        STARTED = True
        # Reuse an exact-version complete result across multiple WSGI workers.
        previous = read_status()
        if previous.get('state')=='complete' and previous.get('runner_sha256')==code_hash() and (OUT/RESULT_NAME).exists():
            if JOB_LOCK is not None:
                JOB_LOCK.close()
                JOB_LOCK = None
            return False
        set_status(state='starting',progress=0,message='Starting EUR/CHF M15 short Pass 2 conditional features',runner_sha256=code_hash(),result_path=None)
    if background:
        threading.Thread(target=run_job,name='eurchf-m15-pass2-research',daemon=True).start()
        return True
    return run_job()


def app(environ, start_response):
    """Dependency-free WSGI app, including status while the worker is busy."""
    path = environ.get('PATH_INFO','/')
    if path.startswith(PREFIX):
        path = path[len(PREFIX):] or '/'
    method = environ.get('REQUEST_METHOD','GET')
    if method not in ('GET','HEAD'):
        body = b'{"error":"GET or HEAD only; no order endpoints"}'
        start_response('405 Method Not Allowed',[('Content-Type','application/json'),('Content-Length',str(len(body)))])
        return [] if method == 'HEAD' else [body]
    if path not in ('/','/health','/start','/status','/results'):
        body = b'{"error":"Not found"}'
        start_response('404 Not Found',[('Content-Type','application/json'),('Content-Length',str(len(body)))])
        return [] if method == 'HEAD' else [body]
    if path=='/start' or (not STARTED and os.getenv('EURCHF_M15_PASS2_AUTOSTART','1')=='1'):
        launch()
    current = read_status()
    code = '200 OK'
    if path=='/results':
        bundle = OUT/RESULT_NAME
        if current.get('state') in ('complete','error') and current.get('result_path') and bundle.exists():
            start_response(code,[('Content-Type','application/zip'),('Content-Length',str(bundle.stat().st_size)),
                                 ('Content-Disposition',f'attachment; filename="{RESULT_NAME}"'),('Cache-Control','no-store')])
            if method=='HEAD':
                return []
            def chunks():
                with bundle.open('rb') as stream:
                    while True:
                        block = stream.read(1024*1024)
                        if not block:
                            break
                        yield block
            return chunks()
        current = dict(error='Results are not ready',**current)
        code = '409 Conflict'
    elif path=='/health':
        current = dict(ok=True,state=current['state'],version=VERSION,orders_supported=False)
    elif path=='/':
        current = dict(service='EUR/CHF M15 SHORT Pass 2 conditional features',version=VERSION,status='/status',results='/results',
                       start='/start',research_state=current['state'],orders_supported=False,trading_enabled=False)
    body = (json.dumps(current,allow_nan=False)+'\n').encode()
    start_response(code,[('Content-Type','application/json'),('Content-Length',str(len(body))),('Cache-Control','no-store')])
    return [] if method=='HEAD' else [body]


class ThreadedWSGIServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--run',action='store_true',help='Run the historical study once without an HTTP server')
    group.add_argument('--self-test',action='store_true',help='Synthetic mathematical/execution checks only; no OANDA request')
    args = parser.parse_args()
    if args.self_test:
        print(json.dumps(dict(version=VERSION,market_data_used=False,checks=self_checks()),indent=2))
        return 0
    if args.run:
        return 0 if launch(background=False) else (0 if read_status().get('state')=='complete' else 1)
    port = int(os.getenv('PORT','8080'))
    with make_server('0.0.0.0',port,app,server_class=ThreadedWSGIServer) as server:
        if os.getenv('EURCHF_M15_PASS2_AUTOSTART','1')=='1':
            launch()
        print(f'{VERSION}: listening on {port}; /status and /results',flush=True)
        server.serve_forever()
    return 0



EMBEDDED_MEMBER_PINS = {'source_candles.csv': {'bytes': 31136287, 'sha256': '59f3a83fb838d9921f9f2433cd928a77ef451301bec466a3fedd67744ae2f707'}, 'source_h1_crosscheck_candles.csv': {'bytes': 7942356, 'sha256': 'ffaa24aa4c371b7885c61ef225f1846ccf5e133d444a45c438af3066cef702a3'}, 'fetch_receipts.csv': {'bytes': 19964, 'sha256': '52423125b321d3c25b5ae2adf89c8fd287b230764540847b375279cc92530890'}, 'parent_pass1_accepted_ledgers.csv': {'bytes': 23899883, 'sha256': '05bef6ac72de3d5d7c4a8e6ca0fc2dc52c13c684f956e9f3c88cf8f4aea4a706'}, 'parent_pass1_provenance.json': {'bytes': 71790, 'sha256': 'f11996dfcd59473762a35b3cd02ab627a3b9757d71f2021b593999f5ce60ea1c'}}
EMBEDDED_PAYLOAD_SHA256 = '511215ae3b95e676829896d30baa1342bbca17e7be30745a22753dab5ba3b849'
FROZEN_SOURCE_FILE = Path(__file__).resolve().with_name('EURCHF_M15_PASS1_FROZEN_DATA.zip')

TIGHT_REFERENCE_SHA256 = 'fc379c03f8ba592b3092aa28d8b4ad7016baf30ed0c9dacf0984312a59c411a2'
TIGHT_REFERENCE_B85 = 'c-pkRZO?5<ZXNbt=CgZHB#ZT;Z+4;>0s`BRIsuY=7zonX8CV%PGh@Vok^df6)!ygS3wfS>_AYg|t}uP?sXnaru!>}nYyJ1%{OO<npFjWRul|q!{+qx5hky9}fBDPr|M7Q!{{8>`Uw{9{fB5}x{^~dIn}7A2fB5G={>MN3r{DeIALW1lo8SH2uYdc${13nWFMs>jzx$ujx!?Vd&PD#+zX9a>C;$AH-~Hte|L~vxTt4dL-~9fMfBDn@EB{0ODF5QZFA#nO|5wh*KmU9A?f>@sfB3I|`NKc|@pu2%KmX(J|4IJfzx(Td{kMPpcfbDb-~Wfd{&&CoyTAT-fA=@P{>}gTH^2UG^$-8yFVj-|SMtxf)F1xxyFdK#AAkSf%6}Uo@cA$Q%b)(xZ22vFm*wC7pMUuCU;bJC<=_9)@BZT-{^_6EZ&6P2TYr-O@eBO6eDy#5{_p?%&wrHv{dd3q`~Oh?_kaBTpUSrU?lh`?sx0`YKm7SWzy34sx6Xmf@4r$^<q!S&5C7?pfB#RD0rXe?G@$+dU_$vC@>Ty!|K<Pk_ka3NzyHheUk};6|MI{2|NfPZB5p-7{^H<Qmv*37mx5rSh{;6_#hf~`*+Nvd=Rg#H8^L4Z*RQ_E_t$^@xBvF9f3*;1iYmg?BkVb?2)BKTHo_RA9}s4q0G=(i0H&P)1O9^USGU7WkEQZ^4+enIC(r<90uxlk8*a)-8i+1+Ac`xkZh9DCyD9pZClKqGA5rYxluQ);J`}?*4u5g{E4tpdZlTzg3bvbKg5Wh2Bbq4kox_$n-*LrmFJ3><Gm6Qj3B~&5dlVVnC12#tC?b}v_d8HTn%ufaF;mifF<`Jz3_v$f4B&MX`|Vw?%x*8pLNSxa0mb&^2NYxMep-TwV%Ulz{X#HBD|b<BOGN`kl0T%On1IcDGP!EQw>Tz8d=toXJ1loBR*bRF@%?)wV~GBZWHgbCTalDYJ(f#-1Cn(q!1s^yh-6A$^~Z$6WnzFD9lnAez8>vFr(cx4(_{bz({p1?OjD}P_{KEO_vCDvg=pG|D1iJtmT>ktqHQT~MzrH4@YE2^AAB>r!0#=A8*Z9qEAGTT-OgB$ia|9venXm9H%*E7N{*8Efg0<DpUY1pJq%&%Qs9R$egKty^M(o3fYDo^24uQ%;L69miX`^Qra(EzKizhK5ydj48X>=sJimX3WPt4LsHbAE4?mK~`&Snpry%E2a?I+%tHhMmt!Dfnr0#Yxp@fw9ea?2L7f=e9k6l?N8%{bbNqL2~?G&2{iiFy6IClldus}OmYoffd9+ZBOf5f#m1h;J|E-33^QASEC%AE4gmubLutTmLAgZ%nGxPG_FD#7qy=r7mBf4hycB44r3`O$xg;|%g-tO+u11v&o$xfHtKImr1)01S|Q_N3jfE?|EH86XP~{tdDO#grxG%M#~i`T}LySv9{Q>eAGd>*+9|eD0KW-)5icj=xcs5Ijxy?QE@yGVMfJVg+!ewEG<8V<{`c$*EOpXOtabvZ5$CIXnMNa@&O@w^{OX6JL*wf%N;A{18gf0_<yZOkh*Xz2AXUIglq{tAQq(ek+<-Pg8z;{dP;~Sq{jbNt)9p9s*P|cLh(+;$u0&)s%+<ciCl&h9+2>Y2GgI7UN*dV;8fAl-8v#hOkpw*jQuAHG~Ky&VPChpG%n;5V#1@X%&4Sv&tl)>8+O;@hKJ8A(xFXpTpgLShsC4BaDTn!+;)4%-;7unkJZGE10nste>Z|Z7G;;?%|Cs$W|&~Lge)Geai^%3Pp6_vmC~e<dF4vJ#v;+NH0PSw{1u(%I8K|hcW=?FeF8cl7dYr&&HZ4$IU2{n`%e*Q7%i7aoq~akx@aJAei?pIk@Gr4po;0-xv7hy4ApJ`vK-_s+cjB6b}OimkoWw^&t(~SAhcei}R&0{~XNsQfz@qOj&ss_brUnu=Lati7&B~j|KJJ6cIZxxz+36SoYgtVkuUnArAIS80;~Aik9+L{3LFV<zeOF{rO(XihaRzAf<#TxrMi0Oo`Clc&3q$Sto0Mf6{!i_WNPppJGP!^cR4q0Ud%8?P8a%qTSr!HiL=&SA?gb@3B;zijV`ClB5R(ObHyK1ttc7NAa$#fS~86UA&uI3>vOwYn&bL`T%LTF;821Tp{!!yB*}>zFJeMWJ67^@v)P>mNMfa_7s&v7IX4eFeKZQZh$Et%Q~2$e}u#NknOg^^wDC)faUr9J1jAI`rw$j6+kM{=<~q#wUlHjhGTMySvh4fQ8ocg5b_NG6U3}vlYKEcF|7!m`(ehfVn!NH+0%d?!N`52{J|S(Cz$dN^3+js6HLj%U40NYyirnjt*-$onW+iNS?F(oDKU>)`tyDKDxS^@n6LdX!Hm(3rG(Rf9!zw7%K5=F_f?C^_RBx1Jk~rtT3<_9k8O}7dQ_@K2rgK?M(A?9k&MX4QqrGf`gS9ppThh7Fwa&oqqRl6z&s4-!IUdFTmqAKf(f-%3Sa}6P}{~F7{@F+j7hsEo%!5`k_o1F(I9h{cq2seA-5#uQq2H?ot|fA?t6nEEzrJp$813oqsWcWhUL(8AD-B7UpWY<v>8hbDzVDb+_Ei|rZ?FhPLLm0)DGJA5~?4x^7oyC7Hi(P0?2*UC(ea}em|__vOLmJ7!TTJz;MtexYl?bw}L4Fco>>d?}J&FGT%!jOOINiRt%4w;Wn<$zk9EJ_ZP+MwLMvdgWin<eLZM7g1enE{ZtGbdsmPE(gO2ntqF7533ExcXv2%?v6Q##L0BT4q`@4sRS-f9w}K-KdT<0omAqAyM=)ReVRrAzfxtsS=rmveQ*!4kou8Puf>|TGzV-M%m{5JznP7&Lf_4D6mIC7d_U|gZ{6X?QNluUgHS4Z=DzbVYyS_N?&@14!$T!;!_F~p_qY_QfRtwG9h>i6jU(&APQSmXJucgf42qKSau$!WZC$y20Z=Dlj(w)=w!7k<*zn2lg#;=t2aEyqW0X>2elHrwLmtZTH@i<xNIhbuJv%B6yWX(At*v<*J4%(wvX>r9c_L3=3#`)e^W?jSxKLxjLfwJ83CVsdTzg!wCp0|0vmKy7W1Af8#tR*W6%`$?E{!UFu?9Y1G)7ONs4Xlg^p8H`V82ngyKMfet6ApbGZG*Y7CdBpGJ#T2Oe=n7+fu)i+DyfPKFuwokxVjJ{z1xW}#}tvg^LWm-T)H%6nWVn3$Xea*^IW{`cFGKh#K1Vf>?Dw#9CfkQgn47Ns1ARWv4_Xgh3}<;<+G9+BxNwo<w%y#N@kvK^k%ypoG}d3kN!EF>$oo?K1>*j8Dk~>G@u7F%dKi7$XgLiNACDE4Sg*|R*5LH+G64|MH4{ea0fuiPgDSh=|Y&NnfmRp$I{r1E!fBR@9|5SFO_a7Y=ttH!2p2AXKr1pG@@tg%*~<Y*9(-szifgDYe?Nd6Zw=VqU>YoioBkv$TC?^R8r{QTTp@d+$!_Xl`~*WRK!o`7L&zX6X%VUqj<CyKX37SFJ<9O2%eQg7nsZo6}SyXed)9EG1h1Exo=T`r3~2Z2bmLyV#biQoCb7YLK%R%<g{rsm<T%prEV#2ff>Bhz)XZDn1TEqFoV}q(18MB?yl}_u%}5OP#Upp-@nI_-L=7B$~&<Hw<+j-EtRZ!QX~$fVcF7i9m`1X_m&VPw*&d1mruA_biH=y$|_!eZ^3@>B`<etw>>tt)VKtCY{Lv~vCt%4*jOG)HJS_4>39pzbE)xe?(rmyTDcF(oR5X4+)y`yBl(yQcL416!vs*w7*mzgfC0g1_fLD+3ZVZ5>MPd<0NYa01dv-x>I`6R`M(OFgh4kRv*lx1f6R^pQPEOT==Q?~P~3ZnZchVx0JGfHHiEbn!C2pZp0^{MOPRByBpJW<f-OJ69)Er%lUoVW<5x0S1=F2ZvTmo$D5#V)Xmn1Rp`DF2QKp?Jmq#HkbFa_+^jOL&Ban}tl_V4dCaa7fTaxb_w4R*t;+cjr!iwc<KTJO@W{g-K1`H7`d<bLx#zIi8gY|j18g3$3mzq-W5kZbFD!C_6JGoQ@DcqT1kdGNN4A_U4KF&GGYd?}%2$J}XN0y?~fF41)PwjpL&s!0cy8(8T^4d~ZdW8&Ho8Vd(`+}g&ogt~&9bOyx*ezb0_kP%=JTU_}j|eV|8GsjwZG6}YW~g;Yw*$;@Fxwahs^pYOb4Q3KJtLH$;+=`e_Ir|_eq^7&7~z#wl8$Zv`dA1xR&1+HE`Pw*yEt@|0JeY8V_Qu$0gYP$ZRCo)1JFVtVs{=;;D+1+s7)SL^2i&lU-GeJw0`-%Oc0dwwe5%hfS?3B;|&Q5ff#`Em+AyQ?F94af!T4?9z8);_q#ZiI+jvi7`mpnWEX0ES|?QSx3P@d#}F|y%b&q-YPPIjsoafHM^3ar{^}euA&<u+dsnkfE|?SA%O;rU@2e7_JIG?)V=l+{Qr5U9C5cV6H?!DEXe#9K7AdEEjC5Cqei8}iP^sS!n{2Gq*~bUaW<U?7fbTCcI=X)WFyl5b&2f9#1$L}~IZ)29jcf4wh<3&=-nk=3gYmKN{9B&Z`mOqebA}1$yy85XYvK%BanAL=dEBl0Udoyzq%u9GRFcX&mieHT0>2;BLVo`pF@6DV3PE>bf{_Ay1^3(^v-%)MQ6AblU5Ya;O0&^KbYEo&>&O+PA%eJxXq(=`Df#WG0$uWIzSD9OHK1~_t0qOqDIpaS3cxQ&s$>--f2IC`6z4YGes?T9#VDjPe|2dlRHzo3aMD&dQy~p_UaNmCWp@AhLY)VigSudLqR7v_zjLc*qtCjZSb!RsP57PLVbVQ{6@;Tdw(mb2w&B_XH~Xzv=E~Cen3TR>N-z3)bUvwAX0vBre(z2(Nj_$kc0bawzKyWS=c^fGx58n-aJb5ua)mOEc{7+u!=U*SFwsxXIRlu0q%|0&U_Oe<nT6re41SsC)yur8k9>nuacx^;9<pMQ8zZ8_mH}6~*80c$N=%6LhTt8MZ?}{l5&1$qpgAd_8SUO9t#KA<4fX+0imf!%VcFSW9=OGd(MQ|&@1c}r{Sx0F$DL40W{qwGqxrEEOub1xlycg_l99UKmd;rH?kvAID5HGsqICs*Fdw)t|Fj@&g!Q#GCaiMs`YsN7+w#Iy{#DG^X1O_S#TAa(<)`=PYbmQA#Y++yK*M!lGtL&o8M@@u1H|B4(0e|;U5Iz<GJ10Q!??uTKAU;BSjaHooN??sQp`1R-dIthGhlsQQKEP3Yxr-FA7sscIg#nV9KSz{>&J}brLQPuS}G0Qewa=xX7v2^Zv%QTOJ~#-rwwi^n7O_?Z2>c{PkIA|{qZ=mfEkT=sr+7`TVMun1iyVpc0dKpb3bh2dIxztfe!;lFgaWya15{+OysF0^FEkZiMYVhBOugH;E<w)Wr(-13|0r=4^^UWJIEFx08ay!?fdso%1_`2XT+WO6*d<?@hRTK?^r6DdJ?|1+AjDt`XU{`fc_4EL1(pwIJ}C_?YVCw(7q<c4B!#KX25_T3@kb}n6w#8tn>5Fo$*-842L57wN_sYXY*hU{vMWI$I^}TCoXGPw(YQ;k!d<u+xH)#jMvW9jTIt)_6R-hZ+I_d^*2bu!t3C6y-@6MmMPr9lF58q87|4$FHa*rG&B@5`oZnD0X>*L=X?oF-U=okx`G?P<XVXjmS3i*%(K+E-Cs#l8oT+|*{3m%KeSsfElBT;Ngq&+7)MlBONRV}KY$vxf?5fNHiWUSr7Y?xFF&#&XooF<nUe(kzMZUoj0|a)KJ)XzuUMY@VTQ5Aj4{DH3>d=L+)vPa2!rBQFq`^m-T`Jj9txcWW@B<*z|6)VL=6%Bt*f)WS1FF^hxS~>G0*+@04DGh$L4VmqTZ$+u8ih~v>D7I9U1q4iQXQH@EKaEbPa>x1DFusPuV_5R2s^&2)l^aD?uTlK1~QcV|ANSTd-~0V`D2np_8L#{BY=2YfWtPPHX}9P|3fSvWRinA$tv5?+xjt@_QI>_^G!ZvHNU=X@&B&8>UiPF=C8xkFVe3N4Zx3x1o$1>p)zIC*!1#bPGy86ywlTO>>M<D;-c9)7GJcuA7Lnqmt8cOikA8qL+Hm8+d!Qmcp?*A^X~#4?<~TlOHR5$2i7-Z1_+~h_Dq-KeM^qhqEqa_I$F-eieBTYvqjUxkxNzDZ!iWF9jFmheM9>Dx&j6EuNY}==zj4(C{s#_-?1H1GpGSdBheLubr(mVaBa6a~*w1j}PFs6iu&kJ<LS~L#y6WYc<iajkhuwidmNr7$**wf>F00<`FD6JCBZf7%(_0*?CK(tpLV#&hsvSZK-VeY3WDTlF@dA>iChnQ?aFEHgQc``W$qbD(ttzbVjj)$ATZx903trBZP~7E0pOM%+nasT`1d9rfw``AT1aw42(ZXQr3G2-!ZQo-cCZRT>pQi$h&&tHaQXHjRkurjebSBnP-bZ6a&GHD2MzZ=A8{U5$COl*Dllz&n>T|vU&PurPrt)m29J&@z&w%?$B}e+d-!DW~4DqZQp-@k_QGB8_JFKqfj+1{6PA96Ux_8R<WKj5v$>Edr;G{yp_pV!`(c#bIuOh?S^@ciV+0UF^sUB!yGO_8Mi{&dWoN(!`)JbR}zM#U8$};S*lRF8!Qa<V+ISukh?4@&u=3>oVLY`(Jc=HdNAdZxKs*^X)Boi$h<cArLR3VU|p)@UDE2hh&hh^U@ET2;ho0>iI&;<#J-%uAzr^#kz+8XO#AXZD^7cEl`YinogZT-3}L=XXLC)Q^H!WI5#EL#?AKD(ctp`Aa%%7t4XuItdw4IAA<`Sf`dZS_PBMbojIevKs~PiMnwE+K4-GFSn8|GgGZ#<WP!@bK=Xn!MA5^EMoQ4#@tPr|48!LM!Kl~}NE2LBOMP12|yOZ;zXTS~$%;!#-KI{F!+fe^&OY}P#Yf2a12N@?BQ9kDfA5Rs(mts>aHUewHzvje0Q;v5uM05V}<urx4(dcb}Ju6&H7!^d@m+u8chqmKSx*%IoY)?yR2a4^f%^H^gOfG6D4jg?}hdpzr!tO)=*LCq8StQ=kh<$C7eH;Te^87czGd0Izs7VsI6JNmkroY2WkEKk>0pDKIW_(*sLB|yDh_1K(7^MSgU?ZEKQQro9U`sA#kP$?J1;yS=?Mj2+hhnJj4^;@{sW5k2D%f6Hv<SV1Vnh?gI~u?8seH#3yS*^EbdyUHigB7@lAj@W$rpJuiii{C_<a=1tFfsVkxA1nWSdfj1KgltI63QA6#MNB^T2Pv7dG)V`C(p^7djQe^&0JJ*oq?kLa<?a>AjSt*i7iu8#CFwRh%5$;H{F68N5|L19@(T&9W+=9Dqy<ZI$G_*J!KaRwQGkWr{nHj0bC#L9&pNSN*Y%+l_jC*`1CG2gbjy$387vRC1L5-gHhu_}nI&NF(7u;>mHoHD^Q3Jai|%Rp@qu>hEhQlPtwYQ1Q(lJT*J!N0Y#^LgYJ<P`5MokPD0occ&>ms3{R&Nm%keP~*{D@?5s3Ej7)#Pql?M96x~?FnSBrfP9BGP(J2WBu9$mHrUW3*=>hi^h-F98J*w1LoyVK3A<|Gw&F+ih5m7$(Q7G_VOJtdr;Kq22+99UU=$#Aq=EgBOO4C_+OaFkWW!1O3LwstXz!i+&~(IUlm-zT!vgJOt%>r+;!rB(OYl2F-L@3lYNHt`sVH;GKVPk(NHx5x_K3kl?f0RauG1)oBK)fRZ{Kj67ys=x%8G+?(L){mmpINKPsW-c<5rNX5XN(@(Y91e2MmyX_N3jfZsad8Kpx}Rj#f~jP7kRt)HBd^;wo8CpMoOaYo8t7IpTfpnRWAKpK#!<3XWz<`^9b(Z`z4>nZ~Qb+RyPmma;M*$^5fI^2)BgXshuQ5WYP*#wb1!qo*hz`GRn+{2ZvROg&*_PPicZ+8q<w)ROXdOa)<PBx@F$aQdxqVm(+NbG-IT>FE!Yffdbv6A$T!nbZ!w*LOnY)W#!LRAcvi9z#unCs><lHuGyW4#os`F>A<mUFyPSJJ*Gc#iv}OiLgP%>9v%Z3>7jtP5)csX)?=Ca*3xqFE)wPt_XsmGUVoixZ4lw;w@%`u>y4%(1VEs(~A9C4q+>p@tD+u=U~RdpkvNm00dP<k0JK`VAj>kI8w){iWSEQ_r(L+cx#36>gBW|er}p|Ko_Quk@%GnXO6+ea1-&k8SyQXHt(gZk+{<5&5FP&Q=632-PgHW?rZR04U{XAzp7w<p0%lFjCIGufWdvkH7+yQSD304+NFB`9L)DpY=KEkS$RA6Wa>Og?sG>_Pd*l+&Slm&E*&J09n*HV9VV7yMH+%)zr@5I<EO|eZ^ci7_{xp)R3+%Olobd|b{a^jEK(6hHvlApOy2`o`ZtY}wZBVoK3V(yFz-?^W0a0Q4d@UofqK5u#)57$nCO4S$L*rWQqd+B1Rqp({KC{{Nz#&>Q1B@Bl@;1E`<krwv2PAR!?kRU-8m{2K^kt%)0Q4r2%fG4ytuCfRSMitGwtU}!>^^x7^(E)DTgfPHYl#@yURA+022?&awU-&ItnL--hMkwA1zi4Sf1a%mv>L)05<`|tpL)&?Fi2StV@|AjHOhNl~Wc|BcxFQOihjFTL2U2+>qG6rMT#6Mey7YGkz5_(s0V22J{F<Cc5&}inJ5V!v4ck+sREZB_nTy_J%jc=(Xkti6YH31?4un!Iv!`)A_Rf*pwf60rRyVCYUk0v8-?!(1VHY8qp{2s~nZ>$Ajqe>CyUH%6fx?gqW4e5rPX=LlPbeKHq}ra?&R(G7YS)v%XWeALiLAW~4#8>olMTv(TSk0+V-w3ALXJU;~&?d&``~oP!RdCV444Z@t1H_>`VbRg`7e=|s!J_)PmlBet*YF&mY{s!_TfAuOk^`|$LJ`)Wf#mH8(Q(ik`Kq=Q7rS+6o;XK<*Uw3LE*(&8PxANiQqr9yhafE4CRLBAhXc3B?pD2yj9%{tKHzi_QPJ8lIt*D;XCIxw%L%y(3QC2Cb$90?R`Tpb0i(C5%oK7J`l-*ulQ$Kl5*^0eyhb-QNztQb4?w;%ze1@h5y6Y{hZ@|W%yJcj&GYhx2~YGsrK@@T42lo#?lkrD<&S>y-ly>36ueqT8pc&HAY1`J?gfGfSCn74vi!#sR0jM<h7wvM9Mmo!Ot`N1~z_GIJeJdEMDr{qszLr<Lf3r|JH8$9T*{Q;XuvY0j9u0#{G)k1TcVq>w$S4nz5y<6YH^R<*YP9bL{s*!JFI@Y75816VHM2j-Ak9-%AwYL$$#;?@*aEy+c0X>2eo?RMXBiIUNJWdvR4rW{>Hiy|5h^#p$1lu`}C+lyqWWFN;-tC4NClw?76zIAI%A!AS;)h%D%LjAQ2K?$$CMz#`x%IM^xg<1e3WaO_P8u5Z$(W&Qz`;zb((i|jVDMuZ{xo1nRd|T0Z7??$h`7GT=MByH@1-meE@U~%l(fYK7#{--t)CFXduBL9@}W%IL@o?j>ALt~IJmKvl(gq6PkyfCb~|MTL}Fk}qp+o$9d)tRgn47>sE&)2L5b&D#IL1-<+F((X==1NlBt`QIo;^rmbld!<1@o_1<Xa=mk}Rk62*+MEPom>Neh2)RNjhUS_#bWBG{H9t6r2@?LBdsq6uK5Vf-3E3_9xu_7RYqN5A{+um{=1iZB95an@AJe5sC0VJno4Tmc@RxpgT(o4*QkX!-R5r5_a^O=0l(7Mz7{_&eeC5h#}rMy$2M+v}6YnU}8LDf7&gCB{5O+;na?S*$f--dIG6N6+!|*1z{s7R*%I>6J5ALRs@Z1#Y88U%IS(jP+su(6=kNr6SnvhbbOh%oviE(|}%Vw#-Of^4YW*OoSbAQn!@1zzp7LU?xHn%o`Fh^<&=a_4z(+nY*rg8|?8@II6;LEE7#nFNN*<(j+nEomhg~6!X58vPkC$iG!vI%^qwl4@I<k=u&?F?V0KFsSA8hzp39J8(d<Z!3{IE#YB^)VPknH)o3nk5H5HvWm3-(VbscfP-cNFK!d{@!4Z`Cm^%P=`(XkoW{j!IX~2MBv>U5EYz5F){@JhtK)*8k4ghlNPn`kGE&o>m%#D2LTL81g9hmx2;kc9(y8W;L6!#vY+tYv^z$}-wjUaACFxI!9=e-H%Qs#Uqd1UQ5TYiE){`_Jl*HVAEZFn)06*1j;G3$2C42i_pu{XZFyiPFBhMS1fPQ=Rt5tljH=RSKZWt9}jN6$(^3ce|6sW5v?cMjb{K0v{8)Nfz0eC>znv&D=N%fo;nyoC=jt>0J_%5@MwZ&2iFOHJwch#<$7#Loy)JH=E4Dcl)ukdIkHB0Yk%JbAwM!xrL_V2($Qqtk#MLAhJ)J_XNP5%jC{>s<tWtt5a&iX%ac(;`{2xi%zEyTghjAG^hh^WG1;tS4r){?QJ>g<%8mA}x*&Tfq#qCh2y784g|?<3N=JGimNvlpZWF50Zm+Sz?ml(6?VHUatkoDv1rnra_cc!M57;LHw{-6^HH;Su;*0zh<HdXxs{DV`k(XfELOTy9<E=N2lG8U{jAlnTxpLo#2zv64txtG70$J5B~u{33kRC5*GR~0Ov0i4Sd=O=Ftzc<ETCQgRHiAim1-3l-Gu?87|pHTT%mD#t<+fq)a~LFNX7FMb@v>(7m%nA8CR7)kS1NE?;PT-F7*-V2*Jwn_!~9uVjSoV48K$buM2^S#zM2vX9#PS#0$*74pb;XawbBq`NY9?YRl&P^sS!o1Cl^-N(n!W<U?7fbTEyLArkdFyl5b&6#}J1$L}~S-j&%G9)pHy*j(i{<2JBUB*xx9){@M98mRJ^$F(;6V7?Xc{JC=8Mfk_4-r!T9OrxtzZge$_6d}#l1pemuBG7Lk87SnfE*!_#6Iu^wkZVNhkMb{<4tVB_S_z`_#h`y9y&ZF3!bS{nu#W$`-)5mr=-T?0pPY2qODdS@v&FaotB!Y@sx`$sq#(%{B=?*S;oj;eg9gz9?ISJm{RWo7LEC<OEc3#HPOVAw&Ixzo4^La;d3do6VMl`J<uG~1+yzf(DQe0)ok=!4;jwF583aB$qxzVB>&qZn*qaV8?HTfv)>A4KJ=HKN{Mt!8G^&;d{V*8W&^$a-koZae9Wrte&la`8)0+TS2M=mg~Nd1c$GWh3dbDtW-yV45%ecu7WL?0f(eSmTD{oJCsH}IAUvARmla5_^X5ME^;yNWZH;-#ibZY=i4I!^T<KaLAn&U&A=WE`cZ9y(QhG?_3nhW(q>V35h4mhtjkD-%aL`?EiWNPUoek!JTdWv;w0-{`O3BtQ(E@VZ38kdg=r$0dA4|d1snkO$r!8oDYl+d+piRX2fs57^^pSlCef<X)IofV(OjzaMofN?jTY7z;n6FKFbJ~h498=9t@6p#%R(Fc`8G|N?INWX7l&Q_|%1%LctB?cLU$eWO!PI5+?DU7(iMM?=Rd2D7N4><1V;7QQu8H%;k`kQ}>+_Nly@OxFe}nuWYyQiLO#kJ0OM{rL?4>U$Wt^K<`u#ASR?NUrL;5hF2a|n2Fl;+*a9hF5_2Fp?n0bBE8#wPBI!%(6Bba(?2MyHMU<PjlzkPc)FM`tDe%MU+r~cj>%m}!E$>9pkV}Q+IBJa=_rjwe=TfuLHCf_=P8B(;c4DlA0!D<Kmp-$9o2id9x;73iw_WgS(1Emknh&%CvDx*6xE#AcMSSp$V6h4r4vW7-%qyu=U_k0Ur(8;eM;UGLg3xJ0aXkU}@-~#OcYz7PnLO&v9Uz340gNb#R{<$+AOPS$NWWN^ci{We@tij*I(i>O~yeMn-*=>jIj7-zP+P?n?WxRH-Zmbabvq|W2gTs3%tHD7M7G9^g>xE*6vrK#kOD6MiWw<0~zdVij(9uxL=%={f2J~S1obx3xc`KNF$P#V<lWQ$LSbmwJGTl<+c7G*JNeS;{pT=zdaM&)Qr0<PMA5e`LM^sl!hWvy-fEu=fTA7D7gt4!sEK({jKe9nn5Do-pP7(@Z!K0c&QSs2reti;1i_CYoHD*LxEE;po!<HeU&H0j?dx%?cZ91xX2d?pWHFOrP?P+SkH5&sGHCm)ww`hC2Qhc=UL|XVW`u+F-Ch!!}=5auxUaUif(FQYZ2D6Az#yw!7w}&Nsh8Uw75y1yAU3`x(dHIQNzli4sf4wRc66$k?*q^{mUIVx7j}5N;h)$lG@xuXJ%{9TzJHZ9qLpc9l%A(I@hwL?Qy*G55st9_x;i%qv(eBA}8|}^r<!d)gnYCiXq|@HM{wUNA?uwtrjWr>z^ptTDSGolyA98bOO0GG^sFe{YBr|pK@TSvX-~FB8x|e#;yLscFBrp#Hzc=TD%9_~Z$7<g(n=v38K2#MVY=zU$G%xqztV@{<phXF7m4Xm!G52wzc>+^%;VsD|n@0KgHPCfvsi}*uuWGrc=R*bBbEB;Dw<^8m5n5QBb~e^T8MmU$bqpdsK7ZR%G(FJuC>N;=t&mHZ%?Oxcq-PN@#jMK>j6;b_8L8VE^DqWr9$ogZWpG)tvzbU+v5e~|=v^$^QrYs@(x$GZs6=L3N|^7|ab^1qy5zo&y9^ul+hO{pSixf%kZ6vD2(EF)MZXow^b6)`I_WNyZ7EZJ7BY|)7EyUL{z2KY-bfJD@lq=FQx?YpDm18H9K=nEM0th5UV16`74c@OEhJHl1verd@`p%x-UJhU-im(hRNe3(^I9sKhjLcBk?OI@HsZGmOcdQ6_RfAg$aLh4G-j{u`wvhu$1COhxUs?%svd?PID&6N`C7^<=QAc^H7IURZpNW}C%>`gzr!F2o@WQ`cEh|<#R!7wNJd!BWDb|0j9Z~>?Z(f~<Zda$D+QLMU9YY^YN}AW8>9{OV+Lu%a4s&w(QhL@oVLY`(Jc=HdN3XPOBKSHwu0%8d~AbX`r4HP)&)!6C9U#{LuO&RD7F~$m_wCFr)aGur1IeMMb{B>48|a8U*PwO)1F&pYrlKv$2bZ@n6J{=TodQK73a#Ex1me>wUjkNQFM)*8c0P$rl9^F(5<_aNN*vdq32!!vl(G`X;(AmyEH9z2#$2+1Ws-%n7Me`hGOB1Y0sNr`k*>(alQVuGK)?5U2THM;(d`I7v-cceAXLMd4*xH<K|!m=5wb^pOx4=O@%~UqTk6_Q}*ya$bOO_<#WXF@!;`$DK<rCBd{hQY?1;TDwkjDk7m8Gw|AlB7cJSB0rog?F=136ZC}0@AC(N}nxx296x)ka+JRzwfwKlG0F#Rviv7c*NejtLq$&qHPQ6mW7s|m*24Q56HlTuboYTG;Z9K@cUTDu$9*fl`!{AP^0c#7u4wpTaGUW<<0u5{?Fad@e^yu8$X_RQG2XSeKcpLD6X}L^Ejvx{&DE2OEmmT~*6hnQYs3Iay)xO(O!FE|p5WI$BL=#26bMP|fJFeL6g-OJlT$)gf)5w$j47p3b$eU3_oaoB$qgWo6O|^-_N3Wo0Q?PKjF}lV_`{)W{zq{-0*=>c*nN5C}F6D)NMPR)~*&4QjNWTzln00zDWhp-sI`#HUHqBKjqup`FWYJvx4CJ{THbJX=asV<dELW2MUSqk6Tak>F#VPJUG9DyX1}8&GUUkPlhBxZ?<&Wk3nr#}M$6GJr#n`8%j!K{J$l#h+h|hhpi8m4s1fU$}TXVM5OiFhGTorRS$OpfcGRakZ1Qp=?!Bw;C^?q+~=Ew?>??gh~&e$_CFskF7ru3ku)LUR@FL@uR@#sH!F8R}zn#SU%LIZ#@4IVIh3)Fy(W9LgG<zt?8N$9@P%W)bT?zh9P045yBkIwJkAsGrKg<V2$Tk)g%bpN;&>9v%}&MOh7Q>MNHgyerFJPMFraqW2QrY7oi>C}}~vf-nBYU^MHw(Zl0t|U&Qs7N?y5eHx=TTP5N)`?Q7WP;xj=(eTU)-26PNyV5`{`qDN*vvT^M8TCeWsICc(aE|Fa?y{~-M8<_jf?wsJ7vYeVxaS*`x3_)<;hwTW!#E#714MuQ`(kl>41T<&z`jF)s1Wi)=gV9yEPWV;5_By*NoSRkt8bCho)%MB5oZ>BjD$zSvPN%pTL3WDma?y@E5~Pz-cGoWq7a7c0ULFSjtL$vYfFBDJ;A8yse@P<leq4S7o9g7kF?*SLwP=xGbYz#S~K>Wr{{%Uz=kBn_5o(4yho_3}($h6HUJrO{^#DV@lY5DLws>t1czYe-jVfMHaFNfF4ool$29VeyFF|iLHZpg0-1u6UA2JU`%iqvxZ!Eka4tgUD#NA$~Bw_8x){kOPRS4xCokzxP;SWR-Y*5^bBHZt=%icBiDMGJ-6E$>kcj!g|QfQ*wW*Q14WDdhz?;ZuJIW0gXg%$LcL=SX_T{6)8b>0fa$_5j4r#M#4dcB559O*8&9w>Ucz(7f4^zgSq;E>q>N?6nVoPk+(bNXMtsXy&U-0qfUb0fvm%qq)aL4Rm~(pS$p(iO^p2bysJMQf(Ww@V)yTt^!IQ%^J~Y@@t*SiSrKJBH*Y{Fv;Y!D2`Gl({Q#i^OnWpctl#d1V#TB`4ZNgdNw%ZL8N--i0fwEuXWRLMvIF+};CxLyXS9z*B^jgXaj=^&vrP^4Of~==jIwWMj#IH1X(jAG#Za>Uh)gM$qF91&iIs_#g#(bsE1>I&a(f^8%`$><bqRmZMln*q9{X+I<N!$`jK>Mh4Oc<JK>YBjvAP2@8u4QZN)>5$u(r|a4w)D6{=qE1iC>Zw@rb^KpYWDqHx%jn|87-A&J>`s*>(QHf@dq*Rw|emh?YWZO^oQ*HveR#e>7&Jp0n79I_j2*65CNM2;#L6ZpofI#0M@0<$w%PSc!y>HQ{%Kz0ZeU1;#&X{==74v+;=G;t_Ys{VMem0uW>kKPXl@cBNJVDe?{5}W+4#bsTJiWn3AS9QhdW3<tWn{B9M}qu%KLi{!t_=E9msweiU$<n?(DqF>#I2jb(?^mL6Ah*Jwj=UuCHvfd_5q(^K}fl=Vah2^;C^j4QZc^(#pp>Tgi{%EywPmJGe33&lsjALcPDW~4z1>@=VUvyiD@0+V-w3ANn{U;~&?ThE-u!-Ec^rlu)7Z#~kX1RLq;1V&kgole_4QiZ0tVEfu0vyn-x8fD=T!g2t+4{vk0uV4gJ$%En`wQ&<qI(Uej^=cz_28Y_AODUL#F7`9Mw+>zOx(Z3p6q@{8KIpf`$~ntBABFMIrCDuSTp6x4b;qr^<~lv{SYhV1l=<e0h*2x>is3ySgAK9M81qa14iu3K-%<9*@Xa;%CCB;zRWJ1u+xa~~e(sv-zGXl<j*tVS1@h5y6Y{hZ@|U6+Jcj(xfnyVLYQ>cW@@UFjpyZf0G)v@ThGt3ZskGX#;t1wzKg`ZxIUIPX8Jz|UU}AtPZKasEf>|Rzd@i}!mI}5)qu7@;;duGMHkJHj<6g3E@LNdlzbWuHo{EfjG>y;w0h^(+m^EFrL=&{tLUZn7V+qMu;d?*5aNol7wUjw{LF7>lf>Si{6mPoYoDdBz%07Qgc~LZe84+y!O7#!NK&ctfBPfyDrMWkPtzgFEWTEF^##Ov?n4*EmnsY+1ofB@AjaoyeGQl{$v*Z=ZINv+V9E}*^r<m6*P*SuHSHi9M<%0)m1AcWWlkT@Pj`^%5F$v8|gNt_tq2y!649%cQo0sxJw;wiw!H>oH(|{ps;o+&(26JQCi0gZO-q5%IUdmGUlI#zXlEJtD;|rl7A{3&3j~Y`Baz%Nllw;xg&|)EvP~Tv%7oSPXmDz2S84rnlF^!6uZuZl~SQF)qMWi}dQbsDCiyXg}3YN<zf~3jZ=18U>U*>S5(c9(V_(%ET9A+V*w-FzP7t`^^*U1-0@~dX?2S4Sl2&R=7{VsxSDY7a@nbp=5mnoV6;`jVI^<yCz1fo3y)*?CHZHGPbCRT(IK#H?gTWM#!690s)P&P^iczop6r2uW#E6ky#*9(+>RFX6m$N8P71a#t8ff=GCqhB1kg@jkH_4WgCizq_BQ|6f~N5Gh-xI^k>a8rLxH<LEjkmAvt{Jcr<y_5yBq>~DfzGRk6=5;DjF5Kurk&m%H_21{5DJ=!TZa+*};$p^-vz!L>Dz>GVb;)PbW-t+Ugh|~}-U2gtr-7LWO)!J=cff2kNJ^!CAXmsLn9T@#QWcJ7!8e$RrdJp!H_au_%sasZw<+d*EoE`l5fTSYZkj#VV7?XF;J4XF3dtiBDrjt7x!3fj*%F50exfn(3hrxvY;cKr1~<&u786ZEhmGZ-RKvNjK?dQql*w3EI`_14AC$2n3((-)jp#@|=EEHTyZtZ$6f?$L<uqVGFxrjR9<~DLD}`;?0ia*Wfd>G&HLcD7=9d4f03MtCEr8je90b>oiStrY==Q?~P~3Y6Z%+ey0JGfIHiEbn!C2pZp0_ESOPQ0X<dL<PZTSiI`16Yy1k`$}AH0~!MQ8L_|L|hg?V1@AiLqnvd}+v^V4e*(5vQGqmj@y)Gquls_E^fQDUgqzm5fx<#AwwNWJ|)GL-)`kP_P^#l_go}_QUkqV#bK&VZadI!iS*NZ>$RCI<lWPNPV@XrhI%vP-2>>B%<PR-gH#rbm!EiV0hy|iww(?=W9P~g)a%_cw|F54d@Y+yVdSf@Vpg4ziP_fMbOt$0$BVx!J*S4S+ZF<3gar@LGT`d&U-)Xx}KN;oJRx~E)Kwp3_3n+1vAv5q}u^zI4E?C164B2q`4zRlM)gtt;Km+#`OL>gS*}xCd(upT!N81X9c$!^+8s#m=%Zi62SH^dT^_aCZcgGqK(m!cOY8WN$fTR3LKqwMS{&z24`E}m+mP?r_mGE_mnK7g75wC9}tviXS^a|p(=y%N$x|ulXil6G{o#UYmWvYt6N^eP<4!@yf<{+aLF#)LRu$Uu$0q0<>ObR_)Q6y^*c3nFD<dPql}Qh+K5cZBf|K+?Q$AO+1E#xVG~UB_tlKh9SpVZxq{|vDQi%alB6Hn3tDV0r9v+E)eTn`^GSDQ-%l>(IaccT!{#w7W%uzpv>DKYDd77{)RFFA0L-`zOmlu;c7YvhU}B#fnvdR*UkaMza5v|8onl3nF|>?Be`GHYZT(h#!a2i)b6#;C%{6g`tvKgH)6_r5IUh4I#*r;P0!mq_X!)4WYboISaCUMcdk!%dnN$4YY)&EQKHN86X4ZW8+#a*~ASY2CT0C8fGtEjf(FAl~l?maL;&?n4+?GPLRSqOT_G-S<auYS4av>#E7mE10uY!w4{@PEI=JibOcE{9^7p!Q^UtOA+7^;OPoU|3rR44{E$QPeWnVo<hj&ab%Kbv^1e)#4)J=gLvqvyImYhBX_oB%xc!(@^aGYChAYz7R6ZMgQ{&3-GG`H*dTszA~$Wylw!^GO9Wn+^1m@7}2<$&yxW_aoEm+X$QGzM3&MFB}F8r)z{u%52P=!9*Gc)1QD@w5Wp#CMe2m^<FcdNabwK;n84|9D8KQ1a+ru!k9E%+t!$;tXSm6i0H6oz?H5w0`k5Z6Jot0ct_;hEu}|9z7Q2?PD*G-yN~H^oJDstOra$2r`f~a9P{ig29568OzBaTy!{fFAjh4kN|KFk1C9Ez6ii)9J*sl>f~MG(7)^28M3kS2+a}Uy`k^Oe%`Vac>uYOFSmg|!l*SKRdWE2vug!jQ+KMY2v(8Vi)Ynp0ql)(#gC?q&@+=#wLY(21r^0V3MDn4iCdBe!@`WU720c&xVT|H!pUv=FEaXvfG2_^fq?l{sys@}MXYl&GxJ2(b*zn&VKggQ@3QVT|a(Hwsmldp>rmr()x6~iHtug&qEW%No`mm+Pm3==(Z2NC;TXD_xIcf{8d41^{IP)DkP2ZLyppC1$qom2fXe@I*cLUB(<o$lwO!=kxVgxe+E?{!FLJk>VGnmLb#ER*pvGP_(9HD8!&R~WVEi6O4g=Mgs1b-+Vb=yI<>H+vsV6lDw9?C%JgEQhz{Ge*`j{J-_@jI4^rhtVHq#d%Mks#>+a`txs3_5wXe9sQR7-#|TFaqrxQ_O%-?({HVKoGpYa$^SC3?|kQ`{&MhEM-PUk^Nf1uYj2cYoL2rdL2s`iBn?Iux#65J0sI{u(t0%LK&}}s~hV^{%jq3-1hKZ%4&O%+=bWK?s}ovku4MXhrth-k1xZ0IlJg-&xekOVo^Wl{kEmYwftu;;mTWa<wN0c1Fl?a^}%w{6qWgx8qNDF$xFI(b0%YUfA}vhJxcF=NgqIs7)MZ7Lx!Y<KVTZRVp>UxHUzY<r7Zp`FF&zC6DAG>W}+fN3qSMKA_$_F1^dhpNb%KkYs}cTSTrV}hb=>FoAV`=_z<_^+7wyy4qW5$sOT(Q8%OklYc>WbYTO97?$h?R^?AHBFZ>$)etZBEcnWUwI6zUaSPxf5`a{|bW|6aud%#3*4^#LIO^0WVjlc#oruZrqr3B(y?boAI;$o>NB-95HeINJ|kOpqs9~)fx5uH4BV1LaE&L8H$L*5B4;2y&I_fi(=E<0qef$P1Y0adlt?>MTrUc7tB>ehZdLiySaQ?RWVG0DHTuRrR)_s@AYlyPHqh%1w2oOGIQLCJ@39h%~8Wq434C{Rdd664`5D^ZxK&YEt*jMv??$-u18_F6HeUvYx=wL2dq+r%h87XFUmjRD&5p}-JfE1rJlfw_-oUCL|_EvzT2mV{U<hg45vVnIu7hOuGmGou`Llv3%z{-#<_O{sLH%7*AB?!pP%bEm8mxYD}h5nEW?cDB}p8MngBbx<NbK7rd(G`-vPFc*alt*A?lhRDE{=~)a+G3yjL<JjU-QtGzGJdOdFN1r`x8GM%Pd?wOXEaN&XdKb&KRJL5U^s#F>Dv_C&QY-a*jUR!*t=tEr3x$2Z9j0H36+D&%iRNgC;2Nu3^jo1!zhIu`m+nH@mNKPmAp>cFQ4zfSNhh=XG0ssPK=mP%=MRXrTKCOC+$2nt7a8ouo4j8UZ>HJ;6vbF@BjO={h=u1(Fwy6&=+{=(4UaRgrLuV_XQf}M9lR#uM~m*)2eG@u@7Zq$nU0*1#{9K?{{c!KMkZ}2H`bU!)zR<+i||b-UrSk~ea1wrhQ;mq%{Y|rWH`ozxJ&(&8sqGs-ENpysu)2q9mxpGnatr5lyNJRts(jOncOX9cqL&-+Vy&H8>>*d8?+AfW7z<+_e!@ksl1H%aM~6#Mz=f+=)rXCFVzTR+6tyW60!|`>1(qNSQjjLm$ce1;(T+exFVCvi5|<`KKcacsLj7#ykC){-YiOeRjBa>Pl%tJW-H2j=f{W&Lzpks*>Dr_ycO|EvbUk5`?ZucZBdkwoElO^L*1aoA$03jCDMD$27#xboW;!uyQ90BG2g0bDM@goD`#<XTfxl5=Qb1$U$lJQ1k(rAeT$gLIG1V^o+4@P8|3Ph-E6-$9)_z$C`)(`LZ1<l;=kvvnf~kjKnYR*YfDT#8E%Rw-Ul2eX;VHY6F<%?7eD#k|MWM1E13V=fBhfIf9<dT?(YU&)7Ls|D%nP0O`zCh65u9B{<V^6&Nl`z-Uiqs$i;+F$FzO<UdB`on`=5GTOn-EQ0Xy*?M==a!2nDyY7pju2GD}@n53x1#0VY}zkc<X$KGTWJ%#O$%Cx+vzc<M~l>v{y`UUSy__0`O@(}Js7qB)0Ja*7y9aGUj0v-)rCNL4j8^h$>dTYK7i{A!(U}N?%PY4n$5cUpg*B$&m2t$3Os5&Db>W4o=*wzWQgJOc<H3%b`5Z+;|lTYP4PS|aP$<&)%njnnRNR<2lxl3Nin;}G;_{?uYSYDM)DT?xRra)-Zvv7bLBE#}2LsO*R+x434Ho~UVCO^!c@<Pobpk5<%4O=0kUkH8}f4T%=TgQ@$CUojGnQR6uP7ZG{V9CdfHl&|XJom!pZk10CD5iw~OVZtI1Xyt^jIr`N#m6wlgZ#>%YDmeep4i9eM)kgIO-HD6odlg*UbtS4V<i_9K+j#V2{;lCOrRX+yKy$n%s6+#Sp{`}P#3-eXIsZ)Sn(2OYH;QcURf%CkLFBeg~)e;oo-v~85kHf@=jBFL{lQZl8)qkh{mJK<g@V4&xqD_rpfrJe$f12LNs7gP}dL*$aLeJm5+JWpSp)9E;k1}-Co#*y@Uh((fR#57(?aMGphk^D|l4j=EJjW<6OsN=amT3DO2A8LO-f&;s`16`*`gREojo9*G}6|nhA~fmVQafE3R#uKC~Ti8g)fNp@*Jvovbw>-dOQTr9KJ%aVXo?VOz2^BPA7LPWk5xG@Mdt72@O|zy1%d->0Fd$LgNj53a%Dx!o>Vajw|s{OGyFaYlGD)<hV$B3v~zK8uuILO36>zJajMp0qpFjqC;n!h*t^v@A&oOqq))b8?x!fLOLwO=W15M@(Ub_}nJzj?F&dz-tv8&Gh$+wI;;06XG(KR|mKsLVT=aB{Mm-D(nogYcJJm*i?wDI$Y(33rTLU<nasDQdvd6bIA{}6D_E|cE&_CwH*5$IYF4&%9?>DlzuCeSkF;@cI9^K=m`zTpGlg#CLY33Ge<3?=Z~V{)Mg=8NPEh%*utL)(`K5@2wRPVF<)KG8d6u6y12nkU14JtDc7(e{2=!9Gt}ogX0ih=f~FQOAv2kEBnd=sy|*M%+Z<@fStHEHZ?_rN4O`3zV=3t{phpr1&K3L78^Ts3<1wBG50Q+8NXMM6DB@?D${s`H^z$9dZv|=mZ7frVat23|v$c_7Ts(`dchS!R#QiQ=XK~@t8tGpdai$tvj5Q&Sn<4&jhUOx~ZGc9<VoUcnD}tv?Z30erRtLA7)ma29edV6Kisa`Jnrg;aF+2<yoHbmdGlP9KrplsSitZ1Qe6Pb6lEjpiM{ZB1=#!_GGkgi8d@Mv=k_`t!j+tq@+Y1v&u_6souU~>*kMUD@l(&K>QF>)Td2(mI*Rdj9@Ek~~5K@syH_#($(`H|yR~j#k1GT?1X;F0VHp9F##f(uz`ZS<}PhwllS2|75ZAKFPuLzF<-D90-Qwo9)DldK^=Ch<}2}4g8e<Qk;6>2-Jl85$Gk%!vs3~JdIyWdkRf;8Ncr!76G5c&Y!4ry^;HK|mop(fGz%spT0n1N7fnp4hJEHsUl#(Qr3uit9CA57&+o-z!Y&Sz`47p8X>D+VCX@83(jr$XLqqK8}2ql3p09->#*F((m$Q=<f$(MyePMnx|{ZE*b-y#zW5q{Nj2i9N0Gott4st71kP4%gFw9=^zZT>QZgX(y6}+lQy#kUL09s@)ju4L_9pT<cIkN@f;<as=LB$d-@k4B37pPf5HW`PvK<$r#;OFE|b8kwkZm&J*|5gUa^fLFf7CM18Gey`MqCMkX{P2`*UuLg;e5k$1?)k}(U>w+!+8uHA2jd8CRNX^`qV4d{_9WapQV<ef-DZFd5AiX_y&FK3b9pu?z1U6x|H1*BwrZxMo+IlpH2`MRhC0=sl$Zol6evyDiM8a3Nd?t|Gw-G^5*+*j}cs>D5UP{Fu^B^?An&U#Z3BZEWje5Dl3^R+S5eto{8*Cj!?uj|A`0<qf+D~T-cZWPAzm1gZ{aa_38EgiQand^K8J@d+Q9rOKDQt_x2Va4j`eiooDuZtb~LiZPaA?YKlaL_x7`YCT(PTp>tOb-<U#}*VMfV5ydT5DpQc4GX}6N6_M9};YAVoa?zvS1udi3!ATD;mOJ>4o&=Zx>f2Uz=fe-O5qGL&@hfU_f$U$Nzw2-il-m<<LDV8nktSt&AwfB~7tiexywWz1IO+T~z*H8ETjCKLu*0UG>qXh`0J^U-fqA6=YkSYcoF<v!+{=Xu`ExXpS;$tN{6{IPa&|=I_CLtz*tI5P4KX+a?&QM?`J><6F0cXfQl}rn!C_5p3{E<qpRXs2R}1Co$NisWF1BNXFw#p@&GuRW5UwN`c6lTSBni5^kNZN59eHgkfw3Q*ey)U9-%kh!K7YW!-{fxz|nba4UHEAXa(`UR}o|(nTM<&stuR(5w{{jmkS2X4Gd=<lpyaX!0w3dFJC<;0t~%sh<W6nF#l4S|1M7ja476Z|M1HGyHoUOG*n%eyC(6F2MMnXDIfB80p;-ggK^&WuhI=Ih1P}h9M@Ma*BF_uSH(A+a@yt5(8rz<tg3lqKmaA#vAKFbylMcDm)tie6JHMZ<Q<{X@asjlBs<sdH#)tYL|mE23Pv&JU25Xyp8xU1Sn>VCHB*RNgeoui}F_Z(#ke|3%<4vSw*1CYNv?H6ixJy!yWW67@}x>$OH4(QNI`V@EE(XZ~FNDgC<($OJ!LKTXAf32Jq~tt?K~V#8H?-OQIJX{V3OHs(Pnx@`!v&6j3&@G&Ek%NnD|_)ho84e?>tB<8z<PvsS!+%t^#U=T4EuTodGtwV`;l1V3!hd#_`GOb7}UJrwD6^AZJa6HH%vt9*?0k@<nO3zjNhw;87JZ!u#?P)-ASq1ZC1bje-QW+V|FM@HQ`-a;~Xry-dLO(X;PJ4iNaKBd~;rzmqvbZ>(_?FmhEy#q4Q^oRoNu1)Py-U%eQ9U<;(9gF3SkQ!I%0;C5U$Vl&3ln^C{11L%`;A%njqLedR>PpxTsVq#K{odHXQXBE_fekaR#X^$^VPgR()$lF+pdj!wnCChs&l?p^RHbYHWgy1_Q*Naj(U5%1hdbzXn_;3SW{g?MX}|zqw40tiY(>vk;@9vPJ-_nl4(M@fC7sdBEy-8W%i)f2vwX~8>5Btl&{8+(Hp50w9D9gmPXl`NvfR%$e7F_9SYLD=HWr-gn6sTEX}$IwEkC;+e}4Fqn+Ve5hc8*h;L`lz;j7ywGwdmK3mV;$W?W}uO^9hH#O0}n%UtS14?WhgY6aw@XC>JL-$blbTs`JH=j*}NuK<~bTEPnBYcos_EoO{B9tI5YD|`rA{l-#Ht|RaH2QjRl;mdD@Gb7gF=%SKi0<{B6g^$9WkpuadF>-)?FzIKty7y+-3RuK}N4B8TfF3@%L+#E2&s*X1tNQ9K_<Su9fW<Hq4waJyKAX%!Qm{J|GxD)p6f^J5uq$<925=tXTPQ35FLKQIuocNr%Z%<ZlHnkkG0suR9h2sW5KUf0C{e^a>5#qmBtQ4aK7TP^E2|_n$lwPx9R=8G(+9c1VpSX(Ljc>q=z*;!nrOzYXf|R)evD=z^ssvfC~$Pztq3-KSIHM|^lr(=lF_^6`&vLyj@7mq{sVjx-Hf*)EEHY<&R;49__Pzrqm||H348SQSpDkaPU;9rc{k`<){<R>^@<ca{x+5=_!uH)X8AMtO+A+NYm~bo=g5f`ykFfvCf@OQG4JZJ$pv$Edf5aM`h9gEbO%4Hdq$%8UdI~8q$I(J_6!zVg-pde-lD>kkCE=g(2oqQO1|A@*mPf|fIdEGHUoMj1$lpoO40obAQ|@{X->h@DHXUNIk3O54QlZE@N~v69<3uAy1s*8@bX&L@6#ucGfW`o739%e6J*#5ay|q;{X>xRF^^)L)Mds?sTGx{Df5jiHBsv$g39l|BgQX~O^05~iY$y|(<`#)=9nc0IfU}ik?B&LNlKcHCY<|fM+m1T#j`2gwhp4r@Gs%8S5upohNuygi(S1aI!+0xutETS!4xH{7<p<h65()eukE+SR8<$CXv|Yxnn@0-g(jA?70Xm+`5zYHU+b8?d%n=tf##Ynm<=T{-)Y{Hj~UI;{eS_~NNfV{+zXRuQLG>wJ+XcN;fxK}UaQ$}1u`GPMNgvPejP)<7oATkklE~Vm*2ZnE|QN~W!sMosBa@|g7<31*mrOkFr2A!np|OQW8RD;(l7#k8_A-?984rZQ6;Nanfb^mM-_%gqw!_fRxjbEKF$qJ1-0#qdA5p0ZVZ7ATLw_+TH_w?D<UD*i-8}<x!pQ?2;>VHfaacrW^8+pKgL=7G1x~usfm5}Kz25m=WMZJ^v?GEdmJSxzr?l2aVL(FL8E(09Db|=Q*%*|qa3oJiIa%~nmVp&P#n3V<C;jHmj~_(JbsbY?)JsRRL<K;R{OA}m%WMk+LSe?t)Rj&Sp4Yje63@(m3T=OgC-t0uoz}j+%-d&gnED&d<${U*R*TWMj7-Z^oQw&w|zF#ZLyF?Wx<SN?~r1y3G&985uFk0!<rGjBVEIDgZv0<p38|$&*cbrWChvEQ~H`wrlp+FZHDQtVg`<~%ZC9ylBK)pin|8470Fy*lb$1)*C)DxCjNNxSdffhDv@!ZTSx|Pgt~njbwCBlb2Dsm_@WRzA{hY}BspARP7JUaN#q|icrPM}l?n?i!vCgkIwKiUw15op7LdW}-}|8s)a?b?EdSs~slfLAdmQD*?}HoSPVk`W%#K`!cfdQ=iKbeEZ!NM5c#TF#M=#vc+zmnJj~?U&*S6ZX5olkHVg`(=oreJfd@vB&*htc5B(YAmKXk)m9W(xk?AIcEv6{{EH28Z!dL2kNl99Nq0onG#c0;D=JZ;~9#4%nwPB+$o{MqXBtW)8=j@79kxe2c$)%6mwgIA_-2S_IK#bme`XOBFM_|TeA%;-m|-v;zZ`keD6BzY^6d<gVCMUrcYJy;%@qB46@Lv(*7BWWb!UuT)dl>E>my|fX%cO`v*G-4bfT`d_>5&nQ^*otW7`gt0(zSgn$pS=9A22Dvf5SZyl!0-FP>c_~COzAT`zsMqYn_&j6#f&kLJPa6u)|@Y?nuoX*$)+BfA0ru$hd*Z_**KFIBr}`(^r10GevM>%f>K=44{f!IOP-tY0ZHH~YRx?B(+9~UuT0E`v>C}Fz!*0miQb-J@EKYebd7u91CkKm@6tX<3>wN~2fK*ZYds;MKJW)UH*}jNTYznQV*@Kco|B6Xdgyt<_`{ra$UA`r+(6g<UdJM$WryrFV7)iQjH=dmyy2nVdXerE6Q&i%*H)OSW5tL`j=X*SQIEV=&9-rj8!J6r86V>$I`ln`d`PdMsX*o!qgEfFHb8A?&UYO_oE??ijbmyOVOM45L2tb6RZt48=>+R*XFiCIiA{bi;2qNw1FYdgxgf$;Ed9)_audtCj@hosF8ftNK}?mSsb>bUV5J0as=X9kke>`Wy{j_K7e#hz3Y+We*g%1|)Y!Xivd-ONAmtHQSp0Ri*2EaMV$5||Aw4^H+d4G8vh^4jfeEcLORZx>2R7bHRw!njpMM+*TuMUSW|$|h+~z#G=wZO%qGYEik+z~2*HO${=(TmS<)Nj6TuVCJ$*F@!?oKt9j`6`YL+NwPW$>@x3)2n73LfiuM03bPaE*^G`mH#oUocN2L$`2j>zJaekb$)5r%)gMr0Q7j9(+f*a%MY;rE>lMm7ebELEDr%l$R6i-81?X;%1&LwNMNMH$oiphX{8z+=QFA!d)9kpFX0z*2(6fo0Wc{dP1@damHI`uDe5m+3y9JZkv(DbhLf{0Y@JAP;4AGR)|7XjPL`=?j0Oo>sVEI#zd?JyX}ok2l7_ZVvSw%*ois2X15jQ)hR|0Os6lxa{O|*gk#)_W9tEac>H$j82(5Yl6H-{_5`Wo=x#70)Q=g=2t%5(h$Fv^_;AP;Ge(Cz4Cs-RyWvu$FQ%<X`XkAD>XE+o&46{0l6Og~*&^mRHgu_=B8PX%5+pWe=M(#K_=b4>CPj|Hm;&wV@~pV)xlgvTxp#gHmN10*`kc)*LC#x2t~_*48>e6ESknwe?Z>InQZ&f?>+j*coP|j5@an5YM_0&*WHZ8UoUUff_h(wl3Ow|!m`EnK70F!O>}l2S#aic0Bz;ibm2w)QeX{`Q-UO)Zo&4CR#I7(u(HE5?LpM%NlAdciC>WpHWO}Rj1D8VmuPw3eWUMIwcpqV$WH|Yp@_RP&`(B4lrPT<mN#>e^_)Iz8(csMa$CuC)=4PR{0rntoF=15TY+t??;2he8KS^$Eg|IysrN<Dq*E4In0Wi6!K{)W^Sv~R0oou-e{a@D=ab%HrM_~1}NA@WU*a+(1=+0aji=ie#-%fA=>jV8`|2)<)IsJWm9Gk&yg##T>yrWXy+FX<_qJa-=einTj@PV4R)H+542^I)@|Fp{qejkLPJ}gv?jYneEZJl8IXHoR?8iWx|2=A!l%BS)jC+s%D<gQIFO%TRu*hqeW+$AsM%@86^<lZ+SEKk6u9z!Ng{f}*e6Ao~L8sX%HUm@)GHq6tw{YKan)8t1{`Gwp>K)uFv8n!}6zYshfGkULM2^|wU^_EOFc@!rHH^`&pV+MKD&nTXIVH2CmCkGVM!W|{K?KSSGxE01&>5<}N7~?@5We^{v<W*1XLv*7MU$&-WrGfFU>%mUT3YB!Ezbl<n;63-qX0}K;P-k+SZ_3$FGvC|^ZdHkWs+ap($D}Ut5ma#V2OrIj`O%oJtPuH5=+kYBJ&*#URNQGwk7!E7S8|iQ57BtEi9FQ8Y3oem=TiZp&8$y|28`Z9G$7xh?30gq6~>W|xXmH-Fm`)k*Ypw&q(bNS?_dmt_Q5U<xUJw(eQtl&EA(2&WVDrd(kTPh0YdUW6A1-K9bsR8(o!SKzjoTnGTCs?zQBj`yx4o2J~R$-8dWz0#IWExS!+VPvHX)tbrSsJz_zW!wpwRKN-D&h^3T_4C_)JDay(-2P<wVLr|X=>p{S<np4&Id=EZZnU9zIyT=Yao&n1pC!jrKk!nhUTDl_p=tFx`sB7Xy6pFL@Jsv9{441~w1wWIWt7}G;4Ebt6;ojpnx)W?;`_c~&SUyfj(8)n_M*(V&xqk^NEHh!_&1e<n(U1sL$B=tkEk9DjhN7DPO@T#(F&(mrE1%z)OjxmZi#ONu?N50^DD?bFP6I0LBm=i9rzP84MHMQjX9YjHxIl-ERCYF9HmROI}XSAyQI(nu9Ww=E1+{8nsVJ5Fb!}OiNIJIep71G$fg~w2n-3ilXnoYJ^je{|fUCbJCT9>*Y+0JQUV|gjp2qHYyMS87c<~@bWP1DSl=$Xs{lU&^C&O1yZwZDO2sQtM4{OvZwx@?OXVXQG72J}ecz<XjpeM8uaWIX2S;31N+klC1H5&%J!qhpADLzi{kGLFn`svg8KTz&CeHr`HQyly$IaGyJ7oy&!KW8_+8#F^i3G2DbZZif5$RLpxFYjCYJXR{&@%GBoBbjNk>mg5>ER09Eu<f$r>pXXw#8Dm-TFko=paE(n2_LZJ0Uv;U>KSc7qjwNXi#FUkXa8IUklB79zl<DMSA?j>mee==5zSwbEcY9$1DORK*1ole=>@j`{mGV~bBxJ8-C6A<cUh7z4uVkTtl)@nuC2<2iBFOYTdZqQ!I8gih6Xyf9-wgBq6f;J}<<o!;z7m$_E8QsQHY17tSA5p#d8`v{Qa|uPWuPxCd6u3l$-xAVB3)Uby_v5`X&>q45HwKBzSvElViBa_mOO3gL51MyN_dO=icF<=4K<^FsL}gc#|(-}C!KP(Vs0bcsvx@T(G4W=U<OxmmZ2YTV#MwD!t~B!#Q@~_{d+z1RL*S^J=}^O9lVP05WTvNIb~St0a-a*F*R-&6}{9XZoWk?flkVZ{R@eUo>utI%`l@?F(Zv~_J;vIe36N+Je?x#M6wX^@YFzZ2T4i08)>}ZhcSAsjX`2YGb2H{fo?EU%g1zPYCpEoM_!P8ZH9?tjBczQoCfqrqPs?~iTi3pW&81<*L-xMzSgmx)gU2er8b1%g4I-nhb+#wNV=T#frv~4QR=Li)NO`&q>347kit3*=#eZe<Cl=+ok&9Mi2`_vB-G|GXOY;T!>Fl7%C=h%YY0B2r$Z2B8Fo6W@i6Mr{?G{QYj4bEBe81KI!6f0Ve3A;rs2No4^SoHiG%RO9Wd!2^KsT&ix?RkY6mQ(U>>k|M?^<H=5-a0o{>L=xg5}MhLuH@_c#jU0ZX%dvUo0BYpIS~k<4{!<C(t7YaR1lR4|EJMHWX=0~=IFL963A^puZZN^*DITgh?walSjP!gbx2ncgbKj%_MP0BON{wA{oy?Zo>f;|0&~K9tYc#G6{7WWhU{S`X#9`%Y+t!4U2ELBp=w46{#Hjs+g-Kc@i$k{IAhn<wV2NY=OxA8Jdsb%L!%D8?mC16_WkP2oD(H9F5=`0c5qQ%J`XXa2%Rk?{uI_-k{(W^gQKO*bmhglo0X9DdkX4)RsQ-B0h(--G#D$DDqUBNEj(x6umg;ZY2C+!CV2)!4_mi(uK?h+u<P%6K@2L(PC5K8eRJjhPW_MKT^|3Oz(Jt`e2Q91BF&+!BKAmdE?^w?H!A5zg+m!VHXx5q=75-GXBgY&XHft>EQ@LFp-Ybsduh7roqcSxZt9nuUYH=YA*MjQSMI@c!>$COPRh!-g;TvA%v9Fk~e>M9?;p8%sc3AJ_BK_WAcZmZlZ5oM1|Z;sT71dxn5ch~YiZ8zT8oCSxKOnyPeNLNFWzRZHsGQ-UWyS82O#G9w@{Fs4x((#<ZqSZiXuu{czxJIZLoLlxiGI>GYRM36Ko*Br?dpv#<YG+j%S>WopBVHg7Dg6+$Q4`YL3##nDZ4VXlEKe#Awg)gmC;kV#x>yT9u%B;4KxJ=PRukjy#jUEP_1_S$$$IV08{a)DPW@1Gc(W5x)lV!eCqouGF$HsX8&yL!<4xmjUg*mh&dco0;qKT%Mc6_U~LN}J3@cOKh%LgshTD|S{J>yIZ*Kd<~)XEZLN+J$Aca1F8niy{^6UC!f_+jJTdmRg7Dh=?;Q7eI~d4~eGnV>J7RX)c0jD6@E58P7l>o&ub-7RJe`N?TOuhd$GnJ#&2+KeQ^<G`p}$6H7S?=&P6p^4-T>6Q91@AblapP|gH(!CA#cqkkd-#3tnrU#Y6t9@zAm-0>^!R?4}U+Y+$Z-m4_lTv06Hjsz9SUqbgzyJ2kbosCazNd)PZ;p*DG0(_`8Q5Z?Nu98<0F-L@7M>~tyw)+9--s}3)jTLeI~JP3;f-hr$}Gzr^t#P3(GxSqtmHIcfG^tJ&>ps;=PTuFc#NK3X><qlxb>CJ=;fB=tLWuM0`x6<+2ZX>{Ty#x>IU6r*yxF453%fNK#yLQ3)+Scx55|ei_XI?gL55o{*yei_BJg)yB>dj1e2?%ztA>3g2{@Q?mU8ZTV}>UV(i$>US3isjAz45xM?Tc<=KbJ9P2}GJ=U@62IQk>CGiB`gsD`FJ*GQn?ZMBl067YtuRy*w!}Qi-#t7tLz!1#BhX~ehEc@g-BA=fMT(xzkoO^_iW6RWM_^2ITDtr{~%p%CgERBvHK3bkIUz=g;T1oWABiGPrKo6hXr*_AI=dJMhRXX(+e7=?oz~ZNosKse<E7{~1l8@aXsF9D|BB*(9hF!H2Gg|9s2j4<@0eF$2#)qv)hFWrTkC6-qMU8QeN+OvwM=UxD7Lo_?K)WC@Nhs*suN1FWdt{Zwh6d6g>Zt%*ZTcV~SgeXelZdPtr;1)P(L^(DMYFLY@?$g$0f^mCK!KyvZbh)kzo3jU-0(~A$>{0o9djAhdvAvS0G~uR<E;n_0T_Vumns83?L_iuYk7Ra9z8x*BRfS@XG_W(Lf6Wc?1C(*;Vfea7!gt?pYj(o^RgoA*J$WoPoj^s;Qi_ZGVzu#G(J_koLn#mtd~tNq2E^&LU)k6x`$GcuXU_BO-k8E?MW=Qa+!*E<U168@-fn#7`wLG1aoZEZ-z||R;uaa<7P9UM^cdYmxvhMzW|bP50d7nyleqG){rcI@gpgb*u!3(s%C##Y_Kk4X!Q=W?QV{s`hEHYa)t@yyn;NMYk~}0LC%Mcr+)}?J_c2clREnZO5MmMwBO58@bC9BPa!~#kVt|Z_yX7zg6_S&Xy@_FH356>jagxkGbj(unUd|!L?+Ed6U}{PB!pA2;@Na=TL;mW1CVIgtC>y9Lev1tMVC~;qyYXpvy?1j<f*=Y{anxDZf{I&bU}*7Jk_O{{h*p?f=OG!Ol8RbsZ{T|j@ifO3(Xy9uIYl=Tq4l<J2z=I8l{J1W+7(mH^U@mgmb|D?TgKT;gAj2-mckiMKT|vMvqiFx^)bxUUWXGNM^I;U4HLQy+}T0)onjgroN4^`QfV>V;90<z;LR{Rd9vkjd?SYNW*aWZ6u56axjquMXsz~X6BQpoK+AW4b966q}OnBAL#n5g4*`QJY2;hH^xAREd!`@t&5NMRgn<u&A^WX-EJL2-M-KSXztnQ+Eh&MG0QlMSq2B8@}>vT1KHVNp0mY@(L3As?{So@{1PP}$DKGz3XSe5@A$C}Osz&ej&jI?rd^g8O##<LoF6!6T|u9uhtOAeaM5z@_Qk|h&f7^v`>>@K&58Ni{57Yopu#b7{OIm{tz$Krc%Lz7!h*wnmQ9V<46iH{WUC5EPyIE!>&Z%8M$bZj7<G8tXH(-A3wcx>%s6%uDdw6WZ!8+o8KOQc8qqt{H9R-SkFe&soXGTCj<?i%*-BLUqEW`V_oUwp(_O_39Ho~J19~Lc_tU(#y9T!v$y}e4o+FvpH@bmW-l5a<TseZNU3PFWeT`)BMyT8ORP&<4+--)<CVvXqy^)N73z8hJ@GAz`j3n}7WnMb@p}ZC9MrhinGm;@i3&;>}0U4}jz8@+<-CmF_3jlsp7Hr?Y$1za);D)#pJg6G9Bg^3(@Q!t&DevF|X$NX(1VcJ{ha}Fo=mni-8WIk&4z!?m7=iZH7!L-{4!vf;03Y<TO!n0nXfu*n$J`&f;jxYx|3vm{6~0)_=6M?YJs`aS<iJw0W|`ey*lx%)ou}>lk2uC_$LYozkUx8Up0z5x*Rfg^Bsbx8zPer_cJRu?cYtIvpG<~}arVg5h!4F9#f*Nw`fWguq|Z5DLXx*4$%nq*QzW?-*@NYgDJqjFHAMGUGLn?;sMjZBWPUhf7g5jmuA~o;MvNn*t0hA!!XFR~TM?}!Ku?3#*E$w6l$RgYplR<00yF&xg+<^|;h+d)=vBQw*`q}oyW1Btek~S_Iptx?5WnVpN&h^=t)Mm?()<|Ic)S8S3)J@7w1Aq~RI(2(UHclz_8g^nXWvb<5L)z`@c~KTDS*v9E7=Dg3Wzq6X)}^V$}w(061_d!;4{P+)es0iAnD?J&dAHpdizCuHu&pxpO8==1;qZ~W%3%bZF6j7<>zzq(TpF?-D<9hY~G11;099n_c|6`EjwhdA?v-Ngj7|-!wnbp*7I~vR@)eGMjT&TVM>-2BPL<<_Vq_)b8uHYG;XZ=aOH)JlVZ{LIP#&}hNgU(V~kp@fI_kr>!BB{@BYqk-9J6(UAS?e>Ypcd-#ha`u}p08V=?a-r5IohA4&%iwqogL(v_Q7)^*I@&7vx{3N47KnER;EJa{R&@D@3e9ix2w8t6K%)YLiGhqByL<Dr7<xl7iuTUFEYh$}4aIvZ<3j9Ve*I=GOY9lLEEnjYMGh>Hq^R-~oOX1q%=(zAG%V%GWm$GO3!P}J><c?JV8kIs77GB_*QiA$udK*n`i^A?b8ooxAPX)M=LQ6e+_B+Peew6b>wH*sI1UFHG%y)Zpetl+WAM>GdN1lK6rqTh;R`UUeeNpuUxwvMUY3K>X?gQ&a!{~!)oZy|{4_$QUxD2q!073R?|&fli;p}ehNFJ=_{3U@QrmT4%)f*auu`9s`0Z-NOuZ-u@#nLd4Hd99Ppvp6dqMfJ2~8}3^rB#Q11XJ)?_WV&!h8ne{){RbSG<CQvn+*m^jRc*o#oV<5%e63^E=@}ET8UnXhG~+D3lge0=*<p~;&a-QFTVY<KVg$i-5+f`}F^5Yy#;rKEHsOayakq})kAg|ku2k0^DODWZ4I+j5F@s29I1U%R<+l+Z4%uSH=#Ym2J(7<7r2=3~Taolf>h;tkeeJ9P>ry4}l2+BlA&D@Z6I(=i%!x{9Q*_G`Qh7%CqH_p224hgPuiSgZUC({8#nQd=W8j1#%-82^t_gD93UZ~bd)iw4TF07rC`v(2jhv#P15ket=+^y7r1yZ)&?T=R*^IDTtE(CF{h5|x14p`Y@Fuqv$z0s*X)W-@a_3DXeNf%CI9-28ndPMXt|vic@w-Tnix$xr-s%m_xx(((k!7%g@wrW=w@M_QraB=mvF>E7DIRzqVLwTD@;M0jY%2J@4x5Ut5m=MnHD~n?p~$cGJG0)9+dEJ4ixBI}0DIQAm@uk$wlCj{cS<sIO|4@qgzX(EJ%+G7s#)_7fXPJ-!u}!A<YQzeQZ;uSr(O!+3(Z|7gQ>BH8c=~c&gtC@H69FBFQ{j3jm2t{-ESw-fb~89v9liQn0f#{frc~_nCQX{7IJP~F-myUBe*m+ybbt3Yg`5yM+6BL2zzI>YYToKgrUAYRDF&|3fXO)U^^=&2wsCQq6s12Id7Ts9VhHI!X&;;E=>@|X*NlIfZQc7<joKwPQ>6hAuO-Nrj|qDd{-c}2~jxQ7%=0b^K(V8-`e%Y?DoNCrzSrN)h}Eqg6TDu)UXvn`i0=>P||xHOE8(xsW)b_xu!~$?2a2Ii)-p<6wke|IZ@@41Bz*(nvy*C8r4+X3S+EnO7Ss_@nD)VcoI_bsw4K{x>2Ane=O(M5Yq70-8u;`20JZ9RQhvA($u`-dv21=gpqKd-Q+mml(VH~^0^b;DmVL7c=xrANp<2QsOaVoPMTe>lzW4|MplS?C-mvI#hzb*QD*KmrAIWS-U2&O$@>tEM<>Zc9i6t$G_5{W69AMs?SRo+hz4wQH($aiAM>m$Dfd-ej?+wUzZZ6aFX2E!bbkL1#!zS?>^gzl3Le#W_-E}zuXRi|T!|;0GLIb~B>yw<P=NG`V#m`qHJPMKhpnuV4e#tzSO+VrZIeE97I7N&I>JFoIG{S&Y685m5|m1z68z(^wyndq=4VDqD!`ob&$ns7X3Npr39dX4W8@Ty?9_FHi}<SUxP32eTpYLCCM)U{1DziompIN4Pu7|c<5q~PM8!jy&$dpB{0)eG_M}~@Ze$>^Zr7r7tuX)w=P4h*X1q>NBr&i)-9)2`Z|fo&(LQ&~x^1)k_zi4R!O=`PzZh<!O*_#pBX)K0`XSoKI#!mG<!DuSSlP9=Y87H2_x57B9uoz*w1X>3Mc1{pWf}eYrI_-l{WGHa+8Gno)N=243<Y845NigSQ2MP<Vm(uzv9R{*=$VdOZz*Y>n|R<5vS3XB^oZ@Iq?T%)Lp|F~Y@NRorp+{)o3$DTV<NklHRQB|7^0oi!p7=St|3Hts;Tr^$4q;`MbM<TC3q&YzC<ynXZTX<$6oOrDb3UDv)#T}_inK$jOC`omL60b2vh7AaR^&MjmID!JOnis794Y?q8y=`s2zhuNEdEiblLqRCE?rf?!^<@cv6M&;+=c+`yI26XaLS5ODiMJ%z%sGCfso|+|Ng7-s@OnZl#r*6`4_{HvOh!oYPYuHi)dCcj4qf1@-ehO|@t&HXgPNJ{+zwlEJ=uR3*JG<@$%9zSm(3R61V1Cr~|^QcS)GAAJv`d@QIhsK|YL63+Ut-By@5iV<lDi~SN6dyJprr@R$A3F9l@$s;MB*E&`x44wli#l#}NV?CYHp}+bic%?U!?g$ljn_=Fh{$S;Kp?4b4!6(5l<}2+g=r$vX{#Sg~3VN&)ZC=14EugX37ot5&AC^$U*GFYx!q8Ml*F=>EbuHFFE&F13g^ERxhWqlgr3V#4KNM-lytuFQR0`Tqv+ReOzpr)7@Tj!fDMzbZiQW`gKZtn06<9ym#g#;+KU?Sbntm@#?<`gfK%U>f*H=%4c-KS^x1vV}H6uJkudZVbCjzI&|1+bP8oi8)UTPx^-=dd5=XFHpzTW_Gh40)9Gj1(iiNoP~8qmWRndr)UD$-6Q3zrX1O(l1blw7<K&l`RyCz#gSfRxPK1m(i>kK$HYK_|NQqZr!U-`Ver32KaPtSy|j^q``<M%Ib@YDWbLJjgm99j>o+tXDTk*mzE7P{9SO=ScERe}hU_K9=+hWN7nTI5+yuFi%%8BMr)0rvW{Zg|qw;lDrd1s4Y_fPmzS$JLW7(9CR2pK}y+n>)j0{x=2q4CdxAGbdKbapfkk<*w@~e%}8R^sH2V$mUGvAcy7aeB_W{7;}ZuVi#uS_!3X55w-_-pIMmKsO2Is9u^+g-b=IQS1wMLy&gADBLBB6nZdso0D2!(<%_7a>#Bi+>J8lIv*I|!m$}O*T%r{g-j9T$ltnMiiY<!)@N?($6pom;}hq6BgWv)3cIo2ngdi|W(j_nEWbIVM}Ez`?!#2X+jc#oEwc&D9szXZbI8QzDc8k=}iE1xWQM^i)sC6~M*2O=Lc<UnH2XVvBjM<idHVRrP&vA{!B=rmwJ5(8Z62*tb=$r|F}L)FT*POx<j#kiy?t;>(JDTyb$_L4}0-$Hu-O@Y7hQDnTM27GP~*wm24tm$$knsBWanqv|hi$uOE#rx^S`g<^6>zH#AL>|?Ux5?M*ky5;)(jy-;NFe(>G37-I_+><}!7If*93!J<Ko6gUW|yYZ2(}^_k28fHA{kfl%VFXLB5Q65!FEfyRTyf$n97jh_<E989OHb~EHf`+gr6c@x8O+8K2r&|f|n0Irl;W5bxeBQ()s1Hmail<%Ly*tnOKsK8S^fKq-$Pk2;FAb@C83s-A@CC1cisEQya;Rg(0qw@A+w`{d*ltEK9OHNJ_He0*o(yhB8lx{yhgwImor-p^|=u>vM&L&_R7!!CurQE!SkXOJ)Eh_Qf=cPP*Ac7h_F`H<pR&97q|0cqkA2S|?b}nh27n5t}2K%66H<jqYrhgX15Ce{-1CdEQ2Rm@iDnvt1|r8_BO~zaKo5x5AfJ3i4a<wROm<4P{n4OkAdDqKDs8-PDhTV9<8<G*pXBcefYzV47GFM)WAox@V;s@k+oGw&K{B65!cMTh{@!si!c9mP0Q%`caJ0RO#k-N)phCM+Ihxl5~D?(iYNNy^h<Dtu3kp{Wh6LtvLUfk+}cAy=(n#+eqU7%YBcrkJ;CK>s@o#0C#E8qy>sYAy6ch2(>N4mYcRf|M#2SC8b?*N2WzKMH=M*qi4}e&HQ#AJ3EhViby7)Fm3dtTrf&xx}@TnDSpRwD2(ygSZG#{CrBkp6N)}9KO>a{|4fyO@zb-VwWD}n!VGetuO}lrp7aEyBVc{FYLc|tg^?6|wqKO20~C@PSP4lh1&O3)a1N5O9Z?|!pEW#iMI<8;bR{Y8QvHrdS~<8|;oiVZl1aBDlAJB!;U3nZTE$5rl`c(bR={Z_kJHZhl>;NS1;KsaWf8M!r;TY=)Tkwu79DCeM)trQ8d<5-5LrD0){{s!Sy(UtWP|^v=gt{8tV1@8^LclrXdd_VJrtUnu`}Eu{4>z!pqDU%L=VZxk0m_;8GMG$mIl5kdJwwC>e=YQxD#)No{Gt)L-brc<QLKFIQ(4n9MSHNv$TL)jp7CgGidZsurp`2Cm==7p@pX56N|!U!i~<%goAz^vQkunP^D{X+`Ay2z69f1loFw5T!Nu-BNC&ZxCBdBMmj-AY@Y4zQ|Jd64+$sXmP^9zuYHuSd7UY(-8xj@K!O2PIC<i6&`^N`<cB^-*LKbR3qWSB$>>~?FoTp<Peu-8M?mJy@)IsBEExB3p_tyyotzcd863KE_|S<lg##zb8jvC?l|4t<N=?XN)`OUjt}qYGpa*85_oni$L_Gm1d}vSUi~~@M!UyA-tY^UoAzr|#RyLXmR`OdV-6q2~IaTMtcU~`>BQxl*9g@M7%i$X}$KYaOGaJy0BB?{nk<CU@cin6VJ&GquTM|TW$X<**^qJ@lq_M~UInJi1ci}R&jI88&8=En*fv6M12|ZaR%M#%t{vt&-&`82*7KJl*Q&h9z^u0ppgaWCRVI{N3={{k+Ps)3yV4}t(lDg8=kWn??kr{9TK6Gz#v&cuy!gzkmcEZ82OCs6jwaiwrU6K!#-;PdavE;<xA|$zNmJZ2sJFY$vK3WFSbw7nP&k4_}vK_U_{fUWLB{|-QR3H*>BY1vS&7948pO3FnGA5AFFBcUOtZNF(W)7h|tV0!YN{c6r(xsS*!c@c?ZPXb;jI_(L5JOsG<c7{s$_%=bn2)FD*PD@m6iGxLCiQG&e*ltZ86?TFdSroOs)VFS4}s(jwo!*6lzG{;a%OtN6I_Ov0<2R_J-Rzf`E&|o#|I$0F~~W&B*=VGkX<(x6lQ|#y80DDQ=@e7Lg2^`$RTgctQJR|%uo@7(#H8Qq2d`T*=k*u-ZW>@sgm^eydgsjsDoUVoyn{jHa&?Xl4vd$M-sdrbTO}JH?AWDJw^cCVJIGL;xVM?KrwA?anh1h)1{O%L!497YEL&-#R+T3q3Z~g%+IOJ4!a`+3P~*GqF7pA`9F8?Z@&(i#|OMC*BnV^ocvnWx$c}qY5X%HQ94zzEDqZnpw}~lY~hGxNRf+TBp}nVdfMf+f<=*Z-7KV8!#$F9h{3;tfh{8G$dq?H*PatE!X>G=?aAh<M<VD>@_~#z4WT0-Q>unfnsTPp!bl34m5iT_q#xm|NhEn;%2}{w<dr9?WT$6UAfdLC#O$3!xmsat3Dn3JQn?~UEO&uAS~5`Ov@|}oTog%&U>VeGKQ~#Y>;i#r9#9fbDM;UTcIO#Jb)Ly8Ml8UfJTN7PRJop@Tu>t^DWrHlpVY@snI$2`!?a*?yOnqA@Il&93MuMaUYdSpl^`d>S}EPU$*@+IOYM2u1JyAjN0=V?LSl-lcQBp1qa_`?W8Ac=Y-JY(m3M{6v&`sW9V*`hzzJO%O=#V4>}C;P#}DUGekHY$p#4|^<F7%UF;dj=@_F=$s)r+ebVprJp~~kLI*}(8704w)UNC$ldvA4S_(&eTUP5v;ZsC&TB1WX-Dm_aZ`xU6;OBzXXY!qZj_(Do9QY7-3&K)f&s0z{=Us`fCTNG3mE}7<o>c%&BSzmf4SZO${GF;H0vU7$}DotdDv9zCp7~XfM%%Ho``!tFiNyEhmNu{TBWz>9OB!!wi=2!N;q=0gO4UdCVhe&E`C?K^t3rJ0+_&pIGN_ZjZu>u_OF@^E_r#Nb9Pl%yd5<DIT+UC6x&jGJn$B^S8K+BSD)v*~-89n9T9Q1U#O*MYc1U;kW2)&L7L0d^AgXiNyI|4HJIKUJuspY~*ictJMQw+OxNLR=x7>DKqTBJVJ@;o4+3?$Fnm>63EGV+2JLnR0LG=BdaM>9>GE*KetJ_Bf80>g0~DuDryDQaAG9gGO=$0{jdrne)>E0xSf4u!L)=S1#;rzkDJKGKqc>R&SnRJACmsv8wJ7gQC3@R|}%YYMe0!EuKv=M;O6wX{S@efGvtp3#vh_5`9vm>khDA=&vvod8rX3TWsTG}nDStV1=C0^F;*G}2c~PM%57OHuQr)X&-|NBz<hAeRLx?fF7_vYw*+pxM!qd9uz;8mq3&qM*i!rfxQ<rn@9$6sWNq^9WQ&)H?((k)LHl<8{^P@=rHvc#$&W1d?10Tv?abI|Q=|d`hLgwhJTaheDbKkQ6}IK7iwKxXsuZ;xv-RnnN(iT8=^$e|9+!6N5q8igK+Wq;9U{vV?484vj4C&w-3)4OiU=<P#Oz+AWDJ7Yj6nKdwVHxm8RkC?N|#OjQcQ$j=c`psugeNzXBDILGn83Uc_FCt@%!J%0UsV0wB(N8@M~jQ^<6LedO|A3YyO)lGmC<RCO(-&cwnNNvgU3h?952Hzg4kJnn^#B>3T<5kAErZZT4w&4KR18Yu<KSQD*kLz`{78$PkL=?gBMZtt&SC%uu4C|1&oxTUjcv1tBin=LnDnj8ZW0_$D7Mv2O<)pPfar!x7co`>sj+;_*!v=i=2f+4hq7-jvvf^@J`AA@g)RGvBMKM;Pw873(ym1{lxQ9zI_QNYm0b3c{0TI9|InU#j#+AF(<dub^pi#mXQVVmgR4%O@Et#}R`siZhqClE)J?B|K#&sMetUmL+bQYD8Jb%g<%y~9=k`s-LQ`d9UCO+i_DUnEp$|HVK4&5Kgr!{pmuqcl9g>ZvfqqA^~>yUF`x#P0r3Hh0kPmP-Nk11!01)`v}ztrF+1mm8gf8%tJ!aa*f#~}fX;f|!z;~hM)oaJz9cf#`>GJ%9%Eed_eLY;f%a#+Way4V$Rj-tLL;qFpvpRIq{IbAhVUPw}eosjuCHGcmLN0n7I(l{;{MbcqP1!P@&&%yDq4i&Rk!b(+ig5!-Ep^MKs%n-HP%%*14n4xGUtRT%O5+V6O6AM04#Y#`Y(JYE%OeCJEij#E+d6Yuil1c3v2^Vo>XS7-ff941zQ+Z^gT$2M4CzPxwBNxbyfD}olU{VOs*hP_q?(8*JA|WK)aLTAkV73(36=G;Di=YZ6i>=3vFQ*q^xnMHZr%tq~LYZ=&r%dBMu}|*+?w(`x2zFqQyHG;wp&=a-PQvXLg*)`Po12DxScfW(@Uw<k(T6g`_<-nm$g?b|l;tF`Y-ExysNhHhorWFA7+O+0iaRJHr>J1d7Ddu|ftwo_Jha3E5=qcS!S!=G^14wXv}9ySa5b7+WM%vrdm~H@Gf@-lxK0m0vLLxV%Sg!u$l5f7w~ajD0pY*@h|ljH@9|sy+}=NI0|_s``}Ut-{cH7Ku2yfp`uf|qt8cz|^M<E@|KsZ~f50E#z5eoxZ(qH9v-)1F{{8ChSKr|?-~8v*+m-*uE6!FSp{HB_x9#2P;e93Wbp7|gHO4AeLA}QR%c{1K8|m0uIE#O{X<QAhYSqTMrfw~$jfTdq@z`diY1z7k+AsTfb5q@KTijv)OZMz_+q`S<k8cvL)trIY2vv*qTJwr4yXMN-S~mXQM!U+bLDx#OT-7yLT*|d5JWUl{mil`4fGh9stH!@=mMnzxO7n=g?RSs$`&C@qgX6d9_4eI%eci6M54-CsNMOJ?yp(rBgcPsV?Pho1u8{tp+I?8#@xJXB@w;tx+y1g&?e5y!)w}9$Rqq~e_x%mrSkzv>`~rpR?dt1qUcO$vdHMRytGBPdfA#i<@bFz*@2lJT*VTR4LfrbZpmxj(jYdC&M*J)MuFI+Mzm0ou@cVXm)B5kYz5Thm-Zrc1`Z^)hKhl5lc)Pyd)juI4I?DUcd;ZkBo4f0FAJz^WI(*`3A3l9|zuTZ9*i<#jdN&V{dkx4%n6Dbt?BVJCZjUg9=ggH<K3U#szXxNO{<^K2?d`kO?Jf%Vs;Pbr2y1<%(EC073lHxe9>TW|{JN=r=~%LAnpLzJel)ig(hqCq+=OiU@YwG5A;9%BN>F>>zw2gqyMO=C&usP&zTaJkuk#+IEqq{b#qKkFmH+5rk6P*{Kk7aHetYwHgCOitT;i%8v5pEQ{Gs1Cq$d9Jf6_MF>UI_O^X49z==ac3&3UC3)tN6Y^ci<yhLH(1pxxgNzoonihr906u!rupcj5c<t8_542t2=S-&KJXJ?`BX|Fx~ca$enpO<(PikbQ+8_q*<nt@^!m+-rWv+&T6%NxnIp0^|39Qqdp2L#CekE<w?ccbvdQtn?<Gp$`?{yRSZU-|48fn5v!Dtx~9p#D*KQ7P=8N2fe8(1?$#mW|+UW*l>or4Up3e)tXSK3F~kfc3k$LKawpC$a!==6nb}DNE=8(4Y$26?P=kz(!HR={qKh@vI-?-byx30Kg6)#@V5{B;HVF~d*mw@HysY?RdtedaC{8+=k@ltwv!1P{HWun52#I!38=&}0+P2We^MnM%ik7cJk0MxUmRk2-0|7`-Nd3&PomU6mcN^`vGgCGP2SB5izaHpa|r=xb{~Qa=O219jvuo8L;oJ~!@~~m_x9lPqc(Dbv%t_A%Ni<fYz?lFN>{R7H@=|LD$xL1WiYM9vrX+bwS}q%$<PopCR^VCl<5{0w#MR9x5f<ZFrecnEOJPNQua^R7<1!23fvctJ1xTzw_uFjm6B3(j*3)kbnMjfw%E6!P}(=7Q(jF6FZNS4##~!woSzE-igz_LAIBKGKgt+OKUc{Cp1d6AGH~NbV=Rism`i_Vk_LZT<dglAjIp^A+Mr+IN8DN4H8TCYZLwAiSo{GlB!7Q2Tg(g6|CcnzP6KXS%}|}Xw%u%e!bTWXB3j(EmfMC4y*5o_dDSS8q7`^hYL!IWZCUJIy`V9MPum#3Kx>SLL}aG1=t6=BA^A_$8prH^AvoX}PH7Wv77Nz+h0o1F+W@$4xI?=vmp8|7CB0AV3w4BHCR|y5s^-|cE($p%P_f{t%Y8I!9J1v85Ob_S3vL}^Wc}DuF~uCSf;sm03TV&q4CE)~%C0c%PcX+X;7Ta#%W)x$*NP3HK4*JuuMT<7_%kFve?)VvrO-+;jm8IL=;Ha8G{>+un_8nc>a3}>5^U32-c}46-QWp$ttwFgQ*F)hgQ~&*YHeLNbt8Rdj$MBak!t!<=n|&BgsCrK>PwjV5~jX{sb`yFtNhI1HSS=!w!)z-w6ba%*|crFX>^M_(rwgQz*=+flGJFF*Qj$jYLs=wn$ICjJ=qjrV$_!y^(97qiBVr-)R!3bvn{dEU>oNvPu6Q+-m|G-Q^~eb8`tWMVm1DH(->t{<0{<qoAuf<xdG8O5_DC64l(M9miQ8&z67W*0qRSD`VyeN1gJ?{{NwNc2W(+Y)c'

if __name__=='__main__':
    raise SystemExit(main())
