"""EUR/CHF M15 SHORT — Pass 1B bounded structure/body extension, 2026-10-08.
Python3.10+ standard library. Exact Pass1 history is in the companion ZIP;
no credentials, broker requests, account reads or orders.
70 geometries/75 memberships/420 cases; fixed RR3; 1/2/4-pip assumed costs.
Run python app.py (HTTP and autostart), python app.py --run (CLI), or
python app.py --self-test (synthetic checks). WSGI app:app; one worker recommended.
PORT=8080; EURCHF_M15_PASS1B_OUTPUT_DIR optional; /status and /results.
Keep EURCHF_M15_PASS1_FROZEN_DATA.zip alongside this file.
Source, H1 aggregation, independent controls and every archived parent ledger
must pass before new-grid outcome evaluation. All history remains exploratory.
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

VERSION = 'EURCHF_M15_SHORT_PASS1B_STRUCTURE_BODY_BOUNDARIES_V2_GITHUB_SPLIT_2026_10_08'
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

PARENT_IDS = ('RAW_ENGULF', 'BASELINE_CONTROL', 'M_L200_D010_B075_R125', 'M_L200_D010_B125_R000', 'M_L060_D010_B125_R150')
FAMILIES = {
 'A_STRUCTURE': dict(lookback=200,distance_atr=.10,body_min_atr=.75,range_min_atr=1.25,axes={'lookback':(150,200,250,300),'distance_atr':DISTANCES}),
 'B_STRUCTURE': dict(lookback=200,distance_atr=.10,body_min_atr=1.25,range_min_atr=0.,axes={'lookback':(150,200,250,300),'distance_atr':DISTANCES}),
 'C_STRUCTURE': dict(lookback=60,distance_atr=.10,body_min_atr=1.25,range_min_atr=1.50,axes={'lookback':(40,60,80,100),'distance_atr':DISTANCES}),
 'B_BODY': dict(lookback=200,distance_atr=.10,body_min_atr=1.25,range_min_atr=0.,axes={'body_min_atr':BODIES}),
 'C_BODY': dict(lookback=60,distance_atr=.10,body_min_atr=1.25,range_min_atr=1.50,axes={'body_min_atr':BODIES}),
}

PINNED_H1_SHA256 = '9c8b5279629daee868ad924f52e998be7f6ce638a15e106ffb4851a2df76feac'
RESULT_NAME = 'EURCHF_M15_SHORT_PASS1B_STRUCTURE_BODY_BOUNDARIES_RESULTS.zip'
OUT = Path(os.getenv('EURCHF_M15_PASS1B_OUTPUT_DIR', '/tmp/eurchf_m15_short_pass1b')).resolve()
PREFIX = '/eurchf-m15-short-pass1b'
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
    return tuple(config[k] for k in ('lookback', 'distance_atr', 'body_min_atr', 'range_min_atr', 'close_max'))


def make_configs():
    configs, by_key, membership = [], {}, []
    def add(label,role,lookback=0,distance=None,body=0.,candle_range=0.,close_max=None):
        row=dict(config_id=label,lookback=lookback,distance_atr=distance,body_min_atr=body,range_min_atr=candle_range,close_max=close_max)
        key=config_key(row)
        if key not in by_key:
            by_key[key]=row;configs.append(row)
        membership.append(dict(requested_label=label,stage_group=role,config_id=by_key[key]['config_id']))
    add('RAW_ENGULF','PARENT_CONTROL')
    add('BASELINE_CONTROL','PARENT_CONTROL',60,.25,.75,1.25)
    add('M_L200_D010_B075_R125','PARENT_CONTROL',200,.10,.75,1.25)
    add('M_L200_D010_B125_R000','PARENT_CONTROL',200,.10,1.25,0.)
    add('M_L060_D010_B125_R150','PARENT_CONTROL',60,.10,1.25,1.50)
    for family,definition in FAMILIES.items():
        rows=[{k:definition[k] for k in ('lookback','distance_atr','body_min_atr','range_min_atr')}]
        for axis,levels in definition['axes'].items():
            rows=[dict(row,**{axis:level}) for row in rows for level in levels]
        for row in rows:
            label=f"{family}_L{row['lookback']:03d}_D{round(row['distance_atr']*1000):04d}_B{round(row['body_min_atr']*100):03d}"
            add(label,family,row['lookback'],row['distance_atr'],row['body_min_atr'],row['range_min_atr'])
    assert len(configs)==70 and len({c['config_id'] for c in configs})==70
    assert len(membership)==75
    return configs,membership


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


def make_features(bars):
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


def selected_indices(config, features):
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


def load_frozen_history(work):
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
    check('configuration_count',len(configs),70)
    check('configuration_keys_unique',len({config_key(c) for c in configs}),70)
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


PROTOCOL = "# EUR/CHF M15 SHORT — Pass 1B structure/body boundaries\nFrozen 8 October 2026, before inspecting any new-grid market results.\nGuide: FOREX_STRATEGY_RESEARCH_TEMPLATE_AUDJPY_2026-09-24.md (flexible).\n\nQuestion: were Pass 1's marginal 2-pip-positive edge cells isolated, or is there\nan interpretable wider neighbourhood at longer lookbacks, tighter distances or\nlarger bodies? Pass 1 had no positive geometry at 4 pips. This bounded extension\nis exploratory, not confirmation or approval to add a strategy.\n\n## Frozen design\n70 unique geometries, 75 study memberships, 420 model/cost cases.\nPreserve five exact parent controls and their original configuration IDs:\nRAW_ENGULF; BASELINE_CONTROL (LB60/D0.25/B0.75/R1.25);\nA=M_L200_D010_B075_R125; B=M_L200_D010_B125_R000;\nC=M_L060_D010_B125_R150. A/B/C are research reference cells, not frozen winners.\n\nThree separate structure grids, each 4 lookbacks x5 distances:\n- A: LB150/200/250/300; D0.05/0.075/0.10/0.15/0.20; B0.75/R1.25 fixed.\n- B: same lookbacks/distances; B1.25/R0 fixed.\n- C: LB40/60/80/100; same distances; B1.25/R1.50 fixed.\nTwo separate body ladders at fixed structure/range:\n- B: LB200/D0.10/R0; B0.75/1.00/1.25/1.50/1.75.\n- C: LB60/D0.10/R1.50; same body minima.\nDo not form a new lookback x distance x body x range Cartesian search.\nNo new range/close/indicator, session/weekday/date exclusion or RR sweep.\nDuplicate geometries run once; requested memberships remain visible. R0 means\nno range filter. Body >=1.25 implies range >=1.25: identical signal streams\nacross families are counted explicitly, not independent successes.\n\nAdjacency is within each frozen family: one step on one study axis only.\nBoundary flags identify every lower/upper tested edge. Neighbourhood counts also\nshow distinct raw-signal fingerprints. A positive boundary is unresolved;\ndo not automatically extend again or select the greatest historical R.\n\n## Exact data and mandatory gates\nDefault is offline: exact completed OANDA unsmoothed MID Pass 1 source in the\ncompanion EURCHF_M15_PASS1_FROZEN_DATA.zip alongside app.py.\nNo token or API call needed. Requested window 2005-01-01 to 2026-10-08T00:00Z,\nexclusive. Native546647 M15 candles, actual first2005-01-02T18:45Z and\nlast2026-10-07T23:45Z. Source CSV SHA256:\n59f3a83fb838d9921f9f2433cd928a77ef451301bec466a3fedd67744ae2f707\nCanonical five-decimal OHLC SHA256:\n63eeea194c7106cfe0ebe248e96a392ab876611da5fc2fb569e3b2f304eaaa22\nNative H1 archive has137939 bars and canonical SHA256:\nb3b686ad3a92569d971d2d9412c8afe64a5ad8f54b5d097840c4a2d6cebbd2c0\n\nVerify the companion ZIP and every member SHA and byte size, then exact M15/H1 counts and\ncanonical hashes. All native M15 hours must exhaust H1 price counts and match\nOHLC; pre-October H1 must reproduce the prior137819 candles/hash. No synthetic\nbars, interpolation or refreshed/extended history. Preserve original receipts\nas archived fetch evidence, not requests performed by this offline run.\n\nATR14 uses TR1..14 seed and causal Wilder recurrence. Original warm-up200\nremains for all old controls. Longer lookbacks additionally require their full\nobserved-bar history; missing prior-high features are blank and ineligible for\nthat configuration only. Direct-slice implementation checks cover all features.\nComplete accepted parent ledgers must reproduce every field at both exit models\nand all three costs BEFORE evaluating new-grid outcomes. Parent ledger count/R\nagreement alone is insufficient. Source/control failure produces an error ZIP\nwithout partial performance results. This is same-source software parity,\nnot fresh out-of-sample evidence. Parent archive had CRLF-only runner changes.\n\n## Execution, unchanged from Pass 1\nExact bearish engulf: prevC>prevO, C<O, O>=prevC, C<=prevO; dojis rejected.\nPrevious high excludes signal; absolute (signalH-priorH)/ATR <=D. Body and range\nminimum equality included. No close-location filter. Previous N observed M15\nbars are market bars, not N*15 minutes of continuous wall-clock time.\nReference entry=completed signal close; stop=signalH+10ticks(1pip);\ntarget=close-3*(stop-close). Fixed RR3. Assumed short entry penalty10/20/40ticks\n(1/2/4pips); stop/target fixed; R denominator=stop-assumed_fill.\nRequire0<target<fill<stop, with invalid geometry/cutoff rejected before occupancy.\nReplay each configuration/model/cost separately with one position at a time;\nopen trade occupies to cutoff and has blank R; signal on exit candle can enter.\n\nPrimary STOP_FIRST_GAP_STRESS: stop-opening gaps fill at max(open,stop), favorable\ntarget-opening gaps capped at target, simultaneous barrier touches lose.\nOpening-gap exits timestamp at candle start; other exits at candle end.\nAlternate NEAREST_OPEN_SENSITIVITY uses nearer extreme first (tie loses), barrier\nfills/candle-end time. It is an OHLC sensitivity assumption, not observed path.\nScan next observed bar; assumed close fill before closures remains disclosed.\nMID+entry penalties do not reconstruct bid/ask, ask-side short stops, financing,\nintrabar jump execution or fill availability; neither model bounds loss.\n\n## Reporting and stop decision\nExport all cases including weak/empty rows; complete normalized accepted ledgers,\nsource candles, path table, five joined control ledgers, full-replay adds/removals\nagainst every parent, grid/memberships, family adjacency/distinct-signal counts,\ncalendar/era/recent figures, zero-entry years and rolling12/24/36month windows,\nCHF-event attribution and cost/exit sensitivity. No date deletion.\nEntry cohorts[start,end) eventual R differ from realized exits(start,end].\n2026/October partial; partialOctober never a complete rolling endpoint.\nR/closed DD are additive units, not NAV% or floating-risk drawdown.\n\nReview breadth at2pips and survival at4pips, frequency, weak eras and worst\nrolling windows. Recovered frequency must account for displaced accepted trades.\nNo automatic winner or portfolio score. If improvements remain isolated,\ncost-fragile or sparse, park this tested branch rather than promise more tuning.\nIf an interpretable region emerges, freeze a few distinct anchors before any\nconditional work. RR stays last; independent final ledgers/current live32\nportfolio admission and prospective execution remain later gates.\nThis runner cannot read accounts, send orders or alter live services.\n"


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
    counts = {name:sink.count for name,sink in sinks.items()}
    write_json(work/'output_row_counts.json',counts)
    return summaries,counts


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
    check(len(c)==70 and len(m)==75,'Complete grid and duplicate membership control')
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
                        'coverage_crosscheck_summary.json','archived_pass1_fetch_receipts.csv','parent_pass1_parity.csv','parent_pass1_provenance.json'}
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
        study='PASS1B_BOUNDED_STRUCTURE_BODY_EXTENSION',start=iso(START),end_exclusive=iso(END),
        rr=RR,cost_ticks=list(COSTS),execution_models=list(MODELS),
        expected_configurations=70,expected_cases=420,
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
        archived_parent_parity(work,bars,features,paths,configs)
        write_inputs(work,bars,features,paths,configs,memberships,manifest['dataset_kind'],volumes)
        (work/'README.md').write_text(RESULT_README,encoding='utf-8')
        manifest.update(source_sha256=source_hash(bars),source_volume_csv_sha256=file_sha(work/'source_candles.csv'),
                        source_h1_sha256=source_hash(h1),source_candles=len(bars),raw_engulf_signals=len(features),
                        source_controls='PASS',hard_controls='PASS',parent_pass1_parity='PASS',software_controls='PASS',protocol_sha256=sha(PROTOCOL.encode()))
        _,counts=analyze(work,bars,features,paths,configs)
        manifest.update(status='COMPLETE',complete=True,output_row_counts=counts,
                        completed_at=iso(datetime.now(UTC)),elapsed_seconds=round(time.monotonic()-RUN_CLOCK,1))
        write_json(work/'run_manifest.json',manifest)
        set_status(state='packaging',progress=96,message='All 420 cases complete; compressing ledgers and diagnostics')
        package(work,True)
        set_status(state='complete',progress=100,message='EUR/CHF M15 short Pass 1B complete; download the results ZIP',
                   hard_controls='PASS',source_controls='PASS',configurations=70,cases=420,result_path='/results',
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


RESULT_README = '# EUR/CHF M15 SHORT Pass 1B results\nBegin with run_manifest.json complete=true, source_controls.csv, hard_controls.csv\nand parent_pass1_parity.csv all PASS. Source and all five full archived parent\nledgers must reproduce before discovery. Error ZIPs contain no performance data.\n70 geometries /75 memberships /420 cases; fixed RR3, same 1/2/4-pip scenarios.\nKeep the companion data ZIP alongside app.py. No network calls. Original Pass1 receipts are labelled archived, not current.\nRead protocol.md, configuration_grid.csv and configuration_memberships.csv.\nReview every cost, year/era/recent and rolling12/24/36month window, empty years,\nfrequency and CHF event exposure. No auto winner; repeated history is exploratory.\nFull accepted_trades.csv joins signal_trade_paths.csv by signal_index/execution_model.\ncontrol_accepted_ledgers.csv includes all five parents. Full-replay comparisons\nreport added and displaced accepted entries against each parent.\nFamily-labelled neighbourhoods use only the frozen study axes; shared raw streams\nare explicitly counted and grouped. Positive edge cells do not prove a plateau.\nPartialOctober is excluded as a complete rolling endpoint. R/closed DD are additive\nunits, not NAV% or floating DD; MID/assumed fills are not executable bid/ask history.\nEntry rules freeze before RR/final independent implementation/portfolio/live gates.\n'


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
        set_status(state='starting',progress=0,message='Starting EUR/CHF M15 short Pass 1B boundaries',runner_sha256=code_hash(),result_path=None)
    if background:
        threading.Thread(target=run_job,name='eurchf-m15-pass1b-research',daemon=True).start()
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
    if path=='/start' or (not STARTED and os.getenv('EURCHF_M15_PASS1B_AUTOSTART','1')=='1'):
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
        current = dict(service='EUR/CHF M15 SHORT Pass 1B boundaries',version=VERSION,status='/status',results='/results',
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
        if os.getenv('EURCHF_M15_PASS1B_AUTOSTART','1')=='1':
            launch()
        print(f'{VERSION}: listening on {port}; /status and /results',flush=True)
        server.serve_forever()
    return 0



EMBEDDED_MEMBER_PINS = {'source_candles.csv': {'bytes': 31136287, 'sha256': '59f3a83fb838d9921f9f2433cd928a77ef451301bec466a3fedd67744ae2f707'}, 'source_h1_crosscheck_candles.csv': {'bytes': 7942356, 'sha256': 'ffaa24aa4c371b7885c61ef225f1846ccf5e133d444a45c438af3066cef702a3'}, 'fetch_receipts.csv': {'bytes': 19964, 'sha256': '52423125b321d3c25b5ae2adf89c8fd287b230764540847b375279cc92530890'}, 'parent_pass1_accepted_ledgers.csv': {'bytes': 23899883, 'sha256': '05bef6ac72de3d5d7c4a8e6ca0fc2dc52c13c684f956e9f3c88cf8f4aea4a706'}, 'parent_pass1_provenance.json': {'bytes': 71790, 'sha256': 'f11996dfcd59473762a35b3cd02ab627a3b9757d71f2021b593999f5ce60ea1c'}}
EMBEDDED_PAYLOAD_SHA256 = '511215ae3b95e676829896d30baa1342bbca17e7be30745a22753dab5ba3b849'
FROZEN_SOURCE_FILE = Path(__file__).resolve().with_name('EURCHF_M15_PASS1_FROZEN_DATA.zip')

if __name__=='__main__':
    raise SystemExit(main())
