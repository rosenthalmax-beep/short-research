#!/usr/bin/env python3
"""EUR/CHF M15 SHORT — Pass 1 controlled engulfing discovery, 2026-10-08.

Single file; Python 3.10+ standard library only; no orders/account endpoints.
Run: python app.py                 (HTTP service + automatic research run)
     python app.py --run           (one research run, no HTTP server)
     python app.py --self-test     (synthetic software checks, no network)
Also exposes a WSGI `app`: gunicorn --workers 1 --threads 4 app:app

Environment: OANDA_TOKEN (existing research-service token), optional
OANDA_API_URL=https://api-fxtrade.oanda.com or https://api-fxpractice.oanda.com,
PORT=8080, EURCHF_M15_PASS1_OUTPUT_DIR=/tmp/eurchf_m15_short_pass1.
Routes: /, /health, /start, /status, /results; descriptive route aliases too.

Guide: FOREX_STRATEGY_RESEARCH_TEMPLATE_AUDJPY_2026-09-24.md. The user clarified
that this is a successful research guide, not rigid pair-independent rules.
This pass adds CHF policy-date diagnostics and stop-first/open-gap sensitivity;
neither is an optimized date filter. RR stays 3.0; no portfolio tuning here.
619 unique geometries; 10/20/40 assumed adverse ticks; two exit assumptions.
New M15 source is checked against broker H1 and the pinned historical H1 source.

All history is exploratory/in-sample. MID candles/assumed costs are not real
fills. Full normalized accepted ledgers and source candles are in the ZIP.
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

VERSION = 'EURCHF_M15_SHORT_PASS1_ENGULFING_DISCOVERY_V1_2026_10_08'
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
LOOKBACKS = (20, 40, 60, 100, 150, 200)
DISTANCES = (0.10, 0.25, 0.50, 0.75)
BODIES = (0.0, 0.50, 0.75, 1.00, 1.25)
RANGES = (0.0, 1.00, 1.25, 1.50, 1.75)
PINNED_H1_SHA256 = '9c8b5279629daee868ad924f52e998be7f6ce638a15e106ffb4851a2df76feac'
RESULT_NAME = 'EURCHF_M15_SHORT_PASS1_ENGULFING_DISCOVERY_RESULTS.zip'
OUT = Path(os.getenv('EURCHF_M15_PASS1_OUTPUT_DIR', '/tmp/eurchf_m15_short_pass1')).resolve()
PREFIX = '/eurchf-m15-short-pass1'
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

    def add(label, role, lookback=0, distance=None, body=0., candle_range=0., close_max=None):
        row = dict(config_id=label, lookback=lookback, distance_atr=distance,
                   body_min_atr=body, range_min_atr=candle_range, close_max=close_max)
        key = config_key(row)
        if key not in by_key:
            by_key[key] = row
            configs.append(row)
        row = by_key[key]
        membership.append(dict(requested_label=label, stage_group=role, config_id=row['config_id']))

    add('RAW_ENGULF', 'CONTROL_RAW')
    add('BASELINE_CONTROL', 'CONTROL_PREDECLARED', 60, .25, .75, 1.25)
    for x in (.25, .50, .75, 1., 1.25, 1.50):
        add(f'BODY_{round(x*100):03d}', 'SINGLE_BODY', body=x)
    for x in (.50, .75, 1., 1.25, 1.50, 1.75, 2.):
        add(f'RANGE_{round(x*100):03d}', 'SINGLE_RANGE', candle_range=x)
    for x in (.10, .20, .25, .33, .50):
        add(f'CLOSE_{round(x*100):03d}', 'SINGLE_CLOSE', close_max=x)
    for lb in LOOKBACKS:
        for distance in DISTANCES:
            add(f'STRUCT_L{lb:03d}_D{round(distance*100):03d}', 'SINGLE_STRUCTURE', lb, distance)
    for lb in LOOKBACKS:
        for distance in DISTANCES:
            for body in BODIES:
                for candle_range in RANGES:
                    label = f'M_L{lb:03d}_D{round(distance*100):03d}_B{round(body*100):03d}_R{round(candle_range*100):03d}'
                    add(label, 'STRUCTURE_BODY_RANGE_MATRIX', lb, distance, body, candle_range)
    assert len(configs) == 619 and len({c['config_id'] for c in configs}) == 619
    assert len(membership) == 644
    return configs, membership


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
            row[f'signed_distance_atr_{lb}'] = (hi-prev[lb][i])/atr[i]
            row[f'abs_distance_atr_{lb}'] = abs(hi-prev[lb][i])/atr[i]
        raw[i] = row
    return raw


def selected_indices(config, features):
    result = []
    for i, row in features.items():
        if row['body_atr'] < config['body_min_atr'] or row['range_atr'] < config['range_min_atr']:
            continue
        if config['lookback'] and row[f"abs_distance_atr_{config['lookback']}"] > config['distance_atr']:
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


def fetch_history(granularity='M15'):
    """Read-only candle GETs. Fixed chunks stay below the 5,000-candle limit."""
    if granularity not in ('M15','H1'):
        raise RuntimeError('Only M15/H1 candle reads are supported.')
    token = os.getenv('OANDA_TOKEN','').strip()
    if not token or any(ord(c)<32 or ord(c)==127 for c in token):
        raise RuntimeError('Configure OANDA_TOKEN without quotes, newlines or control characters.')
    api=os.getenv('OANDA_API_URL','https://api-fxtrade.oanda.com').rstrip('/')
    if api not in ('https://api-fxtrade.oanda.com','https://api-fxpractice.oanda.com'):
        raise RuntimeError('OANDA_API_URL must be the official fxtrade or fxpractice HTTPS API host.')
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self,req,fp,code,msg,headers,newurl):
            raise RuntimeError('Unexpected broker redirect; credentials were not forwarded.')
    opener=urllib.request.build_opener(NoRedirect)
    step=BAR if granularity=='M15' else H1_BAR
    span=timedelta(days=35 if granularity=='M15' else 180)
    total=math.ceil((END-START)/span)
    cursor,by_time,volumes,receipts=START,{},{},[]
    while cursor < END:
        end=min(cursor+span,END)
        params=dict(price='M',granularity=granularity,smooth='false',includeFirst='true',
                    **{'from':iso(cursor),'to':iso(end)})
        url=api+'/v3/instruments/EUR_CHF/candles?'+urllib.parse.urlencode(params)
        payload=None
        begin=time.monotonic()
        for attempt in range(3):
            req=urllib.request.Request(url,headers={'Authorization':'Bearer '+token,'Accept':'application/json'},method='GET')
            try:
                with opener.open(req,timeout=45) as response:
                    payload=json.load(response)
                break
            except urllib.error.HTTPError as exc:
                if exc.code not in (429,500,502,503,504) or attempt==2:
                    raise RuntimeError(f'Broker {granularity} candle request failed: HTTP {exc.code}, chunk {len(receipts)+1}.') from None
            except (urllib.error.URLError,TimeoutError):
                if attempt==2:
                    raise RuntimeError(f'Broker {granularity} candle request failed after three attempts, chunk {len(receipts)+1}.') from None
            time.sleep(attempt+1)
        if not isinstance(payload,dict) or payload.get('instrument')!=PAIR or payload.get('granularity')!=granularity:
            raise RuntimeError('Unexpected broker candle instrument/granularity.')
        candles=payload.get('candles')
        if not isinstance(candles,list) or not candles:
            raise RuntimeError(f'Empty {granularity} history chunk {len(receipts)+1}; shortened history is not accepted.')
        included,duplicates=0,0
        for candle in candles:
            t=when(candle['time'])
            if not cursor <= t <= end:
                raise RuntimeError('Candle outside the requested broker chunk.')
            if not START <= t < END or t==end:
                continue  # exclusive chunk end; next chunk includes it
            if candle.get('complete') is not True:
                raise RuntimeError('Incomplete candle inside the frozen historical window.')
            mid=candle.get('mid')
            volume=candle.get('volume')
            if not isinstance(mid,dict) or isinstance(volume,bool) or not isinstance(volume,int) or volume<=0:
                raise RuntimeError('Completed historical candle lacks valid MID OHLC/price count.')
            row=(t,*[float(mid[k]) for k in ('o','h','l','c')])
            if t in by_time:
                if by_time[t]!=row or volumes[t]!=volume:
                    raise RuntimeError('Conflicting duplicate broker candle at '+iso(t))
                duplicates+=1
            by_time[t]=row
            volumes[t]=volume
            included+=1
        if not included:
            raise RuntimeError('A historical request yielded no candles inside its own chunk.')
        receipts.append(dict(granularity=granularity,start=iso(cursor),end_exclusive=iso(end),
                             candles_returned=len(candles),included=included,duplicate_count=duplicates,
                             elapsed_seconds=round(time.monotonic()-begin,3),http_method='GET',status_code=200))
        fraction=len(receipts)/total
        set_status(state='fetching',progress=round((2+16*fraction) if granularity=='M15' else (18+8*fraction)),
                   message=f'EUR/CHF {granularity} full-history chunk {len(receipts)}/{total}')
        cursor=end
    bars=[by_time[t] for t in sorted(by_time)]
    validate_bars(bars,step)
    return bars,volumes,receipts


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

    No archived EURCHF M15 results exist yet. These are implementation controls
    on the same source, not fresh out-of-sample confirmation of a strategy.
    """
    rows=[]
    def check(name,actual,expected):
        rows.append(dict(check=name,status='PASS' if actual==expected else 'FAIL',actual=actual,expected=expected))
    check('configuration_count',len(configs),619)
    check('configuration_keys_unique',len({config_key(c) for c in configs}),619)
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
    for k in raw_ref:
        high,low,op,cl=bars[k][2],bars[k][3],bars[k][1],bars[k][4]
        for lb in LOOKBACKS:
            previous=max(b[2] for b in bars[k-lb:k])
            maximum_feature_error=max(maximum_feature_error,abs(previous-features[k][f'previous_high_{lb}']))
    check('all_prior_high_features_match_direct_slices',maximum_feature_error==0,True)
    for cid in ('RAW_ENGULF','BASELINE_CONTROL'):
        config=next(c for c in configs if c['config_id']==cid)
        indices=[]
        for k in raw_ref:
            atr=atr_ref[k]
            op,hi,lo,cl=bars[k][1:]
            if abs(cl-op)/atr < config['body_min_atr'] or (hi-lo)/atr < config['range_min_atr']:
                continue
            if config['lookback'] and abs(hi-max(b[2] for b in bars[k-config['lookback']:k]))/atr > config['distance_atr']:
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


PROTOCOL = "# EUR/CHF M15 SHORT — Pass 1 protocol, frozen 8 October 2026\n\nUse FOREX_STRATEGY_RESEARCH_TEMPLATE_AUDJPY_2026-09-24.md as a flexible guide.\nQuestion: does exact bearish engulfing near prior highs have a coherent region\nunder assumed execution costs? This is standalone entry discovery. All tested\nhistory is exploratory; repeated inspection does not create fresh out-of-sample data.\n\n## Predeclared grid\n619 unique geometries, 644 study memberships, 3,714 model/cost cases.\nControls: RAW_ENGULF and BASELINE_CONTROL. The latter is a generic comparison:\nLB60 / absolute high distance <=0.25 ATR / body >=0.75 ATR / range >=1.25 ATR.\nIt is not a previously proven M15 strategy or the frozen H1 strategy copied over.\n\n- Single body minima: 0.25, 0.50, 0.75, 1.00, 1.25, 1.50 ATR.\n- Single range minima: 0.50, 0.75, 1.00, 1.25, 1.50, 1.75, 2.00 ATR.\n- Single close-location maxima: 0.10, 0.20, 0.25, 0.33, 0.50.\n- Structure singles and matrix: previous-bar lookbacks 20, 40, 60, 100, 150, 200;\n  absolute high distances 0.10, 0.25, 0.50, 0.75 ATR.\n- Matrix body minima: 0, 0.50, 0.75, 1.00, 1.25 ATR.\n- Matrix range minima: 0, 1.00, 1.25, 1.50, 1.75 ATR; no matrix close filter.\n\nZero body/range means no filter. LB60 means 15 hours of observed M15 candles,\nnot always 15 wall-clock hours. Prior high excludes the signal candle. Signed\ndistances are exported. Threshold equality is included. Duplicate geometries\nrun once with all requested memberships retained. No session/weekday/date filter,\nnew indicator hunt or RR search is included in this discovery pass.\n\n## Source, coverage and implementation gates\nOfficial OANDA completed, unsmoothed MID M15. Request 2005-01-01 through\n2026-10-08T00:00:00Z exclusive (cutoff: 01:00 BST on 8 October). Warm every\nconfiguration with the first 200 observed bars. ATR14 uses TR1..14 as its seed,\nthen causal Wilder recurrence, including the completed signal candle.\n\nFetch fixed 35-day M15 chunks (at most 3,360 calendar slots) and 180-day H1\nchunks. Requests are candle GETs only, with bounded retries and redirects blocked.\nMissing chunks, malformed data, incomplete candles inside the frozen window or\nconflicting duplicates stop the run. Credentials are not written to results.\n\nThis is the first EURCHF M15 study: no expected M15 count/hash is invented.\nRecord canonical five-decimal OHLC and volume-containing CSV fingerprints, raw\nsignal fingerprints and full accepted-ledger digests for subsequent reuse.\nEvery requested year must contain bars; complete years must reach January and\nDecember. First candle must be in the first requested week, last candle in the\nfinal requested day. Coverage, native gaps and every HTTP chunk receipt are exported.\n\nAggregate ALL native M15 candles by UTC hour. Every observed hour must appear\nin both M15 and native H1 sources; OHLC and sum of price counts must match.\nSparse M15 hours pass only when their native bars exhaust H1 price counts and\nreproduce its OHLC. No flat synthetic candles or interpolation. Whole-hour\nbroker omissions and closures remain visible. This verifies consistency of the\nbroker source, not accuracy against a separate provider. The historical H1\nportion before 2026-10-01 must reproduce our prior 137,819 candles and SHA256:\n9c8b5279629daee868ad924f52e998be7f6ce638a15e106ffb4851a2df76feac\nChanged history or a source mismatch stops discovery; do not silently relax gates.\n\nIndependent calculations check ATR, every raw engulf index and all prior-high\nfeatures using direct slices. Separate chronological bar scans for RAW and\nBASELINE controls must match every accepted entry/exit, price, outcome and R\nat both exit models and all costs. These are implementation checks on the same\nsource, not final independent validation of a subsequently chosen strategy.\nAn error ZIP contains diagnostics without partial performance files.\n\n## Signals and execution assumptions\nExact bearish engulf: previous close > open; current close < open;\ncurrent open >= previous close; current close <= previous open. Reject dojis.\nReference entry = completed signal close, timestamped signal start +15 minutes.\nStop = signal high +0.00010 (10 ticks /1 pip).\nTarget = reference entry -3*(stop-reference entry). RR3 is fixed for entry discovery.\nAssumed adverse short fill = reference entry -10/20/40 ticks (1/2/4 pips).\nStop/target stay fixed; R denominator = stop-assumed fill.\n\nRequire 0 < target < fill < stop. Reject invalid geometry and entries at/after\ncutoff before occupancy. Replay every geometry/model/cost independently with\none position at a time. Open trades occupy the strategy through cutoff and have\nblank exit/R. A signal on the exit candle may enter at that candle's close.\nIsolated qualifying signals can overlap and are labelled separately from replay.\n\nPrimary model STOP_FIRST_GAP_STRESS: observed opening >=stop fills at max(open,stop);\nopening <=target fills at target without favorable improvement; otherwise a\nbar touching both barriers loses. Gap exits use candle start; other exits use\ncandle end. Adverse opening gaps can lose more than 1R.\nNEAREST_OPEN_SENSITIVITY assumes the nearer extreme is touched first on a\nboth-barrier candle (tie loses), uses barrier fills and candle-end exits. It is\nan alternate OHLC assumption, not archived parity or the preferred selection model.\n\nFirst-touch scanning begins on the next observed candle. Hypothetical entry at\nthe signal close is assumed even before a market closure; next observed open\nand delays are disclosed. MID prices plus adverse entry penalties are assumed\nscenarios, not historical bid/ask execution. Ask-side short stops, financing,\nintrabar jump fills and guaranteed fills are unobserved. Neither model bounds\nworst-case loss. Cost fractions relative to stop size are reported explicitly.\n\n## Reporting and decision sequence\nExport every configuration, weak/empty case, membership, source candle, feature,\npath, full normalized accepted ledger, both joined control ledgers, open/invalid\nentry, full-replay comparison, neighbourhood and tested boundary.\nR/drawdown are additive risk units, not account percentages. Closed drawdown\nexcludes floating position risk. No NAV compounding or currency conversion here.\n\nCalendar years, eras, latest 1/2/3/5/10 years and every complete calendar-month\n12/24/36-month rolling window include inactivity. Entry cohorts [start,end)\nreport eventual trade R; realized exits (start,end] report cash R within the period.\nZero-entry years and windows remain visible. October 2026 is a partial month:\nit enters full-history/latest-year figures but is never called a complete rolling\nmonth endpoint. Open trades are right-censored, not zero-profit completed trades.\n\nPre-floor, floor period, 15 January 2015 and post-event diagnostics keep all dates.\nShock exposure is attribution, not a counterfactual deletion of exposed trades.\nReview stressed costs, stable interior regions, frequency, weak eras/recent years\nand event concentration before freezing a few distinguishable anchors. No auto winner.\n\nEntry rules freeze before RR selection, independent final ledgers, exact current\n32-strategy portfolio admission (including EURCHF H1 short overlap) and forward\nexecution checks. Portfolio admission is a later gate, not a tuning objective.\nA weak generic control alone does not exhaust the pair; broad unsupported\nresults do not justify unlimited filter searching. This runner has no orders.\n\nPrimary references:\nhttps://developer.oanda.com/rest-live-v20/instrument-ep/\nhttps://developer.oanda.com/rest-live-v20/instrument-df/\nhttps://www.snb.ch/en/publications/communication/press-releases/2011/pre_20110906\nhttps://www.snb.ch/en/publications/communication/press-releases/2015/pre_20150115\n"


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
        if config['config_id'] in ('RAW_ENGULF','BASELINE_CONTROL'):
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
                        if cid in ('RAW_ENGULF','BASELINE_CONTROL'):
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
                    for reference in ('RAW_ENGULF','BASELINE_CONTROL'):
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


def write_neighbours(work, configs, summaries):
    axes = {'lookback':LOOKBACKS,'distance_atr':DISTANCES,'body_min_atr':BODIES,'range_min_atr':RANGES}
    grid = {config_key(c):c for c in configs if c['lookback'] in LOOKBACKS and c['close_max'] is None
            and c['body_min_atr'] in BODIES and c['range_min_atr'] in RANGES and c['distance_atr'] in DISTANCES}
    rows = []
    for config in grid.values():
        neighbours,edges = set(),[]
        for name,levels in axes.items():
            index = levels.index(config[name])
            if index in (0,len(levels)-1):
                edges.append(name+('=LOWER' if index==0 else '=UPPER'))
            for adjacent in (index-1,index+1):
                if 0 <= adjacent < len(levels):
                    other = dict(config,**{name:levels[adjacent]})
                    if config_key(other) in grid:
                        neighbours.add(grid[config_key(other)]['config_id'])
        for model in MODELS:
            for cost in COSTS:
                values = [summaries[(cid,model,cost)] for cid in sorted(neighbours)]
                rows.append(dict(config_id=config['config_id'],execution_model=model,cost_ticks=cost,
                    tested_boundaries=';'.join(edges),adjacent_configurations=len(values),
                    positive_total_r_neighbours=sum(r['total_r'] > 0 for r in values),
                    median_neighbour_total_r=quantile([r['total_r'] for r in values],.5),
                    minimum_neighbour_total_r=min((r['total_r'] for r in values),default=None),
                    minimum_neighbour_closed_trades=min((r['closed_trades'] for r in values),default=None),
                    neighbour_ids=';'.join(sorted(neighbours))))
    write_csv(work/'neighbourhood_summary.csv',rows,
              CASE+['tested_boundaries','adjacent_configurations','positive_total_r_neighbours',
                    'median_neighbour_total_r','minimum_neighbour_total_r','minimum_neighbour_closed_trades','neighbour_ids'])


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
    check(len(c)==619 and len(m)==644,'Complete grid and duplicate membership control')
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
                        'coverage_crosscheck_summary.json','fetch_receipts.csv'}
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
        dataset_kind='HISTORICAL_OANDA_MID',status='RUNNING',complete=False,
        study='PASS1_ENTRY_DISCOVERY_ONLY',start=iso(START),end_exclusive=iso(END),
        rr=RR,cost_ticks=list(COSTS),execution_models=list(MODELS),
        expected_configurations=619,expected_cases=3714,
        incumbent_target='PORTFOLIO32_2026_10_05_EURCHF_H1_SHORT_PRIMARY_RR3P50_V1; admission deferred',
        template='FOREX_STRATEGY_RESEARCH_TEMPLATE_AUDJPY_2026-09-24.md',
        source_policy='New M15 fingerprint recorded; broker H1 aggregation plus pinned pre-October H1 controls must pass',
        user_clarification='Template is a guide; justified pair/timeframe adaptations allowed.',
        orders_supported=False,trading_enabled=False)
    try:
        (work/'protocol.md').write_text(PROTOCOL,encoding='utf-8')
        shutil.copyfile(__file__,work/'runner_source.py')
        write_csv(work/'software_checks.csv',self_checks())
        set_status(state='validating',progress=1,message='Software checks passed; fetching full M15 source')
        bars,volumes,m15_receipts=fetch_history('M15')
        write_csv(work/'coverage.csv',[dict(pair=PAIR,timeframe=TIMEFRAME,candles=len(bars),
                   first=iso(bars[0][0]),last=iso(bars[-1][0]),sha256=source_hash(bars))])
        write_csv(work/'fetch_receipts.csv',m15_receipts)
        h1,h1_volumes,h1_receipts=fetch_history('H1')
        write_csv(work/'fetch_receipts.csv',m15_receipts+h1_receipts)
        crosscheck_history(work,bars,volumes,h1,h1_volumes)
        write_csv(work/'source_h1_crosscheck_candles.csv',
                  (dict(time=iso(t),open=op,high=hi,low=lo,close=cl,volume=h1_volumes[t]) for t,op,hi,lo,cl in h1))
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
        write_inputs(work,bars,features,paths,configs,memberships,manifest['dataset_kind'],volumes)
        (work/'README.md').write_text(RESULT_README,encoding='utf-8')
        manifest.update(source_sha256=source_hash(bars),source_volume_csv_sha256=file_sha(work/'source_candles.csv'),
                        source_h1_sha256=source_hash(h1),source_candles=len(bars),raw_engulf_signals=len(features),
                        source_controls='PASS',hard_controls='PASS',software_controls='PASS',protocol_sha256=sha(PROTOCOL.encode()))
        _,counts=analyze(work,bars,features,paths,configs)
        manifest.update(status='COMPLETE',complete=True,output_row_counts=counts,
                        completed_at=iso(datetime.now(UTC)),elapsed_seconds=round(time.monotonic()-RUN_CLOCK,1))
        write_json(work/'run_manifest.json',manifest)
        set_status(state='packaging',progress=96,message='All 3,714 cases complete; compressing ledgers and diagnostics')
        package(work,True)
        set_status(state='complete',progress=100,message='EUR/CHF M15 short Pass 1 complete; download the results ZIP',
                   hard_controls='PASS',source_controls='PASS',configurations=619,cases=3714,result_path='/results',
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


RESULT_README = "# EUR/CHF M15 SHORT Pass 1 results\n\nBegin with run_manifest.json: complete=true; source_controls.csv and hard_controls.csv\nmust all pass. Then read coverage.csv, yearly_data_coverage.csv, protocol.md and\nsummary.csv. An error ZIP has diagnostics and no partial performance results.\nfile_manifest.json hashes every other packaged artifact.\n\n619 unique geometries /3,714 cases. configuration_memberships.csv retains the\n644 requested study labels. neighbourhood_summary.csv shows adjacent cells and\nboundaries; overlapping neighbours are not independent evidence. No automatic winner.\n\nFull accepted ledger: select configuration/model/cost in accepted_trades.csv;\njoin signal_trade_paths.csv on (signal_index,execution_model), preserving\naccepted_sequence. Risk=stop-fill; short R=(fill-exit_price)/risk for closed trades.\nOrdinary STOP=-1. Open R is blank. control_accepted_ledgers.csv contains joined\nRAW/BASELINE controls. Summary ledger hash uses canonical sorted JSON lines of\nthe same joined fields, with separators=(',',':').\n\nCompare yearly/era/latest periods, every rolling 12/24/36-month window, empty\nwindows, costs, exit assumptions and event exposure. Entry-cohort eventual R is\nseparate from realized period R. The final October 2026 monthly row is explicitly\npartial; rolling months use complete month boundaries. No account compounding.\n\nNew M15 fingerprints are recorded. H1 overlap must match prior EURCHF history,\nand every native M15 hour's OHLC/price counts must match native H1. Broker-omitted\nwhole hours remain source gaps. No interpolation or manufactured candles.\nMID candles and assumed fills are not executable quote history.\n\nReview stressed regions before choosing a few anchors. RR selection, independent\nfinal strategy confirmation, exact live32 admission and forward execution checks\nremain later work. This research runner only requests candles; no orders/account reads.\n"


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
        set_status(state='starting',progress=0,message='Starting EUR/CHF M15 short discovery',runner_sha256=code_hash(),result_path=None)
    if background:
        threading.Thread(target=run_job,name='eurchf-m15-pass1-research',daemon=True).start()
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
    if path=='/start' or (not STARTED and os.getenv('EURCHF_M15_PASS1_AUTOSTART','1')=='1'):
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
        current = dict(service='EUR/CHF M15 SHORT Pass 1 discovery',version=VERSION,status='/status',results='/results',
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
        if os.getenv('EURCHF_M15_PASS1_AUTOSTART','1')=='1':
            launch()
        print(f'{VERSION}: listening on {port}; /status and /results',flush=True)
        server.serve_forever()
    return 0


if __name__=='__main__':
    raise SystemExit(main())
