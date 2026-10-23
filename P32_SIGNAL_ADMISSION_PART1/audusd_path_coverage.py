#!/usr/bin/env python3
"""Bounded new candle evidence for 25 frozen AUD/USD unresolved paths."""
import argparse
import csv
import fcntl
import gzip
import hashlib
import io
import json
import os
import shutil
import sqlite3
import threading
import zipfile
import zlib
from collections import Counter
from datetime import timedelta
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit
from wsgiref.simple_server import WSGIRequestHandler, make_server
from coverage_support import core as e
from coverage_support.storage import validate

VERSION = 'AUDUSD_PATH_COVERAGE_V1_2026_10_23'
ROOT = Path(__file__).resolve().parent
BROKER = 'https://api-fxtrade.oanda.com'
MAX_REQUESTS, MAX_ATTEMPTS, MAX_S5_PARENTS = 240, 400, 6
MAX_CACHE_BYTES, RESERVE = 16 * 1024 * 1024, 64 * 1024 * 1024
MAX_ZIP_BYTES = 20_000_000


class BudgetStop(ValueError): pass
class EvidenceConflict(ValueError): pass


def require(ok, message):
    if not ok: raise ValueError(message)


def read_json(path): return json.loads(Path(path).read_bytes())


def sha_file(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''): h.update(block)
    return h.hexdigest()


def atomic(path, body):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + '.tmp')
    temp.write_bytes(body); temp.replace(path)


def identity(gran, start, end):
    return dict(schema='MBA_UNSMOOTHED_V1', broker=BROKER, pair='AUD_USD', granularity=gran,
                start=e.stamp(start), end=e.stamp(end), price='MBA', smooth=False, includeFirst=True)


def case_bounds(case):
    return e.dt(case['signal']['entry_utc']), e.dt(case['path']['native_interval_start_utc']) + timedelta(minutes=15)


def m1_plan(case):
    lo, hi = case_bounds(case); result = []
    while lo < hi:
        stop = min(lo + timedelta(days=1), hi)
        result.append(identity('M1', lo, stop)); lo = stop
    return result


def canonical_rows(rows, ident):
    checked = validate(dict(instrument='AUD_USD', granularity=ident['granularity'], candles=rows), ident)
    require(len(checked) == len(rows), 'Incomplete rows in saved canonical evidence')
    return e.canon(dict(instrument='AUD_USD', granularity=ident['granularity'], candles=checked))


def load_packet():
    pins = read_json(ROOT / 'coverage_pins.json')
    require(sha_file(ROOT / 'coverage_inputs.json.gz') == pins['inputs_sha256'], 'Packaged path inputs changed')
    for name, expected in pins['support'].items():
        require(sha_file(ROOT / 'coverage_support' / name) == expected, 'Pinned candle helper changed: ' + name)
    packet = json.loads(gzip.decompress((ROOT / 'coverage_inputs.json.gz').read_bytes()))
    require(len(packet['cases']) == 25 and len({c['signal']['signal_key'] for c in packet['cases']}) == 25,
            'Expected exactly 25 distinct reviewed paths')
    for c in packet['cases']:
        require(c['path']['status'] == 'UNRESOLVED_PATH' and c['signal']['pair'] == 'AUD_USD'
                and c['signal']['side'] == 'BUY' and c['signal']['rr'] == 3
                and c['path']['native_source_sha256'] == packet['native_sha256'], 'Unexpected reviewed path')
    return packet, pins


class CandleCache:
    """Small immutable audit cache; original databases are read-only optional seeds."""
    def __init__(self, work, packet, token='', progress=lambda **x: None, old_paths=(), transport=None):
        self.work, self.packet, self.progress = Path(work), packet, progress
        self.work.mkdir(parents=True, exist_ok=True)
        self.directory = self.work / 'candles'; self.directory.mkdir(exist_ok=True)
        self.state_path = self.work / 'cache_state.json'
        self.state = read_json(self.state_path) if self.state_path.exists() else dict(requests=0, attempts=0, blocks={}, receipts=[])
        self.native = {r['key']: r for r in packet['native_windows']}
        self.m1_keys = {e.sha(e.canon(i)) for c in packet['cases'] for i in m1_plan(c)}
        self.memo = {}; self.old = []; self.used = set()
        self.reader = e.CandleReader(token, BROKER, requests_per_second=2, transport=transport or self.http)
        for path in old_paths:
            path = Path(path)
            if not path.is_file(): continue
            if any(Path(str(path)+suffix).is_file() and Path(str(path)+suffix).stat().st_size
                   for suffix in ('-wal', '-journal')): continue
            con = sqlite3.connect(path.resolve().as_uri()+'?mode=ro&immutable=1', uri=True)
            con.execute('PRAGMA query_only=ON')
            if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='blocks'").fetchone():
                self.old.append(con)
            else: con.close()
        for key, row in self.state['blocks'].items():
            p = self.directory / (key+'.json.gz')
            require(p.is_file() and sha_file(p) == row['compressed_sha256'], 'Audit candle cache changed; keep the existing checkpoint')

    def save(self): atomic(self.state_path, e.canon(self.state))

    def permitted(self, ident):
        require(ident['broker'] == BROKER and ident['pair'] == 'AUD_USD' and ident['price'] == 'MBA'
                and ident['smooth'] is False and ident['includeFirst'] is True, 'Unapproved candle request')
        key = e.sha(e.canon(ident)); gran = ident['granularity']
        lo, hi = e.dt(ident['start']), e.dt(ident['end'])
        require(lo < hi and e.floor_time(lo, e.STEP[gran]) == lo and e.floor_time(hi, e.STEP[gran]) == hi,
                'Unaligned candle evidence request')
        if gran == 'M15': require(key in self.native, 'Native request outside the fixed plan')
        elif gran == 'M1': require(key in self.m1_keys, 'Minute request outside the 25-case plan')
        elif gran == 'S5':
            require(hi-lo == timedelta(minutes=15) and e.floor_time(lo,900) == lo
                    and any(a <= lo and hi <= b for a,b in map(case_bounds,self.packet['cases'])),
                    'Five-second request outside a fixed case interval')
        else: raise ValueError('Unsupported candle granularity')
        return key

    def http(self, request):
        parts = urlsplit(request.full_url); params = parse_qs(parts.query)
        require(parts.scheme == 'https' and parts.netloc == 'api-fxtrade.oanda.com'
                and parts.path == '/v3/instruments/AUD_USD/candles' and request.get_method() == 'GET',
                'Only the AUD/USD historical candle GET is allowed')
        require(params.get('price') == ['MBA'] and params.get('smooth') == ['false']
                and params.get('includeFirst') == ['true'] and set(params) ==
                {'from','to','granularity','price','smooth','includeFirst'}, 'Unexpected candle parameters')
        ident = identity(params['granularity'][0], e.dt(params['from'][0]), e.dt(params['to'][0]))
        self.permitted(ident)
        if self.state['attempts'] >= MAX_ATTEMPTS: raise BudgetStop('HTTP attempt budget exhausted')
        self.state['attempts'] += 1; self.save()
        self.progress(http_attempts=self.state['attempts'], candle_requests=self.state['requests'])
        with self.reader.opener.open(request, timeout=25) as response:
            raw = response.read(8 * 1024 * 1024 + 1)
        require(len(raw) <= 8 * 1024 * 1024, 'Candle response too large')
        return json.loads(raw)

    def get(self, ident, expected=None):
        key = self.permitted(ident); self.used.add(key)
        if key in self.memo: return self.memo[key]
        if key in self.state['blocks']:
            row = self.state['blocks'][key]
            raw = gzip.decompress((self.directory/(key+'.json.gz')).read_bytes())
            require(e.sha(raw) == row['sha256'] and row['identity'] == ident, 'Saved audit window checksum failed')
        else:
            raw = None; origin = 'NEW_CANDLE_GET'
            if expected:
                for con in self.old:
                    old = con.execute('SELECT identity,body,sha256 FROM blocks WHERE key=?',(key,)).fetchone()
                    if not old: continue
                    encoded, compressed, old_sha = old
                    candidate = zlib.decompress(compressed)
                    require(json.loads(encoded) == ident and e.sha(candidate) == old_sha,
                            'Original read-only cache checksum failed')
                    if old_sha != expected: raise EvidenceConflict('Original cache conflicts with frozen native checksum')
                    raw = candidate; origin = 'EXACT_READ_ONLY_NATIVE_REUSE'; break
            if raw is None:
                if self.state['requests'] >= MAX_REQUESTS: raise BudgetStop('Candle request budget exhausted')
                require(bool(self.reader.token), 'Keep your existing OANDA_TOKEN for bounded candle GETs')
                self.state['requests'] += 1; self.save()
                self.progress(data_granularity=ident['granularity'], data_from=ident['start'], data_to=ident['end'],
                              candle_requests=self.state['requests'])
                try:
                    obj = self.reader.get(ident)
                finally:
                    self.state['receipts'].extend(self.reader.receipts); self.reader.receipts.clear(); self.save()
                rows = validate(obj, ident)
                raw = canonical_rows(rows, ident)
            if expected and e.sha(raw) != expected:
                self.state['receipts'].append(dict(event='NATIVE_REVISION_REFUSED', identity=ident,
                    expected_sha256=expected, observed_sha256=e.sha(raw))); self.save()
                raise EvidenceConflict('Re-fetched M15 history differs from frozen source; old history kept')
            packed = gzip.compress(raw, mtime=0)
            current = sum(v['bytes'] for v in self.state['blocks'].values())
            if current+len(packed) > MAX_CACHE_BYTES: raise BudgetStop('16 MiB private evidence cache limit reached')
            if shutil.disk_usage(self.work).free < RESERVE+len(packed): raise BudgetStop('64 MiB storage reserve reached')
            atomic(self.directory/(key+'.json.gz'), packed)
            self.state['blocks'][key] = dict(identity=ident, sha256=e.sha(raw), compressed_sha256=e.sha(packed),
                                             bytes=len(packed), origin=origin)
            self.save()
        if expected and e.sha(raw) != expected: raise EvidenceConflict('Cached native window differs from frozen checksum')
        rows = validate(json.loads(raw), ident)
        self.memo[key] = rows
        # Bound live objects; compressed checked evidence remains on disk.
        while len(self.memo) > 8: self.memo.pop(next(iter(self.memo)))
        return rows

    def native_for(self, case):
        lo, hi = case_bounds(case); rows = []
        for w in self.packet['native_windows']:
            ident = w['identity']
            if e.dt(ident['start']) < hi and lo < e.dt(ident['end']):
                rows.extend(r for r in self.get(ident, w['sha256']) if lo <= e.dt(r['time']) < hi)
        times = [e.dt(r['time']) for r in rows]
        require(times == sorted(set(times)), 'Native source overlap or duplicate')
        return rows

    def close(self):
        for con in self.old: con.close()


def bars_hit(row, stop, target):
    bid = row['bid']; result = set()
    if e.dec(bid['l']) <= e.dec(stop): result.add('STOP')
    if e.dec(bid['h']) >= e.dec(target): result.add('TARGET')
    return result


def tier_check(parent, children, count, stop, target, label, conflicts):
    checks = {component:e.crosscheck(parent,children,component,count,'AUD_USD') for component in ('bid','ask','mid')}
    if any(v == 'SOURCE_TIER_MISMATCH' for v in checks.values()):
        conflicts.append(dict(type=label+'_OHLC_MISMATCH', time=parent['time'], checks=checks))
    if any(not bars_hit(child,stop,target).issubset(bars_hit(parent,stop,target)) for child in children):
        conflicts.append(dict(type=label+'_BARRIER_CONTRADICTION', time=parent['time']))
    return checks


def within(rows, lo, hi): return [r for r in rows if lo <= e.dt(r['time']) < hi]


def analyse_case(case, native, minute_rows, s5_for_parent):
    """Hierarchy proves non-touch intervals; absent critical data never becomes flat bars."""
    lo, hi = case_bounds(case); old = case['path']
    stop, target = old['model_stop'], old['model_target']
    nmap = {e.dt(r['time']):r for r in native}
    mmap = {e.dt(r['time']):r for r in minute_rows}
    require(len(mmap) == len(minute_rows), 'Duplicate M1 evidence')
    first = nmap.get(lo)
    require(first is not None, 'Frozen entry candle missing')
    if abs(e.dec(first['ask']['o'])-e.dec(old['model_ask'])) > e.dec('0.0000000001') or \
       abs(e.dec(first['bid']['o'])-e.dec(old['model_bid'])) > e.dec('0.0000000001'):
        raise EvidenceConflict('Frozen entry quote differs from reviewed path')
    conflicts, checks = [], []
    critical = []
    for ts in sorted(set(nmap) | {e.floor_time(t,900) for t in mmap}):
        if not lo <= ts < hi: continue
        parent = nmap.get(ts); children = within(minute_rows,ts,ts+timedelta(minutes=15))
        if parent:
            check = tier_check(parent,children,15,stop,target,'M15_M1',conflicts)
            checks.append(dict(time=e.stamp(ts), relation='M15_M1', checks=check, observed_children=len(children)))
    # The original exit parent always gets S5 priority. At most five other
    # earliest critical parents may follow. No unbounded recursion or gap fill.
    original_parent = e.dt(old['native_interval_start_utc'])
    candidates = [original_parent]
    ts = lo
    while ts < hi:
        parent = nmap.get(ts)
        children = within(minute_rows,ts,ts+timedelta(minutes=15))
        if (parent is None and not e.conventional_weekend(ts)) or \
           (parent is not None and bars_hit(parent,stop,target)):
            if ts not in candidates: candidates.append(ts)
        ts += timedelta(minutes=15)
    selected_parents = candidates[:MAX_S5_PARENTS]
    s5map = {}; s5_unavailable = []
    for ts in selected_parents:
        try: children = s5_for_parent(ts)
        except BudgetStop as exc:
            children = []; s5_unavailable.append(dict(time=e.stamp(ts),reason=str(exc)))
        for child in children:
            t = e.dt(child['time'])
            require(t not in s5map, 'Duplicate S5 evidence')
            s5map[t] = child
        if ts in nmap:
            check = tier_check(nmap[ts],children,180,stop,target,'M15_S5',conflicts)
            checks.append(dict(time=e.stamp(ts),relation='M15_S5',checks=check,observed_children=len(children)))
        for minute in within(minute_rows,ts,ts+timedelta(minutes=15)):
            begin=e.dt(minute['time']); fine=within(children,begin,begin+timedelta(minutes=1))
            check=tier_check(minute,fine,12,stop,target,'M1_S5',conflicts)
            checks.append(dict(time=minute['time'],relation='M1_S5',checks=check,observed_children=len(fine)))
    gaps = []; assumed_weekend_slots=0; proof_native_non_touch=0
    observed = None; coarse_outcome = None; coarse_prefix_clear=True
    ts = lo
    while ts < hi:
        parent = nmap.get(ts)
        if parent:
            coarse = e.touch(parent,'bid','BUY',e.dec(stop),e.dec(target))
            if coarse is None:
                proof_native_non_touch += 1; ts += timedelta(minutes=15); continue
            if coarse_prefix_clear and coarse_outcome is None and coarse['kind'] != 'AMBIGUOUS_BOTH':
                coarse_outcome = dict(kind=coarse['kind'],interval_start_utc=e.stamp(ts),
                    interval_end_utc=e.stamp(ts+timedelta(minutes=15)),
                    basis='FIRST_FROZEN_M15_BARRIER; PRICE_AND_TIMING_NOT_PROVED')
        else:
            # Same conventional-weekend assumption as the frozen study, stated
            # explicitly; no holiday/no-tick schedule is invented.
            has_fine = any(ts <= t < ts+timedelta(minutes=15) for t in mmap) or \
                       any(ts <= t < ts+timedelta(minutes=15) for t in s5map)
            if e.conventional_weekend(ts) and not has_fine:
                assumed_weekend_slots += 1; ts += timedelta(minutes=15); continue
            coarse_prefix_clear=False
        mt = ts
        while mt < ts+timedelta(minutes=15):
            minute = mmap.get(mt)
            coarse_minute = e.touch(minute,'bid','BUY',e.dec(stop),e.dec(target)) if minute else None
            if minute is not None and coarse_minute is None:
                mt += timedelta(minutes=1); continue
            # A missing/touched critical minute needs exact S5 evidence until
            # first touch, or a legitimate non-rollover M1 fallback.
            st=mt; minute_gaps=[]; fine_hit=None
            while st < mt+timedelta(minutes=1):
                child=s5map.get(st)
                if child is None: minute_gaps.append(e.stamp(st))
                else:
                    hit=e.touch(child,'bid','BUY',e.dec(stop),e.dec(target))
                    if hit:
                        fine_hit=dict(hit,interval_start_utc=e.stamp(st),interval_end_utc=e.stamp(st+timedelta(seconds=5)),resolution='S5')
                        break
                st += timedelta(seconds=5)
            if fine_hit is not None and not minute_gaps:
                observed=fine_hit; break
            can_m1 = minute is not None and coarse_minute is not None and \
                     coarse_minute['kind'] != 'AMBIGUOUS_BOTH' and not e.near_rollover(mt) and old.get('resolution') != 'S5'
            if can_m1:
                observed=dict(coarse_minute,interval_start_utc=e.stamp(mt),interval_end_utc=e.stamp(mt+timedelta(minutes=1)),resolution='M1')
                break
            if fine_hit is not None:
                gaps.extend(minute_gaps); observed=fine_hit; break
            if coarse_minute is not None:
                gaps.extend(minute_gaps)
                observed=dict(coarse_minute,interval_start_utc=e.stamp(mt),interval_end_utc=e.stamp(mt+timedelta(minutes=1)),resolution='M1_UNCONFIRMED_S5')
                break
            if minute is None: gaps.extend(minute_gaps)
            mt += timedelta(minutes=1)
        if observed is not None: break
        ts += timedelta(minutes=15)
    resolved=False
    if conflicts: verdict='UNRESOLVED_SOURCE_CONFLICT'
    elif gaps: verdict='UNRESOLVED_COVERAGE'
    elif observed is None: verdict='UNRESOLVED_NO_CONFIRMED_TOUCH'
    elif observed['kind']=='AMBIGUOUS_BOTH': verdict='UNRESOLVED_INTRABAR_ORDER'
    elif observed['resolution']=='M1_UNCONFIRMED_S5': verdict='UNRESOLVED_FINER_TIMING'
    else:
        resolved=True; verdict='RESOLVED_TO_'+observed['resolution']+'_INTERVAL'
    return dict(signal_key=case['signal']['signal_key'],entry_utc=e.stamp(lo),checked_through_utc=e.stamp(hi),
        original_kind=old['kind'],original_certainty=old['certainty'],original_resolution=old.get('resolution'),
        original_exit_price=old.get('exit_price'),original_interval_start_utc=old.get('interval_start_utc'),
        original_interval_end_utc=old.get('interval_end_utc'),verdict=verdict,resolved_to_candle_interval=resolved,
        new_observed_touch=observed,coarse_native_outcome_only=coarse_outcome,
        original_native_gap_slots=old.get('prior_native_unexplained_slots'),critical_missing_s5_slots=len(gaps),
        first_critical_gap_utc=gaps[0] if gaps else None,critical_gap_times_utc=gaps,
        frozen_native_non_touch_parents_used=proof_native_non_touch,assumed_weekend_native_slots=assumed_weekend_slots,
        s5_parents_requested=[e.stamp(t) for t in selected_parents],s5_parents_not_requested=max(0,len(candidates)-len(selected_parents)),
        s5_unavailable=s5_unavailable,source_conflicts=conflicts,tier_checks=checks,
        original_exit_kind_agrees=observed['kind']==old['kind'] if observed else None,
        original_exit_price_agrees=abs(e.dec(observed['exit_price'])-e.dec(old['exit_price'])) <= e.dec('0.0000000001')
            if observed and observed.get('exit_price') is not None and old.get('exit_price') is not None else None,
        diagnostic_only=True,original_reports_modified=False,account_replayed=False)


README='''# Bounded AUD/USD path coverage check

Exactly 25 previously reviewed unresolved paths from M_D1.25_B1.0 are checked.
No new strategy, filter, RR, account replay, financing or order is introduced.
This report contains NEW historical candle evidence as retrieved now. Earlier
matrix and reconciliation files remain unchanged; there is no revised CAGR.

Frozen M15 windows must match the original canonical SHA256. Available old raw
windows are opened read-only; evicted windows may be downloaded in full to prove
the checksum. Any revision is refused. New M1 candles span entry through the
old observed M15 exit bar only, in <=24-hour requests. S5 covers the old exit
parent first and at most five other earliest critical M15 parents per case.
Budgets: 240 cache-miss requests / 400 HTTP attempts across restarts,
six S5 parents per case, 16 MiB compressed private evidence, 64 MiB free reserve.
Unresolved cases are retained even when a budget or historical coverage ends.

Frozen M15 bars and observed M1 bars that do not touch either barrier prove
non-touch intervals, so absent child candles inside those intervals need not
be invented. Critical missing minutes require complete S5 coverage; absent S5
intervals remain unknown. No forward-filling, zero-volume bars or assumptions
that an absent candle means an unchanged price. The original conventional NY
weekend assumption is counted separately; historical holidays are not guessed.

Crosschecks cover MBA OHLC and execution-bid barrier consistency between tiers.
Complete children must aggregate within one tick; even a sub-tick/tick price
difference that changes barrier reachability is explicitly a source conflict.
Resolution is conditional on observed candle OHLC and the same stop/target/open
proxy semantics. It is not tick-order or broker-fill proof. Non-rollover,
single-barrier M1 fallback follows the old model only for paths previously at
M1 resolution. Original S5, rollover and ambiguous cases still require finer
evidence. An S5 candle touching both barriers remains ambiguous.

`coarse_native_outcome_only` can identify the stop/target side from frozen M15
without proving executable exit price or release timing. It is diagnostic,
not an account-ready correction. If any source conflict exists the case stays
unresolved. New fine candles are not claimed to equal old finer-tier snapshots
whose bodies were evicted; the packet records their checksums and new receipts.

path_coverage.csv is the overview; case_details.jsonl.gz contains full evidence
decisions. evidence_index.json records each retained body and provenance;
candles/ includes only this bounded audit's checked canonical bodies. Inputs
and code hashes tie checkpoints to the reviewed 25 cases. Resolved paths need
a later explicitly versioned replay before any account/portfolio claim changes.
The historical matrix found no passing plateau; this check does not select one.

The candle API schema was checked against OANDA's official instrument definition
and oanda/v20-openapi instrument endpoint schema. Candle absence is not evidence
of no market activity. No untouched out-of-sample claim is made.
'''


def csv_bytes(rows):
    fields=list(dict.fromkeys(k for r in rows for k in r)) or ['empty']
    out=io.StringIO(newline='');w=csv.DictWriter(out,fields);w.writeheader()
    for r in rows:w.writerow({k:json.dumps(v,sort_keys=True) if isinstance(v,(dict,list)) else v for k,v in r.items()})
    return out.getvalue().encode()


class Research:
    def __init__(self, environ=None, packet=None, pins=None):
        self.env=os.environ if environ is None else environ
        self.packet,self.pins=load_packet() if packet is None else (packet,pins or {})
        self.base=Path(self.env.get('AUDUSD_COVERAGE_OUTPUT_DIR','/data/p32_audusd_path_coverage')).resolve()
        old_roots=[Path(self.env.get('AUDUSD_MATRIX_OUTPUT_DIR','/data/p32_audusd_long_matrix')).resolve(),
                   Path(self.env.get('PULLBACK_OUTPUT_DIR','/data/p32_pullback_pass1')).resolve(),
                   Path(self.env.get('AUDUSD_RECON_OUTPUT_DIR','/data/p32_audusd_reconciliation')).resolve(),
                   Path(self.env.get('PULLBACK_LEGACY_CACHE_DIR','/data/p32_signal_admission')).resolve()]
        require(all(not self.base.is_relative_to(p) and not p.is_relative_to(self.base) for p in old_roots),
                'Use a separate coverage output directory; keep previous research directories unchanged')
        self.protocol=dict(service=VERSION,cases=25,pair='AUD_USD',candidate_id=self.packet['candidate_id'],
            rr=3,entry_limit_ticks=15,stop_buffer_ticks=10,max_candle_requests=MAX_REQUESTS,
            max_http_attempts=MAX_ATTEMPTS,max_s5_parents_per_case=MAX_S5_PARENTS,
            raw_cache_limit_bytes=MAX_CACHE_BYTES,storage_reserve_bytes=RESERVE,
            new_evidence_only=True,account_replay=False,orders_supported=False,
            original_matrix_signature=self.packet['matrix_signature'],inputs_sha256=e.sha(e.canon(self.packet)))
        self.proof=dict(code_sha256=sha_file(Path(__file__)),pins=self.pins)
        self.signature=e.sha(e.canon(dict(protocol=self.protocol,code=self.proof)))
        self.work=self.base/self.signature;self.lock=threading.RLock();self.thread=None
        self.state=dict(status='IDLE',phase='WAITING_FOR_STATUS',service=VERSION,signature=self.signature,
            cases_completed=0,cases_required=25,orders_supported=False,orders_sent=False,
            account_replayed=False,result_ready=False,candle_requests=0,http_attempts=0)
        self.old_paths=[old_roots[0]/'canonical_candles/candles.sqlite3',old_roots[1]/'canonical_candles/candles.sqlite3']
        self.old_paths += [old_roots[3]/r for r in ('replay_candles.sqlite3','gbp_valuation_windows_v2/replay_candles.sqlite3',
            'entry_limit_v1/new_execution_windows/replay_candles.sqlite3','gbp_execution_paths_v2/replay_candles.sqlite3')]

    def progress(self,**values):
        with self.lock:
            self.state.update(values,last_progress_utc=e.stamp(e.datetime.now(e.UTC)))
            if self.work.is_dir():atomic(self.work/'status.json',e.canon(self.state))

    def status(self,start=False):
        with self.lock:
            if start and self.thread is None and self.state['status']=='IDLE':
                self.state['status']='STARTING';self.thread=threading.Thread(target=self.run,daemon=True,name='audusd-25-path-coverage');self.thread.start()
            return dict(self.state,worker_alive=bool(self.thread and self.thread.is_alive()))

    def run(self):
        cache=None;handle=None;results=[]
        try:
            self.base.mkdir(parents=True,exist_ok=True);handle=(self.base/'runner.lock').open('ab')
            try:fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:raise ValueError('Another coverage check is already running')
            require(shutil.disk_usage(self.base).free >= RESERVE,'Keep 64 MiB free for the bounded audit; old caches were not changed')
            require(self.env.get('OANDA_ENV',self.env.get('OANDA_ENVIRONMENT','live')).lower()=='live','Keep OANDA_ENV=live for frozen broker evidence')
            self.work.mkdir(parents=True,exist_ok=True)
            atomic(self.work/'protocol.json',e.canon(self.protocol))
            token=next((self.env.get(k,'').strip() for k in ('OANDA_TOKEN','OANDA_API_TOKEN','OANDA_API_KEY','OANDA_ACCESS_TOKEN') if self.env.get(k,'').strip()),'')
            cache=CandleCache(self.work,self.packet,token,self.progress,self.old_paths)
            self.progress(status='RUNNING',phase='CHECK_BOUNDED_PATHS',candle_requests=cache.state['requests'],http_attempts=cache.state['attempts'])
            for case in self.packet['cases']:
                sid=case['signal']['signal_key'];path=self.work/(sid+'.json')
                if path.is_file():
                    cp=read_json(path)
                    require(cp['signature']==self.signature and e.sha(e.canon(cp['result']))==cp['sha256'],'Case checkpoint changed')
                    result=cp['result']
                else:
                    self.progress(phase='CHECK_PATH',entry_utc=case['signal']['entry_utc'],signal_key=sid)
                    try:
                        native=cache.native_for(case);minute=[]
                        for ident in m1_plan(case):minute.extend(cache.get(ident))
                        result=analyse_case(case,native,minute,lambda t:cache.get(identity('S5',t,t+timedelta(minutes=15))))
                    except (EvidenceConflict,BudgetStop,e.CandleFetchError) as exc:
                        if cache.reader.fatal:raise ValueError('Candle authentication failed; check the existing OANDA token') from None
                        result=dict(signal_key=sid,entry_utc=case['signal']['entry_utc'],original_kind=case['path']['kind'],
                            verdict='UNRESOLVED_SOURCE_CONFLICT' if isinstance(exc,EvidenceConflict) else 'UNRESOLVED_BOUNDED_DATA_UNAVAILABLE',
                            resolved_to_candle_interval=False,reason=str(exc),original_reports_modified=False,account_replayed=False)
                    atomic(path,e.canon(dict(signature=self.signature,result=result,sha256=e.sha(e.canon(result)))))
                results.append(result)
                self.progress(cases_completed=len(results),resolved_cases=sum(r['resolved_to_candle_interval'] for r in results),
                    remaining_uncertain=sum(not r['resolved_to_candle_interval'] for r in results))
            self.publish(results,cache,complete=True)
            self.progress(status='COMPLETE',phase='COMPLETE',result_ready=True,result='/results-summary.zip',
                verdict_counts=dict(Counter(r['verdict'] for r in results)),candle_requests=cache.state['requests'],http_attempts=cache.state['attempts'])
        except Exception as exc:
            message=str(exc) if isinstance(exc,(ValueError,e.CandleFetchError,OSError)) else type(exc).__name__
            self.progress(status='ERROR',phase='STOPPED',error=message)
            if cache is not None and results:
                try:self.publish(results,cache,complete=False);self.progress(result_ready=True,result='/results-summary.zip')
                except Exception:pass
        finally:
            if cache is not None:cache.close()
            if handle is not None:handle.close()

    def publish(self,results,cache,complete):
        self.progress(phase='PUBLISH_BOUNDED_REPORT')
        summaries=[{k:v for k,v in r.items() if k not in ('critical_gap_times_utc','tier_checks','source_conflicts')} for r in results]
        bodies={'REVIEW.md':README.encode(),'protocol.json':e.canon(self.protocol),'path_coverage.csv':csv_bytes(summaries),
            'case_details.jsonl.gz':gzip.compress(b''.join(e.canon(r)+b'\n' for r in results),mtime=0),
            'reviewed_inputs.json.gz':gzip.compress(e.canon(self.packet),mtime=0),
            'evidence_index.json':e.canon(dict(blocks=cache.state['blocks'],requests=cache.state['requests'],
                attempts=cache.state['attempts'],receipts=cache.state['receipts'])),
            'code_provenance.json':e.canon(self.proof)}
        manifest={n:dict(bytes=len(b),sha256=e.sha(b)) for n,b in bodies.items()}
        target=self.work/'AUDUSD_PATH_COVERAGE_RESULTS.zip';temp=target.with_suffix('.zip.tmp')
        try:
            with zipfile.ZipFile(temp,'w',zipfile.ZIP_DEFLATED,compresslevel=6) as z:
                for name,body in bodies.items():z.writestr(name,body)
                for key,w in cache.state['blocks'].items():
                    name='candles/'+key+'.json.gz';p=cache.directory/(key+'.json.gz')
                    require(sha_file(p)==w['compressed_sha256'],'Audit candle changed before report publication')
                    z.write(p,name);manifest[name]=dict(bytes=p.stat().st_size,sha256=w['compressed_sha256'])
                z.writestr('report_manifest.json',e.canon(manifest))
                z.writestr('RUN_COMPLETE.json',e.canon(dict(status='COMPLETE_BOUNDED_COVERAGE' if complete else 'PARTIAL',
                    service=VERSION,signature=self.signature,cases_completed=len(results),cases_required=25,
                    resolved_cases=sum(r['resolved_to_candle_interval'] for r in results),
                    uncertain_cases=sum(not r['resolved_to_candle_interval'] for r in results),
                    orders_supported=False,orders_sent=False,account_replayed=False,source_reports_modified=False)))
            require(temp.stat().st_size<MAX_ZIP_BYTES,'Report reached 20 MB cap; evidence and checkpoints kept')
            with zipfile.ZipFile(temp) as z:require(z.testzip() is None,'Report ZIP integrity failed')
            temp.replace(target)
        finally:temp.unlink(missing_ok=True)


def application(job):
    def app(environ,start_response):
        method,path=environ.get('REQUEST_METHOD','GET'),environ.get('PATH_INFO','/')
        status='200 OK';headers=[('Cache-Control','no-store')]
        if method not in ('GET','HEAD'):status='405 Method Not Allowed';payload=dict(error='GET and HEAD only',orders_supported=False)
        elif path in ('/','/health'):payload=dict(service=VERSION,cases=25,start_once='/status',protocol='/protocol',result='/results-summary.zip',orders_supported=False,account_replay=False)
        elif path=='/protocol':payload=job.protocol
        elif path=='/status':payload=job.status(start=method=='GET')
        elif path in ('/results-summary.zip','/results.zip'):
            file=job.work/'AUDUSD_PATH_COVERAGE_RESULTS.zip'
            if not job.state.get('result_ready') or not file.is_file():status='404 Not Found';payload=dict(error='No published report yet; open /status')
            else:
                start_response(status,headers+[('Content-Type','application/zip'),('Content-Length',str(file.stat().st_size)),
                    ('Content-Disposition','attachment; filename=AUDUSD_PATH_COVERAGE_RESULTS.zip')])
                if method=='HEAD':return []
                def chunks():
                    with file.open('rb') as f:yield from iter(lambda:f.read(256*1024),b'')
                return chunks()
        else:status='404 Not Found';payload=dict(error='Unknown route')
        body=e.canon(payload);start_response(status,headers+[('Content-Type','application/json'),('Content-Length',str(len(body)))])
        return [] if method=='HEAD' else [body]
    return app


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--run',action='store_true');args=parser.parse_args()
    job=Research()
    if args.run:
        job.run();print(json.dumps(job.status(),indent=2));raise SystemExit(0 if job.state['status']=='COMPLETE' else 1)
    class Quiet(WSGIRequestHandler):
        def log_message(self,*args):pass
    port=int(os.getenv('PORT','8080'));print(json.dumps(dict(service=VERSION,port=port,orders_supported=False)),flush=True)
    with make_server('0.0.0.0',port,application(job),handler_class=Quiet) as server:server.serve_forever()


if __name__=='__main__':main()
