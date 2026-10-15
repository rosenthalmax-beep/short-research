#!/usr/bin/env python3
"""P32 historical execution correction. Research service; candle GETs only.

This pass reprices the frozen accepted cohort. It does not regenerate strategy
signals, replay portfolio admissions, submit orders, or publish portfolio CAGR.
Python 3.10+. Run python app.py; GET /status starts one resumable run.
"""
import argparse
import base64
import csv
import hashlib
import io
import json
import math
import os
import re
import sqlite3
import statistics
import tempfile
import threading
import time
import zipfile
import zlib
from collections import Counter, OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener
from wsgiref.simple_server import WSGIRequestHandler, make_server
from zoneinfo import ZoneInfo

from reference_payload import REFERENCE_B85, REFERENCE_SHA256, SEED_MANIFEST

VERSION = 'P32_EXECUTION_REPLAY_V1_2026_10_15'
RESULT_NAME = 'P32_EXECUTION_REPLAY_RESULTS.zip'
UTC = timezone.utc
NY = ZoneInfo('America/New_York')
STEP = {'H1': 3600, 'M15': 900, 'M1': 60, 'S5': 5}
PAIRS = ('AUD_JPY', 'AUD_USD', 'EUR_CHF', 'EUR_GBP', 'EUR_JPY', 'EUR_USD', 'GBP_USD', 'USD_CAD', 'USD_JPY')
DAYS = ('MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY')
VARIANTS = ('ROUNDED_MID_FIXED_ENTRY', 'EXECUTABLE_10T_ENTRY_PROXY')
LOCK = threading.RLock()
STATE = dict(status='IDLE', version=VERSION, orders_supported=False,
             scope='FROZEN_COHORT_EXECUTION_REBUILD', full_portfolio_validated=False)
SERVICE_OFFLINE = False
SERVICE_OUTPUT = None

PROTOCOL = {
    'version': VERSION,
    'scope': 'Frozen 3391 accepted trades; independently reprice both variants, no re-admission.',
    'rounding': 'Exact Python round/format arithmetic of supplied executor for reference, stop, target and priceBound. Decimal comparisons after rounding.',
    'entry': 'First BID/ASK open of the native entry candle. A candle-price proxy, not a recovered executor quote or guaranteed fill.',
    'entry_guard': '10 ticks, fixed. BUY uses ASK; SELL uses BID. Fixed target calculated from rounded reference and stop.',
    'exits': 'Long BID / short ASK, DEFAULT protection trigger. Stop gap uses first observed open beyond stop; TP gap uses observed better open.',
    'sequence': 'Native H1/M15 -> M1 at first possible touch; S5 if M1 touches both or lies within 30 minutes of NY 17:00. Still-ambiguous S5 stays unresolved.',
    'horizon': 'Original archive cutoff 2026-09-20T18:29:00Z. Continue past original exits; final partial native interval checked in M1.',
    'gaps': 'Missing observations before a touch qualify it. Conventional NY Fri17-Sun17 closure is an explicit assumption; no blanket historical holiday/daily-break imputation.',
    'funding': 'Separate sensitivity only: frozen 14-Oct-2026 annual rates/weekday schedule and entry notional. No historical rate/holiday/FX-conversion reconstruction.',
    'r': 'Quote-currency price R / entry-to-stop distance; also R / worst-permitted-entry-to-stop distance. No home-currency/whole-unit/NAV sizing claim.',
    'portfolio': 'Withheld: 27 original raw strategy streams unavailable, gates not replayed, latency and broker cancellations not reconstructed.',
    'costs': 'Bid/ask spread already in modeled prices. Never subtract a spread overlay again.',
    'live_services': 'No executor/probe access. Only official broker candle GET routes can be requested.',
}


def canon(obj):
    return json.dumps(obj, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def dt(value):
    value = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    require(value.tzinfo is not None, 'Explicit UTC offset required')
    return value.astimezone(UTC)


def stamp(value):
    return dt(value).isoformat().replace('+00:00', 'Z')


def floor_time(value, seconds):
    return datetime.fromtimestamp(int(dt(value).timestamp()) // seconds * seconds, UTC)


def dec(value):
    result = Decimal(str(value))
    require(result.is_finite(), 'Non-finite price')
    return result


def digits(pair):
    require(pair in PAIRS, 'Unapproved pair')
    return 3 if pair.endswith('_JPY') else 5


def tick(pair):
    return Decimal(1).scaleb(-digits(pair))


def broker_price(pair, value):
    # This deliberately matches the supplied live Python executor, including its
    # half-tick behavior. Do not replace this with a different rounding policy.
    require(math.isfinite(float(value)), 'Non-finite order price')
    return dec(format(round(float(value), digits(pair)), '.%df' % digits(pair)))


def levels(pair, side, reference, stop, rr):
    require(side in ('BUY', 'SELL') and math.isfinite(float(rr)) and float(rr) > 0, 'Invalid direction/RR')
    sign = 1 if side == 'BUY' else -1
    ref, sl = broker_price(pair, reference), broker_price(pair, stop)
    risk = sign * (float(ref) - float(sl))
    require(risk > 0, 'Reference stop must be on the loss side')
    target = broker_price(pair, float(ref) + sign * risk * float(rr))
    bound = broker_price(pair, float(ref) + sign * 10 * float(tick(pair)))
    return dict(reference_entry=ref, stop=sl, target=target, price_bound=bound)


def entry_preview(pair, side, reference, stop, rr, bid, ask):
    result = levels(pair, side, reference, stop, rr)
    bid, ask = broker_price(pair, bid), broker_price(pair, ask)
    require(ask >= bid > 0, 'Invalid quote')
    sign = 1 if side == 'BUY' else -1
    fill = ask if sign == 1 else bid
    deviation = max(dec(0), sign * (fill - result['reference_entry']) / tick(pair))
    status = 'ADMITTED_ENTRY_PROXY'
    if deviation > 10:
        status = 'REJECTED_10T_ENTRY_PROXY'
    elif sign * (fill - result['stop']) <= 0:
        status = 'REJECTED_STOP_GEOMETRY_PROXY'
    elif pair == 'EUR_CHF' and bid <= result['target']:
        status = 'REJECTED_TARGET_ALREADY_PASSED_PROXY'
    result.update(fill=fill, bid=bid, ask=ask, adverse_ticks=deviation,
                  entry_spread_ticks=(ask - bid) / tick(pair), status=status,
                  stop_distance=sign * (fill - result['stop']),
                  worst_stop_distance=sign * (result['price_bound'] - result['stop']))
    return result


def load_reference():
    raw = zlib.decompress(base64.b85decode(REFERENCE_B85))
    require(sha(raw) == REFERENCE_SHA256, 'Pinned reference checksum failed')
    data = json.loads(raw)
    ref = data['reference']
    require(len(ref['book']) == len(ref['geometry']) == 3391, 'Frozen cohort count changed')
    require(sha(canon(ref['book'])) == ref['book_sha256'], 'Frozen accepted book changed')
    registry = {x['archived_sid']: x for x in data['registry']}
    require(len(registry) == 32, 'Expected 32 strategy identities')
    for i, (record, geo) in enumerate(zip(ref['book'], ref['geometry'])):
        require(geo['index'] == i and all(record[k] == geo[k] for k in ('sid', 'pair', 'side')), 'Geometry identity mismatch')
        rule = registry[record['sid']]
        require(record['pair'] == rule['pair'] and record['side'] == rule['side'] and record['tf'] == rule['timeframe'], 'Registry mismatch')
        require(float(record['rr']) in [float(x) for x in rule['rr'].split(';')], 'RR mismatch')
        levels(record['pair'], record['side'], geo['reference_entry'], geo['stop'], record['rr'])
    return data


class NeedData(Exception):
    def __init__(self, identity):
        self.identity = identity
        super().__init__('Additional native candles required')


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class CandleFetchError(RuntimeError):
    """A controlled, credential-free error message from the candle reader."""


class CandleReader:
    """The only network operation exposed is an allowlisted candle GET."""
    def __init__(self, token, broker, requests_per_second=4, transport=None):
        require(broker in ('https://api-fxtrade.oanda.com', 'https://api-fxpractice.oanda.com'), 'Invalid broker host')
        require(0 < requests_per_second <= 5, 'Request rate must be >0 and <=5')
        self.token, self.broker, self.transport = token, broker, transport
        self.opener = build_opener(NoRedirect())
        self.lock = threading.Lock()
        self.next_at = 0.0
        self.spacing = 1 / requests_per_second
        self.receipts = []
        self.fatal = False

    def get(self, identity):
        pair, gran = identity['pair'], identity['granularity']
        require(pair in PAIRS and gran in STEP, 'Only approved candle requests are supported')
        require(identity['broker'] == self.broker, 'Seed and requested broker differ')
        require(not self.fatal, 'Candle reader stopped after authentication failure')
        params = {'from': identity['start'], 'to': identity['end'], 'granularity': gran,
                  'price': 'MBA', 'smooth': 'false', 'includeFirst': 'true'}
        require(0 < (dt(params['to']) - dt(params['from'])).total_seconds() <= 4800 * STEP[gran], 'Candle request exceeds limit')
        url = self.broker + '/v3/instruments/' + pair + '/candles?' + urlencode(params)
        for attempt in range(3):
            with self.lock:
                delay = max(0, self.next_at - time.monotonic())
                self.next_at = max(time.monotonic(), self.next_at) + self.spacing
            if delay:
                time.sleep(delay)
            req = Request(url, method='GET', headers={'Authorization': 'Bearer ' + self.token,
                          'Accept': 'application/json', 'Accept-Datetime-Format': 'RFC3339', 'User-Agent': VERSION})
            try:
                if self.transport:
                    data = self.transport(req)
                else:
                    with self.opener.open(req, timeout=40) as response:
                        raw = response.read(16 * 1024 * 1024 + 1)
                    require(len(raw) <= 16 * 1024 * 1024, 'Candle response exceeds size limit')
                    data = json.loads(raw)
                require(isinstance(data, dict), 'Candle response must be an object')
                with self.lock:
                    self.receipts.append(dict(method='GET', pair=pair, granularity=gran, params=params,
                                              received_utc=stamp(datetime.now(UTC)), response_sha256=sha(canon(data))))
                return data
            except Exception as exc:
                code = exc.code if isinstance(exc, HTTPError) else None
                if code in (401, 403):
                    self.fatal = True
                with self.lock:
                    self.receipts.append(dict(method='GET', pair=pair, granularity=gran, params=params,
                                              attempt=attempt + 1, error='HTTP_' + str(code) if code else type(exc).__name__))
                if code in (400, 401, 403, 404) or isinstance(exc, ValueError) or attempt == 2:
                    raise CandleFetchError('Candle GET failed: ' + ('HTTP ' + str(code) if code else type(exc).__name__)) from None
                time.sleep(0.5 * (attempt + 1))


class CandleStore:
    def __init__(self, directory, broker, reader=None):
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(directory / 'replay_candles.sqlite3'), check_same_thread=False)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('CREATE TABLE IF NOT EXISTS blocks (key TEXT PRIMARY KEY, identity TEXT NOT NULL, body BLOB NOT NULL, sha256 TEXT NOT NULL, origin TEXT NOT NULL)')
        self.db.execute('CREATE TABLE IF NOT EXISTS imports (sha256 TEXT PRIMARY KEY)')
        self.db.commit()
        self.broker, self.reader = broker, reader
        self.lock = threading.RLock()
        self.fetch_locks = {}
        self.index = defaultdict(list)
        self.memo = OrderedDict()
        self.used = set()
        self.downloads = 0
        self.cache_reads = 0
        for key, encoded in self.db.execute('SELECT key,identity FROM blocks'):
            ident = json.loads(encoded)
            require(ident['broker'] == broker, 'Existing cache belongs to another broker')
            self.index[(ident['pair'], ident['granularity'])].append((dt(ident['start']), dt(ident['end']), key))

    def identity(self, pair, gran, start, end):
        require(pair in PAIRS and gran in STEP, 'Invalid candle request')
        require(start <= end and start == floor_time(start, STEP[gran]) and end == floor_time(end, STEP[gran]), 'Unaligned candle range')
        return dict(schema='MBA_UNSMOOTHED_V1', broker=self.broker, pair=pair, granularity=gran,
                    start=stamp(start), end=stamp(end), price='MBA', smooth=False, includeFirst=True)

    @staticmethod
    def validate(obj, ident):
        require(obj.get('instrument') == ident['pair'] and obj.get('granularity') == ident['granularity'], 'Candle response identity mismatch')
        require(isinstance(obj.get('candles'), list), 'Candle array absent')
        previous, rows = None, []
        step = STEP[ident['granularity']]
        start, end = dt(ident['start']), dt(ident['end'])
        for c in obj['candles']:
            t = dt(c['time'])
            require(previous is None or t > previous, 'Duplicate or unordered candle time')
            previous = t
            require(start <= t < end and floor_time(t, step) == t, 'Candle time outside/alignment mismatch')
            if c.get('complete') is not True or t + timedelta(seconds=step) > end:
                continue
            for component in ('mid', 'bid', 'ask'):
                require(isinstance(c.get(component), dict), 'Missing MBA component')
                p = {key: dec(c[component][key]) for key in 'ohlc'}
                require(0 < p['l'] <= min(p['o'], p['c']) <= max(p['o'], p['c']) <= p['h'], 'Invalid OHLC')
            require(dec(c['ask']['o']) >= dec(c['bid']['o']) and dec(c['ask']['c']) >= dec(c['bid']['c']), 'Negative boundary spread')
            rows.append(c)
        return rows

    def import_seeds(self, directory):
        for part in SEED_MANIFEST:
            path = Path(directory) / part['file']
            require(path.is_file(), 'Upload all supplied files; missing ' + part['file'])
            require(sha(path.read_bytes()) == part['sha256'], 'Seed file checksum mismatch: ' + part['file'])
            if self.db.execute('SELECT 1 FROM imports WHERE sha256=?', (part['sha256'],)).fetchone():
                continue
            with zipfile.ZipFile(path) as z:
                entries = json.loads(z.read('source_candle_index.json'))
                require(len(entries) == part['blocks'], 'Seed block count mismatch')
                for row in entries:
                    require(re.fullmatch(r'source_candles/[0-9a-f]{64}\.json', row['file']), 'Invalid seed member')
                    ident = {k: v for k, v in row.items() if k not in ('file', 'sha256')}
                    require(ident['broker'] == self.broker, 'Saved candles are from the live broker; set OANDA_ENV=live')
                    key = sha(canon(ident))
                    require(row['file'] == 'source_candles/' + key + '.json', 'Source identity hash mismatch')
                    raw = z.read(row['file'])
                    require(sha(raw) == row['sha256'], 'Source block checksum mismatch')
                    self.validate(json.loads(raw), ident)
                    existing = self.db.execute('SELECT sha256 FROM blocks WHERE key=?', (key,)).fetchone()
                    require(existing is None or existing[0] == row['sha256'], 'Source cache conflict')
                    if existing is None:
                        self.db.execute('INSERT INTO blocks VALUES (?,?,?,?,?)', (key, canon(ident).decode(), zlib.compress(raw, 6), row['sha256'], part['file']))
                        self.index[(ident['pair'], ident['granularity'])].append((dt(ident['start']), dt(ident['end']), key))
            self.db.execute('INSERT INTO imports VALUES (?)', (part['sha256'],))
            self.db.commit()
            progress(seed_parts_imported=self.db.execute('SELECT COUNT(*) FROM imports').fetchone()[0])

    def intervals(self, pair, gran):
        with self.lock:
            return sorted(self.index[(pair, gran)])

    def segment_end(self, pair, gran, start, desired_end):
        end = start
        for begin, finish, _ in self.intervals(pair, gran):
            if finish <= start:
                continue
            if begin <= end:
                end = max(end, finish)
            elif end == start:
                return min(begin, desired_end)
            else:
                break
        return min(end, desired_end) if end > start else desired_end

    def _read(self, key):
        with self.lock:
            self.used.add(key)
            self.cache_reads += 1
            if key in self.memo:
                self.memo.move_to_end(key)
                return self.memo[key]
            encoded, compressed, checksum = self.db.execute('SELECT identity,body,sha256 FROM blocks WHERE key=?', (key,)).fetchone()
        raw = zlib.decompress(compressed)
        require(sha(raw) == checksum, 'Cached candle checksum failed')
        ident = json.loads(encoded)
        rows = self.validate(json.loads(raw), ident)
        with self.lock:
            self.memo[key] = rows
            while len(self.memo) > 24:
                self.memo.popitem(last=False)
        return rows

    def _fetch(self, pair, gran, start, end):
        ident = self.identity(pair, gran, start, end)
        if self.reader is None:
            raise NeedData(ident)
        key = sha(canon(ident))
        with self.lock:
            fetch_lock = self.fetch_locks.setdefault(key, threading.Lock())
        with fetch_lock:
            with self.lock:
                if self.db.execute('SELECT 1 FROM blocks WHERE key=?', (key,)).fetchone():
                    return
            obj = self.reader.get(ident)
            rows = self.validate(obj, ident)
            raw = canon(dict(instrument=pair, granularity=gran, candles=rows))
            with self.lock:
                self.db.execute('INSERT INTO blocks VALUES (?,?,?,?,?)', (key, canon(ident).decode(), zlib.compress(raw, 6), sha(raw), 'NEW_DOWNLOAD'))
                self.db.commit()
                self.index[(pair, gran)].append((start, end, key))
                self.downloads += 1

    def range(self, pair, gran, start, end):
        self.identity(pair, gran, start, end)
        cursor = start
        while cursor < end:
            intervals = self.intervals(pair, gran)
            covering = [finish for begin, finish, _ in intervals if begin <= cursor < finish]
            if covering:
                cursor = min(end, max(covering))
                continue
            next_start = min((begin for begin, _, _ in intervals if begin > cursor), default=end)
            finish = min(end, next_start, cursor + timedelta(seconds=4800 * STEP[gran]))
            self._fetch(pair, gran, cursor, finish)
        result = {}
        for begin, finish, key in self.intervals(pair, gran):
            if begin >= end or finish <= start:
                continue
            for c in self._read(key):
                t = dt(c['time'])
                if start <= t < end:
                    previous = result.get(t)
                    if previous is not None:
                        require(all(previous[s] == c[s] for s in ('mid', 'bid', 'ask')), 'Overlapping source candles disagree')
                    result[t] = c
        return [result[t] for t in sorted(result)]

    def export(self, archive):
        references, fresh = [], []
        with self.lock:
            # Include cache provenance on resumed runs too: completed checkpoints
            # need not reread their old candles. This is the available cache,
            # not a claim that every block was consulted in the current process.
            for (key,) in self.db.execute('SELECT key FROM blocks ORDER BY key').fetchall():
                ident, compressed, checksum, origin = self.db.execute('SELECT identity,body,sha256,origin FROM blocks WHERE key=?', (key,)).fetchone()
                row = dict(json.loads(ident), sha256=checksum, file='source_candles/' + key + '.json', origin=origin)
                if origin == 'NEW_DOWNLOAD':
                    raw = zlib.decompress(compressed)
                    require(sha(raw) == checksum, 'Export source checksum failed')
                    archive.writestr(row['file'], raw)
                    fresh.append(row)
                else:
                    references.append(row)
        archive.writestr('new_source_candle_index.json', canon(fresh))
        archive.writestr('available_seed_source_index.json', canon(references))

    def close(self):
        self.db.close()


def conventional_weekend(t):
    local = dt(t).astimezone(NY)
    return local.weekday() == 5 or (local.weekday() == 4 and local.hour >= 17) or (local.weekday() == 6 and local.hour < 17)


def coverage(rows, start, end, seconds):
    available = {dt(x['time']) for x in rows}
    missing, weekend = [], 0
    t = start
    while t < end:
        if t not in available:
            if conventional_weekend(t):
                weekend += 1
            else:
                missing.append(stamp(t))
        t += timedelta(seconds=seconds)
    return dict(unexplained_missing_slots=len(missing), assumed_weekend_slots=weekend,
                first_unexplained_gap_utc=missing[0] if missing else None)


def touch(candle, component, side, stop, target):
    p = {k: dec(candle[component][k]) for k in 'ohlc'}
    sign = 1 if side == 'BUY' else -1
    if sign * (p['o'] - stop) <= 0:
        return dict(kind='STOP', exit_price=float(p['o']), open_gap=p['o'] != stop)
    if sign * (p['o'] - target) >= 0:
        return dict(kind='TARGET', exit_price=float(p['o']), open_gap=p['o'] != target)
    stop_hit = p['l'] <= stop if sign == 1 else p['h'] >= stop
    target_hit = p['h'] >= target if sign == 1 else p['l'] <= target
    if stop_hit and target_hit:
        return dict(kind='AMBIGUOUS_BOTH', exit_price=None, open_gap=False)
    if stop_hit:
        return dict(kind='STOP', exit_price=float(stop), open_gap=False)
    if target_hit:
        return dict(kind='TARGET', exit_price=float(target), open_gap=False)
    return None


def crosscheck(parent, children, component, count, pair):
    if not children:
        return 'NO_CHILD_DATA'
    tolerance = tick(pair)
    p = {k: dec(parent[component][k]) for k in 'ohlc'}
    rows = [{k: dec(x[component][k]) for k in 'ohlc'} for x in children]
    # Even incomplete children cannot exceed their parent's observed price range.
    if min(x['l'] for x in rows) < p['l'] - tolerance or max(x['h'] for x in rows) > p['h'] + tolerance:
        return 'SOURCE_TIER_MISMATCH'
    if len(children) == count:
        combined = dict(o=rows[0]['o'], c=rows[-1]['c'], h=max(x['h'] for x in rows), l=min(x['l'] for x in rows))
        if any(abs(combined[k] - p[k]) > tolerance for k in 'ohlc'):
            return 'SOURCE_TIER_MISMATCH'
        return 'MATCH'
    return 'INCOMPLETE_CHILD_INTERVAL'


def near_rollover(t):
    local = dt(t).astimezone(NY)
    return abs(local.hour * 60 + local.minute - 17 * 60) <= 30


def refine(store, pair, side, component, stop, target, parent, gran):
    start = dt(parent['time'])
    end = start + timedelta(seconds=STEP[gran])
    coarse = touch(parent, component, side, stop, target)
    require(coarse is not None, 'Refinement requires a touched parent')
    if gran == 'S5':
        return dict(coarse, interval_start_utc=stamp(start), interval_end_utc=stamp(end),
                    resolution='S5', earlier_gap=False,
                    certainty='AMBIGUOUS_INTRABAR_ORDER' if coarse['kind'] == 'AMBIGUOUS_BOTH' else 'RESOLVED_TO_CANDLE_INTERVAL')
    if gran == 'M1' and coarse['kind'] != 'AMBIGUOUS_BOTH' and not near_rollover(start):
        return dict(coarse, interval_start_utc=stamp(start), interval_end_utc=stamp(end),
                    resolution='M1', earlier_gap=False, certainty='RESOLVED_TO_CANDLE_INTERVAL')
    child_gran = 'S5' if gran == 'M1' else 'M1'
    children = store.range(pair, child_gran, start, end)
    check = crosscheck(parent, children, component, STEP[gran] // STEP[child_gran], pair)
    for child in children:
        if touch(child, component, side, stop, target) is None:
            continue
        result = refine(store, pair, side, component, stop, target, child, child_gran)
        before = coverage(children, start, dt(result['interval_end_utc']), STEP[child_gran])
        earlier_gap = result['earlier_gap'] or before['unexplained_missing_slots'] > 0
        result['earlier_gap'] = earlier_gap
        result[gran.lower() + '_child_crosscheck'] = check
        if check == 'SOURCE_TIER_MISMATCH':
            result['certainty'] = 'SOURCE_TIER_MISMATCH'
        elif earlier_gap and result['certainty'] == 'RESOLVED_TO_CANDLE_INTERVAL':
            result['certainty'] = 'TOUCH_WITH_EARLIER_UNEXPLAINED_GAP'
        return result
    return dict(coarse, interval_start_utc=stamp(start), interval_end_utc=stamp(end),
                resolution=gran, earlier_gap=False,
                certainty='SOURCE_TIER_MISMATCH' if check == 'SOURCE_TIER_MISMATCH' else 'UNCONFIRMED_FINER_TOUCH')


def first_exit(store, record, geometry, component, horizon):
    start, archived_exit = dt(record['entry']), dt(record['exit'])
    cursor = start
    unexplained, weekends = 0, 0
    last_quote, last_quote_time = None, None
    first_gap = None
    while cursor < horizon:
        native_end = floor_time(horizon, STEP[record['tf']])
        gran = record['tf'] if cursor < native_end else 'M1'
        limit = native_end if gran == record['tf'] else floor_time(horizon, 60)
        if cursor >= limit:
            break
        desired = min(limit, archived_exit if cursor < archived_exit else cursor + timedelta(days=7))
        finish = store.segment_end(record['pair'], gran, cursor, desired)
        require(finish > cursor, 'Replay made no progress')
        bars = store.range(record['pair'], gran, cursor, finish)
        for bar in bars:
            hit = touch(bar, component, record['side'], geometry['stop'], geometry['target'])
            if hit is None:
                last_quote = float(bar[component]['c'])
                last_quote_time = stamp(dt(bar['time']) + timedelta(seconds=STEP[gran]))
                continue
            refined = refine(store, record['pair'], record['side'], component, geometry['stop'], geometry['target'], bar, gran)
            prior = coverage(bars, cursor, dt(bar['time']), STEP[gran])
            unexplained += prior['unexplained_missing_slots']
            weekends += prior['assumed_weekend_slots']
            first_gap = first_gap or prior['first_unexplained_gap_utc']
            if unexplained and refined['certainty'] == 'RESOLVED_TO_CANDLE_INTERVAL':
                refined['certainty'] = 'TOUCH_WITH_EARLIER_UNEXPLAINED_GAP'
            refined.update(native_interval_start_utc=bar['time'],
                           native_interval_end_utc=stamp(dt(bar['time']) + timedelta(seconds=STEP[gran])),
                           prior_native_unexplained_slots=unexplained, prior_native_assumed_weekend_slots=weekends,
                           first_native_unexplained_gap_utc=first_gap)
            return refined
        cov = coverage(bars, cursor, finish, STEP[gran])
        unexplained += cov['unexplained_missing_slots']
        weekends += cov['assumed_weekend_slots']
        first_gap = first_gap or cov['first_unexplained_gap_utc']
        cursor = finish
    return dict(kind='OPEN_AT_FIXED_CUTOFF', exit_price=None, resolution=None, open_gap=False,
                interval_start_utc=None, interval_end_utc=None,
                certainty='NO_TOUCH_WITH_UNEXPLAINED_GAPS' if unexplained else 'NO_TOUCH_OBSERVED_TO_CUTOFF',
                prior_native_unexplained_slots=unexplained, prior_native_assumed_weekend_slots=weekends,
                first_native_unexplained_gap_utc=first_gap, last_observed_close=last_quote,
                last_observed_close_time_utc=last_quote_time, observed_through_utc=stamp(horizon))


def funding_sensitivity(record, geometry, outcome, schedule):
    if outcome['kind'] not in ('STOP', 'TARGET') or outcome['certainty'] != 'RESOLVED_TO_CANDLE_INTERVAL':
        return dict(funding_basis='NOT_ESTIMATED_FOR_UNRESOLVED_OR_OPEN_PATH')
    start, low, high = dt(record['entry']), dt(outcome['interval_start_utc']), dt(outcome['interval_end_utc'])
    local = start.astimezone(NY)
    day = local.date()
    charged, uncertain = 0.0, 0.0
    while day <= high.astimezone(NY).date():
        boundary = datetime(day.year, day.month, day.day, 17, tzinfo=NY).astimezone(UTC)
        amount = schedule['days'][DAYS[day.weekday()]]
        if start < boundary < low:
            charged += amount
        elif start <= boundary < high and boundary >= low:
            uncertain += amount
        day += timedelta(days=1)
    # Entry at the rollover boundary also has unresolved intra-minute ordering.
    if start == datetime(local.year, local.month, local.day, 17, tzinfo=NY).astimezone(UTC) and low > start:
        uncertain += schedule['days'][DAYS[local.weekday()]]
    rate = schedule['long_rate'] if record['side'] == 'BUY' else schedule['short_rate']
    scale = float(geometry['fill'] / geometry['stop_distance']) / 365.0
    estimates = (rate * charged * scale, rate * (charged + uncertain) * scale)
    adverse = (-0.06 * charged * scale, -0.06 * (charged + uncertain) * scale)
    return dict(funding_basis='14_OCT_2026_RATE_AND_WEEKDAY_SCHEDULE_ENTRY_NOTIONAL_SENSITIVITY',
                funding_days_definite=charged, funding_days_exit_or_entry_order_uncertain=uncertain,
                funding_snapshot_annual_rate=rate,
                funding_snapshot_r_low=min(estimates), funding_snapshot_r_high=max(estimates),
                funding_adverse_6pct_r_low=min(adverse), funding_adverse_6pct_r_high=max(adverse),
                historical_funding_known=False, holiday_charging_known=False)


def serial_geometry(g):
    return {k: float(v) if isinstance(v, Decimal) else v for k, v in g.items()}


def replay_one(store, data, index, variant):
    record = data['reference']['book'][index]
    original = data['reference']['geometry'][index]
    base = dict(index=index, variant=variant, strategy_id=record['sid'], pair=record['pair'], side=record['side'],
                timeframe=record['tf'], entry_utc=record['entry'], archived_exit_utc=record['exit'], archived_r=record['r'],
                archived_fill=original['fill'], archived_stop=original['stop'], archived_target=original['target'],
                archived_geometry_source=original['geometry_source'], archived_geometry_validation=original['geometry_validation'],
                full_portfolio_trade_admission='NOT_REPLAYED', source_horizon_utc=data['historical_end'])
    try:
        geometry = levels(record['pair'], record['side'], original['reference_entry'], original['stop'], record['rr'])
        sign = 1 if record['side'] == 'BUY' else -1
        if variant == VARIANTS[0]:
            geometry['fill'] = broker_price(record['pair'], original['fill'])
            geometry['stop_distance'] = sign * (geometry['fill'] - geometry['stop'])
            geometry['worst_stop_distance'] = sign * (geometry['price_bound'] - geometry['stop'])
            geometry['status'] = 'FROZEN_FILL_CONTROL_NO_ENTRY_GUARD'
            component = 'mid'
        else:
            entry = dt(record['entry'])
            candles = store.range(record['pair'], record['tf'], entry, entry + timedelta(seconds=STEP[record['tf']]))
            first = next((c for c in candles if dt(c['time']) == entry), None)
            if first is None:
                return dict(base, status='UNRESOLVED_ENTRY', entry_status='MISSING_EXACT_NATIVE_ENTRY_BAR')
            geometry = entry_preview(record['pair'], record['side'], original['reference_entry'], original['stop'], record['rr'], first['bid']['o'], first['ask']['o'])
            component = 'bid' if sign == 1 else 'ask'
        base.update({'model_' + k: v for k, v in serial_geometry(geometry).items()})
        base.update(entry_status=geometry['status'], exit_component=component,
                    entry_model='FROZEN_MODELED_FILL' if variant == VARIANTS[0] else 'FIRST_NATIVE_OPEN_PROXY',
                    live_latency_and_fill_cancellation_modeled=False)
        if geometry['status'].startswith('REJECTED_'):
            return dict(base, status='ENTRY_PROXY_REJECTED')
        require(geometry['stop_distance'] > 0, 'Invalid modeled fill/stop geometry')
        outcome = first_exit(store, record, geometry, component, dt(data['historical_end']))
        base.update(outcome)
        base['status'] = 'CLOSED_RESOLVED' if outcome['kind'] in ('STOP', 'TARGET') and outcome['certainty'] == 'RESOLVED_TO_CANDLE_INTERVAL' else 'OPEN_AT_CUTOFF' if outcome['kind'] == 'OPEN_AT_FIXED_CUTOFF' and outcome['certainty'] == 'NO_TOUCH_OBSERVED_TO_CUTOFF' else 'UNRESOLVED_PATH'
        base['extended_beyond_archived_exit'] = bool(outcome.get('interval_end_utc') and dt(outcome['interval_end_utc']) > dt(record['exit']))
        if outcome.get('exit_price') is not None:
            movement = sign * (dec(outcome['exit_price']) - geometry['fill'])
            base['observed_path_price_r'] = float(movement / geometry['stop_distance'])
            base['observed_path_worst_bound_budget_r'] = float(movement / geometry['worst_stop_distance'])
            base['price_r_included_in_resolved_summary'] = base['status'] == 'CLOSED_RESOLVED'
        if outcome['kind'] == 'AMBIGUOUS_BOTH':
            base['same_bar_stop_r'] = float(sign * (geometry['stop'] - geometry['fill']) / geometry['stop_distance'])
            base['same_bar_target_r'] = float(sign * (geometry['target'] - geometry['fill']) / geometry['stop_distance'])
        base.update(funding_sensitivity(record, geometry, outcome, data['funding'][record['pair']]))
        if base['status'] == 'CLOSED_RESOLVED':
            base['price_plus_snapshot_funding_r_low'] = base['observed_path_price_r'] + base['funding_snapshot_r_low']
            base['price_plus_snapshot_funding_r_high'] = base['observed_path_price_r'] + base['funding_snapshot_r_high']
        return base
    except NeedData as exc:
        return dict(base, status='NEEDS_CANDLE_DATA', pending_request=exc.identity)
    except Exception as exc:
        # Deliberately omit exception strings that a transport might populate
        # with a URL or credential. Native validation errors here are controlled.
        safe = str(exc) if isinstance(exc, (ValueError, CandleFetchError)) else type(exc).__name__
        return dict(base, status='ERROR', error=safe)


def summarize(rows):
    result = []
    for variant in VARIANTS:
        selected = [x for x in rows if x['variant'] == variant]
        closed = [x for x in selected if x['status'] == 'CLOSED_RESOLVED']
        wins = sum(x['observed_path_price_r'] > 0 for x in closed)
        result.append(dict(variant=variant, attempted=len(selected), status_counts=dict(Counter(x['status'] for x in selected)),
                           resolved_closed=len(closed), resolved_price_wins=wins,
                           resolved_only_price_win_rate_pct=100 * wins / len(closed) if closed else None,
                           resolved_only_mean_price_r=statistics.fmean(x['observed_path_price_r'] for x in closed) if closed else None,
                           resolved_only_net_price_r=sum(x['observed_path_price_r'] for x in closed) if closed else None,
                           statistic_scope='RESOLVED_SUBSET_OF_FROZEN_COHORT_NOT_FULL_PORTFOLIO',
                           annual_return=None, cagr=None, portfolio_drawdown=None))
    return result


def progress(**items):
    with LOCK:
        STATE.update(items)


def output_dir():
    if SERVICE_OUTPUT:
        return Path(SERVICE_OUTPUT).resolve()
    fallback = '/data/p32_execution_replay' if Path('/data').is_dir() else str(Path(tempfile.gettempdir()) / 'p32_execution_replay')
    return Path(os.getenv('P32_REPLAY_OUTPUT_DIR', fallback)).resolve()


def csv_bytes(rows):
    fields = list(dict.fromkeys(k for row in rows for k in row)) or ['empty']
    f = io.StringIO(newline='')
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()
    for row in rows:
        writer.writerow({k: json.dumps(v, sort_keys=True) if isinstance(v, (dict, list)) else v for k, v in row.items()})
    return f.getvalue().encode()


def make_reports(directory, data, rows, summary, store, reader):
    paired = defaultdict(dict)
    for row in rows:
        paired[row['index']][row['variant']] = row
    comparisons = []
    for index, variants in sorted(paired.items()):
        original = data['reference']['book'][index]
        row = dict(index=index, strategy_id=original['sid'], archived_r=original['r'], archived_exit=original['exit'])
        for variant, prefix in zip(VARIANTS, ('rounded_mid', 'executable')):
            item = variants.get(variant, {})
            row.update({prefix + '_' + k: item.get(k) for k in ('status', 'kind', 'certainty', 'interval_end_utc', 'observed_path_price_r', 'extended_beyond_archived_exit')})
        comparisons.append(row)
    pending = list({sha(canon(x['pending_request'])): x['pending_request'] for x in rows if x.get('pending_request')}.values())
    files = {
        'RUN_COMPLETE.json': canon(summary), 'protocol.json': canon(PROTOCOL),
        'execution_replay.csv': csv_bytes(rows), 'comparison_to_archive.csv': csv_bytes(comparisons),
        'resolved_subset_summary.json': canon(summarize(rows)),
        'needs_candle_data.json': canon(pending),
        'unresolved_and_open.csv': csv_bytes([x for x in rows if x['status'] not in ('CLOSED_RESOLVED', 'ENTRY_PROXY_REJECTED')]),
        'raw_strategy_coverage.csv': csv_bytes(data['raw_stream_coverage']),
        'frozen_original_book.csv': csv_bytes(data['reference']['book']),
        'frozen_original_geometry.csv': csv_bytes(data['reference']['geometry']),
        'baseline_controls_verified_during_build.json': canon(data['baseline_controls']),
        'source_provenance.json': canon(dict(reference_sha256=REFERENCE_SHA256, executor_sha256=data['executor_sha256'],
                                             source_archive_sha256=data['source_archive_sha256'], seed_manifest=SEED_MANIFEST)),
        'funding_snapshot_not_historical.json': canon(dict(observed_at=data['funding_snapshot_utc'], schedules=data['funding'])),
        'request_receipts.json': canon(reader.receipts if reader else []),
    }
    text = f'''# P32 execution correction: {summary['status']}

Scope: the original 3,391 accepted trades, independently repriced under two
variants. This is an execution-model correction, not an all-signal portfolio
backtest. The original historical book is retained as a control.

Read comparison_to_archive.csv and execution_replay.csv. Missing, ambiguous,
unresolved and still-open cases remain explicit. Resolved-subset win rates and R
are conditional on coverage and entry-proxy admission; they are not portfolio
expectancy. Annual return, CAGR and portfolio drawdown are deliberately absent.

Entry quote timing, execution latency, price-bound cancellations, whole units,
changing home-currency conversion and NAV/margin sizing are not reconstructed.
Historical stops for 2,898 trades retain the documented signal/stop models.
Bid/ask costs already occur in price R: do not subtract spread a second time.

Funding is a sensitivity using the 14 October snapshot and entry notional,
including its weekday charging convention. Historical rates, holidays and
conversion factors are unavailable. It is not actual historical net R.

The archive cutoff is fixed at {data['historical_end']}; trades still open there
are not force-closed. Data beyond the old individual exit may be fetched, but no
future candle beyond this cutoff is used. This history is not independent OOS.

All 32 strategy identities are checked; 27 original raw streams are still
missing. Changed exits require a later chronological admission replay before a
new portfolio return can be assessed. Keep the seed ZIP files: new downloaded
candles are included here, and reused original candles are referenced by hash.

No orders or executor/probe calls are supported by this service.
'''
    files['READ_FIRST.md'] = text.encode()
    files['report_manifest.json'] = canon({name: dict(bytes=len(body), sha256=sha(body)) for name, body in files.items()})
    temp = directory / (RESULT_NAME + '.tmp')
    with zipfile.ZipFile(temp, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        for name, body in sorted(files.items()):
            z.writestr(name, body)
        store.export(z)
    temp.replace(directory / RESULT_NAME)
    (directory / 'completion.json').write_bytes(canon(summary))


def run(offline=False, directory=None, limit=None):
    directory = Path(directory or output_dir())
    directory.mkdir(parents=True, exist_ok=True)
    progress(status='RUNNING', phase='CHECK_REFERENCE', started_at_utc=stamp(datetime.now(UTC)))
    data = load_reference()
    env = os.getenv('OANDA_ENV', os.getenv('OANDA_ENVIRONMENT', 'live')).lower().strip()
    require(env == 'live', 'Bundled evidence uses the live broker: keep OANDA_ENV=live')
    broker = 'https://api-fxtrade.oanda.com'
    reader = None
    if not offline:
        token = next((os.getenv(k, '').strip() for k in ('OANDA_TOKEN', 'OANDA_API_KEY', 'OANDA_API_TOKEN', 'OANDA_ACCESS_TOKEN') if os.getenv(k, '').strip()), '')
        require(bool(token), 'Set OANDA_TOKEN on this separate research service')
        reader = CandleReader(token, broker, float(os.getenv('P32_REPLAY_REQUESTS_PER_SECOND', '4')))
    store = CandleStore(directory, broker, reader)
    try:
        progress(phase='VERIFY_AND_IMPORT_SAVED_CANDLES', seed_parts_required=len(SEED_MANIFEST))
        store.import_seeds(Path(__file__).parent)
        total = min(limit, 3391) if limit else 3391
        implementation_sha256 = sha(Path(__file__).read_bytes())
        identity = sha(canon(dict(version=VERSION, implementation=implementation_sha256, reference=REFERENCE_SHA256,
                                 offline=offline, limit=limit, protocol=PROTOCOL)))
        checkpoint = directory / ('checkpoint_' + identity + '.jsonl')
        completed = {}
        if checkpoint.exists():
            for line in checkpoint.read_text().splitlines():
                try:
                    row = json.loads(line)
                    if row['status'] not in ('NEEDS_CANDLE_DATA', 'ERROR'):
                        completed[(row['index'], row['variant'])] = row
                except (ValueError, KeyError):
                    pass  # A process interruption may leave one partial final line.
        tasks = [(i, v) for i in range(total) for v in VARIANTS]
        rows = [completed[key] for key in tasks if key in completed]
        progress(phase='REPLAY_CORRECTED_EXECUTION', required=len(tasks), completed=len(rows), offline=offline,
                 baseline_control_count_verified_at_build=6, source_candle_blocks=12391)
        workers = int(os.getenv('P32_REPLAY_WORKERS', '3'))
        require(1 <= workers <= 4, 'Workers must be 1 to 4')
        with checkpoint.open('a') as journal, ThreadPoolExecutor(max_workers=workers) as pool:
            pending = {pool.submit(replay_one, store, data, i, variant): (i, variant) for i, variant in tasks if (i, variant) not in completed}
            for future in as_completed(pending):
                row = future.result()
                rows.append(row)
                journal.write(canon(row).decode() + '\n')
                journal.flush()
                progress(completed=len(rows), new_candle_blocks=store.downloads,
                         row_status_counts=dict(Counter(x['status'] for x in rows)))
        rows.sort(key=lambda x: (x['index'], x['variant']))
        counts = Counter(x['status'] for x in rows)
        status = 'ERROR' if counts['ERROR'] else 'INCOMPLETE_OFFLINE' if counts['NEEDS_CANDLE_DATA'] else 'COMPLETE_WITH_LIMITATIONS'
        summary = dict(status=status, version=VERSION, scope='FROZEN_COHORT_EXECUTION_REBUILD', orders_sent=False,
                       live_services_contacted=False, full_portfolio_validated=False, historical_cutoff=data['historical_end'],
                       frozen_trades=3391, trades_attempted=total, variant_rows=len(rows), row_status_counts=dict(counts),
                       offline=offline, test_subset=limit is not None, new_candle_blocks=store.downloads,
                       strategy_identities=32, raw_streams_missing=27, results=summarize(rows),
                       finished_at_utc=stamp(datetime.now(UTC)), reference_sha256=REFERENCE_SHA256,
                       implementation_sha256=implementation_sha256)
        progress(phase='SAVE_RESULTS')
        make_reports(directory, data, rows, summary, store, reader)
        progress(**summary, phase='FINISHED', result='/results.zip')
        return summary
    finally:
        store.close()


def start_once():
    with LOCK:
        if STATE['status'] != 'IDLE':
            return
        STATE.update(status='RUNNING', phase='STARTING')
    def worker():
        try:
            run(offline=SERVICE_OFFLINE or os.getenv('P32_REPLAY_OFFLINE', '').lower() in ('1', 'true'))
        except Exception as exc:
            # Configuration errors are controlled; transport errors are sanitized.
            message = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
            progress(status='ERROR', phase='FAILED', error=message)
    threading.Thread(target=worker, daemon=True, name='p32-candle-replay').start()


def application(environ, start_response):
    method, path = environ.get('REQUEST_METHOD', 'GET'), environ.get('PATH_INFO', '/')
    kind = 'application/json'
    if method not in ('GET', 'HEAD'):
        code, body = '405 Method Not Allowed', canon({'error': 'GET/HEAD only; order routes do not exist'})
    elif path in ('/', '/health'):
        code, body = '200 OK', canon(dict(service=VERSION, scope='FROZEN_COHORT_EXECUTION_REBUILD',
                                          orders_supported=False, start_once='/status', result='/results.zip'))
    elif path == '/status':
        if method == 'GET':
            start_once()
        with LOCK:
            body = canon(dict(STATE))
        code = '200 OK'
    elif path in ('/results.zip', '/results', '/download'):
        with LOCK:
            finished = STATE['status'] in ('COMPLETE_WITH_LIMITATIONS', 'INCOMPLETE_OFFLINE', 'ERROR')
        file = output_dir() / RESULT_NAME
        if finished and file.is_file() and STATE.get('phase') == 'FINISHED':
            code, body, kind = '200 OK', file.read_bytes(), 'application/zip'
        else:
            code, body = '409 Conflict', canon({'message': 'Results not ready; check /status'})
    else:
        code, body = '404 Not Found', canon({'error': 'Unknown route'})
    headers = [('Content-Type', kind), ('Content-Length', str(len(body))), ('Cache-Control', 'no-store')]
    if kind == 'application/zip':
        headers.append(('Content-Disposition', 'attachment; filename=' + RESULT_NAME))
    if code.startswith('405'):
        headers.append(('Allow', 'GET, HEAD'))
    start_response(code, headers)
    return [b'' if method == 'HEAD' else body]


class QuietHandler(WSGIRequestHandler):
    def log_message(self, format, *args):
        pass


def main():
    global SERVICE_OFFLINE, SERVICE_OUTPUT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', action='store_true', help='Run once and exit instead of serving HTTP')
    parser.add_argument('--offline', action='store_true', help='Use saved candles only; report requests still needed')
    parser.add_argument('--output', help='Separate replay cache/result directory')
    parser.add_argument('--limit', type=int, help='Developer test subset; never a full result')
    args = parser.parse_args()
    SERVICE_OFFLINE, SERVICE_OUTPUT = args.offline, args.output
    if args.limit is not None:
        require(args.run and 1 <= args.limit <= 3391, '--limit requires --run and a value from 1 to 3391')
    if args.run:
        print(json.dumps(run(args.offline, args.output, args.limit), indent=2))
        return
    port = int(os.getenv('PORT', '8080'))
    with make_server('0.0.0.0', port, application, handler_class=QuietHandler) as server:
        print(VERSION + ' listening on port ' + str(port), flush=True)
        server.serve_forever()


if __name__ == '__main__':
    main()
