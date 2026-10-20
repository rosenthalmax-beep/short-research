"""Immutable canonical candles, optional exact read-only P32 cache reuse."""
import json
import math
import sqlite3
import zlib
from array import array
from collections import OrderedDict
from datetime import timedelta
from pathlib import Path
from . import core as e

BROKER = 'https://api-fxtrade.oanda.com'
WINDOW_SECONDS = {'M15': 14 * 86400, 'H1': 14 * 86400, 'M1': 86400, 'S5': 3600}


def validate(obj, ident):
    e.require(obj.get('instrument') == ident['pair'] and obj.get('granularity') == ident['granularity'],
              'Candle instrument/granularity mismatch')
    e.require(isinstance(obj.get('candles'), list), 'Missing candle array')
    previous, rows = None, []
    start, end = e.dt(ident['start']), e.dt(ident['end'])
    step = e.STEP[ident['granularity']]
    for candle in obj['candles']:
        t = e.dt(candle['time'])
        e.require(previous is None or t > previous, 'Unordered or duplicate candle')
        previous = t
        e.require(start <= t < end and e.floor_time(t, step) == t, 'Candle outside requested range')
        if candle.get('complete') is not True or t + timedelta(seconds=step) > end:
            continue
        for component in ('mid', 'bid', 'ask'):
            e.require(isinstance(candle.get(component), dict), 'MBA prices required')
            p = {key: e.dec(candle[component][key]) for key in 'ohlc'}
            e.require(0 < p['l'] <= min(p['o'], p['c']) <= max(p['o'], p['c']) <= p['h'], 'Invalid OHLC')
        e.require(e.dec(candle['ask']['o']) >= e.dec(candle['bid']['o']) and
                  e.dec(candle['ask']['c']) >= e.dec(candle['bid']['c']), 'Negative boundary spread')
        rows.append(candle)
    return rows


class Store:
    def __init__(self, directory, reader=None, legacy_root=None, progress=lambda **x: None):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(self.directory / 'candles.sqlite3'))
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('CREATE TABLE IF NOT EXISTS blocks(key TEXT PRIMARY KEY, identity TEXT, body BLOB, sha256 TEXT, origin TEXT)')
        self.db.execute('CREATE TABLE IF NOT EXISTS receipts(id INTEGER PRIMARY KEY, body TEXT)')
        self.db.commit()
        self.reader, self.progress = reader, progress
        self.memo = OrderedDict()
        self.downloads = self.reused = self.hits = 0
        self.used = set()
        self.legacy = []
        if legacy_root:
            root = Path(legacy_root)
            for relative in ('gbp_valuation_windows_v2/replay_candles.sqlite3',
                             'entry_limit_v1/new_execution_windows/replay_candles.sqlite3',
                             'gbp_execution_paths_v2/replay_candles.sqlite3', 'replay_candles.sqlite3'):
                path = (root / relative).resolve()
                if path.is_file():
                    con = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)
                    con.execute('PRAGMA query_only=ON')
                    if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='blocks'").fetchone():
                        self.legacy.append((path, con))
                    else:
                        con.close()

    @staticmethod
    def identity(pair, gran, start, end):
        e.require(pair in e.PAIRS and gran in e.STEP, 'Unknown candle stream')
        step = e.STEP[gran]
        e.require(start < end and e.floor_time(start, step) == start and e.floor_time(end, step) == end,
                  'Unaligned candle window')
        return dict(schema='MBA_UNSMOOTHED_V1', broker=BROKER, pair=pair, granularity=gran,
                    start=e.stamp(start), end=e.stamp(end), price='MBA', smooth=False, includeFirst=True)

    def exact(self, pair, gran, start, end):
        ident = self.identity(pair, gran, start, end)
        key = e.sha(e.canon(ident))
        self.used.add(key)
        if key in self.memo:
            self.hits += 1
            self.memo.move_to_end(key)
            return self.memo[key]
        row = self.db.execute('SELECT identity,body,sha256,origin FROM blocks WHERE key=?', (key,)).fetchone()
        if row is None:
            for path, con in self.legacy:
                old = con.execute('SELECT identity,body,sha256 FROM blocks WHERE key=?', (key,)).fetchone()
                if old is not None:
                    encoded, compressed, checksum = old
                    raw = zlib.decompress(compressed)
                    e.require(json.loads(encoded) == ident and e.sha(raw) == checksum, 'Legacy exact-window checksum failed')
                    validate(json.loads(raw), ident)
                    row = (encoded, compressed, checksum, 'EXACT_READ_ONLY_REUSE:' + str(path))
                    self.db.execute('INSERT INTO blocks VALUES(?,?,?,?,?)', (key, *row))
                    self.db.commit()
                    self.reused += 1
                    break
        if row is None:
            if self.reader is None:
                raise e.NeedData(ident)
            self.progress(data_operation='CANDLE_GET', data_pair=pair, data_granularity=gran,
                          data_from=ident['start'], data_to=ident['end'], downloaded_windows=self.downloads,
                          exact_legacy_windows_reused=self.reused)
            obj = self.reader.get(ident)
            candles = validate(obj, ident)
            raw = e.canon(dict(instrument=pair, granularity=gran, candles=candles))
            row = (e.canon(ident).decode(), zlib.compress(raw, 6), e.sha(raw), 'CANONICAL_DOWNLOAD')
            self.db.execute('INSERT INTO blocks VALUES(?,?,?,?,?)', (key, *row))
            if hasattr(self.reader, 'receipts'):
                for receipt in self.reader.receipts:
                    self.db.execute('INSERT INTO receipts(body) VALUES(?)', (e.canon(receipt).decode(),))
                self.reader.receipts.clear()
            self.db.commit()
            self.downloads += 1
        encoded, compressed, checksum, _ = row
        raw = zlib.decompress(compressed)
        e.require(json.loads(encoded) == ident and e.sha(raw) == checksum, 'Canonical cache checksum failed')
        candles = validate(json.loads(raw), ident)
        self.memo[key] = candles
        while len(self.memo) > 8:
            self.memo.popitem(last=False)
        self.progress(data_operation='CANDLE_READY', data_pair=pair, data_granularity=gran,
                      data_through=ident['end'], downloaded_windows=self.downloads,
                      exact_legacy_windows_reused=self.reused)
        return candles

    def range(self, pair, gran, start, end):
        self.identity(pair, gran, start, end)
        cursor, result = start, []
        while cursor < end:
            begin = e.floor_time(cursor, WINDOW_SECONDS[gran])
            finish = begin + timedelta(seconds=WINDOW_SECONDS[gran])
            for candle in self.exact(pair, gran, begin, finish):
                t = e.dt(candle['time'])
                if start <= t and t + timedelta(seconds=e.STEP[gran]) <= end:
                    result.append(candle)
            cursor = finish
        return result

    def window(self, pair, start, end):
        return self.exact(pair, 'M15', start, end)

    def difference_count(self):
        return 0  # This study uses one canonical source, with no merged overlap.

    def index(self):
        return [dict(json.loads(identity), key=key, sha256=checksum, origin=origin,
                     used_this_process=key in self.used)
                for key, identity, checksum, origin in self.db.execute('SELECT key,identity,sha256,origin FROM blocks ORDER BY key')]

    def close(self):
        for _, con in self.legacy:
            con.close()
        self.db.close()


class Bars:
    """Compact MBA columns: roughly 60 MB for a pair's full M15 history."""
    def __init__(self):
        self.times = array('q')
        self.values = {s: {k: array('d') for k in 'ohlc'} for s in ('mid', 'bid', 'ask')}

    def __len__(self):
        return len(self.times)

    def append(self, candle):
        t = int(e.dt(candle['time']).timestamp())
        e.require(not self.times or t > self.times[-1], 'Duplicate/unsorted compact candles')
        self.times.append(t)
        for s in self.values:
            for k in 'ohlc':
                self.values[s][k].append(float(candle[s][k]))

    def row(self, i):
        return dict(time=e.stamp(e.datetime.fromtimestamp(self.times[i], e.UTC)), complete=True,
                    **{s: {k: self.values[s][k][i] for k in 'ohlc'} for s in self.values})


def load_bars(store, pair, gran, warmup, start, end, progress=lambda **x: None):
    bars, warmup_errors = Bars(), []
    cursor = e.floor_time(warmup, WINDOW_SECONDS[gran])
    while cursor < end:
        finish = cursor + timedelta(seconds=WINDOW_SECONDS[gran])
        try:
            rows = store.exact(pair, gran, cursor, finish)
        except e.CandleFetchError as exc:
            if finish > start or 'HTTP 400' not in str(exc):
                raise
            warmup_errors.append(dict(start=e.stamp(cursor), end=e.stamp(finish), error=str(exc)))
            rows = []
        for c in rows:
            t = e.dt(c['time'])
            if warmup <= t and t + timedelta(seconds=e.STEP[gran]) <= end:
                bars.append(c)
        cursor = finish
        progress(phase='LOAD_HISTORY', pair=pair, granularity=gran, bars_loaded=len(bars),
                 through_utc=e.stamp(min(cursor, end)))
    warmed = sum(t + e.STEP[gran] <= start.timestamp() for t in bars.times)
    e.require(warmed >= 1000 if gran == 'H1' else warmed >= 250,
              pair + ' ' + gran + ': insufficient pre-start indicator warm-up')
    return bars, dict(pair=pair, granularity=gran, observed_bars=len(bars),
                      warmup_bars=warmed, unavailable_warmup_windows=warmup_errors,
                      first_utc=e.stamp(e.datetime.fromtimestamp(bars.times[0], e.UTC)),
                      last_utc=e.stamp(e.datetime.fromtimestamp(bars.times[-1], e.UTC)))
