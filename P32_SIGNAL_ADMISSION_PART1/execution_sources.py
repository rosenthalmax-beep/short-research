"""Frozen coverage first, then isolated immutable canonical execution windows.

Never overwrite frozen seed bars. Fresh/frozen numeric differences qualify the
new path and are recorded; representations such as 1.0 vs 1.000 are equivalent.
"""
import json
from datetime import timedelta
import execution as e


def prices(c):
    return tuple(e.dec(c[s][f]) for s in ('mid', 'bid', 'ask') for f in 'ohlc')


class ExecutionWindows:
    def __init__(self, frozen, fresh, progress=lambda **x: None):
        self.frozen, self.fresh, self.progress = frozen, fresh, progress
        self.memo = fresh.memo
        self.seen_differences = set()
        self.current_differences = set()
        fresh.db.execute('CREATE TABLE IF NOT EXISTS execution_differences(key TEXT PRIMARY KEY, body BLOB NOT NULL)')
        fresh.db.commit()

    @property
    def downloads(self):
        return self.fresh.downloads

    def begin_path(self):
        self.current_differences = set()

    def segment_end(self, pair, gran, start, desired_end):
        return self.frozen.segment_end(pair, gran, start, desired_end)

    def _frozen(self, pair, gran, start, end):
        cursor, found = start, {}
        for begin, finish, key in self.frozen.intervals(pair, gran):
            if begin >= end or finish <= start:
                continue
            if begin > cursor:
                return None
            cursor = max(cursor, finish)
            for c in self.frozen._read(key):
                when = e.dt(c['time'])
                if start <= when < end:
                    if when in found and prices(found[when]) != prices(c):
                        return None
                    found[when] = c
        if cursor < end:
            return None
        return [found[t] for t in sorted(found)]

    def _canonical(self, pair, gran, start, end):
        ident = self.fresh.identity(pair, gran, start, end)
        key = e.sha(e.canon(ident))
        if not self.fresh.db.execute('SELECT 1 FROM blocks WHERE key=?', (key,)).fetchone():
            self.fresh._fetch(pair, gran, start, end)
        return key, self.fresh._read(key)

    def _compare(self, pair, gran, start, end, new_key, rows):
        by_time = {e.dt(c['time']): c for c in rows}
        for begin, finish, old_key in self.frozen.intervals(pair, gran):
            if begin >= end or finish <= start:
                continue
            for old in self.frozen._read(old_key):
                when = e.dt(old['time'])
                if not start <= when < end:
                    continue
                new = by_time.get(when)
                if new is not None and prices(old) == prices(new):
                    continue
                identity = e.sha(e.canon(dict(old=old_key, new=new_key, time=e.stamp(when))))
                body = dict(pair=pair, granularity=gran, time=e.stamp(when),
                            kind='NUMERIC_OHLC_DIFFERENCE' if new else 'FROZEN_BAR_ABSENT_FROM_NEW_RESPONSE',
                            frozen_source_key=old_key, canonical_source_key=new_key,
                            policy='NEW_PATH_USES_CANONICAL_WINDOW; OLD_SAVED_PATH_UNCHANGED',
                            cause_of_difference='NOT_INFERRED')
                self.fresh.db.execute('INSERT OR IGNORE INTO execution_differences VALUES (?,?)', (identity, e.canon(body)))
                self.current_differences.add(identity)
        self.fresh.db.commit()

    def range(self, pair, gran, start, end):
        self.fresh.identity(pair, gran, start, end)
        rows = self._frozen(pair, gran, start, end)
        if rows is not None:
            return rows
        seconds = {'H1': 14*86400, 'M15': 14*86400, 'M1': 86400, 'S5': 3600}[gran]
        cursor, result = start, []
        while cursor < end:
            window_start = e.floor_time(cursor, seconds)
            window_end = window_start + timedelta(seconds=seconds)
            key, candles = self._canonical(pair, gran, window_start, window_end)
            finish = min(end, window_end)
            self._compare(pair, gran, cursor, finish, key, candles)
            result.extend(c for c in candles if cursor <= e.dt(c['time']) < finish)
            cursor = finish
            self.progress(phase='PRICE_NEW_EXECUTION_PATHS', path_pair=pair, path_granularity=gran,
                          path_through_utc=e.stamp(cursor), new_execution_windows=self.downloads)
        return result

    def differences(self):
        return [json.loads(raw) for (raw,) in self.fresh.db.execute('SELECT body FROM execution_differences ORDER BY key')]
