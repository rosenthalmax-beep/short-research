"""One immutable canonical MBA response per valuation window.

The execution seed cache can contain overlapping requests from different
observations. It remains a separate reference dataset. We never merge its
OHLC bars into fresh valuation windows or overwrite any frozen source.
"""
import json
import zlib
import execution as e


class ValuationWindows:
    def __init__(self, store, frozen, legacy=None, progress=lambda **x: None):
        self.store, self.frozen, self.legacy, self.progress = store, frozen, legacy, progress
        self.memo = store.memo
        self.reused_legacy_windows = 0
        store.db.execute('CREATE TABLE IF NOT EXISTS valuation_comparisons (key TEXT PRIMARY KEY)')
        store.db.execute('CREATE TABLE IF NOT EXISTS valuation_source_differences (key TEXT PRIMARY KEY, body BLOB NOT NULL)')
        store.db.execute('CREATE TABLE IF NOT EXISTS valuation_window_origins (key TEXT PRIMARY KEY, body BLOB NOT NULL)')
        store.db.commit()

    @property
    def downloads(self):
        return self.store.downloads

    def _reuse_exact_legacy(self, key, identity):
        if self.legacy is None:
            return False
        item = self.legacy.db.execute('SELECT identity,body,sha256 FROM blocks WHERE key=?', (key,)).fetchone()
        if item is None:
            return False
        encoded, compressed, checksum = item
        e.require(json.loads(encoded) == identity, 'Legacy canonical window identity mismatch')
        raw = zlib.decompress(compressed)
        e.require(e.sha(raw) == checksum, 'Legacy canonical window checksum failed')
        self.store.validate(json.loads(raw), identity)
        self.store.db.execute('INSERT INTO blocks VALUES (?,?,?,?,?)',
                              (key, encoded, compressed, checksum, 'NEW_DOWNLOAD'))
        self.store.db.execute('INSERT OR REPLACE INTO valuation_window_origins VALUES (?,?)',
                              (key, e.canon(dict(origin='REUSED_EXACT_LEGACY_RESPONSE', source_sha256=checksum))))
        self.store.db.commit()
        self.store.index[(identity['pair'], identity['granularity'])].append(
            (e.dt(identity['start']), e.dt(identity['end']), key))
        self.reused_legacy_windows += 1
        return True

    def window(self, pair, start, end):
        identity = self.store.identity(pair, 'M15', start, end)
        key = e.sha(e.canon(identity))
        if self.store.db.execute('SELECT 1 FROM blocks WHERE key=?', (key,)).fetchone() is None:
            if not self._reuse_exact_legacy(key, identity):
                self.store._fetch(pair, 'M15', start, end)
                self.store.db.execute('INSERT OR REPLACE INTO valuation_window_origins VALUES (?,?)',
                                      (key, e.canon(dict(origin='FRESH_CANONICAL_RESPONSE'))))
                self.store.db.commit()
        # Read EXACTLY this response, even if some other requested interval in
        # the database overlaps. This is not first/last-bar conflict selection.
        encoded = self.store.db.execute('SELECT identity FROM blocks WHERE key=?', (key,)).fetchone()[0]
        e.require(json.loads(encoded) == identity, 'Canonical valuation window identity mismatch')
        rows = self.store._read(key)
        marker = e.sha(e.canon(dict(version='NUMERIC_OHLC_COMPARISON_V1', window_key=key,
                                    frozen_scope='PINNED_FIVE_SEEDS_AND_ADMISSION_89')))
        if self.store.db.execute('SELECT 1 FROM valuation_comparisons WHERE key=?', (marker,)).fetchone() is None:
            self._compare(pair, start, end, key, rows)
            self.store.db.execute('INSERT INTO valuation_comparisons VALUES (?)', (marker,))
            self.store.db.commit()
        self.progress(quote_pair=pair, quote_from=e.stamp(start), quote_to=e.stamp(end),
                      valuation_windows_downloaded=self.downloads,
                      valuation_windows_reused_from_previous_cache=self.reused_legacy_windows,
                      valuation_source_disagreements=self.difference_count())
        return rows

    def _compare(self, pair, start, end, window_key, rows):
        by_time = {e.dt(c['time']): c for c in rows}
        for begin, finish, old_key in self.frozen.intervals(pair, 'M15'):
            if begin >= end or finish <= start or old_key == window_key:
                continue
            origin = self.frozen.db.execute('SELECT origin FROM blocks WHERE key=?', (old_key,)).fetchone()[0]
            if origin == 'NEW_DOWNLOAD':
                continue
            for old in self.frozen._read(old_key):
                when = e.dt(old['time'])
                if not start <= when < end:
                    continue
                new = by_time.get(when)
                changes = {}
                if new is None:
                    kind = 'FROZEN_CANDLE_ABSENT_FROM_VALUATION_RESPONSE'
                else:
                    for component in ('mid', 'bid', 'ask'):
                        for field in 'ohlc':
                            if e.dec(old[component][field]) != e.dec(new[component][field]):
                                changes[component + '.' + field] = dict(frozen=old[component][field], valuation=new[component][field])
                    if not changes:
                        continue
                    kind = 'NUMERIC_OHLC_DISAGREEMENT'
                row = dict(pair=pair, granularity='M15', time=e.stamp(when), kind=kind,
                           frozen_source_key=old_key, valuation_source_key=window_key,
                           frozen_origin=origin, changed_fields=changes,
                           policy='VALUATION_USES_EXACT_CANONICAL_RESPONSE; FROZEN_TRADE_PATH_UNCHANGED',
                           cause_of_difference='NOT_INFERRED')
                identity = e.sha(e.canon(dict(time=e.stamp(when), old=old_key, new=window_key)))
                self.store.db.execute('INSERT OR IGNORE INTO valuation_source_differences VALUES (?,?)',
                                      (identity, e.canon(row)))
            while len(self.frozen.memo) > 8:
                self.frozen.memo.popitem(last=False)

    def difference_count(self):
        return self.store.db.execute('SELECT COUNT(*) FROM valuation_source_differences').fetchone()[0]

    def differences(self):
        return [json.loads(raw) for (raw,) in self.store.db.execute('SELECT body FROM valuation_source_differences ORDER BY key')]

    def origins(self):
        return [dict(source_key=key, **json.loads(raw)) for key, raw in self.store.db.execute('SELECT key,body FROM valuation_window_origins ORDER BY key')]

    def summary(self):
        return dict(policy='ONE_CANONICAL_14_DAY_MBA_RESPONSE_PER_VALUATION_WINDOW',
                    execution_and_valuation_caches_separate=True,
                    price_disagreements_recorded=self.difference_count(),
                    windows_reused_from_legacy_this_process=self.reused_legacy_windows,
                    windows_downloaded_this_process=self.downloads,
                    historical_price_revision_cause_verified=False,
                    frozen_candidate_paths_automatically_changed=False)
