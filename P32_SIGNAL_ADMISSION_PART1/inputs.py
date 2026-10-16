"""Pinned, manifest-checked signals and separate saved executable candles."""
import csv
import io
import json
import re
import zipfile
import zlib
from pathlib import Path
import execution as e
from pinned_inputs import PINS


def checked(path):
    path = Path(path)
    e.require(path.name in PINS and e.sha(path.read_bytes()) == PINS[path.name], 'Pinned input checksum failed: ' + path.name)
    return path


def parse_cell(value):
    if value == '':
        return None
    if value in ('True', 'False'):
        return value == 'True'
    if value[0:1] in ('[', '{'):
        return json.loads(value)
    # Preserve timestamps, identities, names. Numeric path quantities become
    # floats only after the original report bytes have passed their manifest.
    try:
        return float(value)
    except ValueError:
        return value


def baseline(directory):
    path = checked(Path(directory) / 'warmed_signal_results.zip')
    with zipfile.ZipFile(path) as z:
        e.require(len(z.namelist()) == len(set(z.namelist())), 'Duplicate baseline member')
        manifest = json.loads(z.read('report_manifest.json'))
        for name, meta in manifest.items():
            raw = z.read(name)
            e.require(len(raw) == meta['bytes'] and e.sha(raw) == meta['sha256'], 'Baseline report manifest failed')
        summary = json.loads(z.read('RUN_COMPLETE.json'))
        signals = [json.loads(x) for x in z.read('raw_signal_witnesses.jsonl').splitlines() if x]
        paths = {r['signal_key']: {k: parse_cell(v) for k, v in r.items()} for r in csv.DictReader(io.StringIO(z.read('candidate_price_paths.csv').decode()))}
        e.require(summary['raw_signals'] == len(signals) == 3629, 'Unexpected warmed signal cohort')
        e.require(len(paths) == 3506 and summary['strategy_streams_scanned'] == 32, 'Unexpected saved path cohort')
        e.require(len({s['signal_key'] for s in signals}) == len(signals), 'Duplicate raw signal')
        e.require(set(paths) <= {s['signal_key'] for s in signals}, 'Orphan candidate path')
        return signals, paths, summary


def import_additions(directory, store):
    path = checked(Path(directory) / 'admission_source_additions.zip')
    checksum = PINS[path.name]
    if store.db.execute('SELECT 1 FROM imports WHERE sha256=?', (checksum,)).fetchone():
        return
    with zipfile.ZipFile(path) as z:
        index = json.loads(z.read('new_source_candle_index.json'))
        e.require(len(index) == 89, 'Expected 89 saved admission source blocks')
        for row in index:
            e.require(re.fullmatch(r'source_candles/[0-9a-f]{64}\.json', row['file']), 'Invalid source member')
            ident = {k: v for k, v in row.items() if k not in ('file', 'sha256', 'origin')}
            key = e.sha(e.canon(ident)); raw = z.read(row['file'])
            e.require(ident['broker'] == store.broker and row['file'] == 'source_candles/' + key + '.json' and e.sha(raw) == row['sha256'], 'Source identity/hash failed')
            store.validate(json.loads(raw), ident)
            old = store.db.execute('SELECT sha256 FROM blocks WHERE key=?', (key,)).fetchone()
            e.require(old is None or old[0] == row['sha256'], 'Saved source conflict')
            if old is None:
                store.db.execute('INSERT INTO blocks VALUES (?,?,?,?,?)', (key, e.canon(ident).decode(), zlib.compress(raw, 6), row['sha256'], path.name))
                store.index[(ident['pair'], ident['granularity'])].append((e.dt(ident['start']), e.dt(ident['end']), key))
    store.db.execute('INSERT INTO imports VALUES (?)', (checksum,)); store.db.commit()


def broker(directory):
    return json.loads(checked(Path(directory) / 'saved_broker_evidence.json').read_bytes())
