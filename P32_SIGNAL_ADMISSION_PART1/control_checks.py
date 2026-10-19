"""Pinned accounting-control fingerprints, including ordered records."""
import json
import math
import execution as e
from control_pins import CONTROL_SHA256

TABLES = ('ledger', 'trades', 'financing', 'cash_events', 'monthly_observations', 'open_positions')


def normalized(value):
    if isinstance(value, dict):
        return {k: normalized(v) for k, v in value.items() if v is not None and v != ''}
    if isinstance(value, (list, tuple)):
        return [normalized(v) for v in value]
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        e.require(math.isfinite(value), 'Nonfinite control value')
        return float(format(value, '.13g'))
    return value


def digest(value):
    return e.sha(e.canon(normalized(value)))


def load(root):
    raw = (root / 'control_expectations.json').read_bytes()
    e.require(e.sha(raw) == CONTROL_SHA256, 'Pinned control expectations changed')
    return json.loads(raw)


def check(result, expectations):
    summary = result['summary']
    name = summary['scenario'] + '__' + summary['financing_mode']
    expected = expectations['lanes'][name]
    rows = []
    for kind in TABLES:
        actual = digest(result[kind])
        target = expected[kind]
        rows.append(dict(lane=name, component=kind, rows=len(result[kind]),
                         expected_rows=target['rows'], actual_sha256=actual,
                         expected_sha256=target['sha256'],
                         passed=actual == target['sha256'] and len(result[kind]) == target['rows']))
    rows.append(dict(lane=name, component='summary', passed=digest(summary) == expected['summary_sha256']))
    failures = [r['component'] for r in rows if not r['passed']]
    return dict(lane=name, status='FAIL' if failures else 'PASS', failed_components=failures, checks=rows)
