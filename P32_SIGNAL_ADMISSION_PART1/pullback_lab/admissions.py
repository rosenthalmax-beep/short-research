"""Explicit conditional event ordering for isolated research accounts."""
import itertools
from . import core as e

SCENARIOS = ('END_ASC', 'END_DESC', 'START_ASC', 'START_DESC')


def ordered(signals, scenario):
    for _, group in itertools.groupby(sorted(signals, key=lambda s: s['entry_utc']),
                                      key=lambda s: s['entry_utc']):
        by_sid = {}
        for signal in group:
            by_sid.setdefault(signal['strategy_id'], []).append(signal)
        for sid in sorted(by_sid, reverse=scenario.endswith('DESC')):
            yield from sorted(by_sid[sid], key=lambda s: (s['same_strategy_trigger_priority'], s['signal_key']))


def release_time(path, scenario):
    field = 'interval_end_utc' if scenario.startswith('END') else 'interval_start_utc'
    e.require(path.get(field) is not None, 'Observed exit interval is required')
    return e.dt(path[field])
