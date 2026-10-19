"""Policy-specific entry guards and sizing bounds over shared price evidence."""
import json
import sqlite3
from pathlib import Path
import admissions
import execution as e
import inputs
from policies import allowance


class PolicyPaths:
    def __init__(self, directory, saved, store, progress=lambda **x: None):
        self.rows = dict(saved)
        self.original = dict(saved)
        self.store, self.progress = store, progress
        self.data = e.load_reference()
        self.policy = 'FIXED_10'
        self.evaluated = {}
        self.computed = 0
        self.db = sqlite3.connect(str(Path(directory) / 'policy_paths.sqlite3'))
        self.db.execute('CREATE TABLE IF NOT EXISTS paths(key TEXT PRIMARY KEY, body BLOB NOT NULL, sha256 TEXT NOT NULL)')
        self.db.commit()
        root = Path(__file__).parent
        self.signature = e.sha(e.canon(dict(inputs=inputs.PINS,
            code={n:e.sha((root/n).read_bytes()) for n in ('execution.py','execution_sources.py','policy_paths.py','policies.py')})))

    def _cached(self, kind, signal, compute):
        identity = e.sha(e.canon(dict(signature=self.signature, kind=kind, signal=signal)))
        found = self.db.execute('SELECT body,sha256 FROM paths WHERE key=?', (identity,)).fetchone()
        if found:
            e.require(e.sha(found[0]) == found[1], 'Path checkpoint checksum failed')
            return json.loads(found[0])
        value = compute()
        if value['status'] == 'NEEDS_CANDLE_DATA':
            raise e.NeedData(value['pending_request'])
        e.require(value['status'] != 'ERROR', 'New path failed: ' + str(value.get('error','unknown')))
        raw = e.canon(value)
        self.db.execute('INSERT INTO paths VALUES (?,?,?)', (identity,raw,e.sha(raw)))
        self.db.commit(); self.computed += 1
        return value

    def base_path(self, signal):
        key = signal['signal_key']
        if key not in self.rows:
            def compute():
                self.store.begin_path()
                row = e.replay_one(self.store, admissions.candidate_data(self.data,signal), 0, e.VARIANTS[1])
                row = {k:v for k,v in row.items() if not k.startswith('archived_') and k not in ('index','extended_beyond_archived_exit')}
                row['signal_key'] = key
                return self.qualify(row)
            self.rows[key] = self._cached('DEFAULT10_NEWLY_UNBLOCKED', signal, compute)
        return self.rows[key]

    def qualify(self, row):
        differences = set(row.get('new_source_difference_ids', ())) | self.store.current_differences
        if differences:
            row['new_source_difference_count'] = len(differences)
            row['new_source_difference_ids'] = sorted(differences)
            if row['status'] in ('CLOSED_RESOLVED','OPEN_AT_CUTOFF'):
                row['status'] = 'UNRESOLVED_PATH'
            row['certainty_before_source_difference'] = row.get('certainty')
            row['certainty'] = 'FRESH_EXECUTION_SOURCE_DIFFERS_FROM_FROZEN_EVIDENCE'
        return row

    def get(self, signal):
        base = self.base_path(signal)
        key = signal['signal_key']
        if self.policy == 'FIXED_10':
            self.evaluated[(self.policy,key)] = base
            return base
        cap = allowance(self.policy, signal)
        # An absent exact entry quote is never guessed from another candle.
        if base['status'] == 'UNRESOLVED_ENTRY' or base.get('model_bid') is None or base.get('model_ask') is None:
            self.evaluated[(self.policy,key)] = base
            return base
        geometry = e.entry_preview(signal['pair'],signal['side'],signal['reference_entry'],signal['stop'],signal['rr'],
                                   base['model_bid'],base['model_ask'],cap)
        row = dict(base)
        row.update({'model_'+k:v for k,v in e.serial_geometry(geometry).items()})
        row['entry_status'] = geometry['status']
        if geometry['status'].startswith('REJECTED_'):
            row['status'] = 'ENTRY_PROXY_REJECTED'
        elif base['status'] == 'ENTRY_PROXY_REJECTED':
            # Stop/target and same saved entry quote make exits cap-independent.
            def compute():
                self.store.begin_path()
                data = admissions.candidate_data(self.data,signal)
                component = 'bid' if signal['side']=='BUY' else 'ask'
                outcome = e.first_exit(self.store,data['reference']['book'][0],geometry,component,e.dt(data['historical_end']))
                result = dict(base, **outcome)
                result.update({'model_'+k:v for k,v in e.serial_geometry(geometry).items()})
                result.update(entry_status='ADMITTED_ENTRY_PROXY',exit_component=component)
                result['status'] = ('CLOSED_RESOLVED' if outcome['kind'] in ('STOP','TARGET') and outcome['certainty']=='RESOLVED_TO_CANDLE_INTERVAL'
                    else 'OPEN_AT_CUTOFF' if outcome['kind']=='OPEN_AT_FIXED_CUTOFF' and outcome['certainty']=='NO_TOUCH_OBSERVED_TO_CUTOFF'
                    else 'UNRESOLVED_PATH')
                return self.qualify(result)
            # The signature records the source and geometry implementation.
            shared = self._cached('NEWLY_ELIGIBLE_EXIT',signal,compute)
            row = dict(shared)
            row.update({'model_'+k:v for k,v in e.serial_geometry(geometry).items()})
            row['entry_status'] = geometry['status']
        if row.get('exit_price') is not None and row['status'] != 'ENTRY_PROXY_REJECTED':
            sign = 1 if signal['side']=='BUY' else -1
            move = sign*(e.dec(row['exit_price'])-geometry['fill'])
            row['observed_path_price_r'] = float(move/geometry['stop_distance'])
            row['observed_path_worst_bound_budget_r'] = float(move/geometry['worst_stop_distance'])
        row['tested_allowance_ticks'] = cap
        self.evaluated[(self.policy,key)] = row
        self.progress(phase='REPLAY_ACCOUNTS',candidate_paths_available=len(self.rows),new_paths_completed=self.computed,
                      evaluated_policy_paths=len(self.evaluated))
        return row

    def close(self):
        self.db.close()
