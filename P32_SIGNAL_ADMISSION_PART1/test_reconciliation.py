"""Offline tests; synthetic records are never historical strategy evidence."""
import copy
import gzip
import io
import json
import tempfile
import unittest
import zipfile
from collections import Counter
from pathlib import Path
from unittest.mock import patch

import audusd_pullback_reconcile as a


def fixture(root):
    signature, native = 'a' * 64, 'f' * 64
    base = root / 'matrix'
    source = base / signature
    source.mkdir(parents=True)
    (base / 'runner.lock').write_bytes(b'')
    cid = 'M_D1.25_B1.0'
    directory = source / 'candidates' / cid
    (directory / 'accounts').mkdir(parents=True)
    signals, opps, paths = [], [], []
    for n, key in enumerate(('A', 'B', 'C', 'D')):
        when = f'2021-01-01T{9+n:02d}:00:00Z'
        end = {'A': '2021-01-01T10:15:00Z', 'B': '2021-01-01T10:30:00Z',
               'C': '2021-01-01T11:30:00Z', 'D': '2021-01-01T13:00:00Z'}[key]
        status = 'UNRESOLVED_PATH' if key == 'C' else 'ENTRY_PROXY_REJECTED' if key == 'D' else 'CLOSED_RESOLVED'
        s = dict(candidate_id=cid, signal_key=key, pair='AUD_USD', side='BUY', timeframe='M15', rr=3.,
                 entry_utc=when, reference_entry=1.2, stop=1.1995, strategy_id='S',
                 features=dict(body_atr=1.2, h1_distance_atr=1.))
        identity = dict(signature='b'*64, native_sha256=native, pair='AUD_USD', side='BUY', entry=when,
                        reference=1.2, stop=1.1995, rr=3., end='2021-01-02T00:00:00Z', limit=15)
        p = dict(path_cache_key=a.digest(a.canon(identity)), native_source_sha256=native,
                 source_horizon_utc=identity['end'], status=status, model_fill=1.2,
                 model_stop=1.1995, kind='STOP' if key in ('C', 'D') else 'TARGET',
                 certainty='TOUCH_WITH_EARLIER_UNEXPLAINED_GAP' if key == 'C' else 'RESOLVED_TO_CANDLE_INTERVAL',
                 interval_start_utc=end, interval_end_utc=end, exit_price=1.1995 if key in ('C', 'D') else 1.2005,
                 prior_native_unexplained_slots=1 if key == 'C' else 0)
        shadow = dict(shadow_status=status)
        if key in ('A', 'B'):
            r = 1. if key == 'A' else 3.
            shadow = dict(shadow_status='CLOSED_RESOLVED', frozen_net_r=r, adverse6_net_r=r,
                          actual_stop_risk_gbp=1., price_pl_gbp=r, units=1000, stale_conversion_quote_seen=False)
        op = dict(signal_key=key, entry_utc=when, path_cache_key=p['path_cache_key'],
                  path_status=status, shadow=shadow)
        signals.append(s); opps.append(op); paths.append(p)
    ledger = [dict(signal_key=s['signal_key'], entry_utc=s['entry_utc'],
                   decision='BLOCKED' if s['signal_key'] == 'B' else 'ENTRY_PROXY_REJECTED' if s['signal_key'] == 'D' else 'ADMITTED',
                   blocking_signal_keys=['A'] if s['signal_key'] == 'B' else [], uncertain_account_lineage=s['signal_key'] == 'C')
              for s in signals]
    trades = []
    for key, pl, fin in [('A', 1., -.2), ('C', -1., -.1)]:
        i = 0 if key == 'A' else 2
        p, s = paths[i], signals[i]
        trades.append(dict(signal_key=key, entry_utc=s['entry_utc'], exit_utc=p['interval_end_utc'],
             price_pl_gbp=pl, financing_gbp=fin, net_pl_gbp=round(pl+fin,4),
             net_r_actual_entry_risk=round(pl+fin,4), actual_entry_stop_risk_gbp=1.,
             exit_reason='OBSERVED_'+p['kind'], units=1000, entry_price=1.2, exit_price=p['exit_price'],
             exit_conversion_quote_max_age_seconds=0))
    cash = [dict(signal_key='A', time=signals[0]['entry_utc'], event='ENTRY', cash_gbp=0., balance_after_gbp=100.),
            dict(signal_key='A', time='2021-01-01T09:45:00Z', event='FINANCING', cash_gbp=-.2, balance_after_gbp=99.8),
            dict(signal_key='A', time=paths[0]['interval_end_utc'], event='EXIT', cash_gbp=1., balance_after_gbp=100.8),
            dict(signal_key='C', time=signals[2]['entry_utc'], event='ENTRY', cash_gbp=0., balance_after_gbp=100.8),
            dict(signal_key='C', time='2021-01-01T11:15:00Z', event='FINANCING', cash_gbp=-.1, balance_after_gbp=100.7),
            dict(signal_key='C', time=paths[2]['interval_end_utc'], event='EXIT', cash_gbp=-1., balance_after_gbp=99.7)]
    financing = [dict(signal_key='A', financing_gbp=-.2), dict(signal_key='C', financing_gbp=-.1)]
    summary = dict(status='COMPLETE_CONDITIONAL', max_positions=1, initial_balance_gbp=100.,
        decisions=dict(Counter(x['decision'] for x in ledger)), closed_trades=2, open_positions=0,
        qualifications=4, qualifications_processed=4, ending_balance_gbp=99.7,
        conditional_ending_nav_gbp=99.7, price_pl_gbp=0., financing_gbp=-.3,
        net_r_sum_actual_entry_risk=-.3, mean_net_r_actual_entry_risk=-.15,
        sampled_mid_nav_drawdown_pct=-1., uncertainty_causes={'OBSERVED_EXIT_AFTER_UNEXPLAINED_GAP': 1},
        first_uncertainty=dict(time=signals[2]['entry_utc'], cause='OBSERVED_EXIT_AFTER_UNEXPLAINED_GAP'),
        uncertain_account_lineage=True)
    result = dict(summary=summary, ledger=ledger, trades=trades, cash_events=cash,
                  open_positions=[], financing=financing, nav_samples=[])
    def put(name, obj, gz=False):
        raw = a.canon(obj)
        (directory / name).write_bytes(gzip.compress(raw) if gz else raw)
    def put_lines(path, rows):
        path.write_bytes(gzip.compress(b''.join(a.canon(r)+b'\n' for r in rows)))
    put_lines(directory/'signals.jsonl.gz', signals)
    put_lines(directory/'opportunities.jsonl.gz', opps)
    put_lines(source/'unique_price_paths.jsonl.gz', paths)
    account_rows = []
    for window in a.WINDOWS:
        for mode in a.MODES:
            rr = copy.deepcopy(result); rr['summary']['financing_mode'] = mode
            expected = dict(candidate_id=cid, window_id=window, financing_mode=mode,
                effective_start='2021-01-01T00:00:00Z', end='2021-01-02T00:00:00Z',
                **{k:str(v) for k,v in summary.items() if isinstance(v,(int,float))})
            account_rows.append(expected)
            put('accounts/'+window+'__'+mode+'.json.gz', dict(signature=signature, result=rr,
                sha256=a.digest(a.canon(rr))), True)
    put('account_summaries.json', account_rows)
    marker = dict(signature=signature, candidate_id=cid, accounts=14,
        files={p.relative_to(directory).as_posix():a.sha_file(p) for p in directory.rglob('*') if p.is_file()})
    put('COMPLETE.json', marker)
    root_files = {
        'run_state.json': dict(status='COMPLETE_CONDITIONAL_MATRIX', accounts_completed=224, candidates_completed=16, signature=signature),
        'baseline_parity.json': dict(status='PASS', exact_signal_records_matched=4086),
        'source_provenance.json': dict(source_pins=dict(signature='b'*64)),
        'protocol.json': dict(test='SYNTHETIC_ONLY')}
    for name,obj in root_files.items(): (source/name).write_bytes(a.canon(obj))
    pins = dict(source_signature=signature, source_native_sha256=native, candidate_id=cid,
        files={n:dict(bytes=(source/n).stat().st_size, sha256=a.sha_file(source/n)) for n in root_files},
        account_rows=account_rows, opportunity_rows=[dict(entry_cohort='ALL_OBSERVED',
            requested_period_start_utc='2021-01-01T00:00:00Z', period_end_utc='2021-01-02T00:00:00Z',
            qualifications='4', resolved_standalone_gbp100_opportunities='2',
            resolved_only_sum_frozen_net_r='4', resolved_only_mean_frozen_net_r='2', resolved_only_mean_adverse6_net_r='2')])
    env=dict(AUDUSD_MATRIX_OUTPUT_DIR=str(base), AUDUSD_RECON_OUTPUT_DIR=str(root/'audit'))
    return dict(signals={s['signal_key']:s for s in signals}, opps={o['signal_key']:o for o in opps},
                paths={p['path_cache_key']:p for p in paths}, result=result,
                expected=account_rows[0], pins=pins, env=env, source=source)


class ReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.f=fixture(Path(self.tmp.name))
    def tearDown(self): self.tmp.cleanup()
    def compare(self, result=None):
        return a.compare_account(self.f['signals'],self.f['opps'],self.f['paths'],
                                 result or self.f['result'],self.f['expected'])
    def test_exact_bridge_separates_blocked_assumed_and_matched_funding(self):
        b,j,g,c,u=self.compare()
        self.assertEqual(b['standalone_resolved_count'],2)
        self.assertEqual(b['subtract_shadow_only_net_r_sum'],3)
        self.assertAlmostEqual(b['matched_account_minus_shadow_net_r_sum'],-.2)
        self.assertAlmostEqual(b['add_account_only_net_r_sum'],-1.1)
        self.assertAlmostEqual(b['account_net_r_sum'],-.3)
        self.assertAlmostEqual(b['account_mean_net_r'],-.15)
        self.assertAlmostEqual(b['bridge_residual'],0)
        matched=next(r for r in j if r['signal_key']=='A')
        self.assertAlmostEqual(matched['matched_funding_r_difference'],-.2)
        self.assertEqual(u['direct_unresolved_path_closed_trades'],1)
    def test_reject_duplicate_decisions(self):
        r=copy.deepcopy(self.f['result']);r['ledger'].append(r['ledger'][0])
        with self.assertRaisesRegex(ValueError,'duplicate'): self.compare(r)
    def test_reject_missing_qualification(self):
        r=copy.deepcopy(self.f['result']);r['ledger'].pop()
        with self.assertRaisesRegex(ValueError,'omit'): self.compare(r)
    def test_reject_orphan_cash(self):
        r=copy.deepcopy(self.f['result']);r['cash_events'][0]['signal_key']='B'
        with self.assertRaisesRegex(ValueError,'orphaned'): self.compare(r)
    def test_reject_changed_running_cash_even_when_final_total_matches(self):
        r=copy.deepcopy(self.f['result']);r['cash_events'][0]['balance_after_gbp']=101
        with self.assertRaisesRegex(ValueError,'Running cash'): self.compare(r)
    def test_reject_wrong_r_denominator(self):
        r=copy.deepcopy(self.f['result']);r['trades'][0]['actual_entry_stop_risk_gbp']=2
        with self.assertRaisesRegex(ValueError,'Per-trade R'): self.compare(r)
    def test_boundary_difference_is_explicit(self):
        paths=copy.deepcopy(self.f['paths'])
        paths[self.f['opps']['B']['path_cache_key']]['interval_end_utc']='2021-01-03T00:00:00Z'
        _,rows,_,_,_=a.compare_account(self.f['signals'],self.f['opps'],paths,self.f['result'],self.f['expected'])
        self.assertTrue(next(r for r in rows if r['signal_key']=='B')['standalone_exit_after_account_window'])
    def test_complete_small_report_no_network_or_source_writes(self):
        before={p.relative_to(self.f['source']).as_posix():a.sha_file(p) for p in self.f['source'].rglob('*') if p.is_file()}
        job=a.Audit(self.f['env'],self.f['pins'])
        with patch('socket.create_connection', side_effect=AssertionError('Network forbidden')):
            job.run()
        self.assertEqual(job.state['status'],'COMPLETE',job.state)
        self.assertEqual(job.state['accounts_completed'],14)
        after={p.relative_to(self.f['source']).as_posix():a.sha_file(p) for p in self.f['source'].rglob('*') if p.is_file()}
        self.assertEqual(before,after)
        with zipfile.ZipFile(job.work/'AUDUSD_RECONCILIATION.zip') as z:
            self.assertIsNone(z.testzip())
            self.assertEqual(json.loads(z.read('validation.json'))['status'],'PASS')
            manifest=json.loads(z.read('report_manifest.json'))
            for name,w in manifest.items(): self.assertEqual(a.digest(z.read(name)),w['sha256'])
        again=a.Audit(self.f['env'],self.f['pins']);again.run()
        self.assertEqual(again.state['status'],'COMPLETE',again.state)
    def test_corrupt_checkpoint_stops_without_report(self):
        path=next((self.f['source']/'candidates').rglob('accounts/*.json.gz'))
        path.write_bytes(b'corrupt')
        job=a.Audit(self.f['env'],self.f['pins']);job.run()
        self.assertEqual(job.state['status'],'ERROR');self.assertIn('checksum',job.state['error'])
        self.assertFalse(job.state['result_ready'])
    def test_reviewed_summary_mismatch_stops(self):
        (self.f['source']/'protocol.json').write_bytes(b'{}')
        job=a.Audit(self.f['env'],self.f['pins']);job.run()
        self.assertEqual(job.state['status'],'ERROR');self.assertIn('summary changed',job.state['error'])
    def test_unsafe_path_rejected(self):
        with self.assertRaisesRegex(ValueError,'unsafe'): a.safe_file(self.f['source'],'../runner.lock')
    def test_health_head_and_post_never_start_audit(self):
        job=a.Audit(self.f['env'],self.f['pins']);app=a.application(job)
        for path,method in [('/health','GET'),('/status','HEAD'),('/status','POST'),('/results-reconciliation.zip','GET')]:
            response=[]
            body=b''.join(app(dict(PATH_INFO=path,REQUEST_METHOD=method),lambda s,h:response.append(s)))
            self.assertIsNone(job.thread)
            if method=='POST':self.assertEqual(response[0],'405 Method Not Allowed')
            if method=='HEAD':self.assertEqual(body,b'')
    def test_matrix_writer_lock_blocks_read(self):
        with (self.f['source'].parent/'runner.lock').open('rb') as lock:
            a.fcntl.flock(lock,a.fcntl.LOCK_EX|a.fcntl.LOCK_NB)
            job=a.Audit(self.f['env'],self.f['pins']);job.run()
            self.assertEqual(job.state['status'],'ERROR');self.assertIn('writer',job.state['error'])
    def test_source_output_overlap_rejected(self):
        env=dict(self.f['env'],AUDUSD_RECON_OUTPUT_DIR=str(self.f['source']/'audit'))
        with self.assertRaisesRegex(ValueError,'separate'): a.Audit(env,self.f['pins'])
    def test_optional_best_five_stat_can_be_unavailable(self):
        expected=dict(self.f['expected'],mean_net_r_without_best_five=None)
        a.check_account(self.f['result'],self.f['signals'],expected)
    def test_reconciliation_writer_lock_prevents_overlapping_publish(self):
        output=Path(self.f['env']['AUDUSD_RECON_OUTPUT_DIR']);output.mkdir()
        with (output/'runner.lock').open('ab') as lock:
            a.fcntl.flock(lock,a.fcntl.LOCK_EX|a.fcntl.LOCK_NB)
            job=a.Audit(self.f['env'],self.f['pins']);job.run()
            self.assertEqual(job.state['status'],'ERROR');self.assertIn('Another reconciliation',job.state['error'])


if __name__=='__main__': unittest.main()
