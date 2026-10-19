"""Fresh-account boundaries, accounting, controls and service regressions."""
import copy, csv, io, json, tempfile, unittest, zipfile
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch
import admissions, app, execution as e, inputs
from accounting import Replay
from funding import Funding, MODES
from test_gbp import T, FixedMarket, signal, path, schedules, instruments
from test_caps import expectations, quoted
from window_replay import windows, select_signals, WindowPaths, audit, comparison, robust_summary


def window(start=T, end=T+timedelta(hours=2), key='TEST'):
    return dict(window_id=key, window_kind='ROLLING_FIVE_YEAR', start=e.stamp(start), end=e.stamp(end))


def replay(signals, paths, w=None, scenario='END_ASC', mode=MODES[0], market=None, terms=None):
    w=w or window(); wrapped=WindowPaths(lambda s:paths[s['signal_key']],w,scenario)
    r=Replay(select_signals(signals,w),wrapped.get,market or FixedMarket(),Funding(schedules()),terms or instruments(),
             scenario,mode,.01,start=w['start'],end=w['end'],keep_nav=True).run()
    return r,wrapped.evaluated


class WindowTests(unittest.TestCase):
    def test_frozen_schedule(self):
        rows=windows('2005-01-01T00:00:00Z','2026-09-20T18:29:00Z')
        self.assertEqual(len(rows),18)
        self.assertEqual(rows[0]['window_id'],'FIVE_YEAR_2005_2010')
        self.assertEqual(rows[-2]['window_id'],'FIVE_YEAR_2021_2026')
        self.assertEqual(rows[-1]['window_kind'],'RECENT_DIAGNOSTIC')
        self.assertEqual(e.dt(rows[-1]['end']),e.dt('2026-09-20T18:29:00Z'))

    def test_all_saved_paths_obey_each_scheduled_window(self):
        signals,saved,baseline=inputs.baseline(Path(__file__).parent)
        total=0
        for w in windows(baseline['evaluation_start'],baseline['historical_cutoff']):
            for scenario in admissions.SCENARIOS:
                wrapped=WindowPaths(lambda s:saved[s['signal_key']],w,scenario)
                for signal_row in select_signals(signals,w):
                    if signal_row['signal_key'] not in saved: continue
                    p=wrapped.get(signal_row);total+=1
                    if p['status'] in ('ENTRY_PROXY_REJECTED','UNRESOLVED_ENTRY','ERROR','NEEDS_CANDLE_DATA'): continue
                    if p['kind']=='OPEN_AT_FIXED_CUTOFF': self.assertIsNone(p.get('exit_price'))
                    else:
                        release=admissions.release_time(p,scenario)
                        self.assertLessEqual(release,e.dt(w['end']))
                        self.assertGreaterEqual(release,e.dt(signal_row['entry_utc']))
        self.assertGreater(total,50000)

    def test_half_open_selection(self):
        ss=[signal(k,offset=n) for k,n in [('a',-15),('b',0),('c',119),('d',120)]]
        self.assertEqual([s['signal_key'] for s in select_signals(ss,window())],['b','c'])

    def test_future_win_loss_censored_without_mutating_original(self):
        s=signal(); a=path(s,180,price=9.);b=path(s,180,price=.01)
        first,ev=replay([s],{'a':a});second,_=replay([s],{'a':b})
        for k in ('summary','ledger','trades','financing','cash_events','open_positions','nav_samples'):
            self.assertEqual(first[k],second[k])
        self.assertEqual(first['summary']['closed_trades'],0)
        self.assertEqual(first['summary']['open_positions'],1)
        self.assertEqual(first['summary']['ending_balance_gbp'],100)
        self.assertIsNone(first['summary']['net_win_rate_pct'])
        self.assertIsNone(ev['a']['exit_price']);self.assertEqual(a['exit_price'],9.)
        self.assertEqual(audit(first,window())['status'],'PASS')

    def test_open_end_uses_causal_mid_and_sided_prices(self):
        s=signal();m=FixedMarket({'GBP_USD':dict(mid=1.252,bid=1.251,ask=1.253)})
        r,_=replay([s],{'a':path(s,180)},market=m);q=r['summary']
        self.assertEqual(q['ending_balance_gbp'],100)
        self.assertGreater(q['conditional_ending_nav_gbp'],100)
        self.assertLess(q['conditional_ending_liquidation_equity_gbp'],q['conditional_ending_nav_gbp'])

    def test_open_financing_stops_at_end_and_counts_once(self):
        s=signal();w=window(end=T+timedelta(days=1))
        r,_=replay([s],{'a':path(s,3*24*60)},w,mode=MODES[3])
        self.assertEqual(len(r['financing']),1);self.assertLess(r['summary']['financing_gbp'],0)
        self.assertEqual(r['summary']['closed_trades'],0);self.assertEqual(audit(r,w)['status'],'PASS')

    def test_cutoff_straddles_exit_interval(self):
        s=signal();p=path(s,60);w=window(end=T+timedelta(minutes=60,seconds=30))
        r,ev=replay([s],{'a':p},w,scenario='END_ASC')
        self.assertEqual(r['summary']['open_positions'],1)
        self.assertTrue(ev['a']['window_exit_interval_straddles_cutoff'])
        self.assertEqual(ev['a']['status'],'UNRESOLVED_PATH')
        self.assertTrue(r['summary']['uncertain_account_lineage'])
        r,_=replay([s],{'a':p},w,scenario='START_ASC');self.assertEqual(r['summary']['closed_trades'],1)

    def test_exit_at_end_allowed_entry_at_end_excluded(self):
        a,b=signal('a'),signal('b',offset=61);w=window(end=T+timedelta(minutes=61))
        r,_=replay([a,b],{'a':path(a,60),'b':path(b,120)},w)
        self.assertEqual(r['summary']['closed_trades'],1);self.assertEqual(r['summary']['qualifications'],1)
        self.assertEqual(r['trades'][0]['exit_utc'],w['end'])

    def test_open_path_and_uncertainty_preserved(self):
        s=signal()
        for status in ('OPEN_AT_CUTOFF','UNRESOLVED_PATH'):
            p=path(s);p.update(kind='OPEN_AT_FIXED_CUTOFF',status=status,exit_price=None)
            r,ev=replay([s],{'a':p})
            self.assertEqual(r['summary']['open_positions'],1);self.assertEqual(ev['a']['status'],status)
        self.assertTrue(r['summary']['uncertain_account_lineage'])

    def test_rejected_unknown_not_promoted(self):
        s=signal()
        for status,decision in [('UNRESOLVED_ENTRY','UNRESOLVED_ENTRY_OMITTED'),('ENTRY_PROXY_REJECTED','ENTRY_PROXY_REJECTED')]:
            r,_=replay([s],{'a':dict(status=status,entry_status='TEST')})
            self.assertEqual(r['ledger'][0]['decision'],decision)

    def test_flat_start_ignores_earlier_blocker(self):
        a,b=signal('a',offset=-15,sid='same'),signal('b',sid='same')
        r,_=replay([a,b],{'a':path(a,180),'b':path(b,60)})
        self.assertEqual([v['signal_key'] for v in r['ledger']],['b'])
        self.assertEqual(r['ledger'][0]['decision'],'ADMITTED')
        self.assertEqual(r['ledger'][0]['risk_budget_gbp'],1)

    def test_new_accounts_restart_at_100(self):
        a,b=signal('a'),signal('b',offset=120);paths={'a':path(a,30),'b':path(b,150)}
        first,_=replay([a,b],paths,window(end=T+timedelta(hours=1)))
        second,_=replay([a,b],paths,window(start=T+timedelta(hours=2),end=T+timedelta(hours=4)))
        self.assertGreater(first['summary']['ending_balance_gbp'],100)
        self.assertEqual(second['summary']['initial_balance_gbp'],100)
        self.assertEqual(second['ledger'][0]['risk_budget_gbp'],1)

    def test_censored_open_position_still_blocks(self):
        a,b=signal('a',sid='same'),signal('b',offset=90,sid='same')
        r,_=replay([a,b],{'a':path(a,180),'b':path(b,100)})
        self.assertEqual([v['decision'] for v in r['ledger']],['ADMITTED','BLOCKED'])

    def test_capacity_rejections_retained_and_flagged(self):
        s=signal();terms=instruments();next(i for i in terms if i['name']=='GBP_USD')['maximumOrderUnits']='1'
        r,ev=replay([s],{'a':path(s)},terms=terms)
        row,_,_,checked=comparison(r,r,window(),'FIXED_10',ev)
        self.assertEqual(checked['maximum_order_unit_rejections'],1);self.assertTrue(row['capacity_limit_affected'])

    def test_cash_financing_and_temporal_audits_detect_changes(self):
        s=signal();r,_=replay([s],{'a':path(s,30)})
        for field in ('future','financing','cash'):
            q=copy.deepcopy(r)
            if field=='future':q['trades'][0]['exit_utc']=e.stamp(T+timedelta(days=1))
            elif field=='financing':q['summary']['financing_gbp']=1.
            else:q['cash_events'][0]['cash_gbp']=9.
            with self.assertRaises(ValueError):audit(q,window())

    def test_missing_quote_fails_audit(self):
        from market import QuoteUnavailable
        s=signal();r,_=replay([s],{'a':path(s,180)},market=FixedMarket({'GBP_USD':QuoteUnavailable('missing')}))
        with self.assertRaises(ValueError):audit(r,window())

    def test_robust_summary_excludes_recent_overlap_diagnostic(self):
        s=signal();r,ev=replay([s],{'a':path(s,30)});row,*_=comparison(r,r,window(),'FIXED_10',ev)
        recent=dict(row,window_kind='RECENT_DIAGNOSTIC',conditional_liquidation_equity_cagr_pct=999.)
        q=robust_summary([row,recent])[0]
        self.assertEqual(q['windows'],1);self.assertFalse(q['overlapping_windows_are_independent'])
        self.assertEqual(q['median_delta_liquidation_cagr_pp'],0)


class ServiceTests(unittest.TestCase):
    def test_readonly_routes_no_unintended_start(self):
        with patch.object(app,'start_once') as start:
            for method,url in [('GET','/health'),('HEAD','/status'),('POST','/status'),('POST','/results.zip')]:
                states=[];body=b''.join(app.application(dict(REQUEST_METHOD=method,PATH_INFO=url),lambda c,h:states.append(c)))
                self.assertEqual(states[0][:3],'405' if method=='POST' else '200')
                if method=='HEAD':self.assertEqual(body,b'')
            start.assert_not_called()

    def test_pipeline_all_controls_window_reports_resume_and_fail_stop(self):
        s=signal();p=quoted(s);evidence=inputs.broker(Path(__file__).parent)
        baseline=dict(evaluation_start=e.stamp(T),historical_cutoff=e.stamp(T+timedelta(hours=2)))
        expected=dict(lanes={})
        for scenario in admissions.SCENARIOS:
            for mode in MODES:
                r=Replay([s],lambda _:p,FixedMarket(),Funding(evidence['funding_schedules']),evidence['instruments'],
                         scenario,mode,.01,start=baseline['evaluation_start'],end=baseline['historical_cutoff']).run()
                expected['lanes'].update(expectations(r)['lanes'])
        scheduled=[window(end=T+timedelta(minutes=30),key='CENSORED'),window(key='CLOSED')]
        def patches(target):
            return [patch.object(app.inputs,'baseline',return_value=([s],{'a':p},baseline)),
                    patch.object(app.control_checks,'load',return_value=target),patch.object(app.e.CandleStore,'import_seeds'),
                    patch.object(app.inputs,'import_additions'),patch.object(app,'Market',side_effect=lambda *args:FixedMarket()),
                    patch.object(app,'windows',return_value=scheduled)]
        from contextlib import ExitStack
        with tempfile.TemporaryDirectory() as d,ExitStack() as stack:
            for q in patches(expected):stack.enter_context(q)
            result=app.run(d,True)
            self.assertEqual(result['status'],'COMPLETE_WITH_LIMITATIONS',result)
            self.assertEqual(result['controls_passed'],16);self.assertEqual(result['lanes_completed'],96)
            with zipfile.ZipFile(Path(d)/'small_account_entry_limit_v1'/app.RESULT) as z:
                self.assertIsNone(z.testzip())
                for name,meta in json.loads(z.read('report_manifest.json')).items():
                    self.assertEqual(len(z.read(name)),meta['bytes']);self.assertEqual(e.sha(z.read(name)),meta['sha256'])
                rows=list(csv.DictReader(io.StringIO(z.read('window_comparison.csv').decode())))
                self.assertEqual(len(rows),96)
                self.assertTrue(all(int(r['open_positions'])==1 for r in rows if r['window_id']=='CENSORED'))
                self.assertTrue(all(int(r['closed_trades'])==1 for r in rows if r['window_id']=='CLOSED'))
            with patch.object(app,'Replay',side_effect=AssertionError('Must resume')):
                resumed=app.run(d,True);self.assertEqual(resumed['status'],'COMPLETE_WITH_LIMITATIONS',resumed)
        bad=copy.deepcopy(expected);next(iter(bad['lanes'].values()))['summary_sha256']='wrong'
        with tempfile.TemporaryDirectory() as d,ExitStack() as stack:
            for q in patches(bad):stack.enter_context(q)
            result=app.run(d,True);self.assertEqual(result['status'],'ERROR');self.assertEqual(result['controls_passed'],0)
            self.assertFalse(list((Path(d)/'small_account_entry_limit_v1').glob('CENSORED__*')))


if __name__=='__main__':unittest.main(verbosity=2)
