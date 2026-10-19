"""Entry-policy, path-source, account, report and hosted-service regressions."""
import copy
import gzip
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from datetime import timedelta
from unittest.mock import patch
import entry_limit_service as app
import execution as e
import inputs
import control_checks as controls
import reports
from execution_sources import ExecutionWindows
from policy_paths import PolicyPaths
from policies import allowance
from accounting import Replay
from funding import Funding,MODES
from test_gbp import signal,path,FixedMarket,instruments,schedules,T
from test_source_windows import put,START,END,T as SOURCE_T

ROOT=Path(__file__).parent


class MemorySources:
    def __init__(self):self.current_differences=set()
    def begin_path(self):self.current_differences=set()


def quoted(s,adverse=0):
    s['target']=float(e.levels(s['pair'],s['side'],s['reference_entry'],s['stop'],s['rr'])['target'])
    sign=1 if s['side']=='BUY' else -1
    fill=s['reference_entry']+sign*adverse*float(e.tick(s['pair']))
    p=path(s)
    p.update(model_bid=fill if sign<0 else fill-.00002,model_ask=fill if sign>0 else fill+.00002,
             model_adverse_ticks=adverse)
    if adverse>10:p.update(status='ENTRY_PROXY_REJECTED',entry_status='REJECTED_10T_ENTRY_PROXY')
    return p


def mini_result(s,p,scenario='END_ASC',mode=MODES[1],terms=None):
    return Replay([s],lambda x:p,FixedMarket(),Funding(schedules()),terms or instruments(),scenario,mode,.01,
                  start=T,end=T+timedelta(hours=2)).run()


def expectations(result):
    s=result['summary'];name=s['scenario']+'__'+s['financing_mode']
    return dict(lanes={name:dict(summary_sha256=controls.digest(s),
               **{kind:dict(rows=len(result[kind]),sha256=controls.digest(result[kind])) for kind in controls.TABLES})})


class CapsTests(unittest.TestCase):
    def test_fixed_allowances_and_inclusive_boundary(self):
        for pair,ref,stop in [('GBP_USD',1.25,1.24),('USD_JPY',150.,149.)]:
            for side in ('BUY','SELL'):
                sl=stop if side=='BUY' else 2*ref-stop
                sign=1 if side=='BUY' else -1
                for cap in (10,15,20,30):
                    fill=ref+sign*cap*float(e.tick(pair))
                    g=e.entry_preview(pair,side,ref,sl,3.,fill,fill,cap)
                    self.assertEqual(g['status'],'ADMITTED_ENTRY_PROXY')
                    self.assertEqual(g['price_bound'],e.broker_price(pair,fill))
                    beyond=fill+sign*float(e.tick(pair))
                    self.assertTrue(e.entry_preview(pair,side,ref,sl,3.,beyond,beyond,cap)['status'].startswith('REJECTED_'))

    def test_relative_rule_floor_rounding_ceiling(self):
        s=signal()
        for distance,expected in [(50,10),(109,10),(159,15),(210,21),(1000,30)]:
            s['stop']=s['reference_entry']-distance*.00001
            self.assertEqual(allowance('STOP10_MAX30',s),expected)

    def test_bad_cap_and_geometry_checks_remain(self):
        for bad in (9,31,12.5):
            with self.assertRaises(ValueError):e.levels('GBP_USD','BUY',1.25,1.24,3,bad)
        self.assertEqual(e.entry_preview('GBP_USD','BUY',1.25,1.24,3,1.23,1.23,30)['status'],'REJECTED_STOP_GEOMETRY_PROXY')
        self.assertEqual(e.entry_preview('EUR_CHF','SELL',1.,1.01,1.,.98,.98,30)['status'],'REJECTED_30T_ENTRY_PROXY')
        self.assertEqual(e.entry_preview('EUR_CHF','SELL',1.,1.00001,.1,.99999,.99999,30)['status'],'REJECTED_TARGET_ALREADY_PASSED_PROXY')

    def test_control_path_is_original_and_alternative_immutable(self):
        s=signal();p=quoted(s);before=copy.deepcopy(p)
        with tempfile.TemporaryDirectory() as d:
            paths=PolicyPaths(d,{s['signal_key']:p},MemorySources())
            self.assertIs(paths.get(s),p)
            paths.policy='FIXED_30';q=paths.get(s)
            self.assertEqual(p,before);self.assertEqual(q['exit_price'],p['exit_price'])
            self.assertGreater(q['model_price_bound'],p['model_price_bound']);paths.close()

    def test_new_eligible_exit_shared_and_checkpoint_resumes(self):
        s=signal();p=quoted(s,13);sources=MemorySources()
        outcome=dict(kind='STOP',exit_price=1.24,interval_start_utc=e.stamp(T+timedelta(hours=1)),
                     interval_end_utc=e.stamp(T+timedelta(hours=1,minutes=1)),certainty='RESOLVED_TO_CANDLE_INTERVAL')
        with tempfile.TemporaryDirectory() as d:
            paths=PolicyPaths(d,{'a':p},sources);paths.policy='FIXED_15'
            with patch('policy_paths.e.first_exit',return_value=outcome) as compute:
                a=paths.get(s);paths.policy='FIXED_30';b=paths.get(s)
                self.assertEqual(compute.call_count,1)
            self.assertEqual(a['exit_price'],b['exit_price']);self.assertNotEqual(a['model_price_bound'],b['model_price_bound'])
            paths.close();paths=PolicyPaths(d,{'a':p},sources);paths.policy='FIXED_20'
            with patch('policy_paths.e.first_exit',side_effect=AssertionError('must resume')):self.assertEqual(paths.get(s)['kind'],'STOP')
            paths.close();self.assertEqual(p['status'],'ENTRY_PROXY_REJECTED')

    def test_unknown_entry_never_invented(self):
        s=signal();p=dict(signal_key='a',status='UNRESOLVED_ENTRY')
        with tempfile.TemporaryDirectory() as d:
            paths=PolicyPaths(d,{'a':p},MemorySources());paths.policy='FIXED_30'
            self.assertIs(paths.get(s),p);paths.close()

    def test_source_difference_qualifies_new_path(self):
        with tempfile.TemporaryDirectory() as d:
            source=MemorySources();source.current_differences={'difference'}
            paths=PolicyPaths(d,{},source)
            p=paths.qualify(dict(status='CLOSED_RESOLVED',certainty='RESOLVED_TO_CANDLE_INTERVAL'))
            self.assertEqual(p['status'],'UNRESOLVED_PATH')
            source.current_differences=set()
            p['status']='CLOSED_RESOLVED'
            self.assertEqual(paths.qualify(p)['status'],'UNRESOLVED_PATH');paths.close()

    def test_wider_bound_reduces_existing_trade_units(self):
        s=signal();p=quoted(s)
        with tempfile.TemporaryDirectory() as d:
            paths=PolicyPaths(d,{'a':p},MemorySources());old=mini_result(s,p)
            paths.policy='FIXED_30';new=mini_result(s,paths.get(s))
            self.assertLess(new['trades'][0]['units'],old['trades'][0]['units']);paths.close()

    def test_new_admission_changes_strategy_blocking(self):
        a=signal('a',sid='same');b=signal('b',sid='same',offset=15)
        pa=quoted(a,13);pb=quoted(b)
        with tempfile.TemporaryDirectory() as d:
            paths=PolicyPaths(d,{'a':pa,'b':pb},MemorySources())
            def run():return Replay([a,b],paths.get,FixedMarket(),Funding(schedules()),instruments(),'END_ASC',MODES[0],.01,start=T,end=T+timedelta(hours=2)).run()
            old=run();paths.policy='FIXED_15'
            outcome=dict(kind='STOP',exit_price=1.24,interval_start_utc=e.stamp(T+timedelta(hours=1)),interval_end_utc=e.stamp(T+timedelta(hours=1,minutes=1)),certainty='RESOLVED_TO_CANDLE_INTERVAL')
            with patch('policy_paths.e.first_exit',return_value=outcome):new=run()
            self.assertEqual([r['decision'] for r in old['ledger']],['ENTRY_PROXY_REJECTED','ADMITTED'])
            self.assertEqual([r['decision'] for r in new['ledger']],['ADMITTED','BLOCKED'])
            _,stats=reports.admission_changes(old,new)
            self.assertEqual((stats['added_admissions'],stats['displaced_admissions']),(1,1));self.assertAlmostEqual(stats['cash_attribution_residual_gbp'],0,places=7)
            paths.close()

    def test_numeric_source_equivalence_and_frozen_first(self):
        with tempfile.TemporaryDirectory() as d:
            frozen=e.CandleStore(Path(d)/'f','https://api-fxtrade.oanda.com');fresh=e.CandleStore(Path(d)/'n','https://api-fxtrade.oanda.com')
            put(frozen,SOURCE_T,SOURCE_T+timedelta(minutes=15),'1.25000');put(frozen,START,END,'1.25')
            source=ExecutionWindows(frozen,fresh)
            self.assertEqual(len(source.range('GBP_USD','M15',SOURCE_T,SOURCE_T+timedelta(minutes=15))),1)
            self.assertEqual(fresh.downloads,0);self.assertEqual(source.differences(),[]);frozen.close();fresh.close()

    def test_conflicting_sources_isolated_without_overwrite(self):
        with tempfile.TemporaryDirectory() as d:
            frozen=e.CandleStore(Path(d)/'f','https://api-fxtrade.oanda.com');fresh=e.CandleStore(Path(d)/'n','https://api-fxtrade.oanda.com')
            key=put(frozen,SOURCE_T,SOURCE_T+timedelta(minutes=15),'1.25');put(frozen,START,END,'1.26');put(fresh,START,END,'1.27','NEW_DOWNLOAD')
            before=frozen.db.execute('SELECT body,sha256 FROM blocks WHERE key=?',(key,)).fetchone()
            source=ExecutionWindows(frozen,fresh);rows=source.range('GBP_USD','M15',SOURCE_T,SOURCE_T+timedelta(minutes=15))
            self.assertEqual(rows[0]['mid']['o'],'1.27');self.assertTrue(source.current_differences)
            self.assertEqual(before,frozen.db.execute('SELECT body,sha256 FROM blocks WHERE key=?',(key,)).fetchone());frozen.close();fresh.close()

    def test_native_segments_use_cached_boundary(self):
        with tempfile.TemporaryDirectory() as d:
            frozen=e.CandleStore(Path(d)/'f','https://api-fxtrade.oanda.com');fresh=e.CandleStore(Path(d)/'n','https://api-fxtrade.oanda.com')
            put(frozen,SOURCE_T,SOURCE_T+timedelta(minutes=15),'1.25');source=ExecutionWindows(frozen,fresh)
            self.assertEqual(source.segment_end('GBP_USD','M15',SOURCE_T,SOURCE_T+timedelta(days=7)),SOURCE_T+timedelta(minutes=15));frozen.close();fresh.close()

    def test_calendar_returns_carry_and_exclude_partial_year(self):
        result=dict(summary=dict(initial_balance_gbp=100),ledger=[],trades=[],monthly_observations=[
            dict(month='2005-01',closing_observed_nav_gbp=110,last_observed_time='2005-01-31T23:45:00Z'),
            dict(month='2006-03',closing_observed_nav_gbp=121,last_observed_time='2006-03-15T00:00:00Z')])
        periods=reports.period_tables(result,'2005-01-01T00:00:00Z','2006-03-15T00:00:00Z')
        self.assertAlmostEqual(periods['monthly'][0]['nav_return_pct'],10)
        self.assertEqual(periods['monthly'][1]['nav_return_pct'],0)
        self.assertTrue(periods['annual'][0]['complete_calendar_year']);self.assertFalse(periods['annual'][1]['complete_calendar_year'])
        self.assertEqual(len(periods['rolling12']),3)
        toy=dict(summary=dict(decisions={'ADMITTED':3}),
                 ledger=[dict(signal_key=k,decision='ADMITTED') for k in ('b','a','c')],
                 trades=[dict(signal_key=k,net_pl_gbp=pl) for k,pl in [('c',-1),('b',-1),('a',2)]])
        self.assertEqual(reports.extra_summary(toy,periods,'2005-01-01T00:00:00Z','2006-03-15T00:00:00Z')['max_losing_streak_entry_order'],1)

    def test_control_checks_detect_cash_and_row_changes(self):
        s=signal();result=mini_result(s,quoted(s));expected=expectations(result)
        self.assertEqual(controls.check(result,expected)['status'],'PASS')
        changed=copy.deepcopy(result);changed['cash_events'][0]['cash_gbp']=.01
        self.assertEqual(controls.check(changed,expected)['status'],'FAIL')
        changed=copy.deepcopy(result);changed['ledger'].append(changed['ledger'][0])
        self.assertEqual(controls.check(changed,expected)['status'],'FAIL')

    def test_checkpoint_checksum(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'lane.gz';result=mini_result(signal(),quoted(signal()))
            p.write_bytes(gzip.compress(e.canon(dict(signature='s',result=result,sha256=e.sha(e.canon(result))))))
            self.assertEqual(app.checkpoint_load(p,'s'),result)
            with self.assertRaises(ValueError):app.checkpoint_load(p,'different')

    def test_http_health_head_and_post_do_not_start_job(self):
        with patch.object(app,'start_once') as start:
            for method,url in [('GET','/health'),('HEAD','/status'),('POST','/status'),('POST','/results.zip')]:
                status=[];list(app.application(dict(REQUEST_METHOD=method,PATH_INFO=url),lambda code,h:status.append(code)))
                self.assertEqual(status[0][:3],'405' if method=='POST' else '200')
            start.assert_not_called()

    def test_full_service_packages_resumes_and_stops_failed_control(self):
        s=signal();p=quoted(s);evidence=inputs.broker(ROOT)
        original=Replay([s],lambda x:p,FixedMarket(),Funding(evidence['funding_schedules']),evidence['instruments'],
                        'END_ASC',MODES[1],.01,start=T,end=T+timedelta(hours=2)).run()
        expected=expectations(original)
        baseline=dict(evaluation_start=e.stamp(T),historical_cutoff=e.stamp(T+timedelta(hours=2)))
        with tempfile.TemporaryDirectory() as d:
            with patch('entry_limit_service.inputs.baseline',return_value=([s],{'a':p},baseline)),patch('entry_limit_service.control_checks.load',return_value=expected),\
                 patch('entry_limit_service.e.CandleStore.import_seeds'),patch('entry_limit_service.inputs.import_additions'),patch('entry_limit_service.Market',side_effect=lambda *args:FixedMarket()):
                result=app.run(d,True,'FIXED_15','END_ASC',MODES[1]);self.assertEqual(result['status'],'COMPLETE_WITH_LIMITATIONS',result)
                z=zipfile.ZipFile(Path(d)/'entry_limit_v1'/app.RESULT);self.assertIsNone(z.testzip())
                self.assertIn('policy_comparison.csv',z.namelist());z.close()
                self.assertTrue((Path(d)/'entry_limit_v1/EQUITY_FIXED_15.zip').is_file())
                with patch('entry_limit_service.Replay',side_effect=AssertionError('must resume')):
                    self.assertEqual(app.run(d,True,'FIXED_15','END_ASC',MODES[1])['status'],'COMPLETE_WITH_LIMITATIONS')
            bad=copy.deepcopy(expected);next(iter(bad['lanes'].values()))['summary_sha256']='bad'
            with tempfile.TemporaryDirectory() as other:
                with patch('entry_limit_service.inputs.baseline',return_value=([s],{'a':p},baseline)),patch('entry_limit_service.control_checks.load',return_value=bad),\
                     patch('entry_limit_service.e.CandleStore.import_seeds'),patch('entry_limit_service.inputs.import_additions'),patch('entry_limit_service.Market',side_effect=lambda *args:FixedMarket()):
                    failed=app.run(other,True,'FIXED_15','END_ASC',MODES[1]);self.assertEqual(failed['status'],'ERROR')
                    self.assertEqual(len(failed['control_checks']),1)
                    self.assertFalse(list((Path(other)/'entry_limit_v1').glob('FIXED_15__*.json.gz')))


if __name__=='__main__':unittest.main()
