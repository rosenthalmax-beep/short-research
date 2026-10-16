"""Offline tests for locked-rule adapters, causal inputs and admission scenarios."""
import copy,importlib.util,json,math,pathlib,random,tempfile,unittest,zipfile,os
from datetime import timedelta
from unittest.mock import patch
import execution as e
import history as h
import locked_rules as rules
import signals
import m15_adapter
import admissions as gates
import app
import test_execution

T=e.dt('2012-06-04T08:00:00Z')

def bars(n,start=T,seconds=900,seed=1):
    rng=random.Random(seed);result=h.Bars();last=1.1
    for i in range(n):
        op=last+rng.gauss(0,.00003);body=rng.gauss(0,.00012)*(6 if i%19==0 else 1);cl=op+body
        high=max(op,cl)+rng.uniform(.00001,.00012);low=min(op,cl)-rng.uniform(.00001,.00012)
        result.append(dict(time=e.stamp(start+timedelta(seconds=i*seconds)),mid=dict(o=op,h=high,l=low,c=cl)))
        last=cl
    return result

def native(t,gran='M15',complete=True):
    return dict(time=e.stamp(t),complete=complete,mid=dict(o='1.1',h='1.2',l='1.0',c='1.11'))

def signal(key,sid='EUR_USD_H1_LONG',side='BUY',pair='EUR_USD',when=T,priority=0):
    return dict(signal_key=key,strategy_id=sid,pair=pair,side=side,timeframe='H1',entry_utc=e.stamp(when),signal_start_utc=e.stamp(when-timedelta(hours=1)),reference_entry=1.1,stop=1.099 if side=='BUY' else 1.101,target=1.104 if side=='BUY' else 1.096,rr=4,same_strategy_trigger_priority=priority)

def path(start=T,end=None,status='CLOSED_RESOLVED',kind='TARGET',r=2):
    end=end or start+timedelta(hours=1)
    return dict(status=status,kind=kind,certainty='RESOLVED_TO_CANDLE_INTERVAL' if status=='CLOSED_RESOLVED' else 'TOUCH_WITH_EARLIER_UNEXPLAINED_GAP',interval_start_utc=e.stamp(end-timedelta(seconds=5)),interval_end_utc=e.stamp(end),observed_path_price_r=r,observed_path_worst_bound_budget_r=r)

class IndicatorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bars=bars(1200);cls.h1=bars(1200,T-timedelta(days=30),3600,2);cls.h4=bars(1200,T-timedelta(days=150),14400,3);cls.daily=bars(600,T-timedelta(days=500),86400,4)
        spec=importlib.util.spec_from_file_location('original_pure_rules',pathlib.Path(__file__).parent/'locked_rules.py');cls.original=importlib.util.module_from_spec(spec);spec.loader.exec_module(cls.original)
    def test_atr_exact_against_original(self):
        self.assertEqual(list(h.atr(self.bars)),rules.atr_series(list(self.bars),14))
    def test_chf_seed_exact_against_original(self):
        self.assertEqual(list(h.atr(self.h1,chf=True)),rules.eurchf_wilder_series(list(self.h1)))
    def test_optional_sma_exact_against_original(self):
        for n in (20,50):self.assertEqual(list(h.sma(h.atr(self.bars),n)),rules.m15_sma_optional(rules.atr_series(list(self.bars),14),n))
    def test_previous_extreme_excludes_signal_and_matches_original(self):
        for lookback in (2,12,60,165,175):
            for field,maxi in [('low',False),('high',True)]:
                for i in (lookback-1,lookback,199,450,1199):
                    self.assertEqual(self.bars.previous_extreme(i,lookback,field,maxi),self.original.m15_previous_extreme(list(self.bars),i,lookback,field,maxi))
    def test_htf_exact_against_original(self):
        state=h.Htf(self.h1,ratio=True)
        for i in (0,20,250,700,1199):
            t=self.h1[i]['time']
            self.assertEqual(state.at(t),self.original.m15_htf_snapshot(list(self.h1),t,True))
    def test_htf_never_selects_current_or_future_candle(self):
        state=h.Htf(self.h1)
        for i in (2,300,900):
            t=self.h1[i]['time'];selected=state.at(t)
            self.assertEqual(selected['time'],self.h1[i-1]['time'])
            self.assertEqual(selected['complete_at'],t)
    def test_m15_adapter_exact_against_unmodified_function_all_pairs(self):
        values=h.atr(self.bars);means=h.sma(values,20);comparisons=0
        for pair in ('AUD_JPY','AUD_USD','EUR_GBP','EUR_JPY','EUR_USD','GBP_USD','USD_CAD','USD_JPY'):
            h1=h.Htf(self.h1,pair=='USD_JPY');h4=h.Htf(self.h4);daily=h.Htf(self.daily)
            for i in (199,220,390,701,999,1199):
                stamp=self.bars[i]['time']
                inputs={'M15':list(self.bars[:i+1]),'H1':list(self.h1),'H4':list(self.h4),'D':list(self.daily)}
                self.original.m15_fetch_recent=lambda instrument,gran,count,complete_only=True:inputs[gran]
                original=self.original.evaluate_latest_m15_pair(pair)
                actual=m15_adapter.evaluate_m15_at_index(pair,self.bars,i,values,means,
                    h1.at(stamp) if pair in {'USD_JPY','USD_CAD','EUR_GBP','EUR_JPY','AUD_USD'} else None,
                    h4.at(stamp) if pair in {'USD_JPY','USD_CAD'} else None,
                    daily.at(stamp) if pair in {'USD_JPY','USD_CAD'} else None)
                self.assertEqual(signals.clean(actual),signals.clean(original),(pair,i));comparisons+=1
        self.assertEqual(comparisons,48)
    def test_h1_daily_lookup_and_evaluation_exact_against_original(self):
        count=0
        for pair,config in rules.STRATEGIES.items():
            fast=h.prepare_long_daily(self.daily,config);original_state=self.original.build_daily_state(list(self.daily),config);atr=h.atr(self.h1)
            for i in (250,599,999):
                t=self.h1[i]['time'];self.assertEqual(fast.at(t),self.original.previous_daily_values(t,original_state,config))
                self.assertEqual(rules.evaluate_signal_at_index(pair,self.h1,atr,i,fast),self.original.evaluate_signal_at_index(pair,list(self.h1),list(atr),i,original_state));count+=1
        for pair,config in rules.SHORT_STRATEGIES.items():
            need=config.get('requires_daily_state',True) and config.get('signal_family') not in ('audusd_two_trigger','compression_breakdown')
            fast=h.prepare_short_daily(self.daily,config) if need else None
            old=self.original.build_short_daily_state(list(self.daily),config) if need else None
            atr=h.atr(self.h1,chf=pair=='EUR_CHF')
            for i in (250,599,999):
                self.assertEqual(rules.evaluate_short_signal_at_index(pair,self.h1,atr,i,fast),self.original.evaluate_short_signal_at_index(pair,list(self.h1),list(atr),i,old));count+=1
        self.assertEqual(count,51)
    def test_future_price_changes_do_not_change_past_m15_signal(self):
        original=copy.deepcopy(self.bars);altered=copy.deepcopy(self.bars);i=500
        for field in altered.columns:
            for j in range(i+1,len(altered)):altered.columns[field][j]*=2
        one=m15_adapter.evaluate_m15_at_index('EUR_USD',original,i,h.atr(original),h.sma(h.atr(original),20),None,None,None)
        two=m15_adapter.evaluate_m15_at_index('EUR_USD',altered,i,h.atr(altered),h.sma(h.atr(altered),20),None,None,None)
        self.assertEqual(one,two)
    def test_all_32_locked_strategy_ids_match_registry(self):
        self.assertEqual(len(signals.validate_registry(e.load_reference()['registry'])),32)
    def test_raw_recovery_before_any_position_filter(self):
        recovered=signals.recover_pair('EUR_CHF',self.h1,self.daily)
        self.assertTrue(all(not x['signal_was_position_filtered'] for x in recovered))
        self.assertEqual(len({x['signal_key'] for x in recovered}),len(recovered))

class NativeTests(unittest.TestCase):
    def ident(self,gran,start,end):return dict(pair='EUR_USD',granularity=gran,start=e.stamp(start),end=e.stamp(end))
    def test_h4_advance_crosses_autumn_clock_change(self):
        start=e.dt('2005-10-30T05:00:00Z')
        self.assertEqual(h.advance(start,'H4',1),e.dt('2005-10-30T10:00:00Z'))
        self.assertEqual(h.bar_end(start,'H4'),e.dt('2005-10-30T10:00:00Z'))
    def test_h4_advance_crosses_spring_clock_change(self):
        start=e.dt('2020-03-08T06:00:00Z')
        self.assertEqual(h.advance(start,'H4',1),e.dt('2020-03-08T09:00:00Z'))
        end=e.dt('2020-03-08T09:00:00Z')
        rows=h.HistoryStore.validate(dict(instrument='EUR_USD',granularity='H4',candles=[native(start,'H4')]),self.ident('H4',start,end))
        self.assertEqual(len(rows),1)
    def test_h4_entire_research_horizon_always_advances(self):
        t=h.align(h.START,'H4');end=h.align(h.END,'H4');count=0
        while t<end and count<50000:
            nxt=h.advance(t,'H4',1)
            self.assertGreater(nxt,t)
            self.assertEqual(h.align(nxt,'H4'),nxt)
            self.assertEqual(nxt.astimezone(e.NY).hour%4,1)
            t=nxt;count+=1
        self.assertEqual(t,end)
        self.assertGreater(count,47000)
    def test_history_coverage_guard_fails_instead_of_hanging(self):
        with tempfile.TemporaryDirectory() as directory:
            reader=h.MidReader('FAKE','https://api-fxtrade.oanda.com',transport=lambda req:dict(instrument='EUR_USD',granularity='H4',candles=[]))
            store=h.HistoryStore(directory,reader)
            original=h.advance
            try:
                with patch.object(h,'advance',side_effect=lambda t,gran,n:t if n==1 else original(t,gran,n)):
                    with self.assertRaisesRegex(ValueError,'coverage cursor did not advance'):store.series('EUR_USD','H4')
            finally:store.close()
    def test_daily_alignment_changes_utc_offset_across_dst(self):
        self.assertEqual(h.align(e.dt('2020-07-01T22:00:00Z'),'D'),e.dt('2020-07-01T21:00:00Z'))
        self.assertEqual(h.align(e.dt('2020-01-01T23:00:00Z'),'D'),e.dt('2020-01-01T22:00:00Z'))
    def test_h4_alignment_uses_ny17_not_utc_floor(self):
        self.assertEqual(h.align(e.dt('2020-07-01T23:00:00Z'),'H4'),e.dt('2020-07-01T21:00:00Z'))
    def test_daily_close_uses_wall_time_across_dst(self):
        self.assertEqual((h.bar_end(e.dt('2020-03-07T22:00:00Z'),'D')-e.dt('2020-03-07T22:00:00Z')).total_seconds(),23*3600)
    def test_historical_partial_daily_candle_not_used(self):
        start=e.dt('2020-07-01T21:00:00Z');end=e.dt('2020-07-02T17:00:00Z')
        self.assertEqual(h.HistoryStore.validate(dict(instrument='EUR_USD',granularity='D',candles=[native(start,'D')]),self.ident('D',start,end)),[])
    def test_boolean_complete_is_strict(self):
        self.assertEqual(h.HistoryStore.validate(dict(instrument='EUR_USD',granularity='M15',candles=[native(T,complete='false')]),self.ident('M15',T,T+timedelta(minutes=15))),[])
    def test_mid_reader_has_only_get_native_candle_route(self):
        calls=[]
        def transport(req):calls.append(req);return dict(instrument='EUR_USD',granularity='D',candles=[])
        reader=h.MidReader('FAKE','https://api-fxtrade.oanda.com',transport=transport)
        reader.get(dict(pair='EUR_USD',granularity='D',price='M',broker=reader.broker,start='2020-01-01T22:00:00Z',end='2020-01-02T22:00:00Z'))
        self.assertEqual(calls[0].get_method(),'GET');self.assertIn('price=M&',calls[0].full_url);self.assertNotIn('count=',calls[0].full_url)
        self.assertIn('dailyAlignment=17',calls[0].full_url)
    def test_mid_reader_refuses_requests_beyond_cutoff(self):
        reader=h.MidReader('FAKE','https://api-fxtrade.oanda.com',transport=lambda req:self.fail('Must not send'))
        with self.assertRaises(ValueError):reader.get(dict(pair='EUR_USD',granularity='H1',price='M',broker=reader.broker,start=e.stamp(h.END),end=e.stamp(h.END+timedelta(hours=1))))

class AdmissionTests(unittest.TestCase):
    def run_case(self,signals,paths,scenario='END_ASC',hedging=False):return gates.replay(signals,scenario,lambda s:paths[s['signal_key']],hedging=hedging)
    def test_pyramiding_blocks_then_releases(self):
        s=[signal('a'),signal('b',when=T+timedelta(minutes=30)),signal('c',when=T+timedelta(hours=1))]
        rows=self.run_case(s,{x['signal_key']:path(e.dt(x['entry_utc'])) for x in s})
        self.assertEqual([x['decision'] for x in rows],['ADMITTED_UNDER_SCENARIO','BLOCKED_UNDER_SCENARIO','ADMITTED_UNDER_SCENARIO'])
    def test_same_direction_different_strategy_overlaps(self):
        s=[signal('a'),signal('b',sid='EUR_USD_M15_LONG')]
        self.assertEqual(sum(r['decision']=='ADMITTED_UNDER_SCENARIO' for r in self.run_case(s,{'a':path(),'b':path()})),2)
    def test_opposite_pair_blocks_on_nonhedging_account(self):
        s=[signal('a'),signal('b',sid='EUR_USD_H1_SHORT',side='SELL')]
        rows=self.run_case(s,{'a':path(),'b':path()});self.assertEqual(rows[1]['gate'],'OPPOSITE_PAIR_NONHEDGING')
    def test_opposite_pair_allowed_when_explicitly_hedged_except_audjpy(self):
        s=[signal('a'),signal('b',sid='EUR_USD_H1_SHORT',side='SELL')]
        self.assertEqual(sum(r['decision']=='ADMITTED_UNDER_SCENARIO' for r in self.run_case(s,{'a':path(),'b':path()},hedging=True)),2)
        for x in s:x['pair']='AUD_JPY'
        self.assertEqual(sum(r['decision']=='ADMITTED_UNDER_SCENARIO' for r in self.run_case(s,{'a':path(),'b':path()},hedging=True)),1)
    def test_entry_rejection_does_not_occupy_slot(self):
        s=[signal('a'),signal('b',when=T+timedelta(minutes=1))]
        rows=self.run_case(s,{'a':dict(status='ENTRY_PROXY_REJECTED',entry_status='REJECTED_10T_ENTRY_PROXY'),'b':path()})
        self.assertEqual([r['decision'] for r in rows],['ENTRY_PROXY_REJECTED','ADMITTED_UNDER_SCENARIO'])
    def test_simultaneous_opposite_order_is_scenario_explicit(self):
        s=[signal('a'),signal('b',sid='EUR_USD_H1_SHORT',side='SELL')];paths={'a':path(),'b':path()}
        asc=self.run_case(s,paths,'END_ASC');desc=self.run_case(s,paths,'END_DESC')
        self.assertEqual([r['signal_key'] for r in asc if r['decision']=='ADMITTED_UNDER_SCENARIO'],['a'])
        self.assertEqual([r['signal_key'] for r in desc if r['decision']=='ADMITTED_UNDER_SCENARIO'],['b'])
    def test_same_strategy_trigger_priority_survives_reverse_order(self):
        s=[signal('b',priority=1),signal('a',priority=0)]
        rows=self.run_case(s,{'a':path(),'b':path()},'END_DESC');self.assertEqual(rows[0]['signal_key'],'a');self.assertEqual(rows[1]['decision'],'BLOCKED_UNDER_SCENARIO')
    def test_exit_interval_start_end_changes_only_declared_occupancy(self):
        s=[signal('a'),signal('b',when=T+timedelta(seconds=58))];paths={'a':path(end=T+timedelta(minutes=1)),'b':path(start=T+timedelta(seconds=58))}
        self.assertEqual(self.run_case(s,paths,'END_ASC')[1]['decision'],'BLOCKED_UNDER_SCENARIO')
        self.assertEqual(self.run_case(s,paths,'START_ASC')[1]['decision'],'ADMITTED_UNDER_SCENARIO')
    def test_missing_path_leaves_uncertain_lineage_after_release(self):
        s=[signal('a'),signal('b',when=T+timedelta(hours=2))];p={'a':path(status='UNRESOLVED_PATH'),'b':path(start=T+timedelta(hours=2))}
        rows=self.run_case(s,p);self.assertTrue(rows[1]['decision_has_uncertain_lineage'])
    def test_no_invented_six_position_limit(self):
        s=[signal(str(i),sid='s'+str(i),pair=pair) for i,pair in enumerate(e.PAIRS[:7])]
        rows=self.run_case(s,{x['signal_key']:path() for x in s});self.assertEqual(max(x['active_positions'] for x in rows),7)
    def test_risk_override_and_price_budget_accounting(self):
        s=[signal('a',sid='EUR_JPY_M15_SHORT',side='SELL',pair='EUR_JPY')];p={'a':path(r=2)};rows=self.run_case(s,p)
        summary,events=gates.model_accounting(rows,s,p);self.assertAlmostEqual(summary['conditional_closed_balance_units'],101.5)
        self.assertFalse(summary['actual_broker_returns']);self.assertFalse(summary['financing_included'])
    def test_unpriced_exit_suppresses_accounting(self):
        s=[signal('a')];p={'a':dict(status='UNRESOLVED_PATH',kind='AMBIGUOUS_BOTH',interval_start_utc=e.stamp(T),interval_end_utc=e.stamp(T+timedelta(seconds=5)))}
        rows=self.run_case(s,p);summary,_=gates.model_accounting(rows,s,p);self.assertIsNone(summary['conditional_closed_equity_cagr_pct'])
    def test_instant_exit_has_entry_before_own_exit_in_accounting(self):
        s=[signal('a')];p={'a':path(end=T,r=1)};rows=self.run_case(s,p);summary,events=gates.model_accounting(rows,s,p)
        self.assertEqual([x['event'] for x in events],['ENTRY','EXIT']);self.assertEqual(summary['conditional_closed_balance_units'],101)

class ServiceTests(unittest.TestCase):
    def test_rule_pins_accept_windows_line_endings_but_detect_code_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=pathlib.Path(tmp)/'rules.py';p.write_bytes(b'x=1\n');pinned=app.normalized_source_hash(p)
            p.write_bytes(b'\xef\xbb\xbfx=1\r\n');self.assertEqual(app.normalized_source_hash(p),pinned)
            p.write_bytes(b'x=2\r\n');self.assertNotEqual(app.normalized_source_hash(p),pinned)
    def test_health_and_head_do_not_start_job(self):
        with patch.object(app,'start',side_effect=AssertionError('Must not start')):
            for method,path in [('GET','/health'),('HEAD','/status')]:app.application({'REQUEST_METHOD':method,'PATH_INFO':path},lambda status,headers:None)
    def test_post_and_order_routes_rejected(self):
        for method,path,wanted in [('POST','/webhook','405'),('GET','/orders','404')]:
            statuses=[];app.application({'REQUEST_METHOD':method,'PATH_INFO':path},lambda status,headers:statuses.append(status));self.assertTrue(statuses[0].startswith(wanted))
    def test_source_download_streams_in_small_chunks(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=pathlib.Path(tmp)/app.SOURCES;p.write_bytes(b'x'*150000)
            with patch.object(app,'output_dir',return_value=pathlib.Path(tmp)),patch.dict(app.STATE,{'phase':'FINISHED'}):
                chunks=list(app.application({'REQUEST_METHOD':'GET','PATH_INFO':'/sources.zip'},lambda status,headers:None))
            self.assertEqual(sum(map(len,chunks)),150000);self.assertLessEqual(max(map(len,chunks)),65536)

class FullPipelineTests(unittest.TestCase):
    def test_all32_recovery_to_report_pipeline_without_network(self):
        # Synthetic prices validate plumbing, causality and every registry stream;
        # they are not a claimed reproduction of the historical trading result.
        prices={};series={}
        for pair in e.PAIRS:
            for gran,n,start,seconds in [('H1',1200,T-timedelta(days=30),3600),('M15',1200,T,900),('D',600,T-timedelta(days=500),86400),('H4',1200,T-timedelta(days=150),14400)]:
                value=bars(n,start,seconds,10+e.PAIRS.index(pair));series[(pair,gran)]=value
                if gran in ('H1','M15'):
                    for c in value:prices[(pair,e.stamp(c['time']+timedelta(seconds=seconds)))]=c['close']
        class FakeHistory:
            def __init__(self,*args,**kwargs):self.coverage=[];self.downloads=0
            def series(self,pair,gran):
                x=series[(pair,gran)];self.coverage.append(dict(pair=pair,granularity=gran,observed_bars=len(x)));return x
            def index(self):return []
            def export(self,z):z.writestr('mid_history_index.json','[]')
            def close(self):pass
        class FakeStore:
            def __init__(self,directory,broker,reader):self.reader=reader;self.downloads=0
            def import_seeds(self,directory):pass
            def segment_end(self,pair,gran,start,end):return end
            def range(self,pair,gran,start,end):
                result=[];t=start
                while t<end:
                    ref=prices.get((pair,e.stamp(t)),1.1);bid=ref-float(e.tick(pair));ask=ref+float(e.tick(pair))
                    result.append(dict(time=e.stamp(t),complete=True,mid=dict(o=ref,h=1.6,l=.4,c=ref),bid=dict(o=bid,h=1.6,l=.4,c=bid),ask=dict(o=ask,h=1.6,l=.4,c=ask)))
                    t+=timedelta(seconds=e.STEP[gran])
                return result
            def export(self,z):z.writestr('available_seed_source_index.json','[]')
            def close(self):pass
        with tempfile.TemporaryDirectory() as tmp,patch.object(app,'HistoryStore',FakeHistory),patch.object(e,'CandleStore',FakeStore),patch.dict(os.environ,{'OANDA_TOKEN':'FAKE','OANDA_ENV':'live'}),patch('urllib.request.OpenerDirector.open',side_effect=AssertionError('No external network')):
            result=app.run(pathlib.Path(tmp))
            self.assertEqual(result['strategy_streams_scanned'],32);self.assertGreater(result['raw_signals'],0);self.assertFalse(result['full_portfolio_validated'])
            with zipfile.ZipFile(pathlib.Path(tmp)/app.RESULT) as z:
                coverage=z.read('strategy_stream_coverage.csv').decode();self.assertEqual(len(coverage.splitlines()),33)
                self.assertIn('admission_ledger.csv',z.namelist());self.assertIn('conditional_price_accounting.json',z.namelist())
                self.assertFalse(json.loads(z.read('RUN_COMPLETE.json'))['orders_sent'])
            self.assertTrue((pathlib.Path(tmp)/app.SOURCES).is_file())
            with patch.object(FakeHistory,'export',side_effect=OSError('simulated source archive failure')):
                with self.assertRaises(OSError):app.run(pathlib.Path(tmp))
            with zipfile.ZipFile(pathlib.Path(tmp)/app.RESULT) as z:
                self.assertIn('raw_signals.csv',z.namelist());self.assertIn('ERROR.json',z.namelist())
                self.assertTrue(json.loads(z.read('ERROR.json'))['research_reports_preserved'])

if __name__=='__main__':
    suite=unittest.TestSuite([unittest.defaultTestLoader.loadTestsFromModule(test_execution),unittest.defaultTestLoader.loadTestsFromModule(__import__('__main__'))])
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
