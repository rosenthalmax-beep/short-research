import ast,gzip,hashlib,json,math,os,sys,tempfile,threading,unittest,zipfile
from pathlib import Path
from datetime import datetime,timedelta,timezone
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import replay
import engine
import app as server_app
UTC=timezone.utc

def bar(t,value=1.1,complete=True):
    return dict(time=t,open=value,close=value+0.0001,high=value+0.0004,low=value-0.0004,volume=10,complete=complete)

def synthetic(gran,start,end,count,pair='EUR_USD'):
    if gran=='D':
        current=engine.current_daily_start(end)
        times=[(current.astimezone(replay.NY)+timedelta(days=i)).astimezone(UTC) for i in range(-count-10,2)]
    else:
        sec=replay.GRAN_SECONDS[gran]
        current=datetime.fromtimestamp(math.floor(end.timestamp()/sec)*sec,UTC)
        times=[current+timedelta(seconds=sec*i) for i in range(-count-10,2)]
    values=[bar(t,1.1+0.0002*math.sin(i*.2)+i*0.0000001) for i,t in enumerate(times)]
    if pair=='EUR_CHF' and gran=='H1':
        by={x['time']:x for x in values}
        for x in engine.EURCHF_FROZEN_ANCHOR['bars']:
            b=dict(x,time=replay.parse(x['time']),complete=True,volume=10);by[b['time']]=b
        values=[by[t] for t in sorted(by)]
    return values

class SourceTests(unittest.TestCase):
    def test_pure_financial_functions_match_supplied_source(self):
        manifest=replay.verify_sources()
        original=ast.parse((replay.ROOT/'reference/strategy_probe.py').read_text())
        extracted=ast.parse((replay.ROOT/'engine.py').read_text())
        a={n.name:ast.dump(n,include_attributes=False) for n in original.body if isinstance(n,ast.FunctionDef)}
        b={n.name:ast.dump(n,include_attributes=False) for n in extracted.body if isinstance(n,ast.FunctionDef)}
        self.assertEqual(len(b),34)
        self.assertTrue(all(a[k]==v for k,v in b.items()))

    def test_all_32_registered_strategies_are_distinct(self):
        rows=json.loads((replay.ROOT/'inputs/registry.json').read_text())
        self.assertEqual((len(rows),len({x['strategy_id'] for x in rows})),(32,32))
        self.assertEqual(sum(x['timeframe']=='H1' for x in rows),17)
        self.assertEqual(sum(x['timeframe']=='M15' for x in rows),15)

    def test_engine_has_no_network_or_watcher_or_database_imports(self):
        tree=ast.parse((replay.ROOT/'engine.py').read_text())
        imports=[]
        for n in tree.body:
            if isinstance(n,ast.Import):imports.extend(a.name for a in n.names)
            if isinstance(n,ast.ImportFrom):imports.append(n.module)
        self.assertFalse(set(imports)&{'requests','flask','os','sqlite3','threading','urllib.request'})
        names={n.name for n in tree.body if isinstance(n,ast.FunctionDef)}
        self.assertFalse(any('watcher' in n or 'post_to_executor' in n or 'process_live' in n for n in names))

    def test_existing_audit_reconciles_to_17_unique_logged_signals(self):
        m=replay.verify_sources();known,events=replay.load_logs(replay.parse(m['start_utc']),replay.parse(m['end_utc']))
        self.assertEqual(len(known),17)
        self.assertTrue(all(x.get('qualified_logged') for x in known.values()))

class CausalityTests(unittest.TestCase):
    def setUp(self):
        self.t=replay.parse('2026-11-06T09:15:00Z')

    def test_raw_count_includes_forming_slot_but_never_future_prices(self):
        data={'M15':[bar(self.t+timedelta(minutes=15*i),1+i*.01) for i in range(-5,3)]}
        view=replay.CausalView(data);view.asof=self.t
        a=view.fetch('EUR_USD','M15',4,True)
        self.assertEqual(len(a),3)
        self.assertEqual(a[-1]['time'],self.t-timedelta(minutes=15))
        raw=view.fetch('EUR_USD','M15',4,False)
        self.assertTrue(math.isnan(raw[-1]['close']))
        self.assertEqual(raw[-1]['time'],self.t)
        self.assertFalse(raw[-1]['complete'])

    def test_completed_count_lane_has_one_more_completed_candle(self):
        view=replay.CausalView({'M15':[bar(self.t+timedelta(minutes=15*i)) for i in range(-5,2)]})
        view.asof=self.t;view.lane='COMPLETED_COUNT'
        self.assertEqual(len(view.fetch('EUR_USD','M15',4,True)),4)

    def test_htf_forming_and_future_ohlc_do_not_change_snapshot(self):
        asof=replay.parse('2026-11-06T09:15:00Z');signal=asof-timedelta(minutes=15)
        data={'H1':synthetic('H1',asof,asof,1000)}
        view=replay.CausalView(data);view.asof=asof
        before=engine.m15_htf_snapshot(view.fetch('EUR_USD','H1',1000,False),signal)
        for b in data['H1']:
            if replay.bar_end(b,'H1')>asof:b.update(open=5000,high=10000,low=1,close=9000)
        after=engine.m15_htf_snapshot(view.fetch('EUR_USD','H1',1000,False),signal)
        self.assertEqual(before,after)
        self.assertLessEqual(before['complete_at'],signal)
        self.assertTrue(math.isfinite(before['ema100']))

    def test_daily_close_boundary_tracks_dst(self):
        a=bar(replay.parse('2026-10-31T21:00:00Z'))
        self.assertEqual(replay.iso(replay.bar_end(a,'D')),'2026-11-01T22:00:00Z')

    def test_insufficient_warmup_fails(self):
        view=replay.CausalView({'M15':[bar(self.t)]});view.asof=self.t
        with self.assertRaisesRegex(ValueError,'Rolling history'):view.fetch('EUR_USD','M15',260)

    def test_incomplete_historical_candle_is_not_used_as_completed(self):
        bars=[bar(self.t+timedelta(minutes=15*i)) for i in range(-5,2)]
        bars[3]['complete']=False
        view=replay.CausalView({'M15':bars});view.asof=self.t
        with self.assertRaisesRegex(ValueError,'reported incomplete'):view.fetch('EUR_USD','M15',4)

    def test_merge_disagreement_is_not_silently_overwritten(self):
        a=bar(self.t);b=dict(a,close=1.1002)
        with self.assertRaisesRegex(ValueError,'disagree'):replay.merge_bars([[a],[b]])

    def test_native_missing_open_signal_bar_is_reported(self):
        start=replay.parse('2026-11-06T09:00:00Z');end=start+timedelta(minutes=30)
        data={'M15':[bar(start-timedelta(minutes=15)),bar(start+timedelta(minutes=15))]}
        gaps=replay.gaps_for('EUR_USD',data,start,end)
        self.assertTrue(any(x['kind']=='MISSING_EXPECTED_SIGNAL_BAR' and x['to_utc']=='2026-11-06T09:15:00Z' for x in gaps))

    def test_weekend_interval_is_not_a_native_data_gap(self):
        a=bar(replay.parse('2026-10-30T20:00:00Z'));b=bar(replay.parse('2026-11-01T22:00:00Z'))
        gaps=replay.gaps_for('EUR_USD',{'H1':[a,b]},a['time'],b['time'])
        self.assertFalse(any(x['kind']=='UNEXPLAINED_NATIVE_INTERVAL' for x in gaps))

class ComparisonTests(unittest.TestCase):
    def signal(self,lane,stop=1.0):
        return dict(lane=lane,signal_id='test-1',pair='EUR_USD',strategy_id='EUR_USD_H1_LONG',side='BUY',entry=1.1,stop=stop,rr=3.5)
    def test_unlogged_qualifier_is_not_called_a_guaranteed_missed_trade(self):
        rows=replay.compare([dict(signals=[self.signal(l) for l in replay.LANES])],{})
        self.assertEqual(rows[0]['classification'],'UNLOGGED_QUALIFICATION_REQUIRES_REVIEW')
    def test_window_sensitivity_is_explicit(self):
        rows=replay.compare([dict(signals=[self.signal(replay.LANES[0])])],{})
        self.assertEqual(rows[0]['classification'],'COUNT_WINDOW_SENSITIVE')
    def test_recorded_price_difference_is_flagged(self):
        known={'test-1':dict(self.signal(''),entry=1.10001,status='order_filled')}
        row=replay.compare([dict(signals=[self.signal(l) for l in replay.LANES])],known)[0]
        self.assertIn('entry',row['recorded_field_differences'])
    def test_logged_signal_not_reproduced_is_explicit(self):
        row=replay.compare([],{'test-1':self.signal('')})[0]
        self.assertEqual(row['classification'],'RECORDED_SIGNAL_NOT_REPRODUCED')

class DownloadTests(unittest.TestCase):
    def test_daily_native_request_boundaries_follow_dst(self):
        start=replay.parse('2026-10-07T08:30:00Z');end=replay.parse('2026-11-06T09:45:00Z')
        calls=[]
        class Fake(replay.ReadOnlyCandles):
            def query(self,pair,g,**bounds):
                calls.append(bounds)
                if 'count' in bounds:return [bar(replay.parse('2026-10-05T21:00:00Z')),bar(replay.parse('2026-10-06T21:00:00Z'))]
                return []
        with tempfile.TemporaryDirectory() as tmp:Fake(tmp).history('EUR_USD','D',start,end,1)
        for b in calls[1:-1]:self.assertEqual(replay.parse(b['to']).astimezone(replay.NY).hour,17)

    def test_only_candle_get_is_requested_and_cache_reused(self):
        t='2026-11-05T10:00:00Z'
        body=dict(instrument='EUR_USD',granularity='H1',candles=[dict(time=t,complete=True,mid=dict(o='1.1',h='1.2',l='1.0',c='1.15'))])
        class Response:
            def __enter__(self):return self
            def __exit__(self,*a):pass
            def read(self):return json.dumps(body).encode()
        calls=[]
        def opening(request,timeout):
            calls.append(request)
            self.assertEqual(request.get_method(),'GET')
            self.assertTrue(request.full_url.startswith('https://api-fxtrade.oanda.com/v3/instruments/EUR_USD/candles?'))
            self.assertNotIn('accounts',request.full_url)
            return Response()
        with tempfile.TemporaryDirectory() as tmp,patch.dict(os.environ,{'OANDA_TOKEN':'offline-fixture'}),patch.object(replay,'urlopen',opening):
            client=replay.ReadOnlyCandles(tmp)
            first=client.query('EUR_USD','H1',to=t,count=1)
            second=client.query('EUR_USD','H1',to=t,count=1)
            self.assertEqual(first,second);self.assertEqual(len(calls),1)
            cache=next(Path(tmp).glob('*.gz'));saved=json.loads(gzip.decompress(cache.read_bytes()))
            saved['body']['candles'][0]['mid']['c']='1.16';cache.write_bytes(gzip.compress(json.dumps(saved).encode()))
            with self.assertRaisesRegex(ValueError,'integrity'):client.query('EUR_USD','H1',to=t,count=1)

class PipelineTests(unittest.TestCase):
    def test_all_32_strategies_evaluate_in_both_lanes_on_offline_candles(self):
        start=replay.parse('2026-11-06T09:00:00Z');end=replay.parse('2026-11-06T09:15:00Z')
        registry=json.loads((replay.ROOT/'inputs/registry.json').read_text());seen={l:set() for l in replay.LANES}
        for pair in sorted({x['pair'] for x in registry}):
            streams={'H1':1000} if pair=='EUR_CHF' else {'H1':1000,'M15':2000 if pair=='AUD_JPY' else 260,'D':2500}
            if pair in {'USD_JPY','USD_CAD'}:streams['H4']=1000
            data={g:synthetic(g,start,end,n,pair) for g,n in streams.items()}
            result=replay.replay_pair(pair,data,start,end)
            for lane,counts in result['evaluations'].items():seen[lane].update(k for k,n in counts.items() if n>0)
            for signal in result['signals']:self.assertNotIn('secret',signal)
        self.assertTrue(all(len(x)==32 for x in seen.values()))

    def test_eurchf_anchor_overlap_mismatch_stops(self):
        start=replay.parse('2026-11-06T09:00:00Z');end=start+timedelta(minutes=15)
        data={'H1':synthetic('H1',start,end,1000,'EUR_CHF')}
        time=replay.parse(engine.EURCHF_FROZEN_ANCHOR['bars'][-1]['time'])
        next(x for x in data['H1'] if x['time']==time)['close']+=.00001
        with self.assertRaisesRegex(ValueError,'overlap differs'):replay.eurchf_replay(data,start,end,replay.LANES[0])

    def test_worker_resume_and_small_result_archive_without_any_broker(self):
        start=replay.parse('2026-11-06T09:00:00Z');end=start+timedelta(minutes=15);calls=[]
        class FakeClient:
            def __init__(self,*a):pass
            def history(self,pair,g,a,b,n):calls.append((pair,g));return synthetic(g,a,b,n,pair)
        with tempfile.TemporaryDirectory() as tmp:
            run=replay.AuditRun(tmp,FakeClient);run.start=start;run.end=end;run.worker()
            self.assertEqual(run.state['status'],'COMPLETE',run.state.get('error'))
            self.assertEqual(run.state['pairs_completed'],9)
            count=len(calls)
            with zipfile.ZipFile(run.result_path) as z:
                self.assertIn('comparison.csv',z.namelist());self.assertIsNone(z.testzip())
            self.assertLess(run.result_path.stat().st_size,24*1024*1024)
            run2=replay.AuditRun(tmp,FakeClient);run2.start=start;run2.end=end;run2.worker()
            self.assertEqual(run2.state['status'],'COMPLETE');self.assertEqual(len(calls),count)
            cp=next(run.directory.glob('AUD_USD.json'));saved=json.loads(cp.read_text())
            saved['result']['signals'].append({'tampered':True});cp.write_text(json.dumps(saved))
            run3=replay.AuditRun(tmp,FakeClient);run3.start=start;run3.end=end;run3.worker()
            self.assertEqual(run3.state['status'],'ERROR')
            self.assertIn('integrity',run3.state['error'])

class ServerTests(unittest.TestCase):
    def test_info_does_not_start_worker_and_order_posts_are_unavailable(self):
        from http.server import ThreadingHTTPServer
        from urllib.request import urlopen,Request
        from urllib.error import HTTPError
        class FakeRun:
            def __init__(self):
                self.state=dict(start_utc='2026-10-07T08:30:00Z',end_utc='2026-11-06T09:45:00Z',persistence={},status='NOT_STARTED')
                self.lock=threading.Lock();self.thread=None;self.started=0
            def start_once(self):self.started+=1
        class Handler(server_app.Handler):run=FakeRun()
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        base='http://127.0.0.1:'+str(server.server_port)
        try:
            with urlopen(base+'/') as response:body=json.load(response)
            self.assertFalse(body['orders_supported']);self.assertEqual(Handler.run.started,0)
            with urlopen(base+'/status') as response:body=json.load(response)
            self.assertEqual(Handler.run.started,1)
            with self.assertRaises(HTTPError) as caught:urlopen(Request(base+'/webhook',data=b'{}',method='POST'))
            self.assertEqual(caught.exception.code,405)
        finally:server.shutdown();server.server_close();thread.join()

if __name__=='__main__':unittest.main(verbosity=2)
