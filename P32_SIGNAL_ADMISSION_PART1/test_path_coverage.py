"""Synthetic evidence tests; never strategy performance evidence."""
import copy
import gzip
import json
import tempfile
import unittest
import zipfile
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch
from urllib.request import Request
import audusd_path_coverage as a


def candle(t, o=1.2, h=1.2, l=1.2, c=1.2):
    h=max(h,o,c);l=min(l,o,c)
    def prices(add):return {k:format(v+add,'.5f') for k,v in dict(o=o,h=h,l=l,c=c).items()}
    return dict(time=a.e.stamp(t),complete=True,volume=1,bid=prices(0),ask=prices(.0001),mid=prices(.00005))


def example(resolution='M1', date='2021-01-04T10:00:00Z'):
    t=a.e.dt(date);minutes=[];fine=[]
    for i in range(15):
        h=1.201 if i==2 else 1.2;l=1.198 if i==5 else 1.2;c=1.1995 if i in (5,14) else 1.2
        minute=candle(t+timedelta(minutes=i),h=h,l=l,c=c);minutes.append(minute)
        for j in range(12):
            fine.append(candle(t+timedelta(minutes=i,seconds=j*5),h=h if j==7 else 1.2,
                l=l if j==7 else 1.2,c=c if j==11 else 1.2))
    native=[candle(t,h=1.201,l=1.198,c=1.1995)]
    case=dict(signal=dict(signal_key='CASE',entry_utc=a.e.stamp(t),pair='AUD_USD',side='BUY',rr=3),
        path=dict(status='UNRESOLVED_PATH',native_interval_start_utc=a.e.stamp(t),model_stop=1.199,
            model_target=1.203,model_ask=1.2001,model_bid=1.2,model_fill=1.2001,kind='STOP',
            certainty='TOUCH_WITH_EARLIER_UNEXPLAINED_GAP',resolution=resolution,
            exit_price=1.199,interval_start_utc=a.e.stamp(t+timedelta(minutes=5)),
            interval_end_utc=a.e.stamp(t+timedelta(minutes=6)),prior_native_unexplained_slots=0))
    return case,native,minutes,fine


class CoverageTests(unittest.TestCase):
    def test_complete_s5_resolves_interval(self):
        c,n,m,s=example();r=a.analyse_case(c,n,m,lambda t:s)
        self.assertEqual(r['verdict'],'RESOLVED_TO_S5_INTERVAL')
        self.assertEqual(r['new_observed_touch']['interval_start_utc'],'2021-01-04T10:05:35Z')
        self.assertFalse(r['source_conflicts'])
    def test_complete_m1_nonrollover_fallback(self):
        c,n,m,s=example();r=a.analyse_case(c,n,m,lambda t:[])
        self.assertEqual(r['verdict'],'RESOLVED_TO_M1_INTERVAL')
    def test_missing_critical_minute_is_not_forward_filled(self):
        c,n,m,s=example();m=m[1:];s=s[12:]
        r=a.analyse_case(c,n,m,lambda t:s)
        self.assertEqual(r['verdict'],'UNRESOLVED_COVERAGE')
        self.assertEqual(r['critical_missing_s5_slots'],12)
        self.assertEqual(r['coarse_native_outcome_only']['kind'],'STOP')
    def test_s5_can_fill_missing_critical_minute(self):
        c,n,m,s=example();r=a.analyse_case(c,n,m[1:],lambda t:s)
        self.assertEqual(r['verdict'],'RESOLVED_TO_S5_INTERVAL')
    def test_original_s5_does_not_fall_back_to_m1(self):
        c,n,m,s=example('S5');r=a.analyse_case(c,n,m,lambda t:[])
        self.assertFalse(r['resolved_to_candle_interval'])
    def test_rollover_does_not_fall_back_to_m1(self):
        c,n,m,s=example(date='2021-01-04T22:00:00Z');r=a.analyse_case(c,n,m,lambda t:[])
        self.assertFalse(r['resolved_to_candle_interval'])
    def test_tier_revision_keeps_path_unresolved(self):
        c,n,m,s=example();s[0]['bid']['h']='1.21000'
        r=a.analyse_case(c,n,m,lambda t:s)
        self.assertEqual(r['verdict'],'UNRESOLVED_SOURCE_CONFLICT')
    def test_one_tick_barrier_contradiction_is_caught(self):
        t=a.e.dt('2021-01-04T10:00:00Z');p=candle(t,h=1.20299);s=[candle(t,h=1.203)]
        conflicts=[];a.tier_check(p,s,12,1.199,1.203,'TEST',conflicts)
        self.assertTrue(any(x['type']=='TEST_BARRIER_CONTRADICTION' for x in conflicts))
    def test_ambiguous_s5_order_stays_unknown(self):
        c,n,m,s=example('S5');n[0]=candle(a.e.dt(n[0]['time']),h=1.204,l=1.198,c=1.1995)
        m[5]=candle(a.e.dt(m[5]['time']),h=1.204,l=1.198,c=1.1995)
        s[5*12+7]=candle(a.e.dt(s[5*12+7]['time']),h=1.204,l=1.198)
        r=a.analyse_case(c,n,m,lambda t:s)
        self.assertEqual(r['verdict'],'UNRESOLVED_INTRABAR_ORDER')
    def test_changed_entry_quote_refused(self):
        c,n,m,s=example();c['path']['model_ask']=1.2002
        with self.assertRaises(a.EvidenceConflict):a.analyse_case(c,n,m,lambda t:s)
    def test_native_nontouch_proof_skips_missing_children(self):
        c,n,m,s=example();t=a.e.dt(c['signal']['entry_utc']);prior=t-timedelta(minutes=15)
        c['signal']['entry_utc']=a.e.stamp(prior);n=[candle(prior)]+n
        r=a.analyse_case(c,n,m,lambda q:s if q==t else [])
        self.assertEqual(r['verdict'],'RESOLVED_TO_S5_INTERVAL')
        self.assertEqual(r['frozen_native_non_touch_parents_used'],1)
    def test_missing_native_prefix_not_assumed_safe(self):
        c,n,m,s=example();t=a.e.dt(c['signal']['entry_utc']);prior=t-timedelta(minutes=30)
        c['signal']['entry_utc']=a.e.stamp(prior);n=[candle(prior)]+n
        r=a.analyse_case(c,n,m,lambda q:s if q==t else [])
        self.assertEqual(r['verdict'],'UNRESOLVED_COVERAGE')
        self.assertIsNone(r['coarse_native_outcome_only'])
    def test_fine_requests_capped_and_old_exit_parent_has_priority(self):
        c,n,m,s=example();old=a.e.dt(c['signal']['entry_utc']);lo=old-timedelta(hours=3)
        c['signal']['entry_utc']=a.e.stamp(lo);n=[candle(lo)]+n;requested=[]
        def get(t):requested.append(t);return []
        r=a.analyse_case(c,n,m,get)
        self.assertEqual(requested[0],old)
        self.assertEqual(len(requested),a.MAX_S5_PARENTS)
        self.assertGreater(r['s5_parents_not_requested'],0)
        self.assertFalse(r['resolved_to_candle_interval'])


def packet_fixture():
    case,native,minutes,fine=example()
    lo=a.e.floor_time(a.e.dt(case['signal']['entry_utc']),14*86400)
    ident=a.identity('M15',lo,lo+timedelta(days=14))
    raw=a.canonical_rows(native,ident)
    cases=[]
    for i in range(25):
        c=copy.deepcopy(case);c['signal']['signal_key']='SYNTHETIC_'+str(i);cases.append(c)
    packet=dict(candidate_id='SYNTHETIC_ONLY',matrix_signature='TEST',cases=cases,
                native_windows=[dict(identity=ident,key=a.e.sha(a.e.canon(ident)),sha256=a.e.sha(raw))])
    def transport(request):
        gran=a.parse_qs(a.urlsplit(request.full_url).query)['granularity'][0]
        rows={'M15':native,'M1':minutes,'S5':fine}[gran]
        return dict(instrument='AUD_USD',granularity=gran,candles=rows)
    return packet,transport


class CacheAndServiceTests(unittest.TestCase):
    def setUp(self):self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
    def tearDown(self):self.tmp.cleanup()
    def cache(self):
        packet,transport=packet_fixture()
        return a.CandleCache(self.root/'cache',packet,'SECRET_TEST_TOKEN',transport=transport)
    def test_native_revision_is_refused_and_not_cached(self):
        cache=self.cache();w=cache.packet['native_windows'][0]
        with self.assertRaises(a.EvidenceConflict):cache.get(w['identity'],'0'*64)
        self.assertFalse(cache.state['blocks']);cache.close()
    def test_original_native_cache_is_read_only(self):
        packet,_=packet_fixture();w=packet['native_windows'][0];c,n,m,s=example()
        raw=a.canonical_rows(n,w['identity']);p=self.root/'original.sqlite3'
        with a.sqlite3.connect(p) as con:
            con.execute('CREATE TABLE blocks(key TEXT PRIMARY KEY,identity TEXT,body BLOB,sha256 TEXT,origin TEXT)')
            con.execute('INSERT INTO blocks VALUES(?,?,?,?,?)',(w['key'],a.e.canon(w['identity']).decode(),a.zlib.compress(raw),w['sha256'],'ORIGINAL'))
        before=a.sha_file(p)
        cache=a.CandleCache(self.root/'cache',packet,old_paths=[p],transport=lambda r:(_ for _ in ()).throw(AssertionError('Native download unexpected')))
        self.assertEqual(cache.get(w['identity'],w['sha256']),n)
        cache.close();self.assertEqual(a.sha_file(p),before)
        self.assertFalse(Path(str(p)+'-wal').exists())
    def test_cache_restart_requires_exact_body_hash(self):
        cache=self.cache();w=cache.packet['native_windows'][0];cache.get(w['identity'],w['sha256']);cache.close()
        next((self.root/'cache/candles').glob('*.json.gz')).write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError,'cache changed'):self.cache()
    def test_request_budget_persists(self):
        cache=self.cache();cache.state['requests']=a.MAX_REQUESTS;cache.save();cache.close()
        cache=self.cache()
        with self.assertRaises(a.BudgetStop):cache.get(cache.packet['native_windows'][0]['identity'])
        self.assertEqual(cache.state['requests'],a.MAX_REQUESTS);cache.close()
    def test_http_allowlist_blocks_order_endpoint(self):
        cache=self.cache()
        with self.assertRaisesRegex(ValueError,'historical candle GET'):
            cache.http(Request('https://api-fxtrade.oanda.com/v3/accounts/123/orders',method='POST'))
        self.assertEqual(cache.state['attempts'],0);cache.close()
    def test_http_attempt_budget_prevents_transport(self):
        cache=self.cache();cache.state['attempts']=a.MAX_ATTEMPTS
        ident=cache.packet['native_windows'][0]['identity']
        from urllib.parse import urlencode
        q=dict(granularity='M15',price='MBA',smooth='false',includeFirst='true',**{'from':ident['start'],'to':ident['end']})
        with self.assertRaises(a.BudgetStop):cache.http(Request(a.BROKER+'/v3/instruments/AUD_USD/candles?'+urlencode(q)))
        cache.close()
    def test_full_25_case_report_and_resume_without_fetching(self):
        packet,transport=packet_fixture();actual_cache=a.CandleCache
        def factory(*args,**kwargs):return actual_cache(*args,**kwargs,transport=transport)
        env=dict(AUDUSD_COVERAGE_OUTPUT_DIR=str(self.root/'output'),OANDA_TOKEN='SECRET_TEST_TOKEN')
        with patch.object(a,'CandleCache',factory),patch('socket.create_connection',side_effect=AssertionError('Live network forbidden')):
            job=a.Research(env,packet,{});job.run()
        self.assertEqual(job.state['status'],'COMPLETE',job.state)
        self.assertEqual(job.state['cases_completed'],25)
        self.assertEqual(job.state['resolved_cases'],25)
        with zipfile.ZipFile(job.work/'AUDUSD_PATH_COVERAGE_RESULTS.zip') as z:
            self.assertIsNone(z.testzip())
            manifest=json.loads(z.read('report_manifest.json'))
            for n,w in manifest.items():self.assertEqual(a.e.sha(z.read(n)),w['sha256'])
            self.assertNotIn(b'SECRET_TEST_TOKEN',b''.join(z.read(n) for n in z.namelist()))
        with patch.object(a.CandleCache,'get',side_effect=AssertionError('Completed cases fetched again')):
            again=a.Research(env,packet,{});again.run()
        self.assertEqual(again.state['status'],'COMPLETE',again.state)
    def test_health_head_post_never_start_research(self):
        packet,_=packet_fixture();job=a.Research(dict(AUDUSD_COVERAGE_OUTPUT_DIR=str(self.root/'out')),packet,{})
        app=a.application(job)
        for method,path in [('GET','/health'),('HEAD','/status'),('POST','/status')]:
            response=[];body=b''.join(app(dict(REQUEST_METHOD=method,PATH_INFO=path),lambda s,h:response.append(s)))
            self.assertIsNone(job.thread)
            if method=='POST':self.assertEqual(response[0],'405 Method Not Allowed')
    def test_existing_research_output_cannot_be_reused(self):
        packet,_=packet_fixture()
        with self.assertRaisesRegex(ValueError,'separate'):
            a.Research(dict(AUDUSD_COVERAGE_OUTPUT_DIR='/data/p32_audusd_long_matrix/new'),packet,{})
    def test_packaged_real_case_plan_is_pinned_and_bounded(self):
        packet,pins=a.load_packet()
        self.assertEqual(len(packet['cases']),25)
        keys=set()
        for c in packet['cases']:keys.update(a.e.sha(a.e.canon(i)) for i in a.m1_plan(c))
        self.assertLessEqual(len(keys)+len(packet['native_windows'])+25*a.MAX_S5_PARENTS,a.MAX_REQUESTS)


if __name__=='__main__':unittest.main()
