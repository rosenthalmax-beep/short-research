"""Offline behavioural checks; no credentials or broker access."""
import copy
import gzip
import json
import math
import random
import sqlite3
import tempfile
import threading
import unittest
import zipfile
from array import array
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import urlsplit
from pullback_lab import core as e
from pullback_lab.accounting import Replay, converted
from pullback_lab.broker_reconcile import reconcile
from pullback_lab.funding import Funding, rollovers
from pullback_lab.market import Market
from pullback_lab.paths import BarrierIndex, Paths, shadow_gbp, truncate
from pullback_lab.reports import stats, windows
from pullback_lab.runner import Research, ROOT
from pullback_lab.signals import exact_engulf, generate, gap_rows
from pullback_lab.storage import Bars, Store, BROKER, WINDOW_SECONDS, validate
from pullback_app import application

T=e.dt('2020-03-02T12:00:00Z')


def candle(t,o=1.1,h=1.1005,l=1.0995,c=1.1,spread=0.):
    mid=dict(o=o,h=h,l=l,c=c)
    return dict(time=e.stamp(t),complete=True,mid=mid,
                bid={k:v-spread/2 for k,v in mid.items()},ask={k:v+spread/2 for k,v in mid.items()})


def bars(rows):
    result=Bars()
    for row in rows:result.append(row)
    return result


def signal(key='a',side='BUY',when=T,sid=None,pair='GBP_USD'):
    return dict(signal_key=key,strategy_id=sid or key,pair=pair,side=side,timeframe='M15',
                entry_utc=e.stamp(when),signal_start_utc=e.stamp(when-timedelta(minutes=15)),
                signal_close_utc=e.stamp(when),same_strategy_trigger_priority=0,control='TREND_PULLBACK_ENGULF',
                reference_entry=1.1,stop=1.099 if side=='BUY' else 1.101,rr=3.)


def schedules():
    return {p:dict(days=dict(zip(e.DAYS,(1,1,3,1,1,0,0))),long_rate=-.04,short_rate=.02) for p in e.PAIRS}


def instruments():
    return [dict(name=p,marginRate='0.033333333333',minimumTradeSize='1',maximumOrderUnits='100000000',tradeUnitsPrecision=0) for p in e.PAIRS]


class FixedMarket:
    current_stale=False;current_max_age=0
    def begin_event(self):pass
    def quote(self,pair,when):return dict(bid=1.1,ask=1.1,mid=1.1)
    def fx_mid(self,currency,when):return 1. if currency=='GBP' else 1/1.1


def price_path(s,finish=T+timedelta(hours=1),exit_price=1.103):
    return dict(status='CLOSED_RESOLVED',kind='TARGET',model_fill=1.1,model_stop=s['stop'],
                model_target=1.103,model_price_bound=1.10015 if s['side']=='BUY' else 1.09985,
                model_bid=1.1,model_ask=1.1,exit_price=exit_price,
                interval_start_utc=e.stamp(finish-timedelta(minutes=1)),interval_end_utc=e.stamp(finish),
                certainty='RESOLVED_TO_CANDLE_INTERVAL')


class FineStore:
    def __init__(self,rows=None):self.rows=rows or {};self.calls=[]
    def range(self,pair,gran,start,end):
        self.calls.append((pair,gran,start,end))
        return [c for c in self.rows.get(gran,[]) if start<=e.dt(c['time'])<end]


class GeometryTests(unittest.TestCase):
    def test_exact_pattern_inclusive_body_edges_and_direction(self):
        prev=dict(o=1.1,c=1.099);now=dict(o=1.099,c=1.1)
        self.assertTrue(exact_engulf(prev,now,'BUY'))
        self.assertFalse(exact_engulf(prev,dict(o=1.09901,c=1.1),'BUY'))
        self.assertTrue(exact_engulf(now,prev,'SELL'))
        self.assertFalse(exact_engulf(dict(o=1.,c=1.),now,'BUY'))

    def test_15_tick_boundary_both_directions(self):
        for pair,ref,stop in (('EUR_USD',1.1,1.099),('USD_JPY',150.,149.9)):
            tick=float(e.tick(pair))
            for side in ('BUY','SELL'):
                sign=1 if side=='BUY' else -1;sl=ref-sign*abs(ref-stop)
                for n,expected in ((15,'ADMITTED_ENTRY_PROXY'),(16,'REJECTED_15T_ENTRY_PROXY')):
                    p=ref+sign*n*tick;bid,ask=(p-2*tick,p) if sign>0 else (p,p+2*tick)
                    g=e.entry_preview(pair,side,ref,sl,3,bid,ask,15)
                    self.assertEqual(g['status'],expected)
                    self.assertAlmostEqual(float(g['target']),ref+sign*3*abs(ref-sl),places=e.digits(pair))

    def test_stop_gap_exceeds_one_r_and_bid_ask_exit_sides(self):
        row=candle(T,1.098,1.099,1.097,1.098,spread=.0002)
        hit=e.touch(row,'bid','BUY',e.dec('1.099'),e.dec('1.103'))
        self.assertLess(hit['exit_price'],1.099);self.assertTrue(hit['open_gap'])
        row=candle(T,1.1,1.1009,1.0999,1.1002,spread=.0004)
        self.assertIsNone(e.touch(row,'bid','SELL',e.dec('1.101'),e.dec('1.097')))
        self.assertEqual(e.touch(row,'ask','SELL',e.dec('1.101'),e.dec('1.097'))['kind'],'STOP')

    def test_tree_matches_brute_force_for_random_barriers(self):
        rng=random.Random(18);lo=[rng.uniform(.9,1.1) for _ in range(255)];hi=[x+rng.uniform(0,.1) for x in lo]
        tree=BarrierIndex(lo,hi)
        for _ in range(500):
            start=rng.randrange(255);end=rng.randrange(start+1,256);lower=rng.uniform(.85,1.1);upper=rng.uniform(1.1,1.3)
            expected=next((i for i in range(start,end) if lo[i]<=lower or hi[i]>=upper),None)
            self.assertEqual(tree.first(start,end,lower,upper),expected)


class SignalTests(unittest.TestCase):
    def fixture(self):
        h=bars([candle(T-timedelta(hours=110-i),200,201,199,200) for i in range(130)])
        rows=[candle(T+timedelta(minutes=15*i),101.,101.5,100.8,101.2) for i in range(15)]
        rows[2]=candle(T+timedelta(minutes=30),100.8,101.,99.,99.8)
        rows[3]=candle(T+timedelta(minutes=45),99.7,101.2,99.5,101.)
        rows[4]=candle(T+timedelta(minutes=60),100.8,101.,99.5,99.8)
        rows[5]=candle(T+timedelta(minutes=75),99.7,101.2,99.5,101.)
        return bars(rows),h

    def fake_ema(self,values,n):
        return array('d',[100.]*len(values)) if n==20 else array('d',[1.+i*.1 for i in range(len(values))])

    def run_fixture(self,m,h,end):
        with patch('pullback_lab.signals.ema',side_effect=self.fake_ema),patch('pullback_lab.signals.atr',side_effect=lambda b:array('d',[1.]*len(b))):
            return generate('GBP_USD',m,h,T,end)[0]

    def test_first_trigger_consumes_pullback_and_stop_contains_earlier_low(self):
        m,h=self.fixture();signals=self.run_fixture(m,h,T+timedelta(hours=2))
        pb=[s for s in signals if s['control']=='TREND_PULLBACK_ENGULF' and s['side']=='BUY']
        self.assertEqual(len(pb),1)
        self.assertEqual(pb[0]['entry_utc'],e.stamp(T+timedelta(minutes=60)))
        self.assertEqual(pb[0]['stop'],98.9999)
        self.assertEqual(pb[0]['features']['duration_bars'],2)

    def test_h1_never_uses_future_hour_and_prefix_has_identical_signals(self):
        m,h=self.fixture();cut=T+timedelta(hours=2)
        full=self.run_fixture(m,h,T+timedelta(hours=3))
        for s in full:
            if s['features']['h1_source_close_utc']:
                self.assertLessEqual(s['features']['h1_source_close_utc'],s['entry_utc'])
        altered=copy.deepcopy(h)
        for i,t in enumerate(altered.times):
            if t>=cut.timestamp():
                for k in 'ohlc':altered.values['mid'][k][i]=1000000.
        a=[s for s in full if s['entry_utc']<e.stamp(cut)]
        b=self.run_fixture(m,altered,cut)
        self.assertEqual(a,b)

    def test_missing_m15_break_resets_setup(self):
        m,h=self.fixture();m.times[3]+=900
        # Drop the following duplicate and all later bars; the confirmation is
        # now after an unexplained missing interval and must not qualify.
        shortened=Bars()
        for i in (0,1,2,3):shortened.append(m.row(i))
        got=self.run_fixture(shortened,h,T+timedelta(hours=2))
        self.assertFalse(any(s['control']=='TREND_PULLBACK_ENGULF' for s in got))

    def test_expired_pullback_does_not_accept_late_engulf(self):
        m,h=self.fixture()
        rows=[m.row(i) for i in range(len(m))]
        for i in range(3,11):rows[i]=candle(T+timedelta(minutes=i*15),99.9,100.2,99.5,99.8)
        rows[11]=candle(T+timedelta(minutes=165),99.7,101.2,99.5,101.)
        got=self.run_fixture(bars(rows),h,T+timedelta(hours=4))
        self.assertFalse(any(s['control']=='TREND_PULLBACK_ENGULF' for s in got))


class PathTests(unittest.TestCase):
    def test_missing_native_interval_qualifies_otherwise_observed_exit(self):
        hit=T+timedelta(minutes=30)
        native=bars([candle(T),candle(hit,1.1,1.104,1.0995,1.103)])
        finer=[candle(hit+timedelta(minutes=i),1.1,1.104,1.0995,1.103) for i in range(15)]
        with tempfile.TemporaryDirectory() as d:
            p=Paths(d,FineStore({'M1':finer}),native,'GBP_USD',T+timedelta(minutes=45),'test')
            result=p.get(signal())
            self.assertEqual(result['status'],'UNRESOLVED_PATH')
            self.assertGreater(result['prior_native_unexplained_slots'],0)
            p.close()

    def test_missing_exact_entry_is_not_replaced_by_later_quote(self):
        with tempfile.TemporaryDirectory() as d:
            p=Paths(d,FineStore(),bars([candle(T+timedelta(minutes=15))]),'GBP_USD',T+timedelta(minutes=30),'test')
            self.assertEqual(p.get(signal())['status'],'UNRESOLVED_ENTRY');p.close()

    def test_candle_interval_truncation_withholds_later_exit(self):
        p=price_path(signal())
        q=truncate(p,T+timedelta(minutes=59,seconds=30))
        self.assertEqual(q['kind'],'OPEN_AT_FIXED_CUTOFF');self.assertIsNone(q['exit_price'])
        self.assertEqual(truncate(p,T+timedelta(hours=1))['kind'],'TARGET')

    def test_refinement_preserves_s5_double_touch_as_unresolved(self):
        parent=candle(T,1.1,1.104,1.098,1.1)
        store=FineStore({'M1':[parent],'S5':[parent]})
        out=e.refine(store,'GBP_USD','BUY','bid',e.dec(1.099),e.dec(1.103),parent,'M15')
        self.assertEqual(out['kind'],'AMBIGUOUS_BOTH');self.assertIsNone(out['exit_price'])
        self.assertEqual(out['resolution'],'S5')

    def test_guard_rejection_keeps_counterfactual_separate(self):
        row=candle(T,1.10016,1.1002,1.1001,1.10016)
        with tempfile.TemporaryDirectory() as d:
            p=Paths(d,FineStore(),bars([row]),'GBP_USD',T+timedelta(minutes=15),'test')
            out=p.get(signal());self.assertEqual(out['status'],'ENTRY_PROXY_REJECTED')
            self.assertIn('counterfactual_outcome_status',out)
            self.assertEqual(shadow_gbp(signal(),out,None,None,None,.01,T)['shadow_status'],'ENTRY_PROXY_REJECTED');p.close()

    def test_cache_tampering_fails(self):
        with tempfile.TemporaryDirectory() as d:
            p=Paths(d,FineStore(),bars([candle(T)]),'GBP_USD',T+timedelta(minutes=15),'test')
            p.get(signal());p.memo.clear()
            p.db.execute("UPDATE paths SET body=?",(b'{}',));p.db.commit()
            with self.assertRaisesRegex(ValueError,'checksum'):p.get(signal())
            p.close()


class AccountTests(unittest.TestCase):
    def run_account(self,signals,paths,mode='ZERO_FINANCING_CONTROL',end=T+timedelta(hours=2),funding=None):
        return Replay(signals,lambda s:paths[s['signal_key']],FixedMarket(),funding or Funding(schedules()),
                      instruments(),'END_ASC',mode,.01,start=T,end=end,keep_nav=True).run()

    def test_same_pair_opposite_direction_and_same_strategy_are_blocked(self):
        a=signal('a');b=signal('b','SELL',T+timedelta(minutes=15));c=signal('c',when=T+timedelta(minutes=30),sid='a')
        r=self.run_account([a,b,c],{s['signal_key']:price_path(s) for s in (a,b,c)})
        self.assertEqual([x['decision'] for x in r['ledger']],['ADMITTED','BLOCKED','BLOCKED'])
        self.assertEqual(r['ledger'][1]['reason'],'OPPOSITE_PAIR_NONHEDGING')
        self.assertEqual(r['ledger'][2]['reason'],'STRATEGY_PYRAMIDING_0')

    def test_same_direction_distinct_strategies_can_overlap(self):
        a,b=signal('a'),signal('b',when=T+timedelta(minutes=15))
        r=self.run_account([a,b],{'a':price_path(a),'b':price_path(b)})
        self.assertEqual(r['summary']['decisions'],{'ADMITTED':2});self.assertEqual(r['summary']['max_positions'],2)

    def test_worst_bound_floor_and_cash_identity(self):
        a=signal();r=self.run_account([a],{'a':price_path(a)})
        units=r['ledger'][0]['units'];expected=math.floor(1./(.00115*(1/1.1)*1.01))
        self.assertEqual(units,expected)
        self.assertLessEqual(r['ledger'][0]['sizing_risk_gbp'],1.)
        self.assertAlmostEqual(r['summary']['ending_balance_gbp'],100+r['summary']['price_pl_gbp']+r['summary']['financing_gbp'],places=4)

    def test_exit_before_rollover_avoids_charge_at_same_instant(self):
        boundary=e.dt('2020-03-02T22:00:00Z');a=signal()
        r=self.run_account([a],{'a':price_path(a,finish=boundary)},mode='ADVERSE_6PCT_ANNUAL',end=boundary+timedelta(hours=1))
        self.assertEqual(r['summary']['financing_gbp'],0)

    def test_funding_charges_base_units_and_preserves_three_day_multiplier(self):
        a=signal();end=e.dt('2020-03-05T23:00:00Z')
        r=self.run_account([a],{'a':price_path(a,finish=end)},mode='ADVERSE_6PCT_ANNUAL',end=end)
        wed=[x for x in r['financing'] if x['ny_date']=='2020-03-04'][0]
        self.assertEqual(wed['charged_days'],3)
        self.assertAlmostEqual(wed['base_financing'],wed['units']*(-.06)*3/365)
        self.assertAlmostEqual(wed['base_to_gbp_mid'],1.)

    def test_saved_broker_evidence_reconciles(self):
        summary,rows=reconcile(json.loads((ROOT/'saved_broker_evidence.json').read_bytes()))
        self.assertEqual(summary['status'],'PASS');self.assertEqual(len(rows),82)
        self.assertAlmostEqual(summary['daily_financing_gbp'],-.2807)
        self.assertAlmostEqual(summary['observed_symmetric_conversion_factor_markup'],.01)

    def test_feature_denominators_do_not_turn_missing_or_rejects_into_losses(self):
        r=stats([dict(path_status='UNRESOLVED_ENTRY',shadow={'shadow_status':'UNRESOLVED_ENTRY'}),
                 dict(path_status='ENTRY_PROXY_REJECTED',shadow={'shadow_status':'ENTRY_PROXY_REJECTED'},
                      counterfactual_outcome_status='CLOSED_RESOLVED',counterfactual_price_r=-1)])
        self.assertEqual(r['qualifications'],2);self.assertEqual(r['preview_rejections'],1)
        self.assertIsNone(r['resolved_only_frozen_net_win_rate_pct']);self.assertEqual(r['rejected_counterfactual_price_win_rate_pct'],0)


class StorageAndServiceTests(unittest.TestCase):
    def test_partial_last_hour_is_not_reported_as_missing_completed_hour(self):
        b=bars([candle(T)])
        gaps=gap_rows(b,3600,'GBP_USD','H1',T,T+timedelta(hours=1,minutes=15))
        self.assertEqual(sum(r['unexplained_slots'] for r in gaps),0)

    def test_negative_spread_and_incomplete_candles(self):
        ident=Store.identity('GBP_USD','M15',T,T+timedelta(minutes=15))
        row=candle(T);row['complete']=False
        obj=dict(instrument='GBP_USD',granularity='M15',candles=[row])
        self.assertEqual(validate(obj,ident),[])
        row['complete']=True;row['ask']={k:v-.0001 for k,v in row['bid'].items()}
        with self.assertRaisesRegex(ValueError,'spread'):validate(obj,ident)

    def test_exact_legacy_reuse_is_read_only_and_checksum_checked(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);legacy=root/'legacy';legacy.mkdir()
            start=e.floor_time(T,14*86400);end=start+timedelta(days=14)
            ident=Store.identity('GBP_USD','M15',start,end);raw=e.canon(dict(instrument='GBP_USD',granularity='M15',candles=[candle(T)]))
            db=sqlite3.connect(legacy/'replay_candles.sqlite3');db.execute('CREATE TABLE blocks(key,identity,body,sha256,origin)')
            db.execute('INSERT INTO blocks VALUES(?,?,?,?,?)',(e.sha(e.canon(ident)),e.canon(ident).decode(),__import__('zlib').compress(raw),e.sha(raw),'TEST'));db.commit();db.close()
            before=(legacy/'replay_candles.sqlite3').read_bytes()
            store=Store(root/'new',legacy_root=legacy)
            self.assertEqual(len(store.exact('GBP_USD','M15',start,end)),1)
            self.assertEqual(store.reused,1);self.assertEqual(store.downloads,0);store.close()
            self.assertEqual(before,(legacy/'replay_candles.sqlite3').read_bytes())

    def test_reader_only_requests_allowlisted_candle_get_and_sanitizes_auth_error(self):
        observed=[]
        def transport(req):
            observed.append(req)
            raise HTTPError(req.full_url,401,'secret-should-not-leak',{},None)
        reader=e.CandleReader('offline-test-placeholder',BROKER,transport=transport)
        ident=Store.identity('GBP_USD','M15',T,T+timedelta(minutes=15))
        with self.assertRaises(e.CandleFetchError) as ctx:reader.get(ident)
        self.assertEqual(str(ctx.exception),'Candle GET failed: HTTP 401')
        self.assertEqual(observed[0].method,'GET')
        self.assertEqual(urlsplit(observed[0].full_url).path,'/v3/instruments/GBP_USD/candles')
        self.assertTrue(reader.fatal)
        bad=dict(ident,pair='UNAPPROVED')
        with self.assertRaises(ValueError):reader.get(bad)

    def test_head_and_health_never_start_work_and_post_is_rejected(self):
        class Stub:
            signature='test';protocol={};directory=Path('/tmp');work=Path('/not-a-directory')
            def __init__(self):self.calls=[]
            def status(self,start=False):self.calls.append(start);return {'status':'IDLE'}
        stub=Stub();app=application(stub)
        for method,path in (('HEAD','/status'),('GET','/health'),('POST','/status')):
            status=[];list(app({'REQUEST_METHOD':method,'PATH_INFO':path},lambda s,h:status.append(s)))
            if method=='POST':self.assertTrue(status[0].startswith('405'))
        self.assertEqual(stub.calls,[False])

    def test_repeated_status_starts_one_worker(self):
        with tempfile.TemporaryDirectory() as d:
            job=Research(output=d,environ={'PULLBACK_REQUIRE_PERSISTENCE':'0'})
            event=threading.Event();calls=[]
            def worker():calls.append(1);event.wait(2)
            job.run=worker
            job.status(start=True);job.status(start=True);event.set();job.thread.join()
            self.assertEqual(calls,[1])


if __name__=='__main__':unittest.main()
