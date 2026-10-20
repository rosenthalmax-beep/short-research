"""End-to-end deterministic synthetic run, including real signals and GBP replay."""
import gzip
import json
import math
import tempfile
import unittest
import zipfile
from collections import OrderedDict
from pathlib import Path
from pullback_lab import core as e
from pullback_lab.runner import Research

EPOCH=e.dt('2020-01-01T00:00:00Z').timestamp()
OFFSETS=(0.,.0001,.0002,.0003,.0004,.0005,.0006,.0007,.0004,.0001,-.0002,-.0004,.0004,.0005,.0006,.0007)


def boundary(n):
    return 1.25+(n//16)*.0008+OFFSETS[n%16]


def price(t):
    q=(t-EPOCH)/900;i=math.floor(q);f=q-i
    return boundary(i)*(1-f)+boundary(i+1)*f


class SyntheticStore:
    calls=0
    def __init__(self,directory,reader=None,legacy_root=None,progress=lambda **x:None):
        self.memo=OrderedDict();self.downloads=0
    def exact(self,pair,gran,start,end):return self.range(pair,gran,start,end)
    def range(self,pair,gran,start,end):
        SyntheticStore.calls+=1;step=e.STEP[gran];out=[]
        for t in range(int(start.timestamp()),int(end.timestamp()),step):
            points=[price(t),price(t+step)]
            points.extend(price(s) for s in range(((t//900)+1)*900,t+step,900))
            mid=dict(o=points[0],h=max(points),l=min(points),c=points[1])
            out.append(dict(time=e.stamp(e.datetime.fromtimestamp(t,e.UTC)),complete=True,mid=mid,
                            bid={k:v-.00002 for k,v in mid.items()},ask={k:v+.00002 for k,v in mid.items()}))
        return out
    def window(self,pair,start,end):return self.range(pair,'M15',start,end)
    def difference_count(self):return 0
    def index(self):return [dict(origin='SYNTHETIC_OFFLINE_TEST_ONLY')]
    def close(self):pass


class IntegrationTests(unittest.TestCase):
    def test_known_183_prestart_bars_delays_pair_and_accounts_without_aborting(self):
        begin=e.dt('2020-03-02T00:00:00Z')
        class SparseWarmupStore(SyntheticStore):
            def range(self,pair,gran,start,end):
                rows=super().range(pair,gran,start,end)
                earliest=begin.timestamp()-183*e.STEP[gran]
                return [r for r in rows if e.dt(r['time'])>=begin or e.dt(r['time']).timestamp()>=earliest]
        with tempfile.TemporaryDirectory() as d:
            env=dict(PULLBACK_START_UTC=e.stamp(begin),PULLBACK_END_UTC='2020-05-01T00:00:00Z',
                     PULLBACK_WARMUP_START_UTC='2020-01-01T00:00:00Z',PULLBACK_PAIRS='GBP_USD',
                     PULLBACK_OFFLINE='1',PULLBACK_REQUIRE_PERSISTENCE='0')
            job=Research(output=d,environ=env,store_factory=SparseWarmupStore);job.run()
            self.assertEqual(job.state['status'],'COMPLETE',job.state)
            directory=job.work/'pairs'/'GBP_USD'
            coverage=json.loads((directory/'coverage.json').read_bytes())
            self.assertTrue(coverage['eligibility']['evaluation_delayed_for_warmup'])
            self.assertEqual([s['warmup_bars'] for s in coverage['streams']],[183,183])
            self.assertEqual([s['required_observed_warmup_bars'] for s in coverage['streams']],[250,1000])
            ready=coverage['eligibility']['effective_evaluation_start_utc']
            self.assertGreater(ready,e.stamp(begin))
            signals=[json.loads(r) for r in gzip.decompress((directory/'qualifying_signals.jsonl.gz').read_bytes()).splitlines()]
            self.assertTrue(signals)
            self.assertTrue(all(s['entry_utc']>=ready for s in signals))
            for r in json.loads((directory/'account_summaries.json').read_bytes()):
                self.assertEqual(r['window_start'],ready)
                self.assertTrue(r['warmup_exclusion_at_window_start'])
                self.assertFalse(r['complete_requested_window'])
                self.assertEqual(r['effective_account_start_utc'],ready)
                years=(e.dt(r['window_end'])-e.dt(ready)).total_seconds()/(365.25*86400)
                nav=r['conditional_ending_nav_gbp']
                if nav>0:
                    self.assertAlmostEqual(r['conditional_nav_cagr_pct'],((nav/100)**(1/years)-1)*100)
            import csv,io
            monthly=list(csv.DictReader(io.StringIO((directory/'monthly_qualifications.csv').read_text())))
            self.assertTrue(all(r['qualifications']=='' for r in monthly if r['month']=='2020-03'))

    def test_full_job_report_manifest_cash_ledgers_and_resume(self):
        with tempfile.TemporaryDirectory() as d:
            env=dict(PULLBACK_START_UTC='2020-03-02T00:00:00Z',PULLBACK_END_UTC='2020-03-09T00:00:00Z',
                     PULLBACK_WARMUP_START_UTC='2020-01-01T00:00:00Z',PULLBACK_PAIRS='GBP_USD',
                     PULLBACK_OFFLINE='1',PULLBACK_REQUIRE_PERSISTENCE='0')
            job=Research(output=d,environ=env,store_factory=SyntheticStore);job.run()
            self.assertEqual(job.state['status'],'COMPLETE',job.state)
            self.assertEqual(job.state['pairs_completed'],1)
            directory=job.work/'pairs'/'GBP_USD'
            signals=[json.loads(row) for row in gzip.decompress((directory/'qualifying_signals.jsonl.gz').read_bytes()).splitlines()]
            self.assertGreater(sum(s['control']=='TREND_PULLBACK_ENGULF' for s in signals),0)
            summaries=json.loads((directory/'account_summaries.json').read_bytes())
            self.assertEqual(len(summaries),6)
            self.assertGreater(sum(r['closed_trades'] for r in summaries),0)
            for result in directory.joinpath('accounts').glob('*.json.gz'):
                r=json.loads(gzip.decompress(result.read_bytes()))['result']
                summary=r['summary']
                self.assertEqual(summary['status'],'COMPLETE_CONDITIONAL')
                self.assertAlmostEqual(summary['ending_balance_gbp'],100+summary['price_pl_gbp']+summary['financing_gbp'],places=4)
            archive=job.work/'P32_PULLBACK_PASS1_RESULTS.zip'
            with zipfile.ZipFile(archive) as z:
                self.assertIsNone(z.testzip());manifest=json.loads(z.read('report_manifest.json'))
                self.assertFalse(any(name.endswith('.sqlite3') for name in z.namelist()))
                for name,meta in manifest.items():
                    raw=z.read(name);self.assertEqual(e.sha(raw),meta['sha256']);self.assertEqual(len(raw),meta['bytes'])
                self.assertEqual(json.loads(z.read('RUN_COMPLETE.json'))['status'],'COMPLETE_RESEARCH_PASS1')
            calls=SyntheticStore.calls
            again=Research(output=d,environ=env,store_factory=SyntheticStore);again.run()
            self.assertEqual(again.state['status'],'COMPLETE',again.state)
            self.assertEqual(SyntheticStore.calls,calls)
            # A corrupt completed witness must fail rather than silently reuse.
            (directory/'generation_counts.json').write_text('{}')
            failed=Research(output=d,environ=env,store_factory=SyntheticStore);failed.run()
            self.assertEqual(failed.state['status'],'ERROR')
            self.assertIn('checkpoint file',failed.state['error'])


if __name__=='__main__':unittest.main()
