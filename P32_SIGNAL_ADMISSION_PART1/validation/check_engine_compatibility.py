"""Optional offline format check using the original matrix package's fixtures."""
import argparse
import json
import sys
import tempfile
from pathlib import Path

parser=argparse.ArgumentParser()
parser.add_argument('--matrix-root', required=True)
args=parser.parse_args()
matrix=Path(args.matrix_root).resolve()
sys.path[:0]=[str(Path(__file__).resolve().parents[1]),str(matrix),str(matrix/'tests')]
import audusd_pullback_reconcile as audit
from fixture import SyntheticStore
from pullback_lab import core as e
from pullback_lab.storage import load_bars
from pullback_lab.market import Market
from pullback_lab.funding import Funding
from pullback_lab.accounting import Replay
from pullback_lab.broker_reconcile import reconcile
from audusd_matrix.signals import generate_grid
from audusd_matrix.pool import Pool
from audusd_matrix.reporting import augment

start='2020-03-02T00:00:00Z';end='2020-03-09T00:00:00Z';warm='2020-01-01T00:00:00Z'
store=SyntheticStore('unused')
m15,_=load_bars(store,'AUD_USD','M15',e.dt(warm),e.dt(start),e.dt(end))
h1,_=load_bars(store,'AUD_USD','H1',e.dt(warm),e.dt(start),e.dt(end))
signals,_=generate_grid(m15,h1,start,end,start)
selected=signals['M_D1.25_B1.0'];assert len(selected)>0
proof=json.loads((matrix/'pullback_lab/saved_broker_evidence.json').read_bytes())
broker,_=reconcile(proof);funding=Funding(proof['funding_schedules'])
term=next(i for i in proof['instruments'] if i['name']=='AUD_USD')
with tempfile.TemporaryDirectory() as temp:
    pool=Pool(Path(temp),store,m15,'AUD_USD',e.dt(end),'SYNTHETIC_COMPATIBILITY')
    opps=[];paths={}
    for s in selected:
        p=pool.get(s);paths[p['path_cache_key']]=p
        shadow=pool.shadow(s,p,Market(store),funding,term,broker['observed_symmetric_conversion_factor_markup'])
        opps.append(dict(signal_key=s['signal_key'],entry_utc=s['entry_utc'],path_cache_key=p['path_cache_key'],path_status=p['status'],shadow=shadow))
    checks=[]
    for mode in audit.MODES:
        result=Replay(selected,pool.get,Market(store),funding,proof['instruments'],'END_ASC',mode,
                      broker['observed_symmetric_conversion_factor_markup'],initial=100,start=start,end=end).run()
        expected=dict(augment(result,start,end),window_id='FULL_HISTORY',effective_start=start,end=end)
        b,_,_,c,_=audit.compare_account({s['signal_key']:s for s in selected},
            {o['signal_key']:o for o in opps},paths,result,expected)
        assert c['status']=='PASS'
        checks.append(dict(mode=mode,closed_trades=len(result['trades']),qualifications=len(selected),
                           cash='PASS',bridge_residual=b['bridge_residual']))
    pool.close()
print(json.dumps(dict(original_engine_synthetic_compatibility='PASS',checks=checks,
                     candle_downloads=0,actual_historical_reconciliation_run=False),indent=2))
