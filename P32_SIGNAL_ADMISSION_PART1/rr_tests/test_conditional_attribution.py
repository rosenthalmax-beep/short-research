import csv,gzip,json,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from audusd_conditional.reporting import write_attribution
class AttributionTests(unittest.TestCase):
 def test_geometry_matching_and_shared_sizing_are_explicit(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);dirs=[]
   for cid,items in [('a',[('old',1.,-1.),('same',2.,3.)]),('b',[('new',3.,2.),('same2',2.,2.5)])]:
    d=root/'candidates'/cid;d.mkdir(parents=True);dirs.append(d);signals=[];trades=[]
    for key,t,r in items:
     signals.append(dict(signal_key=key,entry_utc=str(t),reference_entry=1.,stop=.9,target=1.3))
     trades.append(dict(signal_key=key,net_r_actual_entry_risk=r,net_pl_gbp=r*2))
    with gzip.open(d/'signals.jsonl.gz','wt') as f:
     for s in signals:f.write(json.dumps(s)+'\n')
    (d/'accounts').mkdir()
    for w in ('FULL_HISTORY','RECENT_2021_TO_CUTOFF'):
     for mode in ('FROZEN_14OCT2026_RATES','ADVERSE_6PCT_ANNUAL'):
      (d/'accounts'/(w+'__'+mode+'.json.gz')).write_bytes(gzip.compress(json.dumps(dict(result=dict(trades=trades))).encode()))
   with patch('audusd_conditional.reporting.candidates',return_value=[dict(kind='ONE_FEATURE',candidate_id='b',anchor_id='a')]):write_attribution(SimpleNamespace(work=root),dirs)
   rows=list(csv.DictReader((root/'trade_attribution.csv').read_text().splitlines()));self.assertEqual(len(rows),12)
   first={r['population']:r for r in rows[:3]};self.assertEqual(first['REMOVED']['anchor_net_r'],'-1.0');self.assertEqual(first['NEW']['candidate_net_r'],'2.0');self.assertEqual(first['SHARED']['anchor_net_r'],'3.0');self.assertEqual(first['SHARED']['candidate_net_r'],'2.5')
