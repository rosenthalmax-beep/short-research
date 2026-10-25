"""Regression for resume counters without weakening reviewed price/code evidence."""
import copy
import json
import tempfile
import unittest
from pathlib import Path
from pullback_lab import core as e
from audusd_rr.provenance_guard import COUNTERS,REFERENCES,verify_report
from audusd_rr.reuse import Previous

REFERENCE_SHA=next(k for k,v in REFERENCES.items() if v['archive']=='AUDUSD_LIFETIME_EXTENSION_SUMMARY.zip')
PAYLOAD=REFERENCES[REFERENCE_SHA]['payload']


class ProvenanceGuardTests(unittest.TestCase):
 def check(self,payload,raw=None):
  with tempfile.TemporaryDirectory() as tmp:
   p=Path(tmp)/'source_provenance.json';p.write_bytes(e.canon(payload) if raw is None else raw)
   before=p.read_bytes();result=verify_report(p,REFERENCE_SHA,len(e.canon(PAYLOAD)))
   self.assertEqual(p.read_bytes(),before);return result
 def test_exact_reviewed_payload_passes(self):
  observed,check=self.check(PAYLOAD);self.assertEqual(observed,REFERENCE_SHA);self.assertEqual(check['classification'],'EXACT_BYTES')
 def test_all_three_work_counters_can_change_and_are_reported(self):
  changed=copy.deepcopy(PAYLOAD)
  for k in COUNTERS:changed[k]=0
  observed,check=self.check(changed)
  self.assertEqual(check['classification'],'COUNTERS_ONLY');self.assertEqual(len(check['accepted_counter_changes']),3)
  self.assertNotEqual(observed,REFERENCE_SHA);self.assertTrue(check['locked_fields_match'])
 def test_formatting_only_change_is_separate_from_counter_drift(self):
  raw=json.dumps(PAYLOAD,indent=4,sort_keys=False).encode()
  _,check=self.check(PAYLOAD,raw=raw);self.assertEqual(check['classification'],'JSON_FORMATTING_ONLY')
  self.assertEqual(check['accepted_counter_changes'],[])
 def test_price_native_history_code_and_assumption_changes_are_rejected(self):
  cases=[('path_reuse','native_sha256'),('source_pins','signature'),('engine_pins','pullback_lab/accounting.py'),
         ('extension_code','runner.py'),('history','m15','observed_bars'),('coverage_audit_candles_used',),
         ('source_files_modified',),('previous_matrix_signature',)]
  for keys in cases:
   with self.subTest(keys=keys):
    changed=copy.deepcopy(PAYLOAD);where=changed
    for key in keys[:-1]:where=where[key]
    old=where[keys[-1]];where[keys[-1]]=not old if isinstance(old,bool) else old+1 if isinstance(old,int) else 'CHANGED'
    with self.assertRaisesRegex(ValueError,'locked fields'):self.check(changed)
 def test_bad_counter_types_missing_and_unexpected_fields_are_rejected(self):
  for value in [None,False,True,-1,1.5,'0']:
   changed=copy.deepcopy(PAYLOAD);changed[COUNTERS[0]]=value
   with self.assertRaisesRegex(ValueError,'counter'):self.check(changed)
  changed=copy.deepcopy(PAYLOAD);del changed[COUNTERS[0]]
  with self.assertRaisesRegex(ValueError,'counter'):self.check(changed)
  changed=copy.deepcopy(PAYLOAD);changed['extra_cache_counter']=0
  with self.assertRaisesRegex(ValueError,'locked fields'):self.check(changed)
 def test_duplicate_json_keys_are_rejected(self):
  raw=e.canon(PAYLOAD);raw=raw[:-1]+b',"new_geometry_paths_computed":0}'
  with self.assertRaisesRegex(ValueError,'Duplicate JSON key'):self.check(PAYLOAD,raw)
 def test_unrecognized_provenance_and_changed_results_cannot_use_exception(self):
  with tempfile.TemporaryDirectory() as tmp:
   p=Path(tmp)/'source_provenance.json';p.write_bytes(e.canon(PAYLOAD))
   with self.assertRaisesRegex(ValueError,'Unrecognized'):verify_report(p,'0'*64)
   p=Path(tmp)/'account_matrix.csv';p.write_bytes(b'changed')
   with self.assertRaisesRegex(ValueError,'summary changed'):verify_report(p,REFERENCE_SHA)
 def test_reference_payloads_are_self_verifying(self):
  self.assertEqual(len(REFERENCES),4)
  for digest,record in REFERENCES.items():self.assertEqual(e.sha(e.canon(record['payload'])),digest)
 def test_stable_identity_survives_counter_changes_but_running_bytes_are_locked(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);p=root/'source_provenance.json';p.write_bytes(e.canon(PAYLOAD))
   index=root/'index.gz'
   import gzip
   index.write_bytes(gzip.compress(e.canon([])))
   def proof_for(payload):
    p.write_bytes(e.canon(payload));observed,check=verify_report(p,REFERENCE_SHA)
    obj=Previous.__new__(Previous);obj.root=root;obj.pins={'signature':'test'};obj.witness={'source_provenance.json':observed}
    obj.provenance_identity={'source_provenance.json':dict(reviewed_sha256=REFERENCE_SHA,stable_fields_sha256=check['stable_fields_sha256'])}
    obj.index_semantic_sha256=e.sha(e.canon([]));obj.index_path=index;obj.source_rows=[]
    return obj
   before=proof_for(PAYLOAD);identity=before.proof()
   changed=copy.deepcopy(PAYLOAD);changed[COUNTERS[0]]=0
   after=proof_for(changed);self.assertEqual(identity,after.proof())
   # The initial counter exception does not permit changes during a live replay.
   with self.assertRaisesRegex(ValueError,'changed during'):before.assert_unchanged()
   after.assert_unchanged()


if __name__=='__main__':unittest.main()
