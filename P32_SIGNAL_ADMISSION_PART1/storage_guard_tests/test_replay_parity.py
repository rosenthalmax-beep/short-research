"""Full V2 financial replay parity, cleanup, streamed report and warm resume."""
import gzip
import io
import json
import os
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace
from pullback_lab.runner import Research
from pullback_guard.runtime import configure
from pullback_guard.download import Stream
from test_integration import SyntheticStore

ROOT=Path(os.environ['V2_ROOT'])
ENV=dict(PULLBACK_START_UTC='2020-03-02T00:00:00Z',PULLBACK_END_UTC='2020-03-09T00:00:00Z',
         PULLBACK_WARMUP_START_UTC='2020-01-01T00:00:00Z',PULLBACK_PAIRS='GBP_USD',PULLBACK_OFFLINE='1',PULLBACK_REQUIRE_PERSISTENCE='0')


class ParityTests(unittest.TestCase):
    def test_original_ledgers_identical_and_finished_pairs_resume_without_quotes(self):
        with tempfile.TemporaryDirectory() as a,tempfile.TemporaryDirectory() as b:
            original=Research(output=a,environ=ENV,store_factory=SyntheticStore);original.run()
            job=configure(Research(output=b,environ=ENV,store_factory=SyntheticStore),ROOT)
            # SyntheticStore has no disk cache; the actual bounded Store is
            # exercised separately by the raw-cache/refetch tests.
            job.store_factory=SyntheticStore;job.run()
            self.assertEqual(original.state['status'],'COMPLETE',original.state)
            self.assertEqual(job.state['status'],'COMPLETE',job.state)
            old=original.work/'pairs/GBP_USD';new=job.work/'pairs/GBP_USD'
            self.assertEqual(json.loads((old/'account_summaries.json').read_bytes()),json.loads((new/'account_summaries.json').read_bytes()))
            for path in (old/'accounts').glob('*.json.gz'):
                self.assertEqual(json.loads(gzip.decompress(path.read_bytes())),json.loads(gzip.decompress((new/'accounts'/path.name).read_bytes())))
            self.assertFalse((new/'opportunity_checkpoints.sqlite3').exists())
            self.assertFalse((job.work/'P32_PULLBACK_PASS1_RESULTS.zip').exists())
            with zipfile.ZipFile(io.BytesIO(b''.join(Stream(job)))) as z:
                self.assertIsNone(z.testzip());self.assertEqual(json.loads(z.read('RUN_COMPLETE.json'))['status'],'COMPLETE_RESEARCH_PASS1')
            calls=SyntheticStore.calls
            again=configure(Research(output=b,environ=ENV,store_factory=SyntheticStore),ROOT);again.store_factory=SyntheticStore;again.run()
            self.assertEqual(again.state['status'],'COMPLETE',again.state);self.assertEqual(SyntheticStore.calls,calls)
            self.assertEqual(again.signature,original.signature)

    def test_full_disk_bootstrap_reclaims_duplicate_before_free_space_check(self):
        from test_storage_guard import completed
        with tempfile.TemporaryDirectory() as tmp:
            job=configure(Research(output=tmp,environ=ENV,store_factory=SyntheticStore),ROOT)
            # Minimal synthetic pinned witness with this study signature.
            d,_=completed(job.work)
            pin=json.loads((d/'COMPLETE.json').read_bytes());pin['signature']=job.signature;(d/'COMPLETE.json').write_text(json.dumps(pin))
            archive=job.work/'P32_PULLBACK_PASS1_RESULTS.zip';archive.write_bytes(b'duplicate sentinel')
            def space(_):return SimpleNamespace(free=0 if archive.exists() else 1)
            with patch('pullback_guard.runtime.shutil.disk_usage',side_effect=space):job.run()
            self.assertFalse(archive.exists());self.assertTrue((d/'opportunities.jsonl.gz').exists())
            self.assertEqual(job.state['phase'],'STORAGE_RECOVERY_STOPPED')
            self.assertIn('Not enough scratch',job.state['error'])

if __name__=='__main__':unittest.main()
