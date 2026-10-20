# P32 pullback storage recovery

This is a small add-on for `P32_PULLBACK_PASS1_V2_2026_10_20_WARMUP`.
Follow `START_HERE_STORAGE.txt`. It needs the existing V2 files already deployed;
it is not a replacement strategy engine or a complete standalone service.

## What caused the failure

The status reported zero free bytes while EUR/JPY was being processed. The
existing program retains canonical M1/S5 windows, uncompressed path databases,
opportunity checkpoints, compressed exports and duplicate full download ZIPs.
A larger volume is not the first recovery step: several of these are duplicates.

## Recovery and protections

* Acquire the original runner lock. Health/HEAD requests do not start recovery.
* Verify every file named in each completed pair's COMPLETE marker and verify
  its study signature. Stop if anything is missing or changed.
* Remove the regenerable local full-results ZIP and partial ZIP temp file.
  The user's downloaded archive and the primary reports remain untouched.
* Remove completed opportunity databases only after matching every canonical
  row and checksum against the pinned compressed export. Never remove an
  incomplete pair's opportunity database.
* Copy the canonical candle database into a smaller replacement, retaining
  about 128 MiB of compressed raw bodies by default, all request receipts and
  all evicted source identities/checksums. Prefer native M15/H1 history.
* Copy global path checkpoints into a smaller replacement, removing a completed
  row only when it exactly matches that pair's pinned price-path export.
  Pending rows are copied byte-for-byte. Both replacements are integrity checked
  and atomically installed; failed temporary copies are removed.
* Resume the original V2 runner. Its calculations and signature remain unchanged.
  The four completed pairs are reused; in-progress EUR/JPY checkpoints are kept.
* Continue bounded raw-cache eviction and reclaim completed duplicate checkpoints
  after each new pair. Stop if the configured free-space reserve is reached.

Native histories or fine execution windows evicted from disk may be downloaded
again. Re-fetch checksums must match the saved checksum before returned prices
can be consumed. A revision triggers an explicit stop. This preserves consistency
at the cost of longer possible runtime and less raw history held offline.

The cache limit covers compressed raw bodies, not the whole study. Reports,
metadata, receipts and pending checkpoints still need space. Completion on the
host's current volume cannot be guaranteed without its actual post-cleanup disk
usage; reserve errors preserve the available evidence for a further decision.
The legacy `/data/p32_signal_admission` directory is never pruned by this add-on.

## Reports

The saved compact ZIP is `P32_PULLBACK_SUMMARY.zip`. HTTP `/results-summary.zip`
streams its current summary scope. `/results.zip` streams the full published
completed-pair files with a bounded output queue and per-file SHA-256 manifest.
No second full archive is materialized on disk. A dropped connection can be
retried. Full reports remain large; the summary is sufficient for most reviews.

Top-level aggregate files and published-pair metadata are snapshotted together.
Unfinished pairs do not enter aggregate results. The study remains conditional,
isolated research with unresolved history/funding limitations; this storage fix
does not validate a new trading strategy or the combined live portfolio.

## Validation

See `validation/tests.txt` and `validation/validation.json`.
An end-to-end synthetic replay compares every account payload and summary with
the unchanged V2 runner, verifies a valid streamed ZIP, and then resumes without
requesting quotes for a completed pair. Separate fixtures verify pending rows,
checksum failures, disk-full bootstrap, bounded cache and revised-price refusal.
These are offline tests; no access to the user's deployed service is implied.

For developers, with the V2 source adjacent or the add-on deployed over it:

    V2_ROOT="$PWD" PYTHONPATH="$PWD:$PWD/tests" python -m unittest discover -s storage_guard_tests -v

Tests are packaged under `storage_guard_tests` to avoid overwriting V2 tests.
No third-party packages or order endpoints are introduced.
