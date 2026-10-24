# AUD/USD pullback: final 16 / 20 / 24-bar lifetime extension

Nine fixed cells: setup lifetimes 16, 20, 24 M15 bars crossed with H1 trend
distance caps 1.00, 1.25, 1.50 ATR. Minimum body remains 1.25 M15 ATR.
RR3, 1% NAV risk, 15-tick entry bound, 10-tick stop buffer, source cutoff,
pricing and GBP account rules are unchanged. This is the final bounded
lifetime extension agreed after reviewing AUDUSD_LIFETIME_SUMMARY.zip.

## Install on the SAME separate research service

1. Extract this ZIP. Upload its CONTENTS to the research project root, replacing
   matching files. Keep unrelated files and everything on the mounted /data.
   Do not upload this package to the live executor or strategy probe.
2. Keep all existing variables, including the existing OANDA token and
   OANDA_ENV=live. In particular, preserve these original directory settings:

   PULLBACK_OUTPUT_DIR=/data/p32_pullback_pass1
   PULLBACK_LEGACY_CACHE_DIR=/data/p32_signal_admission
   AUDUSD_MATRIX_OUTPUT_DIR=/data/p32_audusd_long_matrix
   AUDUSD_EXTENSION_OUTPUT_DIR=/data/p32_audusd_long_extension
   AUDUSD_LIFETIME_OUTPUT_DIR=/data/p32_audusd_lifetime

   Keep AUDUSD_MATRIX_SOURCE_DIR if you already set it to a custom location.
   AUDUSD_FREQUENCY_OUTPUT_DIR may remain set; its data is not modified.
3. Add ONE new variable; do not replace AUDUSD_LIFETIME_OUTPUT_DIR:

   AUDUSD_LIFETIME_EXTENSION_OUTPUT_DIR=/data/p32_audusd_lifetime_extension

4. Change the service start command to:

   python audusd_pullback_lifetime_extension.py

   Python 3.10+; standard library only. No additional pip packages are needed.
   Keep the platform PORT variable and /health health path.
5. Redeploy. /health must show:

   AUDUSD_PULLBACK_LIFETIME_EXTENSION_V1_2026_10_24

6. Open GET /status to start, then refresh that URL for progress. /health,
   HEAD /status and /protocol do not start work.
7. Once /status is COMPLETE, expect accounts_completed=126 and
   overlap_account_payloads_matched=42. Download /results-summary.zip and send
   that compact ZIP for review. The summary is capped below 20 MB.
   /results.zip streams full evidence when needed and may be larger.

## What runs

The three SIXTEEN-bar controls run first. They must match all saved signal
fields, standalone opportunity records and all 42 COMPLETE account payloads
from the previous 8/12/16 study. The unfiltered eight-bar baseline must still
match the original 4,086 signals. A mismatch stops the run before new variants;
a failing comparison is not saved as a valid account checkpoint.

Then the six new 20/24-bar cells independently regenerate their signal
sequences using the existing generator. No diagnostic observations are added
onto baseline signals. An N-bar setup may enter on ages 1..N; it expires before
age N+1. Trend failure or a missing M15 bar resets the setup. Failed filters
retain the touch and all intervening lows. Accepted qualifications consume the
setup even when subsequent execution rejects them or a position blocks entry.

Each candidate gets seven fresh GBP100 windows under two funding assumptions:
9 x 7 x 2 = 126 accounts. Windows are full history, five era resets and recent
January-2021-to-cutoff. Frozen 14-Oct-2026 rates and adverse 6% annual funding
are assumptions, not reconstructed historical rates. The source cutoff remains
20 September 2026 at 18:15 UTC. The last era is partial. Original observed
warm-up remains unchanged. Windows overlap; never sum their returns or counts.

## Reports

- account_matrix.csv: trade frequency, wins, net expectancy, financing,
  conditional NAV/CAGR, sampled drawdown and uncertainty per account.
- control_comparisons.csv: differences against the SIXTEEN-bar account at the
  same distance, window and funding assumption. These are fresh-account
  comparisons, not incremental live32 portfolio return or an additional-trade
  win rate. A positive drawdown difference means less negative sampled drawdown.
- candidate_screens.csv and plateau_regions.csv: unchanged robustness criteria
  across four neighbouring 2x2 lifetime/distance blocks.
- overlap_parity.json: exact comparison of all 42 control account payloads.
- baseline_parity.json, broker_reconciliation.json, source_provenance.json,
  previous_matrix_witness.json and study_context.json: verification/context.

The screen still requires >=200 full-history closes, >=60 recent closes and
>=20 per era; positive full/recent NAV and average net R without the five best
trades under BOTH funding modes; and at least four of five supported frozen-rate
eras profitable. Every neighbouring cell must pass. Samples/criteria are not
relaxed for this extension. Unsupported eras are not counted as losses.

## Storage, evidence and restart

All 45 original reference code/data modules are included byte-for-byte unchanged,
including the signal generator, pricing and account engine. All source, matrix,
body-extension and previous-lifetime files/caches are read-only. Ancestor source
indices are verified through the reviewed witness chain. Previously used
AUD/USD and GBP/USD price windows must retain their exact checksums.

Private raw candle cache defaults to 64 MiB, with a 96 MiB free reserve. Native
history, geometry and pricing evidence are reused only with matching identities.
New fine-candle/conversion windows are fetched only when necessary. Finished
accounts checkpoint independently; restart verifies prior evidence/regenerates
signals and reuses finished accounts. Full ZIP output streams without creating
a second large archive on disk. Never delete old directories to install.

A frozen source revision or failed parity check stops the study; it does not
silently replace evidence. PAUSED_STORAGE keeps finished checkpoints. Send
/status if a source, parity or storage check fails. Existing research writers
must have finished; this entry point holds shared locks on ancestor studies.

## Limits and validation

Original gaps, barrier ambiguities, stale valuations, unknown-entry omissions
and conditional STOP_FIRST/END_ASC assumptions remain explicit. Coverage-audit
candles are not imported and uncertain paths are not silently resolved. Sampled
NAV drawdown is not worst intrabar drawdown. No untouched holdout, fully
reconstructed historical broker terms, automatic peak selection, RR sweep,
combined live32 validation, forward-testing claim or live-order change is made.
The grid ends at 24 bars; it does not automatically expand further.

Run offline verification from the extracted project root:

   python -m unittest discover -s lifetime_extension_tests -v

17 offline tests passed, including full synthetic predecessor/final account
replays, exact 42-control parity, expiry boundaries, causality, frozen ancestor
sources, read-only path reuse, checkpoint resume and tamper rejection.
No actual historical run was performed while building this package.
