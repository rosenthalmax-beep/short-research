# AUD/USD pullback: coarse RR comparison

Fixed grid: RR 2.5 / 3.0 / 3.5; lifetimes 16 / 20 / 24 M15 bars; H1 trend
distance caps 1.00 / 1.25 ATR. Body floor 1.25 M15 ATR, 1% NAV risk,
15-tick adverse entry bound, 10-tick stop buffer and all other filters fixed.
18 settings x 7 reset windows x 2 financing assumptions = 252 accounts.
This is a historical research service. It does not modify the live portfolio.

## Install on the SAME research service with mounted /data

1. Extract this ZIP and upload its CONTENTS to the project root, replacing
   matching files. Keep all unrelated files and everything under /data.
   Do not upload this to the live executor or strategy probe.
2. Keep existing variables, including OANDA_ENV=live and the current OANDA token:

   PULLBACK_OUTPUT_DIR=/data/p32_pullback_pass1
   PULLBACK_LEGACY_CACHE_DIR=/data/p32_signal_admission
   AUDUSD_MATRIX_OUTPUT_DIR=/data/p32_audusd_long_matrix
   AUDUSD_EXTENSION_OUTPUT_DIR=/data/p32_audusd_long_extension
   AUDUSD_LIFETIME_OUTPUT_DIR=/data/p32_audusd_lifetime
   AUDUSD_LIFETIME_EXTENSION_OUTPUT_DIR=/data/p32_audusd_lifetime_extension

   Keep AUDUSD_MATRIX_SOURCE_DIR if already set. Other old directory variables
   can remain; they are not repurposed or overwritten.
3. Add ONE variable:

   AUDUSD_RR_OUTPUT_DIR=/data/p32_audusd_rr

4. Set the start command:

   python audusd_pullback_rr.py

   Python 3.10+; standard library only. No additional pip packages are needed.
   Keep the platform PORT variable and /health health path.
5. Redeploy. /health must show:

   AUDUSD_PULLBACK_RR_MATRIX_V1_2026_10_24

6. Open GET /status once to start. Refresh it for progress. /health, HEAD /status
   and /protocol do not start work.
7. Wait for status COMPLETE with accounts_completed=252 and
   overlap_account_payloads_matched=84. Download /results-summary.zip and send
   that compact ZIP for review. The summary is capped below 20 MB.
   /results.zip streams full evidence when needed; it can be larger.

## Controls and qualification logic

The SIX RR3 controls run first. All signal fields, standalone opportunity records
and all 84 COMPLETE prior account payloads must match the reviewed 16/20/24
lifetime-extension results. The original eight-bar unfiltered regression baseline
must still match all 4,086 signal records. Mismatch stops the run before pricing
new RR variants; a failing account is not saved as a valid checkpoint.

The existing causal generator regenerates each distance/lifetime qualification
sequence. RR does not affect trigger conditions, arming, expiry, inherited lows
or setup consumption. A separate sequence check verifies identical entry times,
stops and features across RR. RR3 payloads keep their exact original identities.
New RR targets are calculated from reference entry/stop using the unchanged
broker rounding. No old winning/losing R values are simply multiplied.

Each target gets a separate exit path, with RR in the geometry cache identity.
Every account reruns chronological exits, position blocking, sizing, margin,
financing, GBP conversion and compounding. Different holding durations can change
which later signals become trades, even though qualifications are the same.
Accepted qualifications still consume a setup before execution/admission.

## Windows, funding and reports

Each candidate uses fresh GBP100 accounts for full history, five era windows
and recent January-2021-to-cutoff, under frozen-14-Oct-2026 and adverse 6% annual
financing assumptions. These are modeled rates, not actual historical schedules.
The partial final era ends at the original 20 September 2026, 18:15 UTC cutoff.
Original observed warm-up eligibility is retained. Windows overlap; do not sum.

- account_matrix.csv: trades/frequency, wins, net R, financing, conditional
  NAV/CAGR, sampled drawdown and uncertainty for each fresh account.
- control_comparisons.csv: differences against RR3 at the SAME lifetime,
  distance, window and funding assumption. These reflect changed exits and
  admission, not incremental live32 return or additional-trade win rate.
- qualification_sequence_parity.json: identical qualifications/stops/features
  and correctly rounded targets across RR.
- candidate_screens.csv and plateau_regions.csv: per-cell criteria and FOUR
  adjacent 2x2x2 regions across RR/lifetime/distance, with EIGHT members each.
- overlap_parity.json: all 84 exact control account comparisons.
- baseline_parity.json, source_provenance.json, broker_reconciliation.json,
  previous_matrix_witness.json and study_context.json: provenance/checks.

Each of a region's eight cells must meet the unchanged per-account screen:
>=200 full-history closes, >=60 recent closes, >=20 per era; positive full/recent
NAV and average net R without the best five trades in BOTH funding modes; at
least four supported frozen-funding eras profitable. No numeric threshold is
relaxed. An isolated peak cannot pass a full three-dimensional region. Sparse
eras are unsupported, not automatically losses. No candidate is selected live.

## Storage and restart

All 54 prior reference modules, including the source signal, pricing and account
engine, are included byte-for-byte unchanged. Old research files and caches are
read-only. Source indices are frozen through the latest lifetime-extension,
prior lifetime, body-extension and original matrix witness chain. Matching
geometry evidence is reused; revised frozen candle windows stop the run.

The new raw candle cache defaults to 64 MiB with a 96 MiB free reserve. Fine
execution/conversion windows are fetched only when necessary. Completed account
checkpoints resume independently. A restart verifies sources and regenerates
qualifications, then reuses completed account checkpoints; it does not replay
finished accounts. Full evidence ZIP streams without a second large disk copy.
Do not delete existing research directories to install this package.

If PAUSED_STORAGE, finished checkpoints are retained. If any source/parity check
fails, send /status; do not edit evidence to make it pass. Existing research
writers must have finished because this service takes shared ancestor locks.

## Limits and verification

Original gaps, barrier ambiguity, stale valuation quotes, unknown-entry omissions
and conditional STOP_FIRST/END_ASC ordering are preserved. New coverage-audit
candles are not imported and uncertain paths are not silently resolved. Sampled
NAV drawdown is not worst intrabar drawdown. Historical broker terms/funding
were not fully reconstructed. All researched history was already seen; this
is not an untouched holdout or statistical proof of a plateau.
No further lifetime expansion, new filters, automatic peak selection, combined
live32 validation or live-order/activation change is part of this package.

Offline checks:

   python -m unittest discover -s rr_tests -v

The tests include full synthetic predecessor/RR replays, all 84 control payloads,
checkpoint resume/corruption, causal qualifications, distinct RR geometry and
a price path where RR2.5 wins while RR3/RR3.5 stop. No actual historical run was
performed while building this package.
