# AUD/USD pullback: 8 / 12 / 16-bar lifetime matrix

Nine fixed settings: lifetimes 8, 12, 16 M15 bars crossed with maximum H1 trend
distance 1.00, 1.25, 1.50 ATR. Engulfing body stays at least 1.25 M15 ATR.
RR3, 1% NAV risk, 15-tick entry bound, 10-tick stop buffer and the GBP account
model remain fixed. This is a historical research service, separate from live.

## Deployment: SAME research service and mounted /data

1. Extract this ZIP. Upload its CONTENTS into the research service project root,
   replacing matching files. Keep all existing unrelated files and all /data.
   Do not upload this to the live executor or live strategy probe.
2. Keep existing variables, including the OANDA token and OANDA_ENV=live:

   PULLBACK_OUTPUT_DIR=/data/p32_pullback_pass1
   PULLBACK_LEGACY_CACHE_DIR=/data/p32_signal_admission
   AUDUSD_MATRIX_OUTPUT_DIR=/data/p32_audusd_long_matrix
   AUDUSD_EXTENSION_OUTPUT_DIR=/data/p32_audusd_long_extension

   If AUDUSD_MATRIX_SOURCE_DIR was already set, keep its existing value.
   AUDUSD_FREQUENCY_OUTPUT_DIR can also remain set; its files are not modified.
3. Add ONE variable:

   AUDUSD_LIFETIME_OUTPUT_DIR=/data/p32_audusd_lifetime

4. Set the start command:

   python audusd_pullback_lifetime.py

   Python 3.10+; standard library only. No additional pip install is needed.
   Keep the platform's PORT variable and health path /health.
5. Redeploy. /health must show:

   AUDUSD_PULLBACK_LIFETIME_MATRIX_V1_2026_10_23

6. Open GET /status once to start. Refresh that URL for progress. /health,
   HEAD /status and opening /protocol do not start a run.
7. When /status is COMPLETE with accounts_completed=126 and
   overlap_account_payloads_matched=42, download /results-summary.zip and send
   that compact ZIP for review. It is capped below 20 MB. /results.zip streams
   the full evidence when required; full evidence may be larger.

## What will run

Three unchanged eight-bar controls run FIRST, before the six new cells.
They must reproduce exact saved signals, standalone opportunity records and
ALL 42 complete account payloads from the larger-body extension, not merely
headline statistics. The unfiltered baseline must reproduce 4,086 signals.
Any mismatch stops the run. A failing result is not saved as a valid checkpoint.

Each of nine candidates gets seven independently reset GBP100 accounts under
two funding assumptions (9 x 7 x 2 = 126 accounts): full history, five eras,
and January-2021-to-cutoff. Frozen 14-Oct-2026 rates and adverse 6% annual
financing are assumptions, not actual historical financing. The last era is
partial through the original 20 September 2026 cutoff. The original observed
warm-up remains unchanged. The year/era windows overlap; do not sum them.

Each lifetime regenerates its OWN causal signal sequence. An N-bar lifetime
allows ages 1..N; expiry happens before bar N+1. Missing M15 bars or failure of
the original fresh H1 trend resets the setup. Rejected filters retain the
original touch and all lows. Accepted qualifications consume the setup even
when later execution rejects them or an existing position blocks entry.
No observations from the frequency diagnostic are imported as new signals.

## Review files

- account_matrix.csv: trade counts/frequency, win rate, net R, financing,
  conditional NAV/CAGR, sampled drawdown and uncertainty flags for each account.
- control_comparisons.csv: differences against the eight-bar account with the
  SAME distance, funding assumption and window. This includes changes in
  frequency, expectancy, CAGR, drawdown and financing. It is not the marginal
  return of adding a strategy to live32.
- candidate_screens.csv and plateau_regions.csv: original robustness criteria
  applied to four adjacent 2x2 lifetime/distance blocks. No thresholds relaxed.
- overlap_parity.json: exact comparisons for all 42 saved control accounts.
- baseline_parity.json, broker_reconciliation.json and source_provenance.json.

Minimum screen samples remain 200 full-history closes, 60 recent closes and
20 per era. Full/recent NAV and average net R without the best five trades must
be positive in BOTH funding modes; at least four of five supported frozen-rate
eras must be profitable. Every neighbouring member must pass. Unsupported eras
are not treated as losses. A numeric pass still needs uncertainty review,
combined portfolio admission/overlap checks and forward shadow testing.

## Storage and restarts

Old source, matrix and extension files/caches are read-only. Frozen source
windows from the original study, matrix AND extension must retain exact prices.
The original pricing/account modules are included byte-for-byte unchanged.
The new generator changes setup expiry only; eight-bar controls prove parity.

Private candle cache defaults to 64 MiB with a 96 MiB free reserve. Reusable
geometry paths and finished accounts are independently checkpointed. A restart
verifies previous evidence and regenerates signals, then reuses complete
accounts; it does not replay completed account checkpoints. Old database WAL
writers are not read as immutable sources. Full ZIP output streams without a
second large archive on disk. Never delete old research directories to install.

If storage reaches its reserve, the service reports PAUSED_STORAGE and retains
checkpoints. If a frozen candle window has changed on re-fetch, it stops to
avoid mixing historical revisions. Send /status if a source, parity or storage
check fails; do not modify saved research results to make the check pass.

## Scope and verification

Original gaps, barrier ambiguity, stale valuation quotes and conditional
STOP_FIRST/END_ASC assumptions remain explicit. The bounded coverage audit's
new candles are not imported; its unresolved paths are not silently resolved.
Sampled NAV drawdown is not a guaranteed worst intrabar drawdown. Historical
broker terms/rates were not fully reconstructed; history has already been seen.
There is no untouched holdout, automatic selection, order submission or live
executor/probe modification in this package.

Run offline verification from the extracted directory:

   python -m unittest discover -s lifetime_tests -v

The included tests cover expiry boundaries, retention of inherited lows,
causality, exact eight-bar signal parity, adjacent-region criteria, source
checksum/read-only protections, complete synthetic replays, all 42 control
payloads, checkpoint reuse and corruption rejection. No actual historical run
was performed while building this package.
