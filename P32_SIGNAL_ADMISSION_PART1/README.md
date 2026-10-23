# AUD/USD pullback frequency diagnostic

This is the next research step after AUDUSD_EXTENSION_SUMMARY.zip. It diagnoses
why the existing eight-bar, body-1.25 setups are infrequent. It does not yet
replay a 12/16-bar trading system or estimate returns from late observations.

## Install on the SAME separate research/audit service

1. Extract this ZIP and upload its CONTENTS to the project root, replacing
   matching files. Keep existing unrelated files and the mounted /data volume.
   Do not upload this to the live executor or strategy probe.
2. Keep existing environment variables, including OANDA_ENV=live and the current
   OANDA token. Keep the existing directory variables unchanged:

   PULLBACK_OUTPUT_DIR=/data/p32_pullback_pass1
   PULLBACK_LEGACY_CACHE_DIR=/data/p32_signal_admission
   AUDUSD_MATRIX_OUTPUT_DIR=/data/p32_audusd_long_matrix
   AUDUSD_EXTENSION_OUTPUT_DIR=/data/p32_audusd_long_extension

   If AUDUSD_MATRIX_SOURCE_DIR was already set for the previous runs, keep it.
3. Add:

   AUDUSD_FREQUENCY_OUTPUT_DIR=/data/p32_audusd_frequency

4. Change the service start command to:

   python audusd_pullback_frequency.py

   No new pip dependencies are needed (Python 3.10+; standard library only).
   Keep the existing platform PORT variable.
5. Redeploy, check /health shows
   AUDUSD_PULLBACK_FREQUENCY_DIAGNOSTIC_V1_2026_10_23.
6. Open /status once to start. Refresh that same URL for progress. Health checks
   and HEAD /status do not start work.
7. Once status is COMPLETE, download /results-summary.zip and send that ZIP for
   review. /results.zip contains the detailed setup/attempt/observation rows.
   Both download types are capped below 20 MiB. This code package is much smaller.

## What is fixed

- AUD/USD BUY; body >=1.25 M15 ATR; H1 distance caps 1.00/1.25/1.50 ATR.
- Original EMA trend, arming, engulf, eight-bar lifetime and consumption logic.
- Same observed warm-up and historical cutoff (20 September 2026 at 18:15 UTC).
- Original stop geometry (10-tick buffer), RR3 and 15-tick entry policy.
- Every regenerated control signal must match the extension record exactly;
  the original unfiltered 4,086-signal baseline must match too.
- The included original engine and matrix files are byte-for-byte unchanged.

## Outputs and interpretation

frequency_summary.csv describes setup fates, trigger filter failures and
first qualifying observations after expiry, split by full/recent/era cohorts.
near_misses.csv counts body-only or distance-only failed trigger attempts
within inclusive 0.10/0.25 ATR margins. The two margins overlap. A setup can
have several attempts; both attempt and distinct-setup counts are provided.
parity.json records the exact control and baseline comparisons.

Post-expiry observations stop on a gap, H1 trend failure, first passing trigger
or bar age 16. Bar age 9 is the first expired bar; the age-12 and age-16 counts
are cumulative. They preserve the original touch and update the inherited low.
A flag identifies observations coinciding with an actual fresh baseline signal.
These observations can overlap; do NOT add them to the original signal count.
No exit outcome, return, win rate or profitability is inferred from them.
A proper longer-lifetime state machine and account replay remains a later step.

Setup cohorts use touch time, followed through the full frozen cutoff. Trigger
cohorts use close time. Era outcomes can therefore occur beyond an era end;
these are descriptive cohorts, not fresh-account or untouched holdout results.
History has already been examined, and original source uncertainty remains.

## Storage and restart

Old sources/caches are read-only. Own raw cache is capped at 64 MiB with a
96 MiB free reserve. At most 128 native AUD/USD M15/H1 cache-miss window requests per run
(up to 384 HTTP attempts including retries); no M1/S5 or conversion downloads. Previously frozen candle checksums
are enforced; a revised window stops the run. A completed diagnostic resumes
from verified report checksums without loading history. An interrupted run
repeats signal generation using its private cache; it has no account replays.
Never delete old research directories to install this package.

A BUSY status means an existing research process still holds the source lock.
Use this entry point after the extension finishes. If a checksum, source or
storage check fails, send /status; do not edit saved research results.

## Local verification

python -m unittest discover -s frequency_tests -v

The package was checked with 17 offline tests, including causal-prefix checks,
exact instrumented/original signal parity, expiry boundaries, source read-only
access, tamper rejection, complete synthetic report creation and safe resume.
No real historical run was performed while building this package.
