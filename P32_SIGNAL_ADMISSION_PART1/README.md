# AUD/USD bounded body-size confirmation

This is the final bounded body-relaxation check after the reviewed two-factor
study. It holds the six promising close-position by pre-touch momentum settings
fixed and tests whether a smaller engulfing body can recover useful frequency.
It does not activate a strategy or submit orders.

| Variable | Fixed test values |
|---|---|
| Body minimum, M15 ATR14 | 1.00 / 1.125 / 1.25 |
| Previous completed M15 close location cap | 0.40 / 0.50 / 0.60 |
| Original pre-touch four-hour rise minimum | 0 / 0.50 touch ATR |
| Setup lifetime | 16 M15 bars |
| Completed H1 EMA100 distance cap | 1.25 H1 ATR |
| RR / adverse entry bound / stop buffer | 3 / 15 ticks / 10 ticks |
| Account | Fresh GBP100, 1% modeled NAV risk |

There are 18 two-factor cases. Each body also has its unfiltered account, three
close-only accounts and two momentum-only accounts: 36 configurations in total.
Seven reset windows and two financing cases give 504 accounts. The twelve
unchanged 1.25-body configurations run first, and all 168 complete account
payloads must exactly match the completed combination study. Their signals and
opportunities must also match. A mismatch stops the study.

Each case regenerates causal setup state independently. Accepting a smaller
body may consume the setup earlier and displace a later original-body entry.
Therefore, smaller bodies need not produce a superset of signals or closed
account trades. A failed filter retains the original touch and all subsequent
lows; acceptance consumes the setup before pricing or account admission.

Previous close location is (close-low)/(high-low) for the completed M15 candle
before the signal. A flat candle has no usable value. Pre-touch momentum uses
the close immediately before the ORIGINAL first EMA20 touch minus the close
sixteen M15 bars earlier, divided by ATR at that touch; gaps make it missing.
These definitions and all prior recorded features remain unchanged. Native H1
candles are only used once complete. Eligibility is 9 February 2005 12:00 UTC
through the unchanged 20 September 2026 cutoff; early missing warm-up stays
excluded rather than being filled with synthetic or future observations.

Reports:

- account_matrix.csv: all full, recent and five fresh era accounts.
- control_comparisons.csv: same-filter original body comparisons, plus each
  combination against its same-body unfiltered and both single-filter controls.
- trade_attribution.csv: removed, new and shared CLOSED ACCOUNT trades for every
  comparison, window and funding case. Identity is entry/reference/stop/target;
  shared trade sizing and cash can change with account history.
- frequency_recovery.csv: counts, qualifications, annual frequency, returns,
  sampled NAV DD and best-five sensitivity vs the same-filter 1.25-body case.
- plateau_regions.csv: six adjacent 2x2 close/momentum blocks within bodies and
  four adjacent-body 2x2x2 blocks. Every member must pass for a region to pass.
- candidate_screens.csv: unchanged 200 full / 60 recent / 20 per-era samples,
  era coverage, positivity under both funding cases and best-five sensitivity.

Recovering count alone is not success. Inspect the newly admitted and displaced
trades, weak eras, both funding cases and nearby settings. No thresholds are
lowered and no winner is selected automatically. After reviewing this bounded
confirmation, freeze this family and move to the separate resumption-trigger
hypothesis; there is no third-filter, lifetime, distance or RR search here.

All history has been examined. Overlapping settings and windows are correlated
and cannot be summed or treated as independent validation. All results are
conditional standalone accounts, not incremental live32 portfolio returns.
Original source gaps, stale marks, ambiguous barrier timing and unknown entries
remain explicit. Sampled NAV DD is not a worst intrabar equity bound. Actual
historical funding, margin and conversion schedules are not reconstructed;
frozen 14-Oct-2026 rates, adverse 6% annual funding and current terms are models.
Saved broker reconciliation verifies fixture mechanics, not all historical terms.

All existing pricing/accounting and predecessor Python/JSON files are unchanged.
The preceding combination outputs and all five earlier report/source ancestors
are verified read-only and locked against running writers. Only the existing
three named nonnegative work counters may drift in reviewed provenance; code,
price evidence and history stay locked. Completed accounts resume, partial runs
replay missing checkpoints only, and corrupt checkpoints fail closed.

The private raw cache defaults to 64 MiB, with a 96 MiB free-space reserve. Prior
native history and exact quote/path caches are reused where available. New
geometry may need additional read-only candle GETs. No original source is
rewritten or automatically deleted; the summary archive stays under 20 MB and
full results stream without making a second large ZIP on the volume.

See START_HERE.txt. Offline tests use deterministic synthetic data to check the
software; the real historical replay takes place on your research service.
