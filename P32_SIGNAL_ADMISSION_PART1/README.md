# AUD/USD bounded H1-distance-cap test

This is the single-feature follow-up to the reviewed broad resumption pass.
It tests only inclusive H1 distance caps of 0.20, 0.25 and 0.30 ATR.
It creates 112 accounts: five preceding configurations as exact controls, plus
three independently regenerated cap strategies, each in seven reset windows
under two funding assumptions. There is no automatic strategy selection or live
activation and this service cannot submit orders.

The five controls must match all 70 preceding account payloads, exact signals
and opportunities before new cap pricing/account replay. The original 4086
baseline signals and previous pricing/accounting/source modules stay frozen.

| New configuration | Maximum H1 distance |
|---|---:|
| RES2_H1DIST_0P20 | 0.20 H1 ATR14 |
| RES2_H1DIST_0P25 | 0.25 H1 ATR14 |
| RES2_H1DIST_0P30 | 0.30 H1 ATR14 |

Distance is (most recently COMPLETED native H1 close - H1 EMA100) / H1 ATR14.
It uses that H1 close, not the current M15 signal close. The cap is inclusive
and checked at trigger qualification. H1 uptrend, first EMA20-touch setup,
16-bar lifetime and reset rules stay fixed. A failed distance check does not
consume or reset the touch: original touch and subsequent lows are retained
until expiry, gap or trend failure. A later trigger can therefore qualify even
if absent from the broad/looser stream. Each setup and account is regenerated;
this is not selecting trades from the old diagnostic bin or accepted-trade CSV.
The old <0.25 diagnostic bin is not an exact <=0.25 strategy simulation.

All new cases retain the same resumption trigger: completed M15 close > open,
close > previous completed M15 HIGH and close > current EMA20. No exact engulfing
requirement, body floor, previous-close filter or momentum filter is introduced.
RR3, 15-tick adverse entry bound, 10-tick stop buffer, GBP100 whole units and
1% modeled NAV risk stay fixed. Entry proxy, spread, conversion, margin and
path/barrier conventions are unchanged. Funding is frozen 14-Oct-2026 rates and
adverse 6% annually, not reconstructed historical schedules.

Eligibility stays 9 February 2005 12:00 UTC through 20 September 2026.
Missing early warm-up is excluded rather than synthesized. All history was seen;
this local sensitivity study is not an untouched out-of-sample test.

Reports:

- account_matrix.csv: all full, recent and five fresh era accounts under both costs.
- candidate_screens.csv: original sample, era, profitability and best-five checks.
- cap_regions.csv: adjacent 0.20/0.25 and 0.25/0.30 pairs, plus all three caps.
  Every member must pass the unchanged screen for its region to pass. Original
  requirements remain 200 full / 60 recent / 20 per-era trades, full era coverage,
  four profitable supported eras, and full/recent funding/best-five checks.
  Descriptive positivity is separate from that conditional approval screen.
- control_comparisons.csv: every cap vs broad resumption; each tighter cap vs
  its adjacent looser neighbour. Only the H1 distance cap differs in these pairs.
- trade_attribution.csv: removed/new/shared CLOSED ACCOUNT trades in every reset
  window and funding case. Geometry matching is entry/reference/stop/target;
  shared sizing and cash can differ. This is not incremental live32 return.
- signal_overlap.csv: raw qualification time and geometry changes, distinct
  from account admissions and portfolio overlap.
- feature_diagnostics.csv / feature_coverage.csv: unchanged coarse bins on raw
  opportunities, with missing/rejected/unresolved counts visible. These do not
  replay additional feature filters or estimate compounded account returns.
- research_stage.json: three fixed new cap cases and conditional sensitivity scope.

Diagnostic entry cohorts follow raw outcomes to the fixed full-study cutoff;
fresh reset accounts stop at their own window end. Overlapping windows/caps and
feature groups are correlated and cannot be summed as independent evidence.
Original gaps, stale valuations, unknown entries and barrier-order assumptions
remain explicit. Sampled NAV DD is not an intrabar worst-equity bound. Historical
funding, margin and conversion schedules are unknown; current terms are modeled.
Neither fixture reconciliation nor a local screen pass establishes independent
confirmation or combined live32 admission.

Review all three settings, sample support, weak eras, both funding cases,
best-five sensitivity, and NEW/REMOVED trades. If the neighbourhood is not
supported, park this branch. No automatic threshold extension, extra feature
stacking, lifetime search or RR search is part of this package.

The previous resumption run and all SEVEN earlier research ancestors are
verified read-only, alongside the original pullback source. Source-price identity
and module checksums are pinned. Only the three existing nonnegative operational
provenance counters may drift; price, code and history cannot. Stop other research
writers first. Completed accounts resume; partial runs replay missing checkpoints
only and altered controls/corrupt checkpoints fail closed.

The private raw cache defaults to 64 MiB with 96 MiB free-space reserve. Exact
history and quote/path caches are reused where present, and additional geometry
may require historical candle GETs. Existing studies are not automatically
deleted or rewritten. Summary ZIP is capped below 20 MB; full evidence streams
without a second large ZIP on the data volume.

See START_HERE.txt. Offline tests use deterministic synthetic data to check
software; the real historical replay runs on your research service.
