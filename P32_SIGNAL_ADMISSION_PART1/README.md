# AUD/USD bounded conditional combinations

This study follows the reviewed 79-setting one-feature run. It tests ONLY the
previous-candle close cap by pre-touch four-hour momentum interaction on the two
quality anchors. Existing engine and predecessor modules are unchanged.

| Anchor | Setup lifetime | H1 distance cap | Body minimum |
|---|---:|---:|---:|
| L16_D1.25_B1.25 | 16 M15 bars | 1.25 H1 ATR | 1.25 M15 ATR |
| L24_D1.0_B1.25 | 24 M15 bars | 1.00 H1 ATR | 1.25 M15 ATR |

For each anchor: one unchanged base, three close-only settings (<=0.40/0.50/0.60),
three momentum-only settings (>=0/0.50/1.00 ATR), nine two-factor combinations.
Total: 32 candidates x seven account windows x two funding cases = 448 accounts.
The 14 unchanged controls run first; all signals, opportunities and 196 complete
saved account payloads must match the reviewed previous study. A control failure
stops the run before pricing/replaying any new combination.

The previous candle is the completed M15 candle before the engulfing signal;
its close location is (close-low)/(high-low), with flat candles missing. Momentum
is the close immediately before the FIRST EMA20 touch minus the close sixteen
M15 bars earlier, divided by ATR14 at that touch. Missing/gapped histories reject
that feature. All previously recorded signal feature fields are preserved.

Each candidate has an independent setup state. Failure of either filter leaves
the original touch and all subsequent lows intact until expiry, gap or H1 trend
failure. A qualifying candle consumes the setup even if the executor proxy or
account subsequently rejects it. This regenerates signals instead of intersecting
old accepted-trade CSVs. Later qualifying candles and admission can differ.

Fixed RR3; GBP100 fresh accounts; 1% current modeled NAV risk; 15-tick entry
limit; separate 10-tick stop buffer; original whole-unit sizing, margin,
conversion, spread, funding and barrier conventions. Financing scenarios are
frozen 14-Oct-2026 broker rates and adverse 6% annual assumptions, not historical
rate reconstruction. The start eligibility and source cutoff remain unchanged.

Read account_matrix.csv and control_comparisons.csv to assess full/recent
accounts and five fresh era resets. Comparisons include both single filters
as well as the unfiltered anchor. Do not sum overlapping time windows.
trade_attribution.csv partitions closed trades into removed/new/shared groups
for every comparison, every account window and both funding cases. Signal IDs
differ between new configurations, so identity matching uses entry time,
reference, stop and target. Shared trades can have different sized cash outcomes.

Eight adjacent 2x2 plateau regions are reported. For a region to pass, all four
members must pass the unchanged full/recent/era, sample and best-five screens.
Descriptive profitability and fragility flags are reported separately from that
approval screen. All history has been examined; neighbours are correlated.
A stable region is sensitivity evidence, not independent OOS confirmation.

The original gap, stale-mark, barrier-order and unknown-entry uncertainties
stay explicit. Sampled NAV drawdown is not an intrabar bound. Fresh-account
comparisons are not incremental live32 portfolio returns. No automatic strategy
selection, live orders or combined portfolio admission happens here.

Original study outputs are read-only and locked against running writers. Prior
source checksums are preserved, with the already-defined narrow exception for
three nonnegative work counters in reviewed provenance. Code, history, price
geometry and accounting assumptions cannot change under that exception.
Completed accounts resume; the private raw cache defaults to 64 MiB with a
96 MiB free-space reserve. Existing M15/H1 history and exact quote/path caches
are reused when present. Missing evidence stops or uses read-only candle GETs
subject to the frozen expected hashes; no MID history regeneration is requested.

After review, perform one bounded local confirmation only if justified. The
separately defined pullback-resumption trigger remains the next distinct research
hypothesis afterwards. Do not reopen RR or add a third filter in this study.

See START_HERE.txt for deployment. Offline tests are synthetic software checks;
the actual historical replay runs on the research service.
