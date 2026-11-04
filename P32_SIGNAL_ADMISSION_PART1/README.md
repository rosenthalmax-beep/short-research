# AUD/USD broad pullback-resumption study — pass 1

This opens the distinct resumption-trigger hypothesis after freezing the
engulfing/body-relaxation branch. It tests the broad mechanism before applying
new filters. It is historical research only and cannot submit orders.

Five configurations x seven reset windows x two funding cases = 70 accounts.

| Configuration | Purpose |
|---|---|
| L16_D1.25_B1.0 | Unchanged broad engulfing benchmark with 1.0 ATR body floor |
| L16_D1.25_B1.0__previous_close_location_0 | Unchanged single-filter research reference |
| L16_D1.25_B1.25__C2_M1 | Unchanged higher-quality two-filter benchmark |
| RES1_MATCHED_BROAD_ENGULF | Broad engulfing with no body-size floor |
| RES1_BROAD_PRIOR_HIGH_CLOSE | Broad prior-high resumption, matched context |

The three old benchmarks run first. All signals, opportunities and 42 complete
account payloads must match the reviewed body-recovery results exactly before
new mechanisms run. The original 4086-signal baseline must also reproduce.
Old Python/JSON source and pricing/accounting modules are unchanged.

The primary comparison is RES1_BROAD_PRIOR_HIGH_CLOSE against
RES1_MATCHED_BROAD_ENGULF. Both use the SAME context and setup:

- Most recently completed native H1 close above EMA100, with EMA100 above its
  value three observed H1 bars earlier; original freshness and gap conventions.
- H1 distance from EMA100 capped at 1.25 H1 ATR.
- M15 EMA20 first-touch setup after arming, all subsequent lows retained.
- Setup expires after the unchanged 16-bar lifetime.
- No minimum body size, previous-close cap or momentum filter.
- RR3, 15-tick adverse entry bound, 10-tick stop buffer and 1% modeled NAV risk.
- Original GBP100 whole-unit account, spreads, conversion, margin and funding.

Resumption qualifies when a COMPLETED M15 candle has close > open,
close > the preceding completed M15 HIGH, and close > current EMA20.
All inequalities are strict. Exact engulfing and a previous close below EMA20
are not required. The matched broad engulfing trigger retains the exact old
bullish body-engulf definition and close > current EMA20, without a body floor.
This keeps the trigger comparison separate from removing that old floor.

Each case regenerates its own causal state. Failed context/filter checks keep
the original touch and intervening lows until expiry, gap or trend failure.
Qualification consumes the setup before execution rejection or account blocking.
Different triggers can consume earlier, displace entries and change stop
geometry; neither raw signals nor accepted trades are assumed to be supersets.
Reference is the completed signal close and fills use the unchanged native
next-open proxy with the existing sided stop/target and M1/S5 refinement.

The observed warm-up start remains 9 February 2005 12:00 UTC, with the unchanged
20 September 2026 cutoff. Excluded early history is not synthesized. Funding
cases are frozen 14-Oct-2026 broker rates and adverse 6% annual assumptions,
not actual reconstructed historical schedules.

Reports:

- account_matrix.csv: full, recent and five fresh era accounts, under both costs.
- opportunity_matrix.csv: independent raw opportunities, not compounded accounts.
- control_comparisons.csv: MATCHED_TRIGGER isolates the trigger change; other
  comparisons are labeled legacy/body-floor benchmark comparisons.
- trade_attribution.csv: removed/new/shared CLOSED ACCOUNT trades for every
  comparison, reset window and funding case. Shared sizing/cash can differ.
- signal_overlap.csv: raw qualification time/geometry overlap, distinct from
  account admissions and live32 portfolio overlap.
- feature_diagnostics.csv: predeclared coarse feature bins on raw independent
  opportunities, with missing/rejected/unresolved/nonclosed counts explicit.
- feature_coverage.csv: each feature's full raw qualification denominator.
- candidate_screens.csv: unchanged 200 full / 60 recent / 20 per-era samples,
  era coverage and full/recent best-five/funding sensitivity.
- research_stage.json: no parameter grid or plateau test in this pass.

Feature bins are descriptive. They DO NOT replay filters, regenerate filtered
setup state, estimate account CAGR or automatically select thresholds. Outcomes
follow to the fixed full-study cutoff, even when an entry cohort ends earlier.
This differs from the fresh reset accounts, whose paths stop at their window
end. Features use completed candles and original touch state only. Inspect broad
mechanism quality and support first; only then justify a bounded single-feature
account study, interactions and neighbouring plateau checks. Do not carry the
old winning close/momentum filters into the new trigger automatically.

All history has been examined. Overlapping configurations and windows are
correlated. Actual historical financing, margin and conversion schedules are
unknown; current terms and modeled rates are assumptions. Original candle
gaps, stale marks, barrier-order assumptions and unknown-entry omissions remain
explicit. Sampled NAV DD is not worst intrabar equity. Saved broker fixture
reconciliation and a screen pass do not establish independent validation or
combined live32 admission. No orders, selection or live activation occur here.

The completed body-recovery run and all SIX earlier research ancestors are
verified read-only, including source-price identity. Running predecessor writers
are rejected. Only the already-defined three nonnegative provenance work
counters may differ; history, price evidence and code remain locked. Completed
accounts resume and partial runs replay missing checkpoints only; altered
controls/corrupt checkpoints fail closed.

Private raw cache defaults to 64 MiB, with 96 MiB free-space reserve. Exact prior
history and quote/path evidence are reused when available. Broad signals can
require additional read-only candle GETs; no source is automatically deleted.
Summary ZIP is capped below 20 MB and full results stream without a second large
ZIP on disk. Stop other active research writers first and keep mounted /data.

See START_HERE.txt. The included offline tests use deterministic synthetic data
to check software; the real historical run takes place on your research service.
