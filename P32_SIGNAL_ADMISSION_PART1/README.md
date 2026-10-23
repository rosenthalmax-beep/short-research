# AUD/USD long conditional pullback matrix

This pass regenerates signals and replays GBP accounts for a small joint
matrix. It follows the baseline/single-feature diagnostics and keeps RR fixed.
It does not select or activate a strategy automatically.

## Frozen research design

The nine joint cells combine:

| Parameter | Tested values |
| --- | --- |
| Maximum H1 trend distance, in H1 ATR | 0.75, 1.00, 1.25 |
| Minimum engulfing body, in M15 ATR | 0.50, 0.75, 1.00 |

There are also three distance-only controls, three body-only controls and one
unfiltered baseline: 16 configurations. The controls help separate each
feature's effect from the joint settings. They are research variants, not 16
new live strategies or statistically independent trials.

H1 distance uses the latest completed native H1 CLOSE minus its EMA100 divided
by H1 ATR14. It is not the M15 entry-to-EMA distance. Exact bullish engulfing,
H1 close above rising EMA100, M15 EMA20 pullback, eight-bar expiry, original
stop geometry and observed warm-up requirements remain as in the frozen study.

RR3.0, a 10-tick stop buffer, a separate 15-tick adverse entry allowance and
1% marked NAV risk remain fixed. No RR optimization, session search, daily
regime matrix, frequency-recovery trigger or live32 combination is added.

## Complete conditional signal regeneration

Each setting has its own setup/armed state. A candle that qualifies as an
engulfing confirmation but fails that setting's body or distance filter does
not consume the pullback. A later candle can qualify within the original
expiry. Its stop includes every low from the original touch through the actual
accepted candle. Trend failure, expiry or a M15 gap resets the setup.
An accepted qualification consumes the setup even if execution later rejects
it or a position gate blocks the trade. Filter decisions never inspect future
quotes, exits, trade results or another candidate's state.

Before any new candidate is priced, the unfiltered generator must match all
4,086 saved AUD/USD long first-signal records field-for-field. Native M15
history must match the original path source hash before baseline paths can be
reused. Failure stops the research without changing the source data.

## Price and GBP account model

The exact original V2 price, refinement, funding, margin, conversion and account
modules are included and hash-checked. Native M15 bid/ask opening prices are
entry proxies. Barrier timing uses M1/S5 refinement. Actual live latency and
cancellations are not reconstructed. END_ASC timing and STOP_FIRST ambiguous
barrier handling are conditional assumptions, not mathematical return bounds.

Immutable geometry paths and standalone funding results are shared between
candidates. Signal identity and strategy gates remain candidate-specific. Each
account is fresh GBP100 with one candidate and at most one open position.
Two funding cases are replayed: frozen 14-Oct-2026 terms and adverse6% annual.
Spread and conversion are included once. Historical funding/holiday, margin
and conversion schedules are not independently reconstructed. Rejections,
omissions, unresolved paths and propagated account uncertainty stay visible.

Each variant has 14 accounts: full history, five era accounts and the recent
2021-to-cutoff account, under both funding cases. Total: 224. Era accounts reset
flat; the final era is partial. The first account starts after the same observed
indicator warm-up exclusion. CAGR uses the actual effective duration. Windows
overlap and have all been examined previously; none is an untouched holdout.
These isolated account results do not measure added return in the live32.

Every saved account is checked for cash/P&L/financing consistency and maximum
one open position before being accepted as a reusable checkpoint.

## Plateau definition

The four possible adjacent 2x2 blocks are checked. A single high-profit corner
cannot pass alone. All four members of a region must meet:

- At least 200 full-history and 60 recent closed account trades.
- Positive ending NAV relative to GBP100 in full history and recent years under
  BOTH funding cases.
- Positive mean account net R after removing the five best trades in those
  full/recent funding cases.
- At least 20 closed trades in each frozen-funding era and positive ending NAV
  in at least four of the five eras.

These are declared descriptive screens, not statistical significance or proof
of robustness. Failed sample/era/financing criteria are retained in the output.
Account-lineage uncertainty remains visible even if a region passes; passing
is conditional on that model. Regions overlap and are not independent
candidates. Drawdown, return ranges and minimum trade counts are exposed so
we can review risk and frequency alongside profitability. No setting is
selected automatically and no high-return cell is promoted by itself.

## Reports

`/results-summary.zip` is the small file to upload for review. It contains:

- account_matrix.csv: all completed fresh accounts, costs, frequency, sampled
  drawdown, conditional CAGR and uncertainty flags.
- opportunity_matrix.csv: independent opportunity diagnostics and exclusion
  counts. These are not account returns; rejected counterfactuals are separate.
- candidate_screens.csv: every completed variant's explicit screen results.
- plateau_regions.csv: each 2x2 block, member failures, sample counts, return
  ranges, sampled drawdowns and uncertainty.
- baseline_parity.json, source_provenance.json, broker_reconciliation.json,
  protocol/status/run-state metadata and REVIEW.md.

`/results.zip` streams the full completed-candidate evidence: signals,
standalone opportunities, complete account payloads with cash/trade/financing
ledgers and monthly observations, per-file checksums, one shared unique path
export and the private source index. Opportunity rows link to shared paths by
path_cache_key. This avoids duplicating a full path history for every setting.
Incomplete candidates are excluded from aggregate return/plateau results.
Partial published summaries remain downloadable during a long run.

## Storage and resume

Follow START_HERE.txt. Use the existing mounted /data volume. Outputs and raw
caches go into a NEW matrix directory. The old source files and old caches are
read-only. No cleanup deletes or modifies them. Old caches with a live WAL are
skipped for immutable reuse, and data is fetched with checksum enforcement.

The raw compressed-body budget defaults to 128 MiB. A 96 MiB free reserve stops
the run before filling the volume. Metadata, path checkpoints and reports need
additional space, so this is not a whole-study quota or guaranteed disk fit.
Old bodies may be evicted; source identities/checksums and receipts remain.
Native history is preferred in the cache. A changed previously used source
window stops the run; newly required fine windows are pinned on first use.

The original source index is checked by canonical identities and price checksums
for AUD/USD and GBP/USD. Recompressing the same index or changing cache-residency
flags does not falsely change price provenance. No price checksums are ignored.

Account files checkpoint one by one; candidate COMPLETE markers pin every
finished report. Restart verifies these before reuse. Partial paths commit per
geometry. Completed account results are not silently recomputed or overwritten.
The full archive streams with bounded buffers and cancellation support; no
second large full ZIP is saved on the server. The small summary is stored too.

## Safety and scope

Only historical OANDA candle GETs are supported. No executor URL, webhook,
account-order route or live activation is introduced. The existing broker token
is used solely for historical candles on the pinned live pricing host. The
service HTTP interface supports GET/HEAD and rejects POST. Health/HEAD requests
do not start the worker. Credentials are not written into reports.

Source history remains 2005-to-20-Sep-2026 with the original observed warm-up
exclusion. This does not extend the history or provide results through today.
The first run will re-download evicted candles where exact reuse is unavailable.

## Validation

Run from this package directory:

    python -m unittest discover -s tests -v

Sixteen offline tests cover original-generator parity, H1 causal timestamps,
prefix invariance under future-price changes, gaps, later qualifying triggers,
setup expiry/consumption, independent states, isolated-peak rejection, adjacent
regions, account cash identity, source-revision refusal, bounded cache,
unsupported routes, immutable reports and streamed downloads.

An end-to-end deterministic synthetic fixture exercises all 16 variants with
nonzero generated signals, 224 actual account checkpoints, full publication,
resume without replaying completed accounts, and refusal of a corrupted report.
Synthetic test eras use a short fixture interval; they are functional tests,
not historical performance evidence. The original/regenerated baseline
produces identical full synthetic account payloads.

The real saved source audit verifies five pinned exports, 22,739 relevant
frozen price-window identities/checksums and 4,086 available baseline signals.
Full real-history signal regeneration and matrix replay have NOT run locally:
they require the original candle cache/broker access on your research service.
No real return results are included. See validation/ for test and audit records.
