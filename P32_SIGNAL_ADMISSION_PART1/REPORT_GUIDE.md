# Reading the fresh GBP100 account comparison

Every row carries its window, policy, event-order and funding assumption.
Original accounting controls are separate from the fresh-window controls.
Results remain conditional; no automatic selection or live modification occurs.

## Start here

1. `RUN_COMPLETE.json`: 16 controls passed; 864 fresh-account lanes and 864
   accounting audits completed for the default experiment. Check
   `capacity_affected_window_lanes` rather than assuming capacity effects vanished.
2. `recent_comparison.csv`: all 48 lanes of the 2021-to-cutoff fresh account.
   END_ASC / FROZEN_14OCT2026_RATES is the primary lane for each policy.
3. `rolling_window_summary.csv`: 17 overlapping five-year windows per policy /
   chronology / funding combination. Median, worst and best liquidation-equity
   CAGR; matched deltas vs 10 ticks; windows beating control; worst sampled
   drawdown; trade frequency; any capacity restrictions. These windows overlap;
   do not treat their count as independent observations or derive p-values from it.
4. `window_comparison.csv`: every window's full account statistics, mid-NAV and
   executable-side liquidation returns, closed-trade statistics, funding,
   admission changes, uncertainty, and maximum-unit rejection counts.

Both midpoint NAV CAGR and liquidation-equity CAGR are supplied. The latter
allows for the bid/ask side of ending open positions and the conversion markup;
it does not book a forced closing fill. The original trade sizing still uses
midpoint NAV, exactly as in the pinned accounting replay.

`delta_liquidation_cagr_pp` is policy minus 10-tick CAGR for the same window,
chronology and funding mode. Positive favours the candidate on return.
`delta_drawdown_pp` is candidate signed drawdown minus control signed drawdown:
negative means the candidate has a deeper drawdown. Also inspect cash/NAV return
deltas, trade counts and risk utilisation instead of judging one CAGR alone.
There is no single long-run compounded CAGR across fresh overlapping accounts.

`added_closed_*` describe newly admitted trades that actually closed in that
window. `added_resolved_closed_*` restricts them to CLOSED_RESOLVED price paths;
this does not remove uncertainty inherited from earlier account decisions.
`added_closed_uncertain_account_lineage` counts that inherited uncertainty.
Mean R using actual stop risk and mean R using the allocated entry risk budget
are different measures; wider worst-bound sizing can reduce risk utilisation.

## Detail tables

- `annual.csv`, `monthly.csv`, `rolling12.csv`: within-window periods. Partial
  periods carry flags. Zero-duration boundary months are not complete periods.
  Do not pool overlapping windows or call them independent calendar-year results.
- `grouped_trade_stats.csv`: pair, strategy, entry-deviation band and entry year,
  for every lane. These are diagnostic groups, not fitted per-pair policies.
- `admission_outcomes_primary.csv`: ADDED and DISPLACED admissions vs 10 ticks
  for primary lanes, with realised outcome or explicit OPEN_AT_WINDOW_END.
  Closed win rates exclude still-open trades. Displaced outcomes come from the
  control account; added outcomes come from the candidate account. They are
  path-dependent accounts with different sizing and blocking, not isolated alpha.
- `evaluated_paths_primary.csv`: entry evidence and censored window paths.
  `window_truncated` means the full-history exit cannot be booked in this window.
  Original kind/status fields are provenance only, never a window trade outcome.
- `primary/<window>/<policy>/`: admission ledger, closed trades, cash events,
  financing and end-open positions for END_ASC / frozen-funding lanes.
- `window_account_audits.csv`: cash identity, financing counted once across closed
  and open positions, admitted = closed + end-open, and temporal bounds.
- `control_checks.json`: full-history control summary and all ordered accounting
  tables compared to the pinned original fingerprints; fail stops the study.
- source-difference CSVs, request receipts, broker reconciliation and persistence
  evidence retain the prior source policy and limitations.

Large full account checkpoints are separate: open `/details`, then select a
`.json.gz` URL. Every lane's ledger, trades, cash, financing, open positions,
monthly observations and evaluated paths are available. Primary equity streams
are separate `.csv.gz` downloads listed at `/equity`. They include causal M15 and
account-event observations. No future-bar high/low/close is used at an opening
valuation time. Intrabar worst equity and actual margin-closeout fills remain
unknown. Prior experiment reports remain in the prior persistent directories.

## Interpretation

Prefer a broad improvement over the matched 10-tick controls across windows
and funding assumptions, with a drawdown increase you understand. Keep capacity
flags, open end positions, unknown entries and qualified paths visible.
Do not select a best historical start date, fit more thresholds, interpret
positive overlapping-window counts as independent proof, or quote modelled
returns as a promise. Live rejected-signal outcomes and executable prices remain
necessary evidence about latency and broker price-bound cancellations, which
historical candle-open proxies do not reconstruct.
