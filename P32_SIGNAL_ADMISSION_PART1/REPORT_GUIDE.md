# How to assess the entry-limit experiment

Begin with RUN_COMPLETE.json and control_checks.json. Every selected 10-tick
control must pass. Do not compare alternative policies after a control error.

policy_comparison.csv compares all lanes. Use END_ASC / FROZEN_14OCT2026_RATES
as the predeclared primary lane, then check robustness to chronology and adverse
financing. Do not select a policy solely on the highest historical CAGR.

Compare conditional CAGR, sampled liquidation-equity drawdown, worst full year,
worst and median rolling 12-month return, trade frequency and financing. The
monthly arithmetic average differs from the compounded monthly growth rate.
annual.csv flags incomplete calendar years; 2026 is partial. rolling12.csv uses
only twelve complete calendar months. Monthly NAV is the last causal observation
available by month end, carrying flat account equity when there are no events.
It is not an exact broker month-end valuation.

grouped_trade_stats.csv gives net win rate, mean R, profit factor R and source
uncertainty by pair, strategy, entry band, entry year and before/after 2021.
These overlapping diagnostic groups must not be summed together. Closed-trade
statistics exclude still-open positions. Their cumulative R is not CAGR and
their cash profit factor is affected by compounding.

admission_changes_primary.csv identifies ADDED, DISPLACED and COMMON admissions
against the matching 10-tick account. The cash columns in policy_comparison.csv
reconcile the final balance difference to added trade cash, displaced trade
cash and common-trade resizing/compounding. They include daily financing on
positions still open. Any difference in ending unrealised P/L is reported
separately. This path-dependent cash decomposition is not independent marginal
alpha. A raw entry-limit pass does not guarantee admission after other gates.

primary/<policy>/ contains ledgers, trades, cash, financing and open positions.
evaluated_paths_primary.csv contains candidates actually evaluated in the
primary account, including candidates evaluated in earlier lanes for the same
policy. It does not invent paths for candidates blocked in every account.
/details lists complete per-lane records including their evaluated paths.

new_execution_source_differences.csv and valuation_source_differences.csv keep
price disagreement explicit. Unresolved observed barriers remain conditional
account assumptions; unknown entries are omitted and later lineage is flagged.
Equity is sampled at M15/event times; observed drawdown is not a guaranteed
intrabar maximum. Native entry candle opens are estimates without live latency
or price-bound cancellation reconstruction. Frozen funding is not historical
funding, and the 2021-onward diagnostic has already been examined. No historical
result proves a future return or justifies a live cap change automatically.
