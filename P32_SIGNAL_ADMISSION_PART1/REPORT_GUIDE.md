# Read this first — conditional GBP accounting

Start with RUN_COMPLETE.json and broker_reconciliation_summary.json. Check
every lane's status in gbp_scenario_summaries.json before comparing returns.
COMPLETE_WITH_LIMITATIONS does not mean a fully validated historical portfolio.
A stopped lane has no whole-period CAGR. All curves retain source/admission
uncertainty and frozen account-term assumptions.

The default sixteen lanes cross four existing chronology assumptions with
zero financing, frozen 14-Oct-2026 rates, adverse 3% and adverse 6% annual
base-notional funding. No best lane is selected. The zero lane is an accounting
control; the snapshot is not a historical rate series. Historical rates are
used only if an explicit sourced funding_rates.csv was supplied, and a missing
required rate stops that lane. Verify historical_funding_coverage.csv.

gbp_scenario_comparison.csv shows the combined effect of changing financing,
units, available margin and admissions. It is not a fixed-trade fee subtraction.
gbp_admission_ledger.csv retains all reached raw qualifications, rejected
entries, margin failures and gate blockers. Additional paths may be computed
for candidates newly allowed after a margin rejection. Unknown entries remain
omitted with subsequent ACCOUNT-wide lineage flagged.

gbp_closed_trades.csv and gbp_financing_events.csv show whole units, risk
budgets, cash P/L, conversion estimates and charges/credits. Financing posted
at rollover is already in the cash balance: it is attributed to a closed trade
for reporting but is not debited again at exit. The conversion component is
already included. Bid/ask spread is also already included in fill/exit prices.
Net wins are positive net cash outcomes including that trade's financing.
gbp_cash_events.csv allows the balance identity to be checked independently.
Group statistics contain CLOSED trades only, and are not optimization targets.

The model starts at GBP 100 by default. It uses 1% modeled current mid NAV,
.75% for EUR_JPY_M15_SHORT, executor worst-entry-bound sizing and whole units.
Saved broker cash, conversion factors and initial margin reconcile, but exact
live quote-time sizing/NAV does not: original preview conversion snapshots are
not available. The 1% symmetric home conversion-factor markup is inferred from
saved fill factors. It is applied once to non-GBP modeled cash conversion;
broker conversion-cost diagnostics are not separately subtracted.

Current instrument margin rates and limits are applied throughout historical
prices, not represented as historical rules. There is no invented position cap.
Mid NAV and sided liquidation equity are separate. Sampled mid/liquidation
drawdown uses M15 opens plus entry, exit and financing events; it is not the
true intrabar maximum. Margin closeouts use that same sampling and modeled
liquidation quotes, not an independently reconstructed broker closeout event.

Valuations use only reached M15 opens or earlier completed M15 closes. Old
quotes can be carried for at most seven days when missing, flagged when older
than 15 minutes outside the conventional weekend. This is causal but still
uncertain. Missing essential quotes/unpriced exits stop a lane. No automatic
gap repair or holiday certification is performed. The original 170 unresolved
paths and 11 unknown entries remain explicit; their effects can alter later
account-wide risk sizing. All START/END/ASC/DESC outcomes are assumptions,
not proven return bounds or actual broker returns.
Any additional both-barrier ambiguity uses a flagged STOP_FIRST assumption.
Cash rounding is modeled at four decimals per posting; unobserved rounding
edge cases are not independently validated against the broker.

gbp_monthly_observations.csv describes first/last AVAILABLE event observations
and minima/maxima in each observed month; it is not a zero-filled monthly
return series and does not invent absent month-end marks. /equity.zip contains
full M15/event samples for the prespecified END_ASC zero/snapshot controls.
/sources.zip contains new raw MBA source blocks and references the five seed
ZIPs plus the bundled admission additions. Source hashes, input pins and
report_manifest.json support independent reproduction.

Broker evidence is frozen at 14 October 2026 16:15:05 UTC. The signal window
remains 1 January 2005 to 20 September 2026 18:29 UTC. These data include the
previously developed strategy history; they are not a new out-of-sample test.
The live probe/executor is not contacted or modified; no order route exists.
