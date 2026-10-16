# P32 GBP account replay update — 16 October 2026

Upload this update to the SAME SEPARATE RESEARCH/AUDIT SERVICE that ran the
warm-up/gap audit. Extract this outer ZIP locally and upload all its contents,
replacing matching files. Keep the five existing seed_01.zip through seed_05.zip
files, your existing data directory/volume and environment variables. Keep all
inner input ZIPs zipped. Do not delete the existing files first.

Start command remains `python app.py`. Python 3.10+ and system timezone data
are required; no third-party Python packages are required.

Keep these settings:

| Variable | Value |
|---|---|
| OANDA_TOKEN | Existing token, stored in service variables only |
| OANDA_ENV | live |
| P32_SIGNAL_OUTPUT_DIR | /data/p32_signal_admission, on the existing persistent volume |
| P32_SIGNAL_OFFLINE | Unset for the historical run |

No account ID, executor URL, probe URL or webhook secret is required. This
update uses the saved 14 October broker records included with the package.
Its only network capability is historical candle GETs for the nine approved
portfolio instruments. The live executor and strategy probe are not contacted.

1. Check that the service has a persistent volume mounted at `/data`. Keep
   P32_SIGNAL_OUTPUT_DIR pointed at the previous data directory. `/health`
   reports its containing mount and a marker after the run starts; neither is
   a guarantee that a hosting provider will preserve it across redeployment.
2. Deploy the extracted files with the existing five seed ZIPs still present.
3. Open `/health`; service must be `P32_GBP_ACCOUNT_REPLAY_V1_2026_10_16`.
4. Open `/status` once to start. Refresh this address to follow the run.
   Progress includes the scenario, financing mode, qualifications processed,
   valuation quote windows and the last real progress timestamp.
5. When `result_ready` is true, download `/results.zip` and send that ZIP back.
   Its filename is `P32_GBP_ACCOUNT_REPLAY_RESULTS.zip`. Download `/equity.zip`
   too if requested during review. `/sources.zip` is the larger raw price
   archive, ready after packaging; keep it or keep the persistent data volume.

The code update is under 25 MB. Results, full equity samples and raw sources
are separated so you do not need to upload the larger source archive initially.
No warm-up regeneration or MID-history download is repeated. The 3,629 raw
qualifications and 3,506 saved price paths are pinned to your completed warm-up
result. The original 170 gap-qualified paths and 11 unknown entries remain
explicit. Additional price paths are computed only if changed margin admissions
allow a qualification that was previously blocked.

The first run may still require substantial new MBA history: overlapping open
positions and their GBP conversions need valuation prices between entries and
exits. Fourteen-day requests are cached; later scenarios and restarts reuse them.
All finished account scenarios are checkpointed. Keep the data directory.

## What the default run does

* Reconcile 82 arithmetic checks against the supplied broker evidence: ten
  openings/closed outcomes, four daily financing allocations, quote P/L,
  home conversion and account balance changes. These do not prove exact live
  preview/NAV sizing; the preview conversion snapshots are unavailable.
* Replay all raw signals under the four existing START/END, ASC/DESC orderings.
* For each ordering, run zero financing as a control, frozen 14-Oct rates as
  a sensitivity, and adverse annual base-notional rates of 3% and 6%.
* Start at GBP 100; risk 1% of modeled current NAV, except EUR_JPY_M15_SHORT
  at .75%; round down to whole units from worst-bound stop risk.
* Use recorded instrument margin rates and order limits. Apply nonhedging
  pair and per-strategy gates again from scratch. No six-position cap exists.
* Revalue positions/GBP conversions on causal M15 observations, at entry,
  modeled exit and New York rollover. Track mid NAV, executable liquidation
  equity, sampled drawdown, margin rejections and sampled closeouts separately.
* Keep cash financing, conversion components and net trade outcomes auditable.

The observed symmetric conversion-factor markup is 1% in the supplied broker
records. It is inferred from those records rather than copied from a generic
website example. Historical modeled cash conversion applies that frozen
markup once around chained historical GBP mid-rates; GBP itself has no
conversion markup. It is not a recovered historical broker fee schedule.
The broker's reported conversion-cost diagnostic is not debited again.

Snapshot/3%/6% lanes use the saved instrument-specific weekday multipliers;
USD/CAD's saved triple day differs from most pairs. Historical holidays are
not reconstructed by that schedule. The saved actual USD/JPY financing also
contains an inferred six-day rollover, reported in the reconciliation summary.

## Historical funding coverage

There is NO default claim of historical daily funding rates back to 2005.
`historical_funding_coverage.csv` identifies potential pair/rollover dates and
what has been supplied. Do not manually fill thousands of blanks with zero.
Review this coverage before sourcing the next historical rate dataset.

An optional `funding_rates.csv` activates a fifth lane, HISTORICAL_FILE, under
each chronology. A missing rate stops that lane and suppresses whole-period
CAGR; it never inherits today's rate or a zero value. The included
`funding_rates_TEMPLATE.csv` is intentionally header-only and is NOT loaded.
Use these columns when an appropriately sourced rate dataset is available:

  ny_date,pair,long_rate,short_rate,charged_days,source,basis,rate_basis

Rates are signed ANNUAL decimal rates on BASE-currency notional, e.g. -0.03
means a 3% annual debit, not -3. Charged_days is separate (including actual
holiday multipliers). A rate already multiplied for weekend days must first
be normalized; do not multiply it twice. Basis must be FINALIZED_DAILY_RATE
and rate_basis must be ANNUAL_ONE_DAY_BASE_NOTIONAL. Source must identify the
archived evidence. The importer checks shape/units, not the truth of that source.
The NY date is the local funding date, not the UTC calendar date. Weekend
NY dates are treated as non-posting days for these FX scenarios.

## Limits to read before interpreting returns

These are CURRENT-TERMS, HISTORICAL-PRICE scenarios. The 14 October margin
rates, order limits and conversion assumptions are applied across 2005–2026;
historical regulatory/account changes are not reconstructed. The rate snapshot
is likewise a sensitivity and includes financing credits as well as debits.

Model risk/margin NAV uses mid prices; a second curve uses long bid/short ask
liquidation prices and sign-dependent home conversion. Broker cash and entry
margin are reconciled, but live preview NAV is not exactly reconstructed.
Valuation/conversion prices use reached M15 opens or earlier completed M15
closes, never a future high, low or close. Off-grid exits can therefore use
a conversion observation up to 15 minutes old. Missing observations can carry
an earlier quote for at most seven days, explicitly flagged when older than
15 minutes outside the conventional weekend. No interpolation or fabricated
price is inserted. No earlier quote within that limit stops the scenario.

Drawdown and margin closeouts are SAMPLED at M15 plus events. Intrabar worst
equity, exact liquidation timing/fills, simultaneous real broker ordering,
historical liquidity/latency and price-bound cancellations remain unknown.
Same-timestamp order is existing exits, rollover, mark, then ordered signals;
a newly entered immediate-exit trade closes after its own entry.

Unknown entry cases are omitted and contaminate subsequent ACCOUNT-wide
sizing lineage. Gap-qualified paths keep their observed-exit assumption and
likewise contaminate account-wide NAV. They are never promoted to resolved.
START/END and ascending/descending lanes are assumptions, not return bounds.
Any additional both-barrier ambiguity uses an explicit STOP_FIRST assumption;
it does not establish a lower bound on later portfolio returns.
Unpriced observed exits or unavailable essential quotes stop a lane.

Closed-trade win rates include that lane's modeled financing; open positions'
cash financing is already in balance but is not called a closed trade.
No extra spread is subtracted: bid/ask is already in the modeled fills.
Commission is zero in the supplied records, so no additional commission is
assumed. Taxes, deposits, withdrawals, unused-margin interest and GSLOs are
outside this model. No strategy search or live-risk change is performed.
Modeled cash is rounded to four decimals per posting with Python round;
unobserved broker rounding edge cases are not claimed to be reproduced.

## Optional settings and offline checks

P32_GBP_INITIAL_BALANCE: default 100; a changed balance makes separate checkpoints.
P32_GBP_OUTPUT_DIR: optional directory override; otherwise the existing
P32_SIGNAL_OUTPUT_DIR is retained.
P32_GBP_PACKAGE_SOURCES: default 1; set 0 to omit the optional larger source ZIP.
P32_SIGNAL_REQUESTS_PER_SECOND: default 4; max 5, as in the previous runner.

    python -m unittest test_gbp
    python app.py --reconcile-only
    python app.py --run --offline

The last command requires all needed quotes already saved; it does not turn
missing history into assumed data. Run all tests without credentials. Production
historical downloads/account replays are performed on your research service,
not claimed as already completed in the delivery validation.
