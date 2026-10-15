# P32 all32 signal recovery and admission replay

This is the next historical research pass. It regenerates qualifications from
the **current locked Python rules for all 32 strategies**, before checking open
positions, then replays the current admission gates with corrected execution.

**Deploy only to the separate research/audit service used for the previous two
audits. Keep the live executor and strategy probe in their existing services.**

## Setup

1. Extract the outer `P32_SIGNAL_ADMISSION_READY.zip`.
2. Upload the extracted contents to the research service's application root,
   replacing that research application's previous files. All of these are
   required together:

   ```text
   app.py
   history.py
   signals.py
   admissions.py
   execution.py
   locked_rules.py
   m15_adapter.py
   reference_payload.py
   rules_provenance.json
   seed_01.zip
   seed_02.zip
   seed_03.zip
   seed_04.zip
   seed_05.zip
   requirements.txt
   Procfile
   ```

   Keep **all five seed ZIP files zipped**. The first four are unchanged from the
   preceding package. The fifth adds the 181 candle blocks from your completed
   execution replay. The tests, fixtures and documentation can be uploaded too.

3. Keep/set the following research-service variables:

   | Variable | Value |
   | --- | --- |
   | `OANDA_TOKEN` | Your existing token, stored in the hosting variables |
   | `OANDA_ENV` | `live` |
   | `P32_SIGNAL_OUTPUT_DIR` | `/data/p32_signal_admission` on your existing `/data` persistent volume |

   Leave `P32_SIGNAL_OFFLINE` unset. This program does not use the executor/probe
   URLs, webhook secret or account ID. The previous output-directory variables
   are ignored. The OANDA token aliases supported by the preceding runner still
   work. No account changes or order calls are supported.

4. Keep the start command:

   ```text
   python app.py
   ```

5. Deploy and open the research service's `/health`. Confirm:

   ```text
   P32_SIGNAL_ADMISSION_V1_2026_10_15
   ```

6. Open `/status` to start. Refresh that page for progress. Phases include saved
   candle import, continuous native history downloads, raw signal recovery,
   chronological admissions and result/source packaging.

7. When status becomes **`COMPLETE_WITH_LIMITATIONS`**, download `/results.zip`
   and send **`P32_SIGNAL_ADMISSION_RESULTS.zip`** back for review.

This pass needs continuous history, including bars where no trade was taken.
It will take longer than the preceding replay; its workload is not fixed at
6,782 rows. History is cached per native block, signals per completed pair and
execution paths per candidate. With the persistent directory retained, a
restart reuses completed work. Refreshing `/status` does not start another
concurrent job. `/health` does not start a job.

Keep the persistent cache and the five supplied seed files. The separate
`/sources.zip` download contains the continuous MID history and newly downloaded
execution evidence; it can be substantially larger than the report ZIP. It is
available for reproduction or later source inspection. You can initially send
only the results ZIP.

If status is `ERROR`, download the available result ZIP and send it for diagnosis.
Missing seed/module errors mean the complete package was not uploaded. HTTP
401/403 means the token was not accepted. Never paste the token in chat.

## What is recovered

- All 17 H1 and 15 M15 locked strategies across the nine approved pairs.
- Native MID H1/M15 bars, with native H4/D inputs for the existing filters.
  H4 is used as an input to existing strategies; this adds no H4 strategy.
- Each qualification's identity, trigger, reference/stop/target/RR and diagnostic
  witness, before pyramiding or opposite-position filtering.
- Qualifying opportunities absent from the old accepted trade book, including
  those previously hidden by a position remaining open.
- Explicit reconciliation against the original 3,391 accepted trades and the
  456 available EUR/USD isolated-path records. It does not force a match.

The period remains **1 January 2005 to 20 September 2026 at 18:29 UTC**. Native
H4/D requests align to NY 17:00 and may start at the preceding aligned boundary.
Signals are retained only within the stated period and after their native bar
closes. There are no synthetic holiday/closure bars or unseen future closes.

## Important definition of “recovered”

The runner regenerates the supplied **current predicates on observed bars**.
It cannot recover signals hidden by missing historical observations. It also
does not assume that old research software and live rolling fetch windows had
identical indicator initialization.

This pass uses a fixed full observed-history SMA seed for EMA/RMA, with EUR/CHF's
special TR1..14 seed preserved. Predicate thresholds, filters, sessions, RR and
trigger priority are unchanged. Differences from the old archive are reported
for inspection; additional or missing qualifications are not silently edited
to obtain the old counts.

M15 higher-timeframe filters use the previous candle eligible at the **M15
signal open**, completed at the next actual higher-timeframe open, matching the
supplied probe's causal rule. H1 daily filters use the previous NY session.

## Admission scenarios

The account snapshot supplied on 14 October reported `hedgingEnabled=false`.
The replay therefore blocks opposite directions on the same pair and enforces
one open position per strategy. Same-direction overlaps between strategies are
allowed. There is no invented six-position cap.

Four predeclared chronologies examine uncertain timing:

- `END_ASC`: release at the exit candle's end; simultaneous strategy IDs ascend.
- `END_DESC`: release at the exit candle's end; simultaneous strategy IDs descend.
- `START_ASC`: release at the exit candle's start; simultaneous strategy IDs ascend.
- `START_DESC`: release at the exit candle's start; simultaneous strategy IDs descend.

Within each strategy the source function's trigger priority is retained in all
four cases. These are modeling assumptions, not guaranteed return bounds and
not new live execution policies. No best scenario is selected by the runner.

Open-position gates run before the entry preview, as in the executor. Candidate
paths that are needed by any chronology use the preceding corrected price
engine: broker rounding, first native bid/ask entry estimate, 10-tick cap,
bid/ask exits, M1/S5 refinement and stop gaps. Unneeded blocked paths are not
priced merely to add a new backtest trade.

Uncertain price paths and their later admission consequences stay flagged.
After an uncertain path, subsequent admissions for that pair carry an explicit
uncertainty lineage. An observed later barrier is a conditional exit assumption,
not proof that an earlier missing observation could not have closed the trade.

## Conditional accounting: how to read it

The useful primary outputs are recovered signals, archive reconciliation,
admission differences and source coverage.

`conditional_price_accounting.json` additionally shows **illustrative closed
equity in research units**, starting at 100. Each entry allocates 1% of closed
balance, with 0.75% for EUR/JPY M15 short, and uses worst-permitted-entry budget R.
Both STOP_FIRST and TARGET_FIRST are shown for unresolved same-S5 double touches.
Source-gap paths use the observed-barrier scenario and remain qualified.

These calculations assume static quote/home conversion and fractional position
size. They omit actual GBP conversion, unit rounding, margin, live NAV sizing,
financing and unrealized equity. The CAGR/DD fields are explicitly conditional
**closed-equity** estimates. They are not validated portfolio returns, net
returns, independent out-of-sample results or measured open-equity drawdowns.
Unpriced closed exits suppress whole-cohort accounting; still-open positions
are not forced closed.

The runner never labels the portfolio fully validated simply because all 32
streams have been scanned. Reconciliation, coverage and accounting assumptions
must be reviewed together before any risk/capital decision.

## Files to inspect

- `raw_signals.csv` and `raw_signal_witnesses.jsonl`: all observed qualifications.
- `strategy_stream_coverage.csv`: one row for every strategy, including zero-count streams.
- `native_history_coverage.csv`: real observations and missing calendar slots.
- `archived_3391_signal_reconciliation.csv`: original accepted signal matches/differences.
- `archived_eurusd_raw_reconciliation.csv`: isolated-path controls.
- `admission_ledger.csv`: every candidate's gate/preview/admission decision in each chronology.
- `candidate_price_paths.csv`: unique priced candidates and qualifications.
- `admission_scenario_summaries.json`: counts and resolved-subset statistics.
- `conditional_price_accounting.json`: qualified price-only equity sensitivity.
- `conditional_stop_first_equity_events.csv`: research-unit accounting trail.
- Native source indices, request receipts, rule provenance and original controls.

## Local checks

Python 3.10 or newer is required. `tzdata` supports Windows timezone lookup.

```text
python -m pip install -r requirements.txt
python self_test.py
python app.py --run --offline --output signal_recovery_offline
```

Offline mode works only for already cached continuous history. The supplied
seed ZIPs contain execution evidence, not complete signal-generation history.
An initial offline recovery therefore reports a missing-data request.

For the complete local run, set the OANDA environment variables and use:

```text
python app.py --run --output signal_recovery_results
```

The included offline tests verify entry calculations, saved execution examples,
indicator arithmetic, all32 registry identities, adapters against the original
pure probe functions, causal higher-timeframe selection, current gates,
simultaneous-order scenarios, accounting and a synthetic end-to-end run.
Historical signal parity is checked by the hosted run using its recovered data;
no new market history was downloaded while building this package.
