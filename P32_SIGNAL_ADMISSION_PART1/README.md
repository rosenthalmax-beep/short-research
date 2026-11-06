# Recent live qualification replay — 6 November 2026

## Deploy on your SEPARATE RESEARCH/AUDIT service

Do not replace files on the live executor or live strategy-probe service.

1. Extract this ZIP and upload its contents, preserving the `inputs`,
   `reference` and `validation` folders, to your separate research service.
   Replace matching files there. You do not need to delete unrelated files.
2. Set that research service's start command to `python app.py`.
3. Keep its persistent volume mounted at `/data`. Keep existing research data
   and all old output directories. This run uses a NEW directory:
   `/data/p32_recent_live_replay`.
4. Keep/set `OANDA_TOKEN` to the token already used for candle research.
   No account ID, executor URL, probe URL or webhook secret is needed by this
   package. Do not send credentials back in chat.
5. Redeploy. Open the research service's base URL to check the service label
   `P32_RECENT_LIVE_REPLAY_V1_2026_11_06`. Confirm its persistence metadata says
   `separate_mount_detected: true`.
6. Open the same research URL followed by `/status` to start the replay.
   Reopening `/status` shows progress and does not start a duplicate run.
7. When `status` is `COMPLETE`, download `/results-summary.zip` and attach it
   here. `/results.zip` is an alias for the same small summary archive.
   If `status` is `ERROR`, send the status JSON and summary ZIP instead.

Python 3.11+ is supported; there are no additional pip dependencies. The
standard system timezone database must include `America/New_York` and the
other IANA zones used by your deployed strategy definitions.

## Frozen review window and evidence

The replay covers signal CLOSE timestamps from 7 October 2026 08:30 UTC through
6 November 2026 09:45 UTC, inclusive. This begins with the first saved audit
signal and ends at the morning review cutoff for the logs you supplied.
It does not extend beyond the bundled log snapshot and call later signals
"missing" merely because newer logs were not provided.

The package contains your supplied deployed strategy-probe code and fixed
snapshots of executor signals, actual outcomes, signal audit events and latest
missed-trade outcomes. It recognises 17 distinct recorded signals in this window,
including the strategy-level position rejection absent from executor history.
The original uploads and live databases are not edited.

## What is replayed

All 32 registered strategies: 17 H1 and 15 M15. There is no parameter search.
Thirty-four financial/indicator/payload definitions are extracted verbatim from
the deployed source. The live application is NEVER imported. No watchers,
executor client, account API client, order methods or live database routines
are present in the extracted engine. The service's only broker connection is
an explicit GET to the native instrument-candle endpoint. HTTP POST is rejected.

The runner uses native M15/H1/H4/D MID candles, preserving your daily alignment
and timezone. H4/D candles are only context inputs where the existing M15 rules
require them. This does not add an H4 trading strategy. Higher-timeframe context
is selected with the original engine's causal completion logic.

EUR/CHF uses the embedded frozen ATR state and 61 prior bars. Broker overlap is
checked against the pinned source before causal ATR advancement; a discrepancy
stops the run rather than automatically reseeding its indicators.

Other strategies retain the live rolling candle counts and indicator seeds.
Two count interpretations are replayed:

- `RAW_COUNT_FORMING`: a broker count request includes a current forming candle,
  leaving one fewer completed candle after the original complete-only filter.
- `COMPLETED_COUNT`: the comparison contains the full requested completed count.

The second case is a sensitivity check for historical candle-count availability.
It is not another strategy candidate. An open-time marker carries NaN OHLC for
the forming HTF bar; its future final prices are never supplied to the engine.
The original causal HTF selector can use that observed timestamp but only
returns a previously completed bar. Tests mutate future/forming OHLC and confirm
the selected context is unchanged. Incomplete historical bars cannot be treated
as completed history.

## Results and interpretation

`summary.json` gives evaluation/qualification counts, recorded signal matches,
field mismatches, native-gap counts and limitations. `comparison.csv` is the
compact review table; `comparison.json` retains both count cases and the
recorded source record. `qualified_signals.json` contains replayed payload fields
with the placeholder secret removed. `native_coverage_gaps.json` preserves
unexplained native intervals and absent expected H1/M15 signal bars. The result
ZIP excludes the raw candle cache and is limited to under 24 MiB.

Classifications:

| Classification | Meaning |
| --- | --- |
| MATCHED_RECORDED_SIGNAL | Same signal ID is present under both count cases and in supplied logs. Inspect the separate field-difference column for entry/stop/RR/trigger differences. |
| COUNT_WINDOW_SENSITIVE | Qualification or payload differs between count cases; review before calling it a missing signal. |
| UNLOGGED_QUALIFICATION_REQUIRES_REVIEW | Both cases agree on a qualification absent from supplied logs. It is an investigation candidate, not proof an executable trade was missed. |
| RECORDED_SIGNAL_NOT_REPRODUCED | A logged qualification is absent from both replay cases. Investigate source delivery/revisions, count behavior and deployments. |

This stage audits QUALIFICATIONS. It does not replay account gates, margin,
historical quotes, entry deviations, fills, exits or financing, and it does not
calculate a new portfolio return or drawdown. The corrected historical account
comparison follows after qualification reconciliation. `full_portfolio_validated`
therefore remains false even after a completed qualification audit.

It uses the current supplied rule definitions throughout the fixed window.
Actual historical activation/setting changes are not inferred. Today's retrieved
candles may differ from what the live service received. Missing bars and live
outages remain limits on completeness. Ordinary weekend/daily closures are
recognised; other absent intervals are reported, not filled with invented data.

## Storage, resume and bounded downloads

Only recent native signal history plus the counts needed for indicator warm-up
are fetched. Daily warm-up can reach roughly ten years back, but contains only
about 2,500 D candles per pair, not ten years of intraday candles. M15/H1/H4
warm-up is bounded by the deployed requests. No 20-year MID regeneration and no
M1/S5 trade-path downloads are performed.

The gzip candle cache is capped at 64 MiB. A 96 MiB free-space reserve is required.
The service stops rather than deleting other research files. At most 200 distinct
native requests are allowed per worker session, with bounded retries for transient
GET failures. Exact cached blocks are reused with hash checks; overlapping OHLC
must agree. Pair-level checkpoints are also hash-checked.

If redeployed after interruption, the same run resumes from completed pair
checkpoints and cached blocks. A failed run can be resumed by redeploying after
fixing its stated cause; it does not endlessly retry inside the current process.
An unchanged completed run is reused. A file lock prevents a second worker from
using the same output directory. Compare the persistence marker after a redeploy
to establish that the mounted output survived.

## Offline validation

23 tests passed, including:

- exact financial-definition equivalence to your uploaded probe;
- all 32 distinct strategies evaluated in both count cases;
- no network/watchers/database imports in the extracted engine;
- causal HTF context, incomplete-bar rejection and sufficient rolling history;
- DST-aware daily boundaries, weekend closure and missing-signal-bar reporting;
- native overlap/cache integrity and EUR/CHF anchor-disagreement rejection;
- recorded/unlogged/window-sensitive/field-mismatch classification;
- full fake-candle nine-pair worker run, checkpoint resume, result archive and
  corrupted-checkpoint rejection;
- actual local HTTP service GET/status behavior and blocked order POSTs.

The tests used synthetic offline candles. They do not establish the real replay
results. No OANDA request or order was made while building/testing this package.
