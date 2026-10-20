# P32 pullback research — pass 1, warm-up fix (V2)

This V2 fixes the abort on known sparse pre-2005 AUD/JPY history. Keep the
existing output directory: all already downloaded canonical candles are reused.
Research checkpoints receive a new code signature; the candle cache is shared.

This package runs the baseline and broad single-feature study agreed on
20 October 2026. Use the **separate research/audit service**.

## Upload and start

1. Download and extract the OUTER ZIP.
2. Upload its extracted contents to the application root of the same separate
   research service. Keep the `pullback_lab` and `tests` folders intact.
   **Do not delete the existing application files, input ZIPs or /data contents.**
3. Change the service start command to:

   ```text
   python pullback_app.py
   ```

   The supplied Procfile specifies this too. If the hosting dashboard has a
   custom start command, update that dashboard value explicitly.
4. Keep the existing OANDA token as a service secret and use:

   | Variable | Value |
   | --- | --- |
   | `OANDA_TOKEN` | Your existing token; do not paste it into code |
   | `OANDA_ENV` | `live` |
   | `PULLBACK_OUTPUT_DIR` | `/data/p32_pullback_pass1` |
   | `PULLBACK_LEGACY_CACHE_DIR` | `/data/p32_signal_admission` |

   Keep the existing persistent volume mounted at `/data`. Keep earlier
   P32 variables and archives. Leave `PULLBACK_OFFLINE`, `PULLBACK_PAIRS`,
   `PULLBACK_START_UTC`, `PULLBACK_END_UTC` and `PULLBACK_WARMUP_START_UTC`
   unset for the full hosted study. Old unrelated P32 filter variables do not
   restrict this new study.
5. Redeploy. Open your existing research URL followed by **`/health`**.
   It should show service `P32_PULLBACK_PASS1_V2_2026_10_20_WARMUP`,
   `orders_supported: false`, and `persistence.separate_mount_detected: true`.
   The output directory should be `/data/p32_pullback_pass1`.
6. Open **`/status`** once to start, then refresh it for progress.
   `/health`, HEAD requests and the download route do not start a job.
   Repeated `/status` GETs do not start duplicate workers.
7. At `status: COMPLETE`, download **`/results.zip`** and send that ZIP back.

**Use the research service. The live executor and strategy probe are not the
destination for these files.** No order, webhook, pricing-stream or executor
submission route is present. The network reader only GETs historical candles
from the official live OANDA candle endpoint. An account ID is not required.

## What runs

- Nine pairs, BUY and SELL as research directions, even where a direction is
  not currently registered for live trading.
- Native H1 trend and M15 trigger data. Use available pre-2005 history, then
  quarantine each pair until the unchanged observed-bar warm-up requirements
  are met. Coverage reports identify delayed starts and excluded periods.
- Evaluation from **2005-01-01 through 2026-09-20 18:15 UTC**, a completed M15
  boundary just before the earlier 18:29 archive cutoff. No automatic extension
  to newer data is made.
- Three fixed-3R controls: raw exact engulfing; H1 trend plus engulfing;
  H1 trend plus M15 pullback plus engulfing.
- 32 predeclared diagnostic rows, including the unfiltered row, for pullback
  body/range/close, depth, duration, H1 trend strength/distance, stop size and
  EMA alignment. Results include separate sides and historical entry cohorts.
- 324 fresh GBP100 control accounts: 9 pairs x 3 controls x 6 date windows x
  2 financing modes. Each account contains one pair with both directions and
  uses 1% marked-NAV risk, nonhedging, strategy pyramiding zero, fixed15 entry
  allowance, whole-unit sizing, margin and GBP conversion.
- Five-year era accounts begin in 2005, 2010, 2015, 2020 and 2025; the last
  era is partial. An additional 2021-to-cutoff account overlaps the eras.
- Frozen 14-Oct-2026 financing assumptions and an adverse 6% annual sensitivity.
  Actual historical rates and holiday schedules are not known.

The pullback control changes both selection and stop geometry relative to the
raw engulfing control. They are useful controls, not a pure single-factor
attribution experiment.

**Filter tables are diagnostic partitions of independent GBP100 opportunities.**
They do not replay alternate filters, later replacement triggers or account
gates. Their CAGR/annual-return fields are deliberately blank. Promising
conditional geometries need the next conditional-matrix and executable replay
pass. The current32 portfolio is not included in these control accounts and no
live candidate is automatically selected or activated.

## Rules frozen for this pass

- Exact engulf: opposite previous body; new body covers that previous body,
  with equality allowed. No minimum body ratio in the raw definition.
- H1 long: latest completed H1 close above EMA100; EMA100 above its value
  three observed H1 bars earlier. Reverse for short. An H1 candle completing
  exactly at M15 close is eligible. Reject a stale H1 snapshot.
- EMA uses an initial SMA seed; ATR14 is Wilder smoothing. Warm-up needs at
  least 1,000 observed H1 and 250 M15 bars. If pre-start history is insufficient,
  the pair becomes eligible only when these fixed counts have actually completed.
  No synthetic or future candles are used. Each affected account starts flat
  at its later eligible date; its excluded early period is not counted as a
  zero-return investment period. Shortened windows and actual CAGR start dates
  are explicit in coverage/account reports.
- A valid trend must first show an M15 candle fully clear of EMA20: low above
  its EMA for long, high below for short. A subsequent candle touching the
  **previous** M15 EMA starts the pullback. Include the touch candle in the
  setup's eight-bar lifetime.
- Confirm on the touch candle or within the next seven consecutive M15 bars:
  exact engulf and close back on the trend side of current EMA20.
- Reset on expiry, any M15 gap or failed H1 trend. The first qualifying trigger
  consumes the setup even if its eventual execution is rejected. Rearm only
  after another fully clear candle. One signal per identified pullback.
- Stop beyond pullback extreme through confirmation, plus 10 ticks. Raw/trend
  controls use the signal candle extreme plus 10 ticks.
- First native M15 bid/ask OPEN at the exact next entry time is the execution
  proxy. Missing exact entry data stays unknown. Reject adverse quotes above
  15 ticks; size at the worst permitted bound. Stop and reference-based 3R
  target remain fixed. No spread overlay is deducted again.
- Exit long on BID and short on ASK, refining the first possible touch in M1,
  then S5 for barrier ambiguity/rollover timing. Unresolved paths stay labelled.
  Rejected signal counterfactual price outcomes remain separate from trades.

## Progress, disk space and resuming

The first run can require a substantial historical candle download and several
GB of persistent disk, especially for M1/S5 refinement. Runtime depends on
cache coverage and broker response speed; no fixed completion time is promised.
Status shows pair, phase, data window, processed counts and free disk bytes.

Exact existing canonical cache windows are read and copied into the new cache;
older MID-only data is insufficient to substitute for missing bid/ask history.
The legacy databases are opened read-only. No conflicting candle overlaps are
merged. Every response is checksum-checked and retained in the new persistent
cache. Credentials and request headers are never included in reports.

Completed windows, paths, opportunities and accounts checkpoint to disk.
After a service restart/redeploy with the SAME code/config, open `/status` to
resume. Keep the output directory and marker. Code/config changes receive a
different study signature; previous results are retained. A process-level file
lock prevents concurrent jobs on this output directory.

A partial download is available after each completed pair. Full completion
requires all nine. An error produces `ERROR.json` and preserves finished pair
reports. Authentication errors stop the candle reader. Failed or missing data
is never cached as successful empty history.

## Result files

- `REVIEW.md`: scope, assumptions and interpretation.
- `protocol.json`: exact rules, trial grid, dates, modes and source signature.
- `control_accounts.csv`: conditional one-pair account summaries.
- `feature_study.csv`: independent-opportunity filter diagnostics and counts.
- `pairs/<PAIR>/qualifying_signals.jsonl.gz`: every raw qualification/feature.
- `pairs/<PAIR>/price_paths.jsonl.gz`: price paths, rejections and uncertainty.
- `pairs/<PAIR>/opportunities.jsonl.gz`: independent GBP100 shadow calculations.
- `pairs/<PAIR>/monthly_qualifications.csv`: monthly counts, including zeros.
- `pairs/<PAIR>/accounts/*.json.gz`: account gate decisions, closed trades,
  financing allocations, cash events and monthly NAV observations.
- `coverage.json`, broker reconciliation, source index and report hashes.

Account curve statistics remain conditional on entry-open proxies, END_ASC
release timing, current saved broker terms, funding assumptions and any data
uncertainty. Ambiguous both-barrier account outcomes use STOP_FIRST with an
explicit lineage flag. These assumptions are not mathematical return bounds.
Drawdown samples cannot guarantee the worst intrabar equity.

## Offline verification

Python 3.10+ with system timezone data; standard library only.

```text
python -m unittest discover -s tests -v
```

The shipped validation includes a synthetic end-to-end study and restart
recovery. It proves software behaviour on those fixtures; the real historical
study still needs to run on your research service. No profitability claim is
made by the package tests.

Protocol references: OANDA instrument candle definitions
https://developer.oanda.com/rest-live-v20/instrument-df/
and the existing P32 broker-reconciled accounting modules (provenance included).
