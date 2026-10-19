# P32 fresh GBP100 entry-limit comparison — 19 October 2026

Extract the outer update ZIP and upload **its contents** into the application
root of the **same separate research/audit service**, replacing matching files.
**Do not delete the existing files first.** Retain `seed_01.zip` through
`seed_05.zip`, the two supplied inner input ZIPs, and the mounted `/data` volume.
Keep inner ZIPs zipped. This package is under 25 MB. It contains no credentials.

Start command remains **`python app.py`**. Standard library only; Python 3.10+
with timezone data on Linux. Procfile remains supplied.

| Variable | Value |
| --- | --- |
| OANDA_TOKEN | Keep your existing token in service variables |
| OANDA_ENV | live |
| P32_GBP_OUTPUT_DIR | /data/p32_signal_admission |
| P32_SIGNAL_OUTPUT_DIR | /data/p32_signal_admission |
| P32_SIGNAL_OFFLINE | Leave unset for the hosted run |

This service only supports historical candle GET requests. It has no order,
executor or strategy-probe submission route. Upload to the research service.

## Run and download

1. Upload the extracted contents and redeploy using `python app.py`.
2. Open your research URL followed by `/health`. Check service
   `P32_SMALL_ACCOUNT_ENTRY_LIMIT_V1_2026_10_19` and
   `persistence.separate_mount_detected: true`. The output directory should be
   `/data/p32_signal_admission`. The existing persistence marker is retained.
3. Open `/status` to start; refresh it for progress. Health checks and HEAD
   requests do not start work. Repeated GETs do not start duplicate jobs.
4. First, all **16 original full-history 10-tick account controls** must pass.
   A failure stops the experiment before any fresh-account comparison.
5. Then **864 fresh-account lanes** run. This is 18 windows × 3 policies ×
   4 chronology assumptions × 4 funding assumptions. Watch `lanes_completed`
   against `lanes_required`, `window_id`, `policy` and `through_utc`.
6. At `status: COMPLETE_WITH_LIMITATIONS`, download `/results.zip` and return
   **P32_SMALL_ACCOUNT_ENTRY_LIMIT_RESULTS.zip** for review.

If it reports ERROR, download the diagnostic ZIP from the same `/results.zip`
route. Reopen `/status` after a restart to resume completed checksummed lanes.
A partly completed account lane restarts from its own fresh account start;
previous candle and path calculations remain cached. An exclusive process lock
prevents two workers from writing this experiment simultaneously.

The three caps do not change live. No winner is chosen automatically.

## Frozen experiment

- Policies: FIXED_10 (current control), FIXED_15, STOP10_MAX30.
- Relative rule: `max(10,min(30,floor(0.10*original_stop_distance_ticks)))`.
- Seventeen overlapping five-year windows, starting each 1 January from 2005
  through 2021. Examples: 2005-01-01 to 2010-01-01; 2021-01-01 to 2026-01-01.
- One separate recent diagnostic: 2021-01-01 to 2026-09-20 18:29 UTC, the existing
  pinned data cutoff. This does not extend history through today's live market.
- Every account starts with **GBP100 and no existing positions**. It compounds
  within its window; no withdrawals, scaling resets or clipping of order sizes
  occur inside a window. Strategies retain their original warmed indicators.
- Signals enter at or after the window start and strictly before its end.
  Earlier positions do not carry into a fresh account. This is a deliberate
  fresh-start comparison, not a reconstruction of a continuously funded account.
- Existing per-strategy pyramiding, opposite-pair gates, 1% NAV sizing (0.75% for
  EUR_JPY_M15_SHORT), whole units, margin, conversion markup and broker limits
  stay in force. Capacity rejections are counted and flagged, never suppressed.
- Entry bid/ask evidence, rounded stop and target remain shared. Each cap uses
  its own worst-permitted-entry sizing bound; targets are not moved to restore RR.
- Chronologies: END_ASC, END_DESC, START_ASC, START_DESC, as before.
- Funding assumptions: zero financing, frozen 14-Oct-2026 rates, adverse 3% annual,
  adverse 6% annual, as before. They are not recovered historical rate schedules.
- Primary comparison: END_ASC with frozen 14-Oct-2026 funding. Assess the other
  lanes for sensitivity. Overlapping windows are not independent experiments.

## Correct treatment of the window end

An exit scheduled after a window ends is censored for that account: the
position remains open, accrues financing only through the cutoff, and is valued
using causal M15 midpoint NAV and separate executable-side liquidation equity.
Its later target/stop is never booked in cash or counted as a closed win/loss.
There is no forced end-of-window closing transaction. Existing source/path gaps
remain uncertain. A barrier interval straddling the cutoff under END chronology
is additionally qualified rather than labelled definitely open.

## Storage and re-use

New reports/checkpoints:
`/data/p32_signal_admission/small_account_entry_limit_v1`.

The prior `entry_limit_v1/policy_paths.sqlite3` and its `new_execution_windows`
are reused; the shared `gbp_execution_paths_v2` and `gbp_valuation_windows_v2`
are reused. Do not clear, replace or move these directories. Previous reports
and checkpoints are retained. This update performs no continuous MID-history
or strategy-signal regeneration. Fresh account gates can expose a previously
blocked path; a missing path or valuation window can still need candle GETs.

The previous experiment source is retained as `entry_limit_service.py` for
regression checks; the hosted start command uses the new `app.py`.

## Reports

Start with `recent_comparison.csv`, `rolling_window_summary.csv` and
`window_comparison.csv`. REPORT_GUIDE.md explains comparisons, uncertainty,
open end positions and the separate full-detail/equity downloads.

Local validation:

    python -m unittest test_windows test_caps test_source_windows test_gbp test_execution

Restricted CLI runs still perform all sixteen original controls first:

    python app.py --run --window RECENT_2021_TO_CUTOFF --policy FIXED_15 --scenario END_ASC --funding-mode FROZEN_14OCT2026_RATES

The CLI adds the matching 10-tick window control automatically. Hosted `/status`
always uses the complete frozen experiment. `--offline` requires every needed
candle to be cached already.

These are conditional historical comparisons using selected strategies and
previously examined history. Recent windows are not an untouched holdout;
historical returns are not a forward forecast. Current broker and funding
terms, candle-entry proxies, 11 original unknown entries, 170 qualified original
paths and sampled drawdowns retain their stated limitations. See
GBP_ACCOUNT_ASSUMPTIONS.md and the protocol in the results ZIP.
