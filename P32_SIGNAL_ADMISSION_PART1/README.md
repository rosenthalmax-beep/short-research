# P32 warm-up and gap audit update — 16 October 2026

Apply to the SAME SEPARATE RESEARCH/AUDIT SERVICE that ran the signal admission
replay. Upload every file from this outer update ZIP, replacing matching files.
Keep all five existing seed_01.zip through seed_05.zip files and the existing
persistent data directory. Keep prior_signal_admission_results.zip zipped too.
Do not upload this application to the live executor or live strategy probe.

Existing settings remain:
  OANDA_TOKEN = your existing broker API token (do not put it in source files)
  OANDA_ENV = live
  P32_SIGNAL_OUTPUT_DIR = /data/p32_signal_admission
  P32_SIGNAL_OFFLINE = unset
Start command: python app.py
No account ID, executor URL, probe URL or webhook secret is required.
Only historical OANDA candle GETs are supported.

1. Deploy/restart with this update.
2. Open /health and verify service P32_WARMUP_GAP_AUDIT_V1_2026_10_16.
3. Open /status once to start; refresh to follow progress.
4. When result_ready is true, download /results.zip and upload that file for
   review. Its filename is P32_WARMUP_GAP_AUDIT_RESULTS.zip.
   Status COMPLETE_WITH_LIMITATIONS is expected. It does not mean fully
   validated historical account returns. /sources.zip is a larger optional
   reproduction archive, available when packaging has finished.

What runs:
- Verify the supplied prior result ZIP, import its 89 execution-source blocks,
  and investigate all 171 original uncertain price paths.
- Reconstruct native, M1 and S5 missing windows before observed exits. Requery
  each window in a separate evidence cache, inspect finer quotes, and check one
  other pair when both the original and finer tiers return no candles.
- Report recovered barrier candidates, finer coverage, partial observations
  and coincident/pair-specific absence. Empty responses do not certify a
  historical closure. Evidence is diagnostic; original exits are not
  automatically promoted, repaired or mixed with newly revised quote data.
- Download indicator warm-up history from 2002-01-01 through the existing
  evaluation boundary. Signal evaluation remains 2005-01-01 through
  2026-09-20T18:29Z. No pre-2005 trades enter the results.
- If older history is unavailable, keep that explicit. A rejected pre-2005
  range is recorded as unknown rather than saved as an empty candle response.
  Authentication, in-period and other source errors still stop the run.
- Recover all 32 locked strategies and rerun the four admission chronologies.
- Compare qualifications with the prior 3,600 signals and show what happened
  to the 32 archived signals previously missing in 2005. Exact agreement is
  not forced. Missing/insufficient warm-up remains explicit in coverage.

Progress/cache details:
- INVESTIGATE_171_PRIOR_GAPS uses gap_paths_completed / gap_paths_required.
- DOWNLOAD_CONTINUOUS_HISTORY uses history_segment INDICATOR_WARMUP or
  TEST_PERIOD, pair/granularity, cache hits and download counts.
- RECOVER_RAW_SIGNALS and REPLAY_ADMISSIONS follow the warm-up download.
- last_progress_utc and seconds_since_last_progress describe real updates;
  they are not fabricated progress or a guarantee of timely broker responses.
- Old 2005–2026 history request boundaries are retained to reuse the old cache.
  Signal checkpoints are deliberately regenerated because indicator inputs
  changed. Existing candidate source quotes remain reusable.
- Each completed gap case is checkpointed. Restarting with the same code,
  data directory and baseline reuses it. Source requests also persist.
- If the persistent volume was lost, some history must be downloaded again;
  the five seed ZIPs and embedded prior replay still supply saved execution
  candles. Do not delete the data folder to reset counters.

New report files:
  gap_investigation_summary.json
  gap_path_investigation.csv
  gap_window_evidence.csv
  warmup_comparison_summary.json
  warmup_signal_changes.csv
  prior_missing_32_followup.csv
The usual signal reconciliation, admissions, price paths and conditional
accounting remain included. The gap cohort is the ORIGINAL 171 paths; added
warm-up signals can introduce other unresolved paths outside that cohort.

Limits:
This pass adds warm-up and investigates source gaps. It does not establish
historical broker closures, fills or net account returns. Financing, GBP
conversion, whole-unit/margin effects and open-equity drawdown are still not
part of the illustrative account model. No strategy thresholds, risk targets,
10-tick guard, live services or live trading settings are changed.

Validation performed locally:
  python3 -m unittest self_test test_execution test_gap_warmup
  103 offline tests passed, including the DST regression, prefix cache reuse,
  exclusion of warm-up trades, gap tiers, recovered barrier flags, refusal to
  infer closure from empty candles, checkpoint reuse and service plumbing.
  All 171 prior paths were traced from saved source candles, reconstructing
  403 gap windows: 26 native, 346 M1 and 31 S5.
The fresh historical broker rechecks and 2002–2004 download have not been run
locally. They start on your research service through /status.
