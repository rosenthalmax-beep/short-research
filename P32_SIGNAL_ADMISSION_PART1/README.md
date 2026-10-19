# P32 entry-limit research update — 19 October 2026

Upload this to the **same separate research/audit service** that ran the GBP
account replay. Extract the outer ZIP and upload all contents to the application
root, replacing matching files. **Do not delete the existing files first. Keep
seed_01.zip through seed_05.zip, and keep the persistent /data volume.** Keep the
two inner input ZIPs zipped. Do not upload this to the live executor or probe.

Start command: `python app.py`. Python 3.10+; standard library only.

| Variable | Value |
| --- | --- |
| OANDA_TOKEN | Keep the existing token in service variables |
| OANDA_ENV | live |
| P32_GBP_OUTPUT_DIR | /data/p32_signal_admission |
| P32_SIGNAL_OUTPUT_DIR | /data/p32_signal_admission |
| P32_SIGNAL_OFFLINE | Unset |

The /data directory must be the mounted persistent volume already attached to
this service. Shared execution/valuation caches are reused. New reports and
checkpoints go into `/data/p32_signal_admission/entry_limit_v1`. Earlier GBP
reports are preserved. No continuous MID-history regeneration is performed.

1. Deploy the extracted files with existing seed ZIPs retained.
2. Open `/health`. Check service `P32_ENTRY_LIMIT_RESEARCH_V1_2026_10_19` and
   `separate_mount_detected: true` in persistence.
3. Open `/status` to start. Refresh that URL for progress.
4. All sixteen 10-tick control lanes run first. Their summaries and complete
   ordered account tables must reproduce the pinned GBP results before any
   alternative runs. A mismatch stops the experiment.
5. At `COMPLETE_WITH_LIMITATIONS`, download `/results.zip` and send back
   `P32_ENTRY_LIMIT_RESEARCH_RESULTS.zip`.

At ERROR, download the available results ZIP for diagnosis. After restarting,
reopen /status: completed lanes and computed paths resume from checksummed
files. A partly finished account lane restarts its account loop while reusing
saved price work. Refreshing /status does not duplicate a running job.

## Frozen comparison

Five policies × four chronology assumptions × four funding assumptions =
**80 account lanes**. The frozen protocol is written before computation.

| Policy | Maximum adverse entry deviation |
| --- | --- |
| FIXED_10 | 10 ticks / 1 pip, current control |
| FIXED_15 | 15 ticks / 1.5 pips |
| FIXED_20 | 20 ticks / 2 pips |
| FIXED_30 | 30 ticks / 3 pips |
| STOP10_MAX30 | 10% of rounded original stop distance, rounded down to ticks, with a 10-tick floor and 30-tick ceiling |

The relative comparator is a single rule for the portfolio. No optimum per pair
is fitted. All 3,629 warmed qualifications and 32 existing strategy rules stay
fixed. Entry quotes, rounded stops and targets are shared across policies.
Targets are not moved to restore RR after a worse entry. Existing EURCHF
target-passed and stop geometry checks remain. Position sizes use each policy's
worst permitted entry, whole units and the same 1% NAV risk budget (0.75% for
EUR_JPY_M15_SHORT). Wider allowances can shrink already-eligible positions.

Each lane rebuilds strategy and pair blocking, margin, GBP NAV sizing,
conversion, financing and sampled liquidation equity. Chronologies remain
END_ASC, END_DESC, START_ASC, START_DESC. Funding is zero, frozen 14 October
2026 rates, adverse annual 3%, and adverse annual 6%. These are assumptions,
not reconstructed historical financing rates. The primary comparison is
END_ASC with frozen rates; inspect all lanes for sensitivity.

Rejected candidates are not assigned old midpoint wins or losses. Full bid/ask
exits are computed when newly eligible. Previously blocked signals may become
eligible and need entirely new price paths. New candle downloads can be
substantial, particularly M1/S5. Only official OANDA historical candle GETs are
supported; no account ID, executor URL, webhook secret or order API is needed.

## Source policy and limitations

Consistent frozen execution coverage is used first. Missing or conflicting
coverage is read from independent immutable canonical windows: H1/M15 14-day,
M1 day, S5 hour. Fresh evidence never overwrites frozen bars. Numeric differences
with frozen sources are recorded and qualify the new path. The existing GBP V2
canonical valuation-window policy is retained.

The 170 previously qualified paths and 11 unknown entries remain explicit.
Historical prices are candle proxies, not recovered live quotes or fills.
Latency, broker price-bound cancellation frequency and intrabar worst equity
are not reconstructed. Margin and conversion terms are frozen. Recent-era
data have already been examined and are not an untouched holdout. No policy is
selected automatically or written to the live services.

## Downloads

Start with `/results.zip`: comparisons, periods, groups, control checks,
primary-lane admissions/trades/cash and evaluated paths. Full large account
records remain on the volume; `/details` lists per-lane JSON.GZ downloads.

Primary equity CSVs are separate ZIPs:
`/equity/FIXED_10.zip`, `/equity/FIXED_15.zip`, `/equity/FIXED_20.zip`,
`/equity/FIXED_30.zip`, `/equity/STOP10_MAX30.zip`.

Optional `P32_CAP_PACKAGE_SOURCES=1` creates `/sources.zip` with newly downloaded
execution windows. Default off. Keep earlier source archives and all valuation
caches too: this new archive alone does not replace the prior source evidence.

Local regression checks:

    python -m unittest test_caps test_source_windows test_gbp test_execution

Restricted CLI run, always including the matching 10-tick control:

    python app.py --run --policy FIXED_15 --scenario END_ASC --funding-mode FROZEN_14OCT2026_RATES

`--offline` requires all needed historical candles already cached. Hosted
`/status` runs all 80 lanes. Read REPORT_GUIDE.md and GBP_ACCOUNT_ASSUMPTIONS.md.
