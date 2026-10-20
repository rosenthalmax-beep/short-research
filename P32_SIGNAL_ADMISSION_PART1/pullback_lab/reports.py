"""Discovery reports: denominators and account/opportunity scope stay visible."""
import csv
import gzip
import io
import json
import statistics
from collections import Counter
from datetime import datetime
from pathlib import Path
from . import core as e
from .signals import FEATURE_FILTERS, accepts


def atomic(path, body):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + '.tmp')
    temp.write_bytes(body); temp.replace(path)


def csv_bytes(rows):
    fields = list(dict.fromkeys(k for row in rows for k in row)) or ['empty']
    out = io.StringIO(newline=''); writer = csv.DictWriter(out, fields)
    writer.writeheader()
    for row in rows:
        writer.writerow({k: json.dumps(v, sort_keys=True) if isinstance(v, (dict, list)) else v for k, v in row.items()})
    return out.getvalue().encode()


def write_jsonl(path, rows):
    temp = Path(str(path) + '.tmp')
    with gzip.open(temp, 'wb') as f:
        for row in rows:
            f.write(e.canon(row) + b'\n')
    temp.replace(path)


def windows(start, end):
    start, end = e.dt(start), e.dt(end)
    result = []
    for year in (2005, 2010, 2015, 2020, 2025):
        lo, hi = e.dt(f'{year}-01-01T00:00:00Z'), e.dt(f'{year+5}-01-01T00:00:00Z')
        lo, hi = max(lo, start), min(hi, end)
        if lo < hi:
            result.append(dict(window_id=f'ERA_{year}_{year+4}', start=e.stamp(lo), end=e.stamp(hi),
                               complete_five_years=lo.year == year and hi.year == year+5))
    lo = max(e.dt('2021-01-01T00:00:00Z'), start)
    if lo < end:
        result.append(dict(window_id='RECENT_2021_TO_CUTOFF', start=e.stamp(lo), end=e.stamp(end), complete_five_years=False))
    return result or [dict(window_id='CUSTOM', start=e.stamp(start), end=e.stamp(end), complete_five_years=False)]


def cohorts(start, end):
    result = [dict(window_id='ALL_OBSERVED', start=e.stamp(start), end=e.stamp(end))] + windows(start, end)
    for n in (2, 5, 10):
        lo = max(start, end.replace(year=end.year-n))
        if lo < end:
            result.append(dict(window_id=f'LAST_{n}_YEARS_ENTRY_COHORT', start=e.stamp(lo), end=e.stamp(end)))
    return result


def stats(rows):
    closed = [r for r in rows if r['shadow'].get('shadow_status') == 'CLOSED_RESOLVED']
    values = [r['shadow']['frozen_net_r'] for r in closed]
    stressed = [r['shadow']['adverse6_net_r'] for r in closed]
    wins = sum(v > 0 for v in values)
    positive, negative = sum(v for v in values if v > 0), -sum(v for v in values if v < 0)
    rejection = [r for r in rows if r['path_status'] == 'ENTRY_PROXY_REJECTED']
    resolved_reject = [r for r in rejection if r.get('counterfactual_outcome_status') == 'CLOSED_RESOLVED']
    return dict(qualifications=len(rows), status_counts=dict(Counter(r['shadow'].get('shadow_status') for r in rows)),
                resolved_standalone_gbp100_opportunities=len(closed),
                unresolved_or_nonclosed_or_sizing_excluded=len(rows)-len(closed)-len(rejection),
                preview_rejections=len(rejection),
                resolved_rejected_counterfactuals=len(resolved_reject),
                rejected_counterfactual_price_win_rate_pct=100*sum(r.get('counterfactual_price_r', 0)>0 for r in resolved_reject)/len(resolved_reject) if resolved_reject else None,
                resolved_only_frozen_net_win_rate_pct=100*wins/len(values) if values else None,
                resolved_only_mean_frozen_net_r=statistics.fmean(values) if values else None,
                resolved_only_sum_frozen_net_r=sum(values),
                resolved_only_frozen_net_r_profit_factor=positive/negative if negative else None,
                resolved_only_mean_adverse6_net_r=statistics.fmean(stressed) if stressed else None,
                resolved_only_mean_price_r=statistics.fmean(r['shadow']['price_r'] for r in closed) if closed else None,
                mean_duration_hours=statistics.fmean(r['shadow']['duration_hours'] for r in closed) if closed else None,
                median_duration_hours=statistics.median(r['shadow']['duration_hours'] for r in closed) if closed else None,
                rollover_exposure_pct=100*sum(r['shadow']['rollover_events']>0 for r in closed)/len(closed) if closed else None,
                stale_conversion_opportunities=sum(r['shadow'].get('stale_conversion_quote_seen',False) for r in closed),
                mean_net_r_without_best_five=sum(sorted(values)[:-5])/(len(values)-5) if len(values)>5 else None,
                annual_return=None, cagr=None,
                scope='DIAGNOSTIC_INDEPENDENT_OPPORTUNITIES; NO_ACCOUNT_GATING_COMPOUNDING_OR_FILTER_REPLAY')


def warmup_coverage(start, end, eligible_from):
    start, end = e.dt(start), e.dt(end)
    effective = max(start, e.dt(eligible_from))
    return dict(requested_period_start_utc=e.stamp(start), period_end_utc=e.stamp(end),
                eligible_period_start_utc=e.stamp(effective) if effective < end else None,
                warmup_exclusion_at_period_start=effective > start,
                eligible_calendar_years=max(0., (end-effective).total_seconds())/(365.25*86400),
                warmup_excluded_calendar_days=max(0., (min(effective,end)-start).total_seconds())/86400)


def feature_study(pair, opportunities, start, end, eligible_from=None):
    rows = []
    for cohort in cohorts(start, end):
        for side in ('BUY', 'SELL'):
            subset = [r for r in opportunities if r['control'] == 'TREND_PULLBACK_ENGULF' and r['side'] == side and
                      cohort['start'] <= r['entry_utc'] < cohort['end']]
            for rule in FEATURE_FILTERS:
                selected = [r for r in subset if accepts(r['features'], rule)]
                rows.append(dict(pair=pair, side=side, entry_cohort=cohort['window_id'],
                                 **warmup_coverage(cohort['start'],cohort['end'],eligible_from or start),
                                 filter_id=rule['filter_id'], feature=rule['feature'], operator=rule['operator'], threshold=rule['threshold'],
                                 retained_qualifications_pct=100*len(selected)/len(subset) if subset else None,
                                 **stats(selected),
                                 later_qualifying_trigger_after_failed_filter_researched=False,
                                 outcomes_followed_to_full_study_cutoff=True))
    return rows


def opportunity_summary(pair, opportunities, start, end, eligible_from=None):
    rows = []
    for control in ('RAW_ENGULF', 'TREND_ENGULF', 'TREND_PULLBACK_ENGULF'):
        for side in ('BUY', 'SELL'):
            for cohort in cohorts(start, end):
                selected = [r for r in opportunities if r['control']==control and r['side']==side and
                            cohort['start'] <= r['entry_utc'] < cohort['end']]
                coverage = warmup_coverage(cohort['start'],cohort['end'],eligible_from or start)
                years = coverage['eligible_calendar_years']
                rows.append(dict(pair=pair, side=side, control=control, entry_cohort=cohort['window_id'],
                                 **coverage,
                                 qualifications_per_year=len(selected)/years if years else None, **stats(selected)))
    return rows


def frequency(pair, signals, start, end, eligible_from=None):
    counts = Counter((s['control'], s['side'], s['entry_utc'][:7]) for s in signals)
    month = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    rows = []
    while month < end:
        nxt = month.replace(year=month.year+1, month=1) if month.month==12 else month.replace(month=month.month+1)
        for control in ('RAW_ENGULF', 'TREND_ENGULF', 'TREND_PULLBACK_ENGULF'):
            for side in ('BUY','SELL'):
                rows.append(dict(pair=pair, control=control, side=side, month=month.strftime('%Y-%m'),
                                 qualifications=counts[(control,side,month.strftime('%Y-%m'))] if nxt > (eligible_from or start) else None,
                                 **warmup_coverage(max(month,start),min(nxt,end),eligible_from or start),
                                 partial_calendar_month=month < start or nxt > end))
        month = nxt
    return rows


def readme(protocol, completed_pairs, accounts, features):
    return f'''# P32 pullback research — first pass

Status: {len(completed_pairs)}/{len(protocol['pairs'])} pairs complete. Research only.

Three controls: raw exact engulfing; H1 trend plus engulfing; H1 trend plus
M15 pullback plus engulfing. Both directions are recorded independently.
The pullback control changes selection AND stop geometry; its difference from
the raw control cannot be attributed solely to a trend filter.

`control_accounts.csv` contains fresh GBP100 accounts for ONE pair at a time,
with both directions competing under the saved nonhedging terms. Risk 1% of
marked NAV, whole units, fixed15 bounds, margin, conversion and funding apply.
Each pair retains the 1,000-H1/250-M15 observed-bar warm-up requirements. If
pre-2005 data is sparse, signals are quarantined until both requirements are
met. Coverage reports state the delayed eligible date and excluded period.
Each fresh account begins at the later of its requested start and that date;
the excluded period is not credited as a zero-return flat investment period.
Account summaries flag shortened windows; their CAGR uses the actual start.
The accounts do not contain the live32. They are conditional historical
experiments, not forecasts. Five-year era accounts reset flat with GBP100;
the recent account overlaps those eras. {len(accounts)} account summaries saved.

`feature_study.csv` contains {len(features)} predeclared single-feature
partitions of the FIRST qualifying pullback signal. These are independent
GBP100 shadow opportunities, not filtered account backtests. Rejected, missing,
open and unpriceable outcomes remain in denominators/counts. Closed-only
statistics must be read with those counts. Features cannot be ranked as proven
strategies. A filter may change subsequent triggers and portfolio gates; that
requires the next full conditional replay. No filter selection is automated.

Entry cohorts follow their outcomes to the study cutoff; they are not calendar
year investment returns. Monthly qualification tables include zero months.
Account monthly observations and cash ledgers are saved for inspection.

Path uncertainty: native MBA entry-open proxy, M1 refinement at first possible
barrier, S5 for ambiguous barriers/rollover timing. Missing data and source-tier
disagreement qualify paths. Conditional account scenarios use END_ASC exit
timing; ambiguous both-barrier exits use STOP_FIRST and flag account lineage.
Unknown entries are omitted with explicit lineage flags. END_ASC/STOP_FIRST
are scenario assumptions and are not mathematical return bounds.

Funding uses frozen 14-Oct-2026 rates and an adverse6% annual sensitivity.
Historical rates, holiday schedules, margin terms and conversion fees are not
reconstructed. Conversion and spread are already included once. Drawdown is
sampled at M15 and account events, not the unknowable worst intrabar equity.

All prior research history has been seen. Era and recent comparisons are
descriptive checks, not untouched out-of-sample evidence. There is no combined
P32 admission test, RR optimization, parameter plateau decision or live change
in this first pass. Those follow review of this frozen baseline/discovery run.

The source index records immutable canonical windows and any exact read-only
reuse. Complete raw candles remain in the mounted output cache. Reports include
per-file SHA256 hashes; checkpoints are tied to code/config signatures.
'''
