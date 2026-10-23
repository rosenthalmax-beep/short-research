"""Existing robustness criteria across neighbouring lifetime/distance settings."""
from audusd_matrix.reporting import account_screen
from .config import DISTANCE,LIFETIME


def regions(rules,rows):
    by={r['candidate_id']:[a for a in rows if a['candidate_id']==r['candidate_id']] for r in rules}
    grid={(r['distance_max'],r['lifetime_bars']):r['candidate_id'] for r in rules}
    result=[]
    for i in range(2):
        for j in range(2):
            members=[grid[d,n] for d in DISTANCE[i:i+2] for n in LIFETIME[j:j+2]]
            screens=[dict(candidate_id=cid,**account_screen(by[cid])) for cid in members]
            full=[r for cid in members for r in by[cid] if r['window_id']=='FULL_HISTORY']
            recent=[r for cid in members for r in by[cid] if r['window_id']=='RECENT_2021_TO_CUTOFF']
            complete=all(len(by[cid])==14 and len({(r['window_id'],r['financing_mode']) for r in by[cid]})==14 for cid in members)
            def span(items,field,op):return op((r[field] for r in items if r.get(field) is not None),default=None)
            result.append(dict(distance_low=DISTANCE[i],distance_high=DISTANCE[i+1],
                lifetime_low=LIFETIME[j],lifetime_high=LIFETIME[j+1],body_min=1.25,
                member_candidates=members,all_member_accounts_complete=complete,
                conditional_plateau_screen_pass=complete and all(s['screen_pass'] for s in screens),member_screens=screens,
                full_cagr_min=span(full,'conditional_nav_cagr_pct',min),full_cagr_max=span(full,'conditional_nav_cagr_pct',max),
                recent_cagr_min=span(recent,'conditional_nav_cagr_pct',min),recent_cagr_max=span(recent,'conditional_nav_cagr_pct',max),
                min_positive_eras=min(s['positive_eras'] for s in screens),
                full_worst_sampled_nav_drawdown_pct=span(full,'sampled_mid_nav_drawdown_pct',min),
                recent_worst_sampled_nav_drawdown_pct=span(recent,'sampled_mid_nav_drawdown_pct',min),
                min_full_closed_trades=min((r['closed_trades'] for r in full),default=0),
                min_recent_closed_trades=min((r['closed_trades'] for r in recent),default=0),
                account_lineage_uncertainty_present=any(s['uncertain_account_rows'] for s in screens),
                automatic_selection=False,full_portfolio_validated=False))
    return result


def comparisons(rows):
    controls={(r['distance_max'],r['window_id'],r['financing_mode']):r for r in rows if r['lifetime_bars']==8}
    result=[]
    fields=dict(closed_trades_delta='closed_trades',cagr_difference_percentage_points='conditional_nav_cagr_pct',
        sampled_nav_drawdown_difference_percentage_points='sampled_mid_nav_drawdown_pct',
        mean_net_r_difference='mean_net_r_actual_entry_risk',mean_net_r_without_best_five_difference='mean_net_r_without_best_five',
        ending_nav_difference_gbp='conditional_ending_nav_gbp',financing_difference_gbp='financing_gbp')
    for row in rows:
        old=controls.get((row['distance_max'],row['window_id'],row['financing_mode']))
        if old is None:continue
        delta={label:row[key]-old[key] if row.get(key) is not None and old.get(key) is not None else None for label,key in fields.items()}
        result.append(dict(candidate_id=row['candidate_id'],control_candidate_id=old['candidate_id'],
            distance_max=row['distance_max'],body_min=1.25,lifetime_bars=row['lifetime_bars'],
            window_id=row['window_id'],financing_mode=row['financing_mode'],**delta,
            closed_trades_ratio=row['closed_trades']/old['closed_trades'] if old['closed_trades'] else None,
            interpretation='DIFFERENCE_BETWEEN_FRESH_ISOLATED_ACCOUNTS; NOT_INCREMENTAL_LIVE32_RETURN',
            includes_changed_signal_sequence_and_compounding=True,uncertainty_preserved=True))
    return result


def review_text(done,accounts):
    return f'''# AUD/USD long: setup-lifetime account matrix

Completed cells: {done}/9; completed fresh GBP100 accounts: {accounts}/126.
Lifetimes: 8, 12, 16 M15 bars. Distance caps: 1.00, 1.25, 1.50 H1 ATR.
Minimum engulfing body remains 1.25 M15 ATR. RR3, 15-tick entry bound,
10-tick stop buffer, 1% NAV risk and GBP execution/account engine are fixed.

Each cell regenerates its own chronological setup state. The setup can enter
on ages 1..N; expiry happens BEFORE bar N+1. Trend failure or missing M15 bars
resets it. Filter failure retains the original touch and all intervening lows.
An accepted qualification consumes the setup even if execution later rejects
or an existing position blocks entry. No late diagnostic observations are
added onto baseline signals, and no post-hoc outcome selects a signal.

Before pricing the six new cells, all three eight-bar controls must reproduce
exact extension signals, opportunities and 42 complete account payloads.
The unfiltered eight-bar baseline must match the original 4,086 signal records.
Mismatch stops; a failed parity result is not saved as a valid account checkpoint.

account_matrix.csv: full history, five era resets and recent 2021-to-cutoff,
each with frozen-14-Oct-2026 funding and adverse6 annual financing assumptions.
The partial 2025 era ends at the frozen September-2026 cutoff. Windows overlap
and reset independently; do not sum counts/returns. Full warm-up begins at the
original observed eligibility, not a synthetic pre-2005 history.

control_comparisons.csv: differences against the corresponding eight-bar
fresh account at the SAME distance/window/funding. These are conditional
isolated-account differences, not the marginal return of adding a strategy
to the live portfolio. A positive drawdown difference means a less negative
sampled drawdown. Account stops, execution admission and compounding can alter
results beyond the new signals. No simple additional-trade win rate is claimed.

plateau_regions.csv applies the UNCHANGED prior screen to each of four adjacent
2x2 distance/lifetime regions. Minimum samples: full 200, recent 60, each era 20;
positive full/recent NAV and mean net R after removing the best five trades in
BOTH funding modes, plus at least four of five supported frozen-funding eras
profitable. Every member must pass. No sample threshold has been relaxed.
Zero credited positive eras can mean insufficient samples; inspect supported_eras.

Original candle gaps, ambiguous barriers, stale valuation quotes and conditional
STOP_FIRST/END_ASC ordering remain explicitly flagged. They are not mathematical
return bounds. Coverage-audit candles are NOT imported and uncertain paths are
not silently resolved. Sampled NAV drawdown is not worst intrabar drawdown.
Funding and current broker terms are assumptions, not reconstructed historical
rates/terms. This is previously seen history, not an untouched holdout.

Previous source windows, including both matrix and extension windows, are
frozen by exact identities and checksums. Old path/candle databases are read-only;
geometry is reused only with matching native-history and engine identity.
Private candle cache defaults to 64 MiB; free reserve 96 MiB. Finished account
checkpoints resume independently. Full evidence ZIP streams without a second
large archive on disk. Download /results-summary.zip for the compact review.

No live order, executor/strategy change, RR sweep, automatic activation,
combined live32 admission/overlap validation or forward-validation claim.
'''
