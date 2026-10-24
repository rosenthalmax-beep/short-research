"""Strict neighbouring regions across RR, distance and lifetime; no peak selection."""
from audusd_matrix.reporting import account_screen
from .config import DISTANCE,LIFETIME,RR


def regions(rules,rows):
    by={r['candidate_id']:[a for a in rows if a['candidate_id']==r['candidate_id']] for r in rules}
    grid={(r['distance_max'],r['lifetime_bars'],r['rr']):r['candidate_id'] for r in rules}
    result=[]
    for j in range(2):
        for k in range(2):
            members=[grid[d,n,rr] for d in DISTANCE for n in LIFETIME[j:j+2] for rr in RR[k:k+2]]
            screens=[dict(candidate_id=cid,**account_screen(by[cid])) for cid in members]
            full=[r for cid in members for r in by[cid] if r['window_id']=='FULL_HISTORY']
            recent=[r for cid in members for r in by[cid] if r['window_id']=='RECENT_2021_TO_CUTOFF']
            complete=all(len(by[cid])==14 and len({(r['window_id'],r['financing_mode']) for r in by[cid]})==14 for cid in members)
            def span(items,field,op):return op((r[field] for r in items if r.get(field) is not None),default=None)
            result.append(dict(distance_low=DISTANCE[0],distance_high=DISTANCE[1],lifetime_low=LIFETIME[j],lifetime_high=LIFETIME[j+1],
                rr_low=RR[k],rr_high=RR[k+1],body_min=1.25,member_count=8,member_candidates=members,
                all_member_accounts_complete=complete,conditional_plateau_screen_pass=complete and all(s['screen_pass'] for s in screens),member_screens=screens,
                full_cagr_min=span(full,'conditional_nav_cagr_pct',min),full_cagr_max=span(full,'conditional_nav_cagr_pct',max),
                recent_cagr_min=span(recent,'conditional_nav_cagr_pct',min),recent_cagr_max=span(recent,'conditional_nav_cagr_pct',max),
                full_worst_sampled_nav_drawdown_pct=span(full,'sampled_mid_nav_drawdown_pct',min),
                recent_worst_sampled_nav_drawdown_pct=span(recent,'sampled_mid_nav_drawdown_pct',min),
                min_positive_eras=min(s['positive_eras'] for s in screens),
                min_full_closed_trades=min((r['closed_trades'] for r in full),default=0),
                min_recent_closed_trades=min((r['closed_trades'] for r in recent),default=0),
                account_lineage_uncertainty_present=any(s['uncertain_account_rows'] for s in screens),
                automatic_selection=False,full_portfolio_validated=False))
    return result


def comparisons(rows):
    controls={(r['distance_max'],r['lifetime_bars'],r['window_id'],r['financing_mode']):r for r in rows if r['rr']==3.}
    fields=dict(closed_trades_delta='closed_trades',cagr_difference_percentage_points='conditional_nav_cagr_pct',
        sampled_nav_drawdown_difference_percentage_points='sampled_mid_nav_drawdown_pct',
        mean_net_r_difference='mean_net_r_actual_entry_risk',mean_net_r_without_best_five_difference='mean_net_r_without_best_five',
        ending_nav_difference_gbp='conditional_ending_nav_gbp',financing_difference_gbp='financing_gbp',
        win_rate_difference_percentage_points='net_win_rate_pct')
    result=[]
    for row in rows:
        old=controls.get((row['distance_max'],row['lifetime_bars'],row['window_id'],row['financing_mode']))
        if old is None:continue
        delta={label:row[key]-old[key] if row.get(key) is not None and old.get(key) is not None else None for label,key in fields.items()}
        result.append(dict(candidate_id=row['candidate_id'],control_candidate_id=old['candidate_id'],distance_max=row['distance_max'],
            body_min=1.25,lifetime_bars=row['lifetime_bars'],rr=row['rr'],window_id=row['window_id'],financing_mode=row['financing_mode'],**delta,
            closed_trades_ratio=row['closed_trades']/old['closed_trades'] if old['closed_trades'] else None,
            interpretation='FRESH_ACCOUNT_COMPARISON_WITH_CHANGED_EXIT_AND_ADMISSION; NOT_INCREMENTAL_LIVE32_RETURN',
            uncertainty_preserved=True))
    return result


def review_text(done,accounts):
    return f'''# AUD/USD pullback: coarse RR comparison

Completed cells: {done}/18; fresh isolated GBP100 accounts: {accounts}/252.
RR 2.5 / 3.0 / 3.5; lifetimes 16 / 20 / 24 M15 bars; maximum H1 distance
1.00 / 1.25 ATR. Engulfing body >=1.25 M15 ATR, 1% NAV risk, 15-tick adverse
entry bound, 10-tick stop buffer, filters, consumption and source cutoff fixed.

The six RR3 cells run FIRST and must match exact saved signals, standalone
opportunities and all 84 complete prior account payloads. The unfiltered
eight-bar baseline must match all 4,086 original signal records. The qualification
sequence check requires identical entry times, stops and features across RR;
only target, RR and candidate identity change. All targets use the existing
broker rounding from reference entry/stop. No ex-post R scaling is used.

Every candidate's exit paths are recomputed or exactly geometry-reused with
RR in the cache identity. Each account independently reruns sequential admission,
position blocking, sizing, margin, financing, conversion and compounding.
A different target can alter later trade admission. Qualifications are consumed
before execution/admission outcomes, as in the original strategy engine.

account_matrix.csv contains full history, five fresh era resets and recent
January-2021-to-cutoff, with frozen-14-Oct-2026 funding and adverse6 annual
financing assumptions. Funding/current broker terms are modeled assumptions,
not reconstructed historical schedules. Windows overlap; do not sum results.
The original observed eligibility and 20-Sep-2026 cutoff remain fixed.

control_comparisons.csv compares each RR with RR3 at the SAME lifetime,
distance, window and funding. These are conditional fresh-account differences,
not a live32 incremental return or an additional-trade win rate.

plateau_regions.csv contains FOUR adjacent 2x2x2 regions across RR, lifetime
and distance (eight members each). ALL EIGHT cells must meet the unchanged
per-account criteria: full >=200 closes, recent >=60, every era >=20; positive
full/recent NAV and mean net R without the best five trades in BOTH funding
modes; at least four supported frozen-funding eras profitable. No numeric
threshold is relaxed. An isolated peak cannot pass a three-dimensional region.
Unsupported eras are not counted as losses; check supported_eras separately.

The source generation/pricing/account engine is unchanged. Original candle gaps,
barrier timing ambiguity, stale valuations and unknown-entry omissions remain
explicit. The coverage-audit candles are not imported; no uncertain outcomes
are silently resolved. STOP_FIRST/END_ASC are conditional modeling assumptions,
not return bounds. Sampled NAV drawdown is not worst intrabar drawdown.
All history has previously been examined; no untouched holdout claim is made.

Previous source indices are frozen through the reviewed witness chain, with old
path/candle databases read-only. Private raw cache defaults to 64 MiB, free reserve
96 MiB. Finished accounts checkpoint independently; full results stream without
a second large archive on disk. Download /results-summary.zip for review.

No further lifetime expansion, new filters, automatic peak selection, live
orders/activation or combined live32 portfolio validation is part of this run.
'''
