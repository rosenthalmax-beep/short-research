"""The original sample/profitability screen applied to four new adjacent blocks."""
from audusd_matrix.reporting import account_screen
from .config import DISTANCE,BODY


def regions(rules,rows):
    by={r['candidate_id']:[a for a in rows if a['candidate_id']==r['candidate_id']] for r in rules}
    grid={(r['distance_max'],r['body_min']):r['candidate_id'] for r in rules}
    result=[]
    for i in range(2):
        for j in range(2):
            members=[grid[d,b] for d in DISTANCE[i:i+2] for b in BODY[j:j+2]]
            screens=[dict(candidate_id=cid,**account_screen(by[cid])) for cid in members]
            full=[r for cid in members for r in by[cid] if r['window_id']=='FULL_HISTORY']
            recent=[r for cid in members for r in by[cid] if r['window_id']=='RECENT_2021_TO_CUTOFF']
            complete=all(len(by[cid])==14 and len({(r['window_id'],r['financing_mode']) for r in by[cid]})==14 for cid in members)
            def span(items,field,op):return op((r[field] for r in items if r.get(field) is not None),default=None)
            result.append(dict(distance_low=DISTANCE[i],distance_high=DISTANCE[i+1],body_low=BODY[j],body_high=BODY[j+1],
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
