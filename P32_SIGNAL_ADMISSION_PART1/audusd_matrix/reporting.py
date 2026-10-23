"""Adjacent 2x2 account-region screens, period tables and honest denominators."""
import gzip
import json
import math
import statistics
from collections import Counter
from pullback_lab import core as e
from pullback_lab.reports import stats,cohorts,warmup_coverage
from .signals import DISTANCE,BODY

SCREEN=dict(min_full_closed_trades=200,min_recent_closed_trades=60,min_era_closed_trades=20,
            min_positive_eras=4,region='FOUR_MEMBERS_OF_EACH_ADJACENT_2_BY_2_BLOCK',
            positive_full_recent_frozen_and_adverse6=True,positive_full_recent_without_best_five=True,
            scope='CONDITIONAL_ACCOUNT_SCREEN; NO_STATISTICAL_SIGNIFICANCE_OR_PORTFOLIO_VALIDATION')


def augment(result,start,end):
    summary=result['summary'];trades=result['trades'];cash_rows=result['cash_events']
    # Verify booked cash before admitting any account into matrix summaries.
    cash_sum=sum(r['cash_gbp'] for r in cash_rows)
    e.require(math.isclose(summary['ending_balance_gbp'],100.+cash_sum,abs_tol=.00011),'Account cash ledger failed')
    e.require(math.isclose(summary['ending_balance_gbp'],100.+summary['price_pl_gbp']+summary['financing_gbp'],abs_tol=.0002),'Account P/L and financing failed')
    e.require(summary['max_positions']<=1,'Long-only per-strategy pyramiding failed')
    values=[t['net_r_actual_entry_risk'] for t in trades if t['net_r_actual_entry_risk'] is not None]
    years=(e.dt(end)-e.dt(start)).total_seconds()/(365.25*86400)
    return dict(summary,closed_trades_per_year=len(trades)/years if years else None,
        mean_net_r_without_best_five=statistics.fmean(sorted(values)[:-5]) if len(values)>5 else None,
        ledger_reconciliation='PASS',all_history_seen=True,full_portfolio_validated=False)


def account_screen(rows):
    index={(r['window_id'],r['financing_mode']):r for r in rows}
    reasons=[];era_positive=0;era_supported=0
    for wid,min_n in [('FULL_HISTORY',SCREEN['min_full_closed_trades']),('RECENT_2021_TO_CUTOFF',SCREEN['min_recent_closed_trades'])]:
        for mode in ('FROZEN_14OCT2026_RATES','ADVERSE_6PCT_ANNUAL'):
            row=index.get((wid,mode))
            if row is None:reasons.append(wid+'_'+mode+'_NOT_COMPLETE');continue
            if row['closed_trades']<min_n:reasons.append(wid+'_'+mode+'_SMALL_SAMPLE')
            if row['conditional_ending_nav_gbp'] is None or row['conditional_ending_nav_gbp']<=100:reasons.append(wid+'_'+mode+'_NAV_NOT_POSITIVE')
            if row['mean_net_r_without_best_five'] is None or row['mean_net_r_without_best_five']<=0:reasons.append(wid+'_'+mode+'_WITHOUT_BEST_FIVE_NOT_POSITIVE')
    eras=[r for r in rows if r['window_id'].startswith('ERA_') and r['financing_mode']=='FROZEN_14OCT2026_RATES']
    for r in eras:
        if r['closed_trades']>=SCREEN['min_era_closed_trades']:
            era_supported+=1;era_positive+=r['conditional_ending_nav_gbp'] is not None and r['conditional_ending_nav_gbp']>100
    if len(eras)!=5 or era_supported!=5:reasons.append('ERA_SAMPLE_OR_COVERAGE_INSUFFICIENT')
    if era_positive<SCREEN['min_positive_eras']:reasons.append('INSUFFICIENT_POSITIVE_ERAS')
    return dict(screen_pass=not reasons,screen_reasons=reasons,positive_eras=era_positive,supported_eras=era_supported,
                uncertain_account_rows=sum(bool(r['uncertain_account_lineage']) for r in rows))


def regions(rules,rows):
    by={r['candidate_id']:[x for x in rows if x['candidate_id']==r['candidate_id']] for r in rules}
    grid={(r['distance_max'],r['body_min']):r for r in rules if r['kind']=='MATRIX'}
    result=[]
    for i in range(len(DISTANCE)-1):
        for j in range(len(BODY)-1):
            members=[grid[d,b]['candidate_id'] for d in DISTANCE[i:i+2] for b in BODY[j:j+2]]
            screens=[dict(candidate_id=cid,**account_screen(by[cid])) for cid in members]
            full=[r for cid in members for r in by[cid] if r['window_id']=='FULL_HISTORY']
            recent=[r for cid in members for r in by[cid] if r['window_id']=='RECENT_2021_TO_CUTOFF']
            complete=all(len(by[cid])==14 for cid in members)
            result.append(dict(distance_low=DISTANCE[i],distance_high=DISTANCE[i+1],body_low=BODY[j],body_high=BODY[j+1],
                member_candidates=members,all_member_accounts_complete=complete,
                conditional_plateau_screen_pass=complete and all(s['screen_pass'] for s in screens),
                member_screens=screens,full_cagr_min=min((r['conditional_nav_cagr_pct'] for r in full if r['conditional_nav_cagr_pct'] is not None),default=None),
                full_cagr_max=max((r['conditional_nav_cagr_pct'] for r in full if r['conditional_nav_cagr_pct'] is not None),default=None),
                recent_cagr_min=min((r['conditional_nav_cagr_pct'] for r in recent if r['conditional_nav_cagr_pct'] is not None),default=None),
                recent_cagr_max=max((r['conditional_nav_cagr_pct'] for r in recent if r['conditional_nav_cagr_pct'] is not None),default=None),
                min_positive_eras=min(s['positive_eras'] for s in screens),
                full_worst_sampled_nav_drawdown_pct=min((r.get('sampled_mid_nav_drawdown_pct') for r in full if r.get('sampled_mid_nav_drawdown_pct') is not None),default=None),
                recent_worst_sampled_nav_drawdown_pct=min((r.get('sampled_mid_nav_drawdown_pct') for r in recent if r.get('sampled_mid_nav_drawdown_pct') is not None),default=None),
                min_full_closed_trades=min((r['closed_trades'] for r in full),default=0),
                min_recent_closed_trades=min((r['closed_trades'] for r in recent),default=0),
                automatic_selection=False,full_portfolio_validated=False,
                account_lineage_uncertainty_present=any(s['uncertain_account_rows'] for s in screens)))
    return result


def opportunity_rows(rule,opps,start,end,eligible):
    start,end=e.dt(start),e.dt(end)
    rows=[]
    for p in cohorts(start,end):
        subset=[r for r in opps if p['start']<=r['entry_utc']<p['end']]
        rows.append(dict(**rule,entry_cohort=p['window_id'],**warmup_coverage(p['start'],p['end'],eligible),**stats(subset)))
    return rows
