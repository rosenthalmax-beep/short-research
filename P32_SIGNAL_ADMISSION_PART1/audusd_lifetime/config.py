from audusd_matrix.reporting import SCREEN

VERSION='AUDUSD_PULLBACK_LIFETIME_MATRIX_V1_2026_10_23'
DISTANCE=(1.0,1.25,1.5)
LIFETIME=(8,12,16)
BODY_MIN=1.25
OVERLAPS=tuple('M_D'+str(d)+'_B1.25' for d in DISTANCE)
MODES=('FROZEN_14OCT2026_RATES','ADVERSE_6PCT_ANNUAL')


def candidates():
    # Keep exact eight-bar IDs and signal payloads for strict prior-run parity.
    return [dict(candidate_id=('M_D'+str(d)+'_B1.25' if n==8 else 'L'+str(n)+'_D'+str(d)+'_B1.25'),
        kind='MATRIX',distance_max=d,body_min=BODY_MIN,lifetime_bars=n) for n in LIFETIME for d in DISTANCE]


def configuration():
    return dict(version=VERSION,pair='AUD_USD',side='BUY',candidate_rules=candidates(),
        distance_max_values=list(DISTANCE),lifetime_bar_values=list(LIFETIME),body_min_m15_atr=BODY_MIN,
        overlapping_controls=list(OVERLAPS),accounts=126,account_windows=7,funding_modes=list(MODES),
        rr=3.,initial_gbp=100.,risk_percent=1.,entry_limit_ticks=15,stop_buffer_ticks=10,
        account_scenario='END_ASC',expiry='EXPIRE_BEFORE_PROCESSING_BAR_WHEN_INDEX_MINUS_TOUCH_INDEX_GE_LIFETIME',
        source_window='2005-01-01 THROUGH 2026-09-20T18:15:00Z; ORIGINAL OBSERVED WARMUP',
        rejection_policy='FILTER_FAILURE_RETAINS_SETUP; ACCEPTED_SIGNAL_CONSUMES_SETUP_EVEN_IF_EXECUTION_REJECTS',
        state_policy='INDEPENDENT_CAUSAL_REGENERATION_FOR_EVERY_CELL; LOWS_FROM_ORIGINAL_TOUCH; GAP_AND_H1_TREND_RESETS',
        source_path_policy='UNCHANGED_V2; ORIGINAL_UNCERTAINTY_RETAINED; NO_COVERAGE_AUDIT_CORRECTIONS',
        overlap_checks='EXACT_SIGNALS_OPPORTUNITIES_AND_ALL_42_SAVED_ACCOUNT_PAYLOADS',
        plateau_screen=SCREEN,untouched_holdout=False,automatic_selection=False,
        historical_financing_known=False,full_portfolio_validated=False,orders_supported=False,orders_sent=False)
