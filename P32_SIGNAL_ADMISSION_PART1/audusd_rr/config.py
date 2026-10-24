from audusd_matrix.reporting import SCREEN
VERSION='AUDUSD_PULLBACK_RR_MATRIX_V1_2026_10_24'
DISTANCE=(1.0,1.25)
LIFETIME=(16,20,24)
RR=(2.5,3.0,3.5)
BODY_MIN=1.25
MODES=('FROZEN_14OCT2026_RATES','ADVERSE_6PCT_ANNUAL')


def base_id(distance,lifetime):return 'L'+str(lifetime)+'_D'+str(distance)+'_B1.25'


def candidates():
    return [dict(candidate_id=(base_id(d,n) if rr==3. else base_id(d,n)+'_RR'+str(rr)),
                 kind='MATRIX',distance_max=d,body_min=BODY_MIN,lifetime_bars=n,rr=rr)
            for rr in (3.0,2.5,3.5) for n in LIFETIME for d in DISTANCE]

OVERLAPS=tuple(base_id(d,n) for n in LIFETIME for d in DISTANCE)


def configuration():
    return dict(version=VERSION,pair='AUD_USD',side='BUY',candidate_rules=candidates(),
        distance_max_values=list(DISTANCE),lifetime_bar_values=list(LIFETIME),rr_values=list(RR),body_min_m15_atr=BODY_MIN,
        overlapping_controls=list(OVERLAPS),accounts=252,account_windows=7,funding_modes=list(MODES),
        initial_gbp=100.,risk_percent=1.,entry_limit_ticks=15,stop_buffer_ticks=10,account_scenario='END_ASC',
        source_window='2005-01-01 THROUGH 2026-09-20T18:15:00Z; ORIGINAL OBSERVED WARMUP',
        target_policy='EXISTING_BROKER_ROUNDING_FROM_REFERENCE_ENTRY_AND_STOP; NO_EX_POST_R_SCALING',
        signal_policy='FIXED_CAUSAL_QUALIFICATIONS_PER_DISTANCE_LIFETIME; RR_HAS_NO_SIGNAL_OR_STOP_FEEDBACK',
        admission_policy='FRESH_ACCOUNT_PER_CANDIDATE; DIFFERENT_EXIT_DURATION_CAN_CHANGE_LATER_ADMISSION',
        rejection_policy='FILTER_FAILURE_RETAINS_SETUP; ACCEPTED_SIGNAL_CONSUMES_SETUP_EVEN_IF_EXECUTION_REJECTS',
        overlap_checks='EXACT_SIGNALS_OPPORTUNITIES_AND_ALL_84_SAVED_ACCOUNT_PAYLOADS',
        plateau_screen=dict(SCREEN,region='EIGHT_MEMBERS_OF_EACH_ADJACENT_2_BY_2_BY_2_RR_DISTANCE_LIFETIME_BLOCK'),
        original_uncertainty_retained=True,coverage_audit_candles_used=False,untouched_holdout=False,
        automatic_selection=False,lifetime_search_finished=True,no_new_filters=True,
        historical_financing_known=False,full_portfolio_validated=False,orders_supported=False,orders_sent=False)
