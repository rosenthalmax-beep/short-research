from audusd_matrix.reporting import SCREEN

VERSION='AUDUSD_PULLBACK_LONG_EXTENSION_V1_2026_10_23'
DISTANCE=(1.0,1.25,1.5)
BODY=(1.0,1.25,1.5)
OVERLAPS=('M_D1.0_B1.0','M_D1.25_B1.0')
MODES=('FROZEN_14OCT2026_RATES','ADVERSE_6PCT_ANNUAL')


def candidates():
    rows=[dict(candidate_id='M_D'+str(d)+'_B'+str(b),kind='MATRIX',distance_max=d,body_min=b)
          for d in DISTANCE for b in BODY]
    # Stop on a reference mismatch before pricing the seven new variants.
    return sorted(rows,key=lambda r:(r['candidate_id'] not in OVERLAPS,
                                     r['distance_max'],r['body_min']))


def configuration():
    return dict(version=VERSION,pair='AUD_USD',side='BUY',candidate_rules=candidates(),
        distance_max_values=list(DISTANCE),body_min_values=list(BODY),overlapping_controls=list(OVERLAPS),
        accounts=126,account_windows=7,funding_modes=list(MODES),rr=3.,initial_gbp=100.,risk_percent=1.,
        entry_limit_ticks=15,stop_buffer_ticks=10,account_scenario='END_ASC',
        source_window='2005-01-01 THROUGH 2026-09-20T18:15:00Z; ORIGINAL OBSERVED WARMUP',
        rejection_policy='FILTER_FAILURE_RETAINS_SETUP; ACCEPTED_SIGNAL_CONSUMES_SETUP_EVEN_IF_EXECUTION_REJECTS',
        source_path_policy='UNCHANGED_V2; ORIGINAL_UNCERTAINTY_RETAINED; NO_COVERAGE_AUDIT_CORRECTIONS',
        overlap_checks='EXACT_SIGNALS_OPPORTUNITIES_AND_ALL_28_SAVED_ACCOUNT_PAYLOADS',
        plateau_screen=SCREEN,untouched_holdout=False,automatic_selection=False,
        historical_financing_known=False,full_portfolio_validated=False,orders_supported=False,orders_sent=False)
