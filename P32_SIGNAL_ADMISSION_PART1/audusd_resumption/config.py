from copy import deepcopy
from audusd_matrix.reporting import SCREEN
from audusd_body_recovery.config import candidates as preceding_candidates
VERSION='AUDUSD_PULLBACK_RESUMPTION_PASS1_V1_2026_11_01'
MODES=('FROZEN_14OCT2026_RATES','ADVERSE_6PCT_ANNUAL')
LEGACY_BROAD='L16_D1.25_B1.0'
LEGACY_SINGLE='L16_D1.25_B1.0__previous_close_location_0'
LEGACY_QUALITY='L16_D1.25_B1.25__C2_M1'
ENGULF='RES1_MATCHED_BROAD_ENGULF'
RESUMPTION='RES1_BROAD_PRIOR_HIGH_CLOSE'
OVERLAPS=(LEGACY_BROAD,LEGACY_SINGLE,LEGACY_QUALITY)
def candidates():
 by={r['candidate_id']:r for r in preceding_candidates()}
 out=[dict(deepcopy(by[cid]),is_control=True) for cid in OVERLAPS]
 for cid,trigger in ((ENGULF,'ENGULF'),(RESUMPTION,'PRIOR_HIGH_CLOSE')):
  out.append(dict(candidate_id=cid,kind='BROAD_MECHANISM',is_control=False,trigger=trigger,body_min=None,distance_max=1.25,lifetime_bars=16,rr=3.,diagnostic_features=True))
 return out

def configuration():
 from .diagnostics import bin_protocol
 return dict(version=VERSION,candidate_rules=candidates(),candidates=5,accounts=70,new_mechanisms=2,exact_control_candidates=3,exact_control_accounts=42,
  rr=3.,entry_limit_ticks=15,stop_buffer_ticks=10,initial_gbp=100.,risk_percent=1.,account_scenario='END_ASC',account_windows=7,funding_modes=list(MODES),
  lifetime_bars=16,distance_max=1.25,
  trend='Most recently completed native H1 close above EMA100, EMA100 higher than three observed H1 bars earlier; H1 distance <=1.25 ATR; original freshness/gap policies',
  setup='M15 EMA20 first touch after arming; original extreme retained; 16-bar expiry',
  resumption_trigger='Current completed M15 close > open AND close > preceding completed M15 HIGH AND close > current EMA20; strict inequalities; no engulfing requirement',
  matched_trigger='Exact original bullish body engulf AND close > current EMA20; same setup/context/expiry and no body minimum',
  new_body_floor=None,new_close_or_momentum_filters=False,entry='Completed signal-bar close reference; original next-native-open execution proxy',
  feature_bins=bin_protocol(),diagnostics='Predeclared coarse feature bins on raw independent opportunities; not account filters or filter performance; outcomes followed to fixed study cutoff',
  sample_screen=SCREEN,plateau_regions_tested=0,selection='BROAD_MECHANISM_DIAGNOSTIC; NO_PARAMETER_GRID_OR_AUTOMATIC_WINNER',
  source_window='Observed eligibility 2005-02-09T12:00Z through unchanged 2026-09-20T18:15Z cutoff',
  rejection_policy='Failed trigger/context retains original touch and lows until expiry/gap/trend reset; qualification consumes before execution rejection or blocking',
  no_new_filter_search=True,no_lifetime_search=True,no_rr_search=True,no_timing_search=True,
  historical_financing_known=False,untouched_holdout=False,automatic_selection=False,full_portfolio_validated=False,orders_supported=False,orders_sent=False,
  limitations='All history seen; conditional path ordering; original gaps, stale marks and unknown entries; modeled funding/current terms; sampled NAV DD, not intrabar bound',
  next_steps=['Compare broad mechanisms on raw and gated account outcomes under both costs','Inspect sample/era support, execution loss, feature coverage and best-five sensitivity','Only if supported choose a bounded one-feature study, then justified interactions and neighbouring plateaus'])
