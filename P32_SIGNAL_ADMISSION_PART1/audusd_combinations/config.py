from audusd_matrix.reporting import SCREEN
from audusd_conditional.config import candidates as preceding_candidates
VERSION='AUDUSD_PULLBACK_COMBINATIONS_V1_2026_10_27'
MODES=('FROZEN_14OCT2026_RATES','ADVERSE_6PCT_ANNUAL')
ANCHORS=('L16_D1.25_B1.25','L24_D1.0_B1.25')
CLOSE=(.4,.5,.6)
MOMENTUM=(0.,.5,1.)
FEATURES=('previous_close_location','pre_touch_rise_4h_atr')
def candidates():
 old=preceding_candidates();by={r['candidate_id']:r for r in old};out=[]
 # FOURTEEN historical controls first, with unchanged IDs and signal payloads.
 for anchor in ANCHORS:
  out.append(dict(by[anchor],is_control=True))
  for feature in FEATURES:
   for i in range(3):out.append(dict(by[anchor+'__'+feature+'_'+str(i)],is_control=True))
 for anchor in ANCHORS:
  for i,close in enumerate(CLOSE):
   for j,momentum in enumerate(MOMENTUM):
    out.append(dict(by[anchor],candidate_id=anchor+'__C'+str(i)+'_M'+str(j),kind='COMBINATION',anchor_id=anchor,
     close_max=close,momentum_min=momentum,close_control=anchor+'__previous_close_location_'+str(i),momentum_control=anchor+'__pre_touch_rise_4h_atr_'+str(j),
     filters=[dict(feature='previous_close_location',operator='<=',threshold=close),dict(feature='pre_touch_rise_4h_atr',operator='>=',threshold=momentum)]))
 return out
OVERLAPS=tuple(r['candidate_id'] for r in candidates() if r.get('is_control'))
def combination_id(anchor,i,j):return anchor+'__C'+str(i)+'_M'+str(j)
def configuration():
 return dict(version=VERSION,candidate_rules=candidates(),anchors=list(ANCHORS),previous_close_caps=list(CLOSE),pre_touch_momentum_minima=list(MOMENTUM),
  accounts=448,candidates=32,combinations=18,exact_control_candidates=14,exact_control_accounts=196,
  rr=3.,entry_limit_ticks=15,stop_buffer_ticks=10,initial_gbp=100.,risk_percent=1.,account_scenario='END_ASC',account_windows=7,funding_modes=list(MODES),
  selection='PREDECLARED_BOUNDED_TWO_FACTOR_GRID; NO_AUTOMATIC_WINNER',
  plateau_screen=dict(SCREEN,region='FOUR_MEMBERS_OF_EACH_ADJACENT_CLOSE_BY_MOMENTUM_2X2_BLOCK_WITHIN_ANCHOR'),
  source_window='Original observed eligibility 2005-02-09T12:00Z through 2026-09-20T18:15Z',
  rejection_policy='Failed condition retains original touch and extreme until expiry/gap/trend reset. Qualification consumes before order admission.',
  feature_policy='Identical definitions and all saved feature fields to previous one-feature study; completed H1 only, no future candles',
  no_new_features=True,no_lifetime_search=True,no_rr_search=True,no_timing_search=True,
  historical_financing_known=False,untouched_holdout=False,automatic_selection=False,full_portfolio_validated=False,orders_supported=False,orders_sent=False,
  limitations='Original candle gaps, barrier timing assumptions, stale marks and omitted unknown entries retained; modeled funding; sampled NAV DD',
  next_steps=['Review combined and single-factor accounts, attribution, weak eras and neighbours','Only a justified bounded local confirmation','Separate pullback-resumption trigger study; no automatic live activation'])
