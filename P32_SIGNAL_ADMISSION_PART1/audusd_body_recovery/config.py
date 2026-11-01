from copy import deepcopy
from audusd_matrix.reporting import SCREEN
from audusd_combinations.config import candidates as preceding_candidates
VERSION='AUDUSD_PULLBACK_BODY_RECOVERY_V1_2026_11_01'
MODES=('FROZEN_14OCT2026_RATES','ADVERSE_6PCT_ANNUAL')
ANCHOR='L16_D1.25_B1.25'
BODIES=(1.0,1.125,1.25)
CLOSE=(.4,.5,.6)
MOMENTUM=(0.,.5)

def base_id(body):return 'L16_D1.25_B'+str(body)
def combination_id(body,i,j):return base_id(body)+'__C'+str(i)+'_M'+str(j)
def candidates():
 old=preceding_candidates();by={r['candidate_id']:r for r in old}
 ids=[ANCHOR]+[ANCHOR+'__previous_close_location_'+str(i) for i in range(3)]+[ANCHOR+'__pre_touch_rise_4h_atr_'+str(j) for j in range(2)]+[combination_id(1.25,i,j) for i in range(3) for j in range(2)]
 # Reproduce all 12 old configurations first; only the rule metadata marks controls.
 controls=[dict(deepcopy(by[cid]),is_control=True) for cid in ids]
 out=controls[:]
 for body in BODIES[:-1]:
  for control in controls:
   r=deepcopy(control)
   for key in ('candidate_id','anchor_id','close_control','momentum_control'):
    if r.get(key):r[key]=r[key].replace(ANCHOR,base_id(body))
   r.update(body_min=body,is_control=False,original_body_control=control['candidate_id'])
   out.append(r)
 return out
OVERLAPS=tuple(r['candidate_id'] for r in candidates() if r.get('is_control'))
def configuration():
 return dict(version=VERSION,candidate_rules=candidates(),bodies=list(BODIES),previous_close_caps=list(CLOSE),pre_touch_momentum_minima=list(MOMENTUM),
  accounts=504,candidates=36,combinations=18,new_configurations=24,exact_control_candidates=12,exact_control_accounts=168,
  rr=3.,entry_limit_ticks=15,stop_buffer_ticks=10,initial_gbp=100.,risk_percent=1.,account_scenario='END_ASC',account_windows=7,funding_modes=list(MODES),
  lifetime_bars=16,distance_max=1.25,
  selection='ONE_PREDECLARED_BODY_RELAXATION_CONFIRMATION; NO_AUTOMATIC_WINNER',
  plateau_screen=dict(SCREEN,regions='SIX_WITHIN_BODY_2X2_CLOSE_MOMENTUM_BLOCKS_AND_FOUR_ADJACENT_BODY_2X2X2_BLOCKS; EVERY_MEMBER_MUST_PASS'),
  source_window='Original observed eligibility 2005-02-09T12:00Z through 2026-09-20T18:15Z',
  rejection_policy='Failed filter retains original touch and extreme; accepted qualification consumes before admission.',
  feature_policy='Identical previous close and original first-touch momentum; completed causal candles only.',
  hypothesis='Established context filters may allow smaller engulf bodies to recover frequency without losing quality; removed and newly admitted trades are measured independently.',
  no_new_features=True,no_lifetime_search=True,no_rr_search=True,no_timing_search=True,
  historical_financing_known=False,untouched_holdout=False,automatic_selection=False,full_portfolio_validated=False,orders_supported=False,orders_sent=False,
  limitations='All history seen; correlated trials; original gaps, stale marks, barrier ordering and unknown entries retained; modeled funding and current terms; sampled NAV DD.',
  next_steps=['Review recovered and displaced trades, full/recent and reset eras, both funding cases and neighbouring settings','Keep original 200/60/20 sample and robustness requirements','Freeze this family after this bounded confirmation; separate resumption-trigger study next'])
