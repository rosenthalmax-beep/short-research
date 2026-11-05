from copy import deepcopy
from audusd_matrix.reporting import SCREEN
from audusd_resumption.config import candidates as preceding_candidates,configuration as preceding_configuration
from audusd_resumption.config import MODES,LEGACY_BROAD,LEGACY_SINGLE,LEGACY_QUALITY,ENGULF,RESUMPTION
VERSION='AUDUSD_RESUMPTION_H1_CAP_V1_2026_11_04'
CAPS=(.20,.25,.30)
def cap_id(cap):return 'RES2_H1DIST_'+format(cap,'.2f').replace('.','P')
def candidates():
 prior=preceding_candidates();out=[dict(deepcopy(r),is_control=True) for r in prior]
 base=next(r for r in prior if r['candidate_id']==RESUMPTION)
 for cap in CAPS:
  out.append(dict(deepcopy(base),candidate_id=cap_id(cap),kind='H1_DISTANCE_CAP',is_control=False,distance_max=cap,anchor_id=RESUMPTION))
 return out
OVERLAPS=tuple(r['candidate_id'] for r in preceding_candidates())
def configuration():
 cfg=deepcopy(preceding_configuration())
 cfg.update(version=VERSION,candidate_rules=candidates(),candidates=8,accounts=112,new_mechanisms=0,new_cap_configurations=3,exact_control_candidates=5,exact_control_accounts=70,
  caps=list(CAPS),distance_max='PER_CANDIDATE_RULE; 0.20/0.25/0.30 FOR NEW CAPS',
  trend='Unchanged completed native H1 uptrend; distance from EMA100 divided by native H1 ATR14 is capped INCLUSIVELY at the candidate cap when a trigger qualifies; distance failure does not reset setup',
  sample_screen=dict(SCREEN,region='ALL_MEMBERS_OF_ADJACENT_CAP_PAIRS_AND_FULL_THREE_CAP_SET'),
  plateau_regions_tested=3,selection='PREDECLARED_THREE_NEIGHBOUR_SINGLE_FEATURE_TEST; NO_AUTOMATIC_SELECTION',
  no_new_filter_search=False,no_additional_feature_search=True,filtered_feature_accounts_tested=True,
  hypothesis='Resumption nearer the H1 trend average may improve quality without losing adequate era/full/recent samples; raw-bin outcomes are not filtered account results',
  diagnostics='Same frozen coarse bins; descriptive raw opportunities only. Actual cap performance comes from independently regenerated GBP account replays.',
  next_steps=['Review all three caps, original samples, eras, both funding cases and best-five sensitivity','Inspect removed/new/shared trades and raw state changes versus broad resumption and neighbours','If the neighbourhood is not supported, park this branch; no automatic threshold extension or added filter'])
 return cfg
