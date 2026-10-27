from audusd_matrix.reporting import SCREEN
VERSION='AUDUSD_PULLBACK_CONDITIONAL_V1_2026_10_25'
MODES=('FROZEN_14OCT2026_RATES','ADVERSE_6PCT_ANNUAL')
def base_id(d,n):return 'L'+str(n)+'_D'+str(d)+'_B1.25'
OVERLAPS=tuple(base_id(d,n) for n in (16,20,24) for d in (1.0,1.25))
ANCHORS=('M_D1.25_B1.0','L16_D1.25_B1.25','L24_D1.0_B1.25')
FEATURES={
 'm15_atr_ratio':('<=',(.9,1.1,1.3),'RELATIVE_VOLATILITY'),
 'h1_atr_ratio':('<=',(.9,1.1,1.3),'RELATIVE_VOLATILITY'),
 'pre_touch_rise_4h_atr':('>=',(0.,.5,1.),'PRECEDING_MOVEMENT'),
 'retracement_fraction':('<=',(.25,.5,.75),'PRECEDING_MOVEMENT'),
 'range_atr':('>=',(1.25,1.5,1.75),'CANDLE_QUALITY'),
 'previous_close_location':('<=',(.4,.5,.6),'CANDLE_QUALITY'),
 'prior_low20_distance_atr':('<=',(.25,.5,.75),'PRICE_STRUCTURE'),
 'prior_low60_distance_atr':('<=',(.25,.5,.75),'PRICE_STRUCTURE')}
def controls():
 return [dict(candidate_id=base_id(d,n),kind='CONTROL',distance_max=d,body_min=1.25,lifetime_bars=n,rr=3.) for n in (16,20,24) for d in (1.0,1.25)]+[dict(candidate_id=ANCHORS[0],kind='BROAD_CONTROL',distance_max=1.25,body_min=1.,lifetime_bars=8,rr=3.)]
def candidates():
 out=controls();by={r['candidate_id']:r for r in out}
 for anchor in ANCHORS:
  for feature,(operator,levels,family) in FEATURES.items():
   for n,value in enumerate(levels):
    out.append(dict(by[anchor],candidate_id=anchor+'__'+feature+'_'+str(n),kind='ONE_FEATURE',anchor_id=anchor,feature=feature,operator=operator,threshold=value,family=family))
 return out
def configuration():
 return dict(version=VERSION,candidate_rules=candidates(),anchors=list(ANCHORS),features=FEATURES,accounts=len(candidates())*14,
  account_windows=7,funding_modes=list(MODES),rr=3.,entry_limit_ticks=15,stop_buffer_ticks=10,initial_gbp=100.,risk_percent=1.,
  account_scenario='END_ASC',overlapping_controls=list(OVERLAPS),untouched_holdout=False,automatic_selection=False,
  full_portfolio_validated=False,orders_supported=False,orders_sent=False,historical_financing_known=False,
  plateau_screen=SCREEN,selection='NO_AUTOMATIC_SELECTION; RESEARCH_LEADS_ONLY; NO_COMBINATIONS',
  next_steps=['Review conditional effects and trade attribution','Only justified two-factor combinations','Local plateau confirmation','Independent confirmation and portfolio admission if earned','Separate resumption-trigger study'],
  source_window='2005 observed warmup through 2026-09-20T18:15Z',
  feature_definitions=dict(relative_volatility='ATR14 / mean of previous 50 finite observed ATR14 values; signal ATR allowed only after its close',
   pre_touch_rise='Close immediately before original touch minus close 16 M15 bars earlier, divided by touch ATR; require contiguous observations',
   retracement_fraction='Positive decline from pre-touch close to all-low setup extreme divided by positive pre-touch 4h rise; nonpositive impulse is missing',
   price_structure='max(0, signal low minus minimum of PRECEDING 20 or 60 completed M15 lows) / signal ATR; separate sweep flag recorded',
   previous_close='Previous M15 (close-low)/(high-low); flat candle is missing'),
  rejection_policy='Filter failure keeps original touch/extreme alive; accepted qualification consumes before execution; expiry/trend/gap rules unchanged',
  limitations='Known gaps, ambiguous barrier order, stale valuations and unknown entries retained; sampled drawdown; modeled historical funding; examined history')
