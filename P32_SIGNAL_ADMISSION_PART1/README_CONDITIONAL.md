# AUD/USD pullback conditional-feature study

This additive research package preserves all predecessor code and original GBP
pricing/accounting modules. It adds 72 one-feature variants to seven controls:

| Anchor | Lifetime | H1 distance cap | Body minimum |
|---|---:|---:|---:|
| Broader M_D1.25_B1.0 | 8 | 1.25 ATR | 1.00 M15 ATR |
| Quality L16_D1.25_B1.25 | 16 | 1.25 ATR | 1.25 M15 ATR |
| Quality L24_D1.0_B1.25 | 24 | 1.00 ATR | 1.25 M15 ATR |

The six old RR3 controls run first and must match all 84 previous lifetime-
extension account payloads exactly. The seventh broader control uses the
previously tested original eight-bar geometry; its generator is compared with
the unchanged predecessor, but it is not counted in the 84 payload comparisons.

Each anchor receives eight numerical features with three thresholds apiece:
M15/H1 ATR ratios; pre-touch four-hour rise and retracement fraction; range
and previous-bar close location; distance from preceding 20/60-bar lows.
Four conceptual families, no combinations, no new timing or RR searches.

Definitions and missing-value policies are frozen in /protocol. Relative ATR is
normalized against the previous 50 observed finite ATR14 values, not absolute
pips. The pre-touch rise ends before the FIRST EMA touch. Retracement fraction
requires positive preceding rise. Prior lows exclude the signal candle and
windows crossing an observed M15 gap are not eligible. Higher-timeframe
features use completed native H1 observations only. No future swing pivots.

Each candidate has its own original-touch state. A failed feature keeps the
setup and lowest low alive until expiry, gap or trend reset; a qualification
consumes it even if later blocked or rejected. Thus new rules regenerate full
signals and replay fresh accounts rather than deleting old trade rows.

Every candidate runs seven windows under both frozen-2026-rate and adverse6
funding assumptions: 79 x 7 x 2 = 1106 accounts. RR3, 15-tick entry limit,
10-tick stop buffer, GBP100 and 1% NAV risk stay fixed. Existing spread,
conversion, whole-unit sizing, margin, funding and chronology remain unchanged.

Summary files: account_matrix, control_comparisons, trade_attribution,
opportunity_matrix, candidate_screens and plateau_regions. The last contains
three-neighbour descriptive diagnostics, NOT approval of a combined plateau.
Candidate screens preserve the existing 200/60/20 sample conventions. Do not
promote a sparse peak or lower a gate after inspecting results. Inspect changes
in net R, trade retention, both funding cases, weak eras and best-five fragility.
Full completed ledgers, feature values and generation counts remain downloadable.
Attribution matches closed trades by entry time, reference, stop and target;
shared outcomes can differ with subsequent sizing. Account populations are
conditional and distinct from all independent signal opportunities.

Known source gaps, stale valuations, barrier assumptions and omitted unknown
entries remain explicit. END_ASC is a conditional scenario, sampled NAV DD
is not an intrabar bound, historical broker funding is not reconstructed, and
all historical periods have already been examined. This does not validate
combined live32 admission and cannot activate trading.

Read START_HERE.txt for deployment and the planned result-dependent sequence.
