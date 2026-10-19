"""Predeclared entry allowances; no policy is selected from its results."""
from decimal import ROUND_FLOOR
import execution as e

POLICIES = ('FIXED_10', 'FIXED_15', 'FIXED_20', 'FIXED_30', 'STOP10_MAX30')
PRIMARY_SCENARIO = 'END_ASC'
PRIMARY_FUNDING = 'FROZEN_14OCT2026_RATES'


def allowance(policy, signal):
    e.require(policy in POLICIES, 'Unknown entry policy')
    if policy.startswith('FIXED_'):
        return int(policy.split('_')[1])
    distance = abs(e.broker_price(signal['pair'], signal['reference_entry']) -
                   e.broker_price(signal['pair'], signal['stop'])) / e.tick(signal['pair'])
    return max(10, min(30, int((distance * e.dec('.10')).to_integral_value(rounding=ROUND_FLOOR))))


PROTOCOL = dict(
    version='P32_ENTRY_LIMIT_RESEARCH_V1_2026_10_19',
    policies=list(POLICIES), relative_rule='max(10,min(30,floor(0.10*original_stop_distance_ticks)))',
    primary_scenario=PRIMARY_SCENARIO, primary_financing=PRIMARY_FUNDING,
    lanes=80, selection='No automatic winner or live update',
    cohort='All 3629 warmed raw qualifications; fixed 32 strategy rules and existing historical cutoff',
    pricing='Same saved entry bid/ask across policies; fixed rounded stop/target; newly eligible exits replayed on bid/ask',
    sizing='Whole units against each policy worst permitted entry; 1% NAV, EUR_JPY_M15_SHORT 0.75%',
    admission='Rebuild strategy, opposite-pair, margin and NAV dependencies for every lane',
    control='All 16 FIXED_10 lanes must match pinned summaries and ordered account records before alternatives',
    financing='Zero, frozen 14-Oct-2026 schedule, adverse annual 3% and 6%; not historical rates',
    uncertainty='Keep unknown entries, source differences and unresolved path lineage explicit',
    recent_era='2021 onward is a diagnostic on previously examined data, not untouched out-of-sample evidence',
    evaluation='Compare CAGR, sampled liquidation drawdown, full-year and rolling 12-month returns, trade frequency, cash contribution and admission changes',
    live_execution='Native candle-open proxy; latency and broker price-bound cancellation frequency are not recovered')
