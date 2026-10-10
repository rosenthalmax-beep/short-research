"""EURCHF M15 LONG Pass4: standalone failed-breakdown/reclaim research.
Single Python3.10+ standard-library file. Keep EURCHF_M15_PASS1_FROZEN_DATA.zip
alongside app.py. /status, /results; --run or --self-test. No broker/order calls.
"""
from __future__ import annotations
import argparse,bisect,csv,hashlib,io,json,math,os,shutil,sys,tempfile,threading,time,traceback,zipfile
from collections import defaultdict,deque
from datetime import datetime,timedelta,timezone
from pathlib import Path
from socketserver import ThreadingMixIn
from wsgiref.simple_server import WSGIServer,make_server
VERSION='EURCHF_M15_LONG_PASS4_FAILED_BREAKDOWN_RECLAIM_V1_2026_10_10'
PAIR,SIDE,TIMEFRAME='EUR_CHF','BUY','M15'
UTC=timezone.utc
START=datetime(2005,1,1,tzinfo=UTC)
END=datetime(2026,10,8,tzinfo=UTC)
BAR=timedelta(minutes=15)
H1_BAR=timedelta(hours=1)
PINNED_H1_END=datetime(2026,10,1,tzinfo=UTC)
PINNED_H1_COUNT=137819
PINNED_H1_SHA256='9c8b5279629daee868ad924f52e998be7f6ce638a15e106ffb4851a2df76feac'
TICK,PIP,RR,WARMUP=.00001,.0001,3.,200
COSTS=(10,20,40)
MODELS=('NEAREST_OPEN_SENSITIVITY','STOP_FIRST_GAP_STRESS')
RESULT_NAME='EURCHF_M15_LONG_PASS4_FAILED_BREAKDOWN_RECLAIM_RESULTS.zip'
OUT=Path(os.getenv('EURCHF_M15_LONG_PASS2B_OUTPUT_DIR','/tmp/eurchf_m15_long_pass4')).resolve()
PREFIX='/eurchf-m15-long-pass4'
LOCK=threading.RLock()
STARTED=False
JOB_LOCK=None
RUN_CLOCK=None
STATUS=dict(state='not_started',progress=0,message='Ready',version=VERSION,orders_supported=False,trading_enabled=False,pair=PAIR)
CASE=['config_id', 'execution_model', 'cost_ticks']
SUMMARY_FIELDS=['config_id', 'execution_model', 'cost_ticks', 'raw_signals', 'eligible_isolated_signals', 'isolated_completed', 'isolated_open', 'isolated_total_r', 'isolated_profit_factor', 'geometry_invalid_all_signals', 'p0_blocked_signals', 'invalid_unblocked_entries', 'open_at_data_end', 'closed_trades', 'wins', 'losses', 'win_rate_pct', 'total_r', 'expectancy_r', 'profit_factor', 'max_closed_dd_r', 'max_losing_streak', 'dual_touch_closed', 'gap_stop_closed', 'gap_target_closed', 'entry_next_open_gap_count', 'entries_before_market_closure', 'median_stop_pips', 'median_cost_fraction_reference_risk', 'p90_cost_fraction_reference_risk', 'median_holding_hours', 'max_holding_hours', 'maximum_inter_entry_gap_days', 'leading_no_entry_days', 'trailing_no_entry_days', 'zero_entry_months', 'max_consecutive_zero_entry_months', 'positive_complete_entry_years', 'negative_complete_entry_years', 'zero_entry_complete_years', 'raw_signal_sha256', 'accepted_ledger_sha256', 'worst_12m_realized_r', 'worst_12m_start', 'worst_12m_end', 'zero_entry_12m_windows', 'worst_24m_realized_r', 'worst_24m_start', 'worst_24m_end', 'zero_entry_24m_windows', 'worst_36m_realized_r', 'worst_36m_start', 'worst_36m_end', 'zero_entry_36m_windows']
EMBEDDED_MEMBER_PINS={'source_candles.csv': {'bytes': 31136287, 'sha256': '59f3a83fb838d9921f9f2433cd928a77ef451301bec466a3fedd67744ae2f707'}, 'source_h1_crosscheck_candles.csv': {'bytes': 7942356, 'sha256': 'ffaa24aa4c371b7885c61ef225f1846ccf5e133d444a45c438af3066cef702a3'}, 'fetch_receipts.csv': {'bytes': 19964, 'sha256': '52423125b321d3c25b5ae2adf89c8fd287b230764540847b375279cc92530890'}, 'parent_pass1_accepted_ledgers.csv': {'bytes': 23899883, 'sha256': '05bef6ac72de3d5d7c4a8e6ca0fc2dc52c13c684f956e9f3c88cf8f4aea4a706'}, 'parent_pass1_provenance.json': {'bytes': 71790, 'sha256': 'f11996dfcd59473762a35b3cd02ab627a3b9757d71f2021b593999f5ce60ea1c'}}
EMBEDDED_PAYLOAD_SHA256='511215ae3b95e676829896d30baa1342bbca17e7be30745a22753dab5ba3b849'
FROZEN_SOURCE_FILE=Path(__file__).resolve().with_name('EURCHF_M15_PASS1_FROZEN_DATA.zip')

LOOKBACKS=(60, 100, 120, 200)
ANCHORS=[('M_L200_D0050_B125_R150', 200, 0.05), ('M_L100_D0025_B125_R150', 100, 0.025)]
PARENT_IDS=()
COMPARISON_IDS=PARENT_IDS



ARCHIVED_REFERENCE_JSON='[{"accepted_ledger_sha256":"0a77c7212e25cfa503511b13d615306dde720630e8a03308fe4ab6dfc0f6b8cc","closed_trades":"18091","config_id":"RAW_EXACT_BULLISH_ENGULF","cost_ticks":"10","dual_touch_closed":"57","eligible_isolated_signals":"37095","entries_before_market_closure":"49","entry_next_open_gap_count":"14038","execution_model":"NEAREST_OPEN_SENSITIVITY","expectancy_r":"-0.2592533845678326","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"37094","isolated_open":"1","isolated_profit_factor":"0.6255937976243057","isolated_total_r":"-10898.59014495401","leading_no_entry_days":"4.0","losses":"14083","max_closed_dd_r":"-4690.15298021666","max_consecutive_zero_entry_months":"1","max_holding_hours":"1356.75","max_losing_streak":"31","maximum_inter_entry_gap_days":"56.760416666666664","median_cost_fraction_reference_risk":"0.17241379310347388","median_holding_hours":"1.5","median_stop_pips":"5.799999999999139","negative_complete_entry_years":"21","open_at_data_end":"1","p0_blocked_signals":"19003","p90_cost_fraction_reference_risk":"0.37037037037038073","positive_complete_entry_years":"0","profit_factor":"0.666963503499493","raw_signal_sha256":"0768cf73a8418dbc88829f53b831ee72acb548709a395ae20a1e813cad4fade8","raw_signals":"37095","total_r":"-4690.15298021666","trailing_no_entry_days":"0.03125","win_rate_pct":"22.154662539384223","wins":"4008","worst_12m_end":"2006-01-01T00:00:00Z","worst_12m_realized_r":"-382.154615498013","worst_12m_start":"2005-01-01T00:00:00Z","worst_24m_end":"2007-01-01T00:00:00Z","worst_24m_realized_r":"-669.5571548171639","worst_24m_start":"2005-01-01T00:00:00Z","worst_36m_end":"2008-01-01T00:00:00Z","worst_36m_realized_r":"-846.3857851441394","worst_36m_start":"2005-01-01T00:00:00Z","zero_entry_12m_windows":"0","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"0","zero_entry_months":"1"},{"accepted_ledger_sha256":"19599445f83438f8fab82eb6dd574e796431cc4117dded74ac5341202a518584","closed_trades":"18091","config_id":"RAW_EXACT_BULLISH_ENGULF","cost_ticks":"20","dual_touch_closed":"57","eligible_isolated_signals":"37095","entries_before_market_closure":"49","entry_next_open_gap_count":"14038","execution_model":"NEAREST_OPEN_SENSITIVITY","expectancy_r":"-0.3565844798494743","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"37094","isolated_open":"1","isolated_profit_factor":"0.4977261876261584","isolated_total_r":"-14620.688404390226","leading_no_entry_days":"4.0","losses":"14083","max_closed_dd_r":"-6450.969824956839","max_consecutive_zero_entry_months":"1","max_holding_hours":"1356.75","max_losing_streak":"31","maximum_inter_entry_gap_days":"56.760416666666664","median_cost_fraction_reference_risk":"0.34482758620694776","median_holding_hours":"1.5","median_stop_pips":"5.799999999999139","negative_complete_entry_years":"21","open_at_data_end":"1","p0_blocked_signals":"19003","p90_cost_fraction_reference_risk":"0.7407407407407615","positive_complete_entry_years":"0","profit_factor":"0.5419321291658862","raw_signal_sha256":"0768cf73a8418dbc88829f53b831ee72acb548709a395ae20a1e813cad4fade8","raw_signals":"37095","total_r":"-6450.969824956839","trailing_no_entry_days":"0.03125","win_rate_pct":"22.154662539384223","wins":"4008","worst_12m_end":"2006-01-01T00:00:00Z","worst_12m_realized_r":"-483.5905633410159","worst_12m_start":"2005-01-01T00:00:00Z","worst_24m_end":"2007-01-01T00:00:00Z","worst_24m_realized_r":"-873.0267633982512","worst_24m_start":"2005-01-01T00:00:00Z","worst_36m_end":"2008-01-01T00:00:00Z","worst_36m_realized_r":"-1142.3447267062445","worst_36m_start":"2005-01-01T00:00:00Z","zero_entry_12m_windows":"0","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"0","zero_entry_months":"1"},{"accepted_ledger_sha256":"398a15de61f4c199dfda2cc0057b4ee37ddebc02a1e2dc8cc59ec3d311bbbd75","closed_trades":"18077","config_id":"RAW_EXACT_BULLISH_ENGULF","cost_ticks":"40","dual_touch_closed":"57","eligible_isolated_signals":"37018","entries_before_market_closure":"49","entry_next_open_gap_count":"14028","execution_model":"NEAREST_OPEN_SENSITIVITY","expectancy_r":"-0.48325527649419353","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"77","invalid_unblocked_entries":"16","isolated_completed":"37017","isolated_open":"1","isolated_profit_factor":"0.33617248071270645","isolated_total_r":"-19282.197952738068","leading_no_entry_days":"4.0","losses":"14071","max_closed_dd_r":"-8735.805633185537","max_consecutive_zero_entry_months":"1","max_holding_hours":"1356.75","max_losing_streak":"31","maximum_inter_entry_gap_days":"56.760416666666664","median_cost_fraction_reference_risk":"0.6896551724138955","median_holding_hours":"1.5","median_stop_pips":"5.799999999999139","negative_complete_entry_years":"21","open_at_data_end":"1","p0_blocked_signals":"19001","p90_cost_fraction_reference_risk":"1.481481481481523","positive_complete_entry_years":"0","profit_factor":"0.3791624168015396","raw_signal_sha256":"0768cf73a8418dbc88829f53b831ee72acb548709a395ae20a1e813cad4fade8","raw_signals":"37095","total_r":"-8735.805633185537","trailing_no_entry_days":"0.03125","win_rate_pct":"22.16075676273718","wins":"4006","worst_12m_end":"2006-01-01T00:00:00Z","worst_12m_realized_r":"-616.1620972491415","worst_12m_start":"2005-01-01T00:00:00Z","worst_24m_end":"2007-01-01T00:00:00Z","worst_24m_realized_r":"-1135.9121702448801","worst_24m_start":"2005-01-01T00:00:00Z","worst_36m_end":"2008-01-01T00:00:00Z","worst_36m_realized_r":"-1531.196964026576","worst_36m_start":"2005-01-01T00:00:00Z","zero_entry_12m_windows":"0","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"0","zero_entry_months":"1"},{"accepted_ledger_sha256":"f788f50e5fcbf6062abaa38ae7208218d39eb0911e861cf51884ffc41945f161","closed_trades":"18091","config_id":"RAW_EXACT_BULLISH_ENGULF","cost_ticks":"10","dual_touch_closed":"57","eligible_isolated_signals":"37095","entries_before_market_closure":"49","entry_next_open_gap_count":"14038","execution_model":"STOP_FIRST_GAP_STRESS","expectancy_r":"-0.26923191106548433","gap_stop_closed":"159","gap_target_closed":"58","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"37094","isolated_open":"1","isolated_profit_factor":"0.6152507890136661","isolated_total_r":"-11342.85297729968","leading_no_entry_days":"4.0","losses":"14098","max_closed_dd_r":"-4870.674503085677","max_consecutive_zero_entry_months":"1","max_holding_hours":"1356.75","max_losing_streak":"31","maximum_inter_entry_gap_days":"56.760416666666664","median_cost_fraction_reference_risk":"0.17241379310347388","median_holding_hours":"1.5","median_stop_pips":"5.799999999999139","negative_complete_entry_years":"21","open_at_data_end":"1","p0_blocked_signals":"19003","p90_cost_fraction_reference_risk":"0.37037037037038073","positive_complete_entry_years":"0","profit_factor":"0.6577355691294657","raw_signal_sha256":"0768cf73a8418dbc88829f53b831ee72acb548709a395ae20a1e813cad4fade8","raw_signals":"37095","total_r":"-4870.674503085677","trailing_no_entry_days":"0.03125","win_rate_pct":"22.071748383173954","wins":"3993","worst_12m_end":"2006-01-01T00:00:00Z","worst_12m_realized_r":"-384.7314128836343","worst_12m_start":"2005-01-01T00:00:00Z","worst_24m_end":"2007-01-01T00:00:00Z","worst_24m_realized_r":"-676.5055250743579","worst_24m_start":"2005-01-01T00:00:00Z","worst_36m_end":"2008-01-01T00:00:00Z","worst_36m_realized_r":"-854.629530991146","worst_36m_start":"2005-01-01T00:00:00Z","zero_entry_12m_windows":"0","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"0","zero_entry_months":"1"},{"accepted_ledger_sha256":"ffbac0e7e8602d319eb34caa3bda2e16dc01001826cb2cf7a15eb5e6eda8cbe3","closed_trades":"18091","config_id":"RAW_EXACT_BULLISH_ENGULF","cost_ticks":"20","dual_touch_closed":"57","eligible_isolated_signals":"37095","entries_before_market_closure":"49","entry_next_open_gap_count":"14038","execution_model":"STOP_FIRST_GAP_STRESS","expectancy_r":"-0.3650806622005727","gap_stop_closed":"159","gap_target_closed":"58","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"37094","isolated_open":"1","isolated_profit_factor":"0.4905279200526459","isolated_total_r":"-14992.04160453867","leading_no_entry_days":"4.0","losses":"14098","max_closed_dd_r":"-6604.674259870561","max_consecutive_zero_entry_months":"1","max_holding_hours":"1356.75","max_losing_streak":"31","maximum_inter_entry_gap_days":"56.760416666666664","median_cost_fraction_reference_risk":"0.34482758620694776","median_holding_hours":"1.5","median_stop_pips":"5.799999999999139","negative_complete_entry_years":"21","open_at_data_end":"1","p0_blocked_signals":"19003","p90_cost_fraction_reference_risk":"0.7407407407407615","positive_complete_entry_years":"0","profit_factor":"0.535266119654437","raw_signal_sha256":"0768cf73a8418dbc88829f53b831ee72acb548709a395ae20a1e813cad4fade8","raw_signals":"37095","total_r":"-6604.674259870561","trailing_no_entry_days":"0.03125","win_rate_pct":"22.071748383173954","wins":"3993","worst_12m_end":"2006-01-01T00:00:00Z","worst_12m_realized_r":"-485.9826268330798","worst_12m_start":"2005-01-01T00:00:00Z","worst_24m_end":"2007-01-01T00:00:00Z","worst_24m_realized_r":"-878.9950173665053","worst_24m_start":"2005-01-01T00:00:00Z","worst_36m_end":"2008-01-01T00:00:00Z","worst_36m_realized_r":"-1149.4489077104258","worst_36m_start":"2005-01-01T00:00:00Z","zero_entry_12m_windows":"0","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"0","zero_entry_months":"1"},{"accepted_ledger_sha256":"ba72a75089fe75eb0a6aac8115f871dd0dc1bab64d8be02ec3cb324b1be01254","closed_trades":"18077","config_id":"RAW_EXACT_BULLISH_ENGULF","cost_ticks":"40","dual_touch_closed":"57","eligible_isolated_signals":"37018","entries_before_market_closure":"49","entry_next_open_gap_count":"14028","execution_model":"STOP_FIRST_GAP_STRESS","expectancy_r":"-0.4898818730589024","gap_stop_closed":"159","gap_target_closed":"58","geometry_invalid_all_signals":"77","invalid_unblocked_entries":"16","isolated_completed":"37017","isolated_open":"1","isolated_profit_factor":"0.3322081141189905","isolated_total_r":"-19565.057405132564","leading_no_entry_days":"4.0","losses":"14086","max_closed_dd_r":"-8855.59461928578","max_consecutive_zero_entry_months":"1","max_holding_hours":"1356.75","max_losing_streak":"31","maximum_inter_entry_gap_days":"56.760416666666664","median_cost_fraction_reference_risk":"0.6896551724138955","median_holding_hours":"1.5","median_stop_pips":"5.799999999999139","negative_complete_entry_years":"21","open_at_data_end":"1","p0_blocked_signals":"19001","p90_cost_fraction_reference_risk":"1.481481481481523","positive_complete_entry_years":"0","profit_factor":"0.37528655094891356","raw_signal_sha256":"0768cf73a8418dbc88829f53b831ee72acb548709a395ae20a1e813cad4fade8","raw_signals":"37095","total_r":"-8855.59461928578","trailing_no_entry_days":"0.03125","win_rate_pct":"22.07777839243237","wins":"3991","worst_12m_end":"2006-01-01T00:00:00Z","worst_12m_realized_r":"-618.2620972491418","worst_12m_start":"2005-01-01T00:00:00Z","worst_24m_end":"2007-01-01T00:00:00Z","worst_24m_realized_r":"-1140.6506623083724","worst_24m_start":"2005-01-01T00:00:00Z","worst_36m_end":"2008-01-01T00:00:00Z","worst_36m_realized_r":"-1536.8534745862996","worst_36m_start":"2005-01-01T00:00:00Z","zero_entry_12m_windows":"0","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"0","zero_entry_months":"1"},{"accepted_ledger_sha256":"a848920d4d3f9572e146fd8e057eb0b9e332a0e687168ea235629a10c1fa7035","closed_trades":"57","config_id":"M_L200_D0050_B125_R150","cost_ticks":"10","dual_touch_closed":"0","eligible_isolated_signals":"57","entries_before_market_closure":"0","entry_next_open_gap_count":"45","execution_model":"NEAREST_OPEN_SENSITIVITY","expectancy_r":"0.08914356408068075","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"57","isolated_open":"0","isolated_profit_factor":"1.12702957881497","isolated_total_r":"5.081183152598802","leading_no_entry_days":"26.979166666666668","losses":"40","max_closed_dd_r":"-15.002304147465386","max_consecutive_zero_entry_months":"16","max_holding_hours":"1356.75","max_losing_streak":"11","maximum_inter_entry_gap_days":"496.0416666666667","median_cost_fraction_reference_risk":"0.08620689655172044","median_holding_hours":"5.25","median_stop_pips":"11.600000000000499","negative_complete_entry_years":"12","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.13405405405405374","positive_complete_entry_years":"9","profit_factor":"1.12702957881497","raw_signal_sha256":"6694fe6caffbe3e290268273e8a60d1380afe68ebc6d632bb36a373ec49a0b92","raw_signals":"57","total_r":"5.081183152598802","trailing_no_entry_days":"212.63541666666666","win_rate_pct":"29.82456140350877","wins":"17","worst_12m_end":"2010-05-01T00:00:00Z","worst_12m_realized_r":"-4.0","worst_12m_start":"2009-05-01T00:00:00Z","worst_24m_end":"2011-06-01T00:00:00Z","worst_24m_realized_r":"-7.0","worst_24m_start":"2009-06-01T00:00:00Z","worst_36m_end":"2012-05-01T00:00:00Z","worst_36m_realized_r":"-6.444444444444445","worst_36m_start":"2009-05-01T00:00:00Z","zero_entry_12m_windows":"8","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"0","zero_entry_months":"212"},{"accepted_ledger_sha256":"4d6f22b463fc1de7fdede88379e198d9a1ef304ffde6ae91109e14b570572a49","closed_trades":"57","config_id":"M_L200_D0050_B125_R150","cost_ticks":"20","dual_touch_closed":"0","eligible_isolated_signals":"57","entries_before_market_closure":"0","entry_next_open_gap_count":"45","execution_model":"NEAREST_OPEN_SENSITIVITY","expectancy_r":"0.004719493655705163","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"57","isolated_open":"0","isolated_profit_factor":"1.0067252784593799","isolated_total_r":"0.2690111383751943","leading_no_entry_days":"26.979166666666668","losses":"40","max_closed_dd_r":"-16.542458572126613","max_consecutive_zero_entry_months":"16","max_holding_hours":"1356.75","max_losing_streak":"11","maximum_inter_entry_gap_days":"496.0416666666667","median_cost_fraction_reference_risk":"0.17241379310344088","median_holding_hours":"5.25","median_stop_pips":"11.600000000000499","negative_complete_entry_years":"12","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.2681081081081075","positive_complete_entry_years":"9","profit_factor":"1.0067252784593799","raw_signal_sha256":"6694fe6caffbe3e290268273e8a60d1380afe68ebc6d632bb36a373ec49a0b92","raw_signals":"57","total_r":"0.2690111383751943","trailing_no_entry_days":"212.63541666666666","win_rate_pct":"29.82456140350877","wins":"17","worst_12m_end":"2010-05-01T00:00:00Z","worst_12m_realized_r":"-4.0","worst_12m_start":"2009-05-01T00:00:00Z","worst_24m_end":"2011-06-01T00:00:00Z","worst_24m_realized_r":"-7.0","worst_24m_start":"2009-06-01T00:00:00Z","worst_36m_end":"2012-05-01T00:00:00Z","worst_36m_realized_r":"-6.8","worst_36m_start":"2009-05-01T00:00:00Z","zero_entry_12m_windows":"8","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"0","zero_entry_months":"212"},{"accepted_ledger_sha256":"6eeb94029eb81df2feb541def0476b309b0bd8335b5ca9251d36b480ba3cf256","closed_trades":"57","config_id":"M_L200_D0050_B125_R150","cost_ticks":"40","dual_touch_closed":"0","eligible_isolated_signals":"57","entries_before_market_closure":"0","entry_next_open_gap_count":"45","execution_model":"NEAREST_OPEN_SENSITIVITY","expectancy_r":"-0.1258107954829195","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"57","isolated_open":"0","isolated_profit_factor":"0.8207196164368398","isolated_total_r":"-7.171215342526413","leading_no_entry_days":"26.979166666666668","losses":"40","max_closed_dd_r":"-18.76785178247647","max_consecutive_zero_entry_months":"16","max_holding_hours":"1356.75","max_losing_streak":"11","maximum_inter_entry_gap_days":"496.0416666666667","median_cost_fraction_reference_risk":"0.34482758620688175","median_holding_hours":"5.25","median_stop_pips":"11.600000000000499","negative_complete_entry_years":"13","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.536216216216215","positive_complete_entry_years":"8","profit_factor":"0.8207196164368398","raw_signal_sha256":"6694fe6caffbe3e290268273e8a60d1380afe68ebc6d632bb36a373ec49a0b92","raw_signals":"57","total_r":"-7.171215342526413","trailing_no_entry_days":"212.63541666666666","win_rate_pct":"29.82456140350877","wins":"17","worst_12m_end":"2010-05-01T00:00:00Z","worst_12m_realized_r":"-4.0","worst_12m_start":"2009-05-01T00:00:00Z","worst_24m_end":"2011-06-01T00:00:00Z","worst_24m_realized_r":"-7.0","worst_24m_start":"2009-06-01T00:00:00Z","worst_36m_end":"2012-05-01T00:00:00Z","worst_36m_realized_r":"-7.333333333333332","worst_36m_start":"2009-05-01T00:00:00Z","zero_entry_12m_windows":"8","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"0","zero_entry_months":"212"},{"accepted_ledger_sha256":"4b12c9780b8c78a1f5dda220021cca8d9292e375e2a2b7790ad8af600175ae88","closed_trades":"57","config_id":"M_L200_D0050_B125_R150","cost_ticks":"10","dual_touch_closed":"0","eligible_isolated_signals":"57","entries_before_market_closure":"0","entry_next_open_gap_count":"45","execution_model":"STOP_FIRST_GAP_STRESS","expectancy_r":"0.08914356408068075","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"57","isolated_open":"0","isolated_profit_factor":"1.12702957881497","isolated_total_r":"5.081183152598802","leading_no_entry_days":"26.979166666666668","losses":"40","max_closed_dd_r":"-15.002304147465386","max_consecutive_zero_entry_months":"16","max_holding_hours":"1356.75","max_losing_streak":"11","maximum_inter_entry_gap_days":"496.0416666666667","median_cost_fraction_reference_risk":"0.08620689655172044","median_holding_hours":"5.25","median_stop_pips":"11.600000000000499","negative_complete_entry_years":"12","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.13405405405405374","positive_complete_entry_years":"9","profit_factor":"1.12702957881497","raw_signal_sha256":"6694fe6caffbe3e290268273e8a60d1380afe68ebc6d632bb36a373ec49a0b92","raw_signals":"57","total_r":"5.081183152598802","trailing_no_entry_days":"212.63541666666666","win_rate_pct":"29.82456140350877","wins":"17","worst_12m_end":"2010-05-01T00:00:00Z","worst_12m_realized_r":"-4.0","worst_12m_start":"2009-05-01T00:00:00Z","worst_24m_end":"2011-06-01T00:00:00Z","worst_24m_realized_r":"-7.0","worst_24m_start":"2009-06-01T00:00:00Z","worst_36m_end":"2012-05-01T00:00:00Z","worst_36m_realized_r":"-6.444444444444445","worst_36m_start":"2009-05-01T00:00:00Z","zero_entry_12m_windows":"8","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"0","zero_entry_months":"212"},{"accepted_ledger_sha256":"703eaf8be500e238b5573582f52924bd34dec439436587626caa2519f7b55803","closed_trades":"57","config_id":"M_L200_D0050_B125_R150","cost_ticks":"20","dual_touch_closed":"0","eligible_isolated_signals":"57","entries_before_market_closure":"0","entry_next_open_gap_count":"45","execution_model":"STOP_FIRST_GAP_STRESS","expectancy_r":"0.004719493655705163","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"57","isolated_open":"0","isolated_profit_factor":"1.0067252784593799","isolated_total_r":"0.2690111383751943","leading_no_entry_days":"26.979166666666668","losses":"40","max_closed_dd_r":"-16.542458572126613","max_consecutive_zero_entry_months":"16","max_holding_hours":"1356.75","max_losing_streak":"11","maximum_inter_entry_gap_days":"496.0416666666667","median_cost_fraction_reference_risk":"0.17241379310344088","median_holding_hours":"5.25","median_stop_pips":"11.600000000000499","negative_complete_entry_years":"12","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.2681081081081075","positive_complete_entry_years":"9","profit_factor":"1.0067252784593799","raw_signal_sha256":"6694fe6caffbe3e290268273e8a60d1380afe68ebc6d632bb36a373ec49a0b92","raw_signals":"57","total_r":"0.2690111383751943","trailing_no_entry_days":"212.63541666666666","win_rate_pct":"29.82456140350877","wins":"17","worst_12m_end":"2010-05-01T00:00:00Z","worst_12m_realized_r":"-4.0","worst_12m_start":"2009-05-01T00:00:00Z","worst_24m_end":"2011-06-01T00:00:00Z","worst_24m_realized_r":"-7.0","worst_24m_start":"2009-06-01T00:00:00Z","worst_36m_end":"2012-05-01T00:00:00Z","worst_36m_realized_r":"-6.8","worst_36m_start":"2009-05-01T00:00:00Z","zero_entry_12m_windows":"8","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"0","zero_entry_months":"212"},{"accepted_ledger_sha256":"7d2b91509b054097f0b0c348b788471895b44f3ffad5a1e4a9eb6e49be194aa9","closed_trades":"57","config_id":"M_L200_D0050_B125_R150","cost_ticks":"40","dual_touch_closed":"0","eligible_isolated_signals":"57","entries_before_market_closure":"0","entry_next_open_gap_count":"45","execution_model":"STOP_FIRST_GAP_STRESS","expectancy_r":"-0.1258107954829195","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"57","isolated_open":"0","isolated_profit_factor":"0.8207196164368398","isolated_total_r":"-7.171215342526413","leading_no_entry_days":"26.979166666666668","losses":"40","max_closed_dd_r":"-18.76785178247647","max_consecutive_zero_entry_months":"16","max_holding_hours":"1356.75","max_losing_streak":"11","maximum_inter_entry_gap_days":"496.0416666666667","median_cost_fraction_reference_risk":"0.34482758620688175","median_holding_hours":"5.25","median_stop_pips":"11.600000000000499","negative_complete_entry_years":"13","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.536216216216215","positive_complete_entry_years":"8","profit_factor":"0.8207196164368398","raw_signal_sha256":"6694fe6caffbe3e290268273e8a60d1380afe68ebc6d632bb36a373ec49a0b92","raw_signals":"57","total_r":"-7.171215342526413","trailing_no_entry_days":"212.63541666666666","win_rate_pct":"29.82456140350877","wins":"17","worst_12m_end":"2010-05-01T00:00:00Z","worst_12m_realized_r":"-4.0","worst_12m_start":"2009-05-01T00:00:00Z","worst_24m_end":"2011-06-01T00:00:00Z","worst_24m_realized_r":"-7.0","worst_24m_start":"2009-06-01T00:00:00Z","worst_36m_end":"2012-05-01T00:00:00Z","worst_36m_realized_r":"-7.333333333333332","worst_36m_start":"2009-05-01T00:00:00Z","zero_entry_12m_windows":"8","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"0","zero_entry_months":"212"},{"accepted_ledger_sha256":"47e659a0f64195343fea67df8ba90bd454db9c8cb5fd5db79c44853c9d481645","closed_trades":"50","config_id":"M_L100_D0025_B125_R150","cost_ticks":"10","dual_touch_closed":"0","eligible_isolated_signals":"50","entries_before_market_closure":"0","entry_next_open_gap_count":"32","execution_model":"NEAREST_OPEN_SENSITIVITY","expectancy_r":"0.1034609877041315","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"50","isolated_open":"0","isolated_profit_factor":"1.147801411005902","isolated_total_r":"5.173049385206575","leading_no_entry_days":"23.510416666666668","losses":"35","max_closed_dd_r":"-8.366006102076463","max_consecutive_zero_entry_months":"20","max_holding_hours":"127.25","max_losing_streak":"7","maximum_inter_entry_gap_days":"626.9791666666666","median_cost_fraction_reference_risk":"0.0885579937304015","median_holding_hours":"5.25","median_stop_pips":"11.300000000000754","negative_complete_entry_years":"12","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.12225609756098264","positive_complete_entry_years":"7","profit_factor":"1.147801411005902","raw_signal_sha256":"c0473c622ba0fe1d0913907678c17296617e45cd566aa374c1b6e64bfd032817","raw_signals":"50","total_r":"5.173049385206575","trailing_no_entry_days":"365.1041666666667","win_rate_pct":"30.0","wins":"15","worst_12m_end":"2008-01-01T00:00:00Z","worst_12m_realized_r":"-3.0","worst_12m_start":"2007-01-01T00:00:00Z","worst_24m_end":"2012-11-01T00:00:00Z","worst_24m_realized_r":"-4.0","worst_24m_start":"2010-11-01T00:00:00Z","worst_36m_end":"2013-11-01T00:00:00Z","worst_36m_realized_r":"-6.0","worst_36m_start":"2010-11-01T00:00:00Z","zero_entry_12m_windows":"25","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"2","zero_entry_months":"221"},{"accepted_ledger_sha256":"5a9ce8b8f3f0f622732ce183f48fd5524e406ba31eee0658529d19ffe2212cbd","closed_trades":"50","config_id":"M_L100_D0025_B125_R150","cost_ticks":"20","dual_touch_closed":"0","eligible_isolated_signals":"50","entries_before_market_closure":"0","entry_next_open_gap_count":"32","execution_model":"NEAREST_OPEN_SENSITIVITY","expectancy_r":"0.023777267113728868","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"50","isolated_open":"0","isolated_profit_factor":"1.033967524448184","isolated_total_r":"1.1888633556864434","leading_no_entry_days":"23.510416666666668","losses":"35","max_closed_dd_r":"-9.40418118466895","max_consecutive_zero_entry_months":"20","max_holding_hours":"127.25","max_losing_streak":"7","maximum_inter_entry_gap_days":"626.9791666666666","median_cost_fraction_reference_risk":"0.177115987460803","median_holding_hours":"5.25","median_stop_pips":"11.300000000000754","negative_complete_entry_years":"12","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.2445121951219653","positive_complete_entry_years":"7","profit_factor":"1.033967524448184","raw_signal_sha256":"c0473c622ba0fe1d0913907678c17296617e45cd566aa374c1b6e64bfd032817","raw_signals":"50","total_r":"1.1888633556864434","trailing_no_entry_days":"365.1041666666667","win_rate_pct":"30.0","wins":"15","worst_12m_end":"2008-01-01T00:00:00Z","worst_12m_realized_r":"-3.0","worst_12m_start":"2007-01-01T00:00:00Z","worst_24m_end":"2012-11-01T00:00:00Z","worst_24m_realized_r":"-4.0","worst_24m_start":"2010-11-01T00:00:00Z","worst_36m_end":"2013-11-01T00:00:00Z","worst_36m_realized_r":"-6.0","worst_36m_start":"2010-11-01T00:00:00Z","zero_entry_12m_windows":"25","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"2","zero_entry_months":"221"},{"accepted_ledger_sha256":"4ae1c5c55b9f3f0cfe7ea9ec97851da3ebbb4e5599f227bb19cb4297ee144b87","closed_trades":"50","config_id":"M_L100_D0025_B125_R150","cost_ticks":"40","dual_touch_closed":"0","eligible_isolated_signals":"50","entries_before_market_closure":"0","entry_next_open_gap_count":"32","execution_model":"NEAREST_OPEN_SENSITIVITY","expectancy_r":"-0.10142351005965797","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"50","isolated_open":"0","isolated_profit_factor":"0.8551092713433458","isolated_total_r":"-5.071175502982898","leading_no_entry_days":"23.510416666666668","losses":"35","max_closed_dd_r":"-10.895993179880582","max_consecutive_zero_entry_months":"20","max_holding_hours":"127.25","max_losing_streak":"7","maximum_inter_entry_gap_days":"626.9791666666666","median_cost_fraction_reference_risk":"0.354231974921606","median_holding_hours":"5.25","median_stop_pips":"11.300000000000754","negative_complete_entry_years":"12","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.4890243902439306","positive_complete_entry_years":"7","profit_factor":"0.8551092713433458","raw_signal_sha256":"c0473c622ba0fe1d0913907678c17296617e45cd566aa374c1b6e64bfd032817","raw_signals":"50","total_r":"-5.071175502982898","trailing_no_entry_days":"365.1041666666667","win_rate_pct":"30.0","wins":"15","worst_12m_end":"2008-01-01T00:00:00Z","worst_12m_realized_r":"-3.0","worst_12m_start":"2007-01-01T00:00:00Z","worst_24m_end":"2012-11-01T00:00:00Z","worst_24m_realized_r":"-4.0","worst_24m_start":"2010-11-01T00:00:00Z","worst_36m_end":"2013-11-01T00:00:00Z","worst_36m_realized_r":"-6.0","worst_36m_start":"2010-11-01T00:00:00Z","zero_entry_12m_windows":"25","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"2","zero_entry_months":"221"},{"accepted_ledger_sha256":"0e66e3526fe2b55cead85ef3f93aed983eb3754098e7e22c79c541c817dba580","closed_trades":"50","config_id":"M_L100_D0025_B125_R150","cost_ticks":"10","dual_touch_closed":"0","eligible_isolated_signals":"50","entries_before_market_closure":"0","entry_next_open_gap_count":"32","execution_model":"STOP_FIRST_GAP_STRESS","expectancy_r":"0.09946098770413446","gap_stop_closed":"1","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"50","isolated_open":"0","isolated_profit_factor":"1.141279812079737","isolated_total_r":"4.973049385206723","leading_no_entry_days":"23.510416666666668","losses":"35","max_closed_dd_r":"-8.566006102076315","max_consecutive_zero_entry_months":"20","max_holding_hours":"127.0","max_losing_streak":"7","maximum_inter_entry_gap_days":"626.9791666666666","median_cost_fraction_reference_risk":"0.0885579937304015","median_holding_hours":"5.25","median_stop_pips":"11.300000000000754","negative_complete_entry_years":"12","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.12225609756098264","positive_complete_entry_years":"7","profit_factor":"1.141279812079737","raw_signal_sha256":"c0473c622ba0fe1d0913907678c17296617e45cd566aa374c1b6e64bfd032817","raw_signals":"50","total_r":"4.973049385206723","trailing_no_entry_days":"365.1041666666667","win_rate_pct":"30.0","wins":"15","worst_12m_end":"2008-01-01T00:00:00Z","worst_12m_realized_r":"-3.0","worst_12m_start":"2007-01-01T00:00:00Z","worst_24m_end":"2012-11-01T00:00:00Z","worst_24m_realized_r":"-4.199999999999852","worst_24m_start":"2010-11-01T00:00:00Z","worst_36m_end":"2013-11-01T00:00:00Z","worst_36m_realized_r":"-6.199999999999852","worst_36m_start":"2010-11-01T00:00:00Z","zero_entry_12m_windows":"25","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"2","zero_entry_months":"221"},{"accepted_ledger_sha256":"a3038d0626255935765f62c1b0b80ea95a31b56e3bd2019181d9888b3aba3ffc","closed_trades":"50","config_id":"M_L100_D0025_B125_R150","cost_ticks":"20","dual_touch_closed":"0","eligible_isolated_signals":"50","entries_before_market_closure":"0","entry_next_open_gap_count":"32","execution_model":"STOP_FIRST_GAP_STRESS","expectancy_r":"0.020777267113731082","gap_stop_closed":"1","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"50","isolated_open":"0","isolated_profit_factor":"1.0295551452542406","isolated_total_r":"1.038863355686554","leading_no_entry_days":"23.510416666666668","losses":"35","max_closed_dd_r":"-9.55418118466884","max_consecutive_zero_entry_months":"20","max_holding_hours":"127.0","max_losing_streak":"7","maximum_inter_entry_gap_days":"626.9791666666666","median_cost_fraction_reference_risk":"0.177115987460803","median_holding_hours":"5.25","median_stop_pips":"11.300000000000754","negative_complete_entry_years":"12","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.2445121951219653","positive_complete_entry_years":"7","profit_factor":"1.0295551452542406","raw_signal_sha256":"c0473c622ba0fe1d0913907678c17296617e45cd566aa374c1b6e64bfd032817","raw_signals":"50","total_r":"1.038863355686554","trailing_no_entry_days":"365.1041666666667","win_rate_pct":"30.0","wins":"15","worst_12m_end":"2008-01-01T00:00:00Z","worst_12m_realized_r":"-3.0","worst_12m_start":"2007-01-01T00:00:00Z","worst_24m_end":"2012-11-01T00:00:00Z","worst_24m_realized_r":"-4.1499999999998884","worst_24m_start":"2010-11-01T00:00:00Z","worst_36m_end":"2013-11-01T00:00:00Z","worst_36m_realized_r":"-6.1499999999998884","worst_36m_start":"2010-11-01T00:00:00Z","zero_entry_12m_windows":"25","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"2","zero_entry_months":"221"},{"accepted_ledger_sha256":"73a76808d1b7f3313ba39975ed7bf075128fc2007c9e35fbffd413ccc021ef76","closed_trades":"50","config_id":"M_L100_D0025_B125_R150","cost_ticks":"40","dual_touch_closed":"0","eligible_isolated_signals":"50","entries_before_market_closure":"0","entry_next_open_gap_count":"32","execution_model":"STOP_FIRST_GAP_STRESS","expectancy_r":"-0.10342351005965648","gap_stop_closed":"1","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"50","isolated_open":"0","isolated_profit_factor":"0.8526730625930817","isolated_total_r":"-5.171175502982824","leading_no_entry_days":"23.510416666666668","losses":"35","max_closed_dd_r":"-10.995993179880507","max_consecutive_zero_entry_months":"20","max_holding_hours":"127.0","max_losing_streak":"7","maximum_inter_entry_gap_days":"626.9791666666666","median_cost_fraction_reference_risk":"0.354231974921606","median_holding_hours":"5.25","median_stop_pips":"11.300000000000754","negative_complete_entry_years":"12","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.4890243902439306","positive_complete_entry_years":"7","profit_factor":"0.8526730625930817","raw_signal_sha256":"c0473c622ba0fe1d0913907678c17296617e45cd566aa374c1b6e64bfd032817","raw_signals":"50","total_r":"-5.171175502982824","trailing_no_entry_days":"365.1041666666667","win_rate_pct":"30.0","wins":"15","worst_12m_end":"2008-01-01T00:00:00Z","worst_12m_realized_r":"-3.0","worst_12m_start":"2007-01-01T00:00:00Z","worst_24m_end":"2012-11-01T00:00:00Z","worst_24m_realized_r":"-4.099999999999926","worst_24m_start":"2010-11-01T00:00:00Z","worst_36m_end":"2013-11-01T00:00:00Z","worst_36m_realized_r":"-6.099999999999926","worst_36m_start":"2010-11-01T00:00:00Z","zero_entry_12m_windows":"25","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"2","zero_entry_months":"221"},{"accepted_ledger_sha256":"3eb14743f15446c065cc4c0e50025ad494802f1fccdd6adfa3c69f6650a8feb6","closed_trades":"57","config_id":"M_L200_D0050_B125_R150__AVAILABLE_H1_RELATIVE_VOLATILITY","cost_ticks":"10","dual_touch_closed":"0","eligible_isolated_signals":"57","entries_before_market_closure":"0","entry_next_open_gap_count":"45","execution_model":"NEAREST_OPEN_SENSITIVITY","expectancy_r":"0.08914356408068075","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"57","isolated_open":"0","isolated_profit_factor":"1.12702957881497","isolated_total_r":"5.081183152598802","leading_no_entry_days":"26.979166666666668","losses":"40","max_closed_dd_r":"-15.002304147465386","max_consecutive_zero_entry_months":"16","max_holding_hours":"1356.75","max_losing_streak":"11","maximum_inter_entry_gap_days":"496.0416666666667","median_cost_fraction_reference_risk":"0.08620689655172044","median_holding_hours":"5.25","median_stop_pips":"11.600000000000499","negative_complete_entry_years":"12","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.13405405405405374","positive_complete_entry_years":"9","profit_factor":"1.12702957881497","raw_signal_sha256":"6694fe6caffbe3e290268273e8a60d1380afe68ebc6d632bb36a373ec49a0b92","raw_signals":"57","total_r":"5.081183152598802","trailing_no_entry_days":"212.63541666666666","win_rate_pct":"29.82456140350877","wins":"17","worst_12m_end":"2010-05-01T00:00:00Z","worst_12m_realized_r":"-4.0","worst_12m_start":"2009-05-01T00:00:00Z","worst_24m_end":"2011-06-01T00:00:00Z","worst_24m_realized_r":"-7.0","worst_24m_start":"2009-06-01T00:00:00Z","worst_36m_end":"2012-05-01T00:00:00Z","worst_36m_realized_r":"-6.444444444444445","worst_36m_start":"2009-05-01T00:00:00Z","zero_entry_12m_windows":"8","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"0","zero_entry_months":"212"},{"accepted_ledger_sha256":"6411db59fdb4e9484c962adc21c2e01b33311dde1c6054c02ad6f3fdb0fdb811","closed_trades":"57","config_id":"M_L200_D0050_B125_R150__AVAILABLE_H1_RELATIVE_VOLATILITY","cost_ticks":"20","dual_touch_closed":"0","eligible_isolated_signals":"57","entries_before_market_closure":"0","entry_next_open_gap_count":"45","execution_model":"NEAREST_OPEN_SENSITIVITY","expectancy_r":"0.004719493655705163","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"57","isolated_open":"0","isolated_profit_factor":"1.0067252784593799","isolated_total_r":"0.2690111383751943","leading_no_entry_days":"26.979166666666668","losses":"40","max_closed_dd_r":"-16.542458572126613","max_consecutive_zero_entry_months":"16","max_holding_hours":"1356.75","max_losing_streak":"11","maximum_inter_entry_gap_days":"496.0416666666667","median_cost_fraction_reference_risk":"0.17241379310344088","median_holding_hours":"5.25","median_stop_pips":"11.600000000000499","negative_complete_entry_years":"12","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.2681081081081075","positive_complete_entry_years":"9","profit_factor":"1.0067252784593799","raw_signal_sha256":"6694fe6caffbe3e290268273e8a60d1380afe68ebc6d632bb36a373ec49a0b92","raw_signals":"57","total_r":"0.2690111383751943","trailing_no_entry_days":"212.63541666666666","win_rate_pct":"29.82456140350877","wins":"17","worst_12m_end":"2010-05-01T00:00:00Z","worst_12m_realized_r":"-4.0","worst_12m_start":"2009-05-01T00:00:00Z","worst_24m_end":"2011-06-01T00:00:00Z","worst_24m_realized_r":"-7.0","worst_24m_start":"2009-06-01T00:00:00Z","worst_36m_end":"2012-05-01T00:00:00Z","worst_36m_realized_r":"-6.8","worst_36m_start":"2009-05-01T00:00:00Z","zero_entry_12m_windows":"8","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"0","zero_entry_months":"212"},{"accepted_ledger_sha256":"5af00be79eac266547e984b4c7689c980dab90ea8237eac1a41294815a98875d","closed_trades":"57","config_id":"M_L200_D0050_B125_R150__AVAILABLE_H1_RELATIVE_VOLATILITY","cost_ticks":"40","dual_touch_closed":"0","eligible_isolated_signals":"57","entries_before_market_closure":"0","entry_next_open_gap_count":"45","execution_model":"NEAREST_OPEN_SENSITIVITY","expectancy_r":"-0.1258107954829195","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"57","isolated_open":"0","isolated_profit_factor":"0.8207196164368398","isolated_total_r":"-7.171215342526413","leading_no_entry_days":"26.979166666666668","losses":"40","max_closed_dd_r":"-18.76785178247647","max_consecutive_zero_entry_months":"16","max_holding_hours":"1356.75","max_losing_streak":"11","maximum_inter_entry_gap_days":"496.0416666666667","median_cost_fraction_reference_risk":"0.34482758620688175","median_holding_hours":"5.25","median_stop_pips":"11.600000000000499","negative_complete_entry_years":"13","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.536216216216215","positive_complete_entry_years":"8","profit_factor":"0.8207196164368398","raw_signal_sha256":"6694fe6caffbe3e290268273e8a60d1380afe68ebc6d632bb36a373ec49a0b92","raw_signals":"57","total_r":"-7.171215342526413","trailing_no_entry_days":"212.63541666666666","win_rate_pct":"29.82456140350877","wins":"17","worst_12m_end":"2010-05-01T00:00:00Z","worst_12m_realized_r":"-4.0","worst_12m_start":"2009-05-01T00:00:00Z","worst_24m_end":"2011-06-01T00:00:00Z","worst_24m_realized_r":"-7.0","worst_24m_start":"2009-06-01T00:00:00Z","worst_36m_end":"2012-05-01T00:00:00Z","worst_36m_realized_r":"-7.333333333333332","worst_36m_start":"2009-05-01T00:00:00Z","zero_entry_12m_windows":"8","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"0","zero_entry_months":"212"},{"accepted_ledger_sha256":"cf32b90cf1d08c5789e59752a8d8594ed773e324e736ed1df5a934bbfd925b19","closed_trades":"57","config_id":"M_L200_D0050_B125_R150__AVAILABLE_H1_RELATIVE_VOLATILITY","cost_ticks":"10","dual_touch_closed":"0","eligible_isolated_signals":"57","entries_before_market_closure":"0","entry_next_open_gap_count":"45","execution_model":"STOP_FIRST_GAP_STRESS","expectancy_r":"0.08914356408068075","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"57","isolated_open":"0","isolated_profit_factor":"1.12702957881497","isolated_total_r":"5.081183152598802","leading_no_entry_days":"26.979166666666668","losses":"40","max_closed_dd_r":"-15.002304147465386","max_consecutive_zero_entry_months":"16","max_holding_hours":"1356.75","max_losing_streak":"11","maximum_inter_entry_gap_days":"496.0416666666667","median_cost_fraction_reference_risk":"0.08620689655172044","median_holding_hours":"5.25","median_stop_pips":"11.600000000000499","negative_complete_entry_years":"12","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.13405405405405374","positive_complete_entry_years":"9","profit_factor":"1.12702957881497","raw_signal_sha256":"6694fe6caffbe3e290268273e8a60d1380afe68ebc6d632bb36a373ec49a0b92","raw_signals":"57","total_r":"5.081183152598802","trailing_no_entry_days":"212.63541666666666","win_rate_pct":"29.82456140350877","wins":"17","worst_12m_end":"2010-05-01T00:00:00Z","worst_12m_realized_r":"-4.0","worst_12m_start":"2009-05-01T00:00:00Z","worst_24m_end":"2011-06-01T00:00:00Z","worst_24m_realized_r":"-7.0","worst_24m_start":"2009-06-01T00:00:00Z","worst_36m_end":"2012-05-01T00:00:00Z","worst_36m_realized_r":"-6.444444444444445","worst_36m_start":"2009-05-01T00:00:00Z","zero_entry_12m_windows":"8","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"0","zero_entry_months":"212"},{"accepted_ledger_sha256":"7cb47bb82d91e482467962e2db4488b7cae2c9078fd0201efb4936d4a8e42d09","closed_trades":"57","config_id":"M_L200_D0050_B125_R150__AVAILABLE_H1_RELATIVE_VOLATILITY","cost_ticks":"20","dual_touch_closed":"0","eligible_isolated_signals":"57","entries_before_market_closure":"0","entry_next_open_gap_count":"45","execution_model":"STOP_FIRST_GAP_STRESS","expectancy_r":"0.004719493655705163","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"57","isolated_open":"0","isolated_profit_factor":"1.0067252784593799","isolated_total_r":"0.2690111383751943","leading_no_entry_days":"26.979166666666668","losses":"40","max_closed_dd_r":"-16.542458572126613","max_consecutive_zero_entry_months":"16","max_holding_hours":"1356.75","max_losing_streak":"11","maximum_inter_entry_gap_days":"496.0416666666667","median_cost_fraction_reference_risk":"0.17241379310344088","median_holding_hours":"5.25","median_stop_pips":"11.600000000000499","negative_complete_entry_years":"12","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.2681081081081075","positive_complete_entry_years":"9","profit_factor":"1.0067252784593799","raw_signal_sha256":"6694fe6caffbe3e290268273e8a60d1380afe68ebc6d632bb36a373ec49a0b92","raw_signals":"57","total_r":"0.2690111383751943","trailing_no_entry_days":"212.63541666666666","win_rate_pct":"29.82456140350877","wins":"17","worst_12m_end":"2010-05-01T00:00:00Z","worst_12m_realized_r":"-4.0","worst_12m_start":"2009-05-01T00:00:00Z","worst_24m_end":"2011-06-01T00:00:00Z","worst_24m_realized_r":"-7.0","worst_24m_start":"2009-06-01T00:00:00Z","worst_36m_end":"2012-05-01T00:00:00Z","worst_36m_realized_r":"-6.8","worst_36m_start":"2009-05-01T00:00:00Z","zero_entry_12m_windows":"8","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"0","zero_entry_months":"212"},{"accepted_ledger_sha256":"02140139e2e9a2e9bc942f646b1032d0ba4678ed86de8dbc6b5a9073c35f0fc2","closed_trades":"57","config_id":"M_L200_D0050_B125_R150__AVAILABLE_H1_RELATIVE_VOLATILITY","cost_ticks":"40","dual_touch_closed":"0","eligible_isolated_signals":"57","entries_before_market_closure":"0","entry_next_open_gap_count":"45","execution_model":"STOP_FIRST_GAP_STRESS","expectancy_r":"-0.1258107954829195","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"57","isolated_open":"0","isolated_profit_factor":"0.8207196164368398","isolated_total_r":"-7.171215342526413","leading_no_entry_days":"26.979166666666668","losses":"40","max_closed_dd_r":"-18.76785178247647","max_consecutive_zero_entry_months":"16","max_holding_hours":"1356.75","max_losing_streak":"11","maximum_inter_entry_gap_days":"496.0416666666667","median_cost_fraction_reference_risk":"0.34482758620688175","median_holding_hours":"5.25","median_stop_pips":"11.600000000000499","negative_complete_entry_years":"13","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.536216216216215","positive_complete_entry_years":"8","profit_factor":"0.8207196164368398","raw_signal_sha256":"6694fe6caffbe3e290268273e8a60d1380afe68ebc6d632bb36a373ec49a0b92","raw_signals":"57","total_r":"-7.171215342526413","trailing_no_entry_days":"212.63541666666666","win_rate_pct":"29.82456140350877","wins":"17","worst_12m_end":"2010-05-01T00:00:00Z","worst_12m_realized_r":"-4.0","worst_12m_start":"2009-05-01T00:00:00Z","worst_24m_end":"2011-06-01T00:00:00Z","worst_24m_realized_r":"-7.0","worst_24m_start":"2009-06-01T00:00:00Z","worst_36m_end":"2012-05-01T00:00:00Z","worst_36m_realized_r":"-7.333333333333332","worst_36m_start":"2009-05-01T00:00:00Z","zero_entry_12m_windows":"8","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"0","zero_entry_months":"212"},{"accepted_ledger_sha256":"ed7b96e16101616c6a8da06c3d11e3f7a0466484305a5df0fdb10aef12cfb059","closed_trades":"50","config_id":"M_L100_D0025_B125_R150__AVAILABLE_H1_RELATIVE_VOLATILITY","cost_ticks":"10","dual_touch_closed":"0","eligible_isolated_signals":"50","entries_before_market_closure":"0","entry_next_open_gap_count":"32","execution_model":"NEAREST_OPEN_SENSITIVITY","expectancy_r":"0.1034609877041315","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"50","isolated_open":"0","isolated_profit_factor":"1.147801411005902","isolated_total_r":"5.173049385206575","leading_no_entry_days":"23.510416666666668","losses":"35","max_closed_dd_r":"-8.366006102076463","max_consecutive_zero_entry_months":"20","max_holding_hours":"127.25","max_losing_streak":"7","maximum_inter_entry_gap_days":"626.9791666666666","median_cost_fraction_reference_risk":"0.0885579937304015","median_holding_hours":"5.25","median_stop_pips":"11.300000000000754","negative_complete_entry_years":"12","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.12225609756098264","positive_complete_entry_years":"7","profit_factor":"1.147801411005902","raw_signal_sha256":"c0473c622ba0fe1d0913907678c17296617e45cd566aa374c1b6e64bfd032817","raw_signals":"50","total_r":"5.173049385206575","trailing_no_entry_days":"365.1041666666667","win_rate_pct":"30.0","wins":"15","worst_12m_end":"2008-01-01T00:00:00Z","worst_12m_realized_r":"-3.0","worst_12m_start":"2007-01-01T00:00:00Z","worst_24m_end":"2012-11-01T00:00:00Z","worst_24m_realized_r":"-4.0","worst_24m_start":"2010-11-01T00:00:00Z","worst_36m_end":"2013-11-01T00:00:00Z","worst_36m_realized_r":"-6.0","worst_36m_start":"2010-11-01T00:00:00Z","zero_entry_12m_windows":"25","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"2","zero_entry_months":"221"},{"accepted_ledger_sha256":"7ae97e2b004a890e33a9a2454acd8ced2672eeeb5337ed0f72ef6a116c56a499","closed_trades":"50","config_id":"M_L100_D0025_B125_R150__AVAILABLE_H1_RELATIVE_VOLATILITY","cost_ticks":"20","dual_touch_closed":"0","eligible_isolated_signals":"50","entries_before_market_closure":"0","entry_next_open_gap_count":"32","execution_model":"NEAREST_OPEN_SENSITIVITY","expectancy_r":"0.023777267113728868","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"50","isolated_open":"0","isolated_profit_factor":"1.033967524448184","isolated_total_r":"1.1888633556864434","leading_no_entry_days":"23.510416666666668","losses":"35","max_closed_dd_r":"-9.40418118466895","max_consecutive_zero_entry_months":"20","max_holding_hours":"127.25","max_losing_streak":"7","maximum_inter_entry_gap_days":"626.9791666666666","median_cost_fraction_reference_risk":"0.177115987460803","median_holding_hours":"5.25","median_stop_pips":"11.300000000000754","negative_complete_entry_years":"12","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.2445121951219653","positive_complete_entry_years":"7","profit_factor":"1.033967524448184","raw_signal_sha256":"c0473c622ba0fe1d0913907678c17296617e45cd566aa374c1b6e64bfd032817","raw_signals":"50","total_r":"1.1888633556864434","trailing_no_entry_days":"365.1041666666667","win_rate_pct":"30.0","wins":"15","worst_12m_end":"2008-01-01T00:00:00Z","worst_12m_realized_r":"-3.0","worst_12m_start":"2007-01-01T00:00:00Z","worst_24m_end":"2012-11-01T00:00:00Z","worst_24m_realized_r":"-4.0","worst_24m_start":"2010-11-01T00:00:00Z","worst_36m_end":"2013-11-01T00:00:00Z","worst_36m_realized_r":"-6.0","worst_36m_start":"2010-11-01T00:00:00Z","zero_entry_12m_windows":"25","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"2","zero_entry_months":"221"},{"accepted_ledger_sha256":"ccc1c8ef4bbc6ae877a8886b01bdfa5b8d7c78e0aef7a2dbcc80ce5f0be91a81","closed_trades":"50","config_id":"M_L100_D0025_B125_R150__AVAILABLE_H1_RELATIVE_VOLATILITY","cost_ticks":"40","dual_touch_closed":"0","eligible_isolated_signals":"50","entries_before_market_closure":"0","entry_next_open_gap_count":"32","execution_model":"NEAREST_OPEN_SENSITIVITY","expectancy_r":"-0.10142351005965797","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"50","isolated_open":"0","isolated_profit_factor":"0.8551092713433458","isolated_total_r":"-5.071175502982898","leading_no_entry_days":"23.510416666666668","losses":"35","max_closed_dd_r":"-10.895993179880582","max_consecutive_zero_entry_months":"20","max_holding_hours":"127.25","max_losing_streak":"7","maximum_inter_entry_gap_days":"626.9791666666666","median_cost_fraction_reference_risk":"0.354231974921606","median_holding_hours":"5.25","median_stop_pips":"11.300000000000754","negative_complete_entry_years":"12","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.4890243902439306","positive_complete_entry_years":"7","profit_factor":"0.8551092713433458","raw_signal_sha256":"c0473c622ba0fe1d0913907678c17296617e45cd566aa374c1b6e64bfd032817","raw_signals":"50","total_r":"-5.071175502982898","trailing_no_entry_days":"365.1041666666667","win_rate_pct":"30.0","wins":"15","worst_12m_end":"2008-01-01T00:00:00Z","worst_12m_realized_r":"-3.0","worst_12m_start":"2007-01-01T00:00:00Z","worst_24m_end":"2012-11-01T00:00:00Z","worst_24m_realized_r":"-4.0","worst_24m_start":"2010-11-01T00:00:00Z","worst_36m_end":"2013-11-01T00:00:00Z","worst_36m_realized_r":"-6.0","worst_36m_start":"2010-11-01T00:00:00Z","zero_entry_12m_windows":"25","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"2","zero_entry_months":"221"},{"accepted_ledger_sha256":"03b83b267374462e7b5bbe210328100784c2fa4cd5da0e88fcae1506eef7f020","closed_trades":"50","config_id":"M_L100_D0025_B125_R150__AVAILABLE_H1_RELATIVE_VOLATILITY","cost_ticks":"10","dual_touch_closed":"0","eligible_isolated_signals":"50","entries_before_market_closure":"0","entry_next_open_gap_count":"32","execution_model":"STOP_FIRST_GAP_STRESS","expectancy_r":"0.09946098770413446","gap_stop_closed":"1","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"50","isolated_open":"0","isolated_profit_factor":"1.141279812079737","isolated_total_r":"4.973049385206723","leading_no_entry_days":"23.510416666666668","losses":"35","max_closed_dd_r":"-8.566006102076315","max_consecutive_zero_entry_months":"20","max_holding_hours":"127.0","max_losing_streak":"7","maximum_inter_entry_gap_days":"626.9791666666666","median_cost_fraction_reference_risk":"0.0885579937304015","median_holding_hours":"5.25","median_stop_pips":"11.300000000000754","negative_complete_entry_years":"12","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.12225609756098264","positive_complete_entry_years":"7","profit_factor":"1.141279812079737","raw_signal_sha256":"c0473c622ba0fe1d0913907678c17296617e45cd566aa374c1b6e64bfd032817","raw_signals":"50","total_r":"4.973049385206723","trailing_no_entry_days":"365.1041666666667","win_rate_pct":"30.0","wins":"15","worst_12m_end":"2008-01-01T00:00:00Z","worst_12m_realized_r":"-3.0","worst_12m_start":"2007-01-01T00:00:00Z","worst_24m_end":"2012-11-01T00:00:00Z","worst_24m_realized_r":"-4.199999999999852","worst_24m_start":"2010-11-01T00:00:00Z","worst_36m_end":"2013-11-01T00:00:00Z","worst_36m_realized_r":"-6.199999999999852","worst_36m_start":"2010-11-01T00:00:00Z","zero_entry_12m_windows":"25","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"2","zero_entry_months":"221"},{"accepted_ledger_sha256":"dc641efaf27edd2c73dfdec746f7ff72de3a8e04ee605e224a62a86cf34544f6","closed_trades":"50","config_id":"M_L100_D0025_B125_R150__AVAILABLE_H1_RELATIVE_VOLATILITY","cost_ticks":"20","dual_touch_closed":"0","eligible_isolated_signals":"50","entries_before_market_closure":"0","entry_next_open_gap_count":"32","execution_model":"STOP_FIRST_GAP_STRESS","expectancy_r":"0.020777267113731082","gap_stop_closed":"1","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"50","isolated_open":"0","isolated_profit_factor":"1.0295551452542406","isolated_total_r":"1.038863355686554","leading_no_entry_days":"23.510416666666668","losses":"35","max_closed_dd_r":"-9.55418118466884","max_consecutive_zero_entry_months":"20","max_holding_hours":"127.0","max_losing_streak":"7","maximum_inter_entry_gap_days":"626.9791666666666","median_cost_fraction_reference_risk":"0.177115987460803","median_holding_hours":"5.25","median_stop_pips":"11.300000000000754","negative_complete_entry_years":"12","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.2445121951219653","positive_complete_entry_years":"7","profit_factor":"1.0295551452542406","raw_signal_sha256":"c0473c622ba0fe1d0913907678c17296617e45cd566aa374c1b6e64bfd032817","raw_signals":"50","total_r":"1.038863355686554","trailing_no_entry_days":"365.1041666666667","win_rate_pct":"30.0","wins":"15","worst_12m_end":"2008-01-01T00:00:00Z","worst_12m_realized_r":"-3.0","worst_12m_start":"2007-01-01T00:00:00Z","worst_24m_end":"2012-11-01T00:00:00Z","worst_24m_realized_r":"-4.1499999999998884","worst_24m_start":"2010-11-01T00:00:00Z","worst_36m_end":"2013-11-01T00:00:00Z","worst_36m_realized_r":"-6.1499999999998884","worst_36m_start":"2010-11-01T00:00:00Z","zero_entry_12m_windows":"25","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"2","zero_entry_months":"221"},{"accepted_ledger_sha256":"b9dc78e99aee3eac17458e866ecef4ef978308ab91cc4a06635b57ac3665cd41","closed_trades":"50","config_id":"M_L100_D0025_B125_R150__AVAILABLE_H1_RELATIVE_VOLATILITY","cost_ticks":"40","dual_touch_closed":"0","eligible_isolated_signals":"50","entries_before_market_closure":"0","entry_next_open_gap_count":"32","execution_model":"STOP_FIRST_GAP_STRESS","expectancy_r":"-0.10342351005965648","gap_stop_closed":"1","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"50","isolated_open":"0","isolated_profit_factor":"0.8526730625930817","isolated_total_r":"-5.171175502982824","leading_no_entry_days":"23.510416666666668","losses":"35","max_closed_dd_r":"-10.995993179880507","max_consecutive_zero_entry_months":"20","max_holding_hours":"127.0","max_losing_streak":"7","maximum_inter_entry_gap_days":"626.9791666666666","median_cost_fraction_reference_risk":"0.354231974921606","median_holding_hours":"5.25","median_stop_pips":"11.300000000000754","negative_complete_entry_years":"12","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.4890243902439306","positive_complete_entry_years":"7","profit_factor":"0.8526730625930817","raw_signal_sha256":"c0473c622ba0fe1d0913907678c17296617e45cd566aa374c1b6e64bfd032817","raw_signals":"50","total_r":"-5.171175502982824","trailing_no_entry_days":"365.1041666666667","win_rate_pct":"30.0","wins":"15","worst_12m_end":"2008-01-01T00:00:00Z","worst_12m_realized_r":"-3.0","worst_12m_start":"2007-01-01T00:00:00Z","worst_24m_end":"2012-11-01T00:00:00Z","worst_24m_realized_r":"-4.099999999999926","worst_24m_start":"2010-11-01T00:00:00Z","worst_36m_end":"2013-11-01T00:00:00Z","worst_36m_realized_r":"-6.099999999999926","worst_36m_start":"2010-11-01T00:00:00Z","zero_entry_12m_windows":"25","zero_entry_24m_windows":"0","zero_entry_36m_windows":"0","zero_entry_complete_years":"2","zero_entry_months":"221"},{"accepted_ledger_sha256":"4eba5ef9907ff42686e3f770447cfd4817063638f244618e1243165cc586caa2","closed_trades":"23","config_id":"M_L200_D0050_B125_R150__H1_RELATIVE_VOLATILITY_GE_P1P00","cost_ticks":"10","dual_touch_closed":"0","eligible_isolated_signals":"23","entries_before_market_closure":"0","entry_next_open_gap_count":"20","execution_model":"NEAREST_OPEN_SENSITIVITY","expectancy_r":"0.46082658559874085","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"23","isolated_open":"0","isolated_profit_factor":"1.7570722477693599","isolated_total_r":"10.599011468771039","leading_no_entry_days":"257.3125","losses":"14","max_closed_dd_r":"-7.555555555555452","max_consecutive_zero_entry_months":"31","max_holding_hours":"99.25","max_losing_streak":"5","maximum_inter_entry_gap_days":"986.9479166666666","median_cost_fraction_reference_risk":"0.058823529411763505","median_holding_hours":"17.5","median_stop_pips":"17.000000000000348","negative_complete_entry_years":"7","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.12794326241134482","positive_complete_entry_years":"8","profit_factor":"1.7570722477693599","raw_signal_sha256":"99d1abd6404af4f6bc9f8dcfd33f1abf1ff930e51ffef714c2afaa9d57fef7df","raw_signals":"23","total_r":"10.599011468771039","trailing_no_entry_days":"212.63541666666666","win_rate_pct":"39.130434782608695","wins":"9","worst_12m_end":"2011-06-01T00:00:00Z","worst_12m_realized_r":"-3.0","worst_12m_start":"2010-06-01T00:00:00Z","worst_24m_end":"2012-03-01T00:00:00Z","worst_24m_realized_r":"-4.0","worst_24m_start":"2010-03-01T00:00:00Z","worst_36m_end":"2012-05-01T00:00:00Z","worst_36m_realized_r":"-4.0","worst_36m_start":"2009-05-01T00:00:00Z","zero_entry_12m_windows":"71","zero_entry_24m_windows":"15","zero_entry_36m_windows":"0","zero_entry_complete_years":"6","zero_entry_months":"241"},{"accepted_ledger_sha256":"a72497028ba75d37b134e5338f1c9da8fd22e935032c2603c0dc1f52abfe766f","closed_trades":"23","config_id":"M_L200_D0050_B125_R150__H1_RELATIVE_VOLATILITY_GE_P1P00","cost_ticks":"20","dual_touch_closed":"0","eligible_isolated_signals":"23","entries_before_market_closure":"0","entry_next_open_gap_count":"20","execution_model":"NEAREST_OPEN_SENSITIVITY","expectancy_r":"0.37197131019242713","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"23","isolated_open":"0","isolated_profit_factor":"1.6110957238875587","isolated_total_r":"8.555340134425824","leading_no_entry_days":"257.3125","losses":"14","max_closed_dd_r":"-7.975609756097402","max_consecutive_zero_entry_months":"31","max_holding_hours":"99.25","max_losing_streak":"5","maximum_inter_entry_gap_days":"986.9479166666666","median_cost_fraction_reference_risk":"0.11764705882352701","median_holding_hours":"17.5","median_stop_pips":"17.000000000000348","negative_complete_entry_years":"7","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.25588652482268964","positive_complete_entry_years":"8","profit_factor":"1.6110957238875587","raw_signal_sha256":"99d1abd6404af4f6bc9f8dcfd33f1abf1ff930e51ffef714c2afaa9d57fef7df","raw_signals":"23","total_r":"8.555340134425824","trailing_no_entry_days":"212.63541666666666","win_rate_pct":"39.130434782608695","wins":"9","worst_12m_end":"2011-06-01T00:00:00Z","worst_12m_realized_r":"-3.0","worst_12m_start":"2010-06-01T00:00:00Z","worst_24m_end":"2012-03-01T00:00:00Z","worst_24m_realized_r":"-4.0","worst_24m_start":"2010-03-01T00:00:00Z","worst_36m_end":"2012-05-01T00:00:00Z","worst_36m_realized_r":"-4.0","worst_36m_start":"2009-05-01T00:00:00Z","zero_entry_12m_windows":"71","zero_entry_24m_windows":"15","zero_entry_36m_windows":"0","zero_entry_complete_years":"6","zero_entry_months":"241"},{"accepted_ledger_sha256":"59f010629087551206c35f0c9a50851f2c01c865e21b02806d0adfbe8c6082c6","closed_trades":"23","config_id":"M_L200_D0050_B125_R150__H1_RELATIVE_VOLATILITY_GE_P1P00","cost_ticks":"40","dual_touch_closed":"0","eligible_isolated_signals":"23","entries_before_market_closure":"0","entry_next_open_gap_count":"20","execution_model":"NEAREST_OPEN_SENSITIVITY","expectancy_r":"0.22735915162055656","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"23","isolated_open":"0","isolated_profit_factor":"1.3735186062337714","isolated_total_r":"5.229260487272801","leading_no_entry_days":"257.3125","losses":"14","max_closed_dd_r":"-8.568627450980188","max_consecutive_zero_entry_months":"31","max_holding_hours":"99.25","max_losing_streak":"5","maximum_inter_entry_gap_days":"986.9479166666666","median_cost_fraction_reference_risk":"0.23529411764705402","median_holding_hours":"17.5","median_stop_pips":"17.000000000000348","negative_complete_entry_years":"8","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.5117730496453793","positive_complete_entry_years":"7","profit_factor":"1.3735186062337714","raw_signal_sha256":"99d1abd6404af4f6bc9f8dcfd33f1abf1ff930e51ffef714c2afaa9d57fef7df","raw_signals":"23","total_r":"5.229260487272801","trailing_no_entry_days":"212.63541666666666","win_rate_pct":"39.130434782608695","wins":"9","worst_12m_end":"2011-06-01T00:00:00Z","worst_12m_realized_r":"-3.0","worst_12m_start":"2010-06-01T00:00:00Z","worst_24m_end":"2012-03-01T00:00:00Z","worst_24m_realized_r":"-4.0","worst_24m_start":"2010-03-01T00:00:00Z","worst_36m_end":"2012-05-01T00:00:00Z","worst_36m_realized_r":"-4.0","worst_36m_start":"2009-05-01T00:00:00Z","zero_entry_12m_windows":"71","zero_entry_24m_windows":"15","zero_entry_36m_windows":"0","zero_entry_complete_years":"6","zero_entry_months":"241"},{"accepted_ledger_sha256":"a1089a8548b4e453219e87e804811079f9805e71441f5eb54fe4ef0a7a07ca0a","closed_trades":"23","config_id":"M_L200_D0050_B125_R150__H1_RELATIVE_VOLATILITY_GE_P1P00","cost_ticks":"10","dual_touch_closed":"0","eligible_isolated_signals":"23","entries_before_market_closure":"0","entry_next_open_gap_count":"20","execution_model":"STOP_FIRST_GAP_STRESS","expectancy_r":"0.46082658559874085","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"23","isolated_open":"0","isolated_profit_factor":"1.7570722477693599","isolated_total_r":"10.599011468771039","leading_no_entry_days":"257.3125","losses":"14","max_closed_dd_r":"-7.555555555555452","max_consecutive_zero_entry_months":"31","max_holding_hours":"99.25","max_losing_streak":"5","maximum_inter_entry_gap_days":"986.9479166666666","median_cost_fraction_reference_risk":"0.058823529411763505","median_holding_hours":"17.5","median_stop_pips":"17.000000000000348","negative_complete_entry_years":"7","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.12794326241134482","positive_complete_entry_years":"8","profit_factor":"1.7570722477693599","raw_signal_sha256":"99d1abd6404af4f6bc9f8dcfd33f1abf1ff930e51ffef714c2afaa9d57fef7df","raw_signals":"23","total_r":"10.599011468771039","trailing_no_entry_days":"212.63541666666666","win_rate_pct":"39.130434782608695","wins":"9","worst_12m_end":"2011-06-01T00:00:00Z","worst_12m_realized_r":"-3.0","worst_12m_start":"2010-06-01T00:00:00Z","worst_24m_end":"2012-03-01T00:00:00Z","worst_24m_realized_r":"-4.0","worst_24m_start":"2010-03-01T00:00:00Z","worst_36m_end":"2012-05-01T00:00:00Z","worst_36m_realized_r":"-4.0","worst_36m_start":"2009-05-01T00:00:00Z","zero_entry_12m_windows":"71","zero_entry_24m_windows":"15","zero_entry_36m_windows":"0","zero_entry_complete_years":"6","zero_entry_months":"241"},{"accepted_ledger_sha256":"7f3592fc7e0bd553f092a91becaf4074886ee4a951fc57863e9d166c3b135519","closed_trades":"23","config_id":"M_L200_D0050_B125_R150__H1_RELATIVE_VOLATILITY_GE_P1P00","cost_ticks":"20","dual_touch_closed":"0","eligible_isolated_signals":"23","entries_before_market_closure":"0","entry_next_open_gap_count":"20","execution_model":"STOP_FIRST_GAP_STRESS","expectancy_r":"0.37197131019242713","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"23","isolated_open":"0","isolated_profit_factor":"1.6110957238875587","isolated_total_r":"8.555340134425824","leading_no_entry_days":"257.3125","losses":"14","max_closed_dd_r":"-7.975609756097402","max_consecutive_zero_entry_months":"31","max_holding_hours":"99.25","max_losing_streak":"5","maximum_inter_entry_gap_days":"986.9479166666666","median_cost_fraction_reference_risk":"0.11764705882352701","median_holding_hours":"17.5","median_stop_pips":"17.000000000000348","negative_complete_entry_years":"7","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.25588652482268964","positive_complete_entry_years":"8","profit_factor":"1.6110957238875587","raw_signal_sha256":"99d1abd6404af4f6bc9f8dcfd33f1abf1ff930e51ffef714c2afaa9d57fef7df","raw_signals":"23","total_r":"8.555340134425824","trailing_no_entry_days":"212.63541666666666","win_rate_pct":"39.130434782608695","wins":"9","worst_12m_end":"2011-06-01T00:00:00Z","worst_12m_realized_r":"-3.0","worst_12m_start":"2010-06-01T00:00:00Z","worst_24m_end":"2012-03-01T00:00:00Z","worst_24m_realized_r":"-4.0","worst_24m_start":"2010-03-01T00:00:00Z","worst_36m_end":"2012-05-01T00:00:00Z","worst_36m_realized_r":"-4.0","worst_36m_start":"2009-05-01T00:00:00Z","zero_entry_12m_windows":"71","zero_entry_24m_windows":"15","zero_entry_36m_windows":"0","zero_entry_complete_years":"6","zero_entry_months":"241"},{"accepted_ledger_sha256":"328546a3a02a5509b97cf76e665196292a73864d11e929c052c887d73472165e","closed_trades":"23","config_id":"M_L200_D0050_B125_R150__H1_RELATIVE_VOLATILITY_GE_P1P00","cost_ticks":"40","dual_touch_closed":"0","eligible_isolated_signals":"23","entries_before_market_closure":"0","entry_next_open_gap_count":"20","execution_model":"STOP_FIRST_GAP_STRESS","expectancy_r":"0.22735915162055656","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"23","isolated_open":"0","isolated_profit_factor":"1.3735186062337714","isolated_total_r":"5.229260487272801","leading_no_entry_days":"257.3125","losses":"14","max_closed_dd_r":"-8.568627450980188","max_consecutive_zero_entry_months":"31","max_holding_hours":"99.25","max_losing_streak":"5","maximum_inter_entry_gap_days":"986.9479166666666","median_cost_fraction_reference_risk":"0.23529411764705402","median_holding_hours":"17.5","median_stop_pips":"17.000000000000348","negative_complete_entry_years":"8","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.5117730496453793","positive_complete_entry_years":"7","profit_factor":"1.3735186062337714","raw_signal_sha256":"99d1abd6404af4f6bc9f8dcfd33f1abf1ff930e51ffef714c2afaa9d57fef7df","raw_signals":"23","total_r":"5.229260487272801","trailing_no_entry_days":"212.63541666666666","win_rate_pct":"39.130434782608695","wins":"9","worst_12m_end":"2011-06-01T00:00:00Z","worst_12m_realized_r":"-3.0","worst_12m_start":"2010-06-01T00:00:00Z","worst_24m_end":"2012-03-01T00:00:00Z","worst_24m_realized_r":"-4.0","worst_24m_start":"2010-03-01T00:00:00Z","worst_36m_end":"2012-05-01T00:00:00Z","worst_36m_realized_r":"-4.0","worst_36m_start":"2009-05-01T00:00:00Z","zero_entry_12m_windows":"71","zero_entry_24m_windows":"15","zero_entry_36m_windows":"0","zero_entry_complete_years":"6","zero_entry_months":"241"},{"accepted_ledger_sha256":"dfe290c1c0ac5a16267c967296d34ba6188f46f554beda005a9b0031815b6270","closed_trades":"19","config_id":"M_L100_D0025_B125_R150__H1_RELATIVE_VOLATILITY_GE_P1P00","cost_ticks":"10","dual_touch_closed":"0","eligible_isolated_signals":"19","entries_before_market_closure":"0","entry_next_open_gap_count":"13","execution_model":"NEAREST_OPEN_SENSITIVITY","expectancy_r":"0.9646757552808889","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"19","isolated_open":"0","isolated_profit_factor":"3.0365377055929876","isolated_total_r":"18.32883935033689","leading_no_entry_days":"192.15625","losses":"9","max_closed_dd_r":"-3.0","max_consecutive_zero_entry_months":"66","max_holding_hours":"99.25","max_losing_streak":"3","maximum_inter_entry_gap_days":"2025.8020833333333","median_cost_fraction_reference_risk":"0.06249999999999821","median_holding_hours":"14.75","median_stop_pips":"16.00000000000046","negative_complete_entry_years":"3","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.10000000000001102","positive_complete_entry_years":"8","profit_factor":"3.0365377055929876","raw_signal_sha256":"ebf44e3e7cadacef4f480d694193c7a8309ffb3cb2d3c2e3e6d0ddcf420cd045","raw_signals":"19","total_r":"18.32883935033689","trailing_no_entry_days":"1083.65625","win_rate_pct":"52.63157894736842","wins":"10","worst_12m_end":"2007-11-01T00:00:00Z","worst_12m_realized_r":"-1.0","worst_12m_start":"2006-11-01T00:00:00Z","worst_24m_end":"2012-11-01T00:00:00Z","worst_24m_realized_r":"-1.0","worst_24m_start":"2010-11-01T00:00:00Z","worst_36m_end":"2013-11-01T00:00:00Z","worst_36m_realized_r":"-2.0","worst_36m_start":"2010-11-01T00:00:00Z","zero_entry_12m_windows":"123","zero_entry_24m_windows":"66","zero_entry_36m_windows":"31","zero_entry_complete_years":"10","zero_entry_months":"245"},{"accepted_ledger_sha256":"6b4b95e05269c088d108ca303bc748bd9bc760dfffcb986adb586d8c60910e19","closed_trades":"19","config_id":"M_L100_D0025_B125_R150__H1_RELATIVE_VOLATILITY_GE_P1P00","cost_ticks":"20","dual_touch_closed":"0","eligible_isolated_signals":"19","entries_before_market_closure":"0","entry_next_open_gap_count":"13","execution_model":"NEAREST_OPEN_SENSITIVITY","expectancy_r":"0.8450600864814806","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"19","isolated_open":"0","isolated_profit_factor":"2.78401573812757","isolated_total_r":"16.05614164314813","leading_no_entry_days":"192.15625","losses":"9","max_closed_dd_r":"-3.0","max_consecutive_zero_entry_months":"66","max_holding_hours":"99.25","max_losing_streak":"3","maximum_inter_entry_gap_days":"2025.8020833333333","median_cost_fraction_reference_risk":"0.12499999999999642","median_holding_hours":"14.75","median_stop_pips":"16.00000000000046","negative_complete_entry_years":"3","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.20000000000002205","positive_complete_entry_years":"8","profit_factor":"2.78401573812757","raw_signal_sha256":"ebf44e3e7cadacef4f480d694193c7a8309ffb3cb2d3c2e3e6d0ddcf420cd045","raw_signals":"19","total_r":"16.05614164314813","trailing_no_entry_days":"1083.65625","win_rate_pct":"52.63157894736842","wins":"10","worst_12m_end":"2007-11-01T00:00:00Z","worst_12m_realized_r":"-1.0","worst_12m_start":"2006-11-01T00:00:00Z","worst_24m_end":"2012-11-01T00:00:00Z","worst_24m_realized_r":"-1.0","worst_24m_start":"2010-11-01T00:00:00Z","worst_36m_end":"2013-11-01T00:00:00Z","worst_36m_realized_r":"-2.0","worst_36m_start":"2010-11-01T00:00:00Z","zero_entry_12m_windows":"123","zero_entry_24m_windows":"66","zero_entry_36m_windows":"31","zero_entry_complete_years":"10","zero_entry_months":"245"},{"accepted_ledger_sha256":"810d3d734df3bfc04fd557f67b977e9f47e66b90f0e8113e23d1a900122bbad8","closed_trades":"19","config_id":"M_L100_D0025_B125_R150__H1_RELATIVE_VOLATILITY_GE_P1P00","cost_ticks":"40","dual_touch_closed":"0","eligible_isolated_signals":"19","entries_before_market_closure":"0","entry_next_open_gap_count":"13","execution_model":"NEAREST_OPEN_SENSITIVITY","expectancy_r":"0.650529749558262","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"19","isolated_open":"0","isolated_profit_factor":"2.3733405824007754","isolated_total_r":"12.360065241606979","leading_no_entry_days":"192.15625","losses":"9","max_closed_dd_r":"-3.0","max_consecutive_zero_entry_months":"66","max_holding_hours":"99.25","max_losing_streak":"3","maximum_inter_entry_gap_days":"2025.8020833333333","median_cost_fraction_reference_risk":"0.24999999999999284","median_holding_hours":"14.75","median_stop_pips":"16.00000000000046","negative_complete_entry_years":"3","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.4000000000000441","positive_complete_entry_years":"8","profit_factor":"2.3733405824007754","raw_signal_sha256":"ebf44e3e7cadacef4f480d694193c7a8309ffb3cb2d3c2e3e6d0ddcf420cd045","raw_signals":"19","total_r":"12.360065241606979","trailing_no_entry_days":"1083.65625","win_rate_pct":"52.63157894736842","wins":"10","worst_12m_end":"2007-11-01T00:00:00Z","worst_12m_realized_r":"-1.0","worst_12m_start":"2006-11-01T00:00:00Z","worst_24m_end":"2012-11-01T00:00:00Z","worst_24m_realized_r":"-1.0","worst_24m_start":"2010-11-01T00:00:00Z","worst_36m_end":"2013-11-01T00:00:00Z","worst_36m_realized_r":"-2.0","worst_36m_start":"2010-11-01T00:00:00Z","zero_entry_12m_windows":"123","zero_entry_24m_windows":"66","zero_entry_36m_windows":"31","zero_entry_complete_years":"10","zero_entry_months":"245"},{"accepted_ledger_sha256":"0b42d0efb1c433626b8852ab0955b10fc0d528ae156521cd93c29ccabb753c2a","closed_trades":"19","config_id":"M_L100_D0025_B125_R150__H1_RELATIVE_VOLATILITY_GE_P1P00","cost_ticks":"10","dual_touch_closed":"0","eligible_isolated_signals":"19","entries_before_market_closure":"0","entry_next_open_gap_count":"13","execution_model":"STOP_FIRST_GAP_STRESS","expectancy_r":"0.9646757552808889","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"19","isolated_open":"0","isolated_profit_factor":"3.0365377055929876","isolated_total_r":"18.32883935033689","leading_no_entry_days":"192.15625","losses":"9","max_closed_dd_r":"-3.0","max_consecutive_zero_entry_months":"66","max_holding_hours":"99.25","max_losing_streak":"3","maximum_inter_entry_gap_days":"2025.8020833333333","median_cost_fraction_reference_risk":"0.06249999999999821","median_holding_hours":"14.75","median_stop_pips":"16.00000000000046","negative_complete_entry_years":"3","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.10000000000001102","positive_complete_entry_years":"8","profit_factor":"3.0365377055929876","raw_signal_sha256":"ebf44e3e7cadacef4f480d694193c7a8309ffb3cb2d3c2e3e6d0ddcf420cd045","raw_signals":"19","total_r":"18.32883935033689","trailing_no_entry_days":"1083.65625","win_rate_pct":"52.63157894736842","wins":"10","worst_12m_end":"2007-11-01T00:00:00Z","worst_12m_realized_r":"-1.0","worst_12m_start":"2006-11-01T00:00:00Z","worst_24m_end":"2012-11-01T00:00:00Z","worst_24m_realized_r":"-1.0","worst_24m_start":"2010-11-01T00:00:00Z","worst_36m_end":"2013-11-01T00:00:00Z","worst_36m_realized_r":"-2.0","worst_36m_start":"2010-11-01T00:00:00Z","zero_entry_12m_windows":"123","zero_entry_24m_windows":"66","zero_entry_36m_windows":"31","zero_entry_complete_years":"10","zero_entry_months":"245"},{"accepted_ledger_sha256":"0687cbb3be44548b4665069a3d40505863bf89223ecdd05f9773b1e979a37d3a","closed_trades":"19","config_id":"M_L100_D0025_B125_R150__H1_RELATIVE_VOLATILITY_GE_P1P00","cost_ticks":"20","dual_touch_closed":"0","eligible_isolated_signals":"19","entries_before_market_closure":"0","entry_next_open_gap_count":"13","execution_model":"STOP_FIRST_GAP_STRESS","expectancy_r":"0.8450600864814806","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"19","isolated_open":"0","isolated_profit_factor":"2.78401573812757","isolated_total_r":"16.05614164314813","leading_no_entry_days":"192.15625","losses":"9","max_closed_dd_r":"-3.0","max_consecutive_zero_entry_months":"66","max_holding_hours":"99.25","max_losing_streak":"3","maximum_inter_entry_gap_days":"2025.8020833333333","median_cost_fraction_reference_risk":"0.12499999999999642","median_holding_hours":"14.75","median_stop_pips":"16.00000000000046","negative_complete_entry_years":"3","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.20000000000002205","positive_complete_entry_years":"8","profit_factor":"2.78401573812757","raw_signal_sha256":"ebf44e3e7cadacef4f480d694193c7a8309ffb3cb2d3c2e3e6d0ddcf420cd045","raw_signals":"19","total_r":"16.05614164314813","trailing_no_entry_days":"1083.65625","win_rate_pct":"52.63157894736842","wins":"10","worst_12m_end":"2007-11-01T00:00:00Z","worst_12m_realized_r":"-1.0","worst_12m_start":"2006-11-01T00:00:00Z","worst_24m_end":"2012-11-01T00:00:00Z","worst_24m_realized_r":"-1.0","worst_24m_start":"2010-11-01T00:00:00Z","worst_36m_end":"2013-11-01T00:00:00Z","worst_36m_realized_r":"-2.0","worst_36m_start":"2010-11-01T00:00:00Z","zero_entry_12m_windows":"123","zero_entry_24m_windows":"66","zero_entry_36m_windows":"31","zero_entry_complete_years":"10","zero_entry_months":"245"},{"accepted_ledger_sha256":"e64d9784c3eeb55be52cc31df31e9e309715eaff6c561e948e38c20ff139d99d","closed_trades":"19","config_id":"M_L100_D0025_B125_R150__H1_RELATIVE_VOLATILITY_GE_P1P00","cost_ticks":"40","dual_touch_closed":"0","eligible_isolated_signals":"19","entries_before_market_closure":"0","entry_next_open_gap_count":"13","execution_model":"STOP_FIRST_GAP_STRESS","expectancy_r":"0.650529749558262","gap_stop_closed":"0","gap_target_closed":"0","geometry_invalid_all_signals":"0","invalid_unblocked_entries":"0","isolated_completed":"19","isolated_open":"0","isolated_profit_factor":"2.3733405824007754","isolated_total_r":"12.360065241606979","leading_no_entry_days":"192.15625","losses":"9","max_closed_dd_r":"-3.0","max_consecutive_zero_entry_months":"66","max_holding_hours":"99.25","max_losing_streak":"3","maximum_inter_entry_gap_days":"2025.8020833333333","median_cost_fraction_reference_risk":"0.24999999999999284","median_holding_hours":"14.75","median_stop_pips":"16.00000000000046","negative_complete_entry_years":"3","open_at_data_end":"0","p0_blocked_signals":"0","p90_cost_fraction_reference_risk":"0.4000000000000441","positive_complete_entry_years":"8","profit_factor":"2.3733405824007754","raw_signal_sha256":"ebf44e3e7cadacef4f480d694193c7a8309ffb3cb2d3c2e3e6d0ddcf420cd045","raw_signals":"19","total_r":"12.360065241606979","trailing_no_entry_days":"1083.65625","win_rate_pct":"52.63157894736842","wins":"10","worst_12m_end":"2007-11-01T00:00:00Z","worst_12m_realized_r":"-1.0","worst_12m_start":"2006-11-01T00:00:00Z","worst_24m_end":"2012-11-01T00:00:00Z","worst_24m_realized_r":"-1.0","worst_24m_start":"2010-11-01T00:00:00Z","worst_36m_end":"2013-11-01T00:00:00Z","worst_36m_realized_r":"-2.0","worst_36m_start":"2010-11-01T00:00:00Z","zero_entry_12m_windows":"123","zero_entry_24m_windows":"66","zero_entry_36m_windows":"31","zero_entry_complete_years":"10","zero_entry_months":"245"}]'
ARCHIVED_REFERENCE_SHA256='1999c7113743a4f610941ed171ebe3b9891dcb5fc4caec54c701eb9ca449bc2e'
PARITY_IDS=('RAW_EXACT_BULLISH_ENGULF', 'M_L200_D0050_B125_R150', 'M_L200_D0050_B125_R150__AVAILABLE_H1_RELATIVE_VOLATILITY', 'M_L200_D0050_B125_R150__H1_RELATIVE_VOLATILITY_GE_P1P00', 'M_L100_D0025_B125_R150', 'M_L100_D0025_B125_R150__AVAILABLE_H1_RELATIVE_VOLATILITY', 'M_L100_D0025_B125_R150__H1_RELATIVE_VOLATILITY_GE_P1P00')


def iso(value):
    return value.astimezone(UTC).isoformat().replace('+00:00', 'Z')

def when(value):
    return value if isinstance(value, datetime) else datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(UTC)

def sha(data):
    return hashlib.sha256(data).hexdigest()

def file_sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024*1024), b''):
            digest.update(block)
    return digest.hexdigest()

def code_hash():
    return file_sha(Path(__file__))

def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n', encoding='utf-8')

class CSVFile:
    """Streaming outputs keep memory bounded for full configuration ledgers."""
    def __init__(self, path, fields):
        self.file = path.open('w', newline='', encoding='utf-8')
        self.writer = csv.DictWriter(self.file, fieldnames=fields, extrasaction='raise')
        self.writer.writeheader()
        self.count = 0

    def add(self, row):
        self.writer.writerow(row)
        self.count += 1

    def close(self):
        self.file.close()

def write_csv(path, rows, fields=None):
    """Stream rows; never materialize the full multi-decade source or ledgers."""
    iterator = iter(rows)
    first = next(iterator, None)
    if fields is None:
        fields = list(first) if first is not None else []
    sink = CSVFile(path, fields)
    try:
        if first is not None:
            sink.add(first)
        for row in iterator:
            sink.add(row)
    finally:
        sink.close()
    return sink.count

def set_status(**values):
    with LOCK:
        STATUS.update(values, updated_at=iso(datetime.now(UTC)))
        if RUN_CLOCK is not None:
            STATUS['elapsed_seconds'] = round(time.monotonic() - RUN_CLOCK, 1)
        OUT.mkdir(parents=True, exist_ok=True)
        pending = OUT / f'.status-{os.getpid()}.tmp'
        write_json(pending, STATUS)
        os.replace(pending, OUT / 'status.json')
    print(json.dumps({k: STATUS[k] for k in ('state', 'progress', 'message')}), flush=True)

def read_status():
    try:
        data = json.loads((OUT / 'status.json').read_text())
        if data.get('version') == VERSION:
            return data
    except (OSError, ValueError):
        pass
    with LOCK:
        return dict(STATUS)

def atr14(bars):
    out = [None] * len(bars)
    tr = [None] * len(bars)
    for i in range(1, len(bars)):
        tr[i] = max(bars[i][2] - bars[i][3], abs(bars[i][2] - bars[i-1][4]), abs(bars[i][3] - bars[i-1][4]))
    if len(bars) > 14:
        out[14] = sum(tr[1:15]) / 14
        for i in range(15, len(bars)):
            out[i] = (out[i-1] * 13 + tr[i]) / 14
    return out

def previous_high(highs, lookback):
    """Monotonic queue; excludes the current signal candle."""
    out, queue = [None] * len(highs), deque()
    for i in range(len(highs)):
        while queue and queue[0] < i - lookback:
            queue.popleft()
        if i >= lookback:
            out[i] = highs[queue[0]]
        while queue and highs[queue[-1]] <= highs[i]:
            queue.pop()
        queue.append(i)
    return out

def replay(indices, paths, model, cost):
    """p0 per exact geometry and assumed cost, including unresolved positions."""
    accepted, invalid, blocked = [], [], 0
    occupied_until = -1
    for i in indices:
        if i < occupied_until:
            blocked += 1
            continue
        path = paths[i][model]
        _, problem = geometry(path, cost)
        if problem:
            invalid.append((i, problem))
            continue
        accepted.append(i)
        occupied_until = path['exit_index'] if path['exit_index'] is not None else math.inf
    return accepted, invalid, blocked

def source_hash(bars):
    return sha('\n'.join(f'{iso(t)},{op:.5f},{hi:.5f},{lo:.5f},{cl:.5f}' for t,op,hi,lo,cl in bars).encode())

def signal_hash(indices, bars):
    return sha('\n'.join(iso(bars[i][0]) for i in indices).encode())

def validate_bars(bars, step=BAR):
    if not bars:
        raise RuntimeError('Empty dataset.')
    seconds=int(step.total_seconds())
    for i,(t,op,hi,lo,cl) in enumerate(bars):
        if t.tzinfo is None or t.second or t.microsecond or int(t.timestamp()) % seconds:
            raise RuntimeError('Candle timestamp is not aligned to the requested granularity.')
        if not all(math.isfinite(x) and x > 0 for x in (op,hi,lo,cl)) or not lo <= min(op,cl) <= max(op,cl) <= hi:
            raise RuntimeError('Invalid OHLC at '+iso(t))
        if i and t <= bars[i-1][0]:
            raise RuntimeError('Unsorted or duplicate candle timestamp.')
        if not START <= t < END:
            raise RuntimeError('Candle outside the frozen study window.')

def crosscheck_history(work, bars, volumes, h1, h1_volumes):
    """All observed H1 bars must match grouped native M15 OHLC and price counts.

    A sparse M15 hour is accepted only when its observed bars exhaust the H1
    price count and reproduce its OHLC. No missing candle is manufactured.
    """
    grouped=defaultdict(list)
    for bar in bars:
        grouped[bar[0].replace(minute=0)].append(bar)
    controls=[]
    def check(name,actual,expected):
        controls.append(dict(check=name,status='PASS' if actual==expected else 'FAIL',actual=actual,expected=expected))
    old=[bar for bar in h1 if bar[0]<PINNED_H1_END]
    check('historical_H1_count_matches_prior_research',len(old),PINNED_H1_COUNT)
    check('historical_H1_OHLC_hash_matches_prior_research',source_hash(old),PINNED_H1_SHA256)
    h1_times={b[0] for b in h1}
    check('native_M15_hour_set_equals_H1_hour_set',set(grouped)==h1_times,True)
    mismatch,sparse=0,0
    out=CSVFile(work/'h1_m15_crosschecks.csv',['hour','m15_bars','m15_price_count','h1_price_count','max_ohlc_error_ticks','status'])
    try:
        for t,op,hi,lo,cl in h1:
            rows=grouped.get(t,[])
            if not rows:
                count,error=0,None
                okay=False
            else:
                aggregated=(rows[0][1],max(r[2] for r in rows),min(r[3] for r in rows),rows[-1][4])
                error=max(abs(x-y)/TICK for x,y in zip(aggregated,(op,hi,lo,cl)))
                count=sum(volumes[r[0]] for r in rows)
                okay=len(rows)<=4 and error<=1e-6 and count==h1_volumes[t]
            mismatch+=not okay
            sparse+=bool(rows) and len(rows)<4
            out.add(dict(hour=iso(t),m15_bars=len(rows),m15_price_count=count,h1_price_count=h1_volumes[t],
                         max_ohlc_error_ticks=error,status='PASS_NATIVE_SPARSE_PRICE_INTERVALS' if okay and len(rows)<4 else ('PASS' if okay else 'FAIL')))
    finally:
        out.close()
    check('all_H1_native_M15_aggregation_mismatches',mismatch,0)
    check('first_M15_in_first_requested_week',START <= bars[0][0]<START+timedelta(days=7),True)
    check('last_M15_in_final_requested_day',END-timedelta(days=1)<=bars[-1][0]<END,True)
    annual=[]
    for y in range(START.year,END.year+1):
        subset=[b for b in bars if b[0].year==y]
        annual.append(dict(year=y,candles=len(subset),first=iso(subset[0][0]) if subset else None,
                           last=iso(subset[-1][0]) if subset else None,partial_year=int(y==END.year),
                           first_month=subset[0][0].month if subset else None,last_month=subset[-1][0].month if subset else None))
    write_csv(work/'yearly_data_coverage.csv',annual)
    check('every_requested_year_contains_candles',all(r['candles']>0 for r in annual),True)
    check('complete_years_reach_January_and_December',all(r['first_month']==1 and r['last_month']==12 for r in annual if not r['partial_year']),True)
    write_csv(work/'source_controls.csv',controls)
    write_json(work/'coverage_crosscheck_summary.json',dict(h1_hours=len(h1),native_m15_candles=len(bars),
               aggregation_mismatches=mismatch,sparse_hours_verified_by_price_count=sparse,synthetic_candles=0,
               h1_source_sha256=source_hash(h1),native_m15_source_sha256=source_hash(bars)))
    if any(row['status']!='PASS' for row in controls):
        raise RuntimeError('Full-history source/aggregation controls failed; inspect source_controls.csv and crosschecks. No discovery results are valid.')
    return controls

def stats(values):
    values = list(values)
    wins = [x for x in values if x > 0]
    losses = [x for x in values if x < 0]
    total = peak = dd = 0.
    streak = worst_streak = 0
    for x in values:
        total += x
        peak = max(peak,total)
        dd = min(dd,total-peak)
        streak = streak+1 if x < 0 else 0
        worst_streak = max(worst_streak,streak)
    return dict(closed_trades=len(values), wins=len(wins), losses=len(losses),
                win_rate_pct=100*len(wins)/len(values) if values else None,
                total_r=total, expectancy_r=total/len(values) if values else None,
                profit_factor=sum(wins)/-sum(losses) if losses else None,
                max_closed_dd_r=dd, max_losing_streak=worst_streak)

def quantile(values, fraction):
    if not values:
        return None
    values = sorted(values)
    index = (len(values)-1)*fraction
    lo, hi = math.floor(index), math.ceil(index)
    return values[lo] + (values[hi]-values[lo])*(index-lo)

def month_bounds():
    result = []
    year, month = START.year, START.month
    while datetime(year,month,1,tzinfo=UTC) <= END:
        result.append(datetime(year,month,1,tzinfo=UTC))
        year,month = (year+1,1) if month==12 else (year,month+1)
    if result[-1] < END:
        result.append(END)  # explicit partial final month, never relabelled a full month
    return result

def period_definitions():
    periods = []
    for year in range(2005,2027):
        periods.append((f'YEAR_{year}', 'calendar', datetime(year,1,1,tzinfo=UTC), min(datetime(year+1,1,1,tzinfo=UTC),END)))
    for a,b in ((2005,2010),(2010,2016),(2016,2022),(2022,2027)):
        periods.append((f'ERA_{a}_{b-1}', 'era', datetime(a,1,1,tzinfo=UTC),min(datetime(b,1,1,tzinfo=UTC),END)))
    for years in (1,2,3,5,10):
        periods.append((f'LATEST_{years}Y', 'recent', END.replace(year=END.year-years), END))
    # Calendar-day boundaries are diagnostic labels, not intraday announcement times.
    periods.extend([
        ('CHF_PRE_FLOOR','policy_diagnostic',START,datetime(2011,9,6,tzinfo=UTC)),
        ('CHF_FLOOR_PERIOD','policy_diagnostic',datetime(2011,9,6,tzinfo=UTC),datetime(2015,1,15,tzinfo=UTC)),
        ('CHF_2015_01_15','policy_diagnostic',datetime(2015,1,15,tzinfo=UTC),datetime(2015,1,16,tzinfo=UTC)),
        ('CHF_AFTER_2015_01_15','policy_diagnostic',datetime(2015,1,16,tzinfo=UTC),END),
    ])
    return periods

def package(work, completed):
    """Atomic publication. An error archive cannot include partial performance."""
    allowed_on_error = {'archived_control_parity.csv','archived_control_reference.json','baseline_parity.csv','protocol.md','run_manifest.json','error_report.csv','hard_controls.csv',
                        'software_checks.csv','coverage.csv','data_gaps.csv','runner_source.py',
                        'source_controls.csv','h1_m15_crosschecks.csv','yearly_data_coverage.csv',
                        'coverage_crosscheck_summary.json','archived_pass1_fetch_receipts.csv','parent_pass1_parity.csv','parent_pass1_provenance.json','conditional_feature_controls.csv','archived_anchor_controls.csv','archived_anchor_reference.json'}
    members = [p for p in sorted(work.iterdir()) if p.is_file() and p.name != 'file_manifest.json' and
               (completed or p.name in allowed_on_error)]
    files = [dict(file=p.name,bytes=p.stat().st_size,sha256=file_sha(p)) for p in members]
    write_json(work/'file_manifest.json',files)
    temporary = OUT / (RESULT_NAME+'.tmp')
    with zipfile.ZipFile(temporary,'w',zipfile.ZIP_DEFLATED,compresslevel=6,allowZip64=True) as z:
        for p in members+[work/'file_manifest.json']:
            z.write(p,p.name)
    os.replace(temporary,OUT/RESULT_NAME)

def launch(background=True):
    global STARTED, JOB_LOCK
    with LOCK:
        if STARTED:
            return False
        OUT.mkdir(parents=True,exist_ok=True)
        # Linux Railway/Gunicorn: avoid two workers writing one result archive.
        if os.name == 'posix':
            import fcntl
            handle = (OUT/'run.lock').open('a+')
            try:
                fcntl.flock(handle,fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                handle.close()
                STARTED = True
                return False
            JOB_LOCK = handle
        STARTED = True
        # Reuse an exact-version complete result across multiple WSGI workers.
        previous = read_status()
        if previous.get('state')=='complete' and previous.get('runner_sha256')==code_hash() and (OUT/RESULT_NAME).exists():
            if JOB_LOCK is not None:
                JOB_LOCK.close()
                JOB_LOCK = None
            return False
        set_status(state='starting',progress=0,message='Starting EUR/CHF M15 long Pass4 failed-breakdown/reclaim research',runner_sha256=code_hash(),result_path=None)
    if background:
        threading.Thread(target=run_job,name='eurchf-m15-long-pass4-research',daemon=True).start()
        return True
    return run_job()

def app(environ, start_response):
    """Dependency-free WSGI app, including status while the worker is busy."""
    path = environ.get('PATH_INFO','/')
    if path.startswith(PREFIX):
        path = path[len(PREFIX):] or '/'
    method = environ.get('REQUEST_METHOD','GET')
    if method not in ('GET','HEAD'):
        body = b'{"error":"GET or HEAD only; no order endpoints"}'
        start_response('405 Method Not Allowed',[('Content-Type','application/json'),('Content-Length',str(len(body)))])
        return [] if method == 'HEAD' else [body]
    if path not in ('/','/health','/start','/status','/results'):
        body = b'{"error":"Not found"}'
        start_response('404 Not Found',[('Content-Type','application/json'),('Content-Length',str(len(body)))])
        return [] if method == 'HEAD' else [body]
    if path=='/start' or (not STARTED and os.getenv('EURCHF_M15_LONG_PASS2B_AUTOSTART','1')=='1'):
        launch()
    current = read_status()
    code = '200 OK'
    if path=='/results':
        bundle = OUT/RESULT_NAME
        if current.get('state') in ('complete','error') and current.get('result_path') and bundle.exists():
            start_response(code,[('Content-Type','application/zip'),('Content-Length',str(bundle.stat().st_size)),
                                 ('Content-Disposition',f'attachment; filename="{RESULT_NAME}"'),('Cache-Control','no-store')])
            if method=='HEAD':
                return []
            def chunks():
                with bundle.open('rb') as stream:
                    while True:
                        block = stream.read(1024*1024)
                        if not block:
                            break
                        yield block
            return chunks()
        current = dict(error='Results are not ready',**current)
        code = '409 Conflict'
    elif path=='/health':
        current = dict(ok=True,state=current['state'],version=VERSION,orders_supported=False)
    elif path=='/':
        current = dict(service='EUR/CHF M15 LONG Pass4 failed-breakdown/reclaim research',version=VERSION,status='/status',results='/results',
                       start='/start',research_state=current['state'],orders_supported=False,trading_enabled=False)
    body = (json.dumps(current,allow_nan=False)+'\n').encode()
    start_response(code,[('Content-Type','application/json'),('Content-Length',str(len(body))),('Cache-Control','no-store')])
    return [] if method=='HEAD' else [body]

class ThreadedWSGIServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--run',action='store_true',help='Run the historical study once without an HTTP server')
    group.add_argument('--self-test',action='store_true',help='Synthetic mathematical/execution checks only; no OANDA request')
    args = parser.parse_args()
    if args.self_test:
        print(json.dumps(dict(version=VERSION,market_data_used=False,checks=self_checks()),indent=2))
        return 0
    if args.run:
        return 0 if launch(background=False) else (0 if read_status().get('state')=='complete' else 1)
    port = int(os.getenv('PORT','8080'))
    with make_server('0.0.0.0',port,app,server_class=ThreadedWSGIServer) as server:
        if os.getenv('EURCHF_M15_LONG_PASS2B_AUTOSTART','1')=='1':
            launch()
        print(f'{VERSION}: listening on {port}; /status and /results',flush=True)
        server.serve_forever()
    return 0

def load_frozen_history(work):
    if not FROZEN_SOURCE_FILE.is_file():
        raise RuntimeError('Missing EURCHF_M15_PASS1_FROZEN_DATA.zip. Upload this file alongside app.py in the same folder.')
    payload=FROZEN_SOURCE_FILE.read_bytes()
    if sha(payload)!=EMBEDDED_PAYLOAD_SHA256:
        raise RuntimeError('Frozen source archive SHA mismatch.')
    shutil.copyfile(FROZEN_SOURCE_FILE,work/'EURCHF_M15_PASS1_FROZEN_DATA.zip')
    with zipfile.ZipFile(io.BytesIO(payload)) as bundle:
        if set(bundle.namelist())!=set(EMBEDDED_MEMBER_PINS) or bundle.testzip() is not None:
            raise RuntimeError('Frozen source archive members/CRC mismatch.')
        for name,pin in EMBEDDED_MEMBER_PINS.items():
            data=bundle.read(name)
            if len(data)!=pin['bytes'] or sha(data)!=pin['sha256']:
                raise RuntimeError('Frozen source member SHA/size mismatch: '+name)
            destination='archived_pass1_fetch_receipts.csv' if name=='fetch_receipts.csv' else name
            if name in ('source_candles.csv','source_h1_crosscheck_candles.csv','fetch_receipts.csv'):
                (work/destination).write_bytes(data)
    def read_source(name,step):
        bars,volumes=[],{}
        with (work/name).open(newline='') as stream:
            for row in csv.DictReader(stream):
                t=when(row['time']);v=int(row['volume'])
                if v<0:raise RuntimeError('Negative archived price count.')
                bars.append((t,*[float(row[k]) for k in ('open','high','low','close')]))
                volumes[t]=v
        validate_bars(bars,step)
        return bars,volumes
    bars,volumes=read_source('source_candles.csv',BAR)
    h1,hvolumes=read_source('source_h1_crosscheck_candles.csv',H1_BAR)
    if len(bars)!=546647 or source_hash(bars)!='63eeea194c7106cfe0ebe248e96a392ab876611da5fc2fb569e3b2f304eaaa22':
        raise RuntimeError('Pinned Pass 1 M15 source mismatch.')
    if len(h1)!=137939 or source_hash(h1)!='b3b686ad3a92569d971d2d9412c8afe64a5ad8f54b5d097840c4a2d6cebbd2c0':
        raise RuntimeError('Pinned Pass 1 H1 source mismatch.')
    return bars,volumes,h1,hvolumes

def bullish_engulf(bars,i):
    if i<1:return False
    po,pc,op,cl=bars[i-1][1],bars[i-1][4],bars[i][1],bars[i][4]
    return pc<po and cl>op and op<=pc and cl>=po

def previous_low(lows,lookback):
    return [None if x is None else -x for x in previous_high([-x for x in lows],lookback)]

def find_paths(bars,i):
    reference=bars[i][4];stop=bars[i][3]-10*TICK;target=reference+RR*(reference-stop)
    common=dict(signal_index=i,signal=iso(bars[i][0]),entry=iso(bars[i][0]+BAR),
        reference_entry=reference,stop=stop,target=target,
        next_open=bars[i+1][1] if i+1<len(bars) else None,
        next_candle_delay_hours=(bars[i+1][0]-bars[i][0]-BAR).total_seconds()/3600 if i+1<len(bars) else None)
    for j in range(i+1,len(bars)):
        op,hi,lo=bars[j][1:4];hs,ht=lo<=stop,hi>=target
        if not hs and not ht:continue
        both=hs and ht
        reason=('TARGET' if hi-op<op-lo else 'STOP') if both else ('STOP' if hs else 'TARGET')
        sensitivity=dict(common,exit_index=j,exit=iso(bars[j][0]+BAR),exit_price=stop if reason=='STOP' else target,
            reason=reason,dual_touch=int(both),gap_stop=0,gap_target=0)
        if op<=stop:reason,price,xt=('STOP_GAP' if op<stop else 'STOP'),min(op,stop),bars[j][0]
        elif op>=target:reason,price,xt='TARGET_GAP_CAPPED',target,bars[j][0]
        elif hs:reason,price,xt='STOP',stop,bars[j][0]+BAR
        else:reason,price,xt='TARGET',target,bars[j][0]+BAR
        stress=dict(common,exit_index=j,exit=iso(xt),exit_price=price,reason=reason,dual_touch=int(both),
            gap_stop=int(op<stop),gap_target=int(op>=target))
        return {MODELS[0]:sensitivity,MODELS[1]:stress}
    opened=dict(common,exit_index=None,exit=None,exit_price=None,reason='OPEN_AT_DATA_END',dual_touch=0,gap_stop=0,gap_target=0)
    return {model:dict(opened) for model in MODELS}

def geometry(path,cost):
    fill=path['reference_entry']+cost*TICK
    if when(path['entry'])>=END:return None,'ENTRY_AT_OR_AFTER_CUTOFF'
    if not 0<path['stop']<fill<path['target']:return None,'INVALID_FILL_STOP_TARGET_GEOMETRY'
    return fill,None

def r_value(path,cost):
    fill,error=geometry(path,cost)
    if error or path['exit'] is None:return None
    if path['reason']=='STOP':return -1.
    return (path['exit_price']-fill)/(fill-path['stop'])

def base_features(bars):
    atr=atr14(bars);prior={lb:previous_low([b[3] for b in bars],lb) for lb in LOOKBACKS}
    engulf={};reclaim={}
    for i in range(WARMUP,len(bars)):
        t,op,hi,lo,cl=bars[i]
        if atr[i] is None or atr[i]<=0:continue
        bull=cl>op and hi>lo
        breached=[lb for lb in (60,120,200) if bull and lo<prior[lb][i]<cl]
        is_engulf=bullish_engulf(bars,i)
        if not is_engulf and not breached:continue
        row=dict(signal_index=i,signal=iso(t),entry=iso(t+BAR),open=op,high=hi,low=lo,close=cl,
            atr14=atr[i],body_atr=(cl-op)/atr[i],range_atr=(hi-lo)/atr[i],
            close_location=(cl-lo)/(hi-lo) if hi>lo else None,
            reference_stop_pips=(cl-lo+10*TICK)/PIP)
        for lb in LOOKBACKS:
            row[f'previous_low_{lb}']=prior[lb][i]
            row[f'abs_distance_atr_{lb}']=abs(lo-prior[lb][i])/atr[i]
        for lb in (60,120,200):
            row[f'reclaim_{lb}']=int(lb in breached)
            row[f'breach_depth_atr_{lb}']=(prior[lb][i]-lo)/atr[i]
            row[f'reclaim_above_low_atr_{lb}']=(cl-prior[lb][i])/atr[i]
        if is_engulf:engulf[i]=dict(row)
        if breached:reclaim[i]=dict(row)
    return {'LONG_SIGNAL_LOW':engulf,'RECLAIM_SIGNAL_LOW':reclaim}


def base_selected(config,features):
    result=[]
    for i,row in features[config['geometry_key']].items():
        if row['body_atr']<config['body_min_atr'] or row['range_atr']<config['range_min_atr']:continue
        if config['geometry_key']=='RECLAIM_SIGNAL_LOW':
            if not row[f"reclaim_{config['lookback']}"] or row['close_location']<config['close_min']:continue
        elif config['distance_max_atr'] is not None and row[f"abs_distance_atr_{config['lookback']}"]>config['distance_max_atr']:continue
        result.append(i)
    return result


def build_paths(bars,features,configs):
    cache={};result={}
    for geo in dict.fromkeys(c['geometry_key'] for c in configs):
        result[geo]={}
        for i in features[geo]:
            if i not in cache:cache[i]=find_paths(bars,i)
            result[geo][i]=cache[i]
    return result

def hard_controls(bars,features,configs):
    checks=[]
    def check(name,ok):checks.append(dict(check=name,status='PASS' if ok else 'FAIL'))
    check('34_unique_definitions_24_research_10_controls',len(configs)==34 and len({c['config_id'] for c in configs})==34 and sum(c['role']=='RESEARCH' for c in configs)==24)
    ref=[None]*len(bars)
    if len(bars)>14:
        trs=[max(b[2]-b[3],abs(b[2]-a[4]),abs(b[3]-a[4])) for a,b in zip(bars,bars[1:])]
        ref[14]=math.fsum(trs[:14])/14
        for i in range(15,len(bars)):ref[i]=ref[i-1]+(trs[i-1]-ref[i-1])/14
    check('independent_full_ATR',all((a is None and b is None) or (a is not None and b is not None and abs(a-b)<1e-12) for a,b in zip(ref,atr14(bars))))
    direct={'LONG_SIGNAL_LOW':{},'RECLAIM_SIGNAL_LOW':{}}
    for i in range(WARMUP,len(bars)):
        _,op,hi,lo,cl=bars[i]
        if ref[i] is None or ref[i]<=0:continue
        engulf=bars[i-1][4]<bars[i-1][1] and cl>op and op<=bars[i-1][4] and cl>=bars[i-1][1]
        # Direct slices deliberately avoid the production rolling-min helper.
        lows={lb:min(b[3] for b in bars[i-lb:i]) for lb in LOOKBACKS} if engulf or cl>op else {}
        breaks=[lb for lb in (60,120,200) if cl>op and hi>lo and lo<lows[lb]<cl]
        if not engulf and not breaks:continue
        r=dict(body_atr=(cl-op)/ref[i],range_atr=(hi-lo)/ref[i],close_location=(cl-lo)/(hi-lo) if hi>lo else None)
        for lb in LOOKBACKS:r[f'previous_low_{lb}']=lows[lb];r[f'abs_distance_atr_{lb}']=abs(lo-lows[lb])/ref[i]
        for lb in (60,120,200):
            r[f'reclaim_{lb}']=int(lb in breaks)
            r[f'breach_depth_atr_{lb}']=(lows[lb]-lo)/ref[i]
            r[f'reclaim_above_low_atr_{lb}']=(cl-lows[lb])/ref[i]
        if engulf:direct['LONG_SIGNAL_LOW'][i]=r
        if breaks:direct['RECLAIM_SIGNAL_LOW'][i]=r
    for geo,rows in direct.items():
        check(geo+'_independent_universe',list(rows)==list(features[geo]))
        check(geo+'_direct_features',all(all((v is None and features[geo][i][k] is None) or (v is not None and abs(v-features[geo][i][k])<1e-9) for k,v in r.items()) for i,r in rows.items()))
    for c in configs:
        expected=[]
        for i,r in direct[c['geometry_key']].items():
            if r['body_atr']<c['body_min_atr'] or r['range_atr']<c['range_min_atr']:continue
            if c['geometry_key']=='RECLAIM_SIGNAL_LOW':
                if not r[f"reclaim_{c['lookback']}"] or r['close_location']<c['close_min']:continue
            elif c['distance_max_atr'] is not None and r[f"abs_distance_atr_{c['lookback']}"]>c['distance_max_atr']:continue
            expected.append(i)
        check(c['config_id']+'_independent_base_mask',base_selected(c,features)==expected)
    n=min(10000,len(bars));prefix=base_features(bars[:n])
    for geo in direct:
        check(geo+'_prefix_causality',all(prefix[geo][i]=={k:features[geo][i][k] for k in prefix[geo][i]} for i in prefix[geo]) and list(prefix[geo])==[i for i in features[geo] if i<n])
    return checks



def baseline_parity(work,bars,features,path_sets):
    """Independent raw-control chronology/path implementation; no production helpers.
    All fields of accepted paths and fills/R are compared across six cases.
    """
    ids=list(features['LONG_SIGNAL_LOW']);production=path_sets['LONG_SIGNAL_LOW'];reference={}
    for i in ids:
        close=bars[i][4];stop=bars[i][3]-.00010;target=close+3*(close-stop)
        for model in MODELS:
            row=dict(exit_index=None,exit=None,exit_price=None,reason='OPEN_AT_DATA_END',dual_touch=0,gap_stop=0,gap_target=0)
            for j in range(i+1,len(bars)):
                stamp,o,h,l,_=bars[j];hit_s=l<=stop;hit_t=h>=target
                if not(hit_s or hit_t):continue
                row['exit_index']=j;row['dual_touch']=int(hit_s and hit_t)
                if model=='NEAREST_OPEN_SENSITIVITY':
                    loss=hit_s and (not hit_t or o-l<=h-o)
                    row.update(exit=iso(stamp+BAR),exit_price=stop if loss else target,reason='STOP' if loss else 'TARGET')
                elif o<=stop:
                    row.update(exit=iso(stamp),exit_price=o,reason='STOP_GAP' if o<stop else 'STOP',gap_stop=int(o<stop))
                elif o>=target:row.update(exit=iso(stamp),exit_price=target,reason='TARGET_GAP_CAPPED',gap_target=1)
                else:row.update(exit=iso(stamp+BAR),exit_price=stop if hit_s else target,reason='STOP' if hit_s else 'TARGET')
                break
            row.update(signal_index=i,signal=iso(bars[i][0]),entry=iso(bars[i][0]+BAR),reference_entry=close,stop=stop,target=target,
                next_open=bars[i+1][1] if i+1<len(bars) else None,
                next_candle_delay_hours=(bars[i+1][0]-bars[i][0]-BAR).total_seconds()/3600 if i+1<len(bars) else None)
            reference[i,model]=row
    checks=[]
    for model in MODELS:
        for cost in COSTS:
            accepted=[];invalid=[];occupied=-1;mismatch=0
            for i in ids:
                if i<occupied:continue
                p=reference[i,model];fill=p['reference_entry']+cost*.00001
                if when(p['entry'])>=END:invalid.append((i,'ENTRY_AT_OR_AFTER_CUTOFF'));continue
                if not 0<p['stop']<fill<p['target']:invalid.append((i,'INVALID_FILL_STOP_TARGET_GEOMETRY'));continue
                accepted.append(i);occupied=p['exit_index'] if p['exit_index'] is not None else math.inf
                q=production[i][model]
                for k,v in p.items():
                    other=q[k]
                    mismatch+=not (math.isclose(v,other,rel_tol=0,abs_tol=1e-12) if isinstance(v,float) and isinstance(other,(int,float)) else v==other)
                rr=None if p['exit'] is None else (p['exit_price']-fill)/(fill-p['stop'])
                got=r_value(q,cost);mismatch+=not((got is None and rr is None) or (got is not None and rr is not None and abs(got-rr)<=1e-11))
            got,wrong,_=replay(ids,production,model,cost)
            checks.append(dict(config_id=PARENT_IDS[0],execution_model=model,cost_ticks=cost,
                accepted_entries=len(accepted),field_mismatches=mismatch,status='PASS' if got==accepted and wrong==invalid and mismatch==0 else 'FAIL'))
    write_csv(work/'baseline_parity.csv',checks)
    if any(c['status']!='PASS' for c in checks):raise RuntimeError('Independent raw-control full-ledger parity failed')
    return checks

def write_inputs(work,bars,features,path_sets,configs,memberships,dataset_kind,volumes=None):
    write_csv(work/'configuration_grid.csv',configs);write_csv(work/'research_case_roles.csv',memberships)
    write_csv(work/'period_definitions.csv',(dict(period=a,period_type=k,start=iso(s),end=iso(e)) for a,k,s,e in PERIODS))
    write_csv(work/'coverage.csv',[dict(dataset_kind=dataset_kind,source_candles=len(bars),first=iso(bars[0][0]),last=iso(bars[-1][0]),
        requested_start=iso(START),requested_end=iso(END),source_sha256=source_hash(bars))])
    write_csv(work/'data_gaps.csv',(dict(previous=iso(a[0]),next=iso(b[0]),elapsed_minutes=(b[0]-a[0]).total_seconds()/60,
        absent_intervals=round((b[0]-a[0]).total_seconds()/900)-1,interpretation='Observed gap; no invented bars/exclusions') for a,b in zip(bars,bars[1:]) if b[0]-a[0]!=BAR),
        ['previous','next','elapsed_minutes','absent_intervals','interpretation'])
    write_csv(work/'source_atr14.csv',(dict(signal_index=i,time=iso(bars[i][0]),atr14=x) for i,x in enumerate(atr14(bars))))
    fields=['signal_index','signal','entry','open','high','low','close','atr14','body_atr','range_atr','reference_stop_pips']
    fields += [k for lb in LOOKBACKS for k in (f'previous_low_{lb}',f'abs_distance_atr_{lb}')]
    write_csv(work/'raw_bullish_engulf_features.csv',({k:r[k] for k in fields} for r in features['LONG_SIGNAL_LOW'].values()),fields)
    write_csv(work/'raw_reclaim_features.csv',features['RECLAIM_SIGNAL_LOW'].values())
    fields=['geometry_key','execution_model','signal_index','signal','entry','reference_entry','stop','target','next_open',
        'next_candle_delay_hours','exit_index','exit','exit_price','reason','dual_touch','gap_stop','gap_target']
    write_csv(work/'signal_trade_paths.csv',(dict(geometry_key=g,execution_model=m,**p) for g,paths in path_sets.items() for v in paths.values() for m,p in v.items()),fields)


def write_neighbours(work,configs,summaries):
    rows=[]
    for c in configs:
        if c['role']!='RESEARCH':continue
        neighbours,edges=neighbour_ids(c,configs)
        for model in MODELS:
            for cost in COSTS:
                ours=summaries[c['config_id'],model,cost];other=[summaries[n,model,cost] for n in neighbours]
                rows.append(dict(config_id=c['config_id'],family=c['family'],execution_model=model,cost_ticks=cost,tested_boundaries=';'.join(edges),
                    adjacent_configurations=len(other),distinct_neighbour_raw_streams=len({r['raw_signal_sha256'] for r in other}),
                    neighbours_identical_to_this_raw_stream=sum(r['raw_signal_sha256']==ours['raw_signal_sha256'] for r in other),
                    positive_total_r_neighbours=sum(r['total_r']>0 for r in other),
                    distinct_positive_neighbour_raw_streams=len({r['raw_signal_sha256'] for r in other if r['total_r']>0}),
                    median_neighbour_total_r=quantile([r['total_r'] for r in other],.5),minimum_neighbour_total_r=min((r['total_r'] for r in other),default=None),
                    minimum_neighbour_closed_trades=min((r['closed_trades'] for r in other),default=None),neighbour_ids=';'.join(neighbours)))
    write_csv(work/'neighbourhood_summary.csv',rows)

def self_checks():
    checks=[]
    def check(ok,name):
        if not ok:raise AssertionError(name)
        checks.append(dict(check=name,status='PASS',evidence='SYNTHETIC_SOFTWARE_ONLY'))
    configs,_=make_configs();check(len(configs)==34 and len({c['config_id'] for c in configs})==34,'Full predeclared grid')
    check(len(BOUNDS)==263 and len(PERIODS)==35,'Zero-inclusive time universe')
    t=datetime(2020,1,1,tzinfo=UTC)
    pattern=[(t,1.001,1.0012,.9998,1.),(t+BAR,1.,1.0012,.9997,1.001)]
    check(bullish_engulf(pattern,1),'Exact bullish boundary equalities accepted')
    doji=pattern.copy();doji[0]=(t,1.,1.0012,.9998,1.);check(not bullish_engulf(doji,1),'Previous doji rejected')
    doji=pattern.copy();doji[1]=(t+BAR,1.,1.0012,.9997,1.);check(not bullish_engulf(doji,1),'Signal doji rejected')
    check(previous_low([3.,1.,2.,-100.,4.,5.],3)==[None,None,None,1.,-100.,-100.],'Prior low excludes current candle')
    bars=[(t+i*BAR,1.,1.001,.999,1.) for i in range(204)]
    bars[199]=(t+199*BAR,1.001,1.0012,.9998,1.)
    bars[200]=(t+200*BAR,1.,1.0013,.9989,1.0011)
    f=make_features(bars);check(200 in f['LONG_SIGNAL_LOW'],'Warmup and bullish feature')
    check(f['LONG_SIGNAL_LOW'][200]['previous_low_100']==.999,'Prior structure excludes signal low')
    row=dict(f['LONG_SIGNAL_LOW'][200]);row.update(body_atr=1.25,range_atr=1.5);row['abs_distance_atr_200']=.05
    c=next(c for c in configs if c['family']=='ANCHOR' and c['lookback']==200)
    fixture={'LONG_SIGNAL_LOW':{200:row}};check(selected_indices(c,fixture)==[200],'Inclusive structure/body/range thresholds')
    row['abs_distance_atr_200']=.050001;check(not selected_indices(c,fixture),'Structure above maximum rejected')
    dual=[(t,10.,10.5,9.,10.),(t+BAR,12.9,14.,8.,12.)]
    paths=find_paths(dual,0);check(paths[MODELS[0]]['reason']=='TARGET' and paths[MODELS[1]]['reason']=='STOP','BUY dual-touch alternate/stop-first')
    p=find_paths([(t,10.,10.5,9.,10.),(t+BAR,8.,8.5,7.5,8.2)],0)[MODELS[1]]
    check(p['exit_price']==8. and r_value(p,10)<-1 and p['exit']==iso(t+BAR),'BUY adverse stop gap and open timestamp')
    p=find_paths([(t,10.,10.5,9.,10.),(t+BAR,14.,14.5,13.5,14.1)],0)[MODELS[1]]
    check(p['exit_price']==p['target'] and p['gap_target']==1,'BUY favourable target gap capped')
    fixture=dict(signal_index=0,signal=iso(t),entry=iso(t+BAR),reference_entry=1.,stop=.999,target=1.003,
        exit_index=None,exit=None,exit_price=None,reason='OPEN_AT_DATA_END',dual_touch=0,gap_stop=0,gap_target=0)
    check(geometry(fixture,20)[0]==1.0002,'BUY adverse cost adds to fill')
    normal=dict(fixture,exit=iso(t+2*BAR),exit_price=1.003,reason='TARGET')
    check(abs(r_value(normal,20)-(1.003-1.0002)/(1.0002-.999))<1e-12,'BUY reward and fill-based denominator')
    mapping={i:{m:dict(fixture,signal_index=i) for m in MODELS} for i in (0,1,2)}
    check(replay([0,1,2],mapping,MODELS[0],10)[0]==[0],'Open trade blocks to cutoff')
    mapping[0][MODELS[0]].update(exit_index=1,exit=iso(t+2*BAR),exit_price=.999,reason='STOP')
    check(replay([0,1,2],mapping,MODELS[0],10)[0]==[0,1],'Exit-candle entry allowed')
    mapping[0][MODELS[0]]['target']=1.0002;check(replay([0,1,2],mapping,MODELS[0],40)[0]==[1],'Invalid BUY geometry does not occupy')
    check(geometry(dict(fixture,entry=iso(END)),10)[1]=='ENTRY_AT_OR_AFTER_CUTOFF','Exclusive entry cutoff')
    check(r_value(fixture,10) is None,'Open trade has no invented R')
    check(stats([])['total_r']==0 and stats([2.,-1.,-1.,-1.,2.])['max_closed_dd_r']==-3.,'Empty stats and chronological DD')
    check(rolling_reference([None,2.,4.,6.,8.],2)==[None,None,None,3.,5.],'Trailing ATR reference excludes tested value')
    for op,v,t,want in [('GE',.5,.5,True),('LE',.5,.5,True),('LT',0.,0.,False),('GT',0.,0.,False),('AVAILABLE',None,None,False)]:
        check(condition_pass(dict(feature='x',operator=op,threshold=t),{'x':v})==want,'Condition boundary '+op)
    check(not condition_pass(dict(feature='x',operator='GE',threshold=0.),{'x':float('nan')}),'Nonfinite feature unavailable')
    check(ema_seeded([1.,2.,3.,4.],3)==[None,None,2.,3.],'Daily EMA period-mean seed')
    cutoff=bars[200][0]+BAR
    context=build_context(bars)
    row=context_for_signal(context,200)
    check(all(row[k] is None or when(row[k])<=cutoff for k in ('h1_context_close','d1_context_close')),'Only closed higher-timeframe context')
    check(len(PARITY_IDS)==7 and len(json.loads(ARCHIVED_REFERENCE_JSON))==42,'Seven frozen controls /42 cases')
    check(sum(c['role']=='RESEARCH' for c in configs)==24,'24 new research definitions')
    t=bars[0][0]
    rc=next(c for c in configs if c['family']=='RECLAIM_RAW' and c['lookback']==200)
    bars[199]=(t+199*BAR,1.,1.0012,.9998,1.0001) # preceding bullish: no engulf required
    f=base_features(bars)
    check(200 in selected_indices(rc,f) and 200 not in f['LONG_SIGNAL_LOW'],'Strict reclaim without engulf')
    for lo,cl,name in [(.999,1.0011,'Breach equality rejected'),(.9989,.999,'Reclaim equality rejected'),(.9989,1.,'Signal doji rejected')]:
        changed=list(bars);changed[200]=(t+200*BAR,1.,1.0013,lo,cl)
        check(200 not in selected_indices(rc,base_features(changed)),name)
    row=dict(f['RECLAIM_SIGNAL_LOW'][200]);row.update(body_atr=.5,close_location=.65)
    joint=next(c for c in configs if c['lookback']==200 and c['body_min_atr']==.5 and c['close_min']==.65)
    check(selected_indices(joint,{'RECLAIM_SIGNAL_LOW':{200:row}})==[200],'Inclusive reclaim quality thresholds')
    for k in ('body_atr','close_location'):
        q=dict(row);q[k]-=1e-6
        check(not selected_indices(joint,{'RECLAIM_SIGNAL_LOW':{200:q}}),'Below reclaim threshold '+k)
    middle=next(c for c in configs if c['geometry_key']=='RECLAIM_SIGNAL_LOW' and c['lookback']==120 and c['body_min_atr']==.5 and c['close_min']==.65)
    check(len(neighbour_ids(middle,configs)[0])==6,'Interior six adjacent one-axis neighbours')
    check(len(neighbour_ids(rc,configs)[0])==3,'Corner three neighbours')
    check(all(not c['feature'] and c['range_min_atr']==0. and c['distance_max_atr'] is None for c in configs if c['geometry_key']=='RECLAIM_SIGNAL_LOW'),'No inherited rescue filters')
    check(list(base_features(bars[:201])['RECLAIM_SIGNAL_LOW'])==[i for i in f['RECLAIM_SIGNAL_LOW'] if i<201],'Reclaim prefix stability')
    return checks



def analyze(work, bars, features, path_sets, configs, *, progress=True):
    """Pure reporting/replay layer. Production calls this only after hard gates."""
    sinks = {}
    definitions = {
        'summary.csv': SUMMARY_FIELDS,
        'raw_signal_membership.csv': ['config_id','signal_index'],
        'accepted_trades.csv': CASE+['accepted_sequence','signal_index','historical_fill','risk_price','r'],
        'invalid_entries.csv': CASE+['signal_index','reason'],
        'open_at_data_end.csv': CASE+['signal_index','entry','historical_fill','stop','target','held_hours_to_cutoff'],
        'control_accepted_ledgers.csv': CASE+['accepted_sequence','signal_index','exit_index','signal','entry','exit',
             'reference_entry','historical_fill','stop','target','risk_price','exit_price','reason','r'],
        'accepted_ledgers.csv': CASE+['accepted_sequence','signal_index','exit_index','signal','entry','exit',
             'reference_entry','historical_fill','stop','target','risk_price','exit_price','reason','r'],
        'period_results.csv': CASE+['period','period_type','start','end','partial_calendar_year','entry_count',
             'entry_cohort_completed','entry_cohort_eventual_r','entry_cohort_open',
             'exit_count','realized_r','realized_max_dd_r','open_at_period_end'],
        'monthly_results.csv': CASE+['month','start','end','partial_month','entry_count','exit_count','realized_r','entry_cohort_eventual_r','entry_cohort_open'],
        'rolling_windows.csv': CASE+['months','start','end','entry_count','exit_count','realized_r','zero_entry_window','zero_exit_window'],
        'accepted_comparisons.csv': CASE+['reference_config','common_entries','added_entries','removed_entries',
             'common_completed_r_candidate','common_completed_r_reference','added_completed_r','removed_completed_r',
             'candidate_total_r','reference_total_r','delta_total_r','candidate_open_count','reference_open_count'],
        'chf_event_exposure.csv': CASE+['signal_index','entry','exit','entered_on_event_day','exited_on_event_day',
             'held_across_event_day_start','eventual_r','outcome','attribution_warning'],
    }
    for name,fields in definitions.items():
        sinks[name] = CSVFile(work/name,fields)
    summaries = {}
    times_cache={geo:{(i,model):(when(p['entry']),when(p['exit']) if p['exit'] else None)
        for i,models in paths.items() for model,p in models.items()} for geo,paths in path_sets.items()}
    outcome_cache={geo:{(model,cost):{i:r_value(paths[i][model],cost) for i in paths}
        for model in MODELS for cost in COSTS} for geo,paths in path_sets.items()}
    refs = {}
    for config in configs:
        if config['config_id'] in COMPARISON_IDS:
            indices = selected_indices(config,features)
            paths=path_sets[config['geometry_key']]
            for model in MODELS:
                for cost in COSTS:
                    refs[(config['config_id'],model,cost)] = (replay(indices,paths,model,cost)[0], outcome_cache[config['geometry_key']][(model,cost)])
    shock_start = datetime(2015,1,15,tzinfo=UTC)
    shock_end = datetime(2015,1,16,tzinfo=UTC)
    try:
        for number,config in enumerate(configs,1):
            cid = config['config_id']
            paths=path_sets[config['geometry_key']]
            path_times=times_cache[config['geometry_key']]
            outcome=outcome_cache[config['geometry_key']]
            indices = selected_indices(config,features)
            raw_digest = signal_hash(indices,bars)
            for i in indices:
                sinks['raw_signal_membership.csv'].add(dict(config_id=cid,signal_index=i))
            for model in MODELS:
                for cost in COSTS:
                    tag = dict(config_id=cid,execution_model=model,cost_ticks=cost)
                    rs_by_i = outcome[(model,cost)]
                    accepted,invalid,blocked = replay(indices,paths,model,cost)
                    closed = [i for i in accepted if paths[i][model]['exit'] is not None]
                    open_ids = [i for i in accepted if paths[i][model]['exit'] is None]
                    isolated = [i for i in indices if geometry(paths[i][model],cost)[1] is None]
                    isolated_rs = [rs_by_i[i] for i in isolated if rs_by_i[i] is not None]
                    rs = [rs_by_i[i] for i in closed]
                    digest = hashlib.sha256()
                    month_entries = [0]*(len(BOUNDS)-1)
                    month_exits = [0]*(len(BOUNDS)-1)
                    month_cash = [0.]*(len(BOUNDS)-1)
                    month_cohort = [0.]*(len(BOUNDS)-1)
                    month_open = [0]*(len(BOUNDS)-1)
                    for seq,i in enumerate(accepted,1):
                        p = paths[i][model]
                        fill = geometry(p,cost)[0]
                        full = dict(tag,accepted_sequence=seq,signal_index=i,exit_index=p['exit_index'],
                            signal=p['signal'],entry=p['entry'],exit=p['exit'],reference_entry=p['reference_entry'],
                            historical_fill=fill,stop=p['stop'],target=p['target'],risk_price=fill-p['stop'],
                            exit_price=p['exit_price'],reason=p['reason'],r=rs_by_i[i])
                        sinks['accepted_trades.csv'].add({k:full[k] for k in definitions['accepted_trades.csv']})
                        digest.update((json.dumps(full,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode())
                        sinks['accepted_ledgers.csv'].add(full)
                        if cid in PARITY_IDS:
                            sinks['control_accepted_ledgers.csv'].add(full)
                        et,xt = path_times[(i,model)]
                        bucket = bisect.bisect_right(BOUNDS,et)-1
                        month_entries[bucket] += 1
                        if xt is None:
                            month_open[bucket] += 1
                            sinks['open_at_data_end.csv'].add(dict(tag,signal_index=i,entry=p['entry'],historical_fill=fill,
                                stop=p['stop'],target=p['target'],held_hours_to_cutoff=(END-et).total_seconds()/3600))
                        else:
                            month_cohort[bucket] += rs_by_i[i]
                            exit_bucket = bisect.bisect_left(BOUNDS,xt)-1
                            if not 0 <= exit_bucket < len(month_cash):
                                raise RuntimeError('Exit outside the reporting months.')
                            month_cash[exit_bucket] += rs_by_i[i]
                            month_exits[exit_bucket] += 1
                        if et < shock_end and (xt is None or xt >= shock_start):
                            sinks['chf_event_exposure.csv'].add(dict(tag,signal_index=i,entry=p['entry'],exit=p['exit'],
                                entered_on_event_day=int(shock_start <= et < shock_end),
                                exited_on_event_day=int(xt is not None and shock_start < xt <= shock_end),
                                held_across_event_day_start=int(et < shock_start and (xt is None or xt > shock_start)),
                                eventual_r=rs_by_i[i],outcome=p['reason'],
                                attribution_warning='Exposure attribution only; not an event-removal counterfactual'))
                    for i,reason in invalid:
                        sinks['invalid_entries.csv'].add(dict(tag,signal_index=i,reason=reason))
                    for k in range(len(month_cash)):
                        sinks['monthly_results.csv'].add(dict(tag,month=BOUNDS[k].strftime('%Y-%m'),
                            start=iso(BOUNDS[k]),end=iso(BOUNDS[k+1]),partial_month=int(BOUNDS[k+1].day!=1),
                            entry_count=month_entries[k],exit_count=month_exits[k],realized_r=month_cash[k],
                            entry_cohort_eventual_r=month_cohort[k],entry_cohort_open=month_open[k]))
                    positive_years = negative_years = empty_years = 0
                    for label,kind,a,b in PERIODS:
                        entry_ids = [i for i in accepted if a <= path_times[(i,model)][0] < b]
                        exit_ids = [i for i in closed if a < path_times[(i,model)][1] <= b]
                        cohort_closed = [i for i in entry_ids if rs_by_i[i] is not None]
                        cohort_r = sum(rs_by_i[i] for i in cohort_closed)
                        period_stats = stats(rs_by_i[i] for i in exit_ids)
                        opened = sum(path_times[(i,model)][0] < b and
                                     (path_times[(i,model)][1] is None or path_times[(i,model)][1] > b) for i in accepted)
                        partial = int(kind == 'calendar' and b == END and (END.month != 1 or END.day != 1))
                        sinks['period_results.csv'].add(dict(tag,period=label,period_type=kind,start=iso(a),end=iso(b),
                            partial_calendar_year=partial,entry_count=len(entry_ids),entry_cohort_completed=len(cohort_closed),
                            entry_cohort_eventual_r=cohort_r,entry_cohort_open=len(entry_ids)-len(cohort_closed),
                            exit_count=len(exit_ids),realized_r=period_stats['total_r'],
                            realized_max_dd_r=period_stats['max_closed_dd_r'],open_at_period_end=opened))
                        if kind == 'calendar' and not partial:
                            positive_years += cohort_r > 0
                            negative_years += cohort_r < 0
                            empty_years += not entry_ids
                    prefixes = []
                    for series in (month_entries,month_exits,month_cash):
                        values = [0]
                        for v in series:
                            values.append(values[-1]+v)
                        prefixes.append(values)
                    rolled = {}
                    for width in (12,24,36):
                        worst, empty = None, 0
                        for stop in range(width,len(BOUNDS)):
                            if BOUNDS[stop].day != 1:
                                continue  # rolling windows use complete calendar months
                            start = stop-width
                            en,ex,cash = [p[stop]-p[start] for p in prefixes]
                            row = dict(tag,months=width,start=iso(BOUNDS[start]),end=iso(BOUNDS[stop]),
                                entry_count=en,exit_count=ex,realized_r=cash,zero_entry_window=int(en==0),zero_exit_window=int(ex==0))
                            sinks['rolling_windows.csv'].add(row)
                            empty += en == 0
                            if worst is None or cash < worst['realized_r']:
                                worst = row
                        rolled.update({f'worst_{width}m_realized_r':worst['realized_r'],
                                       f'worst_{width}m_start':worst['start'],f'worst_{width}m_end':worst['end'],
                                       f'zero_entry_{width}m_windows':empty})
                    for reference in COMPARISON_IDS:
                        reference_case = refs.get((reference,model,cost))
                        if reference_case is None:
                            continue  # only used by tiny synthetic subset tests
                        reference_ids,reference_rs=reference_case
                        ours, theirs = set(accepted),set(reference_ids)
                        common, added, removed = ours & theirs, ours-theirs, theirs-ours
                        total = lambda ids: sum(rs_by_i[i] for i in sorted(ids) if rs_by_i[i] is not None)
                        reference_total=lambda ids:sum(reference_rs[i] for i in sorted(ids) if reference_rs[i] is not None)
                        reference_r = reference_total(theirs)
                        sinks['accepted_comparisons.csv'].add(dict(tag,reference_config=reference,
                            common_entries=len(common),added_entries=len(added),removed_entries=len(removed),
                            common_completed_r_candidate=total(common),common_completed_r_reference=reference_total(common),
                            added_completed_r=total(added),removed_completed_r=reference_total(removed),
                            candidate_total_r=sum(rs),reference_total_r=reference_r,delta_total_r=sum(rs)-reference_r,
                            candidate_open_count=len(open_ids),reference_open_count=sum(reference_rs[i] is None for i in reference_ids)))
                    entry_times = [path_times[(i,model)][0] for i in accepted]
                    holdings = [(path_times[(i,model)][1]-path_times[(i,model)][0]).total_seconds()/3600 for i in closed]
                    risk_sizes = [paths[i][model]['reference_entry']-paths[i][model]['stop'] for i in accepted]
                    fractions = [cost*TICK/v for v in risk_sizes]
                    run = maxrun = 0
                    for n in month_entries:
                        run = run+1 if n == 0 else 0
                        maxrun = max(maxrun,run)
                    isolated_stats = stats(isolated_rs)
                    row = dict(tag,raw_signals=len(indices),eligible_isolated_signals=len(isolated),
                        isolated_completed=len(isolated_rs),isolated_open=len(isolated)-len(isolated_rs),
                        isolated_total_r=isolated_stats['total_r'],isolated_profit_factor=isolated_stats['profit_factor'],
                        geometry_invalid_all_signals=len(indices)-len(isolated),p0_blocked_signals=blocked,
                        invalid_unblocked_entries=len(invalid),open_at_data_end=len(open_ids),**stats(rs),
                        dual_touch_closed=sum(paths[i][model]['dual_touch'] for i in closed),
                        gap_stop_closed=sum(paths[i][model]['gap_stop'] for i in closed),
                        gap_target_closed=sum(paths[i][model]['gap_target'] for i in closed),
                        entry_next_open_gap_count=sum(paths[i][model]['next_open'] is not None and
                            abs(paths[i][model]['next_open']-paths[i][model]['reference_entry']) > .5*TICK for i in accepted),
                        entries_before_market_closure=sum((paths[i][model]['next_candle_delay_hours'] or 0) > 0 for i in accepted),
                        median_stop_pips=quantile([v/PIP for v in risk_sizes],.5),
                        median_cost_fraction_reference_risk=quantile(fractions,.5),p90_cost_fraction_reference_risk=quantile(fractions,.9),
                        median_holding_hours=quantile(holdings,.5),max_holding_hours=max(holdings) if holdings else None,
                        maximum_inter_entry_gap_days=max(((b-a).total_seconds()/86400 for a,b in zip(entry_times,entry_times[1:])),default=None),
                        leading_no_entry_days=((entry_times[0] if entry_times else END)-START).total_seconds()/86400,
                        trailing_no_entry_days=(END-(entry_times[-1] if entry_times else START)).total_seconds()/86400,
                        zero_entry_months=sum(n==0 for n in month_entries),max_consecutive_zero_entry_months=maxrun,
                        positive_complete_entry_years=positive_years,negative_complete_entry_years=negative_years,
                        zero_entry_complete_years=empty_years,raw_signal_sha256=raw_digest,accepted_ledger_sha256=digest.hexdigest(),**rolled)
                    sinks['summary.csv'].add(row)
                    summaries[(cid,model,cost)] = row
            if progress and (number % 20 == 0 or number == len(configs)):
                set_status(state='analyzing',progress=round(40+52*number/len(configs)),
                           message=f'{number}/{len(configs)} entry configurations completed; all costs and exit assumptions')
        expected = len(configs)*len(MODELS)*len(COSTS)
        if sinks['summary.csv'].count != expected:
            raise RuntimeError('Missing configuration/cost/model results.')
    finally:
        for sink in sinks.values():
            sink.close()
    write_neighbours(work,configs,summaries)
    export_matched_comparisons(work,configs)
    counts = {name:sink.count for name,sink in sinks.items()}
    for extra in ('matched_raw_comparisons.csv',):
        with (work/extra).open(newline='') as source:counts[extra]=sum(1 for _ in csv.DictReader(source))
    counts.update(export_extra_diagnostics(work,bars,features,path_sets,configs))
    write_json(work/'output_row_counts.json',counts)
    return summaries,counts

BOUNDS=month_bounds()
PERIODS=period_definitions()

PROTOCOL='# EURCHF M15 LONG Pass4: standalone failed-breakdown/reclaim discovery\n\nPredeclared 10 October 2026, before new actual-history outcomes. Template is a\nguide; this is a distinct trigger after six body/range relaxations lost on their\nadded trades. Existing GBPUSD and EURCHF H1 reclaim work supplies a hypothesis,\nnot evidence of an EURCHF M15 edge. No automatic rescue, winner or live action.\n\n## Frozen history and execution\nSame EURCHF_M15_PASS1_FROZEN_DATA.zip next to research app.py: 546647 native MID\nM15 /137939 native H1 observations; requested 2005-01-01 through\n2026-10-08T00:00Z exclusive. Source SHA/member receipts are mandatory gates.\nNo fetch, new credentials, packages, calendar exclusions or synthesized bars.\nCommon warmup200 observed bars, causal current ATR14 seeded TR1..14 then Wilder\nrecurrence. Entry at signal close; stop signal low minus1pip; target reference\nentry plus3 times reference risk. Fixed RR3; BUY adverse entry costs1/2/4pips,\nR denominator assumed fill minus stop. Primary STOP_FIRST_GAP_STRESS: adverse\nstop gaps can lose more than1R, target gaps capped, dual touch stop first.\nNEAREST_OPEN_SENSITIVITY is a separate intrabar sensitivity, not tick truth.\nFull per-case P0; exit-candle reentry allowed; invalid geometry does not occupy;\nunresolved positions occupy through cutoff with no invented R. Entries\n[start,end), exits (start,end]. MID costs omit executable bid/ask and financing.\n\n## Predeclared mechanism and grid\nFor each lookback60/120/200 observed M15 bars, preceding low=min lows[i-L:i],\nstrictly excluding current candle. Require signal low < preceding low,\nsignal close > preceding low, close > open, positive range and positive ATR.\nEquality at prior low fails either breach or reclaim. Exact engulf not required.\nNo inherited distance cap, range floor, H1/daily volatility or trend filter.\nBody minima0/.50/1.00 ATR x signal close-location minima0/.65/.80, each inclusive.\nClose location=(close-low)/(high-low). 3x3x3=27 reclaim definitions:3 raw controls,\n12 single-quality research rows and12 predeclared joint-quality rows. Joint rows\nare bounded hypotheses, not a requirement that each single filter be profitable.\nLookbacks span roughly15/30/50 trading hours; quality levels broadly separate\nsmall/moderate/large bullish bodies and ordinary/upper-range closes. These are\ncoarse discovery axes, not claimed transferable optimal settings. No extra\ndepth, session, minimum-stop, trend, cost-derived or RR search.\n\nSeven exact old engulf controls retained: raw, original/tight unfiltered,\ntheir H1-availability and H1vol200>=1 versions. Their42 prior raw/full accepted\nledger hashes/counts must match BEFORE new actual reclaim paths are evaluated.\nPrevious failed body/range relaxations remain archived in Pass3, not new research.\nTotal34 definitions/204 cost-model cases;24 new research rows,10 controls.\n\n## Reporting and interpretation\nExport full features, strict masks, paths, accepted/invalid/open ledgers,\nisolated and full P0 results,35 periods/262 months/714 rolling windows per case,\nincluding zero periods, partial2026/October, older weak eras, blank years,\ndroughts, stop/cost buckets, stream equivalences and descriptive CHF-event\nexposure. Adjacent neighbours change ONE grid axis by ONE level; endpoints\nexplicit. Identical streams and overlapping outcomes are not independent proof.\nEvery reclaim quality row compares to its own lookback raw baseline. Also report\nboth single-filter parents for each joint row, with actual common/added/removed\naccepted entries and their R. These are independent full P0 replay differences;\nfiltering can displace trades. Do not sum standalone ledgers as a hybrid.\nOld engulf comparisons are diagnostic only. NO combined A+B replay here.\n\nRead raw, single, then joint results. Require a coherent neighbouring region,\nuseful recent activity, plausible older/rolling behaviour and support at2/4pip\ncosts and both models. Lifetime maxR, a single cell or inherited old-anchor R\ncannot qualify this mechanism. Review all weak/zero results; no auto ranking.\nIf supported, freeze a region for subsequent confirmation before combining with\nengulfing; a combined replay must account for overlapping/displaced trades.\nDo not add post-hoc filters or sweep RR to manufacture recovery.\nFull history has been reused; this is exploratory/in-sample, not untouched OOS.\nAdditive R/DD is not NAV%, marked-equity drawdown or a guaranteed gap-loss bound.\nSeparate research service only. Live Portfolio32/EURCHF H1 SHORT unchanged.\nDelivery validation computes source/features/new masks and already-returned\ncontrol performance only; new actual reclaim performance is left to user run.\n'
RESULT_README='# EURCHF M15 LONG Pass4 results\nRead protocol.md and source/software/base/context/raw/archive gates first.\n34 definitions/204 cases:27 reclaim rows(3raw/12single/12joint),7archived engulf\ncontrols.42 old cases must match before new reclaim outcomes. Fixed RR3 and\n1/2/4pip costs, full2005–2026history. Review summary, period, monthly, rolling,\nneighbourhood, equivalence and stop/cost outputs together. Use\nreclaim_parent_comparisons.csv for each joint row versus BOTH single parents;\naccepted_comparisons.csv also includes its own raw baseline and old diagnostics.\nCommon/added/removed trade R comes from full independent P0 chronologies.\nNo combined-strategy results, automatic winner, portfolio or live activation.\n'


def archived_controls(work,bars,features,path_sets,configs):
    if sha(ARCHIVED_REFERENCE_JSON.encode())!=ARCHIVED_REFERENCE_SHA256:raise RuntimeError('Embedded archive reference SHA mismatch')
    reference=json.loads(ARCHIVED_REFERENCE_JSON)
    if len(reference)!=42 or {(r['config_id'],r['execution_model'],int(r['cost_ticks'])) for r in reference}!={(c,m,k) for c in PARITY_IDS for m in MODELS for k in COSTS}:
        raise RuntimeError('Incomplete archived reference case set')
    write_json(work/'archived_control_reference.json',reference)
    byid={c['config_id']:c for c in configs};paths=path_sets['LONG_SIGNAL_LOW'];checks=[]
    for row in reference:
        cid,model,cost=row['config_id'],row['execution_model'],int(row['cost_ticks']);indices=selected_indices(byid[cid],features)
        accepted,invalid,blocked=replay(indices,paths,model,cost);digest=hashlib.sha256()
        for seq,i in enumerate(accepted,1):
            p=paths[i][model];fill=geometry(p,cost)[0]
            record=dict(config_id=cid,execution_model=model,cost_ticks=cost,accepted_sequence=seq,signal_index=i,exit_index=p['exit_index'],
                signal=p['signal'],entry=p['entry'],exit=p['exit'],reference_entry=p['reference_entry'],historical_fill=fill,
                stop=p['stop'],target=p['target'],risk_price=fill-p['stop'],exit_price=p['exit_price'],reason=p['reason'],r=r_value(p,cost))
            digest.update((json.dumps(record,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode())
        actual=digest.hexdigest();raw=signal_hash(indices,bars)
        ok=actual==row['accepted_ledger_sha256'] and raw==row['raw_signal_sha256'] and len(indices)==int(row['raw_signals'])
        ok=ok and len(accepted)==int(row['closed_trades'])+int(row['open_at_data_end']) and blocked==int(row['p0_blocked_signals']) and len(invalid)==int(row['invalid_unblocked_entries'])
        checks.append(dict(config_id=cid,execution_model=model,cost_ticks=cost,status='PASS' if ok else 'FAIL',
            accepted_entries=len(accepted),actual_ledger_sha256=actual,expected_ledger_sha256=row['accepted_ledger_sha256'],raw_signal_sha256=raw))
    write_csv(work/'archived_control_parity.csv',checks)
    if any(r['status']!='PASS' for r in checks):raise RuntimeError('Archived full-ledger parity failed; no new research conclusions')
    return checks



def export_matched_comparisons(work,configs):
    byid={c['config_id']:c for c in configs}
    with (work/'accepted_comparisons.csv').open(newline='') as f:
        reader=csv.DictReader(f);fields=reader.fieldnames
        count=write_csv(work/'matched_anchor_comparisons.csv',(r for r in reader if r['reference_config']==byid[r['config_id']]['matched_anchor']),fields)
    # Preserve generic reporting harness filename; this copy is raw-only.
    with (work/'accepted_comparisons.csv').open(newline='') as f:
        reader=csv.DictReader(f);write_csv(work/'matched_raw_comparisons.csv',(r for r in reader if r['reference_config']==PARENT_IDS[0]),reader.fieldnames)
    return count

def export_extra_diagnostics(work,bars,features,path_sets,configs):
    # Stream one case at a time. Retain all comparison references for matched-ledger attribution.
    refs={};groups=defaultdict(list);byid={c['config_id']:c for c in configs}
    marginal=CSVFile(work/'marginal_entry_differences.csv',CASE+['reference_config','change','signal_index','qualifies_in_candidate','qualifies_in_reference'])
    buckets=CSVFile(work/'stop_cost_attribution.csv',CASE+['bucket_axis','bucket','accepted_entries','closed_trades','open_trades','total_r','winners','losers','interpretation'])
    for c in configs:groups['RAW','ALL',0,signal_hash(selected_indices(c,features),bars)].append(c['config_id'])
    def flush(key,ledger):
        cid,model,cost=key;c=byid[cid];ours={int(r['signal_index']):r for r in ledger}
        groups['ACCEPTED_SIGNAL',model,cost,sha('\n'.join(r['signal_index'] for r in ledger).encode())].append(cid)
        groups['ACCEPTED_EXECUTION',model,cost,sha('\n'.join(json.dumps({k:v for k,v in r.items() if k!='config_id'},sort_keys=True,separators=(',',':')) for r in ledger).encode())].append(cid)
        if cid in PARENT_IDS:refs[cid,model,cost]=ours
        refid=c['matched_anchor'];theirs=refs[refid,model,cost]
        if cid!=refid:
            raw=set(selected_indices(c,features));rawref=set(selected_indices(byid[refid],features))
            for change,ids in [('ADDED',ours.keys()-theirs.keys()),('REMOVED',theirs.keys()-ours.keys())]:
                for i in sorted(ids):marginal.add(dict(config_id=cid,execution_model=model,cost_ticks=cost,reference_config=refid,change=change,
                    signal_index=i,qualifies_in_candidate=int(i in raw),qualifies_in_reference=int(i in rawref)))
        for axis,limits,labels in [('stop_pips',(5.,10.,20.,40.),('<=5','(5,10]','(10,20]','(20,40]','>40')),
            ('cost_fraction_reference_risk',(.05,.1,.2,.4),('<=.05','(.05,.10]','(.10,.20]','(.20,.40]','>.40'))]:
            assigned=[[] for _ in labels]
            for r in ledger:
                risk=float(r['reference_entry'])-float(r['stop']);v=risk/PIP if axis=='stop_pips' else cost*TICK/risk
                assigned[bisect.bisect_left(limits,v)].append(r)
            for label,subset in zip(labels,assigned):
                rs=[float(r['r']) for r in subset if r['r']!='']
                buckets.add(dict(config_id=cid,execution_model=model,cost_ticks=cost,bucket_axis=axis,bucket=label,
                    accepted_entries=len(subset),closed_trades=len(rs),open_trades=len(subset)-len(rs),total_r=math.fsum(rs),
                    winners=sum(r>0 for r in rs),losers=sum(r<0 for r in rs),interpretation='Fixed descriptive attribution; no optimized stop filter'))
    with (work/'accepted_ledgers.csv').open(newline='') as f:
        it=iter(csv.DictReader(f));pending=next(it,None)
        for c in configs:
            for model in MODELS:
                for cost in COSTS:
                    key=c['config_id'],model,cost;ledger=[]
                    while pending and (pending['config_id'],pending['execution_model'],int(pending['cost_ticks']))==key:
                        ledger.append(pending);pending=next(it,None)
                    flush(key,ledger)
        if pending is not None:raise RuntimeError('Unexpected ledger ordering')
    marginal.close();buckets.close()
    eq=[dict(equivalence_kind=k,execution_model=m,cost_ticks=c,fingerprint=h,configuration_count=len(ids),config_ids=';'.join(ids),
        research_configuration_count=sum(byid[i]['role']=='RESEARCH' for i in ids),interpretation='Identical streams are not independent evidence') for (k,m,c,h),ids in sorted(groups.items())]
    with (work/'matched_anchor_comparisons.csv').open(newline='') as f:count=sum(1 for _ in csv.DictReader(f))
    return {'marginal_entry_differences.csv':marginal.count,'stop_cost_attribution.csv':buckets.count,
        'equivalence_groups.csv':write_csv(work/'equivalence_groups.csv',eq),'matched_anchor_comparisons.csv':count}

def run_job():
    global RUN_CLOCK,JOB_LOCK
    RUN_CLOCK=time.monotonic();OUT.mkdir(parents=True,exist_ok=True);work=Path(tempfile.mkdtemp(prefix='working-',dir=OUT))
    manifest=dict(version=VERSION,runner_sha256=code_hash(),pair=PAIR,side=SIDE,timeframe=TIMEFRAME,complete=False,status='RUNNING',
        dataset_kind='ACTUAL_PINNED_NATIVE_OANDA_MID',requested_start=iso(START),requested_end_exclusive=iso(END),rr=RR,cost_ticks=COSTS,
        models=MODELS,primary_model='STOP_FIRST_GAP_STRESS',stop_definition='UNCHANGED_SIGNAL_LOW_MINUS_1_PIP',
        anchors=ANCHORS,reclaim_lookbacks=(60,120,200),body_floors=(0.,.5,1.),close_floors=(0.,.65,.8),configurations=34,research_definitions=24,new_research_definitions=24,retained_research_definitions=0,controls=10,
        cases=204,archived_parity_cases=42,daily_alignment='UTC_OBSERVED_DAYS_NOT_BROKER_NY_D1',
        portfolio_target='PORTFOLIO32_2026_10_05_EURCHF_H1_SHORT_PRIMARY_RR3P50_V1; admission deferred',orders_supported=False,trading_enabled=False)
    try:
        (work/'protocol.md').write_text(PROTOCOL);shutil.copyfile(__file__,work/'runner_source.py')
        write_csv(work/'software_checks.csv',self_checks())
        set_status(state='validating',progress=2,message='Validating full frozen source and separate closed context features')
        bars,volumes,h1,hvolumes=load_frozen_history(work);crosscheck_history(work,bars,volumes,h1,hvolumes)
        configs,memberships=make_configs();features=make_features(bars,h1)
        controls=hard_controls(bars,features,configs);write_csv(work/'hard_controls.csv',controls)
        conditional=context_controls(bars,features,configs);write_csv(work/'conditional_feature_controls.csv',conditional)
        if any(r['status']!='PASS' for r in controls+conditional):raise RuntimeError('Independent feature/conditional controls failed')
        set_status(state='building_paths',progress=25,message='Verifying seven unchanged controls /42 archived cases')
        oldconfigs=[c for c in configs if c['previously_tested']]
        paths=build_paths(bars,features,oldconfigs)
        baseline_parity(work,bars,features,paths);archived_controls(work,bars,features,paths,configs)
        # New actual mechanism paths start ONLY after exact old-control gates.
        paths.update(build_paths(bars,features,[c for c in configs if not c['previously_tested']]))
        write_inputs(work,bars,features,paths,configs,memberships,manifest['dataset_kind'],volumes);write_context_inputs(work,features)
        (work/'README.md').write_text(RESULT_README)
        _,counts=analyze(work,bars,features,paths,configs)
        counts.update(export_conditional_attribution(work,bars,features,paths,configs));write_json(work/'output_row_counts.json',counts)
        manifest.update(complete=True,status='COMPLETE',source_sha256=source_hash(bars),source_candles=len(bars),source_h1_sha256=source_hash(h1),
            source_controls='PASS',hard_controls='PASS',conditional_controls='PASS',baseline_full_ledger_parity='PASS',archived_full_ledger_parity='PASS',
            software_controls='PASS',protocol_sha256=sha(PROTOCOL.encode()),output_row_counts=counts,
            completed_at=iso(datetime.now(UTC)),elapsed_seconds=round(time.monotonic()-RUN_CLOCK,1))
        write_json(work/'run_manifest.json',manifest);set_status(state='packaging',progress=96,message='All204 standalone cases complete; packaging results')
        package(work,True);set_status(state='complete',progress=100,message='EURCHF M15 LONG Pass4 complete; download results ZIP',
            configurations=34,cases=204,result_path='/results',result_bytes=(OUT/RESULT_NAME).stat().st_size)
        return True
    except Exception as exc:
        manifest.update(status='ERROR',complete=False,error_type=type(exc).__name__,error=str(exc));write_json(work/'run_manifest.json',manifest)
        write_csv(work/'error_report.csv',[dict(error_type=type(exc).__name__,message=str(exc),traceback=traceback.format_exc())])
        try:package(work,False);result_path='/results'
        except Exception:result_path=None
        set_status(state='error',progress=100,message=str(exc),result_path=result_path);return False
    finally:
        shutil.rmtree(work,ignore_errors=True)
        if JOB_LOCK is not None:JOB_LOCK.close();JOB_LOCK=None



CONTEXT_KEYS=('prior_move_16_atr', 'prior_move_64_atr', 'prior_move_192_atr', 'prior_move_16_wall_hours', 'prior_move_64_wall_hours', 'prior_move_192_wall_hours', 'previous_close_location', 'signal_close_location', 'm15_relative_volatility', 'h1_relative_volatility', 'd1_relative_volatility', 'd1_close_minus_ema100', 'd1_close_minus_ema200')
CONTEXT_AUDIT_KEYS=('h1_context_index', 'd1_context_index', 'h1_context_close', 'd1_context_close', 'h1_context_age_hours', 'd1_context_age_hours')

def rolling_reference(values, width):
    """Mean of exactly width preceding observed values; excludes values[i]."""
    total=0.;missing=0;result=[None]*len(values)
    for i,value in enumerate(values):
        if i>=width and missing==0:result[i]=total/width
        if value is None:missing+=1
        else:total+=value
        if i>=width:
            old=values[i-width]
            if old is None:missing-=1
            else:total-=old
    return result

def ema_seeded(values, period):
    out=[None]*len(values)
    if len(values)<period:return out
    out[period-1]=math.fsum(values[:period])/period
    alpha=2/(period+1)
    for i in range(period,len(values)):out[i]=out[i-1]+alpha*(values[i]-out[i-1])
    return out

def aggregate_hours(bars):
    groups={}
    for t,op,hi,lo,cl in bars:
        key=t.replace(minute=0,second=0,microsecond=0)
        if key not in groups:groups[key]=[key,op,hi,lo,cl]
        else:
            row=groups[key];row[2]=max(row[2],hi);row[3]=min(row[3],lo);row[4]=cl
    # Used only by software fixtures; production uses pinned native H1.
    cutoff=bars[-1][0]+BAR if bars else START
    return [tuple(row) for key,row in sorted(groups.items()) if key+H1_BAR<=cutoff]

def base_build_context(bars, h1=None):
    if h1 is None:h1=aggregate_hours(bars)
    cutoff=bars[-1][0]+BAR if bars else START
    h1=[b for b in h1 if b[0]+H1_BAR<=cutoff]
    a15=atr14(bars);v15=rolling_reference(a15,200)
    ah=atr14(h1);vh=rolling_reference(ah,200)
    hends=[b[0]+H1_BAR for b in h1]
    groups={}
    for t,op,hi,lo,cl in h1:
        key=t.replace(hour=0,minute=0,second=0,microsecond=0)
        if key not in groups:groups[key]=[key,op,hi,lo,cl,1,t,t]
        else:
            r=groups[key];r[2]=max(r[2],hi);r[3]=min(r[3],lo);r[4]=cl;r[5]+=1;r[7]=t
    first_day=h1[0][0].replace(hour=0,minute=0,second=0,microsecond=0) if h1 else None
    # Initial truncated UTC day is excluded. Later observed days, including
    # short Sunday sessions, remain. UTC bins are not broker New-York D1 bars.
    daily=[r for key,r in sorted(groups.items()) if key!=first_day and key+timedelta(days=1)<=cutoff]
    dbars=[tuple(r[:5]) for r in daily];ad=atr14(dbars);vd=rolling_reference(ad,100)
    em={p:ema_seeded([b[4] for b in dbars],p) for p in (100,200)}
    dends=[b[0]+timedelta(days=1) for b in dbars]
    return dict(bars=bars,h1=h1,atr_m15=a15,mean_m15=v15,atr_h1=ah,mean_h1=vh,
        h1_ends=hends,daily=daily,dbars=dbars,atr_d1=ad,mean_d1=vd,ema_d1=em,d1_ends=dends)


def base_context_for_signal(ctx, i):
    bars=ctx['bars'];entry=bars[i][0]+BAR;p=i-1;a=ctx['atr_m15'][p]
    h=bisect.bisect_right(ctx['h1_ends'],entry)-1
    d=bisect.bisect_right(ctx['d1_ends'],entry)-1
    row={key:None for key in CONTEXT_KEYS}
    row.update(h1_context_index=h if h>=0 else None,d1_context_index=d if d>=0 else None,
        h1_context_close=iso(ctx['h1_ends'][h]) if h>=0 else None,
        d1_context_close=iso(ctx['d1_ends'][d]) if d>=0 else None,
        h1_context_age_hours=(entry-ctx['h1_ends'][h]).total_seconds()/3600 if h>=0 else None,
        d1_context_age_hours=(entry-ctx['d1_ends'][d]).total_seconds()/3600 if d>=0 else None)
    for n in (16,64,192):
        if p>=n and a is not None and a>0:
            row[f'prior_move_{n}_atr']=(bars[p][4]-bars[p-n][4])/a
            row[f'prior_move_{n}_wall_hours']=(bars[p][0]-bars[p-n][0]).total_seconds()/3600
    prev=bars[p];signal=bars[i]
    if prev[2]>prev[3]:row['previous_close_location']=(prev[4]-prev[3])/(prev[2]-prev[3])
    if signal[2]>signal[3]:row['signal_close_location']=(signal[4]-signal[3])/(signal[2]-signal[3])
    mean=ctx['mean_m15'][p]
    if a is not None and mean is not None and mean>0:row['m15_relative_volatility']=a/mean
    if h>=0:
        a,mean=ctx['atr_h1'][h],ctx['mean_h1'][h]
        if a is not None and mean is not None and mean>0:row['h1_relative_volatility']=a/mean
    if d>=0:
        a,mean=ctx['atr_d1'][d],ctx['mean_d1'][d]
        if a is not None and mean is not None and mean>0:row['d1_relative_volatility']=a/mean
        for period in (100,200):
            v=ctx['ema_d1'][period][d]
            if v is not None:row[f'd1_close_minus_ema{period}']=ctx['dbars'][d][4]-v
    return row


def make_features(bars, h1=None):
    features=base_features(bars);ctx=build_context(bars,h1)
    for i,row in features['LONG_SIGNAL_LOW'].items():row.update(context_for_signal(ctx,i))
    features['CONTEXT']=ctx
    return features

def condition_pass(c, row):
    field=c.get('feature')
    if not field:return True
    value=row.get(field)
    if value is None or not math.isfinite(value):return False
    op=c['operator'];level=c['threshold']
    if op=='AVAILABLE':return True
    if op=='GE':return value>=level
    if op=='LE':return value<=level
    if op=='GT':return value>level
    if op=='LT':return value<level
    raise RuntimeError('Unknown condition operator: '+op)

def selected_indices(config,features):
    return [i for i in base_selected(config,features) if condition_pass(config,features[config['geometry_key']][i])]




def context_controls(bars, features, configs):
    """Independent direct-slice indicator and closed-context verification."""
    checks=[];ctx=features['CONTEXT'];rows=features['LONG_SIGNAL_LOW']
    def check(name,ok):checks.append(dict(check=name,status='PASS' if ok else 'FAIL'))
    def independent_atr(series):
        result=[None]*len(series)
        if len(series)>14:
            tr=[max(b[2]-b[3],abs(b[2]-a[4]),abs(b[3]-a[4])) for a,b in zip(series,series[1:])]
            result[14]=math.fsum(tr[:14])/14
            for j in range(15,len(series)):result[j]=(13*result[j-1]+tr[j-1])/14
        return result
    def same(a,b):return (a is None and b is None) or (a is not None and b is not None and abs(a-b)<=1e-9*max(1.,abs(a),abs(b)))
    independent={key:independent_atr(series) for key,series in [('M15',bars),('H1',ctx['h1']),('D1',ctx['dbars'])]}
    for label,key in [('M15','atr_m15'),('H1','atr_h1'),('D1','atr_d1')]:
        check(label+'_independent_ATR',all(same(a,b) for a,b in zip(independent[label],ctx[key])))
    # Independently rebuild UTC daily OHLC from grouped native H1. Initial day
    # excluded; no future/incomplete UTC day can enter the aggregate.
    days=defaultdict(list)
    for b in ctx['h1']:days[b[0].date()].append(b)
    first=min(days) if days else None;db=[]
    for day,values in sorted(days.items()):
        midnight=datetime.combine(day,datetime.min.time(),tzinfo=UTC)
        if day==first or midnight+timedelta(days=1)>bars[-1][0]+BAR:continue
        db.append((midnight,values[0][1],max(b[2] for b in values),min(b[3] for b in values),values[-1][4]))
    check('UTC_daily_OHLC_independent',db==ctx['dbars'])
    ema_ref={}
    for period in (100,200):
        closes=[b[4] for b in db];values=[None]*len(db)
        if len(db)>=period:
            value=math.fsum(closes[:period])/period;values[period-1]=value
            for j in range(period,len(db)):
                value=closes[j]*(2/(period+1))+value*(1-2/(period+1));values[j]=value
        ema_ref[period]=values
        check(f'D1_EMA{period}_independent',all(same(a,b) for a,b in zip(values,ctx['ema_d1'][period])))
    mismatches=defaultdict(int);direct={}
    hstarts=[b[0] for b in ctx['h1']];dstarts=[b[0] for b in db]
    for i,row in rows.items():
        p=i-1;entry=bars[i][0]+BAR;values={}
        for n in (16,64,192):
            den=independent['M15'][p]
            values[f'prior_move_{n}_atr']=(bars[p][4]-bars[p-n][4])/den if p>=n and den is not None and den>0 else None
            values[f'prior_move_{n}_wall_hours']=(bars[p][0]-bars[p-n][0]).total_seconds()/3600 if p>=n and den is not None and den>0 else None
        for k,j in [('previous_close_location',p),('signal_close_location',i)]:
            b=bars[j];values[k]=(b[4]-b[3])/(b[2]-b[3]) if b[2]>b[3] else None
        # Lookup by completed context timestamps; independently check the next
        # context is still in the future, including exact H1/day boundaries.
        h=bisect.bisect_right(hstarts,entry-H1_BAR)-1
        d=bisect.bisect_right(dstarts,entry-timedelta(days=1))-1
        for label,j,width,key in [('M15',p,200,'m15_relative_volatility'),('H1',h,200,'h1_relative_volatility'),('D1',d,100,'d1_relative_volatility')]:
            a=independent[label];prior=a[j-width:j] if j>=width else []
            mean=math.fsum(prior)/width if len(prior)==width and all(v is not None for v in prior) else None
            values[key]=a[j]/mean if j>=0 and mean is not None and mean>0 and a[j] is not None else None
        for period in (100,200):
            em=ema_ref[period][d] if d>=0 else None
            values[f'd1_close_minus_ema{period}']=db[d][4]-em if em is not None else None
        for label,j in [('M15',p),('H1',h)]:
            a=independent[label]
            for width in (100,300):
                prior=a[j-width:j] if j>=width else []
                mean=math.fsum(prior)/width if len(prior)==width and all(v is not None for v in prior) else None
                values[vol_field(label,width)]=a[j]/mean if j>=0 and mean is not None and mean>0 and a[j] is not None else None
        direct[i]=values
        for k,value in values.items():mismatches[k]+=not same(value,row[k])
        mismatches['closed_context_indices']+=row['h1_context_index']!=(h if h>=0 else None) or row['d1_context_index']!=(d if d>=0 else None)
        mismatches['no_future_context']+=any(row[k] is not None and when(row[k])>entry for k in ('h1_context_close','d1_context_close'))
    for k,n in sorted(mismatches.items()):check(k+'_all_signal_rows',n==0)
    for c in configs:
        if c['geometry_key']!='LONG_SIGNAL_LOW':continue
        expected=[]
        for i,row in rows.items():
            if row['body_atr']<c['body_min_atr'] or row['range_atr']<c['range_min_atr']:continue
            if c['distance_max_atr'] is not None and row[f"abs_distance_atr_{c['lookback']}"]>c['distance_max_atr']:continue
            field=c['feature'];op=c['operator'];v=direct[i].get(field) if field else None;t=c['threshold']
            if field and (v is None or not math.isfinite(v)):continue
            if field and not (op=='AVAILABLE' or (op=='GE' and v>=t) or (op=='LE' and v<=t)
                or (op=='GT' and v>t) or (op=='LT' and v<t)):continue
            expected.append(i)
        check(c['config_id']+'_direct_conditional_mask',selected_indices(c,features)==expected)
    n=min(10000,len(bars));cutoff=bars[n-1][0]+BAR
    prefix=make_features(bars[:n],[b for b in ctx['h1'] if b[0]+H1_BAR<=cutoff])
    check('full_context_prefix_causality',prefix['LONG_SIGNAL_LOW']=={i:r for i,r in rows.items() if i<n})
    return checks



def export_conditional_attribution(work,bars,features,path_sets,configs):
    # For joint quality rows, compare with BOTH single parents; not a combined strategy.
    byid={c['config_id']:c for c in configs};rows=[]
    with (work/'accepted_comparisons.csv').open(newline='') as f:
        for r in csv.DictReader(f):
            c=byid[r['config_id']]
            if c['geometry_key']!='RECLAIM_SIGNAL_LOW':continue
            refs={c['matched_anchor']:'OWN_LOOKBACK_RAW'}
            if c['family']=='RECLAIM_JOINT':
                refs[f"RECLAIM_L{c['lookback']:03}_B{round(c['body_min_atr']*100):03}_CL000"]='BODY_ONLY_PARENT'
                refs[f"RECLAIM_L{c['lookback']:03}_B000_CL{round(c['close_min']*100):03}"]='CLOSE_ONLY_PARENT'
            if r['reference_config'] in refs:rows.append(dict(r,reference_role=refs[r['reference_config']],interpretation='Standalone full P0 replay difference; no hybrid admission'))
    return {'reclaim_parent_comparisons.csv':write_csv(work/'reclaim_parent_comparisons.csv',rows)}

VOL_WINDOWS=(100,200,300)
VOL_FLOORS=(.75,1.,1.25)

def vol_field(timeframe,window):
    base=timeframe.lower()+'_relative_volatility'
    return base if window==200 else base+'_w'+str(window)

def build_context(bars,h1=None):
    ctx=base_build_context(bars,h1)
    ctx['vol_means_m15']={w:rolling_reference(ctx['atr_m15'],w) for w in VOL_WINDOWS}
    ctx['vol_means_h1']={w:rolling_reference(ctx['atr_h1'],w) for w in VOL_WINDOWS}
    return ctx

def context_for_signal(ctx,i):
    row=base_context_for_signal(ctx,i)
    for tf,j,atr,means in [('M15',i-1,ctx['atr_m15'],ctx['vol_means_m15']),
                          ('H1',row['h1_context_index'],ctx['atr_h1'],ctx['vol_means_h1'])]:
        for w in (100,300):
            name=vol_field(tf,w);row[name]=None
            if j is not None and j>=0:
                a,mean=atr[j],means[w][j]
                if a is not None and mean is not None and mean>0:row[name]=a/mean
    return row

def retained_pass2b_configs():
    base=dict(geometry_key='LONG_SIGNAL_LOW',body_min_atr=1.25,range_min_atr=1.5,
        close_min=0.,feature='',operator='',threshold=None,previously_tested=True,
        retained_anchor=True,volatility_timeframe='',volatility_window=None,
        mechanism='EXACT_BULLISH_ENGULF',research_question='ARCHIVED_CONTROL')
    raw=dict(base,config_id='RAW_EXACT_BULLISH_ENGULF',role='CONTROL',family='RAW',
        lookback=0,distance_max_atr=None,body_min_atr=0.,range_min_atr=0.,
        matched_anchor='RAW_EXACT_BULLISH_ENGULF',parent_anchor='RAW_EXACT_BULLISH_ENGULF')
    anchors=[dict(base,config_id=cid,role='CONTROL',family='ANCHOR',lookback=lb,
        distance_max_atr=d,matched_anchor=cid,parent_anchor=cid) for cid,lb,d in ANCHORS]
    available=[dict(c,config_id=c['config_id']+'__AVAILABLE_H1_RELATIVE_VOLATILITY',
        family='AVAILABILITY',feature='h1_relative_volatility',operator='AVAILABLE',
        matched_anchor=c['config_id'],volatility_timeframe='H1',volatility_window=200) for c in anchors]
    filtered=[dict(c,config_id=c['config_id']+'__H1_RELATIVE_VOLATILITY_GE_P1P00',
        family='ARCHIVED_FILTERED',feature='h1_relative_volatility',operator='GE',threshold=1.,
        matched_anchor=c['config_id']+'__AVAILABLE_H1_RELATIVE_VOLATILITY',
        volatility_timeframe='H1',volatility_window=200) for c in anchors]
    return [raw]+anchors+available+filtered,[]

def make_configs():
    configs,_=retained_pass2b_configs()
    def cell(lb,body,close):
        cid=f'RECLAIM_L{lb:03}_B{round(body*100):03}_CL{round(close*100):03}'
        raw=f'RECLAIM_L{lb:03}_B000_CL000'
        axes=int(body>0)+int(close>0)
        return dict(config_id=cid,geometry_key='RECLAIM_SIGNAL_LOW',lookback=lb,
            body_min_atr=body,range_min_atr=0.,close_min=close,distance_max_atr=None,
            mechanism='FAILED_BREAKDOWN_RECLAIM',role='CONTROL' if axes==0 else 'RESEARCH',
            family=('RECLAIM_RAW','RECLAIM_SINGLE','RECLAIM_JOINT')[axes],
            matched_anchor=raw,parent_anchor=raw,previously_tested=False,retained_anchor=False,
            feature='',operator='',threshold=None,volatility_timeframe='',volatility_window=None,
            research_question='STANDALONE_RECLAIM_DISCOVERY')
    grid=[cell(lb,b,c) for lb in (60,120,200) for b in (0.,.5,1.) for c in (0.,.65,.8)]
    # Raw and single parents precede joints for streaming comparisons.
    configs+=sorted(grid,key=lambda c:(c['family']!='RECLAIM_RAW',c['family']=='RECLAIM_JOINT',c['lookback'],c['body_min_atr'],c['close_min']))
    fields=('config_id','role','family','mechanism','previously_tested','matched_anchor','parent_anchor',
        'retained_anchor','volatility_timeframe','volatility_window','research_question')
    return configs,[{k:c[k] for k in fields} for c in configs]

def neighbour_ids(c,configs):
    if c['geometry_key']!='RECLAIM_SIGNAL_LOW':return [],[]
    axes=[('lookback',(60,120,200)),('body_min_atr',(0.,.5,1.)),('close_min',(0.,.65,.8))]
    neighbours=[];edges=[]
    for axis,levels in axes:
        pos=levels.index(c[axis])
        if pos in (0,len(levels)-1):edges.append(axis+('=LOWER' if pos==0 else '=UPPER'))
        adjacent={levels[j] for j in (pos-1,pos+1) if 0<=j<len(levels)}
        neighbours.extend(x['config_id'] for x in configs if x['geometry_key']=='RECLAIM_SIGNAL_LOW'
            and x[axis] in adjacent and all(x[k]==c[k] for k,_ in axes if k!=axis))
    return neighbours,edges

def write_context_inputs(work,features):
    ctx=features['CONTEXT']
    write_csv(work/'conditional_signal_features.csv',features['LONG_SIGNAL_LOW'].values())
    write_csv(work/'context_h1.csv',(dict(time=iso(b[0]),close_utc=iso(b[0]+H1_BAR),open=b[1],high=b[2],low=b[3],close=b[4],
        atr14=ctx['atr_h1'][j],preceding_100_atr_mean=ctx['vol_means_h1'][100][j],
        preceding_200_atr_mean=ctx['mean_h1'][j],preceding_300_atr_mean=ctx['vol_means_h1'][300][j]) for j,b in enumerate(ctx['h1'])))
    fields=['time','close_utc','open','high','low','close','observed_hours','first_hour','last_hour','atr14','preceding_100_atr_mean','ema100','ema200']
    write_csv(work/'context_utc_daily.csv',(dict(time=iso(b[0]),close_utc=iso(b[0]+timedelta(days=1)),open=b[1],high=b[2],low=b[3],close=b[4],
        observed_hours=b[5],first_hour=iso(b[6]),last_hour=iso(b[7]),atr14=ctx['atr_d1'][j],
        preceding_100_atr_mean=ctx['mean_d1'][j],ema100=ctx['ema_d1'][100][j],ema200=ctx['ema_d1'][200][j]) for j,b in enumerate(ctx['daily'])),fields)

CONTEXT_KEYS=CONTEXT_KEYS+tuple(vol_field(tf,w) for tf in ('M15','H1') for w in (100,300))

PARENT_IDS=tuple(c["config_id"] for c in make_configs()[0] if c["role"]=="CONTROL" or c["family"]=="RECLAIM_SINGLE")
COMPARISON_IDS=PARENT_IDS




if __name__=='__main__':
    raise SystemExit(main())
