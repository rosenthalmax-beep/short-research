{
  "status": "PASS",
  "input_file": "EURCHF_H1_SHORT_PASS6_RR_STANDALONE_RESULTS.zip",
  "input_sha256": "5f2d8902f4a7feb4fa81b00a857c188d80e62d3625e1c025e735107397b79a6b",
  "delivered_runner_sha256": "e404c682fa47187b0d7cd4f81990420adacfc6f1cfc39db2b52ab65739d6dbad",
  "returned_runner_sha256": "8395afc000fd33d8c7cebe6b0ad7b4801f3d54f935af5dfe1f176bc16b0cb914",
  "runner_difference": "CRLF line endings only; exact normalized bytes and AST parity",
  "source_sha256": "9c8b5279629daee868ad924f52e998be7f6ce638a15e106ffb4851a2df76feac",
  "historical_rule_rr_settings": 18,
  "historical_cases": 108,
  "historical_accepted_rows": 9534,
  "independent_first_barrier_paths": 2124,
  "historical_gates": 179,
  "checks": [
    {
      "check": "Exact bearish engulf accepts boundary equality",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Previous doji rejected",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Prior extreme excludes signal high",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Prior high agrees with direct slices LB1",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Prior high agrees with direct slices LB2",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Prior high agrees with direct slices LB3",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Prior high agrees with direct slices LB5",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Wilder ATR seed and recursion",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Exactly two frozen entry rules, original LB60",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Primary tight structure and central close thresholds frozen",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Existing simpler frequency comparator frozen",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Body/range fixed; no hidden signal condition",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Bounded predeclared nine-level RR neighbourhood",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Complete declared 108-case universe",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Twelve archived RR3 full-ledger controls decode with strict coverage",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Primary inclusive predicate boundaries",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Previous close condition enforced",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Comparator has no previous-close condition",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Primary absolute structure bound enforced",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Different RR recomputes first target barriers",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Different RR recomputes occupancy and displaced later entry",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Dual-touch stop-first diagnostic differs from archived heuristic",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Adverse opening gap can lose more than 1R",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Known opening-gap exit uses opening timestamp",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Favorable target gap gives no improvement",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Open trade occupies p0 through data end",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Exit-candle signal can re-enter",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Invalid high-cost entry does not occupy p0",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Cost-specific replay preserves valid lower-cost entry",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "No new trade at exclusive data cutoff",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Unclosed trade has no invented realized R",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Empty case remains explicit",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Closed R drawdown chronology",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Losing streak reset",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Complete zero-inclusive monthly and period universe",
      "status": "PASS",
      "evidence": "SYNTHETIC_SOFTWARE_ONLY"
    },
    {
      "check": "Python compilation",
      "status": "PASS"
    },
    {
      "check": "Preserved Pass5 mechanics: atr14",
      "status": "PASS"
    },
    {
      "check": "Preserved Pass5 mechanics: previous_high",
      "status": "PASS"
    },
    {
      "check": "Preserved Pass5 mechanics: bearish_engulf",
      "status": "PASS"
    },
    {
      "check": "Preserved Pass5 mechanics: base_make_features",
      "status": "PASS"
    },
    {
      "check": "Preserved Pass5 mechanics: make_features",
      "status": "PASS"
    },
    {
      "check": "Preserved Pass5 mechanics: base_selected_indices",
      "status": "PASS"
    },
    {
      "check": "Preserved Pass5 mechanics: selected_indices",
      "status": "PASS"
    },
    {
      "check": "Preserved Pass5 mechanics: geometry",
      "status": "PASS"
    },
    {
      "check": "Preserved Pass5 mechanics: r_value",
      "status": "PASS"
    },
    {
      "check": "Preserved Pass5 mechanics: replay",
      "status": "PASS"
    },
    {
      "check": "Preserved Pass5 mechanics: source_hash",
      "status": "PASS"
    },
    {
      "check": "Preserved Pass5 mechanics: signal_hash",
      "status": "PASS"
    },
    {
      "check": "Preserved Pass5 mechanics: fetch_history",
      "status": "PASS"
    },
    {
      "check": "Preserved Pass5 mechanics: hard_controls",
      "status": "PASS"
    },
    {
      "check": "Preserved Pass5 mechanics: accepted_row",
      "status": "PASS"
    },
    {
      "check": "Preserved Pass5 mechanics: stats",
      "status": "PASS"
    },
    {
      "check": "Preserved Pass5 mechanics: period_definitions",
      "status": "PASS"
    },
    {
      "check": "Preserved Pass5 mechanics: month_bounds",
      "status": "PASS"
    },
    {
      "check": "First-barrier mechanics preserved; only RR becomes explicit argument",
      "status": "PASS"
    },
    {
      "check": "Production archive integrity",
      "status": "PASS"
    },
    {
      "check": "Manifest declares complete bounded108 cases and fixed entries without chosen RR",
      "status": "PASS"
    },
    {
      "check": "Returned source differs only by CRLF; exact normalized bytes and AST",
      "status": "PASS"
    },
    {
      "check": "Returned raw source fingerprint matches manifest",
      "status": "PASS"
    },
    {
      "check": "Archived protocol exactly matches delivered frozen study",
      "status": "PASS"
    },
    {
      "check": "Every packaged file hash and byte count reconciles",
      "status": "PASS"
    },
    {
      "check": "All35 packaged synthetic controls pass",
      "status": "PASS"
    },
    {
      "check": "All manifest output row counts verified against actual CSV contents",
      "status": "PASS"
    },
    {
      "check": "179 strict archived source/screen/RR3 historical checks pass",
      "status": "PASS"
    },
    {
      "check": "Independent frozen OHLC canonical hash and coverage",
      "status": "PASS"
    },
    {
      "check": "Independent OHLC eligibility reconstructs exact two frozen entry streams",
      "status": "PASS"
    },
    {
      "check": "Entry feature prefix causality at signal 207",
      "status": "PASS"
    },
    {
      "check": "Entry feature prefix causality at signal 63037",
      "status": "PASS"
    },
    {
      "check": "Entry feature prefix causality at signal 137789",
      "status": "PASS"
    },
    {
      "check": "All exported9669 signal features independently match OHLC",
      "status": "PASS"
    },
    {
      "check": "All18 qualifying memberships preserve immutable entry predicates at everyRR",
      "status": "PASS"
    },
    {
      "check": "All2124 RR/model first barriers independently reconstruct exact OHLC prices/reasons/times",
      "status": "PASS"
    },
    {
      "check": "All108 cost/model/RR eligibility, p0 chronologies, complete row fields, hashes and aggregates independently reconcile",
      "status": "PASS"
    },
    {
      "check": "All12 RR3 production summary hashes match ACTUAL supplied Pass5 full ledger and raw hashes",
      "status": "PASS"
    },
    {
      "check": "All28188 zero-inclusive monthly rows independently reconcile",
      "status": "PASS"
    },
    {
      "check": "All3780 calendar/era/latest/CHF rows independently reconcile",
      "status": "PASS"
    },
    {
      "check": "All77112 rolling12/24/36 windows include zeros and independently reconcile",
      "status": "PASS"
    },
    {
      "check": "Every fixed_rr3_comparisons.csv exact case references and common/add/remove/R attribution reconciles",
      "status": "PASS"
    },
    {
      "check": "Every adjacent_rr_comparisons.csv exact case references and common/add/remove/R attribution reconciles",
      "status": "PASS"
    },
    {
      "check": "Every alternative_entry_comparisons.csv exact case references and common/add/remove/R attribution reconciles",
      "status": "PASS"
    },
    {
      "check": "Every execution_model_comparisons.csv exact case references and common/add/remove/R attribution reconciles",
      "status": "PASS"
    },
    {
      "check": "Every cost_sensitivity_comparisons.csv exact case references and common/add/remove/R attribution reconciles",
      "status": "PASS"
    },
    {
      "check": "Every marginal added/removed/common-changed record exactly reconciles in order",
      "status": "PASS"
    },
    {
      "check": "All108 review influence/eras/latest/equivalence values reconcile",
      "status": "PASS"
    },
    {
      "check": "All108 same-entry RR neighbourhoods, explicit boundaries and overlaps reconcile",
      "status": "PASS"
    },
    {
      "check": "All12 full-nine-level RR axis reports reconcile without winner selection",
      "status": "PASS"
    },
    {
      "check": "CHF exposure rows use actual accepted trade clocks",
      "status": "PASS"
    },
    {
      "check": "Accepted normalized and denormalized row counts agree",
      "status": "PASS"
    }
  ],
  "broker_requests_made": false,
  "live_orders_or_account_access": false,
  "unseen_validation": false,
  "scope": "Read-only supplied Pass6 same-history reconciliation: independent OHLC eligibility, first barriers and108 p0/cost chronologies/full fields/hashes, all period/comparison reports. Not later final frozen-strategy independent implementation or prospective validation.",
  "final_freeze": {
    "status": "FINAL_RULES_FROZEN_FOR_INDEPENDENT_CONFIRMATION",
    "date": "2026-10-02",
    "rules": [
      {
        "strategy_id": "EURCHF_H1_SHORT_PRIMARY_L060_D0075_SC035_PC075_RR3P50_V1",
        "config_id": "A__JOINT_CONFIRMATION__L060__D0075__SCMAX0035__PCMIN0075",
        "role": "PRIMARY",
        "pair": "EUR_CHF",
        "direction": "SHORT",
        "timeframe": "H1",
        "rr": 3.5,
        "lookback": 60,
        "distance_atr": 0.075,
        "body_min_atr": 0.75,
        "range_min_atr": 1.25,
        "signal_close_max": 0.35,
        "previous_close_min": 0.75
      },
      {
        "strategy_id": "EURCHF_H1_SHORT_FREQUENCY_L060_D0100_SC035_NO_PC_RR3P00_V1",
        "config_id": "A__SIGNAL_CLOSE_LOCATION__LE__N000__T0035",
        "role": "FREQUENCY_COMPARATOR",
        "pair": "EUR_CHF",
        "direction": "SHORT",
        "timeframe": "H1",
        "rr": 3.0,
        "lookback": 60,
        "distance_atr": 0.1,
        "body_min_atr": 0.75,
        "range_min_atr": 1.25,
        "signal_close_max": 0.35,
        "previous_close_min": null
      }
    ],
    "source_sha256": "9c8b5279629daee868ad924f52e998be7f6ce638a15e106ffb4851a2df76feac",
    "candles": 137819,
    "first_candle": "2005-01-02T18:00:00Z",
    "last_candle": "2026-09-30T23:00:00Z",
    "start": "2005-01-01T00:00:00Z",
    "end_exclusive": "2026-10-01T00:00:00Z",
    "price_type": "HISTORICAL_OANDA_MID",
    "source_csv_file_sha256": "862bf19a2e6a0e8d3c27b961c68c9fc0d104a5d64bd6eaa87fc2665afa38c196",
    "atr_period": 14,
    "atr_initialization": "TR of bars1 through14 inclusive, arithmetic average; Wilder recursion thereafter",
    "warmup_observed_bars": 200,
    "engulf_definition": "previous close>open; signal close<open; signal open>=previous close; signal close<=previous open",
    "prior_high_definition": "max of prior60 completed observed highs, excludes signal",
    "structure_definition": "ABS(signal high-prior high)/signal completed ATR14",
    "close_location_definition": "(close-low)/(high-low); signal and previous completed candle",
    "tick": 1e-05,
    "pip": 0.0001,
    "stop_buffer_ticks": 10,
    "reference_entry_definition": "signal close at signal timestamp plus1hour",
    "stop_definition": "signal high +10*tick",
    "target_definition": "reference close - frozenRR*(stop-reference close)",
    "assumed_fill_definition": "reference close - cost_ticks*tick",
    "r_denominator": "stop-assumed fill",
    "cost_ticks": [
      10,
      20,
      40
    ],
    "execution_models": [
      "SCREEN_PARITY",
      "STOP_FIRST_GAP_STRESS"
    ],
    "pyramiding_zero": true,
    "open_trade_policy": "occupies p0 through cutoff; no invented exit or realizedR",
    "exit_candle_reentry": true,
    "screen_dual_touch": "nearest-open extreme assumed first; tie stop; barrier fills and candle-end exit",
    "stress_execution": "opening at/above stop=max(open,stop); opening at/below target capped at target; other dual touch stop-first; opening gaps timestamp at open, otherwise candle end",
    "geometry_invalidity": "reject entry>=cutoff or unless0<target<assumed_fill<stop",
    "parent_results_zip_sha256": "5f2d8902f4a7feb4fa81b00a857c188d80e62d3625e1c025e735107397b79a6b",
    "canonical_accepted_hash_schema": "accepted_row original schema; sorted JSON keys, compact separators, Python finite float values/None, newline per row; outer rr excluded; original config_id retained",
    "independent_confirmation_cases": 12,
    "independent_confirmation_complete": false,
    "entry_and_rr_search_closed": true,
    "portfolio_admission_complete": false,
    "prospective_validation_complete": false,
    "live_authorization": false,
    "history_is_in_sample": true,
    "selection_rationale": "PrimaryRR3.50 central in shared outcome band3.25\u20133.75; avoids choosing historical peak3.75 adjacent to4.00 outcome reversals. FrequencyRR3 kept as original coherent-era activity control; no higher-RR search or allocation stack."
  },
  "frozen_reference_validation": {
    "status": "PASS",
    "cases": 12,
    "canonical_full_ledger_hashes_verified": 12,
    "accepted_rows": 1062,
    "qualifying_memberships": 177,
    "first_barrier_paths": 354,
    "archive_sha256": "f0e8e9b1378a8f9cc85053d6733e9e702e6ee5c1b74c8c203ea01835bd695e2a"
  },
  "synthetic_controls": 35
}
