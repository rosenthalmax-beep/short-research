# Bounded AUD/USD path coverage check

Exactly 25 previously reviewed unresolved paths from M_D1.25_B1.0 are checked.
No new strategy, filter, RR, account replay, financing or order is introduced.
This report contains NEW historical candle evidence as retrieved now. Earlier
matrix and reconciliation files remain unchanged; there is no revised CAGR.

Frozen M15 windows must match the original canonical SHA256. Available old raw
windows are opened read-only; evicted windows may be downloaded in full to prove
the checksum. Any revision is refused. New M1 candles span entry through the
old observed M15 exit bar only, in <=24-hour requests. S5 covers the old exit
parent first and at most five other earliest critical M15 parents per case.
Budgets: 240 cache-miss requests / 400 HTTP attempts across restarts,
six S5 parents per case, 16 MiB compressed private evidence, 64 MiB free reserve.
Unresolved cases are retained even when a budget or historical coverage ends.

Frozen M15 bars and observed M1 bars that do not touch either barrier prove
non-touch intervals, so absent child candles inside those intervals need not
be invented. Critical missing minutes require complete S5 coverage; absent S5
intervals remain unknown. No forward-filling, zero-volume bars or assumptions
that an absent candle means an unchanged price. The original conventional NY
weekend assumption is counted separately; historical holidays are not guessed.

Crosschecks cover MBA OHLC and execution-bid barrier consistency between tiers.
Complete children must aggregate within one tick; even a sub-tick/tick price
difference that changes barrier reachability is explicitly a source conflict.
Resolution is conditional on observed candle OHLC and the same stop/target/open
proxy semantics. It is not tick-order or broker-fill proof. Non-rollover,
single-barrier M1 fallback follows the old model only for paths previously at
M1 resolution. Original S5, rollover and ambiguous cases still require finer
evidence. An S5 candle touching both barriers remains ambiguous.

`coarse_native_outcome_only` can identify the stop/target side from frozen M15
without proving executable exit price or release timing. It is diagnostic,
not an account-ready correction. If any source conflict exists the case stays
unresolved. New fine candles are not claimed to equal old finer-tier snapshots
whose bodies were evicted; the packet records their checksums and new receipts.

path_coverage.csv is the overview; case_details.jsonl.gz contains full evidence
decisions. evidence_index.json records each retained body and provenance;
candles/ includes only this bounded audit's checked canonical bodies. Inputs
and code hashes tie checkpoints to the reviewed 25 cases. Resolved paths need
a later explicitly versioned replay before any account/portfolio claim changes.
The historical matrix found no passing plateau; this check does not select one.

The candle API schema was checked against OANDA's official instrument definition
and oanda/v20-openapi instrument endpoint schema. Candle absence is not evidence
of no market activity. No untouched out-of-sample claim is made.

Official API references:
- https://developer.oanda.com/rest-live-v20/instrument-df/
- https://github.com/oanda/v20-openapi/blob/master/yaml/separate/v20_instrument.yaml
