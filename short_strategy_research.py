#!/usr/bin/env python3
"""EUR/USD H4 LONG Pass 8. Standard-library Python 3.10+. Research ONLY.
Save as app.py in a separate service. Start: python app.py
GET /status -> COMPLETE; GET /results -> report ZIP. No order endpoints.
"""
import argparse
import bisect
import collections
import csv
import datetime as dt
import hashlib
import io
import itertools
import json
import math
import os
from pathlib import Path
import shutil
import statistics
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

VERSION = 'EURUSD_H4_LONG_PASS8_HYBRID_VOLATILITY_BOUNDARY_V1_2026_10_12'
UTC = dt.timezone.utc
START = int(dt.datetime(2005, 1, 1, tzinfo=UTC).timestamp())
END = int(dt.datetime(2026, 10, 12, tzinfo=UTC).timestamp())
H4 = 14400
PIP = .0001
TICK = .00001
RR = 3.0
LB = (20, 30, 35, 40, 45, 50, 60, 100, 150, 200)
DIST = (.05, .10, .20, .35, .50)
BODY = (.50, .75, 1., 1.25)
RANGE = (.75, 1., 1.50)
MODELS = ('STOP_FIRST_GAP_STRESS', 'NEAREST_OPEN_SENSITIVITY')
CASES = [(f'MID_{p}P_{m}', 'MID', p, m) for p in (1, 2, 4) for m in MODELS]
CASES += [(f'BIDASK_BOUND10_{m}', 'BIDASK', 0, m) for m in MODELS]
SOURCE_NAME = 'EURUSD_H4_PASS1_FROZEN_DATA.zip'
RESULT_NAME = 'EURUSD_H4_LONG_PASS8_RESULTS.zip'
PROTOCOL = '# EUR/USD H4 LONG — Pass 8 H4 hybrid volatility boundary\nVersion: EURUSD_H4_LONG_PASS8_HYBRID_VOLATILITY_BOUNDARY_V1_2026_10_12\nProtocol fixed before assessing new cap returns. Full pinned 2005-01-01 through\n2026-10-12 00:00UTC exclusive source,35952H4/138197H1. Same examined history;\nthis is bounded exploratory boundary work, not fresh OOS or portfolio admission.\n\n## Fixed strategy and three new cap hypotheses\nPRIMARY: exact previous-red/current-green body engulf, inclusive open<=previous\nclose and close>=previous open, positive ranges; LB40 absolute signal-low/prior-low\ndistance<=.50 current H4 ATR, body>=1.40ATR, range>=1.50ATR, body/range>=.60.\nFREQUENCY: identical except LB20. Prior low excludes signal; common200bar warmup.\nATR14 TR1..14 mean then causal Wilder through current signal. Fixed RR2.75.\nH4_VOL=current signal H4 ATR14 divided by mean previous60 H4 ATR14; denominator\nexcludes current ATR. Five hybrid caps0.90/1.00/1.10/1.20/1.25, with THREE new\nvalues0.90/1.00/1.20. Existing1.10 branch and weak1.25 are exact Pass7 controls.\nNo other signal/stop/RR/volatility-memory changes or calendar/daily/new triggers.\n\nEvery hybrid: PRIMARY signal OR (FREQUENCY signal AND H4_VOL<=cap). Shared\nsignal submitted once, with one position per combined stream. No assumption\nthat PRIMARY is a FREQUENCY subset. Primary bypasses cap/missing context; extras\nrequire available H4_VOL. Earliest eligible signal enters; no future primary\nreservation. Extra entries can still block later primary entries. Replay those\ndisplacements and all later unlocked opportunities chronologically.\n\n8 definitions /64execution cases: PRIMARY and FREQUENCY controls, one matched\nHYBRID H4-availability-only control, five cap versions. Three new hypotheses /\n24new execution cases; five existing controls /40cases. Availability is diagnostic:\nPRIMARY OR (FREQUENCY AND H4_VOL available), not another optimisation candidate.\n0.90 and1.25 are tested extremes; report boundary status and all weak neighbours.\nKeep original primary and1.10 hybrid frozen references; do not select the largest\nhistorical R automatically. This single-factor check resolves the lower-boundary\nquestion, without reopening geometry, RR or feature combinations.\n\n## Source/control and independent execution gates\nNative unsmoothed OANDA EUR_USD H4 MID/BID/ASK, UTC alignment0; native H1 MID\nreproduces H4 aggregation. Preserve gaps/partial bins and original price hashes.\nReuse EURUSD_H4_PASS1_FROZEN_DATA.zip; a missing archive permits only a same-\ninterval refetch matching old hashes. Explicit corrupt/missing source fails.\nAll legacy raw/Pass1 anchors,104Pass2,16Pass3,16Pass4,16Pass5 RR3 and16Pass6\nRR2.75 pins retained. Additionally reproduce ALL18archived Pass7 definitions /\n144cases, including previous daily/whole-stream versions strictly as parity\nreferences, not new selectable research. Require full accepted-ledger hashes,\nraw membership/counts, reject/block counts and R. Any mismatch fails closed.\nIndependent scalar H4/daily context plus direct entry masks verify all8study\ndefinitions; independent complete accepted/rejected/blocked replay for64cases.\nInherited completed-day diagnostics retain Pass7UTC/200observed-day convention;\nthey do not influence any new entry in this pass. Old144mask/ledger controls\nare reported separately and are not additional independent confirmations.\n\n## Unchanged execution conventions\nStop=signal MID low-0.0001. Target=MID signal close +2.75*(MID close-stop), fixed\nbefore entry costs; realised R=(exit-fill)/(fill-stop). Six MID cases use assumed\nBUY penalties1/2/4pips, not measured normal spreads. STOP_FIRST_GAP_STRESS keeps\nadverse opening stop gaps (loss can exceed1R), caps favourable target gaps, and\nresolves dual touch stop-first. MID NEAREST_OPEN_SENSITIVITY ignores opening gaps\nand uses nearer low/high to open (stop on tie); a sensitivity, not known sequencing.\nTwo BIDASK approximations enter at next observed H4 ASK open, first constituent\nH1 timestamp proxy. Reject age>120s, adverse entry>10ticks (1pip), invalid\nentry/stop/target or missing next bar. BID exits; both models keep stop gaps and\ntarget-gap caps, differing only on dual touch. Signal bar never inspected for exit.\nEntry at/after cutoff rejected; one open trade, exit-candle signal allowed;\nrejections do not occupy. Open-at-cutoff retains occupation and blank realised R.\nHistorical candle quotes do not prove executable live fills/latency/intrabar order.\nNo financing, commissions or live tick rounding model; no account/order calls.\n\n\n## Reports and interpretation\n64summary /1984period /16768monthly /45696rolling12/24/36M rows, including zeros,\npartial2026/October, four eras, trailing1/2/3/5/10years and every calendar year.\nEntry periods[start,end), exit periods(start,end]; nominal H4 bin END exit time.\nClosed additive R drawdown, not NAV/marked/open risk. No incumbent portfolio replay.\nComplete source/features/context/provenance/membership/path/accepted/rejected/\nblocked/open ledgers; count primary/extra accepted and primary signals blocked.\n26comparison pairs x8cases=208comparison rows; 6448annual/era/trailing deltas.\nCompare each cap against original primary, unfiltered frequency, matched\navailability and frozen1.10 hybrid; adjacent cap comparisons report full P0\ncommon/added/removed R plus blocked-trade attribution. Keep losers/weak periods\nvisible. Compare added contribution across costs, older/recent eras and rolling\nwindows; avoid accepting an isolated spike or gains from a couple of removed\nlosers. Overlapping rules/repeatedly inspected history are not independent OOS.\nNo automatic filter stacking, RR retuning, next boundary extension or live orders.\n\n## Operation\nSingle Python3.10+ standard-library app. Save as app.py in the SAME separate\nresearch service used for Pass7; keep existing start command/credentials.\npython app.py or existing WSGI app/application with one worker. Default output\n/data/eurusd_h4_pass8_research if /data exists, otherwise temporary cache.\nReuses source beside app.py or previous Pass7/6/5/4/3/2/1 caches. Optional\nEURUSD_H4_PASS8_FROZEN_DATA / OUTPUT_DIR / AUTOSTART(defaulttrue), with full prefix\nEURUSD_H4_PASS8_ for each. No required new variables/packages.\nGET/HEAD /status /health /results /source; wait COMPLETE then download/results.\nError archives contain diagnostics only. Delivery checks reproduce previously\nexamined historical controls and assess new-cap execution/reporting on synthetic\ndata; new historical cap returns are not evaluated while delivering code.\n'

PASS1_PINS = {'anchors': {'M_L40_D0.35_B0.75_R1': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '106', 'accepted_hash': 'd979d86a2d02e760a8a1c6fafb0bdae84bda735a0edfc7744d75ae817fcba29a', 'blocked': '3', 'net_r': '8.3450542151461', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '8'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '106', 'accepted_hash': 'd979d86a2d02e760a8a1c6fafb0bdae84bda735a0edfc7744d75ae817fcba29a', 'blocked': '3', 'net_r': '8.3450542151461', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '8'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '114', 'accepted_hash': '48f8a41dff9a17722f2de6fc36edc9ef1469b92049306d3f2a07ea4b907e699c', 'blocked': '3', 'net_r': '19.081030949268005', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '114', 'accepted_hash': '78a2087e6dd35db8eecdd723d2c8ba75ff335a568aeacb543e395696ae236958', 'blocked': '3', 'net_r': '19.026589688523014', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '114', 'accepted_hash': '03161b82a37fe9af87cbe8f2c961718b18da7e1cea17d85e1bd80bf7a139e7a9', 'blocked': '3', 'net_r': '16.308044523646373', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '114', 'accepted_hash': 'f6c9a160a4b082c827ae188a6230a19c9431c5d746c558c6ae204b2459efbb7a', 'blocked': '3', 'net_r': '16.254372207262186', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '114', 'accepted_hash': '40bba3c1fe2437e77333301b71d69137e459973c633fba67923e86a11fd218a3', 'blocked': '3', 'net_r': '11.152458563507945', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '114', 'accepted_hash': '01b8778e70d74aa68df953647bea0649fedd55383dcd1847aa5d6efa475c0848', 'blocked': '3', 'net_r': '11.100260761310135', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}}, 'M_L40_D0.5_B1.25_R1.5': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '52', 'accepted_hash': '7af667268c5fda50ea8eaf3a902ac2e9868163f08bb4919e188cea8fc459d00d', 'blocked': '1', 'net_r': '7.355862354561232', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '5'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '52', 'accepted_hash': '7af667268c5fda50ea8eaf3a902ac2e9868163f08bb4919e188cea8fc459d00d', 'blocked': '1', 'net_r': '7.355862354561232', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '5'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '57', 'accepted_hash': 'c7660408058093dd74beda9f4445e82cb3a9c934fd51ef9d957c669ab9adafa6', 'blocked': '1', 'net_r': '13.896274044063015', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '57', 'accepted_hash': '3d2d17610f9ea033c348a76eb2dd1ed7b17e42afb0b02e45b18a7d25c8827c4a', 'blocked': '1', 'net_r': '13.841832783318022', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '57', 'accepted_hash': '2237d6a869858b55476744b6c270088252e3915dbfbe9661f3adfc4c89d9c923', 'blocked': '1', 'net_r': '12.831981788009386', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '57', 'accepted_hash': 'c1eccd64fd9c8dbfafeb1754e1e40388071d28fbef4845b9dc999aeab951dcb9', 'blocked': '1', 'net_r': '12.7783094716252', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '57', 'accepted_hash': '8064ca9c491dad8154834cb65ae2ccb50291e1ff36c5ead6807d8634cda37405', 'blocked': '1', 'net_r': '10.81186068156517', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '57', 'accepted_hash': '7ebb1926f01bda5d7bdc48dee803ee0a1f0dd80eee5bf901edf6b0eec994a02c', 'blocked': '1', 'net_r': '10.75966287936736', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}}}, 'raw_controls': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': 1407, 'blocked': 1065, 'independent_parity': 'PASS', 'ledger_sha256': '6dcb73d2b7ed43b9e6da129d40bed9af12693f58dba015af9808f1e30a9c6929', 'raw_signals': 2669, 'rejected': 197}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': 1407, 'blocked': 1065, 'independent_parity': 'PASS', 'ledger_sha256': 'a9ddf63bdce468287f3f8655f8da82e20a4e89638f07d3d764d130eb68ffdc36', 'raw_signals': 2669, 'rejected': 197}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': 1512, 'blocked': 1157, 'independent_parity': 'PASS', 'ledger_sha256': '77659c43e40af3d7e14f392825771ea5331488e695165de183ddaef54df0329a', 'raw_signals': 2669, 'rejected': 0}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': 1512, 'blocked': 1157, 'independent_parity': 'PASS', 'ledger_sha256': '3b56b148b22f70b7904e0d70ec391a48ae6897ce119340119c1d3f0989266ad5', 'raw_signals': 2669, 'rejected': 0}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': 1512, 'blocked': 1157, 'independent_parity': 'PASS', 'ledger_sha256': 'fe8932a92e7bcbc7c179d1f615fc787d970035555c01a715a1afbf02edfc29bf', 'raw_signals': 2669, 'rejected': 0}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': 1512, 'blocked': 1157, 'independent_parity': 'PASS', 'ledger_sha256': 'f690dd8becdf7df86835afa98ae4b30bc88ca719a82e01a1967a74a6e4973042', 'raw_signals': 2669, 'rejected': 0}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': 1512, 'blocked': 1157, 'independent_parity': 'PASS', 'ledger_sha256': '163d14184269d601d935c3e68c7bf33540145b8b796889e5543dd9c9e3bbc27b', 'raw_signals': 2669, 'rejected': 0}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': 1512, 'blocked': 1157, 'independent_parity': 'PASS', 'ledger_sha256': '5beec9478053c6350ede8faa8a00683c3c549b1779fb2bf4ea10e61e1650e7a6', 'raw_signals': 2669, 'rejected': 0}}, 'source': {'alignment': 'UTC_00_UNSMOOTHED', 'end': 1791763200, 'instrument': 'EUR_USD', 'members': {'h1.json': '258c40216bef3deb5a74bfaaa2403b4bd9f4a88533dc734c73f9e89a7f9569bb', 'h4.json': '5c7a5ced72379c5696251200c2447dcb142116ed97149d0d4379d42a06381087', 'receipts.json': '0f37599b041b5e45649c81ae6771745af0da3dfd456130c1cb5d3e17c9ae692d'}, 'start': 1104537600}}

PASS2_PINS = {'BROAD__CONTROL': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '106', 'accepted_hash': 'd979d86a2d02e760a8a1c6fafb0bdae84bda735a0edfc7744d75ae817fcba29a', 'blocked': '3', 'net_r': '8.3450542151461', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '8'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '106', 'accepted_hash': 'd979d86a2d02e760a8a1c6fafb0bdae84bda735a0edfc7744d75ae817fcba29a', 'blocked': '3', 'net_r': '8.3450542151461', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '8'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '114', 'accepted_hash': '48f8a41dff9a17722f2de6fc36edc9ef1469b92049306d3f2a07ea4b907e699c', 'blocked': '3', 'net_r': '19.081030949268005', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '114', 'accepted_hash': '78a2087e6dd35db8eecdd723d2c8ba75ff335a568aeacb543e395696ae236958', 'blocked': '3', 'net_r': '19.026589688523014', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '114', 'accepted_hash': '03161b82a37fe9af87cbe8f2c961718b18da7e1cea17d85e1bd80bf7a139e7a9', 'blocked': '3', 'net_r': '16.308044523646373', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '114', 'accepted_hash': 'f6c9a160a4b082c827ae188a6230a19c9431c5d746c558c6ae204b2459efbb7a', 'blocked': '3', 'net_r': '16.254372207262186', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '114', 'accepted_hash': '40bba3c1fe2437e77333301b71d69137e459973c633fba67923e86a11fd218a3', 'blocked': '3', 'net_r': '11.152458563507945', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '114', 'accepted_hash': '01b8778e70d74aa68df953647bea0649fedd55383dcd1847aa5d6efa475c0848', 'blocked': '3', 'net_r': '11.100260761310135', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}}, 'BROAD__H4_VOL_LE_1': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '37', 'accepted_hash': 'a645d7817f780ea1710d5af766c311323e8cc3db89eae1e16147eb67201678e8', 'blocked': '0', 'net_r': '6.202223906365382', 'raw_signal_hash': '5004ab2e942e9c9fa51828995e5e084ad3231f0ad2d619a5da248066e9f0cceb', 'rejected': '3'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '37', 'accepted_hash': 'a645d7817f780ea1710d5af766c311323e8cc3db89eae1e16147eb67201678e8', 'blocked': '0', 'net_r': '6.202223906365382', 'raw_signal_hash': '5004ab2e942e9c9fa51828995e5e084ad3231f0ad2d619a5da248066e9f0cceb', 'rejected': '3'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '40', 'accepted_hash': '62dba3d369214963624a57a203b2b16527c9fb56d17c32cf02a9e83d1b2cb896', 'blocked': '0', 'net_r': '6.781459534622908', 'raw_signal_hash': '5004ab2e942e9c9fa51828995e5e084ad3231f0ad2d619a5da248066e9f0cceb', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '40', 'accepted_hash': '62dba3d369214963624a57a203b2b16527c9fb56d17c32cf02a9e83d1b2cb896', 'blocked': '0', 'net_r': '6.781459534622908', 'raw_signal_hash': '5004ab2e942e9c9fa51828995e5e084ad3231f0ad2d619a5da248066e9f0cceb', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '40', 'accepted_hash': '58a32b8994555cdb69f0ec51959d41056454ad15f6c978e9a4f6f1e3da4c2512', 'blocked': '0', 'net_r': '5.631101006071993', 'raw_signal_hash': '5004ab2e942e9c9fa51828995e5e084ad3231f0ad2d619a5da248066e9f0cceb', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '40', 'accepted_hash': '58a32b8994555cdb69f0ec51959d41056454ad15f6c978e9a4f6f1e3da4c2512', 'blocked': '0', 'net_r': '5.631101006071993', 'raw_signal_hash': '5004ab2e942e9c9fa51828995e5e084ad3231f0ad2d619a5da248066e9f0cceb', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '40', 'accepted_hash': '321601ee89c9c94f409c890583307529b5067c1337b943a472382d72d0ae85b1', 'blocked': '0', 'net_r': '3.511516823115995', 'raw_signal_hash': '5004ab2e942e9c9fa51828995e5e084ad3231f0ad2d619a5da248066e9f0cceb', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '40', 'accepted_hash': '321601ee89c9c94f409c890583307529b5067c1337b943a472382d72d0ae85b1', 'blocked': '0', 'net_r': '3.511516823115995', 'raw_signal_hash': '5004ab2e942e9c9fa51828995e5e084ad3231f0ad2d619a5da248066e9f0cceb', 'rejected': '0'}}, 'BROAD__H4_VOL_LE_1.25': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '93', 'accepted_hash': '6e0cf1c25984d1eec676381476dec33d63e8b1a289c998ee1903f70d24f43bb5', 'blocked': '3', 'net_r': '13.374488435310514', 'raw_signal_hash': 'ff4114fb3bf00b334fbb6420e0f7bcf6627dc5b770daadeaa70b657e6145e328', 'rejected': '7'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '93', 'accepted_hash': '6e0cf1c25984d1eec676381476dec33d63e8b1a289c998ee1903f70d24f43bb5', 'blocked': '3', 'net_r': '13.374488435310514', 'raw_signal_hash': 'ff4114fb3bf00b334fbb6420e0f7bcf6627dc5b770daadeaa70b657e6145e328', 'rejected': '7'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '100', 'accepted_hash': '5d0b47de10950a104436db5dc15cbc4c3904a09019f37f3e73f8703a050c183a', 'blocked': '3', 'net_r': '25.157684982025433', 'raw_signal_hash': 'ff4114fb3bf00b334fbb6420e0f7bcf6627dc5b770daadeaa70b657e6145e328', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '100', 'accepted_hash': 'e4276877b6e268b94930b2879c41dc0e8f37a24393a68de53e8376981e4eeab4', 'blocked': '3', 'net_r': '25.10324372128044', 'raw_signal_hash': 'ff4114fb3bf00b334fbb6420e0f7bcf6627dc5b770daadeaa70b657e6145e328', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '100', 'accepted_hash': '303871ff5634da18b8493f34cb59bb6385b65ee8a3358cba27f3dee58ffef33c', 'blocked': '3', 'net_r': '22.45979258048011', 'raw_signal_hash': 'ff4114fb3bf00b334fbb6420e0f7bcf6627dc5b770daadeaa70b657e6145e328', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '100', 'accepted_hash': '5640d47ecf355969a81f3dd343c9d35a3db5b22313fad300849a96d5e3340cc8', 'blocked': '3', 'net_r': '22.40612026409592', 'raw_signal_hash': 'ff4114fb3bf00b334fbb6420e0f7bcf6627dc5b770daadeaa70b657e6145e328', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '100', 'accepted_hash': '4fb4bc80b1a512ef04866ad503fda24c24a27a36bdfc9dcdc4ec9fa3784548e3', 'blocked': '3', 'net_r': '17.449911789052493', 'raw_signal_hash': 'ff4114fb3bf00b334fbb6420e0f7bcf6627dc5b770daadeaa70b657e6145e328', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '100', 'accepted_hash': 'd927687e7086b6932d3e8b1c53cb7fdb3160b8ffd85b64351e8bc3d438e4fdc7', 'blocked': '3', 'net_r': '17.397713986854686', 'raw_signal_hash': 'ff4114fb3bf00b334fbb6420e0f7bcf6627dc5b770daadeaa70b657e6145e328', 'rejected': '0'}}, 'BROAD__SIGNAL_CLOSE_GE_0.7': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '98', 'accepted_hash': '55b711afc1db3ee8ead7d349f93cfdae78e58544b8933605c77600f06bde72d6', 'blocked': '2', 'net_r': '8.482642230127402', 'raw_signal_hash': '186c6af59bc1f0b6522d0f3ef9f0025ae41b7b480687cafc1f9e8bfbb1abb3f8', 'rejected': '7'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '98', 'accepted_hash': '55b711afc1db3ee8ead7d349f93cfdae78e58544b8933605c77600f06bde72d6', 'blocked': '2', 'net_r': '8.482642230127402', 'raw_signal_hash': '186c6af59bc1f0b6522d0f3ef9f0025ae41b7b480687cafc1f9e8bfbb1abb3f8', 'rejected': '7'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '105', 'accepted_hash': '57ab0ef6cde844b860d3fbbd189ba32d4d8846ac3641fb638ec036b4685734dc', 'blocked': '2', 'net_r': '20.276136769373807', 'raw_signal_hash': '186c6af59bc1f0b6522d0f3ef9f0025ae41b7b480687cafc1f9e8bfbb1abb3f8', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '105', 'accepted_hash': 'e663c29ec6c8d141c8410fb26bc11607ee409326ad4984edf09badea5b609690', 'blocked': '2', 'net_r': '20.221695508628812', 'raw_signal_hash': '186c6af59bc1f0b6522d0f3ef9f0025ae41b7b480687cafc1f9e8bfbb1abb3f8', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '105', 'accepted_hash': 'e0b42f6ca4bf73209691197873a44061e5a93c7063aefe5a3116d010059697eb', 'blocked': '2', 'net_r': '17.688902579739615', 'raw_signal_hash': '186c6af59bc1f0b6522d0f3ef9f0025ae41b7b480687cafc1f9e8bfbb1abb3f8', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '105', 'accepted_hash': 'f13aa42c383af4366b558e261e761959d7b06dbfbd78798b7eb36b5b75cfbfa5', 'blocked': '2', 'net_r': '17.635230263355428', 'raw_signal_hash': '186c6af59bc1f0b6522d0f3ef9f0025ae41b7b480687cafc1f9e8bfbb1abb3f8', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '105', 'accepted_hash': 'f691294f473a40cdac9f87df527290cb0689e61e0e728a9a1f1939017d8dde2d', 'blocked': '2', 'net_r': '12.87934345972578', 'raw_signal_hash': '186c6af59bc1f0b6522d0f3ef9f0025ae41b7b480687cafc1f9e8bfbb1abb3f8', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '105', 'accepted_hash': '3f904571901345eb3fb8e471d02bbae6116c0a6ca848c356c50051e53c5d2ff9', 'blocked': '2', 'net_r': '12.82714565752797', 'raw_signal_hash': '186c6af59bc1f0b6522d0f3ef9f0025ae41b7b480687cafc1f9e8bfbb1abb3f8', 'rejected': '0'}}, 'BROAD__SIGNAL_CLOSE_GE_0.8': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '80', 'accepted_hash': '652b279d261dd3255dca790c38652aaed680c9161fc043cd38b85527848ead88', 'blocked': '1', 'net_r': '10.602814621665539', 'raw_signal_hash': 'afdf58efabf7afc9694ba6f01110c4c5b53de899eada6b17f52b41c630950ed6', 'rejected': '7'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '80', 'accepted_hash': '652b279d261dd3255dca790c38652aaed680c9161fc043cd38b85527848ead88', 'blocked': '1', 'net_r': '10.602814621665539', 'raw_signal_hash': 'afdf58efabf7afc9694ba6f01110c4c5b53de899eada6b17f52b41c630950ed6', 'rejected': '7'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '87', 'accepted_hash': '672e3643d77d566bb67664a0217506d04a00bb5584d51a5c2e15d937a1b13598', 'blocked': '1', 'net_r': '22.61455498129044', 'raw_signal_hash': 'afdf58efabf7afc9694ba6f01110c4c5b53de899eada6b17f52b41c630950ed6', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '87', 'accepted_hash': 'ed066969b6380d5f6ab3adbfb1d7af4898bde82f09bdcdf4c746cbe46323769b', 'blocked': '1', 'net_r': '22.56011372054545', 'raw_signal_hash': 'afdf58efabf7afc9694ba6f01110c4c5b53de899eada6b17f52b41c630950ed6', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '87', 'accepted_hash': '9ae241ae887343c3bec24dd4771e0971e18897b2fe32cb16fa565672c599834f', 'blocked': '1', 'net_r': '20.350270292835564', 'raw_signal_hash': 'afdf58efabf7afc9694ba6f01110c4c5b53de899eada6b17f52b41c630950ed6', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '87', 'accepted_hash': '82e7ca5a171633f89ed55b9f8929aa19317ff8b0df696db2a11389a8b2a8daf8', 'blocked': '1', 'net_r': '20.296597976451377', 'raw_signal_hash': 'afdf58efabf7afc9694ba6f01110c4c5b53de899eada6b17f52b41c630950ed6', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '87', 'accepted_hash': '52831f977c63f33faa20ecf30df30be7a8342375ef476a10d5335a68d7b778bd', 'blocked': '1', 'net_r': '16.144552617164273', 'raw_signal_hash': 'afdf58efabf7afc9694ba6f01110c4c5b53de899eada6b17f52b41c630950ed6', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '87', 'accepted_hash': '84e055c879484655107b1f683f51d4fa42c7153062f11a15e11d7a0a6be88cbc', 'blocked': '1', 'net_r': '16.092354814966463', 'raw_signal_hash': 'afdf58efabf7afc9694ba6f01110c4c5b53de899eada6b17f52b41c630950ed6', 'rejected': '0'}}, 'BROAD__SIGNAL_CLOSE_GE_0.9': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '42', 'accepted_hash': 'dbeb22ee9b2f13945bbe657f10b96013d416cdf6680d2684147bb5e2d7ddd939', 'blocked': '0', 'net_r': '0.9981644055674916', 'raw_signal_hash': 'abb2257715f6dd8a8b15637f808da47d0a3bcad2b3676d2c09a17c6d11a8569b', 'rejected': '4'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '42', 'accepted_hash': 'dbeb22ee9b2f13945bbe657f10b96013d416cdf6680d2684147bb5e2d7ddd939', 'blocked': '0', 'net_r': '0.9981644055674916', 'raw_signal_hash': 'abb2257715f6dd8a8b15637f808da47d0a3bcad2b3676d2c09a17c6d11a8569b', 'rejected': '4'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '46', 'accepted_hash': 'e641217eae8c5738a6e493205b184949c4e59323985b0622fcef4df5737df657', 'blocked': '0', 'net_r': '0.7760775543119975', 'raw_signal_hash': 'abb2257715f6dd8a8b15637f808da47d0a3bcad2b3676d2c09a17c6d11a8569b', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '46', 'accepted_hash': 'c9602d223882002bbc5c4f468e5773f0dea5b6356f704a2276ff57bcbc6f1acf', 'blocked': '0', 'net_r': '0.7216362935670051', 'raw_signal_hash': 'abb2257715f6dd8a8b15637f808da47d0a3bcad2b3676d2c09a17c6d11a8569b', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '46', 'accepted_hash': '6f497215ef18fa8593ebbdd450f5691ed985cf51ea4c983b3680e3a03085f839', 'blocked': '0', 'net_r': '-0.37847679156111314', 'raw_signal_hash': 'abb2257715f6dd8a8b15637f808da47d0a3bcad2b3676d2c09a17c6d11a8569b', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '46', 'accepted_hash': 'fef6950722f13fa7d0c6db1aa39f195b7176e00a80bd45c84bd595c1b48e8ec0', 'blocked': '0', 'net_r': '-0.4321491079453008', 'raw_signal_hash': 'abb2257715f6dd8a8b15637f808da47d0a3bcad2b3676d2c09a17c6d11a8569b', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '46', 'accepted_hash': 'd62083970ad7fa641fbb0f65f4b0744121a04e1c81f488a2de63a119b31a3479', 'blocked': '0', 'net_r': '-2.504361101336658', 'raw_signal_hash': 'abb2257715f6dd8a8b15637f808da47d0a3bcad2b3676d2c09a17c6d11a8569b', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '46', 'accepted_hash': 'fed9d0ca5c6a96322f85a5c7fd89f53ae48a5db78424d4cdfd5ea997d2b4811d', 'blocked': '0', 'net_r': '-2.5565589035344667', 'raw_signal_hash': 'abb2257715f6dd8a8b15637f808da47d0a3bcad2b3676d2c09a17c6d11a8569b', 'rejected': '0'}}, 'STRONG_BODY__BODY_RATIO_GE_0.6': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '49', 'accepted_hash': '64e4bc2cd4cc5804bd014e8fbebd370e28846c028cc4feff7160f03a6f3fa30f', 'blocked': '1', 'net_r': '10.355862354561232', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '4'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '49', 'accepted_hash': '64e4bc2cd4cc5804bd014e8fbebd370e28846c028cc4feff7160f03a6f3fa30f', 'blocked': '1', 'net_r': '10.355862354561232', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '4'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '53', 'accepted_hash': 'f18aeccd8903e39b6f08cd24bb4d9a1b6e1f78bd1b5559cd6eac6487facfe603', 'blocked': '1', 'net_r': '17.896274044063013', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '53', 'accepted_hash': 'bb1d77df80edf62b5e7ea6dafb12171e6d020a4892623e21b92d1b626331eb8c', 'blocked': '1', 'net_r': '17.841832783318022', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '53', 'accepted_hash': 'f51eeb92dc1a030cbaa19e41e9f8093b6a78bcfb73dc36c674d3acdf65cc03a8', 'blocked': '1', 'net_r': '16.831981788009386', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '53', 'accepted_hash': '40896ca6503fda6285abf679ae2f4e640cf43d9efb7247a3f0c3cbb73f6fa972', 'blocked': '1', 'net_r': '16.7783094716252', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '53', 'accepted_hash': '9bdba292b9e7460db24243216a7d608d24922284f652e06918b77673db2df331', 'blocked': '1', 'net_r': '14.81186068156517', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '53', 'accepted_hash': 'dcfd5b1efb118c55eab67aa7aafd5d4768301b720593e53fdddaf23640f313c6', 'blocked': '1', 'net_r': '14.75966287936736', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '0'}}, 'STRONG_BODY__BODY_RATIO_GE_0.7': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '43', 'accepted_hash': 'bd5d186ad60bb27e5d1039aaaf5eeb84ce283605aca38801f187ed8795d1e8c9', 'blocked': '0', 'net_r': '0.4422830119736294', 'raw_signal_hash': 'ae5f253339eda3d1945caa57e18b893693c4db444597038046095f072881aa9e', 'rejected': '2'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '43', 'accepted_hash': 'bd5d186ad60bb27e5d1039aaaf5eeb84ce283605aca38801f187ed8795d1e8c9', 'blocked': '0', 'net_r': '0.4422830119736294', 'raw_signal_hash': 'ae5f253339eda3d1945caa57e18b893693c4db444597038046095f072881aa9e', 'rejected': '2'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '45', 'accepted_hash': 'c06182bcb56ea2cb3bd855705d847e776e7aa7e46e1ccd62c5890ff4135f26c6', 'blocked': '0', 'net_r': '10.097419773232925', 'raw_signal_hash': 'ae5f253339eda3d1945caa57e18b893693c4db444597038046095f072881aa9e', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '45', 'accepted_hash': '937cfe9e322e9fa26f69e2b8cf1edaa0d70c78bad3d38e91957a417dc5463385', 'blocked': '0', 'net_r': '10.042978512487934', 'raw_signal_hash': 'ae5f253339eda3d1945caa57e18b893693c4db444597038046095f072881aa9e', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '45', 'accepted_hash': '3514e59bb744fc4c304584c4c35725739fa87baef49476654fdc1b9ac16d4824', 'blocked': '0', 'net_r': '9.2290488077607', 'raw_signal_hash': 'ae5f253339eda3d1945caa57e18b893693c4db444597038046095f072881aa9e', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '45', 'accepted_hash': '5e1ff76f5a8d9e2a6d60da37ff150c7a654a96a022933efe2feaab0607711a60', 'blocked': '0', 'net_r': '9.175376491376511', 'raw_signal_hash': 'ae5f253339eda3d1945caa57e18b893693c4db444597038046095f072881aa9e', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '45', 'accepted_hash': 'c4428e8bb19016b370fa551565b5febe5a77a5f32a788a72920ed0f6bb37c00d', 'blocked': '0', 'net_r': '7.585917295663544', 'raw_signal_hash': 'ae5f253339eda3d1945caa57e18b893693c4db444597038046095f072881aa9e', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '45', 'accepted_hash': '1fbad545ca620300841e423e7ea70d28b322228bc45785c6a1d1a8be44086326', 'blocked': '0', 'net_r': '7.5337194934657346', 'raw_signal_hash': 'ae5f253339eda3d1945caa57e18b893693c4db444597038046095f072881aa9e', 'rejected': '0'}}, 'STRONG_BODY__BODY_RATIO_GE_0.8': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '23', 'accepted_hash': '6010774e18cfaac62d4aab3418d0f125fd5d3829a13398df4f7681fea50da36b', 'blocked': '0', 'net_r': '-7.2613025795370625', 'raw_signal_hash': '5c38b825627d07579e811e2172e28c9d08db197e41756d83cdb20652e5df9ae3', 'rejected': '1'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '23', 'accepted_hash': '6010774e18cfaac62d4aab3418d0f125fd5d3829a13398df4f7681fea50da36b', 'blocked': '0', 'net_r': '-7.2613025795370625', 'raw_signal_hash': '5c38b825627d07579e811e2172e28c9d08db197e41756d83cdb20652e5df9ae3', 'rejected': '1'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '24', 'accepted_hash': 'c2726013c7a8df263e47473a58dba1930a9294f9b03b260823295819ff8ab48f', 'blocked': '0', 'net_r': '-0.4630606816277534', 'raw_signal_hash': '5c38b825627d07579e811e2172e28c9d08db197e41756d83cdb20652e5df9ae3', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '24', 'accepted_hash': 'c2726013c7a8df263e47473a58dba1930a9294f9b03b260823295819ff8ab48f', 'blocked': '0', 'net_r': '-0.4630606816277534', 'raw_signal_hash': '5c38b825627d07579e811e2172e28c9d08db197e41756d83cdb20652e5df9ae3', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '24', 'accepted_hash': 'e70394f56d6011658204b5753cb2fa044c2a5036228f3175975added653de2a1', 'blocked': '0', 'net_r': '-0.9044910369475363', 'raw_signal_hash': '5c38b825627d07579e811e2172e28c9d08db197e41756d83cdb20652e5df9ae3', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '24', 'accepted_hash': 'e70394f56d6011658204b5753cb2fa044c2a5036228f3175975added653de2a1', 'blocked': '0', 'net_r': '-0.9044910369475363', 'raw_signal_hash': '5c38b825627d07579e811e2172e28c9d08db197e41756d83cdb20652e5df9ae3', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '24', 'accepted_hash': 'f17956d1e71466a74f115dc9658d529a099efe5ffaa132745a65a70c5daa5cd1', 'blocked': '0', 'net_r': '-1.7292955108199983', 'raw_signal_hash': '5c38b825627d07579e811e2172e28c9d08db197e41756d83cdb20652e5df9ae3', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '24', 'accepted_hash': 'f17956d1e71466a74f115dc9658d529a099efe5ffaa132745a65a70c5daa5cd1', 'blocked': '0', 'net_r': '-1.7292955108199983', 'raw_signal_hash': '5c38b825627d07579e811e2172e28c9d08db197e41756d83cdb20652e5df9ae3', 'rejected': '0'}}, 'STRONG_BODY__CONTROL': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '52', 'accepted_hash': '7af667268c5fda50ea8eaf3a902ac2e9868163f08bb4919e188cea8fc459d00d', 'blocked': '1', 'net_r': '7.355862354561232', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '5'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '52', 'accepted_hash': '7af667268c5fda50ea8eaf3a902ac2e9868163f08bb4919e188cea8fc459d00d', 'blocked': '1', 'net_r': '7.355862354561232', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '5'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '57', 'accepted_hash': 'c7660408058093dd74beda9f4445e82cb3a9c934fd51ef9d957c669ab9adafa6', 'blocked': '1', 'net_r': '13.896274044063015', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '57', 'accepted_hash': '3d2d17610f9ea033c348a76eb2dd1ed7b17e42afb0b02e45b18a7d25c8827c4a', 'blocked': '1', 'net_r': '13.841832783318022', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '57', 'accepted_hash': '2237d6a869858b55476744b6c270088252e3915dbfbe9661f3adfc4c89d9c923', 'blocked': '1', 'net_r': '12.831981788009386', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '57', 'accepted_hash': 'c1eccd64fd9c8dbfafeb1754e1e40388071d28fbef4845b9dc999aeab951dcb9', 'blocked': '1', 'net_r': '12.7783094716252', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '57', 'accepted_hash': '8064ca9c491dad8154834cb65ae2ccb50291e1ff36c5ead6807d8634cda37405', 'blocked': '1', 'net_r': '10.81186068156517', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '57', 'accepted_hash': '7ebb1926f01bda5d7bdc48dee803ee0a1f0dd80eee5bf901edf6b0eec994a02c', 'blocked': '1', 'net_r': '10.75966287936736', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}}, 'STRONG_BODY__SIGNAL_CLOSE_GE_0.7': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '50', 'accepted_hash': '487fd277b5bc4f80ad84a8f78705f8b597757ac4f35bcf7c353dd5a79f35554a', 'blocked': '0', 'net_r': '9.355862354561232', 'raw_signal_hash': 'a1e1f34854873fb36555db773ab10d18327096770512b18db97bcba742e03fd6', 'rejected': '5'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '50', 'accepted_hash': '487fd277b5bc4f80ad84a8f78705f8b597757ac4f35bcf7c353dd5a79f35554a', 'blocked': '0', 'net_r': '9.355862354561232', 'raw_signal_hash': 'a1e1f34854873fb36555db773ab10d18327096770512b18db97bcba742e03fd6', 'rejected': '5'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '55', 'accepted_hash': '3269881261a79297501c7f10042360c28acfba8677812f2537e4ec553ff2af82', 'blocked': '0', 'net_r': '15.896274044063015', 'raw_signal_hash': 'a1e1f34854873fb36555db773ab10d18327096770512b18db97bcba742e03fd6', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '55', 'accepted_hash': 'bb82d092975ff90ce7f01c7234bafbb7f7e5cb97e3c5a69b2fc91350ccb07f4b', 'blocked': '0', 'net_r': '15.841832783318022', 'raw_signal_hash': 'a1e1f34854873fb36555db773ab10d18327096770512b18db97bcba742e03fd6', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '55', 'accepted_hash': '20d025c39e39275308856363d8dcf5baa6c086a7a3c95fab0cacc531254309e1', 'blocked': '0', 'net_r': '14.831981788009386', 'raw_signal_hash': 'a1e1f34854873fb36555db773ab10d18327096770512b18db97bcba742e03fd6', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '55', 'accepted_hash': '305dd2311cee5027261785724424ffe876a7eb5190149e7eaccb455d1c75c447', 'blocked': '0', 'net_r': '14.7783094716252', 'raw_signal_hash': 'a1e1f34854873fb36555db773ab10d18327096770512b18db97bcba742e03fd6', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '55', 'accepted_hash': 'beeffc7f9182891de98783f076a85a561af99b510bfe7a6df15175f4de72a15c', 'blocked': '0', 'net_r': '12.81186068156517', 'raw_signal_hash': 'a1e1f34854873fb36555db773ab10d18327096770512b18db97bcba742e03fd6', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '55', 'accepted_hash': '002ef3602e872636f2530b619fd09fddf741c548b13ff3228adbbd7019e04320', 'blocked': '0', 'net_r': '12.75966287936736', 'raw_signal_hash': 'a1e1f34854873fb36555db773ab10d18327096770512b18db97bcba742e03fd6', 'rejected': '0'}}, 'STRONG_BODY__SIGNAL_CLOSE_GE_0.8': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '42', 'accepted_hash': 'c7c00d68bdc3d999a7270127ce58af6fd807976832184c8b73a69c66ecb9043f', 'blocked': '0', 'net_r': '9.440780224376145', 'raw_signal_hash': '4cc22e45f55750af82b875e6a422c1ca64cc62fd0af98065a34bb8caf5326304', 'rejected': '5'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '42', 'accepted_hash': 'c7c00d68bdc3d999a7270127ce58af6fd807976832184c8b73a69c66ecb9043f', 'blocked': '0', 'net_r': '9.440780224376145', 'raw_signal_hash': '4cc22e45f55750af82b875e6a422c1ca64cc62fd0af98065a34bb8caf5326304', 'rejected': '5'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '47', 'accepted_hash': '59f777dd341f73adb0d222b4db5f505d46a6a393b129623ebae539c7cc1c307f', 'blocked': '0', 'net_r': '16.017033831961598', 'raw_signal_hash': '4cc22e45f55750af82b875e6a422c1ca64cc62fd0af98065a34bb8caf5326304', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '47', 'accepted_hash': 'e4ed8a82cd4923786b9a0a828e3b59a7cd5f598e458859bf8b1473e0e02ddf32', 'blocked': '0', 'net_r': '15.962592571216605', 'raw_signal_hash': '4cc22e45f55750af82b875e6a422c1ca64cc62fd0af98065a34bb8caf5326304', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '47', 'accepted_hash': 'a5186e5028e681e24058acd0af3775a2d979a67ed5415c6c5960b6bf0c342d47', 'blocked': '0', 'net_r': '15.069896596884464', 'raw_signal_hash': '4cc22e45f55750af82b875e6a422c1ca64cc62fd0af98065a34bb8caf5326304', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '47', 'accepted_hash': '78a04bd5ec7e90877cf446a2c8da317897f28edf99224d66449575c07aeedf54', 'blocked': '0', 'net_r': '15.016224280500277', 'raw_signal_hash': '4cc22e45f55750af82b875e6a422c1ca64cc62fd0af98065a34bb8caf5326304', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '47', 'accepted_hash': '9cb922df717cd55a5a3fc1adabc3d1be0018260a0289671f690412f5a7a48c39', 'blocked': '0', 'net_r': '13.273900303955758', 'raw_signal_hash': '4cc22e45f55750af82b875e6a422c1ca64cc62fd0af98065a34bb8caf5326304', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '47', 'accepted_hash': '2e183123fd31e5e41334865bbe6c9b7be0fb24033b16c7a63e04741ffb7226cc', 'blocked': '0', 'net_r': '13.22170250175795', 'raw_signal_hash': '4cc22e45f55750af82b875e6a422c1ca64cc62fd0af98065a34bb8caf5326304', 'rejected': '0'}}, 'STRONG_BODY__SIGNAL_CLOSE_GE_0.9': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '22', 'accepted_hash': 'cfcb12020cf759b28bf607fe3c4ad5a12f7770ad34a276c2b919da0c8a5c47e9', 'blocked': '0', 'net_r': '-6.326945558237693', 'raw_signal_hash': 'ed9b93b1ad1bd2bb53a1dbdecba8fe80a67f3c0a0c7e20ac793913dd644d855e', 'rejected': '3'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '22', 'accepted_hash': 'cfcb12020cf759b28bf607fe3c4ad5a12f7770ad34a276c2b919da0c8a5c47e9', 'blocked': '0', 'net_r': '-6.326945558237693', 'raw_signal_hash': 'ed9b93b1ad1bd2bb53a1dbdecba8fe80a67f3c0a0c7e20ac793913dd644d855e', 'rejected': '3'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '25', 'accepted_hash': '0fa7750cde5426933415f11e4563c805cb3dab61dd41043cdf0da93a724e283b', 'blocked': '0', 'net_r': '-1.4397096814192643', 'raw_signal_hash': 'ed9b93b1ad1bd2bb53a1dbdecba8fe80a67f3c0a0c7e20ac793913dd644d855e', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '25', 'accepted_hash': '2eb48d2d245dbdfa0bde9c9aac71d94cd543f5ddaae92333f8547b158b71069e', 'blocked': '0', 'net_r': '-1.4941509421642567', 'raw_signal_hash': 'ed9b93b1ad1bd2bb53a1dbdecba8fe80a67f3c0a0c7e20ac793913dd644d855e', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '25', 'accepted_hash': 'dfb9aeee665a49a757afbcd5bc70f5d4e556da69765f0ef2a3a6842f7b4d4863', 'blocked': '0', 'net_r': '-1.8591676713468663', 'raw_signal_hash': 'ed9b93b1ad1bd2bb53a1dbdecba8fe80a67f3c0a0c7e20ac793913dd644d855e', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '25', 'accepted_hash': '8a15328bdc1e0547ecf9ea45c439637bc54216bc889dddb98fbf82e2ffda6b37', 'blocked': '0', 'net_r': '-1.912839987731054', 'raw_signal_hash': 'ed9b93b1ad1bd2bb53a1dbdecba8fe80a67f3c0a0c7e20ac793913dd644d855e', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '25', 'accepted_hash': 'edee406c3bb80b3a8e6fdc0a8bb93d3d5656f588b8e198657763f10a2b0bea0b', 'blocked': '0', 'net_r': '-2.643805338373309', 'raw_signal_hash': 'ed9b93b1ad1bd2bb53a1dbdecba8fe80a67f3c0a0c7e20ac793913dd644d855e', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '25', 'accepted_hash': 'dd66ba7d2f1904b7955550133af59a9c764405ce9228737ce0d935350c33ffcf', 'blocked': '0', 'net_r': '-2.6960031405711176', 'raw_signal_hash': 'ed9b93b1ad1bd2bb53a1dbdecba8fe80a67f3c0a0c7e20ac793913dd644d855e', 'rejected': '0'}}}

PASS3_PINS = {'STRONG_BODY__BODY1.4_BR0.6': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '39', 'accepted_hash': '23eba4a6f8dc075b408a505c733cac5797bf942e769abbbd351117c30730019d', 'blocked': '0', 'net_r': '12.405531755949378', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '2'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '39', 'accepted_hash': '23eba4a6f8dc075b408a505c733cac5797bf942e769abbbd351117c30730019d', 'blocked': '0', 'net_r': '12.405531755949378', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '2'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '41', 'accepted_hash': '7e88868cb5fc83ff5332b14972bd39226ba832f46b6b2db7106e9ff37deef1cb', 'blocked': '0', 'net_r': '18.07828069221303', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '41', 'accepted_hash': '55ceca9c44c019799066e134038310f753069151982e12bec72011095a6b08f7', 'blocked': '0', 'net_r': '18.023839431468037', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '41', 'accepted_hash': '19fa19a6c5328b11702efc070c8306139f3269ae35e7c79f2cbff75c5623a3c0', 'blocked': '0', 'net_r': '17.1903591261877', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '41', 'accepted_hash': '3fd544859e72d87432818b411f43492f09df7e2fa0f22ddb3e0f79ea92df475e', 'blocked': '0', 'net_r': '17.13668680980351', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '41', 'accepted_hash': '0829b365b87cf7eec217df9a82fa2a6fc61e72e784adb15209156e9b69f441ff', 'blocked': '0', 'net_r': '15.507121159434266', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '41', 'accepted_hash': 'd18e5ead650a3c3b92a34767b927fbc87e4c53ebce27825fef8057eb87b0a4eb', 'blocked': '0', 'net_r': '15.454923357236458', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '0'}}, 'STRONG_BODY__BR0.6_CNONE': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '49', 'accepted_hash': '64e4bc2cd4cc5804bd014e8fbebd370e28846c028cc4feff7160f03a6f3fa30f', 'blocked': '1', 'net_r': '10.355862354561232', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '4'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '49', 'accepted_hash': '64e4bc2cd4cc5804bd014e8fbebd370e28846c028cc4feff7160f03a6f3fa30f', 'blocked': '1', 'net_r': '10.355862354561232', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '4'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '53', 'accepted_hash': 'f18aeccd8903e39b6f08cd24bb4d9a1b6e1f78bd1b5559cd6eac6487facfe603', 'blocked': '1', 'net_r': '17.896274044063013', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '53', 'accepted_hash': 'bb1d77df80edf62b5e7ea6dafb12171e6d020a4892623e21b92d1b626331eb8c', 'blocked': '1', 'net_r': '17.841832783318022', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '53', 'accepted_hash': 'f51eeb92dc1a030cbaa19e41e9f8093b6a78bcfb73dc36c674d3acdf65cc03a8', 'blocked': '1', 'net_r': '16.831981788009386', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '53', 'accepted_hash': '40896ca6503fda6285abf679ae2f4e640cf43d9efb7247a3f0c3cbb73f6fa972', 'blocked': '1', 'net_r': '16.7783094716252', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '53', 'accepted_hash': '9bdba292b9e7460db24243216a7d608d24922284f652e06918b77673db2df331', 'blocked': '1', 'net_r': '14.81186068156517', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '53', 'accepted_hash': 'dcfd5b1efb118c55eab67aa7aafd5d4768301b720593e53fdddaf23640f313c6', 'blocked': '1', 'net_r': '14.75966287936736', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '0'}}}

PASS4_PINS = {'QUALITY__L20_D0.5': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '67', 'accepted_hash': '9810d9a3a7d5372aa029f5060e6a92d8a726333e9f61efbcce9c04c6b1b731c2', 'blocked': '4', 'net_r': '12.060247470015808', 'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'rejected': '2'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '67', 'accepted_hash': '9810d9a3a7d5372aa029f5060e6a92d8a726333e9f61efbcce9c04c6b1b731c2', 'blocked': '4', 'net_r': '12.060247470015808', 'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'rejected': '2'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '69', 'accepted_hash': 'bd47367de09136a257d222b8b84638e8670a5ace3552edafb8a325477dbc507b', 'blocked': '4', 'net_r': '17.64051758865844', 'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '69', 'accepted_hash': '78e0cb4ea059cab57e183666d6b3913dd6ad649efef426bee4580ec230b02228', 'blocked': '4', 'net_r': '17.586076327913446', 'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '69', 'accepted_hash': '6aa948f04e5818bea2e1618cf3eed221bc13f8a3cec3b22828d0f474d14d48cb', 'blocked': '4', 'net_r': '16.330926790695298', 'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '69', 'accepted_hash': '85ccb83881d45c7b8315fb491721cf3b95c8ede3077041be546d903ecc7d9dd8', 'blocked': '4', 'net_r': '16.27725447431111', 'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '69', 'accepted_hash': '1b59562bb8fc2a2b8a7611ffb2ccfcd4c5e22cf1b72069922f74147edc0630f3', 'blocked': '4', 'net_r': '13.848819941249246', 'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '69', 'accepted_hash': 'c2fab83caf668a6c72306c1ccd9693412ad73949c9276643731bb52d91c19e0a', 'blocked': '4', 'net_r': '13.796622139051436', 'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'rejected': '0'}}, 'QUALITY__L40_D0.5': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '39', 'accepted_hash': '23eba4a6f8dc075b408a505c733cac5797bf942e769abbbd351117c30730019d', 'blocked': '0', 'net_r': '12.405531755949378', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '2'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '39', 'accepted_hash': '23eba4a6f8dc075b408a505c733cac5797bf942e769abbbd351117c30730019d', 'blocked': '0', 'net_r': '12.405531755949378', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '2'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '41', 'accepted_hash': '7e88868cb5fc83ff5332b14972bd39226ba832f46b6b2db7106e9ff37deef1cb', 'blocked': '0', 'net_r': '18.07828069221303', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '41', 'accepted_hash': '55ceca9c44c019799066e134038310f753069151982e12bec72011095a6b08f7', 'blocked': '0', 'net_r': '18.023839431468037', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '41', 'accepted_hash': '19fa19a6c5328b11702efc070c8306139f3269ae35e7c79f2cbff75c5623a3c0', 'blocked': '0', 'net_r': '17.1903591261877', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '41', 'accepted_hash': '3fd544859e72d87432818b411f43492f09df7e2fa0f22ddb3e0f79ea92df475e', 'blocked': '0', 'net_r': '17.13668680980351', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '41', 'accepted_hash': '0829b365b87cf7eec217df9a82fa2a6fc61e72e784adb15209156e9b69f441ff', 'blocked': '0', 'net_r': '15.507121159434266', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '41', 'accepted_hash': 'd18e5ead650a3c3b92a34767b927fbc87e4c53ebce27825fef8057eb87b0a4eb', 'blocked': '0', 'net_r': '15.454923357236458', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '0'}}}

PASS5_PINS = {'FREQUENCY_ALT__CONTROL': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '67', 'accepted_hash': '9810d9a3a7d5372aa029f5060e6a92d8a726333e9f61efbcce9c04c6b1b731c2', 'blocked': '4', 'net_r': '12.060247470015808', 'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'rejected': '2'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '67', 'accepted_hash': '9810d9a3a7d5372aa029f5060e6a92d8a726333e9f61efbcce9c04c6b1b731c2', 'blocked': '4', 'net_r': '12.060247470015808', 'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'rejected': '2'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '69', 'accepted_hash': 'bd47367de09136a257d222b8b84638e8670a5ace3552edafb8a325477dbc507b', 'blocked': '4', 'net_r': '17.64051758865844', 'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '69', 'accepted_hash': '78e0cb4ea059cab57e183666d6b3913dd6ad649efef426bee4580ec230b02228', 'blocked': '4', 'net_r': '17.586076327913446', 'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '69', 'accepted_hash': '6aa948f04e5818bea2e1618cf3eed221bc13f8a3cec3b22828d0f474d14d48cb', 'blocked': '4', 'net_r': '16.330926790695298', 'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '69', 'accepted_hash': '85ccb83881d45c7b8315fb491721cf3b95c8ede3077041be546d903ecc7d9dd8', 'blocked': '4', 'net_r': '16.27725447431111', 'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '69', 'accepted_hash': '1b59562bb8fc2a2b8a7611ffb2ccfcd4c5e22cf1b72069922f74147edc0630f3', 'blocked': '4', 'net_r': '13.848819941249246', 'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '69', 'accepted_hash': 'c2fab83caf668a6c72306c1ccd9693412ad73949c9276643731bb52d91c19e0a', 'blocked': '4', 'net_r': '13.796622139051436', 'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'rejected': '0'}}, 'LOCAL__L40_D0.5_B1.4': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '39', 'accepted_hash': '23eba4a6f8dc075b408a505c733cac5797bf942e769abbbd351117c30730019d', 'blocked': '0', 'net_r': '12.405531755949378', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '2'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '39', 'accepted_hash': '23eba4a6f8dc075b408a505c733cac5797bf942e769abbbd351117c30730019d', 'blocked': '0', 'net_r': '12.405531755949378', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '2'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '41', 'accepted_hash': '7e88868cb5fc83ff5332b14972bd39226ba832f46b6b2db7106e9ff37deef1cb', 'blocked': '0', 'net_r': '18.07828069221303', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '41', 'accepted_hash': '55ceca9c44c019799066e134038310f753069151982e12bec72011095a6b08f7', 'blocked': '0', 'net_r': '18.023839431468037', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '41', 'accepted_hash': '19fa19a6c5328b11702efc070c8306139f3269ae35e7c79f2cbff75c5623a3c0', 'blocked': '0', 'net_r': '17.1903591261877', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '41', 'accepted_hash': '3fd544859e72d87432818b411f43492f09df7e2fa0f22ddb3e0f79ea92df475e', 'blocked': '0', 'net_r': '17.13668680980351', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '41', 'accepted_hash': '0829b365b87cf7eec217df9a82fa2a6fc61e72e784adb15209156e9b69f441ff', 'blocked': '0', 'net_r': '15.507121159434266', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '41', 'accepted_hash': 'd18e5ead650a3c3b92a34767b927fbc87e4c53ebce27825fef8057eb87b0a4eb', 'blocked': '0', 'net_r': '15.454923357236458', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '0'}}}


def stamp(t):
    return dt.datetime.fromtimestamp(t, UTC).isoformat().replace('+00:00', 'Z')


def epoch(s):
    return int(dt.datetime.fromisoformat(s.replace('Z', '+00:00')).timestamp())


def canonical(x):
    return json.dumps(x, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(x):
    return hashlib.sha256(canonical(x)).hexdigest()


def write_json(p, x):
    Path(p).write_text(json.dumps(x, indent=2, sort_keys=True, allow_nan=False)+'\n')


def require(ok, message):
    if not ok:
        raise ValueError(message)


ANCHORS = (('BROAD','anchor',40,.35,.75,1.),('STRONG_BODY','anchor',40,.50,1.25,1.50))
COMPARATORS = (('FREQUENCY','comparator',40,.50,1.25,1.50),('QUALITY','comparator',40,.50,1.40,1.50))
BASELINES = (('LOCAL','reference',40,.50,1.40,1.50),('FREQUENCY_ALT','reference',20,.50,1.40,1.50))
FROZEN_ENTRIES = (('PRIMARY','frozen_entry',40,.50,1.40,1.50),('FREQUENCY','frozen_entry',20,.50,1.40,1.50))
STUDY_RR = 2.75
RR_LEVELS = (STUDY_RR,)
VOL_LEVELS = {"H4_VOL":(.90,1.00,1.10,1.20,1.25)}
PASS7_DEFINITIONS = [{'anchor': 'PRIMARY', 'availability': False, 'body': 1.4, 'cap': None, 'distance': 0.5, 'family': 'CONTROL', 'feature': None, 'id': 'PRIMARY__CONTROL', 'lookback': 40, 'ratio': 0.6, 'rr': 2.75, 'scope': 'PRIMARY'}, {'anchor': 'FREQUENCY', 'availability': False, 'body': 1.4, 'cap': None, 'distance': 0.5, 'family': 'CONTROL', 'feature': None, 'id': 'FREQUENCY__CONTROL', 'lookback': 20, 'ratio': 0.6, 'rr': 2.75, 'scope': 'ALL'}, {'anchor': 'FREQUENCY', 'availability': True, 'body': 1.4, 'cap': None, 'distance': 0.5, 'family': 'H4_VOL', 'feature': 'h4_atr_ratio', 'id': 'FREQUENCY__H4_VOL_AVAILABLE', 'lookback': 20, 'ratio': 0.6, 'rr': 2.75, 'scope': 'ALL'}, {'anchor': 'FREQUENCY', 'availability': False, 'body': 1.4, 'cap': 1.1, 'distance': 0.5, 'family': 'H4_VOL', 'feature': 'h4_atr_ratio', 'id': 'FREQUENCY__H4_VOL_LE_1.1', 'lookback': 20, 'ratio': 0.6, 'rr': 2.75, 'scope': 'ALL'}, {'anchor': 'FREQUENCY', 'availability': False, 'body': 1.4, 'cap': 1.25, 'distance': 0.5, 'family': 'H4_VOL', 'feature': 'h4_atr_ratio', 'id': 'FREQUENCY__H4_VOL_LE_1.25', 'lookback': 20, 'ratio': 0.6, 'rr': 2.75, 'scope': 'ALL'}, {'anchor': 'FREQUENCY', 'availability': False, 'body': 1.4, 'cap': 1.4, 'distance': 0.5, 'family': 'H4_VOL', 'feature': 'h4_atr_ratio', 'id': 'FREQUENCY__H4_VOL_LE_1.4', 'lookback': 20, 'ratio': 0.6, 'rr': 2.75, 'scope': 'ALL'}, {'anchor': 'PRIMARY', 'availability': True, 'body': 1.4, 'cap': None, 'distance': 0.5, 'family': 'H4_VOL', 'feature': 'h4_atr_ratio', 'id': 'HYBRID__H4_VOL_AVAILABLE', 'lookback': 20, 'ratio': 0.6, 'rr': 2.75, 'scope': 'EXTRAS'}, {'anchor': 'PRIMARY', 'availability': False, 'body': 1.4, 'cap': 1.1, 'distance': 0.5, 'family': 'H4_VOL', 'feature': 'h4_atr_ratio', 'id': 'HYBRID__H4_VOL_LE_1.1', 'lookback': 20, 'ratio': 0.6, 'rr': 2.75, 'scope': 'EXTRAS'}, {'anchor': 'PRIMARY', 'availability': False, 'body': 1.4, 'cap': 1.25, 'distance': 0.5, 'family': 'H4_VOL', 'feature': 'h4_atr_ratio', 'id': 'HYBRID__H4_VOL_LE_1.25', 'lookback': 20, 'ratio': 0.6, 'rr': 2.75, 'scope': 'EXTRAS'}, {'anchor': 'PRIMARY', 'availability': False, 'body': 1.4, 'cap': 1.4, 'distance': 0.5, 'family': 'H4_VOL', 'feature': 'h4_atr_ratio', 'id': 'HYBRID__H4_VOL_LE_1.4', 'lookback': 20, 'ratio': 0.6, 'rr': 2.75, 'scope': 'EXTRAS'}, {'anchor': 'FREQUENCY', 'availability': True, 'body': 1.4, 'cap': None, 'distance': 0.5, 'family': 'DAILY_VOL', 'feature': 'daily_atr_ratio', 'id': 'FREQUENCY__DAILY_VOL_AVAILABLE', 'lookback': 20, 'ratio': 0.6, 'rr': 2.75, 'scope': 'ALL'}, {'anchor': 'FREQUENCY', 'availability': False, 'body': 1.4, 'cap': 0.75, 'distance': 0.5, 'family': 'DAILY_VOL', 'feature': 'daily_atr_ratio', 'id': 'FREQUENCY__DAILY_VOL_LE_0.75', 'lookback': 20, 'ratio': 0.6, 'rr': 2.75, 'scope': 'ALL'}, {'anchor': 'FREQUENCY', 'availability': False, 'body': 1.4, 'cap': 1.0, 'distance': 0.5, 'family': 'DAILY_VOL', 'feature': 'daily_atr_ratio', 'id': 'FREQUENCY__DAILY_VOL_LE_1', 'lookback': 20, 'ratio': 0.6, 'rr': 2.75, 'scope': 'ALL'}, {'anchor': 'FREQUENCY', 'availability': False, 'body': 1.4, 'cap': 1.25, 'distance': 0.5, 'family': 'DAILY_VOL', 'feature': 'daily_atr_ratio', 'id': 'FREQUENCY__DAILY_VOL_LE_1.25', 'lookback': 20, 'ratio': 0.6, 'rr': 2.75, 'scope': 'ALL'}, {'anchor': 'PRIMARY', 'availability': True, 'body': 1.4, 'cap': None, 'distance': 0.5, 'family': 'DAILY_VOL', 'feature': 'daily_atr_ratio', 'id': 'HYBRID__DAILY_VOL_AVAILABLE', 'lookback': 20, 'ratio': 0.6, 'rr': 2.75, 'scope': 'EXTRAS'}, {'anchor': 'PRIMARY', 'availability': False, 'body': 1.4, 'cap': 0.75, 'distance': 0.5, 'family': 'DAILY_VOL', 'feature': 'daily_atr_ratio', 'id': 'HYBRID__DAILY_VOL_LE_0.75', 'lookback': 20, 'ratio': 0.6, 'rr': 2.75, 'scope': 'EXTRAS'}, {'anchor': 'PRIMARY', 'availability': False, 'body': 1.4, 'cap': 1.0, 'distance': 0.5, 'family': 'DAILY_VOL', 'feature': 'daily_atr_ratio', 'id': 'HYBRID__DAILY_VOL_LE_1', 'lookback': 20, 'ratio': 0.6, 'rr': 2.75, 'scope': 'EXTRAS'}, {'anchor': 'PRIMARY', 'availability': False, 'body': 1.4, 'cap': 1.25, 'distance': 0.5, 'family': 'DAILY_VOL', 'feature': 'daily_atr_ratio', 'id': 'HYBRID__DAILY_VOL_LE_1.25', 'lookback': 20, 'ratio': 0.6, 'rr': 2.75, 'scope': 'EXTRAS'}]
PASS7_PINS = {'PRIMARY__CONTROL': {'MID_1P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'accepted_hash': '41943df7be9b08027b99db153a0ade4a6d21c375eefe73e255f61b750b4c4a56', 'raw_signals': '41', 'accepted': '41', 'rejected': '0', 'blocked': '0', 'net_r': '18.00762799056702'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'accepted_hash': '5b8f87b7d0ff59c295deb34333697a9f8579cbda008d6496418607852a85a73b', 'raw_signals': '41', 'accepted': '41', 'rejected': '0', 'blocked': '0', 'net_r': '18.062069251312014'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'accepted_hash': 'f3ef005161c0a14111121497881f0252917b7e0420b5988a11e429c8afb55299', 'raw_signals': '41', 'accepted': '41', 'rejected': '0', 'blocked': '0', 'net_r': '17.105001719629232'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'accepted_hash': '964845b2124c188b32db84c21570c8fd4245f762ba5f5b39a2f7daa840f0796f', 'raw_signals': '41', 'accepted': '41', 'rejected': '0', 'blocked': '0', 'net_r': '17.158674036013423'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'accepted_hash': 'b96bc84cc280d81e44979870bb9449e597583cb1537ef0fef63b0201edfd0135', 'raw_signals': '41', 'accepted': '41', 'rejected': '0', 'blocked': '0', 'net_r': '15.39441787584999'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'accepted_hash': '069e29be5e2f7a341727ecc9f5cbbbbd0c1f3c46d5699353a284de2208d81a95', 'raw_signals': '41', 'accepted': '41', 'rejected': '0', 'blocked': '0', 'net_r': '15.446615678047799'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'accepted_hash': '86618f514c1cedf08adaa1e4954459498845ae6fcf581c9b62795ca9ffa1089e', 'raw_signals': '41', 'accepted': '39', 'rejected': '2', 'blocked': '0', 'net_r': '12.883601506636053'}, 'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'accepted_hash': '86618f514c1cedf08adaa1e4954459498845ae6fcf581c9b62795ca9ffa1089e', 'raw_signals': '41', 'accepted': '39', 'rejected': '2', 'blocked': '0', 'net_r': '12.883601506636053'}}, 'FREQUENCY__CONTROL': {'MID_1P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': '52ba52ac3f29dbcdf9539b1cca09af6f80832a0e0d794a82830e9000a3476491', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '18.55040860158382'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'bdc8c385c313cc43720dd417dabd7acee222d23b1b22f16d3a0e46f8c4c28440', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '18.604849862328813'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'e0437a164e6c790dc25c10da93b9ede0abce3c66e50bcea58b49b6a006a07859', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '17.206805483403485'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'a22a45d60cc0a7f379c68521307bf94480d2d2ca06c4531a82e7eb442a668a34', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '17.260477799787672'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': '4cad414a518a789197158a2d92238dcab7828b676571d4ad688e5f5ab2c7b5a7', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '14.659255068639068'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': '51c241a8ef729bc48b465509765de69254e589af4ff0727e87ab80122a4dfa58', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '14.711452870836876'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'e155b07f8c17a8e39ed85a08949b6c796a1466f765746d7ad77471da9d549d52', 'raw_signals': '73', 'accepted': '68', 'rejected': '2', 'blocked': '3', 'net_r': '13.522350554906184'}, 'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'e155b07f8c17a8e39ed85a08949b6c796a1466f765746d7ad77471da9d549d52', 'raw_signals': '73', 'accepted': '68', 'rejected': '2', 'blocked': '3', 'net_r': '13.522350554906184'}}, 'FREQUENCY__H4_VOL_AVAILABLE': {'MID_1P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': '52ba52ac3f29dbcdf9539b1cca09af6f80832a0e0d794a82830e9000a3476491', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '18.55040860158382'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'bdc8c385c313cc43720dd417dabd7acee222d23b1b22f16d3a0e46f8c4c28440', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '18.604849862328813'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'e0437a164e6c790dc25c10da93b9ede0abce3c66e50bcea58b49b6a006a07859', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '17.206805483403485'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'a22a45d60cc0a7f379c68521307bf94480d2d2ca06c4531a82e7eb442a668a34', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '17.260477799787672'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': '4cad414a518a789197158a2d92238dcab7828b676571d4ad688e5f5ab2c7b5a7', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '14.659255068639068'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': '51c241a8ef729bc48b465509765de69254e589af4ff0727e87ab80122a4dfa58', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '14.711452870836876'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'e155b07f8c17a8e39ed85a08949b6c796a1466f765746d7ad77471da9d549d52', 'raw_signals': '73', 'accepted': '68', 'rejected': '2', 'blocked': '3', 'net_r': '13.522350554906184'}, 'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'e155b07f8c17a8e39ed85a08949b6c796a1466f765746d7ad77471da9d549d52', 'raw_signals': '73', 'accepted': '68', 'rejected': '2', 'blocked': '3', 'net_r': '13.522350554906184'}}, 'FREQUENCY__H4_VOL_LE_1.1': {'MID_1P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': 'bc9ff3563d2daaa97c09e75f884ebfbe90dda6d0034293cfbc99086042a3f3f5', 'accepted_hash': '372a140208c8272d5aa6d354a26be74cc070c748d1be0f0dd9c09bf9b801fe6b', 'raw_signals': '46', 'accepted': '45', 'rejected': '0', 'blocked': '1', 'net_r': '13.866163069736201'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': 'bc9ff3563d2daaa97c09e75f884ebfbe90dda6d0034293cfbc99086042a3f3f5', 'accepted_hash': 'bde0c73011f8a6a504be002464051c52fd9a53f0c261a58adab69348a4403032', 'raw_signals': '46', 'accepted': '45', 'rejected': '0', 'blocked': '1', 'net_r': '13.920604330481194'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': 'bc9ff3563d2daaa97c09e75f884ebfbe90dda6d0034293cfbc99086042a3f3f5', 'accepted_hash': 'c5a3d3dc6b3b4e0e06964c721df70de002bd18e5cad997a79b2eb9c9579ffd89', 'raw_signals': '46', 'accepted': '45', 'rejected': '0', 'blocked': '1', 'net_r': '12.831286948413858'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': 'bc9ff3563d2daaa97c09e75f884ebfbe90dda6d0034293cfbc99086042a3f3f5', 'accepted_hash': '08e608c7bdf4c806fcb4ffbfc690b299a3fb04c44abe1cf4e7ef9126d5dbfcf9', 'raw_signals': '46', 'accepted': '45', 'rejected': '0', 'blocked': '1', 'net_r': '12.884959264798045'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': 'bc9ff3563d2daaa97c09e75f884ebfbe90dda6d0034293cfbc99086042a3f3f5', 'accepted_hash': 'd9a892cc0afb85727a453ddadcaf97af9de7b08579d934dc6b99c2b3d0734f2b', 'raw_signals': '46', 'accepted': '45', 'rejected': '0', 'blocked': '1', 'net_r': '10.881084193182915'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': 'bc9ff3563d2daaa97c09e75f884ebfbe90dda6d0034293cfbc99086042a3f3f5', 'accepted_hash': '06c8abee58057ef9879a0f4fa131abdd35adb77f27c8ddd403e9a89bcc47d0d2', 'raw_signals': '46', 'accepted': '45', 'rejected': '0', 'blocked': '1', 'net_r': '10.933281995380723'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': 'bc9ff3563d2daaa97c09e75f884ebfbe90dda6d0034293cfbc99086042a3f3f5', 'accepted_hash': '3b8caeb89796d893330228897716bdf415e27ddc4f88864806bf9fa42229ae99', 'raw_signals': '46', 'accepted': '44', 'rejected': '1', 'blocked': '1', 'net_r': '11.492236992073043'}, 'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': 'bc9ff3563d2daaa97c09e75f884ebfbe90dda6d0034293cfbc99086042a3f3f5', 'accepted_hash': '3b8caeb89796d893330228897716bdf415e27ddc4f88864806bf9fa42229ae99', 'raw_signals': '46', 'accepted': '44', 'rejected': '1', 'blocked': '1', 'net_r': '11.492236992073043'}}, 'FREQUENCY__H4_VOL_LE_1.25': {'MID_1P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': 'dce0188edce2651a770520c682bcfafe18effddaab574d960148fcb1ba43e1d9', 'accepted_hash': 'ea4713533f6a15e2cbe7ec477fa869de8c91c51ade443b5e83d7021f89e9e705', 'raw_signals': '63', 'accepted': '62', 'rejected': '0', 'blocked': '1', 'net_r': '15.392368290646232'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': 'dce0188edce2651a770520c682bcfafe18effddaab574d960148fcb1ba43e1d9', 'accepted_hash': '7b2165eedd6d2174ce942bb776214eadc6f39852ae279b8a4a867a364afb3462', 'raw_signals': '63', 'accepted': '62', 'rejected': '0', 'blocked': '1', 'net_r': '15.446809551391224'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': 'dce0188edce2651a770520c682bcfafe18effddaab574d960148fcb1ba43e1d9', 'accepted_hash': 'bf415ce8b0570a36dab5118a191c2b8223bea080494ca2635adfebcd040ca25a', 'raw_signals': '63', 'accepted': '62', 'rejected': '0', 'blocked': '1', 'net_r': '14.139223821629047'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': 'dce0188edce2651a770520c682bcfafe18effddaab574d960148fcb1ba43e1d9', 'accepted_hash': 'f9340449859e5b8f73372a56d841c1ddf648466c940ae737d511cbc8f6a6c627', 'raw_signals': '63', 'accepted': '62', 'rejected': '0', 'blocked': '1', 'net_r': '14.192896138013234'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': 'dce0188edce2651a770520c682bcfafe18effddaab574d960148fcb1ba43e1d9', 'accepted_hash': 'c71a0be75b5a33f541e6101f03cf2250d375ae39fc62e2afa6dd3371bea400da', 'raw_signals': '63', 'accepted': '62', 'rejected': '0', 'blocked': '1', 'net_r': '11.768232229556892'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': 'dce0188edce2651a770520c682bcfafe18effddaab574d960148fcb1ba43e1d9', 'accepted_hash': '389fd356c29d71e145a0eaf85977f290904dcd05f762b85628d9a55c70369627', 'raw_signals': '63', 'accepted': '62', 'rejected': '0', 'blocked': '1', 'net_r': '11.8204300317547'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': 'dce0188edce2651a770520c682bcfafe18effddaab574d960148fcb1ba43e1d9', 'accepted_hash': '51275c7ce760b994065c127f2cbfa8441ee8b8c96601a995bba174e7eb153991', 'raw_signals': '63', 'accepted': '61', 'rejected': '1', 'blocked': '1', 'net_r': '13.10070287516246'}, 'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': 'dce0188edce2651a770520c682bcfafe18effddaab574d960148fcb1ba43e1d9', 'accepted_hash': '51275c7ce760b994065c127f2cbfa8441ee8b8c96601a995bba174e7eb153991', 'raw_signals': '63', 'accepted': '61', 'rejected': '1', 'blocked': '1', 'net_r': '13.10070287516246'}}, 'FREQUENCY__H4_VOL_LE_1.4': {'MID_1P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': 'eeb6507b268502afd04341231a7a5ed4157c2d3260ef9def99ccb2b989815c5d', 'accepted_hash': '7ea78cc0630fc5e649886321b9c072dd921adb00be777f5b595a798a9c94c830', 'raw_signals': '71', 'accepted': '69', 'rejected': '0', 'blocked': '2', 'net_r': '19.55040860158382'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': 'eeb6507b268502afd04341231a7a5ed4157c2d3260ef9def99ccb2b989815c5d', 'accepted_hash': '786ef02a81c4460b7582c71b894f6889a06ffc632643580881d1989999cbd2c1', 'raw_signals': '71', 'accepted': '69', 'rejected': '0', 'blocked': '2', 'net_r': '19.604849862328813'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': 'eeb6507b268502afd04341231a7a5ed4157c2d3260ef9def99ccb2b989815c5d', 'accepted_hash': '36950de7b94b2c10015b4fe85de072f3f572dcdf9e25c64029cc5eb86f35ece9', 'raw_signals': '71', 'accepted': '69', 'rejected': '0', 'blocked': '2', 'net_r': '18.206805483403485'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': 'eeb6507b268502afd04341231a7a5ed4157c2d3260ef9def99ccb2b989815c5d', 'accepted_hash': '2a2c10ae72481ab625a399a199433ad1deacfd3d0ed1e0cd2ab380a79f754075', 'raw_signals': '71', 'accepted': '69', 'rejected': '0', 'blocked': '2', 'net_r': '18.260477799787672'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': 'eeb6507b268502afd04341231a7a5ed4157c2d3260ef9def99ccb2b989815c5d', 'accepted_hash': '777ec44a79e42461fee50c38b10aeee3074ecd01c700ac17aaa8b654bee581e5', 'raw_signals': '71', 'accepted': '69', 'rejected': '0', 'blocked': '2', 'net_r': '15.659255068639068'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': 'eeb6507b268502afd04341231a7a5ed4157c2d3260ef9def99ccb2b989815c5d', 'accepted_hash': 'b4e96640d03afc2a82ce812cf47db086bcb137b9176c9494928091324c90ebef', 'raw_signals': '71', 'accepted': '69', 'rejected': '0', 'blocked': '2', 'net_r': '15.711452870836876'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': 'eeb6507b268502afd04341231a7a5ed4157c2d3260ef9def99ccb2b989815c5d', 'accepted_hash': 'e1703cdab0b96aae27b6f7c5790a9c042827293c611994ea60b3d233e1ae40e4', 'raw_signals': '71', 'accepted': '67', 'rejected': '2', 'blocked': '2', 'net_r': '14.522350554906184'}, 'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': 'eeb6507b268502afd04341231a7a5ed4157c2d3260ef9def99ccb2b989815c5d', 'accepted_hash': 'e1703cdab0b96aae27b6f7c5790a9c042827293c611994ea60b3d233e1ae40e4', 'raw_signals': '71', 'accepted': '67', 'rejected': '2', 'blocked': '2', 'net_r': '14.522350554906184'}}, 'HYBRID__H4_VOL_AVAILABLE': {'MID_1P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': '52ba52ac3f29dbcdf9539b1cca09af6f80832a0e0d794a82830e9000a3476491', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '18.55040860158382'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'bdc8c385c313cc43720dd417dabd7acee222d23b1b22f16d3a0e46f8c4c28440', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '18.604849862328813'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'e0437a164e6c790dc25c10da93b9ede0abce3c66e50bcea58b49b6a006a07859', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '17.206805483403485'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'a22a45d60cc0a7f379c68521307bf94480d2d2ca06c4531a82e7eb442a668a34', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '17.260477799787672'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': '4cad414a518a789197158a2d92238dcab7828b676571d4ad688e5f5ab2c7b5a7', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '14.659255068639068'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': '51c241a8ef729bc48b465509765de69254e589af4ff0727e87ab80122a4dfa58', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '14.711452870836876'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'e155b07f8c17a8e39ed85a08949b6c796a1466f765746d7ad77471da9d549d52', 'raw_signals': '73', 'accepted': '68', 'rejected': '2', 'blocked': '3', 'net_r': '13.522350554906184'}, 'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'e155b07f8c17a8e39ed85a08949b6c796a1466f765746d7ad77471da9d549d52', 'raw_signals': '73', 'accepted': '68', 'rejected': '2', 'blocked': '3', 'net_r': '13.522350554906184'}}, 'HYBRID__H4_VOL_LE_1.1': {'MID_1P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '4b863be992fec7fb3cf10e7372a1b930f21b3a2945f95044ec98f910b4ffbbcd', 'accepted_hash': 'e2c892b760328f64617cf52abce2235134bd2b5a48b3a39fe8069147142a9041', 'raw_signals': '60', 'accepted': '59', 'rejected': '0', 'blocked': '1', 'net_r': '22.123093784004144'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '4b863be992fec7fb3cf10e7372a1b930f21b3a2945f95044ec98f910b4ffbbcd', 'accepted_hash': '1e1dd411405ba323ae8f0b0c2db5e2000468beb66a89a725dfa550be87bf3292', 'raw_signals': '60', 'accepted': '59', 'rejected': '0', 'blocked': '1', 'net_r': '22.177535044749135'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '4b863be992fec7fb3cf10e7372a1b930f21b3a2945f95044ec98f910b4ffbbcd', 'accepted_hash': '1d4ecd99394528d1cc7b28ce9dbaef9bafa405f3eb0505039379a147a5c6ca9b', 'raw_signals': '60', 'accepted': '59', 'rejected': '0', 'blocked': '1', 'net_r': '20.85078766450359'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '4b863be992fec7fb3cf10e7372a1b930f21b3a2945f95044ec98f910b4ffbbcd', 'accepted_hash': '51967621ca0948c98a6daa1dd8b69e4192e073fb92bda2d1395de069e7946571', 'raw_signals': '60', 'accepted': '59', 'rejected': '0', 'blocked': '1', 'net_r': '20.90445998088778'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '4b863be992fec7fb3cf10e7372a1b930f21b3a2945f95044ec98f910b4ffbbcd', 'accepted_hash': 'be9aa8f4f854e2da8b895e51ef05c8519d489ed455e79dca3ae2368254ba4642', 'raw_signals': '60', 'accepted': '59', 'rejected': '0', 'blocked': '1', 'net_r': '18.441821607965178'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '4b863be992fec7fb3cf10e7372a1b930f21b3a2945f95044ec98f910b4ffbbcd', 'accepted_hash': '3e0da9c868f79ef22ae16ba88eb51a297e0bf349aace2bbf5440307053e433c1', 'raw_signals': '60', 'accepted': '59', 'rejected': '0', 'blocked': '1', 'net_r': '18.494019410162984'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '4b863be992fec7fb3cf10e7372a1b930f21b3a2945f95044ec98f910b4ffbbcd', 'accepted_hash': '8cdc7e9a254d2738f04ac1ed6087af481160295bacaf4d7018826ce77243d170', 'raw_signals': '60', 'accepted': '57', 'rejected': '2', 'blocked': '1', 'net_r': '17.105152858625825'}, 'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '4b863be992fec7fb3cf10e7372a1b930f21b3a2945f95044ec98f910b4ffbbcd', 'accepted_hash': '8cdc7e9a254d2738f04ac1ed6087af481160295bacaf4d7018826ce77243d170', 'raw_signals': '60', 'accepted': '57', 'rejected': '2', 'blocked': '1', 'net_r': '17.105152858625825'}}, 'HYBRID__H4_VOL_LE_1.25': {'MID_1P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '2654eeaef4e8764e2d51375d33f35ea41fdf149903c6bfb29fc9a494685b6839', 'accepted_hash': '194d506b39154d6578fecc51e930a525ff49a0074b1d9ea383d91993db10213e', 'raw_signals': '68', 'accepted': '66', 'rejected': '0', 'blocked': '2', 'net_r': '18.83521499612535'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '2654eeaef4e8764e2d51375d33f35ea41fdf149903c6bfb29fc9a494685b6839', 'accepted_hash': 'd5ed66fa70e5acb2951ab2c2d029c5a8a167b7547f5738158e9a14686fac6db5', 'raw_signals': '68', 'accepted': '66', 'rejected': '0', 'blocked': '2', 'net_r': '18.889656256870346'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '2654eeaef4e8764e2d51375d33f35ea41fdf149903c6bfb29fc9a494685b6839', 'accepted_hash': '0ee00e25804ae82b93ae25f48eb65e477630179a7eeeb4270bcbf0adf1c63b27', 'raw_signals': '68', 'accepted': '66', 'rejected': '0', 'blocked': '2', 'net_r': '17.525787664503593'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '2654eeaef4e8764e2d51375d33f35ea41fdf149903c6bfb29fc9a494685b6839', 'accepted_hash': '9684cbfc2f6fea4ff1e3205033ac0f97050517daa291e4f7898f9335ea7eb1cb', 'raw_signals': '68', 'accepted': '66', 'rejected': '0', 'blocked': '2', 'net_r': '17.57945998088778'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '2654eeaef4e8764e2d51375d33f35ea41fdf149903c6bfb29fc9a494685b6839', 'accepted_hash': 'f0bfdacda97d94296888352760b0d69da4b45d77840782f557cc86f3893e5d72', 'raw_signals': '68', 'accepted': '66', 'rejected': '0', 'blocked': '2', 'net_r': '15.044762784435775'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '2654eeaef4e8764e2d51375d33f35ea41fdf149903c6bfb29fc9a494685b6839', 'accepted_hash': 'eabea5e4de46b2e0de5821a431c472424cdfe5baeaa13f9bce450504e0e1e0a2', 'raw_signals': '68', 'accepted': '66', 'rejected': '0', 'blocked': '2', 'net_r': '15.096960586633585'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '2654eeaef4e8764e2d51375d33f35ea41fdf149903c6bfb29fc9a494685b6839', 'accepted_hash': 'ac87d7ef98d16ef39cd1c1c105e4f245720de246ea029489735149281c2332fb', 'raw_signals': '68', 'accepted': '64', 'rejected': '2', 'blocked': '2', 'net_r': '13.832333386009205'}, 'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '2654eeaef4e8764e2d51375d33f35ea41fdf149903c6bfb29fc9a494685b6839', 'accepted_hash': 'ac87d7ef98d16ef39cd1c1c105e4f245720de246ea029489735149281c2332fb', 'raw_signals': '68', 'accepted': '64', 'rejected': '2', 'blocked': '2', 'net_r': '13.832333386009205'}}, 'HYBRID__H4_VOL_LE_1.4': {'MID_1P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': '52ba52ac3f29dbcdf9539b1cca09af6f80832a0e0d794a82830e9000a3476491', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '18.55040860158382'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'bdc8c385c313cc43720dd417dabd7acee222d23b1b22f16d3a0e46f8c4c28440', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '18.604849862328813'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'e0437a164e6c790dc25c10da93b9ede0abce3c66e50bcea58b49b6a006a07859', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '17.206805483403485'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'a22a45d60cc0a7f379c68521307bf94480d2d2ca06c4531a82e7eb442a668a34', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '17.260477799787672'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': '4cad414a518a789197158a2d92238dcab7828b676571d4ad688e5f5ab2c7b5a7', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '14.659255068639068'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': '51c241a8ef729bc48b465509765de69254e589af4ff0727e87ab80122a4dfa58', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '14.711452870836876'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'e155b07f8c17a8e39ed85a08949b6c796a1466f765746d7ad77471da9d549d52', 'raw_signals': '73', 'accepted': '68', 'rejected': '2', 'blocked': '3', 'net_r': '13.522350554906184'}, 'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'e155b07f8c17a8e39ed85a08949b6c796a1466f765746d7ad77471da9d549d52', 'raw_signals': '73', 'accepted': '68', 'rejected': '2', 'blocked': '3', 'net_r': '13.522350554906184'}}, 'FREQUENCY__DAILY_VOL_AVAILABLE': {'MID_1P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '3f45898ca93b4f8aa51bddafeb4b3fb411d4d274e320acb0908e9aacbbafbdc5', 'accepted_hash': '6743b6944edbbf9f9c2c132084eed1778d05dbd5b5c2fa1f10caec760c90a442', 'raw_signals': '72', 'accepted': '69', 'rejected': '0', 'blocked': '3', 'net_r': '19.55040860158382'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '3f45898ca93b4f8aa51bddafeb4b3fb411d4d274e320acb0908e9aacbbafbdc5', 'accepted_hash': '9032e2f37f239b66ebafc4273d94bb959297ccae349bec5d11f59c56978306b5', 'raw_signals': '72', 'accepted': '69', 'rejected': '0', 'blocked': '3', 'net_r': '19.604849862328813'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '3f45898ca93b4f8aa51bddafeb4b3fb411d4d274e320acb0908e9aacbbafbdc5', 'accepted_hash': 'acbffcb3867bc5b1722d356013488aaa1d12f809ce990898a1a1426809d007db', 'raw_signals': '72', 'accepted': '69', 'rejected': '0', 'blocked': '3', 'net_r': '18.206805483403485'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '3f45898ca93b4f8aa51bddafeb4b3fb411d4d274e320acb0908e9aacbbafbdc5', 'accepted_hash': '5902fa9618481724cd37766c34accb249c155c642b87a56f361ece15a36cc5f3', 'raw_signals': '72', 'accepted': '69', 'rejected': '0', 'blocked': '3', 'net_r': '18.260477799787672'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '3f45898ca93b4f8aa51bddafeb4b3fb411d4d274e320acb0908e9aacbbafbdc5', 'accepted_hash': 'a08d4dd4c71d7f501b6fd9cbfc32ab866211aac879ff01e11e8a07cc31508678', 'raw_signals': '72', 'accepted': '69', 'rejected': '0', 'blocked': '3', 'net_r': '15.659255068639068'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '3f45898ca93b4f8aa51bddafeb4b3fb411d4d274e320acb0908e9aacbbafbdc5', 'accepted_hash': '77641efb9c8bf67debfd905bb8f9ee1ab780259aa2fb4824130c08504069351b', 'raw_signals': '72', 'accepted': '69', 'rejected': '0', 'blocked': '3', 'net_r': '15.711452870836876'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '3f45898ca93b4f8aa51bddafeb4b3fb411d4d274e320acb0908e9aacbbafbdc5', 'accepted_hash': 'd5b751a723de8846a9e784f3603ecb05e6ef3cd5b5248ae0cad92c97ed135ea7', 'raw_signals': '72', 'accepted': '67', 'rejected': '2', 'blocked': '3', 'net_r': '14.522350554906184'}, 'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '3f45898ca93b4f8aa51bddafeb4b3fb411d4d274e320acb0908e9aacbbafbdc5', 'accepted_hash': 'd5b751a723de8846a9e784f3603ecb05e6ef3cd5b5248ae0cad92c97ed135ea7', 'raw_signals': '72', 'accepted': '67', 'rejected': '2', 'blocked': '3', 'net_r': '14.522350554906184'}}, 'FREQUENCY__DAILY_VOL_LE_0.75': {'MID_1P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '79a67599654ed71add3ee012f6606494dde5c709f03fe9d3a138c9608cec8c82', 'accepted_hash': 'f2ff606058376ebb939d569d35ac018d2c59bbc83673a7c62c38e215a14525d2', 'raw_signals': '1', 'accepted': '1', 'rejected': '0', 'blocked': '0', 'net_r': '-1.0'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '79a67599654ed71add3ee012f6606494dde5c709f03fe9d3a138c9608cec8c82', 'accepted_hash': 'f2ff606058376ebb939d569d35ac018d2c59bbc83673a7c62c38e215a14525d2', 'raw_signals': '1', 'accepted': '1', 'rejected': '0', 'blocked': '0', 'net_r': '-1.0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '79a67599654ed71add3ee012f6606494dde5c709f03fe9d3a138c9608cec8c82', 'accepted_hash': 'ae2d99694f3a9d9fcb0f48bec63f4be24397be27d465a8efefe24240bf0b5144', 'raw_signals': '1', 'accepted': '1', 'rejected': '0', 'blocked': '0', 'net_r': '-1.0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '79a67599654ed71add3ee012f6606494dde5c709f03fe9d3a138c9608cec8c82', 'accepted_hash': 'ae2d99694f3a9d9fcb0f48bec63f4be24397be27d465a8efefe24240bf0b5144', 'raw_signals': '1', 'accepted': '1', 'rejected': '0', 'blocked': '0', 'net_r': '-1.0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '79a67599654ed71add3ee012f6606494dde5c709f03fe9d3a138c9608cec8c82', 'accepted_hash': 'ee3cb31b16b8cd4ab5c9684ed7ef932665e8fcce71b01591dc04de8ec015f128', 'raw_signals': '1', 'accepted': '1', 'rejected': '0', 'blocked': '0', 'net_r': '-1.0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '79a67599654ed71add3ee012f6606494dde5c709f03fe9d3a138c9608cec8c82', 'accepted_hash': 'ee3cb31b16b8cd4ab5c9684ed7ef932665e8fcce71b01591dc04de8ec015f128', 'raw_signals': '1', 'accepted': '1', 'rejected': '0', 'blocked': '0', 'net_r': '-1.0'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '79a67599654ed71add3ee012f6606494dde5c709f03fe9d3a138c9608cec8c82', 'accepted_hash': 'cc7b7659ae823bf2e2b8cc428bba635d4e3dbb56e5f70e71ce1991a45d82d780', 'raw_signals': '1', 'accepted': '1', 'rejected': '0', 'blocked': '0', 'net_r': '-1.0'}, 'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '79a67599654ed71add3ee012f6606494dde5c709f03fe9d3a138c9608cec8c82', 'accepted_hash': 'cc7b7659ae823bf2e2b8cc428bba635d4e3dbb56e5f70e71ce1991a45d82d780', 'raw_signals': '1', 'accepted': '1', 'rejected': '0', 'blocked': '0', 'net_r': '-1.0'}}, 'FREQUENCY__DAILY_VOL_LE_1': {'MID_1P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '104337c43bc5e950b6207a274581517d4739cea759364461ae3a0d5571561c54', 'accepted_hash': '6a87393b742ead2a04dcf78779e713726317fe863e57c86a409762a602d9aeed', 'raw_signals': '46', 'accepted': '43', 'rejected': '0', 'blocked': '3', 'net_r': '16.050669057650456'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '104337c43bc5e950b6207a274581517d4739cea759364461ae3a0d5571561c54', 'accepted_hash': '6a87393b742ead2a04dcf78779e713726317fe863e57c86a409762a602d9aeed', 'raw_signals': '46', 'accepted': '43', 'rejected': '0', 'blocked': '3', 'net_r': '16.050669057650456'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '104337c43bc5e950b6207a274581517d4739cea759364461ae3a0d5571561c54', 'accepted_hash': '173700f159533af452959336ff27bf318fda6a5f9322a0935bae00cf93b0e4c1', 'raw_signals': '46', 'accepted': '43', 'rejected': '0', 'blocked': '3', 'net_r': '15.137394997685249'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '104337c43bc5e950b6207a274581517d4739cea759364461ae3a0d5571561c54', 'accepted_hash': '173700f159533af452959336ff27bf318fda6a5f9322a0935bae00cf93b0e4c1', 'raw_signals': '46', 'accepted': '43', 'rejected': '0', 'blocked': '3', 'net_r': '15.137394997685249'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '104337c43bc5e950b6207a274581517d4739cea759364461ae3a0d5571561c54', 'accepted_hash': 'ef5e59699848a392acea657c49a437872004e7b07400adb9a18a8c0761315f65', 'raw_signals': '46', 'accepted': '43', 'rejected': '0', 'blocked': '3', 'net_r': '13.409435498704953'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '104337c43bc5e950b6207a274581517d4739cea759364461ae3a0d5571561c54', 'accepted_hash': 'ef5e59699848a392acea657c49a437872004e7b07400adb9a18a8c0761315f65', 'raw_signals': '46', 'accepted': '43', 'rejected': '0', 'blocked': '3', 'net_r': '13.409435498704953'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '104337c43bc5e950b6207a274581517d4739cea759364461ae3a0d5571561c54', 'accepted_hash': '3d03193b9e608466ba47377f888b021599ffc072e20be17a037469186ed6cd57', 'raw_signals': '46', 'accepted': '43', 'rejected': '0', 'blocked': '3', 'net_r': '12.674693473589551'}, 'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '104337c43bc5e950b6207a274581517d4739cea759364461ae3a0d5571561c54', 'accepted_hash': '3d03193b9e608466ba47377f888b021599ffc072e20be17a037469186ed6cd57', 'raw_signals': '46', 'accepted': '43', 'rejected': '0', 'blocked': '3', 'net_r': '12.674693473589551'}}, 'FREQUENCY__DAILY_VOL_LE_1.25': {'MID_1P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7eab09a4a9b0d68659bd9cfbe7f3bd1001ecd7351c81865ee68b5724ef0a28d1', 'accepted_hash': '76c54fafa11c322cc1f6013f2f1b43b49078553cc11620a7d1959c91fe921e34', 'raw_signals': '69', 'accepted': '66', 'rejected': '0', 'blocked': '3', 'net_r': '18.86797616915137'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7eab09a4a9b0d68659bd9cfbe7f3bd1001ecd7351c81865ee68b5724ef0a28d1', 'accepted_hash': '5675124149b07b47eb97b4c42571737ff6a0e4efc84a1710c3531ac759c374d7', 'raw_signals': '69', 'accepted': '66', 'rejected': '0', 'blocked': '3', 'net_r': '18.922417429896363'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7eab09a4a9b0d68659bd9cfbe7f3bd1001ecd7351c81865ee68b5724ef0a28d1', 'accepted_hash': '783022f0928c1ff60623d62d0d942d17f67eb8c6ff9e8e882a854e77d5495668', 'raw_signals': '69', 'accepted': '66', 'rejected': '0', 'blocked': '3', 'net_r': '17.58954884623532'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7eab09a4a9b0d68659bd9cfbe7f3bd1001ecd7351c81865ee68b5724ef0a28d1', 'accepted_hash': '8a3298469426e60607e7a299450912fd3a5d1e53d8bf8373b7be6d88bed06fbe', 'raw_signals': '69', 'accepted': '66', 'rejected': '0', 'blocked': '3', 'net_r': '17.64322116261951'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7eab09a4a9b0d68659bd9cfbe7f3bd1001ecd7351c81865ee68b5724ef0a28d1', 'accepted_hash': 'f659f3f27ac26a12a3a7f86dafadbec678a3a4b9c08004307e898d4558b36856', 'raw_signals': '69', 'accepted': '66', 'rejected': '0', 'blocked': '3', 'net_r': '15.165665325049291'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7eab09a4a9b0d68659bd9cfbe7f3bd1001ecd7351c81865ee68b5724ef0a28d1', 'accepted_hash': '6ee8530e0a1bd8b761cb9ca4c31767f5613aa0e790f886e7bfc8f886ea4ae2be', 'raw_signals': '69', 'accepted': '66', 'rejected': '0', 'blocked': '3', 'net_r': '15.2178631272471'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7eab09a4a9b0d68659bd9cfbe7f3bd1001ecd7351c81865ee68b5724ef0a28d1', 'accepted_hash': '85b23c3b92d4858f9482c97b6a4404c3b702567bb867bbafa0f482c037f014c2', 'raw_signals': '69', 'accepted': '64', 'rejected': '2', 'blocked': '3', 'net_r': '13.826600102826662'}, 'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7eab09a4a9b0d68659bd9cfbe7f3bd1001ecd7351c81865ee68b5724ef0a28d1', 'accepted_hash': '85b23c3b92d4858f9482c97b6a4404c3b702567bb867bbafa0f482c037f014c2', 'raw_signals': '69', 'accepted': '64', 'rejected': '2', 'blocked': '3', 'net_r': '13.826600102826662'}}, 'HYBRID__DAILY_VOL_AVAILABLE': {'MID_1P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': '52ba52ac3f29dbcdf9539b1cca09af6f80832a0e0d794a82830e9000a3476491', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '18.55040860158382'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'bdc8c385c313cc43720dd417dabd7acee222d23b1b22f16d3a0e46f8c4c28440', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '18.604849862328813'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'e0437a164e6c790dc25c10da93b9ede0abce3c66e50bcea58b49b6a006a07859', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '17.206805483403485'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'a22a45d60cc0a7f379c68521307bf94480d2d2ca06c4531a82e7eb442a668a34', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '17.260477799787672'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': '4cad414a518a789197158a2d92238dcab7828b676571d4ad688e5f5ab2c7b5a7', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '14.659255068639068'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': '51c241a8ef729bc48b465509765de69254e589af4ff0727e87ab80122a4dfa58', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '14.711452870836876'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'e155b07f8c17a8e39ed85a08949b6c796a1466f765746d7ad77471da9d549d52', 'raw_signals': '73', 'accepted': '68', 'rejected': '2', 'blocked': '3', 'net_r': '13.522350554906184'}, 'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'e155b07f8c17a8e39ed85a08949b6c796a1466f765746d7ad77471da9d549d52', 'raw_signals': '73', 'accepted': '68', 'rejected': '2', 'blocked': '3', 'net_r': '13.522350554906184'}}, 'HYBRID__DAILY_VOL_LE_0.75': {'MID_1P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '35b978bf28c8896da5b2423389cfb1dd2c3c641fde5029213075a9d1eef2939c', 'accepted_hash': 'c31b26c1db8b179decadeaae4351b409b61b3a28da1fb4ed83edc331f3c9fb61', 'raw_signals': '42', 'accepted': '42', 'rejected': '0', 'blocked': '0', 'net_r': '17.00762799056702'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '35b978bf28c8896da5b2423389cfb1dd2c3c641fde5029213075a9d1eef2939c', 'accepted_hash': '438c3cc79db97983c39f6140111d0f6de982e41134fbbc3aac39e8543c600938', 'raw_signals': '42', 'accepted': '42', 'rejected': '0', 'blocked': '0', 'net_r': '17.062069251312014'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '35b978bf28c8896da5b2423389cfb1dd2c3c641fde5029213075a9d1eef2939c', 'accepted_hash': 'ab6ab620dbccf8025791b6f6e45d683fd0819fa0be21a5cb477f4ee8dfd11337', 'raw_signals': '42', 'accepted': '42', 'rejected': '0', 'blocked': '0', 'net_r': '16.105001719629232'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '35b978bf28c8896da5b2423389cfb1dd2c3c641fde5029213075a9d1eef2939c', 'accepted_hash': '6d96cf7fb1782489147704b79e133835bf9dee75c41a781a19a65164cc992dd3', 'raw_signals': '42', 'accepted': '42', 'rejected': '0', 'blocked': '0', 'net_r': '16.158674036013423'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '35b978bf28c8896da5b2423389cfb1dd2c3c641fde5029213075a9d1eef2939c', 'accepted_hash': 'b45e028c3d9c9dbb3e8397a68f04f73ccb2a7c8506e7d1ed38ef23ec2127d306', 'raw_signals': '42', 'accepted': '42', 'rejected': '0', 'blocked': '0', 'net_r': '14.39441787584999'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '35b978bf28c8896da5b2423389cfb1dd2c3c641fde5029213075a9d1eef2939c', 'accepted_hash': '58e64206783e222dde5faa10626b3e39dfbe6e03a5016e731090a6f4db33a33d', 'raw_signals': '42', 'accepted': '42', 'rejected': '0', 'blocked': '0', 'net_r': '14.446615678047799'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '35b978bf28c8896da5b2423389cfb1dd2c3c641fde5029213075a9d1eef2939c', 'accepted_hash': '094cb584de31b15a9d51a16f9edaa9b7a3b5ac2619804f367fb73b5b72f3fed4', 'raw_signals': '42', 'accepted': '40', 'rejected': '2', 'blocked': '0', 'net_r': '11.883601506636053'}, 'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '35b978bf28c8896da5b2423389cfb1dd2c3c641fde5029213075a9d1eef2939c', 'accepted_hash': '094cb584de31b15a9d51a16f9edaa9b7a3b5ac2619804f367fb73b5b72f3fed4', 'raw_signals': '42', 'accepted': '40', 'rejected': '2', 'blocked': '0', 'net_r': '11.883601506636053'}}, 'HYBRID__DAILY_VOL_LE_1': {'MID_1P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': 'eaf8d2a34c4ffe5cffa20dfefe9593a7fd6fe71a974c845352f5702aaacb0cec', 'accepted_hash': '6ba642110e6675552cd8d25b5e8813b08a2a2d5c93f9b6d0c173c151b0fcfbae', 'raw_signals': '63', 'accepted': '60', 'rejected': '0', 'blocked': '3', 'net_r': '17.492863475775764'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': 'eaf8d2a34c4ffe5cffa20dfefe9593a7fd6fe71a974c845352f5702aaacb0cec', 'accepted_hash': '54dae60e9f1e3e798861cdf8c80e156f1f26beb87047683f3cc6cbe55c9d46f2', 'raw_signals': '63', 'accepted': '60', 'rejected': '0', 'blocked': '3', 'net_r': '17.547304736520758'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': 'eaf8d2a34c4ffe5cffa20dfefe9593a7fd6fe71a974c845352f5702aaacb0cec', 'accepted_hash': 'f884a057a7c993ae6d16ca62e4baca6914a85845b4baa7feadce4388de72bf65', 'raw_signals': '63', 'accepted': '60', 'rejected': '0', 'blocked': '3', 'net_r': '16.33468635229104'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': 'eaf8d2a34c4ffe5cffa20dfefe9593a7fd6fe71a974c845352f5702aaacb0cec', 'accepted_hash': '49fe5871fb5df2ee1895f2a5768d05f29bb9409c44435e7f0d38e158dfdae514', 'raw_signals': '63', 'accepted': '60', 'rejected': '0', 'blocked': '3', 'net_r': '16.388358668675227'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': 'eaf8d2a34c4ffe5cffa20dfefe9593a7fd6fe71a974c845352f5702aaacb0cec', 'accepted_hash': '1d30eb0bac52eccfd76b47255f7478888ce428a31fa19a237774fe4474d47311', 'raw_signals': '63', 'accepted': '60', 'rejected': '0', 'blocked': '3', 'net_r': '14.138463552440493'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': 'eaf8d2a34c4ffe5cffa20dfefe9593a7fd6fe71a974c845352f5702aaacb0cec', 'accepted_hash': '1c6899db63c2ce524d6b46f9d0bec3183d9428bb38e13afe415c8e4b5a039227', 'raw_signals': '63', 'accepted': '60', 'rejected': '0', 'blocked': '3', 'net_r': '14.190661354638301'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': 'eaf8d2a34c4ffe5cffa20dfefe9593a7fd6fe71a974c845352f5702aaacb0cec', 'accepted_hash': '2d83b115e051cd5dc8b01c53d7434a2d7f671fc78d4e55b1cfdfbb0e60c71054', 'raw_signals': '63', 'accepted': '58', 'rejected': '2', 'blocked': '3', 'net_r': '12.397878918241197'}, 'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': 'eaf8d2a34c4ffe5cffa20dfefe9593a7fd6fe71a974c845352f5702aaacb0cec', 'accepted_hash': '2d83b115e051cd5dc8b01c53d7434a2d7f671fc78d4e55b1cfdfbb0e60c71054', 'raw_signals': '63', 'accepted': '58', 'rejected': '2', 'blocked': '3', 'net_r': '12.397878918241197'}}, 'HYBRID__DAILY_VOL_LE_1.25': {'MID_1P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '3c50bd125b88df2938f87d67fac34ca96d9d03596e90d61ad35538fd096d0d98', 'accepted_hash': '50f3aa40955447939b216105fa97230dc52a0b7e49c7e822d01b5701a0eb2116', 'raw_signals': '71', 'accepted': '68', 'rejected': '0', 'blocked': '3', 'net_r': '20.55040860158382'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '3c50bd125b88df2938f87d67fac34ca96d9d03596e90d61ad35538fd096d0d98', 'accepted_hash': 'a447dd48abf481d3a0f79e1bb5a07e083e8e5704b65066eda1629eec84024e02', 'raw_signals': '71', 'accepted': '68', 'rejected': '0', 'blocked': '3', 'net_r': '20.604849862328813'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '3c50bd125b88df2938f87d67fac34ca96d9d03596e90d61ad35538fd096d0d98', 'accepted_hash': '367b10e23d99e4993f9bb71b25515a2f69bcec4a78ecbababa14f2783508ac2a', 'raw_signals': '71', 'accepted': '68', 'rejected': '0', 'blocked': '3', 'net_r': '19.206805483403485'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '3c50bd125b88df2938f87d67fac34ca96d9d03596e90d61ad35538fd096d0d98', 'accepted_hash': 'da684b6e266949d1388698164e05d696ca54a619b622e29b264c56ba63fef114', 'raw_signals': '71', 'accepted': '68', 'rejected': '0', 'blocked': '3', 'net_r': '19.260477799787672'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '3c50bd125b88df2938f87d67fac34ca96d9d03596e90d61ad35538fd096d0d98', 'accepted_hash': '73c06b08711cfc2e89ea904c30e4a43b1734b9c4b66276df8fa44ccd42a86c54', 'raw_signals': '71', 'accepted': '68', 'rejected': '0', 'blocked': '3', 'net_r': '16.65925506863907'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '3c50bd125b88df2938f87d67fac34ca96d9d03596e90d61ad35538fd096d0d98', 'accepted_hash': 'c7fed57c12c0c536c1193d8a17ae7dfe51823efc88ee6608709bfda0ec071553', 'raw_signals': '71', 'accepted': '68', 'rejected': '0', 'blocked': '3', 'net_r': '16.711452870836876'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '3c50bd125b88df2938f87d67fac34ca96d9d03596e90d61ad35538fd096d0d98', 'accepted_hash': 'b84d417b5a292dc5e672b91fa358285ca3a15a80c29148a2f647718bf5c71115', 'raw_signals': '71', 'accepted': '66', 'rejected': '2', 'blocked': '3', 'net_r': '15.522350554906184'}, 'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '3c50bd125b88df2938f87d67fac34ca96d9d03596e90d61ad35538fd096d0d98', 'accepted_hash': 'b84d417b5a292dc5e672b91fa358285ca3a15a80c29148a2f647718bf5c71115', 'raw_signals': '71', 'accepted': '66', 'rejected': '2', 'blocked': '3', 'net_r': '15.522350554906184'}}}
VOL_FEATURES = {"H4_VOL":"h4_atr_ratio","DAILY_VOL":"daily_atr_ratio"}
PASS6_PINS = {'PRIMARY__CONTROL': {'MID_1P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'accepted_hash': '41943df7be9b08027b99db153a0ade4a6d21c375eefe73e255f61b750b4c4a56', 'raw_signals': '41', 'accepted': '41', 'rejected': '0', 'blocked': '0', 'net_r': '18.00762799056702'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'accepted_hash': '5b8f87b7d0ff59c295deb34333697a9f8579cbda008d6496418607852a85a73b', 'raw_signals': '41', 'accepted': '41', 'rejected': '0', 'blocked': '0', 'net_r': '18.062069251312014'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'accepted_hash': 'f3ef005161c0a14111121497881f0252917b7e0420b5988a11e429c8afb55299', 'raw_signals': '41', 'accepted': '41', 'rejected': '0', 'blocked': '0', 'net_r': '17.105001719629232'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'accepted_hash': '964845b2124c188b32db84c21570c8fd4245f762ba5f5b39a2f7daa840f0796f', 'raw_signals': '41', 'accepted': '41', 'rejected': '0', 'blocked': '0', 'net_r': '17.158674036013423'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'accepted_hash': 'b96bc84cc280d81e44979870bb9449e597583cb1537ef0fef63b0201edfd0135', 'raw_signals': '41', 'accepted': '41', 'rejected': '0', 'blocked': '0', 'net_r': '15.39441787584999'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'accepted_hash': '069e29be5e2f7a341727ecc9f5cbbbbd0c1f3c46d5699353a284de2208d81a95', 'raw_signals': '41', 'accepted': '41', 'rejected': '0', 'blocked': '0', 'net_r': '15.446615678047799'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'accepted_hash': '86618f514c1cedf08adaa1e4954459498845ae6fcf581c9b62795ca9ffa1089e', 'raw_signals': '41', 'accepted': '39', 'rejected': '2', 'blocked': '0', 'net_r': '12.883601506636053'}, 'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'accepted_hash': '86618f514c1cedf08adaa1e4954459498845ae6fcf581c9b62795ca9ffa1089e', 'raw_signals': '41', 'accepted': '39', 'rejected': '2', 'blocked': '0', 'net_r': '12.883601506636053'}}, 'FREQUENCY__CONTROL': {'MID_1P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': '52ba52ac3f29dbcdf9539b1cca09af6f80832a0e0d794a82830e9000a3476491', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '18.55040860158382'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'bdc8c385c313cc43720dd417dabd7acee222d23b1b22f16d3a0e46f8c4c28440', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '18.604849862328813'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'e0437a164e6c790dc25c10da93b9ede0abce3c66e50bcea58b49b6a006a07859', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '17.206805483403485'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'a22a45d60cc0a7f379c68521307bf94480d2d2ca06c4531a82e7eb442a668a34', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '17.260477799787672'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': '4cad414a518a789197158a2d92238dcab7828b676571d4ad688e5f5ab2c7b5a7', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '14.659255068639068'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': '51c241a8ef729bc48b465509765de69254e589af4ff0727e87ab80122a4dfa58', 'raw_signals': '73', 'accepted': '70', 'rejected': '0', 'blocked': '3', 'net_r': '14.711452870836876'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'e155b07f8c17a8e39ed85a08949b6c796a1466f765746d7ad77471da9d549d52', 'raw_signals': '73', 'accepted': '68', 'rejected': '2', 'blocked': '3', 'net_r': '13.522350554906184'}, 'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'raw_signal_hash': '7258c58334d0f0a8c787601b82340d064cd5a92f0869587e26148bff63787853', 'accepted_hash': 'e155b07f8c17a8e39ed85a08949b6c796a1466f765746d7ad77471da9d549d52', 'raw_signals': '73', 'accepted': '68', 'rejected': '2', 'blocked': '3', 'net_r': '13.522350554906184'}}}


def study_definitions():
    rows=[{'id':'PRIMARY__CONTROL','anchor':'PRIMARY','family':'CONTROL','scope':'PRIMARY','feature':None,'cap':None,'availability':False},
          {'id':'FREQUENCY__CONTROL','anchor':'FREQUENCY','family':'CONTROL','scope':'ALL','feature':None,'cap':None,'availability':False},
          {'id':'HYBRID__H4_VOL_AVAILABLE','anchor':'PRIMARY','family':'H4_VOL','scope':'EXTRAS','feature':'h4_atr_ratio','cap':None,'availability':True}]
    for cap in VOL_LEVELS['H4_VOL']:
        rows.append({'id':f'HYBRID__H4_VOL_LE_{cap:g}','anchor':'PRIMARY','family':'H4_VOL','scope':'EXTRAS','feature':'h4_atr_ratio','cap':cap,'availability':False})
    for d in rows:d.update(body=1.40,lookback=40 if d['scope']=='PRIMARY' else 20,distance=.50,rr=STUDY_RR,ratio=.60)
    require(len(rows)==8 and len({r['id'] for r in rows})==8,'Pass8 definition count')
    return rows

DEFINITIONS=study_definitions()
DEFINITION_BY_ID={d['id']:d for d in DEFINITIONS}

def configs():return [(d['id'],d['family'],d['lookback'],d['distance'],d['body'],1.50) for d in DEFINITIONS]

def anchor_for(cid):
    return next(a for a in FROZEN_ENTRIES if a[0]==DEFINITION_BY_ID[cid]['anchor'])

def anchor_qualifies(f,anchor):
    _,_,l,d,b,r=anchor
    return f['distances'][l]<=d and f['body_atr']>=b and f['range_atr']>=r

def comparator_qualifies(f,comparator):return anchor_qualifies(f,comparator) and f['body_ratio']>=.60

def condition_passes(f,d):
    if d['feature'] is None:return True
    v=f[d['feature']]
    return v is not None and (d['availability'] or v<=d['cap'])

def condition_metadata(cid,raw):
    d=DEFINITION_BY_ID[cid];extra_only=d['scope']=='EXTRAS'
    missing=sum(comparator_qualifies(f,FROZEN_ENTRIES[1]) and (not extra_only or not comparator_qualifies(f,FROZEN_ENTRIES[0])) and d['feature'] is not None and f[d['feature']] is None for f in raw.values())
    return {'anchor':d['anchor'],'conditions_json':json.dumps({'body_range_floor':.60,'scope':d['scope'],'feature':d['feature'],'cap':d['cap'],'availability_only':d['availability'],'primary_bypasses_condition':extra_only},sort_keys=True),
            'h4_vol_cap':d['cap'] if d['family']=='H4_VOL' else None,'daily_vol_cap':d['cap'] if d['family']=='DAILY_VOL' else None,'condition_scope':d['scope'],'availability_only':d['availability'],'body_ratio_floor':.60,'missing_feature_signals':missing,'reward_risk':STUDY_RR}

def study_boundaries(cid):
    d=DEFINITION_BY_ID[cid]
    return 'CAP_BOUNDARY' if d['cap'] is not None and d['cap'] in (VOL_LEVELS[d['family']][0],VOL_LEVELS[d['family']][-1]) else ''

def comparison_pairs():
    pairs=[('PRIMARY__CONTROL','FREQUENCY__CONTROL','UNFILTERED_FREQUENCY')]
    for d in DEFINITIONS[2:]:
        for control in ('PRIMARY__CONTROL','FREQUENCY__CONTROL'):pairs.append((control,d['id'],'VS_UNFILTERED'))
        if not d['availability']:
            pairs.append(('HYBRID__H4_VOL_AVAILABLE',d['id'],'VS_MATCHED_AVAILABILITY'))
            if d['id']!='HYBRID__H4_VOL_LE_1.1':pairs.append(('HYBRID__H4_VOL_LE_1.1',d['id'],'VS_PASS7_HYBRID'))
    for a,b in zip(VOL_LEVELS['H4_VOL'],VOL_LEVELS['H4_VOL'][1:]):
        pairs.append((f'HYBRID__H4_VOL_LE_{a:g}',f'HYBRID__H4_VOL_LE_{b:g}','CHANGE_CAP'))
    require(len(pairs)==26 and len(set(pairs))==26,'Pass8 comparison count')
    return sorted(pairs)

def condition_neighbours():
    for a,b in zip(VOL_LEVELS['H4_VOL'],VOL_LEVELS['H4_VOL'][1:]):
        x=f'HYBRID__H4_VOL_LE_{a:g}';y=f'HYBRID__H4_VOL_LE_{b:g}'
        yield x,y,'H4_VOL_cap';yield y,x,'H4_VOL_cap'

def verify_pass7(bars,raw,paths):
    parent_masks,_=independent_study_masks(bars,PASS7_DEFINITIONS);rows=[]
    for d in PASS7_DEFINITIONS:
        ids=[i for i,f in raw.items() if qualifies_definition(f,d)]
        require(ids==parent_masks[d['id']],'Independent archived Pass7 mask mismatch '+d['id'])
        for name,pin in PASS7_PINS[d['id']].items():
            a,r,b=replay(ids,paths[name]);sha=digest([core(p) for p in a])
            require(digest(ids)==pin['raw_signal_hash'] and sha==pin['accepted_hash'],'Pinned Pass7 full-ledger mismatch '+d['id']+' '+name)
            require((len(ids),len(a),len(r),len(b))==tuple(int(pin[k]) for k in ('raw_signals','accepted','rejected','blocked')),'Pinned Pass7 counts changed')
            require(abs(sum(p['r'] for p in a if p['r'] is not None)-float(pin['net_r']))<1e-8,'Pinned Pass7 R changed')
            rows.append({'pass7_config':d['id'],'case':name,'raw_signal_hash':digest(ids),'accepted_hash':sha,'accepted':len(a),'parity':'PASS'})
    require(len(rows)==144,'Incomplete Pass7 archive controls')
    return rows

def verify_pass6(raw,paths):
    rows=[]
    for cid,cases in PASS6_PINS.items():
        entry=next(a for a in FROZEN_ENTRIES if a[0]==cid.split('__')[0])
        ids=[i for i,f in raw.items() if comparator_qualifies(f,entry)]
        for name,pin in cases.items():
            a,r,b=replay(ids,paths[name]);sha=digest([core(p) for p in a])
            require(digest(ids)==pin['raw_signal_hash'] and sha==pin['accepted_hash'],'Pinned Pass6 RR2.75 ledger mismatch '+cid+' '+name)
            require((len(ids),len(a),len(r),len(b))==tuple(int(pin[k]) for k in ('raw_signals','accepted','rejected','blocked')),'Pinned Pass6 counts changed')
            require(abs(sum(p['r'] for p in a if p['r'] is not None)-float(pin['net_r']))<1e-8,'Pinned Pass6 R changed')
            rows.append({'config':cid,'case':name,'raw_signal_hash':digest(ids),'accepted_hash':sha,'accepted':len(a),'parity':'PASS'})
    require(len(rows)==16,'Incomplete Pass6 controls')
    return rows

def independent_study_masks(bars,definitions=None):
    """Separate scalar H4/daily ATR/context and direct entry masks. No feature helpers."""
    if definitions is None:definitions=DEFINITIONS
    daily_groups={}
    for b in bars:daily_groups.setdefault(b['t']//86400*86400,[]).append(b)
    days=[];day_atrs=[];seed=[];atr=None;previous_close=None
    for k,(t,group) in enumerate(sorted(daily_groups.items())):
        high=max(b['mid']['h'] for b in group);low=min(b['mid']['l'] for b in group);close=group[-1]['mid']['c']
        if k:
            tr=max(high-low,abs(high-previous_close),abs(low-previous_close))
            if k<=14:seed.append(tr)
            if k==14:atr=sum(seed)/14
            elif k>14:atr=(atr*13+tr)/14
        history=day_atrs[k-60:k] if k>=60 else []
        ratio=atr/(sum(history)/60) if atr is not None and len(history)==60 and all(v is not None and v>0 for v in history) else None
        days.append((t+86400,ratio if k+1>=200 else None,k+1,sum(b['h1_count'] for b in group)))
        day_atrs.append(atr);previous_close=close
    day_ends=[d[0] for d in days];h4_atrs=[None]*len(bars);seed=[];atr=None
    masks={d['id']:[] for d in definitions};audit={}
    for i in range(1,len(bars)):
        p=bars[i]['mid'];q=bars[i-1]['mid'];tr=max(p['h']-p['l'],abs(p['h']-q['c']),abs(p['l']-q['c']))
        if i<=14:seed.append(tr)
        if i==14:atr=sum(seed)/14
        elif i>14:atr=(13*atr+tr)/14
        h4_atrs[i]=atr
        if i<200 or not(q['c']<q['o'] and p['c']>p['o'] and p['o']<=q['c'] and p['c']>=q['o'] and p['h']>p['l'] and q['h']>q['l']):continue
        history=h4_atrs[i-60:i];h4ratio=atr/(sum(history)/60) if all(v is not None and v>0 for v in history) else None
        k=bisect.bisect_right(day_ends,bars[i]['t']+14400)-1;daily=days[k] if k>=0 else None
        dratio=daily[1] if daily is not None else None
        quality=(p['c']-p['o'])/atr>=1.40 and (p['h']-p['l'])/atr>=1.50 and (p['c']-p['o'])/(p['h']-p['l'])>=.60
        primary=quality and abs(p['l']-min(z['mid']['l'] for z in bars[i-40:i]))/atr<=.50
        frequency=quality and abs(p['l']-min(z['mid']['l'] for z in bars[i-20:i]))/atr<=.50
        audit[i]={'h4_atr_ratio':h4ratio,'daily_atr_ratio':dratio,'daily_completed_count':daily[2] if daily else 0,'daily_end_utc':stamp(daily[0]) if daily else None,'daily_observed_h1_count':daily[3] if daily else None,'primary':primary,'frequency':frequency}
        for d in definitions:
            v=h4ratio if d['family']=='H4_VOL' else dratio
            passed=d['feature'] is None or (v is not None and (d['availability'] or v<=d['cap']))
            keep=primary if d['scope']=='PRIMARY' else ((primary or (frequency and passed)) if d['scope']=='EXTRAS' else frequency and passed)
            if keep:masks[d['id']].append(i)
    return masks,audit

def verify_pass5(raw,paths):
    rows=[]
    for entry in FROZEN_ENTRIES:
        oldid='LOCAL__L40_D0.5_B1.4' if entry[0]=='PRIMARY' else 'FREQUENCY_ALT__CONTROL'
        signals=[i for i,f in raw.items() if comparator_qualifies(f,entry)]
        for name,pin in PASS5_PINS[oldid].items():
            a,r,b=replay(signals,paths[name]);sha=digest([core(p) for p in a])
            require(digest(signals)==pin['raw_signal_hash'] and sha==pin['accepted_hash'],'Pinned Pass5 RR3 ledger mismatch '+oldid+' '+name)
            require((len(a),len(r),len(b))==(int(pin['accepted']),int(pin['rejected']),int(pin['blocked'])),'Pinned Pass5 RR3 counts changed')
            require(abs(sum(p['r'] for p in a if p['r'] is not None)-float(pin['net_r']))<1e-8,'Pinned Pass5 RR3 R changed')
            rows.append({'entry_version':entry[0],'pass5_definition':oldid,'case':name,'accepted':len(a),'ledger_sha256':sha,'parity':'PASS'})
    require(len(rows)==16,'Incomplete Pass5 reference controls')
    return rows


def verify_pass4(raw,paths):
    rows=[]
    for baseline in BASELINES:
        cid='QUALITY__L40_D0.5' if baseline[0]=='LOCAL' else 'QUALITY__L20_D0.5'
        signals=[i for i,f in raw.items() if comparator_qualifies(f,baseline)]
        for name,pin in PASS4_PINS[cid].items():
            a,r,b=replay(signals,paths[name]);sha=digest([core(p) for p in a])
            require(digest(signals)==pin['raw_signal_hash'] and sha==pin['accepted_hash'],'Pinned Pass4 reference hash mismatch '+cid+' '+name)
            require((len(a),len(r),len(b))==(int(pin['accepted']),int(pin['rejected']),int(pin['blocked'])),'Pinned Pass4 reference counts changed')
            require(abs(sum(p['r'] for p in a if p['r'] is not None)-float(pin['net_r']))<1e-8,'Pinned Pass4 reference R changed')
            rows.append({'reference':baseline[0],'pass4_definition':cid,'case':name,'accepted':len(a),'ledger_sha256':sha,'parity':'PASS'})
    require(len(rows)==16,'Incomplete Pass4 reference controls')
    return rows


def verify_pass3(raw,paths):
    rows=[]
    for comparator in COMPARATORS:
        cid='STRONG_BODY__BR0.6_CNONE' if comparator[0]=='FREQUENCY' else 'STRONG_BODY__BODY1.4_BR0.6'
        signals=[i for i,f in raw.items() if comparator_qualifies(f,comparator)]
        for name,pin in PASS3_PINS[cid].items():
            a,r,b=replay(signals,paths[name]);sha=digest([core(p) for p in a])
            require(digest(signals)==pin['raw_signal_hash'] and sha==pin['accepted_hash'],'Pinned Pass3 comparator hash mismatch '+cid+' '+name)
            require((len(a),len(r),len(b))==(int(pin['accepted']),int(pin['rejected']),int(pin['blocked'])),'Pinned Pass3 comparator count mismatch')
            require(abs(sum(p['r'] for p in a if p['r'] is not None)-float(pin['net_r']))<1e-8,'Pinned Pass3 comparator R mismatch')
            rows.append({'comparator':comparator[0],'pass3_definition':cid,'case':name,'accepted':len(a),'ledger_sha256':sha,'parity':'PASS'})
    require(len(rows)==16,'Incomplete Pass3 comparator controls')
    return rows


def pass2_signals(cid,raw):
    label,condition=cid.split('__',1);a=next(a for a in ANCHORS if a[0]==label)
    if condition=='CONTROL':feature=None;value=None
    elif condition.startswith('H4_VOL_LE_'):feature='h4_atr_ratio';value=float(condition[len('H4_VOL_LE_'):])
    elif condition.startswith('SIGNAL_CLOSE_GE_'):feature='close_location';value=float(condition[len('SIGNAL_CLOSE_GE_'):])
    elif condition.startswith('BODY_RATIO_GE_'):feature='body_ratio';value=float(condition[len('BODY_RATIO_GE_'):])
    else:raise ValueError('Unexpected pinned Pass2 condition')
    ids=[]
    for i,f in raw.items():
        if not anchor_qualifies(f,a):continue
        if feature is not None:
            x=f[feature]
            if x is None or (x>value if feature=='h4_atr_ratio' else x<value):continue
        ids.append(i)
    return ids


def verify_pass2(raw,paths):
    results=[]
    for cid,cases in PASS2_PINS.items():
        signals=pass2_signals(cid,raw)
        for name,pin in cases.items():
            a,r,b=replay(signals,paths[name]);sha=digest([core(p) for p in a])
            require(digest(signals)==pin['raw_signal_hash'] and sha==pin['accepted_hash'],'Pinned Pass2 ledger mismatch '+cid+' '+name)
            require((len(a),len(r),len(b))==(int(pin['accepted']),int(pin['rejected']),int(pin['blocked'])),'Pinned Pass2 counts changed')
            require(abs(sum(p['r'] for p in a if p['r'] is not None)-float(pin['net_r']))<1e-8,'Pinned Pass2 R changed')
            results.append({'pass2_definition':cid,'case':name,'accepted':len(a),'ledger_sha256':sha,'parity':'PASS'})
    require(len(results)==104,'Incomplete Pass2 controls')
    return results


def paired_reports(out,ledgers,masks):
    fields=['reference_config','candidate_config','question','case','reference_closed_r','candidate_closed_r','delta_r','common','added','removed','common_r_change','added_closed_r','removed_closed_r','identity_error']
    summary=CSV(Path(out)/'volatility_comparisons.csv',fields)
    marginal=CSV(Path(out)/'volatility_marginal_trades.csv',['reference_config','candidate_config','question','case','kind','signal_index','r','qualifies_candidate','displaced_by_signal_index'])
    period_delta=CSV(Path(out)/'volatility_period_deltas.csv',['reference_config','candidate_config','question','case','period','start_utc','end_utc','partial','reference_entries','candidate_entries','entry_delta','reference_net_r','candidate_net_r','net_r_delta'])
    try:
        for reference,candidate,question in comparison_pairs():
            for case in CASES:
                name=case[0];a=ledgers[reference,name];b=ledgers[candidate,name]
                ar={p['signal_index']:p for p in a[0]};br={p['signal_index']:p for p in b[0]}
                common=ar.keys()&br.keys();added=br.keys()-ar.keys();removed=ar.keys()-br.keys()
                val=lambda p:p['r'] if p['r'] is not None else 0.
                ra=sum(val(p) for p in ar.values());rb=sum(val(p) for p in br.values())
                add=sum(val(br[i]) for i in added);rem=sum(val(ar[i]) for i in removed);change=sum(val(br[i])-val(ar[i]) for i in common)
                err=rb-ra-(add-rem+change);require(abs(err)<1e-8,'Component marginal R identity failed')
                meta={'reference_config':reference,'candidate_config':candidate,'question':question,'case':name}
                summary.add(dict(meta,reference_closed_r=ra,candidate_closed_r=rb,delta_r=rb-ra,common=len(common),added=len(added),removed=len(removed),common_r_change=change,added_closed_r=add,removed_closed_r=rem,identity_error=err))
                ix=PeriodIndex(a[0]);iy=PeriodIndex(b[0])
                for interval in periods()[0]:
                    x=ix.row(interval);y=iy.row(interval)
                    period_delta.add(dict(meta,**{k:x[k] for k in ('period','start_utc','end_utc','partial')},reference_entries=x['entries'],candidate_entries=y['entries'],entry_delta=y['entries']-x['entries'],reference_net_r=x['net_r'],candidate_net_r=y['net_r'],net_r_delta=y['net_r']-x['net_r']))
                ab={p['signal_index']:p['blocking_signal_index'] for p in a[2]};bb={p['signal_index']:p['blocking_signal_index'] for p in b[2]}
                for kind,ids,lookup,blocks in (('ADDED',added,br,ab),('REMOVED',removed,ar,bb)):
                    for i in sorted(ids):marginal.add(dict(meta,kind=kind,signal_index=i,r=lookup[i]['r'],qualifies_candidate=i in masks[candidate],displaced_by_signal_index=blocks.get(i)))
    finally:summary.close();marginal.close();period_delta.close()
    return {'volatility_comparisons':summary.count,'volatility_marginal_trades':marginal.count,'volatility_period_deltas':period_delta.count}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError('Broker redirects are disabled')


def broker_settings():
    token = next((os.environ[k].strip() for k in ('OANDA_API_KEY','OANDA_TOKEN','OANDA_API_TOKEN','OANDA_ACCESS_TOKEN') if os.environ.get(k,'').strip()), '')
    require(bool(token), 'Missing OANDA_API_KEY (or OANDA_TOKEN/OANDA_API_TOKEN/OANDA_ACCESS_TOKEN)')
    env = os.environ.get('OANDA_ENV',os.environ.get('OANDA_ENVIRONMENT','practice')).lower()
    require(env in ('practice','demo','live'), 'OANDA_ENV must be practice, demo or live')
    base = 'https://api-fxtrade.oanda.com' if env=='live' else 'https://api-fxpractice.oanda.com'
    base = os.environ.get('OANDA_API_URL',os.environ.get('OANDA_BASE_URL',base)).rstrip('/')
    if base.endswith('/v3'): base=base[:-3]
    require(base in ('https://api-fxtrade.oanda.com','https://api-fxpractice.oanda.com'), 'Only official OANDA HTTPS API hosts allowed')
    return token, base


def fetch(granularity, price, progress=lambda **k:None, transport=None):
    token, base = broker_settings()
    step = H4 if granularity=='H4' else 3600
    opener = urllib.request.build_opener(NoRedirect())
    cursor, rows, receipts = START, [], []
    for page in range(200):
        query = {'from':stamp(cursor),'count':5000,'granularity':granularity,'price':price,
                 'smooth':'false','includeFirst':'true','dailyAlignment':0,'alignmentTimezone':'UTC'}
        url = base+'/v3/instruments/EUR_USD/candles?'+urllib.parse.urlencode(query)
        request=urllib.request.Request(url,headers={'Authorization':'Bearer '+token,'Accept-Datetime-Format':'RFC3339'})
        data = None
        for attempt in range(5):
            try:
                if transport is not None:
                    data=transport(request)
                else:
                    with opener.open(request, timeout=90) as response:
                        data=json.load(response)
                break
            except urllib.error.HTTPError as e:
                if e.code not in (429,500,502,503,504) or attempt==4:
                    raise ValueError(f'OANDA candles HTTP {e.code}; check credentials/environment and retry') from None
                time.sleep(2**attempt)
            except (urllib.error.URLError, TimeoutError):
                if attempt==4: raise ValueError('OANDA candles network timeout/failure') from None
                time.sleep(2**attempt)
        require(isinstance(data,dict) and data.get('instrument')=='EUR_USD' and data.get('granularity')==granularity, 'Broker response identity mismatch')
        candles=data.get('candles'); require(isinstance(candles,list), 'Missing candle array')
        times=[epoch(c['time']) for c in candles]
        require(all(a<b for a,b in zip(times,times[1:])), 'Non-increasing broker page')
        require(not times or times[0]>=cursor, 'Broker pagination moved backwards')
        kept=[c for c,t in zip(candles,times) if c.get('complete') is True and t>=START and t+step<=END]
        require(not rows or not kept or epoch(kept[0]['time'])>epoch(rows[-1]['time']), 'Duplicate broker page')
        rows.extend(kept)
        receipts.append({'granularity':granularity,'page':page,'query':query,'received':len(candles),'kept':len(kept),'payload_sha256':digest(data),'fetched_at_utc':stamp(time.time())})
        progress(phase='FETCH_'+granularity, pages=page+1, candles=len(rows))
        if not times or times[-1]+step>=END or len(candles)<5000: break
        cursor=times[-1]+step
    else: raise ValueError('Pagination limit reached')
    return rows, receipts


def validate_native(rows, step, sides, full):
    out=[]
    for c in rows:
        t=epoch(c['time'])
        require(c.get('complete') is True and START<=t and t+step<=END and t%step==0, 'Incomplete, misaligned, or out-of-range candle')
        require(not out or t>out[-1]['t'], 'Duplicate or non-increasing candles')
        require(float(c.get('volume',0))>0, 'Nonpositive candle volume')
        b={'t':t}
        for side in sides:
            p={k:float(c[side][k]) for k in 'ohlc'}
            require(all(math.isfinite(x) and x>0 for x in p.values()), 'Nonfinite/nonpositive OHLC')
            require(p['l']<=min(p['o'],p['c'])<=max(p['o'],p['c'])<=p['h'], 'Invalid OHLC range')
            b[side]=p
        if 'bid' in sides:
            for k in 'ohlc':
                require(b['bid'][k]<=b['ask'][k]+1e-10, 'Bid exceeds ask')
                require(b['bid'][k]-1.1*TICK<=b['mid'][k]<=b['ask'][k]+1.1*TICK, 'Mid outside quote range')
        out.append(b)
    require(bool(out), 'Empty source')
    if full:
        require(len(out)>=(25000 if step==H4 else 100000), 'Incomplete full-history candle count')
        require(out[0]['t']<=START+7*86400 and out[-1]['t']+step>=END-4*86400, 'Incomplete source endpoints')
        years=collections.Counter(dt.datetime.fromtimestamp(b['t'],UTC).year for b in out)
        for year in range(2005,2027):
            minimum=(1000 if step==H4 else 4000)*(0.65 if year==2026 else 1)
            require(years[year]>=minimum, f'Insufficient coverage {year} granularity {step}')
    return out


def validate_source(h4, h1, full=True):
    bars=validate_native(h4,H4,('mid','bid','ask'),full)
    hours=validate_native(h1,3600,('mid',),full)
    groups=collections.defaultdict(list)
    for h in hours: groups[h['t']//H4*H4].append(h)
    require(set(groups)=={b['t'] for b in bars}, 'H1/H4 bin-set mismatch')
    for b in bars:
        g=groups[b['t']]
        p={'o':g[0]['mid']['o'],'h':max(x['mid']['h'] for x in g),'l':min(x['mid']['l'] for x in g),'c':g[-1]['mid']['c']}
        require(all(abs(p[k]-b['mid'][k])<=1e-10 for k in 'ohlc'), 'Native H4 vs H1 aggregate mismatch at '+stamp(b['t']))
        b['first_h1']=g[0]['t']; b['h1_count']=len(g)
    return bars


def freeze_source(path, h4, h1, receipts):
    payloads={'h4.json':canonical(h4),'h1.json':canonical(h1),'receipts.json':canonical(receipts)}
    manifest={'instrument':'EUR_USD','start':START,'end':END,'alignment':'UTC_00_UNSMOOTHED','members':{n:hashlib.sha256(b).hexdigest() for n,b in payloads.items()}}
    tmp=Path(str(path)+'.tmp')
    with zipfile.ZipFile(tmp,'w',zipfile.ZIP_DEFLATED,compresslevel=6) as z:
        for n,b in payloads.items(): z.writestr(n,b)
        z.writestr('manifest.json',canonical(manifest))
    os.replace(tmp,path)
    return manifest


def read_source(path):
    with zipfile.ZipFile(path) as z:
        require(set(z.namelist())=={'h4.json','h1.json','receipts.json','manifest.json'} and len(z.namelist())==4,'Unexpected source archive members')
        require(sum(i.file_size for i in z.infolist())<500_000_000,'Source archive too large')
        m=json.loads(z.read('manifest.json'))
        require(m.get('instrument')=='EUR_USD' and m.get('start')==START and m.get('end')==END and m.get('alignment')=='UTC_00_UNSMOOTHED','Source archive protocol mismatch')
        require(set(m.get('members',{}))=={'h4.json','h1.json','receipts.json'}, 'Incomplete source manifest')
        values={}
        for name,sha in m['members'].items():
            data=z.read(name);require(hashlib.sha256(data).hexdigest()==sha,'Source hash mismatch: '+name)
            values[name]=json.loads(data)
    return values['h4.json'],values['h1.json'],values['receipts.json'],m


def base_features(bars):
    a=[None]*len(bars); tr=[0.]*len(bars)
    for i in range(1,len(bars)):
        p=bars[i]['mid'];prev=bars[i-1]['mid']['c']
        tr[i]=max(p['h']-p['l'],abs(p['h']-prev),abs(p['l']-prev))
    if len(bars)>14:
        a[14]=sum(tr[1:15])/14
        for i in range(15,len(bars)): a[i]=(a[i-1]*13+tr[i])/14
    found={}
    for i in range(200,len(bars)):
        p=bars[i]['mid'];q=bars[i-1]['mid']
        if not(q['c']<q['o'] and p['c']>p['o'] and p['o']<=q['c'] and p['c']>=q['o'] and p['h']>p['l'] and q['h']>q['l'] and a[i]>0):continue
        r=p['h']-p['l']
        found[i]={'atr':a[i],'body_atr':(p['c']-p['o'])/a[i],'range_atr':r/a[i],
                  'close_location':(p['c']-p['l'])/r,'body_ratio':(p['c']-p['o'])/r,
                  'distances':{l:abs(p['l']-min(b['mid']['l'] for b in bars[i-l:i]))/a[i] for l in LB}}
    return a,found


CONTEXT_FIELDS=['previous_close_location','previous_body_atr','previous_range_atr','lower_wick_body','movement_6','movement_12','movement_30','h4_atr_ratio','daily_completed_count','daily_end_utc','daily_observed_h1_count','daily_atr_ratio','daily_ema_distance_50','daily_ema_distance_100','daily_ema_distance_200','daily_ema50_slope5','daily_change']


def daily_context(bars):
    grouped=collections.defaultdict(list)
    for b in bars:grouped[b['t']//86400*86400].append(b)
    days=[];emas={50:[],100:[],200:[]};atrs=[];tr=[]
    for k,(t,g) in enumerate(sorted(grouped.items())):
        p={'o':g[0]['mid']['o'],'h':max(b['mid']['h'] for b in g),'l':min(b['mid']['l'] for b in g),'c':g[-1]['mid']['c']}
        if k:
            prior=days[-1]['mid']['c'];tr.append(max(p['h']-p['l'],abs(p['h']-prior),abs(p['l']-prior)))
        a=None
        if k==14:a=sum(tr[:14])/14
        elif k>14:a=(atrs[-1]*13+tr[-1])/14
        atrs.append(a)
        for n,series in emas.items():series.append(p['c'] if not series else series[-1]+2/(n+1)*(p['c']-series[-1]))
        history=atrs[max(0,k-60):k]
        ratio=a/(sum(history)/60) if a is not None and len(history)==60 and all(v is not None and v>0 for v in history) else None
        d={'end':t+86400,'mid':p,'count':k+1,'h1_count':sum(b['h1_count'] for b in g),
           'daily_atr_ratio':ratio,'daily_ema50_slope5':None,'daily_change':None}
        for n in emas:d['daily_ema_distance_'+str(n)]=(p['c']-emas[n][-1])/a if a and a>0 else None
        if a and a>0:
            d['daily_ema50_slope5']=(emas[50][-1]-emas[50][-6])/a if k>=5 else None
            d['daily_change']=(p['c']-days[-1]['mid']['c'])/a if k else None
        days.append(d)
    return days


def features(bars):
    a,found=base_features(bars);days=daily_context(bars);ends=[d['end'] for d in days]
    for i,f in found.items():
        p=bars[i]['mid'];q=bars[i-1]['mid'];qr=q['h']-q['l']
        f.update(previous_close_location=(q['c']-q['l'])/qr,previous_body_atr=abs(q['c']-q['o'])/a[i],previous_range_atr=qr/a[i],lower_wick_body=(p['o']-p['l'])/(p['c']-p['o']))
        for n in (6,12,30):f['movement_'+str(n)]=(q['c']-bars[i-1-n]['mid']['c'])/a[i]
        history=a[i-60:i]
        f['h4_atr_ratio']=a[i]/(sum(history)/60) if all(v is not None and v>0 for v in history) else None
        k=bisect.bisect_right(ends,bars[i]['t']+H4)-1;d=days[k] if k>=0 else None
        f['daily_completed_count']=d['count'] if d else 0
        f['daily_end_utc']=stamp(d['end']) if d else None
        f['daily_observed_h1_count']=d['h1_count'] if d else None
        for key in ('daily_atr_ratio','daily_ema_distance_50','daily_ema_distance_100','daily_ema_distance_200','daily_ema50_slope5','daily_change'):
            f[key]=d[key] if d and d['count']>=200 else None
    return a,found


def qualifies_definition(f,d):
    primary=comparator_qualifies(f,FROZEN_ENTRIES[0]);frequency=comparator_qualifies(f,FROZEN_ENTRIES[1])
    if d['scope']=='PRIMARY':return primary
    if d['scope']=='EXTRAS' and primary:return True
    return frequency and condition_passes(f,d)

def qualifies(f,config):return qualifies_definition(f,DEFINITION_BY_ID[config[0]])


def verify_pass1(bars,raw,paths,controls):
    require(len(bars)==35952,'Pass1 H4 count changed')
    for case in CASES:
        name=case[0]
        require(controls[name]==PASS1_PINS['raw_controls'][name],'Pinned raw control mismatch '+name)
        for anchor in ANCHORS:
            signals=[i for i,f in raw.items() if anchor_qualifies(f,anchor)]
            a,r,b=replay(signals,paths[name]);si,aa,rr,bb=independent_raw(bars,case,anchor)
            require(si==signals and [core(p) for p in a]==aa,'Independent anchor full-ledger mismatch')
            require([(p['signal_index'],p['reason']) for p in r]==rr and [(p['signal_index'],p['blocking_signal_index']) for p in b]==bb,'Independent anchor reject/block mismatch')
            oldid='M_L40_D0.35_B0.75_R1' if anchor[0]=='BROAD' else 'M_L40_D0.5_B1.25_R1.5'
            pin=PASS1_PINS['anchors'][oldid][name]
            require(digest(signals)==pin['raw_signal_hash'] and digest(aa)==pin['accepted_hash'],'Pinned Pass1 anchor hash mismatch')
            require(len(a)==int(pin['accepted']) and len(r)==int(pin['rejected']) and len(b)==int(pin['blocked']),'Pinned anchor count mismatch')
            require(abs(sum(p['r'] for p in a if p['r'] is not None)-float(pin['net_r']))<1e-8,'Pinned anchor R mismatch')


def path_for(bars, i, case, reward_risk=RR):
    _,scenario,cost,model=case
    p=bars[i]['mid'];reference=p['c'];stop=p['l']-PIP;target=reference+reward_risk*(reference-stop)
    t=bars[i]['t']+H4
    base={'signal_index':i,'signal_start':stamp(bars[i]['t']),'signal_close':stamp(t),
          'reference':reference,'stop':stop,'target':target,'rr_reference':reward_risk,'reason':''}
    if t>=END:return dict(base,reason='SIGNAL_AT_OR_AFTER_CUTOFF')
    nextbar=bars[i+1] if i+1<len(bars) else None
    base['next_observed_delay_seconds']=None if nextbar is None else nextbar['first_h1']-t
    if scenario=='BIDASK':
        if nextbar is None: return dict(base,reason='NO_NEXT_OBSERVED_OPEN')
        fill=nextbar['ask']['o'];entry_t=nextbar['first_h1'];side='bid'
        if entry_t<t or entry_t-t>120: return dict(base,reason='ENTRY_AGE_EXCEEDS_120S')
        if fill-reference>10*TICK+1e-12:return dict(base,reason='ADVERSE_ENTRY_EXCEEDS_10_TICKS')
        spread=(fill-nextbar['bid']['o'])/PIP
    else:
        fill=reference+cost*PIP;entry_t=t;side='mid';spread=None
    if not (0<stop<fill<target):return dict(base,reason='INVALID_ENTRY_STOP_TARGET')
    risk=fill-stop
    base.update(fill=fill,entry_utc=stamp(entry_t),entry_epoch=entry_t,stop_distance_pips=risk/PIP,
                cost_over_risk=(fill-reference)/risk,entry_spread_pips=spread,
                exit_index=None,exit_utc=None,exit_epoch=None,exit_price=None,r=None,outcome='OPEN',dual_touch=False,exit_gap=False)
    for j in range(i+1,len(bars)):
        x=bars[j][side];o,h,l=x['o'],x['h'],x['l']
        gap=(scenario=='BIDASK' or model=='STOP_FIRST_GAP_STRESS')
        price=None;why='';dual=l<=stop and h>=target
        if gap and o<=stop: price=o;why='STOP_GAP'
        elif gap and o>=target:price=target;why='TARGET_GAP_CAPPED'
        elif dual:
            stop_first=model=='STOP_FIRST_GAP_STRESS' or abs(o-l)<=abs(h-o)
            price=stop if stop_first else target;why='DUAL_STOP' if stop_first else 'DUAL_TARGET'
        elif l<=stop:price=stop;why='STOP'
        elif h>=target:price=target;why='TARGET'
        if price is not None:
            close=bars[j]['t']+H4
            base.update(exit_index=j,exit_utc=stamp(close),exit_epoch=close,exit_price=price,r=(price-fill)/risk,
                        outcome=why,dual_touch=dual,exit_gap='GAP' in why)
            break
    return base


def replay(indices, paths):
    accepted=[];rejected=[];blocked=[];occupied=-1
    for i in indices:
        if i<occupied:
            blocked.append({'signal_index':i,'blocking_signal_index':accepted[-1]['signal_index']});continue
        p=paths[i]
        if p['reason']:rejected.append(p);continue
        accepted.append(p)
        occupied=p['exit_index'] if p['exit_index'] is not None else math.inf
    return accepted,rejected,blocked


def independent_raw(bars, case, anchor=None, reward_risk=3.0, min_body_ratio=None, selected_indices=None):
    """Independent scalar signal, ATR and sequential raw execution control."""
    _,scenario,cost,model=case
    atr=None;seed=[];busy=-1;accepted=[];reject=[];blocked=[];signals=[]
    for i in range(1,len(bars)):
        now=bars[i]['mid'];prev=bars[i-1]['mid']
        v=max(now['h']-now['l'],abs(now['h']-prev['c']),abs(now['l']-prev['c']))
        if i<=14:
            seed.append(v)
            if i==14:atr=sum(seed)/14
        else:atr=(13*atr+v)/14
        if i<200 or atr<=0 or not(prev['o']>prev['c'] and now['c']>now['o'] and now['o']<=prev['c'] and now['c']>=prev['o'] and prev['h']>prev['l'] and now['h']>now['l']):continue
        if anchor is not None:
            _,_,lb,cap,bmin,rmin=anchor
            prior=min(z['mid']['l'] for z in bars[i-lb:i])
            if abs(now['l']-prior)/atr>cap or (now['c']-now['o'])/atr<bmin or (now['h']-now['l'])/atr<rmin:continue
        if min_body_ratio is not None and (now['c']-now['o'])/(now['h']-now['l'])<min_body_ratio:continue
        if selected_indices is not None and i not in selected_indices:continue
        signals.append(i)
        if i<busy:blocked.append((i,accepted[-1][0]));continue
        st=now['l']-.0001;ref=now['c'];tp=ref+reward_risk*(ref-st);et=bars[i]['t']+14400
        why='';fill=ref+cost*.0001
        if et>=END:
            reject.append((i,'SIGNAL_AT_OR_AFTER_CUTOFF'));continue
        if scenario=='BIDASK':
            if i+1>=len(bars):why='NO_NEXT_OBSERVED_OPEN'
            else:
                et=bars[i+1]['first_h1'];fill=bars[i+1]['ask']['o']
                if et<bars[i]['t']+14400 or et-(bars[i]['t']+14400)>120:why='ENTRY_AGE_EXCEEDS_120S'
                elif fill-ref>.000100000001:why='ADVERSE_ENTRY_EXCEEDS_10_TICKS'
        if not why and not(0<st<fill<tp):why='INVALID_ENTRY_STOP_TARGET'
        if why:reject.append((i,why));continue
        exit_i=None;price=None;outcome='OPEN'
        for j in range(i+1,len(bars)):
            x=bars[j]['bid' if scenario=='BIDASK' else 'mid']
            if (scenario=='BIDASK' or model=='STOP_FIRST_GAP_STRESS') and x['o']<=st:price=x['o'];outcome='STOP_GAP'
            elif (scenario=='BIDASK' or model=='STOP_FIRST_GAP_STRESS') and x['o']>=tp:price=tp;outcome='TARGET_GAP_CAPPED'
            elif x['l']<=st and x['h']>=tp:
                lose=model=='STOP_FIRST_GAP_STRESS' or x['o']-x['l']<=x['h']-x['o']
                price=st if lose else tp;outcome='DUAL_STOP' if lose else 'DUAL_TARGET'
            elif x['l']<=st:price=st;outcome='STOP'
            elif x['h']>=tp:price=tp;outcome='TARGET'
            if price is not None:exit_i=j;break
        r=None if price is None else (price-fill)/(fill-st)
        accepted.append((i,et,fill,st,tp,exit_i,price,r,outcome))
        busy=math.inf if exit_i is None else exit_i
    return signals,accepted,reject,blocked


def core(p):
    return (p['signal_index'],p['entry_epoch'],p['fill'],p['stop'],p['target'],p['exit_index'],p['exit_price'],p['r'],p['outcome'])


def stats(trades):
    closed=sorted((p for p in trades if p['r'] is not None),key=lambda p:(p['exit_epoch'],p['signal_index']))
    rs=[p['r'] for p in closed];wins=[r for r in rs if r>0];losses=[r for r in rs if r<0]
    eq=peak=dd=0.;streak=worst=0
    for r in rs:
        eq+=r;peak=max(peak,eq);dd=min(dd,eq-peak)
        streak=streak+1 if r<0 else 0;worst=max(worst,streak)
    loss=-sum(losses)
    return {'closed':len(rs),'wins':len(wins),'losses':len(losses),'flat':len(rs)-len(wins)-len(losses),
            'net_r':sum(rs),'win_rate_pct':100*len(wins)/len(rs) if rs else None,
            'expectancy_r':sum(rs)/len(rs) if rs else None,'profit_factor':sum(wins)/loss if loss else None,
            'no_losing_trades':loss==0,'max_closed_r_drawdown':dd,'max_losing_streak':worst}


def boundary(year, month=1, day=1):return int(dt.datetime(year,month,day,tzinfo=UTC).timestamp())


def periods():
    annual=[(str(y),boundary(y),min(boundary(y+1),END),y==2026) for y in range(2005,2027)]
    eras=[(f'{a}-{b}',boundary(a),min(boundary(b+1),END),b==2026) for a,b in ((2005,2009),(2010,2015),(2016,2021),(2022,2026))]
    trail=[(f'TRAILING_{n}Y',boundary(2026-n,10,12),END,False) for n in (1,2,3,5,10)]
    months=[];bounds=[]
    for y in range(2005,2027):
        for m in range(1,13):
            a=boundary(y,m)
            if a>=END:break
            b=boundary(y+1,1) if m==12 else boundary(y,m+1)
            months.append((f'{y}-{m:02}',a,min(b,END),b>END));bounds.append(a)
    rolls=[]
    for k,b in enumerate(bounds):
        for n in (12,24,36):
            if k>=n:rolls.append((f'{n}M_TO_{stamp(b)[:10]}',bounds[k-n],b,False))
    return annual+eras+trail,months,rolls


def period_row(trades, interval):
    label,a,b,partial=interval
    exits=[p for p in trades if p['exit_epoch'] is not None and a<p['exit_epoch']<=b]
    entries=[p for p in trades if a<=p['entry_epoch']<b]
    out={'period':label,'start_utc':stamp(a),'end_utc':stamp(b),'partial':partial,
         'entries':len(entries),'open_at_end':sum(p['entry_epoch']<b and (p['exit_epoch'] is None or p['exit_epoch']>b) for p in trades),
         'cohort_closed_r':sum(p['r'] for p in entries if p['r'] is not None),
         'cohort_open_at_cutoff':sum(p['r'] is None for p in entries)}
    out.update(stats(exits));return out


class PeriodIndex:
    def __init__(self, trades):
        self.entries=sorted(trades,key=lambda p:p['entry_epoch'])
        self.exits=sorted((p for p in trades if p['exit_epoch'] is not None),key=lambda p:p['exit_epoch'])
        self.et=[p['entry_epoch'] for p in self.entries]
        self.xt=[p['exit_epoch'] for p in self.exits]
    def row(self, interval):
        label,a,b,partial=interval
        lo=bisect.bisect_left(self.et,a);hi=bisect.bisect_left(self.et,b)
        entries=self.entries[lo:hi]
        exlo=bisect.bisect_right(self.xt,a);exhi=bisect.bisect_right(self.xt,b)
        # Every exit has positive holding time, so exits <= b belong to entries < b.
        row={'period':label,'start_utc':stamp(a),'end_utc':stamp(b),'partial':partial,
             'entries':len(entries),'open_at_end':hi-exhi,
             'cohort_closed_r':sum(p['r'] for p in entries if p['r'] is not None),
             'cohort_open_at_cutoff':sum(p['r'] is None for p in entries)}
        row.update(stats(self.exits[exlo:exhi]));return row


def longest_zero(counts):
    run=best=0
    for n in counts:run=0 if n else run+1;best=max(best,run)
    return best


class CSV:
    def __init__(self,path,fields):
        self.handle=open(path,'w',newline='');self.writer=csv.DictWriter(self.handle,fields,extrasaction='raise');self.writer.writeheader();self.count=0
    def add(self,row):self.writer.writerow(row);self.count+=1
    def close(self):self.handle.close()


def package(directory, destination):
    directory=Path(directory);destination=Path(destination)
    require(directory.resolve()!=destination.parent.resolve(), 'ZIP must be outside report directory')
    manifest={}
    for p in sorted(directory.iterdir()):
        if not p.is_file() or p.name=='file_manifest.json':continue
        h=hashlib.sha256()
        with p.open('rb') as f:
            for chunk in iter(lambda:f.read(1024*1024),b''):h.update(chunk)
        manifest[p.name]={'bytes':p.stat().st_size,'sha256':h.hexdigest()}
    write_json(directory/'file_manifest.json',manifest)
    temp=Path(str(destination)+'.tmp')
    with zipfile.ZipFile(temp,'w',zipfile.ZIP_DEFLATED,compresslevel=6) as z:
        for p in sorted(directory.iterdir()):
            if p.is_file():z.write(p,p.name)
    with zipfile.ZipFile(temp) as z:require(z.testzip() is None,'Output archive CRC failure')
    os.replace(temp,destination)


def analyse(bars, out, progress=lambda **k:None):
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    atr,raw=features(bars);indices=list(raw);grid=configs()
    paths={case[0]:{i:path_for(bars,i,case) for i in indices} for case in CASES}
    controls={};baseline={}
    for case in CASES:
        accepted,rejected,blocked=replay(indices,paths[case[0]])
        si,aa,rr,bb=independent_raw(bars,case)
        require(si==indices,'Raw signal independent parity failed')
        require([core(p) for p in accepted]==aa,'Raw full-ledger independent parity failed: '+case[0])
        require([(p['signal_index'],p['reason']) for p in rejected]==rr,'Raw rejection independent parity failed')
        require([(p['signal_index'],p['blocking_signal_index']) for p in blocked]==bb,'Raw blocking independent parity failed')
        baseline[case[0]]=accepted
        controls[case[0]]={'raw_signals':len(indices),'accepted':len(aa),'ledger_sha256':digest(aa),'rejected':len(rr),'blocked':len(bb),'independent_parity':'PASS'}
    verify_pass1(bars,raw,paths,controls)
    control_reconciliation=verify_pass2(raw,paths)
    write_json(out/'pass2_control_reconciliation.json',control_reconciliation)
    comparator_reconciliation=verify_pass3(raw,paths)
    write_json(out/'pass3_comparator_reconciliation.json',comparator_reconciliation)
    reference_reconciliation=verify_pass4(raw,paths)
    write_json(out/'pass4_reference_reconciliation.json',reference_reconciliation)
    rr3_reconciliation=verify_pass5(raw,paths)
    write_json(out/'pass5_rr3_reconciliation.json',rr3_reconciliation)
    frozen_indices=sorted({i for entry in FROZEN_ENTRIES for i,f in raw.items() if comparator_qualifies(f,entry)})
    independent_masks,independent_audit=independent_study_masks(bars)
    require(set(independent_audit)==set(raw),"Independent context raw coverage mismatch")
    for i,f in raw.items():
        for key in ("h4_atr_ratio","daily_atr_ratio"):
            a=f[key];b=independent_audit[i][key]
            require((a is None and b is None) or (a is not None and b is not None and abs(a-b)<1e-12),"Independent volatility context mismatch "+key)
        for key in ("daily_completed_count","daily_end_utc","daily_observed_h1_count"):
            require(f[key]==independent_audit[i][key],"Independent daily provenance mismatch")
    for c in grid:require(independent_masks[c[0]]==[i for i,f in raw.items() if qualifies(f,c)],"Independent volatility mask mismatch "+c[0])
    rr_paths={value:{case[0]:{i:path_for(bars,i,case,value) for i in frozen_indices} for case in CASES} for value in RR_LEVELS}
    pass6_reconciliation=verify_pass6(raw,rr_paths[STUDY_RR])
    write_json(out/"pass6_rr275_reconciliation.json",pass6_reconciliation)
    pass7_reconciliation=verify_pass7(bars,raw,rr_paths[STUDY_RR])
    write_json(out/"pass7_full_reconciliation.json",pass7_reconciliation)
    all_ledgers={};all_masks={};independent_rr_controls=[]
    baselines={(anchor[0],case[0]):replay([i for i,f in raw.items() if comparator_qualifies(f,anchor)],rr_paths[STUDY_RR][case[0]]) for anchor in FROZEN_ENTRIES for case in CASES}
    write_json(out/'independent_raw_controls.json',controls)
    write_json(out/'protocol.json',{'version':VERSION,'start_utc':stamp(START),'cutoff_exclusive_utc':stamp(END),'cases':CASES,'reference_rr':STUDY_RR,'legacy_control_rr':RR,'configurations':8,'definitions':DEFINITIONS,'source_h4_count':len(bars),'raw_indices_sha256':digest(indices)})
    (out/'PROTOCOL.md').write_text(PROTOCOL)
    shutil.copyfile(__file__,out/'research_runner.py')
    fields=list(path_for(bars,indices[0],CASES[0])) if indices else ['signal_index']
    # Fixed superset handles runs where the first path is rejected or where no signals exist.
    fields=['signal_index','signal_start','signal_close','reference','stop','target','rr_reference','reason','next_observed_delay_seconds','fill','entry_utc','entry_epoch','stop_distance_pips','cost_over_risk','entry_spread_pips','exit_index','exit_utc','exit_epoch','exit_price','r','outcome','dual_touch','exit_gap']
    writers={}
    def make(name, fs):writers[name]=CSV(out/(name+'.csv'),fs);return writers[name]
    led=make('accepted_trades',['config','case']+fields)
    rej=make('rejected_entries',['config','case']+fields)
    blk=make('blocked_signals',['config','case','signal_index','blocking_signal_index'])
    opn=make('open_at_cutoff',['config','case']+fields)
    rawpaths=make('raw_reference_rr3_paths',['case']+fields)
    study_paths=make('study_paths',['config','case']+fields)
    memberships=make('raw_memberships',['config','signal_index','qualifies_primary','qualifies_frequency','signal_branch','feature','feature_value','condition_passed'])
    cf=make('configurations',['config','family','lookback','distance_atr_max','body_atr_min','range_atr_min','boundary_dimensions','anchor','conditions_json','h4_vol_cap','daily_vol_cap','condition_scope','availability_only','body_ratio_floor','missing_feature_signals','reward_risk'])
    feature=make('raw_signal_features',['signal_index','signal_start','atr','body_atr','range_atr','close_location','body_ratio']+['distance_lb'+str(l) for l in LB]+CONTEXT_FIELDS)
    source=make('source_h4_features',['index','time','atr','first_observed_h1','h1_count']+[s+'_'+k for s in ('mid','bid','ask') for k in 'ohlc'])
    va=make('volatility_signal_audit',['signal_index','signal_close','qualifies_primary','qualifies_frequency','h4_atr_ratio','daily_atr_ratio','daily_completed_count','daily_end_utc','daily_observed_h1_count','daily_context_age_seconds'])
    for i in frozen_indices:
        f=raw[i];row=independent_audit[i]
        va.add(dict(signal_index=i,signal_close=stamp(bars[i]['t']+H4),qualifies_primary=row['primary'],qualifies_frequency=row['frequency'],**{k:f[k] for k in ('h4_atr_ratio','daily_atr_ratio','daily_completed_count','daily_end_utc','daily_observed_h1_count')},daily_context_age_seconds=bars[i]['t']+H4-epoch(f['daily_end_utc']) if f['daily_end_utc'] else None))
    for i,b in enumerate(bars):source.add(dict(index=i,time=stamp(b['t']),atr=atr[i],first_observed_h1=stamp(b['first_h1']),h1_count=b['h1_count'],**{s+'_'+k:b[s][k] for s in ('mid','bid','ask') for k in 'ohlc'}))
    for i,f in raw.items():feature.add(dict(signal_index=i,signal_start=stamp(bars[i]['t']),**{k:v for k,v in f.items() if k!='distances'},**{'distance_lb'+str(l):f['distances'][l] for l in LB}))
    for case in CASES:
        for p in paths[case[0]].values():rawpaths.add(dict(case=case[0],**p))
    per,months,rolling=periods()
    periodfields=list(period_row([],per[0]));statfields=list(stats([]))
    periodwriters=[make(n,['config','case']+periodfields) for n in ('periods','monthly','rolling')]
    sums=make('summary',['config','case','reward_risk','entry_version','condition_scope','availability_only','primary_accepted','extra_accepted','primary_signals_blocked','accepted_signal_hash','closed_signal_hash','raw_signals','raw_signal_hash','accepted_hash','accepted','rejected','blocked','open_at_cutoff','isolated_eligible','isolated_closed','isolated_net_r','blank_complete_years','blank_complete_months','longest_blank_complete_months','worst_12m_r','worst_24m_r','worst_36m_r','median_stop_pips','median_cost_over_risk','median_entry_spread_pips','median_holding_hours','max_interentry_days','max_wait_including_edges_days']+statfields)
    comparisons=make('baseline_comparisons',['config','case','baseline_closed_r','candidate_closed_r','delta_r','common','added','removed','common_r_change','added_closed_r','removed_closed_r','identity_error'])
    marginal=make('baseline_marginal_trades',['config','case','kind','signal_index','r','qualifies_candidate','displaced_by_signal_index'])
    summary_lookup={};rawhash={};acceptedhash={}
    try:
        for num,c in enumerate(grid):
            cid,family,l,d,b,r=c
            reward_risk=DEFINITION_BY_ID[cid]['rr'];case_paths=rr_paths[reward_risk]
            boundaries=study_boundaries(cid)
            cf.add(dict(config=cid,family=family,lookback=l,distance_atr_max=d,body_atr_min=b,range_atr_min=r,boundary_dimensions=boundaries,**condition_metadata(cid,raw)))
            signals=[i for i,f in raw.items() if qualifies(f,c)];signalset=set(signals);rh=digest(signals);rawhash[cid]=rh
            all_masks[cid]=set(signals)
            for i in signals:
                f=raw[i];de=DEFINITION_BY_ID[cid];pr=comparator_qualifies(f,FROZEN_ENTRIES[0]);fr=comparator_qualifies(f,FROZEN_ENTRIES[1])
                memberships.add(dict(config=cid,signal_index=i,qualifies_primary=pr,qualifies_frequency=fr,signal_branch='PRIMARY' if pr else 'EXTRA',feature=de['feature'],feature_value=f[de['feature']] if de['feature'] else None,condition_passed=condition_passes(f,de)))
            for case in CASES:
                name=case[0];meta={'config':cid,'case':name}
                accepted,rejected,blocked=replay(signals,case_paths[name]);all_ledgers[cid,name]=(accepted,rejected,blocked);ah=digest([core(p) for p in accepted]);acceptedhash[(cid,name)]=ah
                check_signals,check_ledger,check_rejected,check_blocked=independent_raw(bars,case,reward_risk=STUDY_RR,selected_indices=set(independent_masks[cid]))
                require(check_signals==signals and check_ledger==[core(p) for p in accepted],'Independent conditional full-ledger parity failed '+cid+' '+name)
                require(check_rejected==[(p['signal_index'],p['reason']) for p in rejected] and check_blocked==[(p['signal_index'],p['blocking_signal_index']) for p in blocked],'Independent conditional rejection/block parity failed')
                if reward_risk==3.0:require([core(p) for p in accepted]==[core(p) for p in baselines[anchor_for(cid)[0],name][0]],'RR3 study/reference parity failed')
                independent_rr_controls.append({'config':cid,'case':name,'raw_signal_hash':rh,'accepted':len(accepted),'rejected':len(rejected),'blocked':len(blocked),'accepted_hash':ah,'parity':'PASS'})
                for i in signals:study_paths.add(dict(meta,**case_paths[name][i]))
                for p in accepted:
                    led.add(dict(meta,**p))
                    if p['r'] is None:opn.add(dict(meta,**p))
                for p in rejected:rej.add(dict(meta,**p))
                for p in blocked:blk.add(dict(meta,**p))
                periodrows=[];period_index=PeriodIndex(accepted)
                for writer,intervals in zip(periodwriters,(per,months,rolling)):
                    group=[]
                    for interval in intervals:
                        row=period_index.row(interval);writer.add(dict(meta,**row));group.append(row)
                    periodrows.append(group)
                annual=periodrows[0][:22];complete_months=[p['entries'] for p in periodrows[1] if not p['partial']]
                isolated=[case_paths[name][i] for i in signals if not case_paths[name][i]['reason']]
                realised=stats(accepted);med=lambda seq:statistics.median(seq) if seq else None
                entries=sorted(p['entry_epoch'] for p in accepted)
                gaps=[(y-x)/86400 for x,y in zip(entries,entries[1:])]
                edge=[START]+entries+[END]
                s=dict(meta,raw_signals=len(signals),raw_signal_hash=rh,accepted_hash=ah,accepted=len(accepted),rejected=len(rejected),blocked=len(blocked),open_at_cutoff=sum(p['r'] is None for p in accepted),
                       isolated_eligible=len(isolated),isolated_closed=sum(p['r'] is not None for p in isolated),isolated_net_r=sum(p['r'] for p in isolated if p['r'] is not None),
                       blank_complete_years=sum(p['entries']==0 for p in annual if not p['partial']),blank_complete_months=complete_months.count(0),longest_blank_complete_months=longest_zero(complete_months),
                       median_stop_pips=med([p['stop_distance_pips'] for p in accepted]),median_cost_over_risk=med([p['cost_over_risk'] for p in accepted]),median_entry_spread_pips=med([p['entry_spread_pips'] for p in accepted if p['entry_spread_pips'] is not None]),
                       median_holding_hours=med([(p['exit_epoch']-p['entry_epoch'])/3600 for p in accepted if p['exit_epoch'] is not None]),max_interentry_days=max(gaps) if gaps else None,max_wait_including_edges_days=max((y-x)/86400 for x,y in zip(edge,edge[1:])),**realised)
                for n in (12,24,36):s[f'worst_{n}m_r']=min(p['net_r'] for p in periodrows[2] if p['period'].startswith(str(n)+'M'))
                s.update(reward_risk=reward_risk,entry_version=cid.split('__')[0],condition_scope=DEFINITION_BY_ID[cid]['scope'],availability_only=DEFINITION_BY_ID[cid]['availability'],primary_accepted=sum(independent_audit[p['signal_index']]['primary'] for p in accepted),extra_accepted=sum(not independent_audit[p['signal_index']]['primary'] for p in accepted),primary_signals_blocked=sum(independent_audit[p['signal_index']]['primary'] for p in blocked),accepted_signal_hash=digest([p['signal_index'] for p in accepted]),closed_signal_hash=digest([p['signal_index'] for p in accepted if p['r'] is not None]))
                sums.add(s);summary_lookup[(cid,name)]=s
                ba={p['signal_index']:p for p in baselines[anchor_for(cid)[0],name][0]};ca={p['signal_index']:p for p in accepted}
                added=ca.keys()-ba.keys();removed=ba.keys()-ca.keys();common=ba.keys()&ca.keys()
                val=lambda p:0. if p['r'] is None else p['r']
                ar=sum(val(ca[i]) for i in added);rr=sum(val(ba[i]) for i in removed);cr=sum(val(ca[i])-val(ba[i]) for i in common)
                br=sum(val(p) for p in ba.values());delta=realised['net_r']-br
                err=delta-(ar-rr+cr);require(abs(err)<1e-8,'Marginal replay R identity failed')
                comparisons.add(dict(meta,baseline_closed_r=br,candidate_closed_r=realised['net_r'],delta_r=delta,common=len(common),added=len(added),removed=len(removed),common_r_change=cr,added_closed_r=ar,removed_closed_r=rr,identity_error=err))
                cb={x['signal_index']:x['blocking_signal_index'] for x in blocked}
                bb={x['signal_index']:x['blocking_signal_index'] for x in baselines[anchor_for(cid)[0],name][2]}
                for kind,items,ledger,blocks in (('ADDED',added,ca,bb),('REMOVED',removed,ba,cb)):
                    for i in sorted(items):marginal.add(dict(meta,kind=kind,signal_index=i,r=ledger[i]['r'],qualifies_candidate=i in signalset,displaced_by_signal_index=blocks.get(i)))
            progress(phase='REPLAY_AND_REPORTS',completed_configurations=num+1,total_configurations=8)
        neighbours=make('neighbours',['config','neighbour','dimension','case','net_r','neighbour_net_r','accepted','neighbour_accepted','same_raw_stream','same_accepted_stream'])
        for cid,oid,dimension in condition_neighbours():
            for case in CASES:
                name=case[0];x=summary_lookup[cid,name];y=summary_lookup[oid,name]
                neighbours.add(dict(config=cid,neighbour=oid,dimension=dimension,case=name,net_r=x['net_r'],neighbour_net_r=y['net_r'],accepted=x['accepted'],neighbour_accepted=y['accepted'],same_raw_stream=rawhash[cid]==rawhash[oid],same_accepted_stream=acceptedhash[cid,name]==acceptedhash[oid,name]))
        require(sums.count==64 and periodwriters[0].count==64*len(per) and periodwriters[1].count==64*len(months) and periodwriters[2].count==64*len(rolling),'Incomplete result matrix')
        duplicate=collections.defaultdict(list)
        for c,h in rawhash.items():duplicate[h].append(c)
        write_json(out/'duplicate_raw_streams.json',[v for v in duplicate.values() if len(v)>1])
        require(len(independent_rr_controls)==64,'Incomplete independent RR controls')
        write_json(out/'independent_conditional_controls.json',independent_rr_controls)
        component_counts=paired_reports(out,all_ledgers,all_masks)
        write_json(out/'output_row_counts.json',dict({n:w.count for n,w in writers.items()},**component_counts))
        write_json(out/'RUN_COMPLETE.json',{'version':VERSION,'status':'COMPLETE','configurations':8,'execution_cases':8,'fixed_anchors':2,'pass1_anchor_parity':'PASS','pass2_control_parity':'PASS','pass2_control_cases':104,'pass3_comparator_parity':'PASS','pass3_comparator_cases':16,'pass4_reference_parity':'PASS','pass4_reference_cases':16,'frozen_entry_versions':2,'fixed_study_rr':STUDY_RR,'new_conditional_definitions':3,'availability_definitions':1,'retained_control_definitions':5,'pass7_control_parity':'PASS','pass7_control_cases':144,'volatility_levels':VOL_LEVELS,'pass6_rr275_parity':'PASS','pass6_rr275_cases':16,'pass5_rr3_parity':'PASS','pass5_rr3_cases':16,'independent_conditional_full_replay_cases':64,'independent_volatility_context_and_masks':'PASS','summary_rows':64,'independent_raw_parity':'PASS','scope':'Exploratory standalone historical research; not portfolio admission or prospective profitability'})
    finally:
        for w in writers.values():w.close()


STATE={'status':'IDLE','version':VERSION,'read_only':True,'orders_sent':False,'requested_start':stamp(START),'cutoff_exclusive':stamp(END)}
STATE_LOCK=threading.Lock()
START_LOCK=threading.Lock()
OUTPUT=Path(os.environ.get('EURUSD_H4_PASS8_OUTPUT_DIR', '/data/eurusd_h4_pass8_research' if Path('/data').is_dir() else str(Path(tempfile.gettempdir())/'eurusd_h4_pass8_research')))


def update(**kwargs):
    with STATE_LOCK:STATE.update(kwargs,updated_at_utc=stamp(time.time()))


def snapshot():
    with STATE_LOCK:return dict(STATE)


def run():
    lock=None;work=None
    try:
        OUTPUT.mkdir(parents=True,exist_ok=True)
        import fcntl
        lock=open(OUTPUT/'research.lock','a')
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            update(status='OTHER_WORKER_RUNNING',message='Use one process/worker');return
        update(status='RUNNING',phase='SELF_TEST')
        (OUTPUT/RESULT_NAME).unlink(missing_ok=True)
        self_test()
        work=Path(tempfile.mkdtemp(prefix='pass1_',dir=OUTPUT));reports=work/'reports';reports.mkdir()
        explicit=os.environ.get('EURUSD_H4_PASS8_FROZEN_DATA')
        cached=OUTPUT/SOURCE_NAME
        beside=Path(__file__).resolve().parent/SOURCE_NAME
        prior=Path(os.environ.get('EURUSD_H4_PASS1_OUTPUT_DIR','/data/eurusd_h4_pass1_research' if Path('/data').is_dir() else str(Path(tempfile.gettempdir())/'eurusd_h4_pass1_research')))/SOURCE_NAME
        prior2=Path(os.environ.get('EURUSD_H4_PASS2_OUTPUT_DIR','/data/eurusd_h4_pass2_research' if Path('/data').is_dir() else str(Path(tempfile.gettempdir())/'eurusd_h4_pass2_research')))/SOURCE_NAME
        prior3=Path(os.environ.get('EURUSD_H4_PASS3_OUTPUT_DIR','/data/eurusd_h4_pass3_research' if Path('/data').is_dir() else str(Path(tempfile.gettempdir())/'eurusd_h4_pass3_research')))/SOURCE_NAME
        prior4=Path(os.environ.get('EURUSD_H4_PASS4_OUTPUT_DIR','/data/eurusd_h4_pass4_research' if Path('/data').is_dir() else str(Path(tempfile.gettempdir())/'eurusd_h4_pass4_research')))/SOURCE_NAME
        prior5=Path(os.environ.get('EURUSD_H4_PASS5_OUTPUT_DIR','/data/eurusd_h4_pass5_research' if Path('/data').is_dir() else str(Path(tempfile.gettempdir())/'eurusd_h4_pass5_research')))/SOURCE_NAME
        prior6=Path(os.environ.get('EURUSD_H4_PASS6_OUTPUT_DIR','/data/eurusd_h4_pass6_research' if Path('/data').is_dir() else str(Path(tempfile.gettempdir())/'eurusd_h4_pass6_research')))/SOURCE_NAME
        prior7=Path(os.environ.get('EURUSD_H4_PASS7_OUTPUT_DIR','/data/eurusd_h4_pass7_research' if Path('/data').is_dir() else str(Path(tempfile.gettempdir())/'eurusd_h4_pass7_research')))/SOURCE_NAME
        source=Path(explicit) if explicit else next((p for p in (cached,beside,prior7,prior6,prior5,prior4,prior3,prior2,prior) if p.exists()),None)
        if source is not None:
            require(source.is_file(),'Explicit frozen source does not exist')
            h4,h1,receipts,manifest=read_source(source)
            update(phase='VALIDATE_FROZEN_SOURCE')
            bars=validate_source(h4,h1)
            if source.resolve()!=cached.resolve():shutil.copyfile(source,cached)
        else:
            h4,r4=fetch('H4','MBA',update);h1,r1=fetch('H1','M',update);receipts=r4+r1
            update(phase='VALIDATE_FETCHED_SOURCE')
            bars=validate_source(h4,h1);manifest=freeze_source(cached,h4,h1,receipts)
        require(digest(h4)==PASS1_PINS['source']['members']['h4.json'] and digest(h1)==PASS1_PINS['source']['members']['h1.json'],'Pinned Pass1 source prices changed; use the original frozen archive')
        write_json(reports/'source_manifest.json',manifest)
        write_json(reports/'fetch_receipts.json',receipts)
        gaps=[{'previous_start':stamp(a['t']),'next_start':stamp(b['t']),'hours_between_starts':(b['t']-a['t'])/3600} for a,b in zip(bars,bars[1:]) if b['t']-a['t']!=H4]
        write_json(reports/'source_coverage.json',{'first_start':stamp(bars[0]['t']),'last_nominal_close':stamp(bars[-1]['t']+H4),'h4_count':len(h4),'h1_count':len(h1),'h1_h4_parity':'PASS','partial_h4_bins':sum(b['h1_count']<4 for b in bars),'year_counts':dict(collections.Counter(str(dt.datetime.fromtimestamp(b['t'],UTC).year) for b in bars)),'gaps':gaps})
        analyse(bars,reports,update)
        shutil.copyfile(cached,reports/SOURCE_NAME)
        update(phase='PACKAGE_RESULTS')
        package(reports,OUTPUT/RESULT_NAME)
        update(status='COMPLETE',phase='COMPLETE',results='/results',source='/source')
    except Exception as e:
        # Only controlled validation errors are exposed. Broker exception bodies/token URLs never logged.
        msg=str(e) if isinstance(e,ValueError) else type(e).__name__+'; see source/protocol and retry'
        update(status='ERROR',phase='ERROR',error=msg)
        try:
            if work is None:work=Path(tempfile.mkdtemp(prefix='pass1_error_',dir=OUTPUT))
            errors=work/'failure_only';errors.mkdir(exist_ok=True)
            write_json(errors/'ERROR.json',snapshot());(errors/'PROTOCOL.md').write_text(PROTOCOL)
            package(errors,OUTPUT/RESULT_NAME)
        except Exception:pass
    finally:
        if lock:lock.close()
        if work:shutil.rmtree(work,ignore_errors=True)


def start():
    with START_LOCK:
        if snapshot()['status']=='IDLE':
            update(status='STARTING')
            threading.Thread(target=run,daemon=True).start()


def response(method,path):
    if method not in ('GET','HEAD'):return 405,'application/json',canonical({'error':'Read-only: GET/HEAD only'}),None
    path=urllib.parse.urlsplit(path).path
    if path in ('/','/health','/status'):
        return 200,'application/json',canonical(snapshot()),None
    s=snapshot()['status']
    if path=='/results':
        if s not in ('COMPLETE','ERROR'):return 409,'application/json',canonical({'status':s,'message':'Results not ready; poll /status'}),None
        p=OUTPUT/RESULT_NAME
    elif path=='/source':
        if s!='COMPLETE':return 409,'application/json',canonical({'status':s,'message':'Validated source not ready'}),None
        p=OUTPUT/SOURCE_NAME
    else:return 404,'application/json',canonical({'error':'Use /status, /results or /source'}),None
    if not p.is_file():return 404,'application/json',canonical({'error':'Archive unavailable'}),None
    return 200,'application/zip',None,p


def app(environ,start_response):
    if os.environ.get('EURUSD_H4_PASS8_AUTOSTART','true').lower()!='false':start()
    method=environ.get('REQUEST_METHOD','GET');code,mime,body,path=response(method,environ.get('PATH_INFO','/'))
    size=path.stat().st_size if path else len(body)
    headers=[('Content-Type',mime),('Content-Length',str(size)),('Cache-Control','no-store')]
    if path:headers.append(('Content-Disposition','attachment; filename="'+path.name+'"'))
    start_response(str(code)+' '+{200:'OK',404:'Not Found',405:'Method Not Allowed',409:'Conflict'}[code],headers)
    if method=='HEAD':return []
    if not path:return [body]
    def chunks():
        with path.open('rb') as f:
            for chunk in iter(lambda:f.read(1024*1024),b''):yield chunk
    return chunks()


application=app


class Handler(BaseHTTPRequestHandler):
    def handle_request(self):
        def headers(status,values):
            self.send_response(int(status.split()[0]))
            for k,v in values:self.send_header(k,v)
            self.end_headers()
        try:
            for chunk in app({'REQUEST_METHOD':self.command,'PATH_INFO':self.path},headers):self.wfile.write(chunk)
        except (BrokenPipeError,ConnectionResetError):pass
    do_GET=handle_request
    do_HEAD=handle_request
    do_POST=handle_request
    do_PUT=handle_request
    do_DELETE=handle_request
    def log_message(self,*args):pass


def self_test():
    require(len(configs())==8 and len(CASES)==8,'Configuration count')
    require(tuple(map(len,periods()))==(31,262,714),'Period boundaries')
    def bar(t,o,h,l,c):
        p=dict(o=o,h=h,l=l,c=c)
        return {'t':t,'mid':dict(p),'bid':dict(p),'ask':dict(p),'first_h1':t,'h1_count':4}
    b=[bar(START,1.1,1.1006,1.099,1.1005),bar(START+H4,1.1006,1.11,1.09,1.1)]
    p=path_for(b,0,CASES[0]);require(p['outcome']=='DUAL_STOP' and abs(p['r']+1)<1e-12,'Dual stop-first')
    b[1]=bar(START+H4,1.098,1.11,1.09,1.1)
    require(path_for(b,0,CASES[0])['r']<-1,'Gap loss must exceed -1R')
    b[1]=bar(START+H4,1.1005,1.1006,1.1004,1.1005)
    require(path_for(b,0,CASES[-2])['outcome']=='OPEN','Open cutoff retention')
    b[1]['first_h1']+=3600
    require(path_for(b,0,CASES[-2])['reason']=='ENTRY_AGE_EXCEEDS_120S','Age rejection')
    b[1]['first_h1']-=3600;b[1]['ask']['o']=1.10061
    require(path_for(b,0,CASES[-2])['reason']=='ADVERSE_ENTRY_EXCEEDS_10_TICKS','Adverse rejection')
    p={0:{'signal_index':0,'reason':'','exit_index':2},1:{'signal_index':1,'reason':'','exit_index':3},2:{'signal_index':2,'reason':'','exit_index':None}}
    a,_,bl=replay([0,1,2],p);require([v['signal_index'] for v in a]==[0,2] and len(bl)==1,'P0 exit-candle handling')
    require(longest_zero([0,0,1,0,0,0])==3,'Zero-month drought')
    require(response('POST','/')[0]==405,'No writes allowed')
    require(len(DEFINITIONS)==8 and len(comparison_pairs())==26 and len(ANCHORS)==2,'Pass3 study count')
    return {'status':'PASS','scope':'Synthetic software checks only; no historical result'}


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--self-test',action='store_true');parser.add_argument('--run-once',action='store_true');parser.add_argument('--output-dir')
    args=parser.parse_args()
    if args.output_dir:OUTPUT=Path(args.output_dir)
    if args.self_test:print(json.dumps(self_test()));sys.exit(0)
    if args.run_once:
        run();print(json.dumps(snapshot()));sys.exit(0 if snapshot()['status']=='COMPLETE' else 1)
    if os.environ.get('EURUSD_H4_PASS8_AUTOSTART','true').lower()!='false':start()
    ThreadingHTTPServer(('0.0.0.0',int(os.environ.get('PORT','8000'))),Handler).serve_forever()
