#!/usr/bin/env python3
"""EUR/USD H4 LONG Pass 4. Standard-library Python 3.10+. Research ONLY.
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

VERSION = 'EURUSD_H4_LONG_PASS4_STRUCTURE_FREQUENCY_V1_2026_10_12'
UTC = dt.timezone.utc
START = int(dt.datetime(2005, 1, 1, tzinfo=UTC).timestamp())
END = int(dt.datetime(2026, 10, 12, tzinfo=UTC).timestamp())
H4 = 14400
PIP = .0001
TICK = .00001
RR = 3.0
LB = (20, 30, 40, 50, 60, 100, 150, 200)
DIST = (.05, .10, .20, .35, .50)
BODY = (.50, .75, 1., 1.25)
RANGE = (.75, 1., 1.50)
MODELS = ('STOP_FIRST_GAP_STRESS', 'NEAREST_OPEN_SENSITIVITY')
CASES = [(f'MID_{p}P_{m}', 'MID', p, m) for p in (1, 2, 4) for m in MODELS]
CASES += [(f'BIDASK_BOUND10_{m}', 'BIDASK', 0, m) for m in MODELS]
SOURCE_NAME = 'EURUSD_H4_PASS1_FROZEN_DATA.zip'
RESULT_NAME = 'EURUSD_H4_LONG_PASS4_RESULTS.zip'
PROTOCOL = "# EUR/USD H4 LONG — Pass 4 structure and frequency map\nVersion: EURUSD_H4_LONG_PASS4_STRUCTURE_FREQUENCY_V1_2026_10_12\n50 predeclared definitions, eight unchanged execution cases, RR3 fixed.\nRepeatedly inspected historical research, not unseen OOS or portfolio admission.\n\n## Frozen comparators and finite grid\nFREQUENCY control: LB40, absolute low-distance<=.50 current H4 ATR, body>=1.25ATR,\nrange>=1.50ATR, body/range>=.60. Exact Pass3 STRONG_BODY__BR0.6_CNONE.\nQUALITY control: same, except body>=1.40ATR. Exact Pass3\nSTRONG_BODY__BODY1.4_BR0.6. Labels describe previous historical tradeoffs, not\nforecasts or a guarantee that any configuration has high quality/frequency.\n\nFor EACH comparator test lookback20/30/40/50/60 observed H4 bars crossed with\nabsolute low-distance cap.35/.50/.65/.80/1.00ATR. 25 per comparator, 50 total,\nincluding both unchanged controls at40/.50. Body floors stay1.25 or1.40; range\nstays1.50ATR; body/range stays.60; no strong-close, volatility, daily, momentum,\nsession or weekday filter. No RR/range/body-ratio retuning. No complementary\ntrigger or naive combination of the two comparator trade streams.\nLookback20/60 test wider structural neighbourhood, 30/50 bridge the previous40.\nDistance.35 tightens, .65/.80/1.00 relax the old.50 cap. All weak rows retained;\nupper1.00 is a tested boundary, not permission to extend indefinitely.\nCommon first eligible signal index200 observed H4 bars remains unchanged so\ncontrols match. Prior-low excludes the signal. Existing distance features remain,\nnew30/50 lookbacks use identical causal calculation and are reported explicitly.\n\n## Source and control gates\nSame native OANDA source and cutoff as Pass1–3: EUR_USD H4 MID/BID/ASK and H1 MID,\n2005-01-01 through2026-10-12 00:00UTC exclusive. Source price hashes, eight raw\ncontrols, all16 original Pass1 anchor ledgers, 104 selected Pass2 control ledgers,\nand both current Pass3 comparator ledgers across eight cases (16) must match\nbefore any new result reports. These are reproduction checks on examined data.\nAny source/control mismatch stops the run and returns failure-only output.\n\n## Chronology, marginal attribution and reporting\nEvery definition gets full separate chronological one-position replay at all\neight cases. Fixed comparator reference includes body/range.60 (not the older\nunfiltered anchor). Report additions/removals/common R, candidate membership,\nblocked-signal displacement, rejected entries and unfinished trades.\nAdditional105 predeclared comparison pairs:40 distance relaxations,40 adjacent\nlookback shortenings,25 matched-geometry body1.40->1.25 comparisons; eight cases\neach. A shorter lookback can change which structure qualifies rather than simply\nadd a nested mask. More raw eligibility can still displace accepted trades.\nNever assume performance changes equal naive raw-signal ledger subtraction.\nAdded/removed/common R reconciliation is checked for every reference pair.\n\nAll calendar years, four eras, trailing1/2/3/5/10Y, monthly, every complete\nmonthly-ended rolling12/24/36M window including zeros, partial2026/October flags,\nblank years/months, recent activity, inter-entry/wait times, isolated versus P0\noutcomes and cost/stop metrics. Closed additive R drawdown is not NAV or marked\nportfolio risk. Neighbours compare adjacent structure/distance levels and the two\nbody comparators at matched geometry. Duplicate raw/accepted streams identified.\nNo automatic best-row choice, acceptance threshold, optimisation of portfolio\ncontribution or live changes. Judge interior regions and the frequency gained\nagainst risk/return surrendered, not just the largest historical total R.\n\n## Inherited causal source and execution (unchanged)\nNative unsmoothed OANDA H4, dailyAlignment0/timezoneUTC. Independent native H1\nMID aggregation exactly reproduces each H4 MID bin. Source prices retain native\nprecision. Missing bars/days are not filled. Partial market-opening/closing bins\nare retained and flagged. First eligible signal index200, strict prior-red and\ncurrent-green inclusive engulf(open<=previous close, close>=previous open), both\nranges positive. ATR14 TR1..14 mean seed then Wilder through current signal.\nAbsolute signal low minus minimum prior LB low/current ATR, signal excluded.\nBody/range geometry and body/range ratio use completed signal candle only.\nOther inherited causal daily/volatility features are descriptive only; they do\nnot filter this pass. Daily context uses only completed UTC days END<=signal\nclose and200 completed observed days for daily fields.\n\nStop=signal MID low-.0001 (one pip); target=MID close+3*(MID close-stop).\nSix MID proxy cases: assumed entry=MID close+1/+2/+4 adverse pips, target fixed,\nR denominator=assumed fill-stop. Four pips is stress, not measured normal cost.\nSTOP_FIRST_GAP_STRESS retains adverse opening-stop gaps (loss can exceed1R), caps\nfavourable target gaps, stop-first dual-touch. MID NEAREST_OPEN_SENSITIVITY\nignores opening gaps and uses nearer low/high to open, stop on equal distance.\nTwo BIDASK cases: next observed H4 ASK open with earliest constituent H1 timestamp\nas entry-time proxy. Reject delay>120sec, adverse fill>10ticks (.0001), invalid\nstop/entry/target or no next bar. BID exits; both models retain stop gaps and cap\ntarget gaps, only dual touch differs. No extra entry slippage. Historical candle\nquote-side approximation cannot establish actual fill timing, liquidity or\nintrabar ordering. Live tick rounding, financing and account fees unmodelled.\nEntry at/after cutoff rejected; no signal-bar exits. One standalone position,\nexit-candle signal permitted, rejected entries do not occupy, open-at-cutoff\nretains occupation and blank R. Entries[start,end), realised exits(start,end],\nnominal H4 exit-bin END timestamps. No forced cutoff exit.\nNo incumbent portfolio overlap/gates/NAV compounding/currency conversion in this\nstudy. Later frozen independent confirmation, exact portfolio comparison and\nprospective execution evidence remain separate gates.\n\n## Operation\nSingle-file Python3.10+ standard library, same separate read-only research service,\none process/worker. Reuse original EURUSD_H4_PASS1_FROZEN_DATA.zip from current\nor prior Pass1/2/3 cache or beside app.py. If absent, refetch same source and\nrequire pinned price hashes. Explicit missing/corrupt source fails. No silent\nhistory extension. Optional EURUSD_H4_PASS4_FROZEN_DATA / PASS4_OUTPUT_DIR;\ndefault /data/eurusd_h4_pass4_research when /data exists, temporary cache otherwise.\nGET/HEAD /status /health /results /source; no write or broker order calls.\nFull results on COMPLETE or failure-only ZIP on ERROR. During code delivery,\nonly actual prior source/reference parity is evaluated; new selectors, replay,\nfrequency attribution and reports are tested synthetically. No new Pass4\nhistorical performance is assessed or ranked before the user's research run.\nPortfolio reference PORTFOLIO32_2026_10_05_EURCHF_H1_SHORT_PRIMARY_RR3P50_V1.\n"

PASS1_PINS = {'anchors': {'M_L40_D0.35_B0.75_R1': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '106', 'accepted_hash': 'd979d86a2d02e760a8a1c6fafb0bdae84bda735a0edfc7744d75ae817fcba29a', 'blocked': '3', 'net_r': '8.3450542151461', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '8'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '106', 'accepted_hash': 'd979d86a2d02e760a8a1c6fafb0bdae84bda735a0edfc7744d75ae817fcba29a', 'blocked': '3', 'net_r': '8.3450542151461', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '8'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '114', 'accepted_hash': '48f8a41dff9a17722f2de6fc36edc9ef1469b92049306d3f2a07ea4b907e699c', 'blocked': '3', 'net_r': '19.081030949268005', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '114', 'accepted_hash': '78a2087e6dd35db8eecdd723d2c8ba75ff335a568aeacb543e395696ae236958', 'blocked': '3', 'net_r': '19.026589688523014', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '114', 'accepted_hash': '03161b82a37fe9af87cbe8f2c961718b18da7e1cea17d85e1bd80bf7a139e7a9', 'blocked': '3', 'net_r': '16.308044523646373', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '114', 'accepted_hash': 'f6c9a160a4b082c827ae188a6230a19c9431c5d746c558c6ae204b2459efbb7a', 'blocked': '3', 'net_r': '16.254372207262186', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '114', 'accepted_hash': '40bba3c1fe2437e77333301b71d69137e459973c633fba67923e86a11fd218a3', 'blocked': '3', 'net_r': '11.152458563507945', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '114', 'accepted_hash': '01b8778e70d74aa68df953647bea0649fedd55383dcd1847aa5d6efa475c0848', 'blocked': '3', 'net_r': '11.100260761310135', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}}, 'M_L40_D0.5_B1.25_R1.5': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '52', 'accepted_hash': '7af667268c5fda50ea8eaf3a902ac2e9868163f08bb4919e188cea8fc459d00d', 'blocked': '1', 'net_r': '7.355862354561232', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '5'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '52', 'accepted_hash': '7af667268c5fda50ea8eaf3a902ac2e9868163f08bb4919e188cea8fc459d00d', 'blocked': '1', 'net_r': '7.355862354561232', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '5'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '57', 'accepted_hash': 'c7660408058093dd74beda9f4445e82cb3a9c934fd51ef9d957c669ab9adafa6', 'blocked': '1', 'net_r': '13.896274044063015', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '57', 'accepted_hash': '3d2d17610f9ea033c348a76eb2dd1ed7b17e42afb0b02e45b18a7d25c8827c4a', 'blocked': '1', 'net_r': '13.841832783318022', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '57', 'accepted_hash': '2237d6a869858b55476744b6c270088252e3915dbfbe9661f3adfc4c89d9c923', 'blocked': '1', 'net_r': '12.831981788009386', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '57', 'accepted_hash': 'c1eccd64fd9c8dbfafeb1754e1e40388071d28fbef4845b9dc999aeab951dcb9', 'blocked': '1', 'net_r': '12.7783094716252', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '57', 'accepted_hash': '8064ca9c491dad8154834cb65ae2ccb50291e1ff36c5ead6807d8634cda37405', 'blocked': '1', 'net_r': '10.81186068156517', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '57', 'accepted_hash': '7ebb1926f01bda5d7bdc48dee803ee0a1f0dd80eee5bf901edf6b0eec994a02c', 'blocked': '1', 'net_r': '10.75966287936736', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}}}, 'raw_controls': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': 1407, 'blocked': 1065, 'independent_parity': 'PASS', 'ledger_sha256': '6dcb73d2b7ed43b9e6da129d40bed9af12693f58dba015af9808f1e30a9c6929', 'raw_signals': 2669, 'rejected': 197}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': 1407, 'blocked': 1065, 'independent_parity': 'PASS', 'ledger_sha256': 'a9ddf63bdce468287f3f8655f8da82e20a4e89638f07d3d764d130eb68ffdc36', 'raw_signals': 2669, 'rejected': 197}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': 1512, 'blocked': 1157, 'independent_parity': 'PASS', 'ledger_sha256': '77659c43e40af3d7e14f392825771ea5331488e695165de183ddaef54df0329a', 'raw_signals': 2669, 'rejected': 0}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': 1512, 'blocked': 1157, 'independent_parity': 'PASS', 'ledger_sha256': '3b56b148b22f70b7904e0d70ec391a48ae6897ce119340119c1d3f0989266ad5', 'raw_signals': 2669, 'rejected': 0}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': 1512, 'blocked': 1157, 'independent_parity': 'PASS', 'ledger_sha256': 'fe8932a92e7bcbc7c179d1f615fc787d970035555c01a715a1afbf02edfc29bf', 'raw_signals': 2669, 'rejected': 0}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': 1512, 'blocked': 1157, 'independent_parity': 'PASS', 'ledger_sha256': 'f690dd8becdf7df86835afa98ae4b30bc88ca719a82e01a1967a74a6e4973042', 'raw_signals': 2669, 'rejected': 0}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': 1512, 'blocked': 1157, 'independent_parity': 'PASS', 'ledger_sha256': '163d14184269d601d935c3e68c7bf33540145b8b796889e5543dd9c9e3bbc27b', 'raw_signals': 2669, 'rejected': 0}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': 1512, 'blocked': 1157, 'independent_parity': 'PASS', 'ledger_sha256': '5beec9478053c6350ede8faa8a00683c3c549b1779fb2bf4ea10e61e1650e7a6', 'raw_signals': 2669, 'rejected': 0}}, 'source': {'alignment': 'UTC_00_UNSMOOTHED', 'end': 1791763200, 'instrument': 'EUR_USD', 'members': {'h1.json': '258c40216bef3deb5a74bfaaa2403b4bd9f4a88533dc734c73f9e89a7f9569bb', 'h4.json': '5c7a5ced72379c5696251200c2447dcb142116ed97149d0d4379d42a06381087', 'receipts.json': '0f37599b041b5e45649c81ae6771745af0da3dfd456130c1cb5d3e17c9ae692d'}, 'start': 1104537600}}

PASS2_PINS = {'BROAD__CONTROL': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '106', 'accepted_hash': 'd979d86a2d02e760a8a1c6fafb0bdae84bda735a0edfc7744d75ae817fcba29a', 'blocked': '3', 'net_r': '8.3450542151461', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '8'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '106', 'accepted_hash': 'd979d86a2d02e760a8a1c6fafb0bdae84bda735a0edfc7744d75ae817fcba29a', 'blocked': '3', 'net_r': '8.3450542151461', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '8'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '114', 'accepted_hash': '48f8a41dff9a17722f2de6fc36edc9ef1469b92049306d3f2a07ea4b907e699c', 'blocked': '3', 'net_r': '19.081030949268005', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '114', 'accepted_hash': '78a2087e6dd35db8eecdd723d2c8ba75ff335a568aeacb543e395696ae236958', 'blocked': '3', 'net_r': '19.026589688523014', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '114', 'accepted_hash': '03161b82a37fe9af87cbe8f2c961718b18da7e1cea17d85e1bd80bf7a139e7a9', 'blocked': '3', 'net_r': '16.308044523646373', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '114', 'accepted_hash': 'f6c9a160a4b082c827ae188a6230a19c9431c5d746c558c6ae204b2459efbb7a', 'blocked': '3', 'net_r': '16.254372207262186', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '114', 'accepted_hash': '40bba3c1fe2437e77333301b71d69137e459973c633fba67923e86a11fd218a3', 'blocked': '3', 'net_r': '11.152458563507945', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '114', 'accepted_hash': '01b8778e70d74aa68df953647bea0649fedd55383dcd1847aa5d6efa475c0848', 'blocked': '3', 'net_r': '11.100260761310135', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}}, 'BROAD__H4_VOL_LE_1': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '37', 'accepted_hash': 'a645d7817f780ea1710d5af766c311323e8cc3db89eae1e16147eb67201678e8', 'blocked': '0', 'net_r': '6.202223906365382', 'raw_signal_hash': '5004ab2e942e9c9fa51828995e5e084ad3231f0ad2d619a5da248066e9f0cceb', 'rejected': '3'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '37', 'accepted_hash': 'a645d7817f780ea1710d5af766c311323e8cc3db89eae1e16147eb67201678e8', 'blocked': '0', 'net_r': '6.202223906365382', 'raw_signal_hash': '5004ab2e942e9c9fa51828995e5e084ad3231f0ad2d619a5da248066e9f0cceb', 'rejected': '3'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '40', 'accepted_hash': '62dba3d369214963624a57a203b2b16527c9fb56d17c32cf02a9e83d1b2cb896', 'blocked': '0', 'net_r': '6.781459534622908', 'raw_signal_hash': '5004ab2e942e9c9fa51828995e5e084ad3231f0ad2d619a5da248066e9f0cceb', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '40', 'accepted_hash': '62dba3d369214963624a57a203b2b16527c9fb56d17c32cf02a9e83d1b2cb896', 'blocked': '0', 'net_r': '6.781459534622908', 'raw_signal_hash': '5004ab2e942e9c9fa51828995e5e084ad3231f0ad2d619a5da248066e9f0cceb', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '40', 'accepted_hash': '58a32b8994555cdb69f0ec51959d41056454ad15f6c978e9a4f6f1e3da4c2512', 'blocked': '0', 'net_r': '5.631101006071993', 'raw_signal_hash': '5004ab2e942e9c9fa51828995e5e084ad3231f0ad2d619a5da248066e9f0cceb', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '40', 'accepted_hash': '58a32b8994555cdb69f0ec51959d41056454ad15f6c978e9a4f6f1e3da4c2512', 'blocked': '0', 'net_r': '5.631101006071993', 'raw_signal_hash': '5004ab2e942e9c9fa51828995e5e084ad3231f0ad2d619a5da248066e9f0cceb', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '40', 'accepted_hash': '321601ee89c9c94f409c890583307529b5067c1337b943a472382d72d0ae85b1', 'blocked': '0', 'net_r': '3.511516823115995', 'raw_signal_hash': '5004ab2e942e9c9fa51828995e5e084ad3231f0ad2d619a5da248066e9f0cceb', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '40', 'accepted_hash': '321601ee89c9c94f409c890583307529b5067c1337b943a472382d72d0ae85b1', 'blocked': '0', 'net_r': '3.511516823115995', 'raw_signal_hash': '5004ab2e942e9c9fa51828995e5e084ad3231f0ad2d619a5da248066e9f0cceb', 'rejected': '0'}}, 'BROAD__H4_VOL_LE_1.25': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '93', 'accepted_hash': '6e0cf1c25984d1eec676381476dec33d63e8b1a289c998ee1903f70d24f43bb5', 'blocked': '3', 'net_r': '13.374488435310514', 'raw_signal_hash': 'ff4114fb3bf00b334fbb6420e0f7bcf6627dc5b770daadeaa70b657e6145e328', 'rejected': '7'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '93', 'accepted_hash': '6e0cf1c25984d1eec676381476dec33d63e8b1a289c998ee1903f70d24f43bb5', 'blocked': '3', 'net_r': '13.374488435310514', 'raw_signal_hash': 'ff4114fb3bf00b334fbb6420e0f7bcf6627dc5b770daadeaa70b657e6145e328', 'rejected': '7'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '100', 'accepted_hash': '5d0b47de10950a104436db5dc15cbc4c3904a09019f37f3e73f8703a050c183a', 'blocked': '3', 'net_r': '25.157684982025433', 'raw_signal_hash': 'ff4114fb3bf00b334fbb6420e0f7bcf6627dc5b770daadeaa70b657e6145e328', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '100', 'accepted_hash': 'e4276877b6e268b94930b2879c41dc0e8f37a24393a68de53e8376981e4eeab4', 'blocked': '3', 'net_r': '25.10324372128044', 'raw_signal_hash': 'ff4114fb3bf00b334fbb6420e0f7bcf6627dc5b770daadeaa70b657e6145e328', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '100', 'accepted_hash': '303871ff5634da18b8493f34cb59bb6385b65ee8a3358cba27f3dee58ffef33c', 'blocked': '3', 'net_r': '22.45979258048011', 'raw_signal_hash': 'ff4114fb3bf00b334fbb6420e0f7bcf6627dc5b770daadeaa70b657e6145e328', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '100', 'accepted_hash': '5640d47ecf355969a81f3dd343c9d35a3db5b22313fad300849a96d5e3340cc8', 'blocked': '3', 'net_r': '22.40612026409592', 'raw_signal_hash': 'ff4114fb3bf00b334fbb6420e0f7bcf6627dc5b770daadeaa70b657e6145e328', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '100', 'accepted_hash': '4fb4bc80b1a512ef04866ad503fda24c24a27a36bdfc9dcdc4ec9fa3784548e3', 'blocked': '3', 'net_r': '17.449911789052493', 'raw_signal_hash': 'ff4114fb3bf00b334fbb6420e0f7bcf6627dc5b770daadeaa70b657e6145e328', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '100', 'accepted_hash': 'd927687e7086b6932d3e8b1c53cb7fdb3160b8ffd85b64351e8bc3d438e4fdc7', 'blocked': '3', 'net_r': '17.397713986854686', 'raw_signal_hash': 'ff4114fb3bf00b334fbb6420e0f7bcf6627dc5b770daadeaa70b657e6145e328', 'rejected': '0'}}, 'BROAD__SIGNAL_CLOSE_GE_0.7': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '98', 'accepted_hash': '55b711afc1db3ee8ead7d349f93cfdae78e58544b8933605c77600f06bde72d6', 'blocked': '2', 'net_r': '8.482642230127402', 'raw_signal_hash': '186c6af59bc1f0b6522d0f3ef9f0025ae41b7b480687cafc1f9e8bfbb1abb3f8', 'rejected': '7'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '98', 'accepted_hash': '55b711afc1db3ee8ead7d349f93cfdae78e58544b8933605c77600f06bde72d6', 'blocked': '2', 'net_r': '8.482642230127402', 'raw_signal_hash': '186c6af59bc1f0b6522d0f3ef9f0025ae41b7b480687cafc1f9e8bfbb1abb3f8', 'rejected': '7'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '105', 'accepted_hash': '57ab0ef6cde844b860d3fbbd189ba32d4d8846ac3641fb638ec036b4685734dc', 'blocked': '2', 'net_r': '20.276136769373807', 'raw_signal_hash': '186c6af59bc1f0b6522d0f3ef9f0025ae41b7b480687cafc1f9e8bfbb1abb3f8', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '105', 'accepted_hash': 'e663c29ec6c8d141c8410fb26bc11607ee409326ad4984edf09badea5b609690', 'blocked': '2', 'net_r': '20.221695508628812', 'raw_signal_hash': '186c6af59bc1f0b6522d0f3ef9f0025ae41b7b480687cafc1f9e8bfbb1abb3f8', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '105', 'accepted_hash': 'e0b42f6ca4bf73209691197873a44061e5a93c7063aefe5a3116d010059697eb', 'blocked': '2', 'net_r': '17.688902579739615', 'raw_signal_hash': '186c6af59bc1f0b6522d0f3ef9f0025ae41b7b480687cafc1f9e8bfbb1abb3f8', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '105', 'accepted_hash': 'f13aa42c383af4366b558e261e761959d7b06dbfbd78798b7eb36b5b75cfbfa5', 'blocked': '2', 'net_r': '17.635230263355428', 'raw_signal_hash': '186c6af59bc1f0b6522d0f3ef9f0025ae41b7b480687cafc1f9e8bfbb1abb3f8', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '105', 'accepted_hash': 'f691294f473a40cdac9f87df527290cb0689e61e0e728a9a1f1939017d8dde2d', 'blocked': '2', 'net_r': '12.87934345972578', 'raw_signal_hash': '186c6af59bc1f0b6522d0f3ef9f0025ae41b7b480687cafc1f9e8bfbb1abb3f8', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '105', 'accepted_hash': '3f904571901345eb3fb8e471d02bbae6116c0a6ca848c356c50051e53c5d2ff9', 'blocked': '2', 'net_r': '12.82714565752797', 'raw_signal_hash': '186c6af59bc1f0b6522d0f3ef9f0025ae41b7b480687cafc1f9e8bfbb1abb3f8', 'rejected': '0'}}, 'BROAD__SIGNAL_CLOSE_GE_0.8': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '80', 'accepted_hash': '652b279d261dd3255dca790c38652aaed680c9161fc043cd38b85527848ead88', 'blocked': '1', 'net_r': '10.602814621665539', 'raw_signal_hash': 'afdf58efabf7afc9694ba6f01110c4c5b53de899eada6b17f52b41c630950ed6', 'rejected': '7'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '80', 'accepted_hash': '652b279d261dd3255dca790c38652aaed680c9161fc043cd38b85527848ead88', 'blocked': '1', 'net_r': '10.602814621665539', 'raw_signal_hash': 'afdf58efabf7afc9694ba6f01110c4c5b53de899eada6b17f52b41c630950ed6', 'rejected': '7'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '87', 'accepted_hash': '672e3643d77d566bb67664a0217506d04a00bb5584d51a5c2e15d937a1b13598', 'blocked': '1', 'net_r': '22.61455498129044', 'raw_signal_hash': 'afdf58efabf7afc9694ba6f01110c4c5b53de899eada6b17f52b41c630950ed6', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '87', 'accepted_hash': 'ed066969b6380d5f6ab3adbfb1d7af4898bde82f09bdcdf4c746cbe46323769b', 'blocked': '1', 'net_r': '22.56011372054545', 'raw_signal_hash': 'afdf58efabf7afc9694ba6f01110c4c5b53de899eada6b17f52b41c630950ed6', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '87', 'accepted_hash': '9ae241ae887343c3bec24dd4771e0971e18897b2fe32cb16fa565672c599834f', 'blocked': '1', 'net_r': '20.350270292835564', 'raw_signal_hash': 'afdf58efabf7afc9694ba6f01110c4c5b53de899eada6b17f52b41c630950ed6', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '87', 'accepted_hash': '82e7ca5a171633f89ed55b9f8929aa19317ff8b0df696db2a11389a8b2a8daf8', 'blocked': '1', 'net_r': '20.296597976451377', 'raw_signal_hash': 'afdf58efabf7afc9694ba6f01110c4c5b53de899eada6b17f52b41c630950ed6', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '87', 'accepted_hash': '52831f977c63f33faa20ecf30df30be7a8342375ef476a10d5335a68d7b778bd', 'blocked': '1', 'net_r': '16.144552617164273', 'raw_signal_hash': 'afdf58efabf7afc9694ba6f01110c4c5b53de899eada6b17f52b41c630950ed6', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '87', 'accepted_hash': '84e055c879484655107b1f683f51d4fa42c7153062f11a15e11d7a0a6be88cbc', 'blocked': '1', 'net_r': '16.092354814966463', 'raw_signal_hash': 'afdf58efabf7afc9694ba6f01110c4c5b53de899eada6b17f52b41c630950ed6', 'rejected': '0'}}, 'BROAD__SIGNAL_CLOSE_GE_0.9': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '42', 'accepted_hash': 'dbeb22ee9b2f13945bbe657f10b96013d416cdf6680d2684147bb5e2d7ddd939', 'blocked': '0', 'net_r': '0.9981644055674916', 'raw_signal_hash': 'abb2257715f6dd8a8b15637f808da47d0a3bcad2b3676d2c09a17c6d11a8569b', 'rejected': '4'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '42', 'accepted_hash': 'dbeb22ee9b2f13945bbe657f10b96013d416cdf6680d2684147bb5e2d7ddd939', 'blocked': '0', 'net_r': '0.9981644055674916', 'raw_signal_hash': 'abb2257715f6dd8a8b15637f808da47d0a3bcad2b3676d2c09a17c6d11a8569b', 'rejected': '4'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '46', 'accepted_hash': 'e641217eae8c5738a6e493205b184949c4e59323985b0622fcef4df5737df657', 'blocked': '0', 'net_r': '0.7760775543119975', 'raw_signal_hash': 'abb2257715f6dd8a8b15637f808da47d0a3bcad2b3676d2c09a17c6d11a8569b', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '46', 'accepted_hash': 'c9602d223882002bbc5c4f468e5773f0dea5b6356f704a2276ff57bcbc6f1acf', 'blocked': '0', 'net_r': '0.7216362935670051', 'raw_signal_hash': 'abb2257715f6dd8a8b15637f808da47d0a3bcad2b3676d2c09a17c6d11a8569b', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '46', 'accepted_hash': '6f497215ef18fa8593ebbdd450f5691ed985cf51ea4c983b3680e3a03085f839', 'blocked': '0', 'net_r': '-0.37847679156111314', 'raw_signal_hash': 'abb2257715f6dd8a8b15637f808da47d0a3bcad2b3676d2c09a17c6d11a8569b', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '46', 'accepted_hash': 'fef6950722f13fa7d0c6db1aa39f195b7176e00a80bd45c84bd595c1b48e8ec0', 'blocked': '0', 'net_r': '-0.4321491079453008', 'raw_signal_hash': 'abb2257715f6dd8a8b15637f808da47d0a3bcad2b3676d2c09a17c6d11a8569b', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '46', 'accepted_hash': 'd62083970ad7fa641fbb0f65f4b0744121a04e1c81f488a2de63a119b31a3479', 'blocked': '0', 'net_r': '-2.504361101336658', 'raw_signal_hash': 'abb2257715f6dd8a8b15637f808da47d0a3bcad2b3676d2c09a17c6d11a8569b', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '46', 'accepted_hash': 'fed9d0ca5c6a96322f85a5c7fd89f53ae48a5db78424d4cdfd5ea997d2b4811d', 'blocked': '0', 'net_r': '-2.5565589035344667', 'raw_signal_hash': 'abb2257715f6dd8a8b15637f808da47d0a3bcad2b3676d2c09a17c6d11a8569b', 'rejected': '0'}}, 'STRONG_BODY__BODY_RATIO_GE_0.6': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '49', 'accepted_hash': '64e4bc2cd4cc5804bd014e8fbebd370e28846c028cc4feff7160f03a6f3fa30f', 'blocked': '1', 'net_r': '10.355862354561232', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '4'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '49', 'accepted_hash': '64e4bc2cd4cc5804bd014e8fbebd370e28846c028cc4feff7160f03a6f3fa30f', 'blocked': '1', 'net_r': '10.355862354561232', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '4'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '53', 'accepted_hash': 'f18aeccd8903e39b6f08cd24bb4d9a1b6e1f78bd1b5559cd6eac6487facfe603', 'blocked': '1', 'net_r': '17.896274044063013', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '53', 'accepted_hash': 'bb1d77df80edf62b5e7ea6dafb12171e6d020a4892623e21b92d1b626331eb8c', 'blocked': '1', 'net_r': '17.841832783318022', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '53', 'accepted_hash': 'f51eeb92dc1a030cbaa19e41e9f8093b6a78bcfb73dc36c674d3acdf65cc03a8', 'blocked': '1', 'net_r': '16.831981788009386', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '53', 'accepted_hash': '40896ca6503fda6285abf679ae2f4e640cf43d9efb7247a3f0c3cbb73f6fa972', 'blocked': '1', 'net_r': '16.7783094716252', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '53', 'accepted_hash': '9bdba292b9e7460db24243216a7d608d24922284f652e06918b77673db2df331', 'blocked': '1', 'net_r': '14.81186068156517', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '53', 'accepted_hash': 'dcfd5b1efb118c55eab67aa7aafd5d4768301b720593e53fdddaf23640f313c6', 'blocked': '1', 'net_r': '14.75966287936736', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '0'}}, 'STRONG_BODY__BODY_RATIO_GE_0.7': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '43', 'accepted_hash': 'bd5d186ad60bb27e5d1039aaaf5eeb84ce283605aca38801f187ed8795d1e8c9', 'blocked': '0', 'net_r': '0.4422830119736294', 'raw_signal_hash': 'ae5f253339eda3d1945caa57e18b893693c4db444597038046095f072881aa9e', 'rejected': '2'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '43', 'accepted_hash': 'bd5d186ad60bb27e5d1039aaaf5eeb84ce283605aca38801f187ed8795d1e8c9', 'blocked': '0', 'net_r': '0.4422830119736294', 'raw_signal_hash': 'ae5f253339eda3d1945caa57e18b893693c4db444597038046095f072881aa9e', 'rejected': '2'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '45', 'accepted_hash': 'c06182bcb56ea2cb3bd855705d847e776e7aa7e46e1ccd62c5890ff4135f26c6', 'blocked': '0', 'net_r': '10.097419773232925', 'raw_signal_hash': 'ae5f253339eda3d1945caa57e18b893693c4db444597038046095f072881aa9e', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '45', 'accepted_hash': '937cfe9e322e9fa26f69e2b8cf1edaa0d70c78bad3d38e91957a417dc5463385', 'blocked': '0', 'net_r': '10.042978512487934', 'raw_signal_hash': 'ae5f253339eda3d1945caa57e18b893693c4db444597038046095f072881aa9e', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '45', 'accepted_hash': '3514e59bb744fc4c304584c4c35725739fa87baef49476654fdc1b9ac16d4824', 'blocked': '0', 'net_r': '9.2290488077607', 'raw_signal_hash': 'ae5f253339eda3d1945caa57e18b893693c4db444597038046095f072881aa9e', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '45', 'accepted_hash': '5e1ff76f5a8d9e2a6d60da37ff150c7a654a96a022933efe2feaab0607711a60', 'blocked': '0', 'net_r': '9.175376491376511', 'raw_signal_hash': 'ae5f253339eda3d1945caa57e18b893693c4db444597038046095f072881aa9e', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '45', 'accepted_hash': 'c4428e8bb19016b370fa551565b5febe5a77a5f32a788a72920ed0f6bb37c00d', 'blocked': '0', 'net_r': '7.585917295663544', 'raw_signal_hash': 'ae5f253339eda3d1945caa57e18b893693c4db444597038046095f072881aa9e', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '45', 'accepted_hash': '1fbad545ca620300841e423e7ea70d28b322228bc45785c6a1d1a8be44086326', 'blocked': '0', 'net_r': '7.5337194934657346', 'raw_signal_hash': 'ae5f253339eda3d1945caa57e18b893693c4db444597038046095f072881aa9e', 'rejected': '0'}}, 'STRONG_BODY__BODY_RATIO_GE_0.8': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '23', 'accepted_hash': '6010774e18cfaac62d4aab3418d0f125fd5d3829a13398df4f7681fea50da36b', 'blocked': '0', 'net_r': '-7.2613025795370625', 'raw_signal_hash': '5c38b825627d07579e811e2172e28c9d08db197e41756d83cdb20652e5df9ae3', 'rejected': '1'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '23', 'accepted_hash': '6010774e18cfaac62d4aab3418d0f125fd5d3829a13398df4f7681fea50da36b', 'blocked': '0', 'net_r': '-7.2613025795370625', 'raw_signal_hash': '5c38b825627d07579e811e2172e28c9d08db197e41756d83cdb20652e5df9ae3', 'rejected': '1'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '24', 'accepted_hash': 'c2726013c7a8df263e47473a58dba1930a9294f9b03b260823295819ff8ab48f', 'blocked': '0', 'net_r': '-0.4630606816277534', 'raw_signal_hash': '5c38b825627d07579e811e2172e28c9d08db197e41756d83cdb20652e5df9ae3', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '24', 'accepted_hash': 'c2726013c7a8df263e47473a58dba1930a9294f9b03b260823295819ff8ab48f', 'blocked': '0', 'net_r': '-0.4630606816277534', 'raw_signal_hash': '5c38b825627d07579e811e2172e28c9d08db197e41756d83cdb20652e5df9ae3', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '24', 'accepted_hash': 'e70394f56d6011658204b5753cb2fa044c2a5036228f3175975added653de2a1', 'blocked': '0', 'net_r': '-0.9044910369475363', 'raw_signal_hash': '5c38b825627d07579e811e2172e28c9d08db197e41756d83cdb20652e5df9ae3', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '24', 'accepted_hash': 'e70394f56d6011658204b5753cb2fa044c2a5036228f3175975added653de2a1', 'blocked': '0', 'net_r': '-0.9044910369475363', 'raw_signal_hash': '5c38b825627d07579e811e2172e28c9d08db197e41756d83cdb20652e5df9ae3', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '24', 'accepted_hash': 'f17956d1e71466a74f115dc9658d529a099efe5ffaa132745a65a70c5daa5cd1', 'blocked': '0', 'net_r': '-1.7292955108199983', 'raw_signal_hash': '5c38b825627d07579e811e2172e28c9d08db197e41756d83cdb20652e5df9ae3', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '24', 'accepted_hash': 'f17956d1e71466a74f115dc9658d529a099efe5ffaa132745a65a70c5daa5cd1', 'blocked': '0', 'net_r': '-1.7292955108199983', 'raw_signal_hash': '5c38b825627d07579e811e2172e28c9d08db197e41756d83cdb20652e5df9ae3', 'rejected': '0'}}, 'STRONG_BODY__CONTROL': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '52', 'accepted_hash': '7af667268c5fda50ea8eaf3a902ac2e9868163f08bb4919e188cea8fc459d00d', 'blocked': '1', 'net_r': '7.355862354561232', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '5'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '52', 'accepted_hash': '7af667268c5fda50ea8eaf3a902ac2e9868163f08bb4919e188cea8fc459d00d', 'blocked': '1', 'net_r': '7.355862354561232', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '5'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '57', 'accepted_hash': 'c7660408058093dd74beda9f4445e82cb3a9c934fd51ef9d957c669ab9adafa6', 'blocked': '1', 'net_r': '13.896274044063015', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '57', 'accepted_hash': '3d2d17610f9ea033c348a76eb2dd1ed7b17e42afb0b02e45b18a7d25c8827c4a', 'blocked': '1', 'net_r': '13.841832783318022', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '57', 'accepted_hash': '2237d6a869858b55476744b6c270088252e3915dbfbe9661f3adfc4c89d9c923', 'blocked': '1', 'net_r': '12.831981788009386', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '57', 'accepted_hash': 'c1eccd64fd9c8dbfafeb1754e1e40388071d28fbef4845b9dc999aeab951dcb9', 'blocked': '1', 'net_r': '12.7783094716252', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '57', 'accepted_hash': '8064ca9c491dad8154834cb65ae2ccb50291e1ff36c5ead6807d8634cda37405', 'blocked': '1', 'net_r': '10.81186068156517', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '57', 'accepted_hash': '7ebb1926f01bda5d7bdc48dee803ee0a1f0dd80eee5bf901edf6b0eec994a02c', 'blocked': '1', 'net_r': '10.75966287936736', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}}, 'STRONG_BODY__SIGNAL_CLOSE_GE_0.7': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '50', 'accepted_hash': '487fd277b5bc4f80ad84a8f78705f8b597757ac4f35bcf7c353dd5a79f35554a', 'blocked': '0', 'net_r': '9.355862354561232', 'raw_signal_hash': 'a1e1f34854873fb36555db773ab10d18327096770512b18db97bcba742e03fd6', 'rejected': '5'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '50', 'accepted_hash': '487fd277b5bc4f80ad84a8f78705f8b597757ac4f35bcf7c353dd5a79f35554a', 'blocked': '0', 'net_r': '9.355862354561232', 'raw_signal_hash': 'a1e1f34854873fb36555db773ab10d18327096770512b18db97bcba742e03fd6', 'rejected': '5'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '55', 'accepted_hash': '3269881261a79297501c7f10042360c28acfba8677812f2537e4ec553ff2af82', 'blocked': '0', 'net_r': '15.896274044063015', 'raw_signal_hash': 'a1e1f34854873fb36555db773ab10d18327096770512b18db97bcba742e03fd6', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '55', 'accepted_hash': 'bb82d092975ff90ce7f01c7234bafbb7f7e5cb97e3c5a69b2fc91350ccb07f4b', 'blocked': '0', 'net_r': '15.841832783318022', 'raw_signal_hash': 'a1e1f34854873fb36555db773ab10d18327096770512b18db97bcba742e03fd6', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '55', 'accepted_hash': '20d025c39e39275308856363d8dcf5baa6c086a7a3c95fab0cacc531254309e1', 'blocked': '0', 'net_r': '14.831981788009386', 'raw_signal_hash': 'a1e1f34854873fb36555db773ab10d18327096770512b18db97bcba742e03fd6', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '55', 'accepted_hash': '305dd2311cee5027261785724424ffe876a7eb5190149e7eaccb455d1c75c447', 'blocked': '0', 'net_r': '14.7783094716252', 'raw_signal_hash': 'a1e1f34854873fb36555db773ab10d18327096770512b18db97bcba742e03fd6', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '55', 'accepted_hash': 'beeffc7f9182891de98783f076a85a561af99b510bfe7a6df15175f4de72a15c', 'blocked': '0', 'net_r': '12.81186068156517', 'raw_signal_hash': 'a1e1f34854873fb36555db773ab10d18327096770512b18db97bcba742e03fd6', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '55', 'accepted_hash': '002ef3602e872636f2530b619fd09fddf741c548b13ff3228adbbd7019e04320', 'blocked': '0', 'net_r': '12.75966287936736', 'raw_signal_hash': 'a1e1f34854873fb36555db773ab10d18327096770512b18db97bcba742e03fd6', 'rejected': '0'}}, 'STRONG_BODY__SIGNAL_CLOSE_GE_0.8': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '42', 'accepted_hash': 'c7c00d68bdc3d999a7270127ce58af6fd807976832184c8b73a69c66ecb9043f', 'blocked': '0', 'net_r': '9.440780224376145', 'raw_signal_hash': '4cc22e45f55750af82b875e6a422c1ca64cc62fd0af98065a34bb8caf5326304', 'rejected': '5'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '42', 'accepted_hash': 'c7c00d68bdc3d999a7270127ce58af6fd807976832184c8b73a69c66ecb9043f', 'blocked': '0', 'net_r': '9.440780224376145', 'raw_signal_hash': '4cc22e45f55750af82b875e6a422c1ca64cc62fd0af98065a34bb8caf5326304', 'rejected': '5'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '47', 'accepted_hash': '59f777dd341f73adb0d222b4db5f505d46a6a393b129623ebae539c7cc1c307f', 'blocked': '0', 'net_r': '16.017033831961598', 'raw_signal_hash': '4cc22e45f55750af82b875e6a422c1ca64cc62fd0af98065a34bb8caf5326304', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '47', 'accepted_hash': 'e4ed8a82cd4923786b9a0a828e3b59a7cd5f598e458859bf8b1473e0e02ddf32', 'blocked': '0', 'net_r': '15.962592571216605', 'raw_signal_hash': '4cc22e45f55750af82b875e6a422c1ca64cc62fd0af98065a34bb8caf5326304', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '47', 'accepted_hash': 'a5186e5028e681e24058acd0af3775a2d979a67ed5415c6c5960b6bf0c342d47', 'blocked': '0', 'net_r': '15.069896596884464', 'raw_signal_hash': '4cc22e45f55750af82b875e6a422c1ca64cc62fd0af98065a34bb8caf5326304', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '47', 'accepted_hash': '78a04bd5ec7e90877cf446a2c8da317897f28edf99224d66449575c07aeedf54', 'blocked': '0', 'net_r': '15.016224280500277', 'raw_signal_hash': '4cc22e45f55750af82b875e6a422c1ca64cc62fd0af98065a34bb8caf5326304', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '47', 'accepted_hash': '9cb922df717cd55a5a3fc1adabc3d1be0018260a0289671f690412f5a7a48c39', 'blocked': '0', 'net_r': '13.273900303955758', 'raw_signal_hash': '4cc22e45f55750af82b875e6a422c1ca64cc62fd0af98065a34bb8caf5326304', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '47', 'accepted_hash': '2e183123fd31e5e41334865bbe6c9b7be0fb24033b16c7a63e04741ffb7226cc', 'blocked': '0', 'net_r': '13.22170250175795', 'raw_signal_hash': '4cc22e45f55750af82b875e6a422c1ca64cc62fd0af98065a34bb8caf5326304', 'rejected': '0'}}, 'STRONG_BODY__SIGNAL_CLOSE_GE_0.9': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '22', 'accepted_hash': 'cfcb12020cf759b28bf607fe3c4ad5a12f7770ad34a276c2b919da0c8a5c47e9', 'blocked': '0', 'net_r': '-6.326945558237693', 'raw_signal_hash': 'ed9b93b1ad1bd2bb53a1dbdecba8fe80a67f3c0a0c7e20ac793913dd644d855e', 'rejected': '3'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '22', 'accepted_hash': 'cfcb12020cf759b28bf607fe3c4ad5a12f7770ad34a276c2b919da0c8a5c47e9', 'blocked': '0', 'net_r': '-6.326945558237693', 'raw_signal_hash': 'ed9b93b1ad1bd2bb53a1dbdecba8fe80a67f3c0a0c7e20ac793913dd644d855e', 'rejected': '3'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '25', 'accepted_hash': '0fa7750cde5426933415f11e4563c805cb3dab61dd41043cdf0da93a724e283b', 'blocked': '0', 'net_r': '-1.4397096814192643', 'raw_signal_hash': 'ed9b93b1ad1bd2bb53a1dbdecba8fe80a67f3c0a0c7e20ac793913dd644d855e', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '25', 'accepted_hash': '2eb48d2d245dbdfa0bde9c9aac71d94cd543f5ddaae92333f8547b158b71069e', 'blocked': '0', 'net_r': '-1.4941509421642567', 'raw_signal_hash': 'ed9b93b1ad1bd2bb53a1dbdecba8fe80a67f3c0a0c7e20ac793913dd644d855e', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '25', 'accepted_hash': 'dfb9aeee665a49a757afbcd5bc70f5d4e556da69765f0ef2a3a6842f7b4d4863', 'blocked': '0', 'net_r': '-1.8591676713468663', 'raw_signal_hash': 'ed9b93b1ad1bd2bb53a1dbdecba8fe80a67f3c0a0c7e20ac793913dd644d855e', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '25', 'accepted_hash': '8a15328bdc1e0547ecf9ea45c439637bc54216bc889dddb98fbf82e2ffda6b37', 'blocked': '0', 'net_r': '-1.912839987731054', 'raw_signal_hash': 'ed9b93b1ad1bd2bb53a1dbdecba8fe80a67f3c0a0c7e20ac793913dd644d855e', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '25', 'accepted_hash': 'edee406c3bb80b3a8e6fdc0a8bb93d3d5656f588b8e198657763f10a2b0bea0b', 'blocked': '0', 'net_r': '-2.643805338373309', 'raw_signal_hash': 'ed9b93b1ad1bd2bb53a1dbdecba8fe80a67f3c0a0c7e20ac793913dd644d855e', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '25', 'accepted_hash': 'dd66ba7d2f1904b7955550133af59a9c764405ce9228737ce0d935350c33ffcf', 'blocked': '0', 'net_r': '-2.6960031405711176', 'raw_signal_hash': 'ed9b93b1ad1bd2bb53a1dbdecba8fe80a67f3c0a0c7e20ac793913dd644d855e', 'rejected': '0'}}}

PASS3_PINS = {'STRONG_BODY__BODY1.4_BR0.6': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '39', 'accepted_hash': '23eba4a6f8dc075b408a505c733cac5797bf942e769abbbd351117c30730019d', 'blocked': '0', 'net_r': '12.405531755949378', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '2'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '39', 'accepted_hash': '23eba4a6f8dc075b408a505c733cac5797bf942e769abbbd351117c30730019d', 'blocked': '0', 'net_r': '12.405531755949378', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '2'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '41', 'accepted_hash': '7e88868cb5fc83ff5332b14972bd39226ba832f46b6b2db7106e9ff37deef1cb', 'blocked': '0', 'net_r': '18.07828069221303', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '41', 'accepted_hash': '55ceca9c44c019799066e134038310f753069151982e12bec72011095a6b08f7', 'blocked': '0', 'net_r': '18.023839431468037', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '41', 'accepted_hash': '19fa19a6c5328b11702efc070c8306139f3269ae35e7c79f2cbff75c5623a3c0', 'blocked': '0', 'net_r': '17.1903591261877', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '41', 'accepted_hash': '3fd544859e72d87432818b411f43492f09df7e2fa0f22ddb3e0f79ea92df475e', 'blocked': '0', 'net_r': '17.13668680980351', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '41', 'accepted_hash': '0829b365b87cf7eec217df9a82fa2a6fc61e72e784adb15209156e9b69f441ff', 'blocked': '0', 'net_r': '15.507121159434266', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '41', 'accepted_hash': 'd18e5ead650a3c3b92a34767b927fbc87e4c53ebce27825fef8057eb87b0a4eb', 'blocked': '0', 'net_r': '15.454923357236458', 'raw_signal_hash': '0de60e2d51497a55312762176208ddffab195901b27845d0be3fd71fdf42b411', 'rejected': '0'}}, 'STRONG_BODY__BR0.6_CNONE': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '49', 'accepted_hash': '64e4bc2cd4cc5804bd014e8fbebd370e28846c028cc4feff7160f03a6f3fa30f', 'blocked': '1', 'net_r': '10.355862354561232', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '4'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '49', 'accepted_hash': '64e4bc2cd4cc5804bd014e8fbebd370e28846c028cc4feff7160f03a6f3fa30f', 'blocked': '1', 'net_r': '10.355862354561232', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '4'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '53', 'accepted_hash': 'f18aeccd8903e39b6f08cd24bb4d9a1b6e1f78bd1b5559cd6eac6487facfe603', 'blocked': '1', 'net_r': '17.896274044063013', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '53', 'accepted_hash': 'bb1d77df80edf62b5e7ea6dafb12171e6d020a4892623e21b92d1b626331eb8c', 'blocked': '1', 'net_r': '17.841832783318022', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '53', 'accepted_hash': 'f51eeb92dc1a030cbaa19e41e9f8093b6a78bcfb73dc36c674d3acdf65cc03a8', 'blocked': '1', 'net_r': '16.831981788009386', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '53', 'accepted_hash': '40896ca6503fda6285abf679ae2f4e640cf43d9efb7247a3f0c3cbb73f6fa972', 'blocked': '1', 'net_r': '16.7783094716252', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '53', 'accepted_hash': '9bdba292b9e7460db24243216a7d608d24922284f652e06918b77673db2df331', 'blocked': '1', 'net_r': '14.81186068156517', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '53', 'accepted_hash': 'dcfd5b1efb118c55eab67aa7aafd5d4768301b720593e53fdddaf23640f313c6', 'blocked': '1', 'net_r': '14.75966287936736', 'raw_signal_hash': '5e2c7e9f530a53aa458a8f513a1f67f69cfd0f7227b544d901162c7136008fff', 'rejected': '0'}}}


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
STUDY_LOOKBACKS = (20,30,40,50,60)
STUDY_DISTANCES = (.35,.50,.65,.80,1.)


def study_definitions():
    rows=[]
    for comparator in COMPARATORS:
        for lookback,distance in itertools.product(STUDY_LOOKBACKS,STUDY_DISTANCES):
            rows.append({'id':f'{comparator[0]}__L{lookback}_D{distance:g}','anchor':comparator[0],'family':'STRUCTURE_FREQUENCY','body':comparator[4],'lookback':lookback,'distance':distance,'cap':None,'close':None,'ratio':.60})
    require(len(rows)==50 and len({r['id'] for r in rows})==50,'Pass4 study count')
    return rows

DEFINITIONS=study_definitions()
DEFINITION_BY_ID={r['id']:r for r in DEFINITIONS}


def configs():
    return [(d['id'],d['family'],d['lookback'],d['distance'],d['body'],1.50) for d in DEFINITIONS]


def anchor_for(cid):
    return next(a for a in COMPARATORS if a[0]==cid.split('__')[0])


def anchor_qualifies(f,anchor):
    _,_,l,d,b,r=anchor
    return f['distances'][l]<=d and f['body_atr']>=b and f['range_atr']>=r


def comparator_qualifies(f,comparator):
    return anchor_qualifies(f,comparator) and f['body_ratio']>=.60


def conditions_of(d):return [('body_ratio','GE',.60)]


def condition_metadata(cid,raw):
    d=DEFINITION_BY_ID[cid]
    return {'anchor':d['anchor'],'conditions_json':'[["body_ratio","GE",0.6]]','h4_vol_cap':None,'close_floor':None,'body_ratio_floor':.60,'missing_feature_signals':0}


def study_boundaries(cid):
    d=DEFINITION_BY_ID[cid];flags=[]
    if d['lookback'] in (STUDY_LOOKBACKS[0],STUDY_LOOKBACKS[-1]):flags.append('lookback')
    if d['distance'] in (STUDY_DISTANCES[0],STUDY_DISTANCES[-1]):flags.append('distance')
    return '|'.join(flags)


def find_definition(anchor,lookback,distance):
    return f'{anchor}__L{lookback}_D{distance:g}'


def comparison_pairs():
    pairs=[]
    for comparator in COMPARATORS:
        label=comparator[0]
        for lookback in STUDY_LOOKBACKS:
            for a,b in zip(STUDY_DISTANCES,STUDY_DISTANCES[1:]):
                pairs.append((find_definition(label,lookback,a),find_definition(label,lookback,b),'RELAX_DISTANCE'))
        for distance in STUDY_DISTANCES:
            for a,b in zip(STUDY_LOOKBACKS,STUDY_LOOKBACKS[1:]):
                pairs.append((find_definition(label,b,distance),find_definition(label,a,distance),'SHORTEN_LOOKBACK'))
    for lookback,distance in itertools.product(STUDY_LOOKBACKS,STUDY_DISTANCES):
        pairs.append((find_definition('QUALITY',lookback,distance),find_definition('FREQUENCY',lookback,distance),'BODY140_TO125'))
    require(len(pairs)==105,'Pass4 frequency comparison count')
    return sorted(pairs)


def condition_neighbours():
    for reference,candidate,question in comparison_pairs():
        dimension={'RELAX_DISTANCE':'distance','SHORTEN_LOOKBACK':'lookback','BODY140_TO125':'body'}[question]
        yield reference,candidate,dimension
        yield candidate,reference,dimension


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
    summary=CSV(Path(out)/'frequency_comparisons.csv',fields)
    marginal=CSV(Path(out)/'frequency_marginal_trades.csv',['reference_config','candidate_config','question','case','kind','signal_index','r','qualifies_candidate','displaced_by_signal_index'])
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
                ab={p['signal_index']:p['blocking_signal_index'] for p in a[2]};bb={p['signal_index']:p['blocking_signal_index'] for p in b[2]}
                for kind,ids,lookup,blocks in (('ADDED',added,br,ab),('REMOVED',removed,ar,bb)):
                    for i in sorted(ids):marginal.add(dict(meta,kind=kind,signal_index=i,r=lookup[i]['r'],qualifies_candidate=i in masks[candidate],displaced_by_signal_index=blocks.get(i)))
    finally:summary.close();marginal.close()
    return {'frequency_comparisons':summary.count,'frequency_marginal_trades':marginal.count}


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


def qualifies(f,config):
    _,_,l,d,b,r=config
    if not(f['distances'][l]<=d and f['body_atr']>=b and f['range_atr']>=r):return False
    for key,op,value in conditions_of(DEFINITION_BY_ID[config[0]]):
        actual=f[key]
        if actual is None or (actual>value if op=='LE' else actual<value):return False
    return True


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


def path_for(bars, i, case):
    _,scenario,cost,model=case
    p=bars[i]['mid'];reference=p['c'];stop=p['l']-PIP;target=reference+RR*(reference-stop)
    t=bars[i]['t']+H4
    base={'signal_index':i,'signal_start':stamp(bars[i]['t']),'signal_close':stamp(t),
          'reference':reference,'stop':stop,'target':target,'rr_reference':RR,'reason':''}
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


def independent_raw(bars, case, anchor=None):
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
        signals.append(i)
        if i<busy:blocked.append((i,accepted[-1][0]));continue
        st=now['l']-.0001;ref=now['c'];tp=ref+3*(ref-st);et=bars[i]['t']+14400
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
    all_ledgers={};all_masks={}
    baselines={(anchor[0],case[0]):replay([i for i,f in raw.items() if comparator_qualifies(f,anchor)],paths[case[0]]) for anchor in COMPARATORS for case in CASES}
    write_json(out/'independent_raw_controls.json',controls)
    write_json(out/'protocol.json',{'version':VERSION,'start_utc':stamp(START),'cutoff_exclusive_utc':stamp(END),'cases':CASES,'rr':RR,'configurations':50,'source_h4_count':len(bars),'raw_indices_sha256':digest(indices)})
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
    rawpaths=make('raw_independent_paths',['case']+fields)
    memberships=make('raw_memberships',['config','signal_index'])
    cf=make('configurations',['config','family','lookback','distance_atr_max','body_atr_min','range_atr_min','boundary_dimensions','anchor','conditions_json','h4_vol_cap','close_floor','body_ratio_floor','missing_feature_signals'])
    feature=make('raw_signal_features',['signal_index','signal_start','atr','body_atr','range_atr','close_location','body_ratio']+['distance_lb'+str(l) for l in LB]+CONTEXT_FIELDS)
    source=make('source_h4_features',['index','time','atr','first_observed_h1','h1_count']+[s+'_'+k for s in ('mid','bid','ask') for k in 'ohlc'])
    for i,b in enumerate(bars):source.add(dict(index=i,time=stamp(b['t']),atr=atr[i],first_observed_h1=stamp(b['first_h1']),h1_count=b['h1_count'],**{s+'_'+k:b[s][k] for s in ('mid','bid','ask') for k in 'ohlc'}))
    for i,f in raw.items():feature.add(dict(signal_index=i,signal_start=stamp(bars[i]['t']),**{k:v for k,v in f.items() if k!='distances'},**{'distance_lb'+str(l):f['distances'][l] for l in LB}))
    for case in CASES:
        for p in paths[case[0]].values():rawpaths.add(dict(case=case[0],**p))
    per,months,rolling=periods()
    periodfields=list(period_row([],per[0]));statfields=list(stats([]))
    periodwriters=[make(n,['config','case']+periodfields) for n in ('periods','monthly','rolling')]
    sums=make('summary',['config','case','raw_signals','raw_signal_hash','accepted_hash','accepted','rejected','blocked','open_at_cutoff','isolated_eligible','isolated_closed','isolated_net_r','blank_complete_years','blank_complete_months','longest_blank_complete_months','worst_12m_r','worst_24m_r','worst_36m_r','median_stop_pips','median_cost_over_risk','median_entry_spread_pips','median_holding_hours','max_interentry_days','max_wait_including_edges_days']+statfields)
    comparisons=make('baseline_comparisons',['config','case','baseline_closed_r','candidate_closed_r','delta_r','common','added','removed','common_r_change','added_closed_r','removed_closed_r','identity_error'])
    marginal=make('baseline_marginal_trades',['config','case','kind','signal_index','r','qualifies_candidate','displaced_by_signal_index'])
    summary_lookup={};rawhash={};acceptedhash={}
    try:
        for num,c in enumerate(grid):
            cid,family,l,d,b,r=c
            boundaries=study_boundaries(cid)
            cf.add(dict(config=cid,family=family,lookback=l,distance_atr_max=d,body_atr_min=b,range_atr_min=r,boundary_dimensions=boundaries,**condition_metadata(cid,raw)))
            signals=[i for i,f in raw.items() if qualifies(f,c)];signalset=set(signals);rh=digest(signals);rawhash[cid]=rh
            all_masks[cid]=set(signals)
            for i in signals:memberships.add(dict(config=cid,signal_index=i))
            for case in CASES:
                name=case[0];meta={'config':cid,'case':name}
                accepted,rejected,blocked=replay(signals,paths[name]);all_ledgers[cid,name]=(accepted,rejected,blocked);ah=digest([core(p) for p in accepted]);acceptedhash[(cid,name)]=ah
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
                isolated=[paths[name][i] for i in signals if not paths[name][i]['reason']]
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
            progress(phase='REPLAY_AND_REPORTS',completed_configurations=num+1,total_configurations=50)
        neighbours=make('neighbours',['config','neighbour','dimension','case','net_r','neighbour_net_r','accepted','neighbour_accepted','same_raw_stream','same_accepted_stream'])
        for cid,oid,dimension in condition_neighbours():
            for case in CASES:
                name=case[0];x=summary_lookup[cid,name];y=summary_lookup[oid,name]
                neighbours.add(dict(config=cid,neighbour=oid,dimension=dimension,case=name,net_r=x['net_r'],neighbour_net_r=y['net_r'],accepted=x['accepted'],neighbour_accepted=y['accepted'],same_raw_stream=rawhash[cid]==rawhash[oid],same_accepted_stream=acceptedhash[cid,name]==acceptedhash[oid,name]))
        require(sums.count==400 and periodwriters[0].count==400*len(per) and periodwriters[1].count==400*len(months) and periodwriters[2].count==400*len(rolling),'Incomplete result matrix')
        duplicate=collections.defaultdict(list)
        for c,h in rawhash.items():duplicate[h].append(c)
        write_json(out/'duplicate_raw_streams.json',[v for v in duplicate.values() if len(v)>1])
        component_counts=paired_reports(out,all_ledgers,all_masks)
        write_json(out/'output_row_counts.json',dict({n:w.count for n,w in writers.items()},**component_counts))
        write_json(out/'RUN_COMPLETE.json',{'version':VERSION,'status':'COMPLETE','configurations':50,'execution_cases':8,'fixed_anchors':2,'pass1_anchor_parity':'PASS','pass2_control_parity':'PASS','pass2_control_cases':104,'pass3_comparator_parity':'PASS','pass3_comparator_cases':16,'summary_rows':400,'independent_raw_parity':'PASS','scope':'Exploratory standalone historical research; not portfolio admission or prospective profitability'})
    finally:
        for w in writers.values():w.close()


STATE={'status':'IDLE','version':VERSION,'read_only':True,'orders_sent':False,'requested_start':stamp(START),'cutoff_exclusive':stamp(END)}
STATE_LOCK=threading.Lock()
START_LOCK=threading.Lock()
OUTPUT=Path(os.environ.get('EURUSD_H4_PASS4_OUTPUT_DIR', '/data/eurusd_h4_pass4_research' if Path('/data').is_dir() else str(Path(tempfile.gettempdir())/'eurusd_h4_pass4_research')))


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
        explicit=os.environ.get('EURUSD_H4_PASS4_FROZEN_DATA')
        cached=OUTPUT/SOURCE_NAME
        beside=Path(__file__).resolve().parent/SOURCE_NAME
        prior=Path(os.environ.get('EURUSD_H4_PASS1_OUTPUT_DIR','/data/eurusd_h4_pass1_research' if Path('/data').is_dir() else str(Path(tempfile.gettempdir())/'eurusd_h4_pass1_research')))/SOURCE_NAME
        prior2=Path(os.environ.get('EURUSD_H4_PASS2_OUTPUT_DIR','/data/eurusd_h4_pass2_research' if Path('/data').is_dir() else str(Path(tempfile.gettempdir())/'eurusd_h4_pass2_research')))/SOURCE_NAME
        prior3=Path(os.environ.get('EURUSD_H4_PASS3_OUTPUT_DIR','/data/eurusd_h4_pass3_research' if Path('/data').is_dir() else str(Path(tempfile.gettempdir())/'eurusd_h4_pass3_research')))/SOURCE_NAME
        source=Path(explicit) if explicit else next((p for p in (cached,beside,prior3,prior2,prior) if p.exists()),None)
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
    if os.environ.get('EURUSD_H4_PASS4_AUTOSTART','true').lower()!='false':start()
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
    require(len(configs())==50 and len(CASES)==8,'Configuration count')
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
    require(len(DEFINITIONS)==50 and len(comparison_pairs())==105 and len(ANCHORS)==2,'Pass3 study count')
    return {'status':'PASS','scope':'Synthetic software checks only; no historical result'}


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--self-test',action='store_true');parser.add_argument('--run-once',action='store_true');parser.add_argument('--output-dir')
    args=parser.parse_args()
    if args.output_dir:OUTPUT=Path(args.output_dir)
    if args.self_test:print(json.dumps(self_test()));sys.exit(0)
    if args.run_once:
        run();print(json.dumps(snapshot()));sys.exit(0 if snapshot()['status']=='COMPLETE' else 1)
    if os.environ.get('EURUSD_H4_PASS4_AUTOSTART','true').lower()!='false':start()
    ThreadingHTTPServer(('0.0.0.0',int(os.environ.get('PORT','8000'))),Handler).serve_forever()
