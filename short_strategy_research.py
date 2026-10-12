#!/usr/bin/env python3
"""EUR/USD H4 LONG Pass 2. Standard-library Python 3.10+. Research ONLY.
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

VERSION = 'EURUSD_H4_LONG_PASS2_CONDITIONAL_FEATURES_V1_2026_10_12'
UTC = dt.timezone.utc
START = int(dt.datetime(2005, 1, 1, tzinfo=UTC).timestamp())
END = int(dt.datetime(2026, 10, 12, tzinfo=UTC).timestamp())
H4 = 14400
PIP = .0001
TICK = .00001
RR = 3.0
LB = (20, 40, 60, 100, 150, 200)
DIST = (.05, .10, .20, .35, .50)
BODY = (.50, .75, 1., 1.25)
RANGE = (.75, 1., 1.50)
MODELS = ('STOP_FIRST_GAP_STRESS', 'NEAREST_OPEN_SENSITIVITY')
CASES = [(f'MID_{p}P_{m}', 'MID', p, m) for p in (1, 2, 4) for m in MODELS]
CASES += [(f'BIDASK_BOUND10_{m}', 'BIDASK', 0, m) for m in MODELS]
SOURCE_NAME = 'EURUSD_H4_PASS1_FROZEN_DATA.zip'
RESULT_NAME = 'EURUSD_H4_LONG_PASS2_RESULTS.zip'
PROTOCOL = "# EUR/USD H4 LONG — Pass 2: one conditional feature at a time\nVersion: EURUSD_H4_LONG_PASS2_CONDITIONAL_FEATURES_V1_2026_10_12\nPredeclared conditional study on the repeatedly inspected 2005–2026 history.\nAll results exploratory. No combinations, geometry or RR optimisation in this pass.\n\n## Fixed anchors and comparison\nBROAD = exact Pass1 M_L40_D0.35_B0.75_R1: prior-low lookback40,\nabsolute low-distance<=.35 current H4 ATR, body>=.75 ATR, range>=1 ATR.\nSTRONG_BODY = exact Pass1 M_L40_D0.5_B1.25_R1.5: lookback40,\nabsolute low-distance<=.50 current H4 ATR, body>=1.25 ATR, range>=1.50 ATR.\nRR3, stop signal MID low-minus-one-pip; target and all eight execution cases are\nidentical to Pass1. The strong-body anchor lies at a tested body boundary and is\nnot an established plateau. Conditions are added separately to each anchor.\nEvery candidate compares with its OWN unchanged anchor; raw controls are also\nreproduced. No moving best-row baseline. Baseline net R, accepted ledger digests,\nraw membership hashes and source price hashes are pinned to successful Pass1.\nAny changed source or mismatched controls fail before conditional results.\n\n## Finite conditional definitions: 55 per anchor, 110 total\n1 unchanged CONTROL + 54 single-condition alternatives:\n- Signal close location >=.60/.70/.80/.90 (4).\n- Previous candle close location <=.25/.50/.75 (3).\n- Previous body/current H4 ATR >=.25/.50/.75/1.00 (4).\n- Previous range/current H4 ATR >=.75/1.00/1.50 (3).\n- Signal lower wick/body >=.10/.25/.50 (3).\n- Signal body/range >=.60/.70/.80 (3).\n- Prior movement for 6/12/30 observed H4 bars: previous close minus close\n  N observed bars before previous, divided by current H4 ATR; either >=.50/1.00\n  (preceding rise) or <=-.50/-1.00 (preceding fall). 12 alternatives.\n- Current H4 ATR / mean previous60 H4 ATR: <=.75/1.00/1.25 or >=1.00/1.25/1.50 (6).\n- Completed UTC daily close above/below EMA50/100/200: six alternatives.\n- Completed daily EMA50 slope over five observed UTC daily candles: >0 or <0 (2).\n- Completed daily ATR14 / mean previous60 daily ATR: <=.75/1.00/1.25 or\n  >=1.00/1.25/1.50 (6).\n- Last completed daily close rise/fall versus prior daily close: >0 or <0 (2).\n\nClose location=(close-low)/(high-low). Previous candle metrics use its own\npositive range and the signal's current H4 ATR. Wick/body and body/range use\npositive bullish signal body. No candidate changes the anchor's geometry.\nObserved-bar lookbacks include gaps in wall-clock time; no fabricated bars.\nCurrent H4 volatility excludes its own ATR from the trailing60 denominator.\n\n## Completed daily context and causality\nUTC daily OHLC aggregates the pinned native UTC H4 MID bins, already crosschecked\nagainst H1 aggregation. Each daily context ends at 00:00 UTC of the next day.\nOnly daily contexts with END<=nominal signal close are eligible. This includes\na just-completed day for an H4 signal ending at midnight. No unfinished daily\nclose, high, low, ATR or EMA. Missing days are not filled. Partial Sunday/Friday\nmarket days are retained and reported with observed H1 count; this convention\nis not a broker 17:00 New York D1 candle. Daily EMA seeds from first observed\nclose then causal recurrence. Daily ATR14 uses TR1..14 then Wilder recurrence.\nAll daily-filter alternatives additionally require at least200 completed\nobserved UTC daily candles and positive daily ATR. Missing daily context means\nthat condition fails; it never falls back to control. Missing-context counts\nare reported. Daily-volatility denominator excludes the latest daily ATR.\n\n## Reporting and controls\nFull P0 replay per candidate and eight cases. Added/removed/common accepted\ntrades, R identity and blocked-signal displacement compared with each fixed\nanchor. Filtering can unlock later trades: do not interpret raw mask subtraction\nas accepted-trade attribution. All yearly/era/trailing/monthly/rolling reports,\nblank periods, recent counts, costs/stop, open at cutoff and candidate-only\noutcomes retained. Immediate condition thresholds reported within each\nanchor/feature/operator/horizon. Duplicate raw and accepted streams grouped;\nthey are not independent confirmations. No auto-selected candidate or live gate.\n\n## Inherited price, execution and source conventions\nSource: EUR_USD OANDA completed unsmoothed H4 MID/BID/ASK, UTC00 daily alignment,\n2005-01-01 to 2026-10-12 00:00UTC exclusive, independent native H1 MID crosscheck.\nFirst eligible signal index200. Strict previous red/current green inclusive\nbody engulf. ATR14 uses TR1..14 seed, then current-signal Wilder recurrence.\nStop=signal MID low-.0001; target=MID close+3*(MID close-stop).\nAssumed MID entries+1/+2/+4 adverse pips. R uses assumed fill-stop, target fixed.\nSTOP_FIRST_GAP_STRESS keeps adverse opening stop gaps and caps favourable target\ngaps; dual touch stop-first. NEAREST_OPEN_SENSITIVITY MID case ignores opening\ngaps and uses nearer low/high-to-open dual-touch rule, stop on tie.\nBIDASK entry is next observed H4 ASK open timed by earliest observed constituent\nH1 timestamp; reject delay>120sec, adverse entry>10ticks (=1pip), invalid stop/\nentry/target or unavailable next bar. BID exits keep stop gaps and cap target\ngaps in BOTH models; only simultaneous touch differs. No extra slippage applied.\nCandle data cannot prove actual fills, timing or intrabar order. Four pips is an\nassumed stress, not measured normal cost. No financing/fees/portfolio NAV model.\nNominal entry at/after cutoff rejected. Never inspect signal OHLC for exit.\nOne standalone position; signal on exit candle allowed; rejections do not occupy;\nopen-at-cutoff retains occupation and blank R. R drawdown is additive closed R,\nnot NAV or marked/open-risk drawdown. Entry periods[start,end), realised exits\n(start,end] use nominal exit-bin END timestamps, not precise broker fills.\n2026 and October marked partial. Rolling12/24/36M ends at completed month\nboundaries through2026-10-01, including zero-trade windows.\n\n## Operation\nSingle-file Python3.10+ standard library. Separate read-only research service.\nReuse EURUSD_H4_PASS1_FROZEN_DATA.zip beside app.py, in Pass2 cache, or in previous\nPass1 cache. Optional EURUSD_H4_PASS2_FROZEN_DATA explicit path; missing/corrupt\nexplicit source fails. If no archive remains, automatically GET the same broker\nhistory and require identical pinned price hashes. No silent history changes.\nOutput defaults /data/eurusd_h4_pass2_research when /data exists; otherwise temp.\nGET/HEAD /status, /health, /results, /source; no write or broker order endpoints.\nFailure-only ZIP for ERROR, full results for COMPLETE, one process/worker.\nPortfolio reference PORTFOLIO32_2026_10_05_EURCHF_H1_SHORT_PRIMARY_RR3P50_V1.\nExact incumbent overlap/risk admission and prospective fill study are later gates.\n"

PASS1_PINS = {'anchors': {'M_L40_D0.35_B0.75_R1': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '106', 'accepted_hash': 'd979d86a2d02e760a8a1c6fafb0bdae84bda735a0edfc7744d75ae817fcba29a', 'blocked': '3', 'net_r': '8.3450542151461', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '8'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '106', 'accepted_hash': 'd979d86a2d02e760a8a1c6fafb0bdae84bda735a0edfc7744d75ae817fcba29a', 'blocked': '3', 'net_r': '8.3450542151461', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '8'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '114', 'accepted_hash': '48f8a41dff9a17722f2de6fc36edc9ef1469b92049306d3f2a07ea4b907e699c', 'blocked': '3', 'net_r': '19.081030949268005', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '114', 'accepted_hash': '78a2087e6dd35db8eecdd723d2c8ba75ff335a568aeacb543e395696ae236958', 'blocked': '3', 'net_r': '19.026589688523014', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '114', 'accepted_hash': '03161b82a37fe9af87cbe8f2c961718b18da7e1cea17d85e1bd80bf7a139e7a9', 'blocked': '3', 'net_r': '16.308044523646373', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '114', 'accepted_hash': 'f6c9a160a4b082c827ae188a6230a19c9431c5d746c558c6ae204b2459efbb7a', 'blocked': '3', 'net_r': '16.254372207262186', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '114', 'accepted_hash': '40bba3c1fe2437e77333301b71d69137e459973c633fba67923e86a11fd218a3', 'blocked': '3', 'net_r': '11.152458563507945', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '114', 'accepted_hash': '01b8778e70d74aa68df953647bea0649fedd55383dcd1847aa5d6efa475c0848', 'blocked': '3', 'net_r': '11.100260761310135', 'raw_signal_hash': 'a71abfe98810cbfc9c70c3318de13705cc519a03b71ef82c75a309b329cceaf6', 'rejected': '0'}}, 'M_L40_D0.5_B1.25_R1.5': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': '52', 'accepted_hash': '7af667268c5fda50ea8eaf3a902ac2e9868163f08bb4919e188cea8fc459d00d', 'blocked': '1', 'net_r': '7.355862354561232', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '5'}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': '52', 'accepted_hash': '7af667268c5fda50ea8eaf3a902ac2e9868163f08bb4919e188cea8fc459d00d', 'blocked': '1', 'net_r': '7.355862354561232', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '5'}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': '57', 'accepted_hash': 'c7660408058093dd74beda9f4445e82cb3a9c934fd51ef9d957c669ab9adafa6', 'blocked': '1', 'net_r': '13.896274044063015', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': '57', 'accepted_hash': '3d2d17610f9ea033c348a76eb2dd1ed7b17e42afb0b02e45b18a7d25c8827c4a', 'blocked': '1', 'net_r': '13.841832783318022', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': '57', 'accepted_hash': '2237d6a869858b55476744b6c270088252e3915dbfbe9661f3adfc4c89d9c923', 'blocked': '1', 'net_r': '12.831981788009386', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': '57', 'accepted_hash': 'c1eccd64fd9c8dbfafeb1754e1e40388071d28fbef4845b9dc999aeab951dcb9', 'blocked': '1', 'net_r': '12.7783094716252', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': '57', 'accepted_hash': '8064ca9c491dad8154834cb65ae2ccb50291e1ff36c5ead6807d8634cda37405', 'blocked': '1', 'net_r': '10.81186068156517', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': '57', 'accepted_hash': '7ebb1926f01bda5d7bdc48dee803ee0a1f0dd80eee5bf901edf6b0eec994a02c', 'blocked': '1', 'net_r': '10.75966287936736', 'raw_signal_hash': 'efeaae913dde8cb9f9916a786da056fa67bdc2dcde84e435a22552180cc38497', 'rejected': '0'}}}, 'raw_controls': {'BIDASK_BOUND10_NEAREST_OPEN_SENSITIVITY': {'accepted': 1407, 'blocked': 1065, 'independent_parity': 'PASS', 'ledger_sha256': '6dcb73d2b7ed43b9e6da129d40bed9af12693f58dba015af9808f1e30a9c6929', 'raw_signals': 2669, 'rejected': 197}, 'BIDASK_BOUND10_STOP_FIRST_GAP_STRESS': {'accepted': 1407, 'blocked': 1065, 'independent_parity': 'PASS', 'ledger_sha256': 'a9ddf63bdce468287f3f8655f8da82e20a4e89638f07d3d764d130eb68ffdc36', 'raw_signals': 2669, 'rejected': 197}, 'MID_1P_NEAREST_OPEN_SENSITIVITY': {'accepted': 1512, 'blocked': 1157, 'independent_parity': 'PASS', 'ledger_sha256': '77659c43e40af3d7e14f392825771ea5331488e695165de183ddaef54df0329a', 'raw_signals': 2669, 'rejected': 0}, 'MID_1P_STOP_FIRST_GAP_STRESS': {'accepted': 1512, 'blocked': 1157, 'independent_parity': 'PASS', 'ledger_sha256': '3b56b148b22f70b7904e0d70ec391a48ae6897ce119340119c1d3f0989266ad5', 'raw_signals': 2669, 'rejected': 0}, 'MID_2P_NEAREST_OPEN_SENSITIVITY': {'accepted': 1512, 'blocked': 1157, 'independent_parity': 'PASS', 'ledger_sha256': 'fe8932a92e7bcbc7c179d1f615fc787d970035555c01a715a1afbf02edfc29bf', 'raw_signals': 2669, 'rejected': 0}, 'MID_2P_STOP_FIRST_GAP_STRESS': {'accepted': 1512, 'blocked': 1157, 'independent_parity': 'PASS', 'ledger_sha256': 'f690dd8becdf7df86835afa98ae4b30bc88ca719a82e01a1967a74a6e4973042', 'raw_signals': 2669, 'rejected': 0}, 'MID_4P_NEAREST_OPEN_SENSITIVITY': {'accepted': 1512, 'blocked': 1157, 'independent_parity': 'PASS', 'ledger_sha256': '163d14184269d601d935c3e68c7bf33540145b8b796889e5543dd9c9e3bbc27b', 'raw_signals': 2669, 'rejected': 0}, 'MID_4P_STOP_FIRST_GAP_STRESS': {'accepted': 1512, 'blocked': 1157, 'independent_parity': 'PASS', 'ledger_sha256': '5beec9478053c6350ede8faa8a00683c3c549b1779fb2bf4ea10e61e1650e7a6', 'raw_signals': 2669, 'rejected': 0}}, 'source': {'alignment': 'UTC_00_UNSMOOTHED', 'end': 1791763200, 'instrument': 'EUR_USD', 'members': {'h1.json': '258c40216bef3deb5a74bfaaa2403b4bd9f4a88533dc734c73f9e89a7f9569bb', 'h4.json': '5c7a5ced72379c5696251200c2447dcb142116ed97149d0d4379d42a06381087', 'receipts.json': '0f37599b041b5e45649c81ae6771745af0da3dfd456130c1cb5d3e17c9ae692d'}, 'start': 1104537600}}


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

def condition_definitions():
    conditions=[('CONTROL','control',None,None,None,0)]
    def add(family,feature,op,levels,horizon=0):
        for value in levels:
            conditions.append((f'{family}_{op}_{value:g}'+(f'_N{horizon}' if horizon else ''),family,feature,op,value,horizon))
    add('SIGNAL_CLOSE','close_location','GE',(.60,.70,.80,.90))
    add('PREVIOUS_CLOSE','previous_close_location','LE',(.25,.50,.75))
    add('PREVIOUS_BODY','previous_body_atr','GE',(.25,.50,.75,1.))
    add('PREVIOUS_RANGE','previous_range_atr','GE',(.75,1.,1.50))
    add('LOWER_WICK','lower_wick_body','GE',(.10,.25,.50))
    add('BODY_RATIO','body_ratio','GE',(.60,.70,.80))
    for n in (6,12,30):
        add('PRECEDING_MOVE',f'movement_{n}','GE',(.50,1.),n)
        add('PRECEDING_MOVE',f'movement_{n}','LE',(-1.,-.50),n)
    for prefix,feature in (('H4_VOL','h4_atr_ratio'),('DAILY_VOL','daily_atr_ratio')):
        add(prefix,feature,'LE',(.75,1.,1.25));add(prefix,feature,'GE',(1.,1.25,1.50))
    for n in (50,100,200):
        add('DAILY_EMA',f'daily_ema_distance_{n}','GT',(0.,),n)
        add('DAILY_EMA',f'daily_ema_distance_{n}','LT',(0.,),n)
    add('DAILY_SLOPE','daily_ema50_slope5','GT',(0.,),5)
    add('DAILY_SLOPE','daily_ema50_slope5','LT',(0.,),5)
    add('DAILY_DIRECTION','daily_change','GT',(0.,))
    add('DAILY_DIRECTION','daily_change','LT',(0.,))
    require(len(conditions)==55 and len({c[0] for c in conditions})==55,'Conditional definition count')
    return conditions

CONDITIONS=condition_definitions()
CONDITION_BY_ID={c[0]:c for c in CONDITIONS}


def configs():
    return [(a[0]+'__'+c[0],c[1],*a[2:]) for a in ANCHORS for c in CONDITIONS]


def anchor_for(cid):
    return next(a for a in ANCHORS if a[0]==cid.split('__')[0])


def condition_for(cid):
    return CONDITION_BY_ID[cid.split('__',1)[1]]


def anchor_qualifies(f,anchor):
    _,_,l,d,b,r=anchor
    return f['distances'][l]<=d and f['body_atr']>=b and f['range_atr']>=r


def condition_metadata(cid,raw):
    _,family,feature,op,value,horizon=condition_for(cid)
    anchor=anchor_for(cid)
    missing=sum(anchor_qualifies(f,anchor) and feature is not None and f[feature] is None for f in raw.values())
    return {'anchor':anchor[0],'condition_family':family,'feature':feature,'operator':op,'threshold':value,'horizon':horizon,'missing_feature_signals':missing}


def condition_neighbours():
    groups=collections.defaultdict(list)
    for c in CONDITIONS:
        if c[0]!='CONTROL':groups[c[1],c[2],c[3],c[5]].append(c)
    for anchor in ANCHORS:
        for group in groups.values():
            ordered=sorted(group,key=lambda c:c[4])
            for a,b in zip(ordered,ordered[1:]):
                yield anchor[0]+'__'+a[0],anchor[0]+'__'+b[0],a[1]
                yield anchor[0]+'__'+b[0],anchor[0]+'__'+a[0],a[1]


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
    if not anchor_qualifies(f,anchor_for(config[0])):return False
    _,_,feature,op,value,_=condition_for(config[0])
    if feature is None:return True
    actual=f[feature]
    if actual is None:return False
    return {'GE':lambda:actual>=value,'LE':lambda:actual<=value,'GT':lambda:actual>value,'LT':lambda:actual<value}[op]()


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
    baselines={(anchor[0],case[0]):replay([i for i,f in raw.items() if anchor_qualifies(f,anchor)],paths[case[0]]) for anchor in ANCHORS for case in CASES}
    write_json(out/'independent_raw_controls.json',controls)
    write_json(out/'protocol.json',{'version':VERSION,'start_utc':stamp(START),'cutoff_exclusive_utc':stamp(END),'cases':CASES,'rr':RR,'configurations':110,'source_h4_count':len(bars),'raw_indices_sha256':digest(indices)})
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
    cf=make('configurations',['config','family','lookback','distance_atr_max','body_atr_min','range_atr_min','boundary_dimensions','anchor','condition_family','feature','operator','threshold','horizon','missing_feature_signals'])
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
            boundaries=[]
            for name,value,levels in (('lookback',l,LB),('distance',d,DIST),('body',b,BODY),('range',r,RANGE)):
                if value is not None and value!=0 and value in (levels[0],levels[-1]):boundaries.append(name)
            cf.add(dict(config=cid,family=family,lookback=l,distance_atr_max=d,body_atr_min=b,range_atr_min=r,boundary_dimensions='|'.join(boundaries),**condition_metadata(cid,raw)))
            signals=[i for i,f in raw.items() if qualifies(f,c)];signalset=set(signals);rh=digest(signals);rawhash[cid]=rh
            for i in signals:memberships.add(dict(config=cid,signal_index=i))
            for case in CASES:
                name=case[0];meta={'config':cid,'case':name}
                accepted,rejected,blocked=replay(signals,paths[name]);ah=digest([core(p) for p in accepted]);acceptedhash[(cid,name)]=ah
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
            progress(phase='REPLAY_AND_REPORTS',completed_configurations=num+1,total_configurations=110)
        neighbours=make('neighbours',['config','neighbour','dimension','case','net_r','neighbour_net_r','accepted','neighbour_accepted','same_raw_stream','same_accepted_stream'])
        for cid,oid,dimension in condition_neighbours():
            for case in CASES:
                name=case[0];x=summary_lookup[cid,name];y=summary_lookup[oid,name]
                neighbours.add(dict(config=cid,neighbour=oid,dimension=dimension,case=name,net_r=x['net_r'],neighbour_net_r=y['net_r'],accepted=x['accepted'],neighbour_accepted=y['accepted'],same_raw_stream=rawhash[cid]==rawhash[oid],same_accepted_stream=acceptedhash[cid,name]==acceptedhash[oid,name]))
        require(sums.count==880 and periodwriters[0].count==880*len(per) and periodwriters[1].count==880*len(months) and periodwriters[2].count==880*len(rolling),'Incomplete result matrix')
        duplicate=collections.defaultdict(list)
        for c,h in rawhash.items():duplicate[h].append(c)
        write_json(out/'duplicate_raw_streams.json',[v for v in duplicate.values() if len(v)>1])
        write_json(out/'output_row_counts.json',{n:w.count for n,w in writers.items()})
        write_json(out/'RUN_COMPLETE.json',{'version':VERSION,'status':'COMPLETE','configurations':110,'execution_cases':8,'fixed_anchors':2,'pass1_anchor_parity':'PASS','summary_rows':880,'independent_raw_parity':'PASS','scope':'Exploratory standalone historical research; not portfolio admission or prospective profitability'})
    finally:
        for w in writers.values():w.close()


STATE={'status':'IDLE','version':VERSION,'read_only':True,'orders_sent':False,'requested_start':stamp(START),'cutoff_exclusive':stamp(END)}
STATE_LOCK=threading.Lock()
START_LOCK=threading.Lock()
OUTPUT=Path(os.environ.get('EURUSD_H4_PASS2_OUTPUT_DIR', '/data/eurusd_h4_pass2_research' if Path('/data').is_dir() else str(Path(tempfile.gettempdir())/'eurusd_h4_pass2_research')))


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
        explicit=os.environ.get('EURUSD_H4_PASS2_FROZEN_DATA')
        cached=OUTPUT/SOURCE_NAME
        beside=Path(__file__).resolve().parent/SOURCE_NAME
        prior=Path(os.environ.get('EURUSD_H4_PASS1_OUTPUT_DIR','/data/eurusd_h4_pass1_research' if Path('/data').is_dir() else str(Path(tempfile.gettempdir())/'eurusd_h4_pass1_research')))/SOURCE_NAME
        source=Path(explicit) if explicit else (cached if cached.exists() else beside if beside.exists() else prior if prior.exists() else None)
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
    if os.environ.get('EURUSD_H4_PASS2_AUTOSTART','true').lower()!='false':start()
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
    require(len(configs())==110 and len(CASES)==8,'Configuration count')
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
    require(len(CONDITIONS)==55 and len(ANCHORS)==2,'Conditional study count')
    return {'status':'PASS','scope':'Synthetic software checks only; no historical result'}


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--self-test',action='store_true');parser.add_argument('--run-once',action='store_true');parser.add_argument('--output-dir')
    args=parser.parse_args()
    if args.output_dir:OUTPUT=Path(args.output_dir)
    if args.self_test:print(json.dumps(self_test()));sys.exit(0)
    if args.run_once:
        run();print(json.dumps(snapshot()));sys.exit(0 if snapshot()['status']=='COMPLETE' else 1)
    if os.environ.get('EURUSD_H4_PASS2_AUTOSTART','true').lower()!='false':start()
    ThreadingHTTPServer(('0.0.0.0',int(os.environ.get('PORT','8000'))),Handler).serve_forever()
