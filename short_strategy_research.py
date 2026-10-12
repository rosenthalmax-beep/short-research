#!/usr/bin/env python3
"""EUR/USD H4 LONG Pass 1. Standard-library Python 3.10+. Research ONLY.
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

VERSION = 'EURUSD_H4_LONG_PASS1_ENGULFING_DISCOVERY_V2_2026_10_12'
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
RESULT_NAME = 'EURUSD_H4_LONG_PASS1_RESULTS.zip'
PROTOCOL = r'''# EUR/USD H4 LONG — first engulfing discovery
Version: EURUSD_H4_LONG_PASS1_ENGULFING_DISCOVERY_V2_2026_10_12
Protocol fixed before viewing EUR/USD outcomes. All results exploratory, not OOS.

## Source and clock
Request OANDA EUR_USD native completed H4 MID/BID/ASK and independent H1 MID,
2005-01-01 00:00 UTC through 2026-10-12 00:00 UTC exclusive. H4 dailyAlignment=0,
alignmentTimezone=UTC, smooth=false. H4 starts 00/04/08/12/16/20 UTC. This may
differ from your broker-day/TradingView chart. Price strings retain precision.
H1 bins must reproduce every native H4 MID OHLC exactly (1e-10 tolerance).
Partial market-opening/closing bins are retained when crosschecks pass; observed
H1 count/first timestamp are reported. Missing periods are never filled. Source
coverage must pass full-history minimums or the run fails, with no performance
results. H4 minimum 25,000; H1 100,000; every completed calendar year at least
1,000 H4 and 4,000 H1; partial 2026 minimum 650/2,600. First bar within seven days
of requested start, last nominal close within four days of cutoff. These checks
and source gaps are diagnostics, not proof of absence of every missing quote.
Native OHLC cannot resolve tick ordering, latency, liquidity, or all data gaps.
Archive all source data, API receipts, integrity hashes, and exact runner.

## Signal and finite grid
Common first eligible signal index 200. ATR14: TR index 1..14 mean at index14,
then Wilder recurrence through the signal candle; no future data. Strict red
previous candle and strict green current candle. Current open <= prior close;
current close >= prior open; both ranges positive. Prior low excludes signal.
Distance = ABS(signal low - minimum prior LB lows) / current ATR. Body and range
use current ATR. No trend, session, weekday, previous momentum or close-location
filter. Features may be reported descriptively, not searched.
398 configurations: raw1, structure-only30, body-only4, range-only3,
structure x body x range360. Lookbacks20/40/60/100/150/200; distance caps
.05/.10/.20/.35/.50ATR; body floors .50/.75/1/1.25ATR; range floors .75/1/1.50ATR.
All weak neighbours, boundaries and duplicate streams retained. No best-row
selection or automatic admission. RR3 is a reference, not an optimised target.

## Stops, entry, execution and eight cases
Stop = signal MID low - .0001 (one pip); target = signal MID close + 3*(MID close
- stop). R denominator = actual assumed fill - stop; target does not move with
fill cost. Six MID cases: adverse entry +1/+2/+4 pips at signal nominal close,
paired across two intrabar models. Four pips is an assumed stress, not a measured
normal spread or a universal rejection threshold. No extra spread double-count.
MID paths use future MID candles. They can posit non-executable weekend-close
entries; next-observed delay flags make that assumption explicit.
STOP_FIRST_GAP_STRESS: opening below stop exits at open (loss may exceed 1R),
opening above target capped at target; if both hit inside a bar, stop first.
NEAREST_OPEN_SENSITIVITY: for MID proxies only, ignore opening gaps and resolve
simultaneous touch by nearer low/high distance from open (stop on tie). This is
an alternative assumption, not known intrabar order.
Two BIDASK cases: next observed H4 ASK open; earliest constituent H1 timestamp
is the available entry-time proxy. Reject if later than nominal signal close
+120sec, if adverse fill exceeds 10ticks (=1pip), fill<=stop, fill>=target,
or no next bar. Use BID exits, with gap losses/target caps in BOTH models;
only dual-touch ordering differs. No extra slippage added. H1 time granularity
means this is a historical quote-side approximation to the recorded execution
limits, not actual fill parity or proof of tradeability. Adverse 10tick entry
limit is not a total spread cap. Stop/target tick rounding is not imposed here;
live price rounding and exact payload parity belong to later frozen validation.
One open trade per standalone candidate (pyramiding zero), replayed separately
for every case. Rejections do not occupy. A signal on an exit candle can enter
after that candle closes. Entry is after signal; exits never inspect signal-bar
high/low. Signals whose nominal close is at/after the cutoff are rejected.
Open-at-cutoff trades occupy and carry blank realised R. Unfinished
trades are not discarded as if never taken. No forced cutoff exit.

## Reporting and interpretation
Full accepted/rejected/blocked ledgers, raw candidate membership, raw independent
paths, raw baseline control hashes, isolated-signal versus chronological results,
calendar years, eras, trailing1/2/3/5/10years, monthly and every complete
monthly-ended rolling12/24/36M window (including blank windows). Calendar2026
and October2026 are marked partial. Period realised R is assigned by nominal
exit-bar END in (start,end]; entries by [start,end). H4 exit times are bin-end
proxies, not exact fills. Rolling final end=2026-10-01 (October incomplete).
R drawdown is closed additive R in exit order, not compounded NAV or marked/open
risk. Win/loss uses realised R. Open counts and cohort outcome R are separate.
Baseline comparisons use full replay: common+added-removed R identity checked,
including common outcome differences; extra entries can displace other signals.
Neighbour report compares immediate coarse one-axis levels; duplicate raw and
accepted ledgers explicitly identified and are not independent confirmations.
Cost/stop, spread, holding time, inter-entry gaps, blank years/months and recent
activity remain visible. Review economically credible regions, not highest R.

Portfolio reference: PORTFOLIO32_2026_10_05_EURCHF_H1_SHORT_PRIMARY_RR3P50_V1.
No incumbent raw streams, risk sizing, currency conversion, financing, fees,
portfolio gate replay or exact N->N+1 comparison in this discovery runner.
Do not infer portfolio improvement from standalone R. All live code unchanged.
A later candidate requires frozen independent implementation, exact portfolio
comparison and prospective execution evidence. Reused history is exploratory.

## Operational contract
Standard-library Python3.10+, separate service/repository, one process/worker.
GET-only broker candles endpoint; no account/order calls. GET/HEAD /status,
/health, /results and /source. POST rejected. Background research auto-starts.
Results only served after COMPLETE, or a failure-only ZIP after ERROR. No partial
performance on failed source/control/parity gates. Frozen source reused without
refresh. Explicit missing/corrupt source fails. /data cache if available; local
temporary directory otherwise. No persisted cache means source refetch on restart.
No performance conclusions from built-in or delivery synthetic software tests.
API basis: https://github.com/oanda/v20-python/blob/master/src/v20/instrument.py
and https://developer.oanda.com/rest-live-v20/instrument-df/
'''


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


def configs():
    rows = [('RAW', 'raw', 0, None, None, None)]
    rows += [(f'S_L{l}_D{d:g}', 'structure', l, d, None, None) for l,d in itertools.product(LB,DIST)]
    rows += [(f'B_{b:g}', 'body', 0, None, b, None) for b in BODY]
    rows += [(f'R_{r:g}', 'range', 0, None, None, r) for r in RANGE]
    rows += [(f'M_L{l}_D{d:g}_B{b:g}_R{r:g}', 'matrix', l,d,b,r) for l,d,b,r in itertools.product(LB,DIST,BODY,RANGE)]
    require(len(rows)==398 and len({r[0] for r in rows})==398, 'Grid definition failure')
    return rows


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


def features(bars):
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


def qualifies(f, config):
    _,_,l,d,b,r=config
    return (not l or f['distances'][l]<=d) and (b is None or f['body_atr']>=b) and (r is None or f['range_atr']>=r)


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


def independent_raw(bars, case):
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
    write_json(out/'independent_raw_controls.json',controls)
    write_json(out/'protocol.json',{'version':VERSION,'start_utc':stamp(START),'cutoff_exclusive_utc':stamp(END),'cases':CASES,'rr':RR,'configurations':398,'source_h4_count':len(bars),'raw_indices_sha256':digest(indices)})
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
    cf=make('configurations',['config','family','lookback','distance_atr_max','body_atr_min','range_atr_min','boundary_dimensions'])
    feature=make('raw_signal_features',['signal_index','signal_start','atr','body_atr','range_atr','close_location','body_ratio']+['distance_lb'+str(l) for l in LB])
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
            cf.add(dict(config=cid,family=family,lookback=l,distance_atr_max=d,body_atr_min=b,range_atr_min=r,boundary_dimensions='|'.join(boundaries)))
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
                ba={p['signal_index']:p for p in baseline[name]};ca={p['signal_index']:p for p in accepted}
                added=ca.keys()-ba.keys();removed=ba.keys()-ca.keys();common=ba.keys()&ca.keys()
                val=lambda p:0. if p['r'] is None else p['r']
                ar=sum(val(ca[i]) for i in added);rr=sum(val(ba[i]) for i in removed);cr=sum(val(ca[i])-val(ba[i]) for i in common)
                br=sum(val(p) for p in ba.values());delta=realised['net_r']-br
                err=delta-(ar-rr+cr);require(abs(err)<1e-8,'Marginal replay R identity failed')
                comparisons.add(dict(meta,baseline_closed_r=br,candidate_closed_r=realised['net_r'],delta_r=delta,common=len(common),added=len(added),removed=len(removed),common_r_change=cr,added_closed_r=ar,removed_closed_r=rr,identity_error=err))
                cb={x['signal_index']:x['blocking_signal_index'] for x in blocked}
                bb={x['signal_index']:x['blocking_signal_index'] for x in replay(indices,paths[name])[2]}
                for kind,items,ledger,blocks in (('ADDED',added,ca,bb),('REMOVED',removed,ba,cb)):
                    for i in sorted(items):marginal.add(dict(meta,kind=kind,signal_index=i,r=ledger[i]['r'],qualifies_candidate=i in signalset,displaced_by_signal_index=blocks.get(i)))
            progress(phase='REPLAY_AND_REPORTS',completed_configurations=num+1,total_configurations=398)
        neighbours=make('neighbours',['config','neighbour','dimension','case','net_r','neighbour_net_r','accepted','neighbour_accepted','same_raw_stream','same_accepted_stream'])
        byparams={c[2:]:c[0] for c in grid}
        for c in grid:
            for pos,levels,dimension in ((2,LB,'lookback'),(3,DIST,'distance'),(4,BODY,'body'),(5,RANGE,'range')):
                if c[pos] not in levels:continue
                j=levels.index(c[pos])
                for k in (j-1,j+1):
                    if k<0 or k>=len(levels):continue
                    other=list(c);other[pos]=levels[k];oid=byparams.get(tuple(other[2:]))
                    if not oid:continue
                    for case in CASES:
                        name=case[0];x=summary_lookup[c[0],name];y=summary_lookup[oid,name]
                        neighbours.add(dict(config=c[0],neighbour=oid,dimension=dimension,case=name,net_r=x['net_r'],neighbour_net_r=y['net_r'],accepted=x['accepted'],neighbour_accepted=y['accepted'],same_raw_stream=rawhash[c[0]]==rawhash[oid],same_accepted_stream=acceptedhash[c[0],name]==acceptedhash[oid,name]))
        require(sums.count==3184 and periodwriters[0].count==3184*len(per) and periodwriters[1].count==3184*len(months) and periodwriters[2].count==3184*len(rolling),'Incomplete result matrix')
        duplicate=collections.defaultdict(list)
        for c,h in rawhash.items():duplicate[h].append(c)
        write_json(out/'duplicate_raw_streams.json',[v for v in duplicate.values() if len(v)>1])
        write_json(out/'output_row_counts.json',{n:w.count for n,w in writers.items()})
        write_json(out/'RUN_COMPLETE.json',{'version':VERSION,'status':'COMPLETE','configurations':398,'execution_cases':8,'summary_rows':3184,'independent_raw_parity':'PASS','scope':'Exploratory standalone historical research; not portfolio admission or prospective profitability'})
    finally:
        for w in writers.values():w.close()


STATE={'status':'IDLE','version':VERSION,'read_only':True,'orders_sent':False,'requested_start':stamp(START),'cutoff_exclusive':stamp(END)}
STATE_LOCK=threading.Lock()
START_LOCK=threading.Lock()
OUTPUT=Path(os.environ.get('EURUSD_H4_PASS1_OUTPUT_DIR', '/data/eurusd_h4_pass1_research' if Path('/data').is_dir() else str(Path(tempfile.gettempdir())/'eurusd_h4_pass1_research')))


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
        explicit=os.environ.get('EURUSD_H4_PASS1_FROZEN_DATA')
        cached=OUTPUT/SOURCE_NAME
        beside=Path(__file__).resolve().parent/SOURCE_NAME
        source=Path(explicit) if explicit else (cached if cached.exists() else beside if beside.exists() else None)
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
    if os.environ.get('EURUSD_H4_PASS1_AUTOSTART','true').lower()!='false':start()
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
    require(len(configs())==398 and len(CASES)==8,'Configuration count')
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
    return {'status':'PASS','scope':'Synthetic software checks only; no historical result'}


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--self-test',action='store_true');parser.add_argument('--run-once',action='store_true');parser.add_argument('--output-dir')
    args=parser.parse_args()
    if args.output_dir:OUTPUT=Path(args.output_dir)
    if args.self_test:print(json.dumps(self_test()));sys.exit(0)
    if args.run_once:
        run();print(json.dumps(snapshot()));sys.exit(0 if snapshot()['status']=='COMPLETE' else 1)
    if os.environ.get('EURUSD_H4_PASS1_AUTOSTART','true').lower()!='false':start()
    ThreadingHTTPServer(('0.0.0.0',int(os.environ.get('PORT','8000'))),Handler).serve_forever()
