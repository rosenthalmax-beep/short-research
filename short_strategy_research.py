"""EURCHF M15 LONG Pass1: full-history engulfing-first research, not live trading.
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
VERSION='EURCHF_M15_LONG_PASS1_ENGULFING_DISCOVERY_V1_2026_10_09'
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
RESULT_NAME='EURCHF_M15_LONG_PASS1_ENGULFING_DISCOVERY_RESULTS.zip'
OUT=Path(os.getenv('EURCHF_M15_LONG_PASS1_OUTPUT_DIR','/tmp/eurchf_m15_long_pass1')).resolve()
PREFIX='/eurchf-m15-long-pass1'
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

LOOKBACKS=(20,40,60,100,150,200)
DISTANCES=(.05,.10,.25,.50)
BODY_FLOORS=(.50,.75,1.,1.25)
RANGE_FLOORS=(0.,1.,1.50)
SCAN_DISTANCES=(.05,.075,.10,.15,.25,.50,1.)
SCAN_BODIES=(.25,.50,.75,1.,1.25,1.50,1.75,2.,2.50)
SCAN_RANGES=(.50,.75,1.,1.25,1.50,1.75,2.,2.50)
PARENT_IDS=('RAW_EXACT_BULLISH_ENGULF',)
COMPARISON_IDS=PARENT_IDS


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
    allowed_on_error = {'baseline_parity.csv','protocol.md','run_manifest.json','error_report.csv','hard_controls.csv',
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
        set_status(state='starting',progress=0,message='Starting EUR/CHF M15 long Pass1 engulfing discovery',runner_sha256=code_hash(),result_path=None)
    if background:
        threading.Thread(target=run_job,name='eurchf-m15-long-pass1-research',daemon=True).start()
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
    if path=='/start' or (not STARTED and os.getenv('EURCHF_M15_LONG_PASS1_AUTOSTART','1')=='1'):
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
        current = dict(service='EUR/CHF M15 LONG Pass1 engulfing discovery',version=VERSION,status='/status',results='/results',
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
        if os.getenv('EURCHF_M15_LONG_PASS1_AUTOSTART','1')=='1':
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

BOUNDS=month_bounds()
PERIODS=period_definitions()

PROTOCOL='''# EURCHF M15 LONG — Pass1 engulfing-first discovery

Predeclared 9 October 2026, before calculating long performance. Template:
FOREX_STRATEGY_RESEARCH_TEMPLATE_AUDJPY_2026-09-24.md, a guide to staged research.
First question: is there an interpretable structure/quality region for bullish
engulfing under costs? This is not a live candidate or portfolio admission.
Previous unsuccessful short mechanisms and parked H1 long remain in the record.
Long thresholds are predeclared afresh; no short winner is copied as a long anchor.

## Data and signal semantics
SAME pinned EURCHF_M15_PASS1_FROZEN_DATA.zip alongside app.py. Native OANDA MID,
546647 M15 candles, 137939 H1 crosscheck candles. Requested 2005-01-01 through
2026-10-08T00Z exclusive; actual 2005-01-02T18:45Z through 2026-10-07T23:45Z.
Full ZIP/member byte/SHA/CRC gates, H1 OHLC/price-count aggregation, yearly coverage.
No new API fetch, synthetic bars or changed cutoff. UTC; observed bars across
closures/sparse history, not guaranteed elapsed hours. Common warmup200 for ALL
configurations, including raw. ATR14 = mean TR1..14 at index14, then Wilder.
Current completed-bar ATR is causal and includes signal range/gap; not prior ATR.
Exact bullish engulf: prevClose<prevOpen, close>open, open<=prevClose,
close>=prevOpen. Boundary equality accepted, dojis excluded. Prior low excludes
signal: min(low[i-L:i]). ABS distance=abs(signalLow-priorLow)/ATR[i].
Body=(close-open)/ATR[i]; range=(high-low)/ATR[i]. Thresholds inclusive.
No daily/volatility regime, momentum, close-location, session or calendar filters.

## Frozen bounded budget
348 unique definitions, 2088 model/cost cases:
1 raw control; 42 structure-only scans (L20/40/60/100/150/200 × distances
.05/.075/.10/.15/.25/.50/1.00 ATR); 9 body-only (.25/.50/.75/1/1.25/1.5/1.75/2/2.5);
8 range-only (.50/.75/1/1.25/1.5/1.75/2/2.5); 288 controlled matrix settings:
6 lookbacks ×4 distances .05/.10/.25/.50 ×4 bodies .50/.75/1/1.25 ×3 ranges 0/1/1.5.
These are a broad discovery matrix, NOT a local confirmation plateau.
Neighbour maps are family-specific: exactly one coordinate moves one tested step.
Body/range scans are separate families; don't pretend disconnected cells form a
joint plateau. All boundaries flagged, raw/accepted stream duplicates identified.
No automatic winner, adaptive extension, new filters or RR tuning in this pass.

## Long execution, fixed RR and chronological replay
Reference=signal close; entry time=signal open+15m. Stop=signal LOW-0.00010.
Target=reference+3*(reference-stop). RR3 fixed. Costs10/20/40 ticks=1/2/4 adverse
pips: BUY fill=reference+cost*.00001. Stop/target fixed; risk=fill-stop;
R=(exit-fill)/(fill-stop). Require0<stop<fill<target; invalid does not occupy.
Each definition/model/cost gets full independent P0 replay. Signal on exit candle
can enter at that candle's close; unresolved positions occupy through cutoff and
have NO invented R or forced close. No pair gate, compounding or portfolio sizing
at standalone discovery stage; exact Portfolio32 admission is deferred.
Primary STOP_FIRST_GAP_STRESS: opening stop gap exits min(open,stop), can lose
more than1R; favourable target gap capped at target; dual touch stop-first.
Opening gap timed at open, ordinary barrier exit at close. Alternate
NEAREST_OPEN_SENSITIVITY: on dual touch target if high-open < open-low, else stop
(tie loses), barrier-price fills/close timestamps. This is only a sensitivity.
MID is not historical executable bid/ask: long stop/target triggering on actual
broker quotes, financing and actual fills are not reconstructed by entry costs.

## Evidence and gates
Independent full-history ATR recurrence, direct prior-low/body/range masks,
exact bullish predicate, prefix-causality and complete config checks fail closed.
At runtime independently rebuild raw-control execution/trade chronology and
match every accepted field across ALL6 model/cost cases before other research.
Baseline fingerprints and complete control ledgers retained; implementation
parity on inspected history is not fresh out-of-sample evidence.
Full source, ATR, raw features/membership, first-touch paths, accepted/invalid/open
ledgers, all35 periods (22calendar years,4eras,5trailing,4CHF policy diagnostics),
262monthly intervals, 714 complete rolling12/24/36M windows PER case incl zeros.
Partial2026 and October marked; incompleteOctober never a rolling endpoint.
Entry cohort[start,end) and realized exits(start,end] distinct. Show blank years,
zero months and droughts; event exposure is attribution, not event exclusion.
Isolated signals and complete P0 outcomes separate. Full-replay added/removed
entries versus frozen raw control; replacement trades are not filtered ledger
differences. Fixed stop/cost-risk buckets describe costs; no stop optimization.
Additive R/closed-equity drawdown are not NAV% or intratrade risk measurements.

Review neighbouring support, distinct streams, economically useful counts,
cost survival, eras/recent years and rolling weaknesses. No spike-based selection
or treating hundreds of overlapping trials as independent evidence. All full
history already inspected is exploratory/in-sample. Only evidence can justify
freezing a few distinct anchors for a bounded later conditional pass. RR last;
independent final confirmation, portfolio admission and forward execution later.
Research-only single file; no order capability or live service modification.
'''
RESULT_README='''# EURCHF M15 LONG Pass1 results
Check run_manifest.complete=true and ALL source/software/hard/baseline gates PASS.
348 definitions/2088 cases. Read protocol.md and configuration_grid.csv first.
Use STOP_FIRST_GAP_STRESS as primary, NEAREST_OPEN_SENSITIVITY as sensitivity.
Compare all1/2/4pip cases, not only best full-history R. Neighbourhood families,
deduplication, yearly/rolling weak periods and frequency costs matter together.
Raw control + isolated signals + complete P0 accepted ledgers are distinct.
matched_raw_comparisons.csv/marginal_entry_differences.csv use separate replay.
Marginal rows are compact signal indices: join ADDED to candidate accepted_ledgers
and REMOVED to RAW_EXACT_BULLISH_ENGULF at the same model/cost for full trade fields.
No automated strategy selection/live admission. Return this complete ZIP.
'''

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

def make_configs():
    configs=[]
    def add(cid,family,lb=0,d=None,b=0.,r=0.):
        configs.append(dict(config_id=cid,role='CONTROL' if family=='RAW' else 'RESEARCH',family=family,
            geometry_key='LONG_SIGNAL_LOW',lookback=lb,distance_max_atr=d,body_min_atr=b,range_min_atr=r))
    add(PARENT_IDS[0],'RAW')
    for lb in LOOKBACKS:
        for d in SCAN_DISTANCES:add(f'S_L{lb:03d}_D{round(d*1000):04d}','STRUCTURE',lb,d)
    for b in SCAN_BODIES:add(f'B_ONLY_{round(b*100):03d}','BODY',b=b)
    for r in SCAN_RANGES:add(f'R_ONLY_{round(r*100):03d}','RANGE',r=r)
    for lb in LOOKBACKS:
        for d in DISTANCES:
            for b in BODY_FLOORS:
                for r in RANGE_FLOORS:
                    add(f'M_L{lb:03d}_D{round(d*1000):04d}_B{round(b*100):03d}_R{round(r*100):03d}','MATRIX',lb,d,b,r)
    return configs,[{k:c[k] for k in ('config_id','role','family')} for c in configs]

def make_features(bars):
    atr=atr14(bars);lows=[b[3] for b in bars]
    prior={lb:previous_low(lows,lb) for lb in LOOKBACKS};rows={}
    for i in range(WARMUP,len(bars)):
        if not bullish_engulf(bars,i) or atr[i] is None or atr[i]<=0:continue
        t,op,hi,lo,cl=bars[i]
        row=dict(signal_index=i,signal=iso(t),entry=iso(t+BAR),open=op,high=hi,low=lo,close=cl,
            atr14=atr[i],body_atr=(cl-op)/atr[i],range_atr=(hi-lo)/atr[i],reference_stop_pips=(cl-lo+10*TICK)/PIP)
        for lb in LOOKBACKS:
            row[f'previous_low_{lb}']=prior[lb][i]
            row[f'abs_distance_atr_{lb}']=abs(lo-prior[lb][i])/atr[i]
        rows[i]=row
    return {'LONG_SIGNAL_LOW':rows}

def selected_indices(config,features):
    result=[]
    for i,row in features['LONG_SIGNAL_LOW'].items():
        if row['body_atr']<config['body_min_atr'] or row['range_atr']<config['range_min_atr']:continue
        if config['distance_max_atr'] is not None and row[f"abs_distance_atr_{config['lookback']}"]>config['distance_max_atr']:continue
        result.append(i)
    return result

def build_paths(bars,features,configs):
    # Raw control spans every eligible engulf; all settings use identical stop/target.
    return {'LONG_SIGNAL_LOW':{i:find_paths(bars,i) for i in features['LONG_SIGNAL_LOW']}}

def hard_controls(bars,features,configs):
    checks=[]
    def check(name,ok):checks.append(dict(check=name,status='PASS' if ok else 'FAIL'))
    check('348_unique_definitions_288_matrix',len(configs)==348 and len({c['config_id'] for c in configs})==348 and sum(c['family']=='MATRIX' for c in configs)==288)
    ref=[None]*len(bars)
    if len(bars)>14:
        trs=[max(bars[i][2]-bars[i][3],abs(bars[i][2]-bars[i-1][4]),abs(bars[i][3]-bars[i-1][4])) for i in range(1,len(bars))]
        ref[14]=math.fsum(trs[:14])/14
        for i in range(15,len(bars)):ref[i]=ref[i-1]+(trs[i-1]-ref[i-1])/14
    check('independent_full_ATR_seed_recurrence',all((a is None and b is None) or (a is not None and b is not None and abs(a-b)<=1e-12) for a,b in zip(ref,atr14(bars))))
    expected=[i for i in range(WARMUP,len(bars)) if bars[i-1][1]>bars[i-1][4] and bars[i][1]<bars[i][4]
        and bars[i][1]<=bars[i-1][4] and bars[i][4]>=bars[i-1][1] and ref[i] is not None and ref[i]>0]
    rows=features['LONG_SIGNAL_LOW'];check('raw_exact_bullish_indices_independent',list(rows)==expected)
    direct={}
    for i in expected:
        t,op,hi,lo,cl=bars[i]
        direct[i]=dict(body_atr=(cl-op)/ref[i],range_atr=(hi-lo)/ref[i])
        for lb in LOOKBACKS:
            low=min(b[3] for b in bars[i-lb:i]);direct[i][f'previous_low_{lb}']=low
            direct[i][f'abs_distance_atr_{lb}']=abs(lo-low)/ref[i]
    check('all_prior_lows_body_range_direct_slices',all(i in rows and all(abs(rows[i][k]-v)<=1e-10 for k,v in r.items()) for i,r in direct.items()))
    for c in configs:
        independently=[i for i,r in direct.items() if r['body_atr']>=c['body_min_atr'] and r['range_atr']>=c['range_min_atr']
            and (c['distance_max_atr'] is None or r[f"abs_distance_atr_{c['lookback']}"]<=c['distance_max_atr'])]
        check(c['config_id']+'_independent_mask',selected_indices(c,features)==independently)
    n=min(10000,len(bars));check('prefix_causality',make_features(bars[:n])=={'LONG_SIGNAL_LOW':{i:r for i,r in rows.items() if i<n}})
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
    write_csv(work/'raw_bullish_engulf_features.csv',features['LONG_SIGNAL_LOW'].values(),fields)
    fields=['geometry_key','execution_model','signal_index','signal','entry','reference_entry','stop','target','next_open',
        'next_candle_delay_hours','exit_index','exit','exit_price','reason','dual_touch','gap_stop','gap_target']
    write_csv(work/'signal_trade_paths.csv',(dict(geometry_key=g,execution_model=m,**p) for g,paths in path_sets.items() for v in paths.values() for m,p in v.items()),fields)

def neighbour_ids(c,configs):
    axes={'MATRIX':(('lookback',LOOKBACKS),('distance_max_atr',DISTANCES),('body_min_atr',BODY_FLOORS),('range_min_atr',RANGE_FLOORS)),
        'STRUCTURE':(('lookback',LOOKBACKS),('distance_max_atr',SCAN_DISTANCES)),
        'BODY':(('body_min_atr',SCAN_BODIES),),'RANGE':(('range_min_atr',SCAN_RANGES),),'RAW':()}
    neighbours=[];edges=[]
    for axis,values in axes[c['family']]:
        pos=values.index(c[axis])
        if pos in (0,len(values)-1):edges.append(axis+('=LOWER' if pos==0 else '=UPPER'))
        for step in (-1,1):
            q=pos+step
            if not 0<=q<len(values):continue
            for other in configs:
                if other['family']!=c['family']:continue
                if other[axis]==values[q] and all(other[k]==c[k] for k in ('lookback','distance_max_atr','body_min_atr','range_min_atr') if k!=axis):
                    neighbours.append(other['config_id']);break
    return neighbours,edges

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

def export_matched_comparisons(work,configs):
    shutil.copyfile(work/'accepted_comparisons.csv',work/'matched_raw_comparisons.csv')

def export_extra_diagnostics(work,bars,features,path_sets,configs):
    # Ledgers are ordered by case. Stream one case at a time; don't retain the grid.
    refs=defaultdict(dict);groups=defaultdict(list);byid={c['config_id']:c for c in configs}
    marginal=CSVFile(work/'marginal_entry_differences.csv',CASE+['reference_config','change','signal_index','qualifies_in_candidate','qualifies_in_reference'])
    buckets=CSVFile(work/'stop_cost_attribution.csv',CASE+['bucket_axis','bucket','accepted_entries','closed_trades','open_trades','total_r','winners','losers','interpretation'])
    for c in configs:groups['RAW','ALL',0,signal_hash(selected_indices(c,features),bars)].append(c['config_id'])
    def flush(key,ledger):
        cid,model,cost=key;c=byid[cid]
        groups['ACCEPTED_SIGNAL',model,cost,sha('\n'.join(r['signal_index'] for r in ledger).encode())].append(cid)
        groups['ACCEPTED_EXECUTION',model,cost,sha('\n'.join(json.dumps({k:v for k,v in r.items() if k!='config_id'},sort_keys=True,separators=(',',':')) for r in ledger).encode())].append(cid)
        ours={int(r['signal_index']):r for r in ledger}
        if cid==PARENT_IDS[0]:refs[model,cost]=ours
        else:
            theirs=refs[model,cost];raw=set(selected_indices(c,features))
            for change,ids,source in [('ADDED',ours.keys()-theirs.keys(),ours),('REMOVED',theirs.keys()-ours.keys(),theirs)]:
                for i in sorted(ids):
                    marginal.add(dict(config_id=cid,execution_model=model,cost_ticks=cost,reference_config=PARENT_IDS[0],change=change,
                        signal_index=i,qualifies_in_candidate=int(i in raw),qualifies_in_reference=1))
        for axis,limits,labels in [('stop_pips',(5.,10.,20.,40.),('<=5','(5,10]','(10,20]','(20,40]','>40')),
            ('cost_fraction_reference_risk',(.05,.1,.2,.4),('<=.05','(.05,.10]','(.10,.20]','(.20,.40]','>.40'))]:
            assigned=[[] for _ in labels]
            for r in ledger:
                risk=float(r['reference_entry'])-float(r['stop']);v=risk/PIP if axis=='stop_pips' else cost*TICK/risk
                assigned[bisect.bisect_left(limits,v)].append(r)
            for label,subset in zip(labels,assigned):
                rs=[float(r['r']) for r in subset if r['r']!='']
                buckets.add(dict(config_id=cid,execution_model=model,cost_ticks=cost,bucket_axis=axis,bucket=label,accepted_entries=len(subset),
                    closed_trades=len(rs),open_trades=len(subset)-len(rs),total_r=math.fsum(rs),winners=sum(r>0 for r in rs),losers=sum(r<0 for r in rs),
                    interpretation='Fixed descriptive attribution; no optimized stop filter'))
    # Include empty cases too: explicit sequence from config/model/cost grid.
    with (work/'accepted_ledgers.csv').open(newline='') as f:
        it=iter(csv.DictReader(f));pending=next(it,None)
        for c in configs:
            for model in MODELS:
                for cost in COSTS:
                    key=c['config_id'],model,cost;ledger=[]
                    while pending and (pending['config_id'],pending['execution_model'],int(pending['cost_ticks']))==key:
                        ledger.append(pending);pending=next(it,None)
                    flush(key,ledger)
        if pending is not None:raise RuntimeError('Unexpected ledger case ordering')
    marginal.close();buckets.close()
    eq=[dict(equivalence_kind=k,execution_model=m,cost_ticks=c,fingerprint=h,configuration_count=len(ids),config_ids=';'.join(ids),
        research_configuration_count=sum(byid[i]['role']=='RESEARCH' for i in ids),interpretation='Overlapping/identical trials are not independent evidence') for (k,m,c,h),ids in sorted(groups.items())]
    return {'marginal_entry_differences.csv':marginal.count,'stop_cost_attribution.csv':buckets.count,'equivalence_groups.csv':write_csv(work/'equivalence_groups.csv',eq)}

def self_checks():
    checks=[]
    def check(ok,name):
        if not ok:raise AssertionError(name)
        checks.append(dict(check=name,status='PASS',evidence='SYNTHETIC_SOFTWARE_ONLY'))
    configs,_=make_configs();check(len(configs)==348 and len({c['config_id'] for c in configs})==348,'Full predeclared grid')
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
    check(f['LONG_SIGNAL_LOW'][200]['previous_low_20']==.999,'Prior structure excludes signal low')
    row=dict(f['LONG_SIGNAL_LOW'][200]);row.update(body_atr=.75,range_atr=1.);row['abs_distance_atr_60']=.25
    c=next(c for c in configs if c['family']=='MATRIX' and c['lookback']==60 and c['distance_max_atr']==.25 and c['body_min_atr']==.75 and c['range_min_atr']==1.)
    fixture={'LONG_SIGNAL_LOW':{200:row}};check(selected_indices(c,fixture)==[200],'Inclusive structure/body/range thresholds')
    row['abs_distance_atr_60']=.250001;check(not selected_indices(c,fixture),'Structure above maximum rejected')
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
    n,_=neighbour_ids(c,configs);check(len(n)==8,'Interior four-axis neighbours')
    check(stats([])['total_r']==0 and stats([2.,-1.,-1.,-1.,2.])['max_closed_dd_r']==-3.,'Empty stats and chronological DD')
    return checks

def run_job():
    global RUN_CLOCK,JOB_LOCK
    RUN_CLOCK=time.monotonic();OUT.mkdir(parents=True,exist_ok=True);work=Path(tempfile.mkdtemp(prefix='working-',dir=OUT))
    manifest=dict(version=VERSION,runner_sha256=code_hash(),pair=PAIR,side=SIDE,timeframe=TIMEFRAME,complete=False,status='RUNNING',
        dataset_kind='ACTUAL_PINNED_NATIVE_OANDA_MID',requested_start=iso(START),requested_end_exclusive=iso(END),rr=RR,cost_ticks=COSTS,
        models=MODELS,primary_model='STOP_FIRST_GAP_STRESS',stop_definition='SIGNAL_LOW_MINUS_1_PIP',
        research_grid=dict(lookbacks=LOOKBACKS,distances=DISTANCES,bodies=BODY_FLOORS,ranges=RANGE_FLOORS,
            scan_distances=SCAN_DISTANCES,scan_bodies=SCAN_BODIES,scan_ranges=SCAN_RANGES),
        portfolio_target='PORTFOLIO32_2026_10_05_EURCHF_H1_SHORT_PRIMARY_RR3P50_V1; admission deferred',orders_supported=False,trading_enabled=False)
    try:
        (work/'protocol.md').write_text(PROTOCOL);shutil.copyfile(__file__,work/'runner_source.py')
        write_csv(work/'software_checks.csv',self_checks())
        set_status(state='validating',progress=2,message='Validating full frozen source, aggregation and long signal controls')
        bars,volumes,h1,hvolumes=load_frozen_history(work);crosscheck_history(work,bars,volumes,h1,hvolumes)
        configs,memberships=make_configs();features=make_features(bars)
        controls=hard_controls(bars,features,configs);write_csv(work/'hard_controls.csv',controls)
        if any(r['status']!='PASS' for r in controls):raise RuntimeError('Independent long feature controls failed')
        set_status(state='building_paths',progress=25,message='Building long first-touch paths and independent raw-control baseline')
        paths=build_paths(bars,features,configs);baseline_parity(work,bars,features,paths)
        write_inputs(work,bars,features,paths,configs,memberships,manifest['dataset_kind'],volumes);(work/'README.md').write_text(RESULT_README)
        _,counts=analyze(work,bars,features,paths,configs)
        manifest.update(complete=True,status='COMPLETE',source_sha256=source_hash(bars),source_candles=len(bars),source_h1_sha256=source_hash(h1),
            source_controls='PASS',hard_controls='PASS',baseline_full_ledger_parity='PASS',software_controls='PASS',
            configurations=len(configs),cases=len(configs)*6,protocol_sha256=sha(PROTOCOL.encode()),output_row_counts=counts,
            completed_at=iso(datetime.now(UTC)),elapsed_seconds=round(time.monotonic()-RUN_CLOCK,1))
        write_json(work/'run_manifest.json',manifest);set_status(state='packaging',progress=96,message='All2088 cases complete; packaging results')
        package(work,True);set_status(state='complete',progress=100,message='EURCHF M15 LONG Pass1 complete; download results ZIP',
            configurations=348,cases=2088,result_path='/results',result_bytes=(OUT/RESULT_NAME).stat().st_size)
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
                        if cid in PARENT_IDS:
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

if __name__=='__main__':
    raise SystemExit(main())
