"""Qualification replay only. No accounts, orders, executor client or live imports."""
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from collections import Counter, defaultdict, OrderedDict
import bisect, csv, gzip, hashlib, io, json, math, os, shutil, threading, time, zipfile
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
import engine

ROOT=Path(__file__).resolve().parent
VERSION='P32_RECENT_LIVE_REPLAY_V1_2026_11_06'
LANES=('RAW_COUNT_FORMING','COMPLETED_COUNT')
UTC=timezone.utc
NY=ZoneInfo('America/New_York')
RESERVE=96*1024*1024
CACHE_CAP=64*1024*1024
GRAN_SECONDS={'M15':900,'H1':3600,'H4':14400}

def parse(s):
    d=datetime.fromisoformat(s.replace('Z','+00:00'))
    if d.tzinfo is None:raise ValueError('Timestamp must include a timezone')
    return d.astimezone(UTC)

def iso(d):return d.astimezone(UTC).isoformat().replace('+00:00','Z')
def sha(b):return hashlib.sha256(b).hexdigest()
def dumps(x):return json.dumps(x,sort_keys=True,indent=2,allow_nan=False)+'\n'

def atomic(path,data):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_suffix(path.suffix+'.tmp')
    tmp.write_bytes(data if isinstance(data,bytes) else data.encode())
    os.replace(tmp,path)

def verify_sources():
    m=json.loads((ROOT/'source_manifest.json').read_text())
    if sha((ROOT/'engine.py').read_bytes())!=m['engine_sha256']:raise ValueError('Extracted engine changed')
    if sha((ROOT/'reference/strategy_probe.py').read_bytes())!=m['probe_sha256']:raise ValueError('Reference probe changed')
    for name,expected in m['input_sha256'].items():
        if sha((ROOT/'inputs'/name).read_bytes())!=expected:raise ValueError('Input snapshot changed: '+name)
    return m

def closed_interval(start,end):
    p=start
    while p<end:
        n=p.astimezone(NY);minute=n.hour*60+n.minute;day=n.weekday()
        closed=(day==5 or (day==4 and minute>=1019) or (day==6 and minute<1025)
                or (day<4 and 1019<=minute<1025))
        if not closed:return False
        p=min(end,p.replace(second=0,microsecond=0)+timedelta(minutes=1))
    return True

def bar_end(bar,granularity):
    t=bar['time']
    if granularity=='D':return (t.astimezone(NY)+timedelta(days=1)).astimezone(UTC)
    return t+timedelta(seconds=GRAN_SECONDS[granularity])

def parse_native(raw):
    p=raw.get('mid')
    if not isinstance(p,dict):raise ValueError('Missing MID prices')
    t=parse(raw['time']);v={k:float(p[q]) for k,q in [('open','o'),('high','h'),('low','l'),('close','c')]}
    if not all(math.isfinite(x) and x>0 for x in v.values()):raise ValueError('Non-finite/non-positive candle')
    if v['low']>min(v['open'],v['close']) or v['high']<max(v['open'],v['close']):raise ValueError('Invalid candle OHLC')
    return dict(time=t,**v,complete=bool(raw.get('complete',False)),volume=int(raw.get('volume',0)))

def merge_bars(groups):
    out={}
    for group in groups:
        for b in group:
            t=b['time']
            if t in out and any(out[t][k]!=b[k] for k in ['open','high','low','close']):
                raise ValueError('Overlapping native candles disagree at '+iso(t))
            out[t]=b
    return [out[t] for t in sorted(out)]

class ReadOnlyCandles:
    """Only the literal instrument-candle GET endpoint is reachable here."""
    def __init__(self,cache,progress=lambda **k:None):
        self.cache=Path(cache);self.cache.mkdir(parents=True,exist_ok=True)
        self.progress=progress;self.downloads=0;self.hits=0;self.calls=0

    def query(self,pair,granularity,**bounds):
        registry=json.loads((ROOT/'inputs/registry.json').read_text())
        if pair not in {r['pair'] for r in registry} or granularity not in {'M15','H1','H4','D'}:
            raise ValueError('Unregistered candle request')
        params=dict(price='M',granularity=granularity,smooth='false',dailyAlignment=17,
                    alignmentTimezone='America/New_York',**bounds)
        key=sha(dumps(dict(pair=pair,params=params)).encode())
        path=self.cache/(key+'.json.gz')
        if path.exists():
            saved=json.loads(gzip.decompress(path.read_bytes()))
            body=saved['body']
            if saved['query_key']!=key or sha(dumps(body).encode())!=saved['body_sha256']:
                raise ValueError('Native candle-cache integrity failure')
            self.hits+=1
        else:
            if shutil.disk_usage(self.cache).free<RESERVE:raise OSError('Storage reserve reached; no files deleted')
            if sum(p.stat().st_size for p in self.cache.glob('*.gz'))>=CACHE_CAP:raise OSError('64 MiB candle-cache cap reached')
            token=os.getenv('OANDA_TOKEN','').strip()
            if not token:raise ValueError('Set OANDA_TOKEN on this separate research service')
            self.progress(phase='DOWNLOAD_NATIVE_CANDLES',pair=pair,granularity=granularity,
                          request_bounds=bounds,downloads=self.downloads,cache_hits=self.hits)
            url='https://api-fxtrade.oanda.com/v3/instruments/'+pair+'/candles?'+urlencode(params)
            self.calls+=1
            if self.calls>200:raise RuntimeError('Bounded download limit of 200 distinct requests reached')
            for attempt in range(3):
                try:
                    request=Request(url,headers={'Authorization':'Bearer '+token,'Accept':'application/json'},method='GET')
                    with urlopen(request,timeout=20) as response:body=json.load(response)
                    break
                except HTTPError as e:
                    if e.code not in {429,500,502,503,504} or attempt==2:raise RuntimeError('OANDA candle GET failed: HTTP '+str(e.code)) from None
                except (URLError,TimeoutError):
                    if attempt==2:raise RuntimeError('OANDA candle GET timed out/connection failed') from None
                time.sleep(attempt+1)
            packed=gzip.compress(dumps(dict(query_key=key,body=body,body_sha256=sha(dumps(body).encode()))).encode())
            if sum(p.stat().st_size for p in self.cache.glob('*.gz'))+len(packed)>CACHE_CAP:raise OSError('Candle cache would exceed 64 MiB')
            atomic(path,packed);self.downloads+=1
        if body.get('instrument')!=pair or body.get('granularity')!=granularity:raise ValueError('Unexpected native candle response')
        return [parse_native(x) for x in body.get('candles',[])]

    def history(self,pair,granularity,start,end,count):
        # One native count request seeds the EXACT live rolling-window length.
        # Continue with short native ranges; this never regenerates 20 years of MID.
        warm_to=start-timedelta(days=1)
        warm=self.query(pair,granularity,to=iso(warm_to),count=count+8)
        if len(warm)<count:raise ValueError(pair+' '+granularity+': insufficient native pre-start history')
        cursor=warm[-1]['time'];groups=[warm]
        target=end+timedelta(minutes=1)
        step=timedelta(days=7 if granularity=='M15' else 14)
        while cursor<target:
            stop=min(target, (cursor.astimezone(NY)+timedelta(days=14)).astimezone(UTC)
                     if granularity in {'D','H4'} else cursor+step)
            groups.append(self.query(pair,granularity,**{'from':iso(cursor),'to':iso(stop),'includeFirst':'true'}))
            cursor=stop
        bars=merge_bars(groups)
        if not any(bar_end(b,granularity)<=start for b in bars):raise ValueError('No causally completed warm-up candles')
        return bars

class CausalView:
    def __init__(self,data):
        self.data=data
        self.times={g:[x['time'] for x in b] for g,b in data.items()}
        self.ends={g:[bar_end(x,g) for x in b] for g,b in data.items()}
        self.asof=None;self.lane=LANES[0]

    def fetch(self,pair,granularity,count,complete_only=True):
        bars=self.data[granularity]
        n=bisect.bisect_right(self.times[granularity],self.asof)
        completed=bisect.bisect_right(self.ends[granularity],self.asof,0,n)
        complete=bars[max(0,completed-count):completed]
        if any(not b.get('complete',False) for b in complete):
            raise ValueError('Historical candle reported incomplete; cannot reconstruct this window')
        # A marker supplies only a causally known next open TIME. No final OHLC
        # from the forming HTF bar is allowed into an indicator or qualification.
        forming=bars[completed:n]
        marker=None
        if forming:
            marker=dict(time=forming[-1]['time'],open=math.nan,high=math.nan,
                        low=math.nan,close=math.nan,volume=0,complete=False)
        if self.lane=='RAW_COUNT_FORMING':
            complete=complete[-(count-1 if marker else count):]
        else:complete=complete[-count:]
        if len(complete)<count-1:raise ValueError('Rolling history shorter than live request')
        if complete_only:return complete
        return complete+([marker] if marker else [])

def memo_daily(original):
    cache=OrderedDict()
    def wrapped(bars,config):
        key=(id(config),len(bars),bars[0]['time'] if bars else None,bars[-1]['time'] if bars else None)
        if key not in cache:
            cache[key]=original(bars,config)
            if len(cache)>12:cache.popitem(last=False)
        return cache[key]
    return wrapped

def serialize_signal(payload,result,lane):
    p={k:v for k,v in payload.items() if k!='secret'}
    side=p['side'];risk=p['entry']-p['stop'] if side=='BUY' else p['stop']-p['entry']
    if risk<=0:raise ValueError('Qualified signal has invalid rounded geometry')
    p.update(lane=lane,signal_close_utc=p['timestamp'],
             target=round(p['entry']+(1 if side=='BUY' else -1)*p['rr']*risk,
                          3 if p['pair'].endswith('JPY') else 5))
    return p

def eurchf_replay(data,start,end,lane):
    anchor=engine.EURCHF_FROZEN_ANCHOR
    pinned=[dict(b,time=parse(b['time'])) for b in anchor['bars']]
    by={b['time']:b for b in data['H1']}
    for p in pinned:
        found=by.get(p['time'])
        if found is None or any(found[k]!=p[k] for k in ['open','high','low','close']):
            raise ValueError('EUR/CHF pinned source overlap differs; no automatic reseeding')
    bars=list(pinned);value=anchor['atr14'];count=anchor['observed_count'];result=[];evaluations=0
    for b in data['H1']:
        if b['time']<=bars[-1]['time'] or bar_end(b,'H1')>end:continue
        if not b.get('complete',False):raise ValueError('Incomplete EUR/CHF source candle')
        prior=bars[-1]
        tr=max(b['high']-b['low'],abs(b['high']-prior['close']),abs(b['low']-prior['close']))
        value=(13*value+tr)/14;bars=(bars+[b])[-61:];count+=1
        close=bar_end(b,'H1')
        if close<start:continue
        r=engine.eurchf_signal_at_index(bars,[None]*60+[value],60,observed_index=count-1)
        if r is None:raise ValueError('EUR/CHF pinned signal not evaluable')
        evaluations+=1
        if r['qualified']:
            result.append(serialize_signal(engine.build_short_live_payload('EUR_CHF',r),r,lane))
    return result,evaluations

def replay_pair(pair,data,start,end,progress=lambda **k:None):
    registry=json.loads((ROOT/'inputs/registry.json').read_text())
    h1_ids=[r for r in registry if r['pair']==pair and r['timeframe']=='H1']
    results=[];counts={};view=CausalView(data)
    engine.fetch_candles_count=lambda instrument,granularity,count:view.fetch(instrument,granularity,count,True)
    engine.m15_fetch_recent=view.fetch
    # Cache exact daily-state computations only for identical immutable windows.
    long_builder=engine.build_daily_state;short_builder=engine.build_short_daily_state
    engine.build_daily_state=memo_daily(long_builder)
    engine.build_short_daily_state=memo_daily(short_builder)
    try:
        for lane in LANES:
            view.lane=lane
            if pair=='EUR_CHF':
                signals,n=eurchf_replay(data,start,end,lane);results.extend(signals)
                counts[lane]={engine.EURCHF_PRIMARY_ID:n}
                continue
            counts[lane]={r['strategy_id']:0 for r in registry if r['pair']==pair}
            for gran in ['H1','M15']:
                for i,b in enumerate(data[gran]):
                    close=bar_end(b,gran)
                    if close<start or close>end:continue
                    view.asof=close
                    if gran=='M15':
                        r=engine.evaluate_latest_m15_pair(pair)
                        if r['signal_close_utc']!=close:raise ValueError('M15 replay did not select expected candle')
                        if r.get('reason')=='ATR_NOT_READY':raise ValueError('M15 indicator not ready')
                        for reg in registry:
                            if reg['pair']==pair and reg['timeframe']=='M15':counts[lane][reg['strategy_id']]+=1
                        for signal in r['signals']:
                            results.append(serialize_signal(engine.build_m15_live_payload(signal),signal,lane))
                    else:
                        for reg in h1_ids:
                            r=engine.evaluate_latest(pair) if reg['side']=='BUY' else engine.evaluate_latest_short(pair)
                            if r['signal_close_utc']!=close:raise ValueError('H1 replay did not select expected candle')
                            counts[lane][reg['strategy_id']]+=1
                            if r['qualified']:
                                payload=engine.build_live_payload(pair,r) if reg['side']=='BUY' else engine.build_short_live_payload(pair,r)
                                results.append(serialize_signal(payload,r,lane))
                    if i%100==0:progress(phase='REPLAY_QUALIFICATIONS',pair=pair,lane=lane,granularity=gran,through_utc=iso(close))
    finally:
        engine.build_daily_state=long_builder;engine.build_short_daily_state=short_builder
    return dict(pair=pair,signals=results,evaluations=counts)

def gaps_for(pair,data,start,end):
    gaps=[]
    for gran,bars in data.items():
        for a,b in zip(bars,bars[1:]):
            expected=bar_end(a,gran)
            if b['time']>expected and b['time']>=start-timedelta(days=60) and expected<end:
                if not closed_interval(expected,b['time']):
                    gaps.append(dict(pair=pair,granularity=gran,from_utc=iso(expected),to_utc=iso(b['time']),kind='UNEXPLAINED_NATIVE_INTERVAL'))
    # Native bars absent throughout an open signal interval must not vanish from
    # the audit denominator just because only observed bars are replayed.
    for gran in ['H1','M15']:
        if gran not in data:continue
        seconds=GRAN_SECONDS[gran]
        t=datetime.fromtimestamp(math.ceil(start.timestamp()/seconds)*seconds,UTC)
        observed={bar_end(b,gran) for b in data[gran]}
        while t<=end:
            if t not in observed and not closed_interval(t-timedelta(seconds=seconds),t):
                gaps.append(dict(pair=pair,granularity=gran,from_utc=iso(t-timedelta(seconds=seconds)),to_utc=iso(t),kind='MISSING_EXPECTED_SIGNAL_BAR'))
            t+=timedelta(seconds=seconds)
    return gaps

def load_logs(start,end):
    def load(name):return json.loads((ROOT/'inputs'/name).read_text())
    known={};audit=load('signal_audit.json')['events']
    def keep(sid):
        try:return start<=datetime.fromtimestamp(int(sid.rsplit('-',1)[1])/1000,UTC)<=end
        except (ValueError,IndexError):return False
    for x in load('signal_history.json')['signals']:
        if keep(x['signal_id']):known[x['signal_id']]=dict(x,source='executor',entry=x['reference_entry'])
    for x in load('missed_outcomes.json')['setups']:
        sid=x['signal_id']
        if keep(sid) and sid not in known:
            known[sid]=dict(x['snapshot'],source='missed_outcomes',entry=x['snapshot']['reference_entry'],status=x['rejection_kind'])
    for x in audit:
        if x['event']=='QUALIFIED' and keep(x['signal_id']):
            row=known.setdefault(x['signal_id'],dict(x,source='probe_audit_only',status='QUALIFIED_ONLY'))
            row['qualified_logged']=True
    return known,audit

def compare(results,known):
    lanes={lane:{} for lane in LANES}
    for result in results:
        for x in result['signals']:
            lane=lanes[x['lane']]
            if x['signal_id'] in lane and lane[x['signal_id']]!=x:raise ValueError('Conflicting replay signal IDs')
            lane[x['signal_id']]=x
    rows=[]
    for sid in sorted(set(known)|set(lanes[LANES[0]])|set(lanes[LANES[1]])):
        a,b=(lanes[x].get(sid) for x in LANES);record=known.get(sid);ref=a or b or record
        delta=[]
        if a and b:
            for k in ['entry','stop','rr','strategy_id','side','pair','trigger_id']:
                if a.get(k)!=b.get(k):delta.append(k)
        if bool(a)!=bool(b) or delta:classification='COUNT_WINDOW_SENSITIVE'
        elif a and record:classification='MATCHED_RECORDED_SIGNAL'
        elif a:classification='UNLOGGED_QUALIFICATION_REQUIRES_REVIEW'
        else:classification='RECORDED_SIGNAL_NOT_REPRODUCED'
        mismatches=[]
        if record and (a or b):
            replay=a or b
            for k in ['entry','stop','rr','strategy_id','side','pair','trigger_id']:
                if record.get(k) is not None and replay.get(k) is not None and record[k]!=replay[k]:mismatches.append(k)
        rows.append(dict(signal_id=sid,pair=ref.get('pair'),strategy_id=ref.get('strategy_id'),
            classification=classification,recorded_status=record.get('status') if record else None,
            raw_count=a,completed_count=b,recorded=record,
            lane_field_differences=delta,recorded_field_differences=mismatches))
    return rows

def persistence(base):
    marker=base/'persistence_marker.json'
    if not marker.exists():atomic(marker,dumps(dict(created_utc=iso(datetime.now(UTC)),marker=os.urandom(16).hex())))
    best={'target':'/','filesystem':'unknown'}
    try:
        for line in Path('/proc/self/mountinfo').read_text().splitlines():
            left,right=line.split(' - ',1);mount=left.split()[4]
            if (str(base)==mount or str(base).startswith(mount.rstrip('/')+'/')) and len(mount)>=len(best['target']):
                best=dict(target=mount,filesystem=right.split()[0])
    except (OSError,ValueError):pass
    return dict(output_directory=str(base),containing_mount=best,separate_mount_detected=best['target']!='/',
                persistence_marker=json.loads(marker.read_text()),persistence_across_redeploy_not_proved=True)

class AuditRun:
    def __init__(self,base=None,client_factory=ReadOnlyCandles):
        self.manifest=verify_sources();self.start=parse(self.manifest['start_utc']);self.end=parse(self.manifest['end_utc'])
        self.base=Path(base or os.getenv('RECENT_REPLAY_OUTPUT_DIR','/data/p32_recent_live_replay')).resolve()
        self.base.mkdir(parents=True,exist_ok=True)
        signature=sha((dumps(self.manifest)+Path(__file__).read_text()).encode())
        self.directory=self.base/signature;self.directory.mkdir(exist_ok=True)
        self.status_path=self.directory/'status.json';self.result_path=self.directory/'results-summary.zip'
        self.client_factory=client_factory;self.lock=threading.Lock();self.thread=None;self.process_lock=None
        self.state=dict(version=VERSION,signature=signature,status='NOT_STARTED',phase='READY',
                        start_utc=iso(self.start),end_utc=iso(self.end),strategy_count=32,
                        pairs_required=9,pairs_completed=0,orders_supported=False,orders_sent=False,
                        full_portfolio_validated=False,scope='QUALIFIED_SIGNAL_RECONCILIATION_ONLY',
                        result_ready=self.result_path.exists(),result='/results-summary.zip',
                        persistence=persistence(self.base))
        if self.status_path.exists():self.state.update(json.loads(self.status_path.read_text()))
        if self.state['status'] in {'RUNNING','ERROR'}:self.state.update(status='INTERRUPTED',phase='RESUME_AVAILABLE')

    def update(self,**values):
        with self.lock:
            self.state.update(values,last_progress_utc=iso(datetime.now(UTC)))
            atomic(self.status_path,dumps(self.state))

    def start_once(self):
        with self.lock:
            if self.state['status'] in {'COMPLETE','ERROR'} or (self.thread and self.thread.is_alive()):return
            import fcntl
            handle=open(self.base/'worker.lock','a+')
            try:fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:handle.close();raise RuntimeError('Another replay worker holds this output directory')
            self.process_lock=handle
            self.thread=threading.Thread(target=self.worker,daemon=True,name='qualification-replay')
            self.thread.start()

    def worker(self):
        try:
            self.update(status='RUNNING',phase='VERIFY_AND_DOWNLOAD',error=None,result_ready=False)
            registry=json.loads((ROOT/'inputs/registry.json').read_text())
            client=self.client_factory(self.base/'native_cache',self.update)
            results=[];gaps=[]
            for i,pair in enumerate(sorted({r['pair'] for r in registry})):
                cp=self.directory/(pair+'.json')
                if cp.exists():
                    saved=json.loads(cp.read_text())
                    if saved['signature']!=self.state['signature'] or sha(dumps(saved['result']).encode())!=saved['result_sha256'] or sha(dumps(saved['gaps']).encode())!=saved['gaps_sha256']:
                        raise ValueError('Pair checkpoint integrity failure')
                    result=saved['result'];pgaps=saved['gaps']
                else:
                    streams={'H1':1000} if pair=='EUR_CHF' else {'H1':1000,'M15':2000 if pair=='AUD_JPY' else 260,'D':2500}
                    if pair in {'USD_JPY','USD_CAD'}:streams['H4']=1000
                    data={g:client.history(pair,g,self.start,self.end,count) for g,count in streams.items()}
                    pgaps=gaps_for(pair,data,self.start,self.end)
                    result=replay_pair(pair,data,self.start,self.end,self.update)
                    atomic(cp,dumps(dict(signature=self.state['signature'],result=result,gaps=pgaps,
                                         result_sha256=sha(dumps(result).encode()),gaps_sha256=sha(dumps(pgaps).encode()))))
                results.append(result);gaps.extend(pgaps)
                self.update(pairs_completed=i+1,phase='PAIR_COMPLETE',pair=pair,
                            qualifications_found=sum(len(r['signals']) for r in results))
            known,audit=load_logs(self.start,self.end)
            comparison=compare(results,known)
            summary=dict(version=VERSION,status='COMPLETE_CONDITIONAL',scope=self.state['scope'],
                start_utc=iso(self.start),end_utc=iso(self.end),strategy_count=32,
                probe_sha256=self.manifest['probe_sha256'],orders_supported=False,orders_sent=False,
                full_portfolio_validated=False,recorded_unique_signals=len(known),
                reconciliation_counts=dict(Counter(x['classification'] for x in comparison)),
                recorded_payload_mismatch_count=sum(bool(x['recorded_field_differences']) for x in comparison),
                native_gap_rows=len(gaps),
                evaluation_counts={r['pair']:r['evaluations'] for r in results},
                qualifications_by_lane={lane:sum(x['lane']==lane for r in results for x in r['signals']) for lane in LANES},
                strategies_evaluated_by_lane={lane:sum(n>0 for r in results for n in r['evaluations'][lane].values()) for lane in LANES},
                limitations=[
                    'Qualification audit, not a trade/account/financing simulation or return forecast.',
                    'Uses current supplied 32 rules over the fixed audit window; historical activation/settings are not reconstructed.',
                    'Native candles are retrieved now and may differ from the candles delivered live.',
                    'Both raw-count/forming and completed-count windows are checked; differences require investigation.',
                    'Unlogged qualifications are investigation candidates, not guaranteed missed executable trades.',
                    'Missing native bars, live outages and unavailable historical quote/gate states limit completeness.',
                    'EUR/CHF begins at its pinned ATR anchor with broker overlap verification; no reseeding.'])
            self.write_reports(summary,comparison,gaps,results)
            self.update(status='COMPLETE',phase='RESULT_READY',result_ready=True,summary=summary)
        except Exception as e:
            self.update(status='ERROR',phase='FAILED',error=str(e))
            self.write_error_archive()
        finally:
            if self.process_lock:
                self.process_lock.close();self.process_lock=None

    def write_reports(self,summary,comparison,gaps,results):
        atomic(self.directory/'summary.json',dumps(summary))
        atomic(self.directory/'comparison.json',dumps(comparison))
        atomic(self.directory/'native_coverage_gaps.json',dumps(gaps))
        atomic(self.directory/'qualified_signals.json',dumps([x for r in results for x in r['signals']]))
        stream=io.StringIO();fields=['signal_id','pair','strategy_id','classification','recorded_status','lane_field_differences','recorded_field_differences']
        writer=csv.DictWriter(stream,fieldnames=fields);writer.writeheader()
        for row in comparison:writer.writerow({k:json.dumps(row[k]) if isinstance(row[k],list) else row[k] for k in fields})
        atomic(self.directory/'comparison.csv',stream.getvalue())
        archive_tmp=self.result_path.with_suffix('.zip.tmp')
        with zipfile.ZipFile(archive_tmp,'w',zipfile.ZIP_DEFLATED) as z:
            for n in ['summary.json','comparison.json','native_coverage_gaps.json','qualified_signals.json','comparison.csv']:
                z.write(self.directory/n,n)
            z.write(ROOT/'source_manifest.json','source_manifest.json')
        if archive_tmp.stat().st_size>24*1024*1024:raise OSError('Result summary exceeds 24 MiB; preserved on disk')
        os.replace(archive_tmp,self.result_path)

    def write_error_archive(self):
        try:
            with zipfile.ZipFile(self.result_path,'w',zipfile.ZIP_DEFLATED) as z:
                z.write(self.status_path,'error_status.json');z.write(ROOT/'source_manifest.json','source_manifest.json')
            self.update(result_ready=True)
        except OSError:pass
