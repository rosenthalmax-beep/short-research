"""Native MID history, compact bars and causal indicator preparation."""
import bisect, hashlib, json, math, sqlite3, threading, time, zlib
from array import array
from collections import deque
from datetime import datetime,timedelta
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request,build_opener
import execution as e
import locked_rules as rules

GRANS={'M15':900,'H1':3600,'H4':14400,'D':86400}
START=e.dt('2005-01-01T00:00:00Z')
END=e.dt('2026-09-20T18:29:00Z')

def align(t,gran):
    if gran in ('M15','H1'):return e.floor_time(t,GRANS[gran])
    local=t.astimezone(e.NY)
    base=local.replace(hour=17,minute=0,second=0,microsecond=0)
    if local<base:base-=timedelta(days=1)
    if gran=='H4':base+=timedelta(hours=4*int((local-base).total_seconds()//14400))
    return base.astimezone(e.UTC)

def advance(t,gran,n):
    if gran=='D':return (t.astimezone(e.NY)+timedelta(days=n)).astimezone(e.UTC)
    # H4 is day-aligned to NY17. Advance in New York wall time:
    # adding UTC hours and flooring can return the same bar at autumn DST.
    if gran=='H4':return align((t.astimezone(e.NY)+timedelta(hours=4*n)).astimezone(e.UTC),gran)
    return align(t+timedelta(seconds=n*GRANS[gran]),gran)

def bar_end(t,gran):
    if gran=='H4':return advance(t,gran,1)
    return (t.astimezone(e.NY)+timedelta(days=1)).astimezone(e.UTC) if gran=='D' else t+timedelta(seconds=GRANS[gran])

class MidReader(e.CandleReader):
    def get(self,ident):
        pair,gran=ident['pair'],ident['granularity']
        e.require(pair in e.PAIRS and gran in GRANS and ident['price']=='M','MID candle route only')
        e.require(ident['broker']==self.broker and self.broker=='https://api-fxtrade.oanda.com','Broker mismatch')
        e.require(not self.fatal,'Reader stopped after authentication failure')
        params={'from':ident['start'],'to':ident['end'],'granularity':gran,'price':'M','smooth':'false','includeFirst':'true',
                'dailyAlignment':'17','alignmentTimezone':'America/New_York'}
        e.require(e.dt(ident['start'])<e.dt(ident['end'])<=END,'Historical request outside fixed horizon')
        url=self.broker+'/v3/instruments/'+pair+'/candles?'+urlencode(params)
        for attempt in range(3):
            with self.lock:
                delay=max(0,self.next_at-time.monotonic());self.next_at=max(time.monotonic(),self.next_at)+self.spacing
            if delay:time.sleep(delay)
            req=Request(url,method='GET',headers={'Authorization':'Bearer '+self.token,'Accept':'application/json','Accept-Datetime-Format':'RFC3339','User-Agent':'P32_SIGNAL_ADMISSION_V1'})
            try:
                if self.transport:obj=self.transport(req)
                else:
                    with self.opener.open(req,timeout=45) as response:raw=response.read(16*1024*1024+1)
                    e.require(len(raw)<=16*1024*1024,'MID response too large');obj=json.loads(raw)
                with self.lock:self.receipts.append(dict(method='GET',pair=pair,granularity=gran,params=params,response_sha256=e.sha(e.canon(obj))))
                return obj
            except Exception as exc:
                code=exc.code if isinstance(exc,HTTPError) else None
                if code in (401,403):self.fatal=True
                with self.lock:self.receipts.append(dict(method='GET',pair=pair,granularity=gran,params=params,error='HTTP_'+str(code) if code else type(exc).__name__,attempt=attempt+1))
                if code in (400,401,403,404) or isinstance(exc,ValueError) or attempt==2:
                    raise e.CandleFetchError('MID candles: '+('HTTP '+str(code) if code else type(exc).__name__)) from None
                time.sleep(.5*(attempt+1))

class Bars:
    """Columns keep a full pair's history below typical hosting memory limits."""
    def __init__(self):
        self.times=array('q');self.columns={k:array('d') for k in ('open','high','low','close')};self.extremes={}
    def __len__(self):return len(self.times)
    def __getitem__(self,index):
        if isinstance(index,slice):return [self[i] for i in range(*index.indices(len(self)))]
        return dict(time=datetime.fromtimestamp(self.times[index],e.UTC),**{k:v[index] for k,v in self.columns.items()})
    def __iter__(self):
        for i in range(len(self)):yield self[i]
    def append(self,candle):
        t=int(e.dt(candle['time']).timestamp())
        e.require(not self.times or t>self.times[-1],'Duplicate/unsorted MID bars')
        self.times.append(t)
        for name,key in zip(('open','high','low','close'),'ohlc'):self.columns[name].append(float(candle['mid'][key]))
    def previous_extreme(self,index,lookback,field,want_max):
        if index<lookback:return None
        key=(lookback,field,want_max)
        if key not in self.extremes:
            values=self.columns[field];out=array('d',[float('nan')])*len(self);q=deque()
            for i,value in enumerate(values):
                while q and q[0]<i-lookback:q.popleft()
                if i>=lookback:out[i]=values[q[0]]
                while q and ((values[q[-1]]<=value) if want_max else (values[q[-1]]>=value)):q.pop()
                q.append(i)
            self.extremes[key]=out
        return self.extremes[key][index]

class Nullable:
    def __init__(self,values):self.values=values if isinstance(values,array) else array('d',(float('nan') if x is None else x for x in values))
    def __len__(self):return len(self.values)
    def __getitem__(self,index):
        if isinstance(index,slice):return [None if math.isnan(x) else x for x in self.values[index]]
        x=self.values[index];return None if math.isnan(x) else x
    def __iter__(self):
        for x in self.values:yield None if math.isnan(x) else x

def atr(bars,length=14,chf=False):
    n=len(bars);out=array('d',[float('nan')])*n
    if n<length+(1 if chf else 0):return Nullable(out)
    h,l,c=(bars.columns[k] for k in ('high','low','close'))
    tr=array('d',[h[0]-l[0]])
    tr.extend(max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1])) for i in range(1,n))
    seed=length if chf else length-1;out[seed]=sum(tr[1:length+1] if chf else tr[:length])/length
    for i in range(seed+1,n):out[i]=(out[i-1]*(length-1)+tr[i])/length
    return Nullable(out)

def sma(values,length):
    # Preserve the probe's rolling-add/subtract arithmetic, not a mathematically
    # equivalent resummation with different floating-point rounding.
    return Nullable(rules.m15_sma_optional(values,length))

class Htf:
    def __init__(self,bars,ratio=False):
        self.bars=bars;closes=bars.columns['close'];self.ema={n:Nullable(rules.ema_series(closes,n)) if len(closes)>=n else Nullable([None]*len(closes)) for n in (50,100,200)}
        self.atr=atr(bars);self.mean=sma(self.atr,50) if ratio else None
    def at(self,t):
        i=bisect.bisect_right(self.bars.times,int(t.timestamp()))-2
        if i<0:return None
        value=self.atr[i];mean=self.mean[i] if self.mean else None
        return dict(time=self.bars[i]['time'],complete_at=self.bars[i+1]['time'],close=self.bars.columns['close'][i],
                    **{'ema'+str(n):v[i] for n,v in self.ema.items()},atr14=value,atr_ratio50=value/mean if value is not None and mean is not None and mean>0 else None)

class ReadyDaily(list):
    def __init__(self,rows,ready):
        super().__init__(rows);self.ready=[r for r in rows if ready(r)];self.times=[r['time'] for r in self.ready]
    def at(self,t):
        i=bisect.bisect_left(self.times,rules.current_daily_start(t))-1
        return self.ready[i] if i>=0 else None

def prepare_long_daily(bars,config):
    rows=rules.build_daily_state(bars,config)
    return ReadyDaily(rows,lambda r:r['slow_ema'] is not None and (not config['require_daily_fast_above_slow'] or r['fast_ema'] is not None) and (config.get('daily_close_ema') is None or r['daily_close_ema'] is not None))

def prepare_short_daily(bars,config):
    return ReadyDaily(rules.build_short_daily_state(bars,config),lambda r:r['fast_ema'] is not None and r['slow_ema'] is not None and r['daily_atr'] is not None)

class HistoryStore:
    def __init__(self,directory,reader,progress=lambda **x:None):
        self.db=sqlite3.connect(str(Path(directory)/'signal_history.sqlite3'))
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('CREATE TABLE IF NOT EXISTS history (key TEXT PRIMARY KEY, identity TEXT, body BLOB, sha256 TEXT)');self.db.commit()
        self.reader=reader;self.progress=progress;self.downloads=0;self.hits=0;self.coverage=[]
    @staticmethod
    def validate(obj,ident):
        e.require(obj.get('instrument')==ident['pair'] and obj.get('granularity')==ident['granularity'],'MID response identity mismatch')
        e.require(isinstance(obj.get('candles'),list),'Missing MID candle array')
        start,end=e.dt(ident['start']),e.dt(ident['end']);gran=ident['granularity'];previous=None;rows=[]
        for c in obj['candles']:
            t=e.dt(c['time']);e.require(previous is None or t>previous,'Unsorted MID candles');previous=t
            if t<start:continue
            e.require(t<end and align(t,gran)==t,'MID bar outside/alignment mismatch')
            if c.get('complete') is not True or bar_end(t,gran)>end:continue
            p={k:e.dec(c['mid'][k]) for k in 'ohlc'}
            e.require(0<p['l']<=min(p['o'],p['c'])<=max(p['o'],p['c'])<=p['h'],'Invalid MID prices')
            rows.append(dict(time=e.stamp(t),complete=True,mid={k:float(v) for k,v in p.items()}))
        return rows
    def series(self,pair,gran):
        result=Bars();cursor=align(START,gran);end=align(END,gran)
        expected,missing,weekends=0,0,0
        while cursor<end:
            finish=min(end,advance(cursor,gran,4800))
            e.require(finish>cursor,'Native history cursor did not advance')
            self.progress(phase='DOWNLOAD_CONTINUOUS_HISTORY',pair=pair,granularity=gran,timeframe=gran,
                          bars_evaluated=None,bars_required=None,signals_found=None,
                          through_utc=e.stamp(cursor),native_bars=len(result),mid_downloads=self.downloads,mid_cache_hits=self.hits,
                          history_timing_fix='NY17_H4_DST_V1_2026_10_16',last_history_update_utc=e.stamp(datetime.now(e.UTC)))
            ident=dict(schema='MID_HISTORY_NY17_V1',broker='https://api-fxtrade.oanda.com',pair=pair,granularity=gran,start=e.stamp(cursor),end=e.stamp(finish),price='M',smooth=False,dailyAlignment=17,alignmentTimezone='America/New_York')
            key=e.sha(e.canon(ident));cached=self.db.execute('SELECT body,sha256 FROM history WHERE key=?',(key,)).fetchone()
            if cached:
                raw=zlib.decompress(cached[0]);e.require(e.sha(raw)==cached[1],'MID cache checksum mismatch');obj=json.loads(raw);rows=self.validate(obj,ident);self.hits+=1
            else:
                if self.reader is None:raise e.NeedData(ident)
                obj=self.reader.get(ident);rows=self.validate(obj,ident);raw=e.canon(dict(instrument=pair,granularity=gran,candles=rows))
                self.db.execute('INSERT INTO history VALUES(?,?,?,?)',(key,e.canon(ident).decode(),zlib.compress(raw,6),e.sha(raw)));self.db.commit();self.downloads+=1
            available={e.dt(c['time']) for c in rows};t=cursor
            while t<finish:
                expected+=1
                if t not in available:
                    if e.conventional_weekend(t):weekends+=1
                    else:missing+=1
                next_slot=advance(t,gran,1)
                e.require(next_slot>t,'Native history coverage cursor did not advance: '+pair+' '+gran+' '+e.stamp(t))
                t=next_slot
            for c in rows:result.append(c)
            cursor=finish
            self.progress(phase='DOWNLOAD_CONTINUOUS_HISTORY',pair=pair,granularity=gran,through_utc=e.stamp(cursor),native_bars=len(result),mid_downloads=self.downloads,mid_cache_hits=self.hits,last_history_update_utc=e.stamp(datetime.now(e.UTC)))
        self.coverage.append(dict(pair=pair,granularity=gran,observed_bars=len(result),expected_calendar_slots=expected,
                                  missing_nonweekend_slots=missing,assumed_weekend_slots=weekends,
                                  first_observed=e.stamp(result[0]['time']) if len(result) else None,last_observed=e.stamp(result[-1]['time']) if len(result) else None,
                                  start=e.stamp(align(START,gran)),end=e.stamp(end),interpretation='Missing slots can be scheduled closure/holidays/zero quotes or missing observations; no synthetic candles.'))
        return result
    def export(self,z):
        index=[]
        for key,encoded,compressed,checksum in self.db.execute('SELECT key,identity,body,sha256 FROM history ORDER BY key'):
            raw=zlib.decompress(compressed);e.require(e.sha(raw)==checksum,'MID export checksum mismatch')
            name='mid_history/'+key+'.json';z.writestr(name,raw);index.append(dict(json.loads(encoded),file=name,sha256=checksum))
        z.writestr('mid_history_index.json',e.canon(index))
    def index(self):
        return [dict(json.loads(encoded),key=key,sha256=checksum) for key,encoded,checksum in self.db.execute('SELECT key,identity,sha256 FROM history ORDER BY key')]
    def close(self):self.db.close()
