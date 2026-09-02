import os
import threading
import requests
import pandas as pd
from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta

app = Flask(__name__)

OANDA_TOKEN = os.getenv('OANDA_TOKEN')
OANDA_URL = 'https://api-fxtrade.oanda.com'
INSTRUMENT = 'EUR_GBP'
TICK_SIZE = 0.00001
STOP_BUFFER_TICKS = 10
BACKTEST_SLIPPAGE_TICKS = 5
REWARD_RISK = 3.00
MIN_BODY_RATIO = 1.00

STRUCTURE_LOOKBACKS = [80, 90, 100]
MAX_DISTANCE_ATR_VALUES = [0.05, 0.075, 0.10]
MIN_RANGE_ATR_VALUES = [1.00, 1.10, 1.20]
MAX_CLOSE_LOCATION_VALUES = [0.175, 0.20, 0.225]
ROBUST_MOM12_VALUES = [0.15, 0.25, 0.35]
ROBUST_MOM48_VALUES = [0.40, 0.50, 0.60]
ROBUST_STOP_CAP_VALUES = [2.25, 2.50, 2.75]
HIGH_PF_MOM48_VALUES = [0.75, 1.00, 1.25]
HIGH_PF_UPPER_WICK_VALUES = [0.05, 0.10, 0.15]

H1_CHUNK_DAYS = 180
RESEARCH_FROM = datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc)
RESEARCH_TO = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
H1_WARMUP_DAYS = 650
OUTPUT_FILE = 'eurgbp_short_winner_neighbourhood.csv'

ERAS = [
    ('2002_2009', datetime(2002,5,6,20,0,tzinfo=timezone.utc), datetime(2010,1,1,0,0,tzinfo=timezone.utc)),
    ('2010_2017', datetime(2010,1,1,0,0,tzinfo=timezone.utc), datetime(2018,1,1,0,0,tzinfo=timezone.utc)),
    ('2018_2023', datetime(2018,1,1,0,0,tzinfo=timezone.utc), datetime(2024,1,1,0,0,tzinfo=timezone.utc)),
    ('2024_present', datetime(2024,1,1,0,0,tzinfo=timezone.utc), None),
]

ROBUST_TESTS = len(STRUCTURE_LOOKBACKS)*len(MAX_DISTANCE_ATR_VALUES)*len(MIN_RANGE_ATR_VALUES)*len(MAX_CLOSE_LOCATION_VALUES)*len(ROBUST_MOM12_VALUES)*len(ROBUST_MOM48_VALUES)*len(ROBUST_STOP_CAP_VALUES)
HIGH_PF_TESTS = len(STRUCTURE_LOOKBACKS)*len(MAX_DISTANCE_ATR_VALUES)*len(MIN_RANGE_ATR_VALUES)*len(MAX_CLOSE_LOCATION_VALUES)*len(HIGH_PF_MOM48_VALUES)*len(HIGH_PF_UPPER_WICK_VALUES)
TOTAL_TESTS = ROBUST_TESTS + HIGH_PF_TESTS
STATUS = {'state':'not_started','message':'Research has not started','service':'EURGBP Short Winner Neighbourhood','instrument':INSTRUMENT,'research_from':RESEARCH_FROM.isoformat(),'research_to':RESEARCH_TO.isoformat(),'reward_risk':REWARD_RISK,'robust_branch_tests':ROBUST_TESTS,'high_pf_branch_tests':HIGH_PF_TESTS,'total_tests':TOTAL_TESTS,'completed_tests':0,'rows_saved':0,'output_file':None}

def headers():
    if not OANDA_TOKEN: raise RuntimeError('OANDA_TOKEN is not configured')
    return {'Authorization': f'Bearer {OANDA_TOKEN}'}

def iso_utc(dt): return dt.astimezone(timezone.utc).isoformat().replace('+00:00','Z')

def oanda_get(path, params):
    r=requests.get(OANDA_URL+path,headers=headers(),params=params,timeout=30)
    if not r.ok: raise RuntimeError(f'OANDA {r.status_code}: {r.text[:500]}')
    return r.json()

def parse_candle(raw):
    if not raw.get('complete',False) or not raw.get('mid'): return None
    m=raw['mid']
    return {'time':datetime.fromisoformat(raw['time'].replace('Z','+00:00')),'open':float(m['o']),'high':float(m['h']),'low':float(m['l']),'close':float(m['c'])}

def fetch_range(instrument,granularity,start,end):
    d=oanda_get(f'/v3/instruments/{instrument}/candles',{'price':'M','granularity':granularity,'from':iso_utc(start),'to':iso_utc(end),'smooth':'false','includeFirst':'true'})
    return [c for x in d.get('candles',[]) if (c:=parse_candle(x)) is not None]

def fetch_chunked_history(instrument,granularity,start,end):
    out={}; cur=start
    while cur<end:
        ce=min(cur+timedelta(days=H1_CHUNK_DAYS),end)
        print(f'Fetching {granularity}: {cur.date()} -> {ce.date()}',flush=True)
        for c in fetch_range(instrument,granularity,cur,ce): out[c['time']]=c
        cur=ce
    return sorted(out.values(),key=lambda x:x['time'])

def true_ranges(c):
    out=[]
    for i,x in enumerate(c):
        if i==0: tr=x['high']-x['low']
        else:
            pc=c[i-1]['close']; tr=max(x['high']-x['low'],abs(x['high']-pc),abs(x['low']-pc))
        out.append(tr)
    return out

def rma_series(values,length):
    out=[None]*len(values)
    if len(values)<length:return out
    prev=sum(values[:length])/length; out[length-1]=prev
    for i in range(length,len(values)):
        prev=(prev*(length-1)+values[i])/length; out[i]=prev
    return out

def atr_series(c,length=14): return rma_series(true_ranges(c),length)

def build_candidates(h1,atr14):
    out=[]; max_lb=max(max(STRUCTURE_LOOKBACKS),48)
    for i in range(max_lb,len(h1)):
        s=h1[i]
        if s['time']<RESEARCH_FROM: continue
        if s['time']>=RESEARCH_TO: break
        p=h1[i-1]; atr=atr14[i]
        if atr is None or atr<=0: continue
        pb=abs(p['close']-p['open']); cb=abs(s['close']-s['open']); rng=s['high']-s['low']
        if pb<=0 or cb<=0 or rng<=0: continue
        if not (p['close']>p['open'] and s['close']<s['open'] and s['open']>=p['close'] and s['close']<=p['open']): continue
        if cb/pb<MIN_BODY_RATIO: continue
        structure={}
        for lb in STRUCTURE_LOOKBACKS:
            ph=max(x['high'] for x in h1[i-lb:i]); structure[lb]=(ph-s['high'])/atr
        uw=max(0.0,s['high']-max(s['open'],s['close']))
        stop=s['high']+STOP_BUFFER_TICKS*TICK_SIZE
        out.append({'index':i,'time':s['time'],'range_atr':rng/atr,'close_location':(s['close']-s['low'])/rng,'structure':structure,'momentum_12':(s['close']-h1[i-12]['close'])/atr,'momentum_48':(s['close']-h1[i-48]['close'])/atr,'upper_wick_body':uw/cb,'stop_size_atr':(stop-s['close'])/atr})
    return out

EXIT_CACHE={}
def calculate_trade_exit(h1,si):
    if si in EXIT_CACHE:return EXIT_CACHE[si]
    s=h1[si]; ref=s['close']; fill=ref-BACKTEST_SLIPPAGE_TICKS*TICK_SIZE; stop=s['high']+STOP_BUFFER_TICKS*TICK_SIZE; rrisk=stop-ref
    if rrisk<=0: raise RuntimeError('Invalid short reference risk')
    target=ref-rrisk*REWARD_RISK; arisk=stop-fill
    for i in range(si+1,len(h1)):
        c=h1[i]
        if c['time']>=RESEARCH_TO: break
        sh=c['high']>=stop; th=c['low']<=target
        if not(sh or th): continue
        if sh and th:
            if abs(c['high']-c['open'])<abs(c['open']-c['low']): ep=stop; er='STOP'
            else: ep=target; er='TARGET'
        elif sh: ep=stop; er='STOP'
        else: ep=target; er='TARGET'
        r={'status':'CLOSED','signal_index':si,'signal_time':s['time'],'exit_index':i,'exit_time':c['time'],'exit_reason':er,'result_r':(fill-ep)/arisk}; EXIT_CACHE[si]=r; return r
    r={'status':'OPEN','signal_index':si,'signal_time':s['time'],'exit_index':None,'exit_time':None,'exit_reason':None,'result_r':None}; EXIT_CACHE[si]=r; return r

def simulate(h1,eligible):
    trades=[]; exit_i=-1; ignored=0; still=False
    for c in eligible:
        si=c['index']
        if si<exit_i: ignored+=1; continue
        t=calculate_trade_exit(h1,si)
        if t['status']=='OPEN': still=True; break
        trades.append(t); exit_i=t['exit_index']
    return trades,ignored,still

def stats_for_trades(trades,start=None,end=None):
    f=[t for t in trades if (start is None or t['signal_time']>=start) and (end is None or t['signal_time']<end)]
    if not f:return {'trades':0,'winners':0,'losers':0,'win_rate':0.0,'profit_factor':0.0,'total_r':0.0,'expectancy_r':0.0,'max_drawdown_r':0.0,'longest_loss_streak':0}
    rs=[t['result_r'] for t in f]; w=[x for x in rs if x>0]; l=[x for x in rs if x<0]; gp=sum(w); gl=abs(sum(l)); pf=gp/gl if gl>0 else (999.0 if gp>0 else 0.0)
    eq=peak=0.0; dd=0.0; cur=longest=0
    for x in rs:
        eq+=x; peak=max(peak,eq); dd=min(dd,eq-peak)
        if x<0: cur+=1; longest=max(longest,cur)
        else: cur=0
    total=sum(rs)
    return {'trades':len(rs),'winners':len(w),'losers':len(l),'win_rate':round(len(w)/len(rs)*100,2),'profit_factor':round(pf,3),'total_r':round(total,2),'expectancy_r':round(total/len(rs),3),'max_drawdown_r':round(dd,2),'longest_loss_streak':longest}

def make_row(branch,lb,dist,rng,cl,m12,m48,wick,stopcap,raw,eligible,trades,ignored,still,years):
    full=stats_for_trades(trades)
    row={'branch':branch,'structure_lookback':lb,'max_distance_atr':dist,'min_range_atr':rng,'max_close_location':cl,'min_up_momentum_12h_atr':m12,'min_up_momentum_48h_atr':m48,'min_upper_wick_body':wick,'max_stop_size_atr':stopcap,'raw_signals':len(raw),'eligible_signals':len(eligible),'signal_retention_pct':round(len(eligible)/len(raw)*100,2) if raw else 0.0,'ignored_due_to_open_trade':ignored,'still_open_at_end':still,'trades':full['trades'],'trades_per_year':round(full['trades']/years,2),'winners':full['winners'],'losers':full['losers'],'win_rate':full['win_rate'],'profit_factor':full['profit_factor'],'total_r':full['total_r'],'expectancy_r':full['expectancy_r'],'max_drawdown_r':full['max_drawdown_r'],'longest_loss_streak':full['longest_loss_streak']}
    profitable=0; minpf=None; minexp=None
    for name,st,en in ERAS:
        e=stats_for_trades(trades,st,en)
        row[f'{name}_trades']=e['trades']; row[f'{name}_pf']=e['profit_factor']; row[f'{name}_r']=e['total_r']; row[f'{name}_expectancy']=e['expectancy_r']
        if e['trades']>=5:
            if e['total_r']>0: profitable+=1
            minpf=e['profit_factor'] if minpf is None else min(minpf,e['profit_factor'])
            minexp=e['expectancy_r'] if minexp is None else min(minexp,e['expectancy_r'])
    row['profitable_eras_with_5_plus_trades']=profitable; row['minimum_era_pf_5_plus']=minpf; row['minimum_era_expectancy_5_plus']=minexp; row['all_four_eras_profitable']=profitable>=4; row['adequate_90_trades']=full['trades']>=90; row['frequency_4py']=full['trades']/years>=4.0; row['worst_era_pf_120']=minpf is not None and minpf>=1.20; row['worst_era_pf_130']=minpf is not None and minpf>=1.30; row['worst_era_pf_140']=minpf is not None and minpf>=1.40; row['pf_160']=full['profit_factor']>=1.60; row['pf_170']=full['profit_factor']>=1.70; row['pf_180']=full['profit_factor']>=1.80; row['dd_better_than_8r']=full['max_drawdown_r']>=-8.0; row['annual_r_linear']=round(full['expectancy_r']*(full['trades']/years),3)
    return row

def run_research():
    global STATUS
    try:
        STATUS.update({'state':'fetching_data','message':'Fetching EUR/GBP OANDA H1 history'})
        h1=fetch_chunked_history(INSTRUMENT,'H1',RESEARCH_FROM-timedelta(days=H1_WARMUP_DAYS),RESEARCH_TO)
        if not h1: raise RuntimeError('No EUR/GBP H1 candles returned')
        STATUS.update({'state':'precomputing','message':'Building ATR14 and candidate features'})
        atr14=atr_series(h1,14); raw=build_candidates(h1,atr14); STATUS['raw_bearish_engulfing_signals']=len(raw)
        years=(RESEARCH_TO-RESEARCH_FROM).total_seconds()/(365.2425*24*60*60)
        STATUS.update({'state':'running','message':'Running winner-neighbourhood sweep'})
        rows=[]; done=0
        for lb in STRUCTURE_LOOKBACKS:
          for dist in MAX_DISTANCE_ATR_VALUES:
           for rng in MIN_RANGE_ATR_VALUES:
            for cl in MAX_CLOSE_LOCATION_VALUES:
             for m12 in ROBUST_MOM12_VALUES:
              for m48 in ROBUST_MOM48_VALUES:
               for stopcap in ROBUST_STOP_CAP_VALUES:
                eligible=[c for c in raw if c['structure'][lb]<=dist and c['range_atr']>=rng and c['close_location']<=cl and c['momentum_12']>=m12 and c['momentum_48']>=m48 and c['stop_size_atr']<=stopcap]
                trades,ignored,still=simulate(h1,eligible); rows.append(make_row('ROBUST',lb,dist,rng,cl,m12,m48,None,stopcap,raw,eligible,trades,ignored,still,years)); done+=1; STATUS['completed_tests']=done
                if done%100==0: print(f'{done}/{TOTAL_TESTS}',flush=True)
        for lb in STRUCTURE_LOOKBACKS:
          for dist in MAX_DISTANCE_ATR_VALUES:
           for rng in MIN_RANGE_ATR_VALUES:
            for cl in MAX_CLOSE_LOCATION_VALUES:
             for m48 in HIGH_PF_MOM48_VALUES:
              for wick in HIGH_PF_UPPER_WICK_VALUES:
               eligible=[c for c in raw if c['structure'][lb]<=dist and c['range_atr']>=rng and c['close_location']<=cl and c['momentum_48']>=m48 and c['upper_wick_body']>=wick]
               trades,ignored,still=simulate(h1,eligible); rows.append(make_row('HIGH_PF',lb,dist,rng,cl,None,m48,wick,None,raw,eligible,trades,ignored,still,years)); done+=1; STATUS['completed_tests']=done
               if done%100==0 or done==TOTAL_TESTS: print(f'{done}/{TOTAL_TESTS}',flush=True)
        df=pd.DataFrame(rows)
        df=df.sort_values(by=['all_four_eras_profitable','adequate_90_trades','frequency_4py','worst_era_pf_140','worst_era_pf_130','worst_era_pf_120','dd_better_than_8r','pf_180','pf_170','pf_160','minimum_era_pf_5_plus','profit_factor','expectancy_r','annual_r_linear','trades'],ascending=[False]*15)
        df.to_csv(OUTPUT_FILE,index=False)
        STATUS.update({'state':'complete','message':'EUR/GBP winner-neighbourhood completed successfully','completed_tests':TOTAL_TESTS,'rows_saved':len(df),'all_four_eras_profitable_count':int(df['all_four_eras_profitable'].sum()),'all_four_eras_90_trades_count':int((df['all_four_eras_profitable']&df['adequate_90_trades']).sum()),'output_file':OUTPUT_FILE})
    except Exception as e:
        STATUS.update({'state':'error','message':str(e)}); print('ERROR:',e,flush=True)

@app.route('/')
def home(): return jsonify({'service':'EURGBP Short Winner Neighbourhood','status':STATUS,'instrument':INSTRUMENT,'direction':'SHORT','reward_risk':REWARD_RISK,'trading_enabled':False,'orders_supported':False,'executor_connected':False,'download':'/download'})
@app.route('/status')
def status(): return jsonify(STATUS)
@app.route('/download')
def download():
    if not os.path.exists(OUTPUT_FILE): return jsonify({'status':'not_ready','message':'EUR/GBP winner-neighbourhood CSV is not ready yet'}),404
    return send_file(OUTPUT_FILE,as_attachment=True,download_name=OUTPUT_FILE)

if __name__=='__main__':
    threading.Thread(target=run_research,name='eurgbp-short-winner-neighbourhood',daemon=True).start()
    app.run(host='0.0.0.0',port=int(os.getenv('PORT',5000)),debug=False)
