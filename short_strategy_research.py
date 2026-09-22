#!/usr/bin/env python3
"""AUD/USD M15 LONG: FROZEN 0.05 versus 0.10 penetration, exact raw-pair 27->28 test.

No optimisation, live orders, executor changes, or other strategy modifications.

Historical portfolio snapshot: 2026-09-20 18:29 UTC, 3029 trades, 27 IDs.
AUD/USD incumbent raw-entry stream from frozen #27 26->27 historical gate (401 signals).
Historical candidate candle/parity snapshot: 2026-09-22 21:30 UTC.

RISK: Uses 1% of then-realised equity for candidate & other strategies except
EUR_JPY_M15_SHORT = 0.75%; no aggregate currency cap. Same-direction stacking
across strategy IDs is allowed; opposite AUD/USD positions are blocked. A
blocked entry DOES NOT consume its strategy's pyramiding-0 interval. All
signals are processed chronologically and exits release on candle completion.

OANDA midpoint candles, BUY signal reference=close, 10 tick stop buffer,
RR3.5 target from reference risk; 1-pip adverse fill (2-pip standalone stress).
Stop/target intrabar ambiguity uses the archived closer-to-open convention.
This is historical in-sample research, NOT live-expected returns.
"""
from __future__ import annotations
import base64,bisect,csv,datetime as dt,hashlib,io,json,math,os,statistics,sys,threading,time,traceback,zipfile,zlib
from collections import defaultdict, deque
from pathlib import Path
import numpy as np
import requests
try:
 from flask import Flask,jsonify,send_file
except ImportError:
 Flask=None

INSTRUMENT='AUD_USD'; SID='AUD_USD_M15_LONG_PROPOSED28'
CUTOFF='2026-09-20T18:29:00Z'
PARITY_CANDLE_CUTOFF='2026-09-22T21:30:00Z'
BASELINE_EXPECT={'trades':3029,'strategies':27,'balance':1731448888.7485507,
 'closed_dd':-17.089509091188603,'floor_dd':-17.84461250071128,
 'open_positions':6,'max_open_risk_pct':5.903440932560884,
 'weighted_r':1763.8084030796124,'cagr':115.53348121349032}
TICK=.00001;PIP=.0001;RR=3.5;STOP_TICKS=10
FIRST_M15='2002-05-06T20:45:00Z';FIRST_H1='2002-05-06T20:00:00Z'
API=os.getenv('OANDA_API_URL','https://api-fxtrade.oanda.com').rstrip('/')
TOKEN=os.getenv('OANDA_TOKEN','')
OUT=Path(os.getenv('AUD28_OUTPUT_DIR','/tmp/audusd_m15_long_27to28'))
BUNDLE=OUT/'AUDUSD_M15_LONG_27_TO_28_EXACT_PORTFOLIO_RESULTS.zip'
STATE={'state':'idle','progress':0,'message':'Not started', 'orders_supported':False,
 'trading_enabled':False,'portfolio_cutoff':CUTOFF,'historical_snapshot':True}
LOCK=threading.Lock()

def status(state,progress,message):
 with LOCK:STATE.update(state=state,progress=progress,message=message)
def parse(s):return dt.datetime.fromisoformat(s.replace('Z','+00:00'))
def iso(s):return s.astimezone(dt.timezone.utc).isoformat().replace('+00:00','Z')
def strict(a,b,label,atol=1e-7):
 if not math.isclose(float(a),float(b),abs_tol=atol,rel_tol=1e-12):
  raise RuntimeError(f'PARITY {label}: actual={a} expected={b}')
def identity(t):return(t['sid'],t['pair'],t['side'],t['tf'],t['signal'],t['entry'],t['exit'],round(float(t['r']),9),float(t['rr']))
def export_csv(path,rows):
 rows=list(rows);fields=list(dict.fromkeys(k for r in rows for k in r)) or ['no_rows']
 with open(path,'w',encoding='utf-8',newline='') as f:
  w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');w.writeheader();w.writerows(rows)
def embed_load():
 raw=zlib.decompress(base64.b85decode(SOURCE_B85));digest=hashlib.sha256(raw).hexdigest()
 if digest!=SOURCE_SHA256:raise RuntimeError('Embedded archive SHA256 mismatch')
 data=json.loads(raw)
 if len(data['baseline'])!=3029 or len(data['raw_pair'])!=401 or len(data['ledger_010'])!=283:
  raise RuntimeError('Embedded snapshot/ledger count mismatch')
 return data,digest

def req_chunk(gran,start,end):
 if not TOKEN:raise RuntimeError('Set OANDA_TOKEN on research service; no orders are used')
 params={'price':'M','granularity':gran,'smooth':'false','from':iso(start),'to':iso(end),'includeFirst':'true'}
 resp=requests.get(f'{API}/v3/instruments/{INSTRUMENT}/candles',params=params,
    headers={'Authorization':'Bearer '+TOKEN.strip()},timeout=90)
 if resp.status_code>=400:raise RuntimeError(f'OANDA {resp.status_code} {resp.text[:450]} {gran} {iso(start)}')
 out=[]
 for x in resp.json().get('candles',[]):
  if not x.get('complete'):continue
  md=x['mid'];out.append({'time':parse(x['time']),'open':float(md['o']),
       'high':float(md['h']),'low':float(md['l']),'close':float(md['c'])})
 return out

def fetch(gran,start,end,days):
 cur=start;all_bars={};n=0
 while cur<end:
  nxt=min(cur+dt.timedelta(days=days),end);n+=1
  if n%10==1:status('fetch',10 if gran=='M15' else 32,
      f'{gran} historical chunk {n}: {iso(cur)} to {iso(nxt)}')
  for x in req_chunk(gran,cur,nxt):
   if x['time']<=parse(PARITY_CANDLE_CUTOFF):all_bars[x['time']]=x
  cur=nxt
  if n%10==0:time.sleep(.03)
 return [all_bars[x] for x in sorted(all_bars)]

def atr14(bars):
 n=len(bars);tr=np.full(n,np.nan);out=np.full(n,np.nan)
 for i,x in enumerate(bars):
  prev=bars[i-1]['close'] if i else x['close'];tr[i]=max(x['high']-x['low'],abs(x['high']-prev),abs(x['low']-prev))
 if n>=14:
  out[13]=float(np.mean(tr[:14]))
  for i in range(14,n):out[i]=(out[i-1]*13+tr[i])/14
 return out

def prev_min(values,lookback):
 out=np.full(len(values),np.nan);q=deque()
 for i in range(len(values)):
  while q and q[0]<i-lookback:q.popleft()
  if i:
   j=i-1
   while q and values[q[-1]]>=values[j]:q.pop()
   q.append(j)
  if i>=lookback:out[i]=values[q[0]]
 return out

def candidate_indices(m15,atr,penetration):
 o=np.fromiter((x['open'] for x in m15),float,count=len(m15))
 c=np.fromiter((x['close'] for x in m15),float,count=len(m15))
 h=np.fromiter((x['high'] for x in m15),float,count=len(m15))
 l=np.fromiter((x['low'] for x in m15),float,count=len(m15))
 prior=prev_min(l,40);mom=np.full(len(m15),np.nan)
 valid=np.isfinite(atr)&(atr>0)
 mom[17:]=np.divide(c[16:-1]-c[:-17],atr[17:],out=np.full(len(m15)-17,np.nan),where=valid[17:])
 mask=(valid&(c>o)&(l<prior)&(c>np.r_[np.nan,h[:-1]])&
       ((c-o)/atr>=1.)&(mom<=-1.))
 if penetration>0:mask&=((prior-l)/atr>=penetration)
 mask[:200]=False
 return np.flatnonzero(mask).tolist()

def raw_outcome(bars,i,side,tf,rr,cost):
 x=bars[i];ref=x['close'];stop=x['low']-STOP_TICKS*TICK if side=='BUY' else x['high']+STOP_TICKS*TICK
 ref_risk=abs(ref-stop)
 if ref_risk<=0:return None
 target=ref+rr*ref_risk if side=='BUY' else ref-rr*ref_risk
 fill=ref+cost*PIP if side=='BUY' else ref-cost*PIP
 risk=abs(fill-stop)
 if risk<=0:return None
 for j in range(i+1,len(bars)):
  b=bars[j]
  hs=b['low']<=stop if side=='BUY' else b['high']>=stop
  ht=b['high']>=target if side=='BUY' else b['low']<=target
  if not hs and not ht:continue
  if hs and ht:
   high_near=abs(b['high']-b['open'])<abs(b['open']-b['low'])
   reason=('TARGET' if high_near else 'STOP') if side=='BUY' else ('STOP' if high_near else 'TARGET')
  else:reason='STOP' if hs else 'TARGET'
  price=stop if reason=='STOP' else target
  result=(price-fill)/risk if side=='BUY' else (fill-price)/risk
  delta=dt.timedelta(minutes=15 if tf=='M15' else 60)
  return {'signal_index':i,'exit_index':j,'signal':iso(x['time']),
   'entry':iso(x['time']+delta),'exit':iso(b['time']+delta),'r':float(result),
   'reference_entry':ref,'historical_fill':fill,'stop':stop,'target':target,
   'exit_reason':reason,'rr':rr,'cost_pips':cost,
   'signal_open':iso(x['time']),'exit_open':iso(b['time'])}
 return None

def standalone(bars,indices,side,tf,rr,cost):
 out=[];p=0
 while p<len(indices):
  x=raw_outcome(bars,indices[p],side,tf,rr,cost)
  if x is None:p+=1;continue
  out.append(x);p=bisect.bisect_left(indices,x['exit_index'],lo=p+1)
 return out

def pct_period(trades,first,last):
 subset=[t for t in trades if first<=parse(t['entry'])<last]
 return {'count':len(subset),'r':sum(x['r'] for x in subset)}
SOURCE_SHA256='dda8a9b4d96583652667adbc0c31d187548aa20361ce3cd41b32e184a49e2dbb'
SOURCE_B85=(
 'c-p+ZThA=Lavt_y_O&!%9eC264~m6Au^nme3pp1X3Zeu83?Z;}k-*4*PqWG9baPnjnqhapYwdS^HplLVHPywcV%1as;}8G)KmPZB`mg`|FaPw1zxv<)#~=RbU;g#K{l9<s'
 't3RMI_Ad?mrQtss|5t4P%9#K1hrjs4Km9-d{IAVwc(4B7|M<`UZCU;6KmFaG|L6bw56l0>@_+hEc$wutmjB`x+Q0b2fB)zI<Nc98|1V(w{I~z<KmLb5tP2Z-Km6N2{lofy'
 '_`m+;AOGtI4l?CQ|N4I}Ys>%RKm31x(ZOv02(Wxmz`U%w_iDlXr@#OAZ_M}#Zueuz<sDGkO5Q;E{JMd^|C|5xcYl2E;P3zXZ~wNth2_EKEv)etT-vU<1)u(ZYPaxj{`Bwv'
 'yufP3POmEU@`*qH4X80MICjEXG5-xnkQcdvl_B$~)IG=gL-7)!yTi9V7W#rQib4nTM+j#-U;^@JIU0!0l^x*w8`yU6#W&y&j~#sjfPVy8X<&JSVi*6v`!_{t=*1k++u+c+'
 '5SIV^P@(eqv;oz7n=mOA2>bzH3qSO9UNAMjV`}swPe8OH4}UZ$n#Z)-H;>)-)E_V>Nvq9c#-THh%V*KZa*hY%xx@S){_TJM#BBDEmrv*wBOG3~6CsZQFKPpJ@LulEAa5tE'
 'Nd~Pm;Rt^;GU~JnV{7t8J!L@9{1HhfXtaMs`QXImEI2jc6ugPfdxl8P;uZMq;oBeKCy)<*{7Xa6Xm&#J1z?l`Ll1C*(lH-<@2Y&{E5x8dyb4h?(vpZ<71{kTZobuoj$~9p'
 'w}Etwv$%o&A$hRa%OPor!NGFKyMP=%56R7Z>*Sv?krMs_&<b8wUUiSy!VQVWvsynpcrn4A`iA{ua;GRdIkWQ3%$V#Dv3~^dQ8YuutEbD7X8415-uM0jleeb)AcRd%OzZu9'
 'qb4gX(y57)9dP_h#KTwei~arrv(}vW!#Vyb^^R{M#>c)JA8SGT<ZozvldKKq{iSqtg+EXq&YdMzGrJ$e52<fw(04mnP=RU(NFy_zR_)k~cLU^a4R?Bu(s1LNl#oP@Yoq|T'
 'CsH;XN+E}2;`^8>xhICSMsV`5$w<_w_aWz}q|rQ~8sVUVvTCe?6amwk`NFZ7>E#@FForyM_QBHZ$&*28@uQrq%B%5yFE&5OeS=t3Y&xutU&iL3`AV-hAR4`<;^dA%$A>`E'
 '!B~w!hXbq@bV$aCtx7j$4wnsx8ub1E`~;0=Y^xWSX@5F;m^R!SD2>0pd^XD7E2VIwW4PNO#}F^^nRgV~nYTsx(I93_tb9GAobVaJzt#`TkfRQA3gMy*86L8MO_F+um}MU@'
 'uj0p>d59Of$K=-$!Ujb5W~R@QEM`8v+PNgDwuwhA=O9Dw#=Zy5;1{qrHUuP#5x1oLl$!7%em#LddKc~C`w%Wg@cqYb@TbMEeKGCGmZwOMCvMWdQFE)e-zlz;bcR#zSV&}6'
 'q1$v5WqpY3$pzjLvG&!w7E!58f}~WL?0`#_1*7Kl2?l^%`ST7hf4(BwP-)BzCl3cf$fa=d$Pvc5lqKA85y{lwoBXkBDHR*oKxr~5obH+piwcxhqh9N=cY&NB|M`FVyTATl'
 '|Mb`Y@!$Q!Ie2^c)@AkK;oWy$jT7;k`i<ps`OPYgN%x#2f(AO|N^iTc&p1#g=VU~!3^^Q9>cVTv0jzTY7a!Ly?&74Am@)As@Z%vE?4PS>c=2!pIul%QFng|k2S3P(LoSz^'
 'k0?wyTvJX!4u{uv>@iLm!VFpYUPk%8YF*VkjK617GK`><ay*m?br7HAX0M5uHz2z6X8A0l?68>A3!X)Xw`*g|!FsZn2pvHu^n`GXpY9Le@`JuWOhd%a95{{ShLG(LvV9_F'
 'pP%yuVEKqa*E4u`XoXduD(K~l*1U>Z6HOfx{e@b1<y9-Mj=Ais;;~NXKD7wSs~o!~ZKaTSUEQ0_+s?sMxsfV+H0NMOMKWgUor5<N*JU86lrzisB6cmO^;*n72fi7FRRWH('
 'Ca<eV4`in@4y7)qWGaY|)2e?2a(a?cLl$pRZK>~R9i$sFIvWsBy?+4mQDRfnX8i3&E~8+jlm}6`ZEJOf=+IHHmJycVDlq%$Bu+-<lqPTSOhnh;7XN8jDzzZ3X;%hgw^B9k'
 'SEQ>pkU$0f1^|W|2xnO+=k5D%w3{(BKc6}2sYZ#PK(;MzEguMD7nNQq7^o$s&iZMGHKS5l8kZ74VpQW&Wxg?E{|{fgEG&8RS?jp-1y%SnC{W_}hth!#ey8<c=O7=8q=t~g'
 'Ay3#s4yU&zS3wR3(VajR-4xee#r7>_M7P*T1aC}m{Ap%(c3zKP=e3;T552aRM$6fvvFu~|mpNV|e>MInn@QLDWcS59`B6&C0)w;3we`ULVog|%B(!e*&EVY?I{>3hQ@Qmz'
 'Yp?_NMaE=GQ4$U<WGB1lZmm89tq(T!ZZ^W1gB+icvK}(R!NX}FBlOz1zDSxVJ(UfICVlDB<>tPksl`maw8`~tTpJFf#SAEfLCY%lysNfqaMy4d9Pq4ryMfY#M6S(7wHxT3'
 'o2?Dpb?Me~XU+sR8ML9MhvYzs-#|E&_T5)OvjI^`eNY<a+p5o-0?~|uby6mC2>BW*lj)T`Hc^DxwH3$3NtqA+H&`0;;@9+A6wHe+_xf;qm*|GWe1VxHJ>f4h$rDM=57~_|'
 '^GLS=PLrJW#b9mV{M#Z-W)CwYm7@EU(A=kUTZy~FrRSXzUvcO(8}M+Pw-U!^T&nd+ChEitLAEEp++wR+wtp`#*?{QeJh~(CZO6lHexTRil7#grksycl4h8SOdCxGkh)K>@'
 't*eiaZe$!r51EFL-MCU3U%|rruj9$~=L`vz=#Nfni$3)7Ypn~rtFv2-_67RFhrIU?{-C8Rui_K_-v&f;MuuUbT3-DkTk<0<NYIz5g*Eg<8L)pSy_11}67!lp>^ppUHFj@^'
 '$E`YF_swlUG#+1Q3%EW(gL&X8FqCsqD`znY71jbKe;Puj!o^^t;|Rv$C5jT1DXEZ3JB$@KkXGG?GjhXSW_}c_?sf8K!w9sXDA$^hghFYkQ^~n(LdcwYsq~5v451G$1ZzA^'
 'yQ~=$=<$2eoaBgp7(Xuh?A|v~5yQOyx}nn97BVB-(*Fi-tVa(ed{;@I%;*3j^Vk7IMtuN{u3q!1f1rad=r3F2+94n*`L=pL)8O_bjatW+9<RmxOo3p2kX1_A%kEzkSo*F`'
 '|K;bP8nD=Z7S|Zz2^y`~k^_z)FV5`%GLS@Mcdd+*i>(Dr^7)uoA5mi<<%t;E_S6>fnsg&`dy)JGgjCida|%fo4%U~L#|%9lcR$wId4`_YQXP%A5arU>ylQVR$&4!@An6nZ'
 '*BV(#=6I~2y^)`1dzMbjIOtkgR|N`_Yme`YBPazO<xnh^Ynr=*f{a1~7);Amw+CESQA?-syT4JMq^hV(pOu@j)2fvS8n1OlGZ59`l2J+JMP)TSgxTs!ik6Rp8R-Uy^#H%6'
 'Rh9U`=*o*SLdkwq{ABLVOco$PsobzqdUenAvfAgj*+se5q6r(fq=o5<<BCdgz~|2nUeAUE@lI6-Qoo)FGmbDDIIVHD@5^;>wlb36@0kiJ&~0GQ2$xsS=)UMh>>>s4tu-;k'
 'InTQwLOpmZg9k+o45LGg)`Y}3ZgB1hEs$7=o<(krv}${u7%fi{f~N>%n5+R~FE~(<l|&oqvrhD9%c{Ts{yNJ_<<S;UPBfF~B_hFV@2@Kmon<5=y5VVc%~svsGFNeEbOYK8'
 '-vj2nddiOwjW>>9v5IH|#%V=?mVa4Rl#GyM{CpbKr@HR<ADV7nD40+QU%nj9tKZx*{C=2%`bM~*ax&&{S~Qi&rGp1+qa!j~{n5|X9Ms*5%SY3hE9*-`Jbl|c3dJMQ_>AkQ'
 '2GM(ga?yJ7ju!rpXx-v40$%WrCQzW0Im!g^WtAT|f1st58bT&n$FWpBoE6!vErEXVx<WA4&E<m%Bzp;ZP!LtPh~9$<Oyn8-Y|Y7+_}7GY|MbtRbpODSii1=KX+v(@!5ki7'
 '8my1foc_4x`$1L|-|HT|yp5qeNtYhN^?->YX236)vnH$wwmoCAK4U7%6+`9>8SbyS%Q$oqGnU}Gug=@30~#Ol_@>!62JO6VV>jfN^dvL;h&za1P=8KbJyak%&?qw%<ke^V'
 '!3@OcR0UD2frSr>aG{AFg7;{bD8f7MaFSxJU?L@acgTIU@ZD(x`XwIWh67YKaQQ3>^jT@upH|WHRgh6do>f4htR=oz#n$%8znScoSo1xvaKa*@vS~YgRIX3TtI+Rn>0DP~'
 '<CLWHQZICEMN<AOni1v>SIhO4cNNGfXtzx>?ApM(O1Im<`lxdom_I5_Z_BIkivo@xee4Z};(%X23UE-^260;TW7lsnA$Io@an29ifzqi7pzuD{)o?q7;0}lqv}6UhRlvIo'
 'eJT!6DvIQ9yZ2ziM_xx=#c@T|9q3zEgVt8_t81#Yn4@Z<@Alb#DzmP-(>iY(t!H*&3EU$Ddx_YiCY$NX6h`C3R}xLHy76LHYKKGjMy4Ke*>WSF+QF;L2$G8E&Xp;ZAe#|f'
 'kI0)7B=t$~1!f*%zL^|sCmV$}V;Qy6HzIj8oaVV#VtPzH7mmE2j3#%eG!_MmCnfH{Nr~4BtABPll$e9;{h`2(bi;9zA7yl!l6n0=kaJqK<0>nD@lI6&hS_4g9W0{~d#bx}'
 'qS(RSPa$1lSf$WWnB$vQspoQEZBMW*gB}95LlL{vIk@=Z9TkZ6Zt}9yb!LE$Hh?7bac!$-{VmG@>GvcRbWyWQMaRmGc%i4&ZDaVnK9v96J=}nR;{2p`Q1SsZ370w-=e_Hb'
 'R2omE*Sv2R*Hy6EIle!4i0*)BEFrZ~e+&Al^aHg+K!$=2vMr>C*x}rJY@7<SSib4X@FQa$(PvPN)vlm$2su6jnU&GI0`gf<Ia{&*HQ>Q1DUGta+z}!Dv6wNLFA-l%Te3Y#'
 'YcJp!ZGA$%zxATxa7xHtxL>cU@$S*WI~=;BB+uqx0dt?6`4-GkHB_sjvZ87<Wv2q6G#C@u5IIOi$9Dd0)RpVzkAhH9x8XnMV`kh&v{3N7+Lzz%nJOx;+<?T#yjz9alW6%#'
 'y4()6B{?nTaGGz0R{&H0M%aeoVWalio3b4$o!XN*<jk5K<o<WQKV!TB(SZIKad!aCD<L({nQ?`fGYXXh#Vzwr0~n9}5O;J|y3QE85nu%QUT<5a+cRi+8fH|zjN1gF{z;if'
 'z?|~Muwk^8AOeRTmE_lX%a6H8;~hBpCS;qsGmIPYRU4$1AGFN#Q6!hFyXH%<WxRTS3waa`pRt)hw2SQ5zs^vk$##Yc%W1d3!!7VvbS6Kaof(J9NwlRfmn6t*UpI|g8X?t4'
 'Z~Z!c%f!y4d2Gv)HkP_ix19Zh$QDZVA*<GM{KQai9gy)em;ANvZ?H7SQkJuo6(@c%LUAS=ksrl6&B-&xCC$mJtV>n>NG-j4kWN#U&x%POTPiqikCZbEqZG22kAK_Ve@mSs'
 'j=(q{9f0-GPo^d^CQ`zeDoX1rYjtGiAhOzu(w<Zs&}e()`gd4ybs2$eI|W+E^aT&>k4!D~K}zd^!=n<pR|0znV*N>p!;=!b6YsBgY&RSlgXdb__5<cTf5+c1W!GM{$WnIe'
 '96%N<#yaE(PMBm;ksX6!_TB-4g=vv*pfuLQ$=qLq4ctX(R{>e2@PnL5c7hvn^#!qZ#?SiErahwp+^=YCy;t;Zs{sQ;^UiBV6;vUEYe$1OJNR<M$j>5AC`c!AkyjzF;_03I'
 'H6}$zrcggBD@n>T((M;IuFU1r0fvJU-VyP6Gve;YgY&aa>xZ0%_``uTw#6TwIVm?Bq%zwW4%0ST%u!;Q*_}HSt#Pmfz{`IiUr~^>!P1;OUgoKHWqgA`8gdhDdc~v)b$m&l'
 '^uK`>euoC{Y66QL5T(pnb_&<k&eg*~Z5H7=3gssvglWCsKxsTg-_n{k8)yc<NlY`bPK;VMVJA&x#^jVrW9715-_FDo7`Y(6|B|tj*?nJV_jT~Sz*p(EISnTrQj66gM`kfN'
 '6w+^)zfjpwk?I8iklg{_4i>&IbCn;3I;le(>U=!a`O7dzwW1>(Oi6<o`Lw5GQHkA;bJkAZcy6e4rV@i(^pRKb{(Ry3Afvk%`WS1|zMgzK%yKc~+i|-54sO=(-Q=WDnNZ3f'
 'NkR`#%putU(VR!?bQc_m{Ma0H7cpx^qPx2aE!}T}8u)M(c~j^74hX10%c2I`o5XZ==(!<QDKrey6Je$FqX%lRG7hVQj66Ev8=}0sjNS%AbE4<ZN-b&9?2*Q$jXqKUX2KYu'
 'k`qk$v>Pg;+rhx6l?IVqpTOT_rPGr^T}o(qpZg~b_~nGLOAN|DU|{yB`IzT2gdgPsifxr|FWcLmq|}lq=f>An#Amc5DwE?IDCw9<WZE<y3zYbWJuuealS~CWRGKp<wqy4W'
 'JIJf<0inm@{V0SU+hU3qGWTeznkFQB{NefL*Zg3sa!>L&3@>jX^uo*=+>;E%DM16uWvQ`DihCd*6boB{5_FLJN25TY)N({1J0AeRhF}a$b3|;QG^c-D8^Nt$$z$8Dep@Z='
 'hQsJEqul4Ut&+LDIW0fRC^ZEKg`IJ&*i&R{J^4jNj9t{l%%^Lkk_5^Xs(;C*l+}3qJy3a&(<2Te#NXV^{(AO&O6~-*bX=Cn1@w@RRj1|WG=&zid*D8^pkO6nHxhBfRCA|R'
 'IK7M|F0;HEdl#BF<VY$K#mSQAR7ibwX%$WhV&^N_3kuMQxfu-K0sI7wPkEG8Vx}SHV{OW<m`RV>jqEa<w+}B-jic?4<$(T@9XPd_MjD-kd6jR^w3bJy{Obw}1S^8vYZ+*3'
 'XB-=cg|n1lj;gOG$>9dlcmOAEfZ&T>`JZ!Z6@~^f$it=bs_QvTEu|wT1-HSWalua?JRkE6WIcybqkmfUJu3J%x&f%Ks)O0!rV34?^3V=zZ1D$a0^_9#<boq8MIJ^XNA6{*'
 '8d`O7zD}}n-c25%D<oCdbfUaBvT0u#{o%a20IZ2j6df*>#!glj6ii+TU#_UBtG8R2{2-%LadFm^E7KazI8Ty=dH1qekCV<gD5p+keU9So-?B>PfvK()hsqhpWIjd6xW;-$'
 'uOq)V92F$03(JBFMyMe?5cO>G(Fxkik>$!gX2R`}CAOoGhk%2a;mO48XwHgzVAqpLAxszd6jY?u68XXvG7pwHX9mbre(RKZ@>i&g^7a*0ZnVL>3Gj?L$vBke1mxP^GWdW`'
 'F9IDHaFHXVs<7FMz=DaC@Z~b4JOsh;{d0A9NHlJF)?^;)`Tgxv<v~h0XU-88<c!d}aBYd@cC#j+=U9Dv6<A@BO6Yj3=xw)PAGv9$IFy*Z$dX(r`OR4D7qG+cfSiK%E5$hq'
 '*@@li%|rI;lN8B`{8>7foE9^EG0}JqGo_@Z9&@*Kt*-j<@OgZ>o#|d;H9vV#+!NM#3w#Ks+b!6GFz%Udw8KGK%rH=ID^a>!<`-5J=l4(>r2c8+3un7feth7Ivc{9e$N)Z?'
 'x%Qq4igfr>06oBOdLiar9gY={7P~t{HZ694deSGK{GDuO>!*(WiplHX<IpVj6U|psuh&Ou?IN;}!m6w8-ix2@If|^65-{KF4t|Z;CcTi3p6ub=+;O+Vp*w4u^1@Hd9Y)1F'
 'q--_m!2HoD6!+9s9v|=*osJoY?sOEZ9M{!!cM%_vvZ}~2V=_W1Y(*T-TDoXIM-M2>T})L}L1pl8c)+93h<UX~g8CC3OAQ1`fP)kv>^zcSEVE~C0(e8#O=Z1zpg<?_Afz+%'
 'YL_PGMwcW5GCJYs`O1>;GpqCM9WOg1I*9`dH<@+yW|rjo9IcpHpsTE_UyQLfKL(T=42|tx(>VLgL<1RpkKN+h>!%$IomwCW#nKyO{rr@leZ38_N-c85BM54d_&4YYwdfUI'
 'iZb@{0g3G}<GYUZP5%@^UI!o;6yCkOdNMI;Il4o9&wJTOeP&{q4VC6q%p)8mnNyo#yG!O7hF0eGu)7wqmCAcA9C)?Ep|Ku$euI&v9Hd#k`<m$BV~Ex|ARE|CXGYcyASq=Y'
 '<m>`{H`DUh`=*M+>mWDNzZ=L6?{A+tmI>c~-6ry>4K%$U!ws-7bIiXdOx%9|zC)t3ClG{AE??N^0mpa4i8o|7*(f4UUJmbI>0Z{w{v(K6z||usbF+7{!B9dDLJ9J=3UqhX'
 '!%jFV^+xFm-oL5OPfcR<xWr`$0ex`u77<l9@P1`FC*i_uTZQlysWVJAbnQTCUQ}Kwd?f#HBXS3Oe{{AXF*?*zQ^UFn-8!o46`hq$4Zcv!i|;14oU{EKZ3u^I6TU8>&+tIG'
 '6L&ZV+MPIwk9t?n9(Nfmpg36?zC)$4le{(zqP&B6+Am5k0!Qd983&0<?2;V>6}QEPv0sO>uA-4fWnKT;LR8hER9s$s2hf}*Yw;D{Tu{V(!38pxrcbuo9R$(d<8e~QO@z-o'
 'AUYeLE?Q)3DR)BbIcIle?V-o>^W{}a2Bo`Uvuh+PIq~aiXCm{}{U;Sj(1NBRX!>eu;Ex%^j6xM@*wPTxw<)$vBPH{IlzW{?l9UnTJgqJHt^1{npv=?SmFvXIfTKhny1V}I'
 '!63x)JGvP{zY59e(B~5MzWsBBt&s0d*={gMr6FVl3dIbW4@FHvQ;%_xx1TtD!<jLG(vFakZ`)S<V9rZAEcvyPhN7wC%YIMP*)}kfRmQdVbS-|@qv5*VS9dLy<`A-z-_ey;'
 '{w%k+`0ADdyZYPD$8{U^x<+eUThrWZ0pPR~?;@!K9KPR?)N?O72)3nRDY@_bacxulP|j%N{KdM8=JvaA*N$kErR>{5QwPAlT<s4nIK4bt^qhY?6YZ=$-#}?3zR}N)H*kNI'
 'N}mRIgjqU+B<kwRuUE9pz4fCcBaYA553je{natUWijnFz&6#HfF_fZHx9E59(}gt~DxI1%Hm==)>y^1(?p<v#NC%nZDmO2rwOa2a*!q`@pI37rWyzZ#7U}si-tUwi{LMLs'
 '-t%NI;q?`Jel0iv8NNSwnYUen`nEG<pfRCbhxajucA`&T<lqdEx9R-B#UT%qbQl$uDhLW~Ox^B)zA6y^**1oDqT~>=m&+tWVaPj+Iz7JsI6p@dP(iU0e~@oMGkzTVhKq7Q'
 'cqg5&$T@GQG=?&?x8v;&#1V+Qi)f4?SYsF0YcVvIrthIcnSZJK2BPep=hg5;cIZP?L!pC_y_m3`6Ax4eXBemxGq9XB3_{)E)dgl5MNk6<S+D!OrLAR&??1s@VOi95lc6-F'
 'N_q|5X^7V!p`qIxE2{ON0DE0U^MsOU3$QhS@pjs-Z!sgty@H{1DxFZB0wD$$qqVR&hhaMC+(7A0oQ5o`vw_ch+N+y+cR-Y&y?jlciNa4*+rPkv;3I)aR8p>_$9?CnO0PBv'
 '73kE4Ko0ezW=a*NiMG~T7mxzN^4}L6^aRStKbColByGS+{P<nA*`EXF4Tt7BJg%Ok6NTO|BHcrLRdHCIm_@qCyb2nATo)pzD<F%A({)c#$sOqn<+6AWwDLfAsuBf(&s6kn'
 'Ks2@`wv&$<(9Oifo9+r@JDq7L<w0{>Y^y15RBO<7x8;xeA>?qz&&9^qzja{T&&lx&$?4D|$<=UiFokMYDS^8v*&^5rN^dt@My2I><nw3`enQK?0nIAn$oyF=-bxg@V?4W+'
 'h>(uV=zOZD41P8Zx?xh{4@1+kR`!#*m(0Hc@tRCr602MrpH~s@4kHQ*O{_WMWZ!AYS?X|`g=~Ta^Jf41IjB%`wEIF{Ed@fK<jv>wSKhkG>_ovtI{3}R=@!226=F4%Z^rCt'
 'nPA!nsHhY#YWl1fU(`g;+a2$GUngw=<dxEo2VbX^e(bfGU2oYd5KtMJeBiL4GRR-dW|;$>fsjf|8m@x|G_AzxG&VG3BbA}C%a5l^wh#}e<ZugQ#OS}Q2CeUiP3)`><0=lF'
 '*c<YLuGp37+KH0j0z`L;4(Z1V74ms~Rzzbl1_Oi+c(y(ZHZC%SRe{puA>wzEqIA4~i6lz_;`Ny0Nap6Wh%)f*k0U=<MfoYm6ZlfjTon1mfoMTtlz_eLwydkDZv6Z?K~Z3k'
 '5^+2b9yEyM;TBhxQdAfk*92Pf(-tyH)gRX;$;+dZVit#(ZL4rU$x(sf)rgT~#H8rMLK8STQ<|8rYr`t&P`MEjFO<th&Z*u)u*f!=H0Z*I3%&=^-6t0!cOipVR`Cg1n2LmS'
 '@2HalASkn3z}_9_S0FmWB93Cqt8_XDIkmg7;b?>N-f=XONx6baX=9K^hdsK74j&}-`HnKrLs_M9!&P!m2*{&dgpFXJ>z#b5=eQC*=$u;gA|FUv4Lt|z2D^@4#|M%zffwJ*'
 'v>NqV@^fci1)_U1Nv<BxZ0Pro?5j95_f~ldgAsV9%4)oyim~F*SnW&|7ez4}r>K4ld&)f%XD?L>sGyX&Po5!3_<fIc{&KeH`Xo*M0n4-`BV#_}y19ZYrefBLS*jNMoQ!cV'
 'WL3FX{wSghIi8-4_(C$?#r&JMa*0{|2s-?!J;=xg5PB20?(hP)SsJX;ZIB6cVdL66n;Sxj<2sO!Ap~NYHsaWp4i3f6hi|CefRpO6uGaK~!zdvK*@?yVj@O*l)fn{KPl|c2'
 '*g#OavDgmBZZ?2x6Z;tn=@P8D#S^DRb6kkC>4^=K?hwuO5h8wfdd0aYw_iSO%b)0Ht0pU`OEzF8o3QS|Jh<^(SI?La=!N?qo@Xj4s1%K;VVjE4%@A#?fHy~Pp)({zcu~W9'
 'pae;Xsdpw_>}`e4yp5rhQ=^p^r3~RT54TvyC9uaBZ1fOmGcG&hxQpG+)g*Mr4UA;o^gK2An=Y;KyEz3pkLA%q?#FI*)t`x+ys3Yo*U|CQ1{9H8{%AN*FSi>AhceJUEsVwJ'
 'B5`mw8gw}8i0xqM)IJy1bb|h{k+Pe!f)GM@ckl(6G><z}rx}2-@vgJD12U&mis2Pt&y?um+@<Xfm1YMhcM1oY4gP~+GdgR*BSYd#;5o;vJ{L9b0hW?xaRctO=<f=K&H5~>'
 'I#&ndB17S4KO?@O7qAcHGDQVBo}DUQVX;o`BHzQd+Nt@wI+wl!qSN-~f~W;e59A2yla%@m(gfGA5%Bw`Afpzt6kyj?e!v~jhSWV!9mqEi3<z)feHGvoh6E9Cjz>@Xuwwuq'
 'T!OkfDQeDqBv4+7|2QS4#UE2<;qHdi9T1JfLZzs-Z}VvN_}#e9nHQ%q8bY+eI3|l196Z$4`i{3iLhdN9iZ$|AmMK&qMx!gVCV``%D^!lsyNeKaK$N=TWs5@X_5VGWLlp-p'
 '<s1dt?+h7w-FuCC^Lq>ea&*_IcT~}SZy?3fl3JSFzB@~vAOtok$AxbC<jj~*_YH{UEt}E^?aUXp3{@Oj$ZiPPf2HS>MK~3Z7P~*#pcZ>Lo#VP3q6BYp!6gW~!ZgD1`hK{B'
 'Fp_@1DXz%^xRj1Y<H8cfIgA%%0XC+7R=u)K>bA25|ICSmMj8Cj+R<ft$>_Ywy#d$PCmA;!q&oSVQI;LdpULQ}8_MgWtX4UsQsKPnZg2XmD1uHi$Q%YfG=Rw*IuvhAM{Gzm'
 'ULnhzNZTsl13@M$Igu(T5+Q>*WQBypm0!p3U<fNM|26n^aXq(f?H&B_{+pE#K@AauY^Zt751caer5vqw2E*bc20)s2RWd48ZomuNli@wY;ej|}1EPC1A;@<_yfTV}b4CiF'
 'P<|rN6^af)c{#)@>jwyDB^UPU`!D1+9HbID#*sbDja)+(jYYUVTHgL_ovOc5YE)2Ob#C%{^&p*_R+tEPhe0Q!DU;hl?!TRROu%PCxhcy@xhz<WI<RLzo_AJ01UV48ZEs83'
 '9eAlw@iI2yib7=&ZK3~ULF73}ebTodjPcPew%qe#Tju)qcbG^e@KNZn()ybRsuQd}aIl2|s*l*OnSv97B>UQBSRCO-2mf0Of5jBk@WmLyrmg@JPhEh3R9Sk1oAv-QD67Pe'
 'hui56e%4QgoAAyzBs#m&ggGs0cgneXO@y2u1xiVKVa!xr4c+pDD=CI{Ks4DyD^K7H+jI|pGM8nQqxdw~!zq9ES?W+{OSFL9Fy?eZE5hZ*+v!8j^`qjwrfa*J1=6p@DKwa='
 'U>$O}27lD!C&Bu;wxPIR^R(D)XKbMLUuP$~^>?T=mM~*<@;7i^F?5Y3bgip5lzNbp%-iY{XU_U8h^hc0<3d?iqSSS`pE|}nu9ygOT|+xiN>z#S{hM3U9^t&E5+Hh_i0ll7'
 '>52rWB<sltqnzK$BSYYnifuRg7_GOHp@l02=W6%^@y9R7K&B)PT8aDB4|ame=T)zz&YW>sHXs@`U%C5{WY9tKXs^w#Y&bNSL*d%!hn=C=_#rp%d@2792dOXv%4LQ@ph~!!'
 'XQcvBojjqfj8kF0(VhJe;s`rR1W0u?+|Glz1EN!sQ9dFR3vz$WN6;rE!WxGL8L~aSzE8)DbIUmifz0C9cGv}e*nqa*WBsTY97`LxzGNb_<a0yFgR?QO+WRA)&`ztA5yv(A'
 'DpifewG9YPBOkkK!48gR@n=v*iJyc<)ot}GZs1q&KKez^4X58UePpg;)AX!Xh%9e_Q<j~^uPKE$p<RrY=Ck9m#BRzhSo!uXmOg}&4n$`dBeM?{y)$91191mR<F>?gUJMCG'
 '(z<$9BfNxf`<mOh>5%zdg@TGkA%8Z?r9!ctwQ>hvY(;^(mli-yx8zm4J);%dVVgt1EVT3IRog2kyFTWLZGZR>vXvW{@~XP-eMXNP*$nj<>`*Q27v1kOrw>@YUU-2-bz24h'
 'M61jOL}xBEpzmh$`~~#DPXVu(PRHa4ZA^@wNY?OV@mPMUQ7ekr)RBiJ_$Mr$S9WoHK>7p>@QlR^%D^)7!bcG@I#oT*;y*DGv5Aggx=bR23QGJ@PM+k|I39?8N%CaFp?ED5'
 'N4XPI;F-bP#M4}+TSRSLnw)27E|Hl5sVc0`v4_(-!wZg^FA)bZRV7OM@+?HbVReu*b$-_kDXV<<7`M=&@E<C5!_nDdro$U8k~XAiSA-N9y|J^Fusl~QbSF@duB!;Ay#w<B'
 'K|x`3fb+;4NlVa3^_zQQMtX<95&a}%;0~2eXHs5E=&&lQCsc5Ylf9fdkh{Jux#>Ja1*NEvC?giUtTQA9k(XxM3J#~#8Wf|l4B5aF20Xz4C1M&KIf=f99(Xa4)F;j_fbS6T'
 '7l`HS?L4XBoIS-wpx_oCkX;C2zHO?aQks%Dl6j&4F}Kf$+<@q+3hLkrl0l!R&_Y~}%fQ6o$OBI19x5h{`GZl-QeG7Zq<8g?3u?mwmcz#b0P$>LctNEq3{Ris!x_`=fTerP'
 'eHNC`gEJf{pr>f3{jLp_CYZ<=S^f^VmzcQz2+B{=T%V{QyZfv0zl7{$Uu#|MWb2zuN<cJ@jdgKR66{une{&aGK^dKzcwrtxD3cxV^vcRFAI1RI6UWG4XHTe)*(ReepqV<}'
 ';0HK0l}pULzwlx2=Tcb(6X@Wh5o!3!s=vQOw&KuP_vEM;QRmaD)hEMeFtf5YJaeq0tR_u$(0$c~<yoLSg;F`Pgc{q6U7_lLad8DWO`)?$Je{w|ptRZ@<k&ysKVgH#S8`(@'
 'z;cWl7Y(`jP&C7`%CCTqjLDrX@FOp1#-Ta-S5(q9exYRN)A8aVf{hr_P-@JZuE?r8Q0UD{dq2_}eu7T5z_3chR`z&TqPp{rpUAxUxyA<s#9iUR&%PHbhmBROfG>6C4v|~k'
 'Ef5ST8S^Td2lBUi62`<tZAW^Y2J1L|73!~{T!x{ue=DdxTVn@u9GmdO<;AJ!%{WWe2e{xA-C*g=;o!P3lbh>@L{7bp+6%+NxGuDX<<&=M-zpGLsfswlXj_HwK<#e<5_E#j'
 '*K;7KcfKaPzviF<F)D?owg0sPI&LK3YG-!Fk=lTqg3hq45ZrI8_fKP}IL_UW^`qjf!n{hFqF?JHQ^dvhCP#7ZxT&D@`1^*h;&=BZ$SVr31WbYxl2^Y;0R8BMR2W8ySYnIH'
 't9JTq(E(<Sqq;%r*WTw<*K1h2*lVgdSPi+K!mq2O?F{_c+*Qnq6#gd3v78mx-rkczTtZ(A4mv}NzPk3V&(%sj3fJD-DxS_$jMi}~0wP0z&htlZb61a|-9Xe_gV&FS(FTC@'
 '4W&O3byOH$sWeC<$jO89kG!0yYW9w06hog3HLtous_?GF+71cm1nq<=IdzrBBsv$|3hscA(So)z=<pSr;!n}z4#p*r{Z}{MGp741&|ZiMw$(i&w@^Rh3{cF!eRP-Xz^ej;'
 '_?|8@(3JiD?ix53JL6Z*P!EnwGE$4${>1UdZp=Bxx(f~*4C)iJp-69~9uFFL&(P2v4xOfu8J##}dS$YL|N84Jmii>!DTysD+X3^odUG##eUj5+W;y!JdD@#f&ngO4>^R7-'
 ')>ZdFxUS8v=8s0<+M}+jWjtXr)@(nL9<mp_r@R_&r`p^B0hKO;5sDjQ;k1VMfR~zaIGvnFkg??sm5vtgV&Fg=T_6)KP?T>rfO2MJ9Xg}9U$O=;72%;@A~H8gDOXV$<pxN0'
 's<5=94HK=YGnYJ?1v?U2XYFL#bq7mxR$^PW+iU^(l&fV8>5bzhriU~RH%v$L8I;mKqQob%t%5skFENH0mt6#(NPn*Q22jT?vf36SG5p0c0OPDFDKU$&qmX}ld=o4Fnk}Su'
 'vby{YP@8`Fw4BC2a6)Aqq=Sqs=fFuQ_PMxqtv=}s$bJO*jKA$d$W9Ge%+#;E`kjJ@k0zPuyzyYHhu|Y$vjJV^p4W;!>CMcm_Aw5f`bf}YcY<Byb}&%?Y$b0N*P#50pJQde'
 'W4GF&(%8k7C|&e?^J+L9a_SsLiWohhL%1OL=Q8s~6Cm;08zC!@+{|&UY0Nm3dV?(c1DRhsocd&oD`}a@kdNL@dt@0CD&YtDFy(UfeNs6a7AN5S6WwI-a)kvdrLk>1+kOMA'
 '@n|R-EWf;`$q?JN#cA>vVC9jlapOS`Go#woh@GbYbp7ZOu=Lx3@+wGu9s715Tx3nw1i$$5=q|7`8!U|zzk(E<;LPQIGHae+!8*2)$N-bIIGa)5Fp(DjK;`>22_GtN!G<Lo'
 'WH#Q~X_ap%W`+u6Wk1KZbH?tv;Lk9;60tu-rX9qjFA(~z^=F4ery~vjc!L>zZ~o@Nu<#3(Pjj^E|17{95kduJly<$L$8I-pWOKSv=*>OK8D=rIvn`37K$zhWO-gTg8-R$X'
 ')2FuF3Mw>xWepau0x~7<6O|zuhu1*{BYW-=dGXcNdpjVSx6>7NPZ2a8hPd|KMgw5B*$|2FWBLMSL#5n-m(L)#rZM9Ej!TAtN<|4Lh#}s7{gR)f1ssJQtGWtzPlMXwFe<f3'
 'WCJV;8A(B3vvbCSXSUNKPneYU4@ve6U5P;9<kaMVHreiI7awcugq%1wH&nX!G6jDi+$Om;T)nTVWdowQO>(8!rmr==^oZR$y0W5BzM3gA9;kWjC2?=Ce(6Cf&Guy|*hs{D'
 'I_=pm@ml#6qVdzNiEqhrhZ_h28ChP9QX%u<^2kME=ZT!N5ho2xh7^?OqtGK>S0C|GHXs^f?$<e$@@tt_)x|6m(S3dvs+@423ZX^cDzAnoEZ_!&%mturZ}Q_U`tc=b1X(}U'
 '>h8OHP%03kR1X|fJFTmD9-|>xWar2wTSyn!!5%S|U`g`1D&ptUYoaTT9^YP}4G|BDu!C^7@^NFE#t$zpVfv5Av)Lct9?>KC1ae7YD1<U6L^edYID;WraGvc2S3?;Ut5iOV'
 '*o!+*%=fuMWikxaU1(@0B)5pUM{;<1j&OycV9BbO+z|02G6q&A^7BW%aOAA3!bH>E-4FVbfLKdz{d^JHhDvk(gyEHEUbW*QP<{mygN<N0Vzgm8SPoy@cZT*-wu3+F5B;B0'
 'oEr|%Vg^~y(I{>V(ExB{v=}@x@Z^xW3QMW?&B&;B2XK0TE-}8dHgd3*j3o^7uWiC~8T>~~<2Q&0UB98ds1WFxY)W^A#n5mfftd{TGbB=?AJm)WRla?WVoy*q=2+_A`_X2e'
 'L1UkYCSqHrKd>yZ+?w~T;kL^6M-$tlH0Cf4s}J(3ec-|*>Prq{JCL%493O)mmDO46#1*lcFAnZ_E@bbhPjY(5VF)?i&fc+;9jmlCr*tH{0W39gUJsg7BNAh0gi8zBY6h)4'
 '>pcWO6O6TRg{JT)D~6;gLG}XkU<kf@O2H0?&g>=j8Cm8ji2~2OkU=eGghR7;3mJO_5l#NsdR$B1*<cJFrakWsluqX8<(N3OLq%_td1`)*I+<r`Wfts7T0PCREfMA->hc@V'
 'U2(uSjL)*v(_{t^U8%f!=Dc2F5-Sj+1uc0cW!2q%)m@&W@k!E0QS?d5YIv}8ygU-r*rohnTJ^oVz^enY0)v%^otO_Sh~wSYtp$bC0S397Bd<=qgj&F$?Bz%wwPMitImhF!'
 '_LG7`sm9=jGCm*nbVajod6HJjL3CI1R2HF%>f-y!ibQ9e6E0ad@+#k5)>EFODzY!5Kcb8|OcTlL5UV)-;&9AHUbVequg3F0@SY)(<-kj9z!&H?D(@50N1a&ynpeM}0lMNu'
 'M5kk2N+0zCU-G6c=A18d_X7^+$$=^?o!d8HPbsVL%u5>duIiv-I`Qd4FF<Ii3YS3G07A5oCK80%G*8`uLT@reN$8o=(r7b)Q>wBd(TE)i!7@mwokUc9afAR}re_hf+=ZM+'
 'KA8J?TH|{>hE6@AFejm|+WUEGDh@@^o<9rnJK5wDh5kUa<Ozrg13Bj>kI{@t`9?MXJMjH8YLXshm6&0O**!rZT3~1qAB}vASR{_W+K=6k=u|rm5wxP5`}6HS?ooYoA&*`O'
 'fRp#A1rzIp-XtkM;hP6im<5OGZOk(rgd#y$si^P?CcO$ob36`NuQDHcr<AAlO4f>FkLz!kCQt@F305J>2N}PW+D5JneXyP6J_PK9v1N7D>p4#+yr>O{PEEke&TC$E=RLVw'
 '7d^W$U(8b;tEwwa<n1V4_~lhNso<p-VN8Qo);|`=P2a#5Sm)P&YqnPGE3T_(P8moNc|-`_S_8q+R0``2>NJa7K1%(}N?nC;R%im&qp`>aJnzS6|5mv1h6TzU2zkS@8hd%e'
 'OMRC&3?<@p;sz0M$}!QcR}F6zUI|!Wj;yO^nW1Y$oG%=8h`CRTQdaGur}`%M@QnnN#zeZVEUQ6LC<xwycg8v*AKug`Gw-76^0It14izQ#;)t6vaBo0Ll}Xj%J*K_7x~K5C'
 '$p_dYIZH~sarN>)?^U>&)OC-86qvIs90e`GZ|{eH*X#@=XtX1f*0P8h+*d?hKY8JA9CQU_Ly|Q)bnTb`%uB>c!33O2=YNl%`XT;%6>bml_ej91l$-ia`CjVuAZoq{b-0;E'
 'Y>!#19$-e0!Jl*gA*HhP#5V7};c3_V<Fy)X0EBUq?^U?@_P9JrGhniO6b~e0EoAJ~!L|iqXd@Fvhv=5bho&mR_Na7cPqNfe-a&Wpw3rb8tr=|(L=75A))ap~L-!j8DmCR4'
 'a=5vg<9o=Vp9Fueo@A}y<>QXpFS47uIe9<VDy=x;=)^>^hhcY#%+Uwqmr&j^d9;@tL>U&Sw;=n54K2xyepZv38tDzLlm>m1LZzbb<_hpV5RFNpl+86#4uXWi3T=b2rN9o>'
 '--v!TzR#Fc83|fxB#aC{XgI~$a=_r6;Y4sTZEL6<SA2%w32!QuHV@X_%ksoAv;on)oB6X|h>)p_k+^qI%N~bO>3MUd*kA_us&i*PBas2IPDudz*Fa?zpE>ra&2bMQ+x}pV'
 '_v$H08;5AaR>#B}85A~0&=+UuhDxX860g#Gm7hpXkd9g@f0T}Qiw$N1MO!*V@F~P!|7ACS9Brs{QYRQ4M+9FT`&YcC1j@6B*D7+!>*92%;R)y5B7I}g5m+B02aa}L{2MHd'
 '__^gWZngl>>rHAdoYBN+>~fkWI%tc0-ItN!vGU7shmxlD%Wx9&$hBt_ciEvu=^t-uTg<+f4{ez(IZ{9mi!+m=>-oTAL#5n-FoLe=5A~+wUVcH8VJP(^VPa9{K&N^_uMpoq'
 '0XA;8DXqz<bTV(|P`5ka(}~23*>T1LW3cJBzw(Vu;H>hSu=1S|w?!P{4LU4=YxLHec}JcLcbE>n>r?EN)hM;=UF<G!I~sX}8DV5^@W}LjOHt?^hfdqk$qoV+o(H;@q3#Vt'
 'R~*8_>(D&%z@JvKGC2pSK!lb9mZpm5-k?;<{V@rmgFil@+_oi!7-DvE(3Ds0?Zp*VSEBv?s1wph>uSe+ydfjK0nwz7@{|pfLC0R%li+4G%KTMsaRZ_R9s6TA@@guV#z(H^'
 'HY7%c8bA)IKW1Q@>BrAO^oBzdPuSu=N$LS~D2Wl`iR_iEzp36m&)Dt_mQGpVIApusg6NcASjPVbf;v-@C*g@=50a`}$YHbxq7?M-kc4>36?us_lAq*M2fJN4C}hldUiEp&'
 '?N-L2Jj=<9j32@M%=mj8I@8xAVGfvGTfHMxu#jcketA26U2aQSOCfXDdU!4Vb@l#)xhsC0Mzvi#0I<dEdtv3Z1dHz{87=0OfU9r!oZyYL$4vxRe*({+HXAC95!?o$fUpBF'
 'r?pxa6(71TU)KP&SbCjT`2jPSp9H#BvYC0-LWcWUCf+!-ka=VUp*xrLN+ypt3Z>ZtFK4FmoSW13B}l_qt;s)VF3cvoLUOCxNi!w7*-1l9aHF=QCxIIBK*5o};3WAy73*~K'
 'K;M|`5b^2!o4}Exx6zOTz-=7m#HFWe2RNT>K)f<^rP<)_l!McaFfAVqa?_6+P?PK>?v@ucto=TL!bUGYC-I={`SC3s*I@&GO@%&i-;iNgC1O4hX*7say^VRm{oZhBqIkdF'
 'VWB8x<4zgI(zP;MaA`QQkL2+>GoGKXR2OM<+<VpEJQs0~L*o&yp9LCW?&L9Vs4Cll=s>5QG7>eW>3b9U+uv1XHj<s`Z+|}(b$WszUt{Lgc)KC@Eg$(1u$KeOyc!-TA<s`z'
 'IftR`B{)D1ZT4O;D#0QU1H0uH`EZ?8*=CcdcG5&;%}!EGVxG7i`ItsB#0-6orn(AzHvS<7jK-GV&P|_58z_y13CS9X=jb|o<eP3^^K+cM0nr?!VNET{nu4a6OTKi_$s2}J'
 'Pw+U1szIE{U|~#%kUT^~WFe-B56<>y+<%e9`k09@lsGy$W)ABpXM1+@%Is}0F8MD&_T3!x^A$=)rOU%*8MC;9CNnC{MRtqV1uleO5+BI4p${$pu&hF{y#hUKAeQvr-G^yx'
 'xx|Kmz1)wLSNVa~gB3E89x|Lj=G&{?;up!<m0ccXz3B72Hi}(qmX_bv`f0t+pp<F?LpAAlHeK&?#kRw01R0H79~|33diUK6Za_e#C%F!(e*|Un`O~?=#yd|~{`WqDej@ze'
 'gVLNqF<E<*6$SkxnYa~(ioRj34if#8yn6d6`!|lDhAjDm>vi2%7V!6)j*3K~+Xa`E1#NplP&<DkGWeK(M?({AS$k3GWFAIp0(koz`!@`wmZZxIkP-7~Yv&{(+50(03D_Qr'
 '_gldBQ1mnw@ec6b+$puXN1`TaHCWN(OQg0&N?BcL@IHy;M`A`8BIaqLI;E5m)Q~d~O*ewiB)pwOa#Jqt92jtsBUrS~`z!P$!&1cX2_%UPvxRLG?w<3s2ciTW#8g}1x4k`_'
 'DJV3Fwj#$U0&Y?gLM}G4LxXmSA(<IxO*W{D)X(96H*n+U?AoQ6!xuEVzlR22LXIM3Kw0H+nd71kY#p#6Yl61`7dTTJER8d@66SSBT9j4nS;RYBzzvB}*^_jo??hd}E`7}{'
 '`kfwSb;{BdJhYhIX-<k>E)h&bgVB<WY-dmVj7h2MWJK<lyt=<|yJc$J9*5@G$$UaWVHj-0Ob@WqRU}4-+8tiH7PUJqp5~WD4+K8oq9u+mAR?)-lw0s({xz?jU9By;jscB@'
 '^*1`4)%A=?lkzDa6;eKB)jttHVB6d}mU$5G1{1R|o%Y_-EA;_725Ta(Udz0N%&@#p>?!te@(!F-3bYUG15+g9$OZ}+rlX4t$|~{u{$P&xYP@?2@*arpRc$&J8_?J#;Aw=O'
 '=!+A{p|y+<Y4)zCg3=lJOnM}40KcfCoj8~_9BRx`X{3nx@?bwd2z0ugKI&z}k2I-QT5~nxI7IBu>huUqL?#z~ngj191JDd9DA7BS<~prDbDvglcr|3{ZmK|bkEn=}w60*1'
 '4L16NVY#glvjR~XY?&@1t;Q$##0w18i8+tImBdU&;&{8>4bMn)awZT9IP>aRJ|dOlf#K2`!yd2Mf@+H5n$2vdj=Z-}S5I5O$Pos>wVMFKTzaKG_5AhBAC+heU>mPMw|9|N'
 'AYP5R-$7qj@12DFeh%pXgH*OzSN+{x8udx6gzSa-q`VrWIN@@=0IzB5*<tS3z`7c=mnFY8>h@$eG#?NJp2fQAwKFN3(lz~6!wv|jY#vGzP4eo|IgqA=MUU*gag5dI{g}D0'
 'm_Vl|=!6)!!Z#1t4|JNKcl{_y^H`uvHn@MV;~R+o2D9`sHq?9f^po^tH`MDB%;u2^8$Y3rL+x;PhcP1Nu5zGUW52C-E?Aqm5e-H5<N_lY-)V^cE~isrd6nOnbHsx!xYNPB'
 'mq<P1@JgvWSy6D%Ie)Pk`H<ZRGtdZgQ%P|rW}t5{Bg&ZRDb)Z~i);QUk6uX7w78<D;9_E`rsb`4SG<q8v0V2)0?R=GYFPRfpM(u8=w15963zM~858Ng05l_Wf~J>oxI<^q'
 'sLctQ(W{B583Kyv!V<nCc@w=27i~cKp60%8w!#Kja#5~6?l&Z4G>YCd&9$igsix2i2!Oq1lbWO)ULkd_R~tW6oA8Xg@*&3)$a{{vPmv?uU%;_LVswfU1cI<_Rbdgy0J#|4'
 '@-NKIa(-h{Wu{0T!nUpM<K}y4<tmP#I)paNeMQ+;@4nRT32zK~;9{*aFTmVg6jpHrrEZYG6*ho;#|yb3Q3{%e8qK}(o<m5Cujrj;hTsf42Y#}~6yAeYl={tbB9jEZ*lP>x'
 '>w{RSDeh4A+ag};Rd`IJ9qX_M!sxHj8h6m&vCO3_KiEcJoRlj0mvJ_5z_lN3@zLkQp=}4Mub_#X+6~Y%u5EsjS3~Z1M%7ig`Tnmbn-St%*(vEc;v@5fcSv-)oq_~ALapyW'
 'oASUnBudnFq#b=yi;J6DHyjX^mOEhv8FTo8mSF0TZ)7)AX+N?XjVF0u>WB2f4Gn`EYCZBBZJj!}p_OiZpcyfpFT*H!0UDk643W~nGJrWMg$l~3RGVA3`mg~m<Lz#)n%?06'
 'E#^LbMO_VN3Gf}ine&!-j{a$fO5@COaePIouSw1)4jgWSfiY^9-%oD&#CoXhi_PMIgLmwW-P}R@>X{dv%vt%S@~~E8oh@VNiRf~hQlxx4y?j@kS6OStXQb?vx5FQh8o+>3'
 'Ma({}>2@Sx#_;4_amGYC_=&gG$nfETqQ(t}&UfW@M8K!SJ6t|@7;rceIt<ks2H7bV#x{P`(UW1b9GcFfML$<ZzuSOT<{+EzJW!Euo(a1Hq7*tGPL&q4mCB9x<AbfSXmUgk'
 'ZywgE0|<^3592M^q27f1YrHEAt)@s6e%tCfcRB0^k)`{=E6j_E(60xvvjKP_j>wpmCK0V%qLfEG!TpK048y7siveavY@SeTb$rQ%6S8kjY?@BlnHJns?#3n9iP8Ir9N9RY'
 '<n7zR(peLXaz56s<t)#rNmS-zZ3;2OY-ScZJm8*W9H@j$vU{?wev?}JIo)I1UfO|TSb2JSUA@0eu0Bc^D%f_Ydkfm04vf5T#%_J2Q7VZQx^cHvFwbKrCH5eP@^-)+a&;)b'
 'A<?Ng7`gGnwqej6Qzt44r5(wHtRi4KP;cH=5u0(K@|A=^s@twyh>zr8S0Fmy=>yd#W`Jj3MNkpJLz$)4rwV~>YT3;3_{E!;foM{JD=aZATgR5cQ}!bxAr<O|)wn^8=9K6A'
 'B2NU(CxMxu9b&s}UZqp*NMocdZ-0=;U$FsHW-v7y@MBGJuShX)bhx<NhPFZUbGGH14cL)ih2rqmE3=syg;xTOBZ-rF>d;9zg7VN=K8rBIjP%T*gHahw+O{*M#SFcRzqZ)U'
 'IBPw5Z#aZ#XT!3L$tmH3eAnvQYO=cm$qYlK*lrCKql_4(vX~2*5i5?MhTNY?oLA|q8G66ot!^+hhvW)Mbh7iRec+JHPeP@X2QF(~#b+REJys3mM-1L~n7zOdysn~G_#*7m'
 '%=lm<SPqWp3ZZ)qNpSvXINU3>LKidf7vF$vKr}&b9u+FTkMIpe%tRTF>&^!k<O~usMP215ZkjeAMrHT%#2U#|gOvty)zf7;1)~X`EwTp=RBPwEh3H@GASf+282JVvuZH_i'
 '^!y;JR1_Rp!T#i&TdW|U+6sadW-Zj!Fup^$VA>Cq>8JY`e5@LSn839o(ry)WR0dufX3>hoLnY*Gam<J8hH@79yr2vqJu}vey|d&Y8WYehcErx!k2xi|7i?oTjd(!j5ld(D'
 'Eg}ZsWeaFMN0_eB*g$CxOW)EW9yYM9o^c(fn9RRtxp15};u)1wy{R7<n;ii3-qhB~f+en8bcmd{-WoUUWchZyf~-S%15!TIm*xeypWb8xqIqxqu!Jhl$i%abi`q4}{2N`E'
 'H<n&sQ03bR7wrbDlxDkZnnXnrlz`Wc3X%ZRQw^3I5}kc%1lgjF&jiQ5EqodZdQhM41<w5&j{XcpsT`@H1VkIspMi0C+GK;FvmnSLF)yo#&xjdWFXmIsM(fH>_2O)xbth!T'
 'C8D;yj>J3O!A5!~f)>-;UYN#yd8O2c<c5d`w;mZWJx~GT%e{wEF|2&O=hu2Pd_>AtdAsKe4J=D`?nrLeC&M%xd?>$c@YlSJD=gC4y+M@b*X(|Lq@T~X_g+KP4X#hrXdba4'
 '%_Yo8S2E<WpzjZdKSVbW^T*`s1LiQONxPQNA(S~08Y>b?AJYgkJmQ#!(&eN?&1aGhPgcO>N2)wj4fY3aBELiIlcP@(Vt?Qi6iAHUKPGUvzyfZtK&LQ+upq2ed8D|^@y1zl'
 '!8#X=owkReondnJ9Sk;b<nOu79&V78Z)ueIdDhiV_;`n=M<pdsSQ8^sHYcpzM>wu3UetlY`zPA@uGI~d?hFEu!-cRW{y*Q<8|v#;Aj;nTu+|Ssj$vL6w~r&-0U@IV4YD2Y'
 'kH@>-smeH%h8%+2O%c}Qi0_6uUC1*Ez0&^cbOTpCk`5mfn^>+ke0X$7&oB>OLVTzPWW%gd4Fs8RDnrJH{6GXP;Z%@E_^qo1PgE-g<@~<<(J1#`XTW@WO@Br~I>0nw_K)is'
 'emW40Z3Jh*GN#dko#x1I;ggo-KNfB~lHfxxB)h)zdc&cy9&sIGZe)>Ljjx)nBg_BL!?Y=hEw98-=41Tsp%nZNnEU203gh4pYQ7jf1Ku(P8y7VXIDO=N2y(16VW{Y#cMsX#'
 'WrKBjMoapkE3`g*q4iOiw_aE20gi+Xhfe4)$g9P|>u~a>lD94zXBNB2)*8CtRRwL9ZQB6J8`$4%-{xFxICP3Kig4oEMg`B97XY<+fx!%=yy_mP<<3t+C1fiWHmozGB-OaM'
 '@M{C2d?CZ|g^Z_MJpN_KRy&AWHOZu+1pNycM#_z#;G$sXBL@%Zx4WVq=0mKb4Iqp(9lIAKEx8aIM}%xCAor@KzatllW&WrRz8xtLiiPS|PR_|Ff*SBhVf-_Ed>?`vLUwWj'
 'JgyXiaa?+gFEAK4jxq-uwa~;&tBgu_X|jG+jN)QDed)GwXZcB_7P>gHBkKu11z9(;BNm>Jfu5YvyW@)~m1gEY2I&g?UK#6WgtExVNQcqR6h<cN+!8-CqA~WP#%E_(wD^OL'
 'CvG|84T>#uX_ZcsE6ZVOJlXQ9df@59w4n0J4aDIF;v-{f3XZQJOJ0|W5-;<m4*93(WNTPH8;4?k!XXzx3vvcGQVnpAP<EFmc|BxV%<}NZ%f$>%FJ>=xkCat=ce*nJMp6TY'
 '0pM_2Sg@u$Jx6QNd-O=|3N7)vdUVwWPH7vXNqoCLuK&77vvKFu8wlqKXJa|$-?a5K%;?V-=z~broL4(%{^eP$ibIi5NuQ0PpR%l?=@F{75+`)Tp3D-nEy|DcB9Ce}B>@yJ'
 'kJIW}=0ao6%#?jI%MQT$5ZKNZa#m2JIzLfnR!FO$@DusXd1A@ITmV`iIi>h%bdo0V^CKrIlKLb?f5?Wnko|!@;LOP~Kl@nNp=_3PWH2B%t->=jRRxGr%z4QCcbM-h(kqWN'
 'YV6X~>U9=uuRXenJ);36w$7*j;4n=~UQubRI<%u++a1v1wndl=6ph*b7kqDC<d=4FL#45cuu>gJ`U5Qy!cA>4<w2@R4eRCf;lith9C}rHjrYaGi>%|~LHZax&6WdKP{t@X'
 'u&=3XH?U69V0*vJ!)KO7f(${-I4^(koY`lQxw8QcEw5>e5yNbnsDRR>oC`u(&HGK%A0dbF%N86HVH#C3eVk@gM!5l*inXjhLB(1g<&|$L%B_uMH9mPb<wrWDz^xo)K-)kr'
 '-1WPpEO!0vV7hO;g3_H$c%0<%xq%MN{pAkW215fG*R-QR0hw0AVcLzrmtq%L6Jl^NS_jju_7#=pIK@I1Fxml)O?z?(C=H{xDO4hdcD{_i;!^KHx_AL>=idn_br&EK1xBP`'
 'fo5~+Rd$M37uOS17>iU^{R4CX6^Kzu9pu{3w2FuB5NRp}ITMimB6jPdo39kDxRiT9`H;eP9HMZ_GP1SE7@c$Gf>So5@;c2KZy*_cf6m!}0k#USEw48d4t}=*D=<zi$_VYm'
 'Q^A6IYr&vl6To?Qg#~RGjK(@-Mb50edPW1hxbbW-<%iSnYCxOR>bGyf7dt?Cd~;p}^T2?u3PktnqTIExtR|Wsy1mi4z&Niu&dO|sY|XYRup?YnMN(0CHQ;`AKwfo{Lv|tS'
 'Uc~`Q$WEYVtgE-*;MFIw8ghRo;%@ck_M6?4PG7$K*?2(JU<PyWpl`AmJ0PS2-OMcb1bvoK?|NBZIfnH~N_Ec1yQCX{r#WeQfmtF8>ny@@zVPhm^c^alra%xn$@8i>WaDz7'
 '2Mu%Q2C5%U)YaSTxayNg4>^n=BRmFKdAOhviU8{BV~~;bki!V_S%w8)?}5P-Ji-+wu(|0RzKzY$g?0x=w(0)tuO-4?Ty2lcCfa%}I(5FM9Yp0<aCAD_4Y*#=*`_O&@P6yW'
 '=6Ay7*K9?FWwqadADx}KlJI6$p&bs*(fN_fIAG4Jw|6SkC#kGBmdQCJK!&cjeRcQv4hUF{oSX404QT9+Q)n_-iBUZG=mRkL5T=h#Y?*OKLeGuYBL44MoG0_=o5)a)y+5X)'
 'uF~B_Iy)S?_p+h2Z84KnWO--`b&-Ry5ZQ<5C*#Wu$tor81+KEXN;fls)CU<2Vrr*e91xQ*N4?D#lJPF$`Wqlj$C??GSL25Pe(dEtFPAtetF^04qwrR)t6&~@D>odPw=$!O'
 'vXq0An%YHeCVId{Nf4}G9F|X$xGSC0s)?#VH-FU2#}EzL`*&WJ^UEm|aIq^G(4m{kv{g`~#2;i+PQs;@;I90fN8SO^X$@FmeqUY<5{ckmYrG8*-4a;PVp^iTLT$imrugNL'
 'x{-BhBJkT{eNvXsuOCH_4^UdqvPTReg7VO$))DC-wm0X!>w{2<I1UlVuh>myR2ichGZ~=}1k>PWL-&vMt}vViamHjt#OQm2Pp;2=XB@1AY!A(qt&r{E<nMb^@TR6PCL&LA'
 'l<i<?jQ09jF{jJ|L&)X!BR>e#LKiI_w{vTRx_)Cq0AoES^21<f>iR1ttAy`lW5Vrl)0;}3D-cv-jzTS8T@Cje6K*G3j}Yeze3Fy|57fy0JfQ<iDZiF}vkN>)`mlqgNu_W*'
 '7Pi?!Gb2*yQ;i`S$iLBDp)^S9uK;PN=%jH6-$6t`ALfrxZd1g=4X{8Jl2@(b?g}CG4V9mPo%uqAkX5`P6=NJ7sm9-aVb4n1hYE>@J^(*qXc~TrGsLo?$xSzWLak~Ni=3kf'
 '-r~E-?B|<sZaL?NI+8pz6tz72Z^H^ucqOU|wu_lD6fwp*)`}e{jgs3n7bEG6Z3m&=<-yoev#QV;D-UPQt0HaEm9M2$j%HN?en!OKA(m=VGU9O7v4U*;`)`M3<Lh+F21}{e'
 'ET?KEq2J&r+LqaUDE1&nowlV56MPn%?tcsha1ktCvVousLhb5eI$7xPba30X_dgks)nNy7u==g=P48~}YL4t35S^9-$nmEYCgt5=TJl6YotSxa-sDxdz1(ekn>1JUwk4Zu'
 'F*^<W0NNOqFeo)j0GQdBIb%{<^M*UQfsf+M`2DRiguqM0AN>+k1hUgvTH75gCT)P_GB~?B$o7FtAlvq|4Rr>?p(Lcm9C`!<m(K*+At5zt`P9UyX{A8`vW}e#-V$G-DM5XL'
 'Vw4LI@+v=&1zjN{=^+o4JD%19;mgze2aTWXAK0PNsJdJ4Mx~Q;Tjl$!fwxB~L3=qrlvkg)ta7E1T%jrXqcnmH<}t{qhwQ(Ee0>W+#^KbEMF+*TF1pY;ZUGn5w;Y<04LCPl'
 'rt0*!-x;EjU&pl(csL}DU%`;wCO}h&bJTG=P|8;`3}4M~I>QiOWQe@+!CCNzu27($eodRfJPmw+)BHp_Lzz8bj3!Z4vAy7>O{kPFtvKL!TZIQMKb>;E@A^@+BaK0K+Jl#p'
 '{RY6HkH*CiFUa<1R7#y`j@xzvQ7X*7j1sS+P&rzDEjp7T{k$5VNU!8aSv6(^88e?=)?(`RjnR2Vw**47oFEmHQQ~J=%YAz@Dm42VkBQzP1*7QwC0_t5wLA~LXK@34%h-GY'
 '<RgJ0=765i^9A2PlO-8$V9*ii+nHOpd0-g_C?Q9=<-=>p?`MZ0VQ0kp2yn92l9tDn&Edqd*u>nkxLuGgh1eLLM&BGDL-bytN~){j_5leyAR33umue>obY^TDdW@;`Ffv87'
 'NaU;ooWHB}Z4uUB3n7uO?cLhL7V>I58`VQ@@f8u!I~(1Wq5F|ZW`~K?_@yjBo<L~Edb4<1%ell5oXP6XQ-oVPP|7c0&^{3gFF2XoYi>6nbP;H}nR4hS$Wn9>Rwuwdkm1@e'
 'D{P_y@*_RK-$4$tdmGwYC80EtQXD~v{&smWt-tPCE5oz8od63Xz;>$-B<HqAX{su=iK~+M!AbAswP~Y_!ztzL<p@q1{CbCygIC6I>-<jf%!y@?_E5^acv{f{!ul&PUT|Nr'
 '!P1>N1hQ#~rG)r|*gW{%pWvsvR5K`*or!JBSm!G^o$81t28@Z6-E$f>c%7Ze@Ibc{0Qr@TZR96dOc!u#jWdD$l{-oB+&4R6EANDEXRh1<(YOU`Tx6VJ=5y3UXN7bCXxwsS'
 'Sqq+)e=gYq9SY#iwg5sqK;p$lX)MIf1zyk_j^#JAn{|OVQ0nghgjb-@{GV>XCwgc!gu!y+#hcBdZX3`kJ_&Q~u}lf<gn^e&d)^^2I;jtC#qyB4a}Ga91it~TLv+URw0oRU'
 'DbIk$H)uByd*vCI*YL#Fff*tWa^p@c(>OraUpZ0dy5Z2hoH(eIifd&Dp4IS>wY7B{0sLgXJ!4WHuMNNs{3i+<@{{Hv>n4R=b4Bni=CPLO69O}h7Wq*c%t?|UR{du=pIumA'
 'U_S~BQX<A7;&qkooo7~`q^bcda)kcjSXTM&8EiWoilec7HVic*K3I<Xsf<G%k!$|)s!+6eZ9cIAQ3^bWzDSuwciwd7tU+W1i(F%%3Hw)2p-$=`U!<4SCv0|kl+k0B&lzw%'
 '+JQFmw81Y?bRcwKBVDQTTK<GG7D;G^N~^ZVw|afxS%HAcE~1>O%c~D~uLXuvidk+_D68QS#LCu${85lKydeqb2bR{l2qymu7H0Wa3Mi#wlg#z_IT1>DL(84N4GsbUU-^on'
 'k^-{Kcmm-S&8zr;=d$6@yrD2E)hR=Df}(6BNq)@_g?aN1>Z+YGY01j)eeX=Yx*F|sveXDM$Sh%Lm3r!a!yyVv9{&8%C>D+8RWw@6Gr1r5-~=A3c4kDBUPpv4obriMg$hLX'
 'KH_@eDS~!~hT{I<3`Af5)WMpQa>rwh!RZ!I)bOVL_)ZN+<po9lD9C28KVInaV}IkQ)gcZe#AKw|&H}q*q(F0R&jphTKCXe26y>k0=S(xkSf2`^9a9?6Go4r|s7Sd1D>C(#'
 'RXZ+=y2iR}Kooh-@>wTK-Ae<V_Ou(zX&<rt_H@+_R0PTCuQZ6YuF@HC$m}qr9S)<CI>eE#=jz8HW_$$`EmHaIkuTXo)F!xalprswPtdfLM}h7Q&AfS%s$_D%+h=D~96D7&'
 'mV=?ZY9B}>W_Dn;ZVXcS>nc2OcA`3wp9TOOYcCtb1P2pyTiH_u0xB^R2n3q6&N?B9{##A6u99*>aUL~fNXO;{HQ>vrST+<8m4Hb&Tk0xyt5h#E7&{!N27@|7&|1v?ki6&*'
 '_5_TE5N$Ay5pn8Gte8kCa;c@9wgEoprIf}pBltu&Xu(`n7*ydYwH>5lv$|?;wu1FRs6&h>loo8OXLB1Cw<N;u?>mC{tv;@z3d#*g)vMd;*?2&FSr<KQWQ02+qTgm`?@*oE'
 '^!2l3MqdHJ>BT+0$T3?V2nekKJ9mp;K`FlhlIr^EYPfq&PkoZpK*o}W?L*G1_5sLPlC)~b-I~L?8pl=I_*(1skRf%*nP_OzdD+1t#WD3UE4bN0n#yX%)nn<H0%JV+_4nL}'
 'n;j$_d_bY?EU(h!J(e0X;GvOejM)w`+tbuIy_^e+WlRHO(1MLqdGh*Hqf>eaLL^jI`LXhZ{7A342+ALgBkav^vzs!Yo0z`A?1aNKuYQ*8UCnT|0|F`?7e^x1ReyVkp`C%0'
 'KN@7fRC)r>y37-YUtFx=@=vhaP@bu?2!nD1Br_-GRjawJwbA-S`)mR)Zlk^KB_$=FV#R8PJ2@Rujy@&Mm#=|w*^-^TY1ww5$-K}uK}6Q3bis#r*L~7^Xo^be`K;_a0(q6k'
 '_gnJ=?shmdr$4pank{CQ?#K^C<e~ma>}QiUCqEUHA`e2b+_u`8;C=N?#SVy3$vh{VB|X9J_=jtoZ+9?sYJw~Cy(H(RpYEot;tq&WgO=?%Vy`v+`f#X^S<q*6ayUfg3ia+5'
 'tQ`=gas)Z-<hJJdMpKPJ8Fo@Bg%kBT_L_rlu^UZI-cZgY(V1pB!|A<px6}3q(6lm`YR>TwSS@JVBf7sq(isSAL6gwuQ&;2oHC#Nt5CU<uz}CULvi9fH`36e!N^<gAkmL=-'
 '9;?RXRZTl2I@F^)Gizv)E-~1wPc-MrEG$Fr(t)+iPQlUkhYi^)Hh`L14=Qs(2F5$B#s`cyE3Y{7N8@3za05A|ct4|?8^ZYwh*9Y_nUQXjxqp0JeUM7E?J%3XuIjlcen)Ke'
 '$z<EI9kqz*6fwP0wG}ThFB_MLzx^fnAt>>qP>DjFfhBibgV*P11?~m@tZlWEJ8r6fVtAWB%fRzTy=}>N2k)hAdxY98#9okaV=mUVB(OvA52D|7wG%D$3&tkptqy+kV2UVs'
 'CRyGv#(a%GxB($8=zeYCKuNC7#oD%HV5Au+F$vezCqlCPB%_1e+^8NPJE5ER4#>^D?>S^imkD}d!Y#U-foOEuwbVAPoSnqDd+D+6TJ|$p%pdIb9mMW5FQ0gVfTOp8t&P{&'
 '>x|ARH{#{QpDX<_N{-AQsa<w3blL(au&CBmxIb6vN+6{P-d&rDZ6SyIM_X4IUZ)ojWxv%4JxEt`r&b_AE9NMdK)UrJn4bf!)eSWALGIR!xWC=Q&M2%-HJW?gPbr_NMxyR^'
 '1ku0}&TzEW&(Q<+#5mjl3^&l_Z@J-ZZ06k$5g#YCwuoJ?TA)ekq<;EDHJ<))CeB}cY^XG)1HQBv_2UA+=3Hr9?~u}$bq)rT2{@lF@oneRcneVu%e+uLdntQ&MWKqt{5rfY'
 'ih$D;y>Qmtn7Y)1#mE5OziEtrz6s8tboSh@T|X>u;2SosiQJ_xeNHNwBaV~=@r#pw3Ua~$YRCgAr@CHqT9}O6i@E%cXeW=Oj7ckTn+_b3;IHljnVa3g4TjDb<|M5InasiC'
 '=@X>0v9bP-?FIOc8SEkch9%;FKT|uO)BLeFHUy;AxO5sDzm1YJDx=dG0y(Q0+A(0anQ{VURx^Jz%78(lF;F{+dKv37$61O}CNQr?ZGFZ>5$}dWbHuM)-e_dZ;lRmx;~nLO'
 'M28xze4icKa!H|J%9ddZ)-$o4%nN2ntkV;Saw;XPPI5eNsRAmN1T4&Y=AqQYs?g)$4lT3=vv;Mg@`F%`*bNc;`zZ=53@zdyBhIX{ry7FjkQdEPrM$GO9i6}*l=$QNE`xB~'
 '=T*F)&t->1bI8ZiLnonzHHN-D&9=c%LUwZU`-28p7(5eT(GyI()e*O$VpI5?HV+=#p;e!Xt{>#~vs;o;Se<64mzYHzL4TUK$CnT2OmNXUi^ji8b$?I4ZLoAY)08>vx9~|g'
 'd6zlNFf<2d<(Ta{DP^@!v`~FPczF}*km58AeUAT`I?T7S%*$uJyaPONG6}V8HgyB!-$XcFUs_Qqcfew5rLOYb&TZ9CMK&dUR=(=Vt7vY&>ZveDC1x-JT^<!WQFHDa+F;4$'
 'U&}v}Z)(A0b#E#dG0ZHhw~x-LPXaY$BpGry(7XvUq7E|3xAb|na|m5+!3zwpM2w>|2fJN4jh~XHBQRJKnJC6M>8BJ-th6L}9K_MVuP#-esQRuzKnL22Nvpj200n*pVswf^'
 'MwkjkD-EVQ5FOsu<5GcW&PI^g!s}|hd63@@#y5~zhP-cHyRCe;0wERXY$7E&14tbK7l)M<iB8mm>hw>R)Vr)TsLRpQN5e=ta#Tk5>vM*oRE>BbE^Q&lL)6w&4SEG*)7&q%'
 '-eDv&ef3vS>C^;KY!`-&OXnHoUe0x0hj99=l^wpk%1<aaTBW~-Yp^Aqih0%FKWC~wOGUO(h(n!>nonOzev#Sz*_eoaCGo3|wb~6>@y<1I>va<+ZOVQ>?lJhuoPEJWO05~f'
 'H}-xG!mkm#p`N?bJUzV}d1m8nSnOd$;>oOi0cF(q#gGBl0B?p}`wPIjEEN|2Lj(*`Rl%jq0|iG=zLg*|5~fvm%1F4pmC+L$Mr#Q|^s_yd@IiMp-hdM;+w$reJ>fO)Ws}Dq'
 'sR_z;VQJMKIyG;rSFb>HVs0qg8qj%FIbZ#E?z{3NuheZi&Pr`DA3Kq6d>@-B!4hWP$V6?=^X9^~dB&g3FBgqRaGW~m>OkHf-S8bIQseKsDZKHHntOY6c8Jg3#wt=H1NQfy'
 '>)V4&R3Z+-tc<z}<4ETU|8<8%r`RA}ALLcr<9hhG09n+T%k7zm$qSH*N^w+Hj3CM#>?tw)JahIAhf?YT?Z)+W^iOCv=(Rf!xg<eP`jo5JrXF@MK&2Sy1xGcnhSUCAdZi)_'
 'ht`4h4m{(G+gdTPQsP12DzB^kBWHJ`UB}WczwLJ8^z()G4wdE$7{q3I)lGM7t1xSWvmU4eWZTl634V7lh=CJbDR|>156uioi{8t|e4Fxd_r*d55_Fis2t{{X2x5)8vi|3b'
 'g$l%}&KztDcv{dwF_hj}<8ZMm>7RgeA$-LIN|{F?1yNUT?<VFBUFr$sK94YO%&P9@l7SrzjnADy$x=r16-nb{GcI@dXGh{j5Qqr!DxbD_%~zG|a46-?0;64C^@_<-6l?E@'
 'i!M$c_ZgB`N_=%6MTxtfOW1Z|>?_!ux3lin21+A-Y8hk>8=%2puf0t)76L|e%dg7G|GQ13GP*+{e=4)6`e6jEXJ@!Q{j&pNR5AyN6Q{1;%|cWk1UfMT%f~&HxbODJCqKyP'
 '5r+|CFptPsMR3d;vnvRZLtb@VRlfAB++b)z1(GYLwpD;9JXccIPVvU58AXdhBIaGJV9{6tHYPfUVC-Cyeuqk9KDafHhaIp$33l<^qxvLJx!&CJX0@23#NJ(lN_xd$V*oON'
 '0T8#Ie|~k--zvS~lCRWnf#ofP9{=R!?<coe>mlT(ZnJ?5=KgWZ^+75Rc0iYzS6y!iS?i7<5-bjhkJcNH7r+OlJHl<|wUdZA9dSg9j$0cE(GZ1?O4uKiJcA;YZ8_P1JMEa~'
 '7dVK3-h%T!c$f|+zT7|<sXU@$0|u-^j_Z%>6)t_}ws6bOGg2UN!W6&B6wf-lX9EZ>H;@_|V~6QNNn<XzF;<@~{KyA(UQ&NLRhQm`1(Cd)k0A1cQ7LqgW;NE;+ZzM6CqdLn'
 '9Y`p-+ExJ{$X;a}RtY)EQO~w|H*<Y`lvAn(qCmHwS4p}Wys)-lheKnuZ9BK1gBfnW=eF%^g^_09@crupzT^Ev{rlH|>OhX;i>V8^&FZ6!R+&l8*srVlD1J`Pq|DN1P-DpM'
 'tfdvKsXxgVxn={P-GD!QKkWv5Pf-K|G83F_K$I3F%6^kuD{Fb2iLft<92TeBxYQQPGhNUXk=B|-*&%Udp2Qh}l3U<g>%$WJkg?O(uNjmo>Tz;2D;J(iJP#T?VVWwzhB!o;'
 '^|@WQdq;xrWXqUW@$Mn+{HBl97)DpTseTq~TfP63cdb`9#EdL-ZP(TCggdnXQG&K3-R{pgXmy({L*h+zkqpLp-A1d{ZS-=6c7<;qh|XNg3Nu1XW~A9<dYXO1jn4Kcjh6Ve'
 '_e4r6`x3U(?J3y}hZ=LAd!w$}iH-oZpf@0z8wcNx`DsDN9wlXyA7sPcL0o@(Bs-gu-9Ra&9)*rvE+l}Ro3tC;@C}F#G&ngB{@_~dc~|ZgBl!>l#IT5c{>mp~Qr!iv^qZnd'
 '$*cW1en)r`UGUW_M+nnL=Y~r81q3+`T30(?KuZ@7%g1pMzgZka1J_lkWOShXDt_3=<rm3cA<k?HqVDtk{l*mrDG^&a1)h02;St1Enf3PT?WYruqRf;Aw|lc24&6zFQLbC^'
 '?HH}wQWN<}D%UE=9NKx+_PV;8!_#uCf0u}J_UsH=0__c<Xvg#F$zWf6x%v2E!=c1HC{50*{(c8M=*y8qTX5VW_R<~ZmC4vpSe|r%!y&=Y08ZQzaDS^mze!g$5qyyQTk|TN'
 'PG-O4J|vX>R+XS3jC8p_9^-~`pV0aVL(Flc&3KIQ`koUpI_t}Ci*$w1qguPuOcJStI+WtzXS6#bA(h$7q!*&t@j2AEP8im`let}3MC~3JF;;OX)dL52l%Y+F-}z2rRK{a*'
 'B}$Y`Uo4y<Cj(}AiDsN31J-zk$@#JsR8ZnaIf#iu0p9g$5?k(-5oAEwTm9gT9Utm^yjA!PGN961jb(U8-|*$t6N*fHZIeeF`$bzk&o>-xuryXMu6E9eQcuYqe>&#?(FL=p'
 'JNZ=h=r2&4gv#o^gS-KfHh>Zj)NTXD`P$9s!T5Oxe1oNl(_(3ImDDJG2c*As_&GJZ;ZW#J<>2k=P2smaPxk~0#<)f5%R6BGW$B)RPNP2=s^op8RXQ!Uo{uF83RD6nF~3{@'
 'pVr^`Meg20@X^HxID0WzK^dLuI59>ptKsY}8SUcehRB{=u*Ue=#7ae_+d=Bhkd+^-<k4K(e_U`V)jXY_MuzO90l-(5hF3mgg{zxLL|GtnuB)V-^ZIk8X@#M)is(mD?sp1a'
 'JQ{0zl8S#J(VoKSZOmhNZzlTL;n2K)FzRzCw4nBZhytKZM9Feq2_~e7BGVa~c<yK|{s}LhVhEmyryZ?=DtxnnzM??;vfg$X`zgtxrsNk=1nqmX+FO=TbPg~YTHIXd)%|?K'
 '548iO-hdSEC$c)|jqz&L2ApB`-T_25@d~Z+y5K(}l<H<PA*L|;b%#IAvoV<n$Oh|XL%aeiDDlfRsgS7=_*DMRS?8DF&M%r|U|Rmz^|oaEQVB)o^X!nlG%rAOyaY)3OK@>o'
 '|2IsE*eLT~ipSGNz%r~NTNK5F+I{b6y6m%p(!9Xfa%Qy~5c{om6G|<+0MRe*dRX+w&F36pB~n3EkKQN9b_-$x?_h(-M*>64!PqcKUQ;lU?iEP>0Lrhh`+}lw%IY+fGZT={'
 '$!GD!i_M7!kkWq`gVSE+R~yLP2Q0j{xd8q4YO{)iRBvk2SlwXG^ECPHcq2bZaYJVd@1f~h_ieSqc-gW?2B?3kPx)&uU<V3x8*oDLXI+gq-#ym{o#%Y*n?F(%x$m(8UQA%K'
 '!=aS2Kg<wmF^5wFKL0k_4!h<lW-n{TcbI=UW<<n-Vx;?U76AjD4sya5#mH!JiSF#;tn-}7u8$=({(kPl7UIl~r`5J4hZrdu<n(LQBB9n8AXUD|Zj79RFTm*z&BfPU;yo7^'
 'Z*k+&o%;q<UB8+Q*xnG$8;s#Mh~vnx<W+U@=tqxy_b82J$<dQ!S+qR))<?FU+!B`JB%jp(&)d}?Ij-{B9d#iI5bS?rZ44$3L0AdKJ##bf75U1?Y9WMvjw&XVok5vfrLL|f'
 'Osx+B)y~Y<OzF(TGJ9xs9sdaks6c~6J62cSIQ&e1WDb&y7?9E3@d`ky9{l`W|2C$qeFc^pYR@zRn~KNnBH`FYbe?W`1^>o*HhzU5giTMYi@V6`lY&<4Q9enj41u}0FGuGB'
 '84AHiv?0vhJ`Yfelz=<v9A%lKkt#9n46j!pL5;bel5+?3-L+AV-kE6fUmL@`RY#-A&*iU5b2;kjMdm2!46w0o|Kkl$dtcaX$(EEJr54q?8dp~w)h8LH%)L;<aI9W+dL$|&'
 '=g~xPhS1Hsiy4zs!-t{7g*N_^b+=^P@nd0%g}gpgC46eT_c|N)qT?xbtwtYUyDxda8A<0RL%S80RoQuJ!xPObDCYn0A8dQ}Aco~&=Zz+{A(i3pzzf{_r&T*{RrJ<vmp#KK'
 'A_QQbDiJt67PP;CK6$e%JdvGw&vy^C>`8JAItnSh4B99%!TWG7Yh%hPrJn6+yO~j;PHi%?It^ani`hW<t50^|&}qsjx{(=0pE#bVjRTFuJhJb3HC(!pgpplfARu>)ke_fP'
 '=^G1=5|{nHO5J&GZR*srb67qd@eRKKRI%B+oyQX1I6vQBz?N$4i}K#>)?~<owVQP(ij&p7CsZ1(&-|y-A-nT*8os1$u}o=>fJL^OfuE~dtDvm%HxL9qj=CCFcU_)vXdL04'
 'DHe?=b1;`JfHwrLcN7qHLKX*Q<W<}2KWg0=FyN!z{9xR6D$KhvwoJ`Xzd#VR1kB|=y(J~{@w**riC9AF3Y{o~)k*1!!zsn=hKenc5ZxM{&a%;JwwDim+t$AIDYq=aX1$GP'
 'j^6VW=T;Sy9{ot({1p$!guK08-YkbqGqWwOE`O~!PzM=}oPf4%07tk20Wx^@WX$GjS)La;8f7~AgD$wmPjeJGYQU)U3iGb>qEm(Ta^U4Xxqt(R&X2;OACh_KE&}NUoBMS9'
 'WyiO?!WV7{205R;zru2wtqMbVtT(g~4Pr8iu{#z$Vss`z^sX~O@@qYhYV;yW*aiBNVd0355-AQ5^a@|U46Rr#Us(OFBz?D6&|VwHU=4Eq2^i*;hqOMSD-FDT@n`sE^_-~_'
 '4volng7A^Z-4nA)DiE!l2TqD>=LW7aE1ASSDiO1sKX+xE?&XQk+m5v4*li!cH{fRt`R)GI6jzqD1W;9t>ud@LQ6Os_O<p!LB&EJI!)#mhv1e93f<iCuVT02lp^V8W;fK){'
 '-CPR9%e%4-A@b%b1|(1Zj{OQjy<Z{9{%)2(j3X+WW1cESxAVvu#F{jk2(KJG^_rxMYw1MV-+*}=cDIHK_k=@d*hjh1yvolQc6D+ze^sg-*;hL*?}`I7;~<@!gB)@80$=F;'
 'LH>+GY0Z;A?1qr-r8Rfnq!tlQ17Qa21zKy4K^dPNqj!(+_<e`i%flq=s=Ji84GvN(S^&h#!~V0ty>ghOJ2=BxhHszX(y5Q~I?O37DyujIsRi(M%<xYCb%7a-VEytcE*~C!'
 '0-`a0zDxlVLA&0aXF+J?0HOqqauJ1Z2Y@cK*%?PrDhCS$$@^-iL48KftRhIxu)JJc?vn*YTyLs<TU9j@GZ{I0pI3SP)zY`~&Sb>gMn&EbbK7;iDD{`0W0ine-hBHinkP(i'
 '1`^bm#mpI(obYl*&M=%3aeU+VYY>BZ8-=}!Y4C(ZW0-lE7G|NJsktd`-e@2T1}LD7udd&piw9Vr7UuoHlXoNdFV658MQOTvhKL6?nhUL)ay$N<qRc30E#P|%-Zy}Gq!Va*'
 'C7UaBg4H^Kg9{6{Q%jZ=3UE6#YzTRHsv!3^-t7lBw}WM5!~;U<y81-mKuamDg#3hFd<7ZsEjZUe&aOU<6MDpTsOfh94)ad*fl6~G=I^2?^rtQ>2WFkn>FxDe$n=&bTX$JG'
 'fN1jVp)GD{LGxwl=v{JVWg*5+_$@{`LWxU1;7Vs8R)={MmkvS)i74ht-p&fh=&*~tb?!x#Janp4X)D!<Ie`6%gkL$>LCa#7F$URr%By~P=}uG<>Ey4*p~$1+Mqn}8z`*Gf'
 'De`pk=s=|u`Q;n<<r8Lq0S!Ua6EMAY(q>$%C5j%=TW}^a&&_nHpjf9iO|<kA|Mj?8i%i>j9}&PKha2#x$AS_+59Qwtl6=TNY`~qh;A3sqnZwU&`6I|Bf_c@gzw_h=86{$r'
 '8#D82eDYk<@{!lCvW(sKCe1GJ13rW}#5^cQkD7^p^p{vuB0Dm{TXfTm7#WjO!$%OUdNbr}VSMf5*E=-pqugl}3Iy*C_C$T^G`8cS&wvwuwuZqLA^@d9HHNqU9l2ZgXjJ;@'
 'yKs$^-OH=AdYtkJ2dUJh$#=BUhhF!^-J!OOLSs}$#QKN2=+ig9pOk89{rpvFo=0Ad(iF}USEn%%I5mw#^c?2MQcsu^I+B^^2O>vQkzfS!(Rqj4YuIT+UO*}3pE`^LLteP!'
 '9~hH%LSl4M_u`44q^_#;_LPdexAW6EvNT!gT`+k~MO{W{1a_vfsva(H`-D<Tp*N*{W6*NuA+2`%xo<Xy1qJDJJ4K@hdy^mGM=4KIZAaKi$%Ht5T8+;X8x<fyiFq%9A_?<5'
 '5!ouL%Zfvz%ey2c_dAU<=oTB@t;-pO(+L?s-h7}bLB9oXLP1KvGz1)bChq>t2Q8=E5V9NsPpjdJOU>No)5gGyAmz=6-A3$|j@WS6+$Um86MSisr?6tuZN!}8?E4>}BjE}z'
 'pb61AM2h5Q3!p0~jVm%3GIFdwk)KkJ6>(+q$-wxL`GTE&!j_;@8$Xl@pfg}&))PQ~jy4(S1(trdzK~HUjQ^m<4IV>C!$jB_=Vsfq%k&aWWnKQN#F-?%KvTBO@%7_PoK1Zf'
 'P^iRj<pU@WGceb`fCzw)tRi}6>>a-Vt2~ZyR4Mp5jX@e;<hcGvG~KyXP=&i*AUtuNeur76l1bwjx;f?B`4ln^P;S&__+1OxT~2YOzi5I<&2bLnc7)Om8x@>d<b#T~yo#6H'
 'trUEuG%|<$_G2d7D+?y&Hcdx-5aDCcaG)D+VV~q0Bl^fU=ubk$$`^=loPEs;&~@AS^qj&I42{!;yN@EPd^1&Kkq47eT<f^shZ<O+weMKHK1}Pp-1LcYc4Y1e)@7u#g6#SO'
 '{ut-6y2P-;K&8;VFdo8p29PYyY1T)Q8v9t<KzbUsAK6*!qZH)l^}7Rs^HVCKGTsL!L>Y0oO!1^Z@?PSR>OGR9HF<beRG@r;FnR%i9$lYkygx<$jiQ7ETBj&LZ+53eO>Z%c'
 'H6g<j4$xs9+COk#6y8rM2j~qTqR_ZsSDjWley+Vb0Wqp`CRcOV0q8I2O=dx}@^EYteV?H9KGd(7*ZL%-=|d+>Nvx~%3}pKjXBa~EL&*NJmeBs8K?)|i^vSQA&XncbWThGG'
 'n7_-(bCoO~a*say?%MDZ5YU3gf!t$nBI^=WVa=tu2kH&76g;`L)Tg3Q={B~0Z421;j+dJQ556-RXb5?a;Px$bUiHhf*e4(wOO0*LtpN?@5g#?fusSgV2q~Upbv-T235UjF'
 'pqxrOR<GI|D2<xH5KY`{wKp%m{rP(sD)VdpHL+>%BKmgipY6zpI7fhU^P;pV@qG^Tx&!6_qT7@hg_-ewpOXNW45ca8u-(0a4+O|A1ir(rU{p<wKR}8HPM$yomm2Tgi)9T0'
 '->32!p6Vp}n3mOf6MW#br#1m~0l8nPnO8LwL+22(vvDRkSRr{OdJ@Rg2k7r+zBvI=>bn(M9O~+NQsMe2qw>CK7{esN$!hc@IDlv@`|T5$?DUPE^dU_u=_yfF;#)E2y2~-O'
 'd9tC@!OCYS?aXboJP%lb1f8H83ibvwaIY?Q$)0QmN{$81vU=N=>volIOG++ye@p?A4L3q>5JsCkJ2l#|r|BzD2BkFJIUFVJ$4tM!-Z!T<EH=~<g3)n9@K%sDPA%Ap)WQKo'
 '1A13p@p^29yXM_~xNQlcWzEP#Tx7sGm`vjqMka8UJf;GmSb1w_>~@Bwi_+WHT(+;^zTmH)a0D|BR!Uu(8gQ&$@$qeTFT@BgdcS#u&ZC>ZNZ6Lccqn!BBdKHP1>5e|^oAi<'
 'mbu%#M#(4HS0U=_2POis2NI2#GvT@<X19lD@Cm2ngMxk}X+ZgW&#QiI*+YE}>H)J1n56XTox1!Xm?7XG6!X;8_<+fCtzR^Pj3i&z_f@#)mA8My76KXL9XZBV3H8)w+*}f6'
 'R>YNF9FpFFdxz!yZq$JTw3rcOxxY^wU8pKea%Q9|1aj;<ulgrEzx*hx(hc7RQr<xCM9#*oztntCnag7K<?!eqh<Q~_Ry*T%`1TYViWyj_SFfx1K>o?M0r!!dH`|6ruo`(4'
 'o~RT!fPhM!GprG48j~p5KhK-7wn)($u#RWDZ%?q4ufVby*?EnPU)}Ojaag6QuxvzFsL#0DbNUe65O5TD!j9Dk1`|~vMu(Y<d`Hh5$+(tXtD*osV3q;9FNTEt?4Eih!Nc)Y'
 'l(izQK9QQqPtq#|(nu+;f(W>|r>6dO)!<FPfa(sz>GmMXJd-lhB^MR2b+{RA2!Ro&@n=v*iQmfCa~5U|H(Ut;FcHXv?a1*F+*Gaq36sV#*}p56*6pjPkpHGOAreCj0A%m@'
 '1z2UF9k;RBw-*R|uerTs^=1<pQ-a%|hu8d*9;>(N7l5eDgpA>HJDzcwWB489frG-kgJPB+M67pDxUCcWl(D|R?)N)Revry4hEkIuDYe1Nk?Zt_0h}X>!8ne<ZqmI9t%q>|'
 'U#w`9;NS1UX2?+q*}at`w~$@0(_inxs;>$M5Y1H~v`wBZ=&l>vp8nPWVQOFXxEaA|+nqtNPUsEmb8EZtvIqAjJpojD;X&p`+E>Y3X3E@lUYfX_mnQw|56bVKJ={>Jk+kUh'
 '1C{nw43`BCo3|1JG35^qan@ToFlj2;GM%z~*k=l*2kHhwJ52Bj^chwMw11*zFtij?W6)8Kdx!6)Yfo8|q?4pmZgIeK-I|wlj^yoj0}8a6L`OE#4Yox$;<*R0eF1n%=IYMd'
 'C=ZCrt77D*6Z8Jr-;T-wQ56!)fZb(2CzZtb*PnQdF`tll)R!~HeLl5Rft<<DotE)?Dg|{Q(HQQG3x}v_;?D35=%77qGQl&6x`J?NP;PNg@{>@f9iyJayo!%Bti~^z662m2'
 '9H8b0Mj)SXC@?1)Q!7T^3VE<uZ>$RrjdB!Fb_kiDf$VhE^5v^x1R2a@kXZ?NH;Y|muB&7gD|3C4QkhvSrz?!T{y{E>0tj^S2Dv%7tS&FxDbG>rbTc}q0gT}?IzpQZvR%NU'
 'V}yuq`)~G;sa&<X7XZ;*msS6WhG!f)4bNS~BIYg=^MZr1B2l9DVjIXOW$-Figz_Y%a^yOfK4jQEb-tWO4(tNvPwdP@`*v?Og*h^M*khAcMo)@6BoJV*5&ZVQ0%Py9iWwcL'
 'UqlKGmesgC=3O4dO2j0_b_&t4e?Zr@+{_xteCY$GF(XWD3>g;yAl@MHFW~b~N5W<J_!j31+S!gn$Wa({QCC4f=)(MX@*FS}x5!UW9mro?$$kv8eoAf#xoN;^Ag6MB7<X6D'
 'U)U5IZTXx>4;_C{LXX1}s9&=CK~3s88bC__hcP&tU@fRrhY;=zTSDx$tcL5hEf*r^Pn>Vt%8yl54i<zc<Nt;oWTLvN00AXtmUk(BdjtB}4afG-xPS~!*riyqe^=9IEpPf!'
 '{zl^I2wR1v*{UD9(7Zyw`r-bP=>kN>!&f9bQ1AFfKIiu{==cTeASarBFG7ay47X3VW*ZIL^w)QS-rrjkAhkst^vdN`;0Lnv1qf?}Zr?bdTII%Gvb7O<^wtv@A54I4ukv*2'
 'vcl5sPn6R;fbArAAyi?u3z6dx?dIqr-e4k8ne8bS1b?tE_ueV6ttb{C^TA@;J&5+z#GtggSgFD=I>fO1TO=VyX-LK?r53HT+ZnVOmk4OlPPPr4U|kl{<nKydp?Q@rt2plw'
 'I;h%0^H+lqu{~C=x-<5xr;D%|7z_5aGbi8&P0)7#DoVxdd6nKq#PzE-3D0~!of)*>xd9cH;!Hh2_CrN=l~>!A`Xo|Hodk1UR~NVWpMJYKz*eY2Iab%(?*lT@O^`bU?Kc<n'
 'ehryN){jgt{o00XIXDK3`zODuO@Ct#uBceK7inlt`c-*1-`?tzR2C5TI9)f!Qde0!8nUUJ`RbfJfL!{6$9MbmcX`!6K}&T4LORe<wt6UZ2;5q)&U#A9`qelBY!=U7s3?LC'
 'Fj#@7=U4?;d?P-=&}nk^_(aJ5jrHtm0swb?4jpE%V(Hnxiy#zH<<<AnpZ+wr=m|Ch?W~ExBHFpG{u3(QRupH+AFGPc?0K3C0!<&4s2nVxziNjs@ILUPr#A4nx!lnP>&Tmc'
 'ud!qJyAGjM&Bi^xJ1~aGm+9GCJgSVts%1WcY<uDG+b?I|A(lqt4ure{v1&4I{_5a5DUDfLRRB~0sXVC<_SZM;nc&mR)gY@>`h(eZ1Ub#K2qNUX+FEkGzpD15@}qQ4Hnk(f'
 'ThLx=O1ty+m{Dk@Jc#O0sqS6wmQac6C}r%2iqRdaegEw-<6xzf`9jKgk?rF@W08SeV%~^$GVxb2>EQ#6B!0!rf`8<|M;pPRqj=PC%!!{h^;U;~MaxZX0Dsk4lVN}wv3Q<S'
 'o4sGC<GMUT{pvu+u@~!G1zD-rNW*Q}oppbINQBxz=n%3M+J^J0|AO;tN|mjqYdC+E1!nzyHAkh$EI(n8RZRK^DA6~iwuesW89wrPK3L+({-^p_)QLO^m4s{)mDV*{OiOhF'
 'qRB5=nHilA-dlT_t#K5rhy4?gXVu&)dC?DmVodvkJb3K+{Z6EHfY#6+@{5tXA1=wLLpQ>D3YVA|@Q7*?k((?bIKk4yORU_NajdRykgqsErz`NDLTexcz76OSk_l+NV@K&<'
 '5A(T%1C&nY+2s>ofO|3SyL-$}Ky;u*-n+c&-r}=Mcc_x;jt?4SNxfa$t|pY1oPRvspzG+>x{iYj=DrHcNlIByCtX1WS>*6Q4`9Wig6v9(v;-OO8Bf-=h1?^^;NG@w3)#)t'
 'ZS`(EfM|B(_T4BuDS6c`&y(fnpiat7zMuizufX_%*I0|VHyvVt*bfoo10qHh*=|pWpMU|L08GjRjT>HjUE9aDC`jq_m)<PHFqCk7z#!)*X(3CTho@CqJel(JD9ydsl_Z%c'
 'XZz-qUnP8hPS)ioIn@p3z&sE%;MKg1d`m|#4m~m7C3UZtI`By9PieQ+5UlK9B_pSkoDd*c;ck#orjuOZ7_v@=9JCp*iDHHW2dOlpLG_6l9w@o}_<hP!_AQ&>8)7fapuAs~'
 'GmfByjIxk3>Ha0V>nXWm$@@F-qh93xEj`)d_r3YBx5rlx7#h9K-yLWP_x9rLdhOdT!J&|YTs-ObK~y~E%*VfIUcpH)JYSaROZ&0rob+-VSzTSsLe`VxNaVQ(N78E$X>D%y'
 'pbj89%?3Bp6YQQcw_mq13{Z;MjrJn=2RNE44&`P9KT_p%Ip${|d$ls>q$Ejt(Vf!RPKARMY4<Kf7ahYLnw?RB%6_D~&p@;JvVsWz!m0ipU&N6=8<Xa0J(T}YWzaw4W9KJn'
 'RfgYsvpNC~>+_}`b4$iLGCX52JP{qs&(X>_j(k6iZ!@A6$~bDPpL|KF_93${K>B_eH_arnj9DfD<gZ7Z%oIGak_E7$l8W_t&ncSKLv>F$G@FmxAb*1yx{C3J+5SObawd=?'
 'L~}5v{qBcwX>3)A90uph$FPf4cY+K9l!(J<CqjH;CuS5%t==Gsp|ZTn+TnW>A&>)yQmY4zN&J*U{qfrzNg177Jj87}cYT?7284R-(s`JNQp79feu`|_lQlkCFh^y=&v}B3'
 'iFNP^Mmv(=a>g3{5m;lROQcNRMiVAm#JEf|SHKT#mtiY?+q;V0A5{(AA;uD9dja{%YSW*mH1!p|Cjz!%)`Wiu>H_}R!)xKchgXCV>GAXf*3XyC9k4VJhYZpRMm^7VGGn8@'
 '9sAaS2aIMr_Pz0wTtO#PM)wBsZGdX~OXybN-hT4u2U(rk0LXc=y!r^@^dP51Jm?P#d;HR@KSo3Ag^m#8pfYh^)i<Whv{4^Gbel0v;xIy(C$C-<f!e<`SJjhCaUKXR+)!_J'
 'z|zEd_7NHoMSHTXb{Lr!l;s>iG$thO22(nQVyPXz2(KX-M<KP#I_J@adDY3VjOtz=Wfu4P3J=_WqLRctf0f_%V2hZ0gNE)U?L5KI=?S1*CKrV=x#o^+ARgrcn}0~NHZisX'
 'zD5!Ue7mB3!tZbhH2&@8-oAba=Zm!uRLWn#z{TBH!9Ea&%{Yuw=<n1FFUVmxt@bU1zKDPE1Vkxk2{a!wA@G3!)|Z$E@szyk*ZU|@A5e)q6_$(`jns92X9Os=JYiRWg&1LG'
 '-M?yKPsLI$(V=+`7z2Ahec${L)bIsR_G4oSUsyMM0fT_-Qx45qHst4hl?=-&4|<d%T4`(IBN44wk{j1TPnhV9-Ks_0t;iL*c7E+psW=p#c=;~NQnr_q;L}mbWHH9=avXy9'
 '?lUHL+Nl~vDX_d!>T*9^G2wjm2G)dVHt^`8iF0=5Q)ol%eyHQ|vSHl*fu~4zy9*(vf8C_U7f?!#2O8mTC@2>>;{}EiF&Z%>kyfwk{8Jc+gWCy%IPeYfPLP}uJqh!P>S|og'
 '=2f0UO2GVf(lmf`-R33DRGLdf+g&1tG$V1=+p4H^1`=6}8<tf!?m@iCsP|@+_c23BKy<?^Wsb^cQT^>+ZN;I8r<7SE!i^`*Y;e7b*w*C&FrFcj=gp9t=?1=nN^}3oLosmt'
 '0_y`41%?taN+Z9+&cN#~8PqoAK*zxX;r4+%Zw)gjR^pFh2NZx%sxDqDe)m?Obya1|*v=k$!;C`r{@oTlqtF6od0*llOL-vKrXFT-eD&PS(#n{7*UkIsP%99P&Bk^_QVTjr'
 'Jq$N1bAFUk&>*w#WZf{hx<Rg>uu8xv7SEJbH@+6*UnD+pg~#bzae1z)JiQFMF!CGF7xQptL{7Rk)$~<2GI-!5lE~XPV|tX+N;!%(VtKXGEO+;ekP0LyLE~^YB0fO%tDdNl'
 'zbYLqyZ_OT@U&Gl4w1{BU~yWpQFY23`1<0`tL}2n1O55a*a3josEVJwzY5AK@sn^hJ646X)SZdr6OhZ>SpM$&O!^k|H-*TPpu^k6?-I0^H6<*+wVx2HRHNZBZ!ZQ9^i$Rc'
 'krJ^NdV@n5x^h)fpKgz|U19PkOho}JBCiC`!aU(nCZTkcx=B|$IGGakb}ry>LS<AME>@q`)zvAf`XHtC%Ofo$LxNTscz3Emv4)v~i4KXQ$Jv3-Cs;bOf=-|~%&Y#PU5ZXa'
 'ad==dW5k!!%E2>%gUts|V+LPkkdp<#RiH|HLojk(^6Kh|8}(6&(wVSNVUWsx>#Do%;AY115xtwMFsP43jnGk^L!DRM<(w+~v(^|NY}|Cnu)PDmYAAL7D!&!TweYVa$(xo3'
 'dT}>$;K?h-2^Q!yXpf!rt>zv%5Jnc<7z{t*(3r{ny9lz@geRhe6^K!)N*M0cM}k51QK-ipMwk(v!>rwsQ9h9JDm~VNQpH4S>?0kCy;#=GNuK&7RGvU!<zq9i+VPDa{{mTq'
 'Hf#=;cWhw(49(9q`Zo~$Ml5M6bus6Jt9T&M*+7W$4HSj$uqAsFqVf_t2xrw*eg?BHqIG<AaMqxPBPY!+g%H>|gN6_OlPMo1p;8?@w0mRHJ(}9e5V1Md9(_I`!0fFqWAaM)'
 'DD)#AtDS!2drPfPFf><ZYI8&_<ZHf}%_MpQ6pzn1H-4sq0;R~K94n&It$Y={ARnW1;Cyt(!+h|9%HgOn=_D7sA8(*%S(@0Uq%3f+UyU+k>@~A32!m2Pk$Eg<FFPzWIE3DM'
 'AR(l|&VI9vhN7R1UXYTTCLCi)yo?_%n;<unVhF8gmp*O0BV?3eIpwb)&-Eke6~blI>n87o+6jYW)In(Z8K8di8tE@W3;?v9s4m4xJ9R2&zfq+*2^DMQ9t0lvy6UxF<If$l'
 'Cm^IcnJ~&1IxBQq-5%7<Fj%)kGXbM?{L|!g)-S}|V}k69*|yAyt(fVpICM>cO~s+Ig!|+SGtdkx`7C!*)l?v?#O#C->vh#VaLVT=ks7jio3P0lzoMMOKO%2!aGUWX6GOZQ'
 'zwR<&<(YYTwHtX`7+tY#LHY!CO+pc@Mc%w}AN_XbM4rEke)k5_FuJ<>Ku*po$ItCwP5n($a^IHP_lt9$gWK*vNXBeeX!10D&L5~qWy?kKztbB$VPtfSfAy<w2-z>M&pJs*'
 '<4@(mFh&M#znXn8vmcw5i=m+X=$uD?NxE}G6^K<5D~4l$i*(6!n)0lBPWcQZe1n-J%+M>`yFVy?;Lu!eT-%u6Vh$3S)SZgqy0yxZkfYEuSXbi%9&3J**FgsR#wOoD261A;'
 'odumIAUeeWFNfAXrjx9w^{psKC*{T(wt&Uie)no}GX-S`*vfekx6_T{`Vtz~PNg3rj&iWlwREEk_Z~)Nu+qtFbK#gkuzW^u#i6orUhXN*tL`$8kje?21#(1ztTmP<3ktom'
 'tvDQn{R8;^izXxc?$doE@%$t$WR~^X?`ed1-ToJt(Rl*)5z$X;xmV7rJiaO#>&%Uh&&ZsW=6W!!Aw$N?_yYZT%Md*5pSY>|tS$6yAJhmxypKQn6uI$QXYc3fnkOKdE8<SO'
 'Z{)oRQ7VDBTTdWr*iP$L-4JrTq6-a#xM_)ZU<`r33BT3hM<-YL`0w13DadG*#XY=RPRTLZS3AX2_n(>E5>$*J=S*6EH*)c1)w!i#bMFTX&0gF&q5HG0>MD<O*SC~29yoNE'
 '8}fw)Guqx1#rqvVZbMxo%@}#vAGFI0m09g(e}MXgjOH0BJEa41@IQl-r5u;{ECz{)TGVD9?ZAeoW8n_WICML3bf3xO-}dr2WHQlP>)9A|FZR-(4;R+oZbE6elbHGxoqwC('
 'cg$2z{{}lbWaorRqc)j{7sX#epKD`bW+}A%BqQmI4bhn=5iZ#Y+@D4Eu-$RIGutl#Cqq1;v_|XDvoNo<u7bAm$aNy`$_;zq&}j>rx4OSp;^xg^SXUNs03jV{7^yhY<%Gyo'
 'EoH%$4@Z8wy*PtXsyPlyz4PkBTNJIbwfh5IgFRVV^E`H?aZhDQ1G$H{n`+P5+FY#o`jZGC=dZ~5!MYmz{9PQrLU`c5_94ol)T1!GDQk@Rf;WD`#mzWWiW1A!u5Za>Ne5j}'
 'gr+G76fkmNq?&F2nGEFwOJmq$88FqCdfaLML-+o6s-317f~82?i@5OF&-fkv@Bzw1m8Qpb?o;~$C`Bgj%<%MysRgA0E7!p0)$o9x#}aYVK_0M->@3C|cfD;oHqKfE_JPgj'
 'dEIB(W7m%nUL@BaOB*(=@<ev+3k`1yp@$ldR*o^p_D0^nYZxN%v^l$E9J8G-+lXzLWGHp_b_Z==Kn!c$IbQb!M3Fg86s{n8>17oc&$>RrP$)|Nt`t73<jLxxYd_oX3WRi^'
 'Q8b4IG(AuvQ-EmfVWubU4~#ihuLji4Pq#baP|Def$+fg9_MI>68!f0*O-ALrPJRRKO}x4N;QW;2NKruMxG1aZizCW|j1Do9=nsC<7O!!q3W;>o0VWV*wq-S}o-}#Fp)u6Z'
 '_6fXUhTVO?<XflUV;YUH-5ItYGg%u^Flo;70?-WM(*qlEf09m1VALX7Rxk6f95xTi=1+33y69hT$K|<7nKu`V<^^QZ%5#rv43-%jlQ*f*W46h)pz_KWuwuP2NKDCh8<rgf'
 'C{-qPQ-TOMt;Y4XJ3q+i5ev10fF*LeJm^z!XsQPjQ?8f2KWq)nDj{bWPKUS|fzt{ZdX}UK4qD`xCSOMrtusfJ)r2X`s6hDwg9Gzewaa<8>Z3F#ChqBCltKHqyV|<0+loYo'
 'nn1`?#hq$T%u{uIlfIc&DR(oBc%&O+z?+MJbgA6TY}go2n}gD!H~U_<b@(en^CkXUtEZ3WC=QwYRVyU#>*{jv^@QW{CYis>LTab3+KIvr-BzrRQqU&V?*>`Wd&MU$4d<L-'
 'DCHakrh)ik#2p%H`$x!VTnI=GgPET2dV&Q``wAedO6YYAH}-_!lpYB<XHQNqpxd57=y<NH{t>MSX~!uQ{XKovo;l_+y|9Tq*pSDLx=rYJNc?@S%Shs2g+kL~6_y8i@{>qY'
 'Xc~_xFHtlt*d2J?j77KLG1Y*_^e<0N@H~Oi85#&e&ryB>yzJ}WRE9Sz;~m<7$R6kVZ!4&v`vqi#;x7PuMeKLy7fwKQLKhpoVqs6dt_b(rCBj-Vi90#qWCdIWr4_mx*_o#P'
 'V9jN-;!xTal$tf_s(l8ssy}1@YQuukK;Bl-JOY`FCeRaiGD|O%p2WT3)b|vlZA0=FgJZ~_ajposf&w*ulzf)Fip%d1`8h@h7)a<XsjJJ$*Xon35^@|tM!sZBaDUs~60AcE'
 '2rf?5Z&pM~@Zu=ucra9&#jGF|1}YItJfL~i^%@@Tw<1~^(E8OVLxwl+X5E>NrJ7jdd!yX)P*+)B+578t0ZYe&8uCb)yM*vdRKw}DIlNugTgX8&9;Z|YFow9DEFL}h4yV)C'
 '4)=jdb1f!$ioj`gsNCD#U~{&CDFPW=M;8CCG}IQ4jPX06=I&TsPHR_TutsNg3R3Y<r?z;1nVM%DMi03=(k8Fkw*lqjIQ>+_hq`xRNi`shR2$+0jiMC?>I990AXQ$CPiQl0'
 '!%_#EC&`FAY@Rn$^qg?$Y+fVBH_WzW;}v^$FqIkGz-*3DZ$JBdAEc<V#1EFl{$|W{jeTCV)0t!6rd}QmYZFoXK6hhZ{<f_Z`+Nf-oqe-I^&!eJXeV%~$3lMyynbtD7+$9+'
 'Ft{ApmY;f={mVE|4|%XNuV)%<*W3Wkn>1P{1fV%i>556YH8GAP-XCGz1k%VPx|95A!13hmJ!4WzpUee(J>TJsz)7nwZs@%_VFuRyD(;kHuku$(PP6W-R|UK97r?)MFxH|C'
 '{MCFPs5If?j1lEG*zak>0toyooSrs1$MzWX`ZE{y36=5<#&JQuBx$C<G@K!NVlvLh2zO1ctMEXnO;L6j_tnm+bThL-#bI=ifdnG*crfZsR-z~&CmehW<m=gtX*jSY3i*@K'
 'PM=HP;b%G9-~{^Pn|T@PlXTG_C-+CyRlag_!CCeY&Gw0gUd3(vyPZ@vOP#)J<u^D7X@q7e1mx7TyIIMAFDv7Ic6tw3IzuTk^TrO*;VO^7DS3GhOy^)k2k~ch-hzst0|@AJ'
 'o#h;a+b_}XvLqZLq^aYyDT^_1gHMC3OhdSqZ>7Z_d$Ta#gpPla{ifp(ys_Q`-|`z^6*+4xRGa-laItVrcKYm_X)u^-3=Qv}-uT%H&l4=oUiHIUb>^E?lZ^dL4sZgZk-L{?'
 'PsD?olhzv;ubez_U=q{d{=0zlL{(76;nk1<zJz@LEh#_AC?R{<W7${n^6cD>%U6_(;{4H>QR(~_Kg`|xmg`_yD%|U{_rBEY^^Kt4JNLYH4E9u*|3IR%dtl^@^3Hn-yu3&v'
 'qp&(LXZl;oP=A?C`E3-&35U*3gOzX0ey3fyB=jY=WWckF%sx6lee3k?0NSC@t?Z)sLK&~t?HQq4ty8j#bzrxf^wx?#wfY>T0ZElB@@d|5v8@9f)rUD6`I6~NC)2R!0sevu'
 '+nt`tMoK=cn-0busEo?`<?mYg4LU7UOT4}egtVXsr+c3?Z-oz~YgIcz9>}?KnO|_P^ew@iLe3&+-y;#dy$I+_$WG|O%&Xkv;Alf_4$*I0GL1@OW9M%N=rk>v$5&xs0pD9p'
 'mjyR74y_H8sP5xV84VKa$!(p16@{wFVTW`!%b$SHgQu3292___W}0Pk{(TjU#P)RVnXgsI+oyk#^ZzJvR&w(n0L45Tw@n+uB=pkz6)ix3;HR%PtWPZSo;sFa0J~#3M=ZZ~'
 'mi4}(`r-xTT?%;F>wU|S`60+5<RGvp#)DVdUJn5wjs%7_Tb`<Irtb_H72fm$*VPE#b3kKKGT$P-$TxpIjK0F(P}h7a{Z(yNn)LAcvEc@znde+-LMYr8;&$Xu%ZC}(N9p1|'
 'p`1T`&|xrV@9n0%xh)ZbA!KKRoeWNdwoF3SnIO+EuB#7Ze1dX(asH~4cWG!l#23^1RzO<pUQXYKmI_3&Fmclrhi<bk9F4m{_FkFG4=ufGi$6-Sx072Ft#ihjz>r9!{G?YC'
 '+Id(*^kJy$;fscYnavs0s-BK{F9HU4*)_Ns^kC7&{MT$6GwsD>R8IK<?^9vh7qFMN=)_W-Gy3~y06TxDK457ssG$V#2Crb^xGv!qnvBo}G_Np=2Weh)%ddF(IiS>&LB{G>'
 'eITk83K^L0<ZtM)8qjGKAE?C&DgKGd(;ji87|k&0MFHE4La7)yh%kq?fOQ>UZG<@tOM)f9`e_wk0M?!{iF4iz0besPgoxHS;|U|MnZN4i7f@9I#gaPi8|YX}y;(dT<OD=x'
 'p?7wB@nj!S$KPq8V_RT9k}?b|$1Y_9W3v8p5bpP8#|%VwH_N|#kU{&)j<C&tvd(XAHXvD-az4}kQ$}T!QYZOx8TaV;MS_k0;Pi8pME=ZHT|to&e|(EMw7-D;zA%hPY=Q>o'
 '@m1F`9g;Xefl7mr<%Zw9>Mm<K<LM7~2HXB%YBDUvD-?2ZVdv)6^H8IH$8U+qA-mx5)&2HkhNxgtZQ}CXIK=PQyIBQ?rZ1?Hql=JjFAsNr@~}M0YvpXkBdM^o^ZHa)!Qqs0'
 '4r1~!ufoMRGy9jC*lD|1qBkMD$UG)gJG?PM4Mh&{w)q<T0KI_i-TB%ILuXCUiZ}YS8WgP&&c^zeqMaHVd%~n}yvlcl3J`!&`!cKm*{q?l$j17VbheDsPEAFnNG&GjZaKA;'
 'R-L9fW%Je35W@7&X3=M3hy|49!T~U}GkJA=&wF{0QzCXE6{E<Rzb@ij0Fe@V5F@sklia{%A)j?28Ve?F|GwrY{d|;WP*#T@NF+}!<nyebi~DOUQ^i559STnL+KY|3sLbDv'
 'B?z0hm`J8P81LuR9h}Z-&j5`Z!CybXuRckuB~c_SsXX?}Il50sbe0@Jyq=a-d_>R)x(g}nG?N44y&Ypn+A;AH?Y{+x&du6+8VCCk-B<09zvX?(bCf#}<!*?wYM(eF(d**0'
 'X^Mdy9!jhBlE@}`;x&pG4qmL^(W7#z%z+P#j6SX(1<@1J2r(Fu0yV9|Ws48}SxR+T`yU^fr!#{El~Uv&)8&`d%NIEDbW)&BVCB0VifvWZ6?~H6d&RVZc*1XqX*zvXV5ykY'
 'V5glFJ;S`}ms`dB9IKOgs;0FfVDy)?#kU-n`AMXN%woMqUPZNG`Tc^NVHl-a<gsNE<TMb2FD*HfLF15vi)OoJ&fD_dgtHHkqd6vE^gNW;>e}|O*K?c_u<xgl%WAkB6+e6U'
 'lm|IQ7P)*?2#UW}?P@-+GmxvmRTX5{34E<C=JR?zNGs;p4}9m<R736-2fkl8f*LaPW+fHK)Fbh@w^aX(<LVYxQ1ROw&=xaa&N#kHo8f(14M8_^&Kl4S+3}juwJ#t_%bYPw'
 '%7M`9gT3`I?22wIW;-$(P1JUs(Lk#<8j{CRR~OGZe4#LEz+IljvKqb!f=ry(4e$FE;4lJg9yv%FxeXDAp-zOS4iXj3Z24;Jr<w9<!x-4V9-0^_WjKP3WSpCd0iQwX40OYu'
 '*m(i}gjd%ToXH}`c0}D9V%+KW{3R(-d@&WGMLTubdZ9w)3pm+d!j=@J_X6ooe&XW{K@KUXk991J<yPUO=DCzXe1#@XyY5);56Be6j<%nKud4uK|H=MY8yGmI&tPhK3A!IL'
 '+AmB_D|2C*P+5H-tnk7CN|6Wo%80@dv;;CM?~Jrp&q$C-ur9aT!+q4iH;AK*_`2?h{=iYNK7S(mxr&|_DxEdirP|Gkl~wyhR_u&K<8$X34k$zysHNpz(+hA00xD6%;66qz'
 'Wzj09klM!bk$QuX<76ac5bk#hKV&n6ER2sXt9UtNoc@UX>rWszf50EGbjsb(Q?;+aUyMWg`L;uCU)fORG*lDU8%efAO-QdM!oZag<d(d^yMTsZFFYogYG`<YGD`dccf@&h'
 'eGrXGp^EOPk>+@o)%Zmadrr)n(VZ0(&<OJ4{MYL_s0S=>#T<yEwZpGpK%7!B5*Lu{_w_r@ngV*uxVcc<mfZfcaiTZsC8(&AI0|ivuT_6NlYYC_SYUE#qA~g@N{-iajrs*p'
 '*75$`W`*@er@Y1Qpa#~vPY8AY-6rEuN!jiR5-)hH`>MW-``5ORGZ2l(YKI=HEvcIIa<+Px)rv(IBgpq_vn^E?I>_&px-Dd<U<dJ>hM{``$02b}-y!q&VUfcZ*o!Xuf!KZZ'
 '+%DaBrhGt0F@HMGVXpk|3`-M=wL1lk5q}3rJ&v{&YJ;eZJ?x;w=&@~?+E+zOG7K%^AbV4)QAjtL6AVg!YM+$UB?En}eo<Qf`vvj<VpPgZ`^2pNe#~=;%+)X+j;Ej6QaQID'
 'Gc3)1%=31n{pj}Fw7G0xDOa=f1ITVx)m@<&SLn@-5C`~C)iG~_4;FXJUP*i=ZazCIh{_irxqio$8wrhn0UV;i=*WdM5;4wZ<qu36*TuC2CLR26J<k!ya2nj;h>odiKPS91'
 'B&(Eul&{Jz9TMhp0xy$wK*%<Qv&>XK?hBJHs_ga~otN=H(6#Ww;dCNT*{p%c{UzH%$_zTQIbXCXeFy+<s?qg@DkxvT4`uFqJ#$NB?)5Tf*@f9>#F$GfX~Db-q`kkvNUB(8'
 '%U6SZKXNTEO<!l6Aq>$nB~wp0MFRB##j1Vk_e_x#E%$rI&0fC1tLxwRgZpCQ1rAUeSI3JKsIOJ0Cw9SJyhx=rUdMr2zUup(|8-SQr#PYL8Hd+F#`ip613Az9y6^@(pfv=>'
 '@OGratTS*x(rF1=d7t{$U<nPbn@xYQo-$6~^)f0}%YUHeD3-0<MFsRiq15q)9=!wDJfh<bqwo;&Aaw4_<>k@=FmZF*!Fyvu@7DCK1K`V`G}Q}`k)xuSwEY>GCmgD%D2vRp'
 'WtG<t&U)cMC1mRJag<fxbIseXI<p;^L;*0(<-_T!!vmD&W|SeaEOUQ3M01{{>-8wFa#S)KzH|T`RP^ep6+ah^9XK?n&If~k7xP+H&=-zx<Q!zo-9fyIXe_!y9PP{;`WJOX'
 'q@TNURM{DcQcFm#V&*c0%8vPoYLtk;ZI)&Bs?SR9+wxr}2U=K|@ATmsGC{E9*jYlDj?{6>t})UY9PZL4H7!ixz}FBC$Me^I?oNA>@ekxraLIn^w)4nFHtY=&#b74`T^W*A'
 '`Z%&T*yXsqKkt%pD9sF7IZ(u{kGv}H!tI@Ai1@uor$rp3!}Hdrsso13s*sWME?hbmE>ajSLLkp%eI!P*^C^r2md3ElJosgmC3pPhjQ_!YuQ~ri>vDgBUd<}<dXUN=4`nQ<'
 'gg9l2>{am)AcJR5#<<N0Mx2iaWmuql12A%FNGKkeb;BD4oU6#UyXSny@qh&?wE-Nd4bbH*s9Ua{p=BT*O5Ms0ZwF`pRfg`xIcvZkEQNC~V3qA}VJ8rgta?ag!p1D0%zH*c'
 'Dys-X1Eb_U^XwqyM9E>md20iDYXgIR`_TH5(#*C1Zu7Oa!zlznnheW@cEaii{-`B<UxjN5CdX(gfKSt9yq%Z6x;s4Vky=@K8p2~?0=R<|cmcP$1wI_JPUa3+d}o&*>VGO3'
 'l-LhK>CAqj-{wG~^JUP>Rdu0sjN7G?lFyiRk|yYL{=Zf)<GE-9ZkC-wupu60#W)f4I#6kBW!x)uBo8vLsz<fXD0pn=XO2OWY+_;wR4(@*GYpkgi7m@(i<sBXXMN$YO2}~t'
 'IrbP&7nJ$uM`<-<6e-##Ej&SD6!o#8JpI9s5bNFADa6`}inzOd^6qPDT!bVR83J-41ZN=3x+fWuawmdZPmx&Dom7K!b8CABqQpFkV8uP=aZMIZfFY12=K_ZL)JiPjEZw$5'
 'lG_dQYJ8%EB0tJ2g>HvJkG(5%%P{Xl^v=QN&2H|Xq1pIWEctw;(BnwxWS;4Ti*3?dlzG~Ia%XK`?R2o*UkY7;C>+-UONb67V#)5=-duQ)0Mc-6OEG%(ZqhfYveF5CXG^wD'
 'w204K@+%V3N!^Rp&uNu=vrXFV84Y-fWVcJE@jSUZGp2J<{188&SiMqKeOJ|Qz3!AJ*)K4M5$5%zUIhi~RQnq6`aH^)h)E0?rd4;@DM^185v(Ijqivatn@wF6R7U>-vIz&e'
 '^bYT+NT@h;7JP7PNUL7KVoiu4^#wb>o%w`w{n!PP)kz#dtQIP(xIS=Fae&H_;84x;0Qsv5C)~U$;<yh53yx+>oXT^J(iY|tXi@4x%BWO5hrO1+{&)(9*5mJ5pHYGO4T5;?'
 'r`4!;wq~RrG$T#QccbiUK}(e+Rf41*c(x4aw_`udn|F+AUkjsHWSjBBbtaL0WEbsrb57Hr-1N1mz|vIR78#F!1Mb1<t@21vW0y!E(kk^T&)R&PXN%>JH9Vb<`_$Uu3y@ey'
 'R9`^pG&a$euRQhqC{?|m3Ut)-?f#yQ_!hOCyDj6*9^j=aIE+p^0L0W)U0v@8<R_t0%uNot#mv3o<!h7!SXp*kz8geIqkSfbSb(rjJ8*E#-=_i@&|f$wuK6G1nTC=kxpXp3'
 's)0)ef^GA$_ZCxot@kbf&mQ>DEz#;q9ZMiW#NRD;y5Eds96<*ej7XlJXZmywncI24Dh|zt#8T-l#^B&G)|pQBSBbN?(_Cx93rI4D{!E#=`T#9yfkA4-FhESw;G}aF$i~f`'
 'G9ma>7;Nlpe)DtU)bItourpyFg5ab;#ht;dj3TH3ODtez*3%_lh4ZfqCotWhFkZ(2mIL8~Es92?FyumHn?LO<%JN&@uwX(xd@sX?9v@gsAV*{g;$~qyx?tTLg<Qepl<>po'
 '9PJO&yt`zgJV<$RHsPdJ;B?uKnkFPy5_KfY(^*fz?u<(5P(nLY8T&>N`E_lLR^({aN<wA?m|*$*!50iIVi0-F%4&Q-#OXPx20S=62_&}98@sH_b4U-^i^pWTozf#Z0UDiv'
 'GC{1LfvBsO^(how#O=ypOk}WrE*)_Kb!j)COk(RN?%^0VwtCu6h2fPVm(KROx}0vJJ_(hOg@NS7{j-wCeVgTg&NR3YV1S{&3SZ<hr(_H&x}XY5wjC)$cb!)YK9coJl!m#y'
 '=nj5KtW`|z7Wk+dLFtY2M8RM%fb*O38~7xI*6Qj-0n;6!wG$GZ@dG;)`tl*!FBD2Ki#%k<Dt<;?@2-=rKooS?`Mbk2GLHyf%o}jRq0@}a=t5?Md2x9AgadSP=C`nFi&+eB'
 '-&tON0-^^UWaa3VRxMEuR!vAezT0ongwEu-g{Nb}0ZPLWgiI>P<M8e-itS0~0&_ouT34TOLUMJ20`fk&&2;k~aExPH-^XwQLJD<8xx=Ndu5Zh!k7CuKg8OAK2*z$-?eMua'
 '5gUV<<S*4t+xRMk{V`;7f(5!)aIzWMlPE~*Yhf!APzoN5yem<d57Ab|gb=Ma;8JA+ykhY`-PjLM+AS)UX&$SB&sP<P{vy~A$pf3jsB3Rg96-J9gA>XH>Z*DW@}%N`Qx&o0'
 '?|OmGD6jHm4kX1`4Tr*lO)lQ^3SPx+bbu5?4|&zD50+>%3HhsGWMld>iWaF#(AyXUp*=URihK07bXi5A6mu>ym2?~FlHO%II5Cic5IjfRG<>c@-~>vi+318?$GWO01t-Q3'
 'fSikDBh3{O)aQcoH{gW{)yFF0@`?5ph|ZAbftr#bCn0)#yFH<{R8bc5#{kV1@pUBgoj4b`Gu!2Bp;NbTO%%EnXho~gVR_IeqtMDc2x*+U%1;0enluh{CUQ1_n_><jmTB3z'
 '5Re>3wDT$sswyfd!xF_{eO~nsh+Ef_k-zF?kCudvlAFuEDh^gcmd2r+R-d@~P;vis<~p(Yc+vRfF^ubjf(H(rwL=>1MWlz0m}cVCNH-c9@fNXNUp-PEbQ|LML~!jZ%rwX('
 '*g}wUWurGgb@TYZIU?`eyvTL67zaR<9B#WaF^5O)H3t&SX0(Nt+e)Z$E)V<-XjHNG<nOY49`37X*6&B#mM;&LV-PsbtGKv~<am%SC}rEzzCoN0R$T6LWE7+WEDq~~GLTNe'
 'lFVSDC+i$ILzd<Rt{dHV!cP+Zow>uh2X<lxq6rv(i1kzG;2v=FG7i<<WgjsIlAU&i$yJ%&n~AR;Ks0804BGE~AfKFeAGH{fAp~~LB*wM54u$VufaQ$4E1ds!c~bSjp~IXa'
 '@6s*C4SM<SG=Z)yv>B;3MA-zoGB14jx#-%F?pKJNs21`RyL<01AF>&$1;ShGw1e4>D_-aiY_P<o{XK^H^y-01`36od%gC#K`3U<H5Y18J+6tFX&>gWaI3HbG9yA91Uhv<w'
 '+w0Fq*OpU^wq(Ph(8@Wyb;m8V)EM-Znn1n~>|)jrIhrwiO?A60Wxt1qSwsxxk&r@$;qO3fo(d^^g*k_Pts6<%uJ`p0AR3$D+p+L1X!pRN^!g-F%9&-y!?(R-D|G`{>3aCK'
 'drXD-Ztw0xa#w(Z3^-g8?cHNC5)4)%4g$&BzM5)-@p33+2p*{X$FC71GkP5~0}J7pAL+$wR~{MpfY0H(VN}U}WdBVB?cPX0)_Hfwqm-a`RUt`L-hGnZMIdGSqYuTYcGgYb'
 '3Jy>j_jLd6zy$UO6)!P?7}O>(q4Hm1UPWe9AE|Ov^RP`X9PM2AKQBhQ;OWPLo#XO4fzllF+)|WwUcik*I`cde^+~#dxqXPInAhb!y6nf-b^bHk&6hY;<3#3Tn)uHwFclRm'
 'wI;~LB6;;<m)gGoFIHn4;@rf`6DXt7nub}Td4W7}XYrKIiUXBe)9}u>nBygD-5tq~48!Qu1W}GP?5j94Gf#bl9zb*krI||Kg6`G=2nOZUTYa`CTF^mz*<qQ{u&$EU3%BnL'
 ';omrgtRyjneeT%axY@TMdDV6a-EOYyJAi2HMi_Qf?5oSEfGZ4Eig{4imsjxtOBcQ<<O?kyKnJteXF2fK*x4Z+jdjjL=upHA-FaL0ha2*flsixy8Un{$HhjX@4$841aLipB'
 'h&g<<0nN|d$j1OUoo?X3+XzA#QOi?I5Xv7u4Kw@MRA*4A6M7ySCgSHWx~b<Nd-dspgD6kf>ya+1pc0kaYg8q=Pmb18#7B!?8}kV*=}e0sB;KOCMNV3thS;8?Z;+#KHr@|8'
 '<3J_kAP3`*kx}c7c5JVFBgio5=ZG!4n=seWrkRxG(;s#EzcS`kRG@zWSxu5LyyW8a-fwqbq-e=A%(_z_G995$4sI0DH+7B@^|tddk}=yScxUHN&<sntP05IFkcW>51(tUP'
 '>@x}t;5vST1i*03#I#wH6N8)K=}c4wrHE->zkuxpWt*xaLyo~>*gqrA#;d-40W12p^#zt&_VOf9DvcGxf}+w0xA@<W{DQ+MA%`~-ffjOn<1Oi1b^{dZ4<5saa8A?1r_<6`'
 'l!J!7FZF%(YB0eqH;PXdep&4MS0(4X5U2Yq=lnDR%F(=YUO=G|zk8#;Z}GdyF{Vm`a^TPyl#B{wnTOu6nK#Vz7vpvfI$Fu%YbxH{8|Xs#<+~`qLF}2t#t$%TDQt@4aNDdf'
 'J%Xbr*rDD;(f3W1@aB?jZ}Y=Uw4SYV9>Ll$>4+6fUI#y!@X!y_!a=;eo{*&iA(edu7RJrx)oyY9{io871ENySayUG%3JJpVaB6mm41H6R=pkC1t!k^NltPbUOJZ3KR|bAl'
 'bYdI#AB+xfu<(gBft0_3I67594DO%Pg(fh=hCQJ5N49MuUHB=u(F>p)HP5SHmk+wAKv;>{i`Ue$>Yj0Bn&8_Ia}<MKpYtY!Wd3$1HTkOH89QVDzPjiYOiGcH5oU|PF{J~a'
 'X#J~5H0A}{5`$0F5cIQWe&htHFpLgzkln9im6p4VSlanC4i}cZ1__w^?#k^g4iyLJFgxM6#}{|%RovhCuwCjPI%aQYx8n{;ck*M)FVIT-I}7+5w{dw6>HuRDsXfc8y}DH-'
 'dfl*@z4Qsr`WuuyqtYo188KP=$@;#~0v<?o=48-u{>c*GzYCGEWI3SU{1k()ko3>Ro^=4grFFqV!)&_!+jZ4xixFJE7~x7+PB1Rdox^<ejd)#Mo|LOkx{h#R;Y04vC#tKw'
 'nr5RuiL{y_^=llfR~$XteK-<@U@@}WuKAr2;Xepn)x43v8iYPOe9=7*8<FfEnC*HW(>2Z`Svwg&KcO-@O_yqdk5#-*c{#higNysWes)Nd_y*FiAPQ8-$7+Xjx-pQaDaDrA'
 '%o@lOEZy&*ng7yik?qmQwO0$Se|#`@#+9iLwYpzGrkj(@J9>!-_s*)36A+z&PZz%^Z`^&=J_6c*`ivszAOu@*%%FW^xfvb|+FBDYc9+)G=j!*d!`dxtHkPK$zGh=#`=Q0e'
 'GbbP#&Bk^@bqiXI2i@zsWE`ZF^B8isWdEaT!P(93VVbv1=%%9OU&FV;Fd!_9)Yt5e;I=!cwVUY<HbR(9S5#PD_XZ6Sbn^z8yLxdyb;S`>h6Q);5M|8v?S67qYVlh?O??t*'
 'F~4U{TFkyzFMM}i<pji-8$i2D#*R%THvcf9BPK|BTt8XC|Me>%2;CjWDl8r(T2UC?hC~=`$W+JWt~|nskvO=`t7XC%?F}X!RrQJ$VwWkZuJTo8%a~1N%3nfb=W5PRs6e$>'
 '8_LAbH&8J4Z%2|3-baU=%F*UerG_){;y2Kz-Nc=j7J6J=E9@g$742sIu5^-S%!@gDMZ{epQ)x^b0rpq6aWjX^0r!6x=km|TQxybyco0B?tRq&MCVFR^&<Th}NvLf_Y(a;w'
 'xF_as6jWx8s07|LkTlrAd{x8sOW41nFnYj2)R1izFG$_+FL-noqjQvsie8L`zaRe8lFtnF1z}`VkiJN@{2qSJFucl7+!<*HW~G1ji2nHRk;H?AR|lHR2y|1E{L9>2C1x^e'
 '`$sS`C3$r@q4oh<8B(z=+5Q{qyc(8QeAGv&?8yGrGiPOs*<X@FrZBSyaGN_Df&A-={JR-BP-)Ca{;rdy&b>*g?W_+=gm!bw4Kf=uNfaZ#!QfXgc!BHr@|&;X?B>uWQUM?R'
 '_G_5-R=#?Hrb4}Wfu@drS!63e$*hBXb4}teko^!cKL*(;AqP2IYlU%5ZN0EJn**L(K*&WqAArlSjQR~wN|D=A&iKSZbRZ#>N!%nmK#k^+F<B=hI@D+c68yS~56L<s(H%c|'
 'M4*V;zm@DRyUyEoRN*CN7WXBt?}if*3)?V3q7mTb3=|axDiO0lq;{-!nmyi>Ir&wm?l-2c2mY;Mvx-#>ced+6@+YsZR}!aB_v~L43lE*96`TIZg0rdEol~1iyaj#Kp$C>#'
 'Jg@o(oG8~aU<@Hg*@<$ccW-s{-nE?$Q-GI~NqvhL%^UMlgE+Ssz_^HpH{((NT0b8ZJW!F!jHEaYiZbuhfV8(8^7W51^Ou_p3Y5QrK@E^=PttzEB!UG`CZ;l5_UNW#DNbJo'
 '<jO2eQHA#5_7`M_uGXpgxLx+Ay_Ex$QiG&PyT=P$1;Z0(zIpu+&X;~1sB}ju?j3OA8^otP+XIOXb&3qiij!7JOJMtZeD;8$Q<WSU6d{AeaCmd|*a1WhTC|XUP#9D}yUjwz'
 'ZG*aCnT)9;$R`ffhHFQ*FMf{^N)bd~X5!fKuweF^VRzdmg}^5IslK2SCXJf=VcIIMUY1dijkQqKu$w-hygqg1Q#=M`{C&#z`@{P?4xO*NgMu)E3~~9@B0q<9fb;BZNyibT'
 'GxRj<eNzmkV4_0`gf|#--<{|!X*u%9$UMSkR-dFYEWDcz$@5~NeD7{8;bOpafP>JjS669VMYg$$0oetQ#)rt;|1)1xcEHjU*KnF<EE>bS3Xgayyr-~riMO3l7)VA<%bWJ<'
 'b5N%#VCC2fm(o4B@W~@GY{mpQ4XL+&+9}Vdv?6bq;@{q2=i|H%wpeF9rSqQI0v-%i6I&I7@)r>HSdnDV^D1?twqr-}Yc)TBXl(dzpci<zUxFq%<`N2R;Y9?4qCs0e0Ey;9'
 '82Q0QrR$;0iWlV`9=QmG?#Oxx@@A<~x(J26<MNpTiUSGhP&+xrd92d<vbX#wr_>G@xG9-XSUos#rf4Dqv0Bhk=(*lk8wz`AmO(0HHa>lVr|I^k(2a{qFMuczI%1*uHS3+j'
 'j3Vex92}_0PR5DXWhy>6b{0a6)}FYtelAnVsO%jt&@gcbf!vT7on!PC9McUK<?mh~jE>JdOn{bm=^Z#UY99A09Fm3J)tav^_X@<Z<$aTd9NW3RE#Pi7)<-du!!(b{vx9(k'
 'UQ046qkDlgGS|ralzQVk+71uvLSVap5+O}rn|D;Y+R6L^R(=8eVvz5Y%t4#EjqNz(7P6Ht6ZdL4kg|&=78(kYm86s1Z;nrUy6Jmzv+m)6NpsDQ!wYlVvt#CRmqz1^_1nd5'
 '+pjL%p)jPjG8g#qkworyh@&tWHzPKe4Cc3VavnHzhfgZrlG?0y%nw~64kV;Pjk5hK4A6MhAC|Hx5tC-4w}|tKf_hunY`)Icw0g((=U~wOBxQ$z2D8ylkNTB$>U+$C8&F>5'
 'D-#|O%%&l<IkVA#n|2KkP>S7|G#SJfD67k-xt(z6FpD!`@~XQWP;!TFQgCPtcLpWh8qzAi1q%A9ABUSC>nQr|(}SP4h!vJn>OrK-D68&)uu27@Fyo088N_Pfw2IP!d6qj6'
 'ZEkpQF8X*w%B`X@x;K!k&C04>?xd6lY3@FmA=wDkW`#c)ubRUhL1e#49YW#{wzDovMWxdkaWMD3itt4%%CBLeg2F0A9>j_jI7q-QGt1;*yKQnHV2HQqd4{EvIilE#T36|3'
 'nSTKqQQ3S7(DrJ!pq*4y_CAoO{xH$}PHjpJM{1KC1r*^o({B3FL|_CD<?~h4H{Ez$(1GqnGUcYE|7lg7h?`XTj6|dILt><~>Uw78<`G&Ihvr_9zUoEVjJygkGciM_;~564'
 '5sO<iuf~TE>vrW%DRu7lX%UO(=Ua@Q(L*$rw@-)DY5j^y5g01u)F{h5UiL-WKR|*Itl2I_o5PDY`~rfmWGL|_<C}Zdzz6Jw%ipXz{!zO7vk6Qbf{C9K5KmTJxk~{*i*BVC'
 'aBnx*_61xoFW2_4cnS!RKiJ)%K=3|nXQ)6B-xB)m3q+}5;XY#c6AYt*Jg6^EtNc*6WJRJx9mE>SyoyG9yO>gL47w`HLJBS$bo!<(_1Dtof<kG}n~ABPfZ>7C+VUitgq-+?'
 '+EHce4ow~6jL8a@&SMpLT-8K>AzI^kJ8wLg?euh0_TvQV(&SZ$-DFT+b#E~biHgT~WZ=X;LEhkGImijt<*}c?>xBqWUX8uNn|4~zb`3S#{~%4rie}PpIf2ss4pNm-*M?JJ'
 'm>23<&e5W{9r%<#eWXu={#~Km36{~l0*-d>S9#kXckW46s+|j72yoTaus%OjpQWhg%im?;B19e9b-~4ruo)cOXJdsBZxA|bZbb*cevMp(k5?|PO-+c-gfw8q8lH0kDk!JK'
 'A4lRx(%dlU^#kB<5PwvQAKehXza-PzjD39li6+kZ2NjeGf99Ds0)B~w`gSXWts`=USh9;d$Dg?jb=mDs9WAhB#~AD+zf(m;`WFbY$lc|obly|1IJ8>gh3ZNw%-LpfgH}Zm'
 'lz{g<T>t#ft6m>Q{B6j}35VvuKf)1Kzzp=rz4L^Gbg1dA*{wxwHDf(aLotGlvtvC@u#~SLbxhL1UwwUqZWj&b%V!9D!}iqmPaY6QFW@C!t#g>Gv?8WY8}JVNPLH33@~yh+'
 'uYUn`GE7Kg8}E1l)>L!Ri!of<1ZkBY@q7;?I=yeI)mzlq3mM&S)e{QUF_d|CSv>jyd$qXtX3#4PuS865af22y_0qX_4-z;5(cJ#2G)VTfnf85s0h<0cKb*nYu>a!?-Si6Q'
 '!j$nVSYcpuU5(?)1^A2p)O@@B%+cDPP$}QQ$(xh*0#U`a6ph0e4A7}d?pG(BlnwIvt5SRUfnM*+{7ijYfq)Wo6pGO*W`4xFL`^Lk9aPmuMhuT!-l!ddF+@y4$e&&;))(A8'
 '77?r?3=DBIEpDbyn>x(mL-=;6*5PuDz_uw%oM?{~5#JzvtJd&54~%5^XX2KARFtCe=n8R?zauJoz|x&f&G59&D}-kZOEcqbB=(&A0O2sotF*Wy^z<mrZPcc8fv79Cl(eW5'
 'w-f4dAknBt#>IlDk5y%B_RRhBc$8AqUN+;lZ9o%Zh=GhXk<z?+hdk5!bU@OXbX(FgBlNVo_Nn4P9pvN;v#8rZm;Ghiec6P7w0Tz+ARyvgUF!*zPS3YHeZLAlukxF1T6p0+'
 'kmwXWIb%^)74FTjy+87qaRi;JppmuP?Y$1X{EnPaD0RE>`WhKymrLt*FE+y<C1NK>5N*k2T#TI9tR8GvPMf;n4aE7g@<2r@y~cNfo{r4@>XF_j9J;-TUUp?|+eW`oPrTB5'
 '{UFJ1wql<03S(lyW26TMv)$`w%R&xFN+k&bFJfK2AdZDUHlZdu(H^P7iIBvBN~1EaaO8mW3%I84neQ+=;ZS0ZvN_E5JFLGVXBaAT=t_J|l5*w(Q#Mb|Sil2_PR!s(Vs6-p'
 '``wcqY_uN9I~T#hTnBswMXD;0Ogv4)7jRcuWwIeg!Vp|EE+9|)v>6rZ)J7c1NQGb7z!QqDo5@HA&#`Py(kiTP^*ABXX*wB!5wWhq1J|95S!q!SMs}N^H<F~;oX)tK9>gOS'
 'K4ruSv_^bqzZHvY{NTQJW<)lx;ThXDov9kGkW{g7zn4n}LHzqFe9@Twt2I1e=megE-6#qi>GDpWW~lizfy`U$?1;EjtMR_g8jM7aW~4BEc8%MOp>J~rL&Wccky^y@3j)fC'
 'D}-;!=LcHCdyGp8=I7PL)4J<(RN;FbhN`$!1uehxus%|CunqTZN9H`f;DS%nCQd&g`bm~jl1PvxErCG?0`L4QaG5jV7ytuVL~G}BCfxGTj(!6`a>8N6C495a04dWA+|o_8'
 'h^<7xe{&4{075z~d7oF<f{vQ1PBiHTk>~`yck?F>w<8vZLT91R?O46K044rovmKS?Ga8;`*f_ybZd8_QCb%t=U(~mM0;1HL@a8_Xpu^iRKB3tLj79IP!C=Yz8=7snopd>V'
 'g*ZA!n_Bc$&Psle@*Hh2sW$7~#M39yS(Udks?_H_l6R3g=0C6$T?jseXc=HWjm-K|{mT~+haR4pv$r!8q7N~70%#{oC%!}P-+on}=%2DK8Hmoh;Gm2=uU?js1x6ET`(L1Q'
 'ft{O<*&e8j_6@|`a$cpiqYzG#(&<SU`CCo6dY!+eF7Mdc@5P#?@JNeoh2fNlNj`D%YS729CJAEdBemGAe22?}Cc-1qw#qx)&eJajQ^FpW;Rhmyq0D_zK?WmVAwrvG6jE*I'
 '$zvKZ8502o%_iFpP#VJ>cF#S?TGC|@UZ5q-t!5~4FYw#ZF*d+F(n4?`(Ht+ar8as)omazU+9ZJE<WP|h7Q?in=f|oPeJ27J(>^h!2RqlcmN&~;Ho>6*j#N8O?y!p)9xC62'
 '1<veaRZl29(R&)(Sp5iclYQ+7AGN;8?}71Hig+}ah!2fuA>$M7R7OEc8Iycmj;Pe{tY6X+WA`AnfZbau?Yihy27+3(a57&;+=BshHPk4L0jyUA_P4x&(-LOHHdWEQLC&Yp'
 '`s-PKlvj!!VF<cwKo<J38IafMIf5LxilvHgEiu*!hVHOO6lQr)htquF5re3i-jToRM;kL&xvi!;J^@kg#X*^D6uL}W(zn-$v=AIk8*tQC%tm<<ARUb6w3^eq;!vJBdDhPW'
 'vSO5xarG}$p2oQsh`Q29FuTVqRCttC(0fw97Ijn{3Tu+Ti-I*ttNt=Ch<~q&4(ENG6?wsAlp>dN$z|2A9-v#FggVGz#L!w=4VR_i^yh=$yc5IBHW<@mll&^sRzN95E>790'
 't5$zS{<TJ;;!xB`r0>d&8fldklYy#^Z=Viu^1@Rw>2~RWnxU+^N7P(P#*n@m-zX4T$nk+e7$+P$H9?t%sjSi?Vn!toQ~IhErd!ule4wkb0#V9&Cvq1-zt8qqGfn4&Ly38C'
 '((Dr`U^)3xL807$1fn}tR{f<rg|8n|Io+ntb^@h4m8XeF?$j}Bgcb*;Dh!RC{$O%#A=3kase&RXWlUmaTbegv-hKlT&FkihQ1F;|UWsQ@3>B18;xgN1Sw(%**Y5%Q@+=kG'
 'W%+Ixq7F}3Z79V5_kmtrLH#}Ck<sSM3gk5R)F$Hm0G_uC<^1&q^mhd)6gd+96dN@UI3&;}@`jMzXhUWiw`VpaIQ@o%_wLXZwSVjC5jv*;90lvsr!#h5(K9AenZZ2nT*S{k'
 'gK~Eqv%*k9W-*DAhSiu}gzD}RlLCWus@=@i>>##R!_O=@e@x;!^}bH&IT~%aCy&#SZFpW?UzJ=QrL*EVjH{tQC(_Ct1G9-XGJ-Mw2D#%GVK!`;Jla?Ay(#DS-*}FBLrSr~'
 '^Ru>?#Y*H;+1;t6ASLHN=MkT)?zCdxVS(xu0K^zR?Chjh+=?lNA2+uaC?@_k8zygX8ItNK#S!<si2nAHrGkT$kb}sVm*>9#U$&W~z5#NU$oSyHWcg3Q#5(w!DMg{;3b3$g'
 'T+ry=-ak-bk;>8xm`BR0U!NYV!qQ1yZh0uHPh`C-5T%^M5Oh#X<=p(lh?qxPZCXD|UQ|J;UqH$u*VPAHf&3_ipKgy)Ib9~#V-?)hBrfn4c(du&^5%%eNxeCt0__|0S)%PR'
 'm&WKrMA(ZF=|L-)q3vt#PR9YWovA#F!WE}SUAkPycl&q$x@uR`T-7I`QqEExXI*^=vZ{2`z6L3c)cG;US}BVO*wgCr8sGXXt>qKP`q(n6=9(I1yL@g}Afo6IJ;9u3XkQb4'
 'bW&%5oV~94^;~%;B&0GT5Qb8ZZ=7jsQ3t)QkW|uptX}I%a!EGd#2_RR9dlPbb%n<|t;ugfgEukD>rhWXG)Fw{$w29d&#S9>FOCNp)d4@4oNUW9pQ{4=`I~0NK}yIh`y_d)'
 'FD>t6JOR=8Bv_h%tOPpr+}AeUW{C^)vjK`zG20U+QsK|`o&?R#N7NHX=Yd4GAHfTKRCSfclN2yf7P`sl2`4BzVWSkhvSnb$RYzl*`fPlN-nleT9&p~KpelNmzv_k3>bmO3'
 '7v!_9{1`Ai0nu$xu~GN5y12OQc+$D4d5k#-brN|s_C~dSaop%J2UVd&{VvFIj_dnJMaYsmM<IfLT2<F9&73zFRU!O*atERCzOH(03C)cAbO6!VJ!(6_8qg^ijaM-qvt4hD'
 '=jZ{vjf2YjFJ1va6<h0pY+r#S#)2_5$JfW?0pu3YR_fBYlTYM(&=QIKNzxCvfu!Y)KV&J!0I98@SW!z@f1|4V0kxTr25?R}SoeS($v||fkvt?RnzqlgSkDn!aez|JekkW~'
 '$zL^HVs(O{lyZ=-1k@%Z)?~I%I5hju?rfh<)C@3+esgPpt!K0$nqYyZ^~AO$*|MHkc?4fx1!>0jd9Yf@V>8%W2J|?Yny;|D$~CbqM@Z)tc)1Py5agB2hk%z)g8v#Z3IsE!'
 'RnXnQeq~CkPjX7Nz(GF27S^csMmys`otUp7$JMQt-$EuMAC3DEhpVG8)x`ve##pzTK)%BOl2IO%uJl{<@4T*rGcO1SIlle`!}IA$*LHyn6y23vwe%~1Zm#csMR(m|dr9a)'
 '4vyqiySlXGc#<x#?OOVX267&~`@jYG075F&$W^qWYOJ60Zac#yMp8~5E{nSn>Cy+Jzf<wF^@wi%N_N1~ZO1$@K~xm(m1Qg_aX5hJ6eahjlb|RVdx7oyBYy`DjqmMBJkks>'
 '$MtShevsE`cP>?yq)gDGm1)Y4O?>@9hUozD0ZJ$GCgJ<`0=_+n79KSON{ix$#XEU*HQ#rA5GoNnq202sK5#wA4+0%xFmjbZUX4#62BWM7@Ouq}7tWbumCV{X?x#m-P8KiI'
 'G#{&Ame0zqK)g=SdB}$q1dWfhiJX9tO3(;zuv*ZwbGVhJ`M{yX?BB|4Tg<+9wk*Ny-#m!{X5O!dlB=|@o~JNMHMlWHo{S4|&{Uz;lmnIW4M^US`zqiAfe|hYz?#4aw`X39'
 '+4k!CZ*48F&ru!WoFZcV(bcw090Bu4!QX)dRASC0eUg~_1%C^YU|e`hbh?3qKp*eS9M`1!o{d2RXc%pz`ST{Daynf}7jbz*B0W+umtlA%;(@!z#XG-XabgnyFtBBg&O0~j'
 'u4Yi6#2?>s(5=K{ulwWvw9SD-W41#%TKX~%?W}hpZ9`<}AacU#J54A9ZxtfnzuG6EI<2aEv_F+ZZCj3K8J*DmQ0Uz<uG>?Rq0G9{n8T1NGDm_AJx-)Wg61y-9poC^Fetw`'
 'v)3Xe!uMdv2NSKG_rOD2LNt7Z+$d@V!EdWb<HHmG0(<Z#M#F%>asDJdQ0YtzkbJ`orJQ8w+iq`Q*hi%O)1sgEZ4XpBH5&Da9IJLIqiubVQK}7^5#&A-#DW`g=rSn?#yn52'
 'BpEZ{Vltc)4A6;r(3TL|%HDoGu?ZLqy3J!@LlI^@uKlUHR8H8dU!n{@K9Lp4C{zW|v5ZscTf<oHeo7VjxqbWuL}LIyrZ%*o-KAf9N8mMfxius)J2{LQ+y2D0h*^nv(2o#z'
 '%gN&@v-lUi4G_00Tl8#0r1@IW*s_oe)g)D+L{aDfUr-&-bTP-aDrY2iKso9Yl_aptV8V8ZU~&L<)_%uQgrhkoWAUFrQX7)^*UmIJ(fjDP8_?#N)LZ_Pk9)u}I<@g~T4(<i'
 'pt-&^qfb-w$RKa866SX<cx93&&ycV({{3uNe(E2iER<Rkh1oi`GjO8slUtwS6Pr7}9r;9u+E27=XH-g=qm_eO`|2aTi5Z7eLiVFW6#Zo`FlCI1ZOlI&{H&*yF&RC4H-g^@'
 'C*I%FpPyvakYNCMa%%1uG6pSR77wFDCy$pb@OR0Wl}W*vf|>&vlX;|aBR@zZ<RnJ9ilFd$UyrkPE(D}pD;=ge6ACE&27mX&FRN~SZT+Vl<?|*_LXgrvX_YUX+s#WM<Bp$T'
 '!+Z{?!ctt6Ge{}6pH^efnB1-C1qSF8CDl+k5iyuYD!t2-R8iXeU0_jBQoNHNQSOXGrx}R{S2ieMd!w!vXa-oF_f)>hq6x`SWvkT;p3n@dti`OTL@E3Aqu479QcBs2*|fZR'
 'RU&vxzh?zPIx+K5nOQN(9KG{c+*D7&&D9qaOj^xmG1-s~_wNQW%-9cBrWX<_6kGMnDy=V3syKAJ5ywuHIEDB;@Sdt06=JaSYJz$n3R$4^%&YI+i|fizDiV#NKuLG@g`SD)'
 '-4$`IMYKR12@GKfbC;V>E`3?%m+1WywXk0z=C|ANr@YrsD>@*fDza0wKuB{ZUUQIi<jg>7Pe0r4oNfZGu#8I0vp*%if|VH6@3rC3`gLQ>L2TZKof1ypqy4i5gmzBcNVn0;'
 'p$2H@8ZAnY6c}EOSUhrek}zYhG&@8fW65$rZ=%Q9wzGl?l`jy)3Y4@O=jH%?u^BTEonm8@ZD6KmbC*?w5D-k71q3nTd=+7N1EmqiI68Ln>iVkn`U}=raonqoy)1Pr@d2O5'
 'K}JNd01(rhE}q@0*tP`8GRIL_avqVOa8wL$U*8-RN-iQ#Ix48HQBKtGMx>X_tKXQ$pKFvV5Kw6h$n2hFzQ6+sk%EJjat8U#+~*5ih3B`cW}3rLrpyKx-L#qjC{NIRg1|`P'
 'p_jJ&7$Zd{Y_9nB7SS1;ZTByrwBnbG+P78uPGhDSU<)9l!`}43G}zI95x|<D5v)KoM=9<(3R&ODNS*Q>yW;+Oho?%z$>@r!iVk!EyTVg_MdO_k^EPZ&+yqX#ZfO1(BKQhF'
 'i1-a&PGh)BOR^v@kNylxN?Pu#Xf7iwvp0!~;`+GEUyUOvrwWwi1P~R6(Ln|y#J}rmxEvT^LjAirT7K@nc|vt*OAfIj$wIe}P>@$7sMKwcW_Hz8ca;Vt|N4n8-17>$nO%7U'
 'f^cdctNH-*ufrEB4yCqVLPlIoMRYr4{mhFW&zi3hqmUlXtJIru)tHn(>Bh$1!FEu-s(WUX1U1QG4Su|w(Y1NA#zmmW!Nx>KPIsux7cUS;Dns2u_M0z26^GG74x%~CjvA~@'
 '!=7;H>|b`jtdqs;R!?a^;UEQOlqcxa)$o7;MCAmX{i`BNUtL{Z309xvl#r$V`D68J_QIwQ9-^`NlT4hpfY@!4lJHxhT&Avu2Tpcu6C78N;}CMx_v`-Hi*}NZ#tFohCZz=('
 'E@?#QFBPgEe;{^_%J0In^}$a23Of~j-w5u58PB&pVIm}j?fYnX)Ai`%EpHIti0g)Lkn%2)Nu)w{aecLa?blaZ>95`vwgu87#mDeg<=FlLx)k=}$@$~W{saURT0<?VVn^ow'
 'vgqC!*mORB#*T~it%c9eC5_Z+kD;^t0^gK?|E?$0(wYnrJ0VVZAnw%4wq+{88Z;h)bDqJ_WGd(cOXF~5vBFU3xSlbkK1&zH%5xEnh&r#X9X+2PWK@+Ct_&Cuy>Sw#Y3@$@'
 'oPg-GW8`!lt9I>Zn))QK6*LQ-j{9oEGPY1RSk>8BK?PCFXnUKm&Bq4VEC@swz1xmVnzw_f;>dGlLW1aX<@pJ?obk2-aY{WwkfXE*=%H@o-)6f#aXK$!B${oAJ)zR=&Zd#Q'
 '^9Fu#$y<dHl#0SakxgCY2kxEuNvMPzMs_=|Ds_9t|2`h2)Qm8aGnu#O{zA<NYB7^U@K;xP^$@fZ4vpEiGB;FSrQY0tc358UKtRE7|3a9RxK&a^ReWKHKCXASZOe;zff&4u'
 '*h-u&_of1^vQ$Y4**_#?HDWK65$09?f@EgK-W@=UZYK`T6)Vh=z1`l~ZW*#9<~ZDa@iIbklVU{u?u&AKA+N^Em`>tfCF6h@0Oy_5{8-ZCmn6=+ATDmY?#~9Y%*5MG-GyUb'
 'z;DGe6_Qf=@Sc=xq~Db1Evnnd&mlcv{{!Hg6&ygBVX$o#V~f~7WUB$xD%~`Hwutw6F0^=nL<It>dzUTkf&($5c|^=!d26w4w_S^vzo@L6@J$aKpptUFl1s;UzB#UCjXA;4'
 '9pZ>`uxVdKP13-#bBqomm}uwKq@g(;sR>Gr+I7`GR`FUNsdPus4P}2_ZEO>pbMzq-$b`wc-xZU;1-`p1CA}rC@EznR=Yy@#uG5TD49T1F91?jcZa2s|GKGrDstmbnD_q~+'
 'z?H<p>eiPN5Y3(4?YR-@^l@!AWCdwYMWH%;@`y7K47ndr+TU4ad;(&0QXYWLt8w*s_^&ZHd65P)bn~b8S9rOWGH|3DFsQ1_t51Ye^OHaiIesbSws#8<4yv*M!A|c+K=p3S'
 '<;ap^r#cn2^ZGjl;t7dTwXHB(r>?SgX4}j?;Q*qu)DY#+ncK5dpG`Ook(N2w?Dn4y2gG+{m?|u%JR#A@XS@^Yj8~YHEG(6Rr@@IJL#E!fy_I?@*zwVzcQypX`&`-#z^LD3'
 'U>xy@33T;IO5=z@E}L?KPq+kfP?k#VpuUFOl)wN4185P+8}k~>^dm!WMWug%Geb)A0%Rn{?E5WxMxwdi`%*$@1+UQ9%eM2>!5uidP4<i!(awkL4^$fSzGFlwsN!2L?uBwc'
 'kSOKuhjRCCfuePH9<L?N&X<INP4_K7@-nlMaj1;9PqE8^c4Wv`Q#~$<Gk(yNj7p~|@ZKfdz5!oO6Q#f4pcbrqWBqJV(g8}TH7qw?N-uDGPAxx3t2AM#=i$v6>njbhGsh1Y'
 ';OWazey;Wf1}Un2F6AR(tc-(okQ-1fWGhX`Q0L@TN`gk2Gs$1&p^W1rGEPtOO2{NPMeVDdDNXI2ImAt$WBz=NIQqFm>HwuPCBM8t%S~>OT(HqN?~Sw5v7xVDK)g%j)$2+h'
 '3y~8chqu5c%o|4az2Z0cAAI~9YR`da^BcEg^^P6z)eDGW7hm}JhxOj}&)GEM3!~w%-AwH*o+GMbFurB-YF}aJARV$r;HE;SXhtA6-(m7G40!koG$>Qf!*F4J=GM1^E=PJJ'
 '(#L1SxSl(?!cfIu@+f1H5z{3*RJz!~W-1NZ?f<YCX9b=yp%OmI6b#(nT7?|$sY^2+6H0$~dJjyRUAiMgBa8n!zPq`Q+X0&h0E3T?<9rWQ7F*QGzv&}w@Hgzz>vPcD<=~dE'
 '^GNj(RQ$8iAew*%LT7NocZmTmJsdjl`;;C@@e${ujcS9=?YN+!wnG@1$63&N1@B=N<lF>+TgcR#v$~>RZ5NNS`HPE-E26dv)h6%I$IT5h%+6ast=y>X%-jJY$nae+6|<lw'
 'I0G?SnLD`vk=u%qRcC*GkW-3z5dP0ot1o9w6Eo+TS;kUy{)Ww|P6lv05cBf|grep<C-hYyL5Ddr5t4wJt{JWG>GeK<fJ(P%(0Q_t+7@b=@2q0smc&t(avoU@BfwW_BK&(x'
 '<uc!pC&~{t$~SOwE;p~nCq1kDNUijFq%Htb@A1v{JCsfgjs4Cu)1|A#K4AN*$D96wjrDJ2n!QRKm^98+kSTL3epo(Xx&mRHqGSSpk}(TCv5gDvZ23!$r$dbgDo|++pnQo4'
 '?QLXHJUX<6SwoTAw^Hm+z*h|Dv{^=W(SxI4Jz1WvG!HHF&sgGCrg4q?<(n`2Y7DsJPi5o&xv!=VUl4z;@0P@$u0VE7IJWs*#v8WmJvp~sJn06F?^T^k^2b3%T}bZd^?ch6'
 'dbAPQkw>!A5u=PLD1QMgmx_k}=H9JWo*cIm{4Rbr3L+^|l;3)0Y6r&phVGu-hk;Qr+GTG8UboTi!46>fyRR;hnGupGMV^zHQUA-Sst}9G6hjh8kw0$6C(LrwC^x2-o6YN$'
 'zkN4w6Y9+T$1UzT{sM8c#o+zPb+HY=&V`=TTGUZ}zjIVS=+q@x?R75pH1qemtv)l&e0FWtPVkdy4x>8!c5McAbo>2Cru*vJtTLUQ^EjRJXkj0`6iVeL#<>}9`X@9ZH=Wzx'
 '=YrOc!gJFMFqJueXfXnumpGVb&elXUEK9Y>MI_0tTm%gI{TDk6r6=WA+cxhyuD|sy(UZ1$l?qLnYvgm?py>W5S?S+2bE`;jQu=6z5M!}-;O*Us?t+{PAr`S_Leo=4*WY<f'
 'vLNo<M5DJu&9~9_Bk0Dhh2zP2j)zL+R_1sBb$VNY?M)6iEpsr>#s+)&ZRpCe4GY^c=ZzT-ov_<&TTZmZA!Jsd$jQ=HPv17E?oyqY^}CETIe6l`8#wReI4QFp>T6n=hX_(H'
 'e^c&qkUJ@}ReZCx+}7n`!ve{ncj<U1(Qh6$n@DFaQts{2%^8o&e6v@SAKv9SIpa3G%GNMkR+m2>L0mGWf@ksk{mY+CDmpjm;LOXP?F+c~qcim{=Tu}i&A>W}^!9|NO^k3('
 'VuaaSGKTznad>n4oRe=wZTtYnQp9yc`9&Kx;;9a?T)N4U{U+BwI`o0a{U*q%R~brbk~d{`9|oV;K9Ww#F6K?oHcIEQ|Gu8nahK%eTN2D_gRLX+vOymOM8S+Gg@_OQhOhR3'
 '=zB^}XMHL^z!}il5Tgg@F!~q?zPl4XB|g}N2b=i1?kEGvFrk=iS_L8hg87G&g$S9{PIudSEz>-1oQy#ak=QF~a8c#kLF7%UQ~kE8jFWX+g99JP#kxy!Zry^7ytnnzj;k9V'
 'Bu=k9WHh?~9KY`bRjI<+d5`9m;r0#OcjAR7&(xb7=Vl$J%N1eSzUBKFE1MK2mnZVf2ta-!FX+pbR+U0e3yk)Dj$)ydy<A{Cxr3wKin4YmMp)+i65jt{<na9$<YZ&YN@~@l'
 '<k7Iq@9;kWLBQOL2%Ne*KE*fpLOhF`dY5wSC}RoCxVK~F-^HMFBCEamp^-_}+!re=I*rVT>G0<l?1WXQGjoo!HYc@v`3GPoH!;p!18y=oBj|RMcp9EfbE|?e-(Rvi#TWF|'
 'yo+@v28Yua9JZmQG1}V0(vqM;`0bp}yIAB-eg&(beA_PF+Yue_Vw@8h><-V{XgC`q_uralIJOmB3N5zKyv%3%ZXfNt6nt)ZPEy{s(YLpB-aZFU3d|<f|27)mf9l-CIQb^C'
 'ZNB0LtMGw2=9A`Y6>0Jo$7PG#?A-^BkW&iNkLdCHXAa2em%8>1wvKs33s>OPz9qIDjHj;&`q@$(*ZI8uH9>!RDU!2qO3LV<)!@r-Uhi_8TZ`n(u4{P|Mi@M8#w@TncF6?v'
 '5LoFo-{4%+wN1w1;*`>9{)nddCTAGiA`;^@sUoovE4+{Dva`Bp7o1NPATd+;?$W$}@QGZGN-NW^pxG9geua!E<f8z3N8r!}|IL0+cd<@?1vLPs>u5Z2MMxA;Ie^bUF?{ca'
 '@Gcade2b#t@R!kSK)m_#>Mn&pCoqta=5`y!cf$wV#n6)yN25GeZVl&`@QuI@$%nKtYI@WCi|&M~bb7c^oqZ96L-e@)A~<RU-n#h#$zkr)5y&IS_x${wRB&pwnpd?lXgzPG'
 'P~5}JrN-cUx8rxI&Z@vd7p|l9?F;>LQ+^6txRT0r#>0sg;j}9<44&{ihwNRZv-2H{>y-QG>C*P^ok<u@-KOlsn#}nJ%OdX|3FjvqrxQ+ZZ*?`A#MOj@n@u>rzpQYiTYI8$'
 'YJ1_<CwT=D>+>caBZH`l@Hew+?_#-=-#`s_^gjB56os86XWo@bD65W!rNNbsASLHrB$j~uFc`nMou7?sosk~0Q3MN&TS2)0QKauL*4sBYW3gjFJk^;3-d@>p7bBdU^k5s1'
 'bu_Fzy^Puv7!i;nIDW}npW8yw*-2O145*{w-KaMAPok5v9uHn%D{>qaL7K6B<gWzxcXH6F%_Uz=ld&aIi%&d`Fh&=B`X4kZGu-M<zZlZp$>dJUZ@nherry0H&TmR=yd)S5'
 'bl58J5X`RK&2w>n+<tncZ#xv&Hf^@Q0NLPh_fdIwY06HHGjB<qR-R6}yf;*L{~UjEl1q7~pDxRfOmdNfPE9g6<94==!U3ptRUz@U?pHfGPRdNScU(v5?X_X|kAidWMuTV9'
 '${dy{hc75qzkd{*I?l-kPL-|Uwfn?zP8(9{sd=vL<;j?53<r=EBgJqBMDCdV3sxi^dpA!fy+S*US7e=ZIcvCn?@ejoxh6tav2^;T=)7al3osaWFXfrndnvTOKOJbaj_kL)'
 'lE&FI1AK0p_53sg*+SzeSGm1A0S73oh(Z0XqV1bs;Y_;qomOXG!O=3~-6P)q-oYfkn(%%X>g;<H4pEX;{&48{@x~a6Z-F=Fvp(@AeZI89Gl0xaB%?He%A*U}Hx}zz&(d#Y'
 '64pizrNDhvWuVaq67`t8d4})r>Nly*?dpg#1&l#zUu}Wc&pA2eO{d~kV0r@h?W-9HcRAqX-cn(8$(H#UYnHzpUAqYa=Y&@K?xD8O^~?KkpLX=H{lxE<%-*GflW&UJA&5FE'
 '?+*{w4?6j7H`x1)#0}iO(Fg+xRK4TeE_#-IeOCP1qwR$F+*&yY@yqB6Z0|LfC)`@lQ<=T}lGIMZ?n5&Bldnl31#3IuE%p4Qqxp^d`kNeQb`x%IgIby8P@?cg^Q7qXWU#w*'
 'ICgAG4RB&Xy8WFu$L}u4Dw99=qBJn5t@uMwqt`q4yurzy71=by%P1~eM4%AE_U8^*_|+B>GU)X@mEYUfYFv_{@e_e7Zm~_LGV^TT2DUD}Ux+Pnm*eENZMH9#TSs2~&@{)>'
 ')11pAVJ5TDCu(=&>R9>q2OeD)zw-sqodM~tq%zwL$2E}e*iNz>CwIVV1CAZEGHL*1?4k6$!w4t{l>WwVyAC&@&iH4g>8rVg*U=9cvGs#ay(O~U?Tx3r^lMzT0Hhx@B6^?@'
 '37q%H-@UrL33YnP%}c2|8a_1T=WeB>l&6tl5Xx33_Ga9!og8NjLfPumOqu;C`?rbWGa-O;5rQWd!YjVOv1o4j1=M^^AdD=PS4U265}$iZ{2^k|3e3xt*%#Cc?gTlv7@^?P'
 'kUh(w=TpPJyl3C!IQeEw&dE9&KX6XgPddAUM5CRL+?sj9ClgJpKzvSi6O&VJ{S09Zk?$Xw;DF!EJCU|hZ6^IJ<|A!2*0^>$$sLg?5aduB$hYt6WsCZpaguLM6~{|7lzl~4'
 '_pvM#kbQIz1)P5WKK3RRoc;#Z8&p~}!TY!Jt~sUs(u5KN08?P6AI=L8!`3Ym*`xWo)KalkO9JP`aq{zlLyY?EP}WVTb8ibe`%XEW^W%t|V($1WqWQ-2J4LB?nZmjGZ#Kk6'
 'd~zc*;N@51Rg6<_jJ6#*4-%J7O%RurC%a#6f+=-Q`YMv1lwNE*QCpn2_q0U6%MnhBJSjV=ZKi11(HfoS?YAhtLjJR*C%5KM<Gb*#iIEX$`TliV*Q9E)tK6iT$?V?@Kr)(~'
 'xyWP}%jlW>d;ddn-;~n^n0`wP@?EdF$aJo5rjR1p;`?Q8P_nSX<G25A+xELqcvAjg2U@g%_Q@*wK*A2>5MPjdaFdG8y+CT`I%(TK*U-Sdgp)Pdj<3*wo&kj`q&P`?(zbuS'
 'VF@tATlxnAeA5u9BAtAHOgN4E=tpAcdJ<7XwuW1iHwI;trPX#2k7>p^8DNsmD1O2Hke<X5l&$xbv#kLPOQ@T)IziHU<?{0-mjG3&lePh>?XPMJ)%Henrpez}h-_ZG)KPkW'
 'Kv!EoawRcMCH9}1<Z~-CPJQS)`hkmzc;1Qb+M~4+<A<H-`jLEIcAd%oJ&@YdvyCVBjoOaQ%G!?`&I!z5$@~vKXb8mWV?OMu&j#6F3!QqyZ(^Ns{ELugtwh%w?TwSJCrc$_'
 'n*`8GEKAS%1%a(9g+D2<)za@isvp?+>L<xbk;TRhs&kB=cq_6P_$rCxXD0b6kwKpn`8|^iC-*y;*hyWBymwBrVJH24;{6Z*-%s_E|L1?qE}8BeOKN3F*4v+(_b>h!-hJ_-'
 'c(;#F|Mth<|MNfpoWJ#d{oB9(yI=qKo8SE9-~P={|K-;|{_6ky+y9Y2_m4mQ@#lZ4(~#%)Ir(%hUwpTG#}nuvAa!t;2LW%x_akW_U_PMs68@JTfA#C1e*ByN{y+Ze|Hw1*'
 '<6r-Z-A{k@+x#zy>4~`<7%B&ueF1|bOXfQ4yGgC9Xf&s33V-vL|Lw=0e*NSB_2aL9^Q*u5y1spjjoOOVM6}lW{>}gW<Bxy$(|`NR-~8QQ{p!E|_}4%F&2PUY-SK9mlkRwv'
 '^Xb*%%YDUU_8Lt8cg)^ot#Glb_Whp^;@yVHeQ>Rqq7qCrU58(&=^Q$`BW4FL9G;rl+Z8X3#qDl@UpTY$gY&zj=;-;GdDb=;?I^gm8FL?gV|7Em@N9^ehj}XA$9H#ai#%GH'
 'n;eDD?d}^?cQiz+!}~m|5AUCVEh1VgUjsh&f>rN4B}W*=rKOJ~&%B*ko_LtAPTY6^y1)GGtRN_KtkHSMd>`hQyq#%h3j3XjPQMBj0-&jXI><>aKIr#f1&_Pi8wmW2wA^;B'
 'J4#!JuQKEBne}MP2k}Y6<cspHUQa0w1zUnj>n?q2^Sob7m^!Y*7wTBsD$-8AV{yeNkMi5+A8-kK@%ACj7jBjroOXkeu0Nld*0|vw7}MIt$Uo>2N$Gm)?QL=!)(fQ*d+xU3'
 '_s`v8)5%?J(K0*k!}y{z5u%33pe`V0$iZjkvvgSnX9-$|U+WcluNYVbooIMcR<XBBxHhsSTURM(&Sf2jOXWLeY;^J++n#@K?UaVxw<J)F6rS1Jv!&|}I8m*vb@)y0?+$z_'
 'gMK_K`4J|xlRO;YkuEpkL&2tZR`~Ef{O0iCN=9sRn2$1In{)jsJFb3qw2sXC@Jl@l(z2*3FpYb{{0PO-QR{5F593AKR4Bn}^N7C>U+!6iEp%;9q<`%puZQi_KQp6Ja<I)A'
 't;5$CoMEexa}(+EFkSaPhWYG;YiF;<Q_a0cTPWYpUZ;gE&(a8F?}mR#=V7+bgFUI+uwHr|LTMR<)@!*9(~EW=bn2tHWW{CE=$XwY3DV#qwQ`bsq1E~1@|oJ~;cA<`2$1vK'
 'qb(o!gZuV^pd|%hrxc>9l0P3})q;C`tUcLDdmr}KdgnzB5Nqk&hv|*W+uAP6hu1!dqx-4*uzc`-Y9eceJd4&m-iF~VP8y*L5P%>z>q7sf7l9|H@O>CB+~;*G`7xp%_u&gJ'
 '|H@~hv*N?|VZLN*uOE-rA$}iz?R^i4Kn$Sd(&p*(sl^masEw?JA6vYU*DfXvyDNWwX4B6xSG6u!?!$7G1zIG?_Y~2X**d(qqy9*eO0GxjY9S7yLYwgaf_K)VM)D3EFp_^p'
 'Se!UU_u;D?x41RcG@@GfVgJ&}<PbeYOfyMB@kwFLti${8HG65?8Xg*Z>3#SzFEid+4dT$t+zo%!%QQsAk`yP_T-M=mt&tozoN&W7x)0;$?T%OiRyuhb>+qFtV+EKQr$z3D'
 '>2f1J76ce?#b<WIzl4<&3!*JrdauKD)gBp3m>(w6#V6zH?^xH_9z$EqZTL!`rwD;!MP=^8aK+w}KU?Rb=?l|alSv^Q93Vsn%s1xG$cpWPILcaw{R?FcNUQ<NPl(Tn3NRHF'
 ';I*Q90=@kvefllU>jbjnF!szJ!=Lxe%MxxIq8vr5!~Po6yb91b->rOzA1DGkq2t<r&0B)jVZLs+tU{2L*!mdOYh^6Aw)Q;wHc9iFw-qru7IBozk-rcdtRev!VU5SIyy|vW'
 '+#{Xve5Onl1m)O2u#ppGWek7B*3822){B?Mb9x(oPgW1^QZ_TZ+f$O~yi6k>X+QiX$s@@9mwxSaE?BJg>s*Il_Bsd9dj~&sUz0d=jnfYj*WuUYP<jT50Wd@g_$7fSSMiTw'
 'eOW#TBajC#l##ggnbSABphsw}gP;X;!MV$<gFeUjAm26|uagf;G|5u*Hmq06r*rwPnm@>valRNF%Xc5XP(Ez27?1K{3mJKhd^(r!()r6Md0@-efarZ*K6F$4qPtkX=%)I`'
 '*RGG8s*R$~15$7?us$hmn9pW->1?L6j{tmN?{Y79!RLk5xv-Zm9SlYR=wleJldm@1HkDn6^-B4s`lYM&TbqAIOTIFERlW2kKrk++9>d|Ki+PVUAc3BdwsaC4CvrW8udN2#'
 '<X|Ia{1}D{12lIbXfC6_T1GHE!uv3OK5uXP@L>5=yJ5Ka8m-FG^b~7FZ5p9{dJp2#ZdfPs;fu{5jC=<}N22hww6i@3@yR`CX{_Zy#rA^*pEA~J+bUC9*^<yM)4jCq0y_El'
 '((_SDY{XCG3oH1TC$IlXG9gD`uR`>ktPrPj3Gw2&Y~m2Dd}$cnYs=<P0c)mXIA6x_kOt?%zI2(BKoo-L@&zRZc}Ci<$IMaM+Q#LzWdi_C0z{I)f!&kRPL1O5YNI%3>U!_e'
 'PLFAliJyBDLfZ$mllD5)j!l3tpUrUTvhM^K$p=@;m$S1b^OuINoj-z!c~pllzdMI=;o&C>MGXj}x93N|tx1kfL<_Huu!#mIOrk8J21v(=8P)lW>U(E&;sEJ}1UI^ez778Z'
 'vg|imWD?}i7Do7Ud4%=j5hnXj8NPD=scDv`d}a8>r!{i8{lx%J3u_-Xbr^mDn`JBkmM{^N-=4%{&LPYWoY+D@zQFViEYXPzfjqOL=cSEPX<uzjBBCY_O$6Dy@T9almsVdb'
 't%T?^9AWw?Pa5hmBht;J#b>?q7jB#-b-})y*oLpWo5)x1GL;YHYWafgG}d8oFN|$)3_dbr$o<(!o|6@6I+sWn&t)UPNWNcyk)9+mVX+^>aK*Wks~u;e)nRyT{hkZLDxL^A'
 'O8C-@f{p6=7{0{q$!YXx8yVZMzPP+Jo#5y(iinf*XCvNPL5yRq_hG(z4Pm*J2kY(!Z`thb^BTtjxnXMjKK$Y$__T&1mY)FnjIh3BNSX?-!`I-=ZlBLarQCk_JvqRDJ5a)i'
 '{>gc@5Y-;Tm(<N|A0Mr4{yq%9fMxDWvByMEc-c`|QmCDyWTyK7G0JljiZqoI>D8Ss(pjk*qtoO7A@Y}|my4p=61xt|1;H)c8kyMJ=xzA&+kM<XYwW{_OI6xc;Ug?TN}Oy)'
 'b@<|(9lXU-c_OC1uwH_qMxj8CC3p^Bk~mK4CGNxcx)LkJs1lOAfk!A|lhqVAq~<|+{C=2zDLq)KP<eM3dv*a{W!2|$FvXh<<<viAHOJz`<3!N#QaH_g^&2B<+WG}C+4%lB'
 'Y0*z#+B}TcAO59~(mnyU;VX+Qix9x}WNyPRtC(U9X%A3B0X90ZtOd@{OJob28ymS-3GD9B^PmJ@C`zN^bXw3OkKvcqcsWk~GlYC~$nZp9xk_SM)%Rhz#O9~1#K)-cx(~nh'
 'N<n-&SaU=6<VXR3N%=U*@vOu8UO(SDQ|pHp^LTWo?uLIkXDXHGgP$xWGFpv%b-#)nycf+-wxImX>q;eGiY9{UCvZ(o=}Z*M$1q+cou8cW_hGyys8c`ROp@FU|6)3>Wudt3'
 'b#RoKw{Gnpk2lf{GS%6l;dT8~s{l4u#(h|?wl`_em+UC07|m|J+(AeJ_+0ttnd4InD3o*%wgkNm!$p`5_Nx#kH(&SR*IqCAWXB~(i5R1YK+nu`slLjuCJn{C4_}Sb2L5Jj'
 'jA!;fe0fCIZ<Vw&LG$nhm!JNIg~FC-Y{1Waehx$1BKs(49e$b93E4A@$sYo84xX9Nl7MKq9QWa?DntDZ$lFci!|P7YDuC7S^D#`X%MKxr!eRT#NYBZd6Okse=3%;SE~AaX'
 '_!!13<;yqLY|DrE(#_`pK#<lrhOcZz1$56AdmDaP%w9>I6C{^T^Yb0JvLH}=9&0PwhF^^D;BpUoA5;U8e^%OHFQ#qy)dh|&*-AkGqIdBbVbShB{}_I)Q76I@a;_?hJar`X'
 '2<e@${A5^Ro#3Pal7%ls4)uJiP~c(>S)i<sVYmo&IZHrW9s`X9ybWJ&Ay-+VwWjXFudODGNZuzGFTJTEk)E7cFyT6P!|;ok*=!iciKz6_%E#yBLkm{@G5q2V4<TpZ)t~%C'
 'aXS|`gC}dA+Y6^xNc%{0a`>FAb{LfHW<H+?gL0|1o-Ls|9ItyXYss6rBWc^&>$9_`cB{?DFkU%(X2Zb8@a18}BFXd+*H0j;uVkEgA8dOcpO!IB6kdm4c6Aj2DEq=L<Y3=3'
 'R$IPeH`77x!*too$!X_g*}(T<f6=}(_0;%Fp4uy=!-6(qN>_*DyDBkrk;L<BM{bGxW0lJw?f>KJTehoyV}0L;U$(w|M2#*+#@u7|O9E2~ZkUr=hyBa)({{<|pyy3WDM%^7'
 'Xms@$=8HBYDVB~|KYAO!to&N$abmeTPFw`YxjSOZNA9KYD96+u@{yjAHkM+0w4|-W7iS|&30<0A<9+yo8~^gz_8r=N_>~QDC@Mt`2oX8`{nC8Gq(91i_+`5#0p^?|$yIhw'
 'U=3A4$g<w<!`FCaax*YY)noh8et0W_QkQ2QHQVqj!P=5>4rwQfK2X5(!HaOiJRdLc`|#y?(4mBR%_z%#_zDcpP=b4$i^4|`H1}b>9)hOGVm8!#3}02h9;V$NAH#U1d?-`-'
 'P+l19%VEjgFQI2dWxFrnV;HZxHwjx5H*GG!hJbaLegVsd+lqizRC2$7%%V_G;@CR4@5A(YgDu?p(3o?muf8IE`hC<E!LAjs4qtxT6pDXfc51i}$5(b(<lOK_LN2}wB2Ue0'
 '37jT#$T%MBFu$q8yhFesH3vU91MFq7s&WMr*r(<;tXHh6%@ns*)iw<8LR^i`J#!{aE%)#-%iVrgD-=nxhsW>gHjHn1SMi5PVGp+O{Hv;^`zKqqLb^31e_d&tgyeWYk~#*A'
 'UcV%9u4<l0jBP>LrxuqMbJMIDzYo*Z$9|Cn#M+?l!+OaacK8WL(dzJ3u*G@<CIDMLjMsS_dV_X1<&#%?M0tbP5Rio+1L~lB>5Y`h&G~&8FL@(F0YNiYbshdCtm*>!Y_ICj'
 'UJ`z{uG%@3`cr|QoKc=AyK_peh-PcKqnL@QFRWAlhKyOM&X%#MYP?QHoXbcT#Nu=V@i5B5cEfKf3lk@lT%@1@iG;p1yA>(J+1~)d3*nx2rT#(S{8YzsVP1}&k*$0mrt8C='
 'acfO!to-}%YY$QJyCT8(jIcCmQhy)59K$o-fbpCqs5gAi&+S#Q*q1ZS^J}{~@9>mjd;)|p-V9+k5j0-t8W#!b(F1G<ScmEIhVNCFV4+g%hUq<Sd<LgVMzlCD0^ymLAeqH}'
 '^0wAtd@ba;CkHu{6Oy&h^3*g-i8JD`@?kf8dF4ag>Oqalhx_mg%LsFG^hg!^hb(Za)#jz8?U*Ta9lpZQDj!}mB*~Jk4*NG5S`eKN^{0Y==1nLSqei@`45Paas4$kKn#u9M'
 '4ae&af-HeQ0M2#-`c*{dv(P26#KFxrqxWI|`EW%2Y_+mQ?!)}jJ>RpYg##|8f6D37L3B6#uHno17`~{SqzYi-z&wVpvZAsCH{|QV;&}|mH(iDSlX|H?t-zisckY$P*2LCf'
 'xlZx5z-}fVeL%jc;xkYI+ltS_a=Eu$Wieq49>Z4$NH<Bu2t+-G>1BoYNa)qOAf8a^nHkMh)sq>m!*t#1@ol%@R=x(=`_uA?9SMI7>uc+lS?Tu62mI3e8Ep9O$MEN`<?3f6'
 'PDR6K8#KHO9xs1>EKQTVwOiI<{w3^|Is1cblVcl(7lW37=d=d}h&k5um3MQ?H3v&<FtA`g@NRCHVs0XuzX(G%fBdBS?J<0ne}+(uTxVGt_u)@k8WUM@D~K3G5%$AZN06#4'
 'xe48P!Q6*mnfi+qP!)(dK2u(lg&{j1<S~3vns*k!Pu#ToFkHBXxGlgs3V0iS&ov?>EAyTT7WG-d*@)8bhp$em-ej>_T|9>6x|dY!bb-e(UMim&ZL1!`uMHfj3efg9kbmmU'
 '6uUp&e)t!Ic)SS}C$DH7zMwY{!e*j_d34@}e+fsY7DvZiNp<+bl@#s%=8xfU-EoxRDA)ku-4FX;%-X?QtaZG0?!#BCozfaHPgt~MyA8uHX+ASHv=yVltr@@tdD3`mK{&p`'
 'cnrU?;5B1_B$GHe48b}7l<vcZVn4EwcEfMV4XBM^(22&}#^#w>E+9$vEZ5;z&ax;6_MkC(55ix0Yco`hm0tZAep&uaKoHn|K5?;y#cfB+;J*+1i;k$$NX1!rMUUavVs-|U'
 '3>WVMCqL9bpTV%nWvc%^{JQk3z#`jErst*?oOK}FhQVDE4y7Bqqz?D2-SFkPS9f8OpyX^>_tB}By3UkVKd{&G4{=Yu1Cw^Ok72rQ3t&6#`Z4@k@K&GpenM4_jupd`(pqUY'
 'b(pS8vq9RD)9jt84!;->RYcKe)FA)N(O;#Zxv=zVVUaOo$3#!*U(RP(VQFCk-g2kz!{Nm^<34(cQ8Fwxe(9*vw$1lZ>N+f+x3B5e^%W+f>2>}y6$hCo^3~zjrmgA?@J7_5'
 'eo5h7XWCBcS7r~e4^BCk5l2#%FG`FsSy**AT=9x-mC@FAzYSl4r-?oX8^D5}eGI?2N}gTXJ1OQwD}v|MUBoh*hCPN~RsZK=07P;K`J{L9%=FHjr~9zKDt=a5KZ%j3z8`*v'
 'O<K$PpPNlHK^aHQqdvOZR{_Yn0KX5v5PamY*SqsAPqHwzSPCpz>o8t7mwwB0G34_n@?4&UiVI!>Cd1yT2YOCcaF%~`AEqnkvblk8<wL#*O1^$PxyhSYhhJ+WMTO{`=c4WK'
 'l&-mrCmp4&!+h0TmUfvl=?LB~-Q={XH?LxecN@MMNDBQ8b=aKAI{eE0hse>bEEb=UG?wrNTawmcx#*3oMK9*wvkkwtSrtE8dV<0;!lDgl^%%aoYs4l=w9R&~8hXCn-{Eq7'
 'X^M6qz8VoQOX4R{!}~B@GKXA1V1gwYx2<bFxsBJ)H!C*o!}2cNIv;nxJU=(D*+cO5yyjv41-zTw))jPu((9luoInl{Iu!P4=fo#xl&sv(`|u?_o^alQ3cuEY>V<}RZ89}e'
 'v%ES({>$%U7nwc^T8H(5loj48)WJ_=E5nx?m1jlet5!>xua@xIer*|HAp{5%ft;uKmB=q|rT+GoUWfULQ#{+Y(F*?Ehy5$_;z1k+4W1<#!C#ust%k%Dx(<K79vo{QBDD;*'
 '`|xY8c_}`yioVbTxrzlAisv>QUx+)(4#@`(LA@jYMS;zRn2%w+<PE^GL7H=k?e_Z7^IW#NS}>=s4qx3gqDqo)NSrNb9)1~=nqYKMNt`ebEq3d4mlEhLF8RpUxDV?upk`|)'
 '%v9F+3z8LZDl5>Nd^iFJ$=gW2M%t%D`I$B5CsxXR__g6;pJWZ&Ph@#UTDF^mJ%-`B9g7PRfVbt#!+f25QZArP`RXwL0(Mj>=QvLUjhFUF%|e7W6aFY;Xp?EKkTI6RR&yTf'
 '@C)bh@BjSI{^jR?Or$?KaDVm}|MNdKfqwe^zy9OT|2zHvkc{dq#{d5PpZ3q-CfZMb`u+d=bCJ*StyktgG?xFr0ZDraQfPkqUw``D304EL9lP}1e*WXn|N6V1f4WVb`yq(O'
 'H1z-c{@?%fhd=)Qcj@c>)9?S~Uv5K`=kZT}__yu9sZ8KM{PCZE{?qnE4?(x`8UOyj{^d_U<@wFCcoXiAKmWtO|KVTr`upwQ{#_=^1oT5bCVuyafB(}@|MvTT`}aJ3<^TBI'
 'AO7{9e*e!ul^5ji+z*hBi2BAHryn(R(~r@c(;tqdU!&;{2cTPf`VCeCvK>2P`lC7he3*TX+RT0~jMz2z{OIf_zg*hc_r9F_gW1O|r(iPsxb=!&&A!cQKqiaX*Pzzyr=fU!'
 'wI8Zbkwz5}_QZNr#SujcJal^b?XQ0Q-+%n=i!>w1oo2*4Q!_rN=})$$O47m>4yW8;H6YuuGc*$>nrXDrDuZOVD}|4)3vq$k*-y`mbwLH0dcWm#vwtJ;i*;eM8j|_xx`<>='
 'ek^CeWAY<XJNxW+yApnO_Q`c;f)JxM`)x<r!R#jzq~)$;v+BwA?8fiD*zdjsdsiwad7}z%?n-Y~MOt{VRmCRqKoz%p&B+15TO*;>He|Ev$@c7qst5p<DgxlqI*Q_sr}17D'
 '$#}ZXQgS}@mTFG_H<b|=rfR*(h2^efv+BwA?1swVSnP|THaKEqE-7(qtwOxaUIL{4SWXW~$2usu)d({kjKMbg$+cOvWJh*mdzHkrS4qshNiep`5O>)j4W6Jor6H%DkChR&'
 '+t|tOR7t{888)i{Sy(88+4S9*kG>oG_MFZO<g(QuvFx5l1*C_@j-(}iFSC>SMzT5{Y*s_E`SFlU9a90G&DFrvp41%OWhZr-_W%QBSVpUE5v$48R7t|}c(7Rw$inJ)0MnJ`'
 '<5A77-Hn*NyKFHe)1lvu(!*%Yf9^2uXS-1)Sx1s=Rs*tCB+1(@n{Qk;FYVb}mG{9(SHR9bO%KcQHkkCdHAR`c^tbA_)edB{8j#Uq2a;eKs1h0jRXU^8$!8}$dhU0c^iZuy'
 '$0ntIFzHBr#^Go-Emi}vm3z!;?=;O$AAD=~<TAI5+~YLWD*gOd+h9EZ{9APMmwt4yJ)BsBRZDhcH=ciUqv3E2Pq6eS?PpI{blPt;>7kDA2`cz4$Rs3`N&GR?Go14YtCsA@'
 '?)$rYH|ZGdm!5t=&_^Vj;#<R`xw&=ch^2?>EPdqE--SNV9U~5wJ}g!Pvas0rtM@+ENA#xY>C2_b@#Iz85v!$`Z&paGPViA7F?dTM$r!=eO-?3(9{X)H(-y0i?8t7c5H&sH'
 'I_WzlgkA}gs~q~l?FSvdSs_BP-?7AF7$~Hs*Wql(x{)BwPw6SEo@~!<s1QOmy)#N1(m^GJ(3+&gbeT_p8o6H-x;bjS4hn?!#6iB3#QCW)Wi=p+t5XBb2!+REgu=V_9w#l6'
 'c$HI3K-if|=^+^ks2ELXno4&PKR1=8tOjI$VJZ=Ndkr*80NM*?+TY}S*j6PZ_MHOI4+Ew(g)-!RAyWmQ=2v#L3)!q%vLm~p0-Tr*S}w1#&S~dc#{qC3BKoZ3MqaTCQKW~2'
 'V`Xqj#<4>bcM@4D!)7%gla=EplYpIYbA!O#MJeJwsP|zVMCp%&^iYfiQCg+IR0owLEO#WE)qrdjw0_D?@{=ny#`Z4slj$!S`z(v}FpiZGI|S8E8F4FOSe?%{s{t7<&SzHJ'
 'l~!GaGSUh1P!DxUFX7S6*@yUEqt)LQ!a*TX424t_)rl8TNT~Q>3x!NrHDnFD@!da}29wg!U{Y)qqS84IZ=G~b=U2B180%7PCp{Fq-5;6=H<NxR(fO7%Wi=qv#g>%TvUx0M'
 'Z1V6lw65AQ<w7sJ)?{Mr!mH_F5KA4kE8SEbD2J{craCNEE!mOXIO)Q^V-Ax$=78;S9YQ(oKdAu`>@^@Em>P%&)bybS2DO62sWn*jWP5go25bdDGwE%tFYsqQHO{ae-JE-|'
 'HX+;qqP#ulxYcIRV$N+=4Ozo(Jm+HW{OP1UPCB>Sj=Jj-u{qhq{kFq(_ZujnHf#;Emww#xwVzp=)qqSF&ViEHr$CK@?hoJ2IndxQvqxEhx|m&h=nWnC0?lgQtlddux$)Sn'
 '24ssnWCc5;B8?p#wDj#<qXR{EU7B6;AEAp_q=(+vh(b=qMK?QsOWnU)AHO!Mmh8xGsDpr}Q;^2u!9~*mZ7f^!owTWg<QCvQ(?#Fw7Ko7R4rDP>g3AnLTOMb7Qz*$0Zn-zv'
 'tQxY0-B2e!I&)q4nil!ciI*mjrz~+l>z`c~!hPnWh<F{Vpa7^jnQcv4t_g6d0-IG!c4RkJLGWOzpq|J(6?pco4w5m-_7x9#-Te;Xy~`q?>!RBdNWszxOfNwz2^~EEODC|r'
 '1Rb#(>jceUQW`4(x`$4vnkfgJ5GY@C#UmlQ&TbWT!j=xO6p7PpT~u(W^}4WFwPZ(j<64nudTa4`Smb(yMgfqtG&w6Ma#|P3_(B)+jm&!&Mef|tDI_8pHdO3dICTn%TNk(0'
 '`E9dm$QpLTMS*DUQ;2x9ptQ4~NcL5QpIs8cVSiMlha{E;D!?VpG*J7TuQgz?8j$Hi1Nn5bR3K8~s1F@v=!@2Zas;0ZT!gg5Iv*}Q<T#x9+&&Mpabv&xnS_>EF;iA887;G7'
 'H0`P8fe@0NWW<g%Fp?S6)0kV3Sl2!oZJT`*P)djDqpsjBdsMzq@Ac%7$kbCYLTRR_+STQRo(xtES;KCqCq=PRDAQOtDo0&=Y(y8_Wt&tE@^va;=CM7S`=Ifx@;qz}&|zsi'
 'O<1*LM|R^)s-|6yH>v2g*`#7qKceaE-r2L=?GFV)iayO1#5ZS<<%Zv#M$*G!)KyCb@-zZ?Bf&*l>y%YbhKsb;NKDqroyPpZ!-(WsYc_IP6<6iUBy+pJf6Ml{AM3yu#AKQ2'
 'ppdK_?J29CY~^StD>JaE15j>s(3afh((_#;_Otgt8iIDg4h^ctI*={}qtk(us9jvH1DjP#c4Rly0jJ~E(g7nMyt?$#Lt{QgaWyb_+>dt1Qfb@rZ{3k%GGx2ekRA>b?X>{x'
 'gk!>Lr-?!Wi}ZjgtDX!N=>d^NZB0mQG{GdTCJ^AN1hVLS4-k_ds-=V0R~zU6)~3>w)sT$lt3h0_bpYY01Mj_Q$0i#!goz!S&TCaqX<L(Ypz?dY?MMi{dT8wH>8%D3W=rH&'
 'O}JVjHmd=dtbDzIrWF<FXhe~3W991VJ<<o4g$LjH*!f``&wJeP{wB*JZXGdJ^KP>mkom>DGujbx#wH@p4(*%`Wu@y%(H)X%56+O*A`caktoxipp{DD7Cy6V|X~Jqiwn%gp'
 '1@r1o^4;BDA)>C$_D&~F7xbVXAh-M$4k6cT(wlS0z<E|40A<J!*D9853Y}fy#amv+BI|p~swF$J8|p%8?(|R>LUIT|CD*QTuO+CX%c`Lr`##q=KOM$l<E;@j&y+CkRo&07'
 '&1ygvS4xQXf{@rOi1dqFEkwKypur{6tuGZ#Pnj((pqpiN+(=gG##2^3*(%*wD44O>G<J-|=6m7h>Qp>OH>u1@{-O)FAN-iIH*HWs++b~NAdUmA(AE&f+gd3kuqc+Bvg*lT'
 'Q7k8(%pAWo<`0gAc<2rb1`m_eV}dp_`JGD9;R&uALgGj%?VZFxDHTg^YEn&E^<;Z?<5Cr}3+5a(1#`(VP&*A4Jo>A`AOP9x?2%b<Lwx8J!WsH#0t9y_S(K|xSq;dR<tpCO'
 '1Yf2>_<;@}RMIJKhG^fe1y8O=*Z{vp&wDLMfhKe_EfkWKr!i&KldU|B5JAlvx(LxX+2L(&raFncg6t1t`=HZ4E(`fO-Ho*nOODudbBg)kMO<pZX4Q~2><m9Un*N^DYwZ1X'
 '1+~`|8OrZEsA8!%?~NAHQ$N%~zQUnd@z$M$SJu>o)qso_p;=UKt%ckW<**iF`hMDl$K7QWrD?v&yMY$Gi>4NQ_Q~8-3t<U0FlE(}g(cKLthv5JCBRVy$u?|NfL`%B6vW)s'
 'saHXI8Y~rH(|B{B0)%rauviVqbajEvX4;dgxMSv-qQ*&ffduC-Dk_EC6^aH5*%EVy3TcX{M+(tn{j{kNgH=z4tMp6_Ce%P0qhsEa3QCt!z~Q>{A*vhfOtGG8jTUl$Fxc8}'
 '7^n1uWDbZpXEh*O03vF#_06P$ob7$kLX_BEpSpN}UH_~W2wV?ppPquFu@<mxx7ur=lFUP~b5;YgxLOO*+u1ddo9vn(ZT?wIOX0F3f`|u^CrXxsu@-1+j_B>RKm}iT9)z8<'
 'YRTdfdLy?4P}5d&PLBt5=+Zeb?Pz?N7W^Q~DL;*1X@OvpB7P@XAb8JN^<*mq?+83Gme|Lm7KrVjMnnjgRZ*m=kK_jmi5$iX@f%J6*_%@(nOC^aSq;b*74GS^cD4%<8v7M}'
 'du5tk2)fLjCgA~AZhp#^7IJw{oE;H`1l@9TvRU<Hdv;?jRNri@1tD#;&?fGei~8#h>JWIa7PdOsffl&z>r*Wh5`^ViuvztF2+N>`or2jU!3z4es!MCw|C1B14tqN_Jq>D2'
 'Ji;XTbtCaB82dS^mdr0<>=nr_971Xehd^6EOZAA?;YE`|7`*=JsjY>kQ@}_I+_%sjPOrggK!z(VOma97!CTq-ps{t|2vsh-aPV%sfIIdRNKgG(AyRJfdA2Mni7XXjvl@`C'
 '%CCh@Xx%g>Vkt!WO6=fNFGIBVxB+<|6@rLvtdLOdl5wVx&=t*%6=Jh$$&T#C^(@{@<Va)3L=FwmHt17j_1^AFM&kAH=e>Q&^36}*5-2}a!-Cb4*d1n$T_st&*EXvG*}}b!'
 '&YP7JY3x`z5goMYH4!j=_}uneZf-O6%+>}KOB{N-K~<8lTn{#@0a;i^zLiXGjv^hsIS6j%&7ofA&H1#O_T4M})?R0*kZk?v#Jw)N{Hp20oYkJJXJ_Q<%OqDnSOs^v5kPw}'
 '5j2-*8%H?(;L~|Zlw1)~<oNDP8y`0mv!jBdGh%@mwOfuIu?7mNUEUWi#)Q>?Y?Z>Vfz1`84X{wuE*CK7LL>h0;)rgLzmc9I&*t7w?AwhbtPY9^s{z@fBTbr46MZ+0(J|Da'
 'xNjf_of;4>GpE>95WJ&8($g>w2IeAAnp74&NOZXvY*qs@T`dL>^W@&#r-wp9&~7m>T&M+6?#~Pk4o6yma#=u=BJGWYS4jkORxKGXk_ggQcFr(r%h=JVO|yKg{>?NIuX8O}'
 'u}2ijPub7{m6|u(wYZU_O=r3}O<DD1dv-%D02@%FWz6p`O!I?dp<{j$%ii|MHf6xrzyIm!sJC&R2Ve4!!ffUX$)d(|&Z;My*O;cS%#38sd@|H=Xfx$8JT~!5kAxjwR=OgH'
 '(#N3$hnf0ydN3S&^bHh(%f_N}RxR18v1t0rY3q-cLUOR_poMVEs7X&E2c9|E`Bt2n9Uvyl_02UbIfs3q6sqRNKq)m9HO`b$NEQiyb5=dsJmD|D?Ly`$!8NkBTN=L8iomDa'
 'DDtSyZhlJRnU_*VLbLPvMzUHCHmf1od^w1HFvuQ*x=ix1`Z})<hr9c^PoELu`({G<sRv63x#8PF2bE;41B=y=Y_5YX95+;g*L|0>{Ge8a?6tr_{ACvuiUZWe{8R^tmO1AU'
 'HV{NBx=~4B1q?oC)sn#i82no&HFw*Q=UtXxUq?jp`0en$@PDA*^@u6?sihz+?iI`wR7n=`@hPhT*)%?$XBCZW>@r5dR8SOY_bu2(%41(rL>Xr~y~B*rLwvV_g7!F1<*6Sl'
 '$d`%-JKN(<vN*-3tOjJOQ=Dv9kM<>y`M>FSiIg`sl3fbahr>{5wg<O*`DvW#z>a#{Np!hLY*qs@T`Up;qu8#D?ThX5gU;P1A8=A@=aTUoK%xQ=27Pl}R#ki0i1Jj&O5uV5'
 '>t{RHjbx=1o7IqPt`zU0xiUPsqj^iw2O~CEkN@3G#-~?_#z8KQMu%c51Sh2}8ws6RB{r*;jMl3Jqpc8fjY3enRl>BZng6g7BKX01p7R^SSP4>Y+os2blx#k?JT7cjE!mOX'
 'FhH9FtX6<F*6t>|h<Wg>77*eNl<wo*l|#Tmd+T6XRiTvS>P!oTWKl~oXVsI<Ybo+9qP-N5+NA)!78K%@J+23}6mdgIA8UaZYawjK53tq(pI8eWyRjC)^gR%cI)K2^o!Ye1'
 'f$xJ-Nl7=`3f>%($~aX6m9Pwx$*~}^%cKZ(BMD~?ZJSj~7S@M$#(A~<LC2=ML7hLyhe{Xefp0ka?T>1Fvwni>`*@|N0Kw7^hS`Pdog|*nkHu<0wsHr_bOtN5{m!Ba(5{%x'
 'BKzc4HT3fGEv2zl#qu&wx2jlRF4qT!&8i`5*cn@ug1uM8<J6~b*H8T(hrf%Udq`^_!!~*eyU5aex?gA8t~<rc)A1>ufstiWqWU1wl~N|G24uQW3PCjExKuAOck2P1b|;AP'
 'Bp)X!7K3o)WQDQ5yC->CusqNTZG`|MtzewH85;?&LW&bs1G3fSw*~1@hK{GYr+$yp{nhtp@B1(vV?Fv%ehS9QD5cbg3GQ@IN#armHmd<yT<M^OXU9sYIL13II1QZ^SlF_y'
 '-miu9*#j+zacHDJwIGOQTFBAxrLQ+()sn%&*US5h*v1qE*My}6+pq^kR@2wRACgh?h}WvfoA)OU5C>Wb6ilta@mA@m6)Me3aakENW%VE%u^Vf}$`>qSLXB3a3)!aQJ=&My'
 ';MLO&g!XY&_a>!??=z<I)8UP5nW7eQe4rG7Wv<SY)qrf3tHTm(zoTL_@0<;m)*%-s`r%zF401TrQy3<zm5Ipp5O$4HB`K}o*Hdq?YRQi5##8U?nRnrMQp7e%ig&@NLFoKY'
 'Lk3E3UJZeJ<lp=>nyVq>bJ&?{bb}+EcS@bJYRTfFQ)-Qd+lFoA#<10Xi%NEtw+&kWCe4_y`RA@7(VMiAOlRq}qHOI^+0r~l1Us!%l!$rH;5n<7?8t8H1KJ4-C`X;3*DhR@'
 'v=T1@!AY|nM=qbnQBRj`MndjRH`%Ok>$Q182L`K_Oi$<_I$H@5j(&Is`DwL)@jAm6n4xzs(o;0Fz>6&N8wsrUCq9v7K4Le##1JqWs78U<Aoj-=iYNwfgZ>qiwmq#sJm7o#'
 'SEN~w{Z$5nxM=Ev+$=IxNni!SJZIIC!Mc}-*f?EL8l0}Q+&f*wv}~@r%0Ni|%Rme1DUY>KI!f5s@OzS#5oNO)lFbWY(t<F%>VP&~b%0v9-{dHWTve-wIS}5T(CI0eTBr~N'
 'G}A&RSzIVhSq;b*ttuipF!v>@IcEpVkFLFxgR9VF5h?bV2l;6<{E4_h9#57+CGkro*sKO*ex(Fu6OSmp=Eo{Y3SE_?C&sI8xDkDFC3`KTr)X+{XC)YS5?vk(Hmd=dE?ccB'
 '+O5_i9<|^@TaK6BMt2nm5#k1%*JvSaNaM8txWOUYTBsxo!0|b&o@@m;p6qKdaq5yEcf^>8*y5HRfCKYw#b?z(Fu*b`Pe-1cFR>nfY|ngD!<}S-LNsMHAX}girMa`ae7Ob;'
 'C+BswRgq5N!>2*+)6nljIn*oTsi*B4(>e|AB#ZL-DXRh5s(gNPjmJ}u0UIa@X$_;CMsQUrWSZ|mE@pm8(bPgbW+0EW5RX6|rWOoV1F}_}Hh_I512kuHZnx0_dUzkNLTW@0'
 'FxZAX&@kW0mrT5pq%_%vWJ{V^X~AaIlCAEm3;18KA?boPkaQJ4<8UT3h-n$o4{2{8buOlJI|O;N|Ln6My;689yXch?O4*9eHmGo>0%pppC4&=&5(V2(B5e#M*ZI#r?FR0$'
 'r6I)qrJUb7U=6gOTaW9B7AgrXPiULffQ*)&8`@5TBehUg(mUS+H?mxXHeRNlhYFFILTvoN9f?1skU4(fh@GJjB3mJz8ZAJlg)2`C&Z#?{7SdC3LoH-{jcJDLjbzcne9o#T'
 'TedK_kz{r6o^@z-!FSEfU5Jw_p<F$k1dHsG<HzsCNlS_#qJesd*i?_qaMJE1y4tlStOjHYv=t44Wa>wZ>d69Z*Qp2NWmsLvpnZi)ei{sYz=n8cpCXlH<!(<|^<+zTn{6oU'
 'G<Jl-)<oOl&59%B{tg>rT2VPOT670c^QN_tyS4PXKp6(0%FJX3AkMa~Lb6H&oU-c4R*iJjY)Y6$paK4c4q8(_9O_^XgNumt)LH_iL#Ew1XCo>1H>)LJvueqX?1s}$U?Os*'
 'u_GdvqWX5~le6VNJ@@I*>20R;RKS{hf>}QVex!n}=iXxVARDn8&OOxIwc*@@QiHi%1GM|O$4}=laTw*h=BJ~#o^R<Zi<!q|kMcRI0omf>C%5>qHvTkr-1vjCHcnhf?zg|r'
 'S`B`XS;?WoR}HmL5g;a7z*0E6*z%{W4rCp>p%%<5&`?dvhYGg$Zm)t2L&SC)T1>0MsYDjeIDv0YcI9Lp3{K(bys<D=7Z8OcUtz}O!Z>pQF=97XNf1*fAslt$TPH4PuA90l'
 '89&3j^vfaxamTR^cv13uBU!b{nX>B1R&8=Zv~Zrvm_N4Hte|uIQvn>%WeFORald*c@OD2pQbNvyv<v6B1WRFgD?Md(AnVu-l^`afbs9RJ57OO2gaCM1zo=yT*G{;N4z?Dc'
 '10BeAfkQJL)NWj~SO_+&mJF9I1#dgdcsKHRTnOn6YmbNsgqP)KB<F7sYE6D2fT@Kr3wRfj46;kh?Z{@;kTvYaS^!2<3m|yZf^QM`)0Q;>1AMy{)b$XV=_!bzg^(|BCIRnC'
 'vc`FzvTDiJIPbx$H?`oEk6Q53o(h4*`K#I!p+ty-9VtC!OACmLqq&mcaxK`b24su=ENRmT=8ohm1j<eejyf#>&}FyN8IfqD1s%1Tt#d+daV~@{=XAXm$elPBI(9=XV69*n'
 'Y9T4%kUSZ%?bH=0N9V3OoI}RH?39q6s$r#S*`#IWycH;b!z(^kg3YQWJF*)p!PV5fp%R=PdvygTYPS(g2aFqpQ6*n3<R-Kn5#C%06mZbSt2{N9Ml438ow#u)S^Ew)s{z@{'
 'cW~m&W5Nj>j|oSu-3s1c1|*IlIqU-^2)MBla?gP{i>VfpoI1Y%f0(jr$QpKIB?PktNE!>$EDh0C3*hXYzx0Dsdi;{`<_m^wh4;zc=_!vrx*YDGwie{u#ldoEOjxyKM|NYa'
 'M6j1eL~J)KY_*ax{cNoe?0eIOH*Z*XWYU9HbVF=uwIU__BeTAPcalXl>6F!g%rDjof-~0&0vy*$vLaipVA!SLf7-`W9|vJSHCoV0$-Z;5i|HE)EMT0bta>u=EAK+0?Og=d'
 'co*C@-b{AwrxRzAg+YCiUwS(1eINz+9%s1LcM_flnx?D<WW0*FdV2wETkkIBTKilcVy+0`GRICM`N1|6N#a-woZF^M${%hdtESjfRz2CODRy!j)ZT`2hxl=3M%sS3T?_eC'
 'zRZS7cEdm+aRcBSDx`w1PZg4H4(NnJELJVqk=<A!&f5y9Nu&F7!P}J(0sZb$;?uVhG5zk3i2T$zs7usn;&k6h)(Wv%4ajI2fU;MJ*JDjYIx~FhS}+B^Y*|G1qh-N<m}V&@'
 '>CxqG)sxerw#J0zX<@T!$zYi>5Yo$R2V=1b14;~oThq&W#bXSkThe$#;pnxHeBL;_9J-T)rO|4$8jvl@BSLL?KU4xAOH@Jy9Nw!ycpr%UtP03E?c?G3sbR5Z$2jC5w!Ix?'
 '5?wZCp0aAmblI3W9lL7A09oTP2FM|{hk_vZtNaFX84kSHLPqZ!YoV`#>$CtRLD(;~V6$q;cpYxyiSf!FOtXm*C-FdWs8>Os&<)SI)eX65>PC;HK_lJhv0mELjlrrX+p`<$'
 'hSa{RphIWbqi&Mz-kP+BsNjcv405?uSg(*U#sdl^`6wLi)uTf0B#X+SDXRh5qH-vRL{lLWoT(5Y?D7ku!euK22twz2=Uma@ooXnKupR?TokB`Sf|ct%WpyCy*o{rvFo}9-'
 '8};xKTaz~8W%&gJTY`F{g|t?}SPN2eQloi>+(-bHTCiF5WUw-6sa7(M_o)!|xH$!~JD7*Ggq*o7@CT(E`X!{(iQXGHPpXi<s3<@6Xex+jfPZ(AMH<eO)qrf3hT{oM1$m;Q'
 'f;@Ivfj(excBUp!a8YusE7I<jewR{eQ<Fw1;=DJN5@(5;H<DFW&XiS8w#>?5J4cYIv3|S6sN|%4HfNu5$fmb=(FwWnyz6FvPR<NHQ^GZ6)sn4CxRL{H!e<4=BYaj+7bd%v'
 '3dsx@R$|(x#23_Fb#LCS(#9C2R}hIa6cjtJuTxMZ3Co3Hvl@_v#lldulLocmBaN*rbzR6R&H8m#R-SqoC?rHbQ%IPc@5!~!=XO{gA2zE!S<lW`F}5F~(Bw>|^S;*t!DY?|'
 '21M$$koM29^8q)gU3=f!NaAYWny?y>trGj$&S&N(pP32z>_GtY2*)3y3mSd$9M$ip>pdG4AxghdrKe#8#HhtN;~D1uon+w-PFW4eR_<V|H7(;be*!wD`9rrmMP<CI1el23'
 ';J6^JxA_At+~8>@TA(eBgBDt_Sq;cmH{a=WHMJ0&&?=ywulugb^r!5|^gdvEsyx<$<lrCJyVIRy0p~GgH6Wwa?!+eEP#TkFfAU>QXU2^Eutn=heIH3rPeT~beTAe9vlS3F'
 'z{J%GuvztF%iIqNKG^;@AB(<-wF?fWeazc=QJ4X9(n0Tpw+yT|9uB$hcONTEPlvHWExE3Ku`Lx6fTdDwRy`T4vTM<<xu~YcE(L+w8zyr5#?75$ff=ab-j~zAStBv_%~{IR'
 '@o}B6e_(d=dMBaPaXn!*AX_=yfOZrFu!({IbOjiY_NdDa3I^&^YxC3S##(?9gs@Q}3Q5JvUunT+)sQvp#_OWiypI<~H34@k15_qyA-unc4IfsB->S8b3h@$+Wr3yLAI}tm'
 'XW+!AtQxWtaN@zCeGlufxrcSNTx_oemllSf1wnGX1ms@AN+@p*f^a(DdY?N#9ffRTvGkjkz(=j@NT$cfl+}=IQJfJK>|Ez~EUtITeI3BQ{osdhHUvVCY)!d`fE#PUZxC7&'
 'E%*}LTik3+S+!)cPSyd5rf&hk9glI4*0q2LbQLfi2`ThTAw3;-F^?tB($B8JZzQYC&MB*&Y?aw5(Tk-7iM8+5T?t8FvC%?u`CPc{m=N!<w`bE+vb7+`TKSO{B&RsJwH6Fk'
 '1F}U_R-&0Fl7<d0hG<mZ>Y(0$e{$l5264aibco~|i=R79RlAeua^h`P1F}VwNz{J(E9BO0;#;>msbYt6)8E{={h)hu!&3?pdT5FCRNZ(j)Uu!nRAwPTcj8!>uxiK}c4L*G'
 'ncAGjj;YPEMaU0Y@q(8n5Nh%*d&4z74TEb5bOmvpf9uR5y$8vBVa!<#$d(HunmE3B3&Lhka@_7oxozTgS{Ru~>8YApNN*;<Tnm|Go>e|)H6UALl}m18WA9I)*`TCFffCh<'
 'BK66+2UYFoK3QvVJomUIV~{=fm1KS;G-ov+TU-fAu=6_7*fF9Z+4FAZK5f0z(;?=o*yJ82Z-R*4d^(g(vDb>1Xsie-c8l3=b|VR^#O67xmMkn2n*|`6S^;!Nt+>!cV+BRr'
 'Ww)hC*I}?UIE?Ni@|5^e)K0T)$!~S<ez7!cRs*s{RjN4KYDz;Ej&j<bT6F-sStvHywGqh`@FExC&Ebvc2R-a4qv+{aJz7fabhdL95(vvv!)Dc!!78Ny#7-#yX;KQHCY2pR'
 'e*kb*SI0Ep)Fbxhr=ygDZBCkd(UBIGO+x0Z24t%$7$iG!6RAnuM1nTRgol+Bx}*XIgzbluH|GV$45~R$ijLBs>J-(kT9f0WkRY8{8WyXb3~9MEY?K3#2IT;J?3CglOuLoW'
 '13mw&*>3MtiiXf%vF4{^ODiWN&BC#JlEq4yuo{xhR|=4QYXQ{UT0j>EtX(-jxkW`7L_cE2)3YW%Z`*#d8a9%!GNvZ124t&bz;qbc5!f1?JscF>YDi1wv+jj=0MupH<fkK)'
 't~?Xv;_Og5(ak*+xJ6m*L=#pm*^%A&c&M!!$Hzm!=6K-P!E^!RRXC>JdFi*F{8UG#5){O5p2YtiBr@N6rmO~Jve<eykS8O*$shBTvpe19H!+7En+r0QT-ps>fp&baYv)}U'
 'l+fp=qq5;VGgz?v?6dhsvcS8Zv+Bu~c-JcSF`&{M1Ipfac%}|#g%si;(|8|3asuD16#&<lx8$e8GXjd|6^@BzomMKzJfnZkYCyKi=$A=Jy>un@0Nhrq=0N&I9=C^EbInEz'
 'JhWTOoV4gF11;oxH9nyQi&abJ*R^=bQ#SJm1jL(_#9d2@jDtXN(z+<=@cs)iynC4h8Yw8SnhJ`uXyKhCEDsW!)qrf}?FE=X!-F<BMULA<k04iXSm=7cJ3ke&G=X^ROfk?z'
 '-uH2NjM%JtGFat61+Npts0N88r*O8K(BQASx$q#wgY_VOls22^M7B=u6T?cfXS^uDo3m=k_(TC-O^r1`K;)P<%hExn36RbbpAgjNHYGmt4oLh>OGC6zo6S$f*rY;^q8=QI'
 'PATC`+;+;UCtIQkK8KQByNn!h><%#5A6^=P`vw;IDWIhVz)42=o@A{Bi`9^9p@o1pN_HUMVwJZ?1v`bSQe{0P#|C;W=&-X_os;(7bfAS?qQxh)V6ke+j_k&H{ou_Uz~oyW'
 'a{w`RdHp0o_%bcT0E1&928?4Z=z1*_lEcOISPOb?Ep+V0T9^PbQwzvUOg&h+cbBaNM8Yo3AU*ZYSPQYdz*FxcmX}wTYr$sKlEJc*c0e+_)21=s6jVT)1Q<|}Oth%n@^Sae'
 '`}c;$J$mkMS<_RMu~u?{10G}EtyU^F*CI=K&Z;E~>nvqI0mSjOmF*sN7CY2g9E7I4r?gp@-{^vWvntq2A0|vs!`P?D#SnhJO>HE)RE5oIK(@yCU>n`BHlyBSe6&$QFC0F+'
 'HAxtFbLpvyHS-yrbmq-vl7dXS*qSD+TCyWM0|46wAwq0Gi0YNw@e-B&$7eo{W?qranfLk9*)L{(m3ua2)sw**Vm3q@Vpd}VG27;0CL?#BMeymPS-l?|0_w$&wNTmw0?gk3'
 'O0o?6O;`=cR)IgLhuQdwu2!4wFRZ&>2@x;Lck%_tw^u^+5-lZ=n_zSxmTMhap)Jo@J;+AvjB-=3%T1dt$alt3(97_ZJ#N91qYB7@8?A#fAsG<gY)QM3K)yGas$j~hB|EaS'
 '5;rDS)!1BB`!?|}?^w*%bt(M-8ig71rbW6dDtV7xlufYSamXl8IMxbgD`j$hT1n!n%h8-wOBPRbISO{GsbpO?<&EsR?TKp*WYa3dK{~fJ7p$^#Z?;h>S<VApco|_|=b5SL'
 'tajF4C0RoL&RMl&Cm?^(+sAd&^Q+C)Pqre)Nm|q1$968h`A*0u_4gK3eroJlGVfITAi?G8kaefpA-nOO6@={q$G}a2W9PtO^430_UgI;8?_loHqdE7QgKcg)_qwcKnX(#?'
 't>A-jlGuT5Vn?!)LT|lZ)&(!kw|W|AA(C2JnASCJB+FjSb5<?cx>vKrdIpRO8w2yPy3}_-zE0&jAHsw1U@hD_6ppljaaPN9CxH`MuviVqV5J2>yS%if)9tiSt)oLN7t5z-'
 'KJ-8XJ{R4yHS^>qCGMG|UZl-|o93(zWF5P4{Z>G`iY`!7MaS(fl$M11_z8f}ci_xVHH_yzW3tX}pl>9L7UgqRJ=v;7xr7LI{Z_=Lek&KsT39He-s5Fnw8xqo(ketPXCZ8u'
 'Vt%$5fLBlhb5<=Gub~ElGZFpL*b&iBwz}nGL0v?)^ZQUya@SQBMu_pvw+vn!^f>|PDY~&%G~>m<bc?DaYpvL<24pL(s0l5Xhs;{8WT13V5RlU`E=rv7GRVm<PmRO+)-ZOW'
 'j~mHKA2zEY*+L)ouBC9)LC_wK19X?X^x<HW58?+qReH*n4v=youAk{3UnVRHQl_kWGFTL(2&)Mnp2qyK?3$%TAP@$+EHyvha}Lh$+|+xhgj}N&k8p9V5^^<(%l8V4^LxUo'
 'A#2zTl|U?}?L<u<DWPPR+vm4;0k1-;_PO2ZU}wsCYnDQ=?A!LL*hqM_DkiK3WQ&X$K~JVau)<zE6oRg;SP>GK7W?L|!nY`F4XeNL{7yE);QUTck;Ynq&DFBci;ZOIVoX>K'
 '$W|^!+HA}NL%1<>g*vSygXwhDM0~eWe1*$vl;XXIu~Nz{&*Zj(ZZ|y3=+lJNgKWfZZ0f4LRDw23rDn<={Egt_Ro&M-b4A{6l#rf|_Ny`_8STw%TKgbbC}GNKK(<msR2#q}'
 'Y6IXA#d{kJ)A;~bVG5<GkAuWl7B>RJtkZ*j0_nbytUdB6tCno-kq3u1jJ3lfjJ3qrw#7LjFe3DoD`{ncW0c&-=G}LAoK`6ew4&<9S}9%7Y>3ha$s!$j%4$HiNJo}vBhNY8'
 '{nIwpAe|!wGg#&QFaK`k<mK8cCxhBr%E|Z}Fjo%Handd2Sgd-oJv##!kiD&+7NJhN(>=<*k0<u%<6FP4+(#$UyR{O-ppGIv^>M5fDt4)xX{D0*<;h~R8j!87HIgT8UTbWL'
 'UOStsrfMD{S?GwbauCH0U?a*?LkqsZWrf*sawA!-4V%@FY!Oc+Hv#B^Ccq0V8rMM~VP9?Z;Q+7rK}*&A)Ek=>=hJqQuzVv~CG$^N^<=AL{#*`WTD|O!$^7E`&5B9)iQ=6r'
 '0-BAqq^u|zy!^eJRr;tw>9BZqrdIOBk((`(N<t^JVzC;K(Ml^e4pACA;t(Z=v5mU3l3eDCWCwPj1g@)lpoH2)aB5`9*6Ms^`r52|vh`(D?VxDfeem%dmwIz`*r5D=`@(56'
 'yf?$ZXXp><kMZ<0z*s4vAp7BLWmFPc9w0WW0of8=vnFd=NVC)+&34DQhAw;U?F%9j4G!-(1YXWFkz1<FkWF_atGTyX4aw$n&j9vW0Y@ONba=Eykive_HTC^k7yvnzr=}LB'
 '5d3?RwHB5T{6ls_Eu=%(%nb4I*se3}F5D@>efWwg<nG$t%;!8(<5Z3%rC_@E#F9`Z%Tw8A)sn5VHHE~iu1I6Y>WX|7;=3yig-Khfz1QdH&0*V&6*f|eSOKb7N=RX@luEKX'
 'rEOM2viT|PQO!-sV+1Q2;(HWwFX%5z8A;Z0kM^6N5||3n8PM#GgqEwsVl^aN>`!EeKRh+zk8~nnrv(p}-Nfcje7&8No(36eA=e_x+<2`dtLyM7tDbCq9S$%#s+*IVItL<e'
 'XP2!6*1oe>dRh_ehdO}8jZ!8l*ov^!0i3u|>e-FY3m+%9;jXz2r_gGFH0sQO*a4S9q&L@G<cyF$cAoJR$kIx#cK7p@fpEfun6PTeVC_M0tuh=h4bQ<kFqk?h?Br+LIffbV'
 'W`B3CZ;sRkWf&|Q<zWK_DoxYe+`z3Q5SLq*&8jEcvm5)}Y#$(jn*$`&E*%HITghK{fQ)cv^Hbv(&vO#z@$!uXm&?RvH6UB&7&<!vU0sua9-{9YZGy{My7(mgAP}g|%jgL4'
 '<#syEu3sxjTsE|rvTDiVx}k-=64*l%1?)nzaTf%<`W{I!Vvhlwo(>`6eZe1<+3s{FS!5_oSq;cm84ApHdl%-Wy$gHof*^UDzG`<;t*yx#EoekT3tYfa>^zYh$tn~zWz~}{'
 'Ls24jkw$KGc8tw(7m7;G?`5rPeKM`JUklMW4r47yG5Kk>7HV~4Sgr+|RZDhcH`ao;vvrcM*XFTxJ5;Xsn82Fe@<M{fWd1*n)%WaB(e02^4^N||6@Xd(!JPz8XvJbRAj6eb'
 '?2-*}&HfZyeE8BO@Un9|?M@H}ItXg(Kxd6aZX|e02fAt`GGaH@0hoA!X^dJeMCj7dHBO-qn7~W<88E)t^-IQq?=QO})lLKht%PD>+VD7qgkTvupR#Jm8g|CDs<$sNh?}qo'
 '64==XoLkagwoH7W!7^bQ!ln7qMMb9&EmanNxlC+U4Ozp^*s9cCCN&#<U$jA8by=AC-`<g{@E{>(VE;F-5~*!@8>I+?ajM5sQsHJwDI_P9VzcVW)=GIQJKa-vsw-TVc$+M%'
 'f&U#fk{fEF7Pd_A9V-bhgNIXAEg7$ahoRaX<0uGHQ&r}=W}fQy1x+8eaFa1N07FPG14K&;xH!iv$*T9%lvPiL%V43}Emc{Yma1Hl`VMOW!ev#+$$o&owl6=`@s5<Q7U1Ym'
 'cUmYU5SLo8S@mRlc4IAw2@H};`HrzDX6}6MNbV}1o1Nd95gxVRT^z@vmIX4Eq^S`bYvD|R%!u8%mJGzy0@hXv`&zQrV!{BQo_Piyy#HKNh}xOwtwv>nd(F4#-Xaz|Wz~{}'
 'bu1QZ7tZm_JAXWpAwv89qYv@A_djQ4_F710LKthIw)(ZxKloN9K}#*ztXi_LzSu^#PvqR6<@o+nSLdDfpjmU_q_?0>vHbJRfXnQH3xlN*#`Q^jD}9(QjY5KMZYE7x^<;Z?'
 'W2K0FKxY8gqf)r5Z-xC`5%+#Jr5WLv8?KVRxs)<ZKfz$71V2%V9`8>FN<nvQ`e-V}VAYT{?2IyS2lmA`Ky&fkr62g*_5C751a>NYjJ!OJV=Yj(8FgByBnv-o%4$Hi^5e*v'
 'P<CnT2xTXz-3=Zo=~Wdk^^T;|LVg-7EuhW{Ebb)hv)g7hAX}c@k?pfPiZ)t^*zHgnukz#aeD`4R`Ki)G3lQdOA=#t|=d=J}y%rjFh8EmJ3(#r-C%{rkA>k@);5_va1_~KN'
 'Al#>Cu{q_~X*i(}i&aYo>q4+J!?rQig65=y+)osO{<3a`oILEl8D4%mOc>;Hzo`rBwvep6yD6)lZ0X&3+r{vWZR@c+E4;E?w@a~l?sdRA1ditd!HCBzp-u-<u!+_>uvxWa'
 'M|M`~tV~j8o78^jw7@Rvby^5QgRLn@Fzlw#xze2$La|ESa%-|#wPd>N;UeBPn|!MSrY_dx1gG8R)zb~77X5o~?id}oUMJ}(8OMcgT^%PYqmZ1iWo=eH+0vHvlN^6&mqnLR'
 'o-cx|Wr05=Gxweg6^Q7~DnZw$rj@70ff2X<L=%<7u7%N9B{r)e*&@R{P7<MFn+R3=E*?VT`@r~zuZhxpM;$B-azoT2E-+~{Ul^H$vn9G%7!y`4*^!;01#gGOD+usj3kv-O'
 '#bvMtj4aYSdFiQPJmtK5ob}zgljv$KOjr%bbhQ@Hb}twk_aa$`6Ze8Mc9*p*(q?tf_%A=-yjN)u-|I(}4E@|vizoW2O<rWFADdM}*03|cKVmSiw4)O%=U)^%*a;A7l@gPY'
 'lcBD~`MjXstdyuYSTwPU+A2lL9fO}K<xa8yIhe8<kkJy6#ooF^o2^SiS56s{6?56GMh@op$2d6{wjil<qBF?OLV|9IML1>EkTvWKEw~9T4m7wp(pOuc&;;Qb)a8U{Nj6pt'
 '`d<8xWQ@DPg?CQ<;jvP%+|tQRDW&Dt$_1ISYROKtk*r-FjTSvkhaQo(*)UXK?oY0UG{s>w@k3y>`!v9M;$ePJOj!-d=B*1|FzY7M*wNKUQxDQjy(j+k+z;}W^V8uiw`^BE'
 'lft|^iJp1sbyb)*VmF?9F^#A^)!H<sYT+KEiU~yYb{!;JyFZgjgp4!Umw1pfGx~2Nu-<rVRy`TGb;~xHv}|iy@DO+Hj~K4Itw{4da3?4@&l4>iup38Oi22oZXS6V6)sh|A'
 'jkVyxZXoHgX&?!0^OxM<&|ha6Y4q4@L1Q$uz`3(>GAbu)p+>r4xfX0zEt#$x<$<@Cf)7U>;Ghv$`0&Jg=LZ+p`Dq+aeC-FNvxA_L#O3R6vl@`a#q00bOf*YlfemjhIO>}f'
 '`aO_=d{`k62e+r`X)t_*u*e9%li<=S3MVqcN9@Lkrryry$YIkX2ryv>Gb=1?2Vf%yB_Pc16IFPwH#!NPWpHDco_ZZ?B_;!A)-LZ(qD!sVtOjI@pi?@2Z7%^xeREd#c+5FB'
 ';Uj1D-)Bbu_Ve%32meQY=-B0d`_mu);U9la|7Y?a6VpHY3xywIAj#4Cm|H{8pZ^&q-hcKN%s(X0jtW2$M<4V*|NO>##|D)O8%fl`hva#Nm;?KFHlBV^59wWtIf4}x|MNfp'
 '^Zy3`zYC@'
)

def all_events(trades):
    events = []
    for i, trade in enumerate(sorted(trades, key=lambda t: (t['entry'], t['sid']))):
        events.append((trade['entry'], 1, trade['sid'], i, trade))
        events.append((trade['exit'], 0, trade['sid'], i, trade))
    events.sort(key=lambda a: (a[0], a[1], a[2], a[3]))  # exit before entry
    return events

def equity(trades, include_events=False):
    ordered = sorted(trades, key=lambda t: (t['entry'], t['sid']))
    balance = 100.0
    peak = 100.0
    max_closed = 0.0
    max_floor = 0.0
    max_open = 0
    max_open_risk = 0.0
    at_exit = []
    open_positions = {}
    outstanding_risk = 0.0
    samples = []
    candidate_with_audusd = 0
    candidate_with_eurgbp = 0
    candidate_with_eurusd = 0
    candidate_with_others = 0
    candidate_entries = 0
    for when, kind, sid, idx, t in all_events(ordered):
        if kind == 0:
            if idx not in open_positions:
                raise RuntimeError(f'Exit without entry {sid} {when}')
            risk_cash, tr = open_positions.pop(idx)
            outstanding_risk -= risk_cash
            balance += risk_cash * float(t['r'])
            if balance <= 0:
                raise RuntimeError('Non-positive backtest equity')
            peak = max(peak, balance)
            max_closed = min(max_closed, 100 * (balance / peak - 1))
            at_exit.append((when, balance, sid, t['r'], risk_cash))
        else:
            rp = .0075 if sid == 'EUR_JPY_M15_SHORT' else .01
            risk_cash = balance * rp
            outstanding_risk += risk_cash
            open_positions[idx] = (risk_cash, t)
            if sid == 'EUR_AUD_H1_LONG':
                candidate_entries += 1
                if any(v['pair']=='AUD_USD' for _,v in open_positions.values() if v is not t):
                    candidate_with_audusd += 1
                if any(v['pair']=='EUR_GBP' for _,v in open_positions.values() if v is not t):
                    candidate_with_eurgbp += 1
                if any(v['pair']=='EUR_USD' for _,v in open_positions.values() if v is not t):
                    candidate_with_eurusd += 1
                if len(open_positions) > 1:
                    candidate_with_others += 1
        max_open = max(max_open, len(open_positions))
        max_open_risk = max(max_open_risk, 100 * outstanding_risk / balance)
        max_floor = min(max_floor, 100 * ((balance-outstanding_risk)/peak-1))
        if include_events:
            samples.append({'event_time':when,'kind':'EXIT' if kind==0 else 'ENTRY',
                            'strategy':sid,'realised_equity':balance,
                            'open_positions':len(open_positions),
                            'open_risk_cash':outstanding_risk,
                            'open_risk_pct':100*outstanding_risk/balance,
                            'closed_dd_pct':100*(balance/peak-1),
                            'floor_dd_pct':100*((balance-outstanding_risk)/peak-1)})
    return {'balance':balance, 'max_closed_dd':max_closed,
            'max_floor_dd':max_floor,'max_open':max_open,
            'max_open_risk_pct':max_open_risk,'exit_events':at_exit,
            'event_rows':samples, 'candidate_entries':candidate_entries,
            'candidate_overlap_any':candidate_with_others,
            'candidate_overlap_audusd':candidate_with_audusd,
            'candidate_overlap_eurgbp':candidate_with_eurgbp,
            'candidate_overlap_eurusd':candidate_with_eurusd}

def balance_at(sim, when):
    times = sim['exit_times']
    ix = bisect.bisect_right(times, when) - 1
    return sim['exit_balances'][ix] if ix >= 0 else 100.0

def add_balance_index(sim):
    sim['exit_times'] = [x[0] for x in sim['exit_events']]
    sim['exit_balances'] = [x[1] for x in sim['exit_events']]
    return sim

def calendar_anchor(now, yrs):
    return now - dt.timedelta(days=365.2425*yrs)

def cagr(sim, first, last):
    years = (last-first).total_seconds()/(365.2425*86400)
    return ((sim['balance']/100.)**(1/years)-1)*100 if years>0 else None

def summary(label, trades, sim, first, last):
    winner=[t for t in trades if t['r']>0]
    loser=[t for t in trades if t['r']<0]
    weighted_r=sum(t['r']*(.75 if t['sid']=='EUR_JPY_M15_SHORT' else 1.) for t in trades)
    return {'candidate':label,'portfolio_strategies':len({t['sid'] for t in trades}),
            'accepted_trades':len(trades),'accepted_candidate':sim['candidate_entries'],
            'candidate_r':sum(t['r'] for t in trades if t['sid']=='EUR_AUD_H1_LONG'),
            'weighted_r_equivalent_at_1pct':weighted_r,'winners':len(winner),'losers':len(loser),
            'win_rate_pct':100*len(winner)/len(trades),
            'profit_factor':sum(t['r'] for t in winner)/abs(sum(t['r'] for t in loser)),
            'ending_balance_from_100':sim['balance'],'historical_cagr_pct':cagr(sim,first,max(parse(t['exit']) for t in trades)),
            'max_closed_equity_dd_pct':sim['max_closed_dd'],
            'max_open_risk_floor_dd_pct':sim['max_floor_dd'],
            'max_open_positions':sim['max_open'],
            'max_open_risk_pct_of_realised_equity':sim['max_open_risk_pct'],
            'candidate_entries_overlapping_any_incumbent':sim['candidate_overlap_any'],
            'candidate_entries_overlapping_AUD_USD':sim['candidate_overlap_audusd'],
            'candidate_entries_overlapping_EUR_GBP':sim['candidate_overlap_eurgbp'],
            'candidate_entries_overlapping_EUR_USD':sim['candidate_overlap_eurusd'],
            'source_cutoff':CUTOFF}

def period_rows(label, sim, cutoff):
    out=[]
    for years in (1,2,3,5):
        start=calendar_anchor(cutoff,years)
        a=balance_at(sim, stamp(start)); b=balance_at(sim,CUTOFF)
        out.append({'candidate':label,'period':f'LAST_{years}Y',
                    'start':stamp(start),'end':CUTOFF,'start_balance':a,'end_balance':b,
                    'compounded_return_pct':100*(b/a-1),
                    'annualized_return_pct':100*((b/a)**(1/years)-1)})
    return out

def monthly_rows(label,sim,first,cutoff):
    out=[];y=first.year;m=first.month
    while (y,m)<=(cutoff.year,cutoff.month):
        start=dt.datetime(y,m,1,tzinfo=dt.timezone.utc)
        yy,mm=(y+1,1) if m==12 else (y,m+1)
        end=min(dt.datetime(yy,mm,1,tzinfo=dt.timezone.utc),cutoff)
        a=balance_at(sim,stamp(start)); b=balance_at(sim,stamp(end))
        out.append({'candidate':label,'month':f'{y:04d}-{m:02d}',
                    'period_start':stamp(start),'period_end':stamp(end),
                    'partial_month':y==cutoff.year and m==cutoff.month,
                    'start_balance':a,'end_balance':b,
                    'return_pct':100*(b/a-1)})
        y,m=yy,mm
    return out

def rolling_rows(label,sim,first,cutoff):
    months=monthly_rows(label,sim,first,cutoff)
    out=[]
    for span in (12,24,36):
        for ix in range(span-1,len(months)):
            start=months[ix-span+1]['period_start'];end=months[ix]['period_end']
            a=balance_at(sim,start);b=balance_at(sim,end)
            out.append({'candidate':label,'months':span,'start':start,'end':end,
                        'includes_partial_end_month':months[ix]['partial_month'],
                        'compounded_return_pct':100*(b/a-1)})
    return out

def rolling_summary_rows(rows):
    grouped=defaultdict(list)
    for r in rows:
        # Omit partial-September end month from comparable completed windows.
        if not r['includes_partial_end_month']:
            grouped[(r['candidate'],r['months'])].append(float(r['compounded_return_pct']))
    return [{'candidate':candidate,'months':n,'completed_windows':len(vals),
             'pct_positive':100*sum(v>0 for v in vals)/len(vals),
             'worst_return_pct':min(vals),'median_return_pct':statistics.median(vals),
             'best_return_pct':max(vals)}
            for (candidate,n),vals in sorted(grouped.items()) if vals]

def yearly_rows(month_rows):
    grouped=defaultdict(list)
    for m in month_rows:
        grouped[(m['candidate'],m['month'][:4])].append(m)
    out=[]
    for (label,year),months in sorted(grouped.items()):
        ret=100*(months[-1]['end_balance']/months[0]['start_balance']-1)
        out.append({'candidate':label,'year':year,
                    'months_in_year':len(months),
                    'partial_year':year=='2026',
                    'compounded_return_pct':ret})
    return out



def pair_raw(data,h1,m15):
 idx_h1={iso(x['time']):i for i,x in enumerate(h1)}
 idx_m15={iso(x['time']):i for i,x in enumerate(m15)}
 base_idx={(x['sid'],x['signal']):x for x in data['baseline'] if x['pair']=='AUD_USD'}
 if len(base_idx)!=357:raise RuntimeError('Accepted AUD/USD 27 snapshot !=357')
 out=[];diffs=[]
 for s in data['raw_pair']:
  k=(s['sid'],s['signal']);tf=s['tf'];side=s['side']
  b=h1 if tf=='H1' else m15;table=idx_h1 if tf=='H1' else idx_m15
  if s['signal'] not in table:raise RuntimeError('Missing incumbent source signal '+str(k))
  if k in base_idx:
   t=dict(base_idx[k]);strict(t['r'],s['r_ref'],'incumbent source R '+str(k),2e-5)
  else:
   rr=3.25 if side=='BUY' else 3.5
   cost=.5 if tf=='H1' else 1.
   z=raw_outcome(b,table[s['signal']],side,tf,rr,cost)
   if z is None:raise RuntimeError('Rejected incumbent has no exit '+str(k))
   strict(z['r'],s['r_ref'],'recovered raw incumbent R '+str(k),2e-5)
   t={'sid':s['sid'],'pair':INSTRUMENT,'side':side,'tf':tf,
      'signal':z['signal'],'entry':z['entry'],'exit':z['exit'],'r':z['r'],'rr':rr}
   diffs.append({'sid':s['sid'],'signal':s['signal'],'recovered_r':z['r']})
  if t['entry']!=s['entry']:raise RuntimeError('Incumbent source entry timing mismatch '+str(k))
  out.append(t)
 return out,diffs

def candidate_raw(data,m15,indices,cost):
 all_trades=[];missing=0
 for i in indices:
  t=raw_outcome(m15,i,'BUY','M15',RR,cost)
  if t is None:missing+=1;continue
  if t['exit']>CUTOFF:continue
  all_trades.append({'sid':SID,'pair':INSTRUMENT,'side':'BUY','tf':'M15',
   'signal':t['signal'],'entry':t['entry'],'exit':t['exit'],'r':t['r'],'rr':RR,
   'signal_index':i,'exit_index':t['exit_index']})
 return all_trades,missing

def rank(t,mode,side_order):
 return (t['entry'],0 if t['tf']==mode.split('_')[0] else 1,
         0 if ((t['side']=='BUY')==(side_order=='BUY_FIRST')) else 1,t['sid'])

def pair_gate(raw,mode='H1_FIRST',side_order='BUY_FIRST'):
 # Candidate and incumbents reprocessed from all RAW eligible signals;
 # rejected signals don't block subsequent same-ID entries.
 raw=sorted(raw,key=lambda x:rank(x,mode,side_order))
 held={};accept=[];events=[]
 for t in raw:
  for sid,open_t in list(held.items()):
   if open_t['exit']<=t['entry']:del held[sid]
  opp=sorted((x for x in held.values() if x['side']!=t['side']),key=lambda x:(x['exit'],x['sid']))
  blocker=held.get(t['sid']) or (opp[0] if opp else None)
  reason='STRATEGY_PYRAMIDING_0' if t['sid'] in held else ('NON_HEDGING_OPPOSITE_OPEN' if opp else 'ACCEPTED')
  if reason=='ACCEPTED':held[t['sid']]=t;accept.append(t)
  events.append({'mode':mode,'side_priority':side_order,'entry':t['entry'],
     'sid':t['sid'],'side':t['side'],'signal':t['signal'],'r':t['r'],
     'decision':reason,'blocker':blocker['sid'] if blocker else '',
     'blocker_until':blocker['exit'] if blocker else ''})
 return accept,events

def source_stats(rows):
 r=[x['r'] for x in rows];return {'trades':len(rows),'total_r':sum(r),'pf':sum(x for x in r if x>0)/(-sum(x for x in r if x<0)) if any(x<0 for x in r) else 0}

def accepted_parity(base,ctrl,mode,side_order):
 reference=sorted((identity(x) for x in base if x['pair']=='AUD_USD'))
 actual=sorted(identity(x) for x in ctrl)
 if actual!=reference:
  ref=set(reference);act=set(actual)
  raise RuntimeError(f'27 AUD/USD pair parity {mode}/{side_order}: missing={list(ref-act)[:2]} extra={list(act-ref)[:2]}')
 return {'test':'27 raw-pair exact accepted identity','mode':mode,'priority':side_order,
         'accepted':len(actual),'result':'PASS'}

def portfolio_metrics(label,trades,sim,cutoff):
 wins=[x for x in trades if x['r']>0];los=[x for x in trades if x['r']<0]
 first=parse(min(x['entry'] for x in trades));last=parse(max(x['exit'] for x in trades))
 return {'candidate':label,'accepted_trades':len(trades),'strategy_ids':len(set(x['sid'] for x in trades)),
   'weighted_r':sum(x['r']*(.75 if x['sid']=='EUR_JPY_M15_SHORT' else 1.) for x in trades),
   'candidate_trades':sum(x['sid']==SID for x in trades),
   'candidate_r':sum(x['r'] for x in trades if x['sid']==SID),
   'ending_balance_from_100':sim['balance'],
   'historical_cagr_pct':cagr(sim,first,last),
   'max_closed_dd_pct':sim['max_closed_dd'],'max_open_risk_floor_dd_pct':sim['max_floor_dd'],
   'max_positions':sim['max_open'],'max_open_risk_pct':sim['max_open_risk_pct'],
   'winners':len(wins),'losers':len(los),
   'profit_factor':sum(x['r'] for x in wins)/(-sum(x['r'] for x in los)) if los else 0,
   'source_cutoff':CUTOFF,'candidate_risk_pct':1.0 if label!='CONTROL27' else 0}

def csv_bundle(reports):
 paths=[]
 for name,rows in reports.items():
  p=OUT/f'aud28_{name}.csv';export_csv(p,rows);paths.append(p)
 with zipfile.ZipFile(BUNDLE,'w',zipfile.ZIP_DEFLATED,compresslevel=7) as z:
  for p in paths:z.write(p,arcname=p.name)

def research():
 reports={}
 try:
  OUT.mkdir(parents=True,exist_ok=True)
  status('archive',4,'Verifying archived 27-portfolio, 401 raw pair signals and 283-trade frozen ledger')
  data,digest=embed_load();base=data['baseline'];base_ids=[identity(x) for x in base]
  if len(set(base_ids))!=3029 or len(set(x['sid'] for x in base))!=27:raise RuntimeError('Baseline count/uniqueness failure')
  control_sim=add_balance_index(equity(base))
  control=portfolio_metrics('CONTROL27',base,control_sim,CUTOFF)
  for name,x,y in [('trades',control['accepted_trades'],3029),('CAGR',control['historical_cagr_pct'],BASELINE_EXPECT['cagr']),
   ('weighted_R',control['weighted_r'],BASELINE_EXPECT['weighted_r']),('ending_balance',control_sim['balance'],BASELINE_EXPECT['balance']),
   ('closed_dd',control_sim['max_closed_dd'],BASELINE_EXPECT['closed_dd']),('floor_dd',control_sim['max_floor_dd'],BASELINE_EXPECT['floor_dd']),
   ('open_positions',control_sim['max_open'],BASELINE_EXPECT['open_positions']),
   ('open_risk_pct',control_sim['max_open_risk_pct'],BASELINE_EXPECT['max_open_risk_pct'])]:strict(x,y,'27 '+name)
  reports['baseline_parity']=[{'check':'embedded source sha256','value':digest,'result':'PASS'},
    {'check':'portfolio 27 baseline','trades':3029,'strategy_ids':27,'result':'PASS'},
    {'check': 'portfolio 27 replay',**control,'result':'PASS'}]
  status('fetch',8,'Fetching frozen AUD/USD M15 midpoint history')
  start=parse('2002-05-06T20:00:00Z');end=parse('2026-09-22T21:45:00Z')
  m15=fetch('M15',start,end,45);h1=fetch('H1',start,end,170)
  m15=[x for x in m15 if x['time']<=parse(PARITY_CANDLE_CUTOFF)]
  if iso(m15[0]['time'])!=FIRST_M15 or iso(h1[0]['time'])!=FIRST_H1:
   raise RuntimeError('OANDA initial candle shifted: strict historical coverage failure')
  if len(m15)!=547044 or iso(m15[-1]['time'])!=PARITY_CANDLE_CUTOFF:
   raise RuntimeError(f'Stage 3 candle parity failed: {len(m15)} last={iso(m15[-1]["time"])} expected=547044')
  reports['coverage']=[{'pair':INSTRUMENT,'m15_count':len(m15),'h1_count':len(h1),
    'm15_first':iso(m15[0]['time']),'m15_last':iso(m15[-1]['time']),
    'portfolio_cutoff':CUTOFF,'orders_supported':False}]
  status('candidate',45,'Rebuilding only frozen 0.05 and 0.10 LONG signals')
  a=atr14(m15);candidate_sets={};raw_sets={};standalones=[];cost_rows=[];parity=[]
  for threshold,label in [(.05,'P0.050'),(.10,'P0.100')]:
   ix=candidate_indices(m15,a,threshold);raw_sets[label]=ix
   tr1=standalone(m15,ix,'BUY','M15',RR,1.);tr2=standalone(m15,ix,'BUY','M15',RR,2.)
   ex=data['expect'][label]
   strict(len(tr1),ex['trades'],label+' archived trades',0)
   strict(sum(x['r'] for x in tr1),ex['r'],label+' archived R',2e-5)
   strict(sum(x['r'] for x in tr2),ex['2pip'],label+' doubled cost R',2e-5)
   if label=='P0.100':
    old=data['ledger_010']
    for j,(actual,src) in enumerate(zip(tr1,old)):
     for field in ('signal_index','exit_index'):
      if int(actual[field])!=int(src[field]):raise RuntimeError(f'283 ledger {field} mismatch row {j}')
     for field in ('entry_time','exit_time','entry_time_utc','exit_time_utc'):
      intended=actual['signal_open'] if field in ('entry_time','entry_time_utc') else actual['exit_open']
      if intended!=src[field]:raise RuntimeError(f'283 ledger {field} mismatch row {j}')
     for field in ('reference_entry','historical_fill','stop','target','result_r','rr','cost_pips'):
      val=actual['r'] if field=='result_r' else actual[field]
      strict(val,float(src[field]),f'283 ledger {field} row {j}',1e-8)
     if actual['exit_reason']!=src['exit_reason']:raise RuntimeError(f'283 ledger exit mismatch row {j}')
    parity.append({'check':'archived 0.10 283-trade field parity','rows':len(old),'result':'PASS'})
   parity.append({'check':label+' standalone archived summary','rows':len(tr1),'r':sum(x['r'] for x in tr1),'result':'PASS'})
   standalones.append({'candidate':label,**source_stats(tr1),'cost_pips':1.,'raw_signals':len(ix)})
   cost_rows.append({'candidate':label,'cost_pips':2.,**source_stats(tr2)})
   candidate_sets[label],_ =candidate_raw(data,m15,ix,1.)
  reports['candidate_parity']=parity;reports['standalone']=standalones;reports['cost_stress']=cost_rows
  status('incumbents',66,'Rebuilding original 401 raw incumbent entries; hard-gating exact 357-trade pair control')
  existing,reconstructed=pair_raw(data,h1,m15)
  reports['recovered_incumbent_raw']=reconstructed
  nonpair=[x for x in base if x['pair']!=INSTRUMENT]
  if len(nonpair)!=2672:raise RuntimeError('Frozen non AUD/USD incumbent count !=2672')
  allsummaries=[control];periods=period_rows('CONTROL27',control_sim,parse(CUTOFF))
  months=monthly_rows('CONTROL27',control_sim,parse(min(x['entry'] for x in base)),parse(CUTOFF))
  rolling=rolling_rows('CONTROL27',control_sim,parse(min(x['entry'] for x in base)),parse(CUTOFF))
  gates=[];events=[];attributions=[];risk_events=[];entry_trades=[]
  for mode,priority in [('H1_FIRST','BUY_FIRST'),('H1_FIRST','SELL_FIRST'),('M15_FIRST','BUY_FIRST'),('M15_FIRST','SELL_FIRST')]:
   ctrl,ce=pair_gate(existing,mode,priority);accepted_parity(base,ctrl,mode,priority)
   gates.append({'mode':mode,'priority':priority,'scenario':'CONTROL27','accepted_pair':len(ctrl),'result':'PASS'})
   for label in ('P0.050','P0.100'):
    status('portfolio',72 if label=='P0.050' else 82,f'{label} chronological pair replay {mode} {priority}')
    p,ev=pair_gate(existing+candidate_sets[label],mode,priority)
    before={(x['sid'],x['signal']):x for x in ctrl}
    after={(x['sid'],x['signal']):x for x in p}
    adds=[x for x in p if x['sid']==SID]
    if not adds:raise RuntimeError('No accepted candidate trades '+label)
    surviving=[x for x in p if x['sid']!=SID]
    for sid in sorted({x['sid'] for x in ctrl}):
     bk={k:v for k,v in before.items() if k[0]==sid}
     ak={k:v for k,v in after.items() if k[0]==sid}
     lost=[bk[k] for k in bk.keys()-ak.keys()];new=[ak[k] for k in ak.keys()-bk.keys()]
     attributions.append({'candidate':label,'mode':mode,'priority':priority,'sid':sid,
       'baseline':len(bk),'prospective':len(ak),'displaced_count':len(lost),
       'displaced_r':sum(x['r'] for x in lost),'newly_eligible_count':len(new),
       'newly_eligible_r':sum(x['r'] for x in new)})
    full=sorted(nonpair+p,key=lambda x:(x['entry'],x['sid']))
    if len(set(x['sid'] for x in full))!=28:raise RuntimeError('28 IDs gate failure')
    if sorted(identity(x) for x in nonpair)!=sorted(identity(x) for x in base if x['pair']!=INSTRUMENT):
     raise RuntimeError('Non AUD/USD strategy displaced, impossible')
    sim=add_balance_index(equity(full,include_events=True))
    name=label+'_'+mode+'_'+priority
    res=portfolio_metrics(name,full,sim,parse(CUTOFF));allsummaries.append(res)
    periods+=period_rows(name,sim,parse(CUTOFF))
    first=parse(min(x['entry'] for x in base));months+=monthly_rows(name,sim,first,parse(CUTOFF))
    rolling+=rolling_rows(name,sim,first,parse(CUTOFF))
    ev_selected=[dict(x,candidate=label) for x in ev]
    events+=ev_selected
    entry_trades.extend(dict(x,candidate=name) for x in p)
    risk_events.extend(dict(x,candidate=name) for x in sim['event_rows'] if x['strategy']==SID)
    gates.append({'candidate':label,'mode':mode,'priority':priority,
      'raw_candidate_signals':len(raw_sets[label]),
      'candidate_standalone':next(x['trades'] for x in standalones if x['candidate']==label),
      'candidate_raw_with_exit':len(candidate_sets[label]),'candidate_accepted':len(adds),
      'candidate_accepted_r':sum(x['r'] for x in adds),
      'candidate_rejected_own_p0':sum(x['sid']==SID and x['decision']=='STRATEGY_PYRAMIDING_0' for x in ev),
      'candidate_rejected_opposite':sum(x['sid']==SID and x['decision']=='NON_HEDGING_OPPOSITE_OPEN' for x in ev),
      'incumbent_accepted':len(surviving),'incumbent_displaced':len(set(before)-set(after)),
      'incumbent_newly_eligible':len(set(after)-set(before)-{k for k in after if k[0]==SID}),
      'portfolio_trades':len(full),'result':'REPORT_ONLY_NO_AUTO_PROMOTION'})
  reports['pair_gates']=gates;reports['portfolio_summary']=allsummaries
  reports['delta_vs_27']=[{'candidate':x['candidate'],'metric':k,'control27':control[k],
    'prospective28':x[k],'delta':x[k]-control[k]}
    for x in allsummaries[1:] for k in ('accepted_trades','weighted_r','historical_cagr_pct',
        'max_closed_dd_pct','max_open_risk_floor_dd_pct','max_positions','max_open_risk_pct')]
  reports['portfolio_periods']=periods;reports['monthly']=months;reports['rolling']=rolling
  reports['rolling_summary']=rolling_summary_rows(rolling);reports['calendar']=yearly_rows(months)
  reports['pair_attribution']=attributions;reports['pair_events']=events
  reports['accepted_pair_trades']=entry_trades;reports['candidate_risk_events']=risk_events
  reports['methodology']=[{'item':'fixed_snapshot','value':CUTOFF},
   {'item':'candidate_source','value':'P0.050 and P0.100 frozen after fully explored penetration 0.00-0.30; NOT independent holdout'},
   {'item':'p0','value':'Only accepted entries consume strategy pyramiding 0; exit candle signal eligible'},
   {'item':'nonhedging','value':'Opposite AUD/USD sides cannot overlap; same direction separate IDs may overlap'},
   {'item':'historical_cost','value':'1-pip candidate adverse; 2-pip standalone stress ONLY; incumbent historical costs unchanged'},
   {'item':'risk','value':'1% realised balance at entry; EURJPY M15 short 0.75%; no aggregate currency caps'},
   {'item':'status','value':'READ_ONLY. No parameter optimisation, portfolio forecasts, or automatic deployment.'}]
  status('write',96,'Packaging completed full reports')
  csv_bundle(reports);status('complete',100,'Full frozen 27→28 comparisons complete')
 except Exception as exc:
  reports['error']=[{'error_type':type(exc).__name__,'message':str(exc),'traceback':traceback.format_exc()}]
  try:OUT.mkdir(parents=True,exist_ok=True);csv_bundle(reports)
  except Exception:pass
  with LOCK:STATE.update(state='error',message=f'{type(exc).__name__}: {exc}',traceback=traceback.format_exc())
  print(traceback.format_exc(),file=sys.stderr,flush=True)

if Flask:
 app=Flask(__name__)
 @app.get('/audusd-m15-long-27-to-28/status')
 def web_status():return jsonify(dict(STATE))
 @app.get('/audusd-m15-long-27-to-28/results')
 def web_results():
  if not BUNDLE.exists():return jsonify({'error':'Bundle not ready','status':STATE}),409
  return send_file(BUNDLE,as_attachment=True,download_name=BUNDLE.name)
 @app.get('/')
 def web_home():return jsonify({'service':'AUD/USD M15 LONG exact 27-to-28 portfolio research',
    'status':'/audusd-m15-long-27-to-28/status','results':'/audusd-m15-long-27-to-28/results',
    'orders_supported':False,'trading_enabled':False})
if __name__=='__main__':
 if '--run-once' in sys.argv:research()
 else:
  if Flask is None:raise RuntimeError('Install flask or use --run-once')
  threading.Thread(target=research,daemon=True,name='audusd-m15-portfolio').start()
  app.run(host='0.0.0.0',port=int(os.getenv('PORT','8080')),debug=False,use_reloader=False)
