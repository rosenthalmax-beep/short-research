"""Causal features; no broker downloads, future pivots or forward-filled gaps."""
import math
from collections import deque

def ratios(values):
 out=[];q=deque();total=0.;bad=0
 for v in values:
  out.append(v/(total/50) if len(q)==50 and not bad and total>0 and math.isfinite(v) else None)
  q.append(v)
  if math.isfinite(v):total+=v
  else:bad+=1
  if len(q)>50:
   old=q.popleft()
   if math.isfinite(old):total-=old
   else:bad-=1
 return out

def preceding_lows(values,n):
 q=deque();out=[]
 for i,v in enumerate(values):
  while q and q[0][0]<i-n:q.popleft()
  out.append(q[0][1] if i>=n and q else None)
  while q and q[-1][1]>=v:q.pop()
  q.append((i,v))
 return out

def arrays(m,h,av,ha):
 last=[];j=0
 for i,t in enumerate(m.times):
  if i and t!=m.times[i-1]+900:j=i
  last.append(j)
 return dict(m15=ratios(av),h1=ratios(ha),low20=preceding_lows(m.values['mid']['l'],20),low60=preceding_lows(m.values['mid']['l'],60),segment=last)

def extra_features(a,m,h,i,hi,atr):
 p={k:m.values['mid'][k][i-1] for k in 'ohlc'};span=p['h']-p['l']
 out=dict(m15_atr_ratio=a['m15'][i],h1_atr_ratio=a['h1'][hi] if hi>=0 else None,previous_close_location=(p['c']-p['l'])/span if span>0 else None)
 for n in (20,60):
  low=a['low'+str(n)][i] if i-n>=a['segment'][i] else None
  out['prior_low'+str(n)+'_distance_atr']=max(0.,m.values['mid']['l'][i]-low)/atr if low is not None else None
  out['sweeps_prior_low'+str(n)]=m.values['mid']['l'][i]<low if low is not None else None
 return out

def setup_features(a,m,setup,index):
 j=setup['index'];c=m.values['mid']['c'];valid=j>=17 and j-17>=a['segment'][index]
 impulse=c[j-1]-c[j-17] if valid else None
 return dict(pre_touch_rise_4h_atr=impulse/setup['start_atr'] if impulse is not None else None,
  retracement_fraction=max(0.,c[j-1]-setup['extreme'])/impulse if impulse is not None and impulse>0 else None)
