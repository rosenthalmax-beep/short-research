"""Deterministic synthetic candles for offline tests only."""
import math
from collections import OrderedDict
from pullback_lab import core as e

EPOCH=e.dt('2020-01-01T00:00:00Z').timestamp()
OFFSETS=(0.,.0001,.0002,.0003,.0004,.0005,.0006,.0007,.0004,.0001,-.0002,-.0004,.0004,.0005,.0006,.0007)


def boundary(n):
    return 1.25+.0015*math.sin(n/160.)+.0005*math.sin(n*1.05)+.00015*math.sin(n*.3)


def price(t):
    q=(t-EPOCH)/900;i=math.floor(q);f=q-i
    return boundary(i)*(1-f)+boundary(i+1)*f


class SyntheticStore:
    calls=0
    def __init__(self,directory,reader=None,legacy_root=None,progress=lambda **x:None):
        self.memo=OrderedDict();self.downloads=0
    def exact(self,pair,gran,start,end):return self.range(pair,gran,start,end)
    def range(self,pair,gran,start,end):
        SyntheticStore.calls+=1;step=e.STEP[gran];out=[]
        for t in range(int(start.timestamp()),int(end.timestamp()),step):
            points=[price(t),price(t+step)]
            points.extend(price(s) for s in range(((t//900)+1)*900,t+step,900))
            mid=dict(o=points[0],h=max(points),l=min(points),c=points[1])
            out.append(dict(time=e.stamp(e.datetime.fromtimestamp(t,e.UTC)),complete=True,mid=mid,
                            bid={k:v-.00002 for k,v in mid.items()},ask={k:v+.00002 for k,v in mid.items()}))
        return out
    def window(self,pair,start,end):return self.range(pair,'M15',start,end)
    def difference_count(self):return 0
    def index(self):return [dict(origin='SYNTHETIC_OFFLINE_TEST_ONLY')]
    def close(self):pass

