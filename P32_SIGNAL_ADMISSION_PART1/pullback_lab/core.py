"""Read-only price and candle helpers adapted from the corrected P32 replay."""
import hashlib,json,math,threading,time
from datetime import datetime,timedelta,timezone
from decimal import Decimal
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler,Request,build_opener
from zoneinfo import ZoneInfo
UTC=timezone.utc
NY=ZoneInfo('America/New_York')
STEP={'H1':3600,'M15':900,'M1':60,'S5':5}
PAIRS=('AUD_JPY','AUD_USD','EUR_CHF','EUR_GBP','EUR_JPY','EUR_USD','GBP_USD','USD_CAD','USD_JPY')
DAYS=('MONDAY','TUESDAY','WEDNESDAY','THURSDAY','FRIDAY','SATURDAY','SUNDAY')
VERSION='P32_PULLBACK_PASS1_V1_2026_10_20'


def canon(obj):
    return json.dumps(obj, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()

def sha(raw):
    return hashlib.sha256(raw).hexdigest()

def require(condition, message):
    if not condition:
        raise ValueError(message)

def dt(value):
    value = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    require(value.tzinfo is not None, 'Explicit UTC offset required')
    return value.astimezone(UTC)

def stamp(value):
    return dt(value).isoformat().replace('+00:00', 'Z')

def floor_time(value, seconds):
    return datetime.fromtimestamp(int(dt(value).timestamp()) // seconds * seconds, UTC)

def dec(value):
    result = Decimal(str(value))
    require(result.is_finite(), 'Non-finite price')
    return result

def digits(pair):
    require(pair in PAIRS, 'Unapproved pair')
    return 3 if pair.endswith('_JPY') else 5

def tick(pair):
    return Decimal(1).scaleb(-digits(pair))

def broker_price(pair, value):
    # This deliberately matches the supplied live Python executor, including its
    # half-tick behavior. Do not replace this with a different rounding policy.
    require(math.isfinite(float(value)), 'Non-finite order price')
    return dec(format(round(float(value), digits(pair)), '.%df' % digits(pair)))

def levels(pair, side, reference, stop, rr, adverse_limit_ticks=10):
    require(isinstance(adverse_limit_ticks, int) and 10 <= adverse_limit_ticks <= 30, "Entry limit must be whole ticks from 10 to 30")
    require(side in ('BUY', 'SELL') and math.isfinite(float(rr)) and float(rr) > 0, 'Invalid direction/RR')
    sign = 1 if side == 'BUY' else -1
    ref, sl = broker_price(pair, reference), broker_price(pair, stop)
    risk = sign * (float(ref) - float(sl))
    require(risk > 0, 'Reference stop must be on the loss side')
    target = broker_price(pair, float(ref) + sign * risk * float(rr))
    bound = broker_price(pair, float(ref) + sign * adverse_limit_ticks * float(tick(pair)))
    return dict(reference_entry=ref, stop=sl, target=target, price_bound=bound)

def entry_preview(pair, side, reference, stop, rr, bid, ask, adverse_limit_ticks=10):
    result = levels(pair, side, reference, stop, rr, adverse_limit_ticks)
    bid, ask = broker_price(pair, bid), broker_price(pair, ask)
    require(ask >= bid > 0, 'Invalid quote')
    sign = 1 if side == 'BUY' else -1
    fill = ask if sign == 1 else bid
    deviation = max(dec(0), sign * (fill - result['reference_entry']) / tick(pair))
    status = 'ADMITTED_ENTRY_PROXY'
    if deviation > adverse_limit_ticks:
        status = 'REJECTED_' + str(adverse_limit_ticks) + 'T_ENTRY_PROXY'
    elif sign * (fill - result['stop']) <= 0:
        status = 'REJECTED_STOP_GEOMETRY_PROXY'
    elif pair == 'EUR_CHF' and side == 'SELL' and bid <= result['target']:
        status = 'REJECTED_TARGET_ALREADY_PASSED_PROXY'
    result.update(fill=fill, bid=bid, ask=ask, adverse_ticks=deviation,
                  entry_spread_ticks=(ask - bid) / tick(pair), status=status,
                  stop_distance=sign * (fill - result['stop']),
                  worst_stop_distance=sign * (result['price_bound'] - result['stop']))
    return result

class NeedData(Exception):
    def __init__(self, identity):
        self.identity = identity
        super().__init__('Additional native candles required')

class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None

class CandleFetchError(RuntimeError):
    """A controlled, credential-free error message from the candle reader."""

class CandleReader:
    """The only network operation exposed is an allowlisted candle GET."""
    def __init__(self, token, broker, requests_per_second=4, transport=None):
        require(broker in ('https://api-fxtrade.oanda.com', 'https://api-fxpractice.oanda.com'), 'Invalid broker host')
        require(0 < requests_per_second <= 5, 'Request rate must be >0 and <=5')
        self.token, self.broker, self.transport = token, broker, transport
        self.opener = build_opener(NoRedirect())
        self.lock = threading.Lock()
        self.next_at = 0.0
        self.spacing = 1 / requests_per_second
        self.receipts = []
        self.fatal = False

    def get(self, identity):
        pair, gran = identity['pair'], identity['granularity']
        require(pair in PAIRS and gran in STEP, 'Only approved candle requests are supported')
        require(identity['broker'] == self.broker, 'Seed and requested broker differ')
        require(not self.fatal, 'Candle reader stopped after authentication failure')
        params = {'from': identity['start'], 'to': identity['end'], 'granularity': gran,
                  'price': 'MBA', 'smooth': 'false', 'includeFirst': 'true'}
        require(0 < (dt(params['to']) - dt(params['from'])).total_seconds() <= 4800 * STEP[gran], 'Candle request exceeds limit')
        url = self.broker + '/v3/instruments/' + pair + '/candles?' + urlencode(params)
        for attempt in range(3):
            with self.lock:
                delay = max(0, self.next_at - time.monotonic())
                self.next_at = max(time.monotonic(), self.next_at) + self.spacing
            if delay:
                time.sleep(delay)
            req = Request(url, method='GET', headers={'Authorization': 'Bearer ' + self.token,
                          'Accept': 'application/json', 'Accept-Datetime-Format': 'RFC3339', 'User-Agent': VERSION})
            try:
                if self.transport:
                    data = self.transport(req)
                else:
                    with self.opener.open(req, timeout=40) as response:
                        raw = response.read(16 * 1024 * 1024 + 1)
                    require(len(raw) <= 16 * 1024 * 1024, 'Candle response exceeds size limit')
                    data = json.loads(raw)
                require(isinstance(data, dict), 'Candle response must be an object')
                with self.lock:
                    self.receipts.append(dict(method='GET', pair=pair, granularity=gran, params=params,
                                              received_utc=stamp(datetime.now(UTC)), response_sha256=sha(canon(data))))
                return data
            except Exception as exc:
                code = exc.code if isinstance(exc, HTTPError) else None
                if code in (401, 403):
                    self.fatal = True
                with self.lock:
                    self.receipts.append(dict(method='GET', pair=pair, granularity=gran, params=params,
                                              attempt=attempt + 1, error='HTTP_' + str(code) if code else type(exc).__name__))
                if code in (400, 401, 403, 404) or isinstance(exc, ValueError) or attempt == 2:
                    raise CandleFetchError('Candle GET failed: ' + ('HTTP ' + str(code) if code else type(exc).__name__)) from None
                time.sleep(0.5 * (attempt + 1))

def conventional_weekend(t):
    local = dt(t).astimezone(NY)
    return local.weekday() == 5 or (local.weekday() == 4 and local.hour >= 17) or (local.weekday() == 6 and local.hour < 17)

def coverage(rows, start, end, seconds):
    available = {dt(x['time']) for x in rows}
    missing, weekend = [], 0
    t = start
    while t < end:
        if t not in available:
            if conventional_weekend(t):
                weekend += 1
            else:
                missing.append(stamp(t))
        t += timedelta(seconds=seconds)
    return dict(unexplained_missing_slots=len(missing), assumed_weekend_slots=weekend,
                first_unexplained_gap_utc=missing[0] if missing else None)

def touch(candle, component, side, stop, target):
    p = {k: dec(candle[component][k]) for k in 'ohlc'}
    sign = 1 if side == 'BUY' else -1
    if sign * (p['o'] - stop) <= 0:
        return dict(kind='STOP', exit_price=float(p['o']), open_gap=p['o'] != stop)
    if sign * (p['o'] - target) >= 0:
        return dict(kind='TARGET', exit_price=float(p['o']), open_gap=p['o'] != target)
    stop_hit = p['l'] <= stop if sign == 1 else p['h'] >= stop
    target_hit = p['h'] >= target if sign == 1 else p['l'] <= target
    if stop_hit and target_hit:
        return dict(kind='AMBIGUOUS_BOTH', exit_price=None, open_gap=False)
    if stop_hit:
        return dict(kind='STOP', exit_price=float(stop), open_gap=False)
    if target_hit:
        return dict(kind='TARGET', exit_price=float(target), open_gap=False)
    return None

def crosscheck(parent, children, component, count, pair):
    if not children:
        return 'NO_CHILD_DATA'
    tolerance = tick(pair)
    p = {k: dec(parent[component][k]) for k in 'ohlc'}
    rows = [{k: dec(x[component][k]) for k in 'ohlc'} for x in children]
    # Even incomplete children cannot exceed their parent's observed price range.
    if min(x['l'] for x in rows) < p['l'] - tolerance or max(x['h'] for x in rows) > p['h'] + tolerance:
        return 'SOURCE_TIER_MISMATCH'
    if len(children) == count:
        combined = dict(o=rows[0]['o'], c=rows[-1]['c'], h=max(x['h'] for x in rows), l=min(x['l'] for x in rows))
        if any(abs(combined[k] - p[k]) > tolerance for k in 'ohlc'):
            return 'SOURCE_TIER_MISMATCH'
        return 'MATCH'
    return 'INCOMPLETE_CHILD_INTERVAL'

def near_rollover(t):
    local = dt(t).astimezone(NY)
    return abs(local.hour * 60 + local.minute - 17 * 60) <= 30

def refine(store, pair, side, component, stop, target, parent, gran):
    start = dt(parent['time'])
    end = start + timedelta(seconds=STEP[gran])
    coarse = touch(parent, component, side, stop, target)
    require(coarse is not None, 'Refinement requires a touched parent')
    if gran == 'S5':
        return dict(coarse, interval_start_utc=stamp(start), interval_end_utc=stamp(end),
                    resolution='S5', earlier_gap=False,
                    certainty='AMBIGUOUS_INTRABAR_ORDER' if coarse['kind'] == 'AMBIGUOUS_BOTH' else 'RESOLVED_TO_CANDLE_INTERVAL')
    if gran == 'M1' and coarse['kind'] != 'AMBIGUOUS_BOTH' and not near_rollover(start):
        return dict(coarse, interval_start_utc=stamp(start), interval_end_utc=stamp(end),
                    resolution='M1', earlier_gap=False, certainty='RESOLVED_TO_CANDLE_INTERVAL')
    child_gran = 'S5' if gran == 'M1' else 'M1'
    children = store.range(pair, child_gran, start, end)
    check = crosscheck(parent, children, component, STEP[gran] // STEP[child_gran], pair)
    for child in children:
        if touch(child, component, side, stop, target) is None:
            continue
        result = refine(store, pair, side, component, stop, target, child, child_gran)
        before = coverage(children, start, dt(result['interval_end_utc']), STEP[child_gran])
        earlier_gap = result['earlier_gap'] or before['unexplained_missing_slots'] > 0
        result['earlier_gap'] = earlier_gap
        result[gran.lower() + '_child_crosscheck'] = check
        if check == 'SOURCE_TIER_MISMATCH':
            result['certainty'] = 'SOURCE_TIER_MISMATCH'
        elif earlier_gap and result['certainty'] == 'RESOLVED_TO_CANDLE_INTERVAL':
            result['certainty'] = 'TOUCH_WITH_EARLIER_UNEXPLAINED_GAP'
        return result
    return dict(coarse, interval_start_utc=stamp(start), interval_end_utc=stamp(end),
                resolution=gran, earlier_gap=False,
                certainty='SOURCE_TIER_MISMATCH' if check == 'SOURCE_TIER_MISMATCH' else 'UNCONFIRMED_FINER_TOUCH')

def serial_geometry(g):
    return {k: float(v) if isinstance(v, Decimal) else v for k, v in g.items()}
