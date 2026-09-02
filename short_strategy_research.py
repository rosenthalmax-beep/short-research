import os
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

app = Flask(__name__)

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_URL = "https://api-fxtrade.oanda.com"

INSTRUMENT = "EUR_GBP"
TICK_SIZE = 0.00001
STOP_BUFFER_TICKS = 10
BACKTEST_SLIPPAGE_TICKS = 5
REWARD_RISK = 3.00
MIN_BODY_RATIO = 1.00
STRUCTURE_LOOKBACK = 90

NY_TZ = ZoneInfo("America/New_York")
EXCLUDED_NY_HOURS = {9}

H1_CHUNK_DAYS = 180
RESEARCH_FROM = datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc)
RESEARCH_TO = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
H1_WARMUP_DAYS = 700

ROBUST = {
    "name": "ROBUST",
    "max_distance_atr": 0.15,
    "min_range_atr": 1.10,
    "max_close_location": 0.20,
    "min_momentum_12": 0.25,
    "min_momentum_48": 0.40,
    "max_stop_size_atr": 2.50,
    "min_upper_wick_body": None,
    "min_atr_ratio_50": None,
}

HIGH_PF = {
    "name": "HIGH_PF",
    "max_distance_atr": 0.075,
    "min_range_atr": 1.00,
    "max_close_location": 0.225,
    "min_momentum_12": None,
    "min_momentum_48": 1.00,
    "max_stop_size_atr": None,
    "min_upper_wick_body": 0.10,
    "min_atr_ratio_50": 0.80,
}

SUMMARY_FILE = "eurgbp_short_confirmed_summary.csv"
CALENDAR_FILE = "eurgbp_short_confirmed_calendar_years.csv"
ROLLING_FILE = "eurgbp_short_confirmed_rolling_3y.csv"
SLICES_FILE = "eurgbp_short_confirmed_slices.csv"
RECENT_FILE = "eurgbp_short_confirmed_recent_windows.csv"
DRAWDOWN_FILE = "eurgbp_short_confirmed_drawdowns.csv"
TRADE_LOG_FILE = "eurgbp_short_confirmed_trade_log.csv"

FIXED_SLICES = [
    ("first_half_2002_2013", RESEARCH_FROM, datetime(2014, 1, 1, tzinfo=timezone.utc)),
    ("second_half_2014_present", datetime(2014, 1, 1, tzinfo=timezone.utc), None),
    ("2002_2009", RESEARCH_FROM, datetime(2010, 1, 1, tzinfo=timezone.utc)),
    ("2010_2017", datetime(2010, 1, 1, tzinfo=timezone.utc), datetime(2018, 1, 1, tzinfo=timezone.utc)),
    ("2018_2023", datetime(2018, 1, 1, tzinfo=timezone.utc), datetime(2024, 1, 1, tzinfo=timezone.utc)),
    ("2024_present", datetime(2024, 1, 1, tzinfo=timezone.utc), None),
]

STATUS = {
    "state": "not_started",
    "message": "Validation has not started",
    "service": "EURGBP Short Robust Trigger + High-PF Confirmation",
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "reward_risk": REWARD_RISK,
    "excluded_ny_hours": sorted(EXCLUDED_NY_HOURS),
    "output_files": [],
}

def headers():
    if not OANDA_TOKEN:
        raise RuntimeError("OANDA_TOKEN is not configured")
    return {"Authorization": f"Bearer {OANDA_TOKEN}"}

def iso_utc(dt):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def oanda_get(path, params):
    response = requests.get(OANDA_URL + path, headers=headers(), params=params, timeout=30)
    if not response.ok:
        raise RuntimeError(f"OANDA {response.status_code}: {response.text[:500]}")
    return response.json()

def parse_candle(raw):
    if not raw.get("complete", False):
        return None
    mid = raw.get("mid")
    if not mid:
        return None
    return {
        "time": datetime.fromisoformat(raw["time"].replace("Z", "+00:00")),
        "open": float(mid["o"]),
        "high": float(mid["h"]),
        "low": float(mid["l"]),
        "close": float(mid["c"]),
    }

def fetch_range(instrument, granularity, start, end):
    params = {
        "price": "M",
        "granularity": granularity,
        "from": iso_utc(start),
        "to": iso_utc(end),
        "smooth": "false",
        "includeFirst": "true",
    }
    data = oanda_get(f"/v3/instruments/{instrument}/candles", params)
    candles = []
    for raw in data.get("candles", []):
        c = parse_candle(raw)
        if c is not None:
            candles.append(c)
    return candles

def fetch_chunked_history(instrument, granularity, start, end):
    candles_by_time = {}
    cursor = start
    while cursor < end:
        chunk_end = min(cursor + timedelta(days=H1_CHUNK_DAYS), end)
        print(f"Fetching {granularity}: {cursor.date()} -> {chunk_end.date()}", flush=True)
        for candle in fetch_range(instrument, granularity, cursor, chunk_end):
            candles_by_time[candle["time"]] = candle
        cursor = chunk_end
    candles = list(candles_by_time.values())
    candles.sort(key=lambda x: x["time"])
    return candles

def true_ranges(candles):
    out = []
    for i, candle in enumerate(candles):
        if i == 0:
            tr = candle["high"] - candle["low"]
        else:
            prev_close = candles[i - 1]["close"]
            tr = max(
                candle["high"] - candle["low"],
                abs(candle["high"] - prev_close),
                abs(candle["low"] - prev_close),
            )
        out.append(tr)
    return out

def rma_series(values, length):
    result = [None] * len(values)
    if len(values) < length:
        return result
    initial = sum(values[:length]) / length
    result[length - 1] = initial
    prev = initial
    for i in range(length, len(values)):
        cur = (prev * (length - 1) + values[i]) / length
        result[i] = cur
        prev = cur
    return result

def atr_series(candles, length=14):
    return rma_series(true_ranges(candles), length)

def rolling_mean_optional(values, length):
    result = [None] * len(values)
    for i in range(length - 1, len(values)):
        window = values[i - length + 1:i + 1]
        if any(v is None for v in window):
            continue
        result[i] = sum(window) / length
    return result

def build_raw_candidates(h1, h1_atr, atr_mean_50):
    candidates = []
    max_lookback = max(STRUCTURE_LOOKBACK, 48, 50)
    for i in range(max_lookback, len(h1)):
        signal = h1[i]
        if signal["time"] < RESEARCH_FROM:
            continue
        if signal["time"] >= RESEARCH_TO:
            break

        prev = h1[i - 1]
        atr = h1_atr[i]
        if atr is None or atr <= 0:
            continue

        prev_body = abs(prev["close"] - prev["open"])
        cur_body = abs(signal["close"] - signal["open"])
        rng = signal["high"] - signal["low"]
        if prev_body <= 0 or cur_body <= 0 or rng <= 0:
            continue

        bearish_engulfing = (
            prev["close"] > prev["open"]
            and signal["close"] < signal["open"]
            and signal["open"] >= prev["close"]
            and signal["close"] <= prev["open"]
        )
        if not bearish_engulfing:
            continue

        body_ratio = cur_body / prev_body
        if body_ratio < MIN_BODY_RATIO:
            continue

        previous_highest = max(c["high"] for c in h1[i - STRUCTURE_LOOKBACK:i])
        structure_distance_atr = (previous_highest - signal["high"]) / atr
        range_atr = rng / atr
        close_location = (signal["close"] - signal["low"]) / rng
        momentum_12 = (signal["close"] - h1[i - 12]["close"]) / atr
        momentum_48 = (signal["close"] - h1[i - 48]["close"]) / atr

        upper_wick = max(0.0, signal["high"] - max(signal["open"], signal["close"]))
        upper_wick_body = upper_wick / cur_body

        stop = signal["high"] + STOP_BUFFER_TICKS * TICK_SIZE
        stop_size_atr = (stop - signal["close"]) / atr

        atr_ratio_50 = None
        if atr_mean_50[i] is not None and atr_mean_50[i] > 0:
            atr_ratio_50 = atr / atr_mean_50[i]

        ny_time = signal["time"].astimezone(NY_TZ)

        candidates.append({
            "index": i,
            "time": signal["time"],
            "ny_hour": ny_time.hour,
            "body_ratio": body_ratio,
            "structure_distance_atr": structure_distance_atr,
            "range_atr": range_atr,
            "close_location": close_location,
            "momentum_12": momentum_12,
            "momentum_48": momentum_48,
            "upper_wick_body": upper_wick_body,
            "stop_size_atr": stop_size_atr,
            "atr_ratio_50": atr_ratio_50,
        })
    return candidates

def passes_rules(signal, rules):
    if signal["ny_hour"] in EXCLUDED_NY_HOURS:
        return False
    if signal["structure_distance_atr"] > rules["max_distance_atr"]:
        return False
    if signal["range_atr"] < rules["min_range_atr"]:
        return False
    if signal["close_location"] > rules["max_close_location"]:
        return False

    v = rules["min_momentum_12"]
    if v is not None and signal["momentum_12"] < v:
        return False

    v = rules["min_momentum_48"]
    if v is not None and signal["momentum_48"] < v:
        return False

    v = rules["max_stop_size_atr"]
    if v is not None and signal["stop_size_atr"] > v:
        return False

    v = rules["min_upper_wick_body"]
    if v is not None and signal["upper_wick_body"] < v:
        return False

    v = rules["min_atr_ratio_50"]
    if v is not None:
        if signal["atr_ratio_50"] is None or signal["atr_ratio_50"] < v:
            return False

    return True

EXIT_CACHE = {}

def calculate_trade_exit(h1, signal_index):
    if signal_index in EXIT_CACHE:
        return EXIT_CACHE[signal_index]

    signal = h1[signal_index]
    reference_entry = signal["close"]
    backtest_entry = reference_entry - BACKTEST_SLIPPAGE_TICKS * TICK_SIZE
    stop = signal["high"] + STOP_BUFFER_TICKS * TICK_SIZE
    reference_risk = stop - reference_entry
    if reference_risk <= 0:
        raise RuntimeError("Invalid short reference risk")

    target = reference_entry - reference_risk * REWARD_RISK
    actual_risk = stop - backtest_entry
    if actual_risk <= 0:
        raise RuntimeError("Invalid short actual risk")

    for i in range(signal_index + 1, len(h1)):
        candle = h1[i]
        if candle["time"] >= RESEARCH_TO:
            break

        stop_hit = candle["high"] >= stop
        target_hit = candle["low"] <= target

        if not (stop_hit or target_hit):
            continue

        if stop_hit and target_hit:
            distance_to_high = abs(candle["high"] - candle["open"])
            distance_to_low = abs(candle["open"] - candle["low"])
            if distance_to_high < distance_to_low:
                exit_price = stop
                exit_reason = "STOP"
            else:
                exit_price = target
                exit_reason = "TARGET"
        elif stop_hit:
            exit_price = stop
            exit_reason = "STOP"
        else:
            exit_price = target
            exit_reason = "TARGET"

        result = {
            "status": "CLOSED",
            "signal_index": signal_index,
            "signal_time": signal["time"],
            "exit_index": i,
            "exit_time": candle["time"],
            "exit_reason": exit_reason,
            "reference_entry": round(reference_entry, 5),
            "backtest_entry": round(backtest_entry, 5),
            "stop": round(stop, 5),
            "target": round(target, 5),
            "result_r": (backtest_entry - exit_price) / actual_risk,
        }
        EXIT_CACHE[signal_index] = result
        return result

    result = {
        "status": "OPEN",
        "signal_index": signal_index,
        "signal_time": signal["time"],
        "exit_index": None,
        "exit_time": None,
        "exit_reason": None,
        "reference_entry": round(reference_entry, 5),
        "backtest_entry": round(backtest_entry, 5),
        "stop": round(stop, 5),
        "target": round(target, 5),
        "result_r": None,
    }
    EXIT_CACHE[signal_index] = result
    return result

def simulate(h1, eligible):
    trades = []
    position_exit_index = -1
    ignored = 0
    still_open = False

    for signal in eligible:
        idx = signal["index"]
        if idx < position_exit_index:
            ignored += 1
            continue

        trade = calculate_trade_exit(h1, idx)
        if trade["status"] == "OPEN":
            still_open = True
            break

        enriched = dict(trade)
        enriched.update(signal)
        trades.append(enriched)
        position_exit_index = trade["exit_index"]

    return trades, ignored, still_open

def stats_for_trades(trades, start=None, end=None):
    filtered = []
    for trade in trades:
        t = trade["signal_time"]
        if start is not None and t < start:
            continue
        if end is not None and t >= end:
            continue
        filtered.append(trade)

    if not filtered:
        return {
            "trades": 0, "winners": 0, "losers": 0, "win_rate": 0.0,
            "profit_factor": 0.0, "total_r": 0.0, "expectancy_r": 0.0,
            "max_drawdown_r": 0.0, "longest_loss_streak": 0
        }

    results = [t["result_r"] for t in filtered]
    winners = [r for r in results if r > 0]
    losers = [r for r in results if r < 0]

    gp = sum(winners)
    gl = abs(sum(losers))
    total = sum(results)
    pf = gp / gl if gl > 0 else (999.0 if gp > 0 else 0.0)

    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    cur_ls = 0
    longest_ls = 0

    for r in results:
        equity += r
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
        if r < 0:
            cur_ls += 1
            longest_ls = max(longest_ls, cur_ls)
        else:
            cur_ls = 0

    return {
        "trades": len(results),
        "winners": len(winners),
        "losers": len(losers),
        "win_rate": round(len(winners) / len(results) * 100.0, 2),
        "profit_factor": round(pf, 3),
        "total_r": round(total, 2),
        "expectancy_r": round(total / len(results), 3),
        "max_drawdown_r": round(max_dd, 2),
        "longest_loss_streak": longest_ls,
    }

def stats_row(strategy, label, start, end, trades):
    row = {
        "strategy": strategy,
        "label": label,
        "start": start.isoformat() if start else None,
        "end": end.isoformat() if end else None,
    }
    row.update(stats_for_trades(trades, start, end))
    return row

def drawdown_diagnostics(strategy, trades):
    if not trades:
        return {
            "strategy": strategy,
            "trades": 0,
            "max_drawdown_r": 0.0,
            "max_drawdown_start": None,
            "max_drawdown_end": None,
            "max_drawdown_recovery": None,
            "max_drawdown_duration_days": 0.0,
            "longest_time_between_equity_highs_days": 0.0,
            "longest_time_between_equity_highs_start": None,
            "longest_time_between_equity_highs_end": None,
        }

    equity = 0.0
    peak_equity = 0.0
    peak_time = trades[0]["signal_time"]

    worst_dd = 0.0
    worst_dd_start = peak_time
    worst_dd_end = peak_time
    worst_dd_recovery = None

    active_dd_low = 0.0
    active_dd = False

    last_high_time = trades[0]["signal_time"]
    longest_flat_days = 0.0
    longest_flat_start = None
    longest_flat_end = None

    for trade in trades:
        equity += trade["result_r"]
        t = trade["signal_time"]

        if equity >= peak_equity:
            if active_dd and worst_dd_recovery is None and abs(active_dd_low - worst_dd) < 1e-9:
                worst_dd_recovery = t
            active_dd = False
            active_dd_low = 0.0

            gap = (t - last_high_time).total_seconds() / 86400.0
            if gap > longest_flat_days:
                longest_flat_days = gap
                longest_flat_start = last_high_time
                longest_flat_end = t

            peak_equity = equity
            peak_time = t
            last_high_time = t
        else:
            dd = equity - peak_equity
            if not active_dd:
                active_dd = True
                active_dd_low = dd
            else:
                active_dd_low = min(active_dd_low, dd)

            if dd < worst_dd:
                worst_dd = dd
                worst_dd_start = peak_time
                worst_dd_end = t
                worst_dd_recovery = None

    if active_dd:
        end_time = trades[-1]["signal_time"]
        gap = (end_time - last_high_time).total_seconds() / 86400.0
        if gap > longest_flat_days:
            longest_flat_days = gap
            longest_flat_start = last_high_time
            longest_flat_end = end_time

    dd_end = worst_dd_recovery if worst_dd_recovery else trades[-1]["signal_time"]
    dd_duration_days = (dd_end - worst_dd_start).total_seconds() / 86400.0

    return {
        "strategy": strategy,
        "trades": len(trades),
        "max_drawdown_r": round(worst_dd, 2),
        "max_drawdown_start": worst_dd_start.isoformat() if worst_dd_start else None,
        "max_drawdown_end": worst_dd_end.isoformat() if worst_dd_end else None,
        "max_drawdown_recovery": worst_dd_recovery.isoformat() if worst_dd_recovery else None,
        "max_drawdown_duration_days": round(dd_duration_days, 1),
        "longest_time_between_equity_highs_days": round(longest_flat_days, 1),
        "longest_time_between_equity_highs_start": longest_flat_start.isoformat() if longest_flat_start else None,
        "longest_time_between_equity_highs_end": longest_flat_end.isoformat() if longest_flat_end else None,
    }

def build_summary(trades_by_strategy):
    years = (RESEARCH_TO - RESEARCH_FROM).total_seconds() / (365.2425 * 86400)
    rows = []
    for name, trades in trades_by_strategy.items():
        s = stats_for_trades(trades)
        rows.append({
            "strategy": name,
            "research_from": RESEARCH_FROM.isoformat(),
            "research_to": RESEARCH_TO.isoformat(),
            "reward_risk": REWARD_RISK,
            "excluded_ny_hours": "09",
            "trades": s["trades"],
            "trades_per_year": round(s["trades"] / years, 2),
            "winners": s["winners"],
            "losers": s["losers"],
            "win_rate": s["win_rate"],
            "profit_factor": s["profit_factor"],
            "total_r": s["total_r"],
            "expectancy_r": s["expectancy_r"],
            "max_drawdown_r": s["max_drawdown_r"],
            "longest_loss_streak": s["longest_loss_streak"],
            "annual_r_linear": round(s["expectancy_r"] * (s["trades"] / years), 3),
        })
    return pd.DataFrame(rows)

def build_calendar_years(trades_by_strategy):
    rows = []
    for name, trades in trades_by_strategy.items():
        for year in range(RESEARCH_FROM.year, RESEARCH_TO.year + 1):
            start = datetime(year, 1, 1, tzinfo=timezone.utc)
            end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
            actual_start = max(start, RESEARCH_FROM)
            actual_end = min(end, RESEARCH_TO)
            if actual_start < actual_end:
                rows.append(stats_row(name, str(year), actual_start, actual_end, trades))
    return pd.DataFrame(rows)

def build_rolling_3y(trades_by_strategy):
    rows = []
    last_start_year = RESEARCH_TO.year - 2
    for name, trades in trades_by_strategy.items():
        for start_year in range(2002, last_start_year + 1):
            start = datetime(start_year, 1, 1, tzinfo=timezone.utc)
            end = datetime(start_year + 3, 1, 1, tzinfo=timezone.utc)
            actual_start = max(start, RESEARCH_FROM)
            actual_end = min(end, RESEARCH_TO)
            if actual_start >= actual_end:
                continue
            row = stats_row(name, f"{start_year}_{start_year+2}", actual_start, actual_end, trades)
            span_years = (actual_end - actual_start).total_seconds() / (365.2425 * 86400)
            row["trades_per_year"] = round(row["trades"] / span_years, 2)
            rows.append(row)
    return pd.DataFrame(rows)

def build_slices(trades_by_strategy):
    rows = []
    for name, trades in trades_by_strategy.items():
        for label, start, end in FIXED_SLICES:
            actual_end = RESEARCH_TO if end is None else min(end, RESEARCH_TO)
            rows.append(stats_row(name, label, start, actual_end, trades))
    return pd.DataFrame(rows)

def subtract_years_safe(dt, years):
    try:
        return dt.replace(year=dt.year - years)
    except ValueError:
        return dt.replace(month=2, day=28, year=dt.year - years)

def build_recent_windows(trades_by_strategy):
    rows = []
    for name, trades in trades_by_strategy.items():
        for years_back in [2, 5, 10]:
            start = subtract_years_safe(RESEARCH_TO, years_back)
            row = stats_row(name, f"last_{years_back}_years", start, RESEARCH_TO, trades)
            row["trades_per_year"] = round(row["trades"] / years_back, 2)
            rows.append(row)
    return pd.DataFrame(rows)

def build_drawdowns(trades_by_strategy):
    return pd.DataFrame([
        drawdown_diagnostics(name, trades)
        for name, trades in trades_by_strategy.items()
    ])

def build_confirmed_trade_log(trades):
    rows = []
    for trade in trades:
        rows.append({
            "signal_time": trade["signal_time"].isoformat(),
            "exit_time": trade["exit_time"].isoformat() if trade["exit_time"] else None,
            "ny_hour": trade["ny_hour"],
            "exit_reason": trade["exit_reason"],
            "result_r": round(trade["result_r"], 4),
            "reference_entry": trade["reference_entry"],
            "backtest_entry": trade["backtest_entry"],
            "stop": trade["stop"],
            "target": trade["target"],
            "body_ratio": round(trade["body_ratio"], 4),
            "structure_distance_atr": round(trade["structure_distance_atr"], 4),
            "range_atr": round(trade["range_atr"], 4),
            "close_location": round(trade["close_location"], 4),
            "momentum_12_atr": round(trade["momentum_12"], 4),
            "momentum_48_atr": round(trade["momentum_48"], 4),
            "upper_wick_body": round(trade["upper_wick_body"], 4),
            "stop_size_atr": round(trade["stop_size_atr"], 4),
            "atr_ratio_50": round(trade["atr_ratio_50"], 4) if trade["atr_ratio_50"] is not None else None,
        })
    return pd.DataFrame(rows)

def run_validation():
    global STATUS
    try:
        STATUS.update({"state": "fetching_data", "message": "Fetching EUR/GBP OANDA H1 history"})

        h1 = fetch_chunked_history(
            INSTRUMENT,
            "H1",
            RESEARCH_FROM - timedelta(days=H1_WARMUP_DAYS),
            RESEARCH_TO,
        )
        if not h1:
            raise RuntimeError("No EUR/GBP H1 candles returned")

        STATUS.update({"state": "precomputing", "message": "Building ATR14 and signal features"})
        h1_atr = atr_series(h1, 14)
        atr_mean_50 = rolling_mean_optional(h1_atr, 50)
        raw_candidates = build_raw_candidates(h1, h1_atr, atr_mean_50)

        robust_signals = []
        high_pf_signals = []
        confirmed_signals = []

        for signal in raw_candidates:
            robust_pass = passes_rules(signal, ROBUST)
            high_pf_pass = passes_rules(signal, HIGH_PF)

            if robust_pass:
                robust_signals.append(signal)
            if high_pf_pass:
                high_pf_signals.append(signal)
            if robust_pass and high_pf_pass:
                confirmed_signals.append(signal)

        STATUS.update({
            "state": "simulating",
            "message": "Simulating ROBUST, HIGH_PF and CONFIRMED",
            "raw_bearish_engulfing_signals": len(raw_candidates),
            "robust_eligible_signals": len(robust_signals),
            "high_pf_eligible_signals": len(high_pf_signals),
            "confirmed_eligible_signals": len(confirmed_signals),
        })

        signal_sets = {
            "ROBUST": robust_signals,
            "HIGH_PF": high_pf_signals,
            "CONFIRMED": confirmed_signals,
        }

        trades_by_strategy = {}

        for name, eligible in signal_sets.items():
            trades, ignored, still_open = simulate(h1, eligible)
            trades_by_strategy[name] = trades
            STATUS[f"{name.lower()}_trades"] = len(trades)
            STATUS[f"{name.lower()}_ignored_due_to_open_trade"] = ignored
            STATUS[f"{name.lower()}_still_open_at_end"] = still_open
            print(f"{name}: {len(trades)} trades", flush=True)

        STATUS.update({"state": "building_outputs", "message": "Building validation outputs"})

        build_summary(trades_by_strategy).to_csv(SUMMARY_FILE, index=False)
        build_calendar_years(trades_by_strategy).to_csv(CALENDAR_FILE, index=False)
        build_rolling_3y(trades_by_strategy).to_csv(ROLLING_FILE, index=False)
        build_slices(trades_by_strategy).to_csv(SLICES_FILE, index=False)
        build_recent_windows(trades_by_strategy).to_csv(RECENT_FILE, index=False)
        build_drawdowns(trades_by_strategy).to_csv(DRAWDOWN_FILE, index=False)
        build_confirmed_trade_log(trades_by_strategy["CONFIRMED"]).to_csv(TRADE_LOG_FILE, index=False)

        output_files = [
            SUMMARY_FILE,
            CALENDAR_FILE,
            ROLLING_FILE,
            SLICES_FILE,
            RECENT_FILE,
            DRAWDOWN_FILE,
            TRADE_LOG_FILE,
        ]

        STATUS.update({
            "state": "complete",
            "message": "EUR/GBP confirmation validation completed successfully",
            "output_files": output_files,
        })

    except Exception as error:
        STATUS.update({"state": "error", "message": str(error)})
        print("ERROR:", error, flush=True)

@app.route("/")
def home():
    return jsonify({
        "service": "EURGBP Short Robust Trigger + High-PF Confirmation",
        "status": STATUS,
        "instrument": INSTRUMENT,
        "direction": "SHORT",
        "reward_risk": REWARD_RISK,
        "timezone": "America/New_York",
        "timing_basis": "signal candle open time",
        "excluded_ny_hours": sorted(EXCLUDED_NY_HOURS),
        "trading_enabled": False,
        "orders_supported": False,
        "executor_connected": False,
        "robust_trigger": ROBUST,
        "high_pf_confirmation": HIGH_PF,
        "confirmation_rule": "Trade only when ROBUST and HIGH_PF both qualify on the same signal candle",
        "downloads": {
            "summary": "/download/summary",
            "calendar_years": "/download/calendar",
            "rolling_3y": "/download/rolling",
            "slices": "/download/slices",
            "recent_windows": "/download/recent",
            "drawdowns": "/download/drawdowns",
            "trade_log": "/download/trades",
        },
    })

@app.route("/status")
def status():
    return jsonify(STATUS)

def file_download(filename):
    if not os.path.exists(filename):
        return jsonify({"status": "not_ready", "message": f"{filename} is not ready yet"}), 404
    return send_file(filename, as_attachment=True, download_name=filename)

@app.route("/download/summary")
def download_summary():
    return file_download(SUMMARY_FILE)

@app.route("/download/calendar")
def download_calendar():
    return file_download(CALENDAR_FILE)

@app.route("/download/rolling")
def download_rolling():
    return file_download(ROLLING_FILE)

@app.route("/download/slices")
def download_slices():
    return file_download(SLICES_FILE)

@app.route("/download/recent")
def download_recent():
    return file_download(RECENT_FILE)

@app.route("/download/drawdowns")
def download_drawdowns():
    return file_download(DRAWDOWN_FILE)

@app.route("/download/trades")
def download_trades():
    return file_download(TRADE_LOG_FILE)

if __name__ == "__main__":
    validation_thread = threading.Thread(
        target=run_validation,
        name="eurgbp-short-confirmation-validation",
        daemon=True,
    )
    validation_thread.start()

    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
