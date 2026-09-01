import os
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta


# ============================================================
# EUR/GBP SHORT - RAW BASELINE RR SWEEP
# RESEARCH ONLY — NEVER SUBMITS ORDERS.
# ============================================================

app = Flask(__name__)

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_URL = "https://api-fxtrade.oanda.com"

INSTRUMENT = "EUR_GBP"
TICK_SIZE = 0.00001
STOP_BUFFER_TICKS = 10
BACKTEST_SLIPPAGE_TICKS = 5
BODY_RATIO = 1.00
RR_VALUES = [2.00, 2.25, 2.50, 2.75, 3.00, 3.25, 3.50, 3.75, 4.00]
H1_CHUNK_DAYS = 180
RESEARCH_FROM = datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc)
RESEARCH_TO = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
H1_WARMUP_DAYS = 7
OUTPUT_FILE = "eurgbp_short_raw_baseline_rr_sweep.csv"

ERAS = [
    ("2002_2009", datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc), datetime(2010, 1, 1, 0, 0, tzinfo=timezone.utc)),
    ("2010_2017", datetime(2010, 1, 1, 0, 0, tzinfo=timezone.utc), datetime(2018, 1, 1, 0, 0, tzinfo=timezone.utc)),
    ("2018_2023", datetime(2018, 1, 1, 0, 0, tzinfo=timezone.utc), datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)),
    ("2024_present", datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc), None),
]

STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "service": "EURGBP Short Raw Baseline RR Sweep",
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "total_tests": len(RR_VALUES),
    "completed_tests": 0,
    "rows_saved": 0,
    "output_file": None,
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
        candle = parse_candle(raw)
        if candle is not None:
            candles.append(candle)
    return candles


def fetch_chunked_history(instrument, granularity, start, end):
    candles_by_time = {}
    cursor = start
    while cursor < end:
        chunk_end = min(cursor + timedelta(days=H1_CHUNK_DAYS), end)
        print(f"Fetching {granularity}: {cursor.date()} -> {chunk_end.date()}", flush=True)
        chunk = fetch_range(instrument, granularity, cursor, chunk_end)
        for candle in chunk:
            candles_by_time[candle["time"]] = candle
        cursor = chunk_end
    candles = list(candles_by_time.values())
    candles.sort(key=lambda item: item["time"])
    return candles


def build_candidates(h1):
    candidates = []
    for index in range(1, len(h1)):
        signal = h1[index]
        if signal["time"] < RESEARCH_FROM:
            continue
        if signal["time"] >= RESEARCH_TO:
            break
        previous = h1[index - 1]
        previous_body = abs(previous["close"] - previous["open"])
        current_body = abs(signal["close"] - signal["open"])
        if previous_body <= 0 or current_body <= 0:
            continue
        bearish_engulfing = (
            previous["close"] > previous["open"]
            and signal["close"] < signal["open"]
            and signal["open"] >= previous["close"]
            and signal["close"] <= previous["open"]
        )
        if not bearish_engulfing:
            continue
        body_ratio = current_body / previous_body
        if body_ratio < BODY_RATIO:
            continue
        candidates.append({"index": index, "time": signal["time"], "body_ratio": body_ratio})
    return candidates


def calculate_trade_exit(h1, signal_index, reward_risk):
    signal = h1[signal_index]
    reference_entry = signal["close"]
    backtest_entry = reference_entry - BACKTEST_SLIPPAGE_TICKS * TICK_SIZE
    stop = signal["high"] + STOP_BUFFER_TICKS * TICK_SIZE
    reference_risk = stop - reference_entry
    if reference_risk <= 0:
        raise RuntimeError("Invalid short reference risk")
    target = reference_entry - reference_risk * reward_risk
    actual_risk = stop - backtest_entry
    if actual_risk <= 0:
        raise RuntimeError("Invalid short actual risk")
    for index in range(signal_index + 1, len(h1)):
        candle = h1[index]
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
        return {
            "status": "CLOSED",
            "signal_index": signal_index,
            "signal_time": signal["time"],
            "exit_index": index,
            "exit_time": candle["time"],
            "exit_reason": exit_reason,
            "result_r": (backtest_entry - exit_price) / actual_risk,
        }
    return {
        "status": "OPEN",
        "signal_index": signal_index,
        "signal_time": signal["time"],
        "exit_index": None,
        "exit_time": None,
        "exit_reason": None,
        "result_r": None,
    }


def simulate(h1, candidates, reward_risk):
    trades = []
    position_exit_index = -1
    ignored = 0
    still_open = False
    for candidate in candidates:
        signal_index = candidate["index"]
        if signal_index < position_exit_index:
            ignored += 1
            continue
        trade = calculate_trade_exit(h1, signal_index, reward_risk)
        if trade["status"] == "OPEN":
            still_open = True
            break
        trades.append(trade)
        position_exit_index = trade["exit_index"]
    return trades, ignored, still_open


def stats_for_trades(trades, start=None, end=None):
    filtered = []
    for trade in trades:
        signal_time = trade["signal_time"]
        if start is not None and signal_time < start:
            continue
        if end is not None and signal_time >= end:
            continue
        filtered.append(trade)
    if not filtered:
        return {
            "trades": 0,
            "winners": 0,
            "losers": 0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "total_r": 0.0,
            "expectancy_r": 0.0,
            "max_drawdown_r": 0.0,
            "longest_loss_streak": 0,
        }
    results = [trade["result_r"] for trade in filtered]
    winners = [result for result in results if result > 0]
    losers = [result for result in results if result < 0]
    gross_profit = sum(winners)
    gross_loss = abs(sum(losers))
    total_r = sum(results)
    if gross_loss > 0:
        profit_factor = gross_profit / gross_loss
    elif gross_profit > 0:
        profit_factor = 999.0
    else:
        profit_factor = 0.0
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    current_streak = 0
    longest_streak = 0
    for result in results:
        equity += result
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity - peak)
        if result < 0:
            current_streak += 1
            longest_streak = max(longest_streak, current_streak)
        else:
            current_streak = 0
    return {
        "trades": len(results),
        "winners": len(winners),
        "losers": len(losers),
        "win_rate": round(len(winners) / len(results) * 100.0, 2),
        "profit_factor": round(profit_factor, 3),
        "total_r": round(total_r, 2),
        "expectancy_r": round(total_r / len(results), 3),
        "max_drawdown_r": round(max_drawdown, 2),
        "longest_loss_streak": longest_streak,
    }


def make_result_row(reward_risk, candidates, trades, ignored, still_open, years):
    full = stats_for_trades(trades)
    row = {
        "reward_risk": reward_risk,
        "raw_signals": len(candidates),
        "ignored_due_to_open_trade": ignored,
        "still_open_at_end": still_open,
        "trades": full["trades"],
        "trades_per_year": round(full["trades"] / years, 2),
        "winners": full["winners"],
        "losers": full["losers"],
        "win_rate": full["win_rate"],
        "profit_factor": full["profit_factor"],
        "total_r": full["total_r"],
        "expectancy_r": full["expectancy_r"],
        "max_drawdown_r": full["max_drawdown_r"],
        "longest_loss_streak": full["longest_loss_streak"],
    }
    profitable_eras = 0
    minimum_era_pf = None
    minimum_era_expectancy = None
    for era_name, era_start, era_end in ERAS:
        era = stats_for_trades(trades, era_start, era_end)
        row[f"{era_name}_trades"] = era["trades"]
        row[f"{era_name}_pf"] = era["profit_factor"]
        row[f"{era_name}_r"] = era["total_r"]
        row[f"{era_name}_expectancy"] = era["expectancy_r"]
        if era["trades"] > 0:
            if era["total_r"] > 0:
                profitable_eras += 1
            pf = era["profit_factor"]
            expectancy = era["expectancy_r"]
            minimum_era_pf = pf if minimum_era_pf is None else min(minimum_era_pf, pf)
            minimum_era_expectancy = expectancy if minimum_era_expectancy is None else min(minimum_era_expectancy, expectancy)
    row["profitable_eras"] = profitable_eras
    row["minimum_era_pf"] = minimum_era_pf
    row["minimum_era_expectancy"] = minimum_era_expectancy
    row["annual_r_linear"] = round(full["expectancy_r"] * (full["trades"] / years), 3)
    return row


def run_research():
    global STATUS
    try:
        print("\n" + "=" * 76)
        print("EUR/GBP SHORT - RAW BASELINE RR SWEEP")
        print("=" * 76)
        print("No regime / structure / timing filters")
        print(f"RR tests: {RR_VALUES}\n")
        STATUS.update({"state": "fetching_data", "message": "Fetching EUR/GBP OANDA H1 history"})
        h1 = fetch_chunked_history(
            INSTRUMENT,
            "H1",
            RESEARCH_FROM - timedelta(days=H1_WARMUP_DAYS),
            RESEARCH_TO,
        )
        if not h1:
            raise RuntimeError("No EUR/GBP H1 candles returned")
        STATUS.update({"state": "precomputing", "message": "Building raw EUR/GBP bearish engulfing signal set"})
        candidates = build_candidates(h1)
        STATUS["raw_bearish_engulfing_signals"] = len(candidates)
        years = (RESEARCH_TO - RESEARCH_FROM).total_seconds() / (365.2425 * 24 * 60 * 60)
        STATUS.update({"state": "running", "message": "Running EUR/GBP raw RR sweep"})
        rows = []
        for number, reward_risk in enumerate(RR_VALUES, start=1):
            print(f"Running RR {reward_risk:.2f} ({number}/{len(RR_VALUES)})", flush=True)
            trades, ignored, still_open = simulate(h1, candidates, reward_risk)
            rows.append(make_result_row(reward_risk, candidates, trades, ignored, still_open, years))
            STATUS["completed_tests"] = number
        df = pd.DataFrame(rows)
        if df.empty:
            raise RuntimeError("No RR rows generated")
        df = df.sort_values(
            by=["profit_factor", "expectancy_r", "annual_r_linear", "total_r"],
            ascending=[False, False, False, False],
        )
        df.to_csv(OUTPUT_FILE, index=False)
        STATUS.update({
            "state": "complete",
            "message": "EUR/GBP raw baseline RR sweep completed successfully",
            "completed_tests": len(RR_VALUES),
            "rows_saved": len(df),
            "raw_bearish_engulfing_signals": len(candidates),
            "output_file": OUTPUT_FILE,
        })
        print("\n" + "=" * 76)
        print("EUR/GBP RAW BASELINE COMPLETE")
        print("=" * 76)
        print("Raw bearish engulfing signals:", len(candidates))
        print("Rows:", len(df))
        print("Saved:", OUTPUT_FILE)
        print()
        print(df[[
            "reward_risk", "trades", "trades_per_year", "win_rate",
            "profit_factor", "total_r", "expectancy_r", "max_drawdown_r",
            "profitable_eras", "minimum_era_pf"
        ]].to_string(index=False), flush=True)
    except Exception as error:
        STATUS.update({"state": "error", "message": str(error)})
        print("ERROR:", error, flush=True)


@app.route("/")
def home():
    return jsonify({
        "service": "EURGBP Short Raw Baseline RR Sweep",
        "status": STATUS,
        "instrument": INSTRUMENT,
        "direction": "SHORT",
        "trading_enabled": False,
        "orders_supported": False,
        "executor_connected": False,
        "baseline": {
            "bearish_engulfing": True,
            "minimum_body_ratio": BODY_RATIO,
            "stop_buffer_ticks": STOP_BUFFER_TICKS,
            "backtest_slippage_ticks": BACKTEST_SLIPPAGE_TICKS,
            "regime_filter": None,
            "structure_filter": None,
            "momentum_filter": None,
            "range_filter": None,
            "stop_size_filter": None,
            "hour_filter": None,
            "weekday_filter": None,
        },
        "rr_values": RR_VALUES,
        "eras": [
            {"name": name, "start": start.isoformat(), "end": end.isoformat() if end is not None else None}
            for name, start, end in ERAS
        ],
        "download": "/download",
    })


@app.route("/status")
def status():
    return jsonify(STATUS)


@app.route("/download")
def download():
    if not os.path.exists(OUTPUT_FILE):
        return jsonify({"status": "not_ready", "message": "EUR/GBP baseline CSV is not ready yet"}), 404
    return send_file(OUTPUT_FILE, as_attachment=True, download_name=OUTPUT_FILE)


if __name__ == "__main__":
    research_thread = threading.Thread(
        target=run_research,
        name="eurgbp-short-raw-baseline",
        daemon=True,
    )
    research_thread.start()
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
