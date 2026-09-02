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
MAX_DISTANCE_ATR = 0.075
MIN_RANGE_ATR = 1.10
MAX_CLOSE_LOCATION = 0.20

NY_TZ = ZoneInfo("America/New_York")
H1_CHUNK_DAYS = 180

RESEARCH_FROM = datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc)
RESEARCH_TO = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
H1_WARMUP_DAYS = 700

OUTPUT_FILE = "eurgbp_short_dual_branch_light_timing.csv"

BRANCHES = [
    {
        "branch": "ROBUST",
        "min_momentum_12": 0.25,
        "min_momentum_48": 0.50,
        "min_upper_wick_body": None,
        "max_stop_size_atr": 2.50,
        "min_atr_ratio_50": None,
    },
    {
        "branch": "HIGH_PF",
        "min_momentum_12": None,
        "min_momentum_48": 1.00,
        "min_upper_wick_body": 0.10,
        "max_stop_size_atr": None,
        "min_atr_ratio_50": 0.80,
    },
]

TIMING_TESTS = [{
    "timing_type": "BASELINE",
    "timing_label": "baseline",
    "excluded_hours": set(),
    "excluded_weekdays": set(),
}]

for hour in range(24):
    TIMING_TESTS.append({
        "timing_type": "EXCLUDE_NY_HOUR",
        "timing_label": f"exclude_ny_hour_{hour:02d}",
        "excluded_hours": {hour},
        "excluded_weekdays": set(),
    })

weekday_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
for weekday, weekday_name in enumerate(weekday_names):
    TIMING_TESTS.append({
        "timing_type": "EXCLUDE_WEEKDAY",
        "timing_label": f"exclude_{weekday_name.lower()}",
        "excluded_hours": set(),
        "excluded_weekdays": {weekday},
    })

for hour in range(24):
    next_hour = (hour + 1) % 24
    TIMING_TESTS.append({
        "timing_type": "EXCLUDE_2H_NY",
        "timing_label": f"exclude_ny_{hour:02d}_{next_hour:02d}",
        "excluded_hours": {hour, next_hour},
        "excluded_weekdays": set(),
    })

ERAS = [
    ("2002_2009", datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc), datetime(2010, 1, 1, tzinfo=timezone.utc)),
    ("2010_2017", datetime(2010, 1, 1, tzinfo=timezone.utc), datetime(2018, 1, 1, tzinfo=timezone.utc)),
    ("2018_2023", datetime(2018, 1, 1, tzinfo=timezone.utc), datetime(2024, 1, 1, tzinfo=timezone.utc)),
    ("2024_present", datetime(2024, 1, 1, tzinfo=timezone.utc), None),
]

TOTAL_TESTS = len(BRANCHES) * len(TIMING_TESTS)

STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "service": "EURGBP Short Dual-Branch Light Timing",
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "reward_risk": REWARD_RISK,
    "timing_tests_per_branch": len(TIMING_TESTS),
    "total_tests": TOTAL_TESTS,
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
    r = requests.get(OANDA_URL + path, headers=headers(), params=params, timeout=30)
    if not r.ok:
        raise RuntimeError(f"OANDA {r.status_code}: {r.text[:500]}")
    return r.json()


def parse_candle(raw):
    if not raw.get("complete", False) or not raw.get("mid"):
        return None
    mid = raw["mid"]
    return {
        "time": datetime.fromisoformat(raw["time"].replace("Z", "+00:00")),
        "open": float(mid["o"]),
        "high": float(mid["h"]),
        "low": float(mid["l"]),
        "close": float(mid["c"]),
    }


def fetch_range(instrument, granularity, start, end):
    data = oanda_get(
        f"/v3/instruments/{instrument}/candles",
        {
            "price": "M",
            "granularity": granularity,
            "from": iso_utc(start),
            "to": iso_utc(end),
            "smooth": "false",
            "includeFirst": "true",
        },
    )
    out = []
    for raw in data.get("candles", []):
        c = parse_candle(raw)
        if c is not None:
            out.append(c)
    return out


def fetch_chunked_history(instrument, granularity, start, end):
    by_time = {}
    cursor = start
    while cursor < end:
        chunk_end = min(cursor + timedelta(days=H1_CHUNK_DAYS), end)
        print(f"Fetching {granularity}: {cursor.date()} -> {chunk_end.date()}", flush=True)
        for candle in fetch_range(instrument, granularity, cursor, chunk_end):
            by_time[candle["time"]] = candle
        cursor = chunk_end
    candles = list(by_time.values())
    candles.sort(key=lambda x: x["time"])
    return candles


def true_ranges(candles):
    out = []
    for i, c in enumerate(candles):
        if i == 0:
            tr = c["high"] - c["low"]
        else:
            pc = candles[i - 1]["close"]
            tr = max(c["high"] - c["low"], abs(c["high"] - pc), abs(c["low"] - pc))
        out.append(tr)
    return out


def rma_series(values, length):
    out = [None] * len(values)
    if len(values) < length:
        return out
    initial = sum(values[:length]) / length
    out[length - 1] = initial
    prev = initial
    for i in range(length, len(values)):
        cur = (prev * (length - 1) + values[i]) / length
        out[i] = cur
        prev = cur
    return out


def atr_series(candles, length=14):
    return rma_series(true_ranges(candles), length)


def rolling_mean_optional(values, length):
    out = [None] * len(values)
    for i in range(length - 1, len(values)):
        window = values[i - length + 1:i + 1]
        if any(v is None for v in window):
            continue
        out[i] = sum(window) / length
    return out


def build_candidates(h1, h1_atr, atr_mean_50):
    out = []
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
        body = abs(signal["close"] - signal["open"])
        candle_range = signal["high"] - signal["low"]

        if prev_body <= 0 or body <= 0 or candle_range <= 0:
            continue

        bearish_engulfing = (
            prev["close"] > prev["open"]
            and signal["close"] < signal["open"]
            and signal["open"] >= prev["close"]
            and signal["close"] <= prev["open"]
        )
        if not bearish_engulfing:
            continue

        if body / prev_body < MIN_BODY_RATIO:
            continue

        range_atr = candle_range / atr
        if range_atr < MIN_RANGE_ATR:
            continue

        close_location = (signal["close"] - signal["low"]) / candle_range
        if close_location > MAX_CLOSE_LOCATION:
            continue

        prev_highest = max(c["high"] for c in h1[i - STRUCTURE_LOOKBACK:i])
        structure_distance = (prev_highest - signal["high"]) / atr
        if structure_distance > MAX_DISTANCE_ATR:
            continue

        momentum_12 = (signal["close"] - h1[i - 12]["close"]) / atr
        momentum_48 = (signal["close"] - h1[i - 48]["close"]) / atr

        upper_wick = max(0.0, signal["high"] - max(signal["open"], signal["close"]))
        upper_wick_body = upper_wick / body

        stop = signal["high"] + STOP_BUFFER_TICKS * TICK_SIZE
        stop_size_atr = (stop - signal["close"]) / atr

        atr_ratio_50 = None
        if atr_mean_50[i] is not None and atr_mean_50[i] > 0:
            atr_ratio_50 = atr / atr_mean_50[i]

        ny_time = signal["time"].astimezone(NY_TZ)

        out.append({
            "index": i,
            "time": signal["time"],
            "ny_hour": ny_time.hour,
            "ny_weekday": ny_time.weekday(),
            "momentum_12": momentum_12,
            "momentum_48": momentum_48,
            "upper_wick_body": upper_wick_body,
            "stop_size_atr": stop_size_atr,
            "atr_ratio_50": atr_ratio_50,
        })

    return out


def passes_branch(c, branch):
    v = branch["min_momentum_12"]
    if v is not None and c["momentum_12"] < v:
        return False

    v = branch["min_momentum_48"]
    if v is not None and c["momentum_48"] < v:
        return False

    v = branch["min_upper_wick_body"]
    if v is not None and c["upper_wick_body"] < v:
        return False

    v = branch["max_stop_size_atr"]
    if v is not None and c["stop_size_atr"] > v:
        return False

    v = branch["min_atr_ratio_50"]
    if v is not None and (c["atr_ratio_50"] is None or c["atr_ratio_50"] < v):
        return False

    return True


def passes_timing(c, test):
    if c["ny_hour"] in test["excluded_hours"]:
        return False
    if c["ny_weekday"] in test["excluded_weekdays"]:
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
        "result_r": None,
    }
    EXIT_CACHE[signal_index] = result
    return result


def simulate(h1, eligible):
    trades = []
    position_exit_index = -1
    ignored = 0
    still_open = False

    for c in eligible:
        signal_index = c["index"]

        # Locked convention: signal on exact exit candle is allowed.
        if signal_index < position_exit_index:
            ignored += 1
            continue

        trade = calculate_trade_exit(h1, signal_index)

        if trade["status"] == "OPEN":
            still_open = True
            break

        trades.append(trade)
        position_exit_index = trade["exit_index"]

    return trades, ignored, still_open


def stats_for_trades(trades, start=None, end=None):
    filtered = []
    for t in trades:
        signal_time = t["signal_time"]
        if start is not None and signal_time < start:
            continue
        if end is not None and signal_time >= end:
            continue
        filtered.append(t)

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

    results = [t["result_r"] for t in filtered]
    winners = [r for r in results if r > 0]
    losers = [r for r in results if r < 0]

    gross_profit = sum(winners)
    gross_loss = abs(sum(losers))
    total_r = sum(results)

    if gross_loss > 0:
        pf = gross_profit / gross_loss
    elif gross_profit > 0:
        pf = 999.0
    else:
        pf = 0.0

    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    current_loss_streak = 0
    longest_loss_streak = 0

    for r in results:
        equity += r
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)

        if r < 0:
            current_loss_streak += 1
            longest_loss_streak = max(longest_loss_streak, current_loss_streak)
        else:
            current_loss_streak = 0

    return {
        "trades": len(results),
        "winners": len(winners),
        "losers": len(losers),
        "win_rate": round(len(winners) / len(results) * 100.0, 2),
        "profit_factor": round(pf, 3),
        "total_r": round(total_r, 2),
        "expectancy_r": round(total_r / len(results), 3),
        "max_drawdown_r": round(max_dd, 2),
        "longest_loss_streak": longest_loss_streak,
    }


def make_result_row(branch, timing_test, frozen_candidates, eligible, trades, ignored, still_open, years):
    full = stats_for_trades(trades)

    row = {
        "branch": branch["branch"],
        "reward_risk": REWARD_RISK,
        "structure_lookback": STRUCTURE_LOOKBACK,
        "max_distance_atr": MAX_DISTANCE_ATR,
        "min_range_atr": MIN_RANGE_ATR,
        "max_close_location": MAX_CLOSE_LOCATION,
        "min_body_ratio": MIN_BODY_RATIO,
        "min_momentum_12h_atr": branch["min_momentum_12"],
        "min_momentum_48h_atr": branch["min_momentum_48"],
        "min_upper_wick_body": branch["min_upper_wick_body"],
        "max_stop_size_atr": branch["max_stop_size_atr"],
        "min_atr_ratio_50": branch["min_atr_ratio_50"],
        "timing_type": timing_test["timing_type"],
        "timing_label": timing_test["timing_label"],
        "excluded_ny_hours": ",".join(f"{h:02d}" for h in sorted(timing_test["excluded_hours"])),
        "excluded_weekdays": ",".join(str(d) for d in sorted(timing_test["excluded_weekdays"])),
        "frozen_branch_signals": len(frozen_candidates),
        "eligible_signals_after_timing": len(eligible),
        "signals_removed_by_timing": len(frozen_candidates) - len(eligible),
        "signal_retention_pct": round(len(eligible) / len(frozen_candidates) * 100.0, 2) if frozen_candidates else 0.0,
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

        if era["trades"] >= 5:
            if era["total_r"] > 0:
                profitable_eras += 1

            minimum_era_pf = era["profit_factor"] if minimum_era_pf is None else min(minimum_era_pf, era["profit_factor"])
            minimum_era_expectancy = era["expectancy_r"] if minimum_era_expectancy is None else min(minimum_era_expectancy, era["expectancy_r"])

    row["profitable_eras_with_5_plus_trades"] = profitable_eras
    row["minimum_era_pf_5_plus"] = minimum_era_pf
    row["minimum_era_expectancy_5_plus"] = minimum_era_expectancy
    row["all_four_eras_profitable"] = profitable_eras >= 4
    row["adequate_90_trades"] = full["trades"] >= 90
    row["frequency_4py"] = full["trades"] / years >= 4.0
    row["worst_era_pf_120"] = minimum_era_pf is not None and minimum_era_pf >= 1.20
    row["worst_era_pf_130"] = minimum_era_pf is not None and minimum_era_pf >= 1.30
    row["worst_era_pf_140"] = minimum_era_pf is not None and minimum_era_pf >= 1.40
    row["annual_r_linear"] = round(full["expectancy_r"] * (full["trades"] / years), 3)

    return row


def add_baseline_deltas(df):
    df = df.copy()

    delta_columns = [
        "trades",
        "winners",
        "losers",
        "profit_factor",
        "total_r",
        "expectancy_r",
        "max_drawdown_r",
        "minimum_era_pf_5_plus",
        "2024_present_pf",
        "annual_r_linear",
    ]

    for branch_name in ["ROBUST", "HIGH_PF"]:
        mask = df["branch"] == branch_name
        baseline = df[mask & (df["timing_type"] == "BASELINE")]

        if len(baseline) != 1:
            raise RuntimeError(f"Expected exactly one baseline for {branch_name}")

        b = baseline.iloc[0]

        for col in delta_columns:
            df.loc[mask, f"delta_{col}_vs_baseline"] = df.loc[mask, col] - b[col]

    return df


def run_research():
    global STATUS

    try:
        print("=" * 78)
        print("EUR/GBP SHORT - DUAL-BRANCH LIGHT TIMING")
        print("=" * 78)
        print(f"Timing tests per branch: {len(TIMING_TESTS)}")
        print(f"Total tests: {TOTAL_TESTS}")

        STATUS.update({
            "state": "fetching_data",
            "message": "Fetching EUR/GBP OANDA H1 history",
        })

        h1 = fetch_chunked_history(
            INSTRUMENT,
            "H1",
            RESEARCH_FROM - timedelta(days=H1_WARMUP_DAYS),
            RESEARCH_TO,
        )

        if not h1:
            raise RuntimeError("No EUR/GBP H1 candles returned")

        STATUS.update({
            "state": "precomputing",
            "message": "Building ATR14 and frozen branch candidates",
        })

        h1_atr = atr_series(h1, 14)
        atr_mean_50 = rolling_mean_optional(h1_atr, 50)
        base_candidates = build_candidates(h1, h1_atr, atr_mean_50)

        STATUS["shared_geometry_signals"] = len(base_candidates)

        years = (RESEARCH_TO - RESEARCH_FROM).total_seconds() / (365.2425 * 24 * 60 * 60)

        rows = []
        completed = 0

        STATUS.update({
            "state": "running",
            "message": "Running dual-branch light timing scan",
        })

        for branch in BRANCHES:
            frozen_candidates = [
                c for c in base_candidates
                if passes_branch(c, branch)
            ]

            STATUS[f"{branch['branch'].lower()}_frozen_signals"] = len(frozen_candidates)

            print(f"{branch['branch']} frozen signals: {len(frozen_candidates)}", flush=True)

            for timing_test in TIMING_TESTS:
                eligible = [
                    c for c in frozen_candidates
                    if passes_timing(c, timing_test)
                ]

                trades, ignored, still_open = simulate(h1, eligible)

                rows.append(
                    make_result_row(
                        branch,
                        timing_test,
                        frozen_candidates,
                        eligible,
                        trades,
                        ignored,
                        still_open,
                        years,
                    )
                )

                completed += 1
                STATUS["completed_tests"] = completed

                print(
                    f"{completed}/{TOTAL_TESTS} | "
                    f"{branch['branch']} | "
                    f"{timing_test['timing_label']}",
                    flush=True,
                )

        df = pd.DataFrame(rows)

        if df.empty:
            raise RuntimeError("No result rows generated")

        df = add_baseline_deltas(df)
        df["is_baseline"] = df["timing_type"] == "BASELINE"

        df = df.sort_values(
            by=[
                "branch",
                "is_baseline",
                "all_four_eras_profitable",
                "adequate_90_trades",
                "frequency_4py",
                "worst_era_pf_140",
                "worst_era_pf_130",
                "worst_era_pf_120",
                "minimum_era_pf_5_plus",
                "profit_factor",
                "expectancy_r",
                "annual_r_linear",
                "total_r",
            ],
            ascending=[
                True,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
            ],
        )

        df.to_csv(OUTPUT_FILE, index=False)

        STATUS.update({
            "state": "complete",
            "message": "EUR/GBP dual-branch light timing completed successfully",
            "completed_tests": TOTAL_TESTS,
            "rows_saved": len(df),
            "output_file": OUTPUT_FILE,
        })

        print("=" * 78)
        print("EUR/GBP DUAL-BRANCH LIGHT TIMING COMPLETE")
        print("=" * 78)
        print(f"Rows: {len(df)}")
        print(f"Saved: {OUTPUT_FILE}")

    except Exception as error:
        STATUS.update({
            "state": "error",
            "message": str(error),
        })
        print("ERROR:", error, flush=True)


@app.route("/")
def home():
    return jsonify({
        "service": "EURGBP Short Dual-Branch Light Timing",
        "status": STATUS,
        "instrument": INSTRUMENT,
        "direction": "SHORT",
        "reward_risk": REWARD_RISK,
        "timezone": "America/New_York",
        "timing_basis": "signal candle open time",
        "trading_enabled": False,
        "orders_supported": False,
        "executor_connected": False,
        "shared_geometry": {
            "minimum_body_ratio": MIN_BODY_RATIO,
            "structure_lookback": STRUCTURE_LOOKBACK,
            "max_distance_atr": MAX_DISTANCE_ATR,
            "min_range_atr": MIN_RANGE_ATR,
            "max_close_location": MAX_CLOSE_LOCATION,
            "stop_buffer_ticks": STOP_BUFFER_TICKS,
            "backtest_slippage_ticks": BACKTEST_SLIPPAGE_TICKS,
        },
        "branches": BRANCHES,
        "timing_tests_per_branch": len(TIMING_TESTS),
        "total_tests": TOTAL_TESTS,
        "download": "/download",
    })


@app.route("/status")
def status():
    return jsonify(STATUS)


@app.route("/download")
def download():
    if not os.path.exists(OUTPUT_FILE):
        return jsonify({
            "status": "not_ready",
            "message": "EUR/GBP light timing CSV is not ready yet",
        }), 404

    return send_file(
        OUTPUT_FILE,
        as_attachment=True,
        download_name=OUTPUT_FILE,
    )


if __name__ == "__main__":
    research_thread = threading.Thread(
        target=run_research,
        name="eurgbp-short-dual-branch-light-timing",
        daemon=True,
    )
    research_thread.start()

    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
