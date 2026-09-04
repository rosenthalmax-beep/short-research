import os
import threading
import itertools
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# USD/JPY LONG - CORE INTERACTION MATRIX
#
# READ-ONLY RESEARCH - NEVER SUBMITS ORDERS
#
# CORE GRID
#   structure lookback: 40,50,60,80,100,120
#   structure distance ATR: 0,.05,.10,.20,.30,.40,.55
#   body ATR: OFF,.80,1.00,1.20
#   range ATR: OFF,1.10,1.30,1.50
#   daily close > EMA425: OFF/ON
#   exclude Wed+Thu: OFF/ON
#   exclude NY 01:00-02:59: OFF/ON
#
# Total core configs = 5,376
#
# SIDECARS
# Around six quality / high-R structure anchors:
#   body ratio: 1.00,1.20,1.40
#   strong close: OFF,.60,.70
#   EMA425: OFF/ON
#   Wed+Thu exclusion: OFF/ON
#   NY01-03 exclusion: OFF/ON
#
# Total sidecars = 432
#
# LOCKED:
#   exact bullish engulfing
#   OANDA midpoint H1
#   ATR14 Wilder/RMA SMA-seeded
#   tick=.001
#   RR=3.75
#   stop=signal low-10 ticks
#   adverse historical fill=close+5 ticks
#   target based on reference signal-close risk
#   actual R based on adverse fill
#   pyramiding=0
#   exits begin next bar
#   exact exit-candle signal allowed
#   long same-bar tie:
#       open->high closer => target first
#       otherwise stop first
#   dailyAlignment=17 America/New_York
#   previous completed daily candle only
#   signal candle OPEN time for NY filters
# ============================================================


app = Flask(__name__)

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_URL = "https://api-fxtrade.oanda.com"
INSTRUMENT = "USD_JPY"

TICK_SIZE = 0.001
STOP_BUFFER_TICKS = 10
BACKTEST_SLIPPAGE_TICKS = 5
REWARD_RISK = 3.75
ATR_LENGTH = 14

NY_TZ = ZoneInfo("America/New_York")
DAILY_ALIGNMENT_HOUR = 17
DAILY_ALIGNMENT_TIMEZONE = "America/New_York"

RESEARCH_FROM = datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc)
RESEARCH_TO = datetime.now(timezone.utc).replace(
    minute=0, second=0, microsecond=0
)

H1_CHUNK_DAYS = 180
D_CHUNK_DAYS = 1500
H1_WARMUP_DAYS = 260
D_WARMUP_DAYS = 3000

OUTPUT_CORE = "usdjpy_long_core_interaction_matrix.csv"
OUTPUT_SIDECAR = "usdjpy_long_core_interaction_sidecars.csv"

STRUCTURE_LOOKBACK_VALUES = [40, 50, 60, 80, 100, 120]
STRUCTURE_DISTANCE_VALUES = [0.00, 0.05, 0.10, 0.20, 0.30, 0.40, 0.55]
BODY_ATR_VALUES = [None, 0.80, 1.00, 1.20]
RANGE_ATR_VALUES = [None, 1.10, 1.30, 1.50]
DAILY_EMA_OPTIONS = [False, True]
WEEKDAY_OPTIONS = [False, True]
SESSION_OPTIONS = [False, True]

CORE_CONFIGS = list(itertools.product(
    STRUCTURE_LOOKBACK_VALUES,
    STRUCTURE_DISTANCE_VALUES,
    BODY_ATR_VALUES,
    RANGE_ATR_VALUES,
    DAILY_EMA_OPTIONS,
    WEEKDAY_OPTIONS,
    SESSION_OPTIONS,
))

SIDECAR_ANCHORS = [
    (60, 0.05, 1.00, None),
    (80, 0.05, 1.00, None),
    (100, 0.05, 1.00, None),
    (80, 0.30, 1.00, None),
    (100, 0.40, 1.00, None),
    (120, 0.40, 1.00, None),
]

SIDECAR_BODY_RATIO_VALUES = [1.00, 1.20, 1.40]
SIDECAR_STRONG_CLOSE_VALUES = [None, 0.60, 0.70]

ERAS = [
    ("2002_2009", RESEARCH_FROM, datetime(2010,1,1,tzinfo=timezone.utc)),
    ("2010_2017", datetime(2010,1,1,tzinfo=timezone.utc), datetime(2018,1,1,tzinfo=timezone.utc)),
    ("2018_2023", datetime(2018,1,1,tzinfo=timezone.utc), datetime(2024,1,1,tzinfo=timezone.utc)),
    ("2024_present", datetime(2024,1,1,tzinfo=timezone.utc), None),
]

STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "service": "USD/JPY Long Core Interaction Matrix",
    "instrument": INSTRUMENT,
    "core_tests": len(CORE_CONFIGS),
    "sidecar_tests": (
        len(SIDECAR_ANCHORS)
        * len(SIDECAR_BODY_RATIO_VALUES)
        * len(SIDECAR_STRONG_CLOSE_VALUES)
        * 2 * 2 * 2
    ),
    "orders_supported": False,
    "trading_enabled": False,
}


def headers():
    if not OANDA_TOKEN:
        raise RuntimeError("OANDA_TOKEN is not configured")
    return {"Authorization": f"Bearer {OANDA_TOKEN}"}


def iso_utc(dt):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def oanda_get(path, params):
    response = requests.get(
        OANDA_URL + path,
        headers=headers(),
        params=params,
        timeout=30,
    )
    if not response.ok:
        raise RuntimeError(
            f"OANDA {response.status_code}: {response.text[:500]}"
        )
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


def fetch_range(granularity, start, end):
    params = {
        "price": "M",
        "granularity": granularity,
        "from": iso_utc(start),
        "to": iso_utc(end),
        "smooth": "false",
        "includeFirst": "true",
    }
    if granularity == "D":
        params["dailyAlignment"] = DAILY_ALIGNMENT_HOUR
        params["alignmentTimezone"] = DAILY_ALIGNMENT_TIMEZONE

    data = oanda_get(
        f"/v3/instruments/{INSTRUMENT}/candles",
        params,
    )

    out = []
    for raw in data.get("candles", []):
        candle = parse_candle(raw)
        if candle is not None:
            out.append(candle)
    return out


def fetch_chunked(granularity, start, end, chunk_days):
    by_time = {}
    cursor = start
    while cursor < end:
        chunk_end = min(cursor + timedelta(days=chunk_days), end)
        print(
            f"Fetching {granularity}: {cursor.date()} -> {chunk_end.date()}",
            flush=True,
        )
        for candle in fetch_range(granularity, cursor, chunk_end):
            by_time[candle["time"]] = candle
        cursor = chunk_end

    candles = list(by_time.values())
    candles.sort(key=lambda x: x["time"])
    return candles


def true_ranges(candles):
    out = []
    for i, candle in enumerate(candles):
        if i == 0:
            tr = candle["high"] - candle["low"]
        else:
            pc = candles[i-1]["close"]
            tr = max(
                candle["high"] - candle["low"],
                abs(candle["high"] - pc),
                abs(candle["low"] - pc),
            )
        out.append(tr)
    return out


def rma_series(values, length):
    result = [None] * len(values)
    if len(values) < length:
        return result

    initial = sum(values[:length]) / length
    result[length-1] = initial
    prev = initial

    for i in range(length, len(values)):
        cur = ((prev * (length-1)) + values[i]) / length
        result[i] = cur
        prev = cur

    return result


def atr_series(candles):
    return rma_series(true_ranges(candles), ATR_LENGTH)


def ema_series(values, length):
    result = [None] * len(values)
    if len(values) < length:
        return result

    initial = sum(values[:length]) / length
    result[length-1] = initial
    mult = 2.0 / (length + 1.0)
    prev = initial

    for i in range(length, len(values)):
        cur = ((values[i] - prev) * mult) + prev
        result[i] = cur
        prev = cur

    return result


def current_daily_start(timestamp_utc):
    ny = timestamp_utc.astimezone(NY_TZ)
    candidate = ny.replace(
        hour=DAILY_ALIGNMENT_HOUR,
        minute=0,
        second=0,
        microsecond=0,
    )
    if ny < candidate:
        candidate -= timedelta(days=1)
    return candidate.astimezone(timezone.utc)


def prepare_daily(daily):
    closes = [x["close"] for x in daily]
    ema425 = ema_series(closes, 425)
    return [
        {
            "time": candle["time"],
            "close": candle["close"],
            "ema425": ema425[i],
        }
        for i, candle in enumerate(daily)
    ]


def previous_completed_daily(signal_time, daily_state):
    session_start = current_daily_start(signal_time)
    selected = None
    for row in daily_state:
        if row["time"] < session_start:
            selected = row
        else:
            break
    return selected


MAX_STRUCTURE_LOOKBACK = max(STRUCTURE_LOOKBACK_VALUES)


def build_raw_candidates(h1, atr, daily_state):
    rows = []
    start_index = max(ATR_LENGTH, MAX_STRUCTURE_LOOKBACK)

    for i in range(start_index, len(h1)):
        signal = h1[i]

        if signal["time"] < RESEARCH_FROM:
            continue
        if signal["time"] >= RESEARCH_TO:
            break

        previous = h1[i-1]
        current_atr = atr[i]

        if current_atr is None or current_atr <= 0:
            continue

        previous_body = abs(previous["close"] - previous["open"])
        current_body = abs(signal["close"] - signal["open"])
        signal_range = signal["high"] - signal["low"]

        if previous_body <= 0 or current_body <= 0 or signal_range <= 0:
            continue

        bullish_engulfing = (
            previous["close"] < previous["open"]
            and signal["close"] > signal["open"]
            and signal["open"] <= previous["close"]
            and signal["close"] >= previous["open"]
        )

        if not bullish_engulfing:
            continue

        body_ratio = current_body / previous_body
        if body_ratio < 1.00:
            continue

        structure = {}
        for lookback in STRUCTURE_LOOKBACK_VALUES:
            previous_lowest = min(
                x["low"]
                for x in h1[i-lookback:i]
            )
            structure[lookback] = (
                signal["low"] - previous_lowest
            ) / current_atr

        ny = signal["time"].astimezone(NY_TZ)

        rows.append({
            "index": i,
            "time": signal["time"],
            "body_ratio": body_ratio,
            "body_atr": current_body / current_atr,
            "range_atr": signal_range / current_atr,
            "close_location": (
                signal["close"] - signal["low"]
            ) / signal_range,
            "structure": structure,
            "daily": previous_completed_daily(
                signal["time"], daily_state
            ),
            "ny_hour": ny.hour,
            "ny_weekday": ny.weekday(),
        })

    return rows


EXIT_CACHE = {}


def calculate_trade_exit(h1, signal_index):
    if signal_index in EXIT_CACHE:
        return EXIT_CACHE[signal_index]

    signal = h1[signal_index]
    reference_entry = signal["close"]
    backtest_entry = (
        reference_entry
        + BACKTEST_SLIPPAGE_TICKS * TICK_SIZE
    )
    stop = (
        signal["low"]
        - STOP_BUFFER_TICKS * TICK_SIZE
    )
    reference_risk = reference_entry - stop

    if reference_risk <= 0:
        EXIT_CACHE[signal_index] = None
        return None

    target = (
        reference_entry
        + reference_risk * REWARD_RISK
    )
    actual_risk = backtest_entry - stop

    if actual_risk <= 0:
        EXIT_CACHE[signal_index] = None
        return None

    for j in range(signal_index + 1, len(h1)):
        candle = h1[j]

        if candle["time"] >= RESEARCH_TO:
            break

        stop_hit = candle["low"] <= stop
        target_hit = candle["high"] >= target

        if not (stop_hit or target_hit):
            continue

        if stop_hit and target_hit:
            d_high = abs(candle["high"] - candle["open"])
            d_low = abs(candle["open"] - candle["low"])
            exit_price = target if d_high < d_low else stop
        elif target_hit:
            exit_price = target
        else:
            exit_price = stop

        result = {
            "signal_index": signal_index,
            "signal_time": signal["time"],
            "exit_index": j,
            "exit_time": candle["time"],
            "result_r": (
                exit_price - backtest_entry
            ) / actual_risk,
        }

        EXIT_CACHE[signal_index] = result
        return result

    EXIT_CACHE[signal_index] = None
    return None


def simulate_variant(h1, eligible):
    trades = []
    ignored = 0
    position_exit_index = -1

    for signal in eligible:
        i = signal["index"]

        if i < position_exit_index:
            ignored += 1
            continue

        trade = calculate_trade_exit(h1, i)

        if trade is None:
            break

        trades.append(trade)
        position_exit_index = trade["exit_index"]

    return trades, ignored


def stats_for_trades(trades, start=None, end=None):
    selected = []

    for trade in trades:
        t = trade["signal_time"]
        if start is not None and t < start:
            continue
        if end is not None and t >= end:
            continue
        selected.append(trade)

    if not selected:
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

    results = [x["result_r"] for x in selected]
    winners = [r for r in results if r > 0]
    losers = [r for r in results if r < 0]

    gp = sum(winners)
    gl = abs(sum(losers))

    if gl > 0:
        pf = gp / gl
    elif gp > 0:
        pf = 999.0
    else:
        pf = 0.0

    total_r = sum(results)

    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    current_streak = 0
    longest_streak = 0

    for r in results:
        equity += r
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)

        if r < 0:
            current_streak += 1
            longest_streak = max(
                longest_streak,
                current_streak,
            )
        else:
            current_streak = 0

    return {
        "trades": len(results),
        "winners": len(winners),
        "losers": len(losers),
        "win_rate": round(
            len(winners) / len(results) * 100.0, 2
        ),
        "profit_factor": round(pf, 3),
        "total_r": round(total_r, 2),
        "expectancy_r": round(
            total_r / len(results), 3
        ),
        "max_drawdown_r": round(max_dd, 2),
        "longest_loss_streak": longest_streak,
    }


def subtract_years_safe(dt, years):
    try:
        return dt.replace(year=dt.year-years)
    except ValueError:
        return dt.replace(
            month=2,
            day=28,
            year=dt.year-years,
        )


def rolling_3y_worst(trades):
    rows = []

    for start_year in range(
        2002,
        RESEARCH_TO.year - 1,
    ):
        start = max(
            RESEARCH_FROM,
            datetime(
                start_year, 1, 1,
                tzinfo=timezone.utc,
            ),
        )
        end = min(
            RESEARCH_TO,
            datetime(
                start_year + 3, 1, 1,
                tzinfo=timezone.utc,
            ),
        )

        if start >= end:
            continue

        s = stats_for_trades(
            trades, start, end
        )

        if s["trades"] >= 5:
            rows.append({
                "label":
                    f"{start_year}_{start_year+2}",
                "pf":
                    s["profit_factor"],
                "expectancy":
                    s["expectancy_r"],
                "total_r":
                    s["total_r"],
            })

    if not rows:
        return {
            "worst_rolling_3y_pf": None,
            "worst_rolling_3y_pf_label": None,
            "worst_rolling_3y_expectancy": None,
            "worst_rolling_3y_expectancy_label": None,
            "worst_rolling_3y_total_r": None,
            "worst_rolling_3y_total_r_label": None,
        }

    worst_pf = min(rows, key=lambda x: x["pf"])
    worst_exp = min(
        rows, key=lambda x: x["expectancy"]
    )
    worst_total = min(
        rows, key=lambda x: x["total_r"]
    )

    return {
        "worst_rolling_3y_pf":
            worst_pf["pf"],
        "worst_rolling_3y_pf_label":
            worst_pf["label"],
        "worst_rolling_3y_expectancy":
            worst_exp["expectancy"],
        "worst_rolling_3y_expectancy_label":
            worst_exp["label"],
        "worst_rolling_3y_total_r":
            worst_total["total_r"],
        "worst_rolling_3y_total_r_label":
            worst_total["label"],
    }


def make_result_row(
    label,
    eligible,
    trades,
    ignored,
    years,
    lookback,
    distance,
    body_atr,
    range_atr,
    ema425,
    exclude_wed_thu,
    exclude_ny_01_03,
    body_ratio=1.00,
    strong_close=None,
):
    full = stats_for_trades(trades)

    row = {
        "label": label,
        "structure_lookback": lookback,
        "maximum_distance_atr": distance,
        "minimum_body_atr": body_atr,
        "minimum_range_atr": range_atr,
        "daily_close_above_ema425": ema425,
        "exclude_wed_thu": exclude_wed_thu,
        "exclude_ny_01_03": exclude_ny_01_03,
        "minimum_body_ratio": body_ratio,
        "minimum_close_location": strong_close,
        "eligible_signals": len(eligible),
        "ignored_due_to_open_trade": ignored,
        "trades": full["trades"],
        "trades_per_year": round(
            full["trades"] / years, 3
        ),
        "winners": full["winners"],
        "losers": full["losers"],
        "win_rate": full["win_rate"],
        "profit_factor": full["profit_factor"],
        "total_r": full["total_r"],
        "expectancy_r": full["expectancy_r"],
        "max_drawdown_r": full["max_drawdown_r"],
        "longest_loss_streak":
            full["longest_loss_streak"],
    }

    minimum_era_pf = None
    profitable_eras = 0

    for era_name, era_start, era_end in ERAS:
        s = stats_for_trades(
            trades,
            era_start,
            RESEARCH_TO if era_end is None
            else min(era_end, RESEARCH_TO),
        )

        row[f"{era_name}_trades"] = s["trades"]
        row[f"{era_name}_pf"] = s["profit_factor"]
        row[f"{era_name}_r"] = s["total_r"]
        row[f"{era_name}_expectancy"] = s["expectancy_r"]

        if s["trades"] >= 5:
            minimum_era_pf = (
                s["profit_factor"]
                if minimum_era_pf is None
                else min(
                    minimum_era_pf,
                    s["profit_factor"],
                )
            )
            if s["total_r"] > 0:
                profitable_eras += 1

    row["minimum_era_pf_5_plus"] = minimum_era_pf
    row["profitable_eras"] = profitable_eras

    for years_back in [2, 5, 10]:
        start = subtract_years_safe(
            RESEARCH_TO, years_back
        )
        s = stats_for_trades(
            trades, start, RESEARCH_TO
        )

        row[f"last_{years_back}y_trades"] = s["trades"]
        row[f"last_{years_back}y_pf"] = s["profit_factor"]
        row[f"last_{years_back}y_r"] = s["total_r"]
        row[
            f"last_{years_back}y_expectancy"
        ] = s["expectancy_r"]

    row.update(rolling_3y_worst(trades))
    return row


def eligible_for(
    raw,
    lookback,
    distance,
    body_atr,
    range_atr,
    ema425,
    exclude_wed_thu,
    exclude_ny_01_03,
    body_ratio=1.00,
    strong_close=None,
):
    out = []

    for signal in raw:
        if signal["structure"][lookback] > distance:
            continue

        if (
            body_atr is not None
            and signal["body_atr"] < body_atr
        ):
            continue

        if (
            range_atr is not None
            and signal["range_atr"] < range_atr
        ):
            continue

        if signal["body_ratio"] < body_ratio:
            continue

        if (
            strong_close is not None
            and signal["close_location"] < strong_close
        ):
            continue

        if ema425:
            daily = signal["daily"]
            if (
                daily is None
                or daily["ema425"] is None
                or not (
                    daily["close"] > daily["ema425"]
                )
            ):
                continue

        if (
            exclude_wed_thu
            and signal["ny_weekday"] in {2, 3}
        ):
            continue

        if (
            exclude_ny_01_03
            and signal["ny_hour"] in {1, 2}
        ):
            continue

        out.append(signal)

    return out


def run_research():
    try:
        STATUS.update({
            "state": "fetching_h1",
            "message": "Fetching USD/JPY H1 history",
        })

        h1 = fetch_chunked(
            "H1",
            RESEARCH_FROM - timedelta(days=H1_WARMUP_DAYS),
            RESEARCH_TO,
            H1_CHUNK_DAYS,
        )

        if not h1:
            raise RuntimeError("No H1 candles returned")

        STATUS.update({
            "state": "fetching_daily",
            "message": "Fetching USD/JPY daily history",
        })

        daily = fetch_chunked(
            "D",
            RESEARCH_FROM - timedelta(days=D_WARMUP_DAYS),
            RESEARCH_TO,
            D_CHUNK_DAYS,
        )

        if not daily:
            raise RuntimeError("No daily candles returned")

        STATUS.update({
            "state": "precomputing",
            "message": "Precomputing interaction features",
        })

        atr = atr_series(h1)
        daily_state = prepare_daily(daily)
        raw = build_raw_candidates(
            h1, atr, daily_state
        )

        STATUS["raw_candidates"] = len(raw)

        years = (
            RESEARCH_TO - RESEARCH_FROM
        ).total_seconds() / (
            365.2425 * 86400
        )

        # -------------------------
        # CORE
        # -------------------------
        core_rows = []

        STATUS.update({
            "state": "running_core",
            "message":
                f"Running {len(CORE_CONFIGS)} core configs",
            "completed_core": 0,
        })

        for number, cfg in enumerate(
            CORE_CONFIGS, start=1
        ):
            (
                lookback,
                distance,
                body_atr,
                range_atr,
                ema425,
                exclude_wed_thu,
                exclude_ny_01_03,
            ) = cfg

            eligible = eligible_for(
                raw,
                lookback,
                distance,
                body_atr,
                range_atr,
                ema425,
                exclude_wed_thu,
                exclude_ny_01_03,
            )

            trades, ignored = simulate_variant(
                h1, eligible
            )

            body_label = (
                "OFF"
                if body_atr is None
                else f"{body_atr:.2f}"
            )
            range_label = (
                "OFF"
                if range_atr is None
                else f"{range_atr:.2f}"
            )

            label = (
                f"S{lookback}_D{distance:.2f}_"
                f"BODY{body_label}_"
                f"RANGE{range_label}_"
                f"EMA425{'ON' if ema425 else 'OFF'}_"
                f"WEDTHU{'EX' if exclude_wed_thu else 'ALL'}_"
                f"NY0103{'EX' if exclude_ny_01_03 else 'ALL'}"
            )

            core_rows.append(
                make_result_row(
                    label,
                    eligible,
                    trades,
                    ignored,
                    years,
                    lookback,
                    distance,
                    body_atr,
                    range_atr,
                    ema425,
                    exclude_wed_thu,
                    exclude_ny_01_03,
                )
            )

            STATUS["completed_core"] = number

            if (
                number % 100 == 0
                or number == len(CORE_CONFIGS)
            ):
                print(
                    f"Core {number}/{len(CORE_CONFIGS)}",
                    flush=True,
                )

        core_df = pd.DataFrame(core_rows)

        core_df = core_df.sort_values(
            [
                "profit_factor",
                "total_r",
                "worst_rolling_3y_pf",
                "expectancy_r",
            ],
            ascending=[False, False, False, False],
        ).reset_index(drop=True)

        core_df.to_csv(
            os.path.abspath(OUTPUT_CORE),
            index=False,
        )

        # -------------------------
        # SIDECARS
        # -------------------------
        side_rows = []

        side_configs = list(itertools.product(
            SIDECAR_ANCHORS,
            SIDECAR_BODY_RATIO_VALUES,
            SIDECAR_STRONG_CLOSE_VALUES,
            [False, True],
            [False, True],
            [False, True],
        ))

        STATUS.update({
            "state": "running_sidecars",
            "message":
                f"Running {len(side_configs)} sidecar configs",
            "completed_sidecars": 0,
        })

        for number, cfg in enumerate(
            side_configs, start=1
        ):
            (
                anchor,
                body_ratio,
                strong_close,
                ema425,
                exclude_wed_thu,
                exclude_ny_01_03,
            ) = cfg

            (
                lookback,
                distance,
                body_atr,
                range_atr,
            ) = anchor

            eligible = eligible_for(
                raw,
                lookback,
                distance,
                body_atr,
                range_atr,
                ema425,
                exclude_wed_thu,
                exclude_ny_01_03,
                body_ratio=body_ratio,
                strong_close=strong_close,
            )

            trades, ignored = simulate_variant(
                h1, eligible
            )

            sc_label = (
                "OFF"
                if strong_close is None
                else f"{strong_close:.2f}"
            )

            label = (
                f"S{lookback}_D{distance:.2f}_"
                f"BODY{body_atr:.2f}_"
                f"BR{body_ratio:.2f}_"
                f"SC{sc_label}_"
                f"EMA425{'ON' if ema425 else 'OFF'}_"
                f"WEDTHU{'EX' if exclude_wed_thu else 'ALL'}_"
                f"NY0103{'EX' if exclude_ny_01_03 else 'ALL'}"
            )

            side_rows.append(
                make_result_row(
                    label,
                    eligible,
                    trades,
                    ignored,
                    years,
                    lookback,
                    distance,
                    body_atr,
                    range_atr,
                    ema425,
                    exclude_wed_thu,
                    exclude_ny_01_03,
                    body_ratio=body_ratio,
                    strong_close=strong_close,
                )
            )

            STATUS["completed_sidecars"] = number

        side_df = pd.DataFrame(side_rows)

        side_df = side_df.sort_values(
            [
                "profit_factor",
                "total_r",
                "worst_rolling_3y_pf",
                "expectancy_r",
            ],
            ascending=[False, False, False, False],
        ).reset_index(drop=True)

        side_df.to_csv(
            os.path.abspath(OUTPUT_SIDECAR),
            index=False,
        )

        STATUS.update({
            "state": "complete",
            "message":
                "USD/JPY core interaction matrix complete",
            "core_rows": len(core_df),
            "sidecar_rows": len(side_df),
            "outputs": {
                "core": OUTPUT_CORE,
                "sidecars": OUTPUT_SIDECAR,
            },
        })

        print()
        print("=" * 100)
        print("USD/JPY CORE INTERACTION MATRIX COMPLETE")
        print("=" * 100)
        print("TOP CORE")
        print(
            core_df.head(30).to_string(index=False),
            flush=True,
        )
        print()
        print("TOP SIDECARS")
        print(
            side_df.head(30).to_string(index=False),
            flush=True,
        )

    except Exception as error:
        STATUS.update({
            "state": "error",
            "message": str(error),
        })
        print("ERROR:", error, flush=True)


@app.route("/")
def home():
    return jsonify({
        "service":
            "USD/JPY Long Core Interaction Matrix",
        "status": STATUS,
        "mode": "READ_ONLY_RESEARCH",
        "orders_supported": False,
        "trading_enabled": False,
        "downloads": {
            "core": "/download/core",
            "sidecars": "/download/sidecars",
        },
    })


@app.route("/status")
def status():
    return jsonify(STATUS)


def send_output(filename):
    path = os.path.abspath(filename)

    if not os.path.exists(path):
        return jsonify({
            "status": "not_ready",
            "message": f"{filename} is not ready yet",
        }), 404

    return send_file(
        path,
        as_attachment=True,
        download_name=filename,
    )


@app.route("/download/core")
def download_core():
    return send_output(OUTPUT_CORE)


@app.route("/download/sidecars")
def download_sidecars():
    return send_output(OUTPUT_SIDECAR)


if __name__ == "__main__":
    thread = threading.Thread(
        target=run_research,
        name="usdjpy-long-core-interaction",
        daemon=True,
    )
    thread.start()

    port = int(os.getenv("PORT", 5000))

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
    )
