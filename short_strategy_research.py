
import os
import csv
import time
import bisect
import zipfile
import threading
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from flask import Flask, jsonify, send_file


# ============================================================
# GBP/USD M15 SHORT - GEN2 TARGETED STRUCTURAL SEARCH
#
# Seed from first exhaustive pass:
#   exact bearish engulfing
#   structure lookback 150
#   distance <= 0.05 ATR14
#   RR 3.00
#   1.0 pip adverse development cost
#
# First-pass seed result:
#   212 trades
#   PF 1.027769
#   +4.2209R
#
# Purpose:
#   Work out WHAT the weak S150/D0.05 edge actually represents
#   before deciding whether GBP/USD M15 short is viable.
#
# New hypotheses tested around the seed:
#   - fine structure neighbourhood: 100-220 bars / 0.025-0.15 ATR
#   - actual prior-high sweep
#   - prior-high sweep + reclaim
#   - prior upside momentum before reversal
#   - body/range/close-strength/upper-wick quality
#   - session and weekday effects inside structure signals
#   - previous-day high proximity / sweep / reclaim
#   - completed H1/H4 structural-high proximity
#   - RR neighbourhood
#   - controlled two-family interactions
#
# Correctness:
#   - OANDA midpoint M15
#   - ATR14 Wilder/RMA, SMA seeded
#   - signal timestamp = M15 candle OPEN
#   - HTF state only after candle has COMPLETED
#   - H1/H4/Daily completion = next actual OANDA candle open
#   - dailyAlignment 17 America/New_York
#   - prior momentum ends at PREVIOUS M15 candle
#   - stop = signal high + 10 ticks
#   - target based on REFERENCE signal-close risk
#   - adverse short entry = signal close - cost
#   - exits begin NEXT candle
#   - same-bar short tie: high closer => STOP, else TARGET
#   - pyramiding 0
#   - exact exit-candle signal eligible
#
# Validation:
#   - costs 0.5 / 1.0 / 1.5 / 2.0 pips
#   - 4 eras
#   - DEV 2010-2017 / VALIDATION 2018-now
#   - recent 5Y / 2Y
#   - rolling 2Y / 3Y
#   - overlap vs original S150/D0.05 seed
#
# READ ONLY. NEVER SENDS ORDERS.
# ============================================================


app = Flask(__name__)

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_BASE = os.getenv(
    "OANDA_API_URL",
    "https://api-fxtrade.oanda.com",
)

INSTRUMENT = "GBP_USD"

RESEARCH_FROM = datetime(
    2010, 1, 1, tzinfo=timezone.utc
)

RESEARCH_TO = (
    datetime.now(timezone.utc)
    .replace(minute=0, second=0, microsecond=0)
)

NY = ZoneInfo("America/New_York")

TICK_SIZE = 0.00001
PIP_SIZE = 0.0001
STOP_BUFFER_TICKS = 10

PRIMARY_COST_PIPS = 1.00
COST_PIPS_GRID = [0.50, 1.00, 1.50, 2.00]

RR_VALUES = [
    2.50,
    2.75,
    3.00,
    3.25,
    3.50,
    3.75,
    4.00,
]

STRUCTURE_LOOKBACKS = [
    100,
    120,
    150,
    180,
    220,
]

STRUCTURE_DISTANCES = [
    0.025,
    0.050,
    0.075,
    0.100,
    0.150,
]

MIN_TRADES_PRIMARY = 45


# ============================================================
# OUTPUTS
# ============================================================

OUTPUT_NEIGHBOURHOOD = (
    "gbpusd_m15_short_gen2_structure_neighbourhood.csv"
)

OUTPUT_SINGLE = (
    "gbpusd_m15_short_gen2_single_family.csv"
)

OUTPUT_INTERACTIONS = (
    "gbpusd_m15_short_gen2_interactions.csv"
)

OUTPUT_TOP = (
    "gbpusd_m15_short_gen2_top.csv"
)

OUTPUT_ERAS = (
    "gbpusd_m15_short_gen2_eras.csv"
)

OUTPUT_DEVVAL = (
    "gbpusd_m15_short_gen2_dev_validation.csv"
)

OUTPUT_RECENT = (
    "gbpusd_m15_short_gen2_recent.csv"
)

OUTPUT_ROLLING = (
    "gbpusd_m15_short_gen2_rolling.csv"
)

OUTPUT_ROLLING_SUMMARY = (
    "gbpusd_m15_short_gen2_rolling_summary.csv"
)

OUTPUT_OVERLAP = (
    "gbpusd_m15_short_gen2_overlap.csv"
)

OUTPUT_BEST_TRADES = (
    "gbpusd_m15_short_gen2_best_trades.csv"
)

OUTPUT_BUNDLE = (
    "gbpusd_m15_short_gen2_RESULTS.zip"
)

STATUS = {
    "state": "not_started",
    "message": "GBP/USD M15 short Gen2 not started",
    "service": "GBPUSD M15 Short Gen2 Targeted Structural",
    "orders_supported": False,
    "trading_enabled": False,
}


# ============================================================
# HELPERS
# ============================================================

def iso_utc(dt):
    return (
        dt.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def parse_oanda_time(value):
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    if "." in value:
        left, right = value.split(".", 1)
        if "+" in right:
            fraction, offset = right.split("+", 1)
            fraction = fraction[:6].ljust(6, "0")
            value = left + "." + fraction + "+" + offset

    return datetime.fromisoformat(value).astimezone(
        timezone.utc
    )


def years_ago_safe(dt, years):
    try:
        return dt.replace(year=dt.year - years)
    except ValueError:
        return dt.replace(
            month=2,
            day=28,
            year=dt.year - years,
        )


def month_start(dt):
    return datetime(
        dt.year,
        dt.month,
        1,
        tzinfo=timezone.utc,
    )


def add_months(dt, months):
    total = dt.year * 12 + dt.month - 1 + months
    return datetime(
        total // 12,
        total % 12 + 1,
        1,
        tzinfo=timezone.utc,
    )


def clone_config(base, label):
    result = {}
    for key, value in base.items():
        if isinstance(value, set):
            result[key] = set(value)
        else:
            result[key] = value
    result["label"] = label
    return result


def write_csv(path, rows):
    if not rows:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("")
        return

    fields = []
    seen = set()

    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fields.append(key)

    with open(
        path,
        "w",
        newline="",
        encoding="utf-8",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fields,
        )
        writer.writeheader()
        writer.writerows(rows)


def download_file(path):
    if not os.path.exists(path):
        return jsonify({
            "error": "Output not ready yet",
            "path": path,
        }), 404

    return send_file(
        os.path.abspath(path),
        as_attachment=True,
        download_name=os.path.basename(path),
    )


def build_results_bundle():
    files = [
        OUTPUT_NEIGHBOURHOOD,
        OUTPUT_SINGLE,
        OUTPUT_INTERACTIONS,
        OUTPUT_TOP,
        OUTPUT_ERAS,
        OUTPUT_DEVVAL,
        OUTPUT_RECENT,
        OUTPUT_ROLLING,
        OUTPUT_ROLLING_SUMMARY,
        OUTPUT_OVERLAP,
        OUTPUT_BEST_TRADES,
    ]

    with zipfile.ZipFile(
        OUTPUT_BUNDLE,
        "w",
        compression=zipfile.ZIP_DEFLATED,
    ) as archive:
        for path in files:
            if os.path.exists(path):
                archive.write(
                    path,
                    arcname=os.path.basename(path),
                )


# ============================================================
# OANDA FETCH
# ============================================================

def oanda_headers():
    if not OANDA_TOKEN:
        raise RuntimeError(
            "OANDA_TOKEN is not configured"
        )

    return {
        "Authorization":
            "Bearer " + OANDA_TOKEN.strip(),
        "Content-Type":
            "application/json",
    }


def fetch_chunk(
    granularity,
    start,
    end,
    daily_alignment=False,
):
    url = (
        f"{OANDA_BASE}/v3/instruments/"
        f"{INSTRUMENT}/candles"
    )

    params = {
        "price": "M",
        "granularity": granularity,
        "smooth": "false",
        "from": iso_utc(start),
        "to": iso_utc(end),
        "includeFirst": "true",
    }

    if daily_alignment:
        params["dailyAlignment"] = 17
        params["alignmentTimezone"] = (
            "America/New_York"
        )

    response = requests.get(
        url,
        headers=oanda_headers(),
        params=params,
        timeout=60,
    )

    response.raise_for_status()

    rows = []

    for item in response.json().get(
        "candles", []
    ):
        if not item.get("complete", False):
            continue

        mid = item["mid"]

        rows.append({
            "time": parse_oanda_time(
                item["time"]
            ),
            "open": float(mid["o"]),
            "high": float(mid["h"]),
            "low": float(mid["l"]),
            "close": float(mid["c"]),
        })

    return rows


def fetch_history(
    granularity,
    start,
    end,
    chunk_days,
    daily_alignment=False,
):
    cursor = start
    by_time = {}
    chunk_number = 0

    while cursor < end:
        chunk_number += 1

        chunk_end = min(
            cursor + timedelta(days=chunk_days),
            end,
        )

        STATUS.update({
            "state": "fetching",
            "message": (
                f"Fetching {granularity} chunk "
                f"{chunk_number}: {iso_utc(cursor)} "
                f"-> {iso_utc(chunk_end)}"
            ),
        })

        for row in fetch_chunk(
            granularity,
            cursor,
            chunk_end,
            daily_alignment=daily_alignment,
        ):
            by_time[row["time"]] = row

        cursor = chunk_end
        time.sleep(0.02)

    rows = list(by_time.values())
    rows.sort(key=lambda row: row["time"])
    return rows


# ============================================================
# INDICATORS
# ============================================================

def true_ranges(candles):
    result = [None] * len(candles)

    for i, candle in enumerate(candles):
        if i == 0:
            result[i] = (
                candle["high"] - candle["low"]
            )
        else:
            prev_close = candles[i - 1]["close"]
            result[i] = max(
                candle["high"] - candle["low"],
                abs(candle["high"] - prev_close),
                abs(candle["low"] - prev_close),
            )

    return result


def rma(values, length):
    result = [None] * len(values)

    if len(values) < length:
        return result

    seed = values[:length]
    if any(v is None for v in seed):
        return result

    result[length - 1] = sum(seed) / length

    for i in range(length, len(values)):
        if (
            values[i] is None
            or result[i - 1] is None
        ):
            continue

        result[i] = (
            result[i - 1] * (length - 1)
            + values[i]
        ) / length

    return result


def atr14(candles):
    return rma(
        true_ranges(candles),
        14,
    )


def bearish_engulfing(candles, i):
    if i < 1:
        return False

    previous = candles[i - 1]
    current = candles[i]

    return (
        previous["close"] > previous["open"]
        and current["close"] < current["open"]
        and current["open"] >= previous["close"]
        and current["close"] <= previous["open"]
    )


# ============================================================
# HTF STATE - COMPLETED ONLY
# ============================================================

def build_htf_state(
    candles,
    structural_lookbacks,
):
    atr = atr14(candles)
    rows = []

    for i, candle in enumerate(candles):
        structural_highs = {}

        for lookback in structural_lookbacks:
            if i >= lookback:
                structural_highs[lookback] = max(
                    c["high"]
                    for c in candles[
                        i - lookback:i
                    ]
                )
            else:
                structural_highs[lookback] = None

        complete_at = (
            candles[i + 1]["time"]
            if i + 1 < len(candles)
            else None
        )

        rows.append({
            "time": candle["time"],
            "complete_at": complete_at,
            "open": candle["open"],
            "high": candle["high"],
            "low": candle["low"],
            "close": candle["close"],
            "atr14": atr[i],
            "structural_highs":
                structural_highs,
        })

    return rows


def previous_completed_state(
    rows,
    completion_times,
    signal_time,
):
    position = bisect.bisect_right(
        completion_times,
        signal_time,
    ) - 1

    if position < 0:
        return None

    return rows[position]


# ============================================================
# SIGNAL FEATURE CACHE
# ============================================================

def build_signal_cache(
    m15,
    m15_atr,
    h1_state,
    h4_state,
    daily_state,
):
    signals = []

    h1_rows = [
        row for row in h1_state
        if row["complete_at"] is not None
    ]
    h4_rows = [
        row for row in h4_state
        if row["complete_at"] is not None
    ]
    daily_rows = [
        row for row in daily_state
        if row["complete_at"] is not None
    ]

    h1_times = [
        row["complete_at"]
        for row in h1_rows
    ]
    h4_times = [
        row["complete_at"]
        for row in h4_rows
    ]
    daily_times = [
        row["complete_at"]
        for row in daily_rows
    ]

    max_lookback = max(
        max(STRUCTURE_LOOKBACKS),
        97,
    )

    for i in range(
        max_lookback,
        len(m15),
    ):
        if not bearish_engulfing(m15, i):
            continue

        current = m15[i]
        previous = m15[i - 1]
        atr = m15_atr[i]

        if atr is None or atr <= 0:
            continue

        body = (
            current["open"] - current["close"]
        )

        prev_body = abs(
            previous["close"] - previous["open"]
        )

        body_ratio = (
            body / prev_body
            if prev_body > 0
            else 999.0
        )

        candle_range = (
            current["high"] - current["low"]
        )

        body_atr = body / atr
        range_atr = candle_range / atr

        close_location = (
            (
                current["close"] - current["low"]
            ) / candle_range
            if candle_range > 0
            else 1.0
        )

        upper_wick = (
            current["high"]
            - max(
                current["open"],
                current["close"],
            )
        )

        upper_wick_body = (
            upper_wick / body
            if body > 0
            else 0.0
        )

        structure = {}

        for lookback in STRUCTURE_LOOKBACKS:
            prior_high = max(
                candle["high"]
                for candle in m15[
                    i - lookback:i
                ]
            )

            structure[lookback] = {
                "prior_high":
                    prior_high,
                "distance_atr":
                    (
                        abs(
                            current["high"]
                            - prior_high
                        ) / atr
                    ),
                "swept":
                    current["high"] > prior_high,
                "reclaimed":
                    (
                        current["high"] > prior_high
                        and current["close"] < prior_high
                    ),
            }

        # Strictly PRE-SIGNAL momentum.
        prior_close = m15[i - 1]["close"]

        momentum_4h = (
            prior_close
            - m15[i - 17]["close"]
        ) / atr

        momentum_12h = (
            prior_close
            - m15[i - 49]["close"]
        ) / atr

        momentum_24h = (
            prior_close
            - m15[i - 97]["close"]
        ) / atr

        ny = current["time"].astimezone(NY)

        h1_prev = previous_completed_state(
            h1_rows,
            h1_times,
            current["time"],
        )

        h4_prev = previous_completed_state(
            h4_rows,
            h4_times,
            current["time"],
        )

        daily_prev = previous_completed_state(
            daily_rows,
            daily_times,
            current["time"],
        )

        prev_day_high_distance_atr = None
        prev_day_high_sweep = False
        prev_day_high_reclaim = False

        if (
            daily_prev is not None
            and daily_prev["atr14"] is not None
            and daily_prev["atr14"] > 0
        ):
            prev_high = daily_prev["high"]

            prev_day_high_distance_atr = (
                abs(
                    current["high"] - prev_high
                ) / atr
            )

            prev_day_high_sweep = (
                current["high"] > prev_high
            )

            prev_day_high_reclaim = (
                prev_day_high_sweep
                and current["close"] < prev_high
            )

        h1_high_distance = {}
        h4_high_distance = {}

        if (
            h1_prev is not None
            and h1_prev["atr14"] is not None
            and h1_prev["atr14"] > 0
        ):
            for lb in [20, 50]:
                high = h1_prev[
                    "structural_highs"
                ].get(lb)

                h1_high_distance[lb] = (
                    abs(
                        current["high"] - high
                    ) / h1_prev["atr14"]
                    if high is not None
                    else None
                )

        if (
            h4_prev is not None
            and h4_prev["atr14"] is not None
            and h4_prev["atr14"] > 0
        ):
            for lb in [10, 20]:
                high = h4_prev[
                    "structural_highs"
                ].get(lb)

                h4_high_distance[lb] = (
                    abs(
                        current["high"] - high
                    ) / h4_prev["atr14"]
                    if high is not None
                    else None
                )

        signals.append({
            "signal_index": i,
            "time": current["time"],
            "body_ratio": body_ratio,
            "body_atr": body_atr,
            "range_atr": range_atr,
            "close_location": close_location,
            "upper_wick_body":
                upper_wick_body,
            "structure": structure,
            "momentum_4h_atr":
                momentum_4h,
            "momentum_12h_atr":
                momentum_12h,
            "momentum_24h_atr":
                momentum_24h,
            "ny_hour": ny.hour,
            "ny_weekday": ny.weekday(),
            "prev_day_high_distance_atr":
                prev_day_high_distance_atr,
            "prev_day_high_sweep":
                prev_day_high_sweep,
            "prev_day_high_reclaim":
                prev_day_high_reclaim,
            "h1_high_distance":
                h1_high_distance,
            "h4_high_distance":
                h4_high_distance,
        })

    return signals


# ============================================================
# CONFIG
# ============================================================

SEED = {
    "label": "SEED_S150_D0.05",
    "structure_lookback": 150,
    "maximum_structure_distance_atr": 0.05,

    "require_structure_sweep": False,
    "require_structure_reclaim": False,

    "minimum_body_atr": None,
    "minimum_range_atr": None,
    "maximum_close_location": None,
    "minimum_upper_wick_body": None,

    "minimum_momentum_4h_atr": None,
    "maximum_momentum_4h_atr": None,
    "minimum_momentum_12h_atr": None,
    "maximum_momentum_12h_atr": None,
    "minimum_momentum_24h_atr": None,
    "maximum_momentum_24h_atr": None,

    "included_ny_hours": None,
    "excluded_ny_hours": set(),
    "excluded_weekdays": set(),

    "maximum_prev_day_high_distance_atr":
        None,
    "require_prev_day_high_sweep":
        False,
    "require_prev_day_high_reclaim":
        False,

    "maximum_h1_high20_distance_atr":
        None,
    "maximum_h1_high50_distance_atr":
        None,
    "maximum_h4_high10_distance_atr":
        None,
    "maximum_h4_high20_distance_atr":
        None,

    "reward_risk": 3.00,
}


def signal_passes(signal, config):
    lookback = config[
        "structure_lookback"
    ]

    structure = signal[
        "structure"
    ][lookback]

    if (
        structure["distance_atr"]
        >
        config[
            "maximum_structure_distance_atr"
        ]
    ):
        return False

    if (
        config["require_structure_sweep"]
        and not structure["swept"]
    ):
        return False

    if (
        config["require_structure_reclaim"]
        and not structure["reclaimed"]
    ):
        return False

    for key in [
        "minimum_body_atr",
        "minimum_range_atr",
        "minimum_upper_wick_body",
    ]:
        threshold = config[key]
        if threshold is None:
            continue

        source_key = {
            "minimum_body_atr":
                "body_atr",
            "minimum_range_atr":
                "range_atr",
            "minimum_upper_wick_body":
                "upper_wick_body",
        }[key]

        if signal[source_key] < threshold:
            return False

    if (
        config["maximum_close_location"]
        is not None
        and signal["close_location"]
        >
        config["maximum_close_location"]
    ):
        return False

    for horizon in [
        "4h",
        "12h",
        "24h",
    ]:
        value = signal[
            f"momentum_{horizon}_atr"
        ]

        minimum = config[
            f"minimum_momentum_{horizon}_atr"
        ]
        maximum = config[
            f"maximum_momentum_{horizon}_atr"
        ]

        if (
            minimum is not None
            and value < minimum
        ):
            return False

        if (
            maximum is not None
            and value > maximum
        ):
            return False

    included = config[
        "included_ny_hours"
    ]

    if (
        included is not None
        and signal["ny_hour"] not in included
    ):
        return False

    if (
        signal["ny_hour"]
        in config["excluded_ny_hours"]
    ):
        return False

    if (
        signal["ny_weekday"]
        in config["excluded_weekdays"]
    ):
        return False

    if (
        config[
            "maximum_prev_day_high_distance_atr"
        ] is not None
    ):
        value = signal[
            "prev_day_high_distance_atr"
        ]

        if (
            value is None
            or value
            >
            config[
                "maximum_prev_day_high_distance_atr"
            ]
        ):
            return False

    if (
        config["require_prev_day_high_sweep"]
        and not signal["prev_day_high_sweep"]
    ):
        return False

    if (
        config["require_prev_day_high_reclaim"]
        and not signal[
            "prev_day_high_reclaim"
        ]
    ):
        return False

    for lb in [20, 50]:
        threshold = config[
            f"maximum_h1_high{lb}_distance_atr"
        ]

        if threshold is not None:
            value = signal[
                "h1_high_distance"
            ].get(lb)

            if value is None or value > threshold:
                return False

    for lb in [10, 20]:
        threshold = config[
            f"maximum_h4_high{lb}_distance_atr"
        ]

        if threshold is not None:
            value = signal[
                "h4_high_distance"
            ].get(lb)

            if value is None or value > threshold:
                return False

    return True


# ============================================================
# OUTCOME CACHE
# ============================================================

def compute_trade_outcome(
    candles,
    signal_index,
    reward_risk,
    cost_pips,
):
    signal = candles[signal_index]

    reference_entry = signal["close"]

    stop = (
        signal["high"]
        + STOP_BUFFER_TICKS * TICK_SIZE
    )

    reference_risk = stop - reference_entry

    if reference_risk <= 0:
        return None

    target = (
        reference_entry
        - reward_risk * reference_risk
    )

    backtest_entry = (
        reference_entry
        - cost_pips * PIP_SIZE
    )

    actual_risk = stop - backtest_entry

    if actual_risk <= 0:
        return None

    for j in range(
        signal_index + 1,
        len(candles),
    ):
        candle = candles[j]

        hit_stop = candle["high"] >= stop
        hit_target = candle["low"] <= target

        if hit_stop and hit_target:
            distance_high = abs(
                candle["high"] - candle["open"]
            )
            distance_low = abs(
                candle["open"] - candle["low"]
            )

            if distance_high < distance_low:
                exit_price = stop
                exit_reason = "STOP"
            else:
                exit_price = target
                exit_reason = "TARGET"

        elif hit_stop:
            exit_price = stop
            exit_reason = "STOP"

        elif hit_target:
            exit_price = target
            exit_reason = "TARGET"

        else:
            continue

        result_r = (
            backtest_entry - exit_price
        ) / actual_risk

        return {
            "signal_index": signal_index,
            "exit_index": j,
            "entry_time": signal["time"],
            "exit_time": candle["time"],
            "entry_time_utc":
                iso_utc(signal["time"]),
            "exit_time_utc":
                iso_utc(candle["time"]),
            "reference_entry":
                reference_entry,
            "backtest_entry":
                backtest_entry,
            "stop": stop,
            "target": target,
            "exit_reason": exit_reason,
            "result_r": result_r,
            "reward_risk": reward_risk,
            "cost_pips": cost_pips,
        }

    return None


def build_outcome_cache(
    candles,
    signals,
):
    cache = {}

    total = (
        len(signals)
        * len(RR_VALUES)
        * len(COST_PIPS_GRID)
    )

    done = 0

    for signal in signals:
        index = signal["signal_index"]

        for rr in RR_VALUES:
            for cost in COST_PIPS_GRID:
                done += 1

                if done % 1000 == 0:
                    STATUS.update({
                        "state": "precomputing",
                        "message": (
                            f"Caching outcomes "
                            f"{done}/{total}"
                        ),
                        "outcomes_done": done,
                        "outcomes_total": total,
                    })

                cache[
                    (index, rr, cost)
                ] = compute_trade_outcome(
                    candles,
                    index,
                    rr,
                    cost,
                )

    return cache


# ============================================================
# CONFIG CACHE / BACKTEST
# ============================================================

def config_signature(config):
    return tuple(
        sorted(
            (
                key,
                tuple(sorted(value))
                if isinstance(value, set)
                else value,
            )
            for key, value in config.items()
            if key != "label"
        )
    )


CANDIDATE_CACHE = {}


def qualifying_candidates(
    signals,
    config,
):
    key = config_signature(config)

    if key in CANDIDATE_CACHE:
        return CANDIDATE_CACHE[key]

    candidates = [
        signal
        for signal in signals
        if signal_passes(signal, config)
    ]

    CANDIDATE_CACHE[key] = candidates
    return candidates


def run_config_cached(
    signals,
    outcome_cache,
    config,
    cost_pips,
    start=None,
    end=None,
):
    candidates = qualifying_candidates(
        signals,
        config,
    )

    if start is not None or end is not None:
        times = [
            signal["time"]
            for signal in candidates
        ]

        left = (
            0
            if start is None
            else bisect.bisect_left(
                times, start
            )
        )

        right = (
            len(candidates)
            if end is None
            else bisect.bisect_left(
                times, end
            )
        )

        candidates = candidates[left:right]

    indices = [
        signal["signal_index"]
        for signal in candidates
    ]

    trades = []
    position = 0

    while position < len(candidates):
        signal = candidates[position]

        trade = outcome_cache.get(
            (
                signal["signal_index"],
                config["reward_risk"],
                cost_pips,
            )
        )

        if trade is None:
            position += 1
            continue

        trades.append(dict(trade))

        position = bisect.bisect_left(
            indices,
            trade["exit_index"],
            lo=position + 1,
        )

    return trades


# ============================================================
# STATS
# ============================================================

def stats_from_trades(trades):
    results = [
        float(t["result_r"])
        for t in trades
    ]

    winners = [r for r in results if r > 0]
    losers = [r for r in results if r < 0]

    gross_profit = sum(winners)
    gross_loss = abs(sum(losers))
    total_r = sum(results)

    pf = (
        gross_profit / gross_loss
        if gross_loss > 0
        else (
            999.0 if gross_profit > 0 else 0.0
        )
    )

    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    streak = 0
    longest = 0

    for result in results:
        equity += result
        peak = max(peak, equity)
        max_dd = min(
            max_dd,
            equity - peak,
        )

        if result < 0:
            streak += 1
            longest = max(longest, streak)
        else:
            streak = 0

    return {
        "trades": len(results),
        "winners": len(winners),
        "losers": len(losers),
        "win_rate": (
            len(winners) / len(results) * 100
            if results else 0.0
        ),
        "profit_factor": pf,
        "total_r": total_r,
        "expectancy_r": (
            total_r / len(results)
            if results else 0.0
        ),
        "max_drawdown_r": max_dd,
        "longest_loss_streak": longest,
    }


def result_row(
    family,
    config,
    cost,
    trades,
):
    stats = stats_from_trades(trades)

    row = {
        "family": family,
        "candidate": config["label"],
        "cost_pips": cost,
        "trades": stats["trades"],
        "winners": stats["winners"],
        "losers": stats["losers"],
        "win_rate": round(
            stats["win_rate"], 4
        ),
        "profit_factor": round(
            stats["profit_factor"], 6
        ),
        "total_r": round(
            stats["total_r"], 4
        ),
        "expectancy_r": round(
            stats["expectancy_r"], 6
        ),
        "max_drawdown_r": round(
            stats["max_drawdown_r"], 4
        ),
        "longest_loss_streak":
            stats["longest_loss_streak"],
    }

    for key, value in config.items():
        if key == "label":
            continue

        if isinstance(value, set):
            row[key] = ",".join(
                str(x)
                for x in sorted(value)
            )
        else:
            row[key] = value

    return row


# ============================================================
# CONFIG GENERATION
# ============================================================

def structure_neighbourhood_configs():
    configs = []

    for lookback in STRUCTURE_LOOKBACKS:
        for distance in STRUCTURE_DISTANCES:
            for rr in RR_VALUES:
                c = clone_config(
                    SEED,
                    (
                        f"S{lookback}_"
                        f"D{distance:.3f}_"
                        f"RR{rr:.2f}"
                    ),
                )

                c["structure_lookback"] = lookback
                c[
                    "maximum_structure_distance_atr"
                ] = distance
                c["reward_risk"] = rr

                configs.append(
                    ("STRUCTURE_LOCAL", c)
                )

    return configs


def single_family_configs():
    rows = []

    def add(
        family,
        label,
        mutator,
    ):
        config = clone_config(
            SEED,
            label,
        )
        mutator(config)
        rows.append((family, config))

    # Sweep / reclaim nature.
    add(
        "STRUCTURE_SWEEP",
        "S150_D0.05_REQUIRE_SWEEP",
        lambda c:
            c.__setitem__(
                "require_structure_sweep",
                True,
            ),
    )

    add(
        "STRUCTURE_RECLAIM",
        "S150_D0.05_REQUIRE_RECLAIM",
        lambda c:
            (
                c.__setitem__(
                    "require_structure_sweep",
                    True,
                ),
                c.__setitem__(
                    "require_structure_reclaim",
                    True,
                ),
            ),
    )

    # Candle quality.
    for value in [
        0.50,
        0.75,
        1.00,
        1.25,
    ]:
        add(
            "BODY_ATR",
            f"BODY_ATR_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_body_atr", v
                ),
        )

    for value in [
        1.00,
        1.25,
        1.50,
        1.75,
        2.00,
    ]:
        add(
            "RANGE_ATR",
            f"RANGE_ATR_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_range_atr", v
                ),
        )

    for value in [
        0.15,
        0.20,
        0.25,
        0.30,
        0.35,
    ]:
        add(
            "CLOSE_LOCATION",
            f"CLOSE_LOC_MAX_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "maximum_close_location", v
                ),
        )

    for value in [
        0.05,
        0.10,
        0.20,
        0.30,
        0.50,
    ]:
        add(
            "UPPER_WICK",
            f"UPPER_WICK_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_upper_wick_body", v
                ),
        )

    # Prior upside momentum and overextension.
    for horizon in [
        "4h",
        "12h",
        "24h",
    ]:
        for value in [
            0.25,
            0.50,
            1.00,
            1.50,
            2.00,
        ]:
            key = (
                f"minimum_momentum_"
                f"{horizon}_atr"
            )

            add(
                f"MOM_{horizon}_MIN",
                (
                    f"MOM_{horizon}_"
                    f"MIN_{value:.2f}"
                ),
                lambda c,
                k=key,
                v=value:
                    c.__setitem__(k, v),
            )

        for value in [
            0.50,
            1.00,
            1.50,
            2.00,
            3.00,
        ]:
            key = (
                f"maximum_momentum_"
                f"{horizon}_atr"
            )

            add(
                f"MOM_{horizon}_MAX",
                (
                    f"MOM_{horizon}_"
                    f"MAX_{value:.2f}"
                ),
                lambda c,
                k=key,
                v=value:
                    c.__setitem__(k, v),
            )

    # Session clusters.
    sessions = {
        "NY_00_04":
            {0, 1, 2, 3, 4},
        "NY_02_06":
            {2, 3, 4, 5, 6},
        "NY_07_11":
            {7, 8, 9, 10, 11},
        "NY_08_12":
            {8, 9, 10, 11, 12},
        "NY_12_16":
            {12, 13, 14, 15, 16},
        "NY_14_18":
            {14, 15, 16, 17, 18},
    }

    for label, hours in sessions.items():
        add(
            "SESSION_INCLUDE",
            label,
            lambda c, h=hours:
                c.__setitem__(
                    "included_ny_hours",
                    set(h),
                ),
        )

    # Individual hour exclusions.
    for hour in range(24):
        add(
            "HOUR_EXCLUDE",
            f"EX_H{hour:02d}",
            lambda c, h=hour:
                c.__setitem__(
                    "excluded_ny_hours",
                    {h},
                ),
        )

    for day, name in [
        (0, "MON"),
        (1, "TUE"),
        (2, "WED"),
        (3, "THU"),
        (4, "FRI"),
    ]:
        add(
            "WEEKDAY_EXCLUDE",
            f"EX_{name}",
            lambda c, d=day:
                c.__setitem__(
                    "excluded_weekdays",
                    {d},
                ),
        )

    # Previous-day high.
    for value in [
        0.25,
        0.50,
        0.75,
        1.00,
    ]:
        add(
            "PREV_DAY_HIGH_DISTANCE",
            f"PREV_DAY_HIGH_DIST_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "maximum_prev_day_high_distance_atr",
                    v,
                ),
        )

    add(
        "PREV_DAY_HIGH_SWEEP",
        "PREV_DAY_HIGH_SWEEP",
        lambda c:
            c.__setitem__(
                "require_prev_day_high_sweep",
                True,
            ),
    )

    add(
        "PREV_DAY_HIGH_RECLAIM",
        "PREV_DAY_HIGH_RECLAIM",
        lambda c:
            (
                c.__setitem__(
                    "require_prev_day_high_sweep",
                    True,
                ),
                c.__setitem__(
                    "require_prev_day_high_reclaim",
                    True,
                ),
            ),
    )

    # H1/H4 structural highs.
    for lb in [20, 50]:
        for value in [
            0.25,
            0.50,
            0.75,
            1.00,
        ]:
            key = (
                f"maximum_h1_high"
                f"{lb}_distance_atr"
            )

            add(
                "H1_HIGH_DISTANCE",
                (
                    f"H1_HIGH{lb}_"
                    f"DIST_{value:.2f}"
                ),
                lambda c,
                k=key,
                v=value:
                    c.__setitem__(k, v),
            )

    for lb in [10, 20]:
        for value in [
            0.25,
            0.50,
            0.75,
            1.00,
        ]:
            key = (
                f"maximum_h4_high"
                f"{lb}_distance_atr"
            )

            add(
                "H4_HIGH_DISTANCE",
                (
                    f"H4_HIGH{lb}_"
                    f"DIST_{value:.2f}"
                ),
                lambda c,
                k=key,
                v=value:
                    c.__setitem__(k, v),
            )

    # RR around seed.
    for rr in RR_VALUES:
        add(
            "RR",
            f"RR_{rr:.2f}",
            lambda c, v=rr:
                c.__setitem__(
                    "reward_risk", v
                ),
        )

    return rows


def build_controlled_interactions(
    primary_rows,
    config_lookup,
):
    # Best config from each DISTINCT family,
    # but only if it improves meaningfully over the seed.
    family_best = {}

    for row in primary_rows:
        family = row["family"]

        if family in family_best:
            continue

        if (
            float(row["profit_factor"]) >= 1.08
            and int(row["trades"]) >= 45
        ):
            family_best[family] = row

    ranked = sorted(
        family_best.items(),
        key=lambda item:
            (
                float(
                    item[1]["profit_factor"]
                ),
                float(
                    item[1]["expectancy_r"]
                ),
            ),
        reverse=True,
    )[:10]

    pool = [
        (
            family,
            config_lookup[
                row["candidate"]
            ],
        )
        for family, row in ranked
    ]

    def overlay(base, source):
        result = clone_config(
            base,
            base["label"],
        )

        for key, value in source.items():
            if key == "label":
                continue

            seed_value = SEED.get(key)

            if value != seed_value:
                if isinstance(value, set):
                    result[key] = set(value)
                else:
                    result[key] = value

        return result

    interactions = []
    seen = set()
    counter = 0

    for i in range(len(pool)):
        for j in range(i + 1, len(pool)):
            family_a, config_a = pool[i]
            family_b, config_b = pool[j]

            config = clone_config(
                SEED,
                "TEMP",
            )

            config = overlay(
                config,
                config_a,
            )
            config = overlay(
                config,
                config_b,
            )

            signature = config_signature(config)

            if signature in seen:
                continue

            seen.add(signature)
            counter += 1

            config["label"] = (
                f"INT{counter:03d}_"
                f"{family_a}_"
                f"{family_b}"
            )

            interactions.append(
                (
                    (
                        f"INTERACTION_"
                        f"{family_a}_"
                        f"{family_b}"
                    ),
                    config,
                )
            )

    return interactions


# ============================================================
# VALIDATION
# ============================================================

def era_windows():
    return [
        (
            "ERA_2010_2013",
            datetime(
                2010, 1, 1,
                tzinfo=timezone.utc,
            ),
            datetime(
                2014, 1, 1,
                tzinfo=timezone.utc,
            ),
        ),
        (
            "ERA_2014_2017",
            datetime(
                2014, 1, 1,
                tzinfo=timezone.utc,
            ),
            datetime(
                2018, 1, 1,
                tzinfo=timezone.utc,
            ),
        ),
        (
            "ERA_2018_2021",
            datetime(
                2018, 1, 1,
                tzinfo=timezone.utc,
            ),
            datetime(
                2022, 1, 1,
                tzinfo=timezone.utc,
            ),
        ),
        (
            "ERA_2022_NOW",
            datetime(
                2022, 1, 1,
                tzinfo=timezone.utc,
            ),
            RESEARCH_TO,
        ),
    ]


def devval_windows():
    return [
        (
            "DEV_2010_2017",
            datetime(
                2010, 1, 1,
                tzinfo=timezone.utc,
            ),
            datetime(
                2018, 1, 1,
                tzinfo=timezone.utc,
            ),
        ),
        (
            "VALIDATION_2018_NOW",
            datetime(
                2018, 1, 1,
                tzinfo=timezone.utc,
            ),
            RESEARCH_TO,
        ),
    ]


def recent_windows():
    return [
        (
            "LAST_5Y",
            years_ago_safe(
                RESEARCH_TO, 5
            ),
            RESEARCH_TO,
        ),
        (
            "LAST_2Y",
            years_ago_safe(
                RESEARCH_TO, 2
            ),
            RESEARCH_TO,
        ),
    ]


def validation_rows(
    signals,
    cache,
    configs,
    windows,
):
    rows = []

    for rank, config in enumerate(
        configs,
        start=1,
    ):
        for label, start, end in windows:
            trades = run_config_cached(
                signals,
                cache,
                config,
                PRIMARY_COST_PIPS,
                start=start,
                end=end,
            )

            stats = stats_from_trades(trades)

            rows.append({
                "rank": rank,
                "window": label,
                "candidate":
                    config["label"],
                "trades":
                    stats["trades"],
                "profit_factor":
                    round(
                        stats[
                            "profit_factor"
                        ], 6
                    ),
                "total_r":
                    round(
                        stats["total_r"], 4
                    ),
                "expectancy_r":
                    round(
                        stats[
                            "expectancy_r"
                        ], 6
                    ),
                "max_drawdown_r":
                    round(
                        stats[
                            "max_drawdown_r"
                        ], 4
                    ),
                "longest_loss_streak":
                    stats[
                        "longest_loss_streak"
                    ],
            })

    return rows


# ============================================================
# ROLLING
# ============================================================

def monthly_rolling_rows(
    signals,
    cache,
    config,
    months,
):
    rows = []

    cursor = month_start(RESEARCH_FROM)

    last_start = add_months(
        month_start(RESEARCH_TO),
        -months,
    )

    while cursor <= last_start:
        end = add_months(cursor, months)

        if end > RESEARCH_TO:
            break

        trades = run_config_cached(
            signals,
            cache,
            config,
            PRIMARY_COST_PIPS,
            start=cursor,
            end=end,
        )

        stats = stats_from_trades(trades)

        rows.append({
            "candidate":
                config["label"],
            "months": months,
            "window": (
                f"{cursor:%Y-%m-%d}"
                f" -> {end:%Y-%m-%d}"
            ),
            "trades":
                stats["trades"],
            "profit_factor":
                round(
                    stats[
                        "profit_factor"
                    ], 6
                ),
            "total_r":
                round(
                    stats["total_r"], 4
                ),
            "expectancy_r":
                round(
                    stats[
                        "expectancy_r"
                    ], 6
                ),
            "max_drawdown_r":
                round(
                    stats[
                        "max_drawdown_r"
                    ], 4
                ),
            "positive":
                stats["total_r"] > 0,
        })

        cursor = add_months(cursor, 1)

    return rows


def median(values):
    values = sorted(values)
    n = len(values)

    if n == 0:
        return None

    if n % 2:
        return values[n // 2]

    return (
        values[n // 2 - 1]
        + values[n // 2]
    ) / 2


def rolling_summary(rows):
    if not rows:
        return {}

    pfs = [
        float(row["profit_factor"])
        for row in rows
    ]

    rs = [
        float(row["total_r"])
        for row in rows
    ]

    positive = sum(
        1 for row in rows
        if row["positive"]
    )

    worst_pf = min(
        rows,
        key=lambda row:
            float(row["profit_factor"]),
    )

    worst_r = min(
        rows,
        key=lambda row:
            float(row["total_r"]),
    )

    return {
        "candidate":
            rows[0]["candidate"],
        "months":
            rows[0]["months"],
        "windows":
            len(rows),
        "positive_windows":
            positive,
        "positive_windows_pct":
            round(
                positive / len(rows) * 100,
                4,
            ),
        "worst_profit_factor":
            round(min(pfs), 6),
        "median_profit_factor":
            round(median(pfs), 6),
        "worst_total_r":
            round(min(rs), 4),
        "median_total_r":
            round(median(rs), 4),
        "worst_pf_window":
            worst_pf["window"],
        "worst_r_window":
            worst_r["window"],
    }


# ============================================================
# OVERLAP
# ============================================================

def trade_key(trade):
    return (
        trade["entry_time_utc"],
        trade["exit_time_utc"],
    )


def overlap_rows(
    signals,
    cache,
    reference,
    finalist,
):
    reference_trades = run_config_cached(
        signals,
        cache,
        reference,
        PRIMARY_COST_PIPS,
    )

    finalist_trades = run_config_cached(
        signals,
        cache,
        finalist,
        PRIMARY_COST_PIPS,
    )

    reference_keys = {
        trade_key(t)
        for t in reference_trades
    }

    finalist_keys = {
        trade_key(t)
        for t in finalist_trades
    }

    shared = reference_keys & finalist_keys
    added = finalist_keys - reference_keys
    removed = reference_keys - finalist_keys

    groups = [
        (
            "REFERENCE_ALL",
            reference_trades,
        ),
        (
            "FINALIST_ALL",
            finalist_trades,
        ),
        (
            "FINALIST_SHARED",
            [
                t for t in finalist_trades
                if trade_key(t) in shared
            ],
        ),
        (
            "FINALIST_ADDED",
            [
                t for t in finalist_trades
                if trade_key(t) in added
            ],
        ),
        (
            "REFERENCE_REMOVED",
            [
                t for t in reference_trades
                if trade_key(t) in removed
            ],
        ),
    ]

    rows = []

    for name, trades in groups:
        stats = stats_from_trades(trades)

        rows.append({
            "reference":
                reference["label"],
            "finalist":
                finalist["label"],
            "subset": name,
            "trades":
                stats["trades"],
            "profit_factor":
                round(
                    stats[
                        "profit_factor"
                    ], 6
                ),
            "total_r":
                round(
                    stats["total_r"], 4
                ),
            "expectancy_r":
                round(
                    stats[
                        "expectancy_r"
                    ], 6
                ),
            "max_drawdown_r":
                round(
                    stats[
                        "max_drawdown_r"
                    ], 4
                ),
        })

    return rows


# ============================================================
# RUNNER
# ============================================================

def run_research():
    try:
        m15 = fetch_history(
            "M15",
            RESEARCH_FROM,
            RESEARCH_TO,
            30,
        )

        h1 = fetch_history(
            "H1",
            RESEARCH_FROM
            - timedelta(days=500),
            RESEARCH_TO,
            120,
        )

        h4 = fetch_history(
            "H4",
            RESEARCH_FROM
            - timedelta(days=1000),
            RESEARCH_TO,
            500,
        )

        daily = fetch_history(
            "D",
            RESEARCH_FROM
            - timedelta(days=1000),
            RESEARCH_TO,
            2500,
            daily_alignment=True,
        )

        STATUS.update({
            "state": "precomputing",
            "message":
                "Building corrected Gen2 features",
            "m15_candles": len(m15),
            "h1_candles": len(h1),
            "h4_candles": len(h4),
            "daily_candles": len(daily),
        })

        m15_atr = atr14(m15)

        h1_state = build_htf_state(
            h1,
            [20, 50],
        )

        h4_state = build_htf_state(
            h4,
            [10, 20],
        )

        daily_state = build_htf_state(
            daily,
            [],
        )

        signals = build_signal_cache(
            m15,
            m15_atr,
            h1_state,
            h4_state,
            daily_state,
        )

        STATUS.update({
            "state": "precomputing",
            "message":
                "Caching reusable trade outcomes",
            "engulfing_signals":
                len(signals),
        })

        outcome_cache = build_outcome_cache(
            m15,
            signals,
        )

        # ----------------------------------------------
        # 1. Fine structure + RR neighbourhood
        # ----------------------------------------------

        neighbourhood = (
            structure_neighbourhood_configs()
        )

        neighbourhood_rows = []

        STATUS.update({
            "state": "calculating",
            "message":
                "Running fine structure/RR neighbourhood",
            "configs":
                len(neighbourhood),
        })

        for number, (
            family,
            config,
        ) in enumerate(
            neighbourhood,
            start=1,
        ):
            for cost in COST_PIPS_GRID:
                trades = run_config_cached(
                    signals,
                    outcome_cache,
                    config,
                    cost,
                )

                neighbourhood_rows.append(
                    result_row(
                        family,
                        config,
                        cost,
                        trades,
                    )
                )

            if number % 25 == 0:
                STATUS["message"] = (
                    "Structure/RR neighbourhood "
                    f"{number}/{len(neighbourhood)}"
                )

        write_csv(
            OUTPUT_NEIGHBOURHOOD,
            neighbourhood_rows,
        )

        # ----------------------------------------------
        # 2. Single families around exact seed
        # ----------------------------------------------

        singles = single_family_configs()
        single_rows = []

        STATUS.update({
            "state": "calculating",
            "message":
                "Running targeted single-family tests",
            "single_configs":
                len(singles),
        })

        for number, (
            family,
            config,
        ) in enumerate(
            singles,
            start=1,
        ):
            for cost in COST_PIPS_GRID:
                trades = run_config_cached(
                    signals,
                    outcome_cache,
                    config,
                    cost,
                )

                single_rows.append(
                    result_row(
                        family,
                        config,
                        cost,
                        trades,
                    )
                )

            if number % 20 == 0:
                STATUS["message"] = (
                    "Single-family tests "
                    f"{number}/{len(singles)}"
                )

        write_csv(
            OUTPUT_SINGLE,
            single_rows,
        )

        # ----------------------------------------------
        # Primary pool
        # ----------------------------------------------

        primary_rows = [
            row
            for row in (
                neighbourhood_rows
                + single_rows
            )
            if (
                abs(
                    float(row["cost_pips"])
                    - PRIMARY_COST_PIPS
                ) < 1e-12
                and int(row["trades"])
                >= MIN_TRADES_PRIMARY
            )
        ]

        primary_rows.sort(
            key=lambda row: (
                float(row["profit_factor"]),
                float(row["expectancy_r"]),
                float(row["total_r"]),
            ),
            reverse=True,
        )

        config_lookup = {}

        for _, config in (
            neighbourhood + singles
        ):
            config_lookup[
                config["label"]
            ] = config

        # Seed explicit.
        config_lookup[
            SEED["label"]
        ] = SEED

        # ----------------------------------------------
        # 3. Controlled interactions
        # ----------------------------------------------

        interactions = (
            build_controlled_interactions(
                primary_rows,
                config_lookup,
            )
        )

        interaction_rows = []

        STATUS.update({
            "state": "calculating",
            "message":
                "Running controlled interactions",
            "interaction_configs":
                len(interactions),
        })

        for number, (
            family,
            config,
        ) in enumerate(
            interactions,
            start=1,
        ):
            for cost in COST_PIPS_GRID:
                trades = run_config_cached(
                    signals,
                    outcome_cache,
                    config,
                    cost,
                )

                interaction_rows.append(
                    result_row(
                        family,
                        config,
                        cost,
                        trades,
                    )
                )

            STATUS["message"] = (
                "Controlled interactions "
                f"{number}/{len(interactions)}"
            )

        write_csv(
            OUTPUT_INTERACTIONS,
            interaction_rows,
        )

        for _, config in interactions:
            config_lookup[
                config["label"]
            ] = config

        all_primary = list(primary_rows)

        all_primary.extend(
            row
            for row in interaction_rows
            if (
                abs(
                    float(row["cost_pips"])
                    - PRIMARY_COST_PIPS
                ) < 1e-12
                and int(row["trades"])
                >= MIN_TRADES_PRIMARY
            )
        )

        # Add seed.
        seed_trades = run_config_cached(
            signals,
            outcome_cache,
            SEED,
            PRIMARY_COST_PIPS,
        )

        all_primary.append(
            result_row(
                "SEED_REFERENCE",
                SEED,
                PRIMARY_COST_PIPS,
                seed_trades,
            )
        )

        all_primary.sort(
            key=lambda row: (
                float(row["profit_factor"]),
                float(row["expectancy_r"]),
                float(row["total_r"]),
            ),
            reverse=True,
        )

        top_rows = all_primary[:40]

        write_csv(
            OUTPUT_TOP,
            top_rows,
        )

        finalists = [
            config_lookup[
                row["candidate"]
            ]
            for row in top_rows[:15]
        ]

        # ----------------------------------------------
        # 4. Validation
        # ----------------------------------------------

        STATUS.update({
            "state": "validating",
            "message": "Running 4 eras",
        })

        era_rows = validation_rows(
            signals,
            outcome_cache,
            finalists,
            era_windows(),
        )

        STATUS["message"] = (
            "Running dev / validation"
        )

        devval_rows = validation_rows(
            signals,
            outcome_cache,
            finalists,
            devval_windows(),
        )

        STATUS["message"] = (
            "Running recent 5Y / 2Y"
        )

        recent_rows = validation_rows(
            signals,
            outcome_cache,
            finalists,
            recent_windows(),
        )

        write_csv(
            OUTPUT_ERAS,
            era_rows,
        )
        write_csv(
            OUTPUT_DEVVAL,
            devval_rows,
        )
        write_csv(
            OUTPUT_RECENT,
            recent_rows,
        )

        # Robust ranking.
        robust = []

        for config in finalists:
            label = config["label"]

            base = next(
                row for row in top_rows
                if row["candidate"] == label
            )

            eras = [
                row for row in era_rows
                if row["candidate"] == label
                and int(row["trades"]) > 0
            ]

            devval = [
                row for row in devval_rows
                if row["candidate"] == label
                and int(row["trades"]) > 0
            ]

            recent = [
                row for row in recent_rows
                if row["candidate"] == label
                and int(row["trades"]) > 0
            ]

            era_pfs = [
                float(r["profit_factor"])
                for r in eras
            ]

            dev_pfs = [
                float(r["profit_factor"])
                for r in devval
            ]

            recent_pfs = [
                float(r["profit_factor"])
                for r in recent
            ]

            min_era = (
                min(era_pfs)
                if era_pfs else 0.0
            )
            min_dev = (
                min(dev_pfs)
                if dev_pfs else 0.0
            )
            min_recent = (
                min(recent_pfs)
                if recent_pfs else 0.0
            )

            robust.append({
                "candidate": label,
                "trades":
                    int(base["trades"]),
                "full_pf":
                    float(
                        base["profit_factor"]
                    ),
                "full_total_r":
                    float(base["total_r"]),
                "full_expectancy":
                    float(
                        base["expectancy_r"]
                    ),
                "minimum_era_pf":
                    min_era,
                "minimum_devval_pf":
                    min_dev,
                "minimum_recent_pf":
                    min_recent,
                "score": (
                    min_era * 3.0
                    + min_dev * 2.0
                    + min_recent * 2.0
                    + float(
                        base["profit_factor"]
                    )
                ),
            })

        robust.sort(
            key=lambda row: row["score"],
            reverse=True,
        )

        robust_finalists = [
            config_lookup[
                row["candidate"]
            ]
            for row in robust[:6]
        ]

        # ----------------------------------------------
        # 5. Rolling
        # ----------------------------------------------

        STATUS["message"] = (
            "Running rolling 2Y / 3Y"
        )

        rolling_rows = []
        rolling_summary_rows = []

        for config in robust_finalists:
            for months in [24, 36]:
                rows = monthly_rolling_rows(
                    signals,
                    outcome_cache,
                    config,
                    months,
                )

                rolling_rows.extend(rows)
                rolling_summary_rows.append(
                    rolling_summary(rows)
                )

        write_csv(
            OUTPUT_ROLLING,
            rolling_rows,
        )

        write_csv(
            OUTPUT_ROLLING_SUMMARY,
            rolling_summary_rows,
        )

        # ----------------------------------------------
        # 6. Overlap against seed
        # ----------------------------------------------

        STATUS["message"] = (
            "Running overlap vs original seed"
        )

        overlap = []

        for config in robust_finalists:
            overlap.extend(
                overlap_rows(
                    signals,
                    outcome_cache,
                    SEED,
                    config,
                )
            )

        write_csv(
            OUTPUT_OVERLAP,
            overlap,
        )

        best = (
            robust_finalists[0]
            if robust_finalists
            else SEED
        )

        best_trades = run_config_cached(
            signals,
            outcome_cache,
            best,
            PRIMARY_COST_PIPS,
        )

        write_csv(
            OUTPUT_BEST_TRADES,
            best_trades,
        )

        STATUS.update({
            "state": "packaging",
            "message":
                "Building single ZIP results bundle",
        })

        build_results_bundle()

        STATUS.update({
            "state": "complete",
            "message":
                "GBP/USD M15 short Gen2 complete",
            "engulfing_signals":
                len(signals),
            "structure_configs":
                len(neighbourhood),
            "single_configs":
                len(singles),
            "interaction_configs":
                len(interactions),
            "seed_stats":
                stats_from_trades(
                    seed_trades
                ),
            "robust_ranking":
                robust[:12],
            "selected_best":
                best,
            "results_bundle":
                OUTPUT_BUNDLE,
        })

    except Exception as error:
        STATUS.update({
            "state": "error",
            "message": str(error),
        })

        print(
            "ERROR:",
            error,
            flush=True,
        )


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def root():
    return jsonify({
        "service":
            "GBPUSD M15 Short Gen2 Targeted Structural",
        "status":
            STATUS["state"],
        "instrument":
            INSTRUMENT,
        "timeframe":
            "M15",
        "side":
            "SELL",
        "orders_supported":
            False,
        "trading_enabled":
            False,
        "routes": [
            "/gbpusd-m15-short-gen2/status",
            "/gbpusd-m15-short-gen2/results",
        ],
    })


@app.route(
    "/gbpusd-m15-short-gen2/status"
)
def route_status():
    return jsonify(STATUS)


@app.route(
    "/gbpusd-m15-short-gen2/results"
)
def route_results():
    return download_file(
        OUTPUT_BUNDLE
    )


if __name__ == "__main__":
    research_thread = threading.Thread(
        target=run_research,
        name="gbpusd-m15-short-gen2",
        daemon=True,
    )

    research_thread.start()

    port = int(
        os.getenv("PORT", 5000)
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
    )
