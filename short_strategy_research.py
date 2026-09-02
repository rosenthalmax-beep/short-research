import os
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# EUR/GBP SHORT - TIMING x FREQUENCY RECOVERY MATRIX
#
# RESEARCH ONLY — NEVER SUBMITS ORDERS.
#
# Purpose:
#   Take the strongest toxic timing finding (NY hour 09)
#   and test whether frequency can be recovered by relaxing
#   ONE existing filter at a time.
#
# Also includes NY hour 11 as a SECONDARY timing comparison
# for the ROBUST branch only.
#
# IMPORTANT:
#   - Timing exclusion is applied first.
#   - Only ONE strategy filter is relaxed at a time.
#   - No multi-parameter stacking in this stage.
#   - RR remains fixed at 3.00.
#
# Shared frozen execution:
#   OANDA EUR_GBP
#   H1
#   bearish engulfing
#   body ratio >= 1.00
#   stop = signal high + 10 ticks
#   adverse short slippage = 5 ticks
#   pyramiding = 0
#
# Shared frozen geometry baseline:
#   structure lookback = 90
#   max distance = 0.075 ATR14
#   min range = 1.10 ATR14
#   max close location = 0.20
#
# ROBUST branch baseline:
#   12h upward momentum >= 0.25 ATR14
#   48h upward momentum >= 0.50 ATR14
#   stop size <= 2.50 ATR14
#
# HIGH_PF branch baseline:
#   48h upward momentum >= 1.00 ATR14
#   upper wick/body >= 0.10
#   ATR14 / 50-bar ATR14 mean >= 0.80
#
# TIMING PROFILES:
#   ROBUST:
#       exclude NY09
#       exclude NY11
#
#   HIGH_PF:
#       exclude NY09
#
# RELAXATIONS TESTED ONE AT A TIME
#
# Shared:
#   max distance:
#       0.075 baseline
#       0.10
#       0.125
#       0.15
#
#   min range:
#       1.10 baseline
#       1.05
#       1.00
#       0.95
#
#   max close location:
#       0.20 baseline
#       0.225
#       0.25
#       0.275
#
# ROBUST only:
#   12h momentum:
#       0.25 baseline
#       0.20
#       0.15
#       0.10
#
#   48h momentum:
#       0.50 baseline
#       0.40
#       0.30
#       0.20
#
#   stop cap:
#       2.50 baseline
#       2.75
#       3.00
#       None
#
# HIGH_PF only:
#   48h momentum:
#       1.00 baseline
#       0.90
#       0.80
#       0.70
#
#   upper wick/body:
#       0.10 baseline
#       0.075
#       0.05
#       None
#
#   ATR regime:
#       0.80 baseline
#       0.75
#       0.70
#       None
#
# Output:
#   eurgbp_short_timing_frequency_recovery_matrix.csv
# ============================================================


app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_URL = "https://api-fxtrade.oanda.com"

INSTRUMENT = "EUR_GBP"
TICK_SIZE = 0.00001

STOP_BUFFER_TICKS = 10
BACKTEST_SLIPPAGE_TICKS = 5
REWARD_RISK = 3.00

MIN_BODY_RATIO = 1.00
STRUCTURE_LOOKBACK = 90

BASE_MAX_DISTANCE_ATR = 0.075
BASE_MIN_RANGE_ATR = 1.10
BASE_MAX_CLOSE_LOCATION = 0.20

NY_TZ = ZoneInfo("America/New_York")

H1_CHUNK_DAYS = 180

RESEARCH_FROM = datetime(
    2002, 5, 6, 20, 0,
    tzinfo=timezone.utc,
)

RESEARCH_TO = (
    datetime.now(timezone.utc)
    .replace(
        minute=0,
        second=0,
        microsecond=0,
    )
)

H1_WARMUP_DAYS = 700

OUTPUT_FILE = (
    "eurgbp_short_timing_frequency_recovery_matrix.csv"
)


# ============================================================
# BASE BRANCHES
# ============================================================

BRANCHES = {
    "ROBUST": {
        "branch": "ROBUST",
        "min_momentum_12": 0.25,
        "min_momentum_48": 0.50,
        "min_upper_wick_body": None,
        "max_stop_size_atr": 2.50,
        "min_atr_ratio_50": None,
    },
    "HIGH_PF": {
        "branch": "HIGH_PF",
        "min_momentum_12": None,
        "min_momentum_48": 1.00,
        "min_upper_wick_body": 0.10,
        "max_stop_size_atr": None,
        "min_atr_ratio_50": 0.80,
    },
}


# ============================================================
# TIMING PROFILES
# ============================================================

TIMING_PROFILES = [
    {
        "branch": "ROBUST",
        "timing_label": "exclude_ny09",
        "excluded_hours": {9},
    },
    {
        "branch": "ROBUST",
        "timing_label": "exclude_ny11",
        "excluded_hours": {11},
    },
    {
        "branch": "HIGH_PF",
        "timing_label": "exclude_ny09",
        "excluded_hours": {9},
    },
]


# ============================================================
# RELAXATION PROFILES
# ============================================================

def make_shared_relaxations():
    rows = [
        {
            "relax_family": "BASELINE",
            "relax_label": "baseline_after_timing",
            "max_distance_atr": BASE_MAX_DISTANCE_ATR,
            "min_range_atr": BASE_MIN_RANGE_ATR,
            "max_close_location": BASE_MAX_CLOSE_LOCATION,
            "min_momentum_12": None,
            "min_momentum_48": None,
            "max_stop_size_atr": None,
            "min_upper_wick_body": None,
            "min_atr_ratio_50": None,
        }
    ]

    for value in [0.10, 0.125, 0.15]:
        rows.append({
            "relax_family": "STRUCTURE_DISTANCE",
            "relax_label": f"max_distance_{value}",
            "max_distance_atr": value,
            "min_range_atr": BASE_MIN_RANGE_ATR,
            "max_close_location": BASE_MAX_CLOSE_LOCATION,
            "min_momentum_12": None,
            "min_momentum_48": None,
            "max_stop_size_atr": None,
            "min_upper_wick_body": None,
            "min_atr_ratio_50": None,
        })

    for value in [1.05, 1.00, 0.95]:
        rows.append({
            "relax_family": "RANGE",
            "relax_label": f"min_range_{value}",
            "max_distance_atr": BASE_MAX_DISTANCE_ATR,
            "min_range_atr": value,
            "max_close_location": BASE_MAX_CLOSE_LOCATION,
            "min_momentum_12": None,
            "min_momentum_48": None,
            "max_stop_size_atr": None,
            "min_upper_wick_body": None,
            "min_atr_ratio_50": None,
        })

    for value in [0.225, 0.25, 0.275]:
        rows.append({
            "relax_family": "CLOSE_LOCATION",
            "relax_label": f"max_close_{value}",
            "max_distance_atr": BASE_MAX_DISTANCE_ATR,
            "min_range_atr": BASE_MIN_RANGE_ATR,
            "max_close_location": value,
            "min_momentum_12": None,
            "min_momentum_48": None,
            "max_stop_size_atr": None,
            "min_upper_wick_body": None,
            "min_atr_ratio_50": None,
        })

    return rows


def make_robust_relaxations():
    rows = make_shared_relaxations()

    for value in [0.20, 0.15, 0.10]:
        rows.append({
            "relax_family": "MOMENTUM_12H",
            "relax_label": f"min_mom12_{value}",
            "max_distance_atr": BASE_MAX_DISTANCE_ATR,
            "min_range_atr": BASE_MIN_RANGE_ATR,
            "max_close_location": BASE_MAX_CLOSE_LOCATION,
            "min_momentum_12": value,
            "min_momentum_48": None,
            "max_stop_size_atr": None,
            "min_upper_wick_body": None,
            "min_atr_ratio_50": None,
        })

    for value in [0.40, 0.30, 0.20]:
        rows.append({
            "relax_family": "MOMENTUM_48H",
            "relax_label": f"min_mom48_{value}",
            "max_distance_atr": BASE_MAX_DISTANCE_ATR,
            "min_range_atr": BASE_MIN_RANGE_ATR,
            "max_close_location": BASE_MAX_CLOSE_LOCATION,
            "min_momentum_12": None,
            "min_momentum_48": value,
            "max_stop_size_atr": None,
            "min_upper_wick_body": None,
            "min_atr_ratio_50": None,
        })

    for value in [2.75, 3.00, None]:
        label = "none" if value is None else str(value)
        rows.append({
            "relax_family": "STOP_CAP",
            "relax_label": f"max_stop_{label}",
            "max_distance_atr": BASE_MAX_DISTANCE_ATR,
            "min_range_atr": BASE_MIN_RANGE_ATR,
            "max_close_location": BASE_MAX_CLOSE_LOCATION,
            "min_momentum_12": None,
            "min_momentum_48": None,
            "max_stop_size_atr": value,
            "min_upper_wick_body": None,
            "min_atr_ratio_50": None,
        })

    return rows


def make_high_pf_relaxations():
    rows = make_shared_relaxations()

    for value in [0.90, 0.80, 0.70]:
        rows.append({
            "relax_family": "MOMENTUM_48H",
            "relax_label": f"min_mom48_{value}",
            "max_distance_atr": BASE_MAX_DISTANCE_ATR,
            "min_range_atr": BASE_MIN_RANGE_ATR,
            "max_close_location": BASE_MAX_CLOSE_LOCATION,
            "min_momentum_12": None,
            "min_momentum_48": value,
            "max_stop_size_atr": None,
            "min_upper_wick_body": None,
            "min_atr_ratio_50": None,
        })

    for value in [0.075, 0.05, None]:
        label = "none" if value is None else str(value)
        rows.append({
            "relax_family": "UPPER_WICK",
            "relax_label": f"min_upper_wick_{label}",
            "max_distance_atr": BASE_MAX_DISTANCE_ATR,
            "min_range_atr": BASE_MIN_RANGE_ATR,
            "max_close_location": BASE_MAX_CLOSE_LOCATION,
            "min_momentum_12": None,
            "min_momentum_48": None,
            "max_stop_size_atr": None,
            "min_upper_wick_body": value,
            "min_atr_ratio_50": None,
        })

    for value in [0.75, 0.70, None]:
        label = "none" if value is None else str(value)
        rows.append({
            "relax_family": "ATR_REGIME",
            "relax_label": f"min_atr_ratio_{label}",
            "max_distance_atr": BASE_MAX_DISTANCE_ATR,
            "min_range_atr": BASE_MIN_RANGE_ATR,
            "max_close_location": BASE_MAX_CLOSE_LOCATION,
            "min_momentum_12": None,
            "min_momentum_48": None,
            "max_stop_size_atr": None,
            "min_upper_wick_body": None,
            "min_atr_ratio_50": value,
        })

    return rows


RELAXATIONS = {
    "ROBUST": make_robust_relaxations(),
    "HIGH_PF": make_high_pf_relaxations(),
}


# ============================================================
# ERAS
# ============================================================

ERAS = [
    (
        "2002_2009",
        datetime(2002, 5, 6, 20, 0, tzinfo=timezone.utc),
        datetime(2010, 1, 1, 0, 0, tzinfo=timezone.utc),
    ),
    (
        "2010_2017",
        datetime(2010, 1, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2018, 1, 1, 0, 0, tzinfo=timezone.utc),
    ),
    (
        "2018_2023",
        datetime(2018, 1, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
    ),
    (
        "2024_present",
        datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
        None,
    ),
]


# ============================================================
# STATUS
# ============================================================

TOTAL_TESTS = sum(
    len(RELAXATIONS[
        profile["branch"]
    ])
    for profile in TIMING_PROFILES
)

STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "service": "EURGBP Short Timing Frequency Recovery",
    "instrument": INSTRUMENT,
    "research_from": RESEARCH_FROM.isoformat(),
    "research_to": RESEARCH_TO.isoformat(),
    "reward_risk": REWARD_RISK,
    "total_tests": TOTAL_TESTS,
    "completed_tests": 0,
    "rows_saved": 0,
    "output_file": None,
}


# ============================================================
# OANDA
# ============================================================

def headers():
    if not OANDA_TOKEN:
        raise RuntimeError(
            "OANDA_TOKEN is not configured"
        )

    return {
        "Authorization": f"Bearer {OANDA_TOKEN}"
    }


def iso_utc(dt):
    return (
        dt.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def oanda_get(path, params):
    response = requests.get(
        OANDA_URL + path,
        headers=headers(),
        params=params,
        timeout=30,
    )

    if not response.ok:
        raise RuntimeError(
            f"OANDA {response.status_code}: "
            f"{response.text[:500]}"
        )

    return response.json()


def parse_candle(raw):
    if not raw.get("complete", False):
        return None

    mid = raw.get("mid")

    if not mid:
        return None

    return {
        "time": datetime.fromisoformat(
            raw["time"].replace("Z", "+00:00")
        ),
        "open": float(mid["o"]),
        "high": float(mid["h"]),
        "low": float(mid["l"]),
        "close": float(mid["c"]),
    }


def fetch_range(
    instrument,
    granularity,
    start,
    end,
):
    params = {
        "price": "M",
        "granularity": granularity,
        "from": iso_utc(start),
        "to": iso_utc(end),
        "smooth": "false",
        "includeFirst": "true",
    }

    data = oanda_get(
        f"/v3/instruments/{instrument}/candles",
        params,
    )

    candles = []

    for raw in data.get("candles", []):
        candle = parse_candle(raw)

        if candle is not None:
            candles.append(candle)

    return candles


def fetch_chunked_history(
    instrument,
    granularity,
    start,
    end,
):
    candles_by_time = {}
    cursor = start

    while cursor < end:
        chunk_end = min(
            cursor + timedelta(days=H1_CHUNK_DAYS),
            end,
        )

        print(
            f"Fetching {granularity}: "
            f"{cursor.date()} -> {chunk_end.date()}",
            flush=True,
        )

        chunk = fetch_range(
            instrument,
            granularity,
            cursor,
            chunk_end,
        )

        for candle in chunk:
            candles_by_time[
                candle["time"]
            ] = candle

        cursor = chunk_end

    candles = list(
        candles_by_time.values()
    )

    candles.sort(
        key=lambda item: item["time"]
    )

    return candles


# ============================================================
# INDICATORS
# ============================================================

def true_ranges(candles):
    result = []

    for index, candle in enumerate(
        candles
    ):
        if index == 0:
            tr = (
                candle["high"]
                - candle["low"]
            )
        else:
            previous_close = (
                candles[
                    index - 1
                ]["close"]
            )

            tr = max(
                candle["high"]
                - candle["low"],
                abs(
                    candle["high"]
                    - previous_close
                ),
                abs(
                    candle["low"]
                    - previous_close
                ),
            )

        result.append(tr)

    return result


def rma_series(
    values,
    length,
):
    result = [None] * len(values)

    if len(values) < length:
        return result

    initial = (
        sum(values[:length])
        / length
    )

    result[
        length - 1
    ] = initial

    previous = initial

    for index in range(
        length,
        len(values),
    ):
        current = (
            (
                previous
                * (
                    length - 1
                )
            )
            + values[index]
        ) / length

        result[index] = current
        previous = current

    return result


def atr_series(
    candles,
    length=14,
):
    return rma_series(
        true_ranges(
            candles
        ),
        length,
    )


def rolling_mean_optional(
    values,
    length,
):
    result = [None] * len(values)

    for index in range(
        length - 1,
        len(values),
    ):
        window = values[
            index - length + 1:
            index + 1
        ]

        if any(
            value is None
            for value in window
        ):
            continue

        result[index] = (
            sum(window)
            / length
        )

    return result


# ============================================================
# RAW BEARISH ENGULFING FEATURES
# ============================================================

def build_raw_candidates(
    h1,
    h1_atr,
    atr_mean_50,
):
    candidates = []

    max_lookback = max(
        STRUCTURE_LOOKBACK,
        48,
        50,
    )

    for index in range(
        max_lookback,
        len(h1),
    ):
        signal = h1[index]

        if signal["time"] < RESEARCH_FROM:
            continue

        if signal["time"] >= RESEARCH_TO:
            break

        previous = h1[
            index - 1
        ]

        atr = h1_atr[
            index
        ]

        if (
            atr is None
            or atr <= 0
        ):
            continue

        previous_body = abs(
            previous["close"]
            - previous["open"]
        )

        current_body = abs(
            signal["close"]
            - signal["open"]
        )

        candle_range = (
            signal["high"]
            - signal["low"]
        )

        if (
            previous_body <= 0
            or current_body <= 0
            or candle_range <= 0
        ):
            continue

        bearish_engulfing = (
            previous["close"]
            > previous["open"]
            and signal["close"]
            < signal["open"]
            and signal["open"]
            >= previous["close"]
            and signal["close"]
            <= previous["open"]
        )

        if not bearish_engulfing:
            continue

        body_ratio = (
            current_body
            / previous_body
        )

        if (
            body_ratio
            < MIN_BODY_RATIO
        ):
            continue

        previous_highest = max(
            candle["high"]
            for candle in h1[
                index - STRUCTURE_LOOKBACK:
                index
            ]
        )

        structure_distance_atr = (
            previous_highest
            - signal["high"]
        ) / atr

        range_atr = (
            candle_range
            / atr
        )

        close_location = (
            signal["close"]
            - signal["low"]
        ) / candle_range

        momentum_12 = (
            signal["close"]
            - h1[
                index - 12
            ]["close"]
        ) / atr

        momentum_48 = (
            signal["close"]
            - h1[
                index - 48
            ]["close"]
        ) / atr

        upper_wick = max(
            0.0,
            signal["high"]
            - max(
                signal["open"],
                signal["close"],
            )
        )

        upper_wick_body = (
            upper_wick
            / current_body
        )

        stop = (
            signal["high"]
            + STOP_BUFFER_TICKS
            * TICK_SIZE
        )

        stop_size_atr = (
            stop
            - signal["close"]
        ) / atr

        atr_ratio_50 = None

        if (
            atr_mean_50[index]
            is not None
            and atr_mean_50[index] > 0
        ):
            atr_ratio_50 = (
                atr
                / atr_mean_50[index]
            )

        ny_time = (
            signal["time"]
            .astimezone(NY_TZ)
        )

        candidates.append({
            "index": index,
            "time": signal["time"],
            "ny_hour": ny_time.hour,
            "structure_distance_atr": (
                structure_distance_atr
            ),
            "range_atr": range_atr,
            "close_location": close_location,
            "momentum_12": momentum_12,
            "momentum_48": momentum_48,
            "upper_wick_body": upper_wick_body,
            "stop_size_atr": stop_size_atr,
            "atr_ratio_50": atr_ratio_50,
        })

    return candidates


# ============================================================
# EFFECTIVE PARAMETER RESOLUTION
# ============================================================

def effective_parameters(
    branch,
    relaxation,
):
    params = {
        "max_distance_atr": (
            relaxation[
                "max_distance_atr"
            ]
        ),
        "min_range_atr": (
            relaxation[
                "min_range_atr"
            ]
        ),
        "max_close_location": (
            relaxation[
                "max_close_location"
            ]
        ),
        "min_momentum_12": (
            branch[
                "min_momentum_12"
            ]
        ),
        "min_momentum_48": (
            branch[
                "min_momentum_48"
            ]
        ),
        "max_stop_size_atr": (
            branch[
                "max_stop_size_atr"
            ]
        ),
        "min_upper_wick_body": (
            branch[
                "min_upper_wick_body"
            ]
        ),
        "min_atr_ratio_50": (
            branch[
                "min_atr_ratio_50"
            ]
        ),
    }

    family = relaxation[
        "relax_family"
    ]

    if (
        family
        == "MOMENTUM_12H"
    ):
        params[
            "min_momentum_12"
        ] = relaxation[
            "min_momentum_12"
        ]

    elif (
        family
        == "MOMENTUM_48H"
    ):
        params[
            "min_momentum_48"
        ] = relaxation[
            "min_momentum_48"
        ]

    elif (
        family
        == "STOP_CAP"
    ):
        params[
            "max_stop_size_atr"
        ] = relaxation[
            "max_stop_size_atr"
        ]

    elif (
        family
        == "UPPER_WICK"
    ):
        params[
            "min_upper_wick_body"
        ] = relaxation[
            "min_upper_wick_body"
        ]

    elif (
        family
        == "ATR_REGIME"
    ):
        params[
            "min_atr_ratio_50"
        ] = relaxation[
            "min_atr_ratio_50"
        ]

    return params


def passes_parameters(
    candidate,
    params,
):
    if (
        candidate[
            "structure_distance_atr"
        ] > params[
            "max_distance_atr"
        ]
    ):
        return False

    if (
        candidate[
            "range_atr"
        ] < params[
            "min_range_atr"
        ]
    ):
        return False

    if (
        candidate[
            "close_location"
        ] > params[
            "max_close_location"
        ]
    ):
        return False

    value = params[
        "min_momentum_12"
    ]

    if (
        value is not None
        and candidate[
            "momentum_12"
        ] < value
    ):
        return False

    value = params[
        "min_momentum_48"
    ]

    if (
        value is not None
        and candidate[
            "momentum_48"
        ] < value
    ):
        return False

    value = params[
        "max_stop_size_atr"
    ]

    if (
        value is not None
        and candidate[
            "stop_size_atr"
        ] > value
    ):
        return False

    value = params[
        "min_upper_wick_body"
    ]

    if (
        value is not None
        and candidate[
            "upper_wick_body"
        ] < value
    ):
        return False

    value = params[
        "min_atr_ratio_50"
    ]

    if value is not None:
        if (
            candidate[
                "atr_ratio_50"
            ] is None
            or candidate[
                "atr_ratio_50"
            ] < value
        ):
            return False

    return True


# ============================================================
# EXIT SIMULATION
# ============================================================

EXIT_CACHE = {}


def calculate_trade_exit(
    h1,
    signal_index,
):
    if signal_index in EXIT_CACHE:
        return EXIT_CACHE[
            signal_index
        ]

    signal = h1[
        signal_index
    ]

    reference_entry = (
        signal["close"]
    )

    backtest_entry = (
        reference_entry
        - BACKTEST_SLIPPAGE_TICKS
        * TICK_SIZE
    )

    stop = (
        signal["high"]
        + STOP_BUFFER_TICKS
        * TICK_SIZE
    )

    reference_risk = (
        stop
        - reference_entry
    )

    if reference_risk <= 0:
        raise RuntimeError(
            "Invalid short reference risk"
        )

    target = (
        reference_entry
        - reference_risk
        * REWARD_RISK
    )

    actual_risk = (
        stop
        - backtest_entry
    )

    if actual_risk <= 0:
        raise RuntimeError(
            "Invalid short actual risk"
        )

    for index in range(
        signal_index + 1,
        len(h1),
    ):
        candle = h1[index]

        if (
            candle["time"]
            >= RESEARCH_TO
        ):
            break

        stop_hit = (
            candle["high"]
            >= stop
        )

        target_hit = (
            candle["low"]
            <= target
        )

        if not (
            stop_hit
            or target_hit
        ):
            continue

        if (
            stop_hit
            and target_hit
        ):
            distance_to_high = abs(
                candle["high"]
                - candle["open"]
            )

            distance_to_low = abs(
                candle["open"]
                - candle["low"]
            )

            if (
                distance_to_high
                < distance_to_low
            ):
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
            "exit_index": index,
            "exit_time": candle["time"],
            "exit_reason": exit_reason,
            "result_r": (
                backtest_entry
                - exit_price
            ) / actual_risk,
        }

        EXIT_CACHE[
            signal_index
        ] = result

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

    EXIT_CACHE[
        signal_index
    ] = result

    return result


def simulate(
    h1,
    eligible,
):
    trades = []
    position_exit_index = -1
    ignored = 0
    still_open = False

    for candidate in eligible:
        signal_index = (
            candidate[
                "index"
            ]
        )

        # Locked convention:
        # signal on exact candle where previous trade exits is allowed.
        if (
            signal_index
            < position_exit_index
        ):
            ignored += 1
            continue

        trade = calculate_trade_exit(
            h1,
            signal_index,
        )

        if (
            trade[
                "status"
            ] == "OPEN"
        ):
            still_open = True
            break

        trades.append(
            trade
        )

        position_exit_index = (
            trade[
                "exit_index"
            ]
        )

    return (
        trades,
        ignored,
        still_open,
    )


# ============================================================
# STATS
# ============================================================

def stats_for_trades(
    trades,
    start=None,
    end=None,
):
    filtered = []

    for trade in trades:
        signal_time = (
            trade[
                "signal_time"
            ]
        )

        if (
            start is not None
            and signal_time < start
        ):
            continue

        if (
            end is not None
            and signal_time >= end
        ):
            continue

        filtered.append(
            trade
        )

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

    results = [
        trade[
            "result_r"
        ]
        for trade in filtered
    ]

    winners = [
        result
        for result in results
        if result > 0
    ]

    losers = [
        result
        for result in results
        if result < 0
    ]

    gross_profit = sum(
        winners
    )

    gross_loss = abs(
        sum(
            losers
        )
    )

    total_r = sum(
        results
    )

    if gross_loss > 0:
        profit_factor = (
            gross_profit
            / gross_loss
        )
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

        peak = max(
            peak,
            equity,
        )

        max_drawdown = min(
            max_drawdown,
            equity - peak,
        )

        if result < 0:
            current_streak += 1

            longest_streak = max(
                longest_streak,
                current_streak,
            )
        else:
            current_streak = 0

    return {
        "trades": len(
            results
        ),
        "winners": len(
            winners
        ),
        "losers": len(
            losers
        ),
        "win_rate": round(
            len(
                winners
            )
            / len(
                results
            )
            * 100.0,
            2,
        ),
        "profit_factor": round(
            profit_factor,
            3,
        ),
        "total_r": round(
            total_r,
            2,
        ),
        "expectancy_r": round(
            total_r
            / len(
                results
            ),
            3,
        ),
        "max_drawdown_r": round(
            max_drawdown,
            2,
        ),
        "longest_loss_streak": (
            longest_streak
        ),
    }


# ============================================================
# RESULT ROW
# ============================================================

def build_result_row(
    branch,
    timing_profile,
    relaxation,
    params,
    eligible,
    trades,
    ignored,
    still_open,
    years,
):
    full = stats_for_trades(
        trades
    )

    row = {
        "branch": (
            branch[
                "branch"
            ]
        ),

        "timing_label": (
            timing_profile[
                "timing_label"
            ]
        ),

        "excluded_ny_hours": ",".join(
            f"{hour:02d}"
            for hour in sorted(
                timing_profile[
                    "excluded_hours"
                ]
            )
        ),

        "relax_family": (
            relaxation[
                "relax_family"
            ]
        ),

        "relax_label": (
            relaxation[
                "relax_label"
            ]
        ),

        "reward_risk": (
            REWARD_RISK
        ),

        "structure_lookback": (
            STRUCTURE_LOOKBACK
        ),

        "max_distance_atr": (
            params[
                "max_distance_atr"
            ]
        ),

        "min_range_atr": (
            params[
                "min_range_atr"
            ]
        ),

        "max_close_location": (
            params[
                "max_close_location"
            ]
        ),

        "min_momentum_12h_atr": (
            params[
                "min_momentum_12"
            ]
        ),

        "min_momentum_48h_atr": (
            params[
                "min_momentum_48"
            ]
        ),

        "max_stop_size_atr": (
            params[
                "max_stop_size_atr"
            ]
        ),

        "min_upper_wick_body": (
            params[
                "min_upper_wick_body"
            ]
        ),

        "min_atr_ratio_50": (
            params[
                "min_atr_ratio_50"
            ]
        ),

        "eligible_signals": (
            len(
                eligible
            )
        ),

        "ignored_due_to_open_trade": (
            ignored
        ),

        "still_open_at_end": (
            still_open
        ),

        "trades": (
            full[
                "trades"
            ]
        ),

        "trades_per_year": round(
            full[
                "trades"
            ]
            / years,
            2,
        ),

        "winners": (
            full[
                "winners"
            ]
        ),

        "losers": (
            full[
                "losers"
            ]
        ),

        "win_rate": (
            full[
                "win_rate"
            ]
        ),

        "profit_factor": (
            full[
                "profit_factor"
            ]
        ),

        "total_r": (
            full[
                "total_r"
            ]
        ),

        "expectancy_r": (
            full[
                "expectancy_r"
            ]
        ),

        "max_drawdown_r": (
            full[
                "max_drawdown_r"
            ]
        ),

        "longest_loss_streak": (
            full[
                "longest_loss_streak"
            ]
        ),
    }

    profitable_eras = 0
    minimum_era_pf = None
    minimum_era_expectancy = None

    for (
        era_name,
        era_start,
        era_end,
    ) in ERAS:
        era = stats_for_trades(
            trades,
            era_start,
            era_end,
        )

        row[
            f"{era_name}_trades"
        ] = era[
            "trades"
        ]

        row[
            f"{era_name}_pf"
        ] = era[
            "profit_factor"
        ]

        row[
            f"{era_name}_r"
        ] = era[
            "total_r"
        ]

        row[
            f"{era_name}_expectancy"
        ] = era[
            "expectancy_r"
        ]

        if (
            era[
                "trades"
            ] >= 5
        ):
            if (
                era[
                    "total_r"
                ] > 0
            ):
                profitable_eras += 1

            if (
                minimum_era_pf
                is None
            ):
                minimum_era_pf = (
                    era[
                        "profit_factor"
                    ]
                )
            else:
                minimum_era_pf = min(
                    minimum_era_pf,
                    era[
                        "profit_factor"
                    ],
                )

            if (
                minimum_era_expectancy
                is None
            ):
                minimum_era_expectancy = (
                    era[
                        "expectancy_r"
                    ]
                )
            else:
                minimum_era_expectancy = min(
                    minimum_era_expectancy,
                    era[
                        "expectancy_r"
                    ],
                )

    row[
        "profitable_eras_with_5_plus_trades"
    ] = profitable_eras

    row[
        "minimum_era_pf_5_plus"
    ] = minimum_era_pf

    row[
        "minimum_era_expectancy_5_plus"
    ] = minimum_era_expectancy

    row[
        "all_four_eras_profitable"
    ] = (
        profitable_eras >= 4
    )

    row[
        "adequate_90_trades"
    ] = (
        full[
            "trades"
        ] >= 90
    )

    row[
        "adequate_100_trades"
    ] = (
        full[
            "trades"
        ] >= 100
    )

    row[
        "frequency_4py"
    ] = (
        full[
            "trades"
        ]
        / years
        >= 4.0
    )

    row[
        "frequency_45py"
    ] = (
        full[
            "trades"
        ]
        / years
        >= 4.5
    )

    row[
        "worst_era_pf_120"
    ] = (
        minimum_era_pf is not None
        and minimum_era_pf >= 1.20
    )

    row[
        "worst_era_pf_130"
    ] = (
        minimum_era_pf is not None
        and minimum_era_pf >= 1.30
    )

    row[
        "worst_era_pf_140"
    ] = (
        minimum_era_pf is not None
        and minimum_era_pf >= 1.40
    )

    row[
        "pf_160"
    ] = (
        full[
            "profit_factor"
        ] >= 1.60
    )

    row[
        "pf_180"
    ] = (
        full[
            "profit_factor"
        ] >= 1.80
    )

    row[
        "pf_200"
    ] = (
        full[
            "profit_factor"
        ] >= 2.00
    )

    row[
        "annual_r_linear"
    ] = round(
        full[
            "expectancy_r"
        ]
        * (
            full[
                "trades"
            ]
            / years
        ),
        3,
    )

    return row


# ============================================================
# BASELINE DELTAS
# ============================================================

def add_timing_baseline_deltas(
    df,
):
    df = df.copy()

    metrics = [
        "trades",
        "trades_per_year",
        "profit_factor",
        "total_r",
        "expectancy_r",
        "max_drawdown_r",
        "minimum_era_pf_5_plus",
        "2024_present_pf",
        "annual_r_linear",
    ]

    group_cols = [
        "branch",
        "timing_label",
    ]

    for (
        branch_name,
        timing_label,
    ), group in df.groupby(
        group_cols
    ):
        baseline = group[
            group[
                "relax_family"
            ] == "BASELINE"
        ]

        if len(
            baseline
        ) != 1:
            raise RuntimeError(
                f"Expected one timing baseline for "
                f"{branch_name}/{timing_label}"
            )

        baseline_row = (
            baseline.iloc[0]
        )

        mask = (
            (
                df[
                    "branch"
                ] == branch_name
            )
            & (
                df[
                    "timing_label"
                ] == timing_label
            )
        )

        for metric in metrics:
            df.loc[
                mask,
                f"delta_{metric}_vs_timing_baseline"
            ] = (
                df.loc[
                    mask,
                    metric
                ]
                - baseline_row[
                    metric
                ]
            )

    return df


# ============================================================
# RESEARCH
# ============================================================

def run_research():
    global STATUS

    try:
        print()
        print("=" * 84)
        print(
            "EUR/GBP SHORT - TIMING x FREQUENCY RECOVERY MATRIX"
        )
        print("=" * 84)
        print(
            f"Total tests: {TOTAL_TESTS}"
        )
        print()

        STATUS.update({
            "state": "fetching_data",
            "message": (
                "Fetching EUR/GBP OANDA H1 history"
            ),
        })

        h1 = fetch_chunked_history(
            INSTRUMENT,
            "H1",
            RESEARCH_FROM
            - timedelta(
                days=H1_WARMUP_DAYS
            ),
            RESEARCH_TO,
        )

        if not h1:
            raise RuntimeError(
                "No EUR/GBP H1 candles returned"
            )

        STATUS.update({
            "state": "precomputing",
            "message": (
                "Building ATR14 and raw bearish-engulfing features"
            ),
        })

        h1_atr = atr_series(
            h1,
            14,
        )

        atr_mean_50 = (
            rolling_mean_optional(
                h1_atr,
                50,
            )
        )

        raw_candidates = (
            build_raw_candidates(
                h1,
                h1_atr,
                atr_mean_50,
            )
        )

        STATUS[
            "raw_bearish_engulfing_signals"
        ] = len(
            raw_candidates
        )

        years = (
            RESEARCH_TO
            - RESEARCH_FROM
        ).total_seconds() / (
            365.2425
            * 24
            * 60
            * 60
        )

        STATUS.update({
            "state": "running",
            "message": (
                "Running timing x frequency-recovery matrix"
            ),
        })

        rows = []
        completed = 0

        for timing_profile in (
            TIMING_PROFILES
        ):
            branch_name = (
                timing_profile[
                    "branch"
                ]
            )

            branch = (
                BRANCHES[
                    branch_name
                ]
            )

            print()
            print(
                f"{branch_name} | "
                f"{timing_profile['timing_label']}",
                flush=True,
            )

            for relaxation in (
                RELAXATIONS[
                    branch_name
                ]
            ):
                params = (
                    effective_parameters(
                        branch,
                        relaxation,
                    )
                )

                eligible = [
                    candidate
                    for candidate
                    in raw_candidates
                    if (
                        candidate[
                            "ny_hour"
                        ] not in timing_profile[
                            "excluded_hours"
                        ]
                        and passes_parameters(
                            candidate,
                            params,
                        )
                    )
                ]

                (
                    trades,
                    ignored,
                    still_open,
                ) = simulate(
                    h1,
                    eligible,
                )

                rows.append(
                    build_result_row(
                        branch,
                        timing_profile,
                        relaxation,
                        params,
                        eligible,
                        trades,
                        ignored,
                        still_open,
                        years,
                    )
                )

                completed += 1

                STATUS[
                    "completed_tests"
                ] = completed

                print(
                    f"{completed}/{TOTAL_TESTS} | "
                    f"{branch_name} | "
                    f"{timing_profile['timing_label']} | "
                    f"{relaxation['relax_label']}",
                    flush=True,
                )

        df = pd.DataFrame(
            rows
        )

        if df.empty:
            raise RuntimeError(
                "No result rows generated"
            )

        df = add_timing_baseline_deltas(
            df
        )

        df[
            "is_timing_baseline"
        ] = (
            df[
                "relax_family"
            ] == "BASELINE"
        )

        df = df.sort_values(
            by=[
                "branch",
                "timing_label",
                "is_timing_baseline",
                "all_four_eras_profitable",
                "adequate_100_trades",
                "frequency_4py",
                "worst_era_pf_140",
                "worst_era_pf_130",
                "worst_era_pf_120",
                "minimum_era_pf_5_plus",
                "profit_factor",
                "expectancy_r",
                "annual_r_linear",
                "trades",
            ],
            ascending=[
                True,
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

        df.to_csv(
            OUTPUT_FILE,
            index=False,
        )

        STATUS.update({
            "state": "complete",
            "message": (
                "EUR/GBP timing frequency-recovery matrix "
                "completed successfully"
            ),
            "completed_tests": TOTAL_TESTS,
            "rows_saved": len(
                df
            ),
            "output_file": (
                OUTPUT_FILE
            ),
        })

        print()
        print("=" * 84)
        print(
            "EUR/GBP TIMING x FREQUENCY RECOVERY COMPLETE"
        )
        print("=" * 84)
        print(
            "Rows:",
            len(
                df
            ),
        )
        print(
            "Saved:",
            OUTPUT_FILE,
        )

        for (
            branch_name,
            timing_label,
        ), group in df.groupby(
            [
                "branch",
                "timing_label",
            ]
        ):
            print()
            print(
                f"--- {branch_name} | {timing_label} ---"
            )

            print(
                group[
                    [
                        "relax_family",
                        "relax_label",
                        "trades",
                        "trades_per_year",
                        "profit_factor",
                        "total_r",
                        "expectancy_r",
                        "max_drawdown_r",
                        "minimum_era_pf_5_plus",
                        "2024_present_pf",
                        "delta_trades_vs_timing_baseline",
                        "delta_profit_factor_vs_timing_baseline",
                    ]
                ].head(
                    15
                ).to_string(
                    index=False
                ),
                flush=True,
            )

    except Exception as error:
        STATUS.update({
            "state": "error",
            "message": str(
                error
            ),
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
def home():
    return jsonify({
        "service": (
            "EURGBP Short Timing Frequency Recovery"
        ),
        "status": STATUS,
        "instrument": INSTRUMENT,
        "direction": "SHORT",
        "reward_risk": REWARD_RISK,
        "timezone": "America/New_York",
        "timing_basis": (
            "signal candle open time"
        ),
        "trading_enabled": False,
        "orders_supported": False,
        "executor_connected": False,

        "shared_baseline": {
            "minimum_body_ratio": (
                MIN_BODY_RATIO
            ),
            "structure_lookback": (
                STRUCTURE_LOOKBACK
            ),
            "max_distance_atr": (
                BASE_MAX_DISTANCE_ATR
            ),
            "min_range_atr": (
                BASE_MIN_RANGE_ATR
            ),
            "max_close_location": (
                BASE_MAX_CLOSE_LOCATION
            ),
            "stop_buffer_ticks": (
                STOP_BUFFER_TICKS
            ),
            "backtest_slippage_ticks": (
                BACKTEST_SLIPPAGE_TICKS
            ),
        },

        "branches": (
            BRANCHES
        ),

        "timing_profiles": (
            TIMING_PROFILES
        ),

        "relaxation_counts": {
            branch_name: len(
                profiles
            )
            for (
                branch_name,
                profiles
            ) in RELAXATIONS.items()
        },

        "total_tests": (
            TOTAL_TESTS
        ),

        "download": (
            "/download"
        ),
    })


@app.route("/status")
def status():
    return jsonify(
        STATUS
    )


@app.route("/download")
def download():
    if not os.path.exists(
        OUTPUT_FILE
    ):
        return jsonify({
            "status": "not_ready",
            "message": (
                "EUR/GBP timing frequency-recovery CSV "
                "is not ready yet"
            ),
        }), 404

    return send_file(
        OUTPUT_FILE,
        as_attachment=True,
        download_name=OUTPUT_FILE,
    )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    research_thread = threading.Thread(
        target=run_research,
        name=(
            "eurgbp-short-timing-frequency-recovery"
        ),
        daemon=True,
    )

    research_thread.start()

    port = int(
        os.getenv(
            "PORT",
            5000,
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
    )
