
import os
import csv
import time
import bisect
import math
import threading
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from flask import Flask, jsonify, send_file


# ============================================================
# EUR/USD M15 LONG
# EXHAUSTIVE-BUT-CONTROLLED RESEARCH PASS
#
# READ-ONLY RESEARCH. NEVER SENDS ORDERS.
#
# Objective:
#   Exhaust sensible filter families around the current strong
#   provisional incumbent WITHOUT blindly brute-forcing a huge
#   combinatorial grid.
#
# Incumbent:
#   exact bullish engulfing
#   BR >= 1.00
#   body >= 0.75 ATR14
#   structure 165 / <= 0.10 ATR
#   all hours
#   all weekdays
#   no range filter
#   no strong-close filter
#   no wick filter
#   no H1 / daily regime
#   RR 3.75
#   stop = signal low - 10 ticks
#
# Research phases:
#
#   PHASE A - ABLATION
#     Remove incumbent filters one at a time.
#
#   PHASE B - SINGLE-FAMILY OVERLAYS
#     Test one additional hypothesis at a time:
#       - body ratio
#       - body ATR
#       - range ATR
#       - close strength
#       - lower-wick/body
#       - structure neighbourhood
#       - M15 ATR regime
#       - prior momentum 4h / 12h / 24h
#       - stop-size / ATR
#       - NY hours / exclusions
#       - weekdays / exclusions
#       - H1 close vs EMA
#       - H1 EMA alignment
#       - H1 ATR regime
#       - Daily close vs EMA
#       - Daily EMA alignment
#       - Daily ATR regime
#       - RR neighbourhood
#
#   PHASE C - CONTROLLED INTERACTIONS
#     Only the strongest independently useful overlays are
#     paired with each other. No giant all-vs-all lottery.
#
#   PHASE D - VALIDATION
#       - 4 eras
#       - DEV 2010-2017 vs VALIDATION 2018-now
#       - recent 5Y / 2Y
#       - monthly rolling 2Y / 3Y
#       - cost stress 0.5 / 1 / 1.5 / 2 pips
#       - overlap/exclusive comparison against incumbent
#
# Historical conventions preserved:
#   - OANDA midpoint candles
#   - exact bullish engulfing
#   - ATR14 Wilder/RMA, SMA seeded
#   - M15 signal time = candle OPEN
#   - stop = signal low - 10 ticks
#   - target based on REFERENCE signal-close risk
#   - adverse long entry = signal close + cost
#   - exits begin NEXT candle
#   - same-bar long tie:
#       high closer => TARGET first
#       otherwise STOP first
#   - pyramiding 0
#   - exact exit-candle signal eligible
#
# OANDA daily alignment:
#   17:00 America/New_York
# ============================================================


app = Flask(__name__)

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_BASE = os.getenv(
    "OANDA_API_URL",
    "https://api-fxtrade.oanda.com",
)

INSTRUMENT = "EUR_USD"

RESEARCH_FROM = datetime(
    2010, 1, 1, 0, 0,
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

NY = ZoneInfo(
    "America/New_York"
)

TICK_SIZE = 0.00001
PIP_SIZE = 0.0001
STOP_BUFFER_TICKS = 10

PRIMARY_COST_PIPS = 1.00

COST_PIPS_GRID = [
    0.50,
    1.00,
    1.50,
    2.00,
]

INCUMBENT = {
    "label":
        "INCUMBENT",
    "minimum_body_ratio":
        1.00,
    "minimum_body_atr":
        0.75,
    "minimum_range_atr":
        None,
    "minimum_close_location":
        None,
    "minimum_lower_wick_body":
        None,
    "structure_lookback":
        165,
    "maximum_distance_atr":
        0.10,
    "minimum_m15_atr_ratio":
        None,
    "maximum_m15_atr_ratio":
        None,
    "minimum_momentum_4h_atr":
        None,
    "maximum_momentum_4h_atr":
        None,
    "minimum_momentum_12h_atr":
        None,
    "maximum_momentum_12h_atr":
        None,
    "minimum_momentum_24h_atr":
        None,
    "maximum_momentum_24h_atr":
        None,
    "maximum_stop_atr":
        None,
    "minimum_stop_atr":
        None,
    "included_ny_hours":
        None,
    "excluded_ny_hours":
        set(),
    "excluded_weekdays":
        set(),
    "h1_close_above_ema":
        None,
    "h1_fast_ema":
        None,
    "h1_slow_ema":
        None,
    "minimum_h1_atr_ratio":
        None,
    "daily_close_above_ema":
        None,
    "daily_fast_ema":
        None,
    "daily_slow_ema":
        None,
    "minimum_daily_atr_ratio":
        None,
    "reward_risk":
        3.75,
}


OUTPUT_ABLATION = (
    "eurusd_m15_long_exhaustive_ablation.csv"
)

OUTPUT_SINGLE = (
    "eurusd_m15_long_exhaustive_single_family.csv"
)

OUTPUT_SINGLE_TOP = (
    "eurusd_m15_long_exhaustive_single_top.csv"
)

OUTPUT_INTERACTIONS = (
    "eurusd_m15_long_exhaustive_interactions.csv"
)

OUTPUT_TOP = (
    "eurusd_m15_long_exhaustive_top.csv"
)

OUTPUT_ERAS = (
    "eurusd_m15_long_exhaustive_eras.csv"
)

OUTPUT_DEVVAL = (
    "eurusd_m15_long_exhaustive_dev_validation.csv"
)

OUTPUT_RECENT = (
    "eurusd_m15_long_exhaustive_recent.csv"
)

OUTPUT_ROLLING = (
    "eurusd_m15_long_exhaustive_rolling.csv"
)

OUTPUT_ROLLING_SUMMARY = (
    "eurusd_m15_long_exhaustive_rolling_summary.csv"
)

OUTPUT_OVERLAP = (
    "eurusd_m15_long_exhaustive_overlap.csv"
)

OUTPUT_BEST_TRADES = (
    "eurusd_m15_long_exhaustive_best_trades.csv"
)

STATUS = {
    "state": "not_started",
    "message": "Exhaustive EUR/USD M15 long research has not started",
    "service": "EURUSD M15 Long Exhaustive Controlled",
    "orders_supported": False,
    "trading_enabled": False,
}


# ============================================================
# HELPERS
# ============================================================

def iso_utc(dt):
    return (
        dt.astimezone(
            timezone.utc
        )
        .isoformat()
        .replace(
            "+00:00",
            "Z",
        )
    )


def parse_oanda_time(value):
    if value.endswith("Z"):
        value = (
            value[:-1]
            + "+00:00"
        )

    if "." in value:
        left, right = value.split(
            ".",
            1,
        )

        if "+" in right:
            fraction, offset = right.split(
                "+",
                1,
            )
            fraction = (
                fraction[:6]
                .ljust(
                    6,
                    "0",
                )
            )
            value = (
                left
                + "."
                + fraction
                + "+"
                + offset
            )

    return (
        datetime
        .fromisoformat(
            value
        )
        .astimezone(
            timezone.utc
        )
    )


def years_ago_safe(
    dt,
    years,
):
    try:
        return dt.replace(
            year=dt.year - years
        )
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


def add_months(
    dt,
    months,
):
    total = (
        dt.year * 12
        + dt.month - 1
        + months
    )

    return datetime(
        total // 12,
        total % 12 + 1,
        1,
        tzinfo=timezone.utc,
    )


def write_csv(
    path,
    rows,
):
    if not rows:
        with open(
            path,
            "w",
            encoding="utf-8",
        ) as handle:
            handle.write("")
        return

    fields = []
    seen = set()

    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(
                    key
                )
                fields.append(
                    key
                )

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
        writer.writerows(
            rows
        )


def download_file(
    path,
):
    if not os.path.exists(
        path
    ):
        return jsonify({
            "error":
                "Output not ready yet",
            "path":
                path,
        }), 404

    return send_file(
        os.path.abspath(
            path
        ),
        as_attachment=True,
        download_name=os.path.basename(
            path
        ),
    )


def clone_config(
    base,
    label,
):
    result = {}

    for key, value in base.items():
        if isinstance(
            value,
            set,
        ):
            result[
                key
            ] = set(
                value
            )
        else:
            result[
                key
            ] = value

    result[
        "label"
    ] = label

    return result


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
            "Bearer "
            + OANDA_TOKEN.strip(),
        "Content-Type":
            "application/json",
    }


def fetch_candles_chunk(
    granularity,
    start,
    end,
    daily_alignment=False,
):
    url = (
        f"{OANDA_BASE}"
        f"/v3/instruments/"
        f"{INSTRUMENT}/candles"
    )

    params = {
        "price":
            "M",
        "granularity":
            granularity,
        "smooth":
            "false",
        "from":
            iso_utc(
                start
            ),
        "to":
            iso_utc(
                end
            ),
        "includeFirst":
            "true",
    }

    if daily_alignment:
        params[
            "dailyAlignment"
        ] = 17

        params[
            "alignmentTimezone"
        ] = (
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
        "candles",
        [],
    ):
        if not item.get(
            "complete",
            False,
        ):
            continue

        mid = item[
            "mid"
        ]

        rows.append({
            "time":
                parse_oanda_time(
                    item[
                        "time"
                    ]
                ),
            "open":
                float(
                    mid[
                        "o"
                    ]
                ),
            "high":
                float(
                    mid[
                        "h"
                    ]
                ),
            "low":
                float(
                    mid[
                        "l"
                    ]
                ),
            "close":
                float(
                    mid[
                        "c"
                    ]
                ),
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
            cursor
            + timedelta(
                days=chunk_days
            ),
            end,
        )

        STATUS.update({
            "state":
                "fetching",
            "message":
                (
                    f"Fetching {granularity} "
                    f"chunk {chunk_number}: "
                    f"{iso_utc(cursor)} -> "
                    f"{iso_utc(chunk_end)}"
                ),
            "granularity":
                granularity,
            "chunk":
                chunk_number,
        })

        rows = fetch_candles_chunk(
            granularity,
            cursor,
            chunk_end,
            daily_alignment=daily_alignment,
        )

        for row in rows:
            by_time[
                row[
                    "time"
                ]
            ] = row

        cursor = chunk_end

        time.sleep(
            0.02
        )

    result = list(
        by_time.values()
    )

    result.sort(
        key=lambda row:
            row[
                "time"
            ]
    )

    return result


# ============================================================
# INDICATORS
# ============================================================

def true_ranges(
    candles,
):
    result = [
        None
    ] * len(
        candles
    )

    for i in range(
        len(candles)
    ):
        high = candles[
            i
        ][
            "high"
        ]
        low = candles[
            i
        ][
            "low"
        ]

        if i == 0:
            result[
                i
            ] = (
                high
                - low
            )
        else:
            previous_close = candles[
                i - 1
            ][
                "close"
            ]

            result[
                i
            ] = max(
                high - low,
                abs(
                    high
                    - previous_close
                ),
                abs(
                    low
                    - previous_close
                ),
            )

    return result


def rma_from_values(
    values,
    length,
):
    result = [
        None
    ] * len(
        values
    )

    if len(
        values
    ) < length:
        return result

    seed = values[
        :length
    ]

    if any(
        value is None
        for value in seed
    ):
        return result

    result[
        length - 1
    ] = (
        sum(
            seed
        )
        / length
    )

    for i in range(
        length,
        len(values),
    ):
        value = values[
            i
        ]

        if (
            value is None
            or result[
                i - 1
            ] is None
        ):
            continue

        result[
            i
        ] = (
            result[
                i - 1
            ]
            * (
                length - 1
            )
            + value
        ) / length

    return result


def atr14(
    candles,
):
    return rma_from_values(
        true_ranges(
            candles
        ),
        14,
    )


def ema(
    values,
    length,
):
    result = [
        None
    ] * len(
        values
    )

    if len(
        values
    ) < length:
        return result

    seed = values[
        :length
    ]

    if any(
        value is None
        for value in seed
    ):
        return result

    result[
        length - 1
    ] = (
        sum(
            seed
        )
        / length
    )

    alpha = (
        2.0
        / (
            length
            + 1.0
        )
    )

    for i in range(
        length,
        len(values),
    ):
        if (
            values[
                i
            ] is None
            or result[
                i - 1
            ] is None
        ):
            continue

        result[
            i
        ] = (
            alpha
            * values[
                i
            ]
            + (
                1.0
                - alpha
            )
            * result[
                i - 1
            ]
        )

    return result


def sma(
    values,
    length,
):
    result = [
        None
    ] * len(
        values
    )

    running = 0.0
    queue = []

    for i, value in enumerate(
        values
    ):
        queue.append(
            value
        )

        if value is not None:
            running += value

        if len(
            queue
        ) > length:
            removed = queue.pop(
                0
            )
            if removed is not None:
                running -= removed

        if (
            len(queue) == length
            and all(
                item is not None
                for item in queue
            )
        ):
            result[
                i
            ] = (
                running
                / length
            )

    return result


def bullish_engulfing(
    candles,
    i,
):
    if i < 1:
        return False

    previous = candles[
        i - 1
    ]
    current = candles[
        i
    ]

    return (
        previous[
            "close"
        ]
        <
        previous[
            "open"
        ]
        and
        current[
            "close"
        ]
        >
        current[
            "open"
        ]
        and
        current[
            "open"
        ]
        <=
        previous[
            "close"
        ]
        and
        current[
            "close"
        ]
        >=
        previous[
            "open"
        ]
    )


# ============================================================
# HTF STATE
# ============================================================

H1_EMA_LENGTHS = [
    20,
    50,
    100,
    200,
]

DAILY_EMA_LENGTHS = [
    20,
    50,
    100,
    150,
    200,
    300,
]

H1_ALIGNMENT_PAIRS = [
    (
        20,
        50,
    ),
    (
        50,
        100,
    ),
    (
        50,
        200,
    ),
]

DAILY_ALIGNMENT_PAIRS = [
    (
        20,
        50,
    ),
    (
        20,
        100,
    ),
    (
        50,
        100,
    ),
    (
        50,
        200,
    ),
    (
        100,
        200,
    ),
]


def build_htf_state(
    candles,
    ema_lengths,
):
    closes = [
        candle[
            "close"
        ]
        for candle in candles
    ]

    atr = atr14(
        candles
    )

    atr_mean50 = sma(
        atr,
        50,
    )

    ema_map = {
        length:
            ema(
                closes,
                length,
            )
        for length in ema_lengths
    }

    rows = []

    for i, candle in enumerate(
        candles
    ):
        atr_ratio = None

        if (
            atr[
                i
            ] is not None
            and
            atr_mean50[
                i
            ] is not None
            and
            atr_mean50[
                i
            ] > 0
        ):
            atr_ratio = (
                atr[
                    i
                ]
                /
                atr_mean50[
                    i
                ]
            )

        rows.append({
            "time":
                candle[
                    "time"
                ],
            "close":
                candle[
                    "close"
                ],
            "atr14":
                atr[
                    i
                ],
            "atr_ratio_50":
                atr_ratio,
            "emas":
                {
                    length:
                        ema_map[
                            length
                        ][
                            i
                        ]
                    for length in ema_lengths
                },
        })

    return rows


def previous_completed_state(
    state_rows,
    times,
    signal_time,
):
    # state row timestamp is candle OPEN.
    # Previous completed candle must have OPEN strictly before
    # the signal candle's containing HTF candle.
    position = bisect.bisect_left(
        times,
        signal_time,
    ) - 1

    if position < 0:
        return None

    return state_rows[
        position
    ]


# ============================================================
# SIGNAL FEATURE CACHE
# ============================================================

STRUCTURE_LOOKBACKS = [
    100,
    120,
    135,
    150,
    165,
    180,
    200,
]

MOMENTUM_BARS = {
    "4h":
        16,
    "12h":
        48,
    "24h":
        96,
}


def build_signal_cache(
    m15,
    m15_atr,
    h1_state,
    daily_state,
):
    signals = []

    m15_atr_mean50 = sma(
        m15_atr,
        50,
    )

    h1_times = [
        row[
            "time"
        ]
        for row in h1_state
    ]

    daily_times = [
        row[
            "time"
        ]
        for row in daily_state
    ]

    max_lookback = max(
        max(
            STRUCTURE_LOOKBACKS
        ),
        max(
            MOMENTUM_BARS.values()
        ),
    )

    for i in range(
        max(
            14,
            max_lookback,
        ),
        len(m15),
    ):
        if not bullish_engulfing(
            m15,
            i,
        ):
            continue

        current = m15[
            i
        ]
        previous = m15[
            i - 1
        ]
        atr = m15_atr[
            i
        ]

        if (
            atr is None
            or atr <= 0
        ):
            continue

        body = (
            current[
                "close"
            ]
            -
            current[
                "open"
            ]
        )

        previous_body = abs(
            previous[
                "close"
            ]
            -
            previous[
                "open"
            ]
        )

        candle_range = (
            current[
                "high"
            ]
            -
            current[
                "low"
            ]
        )

        lower_wick = (
            min(
                current[
                    "open"
                ],
                current[
                    "close"
                ],
            )
            -
            current[
                "low"
            ]
        )

        body_ratio = (
            body
            /
            previous_body
            if previous_body > 0
            else 999.0
        )

        body_atr = (
            body
            /
            atr
        )

        range_atr = (
            candle_range
            /
            atr
        )

        close_location = (
            (
                current[
                    "close"
                ]
                -
                current[
                    "low"
                ]
            )
            /
            candle_range
            if candle_range > 0
            else 0.0
        )

        lower_wick_body = (
            lower_wick
            /
            body
            if body > 0
            else 0.0
        )

        stop_distance = (
            current[
                "close"
            ]
            -
            (
                current[
                    "low"
                ]
                -
                STOP_BUFFER_TICKS
                * TICK_SIZE
            )
        )

        stop_atr = (
            stop_distance
            /
            atr
        )

        m15_atr_ratio = None

        if (
            m15_atr_mean50[
                i
            ] is not None
            and
            m15_atr_mean50[
                i
            ] > 0
        ):
            m15_atr_ratio = (
                atr
                /
                m15_atr_mean50[
                    i
                ]
            )

        momentum = {}

        for name, bars in MOMENTUM_BARS.items():
            prior_close = m15[
                i - bars
            ][
                "close"
            ]

            momentum[
                name
            ] = (
                current[
                    "close"
                ]
                -
                prior_close
            ) / atr

        structure_distance_atr = {}

        for lookback in STRUCTURE_LOOKBACKS:
            previous_low = min(
                candle[
                    "low"
                ]
                for candle in m15[
                    i - lookback:
                    i
                ]
            )

            structure_distance_atr[
                lookback
            ] = (
                abs(
                    current[
                        "low"
                    ]
                    -
                    previous_low
                )
                /
                atr
            )

        ny_time = (
            current[
                "time"
            ]
            .astimezone(
                NY
            )
        )

        h1_prev = previous_completed_state(
            h1_state,
            h1_times,
            current[
                "time"
            ],
        )

        daily_prev = previous_completed_state(
            daily_state,
            daily_times,
            current[
                "time"
            ],
        )

        signals.append({
            "signal_index":
                i,
            "time":
                current[
                    "time"
                ],
            "body_ratio":
                body_ratio,
            "body_atr":
                body_atr,
            "range_atr":
                range_atr,
            "close_location":
                close_location,
            "lower_wick_body":
                lower_wick_body,
            "stop_atr":
                stop_atr,
            "m15_atr_ratio":
                m15_atr_ratio,
            "momentum_4h_atr":
                momentum[
                    "4h"
                ],
            "momentum_12h_atr":
                momentum[
                    "12h"
                ],
            "momentum_24h_atr":
                momentum[
                    "24h"
                ],
            "structure_distance_atr":
                structure_distance_atr,
            "ny_hour":
                ny_time.hour,
            "ny_weekday":
                ny_time.weekday(),
            "h1_prev":
                h1_prev,
            "daily_prev":
                daily_prev,
        })

    return signals


# ============================================================
# TRADE OUTCOME CACHE
# ============================================================

RR_VALUES = [
    2.50,
    2.75,
    3.00,
    3.25,
    3.50,
    3.75,
    4.00,
    4.25,
    4.50,
]


def compute_trade_outcome(
    candles,
    signal_index,
    reward_risk,
    cost_pips,
):
    signal = candles[
        signal_index
    ]

    reference_entry = (
        signal[
            "close"
        ]
    )

    stop = (
        signal[
            "low"
        ]
        -
        STOP_BUFFER_TICKS
        * TICK_SIZE
    )

    reference_risk = (
        reference_entry
        -
        stop
    )

    if reference_risk <= 0:
        return None

    target = (
        reference_entry
        +
        reward_risk
        * reference_risk
    )

    backtest_entry = (
        reference_entry
        +
        cost_pips
        * PIP_SIZE
    )

    actual_risk = (
        backtest_entry
        -
        stop
    )

    if actual_risk <= 0:
        return None

    for j in range(
        signal_index + 1,
        len(candles),
    ):
        candle = candles[
            j
        ]

        hit_stop = (
            candle[
                "low"
            ]
            <=
            stop
        )

        hit_target = (
            candle[
                "high"
            ]
            >=
            target
        )

        if (
            hit_stop
            and
            hit_target
        ):
            distance_high = abs(
                candle[
                    "high"
                ]
                -
                candle[
                    "open"
                ]
            )

            distance_low = abs(
                candle[
                    "open"
                ]
                -
                candle[
                    "low"
                ]
            )

            if (
                distance_high
                <
                distance_low
            ):
                exit_price = (
                    target
                )
                exit_reason = (
                    "TARGET"
                )
            else:
                exit_price = (
                    stop
                )
                exit_reason = (
                    "STOP"
                )

        elif hit_target:
            exit_price = target
            exit_reason = "TARGET"

        elif hit_stop:
            exit_price = stop
            exit_reason = "STOP"

        else:
            continue

        result_r = (
            exit_price
            -
            backtest_entry
        ) / actual_risk

        return {
            "signal_index":
                signal_index,
            "exit_index":
                j,
            "entry_time":
                signal[
                    "time"
                ],
            "exit_time":
                candle[
                    "time"
                ],
            "entry_time_utc":
                iso_utc(
                    signal[
                        "time"
                    ]
                ),
            "exit_time_utc":
                iso_utc(
                    candle[
                        "time"
                    ]
                ),
            "reference_entry":
                reference_entry,
            "backtest_entry":
                backtest_entry,
            "stop":
                stop,
            "target":
                target,
            "exit_reason":
                exit_reason,
            "result_r":
                result_r,
            "reward_risk":
                reward_risk,
            "cost_pips":
                cost_pips,
        }

    return None


def build_outcome_cache(
    candles,
    signals,
):
    cache = {}

    total = (
        len(
            signals
        )
        *
        len(
            RR_VALUES
        )
        *
        len(
            COST_PIPS_GRID
        )
    )

    done = 0

    for signal in signals:
        signal_index = signal[
            "signal_index"
        ]

        for rr in RR_VALUES:
            for cost in COST_PIPS_GRID:
                done += 1

                if (
                    done % 1000
                    == 0
                ):
                    STATUS.update({
                        "state":
                            "precomputing",
                        "message":
                            (
                                "Caching reusable outcomes "
                                f"{done}/{total}"
                            ),
                        "outcomes_done":
                            done,
                        "outcomes_total":
                            total,
                    })

                cache[
                    (
                        signal_index,
                        rr,
                        cost,
                    )
                ] = compute_trade_outcome(
                    candles,
                    signal_index,
                    rr,
                    cost,
                )

    return cache


# ============================================================
# FILTER EVALUATION
# ============================================================

def state_close_above_ema(
    state,
    length,
):
    if state is None:
        return False

    value = state[
        "emas"
    ].get(
        length
    )

    return (
        value is not None
        and
        state[
            "close"
        ]
        >
        value
    )


def state_ema_alignment(
    state,
    fast,
    slow,
):
    if state is None:
        return False

    fast_value = state[
        "emas"
    ].get(
        fast
    )

    slow_value = state[
        "emas"
    ].get(
        slow
    )

    return (
        fast_value is not None
        and
        slow_value is not None
        and
        fast_value
        >
        slow_value
    )


def signal_passes(
    signal,
    config,
):
    if (
        signal[
            "body_ratio"
        ]
        <
        config[
            "minimum_body_ratio"
        ]
    ):
        return False

    if (
        config[
            "minimum_body_atr"
        ] is not None
        and
        signal[
            "body_atr"
        ]
        <
        config[
            "minimum_body_atr"
        ]
    ):
        return False

    if (
        config[
            "minimum_range_atr"
        ] is not None
        and
        signal[
            "range_atr"
        ]
        <
        config[
            "minimum_range_atr"
        ]
    ):
        return False

    if (
        config[
            "minimum_close_location"
        ] is not None
        and
        signal[
            "close_location"
        ]
        <
        config[
            "minimum_close_location"
        ]
    ):
        return False

    if (
        config[
            "minimum_lower_wick_body"
        ] is not None
        and
        signal[
            "lower_wick_body"
        ]
        <
        config[
            "minimum_lower_wick_body"
        ]
    ):
        return False

    lookback = config[
        "structure_lookback"
    ]

    if (
        lookback is not None
        and
        signal[
            "structure_distance_atr"
        ][
            lookback
        ]
        >
        config[
            "maximum_distance_atr"
        ]
    ):
        return False

    if (
        config[
            "minimum_m15_atr_ratio"
        ] is not None
    ):
        if (
            signal[
                "m15_atr_ratio"
            ] is None
            or
            signal[
                "m15_atr_ratio"
            ]
            <
            config[
                "minimum_m15_atr_ratio"
            ]
        ):
            return False

    if (
        config[
            "maximum_m15_atr_ratio"
        ] is not None
    ):
        if (
            signal[
                "m15_atr_ratio"
            ] is None
            or
            signal[
                "m15_atr_ratio"
            ]
            >
            config[
                "maximum_m15_atr_ratio"
            ]
        ):
            return False

    for key in [
        "4h",
        "12h",
        "24h",
    ]:
        value = signal[
            f"momentum_{key}_atr"
        ]

        minimum = config[
            f"minimum_momentum_{key}_atr"
        ]

        maximum = config[
            f"maximum_momentum_{key}_atr"
        ]

        if (
            minimum is not None
            and
            value < minimum
        ):
            return False

        if (
            maximum is not None
            and
            value > maximum
        ):
            return False

    if (
        config[
            "maximum_stop_atr"
        ] is not None
        and
        signal[
            "stop_atr"
        ]
        >
        config[
            "maximum_stop_atr"
        ]
    ):
        return False

    if (
        config[
            "minimum_stop_atr"
        ] is not None
        and
        signal[
            "stop_atr"
        ]
        <
        config[
            "minimum_stop_atr"
        ]
    ):
        return False

    included_hours = config[
        "included_ny_hours"
    ]

    if (
        included_hours is not None
        and
        signal[
            "ny_hour"
        ]
        not in included_hours
    ):
        return False

    if (
        signal[
            "ny_hour"
        ]
        in
        config[
            "excluded_ny_hours"
        ]
    ):
        return False

    if (
        signal[
            "ny_weekday"
        ]
        in
        config[
            "excluded_weekdays"
        ]
    ):
        return False

    if (
        config[
            "h1_close_above_ema"
        ] is not None
        and
        not state_close_above_ema(
            signal[
                "h1_prev"
            ],
            config[
                "h1_close_above_ema"
            ],
        )
    ):
        return False

    if (
        config[
            "h1_fast_ema"
        ] is not None
        and
        config[
            "h1_slow_ema"
        ] is not None
        and
        not state_ema_alignment(
            signal[
                "h1_prev"
            ],
            config[
                "h1_fast_ema"
            ],
            config[
                "h1_slow_ema"
            ],
        )
    ):
        return False

    if (
        config[
            "minimum_h1_atr_ratio"
        ] is not None
    ):
        state = signal[
            "h1_prev"
        ]

        if (
            state is None
            or
            state[
                "atr_ratio_50"
            ] is None
            or
            state[
                "atr_ratio_50"
            ]
            <
            config[
                "minimum_h1_atr_ratio"
            ]
        ):
            return False

    if (
        config[
            "daily_close_above_ema"
        ] is not None
        and
        not state_close_above_ema(
            signal[
                "daily_prev"
            ],
            config[
                "daily_close_above_ema"
            ],
        )
    ):
        return False

    if (
        config[
            "daily_fast_ema"
        ] is not None
        and
        config[
            "daily_slow_ema"
        ] is not None
        and
        not state_ema_alignment(
            signal[
                "daily_prev"
            ],
            config[
                "daily_fast_ema"
            ],
            config[
                "daily_slow_ema"
            ],
        )
    ):
        return False

    if (
        config[
            "minimum_daily_atr_ratio"
        ] is not None
    ):
        state = signal[
            "daily_prev"
        ]

        if (
            state is None
            or
            state[
                "atr_ratio_50"
            ] is None
            or
            state[
                "atr_ratio_50"
            ]
            <
            config[
                "minimum_daily_atr_ratio"
            ]
        ):
            return False

    return True


def run_config_cached(
    signals,
    cache,
    config,
    cost_pips,
    start=None,
    end=None,
):
    candidates = [
        signal
        for signal in signals
        if (
            signal_passes(
                signal,
                config,
            )
            and
            (
                start is None
                or
                signal[
                    "time"
                ]
                >=
                start
            )
            and
            (
                end is None
                or
                signal[
                    "time"
                ]
                <
                end
            )
        )
    ]

    indices = [
        signal[
            "signal_index"
        ]
        for signal in candidates
    ]

    trades = []

    position = 0

    while position < len(
        candidates
    ):
        signal = candidates[
            position
        ]

        trade = cache.get(
            (
                signal[
                    "signal_index"
                ],
                config[
                    "reward_risk"
                ],
                cost_pips,
            )
        )

        if trade is None:
            position += 1
            continue

        trades.append(
            dict(
                trade
            )
        )

        position = bisect.bisect_left(
            indices,
            trade[
                "exit_index"
            ],
            lo=position + 1,
        )

    return trades


# ============================================================
# STATS
# ============================================================

def stats_from_trades(
    trades,
):
    results = [
        float(
            trade[
                "result_r"
            ]
        )
        for trade in trades
    ]

    winners = [
        value
        for value in results
        if value > 0
    ]

    losers = [
        value
        for value in results
        if value < 0
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

    pf = (
        gross_profit
        /
        gross_loss
        if gross_loss > 0
        else (
            999.0
            if gross_profit > 0
            else 0.0
        )
    )

    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    current_streak = 0
    longest_streak = 0

    for value in results:
        equity += value

        peak = max(
            peak,
            equity,
        )

        max_dd = min(
            max_dd,
            equity - peak,
        )

        if value < 0:
            current_streak += 1
            longest_streak = max(
                longest_streak,
                current_streak,
            )
        else:
            current_streak = 0

    return {
        "trades":
            len(
                results
            ),
        "winners":
            len(
                winners
            ),
        "losers":
            len(
                losers
            ),
        "win_rate":
            (
                len(
                    winners
                )
                /
                len(
                    results
                )
                * 100.0
                if results
                else 0.0
            ),
        "profit_factor":
            pf,
        "total_r":
            total_r,
        "expectancy_r":
            (
                total_r
                /
                len(
                    results
                )
                if results
                else 0.0
            ),
        "max_drawdown_r":
            max_dd,
        "longest_loss_streak":
            longest_streak,
    }


def result_row(
    family,
    config,
    cost,
    trades,
):
    stats = stats_from_trades(
        trades
    )

    return {
        "family":
            family,
        "candidate":
            config[
                "label"
            ],
        "cost_pips":
            cost,
        "trades":
            stats[
                "trades"
            ],
        "winners":
            stats[
                "winners"
            ],
        "losers":
            stats[
                "losers"
            ],
        "win_rate":
            round(
                stats[
                    "win_rate"
                ],
                4,
            ),
        "profit_factor":
            round(
                stats[
                    "profit_factor"
                ],
                6,
            ),
        "total_r":
            round(
                stats[
                    "total_r"
                ],
                4,
            ),
        "expectancy_r":
            round(
                stats[
                    "expectancy_r"
                ],
                6,
            ),
        "max_drawdown_r":
            round(
                stats[
                    "max_drawdown_r"
                ],
                4,
            ),
        "longest_loss_streak":
            stats[
                "longest_loss_streak"
            ],
        "minimum_body_ratio":
            config[
                "minimum_body_ratio"
            ],
        "minimum_body_atr":
            config[
                "minimum_body_atr"
            ],
        "minimum_range_atr":
            config[
                "minimum_range_atr"
            ],
        "minimum_close_location":
            config[
                "minimum_close_location"
            ],
        "minimum_lower_wick_body":
            config[
                "minimum_lower_wick_body"
            ],
        "structure_lookback":
            config[
                "structure_lookback"
            ],
        "maximum_distance_atr":
            config[
                "maximum_distance_atr"
            ],
        "minimum_m15_atr_ratio":
            config[
                "minimum_m15_atr_ratio"
            ],
        "maximum_m15_atr_ratio":
            config[
                "maximum_m15_atr_ratio"
            ],
        "minimum_momentum_4h_atr":
            config[
                "minimum_momentum_4h_atr"
            ],
        "maximum_momentum_4h_atr":
            config[
                "maximum_momentum_4h_atr"
            ],
        "minimum_momentum_12h_atr":
            config[
                "minimum_momentum_12h_atr"
            ],
        "maximum_momentum_12h_atr":
            config[
                "maximum_momentum_12h_atr"
            ],
        "minimum_momentum_24h_atr":
            config[
                "minimum_momentum_24h_atr"
            ],
        "maximum_momentum_24h_atr":
            config[
                "maximum_momentum_24h_atr"
            ],
        "maximum_stop_atr":
            config[
                "maximum_stop_atr"
            ],
        "minimum_stop_atr":
            config[
                "minimum_stop_atr"
            ],
        "included_ny_hours":
            (
                None
                if config[
                    "included_ny_hours"
                ] is None
                else ",".join(
                    str(
                        hour
                    )
                    for hour in sorted(
                        config[
                            "included_ny_hours"
                        ]
                    )
                )
            ),
        "excluded_ny_hours":
            ",".join(
                str(
                    hour
                )
                for hour in sorted(
                    config[
                        "excluded_ny_hours"
                    ]
                )
            ),
        "excluded_weekdays":
            ",".join(
                str(
                    day
                )
                for day in sorted(
                    config[
                        "excluded_weekdays"
                    ]
                )
            ),
        "h1_close_above_ema":
            config[
                "h1_close_above_ema"
            ],
        "h1_fast_ema":
            config[
                "h1_fast_ema"
            ],
        "h1_slow_ema":
            config[
                "h1_slow_ema"
            ],
        "minimum_h1_atr_ratio":
            config[
                "minimum_h1_atr_ratio"
            ],
        "daily_close_above_ema":
            config[
                "daily_close_above_ema"
            ],
        "daily_fast_ema":
            config[
                "daily_fast_ema"
            ],
        "daily_slow_ema":
            config[
                "daily_slow_ema"
            ],
        "minimum_daily_atr_ratio":
            config[
                "minimum_daily_atr_ratio"
            ],
        "reward_risk":
            config[
                "reward_risk"
            ],
    }


# ============================================================
# PHASE A - ABLATION
# ============================================================

def ablation_configs():
    rows = []

    base = clone_config(
        INCUMBENT,
        "ABLATE_NONE",
    )

    rows.append(
        (
            "ABLATION",
            base,
        )
    )

    config = clone_config(
        INCUMBENT,
        "ABLATE_BODY_ATR",
    )

    config[
        "minimum_body_atr"
    ] = None

    rows.append(
        (
            "ABLATION",
            config,
        )
    )

    config = clone_config(
        INCUMBENT,
        "ABLATE_STRUCTURE",
    )

    config[
        "structure_lookback"
    ] = None

    config[
        "maximum_distance_atr"
    ] = None

    rows.append(
        (
            "ABLATION",
            config,
        )
    )

    config = clone_config(
        INCUMBENT,
        "ABLATE_BR",
    )

    config[
        "minimum_body_ratio"
    ] = 0.0

    rows.append(
        (
            "ABLATION",
            config,
        )
    )

    for rr in [
        3.25,
        3.50,
        3.75,
        4.00,
        4.25,
    ]:
        config = clone_config(
            INCUMBENT,
            f"ABLATE_RR_{rr:.2f}",
        )

        config[
            "reward_risk"
        ] = rr

        rows.append(
            (
                "ABLATION_RR",
                config,
            )
        )

    return rows


# ============================================================
# PHASE B - SINGLE-FAMILY OVERLAYS
# ============================================================

def single_family_configs():
    rows = []

    def add(
        family,
        label,
        mutator,
    ):
        config = clone_config(
            INCUMBENT,
            label,
        )

        mutator(
            config
        )

        rows.append(
            (
                family,
                config,
            )
        )

    for value in [
        1.05,
        1.10,
        1.20,
        1.30,
        1.40,
    ]:
        add(
            "BODY_RATIO",
            f"BR_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_body_ratio",
                    v,
                ),
        )

    for value in [
        0.60,
        0.65,
        0.70,
        0.75,
        0.80,
        0.85,
        0.90,
        1.00,
    ]:
        add(
            "BODY_ATR",
            f"BA_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_body_atr",
                    v,
                ),
        )

    for value in [
        0.80,
        1.00,
        1.20,
        1.40,
        1.60,
    ]:
        add(
            "RANGE_ATR",
            f"RA_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_range_atr",
                    v,
                ),
        )

    for value in [
        0.55,
        0.60,
        0.65,
        0.70,
        0.75,
        0.80,
        0.85,
    ]:
        add(
            "CLOSE_LOCATION",
            f"SC_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_close_location",
                    v,
                ),
        )

    for value in [
        0.10,
        0.20,
        0.30,
        0.40,
        0.50,
    ]:
        add(
            "LOWER_WICK",
            f"LW_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_lower_wick_body",
                    v,
                ),
        )

    for lookback in [
        100,
        120,
        135,
        150,
        165,
        180,
        200,
    ]:
        for distance in [
            0.05,
            0.075,
            0.10,
            0.125,
            0.15,
            0.20,
        ]:
            add(
                "STRUCTURE",
                (
                    f"S{lookback}_"
                    f"D{distance:.3f}"
                ),
                lambda c,
                lb=lookback,
                d=distance:
                    (
                        c.__setitem__(
                            "structure_lookback",
                            lb,
                        ),
                        c.__setitem__(
                            "maximum_distance_atr",
                            d,
                        )
                    ),
            )

    for value in [
        0.70,
        0.80,
        0.90,
        1.00,
        1.10,
        1.20,
        1.30,
    ]:
        add(
            "M15_ATR_MIN",
            f"M15ATR_MIN_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_m15_atr_ratio",
                    v,
                ),
        )

    for value in [
        0.80,
        0.90,
        1.00,
        1.10,
        1.20,
        1.30,
        1.50,
    ]:
        add(
            "M15_ATR_MAX",
            f"M15ATR_MAX_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "maximum_m15_atr_ratio",
                    v,
                ),
        )

    for horizon in [
        "4h",
        "12h",
        "24h",
    ]:
        for value in [
            -2.0,
            -1.0,
            -0.5,
            0.0,
            0.5,
            1.0,
        ]:
            key = (
                f"minimum_momentum_"
                f"{horizon}_atr"
            )

            add(
                f"MOM_{horizon}_MIN",
                (
                    f"MOM_{horizon}_"
                    f"MIN_{value:+.2f}"
                ),
                lambda c,
                k=key,
                v=value:
                    c.__setitem__(
                        k,
                        v,
                    ),
            )

        for value in [
            -1.0,
            -0.5,
            0.0,
            0.5,
            1.0,
            2.0,
        ]:
            key = (
                f"maximum_momentum_"
                f"{horizon}_atr"
            )

            add(
                f"MOM_{horizon}_MAX",
                (
                    f"MOM_{horizon}_"
                    f"MAX_{value:+.2f}"
                ),
                lambda c,
                k=key,
                v=value:
                    c.__setitem__(
                        k,
                        v,
                    ),
            )

    for value in [
        0.80,
        1.00,
        1.20,
        1.40,
        1.60,
        2.00,
    ]:
        add(
            "STOP_MAX",
            f"STOP_MAX_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "maximum_stop_atr",
                    v,
                ),
        )

    for value in [
        0.30,
        0.40,
        0.50,
        0.60,
        0.70,
    ]:
        add(
            "STOP_MIN",
            f"STOP_MIN_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_stop_atr",
                    v,
                ),
        )

    # Session inclusions.
    hour_sets = {
        "NY_07_11":
            set(
                range(
                    7,
                    12,
                )
            ),
        "NY_08_12":
            set(
                range(
                    8,
                    13,
                )
            ),
        "NY_08_15":
            set(
                range(
                    8,
                    16,
                )
            ),
        "NY_09_13":
            set(
                range(
                    9,
                    14,
                )
            ),
        "NY_12_16":
            set(
                range(
                    12,
                    17,
                )
            ),
        "NY_00_05":
            set(
                range(
                    0,
                    6,
                )
            ),
        "NY_02_06":
            set(
                range(
                    2,
                    7,
                )
            ),
    }

    for label, hours in hour_sets.items():
        add(
            "SESSION_INCLUDE",
            label,
            lambda c, h=hours:
                c.__setitem__(
                    "included_ny_hours",
                    set(
                        h
                    ),
                ),
        )

    # Single-hour exclusions.
    for hour in range(
        24
    ):
        add(
            "HOUR_EXCLUDE",
            f"EXCLUDE_H{hour:02d}",
            lambda c, h=hour:
                c.__setitem__(
                    "excluded_ny_hours",
                    {
                        h
                    },
                ),
        )

    weekday_names = {
        0:
            "MON",
        1:
            "TUE",
        2:
            "WED",
        3:
            "THU",
        4:
            "FRI",
    }

    for day in range(
        5
    ):
        add(
            "WEEKDAY_EXCLUDE",
            (
                "EXCLUDE_"
                + weekday_names[
                    day
                ]
            ),
            lambda c, d=day:
                c.__setitem__(
                    "excluded_weekdays",
                    {
                        d
                    },
                ),
        )

    for length in H1_EMA_LENGTHS:
        add(
            "H1_CLOSE_EMA",
            f"H1_CLOSE_GT_EMA{length}",
            lambda c, v=length:
                c.__setitem__(
                    "h1_close_above_ema",
                    v,
                ),
        )

    for fast, slow in H1_ALIGNMENT_PAIRS:
        def mutate_h1_alignment(
            c,
            f=fast,
            s=slow,
        ):
            c[
                "h1_fast_ema"
            ] = f

            c[
                "h1_slow_ema"
            ] = s

        add(
            "H1_ALIGNMENT",
            f"H1_EMA{fast}_GT_{slow}",
            mutate_h1_alignment,
        )

    for value in [
        0.70,
        0.80,
        0.90,
        1.00,
        1.10,
        1.20,
    ]:
        add(
            "H1_ATR",
            f"H1_ATR_MIN_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_h1_atr_ratio",
                    v,
                ),
        )

    for length in DAILY_EMA_LENGTHS:
        add(
            "DAILY_CLOSE_EMA",
            f"D_CLOSE_GT_EMA{length}",
            lambda c, v=length:
                c.__setitem__(
                    "daily_close_above_ema",
                    v,
                ),
        )

    for fast, slow in DAILY_ALIGNMENT_PAIRS:
        def mutate_daily_alignment(
            c,
            f=fast,
            s=slow,
        ):
            c[
                "daily_fast_ema"
            ] = f

            c[
                "daily_slow_ema"
            ] = s

        add(
            "DAILY_ALIGNMENT",
            f"D_EMA{fast}_GT_{slow}",
            mutate_daily_alignment,
        )

    for value in [
        0.70,
        0.80,
        0.90,
        1.00,
        1.10,
        1.20,
    ]:
        add(
            "DAILY_ATR",
            f"D_ATR_MIN_{value:.2f}",
            lambda c, v=value:
                c.__setitem__(
                    "minimum_daily_atr_ratio",
                    v,
                ),
        )

    for rr in RR_VALUES:
        add(
            "RR",
            f"RR_{rr:.2f}",
            lambda c, v=rr:
                c.__setitem__(
                    "reward_risk",
                    v,
                ),
        )

    return rows


# ============================================================
# PHASE C - CONTROLLED INTERACTIONS
# ============================================================

def config_signature(
    config,
):
    return tuple(
        sorted(
            (
                key,
                tuple(
                    sorted(
                        value
                    )
                )
                if isinstance(
                    value,
                    set,
                )
                else value,
            )
            for key, value in config.items()
            if key != "label"
        )
    )


def build_controlled_interactions(
    single_rows,
    family_best,
):
    # family_best:
    # list of (family, config)
    #
    # We only pair top independently useful filters from
    # DIFFERENT families, and cap the pool to avoid data-mining.
    pool = []

    seen_families = set()

    for family, config in family_best:
        if family in seen_families:
            continue

        seen_families.add(
            family
        )

        pool.append(
            (
                family,
                config,
            )
        )

        if len(
            pool
        ) >= 10:
            break

    interactions = []
    seen = set()
    counter = 0

    def overlay(
        base,
        source,
    ):
        result = clone_config(
            base,
            base[
                "label"
            ],
        )

        for key, value in source.items():
            if key == "label":
                continue

            incumbent_value = INCUMBENT.get(
                key
            )

            if value != incumbent_value:
                if isinstance(
                    value,
                    set,
                ):
                    result[
                        key
                    ] = set(
                        value
                    )
                else:
                    result[
                        key
                    ] = value

        return result

    for i in range(
        len(
            pool
        )
    ):
        for j in range(
            i + 1,
            len(
                pool
            ),
        ):
            family_a, config_a = pool[
                i
            ]

            family_b, config_b = pool[
                j
            ]

            config = clone_config(
                INCUMBENT,
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

            signature = config_signature(
                config
            )

            if signature in seen:
                continue

            seen.add(
                signature
            )

            counter += 1

            config[
                "label"
            ] = (
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
# VALIDATION WINDOWS
# ============================================================

def era_windows():
    return [
        (
            "ERA_2010_2013",
            datetime(
                2010,
                1,
                1,
                tzinfo=timezone.utc,
            ),
            datetime(
                2014,
                1,
                1,
                tzinfo=timezone.utc,
            ),
        ),
        (
            "ERA_2014_2017",
            datetime(
                2014,
                1,
                1,
                tzinfo=timezone.utc,
            ),
            datetime(
                2018,
                1,
                1,
                tzinfo=timezone.utc,
            ),
        ),
        (
            "ERA_2018_2021",
            datetime(
                2018,
                1,
                1,
                tzinfo=timezone.utc,
            ),
            datetime(
                2022,
                1,
                1,
                tzinfo=timezone.utc,
            ),
        ),
        (
            "ERA_2022_NOW",
            datetime(
                2022,
                1,
                1,
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
                2010,
                1,
                1,
                tzinfo=timezone.utc,
            ),
            datetime(
                2018,
                1,
                1,
                tzinfo=timezone.utc,
            ),
        ),
        (
            "VALIDATION_2018_NOW",
            datetime(
                2018,
                1,
                1,
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
                RESEARCH_TO,
                5,
            ),
            RESEARCH_TO,
        ),
        (
            "LAST_2Y",
            years_ago_safe(
                RESEARCH_TO,
                2,
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
        for (
            label,
            start,
            end,
        ) in windows:
            trades = run_config_cached(
                signals,
                cache,
                config,
                PRIMARY_COST_PIPS,
                start=start,
                end=end,
            )

            stats = stats_from_trades(
                trades
            )

            rows.append({
                "rank":
                    rank,
                "window":
                    label,
                "candidate":
                    config[
                        "label"
                    ],
                "trades":
                    stats[
                        "trades"
                    ],
                "profit_factor":
                    round(
                        stats[
                            "profit_factor"
                        ],
                        6,
                    ),
                "total_r":
                    round(
                        stats[
                            "total_r"
                        ],
                        4,
                    ),
                "expectancy_r":
                    round(
                        stats[
                            "expectancy_r"
                        ],
                        6,
                    ),
                "max_drawdown_r":
                    round(
                        stats[
                            "max_drawdown_r"
                        ],
                        4,
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

    cursor = month_start(
        RESEARCH_FROM
    )

    last_start = add_months(
        month_start(
            RESEARCH_TO
        ),
        -months,
    )

    while (
        cursor
        <=
        last_start
    ):
        end = add_months(
            cursor,
            months,
        )

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

        stats = stats_from_trades(
            trades
        )

        rows.append({
            "candidate":
                config[
                    "label"
                ],
            "months":
                months,
            "window":
                (
                    f"{cursor:%Y-%m-%d}"
                    " -> "
                    f"{end:%Y-%m-%d}"
                ),
            "trades":
                stats[
                    "trades"
                ],
            "profit_factor":
                round(
                    stats[
                        "profit_factor"
                    ],
                    6,
                ),
            "total_r":
                round(
                    stats[
                        "total_r"
                    ],
                    4,
                ),
            "expectancy_r":
                round(
                    stats[
                        "expectancy_r"
                    ],
                    6,
                ),
            "max_drawdown_r":
                round(
                    stats[
                        "max_drawdown_r"
                    ],
                    4,
                ),
            "positive":
                (
                    stats[
                        "total_r"
                    ]
                    > 0
                ),
        })

        cursor = add_months(
            cursor,
            1,
        )

    return rows


def median(
    values,
):
    ordered = sorted(
        values
    )

    n = len(
        ordered
    )

    if n == 0:
        return None

    if n % 2:
        return ordered[
            n // 2
        ]

    return (
        ordered[
            n // 2 - 1
        ]
        +
        ordered[
            n // 2
        ]
    ) / 2.0


def rolling_summary(
    rows,
):
    if not rows:
        return {}

    pfs = [
        float(
            row[
                "profit_factor"
            ]
        )
        for row in rows
    ]

    rs = [
        float(
            row[
                "total_r"
            ]
        )
        for row in rows
    ]

    positive = sum(
        1
        for row in rows
        if row[
            "positive"
        ]
    )

    worst_pf = min(
        rows,
        key=lambda row:
            float(
                row[
                    "profit_factor"
                ]
            ),
    )

    worst_r = min(
        rows,
        key=lambda row:
            float(
                row[
                    "total_r"
                ]
            ),
    )

    return {
        "candidate":
            rows[
                0
            ][
                "candidate"
            ],
        "months":
            rows[
                0
            ][
                "months"
            ],
        "windows":
            len(
                rows
            ),
        "positive_windows":
            positive,
        "positive_windows_pct":
            round(
                positive
                /
                len(
                    rows
                )
                * 100.0,
                4,
            ),
        "worst_profit_factor":
            round(
                min(
                    pfs
                ),
                6,
            ),
        "median_profit_factor":
            round(
                median(
                    pfs
                ),
                6,
            ),
        "worst_total_r":
            round(
                min(
                    rs
                ),
                4,
            ),
        "median_total_r":
            round(
                median(
                    rs
                ),
                4,
            ),
        "worst_pf_window":
            worst_pf[
                "window"
            ],
        "worst_r_window":
            worst_r[
                "window"
            ],
    }


# ============================================================
# OVERLAP / EXCLUSIVE
# ============================================================

def trade_key(
    trade,
):
    return (
        trade[
            "entry_time_utc"
        ],
        trade[
            "exit_time_utc"
        ],
    )


def overlap_rows(
    signals,
    cache,
    incumbent,
    finalists,
):
    incumbent_trades = run_config_cached(
        signals,
        cache,
        incumbent,
        PRIMARY_COST_PIPS,
    )

    incumbent_keys = {
        trade_key(
            trade
        )
        for trade in incumbent_trades
    }

    rows = []

    incumbent_stats = stats_from_trades(
        incumbent_trades
    )

    rows.append({
        "comparison":
            "INCUMBENT_ALL",
        "candidate":
            incumbent[
                "label"
            ],
        "trades":
            incumbent_stats[
                "trades"
            ],
        "profit_factor":
            round(
                incumbent_stats[
                    "profit_factor"
                ],
                6,
            ),
        "total_r":
            round(
                incumbent_stats[
                    "total_r"
                ],
                4,
            ),
        "expectancy_r":
            round(
                incumbent_stats[
                    "expectancy_r"
                ],
                6,
            ),
    })

    for finalist in finalists:
        trades = run_config_cached(
            signals,
            cache,
            finalist,
            PRIMARY_COST_PIPS,
        )

        keys = {
            trade_key(
                trade
            )
            for trade in trades
        }

        shared = (
            keys
            &
            incumbent_keys
        )

        exclusive = (
            keys
            -
            incumbent_keys
        )

        removed = (
            incumbent_keys
            -
            keys
        )

        subsets = [
            (
                "FINALIST_ALL",
                trades,
            ),
            (
                "FINALIST_SHARED",
                [
                    trade
                    for trade in trades
                    if trade_key(
                        trade
                    )
                    in shared
                ],
            ),
            (
                "FINALIST_EXCLUSIVE",
                [
                    trade
                    for trade in trades
                    if trade_key(
                        trade
                    )
                    in exclusive
                ],
            ),
            (
                "INCUMBENT_REMOVED",
                [
                    trade
                    for trade in incumbent_trades
                    if trade_key(
                        trade
                    )
                    in removed
                ],
            ),
        ]

        for subset_name, subset in subsets:
            stats = stats_from_trades(
                subset
            )

            rows.append({
                "comparison":
                    subset_name,
                "candidate":
                    finalist[
                        "label"
                    ],
                "trades":
                    stats[
                        "trades"
                    ],
                "profit_factor":
                    round(
                        stats[
                            "profit_factor"
                        ],
                        6,
                    ),
                "total_r":
                    round(
                        stats[
                            "total_r"
                        ],
                        4,
                    ),
                "expectancy_r":
                    round(
                        stats[
                            "expectancy_r"
                        ],
                        6,
                    ),
            })

    return rows


# ============================================================
# RUNNER
# ============================================================

def run_research():
    try:
        # --------------------
        # DATA
        # --------------------

        m15 = fetch_history(
            "M15",
            RESEARCH_FROM,
            RESEARCH_TO,
            30,
        )

        h1 = fetch_history(
            "H1",
            RESEARCH_FROM
            - timedelta(
                days=500
            ),
            RESEARCH_TO,
            120,
        )

        daily = fetch_history(
            "D",
            RESEARCH_FROM
            - timedelta(
                days=1200
            ),
            RESEARCH_TO,
            2500,
            daily_alignment=True,
        )

        if len(
            m15
        ) < 1000:
            raise RuntimeError(
                "Too few M15 candles returned"
            )

        STATUS.update({
            "state":
                "precomputing",
            "message":
                "Building ATR, H1/Daily state and M15 signal features",
            "m15_candles":
                len(
                    m15
                ),
            "h1_candles":
                len(
                    h1
                ),
            "daily_candles":
                len(
                    daily
                ),
        })

        m15_atr = atr14(
            m15
        )

        h1_state = build_htf_state(
            h1,
            H1_EMA_LENGTHS,
        )

        daily_state = build_htf_state(
            daily,
            DAILY_EMA_LENGTHS,
        )

        signals = build_signal_cache(
            m15,
            m15_atr,
            h1_state,
            daily_state,
        )

        STATUS.update({
            "state":
                "precomputing",
            "message":
                "Caching reusable trade outcomes",
            "engulfing_signals":
                len(
                    signals
                ),
        })

        cache = build_outcome_cache(
            m15,
            signals,
        )

        # --------------------
        # PHASE A
        # --------------------

        ablations = ablation_configs()

        ablation_rows = []

        for family, config in ablations:
            for cost in COST_PIPS_GRID:
                trades = run_config_cached(
                    signals,
                    cache,
                    config,
                    cost,
                )

                ablation_rows.append(
                    result_row(
                        family,
                        config,
                        cost,
                        trades,
                    )
                )

        write_csv(
            OUTPUT_ABLATION,
            ablation_rows,
        )

        # --------------------
        # PHASE B
        # --------------------

        singles = single_family_configs()

        STATUS.update({
            "state":
                "calculating",
            "message":
                "Running single-family overlays",
            "single_configs":
                len(
                    singles
                ),
        })

        single_rows = []

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
                    cache,
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

            if (
                number % 25
                == 0
            ):
                STATUS.update({
                    "state":
                        "calculating",
                    "message":
                        (
                            "Single-family overlays "
                            f"{number}/{len(singles)}"
                        ),
                })

        write_csv(
            OUTPUT_SINGLE,
            single_rows,
        )

        primary_single = [
            row
            for row in single_rows
            if (
                abs(
                    float(
                        row[
                            "cost_pips"
                        ]
                    )
                    -
                    PRIMARY_COST_PIPS
                )
                < 1e-12
                and
                int(
                    row[
                        "trades"
                    ]
                )
                >= 80
            )
        ]

        primary_single.sort(
            key=lambda row:
                (
                    float(
                        row[
                            "profit_factor"
                        ]
                    ),
                    float(
                        row[
                            "expectancy_r"
                        ]
                    ),
                    float(
                        row[
                            "total_r"
                        ]
                    ),
                ),
            reverse=True,
        )

        write_csv(
            OUTPUT_SINGLE_TOP,
            primary_single[
                :100
            ],
        )

        config_by_label = {
            config[
                "label"
            ]:
                config
            for _, config in singles
        }

        # Best independently useful result from each family.
        # Require >=80 trades and PF > incumbent-ish floor 1.45.
        family_best_rows = {}

        for row in primary_single:
            family = row[
                "family"
            ]

            if family in family_best_rows:
                continue

            if (
                float(
                    row[
                        "profit_factor"
                    ]
                )
                >= 1.45
            ):
                family_best_rows[
                    family
                ] = row

        family_best = []

        for family, row in sorted(
            family_best_rows.items(),
            key=lambda item:
                float(
                    item[
                        1
                    ][
                        "profit_factor"
                    ]
                ),
            reverse=True,
        ):
            family_best.append(
                (
                    family,
                    config_by_label[
                        row[
                            "candidate"
                        ]
                    ],
                )
            )

        # --------------------
        # PHASE C
        # --------------------

        interactions = build_controlled_interactions(
            single_rows,
            family_best,
        )

        STATUS.update({
            "state":
                "calculating",
            "message":
                "Running controlled interactions",
            "interaction_configs":
                len(
                    interactions
                ),
        })

        interaction_rows = []

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
                    cache,
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

            if (
                number % 20
                == 0
            ):
                STATUS.update({
                    "state":
                        "calculating",
                    "message":
                        (
                            "Controlled interactions "
                            f"{number}/{len(interactions)}"
                        ),
                })

        write_csv(
            OUTPUT_INTERACTIONS,
            interaction_rows,
        )

        # --------------------
        # FINALIST POOL
        # --------------------

        incumbent_trades = run_config_cached(
            signals,
            cache,
            INCUMBENT,
            PRIMARY_COST_PIPS,
        )

        incumbent_row = result_row(
            "INCUMBENT",
            INCUMBENT,
            PRIMARY_COST_PIPS,
            incumbent_trades,
        )

        combined_primary = [
            incumbent_row
        ]

        combined_primary.extend(
            primary_single
        )

        combined_primary.extend(
            row
            for row in interaction_rows
            if (
                abs(
                    float(
                        row[
                            "cost_pips"
                        ]
                    )
                    -
                    PRIMARY_COST_PIPS
                )
                < 1e-12
                and
                int(
                    row[
                        "trades"
                    ]
                )
                >= 80
            )
        )

        combined_primary.sort(
            key=lambda row:
                (
                    float(
                        row[
                            "profit_factor"
                        ]
                    ),
                    float(
                        row[
                            "expectancy_r"
                        ]
                    ),
                    float(
                        row[
                            "total_r"
                        ]
                    ),
                ),
            reverse=True,
        )

        top_rows = combined_primary[
            :30
        ]

        write_csv(
            OUTPUT_TOP,
            top_rows,
        )

        all_configs = {
            "INCUMBENT":
                INCUMBENT
        }

        for _, config in singles:
            all_configs[
                config[
                    "label"
                ]
            ] = config

        for _, config in interactions:
            all_configs[
                config[
                    "label"
                ]
            ] = config

        finalists = [
            all_configs[
                row[
                    "candidate"
                ]
            ]
            for row in top_rows[
                :12
            ]
        ]

        # --------------------
        # PHASE D VALIDATION
        # --------------------

        era_rows = validation_rows(
            signals,
            cache,
            finalists,
            era_windows(),
        )

        devval_rows = validation_rows(
            signals,
            cache,
            finalists,
            devval_windows(),
        )

        recent_rows = validation_rows(
            signals,
            cache,
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

        # Robust score rewards worst-window strength more than raw PF.
        robust = []

        for config in finalists:
            label = config[
                "label"
            ]

            base = next(
                row
                for row in top_rows
                if row[
                    "candidate"
                ]
                ==
                label
            )

            era_subset = [
                row
                for row in era_rows
                if row[
                    "candidate"
                ]
                ==
                label
            ]

            devval_subset = [
                row
                for row in devval_rows
                if row[
                    "candidate"
                ]
                ==
                label
            ]

            recent_subset = [
                row
                for row in recent_rows
                if row[
                    "candidate"
                ]
                ==
                label
            ]

            era_pfs = [
                float(
                    row[
                        "profit_factor"
                    ]
                )
                for row in era_subset
                if int(
                    row[
                        "trades"
                    ]
                ) > 0
            ]

            devval_pfs = [
                float(
                    row[
                        "profit_factor"
                    ]
                )
                for row in devval_subset
                if int(
                    row[
                        "trades"
                    ]
                ) > 0
            ]

            recent_pfs = [
                float(
                    row[
                        "profit_factor"
                    ]
                )
                for row in recent_subset
                if int(
                    row[
                        "trades"
                    ]
                ) > 0
            ]

            min_era = (
                min(
                    era_pfs
                )
                if era_pfs
                else 0.0
            )

            min_devval = (
                min(
                    devval_pfs
                )
                if devval_pfs
                else 0.0
            )

            min_recent = (
                min(
                    recent_pfs
                )
                if recent_pfs
                else 0.0
            )

            robust.append({
                "candidate":
                    label,
                "full_pf":
                    float(
                        base[
                            "profit_factor"
                        ]
                    ),
                "full_expectancy":
                    float(
                        base[
                            "expectancy_r"
                        ]
                    ),
                "full_total_r":
                    float(
                        base[
                            "total_r"
                        ]
                    ),
                "trades":
                    int(
                        base[
                            "trades"
                        ]
                    ),
                "minimum_era_pf":
                    min_era,
                "minimum_devval_pf":
                    min_devval,
                "minimum_recent_pf":
                    min_recent,
                "score":
                    (
                        min_era
                        * 3.0
                        +
                        min_devval
                        * 2.0
                        +
                        min_recent
                        * 2.0
                        +
                        float(
                            base[
                                "profit_factor"
                            ]
                        )
                    ),
            })

        robust.sort(
            key=lambda row:
                row[
                    "score"
                ],
            reverse=True,
        )

        robust_finalists = [
            all_configs[
                row[
                    "candidate"
                ]
            ]
            for row in robust[
                :5
            ]
        ]

        rolling_rows = []
        rolling_summary_rows = []

        for config in robust_finalists:
            for months in [
                24,
                36,
            ]:
                rows = monthly_rolling_rows(
                    signals,
                    cache,
                    config,
                    months,
                )

                rolling_rows.extend(
                    rows
                )

                rolling_summary_rows.append(
                    rolling_summary(
                        rows
                    )
                )

        write_csv(
            OUTPUT_ROLLING,
            rolling_rows,
        )

        write_csv(
            OUTPUT_ROLLING_SUMMARY,
            rolling_summary_rows,
        )

        overlap = overlap_rows(
            signals,
            cache,
            INCUMBENT,
            robust_finalists,
        )

        write_csv(
            OUTPUT_OVERLAP,
            overlap,
        )

        best = (
            robust_finalists[
                0
            ]
            if robust_finalists
            else INCUMBENT
        )

        best_trades = run_config_cached(
            signals,
            cache,
            best,
            PRIMARY_COST_PIPS,
        )

        write_csv(
            OUTPUT_BEST_TRADES,
            best_trades,
        )

        STATUS.update({
            "state":
                "complete",
            "message":
                "EUR/USD M15 long exhaustive controlled pass complete",
            "m15_candles":
                len(
                    m15
                ),
            "h1_candles":
                len(
                    h1
                ),
            "daily_candles":
                len(
                    daily
                ),
            "engulfing_signals":
                len(
                    signals
                ),
            "single_configs":
                len(
                    singles
                ),
            "interaction_configs":
                len(
                    interactions
                ),
            "incumbent":
                incumbent_row,
            "robust_ranking":
                robust[
                    :10
                ],
            "selected_best":
                best,
            "outputs": {
                "ablation":
                    OUTPUT_ABLATION,
                "single_family":
                    OUTPUT_SINGLE,
                "single_top":
                    OUTPUT_SINGLE_TOP,
                "interactions":
                    OUTPUT_INTERACTIONS,
                "top":
                    OUTPUT_TOP,
                "eras":
                    OUTPUT_ERAS,
                "dev_validation":
                    OUTPUT_DEVVAL,
                "recent":
                    OUTPUT_RECENT,
                "rolling":
                    OUTPUT_ROLLING,
                "rolling_summary":
                    OUTPUT_ROLLING_SUMMARY,
                "overlap":
                    OUTPUT_OVERLAP,
                "best_trades":
                    OUTPUT_BEST_TRADES,
            },
        })

        print()
        print(
            "=" * 100
        )
        print(
            "EUR/USD M15 LONG EXHAUSTIVE CONTROLLED PASS COMPLETE"
        )
        print(
            "=" * 100
        )
        print(
            "Single-family configs:",
            len(
                singles
            ),
        )
        print(
            "Controlled interactions:",
            len(
                interactions
            ),
        )
        print(
            "Incumbent:",
            incumbent_row,
        )
        print(
            "Selected best:",
            best,
        )
        print(
            "Robust ranking:"
        )

        for row in robust[
            :10
        ]:
            print(
                row
            )

    except Exception as error:
        STATUS.update({
            "state":
                "error",
            "message":
                str(
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
def root():
    return jsonify({
        "service":
            "EURUSD M15 Long Exhaustive Controlled",
        "status":
            STATUS[
                "state"
            ],
        "instrument":
            INSTRUMENT,
        "timeframe":
            "M15",
        "side":
            "BUY",
        "orders_supported":
            False,
        "trading_enabled":
            False,
        "routes": [
            "/m15-long-exhaustive/status",
            "/m15-long-exhaustive/ablation",
            "/m15-long-exhaustive/single-family",
            "/m15-long-exhaustive/single-top",
            "/m15-long-exhaustive/interactions",
            "/m15-long-exhaustive/top",
            "/m15-long-exhaustive/eras",
            "/m15-long-exhaustive/dev-validation",
            "/m15-long-exhaustive/recent",
            "/m15-long-exhaustive/rolling",
            "/m15-long-exhaustive/rolling-summary",
            "/m15-long-exhaustive/overlap",
            "/m15-long-exhaustive/best-trades",
        ],
    })


@app.route(
    "/m15-long-exhaustive/status"
)
def route_status():
    return jsonify(
        STATUS
    )


@app.route(
    "/m15-long-exhaustive/ablation"
)
def route_ablation():
    return download_file(
        OUTPUT_ABLATION
    )


@app.route(
    "/m15-long-exhaustive/single-family"
)
def route_single_family():
    return download_file(
        OUTPUT_SINGLE
    )


@app.route(
    "/m15-long-exhaustive/single-top"
)
def route_single_top():
    return download_file(
        OUTPUT_SINGLE_TOP
    )


@app.route(
    "/m15-long-exhaustive/interactions"
)
def route_interactions():
    return download_file(
        OUTPUT_INTERACTIONS
    )


@app.route(
    "/m15-long-exhaustive/top"
)
def route_top():
    return download_file(
        OUTPUT_TOP
    )


@app.route(
    "/m15-long-exhaustive/eras"
)
def route_eras():
    return download_file(
        OUTPUT_ERAS
    )


@app.route(
    "/m15-long-exhaustive/dev-validation"
)
def route_devval():
    return download_file(
        OUTPUT_DEVVAL
    )


@app.route(
    "/m15-long-exhaustive/recent"
)
def route_recent():
    return download_file(
        OUTPUT_RECENT
    )


@app.route(
    "/m15-long-exhaustive/rolling"
)
def route_rolling():
    return download_file(
        OUTPUT_ROLLING
    )


@app.route(
    "/m15-long-exhaustive/rolling-summary"
)
def route_rolling_summary():
    return download_file(
        OUTPUT_ROLLING_SUMMARY
    )


@app.route(
    "/m15-long-exhaustive/overlap"
)
def route_overlap():
    return download_file(
        OUTPUT_OVERLAP
    )


@app.route(
    "/m15-long-exhaustive/best-trades"
)
def route_best_trades():
    return download_file(
        OUTPUT_BEST_TRADES
    )


if __name__ == "__main__":
    research_thread = threading.Thread(
        target=run_research,
        name=(
            "eurusd-m15-long-"
            "exhaustive-controlled"
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
