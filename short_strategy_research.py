import os
import threading
import requests
import pandas as pd

from flask import Flask, jsonify, send_file
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# EUR/GBP LONG - CURRENT LIVE CONTROL BASELINE
#
# RESEARCH ONLY - NEVER SUBMITS ORDERS.
#
# Exact current live EUR/GBP long:
#
# Bullish engulfing
# minimum body ratio >= 1.00
# strong close >= 0.75
# structure lookback 20
# distance <= 0.20 ATR14
# previous completed daily close > EMA150
# previous completed daily EMA20 > EMA150
# London session 08:00-16:59
# Thursday + Friday excluded
# RR 3.00
# stop buffer 10 ticks
# adverse historical fill 5 ticks
# pyramiding 0
#
# OANDA midpoint H1.
# Daily alignment 17:00 America/New_York.
# Previous completed daily candle only.
#
# Research window:
# 2002-05-06 20:00 UTC -> current completed UTC hour.
#
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

MINIMUM_BODY_RATIO = 1.00
MINIMUM_CLOSE_LOCATION = 0.75

ATR_LENGTH = 14

STRUCTURE_LOOKBACK = 20
MAXIMUM_DISTANCE_ATR = 0.20

FAST_DAILY_EMA = 20
SLOW_DAILY_EMA = 150

SESSION_TZ = ZoneInfo("Europe/London")
SESSION_START_HOUR = 8
SESSION_END_HOUR = 17

EXCLUDED_WEEKDAYS = {
    3,  # Thursday
    4,  # Friday
}

NY_TZ = ZoneInfo("America/New_York")

DAILY_ALIGNMENT_HOUR = 17
DAILY_ALIGNMENT_TIMEZONE = "America/New_York"

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

H1_CHUNK_DAYS = 180
D_CHUNK_DAYS = 1500

H1_WARMUP_DAYS = 200
D_WARMUP_DAYS = 2500

OUTPUT_SUMMARY = (
    "eurgbp_long_current_control_summary.csv"
)

OUTPUT_YEARLY = (
    "eurgbp_long_current_control_yearly.csv"
)

OUTPUT_ROLLING = (
    "eurgbp_long_current_control_rolling3y.csv"
)

OUTPUT_TRADES = (
    "eurgbp_long_current_control_trades.csv"
)


ERAS = [
    (
        "2002_2009",
        RESEARCH_FROM,
        datetime(
            2010, 1, 1,
            tzinfo=timezone.utc,
        ),
    ),
    (
        "2010_2017",
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
        "2018_2023",
        datetime(
            2018, 1, 1,
            tzinfo=timezone.utc,
        ),
        datetime(
            2024, 1, 1,
            tzinfo=timezone.utc,
        ),
    ),
    (
        "2024_present",
        datetime(
            2024, 1, 1,
            tzinfo=timezone.utc,
        ),
        None,
    ),
]


STATUS = {
    "state": "not_started",
    "message": "Research has not started",
    "service": "EUR/GBP Long Current Live Control Baseline",
    "instrument": INSTRUMENT,
    "orders_supported": False,
    "trading_enabled": False,
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
        "Authorization": (
            f"Bearer {OANDA_TOKEN}"
        )
    }


def iso_utc(dt):
    return (
        dt
        .astimezone(timezone.utc)
        .isoformat()
        .replace(
            "+00:00",
            "Z",
        )
    )


def oanda_get(
    path,
    params,
):
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
    if not raw.get(
        "complete",
        False,
    ):
        return None

    mid = raw.get("mid")

    if not mid:
        return None

    return {
        "time": datetime.fromisoformat(
            raw["time"].replace(
                "Z",
                "+00:00",
            )
        ),
        "open": float(mid["o"]),
        "high": float(mid["h"]),
        "low": float(mid["l"]),
        "close": float(mid["c"]),
    }


def fetch_range(
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
        "dailyAlignment": DAILY_ALIGNMENT_HOUR,
        "alignmentTimezone": DAILY_ALIGNMENT_TIMEZONE,
    }

    data = oanda_get(
        f"/v3/instruments/{INSTRUMENT}/candles",
        params,
    )

    candles = []

    for raw in data.get(
        "candles",
        [],
    ):
        candle = parse_candle(raw)

        if candle is not None:
            candles.append(candle)

    return candles


def fetch_chunked(
    granularity,
    start,
    end,
    chunk_days,
):
    by_time = {}
    cursor = start

    while cursor < end:
        chunk_end = min(
            cursor
            + timedelta(
                days=chunk_days
            ),
            end,
        )

        print(
            f"Fetching {granularity}: "
            f"{cursor.date()} -> {chunk_end.date()}",
            flush=True,
        )

        chunk = fetch_range(
            granularity,
            cursor,
            chunk_end,
        )

        for candle in chunk:
            by_time[
                candle["time"]
            ] = candle

        cursor = chunk_end

    candles = list(
        by_time.values()
    )

    candles.sort(
        key=lambda x: x["time"]
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
    result = [
        None
    ] * len(values)

    if len(values) < length:
        return result

    initial = (
        sum(
            values[:length]
        )
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
    length,
):
    return rma_series(
        true_ranges(candles),
        length,
    )


def ema_series(
    values,
    length,
):
    result = [
        None
    ] * len(values)

    if len(values) < length:
        return result

    initial = (
        sum(
            values[:length]
        )
        / length
    )

    result[
        length - 1
    ] = initial

    multiplier = (
        2.0
        / (
            length + 1.0
        )
    )

    previous = initial

    for index in range(
        length,
        len(values),
    ):
        current = (
            (
                values[index]
                - previous
            )
            * multiplier
            + previous
        )

        result[index] = current
        previous = current

    return result


# ============================================================
# DAILY STATE
# ============================================================

def current_daily_start(
    timestamp_utc
):
    ny_time = (
        timestamp_utc
        .astimezone(
            NY_TZ
        )
    )

    candidate = ny_time.replace(
        hour=DAILY_ALIGNMENT_HOUR,
        minute=0,
        second=0,
        microsecond=0,
    )

    if ny_time < candidate:
        candidate = (
            candidate
            - timedelta(
                days=1
            )
        )

    return candidate.astimezone(
        timezone.utc
    )


def build_daily_state(
    daily
):
    closes = [
        candle["close"]
        for candle in daily
    ]

    fast = ema_series(
        closes,
        FAST_DAILY_EMA,
    )

    slow = ema_series(
        closes,
        SLOW_DAILY_EMA,
    )

    result = []

    for index, candle in enumerate(
        daily
    ):
        result.append({
            "time": candle["time"],
            "close": candle["close"],
            "fast_ema": fast[index],
            "slow_ema": slow[index],
        })

    return result


def previous_completed_daily(
    signal_time,
    daily_state,
):
    session_start = (
        current_daily_start(
            signal_time
        )
    )

    selected = None

    for row in daily_state:
        if row["time"] < session_start:
            selected = row
        else:
            break

    return selected


# ============================================================
# SIGNALS
# ============================================================

def build_signals(
    h1,
    atr,
    daily_state,
):
    signals = []

    start_index = max(
        ATR_LENGTH,
        STRUCTURE_LOOKBACK,
    )

    for index in range(
        start_index,
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

        current_atr = atr[index]

        if (
            current_atr is None
            or current_atr <= 0
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

        signal_range = (
            signal["high"]
            - signal["low"]
        )

        if (
            previous_body <= 0
            or current_body <= 0
            or signal_range <= 0
        ):
            continue

        bullish_engulfing = (
            previous["close"]
            < previous["open"]
            and signal["close"]
            > signal["open"]
            and signal["open"]
            <= previous["close"]
            and signal["close"]
            >= previous["open"]
        )

        if not bullish_engulfing:
            continue

        body_ratio = (
            current_body
            / previous_body
        )

        if (
            body_ratio
            < MINIMUM_BODY_RATIO
        ):
            continue

        close_location = (
            signal["close"]
            - signal["low"]
        ) / signal_range

        if (
            close_location
            < MINIMUM_CLOSE_LOCATION
        ):
            continue

        previous_lowest_low = min(
            candle["low"]
            for candle
            in h1[
                index - STRUCTURE_LOOKBACK:
                index
            ]
        )

        distance = (
            signal["low"]
            - previous_lowest_low
        )

        if (
            distance
            > (
                current_atr
                * MAXIMUM_DISTANCE_ATR
            )
        ):
            continue

        daily = previous_completed_daily(
            signal["time"],
            daily_state,
        )

        if daily is None:
            continue

        if (
            daily["fast_ema"]
            is None
            or daily["slow_ema"]
            is None
        ):
            continue

        if not (
            daily["close"]
            > daily["slow_ema"]
        ):
            continue

        if not (
            daily["fast_ema"]
            > daily["slow_ema"]
        ):
            continue

        local = (
            signal["time"]
            .astimezone(
                SESSION_TZ
            )
        )

        if not (
            local.hour
            >= SESSION_START_HOUR
            and local.hour
            < SESSION_END_HOUR
        ):
            continue

        if (
            local.weekday()
            in EXCLUDED_WEEKDAYS
        ):
            continue

        signals.append({
            "index": index,
            "time": signal["time"],
        })

    return signals


# ============================================================
# SIMULATION
# ============================================================

def trade_from_signal(
    h1,
    signal_index,
):
    signal = h1[
        signal_index
    ]

    reference_entry = (
        signal["close"]
    )

    backtest_entry = (
        reference_entry
        + BACKTEST_SLIPPAGE_TICKS
        * TICK_SIZE
    )

    stop = (
        signal["low"]
        - STOP_BUFFER_TICKS
        * TICK_SIZE
    )

    reference_risk = (
        reference_entry
        - stop
    )

    if reference_risk <= 0:
        return None

    target = (
        reference_entry
        + reference_risk
        * REWARD_RISK
    )

    actual_risk = (
        backtest_entry
        - stop
    )

    if actual_risk <= 0:
        return None

    for index in range(
        signal_index + 1,
        len(h1),
    ):
        candle = h1[index]

        if candle["time"] >= RESEARCH_TO:
            break

        stop_hit = (
            candle["low"]
            <= stop
        )

        target_hit = (
            candle["high"]
            >= target
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
                exit_price = target
            else:
                exit_price = stop

        elif target_hit:
            exit_price = target

        else:
            exit_price = stop

        result_r = (
            exit_price
            - backtest_entry
        ) / actual_risk

        return {
            "signal_index": signal_index,
            "signal_time": signal["time"],
            "exit_index": index,
            "exit_time": candle["time"],
            "result_r": result_r,
        }

    return None


def simulate(
    h1,
    signals,
):
    trades = []
    ignored = 0
    position_exit_index = -1

    for signal in signals:
        signal_index = (
            signal["index"]
        )

        # Locked convention:
        # signals before the exit candle are ignored;
        # signal on the exact exit candle is allowed.
        if (
            signal_index
            < position_exit_index
        ):
            ignored += 1
            continue

        trade = trade_from_signal(
            h1,
            signal_index,
        )

        if trade is None:
            break

        trades.append(trade)

        position_exit_index = (
            trade["exit_index"]
        )

    return trades, ignored


# ============================================================
# STATS
# ============================================================

def stats_for_trades(
    trades,
    start=None,
    end=None,
):
    selected = []

    for trade in trades:
        t = trade[
            "signal_time"
        ]

        if (
            start is not None
            and t < start
        ):
            continue

        if (
            end is not None
            and t >= end
        ):
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

    results = [
        trade["result_r"]
        for trade in selected
    ]

    winners = [
        r for r in results
        if r > 0
    ]

    losers = [
        r for r in results
        if r < 0
    ]

    gross_profit = sum(winners)
    gross_loss = abs(sum(losers))

    if gross_loss > 0:
        pf = (
            gross_profit
            / gross_loss
        )
    elif gross_profit > 0:
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

        peak = max(
            peak,
            equity,
        )

        max_dd = min(
            max_dd,
            equity - peak,
        )

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
            len(winners)
            / len(results)
            * 100.0,
            2,
        ),
        "profit_factor": round(
            pf,
            3,
        ),
        "total_r": round(
            total_r,
            2,
        ),
        "expectancy_r": round(
            total_r
            / len(results),
            3,
        ),
        "max_drawdown_r": round(
            max_dd,
            2,
        ),
        "longest_loss_streak": (
            longest_streak
        ),
    }


def subtract_years_safe(
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


# ============================================================
# RUN
# ============================================================

def run_research():
    try:
        STATUS.update({
            "state": "fetching_h1",
            "message": (
                "Fetching EUR/GBP H1 history"
            ),
        })

        h1 = fetch_chunked(
            "H1",
            RESEARCH_FROM
            - timedelta(
                days=H1_WARMUP_DAYS
            ),
            RESEARCH_TO,
            H1_CHUNK_DAYS,
        )

        STATUS.update({
            "state": "fetching_daily",
            "message": (
                "Fetching EUR/GBP daily history"
            ),
        })

        daily = fetch_chunked(
            "D",
            RESEARCH_FROM
            - timedelta(
                days=D_WARMUP_DAYS
            ),
            RESEARCH_TO,
            D_CHUNK_DAYS,
        )

        if not h1:
            raise RuntimeError(
                "No H1 candles returned"
            )

        if not daily:
            raise RuntimeError(
                "No daily candles returned"
            )

        STATUS.update({
            "state": "calculating",
            "message": (
                "Running exact current live control"
            ),
        })

        h1_atr = atr_series(
            h1,
            ATR_LENGTH,
        )

        daily_state = (
            build_daily_state(
                daily
            )
        )

        signals = build_signals(
            h1,
            h1_atr,
            daily_state,
        )

        (
            trades,
            ignored,
        ) = simulate(
            h1,
            signals,
        )

        years = (
            RESEARCH_TO
            - RESEARCH_FROM
        ).total_seconds() / (
            365.2425
            * 86400
        )

        full = stats_for_trades(
            trades
        )

        summary = {
            "instrument": INSTRUMENT,
            "strategy": (
                "CURRENT_LIVE_CONTROL"
            ),
            "research_from": (
                RESEARCH_FROM.isoformat()
            ),
            "research_to": (
                RESEARCH_TO.isoformat()
            ),
            "eligible_signals": (
                len(signals)
            ),
            "ignored_due_to_open_trade": (
                ignored
            ),
            "trades_per_year": round(
                full["trades"]
                / years,
                2,
            ),
            **full,
            "annual_r_linear": round(
                full["total_r"]
                / years,
                3,
            ),
        }

        profitable_eras = 0
        minimum_era_pf = None

        for (
            era_name,
            era_start,
            era_end,
        ) in ERAS:
            end = (
                RESEARCH_TO
                if era_end is None
                else min(
                    era_end,
                    RESEARCH_TO,
                )
            )

            era = stats_for_trades(
                trades,
                era_start,
                end,
            )

            summary[
                f"{era_name}_trades"
            ] = era["trades"]

            summary[
                f"{era_name}_pf"
            ] = era[
                "profit_factor"
            ]

            summary[
                f"{era_name}_r"
            ] = era["total_r"]

            summary[
                f"{era_name}_expectancy"
            ] = era[
                "expectancy_r"
            ]

            if era["trades"] >= 5:
                if minimum_era_pf is None:
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

                if era["total_r"] > 0:
                    profitable_eras += 1

        summary[
            "minimum_era_pf_5_plus"
        ] = minimum_era_pf

        summary[
            "profitable_eras"
        ] = profitable_eras

        for years_back in [
            2,
            5,
            10,
        ]:
            start = subtract_years_safe(
                RESEARCH_TO,
                years_back,
            )

            recent = stats_for_trades(
                trades,
                start,
                RESEARCH_TO,
            )

            summary[
                f"last_{years_back}y_trades"
            ] = recent["trades"]

            summary[
                f"last_{years_back}y_pf"
            ] = recent[
                "profit_factor"
            ]

            summary[
                f"last_{years_back}y_r"
            ] = recent["total_r"]

            summary[
                f"last_{years_back}y_expectancy"
            ] = recent[
                "expectancy_r"
            ]

        yearly_rows = []

        for year in range(
            RESEARCH_FROM.year,
            RESEARCH_TO.year + 1,
        ):
            start = max(
                RESEARCH_FROM,
                datetime(
                    year,
                    1,
                    1,
                    tzinfo=timezone.utc,
                ),
            )

            end = min(
                RESEARCH_TO,
                datetime(
                    year + 1,
                    1,
                    1,
                    tzinfo=timezone.utc,
                ),
            )

            if start >= end:
                continue

            stats = stats_for_trades(
                trades,
                start,
                end,
            )

            yearly_rows.append({
                "year": year,
                **stats,
            })

        rolling_rows = []

        for start_year in range(
            2002,
            RESEARCH_TO.year - 1,
        ):
            start = max(
                RESEARCH_FROM,
                datetime(
                    start_year,
                    1,
                    1,
                    tzinfo=timezone.utc,
                ),
            )

            end = min(
                RESEARCH_TO,
                datetime(
                    start_year + 3,
                    1,
                    1,
                    tzinfo=timezone.utc,
                ),
            )

            if start >= end:
                continue

            stats = stats_for_trades(
                trades,
                start,
                end,
            )

            rolling_rows.append({
                "window": (
                    f"{start_year}_"
                    f"{start_year + 2}"
                ),
                "start": start.isoformat(),
                "end": end.isoformat(),
                **stats,
            })

        trade_rows = []

        for trade in trades:
            trade_rows.append({
                "signal_time": (
                    trade[
                        "signal_time"
                    ].isoformat()
                ),
                "exit_time": (
                    trade[
                        "exit_time"
                    ].isoformat()
                ),
                "result_r": round(
                    trade[
                        "result_r"
                    ],
                    6,
                ),
            })

        pd.DataFrame(
            [summary]
        ).to_csv(
            OUTPUT_SUMMARY,
            index=False,
        )

        pd.DataFrame(
            yearly_rows
        ).to_csv(
            OUTPUT_YEARLY,
            index=False,
        )

        pd.DataFrame(
            rolling_rows
        ).to_csv(
            OUTPUT_ROLLING,
            index=False,
        )

        pd.DataFrame(
            trade_rows
        ).to_csv(
            OUTPUT_TRADES,
            index=False,
        )

        STATUS.update({
            "state": "complete",
            "message": (
                "EUR/GBP long current live control complete"
            ),
            "summary": summary,
            "output_files": [
                OUTPUT_SUMMARY,
                OUTPUT_YEARLY,
                OUTPUT_ROLLING,
                OUTPUT_TRADES,
            ],
        })

        print()
        print("=" * 80)
        print(
            "EUR/GBP LONG CURRENT LIVE CONTROL"
        )
        print("=" * 80)
        print(
            pd.DataFrame(
                [summary]
            ).to_string(
                index=False
            ),
            flush=True,
        )

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
def home():
    return jsonify({
        "service": (
            "EUR/GBP Long Current Live Control Baseline"
        ),
        "status": STATUS,
        "mode": "READ_ONLY_RESEARCH",
        "orders_supported": False,
        "trading_enabled": False,
        "downloads": {
            "summary": (
                "/download/summary"
            ),
            "yearly": (
                "/download/yearly"
            ),
            "rolling": (
                "/download/rolling"
            ),
            "trades": (
                "/download/trades"
            ),
        },
    })


@app.route("/status")
def status():
    return jsonify(
        STATUS
    )


def download_named(
    filename
):
    if not os.path.exists(
        filename
    ):
        return jsonify({
            "status": "not_ready",
            "message": (
                f"{filename} is not ready yet"
            ),
        }), 404

    return send_file(
        filename,
        as_attachment=True,
        download_name=filename,
    )


@app.route("/download/summary")
def download_summary():
    return download_named(
        OUTPUT_SUMMARY
    )


@app.route("/download/yearly")
def download_yearly():
    return download_named(
        OUTPUT_YEARLY
    )


@app.route("/download/rolling")
def download_rolling():
    return download_named(
        OUTPUT_ROLLING
    )


@app.route("/download/trades")
def download_trades():
    return download_named(
        OUTPUT_TRADES
    )


if __name__ == "__main__":
    thread = threading.Thread(
        target=run_research,
        name=(
            "eurgbp-long-current-control"
        ),
        daemon=True,
    )

    thread.start()

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
