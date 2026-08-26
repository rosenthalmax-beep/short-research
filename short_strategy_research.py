import os
import requests

from flask import Flask, jsonify
from datetime import datetime, timezone, timedelta


# ==================================================
# FLASK
# ==================================================

app = Flask(__name__)


# ==================================================
# CONFIG
# ==================================================

OANDA_TOKEN = os.getenv("OANDA_TOKEN")
OANDA_URL = "https://api-fxtrade.oanda.com"

INSTRUMENT = "EUR_USD"

SEARCH_FROM = datetime(
    2002, 1, 1,
    tzinfo=timezone.utc
)

SEARCH_TO = datetime.now(
    timezone.utc
)

# 180 days of H1 candles is ~4,320 candles,
# safely below OANDA's usual 5,000-candle limit.
CHUNK_DAYS = 180

DAILY_ALIGNMENT_HOUR = 17
DAILY_ALIGNMENT_TIMEZONE = "America/New_York"


# ==================================================
# STATUS
# ==================================================

STATUS = {
    "state": "not_started",
    "instrument": INSTRUMENT,
    "search_from": SEARCH_FROM.isoformat(),
    "earliest_h1_candle": None,
    "chunks_checked": 0,
    "message": "Probe has not started"
}


# ==================================================
# OANDA
# ==================================================

def headers():

    if not OANDA_TOKEN:

        raise RuntimeError(
            "OANDA_TOKEN is not configured"
        )

    return {
        "Authorization":
            f"Bearer {OANDA_TOKEN}"
    }


def iso_utc(dt):

    return (
        dt.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def fetch_h1_chunk(
    start,
    end
):

    response = requests.get(

        f"{OANDA_URL}/v3/instruments/"
        f"{INSTRUMENT}/candles",

        headers=headers(),

        params={
            "price":
                "M",

            "granularity":
                "H1",

            "from":
                iso_utc(start),

            "to":
                iso_utc(end),

            "smooth":
                "false",

            "includeFirst":
                "true",

            "dailyAlignment":
                DAILY_ALIGNMENT_HOUR,

            "alignmentTimezone":
                DAILY_ALIGNMENT_TIMEZONE
        },

        timeout=30
    )

    response.raise_for_status()

    data = response.json()

    candles = []

    for candle in data.get(
        "candles",
        []
    ):

        if not candle.get(
            "complete",
            False
        ):
            continue

        if not candle.get(
            "mid"
        ):
            continue

        candles.append(
            candle
        )

    return candles


# ==================================================
# HISTORY PROBE
# ==================================================

def run_probe():

    global STATUS

    try:

        STATUS.update({
            "state":
                "running",

            "message":
                "Searching OANDA H1 history",

            "earliest_h1_candle":
                None,

            "chunks_checked":
                0
        })

        cursor = SEARCH_FROM

        print()
        print(
            "===================================="
        )
        print(
            "OANDA EUR/USD H1 HISTORY PROBE"
        )
        print(
            "===================================="
        )
        print()

        while cursor < SEARCH_TO:

            chunk_end = min(
                cursor
                + timedelta(
                    days=CHUNK_DAYS
                ),
                SEARCH_TO
            )

            STATUS[
                "chunks_checked"
            ] += 1

            STATUS[
                "currently_checking_from"
            ] = iso_utc(
                cursor
            )

            STATUS[
                "currently_checking_to"
            ] = iso_utc(
                chunk_end
            )

            print(
                f"Checking "
                f"{cursor.date()} "
                f"to "
                f"{chunk_end.date()}..."
            )

            try:

                candles = fetch_h1_chunk(
                    cursor,
                    chunk_end
                )

            except requests.HTTPError as error:

                response = (
                    error.response
                )

                print(
                    "OANDA HTTP error:",
                    response.status_code,
                    response.text[:500]
                )

                STATUS.update({
                    "state":
                        "error",

                    "message":
                        (
                            f"OANDA returned HTTP "
                            f"{response.status_code}: "
                            f"{response.text[:500]}"
                        )
                })

                return

            if candles:

                earliest = (
                    candles[0][
                        "time"
                    ]
                )

                STATUS.update({
                    "state":
                        "complete",

                    "message":
                        "Earliest available H1 candle found",

                    "earliest_h1_candle":
                        earliest,

                    "first_available_chunk_from":
                        iso_utc(
                            cursor
                        ),

                    "first_available_chunk_to":
                        iso_utc(
                            chunk_end
                        ),

                    "candles_in_first_available_chunk":
                        len(
                            candles
                        )
                })

                print()
                print(
                    "===================================="
                )
                print(
                    "FOUND DATA"
                )
                print(
                    "===================================="
                )
                print(
                    "Earliest H1 candle:",
                    earliest
                )
                print(
                    "Candles in chunk:",
                    len(candles)
                )
                print()

                return

            cursor = (
                chunk_end
            )

        STATUS.update({
            "state":
                "complete",

            "message":
                "No EUR/USD H1 candles found from 2002 onward"
        })

    except Exception as error:

        STATUS.update({
            "state":
                "error",

            "message":
                str(error)
        })

        print(
            "ERROR:",
            error
        )


# ==================================================
# ROUTES
# ==================================================

@app.route("/")
def home():

    return jsonify({

        "service":
            "OANDA EURUSD H1 History Probe",

        "status":
            STATUS,

        "trading_enabled":
            False,

        "orders_supported":
            False,

        "executor_connected":
            False
    })


@app.route("/status")
def status():

    return jsonify(
        STATUS
    )


# ==================================================
# START
# ==================================================

if __name__ == "__main__":

    run_probe()

    port = int(
        os.getenv(
            "PORT",
            5000
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False
    )
