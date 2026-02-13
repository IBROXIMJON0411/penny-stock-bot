import requests
import time
import os

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
BASE_URL = "https://api.polygon.io"

session = requests.Session()

def get_aggs(ticker, minutes=10):
    if "." in ticker:   # warrant ва бошқаларни skip
        return None

    end_time = int(time.time() * 1000)
    start_time = end_time - (minutes * 60 * 1000)

    url = f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/1/minute/{start_time}/{end_time}"

    try:
        r = session.get(
            url,
            params={
                "adjusted": "true",
                "apiKey": POLYGON_API_KEY
            },
            timeout=25
        )

        if r.status_code == 200:
            return r.json().get("results", [])

        elif r.status_code == 403:
            print(f"403 skip: {ticker}")
            return None

        else:
            print(f"Error {r.status_code}: {ticker}")
            return None

    except Exception as e:
        print("Request error:", e)
        return None


# ====== MAIN LOOP ======

TICKERS = ["AAPL", "MSFT", "NVDA", "TSLA"]  # ўз рўйхатингни қўй

while True:
    for ticker in TICKERS:
        data = get_aggs(ticker)

        if data:
            print(ticker, "OK", len(data))

        time.sleep(0.4)   # rate control (жуда муҳим)

    print("Cycle done")
    time.sleep(65)
