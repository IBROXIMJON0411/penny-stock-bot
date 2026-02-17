import requests
import time
import os
from datetime import datetime, timedelta

API_KEY = os.getenv("POLYGON_API_KEY")
BASE_URL = "https://api.polygon.io"
session = requests.Session()

TICKERS = ["AAPL", "TSLA", "NVDA", "MSFT"]

def get_last_minute_candle(ticker):
    now = datetime.utcnow()
    end = now.replace(second=0, microsecond=0)
    start = end - timedelta(minutes=1)

    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")

    url = f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/1/minute/{start_str}/{end_str}"

    for attempt in range(3):
        try:
            r = session.get(
                url,
                params={"apiKey": API_KEY},
                timeout=20
            )

            if r.status_code == 200:
                data = r.json()
                return data.get("results", [])

            elif r.status_code == 403:
                print(f"403 retry {ticker}")
                time.sleep(2)

            else:
                print(f"{ticker} error {r.status_code}")
                return None

        except Exception as e:
            print("Request failed:", e)
            time.sleep(2)

    return None


# ===== MAIN LOOP =====

while True:
    print("=== New Cycle ===")

    for ticker in TICKERS:
        data = get_last_minute_candle(ticker)

        if data:
            print(ticker, "OK", len(data))
        else:
            print(ticker, "No data")

        time.sleep(1)   # Rate control

    time.sleep(60)
