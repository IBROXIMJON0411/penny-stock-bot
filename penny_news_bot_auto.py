import requests
import time
import os
from datetime import datetime
import pytz

POLYGON = os.getenv("POLYGON_API_KEY")
TG_TOKEN = os.getenv("TG_TOKEN")
CHAT_ID = os.getenv("TG_CHAT_ID")

BASE = "https://api.polygon.io"
ET = pytz.timezone("US/Eastern")

PRICE_MIN = 0.20
PRICE_MAX = 10.0

def tg(msg):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": CHAT_ID, "text": msg})

def market_open():
    now = datetime.now(ET)
    return now.weekday() < 5 and 4 <= now.hour <= 20

def get_tickers():
    url = f"{BASE}/v3/reference/tickers"
    params = {
        "market": "stocks",
        "active": "true",
        "limit": 200,
        "apiKey": POLYGON
    }
    r = requests.get(url, params=params, timeout=20)
    return [t["ticker"] for t in r.json().get("results", [])]

def get_last_agg(ticker):
    url = f"{BASE}/v2/aggs/ticker/{ticker}/range/1/minute/now-10/minute/now"
    params = {"adjusted": "true", "apiKey": POLYGON}
    r = requests.get(url, params=params, timeout=20)
    return r.json().get("results", [])

def main():
    tg("✅ Penny volume TEST v2 started")

    tickers = get_tickers()
    tg(f"📌 Tickers loaded: {len(tickers)}")

    while True:
        try:
            if not market_open():
                time.sleep(60)
                continue

            for t in tickers[:100]:  # синов учун 100 та
                aggs = get_last_agg(t)
                if len(aggs) < 6:
                    continue

                last = aggs[-1]
                prev = aggs[-6:-1]

                price = last["c"]
                if not (PRICE_MIN <= price <= PRICE_MAX):
                    continue

                avg_vol = sum(x["v"] for x in prev) / 5
                if avg_vol == 0:
                    continue

                spike = last["v"] / avg_vol
                if spike < 4:
                    continue

                change = (last["c"] - last["o"]) / last["o"] * 100
                if change <= 0:
                    continue

                tg(
                    f"🚀 {t}\n"
                    f"💲 {price:.2f}$\n"
                    f"📊 Volume x{spike:.1f}\n"
                    f"📈 +{change:.2f}% (1m)"
                )

            time.sleep(60)

        except Exception as e:
            tg(f"❌ ERROR: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main()
