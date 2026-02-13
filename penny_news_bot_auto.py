import requests
import time
import os
from datetime import datetime, timedelta
import pytz

# ===== ENV (ўзгармайди) =====
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not POLYGON_API_KEY or not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    print("ENV ERROR")
    exit()

BASE_URL = "https://api.polygon.io"
ET = pytz.timezone("US/Eastern")

PRICE_MIN = 0.2
PRICE_MAX = 10.0
COOLDOWN_MINUTES = 30

last_sent = {}

# ===== TELEGRAM =====
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, json={
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg
    }, timeout=10)

# ===== MARKET =====
def market_open():
    now = datetime.now(ET)
    return now.weekday() < 5 and 4 <= now.hour <= 20

# ===== TICKERS =====
def get_tickers():
    r = requests.get(
        f"{BASE_URL}/v3/reference/tickers",
        params={
            "market": "stocks",
            "active": "true",
            "limit": 200,
            "apiKey": POLYGON_API_KEY
        },
        timeout=20
    )
    return [x["ticker"] for x in r.json().get("results", [])]

# ===== AGGS (timestamp FIX) =====
def get_aggs(ticker, minutes):
    end_time = int(time.time() * 1000)
    start_time = end_time - (minutes * 60 * 1000)

    r = requests.get(
        f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/1/minute/{start_time}/{end_time}",
        params={
            "adjusted": "true",
            "apiKey": POLYGON_API_KEY
        },
        timeout=20
    )

    return r.json().get("results", [])

# ===== COOLDOWN =====
def cooldown_ok(ticker):
    if ticker not in last_sent:
        return True
    return datetime.utcnow() - last_sent[ticker] > timedelta(minutes=COOLDOWN_MINUTES)

# ===== MAIN =====
def main():
    send_telegram("🤖 Penny Bot V5 started")

    tickers = get_tickers()
    send_telegram(f"Loaded tickers: {len(tickers)}")

    while True:
        try:
            if not market_open():
                time.sleep(60)
                continue

            for ticker in tickers[:120]:

                if not cooldown_ok(ticker):
                    continue

                aggs = get_aggs(ticker, 10)

                if len(aggs) < 6:
                    continue

                last = aggs[-1]
                price = last["c"]

                if not (PRICE_MIN <= price <= PRICE_MAX):
                    continue

                last5 = aggs[-6:]

                # upward trend check
                if last5[-1]["c"] <= last5[0]["o"]:
                    continue

                # FAST
                avg_vol = sum(x["v"] for x in aggs[-6:-1]) / 5
                spike = last["v"] / avg_vol if avg_vol > 0 else 0
                change_1m = ((last["c"] - last["o"]) / last["o"]) * 100

                if spike >= 1.5 and change_1m >= 0.2:
                    send_telegram(
                        f"🚀 FAST\n"
                        f"{ticker}\n"
                        f"${price:.2f}\n"
                        f"Vol x{spike:.1f}\n"
                        f"+{change_1m:.2f}% (1m)"
                    )
                    last_sent[ticker] = datetime.utcnow()
                    continue

                # SLOW
                avg5_vol = sum(x["v"] for x in last5[:-1]) / 5
                spike5 = last5[-1]["v"] / avg5_vol if avg5_vol > 0 else 0
                change_5m = ((last5[-1]["c"] - last5[0]["o"]) / last5[0]["o"]) * 100

                if spike5 >= 1.2 and change_5m >= 0.8:
                    send_telegram(
                        f"🐢 SLOW\n"
                        f"{ticker}\n"
                        f"${price:.2f}\n"
                        f"Vol x{spike5:.1f}\n"
                        f"+{change_5m:.2f}% (5m)"
                    )
                    last_sent[ticker] = datetime.utcnow()

            time.sleep(60)

        except Exception as e:
            send_telegram(f"ERROR: {str(e)}")
            time.sleep(60)

if __name__ == "__main__":
    main()
