import requests
import time
import os
from datetime import datetime, timedelta
import pytz

POLYGON_KEY = os.getenv("POLYGON_API_KEY")
TELEGRAM_TOKEN = os.getenv("TG_TOKEN")
CHAT_ID = os.getenv("TG_CHAT_ID")

BASE_URL = "https://api.polygon.io"

seen = {}  # ticker -> {last_time, last_volume_x}

ET = pytz.timezone("US/Eastern")

PRICE_MIN = 0.20
PRICE_MAX = 10.0
COOLDOWN_MIN = 30

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": CHAT_ID, "text": msg})

def market_open():
    now = datetime.now(ET)
    return now.weekday() < 5 and now.hour >= 4 and now.hour <= 20

def get_grouped(minutes):
    date = datetime.now(ET).strftime("%Y-%m-%d")
    url = f"{BASE_URL}/v2/aggs/grouped/locale/us/market/stocks/{minutes}/{date}"
    params = {"adjusted": "true", "apiKey": POLYGON_KEY}
    r = requests.get(url, params=params, timeout=30)
    if r.status_code != 200:
        return []
    return r.json().get("results", [])

def ema(values, period):
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    e = values[0]
    for v in values[1:]:
        e = v * k + e * (1 - k)
    return e

def process(timeframe, volume_x_need):
    bars = get_grouped(timeframe)
    now = datetime.now(ET)

    for b in bars:
        ticker = b["T"]
        close = b["c"]
        volume = b["v"]
        open_price = b["o"]

        if not (PRICE_MIN <= close <= PRICE_MAX):
            continue

        if close <= open_price:
            continue

        avg_volume = volume / volume_x_need
        volume_x = volume / max(avg_volume, 1)

        if volume_x < volume_x_need:
            continue

        prev = seen.get(ticker)
        if prev:
            if now - prev["time"] < timedelta(minutes=COOLDOWN_MIN):
                if volume_x <= prev["vol"]:
                    continue

        msg = (
            f"🚀 {ticker}\n"
            f"💲 Price: {close:.2f}$\n"
            f"📊 Volume spike x{volume_x:.1f}\n"
            f"⏱ TF: {timeframe} min"
        )

        send_telegram(msg)
        seen[ticker] = {"time": now, "vol": volume_x}

def main():
    send_telegram("✅ Volume test bot started")
    while True:
        try:
            if market_open():
                process(1, 5)   # FAST
                process(5, 3)   # SLOW
            time.sleep(60)
        except Exception as e:
            send_telegram(f"❌ Error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main()
