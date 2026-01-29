#!/usr/bin/env python3
# -*- coding: utf-8 -*-

еimport os
уimport time
уimport requests
уimport logging
уimport html

# ================== CONFIG ==================
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

PRICE_MIN = 0.20
PRICE_MAX = 10.0
VOLUME_MULTIPLIER = 5        # x5 volume
CHECK_INTERVAL = 60          # seconds

SNAPSHOT_URL = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"

# ================== LOGGING ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("volume_snapshot")

# ================== TELEGRAM ==================
def send_telegram(text: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram sozlanmagan")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        log.error("Telegram xato: %s", e)

# ================== SNAPSHOT ==================
def fetch_snapshot():
    params = {
        "apiKey": POLYGON_API_KEY
    }
    r = requests.get(SNAPSHOT_URL, params=params, timeout=15)
    r.raise_for_status()
    return r.json().get("tickers", [])

# ================== MAIN LOOP ==================
def main():
    log.info("Volume Snapshot bot started")
    send_telegram("✅ <b>Volume bot ishga tushdi</b>\n📊 Penny акциялар кузатиляпти")

    prev_volumes = {}

    while True:
        try:
            tickers = fetch_snapshot()
            log.info("Snapshot received: %d tickers", len(tickers))

            for t in tickers:
                symbol = t.get("ticker")
                day = t.get("day", {})
                prev = t.get("prevDay", {})

                price = day.get("c")
                volume = day.get("v")
                prev_volume = prev.get("v")

                if not symbol or not price or not volume or not prev_volume:
                    continue

                if price < PRICE_MIN or price > PRICE_MAX:
                    continue

                if prev_volume > 0 and volume >= prev_volume * VOLUME_MULTIPLIER:
                    msg = (
                        f"⚡ <b>{html.escape(symbol)}</b>\n"
                        f"Нарх: <code>{price:.2f}$</code>\n"
                        f"Volume: <code>{volume:,}</code>\n"
                        f"Кеча: <code>{prev_volume:,}</code>\n"
                        f"📈 Volume x{volume / prev_volume:.1f}"
                    )
                    send_telegram(msg)
                    log.info("Spike detected: %s", symbol)

            time.sleep(CHECK_INTERVAL)

        except Exception as e:
            log.error("Xato: %s", e)
            time.sleep(30)

if __name__ == "__main__":
    main()
