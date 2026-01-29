#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import requests
import logging
import html
from datetime import datetime, timedelta
from statistics import mean

# ================== CONFIG ==================
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

PRICE_MIN = 0.2
PRICE_MAX = 10
VOLUME_MULTIPLIER = 5
LOOKBACK_MINUTES = 5
SLEEP_SECONDS = 60

# ================== LOGGING ==================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("volume_test_bot")

# ================== TELEGRAM ==================
def send_telegram(text: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    }
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        logger.error("Telegram error: %s", e)

# ================== POLYGON ==================
def get_active_tickers():
    """Polygon snapshot – актив акциялар"""
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
    params = {"apiKey": POLYGON_API_KEY}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json().get("tickers", [])

def get_minute_bars(symbol: str):
    end = datetime.utcnow()
    start = end - timedelta(minutes=LOOKBACK_MINUTES + 1)

    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{start.date()}/{end.date()}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50,
        "apiKey": POLYGON_API_KEY
    }
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json().get("results", [])

# ================== MAIN LOOP ==================
def run():
    send_telegram("✅ <b>Volume тест бот ишга тушди</b>\nФақат volume кузатилади")
    logger.info("Bot started")

    while True:
        try:
            tickers = get_active_tickers()
            logger.info("Tickers fetched: %s", len(tickers))

            for t in tickers:
                symbol = t.get("ticker")
                price = t.get("lastTrade", {}).get("p")

                if not symbol or not price:
                    continue
                if price < PRICE_MIN or price > PRICE_MAX:
                    continue

                bars = get_minute_bars(symbol)
                if len(bars) < LOOKBACK_MINUTES + 1:
                    continue

                volumes = [b["v"] for b in bars[:-1]]
                avg_volume = mean(volumes)
                last_volume = bars[-1]["v"]

                if avg_volume > 0 and last_volume >= avg_volume * VOLUME_MULTIPLIER:
                    msg = (
                        f"⚡ <b>{html.escape(symbol)}</b>\n"
                        f"Нарх: <code>{price}$</code>\n"
                        f"Volume: <code>{last_volume}</code>\n"
                        f"Ўртача ({LOOKBACK_MINUTES}m): <code>{int(avg_volume)}</code>"
                    )
                    send_telegram(msg)
                    logger.info("Spike %s", symbol)

                time.sleep(0.2)

        except Exception as e:
            logger.error("Loop error: %s", e)

        time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    main()
