#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import requests
import logging
import html
from datetime import datetime, timedelta

# ============ CONFIG ============
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

PRICE_MIN = 0.2
PRICE_MAX = 10.0
VOLUME_MULTIPLIER = 2.0
PRICE_CHANGE_MIN = 1.5   # %
COOLDOWN_SECONDS = 600   # 10 минут
POLL_INTERVAL = 60       # 1 минут

# ============ LOGGING ============
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger("volume_test_bot")

last_alert = {}

# ============ TELEGRAM ============
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

# ============ POLYGON ============
def get_active_tickers():
    url = "https://api.polygon.io/v3/reference/tickers"
    params = {
        "market": "stocks",
        "active": "true",
        "limit": 200,
        "apiKey": POLYGON_API_KEY
    }
    r = requests.get(url, params=params, timeout=15)
    data = r.json()
    return [t["ticker"] for t in data.get("results", [])]

def get_last_minutes(ticker: str):
    end = datetime.utcnow()
    start = end - timedelta(minutes=10)
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{start.date()}/{end.date()}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 10,
        "apiKey": POLYGON_API_KEY
    }
    r = requests.get(url, params=params, timeout=10)
    return r.json().get("results", [])

# ============ MAIN LOGIC ============
def check_ticker(ticker: str):
    candles = get_last_minutes(ticker)
    if len(candles) < 6:
        return

    last = candles[-1]
    prev5 = candles[-6:-1]

    price = last["c"]
    if price < PRICE_MIN or price > PRICE_MAX:
        return

    avg_vol = sum(c["v"] for c in prev5) / 5
    if avg_vol <= 0:
        return

    price_change = ((last["c"] - prev5[-1]["c"]) / prev5[-1]["c"]) * 100
    if last["v"] >= avg_vol * VOLUME_MULTIPLIER and price_change >= PRICE_CHANGE_MIN:
        now = time.time()
        if now - last_alert.get(ticker, 0) < COOLDOWN_SECONDS:
            return

        last_alert[ticker] = now
        msg = (
            f"🚀 <b>{html.escape(ticker)}</b>\n"
            f"💲 Price: {price:.2f}$\n"
            f"📊 Volume spike x{last['v']/avg_vol:.1f}\n"
            f"📈 +{price_change:.2f}% (5 min)"
        )
        logger.info("ALERT %s", ticker)
        send_telegram(msg)

def main():
    logger.info("Volume test bot started")
    send_telegram("✅ Volume test bot ишга тушди")

    tickers = get_active_tickers()
    logger.info("Tickers loaded: %s", len(tickers))

    while True:
        for t in tickers:
            try:
                check_ticker(t)
            except Exception as e:
                logger.debug("Error %s: %s", t, e)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
