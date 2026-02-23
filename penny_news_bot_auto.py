#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import time
import logging
import html
from datetime import datetime, timedelta
from typing import Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ========= CONFIG =========
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

MIN_PRICE = float(os.getenv("MIN_PRICE", "0.20"))
MAX_PRICE = float(os.getenv("MAX_PRICE", "10.00"))

VOLUME_FAST_MULTIPLIER = float(os.getenv("VOLUME_FAST_MULTIPLIER", "5.0"))
VOLUME_SLOW_MULTIPLIER = float(os.getenv("VOLUME_SLOW_MULTIPLIER", "3.0"))

CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "300"))  # seconds
ALERT_COOLDOWN = int(os.getenv("ALERT_COOLDOWN", "1800"))  # seconds

BASE_URL = "https://api.polygon.io"

# ========= LOGGING =========
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("polygon_volume_bot")

# ========= VALIDATE ENV =========
if not POLYGON_API_KEY or not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    logger.error("POLYGON_API_KEY, TELEGRAM_TOKEN yoki TELEGRAM_CHAT_ID not set. Exit.")
    raise SystemExit(1)

# ========= HTTP SESSION =========
session = requests.Session()
retries = Retry(total=3, backoff_factor=0.5, status_forcelist=(429,500,502,503,504))
session.mount("https://", HTTPAdapter(max_retries=retries))
session.params = {"apiKey": POLYGON_API_KEY}

# ========= STATE =========
last_alert: Dict[str, Dict[str, float]] = {}

# ========= TELEGRAM =========
def send_telegram(text: str) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    try:
        r = session.post(url, json=payload, timeout=10)
        r.raise_for_status()
        logger.info("Telegram xabar yuborildi.")
        return True
    except Exception as e:
        logger.error("Telegram yuborish xatosi: %s", e)
        return False

# ========= NEWS =========
def get_news(symbol: str) -> Optional[str]:
    try:
        url = f"{BASE_URL}/v2/reference/news"
        params = {"ticker": symbol, "limit": 1}
        r = session.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        results = data.get("results") or []
        if results:
            return results[0].get("title") or results[0].get("headline")
    except Exception as e:
        logger.debug("get_news error %s: %s", symbol, e)
    return None

# ========= VOLUME FETCH =========
def get_volume(symbol: str, minutes: int) -> Optional[int]:
    end = datetime.utcnow()
    start = end - timedelta(minutes=minutes)
    from_iso = start.strftime("%Y-%m-%dT%H:%M:%S")
    to_iso = end.strftime("%Y-%m-%dT%H:%M:%S")
    try:
        url = f"{BASE_URL}/v2/aggs/ticker/{symbol}/range/1/minute/{from_iso}/{to_iso}"
        r = session.get(url, params={"limit": minutes}, timeout=10)
        r.raise_for_status()
        data = r.json()
        results = data.get("results") or []
        return sum(int(c.get("v",0)) for c in results) if results else None
    except Exception as e:
        logger.debug("get_volume error %s (%dm): %s", symbol, minutes, e)
        return None

# ========= SNAPSHOT =========
def get_market_snapshot() -> list:
    try:
        url = f"{BASE_URL}/v2/snapshot/locale/us/markets/stocks/tickers"
        r = session.get(url, timeout=20)
        r.raise_for_status()
        tickers = r.json().get("tickers") or []
        return tickers
    except Exception as e:
        logger.error("get_market_snapshot error: %s", e)
        return []

# ========= ALERT LOGIC =========
def should_alert(symbol: str, volume: int) -> bool:
    info = last_alert.get(symbol)
    now = time.time()
    if not info:
        return True
    last_vol = info.get("volume",0)
    last_ts = info.get("ts",0)
    if now - last_ts < ALERT_COOLDOWN:
        return False
    if last_vol > 0 and volume <= last_vol*1.3:
        return False
    return True

# ========= MAIN SCAN =========
def scan_market():
    tickers = get_market_snapshot()
    if not tickers:
        logger.warning("Snapshot empty.")
        return
    logger.info("Processing %d tickers", len(tickers))
    for stock in tickers:
        try:
            symbol = stock.get("ticker")
            last_trade = stock.get("lastTrade") or {}
            price = last_trade.get("p")
            if not symbol or price is None:
                continue
            try:
                price = float(price)
            except: 
                continue
            if not (MIN_PRICE <= price <= MAX_PRICE):
                continue
            # Volumes
            vol_5m = get_volume(symbol, 5)
            vol_30m = get_volume(symbol, 30)
            if vol_5m is None or vol_30m is None or vol_30m==0:
                continue
            avg_5m_from_30m = vol_30m/6
            # FAST alert
            if vol_5m >= avg_5m_from_30m*VOLUME_FAST_MULTIPLIER and should_alert(symbol, vol_5m):
                news_title = get_news(symbol)
                msg = f"🚀 <b>VOLUME SURGE</b>\n<b>{html.escape(symbol)}</b>\nPrice: ${price:.2f}\n5m Vol: {vol_5m}\n30m Vol: {vol_30m}\nUTC: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}"
                if news_title:
                    msg += f"\n📰 {html.escape(news_title)}"
                send_telegram(msg)
                last_alert[symbol] = {"volume": vol_5m, "ts": time.time()}
                time.sleep(0.5)
        except Exception as e:
            logger.debug("Error %s: %s", symbol, e)
            continue

# ========= MAIN LOOP =========
def main():
    logger.info("Polygon Volume Bot started.")
    while True:
        try:
            scan_market()
        except Exception as e:
            logger.exception("Scan loop error: %s", e)
        logger.info("Sleeping %d seconds...", CHECK_INTERVAL)
        time.sleep(CHECK_INTERVAL)

if __name__=="__main__":
    main()
