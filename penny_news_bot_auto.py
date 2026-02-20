#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import time
import logging
import html
from datetime import datetime, timedelta
from typing import Optional, Dict

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ========= CONFIG / ENV =========
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

MIN_PRICE = float(os.getenv("MIN_PRICE", "0.20"))
MAX_PRICE = float(os.getenv("MAX_PRICE", "10.00"))

VOLUME_5M_MULTIPLIER = float(os.getenv("VOLUME_5M_MULTIPLIER", "2.0"))
VOLUME_30M_MULTIPLIER = float(os.getenv("VOLUME_30M_MULTIPLIER", "3.0"))

CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "300"))  # seconds
ALERT_COOLDOWN = int(os.getenv("ALERT_COOLDOWN", "1800"))  # seconds - don't alert same symbol too often

BASE_URL = "https://api.polygon.io"

# ========= LOGGING =========
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("polygon_volume_scanner")

# ========= VALIDATE ENV =========
if not POLYGON_API_KEY:
    logger.error("POLYGON_API_KEY не задан. Останов.")
    raise SystemExit(1)
if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    logger.error("TELEGRAM_TOKEN yoki TELEGRAM_CHAT_ID не задан. Останов.")
    raise SystemExit(1)

# ========= HTTP SESSION with retries =========
session = requests.Session()
retries = Retry(total=3, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504))
session.mount("https://", HTTPAdapter(max_retries=retries))
session.params = {"apiKey": POLYGON_API_KEY}  # default param included

# ========= STATE =========
# last_alert: symbol -> {"volume": int, "ts": epoch}
last_alert: Dict[str, Dict[str, float]] = {}

# ========= TELEGRAM =========
def send_telegram(text: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.debug("Telegram credentials missing.")
        return False
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
    """
    Returns one news title (string) or None.
    Uses /v2/reference/news with query by ticker.
    """
    try:
        url = f"{BASE_URL}/v2/reference/news"
        params = {"ticker": symbol, "limit": 1}
        r = session.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        results = data.get("results") or []
        if results:
            title = results[0].get("title") or results[0].get("headline")
            return title
    except Exception as e:
        logger.debug("get_news error for %s: %s", symbol, e)
    return None

# ========= VOLUME CHECK =========
def get_volume_minutes(symbol: str, minutes: int) -> Optional[int]:
    """
    Fetch minute bars between start and end (ISO format). Returns total volume or None.
    Uses /v2/aggs/ticker/{symbol}/range/1/minute/{from}/{to}
    'from' and 'to' are ISO 8601 strings.
    """
    end = datetime.utcnow()
    start = end - timedelta(minutes=minutes)
    # Polygon accepts ISO 8601 datetimes for from/to
    from_iso = start.strftime("%Y-%m-%dT%H:%M:%S")
    to_iso = end.strftime("%Y-%m-%dT%H:%M:%S")
    try:
        url = f"{BASE_URL}/v2/aggs/ticker/{symbol}/range/1/minute/{from_iso}/{to_iso}"
        r = session.get(url, params={"limit": minutes}, timeout=10)
        r.raise_for_status()
        data = r.json()
        results = data.get("results") or []
        if not results:
            return None
        total_volume = sum(int(candle.get("v", 0)) for candle in results)
        return total_volume
    except Exception as e:
        logger.debug("get_volume_minutes error for %s (%dm): %s", symbol, minutes, e)
        return None

# ========= SNAPSHOT =========
def get_market_snapshot() -> list:
    """
    Returns list of tickers from the US stocks snapshot.
    Might be large; consider filtering server-side if available.
    """
    try:
        url = f"{BASE_URL}/v2/snapshot/locale/us/markets/stocks/tickers"
        r = session.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
        tickers = data.get("tickers") or []
        return tickers
    except Exception as e:
        logger.error("get_market_snapshot error: %s", e)
        return []

# ========= MAIN SCANNER =========
def should_alert(symbol: str, volume_5m: int) -> bool:
    """Deduplication logic: cooldown by time and relative increase check."""
    info = last_alert.get(symbol)
    now = time.time()
    if not info:
        return True
    last_ts = info.get("ts", 0)
    last_vol = info.get("volume", 0)
    # If last alert was recent, skip
    if now - last_ts < ALERT_COOLDOWN:
        logger.debug("%s skipped: cooldown not passed (%.0fs remaining)", symbol, ALERT_COOLDOWN - (now - last_ts))
        return False
    # If new volume is not significantly (>30%) larger than last alerted volume, skip
    if last_vol > 0 and volume_5m <= last_vol * 1.3:
        logger.debug("%s skipped: volume %.0f not > 1.3x last %.0f", symbol, volume_5m, last_vol)
        return False
    return True

def scan_market():
    tickers = get_market_snapshot()
    if not tickers:
        logger.warning("No tickers returned from snapshot.")
        return

    logger.info("Tickers count: %d", len(tickers))

    for stock in tickers:
        try:
            symbol = stock.get("ticker")
            last_trade = stock.get("lastTrade") or {}
            price = last_trade.get("p")
            # Validate symbol and price
            if not symbol or price is None:
                continue
            try:
                price = float(price)
            except Exception:
                continue

            if not (MIN_PRICE <= price <= MAX_PRICE):
                continue

            # Fetch volumes
            volume_5m = get_volume_minutes(symbol, 5)
            volume_30m = get_volume_minutes(symbol, 30)

            if volume_5m is None or volume_30m is None or volume_30m == 0:
                continue

            avg_5m_from_30m = volume_30m / 6.0  # 30m -> six 5m periods approximate

            # check 5m surge
            if volume_5m >= avg_5m_from_30m * VOLUME_5M_MULTIPLIER:
                if not should_alert(symbol, volume_5m):
                    continue

                news_title = get_news(symbol)

                message = (
                    f"🚀 <b>VOLUME SURGE</b>\n\n"
                    f"<b>{html.escape(symbol)}</b>\n"
                    f"Price: ${price:.2f}\n"
                    f"5m Volume: {volume_5m}\n"
                    f"30m Volume: {volume_30m}\n"
                    f"Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}\n"
                )
                if news_title:
                    message += f"\n📰 {html.escape(news_title)}"

                send_telegram(message)
                last_alert[symbol] = {"volume": volume_5m, "ts": time.time()}

                # polite pause to avoid hitting rate limits
                time.sleep(0.8)

        except Exception as e:
            logger.debug("Error processing stock entry: %s", e)
            continue

# ========= MAIN LOOP =========
def main():
    logger.info("Polygon Volume Surge Scanner started.")
    while True:
        try:
            logger.info("Scanning market...")
            scan_market()
        except Exception as e:
            logger.exception("Fatal error in scan loop: %s", e)
        logger.info("Sleeping %d seconds...", CHECK_INTERVAL)
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
