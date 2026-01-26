#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Volume + Price Spike Tracker with Social & News Alerts
"""

from __future__ import annotations

import os
import time
import json
import html
import logging
import tempfile
from typing import Dict, Optional
from urllib.parse import quote_plus

import requests
import pandas as pd
import yfinance as yf

# ================= CONFIG =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))
SEEN_FILE = os.getenv("SEEN_FILE", "/tmp/seen.json")

TICKERS_ENV = os.getenv("TICKERS")              # "AAPL,MSFT,TSLA"
TICKERS_FILE = os.getenv("TICKERS_FILE")        # optional file

REDDIT_LIMIT = int(os.getenv("REDDIT_LIMIT", "5"))
PRICE_RISE_THRESHOLD = float(os.getenv("PRICE_RISE_THRESHOLD", "3.0"))
VOLUME_SPIKE_THRESHOLD = float(os.getenv("VOLUME_SPIKE_THRESHOLD", "50.0"))

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
USER_AGENT = os.getenv("USER_AGENT", "VolumeBot/1.0")

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger("volume_bot")

# ================= HTTP SESSION =================
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def create_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504))
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": USER_AGENT})
    return s

SESSION = create_session()

# ================= SEEN =================
def load_seen(path: str) -> Dict[str, float]:
    try:
        if not os.path.exists(path):
            return {}
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning("Seen С„Р°Р№Р»РЅРё СћТ›РёС€РґР° С…Р°С‚Рѕ: %s", e)
        return {}

def save_seen_atomic(path: str, seen: Dict[str, float]) -> None:
    tmp = None
    try:
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as tf:
            json.dump(seen, tf, ensure_ascii=False, indent=2)
            tmp = tf.name
        os.replace(tmp, path)
    except Exception as e:
        logger.error("Seen save error: %s", e)
    finally:
        try:
            if tmp and os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass

def mark_seen(seen: Dict[str, float], key: str):
    seen[key] = time.time()

# ================= TELEGRAM =================
def send_telegram(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "HTML"
    }

    try:
        SESSION.post(url, json=payload, timeout=10).raise_for_status()
    except Exception as e:
        logger.error("Telegram С…Р°С‚Рѕ: %s", e)

# ================= TICKERS =================
def load_tickers():
    if TICKERS_ENV:
        return [t.strip().upper() for t in TICKERS_ENV.split(",") if t.strip()]

    if TICKERS_FILE and os.path.exists(TICKERS_FILE):
        try:
            with open(TICKERS_FILE, "r", encoding="utf-8") as f:
                return [l.strip().upper() for l in f if l.strip()]
        except Exception as e:
            logger.warning("Tickers С„Р°Р№Р»РґР° С…Р°С‚Рѕ: %s", e)

    return []

# ================= PRICE & VOLUME =================
def fetch_price_data(symbol: str) -> Optional[pd.DataFrame]:
    try:
        df = yf.download(symbol, period="7d", interval="1d", progress=False)
        if df is None or df.empty or len(df) < 2:
            return None
        return df
    except Exception as e:
        logger.debug("Price fetch error %s: %s", symbol, e)
        return None

def percent_change(new: float, old: float) -> Optional[float]:
    if old == 0:
        return None
    return (new - old) / old * 100

def check_volume_price_spike(df: pd.DataFrame) -> Optional[str]:
    today = df.iloc[-1]
    prev = df.iloc[-2]

    price_change = percent_change(today["Close"], prev["Close"])
    volume_change = percent_change(today["Volume"], prev["Volume"])

    if price_change is None or volume_change is None:
        return None

    if price_change >= PRICE_RISE_THRESHOLD and volume_change >= VOLUME_SPIKE_THRESHOLD:
        return f"рџ“€ РќР°СЂС… +{price_change:.2f}% | Volume +{volume_change:.2f}%"

    return None

# ================= REDDIT =================
def fetch_reddit():
    url = f"https://www.reddit.com/r/stocks/new/.json?limit={REDDIT_LIMIT}"
    posts = []
    try:
        r = SESSION.get(url, timeout=10)
        r.raise_for_status()
        for p in r.json()["data"]["children"]:
            d = p["data"]
            posts.append((d["title"], "https://reddit.com" + d["permalink"]))
    except Exception:
        pass
    return posts

# ================= MAIN =================
def run_once(seen):
    tickers = load_tickers()
    if not tickers:
        logger.warning("РўРёРєРµСЂР»Р°СЂ Р№СћТ›")
        return

    for symbol in tickers:
        df = fetch_price_data(symbol)
        if not df:
            continue

        spike = check_volume_price_spike(df)
        if spike:
            key = f"{symbol}_{int(time.time() // 3600)}"
            if key not in seen:
                send_telegram(f"вљ пёЏ <b>{symbol}</b>\n{html.escape(spike)}")
                mark_seen(seen, key)
                save_seen_atomic(SEEN_FILE, seen)

        time.sleep(1)

    for title, url in fetch_reddit():
        if url not in seen:
            send_telegram(f"рџ’¬ <b>Reddit</b>\n{html.escape(title)}\n{url}")
            mark_seen(seen, url)
            save_seen_atomic(SEEN_FILE, seen)

def main():
    logger.info("Р‘РѕС‚ РёС€РіР° С‚СѓС€РґРё")
    send_telegram(
        "вњ… <b>Р‘РѕС‚ РёС€РіР° С‚СѓС€РґРё</b>\n"
        "рџ“Љ РќР°СЂС… РІР° Volume РєСѓР·Р°С‚РёР»СЏРїС‚Рё\n"
        "рџ“° Reddit СЏРЅРіРёР»РёРєР»Р°СЂ С‚РµРєС€РёСЂРёР»Р°РґРё"
    )

    seen = load_seen(SEEN_FILE)

    while True:
        run_once(seen)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
