#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Volume + Price Spike Tracker with Reddit & Telegram Alerts
Improved, cleaned and more robust version.
"""

from __future__ import annotations

import os
import time
import json
import html
import logging
import tempfile
from typing import Dict, Optional, Tuple, List
from urllib.parse import quote_plus

import requests
import pandas as pd

# Optional yfinance fallback (install with: pip install yfinance)
try:
    import yfinance as yf
    HAVE_YFINANCE = True
except Exception:
    HAVE_YFINANCE = False

# ================= CONFIG =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))  # seconds
SEEN_FILE = os.getenv("SEEN_FILE", "/tmp/seen.json")

TICKERS_ENV = os.getenv("TICKERS")              # "AAPL,MSFT,TSLA"
TICKERS_FILE = os.getenv("TICKERS_FILE")        # optional file path with tickers, one per line

REDDIT_LIMIT = int(os.getenv("REDDIT_LIMIT", "5"))
PRICE_RISE_THRESHOLD = float(os.getenv("PRICE_RISE_THRESHOLD", "3.0"))       # percent
VOLUME_SPIKE_THRESHOLD = float(os.getenv("VOLUME_SPIKE_THRESHOLD", "50.0"))  # percent

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")  # optional (not used here but kept)
USER_AGENT = os.getenv("USER_AGENT", "VolumeBot/1.0 (+https://example.com)")

# Seen TTL (prune older entries) in seconds (default 24 hours)
SEEN_TTL = int(os.getenv("SEEN_TTL", str(24 * 3600)))

# ================= LOGGING =================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("volume_news_bot")

# ================= HTTP SESSION =================
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def create_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=0.6, status_forcelist=(429, 500, 502, 503, 504))
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": USER_AGENT})
    return s

SESSION = create_session()

# ================= SEEN helpers =================
def load_seen(path: str) -> Dict[str, float]:
    now = time.time()
    try:
        if not os.path.exists(path):
            return {}
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {}
        # prune old entries
        pruned = {}
        for k, v in data.items():
            try:
                ts = float(v)
            except Exception:
                continue
            if now - ts <= SEEN_TTL:
                pruned[k] = ts
        return pruned
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

def mark_seen(seen: Dict[str, float], key: str) -> None:
    seen[key] = time.time()

# ================= TELEGRAM =================
def send_telegram(msg: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.debug("Telegram credentials not set; skipping send.")
        return False

    url = f"https://api.telegram.org/bot        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "HTML",
        "disable_web_page_preview": False
    }
    try:
        r = SESSION.post(url, json=payload, timeout=10)
        r.raise_for_status()
        logger.info("Telegram: xabar yuborildi")
        return True
    except Exception as e:
        logger.error("Telegram yuborishda xato: %s", e)
        return False

# ================= TICKERS =================
def load_tickers() -> List[str]:
    # Preference: TICKERS_ENV -> TICKERS_FILE -> []
    if TICKERS_ENV:
        return [t.strip().upper() for t in TICKERS_ENV.split(",") if t.strip()]
    if TICKERS_FILE and os.path.exists(TICKERS_FILE):
        try:
            with open(TICKERS_FILE, "r", encoding="utf-8") as f:
                lines = [ln.strip().upper() for ln in f if ln.strip()]
            return lines
        except Exception as e:
            logger.warning("Tickers С„Р°Р№Р»РЅРё СћТ›РёС€РґР° С…Р°С‚o: %s", e)
    return []

# ================= PRICE & VOLUME =================
def fetch_price_data_yf(symbol: str) -> Optional[pd.DataFrame]:
    """
    Return daily dataframe for last 7 days (at least 2 rows) with columns: Open, High, Low, Close, Volume
    """
    if not HAVE_YFINANCE:
        logger.debug("yfinance not available")
        return None
    try:
        # Use history to get daily bars
        t = yf.Ticker(symbol)
        df = t.history(period="7d", interval="1d", actions=False)
        if df is None or df.empty or len(df) < 2:
            return None
        # Ensure columns exist
        for col in ("Close", "Volume"):
            if col not in df.columns:
                return None
        return df
    except Exception as e:
        logger.debug("yfinance fetch error %s: %s", symbol, e)
        return None

def percent_change(new: float, old: float) -> Optional[float]:
    try:
        if old == 0 or old is None:
            return None
        return (new - old) / old * 100.0
    except Exception:
        return None

def check_volume_price_spike(df: pd.DataFrame) -> Optional[str]:
    try:
        # Use last row as "today" and previous as "prev"
        today = df.iloc[-1]
        prev = df.iloc[-2]
        price_change = percent_change(float(today["Close"]), float(prev["Close"]))
        volume_change = percent_change(float(today["Volume"]), float(prev["Volume"]))
        if price_change is None or volume_change is None:
            return None
        if price_change >= PRICE_RISE_THRESHOLD and volume_change >= VOLUME_SPIKE_THRESHOLD:
            return f"рџ“€ РќР°СЂС… +{price_change:.2f}% | ТІР°Р¶Рј +{volume_change:.2f}%"
        return None
    except Exception as e:
        logger.debug("check_volume_price_spike error: %s", e)
        return None

# ================= REDDIT =================
def fetch_reddit(limit: int = 5) -> List[Tuple[str, str]]:
    """
    Fetch newest posts from r/stocks (public JSON).
    Returns list of (title, url).
    """
    url = f"https://www.reddit.com/r/stocks/new/.json?limit={int(limit)}"
    headers = {"User-Agent": USER_AGENT}
    posts: List[Tuple[str, str]] = []
    try:
        r = SESSION.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        j = r.json()
        children = j.get("data", {}).get("children", [])
        for c in children:
            d = c.get("data", {})
            title = d.get("title", "")[:300]
            permalink = d.get("permalink")
            if permalink:
                full_url = "https://reddit.com" + permalink
            else:
                full_url = d.get("url") or ""
            posts.append((title, full_url))
    except Exception as e:
        logger.debug("Reddit fetch error: %s", e)
    return posts

# ================= MAIN RUN =================
def run_once(seen: Dict[str, float]) -> None:
    tickers = load_tickers()
    if not tickers:
        logger.warning("РўРёРєРµСЂР»Р°СЂ РјР°РІР¶СѓРґ СЌРјР°СЃ. TICKERS С‘РєРё TICKERS_FILE РЅРё С‚РµРєС€РёСЂРёРЅРі.")
        return

    for symbol in tickers:
        df = fetch_price_data_yf(symbol)
        if df is None:
            logger.debug("РњР°СЉР»СѓРјРѕС‚ С‚РѕРїРёР»РјР°РґРё: %s", symbol)
            continue

        spike_msg = check_volume_price_spike(df)
        if spike_msg:
            # make a key per symbol per day/hour to avoid repeats; here use date-hour
            key = f"{symbol}_{time.strftime('%Y%m%d%H')}"
            if key not in seen:
                text = f"вљ пёЏ <b>{html.escape(symbol)}</b>\n{spike_msg}\n"
                # include last close info
                try:
                    last_close = float(df.iloc[-1]["Close"])
                    text += f"РЎСћРЅРіРё Close: <code>{last_close:.6f}</code>"
                except Exception:
                    pass
                send_telegram(text)
                mark_seen(seen, key)
                save_seen_atomic(SEEN_FILE, seen)
            else:
                logger.debug("РђР»Р»Р°Т›Р°С‡РѕРЅ РєСћСЂСЃР°С‚РёР»РіР°РЅ: %s", symbol)
        time.sleep(1.0)  # short pause between tickers to be nice

    # Reddit scanning
    for title, url in fetch_reddit(limit=REDDIT_LIMIT):
        if not url:
            continue
        key = f"reddit_{url}"
        if key not in seen:
            txt = f"рџ’¬ <b>Reddit /r/stocks</b>\n{html.escape(title)}\n{html.escape(url)}"
            send_telegram(txt)
            mark_seen(seen, key)
            save_seen_atomic(SEEN_FILE, seen)
        else:
            logger.debug("Reddit РїРѕСЃС‚ РѕР»РґРёРЅРґР°РЅ РєСћСЂРёР»РіР°РЅ: %s", url)

def main() -> None:
    logger.info("вњ… Р‘РѕС‚ РёС€РіР° С‚СѓС€РґРё вЂ” РќР°СЂС… РІР° Volume РєСѓР·Р°С‚РёР»СЏРїС‚Рё; Reddit СЏРЅРіРёР»РёРєР»Р°СЂ ТіР°Рј С‚РµРєС€РёСЂРёР»Р°РґРё.")
    # initial startup notification (best effort)
    try:
        send_telegram("вњ… <b>Volume & News Р±РѕС‚ РёС€РіР° С‚СѓС€РґРё</b>\nвљЎ РќР°СЂС… РІР° РІРѕР»СѓРјРµРЅ РєСѓР·Р°С‚РёР»Р° Р±РѕС€Р»Р°РЅРґРё.")
    except Exception:
        pass

    seen = load_seen(SEEN_FILE)

    while True:
        try:
            run_once(seen)
        except Exception:
            logger.exception("run_once РёС€Р»Р°С€РґР° С…Р°С‚Рѕ")
        # sleep with small increments to be interruptible
        total = POLL_INTERVAL
        step = 5
        for _ in range(0, total, step):
            time.sleep(step)

if __name__ == "__main__":
    main()
