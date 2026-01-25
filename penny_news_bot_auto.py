from __future__ import annotations

import os
import time
import json
import html
import logging
import tempfile
from typing import List, Dict, Optional, Any
from urllib.parse import quote_plus

import requests
import pandas as pd
import yfinance as yf  # required for price data

# ================= CONFIG =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))  # seconds
SEEN_FILE = os.getenv("SEEN_FILE", "seen.json")
TICKERS_ENV = os.getenv("TICKERS")       # comma-separated tickers
TICKERS_FILE = os.getenv("TICKERS_FILE") # file path with tickers (one per line)
REDDIT_LIMIT = int(os.getenv("REDDIT_LIMIT", "5"))
PRICE_RISE_THRESHOLD = float(os.getenv("PRICE_RISE_THRESHOLD", "5.0"))   # percent
VOLUME_SPIKE_THRESHOLD = float(os.getenv("VOLUME_SPIKE_THRESHOLD", "5.0")) # percent
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
USER_AGENT = os.getenv("USER_AGENT", "VolumeBot/1.0 (+https://example.com)")

# ================= LOGGING =================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("volume_bot")

# ================= HTTP SESSION with retries =================
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

# ================= SEEN TRACKER =================
def load_seen(path: str) -> Dict[str, float]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return {str(k): float(v) for k, v in data.items()}
    except Exception as e:
        logger.warning("Failed to load seen file %s: %s", path, e)
    return {}

def save_seen_atomic(path: str, seen: Dict[str, float]) -> None:
    try:
        dirpath = os.path.dirname(path) or "."
        with tempfile.NamedTemporaryFile("w", delete=False, dir=dirpath, encoding="utf-8") as tf:
            json.dump(seen, tf, ensure_ascii=False, indent=2)
            tmpname = tf.name
        os.replace(tmpname, path)
    except Exception as e:
        logger.exception("Failed to save seen file: %s", e)

def mark_seen(seen: Dict[str, float], key: str) -> None:
    seen[str(key)] = time.time()

# ================= TELEGRAM =================
def send_telegram(msg: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram token or chat id not set; skipping send.")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "HTML",
        "disable_web_page_preview": False
    }
    try:
        r = SESSION.post(url, json=payload, timeout=10)
        r.raise_for_status()
        j = r.json()
        ok = bool(j.get("ok", False))
        if not ok:
            logger.error("Telegram API returned not-ok: %s", j)
        return ok
    except Exception as e:
        logger.exception("Telegram send error: %s", e)
        return False

# ================= TICKERS =================
def load_tickers() -> List[str]:
    tickers: List[str] = []
    if TICKERS_ENV:
        tickers = [t.strip() for t in TICKERS_ENV.split(",") if t.strip()]
    elif TICKERS_FILE and os.path.exists(TICKERS_FILE):
        try:
            with open(TICKERS_FILE, "r", encoding="utf-8") as f:
                tickers = [line.strip() for line in f if line.strip()]
        except Exception as e:
            logger.warning("Failed to load tickers from file %s: %s", TICKERS_FILE, e)
    return tickers

# ================= PRICE + VOLUME =================
def fetch_price_data(symbol: str, period: str = "10d", interval: str = "1d") -> Optional[pd.DataFrame]:
    try:
        df = yf.download(symbol, period=period, interval=interval, progress=False, threads=False)
        if df is None or df.empty:
            logger.debug("No price data for %s", symbol)
            return None
        return df.sort_index()
    except Exception as e:
        logger.exception("Failed to fetch price for %s: %s", symbol, e)
        return None

def _percent_change(now: float, prev: float) -> Optional[float]:
    try:
        if prev == 0:
            return None
        return (now - prev) / prev * 100.0
    except Exception:
        return None

def check_volume_price_spike(df: pd.DataFrame) -> Optional[str]:
    """
    Return formatted message if both price and volume exceed thresholds.
    Returns None otherwise.
    """
    if df is None or len(df) < 2:
        return None
    today = df.iloc[-1]
    prev = df.iloc[-2]

    # Ensure required columns exist
    if ("Close" not in today) or ("Volume" not in today) or ("Close" not in prev) or ("Volume" not in prev):
        return None

    try:
        price_now = float(today["Close"])
        price_prev = float(prev["Close"])
        vol_now = float(today["Volume"])
        vol_prev = float(prev["Volume"])
    except Exception:
        return None

    price_change = _percent_change(price_now, price_prev)
    volume_change = _percent_change(vol_now, vol_prev)

    price_spike = (price_change is not None and price_change >= PRICE_RISE_THRESHOLD)
    volume_spike = (volume_change is not None and volume_change >= VOLUME_SPIKE_THRESHOLD)

    if price_spike and volume_spike:
        return f"📈 Price + Volume spike: {price_change:.1f}% / {volume_change:.1f}%"

    # handle division-by-zero case (prev == 0)
    if price_change is None and volume_change is None and price_now > 0 and vol_now > 0:
        return "📈 Price + Volume spike (previous=0) — new activity"

    return None

# ================= SOCIAL DISCUSSION (Reddit) =================
def fetch_reddit_discussions() -> List[Dict[str, str]]:
    url = f"https://www.reddit.com/r/CryptoCurrency/new/.json?limit={REDDIT_LIMIT}"
    discussions: List[Dict[str, str]] = []
    try:
        r = SESSION.get(url, timeout=10)
        r.raise_for_status()
        children = r.json().get("data", {}).get("children", [])
        for p in children:
            d = p.get("data", {})
            title = d.get("title") or d.get("selftext") or ""
            permalink = d.get("permalink")
            link = f"https://reddit.com{permalink}" if permalink else d.get("url") or ""
            discussions.append({"source": "Reddit", "title": title, "url": link})
    except Exception as e:
        logger.debug("Reddit fetch failed: %s", e)
    return discussions

def format_social_msg(post: Dict[str, str]) -> str:
    source = html.escape(post.get("source", ""))
    title = html.escape(post.get("title", ""))
    url = html.escape(post.get("url", ""))
    return f"💬 <b>{source}</b>\n📰 {title}\n{url}"

# ================= POLYGON NEWS =================
def fetch_polygon_news(symbol: str) -> List[Dict[str, str]]:
    if not POLYGON_API_KEY:
        return []
    query = quote_plus(symbol)
    url = f"https://api.polygon.io/v2/reference/news?query={query}&limit=5&apiKey={POLYGON_API_KEY}"
    news_list: List[Dict[str, str]] = []
    try:
        r = SESSION.get(url, timeout=10)
        r.raise_for_status()
        results = r.json().get("results", [])
        for art in results:
            publisher = art.get("publisher", {}) or {}
            news_list.append({
                "source": publisher.get("name", "Polygon"),
                "title": art.get("title", "") or "",
                "url": art.get("article_url", "") or art.get("url", "")
            })
    except Exception as e:
        logger.debug("Polygon news fetch failed for %s: %s", symbol, e)
    return news_list

def format_news_msg(article: Dict[str, str]) -> str:
    source = html.escape(article.get("source", ""))
    title = html.escape(article.get("title", ""))
    url = html.escape(article.get("url", ""))
    return f"📰 <b>{source}</b>\n{title}\n{url}"

# ================= MAIN LOOP =================
def run_once(seen: Dict[str, float]) -> None:
   s", ", ".join(tickers))
    else:
        logger.info("No tickers configured; skipping price checks")

    for symbol in tickers:
        try:
            df = fetch_price_data(symbol)
            if df is None:
                continue
            spike_msg = check_volume_price_spike(df)
            if spike_msg:
                msg = f"⚠️ <b>{html.escape(symbol)}</b>\n{html.escape(spike_msg)}"
                if send_telegram(msg):
                    logger.info("Sent spike alert for %s", symbol)
                else:
                    logger.warning("Failed to send spike alert for %s", symbol)
                mark_seen(seen, f"spike_{symbol}")
                save_seen_atomic(SEEN_FILE, seen)

                # Fetch news after spike (if available)
                news_list = fetch_polygon_news(symbol)
                for n in news_list:
                    key = n.get("url") or n.get("title")
                    if not key or key in seen:
                        continue
                    if send_telegram(format_news_msg(n)):
                        mark_seen(seen, key)
                        save_seen_atomic(SEEN_FILE, seen)
                    time.sleep(0.3)
            time.sleep(0.3)
        except Exception as e:
            logger.exception("Error processing %s: %s", symbol, e)

    # Social posts
    posts = fetch_reddit_discussions()
    for p in posts:
        key = p.get("url") or p.get("title")
        if not key or key in seen:
            continue
        try:
            if send_telegram(format_social_msg(p)):
                mark_seen(seen, key)
                save_seen_atomic(SEEN_FILE, seen)
                logger.info("Sent social post: %s", key)
        except Exception as e:
            logger.exception("Failed to send social post: %s", e)
        time.sleep(0.3)

def main() -> None:
    logger.info("Volume Bot started 🚀")
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram not fully configured; bot will run but will not send messages.")
    if not POLYGON_API_KEY:
        logger.info("Polygon API key not set; news fetching disabled.")

    seen = load_seen(SEEN_FILE)
    try:
        while True:
            start = time.time()
            run_once(seen)
            elapsed = time.time() - start
            sleep_for = max(0.0, POLL_INTERVAL - elapsed)
            logger.debug("Sleeping for %.1f seconds", sleep_for)
            time.sleep(sleep_for)
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.exception("Fatal error in main loop: %s", e)
    finally:
        try:
            save_seen_atomic(SEEN_FILE, seen)
        except Exception:
            pass
        logger.info("Volume Bot exited")

if __name__ == "__main__":
    main()
