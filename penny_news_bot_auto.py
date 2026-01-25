from __future__ import annotations

import os
import time
import json
import html
import logging
import tempfile
from typing import List, Dict, Optional
from urllib.parse import quote_plus

import requests
import pandas as pd
import yfinance as yf

# ================= CONFIG =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))
SEEN_FILE = os.getenv("SEEN_FILE", "./seen.json")
TICKERS_ENV = os.getenv("TICKERS")
TICKERS_FILE = os.getenv("TICKERS_FILE")
REDDIT_LIMIT = int(os.getenv("REDDIT_LIMIT", "5"))
PRICE_RISE_THRESHOLD = float(os.getenv("PRICE_RISE_THRESHOLD", "5.0"))
VOLUME_SPIKE_THRESHOLD = float(os.getenv("VOLUME_SPIKE_THRESHOLD", "5.0"))
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
    retries = Retry(total=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.headers.update({"User-Agent": USER_AGENT})
    return s

SESSION = create_session()

# ================= SEEN =================
def load_seen(path: str) -> Dict[str, float]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_seen_atomic(path: str, seen: Dict[str, float]) -> None:
    try:
        dirpath = os.path.dirname(path) or "."
        os.makedirs(dirpath, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            "w", delete=False, dir=dirpath, encoding="utf-8"
        ) as tf:
            json.dump(seen, tf, ensure_ascii=False, indent=2)
            tmp = tf.name
        os.replace(tmp, path)
    except Exception as e:
        logger.error("Seen файлни сақлашда хато: %s", e)

def mark_seen(seen: Dict[str, float], key: str) -> None:
    seen[key] = time.time()

# ================= TELEGRAM =================
def send_telegram(msg: str) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "HTML"
    }
    try:
        SESSION.post(url, json=payload, timeout=10)
    except Exception as e:
        logger.error("Telegram хато: %s", e)

# ================= TICKERS =================
def load_tickers() -> List[str]:
    if TICKERS_ENV:
        return [t.strip() for t in TICKERS_ENV.split(",") if t.strip()]
    if TICKERS_FILE and os.path.exists(TICKERS_FILE):
        with open(TICKERS_FILE, "r", encoding="utf-8") as f:
            return [l.strip() for l in f if l.strip()]
    return []

# ================= PRICE & VOLUME =================
def fetch_price_data(symbol: str) -> Optional[pd.DataFrame]:
    try:
        df = yf.download(symbol, period="10d", interval="1d", progress=False)
        if df is None or df.empty:
            return None
        return df
    except Exception:
        return None

def percent_change(a: float, b: float) -> Optional[float]:
    if b == 0:
        return None
    return (a - b) / b * 100

def check_volume_price_spike(df: pd.DataFrame) -> Optional[str]:
    if len(df) < 2:
        return None

    today = df.iloc[-1]
    prev = df.iloc[-2]

    price_change = percent_change(today["Close"], prev["Close"])
    volume_change = percent_change(today["Volume"], prev["Volume"])

    if (
        price_change is not None
        and volume_change is not None
        and price_change >= PRICE_RISE_THRESHOLD
        and volume_change >= VOLUME_SPIKE_THRESHOLD
    ):
        return f"📈 Нарх {price_change:.1f}% ва Volume {volume_change:.1f}% ошди"

    return None

# ================= REDDIT =================
def fetch_reddit_discussions():
    url = f"https://www.reddit.com/r/stocks/new/.json?limit={REDDIT_LIMIT}"
    posts = []
    try:
        r = SESSION.get(url, timeout=10)
        data = r.json()["data"]["children"]
        for p in data:
            d = p["data"]
            posts.append({
                "title": d.get("title", ""),
                "url": f"https://reddit.com{d.get('permalink', '')}"
            })
    except Exception:
        pass
    return posts

# ================= POLYGON NEWS =================
def fetch_polygon_news(symbol: str):
    if not POLYGON_API_KEY:
        return []
    url = (
        "https://api.polygon.io/v2/reference/news?"
        f"query={quote_plus(symbol)}&limit=3&apiKey={POLYGON_API_KEY}"
    )
    news = []
    try:
        r = SESSION.get(url, timeout=10)
        for n in r.json().get("results", []):
            news.append({
                "title": n.get("title", ""),
                "url": n.get("article_url", "")
            })
    except Exception:
        pass
    return news

# ================= MAIN LOOP =================
def run_once(seen: Dict[str, float]) -> None:
    tickers = load_tickers()

    if not tickers:
        logger.info("Тикерлар берилмаган")
        return

    logger.info("Кузатилаётган тикерлар: %s", ", ".join(tickers))

    for symbol in tickers:
        df = fetch_price_data(symbol)
        if not df:
            continue

        spike = check_volume_price_spike(df)
        if spike:
            send_telegram(
                f"⚠️ <b>{html.escape(symbol)}</b>\n{html.escape(spike)}"
            )
            mark_seen(seen, f"spike_{symbol}")
            save_seen_atomic(SEEN_FILE, seen)

            for n in fetch_polygon_news(symbol):
                key = n["url"]
                if key not in seen:
                    send_telegram(
                        f"📰 <b>{html.escape(symbol)}</b>\n"
                        f"{html.escape(n['title'])}\n{n['url']}"
                    )
                    mark_seen(seen, key)
                    save_seen_atomic(SEEN_FILE, seen)

    for p in fetch_reddit_discussions():
        if p["url"] not in seen:
            send_telegram(
                f"💬 <b>Reddit муҳокамаси</b>\n"
                f"{html.escape(p['title'])}\n{p['url']}"
            )
            mark_seen(seen, p["url"])
            save_seen_atomic(SEEN_FILE, seen)

# ================= ENTRY =================
def main():
    logger.info("Бот ишга тушди")
    send_telegram(
        "✅ <b>Бот ишга тушди</b>\n"
        "📊 Volume ва нарх кузатиляпти\n"
        "📰 Янгиликлар ва Reddit муҳокамалари текширилади"
    )

    seen = load_seen(SEEN_FILE)

    while True:
        run_once(seen)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
