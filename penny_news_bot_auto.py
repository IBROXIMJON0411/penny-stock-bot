from __future__ import annotations

import os
import time
import json
import html
import logging
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
    retries = Retry(total=3, backoff_factor=0.5,
                    status_forcelist=(429, 500, 502, 503, 504))
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": USER_AGENT})
    return s

SESSION = create_session()

# ================= SEEN =================
def load_seen(path: str) -> Dict[str, float]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return {str(k): float(v) for k, v in data.items()}
    except Exception as e:
        logger.warning("Seen С„Р°Р№Р»РЅРё СЋРєР»Р°С€РґР° С…Р°С‚Рѕ: %s", e)
    return {}

def save_seen(path: str, seen: Dict[str, float]) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(seen, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error("Seen save error: %s", e)

def mark_seen(seen: Dict[str, float], key: str) -> None:
    seen[str(key)] = time.time()

# ================= TELEGRAM =================
def send_telegram(msg: str) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        SESSION.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": msg,
                "parse_mode": "HTML"
            },
            timeout=10
        )
    except Exception as e:
        logger.error("Telegram С…Р°С‚Рѕ: %s", e)

# ================= TICKERS =================
def load_tickers() -> List[str]:
    if TICKERS_ENV:
        return [t.strip() for t in TICKERS_ENV.split(",") if t.strip()]
    if TICKERS_FILE and os.path.exists(TICKERS_FILE):
        try:
            with open(TICKERS_FILE, "r", encoding="utf-8") as f:
                return [l.strip() for l in f if l.strip()]
        except Exception:
            pass
    return []

# ================= PRICE & VOLUME =================
def fetch_price_data(symbol: str) -> Optional[pd.DataFrame]:
    try:
        df = yf.download(symbol, period="10d", interval="1d", progress=False)
        return df if df is not None and not df.empty else None
    except Exception:
        return None

def percent_change(a: float, b: float) -> Optional[float]:
    if b == 0:
        return None
    return (a - b) / b * 100

def check_volume_price_spike(df: pd.DataFrame) -> Optional[str]:
    if df is None or len(df) < 2:
        return None

    t, p = df.iloc[-1], df.iloc[-2]
    try:
        pc = percent_change(float(t["Close"]), float(p["Close"]))
        vc = percent_change(float(t["Volume"]), float(p["Volume"]))
    except Exception:
        return None

    if pc is not None and vc is not None:
        if pc >= PRICE_RISE_THRESHOLD and vc >= VOLUME_SPIKE_THRESHOLD:
            return f"рџ“€ РќР°СЂС… {pc:.1f}% РІР° Volume {vc:.1f}% РѕС€РґРё"

    return None

# ================= REDDIT =================
def fetch_reddit_discussions():
    posts = []
    try:
        r = SESSION.get(
            f"https://www.reddit.com/r/stocks/new/.json?limit={REDDIT_LIMIT}",
            timeout=10
        )
        for p in r.json().get("data", {}).get("children", []):
            d = p["data"]
            posts.append({
                "title": d.get("title", ""),
                "url": f"https://reddit.com{d.get('permalink', '')}"
            })
    except Exception:
        pass
    return posts

# ================= POLYGON =================
def fetch_polygon_news(symbol: str):
    if not POLYGON_API_KEY:
        return []
    url = (
        "https://api.polygon.io/v2/reference/news?"
        f"query={quote_plus(symbol)}&limit=3&apiKey={POLYGON_API_KEY}"
    )
    try:
        r = SESSION.get(url, timeout=10)
        return [
            {"title": n.get("title", ""), "url": n.get("article_url", "")}
            for n in r.json().get("results", [])
        ]
    except Exception:
        return []

# ================= MAIN =================
def run_once(seen: Dict[str, float]):
    for symbol in load_tickers():
        df = fetch_price_data(symbol)
        spike = check_volume_price_spike(df)
        if spike:
            send_telegram(f"вљ пёЏ <b>{symbol}</b>\n{spike}")
            mark_seen(seen, f"spike_{symbol}")
            save_seen(SEEN_FILE, seen)

            for n in fetch_polygon_news(symbol):
                key = n["url"]
                if key not in seen:
                    send_telegram(f"рџ“° <b>{symbol}</b>\n{n['title']}\n{n['url']}")
                    mark_seen(seen, key)
                    save_seen(SEEN_FILE, seen)

    for p in fetch_reddit_discussions():
        if p["url"] not in seen:
            send_telegram(f"рџ’¬ <b>Reddit</b>\n{p['title']}\n{p['url']}")
            mark_seen(seen, p["url"])
            save_seen(SEEN_FILE, seen)

def main():
    logger.info("Р‘РѕС‚ РёС€РіР° С‚СѓС€РґРё")
    send_telegram("вњ… <b>Р‘РѕС‚ РёС€РіР° С‚СѓС€РґРё</b>")
    seen = load_seen(SEEN_FILE)
    while True:
        run_once(seen)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
