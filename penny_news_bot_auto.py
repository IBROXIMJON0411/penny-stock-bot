import os
import time
import json
import logging
import html
from datetime import datetime, timezone
from typing import Optional, Dict, List, Tuple
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from deep_translator import GoogleTranslator

# ================= CONFIG =================
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

PRICE_THRESHOLD = float(os.getenv("PRICE_THRESHOLD", "8.0"))
POLL_INTERVAL = 300
MAX_CHECK = 200
PAGE_SIZE_NEWS = 10
MAX_ARTICLES_PER_RUN = 10
SEEN_FILE = "seen_articles.json"

# ================= LOGGING =================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("penny_news_bot")

# ================= HTTP =================
session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=Retry(total=3)))
session.headers.update({"User-Agent": "penny-news-bot/2.0"})

# ================= TRANSLATOR =================
translator = GoogleTranslator(source="en", target="uz")

def uzbek(text: str) -> str:
    try:
        return translator.translate(text)
    except Exception:
        return text

# ================= KEYWORDS =================
KEYWORDS = [
    "earnings", "revenue", "guidance",
    "fda", "approval", "trial", "phase",
    "merger", "acquisition",
    "offering", "registered direct",
    "contract", "partnership", "agreement",
    "trading halt", "reverse split",
    "nasdaq", "nyse", "compliance"
]

GENERIC_BAD_WORDS = [
    "market size", "industry report", "global market",
    "forecast", "cagr", "research report",
    "lawsuit", "class action", "litigation"
]

# ================= UTIL =================
def normalize_url(u: str) -> str:
    p = urlparse(u)
    qs = parse_qs(p.query)
    clean_qs = {k: v for k, v in qs.items() if not k.startswith("utm_")}
    return urlunparse(p._replace(query=urlencode(clean_qs, doseq=True)))

def load_seen() -> Dict[str, float]:
    if not os.path.exists(SEEN_FILE):
        return {}
    with open(SEEN_FILE, "r") as f:
        return json.load(f)

def save_seen(seen: Dict[str, float]):
    with open(SEEN_FILE, "w") as f:
        json.dump(seen, f)

# ================= FILTERS =================
def is_generic_report(article: dict) -> bool:
    text = (article.get("title","") + article.get("summary","")).lower()
    return any(w in text for w in GENERIC_BAD_WORDS)

def is_relevant(article: dict) -> bool:
    text = (article.get("title","") + article.get("summary","")).lower()
    return any(k in text for k in KEYWORDS)

def is_article_for_symbol(article: dict, symbol: str, company: str) -> bool:
    text = (article.get("title","") + article.get("summary","")).lower()
    if f" {symbol.lower()} " in f" {text} ":
        return True
    if company and company.lower() in text:
        return True
    return False

# ================= TELEGRAM =================
def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False
    }
    session.post(url, json=payload, timeout=10)

# ================= POLYGON =================
def get_penny_tickers() -> List[Tuple[str, str, float]]:
    url = "https://api.polygon.io/v3/reference/tickers"
    params = {
        "market": "stocks",
        "active": "true",
        "limit": MAX_CHECK,
        "apiKey": POLYGON_API_KEY
    }
    r = session.get(url, params=params)
    data = r.json().get("results", [])
    result = []
    for t in data:
        price = t.get("market_cap", 0)
        if price and price < PRICE_THRESHOLD:
            result.append((t["ticker"], t.get("name",""), price))
    return result

def fetch_news(symbol: str) -> List[dict]:
    url = "https://api.polygon.io/v2/reference/news"
    params = {
        "query": symbol,
        "limit": PAGE_SIZE_NEWS,
        "apiKey": POLYGON_API_KEY
    }
    r = session.get(url, params=params)
    return r.json().get("results", [])

# ================= FORMAT =================
def format_message(symbol, price, article):
    title = article.get("title","")
    title_uz = uzbek(title)

    published = article.get("published_utc","")
    source = article.get("publisher",{}).get("name","")
    link = article.get("article_url","")

    return (
        f"🚨 <b>{symbol}</b>  <code>${price}</code>\n"
        f"📰 <b>{title_uz}</b>\n"
        f"Source: {html.escape(source)}\n"
        f"Published: {published}\n"
        f"{html.escape(link)}"
    )

# ================= MAIN =================
def main():
    logger.info("penny_news_bot started")
    send_telegram("🚀 Penny News Bot ишга тушди")

    seen = load_seen()

    while True:
        try:
            tickers = get_penny_tickers()
            for symbol, company, price in tickers:
                news = fetch_news(symbol)
                for art in news:
                    url = art.get("article_url")
                    if not url:
                        continue
                    norm = normalize_url(url)
                    if norm in seen:
                        continue
                    if is_generic_report(art):
                        continue
                    if not is_relevant(art):
                        continue
                    if not is_article_for_symbol(art, symbol, company):
                        continue

                    msg = format_message(symbol, price, art)
                    send_telegram(msg)
                    seen[norm] = time.time()
                    save_seen(seen)

        except Exception as e:
            logger.exception("Error")

        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
