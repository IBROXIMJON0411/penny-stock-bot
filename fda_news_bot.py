#!/usr/bin/env python3
import os
import time
import json
import html
import logging
from datetime import datetime
from typing import Dict, List
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from deep_translator import GoogleTranslator

# ================== CONFIG ==================
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

PRICE_THRESHOLD = 8.0
POLL_INTERVAL = 300
MAX_TICKERS = 200
NEWS_LIMIT = 10
SEEN_FILE = "seen_articles.json"

# ================== LOG ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger("penny_news_uz_bot")

# ================== HTTP ==================
session = requests.Session()
session.mount(
    "https://",
    HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1))
)
session.headers.update({"User-Agent": "penny-news-uz-bot/1.0"})

# ================== TRANSLATOR ==================
translator = GoogleTranslator(source="en", target="uz")

def uzbek(text: str) -> str:
    try:
        return translator.translate(text)
    except Exception:
        return text

def short_ai_summary_uz(title: str) -> str:
    try:
        prompt = f"Бир жумла билан трейдер учун хулоса қилиб бер: {title}"
        return translator.translate(prompt)
    except Exception:
        return title

# ================== KEYWORDS ==================
KEYWORDS = [
    "earnings", "revenue", "guidance",
    "fda", "approval", "trial", "phase",
    "merger", "acquisition",
    "offering", "registered direct",
    "contract", "partnership",
    "trading halt", "reverse split",
    "nasdaq", "nyse", "compliance"
]

BAD_WORDS = [
    "market size", "industry report",
    "global market", "forecast",
    "research report", "cagr"
]

# ================== UTILS ==================
def normalize_url(u: str) -> str:
    p = urlparse(u)
    qs = parse_qs(p.query)
    clean = {k: v for k, v in qs.items() if not k.startswith("utm_")}
    return urlunparse(p._replace(query=urlencode(clean, doseq=True)))

def load_seen() -> Dict[str, float]:
    if not os.path.exists(SEEN_FILE):
        return {}
    with open(SEEN_FILE, "r") as f:
        return json.load(f)

def save_seen(seen: Dict[str, float]):
    with open(SEEN_FILE, "w") as f:
        json.dump(seen, f)

# ================== FILTERS ==================
def is_relevant(article: dict) -> bool:
    text = (article.get("title","") + article.get("summary","")).lower()
    return any(k in text for k in KEYWORDS)

def is_bad(article: dict) -> bool:
    text = (article.get("title","") + article.get("summary","")).lower()
    return any(b in text for b in BAD_WORDS)

def article_matches_symbol(article: dict, symbol: str, company: str) -> bool:
    text = (article.get("title","") + article.get("summary","")).lower()
    if f" {symbol.lower()} " in f" {text} ":
        return True
    if company and company.lower() in text:
        return True
    return False

# ================== TELEGRAM ==================
def send_telegram(msg: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "HTML",
        "disable_web_page_preview": False
    }
    session.post(url, json=payload, timeout=10)

# ================== POLYGON ==================
def get_penny_tickers() -> List[tuple]:
    url = "https://api.polygon.io/v3/reference/tickers"
    params = {
        "market": "stocks",
        "active": "true",
        "limit": MAX_TICKERS,
        "apiKey": POLYGON_API_KEY
    }
    r = session.get(url, params=params, timeout=15)
    data = r.json().get("results", [])

    result = []
    for t in data:
        price = t.get("last_trade", {}).get("p")
        if price and price <= PRICE_THRESHOLD:
            result.append((t["ticker"], t.get("name",""), price))
    return result

def fetch_news(symbol: str) -> List[dict]:
    url = "https://api.polygon.io/v2/reference/news"
    params = {
        "query": symbol,
        "limit": NEWS_LIMIT,
        "apiKey": POLYGON_API_KEY
    }
    r = session.get(url, params=params, timeout=15)
    return r.json().get("results", [])

# ================== FORMAT ==================
def format_message(symbol, price, article):
    title_en = article.get("title","")
    title_uz = uzbek(title_en)
    ai_uz = short_ai_summary_uz(title_en)

    source = article.get("publisher",{}).get("name","")
    published = article.get("published_utc","")
    link = article.get("article_url","")

    return (
        f"🚨 <b>{symbol}</b>  <code>${price:.2f}</code>\n\n"
        f"📰 <b>{html.escape(title_uz)}</b>\n\n"
        f"🧠 <i>{html.escape(ai_uz)}</i>\n\n"
        f"🏢 Манба: {html.escape(source)}\n"
        f"⏰ Вақт: {published}\n\n"
        f"🔗 {html.escape(link)}"
    )

# ================== MAIN ==================
def main():
    logger.info("UZ Penny News Bot started")
    send_telegram("🚀 Узбек Penny News Bot ишга тушди")

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
                    if is_bad(art):
                        continue
                    if not is_relevant(art):
                        continue
                    if not article_matches_symbol(art, symbol, company):
                        continue

                    msg = format_message(symbol, price, art)
                    send_telegram(msg)

                    seen[norm] = time.time()
                    save_seen(seen)

        except Exception:
            logger.exception("ERROR")

        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
