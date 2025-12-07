#!/usr/bin/env python3
"""
fda_news_bot.py — FDA press releases мониторинг, Telegramга хабар юбориш.

Environment variables required:
  TELEGRAM_TOKEN
  TELEGRAM_CHAT_ID

Optional:
  POLL_INTERVAL (секунд, default=300)
  SEEN_FILE (default=fda_seen.json)

Requires: requests, feedparser, beautifulsoup4, urllib3
"""
import os
import time
import json
import logging
import signal
import html
from typing import Set, List, Dict

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urljoin

import feedparser
from bs4 import BeautifulSoup

# --- Config ---
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))
SEEN_FILE = os.getenv("SEEN_FILE", "fda_seen.json")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
FDA_BASE = "https://www.fda.gov"

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    raise SystemExit("ERROR: TELEGRAM_TOKEN ва TELEGRAM_CHAT_ID керак!")

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fda_news_bot")

# --- HTTP session with retries ---
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    respect_retry_after_header=True,
    allowed_methods=frozenset(["GET", "POST"])
)
session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
session.headers.update({"User-Agent": "fda-news-bot/1.0"})

# --- Graceful shutdown ---
STOP = False
def _handle_sig(signum, frame):
    global STOP
    log.info("Shutdown signal received (%s). Stopping gracefully...", signum)
    STOP = True

signal.signal(signal.SIGINT, _handle_sig)
signal.signal(signal.SIGTERM, _handle_sig)

# --- Seen persistence (atomic) ---
def load_seen() -> Set[str]:
    try:
        if os.path.exists(SEEN_FILE):
            with open(SEEN_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return set(data)
        return set()
    except Exception:
        log.exception("Failed to load seen file, starting empty.")
        return set()

def save_seen_atomic(seen: Set[str]):
    try:
        tmp = SEEN_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(list(seen), f)
        os.replace(tmp, SEEN_FILE)
    except Exception:
        log.exception("Failed to save seen file")

# --- Telegram send (HTML, escaped) ---
def send_telegram(text: str) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    try:
        resp = session.post(url, json=payload, timeout=15)
        resp.raise_for_status()
        log.info("Sent Telegram message")
        return True
    except Exception:
        log.exception("Failed to send Telegram")
        return False

# --- Candidate RSS/Atom feeds (try these in order) ---
FDA_RSS_CANDIDATES = [
    "https://www.fda.gov/news-events/press-announcements.atom",
    "https://www.fda.gov/news-events/press-announcements.xml",
    "https://www.fda.gov/about-fda/press-releases.atom",
    "https://www.fda.gov/about-fda/press-releases.xml",
    "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feed-press-releases"
]

# --- Fetch news: feedparser first, fallback to scraping ---
def fetch_fda_news() -> List[Dict]:
    # try feeds
    for feed_url in FDA_RSS_CANDIDATES:
        try:
            log.debug("Trying feed: %s", feed_url)
            parsed = feedparser.parse(feed_url)
            status = getattr(parsed, "status", None)
            if status and status >= 400:
                log.warning("Feed %s returned HTTP %s", feed_url, status)
                continue
            entries = parsed.get("entries", []) or []
            if not entries:
                log.debug("Feed %s returned no entries", feed_url)
                continue
            items = []
            for e in entries:
                link = e.get("link") or e.get("id") or ""
                title = (e.get("title") or "").strip()
                published = e.get("published") or e.get("updated") or ""
                uid = e.get("id") or link or (title + "|" + published)
                items.append({
                    "uuid": uid,
                    "title": title,
                    "url": link,
                    "date": published
                })
            log.info("Fetched %d items from feed %s", len(items), feed_url)
            return items
        except Exception:
            log.exception("Error parsing feed %s", feed_url)
            continue

    # fallback scrape
    try:
        fallback_url = "https://www.fda.gov/news-events/press-announcements"
        log.info("Trying fallback scrape: %s", fallback_url)
        r = session.get(fallback_url, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        results = []
        # selectors cover common structures; may need tuning if FDA changes layout
        for a in soup.select("article a, .views-row a, .teaser a, a[href*='/news-events/press-announcements/']"):
            href = a.get("href")
            title = (a.get_text() or "").strip()
            if not href or not title:
                continue
            full = href if href.startswith("http") else urljoin(FDA_BASE, href)
            uid = full
            results.append({
                "uuid": uid,
                "title": title,
                "url": full,
                "date": ""
            })
        if results:
            log.info("Scraped %d items from %s", len(results), fallback_url)
            return results
    except Exception:
        log.exception("Fallback scrape failed")

    log.warning("No FDA news found from feeds or fallback")
    return []

# --- Format message safely ---
def format_msg(item: Dict) -> str:
    title = html.escape(item.get("title") or "No title")
    link = html.escape(item.get("url") or "")
    date = html.escape(item.get("date") or "")
    return (
        f"🔔 <b>FDA</b>\n"
        f"📰 <b>{title}</b>\n"
        f"📅 {date}\n"
        f"🔗 <a href=\"{link}\">Batafsil</a>"
    )

# --- Main loop ---
def main():
    log.info("FDA news bot started. Poll interval: %s sec", POLL_INTERVAL)
    seen = load_seen()

    # startup message (non-fatal)
    try:
        send_telegram("🚀 <b>FDA bot</b> ишга тушди.")
    except Exception:
        log.exception("Startup telegram failed")

    while not STOP:
        try:
            items = fetch_fda_news()
            for it in items:
                uid = it.get("uuid")
                if not uid:
                    continue
                if uid in seen:
                    continue
                try:
                    msg = format_msg(it)
                    send_telegram(msg)
                except Exception:
                    log.exception("Failed to send item")
                seen.add(uid)
                save_seen_atomic(seen)

            # sleep with interruption support
            slept = 0
            while slept < POLL_INTERVAL and not STOP:
                time.sleep(1)
                slept += 1

        except Exception:
            log.exception("Main loop error")
            # small backoff
            for _ in range(5):
                if STOP:
                    break
                time.sleep(1)

    save_seen_atomic(seen)
    log.info("FDA bot stopped.")

if __name__ == "__main__":
    main()
