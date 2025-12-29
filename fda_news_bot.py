import os
import time
import json
import logging
import signal
import html
from typing import Set, List, Dict, Optional, Tuple
from urllib.parse import urljoin
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
load_dotenv()

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import feedparser
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

# --- Config ---
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))
SEEN_FILE = os.getenv("SEEN_FILE", "/var/lib/fda_news_bot/fda_seen.json")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
FDA_BASE = os.getenv("FDA_BASE", "https://www.fda.gov")

# Behaviour
INITIAL_RUN_SEND = os.getenv("INITIAL_RUN_SEND", "false").lower() in ("1", "true", "yes")
MAX_AGE_DAYS = int(os.getenv("MAX_AGE_DAYS", "30"))
SITEMAP_TITLE_FETCH_LIMIT = int(os.getenv("SITEMAP_TITLE_FETCH_LIMIT", "30"))

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    raise SystemExit("ERROR: TELEGRAM_TOKEN ва TELEGRAM_CHAT_ID керак!")

FDA_RSS_CANDIDATES = [
    "https://www.fda.gov/news-events/press-announcements.atom",
    "https://www.fda.gov/news-events/press-announcements.xml",
    "https://www.fda.gov/about-fda/press-releases.atom",
    "https://www.fda.gov/about-fda/press-releases.xml",
    "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feed-press-releases",
]

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fda_news_bot")

# --- HTTP session with retries ---
def _make_session() -> requests.Session:
    s = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        respect_retry_after_header=True,
        allowed_methods=frozenset(["GET", "POST", "HEAD"])
    )
    s.mount("https://", HTTPAdapter(max_retries=retry_strategy))
    s.mount("http://", HTTPAdapter(max_retries=retry_strategy))
    s.headers.update({"User-Agent": "fda-news-bot/1.0"})
    return s

session = _make_session()

# --- Graceful shutdown ---
STOP = False
def _handle_sig(signum, frame):
    global STOP
    log.info("Shutdown signal received (%s). Stopping gracefully...", signum)
    STOP = True

signal.signal(signal.SIGINT, _handle_sig)
signal.signal(signal.SIGTERM, _handle_sig)

# --- Seen persistence --- 
    try:
       url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
        }
        r = session.post(url, json=payload, timeout=15)
        r.raise_for_status()
        log.info("Sent Telegram message")
        return True
    except Exception:
        log.exception("Failed to send Telegram message")
        return False

# --- Date helpers ---
# --- robots/sitemap helpers ---
def get_robots_sitemaps(base_url: str) -> List[str]:
    try:
        robots_url = urljoin(base_url, "/robots.txt")
        r = session.get(robots_url, timeout=10)
        if r.status_code != 200:
            log.debug("robots.txt not available (%s): %s", robots_url, r.status_code)
            return []
        sitemaps = []
    date_iso = None
# --- Fetch logic: feeds -> sitemaps -> fallback scrape ---
def fetch_fda_news() -> List[Dict]:
    # 1) try feeds
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
            items: List[Dict] = []
            for e in entries:
                link = e.get("link") or e.get("id") or ""
                title = (e.get("title") or "").strip()
                pub_iso = None
                if e.get("published_parsed"):
                    pub_iso = parse_struct_time_to_iso(e.get("published_parsed"))
                elif e.get("updated_parsed"):
                    pub_iso = parse_struct_time_to_iso(e.get("updated_parsed"))
                else:
                    pub_iso = parse_date_string(e.get("published") or e.get("updated") or "")
                published = pub_iso or ""
                uid = e.get("id") or link or (title + "|" + published)
                if not link:
                    continue
                items.append({"uid": uid, "title": title, "link": link, "date": published})
            if items:
                log.info("Fetched %d items from feed %s", len(items), feed_url)
                return items
        except Exception:
            log.exception("Error parsing feed %s", feed_url)
            continue

    # 2) sitemaps
    try:
        sitemaps = get_robots_sitemaps(FDA_BASE)
        if not sitemaps:
            sitemaps = [urljoin(FDA_BASE, "/sitemap.xml"), urljoin(FDA_BASE, "/sitemap_index.xml")]
# --- Message formatting ---
def format_msg(item: Dict) -> str:
    title = html.escape(item.get("title") or "No title")
    link = html.escape(item.get("link") or "")
    date = display_date(item.get("date") or "")
    return (
        f"🔔 <b>FDA</b>\n"
        f"📰 <b>{title}</b>\n"
        f"📅 {html.escape(date)}\n"
        f"🔗 <a href=\"{link}\">Batafsil</a>"
    )

# --- Main loop ---
def main():
    log.info("FDA news try")

    # startup ping
    try:
        send_telegram("🚀 <b>FDA bot ishga tushdi</b>")
    except Exception:
        log.exception("Startup telegram failed")

    first_run = not os.path.exists(SEEN_FILE)
    seen = load_seen()

    if first_run and not INITIAL_RUN_SEND:
        try:
            items = fetch_fda_news()
            count = 0
            for it in items:
                uid = it.get("uid")
                if uid:
                    seen.add(uid)
                    count += 1
            save_seen_atomic(seen)
            log.info("Bootstrap: marked %d existing items as seen (no messages sent). Set INITIAL_RUN_SEND=true to override.", count)
        except Exception:
            log.exception("Bootstrap failed")

    while not STOP:
        try:
            items = fetch_fda_news()
            def sort_key(it: Dict):
                d = it.get("date") or ""
                try:
                    return datetime.fromisoformat(d)
                except Exception:
                    return datetime.min
            items_sorted = sorted(items, key=sort_key, reverse=True)

            for it in items_sorted:
                uid = it.get("uid")
                if not uid or uid in seen:
                    continue
                date_iso = it.get("date") or None
                if date_iso and not date_within_max_age(date_iso):
                    log.info("Skipping uid=%s because older than %d days (date=%s)", uid, MAX_AGE_DAYS, date_iso)
                    seen.add(uid)
                    save_seen_atomic(seen)
                    continue
                msg = format_msg(it)
                sent = send_telegram(msg)
                if sent:
                    seen.add(uid)
                    save_seen_atomic(seen)
                else:
                    log.warning("Telegram send failed for uid=%s; will retry next run", uid)

            slept = 0
            while slept < POLL_INTERVAL and not STOP:
                time.sleep(1)
                slept += 1

        except Exception:
            log.exception("Main loop error")
            for _ in range(5):
                if STOP:
                    break
                time.sleep(1)

if __name__ == "__main__":
    main()
