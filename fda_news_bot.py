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

# --- Config (env) ---
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

# Candidate feed URLs (try in order)
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

# --- HTTP session with retries (global) ---
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

session: requests.Session = _make_session()

# --- Graceful shutdown ---
STOP = False
def _handle_sig(signum, frame):
    global STOP
    log.info("Shutdown signal received (%s). Stopping gracefully...", signum)
    STOP = True

signal.signal(signal.SIGINT, _handle_sig)
signal.signal(signal.SIGTERM, _handle_sig)

# --- Seen persistence (atomic save) ---
# --- Telegram send ---
def send_telegram(text: str) -> bool:
    global session
    try:
        if session is None:
            session = _make_session()
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

# --- Date parsing helpers ---
# --- helpers: robots -> sitemap discovery ---
def get_robots_sitemaps(base_url: str) -> List[str]:
    try:
        robots_url = urljoin(base_url, "/robots.txt")
        r = session.get(robots_url, timeout=10)
        if r.status_code != 200:
            log.debug("robots.txt not available (%s): %s", robots_url, r.status_code)
            return []
        sitemaps: List[str] = []
        for line in r.text.splitlines():
            line = line.strip()
            if line.lower().startswith("sitemap:"):
                s = line.split(":", 1)[1].strip()
                if s:
                    sitemaps.append(s)
        log.info("robots.txt sitemaps: %s", sitemaps)
        return sitemaps
    except Exception:
        log.exception("Failed to fetch robots.txt")
        return []

def parse_sitemap_urls(sitemap_url: str) -> List[Dict]:
    try:
        r = session.get(sitemap_url, timeout=20)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        items: List[Dict] = []
    date_iso: Optional[str] = None
# --- Robust fetch: feeds -> robots/sitemap -> fallback scrape ---
def fetch_fda_news() -> List[Dict]:
    # 1) try known feeds
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
                pub_iso: Optional[str] = None
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

    # 2) robots.txt -> sitemaps -> parse for press/news urls
    try:
        sitemaps = get_robots_sitemaps(FDA_BASE)
        if not sitemaps:
            sitemaps = [urljoin(FDA_BASE, "/sitemap.xml"), urljoin(FDA_BASE, "/sitemap_index.xml")]
        collected: List[Dict] = []
        inspected = 0
        for sm in sitemaps:
            entries = parse_sitemap_urls(sm)
            if not entries:
                continue
            sitemap_locs = [e["loc"] for e in entries if e.get("loc", "").lower().endswith(".xml")]
            if sitemap_locs:
                for sub in sitemap_locs:
                    sub_entries = parse_sitemap_urls(sub)
                    if sub_entries:
                        entries.extend(sub_entries)
            for e in entries:
                loc = e.get("loc") or ""
                if not loc:
                    continue
                inspected += 1
                low = loc.lower()
                if any(k in low for k in ("press", "press-release", "press-announcement", "press-announcements", "press-releases", "news-events", "/news-","/news/","/press/")):
                    collected.append(e)
            if collected:
                break
        log.info("Sitemap inspection: inspected %d urls, collected %d candidate news urls", inspected, len(collected))
        if collected:
            items: List[Dict] = []
            for e in collected[:SITEMAP_TITLE_FETCH_LIMIT]:
                loc = e.get("loc")
                title = e.get("title") or ""
                lastmod = e.get("lastmod") or ""
                lastmod_iso = parse_date_string(lastmod) or ""
                if not title or not lastmod_iso:
                    t, d = fetch_title_and_date_from_page(loc)
                    if not title:
                        title = t
                    if not lastmod_iso and d:
                        lastmod_iso = d
                uid = loc
                items.append({"uid": uid, "title": title, "link": loc, "date": lastmod_iso})
            if items:
                log.info("Discovered %d items from sitemap", len(items))
                return items
    except Exception:
        log.exception("Sitemap processing failed")

    # 3) fallback scrapes
    try:
        fallback_paths = [
            "/news-events/press-announcements",
            "/news-events/press-releases",
            "/news-events",
            "/about-fda/press-releases",
            "/press-announcements"
        ]
        for path in fallback_paths:
            fallback = urljoin(FDA_BASE, path)
            log.info("Trying fallback scrape: %s", fallback)
            r = session.get(fallback, timeout=15)
            if r.status_code >= 400:
                log.debug("Fallback page returned status %s for %s", r.status_code, fallback)
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            results: List[Dict] = []
            for a in soup.select("article a, .views-row a, .teaser a, a[href*='/news-events/'], a[href*='/press-']"):
                href = a.get("href")
                title = (a.get_text() or "").strip()
                if not href or not title:
                    continue
                full = href if href.startswith("http") else urljoin(FDA_BASE, href)
                t, d = fetch_title_and_date_from_page(full)
                final_title = title if title else t
                results.append({"uid": full, "title": final_title, "link": full, "date": d or ""})
            if results:
                log.info("Scraped %d items from fallback %s", len(results), fallback)
                return results
    except Exception:
        log.exception("Fallback scrape failed")

    log.warning("No FDA news found by any method")
    return []

# --- Format message ---
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

    # startup ping (won't crash if Telegram unavailable)
    try:
        send_telegram("🚀 <b>FDA bot ishga tushdi</b>")
    except Exception:
        log.exception("Startup telegram failed")

    first_run = not os.path.exists(SEEN_FILE)
    seen = load_seen()

    # Bootstrap: if first run and INITIAL_RUN_SEND is False, mark current items as seen (do not send)
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

            # sort newest first (items with parsable date come first)
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

            # sleep with early exit
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
