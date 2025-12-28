#!/usr/bin/env python3
"""
fda_news_bot.py — robust fetch (feeds, robots/sitemap, fallback scrape) + Telegram alerts.
Requires: python-dotenv, requests, feedparser, beautifulsoup4, urllib3, lxml
"""
import os
import time
import json
import logging
import signal
import html
from typing import Set, List, Dict
from urllib.parse import urljoin

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
SEEN_FILE = os.getenv("SEEN_FILE", "fda_seen.json")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
FDA_BASE = "https://www.fda.gov"

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    raise SystemExit("ERROR: TELEGRAM_TOKEN ва TELEGRAM_CHAT_ID керак!")

# Candidate feeds (try in order)
FDA_RSS_CANDIDATES = [
    "https://www.fda.gov/news-events/press-announcements.atom",
    "https://www.fda.gov/news-events/press-announcements.xml",
    "https://www.fda.gov/about-fda/press-releases.atom",
    "https://www.fda.gov/about-fda/press-releases.xml",
    "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feed-press-releases",
]

SITEMAP_TITLE_FETCH_LIMIT = 30

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
    allowed_methods=frozenset(["GET", "POST", "HEAD"])
)
session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
session.mount("http://", HTTPAdapter(max_retries=retry_strategy))
session.headers.update({"User-Agent": "fda-news-bot/1.0"})

# Graceful shutdown
STOP = False
def _handle_sig(signum, frame):
    global STOP
    log.info("Shutdown signal received (%s). Stopping gracefully...", signum)
    STOP = True

signal.signal(signal.SIGINT, _handle_sig)
signal.signal(signal.SIGTERM, _handle_sig)

# Seen persistence
def ensure_seen_dir():
    d = os.path.dirname(SEEN_FILE)
    if d and not os.path.exists(d):
        try:
            os.makedirs(d, exist_ok=True)
        except Exception:
            log.exception("Failed to create seen directory %s", d)

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
        ensure_seen_dir()
        tmp = SEEN_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(list(seen), f)
        os.replace(tmp, SEEN_FILE)
    except Exception:
        log.exception("Failed to save seen file")

# Telegram send
def send_telegram(text: str) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    try:
        r = session.post(url, json=payload, timeout=15)
        r.raise_for_status()
        log.info("Sent Telegram message")
        return True
    except Exception:
        log.exception("Failed to send Telegram message")
        return False

# robots -> sitemap discovery
def get_robots_sitemaps(base_url: str) -> List[str]:
    try:
        robots_url = urljoin(base_url, "/robots.txt")
        r = session.get(robots_url, timeout=10)
        if r.status_code != 200:
            log.debug("robots.txt not available (%s): %s", robots_url, r.status_code)
            return []
        sitemaps = []
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
    """Return list of {loc, lastmod, title} OR if sitemap_index -> list of sitemap locs"""
    try:
        r = session.get(sitemap_url, timeout=20)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        items = []

        # Detect <urlset> entries
        for url_el in root.findall('.//{*}url'):
            loc_el = url_el.find('.//{*}loc') or url_el.find('loc')
            loc = (loc_el.text or "").strip() if loc_el is not None else ""
            if not loc:
                continue
            lastmod_el = url_el.find('.//{*}lastmod') or url_el.find('lastmod')
            lastmod = (lastmod_el.text or "").strip() if lastmod_el is not None else ""
            news_title_el = url_el.find('.//{http://www.google.com/schemas/sitemap-news/}title')
            title = news_title_el.text.strip() if news_title_el is not None and news_title_el.text else ""
            items.append({"loc": loc, "lastmod": lastmod, "title": title})

        # If no url entries found, maybe it's a sitemap index
        if not items:
            for sm in root.findall('.//{*}sitemap'):
                loc_el = sm.find('.//{*}loc') or sm.find('loc')
                loc = (loc_el.text or "").strip() if loc_el is not None else ""
                if loc:
                    items.append({"loc": loc, "lastmod": "", "title": ""})

        # fallback: any <loc>
        if not items:
            for loc_el in root.findall('.//{*}loc'):
                loc_text = (loc_el.text or "").strip()
                if loc_text:
                    items.append({"loc": loc_text, "lastmod": "", "title": ""})

        return items
    except Exception:
        log.exception("Failed to parse sitemap %s", sitemap_url)
        return []

def fetch_title_from_page(url: str) -> str:
    try:
        r = session.get(url, timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        h1 = soup.find("h1")
        if h1 and h1.get_text(strip=True):
            return h1.get_text(strip=True)
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            return og.get("content").strip()
        tt = soup.find("title")
        if tt and tt.get_text(strip=True):
            return tt.get_text(strip=True)
    except Exception:
        log.debug("Failed to fetch title for %s", url, exc_info=True)
    return ""

# Robust fetch
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
            items = []
            for e in entries:
                link = e.get("link") or e.get("id") or ""
                title = (e.get("title") or "").strip()
                published = e.get("published") or e.get("updated") or ""
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
        collected = []
        inspected = 0

        # Iterate top-level sitemaps; handle sitemap-index by parsing sub-sitemaps
        for sm in sitemaps:
            entries = parse_sitemap_urls(sm)
            if not entries:
                continue

            # If these look like sitemap files (sub-sitemap index), parse them
            sitemap_locs = [e["loc"] for e in entries if e.get("loc", "").lower().endswith(".xml")]
            if sitemap_locs:
                for sub in sitemap_locs:
                    sub_entries = parse_sitemap_urls(sub)
                    if not sub_entries:
                        continue
                    entries.extend(sub_entries)

            # Now entries should contain url entries; filter for press/news-like urls
            for e in entries:
                loc = e.get("loc") or ""
                if not loc:
                    continue
                inspected += 1
                low = loc.lower()
                # broadened pattern list
                if any(k in low for k in ("press", "press-release", "press-announcement", "press-announcements", "press-releases", "news-events", "/news-","/news/","/press/")):
                    collected.append(e)
            # if we found anything, stop (we prefer the first sitemap with news)
            if collected:
                break

        log.info("Sitemap inspection: inspected %d urls, collected %d candidate news urls", inspected, len(collected))

        if collected:
            items = []
            for e in collected[:SITEMAP_TITLE_FETCH_LIMIT]:
                loc = e.get("loc")
                title = e.get("title") or ""
                lastmod = e.get("lastmod") or ""
                if not title:
                    title = fetch_title_from_page(loc)
                uid = loc
                items.append({"uid": uid, "title": title, "link": loc, "date": lastmod})
            if items:
                log.info("Discovered %d items from sitemap", len(items))
                return items
    except Exception:
        log.exception("Sitemap processing failed")

    # 3) fallback: try several likely pages (several variants)
    try:
        fallback_paths = [
            "/news-events/press-announcements",
            "/news-events/press-announcements/",
            "/news-events/press-releases",
            "/news-events/press-releases/",
            "/news-events",
            "/about-fda/press-releases"
        ]
        for path in fallback_paths:
            fallback = urljoin(FDA_BASE, path)
            log.info("Trying fallback scrape: %s", fallback)
            r = session.get(fallback, timeout=15)
            if r.status_code >= 400:
                log.debug("Fallback page returned status %s for %s", r.status_code, fallback)
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            results = []
            for a in soup.select("article a, .views-row a, .teaser a, a[href*='/news-events/'], a[href*='/press-']"):
                href = a.get("href")
                title = (a.get_text() or "").strip()
                if not href or not title:
                    continue
                full = href if href.startswith("http") else urljoin(FDA_BASE, href)
                uid = full
                results.append({"uid": uid, "title": title, "link": full, "date": ""})
            if results:
                log.info("Scraped %d items from fallback %s", len(results), fallback)
                return results
    except Exception:
        log.exception("Fallback scrape failed")

    log.warning("No FDA news found by any method")
    return []

def format_msg(item: Dict) -> str:
    title = html.escape(item.get("title") or "No title")
    link = html.escape(item.get("link") or "")
    date = html.escape(item.get("date") or "")
    return (
        f"🔔 <b>FDA</b>\n"
        f"📰 <b>{title}</b>\n"
        f"📅 {date}\n"
        f"🔗 <a href=\"{link}\">Batafsil</a>"
    )

def main():
    log.info("FDA news try")

    try:
        send_telegram("🚀 <b>FDA bot ishga tushdi</b>")
    except Exception:
        log.exception("Startup telegram failed")

    seen = load_seen()

    while not STOP:
        try:
            items = fetch_fda_news()

            for it in items:
                uid = it.get("uid")
                if not uid:
                    continue
                if uid in seen:
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
