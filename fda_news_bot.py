fda_news_bot.py — fetch (feeds, robots/sitemap, fallback scrape) + Telegram alerts.
Behaviour:
 - INITIAL_RUN_SEND=false (default) => first run: mark existing items as seen, do NOT send them.
 - Parses dates from feed/sitemap/page and shows YYYY-MM-DD in message.
 - Filters out items older than MAX_AGE_DAYS (if date known).
Requires: python-dotenv, requests, feedparser, beautifulsoup4, urllib3
"""
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

# Config
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))
SEEN_FILE = os.getenv("SEEN_FILE", "fda_seen.json")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
FDA_BASE = os.getenv("FDA_BASE", "https://www.fda.gov")

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

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fda_news_bot")

# HTTP session with retries
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

# Date helpers
def parse_struct_time_to_iso(st) -> Optional[str]:
    try:
        if not st:
            return None
        dt = datetime.fromtimestamp(time.mktime(st), tz=timezone.utc)
        return dt.isoformat()
    except Exception:
        return None

def try_parse_iso(dt_str: Optional[str]) -> Optional[str]:
    if not dt_str:
        return None
    s = dt_str.strip()
    fmts = [
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except Exception:
            continue
    try:
        parsed = feedparser._parse_date(s)
        if parsed:
            return parse_struct_time_to_iso(parsed)
    except Exception:
        pass
    return None

def parse_date_string(s: Optional[str]) -> Optional[str]:
    return try_parse_iso(s) if s else None

def date_within_max_age(iso_date: Optional[str]) -> bool:
    if not iso_date:
        return True
    try:
        dt = datetime.fromisoformat(iso_date)
        now = datetime.now(dt.tzinfo or timezone.utc)
        return now - dt <= timedelta(days=MAX_AGE_DAYS)
    except Exception:
        return True

def display_date(iso_date: Optional[str]) -> str:
    if not iso_date:
        return ""
    try:
        dt = datetime.fromisoformat(iso_date)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return iso_date

# robots -> sitemap
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
    try:
        r = session.get(sitemap_url, timeout=20)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        items = []
        for url_el in root.findall('.//{*}url'):
            loc_el = url_el.find('.//{*}loc') or url_el.find('loc')
            if loc_el is None:
                continue
            loc = (loc_el.text or "").strip()
            if not loc:
                continue
            lastmod_el = url_el.find('.//{*}lastmod') or url_el.find('lastmod')
            lastmod = (lastmod_el.text or "").strip() if lastmod_el is not None else ""
            news_title_el = url_el.find('.//{http://www.google.com/schemas/sitemap-news/}title')
            title = news_title_el.text.strip() if news_title_el is not None and news_title_el.text else ""
            items.append({"loc": loc, "lastmod": lastmod, "title": title})
        if not items:
            for sm in root.findall('.//{*}sitemap'):
                loc_el = sm.find('.//{*}loc') or sm.find('loc')
                if loc_el is None:
                    continue
                loc = (loc_el.text or "").strip()
                if loc:
                    items.append({"loc": loc, "lastmod": "", "title": ""})
        if not items:
            for loc_el in root.findall('.//{*}loc'):
                loc_text = (loc_el.text or "").strip()
                if loc_text:
                    items.append({"loc": loc_text, "lastmod": "", "title": ""})
        return items
    except Exception:
        log.exception("Failed to parse sitemap %s", sitemap_url)
        return []

def fetch_title_and_date_from_page(url: str) -> Tuple[str, Optional[str]]:
    title = ""
    date_iso = None
    try:
        r = session.get(url, timeout=12)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        h1 = soup.find("h1")
        if h1 and h1.get_text(strip=True):
            title = h1.get_text(strip=True)
        if not title:
            og = soup.find("meta", property="og:title")
            if og and og.get("content"):
                title = og.get("content").strip()
        if not title:
            tt = soup.find("title")
            if tt and tt.get_text(strip=True):
                title = tt.get_text(strip=True)
        m = soup.find("meta", {"property": "article:published_time"}) or soup.find("meta", {"name": "article:published_time"})
        if m and m.get("content"):
            date_iso = parse_date_string(m.get("content"))
            return title, date_iso
        for name in ("pubdate", "publishdate", "publish-date", "date", "dc.date", "dc.date.issued", "prpubdate"):
            m = soup.find("meta", {"name": name})
            if m and m.get("content"):
                date_iso = parse_date_string(m.get("content"))
                if date_iso:
                    return title, date_iso
        t = soup.find("time")
        if t:
            dt = t.get("datetime") or t.get_text()
            date_iso = parse_date_string(dt)
            if date_iso:
                return title, date_iso
        candidates = soup.select(".date, .posted-date, .published, .updated")
        for c in candidates:
            txt = c.get_text(strip=True)
            date_iso = parse_date_string(txt)
            if date_iso:
                return title, date_iso
    except Exception:
        log.debug("Failed to fetch title/date for %s", url, exc_info=True)
    return title, date_iso

# fetch news
def fetch_fda_news() -> List[Dict]:
    # 1) feeds
    for feed_url in FDA_RSS_CANDIDATES:
        try:
            parsed = feedparser.parse(feed_url)
            status = getattr(parsed, "status", None)
            if status and status >= 400:
                log.warning("Feed %s returned HTTP %s", feed_url, status)
                continue
            entries = parsed.get("entries", []) or []
            if not entries:
                continue
            items = []
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
        collected = []
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
            items = []
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

    # 3) fallback scrapes (several variants)
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
            results = []
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

def main():
    log.info("FDA news try")
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
            # prefer items with date; sort newest first
            def sort_key(it):
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
