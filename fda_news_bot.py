#!/usr/bin/env python3
# fda_news_bot_fixed.py
"""
FDA news bot — yaxshilangan versiya

Env required:
  TELEGRAM_TOKEN
  TELEGRAM_CHAT_ID
Optional:
  POLL_INTERVAL (sek, default 300)
  SEEN_FILE (default fda_seen.json)
"""
import os
import time
import json
import logging
import signal
import html
from typing import Set, List, Dict, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urljoin

# --- Config ---
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300")
SEEN_FILE = os.getenv("SEEN_FILE", "fda_seen.json")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
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

# --- Seen persistence (atomic save) ---
def load_seen() -> Set[str]:
    try:
        if os.path.exists(SEEN_FILE):
            with open(SEEN_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return set(data)
                elif isinstance(data, dict):
                    return set(data.keys())
        return set()
    except Exception:
        log.exception("Failed to load seen file, starting with empty set.")
        return set()

def save_seen_atomic(seen: Set[str]):
    try:
        tmp = SEEN_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(list(seen), f)
        os.replace(tmp, SEEN_FILE)
    except Exception:
        log.exception("Failed to save seen file")

# --- Telegram send (HTML safe) ---
def send_telegram(text: str):
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
    except Exception:
        log.exception("Failed to send Telegram message")

# --- Fetch FDA press releases ---
FDA_API = "https://www.fda.gov/api/updates/press-releases.json"

def fetch_fda_news() -> List[Dict]:
    try:
        resp = session.get(FDA_API, timeout=20)
        if resp.status_code == 429:
            ra = resp.headers.get("Retry-After")
            wait = int(ra) if ra and ra.isdigit() else 60
            log.warning("Rate limited by FDA API (429). Waiting %s seconds.", wait)
            time.sleep(wait)
            return []
        resp.raise_for_status()
        j = resp.json()
        # API returns "results" key with list of items
        results = j.get("results")
        if not isinstance(results, list):
            log.warning("Unexpected FDA response format: 'results' missing or not a list")
            return []
        return results
    except Exception:
        log.exception("Error fetching FDA news")
        return []

# --- Format message with separate FDA tag ---
def make_full_url(path_or_url: str) -> str:
    # if path_or_url appears to be full URL, return as-is; otherwise join with FDA_BASE
    if not path_or_url:
        return FDA_BASE
    p = path_or_url.strip()
    if p.startswith("http://") or p.startswith("https://"):
        return p
    # ensure leading slash
    if not p.startswith("/"):
        p = "/" + p
    return urljoin(FDA_BASE, p)

def format_msg(item: Dict) -> str:
    title = html.escape(item.get("title") or "No title")
    # item may have 'path' (relative link) or 'url' — check both
    path = item.get("path") or item.get("url") or ""
    link = make_full_url(path)
    link_escaped = html.escape(link)
    date = html.escape(item.get("release_date") or item.get("date") or "")
    # Construct message: separate FDA tag
    msg = (
        f"🔔 <b>FDA</b>\n"
        f"🚨 <b>Yangi press release</b>\n"
        f"📰 <b>{title}</b>\n"
        f"📅 {date}\n"
        f"🔗 <a href=\"{link_escaped}\">Batafsil</a>"
    )
    return msg

# --- Main loop ---
def main():
    log.info("FDA news bot started. Poll interval: %s sec", POLL_INTERVAL)
    seen = load_seen()
    # send optional startup message
    try:
        send_telegram(f"🚀 <b>FDA bot</b> ishga tushdi. {html.escape(time.ctime())}")
    except Exception:
        pass

    # Main polling
    while not STOP:
        try:
            items = fetch_fda_news()
            if not items:
                log.debug("No items fetched this run.")
            else:
                # iterate in reverse chronological order (API may already be sorted)
                for it in items:
                    uid = it.get("uuid") or it.get("id") or it.get("link") or it.get("path")
                    if not uid:
                        # fallback: use title+date hash
                        uid = (it.get("title", "") + "|" + str(it.get("release_date", "")))[:200]
                    if uid in seen:
                        continue
                    # New item — format and send
                    try:
                        msg = format_msg(it)
                        send_telegram(msg)
                    except Exception:
                        log.exception("Failed to format/send message for uid=%s", uid)
                    seen.add(uid)
                    # Save after each new message to avoid data loss
                    save_seen_atomic(seen)
            # Sleep with small-granularity to allow graceful stop
            slept = 0
            while slept < POLL_INTERVAL and not STOP:
                time.sleep(1)
                slept += 1
        except Exception:
            log.exception("Unexpected error in main loop")
            # small backoff on unexpected error
            for _ in range(10):
                if STOP:
                    break
                time.sleep(1)

    # on exit, persist seen set
    save_seen_atomic(seen)
    log.info("FDA bot stopped.")

if __name__ == "__main__":
    main()
