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
import xml.etree.ElementTree as ET

# --- Config ---
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))
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

# --- Seen persistence ---
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

# --- Telegram send ---
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
        log.exception("Failed to send Telegram")

# --- Fetch FDA press releases via RSS ---
FDA_RSS = "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feed-press-releases"

def fetch_fda_news() -> List[Dict]:
    try:
        resp = session.get(FDA_RSS, timeout=20)
        resp.raise_for_status()

        root = ET.fromstring(resp.text)

        items = []
        for item in root.findall("./channel/item"):
            title = item.findtext("title", "")
            link = item.findtext("link", "")
            date = item.findtext("pubDate", "")

            items.append({
                "uuid": link,   # unique ID
                "title": title,
                "url": link,
                "date": date
            })

        return items
    except Exception:
        log.exception("Error fetching FDA news")
        return []

# --- Format message ---
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

    try:
        send_telegram("🚀 <b>FDA bot</b> ишга тушди.")
    except:
        pass

    while not STOP:
        try:
            items = fetch_fda_news()

            for it in items:
                uid = it["uuid"]
                if uid in seen:
                    continue

                try:
                    msg = format_msg(it)
                    send_telegram(msg)
                except:
                    log.exception("Failed to send item")

                seen.add(uid)
                save_seen_atomic(seen)

            # Sleep
            for _ in range(POLL_INTERVAL):
                if STOP:
                    break
                time.sleep(1)

        except Exception:
            log.exception("Main loop error")
            time.sleep(5)

    save_seen_atomic(seen)
    log.info("FDA bot stopped.")

if __name__ == "__main__":
    main()
