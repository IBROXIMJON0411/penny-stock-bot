FDA JSON API News Bot — Telegramга янгилик юбориш

Environment required:
  TELEGRAM_TOKEN
  TELEGRAM_CHAT_ID
Optional:
  POLL_INTERVAL (секунд, default 300)
  SEEN_FILE (default fda_seen.json)

Uses FDA enforcement JSON API:
  Drug: https://api.fda.gov/drug/enforcement.json
  Device: https://api.fda.gov/device/enforcement.json
  Food: https://api.fda.gov/food/enforcement.json
"""
import os
import time
import json
import logging
import html
import signal
from typing import Set, Dict, List
import requests

# --- Config ---
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))
SEEN_FILE = os.getenv("SEEN_FILE", "fda_seen.json")

FDA_ENDPOINTS = {
    "Drug": "https://api.fda.gov/drug/enforcement.json?limit=10&sort=date:desc",
    "Device": "https://api.fda.gov/device/enforcement.json?limit=10&sort=date:desc",
    "Food": "https://api.fda.gov/food/enforcement.json?limit=10&sort=date:desc",
}

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    raise SystemExit("ERROR: TELEGRAM_TOKEN ва TELEGRAM_CHAT_ID керак!")

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fda_json_bot")

# --- Graceful shutdown ---
STOP = False
def _handle_sig(signum, frame):
    global STOP
    log.info("Shutdown signal received (%s). Stopping...", signum)
    STOP = True

import signal
signal.signal(signal.SIGINT, _handle_sig)
signal.signal(signal.SIGTERM, _handle_sig)

# --- Seen persistence ---
def load_seen() -> Set[str]:
    if os.path.exists(SEEN_FILE):
        try:
            with open(SEEN_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return set(data if isinstance(data, list) else [])
        except Exception:
            log.exception("Failed to load seen file")
    return set()

def save_seen_atomic(seen: Set[str]):
    try:
        tmp = SEEN_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(list(seen), f)
        os.replace(tmp, SEEN_FILE)
    except Exception:
        log.exception("Failed to save seen file")

# --- Telegram ---
def send_telegram(text: str) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    try:
        r = requests.post(url, json=payload, timeout=15)
        r.raise_for_status()
        log.info("Sent Telegram message")
        return True
    except Exception:
        log.exception("Failed to send Telegram message")
        return False

# --- Fetch FDA JSON ---
def fetch_fda(endpoint_url: str) -> List[Dict]:
    try:
        r = requests.get(endpoint_url, timeout=15)
        r.raise_for_status()
        j = r.json()
        results = j.get("results", [])
        items = []
        for e in results:
            uid = e.get("recall_number") or e.get("classification") + "|" + e.get("product_description", "")
            items.append({
                "uid": uid,
                "type": e.get("product_type", "Unknown"),
                "product": e.get("product_description", ""),
                "reason": e.get("reason_for_recall", ""),
                "date": e.get("recall_initiation_date", ""),
                "link": e.get("url", "")
            })
        return items
    except Exception:
        log.exception("Failed to fetch FDA JSON from %s", endpoint_url)
        return []

# --- Format message ---
def format_msg(item: Dict) -> str:
    return (
        f"🔔 <b>FDA {html.escape(item.get('type', 'Unknown'))} Recall</b>\n"
        f"📰 <b>{html.escape(item.get('product', 'No product'))}</b>\n"
        f"⚠️ {html.escape(item.get('reason', 'No reason'))}\n"
        f"📅 {html.escape(item.get('date', ''))}\n"
        f"🔗 <a href=\"{html.escape(item.get('link',''))}\">Batafsil</a>"
    )

# --- Main loop ---
def main():
    log.info("FDA JSON bot started")
    seen = load_seen()
    send_telegram("🚀 <b>FDA JSON Bot ishga tushdi</b>")

    while not STOP:
        for key, url in FDA_ENDPOINTS.items():
            items = fetch_fda(url)
            for it in items:
                uid = it.get("uid")
                if not uid or uid in seen:
                    continue
                msg = format_msg(it)
                if send_telegram(msg):
                    seen.add(uid)
                    save_seen_atomic(seen)
        slept = 0
        while slept < POLL_INTERVAL and not STOP:
            time.sleep(1)
            slept += 1

if __name__ == "__main__":
    main()
