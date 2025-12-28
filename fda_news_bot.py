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
FDA_BASE = os.getenv("FDA_BASE", "https://www.fda.gov")

# New option: whether to SEND items found on the very first run.
# Default: false (mark existing items as seen and do not send them).
INITIAL_RUN_SEND = os.getenv("INITIAL_RUN_SEND", "false").lower() in ("1", "true", "yes")

            json.dump(sorted(list(seen)), f)
    # Load seen set
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
