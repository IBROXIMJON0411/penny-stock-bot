import os
import time
import json
import requests
import html
from dotenv import load_dotenv

load_dotenv()

# ================= CONFIG =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))
SEEN_FILE = os.getenv("SEEN_FILE", "fda_seen.json")

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    raise SystemExit("TELEGRAM_TOKEN ва TELEGRAM_CHAT_ID шарт")

FDA_API_URL = "https://api.fda.gov/other/announcement.json"
LIMIT = 10
# =========================================


def load_seen():
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    return set()


def save_seen(seen):
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        json.dump(list(seen), f)


def send_telegram(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False
    }
    requests.post(url, json=payload, timeout=15)


def fetch_fda_news():
    params = {
        "limit": LIMIT,
        "sort": "date:desc"
    }

    r = requests.get(FDA_API_URL, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()

    results = []
    for item in data.get("results", []):
        uid = item.get("id")
        title = item.get("title", "FDA Announcement")
        date = item.get("date", "")
        link = item.get("url", "https://www.fda.gov")

        results.append({
            "uid": uid,
            "title": title,
            "date": date,
            "link": link
        })

    return results


def format_message(item):
    return (
        "🔔 <b>FDA Announcement</b>\n"
        f"📰 <b>{html.escape(item['title'])}</b>\n"
        f"📅 {item['date']}\n"
        f"🔗 <a href=\"{item['link']}\">Batafsil</a>"
    )


def main():
    send_telegram("🚀 <b>FDA JSON API bot ишга тушди</b>")
    seen = load_seen()

    while True:
        try:
            news = fetch_fda_news()
            for item in news:
                if item["uid"] in seen:
                    continue

                send_telegram(format_message(item))
                seen.add(item["uid"])
                save_seen(seen)

        except Exception as e:
            send_telegram(f"❌ FDA bot error: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
Express yourself with emojis
💖 👍 😂 🎉
Respond quickly and add fun and personality to your emails
