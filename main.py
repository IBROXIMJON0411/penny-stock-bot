import os
import time
import signal
import requests
import logging
import redis
from typing import Optional, List, Tuple

# Muhit o'zgaruvchilari (Render'да сақланг)
TOKEN = os.environ.get("7762047492:AAFartWb8w-nmi8Cqbl4rdBYmjW1yk8xDRY")
CHAT_ID = os.environ.get("7288340454")
POLY_KEY = os.environ.get("9IMXlZEBOvSD7LQTTsNd_pRZ0T5sFCDm")  # "9IMXlZEBOvSD7LQTTsNd_pRZ0T5sFCDm"
REDIS_URL = os.environ.get("redis://red-d4bqb56r433s73d3l9gg:6379")  # Render Redis'дан

if not (TOKEN and CHAT_ID and POLY_KEY and REDIS_URL):
    raise SystemExit("Iltimos, TELEGRAM_TOKEN, CHAT_ID, POLYGON_API_KEY va REDIS_URL ni belgilang.")

try:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()  # Redis текшириш
    logging.info("Redis connected successfully.")
except Exception as e:
    raise SystemExit(f"Redis connection failed: {e}")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

STOP = False
def handle_sigterm(signum, frame):
    global STOP
    STOP = True
    logging.info("Received shutdown signal.")
signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)

def send_message(msg: str):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        resp = requests.post(url, data=data, timeout=10)
        if resp.status_code != 200:
            logging.warning("Telegram send failed: %s", resp.text)
    except Exception as e:
        logging.exception("Telegram request failed: %s", e)

def api_get_with_retry(url: str, max_retries: int = 3) -> Optional[dict]:
    """API retry билан"""
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                return resp.json()
            else:
                logging.warning(f"API error {resp.status_code}: {url}")
        except Exception as e:
            logging.debug(f"Retry {attempt+1}/{max_retries} for {url}: {e}")
            time.sleep(2 ** attempt)
    return None

def get_price(ticker: str) -> Optional[float]:
    url = f"https://api.polygon.io/v2/last/trade/{ticker}?apiKey={POLY_KEY}"
    j = api_get_with_retry(url)
    if j and "results" in j:
        return float(j["results"]["p"])
    return None

def get_news_title(ticker: str) -> Optional[str]:
    url = f"https://api.polygon.io/v2/reference/news?ticker={ticker}&limit=1&apiKey={POLY_KEY}"
    j = api_get_with_retry(url)
    if j and "results":
        return j["results"][0].get("title")
    return None

def scan_once(max_check: int = 500) -> List[Tuple[str, float]]:
    penny = []
    checked = 0
    cursor = None
    while checked < max_check:
        params = f"market=stocks&active=true&limit=1000&apiKey={POLY_KEY}"
        if cursor:
            params += f"&cursor={cursor}"
        url = f"https://api.polygon.io/v3/reference/tickers?{params}"
        j = api_get_with_retry(url)
        if not j or "results" not in j:
            break
        tickers = j["results"]
        for t in tickers:
            if checked >= max_check:
                break
            symbol = t.get("ticker")
            if not symbol:
                continue
            price = get_price(symbol)
            if price is not None and price < 1.0:
                penny.append((symbol, price))
            checked += 1
            time.sleep(0.15)  # Rate limit
        cursor = j.get("next_url")
        if not cursor:
            break
    return penny

def main_loop():
    logging.info("Worker started. Scanning for penny stocks with news every 20 minutes...")
    send_message("🤖 <b>Penny Stock Alert Bot</b> ишга тушди! Янгиликларни кузатаман...")  # Биринчи хабар
    while not STOP:
        try:
            penny = scan_once(max_check=500)
