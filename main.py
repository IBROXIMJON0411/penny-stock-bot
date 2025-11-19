import os
import time
import signal
import logging
from typing import Optional, List, Tuple
import requests
import redis
from urllib.parse import urlparse, parse_qs

# ---------- .env faylini yuklash (faqat lokalda kerak) ----------
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # Render yoki boshqa muhitda dotenv kerak emas

# ---------- Konfiguratsiya ----------
CHAT_ID = os.environ.get("7288340454")
POLYGON_API_KEY = os.environ.get("9IMXlZEBOvSD7LQTTsNd_pRZ0T5sFCDm")
REDIS_URL = os.environ.get("redis://red-d4bqb56r433s73d3l9gg:6379")
TELEGRAM_TOKEN = os.environ.get("7762047492:AAFartWb8w-nmi8Cqbl4rdBYmjW1yk8xDRY")

ALERT_REPEAT_HOURS = int(os.environ.get("ALERT_REPEAT_HOURS", "6"))
MAX_CHECK = int(os.environ.get("MAX_CHECK", "500"))
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", str(20 * 60)))
REQUEST_TIMEOUT = 10
API_MAX_RETRIES = 3
PAUSE_BETWEEN_TICKERS = float(os.environ.get("PAUSE_BETWEEN_TICKERS", "0.15"))

# ---------- Tekshirish ----------
required = {
    "TELEGRAM_TOKEN": TELEGRAM_TOKEN,
    "CHAT_ID": CHAT_ID,
    "POLYGON_API_KEY": POLYGON_API_KEY,
    "REDIS_URL": REDIS_URL,
}
missing = [k for k, v in required.items() if not v]
if missing:
    raise SystemExit(f"Environment variables missing: {', '.join(missing)}")

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------- Redis connection ----------
try:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
    logging.info("Connected to Redis.")
except Exception as e:
    logging.exception("Failed to connect Redis: %s", e)
    raise SystemExit("Redis connection failed.")

# ---------- Graceful shutdown ----------
STOP = False
def handle_sigterm(signum, frame):
    global STOP
    STOP = True
    logging.info("Shutdown signal received, stopping...")

signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)

# ---------- Helper functions ----------
def send_telegram_message(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        resp = requests.post(url, data=data, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            logging.warning("Telegram send failed: %s", resp.text)
    except Exception as e:
        logging.exception("Telegram request failed: %s", e)

def api_get_with_retry(url: str, params: dict = None, max_retries: int = API_MAX_RETRIES) -> Optional[dict]:
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.json()
            else:
                logging.warning("API error %s for %s (attempt %d)", resp.status_code, url, attempt + 1)
        except Exception as e:
            logging.debug("Request error %s (attempt %d): %s", url, attempt + 1, e)
        time.sleep(2 ** attempt)
    return None

def get_price(ticker: str) -> Optional[float]:
    url = f"https://api.polygon.io/v2/last/trade/{ticker}"
    params = {"apiKey": POLYGON_API_KEY}
    j = api_get_with_retry(url, params=params)
    if j and "results" in j and "p" in j["results"]:
        try:
            return float(j["results"]["p"])
        except Exception:
            return None
    return None

def get_news_title(ticker: str) -> Optional[str]:
    url = "https://api.polygon.io/v2/reference/news"
    params = {"ticker": ticker, "limit": 1, "apiKey": POLYGON_API_KEY}
    j = api_get_with_retry(url, params=params)
    if j and "results" in j and len(j["results"]) > 0:
        return j["results"][0].get("title") or j["results"][0].get("description")
    return None

def extract_cursor_from_next_url(next_url: str) -> Optional[str]:
    if not next_url:
        return None
    parsed = urlparse(next_url)
    qs = parse_qs(parsed.query)
    cur = qs.get("cursor")
    if cur:
        return cur[0]
    return None

def scan_once(max_check: int = MAX_CHECK) -> List[Tuple[str, float]]:
    penny: List[Tuple[str, float]] = []
    checked = 0
    cursor = None
    limit_per_page = 100
    while checked < max_check and not STOP:
        params = {
            "market": "stocks",
            "active": "true",
            "limit": limit_per_page,
            "apiKey": POLYGON_API_KEY
        }
        if cursor:
            params["cursor"] = cursor
        url = "https://api.polygon.io/v3/reference/tickers"
        j = api_get_with_retry(url, params=params)
        if not j or "results" not in j:
            logging.info("No tickers returned or API error.")
            break
        tickers = j["results"]
        for t in tickers:
            if checked >= max_check or STOP:
                break
            symbol = t.get("ticker")
            if not symbol:
                continue
            price = get_price(symbol)
            if price is not None and price < 1.0:
                penny.append((symbol, price))
            checked += 1
            time.sleep(PAUSE_BETWEEN_TICKERS)
        next_url = j.get("next_url") or j.get("next_href") or None
        cursor = extract_cursor_from_next_url(next_url) if next_url else None
        if not cursor:
            break
    return penny

# ---------- Asosiy sikl ----------
def main_loop():
    logging.info("Penny Stock Alert Bot started.")
    send_telegram_message("🤖 <b>Penny Stock Alert Bot</b> ishga tushdi — $1 dan arzon aksiyalar uchun kuzatib boradi.")
    allow_interval = ALERT_REPEAT_HOURS * 3600
    while not STOP:
        try:
            logging.info("Starting scan run...")
            penny = scan_once(max_check=MAX_CHECK)
            if penny:
                logging.info("Found %d penny stocks in this run.", len(penny))
                for symbol, price in penny:
                    if STOP:
                        break
                    key = f"last_alert:{symbol}"
                    last_ts = r.get(key)
                    now = int(time.time())
                    if last_ts and now - int(last_ts) < allow_interval:
                        logging.debug("Skipping %s — alerted recently.", symbol)
                        continue
                    news = get_news_title(symbol)
                    if news:
                        short_news = news if len(news) <= 300 else news[:297] + "..."
                        msg = f"""🚨 <b>{symbol}</b> — yangilik chiqdi!
💰 Narxi: <code>${price:.4f}</code>
📰 {short_news}
🔗 <a href='https://finance.yahoo.com/quote/{symbol}'>Batafsil</a>"""
                        send_telegram_message(msg)
                        logging.info("Alert sent for %s", symbol)
                    else:
                        logging.debug("No recent news for %s (price %s).", symbol, price)
                    r.set(key, str(now))
                    time.sleep(1.0)
            else:
                logging.info("No penny stocks found this run.")
            # Poll interval
            sleep_total = POLL_INTERVAL_SECONDS
            step = 5
            for _ in range(0, sleep_total, step):
                if STOP:
                    break
                time.sleep(step)
        except Exception as e:
            logging.exception("Main loop error: %s", e)
            for _ in range(0, 60, 5):
                if STOP:
                    break
                time.sleep(5)
    logging.info("Worker shutting down gracefully.")
    send_telegram_message("🤖 Penny Stock Alert Bot to'xtadi. Graceful shutdown amalga oshirildi.")

