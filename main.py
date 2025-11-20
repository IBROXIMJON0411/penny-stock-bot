import os
import time
import signal
import logging
import html
from typing import Optional, List, Tuple
import requests
import redis
from urllib.parse import urlparse, parse_qs
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Optional: load .env locally
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ---------- Configuration (read real env var names) ----------
CHAT_ID_ENV = os.environ.get("CHAT_ID")
CHAT_ID: Optional[str] = str(CHAT_ID_ENV) if CHAT_ID_ENV is not None else None
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")
REDIS_URL = os.environ.get("REDIS_URL")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")

ALERT_REPEAT_HOURS = int(os.environ.get("ALERT_REPEAT_HOURS", "6"))
MAX_CHECK = int(os.environ.get("MAX_CHECK", "500"))
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", str(20 * 60)))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "10"))
API_MAX_RETRIES = int(os.environ.get("API_MAX_RETRIES", "3"))
PAUSE_BETWEEN_TICKERS = float(os.environ.get("PAUSE_BETWEEN_TICKERS", "0.15"))

# ---------- Validate required env vars ----------
required = {
    "CHAT_ID": CHAT_ID,
    "POLYGON_API_KEY": POLYGON_API_KEY,
    "REDIS_URL": REDIS_URL,
    "TELEGRAM_TOKEN": TELEGRAM_TOKEN,
}
missing = [k for k, v in required.items() if not v]
if missing:
    raise SystemExit(f"Environment variables missing: {', '.join(missing)}")

# Try to normalize CHAT_ID to string (Telegram accepts string or int)
try:
    # keep as string, but ensure it's numeric or a channel handle like @channelname
    if CHAT_ID.lstrip('-').isdigit():
        CHAT_ID = str(int(CHAT_ID))
except Exception:
    pass

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------- Requests session with retries ----------
session = requests.Session()
retries = Retry(
    total=API_MAX_RETRIES,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
)
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.mount("http://", adapter)

# ---------- Redis connection with simple retry ----------
r: Optional[redis.Redis] = None
for attempt in range(3):
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        r.ping()
        logging.info("Connected to Redis.")
        break
    except Exception as e:
        logging.exception("Redis connection attempt %d failed.", attempt + 1)
        time.sleep(2 ** attempt)
if r is None:
    raise SystemExit("Redis connection failed after retries.")

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
    """
    Send a message to Telegram. Text must be already HTML-escaped.
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        resp = session.post(url, data=data, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            logging.warning("Telegram send failed: status=%s, resp=%s", resp.status_code, resp.text)
    except Exception:
        logging.exception("Telegram request failed.")


def api_get_with_retry(url: str, params: dict = None, max_retries: int = API_MAX_RETRIES) -> Optional[dict]:
    """
    Uses the session configured with retries. Returns parsed JSON or None.
    """
    try:
        resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            try:
                return resp.json()
            except Exception:
                logging.exception("Failed to decode JSON from %s", url)
                return None
        else:
            logging.warning("API error %s for %s", resp.status_code, url)
            return None
    except Exception:
        logging.exception("Request to %s failed.", url)
        return None


def get_price(ticker: str) -> Optional[float]:
    """
    Gets last trade price for a ticker from Polygon API v2.
    """
    url = f"https://api.polygon.io/v2/last/trade/{ticker}"
    params = {"apiKey": POLYGON_API_KEY}
    j = api_get_with_retry(url, params=params)
    if not j:
        return None
    # Polygon usually returns {"status":"OK","results":{"p": price, ...}}
    results = j.get("results") if isinstance(j, dict) else None
    if isinstance(results, dict):
        p = results.get("p") or results.get("price")
        try:
            return float(p) if p is not None else None
        except Exception:
            return None
    return None


def get_news_title(ticker: str) -> Optional[str]:
    """
    Fetch the latest news title or description for a ticker.
    """
    url = "https://api.polygon.io/v2/reference/news"
    params = {"ticker": ticker, "limit": 1, "apiKey": POLYGON_API_KEY}
    j = api_get_with_retry(url, params=params)
    if not j:
        return None
    results = j.get("results")
    if isinstance(results, list) and len(results) > 0:
        item = results[0]
        title = item.get("title") or item.get("description") or item.get("summary")
        if title:
            # Escape HTML to avoid breaking Telegram HTML parse_mode
            return html.escape(title)
    return None


def extract_cursor_from_next_url(next_url: str) -> Optional[str]:
    if not next_url:
        return None
    try:
        parsed = urlparse(next_url)
        qs = parse_qs(parsed.query)
        cur = qs.get("cursor")
        if cur:
            return cur[0]
    except Exception:
        logging.exception("Failed to parse next_url: %s", next_url)
    return None


def scan_once(max_check: int = MAX_CHECK) -> List[Tuple[str, float]]:
    """
    Scans tickers from Polygon reference and returns list of (symbol, price) where price < 1.0
    """
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
        tickers = j.get("results", [])
        if not isinstance(tickers, list):
            logging.warning("Unexpected tickers format.")
            break
        for t in tickers:
            if checked >= max_check or STOP:
                break
            symbol = t.get("ticker") or t.get("symbol")
            if not symbol:
                checked += 1
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

# ---------- Main loop ----------


def main_loop():
    logging.info("Penny Stock Alert Bot started.")
    send_telegram_message(html.escape("🤖 <b>Penny Stock Alert Bot</b> ishga tushdi — $1 dan arzon aksiyalar uchun kuzatib boradi."))
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
                    last_ts = None
                    try:
                        last_ts_raw = r.get(key)
                        if last_ts_raw and str(last_ts_raw).isdigit():
                            last_ts = int(last_ts_raw)
                    except Exception:
                        logging.exception("Failed to read last alert timestamp for %s", symbol)
                    now = int(time.time())
                    if last_ts and now - last_ts < allow_interval:
                        logging.debug("Skipping %s — alerted recently.", symbol)
                        continue
                    news = get_news_title(symbol)
                    if news:
                        # news already escaped in get_news_title
                        short_news = news if len(news) <= 300 else news[:297] + "..."
                        msg = (
                            f"🚨 <b>{html.escape(symbol)}</b> — yangilik chiqdi!\n"
                            f"💰 Narxi: <code>${price:.4f}</code>\n"
                            f"📰 {short_news}\n"
                            f"🔗 <a href='https://finance.yahoo.com/quote/{html.escape(symbol)}'>Batafsil</a>"
                        )
                        send_telegram_message(msg)
                        logging.info("Alert sent for %s", symbol)
                    else:
                        logging.debug("No recent news for %s (price %s).", symbol, price)
                    try:
                        r.set(key, str(now))
                    except Exception:
                        logging.exception("Failed to set last alert timestamp for %s", symbol)
                    time.sleep(1.0)
            else:
                logging.info("No penny stocks found this run.")
            # Poll interval with periodic STOP checks
            sleep_total = POLL_INTERVAL_SECONDS
            step = 5
            for _ in range(0, sleep_total, step):
                if STOP:
                    break
                time.sleep(step)
        except Exception:
            logging.exception("Main loop error.")
            # short backoff but responsive to STOP
            for _ in range(0, 60, 5):
                if STOP:
                    break
                time.sleep(5)
    logging.info("Worker shutting down gracefully.")
    send_telegram_message(html.escape("🤖 Penny Stock Alert Bot to'xtadi. Graceful shutdown amalga oshirildi."))


if _name_ == "_main_":
    main_loop()
