import os
import time
import signal
import logging
import html
import threading
from typing import Optional, List, Tuple
import requests
import redis
from urllib.parse import urlparse, parse_qs
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask, jsonify

# Optional: load .env locally
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ---------- Configuration (env names) ----------
PORT = int(os.environ.get("PORT", os.environ.get("PORT", 8080)))
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

# Rate-limit configuration (set API calls per minute)
API_CALLS_PER_MINUTE = int(os.environ.get("API_CALLS_PER_MINUTE", "20"))
ESTIMATED_CALLS_PER_TICKER = int(os.environ.get("ESTIMATED_CALLS_PER_TICKER", "2"))
# base PAUSE_BETWEEN_TICKERS (can be overridden by env but will be at least min_pause)
env_pause = float(os.environ.get("PAUSE_BETWEEN_TICKERS", "0.15"))
min_pause = (ESTIMATED_CALLS_PER_TICKER * 60.0) / max(1, API_CALLS_PER_MINUTE)
PAUSE_BETWEEN_TICKERS = max(env_pause, min_pause)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

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

# Normalize CHAT_ID
try:
    if CHAT_ID.lstrip('-').isdigit():
        CHAT_ID = str(int(CHAT_ID))
except Exception:
    pass

# ---------- Requests session with retries ----------
session = requests.Session()
# Respect Retry-After and retry on 429, but don't raise too early so we can log and backoff
retries = Retry(
    total=API_MAX_RETRIES,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    respect_retry_after_header=True,
    allowed_methods=["GET", "POST"],
)
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.mount("http://", adapter)

# ---------- Redis connection ----------
r: Optional[redis.Redis] = None
for attempt in range(3):
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        r.ping()
        logging.info("Connected to Redis.")
        break
    except Exception:
        logging.exception("Redis connection attempt %d failed.", attempt + 1)
        time.sleep(2 ** attempt)
if r is None:
    raise SystemExit("Redis connection failed after retries.")

# ---------- Graceful shutdown ----------
STOP = threading.Event()


def handle_sigterm(signum, frame):
    logging.info("Shutdown signal received, stopping...")
    STOP.set()


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
        resp = session.post(url, data=data, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            logging.warning("Telegram send failed: status=%s resp=%s", resp.status_code, resp.text)
    except Exception:
        logging.exception("Telegram request failed.")


def api_get_with_retry(url: str, params: dict = None) -> Optional[dict]:
    """
    Wrapper around session.get. Handles 403 and 429 with logging/backoff.
    Returns JSON dict or None.
    """
    try:
        resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    except requests.exceptions.RetryError as e:
        logging.warning("Too many retries for %s: %s", url, e)
        return None
    except Exception:
        logging.exception("Request to %s failed.", url)
        return None

    if resp.status_code == 200:
        try:
            return resp.json()
        except Exception:
            logging.exception("Failed to decode JSON from %s", url)
            return None
    elif resp.status_code == 429:
        # Rate limited — use Retry-After header if available
        ra = resp.headers.get("Retry-After")
        if ra and ra.isdigit():
            wait = int(ra)
        else:
            wait = 5
        logging.warning("Rate limited (429) for %s, sleeping %s seconds", url, wait)
        time.sleep(wait)
        return None
    elif resp.status_code == 403:
        logging.error("Access forbidden (403) from %s: %s", url, resp.text)
        # 403 likely means bad/disabled API key or permissions — don't retry here
        return None
    else:
        logging.warning("API error %s for %s: %s", resp.status_code, url, resp.text)
        return None


def get_price(ticker: str) -> Optional[float]:
    url = f"https://api.polygon.io/v2/last/trade/{ticker}"
    params = {"apiKey": POLYGON_API_KEY}
    j = api_get_with_retry(url, params=params)
    if not j:
        return None
    results = j.get("results") if isinstance(j, dict) else None
    if isinstance(results, dict):
        p = results.get("p") or results.get("price")
        try:
            return float(p) if p is not None else None
        except Exception:
            return None
    return None


def get_news_title(ticker: str) -> Optional[str]:
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
    penny: List[Tuple[str, float]] = []
    checked = 0
    cursor = None
    limit_per_page = 100
    while checked < max_check and not STOP.is_set():
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
            if checked >= max_check or STOP.is_set():
                break
            symbol = t.get("ticker") or t.get("symbol")
            if not symbol:
                checked += 1
                continue
            price = get_price(symbol)
            if price is not None and price < 1.0:
                penny.append((symbol, price))
            checked += 1
            # throttle to respect configured per-ticker pause
            for _ in range(int(max(1, PAUSE_BETWEEN_TICKERS))):
                if STOP.is_set():
                    break
                time.sleep(1)
            # if PAUSE_BETWEEN_TICKERS is fractional, sleep remainder
            frac = PAUSE_BETWEEN_TICKERS - int(PAUSE_BETWEEN_TICKERS)
            if frac > 0 and not STOP.is_set():
                time.sleep(frac)
        next_url = j.get("next_url") or j.get("next_href") or None
        cursor = extract_cursor_from_next_url(next_url) if next_url else None
        if not cursor:
            break
    return penny


def main_loop():
    logging.info("Penny Stock Alert Bot started (background worker).")
    send_telegram_message(html.escape("🤖 <b>Penny Stock Alert Bot</b> ishga tushdi — $1 dan arzon aksiyalar uchun kuzatib boradi."))
    allow_interval = ALERT_REPEAT_HOURS * 3600
    while not STOP.is_set():
        try:
            logging.info("Starting scan run...")
            penny = scan_once(max_check=MAX_CHECK)
            if penny:
                logging.info("Found %d penny stocks in this run.", len(penny))
                for symbol, price in penny:
                    if STOP.is_set():
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
                    # small pause to avoid hitting Telegram too fast
                    for _ in range(1):
                        if STOP.is_set():
                            break
                        time.sleep(1.0)
            else:
                logging.info("No penny stocks found this run.")
            # Poll interval with STOP checks
            sleep_total = POLL_INTERVAL_SECONDS
            step = 5
            for _ in range(0, sleep_total, step):
                if STOP.is_set():
                    break
                time.sleep(step)
        except Exception:
            logging.exception("Main loop error.")
            for _ in range(0, 60, 5):
                if STOP.is_set():
                    break
                time.sleep(5)
    logging.info("Worker shutting down gracefully.")
    send_telegram_message(html.escape("🤖 Penny Stock Alert Bot to'xtadi. Graceful shutdown amalga oshirildi."))

# ---------- Flask web server (health + control) ----------
app = Flask(_name_)
worker_thread: Optional[threading.Thread] = None


@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "worker_running": worker_thread.is_alive() if worker_thread else False,
                    "pause_between_tickers": PAUSE_BETWEEN_TICKERS,
                    "api_calls_per_minute": API_CALLS_PER_MINUTE}), 200


@app.route("/trigger-scan", methods=["POST"])
def trigger_scan():
    # quick on-demand scan (runs in background)
    def run_scan_once():
        try:
            p = scan_once(max_check=MAX_CHECK)
            logging.info("On-demand scan found %d penny stocks.", len(p))
        except Exception:
            logging.exception("On-demand scan error.")
    threading.Thread(target=run_scan_once, daemon=True).start()
    return jsonify({"status": "scan started"}), 202


def start_worker():
    global worker_thread
    worker_thread = threading.Thread(target=main_loop, daemon=True)
    worker_thread.start()


if __name__ == "__main__":
    # start background worker then run web server (Render expects a bound port)
    start_worker()
    logging.info("Starting web server on port %s", PORT)
    # In production use a production server; built-in is OK for simple deployments
    app.run(host="0.0.0.0", port=PORT)
