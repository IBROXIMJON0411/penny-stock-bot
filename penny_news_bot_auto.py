import os
import time
import json
import logging
import html
from typing import Optional, List, Tuple
from urllib.parse import urlparse, parse_qs

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Optional dotenv
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# --- Config from env ---
TELEGRAM_CHAT_ID = os.getenv("CHAT_ID")
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
REDIS_URL = os.getenv("REDIS_URL")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))  # default 5 min
MAX_CHECK = int(os.getenv("MAX_CHECK", "200"))  # how many tickers to scan per run
API_CALLS_PER_MINUTE = int(os.getenv("API_CALLS_PER_MINUTE", "20"))  # throttle
ESTIMATED_CALLS_PER_TICKER = int(os.getenv("ESTIMATED_CALLS_PER_TICKER", "2"))
PAGE_SIZE_NEWS = int(os.getenv("PAGE_SIZE_NEWS", "5"))

# minimal pause between tickers (seconds) to respect API limits
PAUSE_BETWEEN_TICKERS = max(0.2, (ESTIMATED_CALLS_PER_TICKER * 60.0) / max(1, API_CALLS_PER_MINUTE))

SEEN_FILE = os.getenv("SEEN_FILE", "seen_articles.json")

# Required checks
if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID or not NEWSAPI_KEY or not POLYGON_API_KEY:
    raise SystemExit("ERROR: TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, NEWSAPI_KEY, POLYGON_API_KEY required in env")

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("penny_news_bot_auto")

# --- Requests session with retries ---
session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504],
                respect_retry_after_header=True, allowed_methods=frozenset(["GET", "POST"]))
session.mount("https://", HTTPAdapter(max_retries=retries))
session.headers.update({"User-Agent": "penny-news-auto/1.0"})

# --- Redis optional ---
use_redis = False
rconn = None
if REDIS_URL:
    try:
        import redis as _redis
        rconn = _redis.from_url(REDIS_URL, decode_responses=True)
        rconn.ping()
        use_redis = True
        logger.info("Connected to Redis for persistence.")
    except Exception:
        logger.exception("Redis connection failed; falling back to file persistence.")
        rconn = None
        use_redis = False

# --- Seen storage (Redis set or local file) ---
def load_seen() -> set:
    if use_redis:
        try:
            return set(rconn.smembers("penny_seen_urls") or [])
        except Exception:
            logger.exception("Failed to read seen set from Redis")
            return set()
    else:
        try:
            with open(SEEN_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return set(data)
        except FileNotFoundError:
            return set()
        except Exception:
            logger.exception("Failed to load seen file")
            return set()

def save_seen(seen: set):
    if use_redis:
        try:
            # replace set atomically
            tmp_key = "penny_seen_urls_tmp"
            if seen:
                rconn.delete(tmp_key)
                rconn.sadd(tmp_key, *list(seen))
                rconn.rename(tmp_key, "penny_seen_urls")
            else:
                rconn.delete("penny_seen_urls")
        except Exception:
            logger.exception("Failed to save seen set to Redis")
    else:
        tmp = SEEN_FILE + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(list(seen), f)
            os.replace(tmp, SEEN_FILE)
        except Exception:
            logger.exception("Failed to save seen file")

# --- Telegram send (HTML safe) ---
def send_telegram(msg_text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg_text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    try:
        resp = session.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        logger.info("Telegram message sent")
    except Exception:
        logger.exception("Failed to send Telegram message")

# --- Polygon helpers: list tickers and get last price ---
def extract_cursor(next_url: Optional[str]) -> Optional[str]:
    if not next_url:
        return None
    try:
        parsed = urlparse(next_url)
        qs = parse_qs(parsed.query)
        cur = qs.get("cursor")
        if cur:
            return cur[0]
    except Exception:
        logger.exception("Failed to parse cursor from next_url")
    return None

def get_price_polygon(ticker: str) -> Optional[float]:
    url = f"https://api.polygon.io/v2/last/trade/{ticker}"
    params = {"apiKey": POLYGON_API_KEY}
    try:
        r = session.get(url, params=params, timeout=10)
        if r.status_code == 403:
            logger.error("Polygon returned 403 for price %s: %s", ticker, r.text)
            return None
        if r.status_code == 429:
            ra = r.headers.get("Retry-After")
            wait = int(ra) if ra and ra.isdigit() else 60
            logger.warning("Polygon rate limited; sleeping %s sec", wait)
            time.sleep(wait)
            return None
        r.raise_for_status()
        j = r.json()
        results = j.get("results") or {}
        p = results.get("p") or results.get("price")
        if p is None:
            return None
        return float(p)
    except Exception:
        logger.exception("Error fetching price for %s", ticker)
        return None

def scan_penny_tickers(max_check: int = MAX_CHECK) -> List[Tuple[str, str, float]]:
    """
    Returns list of (symbol, name, price) where price < 1.0
    """
    penny = []
    checked = 0
    cursor = None
    limit_per_page = 100
    while checked < max_check:
        params = {"market": "stocks", "active": "true", "limit": limit_per_page, "apiKey": POLYGON_API_KEY}
        if cursor:
            params["cursor"] = cursor
        url = "https://api.polygon.io/v3/reference/tickers"
        try:
            r = session.get(url, params=params, timeout=15)
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                wait = int(ra) if ra and ra.isdigit() else 60
                logger.warning("Polygon tickers rate limited; sleeping %s sec", wait)
                time.sleep(wait)
                break
            r.raise_for_status()
            j = r.json()
        except Exception:
            logger.exception("Failed to fetch tickers page")
            break
        results = j.get("results") or []
        if not isinstance(results, list) or not results:
            break
        for t in results:
            if checked >= max_check:
                break
            symbol = t.get("ticker") or t.get("symbol")
            name = t.get("name") or ""
            if not symbol:
                checked += 1
                continue
            price = get_price_polygon(symbol)
            if price is not None and price < 1.0:
                penny.append((symbol, name, price))
            checked += 1
            time.sleep(PAUSE_BETWEEN_TICKERS)
        next_url = j.get("next_url") or j.get("next_href") or None
        cursor = extract_cursor(next_url)
        if not cursor:
            break
    logger.info("scan_penny_tickers found %d penny tickers", len(penny))
    return penny

# --- NewsAPI fetch for a ticker (search by ticker and company name) ---
def build_news_query(symbol: str, name: str) -> str:
    parts = [f'"{symbol}"']
    if name:
        # include short company name words but avoid too-long queries
        parts.append(f'"{name}"')
    return " OR ".join(parts)

def fetch_news_for_ticker(symbol: str, name: str, page_size: int = PAGE_SIZE_NEWS) -> List[dict]:
    q = build_news_query(symbol, name)
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": q,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": page_size,
        "apiKey": NEWSAPI_KEY,
    }
    try:
        r = session.get(url, params=params, timeout=12)
        if r.status_code == 429:
            ra = r.headers.get("Retry-After")
            wait = int(ra) if ra and ra.isdigit() else 60
            logger.warning("NewsAPI rate limited; sleeping %s sec", wait)
            time.sleep(wait)
            return []
        r.raise_for_status()
        data = r.json()
        return data.get("articles", []) or []
    except Exception:
        logger.exception("NewsAPI fetch failed for %s", symbol)
        return []

# --- Format and send alerts ---
def format_alert(article: dict, symbol: str, price: Optional[float]) -> str:
    title = html.escape(article.get("title") or "No title")
    src = html.escape((article.get("source") or {}).get("name") or "")
    url = html.escape(article.get("url") or "")
    published = article.get("publishedAt") or ""
    msg = (
        f"🚨 <b>{html.escape(symbol)}</b>  <code>${price:.4f}</code>\n"
        f"📰 <b>{title}</b>\nSource: {src}\nPublished: {published}\n{url}"
    )
    return msg

# --- Main loop ---
def main():
    logger.info("penny_news_bot_auto starting")
    seen = load_seen()
    # Optional startup message
    try:
        send_telegram(f"🚀 penny_news_bot_auto started; polling every {POLL_INTERVAL}s")
    except Exception:
        pass

    while True:
        try:
            penny_list = scan_penny_tickers(MAX_CHECK)
            if not penny_list:
                logger.info("No penny tickers found this run.")
            else:
                for sym, name, price in penny_list:
                    # For each penny ticker, fetch recent news
                    logger.info("Checking news for %s (price=%.4f) name=%s", sym, price, name)
                    arts = fetch_news_for_ticker(sym, name)
                    if not arts:
                        logger.debug("No articles for %s", sym)
                    for a in arts:
                        url = a.get("url")
                        if not url:
                            continue
                        if url in seen:
                            continue
                        # send alert
                        msg = format_alert(a, sym, price)
                        send_telegram(msg)
                        seen.add(url)
                    # mark saved intermittently to avoid data loss
                    save_seen(seen)
                    time.sleep(PAUSE_BETWEEN_TICKERS)
        except Exception:
            logger.exception("Unexpected error in main loop")

        # after run sleep POLL_INTERVAL with interruption every few seconds
        total = POLL_INTERVAL
        step = 5
        for _ in range(0, total, step):
            time.sleep(step)

if _name_ == "_main_":
    main()
