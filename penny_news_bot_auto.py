import os
import time
import json
import logging
import html
from typing import Optional, List, Tuple, Dict
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

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
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
REDIS_URL = os.getenv("REDIS_URL")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "120"))  # default 2 min
MAX_CHECK = int(os.getenv("MAX_CHECK", "200"))  # how many tickers to scan per run
API_CALLS_PER_MINUTE = int(os.getenv("API_CALLS_PER_MINUTE", "20"))  # throttle
ESTIMATED_CALLS_PER_TICKER = int(os.getenv("ESTIMATED_CALLS_PER_TICKER", "2"))
PAGE_SIZE_NEWS = int(os.getenv("PAGE_SIZE_NEWS", "5"))

# threshold for price (only consider tickers with price < PRICE_THRESHOLD)
PRICE_THRESHOLD = float(os.getenv("PRICE_THRESHOLD", "3.0"))

# minimal pause between tickers (seconds) to respect API limits
PAUSE_BETWEEN_TICKERS = max(0.2, (ESTIMATED_CALLS_PER_TICKER * 60.0) / max(1, API_CALLS_PER_MINUTE))

SEEN_FILE = os.getenv("SEEN_FILE", "seen_articles.json")

# Cache and cooldown settings
PRICE_CACHE_TTL = int(os.getenv("PRICE_CACHE_TTL", "60"))  # seconds
POLYGON_COOLDOWN_SECONDS = int(os.getenv("POLYGON_COOLDOWN_SECONDS", str(15 * 60)))  # 15 minutes
MAX_ARTICLES_PER_RUN = int(os.getenv("MAX_ARTICLES_PER_RUN", "35"))  # 15 per cycle

# Keywords filter
KEYWORDS = [
    "earnings", "fda", "approval", "trial",
    "merger", "acquisition", "contract",
    "partnership", "offering", "sec", "lawsuit"
]

# Required checks
if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID or not NEWSAPI_KEY or not POLYGON_API_KEY:
    raise SystemExit("ERROR: TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, NEWSAPI_KEY, POLYGON_API_KEY required in env")

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("penny_news_bot_auto")

# --- Requests session with retries ---
session = requests.Session()
retries = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    respect_retry_after_header=True,
    allowed_methods=frozenset(["GET", "POST", "HEAD"])
)
session.mount("https://", HTTPAdapter(max_retries=retries))
session.mount("http://", HTTPAdapter(max_retries=retries))
session.headers.update({"User-Agent": "penny-news-auto/1.0"})

# optional: import yfinance once (for fallback)
try:
    import yfinance as yf
    HAVE_YFINANCE = True
except Exception:
    HAVE_YFINANCE = False
    logger.info("yfinance not available. Install with: pip install yfinance to enable fallback price source.")

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
            tmp_key = "penny_seen_urls_tmp"
            if seen:
                rconn.delete(tmp_key)
                if seen:
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

# --- Helpers: URL normalization to reduce duplicate articles ---
REMOVE_QUERY_PREFIXES = ("utm_",)
REMOVE_QUERY_KEYS = {"fbclid", "gclid", "mc_cid", "mc_eid"}

def normalize_url(u: str) -> str:
    try:
        p = urlparse(u)
        qs = parse_qs(p.query, keep_blank_values=True)
        # filter query params
        new_qs = {}
        for k, vals in qs.items():
            if any(k.lower().startswith(pref) for pref in REMOVE_QUERY_PREFIXES):
                continue
            if k.lower() in REMOVE_QUERY_KEYS:
                continue
            new_qs[k] = vals
        # sort keys for deterministic order
        q_items = []
        for k in sorted(new_qs.keys()):
            for v in new_qs[k]:
                q_items.append((k, v))
        qstr = urlencode(q_items)
        newp = p._replace(query=qstr, fragment="")
        return urlunparse(newp)
    except Exception:
        return u

# --- Keywords filter ---
def is_relevant_article(article: dict) -> bool:
    text = " ".join([
        (article.get("title") or ""),
        (article.get("description") or ""),
        (article.get("content") or "")
    ]).lower()
    for kw in KEYWORDS:
        if kw.lower() in text:
            return True
    return False

# --- Price caching and Polygon cooldown handling ---
PRICE_CACHE: Dict[str, Tuple[float, float]] = {}  # symbol -> (price, ts)

def _cache_get(symbol: str) -> Optional[float]:
    rec = PRICE_CACHE.get(symbol.upper())
    if not rec:
        return None
    price, ts = rec
    if time.time() - ts > PRICE_CACHE_TTL:
        PRICE_CACHE.pop(symbol.upper(), None)
        return None
    return price

def _cache_set(symbol: str, price: float):
    PRICE_CACHE[symbol.upper()] = (price, time.time())

POLYGON_COOLDOWN: Dict[str, float] = {}  # symbol -> next_allowed_timestamp

# --- Polygon prev-close price with cooldown on 403 ---
def get_price_polygon_prev_close(ticker: str) -> Optional[float]:
    ticker_u = ticker.upper()
    now = time.time()
    next_allowed = POLYGON_COOLDOWN.get(ticker_u)
    if next_allowed and now < next_allowed:
        logger.debug("Polygon cooldown active for %s until %s", ticker_u, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(next_allowed)))
        return None

    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker_u}/prev"
    params = {"apiKey": POLYGON_API_KEY}
    try:
        r = session.get(url, params=params, timeout=10)
        if r.status_code == 403:
            POLYGON_COOLDOWN[ticker_u] = now + POLYGON_COOLDOWN_SECONDS
            logger.warning("Polygon NOT_AUTHORIZED (403) for %s; setting cooldown %s seconds", ticker_u, POLYGON_COOLDOWN_SECONDS)
            return None
        if r.status_code == 429:
            ra = r.headers.get("Retry-After")
            wait = int(ra) if ra and ra.isdigit() else 60
            logger.warning("Polygon rate limited; sleeping %s sec", wait)
            time.sleep(wait)
            return None
        r.raise_for_status()
        j = r.json()
        results = j.get("results")
        if not results or not isinstance(results, list):
            return None
        first = results[0]
        close = first.get("c") or first.get("close") or None
        if close is None:
            return None
        price = float(close)
        _cache_set(ticker_u, price)
        return price
    except Exception:
        logger.exception("Error fetching prev close price for %s from Polygon", ticker_u)
        POLYGON_COOLDOWN[ticker_u] = now + POLYGON_COOLDOWN_SECONDS
        return None

# --- yfinance fallback ---
def get_price_yfinance(ticker: str) -> Optional[float]:
    if not HAVE_YFINANCE:
        return None
    try:
        t = yf.Ticker(ticker)
        info_price = None
        try:
            info = t.info
            info_price = info.get("regularMarketPrice") or info.get("previousClose")
        except Exception:
            info_price = None
        if info_price:
            price = float(info_price)
            _cache_set(ticker, price)
            return price
        df = t.history(period="2d", interval="1d")
        if df is not None and not df.empty:
            price = float(df["Close"].iloc[-1])
            _cache_set(ticker, price)
            return price
    except Exception:
        logger.exception("yfinance failed for %s", ticker)
    return None

# --- unified get_price with caching & polygon-first then fallback ---
def get_price(ticker: str) -> Optional[float]:
    ticker_u = ticker.upper()
    cached = _cache_get(ticker_u)
    if cached is not None:
        return cached
    price = get_price_polygon_prev_close(ticker_u)
    if price is not None:
        return price
    price = get_price_yfinance(ticker_u)
    if price is not None:
        return price
    logger.info("Price unavailable for %s (Polygon unavailable or unauthorized; yfinance fallback failed)", ticker_u)
    return None

# --- helper to extract cursor ---
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

# --- scan tickers: uses get_price and PRICE_THRESHOLD ---
def scan_penny_tickers(max_check: int = MAX_CHECK) -> List[Tuple[str, str, float]]:
    penny: List[Tuple[str, str, float]] = []
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
            if r.status_code == 403:
                logger.error("Polygon returned 403 when listing tickers; cannot continue scanning (requires upgraded plan).")
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
            symbol = (t.get("ticker") or t.get("symbol") or "").strip()
            name = t.get("name") or ""
            if not symbol:
                checked += 1
                continue
            price = get_price(symbol)
            if price is not None and price < PRICE_THRESHOLD:
                penny.append((symbol, name, price))
            checked += 1
            time.sleep(PAUSE_BETWEEN_TICKERS)
        next_url = j.get("next_url") or j.get("next_href") or None
        cursor = extract_cursor(next_url)
        if not cursor:
            break
    logger.info("scan_penny_tickers found %d penny tickers (threshold %.2f)", len(penny), PRICE_THRESHOLD)
    return penny

# --- NewsAPI functions ---
def build_news_query(symbol: str, name: str) -> str:
    parts = [f'"{symbol}"']
    if name:
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

# --- Aggregate articles across tickers then send deduplicated alerts ---
def main():
    logger.info("penny_news_bot_auto starting")
    seen = load_seen()
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
                # collect articles across all tickers to dedupe and aggregate tickers per article
                articles_map: Dict[str, Dict] = {}  # normalized_url -> {"article": article, "tickers": set(), "min_price": float}
                for symbol, name, price in penny_list:
                    logger.debug("Fetching news for %s (price=%.4f)", symbol, price)
                    arts = fetch_news_for_ticker(symbol, name)
                    for a in arts:
                        url = a.get("url")
                        if not url:
                            continue
                        norm = normalize_url(url)
                        if norm in seen:
                            continue
                        # filter by keywords
                        if not is_relevant_article(a):
                            continue
                        entry = articles_map.get(norm)
                        if entry is None:
                            entry = {"article": a, "tickers": set(), "min_price": price}
                            articles_map[norm] = entry
                        entry["tickers"].add(symbol)
                        if price is not None:
                            if entry["min_price"] is None or price < entry["min_price"]:
                                entry["min_price"] = price
                    time.sleep(PAUSE_BETWEEN_TICKERS)

                if not articles_map:
                    logger.info("No relevant articles found for penny tickers this run.")
                else:
                    # Sort articles by publishedAt descending (newest first)
                    def art_pub_key(item):
                        a = item[1]["article"]
                        p = a.get("publishedAt") or a.get("published") or ""
                        return p or ""
                    sorted_items = sorted(articles_map.items(), key=art_pub_key, reverse=True)
                    sent_count = 0
                    for norm_url, info in sorted_items:
                        if sent_count >= MAX_ARTICLES_PER_RUN:
                            break
                        article = info["article"]
                        tickers = sorted(info["tickers"])
                        min_price = info["min_price"]
                        # format combined message
                        tickers_str = ", ".join(html.escape(t) for t in tickers)
                        title = html.escape(article.get("title") or "No title")
                        src = html.escape((article.get("source") or {}).get("name") or "")
                        published = article.get("publishedAt") or article.get("published") or ""
                        link = html.escape(article.get("url") or norm_url)
                        msg = (
                            f"🚨 <b>{tickers_str}</b>  <code>${(min_price or 0):.4f}</code>\n"
                            f"📰 <b>{title}</b>\nSource: {src}\nPublished: {published}\n{link}"
                        )
                        try:
                            send_telegram(msg)
                            sent_count += 1
                            # mark as seen (use normalized URL)
                            seen.add(norm_url)
                        except Exception:
                            logger.exception("Failed to send Telegram for article %s", norm_url)
                    if sent_count:
                        save_seen(seen)
                        logger.info("Sent %d new article alerts this run", sent_count)
        except Exception:
            logger.exception("Unexpected error in main loop")

        # sleep POLL_INTERVAL with small steps
        total = POLL_INTERVAL
        step = 5
        for _ in range(0, total, step):
            time.sleep(step)

if __name__ == "__main__":
    main()
