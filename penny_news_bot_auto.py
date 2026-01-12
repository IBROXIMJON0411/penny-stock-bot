eimport os
import time
import json
import logging
import html
from typing import Optional, List, Tuple, Dict
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from datetime import datetime, timezone
import email.utils

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Optional dotenv
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# --- Config (env) ---
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
REDIS_URL = os.getenv("REDIS_URL")

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))            # seconds between runs
MAX_CHECK = int(os.getenv("MAX_CHECK", "200"))                   # how many tickers to scan per run
PAGE_SIZE_NEWS = int(os.getenv("PAGE_SIZE_NEWS", "10"))          # per-ticker news fetch size
PRICE_THRESHOLD = float(os.getenv("PRICE_THRESHOLD", "3.0"))     # max price to consider "penny"
MAX_ARTICLES_PER_RUN = int(os.getenv("MAX_ARTICLES_PER_RUN", "15"))  # max messages per cycle
SEEN_FILE = os.getenv("SEEN_FILE", "seen_articles.json")
SEEN_TTL = int(os.getenv("SEEN_TTL", str(24 * 3600)))            # seconds to keep seen articles (default 24h)

API_CALLS_PER_MINUTE = int(os.getenv("API_CALLS_PER_MINUTE", "20"))
ESTIMATED_CALLS_PER_TICKER = int(os.getenv("ESTIMATED_CALLS_PER_TICKER", "2"))
PAUSE_BETWEEN_TICKERS = max(0.2, (ESTIMATED_CALLS_PER_TICKER * 60.0) / max(1, API_CALLS_PER_MINUTE))

PRICE_CACHE_TTL = int(os.getenv("PRICE_CACHE_TTL", "60"))        # seconds
POLYGON_COOLDOWN_SECONDS = int(os.getenv("POLYGON_COOLDOWN_SECONDS", str(15 * 60)))  # 15 min

# Expanded important keywords to surface (used in queries and filtering)
KEYWORDS = [
    "earnings", "revenue", "guidance",
    "fda", "approval", "trial", "phase",
    "sec", "investigation", "lawsuit",
    "merger", "acquisition", "buyout",
    "offering", "public offering", "registered direct",
    "contract", "partnership", "agreement",
    "halt", "trading halt",
    "reverse split", "compliance",
    "nasdaq", "nyse", "delisting"
]

# required checks
if not POLYGON_API_KEY or not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    raise SystemExit("ERROR: POLYGON_API_KEY, TELEGRAM_TOKEN and TELEGRAM_CHAT_ID must be set in env")

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("penny_news_bot_polygon")

# --- HTTP session with retries ---
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    respect_retry_after_header=True,
    allowed_methods=frozenset(["GET", "POST", "HEAD"])
)
session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
session.mount("http://", HTTPAdapter(max_retries=retry_strategy))
session.headers.update({"User-Agent": "penny-news-polygon/1.0"})

# optional yfinance fallback
try:
    import yfinance as yf
    HAVE_YFINANCE = True
except Exception:
    HAVE_YFINANCE = False
    logger.info("yfinance not installed — install with: pip install yfinance to enable fallback")

# optional Redis for seen storage
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
        use_redis = False
        rconn = None

# --- Utilities: URL normalize and date parse ---
REMOVE_QUERY_PREFIXES = ("utm_",)
REMOVE_QUERY_KEYS = {"fbclid", "gclid", "mc_cid", "mc_eid"}


def normalize_url(u: str) -> str:
    try:
        p = urlparse(u)
        qs = parse_qs(p.query, keep_blank_values=True)
        new_qs = {}
        for k, vals in qs.items():
            if any(k.lower().startswith(pref) for pref in REMOVE_QUERY_PREFIXES):
                continue
            if k.lower() in REMOVE_QUERY_KEYS:
                continue
            new_qs[k] = vals
        q_items = []
        for k in sorted(new_qs.keys()):
            for v in new_qs[k]:
                q_items.append((k, v))
        qstr = urlencode(q_items)
        newp = p._replace(query=qstr, fragment="")
        return urlunparse(newp)
    except Exception:
        return u


def try_parse_date(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    s = str(s).strip()
    # try ISO forms
    try:
        if s.endswith("Z"):
            s2 = s.replace("Z", "+00:00")
            return datetime.fromisoformat(s2)
        return datetime.fromisoformat(s)
    except Exception:
        pass
    # try email.utils
    try:
        dt = email.utils.parsedate_to_datetime(s)
        return dt
    except Exception:
        pass
    return None


# --- Seen persistence (url -> timestamp) with pruning ---
def load_seen() -> Dict[str, float]:
    now = time.time()
    seen: Dict[str, float] = {}
    if use_redis and rconn:
        try:
            items = rconn.hgetall("penny_seen_map") or {}
            for k, v in items.items():
                try:
                    ts = float(v)
                    if now - ts <= SEEN_TTL:
                        seen[k] = ts
                except Exception:
                    continue
            return seen
        except Exception:
            logger.exception("Failed to load seen from redis")
            return {}
    else:
        try:
            with open(SEEN_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    for k, v in data.items():
                        try:
                            ts = float(v)
                            if now - ts <= SEEN_TTL:
                                seen[k] = ts
                        except Exception:
                            continue
            return seen
        except FileNotFoundError:
            return {}
        except Exception:
            logger.exception("Failed to load seen file")
            return {}


def save_seen(seen: Dict[str, float]):
    if use_redis and rconn:
        try:
            tmp = "penny_seen_map_tmp"
            if seen:
                rconn.delete(tmp)
                mapping = {k: str(v) for k, v in seen.items()}
                rconn.hset(tmp, mapping=mapping)
                rconn.rename(tmp, "penny_seen_map")
            else:
                rconn.delete("penny_seen_map")
        except Exception:
            logger.exception("Failed to save seen to redis")
    else:
        tmp = SEEN_FILE + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(seen, f)
            os.replace(tmp, SEEN_FILE)
        except Exception:
            logger.exception("Failed to save seen file")


def mark_seen(seen: Dict[str, float], url: str):
    seen[url] = time.time()


# --- Telegram send ---
def send_telegram(text: str) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    try:
        r = session.post(url, json=payload, timeout=10)
        r.raise_for_status()
        logger.info("Telegram message sent")
        return True
    except Exception:
        logger.exception("Failed to send Telegram message")
        return False


# --- Price helpers (Polygon prev-close and yfinance fallback) ---
PRICE_CACHE: Dict[str, Tuple[float, float]] = {}
POLYGON_COOLDOWN: Dict[str, float] = {}


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


def get_price_polygon_prev_close(ticker: str) -> Optional[float]:
    ticker_u = ticker.upper()
    now = time.time()
    next_allowed = POLYGON_COOLDOWN.get(ticker_u)
    if next_allowed and now < next_allowed:
        return None
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker_u}/prev"
    params = {"apiKey": POLYGON_API_KEY}
    try:
        r = session.get(url, params=params, timeout=10)
        if r.status_code == 403:
            POLYGON_COOLDOWN[ticker_u] = now + POLYGON_COOLDOWN_SECONDS
            logger.warning("Polygon returned 403 for %s; setting cooldown", ticker_u)
            return None
        if r.status_code == 429:
            ra = r.headers.get("Retry-After")
            wait = int(ra) if ra and ra.isdigit() else 60
            logger.warning("Polygon rate-limited; sleeping %s", wait)
            time.sleep(wait)
            return None
        r.raise_for_status()
        j = r.json()
        results = j.get("results")
        if not results:
            return None
        first = results[0]
        close = first.get("c") or first.get("close") or None
        if close is None:
            return None
        p = float(close)
        _cache_set(ticker_u, p)
        return p
    except Exception:
        logger.exception("Error fetching Polygon price for %s", ticker_u)
        POLYGON_COOLDOWN[ticker_u] = now + POLYGON_COOLDOWN_SECONDS
        return None


def get_price_yfinance(ticker: str) -> Optional[float]:
    if not HAVE_YFINANCE:
        return None
    try:
        t = yf.Ticker(ticker)
        try:
            info = t.info
            val = info.get("regularMarketPrice") or info.get("previousClose")
            if val is not None:
                _cache_set(ticker, float(val))
                return float(val)
        except Exception:
            pass
        df = t.history(period="2d", interval="1d")
        if df is not None and not df.empty:
            price = float(df["Close"].iloc[-1])
            _cache_set(ticker, price)
            return price
    except Exception:
        logger.exception("yfinance failed for %s", ticker)
    return None


def get_price(ticker: str) -> Optional[float]:
    t = ticker.upper()
    cached = _cache_get(t)
    if cached is not None:
        return cached
    price = get_price_polygon_prev_close(t)
    if price is not None:
        return price
    price = get_price_yfinance(t)
    if price is not None:
        return price
    return None


# --- Tickers scan (Polygon reference/tickers) ---
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
        logger.exception("Failed to parse cursor")
    return None


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
                logger.warning("Polygon tickers rate-limited; sleeping %s", wait)
                time.sleep(wait)
                break
            if r.status_code == 403:
                logger.error("Polygon returned 403 when listing tickers; cannot continue scanning.")
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


# --- Polygon News fetch ---
def polygon_news_query_for_symbol(symbol: str) -> str:
    kws = " OR ".join(KEYWORDS)
    return f'"{symbol}" AND ({kws})'


def fetch_news_polygon_for_symbol(symbol: str, page_size: int = PAGE_SIZE_NEWS) -> List[dict]:
    q = polygon_news_query_for_symbol(symbol)
    url = "https://api.polygon.io/v2/reference/news"
    params = {
        "query": q,
        "limit": page_size,
        "apiKey": POLYGON_API_KEY
    }
    try:
        r = session.get(url, params=params, timeout=12)
        if r.status_code == 429:
            ra = r.headers.get("Retry-After")
            wait = int(ra) if ra and ra.isdigit() else 60
            logger.warning("Polygon news rate-limited; sleeping %s", wait)
            time.sleep(wait)
            return []
        if r.status_code == 403:
            logger.error("Polygon news returned 403 (not authorized) for symbol %s", symbol)
            return []
        r.raise_for_status()
        j = r.json()
        results = j.get("results") or []
        return results if isinstance(results, list) else []
    except Exception:
        logger.exception("Failed to fetch Polygon news for %s", symbol)
        return []


# --- New is_relevant_article implementation (uses title, summary, description) ---
def is_relevant_article(article: dict) -> bool:
    text = (
        (article.get("title") or "") + " " +
        (article.get("summary") or "") + " " +
        (article.get("description") or "")
    ).lower()

    for kw in KEYWORDS:
        if kw.lower() in text:
            return True
    return False


# --- Format alert message ---
def format_alert_for_article(article: dict, tickers: List[str], min_price: Optional[float]) -> str:
    url = article.get("article_url") or article.get("url") or article.get("canonical_url") or article.get("permalink") or ""
    title = article.get("title") or article.get("headline") or ""
    summary = article.get("summary") or article.get("description") or ""
    source = ""
    publisher = article.get("publisher") or article.get("source") or {}
    if isinstance(publisher, dict):
        source = publisher.get("name") or ""
    elif isinstance(publisher, str):
        source = publisher
    published_raw = article.get("published_utc") or article.get("published_at") or article.get("published") or article.get("created_utc") or ""
    published_str = ""
    dt = try_parse_date(published_raw)
    if dt:
        try:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            published_str = dt.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
        except Exception:
            published_str = published_raw
    else:
        published_str = published_raw or ""
    tickers_str = ", ".join(html.escape(t) for t in tickers)
    title_esc = html.escape(title or (summary[:120] if summary else "No title"))
    url_esc = html.escape(url)
    price_text = f"${(min_price or 0):.4f}" if min_price is not None else "N/A"
    msg = (
        f"🚨 <b>{tickers_str}</b>  <code>{price_text}</code>\n"
        f"📰 <b>{title_esc}</b>\n"
        f"Source: {html.escape(source)}\n"
        f"Published: {html.escape(published_str)}\n"
        f"{url_esc}"
    )
    return msg


# --- main loop ---
def main():
    logger.info("penny_news_bot_polygon starting")
    seen = load_seen()
    try:
        send_telegram("🚀 penny_news_bot_polygon started")
    except Exception:
        pass

    while True:
        try:
            penny_list = scan_penny_tickers(MAX_CHECK)
            if not penny_list:
                logger.info("No penny tickers found this run.")
            else:
                articles_map: Dict[str, Dict] = {}
                for symbol, name, price in penny_list:
                    logger.debug("Fetch news for %s (price=%.4f)", symbol, price)
                    results = fetch_news_polygon_for_symbol(symbol, PAGE_SIZE_NEWS)
                    for art in results:
                        url = art.get("article_url") or art.get("url") or art.get("canonical_url") or art.get("permalink")
                        if not url:
                            continue
                        norm = normalize_url(url)
                        ts = seen.get(norm)
                        if ts and (time.time() - ts) <= SEEN_TTL:
                            continue
                        pub = art.get("published_utc") or art.get("published_at") or art.get("published") or ""
                        dt = try_parse_date(pub)
                        if dt:
                            if (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() > SEEN_TTL:
                                continue
                        if not is_relevant_article(art):
                            title = (art.get("title") or "").lower()
                            if symbol.lower() not in title:
                                continue
                        entry = articles_map.get(norm)
                        if entry is None:
                            entry = {"article": art, "tickers": set(), "min_price": price}
                            articles_map[norm] = entry
                        entry["tickers"].add(symbol)
                        if price is not None:
                            if entry["min_price"] is None or price < entry["min_price"]:
                                entry["min_price"] = price
                    time.sleep(PAUSE_BETWEEN_TICKERS)

                if not articles_map:
                    logger.info("No relevant Polygon news found this run.")
                else:
                    def pub_key(item):
                        art = item[1]["article"]
                        pub = art.get("published_utc") or art.get("published_at") or art.get("published") or ""
                        dt = try_parse_date(pub)
                        if dt:
                            return dt
                        return datetime.fromtimestamp(0, tz=timezone.utc)
                    sorted_items = sorted(articles_map.items(), key=pub_key, reverse=True)
                    sent = 0
                    for norm, info in sorted_items:
                        if sent >= MAX_ARTICLES_PER_RUN:
                            break
                        art = info["article"]
                        tickers = sorted(info["tickers"])
                        min_price = info.get("min_price")
                        msg = format_alert_for_article(art, tickers, min_price)
                        ok = send_telegram(msg)
                        if ok:
                            sent += 1
                            mark_seen(seen, norm)
                    if sent:
                        save_seen(seen)
                        logger.info("Sent %d alerts this run", sent)

        except Exception:
            logger.exception("Unexpected error in main loop")

        total = POLL_INTERVAL
        step = 5
        for _ in range(0, total, step):
            time.sleep(step)


if __name__ == "__main__":
    main()
