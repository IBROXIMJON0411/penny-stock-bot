import os
import time
import json
import logging
import html
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Optional
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

# --- Config ---
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
REDIS_URL = os.getenv("REDIS_URL")

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))       # seconds
MAX_CHECK = int(os.getenv("MAX_CHECK", "200"))               # tickers per scan
PRICE_THRESHOLD = float(os.getenv("PRICE_THRESHOLD", "8.0")) # max price to watch
VOLUME_MULT = float(os.getenv("VOLUME_MULT", "2.0"))        # volume spike threshold
CANDLE_INTERVAL = "15"                                       # 15-min candle
PAGE_SIZE_NEWS = int(os.getenv("PAGE_SIZE_NEWS", "10"))
MAX_ARTICLES_PER_RUN = int(os.getenv("MAX_ARTICLES_PER_RUN", "15"))
SEEN_TTL = int(os.getenv("SEEN_TTL", str(24*3600)))
SEEN_FILE = os.getenv("SEEN_FILE", "seen_articles.json")

KEYWORDS = [
    "earnings","fda","approval","trial","phase",
    "sec","investigation","lawsuit",
    "merger","acquisition","buyout",
    "offering","contract","partnership","agreement",
    "halt","trading halt","reverse split","compliance",
    "nasdaq","nyse","delisting"
]

# Required checks
if not POLYGON_API_KEY or not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    raise SystemExit("ERROR: POLYGON_API_KEY, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID must be set")

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("penny_news_volume_bot")

# --- HTTP session ---
session = requests.Session()
retry_strategy = Retry(
    total=3, backoff_factor=1, status_forcelist=[429,500,502,503,504],
    respect_retry_after_header=True, allowed_methods=frozenset(["GET","POST"])
)
session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
session.mount("http://", HTTPAdapter(max_retries=retry_strategy))
session.headers.update({"User-Agent":"penny-news-volume-bot/1.0"})

# optional Redis
use_redis = False
rconn = None
if REDIS_URL:
    try:
        import redis as _redis
        rconn = _redis.from_url(REDIS_URL, decode_responses=True)
        rconn.ping()
        use_redis = True
        logger.info("Connected to Redis for seen cache")
    except Exception:
        logger.exception("Redis connection failed, falling back to file")
        use_redis = False

# --- Utilities ---
REMOVE_QUERY_PREFIXES = ("utm_",)
REMOVE_QUERY_KEYS = {"fbclid","gclid","mc_cid","mc_eid"}

def normalize_url(u: str) -> str:
    try:
        p = urlparse(u)
        qs = parse_qs(p.query, keep_blank_values=True)
        new_qs = {}
        for k,v in qs.items():
            if any(k.lower().startswith(pref) for pref in REMOVE_QUERY_PREFIXES):
                continue
            if k.lower() in REMOVE_QUERY_KEYS:
                continue
            new_qs[k]=v
        q_items = [(k,vv) for k in sorted(new_qs.keys()) for vv in new_qs[k]]
        newp = p._replace(query=urlencode(q_items), fragment="")
        return urlunparse(newp)
    except Exception:
        return u

def try_parse_date(s: Optional[str]):
    if not s: return None
    try:
        if s.endswith("Z"): s = s.replace("Z","+00:00")
        return datetime.fromisoformat(s)
    except: pass
    import email.utils
    try: return email.utils.parsedate_to_datetime(s)
    except: return None

# --- Seen cache ---
def load_seen() -> Dict[str,float]:
    now=time.time()
    seen:Dict[str,float]={}
    if use_redis and rconn:
        try:
            items=rconn.hgetall("seen_map") or {}
            for k,v in items.items():
                try:
                    ts=float(v)
                    if now-ts<=SEEN_TTL: seen[k]=ts
                except: continue
            return seen
        except: return {}
    else:
        try:
            with open(SEEN_FILE,"r",encoding="utf-8") as f:
                data=json.load(f)
                if isinstance(data,dict):
                    for k,v in data.items():
                        try:
                            ts=float(v)
                            if now-ts<=SEEN_TTL: seen[k]=ts
                        except: continue
            return seen
        except: return {}
def save_seen(seen:Dict[str,float]):
    if use_redis and rconn:
        try:
            tmp="seen_map_tmp"
            if seen:
                rconn.delete(tmp)
                mapping={k:str(v) for k,v in seen.items()}
                rconn.hset(tmp,mapping=mapping)
                rconn.rename(tmp,"seen_map")
            else: rconn.delete("seen_map")
        except: pass
    else:
        try:
            tmp=SEEN_FILE+".tmp"
            with open(tmp,"w",encoding="utf-8") as f: json.dump(seen,f)
            os.replace(tmp,SEEN_FILE)
        except: pass
def mark_seen(seen:Dict[str,float],url:str): seen[url]=time.time()

# --- Telegram ---
def send_telegram(msg:str)->bool:
    url=f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload={"chat_id":TELEGRAM_CHAT_ID,"text":msg,"parse_mode":"HTML","disable_web_page_preview":False}
    try:
        r=session.post(url,json=payload,timeout=10)
        r.raise_for_status()
        return True
    except: return False

# --- Polygon helpers ---
def get_price_prev(ticker:str)->Optional[float]:
    try:
        r=session.get(f"https://api.polygon.io/v2/aggs/ticker/{ticker.upper()}/prev",params={"apiKey":POLYGON_API_KEY},timeout=10)
        r.raise_for_status()
        j=r.json()
        close=j.get("results",[{}])[0].get("c")
        return float(close) if close else None
    except: return None

def get_15min_candle(ticker:str):
    try:
        url=f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/15/minute/2026-01-01/2026-01-08"
        r=session.get(url,params={"apiKey":POLYGON_API_KEY},timeout=10)
        r.raise_for_status()
        j=r.json()
        return j.get("results",[])
    except: return []

def has_volume_spike(candles:list)->bool:
    if not candles: return False
    vols=[c.get("v",0) for c in candles[:-1]]
    last=candles[-1].get("v",0)
    avg=sum(vols)/len(vols) if vols else 0
    return last>VOLUME_MULT*avg if avg>0 else False

def fetch_news(ticker:str)->list:
    q=f'"{ticker}" AND ({" OR ".join(KEYWORDS)})'
    try:
        r=session.get("https://api.polygon.io/v2/reference/news",params={"query":q,"limit":PAGE_SIZE_NEWS,"apiKey":POLYGON_API_KEY},timeout=12)
        r.raise_for_status()
        j=r.json()
        return j.get("results",[])
    except: return []

def is_relevant(article:dict)->bool:
    text=(article.get("title","")+" "+article.get("summary","")+" "+article.get("description","")).lower()
    return any(kw.lower() in text for kw in KEYWORDS)

def format_msg(ticker:str,price:float,article:dict)->str:
    url=article.get("article_url") or article.get("url") or ""
    title=article.get("title") or article.get("headline") or ""
    source=article.get("publisher",{}).get("name","") if isinstance(article.get("publisher",{}),dict) else article.get("publisher","")
    pub=article.get("published_utc","")
    dt=try_parse_date(pub)
    pub_str=dt.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z") if dt else pub
    msg=f"🚨 <b>{ticker}</b> <code>${price:.2f}</code>\n📰 <b>{html.escape(title)}</b>\nSource: {html.escape(source)}\nPublished: {html.escape(pub_str)}\n{url}"
    return msg

# --- Main ---
def main():
    logger.info("penny_news_volume_bot starting")
    seen=load_seen()
    while True:
        try:
            # Scan tickers
            r=session.get("https://api.polygon.io/v3/reference/tickers",params={"market":"stocks","active":"true","limit":MAX_CHECK,"apiKey":POLYGON_API_KEY},timeout=15)
            r.raise_for_status()
            tickers=r.json().get("results",[])
            for t in tickers:
                sym=t.get("ticker")
                if not sym: continue
                price=get_price_prev(sym)
                if price is None or price>PRICE_THRESHOLD: continue
                candles=get_15min_candle(sym)
                if not has_volume_spike(candles): continue
                # Fetch news
                news=fetch_news(sym)
                for art in news:
                    url=art.get("article_url") or art.get("url") or ""
                    if not url: continue
                    norm=normalize_url(url)
                    if seen.get(norm): continue
                    if not is_relevant(art): continue
                    msg=format_msg(sym,price,art)
                    if send_telegram(msg):
                        mark_seen(seen,norm)
                save_seen(seen)
        except Exception as e:
            logger.exception("Error in main loop")
        time.sleep(POLL_INTERVAL)

if __name__=="__main__":
    main()
