from __future__ import annotations

import os
import time
import json
import html
import logging
import tempfile
from typing import List, Dict, Optional
from urllib.parse import quote_plus

import requests
import pandas as pd
import yfinance as yf

# ================= CONFIG =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))
SEEN_FILE = os.getenv("SEEN_FILE", "./seen.json")
TICKERS_ENV = os.getenv("TICKERS")
TICKERS_FILE = os.getenv("TICKERS_FILE")
REDDIT_LIMIT = int(os.getenv("REDDIT_LIMIT", "5"))
PRICE_RISE_THRESHOLD = float(os.getenv("PRICE_RISE_THRESHOLD", "5.0"))
VOLUME_SPIKE_THRESHOLD = float(os.getenv("VOLUME_SPIKE_THRESHOLD", "5.0"))
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
USER_AGENT = os.getenv("USER_AGENT", "VolumeBot/1.0")

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger("volume_bot")

# ================= HTTP SESSION =================
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def create_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504))
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": USER_AGENT})
    return s

SESSION = create_session()

# ================= SEEN =================
def load_seen(path: str) -> Dict[str, float]:
    """
    Load seen dict from JSON file. Returns empty dict on failure.
    Ensures values are floats (timestamps) where possible.
    """
    try:
        if not os.path.exists(path):
            return {}
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                out: Dict[str, float] = {}
                for k, v in data.items():
                    try:
                        out[str(k)] = float(v)
                    except Exception:
                        out[str(k)] = time.time()
                return out
    Exception as e:
        logger.warning("Seen С„Р°Р№Р»РЅРё СЋРєР»Р°С€РґР° С…Р°С‚Рѕ: %s", e)
    return {}

def save_seen_atomic(path: str, seen: Dict[str, float]) -> None:
    """
    Robust atomic save:
    - Write temp file in system tempdir (avoids needing target dir writable).
    - Then try os.replace to move temp into place.
    - If os.replace fails, attempt to create target dir and write directly.
    - Clean up temp file on errors.
    """
    abs_path = os.path.abspath(path)
    tmpname = None
    try:
        # Create system-temp temporary file (no dir specified)
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as tf:
            json.dump(seen, tf, ensure_ascii=False, indent=2)
            tmpname = tf.name

        # Attempt atomic replace first
        try:
            # Try to ensure target dir exists (best-effort) before replace
            target_dir = os.path.dirname(abs_path) or "."
            try:
                os.makedirs(target_dir, exist_ok=True)
            except Exception:
                # If cannot create, still try replace (may fail)
                logger.debug("Cannot ensure target dir exists (%s); will attempt os.replace anyway.", target_dir)

            os.replace(tmpname, abs_path)
            tmpname = None  # moved successfully
            return
        except Exception as e_replace:
            logger.debug("os.replace failed: %s", e_replace)
            # Try direct write to target as fallback
            try:
                # Ensure directory exists
                try:
                    os.makedirs(os.path.dirname(abs_path) or ".", exist_ok=True)
                except Exception:
                    logger.debug("Could not create target dir for direct write.")
                with open(abs_path, "w", encoding="utf-8") as f:
                    json.dump(seen, f, ensure_ascii=False, indent=2)
                # Clean up temp file if still present
                try:
                    if tmpname and os.path.exists(tmpname):
                        os.remove(tmpname)
                        tmpname = None
                except Exception:
                    pass
                return
            except Exception as e_write:
                logger.error("Direct write to %s failed: %s", abs_path, e_write)
    except Exception as e:
        logger.error("Seen save error: %s", e)
    finally:
        # Cleanup any leftover tempfile
        try:
            if tmpname and os.path.exists(tmpname):
                os.remove(tmpname)
        except Exception:
            pass

def mark_seen(seen: Dict[str, float], key: str) -> None:
    seen[str(key)] = time.time()

# ================= TELEGRAM =================
def send_telegram(msg: str) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.debug("Telegram РєРѕРЅС„РёРіСѓСЂР°С†РёСЏСЃРё Р№СћТ›, С…Р°Р±Р°СЂ СЋР±РѕСЂРёР»РјР°РґРё")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "HTML"
    }
    try:
        r = SESSION.post(url, json=payload, timeout=10)
        r.raise_for_status()
    except Exception as e:
        logger.error("Telegram С…Р°С‚Рѕ: %s", e)

# ================= TICKERS =================
def load_tickers() -> List[str]:
    if TICKERS_ENV:
        return [t.strip() for t in TICKERS_ENV.split(",") if t.strip()]
    if TICKERS_FILE and os.path.exists(TICKERS_FILE):
        try:
            with open(TICKERS_FILE, "r", encoding="utf-8") as f:
                return [l.strip() for l in f ifOLUME =================
def fetch_price_data(symbol: str) -> Optional[pd.DataFrame]:
    try:
        df = yf.download(symbol, period="10d", interval="1d", progress=False)
        if df is None or df.empty:
            return None
        return df
    except Exception as e:
        logger.debug("Price fetch error for %s: %s", symbol, e)
        return None

def percent_change(a: float, b: float) -> Optional[float]:
    try:
        if b == 0:
            return None
        return (a - b) / b * 100
    except Exception:
        return None

def check_volume_price_spike(df: pd.DataFrame) -> Optional[str]:
    if df is None or len(df) < 2:
        return None

    today = df.iloc[-1]
    prev = df.iloc[-2]

    try:
        price_change = percent_change(float(today["Close"]), float(prev["Close"]))
        volume_change = percent_change(float(today["Volume"]), float(prev["Volume"]))
    except Exception:
        return None

    if (
        price_change is not None
        and volume_change is not None
        and price_change >= PRICE_RISE_THRESHOLD
        and volume_change >= VOLUME_SPIKE_THRESHOLD
    ):
        return f"рџ“€ РќР°СЂС… {price_change:.1f}% РІР° Volume {volume_change:.1f}% РѕС€РґРё"

    # handle division-by-zero (prev == 0)
    if price_change is None and volume_change is None and float(today.get("Close", 0)) > 0 and float(today.get("Volume", 0)) > 0:
        return "рџ“€ РќР°СЂС… РІР° VolumeРґР° СЏРЅРіРё С„Р°РѕР»РёСЏС‚ (РёР»Рє РјР°СЉР»СѓРјРѕС‚Р»Р°СЂ)"

    return None

# ================= REDDIT =================
def fetch_reddit_discussions():
    url = f"https://www.reddit.com/r/stocks/new/.json?limit={REDDIT_LIMIT}"
    posts = []
    try:
        r = SESSION.get(url, timeout=10)
        r.raise_for_status()
        data = r.json().get("data", {}).get("children", [])
        for p in data:
            d = p.get("data", {})
            posts.append({
                "title": d.get("title", ""),
                "url": f"https://reddit.com{d.get('permalink', '')}"
            })
    except Exception as e:
        logger.debug("Reddit fetch error: %s", e)
    return posts

# ================= POLYGON NEWS =================
def fetch_polygon_news(symbol: str):
    if not POLYGON_API_KEY:
        return []
    url = (
        "https://api.polygon.io/v2/reference/news?"
        f"query={quote_plus(symbol)}&limit=3&apiKey={POLYGON_API_KEY}"
    )
    news = []
    try:
        r = SESSION.get(url, timeout=10)
        r.raise_for_status()
        for n in r.json().get("results", []):
            news.append({
                "title": n.get("title", ""),
                "url": n.get("article_url", "") or n.get("url", "")
            })
    except Exception as e:
        logger.debug("Polygon fetch %s", symbol, e)
    return news

# ================= MAIN LOOP =================
def run_once(seen: Dict[str, float]) -> None:
    tickers = load_tickers()

    if not tickers:
        logger.info("РўРёРєРµСЂР»Р°СЂ Р±РµСЂРёР»РјР°РіР°РЅ")
        return

    logger.info("РљСѓР·Р°С‚РёР»Р°С‘С‚РіР°РЅ С‚РёРєРµСЂР»Р°СЂ: %s", ", ".join(tickers))

    for symbol in tickers:
        df = fetch_price_data(symbol)
        if not df:
            continue

        spike = check_volume_price_spike(df)
        if spike:
            send_telegram(
                f"вљ пёЏ <b>{html.escape(symbol)}</b>\n{html.escape(spike)}"
            )
            mark_seen(seen, f"spike_{symbol}")
            save_seen_atomic(SEEN_FILE, seen)

            for n in fetch_polygon_news(symbol):
                key = n.get("url") or n.get("title")
                if not key or key in seen:
                    continue
                send_telegram(
                    f"рџ“° <b>{html.escape(symbol)}</b>\n"
                    f"{html.escape(n['title'])}\n{html.escape(n['url'])}"
                )
                mark_seen(seen, key)
                save_seen_atomic(SEEN_FILE, seen)

        time.sleep(0.3)

    for p in fetch_reddit_discussions():
        key = p.get("url") or p.get(" or key in seen:
            continue
        send_telegram(
            f"рџ’¬ <b>Reddit РјСѓТіРѕРєР°РјР°СЃРё</b>\n"
            f"{html.escape(p['title'])}\n{html.escape(p['url'])}"
        )
        mark_seen(seen, key)
        save_seen_atomic(SEEN_FILE, seen)
        time.sleep(0.2)

# ================= ENTRY =================
def main():
    logger.info("Р‘РѕС‚ РёС€РіР° С‚СѓС€РґРё")
    send_telegram(
        "вњ… <b>Р‘РѕС‚ РёС€РіР° С‚СѓС€РґРё</b>\n"
        "рџ“Љ Volume РІР° РЅР°СЂС… РєСѓР·Р°С‚РёР»СЏРїС‚Рё\n"
        "рџ“° РЇРЅРіРёР»РёРєР»Р°СЂ РІР° Reddit РјСѓТіРѕРєР°РјР°Р»Р°СЂРё С‚РµРєС€РёСЂРёР»Р°РґРё"
    )

    seen = load_seen(SEEN_FILE)

    try:
        while True:
            run_once(seen)
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Р‘РѕС‚ С‚СћС…С‚Р°С‚РёР»РґРё (KeyboardInterrupt)")
    finally:
        try:
            save_seen_atomic(SEEN_FILE, seen)
        except Exception:
            pass

if __name__ == "__main__":
    main()
