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
    if not os.path.exists(path):
        return {}
    try:
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
    except Exception as e:
        logger.warning("Seen файлни юклашда хато: %s", e)
    return {}

def save_seen_atomic(path: str, seen: Dict[str, float]) -> None:
    """
    Safely write seen dict to path atomically where possible.
    - Tries to create target directory.
    - Writes temp file (in system tempdir) then tries os.replace to target.
    - If replace fails, falls back to direct write to target (best-effort).
    - Cleans up temporary file on failure.
    """
    abs_path = os.path.abspath(path)
    target_dir = os.path.dirname(abs_path) or "."

    tmpname = None
    # Try to ensure target directory exists
    try:
        os.makedirs(target_dir, exist_ok=True)
    except Exception as e:
        logger.warning("Directory '%s' yaratib bo'lmadi: %s. Fallback system temp ishlatiladi.", target_dir, e)
        # keep going; we'll still try to write to temp and then move

    try:
        # Create temp file in system tempdir (avoids failures when target dir not creatable)
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as tf:
            json.dump(seen, tf, ensure_ascii=False, indent=2)
            tmpname = tf.name

        # Try atomic replace (best option)
        try:
            os.replace(tmpname, abs_path)
            return
        except Exception as e_replace:
            logger.debug("os.replace failed (%s). Trying direct write to target.", e_replace)
            # Attempt direct write to target as fallback
            try:
                with open(abs_path, "w", encoding="utf-8") as f:
                    json.dump(seen, f, ensure_ascii=False, indent=2)
                # cleanup temp
                if tmpname and os.path.exists(tmpname):
                    os.remove(tmpname)
                return
            except Exception as e_write:
                logger.error("Direct write to %s failed: %s", abs_path, e_write)
                # keep going to cleanup below
    except Exception as e:
        logger.error("Seen save error: %s", e)
    finally:
        # Cleanup temp file if it still exists
        try:
            if tmpname and os.path.exists(tmpname):
                os.remove(tmpname)
        except Exception:
            pass

# ================= TELEGRAM =================
def send_telegram(msg: str) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.debug("Telegram конфигурацияси йўқ, хабар юборилмади")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "HTML"
    }
    try:
        SESSION.post(url, json=payload, timeout=10)
    except Exception as e:
        logger.error("Telegram хато: %s", e)

# ================= TICKERS =================
def load_tickers() -> List[str]:
    if TICKERS_ENV:
        return [t.strip() for t in TICKERS_ENV.split(",") if t.strip()]
    if TICKERS_FILE and os.path.exists(TICKERS_FILE):
        try:
            with open(TICKERS_FILE, "r", encoding="utf-8") as f:
                return [l.strip() for l in f if l.strip()]
        except Exception as e:
            logger.warning("Tickers файлни ўқишда хато: %s", e)
    return []

# ================= PRICE & VOLUME =================
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
        return f"📈 Нарх {price_change:.1f}% ва Volume {volume_change:.1f}% ошди"

    # handle division-by-zero (prev == 0)
    if price_change is None and volume_change is None and float(today.get("Close", 0)) > 0 and float(today.get("Volume", 0)) > 0:
        return "📈 Нарх ва Volumeда янги фаолият (илк маълумотлар)"

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
        logger.debug("Polygon fetch error for %s: %s", symbol, e)
    return news

# ================= MAIN LOOP =================
def run_once(seen: Dict[str, float]) -> None:
    tickers = load_tickers()

    if not tickers:
        logger.info("Тикерлар берилмаган")
        return

    logger.info("Кузатилаётган тикерлар: %s", ", ".join(tickers))

    for symbol in tickers:
        df = fetch_price_data(symbol)
        if not df:
            continue

        spike = check_volume_price_spike(df)
        if spike:
            send_telegram(
                f"⚠️ <b>{html.escape(symbol)}</b>\n{html.escape(spike)}"
            )
            mark_seen(seen, f"spike_{symbol}")
            save_seen_atomic(SEEN_FILE, seen)

            for n in fetch_polygon_news(symbol):
                key = n.get("url") or n.get("title")
                if not key or key in seen:
                    continue
                send_telegram(
                    f"📰 <b>{html.escape(symbol)}</b>\n"
                    f"{html.escape(n['title'])}\n{html.escape(n['url'])}"
                )
                mark_seen(seen, key)
                save_seen_atomic(SEEN_FILE, seen)

        time.sleep(0.3)

    for p in fetch_reddit_discussions():
        key = p.get("url") or p.get("title")
        if not key or key in seen:
            continue
        send_telegram(
            f"💬 <b>Reddit муҳокамаси</b>\n"
            f"{html.escape(p['title'])}\n{html.escape(p['url'])}"
        )
        mark_seen(seen, key)
        save_seen_atomic(SEEN_FILE, seen)
        time.sleep(0.2)

# ================= ENTRY =================
def main():
    logger.info("Бот ишга тушди")
    send_telegram(
        "✅ <b>Бот ишга тушди</b>\n"
        "📊 Volume ва нарх кузатиляпти\n"
        "📰 Янгиликлар ва Reddit муҳокамалари текширилади"
    )

    seen = load_seen(SEEN_FILE)

    try:
        while True:
            run_once(seen)
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Бот тўхтатилди (KeyboardInterrupt)")
    finally:
        try:
            save_seen_atomic(SEEN_FILE, seen)
        except Exception:
            pass

if __name__ == "__main__":
    main()
