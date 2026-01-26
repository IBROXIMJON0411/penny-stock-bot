from __future__ import annotations

himport os
import time
import json
import html
import logging
import tempfile
from typing import List, Dict, Optional, Any


import requests
import pandas as pd

# ================= OPTIONAL LIBS =================
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except Exception:
    YFINANCE_AVAILABLE = False

try:
    from deep_translator import GoogleTranslator
except Exception:
    GoogleTranslator = None

# ================= CONFIG =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))
SEEN_FILE = os.getenv("SEEN_FILE", "seen.json")

TICKERS_ENV = os.getenv("TICKERS")
TICKERS_FILE = os.getenv("TICKERS_FILE")

REDDIT_LIMIT = int(os.getenv("REDDIT_LIMIT", "5"))

RSI_OVERBOUGHT = float(os.getenv("RSI_OVERBOUGHT", "70"))
RSI_OVERSOLD = float(os.getenv("RSI_OVERSOLD", "30"))

SEND_ALL_INDICATORS = os.getenv("SEND_ALL_INDICATORS", "false").lower() == "true"

USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (compatible; DynamicBot/1.0)"
)

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger("dynamic_bot")

# ================= TRANSLATOR =================
translator = None
if GoogleTranslator:
    try:
        translator = GoogleTranslator(source="en", target="uz")
    except Exception:
        translator = None

def uzbek(text: str) -> str:
    if not translator or not text:
        return text
    try:
        return translator.translate(text)
    except Exception:
        return text

# ================= HTTP SESSION =================
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def create_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504)
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": USER_AGENT})
    return s

SESSION = create_session()

# ================= SEEN =================
def load_seen(path: str) -> Dict[str, float]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_seen_atomic(path: str, seen: Dict[str, float]) -> None:
    try:
        d = os.path.dirname(path) or "."
        with tempfile.NamedTemporaryFile("w", delete=False, dir=d, encoding="utf-8") as tf:
            json.dump(seen, tf, ensure_ascii=False, indent=2)
            tmp = tf.name
        os.replace(tmp, path)
    except Exception as e:
        logger.exception("Seen save error: %s", e)

# ================= TELEGRAM =================
def send_telegram(text: str) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        SESSION.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": False,
            },
            timeout=10
        )
    except Exception as e:
        logger.error("Telegram error: %s", e)

# ================= TICKERS =================
def load_tickers() -> List[str]:
    if TICKERS_ENV:
        return [t.strip() for t in TICKERS_ENV.split(",") if t.strip()]
    if TICKERS_FILE and os.path.exists(TICKERS_FILE):
        with open(TICKERS_FILE) as f:
            return [l.strip() for l in f if l.strip()]
    return []

# ================= INDICATORS =================
def fetch_price(symbol: str) -> Optional[pd.DataFrame]:
    if not YFINANCE_AVAILABLE:
        return None
    try:
        df = yf.download(symbol, period="90d", interval="1d", progress=False)
        if df.empty:
            return None
        return df.sort_index()
    except Exception:
        return None

def calculate_indicators(df: pd.DataFrame) -> Dict[str, float]:
    close = df["Close"].dropna()
    out: Dict[str, float] = {}

    if len(close) < 2:
        return out

    out["LAST"] = float(close.iloc[-1])

    out["EMA_10"] = float(close.ewm(span=10).mean().iloc[-1])
    out["EMA_50"] = float(close.ewm(span=50).mean().iloc[-1])

    ema12 = close.ewm(span=12).mean()
    ema26 = close.ewm(span=26).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9).mean()

    out["MACD"] = float(macd.iloc[-1])
    out["MACD_SIGNAL"] = float(signal.iloc[-1])

    delta = close.diff().dropna()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(14).mean().iloc[-1]
    avg_loss = loss.rolling(14).mean().iloc[-1]

    out["RSI"] = 100.0 if avg_loss == 0 else float(
        100 - (100 / (1 + avg_gain / avg_loss))
    )

    return out

# ================= SIGNALS =================
def detect_signals(ind: Dict[str, float]) -> List[str]:
    sig = []

    if ind["EMA_10"] > ind["EMA_50"]:
        sig.append("EMA bullish")

    if ind["MACD"] > ind["MACD_SIGNAL"]:
        sig.append("MACD bullish")

    if ind["RSI"] >= RSI_OVERBOUGHT:
        sig.append("RSI overbought")
    elif ind["RSI"] <= RSI_OVERSOLD:
        sig.append("RSI oversold")

    return sig

# ================= SOCIAL =================
def fetch_reddit() -> List[Dict[str, str]]:
    posts = []
    url = f"https://www.reddit.com/r/CryptoCurrency/new/.json?limit={REDDIT_LIMIT}"
    try:
        r = SESSION.get(url, timeout=10)
        j = r.json()
        for p in j.get("data", {}).get("children", []):
            d = p["data"]
            posts.append({
                "title": d.get("title", ""),
                "url": f"https://reddit.com{d.get('permalink')}"
            })
    except Exception:
        pass
    return posts

# ================= MAIN =================
def run_once(seen: Dict[str, float]) -> None:
    for symbol in load_tickers():
        df = fetch_price(symbol)
        if not df:
            continue

        ind = calculate_indicators(df)
        sig = detect_signals(ind)

        if sig:
            msg = (
                f"⚠️ <b>{symbol}</b>\n"
                + "\n".join(f"• {s}" for s in sig)
                + f"\nRSI: {ind['RSI']:.2f}"
            )
            send_telegram(msg)

        time.sleep(0.5)

    for p in fetch_reddit():
        if p["url"] in seen:
            continue
        send_telegram(f"💬 {uzbek(p['title'])}\n{p['url']}")
        seen[p["url"]] = time.time()
        save_seen_atomic(SEEN_FILE, seen)

def main():
    logger.info("Bot started")
    send_telegram("🚀 Bot ишга тушди ва ишлаяпти")
    seen = load_seen(SEEN_FILE)

    while True:
        run_once(seen)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
