import os
import time
import json
import html
import logging
from typing import List, Dict, Optional
from datetime import datetime, timezone

import requests
import pandas as pd
import yfinance as yf
from deep_translator import GoogleTranslator

# ================= CONFIG =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 300))
SEEN_FILE = "seen.json"

# ================= LOGGING =================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("dynamic_bot")

# ================= TRANSLATOR =================
translator = GoogleTranslator(source="en", target="uz")
def uzbek(text: str) -> str:
    try:
        return translator.translate(text)
    except Exception:
        return text

# ================= SEEN TRACKER =================
def load_seen() -> Dict[str, float]:
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE, "r") as f:
            return json.load(f)
    return {}

def save_seen(seen: Dict[str, float]):
    with open(SEEN_FILE, "w") as f:
        json.dump(seen, f)

def mark_seen(seen: Dict[str, float], key: str):
    seen[key] = time.time()

# ================= TELEGRAM =================
def send_telegram(msg: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "HTML",
        "disable_web_page_preview": False
    }
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        logger.exception("Telegram error: %s", e)

# ================= TECHNICAL INDICATORS =================
def fetch_price_data(symbol: str, period="30d", interval="1d") -> Optional[pd.DataFrame]:
    try:
        df = yf.download(symbol, period=period, interval=interval)
        if df.empty:
            return None
        return df
    except Exception:
        return None

def calculate_indicators(df: pd.DataFrame) -> Dict[str, float]:
    indicators = {}
    close = df["Close"]

    indicators["SMA_10"] = close.rolling(10).mean().iloc[-1]
    indicators["SMA_50"] = close.rolling(50).mean().iloc[-1] if len(close) >= 50 else None

    indicators["EMA_10"] = close.ewm(span=10, adjust=False).mean().iloc[-1]
    indicators["EMA_50"] = close.ewm(span=50, adjust=False).mean().iloc[-1] if len(close) >= 50 else None

    # MACD
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    indicators["MACD"] = (ema12 - ema26).iloc[-1]

    # RSI
    delta = close.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()

    if avg_loss.iloc[-1] == 0:
        indicators["RSI"] = 100
    else:
        rs = avg_gain.iloc[-1] / avg_loss.iloc[-1]
        indicators["RSI"] = 100 - (100 / (1 + rs))

    return indicators

def format_indicator_msg(symbol: str, indicators: Dict[str, float]) -> str:
    msg = f"📊 <b>{symbol}</b> Техник индикаторлар:\n"
    for k, v in indicators.items():
        if v is not None:
            msg += f"{k}: {v:.2f}\n"
    return msg

# ================= SOCIAL DISCUSSION =================
SOCIAL_SOURCES = [
    {"name":"Reddit", "url":"https://www.reddit.com/r/CryptoCurrency/new/.json?limit=5"},
    # Twitter RSS yoki boshqa bepul linklarni shu yerga qo'shish mumkin
]

def fetch_social_discussions() -> List[Dict]:
    discussions = []
    for src in SOCIAL_SOURCES:
        try:
            r = requests.get(src["url"], headers={"User-Agent":"Mozilla/5.0"}, timeout=10)
            j = r.json()
            posts = j.get("data", {}).get("children", [])
            for p in posts:
                data = p.get("data", {})
                discussions.append({
                    "source": src["name"],
                    "title": data.get("title"),
                    "url": "https://reddit.com"+data.get("permalink","") if src["name"]=="Reddit" else data.get("link")
                })
        except Exception:
            continue
    return discussions

def format_social_msg(post: Dict) -> str:
    title_uz = uzbek(post.get("title",""))
    url = post.get("url","")
    source = post.get("source","")
    return f"💬 <b>{source}</b>\n📰 {title_uz}\n{html.escape(url)}"

# ================= MAIN LOOP =================
def main():
    logger.info("Dynamic Bot ishga tushdi")
    seen = load_seen()
    while True:
        # --- Technical indicators part ---
        symbols = ["BTC-USD","ETH-USD","AAPL","TSLA"]  # dynamic list, can add/remove
        for s in symbols:
            df = fetch_price_data(s)
            if df is None:
                continue
            indicators = calculate_indicators(df)
            msg = format_indicator_msg(s, indicators)
            send_telegram(msg)
            time.sleep(1)

        # --- Social discussion part ---
        posts = fetch_social_discussions()
        for p in posts:
            key = p.get("url")
            if key in seen:
                continue
            msg = format_social_msg(p)
            send_telegram(msg)
            mark_seen(seen, key)
            save_seen(seen)
            time.sleep(1)

        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
