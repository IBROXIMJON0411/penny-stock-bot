import os
import time
import json
import html
import logging
import tempfile
from typing import List, Dict, Optional, Any
from datetime import datetime
from __future__ import annotations

import requests
import pandas as pd

# Optional: lazy import yfinance only when used
YFINANCE_AVAILABLE = True
try:
    import yfinance as yf
except Exception:
    YFINANCE_AVAILABLE = False

# Optional translator
try:
    from deep_translator import GoogleTranslator
except Exception:
    GoogleTranslator = None

# ================= CONFIG =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))
SEEN_FILE = os.getenv("SEEN_FILE", "seen.json")
TICKERS_ENV = os.getenv("TICKERS")          # comma-separated tickers
TICKERS_FILE = os.getenv("TICKERS_FILE")    # path to file with tickers, one per line
REDDIT_LIMIT = int(os.getenv("REDDIT_LIMIT", "5"))
RSI_OVERBOUGHT = float_OVERBOUGHT", "70"))
RSI_OVERSOLD = float(os.getenv("RSI_OVERSOLD", "30"))
SEND_ALL_INDICATORS = os.getenv("SEND_ALL_INDICATORS", "false").lower() == "true"
USER_AGENT = os.getenv("USER_AGENT", "DynamicBot/1.0 (+https://example.com)")

# ================= LOGGING =================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("dynamic_bot")

# ================= TRANSLATOR =================
_translator = None
if GoogleTranslator is not None:
    try:
        _translator = GoogleTranslator(source="en", target="uz")
    except Exception:
        _translator = None

def uzbek(text: str) -> str:
    if not text:
        return ""
    if _translator is None:
        return text
    try:
        return _translator.translate(text)
    except Exception:
        return text

# ================= HTTP SESSION with retries =================
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def create_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504))
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": USER_AGENT})
    return session

SESSION = create_session()

# ================= SEEN TRACKER =================
def load_seen(path: str) -> Dict[str, float]:
    try:
        if not os.path.exists(path):
            return {}
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return {k: float(v) for k, v in data.items()}
            return {}
    except Exception as e:
        logger.warning("Failed to load seen file %s", path, e)
        return {}

def save_seen_atomic(path: str, seen: Dict[str, float]) -> None:
    try:
        dirpath = os.path.dirname(path) or "."
        with tempfile.NamedTemporaryFile("w", delete=False, dir=dirpath, encoding="utf-8") as e:
        logger.exception("Failed to save seen file: %s", e)

def mark_seen(seen: Dict[str, float], key: str) -> None:
    seen[key] = time.time()

# ================= TELEGRAM =================
def send_telegram(msg: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram token or chat id not configured; skipping send.")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "HTML",
        "disable_web_page_preview": False
    }
    try:
        r = SESSION.post(url, json=payload, timeout=10)
        r.raise_for_status()
        j = r.json()
        if not j.get("ok"):
            logger.error("Telegram API returned not-ok: %s", j)
            return False
        return True
    except Exception as e:
        logger.exception("Telegram send error: %s", e)
        return False

# ================= TICKER LOADING (NO DEFAULT TICKERS) =================
def load_tickers() -> List[str]:
    tickers: List[str] = []
    if TICKERS_ENV:
        tickers = [t.strip() for t in TICKERS_ENV.split(",") if t.strip()]
    elif TICKERS_FILE and os.path.exists(TICKERS_FILE):
        try:
            with open(TICKERS_FILE, "r", encoding="utf-8") as f:
                tickers = [line.strip() for line in f if line.strip()]
        except Exception as e:
            logger.warning("Failed to load tickers from file: %s", e)
    # Intentionally do NOT provide default tickers here
    return tickers

# ================= TECHNICAL INDICATORS =================
def fetch_price_data(symbol: str, period: str = "60d", interval: str = "1d") -> Optional[pd.DataFrame]:
    if not YFINANCE_AVAILABLE:
        logger.error("yfinance is not available (not installed). Cannot fetch price data for %s", symbol)
        return None
    try:
        df = yf.download(symbol, period=period, interval=interval, progress=False, threads=False)
        if df is None or df.empty:
            logger.debug("No price data for %s", symbol)
            return None
        df = df.sort_index()
        return df
    except Exception as e:
        logger.exception("Failed to fetch data for %s: %s", symbol, e)
        return None

def calculate_indicators(df: pd.DataFrame) -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float = {}
    close = df["Close"].dropna()
    n = len(close)
    if n == 0:
        return out
    out["LAST"] = float(close.iloc[-1])
    out["SMA_10"] = float(close.rolling(10).mean().iloc[-1]) if n >= 10 else None
    out["SMA_50"] = float(close.rolling(50).mean().iloc[-1]) if n >= 50 else None
    out["EMA_10"] = float(close.ewm(span=10, adjust=False).mean().iloc[-1]) if n >= 1 else None
    out["EMA_50"] = float(close.ewm(span=50, adjust=False).mean().iloc[-1]) if n >= 50 else None
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    out["MACD"] = float(macd.iloc[-1]) if len(macd) >= 1 else None
    out["MACD_SIGNAL"] = float(macd.ewm(span=9, adjust=False).mean().iloc[-1]) if len(macd) >= 1 else None
    delta = close.diff().dropna()
    if len(delta) >= 14:
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)
        avg_gain = gain.rolling(14).mean().iloc[-1]
        avg_loss = loss.rolling(14).mean().iloc[-1]
        if avg_loss == 0:
            rsi = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100.0 - (100.0 / (1.0 + rs))
        out["RSI"] = float(rsi)
    else:
        out["RSI"] = None
    return out

def format_indicator_msg(symbol: str, indicators: Dict[str, Optional[float]]) -> str:
    lines = [f"📊 <b>{html.escape(symbol)}</b> Техник индикаторлар:"]
    for k in ("LAST", "SMA_10", "SMA_50", "EMA_10", "EMA_50", "MACD", "MACD_SIGNAL", "RSI"):
        v = indicators.get(k)
        if v is None:
            continue
        if k == "RSI":
            lines.append(f"{k}: {v:.2f}")
        else:
            lines.append(f"{k}: {v:.6f}" if isinstance(v, float) else f"{k}: {v}")
    return "\n".join(lines)

def detect_signals(df: pd.DataFrame, indicators: Dict[str, Optional[float]]) -> List[str]:
    signals: List[str] = []
    close = df["Close"].dropna()
    if len(close) < 2:
        return signals

    def cross(series_a: pd.Series, series_b: pd.Series) -> Optional[str]:
        if len(series_a) < 2 or len(series_b) < 2:
            return None
        a_prev, a_now = series_a.iloc[-2], series_a.iloc[-1]
        b_prev, b_now = series_b.iloc[-2], series_b.iloc[-1]
        if pd.isna(a_prev) or pd.isna(a_now) or pd.isna(b_prev) or pd.isna(b_now):
            return None
        if a_prev <= b_prev and a_now > b_now:
            return "bull_cross"
        if a_prev >= b_prev and a_now < b_now:
            return "bear_cross"
        return None

    # SMA cross if enough data
    if len(close) >= 50:
        sma10 = close.rolling(10).mean()
        sma50 = close.rolling(50).mean()
        res = cross(sma10, sma50)
        if res == "bull_cross":
            signals.append("SMA(10) crossed above SMA(50) — bullish")
        elif res == "bear_cross":
            signals.append("SMA(10) crossed below SMA(50) — bearish")

    # EMA cross
    ema10 = close.ewm(span=10, adjust=False).mean()
    ema50 = close.ewm(span=50, adjust=False).mean()
    res = cross(ema10, ema50)
    if res == "bull_cross":
        signals.append("EMA(10) crossed above EMA(50) — bullish")
    elif res == "bear_cross":
        signals.append("EMA(10) crossed below EMA(50) — bearish")

    # MACD cross
    macd = close.ewm(span=12, adjust=False).mean() - close.ewm(span=26, adjust=False).mean()
    macd_signal = macd.ewm(span=9, adjust=False).mean()
    res = cross(macd, macd_signal)
    if res == "bull_cross":
        signals.append("MACD crossed above its signal line — bullish momentum")
    elif res == "bear_cross":
        signals.append("MACD crossed below its signal line — bearish momentum")

    rsi = indicators.get("RSI")
    if rsi is not None:
        if rsi >= RSI_OVERBOUGHT:
            signals.append(f"RSI {rsi:.1f} — overbought")
        elif rsi <= RSI_OVERSOLD:
            signals.append(f"RSI {rsi:.1f} — oversold")

    return signals

def build_signal_message(symbol: str, indicators: Dict[str, Optional[float]], signal_texts: List[str]) -> str:
    title = f"⚠️ <b>{html.escape(symbol)}</b> Сигналлар:"
    body_lines = [f"• {s}" for s in signal_texts]
    body_lines.append("")  # blank line
    body_lines.append(format_indicator_msg(symbol, indicators))
    return "\n".join([title] + body_lines)

# ================= SOCIAL DISCUSSION =================
SOCIAL_SOURCES = [
    {"name": "Reddit", "url": f"https://www.reddit.com/r/CryptoCurrency/new/.json?limit={REDDIT_LIMIT}"},
]

def fetch_social_discussions() -> List[Dict[str, Any]]:
    discussions: List[Dict[str, Any]] = []
    for src in SOCIAL_SOURCES:
        try:
            r = SESSION.get(src["url"], timeout=10)
            r.raise_for_status()
            j = r.json()
            posts = j.get("data", {}).get("children", [])
            for p in posts:
                data = p.get("data", {})
                title = data.get("title") or data.get("selftext") or ""
                permalink = data.get("permalink")
                url = f"https://reddit.com{permalink}" if permalink else data.get("url")
                discussions.append({"source": src["name"], "title": title, "url": url})
        except Exception as e:
            logger.debug("Failed to fetch social source %s: %s", src.get("name"), e)
            continue
    return discussions

def format_social_msg(post: Dict[str, Any]) -> str:
    title_uz = uzbek(post.get("title", ""))
    url = post.get("url", "")
    source = post.get("source", "")
    return f"💬 <b>{html.escape(source)}</b>\n📰 {html.escape(title_uz)}\n{html.escape(url)}"

# ================= MAIN LOOP =================
def run_once(seen: Dict[str, float]) -> None:
    tickers = load_tickers()
    if tickers:
        logger.info("Checking tickers: %s", ", ".join(tickers))
    else:
        logger.info("No tickers configured (TICKERS or TICKERS_FILE). Skipping price checks.")

    for symbol in tickers:
        try:
            df = fetch_price_data(symbol)
            if df is None:
                logger.debug("No data for %s, skipping", symbol)
                continue
            indicators = calculate_indicators(df)
            signals = detect_signals(df, indicators)
            if signals:
                msg = build_signal_message(symbol, indicators, signals)
                if send_telegram(msg):
                    logger.info("Sent signal for %s: %s", symbol, signals)
                else:
                    logger.warning("Failed to send signal message for %s", symbol)
            elif SEND_ALL_INDICATORS:
                msg = format_indicator_msg(symbol, indicators)
                send_telegram(msg)
            time.sleep(0.5)
        except Exception as e:
            logger.exception("Error processing %s: %s", symbol, e)

    # Social discussions
    posts = fetch_social_discussions()
    for p in posts:
        key = p.get("url") or p.get("title")
        if not key:
            continue
        if key in seen:
            continue
        msg = format_social_msg(p)
        if send_telegram(msg):
            mark_seen(seen, key)
            save_seen_atomic(SEEN_FILE, seen)
            logger.info("Sent social post from %s: %s", p.get("source"), p.get("url"))
        time.sleep(0.5)

def main() -> None:
    logger.info("Dynamic Bot starting")
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram is not fully configured; the bot will run but will not send messages.")
    if not YFINANCE_AVAILABLE:
        logger.warning("yfinance not installed. Price/indicator features will be disabled unless yfinance is available.")

    seen = load_seen(SEEN_FILE)
    try:
        while True:
            start = time.time()
            run_once(seen)
            elapsed = time.time() - start
            sleep_for = max(0.0, POLL_INTERVAL - elapsed)
            logger.debug("Sleeping for %.1f seconds", sleep_for)
            time.sleep(sleep_for)
    except KeyboardInterrupt:
        logger.info("Interrupted by user, exiting")
    except Exception as e:
        logger.exception("Fatal error: %s", e)
    finally:
        try:
            save_seen_atomic(SEEN_FILE, seen)
        except Exception:
            pass
        logger.info("Dynamic Bot stopped")

if __name__ == "__main__":
    main()
