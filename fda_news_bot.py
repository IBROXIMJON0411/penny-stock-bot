import os
import time
import json
import html
import logging
import tempfile
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime, timezone

import requests
import yfinance as yf

# Optional translator
try:
    from deep_translator import GoogleTranslator
except Exception:
    GoogleTranslator = None  # type: ignore

# ================= CONFIG =================
@dataclass
class Config:
    telegram_token: Optional[str] = os.getenv("TELEGRAM_TOKEN")
    telegram_chat_id: Optional[str] = os.getenv("TELEGRAM_CHAT_ID")
    poll_interval: int = int(os.getenv("POLL_INTERVAL", "300"))
    seen_file: str = os.getenv("SEEN_FILE", "seen.json")
    tickers_env: Optional[str] = os.getenv("TICKERS")  # comma-separated
    tickers_file: Optional[str] = os.getenv("TICKERS_FILE")
    reddit_limit: int = int(os.getenv("REDDIT_LIMIT", "5"))
    rsi_overbought: float = float(os.getenv("RSI_OVERBOUGHT", "70"))
    rsi_oversold: float = float(os.getenv("RSI_OVERSOLD", "30"))
    send_all_indicators: bool = os.getenv("SEND_ALL_INDICATORS", "false").lower() == "true"
    user_agent: str = os.getenv("USER_AGENT", "DynamicBot/1.0 (+https://example.com)")

cfg = Config()

# Default tickers if none provided
DEFAULT_TICKERS = ["BTC-USD", "ETH-USD", "AAPL", "TSLA"]

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)
logger = logging.getLogger("dynamic_bot")

# ================= TRANSLATOR =================
def get_translator():
    if GoogleTranslator is None:
        return None
    try:
        return GoogleTranslator(source="en", target="uz")
    except Exception:
        return None

_translator = get_translator()

def uzbek(text: str) -> str:
    if not text:
        return ""
    if _translator is None:
        return text
    try:
        return _translator.translate(text)
    except Exception as e:
        logger.debug("Translator error: %s", e)
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
    session.headers.update({"User-Agent": cfg.user_agent})
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
        logger.warning("Failed to load seen file %s: %s", path, e)
        return {}

def save_seen_atomic(path: str, seen: Dict[str, float]) -> None:
    try:
        dirpath = os.path.dirname(path) or "."
        with tempfile.NamedTemporaryFile("w", delete=False, dir=dirpath, encoding="utf-8") as tf:
            json.dump(seen, tf, ensure_ascii=False, indent=2)
            tmpname = tf.name
        os.replace(tmpname, path)
    except Exception as e:
        logger.exception("Failed to save seen file: %s", e)

def mark_seen(seen: Dict[str, float], key: str) -> None:
    seen[key] = time.time()

# ================= TELEGRAM =================
def send_telegram(msg: str) -> bool:
    if not cfg.telegram_token or not cfg.telegram_chat_id:
        logger.warning("Telegram token or chat id not configured, skipping send.")
        return False
    url = f"https://api.telegram.org/bot{cfg.telegram_token}/sendMessage"
    payload = {
        "chat_id": cfg.telegram_chat_id,
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

# ================= HELPERS =================
def load_tickers() -> List[str]:
    tickers: List[str] = []
    if cfg.tickers_env:
        tickers = [t.strip() for t in cfg.tickers_env.split(",") if t.strip()]
    elif cfg.tickers_file and os.path.exists(cfg.tickers_file):
        try:
            with open(cfg.tickers_file, "r", encoding="utf-8") as f:
                tickers = [line.strip() for line in f if line.strip()]
        except Exception as e:
            logger.warning("Failed to load tickers from file: %s", e)
    if not tickers:
        tickers = DEFAULT_TICKERS
    return tickers

# ================= TECHNICAL INDICATORS =================
def fetch_price_data(symbol: str, period: str = "60d", interval: str = "1d") -> Optional[pd.DataFrame]:
    try:
        df = yf.download(symbol, period=period, interval=interval, progress=False, threads=False)
        if df is None or df.empty:
            logger.debug("No price data for %s", symbol)
            return None
        # Ensure index is datetime and sorted
        df = df.sort_index()
        return df
    except Exception as e:
        logger.exception("Failed to fetch data for %s: %s", symbol, e)
        return None

def calculate_indicators(df: pd.DataFrame) -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {}
    close = df["Close"].dropna()
    n = len(close)
    if n == 0:
        return out
    # Simple moving averages
    out["SMA_10"] = float(close.rolling(10).mean().iloc[-1]) if n >= 10 else None
    out["SMA_50"] = float(close.rolling(50).mean().iloc[-1]) if n >= 50 else None
    # Exponential moving averages
    out["EMA_10"] = float(close.ewm(span=10, adjust=False).mean().iloc[-1]) if n >= 1 else None
    out["EMA_50"] = float(close.ewm(span=50, adjust=False).mean().iloc[-1]) if n >= 50 else None
    # MACD: 12-26 EMA and signal 9
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    out["MACD"] = float(macd.iloc[-1]) if len(macd) >= 1 else None
    # MACD Signal
    signal = macd.ewm(span=9, adjust=False).mean()
    out["MACD_SIGNAL"] = float(signal.iloc[-1]) if len(signal) >= 1 else None
    # RSI (Wilder's smoothing approximation using simple rolling for first pass)
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
    # Last close
    out["LAST"] = float(close.iloc[-1])
    return out

def format_indicator_msg(symbol: str, indicators: Dict[str, Optional[float]]) -> str:
    lines = [f"📊 <b>{html.escape(symbol)}</b> Техник индикаторлар:"]
    for k in ("LAST", "SMA_10", "SMA_50", "EMA_10", "EMA_50", "MACD", "MACD_SIGNAL", "RSI"):
        v = indicators.get(k)
        if v is None:
            continue
        if k in ("RSI",):
            lines.append(f"{k}: {v:.2f}")
        else:
            lines.append(f"{k}: {v:.4f}" if isinstance(v, float) and abs(v) >= 0.0001 else f"{k}: {v}")
    return "\n".join(lines)

# ================= SIGNAL DETECTION =================
def detect_signals(symbol: str, df: pd.DataFrame, indicators: Dict[str, Optional[float]]) -> List[str]:
    """
    Generate human-readable signal messages for a ticker.
    Simple signals implemented:
      - SMA10 cross above/below SMA50 (if SMA50 exists)
      - EMA10 cross above/below EMA50 (if EMA50 exists)
      - MACD crossing its signal line
      - RSI overbought/oversold
    """
    signals: List[str] = []
    close = df["Close"].dropna()
    if len(close) < 2:
        return signals

    # Helper to detect cross based on moving series
    def cross(series_a: pd.Series, series_b: pd.Series) -> Optional[str]:
        if len(series_a) < 2 or len(series_b) < 2:
            return None
        a_prev, a_now = series_a.iloc[-2],(a_now) or pd.isna(b_prev) or pd.isna(b_now):
            return None
        if a_prev <= b_prev and a_now > b_now:
            return "bull_cross"
        if a_prev >= b_prev and a_now < b_now:
            return "bear_cross"
        return None

).mean()
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

    # MACD cross signal
    macd = (close.ewm(span=12()ewm(span=26, adjust=False).mean())
    macd_signal = macd.ewm(span=9, adjust=False).mean()
    res = cross(macd, macd_signal)
    if res == "bull_cross":
        signals.append("MACD crossed above its signal line — bullish momentum")
    elif res == "bear_cross":
        signals.append("MACD crossed below its signal line — bearish momentum")

    # RSI thresholds
    rsi = indicators.get("RSI")
    if rsi is not None:
        if rsi >= cfg.rsi_overbought:
            signals.append(f"RSI {rsi:.1f} — overbought")
        elif rsi <= cfg.rsi_oversold:
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
    {"name": "Reddit", "url": "https://www.reddit.com/r/CryptoCurrency/new/.json?limit={limit}".format(limit=cfg.reddit_limit)},
    # Add other sources here as needed
]

def fetch_social_discussions() -> List[Dict[str, Any]]:
    discussions: List[Dict[str, Any]] = []
    for src in SOCIAL_SOURCES:
        try:
            r = SESSION.get(src["url"], timeout=10)
            r.raise_for_status()
            j = r.json()
            # Reddit structure: data -> children -> data
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
    logger.info("Checking tickers: %s", ", ".join(tickers))

    for symbol in tickers:
        try:
            df = fetch_price_data(symbol)
            if df is None:
                logger.debug("No data for %s, skipping", symbol)
                continue
            indicators = calculate_indicators(df)
            signals = detect_signals(symbol, df, indicators)
            if signals:
                msg = build_signal_message(symbol, indicators, signals)
                if send_telegram(msg):
                    logger.info("Sent signal for %s: %s", symbol, signals)
                else:
                    logger.warning("Failed to send signal message for %s", symbol)
            elif cfg.send_all_indicators:
                msg = format_indicator_msg(symbol, indicators)
                send_telegram(msg)
            # Sleep a little to avoid API bursts
            time.sleep(0.5)
        except Exception as e:
            logger.exception("Error processing %s: %s", symbol, e)

    # Social discussions
    posts = fetch_social_discussions()
    for p in posts:
        key = p.get("url") or p.get("title")
        if not key:
            continue
_atomic(cfg.seen_file, seen)
            logger.info("Sent social post from %s: %s", p.get("source"), p.get("url"))
        time.sleep(0.5)

def main() -> None:
    logger.info("Dynamic Bot starting")
    seen = load_seen(cfg.seen_file)
    try:
        while True:
            start = time.time()
            run_once(seen)
            elapsed = time.time() - start
            sleep_for = max(0.0, cfg.poll_interval - elapsed)
            logger.debug("Sleeping for %.1f seconds", sleep_for)
            time.sleep(sleep_for)
    except KeyboardInterrupt:
        logger.info("Interrupted by user, exiting")
    except Exception as e:
        logger.exception("Fatal error: %s", e)
    finally:
        # Persist seen before exit
        try:
            save_seen_atomic(cfg.seen_file, seen)
        except Exception:
            pass
        logger.info("Dynamic Bot stopped")

if __name__ == "__main__":
    main()
