import time
import re
import requests
import pandas as pd
import numpy as np
import yfinance as yf
from bs4 import BeautifulSoup
from datetime import datetime

# ================== CONFIG ==================
SCAN_INTERVAL = 300  # 5 минута
MIN_PRICE = 0.3
MAX_PRICE = 10

MIN_REDDIT_MENTIONS = 3
MIN_SCORE = 7

TELEGRAM_TOKEN = "PUT_YOUR_TOKEN"
TELEGRAM_CHAT_ID = "PUT_YOUR_CHAT_ID"

SUBREDDITS = [
    "pennystocks",
    "stocks",
    "wallstreetbets"
]

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# ================== TELEGRAM ==================
def send_telegram(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text
    }
    requests.post(url, data=data, timeout=10)

# ================== INDICATORS ==================
def rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def macd(series):
    ema12 = series.ewm(span=12).mean()
    ema26 = series.ewm(span=26).mean()
    macd_line = ema12 - ema26
    signal = macd_line.ewm(span=9).mean()
    return macd_line, signal

# ================== TECH ANALYSIS ==================
def technical_score(ticker):
    try:
        df = yf.download(ticker, period="3mo", interval="1d", progress=False)
        if df.empty:
            return 0, None

        price = df["Close"].iloc[-1]
        if price < MIN_PRICE or price > MAX_PRICE:
            return 0, None

        score = 0

        r = rsi(df["Close"]).iloc[-1]
        macd_line, macd_signal = macd(df["Close"])

        ema9 = df["Close"].ewm(span=9).mean().iloc[-1]
        ema21 = df["Close"].ewm(span=21).mean().iloc[-1]

        vol_now = df["Volume"].iloc[-1]
        vol_avg = df["Volume"].rolling(20).mean().iloc[-1]

        if r < 35:
            score += 2
        if macd_line.iloc[-1] > macd_signal.iloc[-1]:
            score += 2
        if ema9 > ema21:
            score += 2
        if vol_now > vol_avg * 2:
            score += 2

        return score, round(price, 2)

    except Exception:
        return 0, None

# ================== REDDIT SCAN ==================
def reddit_mentions():
    mentions = {}

    for sub in SUBREDDITS:
        url = f"https://www.reddit.com/r/{sub}/new/"
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        titles = soup.find_all("h3")
        for t in titles:
            words = re.findall(r"\b[A-Z]{2,5}\b", t.text)
            for w in words:
                mentions[w] = mentions.get(w, 0) + 1

    return mentions

# ================== MAIN LOOP ==================
def main():
    send_telegram("🚀 Scanner ишга тушди")

    while True:
        try:
            reddit = reddit_mentions()

            for ticker, count in reddit.items():
                if count < MIN_REDDIT_MENTIONS:
                    continue

                tech_score, price = technical_score(ticker)
                total_score = tech_score + min(count, 4)

                if total_score >= MIN_SCORE:
                    msg = (
                        f"🤑 SIGNAL\n"
                        f"Ticker: {ticker}\n"
                        f"Price: ${price}\n"
                        f"Tech score: {tech_score}\n"
                        f"Reddit mentions: {count}\n"
                        f"Total score: {total_score}\n"
                        f"Time: {datetime.utcnow()}"
                    )
                    send_telegram(msg)

            time.sleep(SCAN_INTERVAL)

        except Exception as e:
            time.sleep(60)

if __name__ == "__main__":
    main()
