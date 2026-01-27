#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import logging
import html
import asyncio
from collections import defaultdict
from statistics import mean
from typing import Dict, List, Set
from urllib.parse import quote_plus

import requests
import websockets

# ================ CONFIG ================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

VOLUME_MULTIPLIER = float(os.getenv("VOLUME_MULTIPLIER", "5"))   # spike threshold
PRICE_MIN = float(os.getenv("PRICE_MIN", "0.2"))
PRICE_MAX = float(os.getenv("PRICE_MAX", "10"))
AVERAGE_WINDOW = int(os.getenv("AVERAGE_WINDOW", "5"))          # how many recent trades to average
DEDUPE_SECONDS = int(os.getenv("DEDUPE_SECONDS", "300"))        # suppress same-ticker spikes for this many seconds
NEWS_LIMIT = int(os.getenv("NEWS_LIMIT", "3"))                  # how many news items to fetch
SOCKET_URL = os.getenv("POLYGON_SOCKET_URL", "wss://socket.polygon.io/stocks")

# ================ LOGGING ================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("volume_bot")

# ================ TELEGRAM ================
def send_telegram(msg: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram конфигурацияси йўқ; хabar юборилмади.")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
   text": msg, "parse_mode": "HTML", "disable_web_page_preview": False}
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        return True
    except Exception as e:
        logger.error("Telegram юбориш хатоси: %s", e)
        return False

# ================ NEWS (Polygon) ================
def fetch_news(ticker: str, limit: int = NEWS_LIMIT) -> List[Dict[str, str]]:
    if not POLYGON_API_KEY:
        return []
    q = quote_plus(ticker)
    url = f"https://api.polygon.io/v2/reference/news"
    params = {"query": q, "limit": limit, "apiKey": POLYGON_API_KEY}
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        items = data.get("results", []) or []
        news_list = []
        for it in items:
            title = it.get("title") or it.get("headline") or ""
            article_url = it.get("article_url") or it.get("url") or ""
            news_list.append({"title": title, "url": article_url})
        return news_list
    except Exception as e:
        logger.debug("Polygon news fetch error for %s: %s", ticker, e)
        return []

# ================ WEBSOCKET LOGIC ================
trade_history: Dict[str, List[float]] = defaultdict(list)  # ticker -> recent volumes
last_spike_time: Dict[str, float] = {}                     # ticker -> last spike epoch
seen_news_urls: Set[str] = set()

async def polygon_ws_loop():
    backoff = 1
    while True:
        try:
            logger.info("Connecting to Polygon WebSocket: %s", SOCKET_URL)
            async with websockets.connect(SOCKET_URL, ping_interval=20, ping_timeout=10) as ws:
                # auth
                auth_msg = {"action": "auth", "params": POLYGON_API_KEY}
                await ws.send(json.dumps(auth_msg))
                # subscribe to all trades (T.*). Adjust subscription to specific tickers if desired.
                sub_msg = {"action": "subscribe", "params": "T.*"}
                await ws.send(json.dumps(sub_msg))
                logger.info("Authenticated and subscribed to trades.")

                # Reset backoff on successful connection
                backoff = 1

                while True:
                    try:
                        raw = await ws.recv()
            except websockets.ConnectionClosed as e:
                        logger.warning("WebSocket closed: %s", e)
                        break
                    except Exception as e:
                        logger.error("WebSocket recv error: %s", e)
                        await asyncio.sleep(1)
                        continue

                    # Polygon may send newline-separated JSON or JSON array
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        # Not JSON -> skip
                        continue

                    # normalize to list
                    events = msg if isinstance(msg, list) else [msg]
                    for ev in events:
                        # handle status/auth responses quietly
                        if ev.get("ev") is None:
                            # possible ack like {"status":"connected"} or auth response -> ignore
                            continue
                        if ev.get("ev") != "T":
                            continue  # only trades

                        ticker = ev.get("sym")
                        price = ev.get("p", 0)
                        volume = ev.get("s", 0)

                        if not ticker:
                            continue
                        try:
                            price = float(price)
                            volume = float(volume)
                        except Exception:
                            continue

                        # price filter
                        if price < PRICE_MIN or price > PRICE_MAX:
                            continue

                        # update rolling history
                        vols = trade_history[ticker]
                        vols.append(volume)
                        if len(vols) > AVERAGE_WINDOW:
                            vols.pop(0)
                        avg_vol = mean(vols) if vols else 0.0

                        # detect spike
                        if avg_vol > 0 and volume >= avg_vol * VOLUME_MULTIPLIER:
                            now = time.time()
                            last = last_spike_time.get(ticker, 0)
                            if now - last >= DEDUPE_SECONDS:
                                last_spike_time[ticker] = now
                                # Spike detected -> notify
                                msg_text = (
                                    f"⚡ <b>{html.escape(ticker)}</b> — Volume spike detected!\n"
                                    f"Price: <code>{price:.6f}$</code>\n"
                                    f"Volume: <code>{int(volume)}</code>\n"
                                    f"Avg ({len(vols)}): <code>{avg_vol:.1f}</code>"
                                )
                                logger.info("Spike: %s", msg_text)
                                send_telegram(msg_text)

                                # fetch and send news (if any)
                                news_items = fetch_news(ticker)
                                for n in news_items:
                                    nurl = n.get("url") or ""
                                    if not nurl:
                                        continue
                                    if nurl in seen_news_urls:
                                        continue
                                    seen_news_urls.add(nurl)
                                    title = n.get("title") or ""
                                    news_msg = f"📰 <b>{html.escape(ticker)}</b>\n{html.escape(title)}\n{html.escape(nurl)}"
                                    send_telegram(news_msg)
        except Exception as e:
            logger.exception("WebSocket connection error: %s", e)
            logger.info("Reconnect backoff %s seconds...", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 300)  # exponential backoff up to 5 min
            def main():
    logger.info("Polygon Volume Spike Bot starting")
    try:
        asyncio.run(polygon_ws_loop())
    except KeyboardInterrupt:
        logger.info("Interrupted by user, exiting.")
    except Exception:
        logger.exception("Fatal error in main")

if __name__ == "__main__":
    main()
