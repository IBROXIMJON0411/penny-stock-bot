import os
import json
import time
import requests
import websocket
from collections import defaultdict, deque
from datetime import datetime

POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

if not POLYGON_API_KEY:
    print("POLYGON_API_KEY missing!")
    exit()

if not TELEGRAM_TOKEN:
    print("TELEGRAM_TOKEN missing!")
    exit()

if not TELEGRAM_CHAT_ID:
    print("TELEGRAM_CHAT_ID missing!")
    exit()
# ===== SETTINGS =====
MIN_PRICE = 0.20
MAX_PRICE = 10
FAST_MULTIPLIER = 5
SLOW_MULTIPLIER = 3
COOLDOWN = 1800  # 30 min

data_store = defaultdict(lambda: {
    "closes": deque(maxlen=30),
    "volumes": deque(maxlen=30),
    "last_volume_sent": 0,
    "last_alert_time": 0
})

def ema(values, period):
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    ema_val = values[0]
    for price in list(values)[1:]:
        ema_val = price * k + ema_val * (1 - k)
    return ema_val

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    requests.post(url, json=payload)

def process_bar(bar):
    symbol = bar["sym"]
    close_price = bar["c"]
    volume = bar["v"]
    open_price = bar["o"]

    if close_price < MIN_PRICE or close_price > MAX_PRICE:
        return

    store = data_store[symbol]
    store["closes"].append(close_price)
    store["volumes"].append(volume)

    if len(store["closes"]) < 21:
        return

    if close_price <= open_price:
        return

    ema9 = ema(store["closes"], 9)
    ema21 = ema(store["closes"], 21)

    if not ema9 or not ema21:
        return

    if ema9 <= ema21:
        return

    # FAST 1m
    fast_condition = False
    if len(store["volumes"]) >= 10:
        avg_1m = sum(list(store["volumes"])[-10:-1]) / 9
        fast_condition = volume >= avg_1m * FAST_MULTIPLIER

    # SLOW 5m
    slow_condition = False
    if len(store["volumes"]) >= 15:
        last_5 = sum(list(store["volumes"])[-5:])
        prev_5 = sum(list(store["volumes"])[-10:-5])
        if prev_5 > 0:
            slow_condition = last_5 >= prev_5 * SLOW_MULTIPLIER

    if not (fast_condition or slow_condition):
        return

    now = time.time()

    if now - store["last_alert_time"] < COOLDOWN:
        return

    if volume == store["last_volume_sent"]:
        return

    store["last_volume_sent"] = volume
    store["last_alert_time"] = now

    signal_type = "FAST рџљЂ" if fast_condition else "SLOW рџ“€"

    message = (
        f"<b>{signal_type} SIGNAL</b>\n\n"
        f"Symbol: <b>{symbol}</b>\n"
        f"Price: ${close_price:.2f}\n"
        f"Volume: {volume:,}\n"
        f"EMA9: {ema9:.2f}\n"
        f"EMA21: {ema21:.2f}\n"
        f"Time: {datetime.now().strftime('%H:%M:%S')}"
    )

    send_telegram(message)
    print(f"Signal sent: {symbol}")

def on_message(ws, message):
    data = json.loads(message)
    for item in data:
        if item.get("ev") == "AM":
            process_bar(item)

def on_open(ws):
    print("Connected to Polygon")

    ws.send(json.dumps({
        "action": "auth",
        "params": POLYGON_API_KEY
    }))

    ws.send(json.dumps({
        "action": "subscribe",
        "params": "AM.*"
    }))

def on_error(ws, error):
    print("Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("Closed. Reconnecting...")
    time.sleep(5)
    start()

def start():
    ws = websocket.WebSocketApp(
        "wss://socket.polygon.io/stocks",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()

if __name__ == "__main__":
    start()

