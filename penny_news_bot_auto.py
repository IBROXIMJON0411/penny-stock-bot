import asyncio
import websockets
import json
import requests
from collections import defaultdict
from datetime import datetime

# ================= CONFIG =================

POLYGON_API_KEY = "POLYGON_KEY"
TELEGRAM_BOT_TOKEN = "TELEGRAM_TOKEN"
TELEGRAM_CHAT_ID = "CHAT_ID"

PRICE_MIN = 0.20
PRICE_MAX = 10.00

VOLUME_SPIKE_MULTIPLIER = 3      # Average'dan 3x катта бўлса сигнал
RE_ALERT_MULTIPLIER = 1.5        # Аввалги сигналдан 1.5x катта бўлса қайта юбор

# ==========================================

volume_data = defaultdict(lambda: {
    "current_volume": 0,
    "last_minute": None,
    "avg_volume": 0,
    "last_alert_volume": 0
})

# ================= TELEGRAM =================

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    }
    requests.post(url, data=payload)

# ================= SIGNAL LOGIC =================

def process_trade(symbol, price, size):

    if not (PRICE_MIN <= price <= PRICE_MAX):
        return

    now_minute = datetime.utcnow().replace(second=0, microsecond=0)
    data = volume_data[symbol]

    # Янги минут бошланса reset
    if data["last_minute"] != now_minute:
        if data["current_volume"] > 0:
            if data["avg_volume"] == 0:
                data["avg_volume"] = data["current_volume"]
            else:
                data["avg_volume"] = (data["avg_volume"] + data["current_volume"]) / 2

        data["current_volume"] = 0
        data["last_minute"] = now_minute

    data["current_volume"] += size

    # Average текшириш
    if data["avg_volume"] == 0:
        return

    spike_ratio = data["current_volume"] / data["avg_volume"]

    # Фақат ўсувчан томон
    if spike_ratio >= VOLUME_SPIKE_MULTIPLIER:

        # Биринчи сигнал
        if data["last_alert_volume"] == 0:
            send_signal(symbol, price, data["current_volume"], spike_ratio)
            data["last_alert_volume"] = data["current_volume"]

        # Қайта сигнал (яна кучли ошса)
        elif data["current_volume"] >= data["last_alert_volume"] * RE_ALERT_MULTIPLIER:
            send_signal(symbol, price, data["current_volume"], spike_ratio)
            data["last_alert_volume"] = data["current_volume"]

def send_signal(symbol, price, volume, ratio):

    message = (
        f"🚀 VOLUME SPIKE ALERT\n\n"
        f"Symbol: {symbol}\n"
        f"Price: ${price:.2f}\n"
        f"Volume: {int(volume)}\n"
        f"Spike: {ratio:.2f}x\n"
        f"Time: {datetime.utcnow().strftime('%H:%M:%S')} UTC"
    )

    print(message)
    send_telegram(message)

# ================= POLYGON WS =================

async def main():

    uri = "wss://socket.polygon.io/stocks"

    async with websockets.connect(uri) as websocket:

        # Auth
        await websocket.send(json.dumps({
            "action": "auth",
            "params": POLYGON_API_KEY
        }))

        # Subscribe all trades
        await websocket.send(json.dumps({
            "action": "subscribe",
            "params": "T.*"
        }))

        print("✅ Connected to Polygon")

        while True:
            msg = await websocket.recv()
            data = json.loads(msg)

            for trade in data:
                if trade["ev"] == "T":
                    symbol = trade["sym"]
                    price = trade["p"]
                    size = trade["s"]

                    process_trade(symbol, price, size)

asyncio.run(main())
