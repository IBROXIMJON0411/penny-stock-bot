import requests, time, os
from datetime import datetime, timedelta
import pytz

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

BASE = "https://api.polygon.io"
ET = pytz.timezone("US/Eastern")

PRICE_MIN = 0.2
PRICE_MAX = 10.0

COOLDOWN_MIN = 30
last_sent = {}   # ticker -> time

def tg(msg):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": CHAT_ID, "text": msg})

def market_open():
    now = datetime.now(ET)
    return now.weekday() < 5 and 4 <= now.hour <= 20

def get_tickers():
    r = requests.get(
        f"{BASE}/v3/reference/tickers",
        params={"market":"stocks","active":"true","limit":200,"apiKey":POLYGON},
        timeout=20
    )
    return [x["ticker"] for x in r.json().get("results", [])]

def get_aggs(ticker, minutes):
    r = requests.get(
        f"{BASE}/v2/aggs/ticker/{ticker}/range/1/minute/now-{minutes}/minute/now",
        params={"adjusted":"true","apiKey":POLYGON},
        timeout=20
    )
    return r.json().get("results", [])

def cooldown_ok(t):
    if t not in last_sent:
        return True
    return datetime.utcnow() - last_sent[t] > timedelta(minutes=COOLDOWN_MIN)

def main():
    tg("🤖 Penny bot V3 started")
    tickers = get_tickers()
    tg(f"📌 Loaded tickers: {len(tickers)}")

    debug_timer = time.time()
    alive_timer = time.time()

    while True:
        try:
            if not market_open():
                time.sleep(60)
                continue

            checked = price_ok = vol_ok = signals = 0

            for t in tickers[:120]:
                checked += 1
                if not cooldown_ok(t):
                    continue

                aggs = get_aggs(t, 10)
                if len(aggs) < 6:
                    continue

                last = aggs[-1]
                price = last["c"]

                if not (PRICE_MIN <= price <= PRICE_MAX):
                    continue
                price_ok += 1

                # trend check (5m)
                five = aggs[-6:]
                if five[-1]["c"] <= five[0]["o"]:
                    continue

                # FAST
                avg_vol = sum(x["v"] for x in aggs[-6:-1]) / 5
                if avg_vol > 0:
                    spike = last["v"] / avg_vol
                else:
                    spike = 0

                change_1m = (last["c"] - last["o"]) / last["o"] * 100

                if spike >= 2 and change_1m >= 0.3:
                    vol_ok += 1
                    tg(
                        f"🚀 FAST\n"
                        f"{t}\n"
                        f"💲 {price:.2f}$\n"
                        f"📊 Vol x{spike:.1f}\n"
                        f"📈 +{change_1m:.2f}% (1m)"
                    )
                    last_sent[t] = datetime.utcnow()
                    signals += 1
                    continue

                # SLOW
                five_vol = sum(x["v"] for x in five[:-1]) / 5
                if five_vol > 0:
                    spike5 = five[-1]["v"] / five_vol
                else:
                    spike5 = 0

                change_5m = (five[-1]["c"] - five[0]["o"]) / five[0]["o"] * 100

                if spike5 >= 1.5 and change_5m >= 1:
                    vol_ok += 1
                    tg(
                        f"🐢 SLOW\n"
                        f"{t}\n"
                        f"💲 {price:.2f}$\n"
                        f"📊 Vol x{spike5:.1f}\n"
                        f"📈 +{change_5m:.2f}% (5m)"
                    )
                    last_sent[t] = datetime.utcnow()
                    signals += 1

            # DEBUG ҳар 10 мин
            if time.time() - debug_timer > 600:
                tg(
                    f"🧪 DEBUG\n"
                    f"Checked: {checked}\n"
                    f"Price OK: {price_ok}\n"
                    f"Volume OK: {vol_ok}\n"
                    f"Signals: {signals}"
                )
                debug_timer = time.time()

Иброхимжон, [10.02.2026 20:37]
# ALIVE ҳар 1 соат
            if time.time() - alive_timer > 3600:
                tg(f"🤖 Bot alive | {datetime.now(ET).strftime('%H:%M ET')}")
                alive_timer = time.time()

            time.sleep(60)

        except Exception as e:
            tg(f"❌ ERROR: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main()
