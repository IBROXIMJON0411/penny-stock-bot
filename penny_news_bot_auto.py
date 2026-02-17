import requests
import os

API_KEY = os.getenv("POLYGON_API_KEY")

for t in ["AAPL", "TSLA", "NVDA", "MSFT"]:
    r = requests.get(
        f"https://api.polygon.io/v2/aggs/ticker/{t}/range/1/day/2024-01-01/2024-01-10",
        params={"apiKey": API_KEY}
    )
    print(t, r.status_code)
