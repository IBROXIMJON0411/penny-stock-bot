import requests
import os

API_KEY = os.getenv("POLYGON_API_KEY")

r = requests.get(
    "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2024-01-01/2024-01-10",
    params={"apiKey": API_KEY}
)

print(r.status_code)
print(r.text)
