import requests
from bs4 import BeautifulSoup
import re

URL = "https://economictimes.indiatimes.com/commoditysummary/symbol-SILVER.cms"
TIMEOUT = 10

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-IN,en;q=0.9"
}

def clean_price(text):
    return float(re.sub(r"[^\d.]", "", text))

print("Hitting MCX Silver (Economic Times)...")
r = requests.get(URL, headers=HEADERS, timeout=TIMEOUT)

print("HTTP Status:", r.status_code)
print("Payload size:", len(r.text))

if r.status_code != 200:
    raise Exception("Silver MCX page not reachable")

soup = BeautifulSoup(r.text, "html.parser")

# Economic Times uses these spans consistently for commodity price
price_tag = soup.find("span", class_="commodityPrice")

if not price_tag:
    raise Exception("❌ Silver MCX price not found in HTML")

price_kg = clean_price(price_tag.text)

print("Parsed MCX Silver price (₹/kg):", price_kg)

if price_kg <= 0:
    raise Exception("❌ Invalid silver price parsed")

print("✅ MCX Silver test PASSED")
