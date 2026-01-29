import requests
from bs4 import BeautifulSoup
import csv, json, os, re
from datetime import date

# ---------------- CONFIG ----------------
TODAY = date.today().isoformat()
TIMEOUT = 10

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-IN,en;q=0.9"
}

# ---------------- HELPERS ----------------
def clean_price(text):
    return float(re.sub(r"[^\d.]", "", text))

def append_csv(path, row):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if not exists:
            writer.writeheader()
        writer.writerow(row)

def append_json(path, row):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    data = []
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            data = []
    data.append(row)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

# ---------------- GOLD (GoldPricesIndia) ----------------
def scrape_gold():
    url = "https://www.goldpricesindia.com/"
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    if r.status_code != 200:
        raise Exception(f"Gold HTTP {r.status_code}")

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find_all("table")[0]  # verified earlier

    prices = {}

    for row in table.find_all("tr"):
        text = row.get_text(" ", strip=True)
        parts = text.split()

        if "24K" in text and "1" in parts and "g" in parts:
            prices["24K"] = clean_price(parts[-1])

        if "22K" in text and "1" in parts and "g" in parts:
            prices["22K"] = clean_price(parts[-1])

    if len(prices) != 2:
        raise Exception("Gold 1g prices not found")

    for purity, price in prices.items():
        row = {
            "date": TODAY,
            "purity": purity,
            "price_per_gram_inr": price,
            "source": "GoldPricesIndia"
        }
        append_csv("data/gold.csv", row)
        append_json("data/gold.json", row)

# ---------------- SILVER (MCX via Economic Times) ----------------
def scrape_silver():
    url = "https://economictimes.indiatimes.com/commoditysummary/symbol-SILVER.cms"
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    if r.status_code != 200:
        raise Exception(f"Silver HTTP {r.status_code}")

    soup = BeautifulSoup(r.text, "html.parser")
    price_tag = soup.find("span", class_="commodityPrice")

    if not price_tag:
        raise Exception("Silver price not found")

    price_kg = clean_price(price_tag.text)
    price_g = round(price_kg / 1000, 2)

    row = {
        "date": TODAY,
        "price_per_gram_inr": price_g,
        "price_per_kg_inr": price_kg,
        "source": "Economic Times MCX"
    }

    append_csv("data/silver.csv", row)
    append_json("data/silver.json", row)

# ---------------- COPPER (MCX via Economic Times) ----------------
def scrape_copper():
    url = "https://economictimes.indiatimes.com/commoditysummary/symbol-COPPER.cms"
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    if r.status_code != 200:
        raise Exception(f"Copper HTTP {r.status_code}")

    soup = BeautifulSoup(r.text, "html.parser")
    price_tag = soup.find("span", class_="commodityPrice")

    if not price_tag:
        raise Exception("Copper price not found")

    price = clean_price(price_tag.text)

    row = {
        "date": TODAY,
        "price_per_kg_inr": price,
        "source": "Economic Times MCX"
    }

    append_csv("data/copper.csv", row)
    append_json("data/copper.json", row)

# ---------------- RUNNER ----------------
if __name__ == "__main__":
    for fn in (scrape_gold, scrape_silver, scrape_copper):
        try:
            fn()
            print(f"{fn.__name__}: OK")
        except Exception as e:
            print(f"{fn.__name__}: FAILED -> {e}")
