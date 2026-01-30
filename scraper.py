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


def upsert_daily_csv_json(csv_path, json_path, key_fields, new_row, unit="kg"):
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    # ---------- Load JSON ----------
    data = []
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            data = []

    def normalize_close(value):
        if unit == "kg":
            # if accidentally gram-level, convert to kg
            if value < 10000:   # ₹/kg silver is always >> 10k
                return value * 1000
        return value

    updated = False

    for row in data:
        if all(row.get(k) == new_row[k] for k in key_fields):

            # ---- LEGACY NORMALIZATION ----
            if "close" not in row:
                if "price_per_kg_inr" in row:
                    row["close"] = row["price_per_kg_inr"]
                elif "price_per_gram_inr" in row:
                    row["close"] = (
                        row["price_per_gram_inr"] * 1000
                        if unit == "kg"
                        else row["price_per_gram_inr"]
                    )
                else:
                    continue

            # enforce canonical unit
            row["close"] = normalize_close(row["close"])

            row.setdefault("open", row["close"])
            row.setdefault("high", row["close"])
            row.setdefault("low", row["close"])

            row["open"] = normalize_close(row["open"])
            row["high"] = normalize_close(row["high"])
            row["low"] = normalize_close(row["low"])

            # ---- UPDATE OHLC ----
            row["high"] = max(row["high"], new_row["close"])
            row["low"] = min(row["low"], new_row["close"])
            row["close"] = new_row["close"]

            updated = True
            break

    if not updated:
        new_row["close"] = normalize_close(new_row["close"])
        new_row["open"] = new_row["close"]
        new_row["high"] = new_row["close"]
        new_row["low"] = new_row["close"]
        data.append(new_row)

    # ---------- Save JSON ----------
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    # ---------- Rewrite CSV ----------
    fieldnames = []
    for r in data:
        for k in r.keys():
            if k not in fieldnames:
                fieldnames.append(k)

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)

# ---------------- GOLD (GoldPricesIndia) ----------------
def scrape_gold():
    url = "https://www.goldpricesindia.com/"
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    if r.status_code != 200:
        raise Exception(f"Gold HTTP {r.status_code}")

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find_all("table")[0]

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
            "close": price,
            "source": "GoldPricesIndia"
        }

        upsert_daily_csv_json(
            csv_path="data/gold.csv",
            json_path="data/gold.json",
            key_fields=["date", "purity"],
            new_row=row
        )


# ---------------- SILVER (MCX – Economic Times) ----------------
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

    row = {
        "date": TODAY,
        "close": price_kg,
        "source": "Economic Times MCX"
    }

    upsert_daily_csv_json(
        csv_path="data/silver.csv",
        json_path="data/silver.json",
        key_fields=["date"],
        new_row=row
    )


# ---------------- COPPER (MCX – Economic Times) ----------------
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
        "close": price,
        "source": "Economic Times MCX"
    }

    upsert_daily_csv_json(
        csv_path="data/copper.csv",
        json_path="data/copper.json",
        key_fields=["date"],
        new_row=row
    )


# ---------------- RUNNER ----------------
if __name__ == "__main__":
    for fn in (scrape_gold, scrape_silver, scrape_copper):
        try:
            fn()
            print(f"{fn.__name__}: OK")
        except Exception as e:
            print(f"{fn.__name__}: FAILED -> {e}")
