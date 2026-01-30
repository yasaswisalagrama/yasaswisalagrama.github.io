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


def append_hourly_wide_csv(path, date_str, unit, price):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    rows = []
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

    row = next((r for r in rows if r["date"] == date_str), None)
    if not row:
        row = {"date": date_str, "unit": unit}
        rows.append(row)

    price_cols = [c for c in row if c.startswith("p_")]
    col_name = f"p_{len(price_cols) + 1:02d}"
    row[col_name] = price

    fieldnames = ["date", "unit"]
    max_p = max(
        [int(c[2:]) for r in rows for c in r if c.startswith("p_")] or [0]
    )
    for i in range(1, max_p + 1):
        fieldnames.append(f"p_{i:02d}")

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def upsert_daily_csv_json(csv_path, json_path, key_fields, new_row, unit):
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    data = []
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            data = []

    def normalize(val):
        if unit == "kg" and val < 10000:
            return val * 1000
        return val

    updated = False

    for row in data:
        if all(row.get(k) == new_row[k] for k in key_fields):
            if "close" not in row:
                if "price_per_kg_inr" in row:
                    row["close"] = row["price_per_kg_inr"]
                elif "price_per_gram_inr" in row:
                    row["close"] = normalize(row["price_per_gram_inr"])
                else:
                    continue

            row["close"] = normalize(row["close"])
            row.setdefault("open", row["close"])
            row.setdefault("high", row["close"])
            row.setdefault("low", row["close"])

            row["open"] = normalize(row["open"])
            row["high"] = normalize(row["high"])
            row["low"] = normalize(row["low"])

            row["high"] = max(row["high"], new_row["close"])
            row["low"] = min(row["low"], new_row["close"])
            row["close"] = new_row["close"]

            updated = True
            break

    if not updated:
        new_row["open"] = new_row["close"]
        new_row["high"] = new_row["close"]
        new_row["low"] = new_row["close"]
        data.append(new_row)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    fieldnames = []
    for r in data:
        for k in r:
            if k not in fieldnames:
                fieldnames.append(k)

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)

# ---------------- GOLD ----------------
def scrape_gold():
    url = "https://www.goldpricesindia.com/"
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()

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

    for purity, price in prices.items():
        append_hourly_wide_csv(
            f"data/hourly/gold_{purity.lower()}_hourly.csv",
            TODAY,
            "INR/gram",
            price
        )

        upsert_daily_csv_json(
            "data/gold.csv",
            "data/gold.json",
            ["date", "purity"],
            {
                "date": TODAY,
                "purity": purity,
                "close": price,
                "source": "GoldPricesIndia"
            },
            unit="gram"
        )

# ---------------- SILVER ----------------
def scrape_silver():
    url = "https://economictimes.indiatimes.com/commoditysummary/symbol-SILVER.cms"
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    price = clean_price(soup.find("span", class_="commodityPrice").text)

    append_hourly_wide_csv(
        "data/hourly/silver_hourly.csv",
        TODAY,
        "INR/kg",
        price
    )

    upsert_daily_csv_json(
        "data/silver.csv",
        "data/silver.json",
        ["date"],
        {
            "date": TODAY,
            "close": price,
            "source": "Economic Times MCX"
        },
        unit="kg"
    )

# ---------------- COPPER ----------------
def scrape_copper():
    url = "https://economictimes.indiatimes.com/commoditysummary/symbol-COPPER.cms"
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    price = clean_price(soup.find("span", class_="commodityPrice").text)

    append_hourly_wide_csv(
        "data/hourly/copper_hourly.csv",
        TODAY,
        "INR/kg",
        price
    )

    upsert_daily_csv_json(
        "data/copper.csv",
        "data/copper.json",
        ["date"],
        {
            "date": TODAY,
            "close": price,
            "source": "Economic Times MCX"
        },
        unit="kg"
    )

# ---------------- RUNNER ----------------
if __name__ == "__main__":
    for fn in (scrape_gold, scrape_silver, scrape_copper):
        try:
            fn()
            print(f"{fn.__name__}: OK")
        except Exception as e:
            print(f"{fn.__name__}: FAILED -> {e}")
