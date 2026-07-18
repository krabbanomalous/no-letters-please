import os
import re
import csv
import time
import requests
import normalize_address as na

from dotenv import load_dotenv

load_dotenv()

GEONODE_PROXY_USER = os.getenv("GEONODE_PROXY_USER")
GEONODE_PROXY_PASS = os.getenv("GEONODE_PROXY_PASS")
GEONODE_PROXY_HOST = os.getenv("GEONODE_PROXY_HOST")
GEONODE_PROXY_PORT = os.getenv("GEONODE_PROXY_PORT")

PROXIES = {
    "http": f"http://{GEONODE_PROXY_USER}:{GEONODE_PROXY_PASS}@{GEONODE_PROXY_HOST}:{GEONODE_PROXY_PORT}",
    "https": f"http://{GEONODE_PROXY_USER}:{GEONODE_PROXY_PASS}@{GEONODE_PROXY_HOST}:{GEONODE_PROXY_PORT}",
}

API_URL = "https://api.propwire.com/api/property_search"
PAGE_SIZE = 250

# Residential-only property types (drop "land")
RESIDENTIAL_TYPES = ["mobile", "mfh_5_plus", "mfh_2_to_4", "condo", "sfr"]

BASE_HEADERS = {
    "Content-Type": "application/json",
    "Origin": "https://propwire.com",
    "Referer": "https://propwire.com/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
}

# gets payload
def build_payload(state, state_name, result_index):
    return {
        "filters": {
            "locations": [
                {
                    "searchType": "T",
                    "state": state,
                    "title": f"{state_name}, USA",
                    "stateName": state_name,
                }
            ],
            "property_type": RESIDENTIAL_TYPES,
        },
        "size": PAGE_SIZE,
        "result_index": result_index,
        "house": True,
    }

# gets page of addresses
def fetch_page(session, state, state_name, result_index):
    payload = build_payload(state, state_name, result_index)
    response = session.post(
        API_URL,
        headers=BASE_HEADERS,
        json=payload,
        proxies=PROXIES,
        timeout=60,
    )
    response.raise_for_status()
    return response.json()

# gets rows of page
def extract_rows(data):
    rows = []
    for item in data.get("response", []):
        addr = item.get("address") or {}
        street = addr.get("address")
        if not street:
            continue
        rows.append(
            {
                "address" : na.normalize_address(street),
                "city"    : addr.get("city", ""),
                "state"   : addr.get("state", ""),
                "zip"     : addr.get("zip", "")
            }
        )
    return rows

def crawl_state(state, state_name, delay=1.0, max_pages=12500):
    session = requests.session()

    session.get(
        "https://propwire.com/",
        headers=BASE_HEADERS,
        proxies=PROXIES,
        timeout=60
    )

    all_rows = []
    seen = set()
    result_index = 0

    for _ in range(max_pages):
        print(f"{state} - result_index = {result_index}")
        data = fetch_page(session, state, state_name, result_index)
        batch = extract_rows(data)

        if not batch:
            break

        for row in batch:
            key = (row["address"], row["zip"])
            if key not in seen:
                seen.add(key)
                all_rows.append(row)
        
        if len(batch) < PAGE_SIZE:
            break

        result_index += PAGE_SIZE
        time.sleep(delay)

    return all_rows

# saves as CSV file
def save_csv(rows, filename):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["address", "city", "state"])
        writer.writeheader()
        for row in sorted(rows, key=lambda r: r["address"]):
            writer.writerow(row)

# runs everything
if __name__ == "__main__":
    tx_rows = crawl_state("TX", "Texas")
    fl_rows = crawl_state("FL", "Florida")
    
    save_csv(tx_rows, "tx_residential_addrs.csv")
    save_csv(fl_rows, "fl_residential_addrs.csv")

    print(f"Texas: {len(tx_rows):,}")
    print(f"Florida: {len(fl_rows):,}")
    print(f"Total: {len(tx_rows | fl_rows):,}")