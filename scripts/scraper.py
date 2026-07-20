import argparse
import csv
import html
import json
import os
import re
import time

import normalize_address as na

from dotenv import load_dotenv
from curl_cffi import requests as creq

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
# Public key the propwire.com frontend itself sends on every search request.
API_KEY = "b697fa27-fe7a-4f86-b3d8-024d57804125"

# Any Inertia page embeds a short-lived guest JWT in its data-page JSON.
TOKEN_PAGE_URL = (
    "https://propwire.com/search?filters=%7B%22locations%22%3A%5B%7B%22searchType%22%3A%22T%22"
    "%2C%22state%22%3A%22TX%22%2C%22title%22%3A%22Texas%2C%20USA%22%2C%22stateName%22%3A%22Texas%22%7D%5D"
    "%2C%22property_type%22%3A%5B%22sfr%22%5D%7D"
)

ZIP_CSV = "us_zipcodes.csv"
ZIP_CSV_URL = "https://raw.githubusercontent.com/midwire/free_zipcode_data/master/all_us_zipcodes.csv"

PAGE_SIZE = 250
MAX_RESULT_INDEX = 10000          # API returns nothing past this offset, so we partition by zip
TOKEN_TTL_SECONDS = 90 * 60       # guest JWT lasts 2h; refresh early
TOKEN_ROTATE_EVERY = 50           # DataDome throttles per token; swap it out regularly
MAX_ATTEMPTS = 10                 # retries per request before giving up

# Residential-only property types (drop "land")
RESIDENTIAL_TYPES = ["mobile", "mfh_5_plus", "mfh_2_to_4", "condo", "sfr"]
RESIDENTIAL_TYPES_UPPER = {t.upper() for t in RESIDENTIAL_TYPES}

_session = None
_token = None
_token_at = 0.0
_request_count = 0


# curl_cffi impersonates Chrome's TLS/HTTP2 fingerprint, which is what gets us
# past the DataDome bot wall in front of propwire.com (plain requests is 403'd
# no matter which IP it comes from).
def new_session():
    global _session
    _session = creq.Session(impersonate="chrome", proxies=PROXIES)


def get_token(force=False):
    global _token, _token_at
    if not force and _token and time.time() - _token_at < TOKEN_TTL_SECONDS:
        return _token
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            r = _session.get(TOKEN_PAGE_URL, timeout=60)
            if r.status_code == 200:
                m = re.search(r'data-page="((?:[^"\\]|\\.)*)"', r.text)
                if m:
                    _token = json.loads(html.unescape(m.group(1)))["props"]["token"]
                    _token_at = time.time()
                    print("acquired fresh guest token")
                    return _token
            print(f"  token fetch attempt {attempt}: HTTP {r.status_code}")
        except Exception as e:
            print(f"  token fetch attempt {attempt}: {type(e).__name__}")
        # 403 = DataDome flagged this exit IP; new session -> new proxy exit
        new_session()
        time.sleep(min(2 * attempt, 20))
    raise RuntimeError("could not obtain a guest token from propwire.com")


def api_headers():
    return {
        "Content-Type": "application/json",
        "Origin": "https://propwire.com",
        "Referer": "https://propwire.com/",
        "Accept": "application/json, text/plain, */*",
        "Authorization": f"Bearer {get_token()}",
        "x-api-key": API_KEY,
    }


# gets page of addresses
def fetch_page(location, result_index, property_types=None):
    global _request_count
    payload = {
        "locations": [location],
        "property_type": property_types or RESIDENTIAL_TYPES,
        "size": PAGE_SIZE,
        "result_index": result_index,
        "house": True,
    }
    for attempt in range(1, MAX_ATTEMPTS + 1):
        if _request_count >= TOKEN_ROTATE_EVERY:
            new_session()
            get_token(force=True)
            _request_count = 0
        try:
            r = _session.post(API_URL, headers=api_headers(), json=payload, timeout=60)
            if r.status_code == 200:
                try:
                    data = r.json()
                except Exception:
                    data = None  # DataDome sometimes answers 200 with an HTML challenge
                if data is not None:
                    _request_count += 1
                    return data
                print(f"  fetch attempt {attempt}: 200 with non-JSON body (challenge)")
            elif r.status_code == 401:
                # token expired mid-crawl: grab a new one and retry
                get_token(force=True)
                continue
            else:
                print(f"  fetch attempt {attempt}: HTTP {r.status_code}")
        except Exception as e:
            print(f"  fetch attempt {attempt}: {type(e).__name__}")
        # Blocked: the token is what DataDome tracks across our rotating exit
        # IPs, so swap BOTH the session (new IP) and the token before retrying.
        new_session()
        get_token(force=True)
        time.sleep(min(2 * attempt, 20))
    raise RuntimeError(f"API request failed {MAX_ATTEMPTS} times for {location}")


# yields (query_key, result_index, data) for every page of a zip
def iter_zip_pages(state, zip_code, delay):
    location = {"state": state, "zip": zip_code}
    first = fetch_page(location, 0)
    total = first.get("result_count") or 0

    if total <= MAX_RESULT_INDEX:
        yield "ALL", 0, first
        result_index = PAGE_SIZE
        while result_index < total and result_index < MAX_RESULT_INDEX:
            time.sleep(delay)
            data = fetch_page(location, result_index)
            yield "ALL", result_index, data
            if len(data.get("response", [])) < PAGE_SIZE:
                break
            result_index += PAGE_SIZE
    else:
        # zip alone exceeds the 10k window: split into one query per property type
        print(f"  zip {zip_code}: {total:,} results, splitting by property type")
        for ptype in RESIDENTIAL_TYPES:
            result_index = 0
            while result_index < MAX_RESULT_INDEX:
                if result_index > 0 or ptype != RESIDENTIAL_TYPES[0]:
                    time.sleep(delay)
                data = fetch_page(location, result_index, [ptype])
                if result_index == 0:
                    sub_total = data.get("result_count") or 0
                    if sub_total > MAX_RESULT_INDEX:
                        print(
                            f"  WARNING: zip {zip_code} type {ptype} has {sub_total:,} "
                            f"results; only the first {MAX_RESULT_INDEX:,} can be fetched"
                        )
                yield ptype, result_index, data
                if len(data.get("response", [])) < PAGE_SIZE:
                    break
                result_index += PAGE_SIZE


# gets rows of page
def extract_rows(batch):
    rows = []
    for item in batch:
        if not isinstance(item, dict):
            continue
        # belt-and-braces: never let a non-residential row into the CSV, even
        # if the API returns one outside the requested property_type filter
        ptype = str(item.get("property_type") or "").upper()
        if ptype not in RESIDENTIAL_TYPES_UPPER:
            continue
        addr = item.get("address") or {}
        street = addr.get("address")
        if not street:
            continue
        rows.append(
            {
                "address": na.normalize_address(street),
                "city": addr.get("city", ""),
                "state": addr.get("state", ""),
                "zip": addr.get("zip", ""),
            }
        )
    return rows


def load_zips(state):
    if not os.path.exists(ZIP_CSV):
        print(f"downloading zip list -> {ZIP_CSV}")
        r = creq.get(ZIP_CSV_URL, timeout=120)
        r.raise_for_status()
        with open(ZIP_CSV, "w", encoding="utf-8", newline="") as f:
            f.write(r.text)
    zips = []
    with open(ZIP_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["state"] == state:
                zips.append(row["code"].zfill(5))
    return sorted(set(zips))


# Progress file lines are either "zip<TAB>query_key<TAB>result_index" for one
# written page or "zip<TAB>DONE" for a finished zip. Bare "zip" lines from the
# previous version also count as DONE.
def load_progress(progress_path):
    pages_done, zips_done = set(), set()
    if os.path.exists(progress_path):
        with open(progress_path, encoding="utf-8") as f:
            for line in f:
                parts = line.rstrip("\n").split("\t")
                if len(parts) == 1 and parts[0]:
                    zips_done.add(parts[0])
                elif len(parts) == 2 and parts[1] == "DONE":
                    zips_done.add(parts[0])
                elif len(parts) == 3:
                    pages_done.add((parts[0], parts[1], int(parts[2])))
    return pages_done, zips_done


def crawl_state(state, delay=1.0, max_zips=None):
    csv_path = f"{state.lower()}_residential_addrs.csv"
    progress_path = f"{state.lower()}_zips_done.txt"

    pages_done, zips_done = load_progress(progress_path)
    zips = load_zips(state)
    todo = [z for z in zips if z not in zips_done]
    if max_zips:
        todo = todo[:max_zips]
    print(f"{state}: {len(zips)} zips total, {len(zips_done)} done, {len(todo)} to fetch")

    new_file = not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0
    total_rows = 0

    with open(csv_path, "a", newline="", encoding="utf-8") as csv_f, open(
        progress_path, "a", encoding="utf-8"
    ) as prog_f:
        writer = csv.DictWriter(csv_f, fieldnames=["address", "city", "state", "zip"])
        if new_file:
            writer.writeheader()

        for i, zip_code in enumerate(todo, 1):
            zip_rows = 0
            seen = set()
            try:
                for query_key, result_index, data in iter_zip_pages(state, zip_code, delay):
                    if (zip_code, query_key, result_index) in pages_done:
                        continue  # written by a previous (interrupted) run
                    for row in extract_rows(data.get("response", [])):
                        key = (row["address"], row["zip"])
                        if key in seen:
                            continue
                        seen.add(key)
                        writer.writerow(row)
                        zip_rows += 1
                    csv_f.flush()
                    prog_f.write(f"{zip_code}\t{query_key}\t{result_index}\n")
                    prog_f.flush()
            except RuntimeError as e:
                # pages already written stay in the CSV; the next run resumes
                # this zip from its first unwritten page
                print(f"  zip {zip_code}: FAILED ({e}); will resume next run")
                continue
            prog_f.write(f"{zip_code}\tDONE\n")
            prog_f.flush()
            total_rows += zip_rows
            print(f"[{i}/{len(todo)}] zip {zip_code}: {zip_rows} rows | session total {total_rows:,}")

    return total_rows


# runs everything
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crawl Propwire residential addresses by zip.")
    parser.add_argument("states", nargs="*", default=["TX", "FL"], help="state codes (default: TX)")
    parser.add_argument("--max-zips", type=int, default=None, help="limit zips per state (for testing)")
    parser.add_argument("--delay", type=float, default=1.0, help="seconds between API calls")
    args = parser.parse_args()

    new_session()

    try:
        totals = {}
        for state in args.states:
            if state != "FL":
                totals[state] = crawl_state(state.upper(), delay=args.delay, max_zips=args.max_zips)
    except KeyboardInterrupt:
        print("\ninterrupted - progress is saved; rerun to resume where it left off")
        raise SystemExit(130)

    for state, n in totals.items():
        print(f"{state}: {n:,} rows this run")
    print(f"Total: {sum(totals.values()):,} rows this run")
