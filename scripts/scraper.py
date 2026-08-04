import argparse
import csv
import html
import json
import os
import re
import shutil
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
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
PARQUET_BATCH_SIZE = 5000

# Residential-only property types (drop "land")
RESIDENTIAL_TYPES = ["mobile", "mfh_5_plus", "mfh_2_to_4", "condo", "sfr"]
RESIDENTIAL_TYPES_UPPER = {t.upper() for t in RESIDENTIAL_TYPES}

# Parquet has no primary-key constraint, so property_id is the first column and
# is deduplicated before writing. entry_json preserves every API field losslessly.
PARQUET_SCHEMA = pa.schema(
    [
        pa.field("property_id", pa.int64(), nullable=False),
        pa.field("address", pa.string()),
        pa.field("city", pa.string()),
        pa.field("state", pa.string()),
        pa.field("zip", pa.string()),
        pa.field("entry_json", pa.string(), nullable=False),
    ]
)

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


# gets one page of complete property entries
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


def as_string(value):
    if value is None:
        return None
    return str(value)


# converts complete API entries into rows matching PARQUET_SCHEMA
def extract_property_rows(batch):
    rows = []
    for item in batch:
        if not isinstance(item, dict):
            continue
        # belt-and-braces: never let a non-residential row into the Parquet
        # file, even if the API returns one outside the property_type filter
        ptype = str(item.get("property_type") or "").upper()
        if ptype not in RESIDENTIAL_TYPES_UPPER:
            continue
        raw_property_id = item.get("id")
        if isinstance(raw_property_id, bool):
            continue
        try:
            property_id = int(raw_property_id)
        except (TypeError, ValueError):
            continue

        address = item.get("address") or {}
        if not isinstance(address, dict):
            address = {}
        rows.append(
            {
                "property_id": property_id,
                "address": as_string(address.get("address")),
                "city": as_string(address.get("city")),
                "state": as_string(address.get("state")),
                "zip": as_string(address.get("zip")),
                "entry_json": json.dumps(item, ensure_ascii=False, separators=(",", ":")),
            }
        )
    return rows


def rows_to_batch(rows):
    arrays = [
        pa.array([row[field.name] for row in rows], type=field.type)
        for field in PARQUET_SCHEMA
    ]
    return pa.record_batch(arrays, schema=PARQUET_SCHEMA)


# writes one completed zip atomically; an interrupted .tmp file is never used
def write_zip_part(rows, part_path):
    part_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = part_path.with_suffix(".tmp")
    try:
        with pq.ParquetWriter(tmp_path, PARQUET_SCHEMA, compression="zstd") as writer:
            for start in range(0, len(rows), PARQUET_BATCH_SIZE):
                writer.write_batch(rows_to_batch(rows[start : start + PARQUET_BATCH_SIZE]))
        os.replace(tmp_path, part_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


# Parquet files cannot be appended to safely, so completed zip parts are
# compacted into the requested single state file at the end of the run.
def compact_parts(parts_dir, output_path):
    tmp_path = output_path.with_name(output_path.name + ".tmp")
    total_rows = 0
    try:
        with pq.ParquetWriter(tmp_path, PARQUET_SCHEMA, compression="zstd") as writer:
            for part_path in sorted(parts_dir.glob("*.parquet")):
                parquet_file = pq.ParquetFile(part_path)
                for batch in parquet_file.iter_batches(batch_size=PARQUET_BATCH_SIZE):
                    writer.write_batch(batch)
                    total_rows += batch.num_rows
        os.replace(tmp_path, output_path)
        return total_rows
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def format_duration(seconds):
    seconds = max(0, int(seconds))
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    if minutes:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


def progress_bar(completed, total, width=30):
    filled = int(width * completed / total) if total else 0
    return "[" + "█" * filled + "-" * (width - filled) + "]"


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


def load_zips_done(progress_path):
    zips_done = set()
    if progress_path.exists():
        with progress_path.open(encoding="utf-8") as f:
            for line in f:
                zip_code = line.strip().split("\t")[0]
                if zip_code:
                    zips_done.add(zip_code)
    return zips_done


def state_paths(state):
    state_lower = state.lower()
    return (
        Path(f"{state_lower}_residential_properties.parquet"),
        Path(f".{state_lower}_property_parts"),
        Path(f"{state_lower}_properties_zips_done.txt"),
    )


def crawl_state(state, delay=1.0, max_zips=None):
    output_path, parts_dir, progress_path = state_paths(state)
    zips_done = load_zips_done(progress_path)
    zips = load_zips(state)
    todo = [z for z in zips if z not in zips_done]
    if max_zips:
        todo = todo[:max_zips]
    print(f"{state}: {len(zips)} zips total, {len(zips_done)} done, {len(todo)} to fetch")

    total_rows = 0
    planned_total = len(zips_done) + len(todo)
    completed = len(zips_done)
    started_at = time.monotonic()

    with progress_path.open("a", encoding="utf-8") as prog_f:
        for zip_code in todo:
            zip_rows = []
            seen = set()
            try:
                for _, _, data in iter_zip_pages(state, zip_code, delay):
                    for row in extract_property_rows(data.get("response", [])):
                        property_id = row["property_id"]
                        if property_id in seen:
                            continue
                        seen.add(property_id)
                        zip_rows.append(row)
            except RuntimeError as e:
                # no part or progress is written for this zip, so the next run
                # safely refetches it from the beginning
                print(f"  zip {zip_code}: FAILED ({e}); will retry next run")
                continue

            write_zip_part(zip_rows, parts_dir / f"{zip_code}.parquet")
            prog_f.write(f"{zip_code}\n")
            prog_f.flush()
            total_rows += len(zip_rows)
            completed += 1
            done_this_run = completed - len(zips_done)
            elapsed = time.monotonic() - started_at
            eta = elapsed / done_this_run * (planned_total - completed)
            print(
                f"{progress_bar(completed, planned_total)} "
                f"{100 * completed / planned_total:5.1f}% ({completed}/{planned_total} zips) "
                f"| zip {zip_code}: {len(zip_rows)} rows | session total {total_rows:,} "
                f"| ETA {format_duration(eta)}"
            )

    part_files = list(parts_dir.glob("*.parquet"))
    should_compact = part_files and (todo or max_zips is not None or not output_path.exists())
    if should_compact:
        compacted_rows = compact_parts(parts_dir, output_path)
        print(f"{state}: compacted {compacted_rows:,} rows -> {output_path}")
        if max_zips is None:
            shutil.rmtree(parts_dir)
    elif max_zips is None and parts_dir.exists():
        # A prior full compaction succeeded but cleanup was interrupted. Do not
        # rebuild the final file from any partially deleted parts directory.
        shutil.rmtree(parts_dir)
    elif not output_path.exists() and not todo:
        print(f"{state}: no property data found; no Parquet file written")

    return total_rows


# runs everything
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crawl Propwire residential properties by zip.")
    parser.add_argument("states", nargs="*", default=["TX", "FL"], help="state codes (default: TX)")
    parser.add_argument("--max-zips", type=int, default=None, help="limit zips per state (for testing)")
    parser.add_argument("--delay", type=float, default=1.0, help="seconds between API calls")
    args = parser.parse_args()

    new_session()

    try:
        totals = {}
        for state in args.states:
            totals[state] = crawl_state(state.upper(), delay=args.delay, max_zips=args.max_zips)
    except KeyboardInterrupt:
        print("\ninterrupted - completed zips are saved; rerun to resume where it left off")
        raise SystemExit(130)

    for state, n in totals.items():
        print(f"{state}: {n:,} rows this run")
    print(f"Total: {sum(totals.values()):,} rows this run")
