import argparse
import csv
import html
import json
import os
import re
import sys
import time
import urllib.parse
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
# The site's own property-detail endpoint (a Laravel web route, not part of the
# api subdomain). Given a property id it returns ownership, transfer/sale
# history, current + historical mortgages, parcel, tax and MLS history.
DETAIL_URL = "https://propwire.com/pw_property_detail"
# Public key the propwire.com frontend itself sends on every search request.
API_KEY = "b697fa27-fe7a-4f86-b3d8-024d57804125"

# Any Inertia page embeds a short-lived guest JWT in its data-page JSON.
# Loading this page also sets the XSRF-TOKEN + session cookies that
# DETAIL_URL requires, so every token refresh doubles as a detail-session refresh.
TOKEN_PAGE_URL = (
    "https://propwire.com/search?filters=%7B%22locations%22%3A%5B%7B%22searchType%22%3A%22T%22"
    "%2C%22state%22%3A%22TX%22%2C%22title%22%3A%22Texas%2C%20USA%22%2C%22stateName%22%3A%22Texas%22%7D%5D"
    "%2C%22property_type%22%3A%5B%22sfr%22%5D%7D"
)

ZIP_CSV = "us_zipcodes.csv"
ZIP_CSV_URL = "https://raw.githubusercontent.com/midwire/free_zipcode_data/master/all_us_zipcodes.csv"

PAGE_SIZE = 250
MAX_RESULT_INDEX = 10000          # API returns nothing past this offset, so big queries are partitioned
TOKEN_TTL_SECONDS = 90 * 60       # guest JWT lasts 2h; refresh early
TOKEN_ROTATE_EVERY = 50           # DataDome throttles per token; swap it out regularly
MAX_ATTEMPTS = 10                 # retries per request before giving up
PARQUET_BATCH_SIZE = 5000
DETAIL_BATCH_SIZE = 500           # pw_property_detail accepts up to 500 ids per call (verified)

# Residential-only property types (drop "land")
RESIDENTIAL_TYPES = ["mobile", "mfh_5_plus", "mfh_2_to_4", "condo", "sfr"]
RESIDENTIAL_TYPES_UPPER = {t.upper() for t in RESIDENTIAL_TYPES}

# When a zip (+ property type) still exceeds the 10k result window, the query is
# recursively bisected on these range filters (verified against the live API;
# the frontend builds them in DevPropertySearchPayload). beds is a tiny integer
# range; estimated_value absorbs whatever beds cannot split. Sub-ranges overlap
# at the midpoint on purpose (inclusive bounds) - duplicates are removed by
# property_id, while a gap would silently lose properties.
SPLIT_DIMS = [("beds", 0, 99), ("estimated_value", 0, 10**9)]

# Only these sections of each pw_property_detail record are kept in
# detail_json; the rest of the record (equity estimates, foreclosure details,
# images, lead types, owner portfolio, property details, ...) is fetched but
# discarded to save disk space.
DETAIL_KEYS = (
    "owner_details",       # ownership
    "transfer_history",    # transfer/sales history
    "current_mortgages",   # mortgages
    "mortgage_history",    # mortgages
    "parcel_details",      # parcel
    "tax_details",         # tax
    "mls_history",         # MLS history
)

# Parquet has no primary-key constraint, so property_id is the first column and
# is deduplicated before writing. entry_json preserves every search-API field
# losslessly; detail_json keeps only the DETAIL_KEYS sections of each
# pw_property_detail record. It is nullable so rows from older runs
# (pre-detail) still merge cleanly.
PARQUET_SCHEMA = pa.schema(
    [
        pa.field("property_id", pa.int64(), nullable=False),
        pa.field("address", pa.string()),
        pa.field("city", pa.string()),
        pa.field("state", pa.string()),
        pa.field("zip", pa.string()),
        pa.field("entry_json", pa.string(), nullable=False),
        pa.field("detail_json", pa.string()),
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


def detail_headers():
    # DETAIL_URL is a Laravel web route: it wants the session cookies plus the
    # URL-decoded XSRF-TOKEN cookie echoed back in the X-XSRF-TOKEN header
    # (exactly what the site's own axios setup does). Loading TOKEN_PAGE_URL in
    # get_token() is what sets those cookies on the session.
    xsrf = _session.cookies.get("XSRF-TOKEN")
    if not xsrf:
        get_token()
        xsrf = _session.cookies.get("XSRF-TOKEN")
    return {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://propwire.com",
        "Referer": "https://propwire.com/search",
        "X-Requested-With": "XMLHttpRequest",
        "X-XSRF-TOKEN": urllib.parse.unquote(xsrf or ""),
    }


def _rotate_token_if_due():
    global _request_count
    if _request_count >= TOKEN_ROTATE_EVERY:
        new_session()
        get_token(force=True)
        _request_count = 0


# gets one page of complete property entries
def fetch_page(location, result_index, property_types=None, extra_filters=None):
    global _request_count
    payload = {
        "locations": [location],
        "property_type": property_types or RESIDENTIAL_TYPES,
        "size": PAGE_SIZE,
        "result_index": result_index,
        "house": True,
    }
    if extra_filters:
        payload.update(extra_filters)
    for attempt in range(1, MAX_ATTEMPTS + 1):
        _rotate_token_if_due()
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


# gets the full pw_property_detail records for a batch of properties in ONE
# request. The endpoint accepts {"ids": [...]} (verified live up to 500 ids,
# ~5s per call), which keeps DataDome request-volume throttling off our backs:
# a 1,000-property zip needs 2 detail requests, not 1,000. Returns {id: detail}.
# A batch that keeps failing is bisected down to single ids so one bad id
# cannot sink the rest; ids that still fail are simply absent from the result.
def fetch_detail_batch(property_ids):
    global _request_count
    for attempt in range(1, MAX_ATTEMPTS + 1):
        _rotate_token_if_due()
        try:
            r = _session.post(DETAIL_URL, headers=detail_headers(), json={"ids": property_ids}, timeout=120)
            if r.status_code == 200:
                try:
                    data = r.json()
                except Exception:
                    data = None
                if data is not None:
                    _request_count += 1
                    details = {}
                    for record in data.get("response") or []:
                        if isinstance(record, dict) and record.get("id") is not None:
                            details[record["id"]] = record
                    return details
                print(f"  detail attempt {attempt}: 200 with non-JSON body (challenge)")
            else:
                # 419 = CSRF session went stale, 403 = DataDome; both are fixed
                # by a fresh session + page load below
                print(f"  detail attempt {attempt}: HTTP {r.status_code}")
        except Exception as e:
            print(f"  detail attempt {attempt}: {type(e).__name__}")
        new_session()
        get_token(force=True)
        time.sleep(min(2 * attempt, 20))
    if len(property_ids) <= 1:
        # A missing detail record must not fail the whole zip: the row is kept
        # with a null detail_json and the shortfall is reported by the caller.
        print(f"  WARNING: detail lookup failed {MAX_ATTEMPTS} times for property {property_ids[0]}")
        return {}
    mid = len(property_ids) // 2
    print(f"  detail batch of {len(property_ids)} failed {MAX_ATTEMPTS} times; splitting it")
    details = fetch_detail_batch(property_ids[:mid])
    details.update(fetch_detail_batch(property_ids[mid:]))
    return details


def page_results(first, location, property_types, extra_filters, delay):
    """Yields page dicts for a query whose first page is already fetched."""
    total = first.get("result_count") or 0
    yield first
    result_index = PAGE_SIZE
    while result_index < total and result_index < MAX_RESULT_INDEX:
        time.sleep(delay)
        data = fetch_page(location, result_index, property_types, extra_filters)
        yield data
        if len(data.get("response", [])) < PAGE_SIZE:
            break
        result_index += PAGE_SIZE


def iter_partition(location, ptype, delay, extra_filters, dim, lo, hi):
    """Yields every page of a zip/property-type query, recursively bisecting
    SPLIT_DIMS range filters until each sub-query fits inside the 10k window."""
    first = fetch_page(location, 0, [ptype], extra_filters)
    total = first.get("result_count") or 0

    if total > MAX_RESULT_INDEX and dim < len(SPLIT_DIMS):
        field = SPLIT_DIMS[dim][0]
        if lo < hi:
            # split this dimension's range in two; the midpoint is shared on
            # purpose (see SPLIT_DIMS comment)
            if hi - lo <= 1:
                children = ((lo, lo), (hi, hi))
            else:
                mid = (lo + hi) // 2
                children = ((lo, mid), (mid, hi))
            print(f"  {location.get('zip')} {ptype}: {total:,} results, splitting {field} {lo}-{hi}")
            yield first  # still valid data; overlaps are deduplicated downstream
            for sub_lo, sub_hi in children:
                sub_filters = dict(extra_filters)
                sub_filters[field] = {"min": sub_lo, "max": sub_hi}
                time.sleep(delay)
                yield from iter_partition(location, ptype, delay, sub_filters, dim, sub_lo, sub_hi)
            return
        # a single exact value of this dimension still overflows: pin it and
        # escalate to the next dimension
        if dim + 1 < len(SPLIT_DIMS):
            _, next_lo, next_hi = SPLIT_DIMS[dim + 1]
            yield first
            time.sleep(delay)
            yield from iter_partition(location, ptype, delay, extra_filters, dim + 1, next_lo, next_hi)
            return

    if total > MAX_RESULT_INDEX:
        print(
            f"  WARNING: {location.get('zip')} {ptype} {extra_filters} has {total:,} results; "
            f"only the first {MAX_RESULT_INDEX:,} can be fetched"
        )
    yield from page_results(first, location, [ptype], extra_filters, delay)


# yields every search page of a zip, partitioning past the 10k window as needed
def iter_zip_pages(state, zip_code, delay):
    location = {"state": state, "zip": zip_code}
    first = fetch_page(location, 0)
    total = first.get("result_count") or 0

    if total <= MAX_RESULT_INDEX:
        yield from page_results(first, location, RESIDENTIAL_TYPES, None, delay)
    else:
        # zip alone exceeds the 10k window: split by property type, then let
        # iter_partition bisect further on range filters where needed
        print(f"  zip {zip_code}: {total:,} results, splitting by property type")
        yield first
        for ptype in RESIDENTIAL_TYPES:
            time.sleep(delay)
            dim_lo, dim_hi = SPLIT_DIMS[0][1], SPLIT_DIMS[0][2]
            yield from iter_partition(location, ptype, delay, {}, 0, dim_lo, dim_hi)


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
                "detail_json": None,
            }
        )
    return rows


# collects a whole zip: every partition page, deduplicated, plus detail records
def collect_zip_rows(state, zip_code, delay, with_details=True):
    rows = []
    seen = set()
    baseline = None
    for data in iter_zip_pages(state, zip_code, delay):
        if baseline is None:
            # the first page of a zip is always the unfiltered all-types query,
            # so its result_count is the zip's true residential total
            baseline = data.get("result_count") or 0
        for row in extract_property_rows(data.get("response", [])):
            property_id = row["property_id"]
            if property_id in seen:
                continue
            seen.add(property_id)
            rows.append(row)

    if baseline and len(rows) < baseline:
        print(
            f"  zip {zip_code}: coverage {len(rows):,}/{baseline:,} "
            f"({baseline - len(rows):,} properties unreachable past the 10k window)"
        )

    if with_details:
        failures = 0
        for start in range(0, len(rows), DETAIL_BATCH_SIZE):
            if start:
                time.sleep(delay)
            chunk = rows[start : start + DETAIL_BATCH_SIZE]
            details = fetch_detail_batch([row["property_id"] for row in chunk])
            for row in chunk:
                detail = details.get(row["property_id"])
                if detail is None:
                    failures += 1
                else:
                    trimmed = {key: detail.get(key) for key in DETAIL_KEYS}
                    row["detail_json"] = json.dumps(trimmed, ensure_ascii=False, separators=(",", ":"))
        if failures:
            print(f"  zip {zip_code}: {failures:,}/{len(rows):,} detail lookups failed (detail_json left null)")

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


# Older parquet files have no detail_json column; add it (null) so every batch
# matches PARQUET_SCHEMA before it goes into the merged state file.
def align_batch(batch):
    arrays = []
    for field in PARQUET_SCHEMA:
        index = batch.schema.get_field_index(field.name)
        if index == -1:
            arrays.append(pa.nulls(batch.num_rows, type=field.type))
        else:
            arrays.append(batch.column(index).cast(field.type))
    return pa.record_batch(arrays, schema=PARQUET_SCHEMA)


# Streams batches of a parquet file and closes it deterministically. On
# Windows an unclosed ParquetFile keeps the file locked, which would break the
# atomic replace/delete steps below.
def read_batches(path, columns=None):
    parquet_file = pq.ParquetFile(path)
    try:
        for batch in parquet_file.iter_batches(batch_size=PARQUET_BATCH_SIZE, columns=columns):
            yield batch
    finally:
        parquet_file.close()


# Merges the existing state file AND every completed zip part into a new state
# file, atomically. Part rows supersede older state-file rows for the same
# property (a refetched zip wins). Because the previous state file is an input
# rather than being rebuilt from parts alone, zips compacted by an earlier run
# are never lost when a later run compacts again. Only after the new state file
# is safely in place are the merged parts deleted, so an interruption at any
# point leaves a consistent, resumable state.
def merge_parts_into_output(parts_dir, output_path):
    part_paths = sorted(parts_dir.glob("*.parquet"))
    override_ids = set()
    for part_path in part_paths:
        for batch in read_batches(part_path, columns=["property_id"]):
            override_ids.update(batch.column(0).to_pylist())

    tmp_path = output_path.with_name(output_path.name + ".tmp")
    total_rows = 0
    try:
        with pq.ParquetWriter(tmp_path, PARQUET_SCHEMA, compression="zstd") as writer:
            if output_path.exists():
                for batch in read_batches(output_path):
                    batch = align_batch(batch)
                    ids = batch.column("property_id").to_pylist()
                    keep = [i for i, pid in enumerate(ids) if pid not in override_ids]
                    if len(keep) < len(ids):
                        batch = batch.take(pa.array(keep))
                    if batch.num_rows:
                        writer.write_batch(batch)
                        total_rows += batch.num_rows
            for part_path in part_paths:
                for batch in read_batches(part_path):
                    writer.write_batch(align_batch(batch))
                    total_rows += batch.num_rows
        os.replace(tmp_path, output_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

    for part_path in part_paths:
        part_path.unlink()
    return total_rows


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


# A zip counts as done if it has a completed part on disk OR its rows are
# already inside the merged state file (the zip column covers zips compacted by
# earlier runs). No separate progress file to drift out of sync with the data.
def load_zips_done(parts_dir, output_path):
    zips_done = {p.stem for p in parts_dir.glob("*.parquet")}
    if output_path.exists():
        for batch in read_batches(output_path, columns=["zip"]):
            for value in batch.column(0).to_pylist():
                if value:
                    zips_done.add(str(value)[:5].zfill(5))
    return zips_done


def state_paths(state):
    state_lower = state.lower()
    return (
        Path(f"{state_lower}_residential_properties.parquet"),
        Path(f".{state_lower}_property_parts"),
    )


def crawl_state(state, delay=1.0, max_zips=None, with_details=True):
    output_path, parts_dir = state_paths(state)
    zips_done = load_zips_done(parts_dir, output_path)
    zips = load_zips(state)
    todo = [z for z in zips if z not in zips_done]
    if max_zips:
        todo = todo[:max_zips]
    print(f"{state}: {len(zips)} zips total, {len(zips_done)} done, {len(todo)} to fetch")

    total_rows = 0
    planned_total = len(zips_done) + len(todo)
    completed = len(zips_done)
    started_at = time.monotonic()

    for zip_code in todo:
        try:
            zip_rows = collect_zip_rows(state, zip_code, delay, with_details=with_details)
        except RuntimeError as e:
            # no part is written for this zip, so the next run safely refetches
            # it from the beginning
            print(f"  zip {zip_code}: FAILED ({e}); will retry next run")
            continue

        write_zip_part(zip_rows, parts_dir / f"{zip_code}.parquet")
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

    if list(parts_dir.glob("*.parquet")):
        merged_rows = merge_parts_into_output(parts_dir, output_path)
        print(f"{state}: state file now holds {merged_rows:,} rows -> {output_path}")
    elif not output_path.exists():
        print(f"{state}: no property data found; no Parquet file written")

    return total_rows


# runs everything
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crawl Propwire residential properties by zip.")
    parser.add_argument("states", nargs="*", default=["TX", "FL"], help="state codes (default: TX)")
    parser.add_argument("--max-zips", type=int, default=None, help="limit zips per state (for testing)")
    parser.add_argument("--delay", type=float, default=1.0, help="seconds between API calls")
    parser.add_argument(
        "--skip-details",
        action="store_true",
        help="skip pw_property_detail lookups (search data only; detail_json left null)",
    )
    args = parser.parse_args()

    # the progress bar uses a block char that cp1252 consoles cannot print
    try:
        sys.stdout.reconfigure(errors="replace")
    except Exception:
        pass

    new_session()

    try:
        totals = {}
        for state in args.states:
            totals[state] = crawl_state(
                state.upper(), delay=args.delay, max_zips=args.max_zips, with_details=not args.skip_details
            )
    except KeyboardInterrupt:
        print("\ninterrupted - completed zips are saved; rerun to resume where it left off")
        raise SystemExit(130)

    for state, n in totals.items():
        print(f"{state}: {n:,} rows this run")
    print(f"Total: {sum(totals.values()):,} rows this run")
