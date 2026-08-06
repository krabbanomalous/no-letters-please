"""Throwaway probe helpers. Reuses scraper's exact session/token/proxy logic."""
import time

import scraper

REQ_COUNT = 0


def search(payload, max_attempts=8):
    """POST to property_search with an arbitrary payload, scraper-style retries."""
    global REQ_COUNT
    for attempt in range(1, max_attempts + 1):
        try:
            r = scraper._session.post(
                scraper.API_URL, headers=scraper.api_headers(), json=payload, timeout=60
            )
            if r.status_code == 200:
                try:
                    data = r.json()
                except Exception:
                    data = None
                if data is not None:
                    REQ_COUNT += 1
                    return data
                print(f"    attempt {attempt}: 200 non-JSON (challenge)")
            elif r.status_code == 401:
                scraper.get_token(force=True)
                continue
            else:
                print(f"    attempt {attempt}: HTTP {r.status_code}")
        except Exception as e:
            print(f"    attempt {attempt}: {type(e).__name__}: {e}")
        scraper.new_session()
        scraper.get_token(force=True)
        time.sleep(min(2 * attempt, 15))
    raise RuntimeError(f"search failed after {max_attempts} attempts: {payload}")


def count(locations, property_types=None, extra=None, size=1):
    payload = {
        "locations": locations,
        "size": size,
        "result_index": 0,
        "house": True,
    }
    if property_types is not None:
        payload["property_type"] = property_types
    if extra:
        payload.update(extra)
    data = search(payload)
    time.sleep(1.0)
    return data


def init():
    scraper.new_session()
    scraper.get_token()
