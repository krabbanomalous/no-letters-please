"""Probe 3: fetch propwire search page + JS bundles, dump filter-related snippets."""
import html
import json
import re
import time

import scraper

scraper.new_session()

r = scraper._session.get(scraper.TOKEN_PAGE_URL, timeout=60)
print("search page:", r.status_code, len(r.text))

# Inertia data-page: full props JSON
m = re.search(r'data-page="((?:[^"\\]|\\.)*)"', r.text)
page = json.loads(html.unescape(m.group(1)))
print("page component:", page.get("component"))
props = page.get("props", {})
print("props keys:", sorted(props.keys()))
for k in ["filters", "filter", "defaults", "constants", "enums"]:
    if k in props:
        v = props[k]
        s = json.dumps(v)
        print(f"props[{k}] ({len(s)} chars): {s[:2000]}")

# script tags
scripts = re.findall(r'<script[^>]+src="([^"]+)"', r.text)
print(f"\n{len(scripts)} script tags")
js_urls = [u if u.startswith("http") else "https://propwire.com" + u for u in scripts]
for u in js_urls:
    print(" ", u)

# fetch each JS, find property_search context
pat = re.compile(r".{400}property_search.{400}", re.S)
for u in js_urls:
    time.sleep(1.0)
    try:
        j = scraper._session.get(u, timeout=60)
        if j.status_code != 200:
            print(u, j.status_code)
            continue
        txt = j.text
        if "property_search" in txt or "property_type" in txt:
            print(f"\n===== {u} ({len(txt)} chars) =====")
            for mm in pat.finditer(txt):
                print(mm.group(0).replace("\n", " "))
                print("----")
    except Exception as e:
        print(u, type(e).__name__, e)
