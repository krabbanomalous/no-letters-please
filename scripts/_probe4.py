"""Probe 4: walk Vite chunks of the propwire app bundle, find search-payload code."""
import re
import time

import scraper

scraper.new_session()

seen = set()
todo = ["https://propwire.com/build/assets/app-BIdx_GqV.js"]
hits = []
pat = re.compile(r"property_search|pw_property_detail|property_detail|result_index", re.S)

while todo and len(seen) < 40:
    u = todo.pop()
    if u in seen:
        continue
    seen.add(u)
    time.sleep(0.8)
    try:
        r = scraper._session.get(u, timeout=60)
        if r.status_code != 200:
            print(u, r.status_code)
            continue
    except Exception as e:
        print(u, type(e).__name__)
        continue
    txt = r.text
    # discover other chunk files
    for f in re.findall(r'"(assets/[\w.-]+\.js)"', txt):
        v = "https://propwire.com/build/" + f
        if v not in seen:
            todo.append(v)
    if pat.search(txt):
        hits.append((u, len(txt)))

print(f"\nfetched {len(seen)} chunks")
for u, n in hits:
    print("HIT:", u, n)
