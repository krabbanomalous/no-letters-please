"""Probe 2: try candidate filter field names; see which change result_count."""
import _probe_common as pc

pc.init()

loc = [{"state": "TX", "zip": "77084"}]
base = pc.count(loc, ["sfr"]).get("result_count")
print(f"baseline sfr = {base}\n")

candidates = [
    ("beds_min", {"beds_min": 3}),
    ("bed_min", {"bed_min": 3}),
    ("beds", {"beds": [3]}),
    ("min_beds", {"min_beds": 3}),
    ("bedrooms_min", {"bedrooms_min": 3}),
    ("baths_min", {"baths_min": 2}),
    ("bathrooms_min", {"bathrooms_min": 2}),
    ("sqft_min", {"sqft_min": 2000}),
    ("building_size_min", {"building_size_min": 2000}),
    ("living_area_min", {"living_area_min": 2000}),
    ("square_feet_min", {"square_feet_min": 2000}),
    ("year_built_min", {"year_built_min": 2000}),
    ("yearbuilt_min", {"yearbuilt_min": 2000}),
    ("estimated_value_min", {"estimated_value_min": 300000}),
    ("price_min", {"price_min": 300000}),
    ("value_min", {"value_min": 300000}),
    ("lot_size_min", {"lot_size_min": 10000}),
    ("lot_acres_min", {"lot_acres_min": 0.25}),
    ("listing_status", {"listing_status": ["active"]}),
    ("status", {"status": "for_sale"}),
]
for name, extra in candidates:
    try:
        d = pc.count(loc, ["sfr"], extra)
        rc = d.get("result_count")
        marker = "  <== NARROWS" if rc is not None and rc != base else ""
        print(f"{name}: {rc}{marker}")
    except RuntimeError as e:
        print(f"{name}: FAILED {e}")
