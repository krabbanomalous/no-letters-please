"""Probe 1: baseline result_counts for candidate dense zips, per property type."""
import _probe_common as pc

pc.init()

for z in ["75034", "77084"]:
    loc = [{"state": "TX", "zip": z}]
    all_res = pc.count(loc, ["mobile", "mfh_5_plus", "mfh_2_to_4", "condo", "sfr"])
    print(f"zip {z}: residential total = {all_res.get('result_count')}")
    for t in ["sfr", "condo", "mfh_2_to_4", "mfh_5_plus", "mobile"]:
        d = pc.count(loc, [t])
        print(f"  {t}: {d.get('result_count')}")
