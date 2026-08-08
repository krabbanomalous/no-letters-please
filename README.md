To use, run scripts\scraper.py through your IDE or your system's terminal.

Parquet files are saved within the project folder.

## Modules:
- **normalize_address.py**: Reformats addresses to fit a single writing standard. ("Drive" -> Dr. "North Street" -> N Str., etc.)
- **read_parquet.py**: Created to examine individual parquet files. save_as_csv takes the desired state's abbreviation in lowercase (only supports Texas and Florida) and the ZIP code as parameters.
- **scraper.py**: Scrapes Propwire for TX and FL addresses and compiles them into parquet files.

## scraper.py notes:
- The search API returns nothing past result offset 10,000. ZIPs larger than that are split by property type, then recursively bisected on `beds` and `estimated_value` range filters until every sub-query fits inside the window. Per-ZIP coverage (unique properties fetched vs. the API's total) is printed; a small number of properties with no beds/value data may be unreachable in oversized ZIPs.
- For every property, the site's `pw_property_detail` endpoint is queried; only the requested sections (ownership `owner_details`, transfer/sales `transfer_history`, mortgages `current_mortgages` + `mortgage_history`, parcel `parcel_details`, tax `tax_details`, MLS `mls_history`) are kept in the `detail_json` column - the rest of the record is discarded to save disk space (~1.9 KB per property). Details are fetched in batches of up to 500 ids per request (one property per request gets rate-limited by DataDome within a few ZIPs). Use `--skip-details` to collect search data only.
- Resume state is derived from the data itself: a ZIP counts as done if it has a part file in `.<state>_property_parts/` or its rows are already in `<state>_residential_properties.parquet`. Compaction merges the existing state file plus all parts (parts win on conflicts) and only deletes parts after the new state file is atomically in place, so interrupted runs and repeated compactions never lose previously completed ZIPs. The old `<state>_properties_zips_done.txt` progress files are no longer used.
- Files matching `scripts/_probe*.py` and `scripts/_test_driver.py` are throwaway API experiments/tests, not part of the pipeline.

Made with Python 3.14.6.
