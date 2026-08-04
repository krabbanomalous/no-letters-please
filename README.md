To use, run scripts\scraper.py through your IDE or your system's terminal.

Parquet files are saved within the project folder.

## Modules:
- **normalize_address.py**: Reformats addresses to fit a single writing standard. ("Drive" -> Dr. "North Street" -> N Str., etc.)
- **read_parquet.py**: Created to examine individual parquet files. save_as_csv takes the desired state's abbreviation in lowercase (only supports Texas and Florida) and the ZIP code as parameters.
- **scraper.py**: Scrapes Propwire for TX and FL addresses and compiles them into parquet files.

Made with Python 3.14.6.
