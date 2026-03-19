# mrf-etl

Universal MRF CSV parser for hospital price transparency data.

Parses any hospital Machine-Readable File (MRF) CSV — regardless of format, column naming, layout, or number of payers — and loads it into MySQL, Postgres, or normalized CSV files with zero data loss.

---

## Install

```bash
# Core (no dependencies)
pip install -e .

# With MySQL support
pip install -e ".[mysql]"

# With Postgres support
pip install -e ".[postgres]"

# Both
pip install -e ".[all]"
```

---

## Quick Start

### Parse a single file → CSV
```bash
mrf-etl parse --input hospital_standardcharges.csv --output csv --out-dir ./output
```

### Parse a single file → MySQL
```bash
mrf-etl parse --input hospital_standardcharges.csv --output mysql \
  --db-host localhost --db-user root --db-pass secret --db-name mrf_db
```

### Inspect a file (no loading)
```bash
mrf-etl inspect --input hospital_standardcharges.csv
```

### Bulk: process many hospitals
```bash
# Create a sources file (one path or URL per line)
mrf-etl bulk --input sources.txt --output csv --out-dir ./output --workers 4
```

---

## Python API

```python
from mrf_etl import parse_file, parse_metadata, CSVLoader

# Stream rows from any MRF file
for row in parse_file("hospital_standardcharges.csv"):
    print(row.description, row.gross_charge)
    for code in row.billing_codes:
        print(f"  {code.code_type}: {code.code}")
    for rate in row.rates:
        print(f"  {rate.payer_name_raw}: ${rate.negotiated_dollar}")

# Load to CSV
meta   = parse_metadata("hospital_standardcharges.csv")
rows   = parse_file("hospital_standardcharges.csv")
loader = CSVLoader(output_dir="./output")
stats  = loader.load(rows, meta, source_file="hospital_standardcharges.csv")
loader.close()
print(stats)  # {'items': ..., 'codes': ..., 'rates': ..., 'raw': ...}

# Load to MySQL
from mrf_etl import MySQLLoader
loader = MySQLLoader(host="localhost", user="root", password="secret", database="mrf_db")
stats  = loader.load(rows, meta, source_file="hospital_standardcharges.csv")

# Bulk processing
from mrf_etl import run_bulk, read_sources
sources = read_sources("sources.txt")
stats   = run_bulk(sources=sources, loader=loader, workers=4)
print(stats.summary())
```

---

## Output Schema (5 tables)

```
mrf_hospitals       — one row per hospital file
mrf_items           — one row per procedure per hospital
mrf_item_codes      — all billing codes per item (CPT, HCPCS, RC, NDC, CDM...)
mrf_rates           — all payer/plan rates per item
mrf_raw             — original CSV row preserved (audit trail)
```

---

## Supported Formats

- Horizontal layout (payers as columns — up to 500+ payer columns)
- Vertical layout (payer_name / plan_name columns)
- Mixed layout
- Up to N billing codes per procedure (code|1 through code|N)
- Code types: CPT, HCPCS, MS-DRG, RC, NDC, CDM, LOCAL
- Gzipped CSV (.csv.gz)
- Zipped CSV (.zip)
- Local files and HTTP/HTTPS URLs

---

## Bulk Sources File Format

```
# sources.txt — one path or URL per line
# Comments and blank lines are ignored

/path/to/hospital1_standardcharges.csv
/path/to/hospital2_standardcharges.csv
https://hospital.org/standardcharges.csv
https://hospital.org/standardcharges.csv.gz
```

---

## Run Tests

```bash
python tests/test_phase1.py
python tests/test_phase2.py
python tests/test_phase3.py
python tests/test_phase5.py
```

---

## CLI Reference

```
mrf-etl inspect --input <file>
mrf-etl parse   --input <file> --output [csv|mysql|postgres] [options]
mrf-etl bulk    --input <sources.txt> --output [csv|mysql|postgres] [options]

Options:
  --out-dir       Output directory for CSV (default: ./mrf_output)
  --db-host       Database host (default: localhost)
  --db-port       Database port (default: 3306 MySQL / 5432 Postgres)
  --db-user       Database user
  --db-pass       Database password
  --db-name       Database name (default: mrf_db)
  --workers       Parallel threads for bulk (default: 4)
  --chunk-size    Batch insert size (default: 500)
  --max-retries   Retry attempts per file (default: 3)
  --force         Re-load even if file already loaded
  --error-report  Write error details to file
  --failed-sources Write failed paths to file for re-run
```