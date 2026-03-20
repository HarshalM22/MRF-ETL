# mrf-etl

[![PyPI version](https://badge.fury.io/py/mrf-etl.svg)](https://pypi.org/project/mrf-etl/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)

**Universal MRF CSV parser for hospital price transparency data.**

Parse any hospital Machine-Readable File (MRF) into clean, structured data — regardless of layout, column naming, or number of payers — and load it into MySQL, PostgreSQL, or normalized CSV files with zero data loss.

---

## What is an MRF file?

The U.S. Centers for Medicare & Medicaid Services (CMS) requires every hospital to publish a **Machine-Readable File (MRF)** listing their prices for all procedures — including gross charges, discounted cash prices, and negotiated rates for every insurance payer and plan.

The problem: **every hospital formats their file differently.** Some hospitals have 500+ payer columns. Others use vertical layouts. Column names vary wildly. Encoding issues, scientific notation in code fields, pipe-delimited payer names — real files are messy.

`mrf-etl` handles all of that for you.

---

## Install

```bash
# Core (no dependencies — pure stdlib)
pip install mrf-etl

# With MySQL support
pip install "mrf-etl[mysql]"

# With PostgreSQL support
pip install "mrf-etl[postgres]"

# Both databases
pip install "mrf-etl[all]"
```

---

## Quickstart

### Parse a hospital file in 5 lines

```python
from mrf_etl import parse_file, parse_metadata

source = "hospital_standardcharges.csv"  # local file or HTTPS URL

meta = parse_metadata(source)
print(f"Hospital : {meta.hospital_name}")
print(f"Updated  : {meta.last_updated_on}")
print(f"NPI      : {meta.hospital_npi}")

for row in parse_file(source):
    print(f"\nProcedure : {row.description}")
    print(f"Gross     : ${row.gross_charge}")
    print(f"Cash      : ${row.discounted_cash}")

    for code in row.billing_codes:
        print(f"  Code    : {code.code_type} {code.code}")

    for rate in row.rates:
        print(f"  Rate    : {rate.payer_name_raw} → ${rate.negotiated_dollar}")

    break  # remove to stream all rows
```

**Output:**
```
Hospital : Marshall Medical Center
Updated  : 2024-11-20
NPI      : 1234567890

Procedure : Hip Replacement
Gross     : $45000.00
Cash      : $22500.00
  Code    : MS-DRG 470
  Code    : CPT 27130
  Rate    : Blue Cross PPO  → $28500.00
  Rate    : Medicare FFS    → $18200.00
  Rate    : Aetna HMO       → $24100.00
```

---

## Load to CSV

```python
from mrf_etl import parse_file, parse_metadata, CSVLoader

source = "hospital_standardcharges.csv"

meta   = parse_metadata(source)
rows   = parse_file(source)
loader = CSVLoader(output_dir="./output")
stats  = loader.load(rows, meta, source_file=source)
loader.close()

print(stats)
# {'items': 4821, 'codes': 9644, 'rates': 241050, 'raw': 4821, 'skipped': False}
```

Produces 5 normalized CSV files in `./output/`:
```
mrf_hospitals.csv    — one row per hospital
mrf_items.csv        — one row per procedure
mrf_item_codes.csv   — all billing codes (CPT, HCPCS, MS-DRG, RC, NDC...)
mrf_rates.csv        — all payer/plan negotiated rates
mrf_raw.csv          — original row preserved for audit
```

---

## Load to MySQL

```python
from mrf_etl import parse_file, parse_metadata, MySQLLoader

loader = MySQLLoader(
    host="localhost",
    port=3306,
    user="root",
    password="yourpassword",
    database="mrf_db"
)

source = "hospital_standardcharges.csv"
meta   = parse_metadata(source)
rows   = parse_file(source)
stats  = loader.load(rows, meta, source_file=source)

print(stats)
# {'items': 4821, 'codes': 9644, 'rates': 241050, 'raw': 4821, 'skipped': False}
```

Tables are created automatically on first run. Re-running the same file is safe — the loader skips already-loaded files automatically.

---

## Load to PostgreSQL

```python
from mrf_etl import parse_file, parse_metadata, PostgresLoader

loader = PostgresLoader(
    host="localhost",
    port=5432,
    user="postgres",
    password="yourpassword",
    database="mrf_db"
)

meta  = parse_metadata("hospital_standardcharges.csv")
rows  = parse_file("hospital_standardcharges.csv")
stats = loader.load(rows, meta, source_file="hospital_standardcharges.csv")
```

---

## Bulk Processing — Multiple Hospitals

```python
from mrf_etl import run_bulk, read_sources, CSVLoader

# sources.txt — one file path or URL per line
sources = read_sources("sources.txt")
loader  = CSVLoader(output_dir="./output")

stats = run_bulk(
    sources=sources,
    loader=loader,
    workers=4,        # parallel threads
    max_retries=3,    # retry on failure
)

print(stats.summary())
```

**sources.txt format:**
```
# One path or URL per line — comments and blank lines are ignored
/data/hospital1_standardcharges.csv
/data/hospital2_standardcharges.csv
https://hospital.org/standardcharges.csv
https://hospital.org/standardcharges.csv.gz
```

---

## CLI Usage

```bash
# Inspect a file (no loading — just show layout and metadata)
mrf-etl inspect --input hospital_standardcharges.csv

# Parse to CSV
mrf-etl parse --input hospital_standardcharges.csv --output csv --out-dir ./output

# Parse to MySQL
mrf-etl parse --input hospital_standardcharges.csv --output mysql \
  --db-host localhost --db-user root --db-pass secret --db-name mrf_db

# Bulk process many hospitals
mrf-etl bulk --input sources.txt --output csv --out-dir ./output --workers 4
```

---

## Output Schema

All loaders produce the same 5-table schema:

| Table | Description |
|---|---|
| `mrf_hospitals` | One row per hospital file — name, NPI, address, version |
| `mrf_items` | One row per procedure — description, gross charge, cash price |
| `mrf_item_codes` | All billing codes per item — CPT, HCPCS, MS-DRG, RC, NDC, CDM |
| `mrf_rates` | All payer/plan negotiated rates per item |
| `mrf_raw` | Original CSV row preserved as JSON for full audit trail |

---

## What MRF Formats Are Supported?

| Feature | Supported |
|---|---|
| Horizontal layout (payers as columns) | ✅ |
| Vertical layout (payer_name / plan_name columns) | ✅ |
| Mixed layout | ✅ |
| Up to 500+ payer columns | ✅ |
| Multiple billing codes per row (code\|1 through code\|N) | ✅ |
| Gzipped files (.csv.gz) | ✅ |
| Zipped files (.zip) | ✅ |
| Remote files via HTTP/HTTPS | ✅ |
| Scientific notation in code fields (Excel corruption) | ✅ |
| Encoding detection (UTF-8, Latin-1, CP1252) | ✅ |
| Code types: CPT, HCPCS, MS-DRG, RC, NDC, CDM, LOCAL | ✅ |

---

## Data Classes

Every parsed row returns an `MRFRow`:

```python
@dataclass
class MRFRow:
    hospital_name: str
    description: str           # procedure description
    gross_charge: float        # hospital's list price
    discounted_cash: float     # cash-pay price
    min_negotiated: float      # min across all payers
    max_negotiated: float      # max across all payers
    billing_codes: list[BillingCode]
    rates: list[PayerRate]
    setting: str               # inpatient | outpatient | both
    billing_class: str         # facility | professional
    ...

@dataclass
class BillingCode:
    code: str                  # e.g. "27130"
    code_type: str             # CPT | HCPCS | MS-DRG | RC | NDC | CDM
    is_primary: bool

@dataclass
class PayerRate:
    payer_name_raw: str        # e.g. "Blue Cross PPO"
    plan_name_raw: str
    negotiated_dollar: float
    negotiated_percentage: float
    methodology: str           # fee_schedule | per_diem | percent_of_billed | ...
    rate_flag: str             # not_covered | bundled | not_applicable | ...
```

---

## Quick File Inspection

```python
from mrf_etl import MRFParser

parser = MRFParser("hospital_standardcharges.csv")
print(parser.summary())
```

```
============================================================
FILE:     hospital_standardcharges.csv
============================================================
HOSPITAL: Marshall Medical Center
NPI:      1234567890
UPDATED:  2024-11-20
VERSION:  2.0.0
────────────────────────────────────────────────────────────
LAYOUT:   HORIZONTAL
HEADERS:  312 columns
CODE COLS:[1, 2, 3]
PAYER COMBOS: 74
PAYER/PLAN SAMPLE (first 5):
  'Blue Cross PPO'                         | 'Standard'
  'Aetna HMO'                              | 'Tier1'
  'Medicare FFS'                           | ''
  ...
============================================================
```

---

## Why mrf-etl?

- **Zero dependencies** in core — no pandas, no numpy, no ORM required
- **Streaming** — never loads the full file into memory, works on files with millions of rows
- **Idempotent** — re-run the same file safely, duplicates are automatically skipped
- **Battle-tested** — built and validated against real hospital MRF files across multiple layouts
- **Extensible** — implement `BaseLoader` to add your own storage target

---

## Running Tests

```bash
python tests/test_phase1.py
python tests/test_phase2.py
python tests/test_phase3.py
python tests/test_phase5.py
```

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to report issues, add hospital support, and submit pull requests.

---

## License

Apache 2.0 — see [LICENSE](LICENSE)

---

*Built by [HarshalM22](https://github.com/HarshalM22) — open-source tooling for CMS hospital price transparency data.*