# Contributing to mrf-etl

Thank you for your interest in contributing to `mrf-etl` — an open-source Python library
for parsing hospital Machine-Readable Files (MRF) under the CMS price transparency mandate.

---

## Table of Contents
- [Reporting Issues](#reporting-issues)
- [Suggesting Features](#suggesting-features)
- [Submitting Pull Requests](#submitting-pull-requests)
- [Adding Hospital Support](#adding-hospital-support)
- [Project Structure](#project-structure)
- [Code Style](#code-style)
- [Running Tests](#running-tests)
- [License](#license)

---

## Reporting Issues

Use [GitHub Issues](https://github.com/HarshalM22/MRF-ETL/issues) to report bugs.

When reporting a parsing failure, please include:
- Hospital name (if publicly available)
- Layout type if known (`horizontal`, `vertical`, or `mixed`)
- The error message or unexpected output
- A sample of the failing header row or data row (no PHI)

> Only share data from **public** hospital MRF files — never include patient data.

---

## Suggesting Features

Open a GitHub Issue with the label `enhancement`. Describe:
- What problem you're trying to solve
- Which part of the pipeline it affects (ingestion, layout detection, normalization, loading)
- Any example MRF files or CMS spec references that are relevant

---

## Submitting Pull Requests

1. Fork the repo and create a branch:
```bash
   git checkout -b fix/your-fix-name
```

2. Make your changes — keep them focused and minimal

3. Run the existing tests:
```bash
   python tests/test_phase1.py
   python tests/test_phase2.py
   python tests/test_phase3.py
   python tests/test_phase5.py
```

4. Commit with a clear message following this format:
```
   fix: describe what you fixed
   feat: describe new feature
   docs: documentation change
   refactor: code cleanup with no behavior change
```

5. Open a pull request against `main` with a clear description of what changed and why

---

## Adding Hospital Support

`mrf-etl` is designed to handle any CMS-compliant MRF file, but real hospital files
often have quirks. If you find a file that parses incorrectly:

1. Open an issue with the failing header row
2. Describe the layout — is it horizontal, vertical, or mixed?
3. Include what the parser produced vs what you expected

Common areas where new hospitals introduce variants:
- Metadata key names (`location_name` vs `hospital_location`)
- Payer column naming patterns (pipes inside payer names)
- Scientific notation in code fields (Excel export corruption)
- Non-standard date formats in `last_updated_on`

If you fix it, the relevant files are:
- `mrf_etl/schema/key_aliases.py` — for metadata key variants
- `mrf_etl/core/layout_detector.py` — for new column patterns
- `mrf_etl/core/normalizer.py` — for new value edge cases

---

## Project Structure
```
mrf_etl/
├── core/
│   ├── ingester.py          # file opening, encoding, decompression, streaming
│   ├── layout_detector.py   # header parsing, layout detection, payer column mapping
│   ├── meta_parser.py       # rows 0-1 metadata parsing → HospitalMeta
│   ├── normalizer.py        # value cleaning — charges, codes, dates, methodology
│   ├── row_parser.py        # single CSV row → MRFRow (horizontal + vertical)
│   ├── pipeline.py          # parse_file() — full streaming pipeline
│   ├── parser.py            # MRFParser convenience wrapper + file profiling
│   └── bulk_runner.py       # threaded bulk processing with retry + progress
├── loaders/
│   ├── base_loader.py       # abstract BaseLoader + shared chunking logic
│   ├── csv_loader.py        # writes 5 normalized CSV files
│   ├── mysql_loader.py      # loads into MySQL via PyMySQL
│   └── postgres_loader.py   # loads into PostgreSQL via psycopg2
└── schema/
    ├── mrf_row.py           # MRFRow, BillingCode, PayerRate, HospitalMeta dataclasses
    └── key_aliases.py       # metadata key normalization map
```

---

## Code Style

- Python 3.10+ only
- No external dependencies in `mrf_etl/core/` — keep it stdlib only
- `mrf_etl/loaders/` may use optional dependencies (`pymysql`, `psycopg2-binary`)
- Follow existing patterns — dataclasses for data structures, generators for streaming
- All value normalization belongs in `normalizer.py`, not scattered across files
- New loader targets should extend `BaseLoader` from `base_loader.py`

---

## Running Tests
```bash
# From project root
python tests/test_phase1.py
python tests/test_phase2.py
python tests/test_phase3.py
python tests/test_phase5.py
```

---

## License

By contributing to `mrf-etl`, you agree that your contributions will be licensed
under the [Apache 2.0 License](LICENSE) — the same license as the project.

---

*Built by [HarshalM22](https://github.com/HarshalM22)*