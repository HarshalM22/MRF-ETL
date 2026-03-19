"""
cli.py
Command-line interface for mrf-etl.

Commands:
  parse     Parse a single MRF file
  bulk      Parse multiple MRF files from a list
  inspect   Show file profile without loading

Usage examples:
  # Single file → CSV
  mrf-etl parse --input hospital.csv --output csv --out-dir ./output

  # Single file → MySQL
  mrf-etl parse --input hospital.csv --output mysql \\
    --db-host localhost --db-port 3306 --db-user root \\
    --db-pass secret --db-name mrf_db

  # Bulk from URL list → CSV (4 threads)
  mrf-etl bulk --input urls.txt --output csv --out-dir ./output --workers 4

  # Bulk → MySQL with error report
  mrf-etl bulk --input urls.txt --output mysql \\
    --db-host localhost --db-user root --db-pass secret --db-name mrf_db \\
    --workers 8 --error-report errors.txt

  # Inspect a file (no loading)
  mrf-etl inspect --input hospital.csv

Install as a script via pyproject.toml entry_points.
Or run directly: python -m mrf_etl.cli <command> [options]
"""

from __future__ import annotations

import argparse
import os
import sys
import time

# Ensure package is importable when run as __main__
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from mrf_etl.core.pipeline import parse_file, profile_file
from mrf_etl.core.meta_parser import parse_metadata
from mrf_etl.core.bulk_runner import (
    run_bulk, read_sources, write_error_report, write_failed_sources
)
from mrf_etl.loaders.csv_loader import CSVLoader
from mrf_etl.loaders.mysql_loader import MySQLLoader
from mrf_etl.loaders.postgres_loader import PostgresLoader


# ---------------------------------------------------------------------------
# Loader factory
# ---------------------------------------------------------------------------

def _build_loader(args: argparse.Namespace):
    """Build the appropriate loader from CLI args."""
    if args.output == "csv":
        out_dir = getattr(args, "out_dir", "./mrf_output")
        return CSVLoader(output_dir=out_dir, chunk_size=args.chunk_size)

    if args.output == "mysql":
        return MySQLLoader(
            host=args.db_host,
            port=args.db_port,
            user=args.db_user,
            password=args.db_pass,
            database=args.db_name,
            chunk_size=args.chunk_size,
        )

    if args.output == "postgres":
        return PostgresLoader(
            host=args.db_host,
            port=args.db_port,
            user=args.db_user,
            password=args.db_pass,
            database=args.db_name,
            chunk_size=args.chunk_size,
        )

    print(f"ERROR: Unknown output format '{args.output}'", file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Command: inspect
# ---------------------------------------------------------------------------

def cmd_inspect(args: argparse.Namespace):
    """Show file profile without loading anything."""
    source = args.input
    if not os.path.exists(source) and not source.startswith("http"):
        print(f"ERROR: File not found: {source}", file=sys.stderr)
        sys.exit(1)

    print(f"\nInspecting: {source}\n")
    try:
        p = profile_file(source)
        print(f"  Hospital      : {p['hospital_name']}")
        print(f"  Layout        : {p['layout_type']}")
        print(f"  Total columns : {p['total_columns']}")
        print(f"  Payer combos  : {p['payer_plan_combos']}")
        print(f"  Sample rows   : {p['sample_rows']}")
        print(f"  Avg codes/row : {p['avg_codes_per_row']}")
        print(f"  Avg rates/row : {p['avg_rates_per_row']}")
        print(f"  Code types    : {p['code_types_seen']}")
        print(f"  Top payers    : {p['unique_payers_sample'][:5]}")
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Command: parse (single file)
# ---------------------------------------------------------------------------

def cmd_parse(args: argparse.Namespace):
    """Parse a single MRF file and load it."""
    source = args.input
    loader = _build_loader(args)

    print(f"\nParsing: {source}")
    print(f"Output : {args.output}")

    try:
        t0 = time.time()
        meta = parse_metadata(source)
        rows = parse_file(source)
        stats = loader.load(
            rows=rows,
            meta=meta,
            source_file=source,
            skip_if_loaded=not args.force,
            verbose=True,
        )
        elapsed = time.time() - t0

        if stats.get("skipped"):
            print(f"\nSkipped (already loaded). Use --force to reload.")
        else:
            print(f"\nComplete in {elapsed:.1f}s")
            print(f"  Items  : {stats['items']:,}")
            print(f"  Codes  : {stats['codes']:,}")
            print(f"  Rates  : {stats['rates']:,}")
            print(f"  Raw    : {stats['raw']:,}")

    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)
    finally:
        if hasattr(loader, "close"):
            loader.close()


# ---------------------------------------------------------------------------
# Command: bulk
# ---------------------------------------------------------------------------

def cmd_bulk(args: argparse.Namespace):
    """Parse multiple MRF files from an input list."""
    try:
        sources = read_sources(args.input)
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    if not sources:
        print("ERROR: No sources found in input file.", file=sys.stderr)
        sys.exit(1)

    loader = _build_loader(args)

    try:
        run_stats = run_bulk(
            sources=sources,
            loader=loader,
            workers=args.workers,
            max_retries=args.max_retries,
            retry_delay=args.retry_delay,
            verbose=True,
        )
    finally:
        if hasattr(loader, "close"):
            loader.close()

    # Write error report if requested
    if args.error_report and run_stats.failed_files:
        write_error_report(run_stats, args.error_report)

    # Write failed sources for easy re-run
    if args.failed_sources and run_stats.failed_files:
        write_failed_sources(run_stats, args.failed_sources)

    # Exit with error code if any failures
    if run_stats.failed > 0:
        sys.exit(1)


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def _add_db_args(parser: argparse.ArgumentParser):
    """Add database connection arguments (shared by parse and bulk)."""
    db = parser.add_argument_group("database connection")
    db.add_argument("--db-host",  default="localhost",  help="DB host (default: localhost)")
    db.add_argument("--db-port",  default=3306, type=int, help="DB port (default: 3306)")
    db.add_argument("--db-user",  default="root",       help="DB user")
    db.add_argument("--db-pass",  default="",           help="DB password")
    db.add_argument("--db-name",  default="mrf_db",     help="DB name (default: mrf_db)")


def _add_output_args(parser: argparse.ArgumentParser):
    """Add output format arguments."""
    out = parser.add_argument_group("output")
    out.add_argument(
        "--output", "-o",
        choices=["csv", "mysql", "postgres"],
        default="csv",
        help="Output format (default: csv)",
    )
    out.add_argument(
        "--out-dir",
        default="./mrf_output",
        help="Output directory for CSV files (default: ./mrf_output)",
    )
    out.add_argument(
        "--chunk-size",
        default=500, type=int,
        help="Batch insert size (default: 500)",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mrf-etl",
        description="Universal MRF CSV parser for hospital price transparency data.",
    )
    parser.add_argument("--version", action="version", version="mrf-etl 0.1.0")
    sub = parser.add_subparsers(dest="command", required=True)

    # ── inspect ──────────────────────────────────────────────────────
    p_inspect = sub.add_parser("inspect", help="Show file profile without loading")
    p_inspect.add_argument("--input", "-i", required=True, help="MRF CSV file path or URL")

    # ── parse ─────────────────────────────────────────────────────────
    p_parse = sub.add_parser("parse", help="Parse and load a single MRF file")
    p_parse.add_argument("--input", "-i", required=True, help="MRF CSV file path or URL")
    p_parse.add_argument("--force", action="store_true", help="Re-load even if already loaded")
    p_parse.add_argument("--verbose", "-v", action="store_true", help="Show full traceback on error")
    _add_output_args(p_parse)
    _add_db_args(p_parse)

    # ── bulk ──────────────────────────────────────────────────────────
    p_bulk = sub.add_parser("bulk", help="Parse and load multiple MRF files")
    p_bulk.add_argument(
        "--input", "-i", required=True,
        help="Text file with one file path or URL per line",
    )
    p_bulk.add_argument(
        "--workers", "-w", default=4, type=int,
        help="Parallel worker threads (default: 4)",
    )
    p_bulk.add_argument(
        "--max-retries", default=3, type=int,
        help="Max retry attempts per file (default: 3)",
    )
    p_bulk.add_argument(
        "--retry-delay", default=2.0, type=float,
        help="Base retry delay in seconds (default: 2.0, exponential backoff)",
    )
    p_bulk.add_argument(
        "--error-report",
        help="Write detailed error report to this file",
    )
    p_bulk.add_argument(
        "--failed-sources",
        help="Write failed source paths to this file for easy re-run",
    )
    _add_output_args(p_bulk)
    _add_db_args(p_bulk)

    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "inspect":
        cmd_inspect(args)
    elif args.command == "parse":
        cmd_parse(args)
    elif args.command == "bulk":
        cmd_bulk(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()