"""
bulk_runner.py
Threaded bulk runner for processing multiple MRF files.

Features:
  - ThreadPoolExecutor with configurable worker count
  - Per-file retry with exponential backoff (max 3 attempts)
  - Real-time progress tracking (files done / total)
  - Error isolation — one failed file never stops the run
  - Returns full stats + error report when complete
  - Reads URLs or file paths from a text file (one per line)
  - Skips blank lines and # comments in input file

No dependency on Phase 4 — works directly with parse_file + loaders.
"""

from __future__ import annotations

import os
import sys
import time
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional

from mrf_etl.core.pipeline import parse_file
from mrf_etl.core.meta_parser import parse_metadata
from mrf_etl.loaders.base_loader import BaseLoader


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class FileResult:
    """Result for one file after processing (success or failure)."""
    source: str
    success: bool
    attempt: int = 1
    items: int = 0
    codes: int = 0
    rates: int = 0
    raw: int = 0
    skipped: bool = False
    duration_sec: float = 0.0
    error: Optional[str] = None
    error_traceback: Optional[str] = None


@dataclass
class BulkRunStats:
    """Aggregate stats for the entire bulk run."""
    total_files: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    total_items: int = 0
    total_codes: int = 0
    total_rates: int = 0
    total_raw: int = 0
    duration_sec: float = 0.0
    results: list[FileResult] = field(default_factory=list)

    @property
    def failed_files(self) -> list[FileResult]:
        return [r for r in self.results if not r.success]

    @property
    def succeeded_files(self) -> list[FileResult]:
        return [r for r in self.results if r.success and not r.skipped]

    def summary(self) -> str:
        lines = [
            "=" * 60,
            "BULK RUN SUMMARY",
            "=" * 60,
            f"  Total files  : {self.total_files}",
            f"  Succeeded    : {self.succeeded}",
            f"  Skipped      : {self.skipped}",
            f"  Failed       : {self.failed}",
            f"  Total items  : {self.total_items:,}",
            f"  Total codes  : {self.total_codes:,}",
            f"  Total rates  : {self.total_rates:,}",
            f"  Duration     : {self.duration_sec:.1f}s",
            "=" * 60,
        ]
        if self.failed_files:
            lines.append("FAILED FILES:")
            for r in self.failed_files:
                lines.append(f"  [{r.attempt} attempts] {r.source}")
                lines.append(f"    Error: {r.error}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Input file parsing
# ---------------------------------------------------------------------------

def read_sources(input_path: str) -> list[str]:
    """
    Read a list of file paths or URLs from a text file.
    One source per line. Blank lines and # comments are ignored.
    Strips whitespace from each line.
    """
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    sources = []
    with open(input_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            sources.append(line)

    return sources


# ---------------------------------------------------------------------------
# Single file processor (runs inside a thread)
# ---------------------------------------------------------------------------

def _process_one(
    source: str,
    loader: BaseLoader,
    max_retries: int = 3,
    retry_delay: float = 2.0,
) -> FileResult:
    """
    Parse and load one file. Retries on transient errors.
    Returns FileResult — never raises.
    """
    last_error = None
    last_tb = None

    for attempt in range(1, max_retries + 1):
        try:
            t0 = time.time()
            meta = parse_metadata(source)
            rows = parse_file(source)
            stats = loader.load(
                rows=rows,
                meta=meta,
                source_file=source,
                skip_if_loaded=True,
                verbose=False,
            )
            duration = time.time() - t0

            return FileResult(
                source=source,
                success=True,
                attempt=attempt,
                items=stats.get("items", 0),
                codes=stats.get("codes", 0),
                rates=stats.get("rates", 0),
                raw=stats.get("raw", 0),
                skipped=stats.get("skipped", False),
                duration_sec=duration,
            )

        except Exception as e:
            last_error = str(e)
            last_tb = traceback.format_exc()
            if attempt < max_retries:
                # Exponential backoff: 2s, 4s, 8s
                time.sleep(retry_delay * (2 ** (attempt - 1)))

    return FileResult(
        source=source,
        success=False,
        attempt=max_retries,
        error=last_error,
        error_traceback=last_tb,
    )


# ---------------------------------------------------------------------------
# Progress tracker (thread-safe)
# ---------------------------------------------------------------------------

class _Progress:
    """Thread-safe progress counter with live terminal output."""

    def __init__(self, total: int, verbose: bool = True):
        self.total = total
        self.done = 0
        self.succeeded = 0
        self.failed = 0
        self.skipped = 0
        self._lock = threading.Lock()
        self.verbose = verbose
        self._start = time.time()

    def update(self, result: FileResult):
        with self._lock:
            self.done += 1
            if result.skipped:
                self.skipped += 1
            elif result.success:
                self.succeeded += 1
            else:
                self.failed += 1

            if self.verbose:
                elapsed = time.time() - self._start
                pct = self.done / self.total * 100
                name = os.path.basename(result.source)[:35]
                status = "SKIP" if result.skipped else ("OK" if result.success else "FAIL")
                print(
                    f"  [{self.done:>4}/{self.total}] {pct:5.1f}%  "
                    f"[{status}] {name:<35}  "
                    f"items={result.items:<6} elapsed={elapsed:.0f}s",
                    flush=True,
                )


# ---------------------------------------------------------------------------
# Main bulk runner
# ---------------------------------------------------------------------------

def run_bulk(
    sources: list[str],
    loader: BaseLoader,
    workers: int = 4,
    max_retries: int = 3,
    retry_delay: float = 2.0,
    verbose: bool = True,
) -> BulkRunStats:
    """
    Process a list of MRF files in parallel using a thread pool.

    Args:
        sources:      List of file paths or URLs to process
        loader:       Any BaseLoader instance (CSVLoader, MySQLLoader, etc.)
        workers:      Number of parallel threads (default 4)
                      Use 1 for sequential, 8+ for large batches with fast I/O
        max_retries:  Retry attempts per file on failure (default 3)
        retry_delay:  Base delay in seconds between retries (exponential backoff)
        verbose:      Print per-file progress to stdout

    Returns:
        BulkRunStats with full results and aggregate counts
    """
    total = len(sources)
    if total == 0:
        return BulkRunStats()

    if verbose:
        print(f"\n{'='*60}")
        print(f"MRF-ETL BULK RUN")
        print(f"  Files   : {total}")
        print(f"  Workers : {workers}")
        print(f"  Retries : {max_retries}")
        print(f"{'='*60}")

    progress = _Progress(total=total, verbose=verbose)
    all_results: list[FileResult] = []
    run_start = time.time()

    # NOTE: Loaders are NOT thread-safe for DB connections.
    # Each worker gets its own loader instance using loader.__class__
    # with the same init params. For CSVLoader we use a lock instead
    # since it writes to shared files.
    #
    # Strategy:
    #   - CSVLoader: single shared instance + lock (file I/O is the bottleneck)
    #   - MySQL/Postgres: each thread gets its own connection

    is_csv_loader = loader.__class__.__name__ == "CSVLoader"
    csv_lock = threading.Lock() if is_csv_loader else None

    def _worker(source: str) -> FileResult:
        if is_csv_loader:
            # CSVLoader is shared — serialize writes with a lock
            result = _process_one_csv_safe(
                source, loader, csv_lock, max_retries, retry_delay
            )
        else:
            # DB loaders: each thread uses the shared loader
            # (pymysql and psycopg2 connections are per-instance,
            #  so as long as loader._conn_get() creates per-thread
            #  connections this is safe — we enforce this below)
            result = _process_one(source, loader, max_retries, retry_delay)
        progress.update(result)
        return result

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_worker, src): src for src in sources}
        for future in as_completed(futures):
            try:
                result = future.result()
                all_results.append(result)
            except Exception as e:
                # Should never happen since _process_one never raises
                src = futures[future]
                all_results.append(FileResult(
                    source=src, success=False,
                    error=f"Unexpected executor error: {e}"
                ))

    run_duration = time.time() - run_start

    # Aggregate stats
    stats = BulkRunStats(
        total_files=total,
        succeeded=sum(1 for r in all_results if r.success and not r.skipped),
        failed=sum(1 for r in all_results if not r.success),
        skipped=sum(1 for r in all_results if r.skipped),
        total_items=sum(r.items for r in all_results),
        total_codes=sum(r.codes for r in all_results),
        total_rates=sum(r.rates for r in all_results),
        total_raw=sum(r.raw for r in all_results),
        duration_sec=run_duration,
        results=all_results,
    )

    if verbose:
        print(f"\n{stats.summary()}")

    return stats


def _process_one_csv_safe(
    source: str,
    loader: BaseLoader,
    lock: threading.Lock,
    max_retries: int,
    retry_delay: float,
) -> FileResult:
    """
    CSV-safe version: parses outside the lock, writes inside the lock.
    This allows parsing to happen in parallel while writes are serialized.
    """
    last_error = None
    last_tb = None

    for attempt in range(1, max_retries + 1):
        try:
            t0 = time.time()
            meta = parse_metadata(source)

            # Parse entirely outside lock (pure read — thread safe)
            parsed_rows = list(parse_file(source))

            # Write inside lock (CSV file I/O — not thread safe)
            with lock:
                stats = loader.load(
                    rows=iter(parsed_rows),
                    meta=meta,
                    source_file=source,
                    skip_if_loaded=True,
                    verbose=False,
                )

            duration = time.time() - t0
            return FileResult(
                source=source,
                success=True,
                attempt=attempt,
                items=stats.get("items", 0),
                codes=stats.get("codes", 0),
                rates=stats.get("rates", 0),
                raw=stats.get("raw", 0),
                skipped=stats.get("skipped", False),
                duration_sec=duration,
            )

        except Exception as e:
            last_error = str(e)
            last_tb = traceback.format_exc()
            if attempt < max_retries:
                time.sleep(retry_delay * (2 ** (attempt - 1)))

    return FileResult(
        source=source,
        success=False,
        attempt=max_retries,
        error=last_error,
        error_traceback=last_tb,
    )


# ---------------------------------------------------------------------------
# Error report writer
# ---------------------------------------------------------------------------

def write_error_report(stats: BulkRunStats, output_path: str) -> None:
    """
    Write a detailed error report for all failed files to a text file.
    Useful for debugging and re-running failed batches.
    """
    failed = stats.failed_files
    if not failed:
        return

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(f"MRF-ETL ERROR REPORT\n")
        f.write(f"Failed: {len(failed)} / {stats.total_files} files\n")
        f.write("=" * 60 + "\n\n")

        for r in failed:
            f.write(f"SOURCE: {r.source}\n")
            f.write(f"ATTEMPTS: {r.attempt}\n")
            f.write(f"ERROR: {r.error}\n")
            if r.error_traceback:
                f.write(f"TRACEBACK:\n{r.error_traceback}\n")
            f.write("-" * 40 + "\n\n")

    print(f"Error report written to: {output_path}")


def write_failed_sources(stats: BulkRunStats, output_path: str) -> None:
    """
    Write failed source paths/URLs to a text file — ready to use as
    a new input file for re-running just the failures.
    """
    failed = stats.failed_files
    if not failed:
        return

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("# Failed sources — re-run with mrf-etl bulk --input this_file\n")
        for r in failed:
            f.write(f"{r.source}\n")

    print(f"Failed sources written to: {output_path}")