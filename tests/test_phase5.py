"""
test_phase5.py
Phase 5 test suite — bulk runner, progress tracking, retry, CLI.

Tests:
  - read_sources() parses input files correctly
  - run_bulk() processes multiple files in parallel
  - Idempotency works across a bulk run
  - Failed files captured without stopping the run
  - write_error_report() and write_failed_sources()
  - CLI: inspect, parse, bulk commands
  - Thread safety for CSVLoader
  - BulkRunStats aggregation
"""

import sys
import os
import csv
import shutil
import tempfile
import subprocess

sys.path.insert(0, "/home/claude/mrf_etl")

from mrf_etl.core.bulk_runner import (
    run_bulk, read_sources, write_error_report,
    write_failed_sources, FileResult, BulkRunStats,
)
from mrf_etl.loaders.csv_loader import CSVLoader

FILES = {
    "Marshall":     "/mnt/user-data/uploads/83-1651180_Marshall-Medical-Center-North_standardcharges.csv",
    "Chesapeake":   "/mnt/user-data/uploads/237424835_chesapeake-hospital-llc_standardcharges.csv",
    "NorthAlabama": "/mnt/user-data/uploads/272451336_north-alabama-medical-center_standardcharges.csv",
    "Whitfield":    "/mnt/user-data/uploads/636002343_whitfield-regional-hospital_standardcharges.csv",
    "Cordova":      "/mnt/user-data/uploads/920139171_Cordova-Community-Medical-Center_standardcharges.csv",
}

PASS = 0
FAIL = 0

def check(label: str, condition: bool, got=None):
    global PASS, FAIL
    if condition:
        print(f"  ✓ {label}")
        PASS += 1
    else:
        print(f"  ✗ {label}   GOT: {got!r}")
        FAIL += 1

def read_csv(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))

print("\n" + "="*65)
print("MRF-ETL PHASE 5 TEST SUITE")
print("="*65)


# ════════════════════════════════════════════════════════════════════
# UNIT — read_sources
# ════════════════════════════════════════════════════════════════════
print("\n── read_sources() ──────────────────────────────────────────────")

tmpdir = tempfile.mkdtemp()
try:
    src_file = os.path.join(tmpdir, "sources.txt")
    with open(src_file, "w") as f:
        f.write("# This is a comment\n")
        f.write("\n")
        f.write("  /path/to/file1.csv  \n")
        f.write("https://example.com/file2.csv\n")
        f.write("# Another comment\n")
        f.write("/path/to/file3.csv\n")

    sources = read_sources(src_file)
    check("3 sources read",             len(sources) == 3, got=len(sources))
    check("comments excluded",          all(not s.startswith("#") for s in sources))
    check("blank lines excluded",       all(s.strip() for s in sources))
    check("whitespace stripped",        sources[0] == "/path/to/file1.csv")
    check("URL preserved",              sources[1] == "https://example.com/file2.csv")

    # File not found
    try:
        read_sources("/nonexistent/path.txt")
        check("FileNotFoundError raised", False)
    except FileNotFoundError:
        check("FileNotFoundError raised", True)

finally:
    shutil.rmtree(tmpdir)


# ════════════════════════════════════════════════════════════════════
# UNIT — FileResult and BulkRunStats
# ════════════════════════════════════════════════════════════════════
print("\n── BulkRunStats ────────────────────────────────────────────────")

results = [
    FileResult(source="a.csv", success=True,  items=100, codes=200, rates=300, raw=100),
    FileResult(source="b.csv", success=True,  items=50,  codes=75,  rates=150, raw=50, skipped=True),
    FileResult(source="c.csv", success=False, error="Connection refused"),
    FileResult(source="d.csv", success=True,  items=80,  codes=160, rates=240, raw=80),
]
stats = BulkRunStats(
    total_files=4,
    succeeded=2,
    failed=1,
    skipped=1,
    total_items=230,
    total_codes=435,
    total_rates=690,
    total_raw=230,
    duration_sec=42.5,
    results=results,
)
check("failed_files count",     len(stats.failed_files) == 1)
check("failed_files source",    stats.failed_files[0].source == "c.csv")
check("succeeded_files count",  len(stats.succeeded_files) == 2)
check("summary contains totals", "230" in stats.summary())
check("summary contains failed", "c.csv" in stats.summary())


# ════════════════════════════════════════════════════════════════════
# INTEGRATION — run_bulk sequential (workers=1)
# ════════════════════════════════════════════════════════════════════
print("\n── run_bulk() sequential (workers=1) ──────────────────────────")

tmpdir = tempfile.mkdtemp()
try:
    sources = list(FILES.values())
    loader  = CSVLoader(output_dir=tmpdir, chunk_size=300)

    run_stats = run_bulk(
        sources=sources,
        loader=loader,
        workers=1,
        max_retries=1,
        verbose=True,
    )
    loader.close()

    check("all 5 succeeded",        run_stats.succeeded == 5, got=run_stats.succeeded)
    check("0 failed",               run_stats.failed == 0, got=run_stats.failed)
    check("total_items > 0",        run_stats.total_items > 0, got=run_stats.total_items)
    check("total_rates > 0",        run_stats.total_rates > 0, got=run_stats.total_rates)
    check("duration > 0",           run_stats.duration_sec > 0)

    hospitals = read_csv(os.path.join(tmpdir, "mrf_hospitals.csv"))
    items     = read_csv(os.path.join(tmpdir, "mrf_items.csv"))
    check("5 hospital rows",        len(hospitals) == 5, got=len(hospitals))
    check("items loaded",           len(items) > 0)

    print(f"  [STATS] {run_stats.total_items} items | "
          f"{run_stats.total_rates} rates | "
          f"{run_stats.duration_sec:.1f}s")
finally:
    shutil.rmtree(tmpdir)


# ════════════════════════════════════════════════════════════════════
# INTEGRATION — run_bulk parallel (workers=3)
# ════════════════════════════════════════════════════════════════════
print("\n── run_bulk() parallel (workers=3) ────────────────────────────")

tmpdir = tempfile.mkdtemp()
try:
    sources = list(FILES.values())
    loader  = CSVLoader(output_dir=tmpdir, chunk_size=300)

    run_stats = run_bulk(
        sources=sources,
        loader=loader,
        workers=3,
        max_retries=1,
        verbose=False,
    )
    loader.close()

    check("all 5 succeeded (parallel)", run_stats.succeeded == 5, got=run_stats.succeeded)
    check("0 failed (parallel)",        run_stats.failed == 0, got=run_stats.failed)

    hospitals = read_csv(os.path.join(tmpdir, "mrf_hospitals.csv"))
    items     = read_csv(os.path.join(tmpdir, "mrf_items.csv"))
    check("5 hospital rows (parallel)", len(hospitals) == 5, got=len(hospitals))

    # FK integrity after parallel load
    item_ids = {r["id"] for r in items}
    codes = read_csv(os.path.join(tmpdir, "mrf_item_codes.csv"))
    rates = read_csv(os.path.join(tmpdir, "mrf_rates.csv"))
    orphan_codes = [r for r in codes if r["item_id"] not in item_ids]
    orphan_rates = [r for r in rates if r["item_id"] not in item_ids]
    check("no orphan codes (parallel)", len(orphan_codes) == 0, got=len(orphan_codes))
    check("no orphan rates (parallel)", len(orphan_rates) == 0, got=len(orphan_rates))

    print(f"  [PARALLEL] {run_stats.total_items} items in {run_stats.duration_sec:.1f}s")
finally:
    shutil.rmtree(tmpdir)


# ════════════════════════════════════════════════════════════════════
# IDEMPOTENCY — bulk re-run skips already loaded files
# ════════════════════════════════════════════════════════════════════
print("\n── Idempotency across bulk re-run ──────────────────────────────")

tmpdir = tempfile.mkdtemp()
try:
    sources = [FILES["NorthAlabama"], FILES["Cordova"]]
    loader  = CSVLoader(output_dir=tmpdir, chunk_size=300)

    # First run
    stats1 = run_bulk(sources=sources, loader=loader, workers=1, verbose=False)
    items1 = read_csv(os.path.join(tmpdir, "mrf_items.csv"))

    # Second run — same files, should skip
    stats2 = run_bulk(sources=sources, loader=loader, workers=1, verbose=False)
    loader.close()

    items2 = read_csv(os.path.join(tmpdir, "mrf_items.csv"))
    check("second run: all skipped",    stats2.skipped == 2, got=stats2.skipped)
    check("second run: 0 succeeded",    stats2.succeeded == 0)
    check("item count unchanged",       len(items2) == len(items1),
          got=(len(items2), len(items1)))
    hospitals2 = read_csv(os.path.join(tmpdir, "mrf_hospitals.csv"))
    check("hospital count unchanged",   len(hospitals2) == 2, got=len(hospitals2))
finally:
    shutil.rmtree(tmpdir)


# ════════════════════════════════════════════════════════════════════
# ERROR HANDLING — bad file in bulk run doesn't stop others
# ════════════════════════════════════════════════════════════════════
print("\n── Error isolation in bulk run ─────────────────────────────────")

tmpdir = tempfile.mkdtemp()
try:
    sources = [
        FILES["Cordova"],
        "/nonexistent/bad_file.csv",   # will fail
        FILES["NorthAlabama"],
    ]
    loader = CSVLoader(output_dir=tmpdir, chunk_size=300)

    run_stats = run_bulk(
        sources=sources,
        loader=loader,
        workers=1,
        max_retries=1,
        verbose=False,
    )
    loader.close()

    check("2 succeeded",            run_stats.succeeded == 2, got=run_stats.succeeded)
    check("1 failed",               run_stats.failed == 1, got=run_stats.failed)
    check("failed source captured", run_stats.failed_files[0].source == "/nonexistent/bad_file.csv")
    check("error message set",      bool(run_stats.failed_files[0].error))

    hospitals = read_csv(os.path.join(tmpdir, "mrf_hospitals.csv"))
    check("2 hospitals loaded",     len(hospitals) == 2, got=len(hospitals))

    # write_error_report
    report_path = os.path.join(tmpdir, "errors.txt")
    write_error_report(run_stats, report_path)
    check("error report written",   os.path.exists(report_path))
    with open(report_path) as f:
        content = f.read()
    check("error report has source",    "bad_file.csv" in content)

    # write_failed_sources
    failed_path = os.path.join(tmpdir, "failed.txt")
    write_failed_sources(run_stats, failed_path)
    check("failed sources written",     os.path.exists(failed_path))
    failed_sources = read_sources(failed_path)
    check("failed sources has 1 entry", len(failed_sources) == 1, got=failed_sources)
    check("failed sources correct path", "bad_file.csv" in failed_sources[0])

finally:
    shutil.rmtree(tmpdir)


# ════════════════════════════════════════════════════════════════════
# CLI — inspect command
# ════════════════════════════════════════════════════════════════════
print("\n── CLI: inspect ────────────────────────────────────────────────")

result = subprocess.run(
    [sys.executable, "-m", "mrf_etl.cli", "inspect",
     "--input", FILES["Cordova"]],
    capture_output=True, text=True,
    cwd="/home/claude/mrf_etl",
)
check("inspect exit code 0",    result.returncode == 0, got=result.returncode)
check("inspect shows hospital", "Cordova" in result.stdout, got=result.stdout[:200])
check("inspect shows layout",   "horizontal" in result.stdout)
check("inspect shows payers",   "Payer" in result.stdout or "payer" in result.stdout)


# ════════════════════════════════════════════════════════════════════
# CLI — parse command
# ════════════════════════════════════════════════════════════════════
print("\n── CLI: parse ──────────────────────────────────────────────────")

tmpdir = tempfile.mkdtemp()
try:
    result = subprocess.run(
        [sys.executable, "-m", "mrf_etl.cli", "parse",
         "--input", FILES["NorthAlabama"],
         "--output", "csv",
         "--out-dir", tmpdir],
        capture_output=True, text=True,
        cwd="/home/claude/mrf_etl",
    )
    check("parse exit code 0",      result.returncode == 0,
          got=(result.returncode, result.stderr[:200]))
    check("mrf_hospitals.csv created",
          os.path.exists(os.path.join(tmpdir, "mrf_hospitals.csv")))
    check("mrf_items.csv created",
          os.path.exists(os.path.join(tmpdir, "mrf_items.csv")))

    items = read_csv(os.path.join(tmpdir, "mrf_items.csv"))
    check("parse loaded items",     len(items) > 0, got=len(items))
    print(f"  [CLI parse] {len(items)} items loaded")
finally:
    shutil.rmtree(tmpdir)


# ════════════════════════════════════════════════════════════════════
# CLI — bulk command
# ════════════════════════════════════════════════════════════════════
print("\n── CLI: bulk ───────────────────────────────────────────────────")

tmpdir = tempfile.mkdtemp()
try:
    # Write sources file
    sources_file = os.path.join(tmpdir, "sources.txt")
    with open(sources_file, "w") as f:
        f.write("# MRF sources\n")
        for path in list(FILES.values())[:3]:  # first 3 only for speed
            f.write(f"{path}\n")

    out_dir = os.path.join(tmpdir, "output")
    result = subprocess.run(
        [sys.executable, "-m", "mrf_etl.cli", "bulk",
         "--input", sources_file,
         "--output", "csv",
         "--out-dir", out_dir,
         "--workers", "2"],
        capture_output=True, text=True,
        cwd="/home/claude/mrf_etl",
    )
    check("bulk exit code 0",       result.returncode == 0,
          got=(result.returncode, result.stderr[:300]))
    check("output dir created",     os.path.exists(out_dir))

    if os.path.exists(out_dir):
        hospitals = read_csv(os.path.join(out_dir, "mrf_hospitals.csv"))
        check("3 hospitals loaded", len(hospitals) == 3, got=len(hospitals))
        print(f"  [CLI bulk] {len(hospitals)} hospitals loaded via CLI")

    # CLI bulk with a bad file — should exit 1 but still load good ones
    bad_sources_file = os.path.join(tmpdir, "bad_sources.txt")
    with open(bad_sources_file, "w") as f:
        f.write(FILES["Cordova"] + "\n")
        f.write("/nonexistent/bad.csv\n")

    bad_out = os.path.join(tmpdir, "bad_output")
    result2 = subprocess.run(
        [sys.executable, "-m", "mrf_etl.cli", "bulk",
         "--input", bad_sources_file,
         "--output", "csv",
         "--out-dir", bad_out,
         "--workers", "1",
         "--max-retries", "1"],
        capture_output=True, text=True,
        cwd="/home/claude/mrf_etl",
    )
    check("bulk with failures exits 1", result2.returncode == 1,
          got=result2.returncode)
    if os.path.exists(bad_out):
        hospitals2 = read_csv(os.path.join(bad_out, "mrf_hospitals.csv"))
        check("good file still loaded",  len(hospitals2) == 1, got=len(hospitals2))

finally:
    shutil.rmtree(tmpdir)


# ════════════════════════════════════════════════════════════════════
# RESULTS
# ════════════════════════════════════════════════════════════════════
print("\n" + "="*65)
print(f"RESULTS: {PASS} passed  |  {FAIL} failed  |  {PASS+FAIL} total")
print("="*65)
if FAIL == 0:
    print("✓ ALL TESTS PASSED — Phase 5 complete")
else:
    print(f"✗ {FAIL} test(s) failed")