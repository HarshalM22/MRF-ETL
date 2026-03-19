"""
test_phase3.py
Phase 3 test suite — CSV loader end-to-end against all 5 real MRF files.

Tests:
  - CSVLoader produces all 5 output files
  - Row counts are correct across all tables
  - FK integrity (item_id references, hospital_id references)
  - Idempotency (loading same file twice doesn't duplicate rows)
  - All edge-case fields preserved (footnote, extra_fields, rate_flag etc.)
  - base_loader conversion logic (_mrf_row_to_item_dict)
"""

import sys
import os
import csv
import shutil
import tempfile

sys.path.insert(0, "/home/claude/mrf_etl")

from mrf_etl import parse_file, parse_metadata, CSVLoader
from mrf_etl.loaders.base_loader import _mrf_row_to_item_dict, _json_safe

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
print("MRF-ETL PHASE 3 TEST SUITE")
print("="*65)


# ════════════════════════════════════════════════════════════════════
# UNIT — base_loader conversion
# ════════════════════════════════════════════════════════════════════
print("\n── Base Loader Unit Tests ──────────────────────────────────────")

rows_sample = list(parse_file(FILES["Chesapeake"], max_rows=5))
check("parse_file yielded rows", len(rows_sample) == 5)

r = rows_sample[0]
d = _mrf_row_to_item_dict(r, FILES["Chesapeake"])

check("item dict has description",      "description" in d)
check("item dict has gross_charge",     "gross_charge" in d)
check("item dict has _codes",           "_codes" in d)
check("item dict has _rates",           "_rates" in d)
check("item dict has _raw",             "_raw" in d)
check("_raw has raw_row key",           "raw_row" in d["_raw"])
check("_raw raw_row is JSON string",    isinstance(d["_raw"]["raw_row"], str))

if d["_codes"]:
    c = d["_codes"][0]
    check("code dict has code",         "code" in c)
    check("code dict has code_type",    "code_type" in c)
    check("code dict has is_primary",   "is_primary" in c)

# _json_safe
check("json_safe dict",         _json_safe({"a": 1}) == '{"a": 1}')
check("json_safe list",         _json_safe([1, 2]) == '[1, 2]')
check("json_safe None",         _json_safe(None) is None)
check("json_safe empty dict",   _json_safe({}) is None)


# ════════════════════════════════════════════════════════════════════
# INTEGRATION — CSVLoader per file
# ════════════════════════════════════════════════════════════════════

for name, path in FILES.items():
    print(f"\n── CSVLoader: {name} ──────────────────────────────────────────")

    tmpdir = tempfile.mkdtemp()
    try:
        loader = CSVLoader(output_dir=tmpdir, chunk_size=200)
        meta   = parse_metadata(path)
        rows   = parse_file(path, max_rows=100)
        stats  = loader.load(rows, meta, source_file=path, verbose=False)
        loader.close()

        # Check output files exist
        for fname in ["mrf_hospitals.csv", "mrf_items.csv",
                      "mrf_item_codes.csv", "mrf_rates.csv", "mrf_raw.csv"]:
            fpath = os.path.join(tmpdir, fname)
            check(f"{fname} exists", os.path.exists(fpath))

        # Read output
        hospitals = read_csv(os.path.join(tmpdir, "mrf_hospitals.csv"))
        items     = read_csv(os.path.join(tmpdir, "mrf_items.csv"))
        codes     = read_csv(os.path.join(tmpdir, "mrf_item_codes.csv"))
        rates     = read_csv(os.path.join(tmpdir, "mrf_rates.csv"))
        raw       = read_csv(os.path.join(tmpdir, "mrf_raw.csv"))

        # Basic counts
        check("1 hospital row",            len(hospitals) == 1, got=len(hospitals))
        check("items > 0",                 len(items) > 0, got=len(items))
        check("stats items match csv",     stats["items"] == len(items),
              got=(stats["items"], len(items)))
        check("stats codes match csv",     stats["codes"] == len(codes),
              got=(stats["codes"], len(codes)))
        check("stats rates match csv",     stats["rates"] == len(rates),
              got=(stats["rates"], len(rates)))
        check("raw rows == items",         len(raw) == len(items),
              got=(len(raw), len(items)))

        # Hospital fields
        h = hospitals[0]
        check("hospital_name present",     bool(h["hospital_name"]), got=h["hospital_name"])
        check("license_state present",     bool(h["license_state"]), got=h["license_state"])
        check("source_file_hash set",      len(h["source_file_hash"]) == 64)

        # Item fields
        it = items[0]
        check("item hospital_id set",      it["hospital_id"] == "1")
        check("item description set",      bool(it["description"]), got=it["description"][:40])
        check("item layout_type set",      bool(it["layout_type"]), got=it["layout_type"])

        # FK integrity — all item_ids in codes/rates/raw exist in items
        item_ids = {r["id"] for r in items}
        if codes:
            orphan_codes = [r for r in codes if r["item_id"] not in item_ids]
            check("no orphan codes",       len(orphan_codes) == 0, got=len(orphan_codes))
        if rates:
            orphan_rates = [r for r in rates if r["item_id"] not in item_ids]
            check("no orphan rates",       len(orphan_rates) == 0, got=len(orphan_rates))
        if raw:
            orphan_raw = [r for r in raw if r["item_id"] not in item_ids]
            check("no orphan raw",         len(orphan_raw) == 0, got=len(orphan_raw))

        # File-specific checks
        if name == "Marshall":
            # Horizontal — every item should have rates
            items_with_rates = set(r["item_id"] for r in rates)
            check("Marshall: has rate rows",   len(rates) > 0, got=len(rates))
            check("Marshall: layout=horizontal", items[0]["layout_type"] == "horizontal")
            # Payer names in rates
            payer_names = {r["payer_name_raw"] for r in rates}
            check("Marshall: multiple payers", len(payer_names) > 5, got=len(payer_names))
            # Plan tier index preserved
            tier_rates = [r for r in rates if int(r["plan_tier_index"]) > 0]
            check("Marshall: tier index > 0 exists", len(tier_rates) > 0)

        if name == "Chesapeake":
            # Multi-code rows
            items_with_3codes = {}
            for c in codes:
                iid = c["item_id"]
                items_with_3codes[iid] = items_with_3codes.get(iid, 0) + 1
            max_codes = max(items_with_3codes.values()) if items_with_3codes else 0
            check("Chesapeake: 3+ codes per item", max_codes >= 3, got=max_codes)
            # NDC codes
            ndc = [c for c in codes if c["code_type"] == "NDC"]
            check("Chesapeake: NDC codes",     len(ndc) > 0, got=len(ndc))
            # code_original for scientific notation
            has_original = [c for c in codes if c.get("code_original")]
            # CDM codes may or may not trigger sci notation in first 100 rows
            check("Chesapeake: code_original col exists", "code_original" in codes[0])

        if name == "Whitfield":
            # footnote preserved
            footnoted = [r for r in items if r.get("footnote")]
            check("Whitfield: footnote preserved",   len(footnoted) > 0, got=len(footnoted))
            # count_compared_rates
            ccr = [r for r in items if r.get("count_compared_rates")]
            check("Whitfield: count_compared_rates", len(ccr) > 0, got=len(ccr))
            # per_diem methodology
            perdiem = [r for r in rates if r.get("methodology") == "per_diem"]
            check("Whitfield: per_diem in rates",    len(perdiem) > 0, got=len(perdiem))

        if name == "Cordova":
            # Percentage rates
            pct_rates = [r for r in rates if r.get("negotiated_percentage")]
            check("Cordova: percentage rates",       len(pct_rates) > 0, got=len(pct_rates))
            # setting=both
            both_items = [r for r in items if r.get("setting") == "both"]
            check("Cordova: setting=both",           len(both_items) > 0, got=len(both_items))

        if name == "NorthAlabama":
            # Vertical: rate per row
            check("NorthAlabama: has rates",         len(rates) > 0, got=len(rates))
            check("NorthAlabama: layout=vertical",   items[0]["layout_type"] == "vertical")
            payers = {r["payer_name_raw"] for r in rates if r["payer_name_raw"]}
            check("NorthAlabama: named payers",      len(payers) > 1, got=len(payers))

        print(f"  [STATS] items={stats['items']} codes={stats['codes']} "
              f"rates={stats['rates']} raw={stats['raw']}")

        # ── IDEMPOTENCY TEST ─────────────────────────────────────
        print(f"  [IDEMPOTENCY] Loading same file twice...")
        loader2 = CSVLoader(output_dir=tmpdir, chunk_size=200)
        rows2   = parse_file(path, max_rows=100)
        stats2  = loader2.load(rows2, meta, source_file=path,
                               skip_if_loaded=True, verbose=False)
        loader2.close()

        hospitals2 = read_csv(os.path.join(tmpdir, "mrf_hospitals.csv"))
        items2     = read_csv(os.path.join(tmpdir, "mrf_items.csv"))
        check("idempotency: skipped=True",     stats2["skipped"] is True)
        check("idempotency: hospital count",   len(hospitals2) == 1, got=len(hospitals2))
        check("idempotency: item count same",  len(items2) == len(items),
              got=(len(items2), len(items)))

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ════════════════════════════════════════════════════════════════════
# MULTI-FILE LOAD TEST
# ════════════════════════════════════════════════════════════════════
print("\n── Multi-file load (all 5 hospitals into one output dir) ───────")
tmpdir = tempfile.mkdtemp()
try:
    loader = CSVLoader(output_dir=tmpdir, chunk_size=300)
    total_items = 0
    for name, path in FILES.items():
        meta = parse_metadata(path)
        rows = parse_file(path, max_rows=50)
        stats = loader.load(rows, meta, source_file=path, verbose=False)
        total_items += stats["items"]
        print(f"  Loaded {name}: {stats['items']} items")
    loader.close()

    hospitals = read_csv(os.path.join(tmpdir, "mrf_hospitals.csv"))
    items     = read_csv(os.path.join(tmpdir, "mrf_items.csv"))
    check("5 hospital rows",               len(hospitals) == 5, got=len(hospitals))
    check("total items correct",           len(items) == total_items,
          got=(len(items), total_items))

    # All hospital_ids in items reference a valid hospital
    hosp_ids = {h["id"] for h in hospitals}
    bad_items = [r for r in items if r["hospital_id"] not in hosp_ids]
    check("all items have valid hospital_id", len(bad_items) == 0, got=len(bad_items))

    print(f"  [TOTAL] hospitals=5 items={len(items)}")
finally:
    shutil.rmtree(tmpdir, ignore_errors=True)


# ════════════════════════════════════════════════════════════════════
# RESULTS
# ════════════════════════════════════════════════════════════════════
print("\n" + "="*65)
print(f"RESULTS: {PASS} passed  |  {FAIL} failed  |  {PASS+FAIL} total")
print("="*65)
if FAIL == 0:
    print("✓ ALL TESTS PASSED — Phase 3 complete")
else:
    print(f"✗ {FAIL} test(s) failed")