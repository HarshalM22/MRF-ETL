"""
Phase 1 tests — run against all 5 real MRF files.
Tests: ingester, meta_parser, layout_detector.
"""
import sys
sys.path.insert(0, "/home/claude/mrf_etl")

from mrf_etl.core.ingester import peek_rows, stream_rows, file_size_bytes
from mrf_etl.core.meta_parser import parse_metadata
from mrf_etl.core.layout_detector import detect_layout

FILES = {
    "Marshall":    "/mnt/user-data/uploads/83-1651180_Marshall-Medical-Center-North_standardcharges.csv",
    "Chesapeake":  "/mnt/user-data/uploads/237424835_chesapeake-hospital-llc_standardcharges.csv",
    "NorthAlabama":"/mnt/user-data/uploads/272451336_north-alabama-medical-center_standardcharges.csv",
    "Whitfield":   "/mnt/user-data/uploads/636002343_whitfield-regional-hospital_standardcharges.csv",
    "Cordova":     "/mnt/user-data/uploads/920139171_Cordova-Community-Medical-Center_standardcharges.csv",
}

PASS = 0
FAIL = 0

def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        print(f"  ✓ {name}")
        PASS += 1
    else:
        print(f"  ✗ {name}  {detail}")
        FAIL += 1

print("=" * 65)
print("PHASE 1 TEST SUITE — 5 Real MRF Files")
print("=" * 65)

for hosp, path in FILES.items():
    print(f"\n[{hosp}]")

    # --- Ingester ---
    rows = peek_rows(path, n=5)
    check("peek_rows returns 5 rows", len(rows) == 5)
    check("row 2 has description col",
          any("description" in c.lower() for c in rows[2]))

    size = file_size_bytes(path)
    check(f"file size > 0 ({size:,} bytes)", size > 0)

    # --- Meta parser ---
    meta = parse_metadata(path)
    check("hospital_name present", bool(meta.hospital_name),
          f"got: {meta.hospital_name}")
    check("hospital_locations list", isinstance(meta.hospital_locations, list))
    check("last_updated_on present", bool(meta.last_updated_on),
          f"got: {meta.last_updated_on}")
    check("cms_version present", bool(meta.cms_version),
          f"got: {meta.cms_version}")
    check("license_number present", bool(meta.license_number),
          f"got: {meta.license_number}")
    check("license_state present", bool(meta.license_state),
          f"got: {meta.license_state}")
    check("no CMS compliance text in name",
          meta.hospital_name is None or "45 CFR" not in meta.hospital_name)
    print(f"    hospital_name     = {meta.hospital_name}")
    print(f"    hospital_locations= {meta.hospital_locations}")
    print(f"    hospital_addresses= {meta.hospital_addresses}")
    print(f"    last_updated_on   = {meta.last_updated_on}")
    print(f"    cms_version       = {meta.cms_version}")
    print(f"    license_number    = {meta.license_number}")
    print(f"    license_state     = {meta.license_state}")
    if meta.extra_metadata:
        print(f"    extra_metadata    = {meta.extra_metadata}")

    # --- Layout detector ---
    layout = detect_layout(path)
    check("layout_type detected",
          layout.layout_type in ("horizontal", "vertical", "mixed", "unknown"),
          f"got: {layout.layout_type}")
    check("code_columns detected", len(layout.code_columns) > 0,
          f"got: {layout.code_columns}")
    check("gross_charge column found",
          "gross_charge" in layout.standard_field_map,
          f"map keys: {list(layout.standard_field_map.keys())[:10]}")

    print(f"    layout_type       = {layout.layout_type}")
    print(f"    code_columns      = {layout.code_columns}")
    print(f"    total headers     = {len(layout.headers)}")
    print(f"    standard_fields   = {list(layout.standard_field_map.keys())}")

    if layout.layout_type in ("horizontal", "mixed"):
        check("payer_plan_groups > 0", len(layout.payer_plan_groups) > 0,
              f"got: {len(layout.payer_plan_groups)}")
        print(f"    payer_plan_groups = {len(layout.payer_plan_groups)}")
        # Show first 3
        for g in layout.payer_plan_groups[:3]:
            print(f"      payer={g.payer_name_raw!r:40s} plan={g.plan_name_raw!r:20s} tier={g.plan_tier_index}")

    if layout.layout_type in ("vertical", "mixed"):
        check("idx_payer_name found", layout.idx_payer_name is not None,
              f"got: {layout.idx_payer_name}")
        print(f"    idx_payer_name    = {layout.idx_payer_name}")
        print(f"    idx_plan_name     = {layout.idx_plan_name}")

    if layout.unknown_columns:
        print(f"    unknown_columns   = {list(layout.unknown_columns.keys())[:5]}")

print(f"\n{'=' * 65}")
print(f"RESULTS: {PASS} passed  |  {FAIL} failed")
print(f"{'=' * 65}")