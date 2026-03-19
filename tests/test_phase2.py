"""
test_phase2.py
Phase 2 test suite — full row parsing against all 5 real hospital MRF files.

Tests:
  - Billing code extraction (all code types, multi-code, sparse rows)
  - Rate extraction (horizontal + vertical)
  - Value normalization (sentinels, scientific notation, methodology)
  - Field completeness (no dropped data)
  - Edge cases from real files
"""

import sys
sys.path.insert(0, "/home/claude/mrf_etl")

from mrf_etl.core.pipeline import parse_file, profile_file
from mrf_etl.core.normalizer import (
    clean_numeric, check_rate_sentinel, normalize_methodology,
    normalize_code, infer_code_type, extract_setting_from_payer,
)

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

print("\n" + "="*65)
print("MRF-ETL PHASE 2 TEST SUITE")
print("="*65)


# ════════════════════════════════════════════════════════════════════
# UNIT TESTS — normalizer.py
# ════════════════════════════════════════════════════════════════════
print("\n── Normalizer Unit Tests ───────────────────────────────────────")

# clean_numeric
check("plain float",            clean_numeric("12000.00") == 12000.0)
check("currency stripped",      clean_numeric("$12,000.00") == 12000.0)
check("percentage stripped",    clean_numeric("85%") == 85.0)
check("empty → None",           clean_numeric("") is None)
check("sentinel → None",        clean_numeric("999999999") is None)
check("zero kept",              clean_numeric("0.00") == 0.0)
check("text → None",            clean_numeric("Not covered") is None)

# check_rate_sentinel
flag, note = check_rate_sentinel("Not paid by the payer plan")
check("sentinel: not_covered",  flag == "not_covered")
check("sentinel note preserved",note == "Not paid by the payer plan")

flag, note = check_rate_sentinel("Not Medicare Reimbursable")
check("sentinel: not_reimbursable", flag == "not_reimbursable")

flag, note = check_rate_sentinel("service not payable")
check("sentinel: not_payable",  flag == "not_payable")

flag, note = check_rate_sentinel("12345.67")
check("numeric not sentinel",   flag is None)

# normalize_methodology
norm, raw = normalize_methodology("fee schedule")
check("methodology: fee schedule", norm == "fee_schedule")
norm, raw = normalize_methodology("Fee Schedule")
check("methodology: Fee Schedule", norm == "fee_schedule")
norm, raw = normalize_methodology("Percent of Total Billed Charges")
check("methodology: percent",   norm == "percent_of_billed")
norm, raw = normalize_methodology("Per Diem")
check("methodology: per diem",  norm == "per_diem")
norm, raw = normalize_methodology("other")
check("methodology: other",     norm == "other")
norm, raw = normalize_methodology("")
check("methodology: empty → None", norm is None)

# normalize_code — scientific notation
code, orig = normalize_code("2.70E+11")
check("sci notation fixed",     code == "270000000000", got=code)
check("sci notation original",  orig == "2.70E+11")
code, orig = normalize_code("44950")
check("normal code unchanged",  code == "44950" and orig is None)

# infer_code_type
check("CPT inference",          infer_code_type("44950") == "CPT")
check("HCPCS inference",        infer_code_type("C8924") == "HCPCS")
check("RC inference",           infer_code_type("120") == "RC")
check("MS-DRG inference",       infer_code_type("0034") == "MS-DRG", got=infer_code_type("0034"))

# extract_setting_from_payer
check("inpatient from payer",   extract_setting_from_payer("Blue_Cross_Inpatient") == "inpatient")
check("outpatient from payer",  extract_setting_from_payer("Blue_Cross_Outpatient") == "outpatient")
check("ambulance from payer",   extract_setting_from_payer("Blue_Advantage_Ambulance") == "outpatient")
check("no setting in payer",    extract_setting_from_payer("Aetna") is None)


# ════════════════════════════════════════════════════════════════════
# INTEGRATION TESTS — real files
# ════════════════════════════════════════════════════════════════════

# ── MARSHALL (horizontal, 504 cols, 1 code col, MS-DRG) ──────────
print("\n── Marshall Medical (HORIZONTAL) ───────────────────────────────")
rows = list(parse_file(FILES["Marshall"], max_rows=50))
check("parsed 50 rows",         len(rows) == 50, got=len(rows))

r0 = rows[0]
check("description present",    r0.description is not None, got=r0.description)
check("setting = inpatient",    r0.setting == "inpatient", got=r0.setting)
check("gross_charge numeric",   isinstance(r0.gross_charge, float), got=r0.gross_charge)
check("has billing codes",      len(r0.billing_codes) > 0, got=r0.billing_codes)
check("code type = MS-DRG",     r0.billing_codes[0].code_type == "MS-DRG",
                                got=r0.billing_codes[0].code_type)
check("is_primary = True",      r0.billing_codes[0].is_primary)
check("has rates",              len(r0.rates) > 0, got=len(r0.rates))
check("rates have payer names", all(rt.payer_name_raw for rt in r0.rates))
check("layout_source=horizontal", r0.rates[0].layout_source == "horizontal")
check("raw_row preserved",      bool(r0.raw_row))
check("row_number set",         r0.row_number > 0)

# Check setting_from_payer extraction on a payer with embedded setting
inpatient_rates = [rt for rt in r0.rates if rt.setting_from_payer == "inpatient"]
check("setting extracted from payer name", len(inpatient_rates) > 0,
      got=[rt.payer_name_raw for rt in r0.rates[:5]])

# Check tier index on BPR_1
tier_rates = [rt for rt in r0.rates if rt.plan_name_raw == "BPR" and rt.plan_tier_index == 1]
check("plan tier index 1 detected", len(tier_rates) > 0,
      got=[(rt.plan_name_raw, rt.plan_tier_index) for rt in r0.rates[:5]])

# Methodology present
has_method = any(rt.methodology for rt in r0.rates)
check("methodology normalized", has_method)

print(f"  [INFO] row0: desc={r0.description[:40]!r}, codes={len(r0.billing_codes)}, rates={len(r0.rates)}")


# ── CHESAPEAKE (vertical, 4 codes, CDM+HCPCS+RC+NDC) ─────────────
print("\n── Chesapeake Hospital (VERTICAL, 4 codes) ─────────────────────")
rows = list(parse_file(FILES["Chesapeake"], max_rows=100))
check("parsed rows",            len(rows) > 0, got=len(rows))

# Find a row with multiple codes
multi = [r for r in rows if len(r.billing_codes) >= 3]
check("rows with 3+ codes exist", len(multi) > 0, got=len(multi))

if multi:
    r = multi[0]
    types = {c.code_type for c in r.billing_codes}
    check("multiple code types", len(types) > 1, got=types)
    check("CDM code present",   "CDM" in types, got=types)
    check("RC code present",    "RC" in types, got=types)
    print(f"  [INFO] multi-code row: codes={[(c.code, c.code_type) for c in r.billing_codes]}")

# Find NDC row (drug with NDC code)
ndc_rows = [r for r in rows if any(c.code_type == "NDC" for c in r.billing_codes)]
check("NDC drug rows found",    len(ndc_rows) > 0, got=len(ndc_rows))
if ndc_rows:
    r = ndc_rows[0]
    check("drug unit present",  r.drug_unit_of_measure is not None, got=r.drug_unit_of_measure)
    print(f"  [INFO] NDC row: {r.description}, drug_unit={r.drug_unit_of_measure}")

# Vertical: rows without payer (chargemaster only)
no_rate_rows = [r for r in rows if len(r.rates) == 0]
no_rate_with_payer = [r for r in rows if len(r.rates) == 1 and not r.rates[0].payer_name_raw]
check("vertical rows parsed (some may have no payer)", len(rows) > 0)


# ── NORTH ALABAMA (vertical, full payer/plan data) ────────────────
print("\n── North Alabama Medical (VERTICAL, full rates) ────────────────")
rows = list(parse_file(FILES["NorthAlabama"], max_rows=100))
check("parsed rows",            len(rows) > 0)

payer_rows = [r for r in rows if r.rates and r.rates[0].payer_name_raw]
check("rows with payer data",   len(payer_rows) > 0, got=len(payer_rows))

if payer_rows:
    r = payer_rows[0]
    rt = r.rates[0]
    check("payer_name set",     bool(rt.payer_name_raw), got=rt.payer_name_raw)
    check("plan_name set",      bool(rt.plan_name_raw), got=rt.plan_name_raw)
    check("negotiated rate",    rt.negotiated_dollar is not None or
                                rt.negotiated_percentage is not None,
          got=(rt.negotiated_dollar, rt.negotiated_percentage))
    check("min_negotiated",     r.min_negotiated is not None, got=r.min_negotiated)
    check("max_negotiated",     r.max_negotiated is not None, got=r.max_negotiated)
    print(f"  [INFO] payer={rt.payer_name_raw!r} plan={rt.plan_name_raw!r} "
          f"rate=${rt.negotiated_dollar} pct={rt.negotiated_percentage}%")

# Methodology on vertical rows
has_method = any(
    r.rates and r.rates[0].methodology
    for r in payer_rows
)
check("methodology on vertical", has_method)


# ── WHITFIELD (vertical, 3 codes, footnote, count_compared) ───────
print("\n── Whitfield Regional (VERTICAL, 3 codes, extra fields) ────────")
rows = list(parse_file(FILES["Whitfield"], max_rows=100))
check("parsed rows",            len(rows) > 0)

# Check footnote preserved
foot_rows = [r for r in rows if r.footnote]
check("footnote rows found",    len(foot_rows) > 0, got=len(foot_rows))
if foot_rows:
    print(f"  [INFO] footnote sample: {foot_rows[0].footnote[:60]!r}...")

# Check count_compared_rates
ccr_rows = [r for r in rows if r.count_compared_rates is not None]
check("count_compared_rates", len(ccr_rows) > 0, got=len(ccr_rows))

# Check multi-code (CDM + RC)
multi = [r for r in rows if len(r.billing_codes) >= 2]
check("multi-code rows (CDM+RC)", len(multi) > 0, got=len(multi))

# Per diem methodology
perdiem = [r for r in rows
           if r.rates and any(rt.methodology == "per_diem" for rt in r.rates)]
check("per_diem methodology",   len(perdiem) > 0, got=len(perdiem))

# Percent of billed methodology
pct = [r for r in rows
       if r.rates and any(rt.methodology == "percent_of_billed" for rt in r.rates)]
check("percent_of_billed methodology", len(pct) > 0, got=len(pct))


# ── CORDOVA (horizontal, 2 codes, percentage rates) ───────────────
print("\n── Cordova Community (HORIZONTAL, percentage rates) ────────────")
rows = list(parse_file(FILES["Cordova"], max_rows=50))
check("parsed rows",            len(rows) > 0)

r0 = rows[0]
check("has 2 billing codes",    len(r0.billing_codes) == 2, got=len(r0.billing_codes))
code_types = {c.code_type for c in r0.billing_codes}
check("HCPCS + RC codes",       "HCPCS" in code_types and "RC" in code_types,
      got=code_types)

# Cordova uses negotiated_percentage not dollar
pct_rates = [rt for rt in r0.rates if rt.negotiated_percentage is not None]
check("percentage rates extracted", len(pct_rates) > 0, got=len(pct_rates))
check("pct value is numeric",   isinstance(pct_rates[0].negotiated_percentage, float),
      got=pct_rates[0].negotiated_percentage)

# Cordova Aetna = 90%
aetna = next((rt for rt in r0.rates if "Aetna" in rt.payer_name_raw), None)
check("Aetna rate = 90%",       aetna and aetna.negotiated_percentage == 90.0,
      got=aetna.negotiated_percentage if aetna else None)

# BCBS = 100%
bcbs = next((rt for rt in r0.rates if "Blue Cross" in rt.payer_name_raw), None)
check("BCBS rate = 100%",       bcbs and bcbs.negotiated_percentage == 100.0,
      got=bcbs.negotiated_percentage if bcbs else None)

check("setting = both",         r0.setting == "both", got=r0.setting)
check("raw_row preserved",      "description" in r0.raw_row)
print(f"  [INFO] rates: {[(rt.payer_name_raw, rt.negotiated_percentage) for rt in r0.rates]}")


# ── PROFILE STATS ─────────────────────────────────────────────────
print("\n── Profile Stats ───────────────────────────────────────────────")
for name, path in FILES.items():
    try:
        p = profile_file(path)
        print(f"  {name:15s} | layout={p['layout_type']:10s} | "
              f"payers={p['payer_plan_combos']:3d} | "
              f"codes/row={p['avg_codes_per_row']} | "
              f"rates/row={p['avg_rates_per_row']} | "
              f"code_types={p['code_types_seen']}")
    except Exception as e:
        print(f"  {name:15s} | ERROR: {e}")


# ════════════════════════════════════════════════════════════════════
# RESULTS
# ════════════════════════════════════════════════════════════════════
print("\n" + "="*65)
print(f"RESULTS: {PASS} passed  |  {FAIL} failed  |  {PASS+FAIL} total")
print("="*65)
if FAIL == 0:
    print("✓ ALL TESTS PASSED — Phase 2 complete")
else:
    print(f"✗ {FAIL} test(s) failed")