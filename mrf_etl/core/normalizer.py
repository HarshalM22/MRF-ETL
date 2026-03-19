"""
normalizer.py
All value-level cleaning and normalization for mrf-etl Phase 2.

Handles every edge case discovered across 8 real hospital MRF files:
  - Scientific notation in code fields (2.70E+11 → "270000000000")
  - Text sentinel values in rate fields ("Not paid by the payer plan" → None + flag)
  - Numeric cleaning ($12,000.00 → 12000.0, 85% → 85.0)
  - Methodology normalization (case variants → standard enum)
  - Setting normalization (mixed case → lowercase)
  - HTML entity decoding (&amp; → &)
  - Date normalization (M/D/YYYY → YYYY-MM-DD)
  - Sentinel numeric values (999999999 → None)
  - Code type inference when type column is empty
  - Setting extraction from payer name (Blue_Cross_Inpatient → inpatient)
"""

from __future__ import annotations

import html
import re
from typing import Optional


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Sentinel numeric value used by some hospitals (Cordova) to mean "unknown"
_NUMERIC_SENTINELS = frozenset({"999999999", "9999999999", "99999999"})

# Text values that appear in rate fields meaning "not applicable"
# Maps lowercase text → rate_flag value
_RATE_TEXT_SENTINELS: dict[str, str] = {
    "not paid by the payer plan":           "not_covered",
    "not paid by payer":                    "not_covered",
    "not covered":                          "not_covered",
    "non-covered":                          "not_covered",
    "not medicare reimbursable":            "not_reimbursable",
    "not reimbursable":                     "not_reimbursable",
    "service not payable":                  "not_payable",
    "not payable":                          "not_payable",
    "n/a":                                  "not_applicable",
    "na":                                   "not_applicable",
    "see contract":                         "see_contract",
    "bundled":                              "bundled",
    "included":                             "bundled",
    "package":                              "bundled",
    "packaged":                             "bundled",
    "not separately reimbursable":          "bundled",
    "not separately billable":              "bundled",
}

# Methodology normalization: any casing variant → standard enum
_METHODOLOGY_NORM: dict[str, str] = {
    "fee schedule":                         "fee_schedule",
    "fee_schedule":                         "fee_schedule",
    "feeschedule":                          "fee_schedule",
    "fs":                                   "fee_schedule",
    "per diem":                             "per_diem",
    "per_diem":                             "per_diem",
    "perdiem":                              "per_diem",
    "percent of total billed charges":      "percent_of_billed",
    "percent of billed charges":            "percent_of_billed",
    "percentage of billed charges":         "percent_of_billed",
    "% of total billed charges":            "percent_of_billed",
    "percent of total charges":             "percent_of_billed",
    "percentage":                           "percent_of_billed",
    "case rate":                            "case_rate",
    "case_rate":                            "case_rate",
    "carve out":                            "carve_out",
    "carve_out":                            "carve_out",
    "other":                                "other",
    "bundled":                              "bundled",
}

# Setting normalization
_SETTING_NORM: dict[str, str] = {
    "inpatient":    "inpatient",
    "outpatient":   "outpatient",
    "both":         "both",
    "inp":          "inpatient",
    "outp":         "outpatient",
    "i/p":          "inpatient",
    "o/p":          "outpatient",
}

# Billing class normalization
_BILLING_CLASS_NORM: dict[str, str] = {
    "facility":         "facility",
    "professional":     "professional",
    "prof":             "professional",
    "fac":              "facility",
    "institutional":    "facility",
}

# Setting keywords embedded in payer names (Marshall pattern)
# Checked in order — first match wins
_PAYER_SETTING_KEYWORDS: list[tuple[str, str]] = [
    ("inpatient",   "inpatient"),
    ("outpatient",  "outpatient"),
    ("ambulance",   "outpatient"),
    ("lab_only",    "outpatient"),
    ("recurring",   "outpatient"),
    ("day_surgery", "outpatient"),
    ("reference_lab","outpatient"),
]

# Code type inference by value pattern
_CODE_TYPE_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"^\d{5}$"),                        "CPT"),       # 5-digit numeric
    (re.compile(r"^[A-Z]\d{4}$"),                   "HCPCS"),     # letter + 4 digits
    (re.compile(r"^[A-Z]\d{3,4}$"),                 "HCPCS"),     # HCPCS variant
    (re.compile(r"^\d{3}$"),                        "RC"),        # 3-digit revenue code
    (re.compile(r"^\d{4}$"),                        "MS-DRG"),    # 3-4 digit DRG (with leading 0)
    (re.compile(r"^\d{2}-\d{3}-\d{2}-\d{2}$"),     "NDC"),       # NDC format
    (re.compile(r"^\d{5}-\d{4}-\d{2}$"),            "NDC"),       # NDC 11-digit
    (re.compile(r"^\d{11}$"),                       "NDC"),       # NDC plain
    (re.compile(r"^[A-Z0-9]+-[A-Z0-9]+"),           "CDM"),       # hyphenated internal ID
    (re.compile(r"^\d{10,}$"),                      "CDM"),       # long numeric CDM
]

# Scientific notation pattern
_SCI_NOTATION_RE = re.compile(r"^[+-]?\d+\.?\d*[eE][+-]?\d+$")

# Date patterns
_DATE_MDY_RE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")
_DATE_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


# ---------------------------------------------------------------------------
# Core value cleaners
# ---------------------------------------------------------------------------

def clean_str(value: str) -> Optional[str]:
    """Strip whitespace and decode HTML entities. Return None if empty."""
    if not value:
        return None
    v = html.unescape(value.strip())
    return v if v else None


def clean_numeric(value: str) -> Optional[float]:
    """
    Parse a numeric value from a rate/charge field.
    Handles:
      - Plain floats: "12000.00"
      - Currency: "$12,000.00"
      - Percentage: "85%" → 85.0 (percentage symbol stripped, caller interprets)
      - Sentinel: "999999999" → None
      - Empty → None
      - Text sentinels → None (caller should use check_rate_sentinel first)
    """
    if not value:
        return None
    v = value.strip()
    if not v:
        return None

    # Check numeric sentinel
    if v in _NUMERIC_SENTINELS:
        return None

    # Strip currency, commas, percentage
    v = v.replace("$", "").replace(",", "").replace("%", "").strip()

    try:
        return float(v)
    except ValueError:
        return None


def check_rate_sentinel(value: str) -> tuple[Optional[str], Optional[str]]:
    """
    Check if a rate field contains a text sentinel value.

    Returns:
        (rate_flag, rate_note) if sentinel detected
        (None, None) if not a sentinel — caller should parse numerically
    """
    if not value:
        return None, None
    v = value.strip().lower()
    if not v:
        return None, None

    # Exact match
    if v in _RATE_TEXT_SENTINELS:
        return _RATE_TEXT_SENTINELS[v], value.strip()

    # Partial match — check if any sentinel phrase is contained
    for phrase, flag in _RATE_TEXT_SENTINELS.items():
        if phrase in v:
            return flag, value.strip()

    return None, None


def normalize_methodology(value: str) -> tuple[Optional[str], Optional[str]]:
    """
    Normalize a methodology value.

    Returns:
        (normalized_enum, raw_original)
        normalized_enum is None if value is empty or unrecognized
    """
    if not value:
        return None, None
    raw = value.strip()
    norm = _METHODOLOGY_NORM.get(raw.lower())
    return norm, raw if raw else None


def normalize_setting(value: str) -> Optional[str]:
    """Normalize setting to lowercase standard enum."""
    if not value:
        return None
    return _SETTING_NORM.get(value.strip().lower(), value.strip().lower() or None)


def normalize_billing_class(value: str) -> Optional[str]:
    """Normalize billing_class to lowercase standard enum."""
    if not value:
        return None
    return _BILLING_CLASS_NORM.get(value.strip().lower(), value.strip().lower() or None)


def normalize_date(value: str) -> Optional[str]:
    """
    Normalize date to ISO 8601 (YYYY-MM-DD).
    Handles M/D/YYYY, MM/DD/YYYY, YYYY-MM-DD.
    Returns raw string if format unrecognized (don't lose data).
    """
    if not value:
        return None
    v = value.strip()
    if not v:
        return None

    # Already ISO
    if _DATE_ISO_RE.match(v):
        return v

    # M/D/YYYY or MM/DD/YYYY
    m = _DATE_MDY_RE.match(v)
    if m:
        month, day, year = m.groups()
        return f"{year}-{int(month):02d}-{int(day):02d}"

    return v  # return as-is rather than None — don't lose it


# ---------------------------------------------------------------------------
# Code normalization
# ---------------------------------------------------------------------------

def normalize_code(raw_code: str) -> tuple[str, Optional[str]]:
    """
    Normalize a billing code value.
    Handles scientific notation corruption from Excel exports.

    Returns:
        (normalized_code, original_code_if_changed)
        If no change: (code, None)
        If changed:   (corrected_code, original_raw)
    """
    if not raw_code:
        return raw_code, None

    v = raw_code.strip()

    # Detect and fix scientific notation (Excel corruption)
    if _SCI_NOTATION_RE.match(v):
        try:
            corrected = str(int(float(v)))
            return corrected, v  # return (fixed, original)
        except (ValueError, OverflowError):
            pass

    return v, None  # no change


def infer_code_type(code: str) -> Optional[str]:
    """
    Infer code type from the code value pattern when type column is empty.
    Returns None if no pattern matches — don't guess wrong.
    """
    if not code:
        return None
    v = code.strip().upper()
    for pattern, code_type in _CODE_TYPE_PATTERNS:
        if pattern.match(v):
            return code_type
    return None


# ---------------------------------------------------------------------------
# Payer name helpers
# ---------------------------------------------------------------------------

def extract_setting_from_payer(payer_name: str) -> Optional[str]:
    """
    Detect if a setting is embedded in a payer name (Marshall pattern).
    e.g. "Blue_Cross_Inpatient" → "inpatient"
         "Blue_Advantage_Ambulance" → "outpatient"
         "Blue_Cross_Outpatient" → "outpatient"
    Returns None if no setting keyword found.
    """
    if not payer_name:
        return None
    lower = payer_name.lower()
    for keyword, setting in _PAYER_SETTING_KEYWORDS:
        if keyword in lower:
            return setting
    return None


def normalize_payer_name(raw: str) -> str:
    """
    Light normalization of payer name — strip only.
    We preserve raw names for fidelity; heavy normalization is downstream.
    """
    return raw.strip()