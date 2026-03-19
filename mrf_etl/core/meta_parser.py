"""
meta_parser.py
Parses rows 0 and 1 of an MRF CSV file into a HospitalMeta object.

MRF file structure (CMS standard):
  Row 0: metadata field names  (hospital_name, last_updated_on, ...)
  Row 1: metadata field values (Marshall Medical, 2024-11-20, ...)
  Row 2: data column headers
  Row 3+: data rows
"""

from __future__ import annotations
import re
from typing import Union
from pathlib import Path

from mrf_etl.schema.mrf_row import HospitalMeta
from mrf_etl.schema.key_aliases import normalize_metadata_key, is_cms_compliance_field
from mrf_etl.core.ingester import peek_rows


# Pipe variants used as multi-location separator
# Some files use "|", some use " | " (with spaces)
_PIPE_SEP = re.compile(r"\s*\|\s*")

# License number field pattern: license_number|<STATE>
_LICENSE_KEY_RE = re.compile(r"license[_\s]?number\s*\|?\s*([A-Z]{2})?", re.IGNORECASE)

# Date formats to normalize to ISO (YYYY-MM-DD)
_DATE_FORMATS = [
    r"(\d{4})-(\d{2})-(\d{2})",   # 2024-11-20
    r"(\d{1,2})/(\d{1,2})/(\d{4})",  # 9/17/2025 or 02/16/2026
    r"(\d{1,2})-(\d{1,2})-(\d{4})",  # 9-17-2025
]


def _normalize_date(raw: str) -> str | None:
    """Convert any observed date format to ISO YYYY-MM-DD."""
    raw = raw.strip()
    if not raw:
        return None

    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", raw)
    if m:
        return raw  # already ISO

    m = re.match(r"^(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})$", raw)
    if m:
        month, day, year = m.group(1), m.group(2), m.group(3)
        return f"{year}-{int(month):02d}-{int(day):02d}"

    return raw  # return as-is if unrecognized — don't drop it


def _split_pipe_value(value: str) -> list[str]:
    """
    Split a pipe-separated value into a list, strip whitespace from each.
    e.g. "United Hospital | Regina Campus" -> ["United Hospital", "Regina Campus"]
    Single value returns list with one item.
    """
    parts = _PIPE_SEP.split(value.strip())
    return [p.strip() for p in parts if p.strip()]


def _parse_license(key: str, value: str) -> tuple[str | None, str | None]:
    """
    Extract license number and state from key/value pair.

    Handles:
      key="license_number|AL", value="11842"        -> ("11842", "AL")
      key="license_number|AL", value="31473|AL"     -> ("31473", "AL")  [Whitfield]
      key="license_number|AK", value="GACH-010"     -> ("GACH-010", "AK")
    """
    # Extract state from key
    state = None
    m = re.search(r"\|([A-Z]{2})$", key.strip())
    if m:
        state = m.group(1)

    # Clean value — if value also contains |STATE, strip it
    val = value.strip()
    if state and val.endswith(f"|{state}"):
        val = val[: -(len(state) + 1)].strip()

    return (val if val else None), state


def parse_metadata(source: Union[str, "Path"]) -> HospitalMeta:
    """
    Read rows 0 and 1 from an MRF file and return a HospitalMeta.

    Robust against:
    - Leading/trailing spaces in keys or values
    - Variant key names (location_name vs hospital_location)
    - Pipe-separated multi-location values
    - License number with state embedded in key or value
    - Whitfield's as_of_date, South Peninsula's type_2_npi / attester_name
    - CMS compliance statement field (skipped)
    - Extra trailing empty columns common in real files
    """
    rows = peek_rows(source, n=2)
    if len(rows) < 2:
        return HospitalMeta(source_file=str(source))

    keys_row = rows[0]
    vals_row = rows[1]

    # Pad shorter row to match longer
    max_len = max(len(keys_row), len(vals_row))
    keys_row = keys_row + [""] * (max_len - len(keys_row))
    vals_row = vals_row + [""] * (max_len - len(vals_row))

    meta = HospitalMeta(source_file=str(source))

    for raw_key, raw_val in zip(keys_row, vals_row):
        raw_key = raw_key.strip()
        raw_val = raw_val.strip()

        # Skip empty keys
        if not raw_key:
            continue

        # Skip CMS compliance statement
        if is_cms_compliance_field(raw_key):
            continue

        # Handle license_number|STATE specially
        if re.match(r"license[_\s]?number", raw_key, re.IGNORECASE):
            lic_num, lic_state = _parse_license(raw_key, raw_val)
            if lic_num:
                meta.license_number = lic_num
            if lic_state:
                meta.license_state = lic_state
            continue

        # Normalize key name
        std_key = normalize_metadata_key(raw_key)

        if std_key == "hospital_name":
            meta.hospital_name = raw_val or None

        elif std_key == "hospital_location":
            if raw_val:
                meta.hospital_locations = _split_pipe_value(raw_val)

        elif std_key == "hospital_address":
            if raw_val:
                meta.hospital_addresses = _split_pipe_value(raw_val)

        elif std_key == "last_updated_on":
            meta.last_updated_on = _normalize_date(raw_val)

        elif std_key == "as_of_date":
            meta.as_of_date = _normalize_date(raw_val)

        elif std_key == "cms_version":
            meta.cms_version = raw_val or None

        elif std_key == "hospital_npi":
            meta.hospital_npi = raw_val or None

        elif std_key == "attester_name":
            meta.attester_name = raw_val or None

        elif std_key == "financial_aid_policy":
            meta.financial_aid_policy = raw_val or None

        else:
            # Any unrecognized field goes into extra_metadata
            if raw_val:
                meta.extra_metadata[std_key] = raw_val

    return meta