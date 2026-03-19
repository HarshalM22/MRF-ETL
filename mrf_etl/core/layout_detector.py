"""
layout_detector.py
Reads the header row (row 2) of an MRF file and determines:

  1. Layout type: horizontal | vertical | mixed
  2. PayerPlanMap: for horizontal files, maps each payer/plan combo
     to its column indices for each metric
  3. Code columns: all code|N and code|N|type columns present
  4. Standard field map: known non-payer columns → standard names

Horizontal column pattern (4-part pipe-delimited):
  standard_charge|<PAYER>|<PLAN>|<METRIC>
  estimated_amount|<PAYER>|<PLAN>
  additional_payer_notes|<PAYER>|<PLAN>

Critical: payer name itself can contain pipes.
  e.g. standard_charge|Non|Contracted Commercial|negotiated_dollar
  → payer = "Non|Contracted Commercial", plan = (none), metric = negotiated_dollar
  We anchor on known prefixes and known metrics to extract middle segments.
"""

from __future__ import annotations
import re
from dataclasses import dataclass, field
from typing import Union
from pathlib import Path

from mrf_etl.core.ingester import peek_rows


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

KNOWN_PREFIXES = frozenset({
    "standard_charge",
    "estimated_amount",
    "additional_payer_notes",
})

KNOWN_METRICS = frozenset({
    "negotiated_dollar",
    "negotiated_percentage",
    "negotiated_algorithm",
    "methodology",
})

# Standard non-payer columns → canonical field name
STANDARD_COLUMNS: dict[str, str] = {
    "description": "description",
    "modifiers": "modifiers",
    "setting": "setting",
    "billing_class": "billing_class",
    "drug_unit_of_measurement": "drug_unit_of_measure",
    "drug_type_of_measurement": "drug_type_of_measure",
    "standard_charge|gross": "gross_charge",
    "standard_charge|discounted_cash": "discounted_cash",
    "standard_charge|min": "min_negotiated",
    "standard_charge|max": "max_negotiated",
    "payer_name": "payer_name",
    "plan_name": "plan_name",
    "standard_charge|negotiated_dollar": "negotiated_dollar",
    "standard_charge|negotiated_percentage": "negotiated_percentage",
    "standard_charge|negotiated_algorithm": "negotiated_algorithm",
    "standard_charge|methodology": "methodology",
    "estimated_amount": "estimated_amount",
    "additional_generic_notes": "additional_notes",
    "footnote": "footnote",
    "count_of_compared_rates": "count_compared_rates",
    "median_amount": "median_amount",
    "10th_percentile": "percentile_10th",
    "90th_percentile": "percentile_90th",
    "count": "claims_count",
}

# Pattern to detect code|N columns
_CODE_COL_RE = re.compile(r"^code\|(\d+)$", re.IGNORECASE)
_CODE_TYPE_COL_RE = re.compile(r"^code\|(\d+)\|type$", re.IGNORECASE)

# Plan tier suffix pattern: PLAN_1 or PLAN_2 etc.
_TIER_SUFFIX_RE = re.compile(r"^(.+?)_(\d+)$")


# ---------------------------------------------------------------------------
# PayerColumnGroup
# Holds all column indices for one payer/plan combination
# ---------------------------------------------------------------------------

@dataclass
class PayerColumnGroup:
    payer_name_raw: str
    plan_name_raw: str
    plan_tier_index: int = 0

    # column indices (None if column absent in this file)
    idx_negotiated_dollar: int | None = None
    idx_negotiated_percentage: int | None = None
    idx_negotiated_algorithm: int | None = None
    idx_methodology: int | None = None
    idx_estimated_amount: int | None = None
    idx_additional_notes: int | None = None


# ---------------------------------------------------------------------------
# LayoutResult
# Full result returned by detect_layout()
# ---------------------------------------------------------------------------

@dataclass
class LayoutResult:
    layout_type: str                              # horizontal|vertical|mixed
    headers: list[str]                            # raw headers from row 2
    header_row_index: int = 2                     # which row had the headers

    # Code columns: {1: (col_idx_code, col_idx_type), 2: ..., ...}
    code_columns: dict[int, tuple[int, int | None]] = field(default_factory=dict)

    # Standard field map: canonical_name → column_index
    standard_field_map: dict[str, int] = field(default_factory=dict)

    # Horizontal only: list of all payer/plan column groups
    payer_plan_groups: list[PayerColumnGroup] = field(default_factory=list)

    # Vertical only: index of payer_name and plan_name columns
    idx_payer_name: int | None = None
    idx_plan_name: int | None = None

    # Unknown columns: col_name → index (goes to extra_fields)
    unknown_columns: dict[str, int] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Core parsing helpers
# ---------------------------------------------------------------------------

def _parse_payer_column(col: str) -> dict | None:
    """
    Parse a horizontal payer column name into its components.

    Strategy:
      - Must start with a known prefix
      - For standard_charge: last segment must be a known metric
      - For estimated_amount / additional_payer_notes: no metric suffix
      - Everything between prefix and metric (if any) = payer|plan
      - First segment of middle = payer name
      - Remaining segments joined with | = plan name
      - Plan name may have _N suffix indicating tier

    Returns dict with keys: prefix, payer, plan, plan_tier_index, metric
    Returns None if column does not match the payer column pattern.
    """
    parts = col.split("|")
    if not parts or parts[0] not in KNOWN_PREFIXES:
        return None

    prefix = parts[0]
    rest = parts[1:]

    if not rest:
        return None

    # For standard_charge: last part must be a known metric
    if prefix == "standard_charge":
        if rest[-1] not in KNOWN_METRICS:
            return None
        metric = rest[-1]
        middle = rest[:-1]
    else:
        # estimated_amount | additional_payer_notes — no metric suffix
        metric = None
        middle = rest

    if not middle:
        return None

    # First element is payer (may be empty string for Allina pattern)
    payer = middle[0]

    # Remaining elements joined = plan
    plan_raw = "|".join(middle[1:]) if len(middle) > 1 else ""

    # Detect tier suffix on plan: PLAN_1, PLAN_2
    plan_tier_index = 0
    plan = plan_raw
    m = _TIER_SUFFIX_RE.match(plan_raw)
    if m:
        try:
            plan_tier_index = int(m.group(2))
            plan = m.group(1)
        except ValueError:
            pass

    return {
        "prefix": prefix,
        "payer": payer,
        "plan": plan,
        "plan_raw": plan_raw,
        "plan_tier_index": plan_tier_index,
        "metric": metric,
    }


def _find_header_row(rows: list[list[str]]) -> int:
    """
    Find which row index contains the actual data headers.
    In CMS 2.0 format this is always row 2 (index 2).
    Verify by checking for 'description' in the row.
    Falls back to scanning first 10 rows.
    """
    for i, row in enumerate(rows[:10]):
        row_lower = [c.strip().lower() for c in row]
        if "description" in row_lower:
            return i
    return 2  # default


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def detect_layout(source: Union[str, Path]) -> LayoutResult:
    """
    Read the header row from an MRF file and return a full LayoutResult.

    Detects:
    - Layout type (horizontal / vertical / mixed)
    - All code|N column positions (dynamic, up to N)
    - All standard field positions
    - All payer/plan column groups (horizontal)
    - Payer_name / plan_name column positions (vertical)
    - Unknown columns (to be stored in extra_fields)
    """
    rows = peek_rows(source, n=10)
    header_idx = _find_header_row(rows)
    if header_idx >= len(rows):
        raise ValueError(f"Could not find header row in: {source}")

    raw_headers = rows[header_idx]
    # Normalize header strings: strip whitespace, lowercase for matching
    headers_clean = [h.strip() for h in raw_headers]

    result = LayoutResult(
        layout_type="unknown",
        headers=headers_clean,
        header_row_index=header_idx,
    )

    has_payer_col = False
    has_horizontal_payer = False

    # Key: (payer, plan, plan_tier_index) → PayerColumnGroup
    payer_group_map: dict[tuple, PayerColumnGroup] = {}

    for idx, col in enumerate(headers_clean):
        col_lower = col.lower()

        # --- Code columns ---
        m = _CODE_COL_RE.match(col_lower)
        if m:
            n = int(m.group(1))
            existing = result.code_columns.get(n, (None, None))
            result.code_columns[n] = (idx, existing[1])
            continue

        m = _CODE_TYPE_COL_RE.match(col_lower)
        if m:
            n = int(m.group(1))
            existing = result.code_columns.get(n, (None, None))
            result.code_columns[n] = (existing[0], idx)
            continue

        # --- Standard columns ---
        if col_lower in STANDARD_COLUMNS:
            std_name = STANDARD_COLUMNS[col_lower]
            result.standard_field_map[std_name] = idx

            if std_name == "payer_name":
                has_payer_col = True
                result.idx_payer_name = idx
            elif std_name == "plan_name":
                result.idx_plan_name = idx
            continue

        # --- Payer columns (horizontal) ---
        parsed = _parse_payer_column(col)
        if parsed:
            has_horizontal_payer = True
            key = (parsed["payer"], parsed["plan"], parsed["plan_tier_index"])

            if key not in payer_group_map:
                payer_group_map[key] = PayerColumnGroup(
                    payer_name_raw=parsed["payer"],
                    plan_name_raw=parsed["plan"],
                    plan_tier_index=parsed["plan_tier_index"],
                )

            group = payer_group_map[key]
            metric = parsed["metric"]
            prefix = parsed["prefix"]

            if prefix == "standard_charge":
                if metric == "negotiated_dollar":
                    group.idx_negotiated_dollar = idx
                elif metric == "negotiated_percentage":
                    group.idx_negotiated_percentage = idx
                elif metric == "negotiated_algorithm":
                    group.idx_negotiated_algorithm = idx
                elif metric == "methodology":
                    group.idx_methodology = idx
            elif prefix == "estimated_amount":
                group.idx_estimated_amount = idx
            elif prefix == "additional_payer_notes":
                group.idx_additional_notes = idx
            continue

        # --- Unknown column ---
        if col:  # skip truly empty header cells
            result.unknown_columns[col] = idx

    # Determine layout type
    if has_horizontal_payer and has_payer_col:
        result.layout_type = "mixed"
    elif has_horizontal_payer:
        result.layout_type = "horizontal"
    elif has_payer_col:
        result.layout_type = "vertical"
    else:
        result.layout_type = "unknown"

    result.payer_plan_groups = list(payer_group_map.values())

    return result