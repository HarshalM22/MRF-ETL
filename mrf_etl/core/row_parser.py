"""
row_parser.py
Phase 2 core: converts one raw CSV data row into a fully populated MRFRow.

Handles both layout types:
  VERTICAL   — payer_name / plan_name columns, one row per payer
  HORIZONTAL — payer data encoded in column headers, one row per procedure

Uses LayoutResult from Phase 1 so it never re-reads headers.
All value normalization delegated to normalizer.py.
"""

from __future__ import annotations

from typing import Optional

from mrf_etl.schema.mrf_row import MRFRow, BillingCode, PayerRate
from mrf_etl.core.layout_detector import LayoutResult, PayerColumnGroup
from mrf_etl.core.normalizer import (
    clean_str,
    clean_numeric,
    check_rate_sentinel,
    normalize_methodology,
    normalize_setting,
    normalize_billing_class,
    normalize_code,
    infer_code_type,
    extract_setting_from_payer,
    normalize_payer_name,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_get(row: list[str], idx: Optional[int]) -> str:
    """Get cell value safely — returns empty string for missing/out-of-range."""
    if idx is None or idx >= len(row):
        return ""
    return row[idx]


def _extract_billing_codes(
    row: list[str],
    code_columns: dict[int, tuple[int, Optional[int]]],
) -> list[BillingCode]:
    """
    Extract all billing codes from code|N and code|N|type column pairs.
    Dynamically handles 1 to N code columns.
    Skips entries where the code value is empty.
    """
    codes: list[BillingCode] = []

    for code_num in sorted(code_columns.keys()):
        col_idx, type_idx = code_columns[code_num]

        if col_idx is None:
            continue

        raw_code = _safe_get(row, col_idx).strip()
        if not raw_code:
            continue  # sparse — this code slot empty for this row

        raw_type = _safe_get(row, type_idx).strip() if type_idx is not None else ""

        # Normalize code (fix scientific notation etc.)
        normalized_code, original_code = normalize_code(raw_code)

        # Resolve code type
        code_type = raw_type if raw_type else (infer_code_type(normalized_code) or "UNKNOWN")

        codes.append(BillingCode(
            code=normalized_code,
            code_type=code_type.upper(),
            code_index=code_num,
            is_primary=(code_num == 1),
            code_original=original_code,  # None if no normalization was needed
        ))

    return codes


def _parse_one_rate(
    row: list[str],
    group: PayerColumnGroup,
    layout_source: str,
) -> Optional[PayerRate]:
    """
    Build a PayerRate from one PayerColumnGroup and the current row.
    Returns None only if every rate/percentage/algorithm field is empty
    AND there are no payer notes — meaning this payer truly has no data
    for this row (common in horizontal files with sparse payer coverage).
    """
    # Extract raw values
    raw_dollar      = _safe_get(row, group.idx_negotiated_dollar)
    raw_pct         = _safe_get(row, group.idx_negotiated_percentage)
    raw_algo        = _safe_get(row, group.idx_negotiated_algorithm)
    raw_method      = _safe_get(row, group.idx_methodology)
    raw_estimated   = _safe_get(row, group.idx_estimated_amount)
    raw_notes       = _safe_get(row, group.idx_additional_notes)

    # Check if everything is empty — skip this payer entirely
    all_empty = not any([
        raw_dollar.strip(), raw_pct.strip(), raw_algo.strip(),
        raw_method.strip(), raw_estimated.strip(), raw_notes.strip(),
    ])
    if all_empty:
        return None

    # ── negotiated_dollar ────────────────────────────────────────────
    negotiated_dollar: Optional[float] = None
    rate_flag: Optional[str] = None
    rate_note: Optional[str] = None

    if raw_dollar.strip():
        flag, note = check_rate_sentinel(raw_dollar)
        if flag:
            rate_flag = flag
            rate_note = note
        else:
            negotiated_dollar = clean_numeric(raw_dollar)

    # ── negotiated_percentage ────────────────────────────────────────
    negotiated_percentage: Optional[float] = None
    if raw_pct.strip():
        flag, note = check_rate_sentinel(raw_pct)
        if flag:
            if not rate_flag:  # don't overwrite dollar flag
                rate_flag = flag
                rate_note = note
        else:
            negotiated_percentage = clean_numeric(raw_pct)

    # ── negotiated_algorithm ─────────────────────────────────────────
    negotiated_algorithm = clean_str(raw_algo)

    # ── methodology ──────────────────────────────────────────────────
    methodology_norm, methodology_raw = normalize_methodology(raw_method)

    # ── estimated_amount ─────────────────────────────────────────────
    estimated_amount: Optional[float] = None
    if raw_estimated.strip():
        flag, note = check_rate_sentinel(raw_estimated)
        if not flag:
            estimated_amount = clean_numeric(raw_estimated)

    # ── setting embedded in payer name ───────────────────────────────
    setting_from_payer = extract_setting_from_payer(group.payer_name_raw)

    return PayerRate(
        payer_name_raw=normalize_payer_name(group.payer_name_raw),
        plan_name_raw=normalize_payer_name(group.plan_name_raw),
        plan_tier_index=group.plan_tier_index,
        negotiated_dollar=negotiated_dollar,
        negotiated_percentage=negotiated_percentage,
        negotiated_algorithm=negotiated_algorithm,
        methodology=methodology_norm,
        methodology_raw=methodology_raw,
        estimated_amount=estimated_amount,
        rate_flag=rate_flag,
        rate_note=rate_note,
        additional_notes=clean_str(raw_notes),
        setting_from_payer=setting_from_payer,
        layout_source=layout_source,
    )


def _extract_horizontal_rates(
    row: list[str],
    payer_plan_groups: list[PayerColumnGroup],
) -> list[PayerRate]:
    """
    Extract all payer rates from a horizontal layout row.
    Iterates over every payer/plan column group, skips empty ones.
    """
    rates: list[PayerRate] = []
    for group in payer_plan_groups:
        rate = _parse_one_rate(row, group, layout_source="horizontal")
        if rate is not None:
            rates.append(rate)
    return rates


def _extract_vertical_rate(
    row: list[str],
    layout: LayoutResult,
) -> Optional[PayerRate]:
    """
    Extract the single payer rate from a vertical layout row.
    Returns None if payer_name is empty (header-only row or no payer).
    """
    sfm = layout.standard_field_map

    raw_payer = _safe_get(row, layout.idx_payer_name).strip()
    raw_plan  = _safe_get(row, layout.idx_plan_name).strip() if layout.idx_plan_name else ""

    # Build a temporary PayerColumnGroup to reuse _parse_one_rate
    group = PayerColumnGroup(
        payer_name_raw=raw_payer,
        plan_name_raw=raw_plan,
        plan_tier_index=0,
        idx_negotiated_dollar=sfm.get("negotiated_dollar"),
        idx_negotiated_percentage=sfm.get("negotiated_percentage"),
        idx_negotiated_algorithm=sfm.get("negotiated_algorithm"),
        idx_methodology=sfm.get("methodology"),
        idx_estimated_amount=sfm.get("estimated_amount"),
        idx_additional_notes=None,  # vertical files don't have per-payer notes column
    )

    rate = _parse_one_rate(row, group, layout_source="vertical")

    # For vertical files, a row with an empty payer name is just
    # a chargemaster row with no negotiated rate — keep MRFRow, rate=None
    # We signal this by returning None from here; caller handles it
    if rate is None and not raw_payer:
        return None

    # If we got None because all rate fields empty but payer exists, return
    # a minimal rate object preserving the payer association
    if rate is None and raw_payer:
        return PayerRate(
            payer_name_raw=raw_payer,
            plan_name_raw=raw_plan,
            plan_tier_index=0,
            layout_source="vertical",
        )

    return rate


# ---------------------------------------------------------------------------
# Main row parser
# ---------------------------------------------------------------------------

def parse_row(
    row: list[str],
    layout: LayoutResult,
    hospital_name: Optional[str] = None,
    hospital_npi: Optional[str] = None,
    source_file: Optional[str] = None,
    row_number: int = 0,
) -> MRFRow:
    """
    Convert one raw CSV data row into a fully populated MRFRow.

    Args:
        row:           Raw list of strings from the CSV reader
        layout:        LayoutResult from Phase 1 layout_detector
        hospital_name: From HospitalMeta — passed in to avoid re-reading
        hospital_npi:  From HospitalMeta
        source_file:   Source file path/URL for audit trail
        row_number:    1-based row index in file (for audit)

    Returns:
        MRFRow with all fields populated, raw_row preserved.
    """
    sfm = layout.standard_field_map

    # Pad row if shorter than expected (truncated lines)
    max_idx = len(layout.headers)
    if len(row) < max_idx:
        row = row + [""] * (max_idx - len(row))

    # ── Item-level fields ────────────────────────────────────────────
    description     = clean_str(_safe_get(row, sfm.get("description")))
    setting         = normalize_setting(_safe_get(row, sfm.get("setting")))
    billing_class   = normalize_billing_class(_safe_get(row, sfm.get("billing_class")))
    modifiers       = clean_str(_safe_get(row, sfm.get("modifiers")))
    drug_unit       = clean_str(_safe_get(row, sfm.get("drug_unit_of_measure")))
    drug_type       = clean_str(_safe_get(row, sfm.get("drug_type_of_measure")))

    gross_charge    = clean_numeric(_safe_get(row, sfm.get("gross_charge")))
    discounted_cash = clean_numeric(_safe_get(row, sfm.get("discounted_cash")))
    min_negotiated  = clean_numeric(_safe_get(row, sfm.get("min_negotiated")))
    max_negotiated  = clean_numeric(_safe_get(row, sfm.get("max_negotiated")))

    # ── Statistical fields (South Peninsula pattern) ─────────────────
    median_amount   = clean_numeric(_safe_get(row, sfm.get("median_amount")))
    pct_10th        = clean_numeric(_safe_get(row, sfm.get("percentile_10th")))
    pct_90th        = clean_numeric(_safe_get(row, sfm.get("percentile_90th")))
    raw_count       = _safe_get(row, sfm.get("claims_count")).strip()
    claims_count    = int(float(raw_count)) if raw_count and raw_count.replace(".", "").isdigit() else None

    # ── Notes / extra ────────────────────────────────────────────────
    additional_notes    = clean_str(_safe_get(row, sfm.get("additional_notes")))
    footnote            = clean_str(_safe_get(row, sfm.get("footnote")))
    raw_ccr             = _safe_get(row, sfm.get("count_compared_rates")).strip()
    count_compared      = int(float(raw_ccr)) if raw_ccr and raw_ccr.replace(".", "").isdigit() else None

    # ── Unknown columns → extra_fields ──────────────────────────────
    extra_fields: dict = {}
    for col_name, col_idx in layout.unknown_columns.items():
        val = _safe_get(row, col_idx).strip()
        if val:
            extra_fields[col_name] = val

    # ── Billing codes ────────────────────────────────────────────────
    billing_codes = _extract_billing_codes(row, layout.code_columns)

    # ── Rates ────────────────────────────────────────────────────────
    rates: list[PayerRate] = []

    if layout.layout_type == "horizontal":
        rates = _extract_horizontal_rates(row, layout.payer_plan_groups)

    elif layout.layout_type == "vertical":
        rate = _extract_vertical_rate(row, layout)
        if rate is not None:
            rates = [rate]

    elif layout.layout_type == "mixed":
        # Horizontal rates first
        rates = _extract_horizontal_rates(row, layout.payer_plan_groups)
        # Add vertical rate if present
        rate = _extract_vertical_rate(row, layout)
        if rate is not None:
            rates.append(rate)

    # ── Assemble MRFRow ──────────────────────────────────────────────
    return MRFRow(
        hospital_name=hospital_name,
        hospital_npi=hospital_npi,
        source_file=source_file,
        description=description,
        setting=setting,
        billing_class=billing_class,
        modifiers=modifiers,
        drug_unit_of_measure=drug_unit,
        drug_type_of_measure=drug_type,
        gross_charge=gross_charge,
        discounted_cash=discounted_cash,
        min_negotiated=min_negotiated,
        max_negotiated=max_negotiated,
        median_amount=median_amount,
        percentile_10th=pct_10th,
        percentile_90th=pct_90th,
        claims_count=claims_count,
        billing_codes=billing_codes,
        rates=rates,
        additional_notes=additional_notes,
        footnote=footnote,
        count_compared_rates=count_compared,
        extra_fields=extra_fields,
        raw_row=dict(zip(layout.headers, row)),
        row_number=row_number,
        layout_type=layout.layout_type,
        schema_map_used="auto",
    )