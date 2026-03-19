"""
pipeline.py
Full streaming MRF parse pipeline combining Phase 1 + Phase 2.

Usage:
    from mrf_etl.core.pipeline import parse_file

    for row in parse_file("hospital.csv"):
        print(row.description, row.gross_charge, len(row.rates))
"""

from __future__ import annotations

from typing import Generator, Optional

from mrf_etl.core.ingester import stream_rows
from mrf_etl.core.meta_parser import parse_metadata
from mrf_etl.core.layout_detector import detect_layout, LayoutResult
from mrf_etl.core.row_parser import parse_row
from mrf_etl.schema.mrf_row import MRFRow, HospitalMeta


def parse_file(
    source: str,
    max_rows: Optional[int] = None,
    skip_empty_descriptions: bool = True,
) -> Generator[MRFRow, None, None]:
    """
    Full streaming parse of one MRF CSV file.
    Yields MRFRow objects one at a time.
    """
    hospital_meta: HospitalMeta = parse_metadata(source)
    hospital_meta.source_file = source
    layout: LayoutResult = detect_layout(source)

    hospital_name = hospital_meta.hospital_name
    hospital_npi  = hospital_meta.hospital_npi
    data_row_count = 0

    for raw_row_number, row in enumerate(stream_rows(source, skip_empty=True)):
        if raw_row_number <= layout.header_row_index:
            continue

        mrf_row = parse_row(
            row=row,
            layout=layout,
            hospital_name=hospital_name,
            hospital_npi=hospital_npi,
            source_file=source,
            row_number=raw_row_number,
        )

        if skip_empty_descriptions and not mrf_row.description:
            continue

        yield mrf_row
        data_row_count += 1
        if max_rows is not None and data_row_count >= max_rows:
            break


def profile_file(source: str) -> dict:
    """Quick summary stats from first 100 data rows."""
    hospital_meta = parse_metadata(source)
    layout = detect_layout(source)

    sample_rows = list(parse_file(source, max_rows=100))

    total_codes = sum(len(r.billing_codes) for r in sample_rows)
    total_rates = sum(len(r.rates) for r in sample_rows)
    code_types  = set()
    payers      = set()
    for r in sample_rows:
        for c in r.billing_codes:
            code_types.add(c.code_type)
        for rate in r.rates:
            payers.add(rate.payer_name_raw)

    return {
        "hospital_name":         hospital_meta.hospital_name,
        "layout_type":           layout.layout_type,
        "total_columns":         len(layout.headers),
        "payer_plan_combos":     len(layout.payer_plan_groups),
        "sample_rows":           len(sample_rows),
        "avg_codes_per_row":     round(total_codes / max(len(sample_rows), 1), 2),
        "avg_rates_per_row":     round(total_rates / max(len(sample_rows), 1), 2),
        "code_types_seen":       sorted(code_types),
        "unique_payers_sample":  sorted(payers)[:20],
    }