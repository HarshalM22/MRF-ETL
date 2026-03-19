"""
parser.py
MRFParser — convenience class wrapping pipeline + layout_detector.
Provides profile() and summary() for quick file inspection.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from mrf_etl.core.meta_parser import parse_metadata
from mrf_etl.core.layout_detector import detect_layout, LayoutResult
from mrf_etl.schema.mrf_row import HospitalMeta


@dataclass
class FileProfile:
    source: str
    hospital_meta: HospitalMeta
    layout: LayoutResult


class MRFParser:
    """
    Convenience wrapper for quick file inspection.

    Usage:
        parser = MRFParser("hospital.csv")
        print(parser.summary())
    """

    def __init__(self, source: str):
        self.source = source
        self._profile: Optional[FileProfile] = None

    def profile(self) -> FileProfile:
        if self._profile:
            return self._profile
        meta   = parse_metadata(self.source)
        layout = detect_layout(self.source)
        self._profile = FileProfile(
            source=self.source,
            hospital_meta=meta,
            layout=layout,
        )
        return self._profile

    def summary(self) -> str:
        p   = self.profile()
        m   = p.hospital_meta
        lay = p.layout

        lines = [
            f"{'='*60}",
            f"FILE:     {p.source}",
            f"{'='*60}",
            f"HOSPITAL: {m.hospital_name}",
            f"LOCATIONS:{m.hospital_locations}",
            f"ADDRESSES:{m.hospital_addresses}",
            f"LICENSE:  {m.license_number} ({m.license_state})",
            f"NPI:      {m.hospital_npi}",
            f"UPDATED:  {m.last_updated_on}",
            f"VERSION:  {m.cms_version}",
            f"{'─'*60}",
            f"LAYOUT:   {lay.layout_type.upper()}",
            f"HEADERS:  {len(lay.headers)} columns",
            f"CODE COLS:{list(lay.code_columns.keys())}",
            f"PAYER COMBOS: {len(lay.payer_plan_groups)}",
        ]

        if lay.payer_plan_groups:
            lines.append("PAYER/PLAN SAMPLE (first 5):")
            for g in lay.payer_plan_groups[:5]:
                lines.append(f"  {g.payer_name_raw!r:40s} | {g.plan_name_raw!r}")
            if len(lay.payer_plan_groups) > 5:
                lines.append(f"  ... and {len(lay.payer_plan_groups)-5} more")

        if m.extra_metadata:
            lines.append(f"EXTRA METADATA: {m.extra_metadata}")

        lines.append(f"{'='*60}")
        return "\n".join(lines)