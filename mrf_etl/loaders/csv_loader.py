"""
csv_loader.py
CSV loader for mrf-etl.

Outputs 5 normalized CSV files — one per table:
  mrf_hospitals.csv
  mrf_items.csv
  mrf_item_codes.csv
  mrf_rates.csv
  mrf_raw.csv

Features:
  - Streaming write — never loads full file into memory
  - Auto-assigns integer IDs using in-memory counters
  - Idempotency: detects existing output files and appends or skips
  - Output directory is configurable

No external dependencies — uses only stdlib csv module.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
from pathlib import Path
from typing import Optional

from mrf_etl.loaders.base_loader import BaseLoader, _json_safe
from mrf_etl.schema.mrf_row import HospitalMeta


# ---------------------------------------------------------------------------
# Column definitions per file (determines CSV header order)
# ---------------------------------------------------------------------------

HOSPITALS_COLS = [
    "id", "hospital_name", "hospital_locations", "hospital_addresses",
    "license_number", "license_state", "hospital_npi", "attester_name",
    "last_updated_on", "as_of_date", "financial_aid_policy", "cms_version",
    "source_file", "source_file_hash", "extra_metadata",
]

ITEMS_COLS = [
    "id", "hospital_id", "description", "setting", "billing_class",
    "drug_unit_of_measure", "drug_type_of_measure",
    "gross_charge", "discounted_cash", "min_negotiated", "max_negotiated",
    "modifiers", "median_amount", "percentile_10th", "percentile_90th",
    "claims_count", "additional_notes", "footnote", "count_compared_rates",
    "extra_fields", "source_file", "row_number", "layout_type",
]

CODES_COLS = [
    "id", "item_id", "code", "code_original", "code_type", "code_index", "is_primary",
]

RATES_COLS = [
    "id", "item_id", "payer_name_raw", "plan_name_raw", "plan_tier_index",
    "negotiated_dollar", "negotiated_percentage", "negotiated_algorithm",
    "methodology", "methodology_raw", "estimated_amount",
    "rate_flag", "rate_note", "additional_notes",
    "setting_from_payer", "layout_source",
]

RAW_COLS = [
    "id", "hospital_id", "item_id", "raw_row", "source_file", "row_number",
]


class CSVLoader(BaseLoader):
    """
    Writes MRFRow objects to normalized CSV files.

    Usage:
        loader = CSVLoader(output_dir="./output")
        meta = parse_metadata("hospital.csv")
        rows = parse_file("hospital.csv")
        stats = loader.load(rows, meta, source_file="hospital.csv")
    """

    def __init__(self, output_dir: str = "./mrf_output", chunk_size: int = 500):
        super().__init__(chunk_size=chunk_size)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # ID counters (auto-increment simulation)
        self._hospital_id_counter = 0
        self._item_id_counter = 0
        self._code_id_counter = 0
        self._rate_id_counter = 0
        self._raw_id_counter = 0

        # Track loaded file hashes in memory
        self._loaded_hashes: set[str] = set()

        # Open CSV writers (lazily initialized)
        self._writers: dict[str, csv.DictWriter] = {}
        self._file_handles: dict[str, object] = {}

        # Track current hospital_id for raw inserts
        self._current_hospital_id: int = 0

    def _ensure_schema(self) -> None:
        """Create CSV files with headers if they don't exist."""
        specs = [
            ("mrf_hospitals.csv",   HOSPITALS_COLS),
            ("mrf_items.csv",       ITEMS_COLS),
            ("mrf_item_codes.csv",  CODES_COLS),
            ("mrf_rates.csv",       RATES_COLS),
            ("mrf_raw.csv",         RAW_COLS),
        ]
        for filename, cols in specs:
            path = self.output_dir / filename
            if filename not in self._writers:
                is_new = not path.exists()
                fh = open(path, "a", newline="", encoding="utf-8")
                writer = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
                if is_new:
                    writer.writeheader()
                self._writers[filename] = writer
                self._file_handles[filename] = fh

        # Load existing hashes from hospitals file for idempotency
        hosp_path = self.output_dir / "mrf_hospitals.csv"
        if hosp_path.exists():
            try:
                with open(hosp_path, encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        h = row.get("source_file_hash", "")
                        if h:
                            self._loaded_hashes.add(h)
                        # Update ID counter
                        try:
                            self._hospital_id_counter = max(
                                self._hospital_id_counter, int(row.get("id", 0))
                            )
                        except (ValueError, TypeError):
                            pass
            except Exception:
                pass

    def close(self):
        """Flush and close all open file handles."""
        for fh in self._file_handles.values():
            fh.close()
        self._writers.clear()
        self._file_handles.clear()

    def file_already_loaded(self, source_file: str) -> bool:
        h = _file_hash(source_file)
        return h in self._loaded_hashes

    def _upsert_hospital(self, meta: HospitalMeta) -> int:
        h = _file_hash(meta.source_file or "")
        if h in self._loaded_hashes:
            # Find existing id — for CSV we just return the counter
            return self._hospital_id_counter

        self._hospital_id_counter += 1
        hid = self._hospital_id_counter
        self._current_hospital_id = hid

        row = {
            "id":                   hid,
            "hospital_name":        meta.hospital_name,
            "hospital_locations":   _json_safe(meta.hospital_locations),
            "hospital_addresses":   _json_safe(meta.hospital_addresses),
            "license_number":       meta.license_number,
            "license_state":        meta.license_state,
            "hospital_npi":         meta.hospital_npi,
            "attester_name":        meta.attester_name,
            "last_updated_on":      meta.last_updated_on,
            "as_of_date":           meta.as_of_date,
            "financial_aid_policy": meta.financial_aid_policy,
            "cms_version":          meta.cms_version,
            "source_file":          meta.source_file,
            "source_file_hash":     h,
            "extra_metadata":       _json_safe(meta.extra_metadata),
        }
        self._writers["mrf_hospitals.csv"].writerow(row)
        self._file_handles["mrf_hospitals.csv"].flush()
        self._loaded_hashes.add(h)
        return hid

    def _insert_items_batch(self, batch: list[dict], hospital_id: int) -> list[int]:
        ids = []
        writer = self._writers["mrf_items.csv"]
        for item in batch:
            self._item_id_counter += 1
            iid = self._item_id_counter
            row = {"id": iid, "hospital_id": hospital_id}
            row.update({k: v for k, v in item.items() if not k.startswith("_")})
            writer.writerow(row)
            ids.append(iid)
        self._file_handles["mrf_items.csv"].flush()
        return ids

    def _insert_codes_batch(self, batch: list[dict]) -> None:
        if not batch:
            return
        writer = self._writers["mrf_item_codes.csv"]
        for code in batch:
            self._code_id_counter += 1
            row = {"id": self._code_id_counter}
            row.update(code)
            writer.writerow(row)
        self._file_handles["mrf_item_codes.csv"].flush()

    def _insert_rates_batch(self, batch: list[dict]) -> None:
        if not batch:
            return
        writer = self._writers["mrf_rates.csv"]
        for rate in batch:
            self._rate_id_counter += 1
            row = {"id": self._rate_id_counter}
            row.update(rate)
            writer.writerow(row)
        self._file_handles["mrf_rates.csv"].flush()

    def _insert_raw_batch(self, batch: list[dict]) -> None:
        if not batch:
            return
        writer = self._writers["mrf_raw.csv"]
        for raw in batch:
            self._raw_id_counter += 1
            row = {
                "id":           self._raw_id_counter,
                "hospital_id":  self._current_hospital_id,
                "item_id":      raw.get("item_id"),
                "raw_row":      raw.get("raw_row"),
                "source_file":  raw.get("source_file"),
                "row_number":   raw.get("row_number"),
            }
            writer.writerow(row)
        self._file_handles["mrf_raw.csv"].flush()


def _file_hash(path: str) -> str:
    return hashlib.sha256(path.encode()).hexdigest()