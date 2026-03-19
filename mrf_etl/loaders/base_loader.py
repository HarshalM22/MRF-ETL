"""
base_loader.py
Abstract base class for all mrf-etl loaders.
Defines the interface and shared chunking logic.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Generator, Iterable, Iterator

from mrf_etl.schema.mrf_row import MRFRow, HospitalMeta

# Default batch size for bulk inserts
DEFAULT_CHUNK_SIZE = 500


def _chunked(iterable: Iterable, size: int) -> Generator[list, None, None]:
    """Split an iterable into chunks of at most `size` items."""
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _json_safe(value) -> str | None:
    """Serialize a dict/list to JSON string, or None if empty/None."""
    if not value:
        return None
    try:
        return json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError):
        return str(value)


class BaseLoader(ABC):
    """
    Abstract base class for all loaders.
    Subclasses implement _ensure_schema(), _load_hospital(),
    _load_items_batch(), _load_codes_batch(), _load_rates_batch(),
    _load_raw_batch(), and file_already_loaded().
    """

    def __init__(self, chunk_size: int = DEFAULT_CHUNK_SIZE):
        self.chunk_size = chunk_size

    @abstractmethod
    def _ensure_schema(self) -> None:
        """Create tables/headers if they don't exist."""
        ...

    @abstractmethod
    def file_already_loaded(self, source_file: str) -> bool:
        """Return True if this source file has already been loaded (idempotency)."""
        ...

    @abstractmethod
    def _upsert_hospital(self, meta: HospitalMeta) -> int:
        """
        Insert or update hospital record.
        Returns the hospital's integer ID.
        """
        ...

    @abstractmethod
    def _insert_items_batch(
        self, batch: list[dict], hospital_id: int
    ) -> list[int]:
        """
        Bulk-insert mrf_items rows.
        Returns list of inserted item IDs in same order as batch.
        """
        ...

    @abstractmethod
    def _insert_codes_batch(self, batch: list[dict]) -> None:
        """Bulk-insert mrf_item_codes rows."""
        ...

    @abstractmethod
    def _insert_rates_batch(self, batch: list[dict]) -> None:
        """Bulk-insert mrf_rates rows."""
        ...

    @abstractmethod
    def _insert_raw_batch(self, batch: list[dict]) -> None:
        """Bulk-insert mrf_raw rows."""
        ...

    def load(
        self,
        rows: Iterator[MRFRow],
        meta: HospitalMeta,
        source_file: str,
        skip_if_loaded: bool = True,
        verbose: bool = True,
    ) -> dict:
        """
        Main load entry point.
        Streams MRFRow objects and writes to the target in batches.

        Args:
            rows:           Iterator of MRFRow objects from parse_file()
            meta:           HospitalMeta from parse_metadata()
            source_file:    Original file path (for idempotency check)
            skip_if_loaded: If True, skip files already in DB
            verbose:        Print progress

        Returns:
            Stats dict: {items, codes, rates, raw, skipped}
        """
        self._ensure_schema()

        if skip_if_loaded and self.file_already_loaded(source_file):
            if verbose:
                print(f"[SKIP] Already loaded: {source_file}")
            return {"items": 0, "codes": 0, "rates": 0, "raw": 0, "skipped": True}

        hospital_id = self._upsert_hospital(meta)
        if verbose:
            print(f"[LOAD] Hospital ID={hospital_id}  {meta.hospital_name}")

        total_items = total_codes = total_rates = total_raw = 0

        items_buf: list[dict] = []
        codes_buf: list[dict] = []
        rates_buf: list[dict] = []
        raw_buf: list[dict] = []

        def _flush():
            nonlocal total_items, total_codes, total_rates, total_raw
            if not items_buf:
                return

            item_ids = self._insert_items_batch(items_buf, hospital_id)
            total_items += len(item_ids)

            # Attach item_ids to code/rate/raw rows
            for item_id, code_rows in zip(item_ids, [r["_codes"] for r in items_buf]):
                for c in code_rows:
                    c["item_id"] = item_id
                    codes_buf.append(c)

            for item_id, rate_rows in zip(item_ids, [r["_rates"] for r in items_buf]):
                for r in rate_rows:
                    r["item_id"] = item_id
                    rates_buf.append(r)

            for item_id, raw_row in zip(item_ids, [r["_raw"] for r in items_buf]):
                raw_row["item_id"] = item_id
                raw_buf.append(raw_row)

            items_buf.clear()

            # Flush sub-batches
            if codes_buf:
                self._insert_codes_batch(codes_buf)
                total_codes += len(codes_buf)
                codes_buf.clear()

            if rates_buf:
                self._insert_rates_batch(rates_buf)
                total_rates += len(rates_buf)
                rates_buf.clear()

            if raw_buf:
                self._insert_raw_batch(raw_buf)
                total_raw += len(raw_buf)
                raw_buf.clear()

            if verbose:
                print(f"  [BATCH] items={total_items} codes={total_codes} "
                      f"rates={total_rates}", end="\r")

        for mrf_row in rows:
            item_dict = _mrf_row_to_item_dict(mrf_row, source_file)
            items_buf.append(item_dict)

            if len(items_buf) >= self.chunk_size:
                _flush()

        _flush()  # final partial batch

        if verbose:
            print(f"\n[DONE] items={total_items} codes={total_codes} "
                  f"rates={total_rates} raw={total_raw}")

        return {
            "items": total_items,
            "codes": total_codes,
            "rates": total_rates,
            "raw":   total_raw,
            "skipped": False,
        }


# ---------------------------------------------------------------------------
# Row → dict conversion (shared by all loaders)
# ---------------------------------------------------------------------------

def _mrf_row_to_item_dict(row: MRFRow, source_file: str) -> dict:
    """
    Convert an MRFRow into a flat dict for the mrf_items table,
    plus nested _codes, _rates, _raw sub-lists for child tables.
    """
    item = {
        "hospital_name":        row.hospital_name,
        "description":          row.description,
        "setting":              row.setting,
        "billing_class":        row.billing_class,
        "drug_unit_of_measure": row.drug_unit_of_measure,
        "drug_type_of_measure": row.drug_type_of_measure,
        "gross_charge":         row.gross_charge,
        "discounted_cash":      row.discounted_cash,
        "min_negotiated":       row.min_negotiated,
        "max_negotiated":       row.max_negotiated,
        "modifiers":            row.modifiers,
        "median_amount":        row.median_amount,
        "percentile_10th":      row.percentile_10th,
        "percentile_90th":      row.percentile_90th,
        "claims_count":         row.claims_count,
        "additional_notes":     row.additional_notes,
        "footnote":             row.footnote,
        "count_compared_rates": row.count_compared_rates,
        "extra_fields":         _json_safe(row.extra_fields),
        "source_file":          source_file,
        "row_number":           row.row_number,
        "layout_type":          row.layout_type,
    }

    codes = [
        {
            "code":          c.code,
            "code_original": c.code_original,
            "code_type":     c.code_type,
            "code_index":    c.code_index,
            "is_primary":    c.is_primary,
        }
        for c in row.billing_codes
    ]

    rates = [
        {
            "payer_name_raw":          r.payer_name_raw,
            "plan_name_raw":           r.plan_name_raw,
            "plan_tier_index":         r.plan_tier_index,
            "negotiated_dollar":       r.negotiated_dollar,
            "negotiated_percentage":   r.negotiated_percentage,
            "negotiated_algorithm":    r.negotiated_algorithm,
            "methodology":             r.methodology,
            "methodology_raw":         r.methodology_raw,
            "estimated_amount":        r.estimated_amount,
            "rate_flag":               r.rate_flag,
            "rate_note":               r.rate_note,
            "additional_notes":        r.additional_notes,
            "setting_from_payer":      r.setting_from_payer,
            "layout_source":           r.layout_source,
        }
        for r in row.rates
    ]

    raw = {
        "raw_row":    _json_safe(row.raw_row),
        "source_file": source_file,
        "row_number": row.row_number,
    }

    item["_codes"] = codes
    item["_rates"] = rates
    item["_raw"]   = raw
    return item