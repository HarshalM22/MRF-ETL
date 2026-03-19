"""
mrf_row.py
Canonical dataclasses for mrf-etl output.
Every parsed row becomes an MRFRow regardless of source format.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class BillingCode:
    code: str
    code_type: str                        # CPT|HCPCS|MS-DRG|RC|NDC|CDM|LOCAL
    code_index: int                       # 1=first, 2=second ... N
    is_primary: bool                      # True only when code_index == 1
    code_original: Optional[str] = None  # raw value before any fix (e.g. 2.70E+11)


@dataclass
class PayerRate:
    payer_name_raw: str
    plan_name_raw: str
    plan_tier_index: int = 0              # 0=base, 1=_1 suffix, 2=_2 suffix

    negotiated_dollar: Optional[float] = None
    negotiated_percentage: Optional[float] = None
    negotiated_algorithm: Optional[str] = None    # TEXT — can be 500+ chars

    methodology: Optional[str] = None             # normalized lowercase
    methodology_raw: Optional[str] = None         # original casing preserved

    estimated_amount: Optional[float] = None

    rate_flag: Optional[str] = None    # not_covered|not_reimbursable|not_payable
    rate_note: Optional[str] = None    # original text when rate was a sentinel

    additional_notes: Optional[str] = None
    setting_from_payer: Optional[str] = None   # if setting embedded in payer name
    layout_source: str = "unknown"             # horizontal|vertical


@dataclass
class HospitalMeta:
    hospital_name: Optional[str] = None
    hospital_locations: list = field(default_factory=list)
    hospital_addresses: list = field(default_factory=list)
    license_number: Optional[str] = None
    license_state: Optional[str] = None
    hospital_npi: Optional[str] = None
    attester_name: Optional[str] = None
    last_updated_on: Optional[str] = None
    as_of_date: Optional[str] = None
    financial_aid_policy: Optional[str] = None
    cms_version: Optional[str] = None
    source_file: Optional[str] = None
    extra_metadata: dict = field(default_factory=dict)


@dataclass
class MRFRow:
    # Hospital
    hospital_name: Optional[str] = None
    hospital_npi: Optional[str] = None
    source_file: Optional[str] = None

    # Procedure
    description: Optional[str] = None
    setting: Optional[str] = None          # inpatient|outpatient|both
    billing_class: Optional[str] = None    # facility|professional

    # Drug
    drug_unit_of_measure: Optional[str] = None
    drug_type_of_measure: Optional[str] = None

    # Prices
    gross_charge: Optional[float] = None
    discounted_cash: Optional[float] = None
    min_negotiated: Optional[float] = None
    max_negotiated: Optional[float] = None
    modifiers: Optional[str] = None

    # Statistical fields
    median_amount: Optional[float] = None
    percentile_10th: Optional[float] = None
    percentile_90th: Optional[float] = None
    claims_count: Optional[int] = None

    # All billing codes
    billing_codes: list = field(default_factory=list)

    # All payer rates
    rates: list = field(default_factory=list)

    # Extra
    additional_notes: Optional[str] = None
    footnote: Optional[str] = None
    count_compared_rates: Optional[int] = None
    extra_fields: dict = field(default_factory=dict)

    # Audit
    raw_row: dict = field(default_factory=dict)
    row_number: Optional[int] = None
    layout_type: str = "unknown"
    schema_map_used: str = "auto"