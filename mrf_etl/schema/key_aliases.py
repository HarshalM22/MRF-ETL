"""
key_aliases.py
Normalizes variant metadata key names across hospitals to standard field names.
Built from real file analysis — extended as new variants are discovered.
"""

# Maps any observed metadata key → standard field name
METADATA_KEY_ALIASES: dict[str, str] = {
    # hospital name
    "hospital_name": "hospital_name",

    # location — varies across hospitals
    "hospital_location": "hospital_location",
    "location_name": "hospital_location",       # South Peninsula
    "facility_name": "hospital_location",
    "facility": "hospital_location",

    # address
    "hospital_address": "hospital_address",
    "address": "hospital_address",

    # dates
    "last_updated_on": "last_updated_on",
    "last_updated": "last_updated_on",
    "updated_on": "last_updated_on",
    "as_of_date": "as_of_date",                 # Whitfield
    "effective_date": "as_of_date",

    # version
    "version": "cms_version",
    "schema_version": "cms_version",

    # NPI
    "type_2_npi": "hospital_npi",              # South Peninsula
    "npi": "hospital_npi",
    "hospital_npi": "hospital_npi",

    # attestation
    "attester_name": "attester_name",          # South Peninsula
    "attesting_official": "attester_name",

    # financial aid
    "financial_aid_policy": "financial_aid_policy",  # Whitfield
    "charity_care_policy": "financial_aid_policy",
}

# Known CMS compliance statement prefixes — used to detect and skip the
# long compliance text field that appears in every file's metadata row
CMS_COMPLIANCE_PREFIXES = (
    "to the best of its knowledge",
    "to the best of its knowledge and belief",
)


def normalize_metadata_key(raw_key: str) -> str:
    """
    Strip whitespace, lowercase, then map to standard name.
    Returns the standard name, or the cleaned raw key if not in aliases.
    """
    cleaned = raw_key.strip().lower()
    return METADATA_KEY_ALIASES.get(cleaned, cleaned)


def is_cms_compliance_field(key: str) -> bool:
    """
    Returns True if this metadata key is the long CMS compliance statement.
    These are not data fields — skip them during metadata parsing.
    """
    cleaned = key.strip().lower()
    return any(cleaned.startswith(p) for p in CMS_COMPLIANCE_PREFIXES)