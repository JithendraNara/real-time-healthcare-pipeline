"""
HIPAA Safe Harbor de-identification.

The 18 HIPAA identifiers and how we handle each in the OMOP / streaming data model:

  1. Names                  → remove or replace with study id
  2. Geographic subdivisions smaller than state → drop last 3 zip digits for pop < 20k
  3. All elements of dates (except year) related to an individual → year only for ages 90+
  4. Telephone numbers      → remove
  5. Fax numbers            → remove
  6. Email addresses        → remove
  7. SSNs                   → remove
  8. Medical record numbers → hashed surrogate
  9. Health plan beneficiary numbers → remove
  10. Account numbers       → remove
  11. Certificate/license numbers → remove
  12. Vehicle identifiers   → remove
  13. Device identifiers    → hashed surrogate
  14. URLs                  → remove (except general info)
  15. IP addresses          → drop / 0 last octet
  16. Biometric identifiers → remove
  17. Full-face photographs → remove
  18. Any other unique identifier → remove or hash

The functions here work on dict-like OMOP records or streaming events.
They NEVER require encryption (Safe Harbor = remove, not encrypt).

Reference: 45 CFR 164.514(b)(2)
"""
from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime
from typing import Any

log = logging.getLogger("deidentify")


# ---------------------------------------------------------------------------
# Field-level helpers
# ---------------------------------------------------------------------------


def _hash(value: str, salt: str = "deid-2026-06") -> str:
    """Deterministic hashed surrogate. Same input → same output, so the
    de-identified dataset is still joinable across tables."""
    return "DH_" + hashlib.sha256((salt + "::" + str(value)).encode()).hexdigest()[:16]


def redact_zip(zip_code: str | None) -> str | None:
    """Drop last 3 digits when the underlying population is < 20,000. For
    simplicity here we always drop the last 3 — production should hit a
    zip→population lookup to decide."""
    if not zip_code or not isinstance(zip_code, str) or zip_code == "":
        return None
    if len(zip_code) < 5:
        return "00000"
    # Handle ZIP+4 (e.g. "46802-1234") — preserve the +4
    if "-" in zip_code:
        prefix, suffix = zip_code.split("-", 1)
        return prefix[:2] + "***" + "-" + suffix
    return zip_code[:2] + "***"


def generalize_age(birth_datetime: str | None, reference_time: datetime | None = None) -> int | None:
    """Safe Harbor: ages over 89 must be aggregated as '90+'. Returns 90 for
    anyone who is 90+ at the reference time, else exact age."""
    if not birth_datetime:
        return None
    try:
        if isinstance(birth_datetime, str):
            bd = datetime.fromisoformat(birth_datetime.replace("Z", "+00:00"))
        else:
            bd = birth_datetime
        ref = reference_time or datetime.now()
        # Normalize to naive for subtraction
        bd_naive = bd.replace(tzinfo=None) if hasattr(bd, "tzinfo") and bd.tzinfo is not None else bd
        ref_naive = ref.replace(tzinfo=None) if hasattr(ref, "tzinfo") and ref.tzinfo is not None else ref
        # Use 365.25 to handle leap years
        age = round((ref_naive - bd_naive).days / 365.25)
        return 90 if age >= 90 else age
    except Exception:  # noqa: BLE001
        return None


def date_to_year(date_str: str | None) -> int | None:
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00")).year
    except Exception:  # noqa: BLE001
        return None


# ---------------------------------------------------------------------------
# Record-level de-identification
# ---------------------------------------------------------------------------


def deidentify_omop_person(record: dict, salt: str | None = None) -> dict:
    """Apply Safe Harbor to an OMOP person record."""
    out = dict(record)
    # 1. Names — typically not in person table; if present, hash
    if "name" in out:
        out["name"] = _hash(out["name"], salt or "person-name")
    # 3. Birth date → year only (with age cap)
    if "birth_datetime" in out:
        out["birth_datetime"] = None
    if "year_of_birth" in out:
        try:
            yob = int(out["year_of_birth"])
            ref_year = datetime.utcnow().year
            if (ref_year - yob) >= 90:
                out["year_of_birth"] = 1926  # capped
        except (TypeError, ValueError):
            pass
    # 4. Phone, 5. Fax, 6. Email, 7. SSN, 9. Health plan, 10. Account, 11. Cert,
    # 14. URL — drop entirely
    for f in ("phone", "fax", "email", "ssn", "health_plan_number", "account_number",
              "cert_number", "license_number", "url"):
        if f in out:
            out[f] = None
    # 8. MRN → hashed surrogate
    if "mrn" in out and out["mrn"]:
        out["mrn"] = _hash(str(out["mrn"]), salt or "mrn")
    return out


def deidentify_visit(record: dict) -> dict:
    """Apply Safe Harbor to an OMOP visit_occurrence record.

    Dates are reduced to year-only; specific times are dropped. The
    visit_occurrence_id itself is a surrogate OMOP id (not PHI) and is kept.
    """
    out = dict(record)
    for date_field in ("visit_start_datetime", "visit_end_datetime"):
        if date_field in out:
            year = date_to_year(out[date_field])
            out[date_field] = None
            out[date_field.replace("_datetime", "_year")] = year
    return out


def deidentify_iot_event(event: dict) -> dict:
    """De-identify an IoT telemetry event. Device IDs are hashed; location
    data is dropped; the actual telemetry metrics (HR, SpO2, etc.) are
    not PHI and are kept."""
    out = dict(event)
    # 13. Device identifiers → hashed surrogate
    if "device_id" in out and out["device_id"]:
        out["device_id"] = _hash(str(out["device_id"]), "iot-device")
    # Patient IDs: keep hashed surrogate for joinability
    if "patient_id" in out and out["patient_id"]:
        out["patient_id"] = _hash(str(out["patient_id"]), "iot-patient")
    # Drop any free-text that could contain PII
    for f in ("location", "geocoordinates", "free_text", "notes"):
        if f in out:
            out[f] = None
    return out


def deidentify_dict(record: dict, salt: str = "deid-2026-06") -> dict:
    """Best-effort Safe Harbor pass on an arbitrary dict. Walks all keys
    and applies field-name-based rules. Use the type-specific functions
    above for known record types — they preserve the schema better."""
    out = dict(record)
    for k in list(out.keys()):
        v = out[k]
        kl = k.lower()
        if v is None:
            continue
        # Recurse into nested dicts
        if isinstance(v, dict):
            out[k] = deidentify_dict(v, salt)
            continue
        # Names
        if kl in ("name", "first_name", "last_name", "full_name"):
            out[k] = _hash(str(v), salt + "-name")
        # Geographic
        elif kl in ("zip", "zip_code", "postal_code"):
            out[k] = redact_zip(str(v))
        elif kl in ("address_1", "address_2", "street"):
            out[k] = None
        # Contact
        elif kl in ("phone", "fax", "email"):
            out[k] = None
        # Identifiers
        elif kl in ("mrn", "ssn", "health_plan_number", "account_number",
                    "cert_number", "license_number", "device_id"):
            out[k] = _hash(str(v), salt + "-" + kl)
        # Dates → year only
        elif kl.endswith("_datetime") or kl.endswith("_date"):
            out[k] = date_to_year(v) if isinstance(v, str) else None
        # IP addresses → zero last octet
        elif kl in ("ip", "ip_address", "src_ip", "dst_ip"):
            if isinstance(v, str) and re.match(r"^\d+\.\d+\.\d+\.\d+$", v):
                parts = v.split(".")
                parts[-1] = "0"
                out[k] = ".".join(parts)
    return out
