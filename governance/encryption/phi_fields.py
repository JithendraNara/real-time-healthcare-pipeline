"""
PHI field registry — the single source of truth for which fields are PHI
and how they should be handled.

Healthcare data has 18 HIPAA Safe Harbor identifiers. This registry covers
the ones that actually appear in this pipeline's data model. Every consumer
that touches a PHI field should look it up here before deciding whether to
encrypt, mask, log, or pass it through.

Adding a new PHI field: append to PHI_FIELDS, then make sure every
producer/consumer/AI-analyst code path either encrypts on write or
audits-on-read (see governance.audit).
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterable


class PHICategory(str, Enum):
    NAME = "name"
    GEOGRAPHIC = "geographic"
    DATE = "date"               # birth, admission, discharge — dates are PHI except year
    CONTACT = "contact"         # phone, email, fax
    IDENTIFIER = "identifier"   # MRN, SSN, account number
    BIOMETRIC = "biometric"
    PHOTO = "photo"
    DEVICE = "device_id"        # serial numbers, IP/MAC addresses
    OTHER = "other"


class EncryptionMode(str, Enum):
    RANDOM = "random"           # AES-GCM with random IV — semantic security
    DETERMINISTIC = "deterministic"  # AES-GCM with deterministic IV — searchable
    NONE = "none"               # Not actually PHI; misclassified or year-only


@dataclass(frozen=True)
class PHIField:
    """Specification for one PHI field across the data model."""

    path: str                # dotted path, e.g. "person.mrn" or "patient.birth_datetime"
    category: PHICategory
    encryption: EncryptionMode
    safe_harbor: bool        # True if removing/redacting this field preserves utility under Safe Harbor
    description: str = ""


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


PHI_FIELDS: dict[str, PHIField] = {
    # Person / patient demographics (OMOP)
    "person.person_id": PHIField(
        "person.person_id", PHICategory.IDENTIFIER, EncryptionMode.DETERMINISTIC, True,
        "OMOP person_id — surrogate but considered identifier under HIPAA",
    ),
    "person.mrn": PHIField(
        "person.mrn", PHICategory.IDENTIFIER, EncryptionMode.DETERMINISTIC, True,
        "Medical record number — required for clinical workflows but searchable",
    ),
    "person.birth_datetime": PHIField(
        "person.birth_datetime", PHICategory.DATE, EncryptionMode.RANDOM, True,
        "Full birth datetime — must be encrypted or age-generalized for Safe Harbor",
    ),
    "person.year_of_birth": PHIField(
        "person.year_of_birth", PHICategory.DATE, EncryptionMode.NONE, True,
        "Year of birth only — Safe Harbor allows if age < 90",
    ),
    "person.gender_concept_id": PHIField(
        "person.gender_concept_id", PHICategory.OTHER, EncryptionMode.NONE, True,
        "OMOP concept id — not directly PHI but can re-identify small populations",
    ),
    "person.race_concept_id": PHIField(
        "person.race_concept_id", PHICategory.OTHER, EncryptionMode.NONE, True,
        "OMOP concept id — same caveat as gender",
    ),

    # Location
    "location.address_1": PHIField(
        "location.address_1", PHICategory.GEOGRAPHIC, EncryptionMode.RANDOM, True,
        "Street address line 1",
    ),
    "location.city": PHIField(
        "location.city", PHICategory.GEOGRAPHIC, EncryptionMode.NONE, False,
        "City-level geography is generally allowed under Safe Harbor",
    ),
    "location.state": PHIField(
        "location.state", PHICategory.GEOGRAPHIC, EncryptionMode.NONE, True,
        "State is Safe Harbor OK if first 3 zip digits are zeroed for low-pop areas",
    ),
    "location.zip": PHIField(
        "location.zip", PHICategory.GEOGRAPHIC, EncryptionMode.DETERMINISTIC, True,
        "ZIP — must drop last 3 digits for populations < 20k",
    ),

    # Contact
    "person.phone": PHIField(
        "person.phone", PHICategory.CONTACT, EncryptionMode.RANDOM, True,
    ),
    "person.email": PHIField(
        "person.email", PHICategory.CONTACT, EncryptionMode.RANDOM, True,
    ),

    # Visit / encounter
    "visit.admit_time": PHIField(
        "visit.admit_time", PHICategory.DATE, EncryptionMode.RANDOM, False,
        "Date alone is allowed for ages 90+; for clinical ops we encrypt and join on visit_occurrence_id",
    ),
    "visit.discharge_time": PHIField(
        "visit.discharge_time", PHICategory.DATE, EncryptionMode.RANDOM, False,
    ),

    # IoT / device identifiers
    "iot.device_id": PHIField(
        "iot.device_id", PHICategory.DEVICE, EncryptionMode.DETERMINISTIC, True,
        "Device serial — must be encrypted but searchable for telemetry correlation",
    ),
    "iot.firmware_version": PHIField(
        "iot.firmware_version", PHICategory.DEVICE, EncryptionMode.NONE, True,
        "Firmware version alone is not PHI but included for context",
    ),

    # Streaming event top-level fields (used by Module 1 events)
    "event.patient_id": PHIField(
        "event.patient_id", PHICategory.IDENTIFIER, EncryptionMode.DETERMINISTIC, True,
        "Streaming event patient identifier — joins to OMOP person",
    ),
    "event.encounter_id": PHIField(
        "event.encounter_id", PHICategory.IDENTIFIER, EncryptionMode.DETERMINISTIC, True,
        "Streaming event encounter/visit identifier — joins to OMOP visit_occurrence",
    ),
    "event.device_id": PHIField(
        "event.device_id", PHICategory.DEVICE, EncryptionMode.DETERMINISTIC, True,
        "Streaming event device identifier (IoT telemetry)",
    ),
    "event.mrn": PHIField(
        "event.mrn", PHICategory.IDENTIFIER, EncryptionMode.DETERMINISTIC, True,
        "Streaming event MRN",
    ),

    # Provider
    "provider.npi": PHIField(
        "provider.npi", PHICategory.IDENTIFIER, EncryptionMode.DETERMINISTIC, True,
        "National Provider Identifier",
    ),
    "provider.name": PHIField(
        "provider.name", PHICategory.NAME, EncryptionMode.RANDOM, True,
    ),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def is_phi(path: str) -> bool:
    return path in PHI_FIELDS


def get_phi_field(path: str) -> PHIField | None:
    return PHI_FIELDS.get(path)


def phi_fields_in_path(path_prefix: str) -> Iterable[PHIField]:
    """Return all PHI fields whose path starts with the given prefix."""
    return (f for k, f in PHI_FIELDS.items() if k.startswith(path_prefix))


def requires_deterministic(path: str) -> bool:
    spec = PHI_FIELDS.get(path)
    return spec is not None and spec.encryption == EncryptionMode.DETERMINISTIC
