"""
PHI encryptor — applies the right encryption mode (random vs deterministic)
to a dict-like record based on the PHI field registry.

Two usage patterns:

  1. Wrap a record on write:
     encrypt_record({"person_id": "123", "mrn": "M001", "name": "Jane"}, prefix="person")

  2. Wrap an entire event:
     encrypt_event(vitals_event.model_dump())

The result is the same dict with PHI fields replaced by envelope strings.
The consumer / audit / de-identification layers then know what to do.
"""
from __future__ import annotations

import logging
from typing import Any

from governance.encryption.crypto import CryptoService
from governance.encryption.phi_fields import (
    EncryptionMode,
    PHI_FIELDS,
    get_phi_field,
    requires_deterministic,
)

log = logging.getLogger("phi_encryptor")


class PHIEncryptor:
    """Applies the registry-prescribed encryption to any dict-like record."""

    def __init__(self, crypto: CryptoService | None = None):
        self.crypto = crypto or CryptoService()

    def encrypt_value(self, path: str, value: Any, context_id: str | None = None) -> Any:
        """Encrypt a single value. Returns the envelope (str) or the value unchanged
        if not PHI / not a string-able type."""
        spec = get_phi_field(path)
        if spec is None or spec.encryption == EncryptionMode.NONE or value is None or value == "":
            return value
        if not isinstance(value, (str, int, float)):
            return value  # can't encrypt non-scalar cleanly
        deterministic = spec.encryption == EncryptionMode.DETERMINISTIC
        aad = context_id  # bind ciphertext to a record id
        return self.crypto.encrypt(str(value), deterministic=deterministic, aad=aad)

    def decrypt_value(self, path: str, envelope: Any) -> Any:
        """Decrypt a single value. Returns plaintext or the value unchanged
        if not an envelope."""
        if not self.crypto.is_envelope(envelope):
            return envelope
        return self.crypto.decrypt(envelope)

    def encrypt_dict(self, record: dict, prefix: str = "", context_id: str | None = None, skip_envelope_check: bool = True) -> dict:
        """
        Encrypt PHI fields in a dict. `prefix` is the path prefix (e.g., "person")
        so the function knows which field names are PHI under that namespace.

        If skip_envelope_check is True (default), already-encrypted values are
        re-encrypted (idempotency requires skipping — set False if you want
        to force re-encryption, e.g., during a key rotation).
        """
        out: dict[str, Any] = {}
        for k, v in record.items():
            full_path = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                out[k] = self.encrypt_dict(v, prefix=full_path, context_id=context_id, skip_envelope_check=skip_envelope_check)
                continue
            spec = get_phi_field(full_path)
            if spec is None or spec.encryption == EncryptionMode.NONE:
                out[k] = v
                continue
            if v is None or v == "":
                out[k] = v
                continue
            if not skip_envelope_check and self.crypto.is_envelope(v):
                out[k] = v
                continue
            out[k] = self.encrypt_value(full_path, v, context_id=context_id)
        return out

    def encrypt_event(self, event: dict, topic: str | None = None) -> dict:
        """
        Encrypt PHI fields in a streaming event. Topics typically carry:
          - patient_id, encounter_id (identifiers — DETERMINISTIC)
          - device_id (device — DETERMINISTIC)
        The actual measurement values (HR, SpO2, etc.) are NOT PHI.
        """
        ctx = event.get("patient_id") or event.get("device_id")
        return self.encrypt_dict(event, prefix="event", context_id=str(ctx) if ctx else None)

    def decrypt_event(self, event: dict) -> dict:
        """Decrypt PHI fields in a streaming event. Inverse of encrypt_event."""
        out: dict = {}
        for k, v in event.items():
            if isinstance(v, dict):
                out[k] = self.decrypt_event(v)
                continue
            if self.crypto.is_envelope(v):
                out[k] = self.crypto.decrypt(v)
            else:
                out[k] = v
        return out
