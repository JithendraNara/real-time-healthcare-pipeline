"""
AES-256-GCM column-level encryption for PHI fields.

HIPAA's encryption addressable implementation specification requires "any
cryptographic mechanism that transforms the data into an unintelligible form"
with appropriate key management. AES-256-GCM is the de-facto standard:

  - 256-bit keys: brute-force infeasible
  - GCM mode: authenticated encryption (confidentiality + integrity)
  - 96-bit random IV per encryption: semantic security (same plaintext ≠ same ciphertext)
  - 128-bit auth tag: detects any tampering

PHI field values are encoded as base64-encoded JSON envelopes:

  {
    "v": 1,                    # envelope version
    "kid": "key-2026-06",      # key ID (rotation handle)
    "iv":  "<base64>",         # 12-byte IV
    "ct":  "<base64>",         # ciphertext
    "tag": "<base64>"          # 16-byte auth tag
  }

Deterministic mode (searchable encryption) is also supported — same plaintext
produces the same ciphertext when deterministic=True. Use it only for fields
that must be searchable/joinable on ciphertext (e.g., MRN lookup); never for
free-text PHI like name/address.

Key management is handled separately by KeyManager — CryptoService knows
nothing about where keys come from. That keeps the crypto layer testable
and swappable (local dev uses a fixed key in env, prod uses AWS KMS / Vault).
"""
from __future__ import annotations

import base64
import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Protocol

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

log = logging.getLogger("crypto_service")

# 12 bytes (96 bits) is the standard GCM IV size. Random per encryption
# provides semantic security when the same plaintext is encrypted twice.
IV_BYTES = 12
TAG_BYTES = 16
KEY_BYTES = 32  # AES-256

ENVELOPE_VERSION = 1


class KeyManager(Protocol):
    """Pluggable key source. Implementations: local env, AWS KMS, Vault."""

    def get_current_key(self) -> tuple[str, bytes]:
        """Returns (key_id, raw_key_bytes). Called on every encrypt() so the
        caller always uses the latest key."""
        ...

    def get_key_by_id(self, key_id: str) -> bytes:
        """Lookup a historical key by ID. Used during decrypt() of old ciphertext."""
        ...


@dataclass(frozen=True)
class LocalKeyManager:
    """Local-dev key manager. Reads the key from env (base64-encoded 32 bytes).
    For production, swap for AWSKmsKeyManager or VaultKeyManager."""

    env_var: str = "HEALTHCARE_KMS_KEY"
    key_id: str = "local-2026-06"

    def get_current_key(self) -> tuple[str, bytes]:
        raw = os.getenv(self.env_var)
        if not raw:
            raise RuntimeError(
                f"Encryption key not configured. Set {self.env_var} env var to a "
                f"base64-encoded 32-byte key. Generate one with: "
                f"  python -c 'import os,base64; print(base64.b64encode(os.urandom(32)).decode())'"
            )
        key = base64.b64decode(raw)
        if len(key) != KEY_BYTES:
            raise ValueError(f"key must be {KEY_BYTES} bytes, got {len(key)}")
        return self.key_id, key

    def get_key_by_id(self, key_id: str) -> bytes:
        # Local mode has a single key
        kid, key = self.get_current_key()
        if key_id != kid:
            raise KeyError(f"unknown key id: {key_id}")
        return key


# ---------------------------------------------------------------------------
# Envelope encode / decode
# ---------------------------------------------------------------------------


def _encode_envelope(key_id: str, iv: bytes, ct: bytes, aad: str | None = None, deterministic: bool = False) -> str:
    payload = {
        "v": ENVELOPE_VERSION,
        "kid": key_id,
        "iv": base64.b64encode(iv).decode("ascii"),
        "ct": base64.b64encode(ct).decode("ascii"),
    }
    if aad is not None:
        payload["aad"] = aad
    if deterministic:
        payload["deterministic"] = True
    return json.dumps(payload, separators=(",", ":"))


def _decode_envelope(envelope: str) -> tuple[str, bytes, bytes, bytes, str | None]:
    """Returns (key_id, iv, ciphertext, tag, aad). The tag is the last TAG_BYTES of ct
    in GCM mode — that's how AESGCM's combined output works."""
    if isinstance(envelope, dict):
        # Accept dicts in addition to strings (idempotent for already-decrypted)
        return envelope["kid"], base64.b64decode(envelope["iv"]), b"", b"", envelope.get("aad")
    data = json.loads(envelope)
    if data.get("v") != ENVELOPE_VERSION:
        raise ValueError(f"unsupported envelope version: {data.get('v')}")
    iv = base64.b64decode(data["iv"])
    ct = base64.b64decode(data["ct"])
    # AESGCM combined mode: ciphertext || tag (16 bytes)
    ciphertext, tag = ct[:-TAG_BYTES], ct[-TAG_BYTES:]
    return data["kid"], iv, ciphertext, tag, data.get("aad")


# ---------------------------------------------------------------------------
# CryptoService
# ---------------------------------------------------------------------------


class CryptoService:
    """High-level encrypt / decrypt for PHI field values.

    For non-deterministic (default) mode, the same plaintext yields different
    ciphertexts on every call — safe for name/address/etc.

    For deterministic mode, the same plaintext always yields the same ciphertext,
    which makes the field searchable / joinable on encrypted value. Use ONLY
    when the use case requires it (e.g., MRN lookup), and never for fields
    with high cardinality + low entropy (e.g., birth date alone).
    """

    def __init__(self, key_manager: KeyManager | None = None):
        self._km = key_manager or LocalKeyManager()
        # Track deterministic key per process (separate from random key) so we
        # can search on ciphertext. A real system would derive this from a
        # hash-based key + a pepper stored separately.
        self._det_key_id: str | None = None
        self._det_key: bytes | None = None

    def _det_key_handle(self) -> bytes:
        if self._det_key is None:
            # Derive a deterministic key from a separate env var (pepper).
            # Falls back to the main key if not set — both modes are still
            # AES-256, just with different derivation paths.
            self._det_key_id = self._km.get_current_key()[0] + "-det"
            pepper = os.getenv("HEALTHCARE_KMS_DETERMINISTIC_PEPPER")
            if pepper:
                import hashlib
                self._det_key = hashlib.sha256(base64.b64decode(pepper)).digest()
            else:
                _, k = self._km.get_current_key()
                self._det_key = k
        return self._det_key

    def encrypt(self, plaintext: str | bytes | None, deterministic: bool = False, aad: str | None = None) -> str:
        """Encrypt a PHI value. Returns a base64 JSON envelope.

        `aad` is "additional authenticated data" — bind ciphertext to context
        (e.g., patient_id) so the same value can't be swapped across records.
        """
        if plaintext is None:
            return None
        if plaintext == "":
            return ""
        if isinstance(plaintext, str):
            plaintext = plaintext.encode("utf-8")

        if deterministic:
            import hashlib
            # IV = HMAC(key, plaintext)[:12] — same plaintext always → same IV
            det_key = self._det_key_handle()
            iv = hashlib.sha256(det_key + plaintext).digest()[:IV_BYTES]
            aes = AESGCM(det_key)
            ct_with_tag = aes.encrypt(iv, plaintext, aad.encode("utf-8") if aad else None)
            # Use the deterministic-key id, not the random one
            return _encode_envelope(self._det_key_id, iv, ct_with_tag, aad=aad, deterministic=True)

        key_id, key = self._km.get_current_key()
        iv = os.urandom(IV_BYTES)
        aes = AESGCM(key)
        ct_with_tag = aes.encrypt(iv, plaintext, aad.encode("utf-8") if aad else None)
        return _encode_envelope(key_id, iv, ct_with_tag, aad=aad)

    def decrypt(self, envelope: str | dict, aad: str | None = None) -> str:
        """Decrypt a PHI envelope. Returns the plaintext string.

        If the envelope carries an `aad` field, that AAD is used automatically
        (and `aad` arg is ignored unless explicitly provided — at which point
        we use the explicit one and fail loud if it doesn't match). The default
        is to use the envelope's own AAD.

        Raises ValueError for malformed envelopes, InvalidKey/InvalidTag
        from cryptography for tampered/rotated ciphertext.
        """
        if not envelope:
            return ""
        if isinstance(envelope, dict):
            return ""  # already-decrypted idempotent
        kid, iv, ct, tag, env_aad = _decode_envelope(envelope)
        if not ct and not tag:
            return ""
        # Use the envelope's own AAD if the caller didn't override.
        # AAD mismatch will surface as InvalidTag from the GCM check — correct.
        use_aad = aad if aad is not None else env_aad
        # Lookup the right key (might be a historical one for rotated ciphertexts)
        try:
            key = self._km.get_key_by_id(kid)
        except KeyError:
            # Fall back to current key if the historical lookup fails —
            # the IV mismatch will cause InvalidTag, which is the correct error
            key = self._km.get_current_key()[1]
        aes = AESGCM(key)
        plaintext = aes.decrypt(iv, ct + tag, use_aad.encode("utf-8") if use_aad else None)
        return plaintext.decode("utf-8")

    def is_envelope(self, value: Any) -> bool:
        """Quick check for whether a value is a PHI envelope (vs. plaintext / None)."""
        if not isinstance(value, str) or not value.startswith("{"):
            return False
        try:
            data = json.loads(value)
            return data.get("v") == ENVELOPE_VERSION and "kid" in data and "ct" in data
        except json.JSONDecodeError:
            return False
