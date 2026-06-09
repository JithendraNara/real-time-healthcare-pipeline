"""
Tests for the governance module — encryption, audit, de-identification, RBAC.

No external services needed. The audit backend uses a tempdir DuckDB; the
crypto service uses a generated local key.
"""
from __future__ import annotations

import base64
import json
import os
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from governance.audit.audit_logger import (  # noqa: E402
    Action,
    ActorType,
    AuditLogger,
    DuckDBAuditBackend,
    Outcome,
)
from governance.encryption.crypto import (  # noqa: E402
    CryptoService,
    LocalKeyManager,
)
from governance.encryption.encryptor import PHIEncryptor  # noqa: E402
from governance.encryption.phi_fields import (  # noqa: E402
    PHI_FIELDS,
    EncryptionMode,
    is_phi,
    requires_deterministic,
)
from governance.masking.deidentify import (  # noqa: E402
    date_to_year,
    deidentify_dict,
    deidentify_iot_event,
    deidentify_omop_person,
    deidentify_visit,
    generalize_age,
    redact_zip,
)
from governance.rbac.policies import (  # noqa: E402
    AccessRequest,
    Actor,
    PolicyEngine,
    Resource,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def local_key():
    """Generate a fresh 32-byte base64 key for the test session."""
    key = base64.b64encode(os.urandom(32)).decode("ascii")
    os.environ["HEALTHCARE_KMS_KEY"] = key
    yield key
    del os.environ["HEALTHCARE_KMS_KEY"]


@pytest.fixture
def crypto(local_key):
    return CryptoService(LocalKeyManager())


@pytest.fixture
def encryptor(crypto):
    return PHIEncryptor(crypto)


@pytest.fixture
def audit_db():
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "audit.db"
        yield path


@pytest.fixture
def audit(audit_db):
    backend = DuckDBAuditBackend(audit_db)
    return AuditLogger(backend, default_actor_id="test-actor")


# ---------------------------------------------------------------------------
# Crypto
# ---------------------------------------------------------------------------


def test_crypto_roundtrip(crypto):
    pt = "Jane Doe"
    env = crypto.encrypt(pt)
    assert env != pt
    assert crypto.is_envelope(env)
    assert crypto.decrypt(env) == pt


def test_crypto_random_iv_semantic_security(crypto):
    """Same plaintext encrypted twice should yield different ciphertexts (random IV)."""
    a = crypto.encrypt("same value")
    b = crypto.encrypt("same value")
    assert a != b
    # But both decrypt to the same plaintext
    assert crypto.decrypt(a) == crypto.decrypt(b) == "same value"


def test_crypto_deterministic_mode(crypto):
    """Same plaintext + same mode → same ciphertext (searchable)."""
    a = crypto.encrypt("MRN-12345", deterministic=True)
    b = crypto.encrypt("MRN-12345", deterministic=True)
    assert a == b
    assert crypto.decrypt(a) == "MRN-12345"


def test_crypto_empty_string(crypto):
    """Empty string encrypts to empty (not envelope)."""
    assert crypto.encrypt("") == ""
    assert crypto.encrypt(None) is None


def test_crypto_is_envelope(crypto):
    env = crypto.encrypt("hi")
    assert crypto.is_envelope(env) is True
    assert crypto.is_envelope("not json") is False
    assert crypto.is_envelope("{}") is False
    assert crypto.is_envelope(None) is False


def test_crypto_tamper_detection(crypto):
    """Flipping a byte in the ciphertext should fail auth (GCM tag)."""
    from cryptography.exceptions import InvalidTag

    env = crypto.encrypt("important PHI")
    data = json.loads(env)
    # Flip a bit in the ciphertext
    ct = bytearray(base64.b64decode(data["ct"]))
    ct[5] ^= 0xFF
    data["ct"] = base64.b64encode(bytes(ct)).decode()
    tampered = json.dumps(data)
    with pytest.raises(InvalidTag):
        crypto.decrypt(tampered)


def test_crypto_aad_binding(crypto):
    """Same plaintext bound to different AAD should yield different ciphertext."""
    a = crypto.encrypt("x", aad="patient_A")
    b = crypto.encrypt("x", aad="patient_B")
    assert a != b
    # Decryption with wrong AAD fails
    from cryptography.exceptions import InvalidTag
    with pytest.raises(InvalidTag):
        crypto.decrypt(a, aad="patient_B")


# ---------------------------------------------------------------------------
# PHI field registry
# ---------------------------------------------------------------------------


def test_phi_field_registry_has_expected_fields():
    assert "person.mrn" in PHI_FIELDS
    assert "person.birth_datetime" in PHI_FIELDS
    assert "visit.admit_time" in PHI_FIELDS
    assert "iot.device_id" in PHI_FIELDS


def test_phi_field_registry_deterministic_modes():
    assert requires_deterministic("person.mrn") is True
    assert requires_deterministic("person.birth_datetime") is False
    assert requires_deterministic("iot.device_id") is True
    assert is_phi("not.a.phi.field") is False


def test_phi_field_registry_categories():
    from governance.encryption.phi_fields import PHICategory
    assert PHI_FIELDS["person.mrn"].category == PHICategory.IDENTIFIER
    assert PHI_FIELDS["person.birth_datetime"].category == PHICategory.DATE
    assert PHI_FIELDS["person.gender_concept_id"].encryption == EncryptionMode.NONE


# ---------------------------------------------------------------------------
# PHI encryptor
# ---------------------------------------------------------------------------


def test_encryptor_encrypts_phi_only(encryptor):
    rec = {
        "person_id": "12345",
        "mrn": "M001",
        "heart_rate_bpm": 72,  # not PHI
        "spo2_pct": 98.0,        # not PHI
    }
    out = encryptor.encrypt_dict(rec, prefix="person", context_id="12345")
    # PHI fields encrypted
    assert encryptor.crypto.is_envelope(out["mrn"])
    assert encryptor.crypto.is_envelope(out["person_id"])
    # Non-PHI fields untouched
    assert out["heart_rate_bpm"] == 72
    assert out["spo2_pct"] == 98.0


def test_encryptor_deterministic_for_searchable(encryptor):
    """MRN should be deterministically encrypted so joins still work."""
    a = encryptor.encrypt_value("person.mrn", "M001", context_id="p1")
    b = encryptor.encrypt_value("person.mrn", "M001", context_id="p1")
    # Same plaintext + same context → same ciphertext (searchable + AAD-bound)
    assert a == b
    c = encryptor.encrypt_value("person.mrn", "M001", context_id="p2")
    # Different context → different ciphertext
    assert a != c


def test_encryptor_decrypt_dict(encryptor):
    rec = {"person_id": "12345", "mrn": "M001", "name": "Jane"}
    enc = encryptor.encrypt_dict(rec, prefix="person", context_id="12345")
    dec = encryptor.decrypt_event(enc)
    assert dec["person_id"] == "12345"
    assert dec["mrn"] == "M001"


def test_encryptor_event_roundtrip(encryptor):
    event = {
        "event_id": "e1",
        "patient_id": "p123",
        "heart_rate_bpm": 72,
        "spo2_pct": 98.0,
    }
    enc = encryptor.encrypt_event(event)
    # patient_id is encrypted, vitals are not
    assert encryptor.crypto.is_envelope(enc["patient_id"])
    assert enc["heart_rate_bpm"] == 72
    dec = encryptor.decrypt_event(enc)
    assert dec["patient_id"] == "p123"


# ---------------------------------------------------------------------------
# Audit logger
# ---------------------------------------------------------------------------


def test_audit_append_and_count(audit, audit_db):
    assert audit.backend.count() == 0
    audit.read("topic", "healthcare.vitals", ["person.mrn"], purpose="clinical_care")
    audit.write("topic", "iot.telemetry", ["iot.device_id"], purpose="ingest")
    assert audit.backend.count() == 2


def test_audit_query_by_actor(audit):
    audit.read("topic", "healthcare.vitals", ["person.mrn"], actor_id="alice")
    audit.read("topic", "iot.telemetry", ["iot.device_id"], actor_id="bob")
    rows = audit.backend.query(actor_id="alice")
    assert len(rows) == 1
    assert rows[0]["actor_id"] == "alice"


def test_audit_failure_does_not_block(audit, monkeypatch, caplog):
    """Audit failures log to stderr but don't raise — the hot path keeps moving."""
    def boom(_event):
        raise RuntimeError("simulated DB outage")

    monkeypatch.setattr(audit.backend, "append", boom)
    # Should not raise
    audit.read("topic", "healthcare.vitals", ["person.mrn"])
    assert "AUDIT WRITE FAILED" in caplog.text


def test_audit_event_envelope_shape(audit):
    ev = audit.log(
        Action.READ,
        resource_type="table",
        resource_id="omcdm_person",
        fields=["person.mrn", "person.birth_datetime"],
        purpose="clinical_care",
        actor_id="clinician-1",
        actor_type=ActorType.USER,
        context={"trace_id": "abc-123"},
    )
    assert ev.audit_id
    assert ev.actor_id == "clinician-1"
    assert ev.actor_type == "user"
    assert ev.action == "read"
    assert ev.outcome == "success"
    assert ev.context == {"trace_id": "abc-123"}
    assert "person.mrn" in ev.fields


# ---------------------------------------------------------------------------
# De-identification
# ---------------------------------------------------------------------------


def test_redact_zip_drops_last_three():
    assert redact_zip("46802") == "46***"
    assert redact_zip("46802-1234") == "46***-1234"
    assert redact_zip(None) is None
    assert redact_zip("") is None
    assert redact_zip("123") == "00000"


def test_generalize_age_caps_at_90():
    from datetime import datetime, timezone, timedelta
    bd = (datetime.now(timezone.utc) - timedelta(days=91 * 365)).isoformat()
    assert generalize_age(bd) == 90
    bd = (datetime.now(timezone.utc) - timedelta(days=50 * 365)).isoformat()
    assert generalize_age(bd) == 50


def test_date_to_year():
    assert date_to_year("2025-03-15T08:00:00Z") == 2025
    assert date_to_year("not a date") is None
    assert date_to_year(None) is None


def test_deidentify_omop_person_drops_phi():
    rec = {
        "person_id": 12345,
        "mrn": "M001",
        "birth_datetime": "1945-01-15T00:00:00Z",
        "year_of_birth": 1945,
        "phone": "555-1234",
        "email": "jane@example.com",
        "name": "Jane Doe",
    }
    out = deidentify_omop_person(rec)
    assert out["mrn"].startswith("DH_")
    assert out["birth_datetime"] is None
    assert out["phone"] is None
    assert out["email"] is None
    assert out["name"].startswith("DH_")
    assert out["year_of_birth"] == 1945  # Safe Harbor allows year when age < 90


def test_deidentify_omop_person_caps_age_at_90():
    from datetime import datetime, timezone, timedelta
    rec = {
        "person_id": 1,
        "mrn": "M",
        "year_of_birth": (datetime.now(timezone.utc) - timedelta(days=95 * 365)).year,
    }
    out = deidentify_omop_person(rec)
    # Year is capped to 1926 (90+ years ago from 2026) per Safe Harbor
    assert out["year_of_birth"] <= 1926


def test_deidentify_visit_dates_to_year():
    rec = {
        "visit_occurrence_id": 1,
        "visit_start_datetime": "2025-03-15T08:00:00Z",
        "visit_end_datetime": "2025-03-17T14:00:00Z",
    }
    out = deidentify_visit(rec)
    assert out["visit_start_datetime"] is None
    assert out["visit_end_datetime"] is None
    assert out["visit_start_year"] == 2025
    assert out["visit_end_year"] == 2025


def test_deidentify_iot_event_hashes_ids():
    ev = {
        "patient_id": "p123",
        "device_id": "dev_abc",
        "metrics": {"heart_rate": 72, "spo2": 98.0},
    }
    out = deidentify_iot_event(ev)
    assert out["patient_id"].startswith("DH_")
    assert out["device_id"].startswith("DH_")
    # Metrics are not PHI — preserved
    assert out["metrics"]["heart_rate"] == 72
    assert out["metrics"]["spo2"] == 98.0


def test_deidentify_dict_walks_arbitrary_structure():
    rec = {
        "person_id": "1",
        "name": "Jane",
        "phone": "555-1234",
        "zip": "46802",
        "mrn": "M001",
        "birth_datetime": "1980-01-01T00:00:00Z",
        "ip": "10.0.1.42",
        "details": {"ssn": "123-45-6789"},
    }
    out = deidentify_dict(rec)
    assert out["name"].startswith("DH_")
    assert out["phone"] is None
    assert out["zip"] == "46***"
    assert out["mrn"].startswith("DH_")
    assert out["birth_datetime"] == 1980
    assert out["ip"] == "10.0.1.0"
    assert out["details"]["ssn"].startswith("DH_")


# ---------------------------------------------------------------------------
# RBAC
# ---------------------------------------------------------------------------


def test_rbac_admin_allowed_everything():
    engine = PolicyEngine()
    req = AccessRequest(
        actor=Actor(id="admin1", role="admin"),
        action="export",
        resource=Resource(type="table", id="omcdm_person", fields=["person.mrn", "person.birth_datetime"]),
        purpose="compliance_audit",
    )
    d = engine.evaluate(req)
    assert d.allow
    assert d.filtered_fields == ["person.mrn", "person.birth_datetime"]


def test_rbac_clinician_allowed_clinical_read():
    engine = PolicyEngine()
    req = AccessRequest(
        actor=Actor(id="doc1", role="clinician"),
        action="read",
        resource=Resource(type="table", id="omcdm_person", fields=["person.mrn", "person.birth_datetime"]),
        purpose="clinical_care",
    )
    d = engine.evaluate(req)
    assert d.allow


def test_rbac_data_scientist_denied_phi():
    engine = PolicyEngine()
    req = AccessRequest(
        actor=Actor(id="alice", role="data_scientist"),
        action="read",
        resource=Resource(type="table", id="omcdm_person", fields=["person.mrn", "person.birth_datetime"]),
        purpose="model_training",
    )
    d = engine.evaluate(req)
    assert not d.allow


def test_rbac_data_scientist_allowed_omop_only():
    engine = PolicyEngine()
    req = AccessRequest(
        actor=Actor(id="alice", role="data_scientist"),
        action="read",
        resource=Resource(type="table", id="omcdm_condition_occurrence", fields=["omcdm_condition_occurrence.condition_concept_id"]),
        purpose="model_training",
    )
    d = engine.evaluate(req)
    assert d.allow


def test_rbac_purpose_not_allowed():
    engine = PolicyEngine()
    req = AccessRequest(
        actor=Actor(id="alice", role="analyst"),
        action="read",
        resource=Resource(type="table", id="mart_member_roster", fields=["mart_member_roster.member_id"]),
        purpose="marketing",  # not in the analyst's allowed purposes
    )
    d = engine.evaluate(req)
    assert not d.allow


def test_rbac_unknown_role_denied():
    engine = PolicyEngine()
    req = AccessRequest(
        actor=Actor(id="x", role="ghost"),
        action="read",
        resource=Resource(type="table", id="x", fields=["x"]),
        purpose="clinical_care",
    )
    d = engine.evaluate(req)
    assert not d.allow


def test_rbac_model_can_score():
    engine = PolicyEngine()
    req = AccessRequest(
        actor=Actor(id="readmission_v1", role="model", type="model"),
        action="read",
        resource=Resource(type="table", id="vitals", fields=["vitals.heart_rate_bpm", "person.age"]),
        purpose="model_inference",
    )
    d = engine.evaluate(req)
    assert d.allow


def test_rbac_opa_rego_string_present():
    """The rego policy should be available for production deployment."""
    from governance.rbac.policies import OPA_REGO_POLICY
    assert "package healthcare.rbac" in OPA_REGO_POLICY
    assert "default allow = false" in OPA_REGO_POLICY
    assert "data_scientist" in OPA_REGO_POLICY
