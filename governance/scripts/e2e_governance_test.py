"""
End-to-end governance smoke test.

Spins up Redpanda, publishes events with PHI fields, runs a governed consumer
that auto-encrypts PHI on read + writes audit events, then verifies:

  1. Consumer never sees plaintext PHI (only envelopes)
  2. Audit table contains one read event per PHI field accessed
  3. De-identification helpers produce Safe Harbor-compliant output
  4. RBAC engine denies a data scientist from reading raw PHI

This is the integration point that proves the entire HIPAA layer works.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def _wait_for_broker(bootstrap: str, timeout_sec: float = 15.0) -> bool:
    from confluent_kafka.admin import AdminClient
    admin = AdminClient({"bootstrap.servers": bootstrap})
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            md = admin.list_topics(timeout=2.0)
            return True
        except Exception:  # noqa: BLE001
            time.sleep(0.5)
    return False


def _publish_phi_events(bootstrap: str, topic: str) -> list[dict]:
    from confluent_kafka import Producer
    p = Producer({"bootstrap.servers": bootstrap, "acks": "all"})

    events = [
        {
            "event_id": "phi-1",
            "event_time": datetime.now(timezone.utc).isoformat(),
            "ingestion_time": datetime.now(timezone.utc).isoformat(),
            "source": "ehr",
            "schema_version": 1,
            "patient_id": "patient-12345",
            "mrn": "MRN-001",
            "encounter_id": "enc-001",
            "heart_rate_bpm": 72,
            "spo2_pct": 98.0,
        },
        {
            "event_id": "phi-2",
            "event_time": datetime.now(timezone.utc).isoformat(),
            "ingestion_time": datetime.now(timezone.utc).isoformat(),
            "source": "iot",
            "schema_version": 1,
            "patient_id": "patient-67890",
            "device_id": "dev-abc123",
            "metrics": {"heart_rate": 88, "spo2": 96.0},
        },
    ]
    for ev in events:
        p.produce(topic, key=ev["patient_id"].encode(), value=json.dumps(ev).encode())
    p.flush(10)
    return events


def _consume_with_governance(bootstrap: str, audit_path: Path, n: int, topic: str, timeout_sec: float = 8.0) -> list[dict]:
    from confluent_kafka import Consumer
    from governance.audit.audit_logger import AuditLogger, DuckDBAuditBackend
    from governance.encryption.encryptor import PHIEncryptor
    from governance.encryption.crypto import CryptoService, LocalKeyManager

    crypto = CryptoService(LocalKeyManager())
    encryptor = PHIEncryptor(crypto)
    backend = DuckDBAuditBackend(audit_path)
    audit = AuditLogger(backend, default_actor_id="governed-consumer-e2e")

    con = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"governed-{int(time.time())}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    con.subscribe([topic])

    collected: list[dict] = []
    deadline = time.time() + timeout_sec
    while time.time() < deadline and len(collected) < n:
        msg = con.poll(timeout=0.5)
        if msg is None or msg.error():
            continue
        try:
            raw = json.loads(msg.value())
        except Exception:
            continue
        # Only consume events from this test run (event_id starts with "phi-")
        if not raw.get("event_id", "").startswith("phi-"):
            continue
        # Encrypt PHI fields
        encrypted = encryptor.encrypt_event(raw)
        collected.append(encrypted)
        # Emit audit event for every PHI field accessed
        from governance.audit.audit_logger import Action
        # Top-level event fields are namespaced as event.* in the PHI registry
        phi_fields = [f"event.{k}" for k, v in encrypted.items() if encryptor.crypto.is_envelope(v)]
        audit.log(
            Action.READ,
            resource_type="topic",
            resource_id=msg.topic(),
            fields=phi_fields,
            purpose="clinical_care",
            context={"partition": msg.partition(), "offset": msg.offset()},
        )
    con.close()
    return collected


def _deidentify_round_trip() -> dict:
    """Verify Safe Harbor helpers."""
    from governance.masking.deidentify import (
        deidentify_omop_person, deidentify_visit, deidentify_iot_event
    )
    person = deidentify_omop_person({
        "person_id": 1, "mrn": "M001", "name": "Jane Doe",
        "birth_datetime": "1945-01-15T00:00:00Z", "year_of_birth": 1945,
        "phone": "555-1234",
    })
    visit = deidentify_visit({
        "visit_occurrence_id": 1,
        "visit_start_datetime": "2025-03-15T08:00:00Z",
    })
    iot = deidentify_iot_event({
        "patient_id": "p123", "device_id": "dev_abc", "metrics": {"hr": 72},
    })
    return {"person": person, "visit": visit, "iot": iot}


def _rbac_check() -> bool:
    from governance.rbac.policies import Actor, AccessRequest, PolicyEngine, Resource
    engine = PolicyEngine()
    # Data scientist tries to read raw MRN — must be denied
    req = AccessRequest(
        actor=Actor(id="alice", role="data_scientist"),
        action="read",
        resource=Resource(type="table", id="omcdm_person", fields=["person.mrn", "person.birth_datetime"]),
        purpose="model_training",
    )
    denied = not engine.evaluate(req).allow

    # Clinician reads the same fields — must be allowed
    req2 = AccessRequest(
        actor=Actor(id="bob", role="clinician"),
        action="read",
        resource=Resource(type="table", id="omcdm_person", fields=["person.mrn", "person.birth_datetime"]),
        purpose="clinical_care",
    )
    allowed = engine.evaluate(req2).allow
    return denied and allowed


def main() -> int:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    # Use a per-run topic so we don't pick up stale events from prior test runs
    test_topic = f"healthcare.gov_e2e.{int(time.time())}"
    print(f"[1/6] Waiting for broker at {bootstrap}…")
    if not _wait_for_broker(bootstrap):
        print("      ✗ broker not reachable")
        return 1
    print(f"      ✓ broker up")

    # Ensure HEALTHCARE_KMS_KEY is set
    if "HEALTHCARE_KMS_KEY" not in os.environ:
        import base64
        os.environ["HEALTHCARE_KMS_KEY"] = base64.b64encode(os.urandom(32)).decode()

    # Create the test topic
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": bootstrap})
    admin.create_topics([NewTopic(test_topic, num_partitions=1, replication_factor=1)], request_timeout=10)

    print(f"[2/6] Publishing 2 events with PHI to {test_topic}…")
    published = _publish_phi_events(bootstrap, test_topic)
    print(f"      ✓ published {len(published)} events")

    audit_db = Path(tempfile.mkdtemp(prefix="rthp-gov-")) / "audit.db"
    print(f"[3/6] Consuming with auto-encrypt + auto-audit ({audit_db})…")
    encrypted = _consume_with_governance(bootstrap, audit_db, n=2, topic=test_topic)
    if len(encrypted) < 2:
        print(f"      ✗ expected ≥2 encrypted events, got {len(encrypted)}")
        return 1
    print(f"      ✓ consumed and encrypted {len(encrypted)} events")

    print(f"[4/6] Verifying PHI was encrypted (no plaintext leaks)…")
    leaks = []
    for ev in encrypted:
        for k, v in ev.items():
            if k in ("patient_id", "mrn", "encounter_id", "device_id") and v is not None:
                if not isinstance(v, str) or not v.startswith("{"):
                    leaks.append((k, v))
    if leaks:
        print(f"      ✗ plaintext PHI leaked: {leaks}")
        return 1
    print(f"      ✓ all PHI fields are envelopes")

    print(f"[5/6] Verifying audit log captured the reads…")
    from governance.audit.audit_logger import DuckDBAuditBackend
    backend = DuckDBAuditBackend(audit_db)
    rows = backend.query(actor_id="governed-consumer-e2e")
    if len(rows) < 2:
        print(f"      ✗ expected ≥2 audit rows, got {len(rows)}")
        return 1
    fields_seen = set()
    for r in rows:
        fields_seen.update(r.get("fields", "").split(","))
    # The actual event fields are top-level (patient_id, mrn, etc.) — they get
    # namespaced to event.* by the PHIEncryptor. Audit emission also passes them
    # in the namespaced form. Just check that the right set of field names was
    # audited.
    expected_fields = {"event.patient_id", "event.mrn", "event.encounter_id", "event.device_id"}
    missing = expected_fields - fields_seen
    if missing:
        print(f"      ✗ missing audited fields: {missing}")
        print(f"      saw: {sorted(fields_seen)}")
        return 1
    print(f"      ✓ {len(rows)} audit events, fields covered: {sorted(fields_seen)}")

    print(f"[6/6] Verifying Safe Harbor + RBAC…")
    deid = _deidentify_round_trip()
    if not deid["person"]["mrn"].startswith("DH_"):
        print(f"      ✗ mrn not hashed in deid: {deid['person']}")
        return 1
    if deid["person"]["phone"] is not None:
        print(f"      ✗ phone not dropped in deid: {deid['person']}")
        return 1
    if deid["visit"]["visit_start_year"] != 2025:
        print(f"      ✗ visit year not extracted: {deid['visit']}")
        return 1
    if not _rbac_check():
        print("      ✗ RBAC check failed")
        return 1
    print(f"      ✓ Safe Harbor: MRN hashed, phone dropped, year extracted")
    print(f"      ✓ RBAC: data scientist denied PHI; clinician allowed PHI")

    print()
    print("=" * 60)
    print("END-TO-END GOVERNANCE — PASS")
    print("=" * 60)
    print()
    print("PHI envelope sample (patient_id):")
    print(f"  {encrypted[0]['patient_id']}")
    print()
    print("De-identified person record:")
    print(f"  {json.dumps(deid['person'], indent=2)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
