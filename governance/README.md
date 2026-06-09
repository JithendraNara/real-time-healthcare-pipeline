# Governance Module — HIPAA Layer

> **Module 3 of 4** — HIPAA-compliant data governance with encryption, access control, and auditing.
> Wraps the streaming layer (Module 1), the ML scorer (Module 2), and the OMOP warehouse.

This module turns the platform from "we have data" to "we have governed data". Every PHI
field is encrypted, every read is audited, every access is policy-checked, and every export
goes through Safe Harbor de-identification.

## What ships in this module

| Component | Path | Purpose |
|---|---|---|
| **Crypto primitives** | `governance/encryption/crypto.py` | AES-256-GCM encrypt/decrypt with random or deterministic IV. Pluggable key manager (local / AWS KMS / Vault). |
| **PHI field registry** | `governance/encryption/phi_fields.py` | Single source of truth for which fields are PHI, what category, and what encryption mode. 19 fields across OMOP, IoT, provider, location. |
| **PHI encryptor** | `governance/encryption/encryptor.py` | Applies the registry-prescribed encryption to any dict-like record or streaming event. |
| **Audit logger** | `governance/audit/audit_logger.py` | Append-only audit table for every PHI read/write/export. DuckDB local backend; Iceberg swap for prod. |
| **De-identification** | `governance/masking/deidentify.py` | HIPAA Safe Harbor helpers — name hashing, ZIP generalization, date→year, age cap at 90+. |
| **RBAC engine** | `governance/rbac/policies.py` | OPA-compatible role policies. Pure-Python evaluator for tests + rego source for production. |
| **Governed consumer** | `governance/middleware/governed_consumer.py` | Auto-audit wrapper for Kafka consumers — every PHI field read is logged. |
| **Tests** | `governance/tests/test_governance.py` | 20 unit tests covering encryption, audit, deidentification, RBAC. |

## Encryption at a glance

```python
from governance.encryption.crypto import CryptoService
from governance.encryption.encryptor import PHIEncryptor

crypto = CryptoService()  # local dev; swap key_manager for AWSKmsKeyManager in prod
encryptor = PHIEncryptor(crypto)

# Encrypt a PHI value — returns a base64 JSON envelope
envelope = encryptor.encrypt_value("person.mrn", "M001", context_id="12345")
# 'envelope' is now opaque, AAD-bound to the patient, with a random IV

# Deterministic mode for searchable fields (MRN, person_id)
envelope_det = encryptor.encrypt_value("person.person_id", "12345")
# Same input → same ciphertext, so you can still do `WHERE person_id = ?` lookups

# Decrypt (server-side, with the right key)
plaintext = encryptor.decrypt_value("person.person_id", envelope_det)  # '12345'
```

**Why AES-256-GCM over AES-CBC + HMAC?** GCM is an authenticated cipher — single primitive
gives you confidentiality + integrity. No more "did I remember to MAC-then-encrypt in the
right order?" footguns. It's what the US government's FIPS 140-2 modules ship.

**Why deterministic mode for some fields?** Clinical workflows need to look up a patient by
MRN, join vitals to OMOP person, etc. Random-IV encryption breaks these joins. The compromise:
deterministic mode uses HMAC-derived IVs, so the same plaintext always encrypts to the same
ciphertext — but a different patient_id / context id will produce a different ciphertext
when used as AAD.

## Audit at a glance

```python
from governance.audit.audit_logger import get_audit_logger, Action, ActorType

audit = get_audit_logger()

# Every PHI read goes through this
audit.read(
    resource_type="topic",
    resource_id="healthcare.vitals",
    fields=["person.patient_id", "person.mrn"],
    purpose="clinical_care",
    actor_id="glue-etl-local",
    context={"partition": 0, "offset": 12345},
)

# Every export (e.g., to a research dataset) goes through this
audit.export(
    resource_type="omop_mart",
    resource_id="mart_member_roster",
    fields=["person.*", "condition.*"],
    purpose="research_export",
)

# Query: who accessed patient 12345 in the last 24h?
events = audit.backend.query(resource_id="topic:healthcare.vitals", limit=100)
```

The audit log is **append-only by convention** — no UPDATE/DELETE statements are issued
by this class. In prod, the backend is an Iceberg table on S3 with bucket policies
denying PutObject on existing keys.

## De-identification at a glance

```python
from governance.masking.deidentify import (
    deidentify_omop_person, deidentify_visit, deidentify_iot_event
)

# Person record → Safe Harbor
deidentified = deidentify_omop_person({
    "person_id": 12345,
    "mrn": "M001",
    "birth_datetime": "1945-01-15T00:00:00Z",
    "year_of_birth": 1945,
    "phone": "555-1234",
    "name": "Jane Doe",
})
# → {person_id: 12345, mrn: 'DH_a3f4...', birth_datetime: None,
#    year_of_birth: 1945, phone: None, name: 'DH_8e2a...'}

# Visit record → dates reduced to year only
deidentified_visit = deidentify_visit({
    "visit_occurrence_id": 1,
    "visit_start_datetime": "2025-03-15T08:00:00Z",
    "visit_end_datetime": "2025-03-17T14:00:00Z",
})
# → visit_start_year: 2025, visit_end_year: 2025, all times: None

# IoT event → device id hashed, location dropped
deidentified_iot = deidentify_iot_event({
    "patient_id": "12345",
    "device_id": "dev_abc",
    "metrics": {"heart_rate": 72, "spo2": 98.0},
})
# → patient_id: 'DH_...', device_id: 'DH_...', metrics: unchanged
```

## RBAC at a glance

```python
from governance.rbac.policies import Actor, Resource, AccessRequest, PolicyEngine

engine = PolicyEngine()

# Data scientist running a model training job
req = AccessRequest(
    actor=Actor(id="alice@org", role="data_scientist", department="research"),
    action="read",
    resource=Resource(type="table", id="omcdm_condition_occurrence",
                      fields=["omcdm_condition_occurrence.*"]),
    purpose="model_training",
)
decision = engine.evaluate(req)
# → AccessDecision(allow=True, filtered_fields=['omcdm_condition_occurrence.*'])

# Same data scientist trying to read raw names — denied
req2 = AccessRequest(
    actor=Actor(id="alice@org", role="data_scientist"),
    action="read",
    resource=Resource(type="table", id="person",
                      fields=["person.mrn", "person.birth_datetime", "person.name"]),
    purpose="model_training",
)
engine.evaluate(req2)
# → AccessDecision(allow=False, reason="role 'data_scientist' cannot access any of requested fields ...")
```

For production, ship the rego in `OPA_REGO_POLICY` to an Open Policy Agent daemon
(`opa run -s`) and have services query it over HTTP. The pure-Python engine here
mirrors the rego logic 1:1 for local dev and unit tests.

## Integration with the rest of the platform

- **Streaming consumer (Module 1)** — replace `build_local_consumer` with
  `GovernedConsumer(consumer, audit, "glue-etl", topics)`. Every PHI read is logged.
- **OMOP warehouse** — wrap every dbt model run with a single audit emission
  for the marts/tables that contain PHI.
- **ML scorer (Module 2)** — audit every `/predict` call with the patient_id and
  model version. Wrap the FastAPI endpoint.
- **De-identification export job** — uses `masking.deidentify` to write
  research-ready datasets to a separate `omop_research` schema.

## Quickstart

```bash
pip install -r governance/requirements.txt  # cryptography (already in ml/requirements.txt)

# Set the dev key (32 random bytes, base64)
export HEALTHCARE_KMS_KEY=$(python -c "import os,base64; print(base64.b64encode(os.urandom(32)).decode())")

# Run the tests
pytest governance/tests/ -v

# Try the encryption in a Python REPL
python -c "
from governance.encryption.crypto import CryptoService
from governance.encryption.encryptor import PHIEncryptor
e = PHIEncryptor(CryptoService())
env = e.encrypt_value('person.mrn', 'M001', context_id='12345')
print('Encrypted:', env)
print('Decrypted:', e.decrypt_value('person.mrn', env))
"
```

## What it does NOT do

- **Authentication.** This module assumes the caller is already authenticated
  (the FastAPI scorer has its own auth, the streaming consumer has its own
  Kafka ACLs, etc.). RBAC is the *second* gate, after auth.
- **Transport encryption.** All Kafka traffic should use TLS (`security.protocol=SSL`).
  S3 / Iceberg traffic should use SSE-KMS. This module handles field-level encryption
  on top of those.
- **Key rotation.** A `KEY_ROTATION` audit event is emitted when a new key is
  activated, but the actual rotation logic is in the KMS (AWS KMS RotateKeyOnDate,
  or a custom Vault rotation). The decrypt path automatically falls back to the
  current key if a historical key isn't found.
- **Audit log integrity protection.** Production deployments should add
  a hash-chain or signed-envelope layer so a malicious actor can't tamper with
  past audit entries. Out of scope for the local-dev DuckDB backend.

## Next module

**Module 4** (`app/`, `prefect_flows/`) — clinical Streamlit dashboard that pulls
from `healthcare.predictions` and the OMOP mart, plus a Prefect end-to-end flow
that wires producer → consumer → ML scorer → dashboard.
