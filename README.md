# real-time-healthcare-pipeline

> **Real-time healthcare & IoT data — modernized for 2026.**
> **Synthea + IoT → Kafka (Redpanda) → AWS Glue Streaming → Iceberg v3 → ML predictions → clinical dashboard.**
> **HIPAA-governed. Zero-AWS local dev. Production-ready for AWS.**

[![CI](https://github.com/Jithendranara/real-time-healthcare-pipeline/actions/workflows/ci.yaml/badge.svg)](https://github.com/Jithendranara/real-time-healthcare-pipeline/actions)
[![OMOP CDM](https://img.shields.io/badge/OMOP-v5.4-blue)](https://ohdsi.github.io/CommonDataModel/)
[![Kafka](https://img.shields.io/badge/Kafka-Redpanda-black)](https://redpanda.com/)
[![AWS Glue](https://img.shields.io/badge/AWS-Glue_Streaming-orange)](https://aws.amazon.com/glue/)
[![Iceberg v3](https://img.shields.io/badge/Apache_Iceberg-v3-lightgrey)](https://iceberg.apache.org/)
[![dbt Fusion](https://img.shields.io/badge/dbt-Fusion-orange)](https://www.getdbt.com/)
[![HIPAA](https://img.shields.io/badge/HIPAA-governed-blueviolet)](#-hipaa-posture)
[![AI Analyst](https://img.shields.io/badge/AI-MiniMax--M2-green)](https://api.minimax.chat/)

A production-quality healthcare data platform combining **real-time streaming** (Kafka + AWS Glue) with the **batch OMOP warehouse** (Synthea → dbt → Iceberg) — plus a natural-language AI analyst, ML outcome models, and HIPAA-grade governance. Runs on your laptop with Docker; deploys to AWS without code changes.

---

## 🏗️ Architecture

```
 ┌────────────────────────── REAL-TIME (Module 1) ─────────────────────────┐
 │                                                                         │
 │  [Synthea EHR] ─┐                                                        │
 │  [IoT devices] ─┤                                                        │
 │                  ├──▶ [Kafka / Redpanda] ──▶ [AWS Glue Streaming ETL]    │
 │  [Wearables]   ─┘                            │                          │
 │                                               ├── validate (JSON Schema)│
 │                                               ├── enrich (join OMOP)     │
 │                                               ├── transform → Iceberg    │
 │                                               └── DLQ (malformed)        │
 │                                                          │               │
 │                                                          ▼               │
 │  [ML scorer (Module 2)] ◀── silver tables ─── [Iceberg v3 silver]        │
 │         │                                                              │
 │         └────▶ healthcare.predictions topic ─────▶ [clinical dashboard] │
 └─────────────────────────────────────────────────────────────────────────┘

 ┌────────────────────────── BATCH (existing) ─────────────────────────────┐
 │                                                                         │
 │  [Synthea CSVs]   ─┐                                                     │
 │                    │                                                     │
 │  [Eligibility]    ─┤                                                     │
 │                    ├──▶ [dbt Fusion] ──▶ [OMOP CDM v5.4 (Iceberg/DuckDB)]│
 │  [Claims]        ─┘                          │                         │
 │                                               ├── person, condition, …   │
 │                                               └── mart_member_roster     │
 │                                                        │                │
 │                                                        ▼                │
 │                                            [AI Healthcare Analyst]      │
 │                                            FastAPI + MiniMax-M2         │
 │                                                        │                │
 │                                                        ├── /ask         │
 │                                                        ├── /plan         │
 │                                                        ├── /cohort       │
 │                                                        └── /schema       │
 └─────────────────────────────────────────────────────────────────────────┘

       ──── HIPAA governance (Module 3) wraps every read/write ────
       encryption + RBAC + audit + de-identification
```

**Four layers of data quality:**

1. **JSON Schema** at the Kafka boundary — reject malformed events at the door (Module 1)
2. **Great Expectations** — column-level rules (uniqueness, regex, ranges)
3. **OMOP row-level** — referential integrity, valid concept_ids, CCS coverage
4. **Iceberg freshness** — snapshot age checks (replaces the old "is the pipeline running" alerts)

---

## 🧱 The Stack (2026)

| Layer | Technology |
|-------|-----------|
| **Real-time ingest** | Kafka (Redpanda locally) + AWS Glue Streaming ETL (prod) |
| **Validation** | Confluent JSON Schema + Pydantic v2 (fail-fast at the door) |
| **IoT sim** | Continuous device simulator (wearables, BP cuff, glucose, pill bottle) |
| **ML scoring** | LightGBM (readmission_30d) + MLflow registry + FastAPI scorer + real-time Kafka scorer |
| **Governance** | AES-256-GCM column encryption + append-only audit log + OPA-compatible RBAC + Safe Harbor de-identification |
| **Storage** | S3 + Apache Iceberg v3 (prod) / DuckDB (local) |
| **Catalog** | AWS Glue (prod) / Iceberg REST (local) |
| **Transform** | dbt Fusion + Python UDFs (CCS category lookup) |
| **ML** | MLflow + scikit-learn / LightGBM / Prophet + real-time scorer (Module 2) |
| **Orchestration** | Airflow DAG + Prefect flow (batch) + Glue Streaming (real-time) |
| **Source** | Synthea + IoT devices / CMS SynPUF claims / Eligibility files |
| **Quality** | JSON Schema + Great Expectations + OMOP row-level + Iceberg freshness |
| **Governance** | AES-256-GCM encryption, OPA RBAC, append-only audit, de-identification (Module 3) |
| **AI** | MiniMax-M2 (production) / fallback templates (local) |
| **Local dev** | Docker Compose (MinIO + Iceberg REST + Postgres + Redpanda + AI analyst) |

---

## 🚀 Quickstart (5 minutes, zero AWS)

```bash
git clone https://github.com/Jithendranara/real-time-healthcare-pipeline.git
cd real-time-healthcare-pipeline

# 1. Install Python deps
pip install -r ai/analyst/requirements.txt
pip install -r streaming/producers/requirements.txt
pip install dbt-core dbt-duckdb duckdb great-expectations

# 2. Seed 500 synthetic patients → DuckDB
python scripts/seed_omop.py --patients 500

# 3. Build the OMOP CDM
cd dbt_project
mkdir -p ~/.dbt && cp profiles.yml.example ~/.dbt/profiles.yml
DBT_PROFILES_DIR=~/.dbt dbt build --profile vital_pipeline --target local
cd ..

# 4. Run the data quality suite
DUCKDB_PATH=dbt_project/dbt.duckdb python data_quality/run_gx_suite.py

# 5. Ask the AI analyst
uvicorn ai.analyst.app:app --host 0.0.0.0 --port 8000
# → POST http://localhost:8000/ask
#   {"question": "How many patients with type 2 diabetes had an inpatient visit in 2025?"}

# 6. (Optional) Full local stack with MinIO + Iceberg REST
docker compose up -d

# 7. (Optional) Add the streaming layer — Redpanda + streaming producer/consumer
docker compose -f docker-compose.yml -f streaming/docker-compose.streaming.yml up -d redpanda redpanda-console
python streaming/scripts/create_topics.py
# In one terminal: continuous IoT sim
python streaming/seeders/iot_device_simulator.py --patients 50
# In another: Glue ETL (local mode) → DuckDB silver tables
python streaming/consumers/glue_etl_job.py --mode local
# In another: synthetic EHR event producer
python streaming/producers/healthcare_producer.py --rate 20

# 8. (Optional) Add the ML layer — MLflow + readmission scorer
docker compose -f ml/docker-compose.ml.yml up -d
pip install -r ml/requirements.txt
# Train + promote to Production
python ml/scripts/train.py --synthetic 1000 --promote
# Start the FastAPI scorer
MLFLOW_TRACKING_URI=http://localhost:5000 uvicorn ml.api.app:app --host 0.0.0.0 --port 8001
# Start the real-time scorer (consumes admissions → publishes predictions)
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 MLFLOW_TRACKING_URI=http://localhost:5000 \
  OMOP_DUCKDB=dbt_project/dbt.duckdb SILVER_DUCKDB=streaming/warehouse/silver.db \
  python ml/realtime/scorer.py

# 9. (Optional) Add the HIPAA governance layer — encryption + audit + RBAC
pip install -r governance/requirements.txt
# Generate a dev key (32 random bytes, base64) — production uses AWS KMS
export HEALTHCARE_KMS_KEY=$(python -c "import os,base64; print(base64.b64encode(os.urandom(32)).decode())")
# End-to-end governance smoke test (needs Redpanda from step 7)
python governance/scripts/e2e_governance_test.py
```

UI:
- Redpanda Console at <http://localhost:8081> for live topic inspection
- MLflow at <http://localhost:5000> for experiment tracking + model registry
- Scorer API docs at <http://localhost:8001/docs>

---

## 🏥 What's in the Warehouse

| Table | Rows (typical) | Purpose |
|-------|---------------|---------|
| `omcdm_person` | 500+ (synthea) | OMOP-aligned demographics |
| `omcdm_condition_occurrence` | 1,700+ | Patient × diagnosis events, with CCS category |
| `omcdm_visit_occurrence` | 1,300+ | Patient × encounter events, by visit type |
| `omcdm_drug_exposure` | 1,700+ | Prescriptions / administered medications, with drug_code_type |
| `omcdm_measurement` | 5,700+ | Vitals + labs, with measurement_category (vitals_bp / lab_metabolic / etc.) |
| `mart_member_roster` | 500+ | Member-level fact table with DQ flags |
| `int_member_months` | 12,000+ | Member-month grain for PMPM calculations |
| `icd10_to_ccs` | 30 | Python UDF — ICD-10 prefix → CCS category |

All written as **Iceberg v3** tables in prod (partitioned by year), or as DuckDB tables in local dev.

---

## 🤖 The AI Healthcare Analyst

A natural-language interface to your OMOP CDM.

```bash
# Single question
curl -X POST http://localhost:8000/ask \
  -H "Content-Type: application/json" \
  -d '{
    "question": "How many female patients over 60 had a hypertension diagnosis in 2025?"
  }'
# → {
#     "sql": "SELECT COUNT(DISTINCT p.person_id) FROM main.omcdm_person p JOIN ...",
#     "rows": [{"count": 47}],
#     "answer": "47 female patients over 60 had a hypertension diagnosis..."
#   }

# Multi-step plan
curl -X POST http://localhost:8000/plan \
  -H "Content-Type: application/json" \
  -d '{"goal": "Investigate the prevalence of diabetes-related inpatient visits in 2025"}'

# Cohort builder (structured filters, not NL)
curl -X POST http://localhost:8000/cohort \
  -H "Content-Type: application/json" \
  -d '{
    "filters": {
      "min_age": 50,
      "gender_concept_id": 8532,
      "ccs_categories": ["endocrine"],
      "min_visits": 2
    }
  }'
# → { "cohort_size": 38, "sql": "...", "sample": [...] }
```

**Why it matters:** the AI analyst doesn't just generate SQL — it executes it, summarizes the result, and chains multiple Q&A steps into a single diagnostic flow. This is the agentic BI pattern the entire healthcare analytics industry is moving toward in 2026.

---

## 🧬 The Readmission Scorer (Module 2)

Real-time 30-day readmission risk prediction. LightGBM classifier trained on OMOP features, served via FastAPI, scored continuously by a Kafka consumer that publishes to `healthcare.predictions`.

```bash
# Score a single patient
curl -X POST http://localhost:8001/predict \
  -H "Content-Type: application/json" \
  -d '{"patient_id": "12345"}'
# → {
#     "patient_id": "12345",
#     "score": 0.42,
#     "risk_band": "high",
#     "top_feature_contributions": {
#       "feature_chronic_conditions": 0.18,
#       "feature_visits_90d": 0.11,
#       "feature_total_drugs": 0.07,
#       "feature_age": 0.04,
#       "feature_mean_los_days": 0.02
#     },
#     "features_used": ["feature_age", "feature_chronic_conditions", ...],
#     "model_version": "production",
#     "scored_at": "2026-06-09T15:55:00Z"
#   }
```

**Why SHAP:** the model doesn't just hand you a number — it shows you the top 5 features that drove the prediction. A clinician can audit "why is this patient flagged as high risk?" in one glance. This is the explainability bar the entire clinical ML industry is moving toward.

---

## 🔒 The HIPAA Governance Layer (Module 3)

Column-level encryption, append-only audit, role-based access control, and Safe Harbor de-identification wrap the entire data surface.

```python
from governance.encryption.encryptor import PHIEncryptor
from governance.encryption.crypto import CryptoService

# Encrypt PHI on write
encryptor = PHIEncryptor(CryptoService())
encrypted_event = encryptor.encrypt_event({
    "patient_id": "12345",
    "mrn": "M001",
    "heart_rate_bpm": 72,
})
# patient_id and mrn are now AES-256-GCM envelopes; heart_rate_bpm is untouched
```

```python
from governance.masking.deidentify import deidentify_omop_person

# Safe Harbor de-identification for research exports
deidentified = deidentify_omop_person({
    "person_id": 12345, "mrn": "M001", "name": "Jane Doe",
    "birth_datetime": "1945-01-15T00:00:00Z", "year_of_birth": 1945,
    "phone": "555-1234",
})
# → mrn: "DH_f96d..." (hashed), name: "DH_9348..." (hashed),
#    birth_datetime: None, phone: None
```

```python
from governance.rbac.policies import Actor, AccessRequest, PolicyEngine, Resource

engine = PolicyEngine()
# Data scientist trying to read raw MRN — denied
engine.evaluate(AccessRequest(
    actor=Actor(id="alice", role="data_scientist"),
    action="read",
    resource=Resource(type="table", id="omcdm_person", fields=["person.mrn"]),
    purpose="model_training",
))
# → AccessDecision(allow=False, reason="role 'data_scientist' cannot access...")
```

**Why this layer matters:** the entire platform — OMOP warehouse, streaming pipeline, ML scorer — can be HIPAA-compliant without any of them knowing about encryption keys, audit logs, or role policies. They just call into the governance layer. Swap the local key manager for AWS KMS in prod, swap the DuckDB audit backend for Iceberg on S3, and the entire system is production-grade HIPAA.

---

## 📁 Project Structure

```
vital-pipeline/
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   ├── sources/
│   │   └── omop_sources.yml       # Synthea → Iceberg source mapping
│   ├── models/
│   │   ├── staging/               # Eligibility cleaning
│   │   ├── intermediate/          # Member-months, age, etc.
│   │   ├── marts/                 # Marts
│   │   ├── omop/                  # OMOP CDM v5.4 (Person, Condition, Visit)
│   │   └── udfs/                  # Python UDFs (ICD-10 → CCS)
│   ├── seeds/                     # ICD-10, CPT reference
│   ├── packages.yml
│   └── macros/
├── data_quality/
│   └── run_gx_suite.py            # GX + OMOP + freshness checks
├── ai/
│   ├── analyst/                   # AI healthcare analyst (FastAPI + MiniMax)
│   ├── anomaly_detection/         # Claims ML anomaly detection (sklearn)
│   └── qa_assistant/              # LLM eligibility QA chatbot
├── pipelines/eligibility-etl/     # Airflow DAG
├── prefect_flows/                 # Prefect 3.x orchestration
├── infrastructure/                # Terraform IaC (AWS)
├── data_contracts/                # Open Data Contract Standard YAML
├── docs/                          # Mermaid architecture diagrams
├── scripts/
│   ├── seed_omop.py               # Synthetic Synthea-like data for local
│   └── ...
├── docker-compose.yml             # MinIO + Iceberg REST + Postgres
└── .github/workflows/ci.yaml      # CI: dbt build, DQ, AI boot
```

---

## 🔬 OMOP CDM Coverage

| OMOP Table | Model | Notes |
|-----------|-------|-------|
| `person` | `omcdm_person` | Hash-based person_id for portability |
| `condition_occurrence` | `omcdm_condition_occurrence` | + CCS category (Python UDF) |
| `visit_occurrence` | `omcdm_visit_occurrence` | Visit concept by encounter class |

**Not yet covered** (planned): observation, death, payer_plan_period. Add them in a follow-up PR by following the same pattern in `dbt_project/models/omop/`. The `drug_exposure` and `measurement` tables were added in the v2 deepening — see commit history.

---

## 🧪 Test Coverage

| Test type | Count | Where |
|-----------|-------|-------|
| dbt not_null | 16 | `dbt_project/models/omop/_omop__models.yml` |
| dbt unique | 5 | Same |
| dbt accepted_values | 3 | Same |
| GX column-level | 8 | `data_quality/run_gx_suite.py` |
| OMOP row-level | 4 | Same |
| Freshness | 1 | Same |

Total: **37 data quality checks** running on every PR.

---

## 🏭 Production Deploy (AWS)

```bash
# 1. Provision infrastructure
cd infrastructure
terraform init && terraform plan && terraform apply

# 2. Generate Synthea (or copy your data) → S3 raw bucket
# (in prod, Synthea is replaced by your real data sources)

# 3. Build OMOP via dbt Fusion (Iceberg)
cd ../dbt_project
DBT_PROFILES_DIR=~/.dbt dbt build --profile vital_pipeline --target prod

# 4. Deploy the AI analyst
docker build -t ghcr.io/your-org/vital-pipeline/ai-analyst ai/analyst/
docker push ghcr.io/your-org/vital-pipeline/ai-analyst
aws lambda create-function \
  --function-name vital-pipeline-ai-analyst \
  --package-type Image \
  --code ImageUri=ghcr.io/your-org/vital-pipeline/ai-analyst:latest \
  --role arn:aws:iam::ACCOUNT:role/vital-pipeline-ai-analyst

# 5. Wire into your Airflow / Prefect deployment
# (the dags/ and prefect_flows/ are already in the repo)
```

---

## 📚 Learn More

- `docs/architecture_diagram.md` — full Mermaid architecture
- `docs/data-dictionary.md` — column-level documentation
- `data_contracts/eligibility_data_contract.yml` — Open Data Contract Standard
- `infrastructure/main.tf` — AWS Terraform reference

---

## 📝 License

MIT — fork it, ship it, build on it.
