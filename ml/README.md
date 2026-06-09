# ML Module — Patient Outcome Models

> **Module 2 of 4** — Real-time ML scoring for patient outcomes, resource allocation, and care recommendations.
> Consumes the streaming silver tables from Module 1 + the OMOP warehouse, scores patients, publishes predictions to Kafka.

## What ships in this module

| Component | Path | Purpose |
|---|---|---|
| **Feature engineering** | `ml/outcomes/feature_engineering.py` | OMOP DuckDB → 18-feature vector per (patient, discharge) for readmission prediction. Same logic online + offline. |
| **Readmission predictor** | `ml/outcomes/readmission_predictor.py` | LightGBM classifier → 30-day readmission probability. Time-aware train/test split, SHAP-based local explanations. |
| **Model registry wrapper** | `ml/outcomes/model_registry.py` | MLflow logging + version promotion. One place to swap tracking URIs. |
| **Training CLI** | `ml/scripts/train.py` | Train + log to MLflow + (optionally) promote to Production. Works on synthetic data (CI) or real OMOP. |
| **FastAPI scorer** | `ml/api/app.py` | `POST /predict` — real-time scoring by patient_id, returns score + risk band + SHAP top features. |
| **Real-time scorer** | `ml/realtime/scorer.py` | Kafka consumer that scores on every discharge event and publishes to `healthcare.predictions`. |
| **MLflow + MinIO** | `ml/docker-compose.ml.yml` | Local stack for the MLflow tracking server + S3-compatible artifact store. |
| **Tests** | `ml/tests/test_features.py` | 18 unit tests — feature engineering, training, scoring, MLflow round-trip. No broker/OMOP required. |

## Models (planned)

This module ships the first model (`readmission_30d`) and the plumbing for three more:

| Model | Status | Use case | Algorithm |
|---|---|---|---|
| `readmission_30d` | ✅ **shipped** | Identify high-risk discharges for care transition programs | LightGBM |
| `icu_deterioration` | 🔜 | Early warning from streaming vitals (NEWS2-inspired) | LightGBM / RNN |
| `bed_demand_1h` | 🔜 | 1-hour bed demand forecast for staffing | Prophet / LSTM |
| `care_recommendation` | 🔜 | Personalized next-best-action for high-risk patients | Collaborative filter |

The schema, MLflow wrapper, FastAPI surface, and Kafka scorer are all generic — adding
a new model is one new file in `ml/outcomes/`, one new entry in `ml/realtime/scorer.py`'s
model router, and a new registered model in MLflow.

## Quickstart

```bash
# 1. Boot MLflow + MinIO
docker compose -f ml/docker-compose.ml.yml up -d

# 2. Install ML deps
pip install -r ml/requirements.txt
pip install -r streaming/producers/requirements.txt

# 3. Train on synthetic data (CI/demo, no OMOP needed)
python ml/scripts/train.py --synthetic 1000 --promote

# 4. Or train on the real OMOP warehouse
python ml/scripts/train.py --omop-duckdb dbt_project/dbt.duckdb --promote

# 5. Start the FastAPI scorer
MLFLOW_TRACKING_URI=http://localhost:5000 \
MLFLOW_REGISTRY_NAME=readmission_30d \
OMOP_DUCKDB=dbt_project/dbt.duckdb \
SILVER_DUCKDB=streaming/warehouse/silver.db \
uvicorn ml.api.app:app --host 0.0.0.0 --port 8001

# 6. (Optional) Start the real-time scorer — reads admissions, publishes predictions
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
MLFLOW_TRACKING_URI=http://localhost:5000 \
OMOP_DUCKDB=dbt_project/dbt.duckdb \
SILVER_DUCKDB=streaming/warehouse/silver.db \
python ml/realtime/scorer.py

# 7. Score a single patient (smoke test)
python ml/realtime/scorer.py --once <patient_id>
```

## API

`POST /predict`

```json
{
  "patient_id": "12345"
}
```

```json
{
  "patient_id": "12345",
  "score": 0.42,
  "risk_band": "high",
  "top_feature_contributions": {
    "feature_chronic_conditions": 0.18,
    "feature_visits_90d": 0.11,
    "feature_total_drugs": 0.07,
    "feature_age": 0.04,
    "feature_mean_los_days": 0.02
  },
  "features_used": ["feature_age", "feature_chronic_conditions", "feature_total_drugs", ...],
  "model_version": "production",
  "scored_at": "2026-06-09T15:55:00Z"
}
```

## HIPAA + governance hook points

This module is the **read** side of the data — it touches PHI to score patients. Governance is wired in via:

- `MLFLOW_TRACKING_URI` and `OMOP_DUCKDB`/`SILVER_DUCKDB` are env-driven → can be replaced
  with HIPAA-compliant equivalents in prod (e.g., MLflow on a private VPC, S3 with KMS,
  S3 access via IAM roles instead of access keys)
- Model versioning in MLflow = full audit trail of which model version scored which patient
- `PredictionEvent.top_feature_contributions` = SHAP explanation that gets logged to the
  audit table (Module 3) — every score has a "why"
- Real-time scorer publishes to `healthcare.predictions` (encrypted topics, governed topic ACLs
  land in Module 3)

## Next modules

- **Module 3** (`governance/`) — column-level encryption for PHI, OPA RBAC for topic
  access, append-only audit table for every read/write, de-identification helpers
- **Module 4** (`app/`, `prefect_flows/`) — clinical Streamlit dashboard that pulls
  from `healthcare.predictions` + OMOP mart, plus a Prefect end-to-end flow
