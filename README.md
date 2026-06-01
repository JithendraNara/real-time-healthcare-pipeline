# vital-pipeline

> **Healthcare data operations — modernized for 2026.**
> **Synthea → OMOP CDM v5.4 → DuckDB/Iceberg → AI analyst.**
> **Zero-AWS local dev. Production-ready for AWS.**

[![CI](https://github.com/JithendraNara/vital-pipeline/actions/workflows/ci.yaml/badge.svg)](https://github.com/JithendraNara/vital-pipeline/actions)
[![OMOP CDM](https://img.shields.io/badge/OMOP-v5.4-blue)](https://ohdsi.github.io/CommonDataModel/)
[![Iceberg v3](https://img.shields.io/badge/Apache_Iceberg-v3-lightgrey)](https://iceberg.apache.org/)
[![dbt Fusion](https://img.shields.io/badge/dbt-Fusion-orange)](https://www.getdbt.com/)
[![AI Analyst](https://img.shields.io/badge/AI-MiniMax--M2-green)](https://api.minimax.chat/)

A production-quality healthcare data platform: eligibility QA, claims analytics, anomaly detection, and a natural-language AI analyst — all built on a modern data stack that runs on your laptop.

---

## 🏗️ Architecture

```
[Synthea CSVs]   ─┐
                  │
[Eligibility Files]┤
                  ├──▶ [dbt Fusion] ──▶ [OMOP CDM v5.4 (Iceberg v3 / DuckDB)]
[Claims Data]    ─┘                          │
                                              ├── person
                                              ├── condition_occurrence   ← + CCS grouping (Python UDF)
                                              ├── visit_occurrence
                                              └── mart_member_roster
                                                       │
                                                       ▼
                                            [AI Healthcare Analyst]
                                            FastAPI + MiniMax-M2
                                                       │
                                                       ├── /ask       NL → SQL → answer
                                                       ├── /plan      multi-step investigation
                                                       ├── /cohort    SQL-defined patient filters
                                                       └── /schema
```

**Three layers of data quality:**

1. **Great Expectations** — column-level rules (uniqueness, regex, ranges)
2. **OMOP row-level** — referential integrity, valid concept_ids, CCS coverage
3. **Iceberg freshness** — snapshot age checks (replaces the old "is the pipeline running" alerts)

---

## 🧱 The Stack (2026)

| Layer | Technology |
|-------|-----------|
| **Storage** | S3 + Apache Iceberg v3 (prod) / DuckDB (local) |
| **Catalog** | AWS Glue (prod) / Iceberg REST (local) |
| **Transform** | dbt Fusion + Python UDFs (CCS category lookup) |
| **Orchestration** | Airflow DAG **+** Prefect flow (already in repo) |
| **Source** | Synthea synthetic patients / CMS SynPUF claims / Eligibility files |
| **Quality** | Great Expectations + OMOP row-level + Iceberg freshness |
| **AI** | MiniMax-M2 (production) / fallback templates (local) |
| **Local dev** | Docker Compose (MinIO + Iceberg REST + Postgres + AI analyst) |

---

## 🚀 Quickstart (5 minutes, zero AWS)

```bash
git clone https://github.com/JithendraNara/vital-pipeline.git
cd vital-pipeline

# 1. Install Python deps
pip install -r ai/analyst/requirements.txt
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
```

---

## 🏥 What's in the Warehouse

| Table | Rows (typical) | Purpose |
|-------|---------------|---------|
| `omcdm_person` | 500+ (synthea) | OMOP-aligned demographics |
| `omcdm_condition_occurrence` | 2,000+ | Patient × diagnosis events, with CCS category |
| `omcdm_visit_occurrence` | 1,500+ | Patient × encounter events, by visit type |
| `mart_member_roster` | 500+ | Member-level fact table with DQ flags |
| `int_member_months` | 5,000+ | Member-month grain for PMPM calculations |
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

**Not yet covered** (planned): drug_exposure, measurement, observation, death, payer_plan_period. Add them in a follow-up PR by following the same pattern in `dbt_project/models/omop/`.

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
