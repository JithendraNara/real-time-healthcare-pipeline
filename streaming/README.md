# Streaming Module — Real-Time Healthcare & IoT Pipeline

> **Module 1 of 4** — Real-time data ingestion with Kafka (Redpanda locally) and AWS Glue.
> Feeds OMOP CDM v5.4, ML outcome models, and clinical dashboards with sub-second latency.

This module adds a real-time event layer on top of the batch OMOP warehouse. Synthea
patients, EHR events, and IoT device telemetry flow through Kafka topics, get validated
and enriched by a Glue streaming ETL, and land in Iceberg v3 tables ready for ML scoring.

```
[Synthea patients]  ─┐
[EHR mock events]   ─┤
[IoT wearables]     ─┤──▶ [Kafka / Redpanda topics] ──▶ [Glue Streaming ETL]
                     │                                       │
                     │                                       ├── validate (JSON Schema)
                     │                                       ├── enrich (join OMOP person)
                     │                                       ├── transform → Iceberg v3
                     │                                       └── dead-letter (malformed)
                     │                                              │
                     │                                              ▼
                     │                                       [Iceberg v3 silver tables]
                     │                                              │
                     │                                              ▼
                     │                                       [ML scorer → predictions topic]
                     │                                              │
                     │                                              ▼
                     │                                       [Clinical dashboard]
```

## Topics

| Topic | Source | Format | Cardinality |
|---|---|---|---|
| `healthcare.vitals` | EHR + IoT | JSON (Pydantic-validated) | high (~10 events/patient/min) |
| `healthcare.admissions` | EHR | JSON | low (~1/patient/visit) |
| `healthcare.lab_results` | EHR + lab devices | JSON | medium |
| `iot.telemetry` | Wearables, monitors | JSON | very high (~1Hz/patient) |
| `healthcare.predictions` | ML scorer (Module 2) | JSON | medium |
| `healthcare.dlq` | Dead-letter for any malformed/invalid event | JSON + original | low |

All topics use Schema Registry for forward/backward compatibility.

## Quickstart (5 minutes)

```bash
# 1. Start Redpanda (Kafka-compatible, no Zookeeper)
docker compose -f docker-compose.yml -f streaming/docker-compose.streaming.yml up -d redpanda redpanda-console

# 2. Create topics
python streaming/scripts/create_topics.py

# 3. Start the IoT simulator (continuous vitals stream for 50 patients)
python streaming/seeders/iot_device_simulator.py --patients 50 --rate 1.0

# 4. Start the consumer (writes to local Iceberg under streaming/warehouse/)
python streaming/consumers/glue_etl_job.py --local

# 5. Query what landed
duckdb streaming/warehouse/silver.db -c "SELECT * FROM healthcare.vitals_silver LIMIT 10"
```

## Production deployment

In production, the same code path runs in AWS Glue Streaming:

```bash
# Upload job + dependencies
aws s3 cp streaming/consumers/glue_etl_job.py s3://your-bucket/glue/jobs/
aws s3 cp streaming/consumers/glue_dependencies.zip s3://your-bucket/glue/jobs/

# Trigger the job
aws glue start-job-run --job-name healthcare-vitals-streaming
```

The job code is identical — only the runtime context changes. The `--local` flag in
local dev uses DuckDB; in Glue it uses Spark + Iceberg v3 on S3.

## Architecture choices

- **Redpanda locally** — Kafka API-compatible, no Zookeeper, single container, fast
- **Confluent Kafka client** — `confluent-kafka-python` (librdkafka under the hood), the
  production standard for high-throughput Python producers/consumers
- **Pydantic v2** for event models — same library the AI analyst uses, type-safe events
- **JSON Schema** in the Schema Registry — language-agnostic, decouples producer/consumer
- **PyIceberg** writes to local DuckDB (dev) or S3+Iceberg v3 (prod) — same code path
- **Dead-letter topic** (`healthcare.dlq`) — every malformed event is preserved with
  the original payload + validation error for replay/analysis

## HIPAA posture

Even at the streaming layer, PHI is treated carefully:

- No raw PHI in topic names or headers
- Consumer enforces field-level encryption for `mrn`, `birth_datetime`, `name_*` columns
  (keys from `governance/` module — Module 3)
- All consumer reads/writes are wrapped in an audit-log call (`governance/audit/`)
- Topic-level retention is bounded (7 days for vitals, 30 days for admissions)
- See `governance/` module for encryption, RBAC, and full audit trail

## Next modules

- **Module 2** (`ml/`) — Patient outcome, ICU deterioration, and care recommendation models
  that consume the silver tables and write to `healthcare.predictions`
- **Module 3** (`governance/`) — HIPAA layer (encryption, RBAC, audit, masking)
- **Module 4** (`app/`, `prefect_flows/`) — Clinical dashboard + end-to-end orchestration
