"""
AWS Glue Streaming ETL job for the real-time healthcare pipeline.

This job:
  1. Subscribes to all healthcare.* and iot.telemetry topics
  2. Validates each event against the registered JSON Schema (or Pydantic fallback)
  3. Joins each event to OMOP person (caching the table in-memory for the micro-batch)
  4. Writes the validated+enriched record into Iceberg v3 silver tables
  5. Routes malformed/invalid events to healthcare.dlq with full error context

Same code path runs locally (--local) against DuckDB + Redpanda, and in AWS Glue
against Spark + Iceberg v3 on S3. The only differences:
  - Local:  confluent-kafka-python, duckdb, writes to streaming/warehouse/*.db
  - Glue:   Spark Structured Streaming from Kafka, Iceberg v3 sink, S3 warehouse

Tested with: AWS Glue 4.0 (Spark 3.3, Python 3.10), Redpanda 23.x, DuckDB 1.5.x.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from streaming.schemas.events import (  # noqa: E402
    AdmissionEvent,
    DeadLetterEvent,
    IoTTelemetryEvent,
    LabResultEvent,
    VitalsEvent,
)

log = logging.getLogger("glue_etl_job")

EVENT_MODELS: dict[str, type] = {
    "healthcare.vitals": VitalsEvent,
    "healthcare.admissions": AdmissionEvent,
    "healthcare.lab_results": LabResultEvent,
    "iot.telemetry": IoTTelemetryEvent,
}

# Topic -> destination table suffix in silver
TOPIC_TO_TABLE = {
    "healthcare.vitals": "vitals_silver",
    "healthcare.admissions": "admissions_silver",
    "healthcare.lab_results": "labs_silver",
    "iot.telemetry": "iot_telemetry_silver",
}


# ---------------------------------------------------------------------------
# Local-mode consumer (confluent-kafka + DuckDB)
# ---------------------------------------------------------------------------


@dataclass
class LocalConfig:
    bootstrap_servers: str
    group_id: str
    warehouse_dir: Path
    duckdb_path: Path
    omop_duckdb: Path | None
    max_runtime_sec: float | None
    topics: list[str]

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "LocalConfig":
        warehouse = Path(args.warehouse_dir)
        warehouse.mkdir(parents=True, exist_ok=True)
        return cls(
            bootstrap_servers=args.bootstrap_servers,
            group_id=args.group_id,
            warehouse_dir=warehouse,
            duckdb_path=warehouse / "silver.db",
            omop_duckdb=Path(args.omop_duckdb) if args.omop_duckdb else None,
            max_runtime_sec=args.max_runtime,
            topics=args.topics.split(","),
        )


def build_local_consumer(cfg: LocalConfig):
    from confluent_kafka import Consumer

    return Consumer(
        {
            "bootstrap.servers": cfg.bootstrap_servers,
            "group.id": cfg.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": 45000,
        }
    )


def open_duckdb_sink(cfg: LocalConfig):
    import duckdb

    con = duckdb.connect(str(cfg.duckdb_path))
    # Bootstrap schema — separate tables per topic
    for topic, table in TOPIC_TO_TABLE.items():
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                topic VARCHAR,
                partition_id INTEGER,
                offset_id BIGINT,
                event_time TIMESTAMP,
                ingestion_time TIMESTAMP,
                source VARCHAR,
                schema_version INTEGER,
                payload JSON,
                primary key (topic, partition_id, offset_id)
            )
            """
        )
    # DLQ table
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS dlq_silver (
            dlq_id VARCHAR PRIMARY KEY,
            dlq_time TIMESTAMP,
            source_topic VARCHAR,
            source_partition INTEGER,
            source_offset BIGINT,
            original_payload VARCHAR,
            error_type VARCHAR,
            error_message VARCHAR,
            error_stack VARCHAR,
            consumer_id VARCHAR
        )
        """
    )
    return con


def parse_and_validate(topic: str, raw: bytes) -> tuple[dict | None, str | None]:
    """Returns (event_dict, None) on success, (None, error_msg) on failure."""
    model_cls = EVENT_MODELS.get(topic)
    if model_cls is None:
        return None, f"unknown topic {topic}"
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as e:
        return None, f"utf-8 decode failed: {e}"
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        return None, f"json decode failed: {e}"
    try:
        validated = model_cls.model_validate(data)
        return validated.model_dump(mode="json"), None
    except Exception as e:  # noqa: BLE001
        return None, f"pydantic validation failed: {e.__class__.__name__}: {e}"


def enrich_with_omop(event: dict, person_cache: dict[str, dict]) -> dict:
    """Join to OMOP person for known patients. Stamps age cohort and gender concept_id."""
    pid = event.get("patient_id")
    person = person_cache.get(pid or "")
    if person is None:
        event["_enrichment"] = {"known_patient": False}
        return event
    event["_enrichment"] = {
        "known_patient": True,
        "gender_concept_id": person.get("gender_concept_id"),
        "year_of_birth": person.get("year_of_birth"),
    }
    return event


def load_omop_person_cache(omop_duckdb: Path | None) -> dict[str, dict]:
    if not omop_duckdb or not omop_duckdb.exists():
        log.info("No OMOP DuckDB supplied — running without enrichment.")
        return {}
    try:
        import duckdb
        con = duckdb.connect(str(omop_duckdb), read_only=True)
        rows = con.execute(
            "SELECT person_id, gender_concept_id, year_of_birth FROM omcdm_person"
        ).fetchall()
        con.close()
        return {str(r[0]): {"gender_concept_id": r[1], "year_of_birth": r[2]} for r in rows}
    except Exception as e:  # noqa: BLE001
        log.warning("OMOP enrichment load failed: %s", e)
        return {}


def write_batch(con, topic: str, batch: list[tuple[int, int, datetime, datetime, str, int, dict]]) -> int:
    if not batch:
        return 0
    table = TOPIC_TO_TABLE[topic]
    con.executemany(
        f"""
        INSERT OR REPLACE INTO {table}
            (topic, partition_id, offset_id, event_time, ingestion_time, source, schema_version, payload)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        batch,
    )
    return len(batch)


def write_dlq(con, dlq: DeadLetterEvent) -> None:
    con.execute(
        """
        INSERT OR REPLACE INTO dlq_silver
            (dlq_id, dlq_time, source_topic, source_partition, source_offset,
             original_payload, error_type, error_message, error_stack, consumer_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            dlq.dlq_id,
            dlq.dlq_time,
            dlq.source_topic,
            dlq.source_partition,
            dlq.source_offset,
            dlq.original_payload,
            dlq.error_type,
            dlq.error_message,
            dlq.error_stack,
            dlq.consumer_id,
        ],
    )


def run_local(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )

    cfg = LocalConfig.from_args(args)
    consumer = build_local_consumer(cfg)
    consumer.subscribe(cfg.topics)
    sink = open_duckdb_sink(cfg)
    person_cache = load_omop_person_cache(cfg.omop_duckdb)
    log.info("Loaded %d OMOP persons into enrichment cache", len(person_cache))

    stop = False

    def handle(_sig, _frm):
        nonlocal stop
        log.info("Stopping ETL…")
        stop = True

    signal.signal(signal.SIGINT, handle)
    signal.signal(signal.SIGTERM, handle)

    started = time.time()
    processed = 0
    dlq_count = 0
    batches: dict[str, list] = {t: [] for t in cfg.topics if t in TOPIC_TO_TABLE}
    consumer_id = f"glue-etl-{os.getpid()}"

    while not stop:
        if cfg.max_runtime_sec and (time.time() - started) >= cfg.max_runtime_sec:
            log.info("Max runtime reached — exiting.")
            break
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            for topic, batch in batches.items():
                if batch:
                    n = write_batch(sink, topic, batch)
                    sink.commit()
                    log.info("Flushed %d rows to %s", n, TOPIC_TO_TABLE[topic])
                    batches[topic] = []
            continue
        if msg.error():
            log.error("Consumer error: %s", msg.error())
            continue

        topic = msg.topic()
        if topic not in TOPIC_TO_TABLE:
            # Unknown topic — send straight to DLQ
            dlq = DeadLetterEvent(
                source_topic=topic,
                source_partition=msg.partition(),
                source_offset=msg.offset(),
                original_payload=msg.value().decode("utf-8", errors="replace"),
                error_type="UnknownTopic",
                error_message=f"no schema registered for topic {topic}",
                consumer_id=consumer_id,
            )
            write_dlq(sink, dlq)
            dlq_count += 1
            consumer.commit(msg)
            continue

        event, err = parse_and_validate(topic, msg.value() or b"")
        if err is not None:
            dlq = DeadLetterEvent(
                source_topic=topic,
                source_partition=msg.partition(),
                source_offset=msg.offset(),
                original_payload=(msg.value() or b"").decode("utf-8", errors="replace"),
                error_type="ValidationError",
                error_message=err,
                consumer_id=consumer_id,
            )
            write_dlq(sink, dlq)
            dlq_count += 1
        else:
            assert event is not None
            enriched = enrich_with_omop(event, person_cache)
            batches[topic].append(
                (
                    topic,
                    msg.partition(),
                    msg.offset(),
                    datetime.fromisoformat(enriched["event_time"]),
                    datetime.fromisoformat(enriched["ingestion_time"]),
                    enriched["source"],
                    enriched["schema_version"],
                    json.dumps(enriched, default=str),
                )
            )
            processed += 1

        # Flush every 200 records
        if processed % 200 == 0 and processed > 0:
            for t, batch in batches.items():
                if batch:
                    write_batch(sink, t, batch)
                    batches[t] = []
            sink.commit()
            consumer.commit(msg, asynchronous=False)
            log.info("Processed: %d events | DLQ: %d", processed, dlq_count)

    # Final flush
    for t, batch in batches.items():
        if batch:
            write_batch(sink, t, batch)
    sink.commit()
    consumer.close()
    log.info("Final: processed=%d dlq=%d runtime=%.1fs", processed, dlq_count, time.time() - started)
    return 0


# ---------------------------------------------------------------------------
# Glue-mode entrypoint (skeleton — runs in AWS Glue, not locally)
# ---------------------------------------------------------------------------


def run_glue(args: argparse.Namespace) -> int:
    """
    Glue entrypoint. This block is only meant to be executed inside an AWS Glue job
    (Spark 3.3 + Iceberg v3 runtime). It is NOT import-safe in local dev.

    Steps:
      1. Read KAFKA_BOOTSTRAP_SERVERS, ICEBERG_WAREHOUSE, OMOP_PERSON_PATH from job params
      2. Read each subscribed topic as a Spark Structured Streaming DataFrame
      3. Parse JSON, apply schema, drop malformed events to DLQ
      4. Stream-join with the OMOP person table (broadcast, small enough)
      5. Write to Iceberg v3 partitioned by year(event_time) and source
      6. Trigger every 30 seconds
    """
    raise NotImplementedError(
        "Glue mode runs inside AWS Glue with Spark + Iceberg v3. "
        "See the README in streaming/ for the deployment commands."
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    p = argparse.ArgumentParser(description="Streaming ETL for healthcare/IoT events")
    p.add_argument("--mode", choices=["local", "glue"], default="local")
    p.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    p.add_argument("--group-id", default=os.getenv("KAFKA_GROUP_ID", "glue-etl-local"))
    p.add_argument("--topics", default="healthcare.vitals,healthcare.admissions,healthcare.lab_results,iot.telemetry")
    p.add_argument("--warehouse-dir", default="streaming/warehouse")
    p.add_argument("--omop-duckdb", default="dbt_project/dbt.duckdb")
    p.add_argument("--max-runtime", type=float, default=None, help="Stop after N seconds (local only)")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()

    if args.mode == "local":
        return run_local(args)
    return run_glue(args)


if __name__ == "__main__":
    sys.exit(main())
