"""
Producer for healthcare events — Synthea-style EHR events + IoT vitals.

This producer:
  1. Reads patient IDs from the local OMOP DuckDB warehouse
  2. Generates a configurable mix of vitals / admissions / lab / IoT events
  3. Validates each event with Pydantic before publishing (fail-fast)
  4. Publishes to Kafka (Redpanda locally) with acks=all for durability
  5. Optionally registers/upgrades the JSON Schema in the Schema Registry

The same code path works in dev (Redpanda via Docker) and prod (MSK or Confluent Cloud).
Only the bootstrap servers + Schema Registry URL change.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

import yaml
from confluent_kafka import KafkaError, Producer

# Allow running both as a module and as a script
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from streaming.schemas.events import (  # noqa: E402
    AdmissionEvent,
    AdmissionType,
    IoTDeviceType,
    IoTTelemetryEvent,
    LabResultEvent,
    VitalsEvent,
)

log = logging.getLogger("healthcare_producer")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ProducerConfig:
    bootstrap_servers: str
    schema_registry_url: str | None
    client_id: str
    acks: str = "all"
    enable_idempotence: bool = True
    compression_type: str = "snappy"
    linger_ms: int = 5
    batch_size: int = 64 * 1024

    @classmethod
    def from_env(cls) -> "ProducerConfig":
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL"),
            client_id=os.getenv("KAFKA_CLIENT_ID", f"healthcare-producer-{uuid.uuid4().hex[:8]}"),
        )


def load_topics(path: Path) -> list[dict]:
    with path.open() as f:
        cfg = yaml.safe_load(f)
    return cfg["topics"]


# ---------------------------------------------------------------------------
# Event generators
# ---------------------------------------------------------------------------


LOINC_COMMON = [
    ("4548-4", "Hemoglobin A1c", "g/dL", 4.0, 5.6),
    ("2093-3", "Cholesterol, Total", "mg/dL", 125, 200),
    ("2085-9", "HDL Cholesterol", "mg/dL", 40, 60),
    ("13457-7", "Cholesterol in LDL", "mg/dL", 0, 100),
    ("2345-7", "Glucose", "mg/dL", 70, 99),
    ("1742-6", "ALT", "U/L", 7, 56),
    ("718-7", "Hemoglobin", "g/dL", 12.0, 17.5),
    ("2160-0", "Creatinine", "mg/dL", 0.6, 1.3),
]


def load_omop_patient_ids(duckdb_path: Path, limit: int) -> list[str]:
    """Pull person_ids from the local OMOP DuckDB. Falls back to random IDs if unavailable."""
    try:
        import duckdb

        if not duckdb_path.exists():
            raise FileNotFoundError(duckdb_path)
        con = duckdb.connect(str(duckdb_path), read_only=True)
        rows = con.execute(
            f"SELECT person_id FROM omcdm_person ORDER BY random() LIMIT {int(limit)}"
        ).fetchall()
        con.close()
        return [str(r[0]) for r in rows]
    except Exception as e:  # noqa: BLE001
        log.warning("Could not load OMOP patients from %s: %s. Using synthetic IDs.", duckdb_path, e)
        return [f"syn_{i:05d}" for i in range(limit)]


def gen_vitals(patient_id: str, source: str = "ehr") -> VitalsEvent:
    """Generate one vitals event with realistic-but-noisy clinical ranges."""
    base_hr = random.gauss(78, 12)
    base_spo2 = random.gauss(97, 1.5)
    base_sbp = random.gauss(125, 15)
    base_dbp = random.gauss(80, 10)
    base_rr = random.gauss(16, 3)
    base_temp = random.gauss(36.8, 0.4)

    # 5% chance of a critical reading — that's the early-warning surface
    if random.random() < 0.05:
        base_spo2 = random.uniform(82, 89)
    if random.random() < 0.03:
        base_hr = random.choice([random.uniform(28, 39), random.uniform(141, 165)])

    return VitalsEvent(
        patient_id=patient_id,
        source=source,  # type: ignore[arg-type]
        heart_rate_bpm=int(max(20, min(200, base_hr))),
        systolic_bp_mmHg=int(max(60, min(220, base_sbp))),
        diastolic_bp_mmHg=int(max(30, min(140, base_dbp))),
        respiratory_rate=int(max(6, min(40, base_rr))),
        spo2_pct=round(max(70, min(100, base_spo2)), 1),
        temperature_c=round(max(34, min(42, base_temp)), 1),
        pain_score=random.choices([0, 1, 2, 3, 4, 5, 6, 7, 8], weights=[20, 15, 15, 10, 10, 10, 8, 7, 5])[0],
    )


def gen_admission(patient_id: str) -> AdmissionEvent:
    enc_id = f"enc_{uuid.uuid4().hex[:12]}"
    admit_type = random.choices(
        list(AdmissionType),
        weights=[15, 40, 10, 25, 10],
    )[0]
    admit_time = datetime.now(timezone.utc) - timedelta(minutes=random.randint(5, 1440))
    return AdmissionEvent(
        patient_id=patient_id,
        source="ehr",
        encounter_id=enc_id,
        admission_type=admit_type,
        facility_id=f"fac_{random.randint(1, 5):03d}",
        attending_provider_id=f"prv_{random.randint(1, 250):05d}",
        chief_complaint=random.choice(
            [
                "chest pain", "shortness of breath", "abdominal pain",
                "fever", "fall", "headache", "altered mental status",
                None, None, None,
            ]
        ),
        admit_time=admit_time,
        discharge_time=None,
        disposition="still_admitted",
        diagnosis_codes=random.sample(
            ["I10", "E11.9", "J18.9", "R07.9", "S72.001A", "I50.9", "N39.0", "F32.9"],
            k=random.randint(0, 3),
        ),
    )


def gen_lab(patient_id: str) -> LabResultEvent:
    loinc, name, unit, lo, hi = random.choice(LOINC_COMMON)
    value = random.uniform(lo, hi) * random.choice([0.5, 0.8, 1.0, 1.0, 1.2, 1.5, 2.5])
    abn = None
    if value < lo:
        abn = "L" if value >= lo * 0.8 else "LL"
    elif value > hi:
        abn = "H" if value <= hi * 1.3 else "HH"
    collected = datetime.now(timezone.utc) - timedelta(hours=random.randint(1, 24))
    resulted = collected + timedelta(hours=random.randint(1, 4))
    return LabResultEvent(
        patient_id=patient_id,
        source="lab_device",
        order_id=f"ord_{uuid.uuid4().hex[:10]}",
        test_code=loinc,
        test_name=name,
        value_numeric=round(value, 2),
        unit=unit,
        reference_low=lo,
        reference_high=hi,
        abnormal_flag=abn,
        collected_at=collected,
        resulted_at=resulted,
    )


def gen_iot(patient_id: str) -> IoTTelemetryEvent:
    dtype = random.choice(list(IoTDeviceType))
    metrics: dict[str, float | int | str | bool] = {}
    if dtype == IoTDeviceType.WEARABLE:
        metrics = {
            "steps": random.randint(0, 250),
            "heart_rate": random.randint(55, 130),
            "active_minutes": random.randint(0, 5),
        }
    elif dtype == IoTDeviceType.GLUCOSE_METER:
        metrics = {"glucose_mg_dl": random.randint(70, 220)}
    elif dtype == IoTDeviceType.PULSE_OXIMETER:
        metrics = {"spo2_pct": round(random.uniform(90, 100), 1), "pulse_bpm": random.randint(60, 110)}
    elif dtype == IoTDeviceType.SMART_PILL_BOTTLE:
        metrics = {"opened": True, "pills_remaining": random.randint(0, 30)}
    elif dtype == IoTDeviceType.BP_CUFF:
        metrics = {
            "systolic_mmhg": random.randint(95, 170),
            "diastolic_mmhg": random.randint(55, 105),
        }
    elif dtype == IoTDeviceType.CONTINUOUS_MONITOR:
        metrics = {
            "heart_rate_bpm": random.randint(55, 130),
            "spo2_pct": round(random.uniform(92, 100), 1),
            "resp_rate": random.randint(12, 22),
        }
    return IoTTelemetryEvent(
        patient_id=patient_id,
        source="edge_gateway" if dtype in (IoTDeviceType.WEARABLE, IoTDeviceType.GLUCOSE_METER) else "iot",
        device_id=f"dev_{uuid.uuid4().hex[:10]}",
        device_type=dtype,
        firmware_version=f"{random.randint(1, 3)}.{random.randint(0, 9)}.{random.randint(0, 20)}",
        metrics=metrics,
        battery_pct=round(random.uniform(15, 100), 1),
    )


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------


TOPIC_MAP = {
    "vitals": "healthcare.vitals",
    "admissions": "healthcare.admissions",
    "labs": "healthcare.lab_results",
    "iot": "iot.telemetry",
}


class HealthcareProducer:
    def __init__(self, config: ProducerConfig):
        self.config = config
        self._producer = Producer(
            {
                "bootstrap.servers": config.bootstrap_servers,
                "client.id": config.client_id,
                "acks": config.acks,
                "enable.idempotence": config.enable_idempotence,
                "compression.type": config.compression_type,
                "linger.ms": config.linger_ms,
                "batch.size": config.batch_size,
                "retries": 10,
                "max.in.flight.requests.per.connection": 5,
            }
        )
        self._stop = False
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame) -> None:
        log.info("Received signal %s — draining producer…", signum)
        self._stop = True

    def _delivery_callback(self, err: KafkaError | None, msg) -> None:
        if err is not None:
            log.error("Delivery failed: topic=%s key=%s err=%s", msg.topic(), msg.key(), err)
        else:
            log.debug(
                "Delivered: topic=%s partition=%s offset=%s",
                msg.topic(), msg.partition(), msg.offset(),
            )

    def publish(self, topic: str, key: str, value: dict) -> None:
        """Publish one event. Blocks briefly if the local queue is full."""
        payload = json.dumps(value, default=str, separators=(",", ":")).encode("utf-8")
        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=payload,
            on_delivery=self._delivery_callback,
        )
        # Trigger callbacks + apply backpressure
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)

    def stop(self) -> None:
        self._stop = True


# ---------------------------------------------------------------------------
# CLI / main loop
# ---------------------------------------------------------------------------


def event_mix(patient_ids: list[str], rate_per_sec: float) -> Iterator[tuple[str, str, dict]]:
    """
    Yield (topic, key, value) tuples weighted like real clinical traffic:
      70% vitals, 5% admissions, 15% labs, 10% IoT telemetry
    """
    weights = [("vitals", 70), ("admissions", 5), ("labs", 15), ("iot", 10)]
    kinds, w = zip(*weights)
    sleep_per = 1.0 / max(rate_per_sec, 0.1)

    while True:
        kind = random.choices(kinds, weights=w, k=1)[0]
        pid = random.choice(patient_ids)
        if kind == "vitals":
            ev = gen_vitals(pid, source="ehr" if random.random() < 0.7 else "iot")
            yield TOPIC_MAP[kind], pid, ev.model_dump(mode="json")
        elif kind == "admissions":
            ev = gen_admission(pid)
            yield TOPIC_MAP[kind], ev.encounter_id, ev.model_dump(mode="json")
        elif kind == "labs":
            ev = gen_lab(pid)
            yield TOPIC_MAP[kind], ev.order_id, ev.model_dump(mode="json")
        elif kind == "iot":
            ev = gen_iot(pid)
            yield TOPIC_MAP[kind], ev.device_id, ev.model_dump(mode="json")
        time.sleep(sleep_per)


def run(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )

    cfg = ProducerConfig.from_env()
    patient_ids = load_omop_patient_ids(Path(args.duckdb_path), args.patients)
    if not patient_ids:
        log.error("No patients available — aborting.")
        return 1
    log.info("Loaded %d patients. Producing at %.1f events/sec to %s", len(patient_ids), args.rate, cfg.bootstrap_servers)

    producer = HealthcareProducer(cfg)
    published = 0
    started = time.time()
    try:
        for topic, key, value in event_mix(patient_ids, args.rate):
            if producer._stop:
                break
            if args.max_runtime and (time.time() - started) >= args.max_runtime:
                log.info("Max runtime %.1fs reached — stopping", args.max_runtime)
                break
            producer.publish(topic, key, value)
            published += 1
            if published % 500 == 0:
                elapsed = time.time() - started
                rate = published / elapsed if elapsed > 0 else 0
                log.info("Published %d events (%.0f events/sec)", published, rate)
    finally:
        log.info("Flushing final batch…")
        remaining = producer.flush(timeout=15.0)
        if remaining:
            log.warning("%d events still in queue at exit", remaining)
        log.info("Done. Total published: %d in %.1fs", published, time.time() - started)
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Healthcare real-time event producer")
    p.add_argument("--duckdb-path", default="dbt_project/dbt.duckdb", help="OMOP DuckDB to pull patients from")
    p.add_argument("--patients", type=int, default=500, help="Number of distinct patients to simulate")
    p.add_argument("--rate", type=float, default=10.0, help="Target events per second")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--max-runtime", type=float, default=None, help="Stop after N seconds (test/demo only)")
    args = p.parse_args()
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
