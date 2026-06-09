"""
Continuous IoT device simulator for the real-time healthcare pipeline.

Models 4-6 devices per patient, each emitting at its own cadence:
  - Wearable:  every 5 sec  (HR + steps)
  - Pulse ox:  every 10 sec (SpO2 + pulse)
  - BP cuff:   every 5 min  (systolic + diastolic)
  - Glucose:   every 15 min (glucose mg/dL)
  - Pill bottle: on event   (opened / closed)

Run alongside the producer (or as a standalone stream) to drive the
`iot.telemetry` topic with realistic-but-noisy wearable data.
"""
from __future__ import annotations

import argparse
import logging
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import yaml
from confluent_kafka import KafkaError, Producer

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from streaming.schemas.events import IoTDeviceType, IoTTelemetryEvent  # noqa: E402

log = logging.getLogger("iot_simulator")


# Cadence per device type (seconds)
CADENCE = {
    IoTDeviceType.WEARABLE: 5,
    IoTDeviceType.PULSE_OXIMETER: 10,
    IoTDeviceType.BP_CUFF: 300,
    IoTDeviceType.GLUCOSE_METER: 900,
    IoTDeviceType.CONTINUOUS_MONITOR: 2,
    IoTDeviceType.SMART_PILL_BOTTLE: 3600 * 2,  # event-driven, low frequency
}


def device_metrics(dtype: IoTDeviceType) -> dict:
    if dtype == IoTDeviceType.WEARABLE:
        return {
            "steps": random.randint(0, 50),
            "heart_rate_bpm": random.randint(60, 130),
            "active_minutes": random.choice([0, 0, 0, 1, 1, 2]),
        }
    if dtype == IoTDeviceType.PULSE_OXIMETER:
        return {
            "spo2_pct": round(random.uniform(91, 100), 1),
            "pulse_bpm": random.randint(60, 110),
        }
    if dtype == IoTDeviceType.BP_CUFF:
        return {
            "systolic_mmhg": random.randint(95, 170),
            "diastolic_mmhg": random.randint(55, 105),
            "pulse_pressure_mmhg": 0,  # overwritten below
        }
    if dtype == IoTDeviceType.GLUCOSE_METER:
        return {"glucose_mg_dl": random.randint(70, 220)}
    if dtype == IoTDeviceType.CONTINUOUS_MONITOR:
        return {
            "heart_rate_bpm": random.randint(55, 130),
            "spo2_pct": round(random.uniform(92, 100), 1),
            "resp_rate": random.randint(12, 22),
            "skin_temp_c": round(random.uniform(35.5, 37.5), 1),
        }
    if dtype == IoTDeviceType.SMART_PILL_BOTTLE:
        return {"opened": True, "pills_remaining": random.randint(0, 30)}
    return {}


def load_omop_patient_ids(duckdb_path: Path, limit: int) -> list[str]:
    try:
        import duckdb
        if not duckdb_path.exists():
            raise FileNotFoundError(duckdb_path)
        con = duckdb.connect(str(duckdb_path), read_only=True)
        rows = con.execute(f"SELECT person_id FROM omcdm_person ORDER BY random() LIMIT {int(limit)}").fetchall()
        con.close()
        return [str(r[0]) for r in rows]
    except Exception as e:  # noqa: BLE001
        log.warning("Could not load OMOP patients (%s). Using synthetic IDs.", e)
        return [f"syn_{i:05d}" for i in range(limit)]


def assign_devices(patient_ids: list[str], devices_per_patient: int) -> list[tuple[str, IoTDeviceType, str]]:
    """Each patient gets a stable set of devices, keyed (patient, device_type) -> device_id."""
    out: list[tuple[str, IoTDeviceType, str]] = []
    for pid in patient_ids:
        chosen = random.sample(list(IoTDeviceType), k=min(devices_per_patient, len(IoTDeviceType)))
        for dt in chosen:
            did = f"dev_{uuid.uuid5(uuid.NAMESPACE_DNS, f'{pid}.{dt.value}').hex[:10]}"
            out.append((pid, dt, did))
    return out


def main() -> int:
    p = argparse.ArgumentParser(description="IoT device simulator")
    p.add_argument("--duckdb-path", default="dbt_project/dbt.duckdb")
    p.add_argument("--patients", type=int, default=50)
    p.add_argument("--devices-per-patient", type=int, default=3, help="How many distinct device types per patient")
    p.add_argument("--bootstrap-servers", default="localhost:9092")
    p.add_argument("--topic", default="iot.telemetry")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )

    patient_ids = load_omop_patient_ids(Path(args.duckdb_path), args.patients)
    if not patient_ids:
        log.error("No patients — aborting.")
        return 1
    devices = assign_devices(patient_ids, args.devices_per_patient)
    log.info("Simulating %d devices across %d patients", len(devices), len(patient_ids))

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "client.id": "iot-simulator",
            "linger.ms": 10,
            "compression.type": "snappy",
        }
    )

    next_emit: dict[tuple[str, str], float] = {
        (pid, did): time.time() + random.uniform(0, 30) for (pid, _dt, did) in devices
    }
    stop = False

    def handle(_sig, _frm):
        nonlocal stop
        log.info("Stopping simulator…")
        stop = True

    signal.signal(signal.SIGINT, handle)
    signal.signal(signal.SIGTERM, handle)

    published = 0
    started = time.time()
    while not stop:
        now = time.time()
        # Emit any device whose cadence has elapsed
        for (pid, dt, did) in devices:
            key = (pid, did)
            if now < next_emit[key]:
                continue
            metrics = device_metrics(dt)
            if dt == IoTDeviceType.BP_CUFF:
                metrics["pulse_pressure_mmhg"] = metrics["systolic_mmhg"] - metrics["diastolic_mmhg"]
            ev = IoTTelemetryEvent(
                patient_id=pid,
                device_id=did,
                device_type=dt,
                metrics=metrics,
                battery_pct=round(random.uniform(20, 100), 1),
                source="edge_gateway" if dt in (IoTDeviceType.WEARABLE, IoTDeviceType.GLUCOSE_METER) else "iot",
            )
            producer.produce(
                topic=args.topic,
                key=did.encode(),
                value=ev.model_dump_json().encode(),
                on_delivery=lambda err, msg, dt=dt: log.error("deliver err: %s", err) if err else None,
            )
            published += 1
            next_emit[key] = now + CADENCE[dt] + random.uniform(-CADENCE[dt] * 0.1, CADENCE[dt] * 0.1)

        producer.poll(0)
        # Sleep until the soonest next emit
        soonest = min(next_emit.values()) - time.time()
        time.sleep(max(0.1, min(2.0, soonest)))

        if published and published % 1000 == 0:
            log.info("Simulator: %d events in %.0fs (%.0f/s)", published, time.time() - started, published / (time.time() - started))

    producer.flush(10)
    log.info("Simulator exiting. Total events: %d", published)
    return 0


if __name__ == "__main__":
    sys.exit(main())
