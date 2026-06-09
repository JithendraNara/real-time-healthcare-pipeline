"""
Real-time readmission scorer.

Subscribes to:
  - healthcare.admissions (on discharge — score that patient)
  - healthcare.vitals    (continuous — keep recent vitals hot in the cache)

On every discharge event, the scorer:
  1. Builds a feature vector for the patient from OMOP + recent silver vitals
  2. Calls the readmission_30d model (loaded from MLflow Production)
  3. Publishes a PredictionEvent to healthcare.predictions

Run modes:
  --once        score a single patient_id and exit (smoke test)
  --loop        continuous Kafka consumer (production)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, Producer

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from ml.outcomes.feature_engineering import build_features_for_patient  # noqa: E402
from ml.outcomes.model_registry import (  # noqa: E402
    RegistryConfig,
    load_production_model,
)
from ml.outcomes.readmission_predictor import predict  # noqa: E402
from streaming.schemas.events import PredictionEvent, PredictionType  # noqa: E402

log = logging.getLogger("realtime_scorer")


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------


def load_model(registry_name: str = "readmission_30d", tracking_uri: str = "mlruns"):
    cfg = RegistryConfig(tracking_uri=tracking_uri, registry_name=registry_name)
    return load_production_model(cfg)


# ---------------------------------------------------------------------------
# Producer for predictions
# ---------------------------------------------------------------------------


class PredictionPublisher:
    def __init__(self, bootstrap: str, topic: str = "healthcare.predictions"):
        self.topic = topic
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "client.id": f"readmission-scorer-{uuid.uuid4().hex[:8]}",
                "linger.ms": 50,
                "compression.type": "snappy",
                "acks": "all",
            }
        )

    def publish(self, event: PredictionEvent) -> None:
        self._producer.produce(
            topic=self.topic,
            key=str(event.patient_id).encode(),
            value=event.model_dump_json().encode(),
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 5.0) -> int:
        return self._producer.flush(timeout)


# ---------------------------------------------------------------------------
# One-shot scoring (smoke test / API alternative)
# ---------------------------------------------------------------------------


def score_once(patient_id: str, model, feature_names: list[str], publisher: PredictionPublisher, valid_for_minutes: int = 60) -> PredictionEvent:
    omop = Path(os.getenv("OMOP_DUCKDB", "dbt_project/dbt.duckdb"))
    silver = Path(os.getenv("SILVER_DUCKDB", "streaming/warehouse/silver.db"))
    if not omop.exists():
        raise FileNotFoundError(f"OMOP DuckDB not found: {omop}")

    features = build_features_for_patient(patient_id, omop, silver if silver.exists() else None)
    if not features:
        raise ValueError(f"patient {patient_id} not found in OMOP")

    result = predict(model, features)
    now = datetime.now(timezone.utc)
    event = PredictionEvent(
        patient_id=str(patient_id),
        source="ehr",
        prediction_type=PredictionType.READMISSION_30D,
        model_id="readmission_30d",
        model_version=os.getenv("MODEL_VERSION", "production"),
        score=result["score"],
        confidence=min(1.0, max(0.0, 1.0 - abs(result["score"] - 0.5) * 2)),  # crude — closer to 0.5 = less confident
        features_used=sorted(features.keys()),
        top_feature_contributions=result["top_feature_contributions"],
        valid_from=now,
        valid_until=now + timedelta(minutes=valid_for_minutes),
    )
    publisher.publish(event)
    publisher.flush(5)
    return event


# ---------------------------------------------------------------------------
# Continuous loop
# ---------------------------------------------------------------------------


def run_loop(args: argparse.Namespace) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s :: %(message)s")
    log.info("Loading model from MLflow…")
    model, feature_names = load_model(args.registry_name, args.tracking_uri)
    log.info("Loaded model with %d features", len(feature_names))

    publisher = PredictionPublisher(args.bootstrap_servers, topic=args.predictions_topic)
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": args.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([args.admissions_topic])
    log.info("Subscribed to %s", args.admissions_topic)

    stop = False

    def handle(_sig, _frm):
        nonlocal stop
        log.info("Stopping scorer…")
        stop = True

    signal.signal(signal.SIGINT, handle)
    signal.signal(signal.SIGTERM, handle)

    scored = 0
    started = time.time()
    while not stop:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            log.error("Consumer error: %s", msg.error())
            continue
        try:
            payload = json.loads(msg.value())
        except Exception as e:  # noqa: BLE001
            log.warning("Bad JSON on admissions topic: %s", e)
            consumer.commit(msg)
            continue
        patient_id = payload.get("patient_id")
        if not patient_id:
            log.debug("Admission event missing patient_id: %s", payload)
            consumer.commit(msg)
            continue
        try:
            event = score_once(str(patient_id), model, feature_names, publisher)
            scored += 1
            log.info(
                "Scored patient=%s score=%.3f band=%s top=%s",
                patient_id, event.score, event.risk_band if hasattr(event, "risk_band") else "?",
                list(event.top_feature_contributions.items())[:3],
            )
        except FileNotFoundError as e:
            log.warning("Skipping score for patient=%s: %s", patient_id, e)
        except ValueError as e:
            log.debug("Skipping score for patient=%s: %s", patient_id, e)
        except Exception as e:  # noqa: BLE001
            log.exception("Score failed for patient=%s: %s", patient_id, e)
        consumer.commit(msg, asynchronous=False)

        if scored and scored % 50 == 0:
            log.info("Scored %d patients in %.0fs (%.1f/s)", scored, time.time() - started, scored / (time.time() - started))

    publisher.flush(10)
    consumer.close()
    log.info("Final: scored=%d runtime=%.0fs", scored, time.time() - started)
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    p = argparse.ArgumentParser(description="Real-time readmission scorer")
    p.add_argument("--once", help="Score this patient_id and exit (smoke test)")
    p.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    p.add_argument("--admissions-topic", default="healthcare.admissions")
    p.add_argument("--predictions-topic", default="healthcare.predictions")
    p.add_argument("--group-id", default=os.getenv("KAFKA_GROUP_ID", "readmission-scorer"))
    p.add_argument("--registry-name", default=os.getenv("MLFLOW_REGISTRY_NAME", "readmission_30d"))
    p.add_argument("--tracking-uri", default=os.getenv("MLFLOW_TRACKING_URI", "mlruns"))
    args = p.parse_args()

    if args.once:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s :: %(message)s")
        log.info("Loading model for one-shot scoring…")
        model, feature_names = load_model(args.registry_name, args.tracking_uri)
        publisher = PredictionPublisher(args.bootstrap_servers, args.predictions_topic)
        event = score_once(args.once, model, feature_names, publisher)
        print(event.model_dump_json(indent=2))
        return 0

    return run_loop(args)


if __name__ == "__main__":
    sys.exit(main())
