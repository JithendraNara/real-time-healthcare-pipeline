"""
End-to-end smoke test for the ML streaming integration.

Sets up:
  - A tiny in-memory OMOP DuckDB with 5 synthetic patients + 8 visits
  - A live Kafka producer that publishes 3 admission events
  - A one-shot scorer that consumes + scores + publishes to healthcare.predictions
  - A verification consumer that reads back the predictions and asserts shape

Run from repo root with:
  PYTHONPATH=. python ml/scripts/e2e_smoke_test.py

Requires the same Kafka broker as the streaming module (Redpanda on localhost:9092 by default).
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def _build_minimal_omop(duckdb_path: Path) -> dict[str, int]:
    """Create a minimal OMOP DuckDB with 5 patients + 8 visits + 12 conditions + 10 drugs.
    Returns a dict mapping patient_id → age for verification."""
    import duckdb

    con = duckdb.connect(str(duckdb_path))
    con.execute("""
        CREATE TABLE omcdm_person (
            person_id INTEGER PRIMARY KEY,
            gender_concept_id INTEGER,
            race_concept_id INTEGER,
            ethnicity_concept_id INTEGER,
            year_of_birth INTEGER
        );
        CREATE TABLE omcdm_visit_occurrence (
            visit_occurrence_id INTEGER PRIMARY KEY,
            person_id INTEGER,
            visit_start_datetime TIMESTAMP,
            visit_end_datetime TIMESTAMP,
            visit_concept_id INTEGER
        );
        CREATE TABLE omcdm_condition_occurrence (
            condition_occurrence_id INTEGER PRIMARY KEY,
            person_id INTEGER,
            condition_start_datetime TIMESTAMP,
            condition_concept_id INTEGER
        );
        CREATE TABLE omcdm_drug_exposure (
            drug_exposure_id INTEGER PRIMARY KEY,
            person_id INTEGER,
            drug_exposure_start_datetime TIMESTAMP,
            drug_concept_id INTEGER
        );
    """)

    # 5 patients: 1 young healthy, 2 middle-aged, 2 elderly with multiple chronic conditions
    now = datetime.now(timezone.utc)
    patients = [
        (1, 8532, 0, 0, 1995),  # F, young
        (2, 8507, 0, 0, 1975),  # M, middle-aged
        (3, 8532, 0, 0, 1980),  # F, middle-aged
        (4, 8507, 0, 0, 1955),  # M, elderly
        (5, 8532, 0, 0, 1948),  # F, elderly
    ]
    con.executemany(
        "INSERT INTO omcdm_person VALUES (?, ?, ?, ?, ?)",
        patients,
    )

    # Visits: 1-3 per patient, with end_datetime < now
    visits = []
    vid = 1
    for pid, *_ in patients:
        for i in range(1 if pid < 3 else 2):  # elderly patients have more visits
            start = now - timedelta(days=30 * (i + 1) + 5)
            end = start + timedelta(days=2)
            visits.append((vid, pid, start, end, 9201 if i == 0 else 9202))  # IP / OP
            vid += 1
    con.executemany(
        "INSERT INTO omcdm_visit_occurrence VALUES (?, ?, ?, ?, ?)",
        visits,
    )

    # Conditions: more for elderly
    conds = []
    cid = 1
    condition_concepts = {
        1: [444247],  # young: 1 condition
        2: [444247, 201826],  # middle: 2
        3: [444247, 201826, 4193700],  # middle: 3
        4: [444247, 201826, 4193700, 201254, 4149160],  # elderly: 5
        5: [444247, 201826, 4193700, 201254, 4149160, 432867],  # elderly: 6
    }
    for pid, *_ in patients:
        for c in condition_concepts[pid]:
            conds.append((cid, pid, now - timedelta(days=200), c))
            cid += 1
    con.executemany(
        "INSERT INTO omcdm_condition_occurrence VALUES (?, ?, ?, ?)",
        conds,
    )

    # Drugs
    drugs = []
    did = 1
    drug_counts = {1: 1, 2: 3, 3: 4, 4: 8, 5: 10}
    for pid, *_ in patients:
        for j in range(drug_counts[pid]):
            drugs.append((did, pid, now - timedelta(days=10 * (j + 1)), 1000000 + j))
            did += 1
    con.executemany(
        "INSERT INTO omcdm_drug_exposure VALUES (?, ?, ?, ?)",
        drugs,
    )

    con.close()
    return {p[0]: now.year - p[4] for p in patients}


def _publish_admissions(bootstrap: str, patient_ids: list[str]) -> None:
    from confluent_kafka import Producer
    p = Producer({"bootstrap.servers": bootstrap})
    for pid in patient_ids:
        payload = {
            "event_id": f"e2e-{pid}",
            "event_time": datetime.now(timezone.utc).isoformat(),
            "ingestion_time": datetime.now(timezone.utc).isoformat(),
            "source": "ehr",
            "schema_version": 1,
            "patient_id": pid,
            "encounter_id": f"enc-{pid}",
            "admission_type": "inpatient",
            "facility_id": "fac_001",
            "admit_time": (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat(),
            "discharge_time": datetime.now(timezone.utc).isoformat(),
            "disposition": "home",
            "diagnosis_codes": ["I10", "E11.9"],
        }
        p.produce("healthcare.admissions", key=pid.encode(), value=json.dumps(payload).encode())
    p.flush(10)


def _consume_predictions(bootstrap: str, expected: int, timeout_sec: float = 10.0) -> list[dict]:
    from confluent_kafka import Consumer

    c = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"e2e-verify-{int(time.time())}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    c.subscribe(["healthcare.predictions"])
    deadline = time.time() + timeout_sec
    out: list[dict] = []
    while time.time() < deadline and len(out) < expected:
        msg = c.poll(timeout=0.5)
        if msg is None or msg.error():
            continue
        out.append(json.loads(msg.value()))
    c.close()
    return out


def main() -> int:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "sqlite:///mlflow.db")
    registry_name = os.getenv("MLFLOW_REGISTRY_NAME", "readmission_30d")

    print(f"[1/5] Building minimal OMOP DuckDB…")
    omop_dir = Path(tempfile.mkdtemp(prefix="rthp-e2e-"))
    omop_path = omop_dir / "omop.duckdb"
    ages = _build_minimal_omop(omop_path)
    print(f"      Created OMOP with 5 patients, ages {ages}")

    os.environ["OMOP_DUCKDB"] = str(omop_path)
    os.environ["MLFLOW_TRACKING_URI"] = tracking_uri
    os.environ["MLFLOW_REGISTRY_NAME"] = registry_name

    print(f"[2/5] Publishing 3 admission events to {bootstrap}…")
    _publish_admissions(bootstrap, ["1", "3", "5"])  # young / middle / elderly

    print(f"[3/5] Loading model + scoring each patient (one-shot mode)…")
    from ml.outcomes.model_registry import RegistryConfig, load_production_model
    from ml.outcomes.feature_engineering import build_features_for_patient
    from ml.outcomes.readmission_predictor import predict
    from ml.realtime.scorer import PredictionPublisher

    model, feats = load_production_model(RegistryConfig(tracking_uri=tracking_uri, registry_name=registry_name))
    pub = PredictionPublisher(bootstrap, "healthcare.predictions")
    scored: list[dict] = []
    for pid in ["1", "3", "5"]:
        features = build_features_for_patient(pid, omop_path, None)
        if not features:
            print(f"      ✗ no features for patient {pid}")
            return 1
        result = predict(model, features)
        from ml.realtime.scorer import score_once
        ev = score_once(pid, model, feats, pub)
        scored.append({
            "patient_id": pid,
            "age": ages[int(pid)],
            "score": ev.score,
            "top_features": list(ev.top_feature_contributions.items())[:3],
        })
        print(f"      patient {pid} (age {ages[int(pid)]}) → score={ev.score:.3f} top={list(ev.top_feature_contributions.items())[:3]}")

    print(f"[4/5] Reading back predictions from healthcare.predictions…")
    preds = _consume_predictions(bootstrap, expected=3, timeout_sec=8.0)
    if len(preds) < 3:
        print(f"      ✗ expected ≥3 predictions, got {len(preds)}")
        return 1
    print(f"      ✓ received {len(preds)} predictions")

    print(f"[5/5] Verifying prediction shape…")
    for p in preds:
        assert "patient_id" in p, f"missing patient_id: {p}"
        assert "score" in p and 0 <= p["score"] <= 1, f"bad score: {p}"
        assert "prediction_type" in p
        assert p["prediction_type"] == "readmission_30d"
        assert "valid_from" in p and "valid_until" in p
        assert "top_feature_contributions" in p
    print(f"      ✓ all predictions have correct shape")

    print()
    print("=" * 60)
    print("END-TO-END SMOKE TEST — PASS")
    print("=" * 60)
    print()
    print("Risk ordering (youngest → lowest, oldest → highest is the expected signal):")
    for s in sorted(scored, key=lambda x: x["age"]):
        print(f"  age {s['age']:3d} (patient {s['patient_id']})  score={s['score']:.3f}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
