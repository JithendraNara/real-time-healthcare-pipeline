"""
Smoke tests for the streaming module — Pydantic models, event generation,
JSON schema validity. These run without Kafka (no broker needed).
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from streaming.schemas.events import (  # noqa: E402
    AdmissionEvent,
    AdmissionType,
    IoTDeviceType,
    IoTTelemetryEvent,
    LabResultEvent,
    PredictionEvent,
    PredictionType,
    VitalsEvent,
)


# ---------------------------------------------------------------------------
# Pydantic model tests
# ---------------------------------------------------------------------------


def test_vitals_event_basic():
    v = VitalsEvent(patient_id="p1", source="ehr", heart_rate_bpm=72, spo2_pct=98.0)
    assert v.patient_id == "p1"
    assert v.heart_rate_bpm == 72
    assert v.has_critical_value is False


def test_vitals_event_critical_detection():
    v = VitalsEvent(patient_id="p1", source="ehr", spo2_pct=85.0)
    assert v.has_critical_value is True

    v2 = VitalsEvent(patient_id="p1", source="ehr", heart_rate_bpm=150)
    assert v2.has_critical_value is True

    v3 = VitalsEvent(patient_id="p1", source="ehr", heart_rate_bpm=72, spo2_pct=98)
    assert v3.has_critical_value is False


def test_vitals_rejects_out_of_range():
    with pytest.raises(Exception):
        VitalsEvent(patient_id="p1", source="ehr", heart_rate_bpm=10)  # below ge=20
    with pytest.raises(Exception):
        VitalsEvent(patient_id="p1", source="ehr", spo2_pct=150.0)  # above le=100


def test_admission_event_enum():
    e = AdmissionEvent(
        patient_id="p1",
        source="ehr",
        encounter_id="e1",
        admission_type=AdmissionType.EMERGENCY,
        facility_id="fac_001",
        admit_time=__import__("datetime").datetime.now(__import__("datetime").timezone.utc),
    )
    assert e.admission_type == AdmissionType.EMERGENCY
    assert e.disposition == "still_admitted"  # default


def test_lab_result_is_critical():
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    lab = LabResultEvent(
        patient_id="p1",
        source="lab_device",
        order_id="o1",
        test_code="4548-4",
        test_name="HbA1c",
        value_numeric=12.0,
        reference_high=5.6,
        abnormal_flag="HH",
        collected_at=now,
        resulted_at=now,
    )
    assert lab.is_critical is True


def test_iot_event_metrics_dict():
    e = IoTTelemetryEvent(
        patient_id="p1",
        source="iot",
        device_id="d1",
        device_type=IoTDeviceType.GLUCOSE_METER,
        metrics={"glucose_mg_dl": 120, "trend": "rising"},
        battery_pct=80.0,
    )
    assert e.metrics["glucose_mg_dl"] == 120
    assert e.battery_pct == 80.0


def test_prediction_event_score_bounds():
    from datetime import datetime, timedelta, timezone
    now = datetime.now(timezone.utc)
    p = PredictionEvent(
        patient_id="p1",
        source="ehr",
        prediction_type=PredictionType.READMISSION_30D,
        model_id="readmit_v3",
        model_version="3.1.0",
        score=0.42,
        confidence=0.85,
        valid_from=now,
        valid_until=now + timedelta(days=30),
    )
    assert 0 <= p.score <= 1
    assert p.prediction_type == PredictionType.READMISSION_30D


def test_extra_fields_rejected():
    with pytest.raises(Exception):
        VitalsEvent(patient_id="p1", source="ehr", unknown_field="bad")  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# JSON Schema files
# ---------------------------------------------------------------------------


def test_json_schemas_exist():
    base = ROOT / "streaming" / "schemas" / "json_schemas"
    expected = ["vitals.json", "admission.json", "lab_result.json", "iot_telemetry.json", "prediction.json", "dlq.json"]
    for name in expected:
        assert (base / name).exists(), f"missing schema file: {name}"


def test_json_schemas_parse():
    base = ROOT / "streaming" / "schemas" / "json_schemas"
    for f in base.glob("*.json"):
        data = json.loads(f.read_text())
        assert "$schema" in data
        assert "properties" in data
        assert "required" in data


# ---------------------------------------------------------------------------
# Event generation
# ---------------------------------------------------------------------------


def test_vitals_generator_smoke(monkeypatch):
    from streaming.producers.healthcare_producer import gen_vitals
    random_state_before = __import__("random").getstate()
    try:
        v = gen_vitals("p1")
        assert v.patient_id == "p1"
        assert v.heart_rate_bpm is not None
    finally:
        __import__("random").setstate(random_state_before)


def test_admission_generator_smoke():
    from streaming.producers.healthcare_producer import gen_admission
    a = gen_admission("p1")
    assert a.encounter_id.startswith("enc_")
    assert a.admit_time is not None


def test_lab_generator_smoke():
    from streaming.producers.healthcare_producer import gen_lab
    l = gen_lab("p1")
    assert l.test_code
    assert l.unit


def test_iot_generator_smoke():
    from streaming.producers.healthcare_producer import gen_iot
    i = gen_iot("p1")
    assert i.device_id.startswith("dev_")
    assert i.metrics


# ---------------------------------------------------------------------------
# Validation pipeline
# ---------------------------------------------------------------------------


def test_parse_and_validate_happy_path():
    from streaming.consumers.glue_etl_job import parse_and_validate
    good = VitalsEvent(patient_id="p1", source="ehr", heart_rate_bpm=72).model_dump_json().encode()
    event, err = parse_and_validate("healthcare.vitals", good)
    assert err is None
    assert event is not None
    assert event["patient_id"] == "p1"


def test_parse_and_validate_invalid_json():
    from streaming.consumers.glue_etl_job import parse_and_validate
    event, err = parse_and_validate("healthcare.vitals", b"{not json")
    assert event is None
    assert "json" in err.lower()


def test_parse_and_validate_schema_fail():
    from streaming.consumers.glue_etl_job import parse_and_validate
    bad = b'{"patient_id": "p1", "heart_rate_bpm": 9999}'  # out of range
    event, err = parse_and_validate("healthcare.vitals", bad)
    assert event is None
    assert "validation" in err.lower() or "pydantic" in err.lower()


def test_parse_and_validate_unknown_topic():
    from streaming.consumers.glue_etl_job import parse_and_validate
    event, err = parse_and_validate("healthcare.unknown", b"{}")
    assert event is None
    assert "unknown" in err.lower()


def test_enrichment_known_patient():
    from streaming.consumers.glue_etl_job import enrich_with_omop
    cache = {"p1": {"gender_concept_id": 8532, "year_of_birth": 1965}}
    out = enrich_with_omop({"patient_id": "p1"}, cache)
    assert out["_enrichment"]["known_patient"] is True
    assert out["_enrichment"]["gender_concept_id"] == 8532


def test_enrichment_unknown_patient():
    from streaming.consumers.glue_etl_job import enrich_with_omop
    out = enrich_with_omop({"patient_id": "ghost"}, {})
    assert out["_enrichment"]["known_patient"] is False
