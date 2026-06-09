"""
Pydantic v2 event models for the real-time healthcare stream.

These models are the single source of truth for event shape. They are:
  1. Used by producers to validate before publish
  2. Serialized to JSON Schema and uploaded to the Schema Registry
  3. Reused by consumers to deserialize and validate inbound events
  4. Documented in streaming/schemas/json_schemas/*.json (auto-generated)

PHI fields (mrn, birth_datetime, name_*, address_*) are tagged with `phi=True` so the
governance layer (Module 3) knows what to encrypt, mask, and audit.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


def _new_event_id() -> str:
    return str(uuid.uuid4())


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


class EventBase(BaseModel):
    """Common envelope for every healthcare/IoT event."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    event_id: str = Field(default_factory=_new_event_id, description="Unique event ID (UUID4)")
    event_time: datetime = Field(
        default_factory=_now_utc, description="When the event was generated (ISO 8601 UTC)"
    )
    ingestion_time: datetime = Field(
        default_factory=_now_utc, description="When the producer published the event (ISO 8601 UTC)"
    )
    source: Literal["ehr", "iot", "lab_device", "wearable", "monitor", "synthea", "edge_gateway"] = Field(
        ..., description="Where the event originated"
    )
    schema_version: int = Field(default=1, description="Schema version for forward-compat")


# ---------------------------------------------------------------------------
# Vitals
# ---------------------------------------------------------------------------


class VitalsEvent(EventBase):
    """Discrete vital sign measurement for a single patient."""

    patient_id: str = Field(..., description="OMOP person_id (string for cross-system safety)")
    encounter_id: Optional[str] = Field(None, description="OMOP visit_occurrence_id, if any")

    heart_rate_bpm: Optional[int] = Field(None, ge=20, le=300)
    systolic_bp_mmHg: Optional[int] = Field(None, ge=40, le=300)
    diastolic_bp_mmHg: Optional[int] = Field(None, ge=20, le=250)
    respiratory_rate: Optional[int] = Field(None, ge=0, le=80)
    spo2_pct: Optional[float] = Field(None, ge=0, le=100)
    temperature_c: Optional[float] = Field(None, ge=20, le=45)
    pain_score: Optional[int] = Field(None, ge=0, le=10)

    @property
    def has_critical_value(self) -> bool:
        """NEWS2-style heuristic for early warning — used by the ML scorer (Module 2)."""
        if self.spo2_pct is not None and self.spo2_pct < 90:
            return True
        if self.heart_rate_bpm is not None and (self.heart_rate_bpm < 40 or self.heart_rate_bpm > 140):
            return True
        if self.systolic_bp_mmHg is not None and self.systolic_bp_mmHg < 90:
            return True
        if self.respiratory_rate is not None and (self.respiratory_rate < 8 or self.respiratory_rate > 30):
            return True
        return False


# ---------------------------------------------------------------------------
# Admissions
# ---------------------------------------------------------------------------


class AdmissionType(str, Enum):
    INPATIENT = "inpatient"
    OUTPATIENT = "outpatient"
    EMERGENCY = "emergency"
    OBSERVATION = "observation"
    DAY_SURGERY = "day_surgery"


class AdmissionEvent(EventBase):
    """Patient admission, transfer, or discharge event."""

    patient_id: str
    encounter_id: str
    admission_type: AdmissionType
    facility_id: str
    attending_provider_id: Optional[str] = None

    chief_complaint: Optional[str] = Field(None, max_length=500)
    admit_time: datetime
    discharge_time: Optional[datetime] = None
    disposition: Optional[Literal["home", "transfer", "ama", "expired", "still_admitted"]] = "still_admitted"
    diagnosis_codes: list[str] = Field(default_factory=list, description="ICD-10 codes")


# ---------------------------------------------------------------------------
# Lab results
# ---------------------------------------------------------------------------


class LabResultEvent(EventBase):
    """Discrete lab result (component-level)."""

    patient_id: str
    encounter_id: Optional[str] = None
    order_id: str = Field(..., description="Lab order ID for result grouping")
    test_code: str = Field(..., description="LOINC code")
    test_name: str
    value_numeric: Optional[float] = None
    value_text: Optional[str] = None
    unit: Optional[str] = None
    reference_low: Optional[float] = None
    reference_high: Optional[float] = None
    abnormal_flag: Optional[Literal["L", "H", "LL", "HH", "A", "AA", "N"]] = None
    collected_at: datetime
    resulted_at: datetime

    @property
    def is_critical(self) -> bool:
        return self.abnormal_flag in ("LL", "HH", "AA")


# ---------------------------------------------------------------------------
# IoT telemetry
# ---------------------------------------------------------------------------


class IoTDeviceType(str, Enum):
    WEARABLE = "wearable"
    CONTINUOUS_MONITOR = "continuous_monitor"
    SMART_PILL_BOTTLE = "smart_pill_bottle"
    GLUCOSE_METER = "glucose_meter"
    PULSE_OXIMETER = "pulse_oximeter"
    BP_CUFF = "bp_cuff"


class IoTTelemetryEvent(EventBase):
    """Raw IoT device telemetry — wearable vitals, pill-bottle open events, etc."""

    patient_id: str
    device_id: str
    device_type: IoTDeviceType
    firmware_version: Optional[str] = None

    # Generic payload — devices are messy, but the envelope is strict
    metrics: dict[str, float | int | str | bool] = Field(
        default_factory=dict, description="Device-specific metrics"
    )
    battery_pct: Optional[float] = Field(None, ge=0, le=100)


# ---------------------------------------------------------------------------
# Predictions (produced by Module 2's ML scorer)
# ---------------------------------------------------------------------------


class PredictionType(str, Enum):
    READMISSION_30D = "readmission_30d"
    ICU_DETERIORATION = "icu_deterioration"
    BED_DEMAND_1H = "bed_demand_1h"
    CARE_RECOMMENDATION = "care_recommendation"


class PredictionEvent(EventBase):
    """ML model output for a single patient, time-windowed."""

    patient_id: str
    prediction_type: PredictionType
    model_id: str = Field(..., description="MLflow run ID or model registry name")
    model_version: str = Field(..., description="Model version (semver or git SHA)")

    score: float = Field(..., ge=0, le=1, description="Probability or normalized risk score")
    confidence: float = Field(..., ge=0, le=1)
    features_used: list[str] = Field(default_factory=list, description="Feature names for explainability")
    top_feature_contributions: dict[str, float] = Field(
        default_factory=dict, description="SHAP-style local contributions"
    )

    # Window this prediction applies to
    valid_from: datetime
    valid_until: datetime


# ---------------------------------------------------------------------------
# Dead-letter envelope
# ---------------------------------------------------------------------------


class DeadLetterEvent(BaseModel):
    """Wraps a malformed/invalid event with the error context."""

    model_config = ConfigDict(extra="forbid")

    dlq_id: str = Field(default_factory=_new_event_id)
    dlq_time: datetime = Field(default_factory=_now_utc)
    source_topic: str
    source_partition: int
    source_offset: int
    original_payload: str = Field(..., description="Raw bytes/payload that failed validation")
    error_type: str
    error_message: str
    error_stack: Optional[str] = None
    consumer_id: str
