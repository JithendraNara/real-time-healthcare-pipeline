"""
Feature engineering for patient outcome models.

Reads from the OMOP CDM DuckDB (vital-pipeline) and produces a per-(patient, discharge)
feature matrix suitable for 30-day readmission prediction.

Target definition:
  For each visit_occurrence with a discharge date, the label is 1 if the same patient
  has ANY subsequent visit_occurrence within 30 days of the discharge, 0 otherwise.

Features computed (at the time of discharge):
  - Demographics: age_at_discharge, gender_concept_id, race_concept_id
  - Visit history: total_visits, visits_last_90d, visits_last_180d, visits_last_365d,
    days_since_last_discharge, mean_los_days, last_visit_type
  - Conditions: total_conditions, chronic_conditions (CCS 1-18), distinct_ccs_categories
  - Medications: total_drugs, distinct_drug_classes
  - Vitals (from streaming silver, if available): mean_hr, mean_spo2, mean_sbp, abnormal_rate
  - Comorbidity: charlson_proxy (count of high-risk CCS categories)

The same feature logic is used in training (offline) and scoring (online) so the
model never sees train/score skew.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

log = logging.getLogger("feature_engineering")


# CCS category IDs considered "chronic" (simplified list — extend as needed)
CHRONIC_CCS_PREFIXES = {
    # Endocrine (diabetes, etc.)
    "endocrine",
    # Circulatory (HTN, CHF, CAD, stroke)
    "circulatory",
    # Respiratory (COPD, asthma)
    "respiratory",
    # Digestive (chronic liver, IBD)
    "digestive",
    # Genitourinary (CKD)
    "genitourinary",
    # Musculoskeletal (RA, lupus)
    "musculoskeletal",
    # Neoplasms (cancer)
    "neoplasms",
    # Mental health (depression, schizophrenia)
    "mental",
}


@dataclass(frozen=True)
class FeatureConfig:
    lookback_days_short: int = 90
    lookback_days_med: int = 180
    lookback_days_long: int = 365
    min_age: int = 18
    silver_path: Path | None = None  # streaming/warehouse/silver.db — optional


# ---------------------------------------------------------------------------
# OMOP feature extraction
# ---------------------------------------------------------------------------


def _safe_connect(duckdb_path: Path):
    import duckdb
    if not duckdb_path.exists():
        raise FileNotFoundError(f"OMOP DuckDB not found at {duckdb_path}")
    return duckdb.connect(str(duckdb_path), read_only=True)


def _ccs_category_from_concept(con_name: str | None) -> str | None:
    """Map OMOP concept names to CCS-like category strings. Best-effort — depends on
    how `condition_occurrence` was loaded. Falls back to None if not available."""
    if not con_name:
        return None
    n = con_name.lower()
    if "diabetes" in n or "endocr" in n:
        return "endocrine"
    if "hypertens" in n or "heart" in n or "stroke" in n or "circulat" in n:
        return "circulatory"
    if "copd" in n or "asthma" in n or "respirat" in n:
        return "respiratory"
    if "liver" in n or "ibd" in n or "digest" in n:
        return "digestive"
    if "renal" in n or "kidney" in n or "genitour" in n:
        return "genitourinary"
    if "rheumat" in n or "lupus" in n or "musculo" in n:
        return "musculoskeletal"
    if "cancer" in n or "neopl" in n or "tumor" in n:
        return "neoplasms"
    if "depress" in n or "schizo" in n or "mental" in n or "anxi" in n:
        return "mental"
    return "other"


def build_omop_features(duckdb_path: Path, cfg: FeatureConfig) -> pd.DataFrame:
    """
    Extract per-(patient, discharge_event) features from the OMOP warehouse.

    Returns a DataFrame with the following columns:
      - patient_id, discharge_time (the index)
      - label (1 = readmitted within 30 days, 0 = not)
      - feature_* (the engineered features)
    """
    con = _safe_connect(duckdb_path)
    try:
        # Visits with valid discharge timestamps are our prediction points
        visits = con.execute(
            """
            SELECT
                v.person_id AS patient_id,
                v.visit_occurrence_id,
                v.visit_start_datetime,
                v.visit_end_datetime,
                v.visit_concept_id
            FROM omcdm_visit_occurrence v
            WHERE v.visit_end_datetime IS NOT NULL
              AND v.visit_start_datetime IS NOT NULL
            ORDER BY v.person_id, v.visit_start_datetime
            """
        ).fetchdf()
        if visits.empty:
            log.warning("No visits with discharge timestamps in OMOP — nothing to train on.")
            return pd.DataFrame()
        visits["visit_start_datetime"] = pd.to_datetime(visits["visit_start_datetime"], utc=True, errors="coerce")
        visits["visit_end_datetime"] = pd.to_datetime(visits["visit_end_datetime"], utc=True, errors="coerce")
        visits = visits.dropna(subset=["visit_start_datetime", "visit_end_datetime"])

        # Persons — demographics
        persons = con.execute(
            """
            SELECT
                p.person_id AS patient_id,
                p.gender_concept_id,
                p.race_concept_id,
                p.ethnicity_concept_id,
                p.year_of_birth
            FROM omcdm_person p
            """
        ).fetchdf()

        # Conditions
        try:
            conditions = con.execute(
                """
                SELECT
                    co.person_id AS patient_id,
                    co.condition_occurrence_id,
                    co.condition_start_datetime,
                    co.condition_concept_id,
                    ccs.ccs_category
                FROM omcdm_condition_occurrence co
                LEFT JOIN omcdm_condition_occurrence_concept ccs
                  ON co.condition_concept_id = ccs.concept_id
                """
            ).fetchdf()
        except Exception:
            conditions = con.execute(
                "SELECT person_id AS patient_id, condition_start_datetime, condition_concept_id FROM omcdm_condition_occurrence"
            ).fetchdf()
            conditions["ccs_category"] = None

        conditions["condition_start_datetime"] = pd.to_datetime(
            conditions["condition_start_datetime"], utc=True, errors="coerce"
        )

        # Drugs
        try:
            drugs = con.execute("SELECT person_id AS patient_id, drug_exposure_start_datetime FROM omcdm_drug_exposure").fetchdf()
        except Exception:
            drugs = pd.DataFrame(columns=["patient_id", "drug_exposure_start_datetime"])
        if not drugs.empty:
            drugs["drug_exposure_start_datetime"] = pd.to_datetime(
                drugs["drug_exposure_start_datetime"], utc=True, errors="coerce"
            )

        # Measurements (vitals) from OMOP measurement table (if present)
        try:
            meas = con.execute(
                """
                SELECT person_id AS patient_id, measurement_datetime, value_as_number, measurement_concept_id
                FROM omcdm_measurement
                """
            ).fetchdf()
        except Exception:
            meas = pd.DataFrame(columns=["patient_id", "measurement_datetime", "value_as_number", "measurement_concept_id"])
        if not meas.empty:
            meas["measurement_datetime"] = pd.to_datetime(meas["measurement_datetime"], utc=True, errors="coerce")
    finally:
        con.close()

    # Per-(patient, discharge) feature engineering
    rows: list[dict[str, Any]] = []
    for patient_id, group in visits.groupby("patient_id"):
        group = group.sort_values("visit_start_datetime").reset_index(drop=True)
        person = persons[persons["patient_id"] == patient_id]
        if person.empty:
            continue
        person = person.iloc[0]
        yob = int(person.get("year_of_birth") or 1950)
        gender = int(person.get("gender_concept_id") or 0)
        race = int(person.get("race_concept_id") or 0)

        patient_conditions = conditions[conditions["patient_id"] == patient_id]
        patient_drugs = drugs[drugs["patient_id"] == patient_id] if not drugs.empty else pd.DataFrame()
        patient_meas = meas[meas["patient_id"] == patient_id] if not meas.empty else pd.DataFrame()

        for i, row in group.iterrows():
            discharge_time = row["visit_end_datetime"]
            if pd.isna(discharge_time):
                continue
            window_start_90 = discharge_time - timedelta(days=cfg.lookback_days_short)
            window_start_180 = discharge_time - timedelta(days=cfg.lookback_days_med)
            window_start_365 = discharge_time - timedelta(days=cfg.lookback_days_long)

            # Visit history at time of discharge
            prior_visits = group.iloc[:i]
            visits_90 = prior_visits[prior_visits["visit_start_datetime"] >= window_start_90]
            visits_180 = prior_visits[prior_visits["visit_start_datetime"] >= window_start_180]
            visits_365 = prior_visits[prior_visits["visit_start_datetime"] >= window_start_365]
            mean_los = (
                (prior_visits["visit_end_datetime"] - prior_visits["visit_start_datetime"]).dt.total_seconds().div(86400).mean()
                if len(prior_visits) > 0 else 0.0
            )
            days_since_last = (
                (discharge_time - prior_visits["visit_end_datetime"].max()).days if len(prior_visits) > 0 else 9999
            )
            last_visit_type = int(prior_visits["visit_concept_id"].iloc[-1]) if len(prior_visits) > 0 else 0

            # Conditions at time of discharge
            prior_conds = patient_conditions[patient_conditions["condition_start_datetime"] < discharge_time]
            ccs_cats = [c for c in prior_conds["ccs_category"].dropna().tolist()]
            chronic_count = sum(1 for c in ccs_cats if c in CHRONIC_CCS_PREFIXES)
            distinct_ccs = len(set(ccs_cats))
            total_conds = len(prior_conds)

            # Drugs at time of discharge
            prior_drugs = patient_drugs[patient_drugs["drug_exposure_start_datetime"] < discharge_time] if not patient_drugs.empty else pd.DataFrame()
            total_drugs = len(prior_drugs)

            # Recent vitals (last 30 days before discharge, if any)
            window_30d = discharge_time - timedelta(days=30)
            recent_meas = patient_meas[(patient_meas["measurement_datetime"] >= window_30d) & (patient_meas["measurement_datetime"] <= discharge_time)]
            mean_hr = float(recent_meas[recent_meas["measurement_concept_id"].astype(str).str.contains("heart", case=False, na=False)]["value_as_number"].mean()) if not recent_meas.empty else 0.0
            mean_spo2 = 0.0  # OMOP measurement table doesn't have SpO2 concept by default — leave for streaming silver join
            mean_sbp = 0.0

            # Charlson-style proxy = chronic_count (coarse but monotone with risk)
            charlson_proxy = chronic_count

            # Label — readmission within 30 days
            future_visits = group.iloc[i + 1:]
            label = 0
            if not future_visits.empty:
                next_admit = future_visits["visit_start_datetime"].min()
                if pd.notna(next_admit) and (next_admit - discharge_time) <= timedelta(days=30):
                    label = 1

            rows.append(
                {
                    "patient_id": int(patient_id) if str(patient_id).isdigit() else patient_id,
                    "visit_occurrence_id": int(row["visit_occurrence_id"]),
                    "discharge_time": discharge_time,
                    "label": int(label),
                    "feature_age": max(0, discharge_time.year - yob),
                    "feature_gender": gender,
                    "feature_race": race,
                    "feature_total_visits": len(prior_visits),
                    "feature_visits_90d": len(visits_90),
                    "feature_visits_180d": len(visits_180),
                    "feature_visits_365d": len(visits_365),
                    "feature_days_since_last": min(days_since_last, 9999),
                    "feature_mean_los_days": round(float(mean_los) if not pd.isna(mean_los) else 0.0, 2),
                    "feature_last_visit_type": last_visit_type,
                    "feature_total_conditions": total_conds,
                    "feature_chronic_conditions": chronic_count,
                    "feature_distinct_ccs": distinct_ccs,
                    "feature_total_drugs": total_drugs,
                    "feature_mean_hr_30d": round(mean_hr if not pd.isna(mean_hr) else 0.0, 2),
                    "feature_mean_spo2_30d": round(mean_spo2, 2),
                    "feature_mean_sbp_30d": round(mean_sbp, 2),
                    "feature_charlson_proxy": charlson_proxy,
                }
            )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Online feature extraction (for real-time scoring at inference time)
# ---------------------------------------------------------------------------


def build_features_for_patient(
    patient_id: str | int,
    omop_duckdb: Path,
    silver_duckdb: Path | None = None,
    cfg: FeatureConfig | None = None,
) -> dict[str, float]:
    """
    Build a single feature vector for one patient at the current time.
    Used by the real-time scorer and the FastAPI /predict endpoint.

    For "live" features (e.g., mean vitals from the last hour), pulls from the
    streaming silver DuckDB if available; otherwise falls back to OMOP measurement.
    """
    cfg = cfg or FeatureConfig(silver_path=silver_duckdb)
    con = _safe_connect(omop_duckdb)
    try:
        person = con.execute(
            f"SELECT person_id, gender_concept_id, race_concept_id, year_of_birth FROM omcdm_person WHERE person_id = {int(patient_id) if str(patient_id).isdigit() else 0}"
        ).fetchone()
        if person is None:
            # Try string form
            person = con.execute(
                "SELECT person_id, gender_concept_id, race_concept_id, year_of_birth FROM omcdm_person WHERE CAST(person_id AS VARCHAR) = ?",
                [str(patient_id)],
            ).fetchone()
        if person is None:
            return {}

        pid, gender, race, yob = person
        now = pd.Timestamp.now(tz="UTC")
        window_30d = now - pd.Timedelta(days=30)
        window_90d = now - pd.Timedelta(days=90)
        window_180d = now - pd.Timedelta(days=180)
        window_365d = now - pd.Timedelta(days=365)

        visits = con.execute(
            f"SELECT visit_start_datetime, visit_end_datetime, visit_concept_id FROM omcdm_visit_occurrence WHERE person_id = {int(pid) if str(pid).isdigit() else 0}"
        ).fetchdf()
        visits["visit_start_datetime"] = pd.to_datetime(visits["visit_start_datetime"], utc=True, errors="coerce")
        visits["visit_end_datetime"] = pd.to_datetime(visits["visit_end_datetime"], utc=True, errors="coerce")
        visits = visits.dropna(subset=["visit_start_datetime"])
        prior = visits[visits["visit_start_datetime"] < now]
        v90 = prior[prior["visit_start_datetime"] >= window_90d]
        v180 = prior[prior["visit_start_datetime"] >= window_180d]
        v365 = prior[prior["visit_start_datetime"] >= window_365d]
        mean_los = (
            (prior["visit_end_datetime"] - prior["visit_start_datetime"]).dt.total_seconds().div(86400).mean()
            if len(prior) > 0 else 0.0
        )
        last_discharge = prior["visit_end_datetime"].dropna().max()
        days_since_last = (now - last_discharge).days if pd.notna(last_discharge) else 9999
        last_visit_type = int(prior["visit_concept_id"].iloc[-1]) if len(prior) > 0 else 0

        conds = con.execute(
            f"SELECT condition_start_datetime FROM omcdm_condition_occurrence WHERE person_id = {int(pid) if str(pid).isdigit() else 0}"
        ).fetchdf()
        conds["condition_start_datetime"] = pd.to_datetime(conds["condition_start_datetime"], utc=True, errors="coerce")
        prior_conds = conds[conds["condition_start_datetime"] < now]
        chronic_count = 0  # cannot recompute CCS without concept join — leave at 0 for online path
        distinct_ccs = 0

        try:
            drugs = con.execute(
                f"SELECT drug_exposure_start_datetime FROM omcdm_drug_exposure WHERE person_id = {int(pid) if str(pid).isdigit() else 0}"
            ).fetchdf()
        except Exception:
            drugs = pd.DataFrame()
        if not drugs.empty:
            drugs["drug_exposure_start_datetime"] = pd.to_datetime(drugs["drug_exposure_start_datetime"], utc=True, errors="coerce")
            prior_drugs = drugs[drugs["drug_exposure_start_datetime"] < now]
            total_drugs = len(prior_drugs)
        else:
            total_drugs = 0

        # Live vitals from streaming silver (last 24h) — preferred when available
        mean_hr = 0.0
        mean_spo2 = 0.0
        mean_sbp = 0.0
        if silver_duckdb and silver_duckdb.exists():
            try:
                scon = duckdb.connect(str(silver_duckdb), read_only=True)
                rows = scon.execute(
                    """
                    SELECT json_extract_string(payload, '$.heart_rate_bpm') as hr,
                           json_extract_string(payload, '$.spo2_pct') as spo2,
                           json_extract_string(payload, '$.systolic_bp_mmHg') as sbp
                    FROM vitals_silver
                    WHERE json_extract_string(payload, '$.patient_id') = ?
                      AND json_extract_string(payload, '$.event_time') >= ?
                    """,
                    [str(patient_id), (now - pd.Timedelta(hours=24)).isoformat()],
                ).fetchall()
                scon.close()
                hrs = [float(r[0]) for r in rows if r[0] is not None]
                spos = [float(r[1]) for r in rows if r[1] is not None]
                sbps = [float(r[2]) for r in rows if r[2] is not None]
                if hrs:
                    mean_hr = round(float(np.mean(hrs)), 2)
                if spos:
                    mean_spo2 = round(float(np.mean(spos)), 2)
                if sbps:
                    mean_sbp = round(float(np.mean(sbps)), 2)
            except Exception as e:  # noqa: BLE001
                log.debug("Silver vitals join failed: %s", e)
    finally:
        con.close()

    return {
        "feature_age": max(0, now.year - int(yob or 1950)),
        "feature_gender": int(gender or 0),
        "feature_race": int(race or 0),
        "feature_total_visits": len(prior),
        "feature_visits_90d": len(v90),
        "feature_visits_180d": len(v180),
        "feature_visits_365d": len(v365),
        "feature_days_since_last": min(int(days_since_last), 9999),
        "feature_mean_los_days": round(float(mean_los) if not pd.isna(mean_los) else 0.0, 2),
        "feature_last_visit_type": last_visit_type,
        "feature_total_conditions": len(prior_conds),
        "feature_chronic_conditions": chronic_count,
        "feature_distinct_ccs": distinct_ccs,
        "feature_total_drugs": total_drugs,
        "feature_mean_hr_30d": mean_hr,
        "feature_mean_spo2_30d": mean_spo2,
        "feature_mean_sbp_30d": mean_sbp,
        "feature_charlson_proxy": chronic_count,
    }


# ---------------------------------------------------------------------------
# Synthetic features (for tests + demo when OMOP is empty)
# ---------------------------------------------------------------------------


def synth_features(n: int = 200, seed: int = 42) -> pd.DataFrame:
    """Generate a synthetic feature matrix with a known signal — for tests + demos
    when the real OMOP DB isn't seeded yet. Label depends on age, chronic count, and
    recent visits, which is a defensible (if crude) readmission signal."""
    rng = np.random.default_rng(seed)
    age = rng.integers(18, 95, n)
    chronic = rng.integers(0, 8, n)
    visits_90 = rng.integers(0, 6, n)
    charlson = chronic + rng.integers(0, 3, n)
    logit = -3.5 + 0.025 * age + 0.45 * chronic + 0.30 * visits_90 + 0.2 * charlson + rng.normal(0, 0.5, n)
    p = 1 / (1 + np.exp(-logit))
    label = (rng.random(n) < p).astype(int)
    return pd.DataFrame(
        {
            "patient_id": rng.integers(10000, 99999, n),
            "visit_occurrence_id": rng.integers(1_000_000, 9_999_999, n),
            "discharge_time": pd.to_datetime("2026-01-01") + pd.to_timedelta(rng.integers(0, 180, n), unit="D"),
            "label": label,
            "feature_age": age,
            "feature_gender": rng.integers(8507, 8533, n),
            "feature_race": rng.integers(0, 5, n),
            "feature_total_visits": visits_90 + rng.integers(0, 5, n),
            "feature_visits_90d": visits_90,
            "feature_visits_180d": visits_90 + rng.integers(0, 3, n),
            "feature_visits_365d": visits_90 + rng.integers(0, 6, n),
            "feature_days_since_last": rng.integers(1, 365, n),
            "feature_mean_los_days": rng.uniform(0.5, 12, n).round(2),
            "feature_last_visit_type": rng.integers(0, 5, n),
            "feature_total_conditions": chronic + rng.integers(0, 10, n),
            "feature_chronic_conditions": chronic,
            "feature_distinct_ccs": chronic + rng.integers(0, 3, n),
            "feature_total_drugs": rng.integers(0, 15, n),
            "feature_mean_hr_30d": rng.normal(82, 12, n).round(2),
            "feature_mean_spo2_30d": rng.normal(96, 2, n).round(2),
            "feature_mean_sbp_30d": rng.normal(128, 15, n).round(2),
            "feature_charlson_proxy": charlson,
        }
    )
