"""
Local Synthea-like data seeder.

Generates a small synthetic cohort that matches the Synthea CSV schema
(patients, conditions, encounters) and seeds DuckDB so the OMOP models
can be built locally without any AWS or Synthea download.

Run:
    python scripts/seed_omop.py
    cd dbt_project && DBT_PROFILES_DIR=. dbt build --profile vital_pipeline --target local
"""
from __future__ import annotations

import argparse
import os
import random
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).parent.parent
DUCKDB_PATH = ROOT / "dbt_project" / "dbt.duckdb"

CONDITIONS = [
    ("I10",  "Essential (primary) hypertension"),
    ("E11.9", "Type 2 diabetes mellitus without complications"),
    ("J45.909", "Unspecified asthma, uncomplicated"),
    ("M54.5",  "Low back pain"),
    ("F41.9",  "Anxiety disorder, unspecified"),
    ("K21.9",  "Gastro-esophageal reflux disease without esophagitis"),
    ("J06.9",  "Acute upper respiratory infection, unspecified"),
    ("R51.9",  "Headache, unspecified"),
    ("N39.0",  "Urinary tract infection, site not specified"),
    ("B34.9",  "Viral infection, unspecified"),
]
ENCOUNTER_CLASSES = ["inpatient", "outpatient", "emergency", "ambulatory", "wellness"]

# Realistic Synthea-shaped medications (code, description, indication)
MEDICATIONS = [
    ("197361", "Lisinopril 10 MG Oral Tablet", "hypertension"),
    ("617314", "Metformin 500 MG Oral Tablet", "diabetes"),
    ("197935", "Atorvastatin 20 MG Oral Tablet", "hyperlipidemia"),
    ("617320", "Amoxicillin 500 MG Oral Capsule", "infection"),
    ("197884", "Albuterol 0.09 MG/ACTUAT Inhaler", "asthma"),
    ("197361", "Lisinopril 20 MG Oral Tablet", "hypertension"),
    ("6809",   "Metformin 1000 MG Oral Tablet", "diabetes"),
    ("1116628", "Atorvastatin 40 MG Oral Tablet", "hyperlipidemia"),
    ("10180",  "Sertraline 50 MG Oral Tablet", "depression"),
    ("312940", "Ibuprofen 200 MG Oral Tablet", "pain"),
    ("617318", "Acetaminophen 500 MG Oral Tablet", "pain"),
    ("197361", "Hydrochlorothiazide 25 MG Oral Tablet", "hypertension"),
]

# Realistic vital sign + lab measurements
MEASUREMENTS = [
    ("8480-6",  "Systolic blood pressure",      (90, 180),  "mmHg",       "vitals_bp"),
    ("8462-4",  "Diastolic blood pressure",     (50, 110),  "mmHg",       "vitals_bp"),
    ("8867-4",  "Heart rate",                    (50, 120),  "bpm",        "vitals_hr"),
    ("8310-5",  "Body temperature",              (96.0, 101.5), "degF",    "vitals_temp"),
    ("29463-7", "Body weight",                   (100, 250), "lbs",        "vitals_body"),
    ("8302-2",  "Body height",                   (54, 80),   "in",         "vitals_body"),
    ("39156-5", "Body mass index (BMI)",         (18, 40),   "kg/m2",      "vitals_body"),
    ("2345-7",  "Glucose [Mass/volume] in Serum", (70, 200), "mg/dL",      "lab_metabolic"),
    ("4548-4",  "Hemoglobin A1c/Hemoglobin.total in Blood", (5.0, 9.5), "%", "lab_metabolic"),
    ("2093-3",  "Cholesterol [Mass/volume] in Serum or Plasma", (150, 260), "mg/dL", "lab_metabolic"),
    ("2160-0",  "Creatinine [Mass/volume] in Serum or Plasma", (0.6, 2.0), "mg/dL", "lab_renal"),
    ("718-7",   "Hemoglobin [Mass/volume] in Blood", (10, 17), "g/dL",      "lab_hematology"),
    ("3016-3",  "Thyroid stimulating hormone [Units/volume] in Serum or Plasma", (0.5, 5.0), "mIU/L", "lab_endocrine"),
]


def gen_patients(n: int) -> list[dict]:
    out = []
    for i in range(1, n + 1):
        birth_year = random.randint(1940, 2015)
        birth = date(birth_year, random.randint(1, 12), random.randint(1, 28))
        out.append({
            "patient_id": f"p{i:05d}",
            "birthdate": birth.isoformat(),
            "gender": random.choice(["M", "F"]),
        })
    return out


def gen_conditions(patients: list[dict], per_patient_avg: float = 4.0) -> list[dict]:
    out = []
    now = date(2026, 6, 1)
    for p in patients:
        n = max(0, int(random.gauss(per_patient_avg, 2)))
        for _ in range(n):
            code, desc = random.choice(CONDITIONS)
            days_ago = random.randint(0, 365 * 5)
            start = now - timedelta(days=days_ago)
            # Some conditions have an end date (resolved), some don't
            if random.random() < 0.6:
                stop = start + timedelta(days=random.randint(7, 90))
            else:
                stop = ""
            out.append({
                "patient_id": p["patient_id"],
                "start": start.isoformat(),
                "stop": stop.isoformat() if stop else "",
                "code": code,
                "description": desc,
            })
    return out


def gen_encounters(patients: list[dict], per_patient_avg: float = 3.0) -> list[dict]:
    out = []
    now = date(2026, 6, 1)
    enc_id = 1
    for p in patients:
        n = max(0, int(random.gauss(per_patient_avg, 2)))
        for _ in range(n):
            days_ago = random.randint(0, 365 * 3)
            start = now - timedelta(days=days_ago)
            enc_class = random.choice(ENCOUNTER_CLASSES)
            if enc_class == "inpatient":
                duration = random.randint(1, 7)
            elif enc_class == "emergency":
                duration = random.randint(0, 1)
            else:
                duration = 0  # outpatient / wellness same-day
            stop = start + timedelta(days=duration)
            out.append({
                "encounter_id": f"e{enc_id:07d}",
                "patient_id": p["patient_id"],
                "start": start.isoformat(),
                "stop": stop.isoformat(),
                "encounterclass": enc_class,
                "description": f"{enc_class.title()} visit",
            })
            enc_id += 1
    return out


def gen_medications(patients: list[dict], per_patient_avg: float = 4.0) -> list[dict]:
    """Generate prescription records per patient."""
    out = []
    now = date(2026, 6, 1)
    for p in patients:
        n = max(0, int(random.gauss(per_patient_avg, 2)))
        for _ in range(n):
            days_ago = random.randint(0, 365 * 2)
            start = now - timedelta(days=days_ago)
            # Some meds are short courses, others long-term
            if random.random() < 0.3:
                stop = start + timedelta(days=random.randint(7, 30))
            elif random.random() < 0.7:
                stop = start + timedelta(days=random.randint(60, 180))
            else:
                stop = start + timedelta(days=random.randint(180, 365))
            code, desc, indication = random.choice(MEDICATIONS)
            out.append({
                "patient_id": p["patient_id"],
                "start": start.isoformat(),
                "stop": stop.isoformat(),
                "code": code,
                "description": desc,
                "reasondescription": indication,
            })
    return out


def gen_observations(patients: list[dict], per_patient_avg: float = 12.0) -> list[dict]:
    """Generate vital sign + lab records per patient."""
    out = []
    now = date(2026, 6, 1)
    for p in patients:
        n = max(0, int(random.gauss(per_patient_avg, 4)))
        for _ in range(n):
            days_ago = random.randint(0, 365 * 3)
            d = now - timedelta(
                days=days_ago,
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
            )
            code, desc, (low, high), units, _category = random.choice(MEASUREMENTS)
            # Some measurements are integers (BP, HR), others floats (temp, BMI)
            if low == int(low) and high == int(high):
                value = float(random.randint(int(low), int(high)))
            else:
                value = round(random.uniform(low, high), 2)
            out.append({
                "patient_id": p["patient_id"],
                "date": d.isoformat(),
                "code": code,
                "description": desc,
                "value": str(value),
                "units": units,
            })
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--patients", type=int, default=500)
    ap.add_argument("--db", type=str, default=str(DUCKDB_PATH))
    args = ap.parse_args()

    import duckdb
    import pandas as pd

    patients = gen_patients(args.patients)
    conditions = gen_conditions(patients)
    encounters = gen_encounters(patients)
    medications = gen_medications(patients)
    observations = gen_observations(patients)

    df_patients = pd.DataFrame(patients)
    df_conditions = pd.DataFrame(conditions)
    df_encounters = pd.DataFrame(encounters)
    df_medications = pd.DataFrame(medications)
    df_observations = pd.DataFrame(observations)

    con = duckdb.connect(args.db)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw_dev")
    for tbl in ("patients", "conditions", "encounters", "medications", "observations"):
        con.execute(f"DROP TABLE IF EXISTS raw_dev.{tbl}")

    con.register("p_view", df_patients)
    con.register("c_view", df_conditions)
    con.register("e_view", df_encounters)
    con.register("m_view", df_medications)
    con.register("o_view", df_observations)
    con.execute("CREATE TABLE raw_dev.patients AS SELECT * FROM p_view")
    con.execute('CREATE TABLE raw_dev.conditions AS SELECT "patient_id","start","stop","code","description" FROM c_view')
    con.execute('CREATE TABLE raw_dev.encounters AS SELECT "encounter_id","patient_id","start","stop","encounterclass","description" FROM e_view')
    con.execute('CREATE TABLE raw_dev.medications AS SELECT "patient_id","start","stop","code","description","reasondescription" FROM m_view')
    con.execute('CREATE TABLE raw_dev.observations AS SELECT "patient_id","date","code","description","value","units" FROM o_view')
    con.unregister("p_view")
    con.unregister("c_view")
    con.unregister("e_view")
    con.unregister("m_view")
    con.unregister("o_view")

    # Also seed a small eligibility feed so the eligibility models build too
    import random as _r
    _r.seed(42)
    states = ['CA', 'TX', 'NY', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI']
    plan_types = ['HMO', 'PPO', 'EPO', 'HDHP', 'POS']
    relations = ['Self', 'Spouse', 'Child', 'Other']
    elig_rows = []
    for p in patients:
        elig_rows.append({
            "mem_id": p["patient_id"],
            "first_name": f"First{p['patient_id']}",
            "last_name": f"Last{p['patient_id']}",
            "dob": p["birthdate"],
            "email": f"{p['patient_id']}@example.com",
            "phone": f"555{p['patient_id'][-4:]}",
            "address": "123 Main St",
            "city": "Springfield",
            "state": _r.choice(states),
            "zip_code": f"{_r.randint(10000, 99999)}",
            "effective_date": (date(2025, 1, 1) - timedelta(days=_r.randint(0, 365))).isoformat(),
            "termination_date": "Active",
            "covered_relation": _r.choice(relations),
            "plan_type": _r.choice(plan_types),
            "metal_level": _r.choice(["Bronze", "Silver", "Gold", "Platinum"]),
            "hsa_eligible": _r.choice(["Yes", "No"]),
        })

    df_elig = pd.DataFrame(elig_rows)
    con.register("e_view_elig", df_elig)
    con.execute("CREATE TABLE raw_dev.eligibility AS SELECT * FROM e_view_elig")
    con.unregister("e_view_elig")

    print(f"Seeded {len(patients)} patients, {len(conditions)} conditions, {len(encounters)} encounters, {len(medications)} medications, {len(observations)} observations, {len(elig_rows)} eligibility rows → {args.db}")


if __name__ == "__main__":
    main()
