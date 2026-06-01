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

    df_patients = pd.DataFrame(patients)
    df_conditions = pd.DataFrame(conditions)
    df_encounters = pd.DataFrame(encounters)

    con = duckdb.connect(args.db)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw_dev")
    for tbl in ("patients", "conditions", "encounters"):
        con.execute(f"DROP TABLE IF EXISTS raw_dev.{tbl}")

    con.register("p_view", df_patients)
    con.register("c_view", df_conditions)
    con.register("e_view", df_encounters)
    con.execute("CREATE TABLE raw_dev.patients AS SELECT * FROM p_view")
    con.execute('CREATE TABLE raw_dev.conditions AS SELECT "patient_id","start","stop","code","description" FROM c_view')
    con.execute('CREATE TABLE raw_dev.encounters AS SELECT "encounter_id","patient_id","start","stop","encounterclass","description" FROM e_view')
    con.unregister("p_view")
    con.unregister("c_view")
    con.unregister("e_view")

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

    print(f"Seeded {len(patients)} patients, {len(conditions)} conditions, {len(encounters)} encounters, {len(elig_rows)} eligibility rows → {args.db}")


if __name__ == "__main__":
    main()
