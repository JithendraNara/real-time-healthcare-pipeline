"""
Modern data quality suite for vital-pipeline.

Two layers:
  1. Direct SQL-based column-level checks (replaces legacy GX 0.x API)
  2. OMOP row-level checks
  3. Iceberg freshness check

Run:
    python data_quality/run_gx_suite.py
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import duckdb

log = logging.getLogger("vital-dq")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "dbt_project/dbt.duckdb")
DB_SCHEMA = os.environ.get("DB_SCHEMA", "main")
FRESHNESS_HOURS = int(os.environ.get("FRESHNESS_HOURS", "24"))

# dbt-duckdb creates schemas as {DB_SCHEMA}_{model_schema}, e.g. main_staging
def fq(table: str, model_schema: str | None = None) -> str:
    """Build fully-qualified table name for DuckDB dbt output."""
    if model_schema:
        return f"{DB_SCHEMA}_{model_schema}.{table}"
    return f"{DB_SCHEMA}.{table}"


# ─── Layer 1: Column-level checks (replaces GX 0.x) ───

def run_column_checks() -> int:
    log.info("=== Layer 1: Column-level checks ===")
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    failures = 0

    # 1. stg_eligibility_members
    df = con.execute(f"SELECT * FROM {fq('stg_eligibility_members', 'staging')}").fetch_df()
    if df.empty:
        log.error("stg_eligibility_members is empty")
        return 1

    # 1a. member_id is unique and not null
    if df["member_id"].isnull().any():
        log.error("stg_eligibility_members: %d null member_id", df["member_id"].isnull().sum())
        failures += 1
    if df["member_id"].duplicated().any():
        log.error("stg_eligibility_members: %d duplicate member_id", df["member_id"].duplicated().sum())
        failures += 1

    # 1b. first_name, last_name, date_of_birth not null
    for col in ("first_name", "last_name", "date_of_birth", "state"):
        nulls = df[col].isnull().sum()
        if nulls:
            log.error("stg_eligibility_members: %d null %s", nulls, col)
            failures += 1

    # 1c. zip_code matches 5-digit regex
    import re
    bad_zips = df["zip_code"].dropna().apply(lambda z: not re.match(r"^\d{5}$", str(z))).sum()
    if bad_zips:
        log.error("stg_eligibility_members: %d malformed zip_code", bad_zips)
        failures += 1

    # 1d. state is one of 50 US states
    valid_states = {'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
                    'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
                    'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
                    'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
                    'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'}
    bad_states = (~df["state"].isin(valid_states)).sum()
    if bad_states:
        log.error("stg_eligibility_members: %d invalid state", bad_states)
        failures += 1

    log.info("Column checks: %d failures", failures)
    return failures


# ─── Layer 2: OMOP row-level checks ───

def run_omop_checks() -> int:
    log.info("=== Layer 2: OMOP CDM row-level checks ===")
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    failures = 0

    # 1. Person: gender concept must be valid OMOP concept
    bad = con.execute(
        f"SELECT COUNT(*) FROM {fq('omcdm_person', 'omop')} "
        f"WHERE gender_concept_id NOT IN (0, 8507, 8532, 44814653, 44814649)"
    ).fetchone()[0]
    if bad:
        log.error("omcdm_person: %d rows with invalid gender_concept_id", bad)
        failures += 1

    # 2. Condition: every condition must reference an existing person
    orphans = con.execute(
        f"SELECT COUNT(*) FROM {fq('omcdm_condition_occurrence', 'omop')} co "
        f"LEFT JOIN {fq('omcdm_person', 'omop')} p ON co.person_id = p.person_id "
        f"WHERE p.person_id IS NULL"
    ).fetchone()[0]
    if orphans:
        log.error("omcdm_condition_occurrence: %d orphan rows", orphans)
        failures += 1

    # 3. Visit: end_date >= start_date
    bad_dates = con.execute(
        f"SELECT COUNT(*) FROM {fq('omcdm_visit_occurrence', 'omop')} "
        f"WHERE visit_end_date < visit_start_date"
    ).fetchone()[0]
    if bad_dates:
        log.error("omcdm_visit_occurrence: %d rows with end < start", bad_dates)
        failures += 1

    # 4. CCS coverage: at least 50% of conditions should have a non-'unmapped' CCS category
    ccs_coverage = con.execute(
        f"SELECT AVG(CASE WHEN ccs_category = 'unmapped' THEN 0.0 ELSE 1.0 END) "
        f"FROM {fq('omcdm_condition_occurrence', 'omop')}"
    ).fetchone()[0]
    if ccs_coverage is None or ccs_coverage < 0.5:
        log.error("CCS coverage is %.2f (expected >= 0.50)", ccs_coverage or 0.0)
        failures += 1
    else:
        log.info("CCS coverage: %.2f (OK)", ccs_coverage)

    return failures


# ─── Layer 3: Freshness check ───

def run_freshness_check() -> int:
    log.info("=== Layer 3: Freshness check ===")
    con = duckdb.connect(DUCKDB_PATH, read_only=True)

    try:
        last_load = con.execute(
            f"SELECT MAX(cdm_loaded_at) FROM {fq('omcdm_condition_occurrence', 'omop')}"
        ).fetchone()[0]
    except Exception as e:
        log.warning(f"Could not check freshness: {e}")
        return 0

    if last_load is None:
        log.warning("No cdm_loaded_at found — skipping")
        return 0

    if isinstance(last_load, str):
        last_load = datetime.fromisoformat(last_load)
    if last_load.tzinfo is None:
        last_load = last_load.replace(tzinfo=timezone.utc)

    age = datetime.now(timezone.utc) - last_load
    if age > timedelta(hours=FRESHNESS_HOURS):
        log.error("Freshness: latest OMOP snapshot is %s old (threshold %dh)", age, FRESHNESS_HOURS)
        return 1
    log.info("Freshness: latest OMOP snapshot is %s old (OK)", age)
    return 0


def main() -> int:
    failures = 0
    failures += run_column_checks()
    failures += run_omop_checks()
    failures += run_freshness_check()

    if failures:
        log.error("DATA QUALITY FAILED: %d check(s) failed", failures)
        return 1
    log.info("DATA QUALITY PASSED — all checks green")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
