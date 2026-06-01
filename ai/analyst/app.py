"""
Healthcare AI Analyst — natural-language interface to OMOP CDM.

Endpoints:
  POST /ask    {"question": "..."}  → NL → SQL → result + clinical answer
  POST /plan   {"goal": "..."}      → multi-step clinical investigation
  GET  /cohort {"filters": {...}}   → SQL-defined patient cohort
  GET  /health

Stack:
  FastAPI + MiniMax-M2 + DuckDB (local) / Trino+Athena (prod)
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Dict, List, Optional

import duckdb
import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

log = logging.getLogger("vital-analyst")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))

app = FastAPI(
    title="Vital Pipeline AI Analyst",
    description="Natural-language interface to the OMOP CDM.",
    version="0.2.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "dbt_project/dbt.duckdb")
DB_SCHEMA = os.environ.get("DB_SCHEMA", "main")
MINIMAX_API_KEY = os.environ.get("MINIMAX_API_KEY", "")
MINIMAX_BASE_URL = os.environ.get("MINIMAX_BASE_URL", "https://api.minimax.chat/v1")


# --- LLM client (MiniMax) ---

class MiniMaxClient:
    def __init__(self, api_key: str = MINIMAX_API_KEY, model: str = "MiniMax-M2"):
        self.api_key = api_key
        self.model = model

    def chat(self, system: str, user: str, temperature: float = 0.1, response_format_json: bool = False) -> str:
        if not self.api_key:
            return self._fallback(user)
        body: Dict[str, Any] = {
            "model": self.model,
            "temperature": temperature,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        }
        if response_format_json:
            body["response_format"] = {"type": "json_object"}
        r = requests.post(
            f"{MINIMAX_BASE_URL}/chat/completions",
            headers={"Authorization": f"Bearer {self.api_key}"},
            json=body,
            timeout=60,
        )
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]

    def _fallback(self, user: str) -> str:
        return f"[local fallback] {user[:300]}"


# --- SQL generation ---

OMOP_SCHEMA_DESCRIPTION = """
You are a SQL generator for an OMOP CDM v5.4 dataset in DuckDB.

Available tables (dbt-duckdb schema prefix is `{schema}_<model_schema>`):
- {schema}_omop.omcdm_person (person_id BIGINT PK, person_source_value, gender_concept_id, birth_datetime, year_of_birth, race_concept_id, ethnicity_concept_id)
- {schema}_omop.omcdm_condition_occurrence (condition_occurrence_id BIGINT PK, person_id FK, condition_concept_id, condition_start_date, condition_end_date, ccs_category, condition_source_value)
- {schema}_omop.omcdm_visit_occurrence (visit_occurrence_id BIGINT PK, person_id FK, visit_concept_id, visit_start_date, visit_end_date, visit_source_label)
- {schema}_omop.omcdm_drug_exposure (drug_exposure_id BIGINT PK, person_id FK, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date, drug_source_value, drug_source_label, drug_code_type, reason_description)
- {schema}_omop.omcdm_measurement (measurement_id BIGINT PK, person_id FK, measurement_concept_id, measurement_date, measurement_datetime, value_as_number, unit_source_value, measurement_category, measurement_source_value, measurement_source_label)
- {schema}_marts.mart_member_roster (member_id, age_bucket, plan_type, enrollment_status, missing_zip, missing_email, child_overage_flag, days_enrolled, state)
- {schema}_marts.mart_medication_adherence (person_id, drug_source_value, drug_source_label, fill_count, first_fill_date, last_fill_end_date, total_days_on_hand, observation_window_days, pdc_score, adherence_category)
- {schema}_marts.mart_condition_drug_pairs (person_id, condition_source_value, condition_ccs_category, drug_source_value, drug_source_label, first_condition_onset, first_drug_for_condition, days_from_onset_to_drug, drug_fills_for_condition, treatment_lag_bucket)
- {schema}_intermediate.int_member_months (member_id, age, plan_type, state, coverage_month, member_months, is_primary)

ccs_category values include: infectious_disease, neoplasms, blood_disease, endocrine, mental_health, nervous_system, eye_disorder, ear_disorder, circulatory, respiratory, digestive, skin, musculoskeletal, genitourinary, pregnancy, perinatal, congenital, symptoms_signs, injury, external_cause, health_services, unmapped.

measurement_category values include: vitals_bp, vitals_hr, vitals_temp, vitals_body, lab_metabolic, lab_renal, lab_hematology, lab_endocrine, other.

adherence_category values include: adherent (pdc>=0.80), partially_adherent (0.60<=pdc<0.80), non_adherent (pdc<0.60).

treatment_lag_bucket values include: same_day, within_week, within_month, within_quarter, after_quarter.

Rules:
- Use DuckDB SQL syntax (date_trunc, datediff, list, struct, etc.)
- ALWAYS include the full schema prefix in table references (e.g. `{schema}_omop.omcdm_person`)
- Cast dates using `::DATE` and timestamps using `::TIMESTAMP`
- For cohort queries, use a CTE and return person_id + the metric
- Use regexp_matches(string, pattern) for regex matching in DuckDB (NOT the `~` operator)
- Return only SQL — no prose, no markdown fences.
""".strip()


def generate_sql(question: str) -> str:
    llm = MiniMaxClient()
    prompt = OMOP_SCHEMA_DESCRIPTION.format(schema=DB_SCHEMA)
    raw = llm.chat(prompt, question, temperature=0.1).strip().strip("`")
    if raw.lower().startswith("sql\n"):
        raw = raw[4:]
    return raw


def run_query(sql: str) -> List[Dict[str, Any]]:
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        df = con.execute(sql).fetch_df()
        return json.loads(df.to_json(orient="records"))
    except Exception as e:
        log.error("Query failed: %s | SQL=%s", e, sql)
        raise


# --- API models ---

class AskRequest(BaseModel):
    question: str = Field(..., description="Natural-language question about patient cohorts or care patterns.")


class AskResponse(BaseModel):
    question: str
    sql: str
    rows: List[Dict[str, Any]]
    answer: str
    row_count: int


class PlanRequest(BaseModel):
    goal: str = Field(..., description="High-level clinical or operational investigation.")


class StepResult(BaseModel):
    question: str
    sql: Optional[str] = None
    rows: Optional[List[Dict[str, Any]]] = None
    answer: Optional[str] = None
    error: Optional[str] = None
    why: Optional[str] = None


class PlanResponse(BaseModel):
    goal: str
    steps: List[StepResult]


class CohortRequest(BaseModel):
    filters: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Cohort filters. Supported keys: min_age, max_age, gender_concept_id, "
            "ccs_categories, drug_classes, measurement_categories, adherence_categories, "
            "min_pdc, min_visits, min_drugs, min_measurements, state."
        ),
    )


class CohortResponse(BaseModel):
    cohort_size: int
    sql: str
    sample: List[Dict[str, Any]]


# --- Endpoints ---

@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/schema")
def schema() -> Dict[str, Any]:
    return {
        "schema": DB_SCHEMA,
        "tables": [
            "omcdm_person",
            "omcdm_condition_occurrence",
            "omcdm_visit_occurrence",
            "omcdm_drug_exposure",
            "omcdm_measurement",
            "mart_member_roster",
            "mart_medication_adherence",
            "mart_condition_drug_pairs",
            "int_member_months",
        ],
    }


@app.post("/ask", response_model=AskResponse)
def ask(req: AskRequest) -> AskResponse:
    sql = generate_sql(req.question)
    log.info("Generated SQL: %s", sql)
    rows = run_query(sql)
    summary = MiniMaxClient().chat(
        "You are a clinical data analyst. Given the question, SQL, and result rows, write a 2-4 sentence answer in plain English. Be specific and cite numbers. Note any data quality caveats (small sample, etc.).",
        f"Question: {req.question}\nSQL: {sql}\nRows (first 50): {rows[:50]}",
    )
    return AskResponse(
        question=req.question,
        sql=sql,
        rows=rows,
        answer=summary,
        row_count=len(rows),
    )


@app.post("/plan", response_model=PlanResponse)
def plan(req: PlanRequest) -> PlanResponse:
    llm = MiniMaxClient()
    plan_json = llm.chat(
        "You are a clinical data analyst. Given a high-level clinical or operational goal, return a JSON array of 3-6 concrete sub-questions that together would answer the goal. Each entry must have: {\"question\": \"...\", \"why\": \"...\"}. Return ONLY valid JSON.",
        req.goal,
        response_format_json=True,
    )
    try:
        steps = json.loads(plan_json)
    except json.JSONDecodeError:
        steps = [{"question": req.goal, "why": "Single-step fallback."}]

    executed: List[StepResult] = []
    for s in steps:
        q = s.get("question", "").strip()
        if not q:
            continue
        try:
            res = ask(AskRequest(question=q))
            executed.append(StepResult(
                question=q, sql=res.sql, rows=res.rows[:30], answer=res.answer,
            ))
        except Exception as e:
            executed.append(StepResult(question=q, error=str(e), why=s.get("why")))

    return PlanResponse(goal=req.goal, steps=executed)


@app.post("/cohort", response_model=CohortResponse)
def cohort(req: CohortRequest) -> CohortResponse:
    """Build a SQL-defined patient cohort from filters."""
    f = req.filters
    # dbt-duckdb schemas are {DB_SCHEMA}_{model_schema}
    s_person = f"{DB_SCHEMA}_omop.omcdm_person"
    s_cond = f"{DB_SCHEMA}_omop.omcdm_condition_occurrence"
    s_visit = f"{DB_SCHEMA}_omop.omcdm_visit_occurrence"
    s_drug = f"{DB_SCHEMA}_omop.omcdm_drug_exposure"
    s_meas = f"{DB_SCHEMA}_omop.omcdm_measurement"
    s_roster = f"{DB_SCHEMA}_marts.mart_member_roster"
    s_adherence = f"{DB_SCHEMA}_marts.mart_medication_adherence"

    where_clauses = ["1=1"]
    if f.get("min_age") is not None:
        where_clauses.append(f"({s_person}.year_of_birth <= EXTRACT(YEAR FROM CURRENT_DATE) - {int(f['min_age'])})")
    if f.get("max_age") is not None:
        where_clauses.append(f"({s_person}.year_of_birth >= EXTRACT(YEAR FROM CURRENT_DATE) - {int(f['max_age'])})")
    if f.get("gender_concept_id") is not None:
        where_clauses.append(f"{s_person}.gender_concept_id = {int(f['gender_concept_id'])}")
    if f.get("ccs_categories"):
        cats = ", ".join(f"'{c}'" for c in f["ccs_categories"])
        where_clauses.append(
            f"EXISTS (SELECT 1 FROM {s_cond} co "
            f"WHERE co.person_id = {s_person}.person_id AND co.ccs_category IN ({cats}))"
        )
    if f.get("drug_classes"):
        dcs = ", ".join(f"'{c}'" for c in f["drug_classes"])
        where_clauses.append(
            f"EXISTS (SELECT 1 FROM {s_drug} d "
            f"WHERE d.person_id = {s_person}.person_id AND d.drug_code_type IN ({dcs}))"
        )
    if f.get("measurement_categories"):
        mcs = ", ".join(f"'{c}'" for c in f["measurement_categories"])
        where_clauses.append(
            f"EXISTS (SELECT 1 FROM {s_meas} m "
            f"WHERE m.person_id = {s_person}.person_id AND m.measurement_category IN ({mcs}))"
        )
    if f.get("adherence_categories"):
        acs = ", ".join(f"'{c}'" for c in f["adherence_categories"])
        where_clauses.append(
            f"EXISTS (SELECT 1 FROM {s_adherence} ad "
            f"WHERE ad.person_id = {s_person}.person_id AND ad.adherence_category IN ({acs}))"
        )
    if f.get("min_pdc") is not None:
        where_clauses.append(
            f"EXISTS (SELECT 1 FROM {s_adherence} ad "
            f"WHERE ad.person_id = {s_person}.person_id AND ad.pdc_score >= {float(f['min_pdc'])})"
        )
    if f.get("min_visits") is not None:
        where_clauses.append(
            f"(SELECT COUNT(*) FROM {s_visit} v "
            f"WHERE v.person_id = {s_person}.person_id) >= {int(f['min_visits'])}"
        )
    if f.get("min_drugs") is not None:
        where_clauses.append(
            f"(SELECT COUNT(*) FROM {s_drug} d "
            f"WHERE d.person_id = {s_person}.person_id) >= {int(f['min_drugs'])}"
        )
    if f.get("min_measurements") is not None:
        where_clauses.append(
            f"(SELECT COUNT(*) FROM {s_meas} m "
            f"WHERE m.person_id = {s_person}.person_id) >= {int(f['min_measurements'])}"
        )
    if f.get("state"):
        states = ", ".join(f"'{s}'" for s in f["state"])
        where_clauses.append(
            f"EXISTS (SELECT 1 FROM {s_roster} m "
            f"WHERE m.member_id = {s_person}.person_source_value AND m.state IN ({states}))"
        )

    sql = f"""
    SELECT person_id, year_of_birth, gender_concept_id
    FROM {s_person}
    WHERE {' AND '.join(where_clauses)}
    LIMIT 1000
    """
    rows = run_query(sql)
    full_sql = f"""
    SELECT COUNT(DISTINCT person_id) AS cohort_size
    FROM {s_person}
    WHERE {' AND '.join(where_clauses)}
    """
    size_rows = run_query(full_sql)
    size = size_rows[0].get("cohort_size", 0) if size_rows else 0
    return CohortResponse(cohort_size=int(size), sql=sql, sample=rows[:50])


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
