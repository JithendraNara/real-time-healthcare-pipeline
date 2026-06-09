"""
Clinical Dashboard — Streamlit app for the real-time healthcare pipeline.

Reads from:
  - Kafka topic healthcare.predictions (live risk scores)
  - OMOP DuckDB (patient demographics, conditions, prior visits)

Three views:
  1. Live Risk Board — at-a-glance list of high-risk patients, refreshes every 5s
  2. Patient Detail — deep-dive on one patient: vitals history, risk score, SHAP features
  3. Pipeline Health — Kafka topic stats, OMOP row counts, MLflow model version

Run:
    streamlit run app/dashboard/clinical_dashboard.py --server.port 8501

Env (all optional, sensible defaults for local dev):
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092
    OMOP_DUCKDB=dbt_project/dbt.duckdb
    SILVER_DUCKDB=streaming/warehouse/silver.db
    MLFLOW_TRACKING_URI=sqlite:///mlflow.db
    PREDICTIONS_BUFFER_SIZE=200   # how many recent predictions to keep in memory
"""
from __future__ import annotations

import json
import os
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Real-Time Healthcare — Clinical Dashboard",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ---------------------------------------------------------------------------
# Background Kafka consumer — keeps a rolling buffer of recent predictions
# ---------------------------------------------------------------------------


@st.cache_resource
def get_prediction_buffer(max_size: int = 200) -> deque:
    return deque(maxlen=max_size)


@st.cache_resource
def start_kafka_consumer(bootstrap: str, topic: str) -> dict[str, Any]:
    """Start a background thread that consumes from healthcare.predictions
    and pushes events into the shared buffer. Returns a status dict."""
    from confluent_kafka import Consumer, KafkaError

    buf: deque = get_prediction_buffer()
    state = {"running": True, "last_message": None, "messages_consumed": 0, "errors": 0}

    def _loop():
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"clinical-dashboard-{os.getpid()}",
                "auto.offset.reset": "latest",  # only show new predictions
                "enable.auto.commit": True,
            }
        )
        consumer.subscribe([topic])
        while state["running"]:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                state["errors"] += 1
                continue
            try:
                payload = json.loads(msg.value())
                buf.append(payload)
                state["last_message"] = datetime.now(timezone.utc).isoformat()
                state["messages_consumed"] += 1
            except Exception:
                state["errors"] += 1
        consumer.close()

    t = threading.Thread(target=_loop, daemon=True, name="dashboard-consumer")
    t.start()
    return state


# ---------------------------------------------------------------------------
# Data accessors
# ---------------------------------------------------------------------------


@st.cache_data(ttl=10)
def get_pipeline_stats(omop_duckdb: str | None, silver_duckdb: str | None) -> dict[str, Any]:
    """Quick row counts + freshness for the pipeline health view."""
    out: dict[str, Any] = {
        "omop": {"person": 0, "visit": 0, "condition": 0, "drug": 0},
        "silver": {"vitals": 0, "admissions": 0, "labs": 0, "iot": 0, "dlq": 0},
        "mlflow": {"model_version": "unknown", "last_trained": "unknown"},
    }
    if omop_duckdb and Path(omop_duckdb).exists():
        try:
            import duckdb
            con = duckdb.connect(omop_duckdb, read_only=True)
            for t, k in [("omcdm_person", "person"), ("omcdm_visit_occurrence", "visit"),
                         ("omcdm_condition_occurrence", "condition"), ("omcdm_drug_exposure", "drug")]:
                try:
                    out["omop"][k] = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
                except Exception:
                    pass
            con.close()
        except Exception as e:  # noqa: BLE001
            out["omop"]["error"] = str(e)
    if silver_duckdb and Path(silver_duckdb).exists():
        try:
            import duckdb
            con = duckdb.connect(silver_duckdb, read_only=True)
            for t, k in [("vitals_silver", "vitals"), ("admissions_silver", "admissions"),
                         ("labs_silver", "labs"), ("iot_telemetry_silver", "iot"),
                         ("dlq_silver", "dlq")]:
                try:
                    out["silver"][k] = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
                except Exception:
                    pass
            con.close()
        except Exception as e:  # noqa: BLE001
            out["silver"]["error"] = str(e)
    # MLflow
    try:
        import mlflow
        tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "sqlite:///mlflow.db")
        mlflow.set_tracking_uri(tracking_uri)
        client = mlflow.tracking.MlflowClient(tracking_uri=tracking_uri)
        try:
            prod = client.get_latest_versions("readmission_30d", stages=["Production"])
            if prod:
                out["mlflow"]["model_version"] = f"v{prod[0].version}"
        except Exception:
            try:
                # MLflow 3.x: use aliases
                mv = client.get_model_version_by_alias("readmission_30d", "production")
                out["mlflow"]["model_version"] = f"v{mv.version} (alias)"
            except Exception:
                pass
    except Exception as e:  # noqa: BLE001
        out["mlflow"]["error"] = str(e)
    return out


def get_predictions_df(buffer: deque) -> pd.DataFrame:
    """Convert the rolling buffer of predictions to a DataFrame."""
    if not buffer:
        return pd.DataFrame()
    rows = []
    for ev in buffer:
        rows.append(
            {
                "patient_id": ev.get("patient_id"),
                "score": float(ev.get("score", 0.0)),
                "model_id": ev.get("model_id", ""),
                "model_version": ev.get("model_version", ""),
                "prediction_type": ev.get("prediction_type", ""),
                "scored_at": ev.get("event_time") or ev.get("ingestion_time"),
                "top_feature_contributions": json.dumps(ev.get("top_feature_contributions", {})),
            }
        )
    return pd.DataFrame(rows)


def get_patient_detail(patient_id: str, omop_duckdb: str | None, silver_duckdb: str | None) -> dict[str, Any]:
    """Fetch everything we know about one patient: demographics, conditions, recent vitals."""
    out: dict[str, Any] = {"patient_id": patient_id, "demographics": {}, "conditions": [], "visits": [], "recent_vitals": []}
    if not omop_duckdb or not Path(omop_duckdb).exists():
        return out
    try:
        import duckdb
        con = duckdb.connect(omop_duckdb, read_only=True)
        # Try int first, then str
        try:
            pid_int = int(patient_id)
            row = con.execute(
                "SELECT person_id, gender_concept_id, year_of_birth, race_concept_id FROM omcdm_person WHERE person_id = ?",
                [pid_int],
            ).fetchone()
        except (ValueError, TypeError):
            row = con.execute(
                "SELECT person_id, gender_concept_id, year_of_birth, race_concept_id FROM omcdm_person WHERE CAST(person_id AS VARCHAR) = ?",
                [patient_id],
            ).fetchone()
        if row:
            out["demographics"] = {
                "person_id": row[0],
                "gender_concept_id": row[1],
                "year_of_birth": row[2],
                "race_concept_id": row[3],
                "age_2026": 2026 - (row[2] or 1950),
            }
            out["conditions"] = con.execute(
                """
                SELECT condition_concept_id, condition_start_datetime
                FROM omcdm_condition_occurrence
                WHERE person_id = ?
                ORDER BY condition_start_datetime DESC
                LIMIT 20
                """,
                [row[0]],
            ).fetchall()
            out["visits"] = con.execute(
                """
                SELECT visit_occurrence_id, visit_start_datetime, visit_end_datetime, visit_concept_id
                FROM omcdm_visit_occurrence
                WHERE person_id = ?
                ORDER BY visit_start_datetime DESC
                LIMIT 10
                """,
                [row[0]],
            ).fetchall()
        con.close()
    except Exception as e:  # noqa: BLE001
        out["error"] = str(e)
    if silver_duckdb and Path(silver_duckdb).exists():
        try:
            import duckdb
            con = duckdb.connect(silver_duckdb, read_only=True)
            out["recent_vitals"] = con.execute(
                """
                SELECT
                  json_extract_string(payload, '$.event_time') as event_time,
                  json_extract_string(payload, '$.heart_rate_bpm') as hr,
                  json_extract_string(payload, '$.spo2_pct') as spo2,
                  json_extract_string(payload, '$.systolic_bp_mmHg') as sbp,
                  json_extract_string(payload, '$.source') as source
                FROM vitals_silver
                WHERE json_extract_string(payload, '$.patient_id') = ?
                ORDER BY json_extract_string(payload, '$.event_time') DESC
                LIMIT 20
                """,
                [str(patient_id)],
            ).fetchall()
            con.close()
        except Exception:
            pass
    return out


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PREDICTIONS_TOPIC = os.getenv("PREDICTIONS_TOPIC", "healthcare.predictions")
OMOP_DUCKDB = os.getenv("OMOP_DUCKDB", "dbt_project/dbt.duckdb")
SILVER_DUCKDB = os.getenv("SILVER_DUCKDB", "streaming/warehouse/silver.db")

st.title("🏥 Real-Time Healthcare — Clinical Dashboard")
st.caption("Module 4 — Streamlit UI over Kafka + OMOP + MLflow + Audit")

# Sidebar
with st.sidebar:
    st.header("Navigation")
    view = st.radio("View", ["Live Risk Board", "Patient Detail", "Pipeline Health"], label_visibility="collapsed")
    st.divider()
    st.subheader("Connection")
    st.code(f"broker: {KAFKA_BOOTSTRAP}\ntopic: {PREDICTIONS_TOPIC}\nomop: {OMOP_DUCKDB}\nsilver: {SILVER_DUCKDB}")
    st.divider()
    st.subheader("Refresh")
    auto_refresh = st.toggle("Auto-refresh (5s)", value=True)
    if auto_refresh:
        time.sleep(5)
        st.rerun()

# Kick off the background consumer once
state = start_kafka_consumer(KAFKA_BOOTSTRAP, PREDICTIONS_TOPIC)
buf = get_prediction_buffer()

if view == "Live Risk Board":
    st.subheader("Live Risk Board")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Recent predictions", state["messages_consumed"])
    c2.metric("In buffer", len(buf))
    c3.metric("Last message", state["last_message"][:19] if state["last_message"] else "—")
    c4.metric("Consumer errors", state["errors"])

    df = get_predictions_df(buf)
    if df.empty:
        st.info("No predictions in the buffer yet. Start the ML scorer (`python ml/realtime/scorer.py`) to see live risk scores appear here.")
    else:
        # Risk band
        df["risk_band"] = pd.cut(df["score"], bins=[-0.01, 0.10, 0.30, 1.01], labels=["low", "medium", "high"])
        st.dataframe(
            df[["patient_id", "score", "risk_band", "model_version", "scored_at"]].sort_values("score", ascending=False),
            use_container_width=True,
            height=400,
        )

        # Distribution chart
        st.subheader("Risk score distribution")
        try:
            import plotly.express as px
            fig = px.histogram(df, x="score", nbins=20, color="risk_band",
                               color_discrete_map={"low": "#2ecc71", "medium": "#f39c12", "high": "#e74c3c"})
            fig.update_layout(showlegend=True, height=300)
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:  # noqa: BLE001
            st.warning(f"Plotly unavailable: {e}")

        # High-risk patients only
        high = df[df["risk_band"] == "high"].sort_values("score", ascending=False)
        st.subheader(f"High-risk patients ({len(high)})")
        if not high.empty:
            for _, row in high.iterrows():
                with st.expander(f"🚨 {row['patient_id']} — score {row['score']:.2f} — {row['scored_at'][:19]}"):
                    try:
                        contribs = json.loads(row["top_feature_contributions"])
                        contribs_df = pd.DataFrame(
                            [(k, v) for k, v in sorted(contribs.items(), key=lambda kv: abs(kv[1]), reverse=True)[:5]],
                            columns=["feature", "shap_contribution"],
                        )
                        st.dataframe(contribs_df, use_container_width=True, hide_index=True)
                    except Exception:
                        st.write(row["top_feature_contributions"])

elif view == "Patient Detail":
    st.subheader("Patient Detail")
    df = get_predictions_df(buf)
    if df.empty:
        st.info("No predictions yet — pick a patient_id manually below.")
        default_pid = "1"
    else:
        default_pid = df["patient_id"].iloc[0]
    pid = st.text_input("Patient ID", value=default_pid)
    if pid:
        detail = get_patient_detail(pid, OMOP_DUCKDB, SILVER_DUCKDB)
        if not detail.get("demographics"):
            st.warning(f"Patient {pid} not found in OMOP. Check the ID or seed the warehouse first.")
        else:
            demo = detail["demographics"]
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Person ID", demo["person_id"])
            c2.metric("Age (2026)", demo.get("age_2026", "—"))
            c3.metric("Gender concept", demo.get("gender_concept_id", "—"))
            c4.metric("Race concept", demo.get("race_concept_id", "—"))

            # Latest prediction for this patient
            this_df = df[df["patient_id"] == str(pid)]
            if not this_df.empty:
                latest = this_df.sort_values("scored_at", ascending=False).iloc[0]
                st.subheader(f"Latest readmission risk: {latest['score']:.2f} ({latest['risk_band']})")
                st.caption(f"Model: {latest['model_id']} v{latest['model_version']} — scored {latest['scored_at'][:19]}")

            c5, c6 = st.columns(2)
            with c5:
                st.subheader("Conditions")
                if detail["conditions"]:
                    st.dataframe(pd.DataFrame(detail["conditions"], columns=["concept_id", "start"]), use_container_width=True, hide_index=True)
                else:
                    st.caption("No conditions on record.")
                st.subheader("Recent visits")
                if detail["visits"]:
                    st.dataframe(pd.DataFrame(detail["visits"], columns=["visit_id", "start", "end", "concept_id"]), use_container_width=True, hide_index=True)
                else:
                    st.caption("No visits on record.")
            with c6:
                st.subheader("Recent vitals (streaming)")
                if detail["recent_vitals"]:
                    st.dataframe(pd.DataFrame(detail["recent_vitals"], columns=["time", "HR", "SpO2", "SBP", "source"]),
                                 use_container_width=True, hide_index=True)
                else:
                    st.caption("No streaming vitals yet. Run the IoT simulator + streaming consumer first.")

elif view == "Pipeline Health":
    st.subheader("Pipeline Health")
    stats = get_pipeline_stats(OMOP_DUCKDB, SILVER_DUCKDB)
    c1, c2, c3 = st.columns(3)
    c1.metric("MLflow model", stats["mlflow"]["model_version"])
    c2.metric("Kafka broker", KAFKA_BOOTSTRAP)
    c3.metric("Consumer state", f"running ({state['messages_consumed']} msgs)")

    st.subheader("OMOP warehouse")
    cols = st.columns(4)
    for i, k in enumerate(["person", "visit", "condition", "drug"]):
        cols[i].metric(k.capitalize(), stats["omop"][k])

    st.subheader("Streaming silver (Module 1)")
    cols = st.columns(5)
    for i, k in enumerate(["vitals", "admissions", "labs", "iot", "dlq"]):
        cols[i].metric(k.capitalize(), stats["silver"][k])

    st.caption(f"Last Kafka message: {state['last_message'] or '—'}")
    st.caption(f"Consumer errors: {state['errors']}")

    if stats["mlflow"].get("error"):
        st.warning(f"MLflow unavailable: {stats['mlflow']['error']}")
