"""
Smoke tests for the clinical dashboard.

The Streamlit app itself is hard to test in a headless pytest environment, so
these tests focus on the helper functions and data accessors that the app
uses. The end-to-end behavior is verified by running the app manually.
"""
from __future__ import annotations

import json
import sys
import tempfile
import types
from collections import deque
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# Mock streamlit before importing the dashboard module
# ---------------------------------------------------------------------------


class _MockContext:
    def __enter__(self): return self
    def __exit__(self, *a): return False


class _SidebarCtx:
    """`with st.sidebar:` — sidebar is a context manager.

    Real streamlit: `st.sidebar` returns a DeltaGenerator, and `with st.sidebar:`
    creates a sidebar container. Here we return a singleton instance whose
    methods proxy to the mock streamlit statics.
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __enter__(self): return self
    def __exit__(self, *a): return False

    def __getattr__(self, name):
        return getattr(_MockStreamlit, name)


class _MockStreamlit:
    @staticmethod
    def cache_resource(fn=None, **kwargs):
        if fn is None:
            return lambda f: f
        return fn

    @staticmethod
    def cache_data(**kwargs):
        def deco(fn):
            return fn
        return deco

    @staticmethod
    def set_page_config(**kwargs): pass

    @staticmethod
    def code(*a, **kw): pass

    @staticmethod
    def info(*a, **kw): pass

    @staticmethod
    def warning(*a, **kw): pass

    @staticmethod
    def error(*a, **kw): pass

    @staticmethod
    def success(*a, **kw): pass

    @staticmethod
    def metric(*a, **kw): pass

    @staticmethod
    def subheader(*a, **kw): pass

    @staticmethod
    def header(*a, **kw): pass

    @staticmethod
    def caption(*a, **kw): pass

    @staticmethod
    def title(*a, **kw): pass

    @staticmethod
    def dataframe(*a, **kw): pass

    @staticmethod
    def columns(n):
        return [_MockStreamlit() for _ in range(n)]

    @staticmethod
    def plotly_chart(*a, **kw): pass

    @staticmethod
    def toggle(*a, **kw): return False

    @staticmethod
    def radio(*a, **kw): return "Live Risk Board"

    @staticmethod
    def text_input(*a, **kw): return "1"

    @staticmethod
    def expander(*a, **kw): return _MockContext()

    @staticmethod
    def spinner(*a, **kw): return _MockContext()

    @staticmethod
    def empty(*a, **kw): pass

    @staticmethod
    def sidebar(): return _SidebarCtx()

    @staticmethod
    def div(*a, **kw): pass

    @staticmethod
    def divider(*a, **kw): pass

    @staticmethod
    def rerun(*a, **kw): pass

    @staticmethod
    def pyplot(*a, **kw): pass

    @staticmethod
    def markdown(*a, **kw): pass


streamlit = types.ModuleType("streamlit")
for name in dir(_MockStreamlit):
    if not name.startswith("__"):
        setattr(streamlit, name, getattr(_MockStreamlit, name))
# Special-case: `st.sidebar` is a context manager in real streamlit
streamlit.sidebar = _SidebarCtx()
sys.modules["streamlit"] = streamlit


from app.dashboard.clinical_dashboard import (  # noqa: E402
    get_patient_detail,
    get_pipeline_stats,
    get_predictions_df,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def small_omop_db():
    """Build a minimal OMOP DuckDB for testing."""
    import duckdb
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "omop.duckdb"
        con = duckdb.connect(str(path))
        con.execute("""
            CREATE TABLE omcdm_person (person_id INTEGER PRIMARY KEY, gender_concept_id INTEGER, year_of_birth INTEGER, race_concept_id INTEGER);
            CREATE TABLE omcdm_visit_occurrence (visit_occurrence_id INTEGER PRIMARY KEY, person_id INTEGER, visit_start_datetime TIMESTAMP, visit_end_datetime TIMESTAMP, visit_concept_id INTEGER);
            CREATE TABLE omcdm_condition_occurrence (condition_occurrence_id INTEGER PRIMARY KEY, person_id INTEGER, condition_concept_id INTEGER, condition_start_datetime TIMESTAMP);
            CREATE TABLE omcdm_drug_exposure (drug_exposure_id INTEGER PRIMARY KEY, person_id INTEGER, drug_exposure_start_datetime TIMESTAMP, drug_concept_id INTEGER);
        """)
        con.executemany("INSERT INTO omcdm_person VALUES (?, ?, ?, ?)", [
            (1, 8532, 1980, 0),
            (2, 8507, 1975, 0),
            (3, 8532, 1965, 0),
        ])
        con.executemany("INSERT INTO omcdm_visit_occurrence VALUES (?, ?, ?, ?, ?)", [
            (10, 1, "2025-01-01", "2025-01-03", 9201),
            (11, 2, "2025-02-01", "2025-02-02", 9202),
        ])
        con.executemany("INSERT INTO omcdm_condition_occurrence VALUES (?, ?, ?, ?)", [
            (100, 1, 444247, "2024-06-01"),
            (101, 1, 201826, "2024-08-01"),
        ])
        con.close()
        yield path


@pytest.fixture
def small_silver_db():
    """Build a minimal streaming silver DuckDB for testing."""
    import duckdb
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "silver.duckdb"
        con = duckdb.connect(str(path))
        con.execute("""
            CREATE TABLE vitals_silver (topic VARCHAR, partition_id INTEGER, offset_id BIGINT, payload JSON, event_time TIMESTAMP, ingestion_time TIMESTAMP, source VARCHAR, schema_version INTEGER);
        """)
        vitals = [
            json.dumps({"event_time": "2025-03-01T00:00:00Z", "patient_id": "1", "heart_rate_bpm": 72, "spo2_pct": 98, "systolic_bp_mmHg": 120, "source": "ehr"}),
            json.dumps({"event_time": "2025-03-02T00:00:00Z", "patient_id": "1", "heart_rate_bpm": 78, "spo2_pct": 97, "systolic_bp_mmHg": 122, "source": "iot"}),
        ]
        for v in vitals:
            con.execute("INSERT INTO vitals_silver VALUES (?, ?, ?, ?, ?, ?, ?, ?)", ("healthcare.vitals", 0, 0, v, "2025-03-01", "2025-03-01", "ehr", 1))
        con.close()
        yield path


# ---------------------------------------------------------------------------
# get_predictions_df
# ---------------------------------------------------------------------------


def test_predictions_df_empty():
    df = get_predictions_df(deque())
    assert df.empty


def test_predictions_df_one():
    buf = deque([{
        "patient_id": "1", "score": 0.42, "model_id": "readmission_30d",
        "model_version": "1", "prediction_type": "readmission_30d",
        "event_time": "2026-01-01T00:00:00Z",
        "top_feature_contributions": {"feature_age": 0.1},
    }])
    df = get_predictions_df(buf)
    assert len(df) == 1
    assert df.iloc[0]["patient_id"] == "1"
    assert df.iloc[0]["score"] == 0.42


def test_predictions_df_preserves_order():
    buf = deque()
    for i in range(5):
        buf.append({"patient_id": str(i), "score": 0.1 * i, "model_id": "x", "model_version": "1",
                    "prediction_type": "readmission_30d", "event_time": f"2026-01-0{i+1}T00:00:00Z",
                    "top_feature_contributions": {}})
    df = get_predictions_df(buf)
    assert len(df) == 5
    assert df["patient_id"].tolist() == ["0", "1", "2", "3", "4"]


# ---------------------------------------------------------------------------
# get_patient_detail
# ---------------------------------------------------------------------------


def test_patient_detail_found(small_omop_db, small_silver_db):
    detail = get_patient_detail("1", str(small_omop_db), str(small_silver_db))
    assert detail["demographics"]["person_id"] == 1
    assert detail["demographics"]["age_2026"] == 2026 - 1980
    assert len(detail["conditions"]) == 2
    assert len(detail["visits"]) == 1
    assert len(detail["recent_vitals"]) == 2


def test_patient_detail_not_found(small_omop_db, small_silver_db):
    detail = get_patient_detail("9999", str(small_omop_db), str(small_silver_db))
    assert detail["demographics"] == {}
    assert detail["conditions"] == []
    assert detail["visits"] == []


def test_patient_detail_missing_omop(small_silver_db):
    """When OMOP doesn't exist, return empty detail gracefully."""
    detail = get_patient_detail("1", None, str(small_silver_db))
    assert detail["demographics"] == {}


def test_patient_detail_recent_vitals_from_silver(small_omop_db, small_silver_db):
    detail = get_patient_detail("1", str(small_omop_db), str(small_silver_db))
    assert len(detail["recent_vitals"]) == 2


# ---------------------------------------------------------------------------
# get_pipeline_stats
# ---------------------------------------------------------------------------


def test_pipeline_stats_with_omop_and_silver(small_omop_db, small_silver_db):
    stats = get_pipeline_stats(str(small_omop_db), str(small_silver_db))
    assert stats["omop"]["person"] == 3
    assert stats["omop"]["visit"] == 2
    assert stats["omop"]["condition"] == 2
    assert stats["omop"]["drug"] == 0
    assert stats["silver"]["vitals"] == 2


def test_pipeline_stats_no_files():
    stats = get_pipeline_stats(None, None)
    assert stats["omop"]["person"] == 0
    assert stats["silver"]["vitals"] == 0


# ---------------------------------------------------------------------------
# End-to-end smoke: dashboard imports cleanly, helpers work on real shapes
# ---------------------------------------------------------------------------


def test_dashboard_module_imports():
    """The full dashboard module should import without errors (with mocked streamlit)."""
    # The import at the top of the test file already exercises this. Just assert
    # the public functions exist.
    assert callable(get_predictions_df)
    assert callable(get_patient_detail)
    assert callable(get_pipeline_stats)
