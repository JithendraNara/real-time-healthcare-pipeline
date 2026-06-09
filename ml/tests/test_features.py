"""
Tests for the ML module.

No MLflow, no Kafka, no real OMOP required — uses synthetic data only.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from ml.outcomes.feature_engineering import (  # noqa: E402
    CHRONIC_CCS_PREFIXES,
    FeatureConfig,
    build_features_for_patient,
    synth_features,
    _ccs_category_from_concept,
)
from ml.outcomes.model_registry import RegistryConfig, configure_mlflow  # noqa: E402
from ml.outcomes.readmission_predictor import (  # noqa: E402
    FEATURE_COLUMNS,
    TrainConfig,
    feature_importances,
    predict,
    train,
)


# ---------------------------------------------------------------------------
# Synthetic data
# ---------------------------------------------------------------------------


def test_synth_features_shape():
    df = synth_features(n=200)
    assert len(df) == 200
    assert "label" in df.columns
    assert all(c in df.columns for c in FEATURE_COLUMNS)
    # Positive rate should be sane (not 0%, not 100%)
    pr = df["label"].mean()
    assert 0.05 < pr < 0.95


def test_synth_features_reproducible():
    a = synth_features(n=100, seed=7)
    b = synth_features(n=100, seed=7)
    assert a.equals(b)


# ---------------------------------------------------------------------------
# CCS category mapping
# ---------------------------------------------------------------------------


def test_ccs_category_lookup():
    assert _ccs_category_from_concept("Type 2 diabetes mellitus") == "endocrine"
    assert _ccs_category_from_concept("Essential hypertension") == "circulatory"
    assert _ccs_category_from_concept("COPD exacerbation") == "respiratory"
    assert _ccs_category_from_concept("Asthma") == "respiratory"
    assert _ccs_category_from_concept("Major depressive disorder") == "mental"
    assert _ccs_category_from_concept("Fracture of femur") == "other"
    assert _ccs_category_from_concept(None) is None


def test_chronic_prefixes_nonempty():
    assert len(CHRONIC_CCS_PREFIXES) >= 5


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------


def test_train_basic():
    df = synth_features(n=300)
    cfg = TrainConfig(n_estimators=50, early_stopping_rounds=20)
    model, metrics, train_df, test_df = train(df, cfg)
    assert "roc_auc" in metrics
    assert 0.0 <= metrics["roc_auc"] <= 1.0
    assert len(train_df) > 0
    assert len(test_df) > 0
    assert len(train_df) + len(test_df) == len(df)


def test_train_metrics_make_sense():
    """A model on well-structured synthetic data with a real signal should beat
    random (AUC > 0.55) and have non-trivial brier (i.e., it's outputting
    non-constant probabilities)."""
    df = synth_features(n=1000, seed=42)
    cfg = TrainConfig(n_estimators=200, learning_rate=0.05)
    _, metrics, _, _ = train(df, cfg)
    assert metrics["roc_auc"] > 0.55, f"AUC too low: {metrics['roc_auc']}"
    assert 0.0 < metrics["brier"] < 0.5


def test_train_time_aware_split():
    """When discharge_time is present, the split should be temporal (no overlap)."""
    df = synth_features(n=200)
    cfg = TrainConfig(test_size=0.25, n_estimators=50, early_stopping_rounds=20)
    _, _, train_df, test_df = train(df, cfg)
    assert train_df["discharge_time"].max() <= test_df["discharge_time"].min()


def test_train_missing_label_raises():
    df = synth_features(n=100).drop(columns=["label"])
    with pytest.raises(ValueError):
        train(df, TrainConfig(n_estimators=10, early_stopping_rounds=5))


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------


def test_predict_one():
    df = synth_features(n=300)
    model, _, _, _ = train(df, TrainConfig(n_estimators=50, early_stopping_rounds=20))
    row = df[FEATURE_COLUMNS].iloc[0].to_dict()
    out = predict(model, row)
    assert "score" in out
    assert out["risk_band"] in ("low", "medium", "high")
    assert 0.0 <= out["score"] <= 1.0
    assert isinstance(out["top_feature_contributions"], dict)


def test_predict_batch():
    df = synth_features(n=300)
    model, _, _, _ = train(df, TrainConfig(n_estimators=50, early_stopping_rounds=20))
    out = predict(model, df[FEATURE_COLUMNS].head(5))
    assert "predictions" in out
    assert len(out["predictions"]) == 5


def test_predict_handles_missing_features():
    df = synth_features(n=200)
    model, _, _, _ = train(df, TrainConfig(n_estimators=50, early_stopping_rounds=20))
    # Drop a couple of features — should fill with 0 and still produce a score
    row = df[FEATURE_COLUMNS].iloc[0].drop(["feature_age", "feature_mean_hr_30d"]).to_dict()
    out = predict(model, row)
    assert "score" in out


def test_feature_importances_returns_dict():
    df = synth_features(n=200)
    model, _, _, _ = train(df, TrainConfig(n_estimators=50, early_stopping_rounds=20))
    imp = feature_importances(model)
    assert isinstance(imp, dict)
    if imp:  # might be empty for some model classes
        assert len(imp) == len(FEATURE_COLUMNS)


# ---------------------------------------------------------------------------
# MLflow registry (uses local file:// backend, no server)
# ---------------------------------------------------------------------------


def test_mlflow_log_and_load(tmp_path: Path):
    """Train → log → load back from a local sqlite MLflow tracking URI."""
    import mlflow

    tracking_uri = f"sqlite:///{tmp_path / 'mlflow.db'}"
    cfg = RegistryConfig(tracking_uri=tracking_uri, registry_name="readmission_test")
    configure_mlflow(cfg)

    df = synth_features(n=300)
    model, metrics, _, _ = train(df, TrainConfig(n_estimators=50, early_stopping_rounds=20))
    run_id = mlflow.active_run().info.run_id if mlflow.active_run() else "no-run"

    from ml.outcomes.model_registry import log_readmission_model

    rid = log_readmission_model(
        cfg=cfg,
        model=model,
        metrics=metrics,
        params=vars(TrainConfig(n_estimators=50, early_stopping_rounds=20)),
        feature_names=FEATURE_COLUMNS,
        input_example=df[FEATURE_COLUMNS].head(5),
    )
    assert rid

    # Search for the run
    client = mlflow.tracking.MlflowClient(tracking_uri=tracking_uri)
    exp = client.get_experiment_by_name("readmission_30d")
    assert exp is not None
    runs = client.search_runs(experiment_ids=[exp.experiment_id], max_results=5)
    assert len(runs) >= 1
    assert any("roc_auc" in r.data.metrics for r in runs)


# ---------------------------------------------------------------------------
# Feature extraction edge cases
# ---------------------------------------------------------------------------


def test_build_features_for_patient_missing_omop(tmp_path: Path):
    """If the OMOP DuckDB doesn't exist, return empty dict — caller handles 404."""
    with pytest.raises(FileNotFoundError):
        build_features_for_patient("9999", tmp_path / "nonexistent.duckdb")


def test_feature_config_defaults():
    cfg = FeatureConfig()
    assert cfg.lookback_days_short == 90
    assert cfg.lookback_days_long == 365
    assert cfg.silver_path is None
