"""
Readmission-30d predictor — the first ML model on the platform.

End-to-end interface for training + scoring. The training step uses LightGBM
(handles tabular features, gives SHAP-compatible feature importances, trains
fast on OMOP-scale data). The scoring step produces a probability + a
SHAP-based local explanation so the clinical dashboard can show "why this
patient is high risk" rather than just a black-box number.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

log = logging.getLogger("readmission_predictor")

FEATURE_COLUMNS = [
    "feature_age", "feature_gender", "feature_race",
    "feature_total_visits", "feature_visits_90d", "feature_visits_180d", "feature_visits_365d",
    "feature_days_since_last", "feature_mean_los_days", "feature_last_visit_type",
    "feature_total_conditions", "feature_chronic_conditions", "feature_distinct_ccs",
    "feature_total_drugs",
    "feature_mean_hr_30d", "feature_mean_spo2_30d", "feature_mean_sbp_30d",
    "feature_charlson_proxy",
]


@dataclass(frozen=True)
class TrainConfig:
    num_leaves: int = 31
    learning_rate: float = 0.05
    n_estimators: int = 400
    min_child_samples: int = 10
    reg_alpha: float = 0.0
    reg_lambda: float = 0.1
    random_state: int = 42
    n_jobs: int = -1
    test_size: float = 0.2
    early_stopping_rounds: int = 30


def _feature_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Pull just the feature columns out of a feature DataFrame. Missing columns → 0."""
    return df.reindex(columns=FEATURE_COLUMNS, fill_value=0.0)


def train(df: pd.DataFrame, cfg: TrainConfig) -> tuple[Any, dict[str, float], pd.DataFrame, pd.DataFrame]:
    """
    Train a LightGBM model on the (features, label) DataFrame.

    Returns (model, metrics, train_df, test_df) so the caller can log the
    evaluation split to MLflow and so the test split can be held out for
    later drift analysis.
    """
    import lightgbm as lgb
    from sklearn.metrics import (
        average_precision_score,
        brier_score_loss,
        f1_score,
        precision_score,
        recall_score,
        roc_auc_score,
    )
    from sklearn.model_selection import train_test_split

    if "label" not in df.columns:
        raise ValueError("training DataFrame must have a 'label' column")
    X = _feature_matrix(df)
    y = df["label"].astype(int)

    # Time-aware split: train on earlier discharges, test on later — no leakage
    if "discharge_time" in df.columns:
        df = df.sort_values("discharge_time").reset_index(drop=True)
        cut = int(len(df) * (1 - cfg.test_size))
        train_df, test_df = df.iloc[:cut], df.iloc[cut:]
    else:
        train_df, test_df = train_test_split(df, test_size=cfg.test_size, random_state=cfg.random_state, stratify=y)

    X_train, y_train = _feature_matrix(train_df), train_df["label"].astype(int)
    X_test, y_test = _feature_matrix(test_df), test_df["label"].astype(int)

    model = lgb.LGBMClassifier(
        num_leaves=cfg.num_leaves,
        learning_rate=cfg.learning_rate,
        n_estimators=cfg.n_estimators,
        min_child_samples=cfg.min_child_samples,
        reg_alpha=cfg.reg_alpha,
        reg_lambda=cfg.reg_lambda,
        random_state=cfg.random_state,
        n_jobs=cfg.n_jobs,
        verbose=-1,
    )
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        callbacks=[lgb.early_stopping(cfg.early_stopping_rounds, verbose=False), lgb.log_evaluation(0)],
    )

    proba = model.predict_proba(X_test)[:, 1]
    pred = (proba >= 0.5).astype(int)

    metrics = {
        "roc_auc": float(roc_auc_score(y_test, proba)) if y_test.nunique() > 1 else 0.0,
        "average_precision": float(average_precision_score(y_test, proba)) if y_test.nunique() > 1 else 0.0,
        "brier": float(brier_score_loss(y_test, proba)),
        "f1": float(f1_score(y_test, pred, zero_division=0)),
        "precision": float(precision_score(y_test, pred, zero_division=0)),
        "recall": float(recall_score(y_test, pred, zero_division=0)),
        "n_train": int(len(train_df)),
        "n_test": int(len(test_df)),
        "pos_rate_train": float(y_train.mean()) if len(y_train) else 0.0,
        "pos_rate_test": float(y_test.mean()) if len(y_test) else 0.0,
    }
    log.info("Trained readmission model: %s", metrics)
    return model, metrics, train_df, test_df


def predict(model: Any, features: dict[str, float] | pd.DataFrame) -> dict[str, Any]:
    """
    Score one or many patients. Returns:
      - score: probability of 30-day readmission (0-1)
      - risk_band: low/medium/high based on standard clinical cutoffs
      - top_feature_contributions: SHAP-style local explanation
    """
    if isinstance(features, dict):
        df = pd.DataFrame([features])
    else:
        df = features.copy()
    X = _feature_matrix(df)

    proba = model.predict_proba(X)[:, 1]
    out: list[dict[str, Any]] = []
    for i, p in enumerate(proba):
        band = "low" if p < 0.10 else ("medium" if p < 0.30 else "high")
        out.append(
            {
                "score": float(p),
                "risk_band": band,
                "top_feature_contributions": _local_explanation(model, X.iloc[[i]]),
            }
        )
    if len(out) == 1:
        return out[0]
    return {"predictions": out}


def _local_explanation(model: Any, X: pd.DataFrame) -> dict[str, float]:
    """SHAP-style local feature contribution for a single patient. Top 5 only."""
    try:
        import shap

        explainer = shap.TreeExplainer(model)
        sv = explainer.shap_values(X)
        # For binary classification, shap_values may return list [neg_class, pos_class]
        if isinstance(sv, list):
            sv = sv[1]
        contributions = dict(zip(X.columns, sv.flatten().tolist()))
        # Sort by abs magnitude, take top 5
        sorted_c = sorted(contributions.items(), key=lambda kv: abs(kv[1]), reverse=True)[:5]
        return {k: round(float(v), 4) for k, v in sorted_c}
    except Exception as e:  # noqa: BLE001
        log.debug("SHAP explainer unavailable: %s", e)
        return {}


def feature_importances(model: Any) -> dict[str, float]:
    """Return the model's global feature importances (gain-based)."""
    try:
        imp = model.booster_.feature_importance(importance_type="gain")
        return dict(zip(FEATURE_COLUMNS, [float(x) for x in imp]))
    except Exception:
        try:
            return dict(zip(FEATURE_COLUMNS, [float(x) for x in model.feature_importances_]))
        except Exception:
            return {}
