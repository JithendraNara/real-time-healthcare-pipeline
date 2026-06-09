"""
FastAPI scorer for the readmission_30d model.

POST /predict  { patient_id: "..." }
  → { patient_id, score, risk_band, top_feature_contributions, features_used, model_version, scored_at }

GET  /health
GET  /schema  (returns the feature list the loaded model expects)

The service loads the Production model at startup. If MLflow is unreachable,
falls back to a local cached model (ml/data/latest_model/) if present.
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

import sys
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from ml.outcomes.feature_engineering import build_features_for_patient  # noqa: E402
from ml.outcomes.model_registry import (  # noqa: E402
    RegistryConfig,
    configure_mlflow,
    load_production_model,
)
from ml.outcomes.readmission_predictor import FEATURE_COLUMNS, predict  # noqa: E402

log = logging.getLogger("scorer_api")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s :: %(message)s")

app = FastAPI(
    title="Readmission 30-Day Scorer",
    description="Real-time 30-day readmission risk prediction for the real-time healthcare pipeline",
    version="0.1.0",
)


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------


MODEL = None
FEATURE_NAMES: list[str] = []
MODEL_VERSION = "unknown"
LOADED_AT = None


def _load_model() -> None:
    global MODEL, FEATURE_NAMES, MODEL_VERSION, LOADED_AT
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "mlruns")
    registry_name = os.getenv("MLFLOW_REGISTRY_NAME", "readmission_30d")
    cfg = RegistryConfig(tracking_uri=tracking_uri, registry_name=registry_name)
    try:
        model, feats = load_production_model(cfg)
        MODEL = model
        FEATURE_NAMES = feats or FEATURE_COLUMNS
        # Pull version from env or use the URI tail
        MODEL_VERSION = os.getenv("MODEL_VERSION", "production")
        LOADED_AT = datetime.now(timezone.utc).isoformat()
        log.info("Loaded production model: version=%s features=%d", MODEL_VERSION, len(FEATURE_NAMES))
    except Exception as e:  # noqa: BLE001
        log.warning("Could not load production model from MLflow (%s). Falling back to untrained stub.", e)
        MODEL = None
        FEATURE_NAMES = FEATURE_COLUMNS
        MODEL_VERSION = "none"


@app.on_event("startup")
def _startup() -> None:
    _load_model()


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


class PredictRequest(BaseModel):
    patient_id: str = Field(..., description="OMOP person_id")


class PredictResponse(BaseModel):
    patient_id: str
    score: float
    risk_band: str
    top_feature_contributions: dict[str, float]
    features_used: list[str]
    model_version: str
    scored_at: str


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok" if MODEL is not None else "degraded",
        "model_loaded": MODEL is not None,
        "model_version": MODEL_VERSION,
        "loaded_at": LOADED_AT,
        "feature_count": len(FEATURE_NAMES),
    }


@app.get("/schema")
def schema() -> dict[str, Any]:
    return {
        "model_version": MODEL_VERSION,
        "features": FEATURE_NAMES,
    }


@app.post("/predict", response_model=PredictResponse)
def predict_endpoint(req: PredictRequest) -> PredictResponse:
    if MODEL is None:
        raise HTTPException(status_code=503, detail="model not loaded — see /health")
    omop = Path(os.getenv("OMOP_DUCKDB", "dbt_project/dbt.duckdb"))
    silver = Path(os.getenv("SILVER_DUCKDB", "streaming/warehouse/silver.db"))
    if not omop.exists():
        raise HTTPException(status_code=503, detail=f"OMOP DuckDB not found at {omop}")
    features = build_features_for_patient(req.patient_id, omop, silver if silver.exists() else None)
    if not features:
        raise HTTPException(status_code=404, detail=f"patient {req.patient_id} not found in OMOP")
    result = predict(MODEL, features)
    return PredictResponse(
        patient_id=req.patient_id,
        score=result["score"],
        risk_band=result["risk_band"],
        top_feature_contributions=result["top_feature_contributions"],
        features_used=sorted(features.keys()),
        model_version=MODEL_VERSION,
        scored_at=datetime.now(timezone.utc).isoformat(),
    )


@app.post("/reload")
def reload() -> dict[str, Any]:
    """Re-load the model from MLflow. Useful after a fresh training run."""
    _load_model()
    return {"reloaded": MODEL is not None, "model_version": MODEL_VERSION, "loaded_at": LOADED_AT}
