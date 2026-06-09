"""
Thin wrapper over the MLflow client for the readmission prediction model.

Handles:
  - Tracking URI configuration (env-driven; defaults to local file://)
  - Model logging with consistent signature + input example
  - Registry-stage transitions (None → Staging → Production)
  - Load latest Production / Staging model

Why a wrapper: the project uses MLflow across training, scoring, and the FastAPI
service. Centralizing URI/registry logic here means one place to change when
swapping to Databricks-managed MLflow or SageMaker.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

log = logging.getLogger("model_registry")

DEFAULT_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "sqlite:///mlflow.db")
DEFAULT_REGISTRY_NAME = os.getenv("MLFLOW_REGISTRY_NAME", "readmission_30d")

# MLflow 3.x deprecated the file:// backend. Opt out for backwards compat when
# callers explicitly set MLFLOW_TRACKING_URI=mlruns (file://). New installs should
# use sqlite:///mlflow.db or a database backend.
if "MLFLOW_ALLOW_FILE_STORE" not in os.environ:
    os.environ.setdefault("MLFLOW_ALLOW_FILE_STORE", "true")


@dataclass(frozen=True)
class RegistryConfig:
    tracking_uri: str
    registry_name: str
    experiment_name: str = "readmission_30d"


def configure_mlflow(cfg: RegistryConfig) -> None:
    import mlflow

    mlflow.set_tracking_uri(cfg.tracking_uri)
    mlflow.set_experiment(cfg.experiment_name)
    log.info("MLflow tracking_uri=%s experiment=%s", cfg.tracking_uri, cfg.experiment_name)


def log_readmission_model(
    cfg: RegistryConfig,
    model: Any,  # lightgbm.Booster or sklearn-compatible
    metrics: dict[str, float],
    params: dict[str, Any],
    feature_names: list[str],
    input_example: Any = None,
    tags: dict[str, str] | None = None,
) -> str:
    """Log a trained model + metrics + params to MLflow, register it. Returns the
    registered model version URI."""
    import mlflow

    configure_mlflow(cfg)
    with mlflow.start_run() as run:
        mlflow.set_tags(tags or {})
        mlflow.log_params(params)
        mlflow.log_metrics(metrics)
        signature = None
        if input_example is not None:
            try:
                import pandas as pd
                import mlflow.models
                from mlflow.types.schema import Schema
                from mlflow.types import ColSpec, DataType

                example_df = input_example if isinstance(input_example, pd.DataFrame) else pd.DataFrame(input_example, columns=feature_names)
                signature = mlflow.models.infer_signature(example_df, model.predict(example_df))
            except Exception as e:  # noqa: BLE001
                log.warning("Signature inference failed: %s — logging without signature", e)

        mlflow.lightgbm.log_model(
            model,
            artifact_path="model",
            registered_model_name=cfg.registry_name,
            signature=signature,
            input_example=input_example,
        )
        run_id = run.info.run_id
        log.info("Logged run_id=%s metrics=%s", run_id, metrics)
        return run_id


def transition_to_production(cfg: RegistryConfig, version: int) -> None:
    """Move a registered model version to Production, archiving any previous Production."""
    import mlflow
    from mlflow.exceptions import RestException

    client = mlflow.tracking.MlflowClient(tracking_uri=cfg.tracking_uri)
    try:
        # Archive existing production (if any). Stages API is deprecated in MLflow 2.9+;
        # fall back gracefully if the older methods are not available.
        try:
            prod = client.get_latest_versions(cfg.registry_name, stages=["Production"])
        except (AttributeError, TypeError):
            prod = []
        for old in prod:
            if str(old.version) != str(version):
                try:
                    client.transition_model_version_stage(
                        name=cfg.registry_name, version=old.version, stage="Archived"
                    )
                except Exception as e:  # noqa: BLE001
                    log.debug("Could not archive v%s: %s", old.version, e)
    except RestException:
        pass
    try:
        client.transition_model_version_stage(
            name=cfg.registry_name, version=version, stage="Production"
        )
    except Exception as e:  # noqa: BLE001
        log.warning("Could not transition to Production via stages API (%s). Use aliases instead.", e)
        # Newer MLflow: use model aliases. 'production' is a recommended alias.
        try:
            client.set_registered_model_alias(cfg.registry_name, "production", version)
            log.info("Set alias 'production' → v%s (modern MLflow model aliasing)", version)
        except Exception as e2:  # noqa: BLE001
            log.error("Failed to set production alias: %s", e2)
            return
    log.info("Transitioned %s v%s → Production", cfg.registry_name, version)


def load_production_model(cfg: RegistryConfig) -> tuple[Any, list[str]]:
    """Load the current Production model + the feature names it expects."""
    import mlflow
    import mlflow.lightgbm

    configure_mlflow(cfg)
    model_uri = f"models:/{cfg.registry_name}/Production"
    model = mlflow.lightgbm.load_model(model_uri)
    # Feature names are stored on the booster
    try:
        feature_names = list(model.feature_name())
    except Exception:
        feature_names = []
    log.info("Loaded production model from %s with %d features", model_uri, len(feature_names))
    return model, feature_names


def load_model_by_version(cfg: RegistryConfig, version: int) -> tuple[Any, list[str]]:
    import mlflow
    import mlflow.lightgbm

    configure_mlflow(cfg)
    model_uri = f"models:/{cfg.registry_name}/{version}"
    model = mlflow.lightgbm.load_model(model_uri)
    try:
        feature_names = list(model.feature_name())
    except Exception:
        feature_names = []
    return model, feature_names
