"""
CLI for the readmission_30d training pipeline.

Usage:
    # Train on synthetic features (no OMOP needed — good for CI / demo)
    python ml/scripts/train.py --synthetic 500

    # Train on the real OMOP warehouse
    python ml/scripts/train.py --omop-duckdb dbt_project/dbt.duckdb

    # Train + promote to Production
    python ml/scripts/train.py --synthetic 1000 --promote
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from ml.outcomes.feature_engineering import build_omop_features, synth_features  # noqa: E402
from ml.outcomes.model_registry import (  # noqa: E402
    RegistryConfig,
    log_readmission_model,
    transition_to_production,
)
from ml.outcomes.readmission_predictor import (  # noqa: E402
    FEATURE_COLUMNS,
    TrainConfig,
    feature_importances,
    predict,
    train,
)

log = logging.getLogger("train")


def main() -> int:
    p = argparse.ArgumentParser(description="Train the readmission_30d model")
    p.add_argument("--omop-duckdb", default=None, help="Path to OMOP DuckDB (if not using --synthetic)")
    p.add_argument("--synthetic", type=int, default=None, help="N synthetic samples to use instead of OMOP")
    p.add_argument("--tracking-uri", default="mlruns", help="MLflow tracking URI (file://, http://, databricks://)")
    p.add_argument("--registry-name", default="readmission_30d")
    p.add_argument("--promote", action="store_true", help="Transition the new version to Production")
    p.add_argument("--save-test-split", action="store_true", help="Save the held-out test split to ml/data/test.parquet")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(asctime)s %(levelname)s %(name)s :: %(message)s")

    # 1. Load data
    if args.synthetic:
        log.info("Using %d synthetic samples (no OMOP)", args.synthetic)
        df = synth_features(n=args.synthetic)
    elif args.omop_duckdb:
        log.info("Extracting features from OMOP at %s", args.omop_duckdb)
        from ml.outcomes.feature_engineering import FeatureConfig
        df = build_omop_features(Path(args.omop_duckdb), FeatureConfig())
    else:
        log.error("Must supply either --omop-duckdb or --synthetic N")
        return 1
    if df.empty:
        log.error("No training data — aborting.")
        return 1
    log.info("Loaded %d rows, positive rate %.1f%%", len(df), 100 * df["label"].mean())

    # 2. Train
    cfg = TrainConfig()
    model, metrics, train_df, test_df = train(df, cfg)

    # 3. Log to MLflow
    registry_cfg = RegistryConfig(tracking_uri=args.tracking_uri, registry_name=args.registry_name)
    run_id = log_readmission_model(
        cfg=registry_cfg,
        model=model,
        metrics=metrics,
        params=vars(cfg),
        feature_names=FEATURE_COLUMNS,
        input_example=train_df[FEATURE_COLUMNS].head(5),
        tags={"module": "readmission_30d", "omop_version": "v5.4"},
    )

    # 4. Optionally promote
    if args.promote:
        import mlflow
        client = mlflow.tracking.MlflowClient(tracking_uri=args.tracking_uri)
        versions = client.get_latest_versions(args.registry_name, stages=["None"])
        if versions:
            v = int(versions[0].version)
            transition_to_production(registry_cfg, v)

    # 5. Persist test split for downstream drift monitoring
    if args.save_test_split:
        out_dir = ROOT / "ml" / "data"
        out_dir.mkdir(parents=True, exist_ok=True)
        test_df.to_parquet(out_dir / "test.parquet", index=False)
        log.info("Saved test split (%d rows) to %s", len(test_df), out_dir / "test.parquet")

    # 6. Print summary
    print("\n" + "=" * 60)
    print("READMISSION_30D — TRAIN COMPLETE")
    print("=" * 60)
    for k, v in metrics.items():
        if isinstance(v, float):
            print(f"  {k:25s} {v:.4f}")
        else:
            print(f"  {k:25s} {v}")
    importances = feature_importances(model)
    if importances:
        top = sorted(importances.items(), key=lambda kv: kv[1], reverse=True)[:5]
        print("\n  Top-5 features (gain):")
        for k, v in top:
            print(f"    {k:30s} {v:.1f}")
    print(f"\n  MLflow run_id: {run_id}")
    print(f"  Registry:      {args.registry_name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
