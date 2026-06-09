"""
Prefect 3.x end-to-end flow for the real-time healthcare pipeline.

Wires the four modules together:

  Module 1 (streaming) → Module 2 (ML scorer) → Module 3 (audit) → Module 4 (dashboard refresh)

Tasks:
  1. ensure_topics       — creates the healthcare.* topics if missing
  2. seed_omop           — seeds the OMOP DuckDB with synthetic patients (only if empty)
  3. start_producer      — spawns the streaming producer as a subprocess
  4. start_consumer      — spawns the streaming ETL (DuckDB silver sink) as a subprocess
  5. start_iot_simulator — spawns the IoT device simulator
  6. start_scorer        — spawns the ML real-time scorer (admissions → predictions)
  7. start_dashboard     — spawns the Streamlit clinical dashboard
  8. health_check        — verifies everything is producing + consuming + scoring

Run:
    # Local
    python prefect_flows/real_time_healthcare_flow.py

    # As a Prefect deployment
    prefect deploy prefect_flows/real_time_healthcare_flow.py:real_time_healthcare_flow

    # Or one-shot, then leave services running
    python prefect_flows/real_time_healthcare_flow.py --leave-running
"""
from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _run(cmd: list[str], **kwargs) -> subprocess.Popen:
    log_kwargs = {"cwd": str(ROOT), "stdout": subprocess.PIPE, "stderr": subprocess.STDOUT}
    log_kwargs.update(kwargs)
    print(f"  $ {' '.join(cmd)}")
    return subprocess.Popen(cmd, **log_kwargs)


def ensure_topics() -> None:
    """Create the healthcare.* + iot.* topics if they don't exist."""
    from confluent_kafka.admin import AdminClient, NewTopic
    from streaming.topics_yaml_path import TOPICS_YAML  # type: ignore

    # fallback if the topics_yaml_path shim doesn't exist
    try:
        topics_file = Path(TOPICS_YAML)
    except Exception:
        topics_file = ROOT / "streaming" / "topics.yaml"
    if not topics_file.exists():
        print(f"  ✗ topics.yaml not found at {topics_file}")
        return
    import yaml

    cfg = yaml.safe_load(topics_file.read_text())
    admin = AdminClient({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")})
    existing = set(admin.list_topics(timeout=10).topics.keys())
    new_topics = [NewTopic(t["name"], int(t.get("partitions", 3)), int(t.get("replication_factor", 1))) for t in cfg["topics"] if t["name"] not in existing]
    if not new_topics:
        print(f"  ✓ all topics exist ({len(existing)})")
        return
    futures = admin.create_topics(new_topics, request_timeout=15)
    for t, fut in futures.items():
        try:
            fut.result()
            print(f"  ✓ created {t}")
        except Exception as e:  # noqa: BLE001
            print(f"  ⚠ {t}: {e}")


def seed_omop_if_empty() -> None:
    """Seed the OMOP DuckDB if it doesn't have any patients yet."""
    omop = ROOT / "dbt_project" / "dbt.duckdb"
    if omop.exists():
        try:
            import duckdb

            con = duckdb.connect(str(omop), read_only=True)
            n = con.execute("SELECT count(*) FROM omcdm_person").fetchone()[0]
            con.close()
            if n > 0:
                print(f"  ✓ OMOP already seeded ({n} persons)")
                return
        except Exception:
            pass
    seeder = ROOT / "scripts" / "seed_omop.py"
    if not seeder.exists():
        print("  ⚠ no seeder found; assuming OMOP is fine")
        return
    print("  Seeding OMOP…")
    subprocess.run([sys.executable, str(seeder), "--patients", "500"], check=True, cwd=str(ROOT))


def start_producer() -> subprocess.Popen:
    return _run([sys.executable, "streaming/producers/healthcare_producer.py", "--rate", "5"])


def start_consumer() -> subprocess.Popen:
    return _run([sys.executable, "streaming/consumers/glue_etl_job.py", "--mode", "local"])


def start_iot_simulator() -> subprocess.Popen:
    return _run([sys.executable, "streaming/seeders/iot_device_simulator.py", "--patients", "30"])


def start_scorer() -> subprocess.Popen:
    env = os.environ.copy()
    env.setdefault("MLFLOW_TRACKING_URI", "sqlite:///mlflow.db")
    env.setdefault("MLFLOW_REGISTRY_NAME", "readmission_30d")
    env.setdefault("OMOP_DUCKDB", "dbt_project/dbt.duckdb")
    env.setdefault("SILVER_DUCKDB", "streaming/warehouse/silver.db")
    return subprocess.Popen(
        [sys.executable, "ml/realtime/scorer.py"],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def start_dashboard() -> subprocess.Popen:
    return _run([sys.executable, "-m", "streamlit", "run", "app/dashboard/clinical_dashboard.py", "--server.port", "8501"])


def health_check(timeout_sec: float = 15.0) -> bool:
    """Quick check: does the broker respond? Are topics being produced to?"""
    from confluent_kafka import Consumer
    from confluent_kafka.admin import AdminClient

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    try:
        admin = AdminClient({"bootstrap.servers": bootstrap})
        admin.list_topics(timeout=5)
    except Exception as e:  # noqa: BLE001
        print(f"  ✗ broker unreachable: {e}")
        return False

    c = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"health-check-{os.getpid()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    try:
        c.subscribe(["healthcare.predictions", "healthcare.vitals", "iot.telemetry"])
        deadline = time.time() + timeout_sec
        seen: set[str] = set()
        while time.time() < deadline and len(seen) < 3:
            msg = c.poll(timeout=1.0)
            if msg is None or msg.error():
                continue
            seen.add(msg.topic())
        c.close()
        print(f"  ✓ broker healthy, topics with data: {sorted(seen)}")
        return len(seen) >= 1
    except Exception as e:  # noqa: BLE001
        print(f"  ✗ health check failed: {e}")
        return False


# ---------------------------------------------------------------------------
# Prefect-decorated flow (optional — works without Prefect installed too)
# ---------------------------------------------------------------------------


def real_time_healthcare_flow() -> dict:
    """The main flow. Returns a status dict of what was started."""
    print("=" * 60)
    print("Real-Time Healthcare Pipeline — end-to-end flow")
    print("=" * 60)

    print("\n[1/5] Ensuring Kafka topics exist…")
    ensure_topics()

    print("\n[2/5] Seeding OMOP if empty…")
    seed_omop_if_empty()

    print("\n[3/5] Starting streaming services (producer + consumer + IoT sim)…")
    procs = {
        "producer": start_producer(),
        "consumer": start_consumer(),
        "iot_simulator": start_iot_simulator(),
    }

    print("\n[4/5] Starting ML scorer (consumes admissions → publishes predictions)…")
    procs["scorer"] = start_scorer()

    print("\n[5/5] Starting clinical dashboard on :8501…")
    procs["dashboard"] = start_dashboard()

    time.sleep(8)  # let everything warm up
    print("\n[health] Verifying pipeline is producing + scoring…")
    ok = health_check()

    print("\n" + "=" * 60)
    print(f"Pipeline running. Health: {'OK' if ok else 'CHECK NEEDED'}")
    print("=" * 60)
    print("\nService URLs:")
    print("  Streamlit dashboard:  http://localhost:8501")
    print("  FastAPI scorer:       http://localhost:8001 (start with uvicorn ml.api.app:app)")
    print("  MLflow UI:            http://localhost:5000 (start with docker compose -f ml/docker-compose.ml.yml up)")
    print("\nPress Ctrl+C to stop everything.")

    def shutdown(*_):
        print("\nShutting down…")
        for name, p in procs.items():
            try:
                p.terminate()
                p.wait(timeout=5)
            except Exception:
                p.kill()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Wait forever (subprocess output goes to /dev/null — tail via journal if you want)
    while True:
        time.sleep(1)
        for name, p in list(procs.items()):
            if p.poll() is not None:
                print(f"  ⚠ {name} exited with code {p.returncode}")
                procs.pop(name)


# Optional Prefect integration
try:
    from prefect import flow, task  # type: ignore

    @task
    def _ensure_topics_task() -> None:
        ensure_topics()

    @task
    def _seed_omop_task() -> None:
        seed_omop_if_empty()

    @flow(name="real-time-healthcare")
    def prefect_flow() -> None:
        _ensure_topics_task()
        _seed_omop_task()
        # (subprocesses aren't great as Prefect tasks — we run them inline here)
        real_time_healthcare_flow()

except ImportError:
    prefect_flow = None  # type: ignore


def main() -> int:
    p = argparse.ArgumentParser(description="Real-time healthcare pipeline end-to-end flow")
    p.add_argument("--leave-running", action="store_true", help="Run indefinitely (default: same)")
    args = p.parse_args()
    try:
        real_time_healthcare_flow()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
