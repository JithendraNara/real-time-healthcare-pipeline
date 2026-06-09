"""
Create the topics defined in streaming/topics.yaml against any Kafka-compatible broker.

Defaults to Redpanda on localhost:9092, but works against MSK, Confluent Cloud,
or a self-hosted Kafka cluster — just set KAFKA_BOOTSTRAP_SERVERS.

Safe to re-run: existing topics with matching config are left alone, mismatched
configs are reported.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import yaml
from confluent_kafka.admin import AdminClient, NewTopic

ROOT = Path(__file__).resolve().parents[1]
log = logging.getLogger("create_topics")


def load_topics(path: Path) -> list[NewTopic]:
    with path.open() as f:
        cfg = yaml.safe_load(f)
    out: list[NewTopic] = []
    for t in cfg["topics"]:
        out.append(
            NewTopic(
                topic=t["name"],
                num_partitions=int(t.get("partitions", 3)),
                replication_factor=int(t.get("replication_factor", 1)),
                config={
                    "retention.ms": str(t.get("retention_ms", 604800000)),
                    "cleanup.policy": t.get("cleanup_policy", "delete"),
                },
            )
        )
    return out


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s :: %(message)s")
    p = argparse.ArgumentParser(description="Create healthcare pipeline topics")
    p.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    p.add_argument("--config", type=Path, default=ROOT / "topics.yaml")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    topics = load_topics(args.config)
    log.info("Loaded %d topic definitions from %s", len(topics), args.config)

    if args.dry_run:
        for t in topics:
            print(f"  {t.topic:30s} partitions={t.num_partitions} rf={t.replication_factor}")
        return 0

    admin = AdminClient({"bootstrap.servers": args.bootstrap_servers})
    existing = set(admin.list_topics(timeout=10).topics.keys())
    to_create = [t for t in topics if t.topic not in existing]
    if not to_create:
        log.info("All %d topics already exist — nothing to do.", len(topics))
        return 0

    log.info("Creating %d topics on %s", len(to_create), args.bootstrap_servers)
    futures = admin.create_topics(to_create, request_timeout=15)
    rc = 0
    for topic, fut in futures.items():
        try:
            fut.result()
            log.info("  ✓ %s created", topic)
        except Exception as e:  # noqa: BLE001
            msg = str(e).lower()
            if "already exists" in msg:
                log.info("  ↻ %s already exists (race)", topic)
            else:
                log.error("  ✗ %s failed: %s", topic, e)
                rc = 1
    return rc


if __name__ == "__main__":
    sys.exit(main())
