"""
Append-only audit log for every PHI access in the pipeline.

Every read or write of a PHI field MUST go through AuditLogger. The log is
appended to a separate Iceberg table (or DuckDB table in local dev) that
cannot be modified after the fact — it's the HIPAA "audit log" that proves
who touched what, when, and why.

The log is keyed by:
  - audit_id (UUID) — unique per event
  - timestamp (ISO 8601 UTC) — when the access happened
  - actor_id — who (user, service, MLflow run, Kafka consumer group, etc.)
  - actor_type — "user", "service", "model", "system"
  - action — "read", "write", "export", "delete", "key_rotation", "deidentify"
  - resource_type — "table", "topic", "model", "key", "patient"
  - resource_id — which table/topic/model/patient
  - fields — list of PHI fields touched
  - purpose — free-text justification (clinical care, model training, deid export, ...)
  - outcome — "success" / "denied" / "error"
  - context — JSON bag for trace IDs, request IDs, etc.

The audit logger is intentionally fire-and-forget for the hot path:
audit failures do NOT block the read/write (we log the failure to stderr
and continue). This is a deliberate trade-off — the alternative would
be to deny access when audit is down, which is worse for patient care.
A production deployment should set up alerts on the failure metric.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Protocol

import duckdb

log = logging.getLogger("audit_logger")


class Action(str, Enum):
    READ = "read"
    WRITE = "write"
    EXPORT = "export"
    DELETE = "delete"
    KEY_ROTATION = "key_rotation"
    DEIDENTIFY = "deidentify"
    ENCRYPT = "encrypt"
    DECRYPT = "decrypt"


class ActorType(str, Enum):
    USER = "user"
    SERVICE = "service"
    MODEL = "model"
    SYSTEM = "system"


class Outcome(str, Enum):
    SUCCESS = "success"
    DENIED = "denied"
    ERROR = "error"


@dataclass
class AuditEvent:
    audit_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    actor_id: str = ""
    actor_type: str = ActorType.SERVICE.value
    action: str = Action.READ.value
    resource_type: str = ""
    resource_id: str = ""
    fields: list[str] = field(default_factory=list)
    purpose: str = ""
    outcome: str = Outcome.SUCCESS.value
    context: dict[str, Any] = field(default_factory=dict)

    def to_row(self) -> tuple:
        return (
            self.audit_id,
            self.timestamp,
            self.actor_id,
            self.actor_type,
            self.action,
            self.resource_type,
            self.resource_id,
            ",".join(self.fields) if self.fields else "",
            self.purpose,
            self.outcome,
            json.dumps(self.context, separators=(",", ":"), default=str),
        )


# ---------------------------------------------------------------------------
# Storage backend
# ---------------------------------------------------------------------------


class _AuditBackend(Protocol):
    def append(self, event: AuditEvent) -> None: ...
    def query(self, **filters: Any) -> list[dict]: ...


class DuckDBAuditBackend:
    """Local-dev audit backend. DuckDB table is append-only by convention:
    no UPDATE/DELETE statements are issued by this class. In production,
    swap for an Iceberg table (gov.audit.iceberg_backend)."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._con = duckdb.connect(str(self.path))
        self._con.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                audit_id VARCHAR PRIMARY KEY,
                timestamp VARCHAR,
                actor_id VARCHAR,
                actor_type VARCHAR,
                action VARCHAR,
                resource_type VARCHAR,
                resource_id VARCHAR,
                fields VARCHAR,
                purpose VARCHAR,
                outcome VARCHAR,
                context VARCHAR
            )
        """)
        self._con.commit()

    def append(self, event: AuditEvent) -> None:
        with self._lock:
            self._con.execute(
                """
                INSERT INTO audit_log
                  (audit_id, timestamp, actor_id, actor_type, action,
                   resource_type, resource_id, fields, purpose, outcome, context)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                event.to_row(),
            )
            self._con.commit()

    def query(self, *, limit: int = 100, actor_id: str | None = None, resource_id: str | None = None, since: str | None = None) -> list[dict]:
        sql = "SELECT * FROM audit_log WHERE 1=1"
        params: list = []
        if actor_id:
            sql += " AND actor_id = ?"
            params.append(actor_id)
        if resource_id:
            sql += " AND resource_id = ?"
            params.append(resource_id)
        if since:
            sql += " AND timestamp >= ?"
            params.append(since)
        sql += " ORDER BY timestamp DESC LIMIT ?"
        params.append(int(limit))
        cols = [
            "audit_id", "timestamp", "actor_id", "actor_type", "action",
            "resource_type", "resource_id", "fields", "purpose", "outcome", "context",
        ]
        cur = self._con.execute(sql, params)
        rows = cur.fetchall()
        out = []
        for r in rows:
            row = dict(zip(cols, r))
            if row.get("context"):
                try:
                    row["context"] = json.loads(row["context"])
                except Exception:
                    pass
            out.append(row)
        return out

    def count(self) -> int:
        return self._con.execute("SELECT count(*) FROM audit_log").fetchone()[0]


# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------


class AuditLogger:
    """High-level audit emitter. Wraps a backend and handles actor defaults."""

    def __init__(self, backend: DuckDBAuditBackend, default_actor_id: str | None = None, default_actor_type: ActorType = ActorType.SERVICE):
        self.backend = backend
        self.default_actor_id = default_actor_id or os.getenv("ACTOR_ID", "anonymous")
        self.default_actor_type = default_actor_type

    def log(
        self,
        action: Action | str,
        resource_type: str,
        resource_id: str = "",
        fields: Iterable[str] = (),
        purpose: str = "",
        outcome: Outcome | str = Outcome.SUCCESS,
        actor_id: str | None = None,
        actor_type: ActorType | str | None = None,
        context: dict | None = None,
    ) -> AuditEvent:
        if isinstance(action, Action):
            action = action.value
        if isinstance(outcome, Outcome):
            outcome = outcome.value
        ev = AuditEvent(
            actor_id=actor_id or self.default_actor_id,
            actor_type=(actor_type or self.default_actor_type).value if hasattr(actor_type, "value") else (actor_type or self.default_actor_type.value),
            action=action,
            resource_type=resource_type,
            resource_id=str(resource_id) if resource_id else "",
            fields=list(fields),
            purpose=purpose,
            outcome=outcome,
            context=context or {},
        )
        try:
            self.backend.append(ev)
        except Exception as e:  # noqa: BLE001
            # Audit failures must not block the hot path. Surface them loudly
            # so monitoring can pick them up.
            log.error("AUDIT WRITE FAILED: %s event=%s", e, ev.audit_id)
        return ev

    def read(self, resource_type: str, resource_id: str, fields: Iterable[str], purpose: str = "", **kwargs) -> AuditEvent:
        return self.log(Action.READ, resource_type, resource_id, fields, purpose or "clinical_care", **kwargs)

    def write(self, resource_type: str, resource_id: str, fields: Iterable[str], purpose: str = "", **kwargs) -> AuditEvent:
        return self.log(Action.WRITE, resource_type, resource_id, fields, purpose or "clinical_care", **kwargs)

    def export(self, resource_type: str, resource_id: str, fields: Iterable[str], purpose: str = "", **kwargs) -> AuditEvent:
        return self.log(Action.EXPORT, resource_type, resource_id, fields, purpose, **kwargs)

    def deidentify(self, resource_type: str, resource_id: str, fields: Iterable[str], purpose: str = "research_export", **kwargs) -> AuditEvent:
        return self.log(Action.DEIDENTIFY, resource_type, resource_id, fields, purpose, **kwargs)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


_default_logger: AuditLogger | None = None
_default_lock = threading.Lock()


def get_audit_logger(audit_db: str | None = None) -> AuditLogger:
    """Process-wide singleton. Initializes the DuckDB backend on first call."""
    global _default_logger
    with _default_lock:
        if _default_logger is None:
            path = Path(audit_db or os.getenv("AUDIT_DB_PATH", "governance/warehouse/audit.db"))
            backend = DuckDBAuditBackend(path)
            _default_logger = AuditLogger(backend)
    return _default_logger
