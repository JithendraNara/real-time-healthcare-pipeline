"""
Auto-audit wrapper for the streaming consumer.

Wraps the streaming consumer's read path so that every PHI field read is
logged to the audit table. The audit log is keyed by:
  - actor_id = consumer group ID (e.g., "glue-etl-local")
  - action = "read"
  - resource_type = "topic"
  - resource_id = the topic name
  - fields = the PHI fields touched in this event
  - purpose = "clinical_care" (or override)
  - context = { partition, offset, key }

This is the integration point that turns "we have encryption" into
"we have governance" — every read of a PHI envelope gets logged.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Iterable, Iterator

from confluent_kafka import Consumer, KafkaError, Message

from governance.audit.audit_logger import Action, AuditLogger, Outcome
from governance.encryption.crypto import CryptoService
from governance.encryption.phi_fields import is_phi

log = logging.getLogger("governed_consumer")


class GovernedConsumer:
    """A Kafka consumer that auto-audits every PHI field read.

    Wraps a confluent_kafka Consumer + an AuditLogger. On poll, every event
    is decoded and scanned for PHI fields; the audit log records which
    fields were accessed. The event itself is passed through unchanged —
    the consumer's downstream code (the Glue ETL, the ML scorer) decides
    what to do with it.
    """

    def __init__(
        self,
        consumer: Consumer,
        audit: AuditLogger,
        actor_id: str,
        topics: Iterable[str],
        crypto: CryptoService | None = None,
    ):
        self.consumer = consumer
        self.audit = audit
        self.actor_id = actor_id
        self.topics = list(topics)
        self.crypto = crypto or CryptoService()
        self.consumer.subscribe(self.topics)

    def poll(self, timeout: float = 1.0) -> Message | None:
        msg = self.consumer.poll(timeout=timeout)
        if msg is None or msg.error():
            return msg
        self._audit_message(msg)
        return msg

    def _audit_message(self, msg: Message) -> None:
        try:
            payload = json.loads(msg.value() or b"{}")
        except Exception:
            payload = {"_raw": (msg.value() or b"").decode("utf-8", errors="replace")}
        phi_fields = self._detect_phi_fields(payload)
        if not phi_fields:
            return
        try:
            self.audit.log(
                Action.READ,
                resource_type="topic",
                resource_id=msg.topic(),
                fields=phi_fields,
                purpose="clinical_care",
                actor_id=self.actor_id,
                context={
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "key": msg.key().decode("utf-8") if msg.key() else None,
                },
            )
        except Exception as e:  # noqa: BLE001
            log.error("audit emission failed: %s", e)

    def _detect_phi_fields(self, payload: dict, prefix: str = "") -> list[str]:
        out: list[str] = []
        for k, v in payload.items():
            full = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                out.extend(self._detect_phi_fields(v, prefix=full))
                continue
            if isinstance(v, str) and self.crypto.is_envelope(v):
                # Reverse-lookup the PHI field path from the envelope
                # (we don't store the path in the envelope for size, so we
                # match by envelope presence + field name patterns)
                if is_phi(full) or k in (
                    "patient_id", "encounter_id", "device_id", "person_id",
                    "mrn", "ssn", "birth_datetime", "phone", "email",
                ):
                    out.append(full)
        return out

    def commit(self, msg: Message, asynchronous: bool = False) -> None:
        self.consumer.commit(msg, asynchronous=asynchronous)

    def close(self) -> None:
        self.consumer.close()
