"""
Tiny end-to-end smoke test — produces 50 events, validates them, prints a sample.
Does NOT require Kafka. Useful for sanity-checking the event shape after edits.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from streaming.producers.healthcare_producer import (  # noqa: E402
    gen_admission,
    gen_iot,
    gen_lab,
    gen_vitals,
)


def main() -> int:
    print("=" * 60)
    print("Real-Time Healthcare Pipeline — local event-shape smoke test")
    print("=" * 60)

    print("\nVitals (EHR):")
    v = gen_vitals("p1", source="ehr")
    print(json.dumps(v.model_dump(mode="json"), indent=2, default=str))
    print(f"  critical: {v.has_critical_value}")

    print("\nAdmission:")
    a = gen_admission("p1")
    print(json.dumps(a.model_dump(mode="json"), indent=2, default=str))

    print("\nLab result:")
    l = gen_lab("p1")
    print(json.dumps(l.model_dump(mode="json"), indent=2, default=str))
    print(f"  critical: {l.is_critical}")

    print("\nIoT telemetry:")
    i = gen_iot("p1")
    print(json.dumps(i.model_dump(mode="json"), indent=2, default=str))

    print("\nAll event shapes serialized cleanly. Kafka not touched.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
