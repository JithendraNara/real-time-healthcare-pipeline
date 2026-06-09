"""
OPA-compatible RBAC policies + a pure-Python evaluator for tests and
dev environments.

Production deployment should run the Open Policy Agent daemon (opa run -s)
and have services query it over HTTP. For unit tests and local dev, the
policies here are evaluated by `PolicyEngine.evaluate(...)` directly.

The policies encode HIPAA "minimum necessary" access — actors only get the
fields they need for their declared purpose.

Policy model:
  - actor: { id, type, role, department, clearance }
  - action: "read" | "write" | "export" | "deidentify"
  - resource: { type: "table" | "topic" | "model", id, fields: [str] }
  - purpose: free-text justification ("clinical_care", "model_training",
             "research_export", "billing", "compliance_audit")

Decision: "allow" | "deny"

If no policy matches, the default is `deny` (fail-closed).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Iterable

log = logging.getLogger("rbac")


@dataclass
class Actor:
    id: str
    type: str = "user"          # "user" | "service" | "model" | "system"
    role: str = "viewer"        # "admin" | "clinician" | "data_scientist" | "analyst" | "service" | "viewer"
    department: str = ""
    clearance: str = "phi"      # "phi" | "deidentified" | "metadata"


@dataclass
class Resource:
    type: str                   # "table" | "topic" | "model" | "key"
    id: str = ""
    fields: list[str] = field(default_factory=list)


@dataclass
class AccessRequest:
    actor: Actor
    action: str                 # "read" | "write" | "export" | "deidentify" | "key_rotation"
    resource: Resource
    purpose: str = "clinical_care"


@dataclass
class AccessDecision:
    allow: bool
    reason: str
    filtered_fields: list[str] = field(default_factory=list)  # fields the actor IS allowed to see (subset of requested)

    def __bool__(self) -> bool:
        return self.allow


# ---------------------------------------------------------------------------
# Pure-Python policy engine (CI / local)
# ---------------------------------------------------------------------------


# Role-based access matrix. The simplest defensible HIPAA mapping.
ROLE_POLICIES: dict[str, dict[str, Any]] = {
    "admin": {
        "allow": True,
        "fields": "all",                # sees everything
        "purposes": "all",
        "actions": ["read", "write", "export", "deidentify", "key_rotation"],
    },
    "clinician": {
        "allow": True,
        "fields": ["person.*", "visit.*", "condition.*", "drug.*", "measurement.*", "vitals.*"],
        "purposes": ["clinical_care", "quality_improvement", "research_consented"],
        "actions": ["read"],
    },
    "data_scientist": {
        "allow": True,
        "fields": ["omcdm_*"],          # only OMOP-schema fields, no raw identifiers
        "purposes": ["model_training", "model_evaluation", "research_export"],
        "actions": ["read", "deidentify"],
    },
    "analyst": {
        "allow": True,
        "fields": ["mart_*", "omcdm_*"],
        "purposes": ["clinical_care", "research_export", "quality_improvement", "billing"],
        "actions": ["read"],
    },
    "service": {
        "allow": True,
        "fields": "all",
        "purposes": "all",
        "actions": ["read", "write"],   # internal services can read/write the streaming pipeline
    },
    "model": {
        # ML models can read what they need for inference, only for the patient's
        # current admission context.
        "allow": True,
        "fields": ["person.*", "visit.*", "measurement.*", "vitals.*"],
        "purposes": ["clinical_care", "model_inference"],
        "actions": ["read"],
    },
    "viewer": {
        "allow": True,
        "fields": ["mart_*"],
        "purposes": ["clinical_care"],
        "actions": ["read"],
    },
}


def _fields_match(pattern: str, field: str) -> bool:
    """Glob-style: "person.*" matches "person.birth_datetime", "omcdm_*" matches "omcdm_condition_occurrence.*"."""
    import fnmatch
    if pattern == "all":
        return True
    if pattern == field:
        return True
    # fnmatch handles "*" wildcards; escape our ".*" → "*"
    p = pattern.replace(".*", "*")
    return fnmatch.fnmatchcase(field, p)


def _check_field_access(actor: Actor, action: str, fields: list[str]) -> tuple[bool, list[str]]:
    policy = ROLE_POLICIES.get(actor.role)
    if policy is None:
        return False, []
    if action not in policy["actions"]:
        return False, []
    allowed_patterns = policy["fields"]
    if allowed_patterns == "all":
        return True, list(fields)
    if allowed_patterns == "none":
        return False, []
    out: list[str] = []
    for f in fields:
        for p in allowed_patterns:
            if _fields_match(p, f):
                out.append(f)
                break
    return bool(out), out


def _check_purpose(actor: Actor, purpose: str) -> bool:
    policy = ROLE_POLICIES.get(actor.role)
    if policy is None:
        return False
    purposes = policy["purposes"]
    if purposes == "all":
        return True
    return purpose in purposes


class PolicyEngine:
    """Pure-Python policy evaluator. Mirrors what the OPA daemon would return."""

    def evaluate(self, req: AccessRequest) -> AccessDecision:
        # 1. Actor known?
        if req.actor.role not in ROLE_POLICIES:
            return AccessDecision(False, f"unknown role: {req.actor.role}")

        # 2. Action allowed?
        policy = ROLE_POLICIES[req.actor.role]
        if req.action not in policy["actions"]:
            return AccessDecision(False, f"action '{req.action}' not allowed for role '{req.actor.role}'")

        # 3. Purpose allowed?
        if not _check_purpose(req.actor, req.purpose):
            return AccessDecision(False, f"purpose '{req.purpose}' not allowed for role '{req.actor.role}'")

        # 4. Field-level access (the "minimum necessary" piece)
        if not req.resource.fields:
            return AccessDecision(True, "no field-level restriction", filtered_fields=[])

        has_any, allowed_fields = _check_field_access(req.actor, req.action, req.resource.fields)
        if not has_any:
            return AccessDecision(
                False,
                f"role '{req.actor.role}' cannot access any of requested fields {req.resource.fields}",
            )

        return AccessDecision(
            True,
            f"role '{req.actor.role}' allowed {len(allowed_fields)}/{len(req.resource.fields)} fields for purpose '{req.purpose}'",
            filtered_fields=allowed_fields,
        )


# ---------------------------------------------------------------------------
# OPA rego policy (for production)
# ---------------------------------------------------------------------------

OPA_REGO_POLICY = """
package healthcare.rbac

# Default deny
default allow = false

# Role policy lookup
role_policies := {
    "admin":         {"fields": "all",                          "purposes": ["*"],                                    "actions": ["read", "write", "export", "deidentify", "key_rotation"]},
    "clinician":     {"fields": ["person.*", "visit.*", "condition.*", "drug.*", "measurement.*", "vitals.*"], "purposes": ["clinical_care", "quality_improvement", "research_consented"], "actions": ["read"]},
    "data_scientist":{"fields": ["omcdm_*", "mart_*"],          "purposes": ["model_training", "model_evaluation", "research_export"], "actions": ["read", "deidentify"]},
    "analyst":       {"fields": ["mart_*", "omcdm_*"],         "purposes": ["clinical_care", "research_export", "quality_improvement", "billing"], "actions": ["read"]},
    "service":       {"fields": "all",                          "purposes": ["*"],                                    "actions": ["read", "write"]},
    "model":         {"fields": ["person.*", "visit.*", "measurement.*", "vitals.*"], "purposes": ["clinical_care", "model_inference"], "actions": ["read"]},
    "viewer":        {"fields": ["mart_*"],                    "purposes": ["clinical_care"],                       "actions": ["read"]}
}

# Field match: "person.*" matches "person.birth_datetime", "all" matches everything
field_match(pattern, field) = true {
    pattern == "all"
}

field_match(pattern, field) = true {
    pattern == field
}

field_match(pattern, field) = true {
    endswith(pattern, ".*")
    startswith(field, trim_suffix(pattern, ".*"))
}

# Main decision
allow {
    role := input.actor.role
    role != ""
    policy := role_policies[role]
    action := input.action
    action in policy.actions
    purpose := input.purpose
    purpose_allowed(policy.purposes, purpose)
    field_allowed(policy.fields, input.resource.fields)
}

purpose_allowed(purposes, _) {
    "*" in purposes
}

purpose_allowed(purposes, purpose) {
    not "*" in purposes
    purpose in purposes
}

field_allowed("all", _) = true

field_allowed(allowed, requested) {
    not "all" in allowed
    some f in requested
    some p in allowed
    field_match(p, f)
}

# Filtered fields (subset of requested that the actor can see)
filtered_fields[out] {
    allow
    policy := role_policies[input.actor.role]
    some f in input.resource.fields
    some p in policy.fields
    field_match(p, f)
    out := f
}

reason := sprintf("role '%v' allowed for action '%v' purpose '%v'", [input.actor.role, input.action, input.purpose]) {
    allow
}

reason := sprintf("denied: role '%v' cannot %v for purpose '%v'", [input.actor.role, input.action, input.purpose]) {
    not allow
}
"""
