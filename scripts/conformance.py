"""Evaluate selected Paper 1 E0 rules against a pinned graph and retained run evidence.

Passes are finite and scoped. Missing data and unknown clock bounds never establish
universal conformance. This tool is offline and never changes the cluster.
"""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import subprocess
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import yaml
from eirvah_contracts.ulid import is_valid_correlation_id
from eirvah_contracts.uns import parse_uns_topic

ROOT = Path(__file__).resolve().parents[1]
REFERENCE = ROOT / "config/architecture/conformance-reference.json"
MANIFEST = ROOT / "config/architecture/local-control.json"
Status = Literal["pass", "fail", "unknown"]
Check = dict[str, Any]


def verdict(statuses: list[str]) -> Status:
    return (
        "fail"
        if "fail" in statuses
        else "unknown"
        if not statuses or "unknown" in statuses
        else "pass"
    )


def result(
    name: str,
    status: Status,
    reason: str,
    *,
    scope: str,
    evidence: list[Any] | None = None,
    executed: bool = True,
) -> Check:
    return {
        "rule": name.split(".")[0],
        "check": name,
        "status": status,
        "reason": reason,
        "scope": scope,
        "execution": "executed" if executed else "not_run",
        "evidence": evidence or [],
    }


def profile_signature(profile: dict[str, Any]) -> dict[str, Any]:
    return {
        "roles": sorted(profile["roles"]),
        "flow_kinds": sorted(profile["flow_kinds"]),
        "groups": profile["groups"],
        "role_layers": {k: sorted(v) for k, v in profile["role_layers"].items()},
        "permitted_interactions": sorted(tuple(v) for v in profile["permitted_interactions"]),
    }


def reachable(
    edges: list[dict[str, Any]], start: str, end: str, flow: str, blocked: set[str]
) -> bool:
    pending = [start]
    seen = set(blocked)
    while pending:
        node = pending.pop()
        if node in seen:
            continue
        if node == end:
            return True
        seen.add(node)
        pending.extend(e["target"] for e in edges if e["source"] == node and e["flow"] == flow)
    return False


def check_structure(manifest: dict[str, Any], reference: dict[str, Any]) -> list[Check]:
    scope = "declared_manifest_only"
    keys = ("components", "vertices", "contracts", "channels", "edges")
    if manifest.get("format_version") != "1.0" or any(
        not isinstance(manifest.get(k), list) or not manifest[k] for k in keys
    ):
        return [
            result("C01.typed_graph", "unknown", "A nonempty mapping is missing", scope=scope),
            result("C02.required_routes", "unknown", "Graph cannot be evaluated", scope=scope),
        ]
    errors = []
    try:
        indexed = {k: {v["id"]: v for v in manifest[k]} for k in keys}
        for k in keys:
            if len(indexed[k]) != len(manifest[k]):
                errors.append("duplicate IDs: " + k)
            if any(not isinstance(v["id"], str) or not v["id"] for v in manifest[k]):
                errors.append("invalid IDs: " + k)
        vertices, contracts = indexed["vertices"], indexed["contracts"]
        profile = reference["profile"]
        if profile_signature(manifest["profile"]) != profile_signature(profile):
            errors.append("profile differs from pinned reference")
        permitted = {tuple(x) for x in profile["permitted_interactions"]}
        for component in manifest["components"]:
            if component["group"] not in {*profile["groups"], "infrastructure"}:
                errors.append("undeclared group: " + component["id"])
        for contract in contracts.values():
            if contract["kind"] not in profile["flow_kinds"]:
                errors.append("undeclared contract kind: " + contract["id"])
        for vertex in vertices.values():
            if not vertex["roles"] or not set(vertex["roles"]) <= set(profile["roles"]):
                errors.append("undeclared role: " + vertex["id"])
            if (
                not vertex["components"]
                or not set(vertex["components"]) <= indexed["components"].keys()
            ):
                errors.append("unmapped component: " + vertex["id"])
        for channel in manifest["channels"]:
            if (
                not channel["components"]
                or not set(channel["components"]) <= indexed["components"].keys()
            ):
                errors.append("unmapped channel: " + channel["id"])
        for edge in manifest["edges"]:
            if edge["source"] not in vertices or edge["target"] not in vertices:
                errors.append("missing edge endpoint: " + edge["id"])
                continue
            if (
                edge["contract"] not in contracts
                or contracts[edge["contract"]]["kind"] != edge["flow"]
            ):
                errors.append("edge contract kind mismatch: " + edge["id"])
            if not edge["channels"] or not set(edge["channels"]) <= indexed["channels"].keys():
                errors.append("missing edge channel: " + edge["id"])
            if not any(
                (a, edge["flow"], b) in permitted
                for a in vertices[edge["source"]]["roles"]
                for b in vertices[edge["target"]]["roles"]
            ):
                errors.append("forbidden role/flow triple: " + edge["id"])
        for kind in ("components", "contracts", "channels"):
            for item in manifest[kind]:
                if not item.get("sources"):
                    errors.append("missing source binding: " + item["id"])
                for name in item.get("sources", []):
                    path = (ROOT / name).resolve()
                    if not path.is_relative_to(ROOT) or not path.is_file():
                        errors.append("source missing or outside repository: " + name)
    except (KeyError, TypeError, ValueError) as exc:
        return [
            result(
                "C01.typed_graph",
                "unknown",
                "Cannot interpret mapping: " + type(exc).__name__,
                scope=scope,
            ),
            result("C02.required_routes", "unknown", "Malformed graph", scope=scope),
        ]
    checks = [
        result(
            "C01.typed_graph",
            "fail" if errors else "pass",
            "Typed signatures, roles, contracts, channels and source bindings",
            scope=scope,
            evidence=errors,
        )
    ]
    route_errors = []
    try:
        declared = {r["id"]: r for r in manifest.get("required_routes", [])}
        if not reference["required_routes"]:
            raise ValueError("empty reference routes")
        for route in reference["required_routes"]:
            actual = declared.get(route["id"])
            if (
                actual is None
                or any(actual.get(k) != route[k] for k in ("source", "target", "flow"))
                or set(actual.get("must_include_roles", [])) != set(route["must_include_roles"])
            ):
                route_errors.append("missing or altered requirement: " + route["id"])
            if (
                route["source"] not in vertices
                or route["target"] not in vertices
                or not reachable(
                    manifest["edges"], route["source"], route["target"], route["flow"], set()
                )
            ):
                route_errors.append("required route absent: " + route["id"])
                continue
            for role in route["must_include_roles"]:
                blocked = {v["id"] for v in manifest["vertices"] if role in v["roles"]}
                if reachable(
                    manifest["edges"], route["source"], route["target"], route["flow"], blocked
                ):
                    route_errors.append("role bypass: " + route["id"] + " / " + role)
        checks.append(
            result(
                "C02.required_routes",
                "fail" if route_errors else "unknown" if errors else "pass",
                "Nonempty pinned routes and absence of declared namespace/executor bypasses",
                scope=scope,
                evidence=route_errors,
            )
        )
    except (KeyError, TypeError, ValueError):
        checks.append(
            result(
                "C02.required_routes", "unknown", "Cannot interpret required routes", scope=scope
            )
        )
    try:
        placement = manifest["placement"]
        hosts = set(placement["host_set"])
        assignments = placement["component_hosts"]
        if not hosts:
            raise ValueError("missing hosts")
        missing = [
            c["id"]
            for c in manifest["components"]
            if "deployment" in c
            and (not assignments.get(c["id"]) or not set(assignments[c["id"]]) <= hosts)
        ]
        checks.append(
            result(
                "C13.declared_placement",
                "fail" if missing else "pass",
                "Every deployed component has a nonempty declared host set",
                scope=scope,
                evidence=missing,
            )
        )
    except (KeyError, TypeError, ValueError):
        checks.append(
            result("C13.declared_placement", "unknown", "Placement mapping incomplete", scope=scope)
        )
    checks.append(
        result(
            "C13.runtime_placement",
            "unknown",
            "Runtime build/placement and distributed functional evidence not supplied",
            scope="runtime_deployment",
            executed=False,
        )
    )
    return checks


def check_bindings(
    manifest: dict[str, Any], reference: dict[str, Any], policies: list[dict[str, Any]]
) -> list[Check]:
    errors = []
    if not policies:
        return [
            result(
                "C03.selected_commands",
                "unknown",
                "Selected policy data missing",
                scope="selected_command_configuration",
            )
        ]
    try:
        configured = {p["uns_topic"]: p for p in policies}
        if len(configured) != len(policies):
            errors.append("duplicate policy topic bindings")
        native = {
            t["uns_topic"]: t
            for t in yaml.safe_load(
                (
                    ROOT
                    / "deploy/k3s/base/actuation-signal-publisher/native-actuation-targets.yaml"
                ).read_text()
            )["targets"]
        }
        mapping = yaml.safe_load((ROOT / "config/opcua-node-to-uns-mapping.yaml").read_text())[
            "mappings"
        ]
        aliases = {
            n["alias"]
            for n in yaml.safe_load((ROOT / "config/opcua-node-list.yaml").read_text())["nodes"]
        }
        actual = {c["id"]: c for c in manifest["controls"]}
        for control in reference["controls"]:
            if actual.get(control["id"]) != control:
                errors.append("control mapping changed: " + control["id"])
            for topic in control["command_topics"]:
                path = parse_uns_topic(topic)
                if topic not in configured:
                    errors.append("unregistered policy topic: " + topic)
                if control["protocol"] == "opcua":
                    if not any(
                        all(
                            m[k] == getattr(path, k)
                            for k in ("area", "line", "cell", "equipment", "measurement")
                        )
                        and m["node_id"] in aliases
                        for m in mapping
                    ):
                        errors.append("unregistered OPC UA command: " + topic)
                elif (
                    topic not in native
                    or native[topic]["protocol"] != control["protocol"]
                    or native[topic]["host"] != control["physical_component"]
                ):
                    errors.append("native binding mismatch: " + topic)
        command_contract = next(c for c in manifest["contracts"] if c["id"] == "actuation-request")
        if command_contract["kind"] != "command" or command_contract["wire_version"] != "1.0":
            errors.append("unsupported selected command contract")
        return [
            result(
                "C03.selected_commands",
                "fail" if errors else "pass",
                "Selected topic syntax, registered policy/native mappings and declared command version",
                scope="selected_command_configuration",
                evidence=errors,
            ),
            result(
                "C03.full_semantics",
                "unknown",
                "Industrial identity/descriptions, all contracts and receiver compatibility remain partial",
                scope="all_selected_flows",
                executed=False,
            ),
        ]
    except (OSError, KeyError, TypeError, ValueError, StopIteration):
        return [
            result(
                "C03.selected_commands",
                "unknown",
                "Cannot load complete selected command bindings",
                scope="selected_command_configuration",
            )
        ]


def aware(value: Any) -> datetime:
    at = datetime.fromisoformat(value)
    if at.tzinfo is None:
        raise ValueError("naive time")
    return at


def binding(record: dict[str, Any]) -> str:
    return json.dumps(
        [
            record["requester"],
            record["target"],
            record["value_type"],
            type(record["requested_value"]).__name__,
            record["requested_value"],
            aware(record["requested_at"]).astimezone(UTC).isoformat(),
            aware(record["deadline"]).astimezone(UTC).isoformat(),
        ],
        sort_keys=True,
        allow_nan=False,
    )


def uncertainty(record: dict[str, Any]) -> float | None:
    value = record.get("clock_uncertainty_s")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            numeric = float(value)
            if math.isfinite(numeric) and numeric >= 0:
                return numeric
        except OverflowError:
            pass
    return None


def before(first: dict[str, Any], second: dict[str, Any]) -> bool | None:
    if first.get("clock_domain") and first.get("clock_domain") == second.get("clock_domain"):
        if first.get("monotonic_after_ns", first["monotonic_ns"]) < second.get(
            "monotonic_before_ns", second["monotonic_ns"]
        ):
            return True
        if first.get("monotonic_before_ns", first["monotonic_ns"]) > second.get(
            "monotonic_after_ns", second["monotonic_ns"]
        ):
            return False
        return None
    if (
        first["component"] == second["component"]
        and first["process_instance"] == second["process_instance"]
    ):
        return bool(first["monotonic_ns"] < second["monotonic_ns"])
    a, b = uncertainty(first), uncertainty(second)
    if a is None or b is None:
        return None
    delta = (aware(second["recorded_at"]) - aware(first["recorded_at"])).total_seconds()
    return True if delta > a + b else False if delta < -(a + b) else None


def within_window(a: dict[str, Any], b: dict[str, Any], seconds: float) -> bool | None:
    if a["component"] == b["component"] and a["process_instance"] == b["process_instance"]:
        return bool(abs(a["monotonic_ns"] - b["monotonic_ns"]) <= seconds * 1e9)
    u, v = uncertainty(a), uncertainty(b)
    if u is None or v is None:
        return None
    gap = abs((aware(a["recorded_at"]) - aware(b["recorded_at"])).total_seconds())
    return True if gap + u + v <= seconds else False if gap - u - v > seconds else None


def valid_policy(record: dict[str, Any], policies: list[dict[str, Any]]) -> bool:
    policy = next((p for p in policies if p["uns_topic"] == record["target"]), None)
    if (
        policy is None
        or record["requester"] not in policy["allowlist"]
        or record["value_type"] != policy.get("value_type", "double")
    ):
        return False
    value = record["requested_value"]
    expected = {"bool": (bool,), "int64": (int,), "double": (int, float)}.get(
        policy.get("value_type", "double"), ()
    )
    return (
        type(value) in expected
        and policy["allowed_range"][0] <= value <= policy["allowed_range"][1]
        and math.isfinite(value)
    )


def check_execution(
    records: list[dict[str, Any]], reference: dict[str, Any], policies: list[dict[str, Any]]
) -> list[Check]:
    checks = []
    clean = []
    seen: dict[str, str] = {}
    issues = []
    conflicts = []
    for row in records:
        try:
            if row["event"] != "command_execution" or row["execution_schema"] != "1.0":
                raise ValueError("unsupported record")
            for key in ("record_id", "component", "action", "correlation_id", "process_instance"):
                if not isinstance(row[key], str) or not row[key]:
                    raise ValueError("missing identity")
            if type(row["monotonic_ns"]) is not int or row["monotonic_ns"] < 0:
                raise ValueError("invalid monotonic time")
            if not is_valid_correlation_id(row["correlation_id"]):
                raise ValueError("invalid command ID")
            aware(row["recorded_at"])
            normalized = json.dumps(
                {k: v for k, v in row.items() if k != "source"}, sort_keys=True, allow_nan=False
            )
            if row["record_id"] in seen:
                if seen[row["record_id"]] != normalized:
                    conflicts.append(row["record_id"])
                continue
            seen[row["record_id"]] = normalized
            clean.append(row)
        except (KeyError, TypeError, ValueError):
            issues.append("Invalid or incomplete execution record")
    checks.append(
        result(
            "C12.record_integrity",
            "fail" if conflicts else "unknown" if issues or not clean else "pass",
            "Parseable attributable records; identical record IDs deduplicated",
            scope="supplied_records",
            evidence=conflicts + issues,
        )
    )
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in clean:
        grouped[row["correlation_id"]].append(row)
    for command_id, rows in grouped.items():
        actions = Counter(r["action"] for r in rows)
        required = {
            "request_received",
            "pipeline_received",
            "validation_decided",
            "outcome_published",
        }
        if any(r["action"] == "writer_result" for r in rows):
            required.add("writer_result")
        if any(
            r["action"] == "writer_result"
            and r.get("outcome") == "completed"
            and not r.get("replayed")
            for r in rows
        ):
            required.update(("executor_admitted", "write_initiated", "device_acknowledged"))
        missing = sorted(required - actions.keys())
        checks.append(
            result(
                "C12.stage_coverage",
                "unknown" if missing else "pass",
                "Required observed stages for this command; counts retained separately from completeness",
                scope="supplied_records",
                evidence=[command_id, {"missing_actions": missing, "action_counts": dict(actions)}],
            )
        )
        writes = [r for r in rows if r["action"] == "write_initiated"]
        for write in writes:
            ev = [command_id, write["record_id"]]
            try:
                matches = [
                    r
                    for r in rows
                    if r["action"] == "executor_admitted"
                    and r["component"] == reference["writer"]
                    and r["process_instance"] == write["process_instance"]
                    and r.get("attempt_id")
                    and r.get("attempt_id") == write.get("attempt_id")
                    and binding(r) == binding(write)
                    and before(r, write) is True
                ]
                decisions = [
                    r
                    for r in rows
                    if r["component"] == reference["validator"]
                    and r["action"] == "validation_decided"
                ]
                admitted = matches[0] if len(matches) == 1 else None
                if admitted is not None:
                    try:
                        aware(admitted["approval_issued_at"])
                    except (KeyError, TypeError, ValueError):
                        admitted = None
                approvals = [
                    r
                    for r in decisions
                    if r.get("decision") == "approve"
                    and r.get("approved_writer") == reference["writer"]
                    and binding(r) == binding(write)
                    and admitted is not None
                    and bool(r.get("approval_issued_at"))
                    and r.get("approval_issued_at") == admitted.get("approval_issued_at")
                ]
                policy_ok = write["component"] == reference["writer"] and valid_policy(
                    write, policies
                )
                checks.append(
                    result(
                        "C07.write_policy",
                        "unknown"
                        if not policies and write["component"] == reference["writer"]
                        else "pass"
                        if policy_ok
                        else "fail",
                        "Observed writer, target, authority allowlist, types and parameters under pinned configuration",
                        scope="trusted_component_records",
                        evidence=ev,
                    )
                )
                ordered = [r for r in approvals if before(r, write) is True]
                # Verified receipt of the identical signed ticket is causal evidence,
                # even when cloud and edge clocks do not share a domain.
                causal = [
                    r
                    for r in approvals
                    if admitted is not None
                    and write.get("approval_verified") is True
                    and admitted.get("approval_verified") is True
                    and r.get("ingress_verified") is True
                    and isinstance(write.get("approval_fingerprint"), str)
                    and len(write["approval_fingerprint"]) == 64
                    and r.get("approval_fingerprint")
                    == write["approval_fingerprint"]
                    == admitted.get("approval_fingerprint")
                    and isinstance(write.get("command_digest"), str)
                    and len(write["command_digest"]) == 64
                    and r.get("command_digest")
                    == write["command_digest"]
                    == admitted.get("command_digest")
                ]
                admission_status: Status = (
                    "pass" if admitted is not None and (ordered or causal) else "unknown"
                )
                # A known later refusal supersedes the specific approval used by this writer.
                if ordered and any(
                    r.get("decision") == "reject"
                    and binding(r) == binding(write)
                    and before(ordered[0], r) is True
                    and before(r, write) is True
                    for r in decisions
                ):
                    admission_status = "fail"
                if any(
                    before(r, write) is None
                    and not (
                        causal
                        and r.get("decision") == "approve"
                        and binding(r) == binding(write)
                        and r.get("approved_writer") == reference["writer"]
                    )
                    for r in decisions
                ):
                    admission_status = "unknown" if admission_status != "fail" else "fail"
                checks.append(
                    result(
                        "C07.admission",
                        admission_status,
                        "Matching writer admission and attributable prior validator approval; ambiguous ordering remains unknown",
                        scope="trusted_component_records",
                        evidence=ev,
                    )
                )
                deadline = aware(write["deadline"])
                u = uncertainty(write)
                at = aware(write["recorded_at"])
                status: Status = "unknown"
                if u is not None:
                    remaining = (deadline - at).total_seconds()
                    status = "pass" if remaining >= u else "fail" if remaining < -u else "unknown"
                checks.append(
                    result(
                        "C08.deadline",
                        status,
                        "UTC write-initiation interval compared with bound deadline",
                        scope="observed_write_boundary",
                        evidence=ev,
                    )
                )
            except (KeyError, TypeError, ValueError):
                checks.append(
                    result(
                        "C07.admission",
                        "unknown",
                        "Admission or command evidence incomplete",
                        scope="supplied_records",
                        evidence=ev,
                    )
                )
                checks.append(
                    result(
                        "C08.deadline",
                        "unknown",
                        "Deadline or initiation evidence incomplete",
                        scope="supplied_records",
                        evidence=ev,
                    )
                )
        if not writes:
            checks.extend(
                result(
                    name,
                    "unknown",
                    "No observed write initiation in this command trace",
                    scope="supplied_records",
                    evidence=[command_id],
                )
                for name in ("C07.admission", "C08.deadline")
            )
        count_states: list[str] = []
        for a, b in itertools.combinations(writes, 2):
            inside = within_window(a, b, reference["retention_seconds"])
            count_states.append("fail" if inside is True else "unknown")
        checks.append(
            result(
                "C09.initiations",
                verdict(count_states or (["pass"] if writes else ["unknown"])),
                "At most one observed application initiation per ID within the declared retention interval; native frames are not separate initiations",
                scope="observed_initiations_only",
                evidence=[
                    command_id,
                    {
                        "observed_initiations": len(writes),
                        "retention_seconds": reference["retention_seconds"],
                    },
                ],
            )
        )
        bound = [
            r
            for r in rows
            if r["action"] in ("executor_admitted", "write_initiated")
            or (r["action"] == "writer_result" and r.get("outcome") == "completed")
        ]
        states = []
        try:
            for a, b in itertools.combinations(bound, 2):
                if binding(a) != binding(b):
                    inside = within_window(a, b, reference["retention_seconds"])
                    states.append("fail" if inside is True else "unknown")
            checks.append(
                result(
                    "C09.binding",
                    verdict(states or (["pass"] if bound else ["unknown"])),
                    "Admitted/executed target, value, type, requester and times remain immutable within retention; declined conflicting requests are excluded",
                    scope="recorded_command_fields",
                    evidence=[command_id],
                )
            )
        except (KeyError, TypeError, ValueError):
            checks.append(
                result(
                    "C09.binding",
                    "unknown",
                    "Immutable command fields incomplete",
                    scope="supplied_records",
                    evidence=[command_id],
                )
            )
        outcomes = [r for r in rows if r["action"] in ("writer_result", "outcome_published")]
        bad = []
        for row in outcomes:
            outcome = row.get("outcome")
            permitted = (
                ("completed", "not_executed", "pending", "unknown", "conflict")
                if row["action"] == "writer_result"
                else ("approve", "reject", "pending", "unknown")
            )
            if outcome not in permitted:
                bad.append(row["record_id"])
            if outcome in ("completed", "approve"):
                try:
                    aware(row["written_at"])
                except (KeyError, TypeError, ValueError):
                    bad.append(row["record_id"])
            if outcome == "reject" and row.get("reason") in (
                "write_timeout",
                "write_transport_error",
                "bad_write_reply",
                "ledger_unavailable",
                "ledger_failure",
            ):
                bad.append(row["record_id"])
        checks.append(
            result(
                "C10.outcome_meaning",
                "fail" if bad else "pass" if outcomes else "unknown",
                "Typed outcomes: uncertainty is not known refusal; acknowledged completion needs its own timestamp",
                scope="reported_outcome_representation",
                evidence=[command_id, *bad],
            )
        )
        for row in rows:
            if row["action"] == "process_observed":
                try:
                    reject_topics = next(
                        c["command_topics"] for c in reference["controls"] if c["id"] == "reject"
                    )
                    ok = (
                        row["component"] == reference["process_observer"]
                        and isinstance(row.get("inspection_id"), str)
                        and bool(row.get("inspection_id"))
                        and row["target"] in reject_topics
                        and row.get("evidence_scope") == "simulator_feedback"
                        and all(
                            type(row[key]) is int
                            for key in (
                                "observed_bottle",
                                "observed_count",
                                "baseline_count",
                                "requested_value",
                            )
                        )
                        and row["observed_bottle"] == row["requested_value"]
                        and row["observed_count"] > row["baseline_count"]
                        and aware(row["bottle_observed_at"]) >= aware(row["requested_at"])
                        and aware(row["count_observed_at"]) >= aware(row["requested_at"])
                    )
                    checks.append(
                        result(
                            "C06.feedback",
                            "pass" if ok else "fail",
                            "Recorded bottle-specific counter/identity feedback, not transport acknowledgement",
                            scope="reported_simulator_feedback",
                            evidence=[command_id, row["record_id"]],
                        )
                    )
                except (KeyError, TypeError, ValueError, StopIteration):
                    checks.append(
                        result(
                            "C06.feedback",
                            "unknown",
                            "Process feedback fields incomplete",
                            scope="supplied_records",
                            evidence=[command_id],
                        )
                    )
    if not grouped:
        for name in (
            "C07.admission",
            "C08.deadline",
            "C09.initiations",
            "C09.binding",
            "C10.outcome_meaning",
        ):
            checks.append(
                result(
                    name,
                    "unknown",
                    "No usable command trace supplied",
                    scope="runtime_evidence",
                    executed=False,
                )
            )
    checks.append(
        result(
            "C09.complete_history",
            "unknown",
            "Required restart/retention boundary cases and complete initiation history are not established by this package",
            scope="selected_recovery_policy",
            executed=False,
        )
    )
    return checks


def evaluate(
    manifest: dict[str, Any],
    reference: dict[str, Any],
    *,
    records: list[dict[str, Any]],
    policies: list[dict[str, Any]],
    collection_errors: list[str],
    commands: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    checks = (
        check_structure(manifest, reference)
        + check_bindings(manifest, reference, policies)
        + check_execution(records, reference, policies)
    )
    deferred = {
        "C04": "Equivalent native-input golden data and output comparisons not supplied; E2 preparation remains",
        "C05": "State ordering/restart sequences and freshness policy evidence not supplied",
        "C06": "Transport/event/effect reconciliation across retention and restart cases not supplied",
        "C10": "Loss-of-reply and independent device/outcome reconciliation case evidence not supplied",
        "C11": "Loss, congestion, reconnect and mixed-flow policy experiment evidence not supplied",
    }
    checks.extend(
        result(
            rule + ".experiment",
            "unknown",
            reason,
            scope="required_experimental_evidence",
            executed=False,
        )
        for rule, reason in deferred.items()
    )
    checks.append(
        result(
            "C02.runtime_boundary",
            "unknown",
            "Declared graph is not proof of runtime bypass protection; separate unapproved-writer case evidence required",
            scope="runtime_boundary",
            executed=False,
        )
    )
    checks.append(
        result(
            "C12.collection_coverage",
            "unknown",
            "Collection completeness is not established; missing records and unbounded clocks remain explicit",
            scope="bounded_run",
            evidence=collection_errors,
        )
    )
    if commands:
        expected = {
            c["correlation_id"] for c in commands if isinstance(c.get("correlation_id"), str)
        }
        present = {r.get("correlation_id") for r in records}
        checks.append(
            result(
                "C12.command_reconciliation",
                "unknown" if expected - present else "pass",
                "Generated command IDs compared with retained trace IDs; matching IDs do not prove full stage coverage",
                scope="supplied_run_commands",
                evidence=sorted(expected - present),
            )
        )
    rules = []
    for rule in reference["rules"]:
        selected = [c for c in checks if c["rule"] == rule["id"]]
        rules.append(
            {
                **rule,
                "status": verdict([c["status"] for c in selected]),
                "execution": "executed"
                if any(c["execution"] == "executed" for c in selected)
                else "not_run",
                "checks": selected,
            }
        )
    return {
        "profile": reference["id"],
        "evaluated_at": datetime.now(UTC).isoformat(),
        "overall": verdict([r["status"] for r in rules]),
        "universal_conformance": "not_established",
        "scope": "Finite checks of declared structure, selected configuration and supplied observations only",
        "trust_assumptions": "Supplied records and pinned configurations are trusted evidence inputs; signatures and deployed builds are not independently attested by log inspection",
        "rules": rules,
    }


def read_run(
    path: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str], list[Path]]:
    records = []
    commands = []
    errors = []
    files = []
    for name in ("summary.json", "journal.jsonl"):
        source = path / name
        if not source.is_file():
            errors.append("missing " + name)
            continue
        files.append(source)
        try:
            if name == "summary.json":
                summary = json.loads(source.read_text(encoding="utf-8"))
                if not isinstance(summary, dict):
                    raise ValueError("invalid summary shape")
                if summary.get("outcome") != "passed":
                    errors.append(
                        "scenario outcome is not passed; interpret partial work explicitly"
                    )
            else:
                for line in source.read_text(encoding="utf-8").splitlines():
                    item = json.loads(line)
                    if not isinstance(item, dict):
                        raise ValueError("invalid journal shape")
                    if item.get("event") == "command_submitting":
                        command = item.get("command")
                        if not isinstance(command, dict) or not isinstance(
                            command.get("correlation_id"), str
                        ):
                            raise ValueError("invalid command identity")
                        commands.append(command)
        except (ValueError, KeyError, TypeError):
            errors.append("invalid " + name)
    ids = {
        c["correlation_id"]
        for c in commands
        if isinstance(c, dict) and isinstance(c.get("correlation_id"), str)
    }
    for command_id in sorted(ids):
        # IDs must remain a single safe directory name, never an input path.
        if not command_id.isalnum():
            errors.append("invalid command ID directory")
            continue
        directory = path / "traces" / command_id
        for name in ("summary.json", "records.jsonl"):
            source = directory / name
            if not source.is_file():
                errors.append(command_id + ": missing " + name)
                continue
            files.append(source)
            try:
                if name == "summary.json":
                    trace_summary = json.loads(source.read_text(encoding="utf-8"))
                    if not isinstance(trace_summary, dict):
                        raise ValueError("invalid trace summary")
                    errors.extend(
                        command_id + ": " + str(e)
                        for e in trace_summary.get("collection_errors", [])
                    )
                else:
                    for line in source.read_text(encoding="utf-8").splitlines():
                        row = json.loads(line)
                        if not isinstance(row, dict) or row.get("correlation_id") != command_id:
                            errors.append(command_id + ": trace identity mismatch")
                            continue
                        records.append(row)
            except (ValueError, KeyError, TypeError):
                errors.append(command_id + ": invalid " + name)
    return records, commands, errors, files


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=MANIFEST)
    parser.add_argument("--reference", type=Path, default=REFERENCE)
    parser.add_argument(
        "--run", type=Path, help="Scenario evidence directory with journal and collected traces"
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    output = args.output or ROOT / "results/conformance" / datetime.now(UTC).strftime(
        "%Y-%m-%dT%H-%M-%S-%fZ"
    )
    if output.exists():
        parser.error("output must be a new directory")
    try:
        reference = json.loads(args.reference.read_text(encoding="utf-8"))
        if reference["schema_version"] != "1.0" or {r["id"] for r in reference["rules"]} != {
            f"C{i:02}" for i in range(1, 14)
        }:
            raise ValueError("unsupported reference")
        profile_signature(reference["profile"])
        if (
            not reference["required_routes"]
            or not math.isfinite(reference["retention_seconds"])
            or reference["retention_seconds"] <= 0
        ):
            raise ValueError("incomplete reference constraints")
    except (OSError, ValueError, KeyError, TypeError):
        parser.error("a valid pinned reference profile is required")
    input_errors = []
    try:
        manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
        if not isinstance(manifest, dict):
            raise ValueError("invalid manifest shape")
    except (OSError, ValueError):
        manifest = {}
        input_errors.append("manifest unavailable or invalid JSON object")
    policies_path = ROOT / "deploy/k3s/base/actuation-event-validator/actuation-policy.yaml"
    try:
        policies = yaml.safe_load(policies_path.read_text(encoding="utf-8"))["policies"]
    except (OSError, KeyError, TypeError, yaml.YAMLError):
        policies = []
        input_errors.append("selected policy unavailable")
    records, commands, errors, files = (
        read_run(args.run) if args.run else ([], [], ["No runtime package supplied"], [])
    )
    report = evaluate(
        manifest,
        reference,
        records=records,
        policies=policies,
        collection_errors=errors + input_errors,
        commands=commands,
    )
    report["input_errors"] = input_errors
    files.extend(
        [
            args.reference,
            args.manifest,
            policies_path,
            ROOT / "deploy/k3s/base/actuation-signal-publisher/native-actuation-targets.yaml",
            ROOT / "config/opcua-node-to-uns-mapping.yaml",
            ROOT / "config/opcua-node-list.yaml",
            Path(__file__).resolve(),
        ]
    )
    report["inputs"] = [
        {
            "path": str(p.resolve()),
            "sha256": hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else None,
        }
        for p in dict.fromkeys(files)
    ]
    report["evaluator_revision"] = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, timeout=10
    ).strip()
    report["evaluator_worktree_dirty"] = bool(
        subprocess.check_output(
            ["git", "status", "--porcelain"], cwd=ROOT, text=True, timeout=10
        ).strip()
    )
    report["counts"] = dict(Counter(r["status"] for r in report["rules"]))
    output.mkdir(parents=True)
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    lines = [
        "# Scoped E0 conformance report",
        "",
        f"Overall: **{report['overall']}**. Universal conformance: not established.",
        "",
        report["scope"],
        "",
        "| Rule | Verdict | Execution | Check |",
        "| --- | --- | --- | --- |",
    ]
    lines.extend(
        f"| {r['id']} | {r['status']} | {r['execution']} | {r['title']} |" for r in report["rules"]
    )
    lines.extend(
        [
            "",
            "See report.json for each subcheck, evidence references, unknown reasons and input fingerprints.",
            "",
            "A failed deliberate-violation fixture is an expected checker detection, not a live-system failure. Missing evidence is unknown; not-run experiments are marked separately.",
        ]
    )
    lines.extend(["", "## Reasons requiring further evidence", ""])
    for rule in report["rules"]:
        reasons = list(
            dict.fromkeys(c["reason"] for c in rule["checks"] if c["status"] == "unknown")
        )
        if reasons:
            lines.append("- **" + rule["id"] + ":** " + "; ".join(reasons))
    (output / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "overall": report["overall"],
                "counts": report["counts"],
                "report": str(output / "report.json"),
            },
            indent=2,
        )
    )
    raise SystemExit(
        1 if report["overall"] == "fail" else 2 if report["overall"] == "unknown" else 0
    )


if __name__ == "__main__":
    main()
