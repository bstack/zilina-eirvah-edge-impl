"""Offline C05/C06 checks against a prospective finite consumer-case oracle.

No runtime implementation is imported here. An injected invalid input passing its
refusal test does not make that input conformant. Real transport/effects remain separate.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.ulid import is_valid_correlation_id

ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "config/architecture/state-event-policy.json"
PLAN = ROOT / "config/scenarios/state-events.json"
MODE = "in_process_virtual_time_no_transport_no_actuation"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def check(
    name: str, status: str, reason: str, evidence: list[Any] | None = None, *, executed: bool = True
) -> dict[str, Any]:
    return dict(
        rule=name.split(".")[0],
        check=name,
        status=status,
        reason=reason,
        scope="selected_production_consumer_replay",
        execution="executed" if executed else "not_run",
        evidence=evidence or [],
    )


def source_attribution(package: dict[str, Any]) -> bool:
    hashes = package.get("source_sha256")
    required = (
        {"scripts/state_event_case.py", "scripts/data_conformance.py"}
        | {
            "services/decision-agent-stub/src/decision_agent_stub/" + name
            for name in ("service.py", "state.py", "inspection.py", "config.py")
        }
        | {"libs/eirvah-contracts/src/eirvah_contracts/inspection.py"}
    )
    return (
        isinstance(hashes, dict)
        and required.issubset(hashes)
        and all(
            isinstance(v, str) and len(v) == 64 and all(c in "0123456789abcdef" for c in v)
            for v in hashes.values()
        )
    )


def check_data_evidence(package: dict[str, Any] | None) -> list[dict[str, Any]]:
    policy = json.loads(POLICY.read_text(encoding="utf-8"))
    plan = json.loads(PLAN.read_text(encoding="utf-8"))
    if (
        not isinstance(package, dict)
        or package.get("schema_version") != "1.0"
        or package.get("execution_mode") != MODE
        or package.get("policy_sha256") != digest(POLICY)
        or package.get("plan_sha256") != digest(PLAN)
        or not isinstance(package.get("cases"), list)
        or not source_attribution(package)
    ):
        return [
            check(
                r + ".data_evidence",
                "unknown",
                "Valid policy-bound consumer evidence not supplied",
                executed=False,
            )
            for r in ("C05", "C06")
        ]
    try:
        base = datetime.fromisoformat(package["virtual_origin"])
        if base.tzinfo is None:
            raise ValueError("naive origin")
    except (ValueError, KeyError, TypeError):
        return [
            check(
                "C05.data_evidence", "unknown", "Injected clock origin unavailable", executed=False
            )
        ]
    by_id: dict[str, dict[str, Any]] = {}
    checks = []
    for case in package["cases"]:
        if not isinstance(case, dict) or not isinstance(case.get("id"), str):
            checks.append(check("C12.data_integrity", "unknown", "Malformed consumer case"))
            continue
        if case["id"] in by_id:
            checks.append(
                check(
                    "C12.data_integrity", "fail", "Duplicated consumer case identity", [case["id"]]
                )
            )
        by_id[case["id"]] = case
    if set(by_id) - set(policy["required_cases"]):
        checks.append(
            check(
                "C12.data_integrity",
                "fail",
                "Unpinned cases in evidence",
                sorted(set(by_id) - set(policy["required_cases"])),
            )
        )
    command_ids: set[str] = set()
    for expected in plan["cases"]:
        name = ("C05." if expected["kind"] == "state" else "C06.") + expected["id"]
        actual = by_id.get(expected["id"])
        if actual is None or not isinstance(actual.get("records"), list):
            checks.append(
                check(name, "unknown", "Required consumer case not captured", executed=False)
            )
            continue
        if len(actual["records"]) != len(expected["steps"]):
            checks.append(
                check(
                    name,
                    "unknown" if len(actual["records"]) < len(expected["steps"]) else "fail",
                    "Case sequence incomplete or contains extra records",
                )
            )
            continue
        errors = []
        gaps = []
        instance = None
        seen_instances = set()
        latest_at = None
        for index, (step, record) in enumerate(
            zip(expected["steps"], actual["records"], strict=True)
        ):
            # This is the oracle's expected timestamp, independent of which
            # observations survived capture. A missing earlier row must not turn
            # a later correct snapshot into a manufactured counterexample.
            if expected["kind"] == "state":
                if step["op"] == "consumer_restart":
                    latest_at = None
                elif step["op"] == "receive" and step["expect"]["accepted"]:
                    latest_at = (base + timedelta(seconds=step["source_s"])).isoformat()
            try:
                if not isinstance(record, dict) or "command" not in record:
                    raise KeyError("missing decision record")
                required_input = {k: v for k, v in step.items() if k != "expect"}
                topic = (
                    policy["state"]["topic_bindings"][expected["stream"]]
                    if expected["kind"] == "state"
                    else policy["inspection"]["topic"]
                )
                if record["topic"] != topic:
                    errors.append(f"step {index}: topic binding mismatch")
                if record["input"] != required_input:
                    errors.append(f"step {index}: input differs from pinned case")
                now = base + timedelta(seconds=step["now_s"])
                if record["now"] != now.isoformat():
                    errors.append(f"step {index}: evaluation time mismatch")
                current_instance = record["consumer_instance"]
                if not isinstance(current_instance, str) or not current_instance:
                    raise ValueError("missing instance")
                if step["op"] == "consumer_restart":
                    if current_instance in seen_instances:
                        errors.append(
                            f"step {index}: consumer restart did not create a fresh instance"
                        )
                elif instance is not None and current_instance != instance:
                    errors.append(f"step {index}: unrecorded consumer restart")
                instance = current_instance
                seen_instances.add(instance)
                oracle = step["expect"]
                if step["op"] == "receive":
                    decision = record["decision"]
                    if not isinstance(decision, dict) or not {"accepted", "reason"}.issubset(
                        decision
                    ):
                        raise KeyError("missing decision fields")
                    if (
                        decision.get("accepted") is not oracle["accepted"]
                        or decision.get("reason") != oracle["reason"]
                    ):
                        errors.append(f"step {index}: consumer decision violates selected policy")
                    at = base + timedelta(seconds=step["source_s"])
                    payload = record["payload"]
                    if not isinstance(payload, dict):
                        raise KeyError("missing payload")
                    if expected["kind"] == "state":
                        if (
                            payload.get("sosa:hasSimpleResult") != step["value"]
                            or payload.get("sosa:resultTime") != at.isoformat()
                            or payload.get("eirvah:quality") != step["quality"]
                        ):
                            errors.append(f"step {index}: state payload/input mismatch")
                    else:
                        identity = f"{step['session']}:{step['sequence']:04x}"
                        if (
                            payload.get("inspection_id") != identity
                            or payload.get("bottle_id") != step["bottle_id"]
                            or payload.get("passed") is not step["passed"]
                            or payload.get("inspected_at") != at.isoformat()
                        ):
                            errors.append(f"step {index}: inspection payload/input mismatch")
                        if "late_sequence" not in decision:
                            raise KeyError("missing late event diagnostic")
                        if decision["late_sequence"] is not oracle["late_sequence"]:
                            errors.append(f"step {index}: late event diagnostic incorrect")
                        if "missing_sequences" not in decision:
                            raise KeyError("missing coverage diagnostic")
                        if decision.get("missing_sequences") != oracle["missing_sequences"]:
                            errors.append(f"step {index}: missing snapshot diagnostic incorrect")
                command = record.get("command")
                if oracle["command_value"] is None:
                    if command is not None:
                        errors.append(f"step {index}: unexpected command decision")
                elif command is None:
                    errors.append(f"step {index}: expected decision absent")
                else:
                    request = ActuationRequest.model_validate(command)
                    if (
                        request.requested_value != oracle["command_value"]
                        or request.requested_at != now
                        or request.requester != "decision-agent-stub"
                        or not is_valid_correlation_id(request.correlation_id)
                    ):
                        errors.append(f"step {index}: command does not match observed work")
                    if request.correlation_id in command_ids:
                        errors.append(f"step {index}: command identity reused for another decision")
                    command_ids.add(request.correlation_id)
                    if expected["kind"] == "event":
                        deadline = base + timedelta(
                            seconds=step["source_s"] + policy["inspection"]["max_age_s"]
                        )
                        if (
                            request.deadline != deadline
                            or request.value_type != "int64"
                            or request.target_uns_topic != policy["command_topics"]["reject"]
                        ):
                            errors.append(f"step {index}: inspection/command binding mismatch")
                    elif (
                        request.value_type != "double"
                        or request.target_uns_topic != policy["command_topics"]["temperature"]
                    ):
                        errors.append(f"step {index}: state/command binding mismatch")
                if expected["kind"] == "state":
                    state = record["state"]
                    if not isinstance(state, dict) or not {
                        "status",
                        "value",
                        "source_timestamp",
                    }.issubset(state):
                        raise KeyError("missing state snapshot")
                    if (
                        state.get("status") != oracle["status"]
                        or state.get("value") != oracle["value"]
                        or state.get("source_timestamp") != latest_at
                    ):
                        errors.append(f"step {index}: latest state/freshness mismatch")
            except (KeyError, TypeError, ValueError, OverflowError):
                gaps.append(f"step {index}: evidence missing or malformed")
        checks.append(
            check(
                name,
                "fail" if errors else "unknown" if gaps else "pass",
                "Pinned finite input/refusal/restart case checked against recorded production-code decisions",
                errors + gaps or [expected["id"], len(actual["records"])],
            )
        )
    checks.extend(
        [
            check(
                "C05.source_epochs",
                "unknown",
                "Legacy timestamp-only replay supplies no deployed source epoch/sequence evidence",
                executed=False,
            ),
            check(
                "C05.deployed_restart",
                "unknown",
                "In-process recreation is not a measured pod/device restart",
                executed=False,
            ),
            check(
                "C06.business_effects",
                "unknown",
                "No native write, acknowledgement or physical rejection counter is supplied by consumer replay",
                executed=False,
            ),
            check(
                "C06.delivery_coverage",
                "unknown",
                "Consumer replay alone supplies no complete source-event and durable-receipt reconciliation",
                executed=False,
            ),
            check(
                "C06.durable_recovery",
                "unknown",
                "Legacy memory-only replay supplies no deployed durable ownership and command recovery evidence",
                executed=False,
            ),
        ]
    )
    return checks
