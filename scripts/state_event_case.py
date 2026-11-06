"""Retain reproducible C05/C06 consumer decisions without touching a cluster.

Uses production code with an injected clock. Does not send any command or claim
transport, real-time, device-restart or physical-effect evidence.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from data_conformance import MODE, PLAN, POLICY, ROOT, check_data_evidence, digest
from decision_agent_stub.config import DecisionAgentSettings
from decision_agent_stub.inspection import InspectionRule
from decision_agent_stub.service import DecisionAgentRuntime
from eirvah_contracts.inspection import BottleInspection
from prometheus_client.registry import CollectorRegistry

SOURCES = (
    [Path(__file__).resolve(), ROOT / "scripts/data_conformance.py"]
    + [
        ROOT / "services/decision-agent-stub/src/decision_agent_stub" / name
        for name in ("service.py", "inspection.py", "state.py", "config.py")
    ]
    + [
        ROOT / "libs/eirvah-contracts/src/eirvah_contracts" / name
        for name in ("inspection.py", "actuation.py")
    ]
    + [ROOT / "pyproject.toml", ROOT / "uv.lock"]
)


def capture(output: Path) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=False)
    policy = json.loads(POLICY.read_text(encoding="utf-8"))
    plan = json.loads(PLAN.read_text(encoding="utf-8"))
    base = datetime(2026, 1, 1, tzinfo=UTC)
    settings = DecisionAgentSettings(
        subscribe_topic=policy["state"]["topic_bindings"]["speed"],
        reject_count_topic=policy["state"]["topic_bindings"]["reject_count"],
        rejected_bottle_topic=policy["state"]["topic_bindings"]["rejected_bottle"],
        inspection_topic=policy["inspection"]["topic"],
        target_uns_topic=policy["command_topics"]["temperature"],
        reject_target_uns_topic=policy["command_topics"]["reject"],
        enabled=True,
        inspection_enabled=True,
        normal_speed_max=policy["state"]["normal_speed_max"],
        high_speed_min=policy["state"]["high_speed_min"],
        normal_setpoint=policy["state"]["normal_setpoint"],
        high_setpoint=policy["state"]["high_setpoint"],
        max_observation_age_s=policy["state"]["max_age_s"],
        inspection_max_age_s=policy["inspection"]["max_age_s"],
    )
    topics = {
        "speed": settings.subscribe_topic,
        "reject_count": settings.reject_count_topic,
        "rejected_bottle": settings.rejected_bottle_topic,
    }
    package: dict[str, Any] = dict(
        schema_version="1.0",
        execution_mode=MODE,
        scope=policy["scope"],
        virtual_origin=base.isoformat(),
        captured_at=datetime.now(UTC).isoformat(),
        policy_sha256=digest(POLICY),
        plan_sha256=digest(PLAN),
        source_sha256={p.relative_to(ROOT).as_posix(): digest(p) for p in SOURCES},
        clock_model="Injected aware UTC times; not measured distributed clock bounds",
        cases=[],
    )
    for source in [*SOURCES, PLAN, POLICY]:
        dest = output / "sources" / source.relative_to(ROOT)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, dest)
    for case in plan["cases"]:
        runtime = DecisionAgentRuntime(settings, registry=CollectorRegistry())
        inspections = InspectionRule(settings, capacity=policy["inspection"]["capacity"])
        instance = str(uuid.uuid4())
        rows = []
        for step in case["steps"]:
            now = base + timedelta(seconds=step["now_s"])
            command = None
            payload = None
            decision = None
            if step["op"] == "consumer_restart":
                runtime = DecisionAgentRuntime(settings, registry=CollectorRegistry())
                inspections = InspectionRule(settings, capacity=policy["inspection"]["capacity"])
                instance = str(uuid.uuid4())
            elif step["op"] == "receive":
                at = base + timedelta(seconds=step["source_s"])
                if case["kind"] == "state":
                    payload = {
                        "sosa:resultTime": at.isoformat(),
                        "sosa:hasSimpleResult": step["value"],
                        "eirvah:quality": step["quality"],
                        "eirvah:unit": "m/s" if case["stream"] == "speed" else "dimensionless",
                    }
                    command = runtime.observation(topics[case["stream"]], payload, now)
                    decision = runtime.state_decision
                else:
                    event = BottleInspection(
                        inspection_id=f"{step['session']}:{step['sequence']:04x}",
                        bottle_id=step["bottle_id"],
                        passed=step["passed"],
                        inspected_at=at,
                    )
                    payload = {**event.model_dump(mode="json"), "inspected_at": at.isoformat()}
                    command = inspections.update(event, now)
                    decision = dict(inspections.last_decision)
            rows.append(
                dict(
                    topic=topics[case["stream"]]
                    if case["kind"] == "state"
                    else settings.inspection_topic,
                    input={k: v for k, v in step.items() if k != "expect"},
                    now=now.isoformat(),
                    consumer_instance=instance,
                    payload=payload,
                    decision=decision,
                    command=command.model_dump(mode="json") if command else None,
                    state=runtime.state_snapshot(now)[topics[case["stream"]]]
                    if case["kind"] == "state"
                    else None,
                )
            )
        package["cases"].append(dict(id=case["id"], records=rows))
        (output / "evidence.json").write_text(
            json.dumps(package, indent=2) + "\n", encoding="utf-8"
        )
    return package


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    output = args.output or ROOT / "results/state-event-e0" / datetime.now(UTC).strftime(
        "%Y-%m-%dT%H-%M-%S-%fZ"
    )
    if output.exists():
        parser.error("output must be a new directory")
    package = capture(output)
    checks = check_data_evidence(package)
    required = set(json.loads(POLICY.read_text(encoding="utf-8"))["required_cases"])
    selected = [
        c["status"] for c in checks if c["check"].split(".", 1)[1] in required or c["rule"] == "C12"
    ]
    status = (
        "fail"
        if "fail" in selected
        else "unknown"
        if len(selected) < len(required) or "unknown" in selected
        else "pass"
    )
    report = dict(
        consumer_case=status,
        scope=package["scope"],
        universal_conformance="not_established",
        whole_study="unknown",
        checks=checks,
        git_sha=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        worktree_dirty=bool(
            subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip()
        ),
    )
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    lines = [
        "# C05/C06 consumer replay",
        "",
        f"Consumer case: **{status}**. Whole study: **unknown**.",
        "",
        package["scope"],
        "",
        "| Check | Verdict |",
        "| --- | --- |",
    ]
    lines.extend(f"| {c['check']} | {c['status']} |" for c in checks)
    (output / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(
        json.dumps(
            dict(
                consumer_case=status, cases=len(package["cases"]), report=str(output / "report.md")
            ),
            indent=2,
        )
    )
    raise SystemExit(0 if status == "pass" else 1 if status == "fail" else 2)


if __name__ == "__main__":
    main()
