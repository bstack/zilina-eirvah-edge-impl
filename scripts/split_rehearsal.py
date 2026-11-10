"""Rehearse the logical split: bounded E0, automatic feedback and writer unavailability.

Runs only in the dedicated local namespaces. No private keys/credentials enter
artifacts. Namespace placement is not physical-host or network-policy isolation.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import math
import subprocess
import sys
import time
from contextlib import ExitStack
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from asyncua import Client
from configure_actuation_security import SECRET_NAME
from control import build_command, send
from e0_capture import Cluster, validate_fence
from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.authorization import command_digest
from eirvah_observability.experiment import phase
from interoperability import ROOT, digest
from scenario import port_forward
from split_deployment import CLOUD, EDGE, EDGE_KEYS, load_config, target_mapping

ACTORS = [
    "amqp-actuation-event-subscriber",
    "actuation-control-orchestrator",
    "actuation-event-validator",
    "actuation-signal-publisher",
    "decision-agent-stub",
]


def save(output: Path, data: Any) -> None:
    (output / "evidence.json").write_text(
        json.dumps(data, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )


def route_ok(records: list[dict[str, Any]], cid: str, config: dict[str, Any]) -> bool:
    required = {
        "decision-agent-stub": ["request_submitting"],
        "amqp-actuation-event-subscriber": ["request_received"],
        "actuation-control-orchestrator": ["pipeline_received", "outcome_published"],
        "actuation-event-validator": ["validation_decided"],
        "actuation-signal-publisher": ["write_initiated", "device_acknowledged", "writer_result"],
    }
    try:
        rows = [r for r in records if r["correlation_id"] == cid]
        decision = next(
            r
            for r in rows
            if r["component"] == "decision-agent-stub" and r["action"] == "request_submitting"
        )
        for component, actions in required.items():
            selected = [r for r in rows if r["component"] == component]
            binding = config["cloud" if component in CLOUD else "edge"]
            if not all(any(r["action"] == action for r in selected) for action in actions):
                return False
            if any(
                r["source"]["namespace"] != binding["namespace"]
                or r["source"]["context"] != binding["context"]
                for r in selected
            ):
                return False
            if any(
                r["target"] != decision["target"]
                or r["requested_value"] != decision["requested_value"]
                or r["requester"] != "decision-agent-stub"
                for r in selected
            ):
                return False
        return (
            sum(r["action"] == "write_initiated" for r in rows) == 1
            and any(
                r["action"] == "validation_decided" and r.get("decision") == "approve" for r in rows
            )
            and any(
                r["action"] == "writer_result" and r.get("outcome") == "completed" for r in rows
            )
            and any(
                r["action"] == "outcome_published" and r.get("outcome") == "approve" for r in rows
            )
        )
    except (KeyError, TypeError, StopIteration):
        return False


def availability_ok(case: dict[str, Any]) -> bool:
    try:
        cid = case["command"]["correlation_id"]
        fresh = ActuationRequest.model_validate(case["fresh_command"])
        return bool(
            case["unavailable_result"]["decision"] == "unknown"
            and case["after_unavailable"] == case["before"]
            and case["expired_result"]["decision"] == "reject"
            and datetime.fromisoformat(case["expired_replay_sent_at"])
            > datetime.fromisoformat(case["command"]["deadline"])
            and not case["expired_ledger"]
            and case["after_expired"] == case["before"]
            and not any(
                r["correlation_id"] == cid and r["action"] == "write_initiated"
                for r in case["writer_records"]
            )
            and case["fresh_result"]["decision"] == "approve"
            and case["fresh_result"]["correlation_id"] == fresh.correlation_id
            and case["after_recovery"]["setpoint"] == 25.0
            and len(case["fresh_ledger"]) == 1
            and case["fresh_ledger"][0]["state"] == "completed"
            and case["fresh_ledger"][0]["command_id"] == fresh.correlation_id
            and case["fresh_ledger"][0]["digest"] == command_digest(fresh)
            and sum(
                r["correlation_id"] == fresh.correlation_id and r["action"] == "write_initiated"
                for r in case["writer_records"]
            )
            == 1
            and any(
                r["correlation_id"] == fresh.correlation_id and r["action"] == "device_acknowledged"
                for r in case["writer_records"]
            )
        )
    except (KeyError, TypeError, ValueError):
        return False


def inventory_and_trust(cluster: Cluster, config: dict[str, Any]) -> dict[str, Any]:
    inventory = {}
    for zone, expected in [("edge", EDGE), ("cloud", CLOUD)]:
        reader = Cluster(**config[zone])
        deployments = json.loads(reader.run("get", "deployments", "-o", "json"))
        if {d["metadata"]["name"] for d in deployments["items"]} != set(expected):
            raise ValueError("deployment set differs from declared split")
        inventory[zone] = deployments
    edge = json.loads(cluster.run("get", "secret", SECRET_NAME, "-o", "json"))["data"]
    cloud = json.loads(cluster.target("rabbitmq").run("get", "secret", SECRET_NAME, "-o", "json"))[
        "data"
    ]
    trust = dict(
        edge_fields_minimal=set(edge) == EDGE_KEYS,
        approval_matches=edge["approval-key"] == cloud["approval-key"],
        decision_identity_matches=edge["decision-amqp-url"] == cloud["decision-amqp-url"],
        broker_endpoint_matches=all(
            urlsplit(base64.b64decode(cloud[k]).decode()).hostname
            == config["connections"]["amqp_host"]
            for k in cloud
            if k.endswith("amqp-url")
        ),
    )
    if not all(trust.values()):
        raise ValueError("split trust projection invalid")
    return dict(deployments=inventory, trust=trust, actors=cluster.inventory(ACTORS))


async def automatic(
    cluster: Cluster,
    config: dict[str, Any],
    amqp: str,
    opcua_port: int,
    data: dict[str, Any],
    output: Path,
) -> None:
    inventory = await asyncio.to_thread(cluster.inventory, ACTORS)
    starts = {
        pod: await asyncio.to_thread(cluster.probe, pod, info["component"], 0)
        for pod, info in inventory["pods"].items()
    }
    data["automatic"] = dict(inventory=inventory, starts=starts, cases=[])
    initial = await asyncio.to_thread(cluster.native, "conveyor")
    data["original_speed"] = initial["feedback"][10] / 100
    async with Client(f"opc.tcp://127.0.0.1:{opcua_port}/eirvah/simulator") as native:
        ns = await native.get_namespace_index("https://eirvah.uniza/zilina/factory1")
        setpoint = await native.nodes.objects.get_child(
            [f"{ns}:bottler", f"{ns}:SetpointTemperature"]
        )
        temperature = await native.nodes.objects.get_child([f"{ns}:bottler", f"{ns}:Temperature"])
        data["original_setpoint"] = float(await setpoint.read_value())
        for speed, expected in ((0.7, 26.0), (0.4, 22.0)):
            command = build_command("conveyor", "speed", speed)
            case: dict[str, Any] = dict(
                speed=speed,
                expected_setpoint=expected,
                stimulus=command.model_dump(mode="json"),
                samples=[],
            )
            data["automatic"]["cases"].append(case)
            case["result"] = await send(command, amqp)
            end = time.perf_counter() + 60
            stable = 0
            while time.perf_counter() < end:
                measured = float(await temperature.read_value())
                actual = float(await setpoint.read_value())
                case["samples"].append(
                    dict(
                        setpoint=actual,
                        temperature=measured,
                        observed_at=datetime.now(UTC).isoformat(),
                        monotonic=time.perf_counter(),
                    )
                )
                stable = (
                    stable + 1
                    if math.isfinite(measured)
                    and abs(measured - expected) <= 0.75
                    and actual == expected
                    else 0
                )
                if stable >= 3:
                    break
                await asyncio.sleep(0.5)
            case["settled"] = stable >= 3
            save(output, data)
            print(
                f"automatic speed={speed} warmer={expected} settled={case['settled']}", flush=True
            )
            if not case["settled"]:
                raise RuntimeError("automatic warmer response did not settle")
    # Capture complete per-process journal segments; ordering claims stay scoped.
    records: list[dict[str, Any]] = []
    ends = {}
    for pod, info in inventory["pods"].items():
        ends[pod] = await asyncio.to_thread(
            cluster.probe, pod, info["component"], starts[pod]["remote"]["record_seq"]
        )
        if validate_fence(starts[pod], ends[pod]):
            raise RuntimeError("automatic journal segment incomplete or restarted")
        records.extend(
            dict(
                **row,
                source=dict(
                    context=info["context"],
                    namespace=info["namespace"],
                    pod=info["pod"],
                    uid=info["uid"],
                ),
            )
            for row in ends[pod]["remote"]["records"]
        )
    selected = [
        r
        for r in records
        if r["component"] == "decision-agent-stub" and r["action"] == "request_submitting"
    ]
    high = [
        r["correlation_id"]
        for r in selected
        if r["value_type"] == "double"
        and r["requested_value"] == 26.0
        and route_ok(records, r["correlation_id"], config)
    ]
    normal = [
        r["correlation_id"]
        for r in selected
        if r["value_type"] == "double"
        and r["requested_value"] == 22.0
        and route_ok(records, r["correlation_id"], config)
    ]
    confirmed = [
        r["correlation_id"]
        for r in records
        if r["component"] == "decision-agent-stub"
        and r["action"] == "process_observed"
        and route_ok(records, r["correlation_id"], config)
    ]
    ids = sorted(set(high + normal + confirmed))
    data["automatic"].update(
        ends=ends,
        records=records,
        high=high,
        normal=normal,
        confirmed_rejections=confirmed,
        ledger=await asyncio.to_thread(cluster.ledger, ids) if ids else [],
        end_inventory=await asyncio.to_thread(cluster.inventory, ACTORS),
    )
    save(output, data)


async def availability(cluster: Cluster, amqp: str, data: dict[str, Any], output: Path) -> None:
    controller = cluster.target("decision-agent-stub")
    writer = cluster.target("actuation-signal-publisher")
    controller.run("scale", "deployment/decision-agent-stub", "--replicas=0")
    controller.run(
        "wait",
        "--for=delete",
        "pod",
        "-l",
        "app.kubernetes.io/name=decision-agent-stub",
        "--timeout=90s",
        timeout=100,
    )
    before = await asyncio.to_thread(cluster.native, "temperature")
    data["availability"] = dict(before=before)
    writer.run("scale", "deployment/actuation-signal-publisher", "--replicas=0")
    writer.run(
        "wait",
        "--for=delete",
        "pod",
        "-l",
        "app.kubernetes.io/name=actuation-signal-publisher",
        "--timeout=90s",
        timeout=100,
    )
    command = build_command("temperature", "set", 25.0)
    data["availability"]["command"] = command.model_dump(mode="json")
    save(output, data)
    try:
        result = await send(command, amqp)
    except TimeoutError:
        result = dict(decision="unknown", reason="caller_timeout")
    data["availability"].update(
        unavailable_result=result,
        after_unavailable=await asyncio.to_thread(cluster.native, "temperature"),
        ledger_unavailable="writer_offline",
    )
    writer.run("scale", "deployment/actuation-signal-publisher", "--replicas=1")
    writer.run(
        "rollout", "status", "deployment/actuation-signal-publisher", "--timeout=180s", timeout=190
    )
    assert command.deadline is not None
    remaining = (command.deadline - datetime.now(UTC)).total_seconds()
    if remaining >= 0:
        await asyncio.sleep(remaining + 0.1)
    case = data["availability"]
    case["expired_replay_sent_at"] = datetime.now(UTC).isoformat()
    case["expired_result"] = await send(command, amqp)
    case["expired_ledger"] = await asyncio.to_thread(cluster.ledger, [command.correlation_id])
    case["after_expired"] = await asyncio.to_thread(cluster.native, "temperature")
    fresh = build_command("temperature", "set", 25.0)
    case["fresh_command"] = fresh.model_dump(mode="json")
    case["fresh_result"] = await send(fresh, amqp)
    save(output, data)
    case["fresh_ledger"] = await asyncio.to_thread(cluster.ledger, [fresh.correlation_id])
    case["after_recovery"] = await asyncio.to_thread(cluster.native, "temperature")
    save(output, data)
    pods = await asyncio.to_thread(cluster.inventory, ["actuation-signal-publisher"])
    case["writer_records"] = []
    for pod, info in pods["pods"].items():
        probe = await asyncio.to_thread(cluster.probe, pod, info["component"], 0)
        baseline = {"remote": {**probe["remote"], "record_seq": 0, "records": []}}
        if validate_fence(baseline, probe):
            raise RuntimeError("restarted writer journal incomplete")
        case["writer_snapshot"] = probe
        case["writer_records"].extend(probe["remote"]["records"])
    save(output, data)
    print("writer availability/recovery case captured", flush=True)


def evaluate(data: dict[str, Any]) -> dict[str, Any]:
    report: dict[str, Any] = dict(
        local_split="unknown",
        physical_host_separation="not_exercised",
        network_policy_isolation="not_exercised",
        checks=[],
    )
    try:
        if data["errors"] or not data["restored"]:
            return report
        auto, config = data["automatic"], data["config"]
        checks = [
            dict(check="bounded_e0_on_split", passed=data["e0"]["status"] == "pass"),
            dict(check="minimal_matching_trust", passed=all(data["inventory"]["trust"].values())),
        ]
        expected = {"cloud": set(CLOUD), "edge": set(EDGE)}
        actual = {
            z: {d["metadata"]["name"] for d in data["inventory"]["deployments"][z]["items"]}
            for z in expected
        }
        checks.append(dict(check="declared_component_placement", passed=expected == actual))
        good = len(auto["cases"]) == 2 and all(
            c["settled"]
            and c["result"]["decision"] == "approve"
            and len(c["samples"]) >= 3
            and all(
                s["setpoint"] == c["expected_setpoint"]
                and abs(s["temperature"] - c["expected_setpoint"]) <= 0.75
                for s in c["samples"][-3:]
            )
            for c in auto["cases"]
        )
        good = (
            good
            and bool(auto["high"])
            and bool(auto["normal"])
            and bool(auto["confirmed_rejections"])
        )
        for cid in set(auto["high"] + auto["normal"] + auto["confirmed_rejections"]):
            good = (
                good
                and route_ok(auto["records"], cid, config)
                and sum(
                    r["command_id"] == cid and r["state"] == "completed" for r in auto["ledger"]
                )
                == 1
            )
        good = good and auto["inventory"] == auto["end_inventory"]
        good = good and all(
            not validate_fence(auto["starts"][p], auto["ends"][p]) for p in auto["starts"]
        )
        checks.append(dict(check="automatic_cloud_admission_edge_feedback", passed=good))
        checks.append(
            dict(
                check="writer_unavailable_expiry_and_fresh_recovery",
                passed=availability_ok(data["availability"]),
            )
        )
        report.update(
            checks=checks, local_split="pass" if all(c["passed"] for c in checks) else "fail"
        )
    except (KeyError, TypeError, ValueError):
        pass
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deployment", type=Path, required=True)
    parser.add_argument("--e0-run", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    config = load_config(args.deployment / "config.json")
    if any(config[z]["context"] != "kind-eirvah-edge" for z in ("edge", "cloud")):
        parser.error("this simulator rehearsal is restricted to the named local kind cluster")
    output = args.output or ROOT / "results/split-rehearsal" / datetime.now(UTC).strftime(
        "%Y-%m-%dT%H-%M-%S-%fZ"
    )
    output.mkdir(parents=True, exist_ok=False)
    e0 = args.e0_run or output / "e0"
    if args.e0_run is None:
        with (output / "e0.log").open("w", encoding="utf-8") as log:
            subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts/e0_capture.py"),
                    "--targets",
                    str(args.deployment / "targets.json"),
                    "--manifest",
                    str(args.deployment / "architecture.json"),
                    "--assurance",
                    "--output",
                    str(e0),
                ],
                cwd=ROOT,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=600,
                check=True,
            )
    local = json.loads((e0 / "local-case-report.json").read_text())
    if local["status"] != "pass" or json.loads(
        (e0 / "architecture-manifest.json").read_text()
    ) != json.loads((args.deployment / "architecture.json").read_text()):
        parser.error("passing E0 evidence must use this selected split manifest")
    targets = target_mapping(config)
    cluster = Cluster(config["edge"]["context"], config["edge"]["namespace"], targets["components"])
    for z in ("edge", "cloud"):
        ns = json.loads(
            Cluster(**config[z]).run("get", "namespace", config[z]["namespace"], "-o", "json")
        )
        if ns["metadata"].get("labels", {}).get("eirvah.uniza/purpose") != "split-local-rehearsal":
            parser.error("split namespace ownership label missing")
    data: dict[str, Any] = dict(
        config=config,
        e0=local,
        e0_directory=str(e0.resolve()),
        errors=[],
        restored=False,
        source_hashes={
            p.name: digest(p)
            for p in [
                Path(__file__),
                ROOT / "scripts/split_deployment.py",
                args.deployment / "config.json",
                args.deployment / "architecture.json",
            ]
        },
    )
    data["inventory"] = inventory_and_trust(cluster, config)
    for component in ("decision-agent-stub", "actuation-signal-publisher"):
        deployment = json.loads(
            cluster.target(component).run("get", "deployment", component, "-o", "json")
        )
        if deployment["spec"].get("replicas", 1) != 1:
            parser.error("one decision authority and one writer required")
    save(output, data)
    try:
        with ExitStack() as stack:
            rabbit = stack.enter_context(port_forward(*cluster.transport("rabbitmq", 5672)))
            opcua = stack.enter_context(port_forward(*cluster.transport("opcua-simulator", 4840)))
            modbus = stack.enter_context(port_forward(*cluster.transport("modbus-simulator", 5020)))
            cluster.native_ports.update({"opcua-simulator": opcua, "modbus-simulator": modbus})
            encoded = cluster.target("rabbitmq").run(
                "get", "secret", SECRET_NAME, "-o", "jsonpath={.data.experiment-amqp-url}"
            )
            parts = urlsplit(base64.b64decode(encoded).decode())
            amqp = urlunsplit(
                (
                    parts.scheme,
                    parts.netloc.rsplit("@", 1)[0] + f"@127.0.0.1:{rabbit}",
                    parts.path,
                    "",
                    "",
                )
            )

            async def run() -> None:
                try:
                    with phase("automatic-feedback"):
                        await automatic(cluster, config, amqp, opcua, data, output)
                    with phase("writer-unavailable-recovery"):
                        await availability(cluster, amqp, data, output)
                finally:
                    restored_settings = False
                    try:
                        writer = cluster.target("actuation-signal-publisher")
                        writer.run("scale", "deployment/actuation-signal-publisher", "--replicas=1")
                        writer.run(
                            "rollout",
                            "status",
                            "deployment/actuation-signal-publisher",
                            "--timeout=180s",
                            timeout=190,
                        )
                        for target, action, key in [
                            ("conveyor", "speed", "original_speed"),
                            ("temperature", "set", "original_setpoint"),
                        ]:
                            if key in data:
                                result = await send(build_command(target, action, data[key]), amqp)
                                if result.get("decision") != "approve":
                                    raise RuntimeError(
                                        "selected native setting restoration not confirmed"
                                    )
                        restored_settings = True
                    finally:
                        controller = cluster.target("decision-agent-stub")
                        controller.run("scale", "deployment/decision-agent-stub", "--replicas=1")
                        controller.run(
                            "rollout",
                            "status",
                            "deployment/decision-agent-stub",
                            "--timeout=180s",
                            timeout=190,
                        )
                        data["restored"] = restored_settings
                        save(output, data)

            with asyncio.Runner(loop_factory=asyncio.SelectorEventLoop) as runner:
                runner.run(run())
    except (Exception, KeyboardInterrupt) as exc:
        data["errors"].append(type(exc).__name__)
        save(output, data)
    report = evaluate(data)
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    lines = [
        "# Local logical edge/cloud rehearsal",
        "",
        "Verdict: **"
        + report["local_split"]
        + "**. Physical-host/network isolation: not exercised.",
        "",
        "| Check | Result |",
        "| --- | --- |",
    ]
    lines += [
        "| " + c["check"] + " | " + ("pass" if c["passed"] else "fail") + " |"
        for c in report["checks"]
    ]
    (output / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(dict(**report, output=str(output), errors=data["errors"]), indent=2))
    raise SystemExit(0 if report["local_split"] == "pass" else 1)


if __name__ == "__main__":
    main()
