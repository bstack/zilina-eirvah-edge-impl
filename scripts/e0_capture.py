"""Capture and evaluate a prospective bounded local E0 command case.

No HTTP evidence endpoint: journals are read using authenticated kubectl exec.
Clock verdicts are conditional on the recorded bracketing clock model.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import os
import struct
import subprocess
from contextlib import ExitStack
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import yaml
from conformance import evaluate, verdict
from control import build_command, send
from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.authorization import command_digest
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_observability.execution import clock_snapshot
from nats.aio.client import Client as NATSClient
from scenario import MqttObservations, Scenario, port_forward, run_scenario

ROOT = Path(__file__).resolve().parents[1]
PROFILE = ROOT / "config/architecture/local-e0-case.json"

# Read-only in the target container: do not import the application or create journals.
JOURNAL_PROBE = r"""
import json,sys,time,os,hashlib
from pathlib import Path
args=json.load(sys.stdin)
begin=time.monotonic_ns()
root=Path('/tmp/eirvah-execution-evidence')
meta=json.loads((root/'current.json').read_text())
path=(root/meta['journal_file']).resolve()
assert path.is_relative_to(root.resolve())
rows=[]
for line in path.read_text().splitlines():
 try:r=json.loads(line)
 except ValueError:continue
 if args['after'] < r['record_seq'] <= meta['record_seq']:rows.append(r)
files=['libs/eirvah-observability/src/eirvah_observability/execution.py',
       'libs/eirvah-contracts/src/eirvah_contracts/actuation.py',
       'libs/eirvah-contracts/src/eirvah_contracts/authorization.py',
       'services/'+args['component']+'/src/'+args['component'].replace('-','_')+'/service.py']
hashes={name:hashlib.sha256((Path('/workspace')/name).read_bytes()).hexdigest() for name in files}
wall=time.time_ns();finish=time.monotonic_ns()
domain='linux-monotonic:'+Path('/proc/sys/kernel/random/boot_id').read_text().strip()+':'+os.readlink('/proc/self/ns/time')
meta.update(records=rows,source_hashes=hashes,clock=dict(wall_time_ns=wall,
 monotonic_ns=finish,monotonic_before_ns=begin,monotonic_after_ns=finish,clock_domain=domain,timestamp_resolution_ns=1000))
print(json.dumps(meta))
"""


def save(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def validate_fence(start: dict[str, Any], end: dict[str, Any]) -> list[str]:
    a, b = start["remote"], end["remote"]
    errors = []
    if a["process_instance"] != b["process_instance"]:
        errors.append("process restarted inside capture segment")
    if a.get("overflow") or b.get("overflow"):
        errors.append("journal capacity exceeded")
    if b["record_seq"] < a["record_seq"]:
        errors.append("sequence moved backwards")
    rows = b["records"]
    seqs = [r.get("record_seq") for r in rows]
    if seqs != list(range(a["record_seq"] + 1, b["record_seq"] + 1)):
        errors.append("missing or duplicated sequence records")
    if any(r.get("process_instance") != b["process_instance"] for r in rows):
        errors.append("record process identity mismatch")
    return errors


def calibrate_record(
    record: dict[str, Any],
    start: dict[str, Any],
    end: dict[str, Any],
    host_samples: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    pre, post = start["remote"]["clock"], end["remote"]["clock"]
    if (
        not record.get("clock_domain")
        or record["clock_domain"] != pre["clock_domain"]
        or record["clock_domain"] != post["clock_domain"]
    ):
        raise ValueError("clock domain changed")
    if (
        not pre["monotonic_before_ns"]
        <= record["monotonic_before_ns"]
        <= record["monotonic_after_ns"]
        <= post["monotonic_after_ns"]
    ):
        raise ValueError("record outside bracketing calibration")
    probes = [start, end]
    # Causality bounds host-monotonic minus remote-monotonic at each probe.
    offset_low = min(
        p["client_before"]["monotonic_before_ns"] - p["remote"]["clock"]["monotonic_after_ns"]
        for p in probes
    )
    offset_high = max(
        p["client_after"]["monotonic_after_ns"] - p["remote"]["clock"]["monotonic_before_ns"]
        for p in probes
    )
    hosts = [p[k] for p in probes for k in ("client_before", "client_after")] + (host_samples or [])
    for p in probes:
        if p["client_after"]["wall_time_ns"] < p["client_before"]["wall_time_ns"]:
            raise ValueError("reference clock moved backwards")
    wall_low = min(
        h["wall_time_ns"] - h["monotonic_after_ns"] - h["timestamp_resolution_ns"] for h in hosts
    )
    wall_high = max(
        h["wall_time_ns"] - h["monotonic_before_ns"] + h["timestamp_resolution_ns"] for h in hosts
    )
    low = record["monotonic_before_ns"] + offset_low + wall_low
    high = record["monotonic_after_ns"] + offset_high + wall_high
    middle = (low + high) // 2
    at = datetime.fromtimestamp(middle // 1_000_000_000, UTC).replace(
        microsecond=(middle % 1_000_000_000) // 1000
    )
    return {
        **record,
        "raw_recorded_at": record["recorded_at"],
        "raw_clock_uncertainty_s": record.get("clock_uncertainty_s"),
        "recorded_at": at.isoformat(),
        "clock_uncertainty_s": (high - low) / 2e9 + 1e-6,
        "calibration_basis": "bracketing_causal_envelope",
    }


class Cluster:
    def __init__(
        self, context: str, namespace: str, components: dict[str, dict[str, Any]] | None = None
    ) -> None:
        if not context or not namespace:
            raise ValueError("context and namespace required")
        self.context, self.namespace = context, namespace
        self.native_ports: dict[str, int] = {}
        self.components = components or {}
        for binding in self.components.values():
            if not isinstance(binding, dict) or set(binding) - {
                "context",
                "namespace",
                "service",
                "port",
            }:
                raise ValueError("invalid deployment target")
            if any(
                not isinstance(binding.get(k, base), str) or not binding.get(k, base)
                for k, base in (("context", context), ("namespace", namespace))
            ):
                raise ValueError("invalid context or namespace")
            if "port" in binding and (
                type(binding["port"]) is not int or not 1 <= binding["port"] <= 65535
            ):
                raise ValueError("invalid target port")
        self.prefix = ["kubectl", "--context", context, "-n", namespace]

    def target(self, component: str) -> Cluster:
        binding = self.components.get(component, {})
        return Cluster(
            binding.get("context", self.context), binding.get("namespace", self.namespace)
        )

    def transport(self, component: str, default_port: int) -> tuple[list[str], str, int]:
        binding = self.components.get(component, {})
        return (
            self.target(component).prefix,
            binding.get("service", component),
            binding.get("port", default_port),
        )

    def run(self, *args: str, input: str | None = None, timeout: int = 30) -> str:
        return subprocess.check_output(
            [*self.prefix, "--request-timeout=15s", *args],
            input=input,
            text=True,
            encoding="utf-8",
            stderr=subprocess.PIPE,
            timeout=timeout,
        )

    def inventory(self, actors: list[str]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        configs: dict[str, str] = {}
        for component in actors:
            reader = self.target(component)
            pods = json.loads(
                reader.run("get", "pods", "-l", "app.kubernetes.io/name=" + component, "-o", "json")
            )["items"]
            for pod in pods:
                containers = pod.get("status", {}).get("containerStatuses", [])
                if not containers or not all(c["ready"] for c in containers):
                    raise RuntimeError("actor not ready: " + component)
                refs = [
                    v["configMap"]["name"]
                    for v in pod["spec"].get("volumes", [])
                    if "configMap" in v
                ]
                key = reader.context + "|" + reader.namespace + "|" + pod["metadata"]["name"]
                result[key] = {
                    "component": component,
                    "pod": pod["metadata"]["name"],
                    "uid": pod["metadata"]["uid"],
                    "context": reader.context,
                    "namespace": reader.namespace,
                    "node": pod["spec"]["nodeName"],
                    "configmaps": refs,
                    "runtime_policy": [
                        {
                            k: c.get(k)
                            for k in (
                                "name",
                                "resources",
                                "startupProbe",
                                "readinessProbe",
                                "livenessProbe",
                            )
                        }
                        for c in pod["spec"]["containers"]
                    ],
                    "containers": [
                        {k: c.get(k) for k in ("name", "imageID", "restartCount")}
                        for c in containers
                    ],
                }
                for name in refs:
                    config = json.loads(reader.run("get", "configmap", name, "-o", "json"))
                    configs[reader.context + "|" + reader.namespace + "|" + name] = hashlib.sha256(
                        json.dumps(config.get("data", {}), sort_keys=True).encode()
                    ).hexdigest()
        if {p["component"] for p in result.values()} != set(actors):
            raise RuntimeError("required actors missing")
        if sum(p["component"] == "actuation-signal-publisher" for p in result.values()) != 1:
            raise RuntimeError("one authoritative writer required")
        pvc = json.loads(
            self.target("actuation-signal-publisher").run(
                "get", "pvc", "actuation-command-ledger", "-o", "json"
            )
        )
        if pvc["status"]["phase"] != "Bound":
            raise RuntimeError("ledger volume not bound")
        return {
            "pods": result,
            "configmap_hashes": configs,
            "ledger_volume": {"uid": pvc["metadata"]["uid"], "volume": pvc["spec"]["volumeName"]},
        }

    def probe(self, pod: str, component: str, after: int) -> dict[str, Any]:
        before = clock_snapshot()
        remote = json.loads(
            self.target(component).run(
                "exec",
                "-i",
                pod.rsplit("|", 1)[-1],
                "--",
                "python",
                "-c",
                JOURNAL_PROBE,
                input=json.dumps({"component": component, "after": after}),
            )
        )
        return {"client_before": before, "client_after": clock_snapshot(), "remote": remote}

    def native(self, target: str) -> dict[str, Any]:
        # Protocol clients run on the capture host through authenticated loopback
        # port-forwards. Container import/exec latency must not consume command TTL.
        if target == "capper":
            from snap7.client import Client as S7Client

            client = S7Client()
            try:
                client.connect("127.0.0.1", 0, 1, tcp_port=self.native_ports["s7-simulator"])
                data = client.db_read(3, 0, 18)
                return {
                    "mailbox": data[:10].hex(),
                    "feedback": list(struct.unpack(">HHHH", data[10:18])),
                }
            finally:
                client.disconnect()
                client.destroy()
        if target == "temperature":
            from asyncua import Client

            async def read_setpoint() -> dict[str, Any]:
                port = self.native_ports["opcua-simulator"]
                async with Client(f"opc.tcp://127.0.0.1:{port}/eirvah/simulator") as client:
                    ns = await client.get_namespace_index("https://eirvah.uniza/zilina/factory1")
                    node = await client.nodes.objects.get_child(
                        [str(ns) + ":bottler", str(ns) + ":SetpointTemperature"]
                    )
                    return {"setpoint": await node.read_value()}

            return asyncio.run(read_setpoint())
        from pymodbus.client import AsyncModbusTcpClient

        async def read_modbus() -> dict[str, Any]:
            client = AsyncModbusTcpClient("127.0.0.1", port=self.native_ports["modbus-simulator"])
            try:
                if not await client.connect():
                    raise RuntimeError("native observation connection unavailable")
                state = await client.read_holding_registers(0, count=12, slave=1)
                ack = await client.read_holding_registers(50, count=6, slave=1)
                if state.isError() or ack.isError():
                    raise RuntimeError("native observation refused")
                return {"feedback": state.registers, "ack": ack.registers}
            finally:
                client.close()

        return asyncio.run(read_modbus())

    def ledger(self, ids: list[str]) -> list[dict[str, Any]]:
        code = r"""
import sqlite3,json,sys
ids=json.load(sys.stdin)
c=sqlite3.connect('file:/var/lib/eirvah/commands.sqlite3?mode=ro',uri=True);c.row_factory=sqlite3.Row
print(json.dumps([dict(r) for r in c.execute('SELECT command_id,digest,state,result,started_at,keep_until FROM commands WHERE command_id IN ('+','.join('?' for _ in ids)+')',ids)]));c.close()
"""
        data: list[dict[str, Any]] = json.loads(
            self.target("actuation-signal-publisher").run(
                "exec",
                "-i",
                "deployment/actuation-signal-publisher",
                "--",
                "python",
                "-c",
                code,
                input=json.dumps(ids),
            )
        )

        return data


def command_target(command: ActuationRequest) -> str:
    for target, part in [
        ("temperature", "/bottler/"),
        ("conveyor", "/conveyor/"),
        ("filler", "/filler/"),
        ("capper", "/capper/"),
        ("reject", "/reject_station/"),
    ]:
        if part in command.target_uns_topic:
            return target
    raise ValueError("unknown target")


def native_matches(command: dict[str, Any], data: dict[str, Any]) -> bool:
    topic = command["target_uns_topic"]
    value = command["requested_value"]
    if "/capper/" in topic:
        f = data["feedback"]
        if topic.endswith("/run"):
            return bool(bool(f[0]) == value)
        if topic.endswith("/speed_setpoint"):
            return bool(f[2] == value)
        return bool(f[1] == 0)
    if "/bottler/" in topic:
        return bool(abs(data["setpoint"] - value) < 1e-6)
    f = data["feedback"]
    if topic.endswith("/run"):
        return bool(bool(f[9]) == value)
    if topic.endswith("/speed_setpoint"):
        return bool(abs(f[10] / 100 - value) < 1e-6)
    if topic.endswith("/reset_fault"):
        return bool(f[4] == 0)
    if "/filler/" in topic:
        return bool(bool(f[8]) == value)
    return bool(f[11] == value)


def local_report(
    study: dict[str, Any],
    profile: dict[str, Any],
    records: list[dict[str, Any]],
    submissions: list[dict[str, Any]],
    ledger: list[dict[str, Any]],
    unsigned: dict[str, Any],
    errors: list[str],
) -> dict[str, Any]:
    checks = []
    init_ids = {r["correlation_id"] for r in records if r["action"] == "write_initiated"}
    scoped_write_checks = {
        "C07.admission",
        "C07.write_policy",
        "C08.deadline",
        "C09.initiations",
        "C09.binding",
    }
    all_checks = [c for rule in study["rules"] for c in rule["checks"]]
    for name in profile["required_checks"]:
        found = [c for c in all_checks if c["check"] == name]
        if name in scoped_write_checks:
            found = [c for c in found if c["evidence"] and c["evidence"][0] in init_ids]
        checks.append(
            {"check": name, "status": verdict([c["status"] for c in found]), "evidence": found}
        )
    checks.append(
        {
            "check": "capture.fences_and_builds",
            "status": "unknown" if errors else "pass",
            "evidence": errors,
        }
    )
    positive_controls = set()
    native_errors = []
    binding_errors = []
    replays = 0
    policy_refusals = 0
    expired = 0
    stored = {r["command_id"]: r for r in ledger}
    for entry in submissions:
        c = entry["command"]
        res = entry["result"]
        cid = c["correlation_id"]
        row = stored.get(cid)
        if res.get("decision") == "approve":
            if not native_matches(c, entry["native_after"]):
                native_errors.append(cid + ": final feedback mismatch")
            if (
                row is None
                or row["state"] != "completed"
                or row["digest"] != command_digest(ActuationRequest.model_validate(c))
            ):
                binding_errors.append(cid + ": ledger binding/state mismatch")
            positive_controls.add(entry["target"])
            for r in records:
                if (
                    r["correlation_id"] == cid
                    and r["action"]
                    in ("validation_decided", "executor_admitted", "write_initiated")
                    and r.get("decision", "approve") == "approve"
                    and r.get("command_digest")
                    != command_digest(ActuationRequest.model_validate(c))
                ):
                    binding_errors.append(cid + ": approval digest missing or mismatched")
            if res.get("replayed"):
                replays += 1
                if (
                    entry["target"] != "capper"
                    or entry["native_before"]["mailbox"] != entry["native_after"]["mailbox"]
                ):
                    native_errors.append(cid + ": replay changed native mailbox")
        elif res.get("decision") == "reject":
            if entry["target"] == "capper" and entry["native_before"] != entry["native_after"]:
                native_errors.append(cid + ": refusal changed device state")
            if row is not None or cid in init_ids:
                binding_errors.append(cid + ": refused command admitted or initiated")
            if datetime.fromisoformat(c["deadline"]) < datetime.fromisoformat(c["requested_at"]):
                expired += 1
            elif "outside policy range" in res.get("rejection_reason", ""):
                policy_refusals += 1
        else:
            native_errors.append(cid + ": unsettled command")
    unsigned_ok = (
        unsigned["reply"].get("status") == "error"
        and unsigned["reply"].get("payload", {}).get("outcome") == "not_executed"
        and unsigned["native_before"] == unsigned["native_after"]
        and unsigned["command"]["correlation_id"] not in stored
        and unsigned["command"]["correlation_id"] not in init_ids
    )
    checks.extend(
        [
            {
                "check": "cases.five_controls",
                "status": "pass"
                if positive_controls == set(profile["required_controls"])
                else "unknown",
                "evidence": sorted(positive_controls),
            },
            {
                "check": "cases.native_feedback",
                "status": "fail" if native_errors else "pass",
                "evidence": native_errors,
            },
            {
                "check": "cases.full_command_digest_and_ledger",
                "status": "fail" if binding_errors else "pass",
                "evidence": binding_errors,
            },
            {
                "check": "cases.exact_replay",
                "status": "pass" if replays else "unknown",
                "evidence": replays,
            },
            {
                "check": "cases.policy_refusal",
                "status": "pass" if policy_refusals else "unknown",
                "evidence": policy_refusals,
            },
            {
                "check": "cases.expired_fresh_request",
                "status": "pass" if expired else "unknown",
                "evidence": expired,
            },
            {
                "check": "cases.unsigned_writer_refusal",
                "status": "pass" if unsigned_ok else "fail",
                "evidence": unsigned,
            },
        ]
    )
    return {
        "profile": profile["id"],
        "status": verdict([c["status"] for c in checks]),
        "scope": profile["scope"],
        "clock_model": profile["clock_model"],
        "whole_study_status": study["overall"],
        "universal_conformance": "not_established",
        "checks": checks,
    }


async def execute(
    cluster: Cluster,
    output: Path,
    profile: dict[str, Any],
    repetitions: int,
    amqp: str,
    mqtt_port: int,
    nats_port: int,
    starts: dict[str, Any],
    inventory: dict[str, Any],
) -> dict[str, Any]:
    submissions = []
    host_samples = []

    async def sender(command: ActuationRequest) -> dict[str, Any]:
        target = command_target(command)
        native_before = await asyncio.to_thread(cluster.native, target)
        first = clock_snapshot()
        result = await send(command, amqp)
        last = clock_snapshot()
        native_after = await asyncio.to_thread(cluster.native, target)
        entry: dict[str, Any] = {
            "command": command.model_dump(mode="json"),
            "result": result,
            "target": target,
            "native_before": native_before,
            "native_after": native_after,
            "client_before": first,
            "client_after": last,
        }
        submissions.append(entry)
        host_samples.extend([first, last])
        save(output / "submissions.json", submissions)
        return result

    for target in ("temperature", "conveyor", "capper"):
        await asyncio.to_thread(cluster.native, target)
    plan = Scenario.model_validate_json((ROOT / "config/scenarios/local-e0.json").read_text())
    scenario = await run_scenario(
        plan,
        output / "run",
        sender=sender,
        observations=MqttObservations(
            mqtt_port,
            os.environ.get("EIRVAH_MQTT_USERNAME", "eirvah"),
            os.environ.get("EIRVAH_MQTT_PASSWORD", "eirvah-dev-password"),
        ),
        repetitions=repetitions,
        metadata={"capture_profile": profile["id"]},
    )
    if scenario["outcome"] != "passed":
        raise RuntimeError("scenario did not complete as expected")
    for line in (output / "run/journal.jsonl").read_text().splitlines():
        event = json.loads(line)
        host_samples.extend(event.get("issuer_clocks", []))
    print("Testing direct writer request without approval", flush=True)
    command = build_command("capper", "speed", 1200)
    unsigned: dict[str, Any] = {
        "command": command.model_dump(mode="json"),
        "native_before": await asyncio.to_thread(cluster.native, "capper"),
    }
    save(output / "unsigned-writer.json", unsigned)
    nc = NATSClient()
    await nc.connect(servers=[f"nats://127.0.0.1:{nats_port}"])
    try:
        reply = await nc.request(
            "act.work.write_signal",
            NATSEnvelope(
                correlation_id=command.correlation_id, payload=command.model_dump(mode="json")
            )
            .model_dump_json()
            .encode(),
            timeout=5,
        )
        unsigned["reply"] = json.loads(reply.data)
    finally:
        await nc.close()
    unsigned["native_after"] = await asyncio.to_thread(cluster.native, "capper")
    save(output / "unsigned-writer.json", unsigned)
    # End fences occur only after all owned commands have settled. No command is replayed blindly.
    await asyncio.sleep(0.3)
    ends = {
        pod: await asyncio.to_thread(
            cluster.probe, pod, info["component"], starts[pod]["remote"]["record_seq"]
        )
        for pod, info in inventory["pods"].items()
    }
    save(output / "capture-end.json", ends)
    end_inventory = await asyncio.to_thread(cluster.inventory, profile["actors"])
    save(output / "inventory-end.json", end_inventory)
    errors = []
    if inventory != end_inventory:
        errors.append("pod/container/configuration/volume identity changed inside capture")
    records = []
    ids = {s["command"]["correlation_id"] for s in submissions}
    for pod in starts:
        errors.extend(pod + ": " + e for e in validate_fence(starts[pod], ends[pod]))
        for name, digest in starts[pod]["remote"]["source_hashes"].items():
            if (
                not (ROOT / name).is_file()
                or hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != digest
            ):
                errors.append(pod + ": deployed source differs: " + name)
        if starts[pod]["remote"]["source_hashes"] != ends[pod]["remote"]["source_hashes"]:
            errors.append(pod + ": deployed source changed")
        for row in ends[pod]["remote"]["records"]:
            if row["correlation_id"] not in ids:
                continue
            try:
                calibrated = calibrate_record(row, starts[pod], ends[pod], host_samples)
            except ValueError:
                errors.append(pod + ": clock bracket unavailable")
                calibrated = row
            records.append(
                {
                    **calibrated,
                    "source": {
                        "pod": pod,
                        "uid": inventory["pods"][pod]["uid"],
                        "capture": "private_sequence_journal",
                    },
                }
            )
    save(
        output / "calibration.json",
        {
            "model": profile["clock_model"],
            "host_samples": host_samples,
            "clock_uncertainty_s_range": [
                min((r.get("clock_uncertainty_s") or 0) for r in records),
                max((r.get("clock_uncertainty_s") or 0) for r in records),
            ]
            if records
            else None,
        },
    )
    for cid in ids:
        dest = output / "run/traces" / cid
        selected = [r for r in records if r["correlation_id"] == cid]
        save(dest / "summary.json", {"collection_errors": errors, "capture_profile": profile["id"]})
        (dest / "records.jsonl").write_text(
            "".join(json.dumps(r) + "\n" for r in selected), encoding="utf-8"
        )
    ledger = await asyncio.to_thread(cluster.ledger, sorted(ids | {command.correlation_id}))
    save(output / "writer-ledger.json", ledger)
    reference = json.loads((ROOT / "config/architecture/conformance-reference.json").read_text())
    manifest = json.loads((ROOT / "config/architecture/local-control.json").read_text())
    policies = yaml.safe_load(
        (ROOT / "deploy/k3s/base/actuation-event-validator/actuation-policy.yaml").read_text()
    )["policies"]
    study = evaluate(
        manifest,
        reference,
        records=records,
        commands=[s["command"] for s in submissions],
        policies=policies,
        collection_errors=errors,
    )
    save(output / "whole-study-report.json", study)
    local = local_report(study, profile, records, submissions, ledger, unsigned, errors)
    local["scenario_outcome"] = scenario["outcome"]
    local["submission_count"] = len(submissions)
    local["record_count"] = len(records)
    local["capture_errors"] = errors
    save(output / "local-case-report.json", local)
    lines = [
        "# Bounded local E0 case",
        "",
        f"Verdict: **{local['status']}**. Whole study: **{study['overall']}**.",
        "",
        profile["scope"],
        "",
        profile["clock_model"],
        "",
        "| Check | Verdict |",
        "| --- | --- |",
    ]
    lines.extend(f"| {c['check']} | {c['status']} |" for c in local["checks"])
    (output / "local-case-report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return local


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context")
    parser.add_argument(
        "--targets",
        type=Path,
        help="Context/namespace/service bindings for private, public or hybrid capture",
    )
    parser.add_argument("--namespace", default="eirvah-edge")
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.repetitions < 1:
        parser.error("repetitions must be positive")
    routing = json.loads(args.targets.read_text(encoding="utf-8")) if args.targets else {}
    if routing and routing.get("schema_version") != "1.0":
        parser.error("unsupported target mapping")
    default = routing.get("default", {})
    context = args.context or default.get("context")
    namespace = default.get("namespace", args.namespace)
    if not context:
        parser.error("--context or a target mapping default.context is required")
    output = args.output or ROOT / "results/e0-capture" / datetime.now(UTC).strftime(
        "%Y-%m-%dT%H-%M-%S-%fZ"
    )
    if output.exists():
        parser.error("output must be a new directory")
    output.mkdir(parents=True)
    profile = json.loads(PROFILE.read_text())
    save(output / "case-profile.json", profile)
    save(
        output / "provenance.json",
        {
            "context": context,
            "namespace": namespace,
            "git_sha": subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
            ).strip(),
            "source_hashes": {
                str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in [
                    Path(__file__),
                    PROFILE,
                    ROOT / "scripts/conformance.py",
                    ROOT / "config/scenarios/local-e0.json",
                ]
            },
        },
    )
    cluster = Cluster(context, namespace, routing.get("components", {}))
    controller = cluster.target("decision-agent-stub")
    save(
        output / "deployment-targets.json",
        {"default": {"context": context, "namespace": namespace}, "components": cluster.components},
    )
    previous = None
    outcome = None
    try:
        previous = json.loads(
            controller.run("get", "deployment", "decision-agent-stub", "-o", "json")
        )["spec"]["replicas"]
        save(
            output / "controller-state.json",
            {
                "context": controller.context,
                "namespace": controller.namespace,
                "original_replicas": previous,
                "restored": False,
            },
        )
        print("Pausing automatic decisions for the isolated local command case", flush=True)
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
        with ExitStack() as stack:
            rabbit = stack.enter_context(port_forward(*cluster.transport("rabbitmq", 5672)))
            mqtt = stack.enter_context(port_forward(*cluster.transport("mosquitto", 1883)))
            nats = stack.enter_context(port_forward(*cluster.transport("nats", 4222)))
            for simulator, port in (
                ("opcua-simulator", 4840),
                ("modbus-simulator", 5020),
                ("s7-simulator", 102),
            ):
                cluster.native_ports[simulator] = stack.enter_context(
                    port_forward(*cluster.transport(simulator, port))
                )
            inventory = cluster.inventory(profile["actors"])
            save(output / "inventory-start.json", inventory)
            starts = {
                pod: cluster.probe(pod, info["component"], 0)
                for pod, info in inventory["pods"].items()
            }
            save(output / "capture-start.json", starts)
            for probe in starts.values():
                for name, digest in probe["remote"]["source_hashes"].items():
                    if (
                        not (ROOT / name).is_file()
                        or hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != digest
                    ):
                        raise RuntimeError("deployed source does not match capture revision")
            encoded = cluster.target("rabbitmq").run(
                "get", "secret", "actuation-security", "-o", "jsonpath={.data.experiment-amqp-url}"
            )
            parts = urlsplit(base64.b64decode(encoded).decode())
            amqp = urlunsplit(
                (
                    parts.scheme,
                    parts.netloc.split("@")[0] + f"@127.0.0.1:{rabbit}",
                    parts.path,
                    "",
                    "",
                )
            )
            with asyncio.Runner(loop_factory=asyncio.SelectorEventLoop) as runtime:
                outcome = runtime.run(
                    execute(
                        cluster,
                        output,
                        profile,
                        args.repetitions,
                        amqp,
                        mqtt,
                        nats,
                        starts,
                        inventory,
                    )
                )
            print(
                json.dumps(
                    {
                        "local_case": outcome["status"],
                        "whole_study": outcome["whole_study_status"],
                        "records": outcome["record_count"],
                        "submissions": outcome["submission_count"],
                        "report": str(output / "local-case-report.md"),
                    },
                    indent=2,
                )
            )
    except (Exception, KeyboardInterrupt) as exc:
        safe_detail = (
            str(exc)
            if type(exc) is RuntimeError
            and str(exc).startswith(
                (
                    "actor not ready:",
                    "required actors",
                    "one authoritative",
                    "ledger volume",
                    "deployed source",
                    "scenario did not",
                )
            )
            else None
        )
        save(
            output / "capture-error.json",
            {"status": "unknown", "error_type": type(exc).__name__, "detail": safe_detail},
        )
        print(
            "Capture incomplete: " + type(exc).__name__ + "; evidence retained at " + str(output),
            flush=True,
        )
    finally:
        if previous is not None:
            restored = False
            try:
                controller.run(
                    "scale", "deployment/decision-agent-stub", "--replicas=" + str(previous)
                )
                if previous:
                    controller.run(
                        "rollout",
                        "status",
                        "deployment/decision-agent-stub",
                        "--timeout=120s",
                        timeout=130,
                    )
                restored = True
            except (subprocess.SubprocessError, OSError):
                print(
                    "Controller restoration could not be confirmed; see controller-state.json",
                    flush=True,
                )
                outcome = None
            save(
                output / "controller-state.json",
                {
                    "context": controller.context,
                    "namespace": controller.namespace,
                    "original_replicas": previous,
                    "restored": restored,
                },
            )
    raise SystemExit(
        0
        if outcome and outcome["status"] == "pass"
        else 1
        if outcome and outcome["status"] == "fail"
        else 2
    )


if __name__ == "__main__":
    main()
