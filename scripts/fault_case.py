"""Run bounded command faults on an explicitly selected research Kubernetes deployment.

A temporary writer sidecar proxies only the simulated rejection mailbox on pod
loopback. Existing application code, credentials and ledger PVC are retained.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import copy
import hashlib
import json
import subprocess
from contextlib import ExitStack
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import aio_pika
import yaml
from control import build_command, send
from e0_capture import JOURNAL_PROBE, Cluster, save
from eirvah_contracts.actuation import ActuationRequest
from fault_checks import PROFILE, report
from recovery_case import RecoveryRun
from scenario import port_forward

ROOT = Path(__file__).resolve().parents[1]
PROXY = "native-fault-proxy"
PROXY_EDIT = r"""
import json,sys,os
from pathlib import Path
root=Path('/tmp/e0-native-faults');root.mkdir(exist_ok=True)
plan=json.load(sys.stdin);path=root/'plan.new';path.write_text(json.dumps(plan));os.replace(path,root/'plan.json')
print(json.dumps({'installed':plan['id']}))
"""
PROXY_READ = r"""
import json,hashlib
from pathlib import Path
root=Path('/tmp/e0-native-faults')
print(json.dumps({'ready':(root/'ready').is_file(),'records':[json.loads(line) for line in (root/'events.jsonl').read_text().splitlines()] if (root/'events.jsonl').exists() else [],'source_sha256':hashlib.sha256(Path('/opt/e0-faults/native_fault_proxy.py').read_bytes()).hexdigest()}))
"""


class FaultRun:
    def __init__(self, recovery: RecoveryRun) -> None:
        self.r = recovery
        self.cluster, self.output = recovery.cluster, recovery.output
        self.data: dict[str, Any] = dict(
            schema_version="1.0",
            mode="live_command_faults",
            profile_sha256=hashlib.sha256(PROFILE.read_bytes()).hexdigest(),
            cases=[],
            capture_errors=[],
            restored=False,
        )
        self.original: dict[str, Any] | None = None
        self.config_name = "e0-native-faults-" + datetime.now(UTC).strftime("%Y%m%d%H%M%S")
        self.proxy_snapshots: list[dict[str, Any]] = []
        self.journals: list[dict[str, Any]] = []

    def persist(self) -> None:
        save(self.output / "evidence.json", self.data)

    def proxy_exec(self, code: str, input: str | None = None) -> dict[str, Any]:
        return dict(
            json.loads(
                self.cluster.target("actuation-signal-publisher").run(
                    "exec",
                    "-i",
                    "deployment/actuation-signal-publisher",
                    "-c",
                    PROXY,
                    "--",
                    "python",
                    "-c",
                    code,
                    input=input if input is not None else "",
                )
            )
        )

    def proxy(self) -> dict[str, Any]:
        packet = self.proxy_exec(PROXY_READ)
        if (
            packet["source_sha256"]
            != hashlib.sha256(
                (ROOT / "scripts/native_fault_proxy.py").read_text().encode()
            ).hexdigest()
        ):
            raise RuntimeError("fault proxy source differs from the checkout")
        return packet

    def plan(self, name: str, mode: str, command: ActuationRequest) -> None:
        self.proxy_exec(
            PROXY_EDIT, json.dumps(dict(id=name, mode=mode, command_id=command.correlation_id))
        )

    def ledger(self, command: ActuationRequest) -> dict[str, Any]:
        return self.r.read("actuation-signal-publisher", "writer", ids=[command.correlation_id])

    def journal(self) -> dict[str, Any]:
        packet: dict[str, Any] = json.loads(
            self.cluster.target("actuation-signal-publisher").run(
                "exec",
                "-i",
                "deployment/actuation-signal-publisher",
                "-c",
                "actuation-signal-publisher",
                "--",
                "python",
                "-c",
                JOURNAL_PROBE,
                input=json.dumps(dict(component="actuation-signal-publisher", after=0)),
            )
        )
        if packet.get("overflow"):
            raise RuntimeError("writer journal capacity exceeded")
        return packet

    async def install(self) -> None:
        reader = self.cluster.target("actuation-signal-publisher")
        self.original = json.loads(
            await asyncio.to_thread(
                reader.run, "get", "deployment", "actuation-signal-publisher", "-o", "json"
            )
        )
        template = copy.deepcopy(self.original["spec"]["template"])
        writer = next(
            c for c in template["spec"]["containers"] if c["name"] == "actuation-signal-publisher"
        )
        if len(template["spec"]["containers"]) != 1 or self.original["spec"]["replicas"] != 1:
            raise RuntimeError("one authoritative writer container required")
        mount = next(v for v in template["spec"]["volumes"] if v["name"] == "config")
        config = json.loads(
            await asyncio.to_thread(
                reader.run, "get", "configmap", mount["configMap"]["name"], "-o", "json"
            )
        )
        targets = yaml.safe_load(config["data"]["native-actuation-targets.yaml"])
        target = next(t for t in targets["targets"] if t["uns_topic"].endswith("/reject_bottle"))
        if (
            target["protocol"] != "modbus"
            or target["command_address"] != 28
            or target["ack_address"] != 54
        ):
            raise RuntimeError("selected native mailbox is not the declared Modbus profile")
        upstream_host, upstream_port = target["host"], target["port"]
        target.update(host="127.0.0.1", port=15020, timeout=1.5)
        proxy_code = (ROOT / "scripts/native_fault_proxy.py").read_text()
        configmap = dict(
            apiVersion="v1",
            kind="ConfigMap",
            metadata=dict(name=self.config_name, namespace=reader.namespace),
            data={
                "native-targets.yaml": yaml.safe_dump(targets),
                "native_fault_proxy.py": proxy_code,
            },
        )
        await asyncio.to_thread(reader.run, "create", "-f", "-", input=json.dumps(configmap))
        template["spec"]["volumes"].append(
            {"name": "e0-native-faults", "configMap": {"name": self.config_name}}
        )
        volume = {"name": "e0-native-faults", "mountPath": "/opt/e0-faults", "readOnly": True}
        writer.setdefault("volumeMounts", []).append(volume)
        writer["env"] = [
            e
            for e in writer.get("env", [])
            if e["name"] != "ACTUATION_SIGNAL_PUBLISHER_NATIVE_TARGETS_PATH"
        ] + [
            {
                "name": "ACTUATION_SIGNAL_PUBLISHER_NATIVE_TARGETS_PATH",
                "value": "/opt/e0-faults/native-targets.yaml",
            }
        ]
        template["spec"]["containers"].append(
            dict(
                name=PROXY,
                image=writer["image"],
                imagePullPolicy=writer.get("imagePullPolicy", "IfNotPresent"),
                command=[
                    "python",
                    "/opt/e0-faults/native_fault_proxy.py",
                    "--host",
                    upstream_host,
                    "--upstream-port",
                    str(upstream_port),
                ],
                volumeMounts=[volume],
                resources={
                    "requests": {"cpu": "10m", "memory": "32Mi"},
                    "limits": {"cpu": "200m", "memory": "128Mi"},
                },
            )
        )
        # Original template contains only secret references in this selected baseline.
        save(self.output / "writer-template-original.json", self.original["spec"]["template"])
        await asyncio.to_thread(
            reader.run,
            "patch",
            "deployment",
            "actuation-signal-publisher",
            "--type=json",
            "-p",
            json.dumps([{"op": "replace", "path": "/spec/template", "value": template}]),
        )
        await asyncio.to_thread(
            reader.run,
            "rollout",
            "status",
            "deployment/actuation-signal-publisher",
            "--timeout=240s",
            timeout=250,
        )
        await self.r.wait(self.proxy, lambda p: p["ready"])
        self.data["writer_initial"] = self.r.deployment("actuation-signal-publisher")
        self.persist()

    async def extend_restart_barrier(self) -> None:
        reader = self.cluster.target("actuation-signal-publisher")
        config = json.loads(
            await asyncio.to_thread(reader.run, "get", "configmap", self.config_name, "-o", "json")
        )
        targets = yaml.safe_load(config["data"]["native-targets.yaml"])
        next(t for t in targets["targets"] if t["uns_topic"].endswith("/reject_bottle"))[
            "timeout"
        ] = 30
        await asyncio.to_thread(
            reader.run,
            "patch",
            "configmap",
            self.config_name,
            "--type=merge",
            "-p",
            json.dumps({"data": {"native-targets.yaml": yaml.safe_dump(targets)}}),
        )
        await self.r.restart("actuation-signal-publisher")
        await self.r.wait(self.proxy, lambda p: p["ready"])

    async def restore_writer(self) -> None:
        if self.original is None:
            return
        reader = self.cluster.target("actuation-signal-publisher")
        await asyncio.to_thread(
            reader.run,
            "patch",
            "deployment",
            "actuation-signal-publisher",
            "--type=json",
            "-p",
            json.dumps(
                [
                    {
                        "op": "replace",
                        "path": "/spec/template",
                        "value": self.original["spec"]["template"],
                    }
                ]
            ),
        )
        await asyncio.to_thread(
            reader.run,
            "rollout",
            "status",
            "deployment/actuation-signal-publisher",
            "--timeout=240s",
            timeout=250,
        )
        actual = json.loads(
            await asyncio.to_thread(
                reader.run, "get", "deployment", "actuation-signal-publisher", "-o", "json"
            )
        )
        if actual["spec"]["template"] != self.original["spec"]["template"]:
            raise RuntimeError("writer template restoration mismatch")
        await asyncio.to_thread(
            reader.run, "delete", "configmap", self.config_name, "--ignore-not-found=true"
        )

    async def publish_without_reply(self, command: ActuationRequest) -> None:
        async with await aio_pika.connect_robust(self.r.amqp) as connection:
            channel = await connection.channel()
            exchange = await channel.get_exchange("eirvah.actuation.commands")
            # No result queue is declared: the caller has lost its reply subscription.
            await exchange.publish(
                aio_pika.Message(
                    body=command.model_dump_json().encode(),
                    user_id="experiment-driver",
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key="eirvah.actuation.requests",
            )

    async def submit(self, command: ActuationRequest) -> dict[str, Any]:
        try:
            return await send(command, self.r.amqp)
        except TimeoutError:
            return dict(
                correlation_id=command.correlation_id,
                decision="unknown",
                capture_reason="caller_result_timeout",
            )

    async def case(self, name: str, bottle: int) -> None:
        print("Fault case: " + name, flush=True)
        if name == "writer_restart_after_effect":
            await self.extend_restart_barrier()
        if name == "delayed_expired_command":
            await self.r.scale("amqp-actuation-event-subscriber", 0)
        before = await asyncio.to_thread(self.r.source)
        clock = datetime.fromtimestamp(before["clock"]["wall"], UTC)
        command = build_command("reject", "reject", bottle).model_copy(
            update={
                "requested_at": clock,
                "deadline": clock
                + timedelta(seconds=120 if name != "delayed_expired_command" else 10),
            }
        )
        mode = {
            "before_write_disconnect": "disconnect_before_write",
            "commit_dropped": "drop_commit",
            "native_ack_lost": "drop_ack",
            "writer_restart_after_effect": "hold_ack",
        }.get(name, "normal")
        self.data["phase"] = name
        self.persist()
        self.plan(name, mode, command)
        entry: dict[str, Any] = dict(
            id=name,
            command=command.model_dump(mode="json"),
            before=before,
            proxy=[],
            writer_journals=[],
        )
        self.data["cases"].append(entry)
        self.persist()
        first = None
        if name == "caller_reply_lost":
            await self.publish_without_reply(command)
            await self.r.wait(
                lambda: self.ledger(command),
                lambda p: bool(p["rows"]) and p["rows"][0]["state"] == "completed",
            )
            entry["reply_subscription_absent"] = True
        elif name == "writer_restart_after_effect":
            task = asyncio.create_task(self.submit(command))
            held = await self.r.wait(
                self.proxy,
                lambda p: any(
                    r["case_id"] == name and r["event"] == "ack_held" for r in p["records"]
                ),
            )
            entry["proxy"] = [r for r in held["records"] if r["case_id"] == name]
            entry["first_ledger"] = (await asyncio.to_thread(self.ledger, command))["rows"]
            entry["writer_journals"].append(await asyncio.to_thread(self.journal))
            old = self.r.deployment("actuation-signal-publisher")
            self.persist()
            await self.r.restart("actuation-signal-publisher")
            await self.r.wait(self.proxy, lambda p: p["ready"])
            entry["restart"] = dict(
                before=old, after=self.r.deployment("actuation-signal-publisher")
            )
            first = await task
        elif name == "delayed_expired_command":
            await self.publish_without_reply(command)
            published = await asyncio.to_thread(self.r.source)
            entry["published_clock"] = published["clock"]
            await asyncio.sleep(12)
            held = await asyncio.to_thread(self.r.source)
            entry["held_clock"] = held["clock"]
            reader = self.cluster.target("amqp-actuation-event-subscriber")
            entry["held_replicas"] = json.loads(
                await asyncio.to_thread(
                    reader.run, "get", "deployment", "amqp-actuation-event-subscriber", "-o", "json"
                )
            )["spec"]["replicas"]
            await self.r.scale("amqp-actuation-event-subscriber", 1)
        else:
            first = await self.submit(command)
        if name not in ("writer_restart_after_effect", "delayed_expired_command"):
            await self.r.wait(
                lambda: self.ledger(command),
                lambda p: bool(p["rows"]) and p["rows"][0]["state"] not in ("preparing", "started"),
                45,
            )
        entry["first_result"] = first
        if "first_ledger" not in entry:
            entry["first_ledger"] = (await asyncio.to_thread(self.ledger, command))["rows"]
        entry["first_source"] = await asyncio.to_thread(
            self.r.read,
            "modbus-simulator",
            "source",
            since=before["last_serial"],
            effect_since=before["last_effect"],
        )
        self.persist()
        # Replaying preserves every field; only the transport delivery is new.
        replay = ActuationRequest.model_validate(entry["command"])
        entry["replay_command"] = replay.model_dump(mode="json")
        if name != "writer_restart_after_effect":
            entry["proxy"] = [r for r in self.proxy()["records"] if r["case_id"] == name]
        self.plan(name, "normal", command)
        entry["replay_result"] = await self.submit(replay)
        if name != "delayed_expired_command":
            await self.r.wait(
                lambda: self.ledger(command),
                lambda p: bool(p["rows"]) and p["rows"][0]["state"] not in ("preparing", "started"),
                45,
            )
        if name != "writer_restart_after_effect":
            entry["proxy"] = [r for r in self.proxy()["records"] if r["case_id"] == name]
        await asyncio.sleep(0.3)
        entry["after"] = await asyncio.to_thread(
            self.r.read,
            "modbus-simulator",
            "source",
            since=before["last_serial"],
            effect_since=before["last_effect"],
        )
        writer = await asyncio.to_thread(self.ledger, command)
        entry["ledger"], entry["writer_clock"] = writer["rows"], writer["clock"]
        entry["writer_journals"].append(await asyncio.to_thread(self.journal))
        self.persist()

    async def run(self) -> None:
        source = await asyncio.to_thread(self.r.source)
        self.r.data["original_controls"] = source["state"]["block"]["conveyor"]
        self.data["original_controls"] = self.r.data["original_controls"]
        self.persist()
        await self.r.scale("decision-agent-stub", 0)
        if source["state"]["block"]["conveyor"]["faulted"]:
            await self.r.control("reset")
        await self.r.control("stop")
        # Select distinct physical bottle identities already present in the simulator.
        for _ in range(20):
            source = await asyncio.to_thread(self.r.source)
            reject = source["state"]["block"]["reject"]
            bottles = [b for b in reject["_present"] if b not in reject["_rejected"]]
            if len(bottles) >= 6:
                break
            if source["state"]["block"]["conveyor"]["faulted"]:
                await self.r.control("reset")
            await self.r.control("start")
            await asyncio.sleep(1)
            await self.r.control("stop")
        else:
            raise RuntimeError("six distinct available simulator bottles required")
        await self.install()
        await asyncio.sleep(2)
        for name, bottle in zip(
            json.loads(PROFILE.read_text())["cases"], bottles[-6:], strict=True
        ):
            await self.case(name, bottle)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context")
    parser.add_argument("--namespace", default="eirvah-edge")
    parser.add_argument("--targets", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    routing = json.loads(args.targets.read_text()) if args.targets else {}
    context = args.context or routing.get("default", {}).get("context")
    namespace = routing.get("default", {}).get("namespace", args.namespace)
    if not context:
        parser.error("explicit research context required")
    output = args.output or ROOT / "results/command-faults" / datetime.now(UTC).strftime(
        "%Y-%m-%dT%H-%M-%S-%fZ"
    )
    if output.exists():
        parser.error("output must be a new directory")
    cluster = Cluster(context, namespace, routing.get("components", {}))
    counts = {}
    for role in ("decision-agent-stub", "amqp-actuation-event-subscriber"):
        dep = json.loads(cluster.target(role).run("get", "deployment", role, "-o", "json"))
        counts[role] = dep["spec"]["replicas"]
        if counts[role] != 1:
            parser.error("one active research consumer and command ingress required")
    save(output / "restoration.json", dict(restored=False, replicas=counts))
    save(output / "profile.json", json.loads(PROFILE.read_text()))
    fingerprints = {}
    paths = [
        PROFILE,
        Path(__file__),
        ROOT / "scripts/native_fault_proxy.py",
        ROOT / "scripts/fault_checks.py",
        ROOT / "scripts/recovery_case.py",
    ]
    paths.extend(
        (ROOT / "services/actuation-signal-publisher/src/actuation_signal_publisher").glob("*.py")
    )
    for path in paths:
        relative = path.relative_to(ROOT)
        body = path.read_bytes()
        destination = output / "sources" / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(body)
        fingerprints[relative.as_posix()] = hashlib.sha256(body).hexdigest()
    save(
        output / "provenance.json",
        dict(
            revision=subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
            ).strip(),
            sources=fingerprints,
            worktree_dirty=bool(
                subprocess.check_output(
                    ["git", "status", "--porcelain"], cwd=ROOT, text=True
                ).strip()
            ),
        ),
    )
    encoded = cluster.target("rabbitmq").run(
        "get", "secret", "actuation-security", "-o", "jsonpath={.data.experiment-amqp-url}"
    )
    parts = urlsplit(base64.b64decode(encoded).decode())
    runner = None
    cleanup = []
    with ExitStack() as stack:
        port = stack.enter_context(port_forward(*cluster.transport("rabbitmq", 5672)))
        amqp = urlunsplit(
            (parts.scheme, parts.netloc.split("@")[0] + f"@127.0.0.1:{port}", parts.path, "", "")
        )
        native = stack.enter_context(port_forward(*cluster.transport("modbus-simulator", 5020)))
        recovery = RecoveryRun(cluster, output, amqp, 0, native, json.loads(PROFILE.read_text()))
        runner = FaultRun(recovery)
        with asyncio.Runner(loop_factory=asyncio.SelectorEventLoop) as runtime:
            try:
                runtime.run(runner.run())
            except (Exception, KeyboardInterrupt) as exc:
                runner.data["capture_errors"].append(type(exc).__name__)
                if isinstance(exc, (subprocess.CalledProcessError, subprocess.TimeoutExpired)):
                    save(
                        output / "operation-error.json",
                        {
                            "phase": runner.data.get("phase", "setup"),
                            "stderr": str(exc.stderr)[-4000:],
                            "operation": [str(x) for x in exc.cmd[:9]],
                            "output": str(getattr(exc, "output", ""))[-4000:],
                        },
                    )
                save(
                    output / "capture-error.json",
                    dict(
                        error_type=type(exc).__name__,
                        reason=str(exc)
                        if isinstance(exc, (RuntimeError, TimeoutError))
                        else "capture operation failed",
                        status="unknown",
                    ),
                )
                print("Fault capture incomplete: " + type(exc).__name__, flush=True)
            finally:
                try:
                    runtime.run(runner.restore_writer())
                except (Exception, KeyboardInterrupt):
                    cleanup.append("writer restoration not confirmed")
                # Keep automatic decisions paused until the normal writer and desired controls return.
                try:
                    runtime.run(recovery.restore_controls())
                except (Exception, KeyboardInterrupt):
                    cleanup.append("simulator controls restoration not confirmed")
                for role, count in reversed(list(counts.items())):
                    try:
                        runtime.run(recovery.scale(role, count))
                    except (Exception, KeyboardInterrupt):
                        cleanup.append(role + " restoration not confirmed")
    runner.data["capture_errors"].extend(cleanup)
    runner.data["restored"] = not cleanup
    runner.persist()
    save(output / "restoration.json", dict(restored=not cleanup, errors=cleanup, replicas=counts))
    result = report(runner.data)
    save(output / "report.json", result)
    (output / "report.md").write_text(
        "# Bounded command fault checks\n\nVerdict: **"
        + result["fault_case"]
        + "**. Whole study: **unknown**.\n\n"
        + json.loads(PROFILE.read_text())["scope"]
        + "\n\n| Check | Verdict | Reason |\n| --- | --- | --- |\n"
        + "\n".join(
            "| " + c["check"] + " | " + c["status"] + " | " + c["reason"] + " |"
            for c in result["checks"]
        )
        + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            dict(fault_case=result["fault_case"], report=str(output / "report.md")), indent=2
        )
    )
    raise SystemExit(
        0 if result["fault_case"] == "pass" else 1 if result["fault_case"] == "fail" else 2
    )


if __name__ == "__main__":
    main()
