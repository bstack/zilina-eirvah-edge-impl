"""Run bounded live recovery checks on explicitly selected research deployments.

Mutates only named research deployments and simulator controls; retains volumes.
Restores replica counts, simulator jam configuration and desired conveyor controls.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import os
import subprocess
import time
from collections.abc import Callable
from contextlib import ExitStack
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import aiomqtt
from control import build_command, send
from e0_capture import Cluster, save
from eirvah_contracts.inspection import BottleInspection, InspectionReceipt, inspection_digest
from eirvah_contracts.observation import load_bindings
from eirvah_contracts.sosa import SOSAObservation
from eirvah_contracts.ulid import generate_correlation_id
from pymodbus.client import AsyncModbusTcpClient
from recovery_checks import PROFILE, check_recovery
from scenario import port_forward
from state_event_case import capture

ROOT = Path(__file__).resolve().parents[1]
READ_DATABASE = r"""
import sqlite3,json,sys,time,os,hashlib
from pathlib import Path
args=json.load(sys.stdin);kind=args['kind'];before=time.monotonic_ns()
files={'source':'source.sqlite3','consumer':'consumer.sqlite3','producer':'positions.sqlite3','writer':'commands.sqlite3'}
result={}
if kind in files:
 c=sqlite3.connect('file:/var/lib/eirvah/'+files[kind]+'?mode=ro',uri=True);c.row_factory=sqlite3.Row;c.execute('BEGIN')
 if kind=='source':
  result['meta']={r[0]:r[1] for r in c.execute('SELECT key,value FROM source_meta')}
  result['last_serial']=c.execute('SELECT coalesce(max(serial),0) FROM source_events').fetchone()[0]
  result['last_effect']=c.execute('SELECT coalesce(max(serial),0) FROM native_effects').fetchone()[0]
  result['pending']=c.execute('SELECT count(*) FROM source_events WHERE received=0').fetchone()[0]
  result['events']=[dict(r) for r in c.execute('SELECT * FROM source_events WHERE serial>? ORDER BY serial',(args.get('since',0),))]
  result['effects']=[dict(r) for r in c.execute('SELECT * FROM native_effects WHERE serial>? ORDER BY serial',(args.get('effect_since',0),))]
  result['state']=json.loads(c.execute('SELECT payload FROM source_state WHERE id=1').fetchone()[0]);result['state'].pop('rng',None)
  for row in result['events']+result['effects']:row['payload']=json.loads(row['payload'])
 elif kind=='consumer':
  result['db_id']=c.execute("SELECT value FROM consumer_meta WHERE key='db_id'").fetchone()[0]
  result['last_serial']=c.execute('SELECT coalesce(max(serial),0) FROM inspections').fetchone()[0]
  result['last_audit']=c.execute('SELECT coalesce(max(serial),0) FROM state_audit').fetchone()[0]
  result['positions']=[dict(r) for r in c.execute('SELECT * FROM positions')]
  result['inspections']=[dict(r) for r in c.execute('SELECT * FROM inspections WHERE serial>? ORDER BY serial',(args.get('since',0),))]
  for row in result['inspections']:
   for field in ('payload','command','result'):row[field]=json.loads(row[field]) if row[field] else None
  result['audits']=[dict(r) for r in c.execute('SELECT * FROM state_audit WHERE serial>? AND correlation=? ORDER BY serial LIMIT 10',(args.get('audit_since',0),args['audit_correlation']))] if args.get('audit_correlation') else []
  for row in result['audits']:
   for field in ('payload','decision','snapshot'):row[field]=json.loads(row[field])
 elif kind=='producer':
  result['epochs']=[dict(r) for r in c.execute('SELECT * FROM producer_epoch')]
 elif kind=='writer':
  ids=args.get('ids',[])
  result['rows']=[dict(r) for r in c.execute('SELECT command_id,digest,state,result FROM commands WHERE command_id IN ('+','.join('?' for _ in ids)+')',ids)] if ids else []
  for row in result['rows']:row['result']=json.loads(row['result']) if row['result'] else None
 c.close()
paths=['libs/eirvah-controls/src/eirvah_controls/recovery.py','libs/eirvah-contracts/src/eirvah_contracts/signals.py','libs/eirvah-contracts/src/eirvah_contracts/inspection.py','services/'+args['role']+'/src/'+args['role'].replace('-','_')+'/service.py']
if args['role']=='modbus-simulator':paths[-1]=paths[-1].replace('/service.py','/server.py')
result['hashes']={p:hashlib.sha256((Path('/workspace')/p).read_bytes()).hexdigest() for p in paths if (Path('/workspace')/p).is_file()}
result['clock']={'domain':Path('/proc/sys/kernel/random/boot_id').read_text().strip()+':'+os.readlink('/proc/self/ns/time'),'wall':time.time(),'before_ns':before,'after_ns':time.monotonic_ns()}
print(json.dumps(result))
"""


class RecoveryRun:
    def __init__(
        self,
        cluster: Cluster,
        output: Path,
        amqp: str,
        mqtt: int,
        native: int,
        profile: dict[str, Any],
    ) -> None:
        self.cluster = cluster
        self.output = output
        self.amqp = amqp
        self.mqtt = mqtt
        self.native = native
        self.profile = profile
        self.data: dict[str, Any] = dict(
            schema_version="1.0",
            mode="live_cluster_recovery",
            profile_sha256=hashlib.sha256(PROFILE.read_bytes()).hexdigest(),
            scope=profile["scope"],
            capture_errors=[],
        )
        self.source_since = 0
        self.consumer_since = 0
        self.effect_since = 0
        self.audit_since = 0

    def persist(self) -> None:
        save(self.output / "evidence.json", self.data)

    def read(self, role: str, kind: str, **kwargs: Any) -> dict[str, Any]:
        reader = self.cluster.target(role)
        result: dict[str, Any] = json.loads(
            reader.run(
                "exec",
                "-i",
                "deployment/" + role,
                "--",
                "python",
                "-c",
                READ_DATABASE,
                input=json.dumps(dict(role=role, kind=kind, **kwargs)),
            )
        )
        for path, digest in result["hashes"].items():
            if (
                role != "actuation-signal-publisher"
                and hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != digest
            ):
                raise RuntimeError("deployed build differs: " + role)
        return result

    def source(self) -> dict[str, Any]:
        return self.read(
            "modbus-simulator", "source", since=self.source_since, effect_since=self.effect_since
        )

    def consumer(self) -> dict[str, Any]:
        return self.read(
            "decision-agent-stub",
            "consumer",
            since=self.consumer_since,
            audit_since=self.audit_since,
        )

    def deployment(self, role: str) -> dict[str, Any]:
        reader = self.cluster.target(role)
        pods = json.loads(
            reader.run("get", "pods", "-l", "app.kubernetes.io/name=" + role, "-o", "json")
        )["items"]
        pods = [p for p in pods if not p["metadata"].get("deletionTimestamp")]
        if len(pods) != 1:
            raise RuntimeError("single authoritative pod required: " + role)
        pod = pods[0]
        volumes = {}
        for volume in pod["spec"].get("volumes", []):
            if "persistentVolumeClaim" in volume:
                name = volume["persistentVolumeClaim"]["claimName"]
                pvc = json.loads(reader.run("get", "pvc", name, "-o", "json"))
                volumes[name] = pvc["metadata"]["uid"]
        return dict(
            uid=pod["metadata"]["uid"],
            volumes=volumes,
            images=[c["imageID"] for c in pod["status"].get("containerStatuses", [])],
        )

    async def restart(self, role: str) -> None:
        print("Restarting " + role, flush=True)
        reader = self.cluster.target(role)
        await asyncio.to_thread(reader.run, "rollout", "restart", "deployment/" + role)
        await asyncio.to_thread(
            reader.run, "rollout", "status", "deployment/" + role, "--timeout=240s", timeout=250
        )

    async def scale(self, role: str, count: int) -> None:
        reader = self.cluster.target(role)
        await asyncio.to_thread(
            reader.run, "scale", "deployment/" + role, "--replicas=" + str(count)
        )
        if count:
            await asyncio.to_thread(
                reader.run, "rollout", "status", "deployment/" + role, "--timeout=180s", timeout=190
            )
        else:
            await asyncio.to_thread(
                reader.run,
                "wait",
                "--for=delete",
                "pod",
                "-l",
                "app.kubernetes.io/name=" + role,
                "--timeout=90s",
                timeout=100,
            )

    async def wait(
        self,
        read: Callable[[], dict[str, Any]],
        predicate: Callable[[dict[str, Any]], bool],
        seconds: float = 90,
    ) -> dict[str, Any]:
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            try:
                result = await asyncio.to_thread(read)
                if predicate(result):
                    return result
            except (subprocess.SubprocessError, OSError, ValueError):
                pass
            await asyncio.sleep(1)
        raise TimeoutError("recovery observation deadline")

    async def control(self, action: str, value: Any = None) -> None:
        clock = (await asyncio.to_thread(self.source))["clock"]["wall"]
        now = datetime.fromtimestamp(clock, UTC)
        command = build_command("conveyor", action, value).model_copy(
            update={"requested_at": now, "deadline": now + timedelta(seconds=10)}
        )
        result = await send(command, self.amqp)
        save(
            self.output / ("control-" + command.correlation_id + ".json"),
            dict(command=command.model_dump(mode="json"), result=result),
        )
        if result.get("decision") != "approve":
            raise RuntimeError("conveyor control refused: " + action)

    async def packet(
        self, topic: str, predicate: Callable[[dict[str, Any]], bool] | None = None
    ) -> dict[str, Any]:
        reference = (await asyncio.to_thread(self.source))["clock"]["wall"]
        started = time.monotonic()
        async with asyncio.timeout(90):
            async with aiomqtt.Client(
                "127.0.0.1",
                self.mqtt,
                username=os.environ.get("EIRVAH_MQTT_USERNAME", "eirvah"),
                password=os.environ.get("EIRVAH_MQTT_PASSWORD", "eirvah-dev-password"),
            ) as client:
                await client.subscribe(topic, qos=1)
                async for message in client.messages:
                    item: dict[str, Any] = json.loads(message.payload)
                    age = (
                        reference
                        + (time.monotonic() - started)
                        - datetime.fromisoformat(item["sosa:resultTime"]).timestamp()
                    )
                    if not 0 <= age <= 5:
                        continue
                    if item.get("eirvah:position") and (predicate is None or predicate(item)):
                        return item
        raise TimeoutError("no fresh positioned packet")

    async def publish(self, topic: str, payload: dict[str, Any]) -> None:
        async with aiomqtt.Client(
            "127.0.0.1",
            self.mqtt,
            username=os.environ.get("EIRVAH_MQTT_USERNAME", "eirvah"),
            password=os.environ.get("EIRVAH_MQTT_PASSWORD", "eirvah-dev-password"),
        ) as client:
            await client.publish(topic, json.dumps(payload).encode(), qos=1, retain=False)

    async def drain(self) -> None:
        await self.wait(self.source, lambda x: x["pending"] == 0, 120)
        await self.wait(
            self.consumer,
            lambda x: all(r["disposition"] != "pending" for r in x["inspections"]),
            90,
        )
        await asyncio.sleep(1)

    async def run(self) -> dict[str, Any]:
        topic = self.profile["topics"]["modbus"]
        self.data["phase"] = "initialization"
        initial = await asyncio.to_thread(self.source)
        self.data["original_controls"] = initial["state"]["block"]["conveyor"]
        self.persist()
        reader = self.cluster.target("modbus-simulator")
        deployment = json.loads(
            await asyncio.to_thread(
                reader.run, "get", "deployment", "modbus-simulator", "-o", "json"
            )
        )
        binding = next(
            (
                e
                for e in deployment["spec"]["template"]["spec"]["containers"][0].get("env", [])
                if e["name"] == "MODBUS_SIMULATOR_JAM_PROBABILITY"
            ),
            None,
        )
        self.data["original_jam_binding"] = binding
        self.persist()
        await asyncio.to_thread(
            reader.run,
            "set",
            "env",
            "deployment/modbus-simulator",
            "MODBUS_SIMULATOR_JAM_PROBABILITY=0",
        )
        await asyncio.to_thread(
            reader.run,
            "rollout",
            "status",
            "deployment/modbus-simulator",
            "--timeout=240s",
            timeout=250,
        )
        await self.wait(self.source, lambda x: bool(x["state"]))
        now = await asyncio.to_thread(self.source)
        if now["state"]["block"]["conveyor"]["faulted"]:
            await self.control("reset")
        await self.control("stop")
        await self.drain()
        start_source = await asyncio.to_thread(self.source)
        start_consumer = await asyncio.to_thread(self.consumer)
        self.source_since = start_source["last_serial"]
        self.effect_since = start_source["last_effect"]
        self.consumer_since = start_consumer["last_serial"]
        self.audit_since = start_consumer["last_audit"]
        self.data["start"] = dict(source=start_source, consumer=start_consumer)
        self.persist()
        await self.control("speed", 0.4)
        await self.control("start")
        self.data["positions"] = {}
        for role, binding in self.profile["topics"].items():
            self.data["positions"][role] = await self.packet(binding)
        baseline = await self.wait(
            self.consumer,
            lambda x: any(r["disposition"] == "completed" for r in x["inspections"]),
            90,
        )
        original = next(r for r in baseline["inspections"] if r["disposition"] == "completed")
        self.data["seed_inspection"] = original
        self.persist()
        self.data["phase"] = "producer_restart"
        self.persist()
        before_uid = self.deployment("modbus-data-subscriber")
        before_packet = await self.packet(topic)
        await self.restart("modbus-data-subscriber")
        after_packet = await self.packet(
            topic,
            lambda p: p["eirvah:position"]["epoch"] > before_packet["eirvah:position"]["epoch"],
        )
        after_uid = self.deployment("modbus-data-subscriber")
        self.data["producer_restart"] = dict(
            before_uid=before_uid["uid"],
            after_uid=after_uid["uid"],
            before_volume=before_uid["volumes"],
            after_volume=after_uid["volumes"],
            before_packet=before_packet,
            after_packet=after_packet,
        )
        self.persist()
        injection = dict(before_packet)
        identifier = generate_correlation_id()
        current = await asyncio.to_thread(self.consumer)
        injection.update(
            {
                "sosa:resultTime": datetime.fromtimestamp(
                    current["clock"]["wall"], UTC
                ).isoformat(),
                "sosa:hasSimpleResult": 0.7,
                "eirvah:correlationId": identifier,
            }
        )
        await self.publish(topic, injection)
        observed = await self.wait(
            lambda: self.read(
                "decision-agent-stub",
                "consumer",
                since=self.consumer_since,
                audit_since=self.audit_since,
                audit_correlation=identifier,
            ),
            lambda x: any(r["correlation"] == identifier for r in x["audits"]),
        )
        self.data["old_state_audit"] = next(
            r for r in observed["audits"] if r["correlation"] == identifier
        )
        self.persist()
        pub_before = self.deployment("mqtt-uns-publisher")
        pub_packet = await self.packet(topic)
        await self.restart("mqtt-uns-publisher")
        pub_after = await self.packet(
            topic,
            lambda p: (
                p["eirvah:position"]["epoch"] == pub_packet["eirvah:position"]["epoch"]
                and p["eirvah:position"]["sequence"] > pub_packet["eirvah:position"]["sequence"]
            ),
        )
        self.data["publisher_restart"] = dict(
            before_uid=pub_before["uid"],
            after_uid=self.deployment("mqtt-uns-publisher")["uid"],
            before_packet=pub_packet,
            after_packet=pub_after,
        )
        self.persist()
        self.data["phase"] = "consumer_and_source_restart"
        self.persist()
        consumer_before = self.deployment("decision-agent-stub")
        consumer_data = await asyncio.to_thread(self.consumer)
        before_row = next(r for r in consumer_data["inspections"] if r["id"] == original["id"])
        # Hold command ingress while one new inspection is durably owned but not executed.
        await self.scale("amqp-actuation-event-subscriber", 0)
        pending_data = await self.wait(
            self.consumer,
            lambda x: any(r["disposition"] == "pending" for r in x["inspections"]),
            60,
        )
        pending_row = next(r for r in pending_data["inspections"] if r["disposition"] == "pending")
        self.data["pending_recovery"] = {"before_row": pending_row}
        self.persist()
        await self.scale("decision-agent-stub", 0)
        await asyncio.sleep(2)
        held = await self.wait(self.source, lambda x: any(not r["received"] for r in x["events"]))
        head = next(r for r in held["events"] if not r["received"])
        forged = InspectionReceipt(
            inspection_id=head["id"],
            event_digest=inspection_digest(BottleInspection.model_validate(head["payload"])),
            consumer="decision-agent-stub",
            signature="0" * 64,
        )
        await self.publish(self.profile["receipt_topic"], forged.model_dump(mode="json"))
        await asyncio.sleep(1)
        held_after = await asyncio.to_thread(self.source)
        self.data["forged_receipt"] = dict(
            before=head, after=next(r for r in held_after["events"] if r["id"] == head["id"])
        )
        self.persist()
        source_before = self.deployment("modbus-simulator")
        source_data = await asyncio.to_thread(self.source)
        await self.restart("modbus-simulator")
        source_after = await self.wait(
            self.source, lambda x: int(x["meta"]["boots"]) > int(source_data["meta"]["boots"])
        )
        self.data["source_restart"] = dict(
            before_uid=source_before["uid"],
            after_uid=self.deployment("modbus-simulator")["uid"],
            before=source_data,
            after=source_after,
        )
        self.persist()
        await self.scale("decision-agent-stub", 1)
        await self.wait(self.consumer, lambda x: x["db_id"] == consumer_data["db_id"])
        event = original["payload"]
        binding = load_bindings(ROOT / "config/eirvah-line-a.jsonld")[
            self.profile["inspection_topic"]
        ]
        payload = SOSAObservation(
            value_type="string",
            made_by_sensor=binding["sensor"],
            has_feature_of_interest=binding["feature"],
            observed_property=binding["property"],
            has_simple_result=json.dumps(event),
            result_time=datetime.fromisoformat(event["inspected_at"]),
            unit="json",
            quality="good",
            correlation_id=generate_correlation_id(),
        ).to_jsonld()
        await self.publish(self.profile["inspection_topic"], payload)
        await asyncio.sleep(2)
        final_consumer = await asyncio.to_thread(self.consumer)
        self.data["consumer_restart"] = dict(
            before_uid=consumer_before["uid"],
            after_uid=self.deployment("decision-agent-stub")["uid"],
            before_db=consumer_data["db_id"],
            after_db=final_consumer["db_id"],
            before_row=before_row,
            after_row=next(r for r in final_consumer["inspections"] if r["id"] == original["id"]),
        )
        self.persist()
        # The original deadline has elapsed; recovering its identity must not renew it.
        await self.scale("amqp-actuation-event-subscriber", 1)
        settled = await self.wait(
            self.consumer,
            lambda x: any(
                r["id"] == pending_row["id"] and r["disposition"] != "pending"
                for r in x["inspections"]
            ),
            90,
        )
        self.data["pending_recovery"]["after_row"] = next(
            r for r in settled["inspections"] if r["id"] == pending_row["id"]
        )
        self.data["phase"] = "broker_outage"
        self.persist()
        await self.scale("mosquitto", 0)
        await asyncio.sleep(self.profile["outage_seconds"])
        outage_source = await asyncio.to_thread(self.source)
        replicas = json.loads(
            await asyncio.to_thread(
                self.cluster.target("mosquitto").run, "get", "deployment", "mosquitto", "-o", "json"
            )
        )["spec"]["replicas"]
        self.data["outage"] = dict(
            replicas_during=replicas, source_pending_during=outage_source["pending"]
        )
        self.persist()
        await self.scale("mosquitto", 1)
        # Existing loopback port-forwards bind pods: renew them after broker replacement.
        return self.data

    async def finish(self) -> dict[str, Any]:
        self.data["phase"] = "final_reconciliation"
        self.persist()
        await self.wait(self.consumer, lambda x: bool(x["positions"]))
        await self.control("stop")
        await self.drain()
        source = await asyncio.to_thread(self.source)
        consumer = await asyncio.to_thread(self.consumer)
        ids = [r["command_id"] for r in consumer["inspections"] if r["command_id"]]
        writer = await asyncio.to_thread(self.read, "actuation-signal-publisher", "writer", ids=ids)
        self.data["end"] = dict(
            source=source, consumer=consumer, writer=writer["rows"], writer_capture=writer
        )
        self.data["clock_shared"] = (
            len({source["clock"]["domain"], consumer["clock"]["domain"], writer["clock"]["domain"]})
            == 1
        )
        self.persist()
        return self.data

    async def restore_controls(self) -> None:
        original = self.data.get("original_controls")
        if original is None:
            return
        source = await asyncio.to_thread(self.source)
        if source["state"]["block"]["conveyor"]["faulted"] and not original["faulted"]:
            await self.control("reset")
        await self.control("speed", original["setpoint"] / 100)
        if original["faulted"]:
            async with AsyncModbusTcpClient("127.0.0.1", port=self.native) as client:
                previous = await client.read_holding_registers(50, count=2, slave=1)
                sequence = previous.registers[0] % 65535 + 1
                await client.write_registers(21, values=[4, 1], slave=1)
                await client.write_register(20, value=sequence, slave=1)
                for _ in range(80):
                    ack = await client.read_holding_registers(50, count=2, slave=1)
                    if ack.registers == [sequence, 1]:
                        break
                    await asyncio.sleep(0.1)
                else:
                    raise RuntimeError("original fault state not restored")
        else:
            await self.control("start" if original["running"] else "stop")
        restored = await asyncio.to_thread(self.source)
        actual = restored["state"]["block"]["conveyor"]
        if any(actual[k] != original[k] for k in ("setpoint", "running", "faulted")):
            raise RuntimeError("desired conveyor state not restored")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context")
    parser.add_argument("--namespace", default="eirvah-edge")
    parser.add_argument("--targets", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    routing = json.loads(args.targets.read_text()) if args.targets else {}
    default = routing.get("default", {})
    context = args.context or default.get("context")
    namespace = default.get("namespace", args.namespace)
    if not context:
        parser.error("explicit context required")
    output = args.output or ROOT / "results/live-recovery" / datetime.now(UTC).strftime(
        "%Y-%m-%dT%H-%M-%S-%fZ"
    )
    if output.exists():
        parser.error("output must be a new directory")
    output.mkdir(parents=True)
    profile = json.loads(PROFILE.read_text())
    save(output / "profile.json", profile)
    paths = [PROFILE, Path(__file__), ROOT / "scripts/recovery_checks.py", ROOT / "uv.lock"]
    for family in (ROOT / "libs", ROOT / "services"):
        paths.extend(p for p in family.glob("*/src/**/*.py") if "__pycache__" not in p.parts)
    paths.extend((ROOT / "deploy/k3s/base").glob("*/deployment.yaml"))
    paths.extend((ROOT / "deploy/k3s/base").glob("*/pvc.yaml"))
    fingerprints = {}
    for path in paths:
        relative = path.relative_to(ROOT)
        body = path.read_bytes()
        saved = output / "sources" / relative
        saved.parent.mkdir(parents=True, exist_ok=True)
        saved.write_bytes(body)
        fingerprints[relative.as_posix()] = hashlib.sha256(body).hexdigest()
    save(
        output / "provenance.json",
        {
            "revision": subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
            ).strip(),
            "worktree_dirty": bool(
                subprocess.check_output(
                    ["git", "status", "--porcelain"], cwd=ROOT, text=True
                ).strip()
            ),
            "sources": fingerprints,
            "targets": {
                "default": {"context": context, "namespace": namespace},
                "components": routing.get("components", {}),
            },
        },
    )
    cluster = Cluster(context, namespace, routing.get("components", {}))
    restore = {}
    runner = None
    data = None
    cleanup = []
    for role in ("decision-agent-stub", "mosquitto", "amqp-actuation-event-subscriber"):
        dep = json.loads(cluster.target(role).run("get", "deployment", role, "-o", "json"))
        restore[role] = dep["spec"]["replicas"]
        if restore[role] != 1:
            parser.error("recovery test requires one active " + role)
    save(output / "restoration.json", dict(replicas=restore, restored=False))
    encoded = cluster.target("rabbitmq").run(
        "get", "secret", "actuation-security", "-o", "jsonpath={.data.experiment-amqp-url}"
    )
    parts = urlsplit(base64.b64decode(encoded).decode())
    try:
        with ExitStack() as stack:
            amqp_port = stack.enter_context(port_forward(*cluster.transport("rabbitmq", 5672)))
            native_port = stack.enter_context(
                port_forward(*cluster.transport("modbus-simulator", 5020))
            )
            mqtt_port = stack.enter_context(port_forward(*cluster.transport("mosquitto", 1883)))
            amqp = urlunsplit(
                (
                    parts.scheme,
                    parts.netloc.split("@")[0] + f"@127.0.0.1:{amqp_port}",
                    parts.path,
                    "",
                    "",
                )
            )
            runner = RecoveryRun(cluster, output, amqp, mqtt_port, native_port, profile)
            with asyncio.Runner(loop_factory=asyncio.SelectorEventLoop) as runtime:
                data = runtime.run(runner.run())
                # Renew broker and native forwards after pod replacements.
                runner.mqtt = stack.enter_context(
                    port_forward(*cluster.transport("mosquitto", 1883))
                )
                runner.native = stack.enter_context(
                    port_forward(*cluster.transport("modbus-simulator", 5020))
                )
                data = runtime.run(runner.finish())
    except (Exception, KeyboardInterrupt) as exc:
        if runner is not None:
            runner.data["capture_errors"].append(type(exc).__name__)
            runner.persist()
        save(output / "capture-error.json", dict(error_type=type(exc).__name__, status="unknown"))
        print("Recovery capture incomplete: " + type(exc).__name__, flush=True)
    finally:
        for role, count in restore.items():
            try:
                reader = cluster.target(role)
                reader.run("scale", "deployment/" + role, "--replicas=" + str(count))
                reader.run("rollout", "status", "deployment/" + role, "--timeout=180s", timeout=190)
            except (subprocess.SubprocessError, OSError):
                cleanup.append(role + " restoration not confirmed")
        if runner is not None and "original_jam_binding" in runner.data:
            try:
                binding = runner.data["original_jam_binding"]
                reader = cluster.target("modbus-simulator")
                if binding is None:
                    reader.run(
                        "set",
                        "env",
                        "deployment/modbus-simulator",
                        "MODBUS_SIMULATOR_JAM_PROBABILITY-",
                    )
                else:
                    env = {
                        **binding,
                        "value": binding.get("value"),
                        "valueFrom": binding.get("valueFrom"),
                    }
                    reader.run(
                        "patch",
                        "deployment",
                        "modbus-simulator",
                        "--type=strategic",
                        "-p",
                        json.dumps(
                            {
                                "spec": {
                                    "template": {
                                        "spec": {
                                            "containers": [
                                                {"name": "modbus-simulator", "env": [env]}
                                            ]
                                        }
                                    }
                                }
                            }
                        ),
                    )
                reader.run(
                    "rollout",
                    "status",
                    "deployment/modbus-simulator",
                    "--timeout=240s",
                    timeout=250,
                )
                with ExitStack() as forward:
                    runner.native = forward.enter_context(
                        port_forward(*cluster.transport("modbus-simulator", 5020))
                    )
                    rabbit = forward.enter_context(
                        port_forward(*cluster.transport("rabbitmq", 5672))
                    )
                    runner.amqp = urlunsplit(
                        (
                            parts.scheme,
                            parts.netloc.split("@")[0] + f"@127.0.0.1:{rabbit}",
                            parts.path,
                            "",
                            "",
                        )
                    )
                    with asyncio.Runner(loop_factory=asyncio.SelectorEventLoop) as runtime:
                        runtime.run(runner.restore_controls())
            except (Exception, KeyboardInterrupt):
                cleanup.append("source controls/config restoration not confirmed")
        save(
            output / "restoration.json",
            dict(replicas=restore, restored=not cleanup, errors=cleanup),
        )
    if data is None:
        raise SystemExit(2)
    data["capture_errors"].extend(cleanup)
    save(output / "evidence.json", data)
    capture(output / "consumer-replay")
    checks = check_recovery(data)
    status = (
        "fail"
        if any(c["status"] == "fail" for c in checks)
        else "unknown"
        if any(c["status"] != "pass" for c in checks)
        else "pass"
    )
    report = dict(
        recovery_case=status,
        scope=profile["scope"],
        whole_study="unknown",
        checks=checks,
        consumer_evidence=str(output / "consumer-replay/evidence.json"),
    )
    save(output / "report.json", report)
    (output / "report.md").write_text(
        "# Bounded live C05/C06 recovery\n\nVerdict: **"
        + status
        + "**. Whole study: **unknown**.\n\n"
        + profile["scope"]
        + "\n\n| Check | Verdict |\n| --- | --- |\n"
        + "\n".join("| " + c["check"] + " | " + c["status"] + " |" for c in checks)
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps(dict(recovery_case=status, report=str(output / "report.md")), indent=2))
    raise SystemExit(0 if status == "pass" else 1 if status == "fail" else 2)


if __name__ == "__main__":
    main()
