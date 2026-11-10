"""Isolated native E2 rehearsal: actual adapters, NATS pipeline and MQTT, no actuation."""

from __future__ import annotations

import argparse
import asyncio
import copy
import importlib.metadata
import json
import math
import os
import re
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import aiomqtt
import nats
import yaml
from asyncua import Client as UaClient
from decision_agent_stub.service import ProductionModeRule
from eirvah_contracts.observation import load_bindings, validate_observation
from eirvah_observability.experiment import emit, phase
from interoperability import (
    PROFILE,
    ROOT,
    command_meaning,
    digest,
    load_profile,
    local_meaning,
    settings,
    source_specific_value,
)
from interoperability import SOURCES as REPLAY_SOURCES
from pymodbus.client import AsyncModbusTcpClient
from snap7.client import Client as S7Client

SOURCES = [
    *REPLAY_SOURCES,
    Path(__file__),
    ROOT / "scripts/e2_native_fixture.py",
    ROOT / "scripts/e2_source_addition.py",
    ROOT / "config/pipelines/uns-contextualizer.yaml",
    ROOT / "uv.lock",
    ROOT / "libs/eirvah-controls/src/eirvah_controls/recovery.py",
    *[
        ROOT / "libs/eirvah-bus/src/eirvah_bus" / name
        for name in ("client.py", "consumer.py", "request_reply.py")
    ],
    *[
        ROOT / "services/uns-contextualizer-orchestrator/src/uns_contextualizer_orchestrator" / name
        for name in ("service.py", "pipeline.py", "metrics.py")
    ],
]
ROLE_CODE = "import asyncio,importlib,json,sys\nfrom pathlib import Path\nspec=json.loads(Path(sys.argv[1]).read_text())\nmodule=importlib.import_module(spec['module']+'.service')\nconfig=importlib.import_module(spec['module']+'.config')\nrole=getattr(module,spec['role'])(getattr(config,spec['settings'])(**spec['values']))\nwith asyncio.Runner(loop_factory=asyncio.SelectorEventLoop) as runner:\n    runner.run(role.run())\n"


def save(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def selected_cases(profile: dict[str, Any]) -> list[dict[str, Any]]:
    by_id = {s["id"]: s for s in profile["samples"]}
    return [
        dict(speed=by_id[speed], temperature=by_id[temp], count=by_id[count])
        for speed, temp, count in (
            ("speed-normal", "temperature-ambient", "count-0"),
            ("speed-high", "temperature-warm", "count-1"),
            ("speed-upper-bound", "temperature-upper", "count-32767"),
            ("speed-over-range", "temperature-ambient", "count-0"),
        )
    ]


class OwnedResources:
    def __init__(self, output: Path) -> None:
        self.output = output
        self.run_id = output.name.lower().replace("_", "-")
        self.containers: list[dict[str, Any]] = []
        self.processes: list[tuple[subprocess.Popen[Any], Any, str]] = []

    def docker(self, *args: str) -> str:
        return subprocess.check_output(
            ["docker", *args], text=True, encoding="utf-8", stderr=subprocess.PIPE, timeout=45
        ).strip()

    def container(
        self,
        name: str,
        image: str,
        ports: list[int],
        extra: list[str],
        command: list[str] | None = None,
    ) -> dict[str, int]:
        options = [
            "run",
            "-d",
            "--label",
            "eirvah.e2.run=" + self.run_id,
            "--name",
            "eirvah-e2-" + name + "-" + self.run_id[-18:],
        ]
        for port in ports:
            options.extend(["-p", f"127.0.0.1::{port}"])
        cid = self.docker(*options, *extra, image, *(command or []))
        if not re.fullmatch("[a-f0-9]{64}", cid):
            raise RuntimeError("Unexpected container identity")
        emit("container_started", name=name, container_id=cid)
        item = dict(id=cid, name=name, image=image)
        self.containers.append(item)
        info = json.loads(self.docker("inspect", cid))[0]
        item["image_id"] = info["Image"]
        item["ports"] = info["NetworkSettings"]["Ports"]
        return {
            str(p): int(info["NetworkSettings"]["Ports"][f"{p}/tcp"][0]["HostPort"]) for p in ports
        }

    def role(
        self, name: str, module: str, role: str, settings_name: str, values: dict[str, Any]
    ) -> subprocess.Popen[Any]:
        config = self.output / (name + "-config.json")
        save(config, dict(module=module, role=role, settings=settings_name, values=values))
        log = (self.output / (name + ".log")).open("w", encoding="utf-8")
        process = subprocess.Popen(
            [sys.executable, "-u", "-c", ROLE_CODE, str(config.resolve())],
            cwd=ROOT,
            stdout=log,
            stderr=subprocess.STDOUT,
            env=os.environ.copy(),
        )
        self.processes.append((process, log, name))
        emit("process_started", name=name, process_pid=process.pid)
        return process

    def stop(self, process: subprocess.Popen[Any]) -> None:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=8)

    def cleanup(self) -> dict[str, Any]:
        errors = []
        for process, log, name in reversed(self.processes):
            try:
                self.stop(process)
            except (OSError, subprocess.SubprocessError):
                errors.append("process " + name)
            log.close()
        for item in reversed(self.containers):
            try:
                info = json.loads(self.docker("inspect", item["id"]))[0]
                if info["Config"]["Labels"].get("eirvah.e2.run") != self.run_id:
                    raise RuntimeError("Ownership changed")
                try:
                    logs = subprocess.run(
                        ["docker", "logs", item["id"]],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        text=True,
                        encoding="utf-8",
                        check=True,
                        timeout=15,
                    ).stdout
                    (self.output / (item["name"] + "-container.log")).write_text(
                        logs, encoding="utf-8"
                    )
                except (OSError, subprocess.SubprocessError):
                    (self.output / (item["name"] + "-log-unavailable.txt")).write_text(
                        "Log collection failed; container removal still attempted.\n",
                        encoding="utf-8",
                    )
                self.docker("rm", "-f", item["id"])
                if (
                    subprocess.run(
                        ["docker", "inspect", item["id"]], capture_output=True
                    ).returncode
                    == 0
                ):
                    raise RuntimeError("Removal not confirmed")
            except (OSError, RuntimeError, ValueError, subprocess.SubprocessError):
                errors.append("container " + item["name"])
        return dict(confirmed=not errors, errors=errors)


async def until(predicate: Any, description: str, timeout: float = 45) -> None:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return
        await asyncio.sleep(0.1)
    raise TimeoutError(description)


async def read_native(protocol: str, ports: dict[str, int]) -> dict[str, Any]:
    if protocol == "opcua":
        async with UaClient(f"opc.tcp://127.0.0.1:{ports['4840']}/e2/") as client:
            idx = await client.get_namespace_index("urn:eirvah:e2")
            values = {}
            for key in ("speed", "temperature", "count"):
                node = await client.nodes.objects.get_child([f"{idx}:Signals", f"{idx}:{key}"])
                values[key] = await node.read_value()
            return values
    if protocol == "modbus":
        async with AsyncModbusTcpClient("127.0.0.1", port=ports["5020"]) as client:
            result = await client.read_holding_registers(0, count=3, slave=1)
            if result.isError():
                raise RuntimeError("Native read failed")
            return dict(zip(("speed", "temperature", "count"), result.registers, strict=True))

    def read_s7() -> dict[str, Any]:
        client = S7Client()
        try:
            client.connect("127.0.0.1", 0, 1, tcp_port=ports["1102"])
            raw = client.db_read(1, 0, 10)
            return dict(speed=raw[:4].hex(), temperature=raw[4:8].hex(), count=raw[8:10].hex())
        finally:
            client.disconnect()
            client.destroy()

    return await asyncio.to_thread(read_s7)


def adapter_config(protocol: str, ports: dict[str, int], profile: dict[str, Any]) -> dict[str, Any]:
    signals = profile["signals"]
    if protocol == "opcua":
        return dict(
            endpoint=f"opc.tcp://127.0.0.1:{ports['4840']}/e2/",
            namespace_uri="urn:eirvah:e2",
            publishing_interval_ms=100,
            nodes=[
                dict(browse_names=["Signals", key], alias=b["node"]) for key, b in signals.items()
            ],
        )
    if protocol == "modbus":
        return dict(
            host="127.0.0.1",
            port=ports["5020"],
            unit_id=1,
            poll_interval_ms=200,
            registers=[
                dict(
                    address=i,
                    alias=signals[key]["node"],
                    scale=100 if key == "speed" else 10 if key == "temperature" else 1,
                    value_type=signals[key]["value_type"],
                )
                for i, key in enumerate(("speed", "temperature", "count"))
            ],
        )
    return dict(
        host="127.0.0.1",
        port=ports["1102"],
        rack=0,
        slot=1,
        poll_interval_ms=200,
        data_blocks=[
            dict(
                db=1,
                size=10,
                signals=[
                    dict(
                        offset=offset,
                        type="int" if key == "count" else "real",
                        alias=signals[key]["node"],
                        value_type=signals[key]["value_type"],
                    )
                    for key, offset in (("speed", 0), ("temperature", 4), ("count", 8))
                ],
            )
        ],
    )


async def capture(resources: OwnedResources, data: dict[str, Any]) -> None:
    output = resources.output
    profile = load_profile(PROFILE)
    directory = output / "fixture"
    directory.mkdir()
    mqtt_config = output / "mosquitto.conf"
    mqtt_config.write_text(
        "listener 1883\nallow_anonymous true\npersistence false\n", encoding="utf-8"
    )
    nats_ports = resources.container("nats", "nats:2.10-alpine", [4222], [])
    mqtt_ports = resources.container(
        "mqtt",
        "eclipse-mosquitto:2",
        [1883],
        [
            "--mount",
            f"type=bind,source={mqtt_config.resolve()},target=/mosquitto/config/mosquitto.conf,readonly",
        ],
    )
    fixture_ports = resources.container(
        "fixture",
        "actuation-signal-publisher:local",
        [4840, 5020, 1102],
        [
            "--entrypoint",
            "python",
            "--mount",
            f"type=bind,source={(ROOT / 'scripts/e2_native_fixture.py').resolve()},target=/e2_native_fixture.py,readonly",
            "--mount",
            f"type=bind,source={directory.resolve()},target=/cases",
        ],
        ["/e2_native_fixture.py"],
    )
    data["endpoints"] = dict(
        nats=nats_ports, mqtt=mqtt_ports, native=fixture_ports, host="127.0.0.1"
    )
    await until(lambda: (directory / "ready.json").exists(), "native fixture ready")
    url = f"nats://127.0.0.1:{nats_ports['4222']}"
    broker = await nats.connect(url)
    raw_messages: list[dict[str, Any]] = []
    dlq_messages: list[dict[str, Any]] = []
    packets: list[dict[str, Any]] = []
    data["transport_records"] = dict(raw=raw_messages, mqtt=packets, dlq=dlq_messages)

    async def raw_message(msg: Any) -> None:
        raw_messages.append(json.loads(msg.data))

    async def dlq_message(msg: Any) -> None:
        dlq_messages.append(json.loads(msg.data))

    await broker.subscribe("uns.ingress.raw", cb=raw_message)
    await broker.subscribe("uns.dlq.telemetry", cb=dlq_message)
    await broker.flush()
    values: dict[str, Any] = dict(nats_servers=[url])
    roles: list[tuple[str, str, str, str, dict[str, Any], str]] = [
        (
            "convert",
            "data_converter",
            "DataConverterWorker",
            "DataConverterSettings",
            values
            | dict(rules_path=str(ROOT / "deploy/k3s/base/data-converter/conversion-rules.yaml")),
            "data_converter_ready",
        ),
        (
            "context",
            "uns_auto_contextualizer",
            "AutoContextualizerWorker",
            "AutoContextualizerSettings",
            values | dict(ontology_path=str(ROOT / "config/eirvah-line-a.jsonld")),
            "contextualizer_ready",
        ),
        (
            "publish",
            "mqtt_uns_publisher",
            "MqttPublisherWorker",
            "MqttPublisherSettings",
            values
            | dict(
                mqtt_host="127.0.0.1",
                mqtt_port=mqtt_ports["1883"],
                mqtt_username="",
                mqtt_password="",
                mqtt_client_id="e2-isolated",
                retain=False,
            ),
            "mqtt_connected",
        ),
        (
            "pipeline",
            "uns_contextualizer_orchestrator",
            "OrchestratorRuntime",
            "OrchestratorSettings",
            values
            | dict(pipeline_config_path=str(ROOT / "config/pipelines/uns-contextualizer.yaml")),
            "orchestrator_ready",
        ),
    ]
    for name, module, role, cfg, params, _ in roles:
        resources.role(name, module, role, cfg, params)
    async with aiomqtt.Client(hostname="127.0.0.1", port=mqtt_ports["1883"]) as mqtt:
        await mqtt.subscribe("uniza/#", qos=1)

        async def consume() -> None:
            async for msg in mqtt.messages:
                packets.append(dict(topic=str(msg.topic), packet=json.loads(bytes(msg.payload))))

        consumer = asyncio.create_task(consume())
        try:
            await until(
                lambda: all(
                    (
                        (output / (name + ".log")).exists()
                        and marker in (output / (name + ".log")).read_text(encoding="utf-8")
                        for name, _, _, _, _, marker in roles
                    )
                ),
                "pipeline ready",
            )
            sequence = 0
            for protocol in profile["protocols"]:
                config = output / (protocol + "-map.yaml")
                config.write_text(
                    yaml.safe_dump(adapter_config(protocol, fixture_ports, profile)),
                    encoding="utf-8",
                )
                key = {
                    "opcua": "node_list_path",
                    "modbus": "register_map_path",
                    "s7": "unit_map_path",
                }[protocol]
                parameters = values | {
                    key: str(config),
                    "position_path": str(output / (protocol + "-positions.sqlite3")),
                    "source_id": "e2-" + protocol,
                    "reconnect_delay_s": 0.2,
                }
                if protocol == "modbus":
                    parameters["durable_inspections"] = False
                adapter = resources.role(
                    protocol,
                    protocol + "_data_subscriber",
                    "SubscriberRuntime",
                    "SubscriberSettings",
                    parameters,
                )
                canonical_rule = ProductionModeRule(settings(profile))
                baseline_rule = ProductionModeRule(settings(profile))
                try:
                    for case_index, case in enumerate(selected_cases(profile)):
                        print(f"native {protocol} case {case_index}", flush=True)
                        sequence += 1
                        raw_start = len(raw_messages)
                        packet_start = len(packets)
                        dlq_start = len(dlq_messages)
                        save(directory / "control.json", dict(sequence=sequence, signals=case))

                        def applied(sequence: int = sequence) -> bool:
                            try:
                                return bool(
                                    json.loads((directory / "applied.json").read_text())["sequence"]
                                    == sequence
                                )
                            except (OSError, ValueError):
                                return False

                        await until(applied, "fixture case application")
                        independent = await read_native(protocol, fixture_ports)
                        for signal, sample in case.items():
                            binding = profile["signals"][signal]
                            expected_native = copy.deepcopy(sample)
                            expected_native[protocol] = independent[signal]
                            baseline_error = None
                            try:
                                baseline = local_meaning(
                                    protocol,
                                    expected_native,
                                    profile["source_bindings"][protocol][signal],
                                    datetime.now(UTC),
                                )
                            except ValueError:
                                baseline = None
                                baseline_error = "ValueError"
                            found: dict[str, Any] = {}

                            def matched(
                                state: tuple[Any, ...] = (
                                    raw_start,
                                    binding,
                                    independent,
                                    signal,
                                    protocol,
                                    sample,
                                    dlq_start,
                                    packet_start,
                                    found,
                                ),
                            ) -> bool:
                                (
                                    raw_start,
                                    binding,
                                    independent,
                                    signal,
                                    protocol,
                                    sample,
                                    dlq_start,
                                    packet_start,
                                    found,
                                ) = state
                                for env in raw_messages[raw_start:]:
                                    raw = env["payload"]
                                    if raw["node_id"] != binding["node"]:
                                        continue
                                    cid = env["correlation_id"]
                                    native_expected = independent[signal]
                                    if protocol == "s7":
                                        import struct

                                        native_expected = struct.unpack(
                                            ">h" if signal == "count" else ">f",
                                            bytes.fromhex(native_expected),
                                        )[0]
                                    elif protocol == "modbus" and signal != "count":
                                        native_expected /= 100 if signal == "speed" else 10
                                    if not math.isclose(
                                        raw["value"], native_expected, rel_tol=0, abs_tol=1e-09
                                    ):
                                        continue
                                    if sample.get("expected_error"):
                                        reply = next(
                                            (
                                                m
                                                for m in dlq_messages[dlq_start:]
                                                if m["correlation_id"] == cid
                                            ),
                                            None,
                                        )
                                        if reply is not None:
                                            found.update(raw=env, dlq=reply)
                                            return True
                                    else:
                                        reply = next(
                                            (
                                                m
                                                for m in packets[packet_start:]
                                                if m["packet"].get("eirvah:correlationId") == cid
                                            ),
                                            None,
                                        )
                                        if reply is not None:
                                            found.update(raw=env, **reply)
                                            return True
                                return False

                            await until(matched, "correlated " + protocol + "/" + signal)
                            command = None
                            baseline_command = None
                            if signal == "speed" and baseline is not None:
                                observation = validate_observation(
                                    found["topic"],
                                    found["packet"],
                                    load_bindings(ROOT / "config/eirvah-line-a.jsonld"),
                                )
                                at = observation.result_time
                                now = at + timedelta(milliseconds=100)
                                command = canonical_rule.update(
                                    speed=float(observation.has_simple_result),
                                    observed_at=at,
                                    now=now,
                                )
                                baseline_command = baseline_rule.update(
                                    speed=baseline["value"], observed_at=at, now=now
                                )
                            row = dict(
                                protocol=protocol,
                                case=case_index,
                                signal=signal,
                                sample=sample,
                                independent_native=independent[signal],
                                baseline=baseline,
                                baseline_error=baseline_error,
                                command=command_meaning(command),
                                baseline_command=command_meaning(baseline_command),
                                **found,
                            )
                            data["rows"].append(row)
                            save(output / "evidence.json", data)
                finally:
                    resources.stop(adapter)
            if data.get("source_addition_requested"):
                from e2_source_addition import live_addition

                await live_addition(
                    resources,
                    data,
                    dict(
                        profile=profile,
                        nats_url=url,
                        fixture_ports=fixture_ports,
                        directory=directory,
                        raw=raw_messages,
                        mqtt=packets,
                        dlq=dlq_messages,
                        wait=until,
                        adapter_config=adapter_config,
                        read_native=read_native,
                        cases=selected_cases(profile),
                    ),
                )
        finally:
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)
            await broker.close()
    data["transport_records"] = dict(raw=raw_messages, mqtt=packets, dlq=dlq_messages)


def evaluate(data: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = dict(
        native_case="unknown",
        C04="unknown",
        checks=[],
        scope="Selected isolated native acquisition and broker transport; no application command delivery, native actuation, source-addition effort or distributed-placement claim.",
    )
    try:
        attributed = data["source_hashes"] == {
            p.relative_to(ROOT).as_posix(): digest(p) for p in SOURCES
        }
        scoped = (
            data.get("schema_version") == "1.0" and data.get("mode") == "isolated_native_loopback"
        )
        if not attributed or not scoped or (not data["cleanup"]["confirmed"]) or data["errors"]:
            return result
        rows = data["rows"]
        profile = load_profile(PROFILE)
        cases = selected_cases(profile)
        records = data["transport_records"]
        expected_containers = {"fixture", "nats", "mqtt"}
        if data.get("source_addition_requested"):
            expected_containers.add("fixture-added")
        if {c["name"] for c in data["containers"]} != expected_containers:
            return result
        if any(
            not c["image_id"]
            or not c["ports"]
            or any(
                binding["HostIp"] != "127.0.0.1"
                for entries in c["ports"].values()
                if entries
                for binding in entries
            )
            for c in data["containers"]
        ):
            return result
        expected = {
            (p, i, s) for p in profile["protocols"] for i in range(4) for s in profile["signals"]
        }
        keys = [(r["protocol"], r["case"], r["signal"]) for r in rows]
        checks = [
            dict(
                check="complete_matrix",
                status="pass" if len(keys) == len(expected) and set(keys) == expected else "fail",
            )
        ]
        for row in rows:
            good = True
            sample = cases[row["case"]][row["signal"]]
            binding = profile["signals"][row["signal"]]
            env = row["raw"]
            raw = env["payload"]
            cid = env["correlation_id"]
            native_ports = data["endpoints"]["native"]
            endpoint = {
                "opcua": f"opc.tcp://127.0.0.1:{native_ports['4840']}/e2/",
                "modbus": f"modbus-tcp://127.0.0.1:{native_ports['5020']}",
                "s7": f"s7://127.0.0.1:{native_ports['1102']}",
            }[row["protocol"]]
            expected_source = source_specific_value(
                row["protocol"], sample, profile["source_bindings"][row["protocol"]][row["signal"]]
            )
            good = (
                raw["source_endpoint"] == endpoint
                and raw["quality"] == "good"
                and type(raw["value"]) is type(expected_source)
                and math.isclose(raw["value"], expected_source, rel_tol=0, abs_tol=1e-9)
            )
            good = (
                good
                and env in records["raw"]
                and raw["node_id"] == binding["node"]
                and (raw["position"]["producer"] == "e2-" + row["protocol"] + "/" + binding["node"])
            )
            good = (
                good
                and row["sample"] == sample
                and (row["independent_native"] == sample[row["protocol"]])
            )
            if sample.get("expected_error"):
                good = (
                    good
                    and row["baseline_error"] == "ValueError"
                    and (row["dlq"] in records["dlq"])
                    and (row["dlq"]["payload"] == raw)
                    and (row["dlq"]["correlation_id"] == cid)
                    and ("stage=convert reason=ValueError" in row["dlq"]["error"]["message"])
                    and (
                        not any(
                            p["packet"].get("eirvah:correlationId") == cid for p in records["mqtt"]
                        )
                    )
                    and (row["command"] is None)
                    and (row["baseline_command"] is None)
                )
            else:
                good = (
                    good
                    and row["topic"] == binding["topic"]
                    and dict(topic=row["topic"], packet=row["packet"]) in records["mqtt"]
                )
                good = good and all(
                    row["baseline"][field] == binding[field]
                    for field in (
                        "node",
                        "topic",
                        "unit",
                        "value_type",
                        "sensor",
                        "feature",
                        "property",
                    )
                )
                good = good and datetime.fromisoformat(
                    row["packet"]["sosa:resultTime"]
                ) == datetime.fromisoformat(raw["source_timestamp"])
                observation = validate_observation(
                    row["topic"], row["packet"], load_bindings(ROOT / "config/eirvah-line-a.jsonld")
                )
                value = observation.has_simple_result
                good = (
                    good
                    and row["packet"]["eirvah:correlationId"] == cid
                    and (row["packet"]["eirvah:position"] == raw["position"])
                )
                good = (
                    good
                    and type(value) is type(sample["expected"])
                    and math.isclose(
                        float(value), float(sample["expected"]), rel_tol=0, abs_tol=1e-06
                    )
                )
                good = (
                    good
                    and row["baseline"]["unit"] == binding["unit"]
                    and (row["baseline"]["value_type"] == binding["value_type"])
                    and math.isclose(
                        float(row["baseline"]["value"]),
                        float(sample["expected"]),
                        rel_tol=0,
                        abs_tol=1e-06,
                    )
                )
                good = good and row["command"] == row["baseline_command"]
                expected_target = 22.0 if row["case"] == 0 else 26.0 if row["case"] == 1 else None
                if row["signal"] == "speed":
                    good = good and (
                        row["command"] is None
                        if expected_target is None
                        else row["command"]["requested_value"] == expected_target
                        and row["command"]["target_uns_topic"] == profile["command_target"]
                    )
                else:
                    good = good and row["command"] is None
            checks.append(
                dict(
                    check=f"{row['protocol']}/{row['case']}/{row['signal']}",
                    status="pass" if good else "fail",
                )
            )
        if data.get("source_addition_requested"):
            from e2_source_addition import evaluate_addition

            addition = evaluate_addition(data["source_addition"])
            checks.append(dict(check="gateway_addition", status=addition["source_addition"]))
        result["checks"] = checks
        statuses = {c["status"] for c in checks}
        result["native_case"] = (
            "fail" if "fail" in statuses else "unknown" if "unknown" in statuses else "pass"
        )
    except (KeyError, TypeError, ValueError, OSError):
        pass
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--source-addition", action="store_true")
    args = parser.parse_args()
    output = args.output or ROOT / "results/e2-native" / datetime.now(UTC).strftime(
        "%Y-%m-%dT%H-%M-%S-%fZ"
    )
    output.mkdir(parents=True, exist_ok=False)
    resources = OwnedResources(output)
    for source in SOURCES:
        destination = output / "sources" / source.relative_to(ROOT)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(source.read_bytes())
    data: dict[str, Any] = dict(
        schema_version="1.0",
        mode="isolated_native_loopback",
        source_addition_requested=args.source_addition,
        rows=[],
        errors=[],
        python_version=sys.version,
        dependencies={
            name: importlib.metadata.version(name)
            for name in ("asyncua", "pymodbus", "python-snap7", "nats-py", "aiomqtt", "pydantic")
        },
        cleanup=dict(confirmed=False),
        source_hashes={p.relative_to(ROOT).as_posix(): digest(p) for p in SOURCES},
        git_sha=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
    )
    try:
        with (
            asyncio.Runner(loop_factory=asyncio.SelectorEventLoop) as runner,
            phase("native-acquisition-and-gateway"),
        ):
            runner.run(capture(resources, data))
    except (Exception, KeyboardInterrupt) as exc:
        data["errors"].append(dict(type=type(exc).__name__, message=str(exc)[-1500:]))
    finally:
        data["containers"] = resources.containers
        data["cleanup"] = resources.cleanup()
        data["processes"] = [
            dict(name=name, pid=process.pid, returncode=process.returncode)
            for process, _, name in resources.processes
        ]
        save(output / "evidence.json", data)
    report = evaluate(data)
    save(output / "report.json", report)
    (output / "report.md").write_text(
        "# Native E2 rehearsal\n\nNative case: **"
        + report["native_case"]
        + "**. C04/study: **unknown**.\n\n"
        + report["scope"]
        + "\n\n| Check | Verdict |\n| --- | --- |\n"
        + "\n".join("| " + c["check"] + " | " + c["status"] + " |" for c in report["checks"])
        + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            dict(
                native_case=report["native_case"],
                report=str(output / "report.md"),
                errors=data["errors"],
                cleanup=data["cleanup"],
            ),
            indent=2,
        )
    )
    raise SystemExit(0 if report["native_case"] == "pass" else 1)


if __name__ == "__main__":
    main()
