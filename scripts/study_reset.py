"""Known test-fixture reset through the normal protected command path; never deletes history."""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
from contextlib import ExitStack
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from asyncua import Client
from control import send
from e0_capture import Cluster, NativeTargetObservations
from eirvah_contracts.inspection import decode_inspection
from eirvah_observability.experiment import phase
from pymodbus.client import AsyncModbusTcpClient
from scenario import Scenario, port_forward, run_scenario


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--targets", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--park",
        action="store_true",
        help="Stop study conveyor between measurement windows; retain history.",
    )
    parser.add_argument("--require-drained-inspections", action="store_true")
    args = parser.parse_args()
    targets = json.loads(args.targets.read_text())
    cluster = Cluster(**targets["default"], components=targets.get("components", {}))
    controller = cluster.target("decision-agent-stub")
    previous = json.loads(controller.run("get", "deployment", "decision-agent-stub", "-o", "json"))[
        "spec"
    ]["replicas"]
    if previous != 1:
        raise ValueError("reset requires one known decision authority")
    args.output.mkdir(parents=True, exist_ok=False)
    data: dict[str, Any] = dict(reset="unknown", restored=False, counters_and_ledgers="preserved")
    try:
        with ExitStack() as stack:
            amqp_port = stack.enter_context(port_forward(*cluster.transport("rabbitmq", 5672)))
            mqtt = stack.enter_context(port_forward(*cluster.transport("mosquitto", 1883)))
            for name, port in [
                ("opcua-simulator", 4840),
                ("modbus-simulator", 5020),
                ("s7-simulator", 102),
            ]:
                cluster.native_ports[name] = stack.enter_context(
                    port_forward(*cluster.transport(name, port))
                )
            data["native_before"] = {
                t: cluster.native(t) for t in ("temperature", "conveyor", "capper")
            }
            encoded = cluster.target("rabbitmq").run(
                "get", "secret", "actuation-security", "-o", "jsonpath={.data.experiment-amqp-url}"
            )
            parts = urlsplit(base64.b64decode(encoded).decode())
            amqp = urlunsplit(
                (
                    parts.scheme,
                    parts.netloc.rsplit("@", 1)[0] + f"@127.0.0.1:{amqp_port}",
                    parts.path,
                    "",
                    "",
                )
            )
            steps: list[dict[str, Any]] = []
            for target in ("conveyor", "capper"):
                steps.append(
                    dict(
                        name=target + "-fault-reset", target=target, action="reset", if_faulted=True
                    )
                )
                steps += [
                    dict(name=target + "-stop", target=target, action="stop"),
                    dict(
                        name=target + "-speed",
                        target=target,
                        action="speed",
                        value=0.4 if target == "conveyor" else 1200,
                    ),
                    dict(name=target + "-start", target=target, action="start"),
                ]
            steps += [
                dict(name="filler-open", target="filler", action="open"),
                dict(name="warmer-22", target="temperature", action="set", value=22.0),
            ]
            steps = [step for step in steps if step["name"] != "conveyor-start"]
            data["mode"] = "park" if args.park else "reset"

            async def execute() -> None:
                async def sender(command: Any) -> dict[str, Any]:
                    return await send(command, amqp)

                async def scenario(name: str, selected: list[dict[str, Any]]) -> None:
                    plan = Scenario.model_validate(
                        dict(
                            schema_version="1.0",
                            name="study-" + name,
                            description="Fixture preparation; history retained.",
                            steps=selected,
                        )
                    )
                    result = await run_scenario(
                        plan,
                        args.output / name,
                        sender=sender,
                        observations=NativeTargetObservations(
                            cluster, mqtt, "eirvah", "eirvah-dev-password"
                        ),
                        repetitions=1,
                        metadata=dict(scope="reset_not_measurement"),
                    )
                    if result["outcome"] != "passed":
                        raise RuntimeError("fixture step failed: " + name)

                with phase("fixture-park" if args.park else "fixture-reset"):
                    # Halt inspection production before pausing its decision consumer.
                    await scenario(
                        "park", [dict(name="conveyor-park", target="conveyor", action="stop")]
                    )
                    if args.require_drained_inspections:
                        drain = []
                        async with AsyncModbusTcpClient(
                            "127.0.0.1", port=cluster.native_ports["modbus-simulator"]
                        ) as source:
                            empty = 0
                            for _ in range(120):
                                head = await source.read_holding_registers(60, count=10, slave=1)
                                if head.isError():
                                    raise RuntimeError("inspection source head unavailable")
                                event = decode_inspection(head.registers)
                                drain.append(
                                    dict(inspection_id=event.inspection_id if event else None)
                                )
                                empty = empty + 1 if event is None else 0
                                if empty >= 2:
                                    break
                                await asyncio.sleep(1)
                        data["inspection_drain"] = drain
                        if empty < 2:
                            raise RuntimeError(
                                "inspection deliveries did not drain; history retained"
                            )
                    if args.park:
                        return
                    await asyncio.to_thread(
                        controller.run, "scale", "deployment/decision-agent-stub", "--replicas=0"
                    )
                    await asyncio.to_thread(
                        controller.run,
                        "wait",
                        "--for=delete",
                        "pod",
                        "-l",
                        "app.kubernetes.io/name=decision-agent-stub",
                        "--timeout=90s",
                        timeout=100,
                    )
                    await scenario("commands", steps)
                    async with Client(
                        f"opc.tcp://127.0.0.1:{cluster.native_ports['opcua-simulator']}/eirvah/simulator"
                    ) as native:
                        ns = await native.get_namespace_index(
                            "https://eirvah.uniza/zilina/factory1"
                        )
                        node = await native.nodes.objects.get_child(
                            [f"{ns}:bottler", f"{ns}:Temperature"]
                        )
                        samples = []
                        for _ in range(120):
                            samples.append(float(await node.read_value()))
                            if len(samples) >= 3 and all(abs(v - 22) <= 0.75 for v in samples[-3:]):
                                break
                            await asyncio.sleep(0.5)
                        data["temperature_samples"] = samples
                        if len(samples) < 3 or not all(abs(v - 22) <= 0.75 for v in samples[-3:]):
                            raise RuntimeError("baseline process did not settle")
                    await asyncio.to_thread(
                        controller.run, "scale", "deployment/decision-agent-stub", "--replicas=1"
                    )
                    await asyncio.to_thread(
                        controller.run,
                        "rollout",
                        "status",
                        "deployment/decision-agent-stub",
                        "--timeout=180s",
                        timeout=190,
                    )
                    # Start new bottle production only after its consumer is running.
                    await scenario(
                        "start", [dict(name="conveyor-start", target="conveyor", action="start")]
                    )

            with asyncio.Runner(loop_factory=asyncio.SelectorEventLoop) as runner:
                runner.run(execute())
            data["native_after"] = {
                t: cluster.native(t) for t in ("temperature", "conveyor", "capper")
            }
            data["reset"] = "pass"
    except (Exception, KeyboardInterrupt) as exc:
        data["error"] = type(exc).__name__
    finally:
        controller.run("scale", "deployment/decision-agent-stub", "--replicas=" + str(previous))
        controller.run(
            "rollout", "status", "deployment/decision-agent-stub", "--timeout=180s", timeout=190
        )
        data["restored"] = True
        (args.output / "report.json").write_text(
            json.dumps(data, indent=2) + "\n", encoding="utf-8"
        )
    raise SystemExit(0 if data["reset"] == "pass" and data["restored"] else 1)


if __name__ == "__main__":
    main()
