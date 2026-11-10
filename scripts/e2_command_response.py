"""Replay-linked E2 command/device checks against the explicitly selected research cluster."""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import math
import time
from contextlib import ExitStack
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from asyncua import Client
from control import build_command, send
from e0_capture import Cluster
from e2_native import SOURCES as NATIVE_SOURCES
from e2_native import evaluate as evaluate_native
from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.authorization import command_digest
from eirvah_observability.experiment import phase
from interoperability import PROFILE, ROOT, digest, load_profile
from scenario import port_forward

SOURCES = [
    *NATIVE_SOURCES,
    Path(__file__),
    ROOT / "scripts/control.py",
    ROOT / "scripts/e0_capture.py",
    ROOT / "scripts/scenario.py",
]


ACTORS = [
    "amqp-actuation-event-subscriber",
    "actuation-control-orchestrator",
    "actuation-event-validator",
    "actuation-signal-publisher",
    "opcua-simulator",
]


def select_commands(native: dict[str, Any]) -> list[dict[str, Any]]:
    profile = load_profile(PROFILE)
    if native.get("mode") != "isolated_native_loopback":
        raise ValueError("native evidence missing")
    rows = [r for r in native["rows"] if r["signal"] == "speed" and r["case"] in (0, 1)]
    if len(rows) != 6 or {(r["protocol"], r["case"]) for r in rows} != {
        (p, i) for p in profile["protocols"] for i in (0, 1)
    }:
        raise ValueError("incomplete paired inputs")
    for row in rows:
        command = row["command"]
        if (
            command != row["baseline_command"]
            or command["target_uns_topic"] != profile["command_target"]
            or command["requested_value"] != (22.0 if row["case"] == 0 else 26.0)
            or command["value_type"] != "double"
        ):
            raise ValueError("input variants disagree")
    return rows


def save(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, allow_nan=False) + "\n", encoding="utf-8")


async def exercise(
    cluster: Cluster,
    amqp: str,
    opcua_port: int,
    rows: list[dict[str, Any]],
    data: dict[str, Any],
    output: Path,
) -> None:
    async with Client(f"opc.tcp://127.0.0.1:{opcua_port}/eirvah/simulator") as native:
        idx = await native.get_namespace_index("https://eirvah.uniza/zilina/factory1")
        setpoint = await native.nodes.objects.get_child(
            [f"{idx}:bottler", f"{idx}:SetpointTemperature"]
        )
        temperature = await native.nodes.objects.get_child([f"{idx}:bottler", f"{idx}:Temperature"])
        original = float(await setpoint.read_value())
        data["original_setpoint"] = original
        save(output / "evidence.json", data)
        try:
            for source in rows:
                value = float(source["command"]["requested_value"])
                command = build_command("temperature", "set", value)
                command = command.model_copy(
                    update=dict(reason="E2 paired replay of " + source["raw"]["correlation_id"])
                )
                entry = dict(
                    protocol=source["protocol"],
                    case=source["case"],
                    source_observation=source["raw"]["correlation_id"],
                    derived_command=source["command"],
                    command=command.model_dump(mode="json"),
                    process_samples=[],
                    initial_temperature=float(await temperature.read_value()),
                )
                data["rows"].append(entry)
                save(output / "evidence.json", data)
                entry["result"] = await send(command, amqp)
                entry["native_readback"] = float(await setpoint.read_value())
                entry["sampling_started"] = time.monotonic()
                end = entry["sampling_started"] + 45
                stable = 0
                while time.monotonic() < end:
                    current = await temperature.read_data_value()
                    measured = float(current.Value.Value)
                    entry["process_samples"].append(
                        dict(
                            value=measured,
                            observed_at=datetime.now(UTC).isoformat(),
                            source_timestamp=current.SourceTimestamp.isoformat()
                            if current.SourceTimestamp
                            else None,
                            monotonic=time.monotonic(),
                        )
                    )
                    stable = (
                        stable + 1
                        if math.isfinite(measured) and abs(measured - value) <= 0.75
                        else 0
                    )
                    if stable >= 3:
                        break
                    await asyncio.sleep(0.5)
                entry["settled"] = stable >= 3
                entry["ledger"] = await asyncio.to_thread(cluster.ledger, [command.correlation_id])
                save(output / "evidence.json", data)
                print(
                    f"paired {source['protocol']}/{source['case']} outcome={entry['result'].get('decision')} settled={entry['settled']}",
                    flush=True,
                )
                if not entry["settled"]:
                    raise RuntimeError("simulated process did not reach selected response budget")
            invalid = build_command("temperature", "set", 99.0)
            before = float(await setpoint.read_value())
            response = await send(invalid, amqp)
            data["invalid_command"] = dict(
                command=invalid.model_dump(mode="json"),
                result=response,
                before=before,
                after=float(await setpoint.read_value()),
                ledger=await asyncio.to_thread(cluster.ledger, [invalid.correlation_id]),
            )
        finally:
            # Compensation is a new, authenticated operator command; never replay ambiguous work.
            restore = build_command("temperature", "set", original)
            result = await send(restore, amqp)
            actual = float(await setpoint.read_value())
            data["setpoint_restoration"] = dict(
                command=restore.model_dump(mode="json"),
                result=result,
                readback=actual,
                ledger=await asyncio.to_thread(cluster.ledger, [restore.correlation_id]),
                confirmed=result.get("decision") == "approve"
                and result.get("written_at") is not None
                and math.isclose(actual, original, rel_tol=0, abs_tol=1e-6),
            )
            save(output / "evidence.json", data)


def response_matches(command: dict[str, Any], response: dict[str, Any]) -> bool:
    # Results echo the immutable request; timestamps may have equivalent UTC spellings.
    echoed = {k: response[k] for k in command}
    return command_digest(ActuationRequest.model_validate(command)) == command_digest(
        ActuationRequest.model_validate(echoed)
    )


def completed(entry: dict[str, Any]) -> bool:
    command, response, ledger = entry["command"], entry["result"], entry["ledger"]
    return (
        response_matches(command, response)
        and response.get("written_at") is not None
        and response["decision"] == "approve"
        and len(ledger) == 1
        and ledger[0]["command_id"] == command["correlation_id"]
        and ledger[0]["digest"] == command_digest(ActuationRequest.model_validate(command))
        and ledger[0]["state"] == "completed"
        and json.loads(ledger[0]["result"])["outcome"] == "completed"
        and json.loads(ledger[0]["result"])["written_at"] == response["written_at"]
    )


def evaluate(data: dict[str, Any]) -> dict[str, Any]:
    report: dict[str, Any] = dict(
        command_case="unknown",
        mode="paired_replay_to_simulated_process",
        checks=[],
        automatic_closed_loop="not_exercised",
    )
    try:
        if data["mode"] != report["mode"] or data["errors"] or not data["controller_restored"]:
            return report
        if data["source_hashes"] != {p.relative_to(ROOT).as_posix(): digest(p) for p in SOURCES}:
            return report
        native_path = Path(data["native_input"])
        if digest(native_path) != data["native_sha256"]:
            return report
        native = json.loads(native_path.read_text(encoding="utf-8"))
        if evaluate_native(native)["native_case"] != "pass":
            return report
        sources = {(r["protocol"], r["case"]): r for r in select_commands(native)}
        rows = data["rows"]
        matrix = len(rows) == 6 and {(r["protocol"], r["case"]) for r in rows} == set(sources)
        inventory = data["inventory_before"]
        stable = inventory == data["inventory_after"] and {
            p["component"] for p in inventory["pods"].values()
        } == set(ACTORS)
        checks = [
            dict(check="complete_command_matrix", status="pass" if matrix else "fail"),
            dict(check="stable_deployed_path", status="pass" if stable else "fail"),
        ]
        ids = [r["command"]["correlation_id"] for r in rows]
        ids += [
            data[k]["command"]["correlation_id"]
            for k in ("invalid_command", "setpoint_restoration")
        ]
        checks.append(
            dict(
                check="distinct_command_ids", status="pass" if len(ids) == len(set(ids)) else "fail"
            )
        )
        for row in rows:
            source = sources[(row["protocol"], row["case"])]
            derived, command = source["command"], row["command"]
            value = float(derived["requested_value"])
            samples = row["process_samples"]
            good = (
                row["source_observation"] == source["raw"]["correlation_id"]
                and row["derived_command"] == derived
                and command["requester"] == "experiment-driver"
                and all(
                    command[k] == derived[k]
                    for k in ("target_uns_topic", "requested_value", "value_type")
                )
                and command["reason"] == "E2 paired replay of " + row["source_observation"]
                and completed(row)
                and math.isclose(row["native_readback"], value, rel_tol=0, abs_tol=1e-6)
                and math.isfinite(row["initial_temperature"])
                and row["settled"]
                and len(samples) >= 3
                and all(math.isfinite(s["value"]) for s in samples)
                and all(abs(s["value"] - value) <= 0.75 for s in samples[-3:])
                and all(
                    samples[i]["monotonic"] < samples[i + 1]["monotonic"]
                    for i in range(len(samples) - 1)
                )
                and samples[0]["monotonic"] - row["sampling_started"] >= 0
                and samples[-1]["monotonic"] - row["sampling_started"] <= 45
            )
            checks.append(
                dict(
                    check=row["protocol"] + "/" + str(row["case"]),
                    status="pass" if good else "fail",
                )
            )
        invalid = data["invalid_command"]
        good = (
            response_matches(invalid["command"], invalid["result"])
            and invalid["command"]["requested_value"] == 99.0
            and invalid["result"]["decision"] == "reject"
            and invalid["before"] == invalid["after"] == rows[-1]["native_readback"]
            and not invalid["ledger"]
        )
        checks.append(
            dict(check="invalid_request_no_setpoint_change", status="pass" if good else "fail")
        )
        restore = data["setpoint_restoration"]
        good = (
            completed(restore)
            and restore["command"]["requested_value"] == data["original_setpoint"]
            and math.isclose(
                restore["readback"], data["original_setpoint"], rel_tol=0, abs_tol=1e-6
            )
        )
        checks.append(dict(check="original_setpoint_restored", status="pass" if good else "fail"))
        report.update(
            checks=checks,
            command_case="pass" if all(c["status"] == "pass" for c in checks) else "fail",
        )
    except (KeyError, TypeError, ValueError, OSError, IndexError):
        pass
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-run", type=Path, required=True)
    parser.add_argument("--context", required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.context != "kind-eirvah-edge":
        parser.error("this initial paired profile is restricted to the named research kind cluster")
    native_path = args.native_run / "evidence.json"
    native_data = json.loads(native_path.read_text(encoding="utf-8"))
    if evaluate_native(native_data)["native_case"] != "pass":
        parser.error("native evidence does not independently pass at this source revision")
    rows = select_commands(native_data)
    output = args.output or ROOT / "results/e2-command-response" / datetime.now(UTC).strftime(
        "%Y-%m-%dT%H-%M-%S-%fZ"
    )
    output.mkdir(parents=True, exist_ok=False)
    cluster = Cluster(args.context, "eirvah-edge")
    deployment = json.loads(cluster.run("get", "deployment", "decision-agent-stub", "-o", "json"))
    count = deployment["spec"].get("replicas", 1)
    if count != 1:
        parser.error("one active research consumer is required before this case")
    data: dict[str, Any] = dict(
        mode="paired_replay_to_simulated_process",
        context=args.context,
        rows=[],
        errors=[],
        controller_restored=False,
        original_replicas=count,
        native_input=str((output / "native-input.json").resolve()),
        native_sha256=hashlib.sha256(native_path.read_bytes()).hexdigest(),
        source_hashes={p.relative_to(ROOT).as_posix(): digest(p) for p in SOURCES},
    )
    (output / "native-input.json").write_bytes(native_path.read_bytes())
    for source in SOURCES:
        destination = output / "sources" / source.relative_to(ROOT)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(source.read_bytes())
    save(output / "evidence.json", data)
    try:
        cluster.run("scale", "deployment/decision-agent-stub", "--replicas=0")
        cluster.run(
            "wait",
            "--for=delete",
            "pod",
            "-l",
            "app.kubernetes.io/name=decision-agent-stub",
            "--timeout=90s",
            timeout=100,
        )
        data["inventory_before"] = cluster.inventory(ACTORS)
        encoded = cluster.run(
            "get", "secret", "actuation-security", "-o", "jsonpath={.data.experiment-amqp-url}"
        )
        parts = urlsplit(base64.b64decode(encoded).decode())
        with ExitStack() as stack:
            amqp_port = stack.enter_context(port_forward(*cluster.transport("rabbitmq", 5672)))
            opcua_port = stack.enter_context(
                port_forward(*cluster.transport("opcua-simulator", 4840))
            )
            auth = parts.netloc.rsplit("@", 1)[0]
            url = urlunsplit((parts.scheme, auth + f"@127.0.0.1:{amqp_port}", parts.path, "", ""))
            with (
                asyncio.Runner(loop_factory=asyncio.SelectorEventLoop) as runner,
                phase("paired-command-response"),
            ):
                runner.run(exercise(cluster, url, opcua_port, rows, data, output))
        data["inventory_after"] = cluster.inventory(ACTORS)
    except (Exception, KeyboardInterrupt) as exc:
        data["errors"].append(type(exc).__name__)
    finally:
        try:
            cluster.run("scale", "deployment/decision-agent-stub", "--replicas=" + str(count))
            cluster.run(
                "rollout", "status", "deployment/decision-agent-stub", "--timeout=120s", timeout=130
            )
            data["controller_restored"] = True
        except Exception:
            data["errors"].append("controller_restore_not_confirmed")
        save(output / "evidence.json", data)
    report = evaluate(data)
    save(output / "report.json", report)
    (output / "report.md").write_text(
        "# Paired E2 command/process response\n\nVerdict: **"
        + report["command_case"]
        + "**. Automatic closed loop: not exercised.\n\n| Check | Verdict |\n| --- | --- |\n"
        + "\n".join("| " + c["check"] + " | " + c["status"] + " |" for c in report["checks"])
        + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            dict(
                command_case=report["command_case"],
                report=str(output / "report.md"),
                errors=data["errors"],
                controller_restored=data["controller_restored"],
            ),
            indent=2,
        )
    )
    raise SystemExit(0 if report["command_case"] == "pass" else 1)


if __name__ == "__main__":
    main()
