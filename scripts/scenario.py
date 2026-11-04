"""Run versioned functional control scenarios through the authenticated AMQP path."""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import math
import os
import socket
import subprocess
import time
from collections.abc import Awaitable, Callable, Iterator
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol, Self
from urllib.parse import urlsplit, urlunsplit

import aiomqtt
from control import PREFIX, build_command, send
from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.inspection import BottleInspection
from pydantic import BaseModel, ConfigDict, Field, model_validator

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PLAN = ROOT / "config/scenarios/controls.json"


class Step(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    name: str = Field(min_length=1)
    target: str | None = None
    action: str | None = None
    value: int | float | bool | str | None = None
    value_source: Literal["passed_bottle"] | None = None
    replay_of: str | None = None
    if_faulted: bool = False
    expect: Literal["approve", "reject"] = "approve"
    reason_contains: str | None = None

    @model_validator(mode="after")
    def valid_operation(self) -> Self:
        if self.replay_of:
            if (
                self.target
                or self.action
                or self.value is not None
                or self.value_source
                or self.if_faulted
                or self.expect != "approve"
            ):
                raise ValueError("replay must contain only its name and an earlier successful step")
        else:
            if self.target is None or self.action is None:
                raise ValueError("new commands require target and action")
            if self.value_source:
                if (self.target, self.action) != ("reject", "reject") or self.value is not None:
                    raise ValueError(
                        "passed_bottle is only valid for a reject command without a literal value"
                    )
            else:
                if (
                    self.action in ("start", "stop", "reset", "open", "close")
                    and self.value is not None
                ):
                    raise ValueError("this action has a fixed value; omit value")
                build_command(self.target, self.action, self.value)
            if self.if_faulted and (
                self.target not in ("conveyor", "capper") or self.action != "reset"
            ):
                raise ValueError("if_faulted is only valid for motor reset")
        if self.reason_contains and self.expect != "reject":
            raise ValueError("reason_contains requires an expected rejection")
        return self


class Scenario(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    schema_version: Literal["1.0"] = "1.0"
    name: str = Field(min_length=1)
    description: str = ""
    steps: list[Step] = Field(min_length=1)

    @model_validator(mode="after")
    def valid_sequence(self) -> Self:
        earlier: dict[str, Step] = {}
        for step in self.steps:
            if step.name in earlier:
                raise ValueError("step names must be unique")
            if step.replay_of:
                source = earlier.get(step.replay_of)
                if source is None or source.expect != "approve" or source.if_faulted:
                    raise ValueError("replay requires an earlier unconditional successful command")
            earlier[step.name] = step
        return self


def fresh_value(payload: dict[str, Any], now: datetime, unit: str) -> tuple[Any, datetime] | None:
    try:
        at = datetime.fromisoformat(payload["sosa:resultTime"])
        if payload.get("eirvah:quality") != "good" or payload.get("eirvah:unit") != unit:
            return None
        if at.tzinfo is None or not 0 <= (now - at).total_seconds() <= 2:
            return None
        return payload["sosa:hasSimpleResult"], at
    except (KeyError, TypeError, ValueError):
        return None


def passed_inspection(
    payload: dict[str, Any], now: datetime, used: set[str]
) -> dict[str, Any] | None:
    sample = fresh_value(payload, now, "json")
    if sample is None or not isinstance(sample[0], str):
        return None
    try:
        event = BottleInspection.model_validate_json(sample[0])
    except ValueError:
        return None
    if not event.passed or event.inspected_at != sample[1] or event.inspection_id in used:
        return None
    return {
        "value": event.bottle_id,
        "inspection_id": event.inspection_id,
        "observed_at": event.inspected_at.isoformat(),
    }


class Observations(Protocol):
    async def faulted(self, target: str) -> dict[str, Any]: ...
    async def passed_bottle(self) -> dict[str, Any]: ...


class MqttObservations:
    def __init__(self, port: int, username: str, password: str) -> None:
        self.port, self.username, self.password = port, username, password
        self.used: set[str] = set()

    async def wait(
        self, topic: str, select: Callable[[dict[str, Any]], dict[str, Any] | None]
    ) -> dict[str, Any]:
        async with asyncio.timeout(15):
            async with aiomqtt.Client(
                hostname="127.0.0.1", port=self.port, username=self.username, password=self.password
            ) as mqtt:
                await mqtt.subscribe(topic, qos=1)
                async for message in mqtt.messages:
                    try:
                        payload = json.loads(message.payload)
                    except (TypeError, ValueError):
                        continue
                    if isinstance(payload, dict) and str(message.topic) == topic:
                        result = select(payload)
                        if result is not None:
                            return result
        raise TimeoutError("no suitable observation")

    async def faulted(self, target: str) -> dict[str, Any]:
        topic = PREFIX + (
            "conveyor/belt_01/jam_detected" if target == "conveyor" else "capper/motor_01/faulted"
        )

        def select(payload: dict[str, Any]) -> dict[str, Any] | None:
            sample = fresh_value(payload, datetime.now(UTC), "dimensionless")
            if sample is None or type(sample[0]) is not int or sample[0] not in (0, 1):
                return None
            return {"value": bool(sample[0]), "observed_at": sample[1].isoformat(), "topic": topic}

        return await self.wait(topic, select)

    async def passed_bottle(self) -> dict[str, Any]:
        result = await self.wait(
            PREFIX + "inspector/inspector_01/bottle_inspection",
            lambda payload: passed_inspection(payload, datetime.now(UTC), self.used),
        )
        self.used.add(result["inspection_id"])
        return result


def assess(
    step: Step, command: ActuationRequest, result: dict[str, Any], original: dict[str, Any] | None
) -> tuple[str, str]:
    if result.get("decision") not in ("approve", "reject", "unknown", "pending"):
        return "unknown", "malformed_command_result"
    if result.get("decision") == "reject" and not isinstance(result.get("rejection_reason"), str):
        return "unknown", "malformed_command_result"
    if result.get("decision") in ("unknown", "pending"):
        return "unknown", "unsettled_writer_outcome"
    try:
        echoed = ActuationRequest.model_validate(result)
        for field in ActuationRequest.model_fields:
            if getattr(echoed, field) != getattr(command, field):
                return "unknown", "mismatched_command_result"
        if type(echoed.requested_value) is not type(command.requested_value):
            return "unknown", "mismatched_command_result"
        if result.get("decision") == "approve":
            at = datetime.fromisoformat(result["written_at"])
            if at.tzinfo is None:
                return "unknown", "missing_completion_time"
    except (ValueError, TypeError, KeyError):
        return "unknown", "malformed_command_result"
    if result.get("decision") != step.expect:
        return "failed", "unexpected_decision"
    if step.reason_contains and step.reason_contains not in str(result.get("rejection_reason", "")):
        return "failed", "unexpected_rejection_reason"
    if original is not None and (
        result.get("replayed") is not True or result.get("written_at") != original.get("written_at")
    ):
        return "failed", "replay_evidence_mismatch"
    return "passed", "expected_result"


async def run_scenario(
    plan: Scenario,
    output: Path,
    *,
    sender: Callable[[ActuationRequest], Awaitable[dict[str, Any]]],
    observations: Observations,
    repetitions: int = 1,
    interval_s: float = 0.25,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if repetitions < 1 or not math.isfinite(interval_s) or interval_s < 0:
        raise ValueError("positive repetitions and a finite nonnegative interval are required")
    output.mkdir(parents=True, exist_ok=False)
    frozen = plan.model_dump_json(indent=2) + "\n"
    (output / "scenario.json").write_bytes(frozen.encode("utf-8"))
    report: dict[str, Any] = {
        "scenario": plan.name,
        "scenario_sha256": hashlib.sha256(frozen.encode()).hexdigest(),
        "started_at": datetime.now(UTC).isoformat(),
        "repetitions": repetitions,
        "interval_s": interval_s,
        "outcome": "running",
        "steps": [],
        "metadata": metadata or {},
        "formal_conformance": "not_assessed",
        "state_policy": "explicit scenario final state; no automatic rollback or retry",
    }

    def save() -> None:
        (output / "summary.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    def journal(event: str, **fields: Any) -> None:
        with (output / "journal.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(
                json.dumps({"event": event, "at": datetime.now(UTC).isoformat(), **fields}) + "\n"
            )
            stream.flush()
            os.fsync(stream.fileno())

    save()
    try:
        for iteration in range(1, repetitions + 1):
            commands: dict[str, ActuationRequest] = {}
            results: dict[str, dict[str, Any]] = {}
            for step in plan.steps:
                row: dict[str, Any] = {
                    "iteration": iteration,
                    "name": step.name,
                    "expect": step.expect,
                    "outcome": "running",
                }
                report["steps"].append(row)
                try:
                    if step.if_faulted:
                        row["observation"] = await observations.faulted(str(step.target))
                        if not row["observation"]["value"]:
                            row["outcome"] = "skipped"
                            row["reason"] = "motor_not_faulted"
                            journal("step_finished", **row)
                            save()
                            continue
                    if step.replay_of:
                        command = commands[step.replay_of]
                    else:
                        value = step.value
                        if step.value_source:
                            row["observation"] = await observations.passed_bottle()
                            value = row["observation"]["value"]
                        command = build_command(str(step.target), str(step.action), value)
                    commands[step.name] = command
                    row["correlation_id"] = command.correlation_id
                    journal(
                        "command_submitting",
                        iteration=iteration,
                        name=step.name,
                        command=command.model_dump(mode="json"),
                        observation=row.get("observation"),
                    )
                    async with asyncio.timeout(20):
                        result = await sender(command)
                    results[step.name] = result
                    row["result"] = result
                    row["outcome"], row["reason"] = assess(
                        step, command, result, results[step.replay_of] if step.replay_of else None
                    )
                except asyncio.CancelledError:
                    row["outcome"] = "unknown"
                    row["reason"] = "interrupted"
                    journal("step_finished", **row)
                    raise
                except Exception as exc:
                    row["outcome"] = "unknown"
                    row["reason"] = "transport_or_observation_unavailable"
                    row["error_type"] = type(exc).__name__
                journal("step_finished", **row)
                save()
                print(
                    f"iteration={iteration} step={step.name} outcome={row['outcome']}", flush=True
                )
                if row["outcome"] != "passed":
                    report["outcome"] = row["outcome"]
                    return report
                await asyncio.sleep(interval_s)
        report["outcome"] = "passed"
        return report
    except asyncio.CancelledError:
        report["outcome"] = "interrupted"
        raise
    except Exception as exc:
        report["outcome"] = "unknown"
        report["error_type"] = type(exc).__name__
        raise
    finally:
        report["finished_at"] = datetime.now(UTC).isoformat()
        save()


@contextmanager
def port_forward(prefix: list[str], service: str, remote: int) -> Iterator[int]:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    process = subprocess.Popen(
        [*prefix, "port-forward", "svc/" + service, f"{port}:{remote}", "--address=127.0.0.1"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    try:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and process.poll() is None:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    break
            except OSError:
                time.sleep(0.1)
        else:
            raise TimeoutError("port forwarding unavailable")
        yield port
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--context")
    parser.add_argument("--namespace", default="eirvah-edge")
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--interval", type=float, default=0.25)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--plan", action="store_true", help="Validate and display without accessing the cluster"
    )
    parser.add_argument("--collect-traces", action="store_true")
    args = parser.parse_args()
    try:
        plan = Scenario.model_validate_json(args.scenario.read_text(encoding="utf-8"))
        if args.repetitions < 1 or not math.isfinite(args.interval) or args.interval < 0:
            raise ValueError("positive repetitions and nonnegative finite interval required")
    except (ValueError, OSError) as exc:
        parser.error(str(exc))
    if args.plan:
        print(plan.model_dump_json(indent=2))
        return
    if not args.context:
        parser.error("--context is required for a live run")
    output = args.output or ROOT / "results/scenarios" / datetime.now(UTC).strftime(
        "%Y-%m-%dT%H-%M-%S-%fZ"
    )
    if output.exists():
        parser.error("output must be a new directory")
    prefix = ["kubectl", "--context", args.context, "-n", args.namespace]
    print(
        f"Scenario {plan.name}: final settings remain selected; no automatic rollback. Output: {output}",
        flush=True,
    )
    try:
        with ExitStack() as stack:
            rabbit = stack.enter_context(port_forward(prefix, "rabbitmq", 5672))
            mqtt = stack.enter_context(port_forward(prefix, "mosquitto", 1883))
            encoded = subprocess.check_output(
                [
                    *prefix,
                    "get",
                    "secret",
                    "actuation-security",
                    "-o",
                    "jsonpath={.data.experiment-amqp-url}",
                ],
                text=True,
                stderr=subprocess.DEVNULL,
                timeout=30,
            )
            parts = urlsplit(base64.b64decode(encoded).decode())
            url = urlunsplit(
                (
                    parts.scheme,
                    parts.netloc.split("@")[0] + f"@127.0.0.1:{rabbit}",
                    parts.path,
                    "",
                    "",
                )
            )
            revision = subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, timeout=10
            ).strip()
            dirty = bool(
                subprocess.check_output(
                    ["git", "status", "--porcelain"], cwd=ROOT, text=True, timeout=10
                ).strip()
            )

            async def sender(command: ActuationRequest) -> dict[str, Any]:
                return await send(command, url)

            # aiomqtt requires add_reader/add_writer, including on Windows.
            with asyncio.Runner(loop_factory=asyncio.SelectorEventLoop) as runtime:
                report = runtime.run(
                    run_scenario(
                        plan,
                        output,
                        sender=sender,
                        observations=MqttObservations(
                            mqtt,
                            os.environ.get("EIRVAH_MQTT_USERNAME", "eirvah"),
                            os.environ.get("EIRVAH_MQTT_PASSWORD", "eirvah-dev-password"),
                        ),
                        repetitions=args.repetitions,
                        interval_s=args.interval,
                        metadata={
                            "context": args.context,
                            "namespace": args.namespace,
                            "git_sha": revision,
                            "worktree_dirty": dirty,
                        },
                    )
                )
        trace_errors: list[str] = []
        if args.collect_traces:
            from command_trace import collect, summarize

            ids = {s["correlation_id"] for s in report["steps"] if "correlation_id" in s}
            for command_id in sorted(ids):
                try:
                    records, errors = collect(args.context, args.namespace, command_id, "1h")
                except Exception as exc:
                    records, errors = [], ["collection failed: " + type(exc).__name__]
                trace_errors.extend(command_id + ": " + error for error in errors)
                if not records:
                    trace_errors.append(command_id + ": no retained records")
                dest = output / "traces" / command_id
                dest.mkdir(parents=True)
                (dest / "records.jsonl").write_text(
                    "".join(json.dumps(r) + "\n" for r in records), encoding="utf-8"
                )
                (dest / "summary.json").write_text(
                    json.dumps(summarize(records, collection_errors=errors), indent=2) + "\n",
                    encoding="utf-8",
                )
            report["trace_collection"] = {
                "status": "unknown" if trace_errors else "collected",
                "errors": trace_errors,
                "completeness": "not_established",
            }
            (output / "summary.json").write_text(
                json.dumps(report, indent=2) + "\n", encoding="utf-8"
            )
        print("Report: " + str(output / "summary.json"))
        raise SystemExit(
            2
            if trace_errors
            else 0
            if report["outcome"] == "passed"
            else 1
            if report["outcome"] == "failed"
            else 2
        )
    except KeyboardInterrupt:
        print("Interrupted; inspect retained evidence before continuing.")
        raise SystemExit(130) from None
    except Exception as exc:
        if not (output / "summary.json").exists():
            output.mkdir(parents=True, exist_ok=True)
            (output / "summary.json").write_text(
                json.dumps(
                    {"outcome": "unknown", "phase": "setup", "error_type": type(exc).__name__},
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
        print("Runner stopped: " + type(exc).__name__ + "; inspect " + str(output / "summary.json"))
        raise SystemExit(2) from None


if __name__ == "__main__":
    main()
