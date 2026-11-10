"""Versioned local study scheduling, resets, repetition and retained per-stage capture."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from e0_capture import Cluster
from e1_rehearsal import settle
from interoperability import ROOT, digest
from pydantic import BaseModel, ConfigDict, Field, model_validator
from split_deployment import load_config, logical_manifest, render, target_mapping
from study_resources import Sampler

Stage = Literal[
    "probe", "e0", "e1", "e1_commands", "e2_native", "e2_commands", "e2_summary", "split"
]
KEYS = {
    "probe": "workload_case",
    "e0": "status",
    "e1": "local_mechanics",
    "e1_commands": "replicated_commands",
    "e2_native": "native_case",
    "e2_commands": "command_case",
    "e2_summary": "selected_preparation",
    "split": "local_split",
}
SCRIPTS = {
    "probe": "study_workload.py",
    "e0": "e0_capture.py",
    "e1": "e1_rehearsal.py",
    "e1_commands": "e1_replicated_commands.py",
    "e2_native": "e2_native.py",
    "e2_commands": "e2_command_response.py",
    "e2_summary": "e2_suite.py",
    "split": "split_rehearsal.py",
}


class CleanupUnconfirmed(RuntimeError):
    """The owned child may still be operating; retain the suite lock."""


class Plan(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    schema_version: Literal["1.0"] = "1.0"
    scope: Literal["local_engineering"] = "local_engineering"
    id: str = Field(pattern=r"^[a-z0-9-]+$")
    context: str = "kind-eirvah-edge"
    repetitions: int = Field(default=2, ge=1, le=20)
    seed: int = 0
    stages: list[Stage] = Field(min_length=1)
    data_rates: list[int] = Field(default_factory=lambda: [60], min_length=1)
    duration_s: int = Field(default=45, ge=10, le=600)
    command_rate: int = Field(default=5, ge=1, le=100)
    event_rate: int = Field(default=5, ge=1, le=100)
    hpa_rate: int = Field(default=160, ge=1, le=400)
    hpa_seconds: int = Field(default=120, ge=90, le=180)
    sample_interval_s: float = Field(default=5, ge=2, le=60)
    stage_timeout_s: int = Field(default=1800, ge=60, le=7200)
    cleanup_grace_s: int = Field(default=300, ge=60, le=600)
    split_config: str = "config/deployments/study-split.json"
    clock_uncertainty_s: float | None = Field(default=None, ge=0)
    budgets: dict[str, float] | None = None

    @model_validator(mode="after")
    def consistent(self) -> Plan:
        if self.context != "kind-eirvah-edge" or len(set(self.stages)) != len(self.stages):
            raise ValueError("selected local context and unique stages required")
        if any(not 1 <= v <= 400 for v in self.data_rates) or len(set(self.data_rates)) != len(
            self.data_rates
        ):
            raise ValueError("distinct offered data rates must be 1..400")
        for stage, parents in {
            "e2_commands": ["e2_native"],
            "e2_summary": ["e2_native", "e2_commands"],
            "split": ["e0"],
        }.items():
            if stage in self.stages and any(
                p not in self.stages or self.stages.index(p) > self.stages.index(stage)
                for p in parents
            ):
                raise ValueError("stage dependency/order missing: " + stage)
        if self.budgets is not None and (
            set(self.budgets) - {"command_p95_s", "minimum_completion_ratio"}
            or any(not v >= 0 for v in self.budgets.values())
        ):
            raise ValueError("unsupported or negative acceptance budget")
        return self


def commands_for(plan: Plan, repetition: int) -> list[dict[str, Any]]:
    order = ["shared", "separate"] if (repetition + plan.seed) % 2 else ["separate", "shared"]
    rates = list(plan.data_rates)
    random.Random(plan.seed + repetition).shuffle(rates)
    result = []
    for kind in plan.stages:
        for rate in rates if kind == "probe" else [None]:
            result.append(
                dict(
                    kind=kind,
                    label=kind + ("-" + str(rate) if rate is not None else ""),
                    order=order,
                    data_rate=rate,
                )
            )
    return result


def stage_status(exit_code: int | None, report: dict[str, Any], kind: str) -> str:
    status = report.get(KEYS[kind], "unknown")
    if status == "fail":
        return "fail"
    return "pass" if exit_code == 0 and status == "pass" else "unknown"


def reset_e1(context: str, output: Path) -> None:
    cluster = Cluster(context, "eirvah-e1")
    ns = json.loads(cluster.run("get", "namespace", "eirvah-e1", "-o", "json"))
    if ns["metadata"].get("labels", {}).get("eirvah.uniza/purpose") != "e1-local-rehearsal":
        raise ValueError("E1 namespace ownership required")
    before = json.loads(cluster.run("get", "deployments", "-o", "json"))
    cluster.run("delete", "hpa", "uns-auto-contextualizer", "--ignore-not-found=true")
    for name in ("uns-auto-contextualizer", "e1-processing"):
        cluster.run("scale", "deployment/" + name, "--replicas=1")
    cluster.run("set", "env", "deployment/e1-processing", "E1_MODE=shared")
    for name in ("uns-auto-contextualizer", "e1-processing"):
        cluster.run("rollout", "status", "deployment/" + name, "--timeout=120s", timeout=130)
        with asyncio.Runner(loop_factory=asyncio.SelectorEventLoop) as runner:
            runner.run(settle(cluster, name, 1))
    (output / "reset.json").write_text(
        json.dumps(
            dict(
                status="pass",
                scope="E1 replica/mode reset; history retained",
                before=before,
                after=json.loads(cluster.run("get", "deployments", "-o", "json")),
            ),
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def verify_boundary(plan: Plan, kind: str, config: dict[str, Any]) -> None:
    actors = (
        [("eirvah-e1", name) for name in ("uns-auto-contextualizer", "e1-processing")]
        if kind in ("e1", "probe")
        else []
    )
    if kind in ("e0", "split"):
        actors += [
            (config["edge"]["namespace"], name)
            for name in ("decision-agent-stub", "actuation-signal-publisher")
        ]
    if kind in ("e1_commands", "e2_commands"):
        actors += [
            ("eirvah-edge", name)
            for name in (
                "amqp-actuation-event-subscriber",
                "actuation-control-orchestrator",
                "actuation-event-validator",
                "actuation-signal-publisher",
                "decision-agent-stub",
            )
        ]
    for namespace, name in actors:
        cluster = Cluster(plan.context, namespace)
        for _ in range(60):
            items = json.loads(
                cluster.run("get", "pods", "-l", "app.kubernetes.io/name=" + name, "-o", "json")
            )["items"]
            if (
                len(items) == 1
                and not items[0]["metadata"].get("deletionTimestamp")
                and all(c["ready"] for c in items[0].get("status", {}).get("containerStatuses", []))
                and items[0]["status"].get("phase") == "Running"
            ):
                break
            time.sleep(1)
        else:
            raise RuntimeError("stage boundary not quiescent: " + name)


def bindings_for(kind: str, config: dict[str, Any]) -> list[dict[str, str]]:
    if kind in ("probe", "e1"):
        return [dict(context=config["edge"]["context"], namespace="eirvah-e1")]
    if kind in ("e0", "split"):
        return [config["edge"], config["cloud"]]
    if kind in ("e1_commands", "e2_commands"):
        return [dict(context=config["edge"]["context"], namespace="eirvah-edge")]
    return []


def execute(
    script: str,
    args: list[str],
    directory: Path,
    plan: Plan,
    bindings: list[dict[str, str]],
    owner: str | None = None,
) -> dict[str, Any]:
    directory.mkdir(parents=True, exist_ok=True)
    events = directory / "events.jsonl"
    events.touch()
    cancel = directory / "cancel-requested"
    env = os.environ.copy()
    env["EIRVAH_EXPERIMENT_EVENTS"] = str(events.resolve())
    command = [
        sys.executable,
        str(ROOT / "scripts/study_child.py"),
        "--cancel",
        str(cancel.resolve()),
        str(ROOT / "scripts" / script),
        *args,
    ]
    row: dict[str, Any] = dict(
        script=script,
        arguments=args,
        started=datetime.now(UTC).isoformat(),
        directory=str(directory.resolve()),
        timed_out=False,
    )
    sampler = Sampler(
        directory / "resources.jsonl",
        bindings,
        events,
        plan.sample_interval_s,
        docker_owner=owner,
        queues=any(b["namespace"] == "eirvah-e1" for b in bindings),
    )
    with (directory / "console.log").open("w", encoding="utf-8") as log:
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
        row["child_pid"] = process.pid
        sampler.add_process(process.pid)
        sampler.add_process(os.getpid())
        sampler.start()
        deadline = time.perf_counter() + plan.stage_timeout_s
        try:
            while process.poll() is None:
                if time.perf_counter() > deadline:
                    row["timed_out"] = True
                    cancel.touch()
                    try:
                        process.wait(timeout=plan.cleanup_grace_s)
                    except subprocess.TimeoutExpired:
                        row["cleanup"] = "unconfirmed_child_still_running"
                    break
                time.sleep(0.25)
        except KeyboardInterrupt:
            cancel.touch()
            try:
                process.wait(timeout=plan.cleanup_grace_s)
            except subprocess.TimeoutExpired:
                row["cleanup"] = "unconfirmed_child_still_running"
                raise CleanupUnconfirmed("interrupted child cleanup not confirmed") from None
            raise
        finally:
            sampler.stop()
            row.update(
                exit_code=process.poll(),
                finished=datetime.now(UTC).isoformat(),
                resource_errors=sampler.errors,
            )
            (directory / "step.json").write_text(json.dumps(row, indent=2) + "\n", encoding="utf-8")
    if row.get("cleanup"):
        raise CleanupUnconfirmed("child cleanup not confirmed; no subsequent stages will run")
    return row


def report_path(kind: str, output: Path) -> Path:
    return output / ("local-case-report.json" if kind == "e0" else "report.json")


def fixture_group(kind: str) -> str | None:
    return "split" if kind in ("e0", "split") else "baseline" if kind == "e2_commands" else None


def reset_fixture(
    plan: Plan,
    config: dict[str, Any],
    kind: str,
    directory: Path,
    deploy: Path,
    *,
    park: bool = False,
) -> None:
    path = deploy / "targets.json"
    if fixture_group(kind) == "baseline":
        path = directory / "reset-targets.json"
        path.write_text(
            json.dumps(
                dict(default=dict(context=plan.context, namespace="eirvah-edge"), components={})
            )
        )
    label = "park" if park else "reset"
    artifact = directory / ("native-" + label)
    args = ["--targets", str(path.resolve()), "--output", str(artifact.resolve())]
    if park:
        args.append("--park")
    if fixture_group(kind) == "split" and config.get("fixture", {}).get("start_parked"):
        args.append("--require-drained-inspections")
    row = execute(
        "study_reset.py", args, directory / (label + "-driver"), plan, bindings_for(kind, config)
    )
    report = artifact / "report.json"
    if (
        row["exit_code"] != 0
        or row["timed_out"]
        or not report.exists()
        or json.loads(report.read_text()).get("reset") != "pass"
        or not json.loads(report.read_text()).get("restored")
    ):
        raise RuntimeError(
            "native fixture " + label + " failed; subsequent measurements not started"
        )


def run_plan(plan: Plan, output: Path) -> None:
    config = load_config(ROOT / plan.split_config)
    if any(config[z]["context"] != plan.context for z in ("edge", "cloud")):
        raise ValueError("profile/capture context mismatch")
    deploy = output / "deployment"
    deploy.mkdir()
    for name, data in [
        ("config", config),
        ("targets", target_mapping(config)),
        ("architecture", logical_manifest(config)),
    ]:
        (deploy / (name + ".json")).write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    # Rendering verifies component selection; it does not rebuild images or rotate credentials.
    for zone in ("edge", "cloud"):
        render(config, zone)
    manifest: dict[str, Any] = dict(
        schema_version="1.0",
        scope=plan.scope,
        plan=plan.model_dump(),
        status="running",
        trials=[],
        started=datetime.now(UTC).isoformat(),
    )

    def persist() -> None:
        (output / "suite.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    persist()
    try:
        for repetition in range(1, plan.repetitions + 1):
            rep = output / f"r{repetition:03d}"
            rep.mkdir()
            completed: dict[str, Path] = {}
            for specification in commands_for(plan, repetition):
                kind, label = specification["kind"], specification["label"]
                directory = rep / label
                directory.mkdir()
                artifact = directory / ("result-" + output.name[-12:] + "-" + f"{repetition:03d}")
                targets = bindings_for(kind, config)
                if kind in ("probe", "e1"):
                    reset_e1(plan.context, directory)
                if fixture_group(kind):
                    reset_fixture(plan, config, kind, directory, deploy)
                args = ["--output", str(artifact.resolve())]
                if kind == "probe":
                    profile = directory / "workload.json"
                    profile.write_text(
                        json.dumps(
                            dict(
                                duration_s=plan.duration_s,
                                data_rate=specification["data_rate"],
                                command_rate=plan.command_rate,
                                event_rate=plan.event_rate,
                                order=specification["order"],
                            ),
                            indent=2,
                        )
                        + "\n"
                    )
                    args += ["--context", plan.context, "--profile", str(profile.resolve())]
                elif kind == "e0":
                    args += [
                        "--targets",
                        str((deploy / "targets.json").resolve()),
                        "--manifest",
                        str((deploy / "architecture.json").resolve()),
                        "--assurance",
                    ]
                elif kind == "e1":
                    args += [
                        "--context",
                        plan.context,
                        "--hpa-rate",
                        str(plan.hpa_rate),
                        "--hpa-seconds",
                        str(plan.hpa_seconds),
                    ]
                    if specification["order"][0] == "separate":
                        args.append("--reverse-order")
                elif kind in ("e1_commands", "e2_commands"):
                    args += ["--context", plan.context]
                    if kind == "e2_commands":
                        args += ["--native-run", str(completed["e2_native"].resolve())]
                elif kind == "e2_native":
                    args.append("--source-addition")
                elif kind == "e2_summary":
                    args += [
                        "--native-run",
                        str(completed["e2_native"].resolve()),
                        "--command-run",
                        str(completed["e2_commands"].resolve()),
                    ]
                elif kind == "split":
                    args += [
                        "--deployment",
                        str(deploy.resolve()),
                        "--e0-run",
                        str(completed["e0"].resolve()),
                    ]
                print(f"repetition={repetition} stage={label} starting", flush=True)
                row = execute(
                    SCRIPTS[kind],
                    args,
                    directory,
                    plan,
                    targets,
                    artifact.name.lower().replace("_", "-") if kind == "e2_native" else None,
                )
                path = report_path(kind, artifact)
                report = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
                row.update(
                    kind=kind,
                    label=label,
                    repetition=repetition,
                    order=specification["order"],
                    artifact=str(artifact.resolve()),
                    report=report,
                    status=stage_status(row["exit_code"], report, kind),
                )
                if (row["timed_out"] or row["resource_errors"]) and row["status"] != "fail":
                    row["status"] = "unknown"
                row["report_sha256"] = digest(path) if path.exists() else None
                manifest["trials"].append(row)
                persist()
                print(f"repetition={repetition} stage={label} status={row['status']}", flush=True)
                if fixture_group(kind) == "split":
                    reset_fixture(plan, config, kind, directory, deploy, park=True)
                if row["status"] != "pass":
                    raise RuntimeError("stage did not pass; subsequent stages not run")
                verify_boundary(plan, kind, config)
                completed[kind] = artifact
        manifest["status"] = "pass"
    except (Exception, KeyboardInterrupt) as exc:
        manifest.update(status="unknown", error=type(exc).__name__ + ": " + str(exc)[:180])
        if isinstance(exc, CleanupUnconfirmed):
            manifest["cleanup_unconfirmed"] = True
        if any(t["status"] == "fail" for t in manifest["trials"]):
            manifest["status"] = "fail"
    finally:
        manifest["finished"] = datetime.now(UTC).isoformat()
        persist()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--repetitions", type=int)
    parser.add_argument("--plan", action="store_true")
    args = parser.parse_args()
    raw = json.loads(args.profile.read_text(encoding="utf-8"))
    if args.repetitions is not None:
        raw["repetitions"] = args.repetitions
    plan = Plan.model_validate(raw)
    if args.plan:
        print(
            json.dumps(
                [
                    dict(repetition=r, stages=commands_for(plan, r))
                    for r in range(1, plan.repetitions + 1)
                ],
                indent=2,
            )
        )
        return
    output = args.output or ROOT / "results/study" / datetime.now(UTC).strftime(
        "%Y-%m-%dT%H-%M-%S-%fZ"
    )
    output.mkdir(parents=True, exist_ok=False)
    lock = ROOT / "results/active-study.json"
    identity = uuid.uuid4().hex
    descriptor = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.write(
        descriptor,
        json.dumps(dict(id=identity, pid=os.getpid(), output=str(output.resolve()))).encode(),
    )
    os.close(descriptor)
    try:
        (output / "profile.json").write_text(
            plan.model_dump_json(indent=2) + "\n", encoding="utf-8"
        )
        files = subprocess.check_output(["git", "ls-files"], cwd=ROOT, text=True).splitlines()
        new = subprocess.check_output(
            ["git", "ls-files", "--others", "--exclude-standard"], cwd=ROOT, text=True
        ).splitlines()
        selected = [
            ROOT / p
            for p in [*files, *new]
            if p.endswith((".py", ".json", ".yaml", ".toml", ".lock", "Dockerfile"))
            and not p.startswith("docs/")
        ]
        source = {p.relative_to(ROOT).as_posix(): digest(p) for p in selected if p.is_file()}
        for path in source:
            target = output / "sources" / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((ROOT / path).read_bytes())
        (output / "provenance.json").write_text(
            json.dumps(
                dict(
                    git_sha=subprocess.check_output(
                        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
                    ).strip(),
                    source_hashes=source,
                    python=sys.version,
                ),
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        run_plan(plan, output)
        from study_report import write_report

        summary = write_report(output)
        print(
            json.dumps(
                dict(status=summary["execution"], report=str(output / "report.md")), indent=2
            )
        )
        raise SystemExit(0 if summary["execution"] == "pass" else 1)
    finally:
        suite_path = output / "suite.json"
        uncertain = suite_path.exists() and json.loads(suite_path.read_text()).get(
            "cleanup_unconfirmed"
        )
        if lock.exists() and json.loads(lock.read_text()).get("id") == identity and not uncertain:
            lock.unlink()


if __name__ == "__main__":
    main()
