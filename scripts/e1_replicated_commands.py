"""Exercise bounded command replay with replicated stateless front ends and one writer.

Uses the existing E0 capture for authenticated commands, native readback, private
execution journals and ledger reconciliation. Replica settings are restored.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from e0_capture import Cluster
from interoperability import ROOT, digest

ACTORS = [
    "amqp-actuation-event-subscriber",
    "actuation-control-orchestrator",
    "actuation-event-validator",
]


def replicated_verdict(data: dict[str, Any]) -> str:
    try:
        counts = Counter(
            r["correlation_id"] for r in data["records"] if r["action"] == "write_initiated"
        )
        if (
            data["writer_replicas"] != 1
            or any(n > 1 for n in counts.values())
            or data["local"]["status"] == "fail"
        ):
            return "fail"
        if (
            not data["restored"]
            or data["local"]["status"] != "pass"
            or not data["replay_seen"]
            or not counts
        ):
            return "unknown"
        if any(
            len({r["host"] for r in data["records"] if r["component"] == a}) != 2 for a in ACTORS
        ):
            return "unknown"
        return "pass"
    except (KeyError, TypeError):
        return "unknown"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.context != "kind-eirvah-edge":
        parser.error("this local simulator profile requires kind-eirvah-edge")
    cluster = Cluster(args.context, "eirvah-edge")
    output = args.output or ROOT / "results/e1-replicated-commands" / datetime.now(UTC).strftime(
        "%Y-%m-%dT%H-%M-%S-%fZ"
    )
    output.mkdir(parents=True, exist_ok=False)
    data: dict[str, Any] = dict(
        context=args.context,
        original={},
        records=[],
        local={},
        errors=[],
        restored=False,
        runner_sha256=digest(Path(__file__)),
        e0_runner_sha256=digest(ROOT / "scripts/e0_capture.py"),
    )

    def persist() -> None:
        (output / "evidence.json").write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")

    writer = json.loads(
        cluster.run("get", "deployment", "actuation-signal-publisher", "-o", "json")
    )
    data["writer_replicas"] = writer["spec"].get("replicas", 1)
    if data["writer_replicas"] != 1:
        parser.error("one authoritative writer required")
    for actor in ACTORS:
        deployment = json.loads(cluster.run("get", "deployment", actor, "-o", "json"))
        data["original"][actor] = deployment["spec"].get("replicas", 1)
    persist()
    try:
        for actor in ACTORS:
            cluster.run("scale", "deployment/" + actor, "--replicas=2")
        for actor in ACTORS:
            cluster.run("rollout", "status", "deployment/" + actor, "--timeout=120s", timeout=130)
        data["replicated_inventory"] = cluster.inventory([*ACTORS, "actuation-signal-publisher"])
        persist()
        capture = output / "capture"
        with (output / "capture.log").open("w", encoding="utf-8") as log:
            completed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts/e0_capture.py"),
                    "--context",
                    args.context,
                    "--output",
                    str(capture),
                ],
                cwd=ROOT,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=600,
            )
        data["capture_exit_code"] = completed.returncode
        for field, name in [
            ("local", "local-case-report.json"),
            ("records", "all-records.json"),
            ("submissions", "submissions.json"),
            ("controller_state", "controller-state.json"),
            ("source_case_state", "source-case-state.json"),
        ]:
            path = capture / name
            if path.exists():
                data[field] = json.loads(path.read_text(encoding="utf-8"))
        data["replay_seen"] = any(
            s.get("result", {}).get("replayed") is True for s in data.get("submissions", [])
        )
        if completed.returncode != 0:
            data["errors"].append("bounded command capture did not pass")
    except (Exception, KeyboardInterrupt) as exc:
        data["errors"].append(type(exc).__name__)
    finally:
        try:
            for actor, count in data["original"].items():
                cluster.run("scale", "deployment/" + actor, "--replicas=" + str(count))
            for actor in ACTORS:
                cluster.run(
                    "rollout", "status", "deployment/" + actor, "--timeout=120s", timeout=130
                )
            data["restored"] = all(
                json.loads(cluster.run("get", "deployment", a, "-o", "json"))["spec"]["replicas"]
                == count
                for a, count in data["original"].items()
            )
        except (OSError, subprocess.SubprocessError) as exc:
            data["errors"].append("restore:" + type(exc).__name__)
        persist()
    status = replicated_verdict(data)
    if data["errors"] and status == "pass":
        status = "unknown"
    report = dict(
        replicated_commands=status,
        whole_study="unknown",
        writer_count=1,
        scope="selected simulator replay, not multi-writer or broker-fault exactly-once claims",
    )
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(dict(**report, output=str(output), errors=data["errors"]), indent=2))
    raise SystemExit(0 if status == "pass" else 1)


if __name__ == "__main__":
    main()
