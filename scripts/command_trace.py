"""Collect retained command evidence from all current command-service pods.

Wall-clock sorting is for display only. Missing records and cross-process order
remain unknown; this tool is not a conformance checker or durable log service.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

COMPONENTS = (
    "amqp-actuation-event-subscriber",
    "actuation-event-validator",
    "actuation-control-orchestrator",
    "actuation-signal-publisher",
    "decision-agent-stub",
)
REQUIRED_ACTIONS = (
    "request_received",
    "validation_decided",
    "executor_admitted",
    "write_initiated",
    "device_acknowledged",
    "writer_result",
    "outcome_published",
)


def extract_records(
    text: str, command_id: str, *, pod: str, previous: bool
) -> list[dict[str, Any]]:
    records = []
    for line in text.splitlines():
        try:
            record = json.loads(line)
        except (ValueError, TypeError):
            continue
        if not isinstance(record, dict):
            continue
        if record.get("event") != "command_execution" or record.get("correlation_id") != command_id:
            continue
        records.append({**record, "source": {"pod": pod, "previous": previous}})
    return records


def summarize(records: list[dict[str, Any]], *, collection_errors: list[str]) -> dict[str, Any]:
    actions = {r.get("action") for r in records}
    return {
        "record_count": len(records),
        "observed_actions": sorted(a for a in actions if isinstance(a, str)),
        "missing_actions": [a for a in REQUIRED_ACTIONS if a not in actions],
        "process_response": "observed" if "process_observed" in actions else "unknown",
        "collection_errors": collection_errors,
        "coverage": "retained_logs_only; completeness_not_established",
        "ordering": "cross_process_order_not_established",
        "formal_conformance": "not_assessed",
    }


def collect(
    context: str, namespace: str, command_id: str, since: str
) -> tuple[list[dict[str, Any]], list[str]]:
    prefix = ["kubectl", "--context", context, "-n", namespace]
    selector = "app.kubernetes.io/name in (" + ",".join(COMPONENTS) + ")"
    inventory = subprocess.run(
        [*prefix, "get", "pods", "-l", selector, "-o", "json"],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    records: list[dict[str, Any]] = []
    errors: list[str] = []
    pods = json.loads(inventory.stdout)["items"]
    present = {p["metadata"].get("labels", {}).get("app.kubernetes.io/name") for p in pods}
    errors.extend(f"{c}: no current pod" for c in COMPONENTS if c not in present)
    for pod in pods:
        name = pod["metadata"]["name"]
        statuses = pod.get("status", {}).get("containerStatuses", [])
        restarted = any(s.get("restartCount", 0) for s in statuses)
        for previous in [False, True] if restarted else [False]:
            args = [*prefix, "logs", name, "--since=" + since, "--limit-bytes=10485760"]
            if previous:
                args.append("--previous")
            try:
                result = subprocess.run(args, capture_output=True, text=True, timeout=30)
            except subprocess.TimeoutExpired:
                errors.append(f"{name}: logs timed out (previous={previous})")
                continue
            if result.returncode:
                errors.append(f"{name}: logs unavailable (previous={previous})")
                continue
            if len(result.stdout.encode("utf-8")) >= 10485760:
                errors.append(f"{name}: log size limit reached (previous={previous})")
            records.extend(extract_records(result.stdout, command_id, pod=name, previous=previous))
    records.sort(key=lambda r: str(r.get("recorded_at", "")))
    return records, errors


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command_id")
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", default="eirvah-edge")
    parser.add_argument("--since", default="1h")
    parser.add_argument("--output", type=Path, required=True, help="New evidence directory")
    args = parser.parse_args()
    records, errors = collect(args.context, args.namespace, args.command_id, args.since)
    summary = summarize(records, collection_errors=errors)
    summary.update(
        command_id=args.command_id,
        context=args.context,
        namespace=args.namespace,
        collected_at=datetime.now(UTC).isoformat(),
        since=args.since,
    )
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "records.jsonl").write_text(
        "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in records), encoding="utf-8"
    )
    (args.output / "summary.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, indent=2))
    if errors or not records:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
