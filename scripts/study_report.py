"""Combine scoped trial evidence; missing coverage and small samples remain explicit."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from datetime import datetime
from itertools import pairwise
from pathlib import Path
from typing import Any

from experiment_stats import summarize
from interoperability import digest
from study_resources import phase_for_window


def summarize_resources(
    rows: list[dict[str, Any]], interval: float | None = None
) -> dict[str, Any]:
    valid = [
        r
        for r in rows
        if r.get("backend") == "kubernetes" and r.get("status") == "ok" and r.get("values")
    ]
    cpu = [sum(v["cpu_cores"] for v in r["values"]) for r in valid]
    memory = [sum(v["memory_bytes"] for v in r["values"]) for r in valid]
    missing = sum(
        r.get("status") in ("unknown", "missing")
        or r.get("missing_pods") is None
        or bool(r.get("missing_pods"))
        for r in rows
    )
    times = sorted(
        datetime.fromisoformat(r["collected_begin"]).timestamp()
        for r in rows
        if r.get("collected_begin")
    )
    gaps = [b - a for a, b in pairwise(times)]
    delayed = sum(g > 2 * interval for g in gaps) if interval is not None else 0
    return dict(
        coverage="unknown"
        if not valid
        else "partial"
        if missing or delayed
        else "observed_samples",
        largest_collection_gap_s=max(gaps) if gaps else None,
        delayed_collection_intervals=delayed,
        samples=len(valid),
        missing_attempts=missing,
        cpu_peak_cores=max(cpu) if cpu else None,
        memory_peak_bytes=max(memory) if memory else None,
        cpu_sample_mean_cores=statistics.mean(cpu) if cpu else None,
        limitation="namespace snapshot totals; overlapping server averaging windows; not a time-weighted mean or CPU integral",
    )


def paired_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    pairs: dict[tuple[Any, ...], dict[str, float]] = defaultdict(dict)
    for row in rows:
        if row.get("p95_s") is not None:
            pairs[(row.get("label", ""), row["repetition"], row["flow"])][row["mode"]] = row[
                "p95_s"
            ]
    grouped: dict[tuple[str, str], list[float]] = defaultdict(list)
    for (label, _, flow), modes in pairs.items():
        if set(modes) == {"shared", "separate"}:
            grouped[label, flow].append(modes["separate"] - modes["shared"])
    return [
        dict(
            label=key[0],
            flow=key[1],
            pairs=len(values),
            median_separate_minus_shared_s=statistics.median(values),
            minimum_s=min(values),
            maximum_s=max(values),
            paired_deltas_s=values,
            inference="descriptive only; independent unit is the repetition",
        )
        for key, values in grouped.items()
    ]


def workload_rates(rows: list[dict[str, Any]], flow: str) -> dict[str, Any]:
    """Rates over the whole observed schedule plus drain, not nominal offered load."""
    window = max((r["sent"] + r["latency_s"] for r in rows), default=0)
    selected = [r for r in rows if r["flow"] == flow]
    lateness = sorted(max(0, r["sent"] - r["scheduled"]) for r in selected)
    return dict(
        observed_window_s=window,
        achieved_completions_per_s=sum(r["status"] == "ok" for r in selected) / window
        if window > 0
        else None,
        observed_submissions_per_s=len(selected) / window if window > 0 else None,
        submission_delay_p95_s=lateness[math.ceil(0.95 * len(lateness)) - 1] if lateness else None,
        rate_scope="whole mixed workload window including request drain; processing replies, not physical effects",
    )


def read_lines(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def phases_from(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    starts = {r["phase_id"]: r for r in events if r["event"] == "phase_begin"}
    return [
        dict(
            name=r["phase"],
            phase_id=r["phase_id"],
            begin=datetime.fromisoformat(starts[r["phase_id"]]["observed_at"]).timestamp(),
            end=datetime.fromisoformat(r["observed_at"]).timestamp(),
            success=r["success"],
        )
        for r in events
        if r["event"] == "phase_end" and r["phase_id"] in starts
    ]


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = list(dict.fromkeys(k for r in rows for k in r))
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        if fields:
            writer.writeheader()
            writer.writerows(rows)


def write_report(output: Path) -> dict[str, Any]:
    suite = json.loads((output / "suite.json").read_text())
    plan = suite["plan"]
    host_resources: list[dict[str, Any]] = []
    docker_resources: list[dict[str, Any]] = []
    metrics = []
    resources = []
    phase_usage = []
    queue_summaries = []
    integrity = []
    capture_coverage = []
    for trial in suite["trials"]:
        directory = Path(trial["directory"])
        artifact = Path(trial["artifact"])
        report = artifact / ("local-case-report.json" if trial["kind"] == "e0" else "report.json")
        good = report.exists() and digest(report) == trial["report_sha256"]
        integrity.append(
            dict(repetition=trial["repetition"], stage=trial["label"], report_hash_matches=good)
        )
        rows = read_lines(directory / "resources.jsonl")
        phases = phases_from(read_lines(directory / "events.jsonl"))
        capture_coverage.append(
            dict(
                repetition=trial["repetition"],
                stage=trial["label"],
                host_process="observed_samples"
                if any(r.get("backend") == "host-process" and r.get("status") == "ok" for r in rows)
                else "unknown",
                owned_docker=(
                    "observed_samples"
                    if any(
                        r.get("backend") == "docker" and r.get("status") == "ok" and r.get("values")
                        for r in rows
                    )
                    else "unknown"
                )
                if trial["kind"] == "e2_native"
                else "not_applicable",
                kubernetes=(
                    "observed_samples"
                    if any(
                        r.get("backend") == "kubernetes"
                        and r.get("status") == "ok"
                        and r.get("values")
                        for r in rows
                    )
                    else "unknown"
                )
                if trial["kind"] in ("probe", "e0", "e1", "e1_commands", "e2_commands", "split")
                else "not_applicable",
                limitation="sample presence only; gaps/missing pods and raw backend records still apply",
            )
        )
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            if row.get("backend") == "kubernetes":
                binding = row.get("binding", row.get("target", {}))
                groups[
                    binding.get("context", "unknown") + "|" + binding.get("namespace", "unknown")
                ].append(row)
        for binding, samples in groups.items():
            summary = summarize_resources(samples, plan["sample_interval_s"])
            resources.append(
                dict(
                    repetition=trial["repetition"], stage=trial["label"], binding=binding, **summary
                )
            )
            for phase in phases:
                selected = []
                for sample in samples:
                    values = []
                    for value in sample.get("values", []):
                        end = datetime.fromisoformat(value["server_timestamp"]).timestamp()
                        window = float(value["window"].removesuffix("s"))
                        if phase_for_window(
                            end - window, end, [phase], plan.get("clock_uncertainty_s")
                        ):
                            values.append(value)
                    if values and len(values) == len(sample.get("values", [])):
                        selected.append(
                            dict(
                                backend="kubernetes",
                                status=sample["status"],
                                values=values,
                                missing_pods=sample.get("missing_pods"),
                            )
                        )
                phase_usage.append(
                    dict(
                        repetition=trial["repetition"],
                        stage=trial["label"],
                        binding=binding,
                        phase=phase["name"],
                        **summarize_resources(selected),
                    )
                )
        for phase in phases:
            series = []
            for sample in rows:
                if sample.get("backend") != "prometheus" or sample.get("status") != "ok":
                    continue
                for value in sample["values"]:
                    timestamp = float(value["value"][0])
                    number = float(value["value"][1])
                    if math.isfinite(number) and phase_for_window(
                        timestamp, timestamp, [phase], plan.get("clock_uncertainty_s")
                    ):
                        labels = dict(value["metric"])
                        name = labels.pop("__name__")
                        series.append((timestamp, name, json.dumps(labels, sort_keys=True), number))
            if series:
                queue_summaries.append(
                    dict(
                        repetition=trial["repetition"],
                        stage=trial["label"],
                        phase=phase["name"],
                        metrics=summarize(series),
                    )
                )
        process_groups: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
        containers: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            if row.get("backend") == "host-process" and row.get("status") == "ok":
                process_groups[row["pid"], row["identity"]].append(row)
            if row.get("backend") == "docker" and row.get("status") == "ok":
                for value in row["values"]:
                    containers[value["ID"]].append(value)
        for (pid, identity), samples in process_groups.items():
            values = [v["cpu_seconds"] for v in samples]
            host_resources.append(
                dict(
                    repetition=trial["repetition"],
                    stage=trial["label"],
                    pid=pid,
                    identity=identity,
                    samples=len(samples),
                    observed_cpu_seconds=max(values) - min(values) if len(samples) >= 2 else None,
                    rss_peak_bytes=max(v["rss_bytes"] for v in samples),
                    scope="observed process window, includes driver/setup overhead",
                )
            )
        for cid, samples in containers.items():
            percentages = [float(v["CPUPerc"].removesuffix("%")) for v in samples]
            docker_resources.append(
                dict(
                    repetition=trial["repetition"],
                    stage=trial["label"],
                    container=cid,
                    samples=len(samples),
                    cpu_percent_peak=max(percentages),
                    memory_usage_last=samples[-1]["MemUsage"],
                    scope="owned E2 container; Docker native percent/usage units retained",
                )
            )
        path = artifact / "evidence.json"
        if path.exists() and trial["kind"] in ("probe", "e1"):
            data = json.loads(path.read_text())
            cases = data.get("cases", data.get("comparison", []))
            for case in cases:
                for flow, value in case["statistics"].items():
                    metrics.append(
                        dict(
                            repetition=trial["repetition"],
                            label=trial["label"],
                            mode=case["mode"],
                            flow=flow,
                            **value,
                            **workload_rates(case["rows"], flow),
                        )
                    )
    expected = {
        (r, stage + ("-" + str(rate) if stage == "probe" else ""))
        for r in range(1, plan["repetitions"] + 1)
        for stage in plan["stages"]
        for rate in (plan["data_rates"] if stage == "probe" else [None])
    }
    actual = {(t["repetition"], t["label"]) for t in suite["trials"]}
    complete = (
        actual == expected
        and len(suite["trials"]) == len(expected)
        and all(v["report_hash_matches"] for v in integrity)
    )
    execution = (
        suite["status"]
        if complete and suite["status"] in ("pass", "fail", "unknown")
        else "unknown"
    )
    if suite["status"] == "fail":
        execution = "fail"

    budgets = plan.get("budgets")
    acceptance = "unknown"
    if budgets is not None and metrics:
        checks = []
        for row in metrics:
            if "minimum_completion_ratio" in budgets:
                checks.append(row["completed"] / row["sent"] >= budgets["minimum_completion_ratio"])
            if row["flow"] == "command" and "command_p95_s" in budgets:
                checks.append(row["p95_s"] is not None and row["p95_s"] <= budgets["command_p95_s"])
        acceptance = "pass" if checks and all(checks) else "fail" if checks else "unknown"
    if acceptance == "pass" and execution != "pass":
        acceptance = "unknown"
    summary = dict(
        execution=execution,
        scope=plan["scope"],
        scientific_acceptance=acceptance if budgets is not None else "not_configured",
        formal_study="not_established",
        completed_trials=len(suite["trials"]),
        repetitions=plan["repetitions"],
        integrity=integrity,
        capture_coverage=capture_coverage,
        paired=paired_summary(metrics),
        resource_scope="sampled namespaces, owned process counters and owned containers; raw backend coverage retained",
        resources=resources,
        host_resources=host_resources,
        docker_resources=docker_resources,
        analysis_source_sha256=digest(Path(__file__)),
        phase_resources=phase_usage,
        queue_metrics=queue_summaries,
    )
    (output / "analysis.json").write_text(
        json.dumps(summary, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )
    (output / "analysis-source.py").write_bytes(Path(__file__).read_bytes())
    write_csv(output / "host-resources.csv", host_resources)
    write_csv(output / "docker-resources.csv", docker_resources)
    write_csv(output / "workloads.csv", metrics)
    write_csv(output / "resources.csv", resources)
    write_csv(output / "phase-resources.csv", phase_usage)
    lines = [
        "# Experiment automation report",
        "",
        "Execution: **"
        + execution
        + "**. Scope: "
        + plan["scope"]
        + ". Formal study: not established.",
        "",
        "Profile budgets: **"
        + summary["scientific_acceptance"]
        + "** (local engineering criteria only when configured).",
        "",
        "| Repetition | Stage | Status | Evidence |",
        "| --- | --- | --- | --- |",
    ]
    lines += [
        f"| {t['repetition']} | {t['label']} | {t['status']} | [report]({(Path(t['artifact']) / ('local-case-report.json' if t['kind'] == 'e0' else 'report.json')).as_posix()}) |"
        for t in suite["trials"]
    ]
    lines += [
        "",
        "Outputs: `workloads.csv`, `resources.csv`, `phase-resources.csv`, `host-resources.csv`, `docker-resources.csv`, `analysis.json`; raw samples/events and child reports remain beside each trial.",
        "",
        "Paired differences use per-repetition p95 values; requests are not pooled across repetitions. Resource gaps remain unknown, and overlapping metrics-server windows are not integrated into CPU time. Phase attribution requires the explicitly declared clock-uncertainty assumption and a measurement window fully inside a phase.",
        "",
        "E2 native cases are isolated protocol/broker rehearsals; their owned Docker and host-process samples remain raw, separate from Kubernetes totals. E0/C04/whole-study unknowns in scoped child reports are not promoted by this suite.",
    ]
    if suite.get("error"):
        lines += ["", "Stopped: " + suite["error"]]
    (output / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    write_report(parser.parse_args().directory)
