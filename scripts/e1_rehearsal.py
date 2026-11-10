"""Bounded local E1 mechanics in the dedicated eirvah-e1 namespace; not a capacity benchmark."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import subprocess
import time
from collections import Counter
from contextlib import ExitStack
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import nats
from control import build_command
from e0_capture import Cluster
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_contracts.inspection import BottleInspection
from eirvah_contracts.ulid import generate_correlation_id
from eirvah_observability.experiment import phase
from interoperability import ROOT, digest
from load_inject import NODE_IDS, _build_payload, _validate_reply
from scenario import port_forward

PROFILE = ROOT / "deploy/k3s/overlays/e1-local"
WORKER = ROOT / "services/uns-auto-contextualizer/src/uns_auto_contextualizer/service.py"
SOURCES = [
    ROOT / "libs/eirvah-observability/src/eirvah_observability/experiment.py",
    Path(__file__),
    ROOT / "scripts/e1_processing.py",
    WORKER,
    ROOT / "libs/eirvah-bus/src/eirvah_bus/consumer.py",
    ROOT / "deploy/e1.Dockerfile",
    ROOT / "config/eirvah-line-a.jsonld",
    ROOT / "deploy/k3s/base/actuation-event-validator/actuation-policy.yaml",
    *sorted(PROFILE.glob("*.yaml")),
]


def save(output: Path, data: Any) -> None:
    (output / "evidence.json").write_text(
        json.dumps(data, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )


def pods(cluster: Cluster, app: str) -> dict[str, str]:
    items = json.loads(
        cluster.run("get", "pods", "-l", "app.kubernetes.io/name=" + app, "-o", "json")
    )["items"]
    return {
        p["metadata"]["name"]: p["metadata"]["uid"]
        for p in items
        if not p["metadata"].get("deletionTimestamp")
        and all(
            c["ready"] and c["restartCount"] == 0 for c in p["status"].get("containerStatuses", [])
        )
        and p["status"].get("phase") == "Running"
    }


async def settle(cluster: Cluster, app: str, replicas: int) -> None:
    # Deployment rollout status alone can leave terminating consumers alive.
    # They still compete for work and would invalidate the comparison budget.
    for _ in range(60):
        items = json.loads(
            cluster.run("get", "pods", "-l", "app.kubernetes.io/name=" + app, "-o", "json")
        )["items"]
        if len(items) == replicas and len(pods(cluster, app)) == replicas:
            return
        await asyncio.sleep(2)
    raise RuntimeError("old consumers not gone or selected replicas not ready: " + app)


async def query(client: httpx.AsyncClient, expression: str) -> list[dict[str, Any]]:
    response = await client.get("/api/v1/query", params={"query": expression})
    response.raise_for_status()
    result = response.json()
    if result["status"] != "success":
        raise ValueError("Prometheus query failed")
    return list(result["data"]["result"])


async def processing_counters(
    prom: httpx.AsyncClient, worker: str, expected: dict[str, int]
) -> list[dict[str, Any]]:
    """Wait for the selected pod's scrape; absence is never a zero counter."""
    for _ in range(40):
        rows = await query(
            prom, 'eirvah_e1_completed_total{app="e1-processing",pod="' + worker + '"}'
        )
        totals = {
            flow: sum(float(v["value"][1]) for v in rows if v["metric"]["flow"] == flow)
            for flow in expected
        }
        if rows and totals == expected:
            return rows
        await asyncio.sleep(1)
    raise RuntimeError("selected pod counters did not reconcile before deadline")


async def counters(client: httpx.AsyncClient) -> dict[str, float]:
    rows = await query(
        client, 'sum by (pod) (eirvah_worker_handler_total{app="uns-auto-contextualizer"})'
    )
    return {r["metric"]["pod"]: float(r["value"][1]) for r in rows}


def check_sharing(case: dict[str, Any]) -> bool:
    try:
        rows, before, after = case["rows"], case["before"], case["after"]
        expected = case["pods_before"]
        counts = Counter(r["worker"] for r in rows)
        return bool(
            len(expected) == case["replicas"]
            and expected == case["pods_after"]
            and len(rows) > 0
            and len({r["correlation_id"] for r in rows}) == len(rows)
            and all(r["status"] == "ok" for r in rows)
            and set(counts) == set(expected)
            and all(after.get(p, 0) - before.get(p, 0) == counts[p] for p in expected)
            and not (set(after) - set(expected) - set(before))
        )
    except (KeyError, TypeError):
        return False


def check_hpa(case: dict[str, Any]) -> bool:
    try:
        return (
            any(s["ready"] >= 2 and s["cpu_present"] and s["active"] for s in case["samples"])
            and all(s["cpu_present"] and s["active"] for s in case["samples"] if s["ready"] >= 2)
            and case["cooled"]["ready"] == case["cooled"]["desired"] == 1
            and case["cooled"]["cpu_present"]
            and case["cooled"]["active"]
        )
    except (KeyError, TypeError):
        return False


def hpa_sample(cluster: Cluster) -> dict[str, Any]:
    h = json.loads(cluster.run("get", "hpa", "uns-auto-contextualizer", "-o", "json"))
    d = json.loads(cluster.run("get", "deployment", "uns-auto-contextualizer", "-o", "json"))
    return dict(
        at=datetime.now(UTC).isoformat(),
        ready=d["status"].get("readyReplicas", 0),
        desired=d["spec"]["replicas"],
        cpu_present=any(
            "averageUtilization" in m.get("resource", {}).get("current", {})
            for m in (h.get("status", {}).get("currentMetrics") or [])
        ),
        active=any(
            c["type"] == "ScalingActive" and c["status"] == "True"
            for c in (h.get("status", {}).get("conditions") or [])
        ),
        raw=h,
    )


async def request(
    client: Any, flow: str, body: bytes, offset: float, start: float, slots: asyncio.Semaphore
) -> dict[str, Any]:
    envelope = NATSEnvelope.model_validate_json(body)
    await asyncio.sleep(max(0, start + offset - time.perf_counter()))
    async with slots:
        sent = time.perf_counter()
        row = dict(
            correlation_id=envelope.correlation_id,
            flow=flow,
            scheduled=offset,
            sent=sent - start,
            status="unknown",
        )
        try:
            reply = await client.request(
                "uns.work.contextualize" if flow == "context" else "e1." + flow, body, timeout=10
            )
            parsed = NATSEnvelope.model_validate_json(reply.data)
            if parsed.correlation_id != envelope.correlation_id or parsed.status != "ok":
                raise ValueError("reply identity/status mismatch")
            if flow in ("context", "data"):
                _validate_reply(reply.data, envelope.correlation_id)
            elif flow == "command":
                if (
                    parsed.payload is None
                    or parsed.payload["decision"] != "approve"
                    or parsed.payload.get("approval") is not None
                ):
                    raise ValueError("comparison command policy mismatch")
            elif parsed.payload != envelope.payload:
                raise ValueError("inspection contract mismatch")
            row.update(
                status="ok",
                worker=(reply.headers or {}).get("X-Eirvah-Worker"),
                reply=parsed.model_dump(mode="json"),
            )
        except TimeoutError:
            row["status"] = "unknown"
        except Exception as exc:
            row.update(status="error", error=type(exc).__name__)
        row["latency_s"] = time.perf_counter() - sent
        return row


async def load(client: Any, jobs: list[tuple[float, str, bytes]]) -> list[dict[str, Any]]:
    start, slots = time.perf_counter(), asyncio.Semaphore(64)
    return list(
        await asyncio.gather(
            *(request(client, flow, body, offset, start, slots) for offset, flow, body in jobs)
        )
    )


def context_jobs(count: int, rate: int) -> list[tuple[float, str, bytes]]:
    return [
        (i / rate, "context", _build_payload(NODE_IDS[i % len(NODE_IDS)])) for i in range(count)
    ]


def mixed_jobs(
    duration: int, data_rate: int = 120, command_rate: int = 5, event_rate: int = 5
) -> list[tuple[float, str, bytes]]:
    jobs = [
        (i / data_rate, "data", _build_payload(NODE_IDS[i % len(NODE_IDS)]))
        for i in range(duration * data_rate)
    ]
    for i in range(duration * command_rate):
        command = build_command("temperature", "set", 26.0)
        jobs.append(
            (
                i / command_rate,
                "command",
                NATSEnvelope(
                    correlation_id=command.correlation_id, payload=command.model_dump(mode="json")
                )
                .model_dump_json()
                .encode(),
            )
        )
    for i in range(duration * event_rate):
        event = BottleInspection(
            inspection_id=f"0000000000000001:{i + 1:04x}",
            bottle_id=i + 1,
            passed=False,
            inspected_at=datetime.now(UTC),
        )
        jobs.append(
            (
                i / event_rate,
                "event",
                NATSEnvelope(
                    correlation_id=generate_correlation_id(), payload=event.model_dump(mode="json")
                )
                .model_dump_json()
                .encode(),
            )
        )
    return sorted(jobs, key=lambda job: job[0])


def stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    result = {}
    for flow in sorted({r["flow"] for r in rows}):
        selected = [r for r in rows if r["flow"] == flow]
        values = sorted(r["latency_s"] for r in selected if r["status"] == "ok")
        result[flow] = dict(
            sent=len(selected),
            completed=len(values),
            unknown=sum(r["status"] == "unknown" for r in selected),
            errors=sum(r["status"] == "error" for r in selected),
            p95_s=values[math.ceil(0.95 * len(values)) - 1] if values else None,
            p99_s=values[math.ceil(0.99 * len(values)) - 1] if len(values) >= 100 else None,
        )
    return result


async def capture(
    cluster: Cluster,
    nc: Any,
    prom: httpx.AsyncClient,
    output: Path,
    data: dict[str, Any],
    hpa_seconds: int,
    hpa_rate: int,
    reverse_order: bool = False,
) -> None:
    for replicas in (1, 3):
        cluster.run("scale", "deployment/uns-auto-contextualizer", "--replicas=" + str(replicas))
        cluster.run(
            "rollout", "status", "deployment/uns-auto-contextualizer", "--timeout=120s", timeout=130
        )
        await settle(cluster, "uns-auto-contextualizer", replicas)
        await asyncio.sleep(5)
        case: dict[str, Any] = dict(
            replicas=replicas,
            pods_before=pods(cluster, "uns-auto-contextualizer"),
            before=await counters(prom),
        )
        with phase("sharing-" + str(replicas)):
            case["rows"] = await load(nc, context_jobs(120, 40))
        await asyncio.sleep(5)
        case.update(after=await counters(prom), pods_after=pods(cluster, "uns-auto-contextualizer"))
        case["pass"] = check_sharing(case)
        data["sharing"].append(case)
        save(output, data)
        print(
            f"replicas={replicas} sharing={case['pass']} workers={len(set(r.get('worker') for r in case['rows']))}",
            flush=True,
        )
    # Same exact payloads and schedule in both variants; no writes/approvals occur.
    jobs = mixed_jobs(8)
    data["comparison_workload"] = [dict(offset=o, flow=f, body=json.loads(b)) for o, f, b in jobs]
    for mode in ("separate", "shared") if reverse_order else ("shared", "separate"):
        cluster.run("set", "env", "deployment/e1-processing", "E1_MODE=" + mode)
        cluster.run("rollout", "restart", "deployment/e1-processing")
        cluster.run("rollout", "status", "deployment/e1-processing", "--timeout=120s", timeout=130)
        await settle(cluster, "e1-processing", 1)
        await load(nc, mixed_jobs(1))  # Initialize both pools before the measured workload.
        selected = pods(cluster, "e1-processing")
        if len(selected) != 1:
            raise RuntimeError("expected one comparison worker")
        worker = next(iter(selected))
        warmup = {"data": 120, "command": 5, "event": 5}
        before = await processing_counters(prom, worker, warmup)
        row: dict[str, Any] = dict(
            mode=mode, pods_before=pods(cluster, "e1-processing"), before=before
        )
        with phase("comparison-" + mode):
            row["rows"] = await load(nc, jobs)
        after = await processing_counters(
            prom, worker, {f: warmup[f] + sum(r["flow"] == f for r in row["rows"]) for f in warmup}
        )
        row.update(
            after=after,
            pods_after=pods(cluster, "e1-processing"),
            statistics=stats(row["rows"]),
            deployment=json.loads(cluster.run("get", "deployment", "e1-processing", "-o", "json")),
        )
        data["comparison"].append(row)
        save(output, data)
        print("comparison=" + mode + " " + json.dumps(row["statistics"]), flush=True)
    # CPU is supplied by metrics-server. Backlog/service metrics are diagnostics, not fake custom HPA metrics.
    cluster.run("scale", "deployment/uns-auto-contextualizer", "--replicas=1")
    cluster.run(
        "rollout", "status", "deployment/uns-auto-contextualizer", "--timeout=120s", timeout=130
    )
    await settle(cluster, "uns-auto-contextualizer", 1)
    cluster.run("apply", "-f", str(PROFILE / "hpa.yaml"))
    data["hpa"] = dict(samples=[])
    with phase("hpa-load"):
        task = asyncio.create_task(load(nc, context_jobs(hpa_seconds * hpa_rate, hpa_rate)))
        try:
            while not task.done():
                sample = hpa_sample(cluster)
                data["hpa"]["samples"].append(sample)
                print(
                    f"HPA ready={sample['ready']} desired={sample['desired']} cpu={sample['cpu_present']}",
                    flush=True,
                )
                save(output, data)
                await asyncio.sleep(5)
            data["hpa"]["rows"] = await task
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    for _ in range(40):
        sample = hpa_sample(cluster)
        data["hpa"]["cooled"] = sample
        save(output, data)
        if sample["ready"] == sample["desired"] == 1 and sample["cpu_present"]:
            break
        await asyncio.sleep(5)
    print("HPA checked=" + str(check_hpa(data["hpa"])), flush=True)


def evaluate(data: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = dict(
        local_mechanics="unknown", formal_scalability="not_established", checks=[]
    )
    try:
        if (
            data["errors"]
            or not data["restored"]
            or data["source_hashes"] != {p.relative_to(ROOT).as_posix(): digest(p) for p in SOURCES}
        ):
            return result
        checks = [
            dict(
                check="replica_sharing",
                passed=len(data["sharing"]) == 2
                and {s["replicas"] for s in data["sharing"]} == {1, 3}
                and all(check_sharing(s) for s in data["sharing"]),
            ),
            dict(
                check="cpu_hpa_scale_out_and_return",
                passed=check_hpa(data["hpa"])
                and all(r["status"] == "ok" for r in data["hpa"]["rows"]),
            ),
        ]
        comparisons = data["comparison"]
        good = len(comparisons) == 2 and {r["mode"] for r in comparisons} == {"shared", "separate"}
        resources = []
        for case in comparisons:
            rows = case["rows"]
            sent = {r["correlation_id"] for r in rows}
            expected = {j["body"]["correlation_id"] for j in data["comparison_workload"]}
            good = (
                good
                and sent == expected
                and len(rows) == len(expected)
                and all(r["status"] == "ok" for r in rows)
                and case["pods_before"] == case["pods_after"]
                and len(case["pods_before"]) == 1
                and {r.get("worker") for r in rows} == set(case["pods_before"])
            )
            for flow in ("data", "command", "event"):

                def total(samples: list[dict[str, Any]], flow: str = flow) -> float:
                    return sum(
                        float(s["value"][1]) for s in samples if s["metric"].get("flow") == flow
                    )

                good = good and total(case["after"]) - total(case["before"]) == sum(
                    r["flow"] == flow for r in rows
                )
            resources.append(
                case["deployment"]["spec"]["template"]["spec"]["containers"][0]["resources"]
            )
        good = good and resources[0] == resources[1]
        checks.append(dict(check="equal_budget_processing_comparison", passed=good))
        result.update(
            checks=checks, local_mechanics="pass" if all(c["passed"] for c in checks) else "fail"
        )
    except (KeyError, TypeError, ValueError, IndexError, OSError):
        pass
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True)
    parser.add_argument("--hpa-seconds", type=int, default=120)
    parser.add_argument("--hpa-rate", type=int, default=160)
    parser.add_argument("--reverse-order", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not 90 <= args.hpa_seconds <= 180:
        parser.error("HPA rehearsal duration must be 90..180 seconds")
    if not 1 <= args.hpa_rate <= 400:
        parser.error("local HPA offered rate must be 1..400 requests/second")
    cluster = Cluster(args.context, "eirvah-e1")
    namespace = json.loads(cluster.run("get", "namespace", "eirvah-e1", "-o", "json"))
    if namespace["metadata"].get("labels", {}).get("eirvah.uniza/purpose") != "e1-local-rehearsal":
        parser.error("dedicated E1 namespace ownership label required")
    if json.loads(cluster.run("get", "hpa", "-o", "json"))["items"]:
        parser.error("start the fixed-replica rehearsal with no HPA in the E1 namespace")
    output = args.output or ROOT / "results/e1-local" / datetime.now(UTC).strftime(
        "%Y-%m-%dT%H-%M-%S-%fZ"
    )
    output.mkdir(parents=True, exist_ok=False)
    data: dict[str, Any] = dict(
        mode="local_e1_mechanics",
        context=args.context,
        namespace="eirvah-e1",
        hpa_offered_rate=args.hpa_rate,
        hpa_duration_s=args.hpa_seconds,
        sharing=[],
        comparison=[],
        errors=[],
        restored=False,
        client_clock=dict(
            name="perf_counter", resolution_s=time.get_clock_info("perf_counter").resolution
        ),
        source_hashes={p.relative_to(ROOT).as_posix(): digest(p) for p in SOURCES},
    )
    for source in SOURCES:
        target = output / "sources" / source.relative_to(ROOT)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.read_bytes())
    save(output, data)
    try:
        with ExitStack() as stack:
            nats_port = stack.enter_context(port_forward(*cluster.transport("nats", 4222)))
            prom_port = stack.enter_context(port_forward(*cluster.transport("e1-prometheus", 9090)))

            async def run() -> None:
                nc = await nats.connect(f"nats://127.0.0.1:{nats_port}")
                try:
                    async with httpx.AsyncClient(
                        base_url=f"http://127.0.0.1:{prom_port}", timeout=10
                    ) as prom:
                        await capture(
                            cluster,
                            nc,
                            prom,
                            output,
                            data,
                            args.hpa_seconds,
                            args.hpa_rate,
                            args.reverse_order,
                        )
                finally:
                    await nc.close()

            with asyncio.Runner(loop_factory=asyncio.SelectorEventLoop) as runner:
                runner.run(run())
    except (Exception, KeyboardInterrupt) as exc:
        data["errors"].append(type(exc).__name__ + ": " + str(exc)[:250])
    finally:
        try:
            cluster.run("delete", "hpa", "uns-auto-contextualizer", "--ignore-not-found=true")
            cluster.run("scale", "deployment/uns-auto-contextualizer", "--replicas=1")
            cluster.run("set", "env", "deployment/e1-processing", "E1_MODE=shared")
            for app in ("uns-auto-contextualizer", "e1-processing"):
                cluster.run("rollout", "status", "deployment/" + app, "--timeout=120s", timeout=130)
            data["restored"] = (
                len(pods(cluster, "uns-auto-contextualizer"))
                == len(pods(cluster, "e1-processing"))
                == 1
            )
            data["final_deployments"] = json.loads(cluster.run("get", "deployments", "-o", "json"))
        except (OSError, subprocess.SubprocessError) as exc:
            data["errors"].append("restoration:" + type(exc).__name__)
        data["pod_inventory"] = json.loads(cluster.run("get", "pods", "-o", "json"))
        save(output, data)
    report = evaluate(data)
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(dict(**report, output=str(output), errors=data["errors"]), indent=2))
    raise SystemExit(0 if report["local_mechanics"] == "pass" else 1)


if __name__ == "__main__":
    main()
