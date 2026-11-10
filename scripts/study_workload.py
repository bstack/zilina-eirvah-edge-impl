"""Configurable fixed-budget workload rehearsal; no native device commands."""

from __future__ import annotations

import argparse
import asyncio
import json
from contextlib import ExitStack
from pathlib import Path
from typing import Any

import httpx
import nats
from e0_capture import Cluster
from e1_rehearsal import load, mixed_jobs, pods, settle, stats
from e1_rehearsal import processing_counters as counters
from eirvah_observability.experiment import phase
from scenario import port_forward


async def run(
    cluster: Cluster,
    nc: Any,
    prom: httpx.AsyncClient,
    config: dict[str, Any],
    data: dict[str, Any],
    output: Path,
) -> None:
    jobs = mixed_jobs(
        config["duration_s"], config["data_rate"], config["command_rate"], config["event_rate"]
    )
    data["schedule"] = [dict(offset=o, flow=f, payload=json.loads(b)) for o, f, b in jobs]
    for mode in config["order"]:
        cluster.run("set", "env", "deployment/e1-processing", "E1_MODE=" + mode)
        cluster.run("rollout", "restart", "deployment/e1-processing")
        cluster.run("rollout", "status", "deployment/e1-processing", "--timeout=120s", timeout=130)
        await settle(cluster, "e1-processing", 1)
        await load(nc, mixed_jobs(1, 10, 2, 2))
        workers = pods(cluster, "e1-processing")
        if len(workers) != 1:
            raise RuntimeError("expected one comparison worker")
        worker = next(iter(workers))
        warmup = {"data": 10, "command": 2, "event": 2}
        before = await counters(prom, worker, warmup)
        case = dict(
            mode=mode,
            pods_before=pods(cluster, "e1-processing"),
            before=before,
        )
        with phase("workload-" + mode):
            rows = await load(nc, jobs)
        after = await counters(
            prom, worker, {f: warmup[f] + sum(r["flow"] == f for r in rows) for f in warmup}
        )
        case.update(
            rows=rows,
            statistics=stats(rows),
            after=after,
            pods_after=pods(cluster, "e1-processing"),
        )
        good = (
            len(rows) == len(jobs)
            and all(r["status"] == "ok" for r in rows)
            and case["pods_before"] == case["pods_after"]
            and {r.get("worker") for r in rows} == set(case["pods_before"])
        )
        for flow in ("data", "command", "event"):
            before_total = sum(
                float(v["value"][1]) for v in case["before"] if v["metric"]["flow"] == flow
            )
            after_total = sum(
                float(v["value"][1]) for v in case["after"] if v["metric"]["flow"] == flow
            )
            good = good and after_total - before_total == sum(r["flow"] == flow for r in rows)
        case["passed"] = good
        data["cases"].append(case)
        (output / "evidence.json").write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
        print(mode + " " + json.dumps(case["statistics"]), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    config = json.loads(args.profile.read_text())
    if (
        not 5 <= config["duration_s"] <= 600
        or not all(1 <= config[k] <= 400 for k in ("data_rate", "command_rate", "event_rate"))
        or set(config["order"]) != {"shared", "separate"}
        or len(config["order"]) != 2
    ):
        parser.error("invalid selected workload profile")
    args.output.mkdir(parents=True, exist_ok=False)
    cluster = Cluster(args.context, "eirvah-e1")
    data: dict[str, Any] = dict(config=config, cases=[], errors=[], restored=False)
    try:
        with ExitStack() as stack:
            np = stack.enter_context(port_forward(*cluster.transport("nats", 4222)))
            pp = stack.enter_context(port_forward(*cluster.transport("e1-prometheus", 9090)))

            async def capture() -> None:
                nc = await nats.connect(f"nats://127.0.0.1:{np}")
                try:
                    async with httpx.AsyncClient(
                        base_url=f"http://127.0.0.1:{pp}", timeout=10
                    ) as prom:
                        await run(cluster, nc, prom, config, data, args.output)
                finally:
                    await nc.close()

            with asyncio.Runner(loop_factory=asyncio.SelectorEventLoop) as runner:
                runner.run(capture())
    except (Exception, KeyboardInterrupt) as exc:
        data["errors"].append(type(exc).__name__)
    finally:
        cluster.run("set", "env", "deployment/e1-processing", "E1_MODE=shared")
        cluster.run("rollout", "status", "deployment/e1-processing", "--timeout=120s", timeout=130)
        with asyncio.Runner(loop_factory=asyncio.SelectorEventLoop) as runner:
            runner.run(settle(cluster, "e1-processing", 1))
        data["restored"] = True
        (args.output / "evidence.json").write_text(
            json.dumps(data, indent=2) + "\n", encoding="utf-8"
        )
    status = (
        "pass"
        if len(data["cases"]) == 2
        and all(c["passed"] for c in data["cases"])
        and not data["errors"]
        and data["restored"]
        else "unknown"
        if data["errors"]
        else "fail"
    )
    (args.output / "report.json").write_text(
        json.dumps(dict(workload_case=status, formal_scalability="not_established"), indent=2)
        + "\n",
        encoding="utf-8",
    )
    raise SystemExit(0 if status == "pass" else 1)


if __name__ == "__main__":
    main()
