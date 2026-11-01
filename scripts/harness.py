"""Run experiments A/B/C and retain failures, coverage and observed-window statistics."""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import signal
import subprocess
import sys
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
from experiment_stats import Row, summarize


@dataclass
class HarnessConfig:
    experiment: str
    namespace: str = "eirvah-edge"
    prometheus_port: int = 9090
    nats_port: int = 4222
    opcua_port: int = 4840
    output_dir: Path = field(default_factory=lambda: Path("results"))
    duration: int | None = None
    rate: int = 500
    timeout: float = 5.0
    max_pending: int = 1000
    expect_hpa: bool = False
    dry_run: bool = False


def setup_output_dir(root: Path, experiment: str, run_ts: str) -> Path:
    out = root / f"experiment-{experiment}" / run_ts
    out.mkdir(parents=True, exist_ok=False)
    return out


def write_run_json(
    out_dir: Path,
    experiment: str,
    params: dict[str, Any],
    start_time: str,
    end_time: str,
    outcome: str,
    git_sha: str,
    details: dict[str, Any] | None = None,
) -> None:
    data = dict(
        experiment=experiment,
        git_sha=git_sha,
        params=params,
        start_time=start_time,
        end_time=end_time,
        outcome=outcome,
        details=details or {},
    )
    (out_dir / "run.json").write_text(json.dumps(data, indent=2, allow_nan=False), encoding="utf-8")


async def _sleep(seconds: float, dry_run: bool) -> None:
    if not dry_run:
        await asyncio.sleep(seconds)


def _current_git_sha() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


async def _stop_process(proc: asyncio.subprocess.Process) -> None:
    if proc.returncode is None:
        with suppress(ProcessLookupError):
            proc.terminate()
    try:
        await asyncio.wait_for(proc.wait(), timeout=5)
    except TimeoutError:
        proc.kill()
        await proc.wait()


@asynccontextmanager
async def port_forward(
    resource: str, local_port: int, remote_port: int, namespace: str, dry_run: bool = False
) -> AsyncGenerator[None, None]:
    cmd = ["kubectl", "port-forward", "-n", namespace, resource, f"{local_port}:{remote_port}"]
    if dry_run:
        print(f"[dry-run] {' '.join(cmd)}")
        yield
        return
    proc = None
    for attempt in range(2):
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL
        )
        await asyncio.sleep(0.5)
        if proc.returncode is None:
            break
        if attempt == 0:
            await asyncio.sleep(2)
    if proc is None or proc.returncode is not None:
        raise RuntimeError(f"port-forward failed: {resource} {local_port}:{remote_port}")
    try:
        yield
    finally:
        await _stop_process(proc)


class Scraper:
    def __init__(
        self,
        prometheus_url: str,
        queries: dict[str, str],
        interval: float = 10.0,
        optional_queries: set[str] | None = None,
    ) -> None:
        self._url, self._queries, self._interval = prometheus_url, queries, interval
        self._optional = optional_queries or set()
        self.rows: list[Row] = []
        self.issues: list[dict[str, Any]] = []
        self._seen: dict[str, set[str]] = {}
        self._polls = 0
        self._last_poll: float | None = None

    def _issue(self, name: str, reason: str) -> None:
        self.issues.append(
            dict(at=time.time(), metric=name, reason=reason, required=name not in self._optional)
        )
        print(f"[metrics] {name}: {reason}", file=sys.stderr)

    async def check_connectivity(self) -> None:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self._url}/-/healthy", timeout=5)
            response.raise_for_status()

    async def run(self, stop: asyncio.Event) -> None:
        async with httpx.AsyncClient() as client:
            while True:
                await self._poll_once(client)
                if stop.is_set():
                    break
                with suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=self._interval)

    async def _poll_once(self, client: httpx.AsyncClient) -> None:
        now = time.time()
        if self._last_poll is not None and now - self._last_poll > self._interval * 2 + 5:
            for name in self._queries:
                self._issue(name, f"collection gap of {now - self._last_poll:.1f}s")
        self._last_poll = now
        self._polls += 1
        for name, expression in self._queries.items():
            try:
                response = await client.get(
                    f"{self._url}/api/v1/query", params={"query": expression}, timeout=5
                )
                response.raise_for_status()
                data = response.json()
                if data.get("status") != "success":
                    raise ValueError(f"Prometheus query failed: {data.get('error', data)}")
                if data.get("warnings"):
                    self._issue(name, f"Prometheus warnings: {data['warnings']}")
                if data["data"]["resultType"] != "vector":
                    raise ValueError("expected instant vector")
                values = data["data"]["result"]
                if not values:
                    self._issue(name, "no series returned")
                seen: set[str] = set()
                for series in values:
                    tags = {
                        key: value for key, value in series["metric"].items() if key != "__name__"
                    }
                    labels = json.dumps(tags, sort_keys=True)
                    timestamp, value = map(float, series["value"])
                    if not math.isfinite(value) or not math.isfinite(timestamp):
                        raise ValueError("non-finite sample")
                    age = time.time() - timestamp
                    if age > max(15.0, self._interval * 2) or age < -5:
                        self._issue(name, f"stale sample or clock discrepancy: age={age:.1f}s")
                    self.rows.append((timestamp, name, labels, value))
                    seen.add(labels)
                missing = self._seen.get(name, set()) - seen
                if missing:
                    self._issue(name, f"previously observed series missing: {sorted(missing)}")
                self._seen[name] = seen
            except Exception as exc:
                self._issue(name, f"{type(exc).__name__}: {exc}")

    def flush(self, out_dir: Path) -> None:
        out_dir.mkdir(parents=True, exist_ok=True)
        table = pa.table(
            {
                "timestamp": pa.array([row[0] for row in self.rows], type=pa.float64()),
                "metric": pa.array([row[1] for row in self.rows], type=pa.string()),
                "labels": pa.array([row[2] for row in self.rows], type=pa.string()),
                "value": pa.array([row[3] for row in self.rows], type=pa.float64()),
            }
        )
        # Rewrite the retained snapshot: repeated cleanup must not duplicate samples.
        pq.write_table(table, out_dir / "raw.parquet")
        summaries = summarize(self.rows)
        columns = [
            "metric",
            "labels",
            "kind",
            "samples",
            "window_start",
            "window_end",
            "elapsed_seconds",
            "increase",
            "rate_per_second",
            "sample_mean",
            "minimum",
            "maximum",
            "observations",
            "mean_seconds",
            "p50_seconds",
            "p95_seconds",
            "p99_seconds",
            "limitation",
        ]
        with (out_dir / "summary.csv").open("w", newline="", encoding="utf-8") as output:
            writer = csv.DictWriter(output, fieldnames=columns)
            writer.writeheader()
            writer.writerows(summaries)
        missing = sorted(
            name
            for name in self._queries
            if not self._seen.get(name) and name not in self._optional
        )
        complete = (
            self._polls > 0 and not missing and not any(issue["required"] for issue in self.issues)
        )
        report = dict(
            complete=complete,
            polls=self._polls,
            queries=self._queries,
            optional_queries=sorted(self._optional),
            missing_queries=missing,
            issues=self.issues,
            summary_scope="per-label observed sample windows, including baseline and recovery; "
            "not automatically the active workload window or whole-run totals",
        )
        (out_dir / "collection.json").write_text(json.dumps(report, indent=2), encoding="utf-8")


@asynccontextmanager
async def _collect(scraper: Scraper, out_dir: Path) -> AsyncGenerator[None, None]:
    stop = asyncio.Event()
    task = None
    try:
        await scraper.check_connectivity()
        task = asyncio.create_task(scraper.run(stop))
        yield
    finally:
        stop.set()
        try:
            if task is not None:
                await task
        finally:
            scraper.flush(out_dir)


async def _run_child(
    cmd: list[str], out_dir: Path, name: str, *, timeout: float, planned_stop: bool = False
) -> dict[str, Any]:
    start = time.time()
    expired = False
    stopped = False
    with (
        (out_dir / f"{name}.stdout.log").open("wb") as stdout,
        (out_dir / f"{name}.stderr.log").open("wb") as stderr,
    ):
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=stdout, stderr=stderr)
        try:
            await asyncio.wait_for(proc.wait(), timeout=timeout)
        except TimeoutError:
            stopped, expired = planned_stop, not planned_stop
        finally:
            await _stop_process(proc)
            details = dict(
                command=cmd,
                exit_code=proc.returncode,
                timed_out=expired,
                stopped_by_harness=stopped,
                started_at=start,
                ended_at=time.time(),
                stdout=f"{name}.stdout.log",
                stderr=f"{name}.stderr.log",
            )
            (out_dir / f"{name}.process.json").write_text(
                json.dumps(details, indent=2), encoding="utf-8"
            )
    return details


def _result(
    out_dir: Path,
    processes: dict[str, Any],
    *,
    require_generator: bool = False,
    recovery_timed_out: bool = False,
) -> dict[str, Any]:
    failures = []
    for name, process in processes.items():
        if process["timed_out"] or (
            process["exit_code"] != 0 and not process.get("stopped_by_harness")
        ):
            failures.append(
                f"{name} failed: exit={process['exit_code']}, timeout={process['timed_out']}"
            )
        if name == "disturbance" and not process.get("stopped_by_harness"):
            failures.append("disturbance ended before the requested experiment duration")
    generator = None
    if require_generator:
        try:
            generator = json.loads((out_dir / "generator.json").read_text(encoding="utf-8"))
            counts = [generator[key] for key in ("sent", "succeeded", "errors", "unresolved")]
            if any(type(count) is not int or count < 0 for count in counts):
                raise ValueError("counts must be non-negative integers")
            if counts[0] == 0 or counts[0] != sum(counts[1:]):
                raise ValueError("generator counts do not reconcile or no work was sent")
            if counts[2] or counts[3]:
                failures.append("generator reported errors or unresolved requests")
            for key in ("send_rate_per_second", "completion_rate_per_second"):
                if not math.isfinite(generator[key]) or generator[key] < 0:
                    raise ValueError(f"invalid {key}")
        except (OSError, ValueError, KeyError, TypeError) as exc:
            failures.append(f"missing or invalid generator summary: {exc}")
    if recovery_timed_out:
        failures.append("pod recovery timed out")
    collection = json.loads((out_dir / "collection.json").read_text(encoding="utf-8"))
    return dict(
        outcome="failed" if failures else "ok" if collection["complete"] else "incomplete",
        failures=failures,
        processes=processes,
        generator=generator,
        recovery_timed_out=recovery_timed_out,
        measurement_complete=collection["complete"],
    )


def _latency_queries(prefix: str, metric: str) -> dict[str, str]:
    return {f"{prefix}_{suffix}": f"{metric}_{suffix}" for suffix in ("bucket", "sum", "count")}


_QUERIES_A = {
    **_latency_queries("pipeline_e2e_latency", "eirvah_pipeline_e2e_latency_seconds"),
    **_latency_queries("actuation_e2e_latency", "eirvah_actuation_e2e_latency_seconds"),
    "pipeline_success_total": "eirvah_pipeline_success_total",
    "actuation_approved_total": "eirvah_actuation_approved_total",
    "temperature_celsius": "eirvah_simulator_temperature_celsius",
    "setpoint_writes_total": "eirvah_simulator_setpoint_writes_total",
}
_QUERIES_B = {
    **_latency_queries("pipeline_e2e_latency", "eirvah_pipeline_e2e_latency_seconds"),
    "pipeline_success_total": "eirvah_pipeline_success_total",
    "stage_timeout_total": "eirvah_pipeline_stage_timeout_total",
}


async def run_experiment_a(cfg: HarnessConfig, out_dir: Path) -> dict[str, Any]:
    cmd = [
        sys.executable,
        "scripts/disturbance.py",
        "--interval",
        "120",
        "--endpoint",
        f"opc.tcp://localhost:{cfg.opcua_port}/eirvah/simulator",
    ]
    async with (
        port_forward("svc/prometheus", cfg.prometheus_port, 9090, cfg.namespace, cfg.dry_run),
        port_forward("svc/opcua-simulator", cfg.opcua_port, 4840, cfg.namespace, cfg.dry_run),
    ):
        if cfg.dry_run:
            print(f"[dry-run] {' '.join(cmd)}")
            return {"outcome": "dry_run"}
        scraper = Scraper(f"http://localhost:{cfg.prometheus_port}", _QUERIES_A)
        async with _collect(scraper, out_dir):
            await _sleep(30, False)
            process = await _run_child(
                cmd, out_dir, "disturbance", timeout=cfg.duration or 360, planned_stop=True
            )
            await _sleep(60, False)
    return _result(out_dir, {"disturbance": process})


async def _wait_for_pod_running(label: str, namespace: str, timeout: int = 120) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        proc = await asyncio.create_subprocess_exec(
            "kubectl",
            "get",
            "pod",
            "-n",
            namespace,
            "-l",
            label,
            "-o",
            "jsonpath={.items[0].status.phase}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"pod status query failed: {stderr.decode(errors='replace')}")
        if stdout.decode().strip() == "Running":
            return True
        await asyncio.sleep(2)
    return False


async def _delete_pod_and_wait(
    label: str, namespace: str, recovery_timeout: int, dry_run: bool
) -> bool:
    cmd = ["kubectl", "-n", namespace, "delete", "pod", "-l", label]
    if dry_run:
        print(f"[dry-run] {' '.join(cmd)}")
        return True
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"pod deletion failed: {stderr.decode(errors='replace')}")
    return await _wait_for_pod_running(label, namespace, timeout=recovery_timeout)


async def run_experiment_b(cfg: HarnessConfig, out_dir: Path) -> dict[str, Any]:
    labels = [
        "app.kubernetes.io/name=data-converter",
        "app.kubernetes.io/name=opcua-data-subscriber",
    ]
    async with port_forward(
        "svc/prometheus", cfg.prometheus_port, 9090, cfg.namespace, cfg.dry_run
    ):
        if cfg.dry_run:
            for label in labels:
                await _delete_pod_and_wait(label, cfg.namespace, cfg.duration or 120, True)
            return {"outcome": "dry_run"}
        scraper = Scraper(
            f"http://localhost:{cfg.prometheus_port}",
            _QUERIES_B,
            optional_queries={"stage_timeout_total"},
        )
        timed_out = False
        async with _collect(scraper, out_dir):
            await _sleep(30, False)
            for label in labels:
                recovered = await _delete_pod_and_wait(
                    label, cfg.namespace, cfg.duration or 120, False
                )
                timed_out |= not recovered
                await _sleep(60, False)
    return _result(out_dir, {}, recovery_timed_out=timed_out)


async def run_experiment_c(cfg: HarnessConfig, out_dir: Path) -> dict[str, Any]:
    cmd = [
        sys.executable,
        "scripts/load_inject.py",
        "--rate",
        str(cfg.rate),
        "--duration",
        str(cfg.duration or 120),
        "--nats-url",
        f"nats://localhost:{cfg.nats_port}",
        "--timeout",
        str(cfg.timeout),
        "--max-pending",
        str(cfg.max_pending),
        "--summary-json",
        str(out_dir.resolve() / "generator.json"),
    ]
    async with (
        port_forward("svc/prometheus", cfg.prometheus_port, 9090, cfg.namespace, cfg.dry_run),
        port_forward("svc/nats", cfg.nats_port, 4222, cfg.namespace, cfg.dry_run),
    ):
        if cfg.dry_run:
            print(f"[dry-run] {' '.join(cmd)}")
            return {"outcome": "dry_run"}
        selector = f'namespace="{cfg.namespace}",horizontalpodautoscaler="uns-auto-contextualizer"'
        queries = {
            "worker_handler_total": 'eirvah_worker_handler_total{worker="uns-auto-contextualizer"}',
            "deployment_available_replicas": "kube_deployment_status_replicas_available{"
            f'namespace="{cfg.namespace}",deployment="uns-auto-contextualizer"' + "}",
            "hpa_replicas": f"kube_horizontalpodautoscaler_status_current_replicas{{{selector}}}",
            "hpa_desired_replicas": f"kube_horizontalpodautoscaler_status_desired_replicas{{{selector}}}",
        }
        scraper = Scraper(
            f"http://localhost:{cfg.prometheus_port}",
            queries,
            optional_queries=set() if cfg.expect_hpa else {"hpa_replicas", "hpa_desired_replicas"},
        )
        async with _collect(scraper, out_dir):
            await _sleep(30, False)
            process = await _run_child(
                cmd, out_dir, "generator", timeout=(cfg.duration or 120) + cfg.timeout + 15
            )
            await _sleep(90, False)
    return _result(out_dir, {"generator": process}, require_generator=True)


def parse_args(argv: list[str] | None = None) -> HarnessConfig:
    parser = argparse.ArgumentParser(description="EirVah experiment harness")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run")
    run.add_argument("--experiment", required=True, choices=["a", "b", "c"])
    run.add_argument("--namespace", default="eirvah-edge")
    for name, default in (("prometheus", 9090), ("nats", 4222), ("opcua", 4840)):
        run.add_argument(f"--{name}-port", type=int, default=default)
    run.add_argument("--output-dir", type=Path, default=Path("results"))
    run.add_argument("--duration", type=int)
    run.add_argument("--rate", type=int, default=500)
    run.add_argument("--timeout", type=float, default=5.0)
    run.add_argument("--max-pending", type=int, default=1000)
    run.add_argument("--expect-hpa", action="store_true")
    run.add_argument("--dry-run", action="store_true")
    args = vars(parser.parse_args(argv))
    args.pop("command")
    if (
        args["rate"] <= 0
        or args["timeout"] <= 0
        or args["max_pending"] <= 0
        or (args["duration"] is not None and args["duration"] <= 0)
    ):
        parser.error("rate, duration, timeout and max-pending must be positive")
    return HarnessConfig(**args)


_EXPERIMENTS = {"a": run_experiment_a, "b": run_experiment_b, "c": run_experiment_c}


async def _run(cfg: HarnessConfig) -> str:
    if cfg.dry_run:
        await _EXPERIMENTS[cfg.experiment](cfg, cfg.output_dir / "dry-run")
        return "dry_run"
    run_ts = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%S-%f")
    out_dir = setup_output_dir(cfg.output_dir, cfg.experiment, run_ts)
    params = asdict(cfg)
    params["output_dir"] = str(cfg.output_dir)
    start, revision = datetime.now(UTC).isoformat(), _current_git_sha()
    details: dict[str, Any] = {}
    outcome = "interrupted"
    print(f"[harness] experiment={cfg.experiment} out={out_dir}")
    try:
        details = await _EXPERIMENTS[cfg.experiment](cfg, out_dir)
        outcome = details["outcome"]
    except asyncio.CancelledError:
        details = {"error": "run interrupted; retained evidence may be partial"}
        raise
    except Exception as exc:
        outcome = "failed"
        details = {"error": f"{type(exc).__name__}: {exc}"}
    finally:
        write_run_json(
            out_dir,
            cfg.experiment,
            params,
            start,
            datetime.now(UTC).isoformat(),
            outcome,
            revision,
            details,
        )
    print(f"[harness] done outcome={outcome} out={out_dir}")
    return outcome


def main() -> None:
    cfg = parse_args()

    # Let asyncio cancel the running experiment so cleanup can retain partial evidence.
    def terminate(signum: int, frame: object) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, terminate)
    try:
        outcome = asyncio.run(_run(cfg))
    except KeyboardInterrupt:
        sys.exit(130)
    if outcome not in {"ok", "dry_run"}:
        sys.exit(1)


if __name__ == "__main__":
    main()
