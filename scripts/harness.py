"""EirVah experiment harness — unified CLI for Experiments A, B, C."""
from __future__ import annotations

import argparse
import asyncio
import atexit
import json
import signal
import subprocess
import sys
import time
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


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
    dry_run: bool = False


def setup_output_dir(root: Path, experiment: str, run_ts: str) -> Path:
    out = root / f"experiment-{experiment}" / run_ts
    out.mkdir(parents=True, exist_ok=True)
    return out


def write_run_json(
    out_dir: Path,
    experiment: str,
    params: dict[str, Any],
    start_time: str,
    end_time: str,
    outcome: str,
    git_sha: str,
) -> None:
    data = {
        "experiment": experiment,
        "git_sha": git_sha,
        "params": params,
        "start_time": start_time,
        "end_time": end_time,
        "outcome": outcome,
    }
    (out_dir / "run.json").write_text(json.dumps(data, indent=2))


def _current_git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], text=True
        ).strip()
    except Exception:
        return "unknown"


@asynccontextmanager
async def port_forward(
    resource: str,
    local_port: int,
    remote_port: int,
    namespace: str,
    dry_run: bool = False,
) -> AsyncGenerator[None, None]:
    cmd = [
        "kubectl", "port-forward",
        "-n", namespace,
        resource,
        f"{local_port}:{remote_port}",
    ]
    if dry_run:
        print(f"[dry-run] {' '.join(cmd)}")
        yield
        return

    proc: asyncio.subprocess.Process | None = None
    for attempt in range(2):
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.sleep(0.5)
        if proc.returncode is None:
            break
        if attempt == 0:
            await asyncio.sleep(2.0)

    if proc is None or proc.returncode is not None:
        raise RuntimeError(
            f"port-forward failed after 2 attempts: {resource} {local_port}:{remote_port}"
        )

    try:
        yield
    finally:
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            proc.kill()


class Scraper:
    def __init__(
        self,
        prometheus_url: str,
        queries: dict[str, str],
        interval: float = 10.0,
    ) -> None:
        self._url = prometheus_url
        self._queries = queries
        self._interval = interval
        self.rows: list[tuple[float, str, str, float]] = []

    async def check_connectivity(self) -> None:
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(f"{self._url}/-/healthy", timeout=5.0)
                resp.raise_for_status()
            except Exception as exc:
                raise SystemExit(f"Prometheus unreachable at {self._url}: {exc}") from exc

    async def run(self, stop: asyncio.Event) -> None:
        async with httpx.AsyncClient() as client:
            while True:
                await self._poll_once(client)
                if stop.is_set():
                    break
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self._interval)
                except asyncio.TimeoutError:
                    pass

    async def _poll_once(self, client: httpx.AsyncClient) -> None:
        ts = time.time()
        for name, expr in self._queries.items():
            try:
                resp = await client.get(
                    f"{self._url}/api/v1/query",
                    params={"query": expr},
                    timeout=5.0,
                )
                resp.raise_for_status()
                data = resp.json()
                for series in data["data"]["result"]:
                    labels = {
                        k: v
                        for k, v in series["metric"].items()
                        if k != "__name__"
                    }
                    value = float(series["value"][1])
                    self.rows.append(
                        (ts, name, json.dumps(labels, sort_keys=True), value)
                    )
            except Exception:
                pass

    def flush(self, out_dir: Path) -> None:
        if not self.rows:
            return
        out_dir.mkdir(parents=True, exist_ok=True)

        table = pa.table({
            "timestamp": pa.array([r[0] for r in self.rows], type=pa.float64()),
            "metric": pa.array([r[1] for r in self.rows], type=pa.string()),
            "labels": pa.array([r[2] for r in self.rows], type=pa.string()),
            "value": pa.array([r[3] for r in self.rows], type=pa.float64()),
        })

        parquet_path = out_dir / "raw.parquet"
        if parquet_path.exists():
            existing = pq.read_table(parquet_path)
            table = pa.concat_tables([existing, table])
        pq.write_table(table, parquet_path)

        df = table.to_pandas()
        summary = (
            df.groupby(["metric", "labels"])["value"]
            .agg(
                mean="mean",
                p50=lambda x: x.quantile(0.50),
                p95=lambda x: x.quantile(0.95),
                p99=lambda x: x.quantile(0.99),
                min="min",
                max="max",
            )
            .reset_index()
        )
        summary.to_csv(out_dir / "summary.csv", index=False)
