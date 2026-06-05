# Experiment Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `scripts/harness.py` — a unified CLI that orchestrates Experiments A/B/C, scrapes Prometheus metrics throughout each run, and writes raw time-series (Parquet) + summary stats (CSV) + run metadata (JSON) to a timestamped output directory.

**Architecture:** Single Python file with five logical sections: CLI (argparse), PortForward (asynccontextmanager), Scraper (httpx polling → pyarrow flush), per-experiment async functions, and output helpers. Tests live in `tests/scripts/test_harness.py` and mock subprocesses + HTTP so no live cluster is required.

**Tech Stack:** Python 3.12, asyncio, httpx, pyarrow, pandas, pytest-asyncio (auto mode, already configured), unittest.mock.

**Spec:** `docs/superpowers/specs/2026-06-04-experiment-harness-design.md`

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Create | `scripts/harness.py` | Entire harness — CLI, PortForward, Scraper, experiments, output |
| Create | `tests/scripts/__init__.py` | Makes `tests/scripts` a package |
| Create | `tests/scripts/test_harness.py` | All unit tests |
| Modify | `pyproject.toml` | Add `pyarrow`, `pandas` to dev deps |
| Modify | `pytest.ini` | Add `pythonpath = scripts` so `import harness` works in tests |

---

## Task 1: Dev dependencies + pytest path config

**Files:**
- Modify: `pyproject.toml`
- Modify: `pytest.ini`
- Create: `tests/scripts/__init__.py`

- [ ] **Step 1: Add pyarrow and pandas to dev group in pyproject.toml**

In `pyproject.toml`, add to the end of the `dev` list (after `"aiomqtt>=2.0",`):

```toml
    "pyarrow>=16.0",
    "pandas>=2.2",
```

The full `[dependency-groups]` block should end:
```toml
    "aiomqtt>=2.0",
    "pyarrow>=16.0",
    "pandas>=2.2",
]
```

- [ ] **Step 2: Add pythonpath to pytest.ini**

Add `pythonpath = scripts` to `pytest.ini` so `import harness` resolves to `scripts/harness.py`:

```ini
[pytest]
testpaths = libs services tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
asyncio_mode = auto
pythonpath = scripts
filterwarnings =
    error
    ignore::DeprecationWarning:asyncua.*
    ignore::DeprecationWarning:rdflib.*
```

- [ ] **Step 3: Create tests/scripts/__init__.py**

Create an empty file at `tests/scripts/__init__.py`.

- [ ] **Step 4: Sync deps and verify imports**

```bash
uv sync
uv run python -c "import pyarrow, pandas, httpx; print('ok')"
```

Expected: `ok`

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml pytest.ini tests/scripts/__init__.py
git commit -m "chore: add pyarrow, pandas dev deps; configure pytest pythonpath for scripts"
```

---

## Task 2: HarnessConfig dataclass + output directory helpers

**Files:**
- Create: `scripts/harness.py` (initial skeleton)
- Modify: `tests/scripts/test_harness.py` (first tests)

- [ ] **Step 1: Write failing tests for output helpers**

Create `tests/scripts/test_harness.py`:

```python
"""Unit tests for the experiment harness."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pytest


def test_setup_output_dir_creates_timestamped_path(tmp_path: Path) -> None:
    from harness import setup_output_dir

    out = setup_output_dir(root=tmp_path, experiment="a", run_ts="2026-06-05T10-00-00")
    assert out == tmp_path / "experiment-a" / "2026-06-05T10-00-00"
    assert out.exists()


def test_write_run_json(tmp_path: Path) -> None:
    from harness import write_run_json

    write_run_json(
        out_dir=tmp_path,
        experiment="a",
        params={"duration": 360},
        start_time="2026-06-05T10:00:00Z",
        end_time="2026-06-05T10:10:00Z",
        outcome="ok",
        git_sha="abc1234",
    )
    data = json.loads((tmp_path / "run.json").read_text())
    assert data["experiment"] == "a"
    assert data["outcome"] == "ok"
    assert data["git_sha"] == "abc1234"
    assert data["params"] == {"duration": 360}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/scripts/test_harness.py -v
```

Expected: `ImportError` or `ModuleNotFoundError: No module named 'harness'`

- [ ] **Step 3: Create scripts/harness.py with config dataclass and output helpers**

```python
"""EirVah experiment harness — unified CLI for Experiments A, B, C."""
from __future__ import annotations

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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/scripts/test_harness.py -v
```

Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add scripts/harness.py tests/scripts/test_harness.py
git commit -m "feat(harness): HarnessConfig dataclass and output helpers"
```

---

## Task 3: PortForward async context manager

**Files:**
- Modify: `scripts/harness.py`
- Modify: `tests/scripts/test_harness.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/scripts/test_harness.py`:

```python
from unittest.mock import AsyncMock, MagicMock, patch


async def test_port_forward_spawns_and_terminates_process() -> None:
    from harness import port_forward

    mock_proc = MagicMock()
    mock_proc.returncode = None
    mock_proc.terminate = MagicMock()
    mock_proc.wait = AsyncMock()

    with patch("harness.asyncio.create_subprocess_exec", return_value=mock_proc) as mock_exec:
        async with port_forward("svc/prometheus", local_port=9090, remote_port=9090, namespace="eirvah-edge"):
            pass

    mock_exec.assert_called_once()
    call_args = mock_exec.call_args[0]
    assert "kubectl" in call_args
    assert "port-forward" in call_args
    assert "9090:9090" in call_args
    mock_proc.terminate.assert_called_once()
    mock_proc.wait.assert_called_once()


async def test_port_forward_dry_run_does_not_spawn() -> None:
    from harness import port_forward

    with patch("harness.asyncio.create_subprocess_exec") as mock_exec:
        async with port_forward("svc/prometheus", local_port=9090, remote_port=9090, namespace="eirvah-edge", dry_run=True):
            pass

    mock_exec.assert_not_called()


async def test_port_forward_retries_on_immediate_exit() -> None:
    from harness import port_forward

    dead_proc = MagicMock()
    dead_proc.returncode = 1
    dead_proc.terminate = MagicMock()
    dead_proc.wait = AsyncMock()

    alive_proc = MagicMock()
    alive_proc.returncode = None
    alive_proc.terminate = MagicMock()
    alive_proc.wait = AsyncMock()

    with patch("harness.asyncio.create_subprocess_exec", side_effect=[dead_proc, alive_proc]):
        with patch("harness.asyncio.sleep"):
            async with port_forward("svc/prometheus", local_port=9090, remote_port=9090, namespace="eirvah-edge"):
                pass

    alive_proc.terminate.assert_called_once()
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/scripts/test_harness.py::test_port_forward_spawns_and_terminates_process -v
```

Expected: `ImportError` (function not yet defined)

- [ ] **Step 3: Add port_forward to harness.py**

Append to `scripts/harness.py` after `_current_git_sha`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/scripts/test_harness.py -k "port_forward" -v
```

Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add scripts/harness.py tests/scripts/test_harness.py
git commit -m "feat(harness): PortForward async context manager with retry"
```

---

## Task 4: Scraper — poll Prometheus and accumulate rows

**Files:**
- Modify: `scripts/harness.py`
- Modify: `tests/scripts/test_harness.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/scripts/test_harness.py`:

```python
async def test_scraper_accumulates_rows_from_prometheus() -> None:
    from harness import Scraper

    prometheus_response = {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {
                    "metric": {"__name__": "pipeline_success_total", "job": "orchestrator"},
                    "value": [1717584000.0, "42.0"],
                }
            ],
        },
    }

    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = MagicMock(return_value=prometheus_response)

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    scraper = Scraper(
        prometheus_url="http://localhost:9090",
        queries={"pipeline_success_total": "eirvah_pipeline_success_total"},
        interval=10.0,
    )

    with patch("harness.httpx.AsyncClient", return_value=mock_client):
        stop = asyncio.Event()
        stop.set()  # stop immediately after first poll
        await scraper.run(stop)

    assert len(scraper.rows) == 1
    ts, metric, labels, value = scraper.rows[0]
    assert metric == "pipeline_success_total"
    assert value == 42.0
    assert '"job": "orchestrator"' in labels


async def test_scraper_check_prometheus_raises_on_failure() -> None:
    from harness import Scraper

    scraper = Scraper(
        prometheus_url="http://localhost:9090",
        queries={},
        interval=10.0,
    )

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(side_effect=httpx.ConnectError("refused"))
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("harness.httpx.AsyncClient", return_value=mock_client):
        with pytest.raises(SystemExit):
            await scraper.check_connectivity()
```

- [ ] **Step 2: Run to verify they fail**

```bash
uv run pytest tests/scripts/test_harness.py -k "scraper" -v
```

Expected: `ImportError` (Scraper not defined)

- [ ] **Step 3: Add Scraper class to harness.py**

Append to `scripts/harness.py`:

```python
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
            while not stop.is_set():
                await self._poll_once(client)
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/scripts/test_harness.py -k "scraper" -v
```

Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add scripts/harness.py tests/scripts/test_harness.py
git commit -m "feat(harness): Scraper class — Prometheus polling and row accumulation"
```

---

## Task 5: Scraper.flush — Parquet + CSV output

**Files:**
- Modify: `scripts/harness.py`
- Modify: `tests/scripts/test_harness.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/scripts/test_harness.py`:

```python
def test_scraper_flush_writes_parquet_and_csv(tmp_path: Path) -> None:
    from harness import Scraper

    scraper = Scraper(
        prometheus_url="http://localhost:9090",
        queries={},
        interval=10.0,
    )
    scraper.rows = [
        (1717584000.0, "pipeline_success_total", '{"job": "orch"}', 10.0),
        (1717584010.0, "pipeline_success_total", '{"job": "orch"}', 12.0),
        (1717584020.0, "pipeline_success_total", '{"job": "orch"}', 14.0),
    ]

    scraper.flush(tmp_path)

    parquet_path = tmp_path / "raw.parquet"
    assert parquet_path.exists()

    csv_path = tmp_path / "summary.csv"
    assert csv_path.exists()

    table = pq.read_table(parquet_path)
    assert table.schema.field("timestamp").type == pa.float64()
    assert table.schema.field("metric").type == pa.string()
    assert table.schema.field("labels").type == pa.string()
    assert table.schema.field("value").type == pa.float64()
    assert table.num_rows == 3

    import pandas as pd
    df = pd.read_csv(csv_path)
    assert list(df.columns) == ["metric", "labels", "mean", "p50", "p95", "p99", "min", "max"]
    assert len(df) == 1
    assert abs(df.iloc[0]["mean"] - 12.0) < 0.01
    assert abs(df.iloc[0]["min"] - 10.0) < 0.01
    assert abs(df.iloc[0]["max"] - 14.0) < 0.01


def test_scraper_flush_is_noop_when_no_rows(tmp_path: Path) -> None:
    from harness import Scraper

    scraper = Scraper(prometheus_url="http://localhost:9090", queries={}, interval=10.0)
    scraper.flush(tmp_path)

    assert not (tmp_path / "raw.parquet").exists()
    assert not (tmp_path / "summary.csv").exists()
```

- [ ] **Step 2: Run to verify they fail**

```bash
uv run pytest tests/scripts/test_harness.py -k "flush" -v
```

Expected: `AttributeError: 'Scraper' object has no attribute 'flush'`

- [ ] **Step 3: Add flush method to Scraper in harness.py**

Add this method inside the `Scraper` class (after `_poll_once`):

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/scripts/test_harness.py -k "flush" -v
```

Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add scripts/harness.py tests/scripts/test_harness.py
git commit -m "feat(harness): Scraper.flush — Parquet + CSV output"
```

---

## Task 6: Experiment A

**Files:**
- Modify: `scripts/harness.py`
- Modify: `tests/scripts/test_harness.py`

- [ ] **Step 1: Write failing test**

Append to `tests/scripts/test_harness.py`:

```python
async def test_run_experiment_a_calls_disturbance_and_scrapes(tmp_path: Path) -> None:
    from harness import HarnessConfig, run_experiment_a

    cfg = HarnessConfig(experiment="a", dry_run=True, output_dir=tmp_path)

    mock_scraper = MagicMock()
    mock_scraper.check_connectivity = AsyncMock()
    mock_scraper.run = AsyncMock()
    mock_scraper.flush = MagicMock()
    mock_scraper.rows = []

    mock_proc = MagicMock()
    mock_proc.returncode = None
    mock_proc.terminate = MagicMock()
    mock_proc.wait = AsyncMock()

    out_dir = tmp_path / "experiment-a" / "run1"
    out_dir.mkdir(parents=True)

    with patch("harness.asyncio.create_subprocess_exec", return_value=mock_proc):
        with patch("harness.asyncio.sleep"):
            with patch("harness.Scraper", return_value=mock_scraper):
                result = await run_experiment_a(cfg, out_dir)

    assert result["outcome"] == "ok"
    mock_scraper.check_connectivity.assert_called_once()
    mock_scraper.flush.assert_called_once_with(out_dir)
```

- [ ] **Step 2: Run to verify it fails**

```bash
uv run pytest tests/scripts/test_harness.py::test_run_experiment_a_calls_disturbance_and_scrapes -v
```

Expected: `ImportError` (run_experiment_a not defined)

- [ ] **Step 3: Add run_experiment_a to harness.py**

Append to `scripts/harness.py`:

```python
_QUERIES_A: dict[str, str] = {
    "pipeline_duration_sum": "eirvah_pipeline_duration_seconds_sum",
    "pipeline_duration_count": "eirvah_pipeline_duration_seconds_count",
    "pipeline_success_total": "eirvah_pipeline_success_total",
    "actuation_requests_total": "eirvah_actuation_requests_total",
}


async def run_experiment_a(cfg: HarnessConfig, out_dir: Path) -> dict[str, Any]:
    scraper = Scraper(
        prometheus_url=f"http://localhost:{cfg.prometheus_port}",
        queries=_QUERIES_A,
    )

    async with port_forward("svc/prometheus", cfg.prometheus_port, 9090, cfg.namespace, cfg.dry_run):
        async with port_forward("svc/opcua-simulator", cfg.opcua_port, 4840, cfg.namespace, cfg.dry_run):
            await scraper.check_connectivity()

            stop = asyncio.Event()
            scraper_task = asyncio.create_task(scraper.run(stop))

            # baseline
            await asyncio.sleep(30)

            # disturbance subprocess
            duration = cfg.duration or 360
            dist_cmd = [
                sys.executable, "-m", "uv", "run", "python", "scripts/disturbance.py",
                "--interval", "120",
                "--endpoint", f"opc.tcp://localhost:{cfg.opcua_port}/eirvah/simulator",
            ]
            # use the workspace python directly so we don't need uv nesting
            dist_cmd = [
                "uv", "run", "python", "scripts/disturbance.py",
                "--interval", "120",
                "--endpoint", f"opc.tcp://localhost:{cfg.opcua_port}/eirvah/simulator",
            ]
            if cfg.dry_run:
                print(f"[dry-run] {' '.join(dist_cmd)}")
                proc = None
            else:
                proc = await asyncio.create_subprocess_exec(
                    *dist_cmd,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.PIPE,
                )

            await asyncio.sleep(duration)

            if proc is not None:
                proc.terminate()
                await proc.wait()

            # cooldown tail
            await asyncio.sleep(60)

            stop.set()
            await scraper_task
            scraper.flush(out_dir)

    return {"outcome": "ok"}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/scripts/test_harness.py::test_run_experiment_a_calls_disturbance_and_scrapes -v
```

Expected: `1 passed`

- [ ] **Step 5: Commit**

```bash
git add scripts/harness.py tests/scripts/test_harness.py
git commit -m "feat(harness): Experiment A — CPS loop orchestration"
```

---

## Task 7: Experiment B — pod deletion and recovery polling

**Files:**
- Modify: `scripts/harness.py`
- Modify: `tests/scripts/test_harness.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/scripts/test_harness.py`:

```python
async def test_wait_for_pod_running_returns_true_on_success() -> None:
    from harness import _wait_for_pod_running

    mock_proc = MagicMock()
    mock_proc.communicate = AsyncMock(return_value=(b"Running\n", b""))

    with patch("harness.asyncio.create_subprocess_exec", return_value=mock_proc):
        with patch("harness.asyncio.sleep"):
            result = await _wait_for_pod_running(
                label="app.kubernetes.io/name=data-converter",
                namespace="eirvah-edge",
                timeout=30,
            )

    assert result is True


async def test_wait_for_pod_running_returns_false_on_timeout() -> None:
    from harness import _wait_for_pod_running

    mock_proc = MagicMock()
    mock_proc.communicate = AsyncMock(return_value=(b"Pending\n", b""))

    with patch("harness.asyncio.create_subprocess_exec", return_value=mock_proc):
        with patch("harness.asyncio.sleep"):
            result = await _wait_for_pod_running(
                label="app.kubernetes.io/name=data-converter",
                namespace="eirvah-edge",
                timeout=1,
            )

    assert result is False


async def test_run_experiment_b_records_timeout_in_outcome(tmp_path: Path) -> None:
    from harness import HarnessConfig, run_experiment_b

    cfg = HarnessConfig(experiment="b", dry_run=False, output_dir=tmp_path)
    out_dir = tmp_path / "experiment-b" / "run1"
    out_dir.mkdir(parents=True)

    mock_scraper = MagicMock()
    mock_scraper.check_connectivity = AsyncMock()
    mock_scraper.run = AsyncMock()
    mock_scraper.flush = MagicMock()
    mock_scraper.rows = []

    # pod delete succeeds but recovery times out
    delete_proc = MagicMock()
    delete_proc.wait = AsyncMock()

    with patch("harness.asyncio.create_subprocess_exec", return_value=delete_proc):
        with patch("harness.asyncio.sleep"):
            with patch("harness.Scraper", return_value=mock_scraper):
                with patch("harness._wait_for_pod_running", return_value=False):
                    result = await run_experiment_b(cfg, out_dir)

    assert result["recovery_timed_out"] is True
    mock_scraper.flush.assert_called_once_with(out_dir)
```

- [ ] **Step 2: Run to verify they fail**

```bash
uv run pytest tests/scripts/test_harness.py -k "pod_running or experiment_b" -v
```

Expected: `ImportError` (_wait_for_pod_running not defined)

- [ ] **Step 3: Add _wait_for_pod_running and run_experiment_b to harness.py**

Append to `scripts/harness.py`:

```python
_QUERIES_B: dict[str, str] = {
    "pipeline_success_total": "eirvah_pipeline_success_total",
    "stage_timeout_total": "eirvah_pipeline_stage_timeout_total",
    "e2e_latency_sum": "eirvah_pipeline_e2e_latency_seconds_sum",
    "e2e_latency_count": "eirvah_pipeline_e2e_latency_seconds_count",
}


async def _wait_for_pod_running(
    label: str,
    namespace: str,
    timeout: int = 120,
) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        proc = await asyncio.create_subprocess_exec(
            "kubectl", "get", "pod",
            "-n", namespace,
            "-l", label,
            "-o", "jsonpath={.items[0].status.phase}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await proc.communicate()
        phase = stdout.decode().strip()
        if phase == "Running":
            return True
        await asyncio.sleep(2.0)
    return False


async def _delete_pod_and_wait(
    label: str,
    namespace: str,
    recovery_timeout: int,
    dry_run: bool,
) -> bool:
    cmd = ["kubectl", "-n", namespace, "delete", "pod", "-l", label]
    if dry_run:
        print(f"[dry-run] {' '.join(cmd)}")
        return True
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await proc.wait()
    return await _wait_for_pod_running(label, namespace, timeout=recovery_timeout)


async def run_experiment_b(cfg: HarnessConfig, out_dir: Path) -> dict[str, Any]:
    scraper = Scraper(
        prometheus_url=f"http://localhost:{cfg.prometheus_port}",
        queries=_QUERIES_B,
    )
    recovery_timeout = cfg.duration or 120
    timed_out = False

    async with port_forward("svc/prometheus", cfg.prometheus_port, 9090, cfg.namespace, cfg.dry_run):
        await scraper.check_connectivity()

        stop = asyncio.Event()
        scraper_task = asyncio.create_task(scraper.run(stop))

        # baseline
        await asyncio.sleep(30)

        for label in [
            "app.kubernetes.io/name=data-converter",
            "app.kubernetes.io/name=opcua-data-subscriber",
        ]:
            recovered = await _delete_pod_and_wait(
                label, cfg.namespace, recovery_timeout, cfg.dry_run
            )
            if not recovered:
                timed_out = True
            # recovery tail
            await asyncio.sleep(60)

        stop.set()
        await scraper_task
        scraper.flush(out_dir)

    return {"outcome": "ok", "recovery_timed_out": timed_out}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/scripts/test_harness.py -k "pod_running or experiment_b" -v
```

Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add scripts/harness.py tests/scripts/test_harness.py
git commit -m "feat(harness): Experiment B — pod failure and recovery"
```

---

## Task 8: Experiment C

**Files:**
- Modify: `scripts/harness.py`
- Modify: `tests/scripts/test_harness.py`

- [ ] **Step 1: Write failing test**

Append to `tests/scripts/test_harness.py`:

```python
async def test_run_experiment_c_launches_load_injector(tmp_path: Path) -> None:
    from harness import HarnessConfig, run_experiment_c

    cfg = HarnessConfig(experiment="c", dry_run=True, rate=500, output_dir=tmp_path)
    out_dir = tmp_path / "experiment-c" / "run1"
    out_dir.mkdir(parents=True)

    mock_scraper = MagicMock()
    mock_scraper.check_connectivity = AsyncMock()
    mock_scraper.run = AsyncMock()
    mock_scraper.flush = MagicMock()
    mock_scraper.rows = []

    mock_proc = MagicMock()
    mock_proc.returncode = None
    mock_proc.terminate = MagicMock()
    mock_proc.wait = AsyncMock()

    with patch("harness.asyncio.create_subprocess_exec", return_value=mock_proc):
        with patch("harness.asyncio.sleep"):
            with patch("harness.Scraper", return_value=mock_scraper):
                result = await run_experiment_c(cfg, out_dir)

    assert result["outcome"] == "ok"
    mock_scraper.check_connectivity.assert_called_once()
    mock_scraper.flush.assert_called_once_with(out_dir)
```

- [ ] **Step 2: Run to verify it fails**

```bash
uv run pytest tests/scripts/test_harness.py::test_run_experiment_c_launches_load_injector -v
```

Expected: `ImportError` (run_experiment_c not defined)

- [ ] **Step 3: Add run_experiment_c to harness.py**

Append to `scripts/harness.py`:

```python
_QUERIES_C: dict[str, str] = {
    "worker_handler_total": 'worker_handler_total{worker="uns-auto-contextualizer"}',
    "hpa_replicas": "kube_horizontalpodautoscaler_status_current_replicas",
    "cpu_usage": 'container_cpu_usage_seconds_total{container="uns-auto-contextualizer"}',
}


async def run_experiment_c(cfg: HarnessConfig, out_dir: Path) -> dict[str, Any]:
    scraper = Scraper(
        prometheus_url=f"http://localhost:{cfg.prometheus_port}",
        queries=_QUERIES_C,
    )
    load_duration = cfg.duration or 120

    async with port_forward("svc/prometheus", cfg.prometheus_port, 9090, cfg.namespace, cfg.dry_run):
        async with port_forward("svc/nats", cfg.nats_port, 4222, cfg.namespace, cfg.dry_run):
            await scraper.check_connectivity()

            stop = asyncio.Event()
            scraper_task = asyncio.create_task(scraper.run(stop))

            # baseline
            await asyncio.sleep(30)

            # load injector
            inject_cmd = [
                "uv", "run", "python", "scripts/load_inject.py",
                "--rate", str(cfg.rate),
                "--duration", str(load_duration),
                "--nats-url", f"nats://localhost:{cfg.nats_port}",
            ]
            if cfg.dry_run:
                print(f"[dry-run] {' '.join(inject_cmd)}")
                proc = None
            else:
                proc = await asyncio.create_subprocess_exec(
                    *inject_cmd,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.PIPE,
                )

            # scrape during load
            await asyncio.sleep(load_duration)

            if proc is not None:
                await proc.wait()

            # scale-down window
            await asyncio.sleep(90)

            stop.set()
            await scraper_task
            scraper.flush(out_dir)

    return {"outcome": "ok"}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/scripts/test_harness.py::test_run_experiment_c_launches_load_injector -v
```

Expected: `1 passed`

- [ ] **Step 5: Commit**

```bash
git add scripts/harness.py tests/scripts/test_harness.py
git commit -m "feat(harness): Experiment C — autoscaling under load"
```

---

## Task 9: CLI, atexit/SIGTERM handler, and main()

**Files:**
- Modify: `scripts/harness.py`
- Modify: `tests/scripts/test_harness.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/scripts/test_harness.py`:

```python
def test_parse_args_experiment_a() -> None:
    from harness import parse_args

    cfg = parse_args(["run", "--experiment", "a"])
    assert cfg.experiment == "a"
    assert cfg.namespace == "eirvah-edge"
    assert cfg.dry_run is False
    assert cfg.prometheus_port == 9090


def test_parse_args_dry_run_flag() -> None:
    from harness import parse_args

    cfg = parse_args(["run", "--experiment", "c", "--rate", "200", "--dry-run"])
    assert cfg.experiment == "c"
    assert cfg.rate == 200
    assert cfg.dry_run is True


def test_parse_args_invalid_experiment_exits() -> None:
    from harness import parse_args

    with pytest.raises(SystemExit):
        parse_args(["run", "--experiment", "z"])
```

- [ ] **Step 2: Run to verify they fail**

```bash
uv run pytest tests/scripts/test_harness.py -k "parse_args" -v
```

Expected: `ImportError` (parse_args not defined)

- [ ] **Step 3: Add parse_args, atexit/SIGTERM handler, and main() to harness.py**

Append to `scripts/harness.py`:

```python
import argparse

_cleanup_callbacks: list[Callable[[], None]] = []


def _run_cleanup() -> None:
    for fn in _cleanup_callbacks:
        try:
            fn()
        except Exception:
            pass


atexit.register(_run_cleanup)


def _handle_sigterm(signum: int, frame: object) -> None:
    _run_cleanup()
    sys.exit(0)


signal.signal(signal.SIGTERM, _handle_sigterm)


def parse_args(argv: list[str] | None = None) -> HarnessConfig:
    parser = argparse.ArgumentParser(description="EirVah experiment harness")
    sub = parser.add_subparsers(dest="command", required=True)
    run_p = sub.add_parser("run", help="Run an experiment and collect metrics")
    run_p.add_argument("--experiment", required=True, choices=["a", "b", "c"])
    run_p.add_argument("--namespace", default="eirvah-edge")
    run_p.add_argument("--prometheus-port", type=int, default=9090, dest="prometheus_port")
    run_p.add_argument("--nats-port", type=int, default=4222, dest="nats_port")
    run_p.add_argument("--opcua-port", type=int, default=4840, dest="opcua_port")
    run_p.add_argument("--output-dir", type=Path, default=Path("results"), dest="output_dir")
    run_p.add_argument("--duration", type=int, default=None)
    run_p.add_argument("--rate", type=int, default=500)
    run_p.add_argument("--dry-run", action="store_true", dest="dry_run")
    args = parser.parse_args(argv)
    return HarnessConfig(
        experiment=args.experiment,
        namespace=args.namespace,
        prometheus_port=args.prometheus_port,
        nats_port=args.nats_port,
        opcua_port=args.opcua_port,
        output_dir=args.output_dir,
        duration=args.duration,
        rate=args.rate,
        dry_run=args.dry_run,
    )


_EXPERIMENTS = {
    "a": run_experiment_a,
    "b": run_experiment_b,
    "c": run_experiment_c,
}


async def _run(cfg: HarnessConfig) -> None:
    run_ts = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%S")
    out_dir = setup_output_dir(cfg.output_dir, cfg.experiment, run_ts)
    git_sha = _current_git_sha()
    start_time = datetime.now(UTC).isoformat()

    # Register atexit flush: write an interrupted run.json if we crash
    def _on_exit() -> None:
        if not (out_dir / "run.json").exists():
            write_run_json(
                out_dir=out_dir,
                experiment=cfg.experiment,
                params={"duration": cfg.duration, "rate": cfg.rate},
                start_time=start_time,
                end_time=datetime.now(UTC).isoformat(),
                outcome="interrupted",
                git_sha=git_sha,
            )

    _cleanup_callbacks.append(_on_exit)

    print(f"[harness] experiment={cfg.experiment}  out={out_dir}  dry_run={cfg.dry_run}")

    fn = _EXPERIMENTS[cfg.experiment]
    result = await fn(cfg, out_dir)

    end_time = datetime.now(UTC).isoformat()
    write_run_json(
        out_dir=out_dir,
        experiment=cfg.experiment,
        params={"duration": cfg.duration, "rate": cfg.rate},
        start_time=start_time,
        end_time=end_time,
        outcome=result.get("outcome", "ok"),
        git_sha=git_sha,
    )
    print(f"[harness] done  outcome={result.get('outcome')}  out={out_dir}")


def main() -> None:
    cfg = parse_args()
    asyncio.run(_run(cfg))


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/scripts/test_harness.py -k "parse_args" -v
```

Expected: `3 passed`

- [ ] **Step 5: Run full test suite**

```bash
uv run pytest tests/scripts/test_harness.py -v
```

Expected: all tests pass (no failures)

- [ ] **Step 6: Commit**

```bash
git add scripts/harness.py tests/scripts/test_harness.py
git commit -m "feat(harness): CLI, atexit/SIGTERM handler, main entrypoint"
```

---

## Task 10: Fix duplicate dist_cmd assignment + dry-run smoke test

**Files:**
- Modify: `scripts/harness.py` (remove duplicate dist_cmd assignment in run_experiment_a)
- Verify: dry-run smoke test passes

- [ ] **Step 1: Remove duplicate dist_cmd in run_experiment_a**

In `scripts/harness.py`, inside `run_experiment_a`, there are two `dist_cmd = [...]` assignments. Remove the first one (the one using `sys.executable`). The function should only have:

```python
            dist_cmd = [
                "uv", "run", "python", "scripts/disturbance.py",
                "--interval", "120",
                "--endpoint", f"opc.tcp://localhost:{cfg.opcua_port}/eirvah/simulator",
            ]
```

- [ ] **Step 2: Run full test suite to confirm no regressions**

```bash
uv run pytest tests/scripts/test_harness.py -v
```

Expected: all tests pass

- [ ] **Step 3: Dry-run smoke test**

```bash
uv run python scripts/harness.py run --experiment a --dry-run
```

Expected output (order may vary):
```
[harness] experiment=a  out=results/experiment-a/...  dry_run=True
[dry-run] kubectl port-forward -n eirvah-edge svc/prometheus 9090:9090
[dry-run] kubectl port-forward -n eirvah-edge svc/opcua-simulator 4840:4840
[dry-run] uv run python scripts/disturbance.py --interval 120 --endpoint opc.tcp://localhost:4840/eirvah/simulator
[harness] done  outcome=ok  out=results/experiment-a/...
```

```bash
uv run python scripts/harness.py run --experiment b --dry-run
uv run python scripts/harness.py run --experiment c --dry-run
```

All three should print `[dry-run]` prefixed commands and exit cleanly.

- [ ] **Step 4: Verify results directory NOT created for dry-run**

```bash
ls results/  # if directory exists, check no parquet/csv files were written
```

Expected: `results/experiment-a/<timestamp>/run.json` exists (run.json is always written — it records the run metadata including dry_run=True params). No `raw.parquet` or `summary.csv` (scraper had no rows in dry-run).

- [ ] **Step 5: Final commit**

```bash
git add scripts/harness.py
git commit -m "fix(harness): remove duplicate dist_cmd assignment in experiment A"
```

---

## Self-Review Checklist

**Spec coverage:**
- [x] CLI with `run --experiment a|b|c` — Task 9
- [x] `--dry-run`, `--duration`, `--rate`, `--namespace`, port flags — Task 9
- [x] Timestamped output dir `results/experiment-{a,b,c}/YYYY-MM-DDTHH-MM-SS/` — Task 2
- [x] `raw.parquet` with correct schema — Task 5
- [x] `summary.csv` with mean/p50/p95/p99/min/max — Task 5
- [x] `run.json` with git_sha, params, outcome — Task 2
- [x] PortForward context manager with retry — Task 3
- [x] Prometheus connectivity check (fail fast) — Task 4
- [x] Scraper polling at 10 s intervals — Task 4
- [x] Experiment A: disturbance subprocess, baseline, cooldown tail — Task 6
- [x] Experiment B: pod deletion × 2, recovery polling, `recovery_timed_out` flag — Task 7
- [x] Experiment C: load injector subprocess, scale-down window — Task 8
- [x] atexit/SIGTERM → flush partial Parquet + write interrupted run.json — Task 9
- [x] pyarrow + pandas added to dev deps — Task 1
- [x] pytest pythonpath configured — Task 1
