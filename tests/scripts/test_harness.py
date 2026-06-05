"""Unit tests for the experiment harness."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
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
    mock_scraper.check_connectivity.assert_not_called()  # dry_run skips connectivity check
    mock_scraper.flush.assert_called_once_with(out_dir)


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

    delete_proc = MagicMock()
    delete_proc.returncode = None
    delete_proc.terminate = MagicMock()
    delete_proc.wait = AsyncMock()

    with patch("harness.asyncio.create_subprocess_exec", return_value=delete_proc):
        with patch("harness.asyncio.sleep"):
            with patch("harness.Scraper", return_value=mock_scraper):
                with patch("harness._wait_for_pod_running", return_value=False):
                    result = await run_experiment_b(cfg, out_dir)

    assert result["recovery_timed_out"] is True
    mock_scraper.flush.assert_called_once_with(out_dir)


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
    mock_scraper.check_connectivity.assert_not_called()  # dry_run skips connectivity check
    mock_scraper.flush.assert_called_once_with(out_dir)
