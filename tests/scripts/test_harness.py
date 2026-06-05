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
