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
