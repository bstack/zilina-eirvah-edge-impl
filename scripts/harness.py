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
