"""The documented workspace setup must provide experiment subprocess dependencies."""

from __future__ import annotations

import subprocess
import sys


def test_experiment_dependencies_are_available_to_child_python() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import asyncua, httpx, pyarrow, nats; "
            "from eirvah_contracts.envelope import NATSEnvelope",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
