"""Correlated command evidence on the observability path, never a control message."""

from __future__ import annotations

import copy
import json
import math
import os
import tempfile
import time
import uuid
from collections import deque
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from threading import Lock
from typing import Any, Protocol

import structlog

_INSTANCE = str(uuid.uuid4())


class Command(Protocol):
    @property
    def correlation_id(self) -> str: ...
    @property
    def target_uns_topic(self) -> str: ...
    @property
    def requester(self) -> str: ...
    @property
    def requested_value(self) -> object: ...
    @property
    def value_type(self) -> str: ...
    @property
    def requested_at(self) -> datetime: ...
    @property
    def deadline(self) -> datetime | None: ...


@lru_cache(maxsize=1)
def clock_domain() -> str | None:
    """Shared monotonic domain only when Linux kernel and time namespace match."""
    try:
        return (
            "linux-monotonic:"
            + Path("/proc/sys/kernel/random/boot_id").read_text().strip()
            + ":"
            + os.readlink("/proc/self/ns/time")
        )
    except OSError:
        return None


def clock_snapshot() -> dict[str, Any]:
    before = time.monotonic_ns()
    wall = time.time_ns()
    after = time.monotonic_ns()
    return {
        "wall_time_ns": wall,
        "recorded_at": datetime.fromtimestamp(wall // 1_000_000_000, UTC)
        .replace(microsecond=(wall % 1_000_000_000) // 1000)
        .isoformat(),
        "monotonic_ns": (before + after) // 2,
        "monotonic_before_ns": before,
        "monotonic_after_ns": after,
        "clock_domain": clock_domain(),
        "timestamp_resolution_ns": max(
            1000, math.ceil(time.get_clock_info("time").resolution * 1e9)
        ),
    }


class EvidenceBuffer:
    """Bounded process-local records, optionally journalled privately for kubectl exec capture."""

    def __init__(self, capacity: int = 4096, directory: Path | None = None) -> None:
        if capacity < 1:
            raise ValueError("capacity must be positive")
        self._records: deque[dict[str, Any]] = deque(maxlen=capacity)
        self._seq = 0
        self._lock = Lock()
        self.directory = directory
        self.capacity = capacity
        self._journal = Path(_INSTANCE) / "journal.jsonl"
        if directory is not None:
            (directory / _INSTANCE).mkdir(parents=True, exist_ok=True, mode=0o700)
            (directory / _INSTANCE).chmod(0o700)
            path = directory / self._journal
            path.touch(mode=0o600, exist_ok=True)
            self._checkpoint()

    def _checkpoint(self) -> None:
        if self.directory is None:
            return
        metadata = {
            "schema_version": "1.0",
            "process_instance": _INSTANCE,
            "record_seq": self._seq,
            "capacity": self.capacity,
            "overflow": self._seq > self.capacity,
            "journal_file": self._journal.as_posix(),
        }
        temporary = self.directory / (_INSTANCE + ".tmp")
        with temporary.open("w", encoding="utf-8") as stream:
            temporary.chmod(0o600)
            stream.write(json.dumps(metadata))
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(self.directory / "current.json")

    def append(self, record: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            self._seq += 1
            stored = json.loads(
                json.dumps({**record, "record_seq": self._seq}, default=str, allow_nan=False)
            )
            if self.directory is not None:
                if self._seq <= self.capacity:
                    with (self.directory / self._journal).open("a", encoding="utf-8") as stream:
                        stream.write(json.dumps(stored) + "\n")
                        stream.flush()
                        os.fsync(stream.fileno())
                self._checkpoint()
            self._records.append(stored)
            return copy.deepcopy(stored)

    def snapshot(self, after: int = 0) -> dict[str, Any]:
        if after < 0:
            raise ValueError("negative cursor")
        with self._lock:
            first = self._records[0]["record_seq"] if self._records else self._seq + 1
            return {
                "schema_version": "1.0",
                "process_instance": _INSTANCE,
                "record_seq": self._seq,
                "first_available_seq": first,
                "complete": first - 1 <= after <= self._seq
                and not (self.directory and self._seq > self.capacity),
                "clock": clock_snapshot(),
                "records": copy.deepcopy([r for r in self._records if r["record_seq"] > after]),
            }


_BUFFER = EvidenceBuffer(
    directory=Path(tempfile.gettempdir()) / "eirvah-execution-evidence" if os.name == "posix" else None
)


def execution_snapshot(after: int = 0) -> dict[str, Any]:
    return _BUFFER.snapshot(after)


def record_command(component: str, action: str, command: Command, **evidence: Any) -> None:
    """Journal and log command evidence without exposing a new service endpoint."""
    record = _BUFFER.append(
        dict(
            **evidence,
            event="command_execution",
            execution_schema="1.0",
            record_id=str(uuid.uuid4()),
            component=component,
            action=action,
            process_instance=_INSTANCE,
            host=os.environ.get("HOSTNAME", "unknown"),
            **clock_snapshot(),
            clock_uncertainty_s=None,
            correlation_id=command.correlation_id,
            target=command.target_uns_topic,
            requester=command.requester,
            requested_value=command.requested_value,
            value_type=command.value_type,
            requested_at=command.requested_at.isoformat(),
            deadline=command.deadline.isoformat() if command.deadline else None,
        )
    )
    structlog.get_logger(component).info(
        "command_execution", **{k: v for k, v in record.items() if k != "event"}
    )
