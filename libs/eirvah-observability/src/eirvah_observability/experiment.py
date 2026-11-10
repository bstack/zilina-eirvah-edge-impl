"""Opt-in run metadata for experiment drivers; no control payloads or credentials."""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

_lock = threading.Lock()


def emit(event: str, **fields: Any) -> None:
    path = os.environ.get("EIRVAH_EXPERIMENT_EVENTS")
    if not path:
        return
    row = dict(
        schema_version="1.0",
        event=event,
        observed_at=datetime.now(UTC).isoformat(),
        monotonic_ns=time.perf_counter_ns(),
        pid=os.getpid(),
        **fields,
    )
    content = (json.dumps(row, allow_nan=False, separators=(",", ":")) + "\n").encode()
    with _lock:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            if os.write(descriptor, content) != len(content):
                raise OSError("partial experiment event write")
        finally:
            os.close(descriptor)


@contextmanager
def phase(name: str) -> Iterator[None]:
    identity = uuid.uuid4().hex
    emit("phase_begin", phase=name, phase_id=identity)
    success = False
    try:
        yield
        success = True
    finally:
        emit("phase_end", phase=name, phase_id=identity, success=success)
