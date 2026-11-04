"""Correlated command evidence on the observability path, never a control message."""

from __future__ import annotations

import os
import time
import uuid
from datetime import UTC, datetime
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


def record_command(component: str, action: str, command: Command, **evidence: Any) -> None:
    """Emit an observation; clock uncertainty and collection completeness remain unknown."""
    structlog.get_logger(component).info(
        "command_execution",
        **evidence,
        execution_schema="1.0",
        record_id=str(uuid.uuid4()),
        component=component,
        action=action,
        process_instance=_INSTANCE,
        host=os.environ.get("HOSTNAME", "unknown"),
        recorded_at=datetime.now(UTC).isoformat(),
        monotonic_ns=time.monotonic_ns(),
        clock_uncertainty_s=None,
        correlation_id=command.correlation_id,
        target=command.target_uns_topic,
        requester=command.requester,
        requested_value=command.requested_value,
        value_type=command.value_type,
        requested_at=command.requested_at.isoformat(),
        deadline=command.deadline.isoformat() if command.deadline else None,
    )
