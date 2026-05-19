"""Uniform structlog configuration for every EirVah service.

Every log line is JSON. Every line includes a UTC ISO 8601 timestamp, a level,
and the event — plus any structured context including correlation_id.
"""

from __future__ import annotations

import logging
import sys
from typing import IO, Any, TextIO, cast

import structlog


def configure_logging(
    level: str = "INFO",
    stream: IO[str] | None = None,
) -> None:
    """Configure stdlib + structlog to emit JSON lines to *stream* (default stdout)."""
    out = stream if stream is not None else sys.stdout
    log_level = getattr(logging, level.upper())
    logging.basicConfig(stream=out, level=log_level, format="%(message)s", force=True)

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(file=cast(TextIO, out)),
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        cache_logger_on_first_use=False,
    )


def bind_correlation_id(correlation_id: str) -> None:
    """Bind *correlation_id* to all subsequent log calls in this context."""
    structlog.contextvars.bind_contextvars(correlation_id=correlation_id)


def clear_correlation_id() -> None:
    """Clear any previously bound correlation_id."""
    structlog.contextvars.unbind_contextvars("correlation_id")


def _add_logger_name(
    _logger: Any, _method: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    record = event_dict.pop("_record", None)
    if record is not None and hasattr(record, "name"):
        event_dict.setdefault("logger", record.name)
    return event_dict
