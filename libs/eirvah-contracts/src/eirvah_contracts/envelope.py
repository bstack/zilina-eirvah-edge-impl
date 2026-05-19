"""NATS envelope shared by every internal edge message (spec §4.4)."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, field_validator

from eirvah_contracts.ulid import is_valid_correlation_id

Status = Literal["ok", "error"]


class EnvelopeError(BaseModel):
    """Structured error payload accompanying ``status="error"`` envelopes."""

    model_config = ConfigDict(extra="forbid")

    kind: str
    message: str


class NATSEnvelope(BaseModel):
    """The wrapper around every internal NATS message.

    ``payload`` is intentionally loosely typed (``dict[str, Any]``) here so the
    envelope can carry any of the schema-versioned domain payloads defined
    elsewhere in this package. The domain layer is responsible for narrowing.
    """

    model_config = ConfigDict(extra="forbid")

    correlation_id: str
    status: Status = "ok"
    payload: dict[str, Any] | None = None
    context: dict[str, Any] | None = None
    error: EnvelopeError | None = None

    @field_validator("correlation_id")
    @classmethod
    def _validate_correlation_id(cls, value: str) -> str:
        if not is_valid_correlation_id(value):
            raise ValueError(f"invalid correlation_id: {value!r}")
        return value
