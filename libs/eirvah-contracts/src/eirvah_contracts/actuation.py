"""ActuationRequest + result events (spec §4.3)."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator

from eirvah_contracts.signals import SignalValue, SignalValueType
from eirvah_contracts.ulid import is_valid_correlation_id
from eirvah_contracts.uns import parse_uns_topic


class ActuationRequest(BaseModel):
    """AMQP payload on ``eirvah.actuation.requests`` (spec §4.3)."""

    model_config = ConfigDict(extra="allow")

    schema_version: Literal["1.0"] = "1.0"
    correlation_id: str
    requester: str
    target_uns_topic: str
    requested_value: SignalValue
    value_type: SignalValueType
    reason: str
    requested_at: datetime
    deadline: datetime | None = None

    @field_validator("correlation_id")
    @classmethod
    def _validate_correlation_id(cls, value: str) -> str:
        if not is_valid_correlation_id(value):
            raise ValueError(f"invalid correlation_id: {value!r}")
        return value

    @field_validator("target_uns_topic")
    @classmethod
    def _validate_target_topic(cls, value: str) -> str:
        parse_uns_topic(value)
        return value


class ActuationApproveResult(ActuationRequest):
    """Approve event on the AMQP results exchange."""

    decision: Literal["approve"]
    written_at: datetime | None = None


class ActuationRejectResult(ActuationRequest):
    """Reject event on the AMQP results exchange."""

    decision: Literal["reject"]
    rejection_reason: str
