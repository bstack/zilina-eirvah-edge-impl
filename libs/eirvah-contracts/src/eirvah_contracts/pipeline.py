"""Internal pipeline wire contracts shared between orchestrator and workers."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, field_validator

from eirvah_contracts.signals import Quality, SignalValue, SignalValueType
from eirvah_contracts.ulid import is_valid_correlation_id
from eirvah_contracts.uns import UNSPath


class ContextualizeResult(BaseModel):
    """Reply payload from ``uns-auto-contextualizer`` on ``uns.work.contextualize``."""

    model_config = ConfigDict(extra="forbid")

    uns_topic: str
    uns_path: UNSPath
    semantic_type: str


class PublishRequest(BaseModel):
    """Request payload sent to ``mqtt-uns-publisher`` on ``uns.work.publish``.

    Carries everything needed to build a ``TelemetryPayload`` v1.0; the publisher
    only adds ``timestamps.edge_publish`` (set to ``now()`` at publish time).
    """

    model_config = ConfigDict(extra="forbid")

    uns_topic: str
    correlation_id: str
    value: SignalValue
    value_type: SignalValueType
    unit: str
    quality: Quality
    semantic_type: str
    uns_path: UNSPath
    source_endpoint: str
    source_node_id: str
    source_timestamp: datetime
    edge_ingress: datetime

    @field_validator("correlation_id")
    @classmethod
    def _validate_correlation_id(cls, v: str) -> str:
        if not is_valid_correlation_id(v):
            raise ValueError(f"invalid correlation_id: {v!r}")
        return v
