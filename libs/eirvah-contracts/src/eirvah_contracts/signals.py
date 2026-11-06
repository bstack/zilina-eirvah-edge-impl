"""Internal signal envelopes (NOT the public telemetry payload — see telemetry.py)."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

SignalValueType = Literal["double", "int64", "bool", "string"]
Quality = Literal["good", "uncertain", "bad"]
SignalValue = float | int | bool | str


class SourcePosition(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    producer: str = Field(min_length=1, max_length=512)
    epoch: int = Field(ge=1)
    sequence: int = Field(ge=1)
    boundary: Literal["adapter"] = "adapter"


class RawSignalEnvelope(BaseModel):
    """Emitted by ``opcua-data-subscriber`` onto ``uns.ingress.raw``."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0"] = "1.0"

    source_endpoint: str
    node_id: str
    value: SignalValue
    value_type: SignalValueType
    quality: Quality
    position: SourcePosition | None = None
    source_timestamp: datetime
    server_timestamp: datetime
    received_at: datetime


class NormalizedSignalEnvelope(BaseModel):
    """Emitted by ``data-converter`` (the value has been unit-converted/scaled)."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0"] = "1.0"

    node_id: str
    value: SignalValue
    value_type: SignalValueType
    unit: str
    quality: Quality
    position: SourcePosition | None = None
    source_timestamp: datetime
    received_at: datetime
