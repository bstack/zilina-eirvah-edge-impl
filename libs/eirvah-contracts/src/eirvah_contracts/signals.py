"""Internal signal envelopes (NOT the public telemetry payload — see telemetry.py)."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

SignalValueType = Literal["double", "int64", "bool", "string"]
Quality = Literal["good", "uncertain", "bad"]
SignalValue = float | int | bool | str


class RawSignalEnvelope(BaseModel):
    """Emitted by ``opcua-data-subscriber`` onto ``uns.ingress.raw``."""

    model_config = ConfigDict(extra="forbid")

    source_endpoint: str
    node_id: str
    value: SignalValue
    value_type: SignalValueType
    quality: Quality
    source_timestamp: datetime
    server_timestamp: datetime
    received_at: datetime


class NormalizedSignalEnvelope(BaseModel):
    """Emitted by ``data-converter`` (the value has been unit-converted/scaled)."""

    model_config = ConfigDict(extra="forbid")

    node_id: str
    value: SignalValue
    value_type: SignalValueType
    unit: str
    quality: Quality
    source_timestamp: datetime
    received_at: datetime
