"""TelemetryPayload v1.0 — the public MQTT payload (spec §4.2)."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

from eirvah_contracts.signals import Quality, SignalValue, SignalValueType
from eirvah_contracts.ulid import is_valid_correlation_id
from eirvah_contracts.uns import UNSPath

SOURCE_PROTOCOL_OPCUA: Literal["opcua"] = "opcua"


class TelemetrySource(BaseModel):
    """Provenance for a telemetry message: how the value was originally produced."""

    model_config = ConfigDict(extra="forbid")

    protocol: Literal["opcua", "modbus", "siemens_s7"]
    endpoint: str
    node_id: str


class TelemetryTimestamps(BaseModel):
    """Three time-points along the telemetry path (all ISO 8601 UTC)."""

    model_config = ConfigDict(extra="forbid")

    source: datetime          # set by device / simulator
    edge_ingress: datetime    # set by opcua-data-subscriber
    edge_publish: datetime    # set by mqtt-uns-publisher


class TelemetryPayload(BaseModel):
    """Public MQTT payload, schema version 1.0 (spec §4.2).

    Consumers MUST tolerate unknown additional fields within the same major
    version (forward-compatibility). That is what ``extra="allow"`` expresses.
    """

    model_config = ConfigDict(extra="allow")

    schema_version: Literal["1.0"] = "1.0"
    correlation_id: str
    value: SignalValue
    value_type: SignalValueType
    semantic_type: str
    unit: str
    quality: Quality
    uns_path: UNSPath
    source: TelemetrySource
    timestamps: TelemetryTimestamps

    def model_post_init(self, __context: object) -> None:
        if not is_valid_correlation_id(self.correlation_id):
            raise ValueError(f"invalid correlation_id: {self.correlation_id!r}")
