from __future__ import annotations

from datetime import UTC, datetime

import pytest
from eirvah_contracts.pipeline import PublishRequest
from eirvah_contracts.telemetry import TelemetryPayload
from eirvah_contracts.uns import UNSPath


def _uns() -> UNSPath:
    return UNSPath(
        enterprise="uniza",
        site="zilina",
        area="factory1",
        line="line_a",
        cell="bottler",
        equipment="temperature_sensor_01",
        measurement="temperature",
    )


def _pub_request() -> PublishRequest:
    now = datetime.now(UTC)
    return PublishRequest(
        uns_topic="uniza/zilina/factory1/line_a/bottler/temperature_sensor_01/temperature",
        correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A",
        value=23.4,
        value_type="double",
        unit="degC",
        quality="good",
        semantic_type="temperature.celsius",
        uns_path=_uns(),
        source_endpoint="opc.tcp://opcua-simulator:4840",
        source_node_id="Bottler.Temperature01",
        source_timestamp=now,
        edge_ingress=now,
    )


def test_build_telemetry_payload() -> None:
    from mqtt_uns_publisher.service import build_telemetry_payload

    req = _pub_request()
    payload = build_telemetry_payload(req)
    assert isinstance(payload, TelemetryPayload)
    assert payload.schema_version == "1.0"
    assert payload.correlation_id == req.correlation_id
    assert payload.value == 23.4
    assert payload.unit == "degC"
    assert payload.quality == "good"
    assert payload.source.protocol == "opcua"
    assert payload.source.node_id == "Bottler.Temperature01"
    assert payload.timestamps.edge_publish is not None
    assert payload.timestamps.edge_publish >= payload.timestamps.edge_ingress


def test_build_telemetry_payload_json_validates() -> None:
    from mqtt_uns_publisher.service import build_telemetry_payload

    req = _pub_request()
    payload = build_telemetry_payload(req)
    json_str = payload.model_dump_json()
    restored = TelemetryPayload.model_validate_json(json_str)
    assert restored.correlation_id == req.correlation_id
