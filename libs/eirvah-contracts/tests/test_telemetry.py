import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from eirvah_contracts.telemetry import (
    SOURCE_PROTOCOL_OPCUA,
    TelemetryPayload,
    TelemetrySource,
    TelemetryTimestamps,
)
from eirvah_contracts.uns import UNSPath

GOLDEN = Path(__file__).parent / "golden" / "telemetry_v1_0_sample.json"


def test_golden_fixture_validates_as_v1() -> None:
    raw = json.loads(GOLDEN.read_text())
    payload = TelemetryPayload.model_validate(raw)
    assert payload.schema_version == "1.0"
    assert payload.value == 23.4
    assert payload.uns_path.measurement == "temperature"
    assert payload.source.protocol == "opcua"


def test_round_trip_through_json_preserves_fixture_semantics() -> None:
    raw = json.loads(GOLDEN.read_text())
    payload = TelemetryPayload.model_validate(raw)
    re_serialised = json.loads(payload.model_dump_json())
    assert re_serialised == raw


def test_unknown_optional_fields_are_accepted() -> None:
    raw = json.loads(GOLDEN.read_text())
    raw["tags"] = {"site_owner": "uniza-it"}
    raw["lineage"] = ["opcua-data-subscriber", "data-converter"]
    TelemetryPayload.model_validate(raw)  # should not raise


def test_missing_required_field_rejected() -> None:
    raw = json.loads(GOLDEN.read_text())
    del raw["timestamps"]
    with pytest.raises(ValueError):
        TelemetryPayload.model_validate(raw)


def test_construct_programmatically() -> None:
    p = TelemetryPayload(
        correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A",
        value=22.0,
        value_type="double",
        semantic_type="setpoint.target",
        unit="degC",
        quality="good",
        uns_path=UNSPath(
            enterprise="uniza",
            site="zilina",
            area="factory1",
            line="line_a",
            cell="bottler",
            equipment="setpoint_unit",
            measurement="setpoint_temperature",
        ),
        source=TelemetrySource(
            protocol=SOURCE_PROTOCOL_OPCUA,
            endpoint="opc.tcp://opcua-simulator:4840",
            node_id="ns=2;s=Bottler.Setpoint",
        ),
        timestamps=TelemetryTimestamps(
            source=datetime(2026, 5, 16, 13, 45, 22, 123456, tzinfo=UTC),
            edge_ingress=datetime(2026, 5, 16, 13, 45, 22, 150123, tzinfo=UTC),
            edge_publish=datetime(2026, 5, 16, 13, 45, 22, 152456, tzinfo=UTC),
        ),
    )
    assert p.schema_version == "1.0"
