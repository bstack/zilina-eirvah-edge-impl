from __future__ import annotations

from datetime import UTC, datetime

import pytest
from eirvah_contracts.pipeline import ContextualizeResult, PublishRequest
from eirvah_contracts.uns import UNSPath, build_uns_topic


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


def test_contextualize_result_round_trip() -> None:
    path = _uns()
    result = ContextualizeResult(
        uns_topic=build_uns_topic(path),
        uns_path=path,
        semantic_type="temperature.celsius",
    )
    raw = result.model_dump(mode="json")
    restored = ContextualizeResult.model_validate(raw)
    assert restored.uns_topic == result.uns_topic
    assert restored.semantic_type == "temperature.celsius"


def test_publish_request_round_trip() -> None:
    now = datetime.now(UTC)
    req = PublishRequest(
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
    raw = req.model_dump(mode="json")
    restored = PublishRequest.model_validate(raw)
    assert restored.value == 23.4
    assert restored.quality == "good"


def test_publish_request_rejects_bad_correlation_id() -> None:
    from pydantic import ValidationError

    now = datetime.now(UTC)
    with pytest.raises(ValidationError):
        PublishRequest(
            uns_topic="a/b/c/d/e/f/g",
            correlation_id="not-a-ulid",
            value=1.0,
            value_type="double",
            unit="degC",
            quality="good",
            semantic_type="temperature.celsius",
            uns_path=_uns(),
            source_endpoint="opc.tcp://localhost:4840",
            source_node_id="x",
            source_timestamp=now,
            edge_ingress=now,
        )
