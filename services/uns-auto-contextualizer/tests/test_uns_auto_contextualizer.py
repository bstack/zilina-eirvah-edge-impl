from __future__ import annotations

from datetime import UTC, datetime

import pytest
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_contracts.pipeline import ContextualizeResult
from eirvah_contracts.signals import NormalizedSignalEnvelope
from eirvah_contracts.uns import build_uns_topic


def _normalized(node_id: str = "Bottler.Temperature01") -> NormalizedSignalEnvelope:
    now = datetime.now(UTC)
    return NormalizedSignalEnvelope(
        node_id=node_id,
        value=23.4,
        value_type="double",
        unit="degC",
        quality="good",
        source_timestamp=now,
        received_at=now,
    )


def test_contextualize_known_node() -> None:
    from uns_auto_contextualizer.service import MappingEntry, contextualize

    mapping = {
        "Bottler.Temperature01": MappingEntry(
            node_id="Bottler.Temperature01",
            area="factory1",
            line="line_a",
            cell="bottler",
            equipment="temperature_sensor_01",
            measurement="temperature",
            semantic_type="temperature.celsius",
        )
    }
    result = contextualize(
        _normalized(), mapping, enterprise="uniza", site="zilina"
    )
    assert isinstance(result, ContextualizeResult)
    assert result.uns_topic == "uniza/zilina/factory1/line_a/bottler/temperature_sensor_01/temperature"
    assert result.semantic_type == "temperature.celsius"
    assert result.uns_path.enterprise == "uniza"


def test_contextualize_unknown_node_returns_none() -> None:
    from uns_auto_contextualizer.service import contextualize

    result = contextualize(
        _normalized(node_id="Unknown.Node"), {}, enterprise="uniza", site="zilina"
    )
    assert result is None


def test_handle_request_ok() -> None:
    from uns_auto_contextualizer.service import MappingEntry, handle_contextualize_request

    mapping = {
        "Bottler.Temperature01": MappingEntry(
            node_id="Bottler.Temperature01",
            area="factory1",
            line="line_a",
            cell="bottler",
            equipment="temperature_sensor_01",
            measurement="temperature",
            semantic_type="temperature.celsius",
        )
    }
    req = NATSEnvelope(
        correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A",
        payload=_normalized().model_dump(mode="json"),
    )
    reply = handle_contextualize_request(req, mapping, enterprise="uniza", site="zilina")
    assert reply.status == "ok"
    assert reply.payload is not None
    assert "uns_topic" in reply.payload


def test_handle_request_unknown_node_returns_error() -> None:
    from uns_auto_contextualizer.service import handle_contextualize_request

    req = NATSEnvelope(
        correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A",
        payload=_normalized(node_id="Nope.Node").model_dump(mode="json"),
    )
    reply = handle_contextualize_request(req, {}, enterprise="uniza", site="zilina")
    assert reply.status == "error"
