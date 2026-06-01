from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_contracts.pipeline import ContextualizeResult
from eirvah_contracts.signals import NormalizedSignalEnvelope


def _normalized(node_id: str = "Capper.TorqueSensor01") -> NormalizedSignalEnvelope:
    now = datetime.now(UTC)
    return NormalizedSignalEnvelope(
        node_id=node_id,
        value=2.5,
        value_type="double",
        unit="Nm",
        quality="good",
        source_timestamp=now,
        received_at=now,
    )


@pytest.fixture
def ontology_path(tmp_path: Path) -> Path:
    ontology = {
        "@context": {
            "sosa": "http://www.w3.org/ns/sosa/",
            "ssn": "http://www.w3.org/ns/ssn/",
            "eirvah": "https://eirvah.uniza/ontology/"
        },
        "@graph": [
            {"@id": "eirvah:Capper", "@type": ["ssn:System", "sosa:FeatureOfInterest"],
             "eirvah:enterprise": "uniza", "eirvah:site": "zilina",
             "eirvah:area": "factory1", "eirvah:line": "line_a", "eirvah:cell": "capper"},
            {"@id": "eirvah:Torque", "@type": "sosa:ObservableProperty",
             "eirvah:unit": "Nm", "eirvah:valueType": "double", "eirvah:semanticType": "torque.nm"},
            {"@id": "eirvah:CapperTorqueSensor01", "@type": "sosa:Sensor",
             "sosa:isHostedBy": {"@id": "eirvah:Capper"},
             "sosa:observes": {"@id": "eirvah:Torque"},
             "eirvah:nodeId": "Capper.TorqueSensor01",
             "eirvah:equipment": "torque_sensor_01",
             "eirvah:measurement": "torque"}
        ]
    }
    path = tmp_path / "ontology.jsonld"
    path.write_text(json.dumps(ontology))
    return path


def test_contextualize_known_node(ontology_path: Path) -> None:
    from uns_auto_contextualizer.service import load_ontology, contextualize

    graph = load_ontology(ontology_path)
    result = contextualize(_normalized(), graph)
    assert isinstance(result, ContextualizeResult)
    assert result.uns_topic == "uniza/zilina/factory1/line_a/capper/torque_sensor_01/torque"
    assert result.semantic_type == "torque.nm"
    assert "CapperTorqueSensor01" in result.sensor_uri
    assert "Capper" in result.feature_uri
    assert "Torque" in result.property_uri


def test_contextualize_unknown_node_returns_none(ontology_path: Path) -> None:
    from uns_auto_contextualizer.service import load_ontology, contextualize

    graph = load_ontology(ontology_path)
    result = contextualize(_normalized(node_id="Unknown.Node"), graph)
    assert result is None


def test_handle_request_ok(ontology_path: Path) -> None:
    from uns_auto_contextualizer.service import load_ontology, handle_contextualize_request

    graph = load_ontology(ontology_path)
    req = NATSEnvelope(
        correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A",
        payload=_normalized().model_dump(mode="json"),
    )
    reply = handle_contextualize_request(req, graph)
    assert reply.status == "ok"
    assert reply.payload is not None
    assert reply.payload["uns_topic"] == "uniza/zilina/factory1/line_a/capper/torque_sensor_01/torque"
    assert "sensor_uri" in reply.payload


def test_handle_request_unknown_node_returns_error(ontology_path: Path) -> None:
    from uns_auto_contextualizer.service import load_ontology, handle_contextualize_request

    graph = load_ontology(ontology_path)
    req = NATSEnvelope(
        correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A",
        payload=_normalized(node_id="Nope.Node").model_dump(mode="json"),
    )
    reply = handle_contextualize_request(req, graph)
    assert reply.status == "error"
    assert reply.error is not None
    assert reply.error.kind == "UnknownNode"
