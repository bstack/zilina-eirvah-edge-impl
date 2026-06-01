from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from eirvah_contracts.pipeline import PublishRequest
from eirvah_contracts.sosa import SOSAObservation
from eirvah_contracts.uns import UNSPath


def _uns() -> UNSPath:
    return UNSPath(
        enterprise="uniza", site="zilina", area="factory1",
        line="line_a", cell="bottler",
        equipment="temperature_sensor_01", measurement="temperature",
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
        sensor_uri="https://eirvah.uniza/ontology/BottlerTemperatureSensor01",
        feature_uri="https://eirvah.uniza/ontology/Bottler",
        property_uri="https://eirvah.uniza/ontology/Temperature",
    )


def test_build_sosa_observation() -> None:
    from mqtt_uns_publisher.service import build_sosa_observation

    req = _pub_request()
    obs = build_sosa_observation(req)
    assert isinstance(obs, SOSAObservation)
    assert obs.has_simple_result == 23.4
    assert obs.unit == "degC"
    assert obs.quality == "good"
    assert obs.correlation_id == req.correlation_id
    assert "BottlerTemperatureSensor01" in obs.made_by_sensor
    assert "Bottler" in obs.has_feature_of_interest
    assert "Temperature" in obs.observed_property


def test_build_sosa_observation_jsonld() -> None:
    from mqtt_uns_publisher.service import build_sosa_observation

    req = _pub_request()
    obs = build_sosa_observation(req)
    doc = obs.to_jsonld()
    assert doc["@type"] == "sosa:Observation"
    assert doc["sosa:hasSimpleResult"] == 23.4
    assert "@context" in doc
    # Verify it round-trips
    payload_bytes = json.dumps(doc).encode()
    parsed = json.loads(payload_bytes)
    assert parsed["sosa:hasSimpleResult"] == 23.4
