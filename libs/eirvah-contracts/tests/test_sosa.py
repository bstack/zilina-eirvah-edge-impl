from __future__ import annotations

from datetime import UTC, datetime

import pytest


def test_sosa_observation_to_jsonld() -> None:
    from eirvah_contracts.sosa import SOSAObservation

    now = datetime.now(UTC)
    obs = SOSAObservation(
        made_by_sensor="eirvah:TorqueSensor01",
        has_feature_of_interest="eirvah:Capper",
        observed_property="eirvah:Torque",
        has_simple_result=2.5,
        result_time=now,
        unit="Nm",
        quality="good",
        correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A",
    )
    doc = obs.to_jsonld()
    assert doc["@type"] == "sosa:Observation"
    assert doc["sosa:madeBySensor"] == {"@id": "eirvah:TorqueSensor01"}
    assert doc["sosa:hasFeatureOfInterest"] == {"@id": "eirvah:Capper"}
    assert doc["sosa:observedProperty"] == {"@id": "eirvah:Torque"}
    assert doc["sosa:hasSimpleResult"] == 2.5
    assert doc["eirvah:unit"] == "Nm"
    assert doc["eirvah:quality"] == "good"
    assert doc["eirvah:correlationId"] == "01HZXC8P9G7Q3M6V0K2T8R5W4A"
    assert "@context" in doc
    assert doc["@context"]["sosa"] == "http://www.w3.org/ns/sosa/"


def test_sosa_observation_get_value() -> None:
    from eirvah_contracts.sosa import SOSAObservation

    now = datetime.now(UTC)
    obs = SOSAObservation(
        made_by_sensor="eirvah:TorqueSensor01",
        has_feature_of_interest="eirvah:Capper",
        observed_property="eirvah:Torque",
        has_simple_result=42,
        result_time=now,
        unit="dimensionless",
        quality="good",
        correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A",
    )
    assert obs.get_value() == 42


def test_sosa_observation_from_jsonld() -> None:
    from eirvah_contracts.sosa import SOSAObservation

    now = datetime.now(UTC)
    obs = SOSAObservation(
        made_by_sensor="eirvah:TorqueSensor01",
        has_feature_of_interest="eirvah:Capper",
        observed_property="eirvah:Torque",
        has_simple_result=2.5,
        result_time=now,
        unit="Nm",
        quality="good",
        correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A",
    )
    doc = obs.to_jsonld()
    recovered = SOSAObservation.from_jsonld(doc)
    assert recovered.has_simple_result == pytest.approx(2.5)
    assert recovered.get_value() == pytest.approx(2.5)
    assert recovered.correlation_id == "01HZXC8P9G7Q3M6V0K2T8R5W4A"
