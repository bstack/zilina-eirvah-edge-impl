"""Version and semantic binding are checked independently from freshness."""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from eirvah_contracts.sosa import SOSAObservation

ROOT = Path(__file__).resolve().parents[2]


def observation(binding):
    return SOSAObservation(
        made_by_sensor=binding["sensor"],
        has_feature_of_interest=binding["feature"],
        observed_property=binding["property"],
        has_simple_result=0.4,
        result_time=datetime.now(UTC),
        unit=binding["unit"],
        quality="good",
        correlation_id="01ARZ3NDEKTSV4RRFFQ69G5FAV",
    ).to_jsonld()


def test_ontology_binding_and_versioned_observation_roundtrip():
    from eirvah_contracts.observation import load_bindings, validate_observation

    bindings = load_bindings(ROOT / "config/eirvah-line-a.jsonld")
    topic = "uniza/zilina/factory1/line_a/conveyor/belt_01/speed_setpoint"
    packet = observation(bindings[topic])

    accepted = validate_observation(topic, packet, bindings)

    assert packet["eirvah:schemaVersion"] == "1.0"
    assert packet["eirvah:valueType"] == "double"
    assert accepted.has_simple_result == 0.4


@pytest.mark.parametrize(
    "change",
    [
        "version",
        "missing_version",
        "unit",
        "sensor",
        "feature",
        "property",
        "type",
        "bool_as_number",
        "naive_time",
        "topic",
    ],
)
def test_receiver_rejects_wrong_version_or_meaning(change):
    from eirvah_contracts.observation import load_bindings, validate_observation

    bindings = load_bindings(ROOT / "config/eirvah-line-a.jsonld")
    topic = "uniza/zilina/factory1/line_a/conveyor/belt_01/speed_setpoint"
    packet = observation(bindings[topic])
    if change == "version":
        packet["eirvah:schemaVersion"] = "2.0"
    elif change == "missing_version":
        del packet["eirvah:schemaVersion"]
    elif change == "unit":
        packet["eirvah:unit"] = "rpm"
    elif change == "sensor":
        packet["sosa:madeBySensor"]["@id"] = "wrong"
    elif change == "feature":
        packet["sosa:hasFeatureOfInterest"]["@id"] = "wrong"
    elif change == "property":
        packet["sosa:observedProperty"]["@id"] = "wrong"
    elif change == "type":
        packet["eirvah:valueType"] = "int64"
    elif change == "bool_as_number":
        packet["sosa:hasSimpleResult"] = True
    elif change == "naive_time":
        packet["sosa:resultTime"] = "2026-11-06T12:00:00"
    else:
        topic = topic.replace("conveyor", "other")

    with pytest.raises((ValueError, KeyError)):
        validate_observation(topic, packet, bindings)
