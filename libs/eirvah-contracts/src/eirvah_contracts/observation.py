"""Selected public UNS bindings derived from the declared SSN/SOSA ontology.

Contract meaning is checked separately from freshness, ordering and delivery.
This validates declared identity, not cryptographic authenticity of the publisher.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

from eirvah_contracts.inspection import BottleInspection
from eirvah_contracts.sosa import SOSAObservation
from eirvah_contracts.ulid import is_valid_correlation_id
from eirvah_contracts.uns import parse_uns_topic


def load_bindings(path: Path) -> dict[str, dict[str, Any]]:
    document = json.loads(path.read_text(encoding="utf-8"))
    nodes = {n["@id"]: n for n in document["@graph"]}
    bindings = {}

    def expand(value: str) -> str:
        if value.startswith("eirvah:"):
            return str(document["@context"]["eirvah"]) + value.split(":", 1)[1]
        return value

    for node in document["@graph"]:
        if "eirvah:nodeId" not in node:
            continue
        feature = (
            nodes[node["sosa:isHostedBy"]["@id"]]
            if isinstance(node["sosa:isHostedBy"], dict)
            else nodes[node["sosa:isHostedBy"]]
        )
        prop = (
            nodes[node["sosa:observes"]["@id"]]
            if isinstance(node["sosa:observes"], dict)
            else nodes[node["sosa:observes"]]
        )
        topic = "/".join(
            [str(feature["eirvah:" + k]) for k in ("enterprise", "site", "area", "line", "cell")]
            + [node["eirvah:equipment"], node["eirvah:measurement"]]
        )
        parse_uns_topic(topic)
        if topic in bindings:
            raise ValueError("ambiguous ontology topic")
        bindings[topic] = dict(
            node=node["eirvah:nodeId"],
            sensor=expand(node["@id"]),
            feature=expand(feature["@id"]),
            property=expand(prop["@id"]),
            unit=prop["eirvah:unit"],
            value_type=prop["eirvah:valueType"],
            version="1.0",
            flow="event"
            if node["eirvah:measurement"] == "bottle_inspection"
            else "industrial_state",
        )
    if not bindings:
        raise ValueError("empty observation catalogue")
    return bindings


def validate_observation(
    topic: str, packet: dict[str, Any], bindings: dict[str, dict[str, Any]]
) -> SOSAObservation:
    binding = bindings[topic]
    if packet.get("@type") != "sosa:Observation":
        raise ValueError("wrong observation kind")
    observation = SOSAObservation.from_jsonld(packet)
    if observation.result_time.tzinfo is None or not is_valid_correlation_id(
        observation.correlation_id
    ):
        raise ValueError("invalid observation time or identity")
    if any(
        a != b
        for a, b in (
            (observation.made_by_sensor, binding["sensor"]),
            (observation.has_feature_of_interest, binding["feature"]),
            (observation.observed_property, binding["property"]),
            (observation.unit, binding["unit"]),
            (observation.value_type, binding["value_type"]),
        )
    ):
        raise ValueError("observation namespace or contract meaning mismatch")
    value = packet["sosa:hasSimpleResult"]
    types = {"double": (int, float), "int64": (int,), "bool": (bool,), "string": (str,)}
    if type(value) not in types[binding["value_type"]] or (
        type(value) in (int, float) and not math.isfinite(value)
    ):
        raise ValueError("observation value type mismatch")
    if binding["flow"] == "event":
        event = BottleInspection.model_validate_json(value)
        if event.inspected_at != observation.result_time:
            raise ValueError("inspection time binding mismatch")
    return observation
