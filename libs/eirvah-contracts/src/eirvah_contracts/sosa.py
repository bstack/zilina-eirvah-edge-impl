"""SSN/SOSA observation model — replaces TelemetryPayload as the MQTT wire format."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from eirvah_contracts.signals import Quality, SignalValue, SignalValueType, SourcePosition

_CONTEXT = {
    "sosa": "http://www.w3.org/ns/sosa/",
    "ssn": "http://www.w3.org/ns/ssn/",
    "eirvah": "https://eirvah.uniza/ontology/",
}


class SOSAObservation(BaseModel):
    """A sosa:Observation — the MQTT UNS payload format.

    Replaces TelemetryPayload v1.0. Serialise with to_jsonld(); consumers
    read the value via get_value() for a stable accessor.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0"] = "1.0"
    value_type: SignalValueType = "double"
    made_by_sensor: str  # eirvah: URI e.g. "eirvah:TorqueSensor01"
    has_feature_of_interest: str  # eirvah: URI e.g. "eirvah:Capper"
    observed_property: str  # eirvah: URI e.g. "eirvah:Torque"
    has_simple_result: SignalValue
    result_time: datetime
    unit: str
    quality: Quality
    position: SourcePosition | None = None
    correlation_id: str

    def to_jsonld(self) -> dict[str, Any]:
        doc: dict[str, Any] = {
            "@context": _CONTEXT,
            "@type": "sosa:Observation",
            "eirvah:schemaVersion": self.schema_version,
            "eirvah:valueType": self.value_type,
            "sosa:madeBySensor": {"@id": self.made_by_sensor},
            "sosa:hasFeatureOfInterest": {"@id": self.has_feature_of_interest},
            "sosa:observedProperty": {"@id": self.observed_property},
            "sosa:hasSimpleResult": self.has_simple_result,
            "sosa:resultTime": self.result_time.isoformat(),
            "eirvah:unit": self.unit,
            "eirvah:quality": self.quality,
            "eirvah:correlationId": self.correlation_id,
        }

        if self.position is not None:
            doc["eirvah:position"] = self.position.model_dump(mode="json")
        return doc

    def get_value(self) -> SignalValue:
        return self.has_simple_result

    @classmethod
    def from_jsonld(cls, doc: dict[str, Any]) -> SOSAObservation:
        return cls(
            schema_version=doc["eirvah:schemaVersion"],
            value_type=doc["eirvah:valueType"],
            position=SourcePosition.model_validate(doc["eirvah:position"])
            if doc.get("eirvah:position") is not None
            else None,
            made_by_sensor=doc["sosa:madeBySensor"]["@id"],
            has_feature_of_interest=doc["sosa:hasFeatureOfInterest"]["@id"],
            observed_property=doc["sosa:observedProperty"]["@id"],
            has_simple_result=doc["sosa:hasSimpleResult"],
            result_time=datetime.fromisoformat(doc["sosa:resultTime"]),
            unit=doc["eirvah:unit"],
            quality=doc["eirvah:quality"],
            correlation_id=doc["eirvah:correlationId"],
        )
