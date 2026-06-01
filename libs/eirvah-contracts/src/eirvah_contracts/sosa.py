"""SSN/SOSA observation model — replaces TelemetryPayload as the MQTT wire format."""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict

from eirvah_contracts.signals import Quality, SignalValue

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

    made_by_sensor: str           # eirvah: URI e.g. "eirvah:TorqueSensor01"
    has_feature_of_interest: str  # eirvah: URI e.g. "eirvah:Capper"
    observed_property: str        # eirvah: URI e.g. "eirvah:Torque"
    has_simple_result: SignalValue
    result_time: datetime
    unit: str
    quality: Quality
    correlation_id: str

    def to_jsonld(self) -> dict:
        return {
            "@context": _CONTEXT,
            "@type": "sosa:Observation",
            "sosa:madeBySensor": {"@id": self.made_by_sensor},
            "sosa:hasFeatureOfInterest": {"@id": self.has_feature_of_interest},
            "sosa:observedProperty": {"@id": self.observed_property},
            "sosa:hasSimpleResult": self.has_simple_result,
            "sosa:resultTime": self.result_time.isoformat(),
            "eirvah:unit": self.unit,
            "eirvah:quality": self.quality,
            "eirvah:correlationId": self.correlation_id,
        }

    def get_value(self) -> SignalValue:
        return self.has_simple_result

    @classmethod
    def from_jsonld(cls, doc: dict) -> "SOSAObservation":
        return cls(
            made_by_sensor=doc["sosa:madeBySensor"]["@id"],
            has_feature_of_interest=doc["sosa:hasFeatureOfInterest"]["@id"],
            observed_property=doc["sosa:observedProperty"]["@id"],
            has_simple_result=doc["sosa:hasSimpleResult"],
            result_time=datetime.fromisoformat(doc["sosa:resultTime"]),
            unit=doc["eirvah:unit"],
            quality=doc["eirvah:quality"],
            correlation_id=doc["eirvah:correlationId"],
        )
