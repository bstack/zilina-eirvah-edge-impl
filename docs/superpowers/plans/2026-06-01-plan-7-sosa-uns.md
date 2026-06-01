# SSN/SOSA Ontology-Driven UNS Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the hand-coded ISA-95 mapping YAML with a JSON-LD ontology (SSN/SOSA), replacing MQTT payloads from `TelemetryPayload v1.0` to standards-compliant `sosa:Observation` JSON-LD objects.

**Architecture:** `config/eirvah-line-a.jsonld` defines all 8 stations and 21 sensors as SSN/SOSA entities. `uns-auto-contextualizer` loads the ontology at startup via `rdflib` and resolves `node_id → UNS path + SSN/SOSA URIs` via SPARQL. These URIs flow through the pipeline to `mqtt-uns-publisher` which serialises each message as a `sosa:Observation`. The MQTT topic path (`uniza/zilina/factory1/line_a/...`) is unchanged; only the payload format changes.

**Tech Stack:** Python 3.12, `rdflib>=7.0` (BSD-2-Clause, OSI-approved), JSON-LD, W3C SSN/SOSA ontology. All existing libs unchanged.

**Spec reference:** `docs/superpowers/specs/2026-06-01-sosa-uns-design.md`

---

## File structure

```
config/eirvah-line-a.jsonld                                       NEW  full ontology — 8 stations, 21 sensors

libs/eirvah-contracts/src/eirvah_contracts/sosa.py                NEW  SOSAObservation model + to_jsonld()
libs/eirvah-contracts/src/eirvah_contracts/pipeline.py            MODIFY add sensor_uri/feature_uri/property_uri to
                                                                         ContextualizeResult + PublishRequest

services/uns-auto-contextualizer/pyproject.toml                   MODIFY add rdflib>=7.0
services/uns-auto-contextualizer/src/uns_auto_contextualizer/config.py  MODIFY ontology_path replaces mapping_path
services/uns-auto-contextualizer/src/uns_auto_contextualizer/service.py MODIFY rdflib SPARQL replaces YAML dict lookup
services/uns-auto-contextualizer/tests/test_uns_auto_contextualizer.py  MODIFY rdflib graph fixtures

services/uns-contextualizer-orchestrator/src/.../models.py        MODIFY build_publish_request() passes URI fields
services/mqtt-uns-publisher/src/mqtt_uns_publisher/service.py     MODIFY build_sosa_observation() replaces build_telemetry_payload()
services/mqtt-uns-publisher/tests/test_mqtt_uns_publisher.py      MODIFY assert JSON-LD shape

services/decision-agent-stub/src/decision_agent_stub/service.py   MODIFY payload["sosa:hasSimpleResult"]
services/decision-agent-stub/tests/test_decision_agent_stub.py    MODIFY payload fixture

deploy/k3s/base/uns-auto-contextualizer/kustomization.yaml        MODIFY mount eirvah-line-a.jsonld
deploy/k3s/base/uns-auto-contextualizer/deployment.yaml           MODIFY ONTOLOGY_PATH env var

tests/e2e/test_telemetry.py                                       MODIFY SSN/SOSA assertions
tests/e2e/test_modbus_path.py                                     MODIFY SSN/SOSA assertions
```

---

## Task 1: JSON-LD ontology file

**Files:**
- Create: `config/eirvah-line-a.jsonld`

No unit tests for this task — correctness is verified by Task 3's SPARQL queries.

- [ ] **Step 1: Create `config/eirvah-line-a.jsonld`**

```json
{
  "@context": {
    "sosa": "http://www.w3.org/ns/sosa/",
    "ssn": "http://www.w3.org/ns/ssn/",
    "eirvah": "https://eirvah.uniza/ontology/"
  },
  "@graph": [
    {"@id": "eirvah:Bottler", "@type": ["ssn:System", "sosa:FeatureOfInterest"],
     "eirvah:enterprise": "uniza", "eirvah:site": "zilina",
     "eirvah:area": "factory1", "eirvah:line": "line_a", "eirvah:cell": "bottler"},
    {"@id": "eirvah:Filler", "@type": ["ssn:System", "sosa:FeatureOfInterest"],
     "eirvah:enterprise": "uniza", "eirvah:site": "zilina",
     "eirvah:area": "factory1", "eirvah:line": "line_a", "eirvah:cell": "filler"},
    {"@id": "eirvah:Conveyor", "@type": ["ssn:System", "sosa:FeatureOfInterest"],
     "eirvah:enterprise": "uniza", "eirvah:site": "zilina",
     "eirvah:area": "factory1", "eirvah:line": "line_a", "eirvah:cell": "conveyor"},
    {"@id": "eirvah:RejectStation", "@type": ["ssn:System", "sosa:FeatureOfInterest"],
     "eirvah:enterprise": "uniza", "eirvah:site": "zilina",
     "eirvah:area": "factory1", "eirvah:line": "line_a", "eirvah:cell": "reject_station"},
    {"@id": "eirvah:Inspector", "@type": ["ssn:System", "sosa:FeatureOfInterest"],
     "eirvah:enterprise": "uniza", "eirvah:site": "zilina",
     "eirvah:area": "factory1", "eirvah:line": "line_a", "eirvah:cell": "inspector"},
    {"@id": "eirvah:Labeler", "@type": ["ssn:System", "sosa:FeatureOfInterest"],
     "eirvah:enterprise": "uniza", "eirvah:site": "zilina",
     "eirvah:area": "factory1", "eirvah:line": "line_a", "eirvah:cell": "labeler"},
    {"@id": "eirvah:Capper", "@type": ["ssn:System", "sosa:FeatureOfInterest"],
     "eirvah:enterprise": "uniza", "eirvah:site": "zilina",
     "eirvah:area": "factory1", "eirvah:line": "line_a", "eirvah:cell": "capper"},
    {"@id": "eirvah:Palletizer", "@type": ["ssn:System", "sosa:FeatureOfInterest"],
     "eirvah:enterprise": "uniza", "eirvah:site": "zilina",
     "eirvah:area": "factory1", "eirvah:line": "line_a", "eirvah:cell": "palletizer"},

    {"@id": "eirvah:Temperature",       "@type": "sosa:ObservableProperty", "eirvah:unit": "degC",          "eirvah:valueType": "double", "eirvah:semanticType": "temperature.celsius"},
    {"@id": "eirvah:Throughput",         "@type": "sosa:ObservableProperty", "eirvah:unit": "bottle/s",      "eirvah:valueType": "double", "eirvah:semanticType": "flow.bps"},
    {"@id": "eirvah:MotorState",         "@type": "sosa:ObservableProperty", "eirvah:unit": "dimensionless", "eirvah:valueType": "int64",  "eirvah:semanticType": "state.enum"},
    {"@id": "eirvah:MotorRpm",           "@type": "sosa:ObservableProperty", "eirvah:unit": "rpm",           "eirvah:valueType": "double", "eirvah:semanticType": "speed.rpm"},
    {"@id": "eirvah:SetpointTemperature","@type": "sosa:ObservableProperty", "eirvah:unit": "degC",          "eirvah:valueType": "double", "eirvah:semanticType": "setpoint.target"},
    {"@id": "eirvah:FillLevel",          "@type": "sosa:ObservableProperty", "eirvah:unit": "percent",       "eirvah:valueType": "double", "eirvah:semanticType": "level.percent"},
    {"@id": "eirvah:BeltSpeed",          "@type": "sosa:ObservableProperty", "eirvah:unit": "m/s",           "eirvah:valueType": "double", "eirvah:semanticType": "speed.ms"},
    {"@id": "eirvah:JamDetected",        "@type": "sosa:ObservableProperty", "eirvah:unit": "dimensionless", "eirvah:valueType": "int64",  "eirvah:semanticType": "state.enum"},
    {"@id": "eirvah:BottleCount",        "@type": "sosa:ObservableProperty", "eirvah:unit": "dimensionless", "eirvah:valueType": "int64",  "eirvah:semanticType": "count.cumulative"},
    {"@id": "eirvah:RejectCount",        "@type": "sosa:ObservableProperty", "eirvah:unit": "dimensionless", "eirvah:valueType": "int64",  "eirvah:semanticType": "count.cumulative"},
    {"@id": "eirvah:ConveyorActive",     "@type": "sosa:ObservableProperty", "eirvah:unit": "dimensionless", "eirvah:valueType": "int64",  "eirvah:semanticType": "state.enum"},
    {"@id": "eirvah:GoodRate",           "@type": "sosa:ObservableProperty", "eirvah:unit": "percent",       "eirvah:valueType": "double", "eirvah:semanticType": "quality.percent"},
    {"@id": "eirvah:AlignmentScore",     "@type": "sosa:ObservableProperty", "eirvah:unit": "percent",       "eirvah:valueType": "double", "eirvah:semanticType": "quality.percent"},
    {"@id": "eirvah:Torque",             "@type": "sosa:ObservableProperty", "eirvah:unit": "Nm",            "eirvah:valueType": "double", "eirvah:semanticType": "torque.nm"},
    {"@id": "eirvah:CapPresence",        "@type": "sosa:ObservableProperty", "eirvah:unit": "dimensionless", "eirvah:valueType": "int64",  "eirvah:semanticType": "state.enum"},
    {"@id": "eirvah:RejectsPerMin",      "@type": "sosa:ObservableProperty", "eirvah:unit": "dimensionless", "eirvah:valueType": "int64",  "eirvah:semanticType": "count.rate"},
    {"@id": "eirvah:LayerCount",         "@type": "sosa:ObservableProperty", "eirvah:unit": "dimensionless", "eirvah:valueType": "int64",  "eirvah:semanticType": "count.cumulative"},
    {"@id": "eirvah:PalletComplete",     "@type": "sosa:ObservableProperty", "eirvah:unit": "dimensionless", "eirvah:valueType": "int64",  "eirvah:semanticType": "state.enum"},
    {"@id": "eirvah:CyclesPerHour",      "@type": "sosa:ObservableProperty", "eirvah:unit": "dimensionless", "eirvah:valueType": "int64",  "eirvah:semanticType": "count.rate"},

    {"@id": "eirvah:BottlerTemperatureSensor01",  "@type": "sosa:Sensor", "sosa:isHostedBy": {"@id": "eirvah:Bottler"},      "sosa:observes": {"@id": "eirvah:Temperature"},       "eirvah:nodeId": "Bottler.Temperature01",                  "eirvah:equipment": "temperature_sensor_01",  "eirvah:measurement": "temperature"},
    {"@id": "eirvah:BottlerThroughputMeter01",    "@type": "sosa:Sensor", "sosa:isHostedBy": {"@id": "eirvah:Bottler"},      "sosa:observes": {"@id": "eirvah:Throughput"},        "eirvah:nodeId": "Bottler.ThroughputMeter01",              "eirvah:equipment": "throughput_meter_01",    "eirvah:measurement": "throughput"},
    {"@id": "eirvah:BottlerMotorStateSensor01",   "@type": "sosa:Sensor", "sosa:isHostedBy": {"@id": "eirvah:Bottler"},      "sosa:observes": {"@id": "eirvah:MotorState"},        "eirvah:nodeId": "Bottler.Motor01.State",                  "eirvah:equipment": "motor_01",               "eirvah:measurement": "state"},
    {"@id": "eirvah:BottlerMotorRpmSensor01",     "@type": "sosa:Sensor", "sosa:isHostedBy": {"@id": "eirvah:Bottler"},      "sosa:observes": {"@id": "eirvah:MotorRpm"},          "eirvah:nodeId": "Bottler.Motor01.Rpm",                    "eirvah:equipment": "motor_01",               "eirvah:measurement": "rpm"},
    {"@id": "eirvah:BottlerSetpointSensor01",     "@type": "sosa:Sensor", "sosa:isHostedBy": {"@id": "eirvah:Bottler"},      "sosa:observes": {"@id": "eirvah:SetpointTemperature"},"eirvah:nodeId": "Bottler.SetpointUnit.SetpointTemperature","eirvah:equipment": "setpoint_unit",          "eirvah:measurement": "setpoint_temperature"},
    {"@id": "eirvah:FillerFillLevelSensor01",     "@type": "sosa:Sensor", "sosa:isHostedBy": {"@id": "eirvah:Filler"},       "sosa:observes": {"@id": "eirvah:FillLevel"},         "eirvah:nodeId": "Filler.FillLevelSensor01",               "eirvah:equipment": "fill_level_sensor_01",   "eirvah:measurement": "fill_level"},
    {"@id": "eirvah:FillerMotorStateSensor01",    "@type": "sosa:Sensor", "sosa:isHostedBy": {"@id": "eirvah:Filler"},       "sosa:observes": {"@id": "eirvah:MotorState"},        "eirvah:nodeId": "Filler.Motor01.State",                   "eirvah:equipment": "motor_01",               "eirvah:measurement": "state"},
    {"@id": "eirvah:FillerThroughputMeter01",     "@type": "sosa:Sensor", "sosa:isHostedBy": {"@id": "eirvah:Filler"},       "sosa:observes": {"@id": "eirvah:Throughput"},        "eirvah:nodeId": "Filler.ThroughputMeter01",               "eirvah:equipment": "throughput_meter_01",    "eirvah:measurement": "throughput"},
    {"@id": "eirvah:ConveyorBeltSpeedSensor01",   "@type": "sosa:Sensor", "sosa:isHostedBy": {"@id": "eirvah:Conveyor"},     "sosa:observes": {"@id": "eirvah:BeltSpeed"},         "eirvah:nodeId": "Conveyor.Belt01.BeltSpeed",              "eirvah:equipment": "belt_01",                "eirvah:measurement": "belt_speed"},
    {"@id": "eirvah:ConveyorJamSensor01",         "@type": "sosa:Sensor", "sosa:isHostedBy": {"@id": "eirvah:Conveyor"},     "sosa:observes": {"@id": "eirvah:JamDetected"},       "eirvah:nodeId": "Conveyor.Belt01.JamDetected",            "eirvah:equipment": "belt_01",                "eirvah:measurement": "jam_detected"},
    {"@id": "eirvah:ConveyorBottleCounter01",     "@type": "sosa:Sensor", "sosa:isHostedBy": {"@id": "eirvah:Conveyor"},     "sosa:observes": {"@id": "eirvah:BottleCount"},       "eirvah:nodeId": "Conveyor.Belt01.BottleCount",            "eirvah:equipment": "belt_01",                "eirvah:measurement": "bottle_count"},
    {"@id": "eirvah:RejectStationRejectCounter01","@type": "sosa:Sensor", "sosa:isHostedBy": {"@id": "eirvah:RejectStation"},"sosa:observes": {"@id": "eirvah:RejectCount"},       "eirvah:nodeId": "RejectStation.RejectCounter01",          "eirvah:equipment": "reject_counter_01",      "eirvah:measurement": "reject_count"},
    {"@id": "eirvah:RejectStationConveyorSensor01","@type":"sosa:Sensor", "sosa:isHostedBy": {"@id": "eirvah:RejectStation"},"sosa:observes": {"@id": "eirvah:ConveyorActive"},    "eirvah:nodeId": "RejectStation.ConveyorActive01",         "eirvah:equipment": "conveyor_01",            "eirvah:measurement": "conveyor_active"},
    {"@id": "eirvah:InspectorGoodRateSensor01",  "@type": "sosa:Sensor", "sosa:isHostedBy": {"@id": "eirvah:Inspector"},    "sosa:observes": {"@id": "eirvah:GoodRate"},          "eirvah:nodeId": "Inspector.Inspector01.GoodRate",         "eirvah:equipment": "inspector_01",           "eirvah:measurement": "good_rate"},
    {"@id": "eirvah:LabelerAlignmentSensor01",   "@type": "sosa:Sensor", "sosa:isHostedBy": {"@id": "eirvah:Labeler"},      "sosa:observes": {"@id": "eirvah:AlignmentScore"},    "eirvah:nodeId": "Labeler.Labeler01.AlignmentScore",       "eirvah:equipment": "labeler_01",             "eirvah:measurement": "alignment_score"},
    {"@id": "eirvah:CapperTorqueSensor01",       "@type": "sosa:Sensor", "sosa:isHostedBy": {"@id": "eirvah:Capper"},      "sosa:observes": {"@id": "eirvah:Torque"},            "eirvah:nodeId": "Capper.TorqueSensor01",                  "eirvah:equipment": "torque_sensor_01",       "eirvah:measurement": "torque"},
    {"@id": "eirvah:CapperCapSensor01",          "@type": "sosa:Sensor", "sosa:isHostedBy": {"@id": "eirvah:Capper"},      "sosa:observes": {"@id": "eirvah:CapPresence"},       "eirvah:nodeId": "Capper.CapSensor01",                     "eirvah:equipment": "cap_sensor_01",          "eirvah:measurement": "cap_presence"},
    {"@id": "eirvah:CapperRejectCounter01",      "@type": "sosa:Sensor", "sosa:isHostedBy": {"@id": "eirvah:Capper"},      "sosa:observes": {"@id": "eirvah:RejectsPerMin"},     "eirvah:nodeId": "Capper.RejectCounter01",                 "eirvah:equipment": "reject_counter_01",      "eirvah:measurement": "rejects_per_min"},
    {"@id": "eirvah:PalletizerLayerCounter01",   "@type": "sosa:Sensor", "sosa:isHostedBy": {"@id": "eirvah:Palletizer"},  "sosa:observes": {"@id": "eirvah:LayerCount"},        "eirvah:nodeId": "Palletizer.LayerCounter01",              "eirvah:equipment": "layer_counter_01",       "eirvah:measurement": "layer_count"},
    {"@id": "eirvah:PalletizerPalletSensor01",   "@type": "sosa:Sensor", "sosa:isHostedBy": {"@id": "eirvah:Palletizer"},  "sosa:observes": {"@id": "eirvah:PalletComplete"},    "eirvah:nodeId": "Palletizer.PalletSensor01",              "eirvah:equipment": "pallet_sensor_01",       "eirvah:measurement": "pallet_complete"},
    {"@id": "eirvah:PalletizerCycleCounter01",   "@type": "sosa:Sensor", "sosa:isHostedBy": {"@id": "eirvah:Palletizer"},  "sosa:observes": {"@id": "eirvah:CyclesPerHour"},    "eirvah:nodeId": "Palletizer.CycleCounter01",              "eirvah:equipment": "cycle_counter_01",       "eirvah:measurement": "cycles_per_hr"}
  ]
}
```

- [ ] **Step 2: Validate JSON syntax**

```bash
python3 -c "import json; json.load(open('config/eirvah-line-a.jsonld')); print('JSON valid')"
```

Expected: `JSON valid`

- [ ] **Step 3: Commit**

```bash
git add config/eirvah-line-a.jsonld
git commit -m "feat(ontology): add SSN/SOSA JSON-LD ontology for all 8 bottling line stations"
```

---

## Task 2: SOSAObservation contract + update pipeline contracts

**Files:**
- Create: `libs/eirvah-contracts/src/eirvah_contracts/sosa.py`
- Modify: `libs/eirvah-contracts/src/eirvah_contracts/pipeline.py`

- [ ] **Step 1: Write failing tests**

```python
# libs/eirvah-contracts/tests/test_sosa.py
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
```

- [ ] **Step 2: Run — verify FAIL**

```bash
uv run pytest libs/eirvah-contracts/tests/test_sosa.py -v 2>&1 | head -10
```

Expected: `ModuleNotFoundError: No module named 'eirvah_contracts.sosa'`

- [ ] **Step 3: Create `libs/eirvah-contracts/src/eirvah_contracts/sosa.py`**

```python
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
    """A sosa:Observation — the new MQTT UNS payload format.

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
        from datetime import datetime
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
```

- [ ] **Step 4: Add 3 URI fields to `ContextualizeResult` and `PublishRequest` in `libs/eirvah-contracts/src/eirvah_contracts/pipeline.py`**

In `ContextualizeResult`, add after `semantic_type`:
```python
    sensor_uri: str    # e.g. "eirvah:TorqueSensor01"
    feature_uri: str   # e.g. "eirvah:Capper"
    property_uri: str  # e.g. "eirvah:Torque"
```

In `PublishRequest`, add after `edge_ingress`:
```python
    sensor_uri: str
    feature_uri: str
    property_uri: str
```

- [ ] **Step 5: Run tests — verify ALL PASS**

```bash
uv run pytest libs/eirvah-contracts/tests/test_sosa.py -v
```

Expected: `3 passed`.

- [ ] **Step 6: Commit**

```bash
git add libs/eirvah-contracts/
git commit -m "feat(contracts): add SOSAObservation model; add sensor/feature/property URIs to pipeline contracts"
```

---

## Task 3: uns-auto-contextualizer — rdflib + SPARQL

**Files:**
- Modify: `services/uns-auto-contextualizer/pyproject.toml`
- Modify: `services/uns-auto-contextualizer/src/uns_auto_contextualizer/config.py`
- Modify: `services/uns-auto-contextualizer/src/uns_auto_contextualizer/service.py`
- Modify: `services/uns-auto-contextualizer/tests/test_uns_auto_contextualizer.py`
- Modify: `deploy/k3s/base/uns-auto-contextualizer/kustomization.yaml`
- Modify: `deploy/k3s/base/uns-auto-contextualizer/deployment.yaml`

- [ ] **Step 1: Update tests**

Replace the full content of `services/uns-auto-contextualizer/tests/test_uns_auto_contextualizer.py`:

```python
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
    """Minimal ontology with one sensor for testing."""
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
    assert result.sensor_uri == "https://eirvah.uniza/ontology/CapperTorqueSensor01"
    assert result.feature_uri == "https://eirvah.uniza/ontology/Capper"
    assert result.property_uri == "https://eirvah.uniza/ontology/Torque"


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
```

- [ ] **Step 2: Run — verify FAIL**

```bash
uv run pytest services/uns-auto-contextualizer/tests/test_uns_auto_contextualizer.py -v 2>&1 | tail -8
```

Expected: `ImportError` — `load_ontology` not defined yet.

- [ ] **Step 3: Add `rdflib>=7.0` to `services/uns-auto-contextualizer/pyproject.toml`**

In the `dependencies` list, add after `pyyaml>=6.0`:
```toml
    "rdflib>=7.0",
```

Run `uv lock` to update the lock file:
```bash
uv lock
```

- [ ] **Step 4: Update config — add `ontology_path`, remove `mapping_path`**

Replace the full content of `services/uns-auto-contextualizer/src/uns_auto_contextualizer/config.py`:

```python
"""Settings for the UNS auto-contextualizer worker."""
from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AutoContextualizerSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="UNS_AUTO_CONTEXTUALIZER_",
        env_file=None,
        extra="ignore",
    )

    nats_servers: list[str] = ["nats://nats:4222"]
    ontology_path: Path = Path("/etc/uns-auto-contextualizer/eirvah-line-a.jsonld")
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
```

Note: `enterprise` and `site` are now read from the ontology graph, not from env vars.

- [ ] **Step 5: Rewrite `service.py` — replace YAML lookup with rdflib SPARQL**

Replace the full content of `services/uns-auto-contextualizer/src/uns_auto_contextualizer/service.py`:

```python
"""UNS auto-contextualizer NATS req/rep worker — ontology-driven (SSN/SOSA)."""
from __future__ import annotations

import asyncio
from pathlib import Path

import structlog
import uvicorn
from eirvah_bus.client import BusClient
from eirvah_bus.consumer import subscribe_queue_group
from eirvah_contracts.envelope import EnvelopeError, NATSEnvelope
from eirvah_contracts.pipeline import ContextualizeResult
from eirvah_contracts.signals import NormalizedSignalEnvelope
from eirvah_contracts.uns import UNSPath, build_uns_topic
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import configure_logging
from eirvah_observability.metrics import make_counter
from nats.aio.msg import Msg
from rdflib import ConjunctiveGraph, Graph
from rdflib.term import Literal

from uns_auto_contextualizer.config import AutoContextualizerSettings

_log = structlog.get_logger("uns-auto-contextualizer")
SUBJECT = "uns.work.contextualize"

_SPARQL = """
PREFIX sosa: <http://www.w3.org/ns/sosa/>
PREFIX ssn:  <http://www.w3.org/ns/ssn/>
PREFIX eirvah: <https://eirvah.uniza/ontology/>

SELECT ?sensor ?feature ?property
       ?enterprise ?site ?area ?line ?cell ?equipment ?measurement
       ?unit ?semanticType
WHERE {
  ?sensor eirvah:nodeId ?nodeId .
  ?sensor sosa:isHostedBy ?feature .
  ?sensor sosa:observes ?property .
  ?sensor eirvah:equipment ?equipment .
  ?sensor eirvah:measurement ?measurement .
  ?feature eirvah:enterprise ?enterprise .
  ?feature eirvah:site ?site .
  ?feature eirvah:area ?area .
  ?feature eirvah:line ?line .
  ?feature eirvah:cell ?cell .
  ?property eirvah:unit ?unit .
  ?property eirvah:semanticType ?semanticType .
}
"""


def load_ontology(path: Path) -> Graph:
    g: Graph = ConjunctiveGraph()
    g.parse(str(path), format="json-ld")
    return g


def contextualize(
    normalized: NormalizedSignalEnvelope,
    graph: Graph,
) -> ContextualizeResult | None:
    results = list(graph.query(_SPARQL, initBindings={"nodeId": Literal(normalized.node_id)}))
    if not results:
        return None
    row = results[0]
    path = UNSPath(
        enterprise=str(row.enterprise),
        site=str(row.site),
        area=str(row.area),
        line=str(row.line),
        cell=str(row.cell),
        equipment=str(row.equipment),
        measurement=str(row.measurement),
    )
    return ContextualizeResult(
        uns_topic=build_uns_topic(path),
        uns_path=path,
        semantic_type=str(row.semanticType),
        sensor_uri=str(row.sensor),
        feature_uri=str(row.feature),
        property_uri=str(row.property),
    )


def handle_contextualize_request(
    envelope: NATSEnvelope,
    graph: Graph,
) -> NATSEnvelope:
    try:
        normalized = NormalizedSignalEnvelope.model_validate(envelope.payload)
        result = contextualize(normalized, graph)
        if result is None:
            return NATSEnvelope(
                correlation_id=envelope.correlation_id,
                status="error",
                error=EnvelopeError(
                    kind="UnknownNode",
                    message=f"no ontology entry for node_id {normalized.node_id!r}",
                ),
            )
        return NATSEnvelope(
            correlation_id=envelope.correlation_id,
            payload=result.model_dump(mode="json"),
        )
    except Exception as exc:
        return NATSEnvelope(
            correlation_id=envelope.correlation_id,
            status="error",
            error=EnvelopeError(kind=type(exc).__name__, message=str(exc)[:200]),
        )


class AutoContextualizerWorker:
    def __init__(self, settings: AutoContextualizerSettings) -> None:
        self._settings = settings
        self._graph: Graph | None = None
        self._bus: BusClient | None = None
        self._ready = False
        self._handled = make_counter(
            "worker_handler_total",
            "Worker handler invocations",
            labelnames=["worker", "outcome"],
        )

    def is_ready(self) -> bool:
        return self._ready

    async def run(self) -> None:
        self._graph = load_ontology(self._settings.ontology_path)
        node_count = sum(1 for _ in self._graph.subjects())
        self._bus = BusClient(
            servers=self._settings.nats_servers,
            name="uns-auto-contextualizer",
        )
        await self._bus.connect()
        await subscribe_queue_group(nc=self._bus.nc, subject=SUBJECT, handler=self._handle)
        self._ready = True
        _log.info("contextualizer_ready", subject=SUBJECT, ontology_nodes=node_count)
        await asyncio.get_event_loop().create_future()

    async def _handle(self, msg: Msg) -> None:
        try:
            envelope = NATSEnvelope.model_validate_json(msg.data)
        except Exception as exc:
            _log.warning("invalid_envelope", error=str(exc))
            return
        assert self._graph is not None
        reply = handle_contextualize_request(envelope, self._graph)
        self._handled.labels(worker="uns-auto-contextualizer", outcome=reply.status).inc()
        await msg.respond(reply.model_dump_json().encode())


async def run(settings: AutoContextualizerSettings) -> None:
    configure_logging(level=settings.log_level)
    worker = AutoContextualizerWorker(settings)
    health = HealthApp(is_ready=worker.is_ready)
    http_cfg = uvicorn.Config(
        health.asgi,
        host="0.0.0.0",
        port=settings.http_port,
        log_level=settings.log_level.lower(),
    )
    http = uvicorn.Server(http_cfg)
    await asyncio.gather(worker.run(), http.serve())
```

- [ ] **Step 6: Run tests — verify ALL PASS**

```bash
uv run pytest services/uns-auto-contextualizer/tests/test_uns_auto_contextualizer.py -v
```

Expected: `4 passed`.

- [ ] **Step 7: Update kustomization — mount JSON-LD file**

Replace full content of `deploy/k3s/base/uns-auto-contextualizer/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - deployment.yaml
  - service.yaml
configMapGenerator:
  - name: uns-auto-contextualizer-config
    files:
      - eirvah-line-a.yaml=../../../config/eirvah-line-a.jsonld
```

Wait — kustomize configMapGenerator reads local files. Since the ontology is in `config/` (not in this directory), use a symlink or copy. The cleaner approach: copy the ontology into the k8s directory so kustomize can reference it directly.

Instead, copy the file reference using a relative path from the kustomization dir. Kustomize supports `files` with relative paths:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - deployment.yaml
  - service.yaml
configMapGenerator:
  - name: uns-auto-contextualizer-config
    files:
      - eirvah-line-a.jsonld=../../../config/eirvah-line-a.jsonld
```

- [ ] **Step 8: Update deployment — ONTOLOGY_PATH env var, remove ENTERPRISE/SITE**

In `deploy/k3s/base/uns-auto-contextualizer/deployment.yaml`, replace the `env:` section with:

```yaml
          env:
            - name: UNS_AUTO_CONTEXTUALIZER_NATS_SERVERS
              value: '["nats://nats:4222"]'
            - name: UNS_AUTO_CONTEXTUALIZER_ONTOLOGY_PATH
              value: "/etc/uns-auto-contextualizer/eirvah-line-a.jsonld"
```

- [ ] **Step 9: Verify kustomize renders**

```bash
kubectl kustomize deploy/k3s/overlays/local 2>&1 | grep -A3 "uns-auto-contextualizer-config"
```

Expected: configmap entry shows `eirvah-line-a.jsonld`.

- [ ] **Step 10: Commit**

```bash
git add services/uns-auto-contextualizer/ \
        deploy/k3s/base/uns-auto-contextualizer/ \
        uv.lock
git commit -m "feat(uns-auto-contextualizer): replace YAML mapping with rdflib SPARQL on SSN/SOSA ontology"
```

---

## Task 4: Update uns-contextualizer-orchestrator models

The orchestrator's `PipelineContext.build_publish_request()` assembles a `PublishRequest` from normalized + contextualized results. It needs to pass through the three new URI fields.

**Files:**
- Modify: `services/uns-contextualizer-orchestrator/src/uns_contextualizer_orchestrator/models.py`

- [ ] **Step 1: Read `models.py` to find `build_publish_request()`**

```bash
cat services/uns-contextualizer-orchestrator/src/uns_contextualizer_orchestrator/models.py
```

Find the `build_publish_request` method. It currently constructs `PublishRequest` from `self.normalized` and `self.contextualized`. Add `sensor_uri`, `feature_uri`, `property_uri` to the `PublishRequest(...)` call:

```python
    def build_publish_request(self) -> PublishRequest:
        assert self.normalized is not None and self.contextualized is not None
        return PublishRequest(
            uns_topic=self.contextualized.uns_topic,
            correlation_id=self.correlation_id,
            value=self.normalized.value,
            value_type=self.normalized.value_type,
            unit=self.normalized.unit,
            quality=self.normalized.quality,
            semantic_type=self.contextualized.semantic_type,
            uns_path=self.contextualized.uns_path,
            source_endpoint=self.raw.source_endpoint,
            source_node_id=self.raw.node_id,
            source_timestamp=self.raw.source_timestamp,
            edge_ingress=self.ingress_at,
            sensor_uri=self.contextualized.sensor_uri,
            feature_uri=self.contextualized.feature_uri,
            property_uri=self.contextualized.property_uri,
        )
```

- [ ] **Step 2: Run orchestrator tests**

```bash
uv run pytest services/uns-contextualizer-orchestrator/tests/ -v 2>&1 | tail -8
```

Expected: all pass (adding required fields to PublishRequest may break existing test fixtures — fix any failing tests by adding `sensor_uri`, `feature_uri`, `property_uri` to `PublishRequest(...)` calls in test fixtures with placeholder strings like `"eirvah:TestSensor"`, `"eirvah:TestFeature"`, `"eirvah:TestProperty"`).

- [ ] **Step 3: Commit**

```bash
git add services/uns-contextualizer-orchestrator/
git commit -m "feat(orchestrator): pass SSN/SOSA URIs through pipeline to publish stage"
```

---

## Task 5: mqtt-uns-publisher — JSON-LD output

**Files:**
- Modify: `services/mqtt-uns-publisher/src/mqtt_uns_publisher/service.py`
- Modify: `services/mqtt-uns-publisher/tests/test_mqtt_uns_publisher.py` (if it exists)

- [ ] **Step 1: Update `service.py` — replace `build_telemetry_payload` with `build_sosa_observation`**

In `services/mqtt-uns-publisher/src/mqtt_uns_publisher/service.py`:

Replace the import:
```python
from eirvah_contracts.telemetry import TelemetryPayload, TelemetrySource, TelemetryTimestamps
```
with:
```python
from eirvah_contracts.sosa import SOSAObservation
```

Replace the `build_telemetry_payload` function:
```python
def build_sosa_observation(req: PublishRequest) -> SOSAObservation:
    return SOSAObservation(
        made_by_sensor=req.sensor_uri,
        has_feature_of_interest=req.feature_uri,
        observed_property=req.property_uri,
        has_simple_result=req.value,
        result_time=req.source_timestamp,
        unit=req.unit,
        quality=req.quality,
        correlation_id=req.correlation_id,
    )
```

In `_handle`, replace:
```python
            telemetry = build_telemetry_payload(req)
            try:
                await self._mqtt_client.publish(
                    req.uns_topic,
                    payload=telemetry.model_dump_json().encode(),
```
with:
```python
            observation = build_sosa_observation(req)
            try:
                await self._mqtt_client.publish(
                    req.uns_topic,
                    payload=json.dumps(observation.to_jsonld()).encode(),
```

Add `import json` at the top of the file.

- [ ] **Step 2: Run publisher tests (if they exist)**

```bash
uv run pytest services/mqtt-uns-publisher/tests/ -v 2>&1 | tail -8
```

Fix any failures by updating `PublishRequest` fixtures to include `sensor_uri="eirvah:TestSensor"`, `feature_uri="eirvah:TestFeature"`, `property_uri="eirvah:TestProperty"`.

- [ ] **Step 3: Commit**

```bash
git add services/mqtt-uns-publisher/
git commit -m "feat(mqtt-uns-publisher): publish sosa:Observation JSON-LD instead of TelemetryPayload"
```

---

## Task 6: decision-agent-stub — read sosa:hasSimpleResult

**Files:**
- Modify: `services/decision-agent-stub/src/decision_agent_stub/service.py`

- [ ] **Step 1: Update `service.py`**

In `services/decision-agent-stub/src/decision_agent_stub/service.py`, find:

```python
                        payload = json.loads(message.payload)
                        value = float(payload["value"])
                        correlation_id = payload.get("correlation_id") or generate_correlation_id()
```

Replace with:

```python
                        payload = json.loads(message.payload)
                        value = float(payload["sosa:hasSimpleResult"])
                        correlation_id = payload.get("eirvah:correlationId") or generate_correlation_id()
```

- [ ] **Step 2: Run decision agent tests**

```bash
uv run pytest services/decision-agent-stub/tests/ -v 2>&1 | tail -8
```

If `test_decision_agent_stub.py` has payload fixtures using `{"value": ...}`, update them to `{"sosa:hasSimpleResult": ..., "eirvah:correlationId": "...", "@type": "sosa:Observation", ...}`.

- [ ] **Step 3: Commit**

```bash
git add services/decision-agent-stub/
git commit -m "fix(decision-agent-stub): read sosa:hasSimpleResult and eirvah:correlationId from JSON-LD payload"
```

---

## Task 7: e2e tests + deploy + verify

**Files:**
- Modify: `tests/e2e/test_telemetry.py`
- Modify: `tests/e2e/test_modbus_path.py`

- [ ] **Step 1: Update `test_telemetry.py`**

Replace the full content of `tests/e2e/test_telemetry.py`:

```python
"""E2E tests for the telemetry path — SSN/SOSA payload validation."""
from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any

import pytest
from eirvah_contracts.sosa import SOSAObservation
from eirvah_contracts.ulid import is_valid_correlation_id

if TYPE_CHECKING:
    from tests.e2e.conftest import EirVahCluster

pytestmark = pytest.mark.asyncio

SUBSCRIBE_TOPIC = "uniza/zilina/factory1/line_a/bottler/#"
EXPECTED_TOPICS = {
    "uniza/zilina/factory1/line_a/bottler/temperature_sensor_01/temperature",
    "uniza/zilina/factory1/line_a/bottler/throughput_meter_01/throughput",
    "uniza/zilina/factory1/line_a/bottler/motor_01/state",
    "uniza/zilina/factory1/line_a/bottler/motor_01/rpm",
    "uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature",
}


async def _collect_messages(
    cluster: "EirVahCluster",
    *,
    timeout_s: float = 15.0,
    max_messages: int = 50,
) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    async with cluster.mqtt_client() as client:
        await client.subscribe(SUBSCRIBE_TOPIC, qos=1)
        try:
            async with asyncio.timeout(timeout_s):
                async for msg in client.messages:
                    payload = json.loads(msg.payload)
                    payload["_topic"] = str(msg.topic)
                    messages.append(payload)
                    if len(messages) >= max_messages:
                        break
        except TimeoutError:
            pass
    return messages


async def test_telemetry_happy_path(eirvah_cluster: "EirVahCluster") -> None:
    messages = await _collect_messages(eirvah_cluster, timeout_s=15.0, max_messages=30)

    assert messages, "No MQTT messages received within 15 s"

    topics_seen = {m["_topic"] for m in messages}
    missing = EXPECTED_TOPICS - topics_seen
    assert not missing, f"Missing messages for topics: {missing}"

    for msg in messages:
        assert msg.get("@type") == "sosa:Observation", f"Expected sosa:Observation, got {msg.get('@type')}"
        assert "@context" in msg, "Missing @context"
        assert "sosa:hasSimpleResult" in msg, "Missing sosa:hasSimpleResult"
        assert "sosa:madeBySensor" in msg, "Missing sosa:madeBySensor"
        assert is_valid_correlation_id(msg.get("eirvah:correlationId", "")), "Invalid correlationId"
        assert msg.get("eirvah:quality") in {"good", "uncertain", "bad"}, "Invalid quality"
        # Parse via contract
        obs = SOSAObservation.from_jsonld(msg)
        assert obs.get_value() is not None


async def test_quality_propagation(eirvah_cluster: "EirVahCluster") -> None:
    messages = await _collect_messages(eirvah_cluster, timeout_s=20.0, max_messages=100)
    temp_topic = "uniza/zilina/factory1/line_a/bottler/temperature_sensor_01/temperature"
    temp_msgs = [m for m in messages if m.get("_topic") == temp_topic]
    assert len(temp_msgs) >= 5, f"Need at least 5 temperature messages, got {len(temp_msgs)}"

    bad_count = sum(1 for m in temp_msgs if m.get("eirvah:quality") == "bad")
    bad_pct = bad_count / len(temp_msgs)
    assert bad_pct > 0.02, (
        f"Expected some bad-quality messages (bad_quality_pct=0.1 in address-space config), "
        f"got {bad_count}/{len(temp_msgs)}"
    )
```

- [ ] **Step 2: Update `test_modbus_path.py`**

In `tests/e2e/test_modbus_path.py`, update `test_modbus_envelope_schema`:

```python
async def test_modbus_envelope_schema(eirvah_cluster: "EirVahCluster") -> None:
    from eirvah_contracts.sosa import SOSAObservation

    messages = await _collect_raw_messages(eirvah_cluster, timeout_s=10.0, max_messages=20)
    modbus_msgs = [
        m for m in messages
        if m.get("sosa:madeBySensor", {}).get("@id", "").startswith("eirvah:")
        and "Filler" in m.get("sosa:madeBySensor", {}).get("@id", "")
    ]
    # Fall back: look for any Modbus-sourced observation
    if not modbus_msgs:
        modbus_msgs = [m for m in messages if m.get("@type") == "sosa:Observation"]

    assert modbus_msgs, "No SSN/SOSA observations found on uns.ingress.raw"

    for raw in modbus_msgs[:5]:
        obs = SOSAObservation.from_jsonld(raw)
        assert obs.quality == "good"
        assert obs.get_value() is not None
```

Note: `test_modbus_path.py` subscribes to NATS `uns.ingress.raw` which carries `RawSignalEnvelope` (pre-pipeline). The schema test verifies the pipeline output. Update `_collect_raw_messages` to subscribe to MQTT instead, or keep NATS test focused on alias presence (which doesn't change).

For the Modbus e2e test, the simplest fix is to keep `test_modbus_path_publishes_all_aliases` unchanged (it checks NATS, which still uses `RawSignalEnvelope` with `node_id`) and update `test_modbus_envelope_schema` to check the MQTT output instead:

```python
async def test_modbus_envelope_schema(eirvah_cluster: "EirVahCluster") -> None:
    from eirvah_contracts.sosa import SOSAObservation
    import json
    import asyncio

    messages = []
    async with eirvah_cluster.mqtt_client() as client:
        await client.subscribe("uniza/zilina/factory1/line_a/filler/#", qos=1)
        try:
            async with asyncio.timeout(10.0):
                async for msg in client.messages:
                    messages.append(json.loads(msg.payload))
                    if len(messages) >= 5:
                        break
        except TimeoutError:
            pass

    assert messages, "No Filler messages on MQTT within 10s"
    for raw in messages:
        assert raw.get("@type") == "sosa:Observation"
        obs = SOSAObservation.from_jsonld(raw)
        assert obs.quality == "good"
```

- [ ] **Step 3: Build and deploy**

```bash
./scripts/build_all.sh local 2>&1 | grep "^==>"
```

Build affected services: `uns-auto-contextualizer`, `mqtt-uns-publisher`, `decision-agent-stub`.

```bash
for img in uns-auto-contextualizer mqtt-uns-publisher decision-agent-stub; do
  kind load docker-image ${img}:local --name eirvah-edge
done

kubectl apply -k deploy/k3s/overlays/local

kubectl -n eirvah-edge rollout restart \
  deployment/uns-auto-contextualizer \
  deployment/mqtt-uns-publisher \
  deployment/decision-agent-stub

for d in uns-auto-contextualizer mqtt-uns-publisher decision-agent-stub; do
  kubectl -n eirvah-edge rollout status deployment/$d --timeout=90s
done
```

- [ ] **Step 4: Verify SSN/SOSA messages on MQTT**

```bash
kubectl -n eirvah-edge port-forward svc/mosquitto 1883:1883 &>/tmp/pf-mosquitto.log &
sleep 2
uv run python3 -c "
import asyncio, aiomqtt, json
async def main():
    async with aiomqtt.Client('127.0.0.1', 1883, username='eirvah', password='eirvah-dev-password') as c:
        await c.subscribe('uniza/#', qos=1)
        async with asyncio.timeout(5):
            try:
                async for msg in c.messages:
                    p = json.loads(msg.payload)
                    print('@type:', p.get('@type'))
                    print('result:', p.get('sosa:hasSimpleResult'))
                    print('sensor:', p.get('sosa:madeBySensor'))
                    break
            except asyncio.TimeoutError: pass
asyncio.run(main())
" 2>&1
```

Expected output:
```
@type: sosa:Observation
result: <some numeric value>
sensor: {'@id': 'eirvah:...'}
```

- [ ] **Step 5: Run e2e tests**

```bash
kubectl -n eirvah-edge port-forward svc/nats 4222:4222 &>/tmp/pf-nats.log &
sleep 2
uv run pytest tests/e2e/test_telemetry.py tests/e2e/test_modbus_path.py -v 2>&1 | tail -12
```

Expected: all tests PASS.

- [ ] **Step 6: Run experiment A to confirm CPS loop still works**

```bash
uv run python scripts/disturbance.py --interval 999
```

Wait ~40s. Check decision-agent-stub logs:

```bash
kubectl -n eirvah-edge logs deployment/decision-agent-stub --tail=10 | grep -E "actuation|error"
```

Expected: `actuation_request_emitted` log entry — confirms the stub correctly reads `sosa:hasSimpleResult` from the new payload format.

- [ ] **Step 7: Kill disturbance, push, commit**

```bash
kill $(pgrep -f disturbance.py) 2>/dev/null
git add tests/e2e/
git commit -m "fix(e2e): update telemetry + modbus path tests for SSN/SOSA payload format"
git push
```

---

## Self-review

**Spec coverage:**
- JSON-LD ontology for all 8 stations + 21 sensors ✓ Task 1
- SOSAObservation contract with `to_jsonld()`, `get_value()`, `from_jsonld()` ✓ Task 2
- `ContextualizeResult` + `PublishRequest` carry sensor/feature/property URIs ✓ Task 2
- `uns-auto-contextualizer` uses rdflib + SPARQL, ontology replaces YAML mapping ✓ Task 3
- Orchestrator passes URI fields through `build_publish_request()` ✓ Task 4
- `mqtt-uns-publisher` publishes `sosa:Observation` JSON-LD ✓ Task 5
- `decision-agent-stub` reads `sosa:hasSimpleResult` ✓ Task 6
- e2e tests validate `@type == "sosa:Observation"` and `SOSAObservation.from_jsonld()` ✓ Task 7
- Deploy, verify MQTT output, verify CPS loop still closes ✓ Task 7
- MQTT topic path unchanged ✓ (not modified anywhere)

**Placeholder scan:** None found.

**Type consistency:**
- `SOSAObservation.made_by_sensor: str` set in Task 2, used in Task 5 `build_sosa_observation(req.sensor_uri)` ✓
- `ContextualizeResult.sensor_uri` added in Task 2, populated in Task 3 `contextualize()`, passed through in Task 4 `build_publish_request()`, read in Task 5 `req.sensor_uri` ✓
- `SOSAObservation.from_jsonld(doc)` defined in Task 2, used in Task 7 e2e tests ✓
- `payload["sosa:hasSimpleResult"]` in Task 6 matches key set by `to_jsonld()` in Task 2 ✓
- `payload["eirvah:correlationId"]` in Task 6 matches key set by `to_jsonld()` ✓
