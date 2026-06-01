# SSN/SOSA Ontology-Driven UNS Design

**Status:** Approved for implementation
**Date:** 2026-06-01
**Author:** William Francis Stack

---

## 1. Purpose and scope

Replace the hand-coded ISA-95 mapping YAML with a JSON-LD ontology that formally defines every sensor, equipment unit, and observable property on the bottling line using W3C SSN/SOSA classes. The `uns-auto-contextualizer` service resolves `node_id → UNS topic + SSN/SOSA metadata` by querying the ontology graph at runtime. MQTT payloads become standards-compliant `sosa:Observation` JSON-LD objects.

**In scope:**
- JSON-LD ontology file covering all 8 stations and 21 signals
- `uns-auto-contextualizer` refactored to use `rdflib` + SPARQL
- New `SOSAObservation` contract in `libs/eirvah-contracts`
- `mqtt-uns-publisher` updated to publish JSON-LD
- `decision-agent-stub` updated to read `sosa:hasSimpleResult`
- e2e tests updated to assert on SSN/SOSA payload shape

**Out of scope:**
- Triplestore / SPARQL endpoint (rdflib in-memory only)
- QUDT unit ontology (custom `eirvah:unit` string for prototype)
- Actuation path payloads (keep existing format)
- Grafana / Prometheus (reads metrics, not MQTT payloads)

---

## 2. Ontology structure (`config/eirvah-line-a.jsonld`)

Three entity types:

### Equipment (`ssn:System` + `sosa:FeatureOfInterest`)
Carries the full ISA-95 position:

```json
{
  "@id": "eirvah:Capper",
  "@type": ["ssn:System", "sosa:FeatureOfInterest"],
  "eirvah:enterprise": "uniza",
  "eirvah:site": "zilina",
  "eirvah:area": "factory1",
  "eirvah:line": "line_a",
  "eirvah:cell": "capper"
}
```

### Sensors (`sosa:Sensor`)
Linked to equipment and observable property; carries the `node_id` alias used by the pipeline:

```json
{
  "@id": "eirvah:TorqueSensor01",
  "@type": "sosa:Sensor",
  "sosa:isHostedBy": {"@id": "eirvah:Capper"},
  "sosa:observes": {"@id": "eirvah:Torque"},
  "eirvah:nodeId": "Capper.TorqueSensor01",
  "eirvah:equipment": "torque_sensor_01",
  "eirvah:measurement": "torque"
}
```

### Observable properties (`sosa:ObservableProperty`)
Unit and value type; shared across stations where applicable:

```json
{
  "@id": "eirvah:Torque",
  "@type": "sosa:ObservableProperty",
  "eirvah:unit": "Nm",
  "eirvah:valueType": "double"
}
```

Adding a new station = one equipment block + one sensor block per signal + reuse or add observable properties. The ontology file covers all 8 stations (Bottler, Filler, Conveyor, Reject Station, Inspector, Labeler, Capper, Palletizer).

---

## 3. MQTT payload format (new)

Every published message is a `sosa:Observation` JSON-LD object. The `@context` is inlined — no remote URL dependency, works air-gapped.

```json
{
  "@context": {
    "sosa": "http://www.w3.org/ns/sosa/",
    "ssn": "http://www.w3.org/ns/ssn/",
    "eirvah": "https://eirvah.uniza/ontology/"
  },
  "@type": "sosa:Observation",
  "sosa:madeBySensor": {"@id": "eirvah:TorqueSensor01"},
  "sosa:hasFeatureOfInterest": {"@id": "eirvah:Capper"},
  "sosa:observedProperty": {"@id": "eirvah:Torque"},
  "sosa:hasSimpleResult": 2.5,
  "sosa:resultTime": "2026-06-01T11:08:37Z",
  "eirvah:unit": "Nm",
  "eirvah:quality": "good",
  "eirvah:correlationId": "01KT1DY4PGFY60FV05YG2M5VCX"
}
```

**Design decisions:**
- `sosa:hasSimpleResult` carries the scalar — standard-compliant, easy for consumers
- `eirvah:unit` is a flat string (not QUDT) to avoid a third ontology dependency in the prototype
- `eirvah:quality` and `eirvah:correlationId` are custom extensions (standard allows this)
- MQTT topic path is unchanged: `uniza/zilina/factory1/line_a/cell/equipment/measurement`

---

## 4. `uns-auto-contextualizer` internals

### Startup
Load `config/eirvah-line-a.jsonld` into an in-memory `rdflib.ConjunctiveGraph`. Parse is O(n) on file size; ~21 sensors load in <10ms.

### Per-message resolution
One SPARQL SELECT per incoming signal:

```sparql
PREFIX sosa: <http://www.w3.org/ns/sosa/>
PREFIX eirvah: <https://eirvah.uniza/ontology/>

SELECT ?sensor ?feature ?property
       ?enterprise ?site ?area ?line ?cell ?equipment ?measurement ?unit
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
}
```

Returns one row → constructs `UNSPath` + `SOSAObservation` in one step.

### Error handling
- Unknown `node_id` → `BadNodeId` error envelope → DLQ (same behaviour as today)
- Ontology parse failure at startup → pod fails readiness probe (forces restart)
- SPARQL returns multiple rows → first row used, warning logged

### Performance
rdflib in-memory SPARQL on ~21 triples: sub-millisecond. No regression vs current dict lookup.

### New dependency
`rdflib>=7.0` (BSD-2-Clause, OSI-approved) added to `uns-auto-contextualizer/pyproject.toml`.

---

## 5. Contracts (`libs/eirvah-contracts`)

New `SOSAObservation` model in `libs/eirvah-contracts/src/eirvah_contracts/sosa.py`:

```python
class SOSAObservation(BaseModel):
    made_by_sensor: str          # eirvah: URI
    has_feature_of_interest: str # eirvah: URI
    observed_property: str       # eirvah: URI
    has_simple_result: SignalValue
    result_time: datetime
    unit: str
    quality: Quality
    correlation_id: str

    def to_jsonld(self) -> dict: ...
    def get_value(self) -> SignalValue:
        return self.has_simple_result
```

`get_value()` gives consumers a stable accessor. If the format evolves, only the contract changes.

---

## 6. Consumer changes

### `mqtt-uns-publisher`
Replace `payload = {"value": ..., "unit": ...}` construction with `SOSAObservation.to_jsonld()`.

### `decision-agent-stub`
Replace `payload["value"]` with `SOSAObservation.model_validate(payload).get_value()`.

### e2e tests
- `test_telemetry.py`: validate published payload parses as `SOSAObservation`, assert `has_simple_result` is numeric, assert `@type == "sosa:Observation"`
- `test_modbus_path.py`: same assertion on Modbus-sourced observations

---

## 7. Files changed

```
config/eirvah-line-a.jsonld                                   NEW  ontology (all 8 stations)

libs/eirvah-contracts/src/eirvah_contracts/sosa.py            NEW  SOSAObservation model
libs/eirvah-contracts/src/eirvah_contracts/telemetry.py       MODIFY  TelemetryPayload embeds SOSAObservation

services/uns-auto-contextualizer/pyproject.toml               MODIFY  add rdflib>=7.0
services/uns-auto-contextualizer/src/.../service.py           MODIFY  replace YAML lookup with SPARQL
services/uns-auto-contextualizer/tests/test_*.py              MODIFY  mock graph, assert SOSAObservation

services/mqtt-uns-publisher/src/.../service.py                MODIFY  publish JSON-LD
services/mqtt-uns-publisher/tests/test_*.py                   MODIFY  assert JSON-LD shape

services/decision-agent-stub/src/.../service.py               MODIFY  read via get_value()
services/decision-agent-stub/tests/test_*.py                  MODIFY  update payload fixture

deploy/k3s/base/uns-auto-contextualizer/kustomization.yaml    MODIFY  mount eirvah-line-a.jsonld
deploy/k3s/base/uns-auto-contextualizer/deployment.yaml       MODIFY  add ontology path env var

tests/e2e/test_telemetry.py                                   MODIFY  SSN/SOSA payload assertions
tests/e2e/test_modbus_path.py                                 MODIFY  SSN/SOSA payload assertions
```

---

## 8. Not changed

- MQTT topic path structure (`uniza/zilina/factory1/line_a/...`)
- NATS internal pipeline (subjects, orchestrator, DLQ)
- All protocol adapters (OPC UA, Modbus, S7)
- `data-converter` (operates on `NormalizedSignalEnvelope`, upstream of contextualizer)
- Grafana dashboards (read Prometheus metrics, not MQTT)
- Actuation path (separate pipeline)
