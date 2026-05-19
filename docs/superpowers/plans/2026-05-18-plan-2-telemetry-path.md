# Plan 2 — Telemetry Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the telemetry path — five new services that carry OPC UA signals end-to-end from the bottling-line simulator through NATS into the MQTT UNS — and add the "EirVah Edge Pipeline" Grafana dashboard showing live pipeline metrics.

**Architecture:** `opcua-data-subscriber` (asyncua `Client`) subscribes to the simulator, wraps each `DataChange` in a `NATSEnvelope[RawSignalEnvelope]`, and publishes to `uns.ingress.raw`. `uns-contextualizer-orchestrator` (NATS queue-group consumer) drives three stateless req/rep workers in sequence: `data-converter` → `uns-auto-contextualizer` → `mqtt-uns-publisher`. On any stage failure the orchestrator emits a dead-letter event and increments a labelled counter; end-to-end latency is recorded as a Prometheus histogram. All services expose `/healthz`, `/readyz`, `/metrics` on `:8080` via the existing `eirvah-observability` library.

**Tech Stack:** Python 3.12, asyncua 1.x (client), aiomqtt 2.x (ISC licence, OSI-approved), nats-py, pydantic v2, pydantic-settings, structlog, prometheus-client, starlette, uvicorn, pyyaml, python-ulid, k3d, Kustomize.

**Spec reference:** `docs/superpowers/specs/2026-05-16-eirvah-edge-vertical-slice-design.md` §§ 2–4, 6, 7, 8.

---

## Files produced by this plan

```
libs/eirvah-contracts/src/eirvah_contracts/
└── pipeline.py                       # NEW  ContextualizeResult, PublishRequest

config/
├── opcua-node-list.yaml              # NEW  subscriber browse config
├── opcua-node-to-uns-mapping.yaml    # NEW  alias → UNS path
├── conversion-rules.yaml             # NEW  per-node unit/type rules
└── pipelines/
    └── uns-contextualizer.yaml       # NEW  stage list + timeouts

services/opcua-simulator/
├── src/opcua_simulator/address_space.py   # MODIFY  add bad_quality_pct field
└── src/opcua_simulator/server.py          # MODIFY  wire bad_quality_pct
config/opcua-address-space.yaml            # MODIFY  set bad_quality_pct: 0.1 on temp node

services/opcua-data-subscriber/
├── pyproject.toml
├── Dockerfile
├── src/opcua_data_subscriber/__init__.py
├── src/opcua_data_subscriber/__main__.py
├── src/opcua_data_subscriber/config.py
├── src/opcua_data_subscriber/service.py
└── tests/test_opcua_data_subscriber.py

services/data-converter/
├── pyproject.toml
├── Dockerfile
├── src/data_converter/__init__.py
├── src/data_converter/__main__.py
├── src/data_converter/config.py
├── src/data_converter/service.py
└── tests/test_data_converter.py

services/uns-auto-contextualizer/
├── pyproject.toml
├── Dockerfile
├── src/uns_auto_contextualizer/__init__.py
├── src/uns_auto_contextualizer/__main__.py
├── src/uns_auto_contextualizer/config.py
├── src/uns_auto_contextualizer/service.py
└── tests/test_uns_auto_contextualizer.py

services/mqtt-uns-publisher/
├── pyproject.toml
├── Dockerfile
├── src/mqtt_uns_publisher/__init__.py
├── src/mqtt_uns_publisher/__main__.py
├── src/mqtt_uns_publisher/config.py
├── src/mqtt_uns_publisher/service.py
└── tests/test_mqtt_uns_publisher.py

services/uns-contextualizer-orchestrator/
├── pyproject.toml
├── Dockerfile
├── src/uns_contextualizer_orchestrator/__init__.py
├── src/uns_contextualizer_orchestrator/__main__.py
├── src/uns_contextualizer_orchestrator/config.py
├── src/uns_contextualizer_orchestrator/models.py
├── src/uns_contextualizer_orchestrator/metrics.py
├── src/uns_contextualizer_orchestrator/pipeline.py
├── src/uns_contextualizer_orchestrator/service.py
└── tests/test_uns_contextualizer_orchestrator.py

deploy/k3s/base/
├── opcua-data-subscriber/            # NEW  Deployment + Service + ConfigMap
├── data-converter/                   # NEW
├── uns-auto-contextualizer/          # NEW
├── mqtt-uns-publisher/               # NEW
├── uns-contextualizer-orchestrator/  # NEW
└── kustomization.yaml                # MODIFY  add 5 new dirs + pipeline dashboard

deploy/grafana/dashboards/
└── eirvah-edge-pipeline.json         # NEW  telemetry panels

scripts/
└── trace.sh                          # NEW

tests/e2e/
├── conftest.py                       # NEW  EirVahCluster fixture
└── test_telemetry.py                 # NEW  happy path + quality propagation

pyproject.toml                        # MODIFY  add 5 new workspace members
scripts/build_all.sh                  # MODIFY  add 5 services
scripts/dev_up.sh                     # MODIFY  add 5 services
README.md                             # MODIFY  Plan 2 status → complete
```

---

## Conventions used in every task

1. **TDD.** Write the failing test first, run it red, implement, run it green, commit.
2. **Working directory:** `/Users/billy/Documents/research/eirvah-edge-code` (repo root).
3. **Run tests with:** `uv run pytest <path> -v`
4. **Commit style:** Conventional Commits (`feat(scope): …`).
5. **No emojis** in code or commits.
6. **Every new dependency must be OSI-approved open source.**

---

## Task overview

| # | Subject |
|---|---------|
| 1 | Pipeline wire contracts (`eirvah-contracts/pipeline.py`) |
| 2 | Config YAMLs (node list, UNS mapping, conversion rules, pipeline) |
| 3 | Simulator quality fix (bad_quality_pct per node) |
| 4 | `opcua-data-subscriber` |
| 5 | `data-converter` |
| 6 | `uns-auto-contextualizer` |
| 7 | `mqtt-uns-publisher` |
| 8 | `uns-contextualizer-orchestrator`: models + metrics |
| 9 | `uns-contextualizer-orchestrator`: pipeline + service + tests |
| 10 | Workspace + Kustomize manifests + dev scripts |
| 11 | Grafana EirVah Edge Pipeline dashboard |
| 12 | `scripts/trace.sh` |
| 13 | E2E conftest.py (`EirVahCluster` fixture) |
| 14 | E2E `test_telemetry.py` |
| 15 | Smoke test + README update |

---

### Task 1: Pipeline wire contracts

**Why:** The orchestrator, contextualizer, and publisher all exchange typed envelopes over NATS. These types must live in `eirvah-contracts` so every service can import them without duplicating definitions.

**Files:**
- Create: `libs/eirvah-contracts/src/eirvah_contracts/pipeline.py`
- Modify: `libs/eirvah-contracts/src/eirvah_contracts/__init__.py`
- Create: `libs/eirvah-contracts/tests/test_pipeline.py`

- [ ] **Step 1: Write the failing tests**

```python
# libs/eirvah-contracts/tests/test_pipeline.py
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from eirvah_contracts.pipeline import ContextualizeResult, PublishRequest
from eirvah_contracts.signals import Quality
from eirvah_contracts.uns import UNSPath, build_uns_topic


def _uns() -> UNSPath:
    return UNSPath(
        enterprise="uniza",
        site="zilina",
        area="factory1",
        line="line_a",
        cell="bottler",
        equipment="temperature_sensor_01",
        measurement="temperature",
    )


def test_contextualize_result_round_trip() -> None:
    path = _uns()
    result = ContextualizeResult(
        uns_topic=build_uns_topic(path),
        uns_path=path,
        semantic_type="temperature.celsius",
    )
    raw = result.model_dump(mode="json")
    restored = ContextualizeResult.model_validate(raw)
    assert restored.uns_topic == result.uns_topic
    assert restored.semantic_type == "temperature.celsius"


def test_publish_request_round_trip() -> None:
    now = datetime.now(UTC)
    req = PublishRequest(
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
    )
    raw = req.model_dump(mode="json")
    restored = PublishRequest.model_validate(raw)
    assert restored.value == 23.4
    assert restored.quality == "good"


def test_publish_request_rejects_bad_correlation_id() -> None:
    now = datetime.now(UTC)
    with pytest.raises(Exception):
        PublishRequest(
            uns_topic="a/b/c/d/e/f/g",
            correlation_id="not-a-ulid",
            value=1.0,
            value_type="double",
            unit="degC",
            quality="good",
            semantic_type="temperature.celsius",
            uns_path=_uns(),
            source_endpoint="opc.tcp://localhost:4840",
            source_node_id="x",
            source_timestamp=datetime.now(UTC),
            edge_ingress=datetime.now(UTC),
        )
```

- [ ] **Step 2: Run to verify FAIL**

```bash
uv run pytest libs/eirvah-contracts/tests/test_pipeline.py -v
```
Expected: `ImportError: cannot import name 'ContextualizeResult' from 'eirvah_contracts.pipeline'`

- [ ] **Step 3: Create `libs/eirvah-contracts/src/eirvah_contracts/pipeline.py`**

```python
"""Internal pipeline wire contracts shared between orchestrator and workers."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, field_validator

from eirvah_contracts.signals import Quality, SignalValue, SignalValueType
from eirvah_contracts.ulid import is_valid_correlation_id
from eirvah_contracts.uns import UNSPath


class ContextualizeResult(BaseModel):
    """Reply payload from ``uns-auto-contextualizer`` on ``uns.work.contextualize``."""

    model_config = ConfigDict(extra="forbid")

    uns_topic: str
    uns_path: UNSPath
    semantic_type: str


class PublishRequest(BaseModel):
    """Request payload sent to ``mqtt-uns-publisher`` on ``uns.work.publish``.

    Carries everything needed to build a ``TelemetryPayload`` v1.0; the publisher
    only adds ``timestamps.edge_publish`` (set to ``now()`` at publish time).
    """

    model_config = ConfigDict(extra="forbid")

    uns_topic: str
    correlation_id: str
    value: SignalValue
    value_type: SignalValueType
    unit: str
    quality: Quality
    semantic_type: str
    uns_path: UNSPath
    source_endpoint: str
    source_node_id: str
    source_timestamp: datetime
    edge_ingress: datetime

    @field_validator("correlation_id")
    @classmethod
    def _validate_correlation_id(cls, v: str) -> str:
        if not is_valid_correlation_id(v):
            raise ValueError(f"invalid correlation_id: {v!r}")
        return v
```

- [ ] **Step 4: Update `libs/eirvah-contracts/src/eirvah_contracts/__init__.py`**

Read the current file first, then add the exports:

```python
"""EirVah wire contracts — pydantic models for every internal and public message."""

from eirvah_contracts.actuation import (
    ActuationApproveResult,
    ActuationRejectResult,
    ActuationRequest,
)
from eirvah_contracts.envelope import EnvelopeError, NATSEnvelope
from eirvah_contracts.pipeline import ContextualizeResult, PublishRequest
from eirvah_contracts.signals import (
    NormalizedSignalEnvelope,
    Quality,
    RawSignalEnvelope,
    SignalValue,
    SignalValueType,
)
from eirvah_contracts.telemetry import TelemetryPayload, TelemetrySource, TelemetryTimestamps
from eirvah_contracts.ulid import generate_correlation_id, is_valid_correlation_id
from eirvah_contracts.uns import UNSPath, build_uns_topic, parse_uns_topic

__all__ = [
    "ActuationApproveResult",
    "ActuationRejectResult",
    "ActuationRequest",
    "ContextualizeResult",
    "EnvelopeError",
    "NATSEnvelope",
    "NormalizedSignalEnvelope",
    "PublishRequest",
    "Quality",
    "RawSignalEnvelope",
    "SignalValue",
    "SignalValueType",
    "TelemetryPayload",
    "TelemetrySource",
    "TelemetryTimestamps",
    "UNSPath",
    "build_uns_topic",
    "generate_correlation_id",
    "is_valid_correlation_id",
    "parse_uns_topic",
]
```

- [ ] **Step 5: Run tests to verify PASS**

```bash
uv run pytest libs/eirvah-contracts/tests/test_pipeline.py -v
```
Expected: 3 PASSED

- [ ] **Step 6: Run full contracts test suite**

```bash
uv run pytest libs/eirvah-contracts/ -v
```
Expected: all PASSED (no regressions)

- [ ] **Step 7: Commit**

```bash
git add libs/eirvah-contracts/src/eirvah_contracts/pipeline.py \
        libs/eirvah-contracts/src/eirvah_contracts/__init__.py \
        libs/eirvah-contracts/tests/test_pipeline.py
git commit -m "feat(contracts): add ContextualizeResult and PublishRequest pipeline types"
```

---

### Task 2: Config YAMLs

**Why:** Workers and the subscriber read YAML configs mounted as ConfigMaps. Writing them now lets every subsequent task reference concrete values.

**Files:**
- Create: `config/opcua-node-list.yaml`
- Create: `config/opcua-node-to-uns-mapping.yaml`
- Create: `config/conversion-rules.yaml`
- Create: `config/pipelines/uns-contextualizer.yaml`

- [ ] **Step 1: Create `config/opcua-node-list.yaml`**

This file is mounted into `opcua-data-subscriber`. It tells the subscriber which OPC UA nodes to monitor (by browse path under `Objects/`) and assigns each a stable alias used as `node_id` in all downstream messages.

```yaml
# OPC UA node browse config for opcua-data-subscriber (spec §3.1)
endpoint: "opc.tcp://opcua-simulator:4840/eirvah/simulator"
namespace_uri: "https://eirvah.uniza/zilina/factory1"
publishing_interval_ms: 500
nodes:
  - browse_names: ["Bottler", "Temperature"]
    alias: "Bottler.Temperature01"
  - browse_names: ["Bottler", "Throughput"]
    alias: "Bottler.ThroughputMeter01"
  - browse_names: ["Bottler", "State"]
    alias: "Bottler.Motor01.State"
  - browse_names: ["Bottler", "Rpm"]
    alias: "Bottler.Motor01.Rpm"
  - browse_names: ["Bottler", "SetpointTemperature"]
    alias: "Bottler.SetpointUnit.SetpointTemperature"
```

- [ ] **Step 2: Create `config/opcua-node-to-uns-mapping.yaml`**

```yaml
# Maps OPC UA node aliases to ISA-95 UNS paths (spec §4.1, §3.1).
# enterprise and site are injected at runtime via UNS_ENTERPRISE / UNS_SITE env vars.
mappings:
  - node_id: "Bottler.Temperature01"
    area: factory1
    line: line_a
    cell: bottler
    equipment: temperature_sensor_01
    measurement: temperature
    semantic_type: temperature.celsius

  - node_id: "Bottler.ThroughputMeter01"
    area: factory1
    line: line_a
    cell: bottler
    equipment: throughput_meter_01
    measurement: throughput
    semantic_type: flow.bps

  - node_id: "Bottler.Motor01.State"
    area: factory1
    line: line_a
    cell: bottler
    equipment: motor_01
    measurement: state
    semantic_type: state.enum

  - node_id: "Bottler.Motor01.Rpm"
    area: factory1
    line: line_a
    cell: bottler
    equipment: motor_01
    measurement: rpm
    semantic_type: rotational_speed.rpm

  - node_id: "Bottler.SetpointUnit.SetpointTemperature"
    area: factory1
    line: line_a
    cell: bottler
    equipment: setpoint_unit
    measurement: setpoint_temperature
    semantic_type: setpoint.target
```

- [ ] **Step 3: Create `config/conversion-rules.yaml`**

```yaml
# Per-node unit and type rules for data-converter (spec §3.1).
# scale and offset are optional; absent means pass-through.
# drop_bad_quality: false — bad quality messages still flow (spec §4.2).
rules:
  - node_id: "Bottler.Temperature01"
    value_type: double
    unit: degC
    drop_bad_quality: false

  - node_id: "Bottler.ThroughputMeter01"
    value_type: double
    unit: "bottle/s"
    drop_bad_quality: false

  - node_id: "Bottler.Motor01.State"
    value_type: int64
    unit: dimensionless
    drop_bad_quality: false

  - node_id: "Bottler.Motor01.Rpm"
    value_type: double
    unit: rpm
    drop_bad_quality: false

  - node_id: "Bottler.SetpointUnit.SetpointTemperature"
    value_type: double
    unit: degC
    drop_bad_quality: false
```

- [ ] **Step 4: Create `config/pipelines/uns-contextualizer.yaml`**

```yaml
# Pipeline stage definitions for uns-contextualizer-orchestrator (spec §3.1).
stages:
  - name: convert
    subject: uns.work.convert
    timeout_s: 2.0
  - name: contextualize
    subject: uns.work.contextualize
    timeout_s: 2.0
  - name: publish
    subject: uns.work.publish
    timeout_s: 2.0
dlq_subject: uns.dlq.telemetry
```

- [ ] **Step 5: Commit**

```bash
git add config/opcua-node-list.yaml \
        config/opcua-node-to-uns-mapping.yaml \
        config/conversion-rules.yaml \
        config/pipelines/uns-contextualizer.yaml
git commit -m "feat(config): add telemetry pipeline config YAMLs"
```

---

### Task 3: Simulator quality fix

**Why:** `test_quality_propagation` (Task 14) requires the simulator to emit ~10 % bad-quality readings for the temperature node. `NodeDefinition` currently has no `bad_quality_pct` field and `server.py` hardcodes 0.0. This task wires up the field end-to-end.

**Files:**
- Modify: `services/opcua-simulator/src/opcua_simulator/address_space.py`
- Modify: `services/opcua-simulator/src/opcua_simulator/server.py`
- Modify: `config/opcua-address-space.yaml`
- Modify: `services/opcua-simulator/tests/test_address_space.py` (add one test)

- [ ] **Step 1: Write a failing test for NodeDefinition accepting bad_quality_pct**

Open `services/opcua-simulator/tests/test_address_space.py` and add at the end:

```python
def test_node_definition_bad_quality_pct_defaults_to_zero() -> None:
    from opcua_simulator.address_space import NodeDefinition
    node = NodeDefinition(
        id="x",
        kind="measurement",
        cell="c",
        equipment="e",
        measurement="m",
        value_type="double",
        unit="degC",
        initial=0.0,
        semantic_type="temperature.celsius",
    )
    assert node.bad_quality_pct == 0.0
    assert node.uncertain_quality_pct == 0.0


def test_node_definition_accepts_bad_quality_pct() -> None:
    from opcua_simulator.address_space import NodeDefinition
    node = NodeDefinition(
        id="x",
        kind="measurement",
        cell="c",
        equipment="e",
        measurement="m",
        value_type="double",
        unit="degC",
        initial=0.0,
        semantic_type="temperature.celsius",
        bad_quality_pct=0.1,
    )
    assert node.bad_quality_pct == 0.1
```

- [ ] **Step 2: Run to verify FAIL**

```bash
uv run pytest services/opcua-simulator/tests/test_address_space.py -v -k "bad_quality"
```
Expected: FAIL — `NodeDefinition` rejects `bad_quality_pct` due to `extra="forbid"`.

- [ ] **Step 3: Update `NodeDefinition` in `address_space.py`**

Add two optional fields to `NodeDefinition`:

```python
class NodeDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str
    kind: Literal["measurement", "setpoint"]
    cell: str
    equipment: str
    measurement: str
    value_type: Literal["double", "int64", "bool", "string"]
    unit: str
    initial: float | int | bool | str
    semantic_type: str
    dynamics: str | None = None
    policy: NodePolicy | None = None
    bad_quality_pct: float = 0.0
    uncertain_quality_pct: float = 0.0
```

- [ ] **Step 4: Update `server.py` to read quality pcts from NodeDefinition**

In `SimulatorRuntime._build_dynamics`, replace the hardcoded quality emitter loop:

```python
        for node_def in self._address_space.iter_nodes():
            self._quality_per_node[node_def.id] = QualityEmitter(
                rng=self.rng,
                bad_quality_pct=node_def.bad_quality_pct,
                uncertain_quality_pct=node_def.uncertain_quality_pct,
            )
```

- [ ] **Step 5: Update `config/opcua-address-space.yaml`** — add `bad_quality_pct: 0.1` to the Temperature node so `test_quality_propagation` has something to observe.

Find the temperature node entry (id: `TemperatureSensor01.Temperature`) and add the field:

```yaml
      - id: TemperatureSensor01.Temperature
        kind: measurement
        cell: bottler
        equipment: temperature_sensor_01
        measurement: temperature
        value_type: double
        unit: degC
        initial: 22.0
        semantic_type: temperature.celsius
        dynamics: temperature
        bad_quality_pct: 0.1
```

- [ ] **Step 6: Run simulator tests**

```bash
uv run pytest services/opcua-simulator/ -v
```
Expected: all PASSED (including the two new tests).

- [ ] **Step 7: Commit**

```bash
git add services/opcua-simulator/src/opcua_simulator/address_space.py \
        services/opcua-simulator/src/opcua_simulator/server.py \
        config/opcua-address-space.yaml \
        services/opcua-simulator/tests/test_address_space.py
git commit -m "feat(simulator): wire bad_quality_pct from address-space config to QualityEmitter"
```

---

### Task 4: opcua-data-subscriber

**Why:** This is the first pod in the telemetry path. It connects to the OPC UA simulator as a client, subscribes to the configured nodes by browse path, and publishes each `DataChange` as a `NATSEnvelope[RawSignalEnvelope]` onto `uns.ingress.raw`.

**Files:**
- Create: `services/opcua-data-subscriber/pyproject.toml`
- Create: `services/opcua-data-subscriber/Dockerfile`
- Create: `services/opcua-data-subscriber/src/opcua_data_subscriber/__init__.py`
- Create: `services/opcua-data-subscriber/src/opcua_data_subscriber/__main__.py`
- Create: `services/opcua-data-subscriber/src/opcua_data_subscriber/config.py`
- Create: `services/opcua-data-subscriber/src/opcua_data_subscriber/service.py`
- Create: `services/opcua-data-subscriber/tests/test_opcua_data_subscriber.py`

- [ ] **Step 1: Write failing tests**

```python
# services/opcua-data-subscriber/tests/test_opcua_data_subscriber.py
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_contracts.signals import RawSignalEnvelope
from eirvah_contracts.ulid import is_valid_correlation_id


def _make_data_value(value: Any, status_good: bool = True) -> MagicMock:
    dv = MagicMock()
    dv.SourceTimestamp = datetime(2026, 5, 18, 12, 0, 0, tzinfo=UTC)
    dv.ServerTimestamp = datetime(2026, 5, 18, 12, 0, 0, tzinfo=UTC)
    sc = MagicMock()
    sc.is_bad.return_value = not status_good
    sc.is_uncertain.return_value = False
    dv.StatusCode = sc
    return dv


def test_build_raw_envelope_good_quality() -> None:
    from opcua_data_subscriber.service import build_raw_envelope

    node_mock = MagicMock()
    node_mock.nodeid = MagicMock()
    node_mock.nodeid.__str__ = lambda self: "ns=2;i=1001"

    dv = _make_data_value(23.4)
    envelope = build_raw_envelope(
        alias="Bottler.Temperature01",
        value=23.4,
        source_endpoint="opc.tcp://opcua-simulator:4840",
        data_value=dv,
    )
    assert isinstance(envelope, RawSignalEnvelope)
    assert envelope.node_id == "Bottler.Temperature01"
    assert envelope.value == 23.4
    assert envelope.quality == "good"
    assert envelope.source_endpoint == "opc.tcp://opcua-simulator:4840"


def test_build_raw_envelope_bad_quality() -> None:
    from opcua_data_subscriber.service import build_raw_envelope

    dv = _make_data_value(99.0, status_good=False)
    envelope = build_raw_envelope(
        alias="Bottler.Temperature01",
        value=99.0,
        source_endpoint="opc.tcp://opcua-simulator:4840",
        data_value=dv,
    )
    assert envelope.quality == "bad"


def test_wrap_in_nats_envelope() -> None:
    from opcua_data_subscriber.service import wrap_in_nats_envelope

    now = datetime.now(UTC)
    raw = RawSignalEnvelope(
        source_endpoint="opc.tcp://opcua-simulator:4840",
        node_id="Bottler.Temperature01",
        value=23.4,
        value_type="double",
        quality="good",
        source_timestamp=now,
        server_timestamp=now,
        received_at=now,
    )
    env = wrap_in_nats_envelope(raw)
    assert isinstance(env, NATSEnvelope)
    assert is_valid_correlation_id(env.correlation_id)
    assert env.status == "ok"
    assert env.payload is not None
    assert env.payload["node_id"] == "Bottler.Temperature01"


def test_detect_value_type() -> None:
    from opcua_data_subscriber.service import detect_value_type

    assert detect_value_type(True) == "bool"
    assert detect_value_type(42) == "int64"
    assert detect_value_type(3.14) == "double"
    assert detect_value_type("hello") == "string"
```

- [ ] **Step 2: Run to verify FAIL**

```bash
uv run pytest services/opcua-data-subscriber/tests/ -v
```
Expected: `ModuleNotFoundError: No module named 'opcua_data_subscriber'`

- [ ] **Step 3: Create `services/opcua-data-subscriber/pyproject.toml`**

```toml
[project]
name = "opcua-data-subscriber"
version = "0.0.0"
description = "OPC UA → NATS ingress subscriber (spec §3.1)."
requires-python = ">=3.12"
license = { text = "Apache-2.0" }
dependencies = [
    "asyncua>=1.1",
    "nats-py>=2.7",
    "pydantic>=2.8",
    "pydantic-settings>=2.5",
    "pyyaml>=6.0",
    "structlog>=24.0",
    "uvicorn>=0.30",
    "eirvah-contracts",
    "eirvah-bus",
    "eirvah-observability",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/opcua_data_subscriber"]
```

- [ ] **Step 4: Add to workspace — edit `pyproject.toml` at repo root**

Add `"services/opcua-data-subscriber"` to the `[tool.uv.workspace] members` list and a corresponding source entry. (Full workspace update happens in Task 10; for now just add this one so tests run.)

In `pyproject.toml`, add to `members`:
```toml
[tool.uv.workspace]
members = [
    "libs/eirvah-contracts",
    "libs/eirvah-bus",
    "libs/eirvah-observability",
    "services/opcua-simulator",
    "services/opcua-data-subscriber",
]
```

And add to `[tool.uv.sources]`:
```toml
opcua-data-subscriber = { workspace = true }
```

And add to `[dependency-groups] dev`:
```toml
    "opcua-data-subscriber",
```

Then run:
```bash
uv sync
```

- [ ] **Step 5: Create source files**

`services/opcua-data-subscriber/src/opcua_data_subscriber/__init__.py` — empty file.

`services/opcua-data-subscriber/src/opcua_data_subscriber/config.py`:
```python
"""Settings for the OPC UA data subscriber."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SubscriberSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="OPCUA_DATA_SUBSCRIBER_",
        env_file=None,
        extra="ignore",
    )

    nats_servers: list[str] = ["nats://nats:4222"]
    node_list_path: Path = Path("/etc/opcua-data-subscriber/opcua-node-list.yaml")
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
    reconnect_delay_s: float = 5.0
```

`services/opcua-data-subscriber/src/opcua_data_subscriber/service.py`:
```python
"""OPC UA data subscriber service (spec §3.1)."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import structlog
import uvicorn
import yaml
from asyncua import Client
from asyncua.common.subscription import SubHandler
from eirvah_bus.client import BusClient
from eirvah_bus.request_reply import BUS_HEADER_CORRELATION_ID
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_contracts.signals import Quality, RawSignalEnvelope, SignalValueType
from eirvah_contracts.ulid import generate_correlation_id
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import configure_logging
from eirvah_observability.metrics import make_gauge
from pydantic import BaseModel

from opcua_data_subscriber.config import SubscriberSettings

_log = structlog.get_logger("opcua-data-subscriber")


# ---------------------------------------------------------------------------
# Config models (loaded from opcua-node-list.yaml)
# ---------------------------------------------------------------------------

class NodeConfig(BaseModel):
    browse_names: list[str]
    alias: str


class NodeListConfig(BaseModel):
    endpoint: str
    namespace_uri: str
    publishing_interval_ms: int = 500
    nodes: list[NodeConfig]


def load_node_list(path: Path) -> NodeListConfig:
    raw = yaml.safe_load(path.read_text())
    return NodeListConfig.model_validate(raw)


# ---------------------------------------------------------------------------
# Pure helper functions (unit-testable without any live connections)
# ---------------------------------------------------------------------------

def detect_value_type(value: Any) -> SignalValueType:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int64"
    if isinstance(value, float):
        return "double"
    return "string"


def build_raw_envelope(
    *,
    alias: str,
    value: Any,
    source_endpoint: str,
    data_value: Any,
) -> RawSignalEnvelope:
    sc = data_value.StatusCode
    if sc.is_bad():
        quality: Quality = "bad"
    elif sc.is_uncertain():
        quality = "uncertain"
    else:
        quality = "good"

    src_ts = data_value.SourceTimestamp
    srv_ts = data_value.ServerTimestamp
    if src_ts is None or not hasattr(src_ts, "tzinfo"):
        src_ts = datetime.now(UTC)
    elif src_ts.tzinfo is None:
        src_ts = src_ts.replace(tzinfo=UTC)
    if srv_ts is None or not hasattr(srv_ts, "tzinfo"):
        srv_ts = datetime.now(UTC)
    elif srv_ts.tzinfo is None:
        srv_ts = srv_ts.replace(tzinfo=UTC)

    return RawSignalEnvelope(
        source_endpoint=source_endpoint,
        node_id=alias,
        value=value,
        value_type=detect_value_type(value),
        quality=quality,
        source_timestamp=src_ts,
        server_timestamp=srv_ts,
        received_at=datetime.now(UTC),
    )


def wrap_in_nats_envelope(raw: RawSignalEnvelope) -> NATSEnvelope:
    return NATSEnvelope(
        correlation_id=generate_correlation_id(),
        payload=raw.model_dump(mode="json"),
    )


# ---------------------------------------------------------------------------
# OPC UA subscription handler
# ---------------------------------------------------------------------------

class _DataChangeHandler(SubHandler):  # type: ignore[misc]
    def __init__(
        self,
        alias_map: dict[str, str],
        endpoint: str,
        on_message: Any,
    ) -> None:
        self._alias_map = alias_map
        self._endpoint = endpoint
        self._on_message = on_message

    async def datachange_notification(self, node: Any, val: Any, data: Any) -> None:
        node_key = str(node.nodeid)
        alias = self._alias_map.get(node_key)
        if alias is None:
            return
        try:
            raw = build_raw_envelope(
                alias=alias,
                value=val,
                source_endpoint=self._endpoint,
                data_value=data,
            )
            envelope = wrap_in_nats_envelope(raw)
            await self._on_message(envelope)
        except Exception as exc:
            _log.warning("datachange_handler_error", error=str(exc))


# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------

class SubscriberRuntime:
    def __init__(self, settings: SubscriberSettings) -> None:
        self._settings = settings
        self._bus: BusClient | None = None
        self._ready = False
        self._connection_state = make_gauge(
            "ingress_connection_state",
            "1 when the ingress connection is up, 0 otherwise",
            labelnames=["ingress", "state"],
        )

    def is_ready(self) -> bool:
        return self._ready

    async def run(self) -> None:
        cfg = load_node_list(self._settings.node_list_path)
        self._bus = BusClient(servers=self._settings.nats_servers, name="opcua-data-subscriber")
        await self._bus.connect()
        _log.info("nats_connected", servers=self._settings.nats_servers)

        while True:
            try:
                await self._subscribe_loop(cfg)
            except Exception as exc:
                self._ready = False
                self._connection_state.labels(ingress="opcua", state="disconnected").set(1)
                self._connection_state.labels(ingress="opcua", state="connected").set(0)
                _log.warning("opcua_disconnected", error=str(exc))
                await asyncio.sleep(self._settings.reconnect_delay_s)

    async def _subscribe_loop(self, cfg: NodeListConfig) -> None:
        async with Client(url=cfg.endpoint) as client:
            ns_idx = await client.get_namespace_index(cfg.namespace_uri)
            objects = client.nodes.objects

            alias_map: dict[str, str] = {}
            nodes_to_subscribe = []
            for node_cfg in cfg.nodes:
                path = [f"{ns_idx}:{name}" for name in node_cfg.browse_names]
                node = await objects.get_child(path)
                alias_map[str(node.nodeid)] = node_cfg.alias
                nodes_to_subscribe.append(node)

            handler = _DataChangeHandler(
                alias_map=alias_map,
                endpoint=cfg.endpoint,
                on_message=self._publish,
            )
            sub = await client.create_subscription(cfg.publishing_interval_ms, handler)
            await sub.subscribe_data_change(nodes_to_subscribe)

            self._ready = True
            self._connection_state.labels(ingress="opcua", state="connected").set(1)
            self._connection_state.labels(ingress="opcua", state="disconnected").set(0)
            _log.info("opcua_subscribed", node_count=len(nodes_to_subscribe), endpoint=cfg.endpoint)

            await asyncio.get_event_loop().create_future()  # run until exception

    async def _publish(self, envelope: NATSEnvelope) -> None:
        assert self._bus is not None
        headers = {BUS_HEADER_CORRELATION_ID: envelope.correlation_id}
        await self._bus.nc.publish(
            "uns.ingress.raw",
            envelope.model_dump_json().encode(),
            headers=headers,
        )


async def run(settings: SubscriberSettings) -> None:
    configure_logging(level=settings.log_level)
    runtime = SubscriberRuntime(settings)
    health = HealthApp(is_ready=runtime.is_ready)

    http_cfg = uvicorn.Config(
        health.asgi,
        host="0.0.0.0",
        port=settings.http_port,
        log_level=settings.log_level.lower(),
    )
    http = uvicorn.Server(http_cfg)
    await asyncio.gather(runtime.run(), http.serve())
```

`services/opcua-data-subscriber/src/opcua_data_subscriber/__main__.py`:
```python
"""Entry point for the OPC UA data subscriber pod."""

from __future__ import annotations

import asyncio

from opcua_data_subscriber.config import SubscriberSettings
from opcua_data_subscriber.service import run


def main() -> None:
    settings = SubscriberSettings()
    asyncio.run(run(settings))


if __name__ == "__main__":  # pragma: no cover
    main()
```

- [ ] **Step 6: Create Dockerfile**

`services/opcua-data-subscriber/Dockerfile`:
```dockerfile
# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS builder
WORKDIR /workspace
RUN pip install --no-cache-dir uv==0.4.20
COPY pyproject.toml uv.lock /workspace/
COPY libs /workspace/libs
COPY services/opcua-data-subscriber /workspace/services/opcua-data-subscriber
RUN uv sync --frozen --no-dev --package opcua-data-subscriber

FROM python:3.12-slim AS runtime
WORKDIR /workspace
COPY --from=builder /workspace/.venv /workspace/.venv
COPY --from=builder /workspace/libs /workspace/libs
COPY --from=builder /workspace/services/opcua-data-subscriber/src /workspace/services/opcua-data-subscriber/src
ENV PATH="/workspace/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1
USER nobody:nogroup
EXPOSE 8080
ENTRYPOINT ["/workspace/.venv/bin/python", "-m", "opcua_data_subscriber"]
```

- [ ] **Step 7: Run tests to verify PASS**

```bash
uv run pytest services/opcua-data-subscriber/tests/ -v
```
Expected: 4 PASSED

- [ ] **Step 8: Commit**

```bash
git add services/opcua-data-subscriber/ pyproject.toml uv.lock
git commit -m "feat(opcua-data-subscriber): OPC UA client subscription → NATS ingress"
```

---

### Task 5: data-converter

**Why:** Stateless NATS req/rep worker. Receives `NATSEnvelope[RawSignalEnvelope]` on `uns.work.convert`, applies unit/type conversion rules, replies with `NATSEnvelope[NormalizedSignalEnvelope]`.

**Files:**
- Create: `services/data-converter/pyproject.toml`
- Create: `services/data-converter/Dockerfile`
- Create: `services/data-converter/src/data_converter/__init__.py`
- Create: `services/data-converter/src/data_converter/__main__.py`
- Create: `services/data-converter/src/data_converter/config.py`
- Create: `services/data-converter/src/data_converter/service.py`
- Create: `services/data-converter/tests/test_data_converter.py`

- [ ] **Step 1: Write failing tests**

```python
# services/data-converter/tests/test_data_converter.py
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from eirvah_contracts.envelope import EnvelopeError, NATSEnvelope
from eirvah_contracts.signals import RawSignalEnvelope


def _raw(node_id: str = "Bottler.Temperature01", value: object = 23.4) -> RawSignalEnvelope:
    now = datetime.now(UTC)
    return RawSignalEnvelope(
        source_endpoint="opc.tcp://test:4840",
        node_id=node_id,
        value=value,
        value_type="double",
        quality="good",
        source_timestamp=now,
        server_timestamp=now,
        received_at=now,
    )


def test_convert_passthrough() -> None:
    from data_converter.service import ConversionRule, apply_conversion

    raw = _raw()
    rule = ConversionRule(node_id="Bottler.Temperature01", value_type="double", unit="degC", drop_bad_quality=False)
    normalized = apply_conversion(raw, rule)
    assert normalized.node_id == "Bottler.Temperature01"
    assert normalized.value == 23.4
    assert normalized.unit == "degC"
    assert normalized.quality == "good"


def test_convert_with_scale_and_offset() -> None:
    from data_converter.service import ConversionRule, apply_conversion

    raw = _raw(value=100.0)
    rule = ConversionRule(
        node_id="Bottler.Temperature01",
        value_type="double",
        unit="degC",
        drop_bad_quality=False,
        scale=0.1,
        offset=-10.0,
    )
    normalized = apply_conversion(raw, rule)
    assert abs(float(normalized.value) - 0.0) < 1e-9  # 100 * 0.1 - 10.0 = 0.0


def test_convert_drops_bad_quality_when_configured() -> None:
    from data_converter.service import ConversionRule, apply_conversion

    now = datetime.now(UTC)
    raw = RawSignalEnvelope(
        source_endpoint="opc.tcp://test:4840",
        node_id="Bottler.Temperature01",
        value=23.4,
        value_type="double",
        quality="bad",
        source_timestamp=now,
        server_timestamp=now,
        received_at=now,
    )
    rule = ConversionRule(
        node_id="Bottler.Temperature01",
        value_type="double",
        unit="degC",
        drop_bad_quality=True,
    )
    result = apply_conversion(raw, rule)
    assert result is None


def test_handle_request_ok() -> None:
    from data_converter.service import ConversionRule, handle_convert_request

    raw = _raw()
    rules = {"Bottler.Temperature01": ConversionRule(node_id="Bottler.Temperature01", value_type="double", unit="degC", drop_bad_quality=False)}
    req_env = NATSEnvelope(correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A", payload=raw.model_dump(mode="json"))
    reply = handle_convert_request(req_env, rules)
    assert reply.status == "ok"
    assert reply.payload is not None
    assert reply.payload["unit"] == "degC"


def test_handle_request_unknown_node() -> None:
    from data_converter.service import ConversionRule, handle_convert_request

    raw = _raw(node_id="Unknown.Node")
    rules: dict = {}
    req_env = NATSEnvelope(correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A", payload=raw.model_dump(mode="json"))
    reply = handle_convert_request(req_env, rules)
    assert reply.status == "error"
    assert reply.error is not None
    assert "Unknown.Node" in reply.error.message
```

- [ ] **Step 2: Run to verify FAIL**

```bash
uv run pytest services/data-converter/tests/ -v
```
Expected: `ModuleNotFoundError`

- [ ] **Step 3: Create `services/data-converter/pyproject.toml`**

```toml
[project]
name = "data-converter"
version = "0.0.0"
description = "Normalizes raw OPC UA signal envelopes (spec §3.1)."
requires-python = ">=3.12"
license = { text = "Apache-2.0" }
dependencies = [
    "nats-py>=2.7",
    "pydantic>=2.8",
    "pydantic-settings>=2.5",
    "pyyaml>=6.0",
    "structlog>=24.0",
    "uvicorn>=0.30",
    "eirvah-contracts",
    "eirvah-bus",
    "eirvah-observability",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/data_converter"]
```

Add `"services/data-converter"` to the workspace in root `pyproject.toml` and run `uv sync`.

- [ ] **Step 4: Create source files**

`services/data-converter/src/data_converter/__init__.py` — empty.

`services/data-converter/src/data_converter/config.py`:
```python
"""Settings for the data-converter worker."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DataConverterSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DATA_CONVERTER_",
        env_file=None,
        extra="ignore",
    )

    nats_servers: list[str] = ["nats://nats:4222"]
    rules_path: Path = Path("/etc/data-converter/conversion-rules.yaml")
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
```

`services/data-converter/src/data_converter/service.py`:
```python
"""Data-converter NATS req/rep worker (spec §3.1)."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import structlog
import uvicorn
import yaml
from eirvah_bus.client import BusClient
from eirvah_bus.consumer import subscribe_queue_group
from eirvah_contracts.envelope import EnvelopeError, NATSEnvelope
from eirvah_contracts.signals import NormalizedSignalEnvelope, RawSignalEnvelope, SignalValueType
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import configure_logging
from eirvah_observability.metrics import make_counter
from nats.aio.msg import Msg
from pydantic import BaseModel

from data_converter.config import DataConverterSettings

_log = structlog.get_logger("data-converter")

SUBJECT = "uns.work.convert"


class ConversionRule(BaseModel):
    node_id: str
    value_type: SignalValueType
    unit: str
    drop_bad_quality: bool = False
    scale: float | None = None
    offset: float | None = None


def load_rules(path: Path) -> dict[str, ConversionRule]:
    raw = yaml.safe_load(path.read_text())
    return {r["node_id"]: ConversionRule.model_validate(r) for r in raw["rules"]}


def apply_conversion(raw: RawSignalEnvelope, rule: ConversionRule) -> NormalizedSignalEnvelope | None:
    if rule.drop_bad_quality and raw.quality == "bad":
        return None
    value: Any = raw.value
    if rule.scale is not None:
        value = float(value) * rule.scale
    if rule.offset is not None:
        value = float(value) + rule.offset
    return NormalizedSignalEnvelope(
        node_id=raw.node_id,
        value=value,
        value_type=rule.value_type,
        unit=rule.unit,
        quality=raw.quality,
        source_timestamp=raw.source_timestamp,
        received_at=raw.received_at,
    )


def handle_convert_request(
    envelope: NATSEnvelope,
    rules: dict[str, ConversionRule],
) -> NATSEnvelope:
    try:
        raw = RawSignalEnvelope.model_validate(envelope.payload)
        rule = rules.get(raw.node_id)
        if rule is None:
            return NATSEnvelope(
                correlation_id=envelope.correlation_id,
                status="error",
                error=EnvelopeError(kind="UnknownNode", message=f"no conversion rule for node_id {raw.node_id!r}"),
            )
        normalized = apply_conversion(raw, rule)
        if normalized is None:
            return NATSEnvelope(
                correlation_id=envelope.correlation_id,
                status="error",
                error=EnvelopeError(kind="DroppedQuality", message=f"dropped bad-quality reading for {raw.node_id!r}"),
            )
        return NATSEnvelope(
            correlation_id=envelope.correlation_id,
            payload=normalized.model_dump(mode="json"),
        )
    except Exception as exc:
        return NATSEnvelope(
            correlation_id=envelope.correlation_id,
            status="error",
            error=EnvelopeError(kind=type(exc).__name__, message=str(exc)[:200]),
        )


class DataConverterWorker:
    def __init__(self, settings: DataConverterSettings) -> None:
        self._settings = settings
        self._rules: dict[str, ConversionRule] = {}
        self._bus: BusClient | None = None
        self._ready = False
        self._handled = make_counter("worker_handler_total", "Worker handler invocations", labelnames=["worker", "outcome"])

    def is_ready(self) -> bool:
        return self._ready

    async def run(self) -> None:
        self._rules = load_rules(self._settings.rules_path)
        self._bus = BusClient(servers=self._settings.nats_servers, name="data-converter")
        await self._bus.connect()
        await subscribe_queue_group(nc=self._bus.nc, subject=SUBJECT, handler=self._handle)
        self._ready = True
        _log.info("data_converter_ready", subject=SUBJECT, rules=len(self._rules))
        await asyncio.get_event_loop().create_future()

    async def _handle(self, msg: Msg) -> None:
        try:
            envelope = NATSEnvelope.model_validate_json(msg.data)
        except Exception as exc:
            _log.warning("invalid_envelope", error=str(exc))
            return
        reply = handle_convert_request(envelope, self._rules)
        outcome = reply.status
        self._handled.labels(worker="data-converter", outcome=outcome).inc()
        await msg.respond(reply.model_dump_json().encode())


async def run(settings: DataConverterSettings) -> None:
    configure_logging(level=settings.log_level)
    worker = DataConverterWorker(settings)
    health = HealthApp(is_ready=worker.is_ready)
    http_cfg = uvicorn.Config(health.asgi, host="0.0.0.0", port=settings.http_port, log_level=settings.log_level.lower())
    http = uvicorn.Server(http_cfg)
    await asyncio.gather(worker.run(), http.serve())
```

`services/data-converter/src/data_converter/__main__.py`:
```python
from __future__ import annotations
import asyncio
from data_converter.config import DataConverterSettings
from data_converter.service import run

def main() -> None:
    asyncio.run(run(DataConverterSettings()))

if __name__ == "__main__":  # pragma: no cover
    main()
```

`services/data-converter/Dockerfile`:
```dockerfile
# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS builder
WORKDIR /workspace
RUN pip install --no-cache-dir uv==0.4.20
COPY pyproject.toml uv.lock /workspace/
COPY libs /workspace/libs
COPY services/data-converter /workspace/services/data-converter
RUN uv sync --frozen --no-dev --package data-converter

FROM python:3.12-slim AS runtime
WORKDIR /workspace
COPY --from=builder /workspace/.venv /workspace/.venv
COPY --from=builder /workspace/libs /workspace/libs
COPY --from=builder /workspace/services/data-converter/src /workspace/services/data-converter/src
ENV PATH="/workspace/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1
USER nobody:nogroup
EXPOSE 8080
ENTRYPOINT ["/workspace/.venv/bin/python", "-m", "data_converter"]
```

- [ ] **Step 5: Run tests**

```bash
uv run pytest services/data-converter/tests/ -v
```
Expected: 5 PASSED

- [ ] **Step 6: Commit**

```bash
git add services/data-converter/ pyproject.toml uv.lock
git commit -m "feat(data-converter): NATS req/rep worker normalizes raw signal envelopes"
```

---

### Task 6: uns-auto-contextualizer

**Why:** Stateless NATS req/rep worker. Receives `NATSEnvelope[NormalizedSignalEnvelope]` on `uns.work.contextualize`, looks up the node alias in the mapping table, and replies with `NATSEnvelope[ContextualizeResult]`.

**Files:**
- Create: `services/uns-auto-contextualizer/pyproject.toml`
- Create: `services/uns-auto-contextualizer/Dockerfile`
- Create: `services/uns-auto-contextualizer/src/uns_auto_contextualizer/__init__.py`
- Create: `services/uns-auto-contextualizer/src/uns_auto_contextualizer/__main__.py`
- Create: `services/uns-auto-contextualizer/src/uns_auto_contextualizer/config.py`
- Create: `services/uns-auto-contextualizer/src/uns_auto_contextualizer/service.py`
- Create: `services/uns-auto-contextualizer/tests/test_uns_auto_contextualizer.py`

- [ ] **Step 1: Write failing tests**

```python
# services/uns-auto-contextualizer/tests/test_uns_auto_contextualizer.py
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
    result = contextualize(_normalized(), mapping, enterprise="uniza", site="zilina")
    assert isinstance(result, ContextualizeResult)
    assert result.uns_topic == "uniza/zilina/factory1/line_a/bottler/temperature_sensor_01/temperature"
    assert result.semantic_type == "temperature.celsius"
    assert result.uns_path.enterprise == "uniza"


def test_contextualize_unknown_node_returns_none() -> None:
    from uns_auto_contextualizer.service import contextualize

    result = contextualize(_normalized(node_id="Unknown.Node"), {}, enterprise="uniza", site="zilina")
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
```

- [ ] **Step 2: Run to verify FAIL**

```bash
uv run pytest services/uns-auto-contextualizer/tests/ -v
```

- [ ] **Step 3: Create `services/uns-auto-contextualizer/pyproject.toml`**

```toml
[project]
name = "uns-auto-contextualizer"
version = "0.0.0"
description = "Maps node aliases to ISA-95 UNS paths (spec §3.1)."
requires-python = ">=3.12"
license = { text = "Apache-2.0" }
dependencies = [
    "nats-py>=2.7",
    "pydantic>=2.8",
    "pydantic-settings>=2.5",
    "pyyaml>=6.0",
    "structlog>=24.0",
    "uvicorn>=0.30",
    "eirvah-contracts",
    "eirvah-bus",
    "eirvah-observability",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/uns_auto_contextualizer"]
```

Add `"services/uns-auto-contextualizer"` to workspace in root `pyproject.toml`, run `uv sync`.

- [ ] **Step 4: Create source files**

`services/uns-auto-contextualizer/src/uns_auto_contextualizer/__init__.py` — empty.

`services/uns-auto-contextualizer/src/uns_auto_contextualizer/config.py`:
```python
from __future__ import annotations
from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class AutoContextualizerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="UNS_AUTO_CONTEXTUALIZER_", env_file=None, extra="ignore")
    nats_servers: list[str] = ["nats://nats:4222"]
    mapping_path: Path = Path("/etc/uns-auto-contextualizer/opcua-node-to-uns-mapping.yaml")
    enterprise: str = "uniza"
    site: str = "zilina"
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
```

`services/uns-auto-contextualizer/src/uns_auto_contextualizer/service.py`:
```python
"""UNS auto-contextualizer NATS req/rep worker (spec §3.1)."""

from __future__ import annotations

import asyncio
from pathlib import Path

import structlog
import uvicorn
import yaml
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
from pydantic import BaseModel

from uns_auto_contextualizer.config import AutoContextualizerSettings

_log = structlog.get_logger("uns-auto-contextualizer")
SUBJECT = "uns.work.contextualize"


class MappingEntry(BaseModel):
    node_id: str
    area: str
    line: str
    cell: str
    equipment: str
    measurement: str
    semantic_type: str


def load_mapping(path: Path) -> dict[str, MappingEntry]:
    raw = yaml.safe_load(path.read_text())
    return {m["node_id"]: MappingEntry.model_validate(m) for m in raw["mappings"]}


def contextualize(
    normalized: NormalizedSignalEnvelope,
    mapping: dict[str, MappingEntry],
    *,
    enterprise: str,
    site: str,
) -> ContextualizeResult | None:
    entry = mapping.get(normalized.node_id)
    if entry is None:
        return None
    path = UNSPath(
        enterprise=enterprise,
        site=site,
        area=entry.area,
        line=entry.line,
        cell=entry.cell,
        equipment=entry.equipment,
        measurement=entry.measurement,
    )
    return ContextualizeResult(
        uns_topic=build_uns_topic(path),
        uns_path=path,
        semantic_type=entry.semantic_type,
    )


def handle_contextualize_request(
    envelope: NATSEnvelope,
    mapping: dict[str, MappingEntry],
    *,
    enterprise: str,
    site: str,
) -> NATSEnvelope:
    try:
        normalized = NormalizedSignalEnvelope.model_validate(envelope.payload)
        result = contextualize(normalized, mapping, enterprise=enterprise, site=site)
        if result is None:
            return NATSEnvelope(
                correlation_id=envelope.correlation_id,
                status="error",
                error=EnvelopeError(kind="UnknownNode", message=f"no mapping for node_id {normalized.node_id!r}"),
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
        self._mapping: dict[str, MappingEntry] = {}
        self._bus: BusClient | None = None
        self._ready = False
        self._handled = make_counter("worker_handler_total", "Worker handler invocations", labelnames=["worker", "outcome"])

    def is_ready(self) -> bool:
        return self._ready

    async def run(self) -> None:
        self._mapping = load_mapping(self._settings.mapping_path)
        self._bus = BusClient(servers=self._settings.nats_servers, name="uns-auto-contextualizer")
        await self._bus.connect()
        await subscribe_queue_group(nc=self._bus.nc, subject=SUBJECT, handler=self._handle)
        self._ready = True
        _log.info("contextualizer_ready", subject=SUBJECT, mappings=len(self._mapping))
        await asyncio.get_event_loop().create_future()

    async def _handle(self, msg: Msg) -> None:
        try:
            envelope = NATSEnvelope.model_validate_json(msg.data)
        except Exception as exc:
            _log.warning("invalid_envelope", error=str(exc))
            return
        reply = handle_contextualize_request(
            envelope, self._mapping,
            enterprise=self._settings.enterprise,
            site=self._settings.site,
        )
        self._handled.labels(worker="uns-auto-contextualizer", outcome=reply.status).inc()
        await msg.respond(reply.model_dump_json().encode())


async def run(settings: AutoContextualizerSettings) -> None:
    configure_logging(level=settings.log_level)
    worker = AutoContextualizerWorker(settings)
    health = HealthApp(is_ready=worker.is_ready)
    http_cfg = uvicorn.Config(health.asgi, host="0.0.0.0", port=settings.http_port, log_level=settings.log_level.lower())
    http = uvicorn.Server(http_cfg)
    await asyncio.gather(worker.run(), http.serve())
```

`services/uns-auto-contextualizer/src/uns_auto_contextualizer/__main__.py`:
```python
from __future__ import annotations
import asyncio
from uns_auto_contextualizer.config import AutoContextualizerSettings
from uns_auto_contextualizer.service import run

def main() -> None:
    asyncio.run(run(AutoContextualizerSettings()))

if __name__ == "__main__":  # pragma: no cover
    main()
```

`services/uns-auto-contextualizer/Dockerfile`:
```dockerfile
# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS builder
WORKDIR /workspace
RUN pip install --no-cache-dir uv==0.4.20
COPY pyproject.toml uv.lock /workspace/
COPY libs /workspace/libs
COPY services/uns-auto-contextualizer /workspace/services/uns-auto-contextualizer
RUN uv sync --frozen --no-dev --package uns-auto-contextualizer

FROM python:3.12-slim AS runtime
WORKDIR /workspace
COPY --from=builder /workspace/.venv /workspace/.venv
COPY --from=builder /workspace/libs /workspace/libs
COPY --from=builder /workspace/services/uns-auto-contextualizer/src /workspace/services/uns-auto-contextualizer/src
ENV PATH="/workspace/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1
USER nobody:nogroup
EXPOSE 8080
ENTRYPOINT ["/workspace/.venv/bin/python", "-m", "uns_auto_contextualizer"]
```

- [ ] **Step 5: Run tests**

```bash
uv run pytest services/uns-auto-contextualizer/tests/ -v
```
Expected: 4 PASSED

- [ ] **Step 6: Commit**

```bash
git add services/uns-auto-contextualizer/ pyproject.toml uv.lock
git commit -m "feat(uns-auto-contextualizer): NATS req/rep worker maps node alias to ISA-95 UNS path"
```

---

### Task 7: mqtt-uns-publisher

**Why:** Stateless NATS req/rep worker. Receives `NATSEnvelope[PublishRequest]` on `uns.work.publish`, assembles the full `TelemetryPayload` v1.0, and publishes it to Mosquitto at the resolved UNS topic.

**New dependency:** `aiomqtt>=2.0` (ISC licence — OSI-approved). Wraps paho-mqtt with an asyncio-native API.

**Files:**
- Create: `services/mqtt-uns-publisher/pyproject.toml`
- Create: `services/mqtt-uns-publisher/Dockerfile`
- Create: `services/mqtt-uns-publisher/src/mqtt_uns_publisher/__init__.py`
- Create: `services/mqtt-uns-publisher/src/mqtt_uns_publisher/__main__.py`
- Create: `services/mqtt-uns-publisher/src/mqtt_uns_publisher/config.py`
- Create: `services/mqtt-uns-publisher/src/mqtt_uns_publisher/service.py`
- Create: `services/mqtt-uns-publisher/tests/test_mqtt_uns_publisher.py`

- [ ] **Step 1: Write failing tests**

```python
# services/mqtt-uns-publisher/tests/test_mqtt_uns_publisher.py
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_contracts.pipeline import PublishRequest
from eirvah_contracts.telemetry import TelemetryPayload
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
    )


def test_build_telemetry_payload() -> None:
    from mqtt_uns_publisher.service import build_telemetry_payload

    req = _pub_request()
    payload = build_telemetry_payload(req)
    assert isinstance(payload, TelemetryPayload)
    assert payload.schema_version == "1.0"
    assert payload.correlation_id == req.correlation_id
    assert payload.value == 23.4
    assert payload.unit == "degC"
    assert payload.quality == "good"
    assert payload.source.protocol == "opcua"
    assert payload.source.node_id == "Bottler.Temperature01"
    assert payload.timestamps.edge_publish is not None
    assert payload.timestamps.edge_publish >= payload.timestamps.edge_ingress


def test_build_telemetry_payload_json_validates() -> None:
    from mqtt_uns_publisher.service import build_telemetry_payload

    req = _pub_request()
    payload = build_telemetry_payload(req)
    json_str = payload.model_dump_json()
    restored = TelemetryPayload.model_validate_json(json_str)
    assert restored.correlation_id == req.correlation_id
```

- [ ] **Step 2: Run to verify FAIL**

```bash
uv run pytest services/mqtt-uns-publisher/tests/ -v
```

- [ ] **Step 3: Create `services/mqtt-uns-publisher/pyproject.toml`**

```toml
[project]
name = "mqtt-uns-publisher"
version = "0.0.0"
description = "Publishes TelemetryPayload v1.0 to Mosquitto (spec §3.1)."
requires-python = ">=3.12"
license = { text = "Apache-2.0" }
dependencies = [
    "aiomqtt>=2.0",
    "nats-py>=2.7",
    "pydantic>=2.8",
    "pydantic-settings>=2.5",
    "structlog>=24.0",
    "uvicorn>=0.30",
    "eirvah-contracts",
    "eirvah-bus",
    "eirvah-observability",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/mqtt_uns_publisher"]
```

Add `"services/mqtt-uns-publisher"` to workspace, run `uv sync`.

- [ ] **Step 4: Create source files**

`services/mqtt-uns-publisher/src/mqtt_uns_publisher/__init__.py` — empty.

`services/mqtt-uns-publisher/src/mqtt_uns_publisher/config.py`:
```python
from __future__ import annotations
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class MqttPublisherSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MQTT_UNS_PUBLISHER_", env_file=None, extra="ignore")
    nats_servers: list[str] = ["nats://nats:4222"]
    mqtt_host: str = "mosquitto"
    mqtt_port: int = 1883
    mqtt_username: str = "eirvah"
    mqtt_password: str = "eirvah-dev-password"
    mqtt_client_id: str = "mqtt-uns-publisher"
    qos: int = Field(default=1, ge=0, le=2)
    retain: bool = False
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
    reconnect_delay_s: float = 5.0
```

`services/mqtt-uns-publisher/src/mqtt_uns_publisher/service.py`:
```python
"""MQTT UNS publisher NATS req/rep worker (spec §3.1)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import aiomqtt
import structlog
import uvicorn
from eirvah_bus.client import BusClient
from eirvah_bus.consumer import subscribe_queue_group
from eirvah_contracts.envelope import EnvelopeError, NATSEnvelope
from eirvah_contracts.pipeline import PublishRequest
from eirvah_contracts.telemetry import TelemetryPayload, TelemetrySource, TelemetryTimestamps
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import configure_logging
from eirvah_observability.metrics import make_counter, make_gauge
from nats.aio.msg import Msg

from mqtt_uns_publisher.config import MqttPublisherSettings

_log = structlog.get_logger("mqtt-uns-publisher")
SUBJECT = "uns.work.publish"


def build_telemetry_payload(req: PublishRequest) -> TelemetryPayload:
    return TelemetryPayload(
        correlation_id=req.correlation_id,
        value=req.value,
        value_type=req.value_type,
        semantic_type=req.semantic_type,
        unit=req.unit,
        quality=req.quality,
        uns_path=req.uns_path,
        source=TelemetrySource(
            protocol="opcua",
            endpoint=req.source_endpoint,
            node_id=req.source_node_id,
        ),
        timestamps=TelemetryTimestamps(
            source=req.source_timestamp,
            edge_ingress=req.edge_ingress,
            edge_publish=datetime.now(UTC),
        ),
    )


class MqttPublisherWorker:
    def __init__(self, settings: MqttPublisherSettings) -> None:
        self._settings = settings
        self._mqtt_client: aiomqtt.Client | None = None
        self._nats_ready = False
        self._mqtt_ready = False
        self._reconnect_event = asyncio.Event()
        self._handled = make_counter("worker_handler_total", "Worker handler invocations", labelnames=["worker", "outcome"])
        self._conn_state = make_gauge("ingress_connection_state", "Connection state", labelnames=["ingress", "state"])

    def is_ready(self) -> bool:
        return self._nats_ready and self._mqtt_ready

    async def run(self) -> None:
        bus = BusClient(servers=self._settings.nats_servers, name="mqtt-uns-publisher")
        await bus.connect()
        await subscribe_queue_group(nc=bus.nc, subject=SUBJECT, handler=self._handle)
        self._nats_ready = True

        while True:
            try:
                async with aiomqtt.Client(
                    hostname=self._settings.mqtt_host,
                    port=self._settings.mqtt_port,
                    username=self._settings.mqtt_username,
                    password=self._settings.mqtt_password,
                    identifier=self._settings.mqtt_client_id,
                ) as client:
                    self._mqtt_client = client
                    self._mqtt_ready = True
                    self._reconnect_event.clear()
                    self._conn_state.labels(ingress="mqtt", state="connected").set(1)
                    self._conn_state.labels(ingress="mqtt", state="disconnected").set(0)
                    _log.info("mqtt_connected", host=self._settings.mqtt_host)
                    await self._reconnect_event.wait()
            except Exception as exc:
                self._mqtt_ready = False
                self._mqtt_client = None
                self._conn_state.labels(ingress="mqtt", state="connected").set(0)
                self._conn_state.labels(ingress="mqtt", state="disconnected").set(1)
                _log.warning("mqtt_disconnected", error=str(exc))
                await asyncio.sleep(self._settings.reconnect_delay_s)

    async def _handle(self, msg: Msg) -> None:
        try:
            envelope = NATSEnvelope.model_validate_json(msg.data)
            req = PublishRequest.model_validate(envelope.payload)

            if self._mqtt_client is None:
                raise RuntimeError("MQTT not connected")

            telemetry = build_telemetry_payload(req)
            try:
                await self._mqtt_client.publish(
                    req.uns_topic,
                    payload=telemetry.model_dump_json().encode(),
                    qos=self._settings.qos,
                    retain=self._settings.retain,
                )
            except aiomqtt.MqttError as mqtt_exc:
                self._reconnect_event.set()
                raise mqtt_exc

            self._handled.labels(worker="mqtt-uns-publisher", outcome="ok").inc()
            reply = NATSEnvelope(correlation_id=envelope.correlation_id)
        except Exception as exc:
            cid = "UNKNOWN"
            try:
                cid = NATSEnvelope.model_validate_json(msg.data).correlation_id
            except Exception:
                pass
            self._handled.labels(worker="mqtt-uns-publisher", outcome="error").inc()
            reply = NATSEnvelope(
                correlation_id=cid,
                status="error",
                error=EnvelopeError(kind=type(exc).__name__, message=str(exc)[:200]),
            )
        await msg.respond(reply.model_dump_json().encode())


async def run(settings: MqttPublisherSettings) -> None:
    configure_logging(level=settings.log_level)
    worker = MqttPublisherWorker(settings)
    health = HealthApp(is_ready=worker.is_ready)
    http_cfg = uvicorn.Config(health.asgi, host="0.0.0.0", port=settings.http_port, log_level=settings.log_level.lower())
    http = uvicorn.Server(http_cfg)
    await asyncio.gather(worker.run(), http.serve())
```

`services/mqtt-uns-publisher/src/mqtt_uns_publisher/__main__.py`:
```python
from __future__ import annotations
import asyncio
from mqtt_uns_publisher.config import MqttPublisherSettings
from mqtt_uns_publisher.service import run

def main() -> None:
    asyncio.run(run(MqttPublisherSettings()))

if __name__ == "__main__":  # pragma: no cover
    main()
```

`services/mqtt-uns-publisher/Dockerfile`:
```dockerfile
# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS builder
WORKDIR /workspace
RUN pip install --no-cache-dir uv==0.4.20
COPY pyproject.toml uv.lock /workspace/
COPY libs /workspace/libs
COPY services/mqtt-uns-publisher /workspace/services/mqtt-uns-publisher
RUN uv sync --frozen --no-dev --package mqtt-uns-publisher

FROM python:3.12-slim AS runtime
WORKDIR /workspace
COPY --from=builder /workspace/.venv /workspace/.venv
COPY --from=builder /workspace/libs /workspace/libs
COPY --from=builder /workspace/services/mqtt-uns-publisher/src /workspace/services/mqtt-uns-publisher/src
ENV PATH="/workspace/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1
USER nobody:nogroup
EXPOSE 8080
ENTRYPOINT ["/workspace/.venv/bin/python", "-m", "mqtt_uns_publisher"]
```

- [ ] **Step 5: Run tests**

```bash
uv run pytest services/mqtt-uns-publisher/tests/ -v
```
Expected: 2 PASSED

- [ ] **Step 6: Commit**

```bash
git add services/mqtt-uns-publisher/ pyproject.toml uv.lock
git commit -m "feat(mqtt-uns-publisher): NATS req/rep worker publishes TelemetryPayload to Mosquitto"
```

---

### Task 8: uns-contextualizer-orchestrator — models, metrics, config

**Why:** The orchestrator is the most complex service. Split into two tasks. This task builds the config loading, internal models, and Prometheus metrics — the foundation the pipeline runner sits on.

**Files:**
- Create: `services/uns-contextualizer-orchestrator/pyproject.toml`
- Create: `services/uns-contextualizer-orchestrator/src/uns_contextualizer_orchestrator/__init__.py`
- Create: `services/uns-contextualizer-orchestrator/src/uns_contextualizer_orchestrator/config.py`
- Create: `services/uns-contextualizer-orchestrator/src/uns_contextualizer_orchestrator/models.py`
- Create: `services/uns-contextualizer-orchestrator/src/uns_contextualizer_orchestrator/metrics.py`

- [ ] **Step 1: Write failing tests (models + metrics)**

```python
# services/uns-contextualizer-orchestrator/tests/test_uns_contextualizer_orchestrator.py
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from prometheus_client.registry import CollectorRegistry


# ---- models ----------------------------------------------------------------

def test_pipeline_config_loads_from_yaml(tmp_path: Path) -> None:
    from uns_contextualizer_orchestrator.models import PipelineConfig, PipelineStage

    yaml_text = """
stages:
  - name: convert
    subject: uns.work.convert
    timeout_s: 2.0
  - name: contextualize
    subject: uns.work.contextualize
    timeout_s: 2.0
  - name: publish
    subject: uns.work.publish
    timeout_s: 2.0
dlq_subject: uns.dlq.telemetry
"""
    cfg_file = tmp_path / "pipeline.yaml"
    cfg_file.write_text(yaml_text)

    from uns_contextualizer_orchestrator.models import load_pipeline_config
    cfg = load_pipeline_config(cfg_file)
    assert len(cfg.stages) == 3
    assert cfg.stages[0].name == "convert"
    assert cfg.stages[0].timeout_s == 2.0
    assert cfg.dlq_subject == "uns.dlq.telemetry"


def test_pipeline_context_builds_publish_request() -> None:
    from uns_contextualizer_orchestrator.models import PipelineContext
    from eirvah_contracts.signals import RawSignalEnvelope, NormalizedSignalEnvelope
    from eirvah_contracts.pipeline import ContextualizeResult
    from eirvah_contracts.uns import UNSPath, build_uns_topic

    now = datetime.now(UTC)
    raw = RawSignalEnvelope(
        source_endpoint="opc.tcp://test:4840",
        node_id="Bottler.Temperature01",
        value=23.4,
        value_type="double",
        quality="good",
        source_timestamp=now,
        server_timestamp=now,
        received_at=now,
    )
    normalized = NormalizedSignalEnvelope(
        node_id="Bottler.Temperature01",
        value=23.4,
        value_type="double",
        unit="degC",
        quality="good",
        source_timestamp=now,
        received_at=now,
    )
    uns_path = UNSPath(
        enterprise="uniza", site="zilina", area="factory1",
        line="line_a", cell="bottler",
        equipment="temperature_sensor_01", measurement="temperature",
    )
    ctx_result = ContextualizeResult(
        uns_topic=build_uns_topic(uns_path),
        uns_path=uns_path,
        semantic_type="temperature.celsius",
    )
    ctx = PipelineContext(
        correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A",
        raw=raw,
        ingress_at=now,
        normalized=normalized,
        contextualized=ctx_result,
    )
    req = ctx.build_publish_request()
    assert req.value == 23.4
    assert req.unit == "degC"
    assert req.uns_topic == build_uns_topic(uns_path)
    assert req.source_node_id == "Bottler.Temperature01"


# ---- metrics ----------------------------------------------------------------

def test_pipeline_metrics_create_without_error() -> None:
    from uns_contextualizer_orchestrator.metrics import PipelineMetrics
    reg = CollectorRegistry()
    m = PipelineMetrics(registry=reg)
    m.inc_success(path="telemetry")
    m.inc_stage_error(path="telemetry", stage="convert", reason="timeout")
    m.observe_e2e_latency(path="telemetry", seconds=0.05)
```

- [ ] **Step 2: Run to verify FAIL**

```bash
uv run pytest services/uns-contextualizer-orchestrator/tests/ -v
```

- [ ] **Step 3: Create `services/uns-contextualizer-orchestrator/pyproject.toml`**

```toml
[project]
name = "uns-contextualizer-orchestrator"
version = "0.0.0"
description = "Telemetry pipeline orchestrator — owns stage sequencing (spec §3.1)."
requires-python = ">=3.12"
license = { text = "Apache-2.0" }
dependencies = [
    "nats-py>=2.7",
    "pydantic>=2.8",
    "pydantic-settings>=2.5",
    "pyyaml>=6.0",
    "python-ulid>=2.0",
    "structlog>=24.0",
    "uvicorn>=0.30",
    "eirvah-contracts",
    "eirvah-bus",
    "eirvah-observability",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/uns_contextualizer_orchestrator"]
```

Add `"services/uns-contextualizer-orchestrator"` to workspace, run `uv sync`.

- [ ] **Step 4: Create `models.py`**

`services/uns-contextualizer-orchestrator/src/uns_contextualizer_orchestrator/models.py`:
```python
"""Internal models for the UNS contextualizer orchestrator."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import yaml
from eirvah_contracts.pipeline import ContextualizeResult, PublishRequest
from eirvah_contracts.signals import NormalizedSignalEnvelope, RawSignalEnvelope
from pydantic import BaseModel


class PipelineStage(BaseModel):
    name: str
    subject: str
    timeout_s: float = 2.0


class PipelineConfig(BaseModel):
    stages: list[PipelineStage]
    dlq_subject: str


def load_pipeline_config(path: Path) -> PipelineConfig:
    raw = yaml.safe_load(path.read_text())
    return PipelineConfig.model_validate(raw)


@dataclass
class PipelineContext:
    correlation_id: str
    raw: RawSignalEnvelope
    ingress_at: datetime
    normalized: NormalizedSignalEnvelope | None = None
    contextualized: ContextualizeResult | None = None

    def build_publish_request(self) -> PublishRequest:
        assert self.normalized is not None, "normalized must be set before building PublishRequest"
        assert self.contextualized is not None, "contextualized must be set before building PublishRequest"
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
            edge_ingress=self.raw.received_at,
        )
```

- [ ] **Step 5: Create `metrics.py`**

`services/uns-contextualizer-orchestrator/src/uns_contextualizer_orchestrator/metrics.py`:
```python
"""Prometheus metrics for the UNS contextualizer orchestrator (spec §§6.1, 7.2)."""

from __future__ import annotations

from prometheus_client.registry import REGISTRY, CollectorRegistry

from eirvah_observability.metrics import make_counter, make_histogram


class PipelineMetrics:
    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self._success = make_counter(
            "pipeline_success_total",
            "Pipeline messages processed successfully end-to-end",
            labelnames=["path"],
            registry=registry,
        )
        self._stage_error = make_counter(
            "pipeline_stage_error_total",
            "Pipeline stage errors (worker replied status=error)",
            labelnames=["path", "stage", "reason"],
            registry=registry,
        )
        self._stage_timeout = make_counter(
            "pipeline_stage_timeout_total",
            "Pipeline stage timeouts (NATS request-reply timed out)",
            labelnames=["path", "stage"],
            registry=registry,
        )
        self._e2e_latency = make_histogram(
            "pipeline_e2e_latency_seconds",
            "End-to-end latency from ingress to MQTT publish",
            labelnames=["path"],
            registry=registry,
        )

    def inc_success(self, *, path: str) -> None:
        self._success.labels(path=path).inc()

    def inc_stage_error(self, *, path: str, stage: str, reason: str) -> None:
        self._stage_error.labels(path=path, stage=stage, reason=reason).inc()

    def inc_stage_timeout(self, *, path: str, stage: str) -> None:
        self._stage_timeout.labels(path=path, stage=stage).inc()

    def observe_e2e_latency(self, *, path: str, seconds: float) -> None:
        self._e2e_latency.labels(path=path).observe(seconds)
```

`services/uns-contextualizer-orchestrator/src/uns_contextualizer_orchestrator/__init__.py` — empty.

- [ ] **Step 6: Run tests**

```bash
uv run pytest services/uns-contextualizer-orchestrator/tests/ -v
```
Expected: 3 PASSED

- [ ] **Step 7: Commit**

```bash
git add services/uns-contextualizer-orchestrator/ pyproject.toml uv.lock
git commit -m "feat(orchestrator): models, metrics, and pipeline config loader"
```

---

### Task 9: uns-contextualizer-orchestrator — pipeline runner, service, and full tests

**Files:**
- Create: `services/uns-contextualizer-orchestrator/src/uns_contextualizer_orchestrator/config.py`
- Create: `services/uns-contextualizer-orchestrator/src/uns_contextualizer_orchestrator/pipeline.py`
- Create: `services/uns-contextualizer-orchestrator/src/uns_contextualizer_orchestrator/service.py`
- Create: `services/uns-contextualizer-orchestrator/src/uns_contextualizer_orchestrator/__main__.py`
- Create: `services/uns-contextualizer-orchestrator/Dockerfile`
- Extend: `services/uns-contextualizer-orchestrator/tests/test_uns_contextualizer_orchestrator.py`

- [ ] **Step 1: Write failing pipeline runner tests** — append to `tests/test_uns_contextualizer_orchestrator.py`

```python
# append to the existing test file

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch


def _corr_id() -> str:
    return "01HZXC8P9G7Q3M6V0K2T8R5W4A"


def _raw_envelope() -> "NATSEnvelope":
    from eirvah_contracts.envelope import NATSEnvelope
    from eirvah_contracts.signals import RawSignalEnvelope

    now = datetime.now(UTC)
    raw = RawSignalEnvelope(
        source_endpoint="opc.tcp://test:4840",
        node_id="Bottler.Temperature01",
        value=23.4,
        value_type="double",
        quality="good",
        source_timestamp=now,
        server_timestamp=now,
        received_at=now,
    )
    return NATSEnvelope(correlation_id=_corr_id(), payload=raw.model_dump(mode="json"))


@pytest.mark.asyncio
async def test_run_pipeline_success() -> None:
    from uns_contextualizer_orchestrator.models import PipelineConfig, PipelineContext, PipelineStage
    from uns_contextualizer_orchestrator.metrics import PipelineMetrics
    from uns_contextualizer_orchestrator.pipeline import run_pipeline
    from eirvah_contracts.envelope import NATSEnvelope
    from eirvah_contracts.pipeline import ContextualizeResult
    from eirvah_contracts.signals import NormalizedSignalEnvelope
    from eirvah_contracts.uns import UNSPath, build_uns_topic

    now = datetime.now(UTC)
    uns_path = UNSPath(
        enterprise="uniza", site="zilina", area="factory1",
        line="line_a", cell="bottler",
        equipment="temperature_sensor_01", measurement="temperature",
    )
    normalized = NormalizedSignalEnvelope(
        node_id="Bottler.Temperature01", value=23.4, value_type="double",
        unit="degC", quality="good", source_timestamp=now, received_at=now,
    )
    ctx_result = ContextualizeResult(
        uns_topic=build_uns_topic(uns_path), uns_path=uns_path, semantic_type="temperature.celsius",
    )

    cfg = PipelineConfig(
        stages=[
            PipelineStage(name="convert", subject="uns.work.convert", timeout_s=2.0),
            PipelineStage(name="contextualize", subject="uns.work.contextualize", timeout_s=2.0),
            PipelineStage(name="publish", subject="uns.work.publish", timeout_s=2.0),
        ],
        dlq_subject="uns.dlq.telemetry",
    )

    async def fake_request_reply(*, nc, subject, payload, correlation_id, timeout_s):
        msg = MagicMock()
        if subject == "uns.work.convert":
            reply = NATSEnvelope(correlation_id=correlation_id, payload=normalized.model_dump(mode="json"))
        elif subject == "uns.work.contextualize":
            reply = NATSEnvelope(correlation_id=correlation_id, payload=ctx_result.model_dump(mode="json"))
        else:
            reply = NATSEnvelope(correlation_id=correlation_id)
        msg.data = reply.model_dump_json().encode()
        return msg

    nc_mock = MagicMock()
    nc_mock.publish = AsyncMock()
    reg = CollectorRegistry()
    metrics = PipelineMetrics(registry=reg)

    envelope = _raw_envelope()
    await run_pipeline(envelope=envelope, cfg=cfg, nc=nc_mock, metrics=metrics, request_reply_fn=fake_request_reply)

    # DLQ was not published (success path)
    nc_mock.publish.assert_not_called()


@pytest.mark.asyncio
async def test_run_pipeline_stage_timeout_publishes_dlq() -> None:
    from uns_contextualizer_orchestrator.models import PipelineConfig, PipelineStage
    from uns_contextualizer_orchestrator.metrics import PipelineMetrics
    from uns_contextualizer_orchestrator.pipeline import run_pipeline
    from eirvah_bus.request_reply import RequestTimeout

    cfg = PipelineConfig(
        stages=[PipelineStage(name="convert", subject="uns.work.convert", timeout_s=2.0)],
        dlq_subject="uns.dlq.telemetry",
    )

    async def fake_request_reply(**kwargs):
        raise RequestTimeout("timed out")

    nc_mock = MagicMock()
    nc_mock.publish = AsyncMock()
    reg = CollectorRegistry()
    metrics = PipelineMetrics(registry=reg)

    envelope = _raw_envelope()
    await run_pipeline(envelope=envelope, cfg=cfg, nc=nc_mock, metrics=metrics, request_reply_fn=fake_request_reply)

    nc_mock.publish.assert_called_once()
    call_args = nc_mock.publish.call_args
    assert call_args[0][0] == "uns.dlq.telemetry"
```

- [ ] **Step 2: Run to verify FAIL**

```bash
uv run pytest services/uns-contextualizer-orchestrator/tests/ -v -k "pipeline"
```

- [ ] **Step 3: Create `config.py`**

```python
# services/uns-contextualizer-orchestrator/src/uns_contextualizer_orchestrator/config.py
from __future__ import annotations
from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class OrchestratorSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="UNS_CONTEXTUALIZER_ORCHESTRATOR_", env_file=None, extra="ignore")
    nats_servers: list[str] = ["nats://nats:4222"]
    pipeline_config_path: Path = Path("/etc/uns-contextualizer-orchestrator/uns-contextualizer.yaml")
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
```

- [ ] **Step 4: Create `pipeline.py`**

```python
# services/uns-contextualizer-orchestrator/src/uns_contextualizer_orchestrator/pipeline.py
"""Pipeline runner: drives convert → contextualize → publish for one message."""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from typing import Any

import structlog
from eirvah_bus.request_reply import RequestTimeout, request_reply
from eirvah_contracts.envelope import EnvelopeError, NATSEnvelope
from eirvah_contracts.pipeline import ContextualizeResult
from eirvah_contracts.signals import NormalizedSignalEnvelope, RawSignalEnvelope
from nats.aio.client import Client as NATSClient

from uns_contextualizer_orchestrator.metrics import PipelineMetrics
from uns_contextualizer_orchestrator.models import PipelineConfig, PipelineContext

_log = structlog.get_logger("uns-contextualizer-orchestrator")

_PATH = "telemetry"

# Allow injection of a fake request_reply in tests
RequestReplyFn = Callable[..., Coroutine[Any, Any, Any]]


async def run_pipeline(
    *,
    envelope: NATSEnvelope,
    cfg: PipelineConfig,
    nc: NATSClient,
    metrics: PipelineMetrics,
    request_reply_fn: RequestReplyFn = request_reply,
) -> None:
    ingress_at = datetime.now(UTC)

    try:
        raw = RawSignalEnvelope.model_validate(envelope.payload)
    except Exception as exc:
        _log.warning("bad_ingress_envelope", error=str(exc), correlation_id=envelope.correlation_id)
        return

    ctx = PipelineContext(
        correlation_id=envelope.correlation_id,
        raw=raw,
        ingress_at=ingress_at,
    )

    for stage in cfg.stages:
        payload_dict: dict[str, Any]
        if stage.name == "convert":
            payload_dict = raw.model_dump(mode="json")
        elif stage.name == "contextualize":
            assert ctx.normalized is not None
            payload_dict = ctx.normalized.model_dump(mode="json")
        elif stage.name == "publish":
            payload_dict = ctx.build_publish_request().model_dump(mode="json")
        else:
            _log.error("unknown_stage", stage=stage.name)
            continue

        req_env = NATSEnvelope(correlation_id=ctx.correlation_id, payload=payload_dict)

        try:
            reply_msg = await request_reply_fn(
                nc=nc,
                subject=stage.subject,
                payload=req_env.model_dump_json().encode(),
                correlation_id=ctx.correlation_id,
                timeout_s=stage.timeout_s,
            )
        except RequestTimeout:
            metrics.inc_stage_timeout(path=_PATH, stage=stage.name)
            _log.warning("stage_timeout", stage=stage.name, correlation_id=ctx.correlation_id)
            await _publish_dlq(nc, cfg.dlq_subject, ctx, failing_stage=stage.name, reason="timeout")
            return
        except Exception as exc:
            metrics.inc_stage_error(path=_PATH, stage=stage.name, reason=type(exc).__name__)
            _log.warning("stage_error", stage=stage.name, error=str(exc), correlation_id=ctx.correlation_id)
            await _publish_dlq(nc, cfg.dlq_subject, ctx, failing_stage=stage.name, reason=type(exc).__name__)
            return

        try:
            reply_env = NATSEnvelope.model_validate_json(reply_msg.data)
        except Exception as exc:
            metrics.inc_stage_error(path=_PATH, stage=stage.name, reason="BadReply")
            await _publish_dlq(nc, cfg.dlq_subject, ctx, failing_stage=stage.name, reason="BadReply")
            return

        if reply_env.status == "error":
            reason = reply_env.error.kind if reply_env.error else "unknown"
            metrics.inc_stage_error(path=_PATH, stage=stage.name, reason=reason)
            _log.warning("stage_replied_error", stage=stage.name, reason=reason, correlation_id=ctx.correlation_id)
            await _publish_dlq(nc, cfg.dlq_subject, ctx, failing_stage=stage.name, reason=reason)
            return

        # Accumulate context
        if stage.name == "convert":
            ctx.normalized = NormalizedSignalEnvelope.model_validate(reply_env.payload)
        elif stage.name == "contextualize":
            ctx.contextualized = ContextualizeResult.model_validate(reply_env.payload)

    elapsed = (datetime.now(UTC) - ingress_at).total_seconds()
    metrics.inc_success(path=_PATH)
    metrics.observe_e2e_latency(path=_PATH, seconds=elapsed)
    _log.info("pipeline_success", correlation_id=ctx.correlation_id, latency_s=elapsed)


async def _publish_dlq(
    nc: NATSClient,
    dlq_subject: str,
    ctx: PipelineContext,
    *,
    failing_stage: str,
    reason: str,
) -> None:
    dlq_payload = NATSEnvelope(
        correlation_id=ctx.correlation_id,
        status="error",
        error=EnvelopeError(kind="PipelineFailure", message=f"stage={failing_stage} reason={reason}"),
        payload=ctx.raw.model_dump(mode="json"),
    )
    await nc.publish(dlq_subject, dlq_payload.model_dump_json().encode())
```

- [ ] **Step 5: Create `service.py`**

```python
# services/uns-contextualizer-orchestrator/src/uns_contextualizer_orchestrator/service.py
"""Main service entry point for the UNS contextualizer orchestrator."""

from __future__ import annotations

import asyncio

import structlog
import uvicorn
from eirvah_bus.client import BusClient
from eirvah_bus.consumer import subscribe_queue_group
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import configure_logging
from nats.aio.msg import Msg

from uns_contextualizer_orchestrator.config import OrchestratorSettings
from uns_contextualizer_orchestrator.metrics import PipelineMetrics
from uns_contextualizer_orchestrator.models import PipelineConfig, load_pipeline_config
from uns_contextualizer_orchestrator.pipeline import run_pipeline

_log = structlog.get_logger("uns-contextualizer-orchestrator")

INGRESS_SUBJECT = "uns.ingress.raw"


class OrchestratorRuntime:
    def __init__(self, settings: OrchestratorSettings) -> None:
        self._settings = settings
        self._bus: BusClient | None = None
        self._cfg: PipelineConfig | None = None
        self._metrics = PipelineMetrics()
        self._ready = False

    def is_ready(self) -> bool:
        return self._ready

    async def run(self) -> None:
        self._cfg = load_pipeline_config(self._settings.pipeline_config_path)
        self._bus = BusClient(servers=self._settings.nats_servers, name="uns-contextualizer-orchestrator")
        await self._bus.connect()
        await subscribe_queue_group(nc=self._bus.nc, subject=INGRESS_SUBJECT, handler=self._handle)
        self._ready = True
        _log.info("orchestrator_ready", subject=INGRESS_SUBJECT, stages=[s.name for s in self._cfg.stages])
        await asyncio.get_event_loop().create_future()

    async def _handle(self, msg: Msg) -> None:
        assert self._cfg is not None and self._bus is not None
        try:
            envelope = NATSEnvelope.model_validate_json(msg.data)
        except Exception as exc:
            _log.warning("invalid_ingress_message", error=str(exc))
            return
        await run_pipeline(
            envelope=envelope,
            cfg=self._cfg,
            nc=self._bus.nc,
            metrics=self._metrics,
        )


async def run(settings: OrchestratorSettings) -> None:
    configure_logging(level=settings.log_level)
    runtime = OrchestratorRuntime(settings)
    health = HealthApp(is_ready=runtime.is_ready)
    http_cfg = uvicorn.Config(health.asgi, host="0.0.0.0", port=settings.http_port, log_level=settings.log_level.lower())
    http = uvicorn.Server(http_cfg)
    await asyncio.gather(runtime.run(), http.serve())
```

`services/uns-contextualizer-orchestrator/src/uns_contextualizer_orchestrator/__main__.py`:
```python
from __future__ import annotations
import asyncio
from uns_contextualizer_orchestrator.config import OrchestratorSettings
from uns_contextualizer_orchestrator.service import run

def main() -> None:
    asyncio.run(run(OrchestratorSettings()))

if __name__ == "__main__":  # pragma: no cover
    main()
```

`services/uns-contextualizer-orchestrator/Dockerfile`:
```dockerfile
# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS builder
WORKDIR /workspace
RUN pip install --no-cache-dir uv==0.4.20
COPY pyproject.toml uv.lock /workspace/
COPY libs /workspace/libs
COPY services/uns-contextualizer-orchestrator /workspace/services/uns-contextualizer-orchestrator
RUN uv sync --frozen --no-dev --package uns-contextualizer-orchestrator

FROM python:3.12-slim AS runtime
WORKDIR /workspace
COPY --from=builder /workspace/.venv /workspace/.venv
COPY --from=builder /workspace/libs /workspace/libs
COPY --from=builder /workspace/services/uns-contextualizer-orchestrator/src /workspace/services/uns-contextualizer-orchestrator/src
ENV PATH="/workspace/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1
USER nobody:nogroup
EXPOSE 8080
ENTRYPOINT ["/workspace/.venv/bin/python", "-m", "uns_contextualizer_orchestrator"]
```

- [ ] **Step 6: Run all orchestrator tests**

```bash
uv run pytest services/uns-contextualizer-orchestrator/tests/ -v
```
Expected: all PASSED (5 tests)

- [ ] **Step 7: Run full unit test suite**

```bash
uv run pytest services/ libs/ -v
```
Expected: all PASSED

- [ ] **Step 8: Commit**

```bash
git add services/uns-contextualizer-orchestrator/
git commit -m "feat(orchestrator): pipeline runner, service entry point, and full unit tests"
```

---

### Task 10: Workspace + Kustomize deploy manifests + dev scripts

**Why:** Wire all 5 services into the uv workspace (full update), create Kustomize Deployment/Service/ConfigMap resources for each, update the base kustomization to include them, and update `build_all.sh` and `dev_up.sh`.

**Files:**
- Modify: `pyproject.toml` (add all 5 services)
- Create: `deploy/k3s/base/opcua-data-subscriber/` (deployment.yaml, service.yaml, configmap.yaml, kustomization.yaml)
- Create: `deploy/k3s/base/data-converter/` (same)
- Create: `deploy/k3s/base/uns-auto-contextualizer/` (same)
- Create: `deploy/k3s/base/mqtt-uns-publisher/` (same)
- Create: `deploy/k3s/base/uns-contextualizer-orchestrator/` (same)
- Modify: `deploy/k3s/base/kustomization.yaml`
- Modify: `scripts/build_all.sh`
- Modify: `scripts/dev_up.sh`

- [ ] **Step 1: Update root `pyproject.toml`** — final workspace state:

```toml
[tool.uv.workspace]
members = [
    "libs/eirvah-contracts",
    "libs/eirvah-bus",
    "libs/eirvah-observability",
    "services/opcua-simulator",
    "services/opcua-data-subscriber",
    "services/data-converter",
    "services/uns-auto-contextualizer",
    "services/mqtt-uns-publisher",
    "services/uns-contextualizer-orchestrator",
]

[tool.uv.sources]
eirvah-contracts             = { workspace = true }
eirvah-bus                   = { workspace = true }
eirvah-observability         = { workspace = true }
opcua-simulator              = { workspace = true }
opcua-data-subscriber        = { workspace = true }
data-converter               = { workspace = true }
uns-auto-contextualizer      = { workspace = true }
mqtt-uns-publisher           = { workspace = true }
uns-contextualizer-orchestrator = { workspace = true }

[dependency-groups]
dev = [
    "eirvah-contracts",
    "eirvah-bus",
    "eirvah-observability",
    "opcua-simulator",
    "opcua-data-subscriber",
    "data-converter",
    "uns-auto-contextualizer",
    "mqtt-uns-publisher",
    "uns-contextualizer-orchestrator",
    "pytest>=8.3",
    "pytest-asyncio>=0.24",
    "httpx>=0.27",
    "ruff>=0.6",
    "mypy>=1.11",
]
```

Run `uv sync` after.

- [ ] **Step 2: Create Kustomize base for `opcua-data-subscriber`**

`deploy/k3s/base/opcua-data-subscriber/kustomization.yaml`:
```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - deployment.yaml
  - service.yaml
configMapGenerator:
  - name: opcua-data-subscriber-config
    files:
      - opcua-node-list.yaml
```

`deploy/k3s/base/opcua-data-subscriber/deployment.yaml`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: opcua-data-subscriber
  labels: { app.kubernetes.io/name: opcua-data-subscriber }
spec:
  replicas: 1
  selector:
    matchLabels: { app.kubernetes.io/name: opcua-data-subscriber }
  template:
    metadata:
      labels: { app.kubernetes.io/name: opcua-data-subscriber }
      annotations:
        eirvah.uniza/scrape: "true"
        eirvah.uniza/scrape-port: "8080"
    spec:
      containers:
        - name: opcua-data-subscriber
          image: opcua-data-subscriber:local
          imagePullPolicy: IfNotPresent
          env:
            - name: OPCUA_DATA_SUBSCRIBER_NATS_SERVERS
              value: '["nats://nats:4222"]'
          ports:
            - { name: http, containerPort: 8080 }
          volumeMounts:
            - name: config
              mountPath: /etc/opcua-data-subscriber
              readOnly: true
          readinessProbe:
            httpGet: { path: /readyz, port: 8080 }
            initialDelaySeconds: 5
            periodSeconds: 5
          livenessProbe:
            httpGet: { path: /healthz, port: 8080 }
            initialDelaySeconds: 15
            periodSeconds: 10
          resources:
            requests: { cpu: "25m", memory: "128Mi" }
            limits:   { cpu: "200m", memory: "256Mi" }
      volumes:
        - name: config
          configMap:
            name: opcua-data-subscriber-config
```

`deploy/k3s/base/opcua-data-subscriber/service.yaml`:
```yaml
apiVersion: v1
kind: Service
metadata:
  name: opcua-data-subscriber
  labels: { app.kubernetes.io/name: opcua-data-subscriber }
spec:
  selector: { app.kubernetes.io/name: opcua-data-subscriber }
  ports:
    - { name: http, port: 8080, targetPort: 8080 }
```

The kustomization uses `configMapGenerator` with a file reference — create a symlink or copy `config/opcua-node-list.yaml` into the base dir. Use a symlink to avoid duplication:

```bash
ln -s ../../../../config/opcua-node-list.yaml \
  deploy/k3s/base/opcua-data-subscriber/opcua-node-list.yaml
```

- [ ] **Step 3: Create Kustomize base for `data-converter`**

`deploy/k3s/base/data-converter/kustomization.yaml`:
```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - deployment.yaml
  - service.yaml
configMapGenerator:
  - name: data-converter-config
    files:
      - conversion-rules.yaml
```

`deploy/k3s/base/data-converter/deployment.yaml`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: data-converter
  labels: { app.kubernetes.io/name: data-converter }
spec:
  replicas: 1
  selector:
    matchLabels: { app.kubernetes.io/name: data-converter }
  template:
    metadata:
      labels: { app.kubernetes.io/name: data-converter }
      annotations:
        eirvah.uniza/scrape: "true"
        eirvah.uniza/scrape-port: "8080"
    spec:
      containers:
        - name: data-converter
          image: data-converter:local
          imagePullPolicy: IfNotPresent
          env:
            - name: DATA_CONVERTER_NATS_SERVERS
              value: '["nats://nats:4222"]'
          ports:
            - { name: http, containerPort: 8080 }
          volumeMounts:
            - name: config
              mountPath: /etc/data-converter
              readOnly: true
          readinessProbe:
            httpGet: { path: /readyz, port: 8080 }
            initialDelaySeconds: 3
            periodSeconds: 5
          livenessProbe:
            httpGet: { path: /healthz, port: 8080 }
            initialDelaySeconds: 10
            periodSeconds: 10
          resources:
            requests: { cpu: "10m", memory: "64Mi" }
            limits:   { cpu: "100m", memory: "128Mi" }
      volumes:
        - name: config
          configMap:
            name: data-converter-config
```

`deploy/k3s/base/data-converter/service.yaml`:
```yaml
apiVersion: v1
kind: Service
metadata:
  name: data-converter
  labels: { app.kubernetes.io/name: data-converter }
spec:
  selector: { app.kubernetes.io/name: data-converter }
  ports:
    - { name: http, port: 8080, targetPort: 8080 }
```

```bash
ln -s ../../../../config/conversion-rules.yaml \
  deploy/k3s/base/data-converter/conversion-rules.yaml
```

- [ ] **Step 4: Create Kustomize base for `uns-auto-contextualizer`**

`deploy/k3s/base/uns-auto-contextualizer/kustomization.yaml`:
```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - deployment.yaml
  - service.yaml
configMapGenerator:
  - name: uns-auto-contextualizer-config
    files:
      - opcua-node-to-uns-mapping.yaml
```

`deploy/k3s/base/uns-auto-contextualizer/deployment.yaml`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: uns-auto-contextualizer
  labels: { app.kubernetes.io/name: uns-auto-contextualizer }
spec:
  replicas: 1
  selector:
    matchLabels: { app.kubernetes.io/name: uns-auto-contextualizer }
  template:
    metadata:
      labels: { app.kubernetes.io/name: uns-auto-contextualizer }
      annotations:
        eirvah.uniza/scrape: "true"
        eirvah.uniza/scrape-port: "8080"
    spec:
      containers:
        - name: uns-auto-contextualizer
          image: uns-auto-contextualizer:local
          imagePullPolicy: IfNotPresent
          env:
            - name: UNS_AUTO_CONTEXTUALIZER_NATS_SERVERS
              value: '["nats://nats:4222"]'
            - name: UNS_AUTO_CONTEXTUALIZER_ENTERPRISE
              value: uniza
            - name: UNS_AUTO_CONTEXTUALIZER_SITE
              value: zilina
          ports:
            - { name: http, containerPort: 8080 }
          volumeMounts:
            - name: config
              mountPath: /etc/uns-auto-contextualizer
              readOnly: true
          readinessProbe:
            httpGet: { path: /readyz, port: 8080 }
            initialDelaySeconds: 3
            periodSeconds: 5
          livenessProbe:
            httpGet: { path: /healthz, port: 8080 }
            initialDelaySeconds: 10
            periodSeconds: 10
          resources:
            requests: { cpu: "10m", memory: "64Mi" }
            limits:   { cpu: "100m", memory: "128Mi" }
      volumes:
        - name: config
          configMap:
            name: uns-auto-contextualizer-config
```

`deploy/k3s/base/uns-auto-contextualizer/service.yaml`:
```yaml
apiVersion: v1
kind: Service
metadata:
  name: uns-auto-contextualizer
  labels: { app.kubernetes.io/name: uns-auto-contextualizer }
spec:
  selector: { app.kubernetes.io/name: uns-auto-contextualizer }
  ports:
    - { name: http, port: 8080, targetPort: 8080 }
```

```bash
ln -s ../../../../config/opcua-node-to-uns-mapping.yaml \
  deploy/k3s/base/uns-auto-contextualizer/opcua-node-to-uns-mapping.yaml
```

- [ ] **Step 5: Create Kustomize base for `mqtt-uns-publisher`**

`deploy/k3s/base/mqtt-uns-publisher/kustomization.yaml`:
```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - deployment.yaml
  - service.yaml
```

`deploy/k3s/base/mqtt-uns-publisher/deployment.yaml`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mqtt-uns-publisher
  labels: { app.kubernetes.io/name: mqtt-uns-publisher }
spec:
  replicas: 1
  selector:
    matchLabels: { app.kubernetes.io/name: mqtt-uns-publisher }
  template:
    metadata:
      labels: { app.kubernetes.io/name: mqtt-uns-publisher }
      annotations:
        eirvah.uniza/scrape: "true"
        eirvah.uniza/scrape-port: "8080"
    spec:
      containers:
        - name: mqtt-uns-publisher
          image: mqtt-uns-publisher:local
          imagePullPolicy: IfNotPresent
          env:
            - name: MQTT_UNS_PUBLISHER_NATS_SERVERS
              value: '["nats://nats:4222"]'
            - name: MQTT_UNS_PUBLISHER_MQTT_HOST
              value: mosquitto
            - name: MQTT_UNS_PUBLISHER_MQTT_USERNAME
              value: eirvah
            - name: MQTT_UNS_PUBLISHER_MQTT_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: mosquitto-credentials
                  key: password
          ports:
            - { name: http, containerPort: 8080 }
          readinessProbe:
            httpGet: { path: /readyz, port: 8080 }
            initialDelaySeconds: 5
            periodSeconds: 5
          livenessProbe:
            httpGet: { path: /healthz, port: 8080 }
            initialDelaySeconds: 15
            periodSeconds: 10
          resources:
            requests: { cpu: "10m", memory: "64Mi" }
            limits:   { cpu: "100m", memory: "128Mi" }
```

`deploy/k3s/base/mqtt-uns-publisher/service.yaml`:
```yaml
apiVersion: v1
kind: Service
metadata:
  name: mqtt-uns-publisher
  labels: { app.kubernetes.io/name: mqtt-uns-publisher }
spec:
  selector: { app.kubernetes.io/name: mqtt-uns-publisher }
  ports:
    - { name: http, port: 8080, targetPort: 8080 }
```

- [ ] **Step 6: Create Kustomize base for `uns-contextualizer-orchestrator`**

`deploy/k3s/base/uns-contextualizer-orchestrator/kustomization.yaml`:
```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - deployment.yaml
  - service.yaml
configMapGenerator:
  - name: uns-contextualizer-orchestrator-config
    files:
      - uns-contextualizer.yaml
```

`deploy/k3s/base/uns-contextualizer-orchestrator/deployment.yaml`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: uns-contextualizer-orchestrator
  labels: { app.kubernetes.io/name: uns-contextualizer-orchestrator }
spec:
  replicas: 1
  selector:
    matchLabels: { app.kubernetes.io/name: uns-contextualizer-orchestrator }
  template:
    metadata:
      labels: { app.kubernetes.io/name: uns-contextualizer-orchestrator }
      annotations:
        eirvah.uniza/scrape: "true"
        eirvah.uniza/scrape-port: "8080"
    spec:
      containers:
        - name: uns-contextualizer-orchestrator
          image: uns-contextualizer-orchestrator:local
          imagePullPolicy: IfNotPresent
          env:
            - name: UNS_CONTEXTUALIZER_ORCHESTRATOR_NATS_SERVERS
              value: '["nats://nats:4222"]'
          ports:
            - { name: http, containerPort: 8080 }
          volumeMounts:
            - name: config
              mountPath: /etc/uns-contextualizer-orchestrator
              readOnly: true
          readinessProbe:
            httpGet: { path: /readyz, port: 8080 }
            initialDelaySeconds: 3
            periodSeconds: 5
          livenessProbe:
            httpGet: { path: /healthz, port: 8080 }
            initialDelaySeconds: 10
            periodSeconds: 10
          resources:
            requests: { cpu: "25m", memory: "128Mi" }
            limits:   { cpu: "200m", memory: "256Mi" }
      volumes:
        - name: config
          configMap:
            name: uns-contextualizer-orchestrator-config
```

`deploy/k3s/base/uns-contextualizer-orchestrator/service.yaml`:
```yaml
apiVersion: v1
kind: Service
metadata:
  name: uns-contextualizer-orchestrator
  labels: { app.kubernetes.io/name: uns-contextualizer-orchestrator }
spec:
  selector: { app.kubernetes.io/name: uns-contextualizer-orchestrator }
  ports:
    - { name: http, port: 8080, targetPort: 8080 }
```

```bash
ln -s ../../../../config/pipelines/uns-contextualizer.yaml \
  deploy/k3s/base/uns-contextualizer-orchestrator/uns-contextualizer.yaml
```

- [ ] **Step 7: Update `deploy/k3s/base/kustomization.yaml`** to include all 5 new services and the pipeline dashboard:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - namespace.yaml
  - nats
  - mosquitto
  - rabbitmq
  - prometheus
  - opcua-simulator
  - opcua-data-subscriber
  - data-converter
  - uns-auto-contextualizer
  - mqtt-uns-publisher
  - uns-contextualizer-orchestrator
  - grafana
```

Also update `deploy/k3s/base/grafana/kustomization.yaml` to add the pipeline dashboard to the configMapGenerator:

```yaml
configMapGenerator:
  - name: grafana-dashboards
    files:
      - bottling-line-state.json
      - eirvah-edge-pipeline.json
```

(The `eirvah-edge-pipeline.json` file is created in Task 11; symlink it in when that task is done.)

- [ ] **Step 8: Update `scripts/build_all.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
TAG="${1:-local}"
SERVICES=(
  opcua-simulator
  opcua-data-subscriber
  data-converter
  uns-auto-contextualizer
  mqtt-uns-publisher
  uns-contextualizer-orchestrator
)
for svc in "${SERVICES[@]}"; do
  echo "==> building ${svc}:${TAG}"
  docker build --file "services/${svc}/Dockerfile" --tag "${svc}:${TAG}" .
done
```

- [ ] **Step 9: Update `scripts/dev_up.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER="eirvah-edge"
NAMESPACE="eirvah-edge"
SERVICES=(
  opcua-simulator
  opcua-data-subscriber
  data-converter
  uns-auto-contextualizer
  mqtt-uns-publisher
  uns-contextualizer-orchestrator
)

# 1. Cluster
if ! kind get clusters 2>/dev/null | grep -qx "${CLUSTER}"; then
  echo "==> creating kind cluster '${CLUSTER}'"
  kind create cluster --name "${CLUSTER}" --wait 60s
else
  echo "==> kind cluster '${CLUSTER}' already exists"
fi

# 2. Build + import images
./scripts/build_all.sh local

for svc in "${SERVICES[@]}"; do
  echo "==> loading ${svc}:local into kind cluster"
  kind load docker-image "${svc}:local" --name "${CLUSTER}"
done

# 3. Apply manifests
echo "==> applying deploy/k3s/overlays/local"
kubectl apply -k deploy/k3s/overlays/local

# 4. Wait for readiness
echo "==> waiting for all deployments to become Available (up to 5 min)"
kubectl -n "${NAMESPACE}" wait \
  --for=condition=Available \
  --timeout=300s \
  deployment --all

# 5. Hints
echo ""
echo "==> stack is up."
echo "    Grafana:       kubectl -n ${NAMESPACE} port-forward svc/grafana 3000:3000"
echo "    Prometheus:    kubectl -n ${NAMESPACE} port-forward svc/prometheus 9090:9090"
echo "    OPC UA:        kubectl -n ${NAMESPACE} port-forward svc/opcua-simulator 4840:4840"
echo "    Mosquitto:     kubectl -n ${NAMESPACE} port-forward svc/mosquitto 1883:1883"
echo "    NATS:          kubectl -n ${NAMESPACE} port-forward svc/nats 4222:4222"
echo "    RabbitMQ:      kubectl -n ${NAMESPACE} port-forward svc/rabbitmq 15672:15672"
echo "    Credentials:   admin / eirvah-dev-grafana (Grafana)"
```

- [ ] **Step 10: Commit**

```bash
git add deploy/ scripts/ pyproject.toml uv.lock
git commit -m "feat(deploy): Kustomize manifests for 5 telemetry services + update dev scripts"
```

---

### Task 11: Grafana EirVah Edge Pipeline dashboard

**Why:** Spec §6.7 requires a pre-provisioned "EirVah Edge Pipeline" dashboard. Plan 2 adds the telemetry panels; Plan 3 will add actuation panels to the same file.

**Files:**
- Create: `deploy/grafana/dashboards/eirvah-edge-pipeline.json`
- Run: `ln -s` from `deploy/k3s/base/grafana/` (done in step)

- [ ] **Step 1: Create `deploy/grafana/dashboards/eirvah-edge-pipeline.json`**

```json
{
  "__inputs": [],
  "__requires": [],
  "annotations": { "list": [] },
  "description": "EirVah Edge telemetry and actuation pipeline health (spec §6.7)",
  "editable": true,
  "fiscalYearStartMonth": 0,
  "graphTooltip": 1,
  "id": null,
  "links": [],
  "panels": [
    {
      "datasource": { "type": "prometheus", "uid": "eirvah-prometheus" },
      "fieldConfig": {
        "defaults": { "color": { "mode": "thresholds" }, "thresholds": { "mode": "absolute", "steps": [{ "color": "green", "value": null }] }, "unit": "percentunit" },
        "overrides": []
      },
      "gridPos": { "h": 4, "w": 6, "x": 0, "y": 0 },
      "id": 1,
      "options": { "colorMode": "background", "graphMode": "area", "justifyMode": "auto", "orientation": "auto", "reduceOptions": { "calcs": ["lastNotNull"] }, "textMode": "auto" },
      "title": "Telemetry Success Rate (1 m)",
      "type": "stat",
      "targets": [
        {
          "datasource": { "type": "prometheus", "uid": "eirvah-prometheus" },
          "expr": "rate(eirvah_pipeline_success_total{path=\"telemetry\"}[1m]) / (rate(eirvah_pipeline_success_total{path=\"telemetry\"}[1m]) + rate(eirvah_pipeline_stage_timeout_total{path=\"telemetry\"}[1m]) + rate(eirvah_pipeline_stage_error_total{path=\"telemetry\"}[1m]) + 1e-9)",
          "legendFormat": "success rate",
          "refId": "A"
        }
      ]
    },
    {
      "datasource": { "type": "prometheus", "uid": "eirvah-prometheus" },
      "fieldConfig": {
        "defaults": { "unit": "s", "color": { "mode": "palette-classic" } },
        "overrides": []
      },
      "gridPos": { "h": 8, "w": 12, "x": 6, "y": 0 },
      "id": 2,
      "options": { "legend": { "displayMode": "list", "placement": "bottom" }, "tooltip": { "mode": "single" } },
      "title": "Telemetry E2E Latency (p50 / p95 / p99)",
      "type": "timeseries",
      "targets": [
        {
          "datasource": { "type": "prometheus", "uid": "eirvah-prometheus" },
          "expr": "histogram_quantile(0.50, rate(eirvah_pipeline_e2e_latency_seconds_bucket{path=\"telemetry\"}[1m]))",
          "legendFormat": "p50",
          "refId": "A"
        },
        {
          "datasource": { "type": "prometheus", "uid": "eirvah-prometheus" },
          "expr": "histogram_quantile(0.95, rate(eirvah_pipeline_e2e_latency_seconds_bucket{path=\"telemetry\"}[1m]))",
          "legendFormat": "p95",
          "refId": "B"
        },
        {
          "datasource": { "type": "prometheus", "uid": "eirvah-prometheus" },
          "expr": "histogram_quantile(0.99, rate(eirvah_pipeline_e2e_latency_seconds_bucket{path=\"telemetry\"}[1m]))",
          "legendFormat": "p99",
          "refId": "C"
        }
      ]
    },
    {
      "datasource": { "type": "prometheus", "uid": "eirvah-prometheus" },
      "fieldConfig": {
        "defaults": { "unit": "short", "color": { "mode": "palette-classic" } },
        "overrides": []
      },
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 8 },
      "id": 3,
      "options": { "legend": { "displayMode": "list", "placement": "bottom" }, "tooltip": { "mode": "single" } },
      "title": "Stage Error + Timeout Rate",
      "type": "timeseries",
      "targets": [
        {
          "datasource": { "type": "prometheus", "uid": "eirvah-prometheus" },
          "expr": "sum by (stage) (rate(eirvah_pipeline_stage_error_total{path=\"telemetry\"}[1m]))",
          "legendFormat": "error {{stage}}",
          "refId": "A"
        },
        {
          "datasource": { "type": "prometheus", "uid": "eirvah-prometheus" },
          "expr": "sum by (stage) (rate(eirvah_pipeline_stage_timeout_total{path=\"telemetry\"}[1m]))",
          "legendFormat": "timeout {{stage}}",
          "refId": "B"
        }
      ]
    },
    {
      "datasource": { "type": "prometheus", "uid": "eirvah-prometheus" },
      "fieldConfig": {
        "defaults": {
          "color": { "mode": "thresholds" },
          "mappings": [{ "options": { "0": { "color": "red", "text": "DOWN" }, "1": { "color": "green", "text": "UP" } }, "type": "value" }],
          "thresholds": { "mode": "absolute", "steps": [{ "color": "red", "value": null }, { "color": "green", "value": 1 }] }
        },
        "overrides": []
      },
      "gridPos": { "h": 4, "w": 6, "x": 12, "y": 8 },
      "id": 4,
      "options": { "colorMode": "background", "graphMode": "none", "justifyMode": "auto", "orientation": "horizontal", "reduceOptions": { "calcs": ["lastNotNull"] }, "textMode": "auto" },
      "title": "Connection State",
      "type": "stat",
      "targets": [
        {
          "datasource": { "type": "prometheus", "uid": "eirvah-prometheus" },
          "expr": "eirvah_ingress_connection_state{state=\"connected\"}",
          "legendFormat": "{{ingress}}",
          "refId": "A"
        }
      ]
    }
  ],
  "refresh": "10s",
  "schemaVersion": 38,
  "tags": ["eirvah", "pipeline"],
  "templating": { "list": [] },
  "time": { "from": "now-15m", "to": "now" },
  "timepicker": {},
  "timezone": "browser",
  "title": "EirVah Edge Pipeline",
  "uid": "eirvah-pipeline-v1",
  "version": 1
}
```

- [ ] **Step 2: Symlink into Grafana base**

```bash
ln -s ../../../../deploy/grafana/dashboards/eirvah-edge-pipeline.json \
  deploy/k3s/base/grafana/eirvah-edge-pipeline.json
```

- [ ] **Step 3: Commit**

```bash
git add deploy/grafana/dashboards/eirvah-edge-pipeline.json \
        deploy/k3s/base/grafana/
git commit -m "feat(grafana): add EirVah Edge Pipeline dashboard (telemetry panels)"
```

---

### Task 12: scripts/trace.sh

**Why:** Spec §8.3 test 8 and general observability — grep all pod logs for a correlation ID and print lines in timestamp order.

**Files:**
- Create: `scripts/trace.sh`

- [ ] **Step 1: Create `scripts/trace.sh`**

```bash
#!/usr/bin/env bash
# Search all pod logs for a correlation_id and print matching lines sorted by timestamp.
# Usage: scripts/trace.sh <correlation_id> [namespace]
set -euo pipefail

CORRELATION_ID="${1:?Usage: trace.sh <correlation_id> [namespace]}"
NAMESPACE="${2:-eirvah-edge}"

kubectl -n "${NAMESPACE}" get pods \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | \
while IFS= read -r pod; do
  kubectl -n "${NAMESPACE}" logs "${pod}" \
    --since=1h \
    --all-containers=true \
    --prefix=true \
    2>/dev/null | grep "${CORRELATION_ID}" || true
done | sort
```

- [ ] **Step 2: Make executable and test syntax**

```bash
chmod +x scripts/trace.sh
bash -n scripts/trace.sh
```
Expected: no syntax errors.

- [ ] **Step 3: Commit**

```bash
git add scripts/trace.sh
git commit -m "feat(scripts): add trace.sh — search all pod logs by correlation_id"
```

---

### Task 13: E2E conftest.py — EirVahCluster fixture

**Why:** All e2e tests need a running k3d cluster with port-forwarded services. The `EirVahCluster` fixture manages port-forwarding and exposes typed clients.

**New test deps:** `aiomqtt>=2.0`, `nats-py>=2.7` (already in workspace).

**Files:**
- Create: `tests/e2e/__init__.py`
- Create: `tests/e2e/conftest.py`

- [ ] **Step 1: Create `tests/e2e/__init__.py`** — empty.

- [ ] **Step 2: Create `tests/e2e/conftest.py`**

```python
"""E2E test fixtures — requires a running k3d cluster (skip if absent)."""

from __future__ import annotations

import asyncio
import subprocess
import time
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field

import aiomqtt
import pytest
import pytest_asyncio

NAMESPACE = "eirvah-edge"
MQTT_LOCAL_PORT = 11883
NATS_LOCAL_PORT = 14222
OPCUA_LOCAL_PORT = 14840
PROM_LOCAL_PORT = 19090


def _cluster_is_up() -> bool:
    try:
        result = subprocess.run(
            ["kubectl", "-n", NAMESPACE, "get", "pods", "--no-headers"],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0 and b"Running" in result.stdout
    except Exception:
        return False


def _port_forward(service: str, local_port: int, remote_port: int) -> subprocess.Popen:  # type: ignore[type-arg]
    return subprocess.Popen(
        [
            "kubectl", "-n", NAMESPACE,
            "port-forward", f"svc/{service}",
            f"{local_port}:{remote_port}",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


@dataclass
class EirVahCluster:
    mqtt_host: str = "localhost"
    mqtt_port: int = MQTT_LOCAL_PORT
    nats_servers: list[str] = field(default_factory=lambda: [f"nats://localhost:{NATS_LOCAL_PORT}"])
    opcua_endpoint: str = f"opc.tcp://localhost:{OPCUA_LOCAL_PORT}/eirvah/simulator"
    prometheus_url: str = f"http://localhost:{PROM_LOCAL_PORT}"

    @asynccontextmanager
    async def mqtt_client(
        self, *, username: str = "eirvah", password: str = "eirvah-dev-password"
    ) -> AsyncGenerator[aiomqtt.Client, None]:
        async with aiomqtt.Client(
            hostname=self.mqtt_host,
            port=self.mqtt_port,
            username=username,
            password=password,
        ) as client:
            yield client


@pytest.fixture(scope="session")
def eirvah_cluster() -> Generator[EirVahCluster, None, None]:
    if not _cluster_is_up():
        pytest.skip("k3d cluster not running — start with ./scripts/dev_up.sh")

    procs = [
        _port_forward("mosquitto", MQTT_LOCAL_PORT, 1883),
        _port_forward("nats", NATS_LOCAL_PORT, 4222),
        _port_forward("opcua-simulator", OPCUA_LOCAL_PORT, 4840),
        _port_forward("prometheus", PROM_LOCAL_PORT, 9090),
    ]
    time.sleep(2)  # give port-forwards a moment to bind

    try:
        yield EirVahCluster()
    finally:
        for p in procs:
            p.terminate()
```

- [ ] **Step 3: Verify fixture imports cleanly**

```bash
uv run python -c "from tests.e2e.conftest import EirVahCluster; print('ok')"
```
Expected: `ok`

- [ ] **Step 4: Commit**

```bash
git add tests/e2e/
git commit -m "test(e2e): EirVahCluster session fixture with port-forwarding"
```

---

### Task 14: E2E test_telemetry.py

**Why:** Spec §8.3 tests 1 and 2 — assert that telemetry messages flow end-to-end and that bad quality is propagated correctly.

**Files:**
- Create: `tests/e2e/test_telemetry.py`

- [ ] **Step 1: Create `tests/e2e/test_telemetry.py`**

```python
"""E2E tests for the telemetry path (spec §8.3 tests 1–2)."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
import pytest_asyncio
from eirvah_contracts.telemetry import TelemetryPayload
from eirvah_contracts.ulid import is_valid_correlation_id

from tests.e2e.conftest import EirVahCluster

pytestmark = pytest.mark.asyncio

SUBSCRIBE_TOPIC = "uniza/zilina/factory1/line_a/bottler/#"
EXPECTED_NODES = {
    "uniza/zilina/factory1/line_a/bottler/temperature_sensor_01/temperature",
    "uniza/zilina/factory1/line_a/bottler/throughput_meter_01/throughput",
    "uniza/zilina/factory1/line_a/bottler/motor_01/state",
    "uniza/zilina/factory1/line_a/bottler/motor_01/rpm",
    "uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature",
}


async def _collect_messages(
    cluster: EirVahCluster,
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


async def test_telemetry_happy_path(eirvah_cluster: EirVahCluster) -> None:
    """spec §8.3 test 1: within 15 s at least one v1.0 message per monitored node."""
    messages = await _collect_messages(eirvah_cluster, timeout_s=15.0, max_messages=30)

    assert messages, "No MQTT messages received within 15 s — pipeline may not be running"

    topics_seen = {m["_topic"] for m in messages}
    missing = EXPECTED_NODES - topics_seen
    assert not missing, f"Missing messages for nodes: {missing}"

    for msg in messages:
        assert msg.get("schema_version") == "1.0", f"Bad schema_version in {msg}"
        assert is_valid_correlation_id(msg.get("correlation_id", "")), f"Invalid correlation_id in {msg}"
        assert msg.get("quality") in {"good", "uncertain", "bad"}, f"Invalid quality in {msg}"
        # Validate full payload model
        TelemetryPayload.model_validate(msg)


async def test_quality_propagation(eirvah_cluster: EirVahCluster) -> None:
    """spec §8.3 test 2: ~10% of temperature messages carry quality='bad'."""
    messages = await _collect_messages(eirvah_cluster, timeout_s=20.0, max_messages=100)

    temp_msgs = [
        m for m in messages
        if m.get("_topic") == "uniza/zilina/factory1/line_a/bottler/temperature_sensor_01/temperature"
    ]
    assert len(temp_msgs) >= 5, f"Need at least 5 temperature messages to assess quality, got {len(temp_msgs)}"

    bad_count = sum(1 for m in temp_msgs if m.get("quality") == "bad")
    bad_pct = bad_count / len(temp_msgs)

    # bad_quality_pct is 0.1 in address-space config; allow wide tolerance in e2e
    assert bad_pct > 0.02, (
        f"Expected some bad-quality messages for temperature node, got {bad_count}/{len(temp_msgs)}"
    )
```

- [ ] **Step 2: Run e2e tests (requires running cluster)**

```bash
uv run pytest tests/e2e/test_telemetry.py -v -s
```

If cluster is not running, tests are skipped (that's correct). To run for real:
```bash
./scripts/dev_up.sh
uv run pytest tests/e2e/test_telemetry.py -v -s
./scripts/dev_down.sh
```

Expected (with cluster): 2 PASSED.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/test_telemetry.py
git commit -m "test(e2e): telemetry happy path and quality propagation tests (spec §8.3 tests 1–2)"
```

---

### Task 15: Smoke test + README update

**Why:** Confirm all unit tests pass, the plan 2 acceptance criteria are met, and update README status.

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Run full unit + integration test suite**

```bash
uv run pytest services/ libs/ -v --tb=short
```
Expected: all PASSED (no failures or errors).

- [ ] **Step 2: Run ruff and mypy**

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy services/opcua-data-subscriber/src \
             services/data-converter/src \
             services/uns-auto-contextualizer/src \
             services/mqtt-uns-publisher/src \
             services/uns-contextualizer-orchestrator/src \
             libs/
```

Fix any issues found before proceeding.

- [ ] **Step 3: Check all new dependencies are OSI-approved**

Verify each new package added:
- `aiomqtt` — ISC licence ✓ (wraps paho-mqtt, EPL-2.0/EDL-1.0, both OSI-approved ✓)

No SSPL, BSL, Elastic License, or RSALv2 dependencies.

- [ ] **Step 4: Update `README.md` Plan 2 status**

Change:
```markdown
- **Plan 2 — Telemetry path:** queued.
```
To:
```markdown
- **Plan 2 — Telemetry path:** complete — telemetry path live, EirVah Edge Pipeline dashboard added.
```

- [ ] **Step 5: Final commit**

```bash
git add README.md
git commit -m "chore(plan-2): mark telemetry path complete"
```

- [ ] **Step 6: Tag**

```bash
git tag plan-2-complete
```

---

## Self-review

### 1. Spec coverage

| Spec requirement | Task |
|---|---|
| `opcua-data-subscriber` — OPC UA subscription → NATS `uns.ingress.raw` (§3.1) | Task 4 |
| `data-converter` — normalize signals, unit conversion (§3.1) | Task 5 |
| `uns-auto-contextualizer` — ISA-95 mapping, UNS topic (§3.1) | Task 6 |
| `mqtt-uns-publisher` — TelemetryPayload v1.0 to Mosquitto (§3.1, §4.2) | Task 7 |
| `uns-contextualizer-orchestrator` — pipeline owner, DLQ, metrics (§3.1, §6.1) | Tasks 8–9 |
| Pipeline config YAML (§7.1 knobs) | Task 2 |
| Per-node bad_quality_pct (§9.3) | Task 3 |
| `eirvah_pipeline_success_total`, `stage_error_total`, `stage_timeout_total`, `e2e_latency_seconds` (§7.2) | Task 8 |
| `eirvah_ingress_connection_state` (§6.6) | Task 4 |
| Grafana "EirVah Edge Pipeline" dashboard — telemetry panels (§6.7) | Task 11 |
| `scripts/trace.sh` (§5.6) | Task 12 |
| `test_telemetry_happy_path` (§8.3 test 1) | Task 14 |
| `test_quality_propagation` (§8.3 test 2) | Task 14 |
| OSI-approved dependencies only | Task 15 |
| `ContextualizeResult`, `PublishRequest` wire contracts | Task 1 |
| Kustomize deploy for 5 new services (§5.2) | Task 10 |

**Not in Plan 2 (deferred):**
- `tests/e2e/test_actuation.py` — Plan 3
- `deploy/k3s/overlays/lab/` — Plan 4
- Actuation panels in pipeline dashboard — Plan 3

### 2. Placeholder scan

No TBD, TODO, or "similar to" references found.

### 3. Type consistency

- `RawSignalEnvelope` — defined in `eirvah_contracts/signals.py`, used in Tasks 4, 5, 8, 9.
- `NormalizedSignalEnvelope` — defined in `eirvah_contracts/signals.py`, used in Tasks 5, 6, 8, 9.
- `ContextualizeResult` — defined in Task 1 (`pipeline.py`), used in Tasks 6, 8, 9.
- `PublishRequest` — defined in Task 1, used in Tasks 7, 8, 9.
- `TelemetryPayload`, `TelemetrySource`, `TelemetryTimestamps` — defined in `eirvah_contracts/telemetry.py`, used in Task 7.
- `PipelineContext.build_publish_request()` — defined in Task 8, called in Task 9 `pipeline.py`.
- `PipelineMetrics` — defined in Task 8, used in Task 9.
- `load_pipeline_config` — defined in Task 8, used in Task 9.

All names consistent across tasks.
