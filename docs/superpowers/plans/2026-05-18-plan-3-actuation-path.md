# Plan 3 — Actuation Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the actuation path (six new pods + config + e2e tests + two ADRs) to close the CPS feedback loop: temperature spike → decision-agent-stub → AMQP → validator → OPC UA write → setpoint changes → telemetry reflects new value.

**Architecture:** Loop-closure-first — e2e test skeletons are written first (red), then each service is implemented until all four actuation e2e tests are green. Orchestrator owns pipeline state; `actuation-event-validator` and `actuation-signal-publisher` are stateless NATS req/rep workers. `amqp-actuation-event-subscriber` bridges RabbitMQ → NATS. `decision-agent-stub` bridges MQTT → RabbitMQ. All services follow existing patterns in the repo.

**Tech Stack:** Python 3.12, aio-pika 9.x (MIT, AMQP client), asyncua 1.x (OPC UA write), aiomqtt 2.x (MQTT subscribe in stub), nats-py, pydantic v2, pydantic-settings, structlog, prometheus-client, starlette, uvicorn, pyyaml, python-ulid, k3d, Kustomize. All OSI-approved.

**Spec reference:** `docs/superpowers/specs/2026-05-18-plan-3-actuation-path-design.md`

---

## File structure produced by this plan

```
libs/eirvah-contracts/src/eirvah_contracts/actuation.py  MODIFY  add ValidationResult

config/
├── actuation-policy.yaml                                 NEW
└── pipelines/
    └── actuation-control.yaml                            NEW

services/
├── amqp-actuation-event-subscriber/
│   ├── pyproject.toml
│   ├── Dockerfile
│   ├── src/amqp_actuation_event_subscriber/__init__.py
│   ├── src/amqp_actuation_event_subscriber/__main__.py
│   ├── src/amqp_actuation_event_subscriber/config.py
│   ├── src/amqp_actuation_event_subscriber/service.py
│   └── tests/test_amqp_actuation_event_subscriber.py
├── actuation-control-orchestrator/
│   ├── pyproject.toml
│   ├── Dockerfile
│   ├── src/actuation_control_orchestrator/__init__.py
│   ├── src/actuation_control_orchestrator/__main__.py
│   ├── src/actuation_control_orchestrator/config.py
│   ├── src/actuation_control_orchestrator/models.py
│   ├── src/actuation_control_orchestrator/metrics.py
│   ├── src/actuation_control_orchestrator/pipeline.py
│   └── src/actuation_control_orchestrator/service.py
│   └── tests/test_actuation_control_orchestrator.py
├── actuation-event-validator/
│   ├── pyproject.toml
│   ├── Dockerfile
│   ├── src/actuation_event_validator/__init__.py
│   ├── src/actuation_event_validator/__main__.py
│   ├── src/actuation_event_validator/config.py
│   ├── src/actuation_event_validator/service.py
│   └── tests/test_actuation_event_validator.py
├── actuation-signal-publisher/
│   ├── pyproject.toml
│   ├── Dockerfile
│   ├── src/actuation_signal_publisher/__init__.py
│   ├── src/actuation_signal_publisher/__main__.py
│   ├── src/actuation_signal_publisher/config.py
│   ├── src/actuation_signal_publisher/service.py
│   └── tests/test_actuation_signal_publisher.py
└── decision-agent-stub/
    ├── pyproject.toml
    ├── Dockerfile
    ├── src/decision_agent_stub/__init__.py
    ├── src/decision_agent_stub/__main__.py
    ├── src/decision_agent_stub/config.py
    ├── src/decision_agent_stub/service.py
    └── tests/test_decision_agent_stub.py

deploy/k3s/base/
├── amqp-actuation-event-subscriber/  deployment.yaml + service.yaml + kustomization.yaml
├── actuation-control-orchestrator/   deployment.yaml + service.yaml + kustomization.yaml
├── actuation-event-validator/        deployment.yaml + service.yaml + kustomization.yaml
├── actuation-signal-publisher/       deployment.yaml + service.yaml + kustomization.yaml
├── decision-agent-stub/              deployment.yaml + service.yaml + kustomization.yaml
└── kustomization.yaml                MODIFY  add 5 new dirs

deploy/grafana/dashboards/
└── eirvah-edge-pipeline.json         MODIFY  add actuation panel row

docs/adr/
├── 0001-actuation-safety-gate.md     NEW
└── 0002-reverse-mapping-shared-configmap.md  NEW

tests/e2e/
├── conftest.py                       MODIFY  add AMQP port-forward + amqp_url
└── test_actuation.py                 NEW  4 tests

pyproject.toml                        MODIFY  add 5 workspace members
scripts/build_all.sh                  MODIFY  add 5 services
```

---

## Task 1: Extend eirvah-contracts — ValidationResult

**Files:**
- Modify: `libs/eirvah-contracts/src/eirvah_contracts/actuation.py`
- Modify: `libs/eirvah-contracts/tests/test_actuation.py`

- [ ] **Step 1: Write the failing test**

Add to `libs/eirvah-contracts/tests/test_actuation.py`:

```python
def test_validation_result_approve() -> None:
    from eirvah_contracts.actuation import ValidationResult
    r = ValidationResult(decision="approve")
    assert r.decision == "approve"
    assert r.reason is None


def test_validation_result_reject_with_reason() -> None:
    from eirvah_contracts.actuation import ValidationResult
    r = ValidationResult(decision="reject", reason="value 99.0 outside policy range [20.0, 30.0]")
    assert r.decision == "reject"
    assert "99.0" in (r.reason or "")


def test_validation_result_rejects_unknown_decision() -> None:
    from eirvah_contracts.actuation import ValidationResult
    import pytest
    with pytest.raises(Exception):
        ValidationResult(decision="maybe")
```

- [ ] **Step 2: Run to confirm failure**

```bash
uv run pytest libs/eirvah-contracts/tests/test_actuation.py -k "validation_result" -v
```

Expected: `ImportError` or `AttributeError` — `ValidationResult` not defined yet.

- [ ] **Step 3: Implement**

Add to `libs/eirvah-contracts/src/eirvah_contracts/actuation.py` after the existing imports and before `ActuationRequest`:

```python
class ValidationResult(BaseModel):
    """Returned by actuation-event-validator on act.work.validate (spec §3.2)."""

    decision: Literal["approve", "reject"]
    reason: str | None = None
```

- [ ] **Step 4: Run to confirm pass**

```bash
uv run pytest libs/eirvah-contracts/tests/test_actuation.py -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/eirvah-contracts/src/eirvah_contracts/actuation.py \
        libs/eirvah-contracts/tests/test_actuation.py
git commit -m "feat(contracts): add ValidationResult for actuation validator"
```

---

## Task 2: Config files

**Files:**
- Create: `config/actuation-policy.yaml`
- Create: `config/pipelines/actuation-control.yaml`

- [ ] **Step 1: Create actuation-policy.yaml**

```yaml
# config/actuation-policy.yaml
# Per-node actuation policy for actuation-event-validator (spec §3.2).
policies:
  - uns_topic: "uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature"
    allowed_range: [20.0, 30.0]
    allowlist:
      - decision-agent-stub
```

- [ ] **Step 2: Create actuation-control.yaml**

```yaml
# config/pipelines/actuation-control.yaml
# Pipeline stage definitions for actuation-control-orchestrator (spec §3.2).
stages:
  - name: validate
    subject: act.work.validate
    timeout_s: 2.0
  - name: write_signal
    subject: act.work.write_signal
    timeout_s: 5.0
dlq_subject: act.dlq.rejected
```

- [ ] **Step 3: Commit**

```bash
git add config/actuation-policy.yaml config/pipelines/actuation-control.yaml
git commit -m "feat(config): actuation policy and pipeline stage config"
```

---

## Task 3: Workspace wiring

**Files:**
- Modify: `pyproject.toml`
- Modify: `scripts/build_all.sh`

- [ ] **Step 1: Add workspace members to pyproject.toml**

In `pyproject.toml`, extend `[tool.uv.workspace] members` to:

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
    "services/amqp-actuation-event-subscriber",
    "services/actuation-control-orchestrator",
    "services/actuation-event-validator",
    "services/actuation-signal-publisher",
    "services/decision-agent-stub",
]
```

Extend `[tool.uv.sources]`:

```toml
[tool.uv.sources]
eirvah-contracts                         = { workspace = true }
eirvah-bus                               = { workspace = true }
eirvah-observability                     = { workspace = true }
opcua-simulator                          = { workspace = true }
opcua-data-subscriber                    = { workspace = true }
data-converter                           = { workspace = true }
uns-auto-contextualizer                  = { workspace = true }
mqtt-uns-publisher                       = { workspace = true }
uns-contextualizer-orchestrator          = { workspace = true }
amqp-actuation-event-subscriber          = { workspace = true }
actuation-control-orchestrator           = { workspace = true }
actuation-event-validator                = { workspace = true }
actuation-signal-publisher               = { workspace = true }
decision-agent-stub                      = { workspace = true }
```

Extend `[dependency-groups] dev` to include the 5 new packages and `aio-pika`:

```toml
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
    "amqp-actuation-event-subscriber",
    "actuation-control-orchestrator",
    "actuation-event-validator",
    "actuation-signal-publisher",
    "decision-agent-stub",
    "pytest>=8.3",
    "pytest-asyncio>=0.24",
    "httpx>=0.27",
    "ruff>=0.6",
    "mypy>=1.11",
    "aiomqtt>=2.0",
    "aio-pika>=9.0",
]
```

- [ ] **Step 2: Update build_all.sh**

Replace the `SERVICES=(...)` block in `scripts/build_all.sh`:

```bash
SERVICES=(
  opcua-simulator
  opcua-data-subscriber
  data-converter
  uns-auto-contextualizer
  mqtt-uns-publisher
  uns-contextualizer-orchestrator
  amqp-actuation-event-subscriber
  actuation-control-orchestrator
  actuation-event-validator
  actuation-signal-publisher
  decision-agent-stub
)
```

- [ ] **Step 3: Verify workspace resolves**

The service pyproject.toml files will be created in later tasks. Run after all are created:

```bash
uv sync
```

Expected: resolves without error (placeholder — re-run after Task 10).

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml scripts/build_all.sh
git commit -m "chore: wire 5 new actuation services into workspace and build script"
```

---

## Task 4: E2e test skeletons (red)

**Files:**
- Modify: `tests/e2e/conftest.py`
- Create: `tests/e2e/test_actuation.py`

- [ ] **Step 1: Add AMQP support to conftest.py**

Add to `tests/e2e/conftest.py` after the existing port constants:

```python
AMQP_LOCAL_PORT = 25672
AMQP_URL = f"amqp://eirvah:eirvah-dev-password@localhost:{AMQP_LOCAL_PORT}/"
```

Add `rabbitmq` port-forward to the `procs` list inside the `eirvah_cluster` fixture — replace the procs block:

```python
    procs = [
        _port_forward("mosquitto", MQTT_LOCAL_PORT, 1883),
        _port_forward("nats", NATS_LOCAL_PORT, 4222),
        _port_forward("opcua-simulator", OPCUA_LOCAL_PORT, 4840),
        _port_forward("prometheus", PROM_LOCAL_PORT, 9090),
        _port_forward("rabbitmq", AMQP_LOCAL_PORT, 5672),
    ]
```

Add `amqp_url` field to `EirVahCluster`:

```python
@dataclass
class EirVahCluster:
    mqtt_host: str = "localhost"
    mqtt_port: int = MQTT_LOCAL_PORT
    nats_servers: list[str] = field(
        default_factory=lambda: [f"nats://localhost:{NATS_LOCAL_PORT}"]
    )
    opcua_endpoint: str = (
        f"opc.tcp://localhost:{OPCUA_LOCAL_PORT}/eirvah/simulator"
    )
    prometheus_url: str = f"http://localhost:{PROM_LOCAL_PORT}"
    amqp_url: str = AMQP_URL
```

- [ ] **Step 2: Create test_actuation.py**

```python
"""E2E tests for the actuation path (spec §8.4, Plan 3 design §6)."""

from __future__ import annotations

import asyncio
import json
import subprocess
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import aio_pika
import pytest
from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.ulid import generate_correlation_id

if TYPE_CHECKING:
    from tests.e2e.conftest import EirVahCluster

pytestmark = pytest.mark.asyncio

AMQP_RESULTS_EXCHANGE = "eirvah.actuation.results"
AMQP_REQUESTS_QUEUE = "eirvah.actuation.requests"
SETPOINT_TOPIC = (
    "uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature"
)
_VALID_REQUEST_VALUE = 22.0
_OUT_OF_RANGE_VALUE = 99.0


def _build_request(
    *,
    value: float = _VALID_REQUEST_VALUE,
    requester: str = "decision-agent-stub",
    deadline_offset_s: float = 10.0,
) -> ActuationRequest:
    now = datetime.now(UTC)
    return ActuationRequest(
        correlation_id=generate_correlation_id(),
        requester=requester,
        target_uns_topic=SETPOINT_TOPIC,
        requested_value=value,
        value_type="double",
        reason="e2e test",
        requested_at=now,
        deadline=now + timedelta(seconds=deadline_offset_s),
    )


async def _publish_request(amqp_url: str, req: ActuationRequest) -> None:
    connection = await aio_pika.connect_robust(amqp_url)
    async with connection:
        channel = await connection.channel()
        await channel.default_exchange.publish(
            aio_pika.Message(
                body=req.model_dump_json().encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=AMQP_REQUESTS_QUEUE,
        )


async def _consume_result(
    amqp_url: str,
    *,
    timeout_s: float = 15.0,
) -> dict[str, Any]:
    """Bind a temp queue to the results exchange, return first message."""
    connection = await aio_pika.connect_robust(amqp_url)
    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange(
            AMQP_RESULTS_EXCHANGE,
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )
        result_queue = await channel.declare_queue("", exclusive=True)
        await result_queue.bind(exchange, routing_key="#")
        try:
            async with asyncio.timeout(timeout_s):
                async with result_queue.iterator() as q:
                    async for message in q:
                        async with message.process():
                            return json.loads(message.body)
        except TimeoutError:
            pytest.fail(f"No AMQP result received within {timeout_s}s")
    return {}


async def _read_opcua_setpoint(cluster: "EirVahCluster") -> float:
    """Read current setpoint value from OPC UA simulator."""
    from asyncua import Client

    async with Client(url=cluster.opcua_endpoint) as client:
        ns_idx = await client.get_namespace_index(
            "https://eirvah.uniza/zilina/factory1"
        )
        node = await client.nodes.objects.get_child(
            [f"{ns_idx}:bottler", f"{ns_idx}:SetpointTemperature"]
        )
        return float(await node.read_value())


async def test_actuation_full_loop(eirvah_cluster: "EirVahCluster") -> None:
    """Full CPS loop: request → approve → OPC UA write → setpoint changes."""
    req = _build_request(value=_VALID_REQUEST_VALUE)
    await _publish_request(eirvah_cluster.amqp_url, req)
    result = await _consume_result(eirvah_cluster.amqp_url, timeout_s=15.0)

    assert result.get("decision") == "approve", (
        f"Expected approve, got: {result}"
    )
    assert result.get("correlation_id") == req.correlation_id

    # Give the OPC UA write a moment to propagate
    await asyncio.sleep(1.0)
    setpoint = await _read_opcua_setpoint(eirvah_cluster)
    assert abs(setpoint - _VALID_REQUEST_VALUE) < 0.01, (
        f"Expected setpoint {_VALID_REQUEST_VALUE}, got {setpoint}"
    )


async def test_actuation_rejection_policy(eirvah_cluster: "EirVahCluster") -> None:
    """Value outside allowed_range [20.0, 30.0] → reject with policy reason."""
    req = _build_request(value=_OUT_OF_RANGE_VALUE)
    await _publish_request(eirvah_cluster.amqp_url, req)
    result = await _consume_result(eirvah_cluster.amqp_url, timeout_s=10.0)

    assert result.get("decision") == "reject", (
        f"Expected reject, got: {result}"
    )
    assert "outside policy range" in (result.get("rejection_reason") or ""), (
        f"Expected policy range reason, got: {result}"
    )


async def test_actuation_rejection_writes_disabled(
    eirvah_cluster: "EirVahCluster",
) -> None:
    """With allow_writes=false (default), any valid request → writes_disabled."""
    req = _build_request(value=_VALID_REQUEST_VALUE)
    await _publish_request(eirvah_cluster.amqp_url, req)
    result = await _consume_result(eirvah_cluster.amqp_url, timeout_s=10.0)

    assert result.get("decision") == "reject"
    assert result.get("rejection_reason") == "writes_disabled"


async def test_actuation_deadline_expired(
    eirvah_cluster: "EirVahCluster",
) -> None:
    """Request with past deadline → reject with reason 'expired'."""
    req = _build_request(value=_VALID_REQUEST_VALUE, deadline_offset_s=-5.0)
    await _publish_request(eirvah_cluster.amqp_url, req)
    result = await _consume_result(eirvah_cluster.amqp_url, timeout_s=10.0)

    assert result.get("decision") == "reject"
    assert result.get("rejection_reason") == "expired"
```

- [ ] **Step 3: Verify tests are collected (skip — services not deployed)**

```bash
uv run pytest tests/e2e/test_actuation.py --collect-only
```

Expected: 4 tests collected. They will be skipped when cluster check fails.

- [ ] **Step 4: Commit**

```bash
git add tests/e2e/conftest.py tests/e2e/test_actuation.py
git commit -m "test(e2e): actuation path test skeletons (red — services not yet deployed)"
```

---

## Task 5: amqp-actuation-event-subscriber

**Files:**
- Create: `services/amqp-actuation-event-subscriber/pyproject.toml`
- Create: `services/amqp-actuation-event-subscriber/Dockerfile`
- Create: `services/amqp-actuation-event-subscriber/src/amqp_actuation_event_subscriber/__init__.py`
- Create: `services/amqp-actuation-event-subscriber/src/amqp_actuation_event_subscriber/__main__.py`
- Create: `services/amqp-actuation-event-subscriber/src/amqp_actuation_event_subscriber/config.py`
- Create: `services/amqp-actuation-event-subscriber/src/amqp_actuation_event_subscriber/service.py`
- Create: `services/amqp-actuation-event-subscriber/tests/test_amqp_actuation_event_subscriber.py`

- [ ] **Step 1: Create pyproject.toml**

```toml
[project]
name = "amqp-actuation-event-subscriber"
version = "0.0.0"
description = "Bridges RabbitMQ actuation queue to NATS act.ingress.requested (spec §3.2)."
requires-python = ">=3.12"
license = { text = "Apache-2.0" }
dependencies = [
    "aio-pika>=9.0",
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
packages = ["src/amqp_actuation_event_subscriber"]
```

- [ ] **Step 2: Create config.py**

```python
"""Settings for the AMQP actuation event subscriber."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AmqpSubscriberSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="AMQP_ACTUATION_EVENT_SUBSCRIBER_",
        env_file=None,
        extra="ignore",
    )

    nats_servers: list[str] = ["nats://nats:4222"]
    amqp_url: str = "amqp://eirvah:eirvah-dev-password@rabbitmq:5672/"
    amqp_queue: str = "eirvah.actuation.requests"
    amqp_prefetch: int = 1
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
```

- [ ] **Step 3: Write failing tests**

```python
# services/amqp-actuation-event-subscriber/tests/test_amqp_actuation_event_subscriber.py
"""Unit tests for amqp-actuation-event-subscriber."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_contracts.ulid import generate_correlation_id


def _sample_request() -> ActuationRequest:
    now = datetime.now(UTC)
    return ActuationRequest(
        correlation_id=generate_correlation_id(),
        requester="decision-agent-stub",
        target_uns_topic=(
            "uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature"
        ),
        requested_value=22.0,
        value_type="double",
        reason="test",
        requested_at=now,
    )


def test_build_nats_envelope_from_actuation_request() -> None:
    from amqp_actuation_event_subscriber.service import build_nats_envelope

    req = _sample_request()
    body = req.model_dump_json().encode()
    envelope = build_nats_envelope(body)

    assert envelope.correlation_id == req.correlation_id
    assert envelope.status == "ok"
    parsed_req = ActuationRequest.model_validate(envelope.payload)
    assert parsed_req.target_uns_topic == req.target_uns_topic


def test_build_nats_envelope_invalid_json_raises() -> None:
    from amqp_actuation_event_subscriber.service import build_nats_envelope

    with pytest.raises(Exception):
        build_nats_envelope(b"not-json")


def test_build_nats_envelope_missing_field_raises() -> None:
    from amqp_actuation_event_subscriber.service import build_nats_envelope

    incomplete = json.dumps({"schema_version": "1.0"}).encode()
    with pytest.raises(Exception):
        build_nats_envelope(incomplete)
```

- [ ] **Step 4: Run to confirm failure**

```bash
uv run pytest services/amqp-actuation-event-subscriber/tests/ -v
```

Expected: `ModuleNotFoundError` — service not yet implemented.

- [ ] **Step 5: Create service.py**

```python
"""AMQP actuation event subscriber — bridges RabbitMQ to NATS (spec §3.2)."""

from __future__ import annotations

import asyncio

import aio_pika
import structlog
import uvicorn
from eirvah_bus.client import BusClient
from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import configure_logging
from eirvah_observability.metrics import make_counter

from amqp_actuation_event_subscriber.config import AmqpSubscriberSettings

_log = structlog.get_logger("amqp-actuation-event-subscriber")
NATS_SUBJECT = "act.ingress.requested"


def build_nats_envelope(amqp_body: bytes) -> NATSEnvelope:
    req = ActuationRequest.model_validate_json(amqp_body)
    return NATSEnvelope(
        correlation_id=req.correlation_id,
        payload=req.model_dump(mode="json"),
    )


class AmqpSubscriberRuntime:
    def __init__(self, settings: AmqpSubscriberSettings) -> None:
        self._settings = settings
        self._ready = False
        self._handled = make_counter(
            "amqp_subscriber_messages_total",
            "AMQP messages processed",
            labelnames=["outcome"],
        )

    def is_ready(self) -> bool:
        return self._ready

    async def run(self) -> None:
        bus = BusClient(servers=self._settings.nats_servers, name="amqp-actuation-event-subscriber")
        await bus.connect()
        _log.info("nats_connected")

        connection = await aio_pika.connect_robust(self._settings.amqp_url)
        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=self._settings.amqp_prefetch)
            queue = await channel.declare_queue(self._settings.amqp_queue, durable=True)
            self._ready = True
            _log.info("amqp_subscriber_ready", queue=self._settings.amqp_queue)

            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    try:
                        envelope = build_nats_envelope(message.body)
                        await bus.nc.publish(
                            NATS_SUBJECT,
                            envelope.model_dump_json().encode(),
                        )
                        await message.ack()
                        self._handled.labels(outcome="ok").inc()
                        _log.debug(
                            "forwarded_to_nats",
                            correlation_id=envelope.correlation_id,
                        )
                    except Exception as exc:
                        await message.nack(requeue=True)
                        self._handled.labels(outcome="error").inc()
                        _log.warning("forward_failed", error=str(exc))


async def run(settings: AmqpSubscriberSettings) -> None:
    configure_logging(level=settings.log_level)
    runtime = AmqpSubscriberRuntime(settings)
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

- [ ] **Step 6: Create __init__.py and __main__.py**

`src/amqp_actuation_event_subscriber/__init__.py` — empty file.

```python
# src/amqp_actuation_event_subscriber/__main__.py
"""Entry point for the AMQP actuation event subscriber pod."""

from __future__ import annotations

import asyncio

from amqp_actuation_event_subscriber.config import AmqpSubscriberSettings
from amqp_actuation_event_subscriber.service import run


def main() -> None:
    asyncio.run(run(AmqpSubscriberSettings()))


if __name__ == "__main__":  # pragma: no cover
    main()
```

- [ ] **Step 7: Create Dockerfile**

```dockerfile
# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS builder
WORKDIR /workspace
RUN pip install --no-cache-dir uv==0.4.20
COPY pyproject.toml uv.lock /workspace/
COPY libs /workspace/libs
COPY services/amqp-actuation-event-subscriber /workspace/services/amqp-actuation-event-subscriber
RUN uv sync --frozen --no-dev --package amqp-actuation-event-subscriber

FROM python:3.12-slim AS runtime
WORKDIR /workspace
COPY --from=builder /workspace/.venv /workspace/.venv
COPY --from=builder /workspace/libs /workspace/libs
COPY --from=builder /workspace/services/amqp-actuation-event-subscriber/src \
     /workspace/services/amqp-actuation-event-subscriber/src
ENV PATH="/workspace/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1
USER nobody:nogroup
EXPOSE 8080
ENTRYPOINT ["/workspace/.venv/bin/python", "-m", "amqp_actuation_event_subscriber"]
```

- [ ] **Step 8: Run tests to confirm pass**

```bash
uv run pytest services/amqp-actuation-event-subscriber/tests/ -v
```

Expected: 3 tests PASS.

- [ ] **Step 9: Commit**

```bash
git add services/amqp-actuation-event-subscriber/
git commit -m "feat(amqp-subscriber): bridge RabbitMQ actuation queue to NATS"
```

---

## Task 6: actuation-control-orchestrator — models and metrics

**Files:**
- Create: `services/actuation-control-orchestrator/src/actuation_control_orchestrator/models.py`
- Create: `services/actuation-control-orchestrator/src/actuation_control_orchestrator/metrics.py`
- Create: `services/actuation-control-orchestrator/pyproject.toml`

- [ ] **Step 1: Create pyproject.toml**

```toml
[project]
name = "actuation-control-orchestrator"
version = "0.0.0"
description = "Actuation pipeline orchestrator — owns validate/write sequencing (spec §3.2)."
requires-python = ">=3.12"
license = { text = "Apache-2.0" }
dependencies = [
    "aio-pika>=9.0",
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
packages = ["src/actuation_control_orchestrator"]
```

- [ ] **Step 2: Write failing tests for models**

```python
# services/actuation-control-orchestrator/tests/test_actuation_control_orchestrator.py
"""Unit tests for actuation-control-orchestrator."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from datetime import UTC, datetime, timedelta

import pytest
from prometheus_client.registry import CollectorRegistry


# ── models ───────────────────────────────────────────────────────────────────

def test_pipeline_config_loads_from_yaml(tmp_path: Path) -> None:
    from actuation_control_orchestrator.models import load_pipeline_config

    cfg_file = tmp_path / "actuation-control.yaml"
    cfg_file.write_text(
        "stages:\n"
        "  - name: validate\n"
        "    subject: act.work.validate\n"
        "    timeout_s: 2.0\n"
        "  - name: write_signal\n"
        "    subject: act.work.write_signal\n"
        "    timeout_s: 5.0\n"
        "dlq_subject: act.dlq.rejected\n"
    )
    cfg = load_pipeline_config(cfg_file)
    assert len(cfg.stages) == 2
    assert cfg.stages[0].name == "validate"
    assert cfg.stages[1].timeout_s == 5.0
    assert cfg.dlq_subject == "act.dlq.rejected"


# ── metrics ──────────────────────────────────────────────────────────────────

def test_actuation_metrics_create_without_error() -> None:
    from actuation_control_orchestrator.metrics import ActuationMetrics

    reg = CollectorRegistry()
    m = ActuationMetrics(registry=reg)
    m.inc_approved(path="actuation")
    m.inc_rejected(path="actuation", reason="writes_disabled")
    m.inc_stage_timeout(path="actuation", stage="validate")
    m.observe_e2e_latency(path="actuation", seconds=0.1)
```

- [ ] **Step 3: Run to confirm failure**

```bash
uv run pytest services/actuation-control-orchestrator/tests/test_actuation_control_orchestrator.py \
  -k "models or metrics" -v
```

Expected: `ModuleNotFoundError`.

- [ ] **Step 4: Create models.py**

```python
"""Internal models for the actuation control orchestrator."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel


class ActuationPipelineStage(BaseModel):
    name: str
    subject: str
    timeout_s: float = 2.0


class ActuationPipelineConfig(BaseModel):
    stages: list[ActuationPipelineStage]
    dlq_subject: str = "act.dlq.rejected"


def load_pipeline_config(path: Path) -> ActuationPipelineConfig:
    raw = yaml.safe_load(path.read_text())
    return ActuationPipelineConfig.model_validate(raw)
```

- [ ] **Step 5: Create metrics.py**

```python
"""Prometheus metrics for actuation-control-orchestrator."""

from __future__ import annotations

from eirvah_observability.metrics import make_counter, make_histogram
from prometheus_client.registry import REGISTRY, CollectorRegistry


class ActuationMetrics:
    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self._approved = make_counter(
            "actuation_approved_total",
            "Actuation requests approved and written",
            labelnames=["path"],
            registry=registry,
        )
        self._rejected = make_counter(
            "actuation_rejected_total",
            "Actuation requests rejected",
            labelnames=["path", "reason"],
            registry=registry,
        )
        self._stage_timeout = make_counter(
            "actuation_stage_timeout_total",
            "Actuation pipeline stage timeouts",
            labelnames=["path", "stage"],
            registry=registry,
        )
        self._e2e_latency = make_histogram(
            "actuation_e2e_latency_seconds",
            "End-to-end latency from NATS ingress to AMQP result",
            labelnames=["path"],
            registry=registry,
        )

    def inc_approved(self, *, path: str) -> None:
        self._approved.labels(path=path).inc()

    def inc_rejected(self, *, path: str, reason: str) -> None:
        self._rejected.labels(path=path, reason=reason).inc()

    def inc_stage_timeout(self, *, path: str, stage: str) -> None:
        self._stage_timeout.labels(path=path, stage=stage).inc()

    def observe_e2e_latency(self, *, path: str, seconds: float) -> None:
        self._e2e_latency.labels(path=path).observe(seconds)
```

- [ ] **Step 6: Create __init__.py**

Empty file at `src/actuation_control_orchestrator/__init__.py`.

- [ ] **Step 7: Run tests to confirm pass**

```bash
uv run pytest services/actuation-control-orchestrator/tests/test_actuation_control_orchestrator.py \
  -k "models or metrics" -v
```

Expected: 2 tests PASS.

- [ ] **Step 8: Commit**

```bash
git add services/actuation-control-orchestrator/pyproject.toml \
        services/actuation-control-orchestrator/src/actuation_control_orchestrator/__init__.py \
        services/actuation-control-orchestrator/src/actuation_control_orchestrator/models.py \
        services/actuation-control-orchestrator/src/actuation_control_orchestrator/metrics.py \
        services/actuation-control-orchestrator/tests/test_actuation_control_orchestrator.py
git commit -m "feat(actuation-orchestrator): models, metrics, and pyproject"
```

---

## Task 7: actuation-control-orchestrator — pipeline, config, service

**Files:**
- Create: `services/actuation-control-orchestrator/src/actuation_control_orchestrator/config.py`
- Create: `services/actuation-control-orchestrator/src/actuation_control_orchestrator/pipeline.py`
- Create: `services/actuation-control-orchestrator/src/actuation_control_orchestrator/service.py`
- Create: `services/actuation-control-orchestrator/src/actuation_control_orchestrator/__main__.py`
- Create: `services/actuation-control-orchestrator/Dockerfile`

- [ ] **Step 1: Write failing pipeline tests**

Add to `services/actuation-control-orchestrator/tests/test_actuation_control_orchestrator.py`:

```python
# ── pipeline runner ──────────────────────────────────────────────────────────

def _sample_actuation_envelope() -> "NATSEnvelope":  # noqa: F821
    from eirvah_contracts.envelope import NATSEnvelope
    from eirvah_contracts.actuation import ActuationRequest
    from eirvah_contracts.ulid import generate_correlation_id

    now = datetime.now(UTC)
    req = ActuationRequest(
        correlation_id=generate_correlation_id(),
        requester="decision-agent-stub",
        target_uns_topic=(
            "uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature"
        ),
        requested_value=22.0,
        value_type="double",
        reason="test",
        requested_at=now,
        deadline=now + timedelta(seconds=30),
    )
    return NATSEnvelope(
        correlation_id=req.correlation_id,
        payload=req.model_dump(mode="json"),
    )


@pytest.mark.asyncio
async def test_run_actuation_pipeline_approve_writes_disabled() -> None:
    """With allow_writes=False, valid request → reject with writes_disabled."""
    from actuation_control_orchestrator.models import ActuationPipelineConfig, ActuationPipelineStage
    from actuation_control_orchestrator.metrics import ActuationMetrics
    from actuation_control_orchestrator.pipeline import run_actuation_pipeline
    from eirvah_contracts.envelope import NATSEnvelope
    from eirvah_contracts.actuation import ValidationResult

    cfg = ActuationPipelineConfig(
        stages=[
            ActuationPipelineStage(name="validate", subject="act.work.validate", timeout_s=2.0),
            ActuationPipelineStage(name="write_signal", subject="act.work.write_signal", timeout_s=5.0),
        ],
        dlq_subject="act.dlq.rejected",
    )

    async def fake_request_reply(*, nc, subject, payload, correlation_id, timeout_s):
        msg = MagicMock()
        reply = NATSEnvelope(
            correlation_id=correlation_id,
            payload=ValidationResult(decision="approve").model_dump(mode="json"),
        )
        msg.data = reply.model_dump_json().encode()
        return msg

    nc_mock = MagicMock()
    nc_mock.publish = AsyncMock()
    amqp_channel_mock = MagicMock()
    amqp_channel_mock.default_exchange = MagicMock()
    amqp_channel_mock.default_exchange.publish = AsyncMock()
    amqp_exchange_mock = MagicMock()
    amqp_exchange_mock.publish = AsyncMock()

    reg = CollectorRegistry()
    metrics = ActuationMetrics(registry=reg)

    envelope = _sample_actuation_envelope()
    await run_actuation_pipeline(
        envelope=envelope,
        cfg=cfg,
        nc=nc_mock,
        amqp_results_exchange=amqp_exchange_mock,
        metrics=metrics,
        allow_writes=False,
        request_reply_fn=fake_request_reply,
    )

    # NATS DLQ published
    nc_mock.publish.assert_called_once()
    assert nc_mock.publish.call_args[0][0] == "act.dlq.rejected"
    # AMQP result published with reject
    amqp_exchange_mock.publish.assert_called_once()
    result_body = json.loads(amqp_exchange_mock.publish.call_args[0][0].body)
    assert result_body["decision"] == "reject"
    assert result_body["rejection_reason"] == "writes_disabled"


@pytest.mark.asyncio
async def test_run_actuation_pipeline_approve_writes_enabled() -> None:
    """With allow_writes=True, valid request → approve, write stage called."""
    from actuation_control_orchestrator.models import ActuationPipelineConfig, ActuationPipelineStage
    from actuation_control_orchestrator.metrics import ActuationMetrics
    from actuation_control_orchestrator.pipeline import run_actuation_pipeline
    from eirvah_contracts.envelope import NATSEnvelope
    from eirvah_contracts.actuation import ValidationResult

    cfg = ActuationPipelineConfig(
        stages=[
            ActuationPipelineStage(name="validate", subject="act.work.validate", timeout_s=2.0),
            ActuationPipelineStage(name="write_signal", subject="act.work.write_signal", timeout_s=5.0),
        ],
        dlq_subject="act.dlq.rejected",
    )

    calls: list[str] = []

    async def fake_request_reply(*, nc, subject, payload, correlation_id, timeout_s):
        calls.append(subject)
        msg = MagicMock()
        if subject == "act.work.validate":
            reply = NATSEnvelope(
                correlation_id=correlation_id,
                payload=ValidationResult(decision="approve").model_dump(mode="json"),
            )
        else:
            reply = NATSEnvelope(correlation_id=correlation_id)
        msg.data = reply.model_dump_json().encode()
        return msg

    nc_mock = MagicMock()
    nc_mock.publish = AsyncMock()
    amqp_exchange_mock = MagicMock()
    amqp_exchange_mock.publish = AsyncMock()

    reg = CollectorRegistry()
    metrics = ActuationMetrics(registry=reg)

    envelope = _sample_actuation_envelope()
    await run_actuation_pipeline(
        envelope=envelope,
        cfg=cfg,
        nc=nc_mock,
        amqp_results_exchange=amqp_exchange_mock,
        metrics=metrics,
        allow_writes=True,
        request_reply_fn=fake_request_reply,
    )

    assert "act.work.validate" in calls
    assert "act.work.write_signal" in calls
    amqp_exchange_mock.publish.assert_called_once()
    result_body = json.loads(amqp_exchange_mock.publish.call_args[0][0].body)
    assert result_body["decision"] == "approve"


@pytest.mark.asyncio
async def test_run_actuation_pipeline_deadline_expired() -> None:
    """Request with past deadline → reject before validate is called."""
    from actuation_control_orchestrator.models import ActuationPipelineConfig, ActuationPipelineStage
    from actuation_control_orchestrator.metrics import ActuationMetrics
    from actuation_control_orchestrator.pipeline import run_actuation_pipeline
    from eirvah_contracts.envelope import NATSEnvelope
    from eirvah_contracts.actuation import ActuationRequest
    from eirvah_contracts.ulid import generate_correlation_id

    cfg = ActuationPipelineConfig(
        stages=[
            ActuationPipelineStage(name="validate", subject="act.work.validate", timeout_s=2.0),
        ],
        dlq_subject="act.dlq.rejected",
    )

    fake_rr = AsyncMock()
    nc_mock = MagicMock()
    nc_mock.publish = AsyncMock()
    amqp_exchange_mock = MagicMock()
    amqp_exchange_mock.publish = AsyncMock()
    reg = CollectorRegistry()
    metrics = ActuationMetrics(registry=reg)

    now = datetime.now(UTC)
    req = ActuationRequest(
        correlation_id=generate_correlation_id(),
        requester="test",
        target_uns_topic=(
            "uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature"
        ),
        requested_value=22.0,
        value_type="double",
        reason="test",
        requested_at=now,
        deadline=now - timedelta(seconds=5),  # already expired
    )
    envelope = NATSEnvelope(
        correlation_id=req.correlation_id,
        payload=req.model_dump(mode="json"),
    )

    await run_actuation_pipeline(
        envelope=envelope,
        cfg=cfg,
        nc=nc_mock,
        amqp_results_exchange=amqp_exchange_mock,
        metrics=metrics,
        allow_writes=True,
        request_reply_fn=fake_rr,
    )

    fake_rr.assert_not_called()
    amqp_exchange_mock.publish.assert_called_once()
    result_body = json.loads(amqp_exchange_mock.publish.call_args[0][0].body)
    assert result_body["decision"] == "reject"
    assert result_body["rejection_reason"] == "expired"
```

Add `import json` at the top of the test file.

- [ ] **Step 2: Run to confirm failure**

```bash
uv run pytest services/actuation-control-orchestrator/tests/ -k "pipeline" -v
```

Expected: `ImportError`.

- [ ] **Step 3: Create config.py**

```python
"""Settings for the actuation control orchestrator."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ActuationOrchestratorSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ACTUATION_CONTROL_ORCHESTRATOR_",
        env_file=None,
        extra="ignore",
    )

    nats_servers: list[str] = ["nats://nats:4222"]
    amqp_url: str = "amqp://eirvah:eirvah-dev-password@rabbitmq:5672/"
    amqp_results_exchange: str = "eirvah.actuation.results"
    pipeline_config_path: Path = Path(
        "/etc/actuation-control-orchestrator/actuation-control.yaml"
    )
    allow_writes: bool = False
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
```

- [ ] **Step 4: Create pipeline.py**

```python
"""Actuation pipeline runner: drives validate → (conditional) write_signal."""

from __future__ import annotations

import json
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from typing import Any

import aio_pika
import structlog
from eirvah_bus.request_reply import RequestTimeout, request_reply
from eirvah_contracts.actuation import (
    ActuationRejectResult,
    ActuationApproveResult,
    ActuationRequest,
    ValidationResult,
)
from eirvah_contracts.envelope import EnvelopeError, NATSEnvelope
from nats.aio.client import Client as NATSClient

from actuation_control_orchestrator.metrics import ActuationMetrics
from actuation_control_orchestrator.models import ActuationPipelineConfig

_log = structlog.get_logger("actuation-control-orchestrator")
_PATH = "actuation"

RequestReplyFn = Callable[..., Coroutine[Any, Any, Any]]


async def run_actuation_pipeline(
    *,
    envelope: NATSEnvelope,
    cfg: ActuationPipelineConfig,
    nc: NATSClient,
    amqp_results_exchange: aio_pika.abc.AbstractExchange,
    metrics: ActuationMetrics,
    allow_writes: bool,
    request_reply_fn: RequestReplyFn = request_reply,
) -> None:
    ingress_at = datetime.now(UTC)

    try:
        req = ActuationRequest.model_validate(envelope.payload)
    except Exception as exc:
        _log.warning("bad_actuation_envelope", error=str(exc))
        return

    correlation_id = envelope.correlation_id

    # Deadline check
    if req.deadline and datetime.now(UTC) > req.deadline:
        await _emit_reject(nc, cfg.dlq_subject, amqp_results_exchange, req, reason="expired")
        metrics.inc_rejected(path=_PATH, reason="expired")
        return

    # Stage: validate
    validate_stage = next((s for s in cfg.stages if s.name == "validate"), None)
    if validate_stage is None:
        _log.error("missing_validate_stage")
        return

    try:
        reply_msg = await request_reply_fn(
            nc=nc,
            subject=validate_stage.subject,
            payload=NATSEnvelope(
                correlation_id=correlation_id,
                payload=req.model_dump(mode="json"),
            ).model_dump_json().encode(),
            correlation_id=correlation_id,
            timeout_s=validate_stage.timeout_s,
        )
    except RequestTimeout:
        metrics.inc_stage_timeout(path=_PATH, stage="validate")
        _log.warning("validate_timeout", correlation_id=correlation_id)
        await _emit_reject(nc, cfg.dlq_subject, amqp_results_exchange, req, reason="validate_timeout")
        metrics.inc_rejected(path=_PATH, reason="validate_timeout")
        return
    except Exception as exc:
        _log.warning("validate_error", error=str(exc), correlation_id=correlation_id)
        await _emit_reject(nc, cfg.dlq_subject, amqp_results_exchange, req, reason=type(exc).__name__)
        metrics.inc_rejected(path=_PATH, reason=type(exc).__name__)
        return

    try:
        reply_env = NATSEnvelope.model_validate_json(reply_msg.data)
        validation = ValidationResult.model_validate(reply_env.payload)
    except Exception as exc:
        _log.warning("validate_bad_reply", error=str(exc), correlation_id=correlation_id)
        await _emit_reject(nc, cfg.dlq_subject, amqp_results_exchange, req, reason="BadValidateReply")
        metrics.inc_rejected(path=_PATH, reason="BadValidateReply")
        return

    if validation.decision == "reject":
        reason = validation.reason or "rejected"
        await _emit_reject(nc, cfg.dlq_subject, amqp_results_exchange, req, reason=reason)
        metrics.inc_rejected(path=_PATH, reason=reason)
        return

    # Safety gate
    if not allow_writes:
        await _emit_reject(nc, cfg.dlq_subject, amqp_results_exchange, req, reason="writes_disabled")
        metrics.inc_rejected(path=_PATH, reason="writes_disabled")
        return

    # Stage: write_signal
    write_stage = next((s for s in cfg.stages if s.name == "write_signal"), None)
    if write_stage is None:
        _log.error("missing_write_signal_stage")
        return

    try:
        write_reply_msg = await request_reply_fn(
            nc=nc,
            subject=write_stage.subject,
            payload=NATSEnvelope(
                correlation_id=correlation_id,
                payload=req.model_dump(mode="json"),
            ).model_dump_json().encode(),
            correlation_id=correlation_id,
            timeout_s=write_stage.timeout_s,
        )
    except RequestTimeout:
        metrics.inc_stage_timeout(path=_PATH, stage="write_signal")
        await _emit_reject(nc, cfg.dlq_subject, amqp_results_exchange, req, reason="write_timeout")
        metrics.inc_rejected(path=_PATH, reason="write_timeout")
        return
    except Exception as exc:
        await _emit_reject(nc, cfg.dlq_subject, amqp_results_exchange, req, reason=type(exc).__name__)
        metrics.inc_rejected(path=_PATH, reason=type(exc).__name__)
        return

    write_env = NATSEnvelope.model_validate_json(write_reply_msg.data)
    if write_env.status == "error":
        reason = write_env.error.kind if write_env.error else "WriteError"
        await _emit_reject(nc, cfg.dlq_subject, amqp_results_exchange, req, reason=reason)
        metrics.inc_rejected(path=_PATH, reason=reason)
        return

    # Approve
    elapsed = (datetime.now(UTC) - ingress_at).total_seconds()
    await _emit_approve(amqp_results_exchange, req)
    metrics.inc_approved(path=_PATH)
    metrics.observe_e2e_latency(path=_PATH, seconds=elapsed)
    _log.info("actuation_approved", correlation_id=correlation_id, latency_s=elapsed)


async def _emit_reject(
    nc: NATSClient,
    dlq_subject: str,
    exchange: aio_pika.abc.AbstractExchange,
    req: ActuationRequest,
    *,
    reason: str,
) -> None:
    result = ActuationRejectResult(**req.model_dump(), decision="reject", rejection_reason=reason)
    body = result.model_dump_json().encode()
    await nc.publish(dlq_subject, body)
    await exchange.publish(
        aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
        routing_key="actuation.result",
    )
    _log.info("actuation_rejected", correlation_id=req.correlation_id, reason=reason)


async def _emit_approve(
    exchange: aio_pika.abc.AbstractExchange,
    req: ActuationRequest,
) -> None:
    result = ActuationApproveResult(
        **req.model_dump(),
        decision="approve",
        written_at=datetime.now(UTC),
    )
    body = result.model_dump_json().encode()
    await exchange.publish(
        aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
        routing_key="actuation.result",
    )
```

- [ ] **Step 5: Create service.py**

```python
"""Actuation control orchestrator service (spec §3.2)."""

from __future__ import annotations

import asyncio

import aio_pika
import structlog
import uvicorn
from eirvah_bus.client import BusClient
from eirvah_bus.consumer import subscribe_queue_group
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import configure_logging
from nats.aio.msg import Msg

from actuation_control_orchestrator.config import ActuationOrchestratorSettings
from actuation_control_orchestrator.metrics import ActuationMetrics
from actuation_control_orchestrator.models import ActuationPipelineConfig, load_pipeline_config
from actuation_control_orchestrator.pipeline import run_actuation_pipeline

_log = structlog.get_logger("actuation-control-orchestrator")
INGRESS_SUBJECT = "act.ingress.requested"


class ActuationOrchestratorRuntime:
    def __init__(self, settings: ActuationOrchestratorSettings) -> None:
        self._settings = settings
        self._bus: BusClient | None = None
        self._cfg: ActuationPipelineConfig | None = None
        self._amqp_exchange: aio_pika.abc.AbstractExchange | None = None
        self._metrics = ActuationMetrics()
        self._ready = False

    def is_ready(self) -> bool:
        return self._ready

    async def run(self) -> None:
        self._cfg = load_pipeline_config(self._settings.pipeline_config_path)
        self._bus = BusClient(servers=self._settings.nats_servers, name="actuation-control-orchestrator")
        await self._bus.connect()

        amqp_conn = await aio_pika.connect_robust(self._settings.amqp_url)
        amqp_channel = await amqp_conn.channel()
        self._amqp_exchange = await amqp_channel.declare_exchange(
            self._settings.amqp_results_exchange,
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )

        await subscribe_queue_group(
            nc=self._bus.nc, subject=INGRESS_SUBJECT, handler=self._handle
        )
        self._ready = True
        _log.info(
            "actuation_orchestrator_ready",
            subject=INGRESS_SUBJECT,
            allow_writes=self._settings.allow_writes,
        )
        await asyncio.get_event_loop().create_future()

    async def _handle(self, msg: Msg) -> None:
        assert self._cfg is not None and self._bus is not None and self._amqp_exchange is not None
        try:
            envelope = NATSEnvelope.model_validate_json(msg.data)
        except Exception as exc:
            _log.warning("invalid_ingress_message", error=str(exc))
            return
        await run_actuation_pipeline(
            envelope=envelope,
            cfg=self._cfg,
            nc=self._bus.nc,
            amqp_results_exchange=self._amqp_exchange,
            metrics=self._metrics,
            allow_writes=self._settings.allow_writes,
        )


async def run(settings: ActuationOrchestratorSettings) -> None:
    configure_logging(level=settings.log_level)
    runtime = ActuationOrchestratorRuntime(settings)
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

- [ ] **Step 6: Create __main__.py**

```python
"""Entry point for the actuation control orchestrator pod."""

from __future__ import annotations

import asyncio

from actuation_control_orchestrator.config import ActuationOrchestratorSettings
from actuation_control_orchestrator.service import run


def main() -> None:
    asyncio.run(run(ActuationOrchestratorSettings()))


if __name__ == "__main__":  # pragma: no cover
    main()
```

- [ ] **Step 7: Create Dockerfile**

```dockerfile
# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS builder
WORKDIR /workspace
RUN pip install --no-cache-dir uv==0.4.20
COPY pyproject.toml uv.lock /workspace/
COPY libs /workspace/libs
COPY services/actuation-control-orchestrator /workspace/services/actuation-control-orchestrator
RUN uv sync --frozen --no-dev --package actuation-control-orchestrator

FROM python:3.12-slim AS runtime
WORKDIR /workspace
COPY --from=builder /workspace/.venv /workspace/.venv
COPY --from=builder /workspace/libs /workspace/libs
COPY --from=builder /workspace/services/actuation-control-orchestrator/src \
     /workspace/services/actuation-control-orchestrator/src
ENV PATH="/workspace/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1
USER nobody:nogroup
EXPOSE 8080
ENTRYPOINT ["/workspace/.venv/bin/python", "-m", "actuation_control_orchestrator"]
```

- [ ] **Step 8: Run all orchestrator tests**

```bash
uv run pytest services/actuation-control-orchestrator/tests/ -v
```

Expected: all 5 tests PASS.

- [ ] **Step 9: Commit**

```bash
git add services/actuation-control-orchestrator/
git commit -m "feat(actuation-orchestrator): pipeline runner, service, and config"
```

---

## Task 8: actuation-event-validator

**Files:**
- Create: `services/actuation-event-validator/pyproject.toml`
- Create: `services/actuation-event-validator/Dockerfile`
- Create: `services/actuation-event-validator/src/actuation_event_validator/__init__.py`
- Create: `services/actuation-event-validator/src/actuation_event_validator/__main__.py`
- Create: `services/actuation-event-validator/src/actuation_event_validator/config.py`
- Create: `services/actuation-event-validator/src/actuation_event_validator/service.py`
- Create: `services/actuation-event-validator/tests/test_actuation_event_validator.py`

- [ ] **Step 1: Create pyproject.toml**

```toml
[project]
name = "actuation-event-validator"
version = "0.0.0"
description = "Validates actuation requests against policy bounds (spec §3.2)."
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
packages = ["src/actuation_event_validator"]
```

- [ ] **Step 2: Write failing tests**

```python
# services/actuation-event-validator/tests/test_actuation_event_validator.py
"""Unit tests for actuation-event-validator."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from eirvah_contracts.actuation import ActuationRequest, ValidationResult
from eirvah_contracts.ulid import generate_correlation_id


def _sample_request(
    value: float = 22.0,
    requester: str = "decision-agent-stub",
    topic: str = "uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature",
) -> ActuationRequest:
    now = datetime.now(UTC)
    return ActuationRequest(
        correlation_id=generate_correlation_id(),
        requester=requester,
        target_uns_topic=topic,
        requested_value=value,
        value_type="double",
        reason="test",
        requested_at=now,
    )


def _write_policy(tmp_path: Path) -> Path:
    policy_file = tmp_path / "actuation-policy.yaml"
    policy_file.write_text(
        "policies:\n"
        "  - uns_topic: \"uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature\"\n"
        "    allowed_range: [20.0, 30.0]\n"
        "    allowlist:\n"
        "      - decision-agent-stub\n"
    )
    return policy_file


def test_validate_approve(tmp_path: Path) -> None:
    from actuation_event_validator.service import load_policy, validate_request

    policies = load_policy(_write_policy(tmp_path))
    result = validate_request(_sample_request(value=22.0), policies)
    assert result.decision == "approve"
    assert result.reason is None


def test_validate_reject_out_of_range(tmp_path: Path) -> None:
    from actuation_event_validator.service import load_policy, validate_request

    policies = load_policy(_write_policy(tmp_path))
    result = validate_request(_sample_request(value=99.0), policies)
    assert result.decision == "reject"
    assert result.reason is not None
    assert "outside policy range" in result.reason


def test_validate_reject_unknown_requester(tmp_path: Path) -> None:
    from actuation_event_validator.service import load_policy, validate_request

    policies = load_policy(_write_policy(tmp_path))
    result = validate_request(_sample_request(requester="intruder"), policies)
    assert result.decision == "reject"
    assert result.reason is not None
    assert "allowlist" in result.reason


def test_validate_reject_unknown_topic(tmp_path: Path) -> None:
    from actuation_event_validator.service import load_policy, validate_request

    policies = load_policy(_write_policy(tmp_path))
    result = validate_request(
        _sample_request(topic="uniza/zilina/factory1/line_a/bottler/motor_01/rpm"),
        policies,
    )
    assert result.decision == "reject"
    assert result.reason is not None
    assert "no policy" in result.reason


def test_load_policy_from_yaml(tmp_path: Path) -> None:
    from actuation_event_validator.service import load_policy

    policies = load_policy(_write_policy(tmp_path))
    key = "uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature"
    assert key in policies
    assert policies[key].allowed_range == (20.0, 30.0)
    assert "decision-agent-stub" in policies[key].allowlist
```

- [ ] **Step 3: Run to confirm failure**

```bash
uv run pytest services/actuation-event-validator/tests/ -v
```

Expected: `ModuleNotFoundError`.

- [ ] **Step 4: Create service.py**

```python
"""Actuation event validator NATS req/rep worker (spec §3.2)."""

from __future__ import annotations

import asyncio
from pathlib import Path

import structlog
import uvicorn
import yaml
from eirvah_bus.client import BusClient
from eirvah_bus.consumer import subscribe_queue_group
from eirvah_contracts.actuation import ActuationRequest, ValidationResult
from eirvah_contracts.envelope import EnvelopeError, NATSEnvelope
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import configure_logging
from eirvah_observability.metrics import make_counter
from nats.aio.msg import Msg
from pydantic import BaseModel

from actuation_event_validator.config import ValidatorSettings

_log = structlog.get_logger("actuation-event-validator")
SUBJECT = "act.work.validate"


class NodePolicy(BaseModel):
    uns_topic: str
    allowed_range: tuple[float, float]
    allowlist: list[str]


class PolicyConfig(BaseModel):
    policies: list[NodePolicy]


def load_policy(path: Path) -> dict[str, NodePolicy]:
    raw = yaml.safe_load(path.read_text())
    cfg = PolicyConfig.model_validate(raw)
    return {p.uns_topic: p for p in cfg.policies}


def validate_request(
    req: ActuationRequest,
    policies: dict[str, NodePolicy],
) -> ValidationResult:
    policy = policies.get(req.target_uns_topic)
    if policy is None:
        return ValidationResult(
            decision="reject",
            reason=f"no policy for topic {req.target_uns_topic!r}",
        )
    if req.requester not in policy.allowlist:
        return ValidationResult(
            decision="reject",
            reason=f"requester {req.requester!r} not in allowlist",
        )
    try:
        value = float(req.requested_value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return ValidationResult(
            decision="reject",
            reason="requested_value is not numeric",
        )
    lo, hi = policy.allowed_range
    if not (lo <= value <= hi):
        return ValidationResult(
            decision="reject",
            reason=f"value {value} outside policy range [{lo}, {hi}]",
        )
    return ValidationResult(decision="approve")


class ValidatorWorker:
    def __init__(self, settings: ValidatorSettings) -> None:
        self._settings = settings
        self._policies: dict[str, NodePolicy] = {}
        self._ready = False
        self._handled = make_counter(
            "worker_handler_total",
            "Worker handler invocations",
            labelnames=["worker", "outcome"],
        )

    def is_ready(self) -> bool:
        return self._ready

    async def run(self) -> None:
        self._policies = load_policy(self._settings.policy_path)
        bus = BusClient(servers=self._settings.nats_servers, name="actuation-event-validator")
        await bus.connect()
        await subscribe_queue_group(nc=bus.nc, subject=SUBJECT, handler=self._handle)
        self._ready = True
        _log.info("validator_ready", subject=SUBJECT, policies=len(self._policies))
        await asyncio.get_event_loop().create_future()

    async def _handle(self, msg: Msg) -> None:
        correlation_id = "UNKNOWN"
        try:
            envelope = NATSEnvelope.model_validate_json(msg.data)
            correlation_id = envelope.correlation_id
            req = ActuationRequest.model_validate(envelope.payload)
            result = validate_request(req, self._policies)
            self._handled.labels(worker="actuation-event-validator", outcome=result.decision).inc()
            reply = NATSEnvelope(
                correlation_id=correlation_id,
                payload=result.model_dump(mode="json"),
            )
        except Exception as exc:
            self._handled.labels(worker="actuation-event-validator", outcome="error").inc()
            reply = NATSEnvelope(
                correlation_id=correlation_id,
                status="error",
                error=EnvelopeError(kind=type(exc).__name__, message=str(exc)[:200]),
            )
        await msg.respond(reply.model_dump_json().encode())


async def run(settings: ValidatorSettings) -> None:
    configure_logging(level=settings.log_level)
    worker = ValidatorWorker(settings)
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

- [ ] **Step 5: Create config.py**

```python
"""Settings for the actuation event validator."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ValidatorSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ACTUATION_EVENT_VALIDATOR_",
        env_file=None,
        extra="ignore",
    )

    nats_servers: list[str] = ["nats://nats:4222"]
    policy_path: Path = Path("/etc/actuation-event-validator/actuation-policy.yaml")
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
```

- [ ] **Step 6: Create __init__.py and __main__.py**

`src/actuation_event_validator/__init__.py` — empty.

```python
# src/actuation_event_validator/__main__.py
"""Entry point for the actuation event validator pod."""

from __future__ import annotations

import asyncio

from actuation_event_validator.config import ValidatorSettings
from actuation_event_validator.service import run


def main() -> None:
    asyncio.run(run(ValidatorSettings()))


if __name__ == "__main__":  # pragma: no cover
    main()
```

- [ ] **Step 7: Create Dockerfile**

```dockerfile
# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS builder
WORKDIR /workspace
RUN pip install --no-cache-dir uv==0.4.20
COPY pyproject.toml uv.lock /workspace/
COPY libs /workspace/libs
COPY services/actuation-event-validator /workspace/services/actuation-event-validator
RUN uv sync --frozen --no-dev --package actuation-event-validator

FROM python:3.12-slim AS runtime
WORKDIR /workspace
COPY --from=builder /workspace/.venv /workspace/.venv
COPY --from=builder /workspace/libs /workspace/libs
COPY --from=builder /workspace/services/actuation-event-validator/src \
     /workspace/services/actuation-event-validator/src
ENV PATH="/workspace/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1
USER nobody:nogroup
EXPOSE 8080
ENTRYPOINT ["/workspace/.venv/bin/python", "-m", "actuation_event_validator"]
```

- [ ] **Step 8: Run tests to confirm pass**

```bash
uv run pytest services/actuation-event-validator/tests/ -v
```

Expected: 5 tests PASS.

- [ ] **Step 9: Commit**

```bash
git add services/actuation-event-validator/
git commit -m "feat(validator): actuation policy validation worker"
```

---

## Task 9: actuation-signal-publisher

**Files:**
- Create: `services/actuation-signal-publisher/pyproject.toml`
- Create: `services/actuation-signal-publisher/Dockerfile`
- Create: `services/actuation-signal-publisher/src/actuation_signal_publisher/__init__.py`
- Create: `services/actuation-signal-publisher/src/actuation_signal_publisher/__main__.py`
- Create: `services/actuation-signal-publisher/src/actuation_signal_publisher/config.py`
- Create: `services/actuation-signal-publisher/src/actuation_signal_publisher/service.py`
- Create: `services/actuation-signal-publisher/tests/test_actuation_signal_publisher.py`

**Key design:** Loads `opcua-node-to-uns-mapping.yaml` (inverted → `uns_topic → alias`) and `opcua-node-list.yaml` (alias → browse_names). Combined with `ENTERPRISE` + `SITE` env vars, builds `uns_topic → browse_names` for OPC UA writes.

- [ ] **Step 1: Create pyproject.toml**

```toml
[project]
name = "actuation-signal-publisher"
version = "0.0.0"
description = "Resolves UNS topic to OPC UA node and writes value (spec §3.2)."
requires-python = ">=3.12"
license = { text = "Apache-2.0" }
dependencies = [
    "asyncua>=1.0",
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
packages = ["src/actuation_signal_publisher"]
```

- [ ] **Step 2: Write failing tests**

```python
# services/actuation-signal-publisher/tests/test_actuation_signal_publisher.py
"""Unit tests for actuation-signal-publisher."""

from __future__ import annotations

from pathlib import Path

import pytest


def _write_mapping(tmp_path: Path) -> Path:
    f = tmp_path / "opcua-node-to-uns-mapping.yaml"
    f.write_text(
        "mappings:\n"
        "  - node_id: \"Bottler.SetpointUnit.SetpointTemperature\"\n"
        "    area: factory1\n"
        "    line: line_a\n"
        "    cell: bottler\n"
        "    equipment: setpoint_unit\n"
        "    measurement: setpoint_temperature\n"
        "    semantic_type: setpoint.target\n"
        "  - node_id: \"Bottler.Temperature01\"\n"
        "    area: factory1\n"
        "    line: line_a\n"
        "    cell: bottler\n"
        "    equipment: temperature_sensor_01\n"
        "    measurement: temperature\n"
        "    semantic_type: temperature.celsius\n"
    )
    return f


def _write_node_list(tmp_path: Path) -> Path:
    f = tmp_path / "opcua-node-list.yaml"
    f.write_text(
        "endpoint: \"opc.tcp://opcua-simulator:4840/eirvah/simulator\"\n"
        "namespace_uri: \"https://eirvah.uniza/zilina/factory1\"\n"
        "publishing_interval_ms: 500\n"
        "nodes:\n"
        "  - browse_names: [\"bottler\", \"SetpointTemperature\"]\n"
        "    alias: \"Bottler.SetpointUnit.SetpointTemperature\"\n"
        "  - browse_names: [\"bottler\", \"Temperature\"]\n"
        "    alias: \"Bottler.Temperature01\"\n"
    )
    return f


def test_load_reverse_mapping_builds_uns_to_browse(tmp_path: Path) -> None:
    from actuation_signal_publisher.service import load_write_targets

    mapping_path = _write_mapping(tmp_path)
    node_list_path = _write_node_list(tmp_path)
    targets = load_write_targets(
        mapping_path=mapping_path,
        node_list_path=node_list_path,
        enterprise="uniza",
        site="zilina",
    )
    key = "uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature"
    assert key in targets
    assert targets[key].browse_names == ["bottler", "SetpointTemperature"]
    assert targets[key].endpoint == "opc.tcp://opcua-simulator:4840/eirvah/simulator"
    assert targets[key].namespace_uri == "https://eirvah.uniza/zilina/factory1"


def test_load_write_targets_fails_on_non_bijective_mapping(tmp_path: Path) -> None:
    from actuation_signal_publisher.service import load_write_targets

    bad_mapping = tmp_path / "bad.yaml"
    bad_mapping.write_text(
        "mappings:\n"
        "  - node_id: \"NodeA\"\n"
        "    area: factory1\n    line: line_a\n    cell: bottler\n"
        "    equipment: eq1\n    measurement: m1\n    semantic_type: x\n"
        "  - node_id: \"NodeB\"\n"
        "    area: factory1\n    line: line_a\n    cell: bottler\n"
        "    equipment: eq1\n    measurement: m1\n    semantic_type: x\n"  # duplicate UNS topic
    )
    node_list = tmp_path / "nl.yaml"
    node_list.write_text(
        "endpoint: opc.tcp://x:4840\nnamespace_uri: x\npublishing_interval_ms: 500\n"
        "nodes:\n  - browse_names: [\"eq1\"]\n    alias: \"NodeA\"\n"
        "  - browse_names: [\"eq1b\"]\n    alias: \"NodeB\"\n"
    )
    with pytest.raises(ValueError, match="bijective"):
        load_write_targets(
            mapping_path=bad_mapping,
            node_list_path=node_list,
            enterprise="uniza",
            site="zilina",
        )
```

- [ ] **Step 3: Run to confirm failure**

```bash
uv run pytest services/actuation-signal-publisher/tests/ -v
```

Expected: `ModuleNotFoundError`.

- [ ] **Step 4: Create service.py**

```python
"""Actuation signal publisher NATS req/rep worker (spec §3.2)."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import structlog
import uvicorn
import yaml
from asyncua import Client
from eirvah_bus.client import BusClient
from eirvah_bus.consumer import subscribe_queue_group
from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.envelope import EnvelopeError, NATSEnvelope
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import configure_logging
from eirvah_observability.metrics import make_counter
from nats.aio.msg import Msg
from pydantic import BaseModel

from actuation_signal_publisher.config import SignalPublisherSettings

_log = structlog.get_logger("actuation-signal-publisher")
SUBJECT = "act.work.write_signal"


@dataclass
class WriteTarget:
    browse_names: list[str]
    endpoint: str
    namespace_uri: str


class _NodeListEntry(BaseModel):
    browse_names: list[str]
    alias: str


class _NodeListConfig(BaseModel):
    endpoint: str
    namespace_uri: str
    publishing_interval_ms: int = 500
    nodes: list[_NodeListEntry]


def load_write_targets(
    *,
    mapping_path: Path,
    node_list_path: Path,
    enterprise: str,
    site: str,
) -> dict[str, WriteTarget]:
    """Build uns_topic → WriteTarget. Fails if mapping is not bijective."""
    mapping_raw = yaml.safe_load(mapping_path.read_text())
    node_list_raw = yaml.safe_load(node_list_path.read_text())
    node_list = _NodeListConfig.model_validate(node_list_raw)

    alias_to_browse: dict[str, list[str]] = {
        entry.alias: entry.browse_names for entry in node_list.nodes
    }

    targets: dict[str, WriteTarget] = {}
    for entry in mapping_raw["mappings"]:
        alias = entry["node_id"]
        topic = (
            f"{enterprise}/{site}/{entry['area']}/{entry['line']}"
            f"/{entry['cell']}/{entry['equipment']}/{entry['measurement']}"
        )
        if topic in targets:
            raise ValueError(
                f"mapping not bijective: topic {topic!r} maps to multiple node_ids"
            )
        browse_names = alias_to_browse.get(alias)
        if browse_names is None:
            continue  # node not in node-list (not writable via this service)
        targets[topic] = WriteTarget(
            browse_names=browse_names,
            endpoint=node_list.endpoint,
            namespace_uri=node_list.namespace_uri,
        )

    return targets


async def write_opcua_value(
    *,
    target: WriteTarget,
    value: Any,
) -> None:
    async with Client(url=target.endpoint) as client:
        ns_idx = await client.get_namespace_index(target.namespace_uri)
        path = [f"{ns_idx}:{name}" for name in target.browse_names]
        node = await client.nodes.objects.get_child(path)
        await node.write_value(value)


class SignalPublisherWorker:
    def __init__(self, settings: SignalPublisherSettings) -> None:
        self._settings = settings
        self._targets: dict[str, WriteTarget] = {}
        self._ready = False
        self._handled = make_counter(
            "worker_handler_total",
            "Worker handler invocations",
            labelnames=["worker", "outcome"],
        )

    def is_ready(self) -> bool:
        return self._ready

    async def run(self) -> None:
        self._targets = load_write_targets(
            mapping_path=self._settings.mapping_path,
            node_list_path=self._settings.node_list_path,
            enterprise=self._settings.enterprise,
            site=self._settings.site,
        )
        bus = BusClient(servers=self._settings.nats_servers, name="actuation-signal-publisher")
        await bus.connect()
        await subscribe_queue_group(nc=bus.nc, subject=SUBJECT, handler=self._handle)
        self._ready = True
        _log.info("signal_publisher_ready", subject=SUBJECT, targets=len(self._targets))
        await asyncio.get_event_loop().create_future()

    async def _handle(self, msg: Msg) -> None:
        correlation_id = "UNKNOWN"
        try:
            envelope = NATSEnvelope.model_validate_json(msg.data)
            correlation_id = envelope.correlation_id
            req = ActuationRequest.model_validate(envelope.payload)

            target = self._targets.get(req.target_uns_topic)
            if target is None:
                raise ValueError(f"no write target for topic {req.target_uns_topic!r}")

            await write_opcua_value(target=target, value=req.requested_value)

            self._handled.labels(worker="actuation-signal-publisher", outcome="ok").inc()
            reply = NATSEnvelope(correlation_id=correlation_id)
        except Exception as exc:
            self._handled.labels(worker="actuation-signal-publisher", outcome="error").inc()
            reply = NATSEnvelope(
                correlation_id=correlation_id,
                status="error",
                error=EnvelopeError(kind=type(exc).__name__, message=str(exc)[:200]),
            )
        await msg.respond(reply.model_dump_json().encode())


async def run(settings: SignalPublisherSettings) -> None:
    configure_logging(level=settings.log_level)
    worker = SignalPublisherWorker(settings)
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

- [ ] **Step 5: Create config.py**

```python
"""Settings for the actuation signal publisher."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SignalPublisherSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ACTUATION_SIGNAL_PUBLISHER_",
        env_file=None,
        extra="ignore",
    )

    nats_servers: list[str] = ["nats://nats:4222"]
    mapping_path: Path = Path(
        "/etc/actuation-signal-publisher/opcua-node-to-uns-mapping.yaml"
    )
    node_list_path: Path = Path(
        "/etc/actuation-signal-publisher/opcua-node-list.yaml"
    )
    enterprise: str = "uniza"
    site: str = "zilina"
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
```

- [ ] **Step 6: Create __init__.py and __main__.py**

`src/actuation_signal_publisher/__init__.py` — empty.

```python
# src/actuation_signal_publisher/__main__.py
"""Entry point for the actuation signal publisher pod."""

from __future__ import annotations

import asyncio

from actuation_signal_publisher.config import SignalPublisherSettings
from actuation_signal_publisher.service import run


def main() -> None:
    asyncio.run(run(SignalPublisherSettings()))


if __name__ == "__main__":  # pragma: no cover
    main()
```

- [ ] **Step 7: Create Dockerfile**

```dockerfile
# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS builder
WORKDIR /workspace
RUN pip install --no-cache-dir uv==0.4.20
COPY pyproject.toml uv.lock /workspace/
COPY libs /workspace/libs
COPY services/actuation-signal-publisher /workspace/services/actuation-signal-publisher
RUN uv sync --frozen --no-dev --package actuation-signal-publisher

FROM python:3.12-slim AS runtime
WORKDIR /workspace
COPY --from=builder /workspace/.venv /workspace/.venv
COPY --from=builder /workspace/libs /workspace/libs
COPY --from=builder /workspace/services/actuation-signal-publisher/src \
     /workspace/services/actuation-signal-publisher/src
ENV PATH="/workspace/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1
USER nobody:nogroup
EXPOSE 8080
ENTRYPOINT ["/workspace/.venv/bin/python", "-m", "actuation_signal_publisher"]
```

- [ ] **Step 8: Run tests to confirm pass**

```bash
uv run pytest services/actuation-signal-publisher/tests/ -v
```

Expected: 2 tests PASS.

- [ ] **Step 9: Commit**

```bash
git add services/actuation-signal-publisher/
git commit -m "feat(signal-publisher): OPC UA write worker with reverse mapping"
```

---

## Task 10: decision-agent-stub

**Files:**
- Create: `services/decision-agent-stub/pyproject.toml`
- Create: `services/decision-agent-stub/Dockerfile`
- Create: `services/decision-agent-stub/src/decision_agent_stub/__init__.py`
- Create: `services/decision-agent-stub/src/decision_agent_stub/__main__.py`
- Create: `services/decision-agent-stub/src/decision_agent_stub/config.py`
- Create: `services/decision-agent-stub/src/decision_agent_stub/service.py`
- Create: `services/decision-agent-stub/tests/test_decision_agent_stub.py`

- [ ] **Step 1: Create pyproject.toml**

```toml
[project]
name = "decision-agent-stub"
version = "0.0.0"
description = "Closes CPS loop: subscribes MQTT temperature, emits AMQP actuation request (spec §3.3)."
requires-python = ">=3.12"
license = { text = "Apache-2.0" }
dependencies = [
    "aio-pika>=9.0",
    "aiomqtt>=2.0",
    "pydantic>=2.8",
    "pydantic-settings>=2.5",
    "structlog>=24.0",
    "python-ulid>=2.0",
    "eirvah-contracts",
    "eirvah-observability",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/decision_agent_stub"]
```

- [ ] **Step 2: Write failing tests**

```python
# services/decision-agent-stub/tests/test_decision_agent_stub.py
"""Unit tests for decision-agent-stub."""

from __future__ import annotations

from datetime import UTC, datetime


def test_threshold_not_breached_returns_none() -> None:
    from decision_agent_stub.service import TriggerWindow

    window = TriggerWindow(threshold=26.0, duration_s=30.0)
    now = datetime.now(UTC)
    result = window.update(value=25.0, ts=now, correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A")
    assert result is None


def test_threshold_sustained_returns_request() -> None:
    from decision_agent_stub.service import TriggerWindow
    from eirvah_contracts.actuation import ActuationRequest
    import time

    window = TriggerWindow(threshold=26.0, duration_s=1.0)  # 1s for test speed
    now = datetime.now(UTC)
    window.update(value=27.0, ts=now, correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A")

    import asyncio
    time.sleep(1.1)

    from datetime import timedelta
    later = now + timedelta(seconds=1.1)
    result = window.update(value=27.5, ts=later, correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4B")
    assert result is not None
    assert result.requested_value == 22.0
    assert result.requester == "decision-agent-stub"


def test_cooldown_prevents_second_fire() -> None:
    from decision_agent_stub.service import TriggerWindow
    from datetime import timedelta

    window = TriggerWindow(threshold=26.0, duration_s=1.0, cooldown_s=60.0)
    now = datetime.now(UTC)
    window.update(value=27.0, ts=now, correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A")

    import time
    time.sleep(1.1)

    later = now + timedelta(seconds=1.1)
    first = window.update(value=27.0, ts=later, correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4B")
    assert first is not None

    # Immediately try again — cooldown should block it
    even_later = later + timedelta(seconds=0.1)
    second = window.update(value=27.0, ts=even_later, correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4C")
    assert second is None


def test_value_below_threshold_resets_window() -> None:
    from decision_agent_stub.service import TriggerWindow
    from datetime import timedelta

    window = TriggerWindow(threshold=26.0, duration_s=30.0)
    now = datetime.now(UTC)
    window.update(value=27.0, ts=now, correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A")

    below = now + timedelta(seconds=5)
    window.update(value=25.0, ts=below, correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4B")

    # Window reset — should not fire even after original duration
    late = now + timedelta(seconds=31)
    result = window.update(value=27.0, ts=late, correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4C")
    assert result is None
```

- [ ] **Step 3: Run to confirm failure**

```bash
uv run pytest services/decision-agent-stub/tests/ -v
```

Expected: `ModuleNotFoundError`.

- [ ] **Step 4: Create service.py**

```python
"""Decision agent stub — closes the CPS loop (spec §3.3)."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import aio_pika
import aiomqtt
import structlog
from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.ulid import generate_correlation_id
from eirvah_observability.logging import configure_logging
from eirvah_observability.metrics import make_counter

from decision_agent_stub.config import DecisionAgentSettings

_log = structlog.get_logger("decision-agent-stub")


class TriggerWindow:
    """Tracks sustained threshold breach and fires actuation request when triggered."""

    def __init__(
        self,
        *,
        threshold: float,
        duration_s: float,
        cooldown_s: float = 60.0,
        setpoint_target: float = 22.0,
        target_uns_topic: str = (
            "uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature"
        ),
    ) -> None:
        self._threshold = threshold
        self._duration_s = duration_s
        self._cooldown_s = cooldown_s
        self._setpoint_target = setpoint_target
        self._target_uns_topic = target_uns_topic
        self._breach_start: datetime | None = None
        self._last_fired: datetime | None = None

    def update(
        self,
        *,
        value: float,
        ts: datetime,
        correlation_id: str,
    ) -> ActuationRequest | None:
        if value <= self._threshold:
            self._breach_start = None
            return None

        if self._breach_start is None:
            self._breach_start = ts
            return None

        if (ts - self._breach_start).total_seconds() < self._duration_s:
            return None

        # Breach sustained — check cooldown
        if self._last_fired is not None:
            if (ts - self._last_fired).total_seconds() < self._cooldown_s:
                return None

        self._last_fired = ts
        self._breach_start = None  # reset for next cycle
        now = datetime.now(UTC)
        return ActuationRequest(
            correlation_id=correlation_id,
            requester="decision-agent-stub",
            target_uns_topic=self._target_uns_topic,
            requested_value=self._setpoint_target,
            value_type="double",
            reason=f"telemetry threshold breach: temperature > {self._threshold} for {self._duration_s}s",
            requested_at=now,
            deadline=now + timedelta(seconds=10),
        )


class DecisionAgentRuntime:
    def __init__(self, settings: DecisionAgentSettings) -> None:
        self._settings = settings
        self._window = TriggerWindow(
            threshold=settings.threshold,
            duration_s=settings.trigger_duration_s,
            cooldown_s=settings.cooldown_s,
            setpoint_target=settings.setpoint_target,
            target_uns_topic=settings.target_uns_topic,
        )
        self._fired = make_counter(
            "decision_agent_actuation_fired_total",
            "Actuation requests emitted",
            labelnames=["reason"],
        )

    async def run(self) -> None:
        configure_logging(level=self._settings.log_level)
        amqp_conn = await aio_pika.connect_robust(self._settings.amqp_url)

        async with amqp_conn:
            amqp_channel = await amqp_conn.channel()

            async with aiomqtt.Client(
                hostname=self._settings.mqtt_host,
                port=self._settings.mqtt_port,
                username=self._settings.mqtt_username,
                password=self._settings.mqtt_password,
            ) as mqtt:
                await mqtt.subscribe(self._settings.subscribe_topic, qos=1)
                _log.info(
                    "decision_agent_ready",
                    topic=self._settings.subscribe_topic,
                    threshold=self._settings.threshold,
                )

                async for message in mqtt.messages:
                    try:
                        payload = json.loads(message.payload)
                        value = float(payload["value"])
                        correlation_id = payload.get("correlation_id") or generate_correlation_id()
                        ts = datetime.now(UTC)
                        req = self._window.update(value=value, ts=ts, correlation_id=correlation_id)
                        if req is not None:
                            await amqp_channel.default_exchange.publish(
                                aio_pika.Message(
                                    body=req.model_dump_json().encode(),
                                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                                ),
                                routing_key=self._settings.amqp_queue,
                            )
                            self._fired.labels(reason="threshold_breach").inc()
                            _log.info(
                                "actuation_request_emitted",
                                correlation_id=req.correlation_id,
                                value=value,
                            )
                    except Exception as exc:
                        _log.warning("message_processing_error", error=str(exc))


async def run(settings: DecisionAgentSettings) -> None:
    runtime = DecisionAgentRuntime(settings)
    await runtime.run()
```

- [ ] **Step 5: Create config.py**

```python
"""Settings for the decision agent stub."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DecisionAgentSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DECISION_AGENT_STUB_",
        env_file=None,
        extra="ignore",
    )

    mqtt_host: str = "mosquitto"
    mqtt_port: int = 1883
    mqtt_username: str = "eirvah"
    mqtt_password: str = "eirvah-dev-password"
    subscribe_topic: str = (
        "uniza/zilina/factory1/line_a/bottler/temperature_sensor_01/temperature"
    )
    amqp_url: str = "amqp://eirvah:eirvah-dev-password@rabbitmq:5672/"
    amqp_queue: str = "eirvah.actuation.requests"
    threshold: float = 26.0
    trigger_duration_s: float = 30.0
    setpoint_target: float = 22.0
    cooldown_s: float = 60.0
    target_uns_topic: str = (
        "uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature"
    )
    log_level: str = "INFO"
```

- [ ] **Step 6: Create __init__.py and __main__.py**

`src/decision_agent_stub/__init__.py` — empty.

```python
# src/decision_agent_stub/__main__.py
"""Entry point for the decision agent stub pod."""

from __future__ import annotations

import asyncio

from decision_agent_stub.config import DecisionAgentSettings
from decision_agent_stub.service import run


def main() -> None:
    asyncio.run(run(DecisionAgentSettings()))


if __name__ == "__main__":  # pragma: no cover
    main()
```

- [ ] **Step 7: Create Dockerfile**

```dockerfile
# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS builder
WORKDIR /workspace
RUN pip install --no-cache-dir uv==0.4.20
COPY pyproject.toml uv.lock /workspace/
COPY libs /workspace/libs
COPY services/decision-agent-stub /workspace/services/decision-agent-stub
RUN uv sync --frozen --no-dev --package decision-agent-stub

FROM python:3.12-slim AS runtime
WORKDIR /workspace
COPY --from=builder /workspace/.venv /workspace/.venv
COPY --from=builder /workspace/libs /workspace/libs
COPY --from=builder /workspace/services/decision-agent-stub/src \
     /workspace/services/decision-agent-stub/src
ENV PATH="/workspace/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1
USER nobody:nogroup
EXPOSE 8080
ENTRYPOINT ["/workspace/.venv/bin/python", "-m", "decision_agent_stub"]
```

- [ ] **Step 8: Sync workspace and run all unit tests**

```bash
uv sync
uv run pytest services/decision-agent-stub/tests/ -v
```

Expected: 4 tests PASS.

- [ ] **Step 9: Run full unit test suite to confirm nothing broken**

```bash
uv run pytest --ignore=tests/e2e -v
```

Expected: all tests PASS.

- [ ] **Step 10: Commit**

```bash
git add services/decision-agent-stub/
git commit -m "feat(decision-stub): threshold-triggered actuation request emitter"
```

---

## Task 11: k3s manifests

**Files:**
- Create: `deploy/k3s/base/amqp-actuation-event-subscriber/{deployment,service,kustomization}.yaml`
- Create: `deploy/k3s/base/actuation-control-orchestrator/{deployment,service,kustomization}.yaml`
- Create: `deploy/k3s/base/actuation-event-validator/{deployment,service,kustomization}.yaml`
- Create: `deploy/k3s/base/actuation-signal-publisher/{deployment,service,kustomization}.yaml`
- Create: `deploy/k3s/base/decision-agent-stub/{deployment,service,kustomization}.yaml`
- Modify: `deploy/k3s/base/kustomization.yaml`

### amqp-actuation-event-subscriber

- [ ] **Step 1: Create deployment.yaml**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: amqp-actuation-event-subscriber
  labels: { app.kubernetes.io/name: amqp-actuation-event-subscriber }
spec:
  replicas: 1
  selector:
    matchLabels: { app.kubernetes.io/name: amqp-actuation-event-subscriber }
  template:
    metadata:
      labels: { app.kubernetes.io/name: amqp-actuation-event-subscriber }
      annotations:
        eirvah.uniza/scrape: "true"
        eirvah.uniza/scrape-port: "8080"
    spec:
      containers:
        - name: amqp-actuation-event-subscriber
          image: amqp-actuation-event-subscriber:local
          imagePullPolicy: IfNotPresent
          env:
            - name: AMQP_ACTUATION_EVENT_SUBSCRIBER_NATS_SERVERS
              value: '["nats://nats:4222"]'
            - name: AMQP_ACTUATION_EVENT_SUBSCRIBER_AMQP_URL
              value: "amqp://eirvah:eirvah-dev-password@rabbitmq:5672/"
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

- [ ] **Step 2: Create service.yaml**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: amqp-actuation-event-subscriber
  labels: { app.kubernetes.io/name: amqp-actuation-event-subscriber }
spec:
  selector: { app.kubernetes.io/name: amqp-actuation-event-subscriber }
  ports:
    - { name: http, port: 8080, targetPort: 8080 }
```

- [ ] **Step 3: Create kustomization.yaml**

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - deployment.yaml
  - service.yaml
```

### actuation-control-orchestrator

- [ ] **Step 4: Create deployment.yaml**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: actuation-control-orchestrator
  labels: { app.kubernetes.io/name: actuation-control-orchestrator }
spec:
  replicas: 1
  selector:
    matchLabels: { app.kubernetes.io/name: actuation-control-orchestrator }
  template:
    metadata:
      labels: { app.kubernetes.io/name: actuation-control-orchestrator }
      annotations:
        eirvah.uniza/scrape: "true"
        eirvah.uniza/scrape-port: "8080"
    spec:
      containers:
        - name: actuation-control-orchestrator
          image: actuation-control-orchestrator:local
          imagePullPolicy: IfNotPresent
          env:
            - name: ACTUATION_CONTROL_ORCHESTRATOR_NATS_SERVERS
              value: '["nats://nats:4222"]'
            - name: ACTUATION_CONTROL_ORCHESTRATOR_AMQP_URL
              value: "amqp://eirvah:eirvah-dev-password@rabbitmq:5672/"
            - name: ACTUATION_CONTROL_ORCHESTRATOR_ALLOW_WRITES
              value: "false"
          ports:
            - { name: http, containerPort: 8080 }
          volumeMounts:
            - name: config
              mountPath: /etc/actuation-control-orchestrator
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
            name: actuation-control-orchestrator-config
```

- [ ] **Step 5: Create service.yaml**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: actuation-control-orchestrator
  labels: { app.kubernetes.io/name: actuation-control-orchestrator }
spec:
  selector: { app.kubernetes.io/name: actuation-control-orchestrator }
  ports:
    - { name: http, port: 8080, targetPort: 8080 }
```

- [ ] **Step 6: Create kustomization.yaml**

Copy `config/pipelines/actuation-control.yaml` into `deploy/k3s/base/actuation-control-orchestrator/actuation-control.yaml`, then:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - deployment.yaml
  - service.yaml
configMapGenerator:
  - name: actuation-control-orchestrator-config
    files:
      - actuation-control.yaml
```

Also copy `config/pipelines/actuation-control.yaml` to `deploy/k3s/base/actuation-control-orchestrator/actuation-control.yaml`.

### actuation-event-validator

- [ ] **Step 7: Create deployment.yaml**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: actuation-event-validator
  labels: { app.kubernetes.io/name: actuation-event-validator }
spec:
  replicas: 1
  selector:
    matchLabels: { app.kubernetes.io/name: actuation-event-validator }
  template:
    metadata:
      labels: { app.kubernetes.io/name: actuation-event-validator }
      annotations:
        eirvah.uniza/scrape: "true"
        eirvah.uniza/scrape-port: "8080"
    spec:
      containers:
        - name: actuation-event-validator
          image: actuation-event-validator:local
          imagePullPolicy: IfNotPresent
          env:
            - name: ACTUATION_EVENT_VALIDATOR_NATS_SERVERS
              value: '["nats://nats:4222"]'
          ports:
            - { name: http, containerPort: 8080 }
          volumeMounts:
            - name: config
              mountPath: /etc/actuation-event-validator
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
            name: actuation-event-validator-config
```

- [ ] **Step 8: Create service.yaml and kustomization.yaml for validator**

```yaml
# service.yaml
apiVersion: v1
kind: Service
metadata:
  name: actuation-event-validator
  labels: { app.kubernetes.io/name: actuation-event-validator }
spec:
  selector: { app.kubernetes.io/name: actuation-event-validator }
  ports:
    - { name: http, port: 8080, targetPort: 8080 }
```

Copy `config/actuation-policy.yaml` to `deploy/k3s/base/actuation-event-validator/actuation-policy.yaml`, then:

```yaml
# kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - deployment.yaml
  - service.yaml
configMapGenerator:
  - name: actuation-event-validator-config
    files:
      - actuation-policy.yaml
```

### actuation-signal-publisher

- [ ] **Step 9: Create deployment.yaml**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: actuation-signal-publisher
  labels: { app.kubernetes.io/name: actuation-signal-publisher }
spec:
  replicas: 1
  selector:
    matchLabels: { app.kubernetes.io/name: actuation-signal-publisher }
  template:
    metadata:
      labels: { app.kubernetes.io/name: actuation-signal-publisher }
      annotations:
        eirvah.uniza/scrape: "true"
        eirvah.uniza/scrape-port: "8080"
    spec:
      containers:
        - name: actuation-signal-publisher
          image: actuation-signal-publisher:local
          imagePullPolicy: IfNotPresent
          env:
            - name: ACTUATION_SIGNAL_PUBLISHER_NATS_SERVERS
              value: '["nats://nats:4222"]'
            - name: ACTUATION_SIGNAL_PUBLISHER_ENTERPRISE
              value: uniza
            - name: ACTUATION_SIGNAL_PUBLISHER_SITE
              value: zilina
          ports:
            - { name: http, containerPort: 8080 }
          volumeMounts:
            - name: config
              mountPath: /etc/actuation-signal-publisher
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
            name: actuation-signal-publisher-config
```

- [ ] **Step 10: Create service.yaml and kustomization.yaml for signal publisher**

```yaml
# service.yaml
apiVersion: v1
kind: Service
metadata:
  name: actuation-signal-publisher
  labels: { app.kubernetes.io/name: actuation-signal-publisher }
spec:
  selector: { app.kubernetes.io/name: actuation-signal-publisher }
  ports:
    - { name: http, port: 8080, targetPort: 8080 }
```

Copy `config/opcua-node-to-uns-mapping.yaml` and `config/opcua-node-list.yaml` to `deploy/k3s/base/actuation-signal-publisher/`, then:

```yaml
# kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - deployment.yaml
  - service.yaml
configMapGenerator:
  - name: actuation-signal-publisher-config
    files:
      - opcua-node-to-uns-mapping.yaml
      - opcua-node-list.yaml
```

### decision-agent-stub

- [ ] **Step 11: Create deployment.yaml**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: decision-agent-stub
  labels: { app.kubernetes.io/name: decision-agent-stub }
spec:
  replicas: 1
  selector:
    matchLabels: { app.kubernetes.io/name: decision-agent-stub }
  template:
    metadata:
      labels: { app.kubernetes.io/name: decision-agent-stub }
    spec:
      containers:
        - name: decision-agent-stub
          image: decision-agent-stub:local
          imagePullPolicy: IfNotPresent
          env:
            - name: DECISION_AGENT_STUB_MQTT_HOST
              value: mosquitto
            - name: DECISION_AGENT_STUB_MQTT_USERNAME
              value: eirvah
            - name: DECISION_AGENT_STUB_MQTT_PASSWORD
              value: eirvah-dev-password
            - name: DECISION_AGENT_STUB_AMQP_URL
              value: "amqp://eirvah:eirvah-dev-password@rabbitmq:5672/"
          resources:
            requests: { cpu: "10m", memory: "64Mi" }
            limits:   { cpu: "100m", memory: "128Mi" }
```

- [ ] **Step 12: Create service.yaml and kustomization.yaml for stub**

```yaml
# service.yaml
apiVersion: v1
kind: Service
metadata:
  name: decision-agent-stub
  labels: { app.kubernetes.io/name: decision-agent-stub }
spec:
  selector: { app.kubernetes.io/name: decision-agent-stub }
  ports:
    - { name: http, port: 8080, targetPort: 8080 }
```

```yaml
# kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - deployment.yaml
  - service.yaml
```

### Update base kustomization.yaml

- [ ] **Step 13: Add 5 new dirs to deploy/k3s/base/kustomization.yaml**

Replace the `resources:` block:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: eirvah-edge
resources:
  - namespace.yaml
  - nats
  - mosquitto
  - rabbitmq
  - prometheus
  - grafana
  - opcua-simulator
  - opcua-data-subscriber
  - data-converter
  - uns-auto-contextualizer
  - mqtt-uns-publisher
  - uns-contextualizer-orchestrator
  - amqp-actuation-event-subscriber
  - actuation-control-orchestrator
  - actuation-event-validator
  - actuation-signal-publisher
  - decision-agent-stub
labels:
  - includeSelectors: true
    pairs:
      eirvah.uniza/component: edge
```

- [ ] **Step 14: Validate kustomize**

```bash
kubectl kustomize deploy/k3s/overlays/local
```

Expected: outputs full manifest with no errors.

- [ ] **Step 15: Commit**

```bash
git add deploy/k3s/
git commit -m "feat(k3s): actuation path manifests for 5 new pods"
```

---

## Task 12: Build images and deploy

- [ ] **Step 1: Update uv.lock**

```bash
uv sync
```

Expected: resolves all 5 new packages, updates `uv.lock`.

- [ ] **Step 2: Build all service images**

```bash
./scripts/build_all.sh local
```

Expected: 11 images built successfully. Each `==> building <svc>:local` line appears with no error exit.

- [ ] **Step 3: Import images into k3d**

```bash
for svc in amqp-actuation-event-subscriber actuation-control-orchestrator \
            actuation-event-validator actuation-signal-publisher decision-agent-stub; do
  k3d image import "${svc}:local" -c eirvah-local
done
```

Expected: each image imported with `INFO[...] Successfully imported image`.

- [ ] **Step 4: Apply manifests**

```bash
kubectl apply -k deploy/k3s/overlays/local
```

Expected: 5 new deployments created/configured.

- [ ] **Step 5: Verify pods running**

```bash
kubectl -n eirvah-edge get pods --watch
```

Wait until all 5 new pods show `Running 1/1`. Expected output includes:
```
amqp-actuation-event-subscriber-...   1/1   Running
actuation-control-orchestrator-...    1/1   Running
actuation-event-validator-...         1/1   Running
actuation-signal-publisher-...        1/1   Running
decision-agent-stub-...               1/1   Running
```

- [ ] **Step 6: Commit lock file**

```bash
git add uv.lock
git commit -m "chore: update uv.lock for 5 new actuation services"
```

---

## Task 13: Run actuation e2e tests

The first three tests run against the default cluster (`ALLOW_WRITES=false`). `test_actuation_full_loop` requires patching the orchestrator.

- [ ] **Step 1: Run writes-disabled and deadline tests (pass without allow_writes)**

```bash
uv run pytest tests/e2e/test_actuation.py \
  -k "writes_disabled or deadline_expired" -v
```

Expected: 2 tests PASS.

- [ ] **Step 2: Run policy rejection test**

```bash
uv run pytest tests/e2e/test_actuation.py::test_actuation_rejection_policy -v
```

Expected: 1 test PASS (value 99.0 → policy reject, regardless of allow_writes).

- [ ] **Step 3: Enable writes for full-loop test**

```bash
kubectl -n eirvah-edge set env deployment/actuation-control-orchestrator \
  ACTUATION_CONTROL_ORCHESTRATOR_ALLOW_WRITES=true
kubectl -n eirvah-edge rollout status deployment/actuation-control-orchestrator
```

- [ ] **Step 4: Run full-loop test**

```bash
uv run pytest tests/e2e/test_actuation.py::test_actuation_full_loop -v
```

Expected: PASS. Setpoint node in simulator updated to 22.0.

- [ ] **Step 5: Restore allow_writes to false**

```bash
kubectl -n eirvah-edge set env deployment/actuation-control-orchestrator \
  ACTUATION_CONTROL_ORCHESTRATOR_ALLOW_WRITES=false
kubectl -n eirvah-edge rollout status deployment/actuation-control-orchestrator
```

- [ ] **Step 6: Run full e2e suite including telemetry tests**

```bash
uv run pytest tests/e2e/ -v
```

Expected: all 6 tests PASS (2 telemetry + 4 actuation).

---

## Task 14: Grafana actuation dashboard panel

**Files:**
- Modify: `deploy/grafana/dashboards/eirvah-edge-pipeline.json`

- [ ] **Step 1: Add actuation row to dashboard JSON**

In `deploy/grafana/dashboards/eirvah-edge-pipeline.json`, append a new row panel and three stat/graph panels after the existing panels array entries. The JSON fragment to insert before the closing `]` of the `panels` array:

```json
,
{
  "type": "row",
  "title": "Actuation Path",
  "gridPos": {"h": 1, "w": 24, "x": 0, "y": 20},
  "id": 20,
  "collapsed": false
},
{
  "type": "stat",
  "title": "Actuation Approved",
  "gridPos": {"h": 4, "w": 6, "x": 0, "y": 21},
  "id": 21,
  "targets": [{
    "expr": "sum(increase(actuation_approved_total{path=\"actuation\"}[5m]))",
    "legendFormat": "approved"
  }],
  "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value", "graphMode": "area"}
},
{
  "type": "stat",
  "title": "Actuation Rejected",
  "gridPos": {"h": 4, "w": 6, "x": 6, "y": 21},
  "id": 22,
  "targets": [{
    "expr": "sum by (reason)(increase(actuation_rejected_total{path=\"actuation\"}[5m]))",
    "legendFormat": "{{ reason }}"
  }],
  "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value"}
},
{
  "type": "timeseries",
  "title": "Actuation E2E Latency (p50/p95)",
  "gridPos": {"h": 6, "w": 12, "x": 12, "y": 21},
  "id": 23,
  "targets": [
    {
      "expr": "histogram_quantile(0.50, rate(actuation_e2e_latency_seconds_bucket{path=\"actuation\"}[5m]))",
      "legendFormat": "p50"
    },
    {
      "expr": "histogram_quantile(0.95, rate(actuation_e2e_latency_seconds_bucket{path=\"actuation\"}[5m]))",
      "legendFormat": "p95"
    }
  ]
}
```

- [ ] **Step 2: Reload dashboard in Grafana**

The dashboard ConfigMap is auto-reloaded by the Grafana sidecar. Apply the updated manifest:

```bash
kubectl apply -k deploy/k3s/overlays/local
```

Wait 30 s, then open Grafana at `http://localhost:3000` (port-forward already running). Navigate to "EirVah Edge Pipeline" dashboard. Confirm "Actuation Path" row appears.

- [ ] **Step 3: Commit**

```bash
git add deploy/grafana/dashboards/eirvah-edge-pipeline.json
git commit -m "feat(grafana): actuation path panel row — approved/rejected/latency"
```

---

## Task 15: ADR 0001 — Actuation Safety Gate

**Files:**
- Create: `docs/adr/0001-actuation-safety-gate.md`

- [ ] **Step 1: Create the ADR**

```markdown
# ADR 0001 — Actuation Safety Gate

**Date:** 2026-05-18
**Status:** Accepted

## Context

The actuation path writes values back to a physical device (OPC UA setpoint). CPS writes carry physical risk: an unintended setpoint change in a real bottling line can damage equipment or product. The slice must run safely on a developer laptop, in CI, and in a shared lab environment without any risk of unintended writes.

## Decision

Implement a two-layer safety gate:

1. **`ALLOW_WRITES` feature flag** (default `false`) on `actuation-control-orchestrator`. When `false`, the pipeline validates the request but short-circuits before calling `act.work.write_signal`, emitting a rejection with reason `writes_disabled`. No OPC UA write occurs regardless of what the validator decides.

2. **`actuation-event-validator` policy bounds check** (value range + allowlist). Runs regardless of the flag. Prevents unsafe writes even when the flag is enabled.

Neither layer alone is sufficient:
- The flag prevents writes in safe environments but would bypass policy checks if the only gate.
- The validator enforces policy but has no awareness of environment safety.

Together: `allow_writes=true` is an explicit, deliberate act that still cannot bypass policy.

## Consequences

- Full-loop e2e tests require `ALLOW_WRITES=true`. CI defaults to `false`.
- Lab and production k3s overlays set the flag explicitly.
- The safety default means "run everything, prove validation works, but don't touch hardware" — appropriate for a slice whose primary purpose is architectural demonstration.
```

- [ ] **Step 2: Commit**

```bash
git add docs/adr/0001-actuation-safety-gate.md
git commit -m "docs(adr): 0001 — actuation safety gate (allow_writes + policy)"
```

---

## Task 16: ADR 0002 — Reverse Mapping via Shared ConfigMap

**Files:**
- Create: `docs/adr/0002-reverse-mapping-shared-configmap.md`

- [ ] **Step 1: Create the ADR**

```markdown
# ADR 0002 — Reverse Mapping via Shared ConfigMap

**Date:** 2026-05-18
**Status:** Accepted

## Context

`actuation-signal-publisher` must resolve a UNS topic (e.g., `uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature`) to an OPC UA browse path for the write operation. Two options were considered:

**Option A:** Maintain a separate reverse mapping file (`uns-to-opcua-mapping.yaml`) alongside the existing `opcua-node-to-uns-mapping.yaml`.

**Option B:** Invert `opcua-node-to-uns-mapping.yaml` at runtime and load `opcua-node-list.yaml` for browse paths. Both files already exist and are used by the telemetry path.

## Decision

Option B — invert at runtime.

Reasons:
- Single source of truth. A separate reverse file would drift from the forward mapping as nodes are added.
- `opcua-node-to-uns-mapping.yaml` already defines the canonical relationship; the reverse is derived, not independently authoritative.
- `opcua-node-list.yaml` already contains browse paths; duplicating them would create a third file that also drifts.

## Constraints imposed

The mapping must be **bijective**: each UNS topic must map to exactly one `node_id` alias and each alias to exactly one UNS topic. `actuation-signal-publisher` enforces this at startup with a `ValueError` and fails fast if duplicates are found.

For the bottling-line slice this is a safe constraint — one physical line, no repeated measurements. If a future protocol adapter introduces duplicate UNS topics (e.g., redundant sensors), a dedicated reverse mapping file or a tiebreaker field should be introduced.

## Consequences

- `actuation-signal-publisher` mounts two ConfigMaps: `opcua-node-to-uns-mapping.yaml` and `opcua-node-list.yaml`.
- Adding a new writable node requires updating only `opcua-node-to-uns-mapping.yaml` and `opcua-node-list.yaml` — no additional file.
- Startup failure on non-bijective mapping is intentional; it surfaces misconfiguration early.
```

- [ ] **Step 2: Commit**

```bash
git add docs/adr/0002-reverse-mapping-shared-configmap.md
git commit -m "docs(adr): 0002 — reverse mapping via shared ConfigMap"
```

---

## Final verification

- [ ] **Run full unit test suite**

```bash
uv run pytest --ignore=tests/e2e -v
```

Expected: all tests PASS.

- [ ] **Run full e2e suite (cluster must be running)**

```bash
uv run pytest tests/e2e/ -v
```

Expected: 6 tests PASS (2 telemetry + 4 actuation).

- [ ] **Verify all 16 pods Running**

```bash
kubectl -n eirvah-edge get pods
```

Expected: 16 pods, all `1/1 Running`.
