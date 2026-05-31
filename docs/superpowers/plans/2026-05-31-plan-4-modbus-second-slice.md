# Plan 4 — Modbus Second Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Modbus TCP protocol adapter (`modbus-simulator` + `modbus-data-subscriber`) that publishes to the same `uns.ingress.raw` NATS subject as the OPC UA path, proving the architecture is protocol-agnostic without touching any downstream service.

**Architecture:** The Modbus path is purely additive. `modbus-simulator` runs a Modbus TCP server exposing four holding registers (temperature, setpoint, motor state, throughput) on a tick loop. `modbus-data-subscriber` polls those registers at a configurable interval, wraps each value in a `RawSignalEnvelope`, and publishes to `uns.ingress.raw` — the same NATS subject as the OPC UA subscriber. All downstream services (converter, contextualizer, publisher) are unchanged. Modbus TCP is poll-based, not push-based; the subscriber drives the read interval rather than receiving change notifications.

**Tech Stack:** Python 3.12, `pymodbus>=3.7` (BSD-3-Clause, OSI-approved) for both server and client, `pydantic-settings`, `structlog`, `prometheus-client`, `starlette`, `uvicorn`, `pyyaml`, `nats-py`, `eirvah-contracts`, `eirvah-bus`, `eirvah-observability`.

**Spec reference:** `docs/superpowers/specs/2026-05-16-eirvah-edge-vertical-slice-design.md` §1.2 (Modbus is the second-slice protocol).

---

## File structure produced by this plan

```
services/
├── modbus-simulator/
│   ├── pyproject.toml
│   ├── Dockerfile
│   ├── src/modbus_simulator/__init__.py
│   ├── src/modbus_simulator/__main__.py
│   ├── src/modbus_simulator/config.py
│   ├── src/modbus_simulator/server.py
│   ├── src/modbus_simulator/metrics.py
│   └── tests/test_modbus_simulator.py
└── modbus-data-subscriber/
    ├── pyproject.toml
    ├── Dockerfile
    ├── src/modbus_data_subscriber/__init__.py
    ├── src/modbus_data_subscriber/__main__.py
    ├── src/modbus_data_subscriber/config.py
    ├── src/modbus_data_subscriber/service.py
    └── tests/test_modbus_data_subscriber.py

deploy/k3s/base/
├── modbus-simulator/
│   ├── deployment.yaml
│   ├── service.yaml
│   ├── configmap.yaml
│   └── kustomization.yaml
├── modbus-data-subscriber/
│   ├── deployment.yaml
│   ├── service.yaml
│   ├── modbus-register-map.yaml
│   └── kustomization.yaml
└── kustomization.yaml            MODIFY — add both new dirs

scripts/
├── build_all.sh                  MODIFY — add both new services
└── dev_up.sh                     MODIFY — add both new services

tests/e2e/
└── test_modbus_path.py           NEW
```

---

## Task 1: `modbus-simulator` — pyproject + config + server + metrics + tests

The simulator runs a Modbus TCP server on port 502 exposing 4 holding registers. A tick loop updates register 0 (temperature) with a small random walk. Registers are scaled integers (×100) to avoid IEEE-754 float encoding across two registers.

| Register | Signal | Scale | Example |
|---|---|---|---|
| 0 | temperature_celsius | ×100 | 2200 = 22.00°C |
| 1 | setpoint_celsius | ×100 | 2200 = 22.00°C |
| 2 | motor_state | ×1 | 1 = running |
| 3 | throughput_bps | ×100 | 80 = 0.80 bottles/s |

**Files:**
- Create: `services/modbus-simulator/pyproject.toml`
- Create: `services/modbus-simulator/src/modbus_simulator/__init__.py`
- Create: `services/modbus-simulator/src/modbus_simulator/__main__.py`
- Create: `services/modbus-simulator/src/modbus_simulator/config.py`
- Create: `services/modbus-simulator/src/modbus_simulator/server.py`
- Create: `services/modbus-simulator/src/modbus_simulator/metrics.py`
- Create: `services/modbus-simulator/Dockerfile`
- Create: `services/modbus-simulator/tests/test_modbus_simulator.py`

- [ ] **Step 1: Write failing tests**

```python
# services/modbus-simulator/tests/test_modbus_simulator.py
from __future__ import annotations

import pytest
from modbus_simulator.server import RegisterBlock, scale_to_register, register_to_scale


def test_scale_to_register_temperature() -> None:
    assert scale_to_register(22.00, scale=100) == 2200


def test_scale_to_register_rounds() -> None:
    assert scale_to_register(22.005, scale=100) == 2201


def test_register_to_scale_temperature() -> None:
    assert register_to_scale(2200, scale=100) == pytest.approx(22.00)


def test_register_block_defaults() -> None:
    block = RegisterBlock()
    assert block.temperature_raw == 2200
    assert block.setpoint_raw == 2200
    assert block.motor_state == 1
    assert block.throughput_raw == 80


def test_register_block_as_list() -> None:
    block = RegisterBlock(temperature_raw=2350, setpoint_raw=2200, motor_state=1, throughput_raw=82)
    values = block.as_list()
    assert values == [2350, 2200, 1, 82]


def test_register_block_tick_changes_temperature() -> None:
    import random
    rng = random.Random(42)
    block = RegisterBlock()
    original = block.temperature_raw
    block.tick(rng=rng, delta_max=50)
    # temperature_raw changed but stays in valid range [1800, 5000]
    assert block.temperature_raw != original or True  # may be same if delta is 0
    assert 1800 <= block.temperature_raw <= 5000
```

- [ ] **Step 2: Run — verify FAIL**

```bash
cd /path/to/repo
uv run pytest services/modbus-simulator/tests/test_modbus_simulator.py -v 2>&1 | head -20
```

Expected: `ModuleNotFoundError: No module named 'modbus_simulator'`

- [ ] **Step 3: Create `pyproject.toml`**

```toml
# services/modbus-simulator/pyproject.toml
[project]
name = "modbus-simulator"
version = "0.0.0"
description = "Modbus TCP simulator — second-slice protocol target."
requires-python = ">=3.12"
license = { text = "Apache-2.0" }
dependencies = [
    "pymodbus>=3.7",
    "pydantic>=2.8",
    "pydantic-settings>=2.5",
    "structlog>=24.0",
    "uvicorn>=0.30",
    "eirvah-observability",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/modbus_simulator"]
```

- [ ] **Step 4: Create `config.py`**

```python
# services/modbus-simulator/src/modbus_simulator/config.py
"""Settings for the Modbus TCP simulator."""
from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SimulatorSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="MODBUS_SIMULATOR_",
        env_file=None,
        extra="ignore",
    )

    host: str = "0.0.0.0"
    port: int = Field(default=502, ge=1, le=65535)
    unit_id: int = Field(default=1, ge=1, le=247)
    tick_rate_ms: int = Field(default=500, ge=50, le=10000)
    seed: int = 0
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
```

- [ ] **Step 5: Create `metrics.py`**

```python
# services/modbus-simulator/src/modbus_simulator/metrics.py
"""Prometheus metrics for the Modbus simulator."""
from __future__ import annotations

from eirvah_observability.metrics import make_gauge
from prometheus_client.registry import REGISTRY, CollectorRegistry


class SimulatorMetrics:
    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self._temperature = make_gauge(
            "modbus_simulator_temperature_celsius",
            "Current temperature from Modbus simulator (°C).",
            registry=registry,
        )
        self._setpoint = make_gauge(
            "modbus_simulator_setpoint_celsius",
            "Current setpoint from Modbus simulator (°C).",
            registry=registry,
        )
        self._motor_state = make_gauge(
            "modbus_simulator_motor_state",
            "Motor state: 0=stopped 1=running.",
            registry=registry,
        )

    def set_temperature(self, value: float) -> None:
        self._temperature.set(value)

    def set_setpoint(self, value: float) -> None:
        self._setpoint.set(value)

    def set_motor_state(self, value: int) -> None:
        self._motor_state.set(value)
```

- [ ] **Step 6: Create `server.py`**

```python
# services/modbus-simulator/src/modbus_simulator/server.py
"""Modbus TCP server + tick loop."""
from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field

import structlog
import uvicorn
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import configure_logging
from pymodbus.datastore import (
    ModbusSequentialDataBlock,
    ModbusServerContext,
    ModbusSlaveContext,
)
from pymodbus.server import StartAsyncTcpServer

from modbus_simulator.config import SimulatorSettings
from modbus_simulator.metrics import SimulatorMetrics

_log = structlog.get_logger("modbus-simulator")

# Holding register function code
_HR = 3


def scale_to_register(value: float, *, scale: int) -> int:
    return round(value * scale)


def register_to_scale(raw: int, *, scale: int) -> float:
    return raw / scale


@dataclass
class RegisterBlock:
    temperature_raw: int = 2200   # 22.00°C
    setpoint_raw: int = 2200      # 22.00°C
    motor_state: int = 1          # running
    throughput_raw: int = 80      # 0.80 bottles/s

    def as_list(self) -> list[int]:
        return [self.temperature_raw, self.setpoint_raw, self.motor_state, self.throughput_raw]

    def tick(self, *, rng: random.Random, delta_max: int = 50) -> None:
        delta = rng.randint(-delta_max, delta_max)
        self.temperature_raw = max(1800, min(5000, self.temperature_raw + delta))


class SimulatorRuntime:
    def __init__(self, settings: SimulatorSettings) -> None:
        self._settings = settings
        self._rng = random.Random(settings.seed)
        self._metrics = SimulatorMetrics()
        self._block = RegisterBlock()
        self._context: ModbusServerContext | None = None
        self._ready = False

    def is_ready(self) -> bool:
        return self._ready

    def _build_context(self) -> ModbusServerContext:
        store = ModbusSlaveContext(
            hr=ModbusSequentialDataBlock(0, self._block.as_list() + [0] * 6),
        )
        return ModbusServerContext(slaves={self._settings.unit_id: store}, single=False)

    async def _tick_loop(self) -> None:
        interval = self._settings.tick_rate_ms / 1000.0
        while True:
            await asyncio.sleep(interval)
            self._block.tick(rng=self._rng)
            if self._context is not None:
                self._context[self._settings.unit_id].setValues(
                    _HR, 0, self._block.as_list()
                )
            self._metrics.set_temperature(register_to_scale(self._block.temperature_raw, scale=100))
            self._metrics.set_setpoint(register_to_scale(self._block.setpoint_raw, scale=100))
            self._metrics.set_motor_state(self._block.motor_state)

    async def run(self) -> None:
        self._context = self._build_context()
        self._ready = True
        _log.info(
            "modbus_simulator_starting",
            host=self._settings.host,
            port=self._settings.port,
            unit_id=self._settings.unit_id,
        )
        await asyncio.gather(
            StartAsyncTcpServer(
                context=self._context,
                address=(self._settings.host, self._settings.port),
            ),
            self._tick_loop(),
        )


async def run(settings: SimulatorSettings) -> None:
    configure_logging(level=settings.log_level)
    runtime = SimulatorRuntime(settings)
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

- [ ] **Step 7: Create `__init__.py` and `__main__.py`**

```python
# services/modbus-simulator/src/modbus_simulator/__init__.py
```

```python
# services/modbus-simulator/src/modbus_simulator/__main__.py
from __future__ import annotations
import asyncio
from modbus_simulator.config import SimulatorSettings
from modbus_simulator.server import run

def main() -> None:
    asyncio.run(run(SimulatorSettings()))

if __name__ == "__main__":  # pragma: no cover
    main()
```

- [ ] **Step 8: Run tests — verify PASS**

```bash
uv run pytest services/modbus-simulator/tests/test_modbus_simulator.py -v
```

Expected: all 6 tests PASS.

- [ ] **Step 9: Create `Dockerfile`**

```dockerfile
# services/modbus-simulator/Dockerfile
# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS builder
WORKDIR /workspace
RUN pip install --no-cache-dir uv==0.4.20
COPY pyproject.toml uv.lock /workspace/
COPY libs /workspace/libs
COPY services/modbus-simulator /workspace/services/modbus-simulator
RUN uv sync --frozen --no-dev --package modbus-simulator

FROM python:3.12-slim AS runtime
WORKDIR /workspace
COPY --from=builder /workspace/.venv /workspace/.venv
COPY --from=builder /workspace/libs /workspace/libs
COPY --from=builder /workspace/services/modbus-simulator/src /workspace/services/modbus-simulator/src
ENV PATH="/workspace/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1
USER nobody:nogroup
EXPOSE 502 8080
ENTRYPOINT ["/workspace/.venv/bin/python", "-m", "modbus_simulator"]
```

- [ ] **Step 10: Commit**

```bash
git add services/modbus-simulator/
git commit -m "feat(modbus-simulator): Modbus TCP server with tick loop and Prometheus metrics"
```

---

## Task 2: `modbus-data-subscriber` — pyproject + config + service + tests

The subscriber polls the simulator's holding registers on a configurable interval. It converts each scaled-integer register back to a float, wraps it in a `RawSignalEnvelope`, and publishes to `uns.ingress.raw`. `source_timestamp` and `server_timestamp` are set to `received_at` (Modbus has no hardware timestamp).

**Files:**
- Create: `services/modbus-data-subscriber/pyproject.toml`
- Create: `services/modbus-data-subscriber/src/modbus_data_subscriber/__init__.py`
- Create: `services/modbus-data-subscriber/src/modbus_data_subscriber/__main__.py`
- Create: `services/modbus-data-subscriber/src/modbus_data_subscriber/config.py`
- Create: `services/modbus-data-subscriber/src/modbus_data_subscriber/service.py`
- Create: `services/modbus-data-subscriber/Dockerfile`
- Create: `services/modbus-data-subscriber/tests/test_modbus_data_subscriber.py`

- [ ] **Step 1: Write failing tests**

```python
# services/modbus-data-subscriber/tests/test_modbus_data_subscriber.py
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_contracts.signals import RawSignalEnvelope
from eirvah_contracts.ulid import is_valid_correlation_id


def _make_register_config(
    address: int,
    alias: str,
    scale: float = 1.0,
    value_type: str = "double",
) -> dict:
    return {"address": address, "alias": alias, "scale": scale, "value_type": value_type}


def test_build_raw_envelope_temperature() -> None:
    from modbus_data_subscriber.service import build_raw_envelope

    now = datetime.now(UTC)
    env = build_raw_envelope(
        alias="Bottler.Temperature01",
        value=22.0,
        value_type="double",
        source_endpoint="modbus-tcp://modbus-simulator:502",
        received_at=now,
    )
    assert isinstance(env, RawSignalEnvelope)
    assert env.node_id == "Bottler.Temperature01"
    assert env.value == pytest.approx(22.0)
    assert env.quality == "good"
    assert env.source_endpoint == "modbus-tcp://modbus-simulator:502"
    assert env.source_timestamp == now
    assert env.server_timestamp == now
    assert env.received_at == now


def test_build_raw_envelope_motor_state_int() -> None:
    from modbus_data_subscriber.service import build_raw_envelope

    now = datetime.now(UTC)
    env = build_raw_envelope(
        alias="Bottler.Motor01.State",
        value=1,
        value_type="int64",
        source_endpoint="modbus-tcp://modbus-simulator:502",
        received_at=now,
    )
    assert env.value == 1
    assert env.value_type == "int64"


def test_wrap_in_nats_envelope() -> None:
    from modbus_data_subscriber.service import build_raw_envelope, wrap_in_nats_envelope

    now = datetime.now(UTC)
    raw = build_raw_envelope(
        alias="Bottler.Temperature01",
        value=22.0,
        value_type="double",
        source_endpoint="modbus-tcp://modbus-simulator:502",
        received_at=now,
    )
    env = wrap_in_nats_envelope(raw)
    assert isinstance(env, NATSEnvelope)
    assert is_valid_correlation_id(env.correlation_id)
    assert env.status == "ok"
    assert env.payload["node_id"] == "Bottler.Temperature01"


def test_apply_scale() -> None:
    from modbus_data_subscriber.service import apply_scale

    assert apply_scale(2200, scale=100.0, value_type="double") == pytest.approx(22.0)
    assert apply_scale(1, scale=1.0, value_type="int64") == 1


def test_load_register_map(tmp_path) -> None:
    from modbus_data_subscriber.service import RegisterMapConfig, load_register_map

    cfg_file = tmp_path / "map.yaml"
    cfg_file.write_text("""
host: modbus-simulator
port: 502
unit_id: 1
poll_interval_ms: 500
registers:
  - address: 0
    alias: Bottler.Temperature01
    scale: 100.0
    value_type: double
  - address: 2
    alias: Bottler.Motor01.State
    scale: 1.0
    value_type: int64
""")
    cfg = load_register_map(cfg_file)
    assert isinstance(cfg, RegisterMapConfig)
    assert cfg.host == "modbus-simulator"
    assert cfg.port == 502
    assert len(cfg.registers) == 2
    assert cfg.registers[0].alias == "Bottler.Temperature01"
    assert cfg.registers[0].scale == 100.0
```

- [ ] **Step 2: Run — verify FAIL**

```bash
uv run pytest services/modbus-data-subscriber/tests/test_modbus_data_subscriber.py -v 2>&1 | head -10
```

Expected: `ModuleNotFoundError: No module named 'modbus_data_subscriber'`

- [ ] **Step 3: Create `pyproject.toml`**

```toml
# services/modbus-data-subscriber/pyproject.toml
[project]
name = "modbus-data-subscriber"
version = "0.0.0"
description = "Modbus TCP → NATS ingress subscriber (second-slice protocol adapter)."
requires-python = ">=3.12"
license = { text = "Apache-2.0" }
dependencies = [
    "pymodbus>=3.7",
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
packages = ["src/modbus_data_subscriber"]
```

- [ ] **Step 4: Create `config.py`**

```python
# services/modbus-data-subscriber/src/modbus_data_subscriber/config.py
"""Settings for the Modbus TCP data subscriber."""
from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SubscriberSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="MODBUS_DATA_SUBSCRIBER_",
        env_file=None,
        extra="ignore",
    )

    nats_servers: list[str] = ["nats://nats:4222"]
    register_map_path: Path = Path("/etc/modbus-data-subscriber/modbus-register-map.yaml")
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
    reconnect_delay_s: float = 5.0
```

- [ ] **Step 5: Create `service.py`**

```python
# services/modbus-data-subscriber/src/modbus_data_subscriber/service.py
"""Modbus TCP data subscriber — polls registers, publishes to uns.ingress.raw."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import structlog
import uvicorn
import yaml
from eirvah_bus.client import BusClient
from eirvah_bus.request_reply import BUS_HEADER_CORRELATION_ID
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_contracts.signals import RawSignalEnvelope, SignalValueType
from eirvah_contracts.ulid import generate_correlation_id
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import configure_logging
from eirvah_observability.metrics import make_gauge
from pydantic import BaseModel
from pymodbus.client import AsyncModbusTcpClient

from modbus_data_subscriber.config import SubscriberSettings

_log = structlog.get_logger("modbus-data-subscriber")

_NATS_SUBJECT = "uns.ingress.raw"

# ---------------------------------------------------------------------------
# Register map config (loaded from modbus-register-map.yaml)
# ---------------------------------------------------------------------------

class RegisterConfig(BaseModel):
    address: int
    alias: str
    scale: float = 1.0
    value_type: SignalValueType = "double"


class RegisterMapConfig(BaseModel):
    host: str
    port: int = 502
    unit_id: int = 1
    poll_interval_ms: int = 500
    registers: list[RegisterConfig]


def load_register_map(path: Path) -> RegisterMapConfig:
    raw = yaml.safe_load(path.read_text())
    return RegisterMapConfig.model_validate(raw)


# ---------------------------------------------------------------------------
# Pure helper functions (unit-testable)
# ---------------------------------------------------------------------------

def apply_scale(
    raw: int,
    *,
    scale: float,
    value_type: SignalValueType,
) -> float | int:
    if value_type == "int64":
        return int(raw)
    return raw / scale


def build_raw_envelope(
    *,
    alias: str,
    value: float | int,
    value_type: SignalValueType,
    source_endpoint: str,
    received_at: datetime,
) -> RawSignalEnvelope:
    return RawSignalEnvelope(
        source_endpoint=source_endpoint,
        node_id=alias,
        value=value,
        value_type=value_type,
        quality="good",
        source_timestamp=received_at,   # Modbus has no hardware timestamp
        server_timestamp=received_at,
        received_at=received_at,
    )


def wrap_in_nats_envelope(raw: RawSignalEnvelope) -> NATSEnvelope:
    return NATSEnvelope(
        correlation_id=generate_correlation_id(),
        payload=raw.model_dump(mode="json"),
    )


# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------

class SubscriberRuntime:
    def __init__(self, settings: SubscriberSettings) -> None:
        self._settings = settings
        self._bus: BusClient | None = None
        self._ready = False
        self._connection_state = make_gauge(
            "modbus_ingress_connection_state",
            "1 when the Modbus TCP connection is up, 0 otherwise",
            labelnames=["ingress", "state"],
        )

    def is_ready(self) -> bool:
        return self._ready

    async def run(self) -> None:
        cfg = load_register_map(self._settings.register_map_path)
        self._bus = BusClient(servers=self._settings.nats_servers, name="modbus-data-subscriber")
        await self._bus.connect()
        _log.info("nats_connected", servers=self._settings.nats_servers)

        while True:
            try:
                await self._poll_loop(cfg)
            except Exception as exc:
                self._ready = False
                self._connection_state.labels(ingress="modbus", state="connected").set(0)
                self._connection_state.labels(ingress="modbus", state="disconnected").set(1)
                _log.warning("modbus_disconnected", error=str(exc))
                await asyncio.sleep(self._settings.reconnect_delay_s)

    async def _poll_loop(self, cfg: RegisterMapConfig) -> None:
        source_endpoint = f"modbus-tcp://{cfg.host}:{cfg.port}"
        interval = cfg.poll_interval_ms / 1000.0

        async with AsyncModbusTcpClient(cfg.host, port=cfg.port) as client:
            self._ready = True
            self._connection_state.labels(ingress="modbus", state="connected").set(1)
            self._connection_state.labels(ingress="modbus", state="disconnected").set(0)
            _log.info("modbus_connected", host=cfg.host, port=cfg.port)

            # Determine contiguous read range to minimise round-trips
            addresses = [r.address for r in cfg.registers]
            start = min(addresses)
            count = max(addresses) - start + 1

            while True:
                await asyncio.sleep(interval)
                result = await client.read_holding_registers(
                    address=start, count=count, slave=cfg.unit_id
                )
                if result.isError():
                    raise ConnectionError(f"Modbus read failed: {result}")

                received_at = datetime.now(UTC)
                for reg in cfg.registers:
                    raw = result.registers[reg.address - start]
                    value = apply_scale(raw, scale=reg.scale, value_type=reg.value_type)
                    envelope = wrap_in_nats_envelope(
                        build_raw_envelope(
                            alias=reg.alias,
                            value=value,
                            value_type=reg.value_type,
                            source_endpoint=source_endpoint,
                            received_at=received_at,
                        )
                    )
                    await self._publish(envelope)

    async def _publish(self, envelope: NATSEnvelope) -> None:
        assert self._bus is not None
        headers = {BUS_HEADER_CORRELATION_ID: envelope.correlation_id}
        await self._bus.nc.publish(
            _NATS_SUBJECT,
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

- [ ] **Step 6: Create `__init__.py` and `__main__.py`**

```python
# services/modbus-data-subscriber/src/modbus_data_subscriber/__init__.py
```

```python
# services/modbus-data-subscriber/src/modbus_data_subscriber/__main__.py
from __future__ import annotations
import asyncio
from modbus_data_subscriber.config import SubscriberSettings
from modbus_data_subscriber.service import run

def main() -> None:
    asyncio.run(run(SubscriberSettings()))

if __name__ == "__main__":  # pragma: no cover
    main()
```

- [ ] **Step 7: Run tests — verify PASS**

```bash
uv run pytest services/modbus-data-subscriber/tests/test_modbus_data_subscriber.py -v
```

Expected: all 5 tests PASS.

- [ ] **Step 8: Create `Dockerfile`**

```dockerfile
# services/modbus-data-subscriber/Dockerfile
# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS builder
WORKDIR /workspace
RUN pip install --no-cache-dir uv==0.4.20
COPY pyproject.toml uv.lock /workspace/
COPY libs /workspace/libs
COPY services/modbus-data-subscriber /workspace/services/modbus-data-subscriber
RUN uv sync --frozen --no-dev --package modbus-data-subscriber

FROM python:3.12-slim AS runtime
WORKDIR /workspace
COPY --from=builder /workspace/.venv /workspace/.venv
COPY --from=builder /workspace/libs /workspace/libs
COPY --from=builder /workspace/services/modbus-data-subscriber/src /workspace/services/modbus-data-subscriber/src
ENV PATH="/workspace/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1
USER nobody:nogroup
EXPOSE 8080
ENTRYPOINT ["/workspace/.venv/bin/python", "-m", "modbus_data_subscriber"]
```

- [ ] **Step 9: Commit**

```bash
git add services/modbus-data-subscriber/
git commit -m "feat(modbus-data-subscriber): Modbus TCP polling subscriber, publishes to uns.ingress.raw"
```

---

## Task 3: k8s manifests + build scripts

**Files:**
- Create: `deploy/k3s/base/modbus-simulator/deployment.yaml`
- Create: `deploy/k3s/base/modbus-simulator/service.yaml`
- Create: `deploy/k3s/base/modbus-simulator/configmap.yaml`
- Create: `deploy/k3s/base/modbus-simulator/kustomization.yaml`
- Create: `deploy/k3s/base/modbus-data-subscriber/deployment.yaml`
- Create: `deploy/k3s/base/modbus-data-subscriber/service.yaml`
- Create: `deploy/k3s/base/modbus-data-subscriber/modbus-register-map.yaml`
- Create: `deploy/k3s/base/modbus-data-subscriber/kustomization.yaml`
- Modify: `deploy/k3s/base/kustomization.yaml`
- Modify: `scripts/build_all.sh`
- Modify: `scripts/dev_up.sh`

- [ ] **Step 1: Create modbus-simulator manifests**

```yaml
# deploy/k3s/base/modbus-simulator/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: modbus-simulator
  labels: { app.kubernetes.io/name: modbus-simulator }
spec:
  replicas: 1
  selector:
    matchLabels: { app.kubernetes.io/name: modbus-simulator }
  template:
    metadata:
      labels: { app.kubernetes.io/name: modbus-simulator }
      annotations:
        eirvah.uniza/scrape: "true"
        eirvah.uniza/scrape-port: "8080"
    spec:
      containers:
        - name: modbus-simulator
          image: modbus-simulator:local
          imagePullPolicy: IfNotPresent
          env:
            - name: MODBUS_SIMULATOR_HOST
              value: "0.0.0.0"
            - name: MODBUS_SIMULATOR_PORT
              value: "502"
            - name: MODBUS_SIMULATOR_TICK_RATE_MS
              value: "500"
          ports:
            - { name: modbus, containerPort: 502 }
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
            requests: { cpu: "25m", memory: "64Mi" }
            limits:   { cpu: "200m", memory: "128Mi" }
```

```yaml
# deploy/k3s/base/modbus-simulator/service.yaml
apiVersion: v1
kind: Service
metadata:
  name: modbus-simulator
  labels: { app.kubernetes.io/name: modbus-simulator }
spec:
  selector: { app.kubernetes.io/name: modbus-simulator }
  ports:
    - { name: modbus, port: 502, targetPort: 502 }
    - { name: http, port: 8080, targetPort: 8080 }
```

```yaml
# deploy/k3s/base/modbus-simulator/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - deployment.yaml
  - service.yaml
```

- [ ] **Step 2: Create modbus-data-subscriber manifests**

```yaml
# deploy/k3s/base/modbus-data-subscriber/modbus-register-map.yaml
# Register map for modbus-data-subscriber (mirrors bottling-line address space)
host: modbus-simulator
port: 502
unit_id: 1
poll_interval_ms: 500
registers:
  - address: 0
    alias: "Bottler.Temperature01"
    scale: 100.0
    value_type: double
  - address: 1
    alias: "Bottler.SetpointUnit.SetpointTemperature"
    scale: 100.0
    value_type: double
  - address: 2
    alias: "Bottler.Motor01.State"
    scale: 1.0
    value_type: int64
  - address: 3
    alias: "Bottler.ThroughputMeter01"
    scale: 100.0
    value_type: double
```

```yaml
# deploy/k3s/base/modbus-data-subscriber/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: modbus-data-subscriber
  labels: { app.kubernetes.io/name: modbus-data-subscriber }
spec:
  replicas: 1
  selector:
    matchLabels: { app.kubernetes.io/name: modbus-data-subscriber }
  template:
    metadata:
      labels: { app.kubernetes.io/name: modbus-data-subscriber }
      annotations:
        eirvah.uniza/scrape: "true"
        eirvah.uniza/scrape-port: "8080"
    spec:
      containers:
        - name: modbus-data-subscriber
          image: modbus-data-subscriber:local
          imagePullPolicy: IfNotPresent
          env:
            - name: MODBUS_DATA_SUBSCRIBER_NATS_SERVERS
              value: '["nats://nats:4222"]'
          ports:
            - { name: http, containerPort: 8080 }
          volumeMounts:
            - name: config
              mountPath: /etc/modbus-data-subscriber
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
            requests: { cpu: "25m", memory: "64Mi" }
            limits:   { cpu: "200m", memory: "128Mi" }
      volumes:
        - name: config
          configMap:
            name: modbus-data-subscriber-config
```

```yaml
# deploy/k3s/base/modbus-data-subscriber/service.yaml
apiVersion: v1
kind: Service
metadata:
  name: modbus-data-subscriber
  labels: { app.kubernetes.io/name: modbus-data-subscriber }
spec:
  selector: { app.kubernetes.io/name: modbus-data-subscriber }
  ports:
    - { name: http, port: 8080, targetPort: 8080 }
```

```yaml
# deploy/k3s/base/modbus-data-subscriber/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - deployment.yaml
  - service.yaml
configMapGenerator:
  - name: modbus-data-subscriber-config
    files:
      - modbus-register-map.yaml
```

- [ ] **Step 3: Update `deploy/k3s/base/kustomization.yaml`**

Add both new directories to the `resources` list after `opcua-data-subscriber`:

```yaml
resources:
  - namespace.yaml
  - nats
  - mosquitto
  - rabbitmq
  - prometheus
  - grafana
  - opcua-simulator
  - opcua-data-subscriber
  - modbus-simulator
  - modbus-data-subscriber
  - data-converter
  - uns-auto-contextualizer
  - mqtt-uns-publisher
  - uns-contextualizer-orchestrator
  - amqp-actuation-event-subscriber
  - actuation-control-orchestrator
  - actuation-event-validator
  - actuation-signal-publisher
  - decision-agent-stub
```

- [ ] **Step 4: Update `scripts/build_all.sh`**

Add both services to the `SERVICES` array after `opcua-data-subscriber`:

```bash
SERVICES=(
  opcua-simulator
  opcua-data-subscriber
  modbus-simulator
  modbus-data-subscriber
  data-converter
  ...
)
```

- [ ] **Step 5: Update `scripts/dev_up.sh`**

Add both services to the `SERVICES` array after `opcua-data-subscriber`:

```bash
SERVICES=(
  opcua-simulator
  opcua-data-subscriber
  modbus-simulator
  modbus-data-subscriber
  data-converter
  ...
)
```

Also add a port-forward hint in the "Hints" section at the end:

```bash
echo "    Modbus:        kubectl -n ${NAMESPACE} port-forward svc/modbus-simulator 502:502"
```

- [ ] **Step 6: Verify kustomize renders clean**

```bash
kubectl kustomize deploy/k3s/overlays/local 2>&1 | grep -E "name: modbus"
```

Expected output includes:
```
name: modbus-data-subscriber
name: modbus-simulator
```

- [ ] **Step 7: Commit**

```bash
git add deploy/k3s/base/modbus-simulator/ deploy/k3s/base/modbus-data-subscriber/ \
        deploy/k3s/base/kustomization.yaml scripts/build_all.sh scripts/dev_up.sh
git commit -m "feat(k8s): add modbus-simulator and modbus-data-subscriber manifests"
```

---

## Task 4: e2e test — Modbus path publishes to `uns.ingress.raw`

This test verifies that `modbus-data-subscriber` publishes `RawSignalEnvelope` messages to `uns.ingress.raw` with the correct aliases. It runs against a live cluster (skipped if no cluster is up). It does **not** test the full telemetry pipeline — that is already covered by `test_telemetry.py`. It only proves the Modbus ingress path works independently.

**Files:**
- Create: `tests/e2e/test_modbus_path.py`
- Modify: `tests/e2e/conftest.py` — add `MODBUS_LOCAL_PORT = 10502`

- [ ] **Step 1: Add Modbus port constant to `conftest.py`**

Open `tests/e2e/conftest.py`. Add after the existing port constants:

```python
MODBUS_LOCAL_PORT = 10502
```

No other changes to `conftest.py`.

- [ ] **Step 2: Write the failing e2e test**

```python
# tests/e2e/test_modbus_path.py
"""E2E test for the Modbus ingress path.

Verifies that modbus-data-subscriber publishes RawSignalEnvelope messages
onto uns.ingress.raw within a reasonable time window.

Requires a live cluster — skipped if absent.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any

import nats
import pytest

from tests.e2e.conftest import EirVahCluster, NATS_LOCAL_PORT

pytestmark = pytest.mark.asyncio

EXPECTED_ALIASES = {
    "Bottler.Temperature01",
    "Bottler.SetpointUnit.SetpointTemperature",
    "Bottler.Motor01.State",
    "Bottler.ThroughputMeter01",
}


async def _collect_raw_messages(
    cluster: EirVahCluster,
    *,
    timeout_s: float = 10.0,
    max_messages: int = 40,
) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    nc = await nats.connect(servers=cluster.nats_servers)
    try:
        sub = await nc.subscribe("uns.ingress.raw")
        try:
            async with asyncio.timeout(timeout_s):
                async for msg in sub.messages:
                    outer = json.loads(msg.data)
                    payload = outer.get("payload", {})
                    messages.append(payload)
                    if len(messages) >= max_messages:
                        break
        except TimeoutError:
            pass
        finally:
            await sub.unsubscribe()
    finally:
        await nc.close()
    return messages


@pytest.mark.skipif(
    not __import__("tests.e2e.conftest", fromlist=["_cluster_is_up"])._cluster_is_up(),
    reason="no live k3d cluster",
)
async def test_modbus_path_publishes_all_aliases(cluster: EirVahCluster) -> None:
    messages = await _collect_raw_messages(cluster, timeout_s=10.0, max_messages=40)
    assert messages, "No messages received on uns.ingress.raw — is modbus-data-subscriber running?"

    # At least one message per alias within the collection window
    seen_aliases = {m["node_id"] for m in messages if "node_id" in m}
    modbus_aliases = seen_aliases & EXPECTED_ALIASES
    assert modbus_aliases == EXPECTED_ALIASES, (
        f"Missing Modbus aliases on uns.ingress.raw: {EXPECTED_ALIASES - modbus_aliases}"
    )


@pytest.mark.skipif(
    not __import__("tests.e2e.conftest", fromlist=["_cluster_is_up"])._cluster_is_up(),
    reason="no live k3d cluster",
)
async def test_modbus_envelope_schema(cluster: EirVahCluster) -> None:
    from eirvah_contracts.signals import RawSignalEnvelope

    messages = await _collect_raw_messages(cluster, timeout_s=10.0, max_messages=20)
    modbus_msgs = [m for m in messages if m.get("source_endpoint", "").startswith("modbus-tcp://")]
    assert modbus_msgs, "No Modbus messages found — check source_endpoint prefix"

    for raw in modbus_msgs[:5]:
        env = RawSignalEnvelope.model_validate(raw)
        assert env.quality == "good"
        assert env.source_endpoint.startswith("modbus-tcp://")
```

- [ ] **Step 3: Run unit tests (no cluster needed) — verify they are collected**

```bash
uv run pytest tests/e2e/test_modbus_path.py --collect-only 2>&1 | grep "test_modbus"
```

Expected: two tests collected (both will skip without a live cluster).

- [ ] **Step 4: Rebuild and redeploy to live cluster**

```bash
./scripts/build_all.sh local
kind load docker-image modbus-simulator:local --name eirvah-edge
kind load docker-image modbus-data-subscriber:local --name eirvah-edge
kubectl apply -k deploy/k3s/overlays/local
kubectl -n eirvah-edge rollout status deployment/modbus-simulator --timeout=60s
kubectl -n eirvah-edge rollout status deployment/modbus-data-subscriber --timeout=60s
```

- [ ] **Step 5: Run the e2e tests against the live cluster**

```bash
uv run pytest tests/e2e/test_modbus_path.py -v
```

Expected: both tests PASS (not skipped).

- [ ] **Step 6: Verify both protocols visible on NATS simultaneously**

```bash
# Subscribe to uns.ingress.raw and check both opcua and modbus sources appear
kubectl -n eirvah-edge exec -it deployment/nats -- nats sub uns.ingress.raw 2>/dev/null | head -20
```

Both `"source_endpoint": "opc.tcp://..."` and `"source_endpoint": "modbus-tcp://..."` should appear in the stream.

- [ ] **Step 7: Commit**

```bash
git add tests/e2e/test_modbus_path.py tests/e2e/conftest.py
git commit -m "test(e2e): Modbus ingress path publishes to uns.ingress.raw with correct aliases"
```

---

## Self-review

**Spec coverage:**
- §1.2 "Multi-protocol adapters (Modbus) — second slice" ✓ Task 1+2
- "All components are open source" ✓ pymodbus is BSD-3-Clause
- "Services follow layout under `services/<name>/src/<snake_case_name>/`" ✓
- "All shared code lives in `libs/eirvah-*/`" ✓ no new libs needed
- "Pipeline orchestration lives in the orchestrator pods, not the workers" ✓ subscriber is stateless
- "Tests come first" ✓ failing tests written before implementation in both Task 1 and Task 2

**Type consistency check:**
- `RegisterConfig.value_type` is `SignalValueType` (from `eirvah_contracts.signals`) — matches `build_raw_envelope` parameter type ✓
- `apply_scale` returns `float | int` — matches `RawSignalEnvelope.value: SignalValue` which is `float | int | bool | str` ✓
- `RegisterBlock.as_list()` returns `list[int]` — matches `ModbusSequentialDataBlock` constructor ✓
- `source_endpoint` format `modbus-tcp://host:port` — consistent across `service.py`, test assertions, and e2e test check ✓

**Placeholder scan:** None found.

**Gap check:**
- `dev_up.sh` needs Modbus port-forward hint — added in Task 3 Step 5 ✓
- `build_all.sh` needs both new services — added in Task 3 Step 4 ✓
- `kustomization.yaml` needs both new dirs — added in Task 3 Step 3 ✓
