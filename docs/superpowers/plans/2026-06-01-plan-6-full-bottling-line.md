# Plan 6 — Full Bottling Line Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the running system to represent all 8 stations of a bottling line across 3 protocols (OPC UA, Modbus TCP, S7), making protocol-agnosticism visible across the full production line in a single Grafana dashboard.

**Architecture:** No new services beyond the S7 pair. The existing OPC UA simulator gets two new equipment sections (Inspector, Labeler) via a new `quality_rate` dynamics class. The existing Modbus simulator gets 5 new registers (Conveyor + Reject Station). A new S7 simulator exposes two Siemens-style data blocks (DB1=Capper, DB2=Palletizer) via python-snap7; a paired S7 subscriber polls them and publishes to `uns.ingress.raw`. All 8 stations flow through the unchanged converter → contextualizer → MQTT publisher pipeline.

**Tech Stack:** Python 3.12, pymodbus 3.7.x (Modbus), asyncua (OPC UA), python-snap7≥1.3 (MIT, OSI-approved) wrapping snap7 (LGPL-2.1, OSI-approved). All existing libs unchanged.

---

## Full station → protocol mapping

| Station | Protocol | Simulator | Signals |
|---|---|---|---|
| Bottler | OPC UA | opcua-simulator (existing) | temperature, setpoint, motor, RPM, throughput |
| Filler | Modbus TCP | modbus-simulator (existing) | fill level, motor state, throughput |
| Conveyor | Modbus TCP | modbus-simulator (extend) | belt speed, jam detected, bottle count |
| Reject Station | Modbus TCP | modbus-simulator (extend) | reject count, conveyor active |
| Inspector | OPC UA | opcua-simulator (extend) | good rate % |
| Labeler | OPC UA | opcua-simulator (extend) | alignment score % |
| Capper | S7 | s7-simulator DB1 (new) | torque, cap presence, rejects/min |
| Palletizer | S7 | s7-simulator DB2 (new) | layer count, pallet complete, cycles/hr |

## Modbus register layout (full, addresses 0–7)

| Address | Alias | Scale | Default | Station |
|---|---|---|---|---|
| 0 | `Filler.FillLevelSensor01` | ×10 | 750 (75.0%) | Filler |
| 1 | `Filler.Motor01.State` | ×1 | 1 | Filler |
| 2 | `Filler.ThroughputMeter01` | ×100 | 80 (0.80 b/s) | Filler |
| 3 | `Conveyor.Belt01.BeltSpeed` | ×100 | 50 (0.50 m/s) | Conveyor |
| 4 | `Conveyor.Belt01.JamDetected` | ×1 | 0 | Conveyor |
| 5 | `Conveyor.Belt01.BottleCount` | ×1 | 0 (increments) | Conveyor |
| 6 | `RejectStation.RejectCounter01` | ×1 | 0 (rarely increments) | Reject Station |
| 7 | `RejectStation.ConveyorActive01` | ×1 | 1 | Reject Station |

## S7 data block layout

**DB1 (Capper, 8 bytes)**
| Offset | Type | Alias | Default |
|---|---|---|---|
| 0 | REAL (4 b) | `Capper.TorqueSensor01` | 2.5 Nm |
| 4 | INT (2 b) | `Capper.CapSensor01` | 1 (present) |
| 6 | INT (2 b) | `Capper.RejectCounter01` | 0 |

**DB2 (Palletizer, 8 bytes)**
| Offset | Type | Alias | Default |
|---|---|---|---|
| 0 | INT (2 b) | `Palletizer.LayerCounter01` | 0 (increments to 10) |
| 2 | INT (2 b) | `Palletizer.PalletSensor01` | 0 (1 briefly when layer resets) |
| 4 | INT (2 b) | `Palletizer.CycleCounter01` | 12 (cycles/hr) |
| 6 | padding | — | 0 |

---

## Files produced by this plan

```
services/modbus-simulator/src/modbus_simulator/server.py       MODIFY  extend RegisterBlock + tick
services/modbus-simulator/src/modbus_simulator/metrics.py      MODIFY  add 5 new gauges
services/modbus-simulator/tests/test_modbus_simulator.py       MODIFY  update tests

services/opcua-simulator/src/opcua_simulator/quality_rate.py   NEW     QualityRateDynamics class
services/opcua-simulator/src/opcua_simulator/server.py         MODIFY  register quality_rate + wire tick
services/opcua-simulator/src/opcua_simulator/metrics.py        MODIFY  add quality_rate gauge
config/opcua-address-space.yaml                                MODIFY  add inspector + labeler equipment
deploy/k3s/base/opcua-data-subscriber/opcua-node-list.yaml     MODIFY  add inspector + labeler nodes

services/s7-simulator/
├── pyproject.toml                                             NEW
├── Dockerfile                                                 NEW
├── src/s7_simulator/__init__.py                               NEW
├── src/s7_simulator/__main__.py                               NEW
├── src/s7_simulator/config.py                                 NEW
├── src/s7_simulator/server.py                                 NEW
├── src/s7_simulator/metrics.py                                NEW
└── tests/test_s7_simulator.py                                 NEW

services/s7-data-subscriber/
├── pyproject.toml                                             NEW
├── Dockerfile                                                 NEW
├── src/s7_data_subscriber/__init__.py                         NEW
├── src/s7_data_subscriber/__main__.py                         NEW
├── src/s7_data_subscriber/config.py                           NEW
├── src/s7_data_subscriber/service.py                          NEW
└── tests/test_s7_data_subscriber.py                           NEW

deploy/k3s/base/s7-simulator/
├── deployment.yaml                                            NEW
├── service.yaml                                               NEW
└── kustomization.yaml                                         NEW

deploy/k3s/base/s7-data-subscriber/
├── deployment.yaml                                            NEW
├── service.yaml                                               NEW
├── s7-unit-map.yaml                                           NEW
└── kustomization.yaml                                         NEW

deploy/k3s/base/kustomization.yaml                             MODIFY  add s7-simulator + s7-data-subscriber
scripts/build_all.sh                                           MODIFY  add both new services
scripts/dev_up.sh                                              MODIFY  add both new services

deploy/k3s/base/modbus-data-subscriber/modbus-register-map.yaml  MODIFY  add addresses 3–7
deploy/k3s/base/data-converter/conversion-rules.yaml              MODIFY  add 13 new signal rules
deploy/k3s/base/uns-auto-contextualizer/opcua-node-to-uns-mapping.yaml  MODIFY  add all new mappings

deploy/grafana/dashboards/bottling-line-state.json             MODIFY  6 new station rows
```

---

## Task 1: Extend Modbus simulator — Conveyor + Reject Station

**Files:**
- Modify: `services/modbus-simulator/src/modbus_simulator/server.py`
- Modify: `services/modbus-simulator/src/modbus_simulator/metrics.py`
- Modify: `services/modbus-simulator/tests/test_modbus_simulator.py`

- [ ] **Step 1: Update tests first**

Replace `services/modbus-simulator/tests/test_modbus_simulator.py`:

```python
from __future__ import annotations

import pytest
from modbus_simulator.server import RegisterBlock, scale_to_register, register_to_scale


def test_scale_to_register_basic() -> None:
    assert scale_to_register(22.00, scale=100) == 2200


def test_scale_to_register_rounds() -> None:
    assert scale_to_register(22.005, scale=100) == 2201


def test_register_to_scale_fill_level() -> None:
    assert register_to_scale(750, scale=10) == pytest.approx(75.0)


def test_register_block_defaults() -> None:
    block = RegisterBlock()
    assert block.fill_level_raw == 750
    assert block.motor_state == 1
    assert block.throughput_raw == 80
    assert block.belt_speed_raw == 50
    assert block.jam_detected == 0
    assert block.bottle_count == 0
    assert block.reject_count == 0
    assert block.conveyor_active == 1


def test_register_block_as_list_length() -> None:
    block = RegisterBlock()
    assert len(block.as_list()) == 8


def test_register_block_as_list_order() -> None:
    block = RegisterBlock(
        fill_level_raw=700, motor_state=1, throughput_raw=75,
        belt_speed_raw=55, jam_detected=0, bottle_count=10,
        reject_count=1, conveyor_active=1,
    )
    assert block.as_list() == [700, 1, 75, 55, 0, 10, 1, 1]


def test_register_block_tick_changes_fill_level() -> None:
    import random
    rng = random.Random(42)
    block = RegisterBlock()
    block.tick(rng=rng)
    assert 500 <= block.fill_level_raw <= 950


def test_register_block_tick_changes_belt_speed() -> None:
    import random
    rng = random.Random(0)
    block = RegisterBlock()
    for _ in range(10):
        block.tick(rng=rng)
    assert 20 <= block.belt_speed_raw <= 80


def test_register_block_tick_bottle_count_increments() -> None:
    import random
    rng = random.Random(0)
    block = RegisterBlock()
    block.tick(rng=rng)
    assert block.bottle_count == 1


def test_register_block_tick_clamps_fill_level() -> None:
    import random
    rng = random.Random(0)
    block = RegisterBlock(fill_level_raw=500)
    for _ in range(30):
        block.tick(rng=rng, delta_max=200)
    assert block.fill_level_raw >= 500


def test_register_block_tick_clamps_belt_speed() -> None:
    import random
    rng = random.Random(0)
    block = RegisterBlock(belt_speed_raw=80)
    for _ in range(30):
        block.tick(rng=rng)
    assert block.belt_speed_raw <= 80
```

- [ ] **Step 2: Run — verify FAIL**

```bash
uv run pytest services/modbus-simulator/tests/test_modbus_simulator.py -v 2>&1 | tail -10
```

Expected: `AssertionError` — RegisterBlock still has old fields.

- [ ] **Step 3: Update `server.py` — RegisterBlock, tick, _build_context**

Replace the `RegisterBlock` dataclass and `_build_context` / `_tick_loop` in `services/modbus-simulator/src/modbus_simulator/server.py`.

Replace `RegisterBlock`:

```python
@dataclass
class RegisterBlock:
    # Filler (addresses 0–2)
    fill_level_raw: int = 750    # 75.0% (scale ×10)
    motor_state: int = 1          # running
    throughput_raw: int = 80      # 0.80 bottles/s (scale ×100)
    # Conveyor (addresses 3–5)
    belt_speed_raw: int = 50      # 0.50 m/s (scale ×100)
    jam_detected: int = 0
    bottle_count: int = 0         # cumulative, wraps at 65535
    # Reject Station (addresses 6–7)
    reject_count: int = 0         # cumulative, rarely increments
    conveyor_active: int = 1

    def as_list(self) -> list[int]:
        return [
            self.fill_level_raw, self.motor_state, self.throughput_raw,
            self.belt_speed_raw, self.jam_detected, self.bottle_count,
            self.reject_count, self.conveyor_active,
        ]

    def tick(self, *, rng: random.Random, delta_max: int = 20) -> None:
        # Filler fill level random walk
        span = abs(delta_max)
        self.fill_level_raw = max(500, min(950, self.fill_level_raw + rng.randint(-span, span)))
        # Conveyor belt speed random walk
        self.belt_speed_raw = max(20, min(80, self.belt_speed_raw + rng.randint(-5, 5)))
        # Bottle count increments each tick
        self.bottle_count = (self.bottle_count + 1) % 65536
        # Jam: very rare (0.1% chance)
        self.jam_detected = 1 if rng.random() < 0.001 else 0
        # Reject count: rare (1% chance per tick)
        if rng.random() < 0.01:
            self.reject_count = min(self.reject_count + 1, 65535)
```

Replace `_build_context` (8 registers + 2 padding = 10):

```python
    def _build_context(self) -> ModbusServerContext:
        store = ModbusSlaveContext(
            hr=ModbusSequentialDataBlock(0, self._block.as_list() + [0] * 2),
        )
        return ModbusServerContext(slaves={self._settings.unit_id: store}, single=False)
```

Replace `_tick_loop`:

```python
    async def _tick_loop(self) -> None:
        interval = self._settings.tick_rate_ms / 1000.0
        while True:
            await asyncio.sleep(interval)
            self._block.tick(rng=self._rng)
            if self._context is not None:
                self._context[self._settings.unit_id].setValues(
                    _HR, 0, self._block.as_list()
                )
            self._metrics.set_fill_level(register_to_scale(self._block.fill_level_raw, scale=10))
            self._metrics.set_motor_state(self._block.motor_state)
            self._metrics.set_throughput(register_to_scale(self._block.throughput_raw, scale=100))
            self._metrics.set_belt_speed(register_to_scale(self._block.belt_speed_raw, scale=100))
            self._metrics.set_jam_detected(self._block.jam_detected)
            self._metrics.set_bottle_count(self._block.bottle_count)
            self._metrics.set_reject_count(self._block.reject_count)
            self._metrics.set_conveyor_active(self._block.conveyor_active)
```

- [ ] **Step 4: Update `metrics.py`**

Replace full content of `services/modbus-simulator/src/modbus_simulator/metrics.py`:

```python
"""Prometheus metrics for the Modbus multi-station simulator."""
from __future__ import annotations

from eirvah_observability.metrics import make_gauge
from prometheus_client.registry import REGISTRY, CollectorRegistry


class SimulatorMetrics:
    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self._fill_level = make_gauge(
            "modbus_simulator_fill_level_percent",
            "Filler fill level (%).", labelnames=[], registry=registry,
        )
        self._motor_state = make_gauge(
            "modbus_simulator_motor_state",
            "Filler motor state: 0=stopped 1=running.", labelnames=[], registry=registry,
        )
        self._throughput = make_gauge(
            "modbus_simulator_throughput_bottles_per_second",
            "Filler throughput (bottles/s).", labelnames=[], registry=registry,
        )
        self._belt_speed = make_gauge(
            "modbus_simulator_belt_speed_meters_per_second",
            "Conveyor belt speed (m/s).", labelnames=[], registry=registry,
        )
        self._jam_detected = make_gauge(
            "modbus_simulator_jam_detected",
            "Conveyor jam detected: 0=clear 1=jammed.", labelnames=[], registry=registry,
        )
        self._bottle_count = make_gauge(
            "modbus_simulator_bottle_count_total",
            "Conveyor cumulative bottle count.", labelnames=[], registry=registry,
        )
        self._reject_count = make_gauge(
            "modbus_simulator_reject_count_total",
            "Reject station cumulative reject count.", labelnames=[], registry=registry,
        )
        self._conveyor_active = make_gauge(
            "modbus_simulator_conveyor_active",
            "Reject station conveyor active: 0=stopped 1=running.", labelnames=[], registry=registry,
        )

    def set_fill_level(self, value: float) -> None:
        self._fill_level.set(value)

    def set_motor_state(self, value: int) -> None:
        self._motor_state.set(value)

    def set_throughput(self, value: float) -> None:
        self._throughput.set(value)

    def set_belt_speed(self, value: float) -> None:
        self._belt_speed.set(value)

    def set_jam_detected(self, value: int) -> None:
        self._jam_detected.set(value)

    def set_bottle_count(self, value: int) -> None:
        self._bottle_count.set(value)

    def set_reject_count(self, value: int) -> None:
        self._reject_count.set(value)

    def set_conveyor_active(self, value: int) -> None:
        self._conveyor_active.set(value)
```

- [ ] **Step 5: Run tests — verify ALL PASS**

```bash
uv run pytest services/modbus-simulator/tests/test_modbus_simulator.py -v
```

Expected: `12 passed`.

- [ ] **Step 6: Commit**

```bash
git add services/modbus-simulator/
git commit -m "feat(modbus-simulator): add Conveyor + Reject Station registers (addresses 3-7)"
```

---

## Task 2: Extend OPC UA simulator — Inspector + Labeler

**Files:**
- Create: `services/opcua-simulator/src/opcua_simulator/quality_rate.py`
- Modify: `services/opcua-simulator/src/opcua_simulator/server.py`
- Modify: `services/opcua-simulator/src/opcua_simulator/metrics.py`
- Modify: `config/opcua-address-space.yaml`
- Modify: `deploy/k3s/base/opcua-data-subscriber/opcua-node-list.yaml`

`_value_for_node` in `server.py` already has a catch-all `case _: return node_def.initial` — new `quality_rate` nodes need to be handled before the tick loop calls this, by storing current values in `self._quality_rate_current`.

- [ ] **Step 1: Create `quality_rate.py`**

```python
# services/opcua-simulator/src/opcua_simulator/quality_rate.py
"""Mean-reverting quality rate dynamics (e.g. inspection pass rate, label alignment)."""
from __future__ import annotations

from dataclasses import dataclass, field

from opcua_simulator.rng import SimulatorRNG


@dataclass
class QualityRateDynamics:
    """Simulates a quality percentage that wanders near a target value.

    Uses the same mean-reversion formula as TemperatureDynamics but
    with no external setpoint — the target is fixed at construction.
    """

    target: float       # e.g. 98.0 for 98%
    alpha: float = 0.1  # convergence speed
    sigma: float = 0.3  # noise magnitude
    rng: SimulatorRNG = field(default_factory=lambda: SimulatorRNG(seed=0))

    def __post_init__(self) -> None:
        self.value: float = self.target

    def tick(self) -> float:
        noise = self.rng.gauss(0.0, self.sigma) if self.sigma > 0.0 else 0.0
        self.value = self.value + self.alpha * (self.target - self.value) + noise
        self.value = max(85.0, min(100.0, self.value))
        return self.value
```

- [ ] **Step 2: Add `quality_rate_percent` gauge to `metrics.py`**

Read `services/opcua-simulator/src/opcua_simulator/metrics.py` first, then append a new gauge and setter AFTER the last existing gauge (`_hot_spikes`) and AFTER the last existing method:

Add to `__init__` after `self._hot_spikes = ...`:

```python
        self._quality_rate = make_gauge(
            "simulator_quality_rate_percent",
            "Quality pass rate for inspector/labeler equipment (%).",
            labelnames=_ISA95_LABELS,
            registry=registry,
        )
```

Add method after `inc_hot_spike`:

```python
    def set_quality_rate(self, labels: dict[str, str], value: float) -> None:
        self._quality_rate.labels(**labels).set(value)
```

- [ ] **Step 3: Update `server.py` — wire quality_rate dynamics**

Read `services/opcua-simulator/src/opcua_simulator/server.py`.

**3a** — add import at top (after existing imports):

```python
from opcua_simulator.quality_rate import QualityRateDynamics
```

**3b** — add instance variable in `__init__` after `self._quality_per_node`:

```python
        self._quality_rate_nodes: dict[str, QualityRateDynamics] = {}
        self._quality_rate_current: dict[str, float] = {}
```

**3c** — in `_build_dynamics()`, add AFTER the existing `for node_def in self._address_space.iter_nodes():` quality emitter loop:

```python
        for node_def in self._address_space.iter_nodes():
            if node_def.dynamics == "quality_rate":
                self._quality_rate_nodes[node_def.id] = QualityRateDynamics(
                    target=float(node_def.initial),
                    rng=self.rng,
                )
                self._quality_rate_current[node_def.id] = float(node_def.initial)
```

**3d** — in `_tick()`, add AFTER `tput = self._throughput.compute(...)` and BEFORE the `for node_def in` loop:

```python
        for node_id, dyn in self._quality_rate_nodes.items():
            self._quality_rate_current[node_id] = dyn.tick()
```

**3e** — in `_value_for_node()`, add a new case BEFORE the `case _:` catch-all:

```python
            case "quality_rate":
                return float(self._quality_rate_current.get(node_def.id, float(node_def.initial)))
```

**3f** — in `_update_state_metric()`, add a new case BEFORE the `case None if ...` case:

```python
            case "quality_rate":
                self.metrics.set_quality_rate(labels, float(value))
```

- [ ] **Step 4: Add Inspector + Labeler to `config/opcua-address-space.yaml`**

Read the file first. Append two new equipment entries AFTER the closing of the `bottler` equipment block:

```yaml
  - name: inspector
    nodes:
      - id: Inspector01.GoodRate
        kind: measurement
        cell: inspector
        equipment: inspector_01
        measurement: good_rate
        value_type: double
        unit: percent
        initial: 98.0
        semantic_type: quality.percent
        dynamics: quality_rate
        bad_quality_pct: 0.0

  - name: labeler
    nodes:
      - id: Labeler01.AlignmentScore
        kind: measurement
        cell: labeler
        equipment: labeler_01
        measurement: alignment_score
        value_type: double
        unit: percent
        initial: 97.0
        semantic_type: quality.percent
        dynamics: quality_rate
        bad_quality_pct: 0.0
```

- [ ] **Step 5: Add nodes to OPC UA subscriber node list**

Read `deploy/k3s/base/opcua-data-subscriber/opcua-node-list.yaml`. Append two entries to the `nodes:` list:

```yaml
  - browse_names: ["inspector", "GoodRate"]
    alias: "Inspector.Inspector01.GoodRate"
  - browse_names: ["labeler", "AlignmentScore"]
    alias: "Labeler.Labeler01.AlignmentScore"
```

- [ ] **Step 6: Smoke-test OPC UA simulator imports compile**

```bash
uv run python -c "from opcua_simulator.quality_rate import QualityRateDynamics; print('OK')"
```

Expected: `OK`

- [ ] **Step 7: Commit**

```bash
git add services/opcua-simulator/ config/opcua-address-space.yaml \
        deploy/k3s/base/opcua-data-subscriber/opcua-node-list.yaml
git commit -m "feat(opcua-simulator): add Inspector + Labeler via quality_rate dynamics"
```

---

## Task 3: S7 Capper + Palletizer simulator service

**Files:** All new under `services/s7-simulator/`

The snap7 server runs in a background thread (it is synchronous). The async tick loop updates the ctypes data buffers; snap7 serves client reads directly from those buffers.

Port 102 is the S7/ISO-TSAP standard port. The Dockerfile does NOT use `USER nobody:nogroup` — port 102 requires root. The k8s deployment uses `securityContext.runAsUser: 0`.

python-snap7 bundles the snap7 shared library in its wheel for linux/aarch64 — no apt-get needed.

- [ ] **Step 1: Write failing tests**

```python
# services/s7-simulator/tests/test_s7_simulator.py
from __future__ import annotations

import ctypes

import pytest
from s7_simulator.server import CapperBlock, PalletizerBlock, encode_real, encode_int


def test_encode_real_basic() -> None:
    buf = bytearray(4)
    encode_real(buf, 0, 2.5)
    import struct
    assert struct.unpack(">f", buf[0:4])[0] == pytest.approx(2.5, abs=0.001)


def test_encode_int_basic() -> None:
    buf = bytearray(2)
    encode_int(buf, 0, 42)
    import struct
    assert struct.unpack(">h", buf[0:2])[0] == 42


def test_capper_block_defaults() -> None:
    block = CapperBlock()
    assert block.torque_nm == pytest.approx(2.5)
    assert block.cap_presence == 1
    assert block.rejects_per_min == 0


def test_capper_block_to_bytes_length() -> None:
    block = CapperBlock()
    data = block.to_bytes()
    assert len(data) == 8


def test_capper_block_tick_torque_in_range() -> None:
    import random
    rng = random.Random(42)
    block = CapperBlock()
    for _ in range(20):
        block.tick(rng=rng)
    assert 1.5 <= block.torque_nm <= 4.0


def test_palletizer_block_defaults() -> None:
    block = PalletizerBlock()
    assert block.layer_count == 0
    assert block.pallet_complete == 0
    assert block.cycles_per_hr == 12


def test_palletizer_block_to_bytes_length() -> None:
    block = PalletizerBlock()
    assert len(block.to_bytes()) == 8


def test_palletizer_block_layer_increments() -> None:
    import random
    rng = random.Random(0)
    block = PalletizerBlock()
    # tick 12 times — layer_count should increment once (every 12 ticks)
    for _ in range(12):
        block.tick(rng=rng)
    assert block.layer_count == 1


def test_palletizer_block_pallet_complete_resets_layer() -> None:
    import random
    rng = random.Random(0)
    block = PalletizerBlock()
    block.layer_count = 9
    block.tick(rng=rng)  # one more tick → layer=10 → pallet_complete=1, layer resets
    assert block.pallet_complete == 1
    assert block.layer_count == 0
```

- [ ] **Step 2: Run — verify FAIL**

```bash
uv run pytest services/s7-simulator/tests/test_s7_simulator.py -v 2>&1 | head -10
```

Expected: `ModuleNotFoundError: No module named 's7_simulator'`

- [ ] **Step 3: Create `pyproject.toml`**

```toml
# services/s7-simulator/pyproject.toml
[project]
name = "s7-simulator"
version = "0.0.0"
description = "Siemens S7 TCP simulator — Capper (DB1) + Palletizer (DB2)."
requires-python = ">=3.12"
license = { text = "Apache-2.0" }
dependencies = [
    "python-snap7>=1.3",
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
packages = ["src/s7_simulator"]
```

Add `s7-simulator` to root `pyproject.toml` workspace members, sources, and dev deps (same pattern as modbus-simulator).

- [ ] **Step 4: Create `config.py`**

```python
# services/s7-simulator/src/s7_simulator/config.py
"""Settings for the S7 Capper+Palletizer simulator."""
from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SimulatorSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="S7_SIMULATOR_",
        env_file=None,
        extra="ignore",
    )

    host: str = "0.0.0.0"
    tick_rate_ms: int = Field(default=500, ge=50, le=10000)
    seed: int = 0
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
```

- [ ] **Step 5: Create `metrics.py`**

```python
# services/s7-simulator/src/s7_simulator/metrics.py
"""Prometheus metrics for the S7 simulator."""
from __future__ import annotations

from eirvah_observability.metrics import make_gauge
from prometheus_client.registry import REGISTRY, CollectorRegistry


class SimulatorMetrics:
    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self._torque = make_gauge(
            "s7_simulator_capper_torque_nm",
            "Capper torque (Nm).", labelnames=[], registry=registry,
        )
        self._cap_presence = make_gauge(
            "s7_simulator_capper_cap_presence",
            "Capper cap presence: 0=absent 1=present.", labelnames=[], registry=registry,
        )
        self._rejects = make_gauge(
            "s7_simulator_capper_rejects_per_min",
            "Capper rejects per minute.", labelnames=[], registry=registry,
        )
        self._layer_count = make_gauge(
            "s7_simulator_palletizer_layer_count",
            "Palletizer current layer count.", labelnames=[], registry=registry,
        )
        self._pallet_complete = make_gauge(
            "s7_simulator_palletizer_pallet_complete",
            "Palletizer pallet complete flag: 0/1.", labelnames=[], registry=registry,
        )
        self._cycles_per_hr = make_gauge(
            "s7_simulator_palletizer_cycles_per_hr",
            "Palletizer cycles per hour.", labelnames=[], registry=registry,
        )

    def set_capper(self, torque: float, cap_presence: int, rejects: int) -> None:
        self._torque.set(torque)
        self._cap_presence.set(cap_presence)
        self._rejects.set(rejects)

    def set_palletizer(self, layer_count: int, pallet_complete: int, cycles_per_hr: int) -> None:
        self._layer_count.set(layer_count)
        self._pallet_complete.set(pallet_complete)
        self._cycles_per_hr.set(cycles_per_hr)
```

- [ ] **Step 6: Create `server.py`**

```python
# services/s7-simulator/src/s7_simulator/server.py
"""S7 TCP server — Capper (DB1) + Palletizer (DB2) data blocks."""
from __future__ import annotations

import asyncio
import ctypes
import random
import struct
import threading
from dataclasses import dataclass

import structlog
import uvicorn
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import configure_logging
from snap7 import server as snap7_server
from snap7 import types as snap7_types

from s7_simulator.config import SimulatorSettings
from s7_simulator.metrics import SimulatorMetrics

_log = structlog.get_logger("s7-simulator")

DB1_SIZE = 8   # Capper: REAL(4) + INT(2) + INT(2)
DB2_SIZE = 8   # Palletizer: INT(2) + INT(2) + INT(2) + padding(2)


def encode_real(buf: bytearray, offset: int, value: float) -> None:
    struct.pack_into(">f", buf, offset, value)


def encode_int(buf: bytearray, offset: int, value: int) -> None:
    struct.pack_into(">h", buf, offset, value)


@dataclass
class CapperBlock:
    torque_nm: float = 2.5
    cap_presence: int = 1
    rejects_per_min: int = 0

    def tick(self, *, rng: random.Random) -> None:
        self.torque_nm = max(1.5, min(4.0, self.torque_nm + rng.uniform(-0.2, 0.2)))
        self.cap_presence = 0 if rng.random() < 0.002 else 1
        self.rejects_per_min = max(0, self.rejects_per_min + (1 if rng.random() < 0.01 else -1 if self.rejects_per_min > 0 and rng.random() < 0.1 else 0))

    def to_bytes(self) -> bytearray:
        buf = bytearray(DB1_SIZE)
        encode_real(buf, 0, self.torque_nm)
        encode_int(buf, 4, self.cap_presence)
        encode_int(buf, 6, self.rejects_per_min)
        return buf


@dataclass
class PalletizerBlock:
    layer_count: int = 0
    pallet_complete: int = 0
    cycles_per_hr: int = 12
    _ticks_since_layer: int = 0

    def tick(self, *, rng: random.Random) -> None:
        self.pallet_complete = 0
        self._ticks_since_layer += 1
        if self._ticks_since_layer >= 12:  # new layer every ~6s at 500ms tick
            self._ticks_since_layer = 0
            self.layer_count += 1
            if self.layer_count >= 10:
                self.layer_count = 0
                self.pallet_complete = 1
        self.cycles_per_hr = max(8, min(16, self.cycles_per_hr + rng.randint(-1, 1)))

    def to_bytes(self) -> bytearray:
        buf = bytearray(DB2_SIZE)
        encode_int(buf, 0, self.layer_count)
        encode_int(buf, 2, self.pallet_complete)
        encode_int(buf, 4, self.cycles_per_hr)
        return buf


class SimulatorRuntime:
    def __init__(self, settings: SimulatorSettings) -> None:
        self._settings = settings
        self._rng = random.Random(settings.seed)
        self._metrics = SimulatorMetrics()
        self._capper = CapperBlock()
        self._palletizer = PalletizerBlock()
        self._lock = threading.Lock()
        self._server: snap7_server.Server | None = None
        self._db1 = (ctypes.c_uint8 * DB1_SIZE)()
        self._db2 = (ctypes.c_uint8 * DB2_SIZE)()
        self._ready = False

    def is_ready(self) -> bool:
        return self._ready

    def _sync_buffers(self) -> None:
        with self._lock:
            db1_bytes = self._capper.to_bytes()
            db2_bytes = self._palletizer.to_bytes()
        for i, b in enumerate(db1_bytes):
            self._db1[i] = b
        for i, b in enumerate(db2_bytes):
            self._db2[i] = b

    async def _tick_loop(self) -> None:
        interval = self._settings.tick_rate_ms / 1000.0
        while True:
            await asyncio.sleep(interval)
            with self._lock:
                self._capper.tick(rng=self._rng)
                self._palletizer.tick(rng=self._rng)
            self._sync_buffers()
            self._metrics.set_capper(
                self._capper.torque_nm,
                self._capper.cap_presence,
                self._capper.rejects_per_min,
            )
            self._metrics.set_palletizer(
                self._palletizer.layer_count,
                self._palletizer.pallet_complete,
                self._palletizer.cycles_per_hr,
            )

    async def run(self) -> None:
        self._server = snap7_server.Server()
        self._server.register_area(snap7_types.srvAreaDB, 1, self._db1)
        self._server.register_area(snap7_types.srvAreaDB, 2, self._db2)
        self._sync_buffers()
        self._server.start(LocalAddress=self._settings.host)
        self._ready = True
        _log.info("s7_simulator_starting", host=self._settings.host)
        await self._tick_loop()


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
# services/s7-simulator/src/s7_simulator/__init__.py
```

```python
# services/s7-simulator/src/s7_simulator/__main__.py
from __future__ import annotations
import asyncio
from s7_simulator.config import SimulatorSettings
from s7_simulator.server import run

def main() -> None:
    asyncio.run(run(SimulatorSettings()))

if __name__ == "__main__":  # pragma: no cover
    main()
```

- [ ] **Step 8: Run tests — verify ALL PASS**

```bash
uv run pytest services/s7-simulator/tests/test_s7_simulator.py -v
```

Expected: `10 passed`. Tests exercise only pure functions — no snap7 server needed.

- [ ] **Step 9: Create `Dockerfile`**

Note: `USER nobody:nogroup` is omitted — S7 protocol requires port 102 which needs root.

```dockerfile
# services/s7-simulator/Dockerfile
# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS builder
WORKDIR /workspace
RUN pip install --no-cache-dir uv==0.4.20
COPY pyproject.toml uv.lock /workspace/
COPY libs /workspace/libs
COPY services/s7-simulator /workspace/services/s7-simulator
RUN uv sync --frozen --no-dev --package s7-simulator

FROM python:3.12-slim AS runtime
WORKDIR /workspace
COPY --from=builder /workspace/.venv /workspace/.venv
COPY --from=builder /workspace/libs /workspace/libs
COPY --from=builder /workspace/services/s7-simulator/src /workspace/services/s7-simulator/src
ENV PATH="/workspace/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1
EXPOSE 102 8080
ENTRYPOINT ["/workspace/.venv/bin/python", "-m", "s7_simulator"]
```

- [ ] **Step 10: Commit**

```bash
git add services/s7-simulator/ pyproject.toml
git commit -m "feat(s7-simulator): Capper DB1 + Palletizer DB2 via python-snap7 server"
```

---

## Task 4: S7 data subscriber service

**Files:** All new under `services/s7-data-subscriber/`

The subscriber reads DB1 and DB2 from the S7 simulator each poll interval, extracts each signal, wraps in `RawSignalEnvelope`, publishes to `uns.ingress.raw`. snap7 client calls are blocking — wrapped in `asyncio.to_thread`.

- [ ] **Step 1: Write failing tests**

```python
# services/s7-data-subscriber/tests/test_s7_data_subscriber.py
from __future__ import annotations

import struct
from datetime import UTC, datetime

import pytest
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_contracts.signals import RawSignalEnvelope
from eirvah_contracts.ulid import is_valid_correlation_id


def test_decode_real() -> None:
    from s7_data_subscriber.service import decode_real
    buf = bytearray(4)
    struct.pack_into(">f", buf, 0, 2.5)
    assert decode_real(buf, 0) == pytest.approx(2.5, abs=0.001)


def test_decode_int() -> None:
    from s7_data_subscriber.service import decode_int
    buf = bytearray(2)
    struct.pack_into(">h", buf, 0, 42)
    assert decode_int(buf, 0) == 42


def test_decode_int_negative() -> None:
    from s7_data_subscriber.service import decode_int
    buf = bytearray(2)
    struct.pack_into(">h", buf, 0, -1)
    assert decode_int(buf, 0) == -1


def test_build_raw_envelope() -> None:
    from s7_data_subscriber.service import build_raw_envelope
    now = datetime.now(UTC)
    env = build_raw_envelope(
        alias="Capper.TorqueSensor01",
        value=2.5,
        value_type="double",
        source_endpoint="s7://s7-simulator:102",
        received_at=now,
    )
    assert isinstance(env, RawSignalEnvelope)
    assert env.node_id == "Capper.TorqueSensor01"
    assert env.value == pytest.approx(2.5)
    assert env.quality == "good"
    assert env.source_endpoint == "s7://s7-simulator:102"
    assert env.source_timestamp == now


def test_wrap_in_nats_envelope() -> None:
    from s7_data_subscriber.service import build_raw_envelope, wrap_in_nats_envelope
    now = datetime.now(UTC)
    raw = build_raw_envelope(
        alias="Capper.TorqueSensor01",
        value=2.5,
        value_type="double",
        source_endpoint="s7://s7-simulator:102",
        received_at=now,
    )
    env = wrap_in_nats_envelope(raw)
    assert isinstance(env, NATSEnvelope)
    assert is_valid_correlation_id(env.correlation_id)
    assert env.payload["node_id"] == "Capper.TorqueSensor01"


def test_load_s7_unit_map(tmp_path) -> None:
    from s7_data_subscriber.service import S7UnitMapConfig, load_s7_unit_map
    cfg_file = tmp_path / "map.yaml"
    cfg_file.write_text("""
host: s7-simulator
port: 102
rack: 0
slot: 1
poll_interval_ms: 1000
data_blocks:
  - db: 1
    size: 8
    signals:
      - offset: 0
        type: real
        alias: Capper.TorqueSensor01
        value_type: double
      - offset: 4
        type: int
        alias: Capper.CapSensor01
        value_type: int64
""")
    cfg = load_s7_unit_map(cfg_file)
    assert isinstance(cfg, S7UnitMapConfig)
    assert cfg.host == "s7-simulator"
    assert len(cfg.data_blocks) == 1
    assert len(cfg.data_blocks[0].signals) == 2
    assert cfg.data_blocks[0].signals[0].alias == "Capper.TorqueSensor01"
```

- [ ] **Step 2: Run — verify FAIL**

```bash
uv run pytest services/s7-data-subscriber/tests/test_s7_data_subscriber.py -v 2>&1 | head -5
```

Expected: `ModuleNotFoundError: No module named 's7_data_subscriber'`

- [ ] **Step 3: Create `pyproject.toml`**

```toml
# services/s7-data-subscriber/pyproject.toml
[project]
name = "s7-data-subscriber"
version = "0.0.0"
description = "Siemens S7 TCP → NATS ingress subscriber."
requires-python = ">=3.12"
license = { text = "Apache-2.0" }
dependencies = [
    "python-snap7>=1.3",
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
packages = ["src/s7_data_subscriber"]
```

Add `s7-data-subscriber` to root `pyproject.toml` workspace members, sources, dev deps.

- [ ] **Step 4: Create `config.py`**

```python
# services/s7-data-subscriber/src/s7_data_subscriber/config.py
from __future__ import annotations
from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SubscriberSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="S7_DATA_SUBSCRIBER_",
        env_file=None,
        extra="ignore",
    )
    nats_servers: list[str] = ["nats://nats:4222"]
    unit_map_path: Path = Path("/etc/s7-data-subscriber/s7-unit-map.yaml")
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
    reconnect_delay_s: float = 5.0
```

- [ ] **Step 5: Create `service.py`**

```python
# services/s7-data-subscriber/src/s7_data_subscriber/service.py
"""S7 TCP data subscriber — polls DB1+DB2, publishes to uns.ingress.raw."""
from __future__ import annotations

import asyncio
import struct
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
from snap7.client import Client as S7Client

from s7_data_subscriber.config import SubscriberSettings

_log = structlog.get_logger("s7-data-subscriber")
_NATS_SUBJECT = "uns.ingress.raw"
S7SignalType = Literal["real", "int"]


class SignalConfig(BaseModel):
    offset: int
    type: S7SignalType
    alias: str
    value_type: SignalValueType


class BlockConfig(BaseModel):
    db: int
    size: int
    signals: list[SignalConfig]


class S7UnitMapConfig(BaseModel):
    host: str
    port: int = 102
    rack: int = 0
    slot: int = 1
    poll_interval_ms: int = 1000
    data_blocks: list[BlockConfig]


def load_s7_unit_map(path: Path) -> S7UnitMapConfig:
    return S7UnitMapConfig.model_validate(yaml.safe_load(path.read_text()))


def decode_real(buf: bytes | bytearray, offset: int) -> float:
    return float(struct.unpack_from(">f", buf, offset)[0])


def decode_int(buf: bytes | bytearray, offset: int) -> int:
    return int(struct.unpack_from(">h", buf, offset)[0])


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
        source_timestamp=received_at,
        server_timestamp=received_at,
        received_at=received_at,
    )


def wrap_in_nats_envelope(raw: RawSignalEnvelope) -> NATSEnvelope:
    return NATSEnvelope(
        correlation_id=generate_correlation_id(),
        payload=raw.model_dump(mode="json"),
    )


class SubscriberRuntime:
    def __init__(self, settings: SubscriberSettings) -> None:
        self._settings = settings
        self._bus: BusClient | None = None
        self._ready = False
        self._connection_state = make_gauge(
            "s7_ingress_connection_state",
            "1 when S7 TCP connection is up, 0 otherwise",
            labelnames=["ingress", "state"],
        )

    def is_ready(self) -> bool:
        return self._ready

    async def run(self) -> None:
        cfg = load_s7_unit_map(self._settings.unit_map_path)
        self._bus = BusClient(servers=self._settings.nats_servers, name="s7-data-subscriber")
        await self._bus.connect()
        _log.info("nats_connected")
        while True:
            try:
                await self._poll_loop(cfg)
            except Exception as exc:
                self._ready = False
                self._connection_state.labels(ingress="s7", state="connected").set(0)
                self._connection_state.labels(ingress="s7", state="disconnected").set(1)
                _log.warning("s7_disconnected", error=str(exc))
                await asyncio.sleep(self._settings.reconnect_delay_s)

    async def _poll_loop(self, cfg: S7UnitMapConfig) -> None:
        source_endpoint = f"s7://{cfg.host}:{cfg.port}"
        interval = cfg.poll_interval_ms / 1000.0
        client = S7Client()
        await asyncio.to_thread(client.connect, cfg.host, cfg.rack, cfg.slot, cfg.port)
        self._ready = True
        self._connection_state.labels(ingress="s7", state="connected").set(1)
        self._connection_state.labels(ingress="s7", state="disconnected").set(0)
        _log.info("s7_connected", host=cfg.host, port=cfg.port)
        try:
            while True:
                await asyncio.sleep(interval)
                received_at = datetime.now(UTC)
                for block in cfg.data_blocks:
                    data = await asyncio.to_thread(client.db_read, block.db, 0, block.size)
                    for sig in block.signals:
                        value: float | int
                        if sig.type == "real":
                            value = decode_real(data, sig.offset)
                        else:
                            value = decode_int(data, sig.offset)
                        env = wrap_in_nats_envelope(
                            build_raw_envelope(
                                alias=sig.alias,
                                value=value,
                                value_type=sig.value_type,
                                source_endpoint=source_endpoint,
                                received_at=received_at,
                            )
                        )
                        await self._publish(env)
        finally:
            await asyncio.to_thread(client.disconnect)

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
        health.asgi, host="0.0.0.0", port=settings.http_port,
        log_level=settings.log_level.lower(),
    )
    http = uvicorn.Server(http_cfg)
    await asyncio.gather(runtime.run(), http.serve())
```

- [ ] **Step 6: Create `__init__.py` and `__main__.py`**

```python
# services/s7-data-subscriber/src/s7_data_subscriber/__init__.py
```

```python
# services/s7-data-subscriber/src/s7_data_subscriber/__main__.py
from __future__ import annotations
import asyncio
from s7_data_subscriber.config import SubscriberSettings
from s7_data_subscriber.service import run

def main() -> None:
    asyncio.run(run(SubscriberSettings()))

if __name__ == "__main__":  # pragma: no cover
    main()
```

- [ ] **Step 7: Run tests — verify ALL PASS**

```bash
uv run pytest services/s7-data-subscriber/tests/test_s7_data_subscriber.py -v
```

Expected: `6 passed`.

- [ ] **Step 8: Create `Dockerfile`**

```dockerfile
# services/s7-data-subscriber/Dockerfile
# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS builder
WORKDIR /workspace
RUN pip install --no-cache-dir uv==0.4.20
COPY pyproject.toml uv.lock /workspace/
COPY libs /workspace/libs
COPY services/s7-data-subscriber /workspace/services/s7-data-subscriber
RUN uv sync --frozen --no-dev --package s7-data-subscriber

FROM python:3.12-slim AS runtime
WORKDIR /workspace
COPY --from=builder /workspace/.venv /workspace/.venv
COPY --from=builder /workspace/libs /workspace/libs
COPY --from=builder /workspace/services/s7-data-subscriber/src /workspace/services/s7-data-subscriber/src
ENV PATH="/workspace/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1
USER nobody:nogroup
EXPOSE 8080
ENTRYPOINT ["/workspace/.venv/bin/python", "-m", "s7_data_subscriber"]
```

- [ ] **Step 9: Commit**

```bash
git add services/s7-data-subscriber/ pyproject.toml
git commit -m "feat(s7-data-subscriber): S7 TCP polling subscriber, reads DB1+DB2, publishes to uns.ingress.raw"
```

---

## Task 5: k8s manifests for S7 pair + update scripts

**Files:**
- Create: `deploy/k3s/base/s7-simulator/` (3 files)
- Create: `deploy/k3s/base/s7-data-subscriber/` (4 files)
- Modify: `deploy/k3s/base/kustomization.yaml`
- Modify: `scripts/build_all.sh`
- Modify: `scripts/dev_up.sh`

- [ ] **Step 1: Create s7-simulator manifests**

```yaml
# deploy/k3s/base/s7-simulator/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: s7-simulator
  labels: { app.kubernetes.io/name: s7-simulator }
spec:
  replicas: 1
  selector:
    matchLabels: { app.kubernetes.io/name: s7-simulator }
  template:
    metadata:
      labels: { app.kubernetes.io/name: s7-simulator }
      annotations:
        eirvah.uniza/scrape: "true"
        eirvah.uniza/scrape-port: "8080"
    spec:
      containers:
        - name: s7-simulator
          image: s7-simulator:local
          imagePullPolicy: IfNotPresent
          env:
            - name: S7_SIMULATOR_TICK_RATE_MS
              value: "500"
          ports:
            - { name: s7, containerPort: 102 }
            - { name: http, containerPort: 8080 }
          securityContext:
            runAsUser: 0
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
# deploy/k3s/base/s7-simulator/service.yaml
apiVersion: v1
kind: Service
metadata:
  name: s7-simulator
  labels: { app.kubernetes.io/name: s7-simulator }
spec:
  selector: { app.kubernetes.io/name: s7-simulator }
  ports:
    - { name: s7, port: 102, targetPort: 102 }
    - { name: http, port: 8080, targetPort: 8080 }
```

```yaml
# deploy/k3s/base/s7-simulator/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - deployment.yaml
  - service.yaml
```

- [ ] **Step 2: Create s7-data-subscriber manifests**

```yaml
# deploy/k3s/base/s7-data-subscriber/s7-unit-map.yaml
# S7 unit map — Capper (DB1) + Palletizer (DB2)
host: s7-simulator
port: 102
rack: 0
slot: 1
poll_interval_ms: 1000
data_blocks:
  - db: 1
    size: 8
    signals:
      - offset: 0
        type: real
        alias: "Capper.TorqueSensor01"
        value_type: double
      - offset: 4
        type: int
        alias: "Capper.CapSensor01"
        value_type: int64
      - offset: 6
        type: int
        alias: "Capper.RejectCounter01"
        value_type: int64
  - db: 2
    size: 8
    signals:
      - offset: 0
        type: int
        alias: "Palletizer.LayerCounter01"
        value_type: int64
      - offset: 2
        type: int
        alias: "Palletizer.PalletSensor01"
        value_type: int64
      - offset: 4
        type: int
        alias: "Palletizer.CycleCounter01"
        value_type: int64
```

```yaml
# deploy/k3s/base/s7-data-subscriber/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: s7-data-subscriber
  labels: { app.kubernetes.io/name: s7-data-subscriber }
spec:
  replicas: 1
  selector:
    matchLabels: { app.kubernetes.io/name: s7-data-subscriber }
  template:
    metadata:
      labels: { app.kubernetes.io/name: s7-data-subscriber }
      annotations:
        eirvah.uniza/scrape: "true"
        eirvah.uniza/scrape-port: "8080"
    spec:
      containers:
        - name: s7-data-subscriber
          image: s7-data-subscriber:local
          imagePullPolicy: IfNotPresent
          env:
            - name: S7_DATA_SUBSCRIBER_NATS_SERVERS
              value: '["nats://nats:4222"]'
          ports:
            - { name: http, containerPort: 8080 }
          volumeMounts:
            - name: config
              mountPath: /etc/s7-data-subscriber
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
            name: s7-data-subscriber-config
```

```yaml
# deploy/k3s/base/s7-data-subscriber/service.yaml
apiVersion: v1
kind: Service
metadata:
  name: s7-data-subscriber
  labels: { app.kubernetes.io/name: s7-data-subscriber }
spec:
  selector: { app.kubernetes.io/name: s7-data-subscriber }
  ports:
    - { name: http, port: 8080, targetPort: 8080 }
```

```yaml
# deploy/k3s/base/s7-data-subscriber/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - deployment.yaml
  - service.yaml
configMapGenerator:
  - name: s7-data-subscriber-config
    files:
      - s7-unit-map.yaml
```

- [ ] **Step 3: Update `deploy/k3s/base/kustomization.yaml`**

Add after `modbus-data-subscriber`:

```yaml
  - s7-simulator
  - s7-data-subscriber
```

- [ ] **Step 4: Update `scripts/build_all.sh`** — add after `modbus-data-subscriber`:

```bash
  s7-simulator
  s7-data-subscriber
```

- [ ] **Step 5: Update `scripts/dev_up.sh`** — add to SERVICES array after `modbus-data-subscriber`, and add hint line:

```bash
echo "    S7:            kubectl -n ${NAMESPACE} port-forward svc/s7-simulator 102:102"
```

- [ ] **Step 6: Verify kustomize renders clean**

```bash
kubectl kustomize deploy/k3s/overlays/local 2>&1 | grep -E "name: s7"
```

Expected: `name: s7-data-subscriber` and `name: s7-simulator` appear.

- [ ] **Step 7: Commit**

```bash
git add deploy/k3s/base/s7-simulator/ deploy/k3s/base/s7-data-subscriber/ \
        deploy/k3s/base/kustomization.yaml scripts/build_all.sh scripts/dev_up.sh
git commit -m "feat(k8s): add S7 simulator + subscriber manifests for Capper + Palletizer"
```

---

## Task 6: Update pipeline configs

**Files:**
- Modify: `deploy/k3s/base/modbus-data-subscriber/modbus-register-map.yaml`
- Modify: `deploy/k3s/base/data-converter/conversion-rules.yaml`
- Modify: `deploy/k3s/base/uns-auto-contextualizer/opcua-node-to-uns-mapping.yaml`

- [ ] **Step 1: Replace `modbus-register-map.yaml`**

```yaml
# Register map for modbus-data-subscriber — Filler + Conveyor + Reject Station
host: modbus-simulator
port: 5020
unit_id: 1
poll_interval_ms: 500
registers:
  # Filler
  - address: 0
    alias: "Filler.FillLevelSensor01"
    scale: 10.0
    value_type: double
  - address: 1
    alias: "Filler.Motor01.State"
    scale: 1.0
    value_type: int64
  - address: 2
    alias: "Filler.ThroughputMeter01"
    scale: 100.0
    value_type: double
  # Conveyor
  - address: 3
    alias: "Conveyor.Belt01.BeltSpeed"
    scale: 100.0
    value_type: double
  - address: 4
    alias: "Conveyor.Belt01.JamDetected"
    scale: 1.0
    value_type: int64
  - address: 5
    alias: "Conveyor.Belt01.BottleCount"
    scale: 1.0
    value_type: int64
  # Reject Station
  - address: 6
    alias: "RejectStation.RejectCounter01"
    scale: 1.0
    value_type: int64
  - address: 7
    alias: "RejectStation.ConveyorActive01"
    scale: 1.0
    value_type: int64
```

- [ ] **Step 2: Append to `conversion-rules.yaml`**

Append after the last existing rule:

```yaml
  # Conveyor (Modbus)
  - node_id: "Conveyor.Belt01.BeltSpeed"
    value_type: double
    unit: "m/s"
    drop_bad_quality: false
  - node_id: "Conveyor.Belt01.JamDetected"
    value_type: int64
    unit: dimensionless
    drop_bad_quality: false
  - node_id: "Conveyor.Belt01.BottleCount"
    value_type: int64
    unit: dimensionless
    drop_bad_quality: false
  # Reject Station (Modbus)
  - node_id: "RejectStation.RejectCounter01"
    value_type: int64
    unit: dimensionless
    drop_bad_quality: false
  - node_id: "RejectStation.ConveyorActive01"
    value_type: int64
    unit: dimensionless
    drop_bad_quality: false
  # Inspector (OPC UA)
  - node_id: "Inspector.Inspector01.GoodRate"
    value_type: double
    unit: percent
    drop_bad_quality: false
  # Labeler (OPC UA)
  - node_id: "Labeler.Labeler01.AlignmentScore"
    value_type: double
    unit: percent
    drop_bad_quality: false
  # Capper (S7)
  - node_id: "Capper.TorqueSensor01"
    value_type: double
    unit: "Nm"
    drop_bad_quality: false
  - node_id: "Capper.CapSensor01"
    value_type: int64
    unit: dimensionless
    drop_bad_quality: false
  - node_id: "Capper.RejectCounter01"
    value_type: int64
    unit: dimensionless
    drop_bad_quality: false
  # Palletizer (S7)
  - node_id: "Palletizer.LayerCounter01"
    value_type: int64
    unit: dimensionless
    drop_bad_quality: false
  - node_id: "Palletizer.PalletSensor01"
    value_type: int64
    unit: dimensionless
    drop_bad_quality: false
  - node_id: "Palletizer.CycleCounter01"
    value_type: int64
    unit: dimensionless
    drop_bad_quality: false
```

- [ ] **Step 3: Append to `opcua-node-to-uns-mapping.yaml`**

Append after the last existing mapping:

```yaml
  # Conveyor (Modbus)
  - node_id: "Conveyor.Belt01.BeltSpeed"
    area: factory1
    line: line_a
    cell: conveyor
    equipment: belt_01
    measurement: belt_speed
    semantic_type: speed.ms

  - node_id: "Conveyor.Belt01.JamDetected"
    area: factory1
    line: line_a
    cell: conveyor
    equipment: belt_01
    measurement: jam_detected
    semantic_type: state.enum

  - node_id: "Conveyor.Belt01.BottleCount"
    area: factory1
    line: line_a
    cell: conveyor
    equipment: belt_01
    measurement: bottle_count
    semantic_type: count.cumulative

  # Reject Station (Modbus)
  - node_id: "RejectStation.RejectCounter01"
    area: factory1
    line: line_a
    cell: reject_station
    equipment: reject_counter_01
    measurement: reject_count
    semantic_type: count.cumulative

  - node_id: "RejectStation.ConveyorActive01"
    area: factory1
    line: line_a
    cell: reject_station
    equipment: conveyor_01
    measurement: conveyor_active
    semantic_type: state.enum

  # Inspector (OPC UA)
  - node_id: "Inspector.Inspector01.GoodRate"
    area: factory1
    line: line_a
    cell: inspector
    equipment: inspector_01
    measurement: good_rate
    semantic_type: quality.percent

  # Labeler (OPC UA)
  - node_id: "Labeler.Labeler01.AlignmentScore"
    area: factory1
    line: line_a
    cell: labeler
    equipment: labeler_01
    measurement: alignment_score
    semantic_type: quality.percent

  # Capper (S7)
  - node_id: "Capper.TorqueSensor01"
    area: factory1
    line: line_a
    cell: capper
    equipment: torque_sensor_01
    measurement: torque
    semantic_type: torque.nm

  - node_id: "Capper.CapSensor01"
    area: factory1
    line: line_a
    cell: capper
    equipment: cap_sensor_01
    measurement: cap_presence
    semantic_type: state.enum

  - node_id: "Capper.RejectCounter01"
    area: factory1
    line: line_a
    cell: capper
    equipment: reject_counter_01
    measurement: rejects_per_min
    semantic_type: count.rate

  # Palletizer (S7)
  - node_id: "Palletizer.LayerCounter01"
    area: factory1
    line: line_a
    cell: palletizer
    equipment: layer_counter_01
    measurement: layer_count
    semantic_type: count.cumulative

  - node_id: "Palletizer.PalletSensor01"
    area: factory1
    line: line_a
    cell: palletizer
    equipment: pallet_sensor_01
    measurement: pallet_complete
    semantic_type: state.enum

  - node_id: "Palletizer.CycleCounter01"
    area: factory1
    line: line_a
    cell: palletizer
    equipment: cycle_counter_01
    measurement: cycles_per_hr
    semantic_type: count.rate
```

- [ ] **Step 4: Commit**

```bash
git add deploy/k3s/base/modbus-data-subscriber/modbus-register-map.yaml \
        deploy/k3s/base/data-converter/conversion-rules.yaml \
        deploy/k3s/base/uns-auto-contextualizer/opcua-node-to-uns-mapping.yaml
git commit -m "feat(config): add pipeline mappings for all 8 bottling line stations"
```

---

## Task 7: Update Grafana dashboard

Dashboard currently ends at y=29 (panel 11 = y=21, h=8). New rows start at y=29.

**Files:**
- Modify: `deploy/grafana/dashboards/bottling-line-state.json`

- [ ] **Step 1: Read current panel max y to confirm start position**

```bash
python3 -c "
import json
d = json.load(open('deploy/grafana/dashboards/bottling-line-state.json'))
for p in d['panels']:
    print(f'id={p[\"id\"]} y={p[\"gridPos\"][\"y\"]} h={p[\"gridPos\"][\"h\"]} title={p[\"title\"]}')
print('max y+h =', max(p['gridPos']['y'] + p['gridPos']['h'] for p in d['panels']))
"
```

Note the max y+h value — use it as the start y for the new rows. If different from 29, adjust all y values in Step 2 by the difference.

- [ ] **Step 2: Append 6 new station rows to the panels array**

Run this Python script to add the new panels to the live dashboard JSON:

```python
import json

with open('deploy/grafana/dashboards/bottling-line-state.json') as f:
    d = json.load(f)

start_y = max(p['gridPos']['y'] + p['gridPos']['h'] for p in d['panels'])

new_panels = [
    # ── Conveyor ──
    {"id": 20, "type": "row", "title": "Conveyor (Modbus TCP)", "gridPos": {"x": 0, "y": start_y, "w": 24, "h": 1}, "collapsed": False},
    {"id": 21, "type": "timeseries", "title": "Belt speed (m/s)", "gridPos": {"x": 0, "y": start_y+1, "w": 8, "h": 6},
     "targets": [{"expr": "eirvah_modbus_simulator_belt_speed_meters_per_second", "legendFormat": "belt speed", "refId": "A"}]},
    {"id": 22, "type": "stat", "title": "Jam detected", "gridPos": {"x": 8, "y": start_y+1, "w": 4, "h": 6},
     "targets": [{"expr": "eirvah_modbus_simulator_jam_detected", "refId": "A"}],
     "fieldConfig": {"defaults": {"mappings": [{"type": "value", "options": {"0": {"text": "clear", "color": "green"}, "1": {"text": "JAM", "color": "red"}}}]}}},
    {"id": 23, "type": "timeseries", "title": "Bottle count (cumulative)", "gridPos": {"x": 12, "y": start_y+1, "w": 12, "h": 6},
     "targets": [{"expr": "eirvah_modbus_simulator_bottle_count_total", "legendFormat": "bottles", "refId": "A"}]},

    # ── Capper ──
    {"id": 30, "type": "row", "title": "Capper (Siemens S7)", "gridPos": {"x": 0, "y": start_y+7, "w": 24, "h": 1}, "collapsed": False},
    {"id": 31, "type": "timeseries", "title": "Cap torque (Nm)", "gridPos": {"x": 0, "y": start_y+8, "w": 12, "h": 6},
     "targets": [{"expr": "eirvah_s7_simulator_capper_torque_nm", "legendFormat": "torque", "refId": "A"}],
     "fieldConfig": {"defaults": {"unit": "Nm", "min": 0, "max": 6}}},
    {"id": 32, "type": "stat", "title": "Cap presence", "gridPos": {"x": 12, "y": start_y+8, "w": 6, "h": 6},
     "targets": [{"expr": "eirvah_s7_simulator_capper_cap_presence", "refId": "A"}],
     "fieldConfig": {"defaults": {"mappings": [{"type": "value", "options": {"0": {"text": "absent", "color": "red"}, "1": {"text": "capped", "color": "green"}}}]}}},
    {"id": 33, "type": "stat", "title": "Capper rejects/min", "gridPos": {"x": 18, "y": start_y+8, "w": 6, "h": 6},
     "targets": [{"expr": "eirvah_s7_simulator_capper_rejects_per_min", "refId": "A"}]},

    # ── Reject Station ──
    {"id": 40, "type": "row", "title": "Reject Station (Modbus TCP)", "gridPos": {"x": 0, "y": start_y+14, "w": 24, "h": 1}, "collapsed": False},
    {"id": 41, "type": "timeseries", "title": "Reject count (cumulative)", "gridPos": {"x": 0, "y": start_y+15, "w": 12, "h": 6},
     "targets": [{"expr": "eirvah_modbus_simulator_reject_count_total", "legendFormat": "rejects", "refId": "A"}]},
    {"id": 42, "type": "stat", "title": "Reject conveyor", "gridPos": {"x": 12, "y": start_y+15, "w": 6, "h": 6},
     "targets": [{"expr": "eirvah_modbus_simulator_conveyor_active", "refId": "A"}],
     "fieldConfig": {"defaults": {"mappings": [{"type": "value", "options": {"0": {"text": "stopped", "color": "gray"}, "1": {"text": "running", "color": "green"}}}]}}},

    # ── Inspector ──
    {"id": 50, "type": "row", "title": "Vision Inspector (OPC UA)", "gridPos": {"x": 0, "y": start_y+21, "w": 24, "h": 1}, "collapsed": False},
    {"id": 51, "type": "timeseries", "title": "Good rate (%)", "gridPos": {"x": 0, "y": start_y+22, "w": 12, "h": 6},
     "targets": [{"expr": "eirvah_simulator_quality_rate_percent{cell=\"inspector\"}", "legendFormat": "good rate", "refId": "A"}],
     "fieldConfig": {"defaults": {"unit": "percent", "min": 85, "max": 100, "thresholds": {"mode": "absolute", "steps": [{"value": None, "color": "red"}, {"value": 93, "color": "yellow"}, {"value": 96, "color": "green"}]}}}},
    {"id": 52, "type": "timeseries", "title": "Reject rate (%)", "gridPos": {"x": 12, "y": start_y+22, "w": 12, "h": 6},
     "targets": [{"expr": "100 - eirvah_simulator_quality_rate_percent{cell=\"inspector\"}", "legendFormat": "reject rate", "refId": "A"}],
     "fieldConfig": {"defaults": {"unit": "percent", "min": 0, "max": 15}}},

    # ── Labeler ──
    {"id": 60, "type": "row", "title": "Labeler (OPC UA)", "gridPos": {"x": 0, "y": start_y+28, "w": 24, "h": 1}, "collapsed": False},
    {"id": 61, "type": "timeseries", "title": "Label alignment score (%)", "gridPos": {"x": 0, "y": start_y+29, "w": 12, "h": 6},
     "targets": [{"expr": "eirvah_simulator_quality_rate_percent{cell=\"labeler\"}", "legendFormat": "alignment", "refId": "A"}],
     "fieldConfig": {"defaults": {"unit": "percent", "min": 85, "max": 100}}},

    # ── Palletizer ──
    {"id": 70, "type": "row", "title": "Palletizer (Siemens S7)", "gridPos": {"x": 0, "y": start_y+35, "w": 24, "h": 1}, "collapsed": False},
    {"id": 71, "type": "timeseries", "title": "Layer count", "gridPos": {"x": 0, "y": start_y+36, "w": 8, "h": 6},
     "targets": [{"expr": "eirvah_s7_simulator_palletizer_layer_count", "legendFormat": "layers", "refId": "A"}],
     "fieldConfig": {"defaults": {"min": 0, "max": 10}}},
    {"id": 72, "type": "stat", "title": "Pallet complete", "gridPos": {"x": 8, "y": start_y+36, "w": 6, "h": 6},
     "targets": [{"expr": "eirvah_s7_simulator_palletizer_pallet_complete", "refId": "A"}],
     "fieldConfig": {"defaults": {"mappings": [{"type": "value", "options": {"0": {"text": "building", "color": "yellow"}, "1": {"text": "COMPLETE", "color": "green"}}}]}}},
    {"id": 73, "type": "stat", "title": "Cycles/hr", "gridPos": {"x": 14, "y": start_y+36, "w": 6, "h": 6},
     "targets": [{"expr": "eirvah_s7_simulator_palletizer_cycles_per_hr", "refId": "A"}]},
]

d['panels'].extend(new_panels)

with open('deploy/grafana/dashboards/bottling-line-state.json', 'w') as f:
    json.dump(d, f, indent=2)

print(f"Total panels: {len(d['panels'])}")
```

Run it:

```bash
python3 /path/to/above-script.py
```

- [ ] **Step 3: Validate JSON**

```bash
python3 -c "import json; json.load(open('deploy/grafana/dashboards/bottling-line-state.json')); print('JSON valid')"
```

Expected: `JSON valid`

- [ ] **Step 4: Commit**

```bash
git add deploy/grafana/dashboards/bottling-line-state.json
git commit -m "feat(grafana): add Conveyor, Capper, Reject Station, Inspector, Labeler, Palletizer rows"
```

---

## Task 8: Rebuild, deploy, verify

- [ ] **Step 1: Build all 4 modified/new images**

```bash
./scripts/build_all.sh local 2>&1 | grep -E "^==> building|error" | head -30
```

All 13 images should build cleanly.

- [ ] **Step 2: Load new images into kind**

```bash
for img in modbus-simulator opcua-simulator s7-simulator s7-data-subscriber; do
  kind load docker-image ${img}:local --name eirvah-edge
done
```

- [ ] **Step 3: Apply updated configmaps + new resources**

```bash
kubectl apply -k deploy/k3s/overlays/local
```

Expected output includes `configmap/... created` for new S7 configmap, and `deployment.apps/s7-simulator configured` etc.

- [ ] **Step 4: Restart all affected deployments**

```bash
kubectl -n eirvah-edge rollout restart \
  deployment/modbus-simulator \
  deployment/modbus-data-subscriber \
  deployment/opcua-simulator \
  deployment/opcua-data-subscriber \
  deployment/s7-simulator \
  deployment/s7-data-subscriber \
  deployment/data-converter \
  deployment/uns-auto-contextualizer \
  deployment/grafana

for d in modbus-simulator modbus-data-subscriber opcua-simulator opcua-data-subscriber \
          s7-simulator s7-data-subscriber data-converter uns-auto-contextualizer grafana; do
  kubectl -n eirvah-edge rollout status deployment/$d --timeout=90s
done
```

Expected: all 9 show `successfully rolled out`.

- [ ] **Step 5: Verify key Prometheus metrics**

```bash
kubectl -n eirvah-edge port-forward svc/prometheus 9090:9090 &>/tmp/pf-prometheus.log &
sleep 2

# One metric from each protocol
for q in \
  "eirvah_modbus_simulator_belt_speed_meters_per_second" \
  "eirvah_simulator_quality_rate_percent" \
  "eirvah_s7_simulator_capper_torque_nm"; do
  val=$(curl -s "http://localhost:9090/api/v1/query?query=$q" | python3 -c "import sys,json; d=json.load(sys.stdin); r=d['data']['result']; print(r[0]['value'][1] if r else 'MISSING')")
  echo "$q = $val"
done
```

Expected: all three return numeric values (not MISSING).

- [ ] **Step 6: Update and run e2e tests**

Update `EXPECTED_ALIASES` in `tests/e2e/test_modbus_path.py` to include the new Modbus aliases:

```python
EXPECTED_ALIASES = {
    "Filler.FillLevelSensor01",
    "Filler.Motor01.State",
    "Filler.ThroughputMeter01",
    "Conveyor.Belt01.BeltSpeed",
    "Conveyor.Belt01.JamDetected",
    "Conveyor.Belt01.BottleCount",
    "RejectStation.RejectCounter01",
    "RejectStation.ConveyorActive01",
}
```

```bash
kubectl -n eirvah-edge port-forward svc/nats 4222:4222 &>/tmp/pf-nats.log &
sleep 2
uv run pytest tests/e2e/test_modbus_path.py -v
```

Expected: both tests PASS.

- [ ] **Step 7: Push Grafana dashboard to live configmap + restart**

```bash
# Find the configmap that Grafana mounts (same as before)
kubectl -n eirvah-edge get deployment grafana -o jsonpath='{.spec.template.spec.volumes}' | \
  python3 -c "import sys,json; vols=json.loads(sys.stdin.read()); [print(v['configMap']['name']) for v in vols if 'dashboard' in v['name'].lower() and 'configMap' in v]"
```

Use the printed name (e.g. `grafana-dashboards-m6m887ctht`) in:

```bash
DASH_CM=<name-from-above>
DASH=$(cat deploy/grafana/dashboards/bottling-line-state.json | python3 -c "import sys,json; print(json.dumps(sys.stdin.read()))")
kubectl -n eirvah-edge patch configmap $DASH_CM --type merge -p "{\"data\":{\"bottling-line-state.json\": $DASH}}"
kubectl -n eirvah-edge rollout restart deployment/grafana
kubectl -n eirvah-edge rollout status deployment/grafana --timeout=60s
```

- [ ] **Step 8: Commit e2e update**

```bash
git add tests/e2e/test_modbus_path.py
git commit -m "fix(e2e): update Modbus aliases for full Conveyor + Reject Station registers"
```

---

## Self-review

**Spec coverage:**
- Modbus: Conveyor (belt speed, jam, bottle count) + Reject Station ✓ Task 1
- OPC UA: Inspector (good rate) + Labeler (alignment score) via quality_rate dynamics ✓ Task 2
- S7: Capper (DB1) + Palletizer (DB2) simulator + subscriber ✓ Tasks 3+4
- k8s manifests for S7 pair ✓ Task 5
- Pipeline configs for all 13 new signals ✓ Task 6
- Dashboard: 6 new rows (Conveyor, Capper, Reject Station, Inspector, Labeler, Palletizer) ✓ Task 7
- Rebuild + deploy + verify ✓ Task 8

**Placeholder scan:** None found. All code blocks complete.

**Type/name consistency:**
- `RegisterBlock.belt_speed_raw` used in `as_list()[3]`, `tick()`, `_tick_loop metrics call`, tests ✓
- `QualityRateDynamics.target` = `float(node_def.initial)` from address space — `initial: 98.0` is a float ✓
- `_value_for_node` new case `"quality_rate"` reads from `self._quality_rate_current[node_def.id]` — populated in `_tick()` before this is called ✓
- `encode_real`/`decode_real` use `">f"` (big-endian float, 4 bytes) — S7 protocol byte order ✓
- `encode_int`/`decode_int` use `">h"` (big-endian signed short, 2 bytes) — S7 INT type ✓
- Prometheus metric names `eirvah_s7_simulator_capper_torque_nm` — Grafana queries match exactly ✓
- UNS mappings node_ids match conversion-rules.yaml node_ids match register-map aliases ✓
