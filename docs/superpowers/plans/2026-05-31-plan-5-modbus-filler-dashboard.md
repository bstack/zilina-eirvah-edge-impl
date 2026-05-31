# Plan 5 — Modbus Filler Unit + Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Repurpose the Modbus simulator to represent a Filler unit (distinct from the OPC UA Bottler), wire its signals through the full pipeline, and add a Filler row to the Bottling Line State Grafana dashboard — making protocol-agnosticism visible in a single screenshot.

**Architecture:** The Modbus simulator currently mirrors the OPC UA bottler's signals. This plan replaces its register layout with three filler-specific signals (fill level %, motor state, throughput), updates the downstream config files so the converter and contextualizer understand the new `Filler.*` node IDs, and adds a dashboard row querying the Modbus simulator's Prometheus metrics directly (same pattern as the existing Bottler row). No new services; no changes to the OPC UA path.

**Tech Stack:** Python 3.12, pymodbus 3.7.x, Prometheus, Grafana, Kustomize, kind.

**Spec reference:** Extends Plan 4 (`docs/superpowers/plans/2026-05-31-plan-4-modbus-second-slice.md`).

---

## Filler register layout (new)

| Address | Signal | Scale | Default raw | Decoded value |
|---|---|---|---|---|
| 0 | `fill_level_percent` | ×10 | 750 | 75.0 % |
| 1 | `motor_state` | ×1 | 1 | 1 = running |
| 2 | `throughput_bps` | ×100 | 80 | 0.80 bottles/s |

## ISA-95 UNS paths produced (new)

| node_id | UNS topic |
|---|---|
| `Filler.FillLevelSensor01` | `uniza/zilina/factory1/line_a/filler/fill_level_sensor_01/fill_level` |
| `Filler.Motor01.State` | `uniza/zilina/factory1/line_a/filler/motor_01/state` |
| `Filler.ThroughputMeter01` | `uniza/zilina/factory1/line_a/filler/throughput_meter_01/throughput` |

## Files modified by this plan

```
services/modbus-simulator/src/modbus_simulator/server.py    MODIFY  RegisterBlock + tick + _tick_loop
services/modbus-simulator/src/modbus_simulator/metrics.py   MODIFY  replace temperature/setpoint with fill_level/throughput
services/modbus-simulator/tests/test_modbus_simulator.py    MODIFY  update tests for new fields

deploy/k3s/base/modbus-data-subscriber/modbus-register-map.yaml    MODIFY  Filler.* aliases
deploy/k3s/base/data-converter/conversion-rules.yaml                MODIFY  add 3 Filler rules
deploy/k3s/base/uns-auto-contextualizer/opcua-node-to-uns-mapping.yaml  MODIFY  add 3 Filler mappings

deploy/grafana/dashboards/bottling-line-state.json          MODIFY  add Filler row + 3 panels
```

---

## Task 1: Update modbus-simulator to Filler signals

**Files:**
- Modify: `services/modbus-simulator/src/modbus_simulator/server.py`
- Modify: `services/modbus-simulator/src/modbus_simulator/metrics.py`
- Modify: `services/modbus-simulator/tests/test_modbus_simulator.py`

- [ ] **Step 1: Update the tests first (they define the new contract)**

Replace the content of `services/modbus-simulator/tests/test_modbus_simulator.py`:

```python
from __future__ import annotations

import pytest
from modbus_simulator.server import RegisterBlock, scale_to_register, register_to_scale


def test_scale_to_register_temperature() -> None:
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


def test_register_block_as_list() -> None:
    block = RegisterBlock(fill_level_raw=800, motor_state=1, throughput_raw=90)
    assert block.as_list() == [800, 1, 90]


def test_register_block_tick_changes_fill_level() -> None:
    import random
    rng = random.Random(42)
    block = RegisterBlock()
    block.tick(rng=rng, delta_max=20)
    assert 500 <= block.fill_level_raw <= 950


def test_register_block_tick_clamps_at_floor() -> None:
    import random
    rng = random.Random(0)
    block = RegisterBlock(fill_level_raw=500)
    for _ in range(20):
        block.tick(rng=rng, delta_max=200)
    assert block.fill_level_raw >= 500


def test_register_block_tick_clamps_at_ceiling() -> None:
    import random
    rng = random.Random(0)
    block = RegisterBlock(fill_level_raw=950)
    for _ in range(20):
        block.tick(rng=rng, delta_max=200)
    assert block.fill_level_raw <= 950
```

- [ ] **Step 2: Run — verify FAIL**

```bash
uv run pytest services/modbus-simulator/tests/test_modbus_simulator.py -v 2>&1 | tail -15
```

Expected: `AssertionError` on `test_register_block_defaults` (block still has `temperature_raw`).

- [ ] **Step 3: Update `server.py` — RegisterBlock and tick**

In `services/modbus-simulator/src/modbus_simulator/server.py`, replace the `RegisterBlock` dataclass and `_tick_loop` method. The imports and helpers (`scale_to_register`, `register_to_scale`, `SimulatorRuntime`) stay the same.

Replace the `RegisterBlock` class:

```python
@dataclass
class RegisterBlock:
    fill_level_raw: int = 750    # 75.0% (scale ×10)
    motor_state: int = 1          # running
    throughput_raw: int = 80      # 0.80 bottles/s

    def as_list(self) -> list[int]:
        return [self.fill_level_raw, self.motor_state, self.throughput_raw]

    def tick(self, *, rng: random.Random, delta_max: int = 20) -> None:
        span = abs(delta_max)
        delta = rng.randint(-span, span)
        self.fill_level_raw = max(500, min(950, self.fill_level_raw + delta))
```

Replace `_build_context` to use 3 registers (was 4 + 6 padding, now 3 + 7 padding):

```python
    def _build_context(self) -> ModbusServerContext:
        store = ModbusSlaveContext(
            hr=ModbusSequentialDataBlock(0, self._block.as_list() + [0] * 7),
        )
        return ModbusServerContext(slaves={self._settings.unit_id: store}, single=False)
```

Replace `_tick_loop` to use filler metrics:

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
            self._metrics.set_fill_level(
                register_to_scale(self._block.fill_level_raw, scale=10)
            )
            self._metrics.set_motor_state(self._block.motor_state)
            self._metrics.set_throughput(
                register_to_scale(self._block.throughput_raw, scale=100)
            )
```

- [ ] **Step 4: Update `metrics.py` — replace temperature/setpoint with fill_level/throughput**

Replace the full content of `services/modbus-simulator/src/modbus_simulator/metrics.py`:

```python
"""Prometheus metrics for the Modbus filler simulator."""
from __future__ import annotations

from eirvah_observability.metrics import make_gauge
from prometheus_client.registry import REGISTRY, CollectorRegistry


class SimulatorMetrics:
    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self._fill_level = make_gauge(
            "modbus_simulator_fill_level_percent",
            "Current fill level from Modbus filler simulator (%).",
            labelnames=[],
            registry=registry,
        )
        self._motor_state = make_gauge(
            "modbus_simulator_motor_state",
            "Motor state: 0=stopped 1=running.",
            labelnames=[],
            registry=registry,
        )
        self._throughput = make_gauge(
            "modbus_simulator_throughput_bottles_per_second",
            "Current throughput from Modbus filler simulator (bottles/s).",
            labelnames=[],
            registry=registry,
        )

    def set_fill_level(self, value: float) -> None:
        self._fill_level.set(value)

    def set_motor_state(self, value: int) -> None:
        self._motor_state.set(value)

    def set_throughput(self, value: float) -> None:
        self._throughput.set(value)
```

- [ ] **Step 5: Run tests — verify ALL 8 PASS**

```bash
uv run pytest services/modbus-simulator/tests/test_modbus_simulator.py -v
```

Expected: `8 passed`.

- [ ] **Step 6: Commit**

```bash
git add services/modbus-simulator/
git commit -m "feat(modbus-simulator): repurpose as filler unit — fill level, motor, throughput"
```

---

## Task 2: Update pipeline config files

**Files:**
- Modify: `deploy/k3s/base/modbus-data-subscriber/modbus-register-map.yaml`
- Modify: `deploy/k3s/base/data-converter/conversion-rules.yaml`
- Modify: `deploy/k3s/base/uns-auto-contextualizer/opcua-node-to-uns-mapping.yaml`

No tests for these YAML files — correctness verified by the e2e test in Task 4.

- [ ] **Step 1: Replace `modbus-register-map.yaml`**

```yaml
# Register map for modbus-data-subscriber — Filler unit (Modbus TCP)
# Addresses 0-2; scale converts raw integer register to physical value.
host: modbus-simulator
port: 5020
unit_id: 1
poll_interval_ms: 500
registers:
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
```

- [ ] **Step 2: Add Filler rules to `conversion-rules.yaml`**

Append these three entries to the `rules:` list in `deploy/k3s/base/data-converter/conversion-rules.yaml`:

```yaml
  - node_id: "Filler.FillLevelSensor01"
    value_type: double
    unit: percent
    drop_bad_quality: false

  - node_id: "Filler.Motor01.State"
    value_type: int64
    unit: dimensionless
    drop_bad_quality: false

  - node_id: "Filler.ThroughputMeter01"
    value_type: double
    unit: "bottle/s"
    drop_bad_quality: false
```

- [ ] **Step 3: Add Filler mappings to `opcua-node-to-uns-mapping.yaml`**

Append these three entries to the `mappings:` list in `deploy/k3s/base/uns-auto-contextualizer/opcua-node-to-uns-mapping.yaml`:

```yaml
  - node_id: "Filler.FillLevelSensor01"
    area: factory1
    line: line_a
    cell: filler
    equipment: fill_level_sensor_01
    measurement: fill_level
    semantic_type: level.percent

  - node_id: "Filler.Motor01.State"
    area: factory1
    line: line_a
    cell: filler
    equipment: motor_01
    measurement: state
    semantic_type: state.enum

  - node_id: "Filler.ThroughputMeter01"
    area: factory1
    line: line_a
    cell: filler
    equipment: throughput_meter_01
    measurement: throughput
    semantic_type: flow.bps
```

- [ ] **Step 4: Commit**

```bash
git add deploy/k3s/base/modbus-data-subscriber/modbus-register-map.yaml \
        deploy/k3s/base/data-converter/conversion-rules.yaml \
        deploy/k3s/base/uns-auto-contextualizer/opcua-node-to-uns-mapping.yaml
git commit -m "feat(config): add Filler unit mappings for Modbus pipeline path"
```

---

## Task 3: Add Filler row to Bottling Line State dashboard

The dashboard currently has 6 panels (IDs 1–6) ending at y=14. Add a row separator (ID 10) and three Filler panels (IDs 11–13) below it. The panels query the Modbus simulator's Prometheus metrics directly — same pattern as the existing Bottler panels querying `eirvah_simulator_*`.

**Files:**
- Modify: `deploy/grafana/dashboards/bottling-line-state.json`

- [ ] **Step 1: Add the Filler row + panels to `bottling-line-state.json`**

In `deploy/grafana/dashboards/bottling-line-state.json`, find the `"panels": [` array. After the last panel (the setpoint writes table, ID 6), add a comma after its closing `}` and append:

```json
    {
      "id": 10,
      "type": "row",
      "title": "Filler Unit (Modbus TCP)",
      "gridPos": { "x": 0, "y": 14, "w": 24, "h": 1 },
      "collapsed": false
    },
    {
      "id": 11,
      "type": "timeseries",
      "title": "Fill level (%)",
      "gridPos": { "x": 0, "y": 15, "w": 12, "h": 8 },
      "targets": [
        {
          "expr": "eirvah_modbus_simulator_fill_level_percent",
          "legendFormat": "fill level",
          "refId": "A"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "percent",
          "min": 0,
          "max": 100,
          "thresholds": {
            "mode": "absolute",
            "steps": [
              { "value": null, "color": "red" },
              { "value": 20, "color": "yellow" },
              { "value": 50, "color": "green" }
            ]
          }
        }
      }
    },
    {
      "id": 12,
      "type": "stat",
      "title": "Filler motor state",
      "gridPos": { "x": 12, "y": 15, "w": 6, "h": 4 },
      "targets": [
        { "expr": "eirvah_modbus_simulator_motor_state", "refId": "A" }
      ],
      "fieldConfig": {
        "defaults": {
          "mappings": [
            { "type": "value", "options": {
              "0": { "text": "stopped", "color": "gray"  },
              "1": { "text": "running", "color": "green" }
            }}
          ]
        }
      }
    },
    {
      "id": 13,
      "type": "timeseries",
      "title": "Filler throughput (bottles/s)",
      "gridPos": { "x": 18, "y": 15, "w": 6, "h": 4 },
      "targets": [
        {
          "expr": "eirvah_modbus_simulator_throughput_bottles_per_second",
          "legendFormat": "throughput",
          "refId": "A"
        }
      ]
    }
```

- [ ] **Step 2: Verify the JSON is valid**

```bash
python3 -c "import json; json.load(open('deploy/grafana/dashboards/bottling-line-state.json')); print('JSON valid')"
```

Expected: `JSON valid`

- [ ] **Step 3: Commit**

```bash
git add deploy/grafana/dashboards/bottling-line-state.json
git commit -m "feat(grafana): add Filler unit row to Bottling Line State dashboard"
```

---

## Task 4: Rebuild, deploy, verify

- [ ] **Step 1: Build new modbus-simulator image**

```bash
docker build --file services/modbus-simulator/Dockerfile --tag modbus-simulator:local .
```

Expected: build completes with no errors.

- [ ] **Step 2: Load into kind cluster**

```bash
kind load docker-image modbus-simulator:local --name eirvah-edge
```

- [ ] **Step 3: Apply updated configmaps (converter, contextualizer, grafana, register map)**

```bash
kubectl apply -k deploy/k3s/overlays/local
```

This is idempotent — it updates all ConfigMaps (conversion-rules, uns-mapping, grafana dashboard, register map) without restarting pods unless their image or env changed.

- [ ] **Step 4: Restart affected deployments**

```bash
kubectl -n eirvah-edge rollout restart \
  deployment/modbus-simulator \
  deployment/modbus-data-subscriber \
  deployment/data-converter \
  deployment/uns-auto-contextualizer

kubectl -n eirvah-edge rollout status deployment/modbus-simulator --timeout=60s
kubectl -n eirvah-edge rollout status deployment/modbus-data-subscriber --timeout=60s
kubectl -n eirvah-edge rollout status deployment/data-converter --timeout=60s
kubectl -n eirvah-edge rollout status deployment/uns-auto-contextualizer --timeout=60s
```

Expected: all four show `successfully rolled out`.

- [ ] **Step 5: Verify Modbus metrics appear in Prometheus**

```bash
kubectl -n eirvah-edge port-forward svc/prometheus 9090:9090 &>/tmp/pf-prometheus.log &
sleep 2
curl -s "http://localhost:9090/api/v1/query?query=eirvah_modbus_simulator_fill_level_percent" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['data']['result'])"
```

Expected: one result with a value ~75 (fill level percentage).

- [ ] **Step 6: Restart Grafana to reload dashboard configmap**

```bash
kubectl -n eirvah-edge rollout restart deployment/grafana
kubectl -n eirvah-edge rollout status deployment/grafana --timeout=60s
kubectl -n eirvah-edge port-forward svc/grafana 3000:3000 &>/tmp/pf-grafana.log &
```

- [ ] **Step 7: Run e2e tests**

```bash
kubectl -n eirvah-edge port-forward svc/nats 4222:4222 &>/tmp/pf-nats.log &
sleep 2
uv run pytest tests/e2e/test_modbus_path.py -v
```

Expected: both tests PASS. The aliases are now `Filler.*` — update the e2e test's `EXPECTED_ALIASES` set to match:

```python
EXPECTED_ALIASES = {
    "Filler.FillLevelSensor01",
    "Filler.Motor01.State",
    "Filler.ThroughputMeter01",
}
```

Edit `tests/e2e/test_modbus_path.py` to use these aliases, then re-run.

- [ ] **Step 8: Open Grafana and screenshot the Filler row**

Navigate to `http://localhost:3000` → Dashboards → EirVah → Bottling Line State.

Expected: two rows visible:
- **Bottler (OPC UA)** — temperature/setpoint timeseries, motor state, RPM, throughput
- **Filler Unit (Modbus TCP)** — fill level timeseries, motor state stat (green "running"), throughput

This is the proposal artefact: same pipeline, two protocols, one dashboard.

- [ ] **Step 9: Commit e2e test update**

```bash
git add tests/e2e/test_modbus_path.py
git commit -m "fix(e2e): update Modbus aliases to Filler.* after simulator repurpose"
```

---

## Self-review

**Spec coverage:**
- Filler register layout (3 signals) ✓ Task 1
- Metrics renamed to fill_level/throughput ✓ Task 1
- Register map → Filler.* aliases ✓ Task 2
- Conversion rules for Filler signals ✓ Task 2
- UNS mappings for Filler → cell=filler ✓ Task 2
- Dashboard Filler row with 3 panels ✓ Task 3
- Deploy + verify Prometheus metrics ✓ Task 4
- e2e tests updated to new aliases ✓ Task 4

**Placeholder scan:** None found.

**Type consistency:**
- `RegisterBlock.fill_level_raw` used in `as_list()[0]`, `tick()`, `_tick_loop` metrics call, and tests — consistent ✓
- `metrics.set_fill_level()` called in `_tick_loop` — matches `SimulatorMetrics.set_fill_level()` ✓
- `modbus-register-map.yaml` address 0 → `Filler.FillLevelSensor01` — matches `opcua-node-to-uns-mapping.yaml` and `conversion-rules.yaml` node_id ✓
- Prometheus metric `eirvah_modbus_simulator_fill_level_percent` queried in dashboard panel ID 11 — matches metric name in `metrics.py` (make_gauge prepends `eirvah_`) ✓
