# Plan 3 — Actuation Path Design

**Status:** Approved for implementation
**Date:** 2026-05-18
**Author:** William Francis Stack
**Spec reference:** `docs/superpowers/specs/2026-05-16-eirvah-edge-vertical-slice-design.md` §§ 3.2, 3.3, 4.3, 4.4

---

## 1. Goal

Complete the vertical slice by implementing the actuation path — the second half of the CPS feedback loop specified in the EirVah edge design.

After Plan 3, the system runs end-to-end:

```
temperature spike
  → decision-agent-stub detects threshold
  → AMQP actuation request
  → actuation-event-validator approves
  → actuation-signal-publisher writes OPC UA setpoint
  → opcua-simulator setpoint changes
  → next telemetry reading reflects new value
  → loop closed
```

Plans 1 and 2 built the simulator, shared libraries, and telemetry path (7 pods). Plan 3 adds 6 pods and closes the loop.

---

## 2. Pod inventory (new in this plan)

| Service | NATS subject | Role |
|---|---|---|
| `amqp-actuation-event-subscriber` | pub: `act.ingress.requested` | Consumes RabbitMQ queue → NATS |
| `actuation-control-orchestrator` | sub: `act.ingress.requested` | Pipeline owner |
| `actuation-event-validator` | req/rep: `act.work.validate` | Policy validation worker |
| `actuation-signal-publisher` | req/rep: `act.work.write_signal` | OPC UA write worker |
| `decision-agent-stub` | MQTT sub → AMQP pub | Loop-closer |
| *(config only)* | — | `actuation-policy.yaml`, `actuation-control.yaml` |

---

## 3. Data flow

```
RabbitMQ queue: eirvah.actuation.requests  (durable, persistent)
  │ AMQP consume (prefetch 1)
  ▼
amqp-actuation-event-subscriber
  │ NATS pub: act.ingress.requested
  │ AMQP ack only after successful NATS publish (at-least-once into edge)
  ▼
actuation-control-orchestrator  [pipeline owner]
  │ NATS req/rep: act.work.validate  (timeout from pipeline YAML)
  ▼
actuation-event-validator
  returns: { decision: "approve"|"reject", reason? }
  │
  ├─ reject → emit act.dlq.rejected + AMQP exchange eirvah.actuation.results
  │
  └─ approve → (only if allow_writes == true)
       │ NATS req/rep: act.work.write_signal
       ▼
     actuation-signal-publisher
       │ resolve UNS topic → node_id (inverted mapping ConfigMap)
       │ asyncua Write session
       ▼
     opcua-simulator (setpoint node updated)
       │ DataChange subscription fires
       ▼
     opcua-data-subscriber → telemetry path → MQTT  (loop closed)

allow_writes == false:  short-circuit after validate; emit reject with reason "writes_disabled"
```

---

## 4. Component contracts

### 4.1 `amqp-actuation-event-subscriber`

- Connects to RabbitMQ, subscribes to `eirvah.actuation.requests` (durable queue).
- Wraps each AMQP delivery in `NATSEnvelope[ActuationRequest]`, publishes to `act.ingress.requested`.
- Acks AMQP delivery only after successful NATS publish.
- Reconnects with exponential backoff on disconnect.
- Config: `AMQP_URL`, `AMQP_QUEUE`, `AMQP_PREFETCH` (default 1), `NATS_SERVERS`.

### 4.2 `actuation-control-orchestrator`

- Queue-group consumer on `act.ingress.requested`.
- Drives pipeline from `config/pipelines/actuation-control.yaml`: stages, timeouts, retry policy.
- Checks `deadline` field; rejects with `expired` if now > deadline.
- Feature flag `ALLOW_WRITES` (default `false`): when false, skips write stage and emits rejection.
- On approve+write: drives `act.work.write_signal`.
- On reject (any reason): publishes to `act.dlq.rejected` and AMQP exchange `eirvah.actuation.results`.
- Emits per-stage latency histograms and outcome counters (Prometheus).

### 4.3 `actuation-event-validator`

- Stateless NATS req/rep worker on `act.work.validate`.
- Reads `config/actuation-policy.yaml` at startup: per-node `allowed_range` and `allowlist`.
- Validates: (a) target UNS topic resolves to a known writable node, (b) `requested_value` within `allowed_range`, (c) `requester` in `allowlist`.
- Returns `{ decision, reason }`. Any validation failure → `reject`.
- Config: `NATS_SERVERS`, `POLICY_PATH`.

### 4.4 `actuation-signal-publisher`

- Stateless NATS req/rep worker on `act.work.write_signal`.
- Loads `config/opcua-node-to-uns-mapping.yaml` and inverts it at startup (UNS topic → node_id). Fails fast if mapping is not bijective.
- Opens asyncua write session to OPC UA endpoint, performs the write, returns `ok` or `error`.
- Config: `NATS_SERVERS`, `OPCUA_ENDPOINT`, `MAPPING_PATH`.

### 4.5 `decision-agent-stub`

- Connects to Mosquitto with MQTT credentials.
- Subscribes to `uniza/zilina/factory1/line_a/bottler/temperature_sensor_01/temperature`.
- Maintains a sliding window: if value > `THRESHOLD` (default 26.0) for `TRIGGER_DURATION_S` (default 30) seconds, publishes one actuation request to RabbitMQ exchange `eirvah.actuation.requests`.
- Reuses the telemetry `correlation_id` from the triggering measurement.
- Targets `uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature` with `requested_value = SETPOINT_TARGET` (default 22.0).
- Enforces a cooldown (`COOLDOWN_S`, default 60) before firing again.
- Config: `MQTT_BROKER`, `MQTT_USER`, `MQTT_PASSWORD`, `AMQP_URL`, `THRESHOLD`, `TRIGGER_DURATION_S`, `SETPOINT_TARGET`, `COOLDOWN_S`.

---

## 5. Configuration files (new)

### `config/actuation-policy.yaml`

Per-node policy table consumed by `actuation-event-validator`:

```yaml
policies:
  - uns_topic: "uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature"
    allowed_range: [20.0, 30.0]
    allowlist:
      - decision-agent-stub
```

### `config/pipelines/actuation-control.yaml`

Pipeline definition consumed by `actuation-control-orchestrator`:

```yaml
pipeline:
  name: actuation-control
  stages:
    - name: validate
      subject: act.work.validate
      timeout_s: 2.0
    - name: write_signal
      subject: act.work.write_signal
      timeout_s: 5.0
```

---

## 6. E2e tests

Extends `tests/e2e/conftest.py` with an `actuation_cluster` fixture that patches `ALLOW_WRITES=true` on the orchestrator deployment, waits for rollout, yields, then restores the original env.

| Test | `allow_writes` | Assertion |
|---|---|---|
| `test_actuation_full_loop` | `true` | Setpoint node value in simulator changes; AMQP result has `decision: approve` |
| `test_actuation_rejection_policy` | `true` | Value outside policy range → result has `decision: reject`, reason contains "outside policy range" |
| `test_actuation_rejection_writes_disabled` | `false` (default) | Any request → result has `decision: reject`, reason `writes_disabled` |
| `test_actuation_deadline_expired` | `true` | Request with past `deadline` → result has `decision: reject`, reason `expired` |

---

## 7. Observability

**Grafana:** Add an "Actuation Path" panel row to `deploy/grafana/dashboards/eirvah-edge-pipeline.json`:
- Actuation requests received / approved / rejected (counters, by reason)
- Per-stage latency histogram (validate, write_signal)
- Dead-letter queue depth

**Prometheus:** All four new services expose `/metrics` on `:8080` via `eirvah-observability`.

---

## 8. ADRs

### ADR 0001 — Actuation Safety Gate (`docs/adr/0001-actuation-safety-gate.md`)

**Context:** CPS writes carry physical risk. The actuation path needs to run safely in any environment (dev laptop, CI, lab testbed) without risk of unintended device writes.

**Decision:** Two-layer guard — (1) `ALLOW_WRITES` feature flag on the orchestrator (default `false`); (2) `actuation-event-validator` policy bounds check (value range + allowlist) that runs regardless of the flag. Neither layer alone is sufficient: the flag prevents writes in safe environments; the validator prevents unsafe writes in environments where the flag is enabled.

**Consequence:** Full-loop e2e tests require explicit `ALLOW_WRITES=true`. CI defaults to `false`. Lab and production overlays set the flag explicitly and deliberately.

### ADR 0002 — Reverse Mapping via Shared ConfigMap (`docs/adr/0002-reverse-mapping-shared-configmap.md`)

**Context:** `actuation-signal-publisher` needs to resolve a UNS topic back to an OPC UA `node_id`. Two options: (a) maintain a separate reverse mapping file; (b) invert the existing `opcua-node-to-uns-mapping.yaml` at runtime.

**Decision:** Invert at runtime (option b). Single source of truth prevents node_id/UNS drift. Both services mount the same ConfigMap.

**Consequence:** The mapping must be bijective — each UNS topic maps to exactly one node_id and vice versa. `actuation-signal-publisher` enforces this at startup and fails fast with a clear error if duplicates are found. This is a safe constraint for the slice (one bottling line, no repeated measurements).

---

## 9. File structure produced by this plan

```
config/
├── actuation-policy.yaml                          # NEW
└── pipelines/
    └── actuation-control.yaml                     # NEW

services/
├── amqp-actuation-event-subscriber/
│   ├── pyproject.toml
│   ├── Dockerfile
│   ├── src/amqp_actuation_event_subscriber/
│   │   ├── __init__.py
│   │   ├── __main__.py
│   │   ├── config.py
│   │   └── service.py
│   └── tests/test_amqp_actuation_event_subscriber.py
├── actuation-control-orchestrator/
│   ├── pyproject.toml
│   ├── Dockerfile
│   ├── src/actuation_control_orchestrator/
│   │   ├── __init__.py
│   │   ├── __main__.py
│   │   ├── config.py
│   │   ├── models.py
│   │   ├── metrics.py
│   │   ├── pipeline.py
│   │   └── service.py
│   └── tests/test_actuation_control_orchestrator.py
├── actuation-event-validator/
│   ├── pyproject.toml
│   ├── Dockerfile
│   ├── src/actuation_event_validator/
│   │   ├── __init__.py
│   │   ├── __main__.py
│   │   ├── config.py
│   │   └── service.py
│   └── tests/test_actuation_event_validator.py
├── actuation-signal-publisher/
│   ├── pyproject.toml
│   ├── Dockerfile
│   ├── src/actuation_signal_publisher/
│   │   ├── __init__.py
│   │   ├── __main__.py
│   │   ├── config.py
│   │   └── service.py
│   └── tests/test_actuation_signal_publisher.py
└── decision-agent-stub/
    ├── pyproject.toml
    ├── Dockerfile
    ├── src/decision_agent_stub/
    │   ├── __init__.py
    │   ├── __main__.py
    │   ├── config.py
    │   └── service.py
    └── tests/test_decision_agent_stub.py

deploy/k3s/base/
├── amqp-actuation-event-subscriber/  # NEW  Deployment + Service
├── actuation-control-orchestrator/   # NEW
├── actuation-event-validator/        # NEW
├── actuation-signal-publisher/       # NEW
├── decision-agent-stub/              # NEW
└── kustomization.yaml                # MODIFY  add 5 new dirs

deploy/grafana/dashboards/
└── eirvah-edge-pipeline.json         # MODIFY  add actuation panel row

docs/adr/
├── 0001-actuation-safety-gate.md     # NEW
└── 0002-reverse-mapping-shared-configmap.md  # NEW

tests/e2e/
└── test_actuation.py                 # NEW  4 tests

pyproject.toml                        # MODIFY  add 5 new workspace members
scripts/build_all.sh                  # MODIFY  add 5 services
```
