# EirVah Edge — Vertical Slice Design

**Status:** Draft for review
**Date:** 2026-05-16
**Author:** William Francis Stack
**Scope:** First vertical slice of the EirVah Edge Integration Layer (`eirvah-edge-code` repo)

---

## 1. Purpose and scope

This document specifies the **first vertical slice** of the EirVah edge code: a minimal but complete CPS feedback loop running on k3s.

The slice's purpose is to prove the EirVah Edge Integration Layer architecture end-to-end against a single industrial protocol, before any horizontal extension (more protocols, more devices, more replicas, more deployment models). It is the reference implementation that every subsequent experiment in the proposal (baseline, scalability, failure and recovery) runs against.

### 1.1 In scope

A single CPS feedback loop covering both messaging paradigms:

- **Telemetry path** — simulated PLC → OPC UA data subscriber → data converter → UNS auto-contextualizer → MQTT UNS publisher → external consumer.
- **Actuation path** — external consumer → AMQP actuation request → AMQP actuation event subscriber → actuation event validator → actuation signal publisher → OPC UA write back to the simulated PLC.

Plus the supporting infrastructure to operate and observe it: a custom OPC UA simulator, NATS as the internal edge bus, Mosquitto (MQTT), RabbitMQ (AMQP), Prometheus, Grafana, and a `decision-agent-stub` that closes the loop.

All components are open source, run as containers on k3s, and are configured declaratively via Kustomize overlays.

### 1.2 Out of scope (deliberate, defended)

- Multi-protocol adapters (Modbus, Siemens S7) — second slice.
- Multi-replica scaling — exercised by the scalability scenario, not the slice itself.
- Persistence (NATS JetStream, MQTT retained messages, time-series DB, relational DB) — belongs to the Data and Persistence Layer, separate repo.
- Authentication / authorization beyond basic broker credentials — security-focused later slice.
- Distributed tracing (OpenTelemetry, Jaeger, Tempo) — correlation IDs in logs are enough for the slice.
- Centralized log aggregation (Loki, Elastic) — `kubectl logs` and the trace script suffice.
- Multi-node k3s clusters — part of the deployment-model-comparison experiment, not the slice.
- CI/CD pipelines — the slice must be laptop-runnable first; CI is its own slice.
- The Decision and Analytics Layer — `decision-agent-stub` stands in only to close the loop.
- External enterprise gateways (ERP, MES, TMS) — Data Layer.
- Edge offline / store-and-forward operation — failure scenario, not slice feature.

### 1.3 Decisions baked into this spec

| Topic | Decision |
|---|---|
| First industrial protocol | OPC UA |
| Language | Python 3.12 (asyncio + asyncua) |
| Simulator | Custom asyncua server in this repo |
| Slice scope | Full CPS loop: telemetry + actuation |
| UNS conventions | Strict 7-level ISA-95 hierarchy + JSON payloads |
| Deployment | k3s from day 1 (single-node k3d for `local`, single-node k3s for `lab`) |
| Observability | Prometheus + Grafana from day 1; no tracing/log aggregation yet |
| Service granularity | Fine-grained: each named component in Fig 4.2 is its own pod |
| Orchestration pattern | Orchestrators own pipeline state; workers are stateless NATS request-reply services |
| Internal bus | NATS |
| Licensing | OSI-approved open source only |

---

## 2. Architectural overview

### 2.1 The slice as a CPS feedback loop

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                           k3s namespace: eirvah-edge                            │
│                                                                                 │
│  ┌──────────────┐                                                               │
│  │ opcua-       │  OPC UA Server (asyncua, ISA-95-shaped address space)         │
│  │ simulator    │                                                               │
│  └──────┬───────┘                                                               │
│         │ OPC UA                                                                │
│         ▼                                                                       │
│  ┌──────────────────┐                                                           │
│  │ opcua-data-      │  → NATS:  uns.ingress.raw                                 │
│  │ subscriber       │                                                           │
│  └──────────┬───────┘                                                           │
│             │                                                                   │
│             ▼  (entry-point subject)                                            │
│  ┌────────────────────────────────────────────────────────┐                     │
│  │ uns-contextualizer-orchestrator   [pipeline owner]      │                    │
│  │                                                         │                    │
│  │   for each msg on uns.ingress.raw:                      │                    │
│  │     normalized   = call worker: uns.work.convert        │                    │
│  │     contextual'd = call worker: uns.work.contextualize  │                    │
│  │     publish      : call worker: uns.work.publish        │                    │
│  │   emit pipeline metrics, handle errors, dead-letter     │                    │
│  └────┬─────────────────┬─────────────────────┬────────────┘                    │
│       │ NATS req/rep    │ NATS req/rep        │ NATS req/rep                    │
│       ▼                 ▼                     ▼                                 │
│  ┌──────────────┐  ┌─────────────────────┐  ┌────────────────┐                  │
│  │ data-        │  │ uns-auto-           │  │ mqtt-uns-      │ → Mosquitto      │
│  │ converter    │  │ contextualizer      │  │ publisher      │                  │
│  └──────────────┘  └─────────────────────┘  └────────────────┘                  │
│                                                                                 │
│  ─────────────────────────  Actuation path  ─────────────────────────────       │
│                                                                                 │
│  ┌──────────────────────────┐                                                   │
│  │ amqp-actuation-event-    │  → NATS:  act.ingress.requested                   │
│  │ subscriber               │                                                   │
│  └────────────┬─────────────┘                                                   │
│               │                                                                 │
│               ▼  (entry-point subject)                                          │
│  ┌────────────────────────────────────────────────────────┐                     │
│  │ actuation-control-orchestrator   [pipeline owner]       │                    │
│  │                                                         │                    │
│  │   for each msg on act.ingress.requested:                │                    │
│  │     decision = call worker: act.work.validate           │                    │
│  │     if decision == "approve":                           │                    │
│  │        call worker: act.work.write_signal               │                    │
│  │     else: emit rejection event, log                     │                    │
│  └────┬──────────────────────────────────┬─────────────────┘                    │
│       │ NATS req/rep                     │ NATS req/rep                         │
│       ▼                                  ▼                                      │
│  ┌─────────────────────┐              ┌────────────────────────┐                │
│  │ actuation-event-    │              │ actuation-signal-      │ → OPC UA write  │
│  │ validator           │              │ publisher              │                 │
│  └─────────────────────┘              └────────────────────────┘                │
│                                                                                 │
│  ──────────────  Loop-closer (test agent), brokers, observability  ────────     │
│                                                                                 │
│   decision-agent-stub · NATS · Mosquitto · RabbitMQ · Prometheus · Grafana       │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Orchestration pattern

Each **Orchestrator** named in the proposal (UNS Contextualizer Orchestrator, Actuation Control Orchestrator) is a real pod that **owns the pipeline state machine**. Component pods (subscribers, converters, contextualizer, publishers, validator) are stateless NATS request-reply workers driven by the orchestrator.

- Workers know only their own NATS subject and contract.
- Reordering or extending a pipeline is a change in the orchestrator's pipeline YAML, not in workers.
- Worker scaling is independent: NATS queue-group semantics let any worker run with N replicas; the orchestrator load-balances across them transparently.

### 2.3 Pod inventory (16 pods)

| Path | Pod | Role |
|---|---|---|
| (none) | `opcua-simulator` | OPC UA server with bottling-line model |
| Telemetry | `opcua-data-subscriber` | OPC UA subscription → NATS ingress |
| Telemetry | `uns-contextualizer-orchestrator` | Pipeline owner |
| Telemetry | `data-converter` | Worker: normalize signals |
| Telemetry | `uns-auto-contextualizer` | Worker: map to ISA-95 UNS path |
| Telemetry | `mqtt-uns-publisher` | Worker: publish to Mosquitto |
| Actuation | `amqp-actuation-event-subscriber` | AMQP queue → NATS ingress |
| Actuation | `actuation-control-orchestrator` | Pipeline owner |
| Actuation | `actuation-event-validator` | Worker: policy validation |
| Actuation | `actuation-signal-publisher` | Worker: OPC UA write back |
| Loop-closer | `decision-agent-stub` | Subscribes MQTT, emits AMQP actuation; closes the loop |
| Infra | `nats` | Internal bus |
| Infra | `mosquitto` | MQTT (UNS telemetry surface) |
| Infra | `rabbitmq` | AMQP (UNS event surface) |
| Observability | `prometheus` + `grafana` | Metrics scrape + dashboards (two pods) |

---

## 3. Component contracts

One paragraph per pod. Implementation details (libraries, internal class structure) live in the implementation plan, not this spec.

### 3.1 Telemetry path

**`opcua-data-subscriber`** — *Acquires raw signals.* Connects to the OPC UA simulator's endpoint, creates a Subscription with monitored items per the configured node list, and forwards each `DataChange` as a JSON envelope onto NATS subject `uns.ingress.raw`. Envelope: `{ correlation_id, source_endpoint, node_id, value, source_timestamp, server_timestamp, status_code, received_at }`. Reconnects with exponential backoff on disconnect. Config: OPC UA endpoint URL, security policy (None for the slice), list of monitored node IDs, publishing interval.

**`uns-contextualizer-orchestrator`** — *Owns the telemetry pipeline.* Queue-group consumer on `uns.ingress.raw`. For each message: generates a correlation ID if absent, then drives the stages `uns.work.convert` → `uns.work.contextualize` → `uns.work.publish`, each as a NATS request-reply with a configured timeout. On any stage failure: emits a dead-letter event on `uns.dlq.telemetry` and increments a Prometheus counter labeled by stage. Emits an end-to-end latency histogram per pipeline run. Pipeline definition (stage list, timeouts, retry policy) is YAML mounted as a ConfigMap.

**`data-converter`** — *Normalizes signal payloads.* NATS request-reply server on `uns.work.convert`. Applies unit conversion (e.g., Kelvin→Celsius), type coercion (raw int → float with scale), and configurable quality filtering. Returns the normalized envelope or `status: "error"`. Config: per-node-id conversion rules.

**`uns-auto-contextualizer`** — *Maps signals to the UNS.* NATS request-reply server on `uns.work.contextualize`. Looks up the source `node_id` in a mapping table and produces the canonical UNS path (7-level ISA-95) plus enriched payload metadata (unit, semantic type, source descriptor). Returns `{ uns_topic, payload }`. Config: node-id → UNS-path mapping table; enterprise/site identifiers from env vars.

**`mqtt-uns-publisher`** — *Publishes to the UNS over MQTT.* NATS request-reply server on `uns.work.publish`. Connects to Mosquitto with a stable client ID, publishes the payload as JSON to the resolved `uns_topic` at the configured QoS (default 1), replies `ok` once acked. Config: MQTT broker URL, credentials, client ID, QoS, retain flag.

### 3.2 Actuation path

**`amqp-actuation-event-subscriber`** — *Listens for actuation requests.* Subscribes to the `eirvah.actuation.requests` queue on RabbitMQ. Each AMQP delivery is wrapped in a JSON envelope and emitted on NATS `act.ingress.requested`. Acks the AMQP message only after a successful NATS publish (at-least-once into the edge). Config: AMQP URL, queue name, prefetch count.

**`actuation-control-orchestrator`** — *Owns the actuation pipeline.* Queue-group consumer on `act.ingress.requested`. For each message: drives `act.work.validate` (returns `approve` or `reject` with reason). On approve: drives `act.work.write_signal`. On reject: emits a rejection event on `act.dlq.rejected` and on the AMQP results exchange, then stops. Emits per-stage latency and outcome metrics. Config: pipeline YAML, plus a feature flag `allow_writes` (default `false`).

**`actuation-event-validator`** — *Decides whether an actuation request is safe.* NATS request-reply server on `act.work.validate`. Validates: (a) target UNS topic resolves to a writable OPC UA node, (b) requested value is within configured policy bounds, (c) requester is in the allowlist. Returns `{ decision: "approve"|"reject", reason? }`. Config: per-node policy table (allowed range, allowlist).

**`actuation-signal-publisher`** — *Writes the value back to the device.* NATS request-reply server on `act.work.write_signal`. Resolves the target UNS topic back to an OPC UA `node_id` (reverse of the contextualizer's mapping table — shared ConfigMap), opens a write session, performs the write, returns `ok` or `error`. Config: OPC UA endpoint URL, security policy, reverse mapping ConfigMap reference.

### 3.3 Supporting pods

**`opcua-simulator`** — *Industrial signal source and sink.* Custom asyncua server exposing the bottling-line address space (Section 9). Deterministic given a seed. Also exposes its current internal state as Prometheus gauges on the standard `:8080/metrics` endpoint — see Section 9.5 — so the device's truth is observable in Grafana independently of whether messages flowed through the EirVah pipeline. Config: address-space YAML, tick rate, seed.

**`decision-agent-stub`** — *Closes the CPS feedback loop for the slice.* Subscribes to UNS topics on Mosquitto. When the bottling-line temperature crosses a configured threshold, publishes an actuation request to RabbitMQ's `eirvah.actuation.requests` targeting the writable setpoint. The only "fake" component in the slice — it stands in for the real Decision Layer that will exist in a different repo. Config: subscribed topics, threshold rule, target UNS topic.

**`nats`** — Single-replica NATS server, no JetStream in the slice. Prometheus metrics via NATS exporter sidecar.

**`mosquitto`** — Single-replica Mosquitto. Anonymous auth disabled; basic credentials in a Secret. Prometheus metrics via `mosquitto-exporter` sidecar.

**`rabbitmq`** — Single-replica RabbitMQ with management and `rabbitmq_prometheus` plugins. Queues declared at startup via a definitions file.

**`prometheus`** — Scrapes `/metrics` from every workload pod, the three brokers, and kubelet cAdvisor. Single replica, ephemeral storage.

**`grafana`** — Two pre-provisioned dashboards (Section 6.7): "EirVah Edge Pipeline" and "Bottling Line State". Admin password from a Secret.

---

## 4. UNS schema

The public surface of the slice. Strict, versioned, and self-describing.

### 4.1 UNS topic structure

Strict 7-level ISA-95 hierarchy, mandatory:

```
{enterprise}/{site}/{area}/{line}/{cell}/{equipment}/{measurement}
```

**Rules:**
- Lowercase ASCII; allowed characters `[a-z0-9_]`. Hyphens disallowed in identifiers.
- All 7 levels present; no empty segments.
- Segment values come from the contextualizer's mapping config; UNS hierarchy is decoupled from OPC UA naming.
- `enterprise` and `site` are deployment-wide constants from env vars; remaining levels from per-node mapping.

**Examples (bottling-line slice):**
```
uniza/zilina/factory1/line_a/bottler/temperature_sensor_01/temperature
uniza/zilina/factory1/line_a/bottler/throughput_meter_01/throughput
uniza/zilina/factory1/line_a/bottler/motor_01/state
uniza/zilina/factory1/line_a/bottler/motor_01/rpm
uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature
```

The last entry is the writable setpoint that closes the actuation loop (Section 9.6).

### 4.2 Telemetry payload (MQTT) — schema v1.0

JSON, UTF-8, published with `retain=false`, QoS 1 by default.

```json
{
  "schema_version": "1.0",
  "correlation_id": "01HZXC8P9G7Q3M6V0K2T8R5W4A",
  "value": 23.4,
  "value_type": "double",
  "semantic_type": "temperature.celsius",
  "unit": "degC",
  "quality": "good",
  "uns_path": {
    "enterprise": "uniza",
    "site": "zilina",
    "area": "factory1",
    "line": "line_a",
    "cell": "bottler",
    "equipment": "temperature_sensor_01",
    "measurement": "temperature"
  },
  "source": {
    "protocol": "opcua",
    "endpoint": "opc.tcp://opcua-simulator:4840",
    "node_id": "ns=2;s=Bottler.Temperature01"
  },
  "timestamps": {
    "source": "2026-05-16T13:45:22.123456Z",
    "edge_ingress": "2026-05-16T13:45:22.150123Z",
    "edge_publish": "2026-05-16T13:45:22.152456Z"
  }
}
```

**Field rules:**
- `schema_version` — required; consumers MUST tolerate unknown additional fields within the same major version.
- `correlation_id` — required, ULID (26-char Crockford Base32, lexicographically sortable, time-prefixed). Generated by the data subscriber on first contact and propagated through every NATS hop as both a payload field and a NATS header `X-Correlation-Id`. Reappears on actuation events triggered by this measurement for end-to-end traceability without a tracing backend.
- `value` + `value_type` — v1.0 supports `double`, `int64`, `bool`, `string`. Arrays and structs are a v1.x extension.
- `semantic_type` — dotted hint, free-form in v1.0 (e.g., `temperature.celsius`, `flow.lpm`, `state.enum`, `setpoint.target`). Controlled vocabulary in a later iteration.
- `unit` — UCUM-style strings where possible (`degC`, `Cel`, `L/min`, `rpm`); `dimensionless` for booleans/states.
- `quality` — one of `good` / `uncertain` / `bad`, mapped from OPC UA `StatusCode` by the data subscriber. Bad-quality messages still flow; the converter MAY drop them only via explicit config.
- `uns_path` — denormalized copy of the path also encoded in the MQTT topic. Self-describing payloads simplify logs and dead-letter inspection.
- `source` — full protocol-level provenance.
- `timestamps` — three points: `source` (device), `edge_ingress` (subscriber), `edge_publish` (publisher). All ISO 8601 microsecond precision UTC.

**Optional extension fields a publisher MAY add (consumers MUST ignore unknown):** `tags` (free-form k/v), `lineage` (array of edge component IDs).

### 4.3 Actuation payload (AMQP) — schema v1.0

JSON, UTF-8, on RabbitMQ queue `eirvah.actuation.requests` (durable, persistent messages).

```json
{
  "schema_version": "1.0",
  "correlation_id": "01HZXC8P9G7Q3M6V0K2T8R5W4A",
  "requester": "decision-agent-stub",
  "target_uns_topic": "uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature",
  "requested_value": 22.0,
  "value_type": "double",
  "reason": "telemetry threshold breach: temperature > 26.0 for 30s",
  "requested_at": "2026-05-16T13:45:25.000000Z",
  "deadline": "2026-05-16T13:45:30.000000Z"
}
```

**Rules:**
- `correlation_id` — if triggered by an observed UNS message, the requester SHOULD reuse that message's correlation ID.
- `target_uns_topic` — resolved back to an OPC UA `node_id` via the shared reverse mapping ConfigMap.
- `deadline` — orchestrator rejects with reason `expired` if picked up after this time. Default 5 s if absent.
- `reason` — free-form human-readable; preserved in result events.

**Result events on AMQP exchange `eirvah.actuation.results`** (same envelope plus):
```json
{ "decision": "approve", "written_at": "...timestamp..." }
```
or
```json
{ "decision": "reject", "rejection_reason": "value 22.0 outside policy range [25.0, 30.0]" }
```

### 4.4 Internal NATS subjects

Not part of the public UNS surface, but documented so the boundary is explicit.

**Telemetry path:**
```
uns.ingress.raw                pub/sub   data subscriber → orchestrator (entry)
uns.work.convert               req/rep   orchestrator → data-converter
uns.work.contextualize         req/rep   orchestrator → uns-auto-contextualizer
uns.work.publish               req/rep   orchestrator → mqtt-uns-publisher
uns.dlq.telemetry              pub/sub   orchestrator → (no slice consumer)
```

**Actuation path:**
```
act.ingress.requested          pub/sub   amqp subscriber → orchestrator (entry)
act.work.validate              req/rep   orchestrator → actuation-event-validator
act.work.write_signal          req/rep   orchestrator → actuation-signal-publisher
act.dlq.rejected               pub/sub   orchestrator → (no slice consumer)
```

**Conventions:**
- `<domain>.<role>.<name>` — `domain` ∈ {`uns`,`act`}; `role` ∈ {`ingress`,`work`,`dlq`}.
- Workers join NATS queue groups named identically to their subject — scaling to N replicas just works.
- Correlation ID travels as both payload field and NATS header.

---

## 5. Repo layout and developer workflow

### 5.1 Monorepo, not polyrepo

One repo for all 16 pods plus IaC. Wire contracts (telemetry payload, actuation payload, NATS envelope, UNS topic helpers) are shared by 10+ services; monorepo prevents contract drift, keeps schema changes atomic, and pins the whole stack to one commit for experiment reproducibility.

### 5.2 Top-level layout

```
eirvah-edge-code/
├── README.md
├── CLAUDE.md
├── pyproject.toml                  # uv workspace root
├── uv.lock                         # single lockfile for everything
├── .python-version                 # 3.12
├── ruff.toml · mypy.ini · pytest.ini
├── docs/
│   ├── superpowers/specs/          # design docs (this one)
│   └── adr/                        # short ADRs
├── libs/                           # shared workspace libraries
│   ├── eirvah-contracts/           # pydantic models: telemetry, actuation, NATS envelope, UNS helpers
│   ├── eirvah-bus/                 # NATS client wrapper: req/rep, queue groups, headers, timeouts
│   └── eirvah-observability/       # Prometheus helpers, structlog config, /healthz, /readyz, /metrics
├── services/                       # one directory per pod
│   ├── opcua-simulator/
│   ├── opcua-data-subscriber/
│   ├── data-converter/
│   ├── uns-auto-contextualizer/
│   ├── mqtt-uns-publisher/
│   ├── uns-contextualizer-orchestrator/
│   ├── amqp-actuation-event-subscriber/
│   ├── actuation-event-validator/
│   ├── actuation-signal-publisher/
│   ├── actuation-control-orchestrator/
│   └── decision-agent-stub/
├── config/                         # mounted as ConfigMaps; one YAML per concern
│   ├── opcua-address-space.yaml
│   ├── opcua-node-to-uns-mapping.yaml
│   ├── conversion-rules.yaml
│   ├── actuation-policy.yaml
│   └── pipelines/
│       ├── uns-contextualizer.yaml
│       └── actuation-control.yaml
├── deploy/
│   ├── k3s/
│   │   ├── base/                   # per-service Kustomize bases + brokers + observability
│   │   └── overlays/
│   │       ├── local/              # single-node k3d, minimal resources
│   │       └── lab/                # experimental testbed
│   └── grafana/
│       └── dashboards/             # JSON, provisioned via ConfigMap + sidecar
├── scripts/
│   ├── dev_up.sh
│   ├── dev_down.sh
│   ├── build_all.sh
│   └── trace.sh                    # cross-pod log search by correlation_id
└── tests/
    └── e2e/                        # pytest, runs against a live k3d cluster
```

### 5.3 Per-service layout (uniform)

```
services/<name>/
├── pyproject.toml          # depends on libs/eirvah-{contracts,bus,observability}
├── Dockerfile              # multi-stage, distroless final image
├── src/<name_snake>/
│   ├── __init__.py
│   ├── __main__.py         # entry point
│   ├── config.py           # pydantic-settings, env-var-driven
│   ├── service.py          # the service class / coroutine
│   └── stages.py | handlers.py
└── tests/
    └── test_<name>.py      # pytest unit tests with mocks; no live brokers
```

### 5.4 Toolchain (all OSI-approved open source)

| Concern | Tool | License |
|---|---|---|
| Python | 3.12 | PSF |
| Packaging / venv | uv | Apache-2.0 / MIT |
| Lint + format | ruff | MIT |
| Type checking | mypy | MIT |
| Tests | pytest | MIT |
| Config / wire schemas | pydantic v2 + pydantic-settings | MIT |
| Structured logging | structlog | MIT / Apache-2.0 |
| Local k8s | k3d | Apache-2.0 |
| IaC | Kustomize | Apache-2.0 |
| Inner-loop dev | Tilt | Apache-2.0 |

**Kustomize, not Helm** — the stack is mostly static; Kustomize overlays are easier to diff between scenarios. **Tilt, not Skaffold** — Tilt's per-service UI matters when iterating across many services; Skaffold is an acceptable substitute.

### 5.5 Image strategy

- Multi-stage Dockerfile per service. Builder: `python:3.12-slim` + `uv`. Runtime: `gcr.io/distroless/python3-debian12`.
- Tags: `{service_name}:{git_short_sha}` plus a moving `:local` for dev.
- Dev loop: `k3d image import` directly into the cluster, no registry needed.
- Lab overlay introduces a registry separately; out of slice scope.

### 5.6 Developer inner loop

```bash
# One-time setup
brew install uv k3d kubectl kustomize tilt    # or platform equivalent
uv sync

# Bring up the slice locally
./scripts/dev_up.sh
# Creates k3d cluster, builds and imports images, applies deploy/k3s/overlays/local,
# prints Grafana URL when ready.

# Iterate on one service
tilt up
# Watches services/<name>/src/, rebuilds and redeploys on save. UI at http://localhost:10350.

# Trace one message end-to-end
./scripts/trace.sh 01HZXC8P9G7Q3M6V0K2T8R5W4A
# Greps every pod's structured logs for that correlation ID, prints in timestamp order.

# Run e2e tests
uv run pytest tests/e2e

# Tear down
./scripts/dev_down.sh
```

### 5.7 CI

Deferred. Slice must be laptop-runnable first; CI is a follow-up slice once the system works locally.

---

## 6. Error handling, resilience, safety

### 6.1 Per-stage failure semantics (orchestrators)

For each NATS request-reply call from an orchestrator:

- **Timeout:** default 2 s telemetry stages, 5 s actuation stages. Per-stage configurable.
- **Retry policy:** zero retries by default. Retries become a research question informed by per-stage failure-type data.
- **Three outcomes per message:**
  1. **All stages `ok`** → success. Emit end-to-end latency histogram + success counter.
  2. **Stage timeout** → emit on the domain DLQ subject with the partial message and failing stage; increment `eirvah_pipeline_stage_timeout_total{stage}`. Message not retried in slice.
  3. **Stage replies `status: "error"`** → emit on the DLQ with the worker's reason; increment `eirvah_pipeline_stage_error_total{stage,reason}`.

DLQ subjects exist with no slice consumers — observable via NATS and via Prometheus counters. Errors are visible and quantified, not auto-remediated.

### 6.2 Worker failure semantics

Workers don't retry internally. Every request handler is wrapped in try/except; caught exceptions reply `{ status: "error", error: <short class + message> }`. Each error increments `eirvah_worker_handler_error_total{kind}`.

### 6.3 Crash recovery

- All workloads are `restartPolicy: Always` Deployments, `replicas: 1` for the slice.
- Workers and orchestrators are stateless. A crash mid-request loses *that one* message and surfaces as a timeout event.
- Entry-point pods reconnect their upstreams with exponential backoff (1 s → 30 s, jittered). While disconnected, they emit `eirvah_ingress_connection_state{state="disconnected"}=1`.
- AMQP messages are acked only after successful NATS publish — at-least-once into the edge. OPC UA subscription is best-effort by protocol.
- NATS has no JetStream in the slice. A NATS restart clears in-flight pipeline messages; this is an explicit input to the failure-and-recovery evaluation, not a defect.

### 6.4 Backpressure and overload

NATS queue groups absorb pending work; if a worker can't keep up, orchestrator request-reply calls time out, surfacing as `stage_timeout` events. Ingress is capped at the protocol level: OPC UA `publishingInterval` and AMQP `prefetch`. No on-disk spooling.

### 6.5 Actuation safety

Three independent guards on the only path that can write to a device:

1. **`allow_writes` feature flag** on `actuation-control-orchestrator`. Default `false` in `local`. When disabled, the orchestrator runs validation normally but skips `write_signal`, incrementing `eirvah_actuation_writes_blocked_total`. `allow_writes: true` lives only in the `lab` overlay.
2. **Per-node policy** in the validator: every writable node has explicit `{ min, max, allowed_requesters }`. No default-allow.
3. **Validator failure-closed:** if the validator times out or errors, the orchestrator treats the outcome as *reject*. Emits `validator_unavailable_reject` for auditability.

### 6.6 Health and readiness

Every workload pod exposes on `:8080`:
- `GET /healthz` — liveness; `200` unless unrecoverable. k3s `livenessProbe` every 10 s.
- `GET /readyz` — readiness; `200` only when upstream dependencies reachable (NATS + protocol-specific). k3s `readinessProbe` every 5 s.
- `GET /metrics` — Prometheus scrape.

### 6.7 Day-1 Grafana dashboards

Two pre-provisioned dashboards ship with the slice. Both live under `deploy/grafana/dashboards/` as JSON, mounted via ConfigMap + the Grafana provisioning sidecar.

**Dashboard 1 — "EirVah Edge Pipeline".** Operational view of the EirVah pipeline itself:

- Pipeline success rate (telemetry + actuation, separate).
- End-to-end latency histogram (p50/p95/p99) from `uns.ingress.raw` to MQTT publish.
- Per-stage error/timeout rate (heatmap: stage × time).
- Connection state for OPC UA, MQTT, AMQP (0/1 gauges).
- Actuation outcome breakdown (approve / reject by reason / blocked-by-flag / validator-unavailable).
- OPC UA writeback rate (successful writes vs. validated-but-blocked).

**Dashboard 2 — "Bottling Line State".** Device-truth view sourced directly from the simulator's Prometheus gauges (Section 9.5) — independent of the pipeline:

- Temperature time-series with the current setpoint overlaid as a second line. Lets you see the actuation loop visually: when a write lands, the setpoint line jumps and the temperature line bends toward it within ~20–50 ticks.
- Setpoint stat panel showing the current value + the timestamp of the last write.
- Motor state stat panel (`stopped` / `starting` / `running` / `fault`) with colour coding.
- Motor RPM time-series.
- Throughput time-series.
- Recent setpoint writes table (timestamp, value, writer session) — exposed by the simulator as a small counter family `eirvah_simulator_setpoint_writes_total{writer}` plus an INFO-level structured log line per write.

This second dashboard is the visual answer to "show me the bottling line working". When a reviewer asks "did the actuation actually do anything?", this is the panel you point at.

### 6.8 Assumptions a thesis reviewer will probe

1. **No retries.** Every retry policy is a hypothesis about which errors are transient. Defaulting to zero and instrumenting the failure types lets the next iteration choose policies from data. Consistent with the proposal's observability-driven evaluation principle.
2. **Single-replica deployments.** Boundaries define microservices, not replica counts. Every worker is independently deployable, scalable, and replaceable. The slice deliberately runs at `replicas=1` to isolate architecture cost from concurrency effects in the baseline; the scalability scenario varies replicas as an independent variable.

---

## 7. Evaluation hookup

The slice is built so the three experimental scenarios from the proposal (§6.3) can run against it without code changes.

### 7.1 Configurable knobs (no rebuilds required)

| Knob | Where | Range |
|---|---|---|
| Simulator tick rate | `config/opcua-address-space.yaml` | 10 ms – 5 s per node |
| Number of monitored OPC UA nodes | `config/opcua-address-space.yaml` + mapping config | 1 – many |
| OPC UA `publishingInterval` | `services/.../config/opcua-data-subscriber.yaml` | 10 ms – 1 s |
| Value-generator seed, noise model | `config/opcua-address-space.yaml` | fixed per scenario |
| Per-stage timeouts | `config/pipelines/*.yaml` | sweepable |
| Per-worker replicas | overlay patches | 1 – N |
| `allow_writes` | overlay patch on orchestrator config | bool |
| Actuation policy bounds | `config/actuation-policy.yaml` | per scenario |
| Decision-agent rules | `services/.../config/decision-agent-stub.yaml` | per scenario |
| Resource requests/limits | overlay patches | per scenario |
| MQTT QoS, retain | publisher config | 0/1/2; on/off |
| AMQP prefetch | subscriber config | 1 – many |

### 7.2 Metrics that satisfy each proposal evaluation metric

| Proposal metric | Slice metric / source |
|---|---|
| Latency | `eirvah_pipeline_e2e_latency_seconds{path}`; per-stage `eirvah_pipeline_stage_latency_seconds{stage}`; consumer-side `eirvah_consumer_e2e_latency_seconds` |
| Throughput | `eirvah_pipeline_success_total{path}` (rate); `eirvah_worker_handler_total{worker,outcome}` |
| Resource utilization | `container_cpu_usage_seconds_total`, `container_memory_working_set_bytes`, `container_network_*` (kubelet cAdvisor + kube-state-metrics); plus EirVah-attributable `eirvah_message_bytes_published_total{surface}` and `eirvah_message_bytes_received_total{surface}` |
| System availability | `up{job}`; `eirvah_ingress_connection_state{ingress,state}`; MTTR derived offline |
| Message reliability | `eirvah_pipeline_success_total{path="telemetry"}` vs `eirvah_consumer_received_total`; actuation: `eirvah_actuation_requested_total` vs `eirvah_actuation_written_total` |
| Cost efficiency | Composite from utilization × throughput; rate-card multiplication is offline analysis |
| Reproducibility | Pinned image SHAs, `uv.lock`, fixed simulator seed, scenario YAML in overlays |

**Net new instrumentation required beyond what Sections 2–6 already imply:**
1. `eirvah_consumer_received_total` and `eirvah_consumer_e2e_latency_seconds` on `decision-agent-stub`.
2. `eirvah_message_bytes_{published,received}_total{surface}` on MQTT publisher, AMQP subscriber, and `decision-agent-stub`.
3. `eirvah_actuation_requested_total` and `eirvah_actuation_written_total` (rename of the actuation success counter).

### 7.3 Per-scenario hookup

**Baseline scenario.** `lab` overlay, all replicas at 1. Simulator: 5 monitored nodes, 100 ms publishing interval (~50 msg/s). `allow_writes: true`. Decision agent tuned so actuation fires every ~30 s. Duration: 10 minutes; first 60 s discarded as warm-up.

**Scalability scenario.** Two independent axes:
- Device count: 5 → 25 → 100 → 500 monitored nodes (address-space generated by script).
- Publishing rate: 100 ms → 50 ms → 20 ms → 10 ms.
- Optional third axis: replica counts on the worker identified as the bottleneck.

Output: the load level at which p99 latency exceeds a threshold or error rate becomes non-zero.

**Failure and recovery scenario.** Deterministic injection via `kubectl`:
- Worker kill (`kubectl delete pod data-converter-...`) — measure timeout spike and recovery time.
- Internal-bus kill (`kubectl delete pod nats-...`) — measure full-pipeline stop, time to first new success.
- External-broker kill (Mosquitto, RabbitMQ separately) — measure reconnect time, message loss observed at the consumer.

Output: data to choose retry / circuit-breaker / persistence policies for a later iteration.

### 7.4 Cost evaluation

The slice provides accurate utilization data; rate-card translation is an offline spreadsheet exercise.

| Cost component | Source |
|---|---|
| Compute (CPU + memory) | Per-pod cAdvisor × scenario duration |
| Storage | Not exercised in slice |
| Networking | `eirvah_message_bytes_*` + node-level `container_network_*` |
| Operational overhead | Out of runtime; measured separately |

### 7.5 Reproducibility scaffolding

- Pinned image SHAs in `lab`/scenario overlays; `:latest` forbidden.
- Single `uv.lock` committed.
- Fixed simulator seed per scenario YAML; same seed + same writes ⇒ bit-identical OPC UA trace.
- Scenario definitions as YAML overlays.
- `dev_down.sh && dev_up.sh` returns to a clean known state.
- Correlation IDs in every log line.

### 7.6 What the slice still doesn't do for evaluation

- No automated experiment harness (apply overlay, wait N minutes, query Prometheus — manual for now).
- No persistent metric storage (Prometheus is ephemeral; remote-write is a config-only follow-up).
- No comparative baseline against a non-EirVah implementation; that reference system is its own work.

---

## 8. Testing strategy

Five layers, fastest to slowest.

### 8.1 Layer 1 — Lint, format, type-check (sub-second)

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy .
```

Wired into a pre-commit config.

### 8.2 Layer 2 — Unit tests (under 30 s for the suite)

```bash
uv run pytest services/   # all per-service tests
uv run pytest libs/       # contract models, bus helpers, observability helpers
```

Conventions:
- Each `services/<name>/tests/test_<name>.py` mocks NATS, MQTT, AMQP, OPC UA at the boundary. No live brokers.
- Wire-schema tests in `libs/eirvah-contracts/tests/` round-trip golden JSON fixtures through pydantic models — backward-incompatible changes fail loudly.
- Worker handlers tested as pure functions: given request envelope, assert reply envelope.

### 8.3 Layer 3 — End-to-end slice tests (minutes against live k3d)

```bash
./scripts/dev_up.sh
uv run pytest tests/e2e -v
./scripts/dev_down.sh
```

**Scenarios asserted:**

1. **`test_telemetry_happy_path`** — defaults; subscribe `uniza/zilina/factory1/line_a/bottler/+/+`; within 10 s at least one v1.0 message per monitored node; correlation ID in JSON field and MQTT user property.
2. **`test_quality_propagation`** — simulator emits Bad quality on every 10th sample for one node; ~10% of messages for that node carry `quality: "bad"`; converter doesn't drop them (config says pass through).
3. **`test_actuation_happy_path`** — `allow_writes: true`; publish valid request setting setpoint to 18.0; within 5 s the setpoint reads back 18.0 via a separate OPC UA client; `approve` event on results exchange.
4. **`test_actuation_rejected_out_of_range`** — request setpoint 99.0 (outside 15–30 policy); no write; reject event with reason; `eirvah_actuation_rejected_total{reason="out_of_range"}` incremented.
5. **`test_actuation_blocked_by_flag`** — `allow_writes: false`; valid request; validator approves but no write; `eirvah_actuation_writes_blocked_total` incremented.
6. **`test_loop_closure`** — call OPC UA method `TriggerHotSpike()`; within 60 s the decision-agent-stub emits an actuation request; the setpoint observably changes; the temperature trend reverses. The canonical "the whole CPS loop works" test.
7. **`test_resilience_data_converter_restart`** — delete `data-converter` pod mid-run; `eirvah_pipeline_stage_timeout_total{stage="convert"}` increments while down; success rate returns to baseline within 30 s of readiness.
8. **`test_trace_script`** — capture a correlation ID from a telemetry message; `./scripts/trace.sh <id>` returns log lines from at least subscriber, orchestrator, converter, contextualizer, publisher, in timestamp order.

Fixture: `EirVahCluster` in `tests/e2e/conftest.py` port-forwards brokers and exposes typed clients (MQTT, AMQP, NATS, OPC UA).

### 8.4 Layer 4 — Manual / exploratory testing

- **Grafana:** `kubectl port-forward svc/grafana 3000:3000`, login with the dev Secret credentials. Two dashboards: "EirVah Edge Pipeline" (pipeline health, latency, error rates, actuation outcomes) and "Bottling Line State" (simulator's live temperature, setpoint, motor state, throughput — the visual of the actuation loop in action).
- **Watch UNS topics:** `kubectl exec -it deploy/mosquitto -- mosquitto_sub -t '#' -v`.
- **Watch the internal bus:** port-forward NATS, then `nats sub '>'` (NATS CLI, Apache-2.0).
- **Watch AMQP:** port-forward RabbitMQ on 15672, open the management UI.
- **Inspect the OPC UA address space:** `opcua-commander` (GPL-3.0) or `opcua-tui` against `opc.tcp://localhost:4840` (after port-forward). Browse the bottling-line model; read/write values by hand.

### 8.5 Layer 5 — Scenario reruns (evaluation)

Same scaffolding as Layer 3 but assertions replaced with metric capture into CSV/Parquet for offline analysis. The slice exposes everything required; the harness is a follow-up artifact.

### 8.6 Fixtures and reproducibility

- **`tests/e2e/conftest.py`** — `EirVahCluster` fixture: port-forwards brokers, typed clients, teardown.
- **Golden fixtures** — JSON files in `libs/eirvah-contracts/tests/golden/` covering every message shape (telemetry payload, actuation request, approve event, reject event, DLQ event).
- **Deterministic simulator seed** — fixed in test scenarios so e2e assertions about specific values are stable.

---

## 9. What the simulated OPC UA bottling line does

The simulator is its own pod (`opcua-simulator`) and mimics the relevant *dynamics* of a small bottling line — just enough for plausible messages, a real actuation target, and reproducible experiments.

### 9.1 Address space

```
Objects/
└── Bottler                                  (Equipment)
    ├── TemperatureSensor01
    │   └── Temperature         Double, read-only       (°C)
    ├── ThroughputMeter01
    │   └── Throughput          Double, read-only       (bottles/s)
    ├── Motor01
    │   ├── State               Int32,  read-only       (0=stopped, 1=starting, 2=running, 3=fault)
    │   └── Rpm                 Double, read-only       (rpm)
    └── SetpointUnit
        └── SetpointTemperature Double, read-write      (°C, default 22.0)
```

Defined in `config/opcua-address-space.yaml`. Scaled up for the scalability scenario without code changes.

### 9.2 Dynamics

Tick rate: configurable, default 100 ms.

**Temperature** — mean-reverting random walk around `SetpointTemperature`:
```
temperature_t = temperature_{t-1}
              + alpha * (SetpointTemperature - temperature_{t-1})    # reversion
              + Gaussian(0, sigma)                                    # noise
              + spike_contribution_t                                  # see below
```
Defaults: `alpha = 0.05`, `sigma = 0.3 °C`. Initial value = setpoint.

**Hot-spike events** — what triggers actuation in `test_loop_closure`:
- `spike_contribution` jumps to `+5 °C` for one tick, decays at `0.9^n` thereafter.
- Trigger sources:
  - Stochastic: per-tick probability from config (default 0 in unit-test scenarios; nonzero in baseline/scalability).
  - On-demand: OPC UA method `TriggerHotSpike()` — used by e2e tests for deterministic triggering. This method is the only OPC UA method in the slice and exists as a test affordance.

**Motor state machine:**
- `stopped` → after 5 s → `starting`
- `starting` → after 3 s → `running`
- `running` → with per-tick probability (default 1e-5) → `fault`
- `fault` → after 10 s → `stopped`

Fault probability is config-driven; zero for clean baselines, raised for failure scenarios.

**Motor RPM:**
- `running` → target 1500 ± 50 rpm noise.
- `starting` → linear ramp 0 → 1500 over 3 s.
- `stopped` / `fault` → 0.

**Throughput** (bottles/second):
- `running` → `0.0006 × rpm + Gaussian(0, 0.05)` ≈ 0.9 bottles/s at 1500 rpm.
- `starting` → ramps with rpm.
- `stopped` / `fault` → 0.

**Setpoint writes:**
- Accepted unconditionally by the simulator (validation belongs to the EirVah actuation path, not the device).
- Take effect at the *next* tick — the new target is used by the temperature update equation from then on.
- Logged at INFO with the writer's OPC UA session ID, so e2e tests can confirm the write reached the device.

### 9.3 Quality codes

The simulator emits an OPC UA `StatusCode` per data point, controllable from config:
- Default: every reading is `Good`.
- Per-node `bad_quality_pct: N` — fraction of samples carry `Bad`.
- Per-node `uncertain_quality_pct: N` similarly.

Used by `test_quality_propagation`.

### 9.4 Determinism

- `seed: <int>` in simulator config; all random draws (noise, spike triggers, fault triggers, quality codes) use a PRNG seeded from this value.
- Same seed + tick rate + setpoint write history ⇒ bit-identical OPC UA trace.
- Each scenario YAML captures the seed used.

### 9.5 Observability of simulator state

The simulator exposes its current internal state as Prometheus gauges on its standard `:8080/metrics` endpoint, so Grafana can render device-level truth independently of the EirVah pipeline. This is what powers the "Bottling Line State" dashboard (Section 6.7).

**Gauges emitted (one set per Equipment in the address space; labels carry the ISA-95 hierarchy):**

| Metric | Type | Description |
|---|---|---|
| `eirvah_simulator_temperature_celsius{equipment,...}` | gauge | Current temperature reading |
| `eirvah_simulator_setpoint_celsius{equipment,...}` | gauge | Current setpoint value |
| `eirvah_simulator_throughput_bottles_per_second{equipment,...}` | gauge | Current throughput |
| `eirvah_simulator_motor_state{equipment,...}` | gauge | 0=stopped, 1=starting, 2=running, 3=fault |
| `eirvah_simulator_motor_rpm{equipment,...}` | gauge | Current RPM |
| `eirvah_simulator_quality_count_total{equipment,quality,...}` | counter | Samples emitted per quality bucket |
| `eirvah_simulator_setpoint_writes_total{writer}` | counter | Writes received per OPC UA session |
| `eirvah_simulator_hot_spikes_total{trigger}` | counter | Hot-spike triggers by source (`stochastic` / `method`) |

**Diagnostic value:** if the EirVah pipeline shows no MQTT messages, comparing `eirvah_simulator_temperature_celsius` against `eirvah_pipeline_success_total{path="telemetry"}` instantly distinguishes "device produced no values" from "pipeline dropped them". The simulator's metrics surface is the *ground truth* used to diagnose every other panel.

The simulator's structured logs (visible via `kubectl logs deploy/opcua-simulator`) include an INFO line per setpoint write with the writer's OPC UA session ID, providing audit detail beyond what the counter exposes.

### 9.6 What you observe end-to-end with `allow_writes: true`

1. Temperature oscillates gently around 22 °C.
2. Throughput sits at ~0.9 bottles/s once motor is running, with small noise.
3. Periodically a hot-spike fires; temperature jumps to ~27 °C and decays over a few seconds.
4. MQTT-subscribed `decision-agent-stub` sees the spike, sends an AMQP request to set the setpoint to 19 °C.
5. Actuation path validates (within policy), writes the new setpoint, returns an `approve` event on the AMQP results exchange.
6. Temperature now reverts toward 19 °C — visibly trending down in Grafana.
7. After the spike decays, the decision agent sets the setpoint back to 22 °C; system settles.

This sequence is the observable proof the slice works as a CPS feedback loop. `test_loop_closure` asserts it mechanically; Grafana lets you watch it live.

---

## 10. Open questions and follow-ups

Not blockers for this slice, but flagged so they don't get lost:

- **Image registry choice for `lab` overlay** — `ttl.sh` (transient, OK for early experiments) vs. self-hosted Forgejo registry (longer-term, fits the "open" principle). Decide before the first scalability run.
- **Loki + structured log retention** — once we have multi-replica scenarios, `kubectl logs` aggregation across replicas gets painful. Loki (AGPL-3.0) is the obvious add when that pain point arrives.
- **Persistence for Prometheus** — slice uses ephemeral storage. For long evaluation runs, either a PV or remote-write to a longer-term store; config-only change.
- **OpenTelemetry tracing** — explicitly skipped here. When a tracing backend exists, the correlation-ID propagation already in place becomes the seed for span IDs.
- **Sparkplug B coexistence** — a question for the second-protocol slice: does the contextualizer or a separate adapter handle Sparkplug payloads, and how is the UNS topic mapping reconciled with the Sparkplug `spBv1.0/...` convention?

---

## 11. Acceptance criteria for this slice

The slice is "done for the purposes of this spec" when all of the following are true:

1. `./scripts/dev_up.sh` from a clean checkout brings up the 16-pod stack on k3d with no manual steps.
2. `uv run pytest tests/e2e -v` passes all eight scenarios in §8.3.
3. Both Grafana dashboards from §6.7 render with live data: "EirVah Edge Pipeline" (six pipeline panels) and "Bottling Line State" (simulator-state panels showing the actuation loop visually).
4. `./scripts/trace.sh <correlation_id>` returns ordered log lines from the expected pods for a recent message.
5. The `local` overlay defaults `allow_writes: false`; only `lab` enables it.
6. Every dependency in `uv.lock`, every image referenced in `deploy/`, and every tool in the README is OSI-approved open source.
7. A reviewer can reproduce a single baseline-scenario run end-to-end from the README, no tribal knowledge required.

The implementation plan (next step, written by the `writing-plans` skill) breaks these criteria into ordered, dependency-aware tasks.
