# Local Demo Runbook — CPS Loop & Failure Recovery

**Purpose:** Capture proposal artefacts on a local k3d/kind cluster.
Two experiments: (A) CPS loop closes end-to-end, (B) pod failure & recovery.

**Prerequisites:** Docker, kind, kubectl, uv installed and on PATH.

> **Safety gate note:** The base deployment has `ACTUATION_CONTROL_ORCHESTRATOR_ALLOW_WRITES=false`
> by design (ADR 0001). The `local` overlay patches this to `true` so actuation writes reach
> the OPC UA simulator. Lab and production overlays should keep it `false` until explicitly
> enabled for a test run.

---

## 0. Start the stack

```bash
./scripts/dev_up.sh
```

Wait for "stack is up." All deployments must be Available before continuing.

Open port-forwards in separate terminals (or use a multiplexer):

```bash
kubectl -n eirvah-edge port-forward svc/grafana 3000:3000
kubectl -n eirvah-edge port-forward svc/opcua-simulator 4840:4840
```

Verify all pods are Running:

```bash
kubectl -n eirvah-edge get pods
```

Expected: every pod in `Running` state, no restarts.

---

## Experiment A — CPS loop closes end-to-end

**What you're showing:** temperature disturbance → telemetry path → decision-agent fires → actuation path → setpoint written back → temperature recovers.

### Step 1 — Open Grafana

Navigate to `http://localhost:3000` (credentials: `admin` / `eirvah-dev-grafana`).

Open the **EirVah Edge Pipeline** dashboard. Keep it visible.

### Step 2 — Inject a disturbance

In a new terminal:

```bash
uv run python scripts/disturbance.py
```

Default behaviour: writes setpoint 30.0°C every 120 s. Leave it running.

### Step 3 — Observe the loop (expected timeline)

| Time after disturbance | Event |
|---|---|
| ~7 s | Temperature crosses 26°C threshold |
| ~37 s | `decision-agent-stub` fires — actuation request published to RabbitMQ |
| ~40–45 s | Actuation path completes — setpoint written back via OPC UA |
| ~60 s | Actuation cooldown expires |
| 120 s | Next disturbance injected automatically |

### Step 4 — Screenshot artefacts

Capture in Grafana:

1. **Pipeline dashboard** — `eirvah_pipeline_duration_seconds` histogram showing telemetry latency
2. **Actuation panel** — actuation events counter incrementing
3. **Bottling Line State dashboard** — `SetpointTemperature` gauge changing, temperature trend reversing

### Step 5 — Stop the disturbance

`Ctrl-C` in the disturbance terminal once you have the screenshots.

---

## Experiment B — Pod failure and recovery

**What you're showing:** Kubernetes replaces a killed worker pod and the pipeline continues without operator intervention.

> **Local vs lab behaviour:** On kind with cached images, pod restart completes in ~1–2 s — faster
> than the 2 s NATS stage timeout. The pipeline shows no visible dip in throughput.
> On lab hardware with a registry pull (30–60 s restart), the timeout fires and errors accumulate
> before recovery. Local runs demonstrate *that* the system recovers; lab runs demonstrate
> *how long* it takes and what the error signal looks like.

### Step 1 — Establish a baseline

Open Grafana and navigate to the **EirVah Edge Pipeline** dashboard.

Note the steady-state value of `eirvah_pipeline_success_total` (rate ~29 events/s on local).

### Step 2 — Kill the data-converter pod

```bash
kubectl -n eirvah-edge delete pod -l app.kubernetes.io/name=data-converter
```

### Step 3 — Observe recovery

```bash
kubectl -n eirvah-edge get pod -l app.kubernetes.io/name=data-converter -w
```

Watch the replacement pod reach `Running`. On local kind: ~15–30 s. On lab hardware: 30–90 s
depending on image pull time.

In Grafana, watch:
- `eirvah_pipeline_success_total` — may show a brief dip on lab hardware; stays flat on local
- `eirvah_pipeline_e2e_latency_seconds` — latency histogram; look for a spike during downtime

Capture a screenshot showing the pod replacement and metrics behaviour.

### Step 4 — Repeat for other workers (optional)

The same procedure applies to any stateless worker:

```bash
kubectl -n eirvah-edge delete pod -l app.kubernetes.io/name=actuation-event-validator
kubectl -n eirvah-edge delete pod -l app.kubernetes.io/name=actuation-signal-publisher
```

---

## Teardown

```bash
kind delete cluster --name eirvah-edge
```
