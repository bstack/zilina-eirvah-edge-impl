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

### Step 4 — Kill opcua-data-subscriber for a more visible result

Killing the data subscriber cuts off telemetry ingress at the source. On reconnect, the OPC UA
server replays queued change notifications — producing a measurable throughput burst rather than
a gap. Observable even on local kind.

```bash
kubectl -n eirvah-edge delete pod -l app.kubernetes.io/name=opcua-data-subscriber
```

Expected pattern in `rate(eirvah_pipeline_success_total[10s])`:

| Time relative to kill | Observed behaviour |
|---|---|
| 0–5 s | Rate unchanged or minor dip (~28.8/s) |
| ~10–25 s | Catch-up burst: 1.5–2× normal rate as OPC UA backlog drains |
| ~30 s | Returns to steady state (~29/s) |

This demonstrates zero data loss (OPC UA subscription buffers changes during downtime) and
auto-recovery with no operator intervention.

### Step 5 — Repeat for other workers (optional)

```bash
kubectl -n eirvah-edge delete pod -l app.kubernetes.io/name=actuation-event-validator
kubectl -n eirvah-edge delete pod -l app.kubernetes.io/name=actuation-signal-publisher
```

---

## Experiment C — Horizontal autoscaling under load

**What you're showing:** `uns-auto-contextualizer` scales from 1 → N replicas when CPU exceeds threshold under synthetic NATS load, then scales back to 1 when load stops. The NATS queue-group subscription means replicas share work automatically with no code changes.

**How it works:** `scripts/load_inject.py` publishes `NATSEnvelope` messages directly to `uns.work.contextualize` at 500/s (20× normal throughput), cycling all 21 node IDs. The HPA watches CPU utilisation against the pod's 10m request; when it exceeds 60% (6m actual) the HPA adds replicas up to a max of 5. Scale-up window is 15s so the transition is visible; scale-down holds for 60s so pods stay up while load is running.

### Prerequisites

**dev_up.sh now handles metrics-server automatically.** If the stack is already running, verify metrics are flowing (takes ~60s after metrics-server starts):

```bash
kubectl top pods -n eirvah-edge
```

Expected: CPU/memory values for all pods. If you see `Error from server (ServiceUnavailable)`, wait 30s and retry.

**Port-forward NATS:**

```bash
kubectl -n eirvah-edge port-forward svc/nats 4222:4222 &>/tmp/pf-nats.log &
```

---

### Step 1 — Establish baseline

Confirm 1 replica, low CPU:

```bash
kubectl get hpa uns-auto-contextualizer -n eirvah-edge
```

Expected output:
```
NAME                     REFERENCE                           TARGETS   MINPODS   MAXPODS   REPLICAS   AGE
uns-auto-contextualizer  Deployment/uns-auto-contextualizer  2%/60%    1         5         1          ...
```

Note the `TARGETS` column — left value is current CPU utilisation, right is threshold.

---

### Step 2 — Open watchers

In two separate terminals:

```bash
# Terminal A — watch HPA decisions in real time
kubectl get hpa uns-auto-contextualizer -n eirvah-edge -w

# Terminal B — watch pod count change
kubectl get pods -n eirvah-edge -l app.kubernetes.io/name=uns-auto-contextualizer -w
```

Optionally open Grafana (`http://localhost:3000`) and watch:
- `rate(worker_handler_total{worker="uns-auto-contextualizer"}[10s])` — throughput across all replicas
- `kube_horizontalpodautoscaler_status_current_replicas` — replica count over time

---

### Step 3 — Inject load

In a new terminal:

```bash
uv run python scripts/load_inject.py --rate 500 --duration 120
```

Default: 500 messages/second for 120 seconds. The script reports actual throughput every 5s.

---

### Step 4 — Observe scale-out (expected timeline)

| Time after inject starts | Event |
|---|---|
| 0–15 s | CPU climbs; HPA observes utilisation above 60% threshold |
| ~15–30 s | HPA adds 2 replicas (scaleUp policy: +2 per 15s window) |
| ~30–45 s | Further replicas added if CPU still above threshold; caps at 5 |
| Steady state | 3–5 replicas sharing 500 msg/s; CPU per pod drops toward threshold |

In Terminal A you should see `REPLICAS` increment and `TARGETS` drop as load spreads across pods.

> **Note:** With a 10m CPU request and CPU-intensive rdflib SPARQL queries, utilisation will spike well above 60% at 500/s, likely driving the HPA to max replicas quickly. To see more gradual scaling, lower the rate: `--rate 150`.

---

### Step 5 — Verify correctness at scale

While the injector runs, confirm the contextualizer is processing correctly (replies going to the SINK subject are discarded, but the worker still logs outcomes):

```bash
kubectl -n eirvah-edge logs -l app.kubernetes.io/name=uns-auto-contextualizer \
  --prefix --tail=20 | grep -E "contextualizer_ready|outcome"
```

You should see log lines from multiple pod names, confirming all replicas are active.

---

### Step 6 — Observe scale-down

After the injector finishes (or `Ctrl-C`):

```bash
# Injector done — watch HPA scale back to 1
kubectl get hpa uns-auto-contextualizer -n eirvah-edge -w
```

The 60s `scaleDown.stabilizationWindowSeconds` means replicas drop after CPU has been below threshold for 60s. Expected: back to 1 replica within ~90s of load stopping.

---

### Step 7 — Screenshot artefacts

Capture:
1. **HPA watch output** — showing REPLICAS climbing from 1 → N then back to 1
2. **Grafana: throughput panel** — `worker_handler_total` rate staying flat as replicas scale (queue drained consistently)
3. **Grafana: replica panel** — `kube_horizontalpodautoscaler_status_current_replicas` step chart

---

### Step 8 — Cleanup

```bash
# Kill the port-forward
kill %1 2>/dev/null

# HPA stays deployed — remove it if you don't want it running permanently
kubectl delete hpa uns-auto-contextualizer -n eirvah-edge
```

---

## Teardown

```bash
kind delete cluster --name eirvah-edge
```
