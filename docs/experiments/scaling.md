# E1 local scaling and processing-isolation preparation

This profile verifies the mechanics needed before controlled E1 experiments. It does not establish maximum capacity, an isolation benefit, industrial delivery guarantees or performance across deployments.

## Scope and acceptance

1. Send 120 distinct contextualization requests at one replica and at three replicas. Reconcile each correlated reply and responding pod with the per-pod handler-counter increase. Every selected pod must handle work; extra handling, missing replies, changed pod identity or restarts prevent a pass.
2. Run the same 1,040 scheduled requests through shared and separate processing variants: 960 industrial-data contextualizations, 40 command-policy evaluations and 40 inspection-contract validations. Both variants use two process workers, one Kubernetes pod, a 1-core CPU request/limit, 512 MiB request/limit and 128 application queue slots. Shared uses two workers and one queue; separate reserves one worker/64 slots for data and one worker/64 slots for commands/events. NATS subscription buffers also exist and use the same library defaults in both variants. This is application processing isolation, not separate broker clusters.
3. Require identical successful work, matching per-flow handler counts and one unchanged active pod in each variant. Terminating pods must disappear before warm-up/measurement: rollout status alone does not prove they stopped consuming. Worker identity is returned in a NATS header without changing the domain envelope. Client durations use the high-resolution monotonic `perf_counter`; p99 is omitted for the 40-sample control/event cohorts.
4. Exercise real CPU HPA through metrics-server. The contextualizer requests 250 millicores, has a 500-millicore limit and targets 60% of its request (150 millicores). Min/max replicas are 1/3; scale-up and scale-down have explicit stabilization/rate policies. The first 80-request/s trial measured about 126 millicores and correctly did not scale out; the fresh scale-out trial increases real offered work to 160/s, keeping the target unchanged. Offered rate and achieved completions are distinct; backpressure and timeouts remain visible.
5. Separately run the bounded E0 command case with two AMQP bridges, two coordinators and two validators, retaining one authoritative writer and its existing ledger. Capture correlated exact replay, native device acknowledgements and write-initiation records. Both instances of each replicated role must appear in the retained command evidence. Duplicate write initiations fail; missing evidence is unknown. This does not authorize multiple native writers or claim exactly-once physical effects.

The comparison runtime reuses production contextualization, command policy and inspection contract functions. Its command operation does **not** authenticate requests, issue approvals or actuate devices. It is confined to the dedicated E1 broker/subjects; the protected application command path remains separate and is checked by the replicated-command capture.

## Isolated Kubernetes profile

The standalone `deploy/k3s/overlays/e1-local` Kustomize selection creates `eirvah-e1`, an ownership-labelled namespace with its own NATS, contextualizer, comparison runtime and namespace-scoped Prometheus. HPA is deliberately excluded from the default resources so fixed-replica measurements are reproducible; the runner enables it only for its HPA phase and removes it afterward. The old unused base HPA manifest is not this profile.

The contextualizer keeps serial subscription handling but moves CPU work to a background thread so it does not starve HTTP/NATS I/O. It exports `eirvah_worker_pending`, `eirvah_worker_inflight` and `eirvah_worker_service_seconds`, alongside handler counts and process metrics. Pending means this subscription's in-memory queue, not a durable end-to-end backlog. CPU is the actual HPA input; queue and service metrics diagnose it. No custom-metrics adapter or fabricated utilization signal is introduced.

The comparison exports `eirvah_e1_pending`, `eirvah_e1_queue_seconds`, `eirvah_e1_service_seconds` and `eirvah_e1_completed_total`, labelled only by bounded pool/flow/status values. Compare cgroup CPU/memory through metrics-server: the parent process's default Prometheus process metrics alone exclude child-process usage.

## Reproduce locally

From the research repository root in PowerShell, with Docker running and the existing contextualizer image available:

```powershell
docker build -f deploy/e1.Dockerfile -t eirvah-e1:local .
if ($LASTEXITCODE -ne 0) { throw "E1 build failed" }
kind load docker-image eirvah-e1:local --name eirvah-edge
if ($LASTEXITCODE -ne 0) { throw "Image import failed" }
kubectl --context kind-eirvah-edge apply -k deploy/k3s/overlays/e1-local
if ($LASTEXITCODE -ne 0) { throw "E1 deployment failed" }
# Required after rebuilding the same local image tag:
kubectl --context kind-eirvah-edge -n eirvah-e1 rollout restart deployment/uns-auto-contextualizer deployment/e1-processing
kubectl --context kind-eirvah-edge -n eirvah-e1 rollout status deployment/uns-auto-contextualizer --timeout=120s
kubectl --context kind-eirvah-edge -n eirvah-e1 rollout status deployment/e1-processing --timeout=120s
uv run python scripts/e1_rehearsal.py --context kind-eirvah-edge --hpa-rate 160 --hpa-seconds 120
# Run after the isolated mixed-load rehearsal finishes:
uv run python scripts/e1_replicated_commands.py --context kind-eirvah-edge
```

The E1 image extends the existing OSI-only contextualizer image and adds current source/configuration, not new packages. Record its exact image ID and base-image ID for retained experiments; the local tag is a development convenience. The contextualizer base image must already exist, as produced by the project's existing build workflow.

The first runner ends with one contextualizer, one shared comparison pod and no HPA in the dedicated namespace. Its private port-forwards close. The second runner restores the original command-service replica settings; the existing E0 runner restores the automatic consumer and temporary simulator binding. Neither resets the command ledger or simulator PVCs. Failed/interrupted captures remain separate from later passing captures. After a hard interruption, inspect replica/controller/source-state files and actual cluster state before rerunning.

To inspect metrics directly:

```powershell
kubectl --context kind-eirvah-edge -n eirvah-e1 port-forward svc/e1-prometheus 9091:9090
```

Useful PromQL: `sum by (pod) (rate(eirvah_worker_handler_total[1m]))`, `eirvah_worker_pending`, and `histogram_quantile(0.95, sum by (le, flow) (rate(eirvah_e1_queue_seconds_bucket[1m])))`. HPA's current CPU measurement and conditions are available with `kubectl --context kind-eirvah-edge -n eirvah-e1 describe hpa uns-auto-contextualizer` during its phase. Prometheus here uses ephemeral local storage; retain the run evidence rather than relying on dashboard history.

## Evidence and remaining formal E1 work

Each rehearsal retains selected source/config hashes and snapshots, individual input/reply outcomes, per-pod/per-flow counter observations, pod identities, deployment resource settings, HPA CPU/condition snapshots and restoration state. The command capture additionally retains private journals, ledger rows, native readback and the existing scoped conformance checks.

Formal experiments still need declared industrial/command delivery and latency budgets, calibrated workload ladders, controlled infrastructure, repetitions, complete resource/backlog trajectories and causal closed-loop coverage where claimed. The eight-second processing comparison only proves that the variants perform the same selected work under equal configured budgets; it does not demonstrate improved tail latency or a scalability limit. Commands/events remain semantically distinct from industrial data and from observability measurements.

## Local verification - 10 November 2026

- `results/e1-local/2026-11-10T14-13-17-512287Z/` independently re-evaluates as **pass**. The one- and three-replica phases each reconciled 120 unique completions against per-pod handler counts, with every selected pod contributing and no extra handling. Shared and separate variants each completed the same 1,040 scheduled jobs under the declared equal budgets. The HPA phase completed 19,200 requests and scaled **1 -> 2 -> 1** using measured CPU. Both the fixed baseline and removal of the temporary HPA were confirmed.
- `results/e1-replicated-commands/2026-11-10T14-19-45-482279Z/` **passes**. Its bounded E0 capture retained 110 execution records and 15 submissions, with two observed process identities for each AMQP bridge/coordinator/validator role and one writer. Exact replay was observed, no command ID had multiple write initiations, and the scoped conformance checks passed. Front-end replica settings, automatic consumer and temporary simulator binding were restored.
- The retained `build-provenance.json` records the E1/base image IDs and dependency versions. A post-run live-pod check matched all three changed runtime source files to the host revision; pod image IDs/UIDs are retained. The first failed reply-header rehearsal, initial-metric/rollout corrections and below-target calibration are not relabelled as successful runs.
- Final regression validation: **669 passed, 16 skipped**. Strict mypy for all five changed runtime/tool modules, Ruff, manifest rendering and `git diff --check` pass.

This completes the selected local E1 preparation. Controlled E1 performance/scalability experiments and the separate deployment-flexibility work remain. The pilot is not evidence that separated queues improve performance under saturation.
