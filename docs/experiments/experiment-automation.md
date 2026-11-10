# Repeatable local experiment automation

The study runner executes the existing E0, E1, E2 and logical edge/cloud rehearsals as one recorded workflow. It prepares fixture state, repeats selected cases, captures resources during execution and writes a combined report. These runs verify the experiment machinery on the local cluster; they do not establish paper results or public/private/hybrid performance.

## Run from PowerShell

Use the research checkout, with Docker, `kind-eirvah-edge`, the original `eirvah-edge` stack, the dedicated E1 stack and the isolated study split namespaces already deployed using their existing runbooks. The runner does not rebuild images, recreate clusters or rotate credentials. The selected contexts/namespaces are explicit, and one authoritative native writer must be running per plant.

```powershell
cd C:\research\zilina-eirvah-edge-impl

# Prepare the isolated study fixture once; existing split namespaces/history are retained.
docker build -f deploy/study-simulator.Dockerfile -t modbus-simulator:study-automation .
kind load docker-image modbus-simulator:study-automation --name eirvah-edge
uv run python scripts/split_deployment.py apply --config config/deployments/study-split.json

# Read-only: print stages, repetitions, rate order and variant order.
uv run python scripts/study_suite.py --profile config/experiments/local-study.json --plan

# Shorter rehearsal: two paired workload repetitions with resource capture.
uv run python scripts/study_suite.py --profile config/experiments/automation-pilot.json

# Complete local workflow, two repetitions by default.
uv run python scripts/study_suite.py --profile config/experiments/local-study.json

# A single complete rehearsal, explicitly recorded as one repetition.
uv run python scripts/study_suite.py --profile config/experiments/local-study.json --repetitions 1
```

The output directory is `results/study/<UTC-run-id>`. The CLI returns nonzero when a selected stage fails or remains unknown and stops dispatching subsequent stages. Existing child reports retain their original scope: a passing selected E0 case does not turn a whole-study unknown into a pass.

## Workloads and repetitions

The versioned JSON profile selects stages, offered data rates, duration, command/event rates, repetition count, seed, sampling interval and bounded stage/cleanup timeouts. `data_rates` can contain several distinct rates; the seed reproducibly selects their order. Shared/separate processing order alternates between repetitions. Each processing variant starts with a fresh comparison worker outside the measured phase, warms its pools and waits for that pod's Prometheus counters to reconcile. Each paired comparison uses the same payloads and offered schedule; fresh identifiers distinguish new repetitions.

The `probe` stage measures the fixed-budget shared/separate processing fixture without native actuation. Its command latency is processing request/reply latency, not physical actuation latency. E1 separately verifies replica sharing, equal-budget processing variants and HPA mechanics. E2 retains native protocol/source-addition and command-response evidence; E0 and the split rehearsal exercise protected commands and feedback/recovery through their existing drivers.

No universal scientific latency, delivery or scalability budgets are invented. The supplied profiles have `budgets: null`, so the report explicitly says acceptance is not configured. Review experimental workloads, repetitions, statistical treatment and budgets before formal infrastructure runs. Optional profile budgets (`command_p95_s`, `minimum_completion_ratio`) are local criteria and do not replace architecture conformance checks.

## Reset and restoration

Before E1 work the runner restores one worker, shared processing mode and no HPA in the dedicated E1 namespace. Before selected native cases it parks the conveyor through the protected path before pausing the decision authority, conditionally resets conveyor/capper faults, sets their selected speeds, starts the capper, opens the filler and sets the warmer to 22 degrees C. It waits for process settling, restores the decision service and only then starts new bottle production. Commands, readbacks and reset outcomes are retained separately from measurements.

The study profile selects `eirvah-split-study-edge` and `eirvah-split-study-cloud` through `config/deployments/study-split.json`. These have their own credentials and PVCs; the original `eirvah-split-edge`/`eirvah-split-cloud` plant is preserved. The study simulator image starts a fresh conveyor parked so startup does not create an inspection backlog. This setting does not override recovered source state. After each study E0 or split case, the runner parks the conveyor again. Before pausing the consumer, and after parking, it waits for the real receipt path to drain the source head, retaining observations and refusing further measurements if draining is not confirmed within the bound.

Resetting does not erase command ledgers, recovery state, broker history, PVCs or cumulative counters. Workloads use counter differences and fresh identities. Stateful checks retain their own application policies; a reset does not manufacture a clean history by deleting evidence.

Each child retains its existing cleanup/restoration logic. A timeout requests cooperative cancellation, allowing `finally` cleanup to run. If cleanup does not finish within the recorded grace period, the suite stops with uncertainty rather than force-killing a mutating driver and continuing. For a normal interruption, leave the terminal open for cleanup and inspect the report before rerunning.

`results/active-study.json` prevents overlapping suites. If the host or terminal is forcibly closed, inspect its recorded PID/output, the child processes and the selected cluster state before removing a stale lock. Do not remove a lock belonging to a running suite, and do not start standalone mutating rehearsals alongside it.

## Measurements and evidence

| Output | Meaning |
| --- | --- |
| `profile.json`, `provenance.json`, `sources/` | Selected configuration, Git revision and hashed source snapshot, including local changes. |
| `suite.json`, `report.md` | Ordered stage outcomes and links to scoped child artifacts. |
| Per-stage `console.log`, `step.json`, `events.jsonl` | Driver output, bounded execution details and phase boundaries. |
| Per-stage `resources.jsonl` | Raw Kubernetes metrics/pod identities, Prometheus queue/service metrics, owned host-process counters and owned E2 Docker samples. |
| `workloads.csv` | Per-repetition, per-variant and per-flow offered/completed work, latency, achieved processing-reply throughput and submission delay. |
| `resources.csv`, `phase-resources.csv` | Sampled namespace resource summaries and eligible phase windows. |
| `host-resources.csv`, `docker-resources.csv` | Owned process/container observations, kept separate from Kubernetes totals. |
| `analysis.json`, `analysis-source.py` | Combined descriptive analysis, report-integrity checks and the analysis implementation used. |

CPU/memory sampling uses metrics-server and records the server timestamp, averaging window, pod UID/image identities and missing pods. Windows/Linux driver process counters use the standard library; E2 container statistics select only the run-owned Docker label. Suite-process counters describe the Python collector overhead; short-lived CLI children and API-server overhead are not completely attributed. Prometheus diagnostics retain histogram/queue evidence for E1 workload phases.

A missing sample is unknown, never zero. Snapshot CPU/memory maxima and sample means describe observed samples; overlapping metrics-server windows are not summed into CPU time, energy or cost. Phase attribution requires an entire averaging window inside the phase under the explicitly declared clock-uncertainty bound. The supplied 2-second local bound is an assumption for engineering rehearsal, not a measured cross-provider clock guarantee. Without a bound, phase attribution remains unknown. Raw observations remain available for revised analysis.

Achieved throughput uses the full observed schedule and request-drain window; configured offered rates are not substituted for measured rates. Submission delay records when the driver falls behind its schedule. Paired comparisons use each repetition's summary rather than pooling requests across repetitions. One or two local repetitions support a machinery check, not a statistical performance claim. Reports keep native fixture process/container resources separate from cluster resources and retain narrower child coverage limitations.

Analysis can be regenerated without operating the cluster:

```powershell
uv run python scripts/study_report.py results/study/<UTC-run-id>
```

Keep the host awake and avoid unrelated heavy workloads during rehearsals. Controlled infrastructure, protected cross-cluster endpoints, calibrated timing/coverage and the final experimental protocol remain separate work.

## Fixture readiness before interpreting results

A protected reset restores actuator state; it does not discard or automatically reconcile retained inspection deliveries. A long-running plant can have an old inspection FIFO even while current temperature/state measurements are healthy. The decision service must continue to reject stale events according to its existing contract. A feedback case that sees no confirmed automatic rejection remains failed; the report must not reinterpret that absence as a pass.

The selected study policy is an isolated fixture with controlled production windows; the original backlog is preserved. Before retained experimental runs, verify the selected fixture has drained deliveries. Keep the fixture and source-production window controlled between repetitions. Clearing source/consumer journals or command ledgers to obtain a passing result is not a reset policy implemented by this runner.
