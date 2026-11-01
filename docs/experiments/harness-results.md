# Experiment harness results

The harness uses the existing Prometheus deployment. Run from the repository root with its workspace environment. These A/B/C demonstration scenarios are preparation tools, not the study plan's E0–E3 experiments.

```bash
uv run python scripts/harness.py run --experiment c --rate 100 --duration 30
uv run python scripts/harness.py run --experiment c --rate 100 --duration 30 --expect-hpa
uv run python scripts/harness.py run --experiment a --duration 120
uv run python scripts/harness.py run --experiment b
uv run python scripts/harness.py run --experiment c --dry-run
```

Commands use the currently selected Kubernetes context. Select and verify the research context before executing them. Scenario B deletes worker pods; do not run it against an unrelated cluster. No deployment or cluster creation is performed by the harness.

## Local setup

Use Python 3.12 and the tested uv 0.4.20 version, matching the service Dockerfiles. From the repository root:

```powershell
uv --version
uv sync --frozen
uv run --frozen python -c "import asyncua, httpx, pyarrow, nats; from eirvah_contracts.envelope import NATSEnvelope; print('Experiment dependencies ready')"
```

The root `tool.uv.dev-dependencies` declaration installs the workspace services and experiment/test packages. In particular, the disturbance subprocess gets `asyncua` through the OPC UA service dependencies; no manual `pip install` or `uv run --with asyncua` is required. uv 0.4.20 does not read the newer `[dependency-groups]` declaration and does not support `sync --all-packages`.

If uv is absent or a different version is selected, install the tested version with `python -m pip install uv==0.4.20`. The lockfile controls package versions; ordinary setup uses `--frozen` and does not regenerate it. A future uv upgrade should update host instructions and Dockerfile pins together and revalidate workspace setup.

## Run outcomes

- `ok`: experiment actions completed and required queried metrics were collected without recorded gaps. This is not proof of full architecture conformance or zero loss.
- `failed`: a child process failed/timed out, generator counts failed to reconcile, requests failed/remained unresolved, recovery timed out, or execution raised an error.
- `incomplete`: actions completed but required measurement evidence was missing, stale, warned about or failed to collect.
- `interrupted`: cancellation interrupted execution. Partial evidence is retained where available; forced process termination or machine failure cannot guarantee cleanup.
- Dry runs print commands, perform no queries or subprocess actions, and create no result files.

Failed and incomplete CLI runs exit non-zero. In scenario A the disturbance process is intentionally stopped at the end of its window; that stop is recorded separately from an early exit or crash.

## Files in each run folder

| File | Contents |
|---|---|
| `run.json` | Configuration, revision, timestamps, outcome, reasons, process results and generator summary where available |
| `raw.parquet` | Prometheus samples with source timestamps, metric names, labels and values; retained even when empty |
| `collection.json` | Query definitions, optional metrics, poll count, missing queries and timestamped collection issues |
| `summary.csv` | Typed statistics with observed windows and limitations |
| `generator.json` | Scenario C counts, target/achieved sending rate, completion rate, durations, timeout and pending limit |
| `generator.process.json` / `disturbance.process.json` | Executed command, exit code, timeout/planned-stop status and process timestamps |
| `*.stdout.log`, `*.stderr.log` | Child output, including failure details; file-backed so full pipes cannot stall a child |

## Meaning of statistics

Counter increases are last minus first over each observed series window; throughput is that increase divided by elapsed sample time. They exclude work outside that window. Observed resets or insufficient samples leave increases/rates unknown; unseen resets cannot be reconstructed. Labels, including pods, remain separate. Do not sum rate estimates over unequal windows as a whole-system throughput claim.

Gauges report sample mean, minimum and maximum. The sample mean is not a time-weighted mean.

Latency summaries use aligned histogram bucket/count/sum snapshots. Means use the sum increase divided by observation-count increase. p50/p95/p99 are approximate within-bucket interpolations for each label set, not percentiles of counter values or means. Blank tails mean no observations, insufficient/inconsistent evidence, resets or a percentile falling into the infinite bucket. Quantiles across replicas are not pooled; never average per-pod percentiles. Observation counts are provided, but adequacy for tail claims still needs the pilot's statistical assessment.

Prometheus summaries include the baseline, workload and recovery samples. Their explicit first/last timestamps define their scope; they are not automatically active-phase statistics. The child-process record identifies its workload window. Generator sending rate uses its sending interval; completion rate includes waiting for outstanding replies. Generator requests test only contextualization, not end-to-end physical response.

HPA metrics are optional in scenario C unless `--expect-hpa` is supplied. Missing optional metrics remain visible. Available deployment replicas are queried independently. Stage-timeout metrics in scenario B are optional because the application creates some labelled series only after the first event. Missing required metrics, including at startup, make evidence incomplete; an empty query is never silently converted to zero.

The existing load generator bounds concurrent requests (`--max-pending`, default 1000), so achieved load can be below target under backpressure. Its `--timeout` defaults to 5 seconds; an unanswered request remains unknown, not proven lost. These values are retained in the run configuration.

## Current scenario A

Scenario A now runs the bottle-warmer production-mode demo: validated conveyor speed-setpoint commands alternate between 0.7 and 0.4 m/s, with a separate OPC UA cooling injection. The decision service selects the warmer recipe from UNS observations. The harness also forwards RabbitMQ (`--amqp-port`, default 5672). Close conflicting manual port-forwards first. The old direct temperature-setpoint override and 26-degree threshold no longer apply. See [warmer control](warmer-control.md).

An `ok` harness outcome still reports process/metric collection status only: it does not assert that every speed transition caused the expected warmer command or thermal response. Inspect command results, setpoint and measured temperature separately. Historical A results used a different controller and are not directly comparable.
