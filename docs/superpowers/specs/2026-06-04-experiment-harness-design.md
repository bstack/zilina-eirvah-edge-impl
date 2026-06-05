# Experiment Harness Design

**Date:** 2026-06-04
**Scope:** Automated orchestration and metric collection for Experiments A, B, C against a running EirVah edge stack.

---

## 1. Purpose

The harness replaces manual runbook execution with a single CLI invocation. It:
- Orchestrates experiment steps (disturbance injection, pod deletion, load injection)
- Scrapes Prometheus metrics continuously throughout each run
- Writes raw time-series to Parquet and summary statistics to CSV
- Records run metadata (git SHA, parameters, wall-clock duration) to JSON

The stack must already be up (`dev_up.sh`) before invoking the harness. Cluster lifecycle is out of scope.

---

## 2. Interface

```bash
uv run python scripts/harness.py run --experiment a
uv run python scripts/harness.py run --experiment b --duration 120
uv run python scripts/harness.py run --experiment c --rate 500 --duration 120
uv run python scripts/harness.py run --experiment a --dry-run
```

### Arguments

| Flag | Default | Description |
|------|---------|-------------|
| `--experiment` | required | `a`, `b`, or `c` |
| `--duration` | per-experiment default | Override active phase duration (seconds) |
| `--rate` | `500` | Experiment C only — load injector msg/s |
| `--namespace` | `eirvah-edge` | Kubernetes namespace |
| `--prometheus-port` | `9090` | Local port for Prometheus port-forward |
| `--nats-port` | `4222` | Local port for NATS port-forward (Experiment C) |
| `--opcua-port` | `4840` | Local port for OPC UA port-forward (Experiment A) |
| `--output-dir` | `results/` | Root directory for output |
| `--dry-run` | `false` | Print commands without executing |

### Output structure

```
results/
  experiment-{a,b,c}/
    YYYY-MM-DDTHH-MM-SS/
      raw.parquet      # columns: timestamp (unix float), metric, labels (str), value (float)
      summary.csv      # metric, mean, p50, p95, p99, min, max
      run.json         # git_sha, experiment, params, start_time, end_time, outcome
```

---

## 3. Architecture

Single file `scripts/harness.py`. Five logical sections:

### 3.1 CLI
`argparse` subcommand `run`. Validates arguments, resolves output path (timestamped), delegates to experiment function.

### 3.2 PortForward
Context manager wrapping `kubectl port-forward` as a subprocess. Opens on enter, terminates on exit. Retries once with 2 s backoff on bind failure, then raises. Used for Prometheus (all experiments), OPC UA (A), NATS (C).

```python
with port_forward("svc/prometheus", local=9090, remote=9090, namespace=ns):
    ...
```

### 3.3 Scraper
Polls `http://localhost:{prometheus_port}/api/v1/query` at a configurable interval (default 10 s). Accumulates rows as `(timestamp, metric, labels, value)` tuples in memory. On flush, writes to Parquet via `pyarrow`. Connectivity check runs before experiment steps begin — fails fast if Prometheus is unreachable.

PromQL queries are defined per-experiment as a dict `{metric_name: promql_expression}`.

### 3.4 Experiments

Each experiment is a single `async` function receiving a config dataclass and a `Scraper` instance.

**`run_experiment_a(cfg, scraper)`**
1. Port-forward Prometheus + OPC UA
2. Scrape baseline 30 s
3. Launch `disturbance.py` subprocess (default 3 cycles × 120 s = 6 min)
4. Scrape continuously at 10 s intervals during disturbance
5. Kill disturbance subprocess; scrape 60 s cooldown tail
6. Flush metrics

Metrics collected:
- `eirvah_pipeline_duration_seconds` (histogram — use `_sum/_count` for mean)
- `eirvah_actuation_requests_total`
- `eirvah_pipeline_success_total`

**`run_experiment_b(cfg, scraper)`**
1. Port-forward Prometheus
2. Scrape baseline 30 s; record steady-state success rate
3. Delete `data-converter` pod; poll until Running (timeout 120 s)
4. Scrape 60 s recovery tail
5. Delete `opcua-data-subscriber` pod; poll until Running (timeout 120 s)
6. Scrape 60 s recovery tail
7. Flush metrics

Metrics collected:
- `eirvah_pipeline_success_total`
- `eirvah_pipeline_stage_timeout_total` (labelled by stage)
- `eirvah_pipeline_e2e_latency_seconds`

**`run_experiment_c(cfg, scraper)`**
1. Port-forward Prometheus + NATS
2. Scrape baseline 30 s; confirm 1 replica
3. Launch `load_inject.py --rate {rate} --duration {duration}` subprocess
4. Scrape continuously during load + 90 s scale-down window
5. Flush metrics

Metrics collected:
- `worker_handler_total{worker="uns-auto-contextualizer"}`
- `kube_horizontalpodautoscaler_status_current_replicas`
- `container_cpu_usage_seconds_total` (cAdvisor, filtered to `uns-auto-contextualizer`)

### 3.5 Output
On flush:
- `raw.parquet` — written via `pyarrow.Table.from_pylist`; appended if file exists (supports partial flush on crash)
- `summary.csv` — computed via `pandas` groupby on metric+labels; columns: metric, labels, mean, p50, p95, p99, min, max
- `run.json` — written last; absence signals incomplete run

---

## 4. Error handling

| Failure | Behaviour |
|---------|-----------|
| Port-forward fails to bind | Retry once (2 s backoff); `SystemExit(1)` with message |
| Prometheus unreachable at start | `SystemExit(1)` before any experiment steps run |
| Pod deletion timeout (Exp B) | Record `recovery_timed_out: true` in `run.json`; continue with partial data |
| Subprocess exits non-zero (`disturbance.py`, `load_inject.py`) | Log stderr; record exit code in `run.json`; do not abort (scraped data preserved) |
| SIGINT / SIGTERM mid-run | `atexit` handler flushes partial Parquet and writes `run.json` with `outcome: interrupted` |
| Dry-run | All kubectl and subprocess commands printed to stdout; no side effects; no output files written |

---

## 5. Dependencies

All OSI-approved open source:

| Package | License | Purpose |
|---------|---------|---------|
| `httpx` | BSD-3 | Prometheus HTTP queries |
| `pyarrow` | Apache-2.0 | Parquet write |
| `pandas` | BSD-3 | Summary statistics |

`httpx` already present in the root `[dependency-groups] dev`. Add `pyarrow` and `pandas` to that same group in the root `pyproject.toml`.

---

## 6. Testing

- `--dry-run` smoke test: assert all expected kubectl/subprocess commands printed, no files written
- Unit test `Scraper.flush()` with synthetic rows: assert Parquet schema correct, summary CSV columns correct
- Unit test each experiment function with mocked `Scraper` and mocked subprocesses: assert correct sequence of kubectl calls and scraper invocations

No integration tests — those are the experiments themselves.

---

## 7. Out of scope

- Cluster creation/teardown
- Screenshot capture (manual step, covered in runbook)
- Multi-run comparison/report generation
- Lab overlay management
